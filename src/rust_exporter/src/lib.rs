use flate2::read::GzDecoder;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

const IO_BUF_SIZE: usize = 8 * 1024 * 1024;
const SPILL_CHUNK_SIZE: usize = 200_000;
static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Deserialize, Default)]
struct JsonItem {
    #[serde(alias = "p")]
    path: Option<String>,
    #[serde(default, rename = "i", alias = "gid")]
    short_path_id: Option<usize>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    used: Option<u64>,
    #[serde(default, rename = "s")]
    short_size: Option<u64>,
}

impl JsonItem {
    fn file_size(&self) -> u64 {
        self.size.or(self.short_size).unwrap_or(0)
    }

    fn dir_used(&self) -> u64 {
        self.used.or(self.short_size).unwrap_or(0)
    }
}

#[derive(Deserialize, Default)]
struct ReportDir {
    #[serde(default)]
    dirs: Vec<JsonItem>,
}

#[derive(Deserialize, Default)]
struct ReportFile {
    #[serde(default)]
    files: Vec<JsonItem>,
}

#[derive(Deserialize, Default)]
struct UserManifest {
    #[serde(default)]
    paths_dict: String,
    #[serde(default)]
    dirs: DirsRef,
    #[serde(default)]
    files: FilesRef,
}

#[derive(Deserialize, Default)]
struct DirsRef {
    #[serde(default)]
    path: String,
    #[serde(default)]
    parts: Vec<FilePartRef>,
}

#[derive(Deserialize, Default)]
struct FilesRef {
    #[serde(default)]
    parts: Vec<FilePartRef>,
}

#[derive(Deserialize, Default)]
struct FilePartRef {
    #[serde(default)]
    path: String,
}


struct ExportEntry {
    kind: &'static str,
    path: String,
    size: u64,
}

#[derive(Eq)]
struct HeapItem {
    size: u64,
    path: String,
    kind: &'static str,
    idx: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.size.cmp(&other.size).then_with(|| self.path.cmp(&other.path))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size && self.path == other.path && self.kind == other.kind && self.idx == other.idx
    }
}

fn format_size(size_bytes: f64) -> String {
    let n = if size_bytes.is_sign_negative() { 0.0 } else { size_bytes };
    const KB: f64 = 1_000.0;
    const MB: f64 = 1_000_000.0;
    const GB: f64 = 1_000_000_000.0;
    const TB: f64 = 1_000_000_000_000.0;

    if n >= TB {
        format!("{:.2} TB", n / TB)
    } else if n >= GB {
        format!("{:.1} GB", n / GB)
    } else if n >= MB {
        format!("{:.0} MB", n / MB)
    } else if n >= KB {
        format!("{:.0} KB", n / KB)
    } else {
        format!("{} B", n as u64)
    }
}

fn parse_file_items(user: &str, file_path: &str, kind: &'static str, entries: &mut Vec<ExportEntry>, shared_path_dict: Option<&Arc<Vec<String>>>) {
    if !Path::new(file_path).exists() {
        return;
    }

    if file_path.ends_with("manifest.json") {
        parse_manifest_items(user, file_path, kind, entries, shared_path_dict);
        return;
    }

    let f = match File::open(file_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open {}: {}", file_path, e);
            return;
        }
    };

    let pdict: Option<&[String]> = shared_path_dict.map(|d| d.as_slice());

    if file_path.ends_with(".bin") || file_path.ends_with(".bin.gz") {
        entries.extend(parse_bin_path(Path::new(file_path), kind, pdict));
    } else if file_path.ends_with(".ndjson") || file_path.ends_with(".ndjson.gz") {
        entries.extend(parse_ndjson_path(Path::new(file_path), kind, pdict));
    } else if kind == "dir " {
        if let Ok(data) = serde_json::from_reader::<_, ReportDir>(BufReader::new(f)) {
            for d in data.dirs {
                let size = d.dir_used();
                if let Some(path) = d.path {
                    entries.push(ExportEntry { kind, path, size });
                }
            }
        }
    } else if let Ok(data) = serde_json::from_reader::<_, ReportFile>(BufReader::new(f)) {
        for file in data.files {
            let size = file.file_size();
            if let Some(path) = file.path {
                entries.push(ExportEntry { kind, path, size });
            }
        }
    }
}

fn parse_ndjson_reader<R: BufRead>(reader: R, kind: &'static str, entries: &mut Vec<ExportEntry>, path_dict: Option<&[String]>) {
    for line in reader.lines().map_while(Result::ok) {
        if let Ok(item) = serde_json::from_str::<JsonItem>(&line) {
            let size = if kind == "dir " { item.dir_used() } else { item.file_size() };
            let path = if let Some(path) = item.path {
                path
            } else if let Some(path_id) = item.short_path_id {
                path_dict
                    .and_then(|dict| dict.get(path_id))
                    .cloned()
                    .unwrap_or_else(|| path_id.to_string())
            } else {
                String::new()
            };
            entries.push(ExportEntry { kind, path, size });
        }
    }
}

fn parse_ndjson_path(path: &Path, kind: &'static str, path_dict: Option<&[String]>) -> Vec<ExportEntry> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open {}: {}", path.display(), e);
            return Vec::new();
        }
    };
    let mut entries = Vec::new();
    if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        let decoder = GzDecoder::new(file);
        parse_ndjson_reader(BufReader::new(decoder), kind, &mut entries, path_dict);
    } else {
        parse_ndjson_reader(BufReader::new(file), kind, &mut entries, path_dict);
    }
    entries
}

fn parse_bin_path(path: &Path, kind: &'static str, _path_dict: Option<&[String]>) -> Vec<ExportEntry> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open {}: {}", path.display(), e);
            return Vec::new();
        }
    };
    let mut reader: Box<dyn std::io::Read> = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        Box::new(GzDecoder::new(file))
    } else {
        Box::new(file)
    };

    let mut header = [0u8; 8];
    if reader.read_exact(&mut header).is_err() || &header[0..4] != b"CDB4" {
        return Vec::new();
    }
    let rec_kind = header[5];
    let expect_kind = if kind == "dir " { 1u8 } else { 0u8 };
    if rec_kind != expect_kind {
        return Vec::new();
    }

    let mut out = Vec::new();
    if expect_kind == 1 {
        loop {
            let mut rec = [0u8; 12];
            if reader.read_exact(&mut rec).is_err() {
                break;
            }
            let path_id = u32::from_le_bytes([rec[0], rec[1], rec[2], rec[3]]) as usize;
            let used = i64::from_le_bytes([rec[4], rec[5], rec[6], rec[7], rec[8], rec[9], rec[10], rec[11]]);
            let path = path_id.to_string();
            out.push(ExportEntry { kind, path, size: used.max(0) as u64 });
        }
    } else {
        loop {
            let mut base = [0u8; 14];
            if reader.read_exact(&mut base).is_err() {
                break;
            }
            let path_id = u32::from_le_bytes([base[0], base[1], base[2], base[3]]) as usize;
            let size = u64::from_le_bytes([base[4], base[5], base[6], base[7], base[8], base[9], base[10], base[11]]);
            let ext_len = u16::from_le_bytes([base[12], base[13]]) as usize;
            let mut ext_buf = vec![0u8; ext_len];
            if reader.read_exact(&mut ext_buf).is_err() {
                break;
            }
            let path = path_id.to_string();
            out.push(ExportEntry { kind, path, size });
        }
    }
    out
}


/// Fallback: load full path_dict Vec<String> — used when no seek file present.
fn load_path_dict_ndjson(ndjson_path: &Path) -> Vec<String> {
    let file = match File::open(ndjson_path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };
    let reader = BufReader::new(file);
    let mut paths = Vec::new();
    for line in reader.lines().map_while(Result::ok) {
        if let Ok(item) = serde_json::from_str::<JsonItem>(&line) {
            if let (Some(gid), Some(p)) = (item.short_path_id, item.path) {
                if paths.len() <= gid {
                    paths.resize(gid + 1, String::new());
                }
                paths[gid] = p;
            }
        }
    }
    paths
}

fn parse_manifest_items(user: &str, manifest_path: &str, kind: &'static str, entries: &mut Vec<ExportEntry>, shared_path_dict: Option<&Arc<Vec<String>>>) {
    let detail_dir = Path::new(manifest_path).parent().unwrap_or_else(|| Path::new("."));
    let safe_user: String = user
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect();
    let user_manifest_path = detail_dir.join("users").join(safe_user).join("manifest.json");

    let file = match File::open(&user_manifest_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open user manifest {}: {}", user_manifest_path.display(), e);
            return;
        }
    };
    let user_manifest: UserManifest = match serde_json::from_reader(BufReader::new(file)) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to parse user manifest {}: {}", user_manifest_path.display(), e);
            return;
        }
    };
    let user_dir = user_manifest_path.parent().unwrap_or(detail_dir);

    let ndjson_path = if user_manifest.paths_dict.is_empty() {
        detail_dir.join("api").join("path_dict.ndjson")
    } else {
        user_dir.join(&user_manifest.paths_dict)
    };
    // Load path_dict once per manifest (no seek, no lock contention)
    let fallback_dict: Vec<String>;
    let pdict: Option<&[String]> = if let Some(d) = shared_path_dict {
        Some(d.as_slice())
    } else {
        fallback_dict = load_path_dict_ndjson(&ndjson_path);
        if fallback_dict.is_empty() { None } else { Some(&fallback_dict) }
    };

    if kind == "dir " {
        if !user_manifest.dirs.parts.is_empty() {
            for part in user_manifest.dirs.parts {
                if !part.path.is_empty() {
                    let p = user_dir.join(part.path);
                    if p.extension().and_then(|s| s.to_str()) == Some("gz") && p.to_string_lossy().ends_with(".bin.gz") {
                        entries.extend(parse_bin_path(&p, kind, pdict));
                    } else if p.extension().and_then(|s| s.to_str()) == Some("bin") {
                        entries.extend(parse_bin_path(&p, kind, pdict));
                    } else {
                        entries.extend(parse_ndjson_path(&p, kind, pdict));
                    }
                }
            }
        } else {
            let dir_rel = if user_manifest.dirs.path.is_empty() { "dirs.ndjson" } else { user_manifest.dirs.path.as_str() };
            entries.extend(parse_ndjson_path(&user_dir.join(dir_rel), kind, pdict));
        }
    } else {
        let part_paths: Vec<PathBuf> = user_manifest.files.parts
            .into_iter()
            .filter(|part| !part.path.is_empty())
            .map(|part| user_dir.join(part.path))
            .collect();
        let mut part_entries: Vec<ExportEntry> = part_paths
            .par_iter()
            .flat_map_iter(|path| {
                if path.extension().and_then(|s| s.to_str()) == Some("gz") && path.to_string_lossy().ends_with(".bin.gz") {
                    parse_bin_path(path, kind, pdict)
                } else if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                    parse_bin_path(path, kind, pdict)
                } else {
                    parse_ndjson_path(path, kind, pdict)
                }
            })
            .collect();
        entries.append(&mut part_entries);
    }

}

fn spill_chunk(tmp_dir: &Path, user: &str, chunk_index: usize, entries: &mut Vec<ExportEntry>) -> Result<PathBuf, String> {
    entries.sort_unstable_by(|a, b| b.size.cmp(&a.size));
    let safe_user: String = user.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' }).collect();
    let path = tmp_dir.join(format!("{}_chunk_{:05}.tsv", safe_user, chunk_index));
    let file = File::create(&path).map_err(|e| format!("create spill {}: {}", path.display(), e))?;
    let mut writer = BufWriter::with_capacity(IO_BUF_SIZE, file);
    for e in entries.drain(..) {
        writeln!(writer, "{}\t{}\t{}", e.size, e.kind, e.path)
            .map_err(|err| format!("write spill {}: {}", path.display(), err))?;
    }
    writer.flush().map_err(|e| format!("flush spill {}: {}", path.display(), e))?;
    Ok(path)
}

fn read_spill_line(reader: &mut BufReader<File>, idx: usize) -> Option<HeapItem> {
    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let trimmed = line.trim_end_matches(['\n', '\r']);
    let mut parts = trimmed.splitn(3, '\t');
    let size: u64 = parts.next()?.parse().ok()?;
    let kind = parts.next()?;
    let path = parts.next()?.to_string();
    let kind_static: &'static str = if kind == "dir " { "dir " } else { "file" };
    Some(HeapItem { size, path, kind: kind_static, idx })
}

fn write_sorted_entries(user: &str, out_path: &str, entries: &mut Vec<ExportEntry>, tmp_dir: &Path) -> Result<String, String> {
    if entries.is_empty() {
        return Ok("".to_string());
    }
    if let Some(parent) = Path::new(out_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let out_file = File::create(out_path)
        .map_err(|e| format!("Cannot create text file {}: {}", out_path, e))?;
    let mut w = BufWriter::with_capacity(IO_BUF_SIZE, out_file);

    writeln!(w, "{:<4}  {:<20}  {:>12}  Path", "Type", "User", "Size")
        .map_err(|e| e.to_string())?;
    writeln!(w, "{}", "-".repeat(90))
        .map_err(|e| e.to_string())?;

    if entries.len() <= SPILL_CHUNK_SIZE {
        entries.sort_unstable_by(|a, b| b.size.cmp(&a.size));
        for e in entries.drain(..) {
            writeln!(w, "{:<4}  {:<20}  {:>12}  {}", e.kind, user, format_size(e.size as f64), e.path)
                .map_err(|e| e.to_string())?;
        }
        w.flush().map_err(|e| e.to_string())?;
        return Ok(out_path.to_string());
    }

    let mut spill_paths = Vec::new();
    let mut chunk_index = 0usize;
    while !entries.is_empty() {
        let take = entries.len().min(SPILL_CHUNK_SIZE);
        let mut chunk: Vec<ExportEntry> = entries.drain(entries.len() - take..).collect();
        let p = spill_chunk(tmp_dir, user, chunk_index, &mut chunk)
            .map_err(|e| e.to_string())?;
        spill_paths.push(p);
        chunk_index += 1;
    }

    let mut readers = Vec::new();
    let mut heap = BinaryHeap::new();
    for (idx, p) in spill_paths.iter().enumerate() {
        let f = File::open(p).map_err(|e| format!("open spill {}: {}", p.display(), e))?;
        let mut r = BufReader::with_capacity(IO_BUF_SIZE, f);
        if let Some(item) = read_spill_line(&mut r, idx) {
            heap.push(item);
        }
        readers.push(r);
    }

    while let Some(item) = heap.pop() {
        writeln!(w, "{:<4}  {:<20}  {:>12}  {}", item.kind, user, format_size(item.size as f64), item.path)
            .map_err(|e| e.to_string())?;
        if let Some(next_item) = read_spill_line(&mut readers[item.idx], item.idx) {
            heap.push(next_item);
        }
    }

    w.flush().map_err(|e| e.to_string())?;
    for p in spill_paths {
        let _ = std::fs::remove_file(p);
    }
    Ok(out_path.to_string())
}

fn process_internal(
    user: &str,
    dir_path: &str,
    file_path: &str,
    out_path: &str,
    shared_path_dict: Option<&Arc<Vec<String>>>,
) -> Result<String, String> {
    let mut entries = Vec::new();

    if !dir_path.is_empty() {
        parse_file_items(user, dir_path, "dir ", &mut entries, shared_path_dict);
    }

    if !file_path.is_empty() {
        parse_file_items(user, file_path, "file", &mut entries, shared_path_dict);
    }

    if entries.is_empty() {
        return Ok("".to_string());
    }

    let tmp_parent = Path::new(out_path).parent().unwrap_or_else(|| Path::new("."));
    let seq = SPILL_SEQ.fetch_add(1, AtomicOrdering::Relaxed);
    let spill_dir = tmp_parent.join(format!(".export_spill_{}_{}_{}", std::process::id(), user, seq));
    let _ = std::fs::create_dir_all(&spill_dir);
    let result = write_sorted_entries(user, out_path, &mut entries, &spill_dir);
    let _ = std::fs::remove_dir_all(&spill_dir);
    result
}

fn process_user_job(
    user: &str,
    unified_path: &str,
    dir_path: &str,
    file_path: &str,
    output_dir: &str,
    prefix: &str,
) -> Result<Vec<String>, String> {
    let has_unified = !unified_path.is_empty() && Path::new(unified_path).exists();
    let has_dir = !dir_path.is_empty() && Path::new(dir_path).exists();
    let has_file = !file_path.is_empty() && Path::new(file_path).exists();
    if !has_unified && !has_dir && !has_file {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();
    let mut base_parts = Vec::new();
    if !prefix.is_empty() {
        base_parts.push(prefix.to_string());
    }
    base_parts.push("usage".to_string());

    if has_unified {
        let out_dir_fname = format!("{}_dir_{}.txt", base_parts.join("_"), user);
        let out_dir_path = Path::new(output_dir).join(out_dir_fname);
        let dir_out = process_internal(user, unified_path, "", out_dir_path.to_string_lossy().as_ref(), None)?;
        if !dir_out.is_empty() {
            results.push(dir_out);
        }

        let out_file_fname = format!("{}_file_{}.txt", base_parts.join("_"), user);
        let out_file_path = Path::new(output_dir).join(out_file_fname);
        let file_out = process_internal(user, "", unified_path, out_file_path.to_string_lossy().as_ref(), None)?;
        if !file_out.is_empty() {
            results.push(file_out);
        }
        return Ok(results);
    }

    if has_dir {
        let out_dir_fname = format!("{}_dir_{}.txt", base_parts.join("_"), user);
        let out_dir_path = Path::new(output_dir).join(out_dir_fname);
        let dir_out = process_internal(user, dir_path, "", out_dir_path.to_string_lossy().as_ref(), None)?;
        if !dir_out.is_empty() {
            results.push(dir_out);
        }
    }

    if has_file {
        let out_file_fname = format!("{}_file_{}.txt", base_parts.join("_"), user);
        let out_file_path = Path::new(output_dir).join(out_file_fname);
        let file_out = process_internal(user, "", file_path, out_file_path.to_string_lossy().as_ref(), None)?;
        if !file_out.is_empty() {
            results.push(file_out);
        }
    }
    Ok(results)
}


#[pyfunction]
fn process(user: String, dir_path: String, file_path: String, out_path: String) -> PyResult<String> {
    process_internal(&user, &dir_path, &file_path, &out_path, None)
        .map_err(PyRuntimeError::new_err)
}

#[pyfunction(signature = (jobs, workers=4))]
fn process_jobs(
    jobs: Vec<(String, String, String, String, String, String)>,
    workers: usize,
) -> PyResult<Vec<String>> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("build thread pool: {}", e)))?;

    let per_job: Vec<Result<Vec<String>, String>> = pool.install(|| {
        jobs.par_iter()
            .map(|(user, unified_path, dir_path, file_path, output_dir, prefix)| {
                process_user_job(user, unified_path, dir_path, file_path, output_dir, prefix)
            })
            .collect()
    });

    let mut outputs = Vec::new();
    for item in per_job {
        match item {
            Ok(mut files) => outputs.append(&mut files),
            Err(e) => return Err(PyRuntimeError::new_err(e)),
        }
    }
    Ok(outputs)
}

#[pymodule]
fn export_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process, m)?)?;
    m.add_function(wrap_pyfunction!(process_jobs, m)?)?;
    Ok(())
}
