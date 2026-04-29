use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Deserialize, Default)]
struct JsonItem {
    path: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    used: Option<u64>,
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
struct DetailRootManifest {
    #[serde(default)]
    users: Vec<DetailRootUser>,
}

#[derive(Deserialize, Default)]
struct DetailRootUser {
    username: String,
    manifest: String,
}

#[derive(Deserialize, Default)]
struct UserManifest {
    #[serde(default)]
    dirs: DirsRef,
    #[serde(default)]
    files: FilesRef,
}

#[derive(Deserialize, Default)]
struct DirsRef {
    #[serde(default)]
    path: String,
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

fn parse_file_items(user: &str, file_path: &str, kind: &'static str, entries: &mut Vec<ExportEntry>) {
    if !Path::new(file_path).exists() {
        return;
    }

    if file_path.ends_with("data_detail.json") {
        parse_manifest_items(user, file_path, kind, entries);
        return;
    }

    let f = match File::open(file_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open {}: {}", file_path, e);
            return;
        }
    };

    if file_path.ends_with(".ndjson") {
        parse_ndjson_reader(f, kind, entries);
    } else if kind == "dir " {
        if let Ok(data) = serde_json::from_reader::<_, ReportDir>(BufReader::new(f)) {
            for d in data.dirs {
                if let Some(path) = d.path {
                    entries.push(ExportEntry { kind, path, size: d.used.unwrap_or(0) });
                }
            }
        }
    } else if let Ok(data) = serde_json::from_reader::<_, ReportFile>(BufReader::new(f)) {
        for file in data.files {
            if let Some(path) = file.path {
                entries.push(ExportEntry { kind, path, size: file.size.unwrap_or(0) });
            }
        }
    }
}

fn parse_ndjson_reader(file: File, kind: &'static str, entries: &mut Vec<ExportEntry>) {
    for line in BufReader::new(file).lines().map_while(Result::ok) {
        if let Ok(item) = serde_json::from_str::<JsonItem>(&line) {
            if let Some(path) = item.path {
                let size = if kind == "dir " { item.used.unwrap_or(0) } else { item.size.unwrap_or(0) };
                entries.push(ExportEntry { kind, path, size });
            }
        }
    }
}

fn parse_ndjson_path(path: &Path, kind: &'static str) -> Vec<ExportEntry> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open {}: {}", path.display(), e);
            return Vec::new();
        }
    };
    let mut entries = Vec::new();
    parse_ndjson_reader(file, kind, &mut entries);
    entries
}

fn parse_manifest_items(user: &str, manifest_path: &str, kind: &'static str, entries: &mut Vec<ExportEntry>) {
    let file = match File::open(manifest_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open manifest {}: {}", manifest_path, e);
            return;
        }
    };
    let root_manifest: DetailRootManifest = match serde_json::from_reader(BufReader::new(file)) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to parse manifest {}: {}", manifest_path, e);
            return;
        }
    };
    let Some(user_entry) = root_manifest.users.into_iter().find(|entry| entry.username == user) else {
        return;
    };
    let detail_dir = Path::new(manifest_path).parent().unwrap_or_else(|| Path::new("."));
    let user_manifest_path = detail_dir.join(user_entry.manifest);
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

    if kind == "dir " {
        let dir_rel = if user_manifest.dirs.path.is_empty() { "dirs.ndjson" } else { user_manifest.dirs.path.as_str() };
        entries.extend(parse_ndjson_path(&user_dir.join(dir_rel), kind));
    } else {
        let part_paths: Vec<PathBuf> = user_manifest.files.parts
            .into_iter()
            .filter(|part| !part.path.is_empty())
            .map(|part| user_dir.join(part.path))
            .collect();
        let mut part_entries: Vec<ExportEntry> = part_paths
            .par_iter()
            .flat_map_iter(|path| parse_ndjson_path(path, kind))
            .collect();
        entries.append(&mut part_entries);
    }
}


#[pyfunction]
fn process(user: String, dir_path: String, file_path: String, out_path: String) -> PyResult<String> {
    let mut entries = Vec::new();

    if !dir_path.is_empty() {
        parse_file_items(&user, &dir_path, "dir ", &mut entries);
    }

    if !file_path.is_empty() {
        parse_file_items(&user, &file_path, "file", &mut entries);
    }

    if entries.is_empty() {
        return Ok("".to_string());
    }

    entries.sort_unstable_by(|a, b| b.size.cmp(&a.size));

    if let Some(parent) = Path::new(&out_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let out_file = File::create(&out_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Cannot create text file {}: {}", out_path, e)))?;
    let mut w = BufWriter::new(out_file);

    writeln!(w, "{:<4}  {:<20}  {:>12}  Path", "Type", "User", "Size")
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    writeln!(w, "{}", "-".repeat(90))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    for e in entries {
        writeln!(w, "{:<4}  {:<20}  {:>12}  {}", e.kind, user, format_size(e.size as f64), e.path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }

    w.flush().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(out_path)
}

#[pymodule]
fn export_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process, m)?)?;
    Ok(())
}
