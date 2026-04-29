use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyRuntimeError;
use ignore::{WalkBuilder, WalkState};
use rusqlite::{Connection, params};
use dashmap::DashSet;
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::fs;
use std::io::{Write, BufWriter};
use std::time::{Instant, Duration};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use rayon::prelude::*;

mod unified_db;
pub use unified_db::build_unified_dbs;

// Same list as Python's critical_skip_dirs
const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot", ".snapshots", ".zfs",
    "proc", "sys", "dev",
    ".nfs",
];
/// Max in-memory file entries per UID buffer before flushing to disk.
const DETAIL_FLUSH_THRESHOLD: usize = 200_000;
/// Max in-memory dir-size entries per thread buffer before flushing to disk.
const DIR_FLUSH_THRESHOLD: usize = 200_000;
/// Keep balanced compression level for treemap shards.
const TREEMAP_ZLIB_LEVEL: u32 = 6;

fn format_num(mut n: u64) -> String {
    if n == 0 { return "0".to_string(); }
    let mut s = String::new();
    let mut count = 0;
    while n > 0 {
        if count != 0 && count % 3 == 0 { s.insert(0, ','); }
        s.insert(0, (b'0' + (n % 10) as u8) as char);
        n /= 10;
        count += 1;
    }
    s
}

fn format_size(bytes: u64) -> String {
    let kb = 1024_f64;
    let mb = kb * 1024_f64;
    let gb = mb * 1024_f64;
    let tb = gb * 1024_f64;
    let bytes_f = bytes as f64;
    if bytes_f >= tb { format!("{:.2} TB", bytes_f / tb) }
    else if bytes_f >= gb { format!("{:.2} GB", bytes_f / gb) }
    else if bytes_f >= mb { format!("{:.2} MB", bytes_f / mb) }
    else if bytes_f >= kb { format!("{:.2} KB", bytes_f / kb) }
    else { format!("{} B", bytes) }
}

fn format_rate(rate: f64) -> String {
    // e.g. 300,123.4
    let int_part = rate as u64;
    let frac = ((rate - int_part as f64).abs() * 10.0).round() as u8;
    format!("{}.{}", format_num(int_part), frac)
}

/// Read RSS memory from /proc/self/status in MB (Linux only).
fn get_rss_mb() -> f64 {
    // VmRSS line looks like: "VmRSS:   123456 kB"
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let kb: u64 = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                return kb as f64 / 1024.0;
            }
        }
    }
    0.0
}

struct GlobalStats {
    total_files: u64,
    total_dirs: u64,
    total_inodes: u64,
    total_size: u64,
    uid_sizes: HashMap<u32, u64>,
    uid_files: HashMap<u32, u64>,
    // dir_sizes removed — streamed to per-thread TSV files to bound peak RAM
    permission_issues: Vec<(String, String, String, Option<u32>)>, // (path, kind, error, uid)
}

// ProgressStats replaced by 3 AtomicU64 (no lock, exact counts)


struct ThreadLocalState {
    t_files: u64,
    t_dirs: u64,
    t_inodes: u64,
    t_size: u64,
    t_uid_sizes: HashMap<u32, u64>,
    t_uid_files: HashMap<u32, u64>,
    /// Streaming dir-size buffer: (path, uid, size) flushed to TSV when full
    t_dir_buf: Vec<(String, u32, u64)>,
    t_dir_flush_count: u32,
    t_uid_buffers: HashMap<u32, Vec<(String, u64)>>,
    t_flush_counts: HashMap<u32, u32>,
    t_perm_issues: Vec<(String, String, String, Option<u32>)>,
    global_stats: Arc<Mutex<GlobalStats>>,
    prog_files: Arc<AtomicU64>,
    prog_dirs:  Arc<AtomicU64>,
    prog_size:  Arc<AtomicU64>,
    tmpdir: String,
    target_uids: Option<HashSet<u32>>,
    thread_id: usize,
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        // 1. Flush remaining file-detail buffers
        for (uid, buf) in self.t_uid_buffers.iter_mut() {
            if buf.is_empty() { continue; }
            let count = self.t_flush_counts.entry(*uid).or_insert(0);
            *count += 1;
            buf.sort_by(|a, b| b.1.cmp(&a.1));
            let filepath = format!("{}/uid_{}_t{}_c{}.tsv", self.tmpdir, uid, self.thread_id, count);
            if let Ok(f) = fs::File::create(&filepath) {
                let mut w = BufWriter::new(f);
                for (p, s) in buf.iter() {
                    let _ = writeln!(w, "{}\t{}", s, p);
                }
            }
            buf.clear();
            if buf.capacity() > DETAIL_FLUSH_THRESHOLD * 2 {
                buf.shrink_to_fit();
            }
        }

        // 2. Flush remaining dir-size buffer to TSV (streaming — avoids keeping in GlobalStats)
        if !self.t_dir_buf.is_empty() {
            self.t_dir_flush_count += 1;
            let fp = format!("{}/dirs_t{}_c{}.tsv", self.tmpdir, self.thread_id, self.t_dir_flush_count);
            if let Ok(f) = fs::File::create(&fp) {
                let mut w = BufWriter::new(f);
                for (path, uid, size) in self.t_dir_buf.iter() {
                    let _ = writeln!(w, "{}\t{}\t{}", path, uid, size);
                }
            }
            self.t_dir_buf.clear();
            self.t_dir_buf.shrink_to_fit();
        }

        // 3. Merge scalar stats into global state
        if let Ok(mut g) = self.global_stats.lock() {
            g.total_files += self.t_files;
            g.total_dirs  += self.t_dirs;
            g.total_inodes += self.t_inodes;
            g.total_size  += self.t_size;
            for (uid, size) in &self.t_uid_sizes {
                *g.uid_sizes.entry(*uid).or_insert(0) += size;
            }
            for (uid, files) in &self.t_uid_files {
                *g.uid_files.entry(*uid).or_insert(0) += files;
            }
            g.permission_issues.extend(self.t_perm_issues.drain(..));
        }
    }
}

#[pyfunction]
fn scan_disk(
    py: Python,
    directory: String,
    skip_dirs: Vec<String>,
    target_uids: Option<Vec<u32>>,
    max_workers: Option<usize>,
) -> PyResult<PyObject> {
    let _tmpdir = tempfile::Builder::new()
        .prefix("checkdisk_rust_")
        .tempdir()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let tmpdir_str = _tmpdir.path().to_string_lossy().to_string();
    let _ = _tmpdir.keep(); // persist tmp dir; Python side cleans up later

    let global_stats = Arc::new(Mutex::new(GlobalStats {
        total_files: 0, total_dirs: 0, total_inodes: 0, total_size: 0,
        uid_sizes: HashMap::new(), uid_files: HashMap::new(),
        // dir_sizes removed — streamed to per-thread TSV files
        permission_issues: Vec::new(),
    }));
    let prog_files = Arc::new(AtomicU64::new(0));
    let prog_dirs  = Arc::new(AtomicU64::new(0));
    let prog_size  = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));

    // Determine root device for cross-device check (NFS, snapshots, bind-mounts)
    let root_dev: Option<u64> = fs::metadata(&directory).ok().map(|m| m.dev());

    let g_clone = global_stats.clone();
    let pf_clone = prog_files.clone();
    let pd_clone = prog_dirs.clone();
    let ps_clone = prog_size.clone();
    let d_clone = done.clone();
    let dir_clone = directory.clone();
    let skips = skip_dirs.clone();
    let tmpdir_clone = tmpdir_str.clone();

    let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    // Keep a sane default and let CLI max_workers override explicitly.
    let default_threads = (cpus * 2).clamp(4, 32);
    let threads_count = max_workers.unwrap_or(default_threads).max(1);
    let thread_counter = Arc::new(AtomicUsize::new(0));
    let target_uids_shared = Arc::new(
        target_uids.map(|uids| uids.into_iter().collect::<HashSet<u32>>())
    );
    // Shared cross-worker hard-link deduplication — DashSet avoids Mutex bottleneck
    let hardlink_inodes: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    // Shared directory loop/bind-mount deduplication — DashSet (16 shards by default)
    let visited_dirs: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());

    let _walk_thread = thread::spawn(move || {
        WalkBuilder::new(&dir_clone)
            .hidden(false)
            .ignore(false)
            .git_ignore(false)
            .git_exclude(false)
            .git_global(false)
            .threads(threads_count)
            .build_parallel()
            .run(|| {
                let tid = thread_counter.fetch_add(1, Ordering::SeqCst);
                let mut state = ThreadLocalState {
                    t_files: 0, t_dirs: 0, t_inodes: 0, t_size: 0,
                    t_uid_sizes: HashMap::new(),
                    t_uid_files: HashMap::new(),
                    t_dir_buf: Vec::new(),
                    t_dir_flush_count: 0,
                    t_uid_buffers: HashMap::new(),
                    t_flush_counts: HashMap::new(),
                    t_perm_issues: Vec::new(),
                    global_stats: g_clone.clone(),
                    prog_files: pf_clone.clone(),
                    prog_dirs:  pd_clone.clone(),
                    prog_size:  ps_clone.clone(),
                    tmpdir: tmpdir_clone.clone(),
                    target_uids: (*target_uids_shared).clone(),
                    thread_id: tid,
                };
                let skips = skips.clone();
                let dir = dir_clone.clone();
                let hardlinks_shared = hardlink_inodes.clone();
                let visited_dirs_shared = visited_dirs.clone();

                Box::new(move |entry_res| {
                    // --- Error entry: record as permission issue ---
                    let entry = match entry_res {
                        Ok(e) => e,
                        Err(err) => {
                            let err_str = err.to_string();
                            // ignore::Error formats as: "/path/to/dir: Permission denied (os error 13)"
                            let path_str = err_str.find(": ")
                                .map(|idx| err_str[..idx].to_string())
                                .unwrap_or_default();
                            
                            let uid_opt = if !path_str.is_empty() {
                                fs::symlink_metadata(&path_str).map(|m| m.uid()).ok()
                            } else {
                                None
                            };

                            state.t_perm_issues.push((
                                path_str,
                                "directory".to_string(),
                                err_str,
                                uid_opt,
                            ));
                            return WalkState::Continue;
                        }
                    };

                    let path = entry.path();

                    // --- Configured skip_dirs (prefix match — prunes whole subtree) ---
                    for s in &skips {
                        if path.starts_with(s) {
                            return WalkState::Skip;
                        }
                    }

                    let ft = match entry.file_type() {
                        Some(f) => f,
                        None => return WalkState::Continue,
                    };

                    if ft.is_symlink() {
                        state.t_inodes += 1;
                        if let Ok(meta) = fs::symlink_metadata(path) {
                            let uid = meta.uid();
                            let is_target = match &state.target_uids {
                                Some(set) => set.contains(&uid),
                                None => true,
                            };
                            if is_target {
                                *state.t_uid_files.entry(uid).or_insert(0) += 1;
                            }
                        }
                        return WalkState::Continue;
                    }

                    if ft.is_dir() {
                        // --- Name-based skip (critical_skip_dirs) ---
                        if let Some(name) = path.file_name() {
                            let name_str = name.to_string_lossy();
                            if CRITICAL_SKIP_NAMES.contains(&name_str.as_ref()) {
                                return WalkState::Skip;
                            }
                        }

                        // --- Bind mount / Loop deduplication ---
                        if let Ok(meta) = entry.metadata() {
                            let uid = meta.uid();
                            let is_target = match &state.target_uids {
                                Some(set) => set.contains(&uid),
                                None => true,
                            };
                            if is_target {
                                *state.t_uid_files.entry(uid).or_insert(0) += 1;
                            }

                            let key = (meta.ino(), meta.dev());
                            // DashSet.insert() returns false when key already exists
                            if !visited_dirs_shared.insert(key) {
                                return WalkState::Skip;
                            }

                            // --- Cross-device check: skip NFS / snapshots / bind-mounts ---
                            if let Some(rdev) = root_dev {
                                if meta.dev() != rdev {
                                    return WalkState::Skip;
                                }
                            }
                        }

                        state.t_dirs += 1;
                        state.t_inodes += 1;
                        state.prog_dirs.fetch_add(1, Ordering::Relaxed);
                    } else if ft.is_file() {
                        let meta = match entry.metadata() {
                            Ok(m) => m,
                            Err(e) => {
                                let uid_opt = fs::symlink_metadata(path).map(|m| m.uid()).ok();
                                state.t_perm_issues.push((
                                    path.to_string_lossy().into_owned(),
                                    "file".to_string(),
                                    e.to_string(),
                                    uid_opt,
                                ));
                                return WalkState::Continue;
                            }
                        };

                        // --- Hard-link deduplication ---
                        if meta.nlink() > 1 {
                            let key = (meta.ino(), meta.dev());
                            if !hardlinks_shared.insert(key) { return WalkState::Continue; }
                        }

                        // st_blocks * 512 = actual on-disk bytes, same as Python legacy
                        let size = meta.blocks() * 512;
                        let uid  = meta.uid();
                        let is_target = match &state.target_uids {
                            Some(set) => set.contains(&uid),
                            None => true,
                        };

                        state.t_files += 1;
                        state.t_inodes += 1;
                        state.t_size  += size;

                        if is_target {
                            *state.t_uid_sizes.entry(uid).or_insert(0) += size;
                            *state.t_uid_files.entry(uid).or_insert(0) += 1;

                            // Attribute size to direct parent dir only (flat, matches Python legacy)
                            // Stream to TSV file: (path, uid, size) — never accumulate in GlobalStats
                            if let Some(parent) = path.parent() {
                                if parent.starts_with(&dir) {
                                    let parent_str = parent.to_string_lossy().into_owned();
                                    state.t_dir_buf.push((parent_str, uid, size));
                                    if state.t_dir_buf.len() >= DIR_FLUSH_THRESHOLD {
                                        state.t_dir_flush_count += 1;
                                        let fp = format!("{}/dirs_t{}_c{}.tsv",
                                            state.tmpdir, state.thread_id, state.t_dir_flush_count);
                                        if let Ok(f) = fs::File::create(&fp) {
                                            let mut w = BufWriter::new(f);
                                            for (dp, du, ds) in state.t_dir_buf.iter() {
                                                let _ = writeln!(w, "{}\t{}\t{}", dp, du, ds);
                                            }
                                        }
                                        state.t_dir_buf.clear();
                                        if state.t_dir_buf.capacity() > DIR_FLUSH_THRESHOLD * 2 {
                                            state.t_dir_buf.shrink_to_fit();
                                        }
                                    }
                                }
                            }

                            // Streaming TSV buffer (same as Python's DETAIL_FLUSH_THRESHOLD)
                            let buf = state.t_uid_buffers.entry(uid).or_insert_with(Vec::new);
                            buf.push((path.to_string_lossy().into_owned(), size));
                            if buf.len() >= DETAIL_FLUSH_THRESHOLD {
                                let count = state.t_flush_counts.entry(uid).or_insert(0);
                                *count += 1;
                                buf.sort_by(|a, b| b.1.cmp(&a.1));
                                let fp = format!("{}/uid_{}_t{}_c{}.tsv",
                                    state.tmpdir, uid, state.thread_id, count);
                                if let Ok(f) = fs::File::create(&fp) {
                                    let mut w = BufWriter::new(f);
                                    for (p, s) in buf.iter() {
                                        let _ = writeln!(w, "{}\t{}", s, p);
                                    }
                                }
                                buf.clear();
                                if buf.capacity() > DETAIL_FLUSH_THRESHOLD * 2 {
                                    buf.shrink_to_fit();
                                }
                            }
                        }

                        // Progress tracking
                        state.prog_files.fetch_add(1, Ordering::Relaxed);
                        state.prog_size.fetch_add(size, Ordering::Relaxed);
                    }

                    WalkState::Continue
                })
            });

        d_clone.store(true, Ordering::SeqCst);
    });

    // --- Main thread: progress display + KeyboardInterrupt polling ---
    let start_time = Instant::now();
    let mut last_report = start_time;
    let mut last_files: u64 = 0;

    while !done.load(Ordering::SeqCst) {
        py.check_signals()?;
        thread::sleep(Duration::from_millis(200));

        let now = Instant::now();
        let elapsed_secs = now.duration_since(last_report).as_secs();
        if elapsed_secs >= 10 {
            let total_files = prog_files.load(Ordering::Relaxed);
            let total_dirs  = prog_dirs.load(Ordering::Relaxed);
            let total_size  = prog_size.load(Ordering::Relaxed);
            let total_elapsed = now.duration_since(start_time).as_secs();
            let rate = total_files.saturating_sub(last_files) as f64 / elapsed_secs as f64;
            let mem_mb = get_rss_mb();
            println!(
                "[{:02}:{:02}:{:02}] Files: {} | Dirs: {} | Size: {} | Rate: {} files/s | Mem: {:.1} MB",
                total_elapsed / 3600, (total_elapsed % 3600) / 60, total_elapsed % 60,
                format_num(total_files), format_num(total_dirs),
                format_size(total_size), format_rate(rate), mem_mb
            );
            last_report = now;
            last_files = total_files;
        }
    }
    // no trailing newline needed — println already adds one

    // --- Build Python return dict ---
    let g = global_stats.lock().unwrap();

    let result = PyDict::new(py);
    result.set_item("total_files",  g.total_files)?;
    result.set_item("total_dirs",   g.total_dirs)?;
    result.set_item("total_inodes", g.total_inodes)?;
    result.set_item("total_size",   g.total_size)?;
    result.set_item("detail_tmpdir", &tmpdir_str)?;
    // dir_sizes are streamed to dirs_t*.tsv files inside detail_tmpdir
    // Python reads them directly — no huge PyList copy needed
    result.set_item("dir_tmpdir", &tmpdir_str)?;

    let py_uid = PyDict::new(py);
    for (uid, size) in &g.uid_sizes { py_uid.set_item(uid, size)?; }
    result.set_item("uid_sizes", py_uid)?;

    let py_uid_files = PyDict::new(py);
    for (uid, files) in &g.uid_files { py_uid_files.set_item(uid, files)?; }
    result.set_item("uid_files", py_uid_files)?;

    // permission_issues as list of dicts (path, type, error, uid)
    let py_perms = PyList::empty(py);
    for (path, kind, err, uid_opt) in &g.permission_issues {
        let d = PyDict::new(py);
        d.set_item("path", path)?;
        d.set_item("type", kind)?;
        d.set_item("error", err)?;
        if let Some(uid) = uid_opt {
            d.set_item("uid", uid)?;
        } else {
            d.set_item("uid", py.None())?;
        }
        py_perms.append(d)?;
    }
    result.set_item("permission_issues", py_perms)?;

    Ok(result.into())
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2: K-way merge-write — Rust replacement for Python heapq.merge pass
// ─────────────────────────────────────────────────────────────────────────────
//
// Equivalent to Python's _stream_write_file_report() but:
//   • 1 pass only (Python has 2)  — reads each line exactly once
//   • BufReader per chunk         — amortises syscalls
//   • BinaryHeap K-way merge      — O(K log K) in pure Rust, no GIL
//   • Streaming BufWriter         — never holds full list in RAM
//   • UTF-8 sanitise              — replaces bad bytes with U+FFFD (same as Python)
//
// Python call signature:
//   (total_files, total_used) = fast_scanner.merge_write_user_report(
//       tmpdir, uid, username, output_path, timestamp
//   )

use std::io::{BufRead, BufReader};
/// Sanitise a raw byte string (possibly lossy-decoded) so the result is
/// valid UTF-8 JSON: replace any surrogate or invalid code points with U+FFFD.
pub(crate) fn sanitise_path(raw: &str) -> String {
    raw.chars()
        .map(|c| if c == '\u{FFFD}' || (c.is_control() && c != '\t') { '\u{FFFD}' } else { c })
        .collect()
}

/// Escape a string for JSON: wrap in quotes, escape backslash, double-quote,
/// and control characters.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"'  => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[pyfunction]
fn merge_write_user_report(
    tmpdir:      String,
    uids:        Vec<u32>,
    username:    String,
    output_path: String,
    timestamp:   i64,
) -> PyResult<(u64, u64)> {
    // 1. Glob chunk files sorted for all uids in a single readdir pass.
    let chunk_files = glob_module_rust_many(&tmpdir, &uids)?;

    if chunk_files.is_empty() {
        // Write empty report — same structure Python would write
        _write_empty_report(&output_path, &username, timestamp)?;
        return Ok((0, 0));
    }

    // 4. Create output directory
    if let Some(parent) = std::path::Path::new(&output_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", parent.display(), e)))?;
    }

    // 5. Two-phase: first pass collects totals, second streams JSON
    //    To avoid re-reading, we do ONE pass: buffer (size, path) into a temp
    //    sorted vec — but that needs RAM. Instead we do what Python does:
    //    a fast pre-scan pass then re-open chunks for the write pass.
    //
    //    For true single-pass we would need to know total_files/total_used before
    //    writing the header. We replicate Python's 2-pass approach but with Rust
    //    speed on each pass.

    // Pass 1: totals (fast — just reads the size column)
    let (total_files, total_used) = {
        let mut tf: u64 = 0;
        let mut tu: u64 = 0;

        for path in &chunk_files {
            let f = fs::File::open(path)
                .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path, e)))?;
            for line in BufReader::new(f).lines() {
                if let Ok(l) = line {
                    if let Some((sz, _)) = parse_tsv_line(&l) {
                        tf += 1;
                        tu += sz;
                    }
                }
            }
        }
        (tf, tu)
    };

    // Pass 2: K-way merge write
    let out_file = fs::File::create(&output_path)
        .map_err(|e| PyRuntimeError::new_err(format!("create {}: {}", output_path, e)))?;
    let mut w = BufWriter::new(out_file);

    // Write header
    writeln!(w, "{{\"_meta\":{{\"date\":{},\"user\":{},\"total_files\":{},\"total_used\":{}}}}}",
        timestamp, json_escape(&sanitise_path(&username)), total_files, total_used)
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    // K-way merge from re-opened readers
    let mut readers3: Vec<std::io::Lines<BufReader<fs::File>>> = Vec::new();
    for path in &chunk_files {
        let f = fs::File::open(path)
            .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path, e)))?;
        readers3.push(BufReader::new(f).lines());
    }

    let mut heap3: std::collections::BinaryHeap<(u64, String, usize)> =
        std::collections::BinaryHeap::new();
    for (idx, reader) in readers3.iter_mut().enumerate() {
        if let Some(Ok(line)) = reader.next() {
            if let Some((size, path)) = parse_tsv_line(&line) {
                heap3.push((size, path, idx));
            }
        }
    }

    while let Some((size, raw_path, idx)) = heap3.pop() {
        let safe = sanitise_path(&raw_path);
        let xt = std::path::Path::new(&safe).extension().and_then(|s| s.to_str()).unwrap_or("");
        writeln!(w, "{{\"path\":{},\"size\":{},\"xt\":{}}}", json_escape(&safe), size, json_escape(xt))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Pull next line from same chunk
        if let Some(Ok(line)) = readers3[idx].next() {
            if let Some((sz2, p2)) = parse_tsv_line(&line) {
                heap3.push((sz2, p2, idx));
            }
        }
    }

    w.flush().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    // Output omitted to keep terminal clean during parallel processing
    // let mem_mb = get_rss_mb();
    // eprintln!("  [Phase 2] {}: {} files, {} bytes written | Mem: {:.1} MB",
    //     username, total_files, total_used, mem_mb);

    Ok((total_files, total_used))
}


/// Insert a batch of staged rows into TEMP stage_files.
/// Called within an explicit transaction for bulk performance.
fn _insert_stage_batch(
    conn: &Connection,
    batch: &[(String, String, u64, String)],
    dir_cache: &mut HashMap<String, i64>,
    basename_cache: &mut HashMap<String, i64>,
    xt_cache: &mut HashMap<String, i64>,
) -> Result<(), rusqlite::Error> {
    if batch.is_empty() { return Ok(()); }

    let mut missing_dirs: HashSet<&str> = HashSet::new();
    let mut missing_basenames: HashSet<&str> = HashSet::new();
    let mut missing_xts: HashSet<&str> = HashSet::new();

    for (dir_path, basename, _, xt) in batch.iter() {
        if !dir_cache.contains_key(dir_path.as_str()) {
            missing_dirs.insert(dir_path.as_str());
        }
        if !basename_cache.contains_key(basename.as_str()) {
            missing_basenames.insert(basename.as_str());
        }
        if !xt.is_empty() && !xt_cache.contains_key(xt.as_str()) {
            missing_xts.insert(xt.as_str());
        }
    }

    if !missing_dirs.is_empty() {
        let mut insert_dir = conn.prepare_cached(
            "INSERT OR IGNORE INTO dirs_index(path) VALUES (?1)",
        )?;
        for path in missing_dirs.iter() {
            insert_dir.execute(params![path])?;
        }

        let mut select_dir = conn.prepare_cached(
            "SELECT id FROM dirs_index WHERE path = ?1",
        )?;
        for path in missing_dirs.iter() {
            let id: i64 = select_dir.query_row(params![path], |r| r.get(0))?;
            dir_cache.insert((*path).to_string(), id);
        }
    }

    if !missing_basenames.is_empty() {
        let mut insert_basename = conn.prepare_cached(
            "INSERT OR IGNORE INTO basename_index(basename) VALUES (?1)",
        )?;
        for basename in missing_basenames.iter() {
            insert_basename.execute(params![basename])?;
        }

        let mut select_basename = conn.prepare_cached(
            "SELECT id FROM basename_index WHERE basename = ?1",
        )?;
        for basename in missing_basenames.iter() {
            let id: i64 = select_basename.query_row(params![basename], |r| r.get(0))?;
            basename_cache.insert((*basename).to_string(), id);
        }
    }

    if !missing_xts.is_empty() {
        let mut insert_xt = conn.prepare_cached(
            "INSERT OR IGNORE INTO xt_index(xt) VALUES (?1)",
        )?;
        for xt in missing_xts.iter() {
            insert_xt.execute(params![xt])?;
        }

        let mut select_xt = conn.prepare_cached(
            "SELECT id FROM xt_index WHERE xt = ?1",
        )?;
        for xt in missing_xts.iter() {
            let id: i64 = select_xt.query_row(params![xt], |r| r.get(0))?;
            xt_cache.insert((*xt).to_string(), id);
        }
    }

    let mut stmt = conn.prepare_cached(
        "INSERT OR REPLACE INTO stage_files (dir_id, basename_id, size, xt_id) VALUES (?1, ?2, ?3, ?4)",
    )?;
    for (dir_path, basename, size, xt) in batch.iter() {
        let dir_id = *dir_cache.get(dir_path.as_str()).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let basename_id = *basename_cache.get(basename.as_str()).ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let xt_id = if xt.is_empty() {
            None
        } else {
            Some(*xt_cache.get(xt.as_str()).ok_or(rusqlite::Error::QueryReturnedNoRows)?)
        };
        stmt.execute(params![dir_id, basename_id, *size as i64, xt_id])?;
    }
    Ok(())
}

fn _intern_dir_id(
    conn: &Connection,
    dir_cache: &mut HashMap<String, i64>,
    dir_path: &str,
) -> Result<i64, rusqlite::Error> {
    if let Some(&id) = dir_cache.get(dir_path) {
        return Ok(id);
    }
    conn.execute("INSERT OR IGNORE INTO dirs_index(path) VALUES (?1)", params![dir_path])?;
    let id: i64 = conn.query_row(
        "SELECT id FROM dirs_index WHERE path = ?1",
        params![dir_path],
        |r| r.get(0),
    )?;
    dir_cache.insert(dir_path.to_string(), id);
    Ok(id)
}

fn _intern_basename_id(
    conn: &Connection,
    basename_cache: &mut HashMap<String, i64>,
    basename: &str,
) -> Result<i64, rusqlite::Error> {
    if let Some(&id) = basename_cache.get(basename) {
        return Ok(id);
    }
    conn.execute(
        "INSERT OR IGNORE INTO basename_index(basename) VALUES (?1)",
        params![basename],
    )?;
    let id: i64 = conn.query_row(
        "SELECT id FROM basename_index WHERE basename = ?1",
        params![basename],
        |r| r.get(0),
    )?;
    basename_cache.insert(basename.to_string(), id);
    Ok(id)
}

fn _intern_xt_id(
    conn: &Connection,
    xt_cache: &mut HashMap<String, i64>,
    xt: &str,
) -> Result<i64, rusqlite::Error> {
    if let Some(&id) = xt_cache.get(xt) {
        return Ok(id);
    }
    conn.execute("INSERT OR IGNORE INTO xt_index(xt) VALUES (?1)", params![xt])?;
    let id: i64 = conn.query_row(
        "SELECT id FROM xt_index WHERE xt = ?1",
        params![xt],
        |r| r.get(0),
    )?;
    xt_cache.insert(xt.to_string(), id);
    Ok(id)
}

pub(crate) struct MergeReaderState {
    reader: BufReader<fs::File>,
    line_buf: String,
}

impl MergeReaderState {
    pub(crate) fn new(file: fs::File) -> Self {
        Self {
            reader: BufReader::new(file),
            line_buf: String::with_capacity(256),
        }
    }

    pub(crate) fn next_entry(&mut self) -> Option<(u64, String)> {
        loop {
            self.line_buf.clear();
            let n = self.reader.read_line(&mut self.line_buf).ok()?;
            if n == 0 {
                return None;
            }
            if self.line_buf.ends_with('\n') {
                self.line_buf.pop();
                if self.line_buf.ends_with('\r') {
                    self.line_buf.pop();
                }
            }
            if self.line_buf.is_empty() {
                continue;
            }
            if let Some((size, path)) = parse_tsv_line(&self.line_buf) {
                return Some((size, path));
            }
        }
    }
}

#[pyfunction]
fn merge_write_user_report_db(
    tmpdir:      String,
    uids:        Vec<u32>,
    username:    String,
    output_path: String,
    timestamp:   i64,
    batch_size:  Option<usize>,
) -> PyResult<(u64, u64)> {
    let bs = batch_size.unwrap_or(50_000).max(1);
    let profile_enabled = std::env::var("CHECKDISK_RUST_PROFILE").ok().as_deref() == Some("1");
    let t_all = Instant::now();

    let chunk_files = glob_module_rust_many(&tmpdir, &uids)?;

    if let Some(parent) = std::path::Path::new(&output_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", parent.display(), e)))?;
    }

    // ── Detect fresh vs incremental ──
    let is_fresh = !std::path::Path::new(&output_path).exists();

    let conn = Connection::open(&output_path)
        .map_err(|e| PyRuntimeError::new_err(format!("open sqlite {}: {}", output_path, e)))?;

    let t_pragmas = Instant::now();
    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA cache_size = -100000;
         PRAGMA temp_store = MEMORY;
         PRAGMA mmap_size = 268435456;",
    ).map_err(|e| PyRuntimeError::new_err(format!("sqlite pragmas: {}", e)))?;
    let dt_pragmas = t_pragmas.elapsed();

    // ── Schema probe: detect legacy schema for migration ──
    let t_schema_probe = Instant::now();
    let files_data_cols: HashSet<String> = conn
        .prepare("PRAGMA table_info(files_data)")
        .and_then(|mut stmt| {
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            let mut cols = HashSet::new();
            for col in rows.flatten() {
                cols.insert(col);
            }
            Ok(cols)
        })
        .unwrap_or_default();
    let dt_schema_probe = t_schema_probe.elapsed();

    let is_legacy_schema = files_data_cols.contains("basename") || files_data_cols.contains("xt");
    if is_legacy_schema {
        conn.execute_batch(
            "DROP VIEW IF EXISTS files;
             DROP INDEX IF EXISTS idx_files_data_xt_size;
             DROP TABLE IF EXISTS files_data;
             DROP TABLE IF EXISTS basename_index;
             DROP TABLE IF EXISTS xt_index;",
        ).map_err(|e| PyRuntimeError::new_err(format!("sqlite legacy migration: {}", e)))?;
    }

    // Treat as fresh if the DB existed but had legacy schema (tables were dropped)
    let is_fresh = is_fresh || is_legacy_schema;

    let t_setup = Instant::now();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS meta (date INTEGER, user TEXT, total_items INTEGER, total_used INTEGER);
         CREATE TABLE IF NOT EXISTS dirs_index (
             id   INTEGER PRIMARY KEY,
             path TEXT UNIQUE
         );
         CREATE TABLE IF NOT EXISTS basename_index (
             id       INTEGER PRIMARY KEY,
             basename TEXT UNIQUE
         );
         CREATE TABLE IF NOT EXISTS xt_index (
             id INTEGER PRIMARY KEY,
             xt TEXT UNIQUE
         );
         CREATE TABLE IF NOT EXISTS files_data (
             id          INTEGER PRIMARY KEY,
             dir_id      INTEGER NOT NULL,
             basename_id INTEGER NOT NULL,
             size        INTEGER NOT NULL,
             xt_id       INTEGER
         );
         CREATE UNIQUE INDEX IF NOT EXISTS uq_files_data_dir_base
             ON files_data(dir_id, basename_id);
         CREATE VIEW IF NOT EXISTS files AS
             SELECT f.id,
                    d.path || '/' || b.basename AS path,
                    f.size,
                    COALESCE(x.xt, '') AS xt
             FROM files_data f
             JOIN dirs_index d ON f.dir_id = d.id
             JOIN basename_index b ON f.basename_id = b.id
             LEFT JOIN xt_index x ON x.id = f.xt_id;",
    ).map_err(|e| PyRuntimeError::new_err(format!("sqlite setup: {}", e)))?;
    let dt_setup = t_setup.elapsed();

    // Check if DB has existing data (for non-fresh path)
    let is_fresh = is_fresh || conn.query_row(
        "SELECT count(*) FROM files_data", [], |r| r.get::<_, i64>(0)
    ).unwrap_or(0) == 0;

    if is_fresh {
        _merge_fresh_path(
            &conn, &chunk_files, bs, &username, timestamp,
            profile_enabled, t_all, dt_pragmas, dt_schema_probe, dt_setup,
        )
    } else {
        _merge_incremental_path(
            &conn, &chunk_files, bs, &username, timestamp,
            profile_enabled, t_all, dt_pragmas, dt_schema_probe, dt_setup,
        )
    }
}

/// Fresh DB path: Two-Pass Intern + Bulk Insert (no stage table, no UPSERT, no DELETE).
fn _merge_fresh_path(
    conn: &Connection,
    chunk_files: &[String],
    bs: usize,
    username: &str,
    timestamp: i64,
    profile_enabled: bool,
    t_all: Instant,
    dt_pragmas: Duration,
    dt_schema_probe: Duration,
    dt_setup: Duration,
) -> PyResult<(u64, u64)> {
    // ── Pass 1: Read all TSV chunks → in-memory HashMap interning ──
    let t_pass1 = Instant::now();

    let mut dir_intern: HashMap<String, i64> = HashMap::new();
    let mut basename_intern: HashMap<String, i64> = HashMap::new();
    let mut xt_intern: HashMap<String, i64> = HashMap::new();
    let mut dir_counter: i64 = 1;
    let mut basename_counter: i64 = 1;
    let mut xt_counter: i64 = 1;

    // Pre-insert empty-xt as id 0 (sentinel)
    xt_intern.insert(String::new(), 0);

    struct InternedRow {
        dir_id: i64,
        basename_id: i64,
        size: u64,
        xt_id: Option<i64>,
    }

    let mut rows: Vec<InternedRow> = Vec::with_capacity(bs);
    let mut total_files: u64 = 0;
    let mut total_used: u64 = 0;

    if !chunk_files.is_empty() {
        // Sequential read — no heap sort needed for fresh DB
        for path in chunk_files {
            let f = fs::File::open(path)
                .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path, e)))?;
            let mut reader = MergeReaderState::new(f);

            while let Some((size, raw_path)) = reader.next_entry() {
                let safe = sanitise_path(&raw_path);
                let (dir_str, basename, xt) = split_path_for_stage(&safe);

                // In-memory interning (HashMap O(1) lookup, ~10ns per entry)
                let dir_id = match dir_intern.get(dir_str) {
                    Some(&id) => id,
                    None => {
                        let id = dir_counter;
                        dir_counter += 1;
                        dir_intern.insert(dir_str.to_string(), id);
                        id
                    }
                };

                let basename_id = match basename_intern.get(basename) {
                    Some(&id) => id,
                    None => {
                        let id = basename_counter;
                        basename_counter += 1;
                        basename_intern.insert(basename.to_string(), id);
                        id
                    }
                };

                let xt_id = if xt.is_empty() {
                    None
                } else {
                    Some(match xt_intern.get(&xt) {
                        Some(&id) => id,
                        None => {
                            let id = xt_counter;
                            xt_counter += 1;
                            xt_intern.insert(xt.clone(), id);
                            id
                        }
                    })
                };

                rows.push(InternedRow { dir_id, basename_id, size, xt_id });
                total_files += 1;
                total_used += size;
            }
        }
    }

    let dt_pass1 = t_pass1.elapsed();

    // ── Pass 2: Bulk write to SQLite (single transaction, no UPSERT) ──
    let t_pass2 = Instant::now();

    // Drop indexes before bulk insert — rebuild after for much faster inserts
    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_files_data_xt_size;\
         DROP INDEX IF EXISTS uq_files_data_dir_base;"
    ).map_err(|e| PyRuntimeError::new_err(format!("drop index: {}", e)))?;

    conn.execute_batch("BEGIN IMMEDIATE;")
        .map_err(|e| PyRuntimeError::new_err(format!("begin: {}", e)))?;

    // 2a. Bulk INSERT dirs_index (multi-row batch for fewer SQLite calls)
    let t_dirs = Instant::now();
    {
        let mut batch_vals: Vec<(i64, &str)> = Vec::with_capacity(bs.min(dir_intern.len()));
        let mut stmt = conn.prepare_cached(
            "INSERT OR IGNORE INTO dirs_index(id, path) VALUES (?1, ?2)"
        ).map_err(|e| PyRuntimeError::new_err(format!("prepare dirs: {}", e)))?;
        for (path, &id) in &dir_intern {
            batch_vals.push((id, path.as_str()));
            if batch_vals.len() >= bs {
                for &(vid, vp) in &batch_vals {
                    stmt.execute(params![vid, vp])
                        .map_err(|e| PyRuntimeError::new_err(format!("insert dir: {}", e)))?;
                }
                batch_vals.clear();
            }
        }
        for &(vid, vp) in &batch_vals {
            stmt.execute(params![vid, vp])
                .map_err(|e| PyRuntimeError::new_err(format!("insert dir: {}", e)))?;
        }
    }
    let dt_dirs = t_dirs.elapsed();

    // 2b. Bulk INSERT basename_index (multi-row batch)
    let t_basenames = Instant::now();
    {
        let mut stmt = conn.prepare_cached(
            "INSERT OR IGNORE INTO basename_index(id, basename) VALUES (?1, ?2)"
        ).map_err(|e| PyRuntimeError::new_err(format!("prepare basenames: {}", e)))?;
        let mut batch_vals: Vec<(i64, &str)> = Vec::with_capacity(bs.min(basename_intern.len()));
        for (basename, &id) in &basename_intern {
            batch_vals.push((id, basename.as_str()));
            if batch_vals.len() >= bs {
                for &(vid, vb) in &batch_vals {
                    stmt.execute(params![vid, vb])
                        .map_err(|e| PyRuntimeError::new_err(format!("insert basename: {}", e)))?;
                }
                batch_vals.clear();
            }
        }
        for &(vid, vb) in &batch_vals {
            stmt.execute(params![vid, vb])
                .map_err(|e| PyRuntimeError::new_err(format!("insert basename: {}", e)))?;
        }
    }
    let dt_basenames = t_basenames.elapsed();

    // 2c. Bulk INSERT xt_index (skip sentinel 0, multi-row batch)
    let t_xts = Instant::now();
    {
        let mut stmt = conn.prepare_cached(
            "INSERT OR IGNORE INTO xt_index(id, xt) VALUES (?1, ?2)"
        ).map_err(|e| PyRuntimeError::new_err(format!("prepare xts: {}", e)))?;
        for (xt, &id) in &xt_intern {
            if !xt.is_empty() {
                stmt.execute(params![id, xt])
                    .map_err(|e| PyRuntimeError::new_err(format!("insert xt '{}': {}", xt, e)))?;
            }
        }
    }
    let dt_xts = t_xts.elapsed();

    // 2d. Bulk INSERT files_data (chunked multi-row batch — fewer SQLite round-trips)
    let t_files = Instant::now();
    {
        let chunk_size = bs.min(10_000);
        let mut stmt = conn.prepare_cached(
            "INSERT INTO files_data (dir_id, basename_id, size, xt_id) VALUES (?1, ?2, ?3, ?4)"
        ).map_err(|e| PyRuntimeError::new_err(format!("prepare files_data: {}", e)))?;
        for chunk in rows.chunks(chunk_size) {
            for row in chunk {
                stmt.execute(params![row.dir_id, row.basename_id, row.size as i64, row.xt_id])
                    .map_err(|e| PyRuntimeError::new_err(format!("insert files_data: {}", e)))?;
            }
        }
    }
    let dt_files = t_files.elapsed();

    // 2e. Meta
    let t_meta = Instant::now();
    conn.execute("DELETE FROM meta", [])
        .map_err(|e| PyRuntimeError::new_err(format!("meta reset: {}", e)))?;
    conn.execute(
        "INSERT INTO meta(date, user, total_items, total_used) VALUES (?1, ?2, ?3, ?4)",
        params![timestamp, sanitise_path(username), total_files as i64, total_used as i64],
    ).map_err(|e| PyRuntimeError::new_err(format!("meta insert: {}", e)))?;
    let dt_meta = t_meta.elapsed();

    // 2f. Commit + rebuild index
    let t_commit = Instant::now();
    conn.execute_batch(
        "COMMIT;\
         CREATE UNIQUE INDEX IF NOT EXISTS uq_files_data_dir_base ON files_data(dir_id, basename_id);\
         CREATE INDEX IF NOT EXISTS idx_files_data_xt_size ON files_data(xt_id, size DESC);"
    ).map_err(|e| PyRuntimeError::new_err(format!("commit: {}", e)))?;
    let dt_commit = t_commit.elapsed();

    let dt_pass2 = t_pass2.elapsed();

    if profile_enabled {
        eprintln!(
            "[RUST][merge_fresh] user={} files={} used={} batch={} chunks={} \
             pragmas={:.3}s schema_probe={:.3}s setup={:.3}s \
             pass1_intern={:.3}s pass2_total={:.3}s \
             (dirs={:.3}s[{}] basenames={:.3}s[{}] xts={:.3}s[{}] files={:.3}s meta={:.3}s commit={:.3}s) \
             total={:.3}s",
            username, total_files, total_used, bs, chunk_files.len(),
            dt_pragmas.as_secs_f64(),
            dt_schema_probe.as_secs_f64(),
            dt_setup.as_secs_f64(),
            dt_pass1.as_secs_f64(),
            dt_pass2.as_secs_f64(),
            dt_dirs.as_secs_f64(), dir_intern.len(),
            dt_basenames.as_secs_f64(), basename_intern.len(),
            dt_xts.as_secs_f64(), xt_intern.len(),
            dt_files.as_secs_f64(),
            dt_meta.as_secs_f64(),
            dt_commit.as_secs_f64(),
            t_all.elapsed().as_secs_f64(),
        );
    }

    // Drop rows Vec to free memory before returning
    drop(rows);
    drop(dir_intern);
    drop(basename_intern);
    drop(xt_intern);

    Ok((total_files, total_used))
}

/// Incremental path: uses stage table + UPSERT + DELETE stale (original algorithm).
fn _merge_incremental_path(
    conn: &Connection,
    chunk_files: &[String],
    bs: usize,
    username: &str,
    timestamp: i64,
    profile_enabled: bool,
    t_all: Instant,
    dt_pragmas: Duration,
    dt_schema_probe: Duration,
    dt_setup: Duration,
) -> PyResult<(u64, u64)> {
    let mut total_files: u64 = 0;
    let mut total_used:  u64 = 0;
    let mut stage_batch: Vec<(String, String, u64, String)> = Vec::with_capacity(bs);
    let mut dir_cache: HashMap<String, i64> = HashMap::new();
    let mut basename_cache: HashMap<String, i64> = HashMap::new();
    let mut xt_cache: HashMap<String, i64> = HashMap::new();

    let t_stage_setup = Instant::now();
    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_files_data_xt_size;
         BEGIN IMMEDIATE;
         CREATE TEMP TABLE stage_files (
             dir_id      INTEGER NOT NULL,
             basename_id INTEGER NOT NULL,
             size        INTEGER NOT NULL,
             xt_id       INTEGER,
             PRIMARY KEY(dir_id, basename_id)
         );",
    ).map_err(|e| PyRuntimeError::new_err(format!("stage setup: {}", e)))?;
    let dt_stage_setup = t_stage_setup.elapsed();

    let t_stream = Instant::now();
    if !chunk_files.is_empty() {
        let mut readers: Vec<MergeReaderState> = Vec::new();
        for path in chunk_files {
            let f = fs::File::open(path)
                .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path, e)))?;
            readers.push(MergeReaderState::new(f));
        }

        let mut heap: std::collections::BinaryHeap<(u64, String, usize)> =
            std::collections::BinaryHeap::new();
        for (idx, reader) in readers.iter_mut().enumerate() {
            if let Some((size, path)) = reader.next_entry() {
                heap.push((size, path, idx));
            }
        }

        while let Some((size, raw_path, idx)) = heap.pop() {
            let safe = sanitise_path(&raw_path);
            let (dir_str, basename, xt) = split_path_for_stage(&safe);

            stage_batch.push((
                dir_str.to_string(),
                basename.to_string(),
                size,
                xt,
            ));
            total_files += 1;
            total_used += size;

            if stage_batch.len() >= bs {
                _insert_stage_batch(&conn, &stage_batch, &mut dir_cache, &mut basename_cache, &mut xt_cache)
                    .map_err(|e| PyRuntimeError::new_err(format!("stage batch insert: {}", e)))?;
                stage_batch.clear();
            }

            if let Some((sz2, p2)) = readers[idx].next_entry() {
                heap.push((sz2, p2, idx));
            }
        }
    }

    let dt_stream = t_stream.elapsed();

    if !stage_batch.is_empty() {
        _insert_stage_batch(&conn, &stage_batch, &mut dir_cache, &mut basename_cache, &mut xt_cache)
            .map_err(|e| PyRuntimeError::new_err(format!("final stage insert: {}", e)))?;
    }

    let t_insert_new = Instant::now();
    conn.execute(
        "INSERT INTO files_data (dir_id, basename_id, size, xt_id)
         SELECT sf.dir_id, sf.basename_id, sf.size, sf.xt_id
         FROM stage_files sf
         WHERE 1
         ON CONFLICT(dir_id, basename_id) DO UPDATE SET
             size = excluded.size,
             xt_id = excluded.xt_id
         WHERE files_data.size != excluded.size
            OR IFNULL(files_data.xt_id, -1) != IFNULL(excluded.xt_id, -1)",
        [],
    ).map_err(|e| PyRuntimeError::new_err(format!("upsert files_data: {}", e)))?;
    let dt_insert_new = t_insert_new.elapsed();

    let t_delete_stale = Instant::now();
    conn.execute(
        "DELETE FROM files_data
         WHERE NOT EXISTS (
             SELECT 1
             FROM stage_files sf
             WHERE sf.dir_id = files_data.dir_id
               AND sf.basename_id = files_data.basename_id
         )",
        [],
    ).map_err(|e| PyRuntimeError::new_err(format!("delete stale files_data: {}", e)))?;
    let dt_delete_stale = t_delete_stale.elapsed();

    let t_meta = Instant::now();
    conn.execute("DELETE FROM meta", [])
        .map_err(|e| PyRuntimeError::new_err(format!("meta reset: {}", e)))?;
    conn.execute(
        "INSERT INTO meta(date, user, total_items, total_used) VALUES (?1, ?2, ?3, ?4)",
        params![timestamp, sanitise_path(username), total_files as i64, total_used as i64],
    ).map_err(|e| PyRuntimeError::new_err(format!("meta insert: {}", e)))?;
    let dt_meta = t_meta.elapsed();

    let t_commit = Instant::now();
    conn.execute_batch(
        "DROP TABLE IF EXISTS stage_files;
         COMMIT;
         CREATE INDEX IF NOT EXISTS idx_files_data_xt_size ON files_data(xt_id, size DESC);"
    ).map_err(|e| PyRuntimeError::new_err(format!("stage commit: {}", e)))?;
    let dt_commit = t_commit.elapsed();

    if profile_enabled {
        eprintln!(
            "[RUST][merge_incremental] user={} files={} used={} batch={} chunks={} \
             pragmas={:.3}s schema_probe={:.3}s setup={:.3}s stage_setup={:.3}s \
             stream={:.3}s insert_new={:.3}s delete_stale={:.3}s \
             meta={:.3}s commit={:.3}s total={:.3}s",
            username, total_files, total_used, bs, chunk_files.len(),
            dt_pragmas.as_secs_f64(),
            dt_schema_probe.as_secs_f64(),
            dt_setup.as_secs_f64(),
            dt_stage_setup.as_secs_f64(),
            dt_stream.as_secs_f64(),
            dt_insert_new.as_secs_f64(),
            dt_delete_stale.as_secs_f64(),
            dt_meta.as_secs_f64(),
            dt_commit.as_secs_f64(),
            t_all.elapsed().as_secs_f64(),
        );
    }

    Ok((total_files, total_used))
}



pub(crate) fn parse_tsv_line(line: &str) -> Option<(u64, String)> {
    let tab = line.find('\t')?;
    let size: u64 = line[..tab].trim().parse().ok()?;
    let path = line[tab + 1..].to_string();
    Some((size, path))
}

pub(crate) fn glob_module_rust_many(tmpdir: &str, uids: &[u32]) -> PyResult<Vec<String>> {
    if uids.is_empty() {
        return Ok(Vec::new());
    }

    let wanted: HashSet<u32> = uids.iter().copied().collect();
    let mut files = Vec::new();
    let dir = fs::read_dir(tmpdir)
        .map_err(|e| PyRuntimeError::new_err(format!("readdir {}: {}", tmpdir, e)))?;

    for entry in dir.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("uid_") || !name.ends_with(".tsv") {
            continue;
        }

        // Expected: uid_{uid}_t{thread}_c{chunk}.tsv
        let rest = &name[4..];
        let Some(pivot) = rest.find("_t") else { continue };
        if pivot == 0 {
            continue;
        }
        let uid_raw = &rest[..pivot];
        let Ok(uid) = uid_raw.parse::<u32>() else { continue };
        if wanted.contains(&uid) {
            files.push(entry.path().to_string_lossy().to_string());
        }
    }

    files.sort();
    Ok(files)
}

fn _write_empty_report(output_path: &str, username: &str, timestamp: i64) -> PyResult<()> {
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }
    let mut w = BufWriter::new(
        fs::File::create(output_path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
    );
    writeln!(w, "{{\"_meta\":{{\"date\":{},\"user\":{},\"total_files\":0,\"total_used\":0}}}}",
        timestamp, json_escape(username))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Phase 3 Full Rust Pipeline: TreeMap hierarchy → parallel JSON/compress → SQLite
// ═══════════════════════════════════════════════════════════════════════════════

/// Intern a path string: return its integer ID, creating one if necessary.
fn tm_intern(
    path: &str,
    id_to_path: &mut Vec<String>,
    path_to_id: &mut HashMap<String, usize>,
) -> usize {
    if let Some(&id) = path_to_id.get(path) {
        return id;
    }
    let id = id_to_path.len();
    id_to_path.push(path.to_string());
    path_to_id.insert(path.to_string(), id);
    id
}

/// Count extra '/' separators in `path` relative to `root` (= depth level).
fn tm_depth(path: &str, root: &str) -> usize {
    let suffix = if path.len() >= root.len() { &path[root.len()..] } else { return 0 };
    suffix.chars().filter(|&c| c == '/').count()
}

/// Return the ancestor of `path` that is exactly `level` levels below `root`.
fn tm_clamp(path: &str, root: &str, level: usize) -> String {
    let suffix = path[root.len()..].trim_start_matches('/');
    let parts: Vec<&str> = suffix.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() <= level {
        return path.to_string();
    }
    format!("{}/{}", root.trim_end_matches('/'), parts[..level].join("/"))
}

/// Parent directory of `path` (pure string, no I/O).
fn tm_parent(path: &str) -> &str {
    match path.rfind('/') {
        Some(0) => "/",
        Some(pos) => &path[..pos],
        None => path,
    }
}

/// Filename component of `path`.
#[inline]
fn tm_basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(pos) => &path[pos + 1..],
        None => path,
    }
}

#[derive(serde::Serialize)]
struct ShardDirectoryItem {
    id: usize,
    name: String,
    size: i64,
    owner: String,
    children_count: usize,
}

#[derive(serde::Serialize)]
struct ShardFileGroupItem {
    name: String,
    size: i64,
    #[serde(rename = "type")]
    item_type: &'static str,
    owner: String,
    children_count: usize,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum ShardItem {
    Directory(ShardDirectoryItem),
    FileGroup(ShardFileGroupItem),
}

/// Full Phase 3 pipeline implemented entirely in Rust.
///
/// Accepts raw scan data from Python and:
///   1. Builds the integer-ID directory hierarchy (single-threaded, O(n))
///   2. Computes bottom-up recursive sizes (single-threaded, O(n))
///   3. Generates shard JSON + zlib compress in **parallel** (Rayon, all cores)
///   4. Writes to SQLite with a single transaction (minimum fsync overhead)
///
/// The GIL is released for the **entire** computation after Python inputs are
/// converted to Rust types, so Python threads are free to run concurrently.
///
/// # Arguments
/// * `root_dir`       – Absolute path to the scanned root directory
/// * `dir_sizes`      – `{path: {uid_str: used_bytes}}` from Phase 1
/// * `dir_owner_map`  – `{path: username}` from Phase 1
/// * `json_path`       – Output path for root JSON (tree_map_report.json)
/// * `db_path`         – Output SQLite file path (tree_map_data.db)
/// * `max_level`       – Maximum tree depth (deeper paths are clamped)
/// * `min_size_bytes`  – Children smaller than this are omitted from shards
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(crate) fn generate_treemap_sqlite(
    py: Python<'_>,
    root_dir: String,
    dir_sizes: HashMap<String, HashMap<String, i64>>,
    dir_owner_map: HashMap<String, String>,
    json_path: String,   // tree_map_report.json  (root index)
    db_path: String,     // tree_map_data.db     (shard store)
    max_level: usize,
    min_size_bytes: i64,
) -> PyResult<u64> {
    // Convert Python objects → Rust types (GIL held, fast for typical dir counts).
    // After this point the GIL is fully released.
    py.allow_threads(move || -> Result<u64, String> {
        // Guard: "/".trim_end_matches('/') == "" in Rust → keep "/" for fs-root.
        let root = {
            let t = root_dir.trim_end_matches('/');
            if t.is_empty() { "/".to_string() } else { t.to_string() }
        };

        // ── 3.1a: String interning + direct sizes + ancestor traces ──────────
        let mut id_to_path: Vec<String> = Vec::new();
        let mut path_to_id: HashMap<String, usize> = HashMap::new();
        let mut direct_sizes: HashMap<usize, i64> = HashMap::new();
        let mut all_dirs: std::collections::HashSet<usize> =
            std::collections::HashSet::new();

        let root_id = tm_intern(&root, &mut id_to_path, &mut path_to_id);
        all_dirs.insert(root_id);

        for (dpath, uid_sizes) in &dir_sizes {
            let total: i64 = uid_sizes.values().sum();
            if total <= 0 {
                continue;
            }

            // Clamp path that exceeds max_level to its ancestor at max_level.
            let depth = tm_depth(dpath, &root);
            let target = if depth > max_level {
                tm_clamp(dpath, &root, max_level)
            } else {
                dpath.clone()
            };

            let tid = tm_intern(&target, &mut id_to_path, &mut path_to_id);
            *direct_sizes.entry(tid).or_insert(0) += total;

            // Walk up to root, interning every ancestor dir.
            let mut curr = target;
            loop {
                let cid = tm_intern(&curr, &mut id_to_path, &mut path_to_id);
                all_dirs.insert(cid);
                if curr == root {
                    break;
                }
                let parent = tm_parent(&curr).to_string();
                if parent == curr {
                    break; // shouldn't happen, guards against infinite loop
                }
                curr = parent;
            }
        }

        // ── 3.1b: Parent → children map ──────────────────────────────────────
        let mut parent_to_children: HashMap<usize, Vec<usize>> = HashMap::new();
        for &rid in &all_dirs {
            if rid == root_id {
                continue;
            }
            let path = &id_to_path[rid];
            let parent_str = tm_parent(path);
            let pid = path_to_id.get(parent_str).copied().unwrap_or(root_id);
            parent_to_children.entry(pid).or_default().push(rid);
        }

        // ── 3.1c: Bottom-up recursive sizes (deepest dirs first) ─────────────
        let mut sorted_ids: Vec<usize> = all_dirs.iter().copied().collect();
        sorted_ids.sort_by(|&a, &b| id_to_path[b].len().cmp(&id_to_path[a].len()));

        // Save root's direct contribution before consuming direct_sizes.
        let root_direct_size = direct_sizes.get(&root_id).copied().unwrap_or(0);

        let mut recursive_sizes: HashMap<usize, i64> = direct_sizes;
        for &rid in &sorted_ids {
            if rid == root_id {
                continue;
            }
            let path = &id_to_path[rid];
            let parent_str = tm_parent(path);
            if let Some(&pid) = path_to_id.get(parent_str) {
                let s = recursive_sizes.get(&rid).copied().unwrap_or(0);
                *recursive_sizes.entry(pid).or_insert(0) += s;
            }
        }

        // ── 3.1d: Assign stable shard IDs (alphabetical by path) ─────────────
        let mut shard_order: Vec<usize> = all_dirs.iter().copied().collect();
        shard_order.sort_by_key(|&id| &id_to_path[id]);

        let path_to_shard: HashMap<usize, usize> = shard_order
            .iter()
            .enumerate()
            .map(|(i, &rid)| (rid, i))
            .collect();

        // ── 3.1e: Root node JSON ──────────────────────────────────────────────
        {
            let empty = Vec::new();
            let mut root_children: Vec<serde_json::Value> = parent_to_children
                .get(&root_id)
                .unwrap_or(&empty)
                .iter()
                .filter_map(|&cid| {
                    let size = recursive_sizes.get(&cid).copied().unwrap_or(0);
                    if size <= 0 { return None; }
                    let cp = &id_to_path[cid];
                    Some(serde_json::json!({
                        "name":         tm_basename(cp),
                        "path":         cp,
                        "value":        size,
                        "type":         "directory",
                        "owner":        dir_owner_map.get(cp.as_str())
                                            .map(|s| s.as_str())
                                            .unwrap_or("unknown"),
                        "shard_id":     path_to_shard[&cid].to_string(),
                        "has_children": parent_to_children.contains_key(&cid) && max_level > 1,
                    }))
                })
                .collect();

            // Add "file group" for direct files / clamped dirs at root level.
            if root_direct_size > 0 {
                let rb = tm_basename(&root);
                let label = if rb.is_empty() { root.clone() } else { rb.to_string() };
                root_children.push(serde_json::json!({
                    "name":         format!("[Files in {}]", label),
                    "path":         format!("{}/{}", root.trim_end_matches('/'), "__files__"),
                    "value":        root_direct_size,
                    "type":         "file_group",
                    "owner":        dir_owner_map.get(root.as_str()).map(|s| s.as_str()).unwrap_or("unknown"),
                    "has_children": false,
                }));
            }

            root_children.sort_by(|a, b| {
                let va = a["value"].as_i64().unwrap_or(0);
                let vb = b["value"].as_i64().unwrap_or(0);
                vb.cmp(&va)
            });

            let root_total = recursive_sizes.get(&root_id).copied().unwrap_or(0);
            let rb = tm_basename(&root);
            let root_name = if rb.is_empty() { &root as &str } else { rb };
            let has_children = !root_children.is_empty();

            let root_node = serde_json::json!({
                "name":         root_name,
                "path":         &root,
                "value":        root_total,
                "type":         "directory",
                "owner":        dir_owner_map.get(root.as_str()).map(|s| s.as_str()).unwrap_or("unknown"),
                "shard_id":     path_to_shard[&root_id].to_string(),
                "has_children": has_children,
                "children":     root_children,
            });

            let json_str = serde_json::to_string(&root_node).expect("root json");
            std::fs::write(&json_path, json_str.as_bytes())
                .map_err(|e| format!("write root json '{}': {}", json_path, e))?;
        }

        // ── 3.2: Parallel shard generation: JSON → zlib (Rayon) ──────────────
        // All maps are read-only here → safe for parallel access.
        // ── 3.3: Single-transaction SQLite write ──────────────────────────────
        let mut conn = Connection::open(&db_path)
            .map_err(|e| format!("open db '{}': {}", db_path, e))?;

        conn.execute_batch(
            "PRAGMA page_size   = 8192; \
             PRAGMA journal_mode = OFF; \
             PRAGMA synchronous  = OFF; \
             PRAGMA cache_size   = -65536; \
             CREATE TABLE shards ( \
                 id   TEXT PRIMARY KEY, \
                 path TEXT NOT NULL, \
                 data BLOB NOT NULL \
             );",
        )
        .map_err(|e| format!("sqlite setup: {}", e))?;

        let tx = conn
            .transaction()
            .map_err(|e| format!("begin tx: {}", e))?;
        let mut inserted_count: u64 = 0;
        // Build shard payloads in bounded parallel chunks to reduce peak RSS.
        let shard_chunk_size: usize = if shard_order.len() > 200_000 {
            256
        } else if shard_order.len() > 80_000 {
            512
        } else if shard_order.len() > 20_000 {
            1024
        } else {
            2048
        };
        {
            let mut stmt = tx
                .prepare("INSERT INTO shards VALUES (?1, ?2, ?3)")
                .map_err(|e| format!("prepare: {}", e))?;
            for chunk in shard_order.chunks(shard_chunk_size) {
                let rows: Vec<(String, String, Vec<u8>)> = chunk
                    .par_iter()
                    .map(|&rid| {
                        let path = &id_to_path[rid];
                        let shard_id_str = path_to_shard[&rid].to_string();

                        let empty = Vec::new();
                        let children_ids = parent_to_children.get(&rid).unwrap_or(&empty);

                        let mut items: Vec<ShardItem> =
                            Vec::with_capacity(children_ids.len() + 1);

                        for &cid in children_ids {
                            let size = recursive_sizes.get(&cid).copied().unwrap_or(0);
                            if size < min_size_bytes {
                                continue;
                            }
                            let cp = &id_to_path[cid];
                            items.push(ShardItem::Directory(ShardDirectoryItem {
                                id: path_to_shard[&cid],
                                name: tm_basename(cp).to_string(),
                                size,
                                owner: dir_owner_map
                                    .get(cp.as_str())
                                    .cloned()
                                    .unwrap_or_else(|| "unknown".to_string()),
                                children_count: parent_to_children
                                    .get(&cid)
                                    .map(|v| v.len())
                                    .unwrap_or(0),
                            }));
                        }
                        // Add [Files in <dir>] entry for direct file bytes (not subdirs).
                        // d_direct = this dir's total size minus the sum of all children dir sizes.
                        let children_total: i64 = children_ids
                            .iter()
                            .map(|&cid| recursive_sizes.get(&cid).copied().unwrap_or(0))
                            .sum();
                        let d_direct = recursive_sizes.get(&rid).copied().unwrap_or(0) - children_total;
                        if d_direct > 0 && d_direct >= min_size_bytes {
                            let base = tm_basename(path);
                            let label = if base.is_empty() { path.as_str() } else { base };
                            items.push(ShardItem::FileGroup(ShardFileGroupItem {
                                name: format!("[Files in {}]", label),
                                size: d_direct,
                                item_type: "file_group",
                                owner: dir_owner_map
                                    .get(path.as_str())
                                    .cloned()
                                    .unwrap_or_else(|| "unknown".to_string()),
                                children_count: 0,
                            }));
                        }

                        // Compact JSON (no extra spaces) then zlib level-6
                        let json_bytes = serde_json::to_vec(&items).expect("json");
                        let mut enc = ZlibEncoder::new(
                            Vec::with_capacity(json_bytes.len() / 2 + 32),
                            Compression::new(TREEMAP_ZLIB_LEVEL),
                        );
                        enc.write_all(&json_bytes).expect("zlib write");
                        let compressed = enc.finish().expect("zlib finish");

                        (shard_id_str, path.clone(), compressed)
                    })
                    .collect();

                for (id, path, blob) in rows {
                    stmt.execute(rusqlite::params![&id, &path, blob.as_slice()])
                        .map_err(|e| format!("insert '{}': {}", id, e))?;
                    inserted_count += 1;
                }
            }
        }
        tx.commit().map_err(|e| format!("commit: {}", e))?;

        Ok(inserted_count)
    })
    .map_err(PyRuntimeError::new_err)
}

fn _insert_shard_batch(conn: &mut Connection, batch: &[(String, String)]) -> Result<(), rusqlite::Error> {
    if batch.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare("INSERT INTO shards (id, data) VALUES (?1, ?2)")?;
        for (id, data) in batch.iter() {
            stmt.execute(params![id, data])?;
        }
    }
    tx.commit()?;
    Ok(())
}

#[pyfunction]
fn write_shards_tsv_to_sqlite(tsv_path: String, db_path: String, batch_size: Option<usize>) -> PyResult<u64> {
    let bs = batch_size.unwrap_or(5000).max(1);

    let mut conn = Connection::open(&db_path)
        .map_err(|e| PyRuntimeError::new_err(format!("open sqlite {}: {}", db_path, e)))?;

    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA cache_size = -50000;
         PRAGMA temp_store = MEMORY;
         DROP TABLE IF EXISTS shards;
         CREATE TABLE shards (id TEXT PRIMARY KEY, data TEXT);"
    ).map_err(|e| PyRuntimeError::new_err(format!("sqlite setup failed: {}", e)))?;

    let file = fs::File::open(&tsv_path)
        .map_err(|e| PyRuntimeError::new_err(format!("open tsv {}: {}", tsv_path, e)))?;
    let reader = BufReader::new(file);

    let mut batch: Vec<(String, String)> = Vec::with_capacity(bs);
    let mut total: u64 = 0;

    for line in reader.lines() {
        let l = match line {
            Ok(v) => v,
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!("read tsv {}: {}", tsv_path, e)));
            }
        };
        if l.is_empty() {
            continue;
        }
        let tab = match l.find('\t') {
            Some(i) => i,
            None => continue,
        };

        let id = l[..tab].to_string();
        let data = l[tab + 1..].to_string();
        batch.push((id, data));
        total += 1;

        if batch.len() >= bs {
            _insert_shard_batch(&mut conn, &batch)
                .map_err(|e| PyRuntimeError::new_err(format!("sqlite batch insert failed: {}", e)))?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        _insert_shard_batch(&mut conn, &batch)
            .map_err(|e| PyRuntimeError::new_err(format!("sqlite final insert failed: {}", e)))?;
    }

    Ok(total)
}

/// Receive shard data from Python, compress in parallel with Rayon, write to SQLite.
///
/// - `items`: Python list of `(shard_id, path, json_str)` tuples
/// - GIL is released for the entire compression + write pipeline
/// - Phase 1: Rayon `par_iter` → zlib compress each JSON payload (all CPU cores)
/// - Phase 2: Single SQLite transaction → zero per-row commit overhead
/// - Schema: `shards(id TEXT PRIMARY KEY, path TEXT NOT NULL, data BLOB NOT NULL)`
/// - PRAGMAs: page_size=8192, journal_mode=OFF, synchronous=OFF
#[pyfunction]
fn compress_and_write_shards(
    py: Python<'_>,
    items: Vec<(String, String, String)>, // (shard_id, path, json_data)
    db_path: String,
    _batch_size: Option<usize>, // reserved; single-tx is faster for this workload
) -> PyResult<u64> {
    let total = items.len() as u64;

    // Release GIL: the entire compression + write runs on Rust threads.
    py.allow_threads(move || -> Result<u64, String> {
        // ── Phase 1: Parallel zlib compression (rayon thread pool, no GIL) ──
        let compressed: Vec<(String, String, Vec<u8>)> = items
            .into_par_iter()
            .map(|(id, path, json)| {
                let mut enc = ZlibEncoder::new(
                    Vec::with_capacity(json.len() / 2 + 32),
                    Compression::new(TREEMAP_ZLIB_LEVEL),
                );
                enc.write_all(json.as_bytes()).expect("zlib write");
                (id, path, enc.finish().expect("zlib finish"))
            })
            .collect();

        // ── Phase 2: Single-transaction SQLite write ──
        let mut conn = Connection::open(&db_path)
            .map_err(|e| format!("open db '{}': {}", db_path, e))?;

        conn.execute_batch(
            "PRAGMA page_size   = 8192; \
             PRAGMA journal_mode = OFF; \
             PRAGMA synchronous  = OFF; \
             PRAGMA cache_size   = -65536; \
             CREATE TABLE shards ( \
                 id   TEXT PRIMARY KEY, \
                 path TEXT NOT NULL, \
                 data BLOB NOT NULL \
             );",
        )
        .map_err(|e| format!("sqlite setup: {}", e))?;

        // One transaction for all rows → minimum fsync + B-tree overhead.
        let tx = conn
            .transaction()
            .map_err(|e| format!("begin tx: {}", e))?;
        {
            let mut stmt = tx
                .prepare("INSERT INTO shards VALUES (?1, ?2, ?3)")
                .map_err(|e| format!("prepare: {}", e))?;
            for (id, path, blob) in &compressed {
                stmt.execute(rusqlite::params![id, path, blob.as_slice()])
                    .map_err(|e| format!("insert '{}': {}", id, e))?;
            }
        }
        tx.commit().map_err(|e| format!("commit: {}", e))?;

        Ok(total)
    })
    .map_err(PyRuntimeError::new_err)
}

/// Aggregate dir_sizes_map from TSV files in tmpdir entirely in Rust.
/// Returns a Python dict: {dir_path: {username: size}}
/// The uid_map argument maps uid (u32) → username (str).
/// This replaces the Python-side TSV reading loop which was GIL-bound.
#[pyfunction]
fn aggregate_dir_sizes(
    py: Python,
    tmpdir: String,
    uid_map: HashMap<u32, String>,
) -> PyResult<PyObject> {
    // Read all dirs_t*.tsv files from tmpdir
    let pattern = format!("{}/dirs_t*.tsv", tmpdir);
    let mut tsv_files: Vec<String> = glob::glob(&pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob: {}", e)))?
        .filter_map(|entry| entry.ok())
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    tsv_files.sort();

    let dir_map = _aggregate_tsv_to_map(&tsv_files, &uid_map)?;

    // Convert to Python dict
    let result = PyDict::new(py);
    for (dir_path, user_sizes) in &dir_map {
        let inner = PyDict::new(py);
        for (username, &size) in user_sizes {
            inner.set_item(username, size)?;
        }
        result.set_item(dir_path, inner)?;
    }

    Ok(result.into())
}

/// Internal helper: read TSV files and aggregate into HashMap.
pub(crate) fn _aggregate_tsv_to_map(
    tsv_files: &[String],
    uid_map: &HashMap<u32, String>,
) -> PyResult<HashMap<String, HashMap<String, i64>>> {
    let mut dir_map: HashMap<String, HashMap<String, i64>> = HashMap::new();

    for tsv_file in tsv_files {
        let f = match fs::File::open(tsv_file) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let reader = std::io::BufReader::with_capacity(256 * 1024, f);
        use std::io::BufRead;
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            if line.is_empty() {
                continue;
            }
            // Format: "<path>\t<uid>\t<size>"
            let mut parts = line.splitn(3, '\t');
            let dir_path = match parts.next() {
                Some(p) => p,
                None => continue,
            };
            let uid_raw = match parts.next() {
                Some(u) => u,
                None => continue,
            };
            let size_raw = match parts.next() {
                Some(s) => s,
                None => continue,
            };
            let uid: u32 = match uid_raw.parse() {
                Ok(u) => u,
                Err(_) => continue,
            };
            let size: i64 = match size_raw.parse() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if size <= 0 {
                continue;
            }
            let username = match uid_map.get(&uid) {
                Some(name) => name.clone(),
                None => format!("uid-{}", uid),
            };
            let by_user = dir_map.entry(dir_path.to_string()).or_insert_with(HashMap::new);
            *by_user.entry(username).or_insert(0) += size;
        }
    }

    Ok(dir_map)
}

/// Phase 3 pipeline from TSV files — avoids PyO3 HashMap conversion overhead.
/// Reads dir TSV files directly from tmpdir, builds dir_sizes + dir_owner_map
/// internally, then runs the full treemap pipeline.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn generate_treemap_from_tsv(
    py: Python<'_>,
    root_dir: String,
    tmpdir: String,
    uid_map: HashMap<u32, String>,
    json_path: String,
    db_path: String,
    max_level: usize,
    min_size_bytes: i64,
) -> PyResult<u64> {
    // Step 1: Aggregate TSV → dir_sizes HashMap (in Rust, no GIL)
    let pattern = format!("{}/dirs_t*.tsv", tmpdir);
    let mut tsv_files: Vec<String> = glob::glob(&pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob: {}", e)))?
        .filter_map(|entry| entry.ok())
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    tsv_files.sort();

    let dir_sizes = _aggregate_tsv_to_map(&tsv_files, &uid_map)?;

    // Step 2: Build dir_owner_map from dir_sizes (dominant user by size)
    let mut dir_owner_map: HashMap<String, String> = HashMap::with_capacity(dir_sizes.len());
    for (dpath, user_sizes) in &dir_sizes {
        if let Some((owner, _)) = user_sizes.iter().max_by_key(|(_, &s)| s) {
            dir_owner_map.insert(dpath.clone(), owner.clone());
        }
    }

    // Step 3: Call existing generate_treemap_sqlite logic
    generate_treemap_sqlite(
        py,
        root_dir,
        dir_sizes,
        dir_owner_map,
        json_path,
        db_path,
        max_level,
        min_size_bytes,
    )
}

#[pymodule]
fn fast_scanner(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_disk, m)?)?;
    m.add_function(wrap_pyfunction!(merge_write_user_report, m)?)?;
    m.add_function(wrap_pyfunction!(merge_write_user_report_db, m)?)?;
    m.add_function(wrap_pyfunction!(write_shards_tsv_to_sqlite, m)?)?;
    m.add_function(wrap_pyfunction!(compress_and_write_shards, m)?)?;
    m.add_function(wrap_pyfunction!(generate_treemap_sqlite, m)?)?;
    m.add_function(wrap_pyfunction!(aggregate_dir_sizes, m)?)?;
    m.add_function(wrap_pyfunction!(generate_treemap_from_tsv, m)?)?;
    m.add_function(wrap_pyfunction!(build_unified_dbs, m)?)?;
    Ok(())
}

#[inline]
pub(crate) fn split_path_for_stage(path: &str) -> (&str, &str, String) {
    let (dir, basename) = if let Some(slash_pos) = path.rfind('/') {
        let dir_part = &path[..slash_pos];
        let base_part = if slash_pos + 1 < path.len() {
            &path[slash_pos + 1..]
        } else {
            path
        };
        (dir_part, base_part)
    } else {
        ("", path)
    };

    let xt = if let Some(dot_pos) = basename.rfind('.') {
        if dot_pos > 0 && dot_pos + 1 < basename.len() {
            basename[dot_pos + 1..].to_ascii_lowercase()
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    (dir, basename, xt)
}
