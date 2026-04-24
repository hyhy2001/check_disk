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

// Same list as Python's critical_skip_dirs
const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot", ".snapshots", ".zfs",
    "proc", "sys", "dev",
    ".nfs",
];
/// Max in-memory file entries per UID buffer before flushing to disk.
const DETAIL_FLUSH_THRESHOLD: usize = 50_000;
/// Max in-memory dir-size entries per thread buffer before flushing to disk.
const DIR_FLUSH_THRESHOLD: usize = 50_000;
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
fn sanitise_path(raw: &str) -> String {
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
    batch: &[(i64, i64, u64, i64)],
) -> Result<(), rusqlite::Error> {
    if batch.is_empty() { return Ok(()); }
    let mut stmt = conn.prepare_cached(
        "INSERT OR REPLACE INTO stage_files (dir_id, basename_id, size, xt_id) VALUES (?1, ?2, ?3, ?4)",
    )?;
    for (dir_id, basename_id, size, xt_id) in batch.iter() {
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

#[pyfunction]
fn merge_write_user_report_db(
    tmpdir:      String,
    uids:        Vec<u32>,
    username:    String,
    output_path: String,
    timestamp:   i64,
    batch_size:  Option<usize>,
) -> PyResult<(u64, u64)> {
    let bs = batch_size.unwrap_or(5000).max(1);

    let chunk_files = glob_module_rust_many(&tmpdir, &uids)?;

    if let Some(parent) = std::path::Path::new(&output_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", parent.display(), e)))?;
    }

    let conn = Connection::open(&output_path)
        .map_err(|e| PyRuntimeError::new_err(format!("open sqlite {}: {}", output_path, e)))?;

    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA cache_size = -20000;
         PRAGMA temp_store = FILE;",
    ).map_err(|e| PyRuntimeError::new_err(format!("sqlite pragmas: {}", e)))?;

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
         CREATE INDEX IF NOT EXISTS idx_files_data_xt_size
             ON files_data(xt_id, size DESC);
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

    let mut total_files: u64 = 0;
    let mut total_used:  u64 = 0;
    let mut dir_cache: HashMap<String, i64> = HashMap::with_capacity(50_000);
    let mut basename_cache: HashMap<String, i64> = HashMap::with_capacity(50_000);
    let mut xt_cache: HashMap<String, i64> = HashMap::with_capacity(2_000);
    let mut stage_batch: Vec<(i64, i64, u64, i64)> = Vec::with_capacity(bs);

    // Keep cache growth bounded on very large scans (millions of unique names/dirs).
    let cache_soft_cap = 300_000usize;

    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TEMP TABLE stage_files (
             dir_id      INTEGER NOT NULL,
             basename_id INTEGER NOT NULL,
             size        INTEGER NOT NULL,
             xt_id       INTEGER,
             PRIMARY KEY(dir_id, basename_id)
         );",
    ).map_err(|e| PyRuntimeError::new_err(format!("stage setup: {}", e)))?;

    if !chunk_files.is_empty() {
        let mut readers: Vec<std::io::Lines<BufReader<fs::File>>> = Vec::new();
        for path in &chunk_files {
            let f = fs::File::open(path)
                .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path, e)))?;
            readers.push(BufReader::new(f).lines());
        }

        let mut heap: std::collections::BinaryHeap<(u64, String, usize)> =
            std::collections::BinaryHeap::new();
        for (idx, reader) in readers.iter_mut().enumerate() {
            if let Some(Ok(line)) = reader.next() {
                if let Some((size, path)) = parse_tsv_line(&line) {
                    heap.push((size, path, idx));
                }
            }
        }

        while let Some((size, raw_path, idx)) = heap.pop() {
            let safe = sanitise_path(&raw_path);
            let path_ref = std::path::Path::new(&safe);

            let dir_str = path_ref.parent().and_then(|p| p.to_str()).unwrap_or("");
            let basename = path_ref.file_name().and_then(|n| n.to_str()).unwrap_or(&safe);
            let xt = path_ref.extension().and_then(|s| s.to_str()).unwrap_or("").to_ascii_lowercase();

            let dir_id = _intern_dir_id(&conn, &mut dir_cache, dir_str)
                .map_err(|e| PyRuntimeError::new_err(format!("dir intern: {}", e)))?;
            let basename_id = _intern_basename_id(&conn, &mut basename_cache, basename)
                .map_err(|e| PyRuntimeError::new_err(format!("basename intern: {}", e)))?;
            let xt_id = _intern_xt_id(&conn, &mut xt_cache, &xt)
                .map_err(|e| PyRuntimeError::new_err(format!("xt intern: {}", e)))?;

            stage_batch.push((dir_id, basename_id, size, xt_id));
            total_files += 1;
            total_used += size;

            if stage_batch.len() >= bs {
                _insert_stage_batch(&conn, &stage_batch)
                    .map_err(|e| PyRuntimeError::new_err(format!("stage batch insert: {}", e)))?;
                stage_batch.clear();
            }

            if dir_cache.len() > cache_soft_cap {
                dir_cache.clear();
            }
            if basename_cache.len() > cache_soft_cap {
                basename_cache.clear();
            }
            if xt_cache.len() > 16_384 {
                xt_cache.clear();
            }

            if let Some(Ok(line)) = readers[idx].next() {
                if let Some((sz2, p2)) = parse_tsv_line(&line) {
                    heap.push((sz2, p2, idx));
                }
            }
        }
    }

    if !stage_batch.is_empty() {
        _insert_stage_batch(&conn, &stage_batch)
            .map_err(|e| PyRuntimeError::new_err(format!("final stage insert: {}", e)))?;
    }

    conn.execute(
        "INSERT OR IGNORE INTO files_data (dir_id, basename_id, size, xt_id)
         SELECT sf.dir_id, sf.basename_id, sf.size, sf.xt_id
         FROM stage_files sf",
        [],
    ).map_err(|e| PyRuntimeError::new_err(format!("insert new files_data: {}", e)))?;

    conn.execute(
        "UPDATE files_data
         SET size = (
                 SELECT sf.size
                 FROM stage_files sf
                 WHERE sf.dir_id = files_data.dir_id
                   AND sf.basename_id = files_data.basename_id
             ),
             xt_id = (
                 SELECT sf.xt_id
                 FROM stage_files sf
                 WHERE sf.dir_id = files_data.dir_id
                   AND sf.basename_id = files_data.basename_id
             )
         WHERE EXISTS (
             SELECT 1
             FROM stage_files sf
             WHERE sf.dir_id = files_data.dir_id
               AND sf.basename_id = files_data.basename_id
               AND (
                   files_data.size != sf.size
                   OR IFNULL(files_data.xt_id, -1) != IFNULL(sf.xt_id, -1)
               )
         )",
        [],
    ).map_err(|e| PyRuntimeError::new_err(format!("update changed files_data: {}", e)))?;

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

    conn.execute("DELETE FROM meta", [])
        .map_err(|e| PyRuntimeError::new_err(format!("meta reset: {}", e)))?;
    conn.execute(
        "INSERT INTO meta(date, user, total_items, total_used) VALUES (?1, ?2, ?3, ?4)",
        params![timestamp, sanitise_path(&username), total_files as i64, total_used as i64],
    ).map_err(|e| PyRuntimeError::new_err(format!("meta insert: {}", e)))?;

    conn.execute_batch("DROP TABLE stage_files; COMMIT")
        .map_err(|e| PyRuntimeError::new_err(format!("stage commit: {}", e)))?;

    Ok((total_files, total_used))
}



fn parse_tsv_line(line: &str) -> Option<(u64, String)> {
    let tab = line.find('\t')?;
    let size: u64 = line[..tab].trim().parse().ok()?;
    let path = line[tab + 1..].to_string();
    Some((size, path))
}

fn glob_module_rust_many(tmpdir: &str, uids: &[u32]) -> PyResult<Vec<String>> {
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
fn generate_treemap_sqlite(
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

#[pymodule]
fn fast_scanner(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_disk, m)?)?;
    m.add_function(wrap_pyfunction!(merge_write_user_report, m)?)?;
    m.add_function(wrap_pyfunction!(merge_write_user_report_db, m)?)?;
    m.add_function(wrap_pyfunction!(write_shards_tsv_to_sqlite, m)?)?;
    m.add_function(wrap_pyfunction!(compress_and_write_shards, m)?)?;
    m.add_function(wrap_pyfunction!(generate_treemap_sqlite, m)?)?;
    Ok(())
}
