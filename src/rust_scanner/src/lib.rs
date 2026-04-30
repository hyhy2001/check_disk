use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::exceptions::PyRuntimeError;
use ignore::{WalkBuilder, WalkState};
use dashmap::DashSet;
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::fs;
use std::fmt::Write as FmtWrite;
use std::io::{Write, BufWriter};
use std::time::{Instant, Duration};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;

mod unified_output;
pub use unified_output::build_unified_dbs;

// Same list as Python's critical_skip_dirs
const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot", ".snapshots", ".zfs",
    "proc", "sys", "dev",
    ".nfs",
];
/// Max in-memory raw file events per scan worker before flushing to disk.
const SCAN_EVENT_FLUSH_THRESHOLD: usize = 100_000;

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
    permission_issues_count: u64,
}

// ProgressStats replaced by 3 AtomicU64 (no lock, exact counts)


struct ThreadLocalState {
    t_files: u64,
    t_dirs: u64,
    t_inodes: u64,
    t_size: u64,
    t_uid_sizes: HashMap<u32, u64>,
    t_uid_files: HashMap<u32, u64>,
    t_event_buf: String,
    t_event_buf_records: usize,
    t_event_flush_count: u32,
    t_perm_issues: u64,
    global_stats: Arc<Mutex<GlobalStats>>,
    prog_files: Arc<AtomicU64>,
    prog_dirs:  Arc<AtomicU64>,
    prog_size:  Arc<AtomicU64>,
    tmpdir: String,
    target_uids: Option<HashSet<u32>>,
    thread_id: usize,
    prof_metadata_ns: Arc<AtomicU64>,
    prof_path_ns: Arc<AtomicU64>,
    prof_flush_ns: Arc<AtomicU64>,
    prof_flush_bytes: Arc<AtomicU64>,
    prof_flush_count: Arc<AtomicU64>,
    prof_hardlink_checks: Arc<AtomicU64>,
    prof_visited_dir_checks: Arc<AtomicU64>,
    prof_max_event_buf_records: Arc<AtomicU64>,
    prof_max_event_buf_bytes: Arc<AtomicU64>,
}

impl ThreadLocalState {
    fn flush_events(&mut self) {
        if self.t_event_buf.is_empty() {
            return;
        }
        self.t_event_flush_count += 1;
        self.prof_flush_count.fetch_add(1, Ordering::Relaxed);
        let flush_start = Instant::now();
        let bytes_written = self.t_event_buf.len() as u64;
        let fp = format!("{}/scan_t{}_c{}.tsv", self.tmpdir, self.thread_id, self.t_event_flush_count);
        if let Ok(f) = fs::File::create(&fp) {
            let mut w = BufWriter::new(f);
            let _ = w.write_all(self.t_event_buf.as_bytes());
        }
        self.prof_flush_ns.fetch_add(flush_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.prof_flush_bytes.fetch_add(bytes_written, Ordering::Relaxed);
        self.t_event_buf.clear();
        self.t_event_buf_records = 0;
        if self.t_event_buf.capacity() > 64 * 1024 * 1024 {
            self.t_event_buf.shrink_to(8 * 1024 * 1024);
        }
    }

    fn flush_permission_issue(&self, path: &str, kind: &str, error_code: &str) {
        let fp = format!("{}/perm_t{}.tsv", self.tmpdir, self.thread_id);
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&fp) {
            let uid = if path.is_empty() {
                0
            } else {
                fs::symlink_metadata(path).map(|m| m.uid()).unwrap_or(0)
            };
            let _ = writeln!(f, "P	{}	{}	{}	{}", uid, kind, error_code, path);
        }
    }
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        self.flush_events();

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
            g.permission_issues_count += self.t_perm_issues;
        }
    }
}

fn error_code_from_message(msg: &str) -> &'static str {
    if msg.contains("os error 13") || msg.contains("Permission denied") || msg.contains("permission denied") {
        "EACCES"
    } else if msg.contains("os error 2") || msg.contains("No such file") || msg.contains("no such file") {
        "ENOENT"
    } else if msg.contains("os error 5") {
        "EIO"
    } else {
        "EOTHER"
    }
}

#[pyfunction(signature = (directory, skip_dirs, target_uids, max_workers=None, debug=false))]
fn scan_disk(
    py: Python,
    directory: String,
    skip_dirs: Vec<String>,
    target_uids: Option<Vec<u32>>,
    max_workers: Option<usize>,
    debug: bool,
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
        permission_issues_count: 0,
    }));
    let prog_files = Arc::new(AtomicU64::new(0));
    let prog_dirs  = Arc::new(AtomicU64::new(0));
    let prog_size  = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let prof_metadata_ns = Arc::new(AtomicU64::new(0));
    let prof_path_ns = Arc::new(AtomicU64::new(0));
    let prof_flush_ns = Arc::new(AtomicU64::new(0));
    let prof_flush_bytes = Arc::new(AtomicU64::new(0));
    let prof_flush_count = Arc::new(AtomicU64::new(0));
    let prof_hardlink_checks = Arc::new(AtomicU64::new(0));
    let prof_visited_dir_checks = Arc::new(AtomicU64::new(0));
    let prof_max_event_buf_records = Arc::new(AtomicU64::new(0));
    let prof_max_event_buf_bytes = Arc::new(AtomicU64::new(0));

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
    let pm_clone = prof_metadata_ns.clone();
    let pp_clone = prof_path_ns.clone();
    let pfns_clone = prof_flush_ns.clone();
    let pfb_clone = prof_flush_bytes.clone();
    let pfc_clone = prof_flush_count.clone();
    let ph_clone = prof_hardlink_checks.clone();
    let pv_clone = prof_visited_dir_checks.clone();
    let pmaxr_clone = prof_max_event_buf_records.clone();
    let pmaxb_clone = prof_max_event_buf_bytes.clone();

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
    let hardlink_inodes_profile = hardlink_inodes.clone();
    let visited_dirs_profile = visited_dirs.clone();

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
                    t_event_buf: String::with_capacity(8 * 1024 * 1024),
                    t_event_buf_records: 0,
                    t_event_flush_count: 0,
                    t_perm_issues: 0,
                    global_stats: g_clone.clone(),
                    prog_files: pf_clone.clone(),
                    prog_dirs:  pd_clone.clone(),
                    prog_size:  ps_clone.clone(),
                    tmpdir: tmpdir_clone.clone(),
                    target_uids: (*target_uids_shared).clone(),
                    thread_id: tid,
                    prof_metadata_ns: pm_clone.clone(),
                    prof_path_ns: pp_clone.clone(),
                    prof_flush_ns: pfns_clone.clone(),
                    prof_flush_bytes: pfb_clone.clone(),
                    prof_flush_count: pfc_clone.clone(),
                    prof_hardlink_checks: ph_clone.clone(),
                    prof_visited_dir_checks: pv_clone.clone(),
                    prof_max_event_buf_records: pmaxr_clone.clone(),
                    prof_max_event_buf_bytes: pmaxb_clone.clone(),
                };
                let skips = skips.clone();
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
                            
                            state.t_perm_issues += 1;
                            state.flush_permission_issue(&path_str, "directory", error_code_from_message(&err_str));
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
                        let meta_start = Instant::now();
                        if let Ok(meta) = entry.metadata() {
                            state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            let key = (meta.ino(), meta.dev());
                            state.prof_visited_dir_checks.fetch_add(1, Ordering::Relaxed);
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
                        } else {
                            state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }

                        state.t_dirs += 1;
                        state.t_inodes += 1;
                        state.prog_dirs.fetch_add(1, Ordering::Relaxed);
                    } else if ft.is_file() {
                        let meta_start = Instant::now();
                        let meta = match entry.metadata() {
                            Ok(m) => {
                                state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                m
                            },
                            Err(e) => {
                                state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                state.t_perm_issues += 1;
                                let path_str = path.to_string_lossy().into_owned();
                                state.flush_permission_issue(&path_str, "file", error_code_from_message(&e.to_string()));
                                return WalkState::Continue;
                            }
                        };

                        // --- Hard-link deduplication ---
                        if meta.nlink() > 1 {
                            state.prof_hardlink_checks.fetch_add(1, Ordering::Relaxed);
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
                            let path_start = Instant::now();
                            let path_owned = path.to_string_lossy();
                            state.prof_path_ns.fetch_add(path_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            let _ = writeln!(&mut state.t_event_buf, "F\t{}\t{}\t{}", uid, size, path_owned);
                            state.t_event_buf_records += 1;
                            state.prof_max_event_buf_records.fetch_max(state.t_event_buf_records as u64, Ordering::Relaxed);
                            state.prof_max_event_buf_bytes.fetch_max(state.t_event_buf.len() as u64, Ordering::Relaxed);
                            if state.t_event_buf_records >= SCAN_EVENT_FLUSH_THRESHOLD {
                                state.flush_events();
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
    result.set_item("dir_tmpdir", &tmpdir_str)?;

    let py_uid = PyDict::new(py);
    for (uid, size) in &g.uid_sizes { py_uid.set_item(uid, size)?; }
    result.set_item("uid_sizes", py_uid)?;

    let py_uid_files = PyDict::new(py);
    for (uid, files) in &g.uid_files { py_uid_files.set_item(uid, files)?; }
    result.set_item("uid_files", py_uid_files)?;

    result.set_item("permission_issues_count", g.permission_issues_count)?;

    if debug {
        let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
        let metadata_s = prof_metadata_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        let path_s = prof_path_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        let flush_s = prof_flush_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        let flush_bytes = prof_flush_bytes.load(Ordering::Relaxed);
        let flush_count = prof_flush_count.load(Ordering::Relaxed);
        let hardlink_checks = prof_hardlink_checks.load(Ordering::Relaxed);
        let visited_checks = prof_visited_dir_checks.load(Ordering::Relaxed);
        let max_buf_records = prof_max_event_buf_records.load(Ordering::Relaxed);
        let max_buf_bytes = prof_max_event_buf_bytes.load(Ordering::Relaxed);
        let hardlink_set_size = hardlink_inodes_profile.len();
        let visited_set_size = visited_dirs_profile.len();
        println!("\n[Phase 1 Profile]");
        println!("  Wall time:          {:.2}s", elapsed);
        println!("  Metadata time:      {:.2}s aggregate ({:.1}% of worker time)", metadata_s, metadata_s * 100.0 / elapsed);
        println!("  Path stringify:     {:.2}s aggregate", path_s);
        println!("  TSV flush time:     {:.2}s aggregate", flush_s);
        println!("  TSV flushes:        {}", format_num(flush_count));
        println!("  TSV bytes approx:   {}", format_size(flush_bytes));
        println!("  Hardlink checks:    {}", format_num(hardlink_checks));
        println!("  Visited dir checks: {}", format_num(visited_checks));
        println!("  Max event buffer:   {} records / {}", format_num(max_buf_records), format_size(max_buf_bytes));
        println!("  Hardlink set size:  {}", format_num(hardlink_set_size as u64));
        println!("  Visited set size:   {}", format_num(visited_set_size as u64));
    }

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


#[pymodule]
fn fast_scanner(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_disk, m)?)?;
    m.add_function(wrap_pyfunction!(merge_write_user_report, m)?)?;
    m.add_function(wrap_pyfunction!(build_unified_dbs, m)?)?;
    Ok(())
}

