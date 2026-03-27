use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyRuntimeError;
use ignore::{WalkBuilder, WalkState};
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::fs;
use std::io::{Write, BufWriter};
use std::time::{Instant, Duration};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;

// Same list as Python's critical_skip_dirs
const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot", ".snapshots", ".zfs",
    "proc", "sys", "dev",
    ".nfs",
];

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

struct GlobalStats {
    total_files: u64,
    total_dirs: u64,
    total_size: u64,
    uid_sizes: HashMap<u32, u64>,
    dir_sizes: HashMap<String, HashMap<u32, u64>>,
    permission_issues: Vec<(String, String, String)>, // (path, kind, error)
}

// ProgressStats replaced by 3 AtomicU64 (no lock, exact counts)


struct ThreadLocalState {
    t_files: u64,
    t_dirs: u64,
    t_size: u64,
    t_size_last_progress: u64, // tracks last-reported size for delta calculation
    t_uid_sizes: HashMap<u32, u64>,
    t_dir_sizes: HashMap<String, HashMap<u32, u64>>,
    t_uid_buffers: HashMap<u32, Vec<(String, u64)>>,
    t_flush_counts: HashMap<u32, u32>,
    t_perm_issues: Vec<(String, String, String)>,
    global_stats: Arc<Mutex<GlobalStats>>,
    prog_files: Arc<AtomicU64>,
    prog_dirs:  Arc<AtomicU64>,
    prog_size:  Arc<AtomicU64>,
    tmpdir: String,
    thread_id: usize,
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        // 1. Flush remaining buffers
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
        }

        // 2. Merge into global state
        if let Ok(mut g) = self.global_stats.lock() {
            g.total_files += self.t_files;
            g.total_dirs  += self.t_dirs;
            g.total_size  += self.t_size;
            for (uid, size) in &self.t_uid_sizes {
                *g.uid_sizes.entry(*uid).or_insert(0) += size;
            }
            for (dir, user_map) in &self.t_dir_sizes {
                let gm = g.dir_sizes.entry(dir.clone()).or_insert_with(HashMap::new);
                for (uid, size) in user_map {
                    *gm.entry(*uid).or_insert(0) += size;
                }
            }
            g.permission_issues.extend(self.t_perm_issues.drain(..));
        }
    }
}

#[pyfunction]
fn scan_disk(py: Python, directory: String, skip_dirs: Vec<String>) -> PyResult<PyObject> {
    let _tmpdir = tempfile::Builder::new()
        .prefix("checkdisk_rust_")
        .tempdir()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let tmpdir_str = _tmpdir.path().to_string_lossy().to_string();
    let _ = _tmpdir.into_path(); // leak — Python cleans up later

    let global_stats = Arc::new(Mutex::new(GlobalStats {
        total_files: 0, total_dirs: 0, total_size: 0,
        uid_sizes: HashMap::new(), dir_sizes: HashMap::new(),
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
    let threads_count = 16.max(cpus * 4);
    let thread_counter = Arc::new(AtomicUsize::new(0));
    // Shared cross-worker hard-link deduplication (equivalent to Python's inode_lock + hardlink_inodes)
    let hardlink_inodes: Arc<Mutex<HashSet<(u64, u64)>>> = Arc::new(Mutex::new(HashSet::new()));

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
                    t_files: 0, t_dirs: 0, t_size: 0, t_size_last_progress: 0,
                    t_uid_sizes: HashMap::new(),
                    t_dir_sizes: HashMap::new(),
                    t_uid_buffers: HashMap::new(),
                    t_flush_counts: HashMap::new(),
                    t_perm_issues: Vec::new(),
                    global_stats: g_clone.clone(),
                    prog_files: pf_clone.clone(),
                    prog_dirs:  pd_clone.clone(),
                    prog_size:  ps_clone.clone(),
                    tmpdir: tmpdir_clone.clone(),
                    thread_id: tid,
                };
                let skips = skips.clone();
                let dir = dir_clone.clone();
                let hardlinks_shared = hardlink_inodes.clone();

                Box::new(move |entry_res| {
                    // --- Error entry: record as permission issue ---
                    let entry = match entry_res {
                        Ok(e) => e,
                        Err(err) => {
                            state.t_perm_issues.push((
                                String::new(),
                                "unknown".to_string(),
                                err.to_string(),
                            ));
                            return WalkState::Continue;
                        }
                    };

                    let path = entry.path();
                    let path_str = path.to_string_lossy().to_string();

                    // --- Configured skip_dirs (prefix match — prunes whole subtree) ---
                    for s in &skips {
                        if path_str.starts_with(s.as_str()) {
                            return WalkState::Skip;
                        }
                    }

                    let ft = match entry.file_type() {
                        Some(f) => f,
                        None => return WalkState::Continue,
                    };

                    if ft.is_symlink() {
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

                        // --- Cross-device check: skip NFS / snapshots / bind-mounts ---
                        if let Some(rdev) = root_dev {
                            if let Ok(meta) = entry.metadata() {
                                if meta.dev() != rdev {
                                    return WalkState::Skip;
                                }
                            }
                        }

                        state.t_dirs += 1;
                        state.prog_dirs.fetch_add(1, Ordering::Relaxed);

                    } else if ft.is_file() {
                        let meta = match entry.metadata() {
                            Ok(m) => m,
                            Err(e) => {
                                state.t_perm_issues.push((
                                    path_str,
                                    "file".to_string(),
                                    e.to_string(),
                                ));
                                return WalkState::Continue;
                            }
                        };

                        // --- Hard-link deduplication (shared across all workers, like Python inode_lock) ---
                        if meta.nlink() > 1 {
                            let key = (meta.ino(), meta.dev());
                            let mut seen = hardlinks_shared.lock().unwrap();
                            if !seen.insert(key) { return WalkState::Continue; }
                        }

                        // st_blocks * 512 = actual on-disk bytes, same as Python legacy
                        let size = meta.blocks() * 512;
                        let uid  = meta.uid();

                        state.t_files += 1;
                        state.t_size  += size;
                        *state.t_uid_sizes.entry(uid).or_insert(0) += size;

                        // Attribute size to direct parent dir only (flat, matches Python legacy)
                        // Python: dir_sizes[current_dir] = sizes of files DIRECTLY inside it
                        if let Some(parent) = path.parent() {
                            // Only attribute if parent is at or below the scan root
                            if parent.starts_with(&dir) {
                                let parent_str = parent.to_string_lossy().to_string();
                                *state.t_dir_sizes
                                    .entry(parent_str).or_insert_with(HashMap::new)
                                    .entry(uid).or_insert(0) += size;
                            }
                        }

                        // Streaming TSV buffer (same as Python's DETAIL_FLUSH_THRESHOLD)
                        let buf = state.t_uid_buffers.entry(uid).or_insert_with(Vec::new);
                        buf.push((path_str, size));
                        if buf.len() >= 100_000 {
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
                        }

                        // Exact progress tracking via atomics — no lock, no rounding
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
            println!(
                "[{:02}:{:02}:{:02}] Files: {} | Dirs: {} | Size: {} | Rate: {} files/s",
                total_elapsed / 3600, (total_elapsed % 3600) / 60, total_elapsed % 60,
                format_num(total_files), format_num(total_dirs),
                format_size(total_size), format_rate(rate)
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
    result.set_item("total_size",   g.total_size)?;
    result.set_item("detail_tmpdir", &tmpdir_str)?;

    let py_uid = PyDict::new(py);
    for (uid, size) in &g.uid_sizes { py_uid.set_item(uid, size)?; }
    result.set_item("uid_sizes", py_uid)?;

    let py_dir = PyDict::new(py);
    for (dir, user_map) in &g.dir_sizes {
        let m = PyDict::new(py);
        for (uid, size) in user_map { m.set_item(uid, size)?; }
        py_dir.set_item(dir, m)?;
    }
    result.set_item("dir_sizes", py_dir)?;

    // permission_issues as list of dicts (path, type, error)
    let py_perms = PyList::empty(py);
    for (path, kind, err) in &g.permission_issues {
        let d = PyDict::new(py);
        d.set_item("path", path)?;
        d.set_item("type", kind)?;
        d.set_item("error", err)?;
        py_perms.append(d)?;
    }
    result.set_item("permission_issues", py_perms)?;

    Ok(result.into())
}

#[pymodule]
fn fast_scanner(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_disk, m)?)?;
    Ok(())
}
