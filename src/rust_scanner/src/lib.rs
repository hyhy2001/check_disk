use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::exceptions::PyRuntimeError;
use ignore::{WalkBuilder, WalkState};
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::fs;
use std::io::{Write, BufWriter};
use std::time::{Instant, Duration};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

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

    if bytes_f >= tb {
        format!("{:.2} TB", bytes_f / tb)
    } else if bytes_f >= gb {
        format!("{:.2} GB", bytes_f / gb)
    } else if bytes_f >= mb {
        format!("{:.2} MB", bytes_f / mb)
    } else if bytes_f >= kb {
        format!("{:.2} KB", bytes_f / kb)
    } else {
        format!("{} B", bytes)
    }
}

// Struct for global merged stats
struct GlobalStats {
    total_files: u64,
    total_dirs: u64,
    total_size: u64,
    uid_sizes: HashMap<u32, u64>,
    dir_sizes: HashMap<String, HashMap<u32, u64>>,
}

struct ProgressStats {
    files: u64,
    size: u64,
}

// Custom Drop to merge thread local stats into GlobalStats automatically when thread finishes
struct ThreadLocalState {
    t_files: u64,
    t_dirs: u64,
    t_size: u64,
    t_uid_sizes: HashMap<u32, u64>,
    t_dir_sizes: HashMap<String, HashMap<u32, u64>>,
    t_uid_buffers: HashMap<u32, Vec<(String, u64)>>,
    t_flush_counts: HashMap<u32, u32>,
    global_stats: Arc<Mutex<GlobalStats>>,
    progress_stats: Arc<Mutex<ProgressStats>>,
    tmpdir: String,
    thread_id: usize,
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        // 1. Flush remaining buffers natively
        for (uid, buf) in self.t_uid_buffers.iter_mut() {
            if buf.is_empty() { continue; }
            let count = self.t_flush_counts.entry(*uid).or_insert(0);
            *count += 1;
            buf.sort_by(|a, b| b.1.cmp(&a.1));
            let filepath = format!("{}/uid_{}_t{}_c{}.tsv", self.tmpdir, uid, self.thread_id, count);
            if let Ok(f) = fs::File::create(&filepath) {
                let mut ext_writer = BufWriter::new(f);
                for (p, s) in buf.iter() {
                    let _ = writeln!(ext_writer, "{}\t{}", s, p);
                }
            }
            buf.clear();
        }
        
        // 2. Aggregate into global locks
        if let Ok(mut g) = self.global_stats.lock() {
            g.total_files += self.t_files;
            g.total_dirs += self.t_dirs;
            g.total_size += self.t_size;
            
            for (uid, size) in &self.t_uid_sizes {
                *g.uid_sizes.entry(*uid).or_insert(0) += size;
            }
            
            for (dir, user_map) in &self.t_dir_sizes {
                let g_user_map = g.dir_sizes.entry(dir.clone()).or_insert_with(HashMap::new);
                for (uid, size) in user_map {
                    *g_user_map.entry(*uid).or_insert(0) += size;
                }
            }
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
    let _ = _tmpdir.into_path(); // leak
    
    let global_stats = Arc::new(Mutex::new(GlobalStats {
        total_files: 0, total_dirs: 0, total_size: 0,
        uid_sizes: HashMap::new(), dir_sizes: HashMap::new(),
    }));
    
    let progress_stats = Arc::new(Mutex::new(ProgressStats { files: 0, size: 0 }));
    let done = Arc::new(AtomicBool::new(false));
    
    let g_clone = global_stats.clone();
    let p_clone = progress_stats.clone();
    let d_clone = done.clone();
    let dir_clone = directory.clone();
    let skips = skip_dirs.clone();
    let tmpdir_clone = tmpdir_str.clone();
    
    // Auto-scale threadpool with an overprovision ratio suited for network disks
    let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let threads_count = 16.max(cpus * 4);
    let thread_counter = Arc::new(AtomicUsize::new(0));

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
                    t_files: 0, t_dirs: 0, t_size: 0,
                    t_uid_sizes: HashMap::new(), t_dir_sizes: HashMap::new(),
                    t_uid_buffers: HashMap::new(), t_flush_counts: HashMap::new(),
                    global_stats: g_clone.clone(),
                    progress_stats: p_clone.clone(),
                    tmpdir: tmpdir_clone.clone(),
                    thread_id: tid,
                };
                
                let skips = skips.clone();
                let dir = dir_clone.clone();
                let mut hardlinks: HashSet<(u64, u64)> = HashSet::new();
                
                Box::new(move |entry_res| {
                    let entry = match entry_res {
                        Ok(e) => e,
                        Err(_) => return WalkState::Continue,
                    };
                    
                    let path = entry.path();
                    let path_str = path.to_string_lossy().to_string();
                    
                    let mut should_skip = false;
                    for s in &skips {
                        if path_str.starts_with(s) { should_skip = true; break; }
                    }
                    if should_skip { 
                        // ENORMOUS FIX: Skip entirely removes this directory and its children
                        return WalkState::Skip; 
                    }
                    
                    if entry.file_type().map_or(false, |ft| ft.is_dir()) {
                        state.t_dirs += 1;
                    } else if entry.file_type().map_or(false, |ft| ft.is_file()) {
                        if let Ok(meta) = entry.metadata() {
                            if meta.nlink() > 1 {
                                let key = (meta.ino(), meta.dev());
                                if !hardlinks.insert(key) { return WalkState::Continue; }
                            }
                            
                            // Use st_blocks * 512 = actual on-disk bytes (same as Python legacy)
                            let size = meta.blocks() * 512;
                            let uid = meta.uid();
                            
                            state.t_files += 1;
                            state.t_size += size;
                            *state.t_uid_sizes.entry(uid).or_insert(0) += size;
                            
                            if let Ok(rel) = path.strip_prefix(&dir) {
                                if let Some(top_comp) = rel.components().next() {
                                    let dir_trimmed = dir.trim_end_matches('/');
                                    let top_dir_path = format!("{}/{}", dir_trimmed, top_comp.as_os_str().to_string_lossy());
                                    let user_map = state.t_dir_sizes.entry(top_dir_path).or_insert_with(HashMap::new);
                                    *user_map.entry(uid).or_insert(0) += size;
                                }
                            }
                            
                            let buf = state.t_uid_buffers.entry(uid).or_insert_with(Vec::new);
                            buf.push((path_str, size));
                            if buf.len() >= 100_000 {
                                let count = state.t_flush_counts.entry(uid).or_insert(0);
                                *count += 1;
                                buf.sort_by(|a, b| b.1.cmp(&a.1));
                                let filepath = format!("{}/uid_{}_t{}_c{}.tsv", state.tmpdir, uid, state.thread_id, count);
                                if let Ok(f) = fs::File::create(&filepath) {
                                    let mut ext_writer = BufWriter::new(f);
                                    for (p, s) in buf.iter() {
                                        let _ = writeln!(ext_writer, "{}\t{}", s, p);
                                    }
                                }
                                buf.clear();
                            }
                            
                            if state.t_files % 5_000 == 0 {
                                if let Ok(mut p) = state.progress_stats.lock() {
                                    p.files += 5_000;
                                    p.size += state.t_size;
                                }
                            }
                        }
                    }
                    WalkState::Continue
                })
            });
            
        d_clone.store(true, Ordering::SeqCst);
    });

    let start_time = Instant::now();
    let mut last_report = start_time;
    let mut last_files = 0;
    
    while !done.load(Ordering::SeqCst) {
        py.check_signals()?; // Safe exit path for python KeyboardInterrupt
        thread::sleep(Duration::from_millis(200));
        
        let now = Instant::now();
        let elapsed = now.duration_since(last_report).as_secs();
        if elapsed >= 1 {
            if let Ok(p) = progress_stats.lock() {
                let total_elapsed = now.duration_since(start_time).as_secs();
                let rate = (p.files.saturating_sub(last_files)) as f64 / elapsed as f64;
                print!(
                    "\r[{:02}:{:02}:{:02}] Files: {} | Size: {} | Rate: {} files/s   ",
                    total_elapsed / 3600,
                    (total_elapsed % 3600) / 60,
                    total_elapsed % 60,
                    format_num(p.files),
                    format_size(p.size),
                    format_num(rate as u64)
                );
                let _ = std::io::stdout().flush();
                
                last_report = now;
                last_files = p.files;
            }
        }
    }
    println!(""); // clear stdout row
    
    // Build PyDict from verified global state
    let g = global_stats.lock().unwrap();
    
    let result_dict = PyDict::new(py);
    result_dict.set_item("total_files", g.total_files)?;
    result_dict.set_item("total_dirs", g.total_dirs)?;
    result_dict.set_item("total_size", g.total_size)?;
    result_dict.set_item("detail_tmpdir", &tmpdir_str)?;
    
    let py_uid_sizes = PyDict::new(py);
    for (uid, size) in &g.uid_sizes {
        py_uid_sizes.set_item(uid, size)?;
    }
    result_dict.set_item("uid_sizes", py_uid_sizes)?;
    
    let py_dir_sizes = PyDict::new(py);
    for (dir, user_map) in &g.dir_sizes {
        let py_user_map = PyDict::new(py);
        for (uid, size) in user_map {
            py_user_map.set_item(uid, size)?;
        }
        py_dir_sizes.set_item(dir, py_user_map)?;
    }
    result_dict.set_item("dir_sizes", py_dir_sizes)?;
    
    Ok(result_dict.into())
}

#[pymodule]
fn fast_scanner(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_disk, m)?)?;
    Ok(())
}
