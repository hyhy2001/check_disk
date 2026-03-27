use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::exceptions::PyRuntimeError;
use jwalk::WalkDir;
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::fs;
use std::io::{Write, BufWriter};
use std::time::Instant;

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

#[pyfunction]
fn scan_disk(py: Python, directory: String, skip_dirs: Vec<String>) -> PyResult<PyObject> {
    let mut total_files: u64 = 0;
    let mut total_dirs: u64 = 0;
    let mut total_size: u64 = 0;
    
    let mut uid_sizes: HashMap<u32, u64> = HashMap::new();
    let mut dir_sizes: HashMap<String, HashMap<u32, u64>> = HashMap::new();
    let mut hardlink_inodes: HashSet<(u64, u64)> = HashSet::new();
    
    // TSV state
    let _tmpdir = tempfile::Builder::new()
        .prefix("checkdisk_rust_")
        .tempdir()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let tmpdir_str = _tmpdir.path().to_string_lossy().to_string();
    let _ = _tmpdir.into_path(); // Leak to let python clean it up later.
    
    let mut flush_counts: HashMap<u32, u32> = HashMap::new();
    let mut uid_buffers: HashMap<u32, Vec<(String, u64)>> = HashMap::new();
    
    let start_time = Instant::now();
    let mut last_report = start_time;
    let mut last_files = 0;
    
    let mut check_counter: u64 = 0;
    
    for entry_res in WalkDir::new(&directory).skip_hidden(false) {
        check_counter += 1;
        if check_counter % 10_000 == 0 {
            py.check_signals()?;
        }
        
        let entry = match entry_res {
            Ok(e) => e,
            Err(_) => continue,
        };
        
        let path = entry.path();
        let path_str = path.to_string_lossy().to_string();
        
        let mut should_skip = false;
        for s in &skip_dirs {
            if path_str.starts_with(s) {
                should_skip = true;
                break;
            }
        }
        if should_skip { continue; }
        
        if entry.file_type().is_dir() {
            total_dirs += 1;
        } else if entry.file_type().is_file() {
            if let Ok(meta) = entry.metadata() {
                if meta.nlink() > 1 {
                    let key = (meta.ino(), meta.dev());
                    if !hardlink_inodes.insert(key) {
                        continue;
                    }
                }
                
                let size = meta.len();
                let uid = meta.uid();
                
                total_files += 1;
                total_size += size;
                
                if total_files % 50_000 == 0 {
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_report).as_secs();
                    if elapsed >= 5 { // Print every 5 seconds
                        let total_elapsed = now.duration_since(start_time).as_secs();
                        let rate = (total_files - last_files) as f64 / elapsed as f64;
                        println!(
                            "[{:02}:{:02}:{:02}] Files: {} | Dirs: {} | Size: {} | Rate: {} files/s",
                            total_elapsed / 3600,
                            (total_elapsed % 3600) / 60,
                            total_elapsed % 60,
                            format_num(total_files),
                            format_num(total_dirs),
                            format_size(total_size),
                            format_num(rate as u64)
                        );
                        last_report = now;
                        last_files = total_files;
                    }
                }
                
                *uid_sizes.entry(uid).or_insert(0) += size;
                
                if let Ok(rel) = path.strip_prefix(&directory) {
                    if let Some(top_comp) = rel.components().next() {
                        let top_dir_path = format!("{}/{}", directory.trim_end_matches('/'), top_comp.as_os_str().to_string_lossy());
                        let user_map = dir_sizes.entry(top_dir_path).or_insert_with(HashMap::new);
                        *user_map.entry(uid).or_insert(0) += size;
                    }
                }
                
                let buf = uid_buffers.entry(uid).or_insert_with(Vec::new);
                buf.push((path_str.clone(), size));
                
                if buf.len() >= 200_000 {
                    let count = flush_counts.entry(uid).or_insert(0);
                    *count += 1;
                    buf.sort_by(|a, b| b.1.cmp(&a.1));
                    let filepath = format!("{}/uid_{}_t0_c{}.tsv", tmpdir_str, uid, count);
                    if let Ok(f) = fs::File::create(&filepath) {
                        let mut ext_writer = BufWriter::new(f);
                        for (p, s) in buf.iter() {
                            let _ = writeln!(ext_writer, "{}\t{}", s, p);
                        }
                    }
                    buf.clear();
                }
            }
        }
    }
    
    for (uid, buf) in uid_buffers.iter_mut() {
        if buf.is_empty() { continue; }
        let count = flush_counts.entry(*uid).or_insert(0);
        *count += 1;
        buf.sort_by(|a, b| b.1.cmp(&a.1));
        let filepath = format!("{}/uid_{}_t0_c{}.tsv", tmpdir_str, uid, count);
        if let Ok(f) = fs::File::create(&filepath) {
            let mut ext_writer = BufWriter::new(f);
            for (p, s) in buf.iter() {
                let _ = writeln!(ext_writer, "{}\t{}", s, p);
            }
        }
        buf.clear();
    }
    
    let result_dict = PyDict::new(py);
    result_dict.set_item("total_files", total_files)?;
    result_dict.set_item("total_dirs", total_dirs)?;
    result_dict.set_item("total_size", total_size)?;
    result_dict.set_item("detail_tmpdir", &tmpdir_str)?;
    
    let py_uid_sizes = PyDict::new(py);
    for (uid, size) in uid_sizes {
        py_uid_sizes.set_item(uid, size)?;
    }
    result_dict.set_item("uid_sizes", py_uid_sizes)?;
    
    let py_dir_sizes = PyDict::new(py);
    for (dir, user_map) in dir_sizes {
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
