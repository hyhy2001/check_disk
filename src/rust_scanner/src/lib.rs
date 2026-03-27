use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::exceptions::PyRuntimeError;
use jwalk::WalkDir;
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::MetadataExt;
use std::fs;
use std::io::{Write, BufWriter};

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
    
    for entry_res in WalkDir::new(&directory).skip_hidden(false) {
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
