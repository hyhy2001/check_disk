use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;

use crate::pipe_types::{parse_permission_line, parse_scan_event_line, PermissionEvent, ScanEvent};

pub fn get_scan_event_files(tmpdir: &str) -> PyResult<(Vec<PathBuf>, Vec<PathBuf>)> {
    let bin_pattern = format!("{}/scan_t*.bin", tmpdir);
    let mut bin_paths: Vec<PathBuf> = glob::glob(&bin_pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob bin: {}", e)))?
        .filter_map(|entry| entry.ok())
        .collect();
    bin_paths.sort();

    let tsv_pattern = format!("{}/scan_t*.tsv", tmpdir);
    let mut tsv_paths: Vec<PathBuf> = glob::glob(&tsv_pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob: {}", e)))?
        .filter_map(|entry| entry.ok())
        .collect();
    tsv_paths.sort();

    Ok((bin_paths, tsv_paths))
}

pub fn for_each_scan_event_in_file<F>(path: &std::path::Path, is_bin: bool, mut on_event: F) -> PyResult<()>
where
    F: FnMut(ScanEvent),
{
    if is_bin {
        let mut f = fs::File::open(path)
            .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path.display(), e)))?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)
            .map_err(|e| PyRuntimeError::new_err(format!("read {}: {}", path.display(), e)))?;

        let mut offset = 0usize;
        while offset + 1 + 4 + 8 + 4 <= data.len() {
            let tag = data[offset];
            offset += 1;
            if tag != 1 {
                return Err(PyRuntimeError::new_err(format!("invalid scan bin tag {} in {}", tag, path.display())));
            }

            let uid = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap_or([0; 4]));
            offset += 4;
            let size = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap_or([0; 8]));
            offset += 8;
            let path_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap_or([0; 4])) as usize;
            offset += 4;

            if offset + path_len > data.len() {
                return Err(PyRuntimeError::new_err(format!("truncated scan bin record in {}", path.display())));
            }
            let path_str = String::from_utf8_lossy(&data[offset..offset + path_len]).to_string();
            offset += path_len;

            on_event(ScanEvent { uid, size, path: path_str });
        }
    } else {
        let f = fs::File::open(path)
            .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path.display(), e)))?;
        for line in BufReader::new(f).lines() {
            let line = line.map_err(|e| PyRuntimeError::new_err(format!("read {}: {}", path.display(), e)))?;
            if let Some(event) = parse_scan_event_line(&line) {
                on_event(event);
            }
        }
    }
    Ok(())
}

pub fn read_permission_events(tmpdir: &str) -> PyResult<Vec<PermissionEvent>> {
    let pattern = format!("{}/perm_t*.tsv", tmpdir);
    let mut paths: Vec<PathBuf> = glob::glob(&pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob perm: {}", e)))?
        .filter_map(|entry| entry.ok())
        .collect();
    paths.sort();

    let mut events = Vec::new();
    for path in paths {
        let f = fs::File::open(&path)
            .map_err(|e| PyRuntimeError::new_err(format!("open perm {}: {}", path.display(), e)))?;
        for line in BufReader::new(f).lines() {
            let line = line.map_err(|e| PyRuntimeError::new_err(format!("read perm {}: {}", path.display(), e)))?;
            if let Some(evt) = parse_permission_line(&line) {
                events.push(evt);
            }
        }
    }
    Ok(events)
}
