use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::fs;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::pipe_types::{parse_permission_line, DirAggEvent, PermissionEvent, ScanEvent};
use crate::scan_constants::{BIN_MAGIC_LEN, DIR_AGG_BIN_MAGIC_V1, SCAN_EVENT_BIN_MAGIC_V1};

fn prepare_bin_reader(
    path: &std::path::Path,
    expected_magic: &[u8; BIN_MAGIC_LEN],
) -> PyResult<BufReader<fs::File>> {
    let f = fs::File::open(path)
        .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", path.display(), e)))?;
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, f);
    let mut magic = [0u8; BIN_MAGIC_LEN];

    match reader.read_exact(&mut magic) {
        Ok(()) => {
            if &magic == expected_magic {
                return Ok(reader);
            }
            reader
                .seek(SeekFrom::Start(0))
                .map_err(|e| PyRuntimeError::new_err(format!("seek {}: {}", path.display(), e)))?;
            Ok(reader)
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            reader.seek(SeekFrom::Start(0)).map_err(|seek_err| {
                PyRuntimeError::new_err(format!("seek {}: {}", path.display(), seek_err))
            })?;
            Ok(reader)
        }
        Err(e) => Err(PyRuntimeError::new_err(format!(
            "read {}: {}",
            path.display(),
            e
        ))),
    }
}

pub fn get_scan_event_files(tmpdir: &str) -> PyResult<Vec<PathBuf>> {
    let bin_pattern = format!("{}/scan_t*.bin", tmpdir);
    let mut bin_paths: Vec<PathBuf> = glob::glob(&bin_pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob bin: {}", e)))?
        .filter_map(|entry| entry.ok())
        .collect();
    bin_paths.sort();

    Ok(bin_paths)
}

pub fn get_dir_agg_files(tmpdir: &str) -> PyResult<Vec<PathBuf>> {
    let pattern = format!("{}/diragg_t*.bin", tmpdir);
    let mut paths: Vec<PathBuf> = glob::glob(&pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob diragg: {}", e)))?
        .filter_map(|entry| entry.ok())
        .collect();
    paths.sort();
    Ok(paths)
}

pub fn for_each_scan_event_in_file<F>(path: &std::path::Path, mut on_event: F) -> PyResult<()>
where
    F: FnMut(ScanEvent),
{
    let mut reader = prepare_bin_reader(path, &SCAN_EVENT_BIN_MAGIC_V1)?;
    let mut header = [0u8; 16];
    loop {
        match reader.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!(
                    "read {}: {}",
                    path.display(),
                    e
                )))
            }
        }
        let uid = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let size = u64::from_le_bytes(header[4..12].try_into().unwrap());
        let path_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        let mut path_bytes = vec![0u8; path_len];
        reader
            .read_exact(&mut path_bytes)
            .map_err(|e| PyRuntimeError::new_err(format!("read path {}: {}", path.display(), e)))?;
        let path_str = String::from_utf8_lossy(&path_bytes).to_string();
        on_event(ScanEvent {
            uid,
            size,
            path: path_str,
        });
    }
    Ok(())
}

pub fn for_each_dir_agg_in_file<F>(path: &std::path::Path, mut on_event: F) -> PyResult<()>
where
    F: FnMut(DirAggEvent),
{
    let mut reader = prepare_bin_reader(path, &DIR_AGG_BIN_MAGIC_V1)?;
    let mut header = [0u8; 16];
    loop {
        match reader.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!(
                    "read diragg {}: {}",
                    path.display(),
                    e
                )));
            }
        }
        let uid = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let size = i64::from_le_bytes(header[4..12].try_into().unwrap());
        let path_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        let mut path_bytes = vec![0u8; path_len];
        reader.read_exact(&mut path_bytes).map_err(|e| {
            PyRuntimeError::new_err(format!("read diragg path {}: {}", path.display(), e))
        })?;
        let path_str = String::from_utf8_lossy(&path_bytes).to_string();
        on_event(DirAggEvent {
            uid,
            size,
            path: path_str,
        });
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
            let line = line.map_err(|e| {
                PyRuntimeError::new_err(format!("read perm {}: {}", path.display(), e))
            })?;
            if let Some(evt) = parse_permission_line(&line) {
                events.push(evt);
            }
        }
    }
    Ok(events)
}
