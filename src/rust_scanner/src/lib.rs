use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};

mod pipe_events;
mod pipe_binary;
mod pipe_io;
mod pipe_permission;
mod pipe_treemap;
mod pipe_types;
mod pipe_user_finalize;
mod pipe_user_jobs;
mod pipeline_manifest;
mod phase_index;
mod report_pipeline;
mod scan_constants;
mod scan_core;
mod scan_state;
mod scan_utils;
use pipeline_manifest::build_pipeline_impl;

/// Read RSS memory from /proc/self/status in MB (Linux only).
// ProgressStats replaced by 3 AtomicU64 (no lock, exact counts)

#[allow(dead_code)]
#[pyfunction(signature = (directory, skip_dirs, target_uids, max_workers=None, debug=false))]
fn scan_disk_reference(
    py: Python,
    directory: String,
    skip_dirs: Vec<String>,
    target_uids: Option<Vec<u32>>,
    max_workers: Option<usize>,
    debug: bool,
) -> PyResult<PyObject> {
    scan_core::run_scan_core(
        py,
        directory,
        skip_dirs,
        target_uids,
        max_workers,
        debug,
        "reference",
    )
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
        .map(|c| {
            if c == '\u{FFFD}' || (c.is_control() && c != '\t') {
                '\u{FFFD}'
            } else {
                c
            }
        })
        .collect()
}

/// Escape a string for JSON: wrap in quotes, escape backslash, double-quote,
/// and control characters.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
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
    tmpdir: String,
    uids: Vec<u32>,
    username: String,
    output_path: String,
    timestamp: i64,
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
    writeln!(
        w,
        "{{\"_meta\":{{\"date\":{},\"user\":{},\"total_files\":{},\"total_used\":{}}}}}",
        timestamp,
        json_escape(&sanitise_path(&username)),
        total_files,
        total_used
    )
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
        let xt = std::path::Path::new(&safe)
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        writeln!(
            w,
            "{{\"path\":{},\"size\":{},\"xt\":{}}}",
            json_escape(&safe),
            size,
            json_escape(xt)
        )
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Pull next line from same chunk
        if let Some(Ok(line)) = readers3[idx].next() {
            if let Some((sz2, p2)) = parse_tsv_line(&line) {
                heap3.push((sz2, p2, idx));
            }
        }
    }

    w.flush()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

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
        let Some(pivot) = rest.find("_t") else {
            continue;
        };
        if pivot == 0 {
            continue;
        }
        let uid_raw = &rest[..pivot];
        let Ok(uid) = uid_raw.parse::<u32>() else {
            continue;
        };
        if wanted.contains(&uid) {
            files.push(entry.path().to_string_lossy().to_string());
        }
    }

    files.sort();
    Ok(files)
}

fn _write_empty_report(output_path: &str, username: &str, timestamp: i64) -> PyResult<()> {
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        fs::create_dir_all(parent).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }
    let mut w = BufWriter::new(
        fs::File::create(output_path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
    );
    writeln!(
        w,
        "{{\"_meta\":{{\"date\":{},\"user\":{},\"total_files\":0,\"total_used\":0}}}}",
        timestamp,
        json_escape(username)
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(())
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
    scan_core::run_scan_core(
        py,
        directory,
        skip_dirs,
        target_uids,
        max_workers,
        debug,
        "production",
    )
}

#[pyfunction(signature = (tmpdir, uids_map, team_map, pipeline_db_path, treemap_json, treemap_db, treemap_root, max_level, min_size_bytes, timestamp, max_workers, build_treemap=true, debug=false))]
#[allow(clippy::too_many_arguments)]
fn build_pipeline(
    py: Python<'_>,
    tmpdir: String,
    uids_map: HashMap<u32, String>,
    team_map: HashMap<String, String>,
    pipeline_db_path: String,
    treemap_json: String,
    treemap_db: String,
    treemap_root: String,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    max_workers: usize,
    build_treemap: bool,
    debug: bool,
) -> PyResult<u64> {
    build_pipeline_impl(
        py,
        tmpdir,
        uids_map,
        team_map,
        pipeline_db_path,
        treemap_json,
        treemap_db,
        treemap_root,
        max_level,
        min_size_bytes,
        timestamp,
        max_workers,
        build_treemap,
        debug,
    )
}

#[pyfunction(signature = (tmpdir, uids_map, team_map, pipeline_db_path, treemap_json, treemap_db, treemap_root, max_level, min_size_bytes, timestamp, max_workers, build_treemap=true, debug=false))]
#[allow(clippy::too_many_arguments)]
fn build_pipeline_dbs(
    py: Python<'_>,
    tmpdir: String,
    uids_map: HashMap<u32, String>,
    team_map: HashMap<String, String>,
    pipeline_db_path: String,
    treemap_json: String,
    treemap_db: String,
    treemap_root: String,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    max_workers: usize,
    build_treemap: bool,
    debug: bool,
) -> PyResult<u64> {
    report_pipeline::build_pipeline_dbs_impl(
        py,
        tmpdir,
        uids_map,
        team_map,
        pipeline_db_path,
        treemap_json,
        treemap_db,
        treemap_root,
        max_level,
        min_size_bytes,
        timestamp,
        max_workers,
        build_treemap,
        debug,
    )
}

#[pymodule]
fn fast_scanner(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_disk, m)?)?;
    m.add_function(wrap_pyfunction!(merge_write_user_report, m)?)?;
    m.add_function(wrap_pyfunction!(build_pipeline_dbs, m)?)?;
    m.add_function(wrap_pyfunction!(build_pipeline, m)?)?;
    Ok(())
}
