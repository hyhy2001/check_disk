use pyo3::prelude::*;
use std::collections::HashMap;

mod db_writer;
mod pipe_events;
mod pipe_io;
mod pipe_permission;
mod pipe_treemap;
mod pipe_types;
mod report_pipeline;
mod scan_constants;
mod scan_core;
mod scan_state;
mod scan_utils;

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

#[pyfunction(signature = (directory, skip_dirs, target_uids, max_workers=None, debug=false, apparent_size=false))]
fn scan_disk(
    py: Python,
    directory: String,
    skip_dirs: Vec<String>,
    target_uids: Option<Vec<u32>>,
    max_workers: Option<usize>,
    debug: bool,
    apparent_size: bool,
) -> PyResult<PyObject> {
    scan_core::run_scan_core(
        py,
        directory,
        skip_dirs,
        target_uids,
        max_workers,
        debug,
        "production",
        apparent_size,
    )
}

#[pyfunction(signature = (tmpdir, uids_map, team_map, detail_db_path, treemap_db_path, treemap_root, max_level, min_size_bytes, timestamp, max_workers, build_treemap=true, debug=false))]
#[allow(clippy::too_many_arguments)]
fn build_detail_db(
    py: Python<'_>,
    tmpdir: String,
    uids_map: HashMap<u32, String>,
    team_map: HashMap<String, String>,
    detail_db_path: String,
    treemap_db_path: String,
    treemap_root: String,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    max_workers: usize,
    build_treemap: bool,
    debug: bool,
) -> PyResult<(u64, Option<String>)> {
    // Configure glibc allocator to reduce heap fragmentation during large
    // parallel workloads. M_MMAP_THRESHOLD forces allocations > 128KB to
    // use mmap() which is returned to OS immediately on free().
    // M_TRIM_THRESHOLD triggers heap trimming more aggressively.
    #[cfg(target_os = "linux")]
    unsafe {
        extern "C" {
            fn mallopt(param: i32, value: i32) -> i32;
        }
        mallopt(-3, 128 * 1024); // M_MMAP_THRESHOLD = 128KB
        mallopt(-1, 128 * 1024); // M_TRIM_THRESHOLD = 128KB
    }

    let (count, agg_path) = report_pipeline::build_detail_db_impl(
        py, tmpdir, uids_map, team_map, detail_db_path, treemap_db_path,
        treemap_root, max_level, min_size_bytes, timestamp, max_workers,
        build_treemap, debug,
    )?;
    Ok((count, agg_path.map(|p| p.to_string_lossy().into_owned())))
}

#[pyfunction(signature = (aggregates_path, treemap_db_path, treemap_root, max_level, min_size_bytes, timestamp, debug=false))]
#[allow(clippy::too_many_arguments)]
fn build_treemap_db(
    py: Python<'_>,
    aggregates_path: String,
    treemap_db_path: String,
    treemap_root: String,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    debug: bool,
) -> PyResult<()> {
    py.allow_threads(|| {
        report_pipeline::build_treemap_db_impl(
            std::path::Path::new(&aggregates_path),
            std::path::Path::new(&treemap_db_path),
            &treemap_root,
            max_level,
            min_size_bytes,
            timestamp,
            debug,
        )
    })
}

#[pyfunction(signature = (tmpdir, uids_map, team_map, detail_db_path, treemap_db_path, treemap_root, max_level, min_size_bytes, timestamp, max_workers, build_treemap=true, debug=false))]
#[allow(clippy::too_many_arguments)]
fn build_pipeline(
    py: Python<'_>,
    tmpdir: String,
    uids_map: HashMap<u32, String>,
    team_map: HashMap<String, String>,
    detail_db_path: String,
    treemap_db_path: String,
    treemap_root: String,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    max_workers: usize,
    build_treemap: bool,
    debug: bool,
) -> PyResult<u64> {
    // Configure glibc allocator to reduce heap fragmentation during large
    // parallel workloads. M_MMAP_THRESHOLD forces allocations > 128KB to
    // use mmap() which is returned to OS immediately on free().
    // M_TRIM_THRESHOLD triggers heap trimming more aggressively.
    #[cfg(target_os = "linux")]
    unsafe {
        extern "C" {
            fn mallopt(param: i32, value: i32) -> i32;
        }
        mallopt(-3, 128 * 1024); // M_MMAP_THRESHOLD = 128KB
        mallopt(-1, 128 * 1024); // M_TRIM_THRESHOLD = 128KB
    }

    let (count, agg_path) = report_pipeline::build_detail_db_impl(
        py, tmpdir, uids_map, team_map, detail_db_path.clone(), treemap_db_path.clone(),
        treemap_root.clone(), max_level, min_size_bytes, timestamp, max_workers,
        build_treemap, debug,
    )?;
    if let Some(p) = agg_path {
        report_pipeline::build_treemap_db_impl(
            &p,
            std::path::Path::new(&treemap_db_path),
            &treemap_root,
            max_level, min_size_bytes, timestamp, debug,
        )?;
    }
    Ok(count)
}

#[pymodule]
fn fast_scanner(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(scan_disk, m)?)?;
    m.add_function(wrap_pyfunction!(build_detail_db, m)?)?;
    m.add_function(wrap_pyfunction!(build_treemap_db, m)?)?;
    m.add_function(wrap_pyfunction!(build_pipeline, m)?)?;
    Ok(())
}
