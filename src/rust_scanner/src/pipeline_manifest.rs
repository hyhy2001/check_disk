use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

pub fn build_pipeline_impl(
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
    // Run pipeline DB builder, then build compact root manifest metadata.
    let fast_scanner = py
        .import("src.fast_scanner")
        .or_else(|_| py.import("fast_scanner"))?;
    let f = fast_scanner
        .getattr("build_pipeline_dbs")
        .or_else(|_| fast_scanner.getattr("build_pipeline_dbs"))?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("build_treemap", build_treemap)?;
    kwargs.set_item("debug", debug)?;
    let total_files: u64 = f
        .call(
            (
                tmpdir.clone(),
                uids_map.clone(),
                team_map.clone(),
                pipeline_db_path.clone(),
                treemap_json,
                treemap_db,
                treemap_root.clone(),
                max_level,
                min_size_bytes,
                timestamp,
                max_workers,
            ),
            Some(kwargs),
        )?
        .extract()?;

    Ok(total_files)
}
