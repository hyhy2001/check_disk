use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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

    // Compact data_detail.json and per-user manifest.json in-place
    let detail_root = Path::new(&pipeline_db_path)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("detail_users"));
    let data_detail_path = detail_root.join("data_detail.json");
    let txt = fs::read_to_string(&data_detail_path)
        .map_err(|e| PyRuntimeError::new_err(format!("read data_detail.json: {}", e)))?;
    let v: Value = serde_json::from_str(&txt)
        .map_err(|e| PyRuntimeError::new_err(format!("parse data_detail.json: {}", e)))?;

    let scan = v.get("scan").cloned().unwrap_or(json!({}));
    let mut perm_count_by_user: HashMap<String, u64> = HashMap::new();
    let permission_json = detail_root.join("permission_issues.json");
    if permission_json.exists() {
        if let Ok(txt_perm) = fs::read_to_string(&permission_json) {
            if let Ok(v_perm) = serde_json::from_str::<Value>(&txt_perm) {
                if let Some(items) = v_perm.get("issues").and_then(|x| x.as_array()) {
                    for item in items {
                        if let Some(uid) = item.get("uid").and_then(|x| x.as_u64()) {
                            let uname = uids_map
                                .get(&(uid as u32))
                                .cloned()
                                .unwrap_or_else(|| format!("uid-{}", uid));
                            *perm_count_by_user.entry(uname).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }
    let mut users_out = Vec::<Value>::new();

    if let Some(users) = v.get("users").and_then(|x| x.as_array()) {
        for u in users {
            let username = u.get("username").and_then(|x| x.as_str()).unwrap_or("");
            let manifest_rel = u.get("manifest").and_then(|x| x.as_str()).unwrap_or("");
            users_out.push(json!({
                "username": username,
                "uid": u.get("uid").cloned().unwrap_or(json!(0)),
                "team_id": u.get("team_id").cloned().unwrap_or(json!("")),
                "manifest": manifest_rel,
                "files": u.get("total_files").cloned().unwrap_or(json!(0)),
                "dirs": u.get("total_dirs").cloned().unwrap_or(json!(0)),
                "used": u.get("total_used").cloned().unwrap_or(json!(0)),
                "permission_issues": perm_count_by_user.get(username).copied().unwrap_or(0)
            }));
        }
    }

    let root_manifest = json!({
        "version": 2,
        "schema": "check-disk-detail",
        "scan": {
            "root": scan.get("root").cloned().unwrap_or(json!(treemap_root)),
            "timestamp": scan.get("timestamp").cloned().unwrap_or(json!(timestamp)),
            "total_files": scan.get("total_files").cloned().unwrap_or(json!(total_files)),
            "total_dirs": scan.get("total_dirs").cloned().unwrap_or(json!(0)),
            "total_size": scan.get("total_size").cloned().unwrap_or(json!(0))
        },
        "users": users_out
    });

    fs::write(
        detail_root.join("data_detail.json"),
        serde_json::to_string_pretty(&root_manifest)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
    )
    .map_err(|e| PyRuntimeError::new_err(format!("write data_detail.json: {}", e)))?;

    Ok(total_files)
}
