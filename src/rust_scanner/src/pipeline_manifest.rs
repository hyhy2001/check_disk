use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

fn to_compact_file_row(v: &Value) -> Value {
    json!({
        "p": v.get("path").and_then(|x| x.as_str()).unwrap_or(""),
        "s": v.get("size").and_then(|x| x.as_i64()).unwrap_or(0),
        "x": v.get("ext").or_else(|| v.get("xt")).and_then(|x| x.as_str()).unwrap_or("")
    })
}

fn to_compact_dir_row(v: &Value) -> Value {
    json!({
        "p": v.get("path").and_then(|x| x.as_str()).unwrap_or(""),
        "s": v.get("used").and_then(|x| x.as_i64()).unwrap_or(0)
    })
}

fn compact_user_manifest(user_manifest_path: &Path) -> Result<(), String> {
    let txt = fs::read_to_string(user_manifest_path).map_err(|e| e.to_string())?;
    let manifest: Value = serde_json::from_str(&txt).map_err(|e| e.to_string())?;
    let user_dir = user_manifest_path.parent().ok_or("invalid user manifest path")?;

    let username = manifest.get("username").and_then(|v| v.as_str()).unwrap_or("");
    let scan_date = manifest.get("scan_date").and_then(|v| v.as_i64()).unwrap_or(0);

    // Convert top files to compact keys in-place for production
    if let Some(top_files_rel) = manifest.get("top_files").and_then(|v| v.as_str()) {
        let top_files_path = user_dir.join(top_files_rel);
        if top_files_path.exists() {
            let top_txt = fs::read_to_string(&top_files_path).map_err(|e| e.to_string())?;
            let arr: Value = serde_json::from_str(&top_txt).map_err(|e| e.to_string())?;
            if let Some(items) = arr.as_array() {
                let compact: Vec<Value> = items.iter().map(to_compact_file_row).collect();
                fs::write(&top_files_path, serde_json::to_string(&compact).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            }
        }
    }

    if let Some(top_dirs_rel) = manifest.get("top_dirs").and_then(|v| v.as_str()) {
        let top_dirs_path = user_dir.join(top_dirs_rel);
        if top_dirs_path.exists() {
            let top_txt = fs::read_to_string(&top_dirs_path).map_err(|e| e.to_string())?;
            let arr: Value = serde_json::from_str(&top_txt).map_err(|e| e.to_string())?;
            if let Some(items) = arr.as_array() {
                let compact: Vec<Value> = items.iter().map(to_compact_dir_row).collect();
                fs::write(&top_dirs_path, serde_json::to_string(&compact).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            }
        }
    }

    // Convert NDJSON parts for files to compact format
    if let Some(files) = manifest.get("files") {
        if let Some(parts) = files.get("parts").and_then(|v| v.as_array()) {
            for p in parts {
                if let Some(rel) = p.get("path").and_then(|v| v.as_str()) {
                    let part_path = user_dir.join(rel);
                    if part_path.exists() {
                        let input = fs::File::open(&part_path).map_err(|e| e.to_string())?;
                        let reader = BufReader::new(input);
                        let mut out = String::new();
                        for line in reader.lines() {
                            let line = line.map_err(|e| e.to_string())?;
                            if line.trim().is_empty() { continue; }
                            let v: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
                            let c = to_compact_file_row(&v);
                            out.push_str(&serde_json::to_string(&c).map_err(|e| e.to_string())?);
                            out.push('\n');
                        }
                        fs::write(&part_path, out).map_err(|e| e.to_string())?;
                    }
                }
            }
        }
    }

    // Convert dirs.ndjson
    if let Some(dirs) = manifest.get("dirs") {
        if let Some(rel) = dirs.get("path").and_then(|v| v.as_str()) {
            let dirs_path = user_dir.join(rel);
            if dirs_path.exists() {
                let input = fs::File::open(&dirs_path).map_err(|e| e.to_string())?;
                let reader = BufReader::new(input);
                let mut out = String::new();
                for line in reader.lines() {
                    let line = line.map_err(|e| e.to_string())?;
                    if line.trim().is_empty() { continue; }
                    let v: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
                    let c = to_compact_dir_row(&v);
                    out.push_str(&serde_json::to_string(&c).map_err(|e| e.to_string())?);
                    out.push('\n');
                }
                fs::write(&dirs_path, out).map_err(|e| e.to_string())?;
            }
        }
    }

    let summary = manifest.get("summary").cloned().unwrap_or(json!({}));
    let team_id = manifest.get("team_id").cloned().unwrap_or(json!(""));

    let compact_manifest = json!({
        "schema": "check-disk-user",
        "username": username,
        "team_id": team_id,
        "scan_date": scan_date,
        "summary": {
            "files": summary.get("total_files").and_then(|v| v.as_i64()).unwrap_or(0),
            "dirs": summary.get("total_dirs").and_then(|v| v.as_i64()).unwrap_or(0),
            "used": summary.get("total_used").and_then(|v| v.as_i64()).unwrap_or(0)
        },
        "top_files": manifest.get("top_files").cloned().unwrap_or(json!("top_files.json")),
        "top_dirs": manifest.get("top_dirs").cloned().unwrap_or(json!("top_dirs.json")),
        "extensions": manifest.get("extensions").cloned().unwrap_or(json!("extensions.json")),
        "files": {
            "parts": manifest.get("files").and_then(|v| v.get("parts")).cloned().unwrap_or(json!([]))
        },
        "dirs": manifest.get("dirs").cloned().unwrap_or(json!({"path":"dirs.ndjson"}))
    });

    fs::write(user_manifest_path, serde_json::to_string_pretty(&compact_manifest).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;

    Ok(())
}

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
    debug: bool,
) -> PyResult<u64> {
    // Run pipeline DB builder, then compact outputs to production detail schema.
    let fast_scanner = py.import("src.fast_scanner").or_else(|_| py.import("fast_scanner"))?;
    let f = fast_scanner
        .getattr("build_pipeline_dbs")
        .or_else(|_| fast_scanner.getattr("build_pipeline_dbs"))?;
    let total_files: u64 = f
        .call1((
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
            debug,
        ))?
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
        let total_users = users.len();
        for (idx, u) in users.iter().enumerate() {
            let username = u.get("username").and_then(|x| x.as_str()).unwrap_or("");
            let manifest_rel = u.get("manifest").and_then(|x| x.as_str()).unwrap_or("");
            let user_manifest_path = detail_root.join(manifest_rel);
            if user_manifest_path.exists() {
                compact_user_manifest(&user_manifest_path)
                    .map_err(|e| PyRuntimeError::new_err(format!("compact user manifest: {}", e)))?;
            }
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

            let done = idx + 1;
            if done % 20 == 0 || done == total_users {
                eprintln!("[Phase 2] Manifest compact: {}/{} users", done, total_users);
            }
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
        serde_json::to_string_pretty(&root_manifest).map_err(|e| PyRuntimeError::new_err(e.to_string()))?,
    )
    .map_err(|e| PyRuntimeError::new_err(format!("write data_detail.json: {}", e)))?;

    Ok(total_files)
}
