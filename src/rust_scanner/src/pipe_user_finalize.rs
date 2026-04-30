use pyo3::prelude::*;
use serde_json::json;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::pipe_io::{json_line_result, safe_user_dir, write_json_file, write_json_file_result};
use crate::pipe_types::{TOP_RECORDS, FileChunkResult, UserBuildResult, UserJobMeta};

pub fn finalize_user_outputs(
    user_metas: Vec<UserJobMeta>,
    chunk_results: Vec<FileChunkResult>,
) -> Result<Vec<UserBuildResult>, String> {
    let mut chunks_by_user: HashMap<String, Vec<FileChunkResult>> = HashMap::new();
    for chunk in chunk_results {
        chunks_by_user.entry(chunk.username.clone()).or_default().push(chunk);
    }

    let metas_count = user_metas.len();
    let mut user_results = Vec::with_capacity(metas_count);

    for meta in user_metas {
        if meta.tmp_dir.exists() {
            fs::remove_dir_all(&meta.tmp_dir)
                .map_err(|e| format!("rm tmp dir {}: {}", meta.tmp_dir.display(), e))?;
        }
        fs::create_dir_all(&meta.tmp_dir).map_err(|e| format!("mkdir tmp dir {}: {}", meta.tmp_dir.display(), e))?;
        fs::create_dir_all(meta.tmp_dir.join("files")).map_err(|e| format!("mkdir files dir: {}", e))?;

        let mut user_chunks = chunks_by_user.remove(&meta.username).unwrap_or_default();
        user_chunks.sort_by(|a, b| a.chunk_index.cmp(&b.chunk_index));

        let mut file_parts = Vec::new();
        let mut extension_stats: HashMap<String, (i64, i64)> = HashMap::new();
        let mut top_files: BinaryHeap<Reverse<(u64, String)>> = BinaryHeap::new();
        let mut total_files = 0_i64;

        write_user_dirs_result(&meta.tmp_dir, &meta.top_dirs)?;

        for chunk in user_chunks {
            total_files += chunk.total_files;
            for part in chunk.file_parts {
                file_parts.push(part);
            }
            for (ext, (count, size)) in chunk.extension_stats {
                let stat = extension_stats.entry(ext).or_insert((0, 0));
                stat.0 += count;
                stat.1 += size;
            }
            for (size, path) in chunk.top_files {
                crate::pipe_types::push_top_file(&mut top_files, size, &path);
            }
        }

        write_user_metadata_result(&meta, total_files, file_parts, extension_stats, top_files)?;
        fs::rename(&meta.tmp_dir, &meta.final_dir)
            .map_err(|e| format!("rename {} -> {}: {}", meta.tmp_dir.display(), meta.final_dir.display(), e))?;

        user_results.push(UserBuildResult {
            username: meta.username.clone(),
            team_id: meta.team_id,
            total_dirs: meta.total_dirs,
            total_files,
            total_used: meta.total_used,
        });

        let done = user_results.len();
        let total = metas_count;
        let percent = (done as f64 / total as f64) * 100.0;
        eprint!("\r[Phase 2] Detail reports: {}/{} users ({:.1}%) ... ", done, total, percent);
    }
    eprintln!();
    Ok(user_results)
}

fn write_user_dirs_result(user_dir: &Path, top_dirs: &[(String, i64)]) -> Result<(), String> {
    let file = fs::File::create(user_dir.join("dirs.ndjson")).map_err(|e| format!("create dirs.ndjson: {}", e))?;
    let mut writer = BufWriter::new(file);
    for (path, used) in top_dirs {
        json_line_result(&mut writer, json!({"path": path, "used": used}))?;
    }
    writer.flush().map_err(|e| format!("flush dirs: {}", e))?;

    let top_dirs_json: Vec<_> = top_dirs.iter().take(TOP_RECORDS)
        .map(|(path, used)| json!({"path": path, "used": used}))
        .collect();
    write_json_file_result(&user_dir.join("top_dirs.json"), &json!(top_dirs_json))
}

fn write_user_metadata_result(
    meta: &UserJobMeta,
    total_files: i64,
    file_parts: Vec<serde_json::Value>,
    extension_stats: HashMap<String, (i64, i64)>,
    top_files: BinaryHeap<Reverse<(u64, String)>>,
) -> Result<(), String> {
    let mut top_files_vec: Vec<(u64, String)> = top_files.into_iter().map(|Reverse(item)| item).collect();
    top_files_vec.sort_by(|a, b| b.0.cmp(&a.0));
    let top_files_json: Vec<_> = top_files_vec.iter()
        .map(|(size, path)| json!({"path": path, "size": size, "ext": crate::pipe_types::extension_for_path(path)}))
        .collect();
    write_json_file_result(&meta.tmp_dir.join("top_files.json"), &json!(top_files_json))?;

    let mut exts: Vec<_> = extension_stats.iter()
        .map(|(ext, (count, size))| json!({"ext": ext, "count": count, "size": size}))
        .collect();
    exts.sort_by(|a, b| b["size"].as_i64().unwrap_or(0).cmp(&a["size"].as_i64().unwrap_or(0)));
    write_json_file_result(&meta.tmp_dir.join("extensions.json"), &json!(exts))?;

    let manifest = json!({
        "version": 1,
        "username": meta.username,
        "team_id": meta.team_id,
        "scan_date": meta.timestamp,
        "summary": {
            "total_files": total_files,
            "total_dirs": meta.total_dirs,
            "total_used": meta.total_used
        },
        "dirs": {"path": "dirs.ndjson", "sort": "size_desc"},
        "files": {"sort": "chunk_size_desc", "parts": file_parts},
        "top_files": "top_files.json",
        "top_dirs": "top_dirs.json",
        "extensions": "extensions.json"
    });
    write_json_file_result(&meta.tmp_dir.join("manifest.json"), &manifest)
}

pub fn write_detail_manifest(
    detail_root: &Path,
    users: &[UserBuildResult],
    root: &str,
    timestamp: i64,
    total_files: i64,
) -> PyResult<()> {
    let user_entries: Vec<_> = users.iter().map(|user| {
        json!({
            "username": user.username,
            "team_id": user.team_id,
            "total_files": user.total_files,
            "total_dirs": user.total_dirs,
            "total_used": user.total_used,
            "manifest": format!("users/{}/manifest.json", safe_user_dir(&user.username))
        })
    }).collect();
    let total_size: i64 = users.iter().map(|u| u.total_used).sum();
    let total_dirs: i64 = users.iter().map(|u| u.total_dirs).sum();
    let manifest = json!({
        "version": 1,
        "format": "check-disk-detail-ndjson",
        "scan": {
            "timestamp": timestamp,
            "root": root,
            "total_files": total_files,
            "total_dirs": total_dirs,
            "total_size": total_size
        },
        "users": user_entries
    });
    write_json_file(&detail_root.join("data_detail.json"), &manifest)
}
