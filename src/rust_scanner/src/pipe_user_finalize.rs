use pyo3::prelude::*;
use serde_json::json;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::pipe_io::{json_line_result, safe_user_dir, write_json_file, write_json_file_result};
use crate::pipe_types::{TOP_RECORDS, FileChunkResult, UserBuildResult, UserJobMeta};

fn compact_file_row(path: &str, size: u64, ext: &str) -> serde_json::Value {
    json!({"p": path, "s": size, "x": ext})
}

fn size_bin_key(size: u64) -> &'static str {
    if size <= 1023 {
        "0-1KB"
    } else if size <= 1024 * 1024 - 1 {
        "1KB-1MB"
    } else if size <= 10 * 1024 * 1024 - 1 {
        "1MB-10MB"
    } else if size <= 100 * 1024 * 1024 - 1 {
        "10MB-100MB"
    } else if size <= 1024 * 1024 * 1024 - 1 {
        "100MB-1GB"
    } else {
        ">=1GB"
    }
}

fn path_tokens(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    for seg in path.split('/') {
        let token = seg.trim().to_lowercase();
        if token.len() >= 2 {
            out.push(token);
        }
    }
    out
}

fn compact_dir_row(path: &str, used: i64) -> serde_json::Value {
    json!({"p": path, "s": used})
}

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
        if meta.final_dir.exists() {
            fs::remove_dir_all(&meta.final_dir).map_err(|e| {
                format!(
                    "rm final dir {} before rename: {}",
                    meta.final_dir.display(),
                    e
                )
            })?;
        }
        fs::rename(&meta.tmp_dir, &meta.final_dir)
            .map_err(|e| format!("rename {} -> {}: {}", meta.tmp_dir.display(), meta.final_dir.display(), e))?;

        user_results.push(UserBuildResult {
            username: meta.username.clone(),
            team_id: meta.team_id,
            total_dirs: meta.total_dirs,
            total_files,
            total_used: meta.total_used,
        });

        if metas_count > 1 {
            let done = user_results.len();
            let total = metas_count;
            let percent = (done as f64 / total as f64) * 100.0;
            eprint!(
                "\r[Phase 2] Detail reports: {}/{} users ({:.1}%) ... ",
                done, total, percent
            );
        }
    }
    if metas_count > 1 {
        eprintln!();
    }
    Ok(user_results)
}

fn write_user_dirs_result(user_dir: &Path, top_dirs: &[(String, i64)]) -> Result<(), String> {
    let file = fs::File::create(user_dir.join("dirs.ndjson")).map_err(|e| format!("create dirs.ndjson: {}", e))?;
    let mut writer = BufWriter::new(file);
    for (path, used) in top_dirs {
        json_line_result(&mut writer, compact_dir_row(path, *used))?;
    }
    writer.flush().map_err(|e| format!("flush dirs: {}", e))?;

    let top_dirs_json: Vec<_> = top_dirs.iter().take(TOP_RECORDS)
        .map(|(path, used)| compact_dir_row(path, *used))
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
        .map(|(size, path)| compact_file_row(path, *size, &crate::pipe_types::extension_for_path(path)))
        .collect();
    write_json_file_result(&meta.tmp_dir.join("top_files.json"), &json!(top_files_json))?;

    let mut ext_index: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    let mut size_bins: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    let mut token_index: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    for (size, path) in &top_files_vec {
        let ext = crate::pipe_types::extension_for_path(path);
        let row = compact_file_row(path, *size, &ext);
        ext_index.entry(ext.clone()).or_default().push(row.clone());
        size_bins
            .entry(size_bin_key(*size).to_string())
            .or_default()
            .push(row.clone());
        for tok in path_tokens(path).into_iter().take(8) {
            token_index.entry(tok).or_default().push(row.clone());
        }
    }
    write_json_file_result(&meta.tmp_dir.join("ext_index.json"), &json!(ext_index))?;
    write_json_file_result(&meta.tmp_dir.join("size_bins.json"), &json!(size_bins))?;
    write_json_file_result(&meta.tmp_dir.join("path_token_index.json"), &json!(token_index))?;

    let ui_dirs = std::cmp::min(meta.total_dirs.max(0) as usize, TOP_RECORDS) as i64;
    let ui_files = std::cmp::min(total_files.max(0) as usize, TOP_RECORDS) as i64;
    let page_size: i64 = 500;
    let dirs_pages = std::cmp::max(1, (ui_dirs + page_size - 1) / page_size);
    let files_pages = std::cmp::max(1, (ui_files + page_size - 1) / page_size);

    write_json_file_result(&meta.tmp_dir.join("page_index.json"), &json!({
        "version": 1,
        "page_size": page_size,
        "dirs": { "total_full": meta.total_dirs, "total_ui": ui_dirs, "pages": dirs_pages },
        "files": { "total_full": total_files, "total_ui": ui_files, "pages": files_pages }
    }))?;

    let mut exts: Vec<_> = extension_stats.iter()
        .map(|(ext, (count, size))| json!({"ext": ext, "count": count, "size": size}))
        .collect();
    exts.sort_by(|a, b| b["size"].as_i64().unwrap_or(0).cmp(&a["size"].as_i64().unwrap_or(0)));
    write_json_file_result(&meta.tmp_dir.join("extensions.json"), &json!(exts))?;

    let manifest = json!({
        "schema": "check-disk-user",
        "username": meta.username,
        "team_id": meta.team_id,
        "scan_date": meta.timestamp,
        "summary": {
            "files": total_files,
            "dirs": meta.total_dirs,
            "used": meta.total_used,
            "total_files": total_files,
            "total_dirs": meta.total_dirs,
            "total_used": meta.total_used,
            "ui_files": ui_files,
            "ui_dirs": ui_dirs
        },
        "page_index": "page_index.json",
        "top_files": "top_files.json",
        "top_dirs": "top_dirs.json",
        "extensions": "extensions.json",
        "ext_index": "ext_index.json",
        "size_bins": "size_bins.json",
        "path_token_index": "path_token_index.json",
        "files": {"parts": file_parts},
        "dirs": {"path": "dirs.ndjson"}
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
        "version": 2,
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
