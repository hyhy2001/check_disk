use flate2::write::GzEncoder;
use flate2::Compression;
use pyo3::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::pipe_binary::crc32c_of_files;
use crate::pipe_io::{write_json_file, write_json_file_result};
use crate::pipe_types::{FileChunkResult, FILE_PART_RECORDS, UserBuildResult, UserJobMeta};

const BIN_MAGIC: &[u8; 4] = b"CDB4";
const BIN_KIND_DIR: u8 = 1;

fn write_dir_part_header(writer: &mut BufWriter<GzEncoder<fs::File>>) -> Result<(), String> {
    writer
        .write_all(BIN_MAGIC)
        .and_then(|_| writer.write_all(&[1u8, BIN_KIND_DIR, 0u8, 0u8]))
        .map_err(|e| format!("write dir part header: {}", e))
}

fn write_dir_record(
    writer: &mut BufWriter<GzEncoder<fs::File>>,
    path_id: usize,
    used: i64,
) -> Result<(), String> {
    let pid = u32::try_from(path_id).map_err(|_| format!("path id too large: {}", path_id))?;
    writer
        .write_all(&pid.to_le_bytes())
        .and_then(|_| writer.write_all(&used.to_le_bytes()))
        .map_err(|e| format!("write dir record: {}", e))
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
        let mut total_files = 0_i64;

        let dir_parts = write_user_dirs_result(&meta.tmp_dir, &meta.top_dirs)?;

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
        }

        write_user_metadata_result(
            &meta,
            total_files,
            file_parts,
            dir_parts,
            extension_stats,
            "../../index_seed/paths.bin",
        )?;
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

fn write_user_dirs_result(user_dir: &Path, top_dirs: &[(usize, i64)]) -> Result<Vec<serde_json::Value>, String> {
    let dirs_dir = user_dir.join("dirs");
    fs::create_dir_all(&dirs_dir).map_err(|e| format!("create dirs dir: {}", e))?;
    let mut dir_parts: Vec<serde_json::Value> = Vec::new();
    let mut chunk_idx = 0usize;
    let mut part_idx = 0usize;
    let mut current_part_records = 0usize;
    let mut writer: Option<BufWriter<GzEncoder<fs::File>>> = None;

    for (path_id, used) in top_dirs {
        if writer.is_none() || current_part_records >= FILE_PART_RECORDS {
            if let Some(mut w) = writer.take() {
                w.flush().map_err(|e| format!("flush dir part: {}", e))?;
                if !dir_parts.is_empty() {
                    let last_idx = dir_parts.len() - 1;
                    dir_parts[last_idx]["records"] = json!(current_part_records);
                }
            }

            let chunk_dir = dirs_dir.join(format!("chunk-{:05}", chunk_idx));
            fs::create_dir_all(&chunk_dir)
                .map_err(|e| format!("create dir chunk {}: {}", chunk_dir.display(), e))?;

            let rel_path = format!("dirs/chunk-{:05}/part-{:05}.bin.gz", chunk_idx, part_idx);
            let part_path = chunk_dir.join(format!("part-{:05}.bin.gz", part_idx));
            let file = fs::File::create(&part_path)
                .map_err(|e| format!("create dir part {}: {}", part_path.display(), e))?;
            let encoder = GzEncoder::new(file, Compression::default());
            let mut part_writer = BufWriter::new(encoder);
            write_dir_part_header(&mut part_writer)?;
            writer = Some(part_writer);
            dir_parts.push(json!({"path": rel_path, "records": 0}));
            current_part_records = 0;
            chunk_idx += 1;
            part_idx = 0;
        }

        let w = writer.as_mut().unwrap();
        write_dir_record(w, *path_id, *used)?;
        current_part_records += 1;
    }

    if let Some(mut w) = writer {
        w.flush().map_err(|e| format!("flush final dir part: {}", e))?;
        if !dir_parts.is_empty() {
            let last_idx = dir_parts.len() - 1;
            dir_parts[last_idx]["records"] = json!(current_part_records);
        }
    }

    Ok(dir_parts)
}

fn write_user_metadata_result(
    meta: &UserJobMeta,
    total_files: i64,
    file_parts: Vec<serde_json::Value>,
    dir_parts: Vec<serde_json::Value>,
    extension_stats: HashMap<String, (i64, i64)>,
    global_paths_rel: &str,
) -> Result<(), String> {
    let page_size: i64 = 500;
    let dirs_pages = std::cmp::max(1, (meta.total_dirs.max(0) + page_size - 1) / page_size);
    let files_pages = std::cmp::max(1, (total_files.max(0) + page_size - 1) / page_size);

    write_json_file_result(&meta.tmp_dir.join("page_index.json"), &json!({
        "page_size": page_size,
        "dirs": { "total_full": meta.total_dirs, "pages": dirs_pages },
        "files": { "total_full": total_files, "pages": files_pages }
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
            "used": meta.total_used
        },
        "page_index": "page_index.json",
        "extensions": "extensions.json",
        "paths_dict": global_paths_rel,
        "files": {"parts": file_parts},
        "dirs": {"parts": dir_parts}
    });
    write_json_file_result(&meta.tmp_dir.join("manifest.json"), &manifest)
}

pub fn write_detail_manifest(
    detail_root: &Path,
    users: &[UserBuildResult],
    root: &str,
    timestamp: i64,
    total_files: i64,
    total_paths: usize,
    total_exts: usize,
    total_docs: usize,
    total_uid_totals: usize,
    total_dir_user_sizes: usize,
    total_perm_events: usize,
) -> PyResult<()> {
    let total_size: i64 = users.iter().map(|u| u.total_used).sum();
    let total_dirs: i64 = users.iter().map(|u| u.total_dirs).sum();

    let file_bytes = |rel: &str| -> u64 {
        fs::metadata(detail_root.join(rel)).map(|m| m.len()).unwrap_or(0)
    };

    let checksum_files = [
        "dict/paths.dict.bin",
        "dict/paths.parent.bin",
        "dict/exts.dict.bin",
        "dict/users.dict.bin",
        "cols/doc.uid.bin",
        "cols/doc.path_id.bin",
        "cols/doc.size.bin",
        "cols/doc.ext_id.bin",
        "agg/uid_totals.bin",
        "agg/dir_user_sizes.bin",
        "agg/perm_events.bin",
    ];
    let checksum = crc32c_of_files(detail_root, &checksum_files)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

    let manifest = json!({
        "schema": "check-disk-detail",
        "version": 1,
        "created_at": timestamp,
        "total_docs": total_docs,
        "total_users": users.len(),
        "total_paths": total_paths,
        "files": {
            "dict/paths.dict.bin": {"bytes": file_bytes("dict/paths.dict.bin"), "records": total_paths},
            "dict/paths.parent.bin": {"bytes": file_bytes("dict/paths.parent.bin"), "records": total_paths},
            "dict/exts.dict.bin": {"bytes": file_bytes("dict/exts.dict.bin"), "records": total_exts},
            "dict/users.dict.bin": {"bytes": file_bytes("dict/users.dict.bin"), "records": users.len()},
            "cols/doc.uid.bin": {"bytes": file_bytes("cols/doc.uid.bin"), "records": total_docs},
            "cols/doc.path_id.bin": {"bytes": file_bytes("cols/doc.path_id.bin"), "records": total_docs},
            "cols/doc.size.bin": {"bytes": file_bytes("cols/doc.size.bin"), "records": total_docs},
            "cols/doc.ext_id.bin": {"bytes": file_bytes("cols/doc.ext_id.bin"), "records": total_docs},
            "agg/uid_totals.bin": {"bytes": file_bytes("agg/uid_totals.bin"), "records": total_uid_totals},
            "agg/dir_user_sizes.bin": {"bytes": file_bytes("agg/dir_user_sizes.bin"), "records": total_dir_user_sizes},
            "agg/perm_events.bin": {"bytes": file_bytes("agg/perm_events.bin"), "records": total_perm_events},
            "users/": {"count": users.len(), "record_encoding": "binary-v1"}
        },
        "checksum": checksum
    });
    write_json_file(&detail_root.join("manifest.json"), &manifest)?;

    let api_dir = detail_root.join("api");
    fs::create_dir_all(&api_dir)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("mkdir {}: {}", api_dir.display(), e)))?;

    let min_users: Vec<_> = users
        .iter()
        .enumerate()
        .map(|(idx, user)| {
            json!({
                "uid_id": idx,
                "username": user.username,
                "team_id": user.team_id,
                "total_files": user.total_files,
                "total_dirs": user.total_dirs,
                "total_bytes": user.total_used,
            })
        })
        .collect();

    let data_detail_min = json!({
        "scan": {
            "timestamp": timestamp,
            "root": root,
            "total_files": total_files,
            "total_dirs": total_dirs,
            "total_size": total_size
        },
        "totals": {
            "users": users.len(),
            "docs": total_docs,
            "paths": total_paths
        },
        "users": min_users
    });
    write_json_file(&api_dir.join("data_detail.min.json"), &data_detail_min)?;

    let users_index: Vec<_> = users
        .iter()
        .enumerate()
        .map(|(idx, user)| {
            json!({
                "uid_id": idx,
                "file": format!("users/u_{:04}.bin", idx + 1),
                "username": user.username,
                "team_id": user.team_id,
                "total_files": user.total_files,
                "total_bytes": user.total_used,
                "total_dirs": user.total_dirs,
            })
        })
        .collect();
    write_json_file(&api_dir.join("users_index.min.json"), &json!(users_index))
}
