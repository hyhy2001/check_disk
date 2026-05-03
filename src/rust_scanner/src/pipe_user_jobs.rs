use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::pipe_io::safe_user_dir;
use crate::pipe_types::{
    FileChunkJob, FileChunkResult, UserJobMeta, UserOutputMeta, FILE_PART_RECORDS,
};

const BIN_MAGIC: &[u8; 4] = b"CDB4";
const BIN_KIND_FILE: u8 = 0;

fn write_file_part_header(writer: &mut BufWriter<GzEncoder<fs::File>>) -> Result<(), String> {
    writer
        .write_all(BIN_MAGIC)
        .and_then(|_| writer.write_all(&[1u8, BIN_KIND_FILE, 0u8, 0u8]))
        .map_err(|e| format!("write file part header: {}", e))
}

fn write_file_record(
    writer: &mut BufWriter<GzEncoder<fs::File>>,
    path_id: usize,
    size: u64,
    ext: &str,
) -> Result<(), String> {
    let pid = u32::try_from(path_id).map_err(|_| format!("path id too large: {}", path_id))?;
    let ext_bytes = ext.as_bytes();
    let ext_len = u16::try_from(ext_bytes.len())
        .map_err(|_| format!("extension too long: {}", ext))?;
    writer
        .write_all(&pid.to_le_bytes())
        .and_then(|_| writer.write_all(&size.to_le_bytes()))
        .and_then(|_| writer.write_all(&ext_len.to_le_bytes()))
        .and_then(|_| writer.write_all(ext_bytes))
        .map_err(|e| format!("write file record: {}", e))
}

#[allow(dead_code)]
pub fn build_output_jobs(
    detail_root: &Path,
    users: HashMap<String, UserOutputMeta>,
    mut rows_by_user: HashMap<String, Vec<(u64, String)>>,
    team_map: &HashMap<String, String>,
    timestamp: i64,
) -> (Vec<UserJobMeta>, Vec<FileChunkJob>) {
    let mut metas = Vec::new();
    let mut chunk_jobs = Vec::new();
    for (username, user) in users {
        let safe = safe_user_dir(&username);
        let rows = rows_by_user.remove(&username).unwrap_or_default();
        let mut file_paths = Vec::new();
        let mut path_to_id: HashMap<String, usize> = HashMap::new();
        let mut id_rows = Vec::with_capacity(rows.len());
        for (size, raw_path) in rows {
            let safe_path = crate::sanitise_path(&raw_path);
            let ext = crate::pipe_types::extension_for_path(&safe_path);
            let path_id = if let Some(id) = path_to_id.get(&safe_path) {
                *id
            } else {
                let id = file_paths.len();
                file_paths.push(safe_path.clone());
                path_to_id.insert(safe_path, id);
                id
            };
            id_rows.push((size, path_id, ext));
        }
        let tmp_dir = detail_root.join("users").join(format!(".tmp_{}", safe));
        let final_dir = detail_root.join("users").join(&safe);
        if tmp_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
        }
        for (chunk_index, chunk_rows) in id_rows.chunks(FILE_PART_RECORDS).enumerate() {
            chunk_jobs.push(FileChunkJob {
                username: username.clone(),
                chunk_index,
                output_dir: tmp_dir
                    .join("files")
                    .join(format!("chunk-{:05}", chunk_index)),
                rows: chunk_rows.to_vec(),
            });
        }
        let mut top_dirs_idx: Vec<(usize, i64)> = Vec::with_capacity(user.top_dirs.len());
        for (path, used) in user.top_dirs {
            let safe_path = crate::sanitise_path(&path);
            let path_id = if let Some(id) = path_to_id.get(&safe_path) {
                *id
            } else {
                let id = file_paths.len();
                file_paths.push(safe_path.clone());
                path_to_id.insert(safe_path, id);
                id
            };
            top_dirs_idx.push((path_id, used));
        }

        metas.push(UserJobMeta {
            username: username.clone(),
            team_id: if user.team_id.is_empty() {
                team_map.get(&username).cloned().unwrap_or_default()
            } else {
                user.team_id
            },
            final_dir,
            tmp_dir,
            total_dirs: user.total_dirs,
            total_used: user.total_used,
            top_dirs: top_dirs_idx,
            timestamp,
        });
    }
    metas.sort_by(|a, b| a.username.cmp(&b.username));
    chunk_jobs.sort_by(|a, b| {
        a.username
            .cmp(&b.username)
            .then(a.chunk_index.cmp(&b.chunk_index))
    });
    (metas, chunk_jobs)
}

pub fn build_one_file_chunk(job: FileChunkJob) -> Result<FileChunkResult, String> {
    if job.output_dir.exists() {
        fs::remove_dir_all(&job.output_dir)
            .map_err(|e| format!("rm chunk {}: {}", job.output_dir.display(), e))?;
    }
    fs::create_dir_all(&job.output_dir)
        .map_err(|e| format!("mkdir chunk {}: {}", job.output_dir.display(), e))?;

    let mut file_parts: Vec<serde_json::Value> = Vec::new();
    let mut extension_stats: HashMap<String, (i64, i64)> = HashMap::new();
    let mut total_files = 0_i64;
    let mut current_part_idx: Option<usize> = None;
    let mut current_records = 0_usize;
    let mut current_writer: Option<BufWriter<GzEncoder<fs::File>>> = None;

    for (size, path_id, ext) in job.rows {
        if current_writer.is_none() || current_records >= FILE_PART_RECORDS {
            if let Some(mut writer) = current_writer.take() {
                writer
                    .flush()
                    .map_err(|e| format!("flush file part: {}", e))?;
                if let Some(idx) = current_part_idx {
                    file_parts[idx]["records"] = json!(current_records);
                }
            }
            let part_idx = file_parts.len();
            let rel_path = format!(
                "files/chunk-{:05}/part-{:05}.bin.gz",
                job.chunk_index, part_idx
            );
            let file = fs::File::create(job.output_dir.join(format!("part-{:05}.bin.gz", part_idx)))
                .map_err(|e| format!("create {}: {}", rel_path, e))?;
            let encoder = GzEncoder::new(file, Compression::default());
            let mut writer = BufWriter::new(encoder);
            write_file_part_header(&mut writer)?;
            file_parts.push(json!({"path": rel_path, "records": 0}));
            current_part_idx = Some(part_idx);
            current_records = 0;
            current_writer = Some(writer);
        }

        let stat = extension_stats.entry(ext.clone()).or_insert((0, 0));
        stat.0 += 1;
        stat.1 += size as i64;
        let writer = current_writer.as_mut().expect("writer exists");
        write_file_record(writer, path_id, size, &ext)?;
        current_records += 1;
        total_files += 1;
    }

    if let Some(mut writer) = current_writer.take() {
        writer
            .flush()
            .map_err(|e| format!("flush file part: {}", e))?;
        if let Some(idx) = current_part_idx {
            file_parts[idx]["records"] = json!(current_records);
        }
    }

    Ok(FileChunkResult {
        username: job.username,
        chunk_index: job.chunk_index,
        total_files,
        file_parts,
        extension_stats,
    })
}
