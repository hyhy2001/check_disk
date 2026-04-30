use serde_json::json;
use rayon::slice::ParallelSliceMut;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::pipe_io::{json_line_result, safe_user_dir};
use crate::pipe_types::{FILE_PART_RECORDS, FileChunkJob, FileChunkResult, UserJobMeta, UserOutputMeta};

fn compact_file_row(path: &str, size: u64, ext: &str) -> serde_json::Value {
    json!({"p": path, "s": size, "x": ext})
}

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
        let mut rows = rows_by_user.remove(&username).unwrap_or_default();
        rows.par_sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let tmp_dir = detail_root.join("users").join(format!(".tmp_{}", safe));
        let final_dir = detail_root.join("users").join(&safe);
        if tmp_dir.exists() {
            let _ = fs::remove_dir_all(&tmp_dir);
        }
        for (chunk_index, chunk_rows) in rows.chunks(FILE_PART_RECORDS).enumerate() {
            chunk_jobs.push(FileChunkJob {
                username: username.clone(),
                chunk_index,
                output_dir: tmp_dir.join("files").join(format!("chunk-{:05}", chunk_index)),
                rows: chunk_rows.to_vec(),
            });
        }
        metas.push(UserJobMeta {
            username: username.clone(),
            team_id: if user.team_id.is_empty() { team_map.get(&username).cloned().unwrap_or_default() } else { user.team_id },
            final_dir,
            tmp_dir,
            total_dirs: user.total_dirs,
            total_used: user.total_used,
            top_dirs: user.top_dirs,
            timestamp,
        });
    }
    metas.sort_by(|a, b| a.username.cmp(&b.username));
    chunk_jobs.sort_by(|a, b| a.username.cmp(&b.username).then(a.chunk_index.cmp(&b.chunk_index)));
    (metas, chunk_jobs)
}

pub fn build_one_file_chunk(job: FileChunkJob) -> Result<FileChunkResult, String> {
    if job.output_dir.exists() {
        fs::remove_dir_all(&job.output_dir).map_err(|e| format!("rm chunk {}: {}", job.output_dir.display(), e))?;
    }
    fs::create_dir_all(&job.output_dir).map_err(|e| format!("mkdir chunk {}: {}", job.output_dir.display(), e))?;

    let mut file_parts: Vec<serde_json::Value> = Vec::new();
    let mut extension_stats: HashMap<String, (i64, i64)> = HashMap::new();
    let mut top_files: BinaryHeap<Reverse<(u64, String)>> = BinaryHeap::new();
    let mut total_files = 0_i64;
    let mut current_part_idx: Option<usize> = None;
    let mut current_records = 0_usize;
    let mut current_writer: Option<BufWriter<fs::File>> = None;

    for (size, raw_path) in job.rows {
        if current_writer.is_none() || current_records >= FILE_PART_RECORDS {
            if let Some(mut writer) = current_writer.take() {
                writer.flush().map_err(|e| format!("flush file part: {}", e))?;
                if let Some(idx) = current_part_idx {
                    file_parts[idx]["records"] = json!(current_records);
                }
            }
            let part_idx = file_parts.len();
            let rel_path = format!("files/chunk-{:05}/part-{:05}.ndjson", job.chunk_index, part_idx);
            let file = fs::File::create(job.output_dir.join(format!("part-{:05}.ndjson", part_idx)))
                .map_err(|e| format!("create {}: {}", rel_path, e))?;
            file_parts.push(json!({"path": rel_path, "records": 0}));
            current_part_idx = Some(part_idx);
            current_records = 0;
            current_writer = Some(BufWriter::new(file));
        }

        let safe = crate::sanitise_path(&raw_path);
        let ext = crate::pipe_types::extension_for_path(&safe);
        let stat = extension_stats.entry(ext.clone()).or_insert((0, 0));
        stat.0 += 1;
        stat.1 += size as i64;
        crate::pipe_types::push_top_file(&mut top_files, size, &safe);
        json_line_result(current_writer.as_mut().expect("writer exists"), compact_file_row(&safe, size, &ext))?;
        current_records += 1;
        total_files += 1;
    }

    if let Some(mut writer) = current_writer.take() {
        writer.flush().map_err(|e| format!("flush file part: {}", e))?;
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
        top_files: top_files.into_iter().map(|Reverse(item)| item).collect(),
    })
}
