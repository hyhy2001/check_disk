use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::pipe_events::{
    for_each_dir_agg_in_file, for_each_scan_event_in_file, get_dir_agg_files, get_scan_event_files,
    read_permission_events,
};
use crate::pipe_io::{ensure_dir, recreate_dir, safe_user_dir, swap_dir_atomic};
use crate::pipe_permission::write_permission_issues_json;
use crate::pipe_treemap::write_treemap_json_outputs;
use crate::pipe_types::{UserOutputMeta, FILE_PART_RECORDS};

const ROW_SPILL_THRESHOLD: usize = 200_000;

fn cleanup_stale_build_dirs(parent: &Path, prefix: &str, active_path: &Path) {
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == active_path || !path.is_dir() {
            continue;
        }
        let name_ok = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with(prefix))
            .unwrap_or(false);
        if name_ok {
            let _ = fs::remove_dir_all(path);
        }
    }
}

fn spill_rows_to_disk(
    spool_root: &Path,
    seq: &Arc<AtomicU64>,
    rows_by_user: &mut HashMap<String, Vec<(u64, String, String)>>,
    row_spills: &mut HashMap<String, Vec<PathBuf>>,
) -> u64 {
    if rows_by_user.is_empty() {
        return 0;
    }
    let mut drained = HashMap::new();
    let mut bytes_written = 0u64;
    std::mem::swap(rows_by_user, &mut drained);
    for (username, rows) in drained {
        if rows.is_empty() {
            continue;
        }
        let safe = safe_user_dir(&username);
        let user_spills = row_spills.entry(username).or_default();
        for rows_chunk in rows.chunks(FILE_PART_RECORDS) {
            let id = seq.fetch_add(1, Ordering::Relaxed);
            let path = spool_root.join(format!("{}_{}.rows", safe, id));
            let file = match OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
            {
                Ok(f) => f,
                Err(_) => continue,
            };
            let mut writer = BufWriter::with_capacity(1024 * 1024, file);
            let mut write_ok = true;
            for (size, safe_path, ext) in rows_chunk {
                let path_bytes = safe_path.as_bytes();
                let path_len = u32::try_from(path_bytes.len()).unwrap_or(u32::MAX);
                let ext_bytes = ext.as_bytes();
                let ext_len = u16::try_from(ext_bytes.len()).unwrap_or(u16::MAX);
                if writer.write_all(&size.to_le_bytes()).is_err()
                    || writer.write_all(&path_len.to_le_bytes()).is_err()
                    || writer
                        .write_all(&path_bytes[..(path_len as usize).min(path_bytes.len())])
                        .is_err()
                    || writer.write_all(&ext_len.to_le_bytes()).is_err()
                    || writer
                        .write_all(&ext_bytes[..(ext_len as usize).min(ext_bytes.len())])
                        .is_err()
                {
                    write_ok = false;
                    break;
                }
            }
            if write_ok && writer.flush().is_ok() {
                if let Ok(meta) = fs::metadata(&path) {
                    bytes_written += meta.len();
                }
                user_spills.push(path);
            } else {
                let _ = fs::remove_file(path);
            }
        }
    }
    bytes_written
}

fn path_hash(path: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}

fn ensure_path_id(
    path: &str,
    global_path_to_id: &mut HashMap<u64, usize>,
    global_parent_path_id: &mut Vec<u32>,
    next_gid: &mut usize,
    path_dict_writer: &mut BufWriter<fs::File>,
    seek_offsets: &mut Vec<(u64, u32)>,
    ndjson_offset: &mut u64,
) -> Result<usize, String> {
    if let Some(id) = global_path_to_id.get(&path_hash(path)) {
        return Ok(*id);
    }

    let mut missing_paths: Vec<&str> = Vec::new();
    let mut current = path;
    loop {
        if let Some(id) = global_path_to_id.get(&path_hash(current)) {
            let mut parent_id = *id;
            for item in missing_paths.iter().rev() {
                let next_id = *next_gid;
                let item_string = (*item).to_string();
                let row = serde_json::to_vec(&json!({"gid": next_id as u32, "p": item_string}))
                    .map_err(|e| e.to_string())?;
                let row_len = (row.len() + 1) as u32;
                seek_offsets.push((*ndjson_offset, row_len));
                *ndjson_offset += row_len as u64;
                path_dict_writer.write_all(&row).map_err(|e| e.to_string())?;
                path_dict_writer.write_all(b"\n").map_err(|e| e.to_string())?;
                global_parent_path_id.push(parent_id as u32);
                global_path_to_id.insert(path_hash(&item_string), next_id);
                *next_gid += 1;
                parent_id = next_id;
            }
            return Ok(parent_id);
        }

        missing_paths.push(current);
        let trimmed = current.trim_end_matches('/');
        current = match trimmed.rfind('/') {
            Some(0) => "/",
            Some(idx) => &trimmed[..idx],
            None => "/",
        };
    }
}

struct UserFileArtifacts {
    file_parts: Vec<serde_json::Value>,
    ext_rows: Vec<serde_json::Value>,
    total_files: usize,
    files_sorted_by: String,
}

fn write_text_outputs(
    detail_root: &Path,
    _tree_root: &Path,
    users_dict: &[String],
    team_map: &HashMap<String, String>,
    user_file_artifacts: &HashMap<u32, UserFileArtifacts>,
    total_docs: u64,
    total_size: u64,
    dir_user_rows: &[(u32, u32, u64)],
    uid_totals_map: &HashMap<u32, (u64, u64, u64)>,
    timestamp: i64,
    path_dict_writer: &mut BufWriter<fs::File>,
    seek_offsets: &[(u64, u32)],
) -> Result<(), String> {
    fs::create_dir_all(detail_root).map_err(|e| e.to_string())?;
    fs::create_dir_all(detail_root.join("users")).map_err(|e| e.to_string())?;


    let mut user_dirs: Vec<Vec<(u64, u32)>> = vec![Vec::new(); users_dict.len()];
    for (dir_path_id, uid, bytes) in dir_user_rows {
        let u = *uid as usize;
        let p = *dir_path_id as usize;
        if u < user_dirs.len() && p < seek_offsets.len() {
            user_dirs[u].push((*bytes, *dir_path_id));
        }
    }
    for rows in &mut user_dirs {
        rows.sort_by(|a, b| b.0.cmp(&a.0));
    }

    let total_files_u64: u64 = total_docs;
    let total_dirs_u64: u64 = uid_totals_map.values().map(|(_, _, d)| *d).sum();

    for (uid_idx, username) in users_dict.iter().enumerate() {
        let safe = safe_user_dir(username);
        let user_dir = detail_root.join("users").join(&safe);
        fs::create_dir_all(user_dir.join("dirs")).map_err(|e| e.to_string())?;
        fs::create_dir_all(user_dir.join("files")).map_err(|e| e.to_string())?;

        let uid_id = uid_idx as u32;
        let dir_rows = &user_dirs[uid_idx];
        let file_artifacts = user_file_artifacts.get(&uid_id);

        let file_parts: Vec<serde_json::Value> = file_artifacts
            .map(|a| a.file_parts.clone())
            .unwrap_or_default();
        let file_total = file_artifacts.map(|a| a.total_files).unwrap_or(0);
        let file_sorted_by = file_artifacts
            .map(|a| a.files_sorted_by.clone())
            .unwrap_or_else(|| "size_desc".to_string());
        let ext_rows = file_artifacts
            .map(|a| a.ext_rows.clone())
            .unwrap_or_default();

        // dirs chunks
        let dir_chunks = if dir_rows.is_empty() {
            1
        } else {
            (dir_rows.len() + FILE_PART_RECORDS - 1) / FILE_PART_RECORDS
        };
        let mut dir_parts: Vec<serde_json::Value> = Vec::new();
        for chunk_idx in 0..dir_chunks {
            let start = chunk_idx * FILE_PART_RECORDS;
            let end = ((chunk_idx + 1) * FILE_PART_RECORDS).min(dir_rows.len());
            let chunk = if start < end { &dir_rows[start..end] } else { &[] };
            let chunk_dir = user_dir.join("dirs").join(format!("chunk-{:05}", chunk_idx));
            fs::create_dir_all(&chunk_dir).map_err(|e| e.to_string())?;
            let part_name = "part-00000.ndjson";
            let part_path = chunk_dir.join(part_name);
            let mut w = BufWriter::with_capacity(512 * 1024, fs::File::create(&part_path).map_err(|e| e.to_string())?);
            for (size, path_id) in chunk {
                serde_json::to_writer(&mut w, &json!({"gid": path_id, "s": size}))
                    .map_err(|e| e.to_string())?;
                w.write_all(b"\n").map_err(|e| e.to_string())?;
            }
            w.flush().map_err(|e| e.to_string())?;
            dir_parts.push(json!({
                "path": format!("dirs/chunk-{:05}/{}", chunk_idx, part_name),
                "records": chunk.len()
            }));
        }


        let page_index = json!({
            "dirs": {
                "pages": dir_parts.len(),
                "total_full": dir_rows.len()
            },
            "files": {
                "pages": file_parts.len(),
                "total_full": file_total,
                "sorted": {
                    "by": file_sorted_by
                }
            },
            "page_size": FILE_PART_RECORDS
        });
        fs::write(
            user_dir.join("page_index.json"),
            serde_json::to_vec(&page_index).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;

        fs::write(
            user_dir.join("extensions.json"),
            serde_json::to_vec(&ext_rows).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;

        let uid_id = uid_idx as u32;
        let (files_cnt, bytes_cnt, dirs_cnt) = uid_totals_map
            .get(&uid_id)
            .copied()
            .unwrap_or((0, 0, 0));
        let manifest = json!({
            "schema": "check-disk-user",
            "scan_date": timestamp,
            "username": username,
            "team_id": team_map.get(username).cloned().unwrap_or_default(),
            "summary": {
                "dirs": dirs_cnt,
                "files": files_cnt,
                "used": bytes_cnt
            },
            "dirs": {"parts": dir_parts},
            "files": {"parts": file_parts},
            "page_index": "page_index.json",
            "extensions": "extensions.json"
        });
        fs::write(
            user_dir.join("manifest.json"),
            serde_json::to_vec(&manifest).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    }

    let users_for_root: Vec<_> = users_dict
        .iter()
        .enumerate()
        .map(|(idx, username)| {
            let uid_key = idx as u32;
            let (files, bytes, dirs) = uid_totals_map.get(&uid_key).copied().unwrap_or((0, 0, 0));
            let safe = safe_user_dir(username);
            json!({
                "username": username,
                "uid": uid_key,
                "team_id": team_map.get(username).cloned().unwrap_or_default(),
                "manifest": format!("users/{}/manifest.json", safe),
                "files": files,
                "dirs": dirs,
                "used": bytes,
                "permission_issues": 0
            })
        })
        .collect();

    let data_detail = json!({
        "schema": "check-disk-detail",
        "scan": {
            "root": "",
            "timestamp": timestamp,
            "total_dirs": total_dirs_u64,
            "total_files": total_files_u64,
            "total_size": total_size
        },
        "path_dict": "api/path_dict.ndjson",
        "path_dict_seek": "api/path_dict.seek",
        "path_dict_seek_record_size": 16,
        "users": users_for_root
    });
    fs::write(
        detail_root.join("data_detail.json"),
        serde_json::to_vec(&data_detail).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;

    let root_manifest = json!({
        "schema": "check-disk-detail",
        "created_at": timestamp,
        "users": users_for_root
    });
    fs::write(
        detail_root.join("manifest.json"),
        serde_json::to_vec(&root_manifest).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;

    fs::create_dir_all(detail_root.join("api")).map_err(|e| e.to_string())?;

    path_dict_writer.flush().map_err(|e| e.to_string())?;

    let seek_path = detail_root.join("api/path_dict.seek");
    let mut seek_writer = BufWriter::with_capacity(
        512 * 1024,
        fs::File::create(&seek_path).map_err(|e| e.to_string())?,
    );
    seek_writer.write_all(b"PDX1").map_err(|e| e.to_string())?;
    seek_writer.write_all(&1u32.to_le_bytes()).map_err(|e| e.to_string())?;
    seek_writer
        .write_all(&(seek_offsets.len() as u32).to_le_bytes())
        .map_err(|e| e.to_string())?;
    for (gid, (off, len)) in seek_offsets.iter().enumerate() {
        seek_writer.write_all(&(gid as u32).to_le_bytes()).map_err(|e| e.to_string())?;
        seek_writer.write_all(&off.to_le_bytes()).map_err(|e| e.to_string())?;
        seek_writer.write_all(&len.to_le_bytes()).map_err(|e| e.to_string())?;
    }
    seek_writer.flush().map_err(|e| e.to_string())?;

    Ok(())
}
#[pyfunction(signature = (tmpdir, uids_map, team_map, pipeline_db_path, treemap_json, treemap_db, treemap_root, max_level, min_size_bytes, timestamp, max_workers, build_treemap=true, debug=false))]
#[allow(clippy::too_many_arguments)]
pub fn build_pipeline_dbs_impl(
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
    py.allow_threads(move || -> PyResult<u64> {
        let t_all = Instant::now();
        let t_perm_tsv: f64;
        let t_dir_build: f64;
        let t_output_jobs: f64;
        let t_chunk_parallel: f64;
        let t_finalize: f64;
        let t_text_write: f64;
        let t_perm_write: f64;
        let t_swap_detail: f64;
        let mut t_tree: f64 = 0.0;
        let mut t_swap_tree: f64 = 0.0;

        let detail_root = Path::new(&pipeline_db_path)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("detail_users"));
        let detail_parent = detail_root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        ensure_dir(&detail_parent)?;
        let detail_work_root = detail_parent.join(format!(
            ".detail_users_build_{}",
            std::process::id()
        ));
        cleanup_stale_build_dirs(&detail_parent, ".detail_users_build_", &detail_work_root);
        recreate_dir(&detail_work_root)?;
        ensure_dir(&detail_work_root.join("users"))?;
        ensure_dir(&detail_work_root.join("api"))?;

        let tree_data_dir = PathBuf::from(&treemap_db);
        let tree_parent = tree_data_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        ensure_dir(&tree_parent)?;
        let tree_work_dir = tree_parent.join(format!(
            ".tree_map_data_build_{}",
            std::process::id()
        ));
        cleanup_stale_build_dirs(&tree_parent, ".tree_map_data_build_", &tree_work_dir);
        recreate_dir(&tree_work_dir)?;

        let t0 = Instant::now();
        let mut perm_events = read_permission_events(&tmpdir).unwrap_or_default();
        t_perm_tsv = t0.elapsed().as_secs_f64();

        let t1 = Instant::now();

        struct LocalAgg {
            dir_sizes: HashMap<String, HashMap<u32, i64>>,
            rows_by_user: HashMap<String, Vec<(u64, String, String)>>,
            row_spills: HashMap<String, Vec<PathBuf>>,
            row_count: usize,
            spill_bytes_written: u64,
        }
        let spool_root = detail_work_root.join(".rows_spill");
        recreate_dir(&spool_root)?;
        let spill_seq = Arc::new(AtomicU64::new(0));

        let dir_agg_paths = get_dir_agg_files(&tmpdir)?;
        let has_dir_agg = !dir_agg_paths.is_empty();
        let bin_paths = get_scan_event_files(&tmpdir)?;
        println!("[Phase 2] Ingesting and mapping {} event streams...", bin_paths.len());

        let merged_agg = bin_paths
            .into_par_iter()
            .fold(
                || LocalAgg {
                    dir_sizes: HashMap::new(),
                    rows_by_user: HashMap::new(),
                    row_spills: HashMap::new(),
                    row_count: 0,
                    spill_bytes_written: 0,
                },
                |mut agg, path| {
                    let spill_seq_ref = &spill_seq;
                    let _ = for_each_scan_event_in_file(&path, |event| {
                        let username = uids_map
                            .get(&event.uid)
                            .cloned()
                            .unwrap_or_else(|| format!("uid-{}", event.uid));
                        let safe_path = crate::sanitise_path(&event.path);
                        let ext = crate::pipe_types::extension_for_path(&safe_path);
                        agg.rows_by_user
                            .entry(username.clone())
                            .or_default()
                            .push((event.size, safe_path.clone(), ext));
                        agg.row_count += 1;
                        if !has_dir_agg {
                            if let Some(parent) = crate::pipe_types::parent_path(&safe_path) {
                                let user_sizes = agg.dir_sizes.entry(parent).or_default();
                                *user_sizes.entry(event.uid).or_insert(0) += event.size as i64;
                            }
                        }
                    });
                    if agg.row_count >= ROW_SPILL_THRESHOLD {
                        agg.spill_bytes_written += spill_rows_to_disk(
                            &spool_root,
                            spill_seq_ref,
                            &mut agg.rows_by_user,
                            &mut agg.row_spills,
                        );
                        agg.row_count = 0;
                    }
                    agg
                },
            )
            .reduce(
                || LocalAgg {
                    dir_sizes: HashMap::new(),
                    rows_by_user: HashMap::new(),
                    row_spills: HashMap::new(),
                    row_count: 0,
                    spill_bytes_written: 0,
                },
                |mut a, b| {
                    for (user, mut rows) in b.rows_by_user {
                        let row_len = rows.len();
                        a.rows_by_user.entry(user).or_default().append(&mut rows);
                        a.row_count += row_len;
                    }
                    for (user, mut spills) in b.row_spills {
                        a.row_spills.entry(user).or_default().append(&mut spills);
                    }
                    for (dir, sizes_b) in b.dir_sizes {
                        let sizes_a = a.dir_sizes.entry(dir).or_default();
                        for (uid, size) in sizes_b {
                            *sizes_a.entry(uid).or_insert(0) += size;
                        }
                    }
                    a.spill_bytes_written += b.spill_bytes_written;
                    if a.row_count >= ROW_SPILL_THRESHOLD {
                        a.spill_bytes_written += spill_rows_to_disk(&spool_root, &spill_seq, &mut a.rows_by_user, &mut a.row_spills);
                        a.row_count = 0;
                    }
                    a
                },
            );

        let mut total_spill_written = merged_agg.spill_bytes_written;


        let dir_sizes_by_user = if has_dir_agg {
            println!(
                "[Phase 2] Loading {} Phase 1 directory aggregate shards...",
                dir_agg_paths.len()
            );
            dir_agg_paths
                .into_par_iter()
                .fold(HashMap::new, |mut dir_sizes: HashMap<String, HashMap<u32, i64>>, path| {
                    let _ = for_each_dir_agg_in_file(&path, |event| {
                        if event.size > 0 {
                            let user_sizes = dir_sizes.entry(event.path.clone()).or_default();
                            *user_sizes.entry(event.uid).or_insert(0) += event.size;
                        }
                    });
                    dir_sizes
                })
                .reduce(HashMap::new, |mut a, b| {
                    for (dir, sizes_b) in b {
                        let sizes_a = a.entry(dir).or_default();
                        for (uid, size) in sizes_b {
                            *sizes_a.entry(uid).or_insert(0) += size;
                        }
                    }
                    a
                })
        } else {
            merged_agg.dir_sizes
        };

        let dir_sizes: Vec<(String, Vec<(u32, i64)>)> = dir_sizes_by_user
            .into_iter()
            .map(|(dir, user_map)| (dir, user_map.into_iter().collect()))
            .collect();
        let mut rows_by_user = merged_agg.rows_by_user;
        let mut row_spills = merged_agg.row_spills;
        total_spill_written += spill_rows_to_disk(&spool_root, &spill_seq, &mut rows_by_user, &mut row_spills);
        rows_by_user.clear();
        let mut total_spill_read = 0u64;

        let mut dir_owner_map: HashMap<String, String> = HashMap::new();
        let mut users: HashMap<String, UserOutputMeta> = HashMap::new();

        println!("[Phase 2] Grouping directory stats for {} paths...", dir_sizes.len());

        for (dpath, user_sizes) in dir_sizes.iter() {
            let mut d_max_size = 0_i64;
            let mut d_max_user = String::new();
            for (owner_uid, size) in user_sizes {
                if *size <= 0 {
                    continue;
                }
                let owner = uids_map
                    .get(owner_uid)
                    .cloned()
                    .unwrap_or_else(|| format!("uid-{}", owner_uid));
                if *size > d_max_size {
                    d_max_size = *size;
                    d_max_user = owner.clone();
                }
                let entry = users.entry(owner.clone()).or_insert_with(|| UserOutputMeta {
                    team_id: team_map.get(owner.as_str()).cloned().unwrap_or_default(),
                    ..Default::default()
                });
                entry.total_dirs += 1;
                entry.total_used += *size;
            }
            if !d_max_user.is_empty() {
                dir_owner_map.insert(dpath.clone(), d_max_user);
            }
        }

        for username in row_spills.keys() {
            users
                .entry(username.clone())
                .or_insert_with(|| UserOutputMeta {
                    team_id: team_map.get(username).cloned().unwrap_or_default(),
                    ..Default::default()
                });
        }
        t_dir_build = t1.elapsed().as_secs_f64();

        let t2 = Instant::now();
        println!(
            "[Phase 2] Building file chunks per user ({} users)...",
            row_spills.len()
        );
        t_output_jobs = t2.elapsed().as_secs_f64();

        let t3 = Instant::now();
        let detail_workers = max_workers.max(1);
        let mut user_keys: Vec<String> = users.keys().cloned().collect();
        user_keys.sort();
        let total_users = user_keys.len().max(1);

        let mut global_path_to_id: HashMap<u64, usize> = HashMap::new();
        let mut global_parent_path_id: Vec<u32> = vec![0u32];
        global_path_to_id.insert(path_hash("/"), 0usize);
        let path_dict_path = detail_work_root.join("api/path_dict.ndjson");
        let path_dict_file = fs::File::create(&path_dict_path)
            .map_err(|e| PyRuntimeError::new_err(format!("create {}: {}", path_dict_path.display(), e)))?;
        let mut path_dict_writer = BufWriter::with_capacity(2 * 1024 * 1024, path_dict_file);
        let root_row = serde_json::to_vec(&json!({"gid": 0u32, "p": "/"}))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        path_dict_writer
            .write_all(&root_row)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        path_dict_writer
            .write_all(b"\n")
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut seek_offsets: Vec<(u64, u32)> = Vec::new();
        seek_offsets.push((0u64, (root_row.len() + 1) as u32));
        let mut path_dict_ndjson_offset: u64 = (root_row.len() + 1) as u64;
        let mut next_gid: usize = 1;

        let mut user_to_uid: HashMap<String, u32> = HashMap::new();
        let mut users_dict: Vec<String> = Vec::new();
        let mut user_file_artifacts: HashMap<u32, UserFileArtifacts> = HashMap::new();
        let mut total_docs: u64 = 0;
        let mut total_size: u64 = 0;
        let mut user_timings: Vec<(String, f64, usize, usize)> = Vec::new();
        let mut user_results: Vec<crate::pipe_types::UserBuildResult> = Vec::with_capacity(user_keys.len());

        let mut done_users = 0usize;
        for username in user_keys {
            let user_t0 = if debug { Some(Instant::now()) } else { None };
            let user = users.remove(&username).unwrap_or_default();
            let spills = row_spills.remove(&username).unwrap_or_default();
            for path in &spills {
                if let Ok(meta) = fs::metadata(path) {
                    total_spill_read += meta.len();
                }
            }

            let uid = if let Some(found) = user_to_uid.get(&username) {
                *found
            } else {
                let id = users_dict.len() as u32;
                users_dict.push(username.clone());
                user_to_uid.insert(username.clone(), id);
                id
            };

            let user_dir = detail_work_root.join("users").join(safe_user_dir(&username));
            fs::create_dir_all(user_dir.join("files")).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

            let mut ext_map: HashMap<String, (u64, u64)> = HashMap::new();
            let mut file_parts: Vec<serde_json::Value> = Vec::new();
            let mut sorted_file_rows: Vec<(u32, u64, String)> = Vec::new();
            let mut part_index: usize = 0;
            let mut total_user_files: usize = 0;

            for spill_path in &spills {
                let file = fs::File::open(spill_path)
                    .map_err(|e| PyRuntimeError::new_err(format!("open spill {}: {}", spill_path.display(), e)))?;
                let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
                let mut head = [0u8; 12];
                let mut ext_len_buf = [0u8; 2];
                let mut path_bytes: Vec<u8> = Vec::new();
                let mut ext_bytes: Vec<u8> = Vec::new();

                loop {
                    match reader.read_exact(&mut head) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => {
                            return Err(PyRuntimeError::new_err(format!(
                                "read spill header {}: {}",
                                spill_path.display(),
                                e
                            )));
                        }
                    }

                    let size = u64::from_le_bytes(head[0..8].try_into().unwrap_or([0u8; 8]));
                    let path_len = u32::from_le_bytes(head[8..12].try_into().unwrap_or([0u8; 4])) as usize;
                    path_bytes.clear();
                    path_bytes.resize(path_len, 0);
                    reader
                        .read_exact(&mut path_bytes)
                        .map_err(|e| PyRuntimeError::new_err(format!("read spill path {}: {}", spill_path.display(), e)))?;
                    reader
                        .read_exact(&mut ext_len_buf)
                        .map_err(|e| PyRuntimeError::new_err(format!("read spill ext len {}: {}", spill_path.display(), e)))?;
                    let ext_len = u16::from_le_bytes(ext_len_buf) as usize;
                    ext_bytes.clear();
                    ext_bytes.resize(ext_len, 0);
                    reader
                        .read_exact(&mut ext_bytes)
                        .map_err(|e| PyRuntimeError::new_err(format!("read spill ext {}: {}", spill_path.display(), e)))?;

                    let safe_path = String::from_utf8_lossy(&path_bytes).to_string();
                    let ext = String::from_utf8_lossy(&ext_bytes).to_string();

                    let path_id = ensure_path_id(
                        &safe_path,
                        &mut global_path_to_id,
                        &mut global_parent_path_id,
                        &mut next_gid,
                        &mut path_dict_writer,
                        &mut seek_offsets,
                        &mut path_dict_ndjson_offset,
                    )
                    .map_err(PyRuntimeError::new_err)? as u32;

                    sorted_file_rows.push((path_id, size, ext.clone()));

                    let entry = ext_map.entry(ext).or_insert((0, 0));
                    entry.0 += 1;
                    entry.1 += size;
                    total_docs += 1;
                    total_size += size;
                    total_user_files += 1;
                }
            }

            // Sort all file rows by size DESC for this user
            sorted_file_rows.sort_by(|a, b| b.1.cmp(&a.1));

            // Write sorted chunks
            let mut part_writer: Option<BufWriter<fs::File>> = None;
            let mut part_records: usize = 0;
            for (path_id, size, ext) in &sorted_file_rows {
                if part_writer.is_none() {
                    let chunk_dir = user_dir.join("files").join(format!("chunk-{:05}", part_index));
                    fs::create_dir_all(&chunk_dir)
                        .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", chunk_dir.display(), e)))?;
                    let part_path = chunk_dir.join("part-00000_files.ndjson");
                    let file = fs::File::create(&part_path)
                        .map_err(|e| PyRuntimeError::new_err(format!("create {}: {}", part_path.display(), e)))?;
                    part_writer = Some(BufWriter::with_capacity(512 * 1024, file));
                    part_records = 0;
                }

                if let Some(writer) = part_writer.as_mut() {
                    serde_json::to_writer(&mut *writer, &json!({"gid": path_id, "s": size, "x": ext}))
                        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    writer.write_all(b"\n").map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                }

                part_records += 1;
                if part_records >= FILE_PART_RECORDS {
                    if let Some(mut writer) = part_writer.take() {
                        writer.flush().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                    }
                    file_parts.push(json!({
                        "path": format!("files/chunk-{:05}/part-00000_files.ndjson", part_index),
                        "records": part_records
                    }));
                    part_index += 1;
                    part_records = 0;
                }
            }

            if let Some(mut writer) = part_writer.take() {
                writer.flush().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                file_parts.push(json!({
                    "path": format!("files/chunk-{:05}/part-00000_files.ndjson", part_index),
                    "records": part_records
                }));
            }

            let mut ext_rows: Vec<serde_json::Value> = ext_map
                .into_iter()
                .map(|(ext, (count, size))| json!({"ext": ext, "count": count, "size": size}))
                .collect();
            ext_rows.sort_by(|a, b| {
                b.get("size")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0)
                    .cmp(&a.get("size").and_then(|v| v.as_u64()).unwrap_or(0))
            });
            user_file_artifacts.insert(
                uid,
                UserFileArtifacts {
                    file_parts,
                    ext_rows,
                    total_files: total_user_files,
                    files_sorted_by: "size_desc".to_string(),
                },
            );

            let team_id = if user.team_id.is_empty() {
                team_map.get(&username).cloned().unwrap_or_default()
            } else {
                user.team_id
            };
            user_results.push(crate::pipe_types::UserBuildResult {
                username: username.clone(),
                team_id,
                total_dirs: user.total_dirs,
                total_files: total_user_files as i64,
                total_used: user.total_used,
            });

            for path in &spills {
                let _ = fs::remove_file(path);
            }

            done_users += 1;
            if done_users % 10 == 0 || done_users == total_users {
                let pct = (done_users as f64 / total_users as f64) * 100.0;
                let rss_mb = crate::pipe_types::get_rss_mb();
                println!(
                    "[Phase 2] Detail reports: {}/{} users ({:.1}%) | RSS: {:.1} MB",
                    done_users, total_users, pct, rss_mb
                );
            }

            if debug {
                let user_time_s = user_t0.map(|t0| t0.elapsed().as_secs_f64()).unwrap_or(0.0);
                let part_count = user_file_artifacts
                    .get(&uid)
                    .map(|a| a.file_parts.len())
                    .unwrap_or(0);
                user_timings.push((username, user_time_s, spills.len(), part_count));
            }
        }
        t_chunk_parallel = t3.elapsed().as_secs_f64();

        let t4 = Instant::now();
        user_results.sort_by(|a, b| a.username.cmp(&b.username));
        let total_files_processed: i64 = user_results.iter().map(|u| u.total_files).sum();

        let mut errcode_map: HashMap<String, u16> = HashMap::new();
        let mut perm_records: Vec<(u32, u32, u16, u8)> = Vec::new();
        perm_events.sort_by(|a, b| {
            a.uid
                .cmp(&b.uid)
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.errcode.cmp(&b.errcode))
                .then_with(|| a.kind.cmp(&b.kind))
        });
        for event in &perm_events {
            let username = uids_map
                .get(&event.uid)
                .cloned()
                .unwrap_or_else(|| format!("uid-{}", event.uid));
            let uid_id = if let Some(id) = user_to_uid.get(&username) {
                *id
            } else {
                let id = users_dict.len() as u32;
                users_dict.push(username.clone());
                user_to_uid.insert(username.clone(), id);
                id
            };
            let safe_path = crate::sanitise_path(&event.path);
            let path_id = ensure_path_id(
                &safe_path,
                &mut global_path_to_id,
                &mut global_parent_path_id,
                &mut next_gid,
                &mut path_dict_writer,
                &mut seek_offsets,
                &mut path_dict_ndjson_offset,
            )
            .map_err(PyRuntimeError::new_err)? as u32;
            let errcode_id = if let Some(id) = errcode_map.get(&event.errcode) {
                *id
            } else {
                let id = errcode_map.len() as u16;
                errcode_map.insert(event.errcode.clone(), id);
                id
            };
            let kind = match event.kind.as_str() {
                "dir" => 1u8,
                "symlink" => 2u8,
                _ => 0u8,
            };
            perm_records.push((uid_id, path_id, errcode_id, kind));
        }

        let mut uid_totals_map: HashMap<u32, (u64, u64, u64)> = HashMap::new();
        for result in &user_results {
            if let Some(uid_id) = user_to_uid.get(&result.username) {
                uid_totals_map.insert(
                    *uid_id,
                    (
                        result.total_files.max(0) as u64,
                        result.total_used.max(0) as u64,
                        result.total_dirs.max(0) as u64,
                    ),
                );
            }
        }
        let mut dir_user_rows: Vec<(u32, u32, u64)> = Vec::new();
        for (dir_path, users_sizes) in dir_sizes.iter() {
            let dir_id = ensure_path_id(
                dir_path,
                &mut global_path_to_id,
                &mut global_parent_path_id,
                &mut next_gid,
                &mut path_dict_writer,
                &mut seek_offsets,
                &mut path_dict_ndjson_offset,
            )
            .map_err(PyRuntimeError::new_err)? as u32;
            for (owner_uid, bytes) in users_sizes {
                if *bytes <= 0 {
                    continue;
                }
                let username = uids_map
                    .get(owner_uid)
                    .cloned()
                    .unwrap_or_else(|| format!("uid-{}", owner_uid));
                if let Some(uid_id) = user_to_uid.get(&username) {
                    dir_user_rows.push((dir_id, *uid_id, *bytes as u64));
                }
            }
        }
        dir_user_rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Path lookup maps are no longer needed after ID assignment.
        drop(global_path_to_id);
        drop(global_parent_path_id);
        let rss_after_drop = crate::pipe_types::get_rss_mb();
        println!("[Phase 2] Post-index RSS: {:.1} MB", rss_after_drop);

        t_finalize = t4.elapsed().as_secs_f64();

        let t5 = Instant::now();
        write_text_outputs(
            &detail_work_root,
            &tree_work_dir,
            &users_dict,
            &team_map,
            &user_file_artifacts,
            total_docs,
            total_size,
            &dir_user_rows,
            &uid_totals_map,
            timestamp,
            &mut path_dict_writer,
            &seek_offsets,
        )
        .map_err(PyRuntimeError::new_err)?;
        drop(path_dict_writer);
        drop(seek_offsets);
        let rss_after_text = crate::pipe_types::get_rss_mb();
        println!("[Phase 2] Post-text RSS: {:.1} MB", rss_after_text);
        t_text_write = t5.elapsed().as_secs_f64();

        let t7 = Instant::now();
        let perm_out_dir = detail_work_root.parent().unwrap_or(Path::new(".")).to_path_buf();
        write_permission_issues_json(&perm_events, &uids_map, &perm_out_dir, &treemap_root, timestamp)?;
        t_perm_write = t7.elapsed().as_secs_f64();

        let t8 = Instant::now();
        let _ = fs::remove_dir_all(&spool_root);
        swap_dir_atomic(&detail_work_root, &detail_root)?;
        t_swap_detail = t8.elapsed().as_secs_f64();

        if build_treemap {
            println!("[Phase 2] Detail text done. Starting deferred TreeMap JSON build...");
            let t9 = Instant::now();
            let dir_sizes_for_treemap: HashMap<String, Vec<(String, i64)>> = dir_sizes
                .into_iter()
                .map(|(dir, entries)| {
                    let mapped = entries
                        .into_iter()
                        .map(|(owner_uid, size)| {
                            (
                                uids_map
                                    .get(&owner_uid)
                                    .cloned()
                                    .unwrap_or_else(|| format!("uid-{}", owner_uid)),
                                size,
                            )
                        })
                        .collect();
                    (dir, mapped)
                })
                .collect();
            write_treemap_json_outputs(
                &treemap_root,
                dir_sizes_for_treemap,
                dir_owner_map,
                &treemap_json,
                &tree_work_dir,
                max_level.max(1),
                min_size_bytes,
            )?;
            t_tree = t9.elapsed().as_secs_f64();
            println!("[Phase 2] TreeMap build completed in {:.2}s", t_tree);
            let t10 = Instant::now();
            swap_dir_atomic(&tree_work_dir, &tree_data_dir)?;
            t_swap_tree = t10.elapsed().as_secs_f64();
        } else {
            drop(dir_sizes);
            let _ = fs::remove_dir_all(&tree_work_dir);
        }

        if debug {
            let rss_mb = crate::pipe_types::get_rss_mb();
            println!(
                "JSON/NDJSON outputs built in {:.2}s with {} detail workers. Total files: {}, perms: {}",
                t_all.elapsed().as_secs_f64(),
                detail_workers,
                total_files_processed,
                perm_events.len()
            );
            println!("[Phase 2 Profile]");
            println!("  Perm TSV read:      {:.4}s", t_perm_tsv);
            println!("  Dir grouping:       {:.4}s", t_dir_build);
            println!("  Output jobs build:  {:.4}s", t_output_jobs);
            println!("  Spill written:      {:.2} MB", total_spill_written as f64 / (1024.0 * 1024.0));
            println!("  Spill reread:       {:.2} MB", total_spill_read as f64 / (1024.0 * 1024.0));
            if total_spill_written > 0 {
                println!(
                    "  Spill reread x:     {:.2}x",
                    total_spill_read as f64 / total_spill_written as f64
                );
            }
            println!("  Chunk parallel:     {:.4}s", t_chunk_parallel);
            println!("  Finalize+manifest:  {:.4}s", t_finalize);
            println!("  Text write:          {:.4}s", t_text_write);
            println!("  Perm JSON write:    {:.4}s", t_perm_write);
            println!("  Swap detail dir:    {:.4}s", t_swap_detail);
            println!("  TreeMap build:      {:.4}s", t_tree);
            println!("  Swap treemap dir:   {:.4}s", t_swap_tree);
            if !user_timings.is_empty() {
                let total_users_t = user_timings.len() as f64;
                let total_user_time: f64 = user_timings.iter().map(|(_, t, _, _)| *t).sum();
                let avg_user_time = total_user_time / total_users_t;
                println!("  Detail/user avg:    {:.4}s", avg_user_time);
                user_timings.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                for (idx, (username, elapsed, spill_count, _)) in user_timings.iter().take(5).enumerate() {
                    println!(
                        "  Detail/user top{}:   {} = {:.4}s ({} spill files)",
                        idx + 1,
                        username,
                        elapsed,
                        spill_count
                    );
                }
            }
            println!("  Peak RSS:           {:.1} MB", rss_mb);
        }
        Ok(total_files_processed as u64)
    })
}

