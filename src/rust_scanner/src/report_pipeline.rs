use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::pipe_binary::{
    write_detail_user_bins, write_detail_users_agg, write_detail_users_cols,
    write_detail_users_dict_all, write_dict_header_16, write_dict_offsets_blob,
    write_paths_header_24, write_treemap_bins, write_treemap_manifest, MAGIC_EXTS, MAGIC_USER,
};
use crate::pipe_events::{
    for_each_dir_agg_in_file, for_each_scan_event_in_file, get_dir_agg_files, get_scan_event_files,
    read_permission_events,
};
use crate::pipe_io::{ensure_dir, recreate_dir, safe_user_dir, swap_dir_atomic};
use crate::pipe_permission::write_permission_issues_json;
use crate::pipe_treemap::write_treemap_json_outputs;
use crate::pipe_types::{FileChunkJob, UserJobMeta, UserOutputMeta, FILE_PART_RECORDS};
use crate::pipe_user_finalize::{finalize_user_outputs, write_detail_manifest};
use crate::pipe_user_jobs::build_one_file_chunk;

const ROW_SPILL_THRESHOLD: usize = 200_000;
const SEED_MAGIC: &[u8; 4] = b"SDX1";

fn write_seed_header<W: Write>(writer: &mut W) -> Result<(), String> {
    writer
        .write_all(SEED_MAGIC)
        .and_then(|_| writer.write_all(&1u32.to_le_bytes()))
        .map_err(|e| format!("write seed header: {}", e))
}

fn write_seed_record<W: Write>(
    writer: &mut W,
    uid: u32,
    gid: u32,
    size: u64,
    eid: u32,
) -> Result<(), String> {
    writer
        .write_all(&uid.to_le_bytes())
        .and_then(|_| writer.write_all(&gid.to_le_bytes()))
        .and_then(|_| writer.write_all(&size.to_le_bytes()))
        .and_then(|_| writer.write_all(&eid.to_le_bytes()))
        .map_err(|e| format!("write seed record: {}", e))
}

fn write_dict_binary<W: Write>(writer: &mut W, items: &[String]) -> Result<(), String> {
    write_dict_header_16(writer, MAGIC_EXTS, 1, items.len() as u32)?;
    write_dict_offsets_blob(writer, items)
}

fn write_paths_binary(path: &Path, paths: &[String]) -> Result<(), String> {
    let file = fs::File::create(path).map_err(|e| format!("create {}: {}", path.display(), e))?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    write_paths_header_24(&mut writer, 1, paths.len() as u32)?;
    write_dict_offsets_blob(&mut writer, paths)?;
    writer.flush().map_err(|e| e.to_string())
}

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
    rows_by_user: &mut HashMap<String, Vec<(u64, String)>>,
    row_spills: &mut HashMap<String, Vec<PathBuf>>,
) {
    if rows_by_user.is_empty() {
        return;
    }
    let mut drained = HashMap::new();
    std::mem::swap(rows_by_user, &mut drained);
    for (username, rows) in drained {
        if rows.is_empty() {
            continue;
        }
        let safe = safe_user_dir(&username);
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
        for (size, raw_path) in rows {
            let path_bytes = raw_path.as_bytes();
            let len = u32::try_from(path_bytes.len()).unwrap_or(u32::MAX);
            if writer.write_all(&size.to_le_bytes()).is_err()
                || writer.write_all(&len.to_le_bytes()).is_err()
                || writer
                    .write_all(&path_bytes[..(len as usize).min(path_bytes.len())])
                    .is_err()
            {
                write_ok = false;
                break;
            }
        }
        if write_ok && writer.flush().is_ok() {
            row_spills.entry(username).or_default().push(path);
        } else {
            let _ = fs::remove_file(path);
        }
    }
}

fn build_chunk_jobs_from_spills(
    detail_root: &Path,
    username: &str,
    spill_paths: &[PathBuf],
    global_path_to_id: &mut HashMap<String, usize>,
    global_paths: &mut Vec<String>,
    global_parent_path_id: &mut Vec<u32>,
) -> Result<Vec<FileChunkJob>, String> {
    let safe = safe_user_dir(username);
    let tmp_dir = detail_root.join("users").join(format!(".tmp_{}", safe));
    let mut jobs = Vec::new();
    let mut rows: Vec<(u64, usize, String)> = Vec::with_capacity(FILE_PART_RECORDS);
    let mut chunk_index: usize = 0;
    for spill_path in spill_paths {
        let file = fs::File::open(spill_path)
            .map_err(|e| format!("open spill {}: {}", spill_path.display(), e))?;
        let mut reader = BufReader::with_capacity(1024 * 1024, file);
        loop {
            let mut head = [0u8; 12];
            match reader.read_exact(&mut head) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("read spill header {}: {}", spill_path.display(), e)),
            }
            let size = u64::from_le_bytes(head[0..8].try_into().unwrap_or([0u8; 8]));
            let path_len = u32::from_le_bytes(head[8..12].try_into().unwrap_or([0u8; 4])) as usize;
            let mut path_bytes = vec![0u8; path_len];
            reader
                .read_exact(&mut path_bytes)
                .map_err(|e| format!("read spill path {}: {}", spill_path.display(), e))?;
            let raw_path = String::from_utf8_lossy(&path_bytes).to_string();
            let safe_path = crate::sanitise_path(&raw_path);
            let ext = crate::pipe_types::extension_for_path(&safe_path);
            let path_id = ensure_path_id(
                &safe_path,
                global_path_to_id,
                global_paths,
                global_parent_path_id,
            );
            rows.push((size, path_id, ext));
            if rows.len() >= FILE_PART_RECORDS {
                jobs.push(FileChunkJob {
                    username: username.to_string(),
                    chunk_index,
                    output_dir: tmp_dir
                        .join("files")
                        .join(format!("chunk-{:05}", chunk_index)),
                    rows: std::mem::take(&mut rows),
                });
                chunk_index += 1;
            }
        }
    }
    if !rows.is_empty() {
        jobs.push(FileChunkJob {
            username: username.to_string(),
            chunk_index,
            output_dir: tmp_dir
                .join("files")
                .join(format!("chunk-{:05}", chunk_index)),
            rows,
        });
    }
    Ok(jobs)
}

fn ensure_path_id(
    path: &str,
    global_path_to_id: &mut HashMap<String, usize>,
    global_paths: &mut Vec<String>,
    global_parent_path_id: &mut Vec<u32>,
) -> usize {
    if let Some(id) = global_path_to_id.get(path) {
        return *id;
    }

    let parent_id = crate::pipe_types::parent_path(path)
        .map(|parent| {
            if parent == path {
                0usize
            } else {
                ensure_path_id(
                    &parent,
                    global_path_to_id,
                    global_paths,
                    global_parent_path_id,
                )
            }
        })
        .unwrap_or(0usize);

    let id = global_paths.len();
    global_paths.push(path.to_string());
    global_parent_path_id.push(parent_id as u32);
    global_path_to_id.insert(path.to_string(), id);
    id
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
        let t_paths_global_write: f64;
        let t_index_build: f64;
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
        let perm_events = read_permission_events(&tmpdir).unwrap_or_default();
        t_perm_tsv = t0.elapsed().as_secs_f64();

        let t1 = Instant::now();

        struct LocalAgg {
            dir_sizes: HashMap<String, HashMap<String, i64>>,
            rows_by_user: HashMap<String, Vec<(u64, String)>>,
            row_spills: HashMap<String, Vec<PathBuf>>,
            row_count: usize,
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
                },
                |mut agg, path| {
                    let spill_seq_ref = &spill_seq;
                    let _ = for_each_scan_event_in_file(&path, |event| {
                        let username = uids_map
                            .get(&event.uid)
                            .cloned()
                            .unwrap_or_else(|| format!("uid-{}", event.uid));
                        agg.rows_by_user
                            .entry(username.clone())
                            .or_default()
                            .push((event.size, event.path.clone()));
                        agg.row_count += 1;
                        if !has_dir_agg {
                            if let Some(parent) = crate::pipe_types::parent_path(&event.path) {
                                let user_sizes = agg.dir_sizes.entry(parent).or_default();
                                *user_sizes.entry(username).or_insert(0) += event.size as i64;
                            }
                        }
                    });
                    if agg.row_count >= ROW_SPILL_THRESHOLD {
                        spill_rows_to_disk(
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
                },
                |mut a, b| {
                    for (user, mut rows) in b.rows_by_user {
                        a.rows_by_user.entry(user).or_default().append(&mut rows);
                        a.row_count += 1;
                    }
                    for (user, mut spills) in b.row_spills {
                        a.row_spills.entry(user).or_default().append(&mut spills);
                    }
                    for (dir, sizes_b) in b.dir_sizes {
                        let sizes_a = a.dir_sizes.entry(dir).or_default();
                        for (username, size) in sizes_b {
                            *sizes_a.entry(username).or_insert(0) += size;
                        }
                    }
                    if a.row_count >= ROW_SPILL_THRESHOLD {
                        spill_rows_to_disk(&spool_root, &spill_seq, &mut a.rows_by_user, &mut a.row_spills);
                        a.row_count = 0;
                    }
                    a
                },
            );

        let dir_sizes_by_user = if has_dir_agg {
            println!(
                "[Phase 2] Loading {} Phase 1 directory aggregate shards...",
                dir_agg_paths.len()
            );
            dir_agg_paths
                .into_par_iter()
                .fold(HashMap::new, |mut dir_sizes: HashMap<String, HashMap<String, i64>>, path| {
                    let _ = for_each_dir_agg_in_file(&path, |event| {
                        let username = uids_map
                            .get(&event.uid)
                            .cloned()
                            .unwrap_or_else(|| format!("uid-{}", event.uid));
                        if event.size > 0 {
                            let user_sizes = dir_sizes.entry(event.path.clone()).or_default();
                            *user_sizes.entry(username).or_insert(0) += event.size;
                        }
                    });
                    dir_sizes
                })
                .reduce(HashMap::new, |mut a, b| {
                    for (dir, sizes_b) in b {
                        let sizes_a = a.entry(dir).or_default();
                        for (username, size) in sizes_b {
                            *sizes_a.entry(username).or_insert(0) += size;
                        }
                    }
                    a
                })
        } else {
            merged_agg.dir_sizes
        };

        let dir_sizes: HashMap<String, Vec<(String, i64)>> = dir_sizes_by_user
            .into_iter()
            .map(|(dir, user_map)| (dir, user_map.into_iter().collect()))
            .collect();
        let mut rows_by_user = merged_agg.rows_by_user;
        let mut row_spills = merged_agg.row_spills;
        spill_rows_to_disk(&spool_root, &spill_seq, &mut rows_by_user, &mut row_spills);
        rows_by_user.clear();

        let mut dir_owner_map: HashMap<String, String> = HashMap::new();
        let mut users: HashMap<String, UserOutputMeta> = HashMap::new();

        println!("[Phase 2] Grouping directory stats for {} paths...", dir_sizes.len());

        for (dpath, user_sizes) in &dir_sizes {
            let mut d_max_size = 0_i64;
            let mut d_max_user = String::new();
            for (owner, size) in user_sizes {
                if *size <= 0 {
                    continue;
                }
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
                entry.top_dirs.push((dpath.clone(), *size));
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
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(detail_workers)
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("detail thread pool: {}", e)))?;
        let mut user_results = Vec::new();
        let mut user_keys: Vec<String> = users.keys().cloned().collect();
        user_keys.sort();
        let total_users = user_keys.len().max(1);
        let mut done_users = 0usize;
        let mut global_path_to_id: HashMap<String, usize> = HashMap::new();
        let mut global_paths: Vec<String> = vec!["/".to_string()];
        let mut global_parent_path_id: Vec<u32> = vec![0u32];
        global_path_to_id.insert("/".to_string(), 0usize);
        let mut ext_to_id: HashMap<String, u32> = HashMap::new();
        let mut exts: Vec<String> = Vec::new();
        ext_to_id.insert(String::new(), 0);
        exts.push(String::new());

        let mut user_to_uid: HashMap<String, u32> = HashMap::new();
        let mut users_dict: Vec<String> = Vec::new();
        let mut detail_docs: Vec<(u32, u32, u64, u32)> = Vec::new();
        let mut top_dirs_by_uid: HashMap<u32, Vec<(u32, i64)>> = HashMap::new();
        let mut team_ids_by_uid: HashMap<u32, u32> = HashMap::new();

        let seed_dir = detail_work_root.join("index_seed");
        fs::create_dir_all(&seed_dir)
            .map_err(|e| PyRuntimeError::new_err(format!("mkdir index_seed: {}", e)))?;
        let seed_docs_path = seed_dir.join("docs.bin");
        let seed_docs_file = fs::File::create(&seed_docs_path)
            .map_err(|e| PyRuntimeError::new_err(format!("create {}: {}", seed_docs_path.display(), e)))?;
        let mut seed_docs_writer = BufWriter::with_capacity(8 * 1024 * 1024, seed_docs_file);
        write_seed_header(&mut seed_docs_writer).map_err(PyRuntimeError::new_err)?;
        println!("[Phase 2] Writing detail chunks in semi-streaming mode...");
        for username in user_keys {
            let user = users
                .remove(&username)
                .unwrap_or_default();
            let safe = safe_user_dir(&username);
            let tmp_dir = detail_work_root.join("users").join(format!(".tmp_{}", safe));
            let final_dir = detail_work_root.join("users").join(&safe);
            if tmp_dir.exists() {
                let _ = fs::remove_dir_all(&tmp_dir);
            }
            let spills = row_spills.remove(&username).unwrap_or_default();

            let chunk_jobs = build_chunk_jobs_from_spills(
                &detail_work_root,
                &username,
                &spills,
                &mut global_path_to_id,
                &mut global_paths,
                &mut global_parent_path_id,
            )
            .map_err(PyRuntimeError::new_err)?;

            let mut top_dirs_idx: Vec<(usize, i64)> = Vec::with_capacity(user.top_dirs.len());
            for (path, used) in user.top_dirs {
                let path_id = ensure_path_id(
                    &path,
                    &mut global_path_to_id,
                    &mut global_paths,
                    &mut global_parent_path_id,
                );
                top_dirs_idx.push((path_id, used));
            }

            let top_dirs_for_user = top_dirs_idx.clone();

            let meta = UserJobMeta {
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
            };

            let chunk_results = pool
                .install(|| {
                    chunk_jobs
                        .into_par_iter()
                        .map(build_one_file_chunk)
                        .collect::<Result<Vec<_>, String>>()
                })
                .map_err(PyRuntimeError::new_err)?;
            let one_user = finalize_user_outputs(vec![meta], chunk_results)
                .map_err(PyRuntimeError::new_err)?;

            let uid = if let Some(found) = user_to_uid.get(&username) {
                *found
            } else {
                let id = users_dict.len() as u32;
                users_dict.push(username.clone());
                user_to_uid.insert(username.clone(), id);
                id
            };

            let team_id_num = team_map
                .get(&username)
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(0);
            team_ids_by_uid.insert(uid, team_id_num);
            top_dirs_by_uid.insert(
                uid,
                top_dirs_for_user
                    .iter()
                    .map(|(path_id, bytes)| (*path_id as u32, *bytes))
                    .collect(),
            );

            for path in &spills {
                let file = fs::File::open(path)
                    .map_err(|e| PyRuntimeError::new_err(format!("open spill {}: {}", path.display(), e)))?;
                let mut reader = BufReader::with_capacity(1024 * 1024, file);
                loop {
                    let mut head = [0u8; 12];
                    match reader.read_exact(&mut head) {
                        Ok(()) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(err) => {
                            return Err(PyRuntimeError::new_err(format!(
                                "read spill header {}: {}",
                                path.display(),
                                err
                            )))
                        }
                    }
                    let size = u64::from_le_bytes(head[0..8].try_into().unwrap_or([0u8; 8]));
                    let path_len = u32::from_le_bytes(head[8..12].try_into().unwrap_or([0u8; 4])) as usize;
                    let mut path_bytes = vec![0u8; path_len];
                    reader.read_exact(&mut path_bytes).map_err(|err| {
                        PyRuntimeError::new_err(format!("read spill path {}: {}", path.display(), err))
                    })?;
                    let raw_path = String::from_utf8_lossy(&path_bytes).to_string();
                    let safe_path = crate::sanitise_path(&raw_path);
                    let path_id = ensure_path_id(
                        &safe_path,
                        &mut global_path_to_id,
                        &mut global_paths,
                        &mut global_parent_path_id,
                    ) as u32;
                    let ext = crate::pipe_types::extension_for_path(&safe_path);
                    let eid = if let Some(id) = ext_to_id.get(&ext) {
                        *id
                    } else {
                        let id = exts.len() as u32;
                        exts.push(ext.clone());
                        ext_to_id.insert(ext, id);
                        id
                    };
                    write_seed_record(&mut seed_docs_writer, uid, path_id, size, eid)
                        .map_err(PyRuntimeError::new_err)?;
                    detail_docs.push((uid, path_id, size, eid));
                }
            }
            for path in &spills {
                let _ = fs::remove_file(path);
            }
            user_results.extend(one_user);
            done_users += 1;
            let pct = (done_users as f64 / total_users as f64) * 100.0;
            print!(
                "\r[Phase 2] Detail reports: {}/{} users ({:.1}%) ... ",
                done_users, total_users, pct
            );
            let _ = std::io::stdout().flush();
        }
        println!();
        t_chunk_parallel = t3.elapsed().as_secs_f64();

        let t4 = Instant::now();
        user_results.sort_by(|a, b| a.username.cmp(&b.username));
        let total_files_processed: i64 = user_results.iter().map(|u| u.total_files).sum();

        let t5 = Instant::now();
        write_paths_binary(&seed_dir.join("paths.bin"), &global_paths)
            .map_err(PyRuntimeError::new_err)?;
        let mut exts_file = fs::File::create(seed_dir.join("exts.bin"))
            .map_err(|e| PyRuntimeError::new_err(format!("create exts.bin: {}", e)))?;
        write_dict_binary(&mut exts_file, &exts)
            .map_err(|e| PyRuntimeError::new_err(format!("write exts.bin: {}", e)))?;
        let mut users_file = fs::File::create(seed_dir.join("users.bin"))
            .map_err(|e| PyRuntimeError::new_err(format!("create users.bin: {}", e)))?;
        write_dict_header_16(&mut users_file, MAGIC_USER, 1, users_dict.len() as u32)
            .map_err(|e| PyRuntimeError::new_err(format!("write users.bin header: {}", e)))?;
        write_dict_offsets_blob(&mut users_file, &users_dict)
            .map_err(|e| PyRuntimeError::new_err(format!("write users.bin: {}", e)))?;

        // New spec files under detail_users/{dict,cols}
        // Ensure permission events are representable in ids before dict/cols/agg output.
        let mut errcode_map: HashMap<String, u16> = HashMap::new();
        let mut perm_records: Vec<(u32, u32, u16, u8)> = Vec::new();
        let mut perm_sorted = perm_events.clone();
        perm_sorted.sort_by(|a, b| {
            a.uid
                .cmp(&b.uid)
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.errcode.cmp(&b.errcode))
                .then_with(|| a.kind.cmp(&b.kind))
        });
        for event in &perm_sorted {
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
                team_ids_by_uid.insert(
                    id,
                    team_map
                        .get(&username)
                        .and_then(|v| v.parse::<u32>().ok())
                        .unwrap_or(0),
                );
                id
            };
            let safe_path = crate::sanitise_path(&event.path);
            let path_id = ensure_path_id(
                &safe_path,
                &mut global_path_to_id,
                &mut global_paths,
                &mut global_parent_path_id,
            ) as u32;
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

        write_detail_users_dict_all(
            &detail_work_root.join("dict"),
            &global_paths,
            &global_parent_path_id,
            &exts,
            &users_dict,
        )
        .map_err(PyRuntimeError::new_err)?;
        write_detail_users_cols(&detail_work_root.join("cols"), &detail_docs)
            .map_err(PyRuntimeError::new_err)?;

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
        let mut uid_totals_rows: Vec<(u32, u64, u64, u64)> = uid_totals_map
            .iter()
            .map(|(uid, (files, bytes, dirs))| (*uid, *files, *bytes, *dirs))
            .collect();
        uid_totals_rows.sort_by_key(|(uid, _, _, _)| *uid);

        let mut dir_user_rows: Vec<(u32, u32, u64)> = Vec::new();
        for (dir_path, users_sizes) in &dir_sizes {
            let dir_id = ensure_path_id(
                dir_path,
                &mut global_path_to_id,
                &mut global_paths,
                &mut global_parent_path_id,
            ) as u32;
            for (username, bytes) in users_sizes {
                if *bytes <= 0 {
                    continue;
                }
                if let Some(uid_id) = user_to_uid.get(username) {
                    dir_user_rows.push((dir_id, *uid_id, *bytes as u64));
                }
            }
        }
        dir_user_rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        write_detail_users_agg(
            &detail_work_root.join("agg"),
            &uid_totals_rows,
            &dir_user_rows,
            &perm_records,
        )
        .map_err(PyRuntimeError::new_err)?;

        let mut uid_order: Vec<u32> = (0..users_dict.len() as u32).collect();
        uid_order.sort_unstable();

        let mut user_files_map: HashMap<u32, Vec<(u32, u64)>> = HashMap::new();
        for (uid_id, path_id, size, _eid) in &detail_docs {
            user_files_map
                .entry(*uid_id)
                .or_default()
                .push((*path_id, *size));
        }

        write_detail_user_bins(
            &detail_work_root.join("users"),
            &uid_order,
            &team_ids_by_uid,
            &uid_totals_map,
            &top_dirs_by_uid,
            &user_files_map,
            FILE_PART_RECORDS,
        )
        .map_err(PyRuntimeError::new_err)?;

        write_detail_manifest(
            &detail_work_root,
            &user_results,
            &treemap_root,
            timestamp,
            total_files_processed,
            global_paths.len(),
            exts.len(),
            detail_docs.len(),
            uid_totals_rows.len(),
            dir_user_rows.len(),
            perm_records.len(),
        )?;
        t_finalize = t4.elapsed().as_secs_f64();

        // Build all_paths for tree_map
        let all_paths: Vec<(String, u32)> = global_paths
            .iter()
            .enumerate()
            .map(|(i, p)| (p.clone(), i as u32))
            .collect();
        let path_to_id_tm: HashMap<String, u32> = all_paths.iter().map(|(p, id)| (p.clone(), *id)).collect();
        let (node_count, child_count, top_root_count, name_count) = write_treemap_bins(
            &tree_work_dir,
            &treemap_root,
            &dir_sizes,
            &dir_owner_map,
            &all_paths,
            &path_to_id_tm,
        )
        .map_err(PyRuntimeError::new_err)?;
        write_treemap_manifest(
            &tree_work_dir,
            node_count,
            child_count,
            top_root_count,
            name_count,
            timestamp,
        )
        .map_err(PyRuntimeError::new_err)?;

        seed_docs_writer
            .flush()
            .map_err(|e| PyRuntimeError::new_err(format!("flush docs.bin: {}", e)))?;
        t_paths_global_write = t5.elapsed().as_secs_f64();

        let t6 = Instant::now();
        crate::phase_index::build_mmi_index(&detail_work_root)
            .map_err(PyRuntimeError::new_err)?;
        t_index_build = t6.elapsed().as_secs_f64();

        let t7 = Instant::now();
        let perm_out_dir = detail_work_root.parent().unwrap_or(Path::new(".")).to_path_buf();
        write_permission_issues_json(&perm_events, &uids_map, &perm_out_dir, &treemap_root, timestamp)?;
        t_perm_write = t7.elapsed().as_secs_f64();

        let t8 = Instant::now();
        swap_dir_atomic(&detail_work_root, &detail_root)?;
        t_swap_detail = t8.elapsed().as_secs_f64();

        if build_treemap {
            println!("[Phase 2] Detail/index done. Starting deferred TreeMap build...");
            let t9 = Instant::now();
            write_treemap_json_outputs(
                &treemap_root,
                dir_sizes,
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
            let _ = fs::remove_dir_all(&tree_work_dir);
        }

        let _ = fs::remove_dir_all(&spool_root);

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
            println!("  Chunk parallel:     {:.4}s", t_chunk_parallel);
            println!("  Finalize+manifest:  {:.4}s", t_finalize);
            println!("  Paths dict write:   {:.4}s", t_paths_global_write);
            println!("  Index build:        {:.4}s", t_index_build);
            println!("  Perm JSON write:    {:.4}s", t_perm_write);
            println!("  Swap detail dir:    {:.4}s", t_swap_detail);
            println!("  TreeMap build:      {:.4}s", t_tree);
            println!("  Swap treemap dir:   {:.4}s", t_swap_tree);
            println!("  Peak RSS:           {:.1} MB", rss_mb);
        }
        Ok(total_files_processed as u64)
    })
}
