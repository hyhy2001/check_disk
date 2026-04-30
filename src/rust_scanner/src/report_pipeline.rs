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

use crate::pipe_events::{
    for_each_dir_agg_in_file, for_each_scan_event_in_file, get_dir_agg_files, get_scan_event_files,
    read_permission_events,
};
use crate::pipe_io::{ensure_dir, recreate_dir, safe_user_dir};
use crate::pipe_permission::write_permission_issues_json;
use crate::pipe_treemap::write_treemap_json_outputs;
use crate::pipe_types::{FileChunkJob, UserJobMeta, UserOutputMeta, FILE_PART_RECORDS};
use crate::pipe_user_finalize::{finalize_user_outputs, write_detail_manifest};
use crate::pipe_user_jobs::build_one_file_chunk;

const ROW_SPILL_THRESHOLD: usize = 200_000;

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
        let file = match OpenOptions::new().create(true).write(true).truncate(true).open(&path) {
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
                || writer.write_all(&path_bytes[..(len as usize).min(path_bytes.len())]).is_err()
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
) -> Result<Vec<FileChunkJob>, String> {
    let safe = safe_user_dir(username);
    let tmp_dir = detail_root.join("users").join(format!(".tmp_{}", safe));
    let mut jobs = Vec::new();
    let mut rows = Vec::with_capacity(FILE_PART_RECORDS);
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
            let path = String::from_utf8_lossy(&path_bytes).to_string();
            rows.push((size, path));
            if rows.len() >= FILE_PART_RECORDS {
                jobs.push(FileChunkJob {
                    username: username.to_string(),
                    chunk_index,
                    output_dir: tmp_dir.join("files").join(format!("chunk-{:05}", chunk_index)),
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
            output_dir: tmp_dir.join("files").join(format!("chunk-{:05}", chunk_index)),
            rows,
        });
    }
    Ok(jobs)
}

#[pyfunction(signature = (tmpdir, uids_map, team_map, pipeline_db_path, treemap_json, treemap_db, treemap_root, max_level, min_size_bytes, timestamp, max_workers, debug=false))]
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
    debug: bool,
) -> PyResult<u64> {
    py.allow_threads(move || -> PyResult<u64> {
        let t_all = Instant::now();
        let t_perm_tsv: f64;
        let t_dir_build: f64;
        let t_output_jobs: f64;
        let t_chunk_parallel: f64;
        let t_finalize: f64;
        let t_tree: f64;
        let t_perm_write: f64;

        let detail_root = Path::new(&pipeline_db_path)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("detail_users"));
        recreate_dir(&detail_root)?;
        ensure_dir(&detail_root.join("users"))?;

        let tree_data_dir = PathBuf::from(&treemap_db);
        recreate_dir(&tree_data_dir)?;

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
        let spool_root = detail_root.join(".rows_spill");
        recreate_dir(&spool_root)?;
        let spill_seq = Arc::new(AtomicU64::new(0));

        let dir_agg_paths = get_dir_agg_files(&tmpdir)?;
        let has_dir_agg = !dir_agg_paths.is_empty();
        let (bin_paths, tsv_paths) = get_scan_event_files(&tmpdir)?;
        println!("[Phase 2] Ingesting and mapping {} event streams...", bin_paths.len() + tsv_paths.len());
        let mut all_tasks = Vec::new();
        for p in bin_paths {
            all_tasks.push((p, true));
        }
        for p in tsv_paths {
            all_tasks.push((p, false));
        }

        let merged_agg = all_tasks
            .into_par_iter()
            .fold(
                || LocalAgg {
                    dir_sizes: HashMap::new(),
                    rows_by_user: HashMap::new(),
                    row_spills: HashMap::new(),
                    row_count: 0,
                },
                |mut agg, (path, is_bin)| {
                    let spill_seq_ref = &spill_seq;
                    let _ = for_each_scan_event_in_file(&path, is_bin, |event| {
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
        println!("[Phase 2] Writing detail chunks in semi-streaming mode...");
        for username in user_keys {
            let user = users
                .remove(&username)
                .unwrap_or_default();
            let safe = safe_user_dir(&username);
            let tmp_dir = detail_root.join("users").join(format!(".tmp_{}", safe));
            let final_dir = detail_root.join("users").join(&safe);
            if tmp_dir.exists() {
                let _ = fs::remove_dir_all(&tmp_dir);
            }
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
                top_dirs: user.top_dirs,
                timestamp,
            };
            let spills = row_spills.remove(&username).unwrap_or_default();
            let chunk_jobs =
                build_chunk_jobs_from_spills(&detail_root, &username, &spills).map_err(PyRuntimeError::new_err)?;
            for path in spills {
                let _ = fs::remove_file(path);
            }
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
        t_finalize = t4.elapsed().as_secs_f64();

        println!("[Phase 2] Detail reports completed. Starting TreeMap build...");

        let t5 = Instant::now();
        write_treemap_json_outputs(
            &treemap_root,
            dir_sizes,
            dir_owner_map,
            &treemap_json,
            &tree_data_dir,
            max_level.max(1),
            min_size_bytes,
        )?;
        t_tree = t5.elapsed().as_secs_f64();
        println!("[Phase 2] TreeMap build completed in {:.2}s", t_tree);

        user_results.sort_by(|a, b| a.username.cmp(&b.username));
        let total_files_processed: i64 = user_results.iter().map(|u| u.total_files).sum();
        write_detail_manifest(
            &detail_root,
            &user_results,
            &treemap_root,
            timestamp,
            total_files_processed,
        )?;

        let t6 = Instant::now();
        let perm_out_dir = detail_root.parent().unwrap_or(Path::new(".")).to_path_buf();
        write_permission_issues_json(&perm_events, &uids_map, &perm_out_dir, &treemap_root, timestamp)?;
        t_perm_write = t6.elapsed().as_secs_f64();
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
            println!("  Finalize users:     {:.4}s", t_finalize);
            println!("  TreeMap build:      {:.4}s", t_tree);
            println!("  Perm JSON write:    {:.4}s", t_perm_write);
            println!("  Peak RSS:           {:.1} MB", rss_mb);
        }
        Ok(total_files_processed as u64)
    })
}
