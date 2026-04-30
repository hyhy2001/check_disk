use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::pipe_events::{
    for_each_dir_agg_in_file, for_each_scan_event_in_file, get_dir_agg_files, get_scan_event_files,
    read_permission_events,
};
use crate::pipe_io::{ensure_dir, recreate_dir};
use crate::pipe_permission::write_permission_issues_json;
use crate::pipe_treemap::write_treemap_json_outputs;
use crate::pipe_types::UserOutputMeta;
use crate::pipe_user_finalize::{finalize_user_outputs, write_detail_manifest};
use crate::pipe_user_jobs::{build_one_file_chunk, build_output_jobs};

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
        }

        let dir_agg_paths = get_dir_agg_files(&tmpdir)?;
        let has_dir_agg = !dir_agg_paths.is_empty();
        let (bin_paths, tsv_paths) = get_scan_event_files(&tmpdir)?;
        eprintln!("[Phase 2] Ingesting and mapping {} event streams...", bin_paths.len() + tsv_paths.len());
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
                },
                |mut agg, (path, is_bin)| {
                    let _ = for_each_scan_event_in_file(&path, is_bin, |event| {
                        let username = uids_map
                            .get(&event.uid)
                            .cloned()
                            .unwrap_or_else(|| format!("uid-{}", event.uid));
                        agg.rows_by_user
                            .entry(username.clone())
                            .or_default()
                            .push((event.size, event.path.clone()));
                        if !has_dir_agg {
                            if let Some(parent) = crate::pipe_types::parent_path(&event.path) {
                                let user_sizes = agg.dir_sizes.entry(parent).or_default();
                                *user_sizes.entry(username).or_insert(0) += event.size as i64;
                            }
                        }
                    });
                    agg
                },
            )
            .reduce(
                || LocalAgg {
                    dir_sizes: HashMap::new(),
                    rows_by_user: HashMap::new(),
                },
                |mut a, b| {
                    for (user, mut rows) in b.rows_by_user {
                        a.rows_by_user.entry(user).or_default().append(&mut rows);
                    }
                    for (dir, sizes_b) in b.dir_sizes {
                        let sizes_a = a.dir_sizes.entry(dir).or_default();
                        for (username, size) in sizes_b {
                            *sizes_a.entry(username).or_insert(0) += size;
                        }
                    }
                    a
                },
            );

        let dir_sizes_by_user = if has_dir_agg {
            eprintln!(
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
        let rows_by_user = merged_agg.rows_by_user;

        let mut dir_owner_map: HashMap<String, String> = HashMap::new();
        let mut users: HashMap<String, UserOutputMeta> = HashMap::new();

        eprintln!("[Phase 2] Grouping directory stats for {} paths...", dir_sizes.len());

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

        for username in rows_by_user.keys() {
            users
                .entry(username.clone())
                .or_insert_with(|| UserOutputMeta {
                    team_id: team_map.get(username).cloned().unwrap_or_default(),
                    ..Default::default()
                });
        }
        t_dir_build = t1.elapsed().as_secs_f64();

        let t2 = Instant::now();
        eprintln!("[Phase 2] Building file chunks & sorting per user ({} users)...", rows_by_user.len());
        let (user_metas, chunk_jobs) =
            build_output_jobs(&detail_root, users, rows_by_user, &team_map, timestamp);
        t_output_jobs = t2.elapsed().as_secs_f64();

        let t3 = Instant::now();
        let detail_workers = max_workers.max(1).min(chunk_jobs.len().max(1));
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(detail_workers)
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("detail thread pool: {}", e)))?;
        eprintln!("[Phase 2] Writing {} detail chunk chunks in parallel...", chunk_jobs.len());
        let chunk_results = pool
            .install(|| {
                chunk_jobs
                    .into_par_iter()
                    .map(build_one_file_chunk)
                    .collect::<Result<Vec<_>, String>>()
            })
            .map_err(PyRuntimeError::new_err)?;
        t_chunk_parallel = t3.elapsed().as_secs_f64();

        let t4 = Instant::now();
        let mut user_results = finalize_user_outputs(user_metas, chunk_results)
            .map_err(PyRuntimeError::new_err)?;
        t_finalize = t4.elapsed().as_secs_f64();

        eprintln!("[Phase 2] Detail reports completed. Starting TreeMap build...");

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
        eprintln!("[Phase 2] TreeMap build completed in {:.2}s", t_tree);

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
