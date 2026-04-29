use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use serde_json::json;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::_aggregate_tsv_to_map;
use crate::glob_module_rust_many;

const FILE_PART_RECORDS: usize = 100_000;
const TOP_RECORDS: usize = 1_000;

#[derive(Default)]
struct UserOutputMeta {
    team_id: String,
    total_dirs: i64,
    total_used: i64,
    top_dirs: Vec<(String, i64)>,
}

struct UserJobMeta {
    username: String,
    team_id: String,
    final_dir: PathBuf,
    tmp_dir: PathBuf,
    total_dirs: i64,
    total_used: i64,
    top_dirs: Vec<(String, i64)>,
    timestamp: i64,
}

struct FileChunkJob {
    username: String,
    chunk_index: usize,
    chunk_path: String,
    output_dir: PathBuf,
}

struct FileChunkResult {
    username: String,
    chunk_index: usize,
    total_files: i64,
    file_parts: Vec<serde_json::Value>,
    extension_stats: HashMap<String, (i64, i64)>,
    top_files: Vec<(u64, String)>,
}

struct UserBuildResult {
    username: String,
    team_id: String,
    total_dirs: i64,
    total_files: i64,
    total_used: i64,
}

#[pyfunction(signature = (tmpdir, uids_map, team_map, unified_db_path, treemap_json, treemap_db, treemap_root, max_level, min_size_bytes, timestamp, max_workers, debug=false))]
#[allow(clippy::too_many_arguments)]
pub fn build_unified_dbs(
    py: Python<'_>,
    tmpdir: String,
    uids_map: HashMap<u32, String>,
    team_map: HashMap<String, String>,
    unified_db_path: String,
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
        let uids: Vec<u32> = uids_map.keys().copied().collect();

        let detail_root = Path::new(&unified_db_path)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("detail_users"));
        recreate_dir(&detail_root)?;
        ensure_dir(&detail_root.join("users"))?;

        let tree_data_dir = PathBuf::from(&treemap_db);
        recreate_dir(&tree_data_dir)?;

        let pattern = format!("{}/dirs_t*.tsv", tmpdir);
        let mut dirs_tsv: Vec<String> = glob::glob(&pattern)
            .map_err(|e| PyRuntimeError::new_err(format!("glob: {}", e)))?
            .filter_map(|entry| entry.ok())
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        dirs_tsv.sort();

        let dir_sizes = _aggregate_tsv_to_map(&dirs_tsv, &uids_map)?;
        let mut dir_owner_map: HashMap<String, String> = HashMap::with_capacity(dir_sizes.len());
        let mut users: HashMap<String, UserOutputMeta> = HashMap::new();

        for (dpath, user_sizes) in &dir_sizes {
            let mut d_max_size = 0_i64;
            let mut d_max_user = String::new();
            for (owner, &size) in user_sizes {
                if size <= 0 {
                    continue;
                }
                if size > d_max_size {
                    d_max_size = size;
                    d_max_user = owner.clone();
                }
                let entry = users.entry(owner.clone()).or_insert_with(|| UserOutputMeta {
                    team_id: team_map.get(owner).cloned().unwrap_or_default(),
                    ..Default::default()
                });
                entry.total_dirs += 1;
                entry.total_used += size;
                entry.top_dirs.push((dpath.clone(), size));
            }
            if !d_max_user.is_empty() {
                dir_owner_map.insert(dpath.clone(), d_max_user);
            }
        }

        let mut chunks_by_uid = group_chunks_by_uid(&tmpdir, &uids)?;
        for chunks in chunks_by_uid.values_mut() {
            chunks.sort();
        }

        for (&uid, chunks) in &chunks_by_uid {
            let username = uids_map.get(&uid).cloned().unwrap_or_else(|| format!("uid-{}", uid));
            users.entry(username.clone()).or_insert_with(|| UserOutputMeta {
                team_id: team_map.get(&username).cloned().unwrap_or_default(),
                ..Default::default()
            });
            if chunks.is_empty() {
                continue;
            }
        }

        let (user_metas, chunk_jobs) = build_output_jobs(&detail_root, users, chunks_by_uid, &uids_map, &team_map, timestamp);

        let tree_handle = std::thread::spawn({
            let treemap_root = treemap_root.clone();
            let treemap_json = treemap_json.clone();
            let tree_data_dir = tree_data_dir.clone();
            move || write_treemap_json_outputs(
                &treemap_root,
                dir_sizes,
                dir_owner_map,
                &treemap_json,
                &tree_data_dir,
                max_level.max(1),
                min_size_bytes,
            )
        });

        let detail_workers = max_workers.max(1).min(chunk_jobs.len().max(1));
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(detail_workers)
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("detail thread pool: {}", e)))?;
        let chunk_results = pool.install(|| {
            chunk_jobs
                .into_par_iter()
                .map(build_one_file_chunk)
                .collect::<Result<Vec<_>, String>>()
        }).map_err(PyRuntimeError::new_err)?;
        let mut user_results = finalize_user_outputs(user_metas, chunk_results)
            .map_err(PyRuntimeError::new_err)?;

        match tree_handle.join() {
            Ok(result) => {
                result?;
            }
            Err(_) => return Err(PyRuntimeError::new_err("treemap writer thread panicked")),
        }

        user_results.sort_by(|a, b| a.username.cmp(&b.username));
        let total_files_processed: i64 = user_results.iter().map(|u| u.total_files).sum();
        write_detail_manifest(&detail_root, &user_results, &treemap_root, timestamp, total_files_processed)?;

        if debug {
            println!(
                "JSON/NDJSON outputs built in {:.2}s with {} detail workers. Total files: {}",
                t_all.elapsed().as_secs_f64(),
                detail_workers,
                total_files_processed
            );
        }
        Ok(total_files_processed as u64)
    })
}

fn recreate_dir(path: &Path) -> PyResult<()> {
    if path.exists() {
        fs::remove_dir_all(path).map_err(|e| PyRuntimeError::new_err(format!("rm dir {}: {}", path.display(), e)))?;
    }
    fs::create_dir_all(path).map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", path.display(), e)))
}

fn ensure_dir(path: &Path) -> PyResult<()> {
    fs::create_dir_all(path).map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", path.display(), e)))
}

fn safe_user_dir(username: &str) -> String {
    username
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

fn json_line_result(writer: &mut BufWriter<fs::File>, value: serde_json::Value) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, &value).map_err(|e| format!("json write: {}", e))?;
    writer.write_all(b"\n").map_err(|e| format!("write newline: {}", e))
}

fn group_chunks_by_uid(tmpdir: &str, uids: &[u32]) -> PyResult<HashMap<u32, Vec<String>>> {
    let chunk_files = glob_module_rust_many(tmpdir, uids)?;
    let mut chunks_by_uid: HashMap<u32, Vec<String>> = HashMap::new();
    for path in chunk_files {
        let filename = Path::new(&path)
            .file_name()
            .ok_or_else(|| PyRuntimeError::new_err(format!("invalid chunk path: {}", path)))?
            .to_string_lossy()
            .to_string();
        let uid = extract_uid_from_filename(&filename);
        chunks_by_uid.entry(uid).or_default().push(path);
    }
    Ok(chunks_by_uid)
}

fn build_output_jobs(
    detail_root: &Path,
    users: HashMap<String, UserOutputMeta>,
    chunks_by_uid: HashMap<u32, Vec<String>>,
    uids_map: &HashMap<u32, String>,
    team_map: &HashMap<String, String>,
    timestamp: i64,
) -> (Vec<UserJobMeta>, Vec<FileChunkJob>) {
    let mut chunks_by_user: HashMap<String, Vec<String>> = HashMap::new();
    for (uid, chunks) in chunks_by_uid {
        let username = uids_map.get(&uid).cloned().unwrap_or_else(|| format!("uid-{}", uid));
        chunks_by_user.entry(username).or_default().extend(chunks);
    }

    let mut metas = Vec::new();
    let mut chunk_jobs = Vec::new();
    for (username, user) in users {
        let safe = safe_user_dir(&username);
        let mut chunks = chunks_by_user.remove(&username).unwrap_or_default();
        chunks.sort();
        let tmp_dir = detail_root.join("users").join(format!(".tmp_{}", safe));
        let final_dir = detail_root.join("users").join(&safe);
        for (chunk_index, chunk_path) in chunks.into_iter().enumerate() {
            chunk_jobs.push(FileChunkJob {
                username: username.clone(),
                chunk_index,
                chunk_path,
                output_dir: tmp_dir.join("files").join(format!("chunk-{:05}", chunk_index)),
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

    for (username, mut chunks) in chunks_by_user {
        chunks.sort();
        let safe = safe_user_dir(&username);
        let tmp_dir = detail_root.join("users").join(format!(".tmp_{}", safe));
        let final_dir = detail_root.join("users").join(&safe);
        for (chunk_index, chunk_path) in chunks.into_iter().enumerate() {
            chunk_jobs.push(FileChunkJob {
                username: username.clone(),
                chunk_index,
                chunk_path,
                output_dir: tmp_dir.join("files").join(format!("chunk-{:05}", chunk_index)),
            });
        }
        metas.push(UserJobMeta {
            username: username.clone(),
            team_id: team_map.get(&username).cloned().unwrap_or_default(),
            final_dir,
            tmp_dir,
            total_dirs: 0,
            total_used: 0,
            top_dirs: Vec::new(),
            timestamp,
        });
    }
    metas.sort_by(|a, b| a.username.cmp(&b.username));
    chunk_jobs.sort_by(|a, b| a.username.cmp(&b.username).then(a.chunk_index.cmp(&b.chunk_index)));
    (metas, chunk_jobs)
}

fn build_one_file_chunk(job: FileChunkJob) -> Result<FileChunkResult, String> {
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

    let f = fs::File::open(&job.chunk_path).map_err(|e| format!("open {}: {}", job.chunk_path, e))?;
    let mut reader = crate::MergeReaderState::new(f);
    while let Some((size, raw_path)) = reader.next_entry() {
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
        let ext = extension_for_path(&safe);
        let stat = extension_stats.entry(ext.clone()).or_insert((0, 0));
        stat.0 += 1;
        stat.1 += size as i64;
        push_top_file(&mut top_files, size, &safe);
        json_line_result(
            current_writer.as_mut().expect("writer exists"),
            json!({"path": safe, "size": size, "ext": ext}),
        )?;
        current_records += 1;
        total_files += 1;
    }

    if let Some(mut writer) = current_writer.take() {
        writer.flush().map_err(|e| format!("flush file part: {}", e))?;
        if let Some(idx) = current_part_idx {
            file_parts[idx]["records"] = json!(current_records);
        }
    }

    let top_files_vec: Vec<(u64, String)> = top_files.into_iter().map(|Reverse(item)| item).collect();
    Ok(FileChunkResult {
        username: job.username,
        chunk_index: job.chunk_index,
        total_files,
        file_parts,
        extension_stats,
        top_files: top_files_vec,
    })
}

fn finalize_user_outputs(
    metas: Vec<UserJobMeta>,
    chunk_results: Vec<FileChunkResult>,
) -> Result<Vec<UserBuildResult>, String> {
    let mut results_by_user: HashMap<String, Vec<FileChunkResult>> = HashMap::new();
    for result in chunk_results {
        results_by_user.entry(result.username.clone()).or_default().push(result);
    }

    let mut user_results = Vec::new();
    for mut meta in metas {
        if meta.final_dir.exists() {
            fs::remove_dir_all(&meta.final_dir).map_err(|e| format!("rm final {}: {}", meta.final_dir.display(), e))?;
        }
        fs::create_dir_all(meta.tmp_dir.join("files")).map_err(|e| format!("mkdir user tmp {}: {}", meta.tmp_dir.display(), e))?;

        meta.top_dirs.sort_by(|a, b| b.1.cmp(&a.1));
        write_user_dirs_result(&meta.tmp_dir, &meta.top_dirs)?;

        let mut chunks = results_by_user.remove(&meta.username).unwrap_or_default();
        chunks.sort_by(|a, b| a.chunk_index.cmp(&b.chunk_index));
        let mut file_parts = Vec::new();
        let mut extension_stats: HashMap<String, (i64, i64)> = HashMap::new();
        let mut top_files: BinaryHeap<Reverse<(u64, String)>> = BinaryHeap::new();
        let mut total_files = 0_i64;

        for chunk in chunks {
            total_files += chunk.total_files;
            file_parts.extend(chunk.file_parts);
            for (ext, (count, size)) in chunk.extension_stats {
                let stat = extension_stats.entry(ext).or_insert((0, 0));
                stat.0 += count;
                stat.1 += size;
            }
            for (size, path) in chunk.top_files {
                push_top_file(&mut top_files, size, &path);
            }
        }

        write_user_metadata_result(&meta, total_files, file_parts, extension_stats, top_files)?;
        fs::rename(&meta.tmp_dir, &meta.final_dir)
            .map_err(|e| format!("rename {} -> {}: {}", meta.tmp_dir.display(), meta.final_dir.display(), e))?;

        user_results.push(UserBuildResult {
            username: meta.username,
            team_id: meta.team_id,
            total_dirs: meta.total_dirs,
            total_files,
            total_used: meta.total_used,
        });
    }
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
        .map(|(size, path)| json!({"path": path, "size": size, "ext": extension_for_path(path)}))
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

fn push_top_file(heap: &mut BinaryHeap<Reverse<(u64, String)>>, size: u64, path: &str) {
    heap.push(Reverse((size, path.to_string())));
    if heap.len() > TOP_RECORDS {
        heap.pop();
    }
}

fn extension_for_path(path: &str) -> String {
    Path::new(path)
        .extension()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default()
}

fn write_detail_manifest(
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

fn write_json_file(path: &Path, value: &serde_json::Value) -> PyResult<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let file = fs::File::create(path)
        .map_err(|e| PyRuntimeError::new_err(format!("create {}: {}", path.display(), e)))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, value)
        .map_err(|e| PyRuntimeError::new_err(format!("json {}: {}", path.display(), e)))?;
    writer.write_all(b"\n").map_err(|e| PyRuntimeError::new_err(format!("newline {}: {}", path.display(), e)))?;
    writer.flush().map_err(|e| PyRuntimeError::new_err(format!("flush {}: {}", path.display(), e)))
}

fn write_json_file_result(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
    }
    let file = fs::File::create(path).map_err(|e| format!("create {}: {}", path.display(), e))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, value).map_err(|e| format!("json {}: {}", path.display(), e))?;
    writer.write_all(b"\n").map_err(|e| format!("newline {}: {}", path.display(), e))?;
    writer.flush().map_err(|e| format!("flush {}: {}", path.display(), e))
}

fn write_treemap_json_outputs(
    root_dir: &str,
    dir_sizes: HashMap<String, HashMap<String, i64>>,
    dir_owner_map: HashMap<String, String>,
    json_path: &str,
    data_dir: &Path,
    max_level: usize,
    min_size_bytes: i64,
) -> PyResult<u64> {
    let root = normalize_root(root_dir);
    let mut direct_sizes: HashMap<String, i64> = HashMap::new();
    let mut all_dirs: HashMap<String, ()> = HashMap::new();
    all_dirs.insert(root.clone(), ());

    for (dpath, uid_sizes) in &dir_sizes {
        let total: i64 = uid_sizes.values().sum();
        if total <= 0 {
            continue;
        }
        let target = if tm_depth(dpath, &root) > max_level {
            tm_clamp(dpath, &root, max_level)
        } else {
            dpath.clone()
        };
        *direct_sizes.entry(target.clone()).or_insert(0) += total;
        let mut curr = target;
        loop {
            all_dirs.insert(curr.clone(), ());
            if curr == root {
                break;
            }
            let parent = tm_parent(&curr).to_string();
            if parent == curr {
                break;
            }
            curr = parent;
        }
    }

    let mut paths: Vec<String> = all_dirs.keys().cloned().collect();
    paths.sort();
    let path_to_shard: HashMap<String, String> = paths.iter().enumerate()
        .map(|(idx, path)| (path.clone(), format!("{:06}", idx)))
        .collect();

    let mut parent_to_children: HashMap<String, Vec<String>> = HashMap::new();
    for path in &paths {
        if path == &root {
            continue;
        }
        let parent = tm_parent(path).to_string();
        parent_to_children.entry(parent).or_default().push(path.clone());
    }

    let mut recursive_sizes = direct_sizes;
    let mut deepest = paths.clone();
    deepest.sort_by(|a, b| b.len().cmp(&a.len()));
    for path in deepest {
        if path == root {
            continue;
        }
        let size = recursive_sizes.get(&path).copied().unwrap_or(0);
        let parent = tm_parent(&path).to_string();
        *recursive_sizes.entry(parent).or_insert(0) += size;
    }

    let shards_dir = data_dir.join("shards");
    recreate_dir(&shards_dir)?;
    paths.par_iter().try_for_each(|path| -> Result<(), String> {
        let shard_id = path_to_shard.get(path).ok_or_else(|| format!("missing shard id for {}", path))?;
        let children = shard_children_json(path, &parent_to_children, &recursive_sizes, &dir_owner_map, &path_to_shard, min_size_bytes);
        let prefix = &shard_id[..2];
        write_json_file_result(&shards_dir.join(prefix).join(format!("{}.json", shard_id)), &json!(children))
    }).map_err(PyRuntimeError::new_err)?;
    let shard_count = paths.len() as u64;

    let root_children = shard_children_json(&root, &parent_to_children, &recursive_sizes, &dir_owner_map, &path_to_shard, min_size_bytes);
    let root_node = json!({
        "version": 1,
        "format": "check-disk-treemap-json",
        "name": tm_basename(&root),
        "path": root,
        "value": recursive_sizes.get(&root).copied().unwrap_or(0),
        "type": "directory",
        "owner": dir_owner_map.get(&root).cloned().unwrap_or_else(|| "unknown".to_string()),
        "shard_id": path_to_shard.get(&root).cloned().unwrap_or_else(|| "000000".to_string()),
        "has_children": !root_children.is_empty(),
        "children": root_children,
        "shard_store": {
            "format": "check-disk-treemap-shards-json",
            "manifest": "tree_map_data/manifest.json"
        }
    });
    write_json_file(Path::new(json_path), &root_node)?;

    write_json_file(&data_dir.join("manifest.json"), &json!({
        "version": 1,
        "format": "check-disk-treemap-shards-json",
        "root_shard_id": path_to_shard.get(&root).cloned().unwrap_or_else(|| "000000".to_string()),
        "shard_count": shard_count,
        "shard_path_template": "shards/{prefix}/{shard_id}.json"
    }))?;

    Ok(shard_count)
}

fn shard_children_json(
    path: &str,
    parent_to_children: &HashMap<String, Vec<String>>,
    recursive_sizes: &HashMap<String, i64>,
    dir_owner_map: &HashMap<String, String>,
    path_to_shard: &HashMap<String, String>,
    min_size_bytes: i64,
) -> Vec<serde_json::Value> {
    let mut items: Vec<_> = parent_to_children.get(path).into_iter().flatten()
        .filter_map(|child| {
            let size = recursive_sizes.get(child).copied().unwrap_or(0);
            if size < min_size_bytes {
                return None;
            }
            Some(json!({
                "name": tm_basename(child),
                "path": child,
                "value": size,
                "type": "directory",
                "owner": dir_owner_map.get(child).cloned().unwrap_or_else(|| "unknown".to_string()),
                "shard_id": path_to_shard.get(child).cloned().unwrap_or_default(),
                "has_children": parent_to_children.contains_key(child)
            }))
        })
        .collect();
    items.sort_by(|a, b| b["value"].as_i64().unwrap_or(0).cmp(&a["value"].as_i64().unwrap_or(0)));
    items
}

fn normalize_root(root_dir: &str) -> String {
    let trimmed = root_dir.trim_end_matches('/');
    if trimmed.is_empty() { "/".to_string() } else { trimmed.to_string() }
}

fn tm_depth(path: &str, root: &str) -> usize {
    let rel = path.strip_prefix(root).unwrap_or(path).trim_matches('/');
    if rel.is_empty() { 0 } else { rel.split('/').count() }
}

fn tm_clamp(path: &str, root: &str, max_level: usize) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path).trim_matches('/');
    if rel.is_empty() || max_level == 0 {
        return root.to_string();
    }
    let parts: Vec<&str> = rel.split('/').take(max_level).collect();
    if root == "/" {
        format!("/{}", parts.join("/"))
    } else {
        format!("{}/{}", root.trim_end_matches('/'), parts.join("/"))
    }
}

fn tm_parent(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed == "/" || trimmed.is_empty() {
        return "/";
    }
    match trimmed.rfind('/') {
        Some(0) => "/",
        Some(idx) => &trimmed[..idx],
        None => "/",
    }
}

fn tm_basename(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed == "/" || trimmed.is_empty() {
        return trimmed.to_string();
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_string()
}

fn extract_uid_from_filename(filename: &str) -> u32 {
    if let Some(rest) = filename.strip_prefix("uid_") {
        if let Some(pivot) = rest.find("_t") {
            return rest[..pivot].parse().unwrap_or(0);
        }
    }
    if let Some(u_idx) = filename.rfind("_u") {
        if let Some(dot_idx) = filename[u_idx..].find('.') {
            let uid_str = &filename[u_idx + 2..u_idx + dot_idx];
            return uid_str.parse().unwrap_or(0);
        }
    }
    0
}
