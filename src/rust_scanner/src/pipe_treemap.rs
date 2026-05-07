use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

const BUF_SIZE: usize = 256 * 1024;

use crate::pipe_io::{recreate_dir, write_json_file};

pub fn write_treemap_json_outputs(
    root_dir: &str,
    dir_sizes: HashMap<String, Vec<(String, i64)>>,
    dir_owner_map: HashMap<String, String>,
    json_path: &str,
    data_dir: &Path,
    max_level: usize,
    min_size_bytes: i64,
) -> PyResult<u64> {
    println!("[Phase 2] TreeMap: grouping directories...");
    let root = normalize_root(root_dir);
    let mut direct_sizes: HashMap<String, i64> = HashMap::new();
    let mut direct_owner_weights: HashMap<String, HashMap<String, i64>> = HashMap::new();
    let mut all_dirs: HashSet<String> = HashSet::new();
    all_dirs.insert(root.clone());

    for (dpath, uid_sizes) in &dir_sizes {
        let total: i64 = uid_sizes.iter().map(|(_, size)| *size).sum();
        if total <= 0 {
            continue;
        }
        let target = if tm_depth(dpath, &root) > max_level {
            tm_clamp(dpath, &root, max_level)
        } else {
            dpath.clone()
        };
        *direct_sizes.entry(target.clone()).or_insert(0) += total;
        let owner_bucket = direct_owner_weights.entry(target.clone()).or_insert_with(HashMap::new);
        for (owner, size) in uid_sizes {
            if *size <= 0 {
                continue;
            }
            *owner_bucket.entry(owner.clone()).or_insert(0) += *size;
        }

        let mut curr = target;
        loop {
            all_dirs.insert(curr.clone());
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

    let base_paths: Vec<String> = all_dirs.iter().cloned().collect();

    let mut has_children_paths: HashSet<String> = HashSet::new();
    for path in &base_paths {
        if path == &root {
            continue;
        }
        has_children_paths.insert(tm_parent(path).to_string());
    }

    let mut recursive_sizes = direct_sizes.clone();
    let mut recursive_owner_weights = direct_owner_weights;

    let mut descending_paths = base_paths.clone();
    descending_paths.sort_by(|a, b| b.len().cmp(&a.len()));
    for path in &descending_paths {
        if path == &root {
            continue;
        }

        let size = recursive_sizes.get(path).copied().unwrap_or(0);
        let parent = tm_parent(path).to_string();
        *recursive_sizes.entry(parent.clone()).or_insert(0) += size;

        if let Some(weights) = recursive_owner_weights.get(path).cloned() {
            let parent_weights = recursive_owner_weights.entry(parent).or_insert_with(HashMap::new);
            for (owner, weight) in weights {
                *parent_weights.entry(owner).or_insert(0) += weight;
            }
        }
    }

    let mut resolved_owner_by_path: HashMap<String, String> = HashMap::new();
    for path in &base_paths {
        if let Some(owner) = dir_owner_map.get(path) {
            if !owner.is_empty() {
                resolved_owner_by_path.insert(path.clone(), owner.clone());
                continue;
            }
        }

        if let Some(weights) = recursive_owner_weights.get(path) {
            if let Some(owner) = tm_pick_owner(weights) {
                resolved_owner_by_path.insert(path.clone(), owner);
            }
        }
    }

    let root_owner = resolved_owner_by_path
        .get(&root)
        .cloned()
        .or_else(|| dir_owner_map.get(&root).cloned())
        .unwrap_or_else(|| "unknown".to_string());

    let mut paths = base_paths;

    let mut file_bucket_paths: Vec<String> = Vec::new();
    for path in &paths {
        let direct = direct_sizes.get(path).copied().unwrap_or(0);
        if direct <= 0 {
            continue;
        }
        let bucket_path = tm_files_bucket_path(path);
        file_bucket_paths.push(bucket_path.clone());
        has_children_paths.insert(path.clone());
        recursive_sizes.insert(bucket_path.clone(), direct);

        let owner = resolved_owner_by_path
            .get(path)
            .cloned()
            .unwrap_or_else(|| root_owner.clone());
        resolved_owner_by_path.insert(bucket_path, owner);
    }

    for bucket_path in file_bucket_paths {
        paths.push(bucket_path);
    }

    paths.sort();
    println!("[Phase 2] TreeMap: building {} shard(s)...", paths.len());

    for path in &paths {
        if resolved_owner_by_path.contains_key(path) {
            continue;
        }

        let mut cur = tm_parent(path).to_string();
        let mut owner = None;
        loop {
            if let Some(found) = resolved_owner_by_path.get(&cur).cloned() {
                owner = Some(found);
                break;
            }
            if cur == "/" {
                break;
            }
            let next = tm_parent(&cur).to_string();
            if next == cur {
                break;
            }
            cur = next;
        }
        resolved_owner_by_path.insert(path.clone(), owner.unwrap_or_else(|| root_owner.clone()));
    }

    resolved_owner_by_path
        .entry(root.clone())
        .or_insert_with(|| root_owner.clone());

    if direct_sizes.get(&root).copied().unwrap_or(0) > 0 {
        has_children_paths.insert(root.clone());
    }

    paths.sort_by(|a, b| {
        let a_bucket = tm_is_files_bucket(a);
        let b_bucket = tm_is_files_bucket(b);
        if a_bucket != b_bucket {
            return if a_bucket { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less };
        }
        a.cmp(b)
    });

    let has_children_paths = has_children_paths;
    let resolved_owner_by_path = resolved_owner_by_path;
    let recursive_sizes = recursive_sizes;
    let root_owner = root_owner;

    let paths = paths;

    let shards_dir = data_dir.join("shards");
    recreate_dir(&shards_dir)?;

    let total_paths = paths.len().max(1);
    let progress = AtomicUsize::new(0);
    let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut records_per_prefix: HashMap<String, u64> = HashMap::new();
    let mut bucket_offsets: HashMap<String, u64> = HashMap::new();
    let mut pid_seek_records: Vec<(u64, u64, u8, u64, u32)> = Vec::new();
    let mut total_records: u64 = 0;
    let mut path_to_gid: HashMap<String, u32> = HashMap::new();
    let mut gid_paths: Vec<String> = Vec::new();

    let mut ensure_gid = |p: &str| -> u32 {
        if let Some(existing) = path_to_gid.get(p) {
            return *existing;
        }
        let gid = gid_paths.len() as u32;
        gid_paths.push(p.to_string());
        path_to_gid.insert(p.to_string(), gid);
        gid
    };

    let progress_step = usize::max(1, total_paths / 10);
    let mut last_logged_done: usize = 0;

    for path in &paths {
        let size = recursive_sizes.get(path).copied().unwrap_or(0);
        if size < min_size_bytes && path != &root {
            let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
            if (done % progress_step == 0 || done == total_paths) && done != last_logged_done {
                let percent = (done as f64 / total_paths as f64) * 100.0;
                println!("[Phase 2] TreeMap shards: {}/{} ({:.1}%)", done, total_paths, percent);
                last_logged_done = done;
            }
            continue;
        }

        let is_files_bucket = tm_is_files_bucket(path);
        let logical_parent_path = if is_files_bucket {
            tm_parent_of_files_bucket(path).unwrap_or_else(|| root.clone())
        } else {
            tm_parent(path).to_string()
        };
        let shard_id = shard_id_for_path(path);
        let prefix = shard_bucket_prefix(&shard_id);
        let file = writers.entry(prefix.clone()).or_insert_with(|| {
            let bucket_dir = shards_dir.join(&prefix);
            fs::create_dir_all(&bucket_dir).expect("failed to create treemap shard dir");
            let p = bucket_dir.join("bucket.ndjson");
            let inner = File::create(p).expect("failed to create treemap shard ndjson");
            BufWriter::with_capacity(BUF_SIZE, inner)
        });

        let gid = ensure_gid(path);
        let _parent_gid = ensure_gid(&logical_parent_path);
        let owner = resolved_owner_by_path
            .get(path)
            .cloned()
            .unwrap_or_else(|| root_owner.clone());
        let has_children = !is_files_bucket && has_children_paths.contains(path);
        let parent_shard_id = shard_id_for_path(&logical_parent_path);
        let row = json!({
            "id": shard_id,
            "pid": parent_shard_id,
            "gid": gid,
            "n": if is_files_bucket { "[files]".to_string() } else { tm_basename(path) },
            "v": size,
            "o": owner,
            "h": has_children,
            "t": if is_files_bucket { "g" } else { "d" }
        });

        let mut row_bytes = serde_json::to_vec(&row)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to serialize treemap row: {}", e)))?;
        row_bytes.push(b'\n');
        let row_len = row_bytes.len() as u32;

        let row_offset = *bucket_offsets.entry(prefix.clone()).or_insert(0);
        file.write_all(&row_bytes)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write treemap row: {}", e)))?;
        *bucket_offsets.entry(prefix.clone()).or_insert(0) += row_len as u64;

        let pid_int = u64::from_str_radix(&parent_shard_id, 16).unwrap_or(0);
        let pid_hi = (pid_int >> 32) as u32 as u64;
        let pid_lo = (pid_int & 0xffff_ffff) as u64;
        let bucket_prefix = u8::from_str_radix(&prefix, 16).unwrap_or(0);
        pid_seek_records.push((pid_hi, pid_lo, bucket_prefix, row_offset, row_len));

        *records_per_prefix.entry(prefix).or_insert(0) += 1;
        total_records += 1;

        let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
        if done % progress_step == 0 || done == total_paths {
            let percent = (done as f64 / total_paths as f64) * 100.0;
            println!("[Phase 2] TreeMap shards: {}/{} ({:.1}%)", done, total_paths, percent);
        }
    }

    for writer in writers.values_mut() {
        writer
            .flush()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to flush treemap shard writer: {}", e)))?;
    }

    // Sort pid_seek records by (pid_hi, pid_lo) for binary search
    pid_seek_records.sort_by(|a, b| {
        match a.0.cmp(&b.0) {
            std::cmp::Ordering::Equal => a.1.cmp(&b.1),
            other => other,
        }
    });

    println!("[Phase 2] TreeMap: writing pid_seek index ({} records)...", pid_seek_records.len());
    let pid_seek_path = shards_dir.join("pid_seek.bin");
    let mut pid_seek_writer = BufWriter::with_capacity(
        BUF_SIZE,
        File::create(&pid_seek_path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create pid_seek.bin: {}", e)))?,
    );
    // Header: magic(4) + version u32(4) + count u64(8) = 16 bytes
    pid_seek_writer.write_all(b"PSDX")
        .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek magic: {}", e)))?;
    pid_seek_writer.write_all(&1u32.to_le_bytes())
        .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek version: {}", e)))?;
    pid_seek_writer.write_all(&(pid_seek_records.len() as u64).to_le_bytes())
        .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek count: {}", e)))?;
    // Records: pid_hi(u32) + pid_lo(u64) + bucket_prefix(u8) + offset(u64) + len(u32) = 25 bytes
    for (pid_hi, pid_lo, bucket_prefix, offset, len) in &pid_seek_records {
        pid_seek_writer.write_all(&(*pid_hi as u32).to_le_bytes())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek pid_hi: {}", e)))?;
        pid_seek_writer.write_all(&pid_lo.to_le_bytes())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek pid_lo: {}", e)))?;
        pid_seek_writer.write_all(&[*bucket_prefix])
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek bucket_prefix: {}", e)))?;
        pid_seek_writer.write_all(&offset.to_le_bytes())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek offset: {}", e)))?;
        pid_seek_writer.write_all(&len.to_le_bytes())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write pid_seek len: {}", e)))?;
    }
    pid_seek_writer.flush()
        .map_err(|e| PyRuntimeError::new_err(format!("failed to flush pid_seek.bin: {}", e)))?;
    drop(pid_seek_writer);

    let root_shard_id = shard_id_for_path(&root);
    let root_final_owner = resolved_owner_by_path.get(&root).cloned().unwrap_or(root_owner);
    let root_node = json!({
        "version": 1,
        "format": "check-disk-treemap-json",
        "name": "/",
        "path": root,
        "value": recursive_sizes.get(&root).copied().unwrap_or(0),
        "type": "directory",
        "owner": root_final_owner,
        "shard_id": root_shard_id,
        "has_children": has_children_paths.contains(&root),
        "children": [],
        "shard_store": {
            "format": "check-disk-treemap-shards-ndjson",
            "manifest": "tree_map_data/manifest.json"
        }
    });
    write_json_file(Path::new(json_path), &root_node)?;

    let api_dir = data_dir.join("api");
    fs::create_dir_all(&api_dir)
        .map_err(|e| PyRuntimeError::new_err(format!("failed to create treemap api dir: {}", e)))?;

    let path_dict_path = api_dir.join("path_dict.ndjson");
    let mut path_dict_writer = BufWriter::with_capacity(
        BUF_SIZE,
        File::create(&path_dict_path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create treemap path dict: {}", e)))?,
    );
    // Track byte offsets for each gid so we can write path_dict.seek
    let mut seek_records: Vec<(u32, u64, u32)> = Vec::with_capacity(gid_paths.len());
    let mut ndjson_offset: u64 = 0;
    for (gid, p) in gid_paths.iter().enumerate() {
        let row_bytes = {
            let mut buf = Vec::new();
            serde_json::to_writer(&mut buf, &json!({"gid": gid as u32, "p": p}))
                .map_err(|e| PyRuntimeError::new_err(format!("failed to serialize path dict row: {}", e)))?;
            buf.push(b'\n');
            buf
        };
        let row_len = row_bytes.len() as u32;
        seek_records.push((gid as u32, ndjson_offset, row_len));
        ndjson_offset += row_len as u64;
        path_dict_writer.write_all(&row_bytes)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write treemap path dict row: {}", e)))?;
    }
    path_dict_writer
        .flush()
        .map_err(|e| PyRuntimeError::new_err(format!("failed to flush treemap path dict: {}", e)))?;

    // Write path_dict.seek: binary index for O(1) gid->offset lookup
    // Format: magic "PDX1"(4) + version u32(4) + count u32(4) = 12 bytes header
    //         then N records of: gid(u32) + offset(u64) + len(u32) = 16 bytes each
    // Records are sorted by gid (naturally 0..N-1 so already sorted)
    let seek_path = api_dir.join("path_dict.seek");
    let mut seek_writer = BufWriter::with_capacity(
        BUF_SIZE,
        File::create(&seek_path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create path_dict.seek: {}", e)))?,
    );
    // Header
    seek_writer.write_all(b"PDX1")
        .map_err(|e| PyRuntimeError::new_err(format!("failed to write path_dict.seek magic: {}", e)))?;
    seek_writer.write_all(&1u32.to_le_bytes())
        .map_err(|e| PyRuntimeError::new_err(format!("failed to write path_dict.seek version: {}", e)))?;
    seek_writer.write_all(&(seek_records.len() as u32).to_le_bytes())
        .map_err(|e| PyRuntimeError::new_err(format!("failed to write path_dict.seek count: {}", e)))?;
    // Records
    for (gid, off, len) in &seek_records {
        seek_writer.write_all(&gid.to_le_bytes())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write seek gid: {}", e)))?;
        seek_writer.write_all(&off.to_le_bytes())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write seek offset: {}", e)))?;
        seek_writer.write_all(&len.to_le_bytes())
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write seek len: {}", e)))?;
    }
    seek_writer.flush()
        .map_err(|e| PyRuntimeError::new_err(format!("failed to flush path_dict.seek: {}", e)))?;
    drop(seek_writer);


    write_json_file(
        &api_dir.join("shards_manifest.json"),
        &json!({
            "version": 11,
            "format": "check-disk-treemap-shards-ndjson",
            "root_shard_id": root_shard_id,
            "shard_count": total_records,
            "bucket_count": records_per_prefix.len(),
            "shard_lookup_key": "id",
            "path_lookup_key": "gid",
            "path_dict": "api/path_dict.ndjson",
            "path_dict_seek": "api/path_dict.seek",
            "path_dict_seek_record_size": 16,
            "shard_path_template": "shards/{prefix}/bucket.ndjson",
            "pid_seek": "shards/pid_seek.bin",
            "pid_seek_record_size": 25
        }),
    )?;

    write_json_file(
        &data_dir.join("manifest.json"),
        &json!({
            "schema": "check-disk-detail-treemap",
            "version": 11,
            "files": {
                "api/shards_manifest.json": {"records": 1},
                "api/path_dict.ndjson": {"records": gid_paths.len()},
                "api/path_dict.seek": {"records": gid_paths.len()},
                "shards": {"records": total_records},
                "shards/pid_seek.bin": {"records": pid_seek_records.len()}
            }
        }),
    )?;

    Ok(total_records)
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

fn shard_id_for_path(path: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn shard_bucket_prefix(shard_id: &str) -> String {
    let prefix = shard_id
        .get(0..2)
        .and_then(|hex| u8::from_str_radix(hex, 16).ok())
        .map(|value| value & 0x7f)
        .unwrap_or(0);
    format!("{:02x}", prefix)
}

const FILES_BUCKET_SUFFIX: &str = "/__files__";

fn tm_files_bucket_path(dir_path: &str) -> String {
    if dir_path == "/" {
        return FILES_BUCKET_SUFFIX.to_string();
    }
    format!("{}{}", dir_path.trim_end_matches('/'), FILES_BUCKET_SUFFIX)
}

fn tm_is_files_bucket(path: &str) -> bool {
    path.ends_with(FILES_BUCKET_SUFFIX)
}

fn tm_parent_of_files_bucket(path: &str) -> Option<String> {
    if !tm_is_files_bucket(path) {
        return None;
    }
    let parent = path.trim_end_matches(FILES_BUCKET_SUFFIX);
    if parent.is_empty() {
        Some("/".to_string())
    } else {
        Some(parent.to_string())
    }
}

fn tm_pick_owner(weights: &HashMap<String, i64>) -> Option<String> {
    weights
        .iter()
        .filter(|(_, &v)| v > 0)
        .max_by_key(|(_, &v)| v)
        .map(|(owner, _)| owner.clone())
}
