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

    let mut paths: Vec<String> = all_dirs.into_iter().collect();
    paths.sort();
    println!("[Phase 2] TreeMap: building {} shard(s)...", paths.len());

    let mut has_children_paths: HashSet<String> = HashSet::new();
    for path in &paths {
        if path == &root {
            continue;
        }
        has_children_paths.insert(tm_parent(path).to_string());
    }

    let mut recursive_sizes = direct_sizes;
    paths.sort_by(|a, b| b.len().cmp(&a.len()));
    for path in &paths {
        if path == &root {
            continue;
        }
        let size = recursive_sizes.get(path).copied().unwrap_or(0);
        let parent = tm_parent(path).to_string();
        *recursive_sizes.entry(parent).or_insert(0) += size;
    }
    paths.sort();

    let shards_dir = data_dir.join("shards");
    recreate_dir(&shards_dir)?;

    let total_paths = paths.len().max(1);
    let progress = AtomicUsize::new(0);
    let mut writers: HashMap<String, BufWriter<File>> = HashMap::new();
    let mut records_per_prefix: HashMap<String, u64> = HashMap::new();
    let mut total_records: u64 = 0;

    for path in &paths {
        let size = recursive_sizes.get(path).copied().unwrap_or(0);
        if size < min_size_bytes && path != &root {
            let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
            if done % 10_000 == 0 || done == total_paths {
                let percent = (done as f64 / total_paths as f64) * 100.0;
                print!("\r[Phase 2] TreeMap shards: {}/{} ({:.1}%) ... ", done, total_paths, percent);
            }
            continue;
        }

        let shard_id = shard_id_for_path(path);
        let prefix = shard_bucket_prefix(&shard_id);
        let file = writers.entry(prefix.clone()).or_insert_with(|| {
            let bucket_dir = shards_dir.join(&prefix);
            fs::create_dir_all(&bucket_dir).expect("failed to create treemap shard dir");
            let p = bucket_dir.join("bucket.ndjson");
            let inner = File::create(p).expect("failed to create treemap shard ndjson");
            BufWriter::with_capacity(BUF_SIZE, inner)
        });

        let row = json!({
            "id": shard_id,
            "n": tm_basename(path),
            "v": size,
            "o": dir_owner_map.get(path).cloned().unwrap_or_else(|| "unknown".to_string()),
            "p": path,
            "h": has_children_paths.contains(path),
            "t": "d"
        });

        serde_json::to_writer(&mut *file, &row)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to write treemap row: {}", e)))?;
        file.write_all(b"\n")
            .map_err(|e| PyRuntimeError::new_err(format!("failed to terminate treemap row: {}", e)))?;

        *records_per_prefix.entry(prefix).or_insert(0) += 1;
        total_records += 1;

        let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
        if done % 10_000 == 0 || done == total_paths {
            let percent = (done as f64 / total_paths as f64) * 100.0;
            print!("\r[Phase 2] TreeMap shards: {}/{} ({:.1}%) ... ", done, total_paths, percent);
        }
    }
    println!();

    let root_shard_id = shard_id_for_path(&root);
    let root_node = json!({
        "version": 1,
        "format": "check-disk-treemap-json",
        "name": tm_basename(&root),
        "path": root,
        "value": recursive_sizes.get(&root).copied().unwrap_or(0),
        "type": "directory",
        "owner": dir_owner_map.get(&root).cloned().unwrap_or_else(|| "unknown".to_string()),
        "shard_id": root_shard_id,
        "has_children": has_children_paths.contains(&root),
        "children": [],
        "shard_store": {
            "format": "check-disk-treemap-shards-ndjson",
            "manifest": "tree_map_data/manifest.json"
        }
    });
    write_json_file(Path::new(json_path), &root_node)?;

    write_json_file(
        &data_dir.join("api").join("shards_manifest.json"),
        &json!({
            "version": 3,
            "format": "check-disk-treemap-shards-ndjson",
            "root_shard_id": root_shard_id,
            "shard_count": total_records,
            "bucket_count": records_per_prefix.len(),
            "shard_lookup_key": "id",
            "shard_path_template": "shards/{prefix}/bucket.ndjson"
        }),
    )?;

    write_json_file(
        &data_dir.join("manifest.json"),
        &json!({
            "schema": "check-disk-detail-treemap",
            "version": 3,
            "files": {
                "api/shards_manifest.json": {"records": 1},
                "shards": {"records": total_records}
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
