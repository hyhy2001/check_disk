use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::pipe_io::{recreate_dir, write_json_file, write_json_file_result};

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
    let mut all_dirs: HashMap<String, ()> = HashMap::new();
    all_dirs.insert(root.clone(), ());

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
    println!("[Phase 2] TreeMap: building {} shard(s)...", paths.len());
    let path_to_shard: HashMap<String, String> = paths.iter()
        .map(|path| (path.clone(), shard_id_for_path(path)))
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
    for prefix in 0..=255usize {
        let _ = fs::create_dir_all(shards_dir.join(format!("{:02x}", prefix)));
    }
    let total_paths = paths.len().max(1);
    let progress = AtomicUsize::new(0);
    let written = AtomicUsize::new(0);
    let bucket_map: Arc<Mutex<HashMap<String, HashMap<String, serde_json::Value>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    paths.par_iter().try_for_each(|path| -> Result<(), String> {
        let shard_id = path_to_shard.get(path).ok_or_else(|| format!("missing shard id for {}", path))?;
        let children = shard_children_json(path, &parent_to_children, &recursive_sizes, &dir_owner_map, &path_to_shard, min_size_bytes);
        if !children.is_empty() || path == &root {
            let prefix = shard_bucket_prefix(shard_id);
            let mut guard = bucket_map.lock().map_err(|_| "bucket map lock poisoned".to_string())?;
            guard
                .entry(prefix.to_string())
                .or_default()
                .insert(path.clone(), json!(children));
            written.fetch_add(1, Ordering::Relaxed);
        }
        let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
        if done % 10_000 == 0 || done == total_paths {
            let percent = (done as f64 / total_paths as f64) * 100.0;
            print!("\r[Phase 2] TreeMap shards: {}/{} ({:.1}%) ... ", done, total_paths, percent);
        }
        Ok(())
    }).map_err(PyRuntimeError::new_err)?;
    println!();
    let buckets = bucket_map
        .lock()
        .map_err(|_| PyRuntimeError::new_err("bucket map lock poisoned"))?;
    for (prefix, payload) in buckets.iter() {
        write_json_file_result(
            &shards_dir.join(prefix).join("bucket.json"),
            &json!(payload),
        ).map_err(PyRuntimeError::new_err)?;
    }
    let shard_count = written.load(Ordering::Relaxed) as u64;
    let bucket_count = buckets.len() as u64;

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
            "format": "check-disk-treemap-shards-bucketed-json",
            "manifest": "tree_map_data/manifest.json"
        }
    });
    write_json_file(Path::new(json_path), &root_node)?;

    write_json_file(&data_dir.join("api").join("shards_manifest.json"), &json!({
        "version": 2,
        "format": "check-disk-treemap-shards-bucketed-json",
        "root_shard_id": path_to_shard.get(&root).cloned().unwrap_or_else(|| "000000".to_string()),
        "shard_count": shard_count,
        "bucket_count": bucket_count,
        "shard_lookup_key": "path",
        "shard_path_template": "shards/{prefix}/bucket.json"
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

fn shard_id_for_path(path: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn shard_bucket_prefix(shard_id: &str) -> &str {
    if shard_id.len() >= 2 {
        &shard_id[..2]
    } else {
        "00"
    }
}
