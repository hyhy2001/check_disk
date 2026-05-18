// TreeMap path helpers — used by report_pipeline.rs to walk path strings.
// Heavy lifting (shard NDJSON, pid_seek index, etc.) is gone — output is now
// SQLite via `db_writer::build_treemap_db`.

use std::collections::HashMap;

pub fn normalize_root(root_dir: &str) -> String {
    let trimmed = root_dir.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn tm_depth(path: &str, root: &str) -> usize {
    let rel = path.strip_prefix(root).unwrap_or(path).trim_matches('/');
    if rel.is_empty() {
        0
    } else {
        rel.split('/').count()
    }
}

pub fn tm_clamp(path: &str, root: &str, max_level: usize) -> String {
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

pub fn tm_parent(path: &str) -> &str {
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

pub fn tm_basename(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed == "/" || trimmed.is_empty() {
        return trimmed.to_string();
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_string()
}

pub fn tm_pick_owner(weights: &HashMap<String, i64>) -> Option<String> {
    weights
        .iter()
        .filter(|(_, &v)| v > 0)
        .max_by_key(|(_, &v)| v)
        .map(|(owner, _)| owner.clone())
}
