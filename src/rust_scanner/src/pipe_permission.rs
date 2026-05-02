use pyo3::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;

use crate::pipe_io::{ensure_dir, write_json_file};
use crate::pipe_types::PermissionEvent;

pub fn write_permission_issues_json(
    events: &[PermissionEvent],
    uids_map: &HashMap<u32, String>,
    output_dir: &Path,
    directory: &str,
    timestamp: i64,
) -> PyResult<()> {
    let out_path = output_dir.join("permission_issues.json");

    let mut users_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    let mut unknown_items: Vec<serde_json::Value> = Vec::new();

    for evt in events {
        let item = json!({
            "path": evt.path,
            "type": evt.kind,
            "error": evt.errcode,
        });
        let owner = uids_map.get(&evt.uid).cloned().unwrap_or_else(|| {
            if evt.uid == 0 {
                "unknown".to_string()
            } else {
                format!("uid-{}", evt.uid)
            }
        });
        if owner == "unknown" {
            unknown_items.push(item);
        } else {
            users_map.entry(owner).or_default().push(item);
        }
    }

    let mut users: Vec<serde_json::Value> = users_map
        .into_iter()
        .map(|(name, items)| json!({"name": name, "inaccessible_items": items}))
        .collect();
    users.sort_by(|a, b| {
        a["name"]
            .as_str()
            .unwrap_or("")
            .cmp(b["name"].as_str().unwrap_or(""))
    });

    let report = json!({
        "date": timestamp,
        "directory": directory,
        "permission_issues": {
            "users": users,
            "unknown_items": unknown_items,
            "count": events.len(),
        }
    });

    ensure_dir(out_path.parent().unwrap_or(Path::new(".")))?;
    write_json_file(&out_path, &report)?;
    Ok(())
}
