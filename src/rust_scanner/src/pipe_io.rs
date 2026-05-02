use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn recreate_dir(path: &Path) -> PyResult<()> {
    if path.exists() {
        fs::remove_dir_all(path)
            .map_err(|e| PyRuntimeError::new_err(format!("rm dir {}: {}", path.display(), e)))?;
    }
    fs::create_dir_all(path)
        .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", path.display(), e)))
}

pub fn swap_dir_atomic(work_dir: &Path, final_dir: &Path) -> PyResult<()> {
    let parent = final_dir
        .parent()
        .ok_or_else(|| PyRuntimeError::new_err(format!("no parent for {}", final_dir.display())))?;
    fs::create_dir_all(parent)
        .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", parent.display(), e)))?;

    let backup = parent.join(format!(
        ".swap_old_{}",
        final_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("dir")
    ));
    if backup.exists() {
        fs::remove_dir_all(&backup).map_err(|e| {
            PyRuntimeError::new_err(format!("rm backup {}: {}", backup.display(), e))
        })?;
    }

    if final_dir.exists() {
        fs::rename(final_dir, &backup).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "rename {} -> {}: {}",
                final_dir.display(),
                backup.display(),
                e
            ))
        })?;
    }

    if let Err(e) = fs::rename(work_dir, final_dir) {
        if backup.exists() {
            let _ = fs::rename(&backup, final_dir);
        }
        return Err(PyRuntimeError::new_err(format!(
            "rename {} -> {}: {}",
            work_dir.display(),
            final_dir.display(),
            e
        )));
    }

    if backup.exists() {
        fs::remove_dir_all(&backup)
            .map_err(|e| PyRuntimeError::new_err(format!("rm old {}: {}", backup.display(), e)))?;
    }
    Ok(())
}

pub fn ensure_dir(path: &Path) -> PyResult<()> {
    fs::create_dir_all(path)
        .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", path.display(), e)))
}

pub fn safe_user_dir(username: &str) -> String {
    username
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn json_line_result<W: Write>(
    writer: &mut W,
    value: serde_json::Value,
) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, &value).map_err(|e| format!("json write: {}", e))?;
    writer
        .write_all(b"\n")
        .map_err(|e| format!("write newline: {}", e))
}

pub fn write_json_file(path: &Path, value: &serde_json::Value) -> PyResult<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let file = fs::File::create(path)
        .map_err(|e| PyRuntimeError::new_err(format!("create {}: {}", path.display(), e)))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, value)
        .map_err(|e| PyRuntimeError::new_err(format!("json {}: {}", path.display(), e)))?;
    writer
        .write_all(b"\n")
        .map_err(|e| PyRuntimeError::new_err(format!("newline {}: {}", path.display(), e)))?;
    writer
        .flush()
        .map_err(|e| PyRuntimeError::new_err(format!("flush {}: {}", path.display(), e)))
}

pub fn write_json_file_result(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
    }
    let file = fs::File::create(path).map_err(|e| format!("create {}: {}", path.display(), e))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, value)
        .map_err(|e| format!("json {}: {}", path.display(), e))?;
    writer
        .write_all(b"\n")
        .map_err(|e| format!("newline {}: {}", path.display(), e))?;
    writer
        .flush()
        .map_err(|e| format!("flush {}: {}", path.display(), e))
}
