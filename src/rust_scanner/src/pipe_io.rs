use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn recreate_dir(path: &Path) -> PyResult<()> {
    if path.exists() {
        fs::remove_dir_all(path).map_err(|e| PyRuntimeError::new_err(format!("rm dir {}: {}", path.display(), e)))?;
    }
    fs::create_dir_all(path).map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", path.display(), e)))
}

pub fn ensure_dir(path: &Path) -> PyResult<()> {
    fs::create_dir_all(path).map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", path.display(), e)))
}

pub fn safe_user_dir(username: &str) -> String {
    username
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

pub fn json_line_result(writer: &mut BufWriter<fs::File>, value: serde_json::Value) -> Result<(), String> {
    serde_json::to_writer(&mut *writer, &value).map_err(|e| format!("json write: {}", e))?;
    writer.write_all(b"\n").map_err(|e| format!("write newline: {}", e))
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
    writer.write_all(b"\n").map_err(|e| PyRuntimeError::new_err(format!("newline {}: {}", path.display(), e)))?;
    writer.flush().map_err(|e| PyRuntimeError::new_err(format!("flush {}: {}", path.display(), e)))
}

pub fn write_json_file_result(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
    }
    let file = fs::File::create(path).map_err(|e| format!("create {}: {}", path.display(), e))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, value).map_err(|e| format!("json {}: {}", path.display(), e))?;
    writer.write_all(b"\n").map_err(|e| format!("newline {}: {}", path.display(), e))?;
    writer.flush().map_err(|e| format!("flush {}: {}", path.display(), e))
}
