use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

#[derive(Deserialize, Default)]
struct JsonItem {
    path: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    used: Option<u64>,
}

#[derive(Deserialize, Default)]
struct ReportDir {
    #[serde(default)]
    dirs: Vec<JsonItem>,
}

#[derive(Deserialize, Default)]
struct ReportFile {
    #[serde(default)]
    files: Vec<JsonItem>,
}

struct ExportEntry {
    kind: &'static str,
    path: String,
    size: u64,
}

fn format_size(mut n: f64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut i = 0;
    while n >= 1024.0 && i < units.len() - 1 {
        n /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n as u64, units[0])
    } else {
        format!("{:.2} {}", n, units[i])
    }
}

#[pyfunction]
fn process(user: String, dir_path: String, file_path: String, out_path: String) -> PyResult<String> {
    let mut entries = Vec::new();

    // 1. Read dir JSON if exists
    if Path::new(&dir_path).exists() {
        match File::open(&dir_path) {
            Ok(f) => match serde_json::from_reader::<_, ReportDir>(BufReader::new(f)) {
                Ok(data) => {
                    for d in data.dirs {
                        let sz = d.used.unwrap_or(0);
                        entries.push(ExportEntry { kind: "dir ", path: d.path, size: sz });
                    }
                }
                Err(e) => eprintln!("  [rust-warn] Failed to parse {}: {}", dir_path, e),
            },
            Err(e) => eprintln!("  [rust-warn] Failed to open {}: {}", dir_path, e),
        }
    }

    // 2. Read file JSON if exists
    if Path::new(&file_path).exists() {
        match File::open(&file_path) {
            Ok(f) => match serde_json::from_reader::<_, ReportFile>(BufReader::new(f)) {
                Ok(data) => {
                    for file in data.files {
                        let sz = file.size.unwrap_or(0);
                        entries.push(ExportEntry { kind: "file", path: file.path, size: sz });
                    }
                }
                Err(e) => eprintln!("  [rust-warn] Failed to parse {}: {}", file_path, e),
            },
            Err(e) => eprintln!("  [rust-warn] Failed to open {}: {}", file_path, e),
        }
    }

    if entries.is_empty() {
        return Ok("".to_string());
    }

    // 3. Sort by size desc
    entries.sort_unstable_by(|a, b| b.size.cmp(&a.size));

    // 4. Write to TXT
    if let Some(parent) = Path::new(&out_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let out_file = File::create(&out_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Cannot create text file {}: {}", out_path, e)))?;
    let mut w = BufWriter::new(out_file);

    writeln!(w, "{:<4}  {:<20}  {:>12}  Path", "Type", "User", "Size")
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    writeln!(w, "{}", "-".repeat(90))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    for e in entries {
        writeln!(w, "{:<4}  {:<20}  {:>12}  {}", e.kind, user, format_size(e.size as f64), e.path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    }

    w.flush().map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

    Ok(out_path)
}

#[pymodule]
fn export_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process, m)?)?;
    Ok(())
}
