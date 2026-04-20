use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

#[derive(Deserialize, Default)]
struct JsonItem {
    path: Option<String>,
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

fn parse_file_items(file_path: &str, kind: &'static str, entries: &mut Vec<ExportEntry>) {
    if !Path::new(file_path).exists() {
        return;
    }

    if file_path.ends_with(".db") {
        let conn = match Connection::open_with_flags(file_path, OpenFlags::SQLITE_OPEN_READ_ONLY) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  [rust-warn] Failed to open sqlite {}: {}", file_path, e);
                return;
            }
        };

        if kind == "dir " {
            let mut stmt = match conn.prepare("SELECT path, used FROM dirs ORDER BY used DESC") {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  [rust-warn] Failed to query dirs in {}: {}", file_path, e);
                    return;
                }
            };
            let rows = match stmt.query_map([], |row| {
                let path: String = row.get(0)?;
                let size: u64 = row.get(1)?;
                Ok((path, size))
            }) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("  [rust-warn] Failed to iterate dirs in {}: {}", file_path, e);
                    return;
                }
            };
            for r in rows.flatten() {
                entries.push(ExportEntry { kind, path: r.0, size: r.1 });
            }
        } else {
            let mut stmt = match conn.prepare("SELECT path, size FROM files ORDER BY size DESC") {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  [rust-warn] Failed to query files in {}: {}", file_path, e);
                    return;
                }
            };
            let rows = match stmt.query_map([], |row| {
                let path: String = row.get(0)?;
                let size: u64 = row.get(1)?;
                Ok((path, size))
            }) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("  [rust-warn] Failed to iterate files in {}: {}", file_path, e);
                    return;
                }
            };
            for r in rows.flatten() {
                entries.push(ExportEntry { kind, path: r.0, size: r.1 });
            }
        }
        return;
    }

    let f = match File::open(file_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("  [rust-warn] Failed to open {}: {}", file_path, e);
            return;
        }
    };
    
    if file_path.ends_with(".ndjson") {
        for line in BufReader::new(f).lines() {
            if let Ok(l) = line {
                if let Ok(item) = serde_json::from_str::<JsonItem>(&l) {
                    if let Some(path) = item.path {
                        let sz = if kind == "dir " { item.used.unwrap_or(0) } else { item.size.unwrap_or(0) };
                        entries.push(ExportEntry { kind, path, size: sz });
                    }
                }
            }
        }
    } else {
        // legacy JSON
        if kind == "dir " {
            if let Ok(data) = serde_json::from_reader::<_, ReportDir>(BufReader::new(f)) {
                for d in data.dirs {
                    if let Some(path) = d.path {
                        let sz = d.used.unwrap_or(0);
                        entries.push(ExportEntry { kind, path, size: sz });
                    }
                }
            }
        } else {
            if let Ok(data) = serde_json::from_reader::<_, ReportFile>(BufReader::new(f)) {
                for file in data.files {
                    if let Some(path) = file.path {
                        let sz = file.size.unwrap_or(0);
                        entries.push(ExportEntry { kind, path, size: sz });
                    }
                }
            }
        }
    }
}

#[pyfunction]
fn process(user: String, dir_path: String, file_path: String, out_path: String) -> PyResult<String> {
    let mut entries = Vec::new();

    // 1. Read dir JSON/NDJSON/DB if exists
    if !dir_path.is_empty() {
        parse_file_items(&dir_path, "dir ", &mut entries);
    }

    // 2. Read file JSON/NDJSON/DB if exists
    if !file_path.is_empty() {
        parse_file_items(&file_path, "file", &mut entries);
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
