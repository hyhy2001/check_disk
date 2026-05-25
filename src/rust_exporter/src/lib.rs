// export_rust — read per-user disk usage detail from the SQLite databases
// (`data_detail.db` + `treemap.db`) and dump plain-text TSV reports per user.
//
// Replaces the legacy NDJSON/manifest-driven pipeline. The Python wrapper is
// `scripts/export_user_reports.py`.
//
// FFI surface:
//   process(user, detail_db, treemap_db, output_dir, prefix) -> Vec<String>
//   process_jobs([(user, detail_db, treemap_db, output_dir, prefix), ...], workers)
//      -> Vec<String>

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rusqlite::{params, Connection, OpenFlags};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const IO_BUF_SIZE: usize = 8 * 1024 * 1024;

fn format_size(size_bytes: f64) -> String {
    let n = if size_bytes.is_sign_negative() { 0.0 } else { size_bytes };
    const KB: f64 = 1_000.0;
    const MB: f64 = 1_000_000.0;
    const GB: f64 = 1_000_000_000.0;
    const TB: f64 = 1_000_000_000_000.0;

    if n >= TB {
        format!("{:.2} TB", n / TB)
    } else if n >= GB {
        format!("{:.1} GB", n / GB)
    } else if n >= MB {
        format!("{:.0} MB", n / MB)
    } else if n >= KB {
        format!("{:.0} KB", n / KB)
    } else {
        format!("{} B", n as u64)
    }
}

fn open_detail(detail_db: &str, treemap_db: Option<&str>) -> Result<Connection, String> {
    let conn = Connection::open_with_flags(
        detail_db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("open detail.db {}: {}", detail_db, e))?;
    let _ = conn.pragma_update(None, "query_only", 1);
    let _ = conn.pragma_update(None, "mmap_size", 268_435_456i64);
    let _ = conn.pragma_update(None, "cache_size", -65_536i64);
    if let Some(tm) = treemap_db {
        if !tm.is_empty() && Path::new(tm).is_file() {
            let escaped = tm.replace('\'', "''");
            conn.execute_batch(&format!("ATTACH DATABASE 'file:{}?mode=ro' AS tm;", escaped))
                .map_err(|e| format!("attach treemap.db {}: {}", tm, e))?;
        }
    }
    Ok(conn)
}


/// Pre-load every (dir_id → full path) into memory so per-row path lookup is
/// O(1). New schema has dirs.path pre-computed by scanner.
struct DirPathMap {
    paths: HashMap<i64, String>,
}

impl DirPathMap {
    fn load(conn: &Connection) -> Result<Self, String> {
        let mut stmt = conn
            .prepare("SELECT DISTINCT id, path FROM dirs")
            .map_err(|e| format!("prepare dir path map: {}", e))?;
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(|e| format!("query dir path map: {}", e))?;

        let mut paths: HashMap<i64, String> = HashMap::new();
        for row in rows {
            let (id, path) = row.map_err(|e| format!("row: {}", e))?;
            paths.insert(id, path);
        }
        Ok(Self { paths })
    }

    fn get(&self, dir_id: i64) -> String {
        self.paths.get(&dir_id).cloned().unwrap_or_default()
    }
}

fn lookup_user_uid(conn: &Connection, username: &str) -> Option<i64> {
    let mut stmt = conn
        .prepare_cached("SELECT uid FROM users WHERE username = ?")
        .ok()?;
    stmt.query_row(params![username], |r| r.get(0)).ok()
}

fn process_internal(
    user: &str,
    detail_db: &str,
    treemap_db: &str,
    out_path: &str,
    kind: &str, // "dir" or "file"
) -> Result<String, String> {
    if !Path::new(detail_db).is_file() {
        return Ok(String::new());
    }
    let conn = open_detail(detail_db, Some(treemap_db))?;
    let uid = match lookup_user_uid(&conn, user) {
        Some(u) => u,
        None => return Ok(String::new()),
    };
    let dir_paths = DirPathMap::load(&conn)?;

    if let Some(parent) = Path::new(out_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let f = File::create(out_path)
        .map_err(|e| format!("create text file {}: {}", out_path, e))?;
    let mut w = BufWriter::with_capacity(IO_BUF_SIZE, f);

    writeln!(
        w,
        "{:<4}  {:<20}  {:>12}  Path",
        "Type", "User", "Size"
    )
    .map_err(|e| e.to_string())?;
    writeln!(w, "{}", "-".repeat(90)).map_err(|e| e.to_string())?;

    let mut row_count = 0usize;
    if kind == "dir" {
        let mut stmt = conn
            .prepare(
                "SELECT id, size FROM dirs \
                 WHERE uid = ? ORDER BY size DESC",
            )
            .map_err(|e| format!("prepare dir query: {}", e))?;
        let rows = stmt
            .query_map(params![uid], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(|e| format!("query dir: {}", e))?;
        for row in rows {
            let (dir_id, size) = row.map_err(|e| format!("row: {}", e))?;
            let path = dir_paths.get(dir_id);
            writeln!(
                w,
                "{:<4}  {:<20}  {:>12}  {}",
                "dir ",
                user,
                format_size(size as f64),
                path
            )
            .map_err(|e| e.to_string())?;
            row_count += 1;
        }
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT f.dir_id, n.name, f.size \
                 FROM files f JOIN file_names n ON f.name_id = n.id \
                 WHERE f.uid = ? ORDER BY f.size DESC",
            )
            .map_err(|e| format!("prepare file query: {}", e))?;
        let rows = stmt
            .query_map(params![uid], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(|e| format!("query file: {}", e))?;
        for row in rows {
            let (dir_id, basename, size) = row.map_err(|e| format!("row: {}", e))?;
            let parent = dir_paths.get(dir_id);
            let full_path = if parent.is_empty() {
                basename
            } else if parent == "/" {
                let mut s = String::with_capacity(1 + basename.len());
                s.push('/');
                s.push_str(&basename);
                s
            } else {
                let mut s = String::with_capacity(parent.len() + 1 + basename.len());
                s.push_str(&parent);
                s.push('/');
                s.push_str(&basename);
                s
            };
            writeln!(
                w,
                "{:<4}  {:<20}  {:>12}  {}",
                "file",
                user,
                format_size(size as f64),
                full_path
            )
            .map_err(|e| e.to_string())?;
            row_count += 1;
        }
    }

    w.flush().map_err(|e| e.to_string())?;

    if row_count == 0 {
        // Drop empty file so callers can detect "no data" by absence.
        let _ = std::fs::remove_file(out_path);
        return Ok(String::new());
    }
    Ok(out_path.to_string())
}

fn process_user_job(
    user: &str,
    detail_db: &str,
    treemap_db: &str,
    output_dir: &str,
    prefix: &str,
) -> Result<Vec<String>, String> {
    if !Path::new(detail_db).is_file() {
        return Ok(Vec::new());
    }

    let mut base_parts = Vec::new();
    if !prefix.is_empty() {
        base_parts.push(prefix.to_string());
    }
    base_parts.push("usage".to_string());
    let base = base_parts.join("_");

    let safe_user: String = user
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let dir_path: PathBuf = Path::new(output_dir).join(format!("{}_dir_{}.txt", base, safe_user));
    let file_path: PathBuf = Path::new(output_dir).join(format!("{}_file_{}.txt", base, safe_user));

    let mut results = Vec::new();
    let dir_out = process_internal(
        user,
        detail_db,
        treemap_db,
        dir_path.to_string_lossy().as_ref(),
        "dir",
    )?;
    if !dir_out.is_empty() {
        results.push(dir_out);
    }
    let file_out = process_internal(
        user,
        detail_db,
        treemap_db,
        file_path.to_string_lossy().as_ref(),
        "file",
    )?;
    if !file_out.is_empty() {
        results.push(file_out);
    }
    Ok(results)
}

#[pyfunction]
fn process(
    user: String,
    detail_db: String,
    treemap_db: String,
    output_dir: String,
    prefix: String,
) -> PyResult<Vec<String>> {
    process_user_job(&user, &detail_db, &treemap_db, &output_dir, &prefix)
        .map_err(PyRuntimeError::new_err)
}

/// Each job: (user, detail_db, treemap_db, output_dir, prefix)
#[pyfunction(signature = (jobs, workers=4))]
fn process_jobs(
    jobs: Vec<(String, String, String, String, String)>,
    workers: usize,
) -> PyResult<Vec<String>> {
    let pool = ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("build thread pool: {}", e)))?;

    let per_job: Vec<Result<Vec<String>, String>> = pool.install(|| {
        jobs.par_iter()
            .map(|(user, detail_db, treemap_db, output_dir, prefix)| {
                process_user_job(user, detail_db, treemap_db, output_dir, prefix)
            })
            .collect()
    });

    let mut outputs = Vec::new();
    for item in per_job {
        match item {
            Ok(mut files) => outputs.append(&mut files),
            Err(e) => return Err(PyRuntimeError::new_err(e)),
        }
    }
    Ok(outputs)
}

#[pymodule]
fn export_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(process, m)?)?;
    m.add_function(wrap_pyfunction!(process_jobs, m)?)?;
    Ok(())
}
