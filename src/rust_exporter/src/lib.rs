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
use std::sync::Arc;

const IO_BUF_SIZE: usize = 8 * 1024 * 1024;

fn format_size(size_bytes: f64) -> String {
    let n = if size_bytes.is_sign_negative() { 0.0 } else { size_bytes };
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    const GB: f64 = 1024.0 * 1024.0 * 1024.0;
    const TB: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;

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
    fn empty() -> Self {
        Self { paths: HashMap::new() }
    }

    fn load(conn: &Connection) -> Result<Self, String> {
        let scan_root = read_scan_root(conn);
        let root_basename = scan_root.trim_end_matches('/').rsplit('/').next().unwrap_or("").to_string();

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
            let abs_path = if path.starts_with('/') {
                path
            } else if !root_basename.is_empty() && (path == root_basename || path.starts_with(&format!("{}/", root_basename))) {
                let suffix = path.strip_prefix(&root_basename).unwrap_or(&path);
                let suffix = suffix.strip_prefix('/').unwrap_or(suffix);
                if suffix.is_empty() {
                    scan_root.trim_end_matches('/').to_string()
                } else {
                    format!("{}/{}", scan_root.trim_end_matches('/'), suffix)
                }
            } else if !scan_root.is_empty() {
                format!("{}/{}", scan_root.trim_end_matches('/'), path)
            } else {
                path
            };
            paths.insert(id, abs_path);
        }
        Ok(Self { paths })
    }

    fn get(&self, dir_id: i64) -> String {
        self.paths.get(&dir_id).cloned().unwrap_or_default()
    }
}

fn read_scan_root(conn: &Connection) -> String {
    conn.query_row(
        "SELECT value FROM meta WHERE key = 'scan_root'",
        [],
        |r| r.get::<_, String>(0),
    ).unwrap_or_default()
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
    dir_paths: &DirPathMap,
) -> Result<String, String> {
    if !Path::new(detail_db).is_file() {
        return Ok(String::new());
    }
    let conn = open_detail(detail_db, Some(treemap_db))?;
    let uid = match lookup_user_uid(&conn, user) {
        Some(u) => u,
        None => return Ok(String::new()),
    };

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
                "SELECT id, path, size FROM dirs \
                 WHERE uid = ?",
            )
            .map_err(|e| format!("prepare dir query: {}", e))?;
        let rows = stmt
            .query_map(params![uid], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(|e| format!("query dir: {}", e))?;
        let mut entries: Vec<(i64, String, i64)> = Vec::new();
        for row in rows {
            entries.push(row.map_err(|e| format!("row: {}", e))?);
        }

        if entries.is_empty() {
            let _ = std::fs::remove_file(out_path);
            return Ok(String::new());
        }

        let n = entries.len();
        // Precompute path depth once.
        let depth: Vec<usize> = entries.iter().map(|e| e.1.as_bytes().iter().filter(|&&b| b == b'/').count()).collect();

        // Accumulate: use path strings for ancestry (bypasses missing
        // intermediate dirs that have no entry in detail.db).
        let mut acc: HashMap<&str, i64> = HashMap::with_capacity(n);
        for (_, ref path, size) in &entries {
            acc.insert(path.as_str(), *size);
        }

        // Process deepest first via index sort (avoids moving entries).
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_unstable_by(|&a, &b| depth[b].cmp(&depth[a]));
        for &idx in &order {
            let path = &entries[idx].1;
            let child_total = match acc.get(path.as_str()) {
                Some(&v) => v,
                None => continue,
            };
            if child_total <= 0 {
                continue;
            }
            let mut cur: &str = path.as_str();
            loop {
                match cur.rfind('/') {
                    Some(0) => {
                        if cur != "/" {
                            if let Some(pa) = acc.get_mut("/") {
                                *pa += child_total;
                            }
                        }
                        break;
                    }
                    Some(pos) => {
                        let parent = &cur[..pos];
                        if let Some(pa) = acc.get_mut(parent) {
                            *pa += child_total;
                            break;
                        }
                        cur = parent;
                    }
                    None => break,
                }
            }
        }

        // Sort indices by accumulated size (no path cloning).
        order.sort_unstable_by(|&a, &b| {
            let sa = acc.get(entries[a].1.as_str()).copied().unwrap_or(0);
            let sb = acc.get(entries[b].1.as_str()).copied().unwrap_or(0);
            sb.cmp(&sa)
        });

        for &idx in &order {
            let size = acc.get(entries[idx].1.as_str()).copied().unwrap_or(0);
            writeln!(
                w,
                "{:<4}  {:<20}  {:>12}  {}",
                "dir ",
                user,
                format_size(size as f64),
                entries[idx].1
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
    dir_paths: &DirPathMap,
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
        dir_paths,
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
        dir_paths,
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
    let conn = open_detail(&detail_db, if treemap_db.is_empty() { None } else { Some(&treemap_db) })
        .map_err(|e| PyRuntimeError::new_err(e))?;
    let dir_paths = DirPathMap::load(&conn).map_err(|e| PyRuntimeError::new_err(e))?;
    process_user_job(&user, &detail_db, &treemap_db, &output_dir, &prefix, &dir_paths)
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

    let first_detail_db = jobs.first().map(|j| j.1.as_str()).unwrap_or("");
    let first_treemap_db = jobs.first().map(|j| j.2.as_str()).unwrap_or("");
    let dir_paths = if !first_detail_db.is_empty() && Path::new(first_detail_db).is_file() {
        let conn = open_detail(first_detail_db, if first_treemap_db.is_empty() { None } else { Some(first_treemap_db) })
            .map_err(|e| PyRuntimeError::new_err(e))?;
        Arc::new(DirPathMap::load(&conn).map_err(|e| PyRuntimeError::new_err(e))?)
    } else {
        Arc::new(DirPathMap::empty())
    };

    let per_job: Vec<Result<Vec<String>, String>> = pool.install(|| {
        jobs.par_iter()
            .map(|(user, detail_db, treemap_db, output_dir, prefix)| {
                process_user_job(user, detail_db, treemap_db, output_dir, prefix, &dir_paths)
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
