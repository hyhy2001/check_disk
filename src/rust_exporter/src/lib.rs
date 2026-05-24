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

/// Build full path string from a tm.dirs id by walking parent_id upward.
fn build_path(conn: &Connection, dir_id: i64) -> String {
    let mut stmt = match conn.prepare_cached(
        "WITH RECURSIVE walk(id, parent_id, name_id, lvl) AS (\n\
           SELECT id, parent_id, name_id, 0 FROM tm.dirs WHERE id = ?\n\
           UNION ALL\n\
           SELECT d.id, d.parent_id, d.name_id, w.lvl + 1\n\
             FROM tm.dirs d JOIN walk w ON d.id = w.parent_id\n\
         ) SELECT GROUP_CONCAT(name, '/') FROM (\n\
           SELECT n.name AS name FROM walk w\n\
             JOIN tm.names n ON w.name_id = n.id\n\
             ORDER BY w.lvl DESC\n\
         )",
    ) {
        Ok(s) => s,
        Err(_) => return String::new(),
    };
    let row: Option<String> = stmt.query_row(params![dir_id], |r| r.get(0)).ok();
    match row {
        Some(joined) => {
            let mut p = String::with_capacity(joined.len() + 1);
            p.push('/');
            // Strip the leading "/" segment from the root so we don't get "//foo".
            p.push_str(joined.trim_start_matches('/'));
            p
        }
        None => String::new(),
    }
}

/// Pre-load every (dir_id → full path) into memory so per-row path lookup is
/// O(1) instead of running a recursive CTE on each query result. With ~100k
/// Compact dir → full-path resolver.
///
/// Instead of storing every full path string (~80 B/dir × 100k dirs ≈ 8 MB
/// demo / ~1.2 GB at 75M-file scale), this stores the (parent_id, name) edge
/// per dir and reconstructs paths on demand. A small LRU cache of recently
/// reconstructed full paths keeps repeated lookups (very common — file rows
/// share parents) at O(1).
///
/// Memory: ~24 B/dir for the edges (Vec<i64> + interned name index). Roughly
/// 4× cheaper than caching every full path eagerly. The cache is bounded so
/// peak RSS stays predictable regardless of dir count.
struct DirPathMap {
    /// dir_id → (parent_id_or_neg1, name_index)
    edges: HashMap<i64, (i64, u32)>,
    /// Distinct path segments (interned).
    names: Vec<String>,
    name_index: HashMap<String, u32>,
    /// Bounded materialization cache.
    cache: std::cell::RefCell<HashMap<i64, String>>,
    cache_cap: usize,
}

impl DirPathMap {
    fn load(conn: &Connection) -> Result<Self, String> {
        let has_detail_dirs = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='dirs'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;

        let query = if has_detail_dirs {
            "SELECT d.id, d.parent_id, n.name \
             FROM dirs d JOIN names n ON d.name_id = n.id \
             ORDER BY d.id"
        } else {
            "SELECT d.id, d.parent_id, n.name \
             FROM tm.dirs d JOIN tm.names n ON d.name_id = n.id \
             ORDER BY d.id"
        };

        let mut stmt = conn
            .prepare(query)
            .map_err(|e| format!("prepare dir path map: {}", e))?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| format!("query dir path map: {}", e))?;

        let mut edges: HashMap<i64, (i64, u32)> = HashMap::new();
        let mut names: Vec<String> = Vec::new();
        let mut name_index: HashMap<String, u32> = HashMap::new();
        for row in rows {
            let (id, parent_id, name) = row.map_err(|e| format!("row: {}", e))?;
            let nidx = match name_index.get(&name) {
                Some(i) => *i,
                None => {
                    let i = names.len() as u32;
                    names.push(name.clone());
                    name_index.insert(name, i);
                    i
                }
            };
            edges.insert(id, (parent_id.unwrap_or(-1), nidx));
        }
        Ok(Self {
            edges,
            names,
            name_index,
            cache: std::cell::RefCell::new(HashMap::with_capacity(2048)),
            cache_cap: 2048,
        })
    }

    fn name_of(&self, idx: u32) -> &str {
        self.names.get(idx as usize).map(|s| s.as_str()).unwrap_or("")
    }

    /// Resolve dir_id → full path. Walks parent chain at most once per
    /// distinct dir_id (cached afterwards).
    fn get(&self, dir_id: i64) -> String {
        if let Some(s) = self.cache.borrow().get(&dir_id) {
            return s.clone();
        }
        // Walk up to root, collect segment indices.
        let mut chain: Vec<u32> = Vec::new();
        let mut cur = dir_id;
        loop {
            match self.edges.get(&cur) {
                Some(&(parent, nidx)) => {
                    chain.push(nidx);
                    if parent < 0 {
                        break;
                    }
                    cur = parent;
                }
                None => break,
            }
        }
        // Build full path from collected segments (root first).
        let total_len: usize = chain.iter().rev().enumerate().map(|(i, &nidx)| {
            let n = self.name_of(nidx);
            // Skip the first '/' for root, add separator for others.
            if i == 0 {
                if n == "/" || n.is_empty() { 1 } else { 1 + n.len() }
            } else {
                1 + n.len()
            }
        }).sum::<usize>().max(1);
        let mut out = String::with_capacity(total_len);
        for (i, &nidx) in chain.iter().rev().enumerate() {
            let n = self.name_of(nidx);
            if i == 0 {
                if n == "/" || n.is_empty() {
                    out.push('/');
                } else {
                    out.push('/');
                    out.push_str(n);
                }
            } else {
                if !out.ends_with('/') {
                    out.push('/');
                }
                out.push_str(n);
            }
        }
        // Bounded cache: drop oldest when full.
        let mut cache = self.cache.borrow_mut();
        if cache.len() >= self.cache_cap {
            // Cheap eviction: clear half. Avoids LRU bookkeeping cost — these
            // entries are also cheaply reconstructible.
            let drop_n = cache.len() / 2;
            let keys: Vec<i64> = cache.keys().take(drop_n).copied().collect();
            for k in keys {
                cache.remove(&k);
            }
        }
        cache.insert(dir_id, out.clone());
        out
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
                "SELECT dir_id, size FROM dir_user_size \
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
                 FROM files f JOIN names n ON f.name_id = n.id \
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
