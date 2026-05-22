// SQLite builders for the Phase 2 reports.
//
// Two output databases are produced per scan:
//   * `treemap.db`     — directory tree (adjacency list + per-dir aggregates)
//   * `data_detail.db` — per-user file/dir breakdown
//
// Schemas: STRICT tables, INTEGER PRIMARY KEY rowid aliases, lookup tables
// for path segments and extensions, partial / covering indexes.
// See `/root/.claude/plans/immutable-purring-adleman.md` for the full design.
//
// `treemap.db` is built via `build_treemap_db()` from a single in-memory
// `TreemapInput`.
//
// `data_detail.db` is built incrementally:
//   1. `detail_open()`            — DDL only
//   2. `detail_insert_files_chunk()` — repeated while streaming spill files
//   3. `detail_insert_*` helpers  — for dictionaries / aggregates
//   4. `detail_finalize()`         — CREATE INDEX + ANALYZE + VACUUM INTO + atomic rename

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rusqlite::{params, params_from_iter, Connection, ToSql};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

// ─── Constants / magic ────────────────────────────────────────────────

pub const TREEMAP_APP_ID: i32 = 0xC0DD15C0u32 as i32;
pub const DETAIL_APP_ID: i32 = 0xC0DD15D1u32 as i32;
pub const SCHEMA_VERSION: i32 = 1;
pub const PAGE_SIZE: i32 = 16384;

pub const FILE_INSERT_CHUNK: usize = 200_000;
pub const DEFAULT_TOP_K: usize = 1000;

/// Rows packed into one `INSERT … VALUES (…),(…),…` statement.
/// SQLite default binding cap is 32766; 100 rows × 9 cols = 900 binds is safe.
const PACK_ROWS: usize = 100;

// ─── DDL ──────────────────────────────────────────────────────────────

const TREEMAP_DDL: &str = "
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);

-- DIR segment dictionary. Lives in treemap.db only — referenced by tm.dirs
-- for path reconstruction. File basenames have their own table in detail.db
-- (so consumers wanting paths ATTACH treemap.db; detail-only consumers stay
-- self-contained).
CREATE TABLE names (
  id   INTEGER PRIMARY KEY,
  name TEXT    NOT NULL
);

CREATE TABLE owners (
  uid      INTEGER PRIMARY KEY,
  username TEXT    NOT NULL
);

CREATE TABLE dirs (
  id          INTEGER PRIMARY KEY,
  parent_id   INTEGER,
  name_id     INTEGER NOT NULL,
  total_size  INTEGER NOT NULL,
  file_count  INTEGER NOT NULL,
  dir_count   INTEGER NOT NULL,
  owner_uid   INTEGER NOT NULL,
  has_files   INTEGER NOT NULL
);
";

const TREEMAP_INDEX_DDL: &str = "
-- (parent_id, total_size DESC) covers both `WHERE parent_id=?` (leftmost
-- prefix lookup) and the UI sort. A separate (parent_id) index is redundant.
CREATE INDEX ix_dirs_parent_size ON dirs(parent_id, total_size DESC);
";

const DETAIL_DDL: &str = "
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE users (
  uid               INTEGER PRIMARY KEY,
  username          TEXT    NOT NULL,
  team_id           TEXT,
  total_files       INTEGER NOT NULL,
  total_dirs        INTEGER NOT NULL,
  total_size        INTEGER NOT NULL,
  permission_issues INTEGER NOT NULL DEFAULT 0,
  is_target         INTEGER NOT NULL DEFAULT 0
);

-- Extension dictionary. Same dedupe-via-Rust rationale as `names`.
CREATE TABLE exts (
  id  INTEGER PRIMARY KEY,
  ext TEXT    NOT NULL
);

-- File basename dictionary. Lives in detail.db (not tm.names) because the
-- treemap UI only needs DIR segment names — file basenames here only matter
-- to detail-page consumers. Splitting keeps treemap.db ~50% smaller and
-- semantically tighter.
CREATE TABLE names (
  id   INTEGER PRIMARY KEY,
  name TEXT    NOT NULL
);

-- File rows. `dir_id` references tm.dirs.id (cross-database via ATTACH).
-- `name_id` references THIS database's `names` table (file basenames).
CREATE TABLE files (
  id      INTEGER PRIMARY KEY,
  dir_id  INTEGER NOT NULL,
  name_id INTEGER NOT NULL,
  ext_id  INTEGER NOT NULL,
  uid     INTEGER NOT NULL,
  size    INTEGER NOT NULL
);

CREATE TABLE top_files (
  uid     INTEGER NOT NULL,
  rank    INTEGER NOT NULL,
  file_id INTEGER NOT NULL,
  size    INTEGER NOT NULL,
  PRIMARY KEY (uid, rank)
);

CREATE TABLE dir_user_size (
  uid    INTEGER NOT NULL,
  dir_id INTEGER NOT NULL,
  size   INTEGER NOT NULL,
  files  INTEGER NOT NULL,
  PRIMARY KEY (uid, dir_id)
);
";

const DETAIL_INDEX_DDL: &str = "
-- Full (uid, size DESC) index. Replaces the previous partial-on-1MB version
-- because the dashboard issues `ORDER BY size DESC` for arbitrary pages, and
-- the partial only covered ~5% of rows — leaving the rest to a temp B-tree
-- sort over every row of the user (~700k for `root` at demo scale → 1+s
-- per page). Full index streams the result already-sorted: any page is
-- ~ms regardless of OFFSET, at the cost of ~10 MB demo / ~1.5 GB at 75M.
CREATE INDEX ix_files_uid_size       ON files(uid, size DESC);
-- Cover (uid, ext_id) lookups so ext-only filter — and ext + size combined —
-- hits an index instead of scanning files. Critical at 75M-row scale: a full
-- scan would be ~25s; this index makes ext filter ~ms regardless of size.
CREATE INDEX ix_files_uid_ext_size   ON files(uid, ext_id, size DESC);
-- Cover keyword search via LIKE-on-tm.names + name_id JOIN. The (name_id, uid)
-- prefix is enough; the size column was dropped because keyword matches
-- typically yield only hundreds–thousands of rows and the temp-B-tree sort on
-- that small set takes microseconds.
CREATE INDEX ix_files_name_uid       ON files(name_id, uid);
CREATE INDEX ix_dus_uid_size         ON dir_user_size(uid, size DESC);
";

// ─── PRAGMA / lifecycle helpers ───────────────────────────────────────

fn apply_build_pragmas(conn: &Connection) -> rusqlite::Result<()> {
    // page_size MUST be set before any table is created.
    conn.pragma_update(None, "page_size", PAGE_SIZE)?;
    conn.pragma_update(None, "journal_mode", "OFF")?;
    conn.pragma_update(None, "synchronous", "OFF")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
    conn.pragma_update(None, "cache_size", -1_048_576i64)?;
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    Ok(())
}

fn open_for_build(build_path: &Path) -> PyResult<Connection> {
    if build_path.exists() {
        fs::remove_file(build_path).map_err(|e| {
            PyRuntimeError::new_err(format!("rm old {}: {}", build_path.display(), e))
        })?;
    }
    if let Some(parent) = build_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}: {}", parent.display(), e)))?;
    }
    let conn = Connection::open(build_path)
        .map_err(|e| PyRuntimeError::new_err(format!("open {}: {}", build_path.display(), e)))?;
    apply_build_pragmas(&conn)
        .map_err(|e| PyRuntimeError::new_err(format!("pragma: {}", e)))?;
    Ok(conn)
}

fn stamp_db(conn: &Connection, app_id: i32) -> PyResult<()> {
    conn.pragma_update(None, "application_id", app_id)
        .map_err(|e| PyRuntimeError::new_err(format!("application_id: {}", e)))?;
    conn.pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(|e| PyRuntimeError::new_err(format!("user_version: {}", e)))?;
    Ok(())
}

/// VACUUM INTO is skipped outside the `[MIN, MAX]` range. Inserts happen in
/// PK-ascending order with `journal_mode=OFF`, so freed pages are ~zero and
/// page clustering is already near-optimal — VACUUM mostly buys a marginally
/// smaller file at the cost of read-all + write-all I/O.
///
/// - Below MIN: page-clustering gain doesn't justify the extra IO.
/// - Above MAX: rewrite cost dominates (e.g. ~3min on 15 GB DB observed in
///   production). Build files are already well-clustered; ship them as-is.
const VACUUM_SIZE_MIN_BYTES: u64 = 100 * 1024 * 1024;
const VACUUM_SIZE_MAX_BYTES: u64 = 1024 * 1024 * 1024;

fn finalize_db(conn: Connection, build_path: &Path, final_path: &Path) -> PyResult<()> {
    let tmp_path = final_path.with_extension("tmp.db");
    if tmp_path.exists() {
        let _ = fs::remove_file(&tmp_path);
    }

    // Run ANALYZE / VACUUM INTO inside a closure so we can clean up the
    // partial tmp.db on any error path before propagating the failure.
    // VACUUM INTO is the only step here that can leave a half-written file
    // around (e.g. when /tmp or the destination disk runs out of space mid-
    // write); a leaked tmp.db would otherwise sit on disk until the next run.
    let result: PyResult<()> = (|| {
        let t_analyze = Instant::now();
        conn.execute_batch("PRAGMA optimize;")
            .map_err(|e| PyRuntimeError::new_err(format!("optimize: {}", e)))?;
        let analyze_secs = t_analyze.elapsed().as_secs_f64();

        let build_size = fs::metadata(build_path).map(|m| m.len()).unwrap_or(0);
        let skip_vacuum =
            build_size < VACUUM_SIZE_MIN_BYTES || build_size > VACUUM_SIZE_MAX_BYTES;

        let mut vacuum_secs = 0.0f64;
        if !skip_vacuum {
            let tmp_str = tmp_path.to_string_lossy().replace('\'', "''");
            let t_vacuum = Instant::now();
            conn.execute(&format!("VACUUM INTO '{}'", tmp_str), [])
                .map_err(|e| PyRuntimeError::new_err(format!("vacuum into: {}", e)))?;
            vacuum_secs = t_vacuum.elapsed().as_secs_f64();
        }
        drop(conn);

        let final_name = final_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("<db>");
        let size_mb = build_size as f64 / (1024.0 * 1024.0);
        if skip_vacuum {
            let reason = if build_size < VACUUM_SIZE_MIN_BYTES {
                "small"
            } else {
                "huge"
            };
            println!(
                "[finalize] {}: analyze {:.2}s, vacuum skipped ({}, {:.1} MB)",
                final_name, analyze_secs, reason, size_mb
            );
        } else {
            println!(
                "[finalize] {}: analyze {:.2}s, vacuum {:.2}s ({:.1} MB)",
                final_name, analyze_secs, vacuum_secs, size_mb
            );
        }

        if final_path.exists() {
            fs::remove_file(final_path).map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "rm old final {}: {}",
                    final_path.display(),
                    e
                ))
            })?;
        }

        let source = if skip_vacuum { build_path } else { &tmp_path };
        fs::rename(source, final_path).map_err(|e| {
            PyRuntimeError::new_err(format!(
                "rename {} -> {}: {}",
                source.display(),
                final_path.display(),
                e
            ))
        })?;

        if !skip_vacuum {
            let _ = fs::remove_file(build_path);
        }
        Ok(())
    })();

    if result.is_err() && tmp_path.exists() {
        let _ = fs::remove_file(&tmp_path);
    }
    result
}

fn insert_meta(conn: &mut Connection, meta: &[(String, String)]) -> PyResult<()> {
    if meta.is_empty() {
        return Ok(());
    }
    let tx = conn
        .transaction()
        .map_err(|e| PyRuntimeError::new_err(format!("tx meta: {}", e)))?;
    {
        let mut stmt = tx
            .prepare("INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)")
            .map_err(|e| PyRuntimeError::new_err(format!("prep meta: {}", e)))?;
        for (k, v) in meta {
            stmt.execute(params![k, v])
                .map_err(|e| PyRuntimeError::new_err(format!("ins meta: {}", e)))?;
        }
    }
    tx.commit()
        .map_err(|e| PyRuntimeError::new_err(format!("commit meta: {}", e)))?;
    Ok(())
}

/// Build "(?,?,...,?)" placeholder group for `cols` columns.
fn placeholder_group(cols: usize) -> String {
    let mut s = String::with_capacity(2 + cols * 2);
    s.push('(');
    for i in 0..cols {
        if i > 0 {
            s.push(',');
        }
        s.push('?');
    }
    s.push(')');
    s
}

/// Multi-row `INSERT INTO <table>(<cols>) VALUES (...),(...),...` packing.
///
/// Splits `rows` into chunks of [`PACK_ROWS`] and emits one statement per
/// chunk inside a single transaction. This collapses N parameter-binding +
/// plan-execution round trips into N/PACK_ROWS, giving 2-5× speedup for
/// the hot insert paths (files, names, dirs, dir_user_size).
fn packed_insert<F>(
    conn: &mut Connection,
    table: &str,
    columns: &str,
    cols: usize,
    rows_len: usize,
    mut bind_row: F,
    label: &str,
) -> PyResult<()>
where
    F: FnMut(usize) -> Vec<Box<dyn ToSql + Send>>,
{
    if rows_len == 0 {
        return Ok(());
    }
    let tx = conn
        .transaction()
        .map_err(|e| PyRuntimeError::new_err(format!("tx {}: {}", label, e)))?;
    {
        let group = placeholder_group(cols);

        let full_chunks = rows_len / PACK_ROWS;
        let tail = rows_len % PACK_ROWS;

        // Cache the prepared statement for full-size chunks.
        let mut full_sql = String::new();
        let mut full_stmt_opt = None;
        if full_chunks > 0 {
            full_sql.push_str("INSERT INTO ");
            full_sql.push_str(table);
            full_sql.push('(');
            full_sql.push_str(columns);
            full_sql.push_str(") VALUES ");
            for i in 0..PACK_ROWS {
                if i > 0 {
                    full_sql.push(',');
                }
                full_sql.push_str(&group);
            }
            let stmt = tx
                .prepare(&full_sql)
                .map_err(|e| PyRuntimeError::new_err(format!("prep {} full: {}", label, e)))?;
            full_stmt_opt = Some(stmt);
        }

        let mut row_idx = 0usize;
        if let Some(mut stmt) = full_stmt_opt {
            for _ in 0..full_chunks {
                let mut binds: Vec<Box<dyn ToSql + Send>> =
                    Vec::with_capacity(PACK_ROWS * cols);
                for _ in 0..PACK_ROWS {
                    binds.append(&mut bind_row(row_idx));
                    row_idx += 1;
                }
                stmt.execute(params_from_iter(binds.iter().map(|b| b.as_ref())))
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("ins {} (packed): {}", label, e))
                    })?;
            }
        }

        if tail > 0 {
            let mut sql = String::new();
            sql.push_str("INSERT INTO ");
            sql.push_str(table);
            sql.push('(');
            sql.push_str(columns);
            sql.push_str(") VALUES ");
            for i in 0..tail {
                if i > 0 {
                    sql.push(',');
                }
                sql.push_str(&group);
            }
            let mut stmt = tx
                .prepare(&sql)
                .map_err(|e| PyRuntimeError::new_err(format!("prep {} tail: {}", label, e)))?;
            let mut binds: Vec<Box<dyn ToSql + Send>> = Vec::with_capacity(tail * cols);
            for _ in 0..tail {
                binds.append(&mut bind_row(row_idx));
                row_idx += 1;
            }
            stmt.execute(params_from_iter(binds.iter().map(|b| b.as_ref())))
                .map_err(|e| PyRuntimeError::new_err(format!("ins {} (tail): {}", label, e)))?;
        }
    }
    tx.commit()
        .map_err(|e| PyRuntimeError::new_err(format!("commit {}: {}", label, e)))?;
    Ok(())
}

// ─── Treemap input + builder ──────────────────────────────────────────

#[derive(Clone)]
pub struct DirRow {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub name_id: i64,
    pub total_size: i64,
    pub file_count: i64,
    pub dir_count: i64,
    pub owner_uid: i64,
    pub has_files: i64,
}

pub struct OwnerRow {
    pub uid: i64,
    pub username: String,
}

pub struct TreemapInput {
    pub names: Vec<String>,
    pub owners: Vec<OwnerRow>,
    pub dirs: Vec<DirRow>,
    pub meta: Vec<(String, String)>,
}

pub fn build_treemap_db(
    final_path: &Path,
    work_dir: &Path,
    input: TreemapInput,
    debug: bool,
) -> PyResult<()> {
    let build_path: PathBuf = work_dir.join("treemap.build.db");
    let mut conn = open_for_build(&build_path)?;

    conn.execute_batch(TREEMAP_DDL)
        .map_err(|e| PyRuntimeError::new_err(format!("treemap ddl: {}", e)))?;

    // names: 2 cols × N rows
    {
        let names = &input.names;
        packed_insert(
            &mut conn,
            "names",
            "id, name",
            2,
            names.len(),
            |i| {
                let id = i as i64;
                let name = names[i].clone();
                vec![Box::new(id), Box::new(name)]
            },
            "names",
        )?;
    }

    // owners: 2 cols × N rows
    {
        let owners = &input.owners;
        packed_insert(
            &mut conn,
            "owners",
            "uid, username",
            2,
            owners.len(),
            |i| {
                let uid = owners[i].uid;
                let username = owners[i].username.clone();
                vec![Box::new(uid), Box::new(username)]
            },
            "owners",
        )?;
    }

    // dirs: 8 cols × N rows
    {
        let dirs = &input.dirs;
        packed_insert(
            &mut conn,
            "dirs",
            "id, parent_id, name_id, total_size, file_count, dir_count, owner_uid, has_files",
            8,
            dirs.len(),
            |i| {
                let d = &dirs[i];
                vec![
                    Box::new(d.id),
                    Box::new(d.parent_id),
                    Box::new(d.name_id),
                    Box::new(d.total_size),
                    Box::new(d.file_count),
                    Box::new(d.dir_count),
                    Box::new(d.owner_uid),
                    Box::new(d.has_files),
                ]
            },
            "dirs",
        )?;
    }

    insert_meta(&mut conn, &input.meta)?;

    conn.execute_batch(TREEMAP_INDEX_DDL)
        .map_err(|e| PyRuntimeError::new_err(format!("treemap idx: {}", e)))?;

    stamp_db(&conn, TREEMAP_APP_ID)?;

    if debug {
        println!(
            "[Phase 2] treemap.db built (names={}, dirs={}, owners={})",
            input.names.len(),
            input.dirs.len(),
            input.owners.len()
        );
    }

    finalize_db(conn, &build_path, final_path)?;
    Ok(())
}

// ─── Detail incremental builder ───────────────────────────────────────

pub struct UserRow {
    pub uid: i64,
    pub username: String,
    pub team_id: String,
    pub total_files: i64,
    pub total_dirs: i64,
    pub total_size: i64,
    pub permission_issues: i64,
    pub is_target: i64,
}

pub struct DirUserSizeRow {
    pub uid: i64,
    pub dir_id: i64,
    pub size: i64,
    pub files: i64,
}

pub struct FileRow {
    pub dir_id: i64,
    pub name_id: i64,
    pub ext_id: i64,
    pub uid: i64,
    pub size: i64,
}

pub struct DetailBuildHandle {
    conn: Connection,
    build_path: PathBuf,
    final_path: PathBuf,
    debug: bool,
    files_inserted: i64,
}

pub fn detail_open(
    final_path: &Path,
    work_dir: &Path,
    debug: bool,
) -> PyResult<DetailBuildHandle> {
    let build_path: PathBuf = work_dir.join("data_detail.build.db");
    let conn = open_for_build(&build_path)?;

    conn.execute_batch(DETAIL_DDL)
        .map_err(|e| PyRuntimeError::new_err(format!("detail ddl: {}", e)))?;

    Ok(DetailBuildHandle {
        conn,
        build_path,
        final_path: final_path.to_path_buf(),
        debug,
        files_inserted: 0,
    })
}

pub fn detail_insert_users(handle: &mut DetailBuildHandle, users: &[UserRow]) -> PyResult<()> {
    if users.is_empty() {
        return Ok(());
    }
    let tx = handle
        .conn
        .transaction()
        .map_err(|e| PyRuntimeError::new_err(format!("tx users: {}", e)))?;
    {
        let mut stmt = tx
            .prepare(
                "INSERT INTO users(uid, username, team_id, total_files, total_dirs, \
                 total_size, permission_issues, is_target) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .map_err(|e| PyRuntimeError::new_err(format!("prep users: {}", e)))?;
        for u in users {
            stmt.execute(params![
                u.uid,
                u.username,
                u.team_id,
                u.total_files,
                u.total_dirs,
                u.total_size,
                u.permission_issues,
                u.is_target,
            ])
            .map_err(|e| PyRuntimeError::new_err(format!("ins user: {}", e)))?;
        }
    }
    tx.commit()
        .map_err(|e| PyRuntimeError::new_err(format!("commit users: {}", e)))?;
    Ok(())
}

pub fn detail_insert_exts(handle: &mut DetailBuildHandle, exts: &[String]) -> PyResult<()> {
    let tx = handle
        .conn
        .transaction()
        .map_err(|e| PyRuntimeError::new_err(format!("tx exts: {}", e)))?;
    {
        let mut stmt = tx
            .prepare("INSERT INTO exts(id, ext) VALUES (?, ?)")
            .map_err(|e| PyRuntimeError::new_err(format!("prep exts: {}", e)))?;
        for (id, ext) in exts.iter().enumerate() {
            stmt.execute(params![id as i64, ext])
                .map_err(|e| PyRuntimeError::new_err(format!("ins ext: {}", e)))?;
        }
    }
    tx.commit()
        .map_err(|e| PyRuntimeError::new_err(format!("commit exts: {}", e)))?;
    Ok(())
}

/// Insert file basenames into detail.db's local `names` table.
/// Multi-row VALUES packing for fast bulk insert at scale.
pub fn detail_insert_names(handle: &mut DetailBuildHandle, names: &[String]) -> PyResult<()> {
    let len = names.len();
    if len == 0 {
        return Ok(());
    }
    packed_insert(
        &mut handle.conn,
        "names",
        "id, name",
        2,
        len,
        |i| {
            let id = i as i64;
            let name = names[i].clone();
            vec![Box::new(id), Box::new(name)]
        },
        "names",
    )
}

pub fn detail_insert_files_chunk(
    handle: &mut DetailBuildHandle,
    rows: &[FileRow],
) -> PyResult<()> {
    let len = rows.len();
    if len == 0 {
        return Ok(());
    }
    // Fast path: files is the hot insert at 48M+ rows, all-i64. The generic
    // `packed_insert` path goes through `Box<dyn ToSql>` per cell, which at
    // 5 cols × 48M rows = 240M heap allocations alone. We instead bind a
    // contiguous `&[i64]` per packed statement so each cell is a stack copy
    // — same 100-row packing strategy, no boxing, no FnMut indirection.
    let tx = handle
        .conn
        .transaction()
        .map_err(|e| PyRuntimeError::new_err(format!("tx files: {}", e)))?;
    {
        const COLS: usize = 5;
        let full_chunks = len / PACK_ROWS;
        let tail = len % PACK_ROWS;

        // Cached prepared statement for full PACK_ROWS chunks.
        let mut full_stmt = if full_chunks > 0 {
            let group = placeholder_group(COLS);
            let mut full_sql = String::with_capacity(48 + group.len() * PACK_ROWS);
            full_sql.push_str(
                "INSERT INTO files(dir_id, name_id, ext_id, uid, size) VALUES ",
            );
            for i in 0..PACK_ROWS {
                if i > 0 {
                    full_sql.push(',');
                }
                full_sql.push_str(&group);
            }
            Some(
                tx.prepare(&full_sql)
                    .map_err(|e| PyRuntimeError::new_err(format!("prep files full: {}", e)))?,
            )
        } else {
            None
        };

        let mut binds: Vec<i64> = Vec::with_capacity(PACK_ROWS * COLS);
        let mut row_idx = 0usize;
        if let Some(ref mut stmt) = full_stmt {
            for _ in 0..full_chunks {
                binds.clear();
                for _ in 0..PACK_ROWS {
                    let r = unsafe { rows.get_unchecked(row_idx) };
                    binds.push(r.dir_id);
                    binds.push(r.name_id);
                    binds.push(r.ext_id);
                    binds.push(r.uid);
                    binds.push(r.size);
                    row_idx += 1;
                }
                stmt.execute(params_from_iter(binds.iter()))
                    .map_err(|e| PyRuntimeError::new_err(format!("ins files (packed): {}", e)))?;
            }
        }

        if tail > 0 {
            let group = placeholder_group(COLS);
            let mut sql = String::with_capacity(48 + group.len() * tail);
            sql.push_str("INSERT INTO files(dir_id, name_id, ext_id, uid, size) VALUES ");
            for i in 0..tail {
                if i > 0 {
                    sql.push(',');
                }
                sql.push_str(&group);
            }
            let mut stmt = tx
                .prepare(&sql)
                .map_err(|e| PyRuntimeError::new_err(format!("prep files tail: {}", e)))?;
            binds.clear();
            for _ in 0..tail {
                let r = unsafe { rows.get_unchecked(row_idx) };
                binds.push(r.dir_id);
                binds.push(r.name_id);
                binds.push(r.ext_id);
                binds.push(r.uid);
                binds.push(r.size);
                row_idx += 1;
            }
            stmt.execute(params_from_iter(binds.iter()))
                .map_err(|e| PyRuntimeError::new_err(format!("ins files (tail): {}", e)))?;
        }
    }
    tx.commit()
        .map_err(|e| PyRuntimeError::new_err(format!("commit files: {}", e)))?;
    handle.files_inserted += len as i64;
    Ok(())
}

pub fn detail_insert_dir_user_size(
    handle: &mut DetailBuildHandle,
    rows: &[DirUserSizeRow],
) -> PyResult<()> {
    packed_insert(
        &mut handle.conn,
        "dir_user_size",
        "uid, dir_id, size, files",
        4,
        rows.len(),
        |i| {
            let r = &rows[i];
            vec![
                Box::new(r.uid),
                Box::new(r.dir_id),
                Box::new(r.size),
                Box::new(r.files),
            ]
        },
        "dir_user_size",
    )
}

/// `entries` = `[(uid, sorted_desc_by_size [(size, file_id), ...])]`. Rank starts at 1.
pub fn detail_insert_top_files(
    handle: &mut DetailBuildHandle,
    entries: &[(i64, Vec<(i64, i64)>)],
) -> PyResult<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let tx = handle
        .conn
        .transaction()
        .map_err(|e| PyRuntimeError::new_err(format!("tx top_files: {}", e)))?;
    {
        let mut stmt = tx
            .prepare("INSERT INTO top_files(uid, rank, file_id, size) VALUES (?, ?, ?, ?)")
            .map_err(|e| PyRuntimeError::new_err(format!("prep top_files: {}", e)))?;
        for (uid, sorted) in entries {
            for (rank, (size, file_id)) in sorted.iter().enumerate() {
                stmt.execute(params![uid, (rank + 1) as i64, file_id, size])
                    .map_err(|e| PyRuntimeError::new_err(format!("ins top_file: {}", e)))?;
            }
        }
    }
    tx.commit()
        .map_err(|e| PyRuntimeError::new_err(format!("commit top_files: {}", e)))?;
    Ok(())
}

pub fn detail_set_meta(handle: &mut DetailBuildHandle, meta: &[(String, String)]) -> PyResult<()> {
    insert_meta(&mut handle.conn, meta)
}

pub fn detail_finalize(handle: DetailBuildHandle) -> PyResult<i64> {
    // Boost cache + mmap for CREATE INDEX phase. Indexes are built by
    // scanning the entire `files` table multiple times — bigger cache
    // means fewer disk re-reads. Restored to default after finalize.
    handle
        .conn
        .execute_batch(
            "PRAGMA cache_size = -4194304;\
             PRAGMA mmap_size = 8589934592;",
        )
        .map_err(|e| PyRuntimeError::new_err(format!("pragma boost: {}", e)))?;

    handle
        .conn
        .execute_batch(DETAIL_INDEX_DDL)
        .map_err(|e| PyRuntimeError::new_err(format!("detail idx: {}", e)))?;

    stamp_db(&handle.conn, DETAIL_APP_ID)?;

    if handle.debug {
        println!(
            "[Phase 2] data_detail.db built (files={})",
            handle.files_inserted
        );
    }

    let DetailBuildHandle {
        conn,
        build_path,
        final_path,
        files_inserted,
        ..
    } = handle;
    finalize_db(conn, &build_path, &final_path)?;
    Ok(files_inserted)
}
