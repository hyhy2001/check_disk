use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::time::Instant;
use std::fs;
use crate::{glob_module_rust_many, split_path_for_stage, sanitise_path};
use crate::_aggregate_tsv_to_map;
use crate::generate_treemap_sqlite;

fn intern_string(dict_cache: &mut HashMap<String, u32>, dict_id_seq: &mut u32, value: &str) -> u32 {
    if let Some(&id) = dict_cache.get(value) {
        id
    } else {
        let id = *dict_id_seq;
        *dict_id_seq += 1;
        dict_cache.insert(value.to_string(), id);
        id
    }
}

#[pyfunction(signature = (tmpdir, uids_map, team_map, unified_db_path, treemap_json, treemap_db, treemap_root, max_level, min_size_bytes, timestamp, max_workers, debug=false))]
#[allow(clippy::too_many_arguments)]
pub fn build_unified_dbs(
    py: Python<'_>,
    tmpdir: String,
    uids_map: HashMap<u32, String>,
    team_map: HashMap<String, String>, // username -> team
    unified_db_path: String,
    treemap_json: String,
    treemap_db: String,
    treemap_root: String,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    max_workers: usize,
    debug: bool,
) -> PyResult<u64> {
    let t_all = Instant::now();
    let _effective_workers = max_workers.max(1);
    let uids: Vec<u32> = uids_map.keys().copied().collect();

    // 1. Dirs TSV: Treemap & Dir Meta
    let pattern = format!("{}/dirs_t*.tsv", tmpdir);
    let mut dirs_tsv: Vec<String> = glob::glob(&pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("glob: {}", e)))?
        .filter_map(|entry| entry.ok())
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    dirs_tsv.sort();

    let dir_sizes = _aggregate_tsv_to_map(&dirs_tsv, &uids_map)?;

    let mut dir_owner_map: HashMap<String, String> = HashMap::with_capacity(dir_sizes.len());
    let mut user_meta: HashMap<String, (i64, i64, i64)> = HashMap::new(); // user -> (dirs, files, used)

    for (dpath, user_sizes) in &dir_sizes {
        let mut d_max_size = 0;
        let mut d_max_user = String::new();
        for (owner, &s) in user_sizes {
            if s > d_max_size {
                d_max_size = s;
                d_max_user = owner.clone();
            }
            let meta = user_meta.entry(owner.clone()).or_insert((0, 0, 0));
            meta.0 += 1;
            meta.2 += s;
        }
        if !d_max_user.is_empty() {
            dir_owner_map.insert(dpath.clone(), d_max_user);
        }
    }

    // 2. Create Unified DB Schema
    if let Some(parent) = std::path::Path::new(&unified_db_path).parent() {
        fs::create_dir_all(parent)
            .map_err(|e| PyRuntimeError::new_err(format!("mkdir {}", e)))?;
    }
    if std::path::Path::new(&unified_db_path).exists() {
        fs::remove_file(&unified_db_path)
            .map_err(|e| PyRuntimeError::new_err(format!("rm {}", e)))?;
    }

    let mut conn = Connection::open(&unified_db_path)
        .map_err(|e| PyRuntimeError::new_err(format!("open {}", e)))?;

    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = OFF;
         PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA page_size = 8192;
         PRAGMA cache_size = -100000;
         PRAGMA temp_store = MEMORY;

         CREATE TABLE users (
             user_id INTEGER PRIMARY KEY,
             username TEXT NOT NULL UNIQUE,
             team_id TEXT,
             total_files INTEGER DEFAULT 0,
             total_dirs INTEGER DEFAULT 0,
             total_used INTEGER DEFAULT 0,
             scan_date INTEGER
         );

         CREATE TABLE dirs_dict (
             dir_id INTEGER PRIMARY KEY,
             path TEXT UNIQUE NOT NULL
         );

         CREATE TABLE basename_dict (
             basename_id INTEGER PRIMARY KEY,
             basename TEXT UNIQUE NOT NULL
         );

         CREATE TABLE ext_dict (
             ext_id INTEGER PRIMARY KEY,
             ext TEXT UNIQUE NOT NULL
         );

         CREATE TABLE dir_detail (
             user_id INTEGER NOT NULL,
             dir_id INTEGER NOT NULL,
             size INTEGER NOT NULL,
             PRIMARY KEY(user_id, dir_id)
         ) WITHOUT ROWID;

         CREATE TABLE file_detail (
             user_id INTEGER NOT NULL,
             dir_id INTEGER NOT NULL,
             basename_id INTEGER NOT NULL,
             ext_id INTEGER NOT NULL,
             size INTEGER NOT NULL,
             PRIMARY KEY(user_id, dir_id, basename_id)
         ) WITHOUT ROWID;
         "
    ).map_err(|e| PyRuntimeError::new_err(format!("sqlite schema: {}", e)))?;

    // 3. Domain-specific interning maps
    let mut user_cache: HashMap<String, u32> = HashMap::with_capacity(user_meta.len() + 32);
    let mut dir_cache: HashMap<String, u32> = HashMap::with_capacity(dir_sizes.len() + 64_000);
    let mut basename_cache: HashMap<String, u32> = HashMap::with_capacity(200_000);
    let mut ext_cache: HashMap<String, u32> = HashMap::with_capacity(1024);
    let mut user_id_seq: u32 = 1;
    let mut dir_id_seq: u32 = 1;
    let mut basename_id_seq: u32 = 1;
    let mut ext_id_seq: u32 = 1;

    let ext_empty_id = 0;
    ext_cache.insert(String::new(), ext_empty_id);

    // Load meta users + populate users
    for (username, (d_cnt, f_cnt, d_used)) in user_meta.iter_mut() {
        let u_id = intern_string(&mut user_cache, &mut user_id_seq, username);
        let team = team_map.get(username).cloned().unwrap_or_default();
        conn.execute(
            "INSERT INTO users (user_id, username, team_id, total_dirs, total_files, total_used, scan_date)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![u_id, username, team, *d_cnt, *f_cnt, *d_used, timestamp]
        ).map_err(|e| PyRuntimeError::new_err(format!("insert users: {}", e)))?;
    }

    let tx = conn.transaction().map_err(|e| PyRuntimeError::new_err(format!("tx: {}", e)))?;
    {
        let mut stmt = tx.prepare_cached("INSERT INTO dir_detail(user_id, dir_id, size) VALUES(?1, ?2, ?3)")
            .map_err(|e| PyRuntimeError::new_err(format!("prepare dir_detail: {}", e)))?;
        for (dpath, user_sizes) in &dir_sizes {
            let d_id = intern_string(&mut dir_cache, &mut dir_id_seq, dpath);
            for (uname, &sz) in user_sizes {
                if sz <= 0 { continue; }
                let u_id = intern_string(&mut user_cache, &mut user_id_seq, uname);
                stmt.execute(params![u_id, d_id, sz])
                    .map_err(|e| PyRuntimeError::new_err(format!("insert dir_detail: {}", e)))?;
            }
        }
    }
    tx.commit().map_err(|e| PyRuntimeError::new_err(format!("commit dir: {}", e)))?;

    generate_treemap_sqlite(
        py,
        treemap_root,
        dir_sizes,
        dir_owner_map,
        treemap_json,
        treemap_db,
        max_level,
        min_size_bytes,
    )?;

    // 4. Process files TSV and insert into file_detail
    let chunk_files = glob_module_rust_many(&tmpdir, &uids)?;
    let mut total_files_processed = 0;

    let tx = conn.transaction().map_err(|e| PyRuntimeError::new_err(format!("tx: {}", e)))?;
    {
        let mut stmt = tx.prepare_cached("INSERT INTO file_detail(user_id, dir_id, basename_id, ext_id, size) VALUES(?1, ?2, ?3, ?4, ?5)")
            .map_err(|e| PyRuntimeError::new_err(format!("prepare file_detail: {}", e)))?;
        for path in chunk_files {
            let f = fs::File::open(&path).map_err(|e| PyRuntimeError::new_err(format!("open {}", e)))?;
            let mut reader = crate::MergeReaderState::new(f);
            let filename = std::path::Path::new(&path)
                .file_name()
                .ok_or_else(|| PyRuntimeError::new_err(format!("invalid chunk path: {}", path)))?
                .to_string_lossy()
                .to_string();
            let uid = extract_uid_from_filename(&filename);
            let uname = uids_map.get(&uid).cloned().unwrap_or_else(|| format!("uid-{}", uid));
            let u_id = intern_string(&mut user_cache, &mut user_id_seq, &uname);
            let mut last_dir = String::new();
            let mut last_dir_id = 0_u32;

            while let Some((size, raw_path)) = reader.next_entry() {
                let safe = sanitise_path(&raw_path);
                let (dir_str, basename, xt) = split_path_for_stage(&safe);
                let d_id = if dir_str == last_dir {
                    last_dir_id
                } else {
                    let id = intern_string(&mut dir_cache, &mut dir_id_seq, dir_str);
                    last_dir.clear();
                    last_dir.push_str(dir_str);
                    last_dir_id = id;
                    id
                };
                let b_id = intern_string(&mut basename_cache, &mut basename_id_seq, basename);
                let x_id = if xt.is_empty() {
                    ext_empty_id
                } else {
                    intern_string(&mut ext_cache, &mut ext_id_seq, &xt)
                };
                stmt.execute(params![u_id, d_id, b_id, x_id, size as i64])
                    .map_err(|e| PyRuntimeError::new_err(format!("insert file_detail: {}", e)))?;
                total_files_processed += 1;
            }
        }
    }
    tx.commit().map_err(|e| PyRuntimeError::new_err(format!("commit files: {}", e)))?;

    let tx = conn.transaction().map_err(|e| PyRuntimeError::new_err(format!("tx: {}", e)))?;
    {
        let mut stmt = tx.prepare_cached("INSERT INTO dirs_dict(dir_id, path) VALUES(?1, ?2)")
            .map_err(|e| PyRuntimeError::new_err(format!("prepare dirs_dict: {}", e)))?;
        for (path, &id) in &dir_cache {
            stmt.execute(params![id, path])
                .map_err(|e| PyRuntimeError::new_err(format!("insert dirs_dict: {}", e)))?;
        }
    }
    {
        let mut stmt = tx.prepare_cached("INSERT INTO basename_dict(basename_id, basename) VALUES(?1, ?2)")
            .map_err(|e| PyRuntimeError::new_err(format!("prepare basename_dict: {}", e)))?;
        for (basename, &id) in &basename_cache {
            stmt.execute(params![id, basename])
                .map_err(|e| PyRuntimeError::new_err(format!("insert basename_dict: {}", e)))?;
        }
    }
    {
        let mut stmt = tx.prepare_cached("INSERT INTO ext_dict(ext_id, ext) VALUES(?1, ?2)")
            .map_err(|e| PyRuntimeError::new_err(format!("prepare ext_dict: {}", e)))?;
        for (ext, &id) in &ext_cache {
            stmt.execute(params![id, ext])
                .map_err(|e| PyRuntimeError::new_err(format!("insert ext_dict: {}", e)))?;
        }
    }
    tx.commit().map_err(|e| PyRuntimeError::new_err(format!("commit dict: {}", e)))?;

    // 6. Build Indexes
    conn.execute_batch(
        "CREATE INDEX idx_dir_detail_size ON dir_detail(user_id, size DESC);
         CREATE INDEX idx_file_detail_size ON file_detail(user_id, size DESC);
         CREATE INDEX idx_file_detail_ext ON file_detail(user_id, ext_id, size DESC);

         CREATE VIEW user_meta AS
         SELECT user_id, username, team_id, total_files, total_dirs, total_used, scan_date
         FROM users;

         CREATE VIEW meta AS SELECT scan_date as date, username as user, total_files as total_items, total_used FROM users;
         CREATE VIEW meta_dirs AS SELECT scan_date as date, username as user, total_dirs, total_used FROM users;

         CREATE VIEW dirs AS
         SELECT users.username as user, dirs_dict.path as path, dir_detail.size as used
         FROM dir_detail
         JOIN dirs_dict ON dir_detail.dir_id = dirs_dict.dir_id
         JOIN users ON dir_detail.user_id = users.user_id;

         CREATE VIEW files AS
         SELECT users.username as user,
                dirs_dict.path || '/' || basename_dict.basename as path,
                file_detail.size,
                ext_dict.ext as xt
         FROM file_detail
         JOIN dirs_dict ON file_detail.dir_id = dirs_dict.dir_id
         JOIN basename_dict ON file_detail.basename_id = basename_dict.basename_id
         JOIN ext_dict ON file_detail.ext_id = ext_dict.ext_id
         JOIN users ON file_detail.user_id = users.user_id;
         "
    ).map_err(|e| PyRuntimeError::new_err(format!("index: {}", e)))?;

    conn.execute(
        "UPDATE users SET total_files = (SELECT count(*) FROM file_detail WHERE user_id = users.user_id)",
        []
    ).ok();

    if debug {
        println!("Unified DB built in {:.2}s. Total files: {}", t_all.elapsed().as_secs_f64(), total_files_processed);
    }
    Ok(total_files_processed as u64)
}

fn extract_uid_from_filename(filename: &str) -> u32 {
    // Current chunks: uid_{uid}_t{thread}_c{chunk}.tsv
    if let Some(rest) = filename.strip_prefix("uid_") {
        if let Some(pivot) = rest.find("_t") {
            return rest[..pivot].parse().unwrap_or(0);
        }
    }

    // Older chunks: files_t{thread}_u{uid}.tsv
    if let Some(u_idx) = filename.rfind("_u") {
        if let Some(dot_idx) = filename[u_idx..].find('.') {
            let uid_str = &filename[u_idx + 2..u_idx + dot_idx];
            return uid_str.parse().unwrap_or(0);
        }
    }
    0
}
