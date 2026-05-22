use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use dashmap::DashMap;
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::db_writer::{
    self, DEFAULT_TOP_K, DirRow, DirUserSizeRow, FileRow, OwnerRow, TreemapInput, UserRow,
    FILE_INSERT_CHUNK,
};
use crate::pipe_events::{
    for_each_dir_agg_in_file, for_each_scan_event_in_file, get_dir_agg_files, get_scan_event_files,
    read_permission_events,
};
use crate::pipe_io::{ensure_dir, recreate_dir};
use crate::pipe_permission::write_permission_issues_db;
use crate::pipe_treemap::{tm_basename, tm_parent, normalize_root};
use crate::pipe_types::FILE_PART_RECORDS;

const ROW_SPILL_THRESHOLD: usize = 200_000;
const TREEMAP_AGG_VERSION: u32 = 1;

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct TreemapAggregates {
    pub(crate) version: u32,
    pub(crate) names: Vec<String>,
    pub(crate) dirs_in_order: Vec<SerializableDirRow>,
    pub(crate) owner_weights: Vec<(i64, i64, i64)>,
    pub(crate) subtree_size: Vec<i64>,
    pub(crate) subtree_files: Vec<i64>,
    pub(crate) direct_dir_count: Vec<i64>,
    pub(crate) direct_file_count: Vec<i64>,
    pub(crate) total_size_by_dir: Vec<i64>,
    pub(crate) uid_to_username: HashMap<i64, String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct SerializableDirRow {
    pub(crate) id: i64,
    pub(crate) parent_id: Option<i64>,
    pub(crate) name_id: i64,
    pub(crate) depth: i64,
}

fn persist_aggregates(path: &Path, agg: &TreemapAggregates) -> PyResult<()> {
    let file = std::fs::File::create(path).map_err(|e| {
        PyRuntimeError::new_err(format!("persist_aggregates create {}: {}", path.display(), e))
    })?;
    let mut encoder = zstd::Encoder::new(file, 3)
        .map_err(|e| PyRuntimeError::new_err(format!("persist_aggregates zstd init: {}", e)))?;
    bincode::serialize_into(&mut encoder, agg)
        .map_err(|e| PyRuntimeError::new_err(format!("persist_aggregates bincode: {}", e)))?;
    encoder
        .finish()
        .map_err(|e| PyRuntimeError::new_err(format!("persist_aggregates zstd finish: {}", e)))?;
    Ok(())
}

fn load_aggregates(path: &Path) -> PyResult<TreemapAggregates> {
    let file = std::fs::File::open(path)
        .map_err(|e| PyRuntimeError::new_err(format!("load_aggregates open {}: {}", path.display(), e)))?;
    let decoder = zstd::Decoder::new(file)
        .map_err(|e| PyRuntimeError::new_err(format!("load_aggregates zstd decoder: {}", e)))?;
    let agg: TreemapAggregates = bincode::deserialize_from(decoder)
        .map_err(|e| PyRuntimeError::new_err(format!("load_aggregates bincode: {}", e)))?;
    if agg.version != TREEMAP_AGG_VERSION {
        return Err(PyRuntimeError::new_err(format!(
            "aggregates version mismatch: file={} expected={}",
            agg.version, TREEMAP_AGG_VERSION
        )));
    }
    Ok(agg)
}


/// Remove old `.detail_users_build_*` / `.tree_map_data_build_*` from previous
/// crashed runs (anything that does not match the active build directory).
pub fn cleanup_stale_build_dirs(parent: &Path, prefix: &str, active_path: &Path) {
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == active_path || !path.is_dir() {
            continue;
        }
        let name_ok = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with(prefix))
            .unwrap_or(false);
        if name_ok {
            let _ = fs::remove_dir_all(path);
        }
    }
}

fn spill_rows_to_disk(
    spool_root: &Path,
    seq: &Arc<AtomicU64>,
    rows_by_user: &mut HashMap<String, Vec<(u64, String, String)>>,
    row_spills: &mut HashMap<String, Vec<PathBuf>>,
) -> u64 {
    if rows_by_user.is_empty() {
        return 0;
    }
    let mut drained = HashMap::new();
    let mut bytes_written = 0u64;
    std::mem::swap(rows_by_user, &mut drained);
    for (username, rows) in drained {
        if rows.is_empty() {
            continue;
        }
        let safe = sanitize_filename(&username);
        let user_spills = row_spills.entry(username).or_default();
        for rows_chunk in rows.chunks(FILE_PART_RECORDS) {
            let id = seq.fetch_add(1, Ordering::Relaxed);
            let path = spool_root.join(format!("{}_{}.rows", safe, id));
            let file = match OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
            {
                Ok(f) => f,
                Err(_) => continue,
            };
            let mut writer = FrameEncoder::new(file);
            let mut write_ok = true;
            for (size, safe_path, ext) in rows_chunk {
                let path_bytes = safe_path.as_bytes();
                let path_len = u32::try_from(path_bytes.len()).unwrap_or(u32::MAX);
                let ext_bytes = ext.as_bytes();
                let ext_len = u16::try_from(ext_bytes.len()).unwrap_or(u16::MAX);
                if writer.write_all(&size.to_le_bytes()).is_err()
                    || writer.write_all(&path_len.to_le_bytes()).is_err()
                    || writer
                        .write_all(&path_bytes[..(path_len as usize).min(path_bytes.len())])
                        .is_err()
                    || writer.write_all(&ext_len.to_le_bytes()).is_err()
                    || writer
                        .write_all(&ext_bytes[..(ext_len as usize).min(ext_bytes.len())])
                        .is_err()
                {
                    write_ok = false;
                    break;
                }
            }
            if write_ok && writer.flush().is_ok() {
                if let Ok(meta) = fs::metadata(&path) {
                    bytes_written += meta.len();
                }
                user_spills.push(path);
            } else {
                let _ = fs::remove_file(path);
            }
        }
    }
    bytes_written
}

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

// Helper: write a single compact spill row (size:u64 LE, dir_id:u32 LE, name_id:u32 LE, ext_id:u16 LE)
fn write_compact_row(
    writer: &mut FrameEncoder<std::fs::File>,
    size: u64,
    dir_id: u32,
    name_id: u32,
    ext_id: u16,
) -> std::io::Result<()> {
    writer.write_all(&size.to_le_bytes())?;
    writer.write_all(&dir_id.to_le_bytes())?;
    writer.write_all(&name_id.to_le_bytes())?;
    writer.write_all(&ext_id.to_le_bytes())?;
    Ok(())
}

// Helper: read compact spill rows back into Vec
fn read_compact_spill(path: &Path) -> PyResult<Vec<(u64, u32, u32, u16)>> {
    let file = std::fs::File::open(path).map_err(|e| PyRuntimeError::new_err(format!(
        "read_compact_spill open {}: {}", path.display(), e
    )))?;
    let mut decoder = FrameDecoder::new(file);
    let mut rows = Vec::new();
    let mut buf_size = [0u8; 8];
    let mut buf_dir = [0u8; 4];
    let mut buf_name = [0u8; 4];
    let mut buf_ext = [0u8; 2];
    let mut row_idx: usize = 0;
    loop {
        match decoder.read_exact(&mut buf_size) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(PyRuntimeError::new_err(format!(
                "read_compact_spill: size at row {} in {}: {}", row_idx, path.display(), e
            ))),
        }
        decoder.read_exact(&mut buf_dir).map_err(|e| PyRuntimeError::new_err(format!(
            "read_compact_spill: dir_id at row {} in {}: {}", row_idx, path.display(), e
        )))?;
        decoder.read_exact(&mut buf_name).map_err(|e| PyRuntimeError::new_err(format!(
            "read_compact_spill: name_id at row {} in {}: {}", row_idx, path.display(), e
        )))?;
        decoder.read_exact(&mut buf_ext).map_err(|e| PyRuntimeError::new_err(format!(
            "read_compact_spill: ext_id at row {} in {}: {}", row_idx, path.display(), e
        )))?;
        rows.push((
            u64::from_le_bytes(buf_size),
            u32::from_le_bytes(buf_dir),
            u32::from_le_bytes(buf_name),
            u16::from_le_bytes(buf_ext),
        ));
        row_idx += 1;
    }
    Ok(rows)
}

// ─── Path tree assembly ───────────────────────────────────────────────

/// Walks the set of directory paths discovered during ingestion + every parent
/// up to `root`. Assigns BFS-ordered `dir_id` (root = 0) and interns every
/// path segment into `name_id`.
struct PathTree {
    /// dir path -> dir_id
    dir_id_of: HashMap<String, i64>,
    /// dir path stored alongside parent_id for downstream metadata building.
    dirs_in_order: Vec<DirRowDraft>,
    /// segment string -> name_id (extended later with file basenames)
    #[allow(dead_code)]
    name_id_of: HashMap<String, i64>,
    /// names[i] is the segment string for name_id == i (extended later).
    names: Vec<String>,
}

struct DirRowDraft {
    id: i64,
    parent_id: Option<i64>,
    name_id: i64,
    depth: i64,
    // NOTE: `path: String` was removed. After PathTree::build, callers only
    // touch (id, parent_id, name_id, depth) — never the resolved path —
    // so carrying it for 15M dirs cost ~1.2 GB without benefit.
    // If a future caller needs the path, derive it via `dir_id_of`
    // (when still alive) or by walking parent_id up name_id segments.
}

impl PathTree {
    fn build(root: &str, dir_paths: &HashSet<String>) -> Self {
        let mut all_paths: HashSet<String> = HashSet::new();
        all_paths.insert(root.to_string());
        for dp in dir_paths {
            let mut cur = dp.clone();
            loop {
                if !all_paths.insert(cur.clone()) {
                    break;
                }
                if cur == root {
                    break;
                }
                let parent = tm_parent(&cur).to_string();
                if parent == cur {
                    break;
                }
                cur = parent;
            }
        }

        // BFS from root: ensures parent_id < id, gives varint locality.
        let mut by_parent: HashMap<String, Vec<String>> = HashMap::new();
        for p in &all_paths {
            if p == root {
                continue;
            }
            let parent = tm_parent(p).to_string();
            by_parent.entry(parent).or_default().push(p.clone());
        }
        for v in by_parent.values_mut() {
            v.sort();
        }

        let mut name_id_of: HashMap<String, i64> = HashMap::new();
        let mut names: Vec<String> = Vec::new();
        let mut intern = |seg: String| -> i64 {
            if let Some(id) = name_id_of.get(&seg) {
                return *id;
            }
            let id = names.len() as i64;
            names.push(seg.clone());
            name_id_of.insert(seg, id);
            id
        };

        let root_name = if root == "/" {
            "/".to_string()
        } else {
            tm_basename(root)
        };
        let root_name_id = intern(root_name);

        let mut dir_id_of: HashMap<String, i64> = HashMap::new();
        let mut dirs_in_order: Vec<DirRowDraft> = Vec::new();

        dir_id_of.insert(root.to_string(), 0);
        dirs_in_order.push(DirRowDraft {
            id: 0,
            parent_id: None,
            name_id: root_name_id,
            depth: 0,
        });

        let mut queue: VecDeque<(String, i64, i64)> = VecDeque::new();
        queue.push_back((root.to_string(), 0, 0));
        let mut next_id: i64 = 1;
        while let Some((parent_path, parent_id, depth)) = queue.pop_front() {
            let Some(children) = by_parent.remove(&parent_path) else { continue };
            for child in children {
                let id = next_id;
                next_id += 1;
                let seg = tm_basename(&child);
                let name_id = intern(seg);
                dir_id_of.insert(child.clone(), id);
                dirs_in_order.push(DirRowDraft {
                    id,
                    parent_id: Some(parent_id),
                    name_id,
                    depth: depth + 1,
                });
                queue.push_back((child, id, depth + 1));
            }
        }

        // BFS done. by_parent's children Vecs were consumed by remove(),
        // but the outer HashMap still holds 15M String keys plus its own
        // overhead (~1.2 GB at 15M-dir scale). Explicit drop frees it
        // immediately rather than waiting for end-of-function.
        drop(by_parent);

        Self {
            dir_id_of,
            dirs_in_order,
            name_id_of,
            names,
        }
    }
}

// ─── Aggregation containers ───────────────────────────────────────────

#[derive(Default)]
struct LocalAgg {
    dir_sizes: HashMap<String, HashMap<u32, i64>>,
    rows_by_user: HashMap<String, Vec<(u64, String, String)>>,
    row_spills: HashMap<String, Vec<PathBuf>>,
    row_count: usize,
    spill_bytes_written: u64,
}

#[derive(Default)]
struct UserTotals {
    files: i64,
    size: i64,
    dirs: i64,
}

// ─── Main entry point ─────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_detail_db_impl(
    py: Python<'_>,
    tmpdir: String,
    uids_map: HashMap<u32, String>,
    team_map: HashMap<String, String>,
    detail_db_path: String,
    treemap_db_path: String,
    treemap_root: String,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    _max_workers: usize,
    build_treemap: bool,
    debug: bool,
) -> PyResult<(u64, Option<PathBuf>)> {
    py.allow_threads(move || -> PyResult<(u64, Option<PathBuf>)> {
        let t_all = Instant::now();
        #[allow(unused_assignments)]
        let mut t_perm_tsv = 0.0f64;
        #[allow(unused_assignments)]
        let mut t_ingest = 0.0f64;
        let t_path_tree = 0.0f64;
        #[allow(unused_assignments)]
        let mut t_treemap_db = 0.0f64;
        #[allow(unused_assignments)]
        let mut t_files_db = 0.0f64;
        #[allow(unused_assignments)]
        let mut t_finalize_detail = 0.0f64;
        #[allow(unused_assignments)]
        let mut t_perm_write = 0.0f64;

        // ─── Setup work dirs ───────────────────────────────────────
        let detail_db_pb = PathBuf::from(&detail_db_path);
        let detail_root = detail_db_pb
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("detail_users"));
        let detail_parent = detail_root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        ensure_dir(&detail_parent)?;
        ensure_dir(&detail_root)?;

        let detail_work_root =
            detail_parent.join(format!(".detail_users_build_{}", std::process::id()));
        cleanup_stale_build_dirs(&detail_parent, ".detail_users_build_", &detail_work_root);
        recreate_dir(&detail_work_root)?;

        let treemap_db_pb = PathBuf::from(&treemap_db_path);
        let treemap_root_dir = treemap_db_pb
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("tree_map_data"));
        let treemap_parent = treemap_root_dir
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        ensure_dir(&treemap_parent)?;
        ensure_dir(&treemap_root_dir)?;

        let treemap_work_dir =
            treemap_parent.join(format!(".tree_map_data_build_{}", std::process::id()));
        cleanup_stale_build_dirs(&treemap_parent, ".tree_map_data_build_", &treemap_work_dir);
        recreate_dir(&treemap_work_dir)?;

        let spool_root = detail_work_root.join(".rows_spill");
        recreate_dir(&spool_root)?;

        // ─── Phase 1 spill files ───────────────────────────────────
        let t0 = Instant::now();
        let mut perm_events = read_permission_events(&tmpdir).unwrap_or_default();
        t_perm_tsv = t0.elapsed().as_secs_f64();

        let dir_agg_paths = get_dir_agg_files(&tmpdir)?;
        let has_dir_agg = !dir_agg_paths.is_empty();
        let bin_paths = get_scan_event_files(&tmpdir)?;
        println!(
            "[Phase 2] Ingesting and mapping {} event streams...",
            bin_paths.len()
        );

        // ─── STAGE 1: Parallel ingest + aggregate ─────────────────
        let t1 = Instant::now();
        let spill_seq = Arc::new(AtomicU64::new(0));

        let merged_agg = bin_paths
            .into_par_iter()
            .fold(LocalAgg::default, |mut agg, path| {
                let _ = for_each_scan_event_in_file(&path, |event| {
                    let username = uids_map
                        .get(&event.uid)
                        .cloned()
                        .unwrap_or_else(|| format!("uid-{}", event.uid));
                    let safe_path = crate::sanitise_path(&event.path);
                    let ext = crate::pipe_types::extension_for_path(&safe_path);
                    agg.rows_by_user
                        .entry(username)
                        .or_default()
                        .push((event.size, safe_path.clone(), ext));
                    agg.row_count += 1;
                    if !has_dir_agg {
                        if let Some(parent) = crate::pipe_types::parent_path(&safe_path) {
                            let user_sizes = agg.dir_sizes.entry(parent).or_default();
                            *user_sizes.entry(event.uid).or_insert(0) += event.size as i64;
                        }
                    }
                });
                if agg.row_count >= ROW_SPILL_THRESHOLD {
                    agg.spill_bytes_written += spill_rows_to_disk(
                        &spool_root,
                        &spill_seq,
                        &mut agg.rows_by_user,
                        &mut agg.row_spills,
                    );
                    agg.row_count = 0;
                }
                agg
            })
            .reduce(LocalAgg::default, |mut a, b| {
                for (user, mut rows) in b.rows_by_user {
                    let row_len = rows.len();
                    a.rows_by_user.entry(user).or_default().append(&mut rows);
                    a.row_count += row_len;
                }
                for (user, mut spills) in b.row_spills {
                    a.row_spills.entry(user).or_default().append(&mut spills);
                }
                for (dir, sizes_b) in b.dir_sizes {
                    let sizes_a = a.dir_sizes.entry(dir).or_default();
                    for (uid, size) in sizes_b {
                        *sizes_a.entry(uid).or_insert(0) += size;
                    }
                }
                a.spill_bytes_written += b.spill_bytes_written;
                if a.row_count >= ROW_SPILL_THRESHOLD {
                    a.spill_bytes_written += spill_rows_to_disk(
                        &spool_root,
                        &spill_seq,
                        &mut a.rows_by_user,
                        &mut a.row_spills,
                    );
                    a.row_count = 0;
                }
                a
            });

        let mut total_spill_written = merged_agg.spill_bytes_written;

        let mut dir_sizes_by_user: HashMap<String, HashMap<u32, i64>> = if has_dir_agg {
            println!(
                "[Phase 2] Loading {} Phase 1 directory aggregate shards...",
                dir_agg_paths.len()
            );
            dir_agg_paths
                .into_par_iter()
                .fold(HashMap::new, |mut dir_sizes: HashMap<String, HashMap<u32, i64>>, path| {
                    let _ = for_each_dir_agg_in_file(&path, |event| {
                        if event.size > 0 {
                            let user_sizes = dir_sizes.entry(event.path.clone()).or_default();
                            *user_sizes.entry(event.uid).or_insert(0) += event.size;
                        }
                    });
                    dir_sizes
                })
                .reduce(HashMap::new, |mut a, b| {
                    for (dir, sizes_b) in b {
                        let sizes_a = a.entry(dir).or_default();
                        for (uid, size) in sizes_b {
                            *sizes_a.entry(uid).or_insert(0) += size;
                        }
                    }
                    a
                })
        } else {
            merged_agg.dir_sizes
        };

        let mut rows_by_user = merged_agg.rows_by_user;
        let mut row_spills = merged_agg.row_spills;
        total_spill_written += spill_rows_to_disk(
            &spool_root,
            &spill_seq,
            &mut rows_by_user,
            &mut row_spills,
        );
        rows_by_user.clear();
        t_ingest = t1.elapsed().as_secs_f64();
        if debug {
            println!("[RSS checkpoint] after stage 1 ingest: {:.1} MB", crate::pipe_types::get_rss_mb());
        }

        // ─── STAGE 2: Path tree + dir aggregate ────────────────────
        let _t2 = Instant::now();
        let root = normalize_root(&treemap_root);

        let dir_paths_set: HashSet<String> =
            dir_sizes_by_user.keys().cloned().collect();
        println!(
            "[Phase 2] Building path tree for {} directories...",
            dir_paths_set.len()
        );
        let mut path_tree = PathTree::build(&root, &dir_paths_set);
        if debug {
            println!("[RSS checkpoint] after path tree built: {:.1} MB", crate::pipe_types::get_rss_mb());
        }
        // Free the path-keyed set now — path_tree owns the canonical layout
        // and lookups go through dir_id_of from here on.
        drop(dir_paths_set);

        // ─── STAGE 2b: re-encode full-path spills -> compact ID spills ─────
        // Replaces (size, path_str, ext_str) rows with (size, dir_id, name_id, ext_id).
        // Eliminates per-row String allocations in the streaming insert pass.
        let t_reencode = Instant::now();

        let file_name_id_of: Arc<DashMap<String, u32>> = Arc::new(DashMap::new());
        let next_name_id = Arc::new(AtomicU32::new(0));
        let ext_id_map: Arc<DashMap<String, u16>> = Arc::new(DashMap::new());
        let next_ext_id_atomic = Arc::new(AtomicU16::new(1));

        // Intern empty ext as id 0.
        ext_id_map.insert(String::new(), 0u16);

        let spill_seq_compact = Arc::new(AtomicU64::new(1_000_000));

        let compact_entries: Vec<(String, Vec<PathBuf>)> = row_spills
            .par_iter()
            .map(|(username, spill_paths)| -> PyResult<(String, Vec<PathBuf>)> {
                let safe = sanitize_filename(username);
                let mut compact_paths = Vec::new();
                for spill_path in spill_paths {
                    // Read full-path spill
                    let file = match std::fs::File::open(spill_path) {
                        Ok(f) => f,
                        Err(_) => continue,
                    };
                    let mut decoder = FrameDecoder::new(file);

                    let id = spill_seq_compact.fetch_add(1, Ordering::Relaxed);
                    let compact_path = spool_root.join(format!("{}_{}.rows.compact", safe, id));
                    let compact_file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&compact_path)
                        .map_err(|e| PyRuntimeError::new_err(format!(
                            "re-encode open compact {}: {}", compact_path.display(), e
                        )))?;
                    let mut compact_writer = FrameEncoder::new(compact_file);

                    let mut size_buf = [0u8; 8];
                    let mut plen_buf = [0u8; 4];
                    let mut elen_buf = [0u8; 2];
                    let mut row_idx_local: usize = 0;
                    loop {
                        match decoder.read_exact(&mut size_buf) {
                            Ok(_) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                            Err(e) => return Err(PyRuntimeError::new_err(format!(
                                "re-encode read size at row {} in {}: {}", row_idx_local, spill_path.display(), e
                            ))),
                        }
                        decoder.read_exact(&mut plen_buf).map_err(|e| PyRuntimeError::new_err(format!(
                            "re-encode read path_len at row {} in {}: {}", row_idx_local, spill_path.display(), e
                        )))?;
                        let plen = u32::from_le_bytes(plen_buf) as usize;
                        let mut path_bytes = vec![0u8; plen];
                        decoder.read_exact(&mut path_bytes).map_err(|e| PyRuntimeError::new_err(format!(
                            "re-encode read path_bytes at row {} in {}: {}", row_idx_local, spill_path.display(), e
                        )))?;
                        decoder.read_exact(&mut elen_buf).map_err(|e| PyRuntimeError::new_err(format!(
                            "re-encode read ext_len at row {} in {}: {}", row_idx_local, spill_path.display(), e
                        )))?;
                        let elen = u16::from_le_bytes(elen_buf) as usize;
                        let mut ext_bytes = vec![0u8; elen];
                        decoder.read_exact(&mut ext_bytes).map_err(|e| PyRuntimeError::new_err(format!(
                            "re-encode read ext_bytes at row {} in {}: {}", row_idx_local, spill_path.display(), e
                        )))?;

                        let size = u64::from_le_bytes(size_buf);
                        let safe_path = match std::str::from_utf8(&path_bytes) {
                            Ok(s) => s.to_string(),
                            Err(_) => String::from_utf8_lossy(&path_bytes).into_owned(),
                        };
                        let ext = match std::str::from_utf8(&ext_bytes) {
                            Ok(s) => s.to_string(),
                            Err(_) => String::from_utf8_lossy(&ext_bytes).into_owned(),
                        };

                        // Resolve dir_id
                        let parent = crate::pipe_types::parent_path(&safe_path)
                            .unwrap_or_else(|| root.clone());
                        let dir_id = path_tree.dir_id_of.get(&parent).copied().unwrap_or(0) as u32;

                        // Intern basename -> name_id
                        let basename = tm_basename(&safe_path);
                        let name_id = if let Some(id) = file_name_id_of.get(&basename) {
                            *id
                        } else {
                            let new_id = next_name_id.fetch_add(1, Ordering::Relaxed);
                            *file_name_id_of.entry(basename).or_insert(new_id)
                        };

                        // Intern ext -> ext_id
                        let ext_id = if let Some(id) = ext_id_map.get(&ext) {
                            *id
                        } else {
                            let new_id = next_ext_id_atomic.fetch_add(1, Ordering::Relaxed);
                            *ext_id_map.entry(ext).or_insert(new_id)
                        };

                        write_compact_row(&mut compact_writer, size, dir_id, name_id, ext_id)
                            .map_err(|e| PyRuntimeError::new_err(format!(
                                "re-encode write row {} to {}: {}", row_idx_local, compact_path.display(), e
                            )))?;
                        row_idx_local += 1;
                    }

                    compact_writer.flush().map_err(|e| PyRuntimeError::new_err(format!(
                        "re-encode flush {}: {}", compact_path.display(), e
                    )))?;

                    compact_paths.push(compact_path);
                    // Delete original full-path spill
                    let _ = fs::remove_file(spill_path);
                }
                Ok((username.clone(), compact_paths))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let compact_row_spills: HashMap<String, Vec<PathBuf>> = compact_entries.into_iter().collect();
        drop(row_spills); // free original spill map

        let total_names = next_name_id.load(Ordering::Relaxed) as usize;
        let mut file_names: Vec<String> = vec![String::new(); total_names];
        for entry in file_name_id_of.iter() {
            let idx = *entry.value() as usize;
            if idx < file_names.len() {
                file_names[idx] = entry.key().clone();
            }
        }
        // Drop large DashMaps/Arcs now that we've converted them to owned
        // collections (file_names, ext_id_of_lookup). This frees the
        // basename/ext lookup tables from memory before the heavy streaming
        // and insert phases that follow.
        drop(file_name_id_of);
        drop(next_name_id);

        let mut ext_id_of_lookup: HashMap<String, u16> = HashMap::new();
        for entry in ext_id_map.iter() {
            ext_id_of_lookup.insert(entry.key().clone(), *entry.value());
        }
        // Drop ext map & its atomic counter as they're no longer needed.
        drop(ext_id_map);
        let next_ext_id = next_ext_id_atomic.load(Ordering::Relaxed);
        drop(next_ext_id_atomic);

        if debug {
            println!("[Phase 2] Re-encoded spills in {:.2}s ({} users, parallel)",
                t_reencode.elapsed().as_secs_f64(), compact_row_spills.len());
            println!("[RSS checkpoint] after re-encode: {:.1} MB", crate::pipe_types::get_rss_mb());
        }

        // username -> uid (POSIX preferred; for unknown users keep negative slot.)
        let mut username_to_uid: HashMap<String, i64> = HashMap::new();
        let mut uid_to_username: HashMap<i64, String> = HashMap::new();
        for (uid, name) in &uids_map {
            let id = *uid as i64;
            username_to_uid.insert(name.clone(), id);
            uid_to_username.insert(id, name.clone());
        }
        // Reserve synthetic uid for users referenced only via "uid-N" placeholder.
        let mut next_synthetic_uid: i64 = 1_000_000_000;
        let mut intern_user = |username: &str,
                               username_to_uid: &mut HashMap<String, i64>,
                               uid_to_username: &mut HashMap<i64, String>|
         -> i64 {
            if let Some(uid) = username_to_uid.get(username) {
                return *uid;
            }
            let synth = next_synthetic_uid;
            next_synthetic_uid += 1;
            username_to_uid.insert(username.to_string(), synth);
            uid_to_username.insert(synth, username.to_string());
            synth
        };

        // Per-dir aggregates: total_size + per-owner weight breakdown.
        // owner_weights stays as a flat Vec<(dir_id, uid, size)> to keep
        // memory bounded at large scales.
        //
        // dir_ids are dense 0..N (BFS-assigned), so per-dir scalar maps go
        // into Vec instead of HashMap — saves the 24-32 B/entry HashMap
        // overhead. Roughly 1.4 GB -> 480 MB at 75M scale.
        //
        // owner_weights replaces the previous HashMap<i64, HashMap<i64, i64>>:
        // - Outer HashMap of 4.25M dirs alone cost ~250 MB just in alloc
        //   metadata, before any data.
        // - Vec<(i64,i64,i64)> = 24 B/entry, vs ~120 B/entry for the nested
        //   form once we account for both HashMap headers per dir.
        // - All downstream use sites (max-owner-per-dir, collect-all-uids)
        //   are linear scans that don't
        //   need the random-access semantics the HashMap provided.
        let n_dirs = path_tree.dirs_in_order.len();
        let mut total_size_by_dir: Vec<i64> = vec![0; n_dirs];
        let mut owner_weights: Vec<(i64, i64, i64)> = Vec::new();
        let mut user_dir_count: HashMap<i64, i64> = HashMap::new();
        let mut user_total_size_from_dirs: HashMap<i64, i64> = HashMap::new();

        // Drain dir_sizes_by_user into dir_id-keyed form. The String keys are
        // dropped here (~7 MB demo / ~1.2 GB worst case) — owner aggregates
        // and downstream lookups go through dir_id only.
        let dir_sizes_drained: Vec<(i64, HashMap<u32, i64>)> = dir_sizes_by_user
            .drain()
            .filter_map(|(dir_path, uid_sizes)| {
                path_tree.dir_id_of.get(&dir_path).map(|id| (*id, uid_sizes))
            })
            .collect();
        drop(dir_sizes_by_user);

        // dir_id_of is no longer needed after re-encode and dir aggregate remap.
        path_tree.dir_id_of = HashMap::new();

        for (dir_id, uid_sizes) in &dir_sizes_drained {
            let mut total: i64 = 0;
            for (raw_uid, size) in uid_sizes {
                if *size <= 0 {
                    continue;
                }
                let username = uids_map
                    .get(raw_uid)
                    .cloned()
                    .unwrap_or_else(|| format!("uid-{}", raw_uid));
                let uid = intern_user(&username, &mut username_to_uid, &mut uid_to_username);
                total += *size;
                owner_weights.push((*dir_id, uid, *size));
                *user_dir_count.entry(uid).or_insert(0) += 1;
                *user_total_size_from_dirs.entry(uid).or_insert(0) += *size;
            }
            if (*dir_id as usize) < total_size_by_dir.len() {
                total_size_by_dir[*dir_id as usize] = total;
            }
        }
        drop(dir_sizes_drained);

        // Sort by (dir_id, uid) so downstream linear scans see contiguous
        // per-dir runs. dedup_by merges any (dir_id, uid) duplicates that
        // result from intern_user collapsing distinct raw uids onto the same
        // intern uid (e.g. when uids_map maps two raw IDs to the same name).
        owner_weights.sort_unstable_by_key(|&(d, u, _)| (d, u));
        owner_weights.dedup_by(|next, acc| {
            if next.0 == acc.0 && next.1 == acc.1 {
                acc.2 += next.2;
                true
            } else {
                false
            }
        });
        owner_weights.shrink_to_fit();

        // Aggregate subtree sizes bottom-up so dirs.total_size includes children.
        // BFS reverse order (deepest first) is enough.
        // dir_ids are dense 0..N — Vec wins over HashMap (24-byte/entry
        // overhead). At 15M dirs, 4 × Vec<i64> = 480 MB vs HashMap's 1.4 GB.
        let mut dirs_by_depth: Vec<&DirRowDraft> = path_tree.dirs_in_order.iter().collect();
        dirs_by_depth.sort_by(|a, b| b.depth.cmp(&a.depth));

        let mut subtree_size: Vec<i64> = vec![0; n_dirs];
        let mut subtree_files: Vec<i64> = vec![0; n_dirs];
        let mut direct_dir_count: Vec<i64> = vec![0; n_dirs];
        let mut direct_file_count: Vec<i64> = vec![0; n_dirs];

        for d in &path_tree.dirs_in_order {
            if let Some(parent) = d.parent_id {
                if (parent as usize) < direct_dir_count.len() {
                    direct_dir_count[parent as usize] += 1;
                }
            }
        }

        // Initial subtree size = direct size from total_size_by_dir.
        for d in &path_tree.dirs_in_order {
            let idx = d.id as usize;
            if idx < subtree_size.len() && idx < total_size_by_dir.len() {
                subtree_size[idx] = total_size_by_dir[idx];
            }
        }

        for d in dirs_by_depth {
            let idx = d.id as usize;
            if idx >= subtree_size.len() {
                continue;
            }
            let size = subtree_size[idx];
            let files = subtree_files[idx];
            if let Some(parent) = d.parent_id {
                let pidx = parent as usize;
                if pidx < subtree_size.len() {
                    subtree_size[pidx] += size;
                    subtree_files[pidx] += files;
                }
            }
        }

        // ─── STAGE 3a/3b combined — single pass over spill files ──
        // Optimization: previously we did Pass 0 (intern names + ext, aggregate
        // per-user/per-dir totals) and Pass 1 (insert files). That re-read the
        // entire spill payload twice. Now we do everything in ONE pass:
        //
        //   • Open detail.db, set up DDL.
        //   • Stream each spill row -> intern basename into path_tree.names,
        //     intern ext, aggregate user_dir_size/totals, push
        //     FileRow into a chunk buffer (flushed every FILE_INSERT_CHUNK
        //     rows), maintain top-K heap per user.
        //   • Once spill stream is done, build treemap.db (path_tree.names is
        //     now complete with both dir segments and file basenames).
        //   • Insert top_files, dict tables, finalize.
        //
        // detail.db.files references tm.names.id by integer — the FK isn't
        // enforced and consumers ATTACH treemap.db at query time, so writing
        // treemap.db AFTER detail.db.files is fine: both DBs are atomic-renamed
        // at the very end of build_pipeline_dbs_impl.
        let t_pipeline = Instant::now();

        let mut detail_handle = db_writer::detail_open(&detail_db_pb, &detail_work_root, debug)?;

        // Note: file_name_id_of, file_names, ext_id_of_lookup and next_ext_id were
        // populated during the re-encode pass above so the streaming loop can
        // operate purely on integer IDs without allocating path Strings per row.

        let mut user_totals: HashMap<i64, UserTotals> = HashMap::new();
        let mut user_dir_size: HashMap<(i64, i64), (i64, i64)> = HashMap::new();
        let mut total_docs: i64 = 0;
        let mut total_size: i64 = 0;
        let mut total_spill_read: u64 = 0;

        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut next_file_id: i64 = 1;
        let mut chunk: Vec<FileRow> = Vec::with_capacity(FILE_INSERT_CHUNK);

        let mut sorted_users: Vec<String> = compact_row_spills.keys().cloned().collect();
        sorted_users.sort();

        // Pre-size top_heaps with the exact user count so we don't pay the
        // 7 grow-and-rehash cycles HashMap does from default capacity.
        let mut top_heaps: HashMap<i64, BinaryHeap<Reverse<(i64, i64)>>> =
            HashMap::with_capacity(sorted_users.len());

        let user_spill_paths: HashMap<String, Vec<PathBuf>> = compact_row_spills
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let total_users = sorted_users.len();
        if debug {
            println!("[RSS checkpoint] before streaming insert: {:.1} MB", crate::pipe_types::get_rss_mb());
        }
        println!(
            "[Phase 2] Building user detail for {} users...",
            total_users
        );
        let progress_step = (total_users / 10).max(1);
        for (idx, username) in sorted_users.iter().enumerate() {
            let uid = intern_user(username, &mut username_to_uid, &mut uid_to_username);
            let totals = user_totals.entry(uid).or_default();
            let Some(spills) = user_spill_paths.get(username) else { continue };
            for spill_path in spills {
                if let Ok(meta) = fs::metadata(spill_path) {
                    total_spill_read += meta.len();
                }
                let rows = read_compact_spill(spill_path)?;
                for (size, dir_id_u32, name_id_u32, ext_id_u16) in rows {
                    let size_i64 = size as i64;
                    let dir_id = dir_id_u32 as i64;
                    let name_id = name_id_u32 as i64;
                    let ext_id = ext_id_u16 as i64;

                    let dus = user_dir_size.entry((uid, dir_id)).or_insert((0, 0));
                    dus.0 += size_i64;
                    dus.1 += 1;

                    if (dir_id as usize) < direct_file_count.len() {
                        direct_file_count[dir_id as usize] += 1;
                    }

                    totals.files += 1;
                    totals.size += size_i64;
                    total_docs += 1;
                    total_size += size_i64;

                    // Maintain top-K heap.
                    let file_id = next_file_id;
                    next_file_id += 1;
                    let h = top_heaps.entry(uid).or_default();
                    if h.len() < DEFAULT_TOP_K {
                        h.push(Reverse((size_i64, file_id)));
                    } else if let Some(&Reverse((min_size, _))) = h.peek() {
                        if size_i64 > min_size {
                            h.pop();
                            h.push(Reverse((size_i64, file_id)));
                        }
                    }

                    chunk.push(FileRow {
                        dir_id,
                        name_id,
                        ext_id,
                        uid,
                        size: size_i64,
                    });
                    if chunk.len() >= FILE_INSERT_CHUNK {
                        db_writer::detail_insert_files_chunk(&mut detail_handle, &chunk)?;
                        chunk.clear();
                    }
                }
                let _ = fs::remove_file(spill_path);
            }
            let done = idx + 1;
            if done == total_users || done % progress_step == 0 {
                let pct = (done as f64) * 100.0 / (total_users.max(1) as f64);
                println!(
                    "[Phase 2]   user detail: {}/{} ({:.0}%)",
                    done, total_users, pct
                );
            }
        }
        if !chunk.is_empty() {
            db_writer::detail_insert_files_chunk(&mut detail_handle, &chunk)?;
            chunk.clear();
        }
        if debug {
            println!("[RSS checkpoint] after streaming insert: {:.1} MB", crate::pipe_types::get_rss_mb());
        }
        // Streaming pass is done — compact spill map can be dropped now.
        drop(compact_row_spills);
        t_files_db = t_pipeline.elapsed().as_secs_f64();

        // path_tree.dir_id_of already cleared earlier after re-encode

        // Recompute subtree_files bottom-up now that direct_file_count is final.
        for d in &path_tree.dirs_in_order {
            let idx = d.id as usize;
            if idx < subtree_files.len() && idx < direct_file_count.len() {
                subtree_files[idx] += direct_file_count[idx];
            }
        }
        // Scope `dirs_by_depth_rev` to drop the temporary Vec<&DirRowDraft>
        // (~120 MB at 15M dirs) immediately after the bottom-up walk
        // finishes — without this it would live to end of function.
        {
            let mut dirs_by_depth_rev: Vec<&DirRowDraft> = path_tree.dirs_in_order.iter().collect();
            dirs_by_depth_rev.sort_by(|a, b| b.depth.cmp(&a.depth));
            for d in &dirs_by_depth_rev {
                let idx = d.id as usize;
                if idx >= subtree_files.len() {
                    continue;
                }
                let f = subtree_files[idx];
                if let Some(parent) = d.parent_id {
                    let pidx = parent as usize;
                    if pidx < subtree_files.len() {
                        subtree_files[pidx] += f;
                    }
                }
            }
        }

        // ─── STAGE 3a: persist treemap aggregates (if build_treemap) ──────────
        // Treemap building is now a separate phase — see build_treemap_db_impl.
        // Aggregates needed by treemap stage are computed here and serialized
        // to disk. Phase 2 (detail) returns; phase 3 (treemap) loads them with
        // a clean Rust heap.
        let dirs_in_order_arc = Arc::new(std::mem::take(&mut path_tree.dirs_in_order));
        let uid_to_username_arc = Arc::new(uid_to_username);

        let t3a = Instant::now();

        // ─── Insert dictionary + aggregate tables into detail.db ───
        // exts: order by id ascending. (names live in tm.names, not detail.db.)
        let mut ext_strings: Vec<String> = vec![String::new(); next_ext_id as usize];
        for (ext, id) in &ext_id_of_lookup {
            if (*id as usize) < ext_strings.len() {
                ext_strings[*id as usize] = ext.clone();
            }
        }
        db_writer::detail_insert_exts(&mut detail_handle, &ext_strings)?;
        drop(ext_strings);
        drop(ext_id_of_lookup);

        // File basenames (detail.db's local names table; tm.names holds dir
        // segments only).
        db_writer::detail_insert_names(&mut detail_handle, &file_names)?;
        drop(file_names);

        // users
        let mut user_rows: Vec<UserRow> = Vec::with_capacity(user_totals.len());
        for (uid, totals) in &user_totals {
            let username = uid_to_username_arc
                .get(uid)
                .cloned()
                .unwrap_or_else(|| format!("uid-{}", uid));
            let team_id = team_map.get(&username).cloned().unwrap_or_default();
            let dirs_count = user_dir_count.get(uid).copied().unwrap_or(totals.dirs);
            user_rows.push(UserRow {
                uid: *uid,
                username,
                team_id,
                total_files: totals.files,
                total_dirs: dirs_count,
                total_size: totals.size,
                permission_issues: 0,
                is_target: 0,
            });
        }
        user_rows.sort_by_key(|u| u.uid);
        db_writer::detail_insert_users(&mut detail_handle, &user_rows)?;
        drop(user_totals);
        drop(user_dir_count);
        drop(user_rows);

        // dir_user_size
        let dus_rows: Vec<DirUserSizeRow> = user_dir_size
            .drain()
            .map(|((uid, dir_id), (size, files))| DirUserSizeRow {
                uid,
                dir_id,
                size,
                files,
            })
            .collect();
        drop(user_dir_size);
        db_writer::detail_insert_dir_user_size(&mut detail_handle, &dus_rows)?;
        drop(dus_rows);

        // top_files
        let mut top_entries: Vec<(i64, Vec<(i64, i64)>)> = Vec::with_capacity(top_heaps.len());
        for (uid, heap) in top_heaps {
            let mut sorted: Vec<(i64, i64)> =
                heap.into_iter().map(|Reverse((sz, id))| (sz, id)).collect();
            sorted.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            top_entries.push((uid, sorted));
        }
        top_entries.sort_by_key(|(uid, _)| *uid);
        db_writer::detail_insert_top_files(&mut detail_handle, &top_entries)?;
        drop(top_entries);

        #[cfg(target_os = "linux")]
        {
            unsafe extern "C" {
                fn malloc_trim(pad: usize) -> i32;
            }
            let rss_before = crate::pipe_types::get_rss_mb();
            if debug {
                println!("[RSS checkpoint] before malloc_trim: {:.1} MB", rss_before);
            }
            let trim_result = unsafe { malloc_trim(0) };
            let rss_after = crate::pipe_types::get_rss_mb();
            if debug {
                println!("[RSS checkpoint] after malloc_trim: {:.1} MB", rss_after);
                println!(
                    "[Phase 2] malloc_trim(0)={} RSS: {:.1} MB -> {:.1} MB (delta: {:+.1} MB)",
                    trim_result, rss_before, rss_after, rss_after - rss_before
                );
            }
        }

        // detail.db meta
        let detail_meta = vec![
            ("scan_root".to_string(), root.clone()),
            ("scan_timestamp".to_string(), timestamp.to_string()),
            ("total_files".to_string(), total_docs.to_string()),
            (
                "total_dirs".to_string(),
                dirs_in_order_arc.len().to_string(),
            ),
            ("total_size".to_string(), total_size.to_string()),
            ("schema_version".to_string(), "1".to_string()),
            (
                "treemap_db".to_string(),
                treemap_db_pb
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("treemap.db")
                    .to_string(),
            ),
        ];
        db_writer::detail_set_meta(&mut detail_handle, &detail_meta)?;

        // ─── STAGE 5: index + finalize detail.db ─────────────────
        let t5 = Instant::now();
        println!("[Phase 2] Finalizing detail.db (index + vacuum)...");
        let files_inserted = db_writer::detail_finalize(detail_handle)?;
        t_finalize_detail = t5.elapsed().as_secs_f64();

        // ─── STAGE 6: persist treemap aggregates if requested ─────────
        let agg_path_out: Option<PathBuf> = if build_treemap {
            let t_persist = Instant::now();
            let dirs_serialized: Vec<SerializableDirRow> = dirs_in_order_arc
                .iter()
                .map(|d| SerializableDirRow {
                    id: d.id,
                    parent_id: d.parent_id,
                    name_id: d.name_id,
                    depth: d.depth,
                })
                .collect();
            let agg = TreemapAggregates {
                version: TREEMAP_AGG_VERSION,
                names: std::mem::take(&mut path_tree.names),
                dirs_in_order: dirs_serialized,
                owner_weights: std::mem::take(&mut owner_weights),
                subtree_size: std::mem::take(&mut subtree_size),
                subtree_files: std::mem::take(&mut subtree_files),
                direct_dir_count: std::mem::take(&mut direct_dir_count),
                direct_file_count: std::mem::take(&mut direct_file_count),
                total_size_by_dir: std::mem::take(&mut total_size_by_dir),
                uid_to_username: Arc::try_unwrap(uid_to_username_arc.clone())
                    .unwrap_or_else(|arc| (*arc).clone()),
            };
            let agg_path = treemap_work_dir.join("aggregates.bin.zst");
            persist_aggregates(&agg_path, &agg)?;
            t_treemap_db = t3a.elapsed().as_secs_f64();
            if debug {
                println!(
                    "[Phase 2] Persisted treemap aggregates ({:.2}s) -> {}",
                    t_persist.elapsed().as_secs_f64(),
                    agg_path.display()
                );
            }
            Some(agg_path)
        } else {
            t_treemap_db = 0.0;
            None
        };

        // ─── Permission issues ─────────────────────────────────────
        let t_perm = Instant::now();
        perm_events.sort_by(|a, b| {
            a.uid
                .cmp(&b.uid)
                .then_with(|| a.path.cmp(&b.path))
                .then_with(|| a.errcode.cmp(&b.errcode))
                .then_with(|| a.kind.cmp(&b.kind))
        });
        let perm_out_dir = detail_root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        // Indexed SQLite is the only artifact: the dashboard reads it via
        // LIMIT/OFFSET + WHERE, and the JSON variant has been removed.
        write_permission_issues_db(&perm_events, &uids_map, &perm_out_dir, &root, timestamp)?;
        t_perm_write = t_perm.elapsed().as_secs_f64();

        // ─── Cleanup work dirs ─────────────────────────────────────
        let _ = fs::remove_dir_all(&spool_root);
        let _ = fs::remove_dir_all(&detail_work_root);
        // Keep treemap_work_dir when aggregates were persisted there —
        // build_treemap_db_impl will clean it up after phase 3 completes.
        if agg_path_out.is_none() {
            let _ = fs::remove_dir_all(&treemap_work_dir);
        }
        cleanup_legacy_artifacts(&detail_root, &treemap_root_dir);

        if debug {
            let rss_mb = crate::pipe_types::get_rss_mb();
            println!(
                "SQLite outputs built in {:.2}s. Total files: {}, perms: {}",
                t_all.elapsed().as_secs_f64(),
                files_inserted,
                perm_events.len()
            );
            println!("[Phase 2 Profile]");
            println!("  Perm TSV read:      {:.4}s", t_perm_tsv);
            println!("  Ingest+aggregate:   {:.4}s", t_ingest);
            println!("  Spill written:      {:.2} MB", total_spill_written as f64 / (1024.0 * 1024.0));
            println!("  Spill reread:       {:.2} MB", total_spill_read as f64 / (1024.0 * 1024.0));
            println!("  Path tree assembly: {:.4}s", t_path_tree);
            println!("  treemap.db build:   {:.4}s", t_treemap_db);
            println!("  detail files build: {:.4}s", t_files_db);
            println!("  detail finalize:    {:.4}s", t_finalize_detail);
            println!("  Perm JSON write:    {:.4}s", t_perm_write);
            println!("  Peak RSS:           {:.1} MB", rss_mb);
        }
        Ok((files_inserted as u64, agg_path_out))
    })
}

pub(crate) fn build_treemap_db_impl(
    aggregates_path: &Path,
    treemap_db_path: &Path,
    treemap_root: &str,
    max_level: usize,
    min_size_bytes: i64,
    timestamp: i64,
    debug: bool,
) -> PyResult<()> {
    let t_start = Instant::now();
    let agg = load_aggregates(aggregates_path)?;
    if debug {
        println!("[Phase 3] Loaded aggregates in {:.2}s ({} dirs)",
            t_start.elapsed().as_secs_f64(), agg.dirs_in_order.len());
    }

    let treemap_parent = treemap_db_path.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let treemap_work_dir = treemap_parent.join(format!(".tree_map_data_build_{}", std::process::id()));
    cleanup_stale_build_dirs(&treemap_parent, ".tree_map_data_build_", &treemap_work_dir);
    recreate_dir(&treemap_work_dir)?;

    let t_owners = Instant::now();
    let mut all_uids: HashSet<i64> = agg.uid_to_username.keys().copied().collect();
    for &(_, uid, _) in &agg.owner_weights {
        all_uids.insert(uid);
    }
    let mut owners: Vec<OwnerRow> = all_uids
        .into_iter()
        .filter_map(|uid| {
            agg.uid_to_username.get(&uid).cloned()
                .map(|username| OwnerRow { uid, username })
        })
        .collect();
    owners.sort_by_key(|o| o.uid);

    let owner_uid_fallback = owners.first().map(|o| o.uid).unwrap_or(0);
    let n_dirs = agg.dirs_in_order.len();
    let vec_at = |v: &Vec<i64>, id: i64| -> i64 {
        let idx = id as usize;
        if idx < v.len() { v[idx] } else { 0 }
    };

    let mut owner_uid_by_dir: Vec<i64> = vec![owner_uid_fallback; n_dirs];
    {
        let mut i = 0;
        while i < agg.owner_weights.len() {
            let dir_id = agg.owner_weights[i].0;
            let mut best_uid = agg.owner_weights[i].1;
            let mut best_size = agg.owner_weights[i].2;
            let mut j = i + 1;
            while j < agg.owner_weights.len() && agg.owner_weights[j].0 == dir_id {
                if agg.owner_weights[j].2 > best_size {
                    best_size = agg.owner_weights[j].2;
                    best_uid = agg.owner_weights[j].1;
                }
                j += 1;
            }
            if (dir_id as usize) < owner_uid_by_dir.len() {
                owner_uid_by_dir[dir_id as usize] = best_uid;
            }
            i = j;
        }
    }
    let t_owners_done = t_owners.elapsed();

    let t_filter = Instant::now();
    let mut included: HashSet<i64> = HashSet::new();
    for d in &agg.dirs_in_order {
        if d.depth as usize > max_level { continue; }
        let size = vec_at(&agg.subtree_size, d.id);
        if d.id != 0 && size < min_size_bytes { continue; }
        included.insert(d.id);
    }
    included.insert(0);
    let t_filter_done = t_filter.elapsed();

    let t_dir_rows = Instant::now();
    let dir_rows: Vec<DirRow> = agg.dirs_in_order.iter()
        .filter(|d| included.contains(&d.id))
        .map(|d| {
            let total = vec_at(&agg.subtree_size, d.id);
            let dir_count = vec_at(&agg.direct_dir_count, d.id);
            let file_count = vec_at(&agg.direct_file_count, d.id);
            let owner_uid = vec_at(&owner_uid_by_dir, d.id);
            let has_files = if file_count > 0 || vec_at(&agg.total_size_by_dir, d.id) > 0 { 1 } else { 0 };
            DirRow {
                id: d.id,
                parent_id: d.parent_id,
                name_id: d.name_id,
                total_size: total,
                file_count,
                dir_count,
                owner_uid,
                has_files,
            }
        })
        .collect();
    let t_dir_rows_done = t_dir_rows.elapsed();

    let treemap_total_size = vec_at(&agg.subtree_size, 0);
    let meta = vec![
        ("scan_root".to_string(), normalize_root(treemap_root).to_string()),
        ("scan_timestamp".to_string(), timestamp.to_string()),
        ("max_level".to_string(), max_level.to_string()),
        ("total_size".to_string(), treemap_total_size.to_string()),
        ("total_dirs".to_string(), agg.dirs_in_order.len().to_string()),
        ("schema_version".to_string(), "1".to_string()),
    ];

    let input = TreemapInput { names: agg.names, owners, dirs: dir_rows, meta };
    println!("[Phase 3] Building treemap.db ({} dirs, max_level {})...", input.dirs.len(), max_level);

    let t_build = Instant::now();
    db_writer::build_treemap_db(treemap_db_path, &treemap_work_dir, input, debug)?;
    let t_build_done = t_build.elapsed();

    if debug {
        eprintln!("[Phase 3] owners={:.2}s filter={:.2}s dir_rows={:.2}s build_db={:.2}s total={:.2}s",
            t_owners_done.as_secs_f64(), t_filter_done.as_secs_f64(),
            t_dir_rows_done.as_secs_f64(), t_build_done.as_secs_f64(),
            t_start.elapsed().as_secs_f64());
    }

    let _ = std::fs::remove_file(aggregates_path);
    let _ = std::fs::remove_dir_all(&treemap_work_dir);
    Ok(())
}

/// Remove legacy NDJSON / shard outputs from previous runs that now have a
/// fresh `data_detail.db` / `treemap.db`. Best-effort.
fn cleanup_legacy_artifacts(detail_dir: &Path, treemap_dir: &Path) {
    let detail_legacy = [
        "data_detail.json",
        "manifest.json",
        "users",
        "api",
    ];
    for name in detail_legacy {
        let p = detail_dir.join(name);
        if p.is_dir() {
            let _ = fs::remove_dir_all(&p);
        } else if p.is_file() {
            let _ = fs::remove_file(&p);
        }
    }

    let treemap_legacy = ["shards", "api", "manifest.json"];
    for name in treemap_legacy {
        let p = treemap_dir.join(name);
        if p.is_dir() {
            let _ = fs::remove_dir_all(&p);
        } else if p.is_file() {
            let _ = fs::remove_file(&p);
        }
    }
}
