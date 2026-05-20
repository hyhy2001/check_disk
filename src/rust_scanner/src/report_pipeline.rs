use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::db_writer::{
    self, DEFAULT_TOP_K, DirOwnerRow, DirRow, DirUserSizeRow, FileRow, OwnerRow, TreemapInput,
    UserRow, FILE_INSERT_CHUNK,
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
            let mut writer = BufWriter::with_capacity(1024 * 1024, file);
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

// ─── Path tree assembly ───────────────────────────────────────────────

/// Walks the set of directory paths discovered during ingestion + every parent
/// up to `root`. Assigns BFS-ordered `dir_id` (root = 0) and interns every
/// path segment into `name_id`.
struct PathTree {
    /// dir path → dir_id
    dir_id_of: HashMap<String, i64>,
    /// dir path stored alongside parent_id for downstream metadata building.
    dirs_in_order: Vec<DirRowDraft>,
    /// segment string → name_id (extended later with file basenames)
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
pub fn build_pipeline_dbs_impl(
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
    max_workers: usize,
    build_treemap: bool,
    debug: bool,
) -> PyResult<u64> {
    py.allow_threads(move || -> PyResult<u64> {
        let t_all = Instant::now();
        let mut t_perm_tsv = 0.0f64;
        let mut t_ingest = 0.0f64;
        let mut t_path_tree = 0.0f64;
        let mut t_treemap_db = 0.0f64;
        let mut t_files_db = 0.0f64;
        let mut t_finalize_detail = 0.0f64;
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

        // ─── STAGE 2: Path tree + dir aggregate ────────────────────
        let t2 = Instant::now();
        let root = normalize_root(&treemap_root);

        let dir_paths_set: HashSet<String> =
            dir_sizes_by_user.keys().cloned().collect();
        println!(
            "[Phase 2] Building path tree for {} directories...",
            dir_paths_set.len()
        );
        let mut path_tree = PathTree::build(&root, &dir_paths_set);
        // Free the path-keyed set now — path_tree owns the canonical layout
        // and lookups go through dir_id_of from here on.
        drop(dir_paths_set);

        // username → uid (POSIX preferred; for unknown users keep negative slot.)
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
        // We do NOT materialize a separate `dir_owner_rows: Vec` — the same
        // (dir_id, uid, size) tuples can be derived on-the-fly from
        // `owner_weights` when emitting treemap.db. Skipping the Vec
        // saves ~1 GB RAM at 75M-file scale.
        //
        // dir_ids are dense 0..N (BFS-assigned), so per-dir scalar maps go
        // into Vec instead of HashMap — saves the 24-32 B/entry HashMap
        // overhead. Roughly 1.4 GB → 480 MB at 75M scale.
        //
        // owner_weights replaces the previous HashMap<i64, HashMap<i64, i64>>:
        // - Outer HashMap of 4.25M dirs alone cost ~250 MB just in alloc
        //   metadata, before any data.
        // - Vec<(i64,i64,i64)> = 24 B/entry, vs ~120 B/entry for the nested
        //   form once we account for both HashMap headers per dir.
        // - All three downstream use sites (max-owner-per-dir,
        //   collect-all-uids, emit-dir_owner) are linear scans that don't
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
        //   • Stream each spill row → intern basename into path_tree.names,
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

        let mut ext_id_of_lookup: HashMap<String, i64> = HashMap::new();
        let mut next_ext_id: i64 = 0;

        // File basenames live in detail.db, separately from tm.names (which
        // holds DIR segments only). Splitting keeps treemap.db lean for the
        // treemap UI consumer.
        let mut file_name_id_of: HashMap<String, i64> = HashMap::new();
        let mut file_names: Vec<String> = Vec::new();

        let mut user_totals: HashMap<i64, UserTotals> = HashMap::new();
        let mut user_dir_size: HashMap<(i64, i64), (i64, i64)> = HashMap::new();
        let mut total_docs: i64 = 0;
        let mut total_size: i64 = 0;
        let mut total_spill_read: u64 = 0;

        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut next_file_id: i64 = 1;
        let mut chunk: Vec<FileRow> = Vec::with_capacity(FILE_INSERT_CHUNK);

        let mut sorted_users: Vec<String> = row_spills.keys().cloned().collect();
        sorted_users.sort();

        // Pre-size top_heaps with the exact user count so we don't pay the
        // 7 grow-and-rehash cycles HashMap does from default capacity.
        let mut top_heaps: HashMap<i64, BinaryHeap<Reverse<(i64, i64)>>> =
            HashMap::with_capacity(sorted_users.len());

        let user_spill_paths: HashMap<String, Vec<PathBuf>> = row_spills
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let total_users = sorted_users.len();
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
                let mut reader = match fs::File::open(spill_path) {
                    Ok(f) => BufReader::with_capacity(8 * 1024 * 1024, f),
                    Err(_) => continue,
                };
                let mut head = [0u8; 12];
                let mut ext_len_buf = [0u8; 2];
                let mut path_bytes: Vec<u8> = Vec::new();
                let mut ext_bytes: Vec<u8> = Vec::new();
                loop {
                    match reader.read_exact(&mut head) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => {
                            return Err(PyRuntimeError::new_err(format!(
                                "read spill head {}: {}",
                                spill_path.display(),
                                e
                            )));
                        }
                    }
                    let size = u64::from_le_bytes(head[0..8].try_into().unwrap_or([0u8; 8]));
                    let path_len =
                        u32::from_le_bytes(head[8..12].try_into().unwrap_or([0u8; 4])) as usize;
                    path_bytes.clear();
                    path_bytes.resize(path_len, 0);
                    reader.read_exact(&mut path_bytes).map_err(|e| {
                        PyRuntimeError::new_err(format!(
                            "read spill path {}: {}",
                            spill_path.display(),
                            e
                        ))
                    })?;
                    reader.read_exact(&mut ext_len_buf).map_err(|e| {
                        PyRuntimeError::new_err(format!(
                            "read spill ext len {}: {}",
                            spill_path.display(),
                            e
                        ))
                    })?;
                    let ext_len = u16::from_le_bytes(ext_len_buf) as usize;
                    ext_bytes.clear();
                    ext_bytes.resize(ext_len, 0);
                    reader.read_exact(&mut ext_bytes).map_err(|e| {
                        PyRuntimeError::new_err(format!(
                            "read spill ext {}: {}",
                            spill_path.display(),
                            e
                        ))
                    })?;
                    let safe_path = String::from_utf8_lossy(&path_bytes).to_string();
                    let ext = String::from_utf8_lossy(&ext_bytes).to_string();
                    let basename = tm_basename(&safe_path);

                    // Intern basename into detail.db's local file_names table.
                    let name_id = match file_name_id_of.get(&basename) {
                        Some(id) => *id,
                        None => {
                            let id = file_names.len() as i64;
                            file_names.push(basename.clone());
                            file_name_id_of.insert(basename, id);
                            id
                        }
                    };

                    // Intern ext.
                    let ext_id = match ext_id_of_lookup.get(&ext) {
                        Some(id) => *id,
                        None => {
                            let id = next_ext_id;
                            next_ext_id += 1;
                            ext_id_of_lookup.insert(ext.clone(), id);
                            id
                        }
                    };

                    let parent = crate::pipe_types::parent_path(&safe_path)
                        .unwrap_or_else(|| root.clone());
                    let dir_id = path_tree.dir_id_of.get(&parent).copied().unwrap_or(0);
                    let dus = user_dir_size.entry((uid, dir_id)).or_insert((0, 0));
                    dus.0 += size as i64;
                    dus.1 += 1;

                    if (dir_id as usize) < direct_file_count.len() {
                        direct_file_count[dir_id as usize] += 1;
                    }

                    totals.files += 1;
                    totals.size += size as i64;
                    total_docs += 1;
                    total_size += size as i64;

                    // Maintain top-K heap.
                    let file_id = next_file_id;
                    next_file_id += 1;
                    let h = top_heaps.entry(uid).or_default();
                    let size_i = size as i64;
                    if h.len() < DEFAULT_TOP_K {
                        h.push(Reverse((size_i, file_id)));
                    } else if let Some(&Reverse((min_size, _))) = h.peek() {
                        if size_i > min_size {
                            h.pop();
                            h.push(Reverse((size_i, file_id)));
                        }
                    }

                    chunk.push(FileRow {
                        dir_id,
                        name_id,
                        ext_id,
                        uid,
                        size: size_i,
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
        t_files_db = t_pipeline.elapsed().as_secs_f64();

        // Streaming pass is done — drop two big lookup tables that are no
        // longer needed. Verified: `file_name_id_of` is only used inside the
        // stream loop (the dedup'd basenames live in `file_names: Vec`,
        // which is consumed later by detail_insert_names). `dir_id_of` is
        // only used in the stream loop to map parent_path → dir_id; the
        // remaining stages key everything by dir_id directly.
        // Saves ~1.5 GB (basename map) + ~1.5 GB (dir_id_of) at 75M-file
        // / 15M-dir scale.
        drop(file_name_id_of);
        path_tree.dir_id_of = HashMap::new();

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

        // ─── STAGE 3a (after streaming): build treemap.db ──────────
        // Spawn the treemap build on a background thread so it runs in
        // parallel with detail.db's remaining dict + aggregate inserts and
        // (more importantly) the detail.db CREATE INDEX + VACUUM finalize.
        // Both DBs are atomically renamed into place at the very end of this
        // function, so concurrent build is safe — neither file references the
        // other at write time.
        let t3a = Instant::now();
        let treemap_thread: Option<std::thread::JoinHandle<PyResult<()>>> = if build_treemap {
            // Precompute owner_uid per dir (= uid with max weight in that
            // dir) in one linear scan over the sorted owner_weights. Avoids
            // re-iterating a HashMap of HashMaps per included dir below.
            // dir_ids are dense 0..n_dirs so a Vec is enough; entries with
            // no weight stay at owner_uid_fallback after the loop.
            let mut all_uids: HashSet<i64> = uid_to_username.keys().copied().collect();
            for &(_, uid, _) in &owner_weights {
                all_uids.insert(uid);
            }
            let mut owners: Vec<OwnerRow> = all_uids
                .into_iter()
                .filter_map(|uid| {
                    uid_to_username
                        .get(&uid)
                        .cloned()
                        .map(|username| OwnerRow { uid, username })
                })
                .collect();
            owners.sort_by_key(|o| o.uid);

            let owner_uid_fallback = owners.first().map(|o| o.uid).unwrap_or(0);
            // Helper: read a Vec slot indexed by dir_id, with bounds check.
            let vec_at = |v: &Vec<i64>, id: i64| -> i64 {
                let idx = id as usize;
                if idx < v.len() { v[idx] } else { 0 }
            };

            // Precompute owner_uid_by_dir from sorted owner_weights in a
            // single linear scan: per (dir_id, uid) run, track max size and
            // emit the winning uid into the dir's slot. Replaces 4M+ HashMap
            // lookups + per-dir argmax scans done in the previous version.
            let mut owner_uid_by_dir: Vec<i64> = vec![owner_uid_fallback; n_dirs];
            {
                let mut i = 0;
                while i < owner_weights.len() {
                    let dir_id = owner_weights[i].0;
                    let mut best_uid = owner_weights[i].1;
                    let mut best_size = owner_weights[i].2;
                    let mut j = i + 1;
                    while j < owner_weights.len() && owner_weights[j].0 == dir_id {
                        if owner_weights[j].2 > best_size {
                            best_size = owner_weights[j].2;
                            best_uid = owner_weights[j].1;
                        }
                        j += 1;
                    }
                    if (dir_id as usize) < owner_uid_by_dir.len() {
                        owner_uid_by_dir[dir_id as usize] = best_uid;
                    }
                    i = j;
                }
            }

            // Filter dirs by max_level + min_size_bytes.
            let mut included: HashSet<i64> = HashSet::new();
            for d in &path_tree.dirs_in_order {
                if d.depth as usize > max_level {
                    continue;
                }
                let size = vec_at(&subtree_size, d.id);
                if d.id != 0 && size < min_size_bytes {
                    continue;
                }
                included.insert(d.id);
            }
            // Always include root.
            included.insert(0);

            let dir_rows: Vec<DirRow> = path_tree
                .dirs_in_order
                .iter()
                .filter(|d| included.contains(&d.id))
                .map(|d| {
                    let total = vec_at(&subtree_size, d.id);
                    let files = vec_at(&subtree_files, d.id);
                    let _ = files;
                    let dir_count = vec_at(&direct_dir_count, d.id);
                    let file_count = vec_at(&direct_file_count, d.id);
                    let owner_uid = vec_at(&owner_uid_by_dir, d.id);
                    let has_files = if file_count > 0 || vec_at(&total_size_by_dir, d.id) > 0 {
                        1
                    } else {
                        0
                    };
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

            // Derive dir_owner rows directly from owner_weights.
            // owner_weights is already sorted by (dir_id, uid) so the output
            // satisfies treemap.db's expected ordering with no extra sort.
            let mut dir_owner_filtered: Vec<DirOwnerRow> =
                Vec::with_capacity(owner_weights.len());
            for &(dir_id, uid, size) in &owner_weights {
                if size <= 0 || !included.contains(&dir_id) {
                    continue;
                }
                dir_owner_filtered.push(DirOwnerRow { dir_id, uid, size });
            }
            // owner_weights is no longer needed after this point — drop it
            // explicitly so we release ~hundreds of MB before the treemap
            // build thread takes over and detail.db finalize starts.
            drop(owner_weights);

            let treemap_total_size = vec_at(&subtree_size, 0);
            let meta = vec![
                ("scan_root".to_string(), root.clone()),
                ("scan_timestamp".to_string(), timestamp.to_string()),
                ("max_level".to_string(), max_level.to_string()),
                ("total_size".to_string(), treemap_total_size.to_string()),
                (
                    "total_dirs".to_string(),
                    path_tree.dirs_in_order.len().to_string(),
                ),
                ("schema_version".to_string(), "1".to_string()),
            ];

            let input = TreemapInput {
                names: std::mem::take(&mut path_tree.names),
                owners,
                dirs: dir_rows,
                dir_owner: dir_owner_filtered,
                meta,
            };
            // path_tree.names was moved into `input` above (no clone). After
            // this point the main thread doesn't read it again, and the
            // background treemap thread owns the only copy.

            let treemap_db_pb_clone = treemap_db_pb.clone();
            let treemap_work_dir_clone = treemap_work_dir.clone();
            println!(
                "[Phase 2] Building treemap ({} dirs, max_level {}) in background...",
                input.dirs.len(),
                max_level
            );
            Some(std::thread::spawn(move || -> PyResult<()> {
                db_writer::build_treemap_db(
                    &treemap_db_pb_clone,
                    &treemap_work_dir_clone,
                    input,
                    debug,
                )
            }))
        } else {
            None
        };

        // ─── Insert dictionary + aggregate tables into detail.db ───
        // exts: order by id ascending. (names live in tm.names, not detail.db.)
        let mut ext_strings: Vec<String> = vec![String::new(); next_ext_id as usize];
        for (ext, id) in &ext_id_of_lookup {
            if (*id as usize) < ext_strings.len() {
                ext_strings[*id as usize] = ext.clone();
            }
        }
        db_writer::detail_insert_exts(&mut detail_handle, &ext_strings)?;

        // File basenames (detail.db's local names table; tm.names holds dir
        // segments only).
        db_writer::detail_insert_names(&mut detail_handle, &file_names)?;

        // users
        let mut user_rows: Vec<UserRow> = Vec::with_capacity(user_totals.len());
        for (uid, totals) in &user_totals {
            let username = uid_to_username
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

        // dir_user_size
        let dus_rows: Vec<DirUserSizeRow> = user_dir_size
            .iter()
            .map(|(&(uid, dir_id), &(size, files))| DirUserSizeRow {
                uid,
                dir_id,
                size,
                files,
            })
            .collect();
        db_writer::detail_insert_dir_user_size(&mut detail_handle, &dus_rows)?;

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

        // detail.db meta
        let detail_meta = vec![
            ("scan_root".to_string(), root.clone()),
            ("scan_timestamp".to_string(), timestamp.to_string()),
            ("total_files".to_string(), total_docs.to_string()),
            (
                "total_dirs".to_string(),
                path_tree.dirs_in_order.len().to_string(),
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

        // ─── STAGE 5: index + finalize detail.db ───────────────────
        let t5 = Instant::now();
        println!("[Phase 2] Finalizing detail.db (index + vacuum)...");
        let files_inserted = db_writer::detail_finalize(detail_handle)?;
        t_finalize_detail = t5.elapsed().as_secs_f64();

        // Join the parallel treemap-build thread spawned at Stage 3a.
        if let Some(handle) = treemap_thread {
            println!("[Phase 2] Waiting for treemap build to finish...");
            match handle.join() {
                Ok(result) => result?,
                Err(_) => {
                    return Err(PyRuntimeError::new_err(
                        "treemap build thread panicked".to_string(),
                    ));
                }
            }
        }
        t_treemap_db = t3a.elapsed().as_secs_f64();

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
        let _ = fs::remove_dir_all(&treemap_work_dir);
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
        Ok(files_inserted as u64)
    })
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
