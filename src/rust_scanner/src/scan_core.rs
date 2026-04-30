use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use ignore::{WalkBuilder, WalkState};
use dashmap::DashSet;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::scan_constants::{
    CRITICAL_SKIP_NAMES,
    SCAN_EVENT_FLUSH_BYTES_THRESHOLD,
    SCAN_EVENT_FLUSH_THRESHOLD,
};
use crate::scan_state::{GlobalStats, ThreadLocalState};
use crate::scan_utils::{error_code_from_message, format_num, format_rate, format_size, get_rss_mb};

pub(crate) fn run_scan_core(
    py: Python,
    directory: String,
    skip_dirs: Vec<String>,
    target_uids: Option<Vec<u32>>,
    max_workers: Option<usize>,
    debug: bool,
    engine: &str,
) -> PyResult<PyObject> {
    let _tmpdir = tempfile::Builder::new()
        .prefix("checkdisk_rust_")
        .tempdir()
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    let tmpdir_str = _tmpdir.path().to_string_lossy().to_string();
    let _ = _tmpdir.keep(); // persist tmp dir; Python side cleans up later

    let global_stats = Arc::new(Mutex::new(GlobalStats {
        total_files: 0, total_dirs: 0, total_inodes: 0, total_size: 0,
        uid_sizes: HashMap::new(), uid_files: HashMap::new(),
        permission_issues_count: 0,
    }));
    let prog_files = Arc::new(AtomicU64::new(0));
    let prog_dirs  = Arc::new(AtomicU64::new(0));
    let prog_size  = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let prof_metadata_ns = Arc::new(AtomicU64::new(0));
    let prof_path_ns = Arc::new(AtomicU64::new(0));
    let prof_flush_ns = Arc::new(AtomicU64::new(0));
    let prof_flush_bytes = Arc::new(AtomicU64::new(0));
    let prof_flush_count = Arc::new(AtomicU64::new(0));
    let prof_hardlink_checks = Arc::new(AtomicU64::new(0));
    let prof_visited_dir_checks = Arc::new(AtomicU64::new(0));
    let prof_max_event_buf_records = Arc::new(AtomicU64::new(0));
    let prof_max_event_buf_bytes = Arc::new(AtomicU64::new(0));

    // Determine root device for cross-device check (NFS, snapshots, bind-mounts)
    let root_dev: Option<u64> = fs::metadata(&directory).ok().map(|m| m.dev());

    let g_clone = global_stats.clone();
    let pf_clone = prog_files.clone();
    let pd_clone = prog_dirs.clone();
    let ps_clone = prog_size.clone();
    let d_clone = done.clone();
    let dir_clone = directory.clone();
    let skips = skip_dirs.clone();
    let tmpdir_clone = tmpdir_str.clone();
    let pm_clone = prof_metadata_ns.clone();
    let pp_clone = prof_path_ns.clone();
    let pfns_clone = prof_flush_ns.clone();
    let pfb_clone = prof_flush_bytes.clone();
    let pfc_clone = prof_flush_count.clone();
    let ph_clone = prof_hardlink_checks.clone();
    let pv_clone = prof_visited_dir_checks.clone();
    let pmaxr_clone = prof_max_event_buf_records.clone();
    let pmaxb_clone = prof_max_event_buf_bytes.clone();

    let cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    // Keep a sane default and let CLI max_workers override explicitly.
    let default_threads = (cpus * 2).clamp(4, 32);
    let threads_count = max_workers.unwrap_or(default_threads).max(1);
    let thread_counter = Arc::new(AtomicUsize::new(0));
    let target_uids_shared = Arc::new(
        target_uids.map(|uids| uids.into_iter().collect::<HashSet<u32>>())
    );
    // Shared cross-worker hard-link deduplication — DashSet avoids Mutex bottleneck
    let hardlink_inodes: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    // Shared directory loop/bind-mount deduplication — DashSet (16 shards by default)
    let visited_dirs: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    let hardlink_inodes_profile = hardlink_inodes.clone();
    let visited_dirs_profile = visited_dirs.clone();

    let _walk_thread = thread::spawn(move || {
        WalkBuilder::new(&dir_clone)
            .hidden(false)
            .ignore(false)
            .git_ignore(false)
            .git_exclude(false)
            .git_global(false)
            .threads(threads_count)
            .build_parallel()
            .run(|| {
                let tid = thread_counter.fetch_add(1, Ordering::SeqCst);
                let mut state = ThreadLocalState {
                    t_files: 0, t_dirs: 0, t_inodes: 0, t_size: 0,
                    t_uid_sizes: HashMap::new(),
                    t_uid_files: HashMap::new(),
                    t_event_bin_buf: Vec::with_capacity(8 * 1024 * 1024),
                    t_event_buf_records: 0,
                    t_event_flush_count: 0,
                    event_bin_writer: None,
                    t_perm_issues: 0,
                    global_stats: g_clone.clone(),
                    prog_files: pf_clone.clone(),
                    prog_dirs:  pd_clone.clone(),
                    prog_size:  ps_clone.clone(),
                    tmpdir: tmpdir_clone.clone(),
                    target_uids: (*target_uids_shared).clone(),
                    thread_id: tid,
                    prof_metadata_ns: pm_clone.clone(),
                    prof_path_ns: pp_clone.clone(),
                    prof_flush_ns: pfns_clone.clone(),
                    prof_flush_bytes: pfb_clone.clone(),
                    prof_flush_count: pfc_clone.clone(),
                    prof_hardlink_checks: ph_clone.clone(),
                    prof_visited_dir_checks: pv_clone.clone(),
                    prof_max_event_buf_records: pmaxr_clone.clone(),
                    prof_max_event_buf_bytes: pmaxb_clone.clone(),
                };
                let skips = skips.clone();
                let hardlinks_shared = hardlink_inodes.clone();
                let visited_dirs_shared = visited_dirs.clone();

                Box::new(move |entry_res| {
                    // --- Error entry: record as permission issue ---
                    let entry = match entry_res {
                        Ok(e) => e,
                        Err(err) => {
                            let err_str = err.to_string();
                            // ignore::Error formats as: "/path/to/dir: Permission denied (os error 13)"
                            let path_str = err_str.find(": ")
                                .map(|idx| err_str[..idx].to_string())
                                .unwrap_or_default();
                            
                            state.t_perm_issues += 1;
                            state.flush_permission_issue(&path_str, "directory", error_code_from_message(&err_str));
                            return WalkState::Continue;
                        }
                    };

                    let path = entry.path();

                    // --- Configured skip_dirs (prefix match — prunes whole subtree) ---
                    for s in &skips {
                        if path.starts_with(s) {
                            return WalkState::Skip;
                        }
                    }

                    let ft = match entry.file_type() {
                        Some(f) => f,
                        None => return WalkState::Continue,
                    };

                    if ft.is_symlink() {
                        state.t_inodes += 1;
                        return WalkState::Continue;
                    }

                    if ft.is_dir() {
                        // --- Name-based skip (critical_skip_dirs) ---
                        if let Some(name) = path.file_name() {
                            let name_str = name.to_string_lossy();
                            if CRITICAL_SKIP_NAMES.contains(&name_str.as_ref()) {
                                return WalkState::Skip;
                            }
                        }

                        // --- Bind mount / Loop deduplication ---
                        let meta_start = Instant::now();
                        if let Ok(meta) = entry.metadata() {
                            state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            let key = (meta.ino(), meta.dev());
                            state.prof_visited_dir_checks.fetch_add(1, Ordering::Relaxed);
                            // DashSet.insert() returns false when key already exists
                            if !visited_dirs_shared.insert(key) {
                                return WalkState::Skip;
                            }

                            // --- Cross-device check: skip NFS / snapshots / bind-mounts ---
                            if let Some(rdev) = root_dev {
                                if meta.dev() != rdev {
                                    return WalkState::Skip;
                                }
                            }
                        } else {
                            state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        }

                        state.t_dirs += 1;
                        state.t_inodes += 1;
                        state.prog_dirs.fetch_add(1, Ordering::Relaxed);
                    } else if ft.is_file() {
                        let meta_start = Instant::now();
                        let meta = match entry.metadata() {
                            Ok(m) => {
                                state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                m
                            },
                            Err(e) => {
                                state.prof_metadata_ns.fetch_add(meta_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                state.t_perm_issues += 1;
                                let path_str = path.to_string_lossy().into_owned();
                                state.flush_permission_issue(&path_str, "file", error_code_from_message(&e.to_string()));
                                return WalkState::Continue;
                            }
                        };

                        // --- Hard-link deduplication ---
                        if meta.nlink() > 1 {
                            state.prof_hardlink_checks.fetch_add(1, Ordering::Relaxed);
                            let key = (meta.ino(), meta.dev());
                            if !hardlinks_shared.insert(key) { return WalkState::Continue; }
                        }

                        // st_blocks * 512 = actual on-disk bytes, same as Python legacy
                        let size = meta.blocks() * 512;
                        let uid  = meta.uid();
                        let is_target = match &state.target_uids {
                            Some(set) => set.contains(&uid),
                            None => true,
                        };

                        state.t_files += 1;
                        state.t_inodes += 1;
                        state.t_size  += size;

                        if is_target {
                            *state.t_uid_sizes.entry(uid).or_insert(0) += size;
                            *state.t_uid_files.entry(uid).or_insert(0) += 1;
                            let path_start = Instant::now();
                            let path_owned = path.to_string_lossy();
                            let path_str = path_owned.as_ref();
                            state.prof_path_ns.fetch_add(path_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            state.push_event_binary(1, uid, size, path_str);
                            state.t_event_buf_records += 1;
                            state.prof_max_event_buf_records.fetch_max(state.t_event_buf_records as u64, Ordering::Relaxed);
                            state.prof_max_event_buf_bytes.fetch_max(state.t_event_bin_buf.len() as u64, Ordering::Relaxed);
                            if state.t_event_buf_records >= SCAN_EVENT_FLUSH_THRESHOLD
                                || state.t_event_bin_buf.len() >= SCAN_EVENT_FLUSH_BYTES_THRESHOLD
                            {
                                state.flush_events();
                            }
                        }

                        // Progress tracking
                        state.prog_files.fetch_add(1, Ordering::Relaxed);
                        state.prog_size.fetch_add(size, Ordering::Relaxed);
                    }

                    WalkState::Continue
                })
            });

        d_clone.store(true, Ordering::SeqCst);
    });

    // --- Main thread: progress display + KeyboardInterrupt polling ---
    let start_time = Instant::now();
    let mut last_report = start_time;
    let mut last_files: u64 = 0;

    while !done.load(Ordering::SeqCst) {
        py.check_signals()?;
        thread::sleep(Duration::from_millis(200));

        let now = Instant::now();
        let elapsed_secs = now.duration_since(last_report).as_secs();
        if elapsed_secs >= 10 {
            let total_files = prog_files.load(Ordering::Relaxed);
            let total_dirs  = prog_dirs.load(Ordering::Relaxed);
            let total_size  = prog_size.load(Ordering::Relaxed);
            let total_elapsed = now.duration_since(start_time).as_secs();
            let rate = total_files.saturating_sub(last_files) as f64 / elapsed_secs as f64;
            let mem_mb = get_rss_mb();
            println!(
                "[{:02}:{:02}:{:02}] Files: {} | Dirs: {} | Size: {} | Rate: {} files/s | Mem: {:.1} MB",
                total_elapsed / 3600, (total_elapsed % 3600) / 60, total_elapsed % 60,
                format_num(total_files), format_num(total_dirs),
                format_size(total_size), format_rate(rate), mem_mb
            );
            last_report = now;
            last_files = total_files;
        }
    }
    // no trailing newline needed — println already adds one

    // --- Build Python return dict ---
    let g = global_stats.lock().unwrap();

    let result = PyDict::new(py);
    result.set_item("total_files",  g.total_files)?;
    result.set_item("total_dirs",   g.total_dirs)?;
    result.set_item("total_inodes", g.total_inodes)?;
    result.set_item("total_size",   g.total_size)?;
    result.set_item("detail_tmpdir", &tmpdir_str)?;
    result.set_item("dir_tmpdir", &tmpdir_str)?;

    let py_uid = PyDict::new(py);
    for (uid, size) in &g.uid_sizes { py_uid.set_item(uid, size)?; }
    result.set_item("uid_sizes", py_uid)?;

    let py_uid_files = PyDict::new(py);
    for (uid, files) in &g.uid_files { py_uid_files.set_item(uid, files)?; }
    result.set_item("uid_files", py_uid_files)?;

    result.set_item("permission_issues_count", g.permission_issues_count)?;
    result.set_item("engine", engine)?;
    if engine == "production" {
        result.set_item("schema", "check-disk-scan")?;
    }

    if debug {
        let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
        let metadata_s = prof_metadata_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        let path_s = prof_path_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        let flush_s = prof_flush_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        let flush_bytes = prof_flush_bytes.load(Ordering::Relaxed);
        let flush_count = prof_flush_count.load(Ordering::Relaxed);
        let hardlink_checks = prof_hardlink_checks.load(Ordering::Relaxed);
        let visited_checks = prof_visited_dir_checks.load(Ordering::Relaxed);
        let max_buf_records = prof_max_event_buf_records.load(Ordering::Relaxed);
        let max_buf_bytes = prof_max_event_buf_bytes.load(Ordering::Relaxed);
        let hardlink_set_size = hardlink_inodes_profile.len();
        let visited_set_size = visited_dirs_profile.len();
        println!("\n[Phase 1 Profile]");
        println!("  Wall time:          {:.2}s", elapsed);
        println!("  Metadata time:      {:.2}s aggregate ({:.1}% of worker time)", metadata_s, metadata_s * 100.0 / elapsed);
        println!("  Path stringify:     {:.2}s aggregate", path_s);
        println!("  TSV flush time:     {:.2}s aggregate", flush_s);
        println!("  TSV flushes:        {}", format_num(flush_count));
        println!("  TSV bytes approx:   {}", format_size(flush_bytes));
        println!("  Hardlink checks:    {}", format_num(hardlink_checks));
        println!("  Visited dir checks: {}", format_num(visited_checks));
        println!("  Max event buffer:   {} records / {}", format_num(max_buf_records), format_size(max_buf_bytes));
        println!("  Hardlink set size:  {}", format_num(hardlink_set_size as u64));
        println!("  Visited set size:   {}", format_num(visited_set_size as u64));
    }

    Ok(result.into())
}
