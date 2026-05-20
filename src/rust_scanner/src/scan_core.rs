use dashmap::DashSet;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use crossbeam_channel::{bounded, RecvTimeoutError, Sender};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::scan_constants::{
    CRITICAL_SKIP_NAMES, SCAN_EVENT_FLUSH_BYTES_THRESHOLD, SCAN_EVENT_FLUSH_THRESHOLD,
};
use crate::scan_state::{GlobalStats, ThreadLocalState};
use crate::scan_utils::{
    error_code_from_message, format_num, format_rate, format_size, get_rss_mb,
};

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
        total_files: 0,
        total_dirs: 0,
        total_inodes: 0,
        total_size: 0,
        uid_sizes: HashMap::new(),
        uid_files: HashMap::new(),
        permission_issues_count: 0,
    }));
    let prog_files = Arc::new(AtomicU64::new(0));
    let prog_dirs = Arc::new(AtomicU64::new(0));
    let prog_size = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let prof_metadata_ns = Arc::new(AtomicU64::new(0));
    let prof_path_ns = Arc::new(AtomicU64::new(0));
    let prof_flush_ns = Arc::new(AtomicU64::new(0));
    let prof_flush_bytes = Arc::new(AtomicU64::new(0));
    let prof_flush_count = Arc::new(AtomicU64::new(0));
    let prof_hardlink_checks = Arc::new(AtomicU64::new(0));
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
    let pmaxr_clone = prof_max_event_buf_records.clone();
    let pmaxb_clone = prof_max_event_buf_bytes.clone();

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    // Phase 1 is metadata-I/O-bound (lstat dominates wall time on NFS /
    // large filesystems — production logs show ~25 threads blocked on
    // metadata vs ~559s wall). cpus*4 lets the kernel keep more inflight
    // requests per disk; clamp(4, 64) keeps small machines reasonable
    // while not capping I/O-rich storage. CLI `--max-workers` overrides.
    let default_threads = (cpus * 4).clamp(4, 64);
    let threads_count = max_workers.unwrap_or(default_threads).max(1);
    let thread_counter = Arc::new(AtomicUsize::new(0));
    let target_uids_shared =
        Arc::new(target_uids.map(|uids| uids.into_iter().collect::<HashSet<u32>>()));
    // Shared cross-worker hard-link deduplication — DashSet avoids Mutex bottleneck
    let hardlink_inodes: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    let hardlink_inodes_profile = hardlink_inodes.clone();

    let _walk_thread = thread::spawn(move || {
        // Ensure `done` flips to true even if the parallel walk panics —
        // otherwise the main progress loop spins forever waiting on a flag
        // that will never be set. RAII guard runs on every exit path.
        struct DoneGuard(Arc<AtomicBool>);
        impl Drop for DoneGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let _done_guard = DoneGuard(d_clone.clone());

        // ─── Bounded-queue parallel walker ─────────────────────────
        // Replaces ignore::WalkBuilder. Caps queue at 200K dirs to
        // bound RAM (was ~6 GB unbounded internal queue → ~40 MB now).
        // Workers fall back to inline DFS via local Vec stack when
        // the shared channel is full — no blocking, no deadlock.
        const QUEUE_CAP: usize = 200_000;
        let (tx, rx) = bounded::<PathBuf>(QUEUE_CAP);
        let rx = Arc::new(rx);
        let active_count = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        // Seed root dir
        let _ = tx.send(PathBuf::from(&dir_clone));

        let mut handles = Vec::with_capacity(threads_count);
        for _ in 0..threads_count {
            let tid = thread_counter.fetch_add(1, Ordering::SeqCst);
            let tx_w: Sender<PathBuf> = tx.clone();
            let rx_w = Arc::clone(&rx);
            let active_w = Arc::clone(&active_count);
            let shutdown_w = Arc::clone(&shutdown);

            // Per-worker Arc clones (same as WalkBuilder factory)
            let g_w = g_clone.clone();
            let pf_w = pf_clone.clone();
            let pd_w = pd_clone.clone();
            let ps_w = ps_clone.clone();
            let tmpdir_w = tmpdir_clone.clone();
            let target_uids_w = target_uids_shared.clone();
            let pm_w = pm_clone.clone();
            let pp_w = pp_clone.clone();
            let pfns_w = pfns_clone.clone();
            let pfb_w = pfb_clone.clone();
            let pfc_w = pfc_clone.clone();
            let ph_w = ph_clone.clone();
            let pmaxr_w = pmaxr_clone.clone();
            let pmaxb_w = pmaxb_clone.clone();
            let skips_w = skips.clone();
            let hardlinks_w = hardlink_inodes.clone();

            handles.push(thread::spawn(move || {
                let mut state = ThreadLocalState {
                    t_files: 0,
                    t_dirs: 0,
                    t_inodes: 0,
                    t_size: 0,
                    t_uid_sizes: HashMap::new(),
                    t_uid_files: HashMap::new(),
                    t_dir_sizes: HashMap::new(),
                    t_event_bin_bufs: (0..ThreadLocalState::EVENT_BUCKETS)
                        .map(|_| Vec::with_capacity(1024 * 1024))
                        .collect(),
                    t_event_buf_records: vec![0; ThreadLocalState::EVENT_BUCKETS],
                    t_event_flush_count: 0,
                    event_bin_writers: (0..ThreadLocalState::EVENT_BUCKETS)
                        .map(|_| None)
                        .collect(),
                    t_perm_issues: 0,
                    global_stats: g_w,
                    prog_files: pf_w,
                    prog_dirs: pd_w,
                    prog_size: ps_w,
                    pending_prog_files: 0,
                    pending_prog_dirs: 0,
                    pending_prog_size: 0,
                    tmpdir: tmpdir_w,
                    target_uids: (*target_uids_w).clone(),
                    thread_id: tid,
                    profile_enabled: debug,
                    prof_metadata_ns: pm_w,
                    prof_path_ns: pp_w,
                    prof_flush_ns: pfns_w,
                    prof_flush_bytes: pfb_w,
                    prof_flush_count: pfc_w,
                    prof_hardlink_checks: ph_w,
                    prof_max_event_buf_records: pmaxr_w,
                    prof_max_event_buf_bytes: pmaxb_w,
                    perm_writer: None,
                    dir_agg_writer: None,
                };

                loop {
                    if shutdown_w.load(Ordering::Relaxed) {
                        break;
                    }
                    let dir_path = match rx_w.recv_timeout(Duration::from_millis(5)) {
                        Ok(p) => p,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    };

                    active_w.fetch_add(1, Ordering::AcqRel);
                    process_dir_inline(
                        dir_path,
                        &tx_w,
                        &mut state,
                        &hardlinks_w,
                        &skips_w,
                        root_dev,
                        debug,
                    );
                    let prev = active_w.fetch_sub(1, Ordering::AcqRel);

                    // Termination protocol: last active worker drains
                    // remaining items, then signals shutdown if empty.
                    if prev == 1 {
                        loop {
                            match rx_w.try_recv() {
                                Ok(extra) => {
                                    active_w.fetch_add(1, Ordering::AcqRel);
                                    process_dir_inline(
                                        extra,
                                        &tx_w,
                                        &mut state,
                                        &hardlinks_w,
                                        &skips_w,
                                        root_dev,
                                        debug,
                                    );
                                    let p2 = active_w.fetch_sub(1, Ordering::AcqRel);
                                    if p2 != 1 {
                                        break;
                                    }
                                }
                                Err(_) => {
                                    shutdown_w.store(true, Ordering::SeqCst);
                                    break;
                                }
                            }
                        }
                    }
                }
                // state drops here → ThreadLocalState::drop() flushes all buffers
            }));
        }
        drop(tx); // Main thread drops its sender

        // Join all workers
        for h in handles {
            let _ = h.join();
        }

        // The DoneGuard above already flips `done` on drop; this explicit
        // store is redundant but harmless and keeps the happy-path obvious.
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
            let total_dirs = prog_dirs.load(Ordering::Relaxed);
            let total_size = prog_size.load(Ordering::Relaxed);
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
    result.set_item("total_files", g.total_files)?;
    result.set_item("total_dirs", g.total_dirs)?;
    result.set_item("total_inodes", g.total_inodes)?;
    result.set_item("total_size", g.total_size)?;
    result.set_item("detail_tmpdir", &tmpdir_str)?;
    result.set_item("dir_tmpdir", &tmpdir_str)?;

    let py_uid = PyDict::new(py);
    for (uid, size) in &g.uid_sizes {
        py_uid.set_item(uid, size)?;
    }
    result.set_item("uid_sizes", py_uid)?;

    let py_uid_files = PyDict::new(py);
    for (uid, files) in &g.uid_files {
        py_uid_files.set_item(uid, files)?;
    }
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
        let max_buf_records = prof_max_event_buf_records.load(Ordering::Relaxed);
        let max_buf_bytes = prof_max_event_buf_bytes.load(Ordering::Relaxed);
        let hardlink_set_size = hardlink_inodes_profile.len();
        println!("\n[Phase 1 Profile]");
        println!("  Wall time:          {:.2}s", elapsed);
        println!(
            "  Metadata time:      {:.2}s aggregate ({:.1}% of worker time)",
            metadata_s,
            metadata_s * 100.0 / elapsed
        );
        println!("  Path stringify:     {:.2}s aggregate", path_s);
        println!("  TSV flush time:     {:.2}s aggregate", flush_s);
        println!("  TSV flushes:        {}", format_num(flush_count));
        println!("  TSV bytes approx:   {}", format_size(flush_bytes));
        println!("  Hardlink checks:    {}", format_num(hardlink_checks));
        println!(
            "  Max event buffer:   {} records / {}",
            format_num(max_buf_records),
            format_size(max_buf_bytes)
        );
        println!(
            "  Hardlink set size:  {}",
            format_num(hardlink_set_size as u64)
        );
    }

    Ok(result.into())
}

#[inline]
fn record_metadata_ns(state: &mut ThreadLocalState, start: Option<Instant>, debug: bool) {
    if debug {
        if let Some(s) = start {
            state
                .prof_metadata_ns
                .fetch_add(s.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
}

#[inline]
fn record_path_ns(state: &mut ThreadLocalState, start: Option<Instant>, debug: bool) {
    if debug {
        if let Some(s) = start {
            state
                .prof_path_ns
                .fetch_add(s.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
}

fn process_dir_inline(
    root: std::path::PathBuf,
    tx: &crossbeam_channel::Sender<std::path::PathBuf>,
    state: &mut ThreadLocalState,
    hardlinks: &Arc<DashSet<(u64, u64)>>,
    skips: &[String],
    root_dev: Option<u64>,
    debug: bool,
) {
    let mut stack: Vec<std::path::PathBuf> = vec![root];
    while let Some(dir_path) = stack.pop() {
        let read_iter = match fs::read_dir(&dir_path) {
            Ok(it) => it,
            Err(e) => {
                state.t_perm_issues += 1;
                state.flush_permission_issue(
                    &dir_path.to_string_lossy(),
                    "directory",
                    error_code_from_message(&e.to_string()),
                );
                continue;
            }
        };
        'entry: for entry_res in read_iter {
            let entry = match entry_res {
                Ok(e) => e,
                Err(e) => {
                    state.t_perm_issues += 1;
                    state.flush_permission_issue(
                        &dir_path.to_string_lossy(),
                        "directory",
                        error_code_from_message(&e.to_string()),
                    );
                    continue;
                }
            };
            let path = entry.path();

            for s in skips {
                if path.starts_with(s) {
                    continue 'entry;
                }
            }

            let ft = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };

            if ft.is_symlink() {
                state.t_inodes += 1;
                continue;
            }

            let meta_start = debug.then(Instant::now);
            let meta = match fs::symlink_metadata(&path) {
                Ok(m) => m,
                Err(e) => {
                    record_metadata_ns(state, meta_start, debug);
                    state.t_perm_issues += 1;
                    state.flush_permission_issue(
                        &path.to_string_lossy(),
                        "file",
                        error_code_from_message(&e.to_string()),
                    );
                    continue;
                }
            };
            record_metadata_ns(state, meta_start, debug);

            if ft.is_dir() {
                if let Some(name) = path.file_name() {
                    if CRITICAL_SKIP_NAMES.contains(&name.to_string_lossy().as_ref()) {
                        continue;
                    }
                }
                if let Some(rdev) = root_dev {
                    if meta.dev() != rdev {
                        continue;
                    }
                }

                state.t_dirs += 1;
                state.t_inodes += 1;
                state.add_progress(0, 1, 0);

                match tx.try_send(path.clone()) {
                    Ok(_) => {}
                    Err(_) => {
                        stack.push(path);
                    }
                }
            } else if ft.is_file() {
                if meta.nlink() > 1 {
                    if debug {
                        state.prof_hardlink_checks.fetch_add(1, Ordering::Relaxed);
                    }
                    if !hardlinks.insert((meta.ino(), meta.dev())) {
                        continue;
                    }
                }

                let size = meta.blocks() * 512;
                let uid = meta.uid();
                let is_target = match state.target_uids.as_ref() {
                    Some(s) => s.contains(&uid),
                    None => true,
                };

                state.t_files += 1;
                state.t_inodes += 1;
                state.t_size += size;

                if is_target {
                    *state.t_uid_sizes.entry(uid).or_insert(0) += size;
                    *state.t_uid_files.entry(uid).or_insert(0) += 1;
                    let path_start = debug.then(Instant::now);
                    let path_str = path.to_string_lossy();
                    record_path_ns(state, path_start, debug);
                    state.push_event_binary(1, uid, size, path_str.as_ref());
                    state.add_dir_size(uid, size, path_str.as_ref());
                    let recs = state.event_records();
                    let bytes = state.event_buffer_bytes();
                    if debug {
                        state
                            .prof_max_event_buf_records
                            .fetch_max(recs as u64, Ordering::Relaxed);
                        state
                            .prof_max_event_buf_bytes
                            .fetch_max(bytes as u64, Ordering::Relaxed);
                    }
                    if recs >= SCAN_EVENT_FLUSH_THRESHOLD
                        || bytes >= SCAN_EVENT_FLUSH_BYTES_THRESHOLD
                    {
                        state.flush_events();
                    }
                }
                state.add_progress(1, 0, size);
            }
        }
    }
}
