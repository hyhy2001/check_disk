use dashmap::{DashMap, DashSet};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rayon::Scope;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::scan_constants::{
    CRITICAL_SKIP_NAMES, DIR_CHUNK_SIZE, DIR_CHUNK_THRESHOLD, SCAN_EVENT_FLUSH_BYTES_THRESHOLD,
    SCAN_EVENT_FLUSH_THRESHOLD,
};
use crate::scan_state::{GlobalStats, ThreadLocalState};
use crate::scan_utils::{
    error_code_from_message, format_num, format_rate, format_size, get_rss_mb,
};

// Shared state accessible to all Rayon tasks.
struct SharedScanState {
    hardlinks: Arc<DashSet<(u64, u64)>>,
    global_stats: Arc<Mutex<GlobalStats>>,
    prog_files: Arc<AtomicU64>,
    prog_dirs: Arc<AtomicU64>,
    prog_size: Arc<AtomicU64>,
    prof_metadata_ns: Arc<AtomicU64>,
    prof_path_ns: Arc<AtomicU64>,
    prof_flush_ns: Arc<AtomicU64>,
    prof_flush_bytes: Arc<AtomicU64>,
    prof_flush_count: Arc<AtomicU64>,
    prof_hardlink_checks: Arc<AtomicU64>,
    prof_max_event_buf_records: Arc<AtomicU64>,
    prof_max_event_buf_bytes: Arc<AtomicU64>,
    tmpdir: String,
    target_uids: Option<HashSet<u32>>,
    skips: Vec<String>,
    root_dev: Option<u64>,
    debug: bool,
    registry: Arc<DashMap<usize, Arc<Mutex<ThreadLocalState>>>>,
}

thread_local! {
    static WORKER_STATE: std::cell::RefCell<Option<Arc<Mutex<ThreadLocalState>>>> = std::cell::RefCell::new(None);
}

fn get_or_init_state(shared: &Arc<SharedScanState>) -> Arc<Mutex<ThreadLocalState>> {
    WORKER_STATE.with(|cell| {
        let mut opt = cell.borrow_mut();
        if let Some(s) = opt.as_ref() {
            return Arc::clone(s);
        }
        let idx = rayon::current_thread_index().unwrap_or(usize::MAX);
        let state = Arc::new(Mutex::new(ThreadLocalState {
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
            queue_full_fallbacks: 0,
            idle_time_ns: 0,
            active_time_ns: 0,
            tasks_processed: 0,
            global_stats: shared.global_stats.clone(),
            prog_files: shared.prog_files.clone(),
            prog_dirs: shared.prog_dirs.clone(),
            prog_size: shared.prog_size.clone(),
            pending_prog_files: 0,
            pending_prog_dirs: 0,
            pending_prog_size: 0,
            tmpdir: shared.tmpdir.clone(),
            target_uids: shared.target_uids.clone(),
            thread_id: idx,
            profile_enabled: shared.debug,
            prof_metadata_ns: shared.prof_metadata_ns.clone(),
            prof_path_ns: shared.prof_path_ns.clone(),
            prof_flush_ns: shared.prof_flush_ns.clone(),
            prof_flush_bytes: shared.prof_flush_bytes.clone(),
            prof_flush_count: shared.prof_flush_count.clone(),
            prof_hardlink_checks: shared.prof_hardlink_checks.clone(),
            prof_max_event_buf_records: shared.prof_max_event_buf_records.clone(),
            prof_max_event_buf_bytes: shared.prof_max_event_buf_bytes.clone(),
            perm_writer: None,
            dir_agg_writer: None,
        }));
        shared.registry.insert(idx, Arc::clone(&state));
        *opt = Some(Arc::clone(&state));
        Arc::clone(opt.as_ref().unwrap())
    })
}

fn process_dir_rayon<'scope>(
    dir: PathBuf,
    scope: &Scope<'scope>,
    shared: Arc<SharedScanState>,
) {
    let entries = match fs::read_dir(&dir) {
        Ok(e) => e,
        Err(err) => {
            // log perm error to thread-local state
            let state_arc = get_or_init_state(&shared);
            let mut state = state_arc.lock().unwrap();
            state.t_perm_issues += 1;
            state.flush_permission_issue(&dir.to_string_lossy(), "directory", "EACCES");
            return;
        }
    };

    let mut subdirs = Vec::new();
    let mut files = Vec::new();

    'entry_collect: for entry_res in entries {
        let entry = match entry_res {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        for s in &shared.skips {
            if path.starts_with(s) {
                continue 'entry_collect;
            }
        }
        let ftype = match entry.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };
        if ftype.is_symlink() {
            let state_arc = get_or_init_state(&shared);
            let mut state = state_arc.lock().unwrap();
            state.t_inodes += 1;
            continue;
        }
        if ftype.is_dir() {
            if let Some(name) = path.file_name() {
                if CRITICAL_SKIP_NAMES.contains(&name.to_string_lossy().as_ref()) {
                    continue;
                }
            }
            subdirs.push(path);
        } else if ftype.is_file() {
            if let Some(name) = path.file_name() {
                files.push(name.to_os_string());
            }
        }
    }

    for subdir in subdirs {
        let s = Arc::clone(&shared);
        let sub = subdir.clone();
        // update progress counts
        let state_arc = get_or_init_state(&shared);
        {
            let mut state = state_arc.lock().unwrap();
            state.t_dirs += 1;
            state.t_inodes += 1;
            state.add_progress(0, 1, 0);
        }
        scope.spawn(move |sco| process_dir_rayon(sub, sco, s));
    }

    let dir_arc = Arc::new(dir);
    if files.len() <= DIR_CHUNK_THRESHOLD {
        let state_arc = get_or_init_state(&shared);
        let mut state = state_arc.lock().unwrap();
        for name in files {
            let path = dir_arc.join(&name);
            process_file_entry(&path, &mut state, &shared.hardlinks, &shared.skips, shared.root_dev, shared.debug);
        }
    } else {
        for chunk in files.chunks(DIR_CHUNK_SIZE) {
            let chunk_vec: Vec<std::ffi::OsString> = chunk.to_vec();
            let dir_clone = Arc::clone(&dir_arc);
            let s = Arc::clone(&shared);
            scope.spawn(move |_| {
                let state_arc = get_or_init_state(&s);
                let mut state = state_arc.lock().unwrap();
                for name in chunk_vec {
                    let path = dir_clone.join(&name);
                    process_file_entry(&path, &mut state, &s.hardlinks, &s.skips, s.root_dev, s.debug);
                }
            });
        }
    }
}

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
    let _ = _tmpdir.keep();

    let global_stats = Arc::new(Mutex::new(GlobalStats::new()));
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

    let root_dev: Option<u64> = fs::metadata(&directory).ok().map(|m| m.dev());

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let default_threads = (cpus * 4).clamp(4, 64);
    let threads_count = max_workers.unwrap_or(default_threads).max(1);

    let shared = Arc::new(SharedScanState {
        hardlinks: Arc::new(DashSet::new()),
        global_stats: global_stats.clone(),
        prog_files: prog_files.clone(),
        prog_dirs: prog_dirs.clone(),
        prog_size: prog_size.clone(),
        prof_metadata_ns: prof_metadata_ns.clone(),
        prof_path_ns: prof_path_ns.clone(),
        prof_flush_ns: prof_flush_ns.clone(),
        prof_flush_bytes: prof_flush_bytes.clone(),
        prof_flush_count: prof_flush_count.clone(),
        prof_hardlink_checks: prof_hardlink_checks.clone(),
        prof_max_event_buf_records: prof_max_event_buf_records.clone(),
        prof_max_event_buf_bytes: prof_max_event_buf_bytes.clone(),
        tmpdir: tmpdir_str.clone(),
        target_uids: target_uids.map(|uids| uids.into_iter().collect::<HashSet<u32>>()),
        skips: skip_dirs.clone(),
        root_dev,
        debug,
        registry: Arc::new(DashMap::new()),
    });
    let hardlink_inodes_profile = shared.hardlinks.clone();

    let d_clone = done.clone();
    let dir_clone = directory.clone();
    let shared_for_scan = Arc::clone(&shared);

    let _walk_thread = thread::spawn(move || {
        struct DoneGuard(Arc<AtomicBool>);
        impl Drop for DoneGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let _done_guard = DoneGuard(d_clone.clone());

        let pool = match rayon::ThreadPoolBuilder::new()
            .num_threads(threads_count)
            .build()
        {
            Ok(p) => p,
            Err(_) => {
                d_clone.store(true, Ordering::SeqCst);
                return;
            }
        };

        let root_path = PathBuf::from(&dir_clone);
        pool.scope(|scope| {
            let s = Arc::clone(&shared_for_scan);
            scope.spawn(move |sc| process_dir_rayon(root_path, sc, s));
        });

        let mut worker_states: Vec<Arc<Mutex<ThreadLocalState>>> = Vec::new();
        for entry in shared_for_scan.registry.iter() {
            worker_states.push(entry.value().clone());
        }

        for state_arc in worker_states {
            if let Ok(mut state) = state_arc.lock() {
                state.flush_events();
                state.flush_dir_aggregates();
                state.flush_progress();
                for writer in &mut state.event_bin_writers {
                    if let Some(w) = writer.as_mut() {
                        let _ = std::io::Write::flush(w);
                    }
                }
                if let Some(writer) = state.perm_writer.as_mut() {
                    let _ = std::io::Write::flush(writer);
                }
                if let Some(writer) = state.dir_agg_writer.as_mut() {
                    let _ = std::io::Write::flush(writer);
                }
                state.merge_into_global();
            }
        }

        d_clone.store(true, Ordering::SeqCst);
    });

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
                total_elapsed / 3600,
                (total_elapsed % 3600) / 60,
                total_elapsed % 60,
                format_num(total_files),
                format_num(total_dirs),
                format_size(total_size),
                format_rate(rate),
                mem_mb
            );
            last_report = now;
            last_files = total_files;
        }
    }

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
        let total_queue_full_fallbacks = g.total_queue_full_fallbacks;
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
        println!("  Hardlink set size:  {}", format_num(hardlink_set_size as u64));
        println!(
            "  Queue-full fallbacks: {}",
            format_num(total_queue_full_fallbacks)
        );

        if !g.worker_files.is_empty() {
            let n = g.worker_files.len();
            let min_files = *g.worker_files.iter().min().unwrap_or(&0);
            let max_files = *g.worker_files.iter().max().unwrap_or(&0);
            let avg_files = g.worker_files.iter().sum::<u64>() / n as u64;

            let min_active = *g.worker_active_ns.iter().min().unwrap_or(&0);
            let max_active = *g.worker_active_ns.iter().max().unwrap_or(&0);
            let avg_active = g.worker_active_ns.iter().sum::<u64>() / n as u64;

            let min_idle = *g.worker_idle_ns.iter().min().unwrap_or(&0);
            let max_idle = *g.worker_idle_ns.iter().max().unwrap_or(&0);
            let avg_idle = g.worker_idle_ns.iter().sum::<u64>() / n as u64;

            let avg_idle_pct = if avg_active + avg_idle > 0 {
                (avg_idle * 100) / (avg_active + avg_idle)
            } else {
                0
            };

            println!("  Worker utilization ({} workers):", n);
            println!(
                "    Files processed:  min={}  max={}  avg={}",
                format_num(min_files),
                format_num(max_files),
                format_num(avg_files)
            );
            println!(
                "    Active time:      min={:.1}s   max={:.1}s       avg={:.1}s",
                min_active as f64 / 1e9,
                max_active as f64 / 1e9,
                avg_active as f64 / 1e9
            );
            println!(
                "    Idle time:        min={:.1}s   max={:.1}s       avg={:.1}s",
                min_idle as f64 / 1e9,
                max_idle as f64 / 1e9,
                avg_idle as f64 / 1e9
            );
            println!(
                "    Idle %:           min={}%     max={}%        avg={}%",
                0,
                0,
                avg_idle_pct
            );
        }
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

fn process_file_entry(
    path: &std::path::PathBuf,
    state: &mut ThreadLocalState,
    hardlinks: &Arc<DashSet<(u64, u64)>>,
    skips: &[String],
    root_dev: Option<u64>,
    debug: bool,
) {
    for s in skips {
        if path.starts_with(s) {
            return;
        }
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
            return;
        }
    };
    record_metadata_ns(state, meta_start, debug);

    if let Ok(ft) = path.symlink_metadata().and_then(|m| Ok(m.file_type())) {
        if ft.is_dir() {
            if let Some(rdev) = root_dev {
                if meta.dev() != rdev {
                    return;
                }
            }
            state.t_dirs += 1;
            state.t_inodes += 1;
            state.add_progress(0, 1, 0);
            match state.global_stats.lock() {
                Ok(_) => {}
                Err(_) => {}
            }
            // Attempt to enqueue dir for further processing
            // Note: we don't have tx here; directories are primarily handled in process_dir_inline
            return;
        }
    }

    if meta.file_type().is_symlink() {
        state.t_inodes += 1;
        return;
    }

    if meta.nlink() > 1 {
        if debug {
            state.prof_hardlink_checks.fetch_add(1, Ordering::Relaxed);
        }
        if !hardlinks.insert((meta.ino(), meta.dev())) {
            return;
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
        if recs >= SCAN_EVENT_FLUSH_THRESHOLD || bytes >= SCAN_EVENT_FLUSH_BYTES_THRESHOLD
        {
            state.flush_events();
        }
    }
    state.add_progress(1, 0, size);
}

