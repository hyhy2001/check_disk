use dashmap::DashSet;
use ignore::{WalkBuilder, WalkState};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
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

#[cfg(target_os = "linux")]
mod statx_helper {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    const STATX_TYPE: u32 = 0x0001;
    const STATX_MODE: u32 = 0x0002;
    const STATX_NLINK: u32 = 0x0004;
    const STATX_UID: u32 = 0x0008;
    const STATX_INO: u32 = 0x0100;
    const STATX_SIZE: u32 = 0x0200;
    const STATX_MINIMAL: u32 =
        STATX_TYPE | STATX_MODE | STATX_NLINK | STATX_UID | STATX_INO | STATX_SIZE;

    const AT_FDCWD: i32 = -100;
    const AT_SYMLINK_NOFOLLOW: i32 = 0x100;
    const AT_NO_AUTOMOUNT: i32 = 0x800;

    pub(crate) struct StatxResult {
        pub(crate) stx_mode: u16,
        pub(crate) stx_nlink: u32,
        pub(crate) stx_uid: u32,
        pub(crate) stx_size: u64,
        pub(crate) stx_ino: u64,
        pub(crate) stx_dev_major: u32,
        pub(crate) stx_dev_minor: u32,
    }

    impl StatxResult {
        pub(crate) fn is_file(&self) -> bool {
            (self.stx_mode & 0xf000) == 0x8000
        }
        pub(crate) fn is_dir(&self) -> bool {
            (self.stx_mode & 0xf000) == 0x4000
        }
        pub(crate) fn is_symlink(&self) -> bool {
            (self.stx_mode & 0xf000) == 0xa000
        }
        pub(crate) fn dev(&self) -> u64 {
            ((self.stx_dev_major as u64) << 32) | (self.stx_dev_minor as u64)
        }
    }

    pub(crate) fn statx_nofollow(path: &Path) -> Option<StatxResult> {
        let cpath = CString::new(path.as_os_str().as_bytes()).ok()?;

        let mut buf = std::mem::MaybeUninit::<libc::statx>::zeroed();

        let ret = unsafe {
            libc::syscall(
                libc::SYS_statx,
                AT_FDCWD,
                cpath.as_ptr(),
                AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT,
                STATX_MINIMAL,
                buf.as_mut_ptr(),
            )
        };

        if ret != 0 {
            return None;
        }

        let s = unsafe { buf.assume_init() };
        Some(StatxResult {
            stx_mode: s.stx_mode,
            stx_nlink: s.stx_nlink,
            stx_uid: s.stx_uid,
            stx_size: s.stx_size,
            stx_ino: s.stx_ino,
            stx_dev_major: s.stx_dev_major,
            stx_dev_minor: s.stx_dev_minor,
        })
    }
}

#[cfg(not(target_os = "linux"))]
mod statx_helper {
    use std::path::Path;

    pub(crate) struct StatxResult;

    pub(crate) fn statx_nofollow(_path: &Path) -> Option<StatxResult> {
        None
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
                    event_bin_writers: (0..ThreadLocalState::EVENT_BUCKETS).map(|_| None).collect(),
                    t_perm_issues: 0,
                    global_stats: g_clone.clone(),
                    prog_files: pf_clone.clone(),
                    prog_dirs: pd_clone.clone(),
                    prog_size: ps_clone.clone(),
                    pending_prog_files: 0,
                    pending_prog_dirs: 0,
                    pending_prog_size: 0,
                    tmpdir: tmpdir_clone.clone(),
                    target_uids: (*target_uids_shared).clone(),
                    thread_id: tid,
                    profile_enabled: debug,
                    prof_metadata_ns: pm_clone.clone(),
                    prof_path_ns: pp_clone.clone(),
                    prof_flush_ns: pfns_clone.clone(),
                    prof_flush_bytes: pfb_clone.clone(),
                    prof_flush_count: pfc_clone.clone(),
                    prof_hardlink_checks: ph_clone.clone(),
                    prof_max_event_buf_records: pmaxr_clone.clone(),
                    prof_max_event_buf_bytes: pmaxb_clone.clone(),
                    perm_writer: None,
                    dir_agg_writer: None,
                };
                let skips = skips.clone();
                let hardlinks_shared = hardlink_inodes.clone();

                Box::new(move |entry_res| {
                    // --- Error entry: record as permission issue ---
                    let entry = match entry_res {
                        Ok(e) => e,
                        Err(err) => {
                            let err_str = err.to_string();
                            // ignore::Error formats as: "/path/to/dir: Permission denied (os error 13)"
                            let path_str = err_str
                                .find(": ")
                                .map(|idx| err_str[..idx].to_string())
                                .unwrap_or_default();

                            state.t_perm_issues += 1;
                            state.flush_permission_issue(
                                &path_str,
                                "directory",
                                error_code_from_message(&err_str),
                            );
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

                        // --- Cross-device check: skip NFS / snapshots / bind-mounts ---
                        // We previously also kept a DashSet<(ino, dev)> of every
                        // visited dir to break loops, but at 7M+ entries it cost
                        // ~25% wall time to cache-miss into and only ever fired
                        // once per scan in practice (POSIX forbids same-device
                        // hardlinked dirs; ignore::WalkBuilder doesn't follow
                        // symlinks by default; bind-mounts cross devices and
                        // are already caught below).
                        let meta_start = debug.then(Instant::now);
                        if let Ok(meta) = entry.metadata() {
                            if let Some(start) = meta_start {
                                state.prof_metadata_ns.fetch_add(
                                    start.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                            }
                            if let Some(rdev) = root_dev {
                                if meta.dev() != rdev {
                                    return WalkState::Skip;
                                }
                            }
                        } else if let Some(start) = meta_start {
                            state.prof_metadata_ns.fetch_add(
                                start.elapsed().as_nanos() as u64,
                                Ordering::Relaxed,
                            );
                        }

                        state.t_dirs += 1;
                        state.t_inodes += 1;
                        state.add_progress(0, 1, 0);
                    } else if ft.is_file() {
                        let meta_start = debug.then(Instant::now);
                        #[cfg(target_os = "linux")]
                        let statx_res = statx_helper::statx_nofollow(path);
                        #[cfg(target_os = "linux")]
                        let (_file_type_is_file, _file_type_is_symlink, _file_type_is_dir, size, uid, nlink, ino, dev) =
                            match statx_res {
                                Some(s) => (
                                    s.is_file(),
                                    s.is_symlink(),
                                    s.is_dir(),
                                    s.stx_size,
                                    s.stx_uid,
                                    s.stx_nlink,
                                    s.stx_ino,
                                    s.dev(),
                                ),
                                None => match fs::symlink_metadata(path) {
                                    Ok(m) => (
                                        m.file_type().is_file(),
                                        m.file_type().is_symlink(),
                                        m.file_type().is_dir(),
                                        m.size(),
                                        m.uid(),
                                        m.nlink() as u32,
                                        m.ino(),
                                        m.dev(),
                                    ),
                                    Err(e) => {
                                        if let Some(start) = meta_start {
                                            state.prof_metadata_ns.fetch_add(
                                                start.elapsed().as_nanos() as u64,
                                                Ordering::Relaxed,
                                            );
                                        }
                                        state.t_perm_issues += 1;
                                        let path_str = path.to_string_lossy().into_owned();
                                        state.flush_permission_issue(
                                            &path_str,
                                            "file",
                                            error_code_from_message(&e.to_string()),
                                        );
                                        return WalkState::Continue;
                                    }
                                },
                            };
                        #[cfg(not(target_os = "linux"))]
                        let (_file_type_is_file, _file_type_is_symlink, _file_type_is_dir, size, uid, nlink, ino, dev) =
                            match fs::symlink_metadata(path) {
                                Ok(m) => (
                                    m.file_type().is_file(),
                                    m.file_type().is_symlink(),
                                    m.file_type().is_dir(),
                                    m.size(),
                                    m.uid(),
                                    m.nlink() as u32,
                                    m.ino(),
                                    m.dev(),
                                ),
                                Err(e) => {
                                    if let Some(start) = meta_start {
                                        state.prof_metadata_ns.fetch_add(
                                            start.elapsed().as_nanos() as u64,
                                            Ordering::Relaxed,
                                        );
                                    }
                                    state.t_perm_issues += 1;
                                    let path_str = path.to_string_lossy().into_owned();
                                    state.flush_permission_issue(
                                        &path_str,
                                        "file",
                                        error_code_from_message(&e.to_string()),
                                    );
                                    return WalkState::Continue;
                                }
                            };
                        if let Some(start) = meta_start {
                            state.prof_metadata_ns.fetch_add(
                                start.elapsed().as_nanos() as u64,
                                Ordering::Relaxed,
                            );
                        }

                        // --- Hard-link deduplication ---
                        if nlink > 1 {
                            if debug {
                                state.prof_hardlink_checks.fetch_add(1, Ordering::Relaxed);
                            }
                            let key = (ino, dev);
                            if !hardlinks_shared.insert(key) {
                                return WalkState::Continue;
                            }
                        }

                        let is_target = match &state.target_uids {
                            Some(set) => set.contains(&uid),
                            None => true,
                        };

                        state.t_files += 1;
                        state.t_inodes += 1;
                        state.t_size += size;

                        if is_target {
                            *state.t_uid_sizes.entry(uid).or_insert(0) += size;
                            *state.t_uid_files.entry(uid).or_insert(0) += 1;
                            let path_start = debug.then(Instant::now);
                            let path_owned = path.to_string_lossy();
                            let path_str = path_owned.as_ref();
                            if let Some(start) = path_start {
                                state.prof_path_ns.fetch_add(
                                    start.elapsed().as_nanos() as u64,
                                    Ordering::Relaxed,
                                );
                            }
                            state.push_event_binary(1, uid, size, path_str);
                            state.add_dir_size(uid, size, path_str);
                            let total_records = state.event_records();
                            let total_bytes = state.event_buffer_bytes();
                            if debug {
                                state
                                    .prof_max_event_buf_records
                                    .fetch_max(total_records as u64, Ordering::Relaxed);
                                state
                                    .prof_max_event_buf_bytes
                                    .fetch_max(total_bytes as u64, Ordering::Relaxed);
                            }
                            if total_records >= SCAN_EVENT_FLUSH_THRESHOLD
                                || total_bytes >= SCAN_EVENT_FLUSH_BYTES_THRESHOLD
                            {
                                state.flush_events();
                            }
                        }

                        // Progress tracking
                        state.add_progress(1, 0, size);
                    }

                    WalkState::Continue
                })
            });

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
