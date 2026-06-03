use dashmap::DashSet;
use ignore::{WalkBuilder, WalkState};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs as std_fs;
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Filesystem magic numbers for pseudo/virtual filesystems (ncdu exclude_kernfs approach).
const KERNFS_MAGIC: &[i64] = &[
    0x9fa0,      // proc
    0x62656572,  // sysfs
    0x64626720,  // debugfs
    0x01021994,  // tmpfs
    0x1cd1,      // devpts
    0x42494e4d,  // binfmtfs
    0x27e0eb,    // cgroup
    0x63677270,  // cgroup2
    0x794c7630,  // overlayfs
    0x858458f6,  // ramfs
    0x73636673,  // securityfs
    0x67596969,  // tracefs
];

fn is_kernfs_path(path: &std::path::Path) -> bool {
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;
    let c_path = match CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => return false,
    };
    unsafe {
        let mut buf = MaybeUninit::<libc::statfs>::uninit();
        if libc::statfs(c_path.as_ptr(), buf.as_mut_ptr()) != 0 {
            return false;
        }
        let fs = buf.assume_init();
        KERNFS_MAGIC.contains(&(fs.f_type as i64))
    }
}

/// NFS superblock magic numbers (`f_type` from statfs). NFSv2/3/4 all report
/// the same magic. Used to decide whether hardlink dedup must fall back to a
/// statx-based key, because NFS clients hand out unstable/anonymous `st_dev`
/// values — the same inode reached via two paths can report different `st_dev`,
/// so the legacy `(ino, dev)` key fails to collapse hardlinks and the file's
/// bytes get counted more than once (df-vs-scan size inflation).
const NFS_SUPER_MAGIC: i64 = 0x6969;

/// True when `path` lives on an NFS mount (statfs f_type == NFS magic).
/// Determined once at the scan root, not per file.
fn is_nfs_path(path: &str) -> bool {
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    let c_path = match CString::new(path.as_bytes()) {
        Ok(p) => p,
        Err(_) => return false,
    };
    unsafe {
        let mut buf = MaybeUninit::<libc::statfs>::uninit();
        if libc::statfs(c_path.as_ptr(), buf.as_mut_ptr()) != 0 {
            return false;
        }
        let fs = buf.assume_init();
        (fs.f_type as i64) == NFS_SUPER_MAGIC
    }
}

/// Stable hardlink-dedup key for one file, robust on NFS.
///
/// On a normal local filesystem `(st_ino, st_dev)` uniquely identifies an
/// inode, so we use it directly. On NFS, `st_dev` is an anonymous client-side
/// value that is not guaranteed stable across paths/mounts, which breaks the
/// dedup set. When `use_statx` is set we instead query `statx()` and key on
/// `(stx_ino, stx_mnt_id)`: the mount id is stable for the duration of the
/// scan, giving a reliable identity. If the `statx` syscall is unavailable
/// (very old kernel) we fall back to `(ino, dev)`.
fn hardlink_key(path: &std::path::Path, ino: u64, dev: u64, use_statx: bool) -> (u64, u64) {
    if !use_statx {
        return (ino, dev);
    }
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;
    let c_path = match CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => return (ino, dev),
    };
    unsafe {
        let mut stx = MaybeUninit::<libc::statx>::uninit();
        let rc = libc::statx(
            libc::AT_FDCWD,
            c_path.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW | libc::AT_STATX_DONT_SYNC,
            (libc::STATX_INO | libc::STATX_MNT_ID) as u32,
            stx.as_mut_ptr(),
        );
        if rc != 0 {
            return (ino, dev); // statx unsupported / failed: keep legacy key
        }
        let s = stx.assume_init();
        // stx_mnt_id is only valid if the kernel set STATX_MNT_ID in the mask.
        let mnt_id = if (s.stx_mask & libc::STATX_MNT_ID) != 0 {
            s.stx_mnt_id
        } else {
            dev // mnt_id unavailable: degrade to dev, still better than nothing
        };
        (s.stx_ino, mnt_id)
    }
}

/// True if `mp` is the scan root itself or a descendant path of it.
/// Scanning "/" treats every mount as a descendant.
fn mount_is_under_root(mp: &str, root_norm: &str) -> bool {
    if root_norm.is_empty() || root_norm == "/" {
        return true;
    }
    mp == root_norm || mp.starts_with(&format!("{}/", root_norm))
}

/// Pure mountinfo analysis: given the raw `/proc/self/mountinfo` text and the
/// scan root, return the set of mount points STRICTLY under the root that are
/// bind-mount duplicates of a source `(major:minor, fs_root)` ALREADY counted
/// within this scan. Kept pure (no syscalls) so it is unit-testable.
///
/// Correctness rule (important): we only skip an in-root mount point when the
/// *first* mount of the same source bytes is also under the scan root — i.e.
/// those bytes are genuinely reached and counted elsewhere in this walk.
/// If the first/source mount lies OUTSIDE the scan root, the in-root mount is
/// the only reachable copy and must NOT be skipped, or the scan undercounts.
fn child_bind_dups_to_skip(mountinfo: &str, scan_root: &str) -> HashSet<String> {
    let root_norm = scan_root.trim_end_matches('/');
    let mut skip: HashSet<String> = HashSet::new();
    // (major:minor, fs_root_subpath) -> was the first mount of this source
    // located *under the scan root*? Only then are its bytes already counted,
    // making later in-root copies safe to skip.
    let mut origin_under_root: std::collections::HashMap<(String, String), bool> =
        std::collections::HashMap::new();

    for line in mountinfo.lines() {
        // mountinfo: id pid major:minor root mount_point options...
        //            [0] [1]   [2]       [3]  [4]
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 5 {
            continue;
        }
        let origin_key = (parts[2].to_string(), parts[3].to_string());
        let mp = parts[4];
        let under_root = mount_is_under_root(mp, root_norm);
        match origin_under_root.get(&origin_key) {
            Some(&first_under_root) => {
                // A later mount of the same source. Skip it only if it is under
                // the root (so the walk would otherwise reach it) AND the first
                // copy was already under the root (so the bytes are counted).
                if under_root && mp != root_norm && first_under_root {
                    skip.insert(mp.to_string());
                }
            }
            None => {
                // Remember where the FIRST copy of this source lives.
                origin_under_root.insert(origin_key, under_root);
            }
        }
    }
    skip
}

/// Read /proc/self/mountinfo and return:
/// 1. Set of (dev, ino) for bind-mount duplicates
/// 2. Set of mount-point paths on pseudo/virtual filesystems
/// 3. Set of mount-point paths strictly under `scan_root` that must be
///    skipped outright to avoid double-counting. This covers bind mounts of
///    a source tree that is already reachable elsewhere in the scan (same
///    `major:minor` + filesystem-root sub-path seen at >1 mount point — the
///    authoritative bind signal from mountinfo fields 3 and 4), as well as
///    tmpfs/overlay/kernfs mounts that hang off the scan root. Without this,
///    a source bind-mounted at N places is walked N times → N× inflated size.
///
/// `scan_root` is the directory being scanned; only mounts whose mount point
/// is the root itself or a descendant of it are eligible for child-skip, so
/// unrelated bind mounts elsewhere on the host never affect this scan.
fn build_mount_skip_sets(
    scan_root: &str,
) -> (HashSet<(u64, u64)>, HashSet<String>, HashSet<String>) {
    let mut bind_set: HashSet<(u64, u64)> = HashSet::new();
    let mut kernfs_set: HashSet<String> = HashSet::new();
    let mut child_mount_skip: HashSet<String> = HashSet::new();

    let content = match std_fs::read_to_string("/proc/self/mountinfo") {
        Ok(s) => s,
        Err(_) => return (bind_set, kernfs_set, child_mount_skip),
    };

    let root_norm = scan_root.trim_end_matches('/');

    // Bind-duplicate child mounts (pure analysis of the mountinfo text).
    child_mount_skip = child_bind_dups_to_skip(&content, scan_root);

    let mut seen: std::collections::HashMap<(u64, u64), bool> = std::collections::HashMap::new();

    for line in content.lines() {
        // mountinfo: id pid major:minor root mount_point options...
        //            [0] [1]   [2]       [3]  [4]
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 5 { continue; }
        let mp = parts[4];
        let path = std::path::Path::new(mp);

        if is_kernfs_path(path) {
            kernfs_set.insert(mp.to_string());
            // A tmpfs/overlay/proc mount sitting *inside* the scan root is not
            // real disk usage of this filesystem — skip its whole subtree.
            if mount_is_under_root(mp, root_norm) && mp != root_norm {
                child_mount_skip.insert(mp.to_string());
            }
        }

        if let Ok(meta) = std_fs::metadata(mp) {
            let key = (meta.dev(), meta.ino());
            if seen.contains_key(&key) {
                bind_set.insert(key);
            } else {
                seen.insert(key, true);
            }
        }
    }

    (bind_set, kernfs_set, child_mount_skip)
}

#[cfg(test)]
mod mount_skip_tests {
    use super::*;

    // Minimal mountinfo lines: "id pid major:minor root mount_point opts fstype..."
    // Only fields 3 (major:minor), 4 (root), 5 (mount_point) matter here.
    const FIXTURE: &str = "\
21 20 0:20 / /projects/big_sumo_disk rw shared:1 - ext4 /dev/sdb rw
22 21 0:20 / /projects/big_sumo_disk/portal rw shared:1 - ext4 /dev/sdb rw
23 21 0:20 / /none rw shared:1 - ext4 /dev/sdb rw
24 21 0:99 / /projects/big_sumo_disk/cache rw - tmpfs tmpfs rw";

    #[test]
    fn skips_bind_duplicate_under_root_keeps_first() {
        let skip = child_bind_dups_to_skip(FIXTURE, "/projects/big_sumo_disk");
        // The bind duplicate of the same source under the root is skipped...
        assert!(skip.contains("/projects/big_sumo_disk/portal"));
        // ...the first/source mount (the root itself) is NOT skipped...
        assert!(!skip.contains("/projects/big_sumo_disk"));
        // ...and a duplicate mounted OUTSIDE the scan root is ignored.
        assert!(!skip.contains("/none"));
    }

    #[test]
    fn unrelated_root_skips_nothing() {
        let skip = child_bind_dups_to_skip(FIXTURE, "/data/other");
        assert!(skip.is_empty());
    }

    // Regression (Codex P1): if the FIRST mount of a source is OUTSIDE the scan
    // root, the in-root bind copy is the only reachable instance and must NOT be
    // skipped — otherwise the scan undercounts those bytes.
    const FIXTURE_ORIGIN_OUTSIDE: &str = "\
30 2 0:40 / /data/project rw shared:9 - ext4 /dev/sdc rw
31 2 0:40 / /scan/project rw shared:9 - ext4 /dev/sdc rw
32 2 0:40 / /scan rw shared:1 - ext4 /dev/sda rw";

    #[test]
    fn keeps_in_root_copy_when_origin_is_outside_root() {
        // Scanning /scan: source first seen at /data/project (outside /scan),
        // then bind-mounted at /scan/project (inside). /scan/project is the only
        // copy the walk reaches, so it must be KEPT (not skipped).
        let skip = child_bind_dups_to_skip(FIXTURE_ORIGIN_OUTSIDE, "/scan");
        assert!(
            !skip.contains("/scan/project"),
            "in-root bind copy wrongly skipped though its source is outside the scan"
        );
        assert!(skip.is_empty());
    }

    #[test]
    fn skips_second_in_root_copy_when_first_is_in_root() {
        // Two in-root copies of the same source: first /scan/a (counted),
        // second /scan/b (duplicate) -> only /scan/b is skipped.
        let mi = "\
40 2 0:50 / /scan/a rw shared:5 - ext4 /dev/sdd rw
41 2 0:50 / /scan/b rw shared:5 - ext4 /dev/sdd rw";
        let skip = child_bind_dups_to_skip(mi, "/scan");
        assert!(skip.contains("/scan/b"));
        assert!(!skip.contains("/scan/a"));
    }

    #[test]
    fn deep_source_and_deep_bind_both_in_root() {
        // The case the user asked about: the SOURCE lives deep inside the scan
        // (/scan/a/source) and is bind-mounted to another deep location inside
        // the same scan (/scan/b/mirror). mountinfo field 4 (fs_root) is the
        // in-filesystem sub-path, identical for source and its bind mount.
        // Expected: count the source once, skip the mirror. Net total correct.
        let mi = "\
50 2 0:60 /a/source /scan/a/source rw shared:7 - ext4 /dev/sde rw
51 2 0:60 /a/source /scan/b/mirror rw shared:7 - ext4 /dev/sde rw";
        let skip = child_bind_dups_to_skip(mi, "/scan");
        assert!(skip.contains("/scan/b/mirror"), "duplicate mirror must be skipped");
        assert!(!skip.contains("/scan/a/source"), "the source copy must be kept");
        assert_eq!(skip.len(), 1);
    }

    #[test]
    fn under_root_matches_only_path_boundary() {
        // "/projects/big" must NOT match "/projects/big_sumo_disk".
        assert!(!mount_is_under_root("/projects/big_sumo_disk", "/projects/big"));
        assert!(mount_is_under_root("/projects/big_sumo_disk/x", "/projects/big_sumo_disk"));
        assert!(mount_is_under_root("/projects/big_sumo_disk", "/projects/big_sumo_disk"));
        // Scanning "/" makes everything a descendant.
        assert!(mount_is_under_root("/anything", "/"));
    }

    #[test]
    fn hardlink_key_local_uses_ino_dev() {
        // On a local FS (use_statx = false) the key must be exactly (ino, dev),
        // unchanged from legacy behavior — no syscall, deterministic.
        let p = std::path::Path::new("/tmp/whatever");
        assert_eq!(hardlink_key(p, 42, 7, false), (42, 7));
        assert_eq!(hardlink_key(p, 0, 0, false), (0, 0));
    }
}


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
    // On NFS, st_dev is unstable, so hardlink dedup keyed on (ino, dev) leaks
    // and inflates total size. Detect NFS once here and switch dedup to a
    // statx-based key for the whole scan.
    let use_statx_dedup = is_nfs_path(&directory);
    if use_statx_dedup {
        eprintln!(
            "[SCAN] Scan root is on NFS — using statx(ino, mnt_id) for hardlink \
             dedup (st_dev is unstable on NFS)"
        );
    }
    let (bind_raw, kernfs_raw, child_mount_raw) = build_mount_skip_sets(&directory);
    if !bind_raw.is_empty() {
        eprintln!(
            "[SCAN] Detected {} bind mount destination(s) (will skip duplicates)",
            bind_raw.len()
        );
    }
    if !child_mount_raw.is_empty() {
        eprintln!(
            "[SCAN] Detected {} nested mount(s) under scan root (will skip to avoid double-counting):",
            child_mount_raw.len()
        );
        let mut sorted: Vec<&String> = child_mount_raw.iter().collect();
        sorted.sort();
        for mp in sorted {
            eprintln!("[SCAN]   skip nested mount: {}", mp);
        }
    }
    if !kernfs_raw.is_empty() {
        eprintln!(
            "[SCAN] Detected {} kernel/virtual FS mount(s) (will skip)",
            kernfs_raw.len()
        );
    }
    let bind_mount_set: Arc<HashSet<(u64, u64)>> = Arc::new(bind_raw);
    let kernfs_set: Arc<HashSet<String>> = Arc::new(kernfs_raw);
    let child_mount_set: Arc<HashSet<String>> = Arc::new(child_mount_raw);
    let bind_mount_clone = bind_mount_set.clone();
    let kernfs_clone = kernfs_set.clone();
    let child_mount_clone = child_mount_set.clone();

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
    // Track visited (dev, ino) pairs for dirs to detect bind mount loops at
    // runtime. Catches loops that mountinfo parsing misses (e.g. container
    // bind mounts not in host mountinfo, NFS re-exports). Memory: ~32 bytes
    // × N dirs = ~250MB at 7.7M-dir scale.
    let visited_dirs: Arc<DashSet<(u64, u64)>> = Arc::new(DashSet::new());
    let visited_dirs_profile = visited_dirs.clone();
    let visited_dirs_clone = visited_dirs.clone();

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
                let tid = thread_counter.fetch_add(1, Ordering::Relaxed);
                let mut state = ThreadLocalState {
                    t_files: 0,
                    t_dirs: 0,
                    t_inodes: 0,
                    t_size: 0,
                    t_uid_sizes: HashMap::with_capacity(256),
                    t_uid_files: HashMap::with_capacity(256),
                    t_dir_sizes: HashMap::with_capacity(50_000),
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
                let bind_mount = bind_mount_clone.clone();
                let kernfs = kernfs_clone.clone();
                let child_mount = child_mount_clone.clone();
                let visited_dirs = visited_dirs_clone.clone();

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

                    // --- Nested-mount skip (bind/tmpfs/overlay duplicates) ---
                    // A source tree bind-mounted at multiple points under the
                    // scan root would otherwise be walked once per mount point,
                    // inflating total size by N×. `child_mount` holds the
                    // duplicate mount points (computed from /proc/self/mountinfo)
                    // whose bytes are already counted via their first mount.
                    // Prune the whole subtree here, before any metadata work.
                    if !child_mount.is_empty() {
                        if let Some(path_str) = path.to_str() {
                            if child_mount.contains(path_str) {
                                return WalkState::Skip;
                            }
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
                            // Bind mount detection: skip dirs whose (dev, ino)
                            // matches a known bind mount destination.
                            let key = (meta.dev(), meta.ino());
                            if bind_mount.contains(&key) {
                                return WalkState::Skip;
                            }
                            // Skip kernel/pseudo filesystem mount points (ncdu exclude_kernfs)
                            if let Some(path_str) = path.to_str() {
                                if kernfs.contains(path_str) {
                                    return WalkState::Skip;
                                }
                            }
                            if let Some(rdev) = root_dev {
                                if meta.dev() != rdev {
                                    return WalkState::Skip;
                                }
                            }
                            // Runtime bind mount loop detection: skip if we've already visited
                            // this (dev, ino) pair via another path. Catches loops not in
                            // /proc/self/mountinfo.
                            if !visited_dirs.insert(key) {
                                return WalkState::Skip;
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
                        let meta = match entry.metadata() {
                            Ok(m) => {
                                if let Some(start) = meta_start {
                                    state.prof_metadata_ns.fetch_add(
                                        start.elapsed().as_nanos() as u64,
                                        Ordering::Relaxed,
                                    );
                                }
                                m
                            }
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

                        // --- Hard-link deduplication ---
                        if meta.nlink() > 1 {
                            if debug {
                                state.prof_hardlink_checks.fetch_add(1, Ordering::Relaxed);
                            }
                            // (ino, dev) on local FS; (stx_ino, stx_mnt_id) on
                            // NFS where st_dev is unstable. Counting the same
                            // inode twice is what inflated total size vs df.
                            let key = hardlink_key(path, meta.ino(), meta.dev(), use_statx_dedup);
                            if !hardlinks_shared.insert(key) {
                                return WalkState::Continue;
                            }
                        }

                        // st_blocks * 512 = actual on-disk bytes, same as Python legacy
                        let size = meta.blocks() * 512;
                        let uid = meta.uid();
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
        let visited_dirs_size = visited_dirs_profile.len();
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
        println!(
            "  Visited dirs set:   {}",
            format_num(visited_dirs_size as u64)
        );
    }

    Ok(result.into())
}
