pub(crate) const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot", ".snapshots", ".zfs",
    "proc", "sys", "dev",
    ".nfs",
];

pub(crate) const SCAN_EVENT_FLUSH_THRESHOLD: usize = 500_000;
pub(crate) const SCAN_EVENT_FLUSH_BYTES_THRESHOLD: usize = 64 * 1024 * 1024;
pub(crate) const SCAN_DIR_AGG_FLUSH_DIRS_THRESHOLD: usize = 20_000;
