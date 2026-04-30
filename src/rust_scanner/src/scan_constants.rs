pub(crate) const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot", ".snapshots", ".zfs",
    "proc", "sys", "dev",
    ".nfs",
];

pub(crate) const SCAN_EVENT_FLUSH_THRESHOLD: usize = 100_000;
pub(crate) const SCAN_EVENT_FLUSH_BYTES_THRESHOLD: usize = 16 * 1024 * 1024;

