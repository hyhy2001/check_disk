pub(crate) const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot", ".snapshots", ".zfs",
    "proc", "sys", "dev",
    ".nfs",
];

pub(crate) const SCAN_EVENT_FLUSH_THRESHOLD: usize = 250_000;
pub(crate) const SCAN_EVENT_FLUSH_BYTES_THRESHOLD: usize = 32 * 1024 * 1024;
