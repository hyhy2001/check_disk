pub(crate) const CRITICAL_SKIP_NAMES: &[&str] = &[
    ".snapshot",
    ".snapshots",
    ".zfs",
    "proc",
    "sys",
    "dev",
    ".nfs",
];

pub(crate) const SCAN_EVENT_FLUSH_THRESHOLD: usize = 250_000;
pub(crate) const SCAN_EVENT_FLUSH_BYTES_THRESHOLD: usize = 32 * 1024 * 1024;

pub(crate) const BIN_MAGIC_LEN: usize = 8;
pub(crate) const SCAN_EVENT_BIN_MAGIC_V1: [u8; BIN_MAGIC_LEN] = *b"CDSKSEV1";
pub(crate) const DIR_AGG_BIN_MAGIC_V1: [u8; BIN_MAGIC_LEN] = *b"CDSKDAV1";
