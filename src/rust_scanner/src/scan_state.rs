use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub(crate) struct GlobalStats {
    pub(crate) total_files: u64,
    pub(crate) total_dirs: u64,
    pub(crate) total_inodes: u64,
    pub(crate) total_size: u64,
    pub(crate) uid_sizes: HashMap<u32, u64>,
    pub(crate) uid_files: HashMap<u32, u64>,
    pub(crate) permission_issues_count: u64,
}

pub(crate) struct ThreadLocalState {
    pub(crate) t_files: u64,
    pub(crate) t_dirs: u64,
    pub(crate) t_inodes: u64,
    pub(crate) t_size: u64,
    pub(crate) t_uid_sizes: HashMap<u32, u64>,
    pub(crate) t_uid_files: HashMap<u32, u64>,
    pub(crate) t_event_bin_buf: Vec<u8>,
    pub(crate) t_event_buf_records: usize,
    pub(crate) t_event_flush_count: u32,
    pub(crate) t_perm_issues: u64,
    pub(crate) global_stats: Arc<Mutex<GlobalStats>>,
    pub(crate) prog_files: Arc<AtomicU64>,
    pub(crate) prog_dirs: Arc<AtomicU64>,
    pub(crate) prog_size: Arc<AtomicU64>,
    pub(crate) tmpdir: String,
    pub(crate) target_uids: Option<HashSet<u32>>,
    pub(crate) thread_id: usize,
    pub(crate) prof_metadata_ns: Arc<AtomicU64>,
    pub(crate) prof_path_ns: Arc<AtomicU64>,
    pub(crate) prof_flush_ns: Arc<AtomicU64>,
    pub(crate) prof_flush_bytes: Arc<AtomicU64>,
    pub(crate) prof_flush_count: Arc<AtomicU64>,
    pub(crate) prof_hardlink_checks: Arc<AtomicU64>,
    pub(crate) prof_visited_dir_checks: Arc<AtomicU64>,
    pub(crate) prof_max_event_buf_records: Arc<AtomicU64>,
    pub(crate) prof_max_event_buf_bytes: Arc<AtomicU64>,
}

impl ThreadLocalState {
    pub(crate) fn push_event_binary(&mut self, uid: u32, size: u64, path: &str) {
        // Record format:
        // [tag:u8=1][uid:u32 LE][size:u64 LE][path_len:u32 LE][path_bytes]
        self.t_event_bin_buf.push(1u8);
        self.t_event_bin_buf.extend_from_slice(&uid.to_le_bytes());
        self.t_event_bin_buf.extend_from_slice(&size.to_le_bytes());
        let path_bytes = path.as_bytes();
        let len = u32::try_from(path_bytes.len()).unwrap_or(u32::MAX);
        self.t_event_bin_buf.extend_from_slice(&len.to_le_bytes());
        let safe_len = usize::try_from(len).unwrap_or(path_bytes.len());
        self.t_event_bin_buf.extend_from_slice(&path_bytes[..safe_len.min(path_bytes.len())]);
    }

    pub(crate) fn flush_events(&mut self) {
        if self.t_event_bin_buf.is_empty() {
            return;
        }
        self.t_event_flush_count += 1;
        self.prof_flush_count.fetch_add(1, Ordering::Relaxed);
        let flush_start = Instant::now();
        let bytes_written = self.t_event_bin_buf.len() as u64;

        if !self.t_event_bin_buf.is_empty() {
            let fp = format!("{}/scan_t{}_c{}.bin", self.tmpdir, self.thread_id, self.t_event_flush_count);
            if let Ok(f) = fs::File::create(&fp) {
                let mut w = BufWriter::new(f);
                let _ = w.write_all(&self.t_event_bin_buf);
            }
        }

        self.prof_flush_ns.fetch_add(flush_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.prof_flush_bytes.fetch_add(bytes_written, Ordering::Relaxed);
        self.t_event_bin_buf.clear();
        self.t_event_buf_records = 0;
        if self.t_event_bin_buf.capacity() > 64 * 1024 * 1024 {
            self.t_event_bin_buf.shrink_to(8 * 1024 * 1024);
        }
    }

    pub(crate) fn flush_permission_issue(&self, path: &str, kind: &str, error_code: &str) {
        let fp = format!("{}/perm_t{}.tsv", self.tmpdir, self.thread_id);
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&fp) {
            let uid = if path.is_empty() {
                0
            } else {
                fs::symlink_metadata(path).map(|m| m.uid()).unwrap_or(0)
            };
            let _ = writeln!(f, "P\t{}\t{}\t{}\t{}", uid, kind, error_code, path);
        }
    }
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        self.flush_events();

        if let Ok(mut g) = self.global_stats.lock() {
            g.total_files += self.t_files;
            g.total_dirs += self.t_dirs;
            g.total_inodes += self.t_inodes;
            g.total_size += self.t_size;
            for (uid, size) in &self.t_uid_sizes {
                *g.uid_sizes.entry(*uid).or_insert(0) += size;
            }
            for (uid, files) in &self.t_uid_files {
                *g.uid_files.entry(*uid).or_insert(0) += files;
            }
            g.permission_issues_count += self.t_perm_issues;
        }
    }
}
