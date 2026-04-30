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
    pub(crate) t_dir_sizes: HashMap<(u32, String), i64>,
    pub(crate) t_event_bin_buf: Vec<u8>,
    pub(crate) t_event_buf_records: usize,
    pub(crate) t_event_flush_count: u32,
    pub(crate) event_bin_writer: Option<BufWriter<fs::File>>,
    pub(crate) t_perm_issues: u64,
    pub(crate) global_stats: Arc<Mutex<GlobalStats>>,
    pub(crate) prog_files: Arc<AtomicU64>,
    pub(crate) prog_dirs: Arc<AtomicU64>,
    pub(crate) prog_size: Arc<AtomicU64>,
    pub(crate) pending_prog_files: u64,
    pub(crate) pending_prog_dirs: u64,
    pub(crate) pending_prog_size: u64,
    pub(crate) tmpdir: String,
    pub(crate) target_uids: Option<HashSet<u32>>,
    pub(crate) thread_id: usize,
    pub(crate) profile_enabled: bool,
    pub(crate) prof_metadata_ns: Arc<AtomicU64>,
    pub(crate) prof_path_ns: Arc<AtomicU64>,
    pub(crate) prof_flush_ns: Arc<AtomicU64>,
    pub(crate) prof_flush_bytes: Arc<AtomicU64>,
    pub(crate) prof_flush_count: Arc<AtomicU64>,
    pub(crate) prof_hardlink_checks: Arc<AtomicU64>,
    pub(crate) prof_visited_dir_checks: Arc<AtomicU64>,
    pub(crate) prof_max_event_buf_records: Arc<AtomicU64>,
    pub(crate) prof_max_event_buf_bytes: Arc<AtomicU64>,
    pub(crate) perm_writer: Option<BufWriter<fs::File>>,
    pub(crate) dir_agg_writer: Option<BufWriter<fs::File>>,
}

impl ThreadLocalState {
    const PROGRESS_FLUSH_THRESHOLD: u64 = 4096;

    pub(crate) fn add_progress(&mut self, files: u64, dirs: u64, size: u64) {
        self.pending_prog_files += files;
        self.pending_prog_dirs += dirs;
        self.pending_prog_size += size;

        if self.pending_prog_files + self.pending_prog_dirs >= Self::PROGRESS_FLUSH_THRESHOLD {
            self.flush_progress();
        }
    }

    pub(crate) fn flush_progress(&mut self) {
        if self.pending_prog_files != 0 {
            self.prog_files
                .fetch_add(self.pending_prog_files, Ordering::Relaxed);
            self.pending_prog_files = 0;
        }
        if self.pending_prog_dirs != 0 {
            self.prog_dirs
                .fetch_add(self.pending_prog_dirs, Ordering::Relaxed);
            self.pending_prog_dirs = 0;
        }
        if self.pending_prog_size != 0 {
            self.prog_size
                .fetch_add(self.pending_prog_size, Ordering::Relaxed);
            self.pending_prog_size = 0;
        }
    }

    pub(crate) fn push_event_binary(&mut self, tag: u8, uid: u32, size: u64, path: &str) {
        // Record format:
        // [tag:u8][uid:u32 LE][size:u64 LE][path_len:u32 LE][path_bytes]
        self.t_event_bin_buf.push(tag);
        self.t_event_bin_buf.extend_from_slice(&uid.to_le_bytes());
        self.t_event_bin_buf.extend_from_slice(&size.to_le_bytes());
        let path_bytes = path.as_bytes();
        let len = u32::try_from(path_bytes.len()).unwrap_or(u32::MAX);
        self.t_event_bin_buf.extend_from_slice(&len.to_le_bytes());
        let safe_len = usize::try_from(len).unwrap_or(path_bytes.len());
        self.t_event_bin_buf
            .extend_from_slice(&path_bytes[..safe_len.min(path_bytes.len())]);
    }

    pub(crate) fn add_dir_size(&mut self, uid: u32, size: u64, path: &str) {
        let Some(parent) = parent_path_str(path) else {
            return;
        };
        *self
            .t_dir_sizes
            .entry((uid, parent.to_string()))
            .or_insert(0) += size as i64;
    }

    pub(crate) fn flush_events(&mut self) {
        if self.t_event_bin_buf.is_empty() {
            return;
        }
        self.t_event_flush_count += 1;
        if self.profile_enabled {
            self.prof_flush_count.fetch_add(1, Ordering::Relaxed);
        }
        let flush_start = self.profile_enabled.then(Instant::now);
        let bytes_written = self.t_event_bin_buf.len() as u64;

        if !self.t_event_bin_buf.is_empty() {
            if self.event_bin_writer.is_none() {
                let fp = format!("{}/scan_t{}.bin", self.tmpdir, self.thread_id);
                if let Ok(f) = fs::OpenOptions::new().create(true).append(true).open(&fp) {
                    self.event_bin_writer = Some(BufWriter::with_capacity(16 * 1024 * 1024, f));
                }
            }
            if let Some(writer) = self.event_bin_writer.as_mut() {
                let _ = writer.write_all(&self.t_event_bin_buf);
            }
        }

        if let Some(start) = flush_start {
            self.prof_flush_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.prof_flush_bytes
                .fetch_add(bytes_written, Ordering::Relaxed);
        }
        self.t_event_bin_buf.clear();
        self.t_event_buf_records = 0;
        if self.t_event_bin_buf.capacity() > 128 * 1024 * 1024 {
            self.t_event_bin_buf.shrink_to(64 * 1024 * 1024);
        }
    }

    pub(crate) fn flush_permission_issue(&mut self, path: &str, kind: &str, error_code: &str) {
        if self.perm_writer.is_none() {
            let fp = format!("{}/perm_t{}.tsv", self.tmpdir, self.thread_id);
            if let Ok(f) = fs::OpenOptions::new().create(true).append(true).open(&fp) {
                self.perm_writer = Some(BufWriter::with_capacity(1024 * 1024, f));
            }
        }

        if let Some(writer) = self.perm_writer.as_mut() {
            let uid = if path.is_empty() {
                0
            } else {
                fs::symlink_metadata(path).map(|m| m.uid()).unwrap_or(0)
            };
            let _ = writeln!(writer, "P\t{}\t{}\t{}\t{}", uid, kind, error_code, path);
        }
    }

    pub(crate) fn flush_dir_aggregates(&mut self) {
        if self.t_dir_sizes.is_empty() {
            return;
        }
        if self.dir_agg_writer.is_none() {
            let fp = format!("{}/diragg_t{}.bin", self.tmpdir, self.thread_id);
            if let Ok(f) = fs::OpenOptions::new().create(true).append(true).open(&fp) {
                self.dir_agg_writer = Some(BufWriter::with_capacity(8 * 1024 * 1024, f));
            }
        }
        if let Some(writer) = self.dir_agg_writer.as_mut() {
            for ((uid, path), size) in self.t_dir_sizes.drain() {
                let path_bytes = path.as_bytes();
                let len = u32::try_from(path_bytes.len()).unwrap_or(u32::MAX);
                let safe_len = usize::try_from(len).unwrap_or(path_bytes.len());
                let _ = writer.write_all(&uid.to_le_bytes());
                let _ = writer.write_all(&size.to_le_bytes());
                let _ = writer.write_all(&len.to_le_bytes());
                let _ = writer.write_all(&path_bytes[..safe_len.min(path_bytes.len())]);
            }
        }
    }
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        self.flush_progress();
        self.flush_events();
        self.flush_dir_aggregates();
        if let Some(writer) = self.event_bin_writer.as_mut() {
            let _ = writer.flush();
        }
        if let Some(writer) = self.perm_writer.as_mut() {
            let _ = writer.flush();
        }
        if let Some(writer) = self.dir_agg_writer.as_mut() {
            let _ = writer.flush();
        }

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

fn parent_path_str(path: &str) -> Option<&str> {
    let trimmed = path.trim_end_matches('/');
    if trimmed == "/" || trimmed.is_empty() {
        return None;
    }
    match trimmed.rfind('/') {
        Some(0) => Some("/"),
        Some(idx) => Some(&trimmed[..idx]),
        None => None,
    }
}
