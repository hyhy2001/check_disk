use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use dashmap::DashSet;
use ignore::{WalkBuilder, WalkState};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::exceptions::PyRuntimeError;
use std::os::unix::fs::MetadataExt;

// 1. Binary Protocol Định nghĩa:
// EVENT_FILE = 0x01
// EVENT_PERM = 0x02
// [u8 type][u32 uid][u64 size/errcode][u16 path_len][bytes path]

// Struct for Phase 1 V2
pub struct V2Stats {
    total_files: AtomicU64,
    total_dirs: AtomicU64,
    total_size: AtomicU64,
    perm_count: AtomicU64,
}

impl V2Stats {
    pub fn new() -> Self {
        Self {
            total_files: AtomicU64::new(0),
            total_dirs: AtomicU64::new(0),
            total_size: AtomicU64::new(0),
            perm_count: AtomicU64::new(0),
        }
    }
}

// TODO: Tiếp tục...
