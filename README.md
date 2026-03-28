# check_disk

A high-performance disk usage monitoring CLI for Linux servers.
Powered by a **native Rust core** (`fast_scanner.abi3.so`) that bypasses Python's GIL for true
parallel traversal, with automatic fallback to a pure-Python engine if the binary is unavailable.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Module Reference](#module-reference)
- [CLI Reference](#cli-reference)
- [Configuration Format](#configuration-format)
- [Report Formats](#report-formats)
- [Scripts](#scripts)
- [Performance & Memory Notes](#performance--memory-notes)

---

## Features

### Scanning Engine (Dual Core)

- **Rust High-Performance Core (`fast_scanner.abi3.so`)** — precompiled native binary.
  - Parallel `WalkBuilder` traversal — bypasses Python's GIL, true multi-thread I/O
  - Auto-scales threads: `max(16, cpu_count × 4)` — optimal for high-latency NFS/SAN
  - Progress reporting every 10 s: `Files | Dirs | Size | Rate | Mem`
  - Graceful `Ctrl+C` via `py.check_signals()`
- **Pure-Python Auto-Fallback** — work-stealing `scandir()` if `.so` fails to load
- **Single `stat()` per entry** — minimal syscall overhead
- **Hard-link deduplication** — `st_nlink == 1` fast-path; multi-link inodes use a shared `Arc<Mutex<HashSet<(ino, dev)>>>` across all worker threads
- **Snapshot/NFS detection** — skips directories whose `st_dev` differs from the root
- **Name-based skip** — `.snapshot`, `.snapshots`, `.zfs`, `proc`, `sys`, `dev` skipped before any `stat()`
- **Sparse file support** — `st_blocks × 512` for accurate on-disk size

### Phase 2: Report Generation (Also Rust-accelerated)

- **`fast_scanner.merge_write_user_report()`** — Rust K-way `BinaryHeap` merge of sorted TSV chunks → streams JSON directly to disk
  - No GIL, `BufReader`/`BufWriter`, zero Python object overhead
  - Replaces Python `heapq.merge()` — ~5–10× faster for millions of files per user
  - Falls back to Python automatically if Rust extension unavailable
- **Streaming flush** — file paths flushed to temp `.tsv` chunks at 100 K entries per thread
- **K-way merge** — final JSON assembled from sorted chunks; RAM = O(chunks), not O(total files)
- **Non-UTF-8 paths** — invalid bytes replaced with `U+FFFD`; output always valid UTF-8 JSON

### Other Features

- **Team and user tracking** — maps filesystem UIDs to config users and groups by team
- **Permission issue tracking** — records every inaccessible item per user
- **Per-user directory reports** — top directories sorted by disk usage (JSON)
- **Per-user file reports** — all files sorted by size (streaming JSON, safe for 75M+ file trees)
- **Multi-report comparison** — compare N reports side-by-side with growth/usage ranking
- **Plain-text export** — `scripts/export_user_reports.py`

---

## Requirements

- **Python 3.6+**

> The precompiled `fast_scanner.abi3.so` is included in the repository.
> It is built for **Linux x86_64, glibc ≥ 2.17** (RHEL 7/8, CentOS 7/8, Ubuntu 18+).
> No Rust toolchain or compilation step required on the target server.

---

## Quick Start

```bash
# 1. Initialize config
python disk_checker.py --init --dir /data/shared

# 2. Define teams
python disk_checker.py --add-team backend
python disk_checker.py --add-team frontend

# 3. Add users to teams
python disk_checker.py --add-user alice bob carol --team backend
python disk_checker.py --add-user dave eve --team frontend

# 4. Run a scan
python disk_checker.py --run

# 5. View the report
python disk_checker.py --show-report --files disk_usage_report.json

# 6. Check per-user detail
python disk_checker.py --check-users alice bob

# 7. Export plain-text
python scripts/export_user_reports.py --input-dir /reports/ --output-dir /reports/txt/ --workers 8
```

---

## Architecture

### Data Flow

```
disk_checker_config.json
         |
         v
  ConfigManager           <- reads/writes JSON config
         |
         v
  DiskScanner.scan()      <- proxy: Rust core → Python fallback
   |-- fast_scanner.scan_disk()        [RUST] parallel WalkBuilder
   |    |-- N worker threads (native)  <- stat, dedup, TSV flush
   |    |-- AtomicU64 progress         <- lock-free Files/Dirs/Size counters
   |    `-- py.check_signals()         <- Ctrl+C support
   |
   `-- LegacyDiskScanner (fallback)    [PYTHON] os.scandir work-stealing
         |
         v
  ReportGenerator         <- ScanResult -> JSON files
   |-- disk_usage_report.json
   |-- permission_issues.json
   |-- detail_users/
   |    |-- detail_report_dir_{user}.json   (in-memory, bounded by dir count)
   |    `-- detail_report_file_{user}.json  (K-way merge from TSV chunks)
   |         fast_scanner.merge_write_user_report()  [RUST preferred]
   |         heapq.merge()                          [Python fallback]
   `-- [tmp dir auto-cleaned on exit]
```

### Source Layout

```
disk_checker.py          <- CLI entry point
disk_checker_config.json <- configuration (created by --init)
fast_scanner.abi3.so     <- precompiled Rust extension (Linux x86_64, glibc ≥ 2.17)

src/
  cli_interface.py       <- argparse, argument routing
  config_manager.py      <- JSON config CRUD
  disk_scanner.py        <- DiskScanner proxy + LegacyDiskScanner
  report_generator.py    <- JSON report writer (Rust Phase 2 + Python fallback)
  utils.py               <- shared helpers, ScanHelper

  formatters/
    base_formatter.py    <- terminal size, usage bar
    table_formatter.py   <- ASCII grid tables
    config_display.py    <- --list output
    report_formatter.py  <- --show-report and --check-users output
    report_comparison.py <- multi-report diff table

scripts/
  export_user_reports.py <- standalone: JSON detail reports -> plain-text
  backfill_team_ids.py   <- standalone: inject team_id into existing reports
```

---

## Module Reference

### `fast_scanner.abi3.so` — Rust Native Extension

Precompiled PyO3 module. Compatible with Python 3.8+ (abi3). Exposes two functions:

#### `fast_scanner.scan_disk(directory, skip_dirs, target_uids) -> dict`

Scans `directory` in parallel using `ignore::WalkBuilder`.
- `target_uids`: Optional list of Linux UIDs to specifically track. Drastically improves RAM and I/O performance by only recording buffers for these users, while still calculating accurate total disk usage metrics globally.

Returns:

```python
{
  "total_files":     int,
  "total_dirs":      int,
  "total_size":      int,          # st_blocks * 512 (on-disk, sparse-aware)
  "uid_sizes":       {uid: bytes}, # per-UID total bytes
  "dir_sizes":       {path: {uid: bytes}},  # direct-child file sizes per dir
  "detail_tmpdir":   str,          # path to temp dir with uid_*_t*.tsv chunks
  "permission_issues": [(path, kind, error), ...]
}
```

**Progress output** (every 10 s to stdout):
```
[HH:MM:SS] Files: 8,234,571 | Dirs: 1,182,344 | Size: 45.23 GB | Rate: 823,457.1 files/s | Mem: 234.5 MB
```

#### `fast_scanner.merge_write_user_report(tmpdir, uid, username, output_path, timestamp) -> (total_files, total_used)`

K-way `BinaryHeap` merge of all `uid_{uid}_t*.tsv` chunks → streams a sorted-by-size JSON file report.

- 2-pass: Pass 1 counts totals, Pass 2 writes JSON (matches Python `_stream_write_file_report`)
- Invalid UTF-8 bytes → `U+FFFD` replacement
- Creates output directory if needed
- Returns `(total_files, total_used)` as Python tuple

---

### `src/disk_scanner.py` — DiskScanner Proxy

Wraps both engines behind a single `scan()` interface.

**Rust path** (default when `.so` loads):
1. Calls `fast_scanner.scan_disk()`
2. Maps UID → username
3. Assembles `ScanResult` with `detail_tmpdir` set

**Python fallback** (`LegacyDiskScanner`):
- Work-stealing deque, N threads, `os.scandir()`
- Same `ThreadStats` → `ScanResult` interface

**Key parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_workers` | `min(cpu*2, 64)` | Worker thread count (Python fallback only) |
| `use_rust` | `true` | Set `false` in config to force Python fallback |
| `DETAIL_FLUSH_THRESHOLD` | `100_000` | Per-thread file-path buffer flush threshold |

**ScanResult fields:**

```python
@dataclass
class ScanResult:
    general_system:      Dict[str, int]   # total, used, available bytes
    team_usage:          List[Dict]       # [{name, used}]
    user_usage:          List[Dict]       # [{name, used}] — users in config
    other_usage:         List[Dict]       # [{name, used}] — UIDs not in config
    timestamp:           int              # Unix epoch
    top_dir:             List[Dict]       # [{dir, user, user_usage}]
    permission_issues:   Dict             # {total: N, items: [{user, path, type, error}]}
    detail_files:        Dict[str, List]  # username -> [(path, size)] (in-memory mode)
    detail_tmpdir:       Optional[str]    # tmp dir with .tsv chunks (streaming mode)
    detail_uid_username: Dict[int, str]   # uid -> username
```

---

### `src/report_generator.py` — ReportGenerator

Converts a `ScanResult` to JSON files.

| Method | Output | When |
|--------|--------|------|
| `generate_report(scan_result)` | `disk_usage_report.json` | Always with `--run` |
| `generate_detail_reports(scan_result, max_workers)` | `detail_report_dir_{user}.json` + `detail_report_file_{user}.json` | Always with `--run` |

**File report streaming:**

1. **Rust path** — calls `fast_scanner.merge_write_user_report()` (preferred)
2. **Python fallback** — 2-pass `heapq.merge()` over sorted TSV chunks

Peak RAM for file reports = O(number of open chunks), safe for 75M+ file trees.

---

### `src/cli_interface.py`, `src/utils.py`, `src/formatters/`

Unchanged from previous version — see [Module Reference](#module-reference).

---

## CLI Reference

### Configuration

```bash
python disk_checker.py --init --dir /data/shared
python disk_checker.py --add-team backend
python disk_checker.py --add-user alice bob carol --team backend
python disk_checker.py --remove-user alice
python disk_checker.py --list
```

### Scanning

```bash
# Basic scan
python disk_checker.py --run

# Custom output
python disk_checker.py --run --output-dir /reports/ --prefix sda1 --date

# Targeted scan (Drastically accelerates Phase 2 by isolating specific users)
python disk_checker.py --run --user root alice bob --output-dir /reports/ --date

# Force Python fallback (no Rust)
# set "use_rust": false in disk_checker_config.json

# Parallel workers (Python fallback only)
python disk_checker.py --run --workers 32
```

### Report Viewing

```bash
python disk_checker.py --show-report --files disk_usage_report.json
python disk_checker.py --show-report --files report1.json report2.json --compare-by growth
python disk_checker.py --check-users alice bob
python disk_checker.py --check-users alice --output-dir /reports/ --prefix sda1
```

---

## Configuration Format

`disk_checker_config.json`:

```json
{
  "directory": "/data/shared",
  "output_file": "disk_usage_report.json",
  "use_rust": true,
  "teams": [
    { "name": "backend",  "team_id": 1 },
    { "name": "frontend", "team_id": 2 }
  ],
  "users": [
    { "name": "alice", "team_id": 1 },
    { "name": "bob",   "team_id": 1 },
    { "name": "carol", "team_id": 2 }
  ]
}
```

---

## Report Formats

### `disk_usage_report.json`

```json
{
  "date": 1742600000,
  "directory": "/data/shared",
  "general_system": { "total": 10995116277760, "used": 8246337208320, "available": 2748779069440 },
  "team_usage":  [{ "name": "backend", "used": 3298534883328, "team_id": 1 }],
  "user_usage":  [{ "name": "alice",   "used": 2748779069440, "team_id": 1 }],
  "other_usage": [{ "name": "uid-1234","used": 549755813888  }]
}
```

### `detail_report_dir_{user}.json`

```json
{
  "date": 1742600000, "directory": "/data/shared", "user": "alice",
  "total_used": 2748779069440,
  "dirs": [
    { "path": "/data/shared/alice/models",   "used": 1099511627776 },
    { "path": "/data/shared/alice/datasets", "used":  549755813888 }
  ]
}
```

### `detail_report_file_{user}.json`

```json
{
  "date": 1742600000, "user": "alice",
  "total_files": 45231, "total_used": 2748779069440,
  "files": [
    { "path": "/data/shared/alice/models/weights.bin", "size": 536870912 },
    { "path": "/data/shared/alice/datasets/train.tar", "size": 268435456 }
  ]
}
```

Files sorted by `size` descending. Large arrays written via Rust streaming merge — safe to stream via paginated API.

### `permission_issues.json`

```json
{
  "date": 1742600000, "directory": "/data/shared",
  "general_system": { "total": 10995116277760, "used": 8246337208320, "available": 2748779069440 },
  "permission_issues": {
    "total": 2,
    "items": [
      { "user": "alice", "path": "/data/shared/alice/private", "type": "directory", "error": "Permission denied" }
    ]
  }
}
```

---

## Scripts

### `scripts/export_user_reports.py`

Converts JSON detail reports to sorted `.txt` files.

```bash
python scripts/export_user_reports.py --input-dir /reports/ --workers 8
python scripts/export_user_reports.py --input-dir /reports/ --output-dir /exports/txt/ --prefix sda1
python scripts/export_user_reports.py --input-dir /reports/ --users alice bob
```

### `scripts/backfill_team_ids.py`

Injects `team_id` into existing reports (idempotent).

```bash
python scripts/backfill_team_ids.py --report disk_usage_report.json
python scripts/backfill_team_ids.py --reports "reports/disk_usage_report_*.json"
```

### `scripts/migrate_permission_issues.py`

Converts old nested permission format to flat format (idempotent).

```bash
python scripts/migrate_permission_issues.py --reports "/reports/**/permission_issues*.json" --dry-run
python scripts/migrate_permission_issues.py --reports "/reports/**/permission_issues*.json"
```

---

## Performance & Memory Notes

### Throughput guide

| Scenario | Engine | Expected time |
|----------|--------|---------------|
| NVMe local, 10M files | Rust | < 5 min |
| SSD local, 50M files | Rust | 5–15 min |
| NFS/SAN high-latency, 75M files | Rust (overprovisions threads) | 15–40 min |
| HDD single disk | Rust (I/O bound) | 30–90 min |
| Python fallback, 75M files | Python | 60–180 min |

### Memory consumption

The Rust engine is significantly more memory-efficient than the Python legacy:

| Component | Rust | Python legacy |
|-----------|------|---------------|
| `dir_sizes` (6.5M dirs) | ~0.8 GB (HashMap + String) | ~2.5 GB (Python dicts) |
| File-path buffers (100 K × threads) | ~0.4 GB | ~1.3 GB |
| Runtime overhead | ~0.1 GB | ~0.5–1 GB |
| **Total peak** | **~1.5 GB** | **~5 GB** |

> `DETAIL_FLUSH_THRESHOLD` (default `100_000`) controls trade-off between flush frequency and RAM.
> Increase to `200_000` only if you have 8+ GB free RAM.

### Phase 2 performance (file report generation)

| Files per user | Python `heapq.merge` | Rust `BinaryHeap` merge |
|----------------|----------------------|-------------------------|
| 500 K | ~15–30 s | ~3–5 s |
| 5 M   | ~3–5 min | ~20–40 s |
| 20 M  | ~15–20 min | ~2–4 min |