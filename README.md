# check_disk

A high-performance Python CLI tool for monitoring disk usage across teams and users on Linux servers.
Uses parallel `scandir()` with work-stealing threads, streaming memory-efficient reporting, and
produces structured JSON output consumed by the companion **disk_usage** web dashboard.

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
- **Rust High-Performance Core (`fast_scanner.so`)** — native parallel tree traversal via `jwalk` bypassing Python's GIL. Can scan 10M+ files in seconds. Active by default if compiled (can be disabled via `"use_rust": false` in config).
- **Pure-Python Auto-Fallback** — graceful fallback to work-stealing multi-thread `scandir()` if the Rust extension is not installed.
- **Single `stat()` per entry** — avoids chained checks by directly accessing metadata.
- **Hard-link deduplication** — `st_nlink == 1` fast-path avoids lock contention; only multi-linked inodes acquire a lock
- **Snapshot/NFS detection** — skips directories on a different device ID (covers ZFS snapshots, Btrfs subvols, NFS, bind-mounts)
- **Name-based skip** — `.snapshot`, `.snapshots`, `.zfs`, `proc`, `sys`, `dev`, `run`, and similar pseudo-filesystems are skipped before any `stat()` call
- **Event-driven idle** — idle threads sleep on `threading.Event` instead of busy-waiting; woken immediately when new dirs are pushed
- **Sparse file support** — uses `st_blocks * 512` for accurate on-disk size (not logical file size)
- **Stall detection** — automatically aborts if the scan makes no progress for 5 minutes
- **Memory monitoring** — terminates gracefully if RSS exceeds the configured limit

### Reporting
- **Team and user tracking** — maps filesystem UIDs to configured users and groups them by team
- **Permission issue tracking** — records every inaccessible file/directory per user
- **Per-user directory reports** — top directories sorted by disk usage per user (JSON)
- **Per-user file reports** — all files sorted by size with streaming writes (JSON)
- **Multi-report comparison** — compare N reports side-by-side with growth/usage ranking
- **Plain-text export** — `scripts/export_user_reports.py` converts JSON detail reports to sorted `.txt` files
- **Team ID backfill** — `scripts/backfill_team_ids.py` patches old reports without `team_id`

### Memory Efficiency (Large Disks)
- **Streaming flush** — file paths are flushed to temporary `.tsv` files in sorted chunks once per-thread buffer reaches `DETAIL_FLUSH_THRESHOLD` (default 100,000 entries), keeping RAM usage bounded
- **K-way merge** — final detail report is assembled via a min-heap merge of sorted chunks; RAM is O(chunks), not O(total_files)
- **Non-UTF-8 paths** — surrogate-escape encoding prevents crashes on exotic Linux filenames; replaced with `U+FFFD` (replacement character) in JSON output

---

## Requirements

- Python 3.6+
- [`psutil`](https://pypi.org/project/psutil/) — memory monitoring
- (Optional) **Rust toolchain & `maturin`** — required to compile the high-performance underlying scanner.

```bash
pip install psutil
```

**Compiling the High-Performance Rust extension:**
```bash
# 1. Ensure Rust is installed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# 2. Build and install the PyO3 module
pip install maturin
cd src/rust_scanner
maturin build --release
pip install target/wheels/fast_scanner*.whl --force-reinstall
```

---

## Quick Start

```bash
# 1. Initialize config with the directory to scan
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

# 6. Check per-user detail reports in the terminal
python disk_checker.py --check-users alice bob

# 7. Export plain-text usage files (one per user, parallel 8 workers)
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
  DiskScanner.scan()      <- multi-thread scandir()
   |-- _worker() x N      <- per-thread: stat(), mode dispatch, dedup
   |    |-- ThreadStats   <- uid_sizes, dir_sizes, file_paths (streamed to tmp)
   |    `-- inode lock    <- acquired only for hard-linked files (st_nlink > 1)
   |-- _flush_thread_paths()  <- sort + write chunks to tmp .tsv files
   `-- _stream_merge_to_results()  <- collect UID→username mapping
         |
         v
  ReportGenerator         <- converts ScanResult -> JSON files
   |-- disk_usage_report.json             (always)
   |-- permission_issues.json             (if issues found)
   |-- detail_users/
   |    |-- detail_report_dir_{user}.json  (per user, always)
   |    `-- detail_report_file_{user}.json (per user, k-way merge from tmp tsv)
   `-- [tmp dir auto-cleaned on exit]
```

### Source Layout

```
disk_checker.py          <- CLI entry point
disk_checker_config.json <- configuration (created by --init)

src/
  cli_interface.py       <- argparse, argument routing
  config_manager.py      <- JSON config CRUD
  disk_scanner.py        <- DiskScanner (parallel core + streaming flush)
  report_generator.py    <- JSON report writer (streaming for file reports)
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

### `src/disk_scanner.py` — DiskScanner

Core parallel scanner using Python threads and `os.scandir()`.

**Key parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_workers` | `min(cpu*2, 64)` | Worker thread count |
| `debug` | `False` | Print per-directory and skip events |
| `DETAIL_FLUSH_THRESHOLD` | `100_000` | Flush per-thread file-path buffer to disk at this size |

**Threading model:**

- Each thread owns a local `list` queue; no contention during normal operation
- When empty, threads batch-steal from a shared `deque` (atomic under GIL)
- Idle threads sleep on `threading.Event`; woken immediately when new dirs are pushed
- A dedicated progress thread prints stats every 10 s and detects stalls

**Entry dispatch per `scandir` entry (single `stat()` call):**

```
entry.stat(follow_symlinks=False)         <- one syscall
  S_ISLNK(mode) -> skip
  S_ISDIR(mode) -> name check -> device ID check -> enqueue
  S_ISREG(mode) -> st_nlink check -> count size -> buffer path
```

**Hard-link deduplication:**

- `st_nlink == 1`: no lock acquired, file counted immediately (fast path, ~99% of files)
- `st_nlink > 1`: `(st_ino, st_dev)` checked under a single `Lock` (rare path)

**Streaming file-path flush:**

Each worker accumulates `(size, path)` tuples in `file_paths` dict keyed by UID. When
total accumulated entries exceed `DETAIL_FLUSH_THRESHOLD`, the buffer is:
1. Sorted descending by size per UID
2. Written as a TSV chunk to a per-thread temporary file

After scanning completes, `ReportGenerator` performs a k-way merge of all chunks per UID
to produce the final `detail_report_file_{user}.json` without ever loading all paths into RAM.

**ScanResult fields:**

```python
@dataclass
class ScanResult:
    general_system:    Dict[str, int]   # total, used, available bytes
    team_usage:        List[Dict]       # [{name, used}]
    user_usage:        List[Dict]       # [{name, used}] -- users in config
    other_usage:       List[Dict]       # [{name, used}] -- UIDs not in config
    timestamp:         int              # Unix epoch
    top_dir:           List[Dict]       # [{dir, user, user_usage}]
    permission_issues: Dict             # flat: {total: N, items: [{user, path, type, error}]}
    detail_files:      Dict[str, List]  # username -> [(path, size), ...]
    detail_tmpdir:     Optional[str]    # path to tmp dir with .tsv chunks (streaming mode)
    detail_uid_username: Dict[int, str] # uid -> username mapping
```

---

### `src/report_generator.py` — ReportGenerator

Converts a `ScanResult` to JSON files. All sibling reports share the same `prefix` and directory.

| Method | Output file | When |
|--------|-------------|------|
| `generate_report(scan_result)` | `disk_usage_report.json` | Always with `--run` |
| `generate_detail_reports(scan_result, max_workers=1)` | `detail_report_dir_{user}.json` + `detail_report_file_{user}.json` | Always with `--run`; uses `scanner.max_workers` for parallel per-user writes |

**Streaming mode** (activated when `scan_result.detail_tmpdir` is set):

1. **Pass 1** — iterate all TSV chunks for a UID to count `total_files` and `total_used`
2. **Pass 2** — k-way min-heap merge of sorted chunks → write JSON entries line-by-line

Peak RAM for file reports = O(number of open chunks), not O(total files). Safe for 36M+ file trees.

**File naming:**

```
prefix=sda1  base=detail_report_dir  user=alice
-> detail_users/sda1_detail_report_dir_alice.json
```

---

### `src/cli_interface.py` — CLIInterface

Builds the `argparse` parser and handles:

- Splitting quoted multi-user strings (`"alice bob"` -> `["alice", "bob"]`)
- Wildcard expansion for `--files *.json`
- `--check-users` to display per-user detail reports from disk

---

### `src/utils.py` — Utilities

| Function | Description |
|----------|-------------|
| `format_size(bytes)` | `1073741824` -> `"1.00 GB"` |
| `parse_size(str)` | `"2TB"` -> `2199023255552` |
| `format_time_duration(sec)` | `3661` -> `"1h 1m 1s"` |
| `get_actual_disk_usage(stat)` | `stat.st_blocks * 512` (sparse-file aware) |
| `create_usage_bar(pct, width)` | `50, 20` -> `"[##########----------]"` |
| `build_uid_cache()` | Pre-loads all `/etc/passwd` entries once at startup |
| `save_json_report(data, path)` | Atomic JSON write, creates dirs if needed |

**`ScanHelper` class (static methods):**

| Method | Description |
|--------|-------------|
| `process_user_data(uid_sizes, uid_cache, user_map)` | Returns `(user_usage, team_usage, other_usage)` |
| `create_user_list(usage_dict)` | Dict -> `[{name, used}]` sorted by size |
| `filter_users_by_names(user_list, names_set)` | Keep only named users |

---

### `src/formatters/` — Display Layer

| Class | File | Responsibility |
|-------|------|----------------|
| `BaseFormatter` | `base_formatter.py` | Terminal size, usage bar |
| `TableFormatter` | `table_formatter.py` | ASCII grid tables with auto column widths |
| `ConfigDisplay` | `config_display.py` | `--list` output: team/user tables |
| `ReportFormatter` | `report_formatter.py` | `--show-report` and `--check-users` display |
| `ReportComparison` | `report_comparison.py` | Multi-report diff: growth rate, trend |

---

## CLI Reference

### Configuration

```bash
# Initialize with scan target
python disk_checker.py --init --dir /data/shared

# Change target directory
python disk_checker.py --dir /new/path

# Add teams
python disk_checker.py --add-team backend
python disk_checker.py --add-team frontend

# Add users
python disk_checker.py --add-user alice --team backend
python disk_checker.py --add-user alice bob carol --team backend

# Remove users
python disk_checker.py --remove-user alice

# List configuration
python disk_checker.py --list
python disk_checker.py --list --team backend
```

### Scanning

```bash
# Basic scan (auto workers = min(cpu*2, 64))
python disk_checker.py --run

# Custom output location
python disk_checker.py --run --output-dir /reports/
python disk_checker.py --run --prefix sda1
# -> detail_users/sda1_detail_report_dir_*.json
# -> detail_users/sda1_detail_report_file_*.json

python disk_checker.py --run --prefix sda1 --date
# -> sda1_disk_usage_report_20260322.json

# Control parallelism
python disk_checker.py --run --workers 32

# Verbose debug output
python disk_checker.py --run --debug
```

### Report Viewing

```bash
# Display a single report
python disk_checker.py --show-report --files disk_usage_report.json

# Display with user filter
python disk_checker.py --show-report --files disk_usage_report.json --user alice

# Compare multiple reports side by side
python disk_checker.py --show-report --files report1.json report2.json report3.json

# Wildcards
python disk_checker.py --show-report --files "sda1_disk_usage_report_*.json"

# Ranking strategy (usage = total size, growth = delta between reports)
python disk_checker.py --show-report --files r1.json r2.json --compare-by usage
python disk_checker.py --show-report --files r1.json r2.json --compare-by growth

# View per-user detail reports in terminal
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
  "teams": [
    { "name": "backend",  "team_ID": 1 },
    { "name": "frontend", "team_ID": 2 }
  ],
  "users": [
    { "name": "alice", "team_ID": 1 },
    { "name": "bob",   "team_ID": 1 },
    { "name": "carol", "team_ID": 2 }
  ]
}
```

Users are always sorted alphabetically when saved.

---

## Report Formats

### `disk_usage_report.json` — Main Report

```json
{
  "date": 1742600000,
  "directory": "/data/shared",
  "general_system": {
    "total":     10995116277760,
    "used":       8246337208320,
    "available":  2748779069440
  },
  "team_usage": [
    { "name": "backend",  "used": 3298534883328, "team_id": 1 },
    { "name": "Other",    "used":  549755813888 }
  ],
  "user_usage": [
    { "name": "alice", "used": 2748779069440, "team_id": 1 }
  ],
  "other_usage": [
    { "name": "uid-1234", "used": 549755813888 }
  ]
}
```

> `other_usage` contains UIDs found on disk but not registered in config.

---

### `detail_report_dir_{user}.json` — Directory Detail

```json
{
  "date": 1742600000,
  "directory": "/data/shared",
  "user": "alice",
  "total_used": 2748779069440,
  "dirs": [
    { "path": "/data/shared/alice/models",   "used": 1099511627776 },
    { "path": "/data/shared/alice/datasets", "used":  549755813888 }
  ]
}
```

Dirs are sorted by `used` descending.

---

### `detail_report_file_{user}.json` — File Detail

```json
{
  "date": 1742600000,
  "user": "alice",
  "total_files": 45231,
  "total_used": 2748779069440,
  "files": [
    { "path": "/data/shared/alice/models/weights.bin", "size": 536870912 },
    { "path": "/data/shared/alice/datasets/train.tar", "size": 268435456 }
  ]
}
```

Files are sorted by `size` descending. On large disks (36M+ files), the array is written
via streaming k-way merge — the file can be hundreds of MB in size and is safe to stream
via the companion web dashboard's paginated API.

---

### `permission_issues.json`

Flat array format — each inaccessible item is one entry. Named users are sorted
alphabetically; orphaned/unknown inodes use `user: "__unknown__"`.

```json
{
    "date": 1742600000,
    "directory": "/data/shared",
    "general_system": { "total": 10995116277760, "used": 8246337208320, "available": 2748779069440 },
    "permission_issues": {
        "total": 4,
        "items": [
            { "user": "alice",       "path": "/data/shared/alice/private", "type": "directory", "error": "Permission denied" },
            { "user": "alice",       "path": "/data/shared/alice/keys",    "type": "file",      "error": "Operation not permitted" },
            { "user": "bob",         "path": "/data/shared/bob/secret",   "type": "file",      "error": "Access denied" },
            { "user": "__unknown__", "path": "/orphan/inode",             "type": "directory", "error": "Cannot stat" }
        ]
    }
}
```

> `date` is a Unix timestamp (int). `general_system` mirrors the main report.
> Each item is written on **one line** in the file (compact dict, 4-space outer indent).
> This makes `grep` and streaming reads efficient on large permission reports.

---

## Scripts

### `scripts/export_user_reports.py`

Reads per-user JSON detail reports and exports one plain-text file per user, listing all
directories and files sorted by size (largest first).

**Auto-detects two directory layouts:**
- `--input-dir /reports/` → looks in `/reports/detail_users/`
- `--input-dir /reports/detail_users/` → looks directly in that folder (flat layout)

```bash
# Export all users (parallel, 4 workers default)
python scripts/export_user_reports.py --input-dir /reports/

# Custom output dir and filename prefix
python scripts/export_user_reports.py \
    --input-dir /reports/ \
    --output-dir /exports/txt/ \
    --prefix sda1

# Only specific users
python scripts/export_user_reports.py \
    --input-dir /reports/ \
    --users alice bob carol

# Parallel export with 8 workers (useful for many users)
python scripts/export_user_reports.py \
    --input-dir /reports/ \
    --workers 8

# Sequential mode (legacy, workers=1)
python scripts/export_user_reports.py \
    --input-dir /reports/ \
    --workers 1
```

**Output format** (`usage_alice.txt`):

```
Type  User                          Size  Path
------------------------------------------------------------------------------------------
dir   alice                      1.96 TB  /data/shared/alice/models
dir   alice                    512.00 GB  /data/shared/alice/datasets
file  alice                    256.00 GB  /data/shared/alice/models/weights.bin
file  alice                     64.00 GB  /data/shared/alice/datasets/train.tar
```

---

### `scripts/backfill_team_ids.py`

Patches existing `disk_usage_report.json` files by injecting `team_id` fields into
`team_usage` and `user_usage` entries, using mappings from `disk_checker_config.json`.

Useful for reports generated before `team_id` support was added. **Idempotent** — safe
to run multiple times; already-patched entries are skipped.

```bash
# Basic — auto-detect config, overwrite in-place
python scripts/backfill_team_ids.py --report disk_usage_report.json

# Explicit config path
python scripts/backfill_team_ids.py \
    --config disk_checker_config.json \
    --report /reports/disk_usage_report.json

# Dry-run preview (no writes)
python scripts/backfill_team_ids.py --report report.json --dry-run

# Write to a new file, keep original untouched
python scripts/backfill_team_ids.py --report report.json --output patched.json

# Batch: patch all dated reports in a directory
python scripts/backfill_team_ids.py --reports "reports/disk_usage_report_*.json"
```

**Batch output example:**

```
Backfilling 5 file(s) using disk_checker_config.json ...

  ✓  disk_usage_report_20250101.json   4 field(s) added
  ✓  disk_usage_report_20250102.json   4 field(s) added
  ✓  disk_usage_report_20250103.json   0 field(s) added   <- already patched
  ✓  disk_usage_report_20250104.json   4 field(s) added
  ✗  disk_usage_report_corrupt.json   ERROR: ...

Done: 5 file(s), 12 team_id field(s) added, 1 error(s)
```

---

### `scripts/migrate_permission_issues.py`

One-time migration script: converts `permission_issues_*.json` files from the old
nested format (`users[].inaccessible_items[]`) to the new flat format (`items[]`).

**Idempotent** — files already in the new format are detected and skipped.

```bash
# Dry-run: preview what would change
python scripts/migrate_permission_issues.py \
    --reports "/reports/**/permission_issues*.json" \
    --dry-run

# Migrate all matching files in-place
python scripts/migrate_permission_issues.py \
    --reports "/reports/**/permission_issues*.json"

# Single file, write to a new path
python scripts/migrate_permission_issues.py \
    --report old_permission_issues.json \
    --output new_permission_issues.json
```

**Output example:**

```
Migrating 12 file(s)...

  ✓  permission_issues_20250101.json   87 item(s) migrated
  ✓  permission_issues_20250201.json  124 item(s) migrated
  →  permission_issues_20260515.json  already in new format
  ✗  permission_issues_corrupt.json   ERROR: Invalid JSON

Done: 11 migrated, 1 skipped, 1 error(s). Total items converted: 1,245.
```

---

## Performance & Memory Notes

### Throughput guide

| Scenario | Recommendation |
|----------|----------------|
| NVMe local, ~10M files | Default workers (auto), completes in under 1 min |
| SSD local, 50M+ files | Default or `--workers 32`, expect 5–20 min |
| HDD single disk | Use `--workers 4–8` to avoid random-seek thrashing |
| NFS / SAN high-latency | Use `--workers 32–64`; threads compensate for per-request latency |
| Very large trees (80 TB, 36M files) | Default workers, expect 30–90 min; stall detector aborts after 5 min |
| Periodic reports | Use `--prefix` + `--date` in cron: `0 2 * * 0 python disk_checker.py --run --prefix weekly --date` |

### Memory consumption

During a large scan, memory usage breaks down as:

| Component | Approx. (36M files) |
|-----------|---------------------|
| `dir_sizes` dict (6.5M dirs) | ~2.5 GB |
| `file_paths` buffers (100K threshold × 64 threads) | ~1.3 GB |
| Python runtime + OS page cache | ~0.5–1 GB |
| **Total peak** | **~5 GB** |

> `DETAIL_FLUSH_THRESHOLD` controls the trade-off between flush frequency and RAM usage:
> - `100_000` → ~360 flushes, ~1.3 GB RAM — **current default**
> - `200_000` → ~180 flushes, ~2.56 GB RAM — only if you have 8+ GB free
> - `300_000+` → fewer flushes, diminishing returns; use only with 16+ GB RAM

### Sawtooth throughput pattern

Scan speed naturally varies in a sawtooth pattern:

1. **Initial burst (~15 k files/s)** — root dirs hot in OS page cache; no flush overhead yet
2. **Drop (~5–10 k files/s)** — threads hit `DETAIL_FLUSH_THRESHOLD` and flush (sort + write); deeper dirs cause more cache misses
3. **Recovery (~10 k files/s steady)** — flush cycles stabilise; throughput becomes consistent

This is expected behaviour. Use `--debug` to observe per-second stats.