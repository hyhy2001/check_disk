# Disk Usage Checker

A high-performance Python CLI tool for monitoring disk usage by team and user,
with parallel scanning, JSON reporting, plain-text export, and multi-report comparison.

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
- [Performance Notes](#performance-notes)

---

## Features

- **Parallel disk scanning** — work-stealing multi-thread `scandir()`, auto-scales to CPU count (cap 64)
- **Single `stat()` per entry** — uses `S_ISDIR`/`S_ISREG` from `st_mode` instead of chained `is_dir()`/`is_file()` calls
- **Hard-link deduplication** — `st_nlink == 1` fast path avoids lock contention; only hard-linked inodes acquire a lock
- **Snapshot/NFS detection** — automatically skips directories on a different device ID (covers ZFS, Btrfs, NFS, bind-mounts)
- **Name-based skip** — `.snapshot`, `.snapshots`, `.zfs`, `proc`, `sys`, `dev` skipped without a `stat()` call
- **Event-driven idle** — idle threads sleep on `threading.Event` instead of busy-waiting every 5 ms
- **Team/user tracking** — maps filesystem UIDs to configured users and teams
- **Sparse file support** — uses `st_blocks * 512` for accurate on-disk size (not logical file size)
- **Permission issue tracking** — records inaccessible files/dirs per user
- **Per-user detail reports** — per-user JSON files listing top dirs and files by size
- **Plain-text export** — `scripts/export_user_reports.py` converts JSON detail reports to sorted plain-text files
- **Team ID backfill** — `scripts/backfill_team_ids.py` patches existing reports without `team_id` using config mapping
- **Multi-report comparison** — compare N reports side-by-side with growth/usage ranking
- **Stall detection** — automatically aborts if scan stops making progress for 5 minutes
- **Memory monitoring** — terminates if RSS exceeds configured limit

---

## Requirements

- Python 3.6+
- [`psutil`](https://pypi.org/project/psutil/) (memory monitoring)

```bash
pip install psutil
```

---

## Quick Start

```bash
# 1. Initialize config with the directory to scan
python disk_checker.py --init --dir /data/users

# 2. Define teams
python disk_checker.py --add-team JP
python disk_checker.py --add-team VN

# 3. Add users to teams
python disk_checker.py --add-user Hirakimoto Sasi --team JP
python disk_checker.py --add-user Binh --team VN

# 4. Run a scan
python disk_checker.py --run

# 5. View the report
python disk_checker.py --show-report --files disk_usage_report.json

# 6. Check per-user detail reports
python disk_checker.py --check-users Binh Sasi

# 7. Export plain-text usage files (one per user)
python scripts/export_user_reports.py --input-dir /reports/ --output-dir /reports/txt/
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
   |    |-- ThreadStats   <- uid_sizes, dir_sizes, file_paths, permission_issues
   |    `-- inode lock    <- acquired only for hard-linked files (st_nlink > 1)
   `-- ScanResult         <- merged data from all threads
         |
         v
  ReportGenerator         <- converts ScanResult -> JSON files
   |-- disk_usage_report.json         (always)
   |-- permission_issues.json         (auto, if issues found)
   |-- detail_report_dir_{user}.json  (per user, always)
   `-- detail_report_file_{user}.json (per user, always)
```

### Source Layout

```
disk_checker.py          <- CLI entry point
disk_checker_config.json <- configuration file

src/
  cli_interface.py       <- argparse, argument routing
  config_manager.py      <- JSON config CRUD
  disk_scanner.py        <- DiskScanner (parallel core)
  report_generator.py    <- JSON report writer
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
  S_ISREG(mode) -> st_nlink check -> count size
```

**Hard-link deduplication:**

- `st_nlink == 1`: no lock acquired, file counted immediately (fast path, ~99% of files)
- `st_nlink > 1`: `(st_ino, st_dev)` checked under a single `Lock` (rare path)

**ScanResult fields:**

```python
@dataclass
class ScanResult:
    general_system:    Dict[str, int]              # total, used, available bytes
    team_usage:        List[Dict]                  # [{name, used}]
    user_usage:        List[Dict]                  # [{name, used}] -- users in config
    other_usage:       List[Dict]                  # [{name, used}] -- UIDs not in config
    timestamp:         int                         # Unix epoch
    top_dir:           List[Dict]                  # [{dir, user, user_usage}]
    permission_issues: Dict                        # {users: [...], unknown_items: [...]}
    detail_files:      Dict[str, List[Tuple]]      # username -> [(path, size), ...]
```

---

### `src/report_generator.py` — ReportGenerator

Converts a `ScanResult` to JSON files. All sibling reports share the same
`prefix` and directory as the main report.

| Method | Output file | When |
|--------|-------------|------|
| `generate_report(scan_result)` | `disk_usage_report.json` | Always with `--run` |
| `generate_detail_reports(scan_result)` | `detail_report_dir_{user}.json` + `detail_report_file_{user}.json` | Always with `--run` |

**File naming:**

```
prefix=sda1  base=permission_issues
-> sda1_permission_issues.json

prefix=sda1  base=detail_report_dir  user=Binh
-> sda1_detail_report_dir_Binh.json
```

---

### `src/cli_interface.py` — CLIInterface

Builds the `argparse` parser and handles:

- Splitting quoted multi-user strings (`"user1 user2"` -> `["user1", "user2"]`)
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
| `build_uid_cache()` | Pre-loads all `/etc/passwd` entries once |
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
python disk_checker.py --init --dir /path/to/scan

# Change target directory
python disk_checker.py --dir /new/path

# Add teams
python disk_checker.py --add-team JP
python disk_checker.py --add-team VN

# Add users
python disk_checker.py --add-user Hirakimoto --team JP
python disk_checker.py --add-user Hirakimoto Sasi --team JP

# Remove users
python disk_checker.py --remove-user Hirakimoto

# List configuration
python disk_checker.py --list
python disk_checker.py --list --team JP
```

### Scanning

```bash
# Basic scan (auto workers = min(cpu*2, 64))
python disk_checker.py --run

# Custom output location
python disk_checker.py --run --output-dir /reports/
python disk_checker.py --run --prefix sda1
# -> sda1_disk_usage_report.json + sda1_permission_issues.json

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
python disk_checker.py --show-report --files disk_usage_report.json --user Binh

# Compare multiple reports
python disk_checker.py --show-report --files report1.json report2.json report3.json

# Wildcards
python disk_checker.py --show-report --files "sda1_disk_usage_report_*.json"

# Ranking strategy
python disk_checker.py --show-report --files r1.json r2.json --compare-by usage
python disk_checker.py --show-report --files r1.json r2.json --compare-by growth

# View per-user detail reports
python disk_checker.py --check-users Binh Sasi
python disk_checker.py --check-users Binh --output-dir /reports/ --prefix sda1
```

---

## Configuration Format

`disk_checker_config.json`:

```json
{
  "directory": "/data/users",
  "output_file": "disk_usage_report.json",
  "teams": [
    { "name": "JP",  "team_ID": 1 },
    { "name": "VN",  "team_ID": 2 }
  ],
  "users": [
    { "name": "Binh",       "team_ID": 2 },
    { "name": "Hirakimoto", "team_ID": 1 },
    { "name": "Sasi",       "team_ID": 1 }
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
  "directory": "/data/users",
  "general_system": {
    "total":     10995116277760,
    "used":       8246337208320,
    "available":  2748779069440
  },
  "team_usage": [
    { "name": "JP",    "used": 3298534883328, "team_id": 1 },
    { "name": "Other", "used":  549755813888 }
  ],
  "user_usage": [
    { "name": "Binh", "used": 2748779069440, "team_id": 2 }
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
  "directory": "/data/users",
  "user": "Binh",
  "total_used": 2748779069440,
  "dirs": [
    { "path": "/data/users/Binh/projects", "used": 1099511627776 }
  ]
}
```

### `detail_report_file_{user}.json` — File Detail

```json
{
  "date": 1742600000,
  "user": "Binh",
  "total_files": 12345,
  "total_used": 2748779069440,
  "files": [
    { "path": "/data/users/Binh/projects/model.bin", "size": 536870912 }
  ]
}
```

Files are sorted by size descending.

---

### `permission_issues.json`

```json
{
  "permission_issues": {
    "users": [
      {
        "name": "Hirakimoto",
        "inaccessible_items": [
          {
            "path": "/data/users/Hirakimoto/private",
            "type": "directory",
            "error": "[Errno 13] Permission denied"
          }
        ]
      }
    ],
    "unknown_items": []
  }
}
```

---

## Scripts

### `scripts/export_user_reports.py`

Reads per-user JSON detail reports and exports one plain-text file per user.
Each file lists directories and files sorted by size (high to low).

```bash
# Export all users found in input dir
python scripts/export_user_reports.py --input-dir /reports/

# Custom output dir and prefix
python scripts/export_user_reports.py \
    --input-dir /reports/ \
    --output-dir /reports/txt/ \
    --prefix sda1

# Only specific users
python scripts/export_user_reports.py --input-dir /reports/ --users Binh Sasi
```

**Output format** (`usage_Binh.txt`):

```
Type  User                          Size  Path
------------------------------------------------------------------------------------------
dir   Binh                      1.96 MB  /data/users/Binh/projects
file  Binh                     96.00 KB  /data/users/Binh/notes.txt
```

---

### `scripts/backfill_team_ids.py`

Patches an existing `disk_usage_report.json` by injecting `team_id` fields into
`team_usage` and `user_usage` entries, using the mappings defined in `disk_checker_config.json`.
Useful for reports generated before `team_id` support was added.

The script is **idempotent** — running it multiple times will not duplicate or overwrite
existing `team_id` values.

```bash
# Basic — auto-detect config next to report, overwrite in-place
python scripts/backfill_team_ids.py --report disk_usage_report.json

# Explicit config path
python scripts/backfill_team_ids.py \
    --config disk_checker_config.json \
    --report /reports/disk_usage_report.json

# Preview without writing
python scripts/backfill_team_ids.py --report report.json --dry-run

# Write to a new file, keep original untouched
python scripts/backfill_team_ids.py --report report.json --output patched.json
```

**What it patches** (only entries missing `team_id`):

```json
// Before
{ "name": "JP", "used": 3298534883328 }

// After
{ "name": "JP", "used": 3298534883328, "team_id": 1 }
```

---

## Performance Notes

| Scenario | Recommendation |
|----------|----------------|
| NVMe local, ~10M files | Default workers (auto), scan completes in under 1 min |
| SSD local, 100M files | Default or `--workers 32`, ~5-15 min |
| HDD single disk | Use `--workers 4-8` to avoid random-seek contention |
| NFS / SAN high-latency | Use `--workers 32-64`; more threads compensate for per-request latency |
| Very large trees | Monitor with `--debug`; stall detector aborts after 5 min of no progress |
| Slow `/etc/passwd` | UID cache is pre-built once at startup via `build_uid_cache()` |
| Periodic reports | Use `--prefix` + `--date` in cron: `0 2 * * 0 python disk_checker.py --run --prefix weekly --date` |