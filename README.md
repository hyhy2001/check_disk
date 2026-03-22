# Disk Usage Checker

A high-performance Python CLI tool for monitoring disk usage by team and user, with parallel scanning, JSON reporting, and multi-report comparison.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Module Reference](#module-reference)
- [CLI Reference](#cli-reference)
- [Configuration Format](#configuration-format)
- [Report Formats](#report-formats)
- [Performance Notes](#performance-notes)

---

## Features

- **Parallel disk scanning** — multi-threaded `scandir()` with work-stealing across threads
- **Team/user tracking** — maps filesystem UIDs to configured users and teams
- **Sparse file support** — uses `st_blocks` for accurate on-disk size (not logical size)
- **Inode deduplication** — hard-linked files are counted only once
- **Permission issue tracking** — records inaccessible files/dirs per user
- **Four report types** — main, permission-issues, top-user, check-user (all JSON)
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

No other external dependencies.

---

## Installation

```bash
git clone <repo-url>
cd check_disk
chmod +x disk_checker.py
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
python disk_checker.py --add-team IND

# 3. Add users to teams
python disk_checker.py --add-user Hirakimoto Sasi --team JP
python disk_checker.py --add-user Binh --team VN
python disk_checker.py --add-user Deepti --team IND

# 4. Run a scan
python disk_checker.py --run

# 5. View the report
python disk_checker.py --show-report --files disk_usage_report.json
```

---

## Architecture

### Data Flow

```
disk_checker_config.json
         │
         ▼
  ConfigManager           ← reads/writes JSON config
         │
         ▼
  DiskScanner.scan()      ← multi-thread scandir()
   ├── _worker() × N      ← each thread processes directories
   │    ├── ThreadStats   ← uid_sizes, dir_sizes, permission_issues
   │    └── inode lock    ← global dedup via (ino, dev) key
   └── ScanResult         ← merged data from all threads
         │
         ▼
  ReportGenerator         ← converts ScanResult → JSON files
   ├── disk_usage_report.json        (always)
   ├── permission_issues.json        (auto, if issues found)
   ├── top_user.json                 (opt: --top-user)
   └── check_user.json              (opt: --check-user)
```

### Layer Diagram

```
┌─────────────────────────────────────────────┐
│  Entry point: disk_checker.py               │
│  Helper: _resolve_output_path()             │
└──────────────┬──────────────────────────────┘
               │
  ┌────────────▼───────────────┐
  │  src/                      │
  │                            │
  │  cli_interface.py          │ ← argparse, wildcard expansion
  │  config_manager.py         │ ← JSON config CRUD
  │  disk_scanner.py           │ ← DiskScanner (core)
  │  report_generator.py       │ ← ReportGenerator
  │  utils.py                  │ ← shared helpers + ScanHelper
  │                            │
  │  formatters/               │
  │   base_formatter.py        │ ← BaseFormatter (terminal size, bar)
  │   table_formatter.py       │ ← ASCII table rendering
  │   config_display.py        │ ← --list output
  │   report_formatter.py      │ ← --show-report output
  │   report_comparison.py     │ ← multi-report diff table
  └────────────────────────────┘
```

---

## Module Reference

### `disk_checker.py` — Entry Point

Top-level CLI router. Dispatches each `--flag` to the appropriate manager class.

Helper function:

```python
_resolve_output_path(base, output_dir, output_override, prefix, add_date)
    → (output_file: str, prefix_str: str, date_str: str)
```

Computes the final output path and extracts the `prefix` and `date_suffix` so that sibling reports (`permission_issues`, `top_user`, etc.) automatically share the same naming convention.

---

### `src/config_manager.py` — ConfigManager

Manages `disk_checker_config.json`.

| Method | Description |
|--------|-------------|
| `initialize_config(dir)` | Create new config with target directory |
| `update_directory(dir)` | Change scan target in existing config |
| `add_team(name)` | Add team, auto-assign team_ID |
| `add_user(username, team)` | Add user to team |
| `remove_user(username)` | Remove user from config |
| `add_users_batch(names, team)` | Bulk add, returns already-existing names |
| `get_config()` | Return full config dict |
| `get_users_by_team(team)` | Return sorted username list for a team |

Config is always sorted by username before saving.

---

### `src/disk_scanner.py` — DiskScanner

Core parallel scanner using Python threads and `os.scandir()`.

**Key parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_workers` | `min(cpu×2, 32)` | Worker thread count |
| `debug` | `False` | Print per-directory timing |
| `check_users` | `None` | If set, only collect data for these users |
| `top_user_count` | `None` | If set, enable top-user report generation |
| `min_usage` | 2 TB | Minimum threshold for top-user filter |

**Threading model:**

- Each thread has its own local queue (`work_queues[i]`)
- When a thread's queue is empty, it **steals** from the global queue in batches of 5000
- When all queues and the global queue are empty, threads exit
- A dedicated **progress thread** prints stats every 10 s and detects stalls (no new files for 5 min)

**Deduplication:**

Every file is checked against a global `set` of `(st_ino, st_dev)` pairs under a lock, so hard-linked files are only counted once.

**ScanResult fields:**

```python
@dataclass
class ScanResult:
    general_system: Dict[str, int]   # total, used, available bytes
    team_usage:     List[Dict]        # [{name, used}]
    user_usage:     List[Dict]        # [{name, used}]  — users in config
    other_usage:    List[Dict]        # [{name, used}]  — UIDs not in config
    timestamp:      int               # Unix epoch
    top_dir:        List[Dict]        # [{dir, user, user_usage}]
    permission_issues: Dict           # {users: [...], unknown_items: [...]}
```

---

### `src/report_generator.py` — ReportGenerator

Converts a `ScanResult` object into JSON files.

**File naming:**

The generator reads `config['output_prefix']` and `config['output_date_suffix']` (set by `disk_checker.py`) to build sibling filenames consistently:

```
prefix=sda1  date=20260322  base=permission_issues
→  sda1_permission_issues_20260322.json
```

**Methods:**

| Method | Output file | When |
|--------|-------------|------|
| `generate_report(scan_result)` | `disk_usage_report.json` | Always with `--run` |
| `generate_permission_issues_report(scan_result)` | `permission_issues.json` | Auto if any issues |
| `generate_top_user_report(scan_result, top_n, min_usage)` | `top_user.json` | `--top-user` flag |
| `generate_check_user_report(scan_result, users)` | `check_user.json` | `--check-user` flag |

---

### `src/cli_interface.py` — CLIInterface

Builds the `argparse` parser and handles:

- Splitting quoted multi-user strings (`"user1 user2"` → `["user1", "user2"]`)
- Wildcard expansion for `--files *.json`
- Dispatching `--show-report` to `ReportFormatter`

---

### `src/utils.py` — Utilities

Pure functions with no side effects. Key entries:

| Function | Description |
|----------|-------------|
| `format_size(bytes)` | `1073741824` → `"1.00 GB"` |
| `parse_size(str)` | `"2TB"` → `2199023255552` |
| `format_time_duration(sec)` | `3661` → `"1h 1m 1s"` |
| `format_timestamp(epoch)` | `1623456789` → `"2021-06-12 01:33:09"` |
| `get_actual_disk_usage(stat)` | `stat.st_blocks * 512` (sparse-file aware) |
| `create_usage_bar(pct, width)` | `50, 20` → `"[##########----------]"` |
| `build_uid_cache()` | Pre-loads all `/etc/passwd` entries |
| `get_username_from_uid(uid, cache)` | UID → username with cache |
| `save_json_report(data, path)` | Atomic JSON write, creates dirs if needed |
| `load_json_report(path)` | Safe JSON load, returns `{}` on error |

**`ScanHelper` class** (static methods):

| Method | Description |
|--------|-------------|
| `process_user_data(uid_sizes, uid_cache, user_map)` | Returns `(user_usage, team_usage, other_usage)` dicts |
| `create_user_list(usage_dict, sort=True)` | Dict → `[{name, used}]` sorted by size |
| `filter_users_by_min_usage(user_list, min_bytes)` | Filter below threshold |
| `filter_users_by_names(user_list, names_set)` | Keep only named users |

---

### `src/formatters/` — Display Layer

All formatters inherit from `BaseFormatter`.

| Class | File | Responsibility |
|-------|------|---------------|
| `BaseFormatter` | `base_formatter.py` | Terminal size, text padding, usage bar |
| `TableFormatter` | `table_formatter.py` | ASCII grid tables with auto column widths |
| `ConfigDisplay` | `config_display.py` | `--list` output: team/user tables |
| `ReportFormatter` | `report_formatter.py` | `--show-report` single report display |
| `ReportComparison` | `report_comparison.py` | Multi-report diff: growth rate, trend arrows |

`create_usage_bar()` is defined once in `utils.py` and delegated to by `BaseFormatter._create_usage_bar()` and `DiskScanner._create_usage_bar()`.

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

# Add users (multiple ways)
python disk_checker.py --add-user Hirakimoto --team JP
python disk_checker.py --add-user Hirakimoto Sasi --team JP
python disk_checker.py --add-user "Hirakimoto Sasi" --team JP

# Remove users
python disk_checker.py --remove-user Hirakimoto
python disk_checker.py --remove-user Hirakimoto Sasi

# List configuration
python disk_checker.py --list
python disk_checker.py --list --team JP
```

### Scanning

```bash
# Basic scan
python disk_checker.py --run

# Custom output file
python disk_checker.py --run --output /reports/disk_usage_report.json

# Output directory (uses default filename)
python disk_checker.py --run --output-dir /reports/

# Prefix and/or date in filename
python disk_checker.py --run --prefix sda1
# → sda1_disk_usage_report.json
# → sda1_permission_issues.json  (if issues found)

python disk_checker.py --run --prefix sda1 --date
# → sda1_disk_usage_report_20260322.json

python disk_checker.py --run --output-dir /reports/ --prefix sda1 --date
# → /reports/sda1_disk_usage_report_20260322.json

# Control parallelism
python disk_checker.py --run --workers 16

# Debug mode (verbose per-directory output)
python disk_checker.py --run --debug
```

### Specialized Scans

```bash
# Top N users by usage (generates top_user.json)
python disk_checker.py --run --top-user
python disk_checker.py --run --top-user 20
python disk_checker.py --run --top-user 20 --min-usage 500GB
python disk_checker.py --run --top-user 20 --min-usage 1TB

# Focused scan for specific users (generates check_user.json)
python disk_checker.py --run --check-user "user1 user2 user3"
```

### Report Viewing

```bash
# Display a single report
python disk_checker.py --show-report --files disk_usage_report.json

# Display with user filter
python disk_checker.py --show-report --files disk_usage_report.json --user Binh

# Compare multiple reports (ranked by growth rate by default)
python disk_checker.py --show-report --files report1.json report2.json report3.json

# Wildcards
python disk_checker.py --show-report --files "sda1_disk_usage_report_*.json"

# Change ranking strategy
python disk_checker.py --show-report --files r1.json r2.json --compare-by usage
python disk_checker.py --show-report --files r1.json r2.json --compare-by growth
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
    { "name": "VN",  "team_ID": 2 },
    { "name": "IND", "team_ID": 3 }
  ],
  "users": [
    { "name": "Binh",       "team_ID": 2 },
    { "name": "Deepti",     "team_ID": 3 },
    { "name": "Hirakimoto", "team_ID": 1 },
    { "name": "Sasi",       "team_ID": 1 }
  ]
}
```

Users are always sorted alphabetically when saved.

---

## Report Formats

### `disk_usage_report.json` — Main Report

Generated by every `--run` call.

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
    { "name": "JP",    "used": 3298534883328 },
    { "name": "VN",    "used": 2748779069440 },
    { "name": "Other", "used":  549755813888 }
  ],
  "user_usage": [
    { "name": "Hirakimoto", "used": 1649267441664 },
    { "name": "Sasi",       "used": 1649267441664 },
    { "name": "Binh",       "used": 2748779069440 }
  ],
  "other_usage": [
    { "name": "uid-1234", "used": 549755813888 }
  ]
}
```

> **`other_usage`** contains UIDs that exist on disk but are not registered in the config. These are users whose directories were found during the scan but have no matching entry in `disk_checker_config.json`.

---

### `permission_issues.json` — Permission Issues Report

Auto-generated by `generate_report()` whenever the scan encounters any inaccessible paths.

```json
{
  "date": 1742600000,
  "directory": "/data/users",
  "general_system": { "total": 0, "used": 0, "available": 0 },
  "permission_issues": {
    "users": [
      {
        "name": "Hirakimoto",
        "inaccessible_items": [
          {
            "path": "/data/users/Hirakimoto/private",
            "type": "directory",
            "error": "[Errno 13] Permission denied: '/data/users/Hirakimoto/private'"
          }
        ]
      }
    ],
    "unknown_items": [
      {
        "path": "/data/scratch/tmp_xyz",
        "type": "file",
        "error": "[Errno 13] Permission denied"
      }
    ]
  }
}
```

- **`users`** — errors grouped by the owning username
- **`unknown_items`** — errors where ownership couldn't be determined

---

### `top_user.json` — Top User Report

Generated only with `--top-user [N]`.

```json
{
  "date": 1742600000,
  "directory": "/data/users",
  "top_user": 20,
  "min_usage": "2.00 TB",
  "team_usage": [ { "name": "JP", "used": 3298534883328 } ],
  "user_usage": [ { "name": "Binh", "used": 2748779069440 } ],
  "other_usage": [],
  "detail_dir": [
    { "dir": "/data/users/Binh/projects", "user": "Binh", "user_usage": 1099511627776 }
  ]
}
```

`detail_dir` contains every directory entry attributed to a top-N user, sorted by `user_usage` descending.

---

### `check_user.json` — Check-User Report

Generated only with `--check-user`.

```json
{
  "date": 1742600000,
  "directory": "/data/users",
  "check_users": ["Binh", "Deepti"],
  "user_usage": [
    { "name": "Binh",   "used": 2748779069440 },
    { "name": "Deepti", "used": 0 }
  ],
  "detail_dir": [
    { "dir": "/data/users/Binh/datasets", "user": "Binh", "user_usage": 1649267441664 }
  ],
  "permission_issues": {
    "users": []
  }
}
```

Users listed in `check_users` who have zero usage still appear in `user_usage` with `"used": 0`.

---

## Performance Notes

| Scenario | Recommendation |
|----------|---------------|
| NFS mounts | Keep `--workers` ≤ 32 (default cap) |
| Very large dirs (100M+ files) | Use `--debug` to check for stalled threads |
| Slow `/etc/passwd` resolution | Cache is pre-built once via `build_uid_cache()` |
| High memory usage | Scanner monitors RSS and aborts if limit exceeded |
| Interrupted scan | Press Ctrl+C — partial results are discarded cleanly |
| Periodic reports | Use `--prefix` + `--date` in a cron job to accumulate history:  `0 2 * * 0 python disk_checker.py --run --prefix weekly --date` |