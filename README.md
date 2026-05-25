# Check Disk

> High-performance disk usage scanner for Linux, written in Rust + Python. Designed for filesystems with tens of millions of files, with bounded RAM, atomic remote sync, and a SQLite-backed report format.

`check_disk` walks a Linux filesystem in parallel, classifies usage by configured teams/users, and writes report artifacts (JSON summaries + SQLite detail/treemap databases) suitable for terminal inspection, remote dashboards, and per-user export.

---

## Quick Start

### 1. Initialize config

```bash
python3 disk_checker.py --init --dir /data/shared
```

### 2. Add teams and users

```bash
python3 disk_checker.py --add-team backend
python3 disk_checker.py --add-user alice bob carol --team backend
```

### 3. Run a scan

```bash
python3 disk_checker.py --run --tree-map --output-dir /reports
```

### 4. View reports

```bash
# Summary report
python3 disk_checker.py --show-report --files /reports/disk_usage_report.json

# Per-user detail (dirs + files breakdown)
python3 disk_checker.py --detail --user alice --output-dir /reports --top 20

# Directory tree visualization
python3 disk_checker.py --tree-show --output-dir /reports --user alice --level 3

# Search for specific paths
python3 disk_checker.py --detail --user alice --output-dir /reports --search backup
python3 disk_checker.py --tree-show --output-dir /reports --search config --limit 20
```

### 5. Export per-user text reports

```bash
python3 scripts/export_user_reports.py \
  --input-dir /reports \
  --output-dir /reports/exports \
  --workers 4
```

---

## Features

### Phase 1: Rust parallel filesystem scanner

- Uses `ignore::WalkBuilder` with work-stealing parallel traversal (default: `cpus × 4`, max 64 workers).
- Skips critical pseudo-mounts (`proc`, `sys`, `dev`, `.snapshot`, `.zfs`, `.nfs`) automatically.
- Cross-device check prevents descending into bind mounts or NFS sub-mounts.
- Hard-link dedup (`DashSet<(ino, dev)>`) avoids double-counting cross-linked bytes.
- Streams events to per-thread binary `.bin` spill files (uncompressed, 16MB BufWriter) in a temp dir.
- Panic-safe: `DoneGuard` RAII ensures the progress loop never spins forever.

### Phase 2: Rust SQLite report pipeline (split into 2 phases)

- **Phase 2 (detail)**: reads spill files via Rayon, builds `data_detail.db`:
  - `data_detail.db`: 5 tables — `meta`, `users`, `file_names`, `dirs` (path pre-computed), `files` (ext inline). No FTS virtual tables. Keyset-pagination indexes for O(limit) cursor queries regardless of dataset size.
  - Compact spill re-encoding (18 bytes/row, LZ4-compressed) eliminates path String allocations.
  - `mallopt(M_MMAP_THRESHOLD=128KB)` reduces glibc heap fragmentation during build.
  - `malloc_trim(0)` after large drops returns freed heap pages to OS.
- **Phase 3 (treemap)**: loads persisted aggregates, builds `tree_map_data/treemap.db`:
  - All dirs included (no depth filter) for complete path resolution.
  - Runs independently — can be skipped or rerun without re-scanning.
- `permission_issues.db` — indexed access-error log.
- All databases built with `journal_mode=OFF` and atomically renamed into place.

### Atomic remote sync

- Streams artifacts over SSH using `tar | ssh tar` with SSH ControlMaster multiplexing.
- Per-file atomicity: staging → `mv -f` into place.
- Per-directory atomicity: staging dir → rotate old → promote new.
- Ctrl+C drains subprocesses, sweeps remote staging artifacts, pushes final heartbeat.

### Heartbeat + status

- `scan_status.json` updated atomically every 5s with stage, elapsed, running, message.
- Enqueued every 30s (and on phase changes) when sync is enabled.

---

## Requirements

- **Python**: 3.6+ (per `pyproject.toml`)
- **OS**: Linux x86_64
- **glibc**: bundled `.so` targets glibc 2.17+ (CentOS 7, RHEL 7+, Debian 9+, Ubuntu 14.04+)
- **SQLite**: 3.7.x+
- **For sync (optional)**: `ssh` with key-based auth, or `sshpass` for password auth

The Rust shared objects (`src/fast_scanner.abi3.so`, `src/export_rust.abi3.so`) ship in the repo. Rebuild after Rust changes:

```bash
bash src/rust_scanner/build.sh         # glibc 2.17 (widest compat)
bash src/rust_scanner/build.sh 2.28    # glibc 2.28
bash src/rust_scanner/build.sh native  # host glibc only
bash src/rust_exporter/build.sh
```

---

## Configuration

`disk_checker_config.json` is created by `--init`:

```json
{
  "directory": "/data/shared",
  "output_file": "disk_usage_report.json",
  "teams": [
    { "name": "backend", "team_id": 1 }
  ],
  "users": [
    { "name": "alice", "team_id": 1 },
    { "name": "bob", "team_id": 1 }
  ]
}
```

---

## CLI Reference

### Configuration Commands

| Command | Description |
|---|---|
| `--init --dir <path>` | Create config for a scan root. |
| `--dir <path>` | Update scan directory in existing config. |
| `--add-team <name>` | Add a team. |
| `--add-user <u1> [u2 ...] --team <team>` | Add users to a team. |
| `--remove-user <u1> [u2 ...]` | Remove users from config. |
| `--list` | List all teams and users. |
| `--list --team <name>` | List users in one team. |

### Scanning Commands

| Command | Description |
|---|---|
| `--run` | Run a full scan and generate reports. |
| `--run --workers <N>` | Override worker count (default: `min(32, cpus × 2)`). |
| `--run --debug` | Print Phase 1/2/3 timing + RSS diagnostics. |
| `--run --output-dir <dir>` | Write all reports to a specific directory. |
| `--run --prefix <name>` | Prefix generated report filenames. |
| `--run --date` | Append `YYYYMMDD` to output filenames. |
| `--run --tree-map` | Build `tree_map_data/treemap.db` (required for `--tree-show`). All dirs included regardless of `--level`. |
| `--run --level <N>` | Display depth metadata stored in treemap.db (default: 3). For actual tree depth at view time, use `--tree-show --level`. |
| `--run --webhook-url <URL>` | POST a Microsoft Teams summary on completion. |

### Sync Commands

| Command | Description |
|---|---|
| `--sync --sync-user <U> --sync-host <H> --sync-dest-dir <D>` | Sync reports to remote over SSH. |
| `--sync ... --sync-pass <pwd>` | Password auth via `sshpass`. |

### Report Commands

| Command | Description |
|---|---|
| `--show-report --files <f.json>` | Display a single summary report. |
| `--show-report --files "disk_usage_*.json"` | Match multiple reports by wildcard. |
| `--show-report --files <a.json> <b.json>` | Compare two reports (default: by growth). |
| `--show-report --files ... --compare-by usage` | Compare by total usage. |
| `--show-report --files ... --user <u1> [u2 ...]` | Filter displayed users. |
| `--detail` | Per-user detail from `data_detail.db`. See below. |
| `--tree-show` | ASCII directory tree from `treemap.db`. See below. |

### Detail Options (for `--detail`)

Requires `--user`. Reads from `--output-dir` (default: `.`).

```bash
python3 disk_checker.py --detail --user alice --output-dir /reports
```

| Option | Default | Description |
|---|---|---|
| `--user <u1> [u2 ...]` | required | User(s) to display. |
| `--output-dir <dir>` | `.` | Directory containing `detail_users/data_detail.db`. |
| `--type <choice>` | `report` | Section to display (see below). |
| `--top <N>` | 30 | Max rows per table. |
| `--search <keyword>` | (none) | Filter entries whose path contains keyword. |
| `--level <N>` | 3 | Depth for inode tree view. |
| `--limit <N>` | 20 | Max rows for inode tree view. |
| `--path <PATH>` | scan root | Start path for inode tree view. |

**`--type` choices:**

| Type | Displays |
|---|---|
| `report` (default) | Directory breakdown + largest files |
| `dirs` | Directory breakdown only (sorted by size) |
| `files` | Largest files only |
| `inode` | Directory breakdown sorted by file count |
| `permission` | Permission errors from `<output-dir>/permission_issues.db` |

**Examples:**

```bash
# Default: dirs + files
python3 disk_checker.py --detail --user alice --output-dir /reports

# Only files, filter by keyword
python3 disk_checker.py --detail --user alice --output-dir /reports --type files --search ".log"

# Top 50 dirs
python3 disk_checker.py --detail --user alice --output-dir /reports --type dirs --top 50

# File count per directory (inode view)
python3 disk_checker.py --detail --user alice --output-dir /reports --type inode

# Permission errors
python3 disk_checker.py --detail --user alice --output-dir /reports --type permission

# Multiple users
python3 disk_checker.py --detail --user alice bob --output-dir /reports --top 20
```

### Tree Options (for `--tree-show`)

Requires `--run --tree-map` to have been run first.

```bash
python3 disk_checker.py --tree-show --output-dir /reports
```

| Option | Default | Description |
|---|---|---|
| `--output-dir <dir>` | `.` | Directory containing `tree_map_data/treemap.db`. |
| `--user <u1> [u2 ...]` | (none) | Filter by user(s). Without `--user`, shows total dir sizes. |
| `--level <N>` | 3 | Maximum tree depth. |
| `--limit <N>` | 20 | Max results per level (or max flat list results when `--search`). |
| `--path <PATH>` | scan root | Start tree from a specific path. |
| `--search <keyword>` | (none) | Show flat list of dirs matching keyword, sorted by size. |

**Examples:**

```bash
# Full tree (all users, total sizes)
python3 disk_checker.py --tree-show --output-dir /reports --level 3 --limit 10

# Tree for a specific user
python3 disk_checker.py --tree-show --output-dir /reports --user alice --level 4

# Search for matching dirs (flat list, sorted by size)
python3 disk_checker.py --tree-show --output-dir /reports --search config --limit 20

# Start from a specific path
python3 disk_checker.py --tree-show --output-dir /reports --path /data/projects --level 2
```

**Sample output (with `--search`):**

```
============================================================
TREE-SHOW (all users)
============================================================
Root path: /
Search: 'config'
Depth: 3  |  Limit: 10 per level

  /www/server/panel/pyenv/lib/python3.12/config-3.12-x86_64-linux-gnu  [65 MB, 12 files]
  /usr/lib/python3.12/config-3.12-x86_64-linux-gnu  [28 MB, 13 files]
  /www/server/panel/config  [10 MB, 26 files]
  ...

  Total: 469 matching directories
```

### Per-user text export

```bash
python3 scripts/export_user_reports.py \
  --input-dir /reports \
  --output-dir /reports/exports \
  --workers 4
```

Writes two `.txt` files per user:
- `usage_dir_<user>.txt` — top directories by size
- `usage_file_<user>.txt` — top files by size

Both files contain full absolute paths. The exporter reads `data_detail.db` directly from `dirs.path`; `treemap.db` is optional and only used as a fallback by the Rust exporter.

---

## Output Layout

```text
<output_dir>/
├── disk_usage_report.json         # summary: general system + team + user usage
├── inode_usage_report.json        # inode counts per user
├── permission_issues.db           # access-error log (indexed SQLite; produced by Phase 2)
├── scan_status.json               # heartbeat: stage, elapsed, running, message
├── detail_users/
│   └── data_detail.db             # per-user files/dirs/exts, indexed
└── tree_map_data/                 # only when --tree-map
    └── treemap.db                 # path dictionary + directory tree
```

---

## Database Schemas

### `data_detail.db` (application_id = `0xC0DD15D1`)

5 tables only. No FTS. No ext dictionary. No top_files. No dir_user_size.

```
meta        — key/value: scan_root, scan_timestamp
users       — uid, username, team_id, total_files, total_dirs, total_size, permission_issues, is_target
file_names  — id, name (unique file basename dictionary)
dirs        — id, uid, parent_id, path (pre-computed absolute), owner_uid, size, files
              PRIMARY KEY (id, uid) — one row per (dir entity, user) pair
files       — dir_id, name_id, ext (inline TEXT), uid, size
              no surrogate id
```

Indexes (keyset pagination optimized):
```
ix_files_uid_size_dir_name      ON files(uid, size DESC, dir_id ASC, name_id ASC)
ix_files_uid_ext_size_dir_name  ON files(uid, ext, size DESC, dir_id ASC, name_id ASC)
ix_files_dir_uid_ext_size_name  ON files(dir_id, uid, ext, size DESC, name_id ASC)
ix_dirs_uid_size_dir            ON dirs(uid, size DESC, id ASC)
ix_file_names_name              ON file_names(name)
```

Cursor pagination: files cursor = `{size, dir_id, name_id}`, dirs cursor = `{size, id}`.

### `treemap.db` (application_id = `0xC0DD15C0`)

| Table | Purpose |
|---|---|
| `meta` | scan_root, scan_timestamp, max_level, total_size, total_dirs |
| `names` | directory segment dictionary |
| `dirs` | id, parent_id, name_id, total_size, file_count, dir_count, owner_uid, has_files |
| `owners` | uid → username |

---

## Architecture

### Pipeline phases

```
Phase 1: Rust scan (WalkBuilder, 64 workers)
  → /tmp/checkdisk_rust_*/scan_t*_b*.bin  (uncompressed binary events)
  → /tmp/.../perm_t*.tsv                  (permission errors)
  → /tmp/.../diragg_t*.bin                (dir aggregates)
         │
         ▼
Python: write summary JSON reports
  → disk_usage_report.json + siblings
         │
         ▼
Phase 2: Rust detail pipeline (Rayon)
  → compact spill re-encoding (18 bytes/row)
  → path tree assembly
  → data_detail.db (files, dirs[path pre-computed], top_files, ...)
  → persist treemap aggregates (aggregates.bin.zst)  [only with --tree-map]
         │
         ▼
Phase 3: Rust treemap pipeline  [only with --tree-map]
  → load aggregates.bin.zst
  → treemap.db (all dirs, no depth filter)
  → cleanup aggregates
         │
         ▼
Phase 4: final heartbeat (drain sync if enabled)
```

### Rust crates

| Crate | Module | Purpose |
|---|---|---|
| `src/rust_scanner/` | `scan_core.rs` | Phase 1 WalkBuilder parallel walker |
| | `scan_state.rs` | Per-thread buffers + uncompressed binary/TSV spill writers |
| | `report_pipeline.rs` | Phase 2/3 ingest → detail.db + treemap.db |
| | `db_writer.rs` | DDL, bulk insert, ANALYZE, atomic rename |
| | `pipe_events.rs` | Binary spill format reader |
| | `pipe_permission.rs` | Permission TSV → permission_issues.db |
| | `pipe_treemap.rs` | Path normalization helpers |
| | `pipe_types.rs` | Shared types + extension/parent helpers |
| `src/rust_exporter/` | `lib.rs` | Parallel TXT export via Rayon |

### Python modules

| Module | Description |
|---|---|
| `disk_checker.py` | CLI dispatcher |
| `src/cli_interface.py` | argparse setup + report rendering |
| `src/config_manager.py` | Config CRUD: teams, users, scan directory |
| `src/disk_scanner.py` | Wraps `fast_scanner.scan_disk`, builds `ScanResult` |
| `src/report_generator.py` | Writes JSON summaries, invokes Rust pipeline |
| `src/sync_manager.py` | `AsyncSyncPipeline` + atomic tar-stream sync |
| `src/scan_status.py` | Atomic `scan_status.json` + heartbeat thread |
| `src/formatters/` | Terminal table rendering |
| `src/constants.py` | Filenames, directory names, intervals |

---

## Performance

### Benchmarks (48M files, 4.3M dirs, 131 users, 64 workers, NFS)

| Metric | Value |
|---|---|
| Phase 1 wall time | ~730s |
| Phase 2+3 wall time | ~190s |
| **Total pipeline** | **~920s** |
| Phase 2 peak RSS | ~4-6 GB |
| detail.db size | ~2.4 GB |

### Key optimizations

**Phase 1:**
- `ignore::WalkBuilder` work-stealing (better than custom bounded queue for NFS)
- Per-thread event writers: 16MB BufWriter, uncompressed `.bin` files, flush at 32MB
- 64 workers default (Python layer: `min(32, cpus × 2)` when `--workers` not specified; Rust Phase 1 supports up to `cpus × 4` clamped to 64 if called directly)

**Phase 2:**
- Phase 2/3 split: detail.db and treemap.db built independently → RAM freed between phases
- Compact spill re-encoding: 18 bytes/row LZ4-compressed (was ~200 bytes with String paths)
- Parallel re-encode pass (Rayon + DashMap) for 131-user workloads
- `mallopt(M_MMAP_THRESHOLD=128KB)` + `malloc_trim(0)` returns ~10GB heap to OS after large drops
- `PRAGMA optimize` instead of full `ANALYZE` (saves ~18s)
- Replaced FTS4 virtual tables with LIKE-based keyword search + covering keyset indexes → simpler schema, no FTS tokenizer overhead

---

## Reliability

### Bounded RAM

| Source | Bound |
|---|---|
| Phase 1 event buffer | `SCAN_EVENT_FLUSH_BYTES_THRESHOLD = 32MB` per bucket per worker |
| Phase 2 row spill | `ROW_SPILL_THRESHOLD = 200K` rows → `.rows` spill files |
| Phase 2 compact spill | 18 bytes/row after re-encoding |
| Treemap aggregates | persisted to `aggregates.bin.zst`, freed before Phase 3 |

### Hang protection

| Risk | Protection |
|---|---|
| Walker panic | `DoneGuard` RAII flips `done` flag on drop |
| SSH hang | `ServerAliveInterval=30 × CountMax=3` drops dead connections |
| `/tmp` orphans | `_cleanup_orphan_tmpdirs()` sweeps at run start |
| Ctrl+C mid-sync | Flushes status, drains pipeline, sweeps remote staging |
| Partial DB write | `finalize_db` deletes `<final>.tmp.db` on any error |

---

## Verification

```bash
# Python tests
python3 -m pytest tests/ -q

# Rebuild Rust artifacts
bash src/rust_scanner/build.sh
bash src/rust_exporter/build.sh

# Check SQLite schema of a generated detail DB
sqlite3 /reports/detail_users/data_detail.db ".schema"
sqlite3 /reports/detail_users/data_detail.db "SELECT * FROM meta"
```
