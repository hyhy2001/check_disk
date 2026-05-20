# Check Disk

> High-performance disk usage scanner for Linux, written in Rust + Python. Designed for filesystems with tens of millions of files, with bounded RAM, atomic remote sync, and a SQLite-backed dashboard format.

`check_disk` walks a Linux filesystem in parallel, classifies usage by configured teams/users, and writes report artifacts (JSON summaries + SQLite detail/treemap databases) suitable for terminal inspection, remote dashboards, and per-user export. The Rust core (`fast_scanner.abi3.so`) handles the hot path so traversal and report building avoid Python's GIL and stay memory-bounded at 75M-file scale.

---

## Quick Start

### 1. Initialize config

```bash
python3 disk_checker.py --init --dir /data/shared
```

### 2. Add teams and users

```bash
python3 disk_checker.py --add-team backend
python3 disk_checker.py --add-team frontend

python3 disk_checker.py --add-user alice bob carol --team backend
python3 disk_checker.py --add-user dave eve --team frontend
```

### 3. Run a scan

```bash
python3 disk_checker.py --run --tree-map
```

### 4. View reports

```bash
# Summary report
python3 disk_checker.py --show-report --files disk_usage_report.json

# Per-user detail (dirs + files breakdown)
python3 disk_checker.py --detail --user alice bob --top 20

# Directory tree visualization
python3 disk_checker.py --tree-show --user alice --level 3
```

### 5. Export per-user text reports

```bash
python3 scripts/export_user_reports.py \
  --input-dir . \
  --output-dir ./exports \
  --workers 4
```

---

## Features

### Native Rust scanner (bounded-queue parallel walker)

- Custom parallel walker with a bounded `crossbeam_channel` queue (cap 200K dirs) — RAM stays flat regardless of directory count.
- Workers fall back to inline depth-first traversal when the queue is full (no blocking, no deadlock).
- Skips critical pseudo mounts (`proc`, `sys`, `dev`, …) and snapshots automatically.
- Cross-device check prevents descending into bind mounts or NFS mounts.
- Hard-link dedup (`DashSet<(ino, dev)>`) avoids double-counting cross-link bytes.
- Streams events to bounded binary spill files (lz4-compressed) in a per-scan temp dir.
- Panic-safe: a `DoneGuard` ensures the progress loop never spins forever even if the walker crashes.

### Rust SQLite report pipeline

- Phase 2 reads spill files via Rayon, builds three SQLite databases:
  - `detail_users/data_detail.db` — per-user file/dir breakdown, indexed for `ORDER BY size DESC` pagination.
  - `tree_map_data/treemap.db` — shared path dictionary + directory tree, depth-filtered by `--level`.
  - `permission_issues.db` — indexed access-error log.
- All databases built with `journal_mode=OFF` (no WAL/SHM sidecars) and atomically renamed into place via `fs::rename`.
- Cross-DB queries via `ATTACH treemap.db` resolve directory paths from `detail.db` rows on demand.
- Compatible with SQLite 3.7.x+ (no `WITHOUT ROWID` syntax).

### Atomic remote sync

- Streams artifacts over SSH using `tar -czf | ssh tar -xzOf` (single files) or `tar -czf | ssh tar -xzf` (directories), multiplexed through SSH ControlMaster.
- **Per-file atomicity**: each file lands in `.<name>.__staging__.<pid>` then `mv -f` into place.
- **Per-directory atomicity**: each directory lands in `<dir>.__staging__/`, the live target rotates to `<dir>.__old__/`, then staging is promoted with `mv -T`.
- Ctrl+C drains running subprocesses, sweeps leftover staging artifacts on the remote, and pushes a final `error / interrupted` heartbeat.

### Heartbeat + status

- `scan_status.json` is updated atomically every 5s with `{ stage, phase_elapsed_sec, total_elapsed_sec, running, message, host, pid, ... }`.
- When sync is enabled, the status file is enqueued every 30s (and on every phase change) so dashboards see live progress.

### Reporting + notifications

- Compare historical summary reports by total usage or growth rate.
- Microsoft Teams Workflow notification on completion.

---

## Requirements

- **Python**: 3.8+
- **OS**: Linux x86_64
- **glibc**: bundled `.so` artifacts target glibc 2.17+ (CentOS 7, RHEL 7+, Debian 9+, Ubuntu 14.04+)
- **SQLite**: 3.7.x+ (for reading output databases)
- **For sync (optional)**:
  - `ssh` client + key-based auth, or
  - `sshpass` if using `--sync-pass`

The Rust shared objects (`src/fast_scanner.abi3.so`, `src/export_rust.abi3.so`) ship in the repo. Rebuild after Rust changes:

```bash
bash src/rust_scanner/build.sh         # default: glibc 2.17 (widest)
bash src/rust_scanner/build.sh 2.28    # glibc 2.28 (smaller binary)
bash src/rust_scanner/build.sh native  # build for host glibc only
bash src/rust_exporter/build.sh        # same options
```

The build wrapper uses `cargo-zigbuild` for cross-glibc compatibility.

---

## Configuration

`disk_checker_config.json` is created by `--init` and uses a flat team/user model:

```json
{
  "directory": "/data/shared",
  "output_file": "disk_usage_report.json",
  "teams": [
    { "name": "backend", "team_id": 1 },
    { "name": "frontend", "team_id": 2 }
  ],
  "users": [
    { "name": "alice", "team_id": 1 },
    { "name": "bob", "team_id": 1 }
  ]
}
```

---

## CLI Reference

### Configuration commands

| Command | Description |
|---|---|
| `--init --dir <path>` | Create `disk_checker_config.json` for a scan root. |
| `--dir <path>` | Update the scan directory in the current config. |
| `--add-team <name>` | Add a team. |
| `--add-user <u1> [u2 ...] --team <team>` | Add one or more users to a team. |
| `--remove-user <u1> [u2 ...]` | Remove users from the config. |
| `--list` | List all teams and users. |
| `--list --team <name>` | List users in one team. |

### Scan commands

| Command | Description |
|---|---|
| `--run` | Run a full scan and generate reports. |
| `--run --workers <N>` | Override Rust scan/build worker count (default: auto). |
| `--run --debug` | Print Phase 1/Phase 2 timing + RSS diagnostics. |
| `--run --user <u1> [u2 ...]` | Targeted scan: only the listed users are tracked at file detail. |
| `--run --output <file>` | Override the main summary report path. |
| `--run --output-dir <dir>` | Write all reports to a specific directory. |
| `--run --prefix <name>` | Prefix generated report filenames. |
| `--run --date` | Append `YYYYMMDD` to output filenames. |
| `--run --tree-map` | Build `tree_map_data/treemap.db` for dashboard TreeMap visualization. |
| `--run --level <N>` | Maximum TreeMap depth (default: `3`). |
| `--run --webhook-url <URL>` | POST a Microsoft Teams summary on completion. |

`--prefix`, `--date`, and `--output-dir` can be combined:

```bash
python3 disk_checker.py --run --output-dir /reports --prefix sda1 --date
# /reports/sda1_disk_usage_report_YYYYMMDD.json
```

### Remote sync commands

| Command | Description |
|---|---|
| `--run --sync --sync-user <U> --sync-host <H> --sync-dest-dir <D>` | Sync generated reports to a remote directory over SSH. |
| `--run --sync ... --sync-pass <pwd>` | Use password auth via `sshpass` (key-based SSH preferred). |

What gets synced (only artifacts produced by this run):

- Main JSON: `disk_usage_report.json`
- Sibling reports: `inode_usage_report.json`, `permission_issues.db`
- Detail directory: `detail_users/` (contains `data_detail.db`)
- TreeMap directory: `tree_map_data/` (contains `treemap.db`, when `--tree-map`)
- Heartbeat: `scan_status.json` (every 30s + at phase changes + on completion/error)

Both file and directory syncs use staging+rename for atomicity. Ctrl+C will:
1. Push a final `error / interrupted` heartbeat.
2. Drain pending sync workers (kill in-flight tar/ssh subprocesses).
3. Sweep `<dest>/.__staging__.*` files and `<dest>/*.__staging__/`, `<dest>/*.__old__/` directories on the remote.

### Inspecting reports

| Command | Description |
|---|---|
| `--show-report --files <report.json>` | Display a single summary report. |
| `--show-report --files "disk_usage_*.json"` | Match multiple reports by wildcard. |
| `--show-report --files <a.json> <b.json> --compare-by growth` | Compare two reports by growth rate (default). |
| `--show-report --files <a.json> <b.json> --compare-by usage` | Compare by total usage. |
| `--show-report --files ... --user <u1> [u2 ...]` | Filter displayed users. |

### Per-user detail: `--detail`

Display per-user breakdown from `data_detail.db`. Requires `--user` to specify user(s).

```bash
python3 disk_checker.py --detail --user alice --output-dir /reports
```

| Option | Default | Description |
|---|---|---|
| `--user <u1> [u2 ...]` | required | User(s) to display. |
| `--type <choice>` | `report` | Section to display (see below). |
| `--top <N>` | 30 | Max rows in each table. |
| `--search <keyword>` | (none) | Filter results to entries whose path contains keyword (case-insensitive). Matching text is highlighted in bold yellow on TTY output. |
| `--output-dir <dir>` | `.` | Directory containing `detail_users/data_detail.db`. |

**`--type` choices:**

| Type | Displays |
|---|---|
| `report` (default) | Directory breakdown + largest files |
| `dirs` | Directory breakdown only (sorted by size) |
| `files` | Largest files only |
| `inode` | Directory breakdown sorted by file count |
| `permission` | Permission errors from `permission_issues.db` |

**Examples:**

```bash
# Default: dirs + files
python3 disk_checker.py --detail --user alice --output-dir /reports

# Only directory breakdown, filter by keyword
python3 disk_checker.py --detail --user alice --output-dir /reports --type dirs --search "backup"

# File count per directory
python3 disk_checker.py --detail --user alice --output-dir /reports --type inode

# Permission errors matching a path
python3 disk_checker.py --detail --user alice --output-dir /reports --type permission --search "/home"

# Multiple users
python3 disk_checker.py --detail --user alice bob --output-dir /reports --type files --top 50
```

### Directory tree: `--tree-show`

Render an ASCII directory tree from `tree_map_data/treemap.db`. Requires `--run --tree-map` to have been run first.

```bash
python3 disk_checker.py --tree-show --output-dir /reports
```

| Option | Default | Description |
|---|---|---|
| `--user <u1> [u2 ...]` | (none) | Filter by user(s). Without `--user`, shows total dir sizes. Multiple users render separate trees. |
| `--path <PATH>` | scan root | Start tree from a specific path. |
| `--level <N>` | 3 | Maximum tree depth. |
| `--limit <N>` | 20 | Maximum child folders per level. |
| `--search <keyword>` | (none) | Show only branches containing dirs matching keyword. Matching dir names are highlighted in bold yellow on TTY output. |
| `--output-dir <dir>` | `.` | Directory containing `tree_map_data/treemap.db`. |

**Examples:**

```bash
# Full tree (all users, total sizes)
python3 disk_checker.py --tree-show --output-dir /reports --level 3 --limit 10

# Tree for a specific user
python3 disk_checker.py --tree-show --output-dir /reports --user alice --level 4

# Multiple users (separate tree per user)
python3 disk_checker.py --tree-show --output-dir /reports --user alice bob --level 3

# Search for directories matching keyword
python3 disk_checker.py --tree-show --output-dir /reports --search "backup" --level 5

# Start from a specific path
python3 disk_checker.py --tree-show --output-dir /reports --path /data/projects --level 2

# User + search + custom path
python3 disk_checker.py --tree-show --output-dir /reports --user alice --search "data" --path /home --level 3
```

**Sample output:**

```
============================================================
TREE-SHOW - alice
============================================================
Root path: /data/shared
User 'alice': 11.30 TB, 1,234,567 files
Search: 'backup'
Depth: 3  |  Limit: 10 per level

|-- projects  [8.5 TB, 800,000 files]
|   |-- ml_data  [6.0 TB, 600,000 files]
|   \-- backup  [2.5 TB, 200,000 files]    ← highlighted in yellow on TTY
\-- archive  [2.8 TB, 434,567 files]
    \-- backup_2024  [2.8 TB, 434,567 files]    ← highlighted in yellow on TTY
```

### Per-user text export

`scripts/export_user_reports.py` reads the SQLite databases produced by `--run` and writes plain-text usage files:

```bash
python3 scripts/export_user_reports.py \
  --input-dir /reports \
  --output-dir /reports/exports \
  --workers 4
```

The Rust exporter (`export_rust.abi3.so`) joins `detail_users/data_detail.db` with `tree_map_data/treemap.db` via SQLite `ATTACH` to resolve full paths, processing users in parallel via Rayon.

---

## Output Layout

```text
<output_dir>/
├── disk_usage_report.json         # main summary (general system + team + user usage)
├── inode_usage_report.json        # inode counts per user
├── permission_issues.db           # access-error report (indexed)
├── scan_status.json               # heartbeat (running stage, elapsed, message, …)
├── detail_users/
│   └── data_detail.db             # per-user files/dirs/exts, indexed
└── tree_map_data/                 # only when --tree-map
    └── treemap.db                 # path dictionary + directory tree
```

When `--run` is executed without `--tree-map`, any stale `tree_map_data/treemap.db` from a prior run is auto-cleaned. Stale legacy `tree_map_report.json`, `permission_issues.json`, and `*.ndjson` files in `detail_users/` are also swept.

### Database schemas

`data_detail.db` (application_id = `0xC0DD15D1`):

| Table | Purpose |
|---|---|
| `meta` | scan_root, scan_timestamp, total_files, total_dirs, total_size, treemap_db |
| `users` | uid, username, team_id, totals, permission_issues, is_target |
| `files` | dir_id, name_id, ext_id, uid, size — primary detail rows |
| `names` | file basename dictionary |
| `exts` | extension dictionary |
| `dir_user_size` | per-(uid, dir_id) size + file count for fast roll-ups |
| `top_files` | top-K file ids per user, ranked by size |

Indexes: `ix_files_uid_size`, `ix_files_uid_ext_size`, `ix_files_name_uid`, `ix_dus_uid_size`.

`treemap.db` (application_id = `0xC0DD15C0`):

| Table | Purpose |
|---|---|
| `meta` | scan_root, scan_timestamp, max_level, total_size, total_dirs |
| `names` | directory segment dictionary |
| `dirs` | id, parent_id, name_id, total_size, file_count, dir_count, owner_uid, has_files |
| `owners` | uid → username |

Index: `ix_dirs_parent_size` (covers `WHERE parent_id=? ORDER BY total_size DESC`).

Frontend queries that need full paths `ATTACH treemap.db AS tm` and walk `tm.dirs.parent_id` recursively, joined with `tm.names`.

---

## Architecture

### Pipeline phases

```
        ┌─────────────────────────┐
        │  Phase 1: Rust scan     │  fast_scanner.scan_disk()
        │  Bounded-queue walker   │  → /tmp/checkdisk_rust_*/scan_t*_b*.bin
        │  (200K dir cap, 64w)    │     (lz4-compressed binary spill)
        │  ThreadLocalState +     │  → /tmp/.../perm_t*.tsv  diragg_t*.bin
        │  spill BufWriters       │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Python: write summary  │  ReportGenerator.generate_report()
        │  JSON reports           │  → disk_usage_report.json + siblings
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Phase 2: Rust pipeline │  fast_scanner.build_pipeline()
        │  Rayon-parallel ingest  │  → spool .rows files
        │  Path tree assembly     │  → detail.db build dir
        │  Detail.db files insert │  → treemap.db (parallel thread)
        │  ANALYZE + rename       │  → atomic rename to final paths
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Phase 3: TreeMap join  │  generate_tree_map() (verifies result)
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Phase 4: drain sync    │  AsyncSyncPipeline.wait() / close()
        │  + final heartbeat      │  + cleanup_remote_staging() on abort
        └─────────────────────────┘
```

### Rust crates

| Crate | Module | Purpose |
|---|---|---|
| `src/rust_scanner/` | `lib.rs` | PyO3 entry points: `scan_disk`, `build_pipeline` |
| | `scan_core.rs` | Phase 1 bounded-queue parallel walker (panic-safe via `DoneGuard`) |
| | `scan_state.rs` | Per-thread scan buffers + lz4-compressed spill writers |
| | `scan_constants.rs` | Critical-skip names, flush thresholds, binary magic numbers |
| | `scan_utils.rs` | Number / size / rate formatting + `/proc/self/status` RSS |
| | `report_pipeline.rs` | Phase 2 ingest → path tree → detail.db + treemap.db |
| | `db_writer.rs` | DDL, bulk insert, `ANALYZE`, atomic rename |
| | `pipe_events.rs` | Binary spill format reader for scan + dir aggregates |
| | `pipe_io.rs` | Path safety helpers (`ensure_dir`, `recreate_dir`, …) |
| | `pipe_permission.rs` | Permission TSV → permission_issues.db |
| | `pipe_treemap.rs` | Path normalization helpers |
| | `pipe_types.rs` | Shared types + `extension_for_path`, `parent_path` |
| `src/rust_exporter/` | `lib.rs` | `export_rust.process_jobs` — Rayon parallel TXT export |

### Python modules

| Module | Description |
|---|---|
| `disk_checker.py` | CLI dispatcher: `cmd_init`, `cmd_run`, `cmd_detail`, `cmd_tree_show`, … |
| `src/cli_interface.py` | argparse setup + `--show-report` / `--detail` / `--tree-show` rendering |
| `src/config_manager.py` | Config CRUD: teams, users, scan directory |
| `src/disk_scanner.py` | Wraps `fast_scanner.scan_disk`, classifies usage, builds `ScanResult` |
| `src/report_generator.py` | Writes JSON summaries, invokes `fast_scanner.build_pipeline` |
| `src/sync_manager.py` | `AsyncSyncPipeline` + atomic tar-stream sync over SSH |
| `src/scan_status.py` | Atomic `scan_status.json` writer + heartbeat thread |
| `src/formatters/` | Terminal table rendering (summary, comparison, detail, tree) |
| `src/msteams_notifier.py` | Adaptive Card webhook for MS Teams |
| `src/utils.py` | Size formatting, UID resolution, JSON helpers |
| `src/constants.py` | Filenames, directory names, heartbeat intervals |

### Scripts

| Script | Description |
|---|---|
| `scripts/export_user_reports.py` | Per-user TXT export via `export_rust` (parallel) |

---

## Reliability + Failure Modes

### Bounded RAM

| Source | Bound |
|---|---|
| Phase 1 queue | `crossbeam_channel::bounded(200_000)` dirs |
| Phase 1 per-worker event buffer | `SCAN_EVENT_FLUSH_BYTES_THRESHOLD` (lz4-compressed spill) |
| Phase 2 row aggregation | `ROW_SPILL_THRESHOLD = 200_000` rows → `.rows` spill |
| `users` HashMap | Sized to actual user count |
| File basename dict (`names`) | Interned once; freed after stream |

Use `--debug` to print Phase 1 / Phase 2 profiles and peak RSS.

### Hang protection

| Risk | Protection |
|---|---|
| Walker panic mid-scan | `DoneGuard` RAII flips the `done` flag on drop, releasing the progress loop |
| Tar producer hangs after ssh fails | `_drain_tar_proc` closes stdout, `wait(5s)`, then escalates to `kill` |
| Future stuck forever | Streams have no Python timeout; SSH `ServerAliveInterval=30` × `CountMax=3` drops dead connections in ~90s |
| `/tmp` orphan accumulation | `_cleanup_orphan_tmpdirs()` sweeps `checkdisk_rust_*` at the start of every run |
| Ctrl+C mid-sync | `signal_handler` raises `KeyboardInterrupt` so `cmd_run`'s `except` block flushes status, drains the pipeline, and sweeps remote staging |
| Partial DB write | `finalize_db` deletes `<final>.tmp.db` on any error before propagating |

### What survives an abort

After `Ctrl+C` mid-sync:

| Artifact | State on remote |
|---|---|
| Main report files already promoted | preserved (atomic) |
| `<file>.__staging__.<pid>` partial files | swept |
| `<dir>.__staging__/` partial directories | swept |
| `<dir>.__old__/` rotation backups | swept |
| `scan_status.json` | `running=false, stage="error", message="Scan interrupted by user"` |

---

## Companion Dashboard

`check_disk` feeds the `disk_usage` web dashboard.

Recommended report root layout (mirrors what sync produces on the remote):

```text
<report_root>/disk_usage_report*.json
<report_root>/permission_issues.db
<report_root>/inode_usage_report*.json
<report_root>/scan_status.json
<report_root>/detail_users/data_detail.db
<report_root>/tree_map_data/treemap.db    # when --tree-map
```

Point `disk_usage/disks.json` entries at directories containing these files. The dashboard `ATTACH`es `treemap.db` from `data_detail.db` for path resolution and uses indexed `ORDER BY size DESC` queries against `files` for paginated detail views.

---

## Performance Notes

### Benchmarks (48.7M files, 7.67M dirs, 56.7 TB, 131 users, 64 workers)

| Phase | Time | RAM peak |
|---|---|---|
| Phase 1 (scan) | ~460s | ~2.5 GB |
| Phase 2 (SQLite build) | ~370s | ~13 GB |
| **Total** | **~830s** | — |

### Phase 1 optimizations

- **Bounded-queue walker**: caps queue at 200K dirs → Phase 1 RAM from ~8.9 GB to ~2.5 GB (-72%).
- **lz4 spill compression**: spill files compressed ~11× (9 GB → 800 MB).
- **Inline DFS fallback**: when queue is full, workers process subtrees locally without blocking.

### Phase 2 optimizations

- `journal_mode=OFF` + `synchronous=OFF` + `cache_size=-1_048_576` (1 GiB) + bulk transactions.
- `PAGE_SIZE=16384` reduces B-tree levels for large index builds.
- UTF-8 fast path in spill reader skips redundant allocations for valid UTF-8 paths.
- TreeMap is built on a parallel thread alongside `detail.db` finalize.
- `VACUUM INTO` is skipped for DBs outside the `[100 MB, 1 GB]` range.

### Progress output

While scanning, Phase 1 prints a snapshot line every 10 seconds:

```
[HH:MM:SS] Files: 12,345 | Dirs: 678 | Size: 90 GB | Rate: 5,432 files/s | Mem: 145.2 MB
```

Phase 2 prints discrete stage markers (no inline `\r`):

```
[Phase 2] Ingesting and mapping 16 event streams...
[Phase 2] Loading 16 Phase 1 directory aggregate shards...
[Phase 2] Building path tree for 152340 directories...
[Phase 2] Building user detail for 87 users...
[Phase 2]   user detail: 87/87 (100%)
[Phase 2] Building treemap (12480 dirs, max_level 3) in background...
[Phase 2] Finalizing detail.db (index + vacuum)...
[Phase 2] Waiting for treemap build to finish...
```

Use `--debug` to print Phase 1 / Phase 2 profiles + peak RSS.

---

## Verification

After code changes:

```bash
# Cargo type-check
cargo check --manifest-path src/rust_scanner/Cargo.toml --message-format short
cargo check --manifest-path src/rust_exporter/Cargo.toml --message-format short

# Python tests
python3 -m pytest tests/ -q
```

Rebuild Rust artifacts for distribution:

```bash
bash src/rust_scanner/build.sh
bash src/rust_exporter/build.sh
```

The build wrapper strips the `.so` and verifies glibc requirements via `objdump -p`.
