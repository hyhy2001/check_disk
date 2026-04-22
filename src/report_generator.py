"""
Report Generator Module

Handles generating and saving disk usage reports.
"""

import os
import json
import heapq
import sqlite3
import glob as glob_module
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional, List, Tuple

from src.disk_scanner import ScanResult
from src.utils import format_size, save_json_report, ScanHelper
from src.tree_map_generator import TreeMapGenerator

try:
    from src import fast_scanner as _fast_scanner
    HAS_RUST_PHASE2 = hasattr(_fast_scanner, 'merge_write_user_report')
    HAS_RUST_PHASE2_DB = hasattr(_fast_scanner, 'merge_write_user_report_db')
except ImportError:
    _fast_scanner = None  # type: ignore
    HAS_RUST_PHASE2 = False
    HAS_RUST_PHASE2_DB = False


class ReportGenerator:
    """Generates and saves disk usage reports."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the report generator.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.output_file = config.get("output_file", "disk_usage_report.json")
        detail_fts = str(config.get("detail_fts", "off")).strip().lower()
        self.enable_detail_fts = detail_fts not in ("off", "0", "false", "no")
        detail_size_index = str(config.get("detail_size_index", "off")).strip().lower()
        self.enable_detail_size_index = detail_size_index not in ("off", "0", "false", "no")

    # ------------------------------------------------------------------ #
    # Path helpers                                                         #
    # ------------------------------------------------------------------ #

    def _get_output_filename(self, base_filename: str) -> str:
        """
        Generate an output filename using the same prefix as the main output file.
        Sibling reports (permission_issues, check_user) never include a date suffix.

        Args:
            base_filename: Base name without extension (e.g. 'permission_issues')

        Returns:
            Full output path (e.g. '/reports/sda1_permission_issues.json')
        """
        dir_part = os.path.dirname(self.output_file)
        prefix = self.config.get('output_prefix', '')

        parts = [p for p in [prefix, base_filename] if p]
        new_filename = '_'.join(parts) + '.json'

        return os.path.join(dir_part, new_filename) if dir_part else new_filename

    def _get_user_detail_filename(self, base: str, user: str, ext: str = "ndjson") -> str:
        """
        Build path for a per-user detail report inside the detail_users/ subdir.
        Never includes a date suffix.

        Args:
            base: Middle segment, e.g. 'detail_report_dir' or 'detail_report_file'
            user: Username

        Returns:
            Full output path, e.g. '/reports/detail_users/sda1_detail_report_dir_Binh.json'
        """
        dir_part = os.path.dirname(self.output_file)
        detail_dir = os.path.join(dir_part, "detail_users") if dir_part else "detail_users"
        prefix = self.config.get("output_prefix", "")
        parts = [p for p in [prefix, base, user] if p]
        ext = (ext or "ndjson").lstrip(".")
        fname = "_".join(parts) + "." + ext
        return os.path.join(detail_dir, fname)

    def clear_old_detail_reports(self) -> None:
        """
        Remove all old JSON files in the detail_users directory before scanning.
        """
        dir_part = os.path.dirname(self.output_file)
        detail_dir = os.path.join(dir_part, "detail_users") if dir_part else "detail_users"
        if os.path.exists(detail_dir):
            import glob
            old_files = (
                glob.glob(os.path.join(detail_dir, "*.json")) +
                glob.glob(os.path.join(detail_dir, "*.ndjson")) +
                glob.glob(os.path.join(detail_dir, "*.db"))
            )
            if old_files:
                print(f"Cleaning up {len(old_files)} old output files in {detail_dir}...")
                for fpath in old_files:
                    try:
                        os.remove(fpath)
                    except OSError:
                        pass

    # ------------------------------------------------------------------ #
    # Legacy helpers                                                       #
    # ------------------------------------------------------------------ #

    def _is_valid_date(self, date_str: str) -> bool:
        """Check if a string is a valid date in YYYYMMDD format."""
        if len(date_str) != 8:
            return False
        try:
            year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
            return (1900 <= year <= 2100) and (1 <= month <= 12) and (1 <= day <= 31)
        except ValueError:
            return False

    def _build_team_id_maps(self):
        """Build team_name -> team_id and username -> team_id lookup dicts from config."""
        team_id_map = {t["name"]: t["team_id"] for t in self.config.get("teams", [])}
        user_team_id_map = {}
        for user in self.config.get("users", []):
            user_team_id_map[user["name"]] = user["team_id"]
        return team_id_map, user_team_id_map

    # ------------------------------------------------------------------ #
    # Main summary report                                                  #
    # ------------------------------------------------------------------ #

    def generate_report(self, scan_result: Optional[ScanResult] = None) -> Dict[str, Any]:
        """
        Generate a report from scan results.

        Args:
            scan_result: ScanResult object with disk usage data, or None

        Returns:
            Dictionary containing the report data
        """
        if scan_result is None:
            print("Warning: No scan results provided. Generating empty report.")
            report = {
                "date": int(time.time()),
                "directory": self.config.get("directory", ""),
                "general_system": {"total": 0, "used": 0, "available": 0},
                "team_usage": [],
                "user_usage": [],
                "other_usage": []
            }
        else:
            team_id_map, user_team_id_map = self._build_team_id_maps()

            # Inject team_id into each team entry
            team_usage = []
            for t in scan_result.team_usage:
                entry = dict(t)
                tid = team_id_map.get(t["name"])
                if tid is not None:
                    entry["team_id"] = tid
                team_usage.append(entry)

            # Inject team_id into each user entry
            user_usage = []
            for u in scan_result.user_usage:
                entry = dict(u)
                tid = user_team_id_map.get(u["name"])
                if tid is not None:
                    entry["team_id"] = tid
                user_usage.append(entry)

            filtered_general_system = {
                k: v for k, v in scan_result.general_system.items()
                if not k.startswith("inodes_")
            }

            report = {
                "date": scan_result.timestamp,
                "directory": self.config.get("directory", ""),
                "general_system": filtered_general_system,
                "team_usage": team_usage,
                "user_usage": user_usage,
                "other_usage": scan_result.other_usage
            }

            if hasattr(scan_result, 'permission_issues'):
                self.generate_permission_issues_report(scan_result)
            
            if hasattr(scan_result, 'user_inodes'):
                self.generate_inode_report(scan_result)

        save_json_report(report, self.output_file)
        return report

    # ------------------------------------------------------------------ #
    # Permission issues report                                             #
    # ------------------------------------------------------------------ #

    def generate_permission_issues_report(self, scan_result: ScanResult) -> Dict[str, Any]:
        """
        Generate a report for permission issues.

        Args:
            scan_result: ScanResult object with disk usage data

        Returns:
            Dictionary containing the report data
        """
        report = {
            "date": scan_result.timestamp,
            "directory": self.config.get("directory", ""),
            "general_system": scan_result.general_system,
            "permission_issues": scan_result.permission_issues
        }

        output_path = self._get_output_filename("permission_issues")
        save_json_report(report, output_path)
        print(f"Permission issues report saved to: {output_path}")

        return report

    # ------------------------------------------------------------------ #
    # Inode usage report                                                   #
    # ------------------------------------------------------------------ #

    def generate_inode_report(self, scan_result: ScanResult) -> Dict[str, Any]:
        """
        Generate a report for inode usage (files count).

        Args:
            scan_result: ScanResult object with disk usage data

        Returns:
            Dictionary containing the report data
        """
        report = {
            "date": scan_result.timestamp,
            "directory": self.config.get("directory", ""),
            "inodes_total": scan_result.general_system.get("inodes_total", 0),
            "inodes_used": scan_result.general_system.get("inodes_used", 0),
            "inodes_free": scan_result.general_system.get("inodes_free", 0),
            "inodes_scanned": scan_result.general_system.get("inodes_scanned", 0),
            "users": scan_result.user_inodes
        }

        output_path = self._get_output_filename("inode_usage_report")
        save_json_report(report, output_path)
        print(f"Inode usage report saved to: {output_path}")

        return report

    # ------------------------------------------------------------------ #
    # TreeMap report                                                       #
    # ------------------------------------------------------------------ #

    def generate_tree_map(
        self,
        scan_result: ScanResult,
        level: int = 3,
        max_workers: Optional[int] = None,
    ) -> str:
        """
        Generate a TreeMap JSON report for directory visualization.

        Args:
            scan_result: ScanResult object with disk usage data
            level: Maximum depth level for the tree

        Returns:
            The output path that was written.
        """
        # Resolve output path
        output_path = self._get_output_filename("tree_map_report")

        # Prefer direct map produced by scanner to avoid re-materializing
        # from top_dir (saves Phase-3 memory/CPU on very large scans).
        dir_sizes_map = scan_result.dir_sizes_map if scan_result.dir_sizes_map else {}
        if not dir_sizes_map:
            # Backward-compatible fallback path
            for entry in scan_result.top_dir:
                d = entry['dir']
                u = entry['user']
                s = entry['user_usage']
                if d not in dir_sizes_map:
                    dir_sizes_map[d] = {}
                dir_sizes_map[d][u] = dir_sizes_map[d].get(u, 0) + s

        # Build dir_owner_map: path -> dominant user (by size) from Phase 1 data.
        # This lets Phase 3 skip os.stat() syscalls entirely.
        dir_owner_map: Dict[str, str] = {}
        for dpath, user_sizes in dir_sizes_map.items():
            if user_sizes:
                dir_owner_map[os.path.abspath(dpath)] = max(user_sizes, key=user_sizes.get)

        tree_workers = int(max_workers if max_workers is not None else self.config.get("workers", 4))

        generator = TreeMapGenerator(
            root_dir=self.config.get("directory", "/"),
            dir_sizes=dir_sizes_map,
            max_level=level,
            max_workers=max(1, tree_workers),
            dir_owner_map=dir_owner_map,
        )

        generator.save(output_path)
        return output_path

    # ------------------------------------------------------------------ #
    # Streaming file-detail report writer                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _iter_uid_tsv(tmpdir: str, uids: List[int]):
        """Yield (size, path) tuples from all temp TSV chunks for all *uids*.

        Each chunk is sorted descending (written that way by _flush_thread_paths).
        Using ``heapq.merge(..., reverse=True)`` over these iterators gives a
        globally sorted descending stream with O(num_chunks) RAM.

        Opens files with ``errors='surrogateescape'`` to handle paths that
        contain non-UTF-8 byte sequences (common on Linux filesystems).
        """
        def _read_chunk(fpath: str):
            with open(fpath, encoding='utf-8', errors='surrogateescape') as fh:
                for line in fh:
                    line = line.rstrip('\n')
                    if not line:
                        continue
                    tab = line.find('\t')
                    if tab < 0:
                        continue  # skip malformed lines
                    yield int(line[:tab]), line[tab + 1:]

        chunk_files = []
        for uid in uids:
            chunk_files.extend(glob_module.glob(os.path.join(tmpdir, f'uid_{uid}_t*.tsv')))
        chunk_files.sort()
        
        if not chunk_files:
            return

        yield from heapq.merge(*[_read_chunk(f) for f in chunk_files], reverse=True)

    def _stream_write_file_report(
        self,
        tmpdir: str,
        uids: List[int],
        username: str,
        output_path: str,
        timestamp: int,
    ) -> str:
        """Write a complete per-user file detail JSON by streaming from temp files.

        Tries the native Rust merge_write_user_report() for maximum performance
        (single BufRead pass, no GIL). Falls back to Python heapq.merge path
        if the Rust extension is unavailable.

        Returns:
            The output path that was written.
        """
        # ── Rust fast path ────────────────────────────────────────────────────
        if HAS_RUST_PHASE2:
            try:
                _fast_scanner.merge_write_user_report(
                    tmpdir, uids, username, output_path, timestamp
                )
                return output_path
            except Exception as e:
                print(f"  [warn] Rust Phase 2 failed for {username!r}, falling back to Python: {e}")

        # ── Python fallback ───────────────────────────────────────────────────
        def _safe_path(p: str) -> str:
            """Replace surrogate chars with U+FFFD so json.dumps never raises."""
            return p.encode('utf-8', errors='replace').decode('utf-8')

        # Pass 1: totals
        total_files = 0
        total_used = 0
        for size, _ in self._iter_uid_tsv(tmpdir, uids):
            total_files += 1
            total_used += size

        # Pass 2: stream write
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as out:
            meta = {
                "_meta": {
                    "date": timestamp,
                    "user": _safe_path(username),
                    "total_files": total_files,
                    "total_used": total_used
                }
            }
            out.write(json.dumps(meta, separators=(',', ':')) + '\n')

            for size, path in self._iter_uid_tsv(tmpdir, uids):
                safe_path = _safe_path(path)
                xt = os.path.splitext(safe_path)[1][1:]
                line = {
                    "path": safe_path,
                    "size": size,
                    "xt": xt
                }
                out.write(json.dumps(line, separators=(',', ':')) + '\n')

        return output_path

    def _write_dir_report_db(
        self,
        user: str,
        scan_result: ScanResult,
        dirs: List[Dict[str, Any]],
        output_path: str,
        total_dir_used: int,
    ) -> str:
        """Write per-user directory detail.

        Preferred (combined) path: appends dirs_data + VIEW dirs + meta_dirs
        into the sibling file_db, reusing its dirs_index — no redundant paths.

        Fallback (standalone) path: legacy separate DB with full paths stored.
        Returns '' when combined mode succeeds (no standalone file created).
        """
        # Try combined mode: append dirs into the sibling file_db.
        sibling = output_path.replace("detail_report_dir_", "detail_report_file_", 1)
        if sibling != output_path and os.path.isfile(sibling):
            try:
                if self._write_dirs_into_file_db(user, scan_result, dirs, sibling, total_dir_used):
                    return ""  # combined success — no standalone dir file needed
            except Exception as e:
                print(f"  [warn] Combined dir merge failed for {user!r}: {e}")

        # Fallback: standalone dirs DB with full paths (legacy schema).
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if os.path.exists(output_path):
            os.remove(output_path)
        conn = sqlite3.connect(output_path)
        conn.execute("PRAGMA page_size = 8192")
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("CREATE TABLE meta (date INTEGER, user TEXT, total_items INTEGER, total_used INTEGER)")
        conn.execute("CREATE TABLE dirs (id INTEGER PRIMARY KEY, path TEXT, used INTEGER)")
        conn.execute(
            "INSERT INTO meta(date, user, total_items, total_used) VALUES (?, ?, ?, ?)",
            (scan_result.timestamp, user, len(dirs), int(total_dir_used)),
        )
        rows = [(d['dir'], int(d['user_usage'])) for d in sorted(dirs, key=lambda x: x['user_usage'], reverse=True)]
        if rows:
            conn.executemany("INSERT INTO dirs(path, used) VALUES (?, ?)", rows)
        conn.commit()
        conn.close()
        return output_path

    def _write_dirs_into_file_db(
        self,
        user: str,
        scan_result: ScanResult,
        dirs: List[Dict[str, Any]],
        file_db_path: str,
        total_dir_used: int,
    ) -> bool:
        """Append dirs_data + VIEW dirs + meta_dirs to the existing combined file_db.

        The combined file_db (written by Rust) contains a dirs_index table mapping
        id → path.  dirs_data stores only (dir_id, used) integer pairs, sharing
        that index — zero redundant path strings for dir entries.

        Returns True on success; False when file_db lacks dirs_index (old schema).
        """
        conn = sqlite3.connect(file_db_path)
        try:
            # Only proceed when Rust/new-schema file_db has dirs_index.
            has_idx = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dirs_index'"
            ).fetchone() is not None
            if not has_idx:
                return False

            conn.execute("PRAGMA journal_mode = OFF")
            conn.execute("PRAGMA synchronous = OFF")

            # dirs_data: integer-only table sharing dirs_index for path storage.
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dirs_data "
                "(dir_id INTEGER PRIMARY KEY, used INTEGER NOT NULL)"
            )

            # VIEW dirs: exposes (path, used) for backward-compat PHP queries.
            conn.execute(
                "CREATE VIEW IF NOT EXISTS dirs AS "
                "SELECT di.path AS path, dd.used AS used "
                "FROM dirs_data dd "
                "JOIN dirs_index di ON dd.dir_id = di.id"
            )

            # meta_dirs: dirs-specific metadata, separate from files meta.
            conn.execute(
                "CREATE TABLE IF NOT EXISTS meta_dirs "
                "(date INTEGER, user TEXT, total_dirs INTEGER, total_used INTEGER)"
            )
            conn.execute(
                "INSERT INTO meta_dirs(date, user, total_dirs, total_used) VALUES (?, ?, ?, ?)",
                (scan_result.timestamp, user, len(dirs), int(total_dir_used)),
            )

            # Sort by used DESC (consistent with old standalone schema ordering).
            dir_rows = [
                (d['dir'], int(d['user_usage']))
                for d in sorted(dirs, key=lambda x: x['user_usage'], reverse=True)
            ]

            # Ensure all dir paths exist in dirs_index (covers empty dirs).
            conn.executemany(
                "INSERT OR IGNORE INTO dirs_index(path) VALUES (?)",
                [(p,) for p, _ in dir_rows],
            )

            # Bulk insert via temp table JOIN — avoids N individual path lookups.
            conn.execute("CREATE TEMP TABLE _dt (path TEXT, used INTEGER)")
            conn.executemany("INSERT INTO _dt VALUES (?, ?)", dir_rows)
            conn.execute(
                "INSERT OR REPLACE INTO dirs_data(dir_id, used) "
                "SELECT di.id, t.used FROM _dt t "
                "JOIN dirs_index di ON di.path = t.path"
            )
            conn.execute("DROP TABLE _dt")
            conn.commit()

            # ── Critical performance indexes ──────────────────────────────────
            # Built AFTER bulk insert (much faster than maintaining during inserts).
            # idx_dirs_data_used: ORDER BY dd.used DESC → only reads N rows, no temp sort.
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dirs_data_used ON dirs_data(used DESC)"
            )
            # idx_files_data_size: ORDER BY fd.size DESC → only reads N rows, no temp sort.
            if self.enable_detail_size_index:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_files_data_size ON files_data(size DESC)"
                )
            conn.commit()

            # ── Optional FTS4 full-text search indexes ────────────────────────
            # Use FTS4 (not FTS5) so PHP's bundled SQLite 3.8.x can read tables.
            # When disabled, frontend search falls back to LIKE queries.
            if self.enable_detail_fts:
                try:
                    conn.execute(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_dirs USING fts4("
                        "path, content='dirs_index', tokenize='unicode61')"
                    )
                    conn.execute(
                        "INSERT INTO fts_dirs(docid, path) SELECT id, path FROM dirs_index"
                    )
                    conn.execute(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_files USING fts4("
                        "fullpath, tokenize='unicode61')"
                    )
                    conn.execute(
                        "INSERT INTO fts_files(docid, fullpath) "
                        "SELECT fd.id, di.path || '/' || fd.basename "
                        "FROM files_data fd JOIN dirs_index di ON fd.dir_id = di.id"
                    )
                    conn.commit()
                except Exception:
                    pass  # FTS4 unavailable → PHP falls back to LIKE automatically
            return True
        finally:
            conn.close()

    def _write_file_report_db_streaming(
        self,
        user: str,
        scan_result: ScanResult,
        output_path: str,
        uids: List[int],
    ) -> str:
        """Write per-user file detail report as SQLite DB from streaming temp chunks."""
        if HAS_RUST_PHASE2_DB:
            try:
                _fast_scanner.merge_write_user_report_db(
                    scan_result.detail_tmpdir,
                    uids,
                    user,
                    output_path,
                    scan_result.timestamp,
                    5000,
                )
                return output_path
            except Exception as e:
                print(f"  [warn] Rust Phase 2 DB failed for {user!r}, falling back to Python: {e}")

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if os.path.exists(output_path):
            os.remove(output_path)

        conn = sqlite3.connect(output_path)
        conn.execute("PRAGMA page_size = 8192")  # Fix 2: fresh file, set before any DDL
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("CREATE TABLE meta (date INTEGER, user TEXT, total_items INTEGER, total_used INTEGER)")
        conn.execute(
            "CREATE TABLE files ("
            "id INTEGER PRIMARY KEY, "
            "path TEXT, "
            "size INTEGER, "
            "xt TEXT)"
        )
        # Fix 1: idx_files_size removed; rows merge in size DESC order
        conn.execute("CREATE INDEX idx_files_xt_size ON files(xt, size DESC)")
        total_files = 0
        total_used = 0
        batch = []
        batch_size = 5000

        for size, path in self._iter_uid_tsv(scan_result.detail_tmpdir, uids):
            xt = os.path.splitext(path)[1][1:]
            xt = xt.lower()
            batch.append((
                path,
                int(size),
                xt,
            ))
            total_files += 1
            total_used += int(size)
            if len(batch) >= batch_size:
                conn.executemany(
                    "INSERT INTO files(path, size, xt) VALUES (?, ?, ?)",
                    batch
                )
                batch = []

        if batch:
            conn.executemany(
                "INSERT INTO files(path, size, xt) VALUES (?, ?, ?)",
                batch
            )
        conn.execute(
            "INSERT INTO meta(date, user, total_items, total_used) VALUES (?, ?, ?, ?)",
            (scan_result.timestamp, user, int(total_files), int(total_used)),
        )
        conn.commit()
        conn.close()
        return output_path

    def _write_file_report_db_legacy(
        self,
        user: str,
        scan_result: ScanResult,
        output_path: str,
        user_files: List[Tuple[str, int]],
    ) -> str:
        """Write per-user file detail report as SQLite DB from in-memory file list."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if os.path.exists(output_path):
            os.remove(output_path)

        conn = sqlite3.connect(output_path)
        conn.execute("PRAGMA page_size = 8192")  # Fix 2
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("CREATE TABLE meta (date INTEGER, user TEXT, total_items INTEGER, total_used INTEGER)")
        conn.execute(
            "CREATE TABLE files ("
            "id INTEGER PRIMARY KEY, "
            "path TEXT, "
            "size INTEGER, "
            "xt TEXT)"
        )
        # Fix 1: idx_files_size removed; legacy list is pre-sorted by size DESC
        conn.execute("CREATE INDEX idx_files_xt_size ON files(xt, size DESC)")
        total_used = sum(s for _, s in user_files)
        rows = []
        for p, s in user_files:
            xt = os.path.splitext(p)[1][1:]
            xt = xt.lower()
            rows.append((
                p,
                int(s),
                xt,
            ))
        if rows:
            conn.executemany(
                "INSERT INTO files(path, size, xt) VALUES (?, ?, ?)",
                rows
            )
        conn.execute(
            "INSERT INTO meta(date, user, total_items, total_used) VALUES (?, ?, ?, ?)",
            (scan_result.timestamp, user, len(user_files), int(total_used)),
        )
        conn.commit()
        conn.close()
        return output_path

    # ------------------------------------------------------------------ #
    # Per-user detail reports (parallel)                                   #
    # ------------------------------------------------------------------ #

    def _write_user_detail(
        self,
        user: str,
        scan_result: ScanResult,
        user_dirs: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[str, str]:
        """
        Write directory + file detail JSON reports for a single user.

        This method is designed to be called concurrently from multiple threads.
        Each user writes to its own pair of files so there are no shared-write
        race conditions.  The only shared state is ``os.makedirs(..., exist_ok=True)``
        which is safe under concurrent calls.

        Args:
            user: Username string
            scan_result: Full ScanResult from Phase 1

        Returns:
            Tuple (dir_path, file_path) of written files.
            Either element may be empty-string if that report was skipped.
        """
        # ── Gather dir data (collected before writing; used after file_db exists) ─
        dirs = user_dirs if user_dirs is not None else [e for e in scan_result.top_dir if e['user'] == user]
        total_dir_used = sum(d['user_usage'] for d in dirs)

        # ── File detail report FIRST — combined mode appends dirs into this DB ─
        file_path = self._get_user_detail_filename('detail_report_file', user, ext='db')
        streaming = bool(scan_result.detail_tmpdir)
        if streaming:
            # user key is either a real username ('nobody', 'mysql') or 'uid-{num}' for unknowns.
            # Fast path: uid-{num} format → parse directly.
            # Slow path: real username → reverse-scan uid_cache.
            if user.startswith("uid-") and user[4:].isdigit():
                uids = [int(user[4:])]
            else:
                uids = [u for u, n in scan_result.detail_uid_username.items() if n == user]
            if uids:
                self._write_file_report_db_streaming(user, scan_result, file_path, uids)
            else:
                print(f"  [warn] No UIDs found for user {user!r}, skipping file report")
                file_path = ""
        else:
            # Legacy in-memory path
            user_files = scan_result.detail_files.get(user, [])
            self._write_file_report_db_legacy(user, scan_result, file_path, user_files)

        # ── Directory detail report — tries combined append into file_db first ─
        dir_path = self._get_user_detail_filename('detail_report_dir', user, ext='db')
        self._write_dir_report_db(user, scan_result, dirs, dir_path, total_dir_used)

        return dir_path, file_path

    def generate_detail_reports(
        self,
        scan_result: ScanResult,
        max_workers: int = 1,
    ) -> List[str]:
        """
        Generate per-user directory and file detail reports.

        When ``scan_result.detail_tmpdir`` is set (streaming mode), file detail
        reports are written directly from temp TSV files without loading data
        into memory — suitable for disks with tens of millions of files.

        Otherwise falls back to the legacy in-memory path using
        ``scan_result.detail_files``.

        Args:
            scan_result:  ScanResult object from Phase 1.
            max_workers:  Number of threads for parallel user-report writing.
                          Pass ``scanner.max_workers`` to reuse Phase-1 concurrency.
                          Defaults to 1 (sequential, backward-compatible).

        Returns:
            Sorted list of created file paths.
        """
        # Build per-user dir list from dir_sizes_map (top_dir is deprecated).
        # This avoids a redundant data structure that cost ~2 GB RAM.
        user_dirs_map: Dict[str, List[Dict[str, Any]]] = {}
        for dpath, user_sizes in scan_result.dir_sizes_map.items():
            for user, size in user_sizes.items():
                user_dirs_map.setdefault(user, []).append({'dir': dpath, 'user_usage': size})
        users = sorted(user_dirs_map.keys())

        if not users:
            print("No users found in scan results — nothing to generate.")
            return []

        streaming = bool(scan_result.detail_tmpdir)
        mode_label = "streaming" if streaming else "in-memory"
        workers_label = f"parallel {max_workers}w" if max_workers > 1 else "sequential"
        print(f"Phase 2: Writing {len(users)} user detail reports "
              f"[{mode_label}, {workers_label}]...")

        created: List[str] = []
        failed: List[str] = []
        total_users = len(users)
        completed = 0

        # Phase 2 is I/O-bound (reading TSV chunks + writing SQLite).
        # Capping at 4 threads avoids disk thrashing when max_workers is high.
        effective_workers = max(1, min(int(max_workers), 4))
        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            futures = {
                executor.submit(self._write_user_detail, u, scan_result, user_dirs_map.get(u, [])): u
                for u in users
            }
            for fut in as_completed(futures):
                user = futures[fut]
                completed += 1
                
                try:
                    dir_p, file_p = fut.result()
                    if dir_p:
                        created.append(dir_p)
                    if file_p:
                        created.append(file_p)
                except Exception as exc:
                    failed.append(user)
                    print(f"  [warn] Failed report for {user!r}: {exc}")
                
                if completed % 20 == 0 or completed == total_users:
                    print(f"  [Phase 2] Progress: {completed}/{total_users} users completed...")

        if failed:
            print(f"  [warn] {len(failed)} user report(s) failed: {failed}")

        output_dir = os.path.dirname(created[0]) if created else '.'
        print(f"Generated {len(users)} user detail report(s) "
              f"[{mode_label}, {workers_label}] -> {output_dir}")

        return sorted(created)
