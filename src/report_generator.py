"""
Report Generator Module

Handles generating and saving disk usage reports.
"""

import os
import sqlite3
import tempfile
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from .disk_scanner import ScanResult
from .tree_map_generator import TreeMapGenerator
from .utils import format_size, save_json_report

try:
    from src import fast_scanner as _fast_scanner
    HAS_RUST_PHASE2_DB = hasattr(_fast_scanner, 'merge_write_user_report_db')
except ImportError:
    _fast_scanner = None  # type: ignore
    HAS_RUST_PHASE2_DB = False


@dataclass
class UserDetailTiming:
    user: str
    total_sec: float
    file_sec: float
    dir_sec: float
    file_db_mb: float
    dir_db_mb: float
    dir_items: int
    dir_total_used: int


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
        self.debug = bool(config.get("debug", False))
        base_batch = int(config.get("phase2_merge_batch", 50000) or 50000)
        self.phase2_merge_batch = max(1000, base_batch)
        large_batch = int(config.get("phase2_merge_batch_large", 100000) or 100000)
        self.phase2_merge_batch_large = max(self.phase2_merge_batch, large_batch)

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

    def cleanup_stale_detail_reports(self, keep_paths: List[str]) -> None:
        """
        Remove stale files in detail_users/ that were not regenerated in this run.

        This keeps sync incremental-friendly (we don't wipe all files up front),
        while still preventing orphan reports from users that no longer exist.
        """
        dir_part = os.path.dirname(self.output_file)
        detail_dir = os.path.join(dir_part, "detail_users") if dir_part else "detail_users"
        if not os.path.isdir(detail_dir):
            return

        keep_abs = {os.path.abspath(p) for p in keep_paths if p}
        # Keep sidecar files for current DBs too.
        keep_with_sidecars = set(keep_abs)
        for p in list(keep_abs):
            if p.endswith(".db"):
                keep_with_sidecars.add(p + "-wal")
                keep_with_sidecars.add(p + "-shm")

        removed = 0
        for name in os.listdir(detail_dir):
            if not (
                name.endswith(".json")
                or name.endswith(".ndjson")
                or name.endswith(".db")
                or name.endswith(".db-wal")
                or name.endswith(".db-shm")
            ):
                continue
            fp = os.path.abspath(os.path.join(detail_dir, name))
            if fp in keep_with_sidecars:
                continue
            try:
                os.remove(fp)
                removed += 1
            except OSError:
                pass

        if removed > 0:
            print(f"Cleaned up {removed} stale detail file(s) in {detail_dir}.")

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

        phase3_start = time.time()
        phase3_mem_start = self._get_rss_mb()
        print(f"[Phase 3] RAM before TreeMap: {phase3_mem_start:.1f} MB")

        # ── Fast path: Rust reads TSV directly (no PyO3 HashMap conversion) ──
        _has_rust_tsv = False
        try:
            from src import fast_scanner as _fs
            _has_rust_tsv = hasattr(_fs, 'generate_treemap_from_tsv')
        except ImportError:
            pass

        if _has_rust_tsv and scan_result.detail_tmpdir and scan_result.detail_uid_username:
            db_path = os.path.join(os.path.dirname(output_path), "tree_map_data.db")
            if os.path.exists(db_path):
                os.remove(db_path)
            count = _fs.generate_treemap_from_tsv(
                self.config.get("directory", "/"),
                scan_result.detail_tmpdir,
                scan_result.detail_uid_username,
                output_path,
                db_path,
                level,
                0,
            )
            phase3_mem_end = self._get_rss_mb()
            phase3_elapsed = time.time() - phase3_start
            print(
                f"Phase 3 [Rust TSV-direct]: {count:,} shards, "
                f"elapsed: {phase3_elapsed:.2f}s"
            )
            print(
                f"[Phase 3] RAM after TreeMap: {phase3_mem_end:.1f} MB "
                f"(delta: {phase3_mem_end - phase3_mem_start:+.1f} MB, elapsed: {phase3_elapsed:.2f}s)"
            )
            return output_path

        # ── Fallback: build dir_sizes_map in Python, pass to Rust/Python TreeMap ──
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
        phase3_mem_end = self._get_rss_mb()
        phase3_elapsed = time.time() - phase3_start
        print(
            f"[Phase 3] RAM after TreeMap: {phase3_mem_end:.1f} MB "
            f"(delta: {phase3_mem_end - phase3_mem_start:+.1f} MB, elapsed: {phase3_elapsed:.2f}s)"
        )
        return output_path

    # ------------------------------------------------------------------ #
    # Streaming file-detail report writer                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _iter_batched_pairs(
        dirs: Iterable[Union[Dict[str, Any], Tuple[str, int]]],
        batch_size: int = 5000,
    ):
        """Yield batched (path, used) tuples to keep peak memory bounded."""
        batch: List[Tuple[str, int]] = []
        for d in dirs:
            if isinstance(d, dict):
                path = d.get("dir")
                if not path:
                    continue
                used = int(d.get("user_usage", 0))
            else:
                if len(d) != 2:
                    continue
                path = d[0]
                if not path:
                    continue
                used = int(d[1])
            batch.append((path, used))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def _get_rss_mb() -> float:
        """Return current process RSS in MB (Linux)."""
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            return int(parts[1]) / 1024.0
        except OSError:
            pass
        return 0.0

    def _write_dir_report_db(
        self,
        user: str,
        scan_result: ScanResult,
        dirs: Iterable[Tuple[str, int]],
        output_path: str,
        total_dir_used: int,
        total_dir_count: int,
        dirs_factory: Optional[Callable[[], Iterable[Tuple[str, int]]]] = None,
    ) -> str:
        """Write per-user directory detail.

        Preferred (combined) path: appends dirs_data + VIEW dirs + meta_dirs
        into the sibling file_db, reusing its dirs_index — no redundant paths.

        Fallback (standalone) path: legacy separate DB with full paths stored.
        Returns '' when combined mode succeeds (no standalone file created).
        """
        combined_start = time.time()
        # Try combined mode: append dirs into the sibling file_db.
        sibling = output_path.replace("detail_report_dir_", "detail_report_file_", 1)
        if sibling != output_path and os.path.isfile(sibling):
            try:
                combined_dirs = dirs_factory() if dirs_factory is not None else dirs
                if self._write_dirs_into_file_db(
                    user,
                    scan_result,
                    combined_dirs,
                    sibling,
                    total_dir_used,
                    total_dir_count,
                ):
                    if self.debug:
                        print(f"  [Phase 2][dir] {user}: combined={time.time() - combined_start:.2f}s")
                    return ""  # combined success — no standalone dir file needed
            except Exception as e:
                print(f"  [warn] Combined dir merge failed for {user!r}: {e}")
        combined_elapsed = time.time() - combined_start

        # Fallback: standalone dirs DB with full paths (legacy schema).
        fallback_start = time.time()
        fallback_dirs = dirs_factory() if dirs_factory is not None else dirs
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if os.path.exists(output_path):
            os.remove(output_path)
        conn = sqlite3.connect(output_path)
        conn.execute("PRAGMA page_size = 8192")
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        schema_start = time.time()
        conn.execute("CREATE TABLE meta (date INTEGER, user TEXT, total_items INTEGER, total_used INTEGER)")
        conn.execute("CREATE TABLE dirs (id INTEGER PRIMARY KEY, path TEXT, used INTEGER)")
        conn.execute(
            "INSERT INTO meta(date, user, total_items, total_used) VALUES (?, ?, ?, ?)",
            (scan_result.timestamp, user, int(total_dir_count), int(total_dir_used)),
        )
        schema_elapsed = time.time() - schema_start
        insert_start = time.time()
        for batch in self._iter_batched_pairs(fallback_dirs):
            conn.executemany("INSERT INTO dirs(path, used) VALUES (?, ?)", batch)
        conn.commit()
        insert_elapsed = time.time() - insert_start
        conn.close()
        fallback_elapsed = time.time() - fallback_start
        if self.debug:
            print(
                f"  [Phase 2][dir] {user}: combined={combined_elapsed:.2f}s, "
                f"fallback={fallback_elapsed:.2f}s (schema={schema_elapsed:.2f}s, insert={insert_elapsed:.2f}s)"
            )
        return output_path

    def _write_dirs_into_file_db(
        self,
        user: str,
        scan_result: ScanResult,
        dirs: Iterable[Tuple[str, int]],
        file_db_path: str,
        total_dir_used: int,
        total_dir_count: int,
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

            schema_start = time.time()
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
            # Keep a single dirs metadata row per run/user DB (avoid unbounded append).
            conn.execute("DELETE FROM meta_dirs")
            conn.execute(
                "INSERT INTO meta_dirs(date, user, total_dirs, total_used) VALUES (?, ?, ?, ?)",
                (scan_result.timestamp, user, int(total_dir_count), int(total_dir_used)),
            )
            schema_elapsed = time.time() - schema_start

            insert_start = time.time()
            # Chunked inserts keep per-user RAM bounded with very large dir sets.
            conn.execute("CREATE TEMP TABLE _dt (path TEXT, used INTEGER)")
            for batch in self._iter_batched_pairs(dirs):
                conn.executemany(
                    "INSERT OR IGNORE INTO dirs_index(path) VALUES (?)",
                    [(p,) for p, _ in batch],
                )
                conn.executemany("INSERT INTO _dt VALUES (?, ?)", batch)

            conn.execute(
                "INSERT OR REPLACE INTO dirs_data(dir_id, used) "
                "SELECT di.id, t.used FROM _dt t "
                "JOIN dirs_index di ON di.path = t.path"
            )
            conn.execute("DROP TABLE _dt")
            conn.commit()
            insert_elapsed = time.time() - insert_start

            index_start = time.time()
            # ── Critical performance indexes ──────────────────────────────────
            # Built AFTER bulk insert (much faster than maintaining during inserts).
            # idx_dirs_data_used: ORDER BY dd.used DESC → only reads N rows, no temp sort.
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dirs_data_used ON dirs_data(used DESC)"
            )
            files_data_cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(files_data)").fetchall()
                if len(row) >= 2
            }
            {"basename_id", "xt_id"}.issubset(files_data_cols)
            # idx_files_data_size: ORDER BY fd.size DESC → only reads N rows, no temp sort.
            if self.enable_detail_size_index:
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_files_data_size ON files_data(size DESC)"
                )
            conn.commit()
            index_elapsed = time.time() - index_start

            fts_elapsed = 0.0
            # ── FTS4 build is DEFERRED to post-Phase-2 batch pass ──────────
            # Skipped here to keep per-user critical path fast.
            # The caller (generate_detail_reports) runs _build_fts_deferred()
            # after all user reports are written.

            if self.debug:
                print(
                    f"  [Phase 2][dir-merge] {user}: "
                    f"schema={schema_elapsed:.2f}s, insert={insert_elapsed:.2f}s, "
                    f"index={index_elapsed:.2f}s, fts={fts_elapsed:.2f}s"
                )
            return True
        finally:
            conn.close()

    def _build_fts_for_db(self, db_path: str) -> None:
        """Build FTS4 indexes for a single combined user DB (deferred pass)."""
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode = OFF")
            conn.execute("PRAGMA synchronous = OFF")

            files_data_cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(files_data)").fetchall()
                if len(row) >= 2
            }
            files_data_has_ids = {"basename_id", "xt_id"}.issubset(files_data_cols)

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
            if files_data_has_ids:
                conn.execute(
                    "INSERT INTO fts_files(docid, fullpath) "
                    "SELECT fd.id, di.path || '/' || bi.basename "
                    "FROM files_data fd "
                    "JOIN dirs_index di ON fd.dir_id = di.id "
                    "JOIN basename_index bi ON bi.id = fd.basename_id"
                )
            else:
                conn.execute(
                    "INSERT INTO fts_files(docid, fullpath) "
                    "SELECT fd.id, di.path || '/' || fd.basename "
                    "FROM files_data fd JOIN dirs_index di ON fd.dir_id = di.id"
                )
            conn.commit()
        except Exception as _fts_err:
            # FTS4 unavailable (old SQLite) → PHP falls back to LIKE automatically.
            # Log only in debug mode so unexpected errors (disk full, etc.) are visible.
            if self.debug:
                print(f"  [debug] FTS build skipped for {db_path}: {_fts_err}")
        finally:
            conn.close()

    def _build_fts_deferred(self, created: List[str]) -> None:
        """Build FTS4 indexes for all combined user DBs after Phase 2 completes.

        Only processes file DBs that have dirs_index (combined schema).
        Runs sequentially — FTS build is CPU-bound, parallelism adds no benefit.
        """
        if not self.enable_detail_fts:
            return

        # Filter to file DBs only (combined DBs that have dirs_index)
        file_dbs = [p for p in created if "detail_report_file_" in os.path.basename(p)]
        if not file_dbs:
            return

        fts_start = time.time()
        built = 0
        for db_path in file_dbs:
            try:
                self._build_fts_for_db(db_path)
                built += 1
            except Exception as e:
                if self.debug:
                    print(f"  [Phase 2][fts] Failed for {db_path}: {e}")
        fts_elapsed = time.time() - fts_start
        print(f"  [Phase 2][fts] Built FTS indexes for {built}/{len(file_dbs)} DBs in {fts_elapsed:.2f}s")

    @staticmethod
    def _iter_user_dirs_from_spool_db(
        spool_db_path: str,
        user: str,
        batch_size: int = 5000,
    ) -> Iterable[Tuple[str, int]]:
        """Yield (path, used) rows for one user from the Phase-2 spool DB."""
        conn = sqlite3.connect(f"file:{spool_db_path}?mode=ro", uri=True, check_same_thread=False)
        try:
            conn.execute("PRAGMA cache_size = -2000")
            cur = conn.execute(
                "SELECT path, used FROM dirs_spool WHERE user = ?",
                (user,),
            )
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for path, used in rows:
                    yield path, int(used)
        finally:
            conn.close()

    @staticmethod
    def _build_dirs_spool_db(
        scan_result: ScanResult,
        spool_db_path: str,
        batch_size: int = 10_000,
    ) -> Tuple[List[str], Dict[str, int], Dict[str, int]]:
        """Build an on-disk user->dirs spool DB to keep Phase-2 RAM flat."""
        user_dir_count: Dict[str, int] = {}
        user_dir_used: Dict[str, int] = {}

        conn = sqlite3.connect(spool_db_path)
        try:
            conn.execute("PRAGMA journal_mode = OFF")
            conn.execute("PRAGMA synchronous = OFF")
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA cache_size = -8000")
            conn.execute(
                "CREATE TABLE dirs_spool ("
                "user TEXT NOT NULL, "
                "path TEXT NOT NULL, "
                "used INTEGER NOT NULL)"
            )
            conn.execute("BEGIN")
            rows: List[Tuple[str, str, int]] = []
            for dpath, user_sizes in scan_result.dir_sizes_map.items():
                if not dpath:
                    continue
                for user, size in user_sizes.items():
                    used = int(size)
                    rows.append((user, dpath, used))
                    user_dir_count[user] = user_dir_count.get(user, 0) + 1
                    user_dir_used[user] = user_dir_used.get(user, 0) + used
                    if len(rows) >= batch_size:
                        conn.executemany(
                            "INSERT INTO dirs_spool(user, path, used) VALUES (?, ?, ?)",
                            rows,
                        )
                        rows.clear()
            if rows:
                conn.executemany(
                    "INSERT INTO dirs_spool(user, path, used) VALUES (?, ?, ?)",
                    rows,
                )
            conn.execute("COMMIT")
            conn.execute("CREATE INDEX idx_dirs_spool_user ON dirs_spool(user)")
            conn.commit()
        finally:
            conn.close()

        return sorted(user_dir_count.keys()), user_dir_count, user_dir_used

    def _write_file_report_db_streaming(
        self,
        user: str,
        scan_result: ScanResult,
        output_path: str,
        uids: List[int],
        dir_items_hint: int = 0,
    ) -> str:
        """Write per-user file detail report via Rust core only."""
        if not HAS_RUST_PHASE2_DB:
            raise RuntimeError(
                "Rust Phase 2 DB core is required. "
                "Please build/install fast_scanner with merge_write_user_report_db."
            )

        try:
            batch_size = self.phase2_merge_batch_large if int(dir_items_hint or 0) >= 10000 else self.phase2_merge_batch
            _fast_scanner.merge_write_user_report_db(
                scan_result.detail_tmpdir,
                uids,
                user,
                output_path,
                scan_result.timestamp,
                batch_size,
            )
            if self.debug:
                print(f"  [Phase 2][file] {user}: merge_batch={batch_size}, dir_items_hint={int(dir_items_hint or 0)}")
            return output_path
        except Exception as e:
            raise RuntimeError(f"Rust Phase 2 DB failed for {user!r}: {e}") from e

    # ------------------------------------------------------------------ #
    # Per-user detail reports (parallel)                                   #
    # ------------------------------------------------------------------ #

    def _write_user_detail(
        self,
        user: str,
        scan_result: ScanResult,
        user_dirs: Optional[Sequence[Tuple[str, int]]] = None,
        user_uids_map: Optional[Dict[str, List[int]]] = None,
        user_dirs_spool_db: Optional[str] = None,
        user_dir_count: int = 0,
        user_dir_total: int = 0,
    ) -> Tuple[str, str, UserDetailTiming]:
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
        dirs_factory: Optional[Callable[[], Iterable[Tuple[str, int]]]] = None
        if user_dirs_spool_db:
            def dirs_factory():
                return self._iter_user_dirs_from_spool_db(user_dirs_spool_db, user)
            dirs: Iterable[Tuple[str, int]] = dirs_factory()
            total_dir_used = int(user_dir_total)
            total_dir_count = int(user_dir_count)
        elif user_dirs is not None:
            dirs = list(user_dirs)
            total_dir_count = len(dirs)
            total_dir_used = sum(used for _, used in dirs)
        else:
            dirs = [
                (e["dir"], int(e["user_usage"]))
                for e in scan_result.top_dir
                if e["user"] == user and e.get("dir")
            ]
            total_dir_count = len(dirs)
            total_dir_used = sum(used for _, used in dirs)

        user_started = time.time()

        # ── File detail report FIRST — combined mode appends dirs into this DB ─
        file_started = time.time()
        file_path = self._get_user_detail_filename('detail_report_file', user, ext='db')
        streaming = bool(scan_result.detail_tmpdir)
        if streaming:
            if user_uids_map is not None:
                uids = user_uids_map.get(user, [])
            else:
                if user.startswith("uid-") and user[4:].isdigit():
                    uids = [int(user[4:])]
                else:
                    uids = [u for u, n in scan_result.detail_uid_username.items() if n == user]
            if uids:
                self._write_file_report_db_streaming(
                    user,
                    scan_result,
                    file_path,
                    uids,
                    dir_items_hint=total_dir_count,
                )
            else:
                print(f"  [warn] No UIDs found for user {user!r}, skipping file report")
                file_path = ""
        else:
            raise RuntimeError(
                "Phase 2 requires Rust streaming outputs (detail_tmpdir). "
                "Python in-memory fallback has been removed."
            )
        file_elapsed = time.time() - file_started

        # ── Directory detail report — tries combined append into file_db first ─
        dir_started = time.time()
        dir_path = self._get_user_detail_filename('detail_report_dir', user, ext='db')
        dir_sema = getattr(self, '_phase2_dir_semaphore', None)
        if dir_sema is not None:
            with dir_sema:
                dir_path = self._write_dir_report_db(
                    user,
                    scan_result,
                    dirs,
                    dir_path,
                    total_dir_used,
                    total_dir_count,
                    dirs_factory=dirs_factory,
                )
        else:
            dir_path = self._write_dir_report_db(
                user,
                scan_result,
                dirs,
                dir_path,
                total_dir_used,
                total_dir_count,
                dirs_factory=dirs_factory,
            )
        dir_elapsed = time.time() - dir_started

        def _size_mb(path: str) -> float:
            try:
                if path and os.path.exists(path):
                    return os.path.getsize(path) / (1024.0 * 1024.0)
            except Exception:
                pass
            return 0.0

        timing = UserDetailTiming(
            user=user,
            total_sec=time.time() - user_started,
            file_sec=file_elapsed,
            dir_sec=dir_elapsed,
            file_db_mb=_size_mb(file_path),
            dir_db_mb=_size_mb(dir_path),
            dir_items=int(total_dir_count),
            dir_total_used=int(total_dir_used),
        )

        return dir_path, file_path, timing

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
        spool_min_dirs = 1_000_000
        use_spool = len(scan_result.dir_sizes_map) >= spool_min_dirs
        user_dirs_map: Dict[str, List[Tuple[str, int]]] = {}
        user_dir_count_map: Dict[str, int] = {}
        user_dir_used_map: Dict[str, int] = {}
        dirs_spool_db_path: Optional[str] = None
        spool_tmpdir_ctx: Optional[tempfile.TemporaryDirectory[str]] = None

        if use_spool:
            spool_tmpdir_ctx = tempfile.TemporaryDirectory(prefix="checkdisk_phase2_dirs_")
            dirs_spool_db_path = os.path.join(spool_tmpdir_ctx.name, "dirs_spool.db")
            print("  [Phase 2] Preparing directory spool...")
            spool_start = time.time()
            users, user_dir_count_map, user_dir_used_map = self._build_dirs_spool_db(
                scan_result,
                dirs_spool_db_path,
            )
            print(
                f"  [Phase 2] Directory spool ready for {len(users)} users "
                f"({time.time() - spool_start:.2f}s)"
            )
        else:
            for dpath, user_sizes in scan_result.dir_sizes_map.items():
                for user, size in user_sizes.items():
                    used = int(size)
                    user_dirs_map.setdefault(user, []).append((dpath, used))
                    user_dir_count_map[user] = user_dir_count_map.get(user, 0) + 1
                    user_dir_used_map[user] = user_dir_used_map.get(user, 0) + used
            users = sorted(user_dirs_map.keys())
            print(
                f"  [Phase 2] Using in-memory directory map "
                f"({len(scan_result.dir_sizes_map):,} dirs < {spool_min_dirs:,} threshold)"
            )

        try:
            if not users:
                print("No users found in scan results — nothing to generate.")
                return []

            streaming = bool(scan_result.detail_tmpdir)
            mode_label = "streaming" if streaming else "in-memory"
            workers_label = f"parallel {max_workers}w" if max_workers > 1 else "sequential"
            print(f"Phase 2: Writing {len(users)} user detail reports "
                  f"[{mode_label}, {workers_label}]...")
            phase2_start = time.time()
            phase2_mem_start = self._get_rss_mb()
            phase2_peak_mb = phase2_mem_start
            print(f"[Phase 2] RAM at start: {phase2_mem_start:.1f} MB")

            created: List[str] = []
            failed: List[str] = []
            timings: List[UserDetailTiming] = []
            total_users = len(users)
            completed = 0

            if not streaming:
                raise RuntimeError(
                    "Phase 2 requires Rust streaming outputs (detail_tmpdir). "
                    "Python in-memory fallback has been removed."
                )
            if not HAS_RUST_PHASE2_DB:
                raise RuntimeError(
                    "Rust Phase 2 DB core is required. "
                    "Please build/install fast_scanner with merge_write_user_report_db."
                )

            user_uids_map: Dict[str, List[int]] = {}
            for uid, uname in scan_result.detail_uid_username.items():
                user_uids_map.setdefault(uname, []).append(uid)
            for user in users:
                if user.startswith("uid-") and user[4:].isdigit() and user not in user_uids_map:
                    user_uids_map[user] = [int(user[4:])]

            # I/O-aware scheduling: heavier users first to reduce long tail
            users = sorted(
                users,
                key=lambda u: (user_dir_used_map.get(u, 0), user_dir_count_map.get(u, 0)),
                reverse=True,
            )
            heavy_threshold = 1_000_000_000  # 1 GB
            heavy_users = [u for u in users if user_dir_used_map.get(u, 0) >= heavy_threshold]
            if heavy_users and self.debug:
                print(
                    f"  [Phase 2] I/O-aware ordering: {len(heavy_users)} heavy user(s) first "
                    f"(>= {format_size(heavy_threshold)})"
                )

            # Phase 2 is primarily I/O bound. Keep conservative defaults for unstable
            # storage, but allow wider parallelism on large runs so fast disks are used.
            requested_workers = max(1, int(max_workers))
            cpu_cap = max(1, os.cpu_count() or 1)
            if total_users >= 256:
                io_cap = 16
            elif total_users >= 64:
                io_cap = 12
            elif total_users >= 16:
                io_cap = 8
            else:
                io_cap = 4
            effective_workers = max(1, min(requested_workers, io_cap, cpu_cap))

            if self.debug:
                print(
                    f"  [Phase 2] Worker tuning: requested={requested_workers}, "
                    f"using={effective_workers} (users={total_users}, streaming={streaming})"
                )

            # Split pool by step: keep file writes wider, adaptive dir merge concurrency
            # Scale dir workers with effective_workers instead of hard-capping at 4.
            # SSD/NVMe can handle higher concurrency; HDD benefits from less thrashing.
            if total_users >= 128:
                dir_workers = max(1, min(effective_workers, 12))
            elif total_users >= 32:
                dir_workers = max(1, min(effective_workers, 8))
            else:
                dir_workers = max(1, min(effective_workers, 6))
            self._phase2_dir_semaphore = threading.Semaphore(dir_workers)
            if self.debug:
                print(f"  [Phase 2] Split pools: file<={effective_workers}, dir<={dir_workers}")

            with ThreadPoolExecutor(max_workers=effective_workers) as executor:
                user_iter = iter(users)
                inflight = {}
                max_inflight = effective_workers  # Maximize IO parallelism

                def _submit_next() -> bool:
                    try:
                        u = next(user_iter)
                    except StopIteration:
                        return False
                    fut = executor.submit(
                        self._write_user_detail,
                        u,
                        scan_result,
                        user_dirs_map.get(u, []) if not use_spool else None,
                        user_uids_map,
                        dirs_spool_db_path if use_spool else None,
                        user_dir_count_map.get(u, 0),
                        user_dir_used_map.get(u, 0),
                    )
                    inflight[fut] = u
                    return True

                for _ in range(min(max_inflight, total_users)):
                    _submit_next()

                while inflight:
                    done, _ = wait(tuple(inflight.keys()), return_when=FIRST_COMPLETED)
                    for fut in done:
                        user = inflight.pop(fut)
                        completed += 1
                        try:
                            dir_p, file_p, t = fut.result()
                            timings.append(t)
                            if dir_p:
                                created.append(dir_p)
                            if file_p:
                                created.append(file_p)
                        except Exception as exc:
                            failed.append(user)
                            print(f"  [warn] Failed report for {user!r}: {exc}")
                        _submit_next()

                        if completed % 20 == 0 or completed == total_users:
                            current_mb = self._get_rss_mb()
                            if current_mb > phase2_peak_mb:
                                phase2_peak_mb = current_mb
                            print(
                                f"  [Phase 2] Progress: {completed}/{total_users} users completed... "
                                f"RAM: {current_mb:.1f} MB (peak: {phase2_peak_mb:.1f} MB)"
                            )

            if failed:
                print(f"  [warn] {len(failed)} user report(s) failed: {failed}")

            output_dir = os.path.dirname(created[0]) if created else '.'
            print(f"Generated {len(users)} user detail report(s) "
                  f"[{mode_label}, {workers_label}] -> {output_dir}")
            phase2_mem_end = self._get_rss_mb()
            if phase2_mem_end > phase2_peak_mb:
                phase2_peak_mb = phase2_mem_end
            phase2_elapsed = time.time() - phase2_start
            print(
                f"[Phase 2] RAM end: {phase2_mem_end:.1f} MB "
                f"(peak: {phase2_peak_mb:.1f} MB, delta: {phase2_mem_end - phase2_mem_start:+.1f} MB, "
                f"elapsed: {phase2_elapsed:.2f}s)"
            )

            if timings and self.debug:
                timings_sorted = sorted(timings, key=lambda x: x.total_sec, reverse=True)
                top_n = min(5, len(timings_sorted))
                print(f"[Phase 2] Slowest users (top {top_n}):")
                for item in timings_sorted[:top_n]:
                    print(
                        "  [Phase 2][user] "
                        f"{item.user}: total={item.total_sec:.2f}s "
                        f"(file={item.file_sec:.2f}s, dir={item.dir_sec:.2f}s), "
                        f"file_db={item.file_db_mb:.1f}MB, dir_db={item.dir_db_mb:.1f}MB, "
                        f"dir_items={item.dir_items}, dir_total={format_size(item.dir_total_used)}"
                    )
                total_file_sec = sum(t.file_sec for t in timings_sorted)
                total_dir_sec = sum(t.dir_sec for t in timings_sorted)
                print(
                    f"[Phase 2] Aggregate user work: file={total_file_sec:.2f}s, "
                    f"dir={total_dir_sec:.2f}s, users={len(timings_sorted)}"
                )

            # ── Deferred FTS build (after all user reports are done) ────────
            self._build_fts_deferred(created)

            return sorted(created)
        finally:
            if hasattr(self, '_phase2_dir_semaphore'):
                try:
                    delattr(self, '_phase2_dir_semaphore')
                except Exception:
                    pass
            if spool_tmpdir_ctx is not None:
                spool_tmpdir_ctx.cleanup()
