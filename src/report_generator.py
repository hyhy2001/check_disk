"""
Report Generator Module

Handles generating and saving disk usage reports.
"""

import os
import json
import heapq
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
except ImportError:
    _fast_scanner = None  # type: ignore
    HAS_RUST_PHASE2 = False


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

    def _get_user_detail_filename(self, base: str, user: str) -> str:
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
        fname = "_".join(parts) + ".ndjson"
        return os.path.join(detail_dir, fname)

    def clear_old_detail_reports(self) -> None:
        """
        Remove all old JSON files in the detail_users directory before scanning.
        """
        dir_part = os.path.dirname(self.output_file)
        detail_dir = os.path.join(dir_part, "detail_users") if dir_part else "detail_users"
        if os.path.exists(detail_dir):
            import glob
            old_files = glob.glob(os.path.join(detail_dir, "*.json")) + glob.glob(os.path.join(detail_dir, "*.ndjson"))
            if old_files:
                print(f"Cleaning up {len(old_files)} old JSON files in {detail_dir}...")
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

    def generate_tree_map(self, scan_result: ScanResult, level: int = 3) -> str:
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
        
        # Build dir_sizes map from top_dir if not directly available in scan_result
        # TreeMapGenerator expects Dict[str, Dict[str, int]]
        dir_sizes_map = {}
        for entry in scan_result.top_dir:
            d = entry['dir']
            u = entry['user']
            s = entry['user_usage']
            if d not in dir_sizes_map:
                dir_sizes_map[d] = {}
            dir_sizes_map[d][u] = s

        generator = TreeMapGenerator(
            root_dir=self.config.get("directory", "/"),
            dir_sizes=dir_sizes_map,
            max_level=level,
            max_workers=self.config.get("workers", 4)
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

    # ------------------------------------------------------------------ #
    # Per-user detail reports (parallel)                                   #
    # ------------------------------------------------------------------ #

    def _write_user_detail(self, user: str, scan_result: ScanResult) -> Tuple[str, str]:
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
        # ── Directory detail report (always in-memory — bounded by dir count) ──
        dirs = [e for e in scan_result.top_dir if e['user'] == user]
        total_dir_used = sum(d['user_usage'] for d in dirs)
        dir_path = self._get_user_detail_filename('detail_report_dir', user)
        
        # Ensure target directory exists
        os.makedirs(os.path.dirname(dir_path), exist_ok=True)
        
        with open(dir_path, 'w', encoding='utf-8') as out:
            meta = {
                "_meta": {
                    "date": scan_result.timestamp,
                    "user": user,
                    "total_dirs": len(dirs),
                    "total_used": total_dir_used
                }
            }
            out.write(json.dumps(meta, separators=(',', ':')) + '\n')
            for d in sorted(dirs, key=lambda x: x['user_usage'], reverse=True):
                line = {
                    "path": d['dir'],
                    "used": d['user_usage']
                }
                out.write(json.dumps(line, separators=(',', ':')) + '\n')

        # ── File detail report ────────────────────────────────────────────────
        file_path = self._get_user_detail_filename('detail_report_file', user)

        streaming = bool(scan_result.detail_tmpdir)
        if streaming:
            uids = [u for u, n in scan_result.detail_uid_username.items() if n == user]
            if uids:
                self._stream_write_file_report(
                    scan_result.detail_tmpdir, uids, user, file_path,
                    scan_result.timestamp,
                )
            else:
                print(f"  [warn] No UIDs found for user {user!r}, skipping file report")
                file_path = ""
        else:
            # Legacy in-memory path
            user_files = scan_result.detail_files.get(user, [])
            with open(file_path, 'w', encoding='utf-8') as out:
                meta = {
                    "_meta": {
                        "date": scan_result.timestamp,
                        "user": user,
                        "total_files": len(user_files),
                        "total_used": sum(s for _, s in user_files)
                    }
                }
                out.write(json.dumps(meta, separators=(',', ':')) + '\n')
                for p, s in user_files:
                    xt = os.path.splitext(p)[1][1:]
                    line = {
                        "path": p,
                        "size": s,
                        "xt": xt
                    }
                    out.write(json.dumps(line, separators=(',', ':')) + '\n')

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
        users = sorted({entry['user'] for entry in scan_result.top_dir})

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

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._write_user_detail, u, scan_result): u
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
