"""
Report Generator Module

Handles generating and saving disk usage reports.
"""

import os
import json
import heapq
import glob as glob_module
import time
from typing import Dict, Any, Optional, List, Tuple
from src.disk_scanner import ScanResult
from src.utils import format_size, save_json_report, ScanHelper

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
        fname = "_".join(parts) + ".json"
        return os.path.join(detail_dir, fname)

    # ------------------------------------------------------------------ #
    # Legacy helpers kept for backward compatibility                       #
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
        team_id_map = {t["name"]: t["team_ID"] for t in self.config.get("teams", [])}
        user_team_id_map = {}
        for user in self.config.get("users", []):
            user_team_id_map[user["name"]] = user["team_ID"]
        return team_id_map, user_team_id_map

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

            report = {
                "date": scan_result.timestamp,
                "directory": self.config.get("directory", ""),
                "general_system": scan_result.general_system,
                "team_usage": team_usage,
                "user_usage": user_usage,
                "other_usage": scan_result.other_usage
            }

            if hasattr(scan_result, 'permission_issues') and scan_result.permission_issues:
                self.generate_permission_issues_report(scan_result)

        save_json_report(report, self.output_file)
        return report

    
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
        
        # Generate output filename with the same pattern as the main output
        output_path = self._get_output_filename("permission_issues")
        
        # Save to permission_issues.json
        save_json_report(report, output_path)
        print(f"Permission issues report saved to: {output_path}")
        
        return report

    # ------------------------------------------------------------------ #
    # Streaming file-detail report writer                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _iter_uid_tsv(tmpdir: str, uid: int):
        """Yield (size, path) tuples from all temp TSV chunks for *uid*.

        Each chunk is sorted descending (written that way by _flush_thread_paths).
        Using ``heapq.merge(..., reverse=True)`` over these iterators gives a
        globally sorted descending stream with O(num_chunks) RAM.

        Opens files with ``errors='surrogateescape'`` to handle paths that
        contain non-UTF-8 byte sequences (common on Linux filesystems).
        """
        def _read_chunk(fpath: str):
            # surrogateescape: non-UTF-8 bytes were stored as surrogates; read them back
            with open(fpath, encoding='utf-8', errors='surrogateescape') as fh:
                for line in fh:
                    line = line.rstrip('\n')
                    if not line:
                        continue
                    tab = line.index('\t')
                    yield int(line[:tab]), line[tab + 1:]

        chunk_files = sorted(
            glob_module.glob(os.path.join(tmpdir, f'uid_{uid}_t*.tsv'))
        )
        if not chunk_files:
            return

        yield from heapq.merge(*[_read_chunk(f) for f in chunk_files], reverse=True)

    def _stream_write_file_report(
        self,
        tmpdir: str,
        uid: int,
        username: str,
        output_path: str,
        timestamp: int,
    ) -> str:
        """Write a complete per-user file detail JSON by streaming from temp files.

        Uses two passes over the temp files:
          - Pass 1: count total_files and sum total_used  (O(1) RAM)
          - Pass 2: k-way merge of sorted chunks → write JSON  (O(1) RAM)

        Paths with non-UTF-8 bytes (surrogate-escaped) are sanitised with
        ``errors='replace'`` before JSON serialisation so the output file
        is always valid UTF-8 JSON.

        Returns:
            The output path that was written.
        """
        def _safe_path(p: str) -> str:
            """Replace surrogate chars with U+FFFD so json.dumps never raises."""
            return p.encode('utf-8', errors='replace').decode('utf-8')

        # ── Pass 1: totals ────────────────────────────────────────────────
        total_files = 0
        total_used  = 0
        for size, _ in self._iter_uid_tsv(tmpdir, uid):
            total_files += 1
            total_used  += size

        # ── Pass 2: stream write ──────────────────────────────────────────
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as out:
            # Header with known totals
            out.write('{\n')
            out.write(f'  "date": {timestamp},\n')
            out.write(f'  "user": {json.dumps(_safe_path(username))},\n')
            out.write(f'  "total_files": {total_files},\n')
            out.write(f'  "total_used": {total_used},\n')
            out.write('  "files": [')

            first = True
            for size, path in self._iter_uid_tsv(tmpdir, uid):
                sep = '' if first else ','
                out.write(f'{sep}\n    {{"path": {json.dumps(_safe_path(path))}, "size": {size}}}')
                first = False

            out.write('\n  ]\n}\n')

        return output_path

    def generate_detail_reports(self, scan_result: ScanResult) -> List[str]:
        """
        Generate per-user directory and file detail reports.

        When ``scan_result.detail_tmpdir`` is set (streaming mode), file detail
        reports are written directly from temp TSV files without loading data
        into memory — suitable for disks with tens of millions of files.

        Otherwise falls back to the legacy in-memory path using
        ``scan_result.detail_files``.

        Args:
            scan_result: ScanResult object with top_dir and detail_files data

        Returns:
            List of created file paths
        """
        streaming = bool(scan_result.detail_tmpdir)
        created: List[str] = []

        # Collect users that have directory data
        users = sorted({entry['user'] for entry in scan_result.top_dir})

        for user in users:
            # ── Directory detail report (always in-memory — bounded by dir count) ──
            dirs = [e for e in scan_result.top_dir if e['user'] == user]
            total_dir_used = sum(d['user_usage'] for d in dirs)
            dir_data = {
                'date': scan_result.timestamp,
                'directory': self.config.get('directory', ''),
                'user': user,
                'total_used': total_dir_used,
                'dirs': [
                    {'path': d['dir'], 'used': d['user_usage']}
                    for d in sorted(dirs, key=lambda x: x['user_usage'], reverse=True)
                ],
            }
            dir_path = self._get_user_detail_filename('detail_report_dir', user)
            save_json_report(dir_data, dir_path)
            created.append(dir_path)

            # ── File detail report ────────────────────────────────────────
            file_path = self._get_user_detail_filename('detail_report_file', user)

            if streaming:
                # Look up the uid for this username
                uid = next(
                    (u for u, n in scan_result.detail_uid_username.items() if n == user),
                    None,
                )
                if uid is not None:
                    self._stream_write_file_report(
                        scan_result.detail_tmpdir, uid, user, file_path,
                        scan_result.timestamp,
                    )
                    created.append(file_path)
                else:
                    print(f"  [warn] No UID found for user {user!r}, skipping file report")
            else:
                # Legacy: build from in-memory list
                user_files = scan_result.detail_files.get(user, [])
                file_data = {
                    'date': scan_result.timestamp,
                    'user': user,
                    'total_files': len(user_files),
                    'total_used': sum(s for _, s in user_files),
                    'files': [{'path': p, 'size': s} for p, s in user_files],
                }
                save_json_report(file_data, file_path)
                created.append(file_path)

        output_dir = os.path.dirname(created[0]) if created else '.'
        mode_label = "streaming (all files)" if streaming else "in-memory"
        print(f"Generated {len(users)} user detail report(s) [{mode_label}] -> {output_dir}")
        return created

    
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
        fname = "_".join(parts) + ".json"
        return os.path.join(detail_dir, fname)

    # ------------------------------------------------------------------ #
    # Legacy helpers kept for backward compatibility                       #
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
        team_id_map = {t["name"]: t["team_ID"] for t in self.config.get("teams", [])}
        user_team_id_map = {}
        for user in self.config.get("users", []):
            user_team_id_map[user["name"]] = user["team_ID"]
        return team_id_map, user_team_id_map

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

            report = {
                "date": scan_result.timestamp,
                "directory": self.config.get("directory", ""),
                "general_system": scan_result.general_system,
                "team_usage": team_usage,
                "user_usage": user_usage,
                "other_usage": scan_result.other_usage
            }

            if hasattr(scan_result, 'permission_issues') and scan_result.permission_issues:
                self.generate_permission_issues_report(scan_result)

        save_json_report(report, self.output_file)
        return report

    
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
        
        # Generate output filename with the same pattern as the main output
        output_path = self._get_output_filename("permission_issues")
        
        # Save to permission_issues.json
        save_json_report(report, output_path)
        print(f"Permission issues report saved to: {output_path}")
        
        return report

