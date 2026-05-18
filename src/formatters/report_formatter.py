"""
Report Formatter Module

Contains the ReportFormatter class for formatting and displaying reports.
"""

import json
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from ..constants import (
    DETAIL_USERS_DB_FILENAME,
    DETAIL_USERS_DIRNAME,
    TREE_MAP_DATA_DIRNAME,
    TREE_MAP_DB_FILENAME,
)
from ..utils import format_size, format_timestamp
from .base_formatter import BaseFormatter
from .config_display import ConfigDisplay
from .report_comparison import ReportComparison
from .table_formatter import TableFormatter


class ReportFormatter(BaseFormatter):
    """Helper class for formatting and displaying reports."""

    def __init__(self):
        """Initialize the report formatter."""
        super().__init__()
        self.table_formatter = TableFormatter()
        self.config_display = ConfigDisplay()
        self.report_comparison = ReportComparison()

    def display_report_summary(self, report: Dict[str, Any], report_path: str, filter_users: List[str] = None) -> None:
        """Display a summary of a disk usage report."""
        print("\n" + "=" * 60)
        print("DISK USAGE REPORT SUMMARY")
        print("=" * 60)
        print(f"Directory: {report.get('directory', 'Unknown')}")
        timestamp = report.get('date', 0)
        if timestamp:
            print(f"Date: {format_timestamp(timestamp)}")

        # Display general system info
        self._display_system_info(report)

        # Display appropriate report sections based on report type
        if 'check_users' in report:
            self._display_checked_users_report(report, report_path, filter_users)
        elif 'top_user' in report:
            self._display_top_users_report(report, report_path, filter_users)
        else:
            self._display_standard_report(report, report_path, filter_users)

        print(f"\nFull report saved to: {report_path}")

    def _display_system_info(self, report: Dict[str, Any]) -> None:
        """Display general system information."""
        system = report.get('general_system', {})
        system.get('total', 1)

        # Create a table for general system info
        headers = ["Metric", "Value", "Percentage"]
        rows = [
            ["Total Capacity", format_size(system.get('total', 0)), "100%"],
            ["Used Space", format_size(system.get('used', 0)), f"{system.get('used', 0) * 100 / system.get('total', 1):.1f}%"],
            ["Available", format_size(system.get('available', 0)), f"{system.get('available', 0) * 100 / system.get('total', 1):.1f}%"]
        ]

        system_table = self.table_formatter.format_table(headers, rows, title="General System Information")
        print(f"\n{system_table}")

    def _display_checked_users_report(self, report: Dict[str, Any], report_path: str, filter_users: List[str] = None) -> None:
        """Display checked users report."""
        # Create a table for checked users
        headers = ["Username", "Disk Usage", "Percent"]
        rows = []

        user_usage = report.get('user_usage', [])
        total_capacity = report.get('general_system', {}).get('total', 1)

        # Apply user filter if provided
        if filter_users:
            user_usage = [u for u in user_usage if u['name'] in filter_users]

        for user in sorted(user_usage, key=lambda x: x.get('used', 0), reverse=True):
            size = user.get('used', 0)
            percent = (size / total_capacity) * 100
            usage_bar = self._create_usage_bar(percent)
            rows.append([user['name'], format_size(size), f"{usage_bar} {percent:.1f}%"])

        if rows:
            table = self.table_formatter.format_table(headers, rows, title="Checked Users")
            print("\n" + table)

    def _display_top_users_report(self, report: Dict[str, Any], report_path: str, filter_users: List[str] = None) -> None:
        """Display top users report."""
        total_capacity = report.get('general_system', {}).get('total', 1)

        # Display top users
        top_n   = report.get('top_user', 10)
        min_use = report.get('min_usage', '')
        title   = f'Top {top_n} Users' + (f' (min usage: {min_use})' if min_use else '')
        self._display_user_usage_table(
            report.get('user_usage', []),
            total_capacity,
            title,
            filter_users
        )

        # Display other users
        if 'other_usage' in report and report['other_usage']:
            self._display_user_usage_table(
                report.get('other_usage', []),
                total_capacity,
                "Top Other Users (not in config)",
                filter_users
            )

        # Display team usage if available
        if 'team_usage' in report and report['team_usage']:
            self._display_team_usage_table(report.get('team_usage', []), total_capacity)

    def _display_standard_report(self, report: Dict[str, Any], report_path: str, filter_users: List[str] = None) -> None:
        """Display standard report with teams and users."""
        total_capacity = report.get('general_system', {}).get('total', 1)

        # Team usage
        sorted_teams = sorted(report.get('team_usage', []), key=lambda x: x.get('used', 0), reverse=True)
        if sorted_teams:
            self._display_team_usage_table(sorted_teams, total_capacity)

        # Top users - filter_users applied inside _display_user_usage_table
        user_usage = sorted(
            report.get('user_usage', []),
            key=lambda x: x.get('used', 0), reverse=True
        )[:10]
        if user_usage:
            self._display_user_usage_table(user_usage, total_capacity, "Top Users", filter_users)

        # Other users - filter_users applied inside _display_user_usage_table
        other_usage = sorted(
            report.get('other_usage', []),
            key=lambda x: x.get('used', 0), reverse=True
        )[:10]
        if other_usage:
            self._display_user_usage_table(other_usage, total_capacity, "Top Other Users (not in config)", filter_users)

    def _display_user_usage_table(self, users: List[Dict[str, Any]], total_capacity: int,
                                 title: str, filter_users: List[str] = None) -> None:
        """Display a table of user disk usage."""
        if filter_users:
            users = [u for u in users if u['name'] in filter_users]

        if not users:
            return

        headers = ["Username", "Disk Usage", "Percent"]
        rows = []

        for user in sorted(users, key=lambda x: x.get('used', 0), reverse=True):
            size = user.get('used', 0)
            percent = (size / total_capacity) * 100
            usage_bar = self._create_usage_bar(percent)
            rows.append([user['name'], format_size(size), f"{usage_bar} {percent:.1f}%"])

        table = self.table_formatter.format_table(headers, rows, title=title)
        print("\n" + table)

    def _display_team_usage_table(self, teams: List[Dict[str, Any]], total_capacity: int) -> None:
        """Display a table of team disk usage."""
        headers = ["Team", "Disk Usage", "Percent"]
        rows = []

        for team in sorted(teams, key=lambda x: x.get('used', 0), reverse=True):
            size = team.get('used', 0)
            percent = (size / total_capacity) * 100
            usage_bar = self._create_usage_bar(percent)
            rows.append([team['name'], format_size(size), f"{usage_bar} {percent:.1f}%"])

        table = self.table_formatter.format_table(headers, rows, title="Team Usage")
        print("\n" + table)

    def compare_reports(self, reports: List[Tuple[str, Dict[str, Any]]], filter_users: List[str] = None, compare_by: str = "growth") -> None:
        """Compare multiple reports and display a comparison table."""
        self.report_comparison.compare_reports(reports, filter_users, compare_by)

    def display_config(self, config: Dict[str, Any], team_filter: str = None) -> None:
        """Display the current configuration."""
        self.config_display.display_config_summary(config)
        self.config_display.display_users_by_team_table(config, team_filter)

    def display_user_detail_report(
        self,
        user: str,
        dir_report: Optional[Dict[str, Any]],
        file_report: Optional[Dict[str, Any]],
        top: int = 30,
        *,
        not_found_reason: Optional[str] = None,
    ) -> None:
        """Render dir + file detail reports for a single user.

        Args:
            user:        username (display only)
            dir_report:  directory breakdown dict, or None when no data
            file_report: file breakdown dict, or None when no data
            top:         truncate breakdown tables to this many rows
            not_found_reason: optional message displayed verbatim when both
                reports are None — used to explain WHY (no DB / unknown
                user / user has no files) instead of the previous opaque
                "[dir detail report not found]".
        """
        print("\n" + "=" * 60)
        print(f"DETAIL REPORT - {user}")
        print("=" * 60)

        # Both reports None → display the explanatory reason and bail out.
        if dir_report is None and file_report is None:
            if not_found_reason:
                print(f"  {not_found_reason}")
            else:
                print(f"  No data found for user '{user}'.")
            return

        # --- Directory breakdown ---
        if dir_report:
            timestamp = dir_report.get('date', 0)
            if timestamp:
                print(f"Date      : {format_timestamp(timestamp)}")
            print(f"Directory : {dir_report.get('directory', '')}")
            print(f"Total used: {format_size(dir_report.get('total_used', 0))}")

            dirs = dir_report.get('dirs', [])
            total_dirs = dir_report.get('total_dirs', len(dirs))
            display_dirs = dirs[:top]
            if display_dirs:
                headers = ["Directory", "Used"]
                rows = [[d['path'], format_size(d['used'])] for d in display_dirs]
                title = f"Directory Breakdown (top {len(display_dirs)} of {total_dirs:,})"
                table = self.table_formatter.format_table(headers, rows, title=title)
                print("\n" + table)
            else:
                print("  (no directory data)")
        else:
            print("  (no directory data)")

        # --- File breakdown ---
        if file_report:
            files = file_report.get('files', [])
            total_files = file_report.get('total_files', len(files))
            total_used  = file_report.get('total_used', 0)
            print(f"\nTotal files: {total_files:,}  |  Total size: {format_size(total_used)}")

            files_sorted = sorted(files, key=lambda f: int(f.get('size', 0) or 0), reverse=True)
            display = files_sorted[:top]
            if display:
                headers = ["File", "Size"]
                rows = [[f['path'], format_size(f['size'])] for f in display]
                title = f"Largest Files (top {len(display)} of {total_files:,})"
                table = self.table_formatter.format_table(headers, rows, title=title)
                print("\n" + table)
            else:
                print("  (no file data)")
        else:
            print("  (no file data)")

    def display_user_detail_reports(
        self,
        users: List[str],
        dir_files: Dict[str, Optional[str]],
        file_files: Dict[str, Optional[str]],
        top: int = 30,
    ) -> None:
        """Load and render detail reports for multiple users from SQLite.

        `dir_files` / `file_files` historically mapped users to NDJSON paths.
        With the SQLite pipeline both arguments are accepted but only the
        first non-empty entry is used to derive the output directory: every
        user reads from the same `data_detail.db`.
        """
        sample_path = next(
            (p for p in list(dir_files.values()) + list(file_files.values()) if p),
            None,
        )
        detail_db, treemap_db = self._resolve_db_paths(sample_path)
        if not detail_db or not os.path.isfile(detail_db):
            db_hint = detail_db or "<output dir>/detail_users/data_detail.db"
            reason = (
                f"Detail database not found at: {db_hint}\n"
                f"  Run a scan first: disk_checker.py --run --output-dir <dir>"
            )
            for user in users:
                self.display_user_detail_report(
                    user, None, None, top, not_found_reason=reason
                )
            return

        conn = sqlite3.connect(f"file:{detail_db}?mode=ro", uri=True)
        try:
            self._configure_read_conn(conn, treemap_db)
            scan_root = self._meta_get(conn, "scan_root") or "<unknown>"
            known_users = self._known_usernames(conn)
            for user in users:
                if user not in known_users:
                    reason = (
                        f"User '{user}' not found in scan results.\n"
                        f"  Scanned root: {scan_root}\n"
                        f"  No files owned by this user were captured. "
                        f"Check the username spelling, or rescan if the user "
                        f"only recently created files."
                    )
                    self.display_user_detail_report(
                        user, None, None, top, not_found_reason=reason
                    )
                    continue

                dir_data = self._load_user_dirs(conn, user, top)
                file_data = self._load_user_files(conn, user, top)
                if (
                    dir_data
                    and file_data
                    and (dir_data.get('total_files') or 0) == 0
                    and (file_data.get('total_files') or 0) == 0
                ):
                    reason = (
                        f"User '{user}' is registered but has no files in scan.\n"
                        f"  Scanned root: {scan_root}"
                    )
                    self.display_user_detail_report(
                        user, None, None, top, not_found_reason=reason
                    )
                    continue
                self.display_user_detail_report(user, dir_data, file_data, top)
        finally:
            conn.close()

    @staticmethod
    def _known_usernames(conn: sqlite3.Connection) -> set:
        try:
            return {r[0] for r in conn.execute("SELECT username FROM users")}
        except sqlite3.DatabaseError:
            return set()

    @staticmethod
    def _resolve_db_paths(hint_path: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        """Locate `data_detail.db` + `treemap.db` from a sample path the
        caller passed. Falls back to walking up to find the right output
        directory.
        """
        if not hint_path:
            return (None, None)
        # Common shapes:
        #   .../detail_users/data_detail.db
        #   .../detail_users/manifest.json (legacy)
        #   .../detail_users/users/<user>/manifest.json (legacy)
        cur = os.path.abspath(hint_path)
        if os.path.isdir(cur):
            base = cur
        else:
            base = os.path.dirname(cur)
        for _ in range(6):
            detail_db = os.path.join(base, DETAIL_USERS_DIRNAME, DETAIL_USERS_DB_FILENAME)
            treemap_db = os.path.join(base, TREE_MAP_DATA_DIRNAME, TREE_MAP_DB_FILENAME)
            if os.path.isfile(detail_db):
                return (
                    detail_db,
                    treemap_db if os.path.isfile(treemap_db) else None,
                )
            # Maybe `cur` is already the detail_users dir.
            inline_db = os.path.join(base, DETAIL_USERS_DB_FILENAME)
            if os.path.isfile(inline_db):
                tm = os.path.join(
                    os.path.dirname(base), TREE_MAP_DATA_DIRNAME, TREE_MAP_DB_FILENAME
                )
                return (inline_db, tm if os.path.isfile(tm) else None)
            parent = os.path.dirname(base)
            if parent == base:
                break
            base = parent
        return (None, None)

    @staticmethod
    def _configure_read_conn(conn: sqlite3.Connection, treemap_db: Optional[str]) -> None:
        cur = conn.cursor()
        try:
            cur.execute("PRAGMA query_only=1")
            cur.execute("PRAGMA mmap_size=268435456")
            cur.execute("PRAGMA cache_size=-65536")
        except sqlite3.DatabaseError:
            pass
        if treemap_db and os.path.isfile(treemap_db):
            cur.execute(
                "ATTACH DATABASE ? AS tm",
                (f"file:{treemap_db}?mode=ro",),
            )
        cur.close()

    @staticmethod
    def _has_treemap(conn: sqlite3.Connection) -> bool:
        try:
            row = conn.execute(
                "SELECT 1 FROM tm.dirs LIMIT 1"
            ).fetchone()
            return bool(row)
        except sqlite3.DatabaseError:
            return False

    def _build_path(self, conn: sqlite3.Connection, dir_id: int) -> str:
        """Reconstruct full path for a treemap dir_id via recursive CTE."""
        if dir_id is None:
            return ""
        try:
            row = conn.execute(
                """
                WITH RECURSIVE walk(id, parent_id, name_id, lvl) AS (
                    SELECT id, parent_id, name_id, 0 FROM tm.dirs WHERE id = ?
                    UNION ALL
                    SELECT d.id, d.parent_id, d.name_id, w.lvl + 1
                      FROM tm.dirs d JOIN walk w ON d.id = w.parent_id
                )
                SELECT GROUP_CONCAT(name, '/')
                  FROM (
                      SELECT n.name AS name FROM walk w
                        JOIN tm.names n ON w.name_id = n.id
                       ORDER BY w.lvl DESC
                  )
                """,
                (dir_id,),
            ).fetchone()
        except sqlite3.DatabaseError:
            return ""
        if not row or not row[0]:
            return ""
        joined = row[0]
        # Root segment is "/" so we can get "//foo/bar". Normalize.
        return "/" + joined.lstrip("/").lstrip("/")

    def _load_user_dirs(
        self, conn: sqlite3.Connection, user: str, top: int
    ) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            "SELECT uid, total_dirs, total_files, total_size FROM users WHERE username = ?",
            (user,),
        ).fetchone()
        if not row:
            return None
        uid, total_dirs, total_files, total_used = row
        scan_root = self._meta_get(conn, "scan_root")
        scan_ts = int(self._meta_get(conn, "scan_timestamp") or 0)

        # Always pull dir_user_size — it lives in detail.db, no ATTACH needed.
        # If treemap.db is also attached, we get human-readable paths via
        # recursive CTE; otherwise we fall back to "<dir id N>" labels so
        # the breakdown still appears (rather than silently disappearing
        # when a scan was run without --tree-map).
        has_tm = self._has_treemap(conn)
        dirs: List[Dict[str, Any]] = []
        cursor = conn.execute(
            "SELECT dir_id, size FROM dir_user_size "
            "WHERE uid = ? ORDER BY size DESC LIMIT ?",
            (uid, top),
        )
        for dir_id, size in cursor:
            path = self._build_path(conn, dir_id) if has_tm else f"<dir id {dir_id}>"
            dirs.append({"path": path, "used": int(size)})
        return {
            "date": scan_ts,
            "user": user,
            "directory": scan_root,
            "total_dirs": int(total_dirs or 0),
            "total_files": int(total_files or 0),
            "total_used": int(total_used or 0),
            "dirs": dirs,
        }

    def _load_user_files(
        self, conn: sqlite3.Connection, user: str, top: int
    ) -> Optional[Dict[str, Any]]:
        row = conn.execute(
            "SELECT uid, total_files, total_size FROM users WHERE username = ?",
            (user,),
        ).fetchone()
        if not row:
            return None
        uid, total_files, total_used = row
        scan_root = self._meta_get(conn, "scan_root")
        scan_ts = int(self._meta_get(conn, "scan_timestamp") or 0)

        files: List[Dict[str, Any]] = []
        has_tm = self._has_treemap(conn)
        cursor = conn.execute(
            """
            SELECT f.dir_id, n.name, e.ext, t.size
              FROM top_files t
              JOIN files f ON f.id = t.file_id
              JOIN names n ON f.name_id = n.id
              JOIN exts e  ON f.ext_id = e.id
             WHERE t.uid = ? AND t.rank <= ?
             ORDER BY t.rank
            """,
            (uid, top),
        )
        for dir_id, basename, ext, size in cursor:
            parent = self._build_path(conn, dir_id) if has_tm else ""
            full_path = f"{parent.rstrip('/')}/{basename}" if parent else basename
            files.append({"path": full_path, "size": int(size), "ext": ext or ""})
        return {
            "date": scan_ts,
            "user": user,
            "directory": scan_root,
            "total_files": int(total_files or 0),
            "total_used": int(total_used or 0),
            "files": files,
        }

    @staticmethod
    def _meta_get(conn: sqlite3.Connection, key: str) -> str:
        try:
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        except sqlite3.DatabaseError:
            return ""
        return str(row[0]) if row and row[0] is not None else ""
