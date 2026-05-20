"""
Report Formatter Module

Contains the ReportFormatter class for formatting and displaying reports.
"""

import json
import os
import re
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

from ..constants import (
    DETAIL_USERS_DB_FILENAME,
    DETAIL_USERS_DIRNAME,
    PERMISSION_ISSUES_DB_FILENAME,
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
        # ANSI color codes for highlighting search matches (bold yellow on
        # default background). Disabled when stdout is not a TTY (piped output)
        # or when NO_COLOR env var is set, per https://no-color.org/.
        self._color_on = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None
        self._hl_open = "\033[1;33m" if self._color_on else ""
        self._hl_close = "\033[0m" if self._color_on else ""

    def _apply_search_highlight(self, text: str, keyword: str) -> str:
        """Wrap occurrences of keyword in `text` with ANSI color codes
        (case-insensitive). Returns text unchanged if color is disabled or
        keyword is empty.
        """
        if not keyword or not self._color_on or not text:
            return text
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)
        return pattern.sub(
            lambda m: f"{self._hl_open}{m.group(0)}{self._hl_close}",
            text,
        )

    def _colorize_table(self, table_text: str, keyword: str) -> str:
        """Colorize all occurrences of keyword in a pre-formatted table.

        Apply AFTER `format_table` so column widths are calculated against
        plain text. ANSI codes are inserted via regex replace post-hoc.
        """
        if not keyword or not self._color_on:
            return table_text
        return self._apply_search_highlight(table_text, keyword)

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
        search: str = "",
    ) -> None:
        print("\n" + "=" * 60)
        print(f"DETAIL REPORT - {user}")
        print("=" * 60)

        if dir_report is None and file_report is None:
            if not_found_reason:
                print(f"  {not_found_reason}")
            else:
                print(f"  No data found for user '{user}'.")
            return

        kw = search.lower() if search else ""
        if search:
            print(f"Search: '{search}'")

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
                print("\n" + self._colorize_table(table, search))
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
                print("\n" + self._colorize_table(table, search))
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
        *,
        type_filter: str = "report",
        output_dir: str = ".",
        tree_path: str = "",
        tree_level: int = 3,
        tree_limit: int = 20,
        search: str = "",
    ) -> None:
        """Load and render detail reports for multiple users from SQLite.

        type_filter selects which section to render:
          report     — dirs + files (default)
          dirs       — directory breakdown only
          files      — file breakdown only
          inode      — directory breakdown sorted by file count
          permission — permission issues from permission_issues.db
          tree-map   — ASCII directory tree (requires treemap.db)
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

                # Route by type_filter
                if type_filter == "permission":
                    perm = self._load_user_permissions(output_dir, user, top, search=search)
                    self._display_permission_section(user, perm, search=search)
                    continue

                if type_filter == "inode":
                    inode = self._load_user_inodes(conn, user, top, search=search)
                    self._display_inode_section(user, inode, search=search)
                    continue

                # report / dirs / files all use detail_user_report rendering
                dir_data = self._load_user_dirs(conn, user, top, search=search) if type_filter in ("report", "dirs") else None
                file_data = self._load_user_files(conn, user, top, search=search) if type_filter in ("report", "files") else None
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
                self.display_user_detail_report(user, dir_data, file_data, top, search=search)
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
        self, conn: sqlite3.Connection, user: str, top: int, search: str = ""
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

        has_tm = self._has_treemap(conn)
        kw = search.lower() if search else ""
        # Load more rows when searching so we can post-filter to top N
        fetch_limit = top * 5 if kw else top
        dirs: List[Dict[str, Any]] = []
        cursor = conn.execute(
            "SELECT dir_id, size FROM dir_user_size "
            "WHERE uid = ? ORDER BY size DESC LIMIT ?",
            (uid, fetch_limit),
        )
        for dir_id, size in cursor:
            path = self._build_path(conn, dir_id) if has_tm else f"<dir id {dir_id}>"
            if kw and kw not in path.lower():
                continue
            dirs.append({"path": path, "used": int(size)})
            if len(dirs) >= top:
                break
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
        self, conn: sqlite3.Connection, user: str, top: int, search: str = ""
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
        kw = search.lower() if search else ""
        # Top_files only stores top-K per user; if filtering, expand the
        # rank window so we still find matches further down.
        rank_limit = top * 5 if kw else top
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
            (uid, rank_limit),
        )
        for dir_id, basename, ext, size in cursor:
            parent = self._build_path(conn, dir_id) if has_tm else ""
            full_path = f"{parent.rstrip('/')}/{basename}" if parent else basename
            if kw and kw not in full_path.lower():
                continue
            files.append({"path": full_path, "size": int(size), "ext": ext or ""})
            if len(files) >= top:
                break
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

    # ------------------------------------------------------------------ #
    # New type-filter query methods                                        #
    # ------------------------------------------------------------------ #

    def _load_user_inodes(
        self, conn: sqlite3.Connection, user: str, top: int, search: str = ""
    ) -> Optional[Dict[str, Any]]:
        """Load per-directory file count (inode) breakdown for a user."""
        row = conn.execute(
            "SELECT uid, total_files, total_dirs FROM users WHERE username = ?",
            (user,),
        ).fetchone()
        if not row:
            return None
        uid, total_files, total_dirs = row
        scan_root = self._meta_get(conn, "scan_root")
        scan_ts = int(self._meta_get(conn, "scan_timestamp") or 0)
        has_tm = self._has_treemap(conn)

        kw = search.lower() if search else ""
        fetch_limit = top * 5 if kw else top
        dirs: List[Dict[str, Any]] = []
        cursor = conn.execute(
            "SELECT dir_id, files, size FROM dir_user_size "
            "WHERE uid = ? ORDER BY files DESC LIMIT ?",
            (uid, fetch_limit),
        )
        for dir_id, files, size in cursor:
            path = self._build_path(conn, dir_id) if has_tm else f"<dir id {dir_id}>"
            if kw and kw not in path.lower():
                continue
            dirs.append({"path": path, "files": int(files), "size": int(size)})
            if len(dirs) >= top:
                break
        return {
            "date": scan_ts,
            "user": user,
            "directory": scan_root,
            "total_files": int(total_files or 0),
            "total_dirs": int(total_dirs or 0),
            "dirs": dirs,
        }

    @staticmethod
    def _load_user_permissions(
        output_dir: str, user: str, top: int, search: str = ""
    ) -> Optional[Dict[str, Any]]:
        """Load permission issues for a user from permission_issues.db."""
        perm_db = os.path.join(output_dir, PERMISSION_ISSUES_DB_FILENAME)
        if not os.path.isfile(perm_db):
            return None
        try:
            conn = sqlite3.connect(f"file:{perm_db}?mode=ro", uri=True)
            try:
                if search:
                    pattern = f"%{search}%"
                    total_row = conn.execute(
                        "SELECT COUNT(*) FROM issues WHERE user = ? AND path LIKE ? COLLATE NOCASE",
                        (user, pattern),
                    ).fetchone()
                    total = int(total_row[0]) if total_row else 0
                    rows = conn.execute(
                        "SELECT item_type, error, path FROM issues "
                        "WHERE user = ? AND path LIKE ? COLLATE NOCASE ORDER BY id LIMIT ?",
                        (user, pattern, top),
                    ).fetchall()
                else:
                    total_row = conn.execute(
                        "SELECT COUNT(*) FROM issues WHERE user = ?", (user,)
                    ).fetchone()
                    total = int(total_row[0]) if total_row else 0
                    rows = conn.execute(
                        "SELECT item_type, error, path FROM issues "
                        "WHERE user = ? ORDER BY id LIMIT ?",
                        (user, top),
                    ).fetchall()
            finally:
                conn.close()
        except sqlite3.DatabaseError:
            return None
        return {
            "total": total,
            "issues": [{"type": r[0], "error": r[1], "path": r[2]} for r in rows],
        }

    def _display_inode_section(
        self, user: str, data: Optional[Dict[str, Any]], search: str = ""
    ) -> None:
        print("\n" + "=" * 60)
        print(f"INODE REPORT - {user}")
        print("=" * 60)
        if not data:
            print(f"  No inode data found for user '{user}'.")
            return
        print(f"Total files : {data['total_files']:,}")
        print(f"Total dirs  : {data['total_dirs']:,}")
        if search:
            print(f"Search: '{search}'")
        dirs = data.get("dirs", [])
        if dirs:
            headers = ["Directory", "Files", "Size"]
            rows = [
                [d["path"], f"{d['files']:,}", format_size(d["size"])]
                for d in dirs
            ]
            title = f"Directory File Count (top {len(dirs):,} of {data['total_dirs']:,})"
            table = self.table_formatter.format_table(headers, rows, title=title)
            print("\n" + self._colorize_table(table, search))
        else:
            print("  (no directory data)")

    def _display_permission_section(
        self, user: str, data: Optional[Dict[str, Any]], search: str = ""
    ) -> None:
        print("\n" + "=" * 60)
        print(f"PERMISSION REPORT - {user}")
        print("=" * 60)
        if data is None:
            print("  permission_issues.db not found. Run a scan first.")
            return
        total = data.get("total", 0)
        issues = data.get("issues", [])
        print(f"Total permission issues: {total:,}")
        if search:
            print(f"Search: '{search}'")
        if issues:
            headers = ["Type", "Error", "Path"]
            rows = [[i["type"], i["error"], i["path"]] for i in issues]
            title = f"Permission Issues (top {len(issues):,} of {total:,})"
            table = self.table_formatter.format_table(headers, rows, title=title)
            print("\n" + self._colorize_table(table, search))
        else:
            print("  No permission issues found for this user.")

    # ------------------------------------------------------------------ #
    # Tree-map view (ASCII directory tree)                                #
    # ------------------------------------------------------------------ #

    def _find_dir_id_by_path(
        self, conn: sqlite3.Connection, path: str
    ) -> Optional[int]:
        """Resolve a filesystem path to tm.dirs.id. Returns None if not found.

        Empty path / "/" returns the scan root dir_id.
        """
        # Get scan root id
        try:
            row = conn.execute(
                "SELECT id FROM tm.dirs WHERE parent_id IS NULL LIMIT 1"
            ).fetchone()
        except sqlite3.DatabaseError:
            return None
        if not row:
            return None
        root_id = row[0]

        if not path or path == "/" or path.strip() == "":
            return root_id

        # Strip scan_root prefix if user passed an absolute path matching it
        scan_root = (self._meta_get(conn, "scan_root") or "").rstrip("/")
        normalized = path
        if scan_root and normalized.startswith(scan_root):
            normalized = normalized[len(scan_root):]

        segments = [s for s in normalized.strip("/").split("/") if s]
        if not segments:
            return root_id

        # Walk segments
        current_id = root_id
        for seg in segments:
            row = conn.execute(
                "SELECT d.id FROM tm.dirs d "
                "JOIN tm.names n ON d.name_id = n.id "
                "WHERE d.parent_id = ? AND n.name = ? LIMIT 1",
                (current_id, seg),
            ).fetchone()
            if not row:
                return None
            current_id = row[0]
        return current_id

    def display_user_tree(
        self,
        conn: sqlite3.Connection,
        user: str,
        start_path: str = "",
        max_depth: int = 3,
        limit: int = 20,
        search: str = "",
        visible_ids: Optional[set] = None,
    ) -> None:
        """Render an ASCII directory tree of the user's contribution under
        start_path, capped at max_depth levels and limit children per level.

        If `search` is set and `visible_ids` is provided, only branches with
        matching dirs are rendered; matching dirs are highlighted with `>>>`.
        """
        tw = max(40, int(getattr(self, "terminal_width", 80) or 80))

        row = conn.execute(
            "SELECT uid, total_size, total_files FROM users WHERE username = ?",
            (user,),
        ).fetchone()
        if not row:
            print(f"\n  User '{user}' not found.")
            return
        uid = row[0]

        start_id = self._find_dir_id_by_path(conn, start_path)
        if start_id is None:
            label = start_path or "<scan root>"
            print(f"\n  Path '{label}' not found in scan tree.")
            return

        scan_root = self._meta_get(conn, "scan_root") or "/"
        if start_id == 0 or not start_path:
            start_path_str = scan_root
        else:
            start_path_str = self._build_path(conn, start_id) or scan_root

        root_user = conn.execute(
            "SELECT size, files FROM dir_user_size WHERE uid = ? AND dir_id = ?",
            (uid, start_id),
        ).fetchone()

        print("\n" + "=" * 60)
        print(f"TREE-SHOW - {user}")
        print("=" * 60)
        print(f"Root path: {start_path_str}")
        if root_user:
            print(
                f"User '{user}': "
                f"{format_size(int(root_user[0]))}, "
                f"{int(root_user[1]):,} files"
            )
        else:
            print(f"User '{user}': no files in this subtree")
        if search:
            print(f"Search: '{search}'")
        print(f"Depth: {max_depth}  |  Limit: {limit} per level\n")

        self._render_tree_node(
            conn, uid, start_id, "", 0, max_depth, limit, tw,
            search=search, visible_ids=visible_ids,
        )

    def _render_tree_node(
        self,
        conn: sqlite3.Connection,
        uid: int,
        dir_id: int,
        prefix: str,
        depth: int,
        max_depth: int,
        limit: int,
        tw: int,
        search: str = "",
        visible_ids: Optional[set] = None,
    ) -> None:
        if depth >= max_depth:
            return

        children = conn.execute(
            """
            SELECT d.id, n.name, d.dir_count,
                   COALESCE(dus.size, 0) AS user_size,
                   COALESCE(dus.files, 0) AS user_files
              FROM tm.dirs d
              JOIN tm.names n ON d.name_id = n.id
              LEFT JOIN dir_user_size dus
                     ON dus.dir_id = d.id AND dus.uid = ?
             WHERE d.parent_id = ?
               AND COALESCE(dus.size, 0) > 0
             ORDER BY COALESCE(dus.size, 0) DESC
             LIMIT ?
            """,
            (uid, dir_id, limit + 1),
        ).fetchall()

        # Filter by visible_ids when search is active.
        if visible_ids is not None:
            children = [c for c in children if c[0] in visible_ids]

        has_more = len(children) > limit
        if has_more:
            children = children[:limit]

        kw_lower = search.lower() if search else ""
        n = len(children)
        for i, (cid, name, dir_count, user_size, user_files) in enumerate(children):
            is_last = (i == n - 1) and not has_more
            connector = "\\-- " if is_last else "|-- "
            child_prefix = prefix + ("    " if is_last else "|   ")

            is_match = bool(kw_lower) and kw_lower in name.lower()
            size_str = format_size(int(user_size))
            info = f"  [{size_str}, {int(user_files):,} files]"

            avail = tw - len(prefix) - len(connector) - len(info) - 1
            if avail < 4:
                avail = 4
            display_name = name if len(name) <= avail else (name[: max(1, avail - 2)] + "..")
            if is_match:
                display_name = self._apply_search_highlight(display_name, search)

            print(f"{prefix}{connector}{display_name}{info}")

            if dir_count > 0 and depth + 1 < max_depth:
                self._render_tree_node(
                    conn, uid, cid, child_prefix,
                    depth + 1, max_depth, limit, tw,
                    search=search, visible_ids=visible_ids,
                )

        if has_more:
            print(f"{prefix}\\-- ... ({limit}+ more, increase --limit to see)")

    # ------------------------------------------------------------------ #
    # Standalone --tree-show entry point                                   #
    # ------------------------------------------------------------------ #

    def _find_matching_dir_ids(
        self, conn: sqlite3.Connection, keyword: str
    ) -> set:
        """Return set of dir_ids whose name contains keyword (case-insensitive)."""
        if not keyword:
            return set()
        try:
            rows = conn.execute(
                "SELECT d.id FROM tm.dirs d "
                "JOIN tm.names n ON d.name_id = n.id "
                "WHERE n.name LIKE ? COLLATE NOCASE",
                (f"%{keyword}%",),
            ).fetchall()
        except sqlite3.DatabaseError:
            return set()
        return {r[0] for r in rows}

    def _collect_ancestors(
        self, conn: sqlite3.Connection, dir_ids: set
    ) -> set:
        """Walk parent chain for each dir_id, return set of all ancestor dir_ids."""
        visible = set(dir_ids)
        queue = list(dir_ids)
        while queue:
            batch = queue[:500]
            queue = queue[500:]
            placeholders = ",".join("?" * len(batch))
            try:
                rows = conn.execute(
                    f"SELECT id, parent_id FROM tm.dirs WHERE id IN ({placeholders})",
                    batch,
                ).fetchall()
            except sqlite3.DatabaseError:
                break
            for _did, parent_id in rows:
                if parent_id is not None and parent_id not in visible:
                    visible.add(parent_id)
                    queue.append(parent_id)
        return visible

    def display_tree_show(
        self,
        detail_db: Optional[str],
        treemap_db: Optional[str],
        users: List[str],
        path: str = "",
        level: int = 3,
        limit: int = 20,
        search: str = "",
    ) -> None:
        """Standalone entry point for --tree-show.

        - Without users: total dir tree from tm.dirs (all-user totals)
        - With users:    separate tree per user from dir_user_size
        - With search:   only branches containing matching dirs are rendered
        """
        if not treemap_db or not os.path.isfile(treemap_db):
            print("  Tree-show requires treemap.db. Run scan with --tree-map flag.")
            return

        # detail.db is required when filtering by user (dir_user_size lives there).
        # Without users we can rely solely on treemap.db.
        if users and (not detail_db or not os.path.isfile(detail_db)):
            print("  Tree-show with --user requires data_detail.db. Run scan first.")
            return

        if users and detail_db:
            conn = sqlite3.connect(f"file:{detail_db}?mode=ro", uri=True)
            owns_conn = True
        else:
            conn = sqlite3.connect(f"file:{treemap_db}?mode=ro", uri=True)
            owns_conn = True

        try:
            if users and detail_db:
                self._configure_read_conn(conn, treemap_db)
            else:
                # Treat opened treemap.db as the "tm" attached schema for path
                # helpers, and disable writes.
                try:
                    conn.execute("PRAGMA query_only = 1")
                except sqlite3.DatabaseError:
                    pass
                try:
                    conn.execute("ATTACH DATABASE ? AS tm",
                                  (f"file:{treemap_db}?mode=ro",))
                except sqlite3.DatabaseError:
                    # Already opened on the same file; ignore.
                    pass

            # Build visibility set when search is requested.
            visible_ids: Optional[set] = None
            if search:
                matching = self._find_matching_dir_ids(conn, search)
                if not matching:
                    print(f"  No directories found matching '{search}'.")
                    return
                visible_ids = self._collect_ancestors(conn, matching)
                visible_ids.update(matching)

            if not users:
                self._display_total_tree(conn, path, level, limit, search, visible_ids)
                return

            known = self._known_usernames(conn)
            for user in users:
                if user not in known:
                    print(f"\n  User '{user}' not found in scan results.")
                    continue
                self.display_user_tree(
                    conn, user, start_path=path,
                    max_depth=level, limit=limit,
                    search=search, visible_ids=visible_ids,
                )
        finally:
            if owns_conn:
                conn.close()

    def _display_total_tree(
        self,
        conn: sqlite3.Connection,
        path: str,
        level: int,
        limit: int,
        search: str,
        visible_ids: Optional[set],
    ) -> None:
        tw = max(40, int(getattr(self, "terminal_width", 80) or 80))
        start_id = self._find_dir_id_by_path(conn, path)
        if start_id is None:
            print(f"  Path '{path or '<root>'}' not found in scan tree.")
            return
        scan_root = self._meta_get(conn, "scan_root") or "/"
        start_str = self._build_path(conn, start_id) if path else scan_root
        if not start_str:
            start_str = scan_root

        print("\n" + "=" * 60)
        print("TREE-SHOW (all users)")
        print("=" * 60)
        print(f"Root path: {start_str}")
        if search:
            print(f"Search: '{search}'")
        print(f"Depth: {level}  |  Limit: {limit} per level\n")

        self._render_total_tree_node(
            conn, start_id, "", 0, level, limit, tw,
            search, visible_ids,
        )

    def _render_total_tree_node(
        self,
        conn: sqlite3.Connection,
        dir_id: int,
        prefix: str,
        depth: int,
        max_depth: int,
        limit: int,
        tw: int,
        search: str,
        visible_ids: Optional[set],
    ) -> None:
        if depth >= max_depth:
            return
        try:
            children = conn.execute(
                """
                SELECT d.id, n.name, d.dir_count, d.total_size, d.file_count
                  FROM tm.dirs d
                  JOIN tm.names n ON d.name_id = n.id
                 WHERE d.parent_id = ?
                 ORDER BY d.total_size DESC
                 LIMIT ?
                """,
                (dir_id, limit + 1),
            ).fetchall()
        except sqlite3.DatabaseError:
            return

        if visible_ids is not None:
            children = [c for c in children if c[0] in visible_ids]

        has_more = len(children) > limit
        if has_more:
            children = children[:limit]

        kw_lower = search.lower() if search else ""
        n = len(children)
        for i, (cid, name, dir_count, total_size, file_count) in enumerate(children):
            is_last = (i == n - 1) and not has_more
            connector = "\\-- " if is_last else "|-- "
            child_prefix = prefix + ("    " if is_last else "|   ")

            is_match = bool(kw_lower) and kw_lower in name.lower()
            info = f"  [{format_size(int(total_size))}, {int(file_count):,} files]"

            avail = tw - len(prefix) - len(connector) - len(info) - 1
            if avail < 4:
                avail = 4
            display_name = name if len(name) <= avail else (name[: max(1, avail - 2)] + "..")
            if is_match:
                display_name = self._apply_search_highlight(display_name, search)

            print(f"{prefix}{connector}{display_name}{info}")

            if dir_count > 0 and depth + 1 < max_depth:
                self._render_total_tree_node(
                    conn, cid, child_prefix,
                    depth + 1, max_depth, limit, tw,
                    search, visible_ids,
                )

        if has_more:
            print(f"{prefix}\\-- ... ({limit}+ more, increase --limit to see)")
