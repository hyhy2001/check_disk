"""
Report Formatter Module

Contains the ReportFormatter class for formatting and displaying reports.
"""

import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from .base_formatter import BaseFormatter
from .config_display import ConfigDisplay
from .report_comparison import ReportComparison
from .table_formatter import TableFormatter
from ..utils import format_size, format_timestamp


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
    ) -> None:
        """Render dir + file detail reports for a single user."""
        print("\n" + "=" * 60)
        print(f"DETAIL REPORT - {user}")
        print("=" * 60)

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
            print("  [dir detail report not found]")

        # --- File breakdown ---
        if file_report:
            files = file_report.get('files', [])
            total_files = file_report.get('total_files', len(files))
            total_used  = file_report.get('total_used', 0)
            print(f"\nTotal files: {total_files:,}  |  Total size: {format_size(total_used)}")

            display = files[:top]
            if display:
                headers = ["File", "Size"]
                rows = [[f['path'], format_size(f['size'])] for f in display]
                title = f"Largest Files (top {len(display)} of {total_files:,})"
                table = self.table_formatter.format_table(headers, rows, title=title)
                print("\n" + table)
            else:
                print("  (no file data)")
        else:
            print("  [file detail report not found]")

    def display_user_detail_reports(
        self,
        users: List[str],
        dir_files: Dict[str, Optional[str]],
        file_files: Dict[str, Optional[str]],
        top: int = 30,
    ) -> None:
        """Load and render detail reports for multiple users."""
        for user in users:
            dir_path  = dir_files.get(user)
            file_path = file_files.get(user)
            dir_data  = self._load_detail_report(dir_path, is_dir=True) if dir_path else None
            file_data = self._load_detail_report(file_path, is_dir=False) if file_path else None
            self.display_user_detail_report(user, dir_data, file_data, top)

    def _load_detail_report(self, path: str, is_dir: bool) -> Dict[str, Any]:
        """Load detail report from DB, NDJSON, or legacy JSON and normalize shape."""
        if path.endswith('.db'):
            return self._load_detail_from_db(path, is_dir)
        if path.endswith('.ndjson'):
            return self._load_detail_from_ndjson(path, is_dir)

        from ..utils import load_json_report
        return load_json_report(path)

    def _load_detail_from_db(self, path: str, is_dir: bool) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        conn = sqlite3.connect(path)
        try:
            if is_dir:
                meta_row = conn.execute(
                    "SELECT date, user, total_dirs, total_used FROM meta_dirs LIMIT 1"
                ).fetchone() if self._table_exists(conn, "meta_dirs") else None
                if meta_row:
                    data["date"] = int(meta_row[0] or 0)
                    data["user"] = meta_row[1] or ""
                    data["total_dirs"] = int(meta_row[2] or 0)
                    data["total_used"] = int(meta_row[3] or 0)
                else:
                    row = conn.execute(
                        "SELECT date, user, total_items, total_used FROM meta LIMIT 1"
                    ).fetchone()
                    if row:
                        data["date"] = int(row[0] or 0)
                        data["user"] = row[1] or ""
                        data["total_dirs"] = int(row[2] or 0)
                        data["total_used"] = int(row[3] or 0)
                rows = conn.execute("SELECT path, used FROM dirs ORDER BY used DESC").fetchall()
                data["dirs"] = [{"path": r[0], "used": int(r[1] or 0)} for r in rows]
            else:
                row = conn.execute(
                    "SELECT date, user, total_items, total_used FROM meta LIMIT 1"
                ).fetchone()
                if row:
                    data["date"] = int(row[0] or 0)
                    data["user"] = row[1] or ""
                    data["total_files"] = int(row[2] or 0)
                    data["total_used"] = int(row[3] or 0)
                rows = conn.execute("SELECT path, size FROM files ORDER BY size DESC").fetchall()
                data["files"] = [{"path": r[0], "size": int(r[1] or 0)} for r in rows]
        finally:
            conn.close()
        return data

    @staticmethod
    def _table_exists(conn, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE (type='table' OR type='view') AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None

    def _load_detail_from_ndjson(self, path: str, is_dir: bool) -> Dict[str, Any]:
        data: Dict[str, Any] = {"dirs": []} if is_dir else {"files": []}
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue

                if "_meta" in obj:
                    meta = obj.get("_meta") or {}
                    data["date"] = int(meta.get("date", 0) or 0)
                    data["user"] = meta.get("user", "")
                    if is_dir:
                        data["total_dirs"] = int(meta.get("total_dirs", 0) or 0)
                        data["total_used"] = int(meta.get("total_used", 0) or 0)
                    else:
                        data["total_files"] = int(meta.get("total_files", 0) or 0)
                        data["total_used"] = int(meta.get("total_used", 0) or 0)
                    continue

                if is_dir:
                    p = obj.get("path")
                    if p is not None:
                        data.setdefault("dirs", []).append({
                            "path": p,
                            "used": int(obj.get("used", 0) or 0),
                        })
                else:
                    p = obj.get("path")
                    if p is not None:
                        data.setdefault("files", []).append({
                            "path": p,
                            "size": int(obj.get("size", 0) or 0),
                        })
        return data
