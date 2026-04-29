"""
Disk Scanner Module - Optimized Version
"""
import glob as glob_module
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

from .utils import (
    ScanHelper,
    create_usage_bar,
    format_size,
    format_time_duration,
    get_general_system_info,
    get_username_from_uid,
)

try:
    from src import fast_scanner
    _HAS_FAST_SCANNER = True
except ImportError:
    fast_scanner = None  # type: ignore[assignment]
    _HAS_FAST_SCANNER = False


def _get_rss_mb() -> float:
    """Read RSS memory from /proc/self/status in MB (Linux only) to avoid psutil dependency."""
    try:
        with open('/proc/self/status') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) / 1024.0
    except Exception:
        pass
    return 0.0


@dataclass
class ScanResult:
    """Class for storing scan results"""
    general_system: Dict[str, int]
    team_usage: List[Dict[str, Any]]
    user_usage: List[Dict[str, Any]]
    other_usage: List[Dict[str, Any]]
    timestamp: int
    top_dir: List[Dict[str, Any]] = field(default_factory=list)
    permission_issues: Dict[str, Any] = field(default_factory=dict)
    user_inodes: List[Dict[str, Any]] = field(default_factory=list)
    # username -> [(path, size), ...] sorted by size desc (legacy / small scans)
    detail_files: Dict[str, List[Tuple[str, int]]] = field(default_factory=dict)
    # Streaming mode: temp dir + uid mapping populated instead of detail_files
    detail_tmpdir: str = ""
    detail_uid_username: Dict[int, str] = field(default_factory=dict)  # uid -> username
    # TreeMap fast-path: dir -> {username -> direct_size}
    dir_sizes_map: Dict[str, Dict[str, int]] = field(default_factory=dict)


class DiskScanner:
    """Proxy class routing scanning to the high-performance Rust core or Python fallback"""

    def __init__(self, config: Dict[str, Any], max_workers: int = None, debug: bool = False):
        import os
        self.config = config
        self.max_workers = (
            max_workers if max_workers
            else config.get("workers", min(32, (os.cpu_count() or 1) * 2))
        )
        self.debug = debug
        self.use_rust = config.get("use_rust", True) and _HAS_FAST_SCANNER

        if not self.use_rust:
            raise NotImplementedError("Rust core 'fast_scanner' is required.")
        self._scanner = None

    def scan(self) -> ScanResult:
        if self.use_rust:
            return self._rust_scan()
        return self._scanner.scan()

    def _rust_scan(self) -> ScanResult:
        """Execute the Rust core scanner"""
        print("\n[RUST] Initiating High-Performance Rust Core Engine")

        directory = self.config.get("directory", "/")
        skip_dirs = self.config.get("exclude_patterns", [])
        target_users_only = self.config.get('target_users_only', False)
        target_uids = None
        if target_users_only and self.config.get('users'):
            import pwd
            target_uids = []
            for u in self.config.get('users'):
                try:
                    target_uids.append(pwd.getpwnam(u['name']).pw_uid)
                except KeyError:
                    pass

        print("Calling fast_scanner.scan_disk()...")
        start = time.time()
        # New Rust ABI supports max_workers as the 4th arg.
        # Keep backward compatibility with old prebuilt .so.
        try:
            result = fast_scanner.scan_disk(directory, skip_dirs, target_uids, self.max_workers)
        except TypeError:
            result = fast_scanner.scan_disk(directory, skip_dirs, target_uids)
        duration = time.time() - start

        from .utils import format_size

        mem_usage = _get_rss_mb()

        total_files = result.get('total_files', 0)
        total_dirs = result.get('total_dirs', 0)
        total_size = result.get('total_size', 0)
        avg_rate = total_files / duration if duration > 0 else 0

        print(f"\n{'='*60}")
        print(f"SCAN COMPLETED in {format_time_duration(duration)}")
        print(f"{'='*60}")
        print(f"Directory scanned: {directory}")
        print(f"Total directories: {total_dirs:,}")
        print(f"Total files:      {total_files:,}")
        print(f"Total size:       {format_size(total_size)}")
        print(f"Scan rate:        {avg_rate:,.0f} files/sec")
        print(f"Memory usage:     {mem_usage:.1f} MB")

        # Build UID mapping

        uid_cache = {}
        for uid_str in result.get("uid_sizes", {}).keys():
            uid = int(uid_str)
            if uid not in uid_cache:
                uid_cache[uid] = get_username_from_uid(uid)

        user_usage_results = defaultdict(int)
        user_inode_results = defaultdict(int)
        team_usage_results = defaultdict(int)
        other_usage_results = defaultdict(int)

        valid_users = {u["name"]: u for u in self.config.get("users", [])}
        valid_teams = {t["name"]: t for t in self.config.get("teams", [])}

        uid_files_dict = result.get("uid_files", {})

        for uid_str, size in result.get("uid_sizes", {}).items():
            uid = int(uid_str)
            username = uid_cache[uid]
            file_count = uid_files_dict.get(uid_str, 0)

            if username in valid_users:
                user_usage_results[username] += size
                user_inode_results[username] += file_count
                team_id = valid_users[username].get("team_id")
                team_name = next((t for t, v in valid_teams.items() if v.get("team_id") == team_id), "Other")
                team_usage_results[team_name] += size
            elif not self.config.get('target_users_only', False):
                other_usage_results[username] += size
                user_inode_results[username] += file_count

        user_list = ScanHelper.create_user_list(user_usage_results)
        team_list = ScanHelper.create_user_list(team_usage_results)
        other_list = ScanHelper.create_user_list(other_usage_results)

        user_inode_list = [{"name": name, "inodes": count} for name, count in user_inode_results.items()]
        user_inode_list.sort(key=lambda x: x["inodes"], reverse=True)

        other_total = sum(item["used"] for item in other_list)
        team_list.append({"name": "Other", "used": other_total})


        dir_sizes_map: Dict[str, Dict[str, int]] = {}


        # Build permission_issues in the same nested format as Python legacy
        rust_perm_flat = result.get("permission_issues", [])
        perm_by_user: Dict[str, List] = {}
        for item in rust_perm_flat:
            path = item.get("path", "")
            kind = item.get("type", "unknown")
            err  = item.get("error", "")

            uid_value = item.get("uid")
            if uid_value is not None:
                # Dùng uid trực tiếp kết hợp bộ đệm `uid_cache` để lấy chuẩn user name
                owner = uid_cache.get(uid_value, get_username_from_uid(uid_value, uid_cache))
            else:
                owner = "unknown"

            if self.config.get('target_users_only', False) and owner not in valid_users:
                continue
            perm_by_user.setdefault(owner, []).append({"path": path, "type": kind, "error": err})

        # Format into users and unknown_items exactly like legacy Python expected output
        perm_formatted = {
            "users": [],
            "unknown_items": perm_by_user.get("unknown", [])
        }
        for owner, issues in sorted(perm_by_user.items()):
            if owner != "unknown":
                perm_formatted["users"].append({
                    "name": owner,
                    "inaccessible_items": issues
                })

        # Re-use LegacyDiskScanner's table formatting for the console summary
        # Use one final filesystem snapshot after scan for consistency
        # between terminal output and report JSON.
        system_info = get_general_system_info(directory)
        system_info["inodes_scanned"] = result.get("total_inodes", 0)

        print(f"{'='*60}")
        print("Disk Information (final snapshot):")
        print(f"  Total capacity: {format_size(system_info.get('total', 0))}")
        print(f"  Used space:     {format_size(system_info.get('used', 0))} ({system_info.get('used', 0) * 100 / system_info.get('total', 1):.1f}%)")
        print(f"  Available:      {format_size(system_info.get('available', 0))}")
        print(f"{'='*60}")

        self.general_system = system_info
        self.team_usage_results = team_usage_results
        self.user_usage_results = user_usage_results
        self.other_usage_results = other_usage_results
        self.permission_issues = perm_by_user
        self._display_scan_summary()

        return ScanResult(
            general_system=system_info,
            team_usage=team_list,
            user_usage=user_list,
            other_usage=other_list,
            timestamp=int(time.time()),
            top_dir=[],  # deprecated — derive from dir_sizes_map instead
            permission_issues=perm_formatted,
            user_inodes=user_inode_list,
            detail_tmpdir=result.get("detail_tmpdir", ""),
            detail_uid_username=uid_cache,
            dir_sizes_map=dir_sizes_map,
        )


    def _display_scan_summary(self) -> None:
        """Display a summary of the scan results."""
        # Import TableFormatter here to avoid circular imports
        from .formatters.table_formatter import TableFormatter
        table_formatter = TableFormatter()

        # Display team usage report
        if self.team_usage_results:
            print("\nTeam disk usage summary:")

            # Create table for team summary
            headers = ["Team", "Disk Usage", "Percent"]
            rows = []

            # Get total disk capacity for percentage calculation
            total_capacity = self.general_system.get('total', 1)

            # Add "Other" category to team usage
            other_total = sum(self.other_usage_results.values())
            team_usage_with_other = dict(self.team_usage_results)
            team_usage_with_other["Other"] = other_total

            for team, size in sorted(team_usage_with_other.items(), key=lambda x: x[1], reverse=True):
                percent = (size / total_capacity) * 100
                usage_bar = self._create_usage_bar(percent)
                rows.append([team, format_size(size), f"{usage_bar} {percent:.1f}%"])

            if rows:
                table = table_formatter.format_table(headers, rows, title="Team Disk Usage")
                print(table)

        # Show top users in console
        print("\nTop users by disk usage:")
        headers = ["Username", "Disk Usage", "Percent"]
        rows = []
        total_capacity = self.general_system.get('total', 1)
        for user, size in sorted(self.user_usage_results.items(), key=lambda x: x[1], reverse=True)[:20]:
            percent = (size / total_capacity) * 100
            usage_bar = self._create_usage_bar(percent)
            rows.append([user, format_size(size), f"{usage_bar} {percent:.1f}%"])
        if rows:
            table = table_formatter.format_table(headers, rows, title="Top 20 Users by Disk Usage")
            print(table)

        # Show top OTHER users in console
        if self.other_usage_results:
            print("\nTop other users by disk usage:")
            headers = ["Username", "Disk Usage", "Percent"]
            rows = []
            for user, size in sorted(self.other_usage_results.items(), key=lambda x: x[1], reverse=True)[:20]:
                percent = (size / total_capacity) * 100
                usage_bar = self._create_usage_bar(percent)
                rows.append([user, format_size(size), f"{usage_bar} {percent:.1f}%"])
            if rows:
                table = table_formatter.format_table(headers, rows, title="Top 20 Other Users by Disk Usage")
                print(table)

        # Display users with permission issues
        if self.permission_issues:
            print("\nUsers with permission issues:")

            # Create table for permission issues
            headers = ["Username", "Inaccessible Items"]
            rows = []

            for username, issues in sorted(self.permission_issues.items()):
                rows.append([username, len(issues)])

            if rows:
                table = table_formatter.format_table(headers, rows, title="Permission Issues (Count by User)")
                print(table)

    def _create_usage_bar(self, percent: float, width: int = 20) -> str:
        """Delegate to shared utility."""
        return create_usage_bar(percent, width)

