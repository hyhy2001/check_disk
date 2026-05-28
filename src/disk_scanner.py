"""
Disk Scanner Module - Optimized Version
"""
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


def _detect_bind_mounts(scan_root):
    """Detect bind mount destinations under scan_root by reading
    /proc/self/mountinfo. Returns paths that should be skipped to avoid
    double-counting (bind mounts share inodes with their source).
    """
    import os
    skip = []
    try:
        # Track (dev, ino) of seen mount points. A duplicate means bind mount.
        seen = {}  # (dev, ino) -> first_path_seen
        with open("/proc/self/mountinfo", "r") as f:
            mounts = []
            for line in f:
                parts = line.split()
                if len(parts) < 5:
                    continue
                # Field index 4 = mount point
                mount_point = parts[4]
                mounts.append(mount_point)

        scan_root_abs = os.path.abspath(scan_root).rstrip("/") or "/"
        for mp in mounts:
            try:
                st = os.stat(mp)
            except OSError:
                continue
            key = (st.st_dev, st.st_ino)
            if key in seen:
                # Bind mount destination — skip whichever is under scan_root
                # (or the longer path, since it's likely the bind dest).
                first = seen[key]
                # Pick the one that's UNDER scan_root and isn't the original
                under_scan = []
                if mp == scan_root_abs or mp.startswith(scan_root_abs + "/"):
                    under_scan.append(mp)
                if first == scan_root_abs or first.startswith(scan_root_abs + "/"):
                    under_scan.append(first)
                if len(under_scan) >= 2:
                    # Both under scan root — skip the longer one (likely the bind dest)
                    bind_dest = max(under_scan, key=len)
                    if bind_dest not in skip:
                        skip.append(bind_dest)
                elif len(under_scan) == 1:
                    # One is under scan root, the other is outside.
                    # The one under scan root is the bind dest — skip it.
                    bind_dest = under_scan[0]
                    if bind_dest not in skip:
                        skip.append(bind_dest)
            else:
                seen[key] = mp
    except (IOError, OSError):
        pass
    return skip


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
        directory = self.config.get("directory", "/")
        if not os.path.isdir(directory):
            raise RuntimeError(f"Scan directory does not exist: {directory}")
        if self.use_rust:
            return self._rust_scan()
        return self._scanner.scan()

    def _rust_scan(self) -> ScanResult:
        """Execute the Rust core scanner.

        Orchestrates the Phase 1 pipeline by delegating to focused helpers:
        resolve target uids, invoke the Rust extension, classify usage,
        format permission issues, take a final filesystem snapshot, then
        save state and render the console summary.
        """
        print("\n[RUST] Initiating High-Performance Rust Core Engine")

        directory = self.config.get("directory", "/")
        skip_dirs = self.config.get("exclude_patterns", [])

        # Auto-skip container overlay/snapshot dirs that duplicate host data
        CONTAINER_SKIP_PREFIXES = [
            "/var/lib/containerd/io.containerd.snapshotter",
            "/var/lib/docker/overlay2",
            "/var/lib/docker/aufs",
            "/var/lib/lxc",
            "/var/lib/lxd/storage-pools",
        ]
        container_skips = [
            p for p in CONTAINER_SKIP_PREFIXES
            if p.startswith(directory.rstrip("/")) or directory == "/"
        ]
        if container_skips:
            skip_dirs = list(skip_dirs) + container_skips

        # Auto-skip .snapshot dirs under scan root (NFS/NetApp snapshots).
        # NetApp creates .snapshot at every volume/qtree level, not just root.
        # Find all .snapshot dirs (shallow search, max 3 levels) and add to skip_dirs
        # as prefix match — more reliable than relying on WalkBuilder name check.
        import os
        import subprocess
        scan_root_abs = os.path.abspath(directory).rstrip("/") or "/"
        try:
            find_proc = subprocess.run(
                ["find", scan_root_abs, "-maxdepth", "4", "-type", "d", "-name", ".snapshot"],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, timeout=30,
            )
            snapshot_dirs = [p.strip() for p in find_proc.stdout.splitlines() if p.strip()]
            if snapshot_dirs:
                skip_dirs = list(skip_dirs) + snapshot_dirs
                print(f"[SCAN] Detected {len(snapshot_dirs)} .snapshot dir(s) — will skip:")
                for sd in snapshot_dirs[:10]:
                    print(f"  {sd}")
                if len(snapshot_dirs) > 10:
                    print(f"  ... and {len(snapshot_dirs) - 10} more")
        except (subprocess.TimeoutExpired, OSError):
            # Fallback: just check root level
            snapshot_path = os.path.join(scan_root_abs, ".snapshot")
            if os.path.isdir(snapshot_path):
                skip_dirs = list(skip_dirs) + [snapshot_path]
                print(f"[SCAN] Detected .snapshot dir — will skip: {snapshot_path}")

        # Auto-detect bind mounts under scan root to avoid double-counting
        bind_mounts = _detect_bind_mounts(directory)
        if bind_mounts:
            print(f"[SCAN] Detected {len(bind_mounts)} bind mount(s) — will skip:")
            for bm in bind_mounts:
                print(f"  {bm}")
            skip_dirs = list(skip_dirs) + bind_mounts

        target_uids = self._resolve_target_uids()

        result, duration = self._invoke_rust_scanner(directory, skip_dirs, target_uids)
        self._print_phase1_summary(directory, result, duration)

        uid_cache = self._build_uid_cache(result)
        team_list, user_list, other_list, user_inode_list, \
            team_usage_results, user_usage_results, other_usage_results = \
            self._classify_usage(result, uid_cache)

        perm_by_user, perm_formatted = self._format_permission_issues(result, uid_cache)

        system_info = self._build_system_info(directory, result)
        self._print_disk_snapshot(system_info)

        self._save_display_state(
            system_info,
            team_usage_results,
            user_usage_results,
            other_usage_results,
            user_inode_list,
            perm_by_user,
        )
        if self.config.get('target_users_only', False):
            self._display_targeted_summary()
        else:
            self._display_scan_summary()

        return ScanResult(
            general_system=system_info,
            team_usage=team_list,
            user_usage=user_list,
            other_usage=other_list,
            timestamp=int(time.time()),
            top_dir=[],
            permission_issues=perm_formatted,
            user_inodes=user_inode_list,
            detail_tmpdir=result.get("detail_tmpdir", ""),
            detail_uid_username=uid_cache,
            dir_sizes_map={},
        )

    def _resolve_target_uids(self) -> List[int]:
        """Resolve --user filter to a list of POSIX uids, or None for all users."""
        if not self.config.get('target_users_only', False):
            return None
        users = self.config.get('users') or []
        if not users:
            return None
        import pwd
        uids = []
        for u in users:
            try:
                uids.append(pwd.getpwnam(u['name']).pw_uid)
            except KeyError:
                pass
        return uids

    def _invoke_rust_scanner(self, directory: str, skip_dirs: List[str], target_uids):
        """Call into the Rust extension and return (result_dict, duration_seconds)."""
        print("Calling fast_scanner.scan_disk()...")
        start = time.time()
        result = fast_scanner.scan_disk(
            directory, skip_dirs, target_uids, self.max_workers, self.debug
        )
        return result, time.time() - start

    def _print_phase1_summary(self, directory: str, result: Dict[str, Any], duration: float) -> None:
        """Print throughput / memory summary right after the Rust scan returns."""
        total_files = result.get('total_files', 0)
        total_dirs = result.get('total_dirs', 0)
        total_size = result.get('total_size', 0)
        avg_rate = total_files / duration if duration > 0 else 0
        mem_usage = _get_rss_mb()

        print(f"\n{'='*60}")
        print(f"SCAN COMPLETED in {format_time_duration(duration)}")
        print(f"{'='*60}")
        print(f"Directory scanned: {directory}")
        print(f"Total directories: {total_dirs:,}")
        print(f"Total files:      {total_files:,}")
        print(f"Total size:       {format_size(total_size)}")
        print(f"Scan rate:        {avg_rate:,.0f} files/sec")
        print(f"Memory usage:     {mem_usage:.1f} MB")

    def _build_uid_cache(self, result: Dict[str, Any]) -> Dict[int, str]:
        """Resolve every uid seen in the scan to a username (with /etc/passwd lookup)."""
        uid_cache: Dict[int, str] = {}
        for uid_str in result.get("uid_sizes", {}).keys():
            uid = int(uid_str)
            if uid not in uid_cache:
                uid_cache[uid] = get_username_from_uid(uid)
        return uid_cache

    def _classify_usage(self, result: Dict[str, Any], uid_cache: Dict[int, str]):
        """Split per-uid usage into configured users / teams / "Other".

        Returns a tuple of:
            (team_list, user_list, other_list, user_inode_list,
             team_usage_results, user_usage_results, other_usage_results)
        """
        user_usage_results = defaultdict(int)
        user_inode_results = defaultdict(int)
        team_usage_results = defaultdict(int)
        other_usage_results = defaultdict(int)

        valid_users = {u["name"]: u for u in self.config.get("users", [])}
        valid_teams = {t["name"]: t for t in self.config.get("teams", [])}
        target_users_only = self.config.get('target_users_only', False)
        uid_files_dict = result.get("uid_files", {})

        for uid_str, size in result.get("uid_sizes", {}).items():
            uid = int(uid_str)
            username = uid_cache[uid]
            file_count = uid_files_dict.get(uid_str, 0)

            if username in valid_users:
                user_usage_results[username] += size
                user_inode_results[username] += file_count
                team_id = valid_users[username].get("team_id")
                team_name = next(
                    (t for t, v in valid_teams.items() if v.get("team_id") == team_id),
                    "Other",
                )
                team_usage_results[team_name] += size
            elif not target_users_only:
                other_usage_results[username] += size
                user_inode_results[username] += file_count

        user_list = ScanHelper.create_user_list(user_usage_results)
        team_list = ScanHelper.create_user_list(team_usage_results)
        other_list = ScanHelper.create_user_list(other_usage_results)

        user_inode_list = [
            {"name": name, "inodes": count} for name, count in user_inode_results.items()
        ]
        user_inode_list.sort(key=lambda x: x["inodes"], reverse=True)

        other_total = sum(item["used"] for item in other_list)
        team_list.append({"name": "Other", "used": other_total})

        return (
            team_list,
            user_list,
            other_list,
            user_inode_list,
            team_usage_results,
            user_usage_results,
            other_usage_results,
        )

    def _format_permission_issues(self, result: Dict[str, Any], uid_cache: Dict[int, str]):
        """Reshape the flat Rust permission_issues into the nested report format.

        Returns (perm_by_user, perm_formatted).
        """
        rust_perm_flat = result.get("permission_issues", [])
        valid_users = {u["name"]: u for u in self.config.get("users", [])}
        target_users_only = self.config.get('target_users_only', False)

        perm_by_user: Dict[str, List] = {}
        for item in rust_perm_flat:
            path = item.get("path", "")
            kind = item.get("type", "unknown")
            err = item.get("error", "")

            uid_value = item.get("uid")
            if uid_value is not None:
                owner = uid_cache.get(uid_value, get_username_from_uid(uid_value, uid_cache))
            else:
                owner = "unknown"

            if target_users_only and owner not in valid_users:
                continue
            perm_by_user.setdefault(owner, []).append(
                {"path": path, "type": kind, "error": err}
            )

        # Phase 1 may only emit permission_issues_count to bound RAM; fall
        # back to the actual list length when paths were emitted.
        permission_issues_count = int(
            result.get("permission_issues_count", len(rust_perm_flat))
        )

        perm_formatted = {
            "users": [],
            "unknown_items": perm_by_user.get("unknown", []),
            "count": permission_issues_count,
        }
        for owner, issues in sorted(perm_by_user.items()):
            if owner != "unknown":
                perm_formatted["users"].append(
                    {"name": owner, "inaccessible_items": issues}
                )
        return perm_by_user, perm_formatted

    def _build_system_info(self, directory: str, result: Dict[str, Any]) -> Dict[str, int]:
        """Take the post-scan filesystem snapshot used by both report and console."""
        system_info = get_general_system_info(directory)
        system_info["inodes_scanned"] = result.get("total_inodes", 0)
        return system_info

    def _print_disk_snapshot(self, system_info: Dict[str, int]) -> None:
        """Print the final-snapshot disk capacity block."""
        print(f"{'='*60}")
        print("Disk Information (final snapshot):")
        total_capacity = float(system_info.get('total', 0) or 0)
        used_space = float(system_info.get('used', 0) or 0)
        used_percent = (used_space * 100.0 / total_capacity) if total_capacity > 0 else 0.0
        print(f"  Total capacity: {format_size(total_capacity)}")
        print(f"  Used space:     {format_size(used_space)} ({used_percent:.1f}%)")
        print(f"  Available:      {format_size(system_info.get('available', 0))}")
        print(f"{'='*60}")

    def _save_display_state(
        self,
        system_info: Dict[str, int],
        team_usage_results,
        user_usage_results,
        other_usage_results,
        user_inode_list,
        permission_issues,
    ) -> None:
        """Stash data on the instance so display_*_summary methods can render."""
        self.general_system = system_info
        self.team_usage_results = team_usage_results
        self.user_usage_results = user_usage_results
        self.other_usage_results = other_usage_results
        self.user_inode_list = user_inode_list
        self.permission_issues = permission_issues



    def _display_targeted_summary(self) -> None:
        """Display a specialized summary for targeted scans (--user)."""
        from .formatters.table_formatter import TableFormatter
        table_formatter = TableFormatter()

        target_names = [u["name"] for u in self.config.get("users", [])]
        print(f"\nTargeted scan summary for user(s): {', '.join(target_names)}")

        headers = ["Username", "Disk Usage", "Inodes", "Percent"]
        rows = []
        total_capacity = self.general_system.get('total', 1)

        # Get inode counts mapped by username
        inode_map = {item["name"]: item["inodes"] for item in getattr(self, "user_inode_list", [])}

        for user in target_names:
            size = self.user_usage_results.get(user, 0)
            inodes = inode_map.get(user, 0)
            percent = (size / total_capacity) * 100
            usage_bar = self._create_usage_bar(percent, width=15)
            rows.append([user, format_size(size), f"{inodes:,}", f"{usage_bar} {percent:.1f}%"])

        if rows:
            table = table_formatter.format_table(headers, rows, title="Targeted User Usage")
            print(table)
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

