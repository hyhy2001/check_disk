"""
Disk Scanner Module - Optimized Version
"""
import atexit
import glob as glob_module
import os
import shutil
import stat as stat_module
import tempfile
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from os import scandir
from typing import Any, Dict, List, Optional, Set, Tuple

from .utils import (
    ScanHelper,
    build_uid_cache,
    create_usage_bar,
    format_size,
    format_time_duration,
    get_actual_disk_usage,
    get_general_system_info,
    get_owner_from_path,
    get_username_from_uid,
)


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

try:
    import .fast_scanner  # noqa: F401
    HAS_RUST_CORE = True
except ImportError:
    HAS_RUST_CORE = False


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
    # username -> [(path, size), ...] sorted by size desc (legacy / small scans)
    detail_files: Dict[str, List[Tuple[str, int]]] = field(default_factory=dict)
    # Streaming mode: temp dir + uid mapping populated instead of detail_files
    detail_tmpdir: str = ""
    detail_uid_username: Dict[int, str] = field(default_factory=dict)  # uid -> username


@dataclass
class ThreadStats:
    """Class for tracking thread-specific statistics"""
    files: int = 0
    dirs: int = 0
    total_size: int = 0
    uid_sizes: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    dir_sizes: Dict[str, Dict[int, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    permission_issues: Dict[str, List[Dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    # In-memory buffer flushed periodically to tmpdir (never holds full 36M entries)
    file_paths: Dict[int, List[Tuple[str, int]]] = field(default_factory=lambda: defaultdict(list))
    file_path_count: int = 0  # total unflushed entries across all uids
    flush_count: int = 0      # Increment each time to ensure chunk uniqueness


class LegacyDiskScanner:
    """Optimized disk scanner with per-thread queues and streaming file-path flush."""

    BATCH_SIZE            = 2000   # dirs to steal from global queue at once
    DETAIL_FLUSH_THRESHOLD = 200000  # flush when a thread accumulates this many file entries

    def __init__(self, config: Dict[str, Any], max_workers: int = None, debug: bool = False):
        """
        Initialize the disk scanner.

        Args:
            config: Configuration dictionary
            max_workers: Number of worker threads (None for auto)
            debug: Enable debug output
        """
        self.config = config
        self.location = config.get("directory", "")
        self.debug = debug

        # Build user-team mapping
        self.user_map = self._build_user_team_mapping()

        # Cache for UID to username mapping
        self.uid_to_username = build_uid_cache()

        # Determine optimal thread count
        self.max_workers = self._determine_thread_count(max_workers)

        # Results storage
        self.user_usage_results = {}
        self.team_usage_results = {}
        self.other_usage_results = {}
        self.dir_usage_results = {}
        self.file_paths_results: Dict[str, List[Tuple[str, int]]] = {}  # username -> sorted top-N
        self.permission_issues = {}

        # Hard-link dedup
        self.hardlink_inodes: Set[Tuple[int, int]] = set()
        self.inode_lock = threading.Lock()
        self.root_dev: int = 0

        # Temp directory for streaming file-path flush (auto-cleaned on exit)
        self._tmpdir = tempfile.mkdtemp(prefix='diskscanner_detail_')
        atexit.register(shutil.rmtree, self._tmpdir, ignore_errors=True)

        # Progress reporting
        self.progress_interval = 10.0  # Report progress every 10 seconds

        # Store general system info
        self.general_system = get_general_system_info(self.location)

        self.critical_skip_dirs = [
            # Snapshot directories (most important)
            '.snapshot', '.snapshots', '.zfs',
            # System virtual directories
            'proc', 'sys', 'dev',
            # Network special directories
            '.nfs'
        ]
    def _build_user_team_mapping(self) -> Dict[str, str]:
        """Build mapping from username to team name"""
        user_map = {}
        team_id_to_name = {team["team_ID"]: team["name"] for team in self.config.get("teams", [])}

        for user in self.config.get("users", []):
            username = user["name"]
            team_id = user["team_ID"]
            if team_id in team_id_to_name:
                user_map[username] = team_id_to_name[team_id]

        return user_map


    def _determine_thread_count(self, max_workers: Optional[int]) -> int:
        """Determine optimal thread count based on system resources"""
        if max_workers is not None:
            return max_workers

        cpu_count = os.cpu_count() or 8
        # Auto: 2x CPU cores, capped at 64 for high-latency storage (NFS/SAN)
        return min(cpu_count * 2, 64)

    def _worker(self, thread_id: int,
                work_queues: List[List[str]],
                thread_stats: List[ThreadStats],
                global_queue: deque,
                global_lock: threading.Lock,
                active_workers: List[int],
                done_flag: List[bool],
                work_event: threading.Event) -> None:
        """Worker thread function for parallel directory scanning"""
        my_stats = thread_stats[thread_id]
        my_queue = work_queues[thread_id]

        MAX_DIR_PROCESS_TIME = 1800  # Maximum time to spend on a single directory (s)

        while not done_flag[0]:
            # --- Grab work ---
            current_dir = None

            if my_queue:
                current_dir = my_queue.pop()
            else:
                # Batch-steal from global queue (deque.pop() atomic under GIL)
                stolen = 0
                while stolen < self.BATCH_SIZE:
                    try:
                        my_queue.append(global_queue.pop())
                        stolen += 1
                    except IndexError:
                        break
                if my_queue:
                    current_dir = my_queue.pop()

            # --- No work available: sleep until signalled ---
            if current_dir is None:
                active_workers[thread_id] = 0

                # Done when: global empty, all local queues empty, no active workers
                with global_lock:
                    if (not global_queue
                            and not any(work_queues)
                            and sum(active_workers) == 0):
                        done_flag[0] = True
                        work_event.set()   # wake all sleeping siblings
                        break

                # Sleep until another thread pushes new directories
                work_event.clear()
                work_event.wait(timeout=0.05)
                continue

            active_workers[thread_id] = 1
            dir_start_time = time.time()

            # --- Scan directory ---
            subdirs: List[str] = []
            dir_uid_sizes: Dict[int, int] = defaultdict(int)

            try:
                with scandir(current_dir) as it:
                    for entry in it:
                        if time.time() - dir_start_time > MAX_DIR_PROCESS_TIME:
                            if self.debug:
                                print(f"Thread {thread_id}: Directory taking too long: {current_dir}")
                            username = get_owner_from_path(current_dir, self.uid_to_username)
                            my_stats.permission_issues[username].append({
                                "path": current_dir,
                                "type": "directory",
                                "error": "Directory processing timeout"
                            })
                            break

                        try:
                            # Single stat call per entry (was 2-4 syscalls before)
                            st = entry.stat(follow_symlinks=False)
                            mode = st.st_mode

                            if stat_module.S_ISLNK(mode):
                                continue

                            elif stat_module.S_ISDIR(mode):
                                # Fast path: name-based skip (no extra stat)
                                if entry.name in self.critical_skip_dirs:
                                    if self.debug:
                                        print(f"Skipping critical directory: {entry.path}")
                                    continue
                                # Cross-device check: snapshot / NFS / bind-mount
                                if self.root_dev and st.st_dev != self.root_dev:
                                    if self.debug:
                                        print(f"Skipping cross-device directory: {entry.path} "
                                              f"(dev {st.st_dev} != root {self.root_dev})")
                                    continue
                                subdirs.append(entry.path)

                            elif stat_module.S_ISREG(mode):
                                # Hard-link fast path: st_nlink == 1 means no dedup needed
                                if st.st_nlink == 1:
                                    process_file = True
                                else:
                                    inode_key = (st.st_ino, st.st_dev)
                                    with self.inode_lock:
                                        if inode_key not in self.hardlink_inodes:
                                            self.hardlink_inodes.add(inode_key)
                                            process_file = True
                                        else:
                                            process_file = False

                                if process_file:
                                    size = get_actual_disk_usage(st)
                                    if size > 0:
                                        uid = st.st_uid
                                        my_stats.uid_sizes[uid] += size
                                        dir_uid_sizes[uid] += size
                                        my_stats.total_size += size
                                        my_stats.files += 1
                                        my_stats.file_paths[uid].append((entry.path, size))
                                        my_stats.file_path_count += 1
                                        # Flush to disk when buffer is full to keep RAM bounded
                                        if my_stats.file_path_count >= self.DETAIL_FLUSH_THRESHOLD:
                                            self._flush_thread_paths(my_stats, thread_id)

                        except OSError as e:
                            username = get_owner_from_path(entry.path, self.uid_to_username)
                            my_stats.permission_issues[username].append({
                                "path": entry.path,
                                "type": "unknown",
                                "error": str(e)
                            })

            except OSError as e:
                username = get_owner_from_path(current_dir, self.uid_to_username)
                my_stats.permission_issues[username].append({
                    "path": current_dir,
                    "type": "directory",
                    "error": str(e)
                })

            my_stats.dirs += 1

            if dir_uid_sizes:
                my_stats.dir_sizes[current_dir] = dict(dir_uid_sizes)

            # --- Distribute subdirs, wake sleeping threads ---
            if subdirs:
                my_queue.append(subdirs[0])
                if len(subdirs) > 1:
                    global_queue.extend(subdirs[1:])
                # Signal that new work is available
                work_event.set()

    # ── Streaming file-path helpers ──────────────────────────────────────────

    def _flush_thread_paths(self, stats: ThreadStats, thread_id: int) -> None:
        """Append buffered file paths to per-uid temp TSV files, then free RAM.

        Each chunk is written **sorted descending by size** so that later
        k-way merges with ``heapq.merge(reverse=True)`` produce a globally
        sorted output without loading the full list into memory.

        TSV format: ``<size_bytes>\t<absolute_path>\n``
        """
        stats.flush_count += 1
        for uid, files in stats.file_paths.items():
            if not files:
                continue
            # Sort descending so heapq.merge(reverse=True) works correctly
            files.sort(key=lambda x: x[1], reverse=True)
            tmpfile = os.path.join(self._tmpdir, f"uid_{uid}_t{thread_id}_c{stats.flush_count}.tsv")
            with open(tmpfile, 'w', encoding='utf-8', errors='surrogateescape') as fh:
                for path, size in files:
                    fh.write(f"{size}\t{path}\n")
            files.clear()
        stats.file_path_count = 0

    def _stream_merge_to_results(self) -> None:
        """Collect uid→username mapping from temp files.

        No file data is loaded into memory here.  The actual per-user detail
        JSON reports are written by report_generator using streaming I/O.
        """
        uid_set: Set[int] = set()
        for fpath in glob_module.glob(os.path.join(self._tmpdir, 'uid_*_t*.tsv')):
            fname = os.path.basename(fpath)
            try:
                uid = int(fname.split('_')[1])
                uid_set.add(uid)
            except (IndexError, ValueError):
                pass

        self._detail_uid_username: Dict[int, str] = {
            uid: get_username_from_uid(uid, self.uid_to_username)
            for uid in uid_set
        }
        print(f"  {len(uid_set)} UIDs ready for streaming write "
              f"(all files, no cap)")

    def get_tree_size(self, path: str) -> int:
        """
        Calculate the total size of a directory tree.

        Args:
            path: Root directory path

        Returns:
            Total size in bytes
        """
        start_time = time.time()

        # Capture root device ID once: any subdir on a different device is a
        # cross-mount point (NFS, snapshot, bind-mount) and will be skipped.
        try:
            self.root_dev = os.stat(path).st_dev
        except OSError:
            self.root_dev = 0  # unknown: skip device check

        print(f"Starting scan with {self.max_workers} workers...")  # full scan

        thread_stats = [ThreadStats() for _ in range(self.max_workers)]
        work_queues = [[] for _ in range(self.max_workers)]

        global_queue: deque = deque([path])
        global_lock = threading.Lock()  # Used only for done-detection snapshot
        work_event = threading.Event()  # Signalled whenever new dirs are pushed
        work_event.set()  # initial seed is already in queue

        active_workers = [0] * self.max_workers
        done_flag = [False]

        last_report = [start_time]
        last_files = [0]
        last_dirs = [0]
        peak_rate = [0]

        # Add stalled detection (use lists for shared state)
        last_progress_time = [time.time()]
        last_progress_files = [0]
        stalled_time = [0]
        MAX_STALL_TIME = 1800  # Maximum time to allow stalled progress (seconds)

        def report_progress():
            """Progress reporting thread function"""
            while not done_flag[0]:
                time.sleep(0.5)
                current_time = time.time()

                # Check for stalled progress
                total_files = sum(s.files for s in thread_stats)
                if total_files == last_progress_files[0]:
                    stalled_time[0] = current_time - last_progress_time[0]
                    if stalled_time[0] > MAX_STALL_TIME:
                        print(f"\nWARNING: Scan appears stalled for {stalled_time[0]:.1f} seconds. Checking for stuck workers...")

                        # Check which workers are active but not making progress
                        stuck_workers = []
                        for i, active in enumerate(active_workers):
                            if active == 1:
                                stuck_workers.append(i)

                        if stuck_workers:
                            print(f"Workers {stuck_workers} appear to be stuck. Attempting to terminate scan gracefully.")
                            done_flag[0] = True
                            break
                else:
                    last_progress_files[0] = total_files
                    last_progress_time[0] = current_time
                    stalled_time[0] = 0

                if current_time - last_report[0] >= self.progress_interval:
                    elapsed = current_time - start_time
                    total_files = sum(s.files for s in thread_stats)
                    total_dirs = sum(s.dirs for s in thread_stats)
                    total_size = sum(s.total_size for s in thread_stats)
                    interval = current_time - last_report[0]
                    file_rate = (total_files - last_files[0]) / interval if interval > 0 else 0

                    if file_rate > peak_rate[0]:
                        peak_rate[0] = file_rate
                    active = sum(active_workers)
                    local_q = sum(len(q) for q in work_queues)
                    global_q = len(global_queue)  # len(deque) is atomic under GIL

                    # Get memory usage
                    mem_usage = _get_rss_mb()

                    # Check memory limit
                    if hasattr(self, 'max_memory_mb') and mem_usage > getattr(self, 'max_memory_mb', 20480):
                        print(f"\nWARNING: Memory usage ({mem_usage:.1f} MB) exceeds limit. Terminating scan.")
                        done_flag[0] = True
                        return

                    # Compact progress output with memory usage
                    print(f"[{format_time_duration(elapsed)}] Files: {total_files:,} | Dirs: {total_dirs:,} | Size: {format_size(total_size)} | Rate: {file_rate:,.0f} files/s | Workers: {active}/{self.max_workers} | Queue: {local_q + global_q:,} | Mem: {mem_usage:.1f} MB")
                    last_report[0] = current_time
                    last_files[0] = total_files
                    last_dirs[0] = total_dirs

        # Start progress reporting thread
        progress_thread = threading.Thread(target=report_progress, daemon=True)
        progress_thread.start()

        # Start worker threads
        threads = []
        for i in range(self.max_workers):
            t = threading.Thread(
                target=self._worker,
                args=(i, work_queues, thread_stats, global_queue, global_lock,
                      active_workers, done_flag, work_event),
                daemon=True
            )
            t.start()
            threads.append(t)

        try:
            # Wait for all threads to complete
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            print("\nInterrupted!")
            done_flag[0] = True
        finally:
            done_flag[0] = True
            progress_thread.join(timeout=1)

        elapsed = time.time() - start_time

        # Merge results from all threads
        print("\nMerging results...")

        self._process_scan_results(thread_stats)

        # Calculate and display statistics
        total_files = sum(s.files for s in thread_stats)
        total_dirs = sum(s.dirs for s in thread_stats)
        total_size = sum(s.total_size for s in thread_stats)
        avg_rate = total_files / elapsed if elapsed > 0 else 0

        # Get memory usage
        mem_usage = _get_rss_mb()

        # Get general system info
        system = self.general_system

        print(f"\n{'='*60}")
        print(f"SCAN COMPLETED in {format_time_duration(elapsed)}")
        print(f"{'='*60}")
        print(f"Directory scanned: {self.location}")
        print(f"Total directories: {total_dirs:,}")
        print(f"Total files:      {total_files:,}")
        print(f"Total size:       {format_size(total_size)}")
        print(f"Scan rate:        {avg_rate:,.0f} files/sec (peak: {peak_rate[0]:,.0f})")
        print(f"Memory usage:     {mem_usage:.1f} MB")
        print(f"{'='*60}")
        print("Disk Information:")
        print(f"  Total capacity: {format_size(system.get('total', 0))}")
        print(f"  Used space:     {format_size(system.get('used', 0))} ({system.get('used', 0) * 100 / system.get('total', 1):.1f}%)")
        print(f"  Available:      {format_size(system.get('available', 0))}")
        print(f"{'='*60}")

        self._display_scan_summary()

        return total_size

    def _process_scan_results(self, thread_stats: List[ThreadStats]) -> None:
        """
        Process and merge results from all worker threads.

        Args:
            thread_stats: List of ThreadStats objects from worker threads
        """
        # Clear previous results
        self.user_usage_results.clear()
        self.team_usage_results.clear()
        self.other_usage_results.clear()
        self.dir_usage_results.clear()
        self.file_paths_results.clear()
        self.permission_issues.clear()

        # Initialize team usage counters
        for team in self.config.get("teams", []):
            self.team_usage_results[team["name"]] = 0

        # Merge UID sizes from all threads
        merged_uid_sizes: Dict[int, int] = defaultdict(int)
        for stats in thread_stats:
            for uid, size in stats.uid_sizes.items():
                merged_uid_sizes[uid] += size

            # Merge permission issues
            for username, issues in stats.permission_issues.items():
                if username not in self.permission_issues:
                    self.permission_issues[username] = []
                self.permission_issues[username].extend(issues)

        # Process user data using ScanHelper
        self.user_usage_results, self.team_usage_results, self.other_usage_results = (
            ScanHelper.process_user_data(merged_uid_sizes, self.uid_to_username, self.user_map)
        )

        # Flush any remaining in-memory file-path buffers to tmpdir, then
        # stream-merge the temp files to produce top-N results per user.
        # This is the key memory optimization: peak RAM for file paths is
        # O(threads x FLUSH_THRESHOLD) during scan, and O(top_n) during merge.
        print("  Flushing remaining file-path buffers to disk...")
        for i, stats in enumerate(thread_stats):
            self._flush_thread_paths(stats, i)

        self._stream_merge_to_results()

        # Process directory data
        self._process_directory_data(thread_stats)

    # Kept for API compatibility; streaming version replaces the in-memory sort.
    def _merge_file_paths(self, merged: Dict[int, List[Tuple[str, int]]]) -> None:
        """Deprecated: use _stream_merge_to_results() instead."""
        for uid, files in merged.items():
            username = get_username_from_uid(uid, self.uid_to_username)
            files.sort(key=lambda x: x[1], reverse=True)
            self.file_paths_results[username] = files

    def _process_directory_data(self, thread_stats: List[ThreadStats]) -> None:
        """
        Process directory data from all worker threads.

        Args:
            thread_stats: List of ThreadStats objects from worker threads
        """
        print("Processing directory data...")
        dir_uid_merged = {}

        # Process each thread's directory data separately
        for stats in thread_stats:
            for dir_path, uid_sizes in stats.dir_sizes.items():
                if dir_path not in dir_uid_merged:
                    dir_uid_merged[dir_path] = defaultdict(int)

                for uid, size in uid_sizes.items():
                    dir_uid_merged[dir_path][uid] += size

        # Convert directory data to user format
        print("Converting directory data to user format...")
        for dir_path, uid_sizes in dir_uid_merged.items():
            self.dir_usage_results[dir_path] = {}
            for uid, size in uid_sizes.items():
                username = get_username_from_uid(uid, self.uid_to_username)
                self.dir_usage_results[dir_path][username] = size

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
        all_users = {**self.user_usage_results, **self.other_usage_results}
        for user, size in sorted(all_users.items(), key=lambda x: x[1], reverse=True)[:20]:
            percent = (size / total_capacity) * 100
            usage_bar = self._create_usage_bar(percent)
            rows.append([user, format_size(size), f"{usage_bar} {percent:.1f}%"])
        if rows:
            table = table_formatter.format_table(headers, rows, title="Top 20 Users by Disk Usage")
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

    def scan(self) -> ScanResult:
        """
        Perform a complete disk scan.

        Returns:
            ScanResult object with all scan data
        """
        print(f"Scanning: {self.location}")
        print(f"Workers: {self.max_workers}\n")

        try:
            self.get_tree_size(self.location)

            # Create user_list and other_list with all users (no filtering)
            user_list = ScanHelper.create_user_list(self.user_usage_results)
            team_list = ScanHelper.create_user_list(self.team_usage_results)
            other_list = ScanHelper.create_user_list(self.other_usage_results)

            # Add "Other" category to team list
            other_total = sum(item["used"] for item in other_list)
            team_list.append({"name": "Other", "used": other_total})

            # Create top directories list
            print("Creating directory details list...")
            top_dir_list = self._create_top_dir_list()

            # Format permission issues
            permission_issues_data = self._format_permission_issues()

            print(f"Found {len(top_dir_list)} directory entries")

            return ScanResult(
                general_system=self.general_system,
                team_usage=team_list,
                user_usage=user_list,
                other_usage=other_list,
                timestamp=int(time.time()),
                top_dir=top_dir_list,
                permission_issues=permission_issues_data,
                # Streaming mode: file detail is written directly from tmpdir
                detail_files={},
                detail_tmpdir=self._tmpdir,
                detail_uid_username=getattr(self, '_detail_uid_username', {}),
            )
        except Exception:
            import traceback
            traceback.print_exc()
            return ScanResult(
                general_system=self.general_system,
                team_usage=[], user_usage=[], other_usage=[],
                timestamp=int(time.time())
            )

    def _create_top_dir_list(self) -> List[Dict[str, Any]]:
        """
        Create a list of top directories by usage.

        Returns:
            List of directory dictionaries
        """
        # Always generate the directory list for all users with usage
        top_dir_list = []
        relevant_users = set(self.user_usage_results.keys()) | set(self.other_usage_results.keys())

        for dir_path, user_sizes in self.dir_usage_results.items():
            for username, size in user_sizes.items():
                if username in relevant_users and size > 0:
                    top_dir_list.append({
                        "dir": dir_path,
                        "user": username,
                        "user_usage": size
                    })

        top_dir_list.sort(key=lambda x: x["user_usage"], reverse=True)
        return top_dir_list

    def _format_permission_issues(self) -> Dict[str, Any]:
        """
        Format permission issues in the nested structure.
        """
        users_list = []

        # Named users — sorted alphabetically
        for username in sorted(k for k in self.permission_issues if k != "unknown"):
            inaccessible_items = []
            for issue in self.permission_issues[username]:
                inaccessible_items.append({
                    "path":  issue.get("path",  ""),
                    "type":  issue.get("type",  ""),
                    "error": issue.get("error", ""),
                })

            users_list.append({
                "name": username,
                "inaccessible_items": inaccessible_items
            })

        # Unknown / orphan items
        unknown_items = []
        for issue in self.permission_issues.get("unknown", []):
            unknown_items.append({
                "path":  issue.get("path",  ""),
                "type":  issue.get("type",  ""),
                "error": issue.get("error", ""),
            })

        return {
            "users": users_list,
            "unknown_items": unknown_items
        }


