#!/usr/bin/env python3
"""
Disk Usage Checker - Main Script

This script provides a command-line interface for checking disk usage
across teams and users in a specified location.
"""

import datetime
import os
import signal
import sys
import time
import traceback

from src.cli_interface import CLIInterface
from src.config_manager import ConfigManager
from src.disk_scanner import DiskScanner
from src.report_generator import ReportGenerator
from src.sync_manager import AsyncSyncPipeline


def signal_handler(_sig, _frame):
    """Handle Ctrl+C gracefully."""
    print("\nScan interrupted. Cleaning up...", flush=True)
    sys.exit(1)


def _resolve_output_path(
    base: str,
    output_dir: str = None,
    output_override: str = None,
    prefix: str = None,
    add_date: bool = False,
):
    """
    Compute the final output file path plus the prefix and date suffix
    that sibling reports (permission_issues, top_user, etc.) should share.

    Returns:
        (output_file, prefix_str, date_str)
    """
    path = base

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, os.path.basename(path))
        print(f"Output directory set to: {output_dir}")

    if output_override:
        path = output_override

    date_str = datetime.datetime.now().strftime("%Y%m%d") if add_date else ""
    prefix_str = prefix or ""

    if prefix_str or date_str:
        dir_part = os.path.dirname(path)
        base_name, ext = os.path.splitext(os.path.basename(path))
        parts = [p for p in [prefix_str, base_name, date_str] if p]
        new_name = "_".join(parts) + ext
        path = os.path.join(dir_part, new_name) if dir_part else new_name
        print(f"Output file set to: {path}")

    return path, prefix_str, date_str


def _write_scan_status(out_dir: str, stage: str, running: bool, message: str = "", error: str = "", started_at: float = 0, phase_started_at: float = 0, tree_map_enabled: bool = False, sync_enabled: bool = False):
    try:
        import json
        import os
        import socket
        import time
        now = int(time.time())
        started = int(started_at) if started_at else now
        phase_started = int(phase_started_at) if phase_started_at else started
        status_path = os.path.join(out_dir, 'scan_status.json')
        tmp_path = status_path + '.tmp'
        payload = {
            "running": running,
            "stage": stage,
            "started_at": started,
            "phase_started_at": phase_started,
            "phase_elapsed_sec": max(0, now - phase_started),
            "total_elapsed_sec": max(0, now - started),
            "updated_at": now,
            "finished_at": now if not running else 0,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "message": message,
            "error": error,
            "tree_map_enabled": tree_map_enabled,
            "sync_enabled": sync_enabled,
        }
        with open(tmp_path, 'w') as f:
            json.dump(payload, f)
        os.replace(tmp_path, status_path)
        return status_path
    except Exception:
        pass
    return None

def _update_status(pipeline, out_dir: str, stage: str, running: bool, message: str = "", error: str = "", started_at: float = 0, phase_started_at: float = 0, tree_map_enabled: bool = False, sync_enabled: bool = False):
    """Write status JSON and (if pipeline is configured) immediately enqueue it for sync."""
    p = _write_scan_status(
        out_dir,
        stage,
        running,
        message,
        error,
        started_at,
        phase_started_at,
        tree_map_enabled,
        sync_enabled,
    )
    if p and pipeline:
        try:
            pipeline.enqueue_file(p)
        except Exception:
            pass


class _ScanStatusHeartbeat:
    """Background thread that touches scan_status.json so updated_at moves
    while long phases are running. Caller controls the active phase via
    set_phase(). Calling stop() halts the thread."""

    def __init__(
        self,
        out_dir: str,
        started_at: float,
        sync_pipeline=None,
        interval: float = 5.0,
        sync_interval: float = 30.0,
    ):
        import threading
        import time
        self.out_dir = out_dir
        self.started_at = started_at
        self.sync_pipeline = sync_pipeline
        self.tree_map_enabled = False
        self.sync_enabled = sync_pipeline is not None
        self.interval = interval
        self.sync_interval = max(interval, sync_interval)
        self._last_sync = time.monotonic()
        self._stop_evt = threading.Event()
        self._lock = threading.Lock()
        self._stage = "scan"
        self._message = ""
        self._phase_started_at = started_at
        self._thread = threading.Thread(target=self._run, name="scan-status-heartbeat", daemon=True)

    def set_phase(self, stage: str, message: str = ""):
        import time
        with self._lock:
            self._stage = stage
            self._message = message
            self._phase_started_at = time.time()
        # Write immediately so the new phase shows up without waiting for next tick
        _update_status(
            self.sync_pipeline,
            self.out_dir,
            stage,
            True,
            message,
            started_at=self.started_at,
            phase_started_at=self._phase_started_at,
            tree_map_enabled=self.tree_map_enabled,
            sync_enabled=self.sync_enabled,
        )

    def _run(self):
        import time
        while not self._stop_evt.wait(self.interval):
            with self._lock:
                stage = self._stage
                message = self._message
                phase_started_at = self._phase_started_at
            status_path = _write_scan_status(
                self.out_dir,
                stage,
                True,
                message,
                started_at=self.started_at,
                phase_started_at=phase_started_at,
                tree_map_enabled=self.tree_map_enabled,
                sync_enabled=self.sync_enabled,
            )
            now = time.monotonic()
            if status_path and self.sync_pipeline and now - self._last_sync >= self.sync_interval:
                try:
                    self.sync_pipeline.enqueue_file(status_path)
                    self._last_sync = now
                except Exception:
                    pass

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_evt.set()
        try:
            self._thread.join(timeout=2)
        except Exception:
            pass


def main():
    """Main entry point for the disk usage checker application."""

    # Set up signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    cli = CLIInterface()
    args = cli.parse_arguments()

    # Initialize the configuration manager
    config_manager = ConfigManager()

    # Handle CLI commands
    if args.init:
        if not args.dir:
            print("Error: --dir is required with --init")
            sys.exit(1)
        config_manager.initialize_config(args.dir)
        print(f"Configuration initialized with directory: {args.dir}")

    # Handle directory change
    elif args.dir and not args.run:
        config_manager.update_directory(args.dir)
        print(f"Directory in configuration updated to: {args.dir}")

    if args.add_team:
        config_manager.add_team(args.add_team)
        print(f"Team '{args.add_team}' added successfully")

    elif args.add_user:
        if not args.team:
            print("Error: --team is required with --add-user")
            sys.exit(1)

        # Process each user separately (already processed by CLIInterface)
        for user in args.add_user:
            exists = config_manager.add_user(user, args.team)
            if not exists:
                print(f"User '{user}' added to team '{args.team}'")

    elif args.remove_user:
        # Process each user separately (already processed by CLIInterface)
        for user in args.remove_user:
            config_manager.remove_user(user)

    elif args.list:
        config = config_manager.get_config()
        # Pass the team name as a filter if provided
        cli.display_config(config, args.team)

    elif args.show_report:
        # Check if --files is provided
        if not args.files:
            print("Error: --files is required with --show-report")
            print("Usage: disk_checker.py --show-report --files report1.json [report2.json ...]")
            print("       disk_checker.py --show-report --files \"disk_usage_*.json\"")
            sys.exit(1)

        # Display report(s) - file wildcards already expanded by CLIInterface
        cli.display_report(args.files, args.user, args.compare_by)

    elif args.check_users:
        # Resolve output directory and prefix (same flags as --run)
        output_dir = args.output_dir or "."
        prefix     = args.prefix    or ""
        users      = args.check_users

        # Expand multi-word single-arg (e.g. --check-users "Binh Minh")
        if len(users) == 1 and " " in users[0]:
            users = users[0].split()

        top = max(1, int(getattr(args, 'top', 30) or 30))
        cli.display_check_users(users, prefix=prefix, output_dir=output_dir, top=top)


    elif args.run:
        run_started_at = time.time()
        tree_map_enabled = bool(getattr(args, 'tree_map', False))

        # Load configuration
        config = config_manager.get_config()
        if not config:
            print("Error: No configuration found. Run --init first.")
            sys.exit(1)

        # Resolve the final output path (and carry prefix/date for sibling reports)
        output_file, output_prefix, output_date = _resolve_output_path(
            base=config.get('output_file', 'disk_usage_report.json'),
            output_dir=args.output_dir,
            output_override=args.output,
            prefix=args.prefix,
            add_date=args.date,
        )

        # Store resolved values so ReportGenerator can build sibling filenames correctly
        config['output_file'] = output_file
        config['output_prefix'] = output_prefix
        config['output_date_suffix'] = output_date
        config['debug'] = getattr(args, 'debug', False)

        # Allow one-shot scan directory override with --run --dir
        if getattr(args, 'dir', None):
            config['directory'] = args.dir
            print(f"Scan directory overridden for this run: {args.dir}")

        target_users = getattr(args, 'user', None)
        if target_users:
            original_users = {u['name']: u for u in config.get('users', [])}
            filtered_users = []
            for tu in target_users:
                if tu in original_users:
                    filtered_users.append(original_users[tu])
                else:
                    filtered_users.append({"name": tu, "team_id": None})
            config['users'] = filtered_users
            config['target_users_only'] = True

        heartbeat = None
        out_dir = '.'
        sync_pipeline = None
        try:
            # Safety check: refuse to scan if directory is not explicitly configured
            scan_dir = config.get('directory', '')
            if not scan_dir:
                print("Error: No scan directory configured. Run --init or set 'directory' in disk_checker_config.json.")
                return

            # Initialize async sync pipeline early (if --sync flag is provided)
            if getattr(args, 'sync', False):
                out_dir = os.path.dirname(config.get('output_file', 'disk_usage_report.json'))
                if not out_dir:
                    out_dir = "."
                sync_pipeline = AsyncSyncPipeline(
                    base_dir=out_dir,
                    user=getattr(args, 'sync_user', None),
                    host=getattr(args, 'sync_host', None),
                    dest_dir=getattr(args, 'sync_dest_dir', None),
                    password=getattr(args, 'sync_pass', None),
                )

            main_report_path = config.get('output_file', 'disk_usage_report.json')
            out_dir = os.path.dirname(main_report_path) if main_report_path else '.'

            # Initialize status heartbeat (touch every 5s so updated_at refreshes)
            heartbeat = _ScanStatusHeartbeat(out_dir, run_started_at, sync_pipeline, interval=5.0)
            heartbeat.tree_map_enabled = tree_map_enabled
            heartbeat.set_phase("scan", "Scanning filesystem")
            heartbeat.start()

            print("\n=== STARTING DISK USAGE SCAN ===")
            print(f"Target directory: {config.get('directory', '')}")

            report_generator = ReportGenerator(config)

            scanner = DiskScanner(
                config,
                max_workers=args.workers,
                debug=args.debug,
            )


            scan_results = scanner.scan()
            prefix = config.get('output_prefix', '')

            heartbeat.set_phase("report", "Generating main summary reports")
            print("\n=== PHASE 1: SUMMARY REPORTS ===")

            # Main summary report
            report_generator.generate_report(scan_results)

            # Enqueue main + sibling reports AFTER they are written to disk
            if sync_pipeline and main_report_path:
                sync_pipeline.enqueue_file(main_report_path)
                for rel_name in ['inode_usage_report.json', 'permission_issues.json']:
                    if prefix:
                        fname = f"{prefix}_{rel_name}"
                    else:
                        fname = rel_name
                    full_path = os.path.join(out_dir, fname)
                    if os.path.exists(full_path):
                        sync_pipeline.enqueue_file(full_path)

            # Per-user detail reports (dir + file) - runs with same
            # concurrency level as the scanner (Phase 1 workers reused)
            heartbeat.set_phase("detail", "Generating user detail reports")
            print("=== PHASE 2: DETAIL PIPELINE ===")
            tree_level = getattr(args, 'level', 3)
            created = report_generator.generate_detail_reports_with_level(
                scan_results,
                level=tree_level,
                max_workers=scanner.max_workers,
                build_treemap=bool(getattr(args, 'tree_map', False)),
            )
            # Batch-sync the detail_users/ directory (single rsync, much faster)
            if sync_pipeline and created:
                detail_dirs_seen = set()
                standalone_files = []
                for fpath in created:
                    parent = os.path.dirname(os.path.abspath(fpath))
                    if os.path.basename(parent) == "detail_users":
                        detail_dirs_seen.add(parent)
                    else:
                        standalone_files.append(fpath)
                for ddir in detail_dirs_seen:
                    sync_pipeline.enqueue_directory(ddir)
                for fpath in standalone_files:
                    sync_pipeline.enqueue_file(fpath)
            # Keep incremental-sync benefits by avoiding pre-run wipe.
            # Remove only stale detail files after new reports are generated.
            if not config.get('target_users_only', False):
                report_generator.cleanup_stale_detail_reports(created)

            # TreeMap report runs after detail export (Phase 3)
            if getattr(args, 'tree_map', False):
                heartbeat.set_phase("treemap", "Generating interactive tree map")
                level = getattr(args, 'level', 3)
                tree_map_path = report_generator.generate_tree_map(
                    scan_results,
                    level=level,
                    max_workers=scanner.max_workers,
                )
                if sync_pipeline and tree_map_path:
                    sync_pipeline.enqueue_file(tree_map_path)
                    tree_map_data = os.path.join(
                        os.path.dirname(tree_map_path), "tree_map_data"
                    )
                    if os.path.isdir(tree_map_data):
                        sync_pipeline.enqueue_directory(tree_map_data)

            if sync_pipeline:
                heartbeat.set_phase("sync", "Syncing reports to remote server")
                print("[SYNC] Waiting for async pipeline to complete...")
                sync_pipeline.wait()
                sync_pipeline.close()
                print("[SYNC] Async pipeline done")

            heartbeat.stop()
            _update_status(sync_pipeline, out_dir, "done", False, "Scan completed successfully", started_at=run_started_at, phase_started_at=run_started_at, tree_map_enabled=tree_map_enabled, sync_enabled=bool(sync_pipeline))
            print("\n=== SCAN COMPLETED SUCCESSFULLY ===")
            if main_report_path:
                print(f"Summary report: {main_report_path}")
            detail_manifest = os.path.join(out_dir, "detail_users", "data_detail.json")
            if os.path.exists(detail_manifest):
                print(f"Detail manifest: {detail_manifest}")
            total_elapsed = time.time() - run_started_at
            print(f"Total pipeline elapsed (wall-clock): {total_elapsed:.2f}s")

            if getattr(args, 'webhook_url', None):
                from src.msteams_notifier import send_msteams_notification
                send_msteams_notification(args.webhook_url, scan_results, config)

        except KeyboardInterrupt:
            if heartbeat:
                heartbeat.stop()
            _update_status(sync_pipeline, out_dir, "error", False, "Scan interrupted by user", started_at=run_started_at, phase_started_at=run_started_at, tree_map_enabled=tree_map_enabled, sync_enabled=bool(sync_pipeline))
            print("\nScan interrupted by user.")
            sys.exit(1)
        except Exception as e:
            if heartbeat:
                heartbeat.stop()
            _update_status(sync_pipeline, out_dir, "error", False, f"Scan failed: {e}", str(e), started_at=run_started_at, phase_started_at=run_started_at, tree_map_enabled=tree_map_enabled, sync_enabled=bool(sync_pipeline))
            print(f"\nUnexpected error: {e}")
            traceback.print_exc()
            sys.exit(1)

    else:
        cli.print_help()

if __name__ == "__main__":
    main()
