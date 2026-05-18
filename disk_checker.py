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
from src.constants import (
    DEFAULT_REPORT_FILENAME,
    DETAIL_USERS_DB_FILENAME,
    DETAIL_USERS_DIRNAME,
    SIBLING_REPORT_FILENAMES,
    TREE_MAP_DATA_DIRNAME,
    TREE_MAP_DB_FILENAME,
)
from src.disk_scanner import DiskScanner
from src.report_generator import ReportGenerator
from src.scan_status import ScanStatusHeartbeat, update_status
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
            base=config.get('output_file', DEFAULT_REPORT_FILENAME),
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
                out_dir = os.path.dirname(config.get('output_file', DEFAULT_REPORT_FILENAME))
                if not out_dir:
                    out_dir = "."
                sync_pipeline = AsyncSyncPipeline(
                    base_dir=out_dir,
                    user=getattr(args, 'sync_user', None),
                    host=getattr(args, 'sync_host', None),
                    dest_dir=getattr(args, 'sync_dest_dir', None),
                    password=getattr(args, 'sync_pass', None),
                )

            main_report_path = config.get('output_file', DEFAULT_REPORT_FILENAME)
            out_dir = os.path.dirname(main_report_path) if main_report_path else '.'

            # Initialize status heartbeat (touch every 5s so updated_at refreshes)
            heartbeat = ScanStatusHeartbeat(out_dir, run_started_at, sync_pipeline)
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
                for rel_name in SIBLING_REPORT_FILENAMES:
                    fname = f"{prefix}_{rel_name}" if prefix else rel_name
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
            # Sync the detail/treemap SQLite DB files (single rsync per file).
            if sync_pipeline and created:
                for fpath in created:
                    if os.path.isfile(fpath):
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
                if sync_pipeline and tree_map_path and os.path.isfile(tree_map_path):
                    sync_pipeline.enqueue_file(tree_map_path)

            if sync_pipeline:
                heartbeat.set_phase("sync", "Syncing reports to remote server")
                print("[SYNC] Waiting for async pipeline to complete...")
                sync_pipeline.wait()
                print("[SYNC] Async pipeline done")

            heartbeat.stop()
            update_status(sync_pipeline, out_dir, "done", False, "Scan completed successfully", started_at=run_started_at, phase_started_at=run_started_at, tree_map_enabled=tree_map_enabled, sync_enabled=bool(sync_pipeline))
            if sync_pipeline:
                sync_pipeline.wait()
                sync_pipeline.close()
            print("\n=== SCAN COMPLETED SUCCESSFULLY ===")
            if main_report_path:
                print(f"Summary report: {main_report_path}")
            detail_db = os.path.join(out_dir, DETAIL_USERS_DIRNAME, DETAIL_USERS_DB_FILENAME)
            if os.path.exists(detail_db):
                print(f"Detail DB:      {detail_db}")
            treemap_db = os.path.join(out_dir, TREE_MAP_DATA_DIRNAME, TREE_MAP_DB_FILENAME)
            if os.path.exists(treemap_db):
                print(f"TreeMap DB:     {treemap_db}")
            total_elapsed = time.time() - run_started_at
            print(f"Total pipeline elapsed (wall-clock): {total_elapsed:.2f}s")

            if getattr(args, 'webhook_url', None):
                from src.msteams_notifier import send_msteams_notification
                send_msteams_notification(args.webhook_url, scan_results, config)

        except KeyboardInterrupt:
            if heartbeat:
                heartbeat.stop()
            update_status(sync_pipeline, out_dir, "error", False, "Scan interrupted by user", started_at=run_started_at, phase_started_at=run_started_at, tree_map_enabled=tree_map_enabled, sync_enabled=bool(sync_pipeline))
            print("\nScan interrupted by user.")
            sys.exit(1)
        except Exception as e:
            if heartbeat:
                heartbeat.stop()
            update_status(sync_pipeline, out_dir, "error", False, f"Scan failed: {e}", str(e), started_at=run_started_at, phase_started_at=run_started_at, tree_map_enabled=tree_map_enabled, sync_enabled=bool(sync_pipeline))
            err_text = str(e)
            if "No space left on device" in err_text or "ENOSPC" in err_text:
                print(f"\nTemporary storage is full: {e}")
                sys.exit(2)
            print(f"\nUnexpected error: {e}")
            traceback.print_exc()
            sys.exit(1)

    else:
        cli.print_help()

if __name__ == "__main__":
    main()
