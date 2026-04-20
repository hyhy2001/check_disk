#!/usr/bin/env python3
"""
Disk Usage Checker - Main Script

This script provides a command-line interface for checking disk usage
across teams and users in a specified location.
"""

import os
import sys
import signal
import time
import traceback
import datetime
from src.config_manager import ConfigManager
from src.disk_scanner import DiskScanner
from src.report_generator import ReportGenerator
from src.cli_interface import CLIInterface
from src.sync_manager import ReportSyncer

def signal_handler(sig, frame):
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

        cli.display_check_users(users, prefix=prefix, output_dir=output_dir)


    elif args.run:
        run_started_at = time.time()

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

        try:
            print("\n=== STARTING DISK USAGE SCAN ===")
            print(f"Target directory: {config.get('directory', '')}")

            report_generator = ReportGenerator(config)
            report_generator.clear_old_detail_reports()

            scanner = DiskScanner(
                config,
                max_workers=args.workers,
                debug=args.debug,
            )

            scan_results = scanner.scan()

            print("\n=== GENERATING REPORTS ===")

            # Main summary report
            report_generator.generate_report(scan_results)
            print(f"Main report: {config.get('output_file', 'disk_usage_report.json')}")

            # Per-user detail reports (dir + file) - runs with same
            # concurrency level as the scanner (Phase 1 workers reused)
            created = report_generator.generate_detail_reports(
                scan_results,
                max_workers=scanner.max_workers,
            )

            # TreeMap report runs after detail export (Phase 3)
            if getattr(args, 'tree_map', False):
                level = getattr(args, 'level', 3)
                report_generator.generate_tree_map(scan_results, level=level)

            print("\n=== SCAN COMPLETED SUCCESSFULLY ===")
            total_elapsed = time.time() - run_started_at
            print(f"Total pipeline elapsed (wall-clock): {total_elapsed:.2f}s")

            if getattr(args, 'sync', False):
                out_dir = os.path.dirname(config.get('output_file', ''))
                if not out_dir:
                    out_dir = "."
                
                ReportSyncer.sync_to_remote(
                    output_dir=out_dir,
                    user=getattr(args, 'sync_user', None),
                    host=getattr(args, 'sync_host', None),
                    dest_dir=getattr(args, 'sync_dest_dir', None),
                    password=getattr(args, 'sync_pass', None)
                )

            if getattr(args, 'webhook_url', None):
                from src.msteams_notifier import send_msteams_notification
                send_msteams_notification(args.webhook_url, scan_results, config)

        except KeyboardInterrupt:
            print("\nScan interrupted by user.")
            sys.exit(1)
        except Exception as e:
            print(f"\nUnexpected error: {e}")
            traceback.print_exc()
            sys.exit(1)
    
    else:
        cli.print_help()

if __name__ == "__main__":
    main()
