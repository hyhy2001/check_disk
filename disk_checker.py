#!/usr/bin/env python3
"""
Disk Usage Checker - Main Script

This script provides a command-line interface for checking disk usage
across teams and users in a specified location.
"""

import datetime
import glob as _glob
import os
import shutil
import signal
import sys
import tempfile
import time
import traceback

from src.cli_interface import CLIInterface
from src.config_manager import ConfigManager
from src.constants import (
    DEFAULT_REPORT_FILENAME,
    DETAIL_USERS_DB_FILENAME,
    DETAIL_USERS_DIRNAME,
    INODE_USAGE_REPORT_FILENAME,
    PERMISSION_ISSUES_DB_FILENAME,
    SIBLING_REPORT_FILENAMES,
    TREE_MAP_DATA_DIRNAME,
    TREE_MAP_DB_FILENAME,
)
from src.disk_scanner import DiskScanner
from src.report_generator import ReportGenerator
from src.scan_status import ScanStatusHeartbeat, update_status
from src.sync_manager import AsyncSyncPipeline


def signal_handler(_sig, _frame):
    """Handle Ctrl+C by raising KeyboardInterrupt instead of exiting hard.

    Python's default SIGINT handler already raises KeyboardInterrupt, which
    ``cmd_run`` catches to drain the sync pipeline, stop the heartbeat, and
    clean up the Rust temp dir. The custom message just makes the intent
    visible before the exception propagates.
    """
    print("\nScan interrupted. Cleaning up...", flush=True)
    raise KeyboardInterrupt


def _resolve_output_path(
    base: str,
    output_dir: str = None,
    output_override: str = None,
    prefix: str = None,
    add_date: bool = False,
):
    """
    Compute the final output file path plus the prefix and date suffix
    that sibling reports (permission_issues, inode_usage, etc.) should share.

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


# ─── Subcommand handlers ────────────────────────────────────────────────


def cmd_init(args, config_manager: ConfigManager) -> None:
    if not args.dir:
        print("Error: --dir is required with --init")
        sys.exit(1)
    config_manager.initialize_config(args.dir)
    print(f"Configuration initialized with directory: {args.dir}")


def cmd_update_directory(args, config_manager: ConfigManager) -> None:
    config_manager.update_directory(args.dir)
    print(f"Directory in configuration updated to: {args.dir}")


def cmd_add_team(args, config_manager: ConfigManager) -> None:
    config_manager.add_team(args.add_team)
    print(f"Team '{args.add_team}' added successfully")


def cmd_add_user(args, config_manager: ConfigManager) -> None:
    if not args.team:
        print("Error: --team is required with --add-user")
        sys.exit(1)
    for user in args.add_user:
        exists = config_manager.add_user(user, args.team)
        if not exists:
            print(f"User '{user}' added to team '{args.team}'")


def cmd_remove_user(args, config_manager: ConfigManager) -> None:
    for user in args.remove_user:
        config_manager.remove_user(user)


def cmd_list(args, cli: CLIInterface, config_manager: ConfigManager) -> None:
    config = config_manager.get_config()
    cli.display_config(config, args.team)


def cmd_show_report(args, cli: CLIInterface) -> None:
    if not args.files:
        print("Error: --files is required with --show-report")
        print("Usage: disk_checker.py --show-report --files report1.json [report2.json ...]")
        print("       disk_checker.py --show-report --files \"disk_usage_*.json\"")
        sys.exit(1)
    cli.display_report(args.files, args.user, args.compare_by)


def cmd_detail(args, cli: CLIInterface) -> None:
    output_dir = args.output_dir or "."
    prefix = args.prefix or ""
    users = getattr(args, "user", None) or []
    if isinstance(users, str):
        users = users.split()

    # Expand multi-word single-arg (e.g. --user "Binh Minh")
    if len(users) == 1 and " " in users[0]:
        users = users[0].split()

    if not users:
        print("Error: --detail requires --user USER [USER ...] to specify user(s).")
        return

    top = max(1, int(getattr(args, "top", 30) or 30))
    type_filter = getattr(args, "type", "report") or "report"
    tree_path = getattr(args, "path", "") or ""
    tree_level = max(1, int(getattr(args, "level", 3) or 3))
    tree_limit = max(1, int(getattr(args, "limit", 20) or 20))
    search = getattr(args, "search", "") or ""
    cli.display_check_users(
        users,
        prefix=prefix,
        output_dir=output_dir,
        top=top,
        type_filter=type_filter,
        tree_path=tree_path,
        tree_level=tree_level,
        tree_limit=tree_limit,
        search=search,
    )


def cmd_tree_show(args, cli: CLIInterface) -> None:
    output_dir = args.output_dir or "."
    users = getattr(args, "user", None) or []
    if isinstance(users, str):
        users = users.split()
    path = getattr(args, "path", "") or ""
    level = max(1, int(getattr(args, "level", 3) or 3))
    limit = max(1, int(getattr(args, "limit", 20) or 20))
    search = getattr(args, "search", "") or ""
    cli.display_tree_show(output_dir, users, path=path, level=level,
                          limit=limit, search=search)


def _prepare_run_config(args, config_manager: ConfigManager) -> dict:
    """Load config, resolve output paths, apply --user / --dir overrides."""
    config = config_manager.get_config()
    if not config:
        print("Error: No configuration found. Run --init first.")
        sys.exit(1)

    output_file, output_prefix, output_date = _resolve_output_path(
        base=config.get("output_file", DEFAULT_REPORT_FILENAME),
        output_dir=args.output_dir,
        output_override=args.output,
        prefix=args.prefix,
        add_date=args.date,
    )
    config["output_file"] = output_file
    config["output_prefix"] = output_prefix
    config["output_date_suffix"] = output_date
    config["debug"] = getattr(args, "debug", False)

    if getattr(args, "dir", None):
        config["directory"] = args.dir
        print(f"Scan directory overridden for this run: {args.dir}")

    target_users = getattr(args, "user", None)
    if target_users:
        original_users = {u["name"]: u for u in config.get("users", [])}
        filtered_users = []
        for tu in target_users:
            if tu in original_users:
                filtered_users.append(original_users[tu])
            else:
                filtered_users.append({"name": tu, "team_id": None})
        config["users"] = filtered_users
        config["target_users_only"] = True

    return config


def _make_sync_pipeline(args, out_dir: str):
    """Build the AsyncSyncPipeline if --sync was passed, else None."""
    if not getattr(args, "sync", False):
        return None
    return AsyncSyncPipeline(
        base_dir=out_dir or ".",
        user=getattr(args, "sync_user", None),
        host=getattr(args, "sync_host", None),
        dest_dir=getattr(args, "sync_dest_dir", None),
        password=getattr(args, "sync_pass", None),
    )


def _enqueue_phase1_outputs(sync_pipeline, main_report_path: str, out_dir: str, prefix: str) -> None:
    """Enqueue main + sibling JSON/DB outputs after Phase 1 writes them."""
    if not sync_pipeline or not main_report_path:
        return
    sync_pipeline.enqueue_file(main_report_path)
    for rel_name in SIBLING_REPORT_FILENAMES:
        fname = f"{prefix}_{rel_name}" if prefix else rel_name
        full_path = os.path.join(out_dir, fname)
        if os.path.exists(full_path):
            sync_pipeline.enqueue_file(full_path)


def _enqueue_directory(sync_pipeline, out_dir: str, dirname: str) -> None:
    """Enqueue a Phase 2/3 output directory for atomic batched sync."""
    if not sync_pipeline:
        return
    dir_path = os.path.join(out_dir, dirname)
    if os.path.isdir(dir_path):
        sync_pipeline.enqueue_directory(dir_path)


def _fmt_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    s = float(n)
    i = 0
    while s >= 1024 and i < len(units) - 1:
        s /= 1024
        i += 1
    return f"{s:.1f} {units[i]}" if i > 0 else f"{int(s)} B"


def _print_run_summary(out_dir: str, main_report_path: str, run_started_at: float) -> None:
    print("\n=== SCAN COMPLETED SUCCESSFULLY ===")

    def _line(label: str, path: str) -> None:
        if path and os.path.exists(path):
            try:
                size = os.path.getsize(path)
                print(f"{label:<16}{path}  ({_fmt_size(size)})")
            except OSError:
                print(f"{label:<16}{path}")

    if main_report_path:
        _line("Summary report:", main_report_path)
    _line("Inode report:", os.path.join(out_dir, INODE_USAGE_REPORT_FILENAME))
    _line("Detail DB:", os.path.join(out_dir, DETAIL_USERS_DIRNAME, DETAIL_USERS_DB_FILENAME))
    _line("TreeMap DB:", os.path.join(out_dir, TREE_MAP_DATA_DIRNAME, TREE_MAP_DB_FILENAME))
    _line("Permission DB:", os.path.join(out_dir, PERMISSION_ISSUES_DB_FILENAME))
    total_elapsed = time.time() - run_started_at
    print(f"Total pipeline elapsed (wall-clock): {total_elapsed:.2f}s")


def _cleanup_detail_tmpdir(scan_results) -> None:
    """Best-effort removal of the per-scan Rust temp directory."""
    tmpdir = getattr(scan_results, "detail_tmpdir", "") if scan_results else ""
    if tmpdir and os.path.isdir(tmpdir):
        try:
            shutil.rmtree(tmpdir)
        except OSError:
            pass


def _cleanup_orphan_tmpdirs() -> None:
    """Remove leftover ``checkdisk_rust_*`` / ``checkdisk_sync_*`` dirs from
    previously crashed runs.

    The Rust scanner persists its temp directory and relies on Python to
    delete it; the sync pipeline also snapshots files into temp dirs before
    transfer. If Python is killed mid-scan (or a sync snapshot falls back to
    /tmp and leaks), these dirs accumulate. Sweep at the start of each
    ``--run`` so /tmp doesn't fill up over time.
    """
    base = tempfile.gettempdir()
    patterns = (
        os.path.join(base, "checkdisk_rust_*"),
        os.path.join(base, "checkdisk_sync_*"),
        os.path.join(base, ".checkdisk_sync_*"),
    )
    for pattern in patterns:
        for path in _glob.glob(pattern):
            if not os.path.isdir(path):
                continue
            try:
                shutil.rmtree(path)
            except OSError:
                # Another live scan may own this dir; skip silently.
                pass


def cmd_run(args, config_manager: ConfigManager) -> None:
    run_started_at = time.time()
    tree_map_enabled = bool(getattr(args, "tree_map", False))

    _cleanup_orphan_tmpdirs()

    config = _prepare_run_config(args, config_manager)
    main_report_path = config.get("output_file", DEFAULT_REPORT_FILENAME)
    out_dir = os.path.dirname(main_report_path) if main_report_path else "."
    if not out_dir:
        out_dir = "."

    heartbeat = None
    sync_pipeline = None
    scan_results = None
    try:
        scan_dir = config.get("directory", "")
        if not scan_dir:
            print(
                "Error: No scan directory configured. Run --init or set 'directory' "
                "in disk_checker_config.json."
            )
            return

        sync_pipeline = _make_sync_pipeline(args, out_dir)

        heartbeat = ScanStatusHeartbeat(out_dir, run_started_at, sync_pipeline)
        heartbeat.tree_map_enabled = tree_map_enabled
        heartbeat.set_phase("scan", "Scanning filesystem")
        heartbeat.start()

        print("\n=== STARTING DISK USAGE SCAN ===")
        print(f"Target directory: {scan_dir}")

        report_generator = ReportGenerator(config)
        scanner = DiskScanner(config, max_workers=args.workers, debug=args.debug)
        scan_results = scanner.scan()
        prefix = config.get("output_prefix", "")

        # Phase 1: summary reports
        heartbeat.set_phase("report", "Generating main summary reports")
        print("\n=== PHASE 1: SUMMARY REPORTS ===")
        report_generator.generate_report(scan_results)
        _enqueue_phase1_outputs(sync_pipeline, main_report_path, out_dir, prefix)

        # Phase 2: detail pipeline
        heartbeat.set_phase("detail", "Generating user detail reports")
        print("=== PHASE 2: DETAIL PIPELINE ===")
        tree_level = getattr(args, "level", 3)
        created = report_generator.generate_detail_reports_with_level(
            scan_results,
            level=tree_level,
            max_workers=scanner.max_workers,
            build_treemap=tree_map_enabled,
        )
        if created:
            _enqueue_directory(sync_pipeline, out_dir, DETAIL_USERS_DIRNAME)
        if not config.get("target_users_only", False):
            report_generator.cleanup_stale_detail_reports(created)

        # Phase 3: treemap (optional)
        if tree_map_enabled:
            heartbeat.set_phase("treemap", "Generating interactive tree map")
            tree_map_path = report_generator.generate_tree_map(
                scan_results,
                level=getattr(args, "level", 3),
                max_workers=scanner.max_workers,
            )
            if tree_map_path and os.path.isfile(tree_map_path):
                _enqueue_directory(sync_pipeline, out_dir, TREE_MAP_DATA_DIRNAME)

        # Phase 4: drain sync pipeline
        if sync_pipeline:
            heartbeat.set_phase("sync", "Syncing reports to remote server")
            print("[SYNC] Waiting for async pipeline to complete...")
            sync_pipeline.wait()
            print("[SYNC] Async pipeline done")

        heartbeat.stop()
        update_status(
            sync_pipeline,
            out_dir,
            "done",
            False,
            "Scan completed successfully",
            started_at=run_started_at,
            phase_started_at=run_started_at,
            tree_map_enabled=tree_map_enabled,
            sync_enabled=bool(sync_pipeline),
        )
        if sync_pipeline:
            sync_pipeline.wait()
            sync_pipeline.close()

        _print_run_summary(out_dir, main_report_path, run_started_at)

        if getattr(args, "webhook_url", None):
            from src.msteams_notifier import send_msteams_notification
            send_msteams_notification(args.webhook_url, scan_results, config)

    except KeyboardInterrupt:
        if heartbeat:
            heartbeat.stop()
        # Order matters on the abort path:
        #   1. Push the final "error" status into the live pipeline FIRST,
        #      while it can still enqueue + flush to the remote. After
        #      close() the executor is shut down and update_status() falls
        #      back to local-only.
        #   2. Then drain pipeline (cancel pending, kill running tar/ssh
        #      via _drain_tar_proc).
        #   3. Then sweep remote orphan staging artifacts so the remote
        #      directory is consistent with what the dashboard sees.
        update_status(
            sync_pipeline,
            out_dir,
            "error",
            False,
            "Scan interrupted by user",
            started_at=run_started_at,
            phase_started_at=run_started_at,
            tree_map_enabled=tree_map_enabled,
            sync_enabled=bool(sync_pipeline),
        )
        if sync_pipeline:
            try:
                sync_pipeline.close()
            except Exception:
                pass
            try:
                sync_pipeline.cleanup_remote_staging()
            except Exception:
                pass
        print("\nScan interrupted by user.")
        sys.exit(1)
    except Exception as e:
        if heartbeat:
            heartbeat.stop()
        update_status(
            sync_pipeline,
            out_dir,
            "error",
            False,
            f"Scan failed: {e}",
            str(e),
            started_at=run_started_at,
            phase_started_at=run_started_at,
            tree_map_enabled=tree_map_enabled,
            sync_enabled=bool(sync_pipeline),
        )
        if sync_pipeline:
            try:
                sync_pipeline.close()
            except Exception:
                pass
            try:
                sync_pipeline.cleanup_remote_staging()
            except Exception:
                pass
        err_text = str(e)
        if "No space left on device" in err_text or "ENOSPC" in err_text:
            print(f"\nTemporary storage is full: {e}")
            sys.exit(2)
        print(f"\nUnexpected error: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        _cleanup_detail_tmpdir(scan_results)


def main():
    """Main entry point for the disk usage checker application."""
    signal.signal(signal.SIGINT, signal_handler)

    cli = CLIInterface()
    args = cli.parse_arguments()
    config_manager = ConfigManager()

    if args.init:
        cmd_init(args, config_manager)
    elif args.dir and not args.run:
        cmd_update_directory(args, config_manager)

    if args.add_team:
        cmd_add_team(args, config_manager)
    elif args.add_user:
        cmd_add_user(args, config_manager)
    elif args.remove_user:
        cmd_remove_user(args, config_manager)
    elif args.list:
        cmd_list(args, cli, config_manager)
    elif args.show_report:
        cmd_show_report(args, cli)
    elif getattr(args, "detail", False):
        cmd_detail(args, cli)
    elif getattr(args, "tree_show", False):
        cmd_tree_show(args, cli)
    elif args.run:
        cmd_run(args, config_manager)
    else:
        cli.print_help()


if __name__ == "__main__":
    main()
