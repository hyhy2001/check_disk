"""
Report Generator Module

Handles generating and saving disk usage reports.
"""

import os
import shutil
import threading
import time
from typing import Any, Dict, List, Optional

from .disk_scanner import ScanResult
from .utils import save_json_report

try:
    from src import fast_scanner as _fast_scanner
    HAS_RUST_PIPELINE = hasattr(_fast_scanner, 'build_pipeline')
except ImportError:
    _fast_scanner = None  # type: ignore
    HAS_RUST_PIPELINE = False


class ReportGenerator:
    """Generates and saves disk usage reports."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the report generator.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.output_file = config.get("output_file", "disk_usage_report.json")
        self.debug = bool(config.get("debug", False))

    # ------------------------------------------------------------------ #
    # Path helpers                                                         #
    # ------------------------------------------------------------------ #

    def _get_output_filename(self, base_filename: str) -> str:
        """
        Generate an output filename using the same prefix as the main output file.
        Sibling reports (permission_issues, check_user) never include a date suffix.

        Args:
            base_filename: Base name without extension (e.g. 'permission_issues')

        Returns:
            Full output path (e.g. '/reports/sda1_permission_issues.json')
        """
        dir_part = os.path.dirname(self.output_file)
        prefix = self.config.get('output_prefix', '')

        parts = [p for p in [prefix, base_filename] if p]
        new_filename = '_'.join(parts) + '.json'

        return os.path.join(dir_part, new_filename) if dir_part else new_filename

    def cleanup_stale_detail_reports(self, keep_paths: List[str]) -> None:
        """
        Remove stale files in detail_users/ that were not regenerated in this run.

        This keeps sync incremental-friendly (we don't wipe all files up front),
        while still preventing orphan reports from users that no longer exist.
        """
        dir_part = os.path.dirname(self.output_file)
        detail_dir = os.path.join(dir_part, "detail_users") if dir_part else "detail_users"
        if not os.path.isdir(detail_dir):
            return

        keep_abs = {os.path.abspath(p) for p in keep_paths if p}

        removed = 0
        for name in os.listdir(detail_dir):
            if not (
                name.endswith(".json")
                or name.endswith(".ndjson")
            ):
                continue
            fp = os.path.abspath(os.path.join(detail_dir, name))
            if fp in keep_abs:
                continue
            try:
                os.remove(fp)
                removed += 1
            except OSError:
                pass

        if removed > 0:
            print(f"Cleaned up {removed} stale detail file(s) in {detail_dir}.")

    # ------------------------------------------------------------------ #
    # Legacy helpers                                                       #
    # ------------------------------------------------------------------ #

    def _build_team_id_maps(self):
        """Build team_name -> team_id and username -> team_id lookup dicts from config."""
        team_id_map = {t["name"]: t["team_id"] for t in self.config.get("teams", [])}
        user_team_id_map = {}
        for user in self.config.get("users", []):
            user_team_id_map[user["name"]] = user["team_id"]
        return team_id_map, user_team_id_map

    # ------------------------------------------------------------------ #
    # Main summary report                                                  #
    # ------------------------------------------------------------------ #

    def generate_report(self, scan_result: Optional[ScanResult] = None) -> Dict[str, Any]:
        """
        Generate a report from scan results.

        Args:
            scan_result: ScanResult object with disk usage data, or None

        Returns:
            Dictionary containing the report data
        """
        if scan_result is None:
            print("Warning: No scan results provided. Generating empty report.")
            report = {
                "date": int(time.time()),
                "directory": self.config.get("directory", ""),
                "general_system": {"total": 0, "used": 0, "available": 0},
                "team_usage": [],
                "user_usage": [],
                "other_usage": []
            }
        else:
            team_id_map, user_team_id_map = self._build_team_id_maps()

            # Inject team_id into each team entry
            team_usage = []
            for t in scan_result.team_usage:
                entry = dict(t)
                tid = team_id_map.get(t["name"])
                if tid is not None:
                    entry["team_id"] = tid
                team_usage.append(entry)

            # Inject team_id into each user entry
            user_usage = []
            for u in scan_result.user_usage:
                entry = dict(u)
                tid = user_team_id_map.get(u["name"])
                if tid is not None:
                    entry["team_id"] = tid
                user_usage.append(entry)

            filtered_general_system = {
                k: v for k, v in scan_result.general_system.items()
                if not k.startswith("inodes_")
            }

            report = {
                "date": scan_result.timestamp,
                "directory": self.config.get("directory", ""),
                "general_system": filtered_general_system,
                "team_usage": team_usage,
                "user_usage": user_usage,
                "other_usage": scan_result.other_usage
            }

            # Permission issues JSON is generated natively by Rust Phase 2 via TSV stream.
            # Fall back to Python generation only if the file is absent.
            perm_path = self._get_output_filename("permission_issues")
            if os.path.exists(perm_path):
                print(f"Permission issues report saved to: {perm_path}")
            elif hasattr(scan_result, 'permission_issues'):
                self.generate_permission_issues_report(scan_result)

            if hasattr(scan_result, 'user_inodes'):
                self.generate_inode_report(scan_result)

        save_json_report(report, self.output_file)
        return report

    # ------------------------------------------------------------------ #
    # Permission issues report                                             #
    # ------------------------------------------------------------------ #

    def generate_permission_issues_report(self, scan_result: ScanResult) -> Dict[str, Any]:
        """
        Generate a report for permission issues.

        Args:
            scan_result: ScanResult object with disk usage data

        Returns:
            Dictionary containing the report data
        """
        report = {
            "date": scan_result.timestamp,
            "directory": self.config.get("directory", ""),
            "general_system": scan_result.general_system,
            "permission_issues": scan_result.permission_issues
        }

        output_path = self._get_output_filename("permission_issues")
        save_json_report(report, output_path)
        print(f"Permission issues report saved to: {output_path}")

        return report

    # ------------------------------------------------------------------ #
    # Inode usage report                                                   #
    # ------------------------------------------------------------------ #

    def generate_inode_report(self, scan_result: ScanResult) -> Dict[str, Any]:
        """
        Generate a report for inode usage (files count).

        Args:
            scan_result: ScanResult object with disk usage data

        Returns:
            Dictionary containing the report data
        """
        report = {
            "date": scan_result.timestamp,
            "directory": self.config.get("directory", ""),
            "inodes_total": scan_result.general_system.get("inodes_total", 0),
            "inodes_used": scan_result.general_system.get("inodes_used", 0),
            "inodes_free": scan_result.general_system.get("inodes_free", 0),
            "inodes_scanned": scan_result.general_system.get("inodes_scanned", 0),
            "users": scan_result.user_inodes
        }

        output_path = self._get_output_filename("inode_usage_report")
        save_json_report(report, output_path)
        print(f"Inode usage report saved to: {output_path}")

        return report

    # ------------------------------------------------------------------ #
    # TreeMap report                                                       #
    # ------------------------------------------------------------------ #

    def generate_tree_map(
        self,
        scan_result: ScanResult,
        level: int = 3,
        max_workers: Optional[int] = None,
    ) -> str:
        """Return the TreeMap report path built by the Rust pipeline."""
        self.config["tree_map_level"] = int(level)
        if self.debug and max_workers is not None and scan_result.detail_tmpdir:
            print(f"[Phase 3] TreeMap already built by Rust pipeline ({max_workers}w)")
        output_path = self._get_output_filename("tree_map_report")
        if os.path.exists(output_path):
            return output_path
        raise RuntimeError(
            "TreeMap generation is now part of Phase 2 JSON/NDJSON output build. "
            "Call generate_detail_reports() before generate_tree_map()."
        )

    @staticmethod
    def _get_rss_mb() -> float:
        """Return current process RSS in MB (Linux)."""
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            return int(parts[1]) / 1024.0
        except OSError:
            pass
        return 0.0

    def generate_detail_reports(
        self,
        scan_result: ScanResult,
        max_workers: int = 1,
    ) -> List[str]:
        """Generate JSON/NDJSON detail data and TreeMap outputs via Rust."""
        if not scan_result.detail_tmpdir:
            raise RuntimeError(
                "Phase 2 requires Rust streaming outputs (detail_tmpdir). "
                "Python in-memory fallback has been removed."
            )
        if not HAS_RUST_PIPELINE:
            raise RuntimeError(
                "Rust pipeline core is required. "
                "Please build/install fast_scanner with build_pipeline."
            )

        output_dir = os.path.dirname(self.output_file) or "."
        detail_dir = os.path.join(output_dir, "detail_users")
        os.makedirs(detail_dir, exist_ok=True)

        unified_path = os.path.join(detail_dir, "data_detail.json")
        tree_json_path = self._get_output_filename("tree_map_report")
        tree_data_path = os.path.join(output_dir, "tree_map_data")
        stale_paths = (
            unified_path,
            tree_json_path,
            tree_data_path,
        )
        for stale_path in stale_paths:
            if os.path.isdir(stale_path):
                shutil.rmtree(stale_path)
            elif os.path.exists(stale_path):
                os.remove(stale_path)
        phase2_start = time.time()
        phase2_mem_start = self._get_rss_mb() if self.debug else 0.0
        if self.debug:
            print(f"[Phase 2] RAM at start: {phase2_mem_start:.1f} MB")
            print(f"Phase 2: Building JSON/NDJSON detail outputs via Rust [streaming, {max(1, int(max_workers))}w]...")

        team_map = {
            str(user.get("name", "")): str(user.get("team_id", ""))
            for user in self.config.get("users", [])
            if user.get("name")
        }

        tree_map_level = int(self.config.get("tree_map_level", 3) or 3)

        build_args = (
            scan_result.detail_tmpdir,
            scan_result.detail_uid_username,
            team_map,
            unified_path,
            tree_json_path,
            tree_data_path,
            self.config.get("directory", "/"),
            int(max(1, tree_map_level)),
            0,
            int(scan_result.timestamp),
            int(max(1, int(max_workers))),
        )
        if not hasattr(_fast_scanner, "build_pipeline"):
            raise RuntimeError("fast_scanner.build_pipeline is required")
        print("[Phase 2] Rust pipeline started (large datasets may take a while)...")
        build_started = time.time()
        total_files_holder: Dict[str, int] = {"value": 0}
        build_error_holder: Dict[str, Optional[Exception]] = {"error": None}

        def _run_build_pipeline() -> None:
            try:
                try:
                    total_files_holder["value"] = int(
                        _fast_scanner.build_pipeline(*build_args, bool(self.debug))
                    )
                except TypeError as exc:
                    if "positional arguments" not in str(exc):
                        raise
                    total_files_holder["value"] = int(_fast_scanner.build_pipeline(*build_args))
            except Exception as exc:  # pragma: no cover - exception relay path
                build_error_holder["error"] = exc

        build_thread = threading.Thread(target=_run_build_pipeline, daemon=True)
        build_thread.start()
        while build_thread.is_alive():
            build_thread.join(timeout=10.0)
            if build_thread.is_alive():
                elapsed = int(time.time() - build_started)
                print(f"[Phase 2] Still processing... elapsed {elapsed}s")

        if build_error_holder["error"] is not None:
            raise build_error_holder["error"]
        total_files = total_files_holder["value"]

        root_manifest_path = unified_path

        created = [root_manifest_path, tree_json_path, os.path.join(tree_data_path, "manifest.json")]
        self.cleanup_stale_detail_reports(created)

        detail_users_count = 0
        root_manifest_data = None
        try:
            import json
            with open(root_manifest_path, "r", encoding="utf-8") as fh:
                root_manifest_data = json.load(fh)
            detail_users_count = len(root_manifest_data.get("users", []))
        except Exception:
            detail_users_count = 0

        # Cleanup temporary Rust scan segments after Phase 2 completes.
        try:
            if scan_result.detail_tmpdir and os.path.isdir(scan_result.detail_tmpdir):
                shutil.rmtree(scan_result.detail_tmpdir)
                if self.debug:
                    print(f"  [Phase 2] Cleaned temp scan segments: {scan_result.detail_tmpdir}")
        except OSError as exc:
            if self.debug:
                print(f"  [Phase 2] Warning: failed to remove temp scan segments {scan_result.detail_tmpdir}: {exc}")

        phase2_elapsed = time.time() - phase2_start
        if self.debug:
            phase2_mem_end = self._get_rss_mb()
            print(f"  [Phase 2] Detail manifest ready: {root_manifest_path}")
            print(f"  [Phase 2] Users processed: {detail_users_count:,}")
            print(f"  [Phase 2] User details ready: {os.path.join(detail_dir, 'users')}")
            print(f"  [Phase 3] TreeMap outputs ready: {tree_json_path}, {tree_data_path}")
            print(
                f"[Phase 2] RAM end: {phase2_mem_end:.1f} MB "
                f"(delta: {phase2_mem_end - phase2_mem_start:+.1f} MB, elapsed: {phase2_elapsed:.2f}s, "
                f"files: {int(total_files):,}, users: {detail_users_count:,})"
            )
        else:
            print(f"Reports generated in {phase2_elapsed:.2f}s ({int(total_files):,} files, {detail_users_count:,} users):")
            print(f"  Detail manifest: {root_manifest_path}")
            print(f"  Users processed: {detail_users_count:,}")
            print(f"  User details: {os.path.join(detail_dir, 'users')}")
            print(f"  TreeMap JSON: {tree_json_path}")
            print(f"  TreeMap data: {tree_data_path}")
        return sorted(created)

    def generate_detail_reports_with_level(
        self,
        scan_result: ScanResult,
        level: int,
        max_workers: int = 1,
    ) -> List[str]:
        self.config["tree_map_level"] = int(level)
        return self.generate_detail_reports(scan_result, max_workers=max_workers)
