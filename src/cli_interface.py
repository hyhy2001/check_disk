"""
CLI Interface Module

Handles command-line interface for the disk usage checker.
"""

import argparse
import os
import sys
import glob
from typing import Dict, Any, List

from src.utils import parse_size, load_json_report
from src.formatters.report_formatter import ReportFormatter

class CLIInterface:
    """Command-line interface for the disk usage checker."""
    
    def __init__(self):
        """Initialize the CLI interface."""
        self.parser = self._create_parser()
        self.report_formatter = ReportFormatter()
    
    def _create_parser(self) -> argparse.ArgumentParser:
        """
        Create the argument parser.
        
        Returns:
            Configured ArgumentParser instance
        """
        parser = argparse.ArgumentParser(
            description="Disk Usage Checker - Monitor disk usage by team and user"
        )
        
        # Create command groups for better organization
        config_group = parser.add_argument_group('Configuration Commands')
        scan_group = parser.add_argument_group('Scanning Commands')
        report_group = parser.add_argument_group('Report Commands')
        
        # Configuration commands
        config_group.add_argument("--init", action="store_true", help="Initialize configuration")
        config_group.add_argument("--dir", help="Path to directory for scanning (changes directory in config)")
        config_group.add_argument("--add-team", metavar="TEAM", help="Add a new team")
        config_group.add_argument("--add-user", metavar="USER", nargs="+", help="Add one or more users to a team")
        config_group.add_argument("--team", metavar="TEAM", help="Team name")
        config_group.add_argument("--remove-user", metavar="USER", nargs="+", help="Remove one or more users")
        config_group.add_argument("--list", action="store_true", help="List current configuration (grouped by team by default)")

        # Scanning commands
        scan_group.add_argument("--run", action="store_true", help="Run disk usage check")
        scan_group.add_argument("--workers", type=int, help="Number of worker threads to use for scanning (default: auto)")
        scan_group.add_argument("--debug", action="store_true", help="Enable debug output")
        scan_group.add_argument("--prefix", metavar="PREFIX", help="Add prefix to output filenames")
        scan_group.add_argument("--date", action="store_true", help="Add current date (YYYYMMDD) to output filenames")
        scan_group.add_argument("--output", metavar="FILE", help="Output file for report (full path including filename)")
        scan_group.add_argument("--output-dir", metavar="DIR", help="Directory to store output reports (will use default filename)")
        
        # Report commands
        report_group.add_argument("--show-report", action="store_true", help="Show disk usage report(s)")
        report_group.add_argument("--files", metavar="FILE", nargs="+", help="Report file(s) to display or compare (required with --show-report). Supports wildcards like *.json")
        report_group.add_argument("--check-users", metavar="USER", nargs="+",
                                help="Display detail reports for specific user(s). Reads detail_report_dir and detail_report_file JSON files.")

        # Report filtering options
        filter_group = parser.add_argument_group('Report Filtering Options')
        filter_group.add_argument("--user", metavar="USER", nargs="+", help="Filter report by specific user(s)")
        filter_group.add_argument("--compare-by", choices=["usage", "growth"], default="growth",
                                help="Method for selecting top users when comparing multiple reports: 'usage' (total size) or 'growth' (growth rate) (default: growth)")

        return parser
    
    def parse_arguments(self) -> argparse.Namespace:
        """
        Parse command-line arguments.
        
        Returns:
            Parsed arguments namespace
        """
        args = self.parser.parse_args()
        
        # Process arguments that use nargs="+" to handle both quoted and unquoted inputs
        self._process_nargs_arguments(args)
        
        return args
    
    def _process_nargs_arguments(self, args) -> None:
        """
        Process arguments that use nargs="+".
        This handles both quoted strings with spaces and multiple arguments.
        
        Args:
            args: Parsed arguments namespace
        """
        # Process --add-user argument
        if hasattr(args, 'add_user') and args.add_user:
            # If we have a single element that contains spaces, split it
            if len(args.add_user) == 1 and ' ' in args.add_user[0]:
                args.add_user = args.add_user[0].split()
        
        # Process --remove-user argument
        if hasattr(args, 'remove_user') and args.remove_user:
            # If we have a single element that contains spaces, split it
            if len(args.remove_user) == 1 and ' ' in args.remove_user[0]:
                args.remove_user = args.remove_user[0].split()
        
        # Process --user argument
        if hasattr(args, 'user') and args.user:
            # If we have a single element that contains spaces, split it
            if len(args.user) == 1 and ' ' in args.user[0]:
                args.user = args.user[0].split()
        
        # Process --files argument with wildcard support
        if hasattr(args, 'files') and args.files:
            expanded_files = []
            for file_pattern in args.files:
                # Check if the pattern contains wildcards
                if '*' in file_pattern or '?' in file_pattern:
                    # Expand wildcards
                    matching_files = glob.glob(file_pattern)
                    if matching_files:
                        expanded_files.extend(matching_files)
                    else:
                        # Keep the original pattern if no matches found
                        expanded_files.append(file_pattern)
                else:
                    # No wildcards, keep as is
                    expanded_files.append(file_pattern)
            
            # Update the files list with expanded files
            args.files = expanded_files
    
    def print_help(self) -> None:
        """Print the help message."""
        self.parser.print_help()
        
        # Print additional usage examples
        print("\nUsage Examples:")
        print("  # Initialize configuration")
        print("  disk_checker.py --init --dir /path/to/scan")
        print("\n  # Change directory in existing configuration")
        print("  disk_checker.py --dir /new/path/to/scan")
        print("\n  # Add a team and users")
        print("  disk_checker.py --add-team TeamName")
        print("  disk_checker.py --add-user user1 user2 --team TeamName")
        print("  disk_checker.py --add-user \"user1 user2 user3\" --team TeamName")
        print("\n  # List configuration")
        print("  disk_checker.py --list")
        print("  disk_checker.py --list --team")
        print("\n  # Run a scan")
        print("  disk_checker.py --run")
        print("  disk_checker.py --run --prefix myproject --date")
        print("  disk_checker.py --run --output-dir /path/to/reports --prefix DE --date")
        print("\n  # View a report")
        print("  disk_checker.py --show-report --files report.json")
        print("\n  # View multiple reports using wildcards")
        print("  disk_checker.py --show-report --files \"disk_usage_*.json\"")
        print("  disk_checker.py --show-report --files disk_usage_*.json")
        print("\n  # Compare multiple reports")
        print("  disk_checker.py --show-report --files report1.json report2.json")
        print("  disk_checker.py --show-report --files report1.json report2.json --compare-by usage")
        print("  disk_checker.py --show-report --files report1.json report2.json --compare-by growth")
        print("\n  # Filter reports by user")
        print("  disk_checker.py --show-report --files report1.json report2.json --user user1 user2")
        print("  disk_checker.py --show-report --files report1.json report2.json --user \"user1 user2\"")
    
    def display_config(self, config: Dict[str, Any], team_filter: str = None) -> None:
        """
        Display the current configuration.
        
        Args:
            config: Configuration dictionary
        team_filter: Optional team name to filter by
        """
        # Display configuration summary
        self.report_formatter.display_config(config, team_filter)
    
    def display_report(self, report_files: List[str], filter_users: List[str] = None, compare_by: str = "growth") -> None:
        """
        Display a summary of the generated report(s).
        
        Args:
            report_files: List of paths to the report files
            filter_users: Optional list of usernames to filter by
            compare_by: Method for selecting top users when comparing reports ('usage' or 'growth')
        """
        if not report_files:
            print("Error: No report files specified")
            return
            
        # Check if all report files exist
        missing_files = [f for f in report_files if not os.path.exists(f)]
        if missing_files:
            print(f"Error: The following report files were not found: {', '.join(missing_files)}")
            return
            
        # If only one report file, display it normally
        if len(report_files) == 1:
            report = load_json_report(report_files[0])
            if report:
                self.report_formatter.display_report_summary(report, report_files[0], filter_users)
            else:
                print(f"Error: Could not load report from {report_files[0]}")
        else:
            # If multiple report files, compare them
            reports = []
            for file_path in report_files:
                report = load_json_report(file_path)
                if report:
                    reports.append((file_path, report))
                else:
                    print(f"Warning: Could not load report from {file_path}")
            
            if reports:
                self.report_formatter.compare_reports(reports, filter_users, compare_by)
            else:
                print("Error: No valid reports found for comparison")

    def display_check_users(
        self,
        users: List[str],
        prefix: str = "",
        output_dir: str = ".",
        top_files: int = 30,
    ) -> None:
        """Display per-user detail reports (dir + file breakdown).

        Locates JSON files inside the detail_users/ subdirectory:
            {output_dir}/detail_users/{prefix}_detail_report_dir_{user}.json
            {output_dir}/detail_users/{prefix}_detail_report_file_{user}.json
        """
        detail_dir = os.path.join(output_dir, "detail_users")

        def _build_path(base: str, user: str) -> str:
            parts = [p for p in [prefix, base, user] if p]
            fname = "_".join(parts) + ".json"
            return os.path.join(detail_dir, fname)

        dir_files: Dict[str, str] = {}
        file_files: Dict[str, str] = {}

        for user in users:
            dp = _build_path("detail_report_dir",  user)
            fp = _build_path("detail_report_file", user)

            dir_files[user]  = dp if os.path.exists(dp) else None
            file_files[user] = fp if os.path.exists(fp) else None

            if dir_files[user] is None:
                print(f"Warning: dir detail not found for '{user}': {dp}")
            if file_files[user] is None:
                print(f"Warning: file detail not found for '{user}': {fp}")

        self.report_formatter.display_user_detail_reports(
            users, dir_files, file_files, top_files
        )