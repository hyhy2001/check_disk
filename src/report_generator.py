"""
Report Generator Module

Handles generating and saving disk usage reports.
"""

import os
import time
import datetime
from typing import Dict, Any, Optional, List, Set
from src.disk_scanner import ScanResult
from src.utils import format_size, save_json_report, ScanHelper

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

    def _get_user_detail_filename(self, base: str, user: str) -> str:
        """
        Build path for a per-user detail report. Never includes a date.

        Args:
            base: Middle segment, e.g. 'detail_report_dir' or 'detail_report_file'
            user: Username

        Returns:
            Full output path (e.g. 'sda1_detail_report_dir_Binh.json')
        """
        dir_part = os.path.dirname(self.output_file)
        prefix = self.config.get('output_prefix', '')
        parts = [p for p in [prefix, base, user] if p]
        fname = '_'.join(parts) + '.json'
        return os.path.join(dir_part, fname) if dir_part else fname

    # ------------------------------------------------------------------ #
    # Legacy helpers kept for backward compatibility                       #
    # ------------------------------------------------------------------ #

    def _is_valid_date(self, date_str: str) -> bool:
        """Check if a string is a valid date in YYYYMMDD format."""
        if len(date_str) != 8:
            return False
        try:
            year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
            return (1900 <= year <= 2100) and (1 <= month <= 12) and (1 <= day <= 31)
        except ValueError:
            return False
    
    def generate_report(self, scan_result: Optional[ScanResult] = None) -> Dict[str, Any]:
        """
        Generate a report from scan results.
        
        Args:
            scan_result: ScanResult object with disk usage data, or None
            
        Returns:
            Dictionary containing the report data
        """
        # Handle case where scan_result is None
        if scan_result is None:
            print("Warning: No scan results provided. Generating empty report.")
            # Create an empty report with current timestamp
            report = {
                "date": int(time.time()),
                "directory": self.config.get("directory", ""),
                "general_system": {"total": 0, "used": 0, "available": 0},
                "team_usage": [],
                "user_usage": [],
                "other_usage": []
            }
        else:
            # Create report structure from scan results
            report = {
                "date": scan_result.timestamp,
                "directory": self.config.get("directory", ""),
                "general_system": scan_result.general_system,
                "team_usage": scan_result.team_usage,
                "user_usage": scan_result.user_usage,
                "other_usage": scan_result.other_usage
            }
            
            # Generate permission issues report if there are any issues
            if hasattr(scan_result, 'permission_issues') and scan_result.permission_issues:
                self.generate_permission_issues_report(scan_result)
        
        # Save report to file
        save_json_report(report, self.output_file)
        
        return report
    
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
        
        # Generate output filename with the same pattern as the main output
        output_path = self._get_output_filename("permission_issues")
        
        # Save to permission_issues.json
        save_json_report(report, output_path)
        print(f"Permission issues report saved to: {output_path}")
        
        return report
    
    def generate_check_user_report(self, scan_result: ScanResult, check_users: List[str]) -> Dict[str, Any]:
        """
        Generate a report for specific users.
        
        Args:
            scan_result: ScanResult object with disk usage data
            check_users: List of usernames to include in the report
            
        Returns:
            Dictionary containing the report data
        """
        report = {
            "date": scan_result.timestamp,
            "directory": self.config.get("directory", ""),
            "check_users": check_users,
            "user_usage": [],
            "detail_dir": []  # Changed from top_dir to detail_dir
        }
        
        # Filter user usage data
        user_set = set(check_users)
        
        # Filter users from user_usage and other_usage
        user_data = ScanHelper.filter_users_by_names(scan_result.user_usage, user_set)
        other_users = ScanHelper.filter_users_by_names(scan_result.other_usage, user_set)
        
        # Combine the filtered users
        report["user_usage"] = user_data + other_users
        
        # If no users were found in the scan results, add them with zero usage
        found_users = {user["name"] for user in report["user_usage"]}
        for username in user_set:
            if username not in found_users:
                report["user_usage"].append({"name": username, "used": 0})
        
        # Include all directories for specified users, sorted by usage
        if hasattr(scan_result, 'top_dir') and scan_result.top_dir:
            # Filter directories by specified users
            filtered_dirs = [
                dir_entry for dir_entry in scan_result.top_dir
                if dir_entry["user"] in user_set
            ]
            
            # Sort by usage (highest to lowest)
            filtered_dirs.sort(key=lambda x: x["user_usage"], reverse=True)
            report["detail_dir"] = filtered_dirs  # Include all entries without limit
        
        # Include permission issues for these users if available
        if hasattr(scan_result, 'permission_issues') and scan_result.permission_issues:
            user_permission_issues = {
                "users": [
                    user for user in scan_result.permission_issues.get("users", [])
                    if user["name"] in user_set
                ]
            }
            if user_permission_issues["users"]:
                report["permission_issues"] = user_permission_issues
        
        # Generate output filename with the same pattern as the main output
        output_path = self._get_output_filename("check_user")
        save_json_report(report, output_path)
        return report

    def generate_detail_reports(self, scan_result: ScanResult) -> List[str]:
        """
        Generate per-user directory and file detail reports.

        Args:
            scan_result: ScanResult object with top_dir and detail_files data

        Returns:
            List of created file paths
        """
        # Collect all users that have directory data
        users = sorted({entry['user'] for entry in scan_result.top_dir})
        created: List[str] = []

        for user in users:
            # --- Directory detail report ---
            dirs = [
                e for e in scan_result.top_dir if e['user'] == user
            ]
            total_dir_used = sum(d['user_usage'] for d in dirs)
            dir_data = {
                'date': scan_result.timestamp,
                'directory': self.config.get('directory', ''),
                'user': user,
                'total_used': total_dir_used,
                'dirs': [
                    {'path': d['dir'], 'used': d['user_usage']}
                    for d in sorted(dirs, key=lambda x: x['user_usage'], reverse=True)
                ],
            }
            dir_path = self._get_user_detail_filename('detail_report_dir', user)
            save_json_report(dir_data, dir_path)
            created.append(dir_path)

            # --- File detail report ---
            user_files = scan_result.detail_files.get(user, [])
            file_data = {
                'date': scan_result.timestamp,
                'user': user,
                'total_files': len(user_files),
                'total_used': sum(s for _, s in user_files),
                'files': [{'path': p, 'size': s} for p, s in user_files],
            }
            file_path = self._get_user_detail_filename('detail_report_file', user)
            save_json_report(file_data, file_path)
            created.append(file_path)

        output_dir = os.path.dirname(created[0]) if created else '.'
        print(f"Generated {len(users)} user detail report(s) -> {output_dir}")
        return created
    
    def get_report_path(self) -> str:
        """
        Get the path to the generated report.
        
        Returns:
            Path to the report file
        """
        return self.output_file