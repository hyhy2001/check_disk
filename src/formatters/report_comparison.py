"""
Report Comparison Module

Contains functionality for comparing multiple reports.
"""

from typing import Dict, Any, List, Optional, Tuple
from src.utils import format_size, format_timestamp
from src.formatters.base_formatter import BaseFormatter
from src.formatters.table_formatter import TableFormatter

class ReportComparison(BaseFormatter):
    """Helper class for comparing multiple reports."""
    
    def __init__(self):
        """Initialize the report comparison helper."""
        super().__init__()
        self.table_formatter = TableFormatter()
    
    def compare_reports(self, reports: List[Tuple[str, Dict[str, Any]]], filter_users: List[str] = None, compare_by: str = "growth") -> None:
        """Compare multiple reports and display a comparison table."""
        if not reports:
            print("No reports to compare")
            return
            
        print("\n" + "=" * 60)
        
        # Determine comparison mode based on number of reports and compare_by parameter
        if len(reports) >= 2 and compare_by == "growth":
            print("DISK USAGE REPORT COMPARISON (TOP 10 BY GROWTH RATE)")
            print("=" * 60)
            print("Showing users with highest growth rate between reports")
        else:
            print("DISK USAGE REPORT COMPARISON (TOP 10 BY USAGE)")
            print("=" * 60)
            if compare_by == "usage" or len(reports) < 2:
                print("Showing users with highest total disk usage")
        
        # Extract dates and create a mapping of dates to report indices
        dates = []
        date_to_index = {}
        for i, (file_path, report) in enumerate(reports):
            timestamp = report.get('date', 0)
            date_str = format_timestamp(timestamp).split()[0]  # Get just the date part
            dates.append(date_str)
            date_to_index[date_str] = i
        
        # Sort dates chronologically
        dates.sort()
        
        # Get top users across all reports
        all_users = self._get_top_users_across_reports(reports, filter_users, compare_by)
        user_list = sorted(all_users)
        
        # Always use transposed view (dates as rows, users as columns)
        self._display_transposed_comparison_table(reports, dates, date_to_index, user_list)
        
        # Display report file paths
        print("\nReport files:")
        for i, (file_path, _) in enumerate(reports):
            print(f"  {i+1}. {file_path}")
    
    def _display_transposed_comparison_table(self, reports: List[Tuple[str, Dict[str, Any]]], 
                                           dates: List[str], date_to_index: Dict[str, int], 
                                           user_list: List[str]) -> None:
        """Display a transposed comparison table with dates as rows and users as columns."""
        # Create headers with users
        headers = ["Date"]
        headers.extend(user_list)
        
        rows = []
        
        # For each date, create a row with usage for each user
        for date in dates:
            row = [date]
            report_index = date_to_index[date]
            _, report = reports[report_index]
            
            # Add usage for each user
            for user in user_list:
                usage = self._find_user_usage_in_report(user, report)
                row.append(format_size(usage))
            
            rows.append(row)
        
        # Add growth rows if we have at least 2 reports
        if len(dates) >= 2:
            # Add absolute growth row
            abs_growth_row = ["Abs Growth"]
            
            # Add percentage growth row
            pct_growth_row = ["% Growth"]
            
            # Add trend row
            trend_row = ["Trend"]
            
            # Calculate trend-based growth for each user
            for user in user_list:
                # Get all usage values for this user across all reports
                user_usages = []
                for date in dates:
                    report_index = date_to_index[date]
                    _, report = reports[report_index]
                    usage = self._find_user_usage_in_report(user, report)
                    user_usages.append(usage)
                
                # Calculate growth metrics using all data points
                abs_growth, pct_growth, trend_indicator = self._calculate_comprehensive_growth(user_usages)
                
                # Format and add to growth rows
                abs_growth_row.append(format_size(abs_growth))
                
                if pct_growth is not None:
                    pct_growth_row.append(f"{pct_growth:.1f}%")
                else:
                    pct_growth_row.append("N/A")
                
                trend_row.append(trend_indicator)
            
            # Add growth rows to the table
            rows.append(abs_growth_row)
            rows.append(pct_growth_row)
            rows.append(trend_row)
        
        # Display the transposed table
        table = self.table_formatter.format_table(headers, rows, title="Usage Comparison")
        print("\n" + table)
    
    def _calculate_comprehensive_growth(self, usage_values: List[int]) -> Tuple[int, Optional[float], str]:
        """
        Calculate growth metrics using all data points.
        
        Args:
            usage_values: List of usage values in chronological order
            
        Returns:
            Tuple of (absolute_growth, percentage_growth, trend_indicator)
        """
        if not usage_values or len(usage_values) < 2:
            return 0, None, "-"
        
        # Calculate absolute growth (difference between last and first valid values)
        first_valid = usage_values[0]
        last_valid = usage_values[-1]
        
        # If first value is zero, find first non-zero value
        if first_valid == 0:
            for v in usage_values:
                if v > 0:
                    first_valid = v
                    break
        
        abs_growth = last_valid - first_valid
        
        # Calculate percentage growth
        if first_valid > 0:
            pct_growth = (abs_growth / first_valid) * 100
        else:
            # If all starting values are zero, we can't calculate percentage
            pct_growth = None
        
        # Calculate trend using all data points
        trend_indicator = self._calculate_trend_indicator(usage_values)
            
        return abs_growth, pct_growth, trend_indicator
    
    def _calculate_trend_indicator(self, usage_values: List[int]) -> str:
        """
        Calculate a trend indicator using all data points.
        
        Args:
            usage_values: List of usage values in chronological order
            
        Returns:
            String representing the trend: "^" for upward, "v" for downward, 
            "~" for fluctuating, "-" for stable
        """
        if not usage_values or len(usage_values) < 3:
            # Not enough data points for trend analysis
            return "-"
        
        # Check if all values are the same
        if all(x == usage_values[0] for x in usage_values):
            return "-"  # Stable
        
        # Calculate differences between consecutive points
        diffs = [usage_values[i+1] - usage_values[i] for i in range(len(usage_values)-1)]
        
        # Count positive and negative changes
        pos_changes = sum(1 for d in diffs if d > 0)
        neg_changes = sum(1 for d in diffs if d < 0)
        
        # Calculate the overall trend direction
        if pos_changes > len(diffs) * 0.7:
            return "^"  # Consistently increasing
        elif neg_changes > len(diffs) * 0.7:
            return "v"  # Consistently decreasing
        else:
            return "~"  # Fluctuating
    
    def _get_top_users_across_reports(self, reports: List[Tuple[str, Dict[str, Any]]], 
                                     filter_users: List[str] = None, compare_by: str = "growth") -> set:
        """
        Get the top users across all reports.
        
        Args:
            reports: List of (file_path, report) tuples
            filter_users: Optional list of users to filter by
            compare_by: Method for selecting top users ('usage' or 'growth')
            
        Returns:
            Set of usernames for the top users
        """
        # Extract all users from all reports
        all_users = set()
        for _, report in reports:
            # Extract users from user_usage
            for user in report.get('user_usage', []):
                all_users.add(user['name'])
            
            # Extract users from other_usage
            for user in report.get('other_usage', []):
                all_users.add(user['name'])
        
        # Apply user filter if provided
        if filter_users:
            all_users = {user for user in all_users if user in filter_users}
        
        # If we have at least 2 reports, more than 10 users, and compare_by is 'growth',
        # select based on growth rate
        if len(reports) >= 2 and len(all_users) > 10 and compare_by == "growth":
            # Sort reports chronologically by date
            sorted_reports = sorted(reports, key=lambda r: r[1].get('date', 0))
            
            # Calculate trend-based growth rate for each user
            user_growth_rates = {}
            for user in all_users:
                # Get all usage values for this user across all reports
                user_usages = []
                for _, report in sorted_reports:
                    usage = self._find_user_usage_in_report(user, report)
                    user_usages.append(usage)
                
                # Calculate comprehensive growth
                abs_growth, pct_growth, _ = self._calculate_comprehensive_growth(user_usages)
                
                # Store growth information for sorting
                if pct_growth is not None:
                    user_growth_rates[user] = (pct_growth, abs_growth)
                else:
                    # If percentage growth can't be calculated, use absolute growth
                    user_growth_rates[user] = (0, abs_growth)
            
            # Sort users by growth rate (descending) and take top 10
            # Use absolute growth as a tie-breaker
            top_users = sorted(user_growth_rates.items(), 
                            key=lambda x: (x[1][0], x[1][1]), 
                            reverse=True)[:10]
            all_users = {user for user, _ in top_users}
        
        # If we don't have enough reports for growth calculation, compare_by is 'usage',
        # or we have fewer than 10 users, use the total usage method
        elif len(all_users) > 10:
            # Calculate total usage for each user across all reports
            user_total_usage = {}
            for user in all_users:
                total_usage = 0
                for _, report in reports:
                    # Check in user_usage
                    for u in report.get('user_usage', []):
                        if u['name'] == user:
                            total_usage += u.get('used', 0)
                    
                    # Check in other_usage
                    for u in report.get('other_usage', []):
                        if u['name'] == user:
                            total_usage += u.get('used', 0)
                
                user_total_usage[user] = total_usage
            
            # Sort users by total usage and take top 10
            top_users = sorted(user_total_usage.items(), key=lambda x: x[1], reverse=True)[:10]
            all_users = {user for user, _ in top_users}
        
        return all_users
    
    def _find_user_usage_in_report(self, user: str, report: Dict[str, Any]) -> int:
        """Find a user's usage in a specific report."""
        usage = 0
        
        # Check in user_usage
        for u in report.get('user_usage', []):
            if u['name'] == user:
                usage = u.get('used', 0)
                break
        
        # If not found in user_usage, check in other_usage
        if usage == 0:
            for u in report.get('other_usage', []):
                if u['name'] == user:
                    usage = u.get('used', 0)
                    break
        
        return usage