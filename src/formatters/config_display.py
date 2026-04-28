"""
Configuration Display Module

Contains functionality for displaying configuration information.
"""

from typing import Any, Dict, List

from .base_formatter import BaseFormatter
from .table_formatter import TableFormatter


class ConfigDisplay(BaseFormatter):
    """Helper class for displaying configuration information."""

    def __init__(self):
        """Initialize the config display helper."""
        super().__init__()
        self.table_formatter = TableFormatter()


    def display_config_summary(self, config: Dict[str, Any]) -> None:
        """Display a summary of the configuration."""
        print("\nConfiguration Summary")
        print(f"Directory: {config.get('directory', 'Not set')}")
        print(f"Output File: {config.get('output_file', 'disk_usage_report.json')}")

        # Count teams and users
        team_count = len(config.get("teams", []))
        user_count = len(config.get("users", []))

        print(f"Teams: {team_count}")
        print(f"Users: {user_count}")

    def display_users_by_team_table(self, config: Dict[str, Any], team_filter: str = None) -> None:
        """Display users grouped by team as a table."""
        teams = config.get("teams", [])
        if not teams:
            print("\nNo teams defined in configuration.")
            return

        # Create a mapping of team_id to team name
        team_id_map = {team["team_id"]: team["name"] for team in teams}

        # Filter teams if team_filter is provided
        if team_filter:
            # Find teams that match the filter (case-insensitive)
            filtered_teams = [team for team in teams
                             if team_filter.lower() in team["name"].lower()]

            if not filtered_teams:
                print(f"\nNo teams found matching '{team_filter}'.")
                return

            teams = filtered_teams

        # Group users by team
        users_by_team = {}
        for team in teams:
            users_by_team[team["team_id"]] = []

        for user in config.get("users", []):
            team_id = user["team_id"]
            if team_id in users_by_team:
                users_by_team[team_id].append(user["name"])

        # Sort teams by name
        sorted_teams = sorted(teams, key=lambda t: t["name"])

        # Display title based on whether we're filtering
        if team_filter:
            print(f"\nUSERS IN TEAM(S) MATCHING '{team_filter}'")
        else:
            print("\nUSERS BY TEAM")

        # Display a separate table for each team with better formatting
        for team in sorted_teams:
            self._display_team_users_table(team, users_by_team)

        # Only show users with unknown team IDs if not filtering by team
        if not team_filter:
            self._display_unknown_team_users(config, team_id_map)

    def _display_team_users_table(self, team: Dict[str, Any], users_by_team: Dict[str, List[str]]) -> None:
        """Display a table of users for a specific team."""
        team_id = team["team_id"]
        team_users = sorted(users_by_team.get(team_id, []))

        if not team_users:
            print(f"\n{team['name']} ({len(team_users)} users)")
            print("No users in this team.")
            return

        # Create table headers and title
        title = f"{team['name']} ({len(team_users)} users)"

        # Calculate optimal column count based on terminal width and username lengths
        max_username_length = max([len(user) for user in team_users] + [10]) + 2  # +2 for padding

        # Determine number of columns that can fit in the terminal
        num_columns = max(1, min(4, self.table_formatter.terminal_width // (max_username_length + 4)))

        # Create headers for each column
        headers = ["Username"] * num_columns

        # Calculate number of rows needed
        num_rows = (len(team_users) + num_columns - 1) // num_columns

        # Create rows with sequential numbering in top-to-bottom order
        rows = self.table_formatter._create_multicolumn_rows(team_users, num_rows, num_columns)

        # Display the table with appropriate styling
        table = self.table_formatter.format_table(headers, rows, title=title)
        print(f"\n{table}")

    def _display_unknown_team_users(self, config: Dict[str, Any], team_id_map: Dict[str, str]) -> None:
        """Display users with unknown team IDs."""
        # Check for users with unknown team IDs
        unknown_team_users = [user["name"] for user in config.get("users", [])
                            if user["team_id"] not in team_id_map]

        if not unknown_team_users:
            return

        # Sort unknown team users alphabetically
        unknown_team_users.sort()

        # Display users with unknown teams in a separate table
        title = f"Unknown Team ({len(unknown_team_users)} users)"

        # Calculate optimal column count for unknown users
        max_username_length = max([len(user) for user in unknown_team_users] + [10]) + 2
        num_columns = max(1, min(4, self.table_formatter.terminal_width // (max_username_length + 4)))

        # Create headers for each column
        headers = ["Username"] * num_columns

        # Calculate number of rows needed
        num_rows = (len(unknown_team_users) + num_columns - 1) // num_columns

        # Create rows with sequential numbering in top-to-bottom order
        rows = self.table_formatter._create_multicolumn_rows(unknown_team_users, num_rows, num_columns)

        table = self.table_formatter.format_table(headers, rows, title=title)
        print(f"\n{table}")
        print("\nWARNING: These users have unknown team IDs.")
