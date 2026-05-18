"""
Configuration Manager Module

Handles all configuration-related operations for the disk usage checker.
"""

import json
import os
import sys
from typing import Any, Dict


class ConfigManager:
    """Manages the configuration for the disk usage checker."""

    CONFIG_FILE = "disk_checker_config.json"

    def __init__(self, config_file: str = None):
        """
        Initialize the configuration manager.

        Args:
            config_file: Optional custom configuration file path
        """
        if config_file:
            self.config_file = config_file
        else:
            # Get the directory of the main script (disk_checker.py)
            script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
            # Use the config file from the same directory as the script
            self.config_file = os.path.join(script_dir, self.CONFIG_FILE)

        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from the config file.

        Returns:
            Dict containing the configuration or empty dict if file doesn't exist
        """
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Error loading configuration: {e}")
                return {}
        return {}

    def _save_config(self) -> None:
        """Save the current configuration to the config file."""
        from .utils import _compact_json
        try:
            if "users" in self._config:
                self._config["users"] = sorted(self._config["users"], key=lambda u: u["name"])

            with open(self.config_file, "w", encoding="utf-8") as f:
                f.write(_compact_json(self._config))
        except IOError as e:
            print(f"Error saving configuration: {e}")

    def initialize_config(self, directory: str, output_file: str = None) -> None:
        """
        Initialize a new configuration.

        Args:
            directory: Path to the directory to scan
            output_file: Path where the report will be saved
        """
        from .constants import DEFAULT_REPORT_FILENAME
        self._config = {
            "directory": directory,
            "output_file": output_file or DEFAULT_REPORT_FILENAME,
            "teams": [],
            "users": []
        }
        self._save_config()

    def update_directory(self, directory: str) -> None:
        """
        Update the directory in the configuration.

        Args:
            directory: New directory path to use for scanning
        """
        if not self._config:
            print("Error: No configuration found. Run --init first.")
            return

        self._config["directory"] = directory
        self._save_config()

    def add_team(self, team_name: str) -> None:
        """
        Add a new team to the configuration.

        Args:
            team_name: Name of the team to add
        """
        # Check if team already exists
        for team in self._config.get("teams", []):
            if team["name"] == team_name:
                print(f"Team '{team_name}' already exists")
                return

        # Add new team with next available team_id
        if "teams" not in self._config:
            self._config["teams"] = []

        # Find the highest team_id and increment by 1
        next_id = 1
        if self._config["teams"]:
            next_id = max(team.get("team_id", 0) for team in self._config["teams"]) + 1

        self._config["teams"].append({
            "name": team_name,
            "team_id": next_id
        })
        self._save_config()

    def add_user(self, username: str, team_name: str) -> bool:
        """
        Add a user to a team.

        Args:
            username: Name of the user to add
            team_name: Name of the team to add the user to

        Returns:
            True if user already exists, False otherwise
        """
        # Find the team_id for the given team_name
        team_id = None
        for team in self._config.get("teams", []):
            if team["name"] == team_name:
                team_id = team["team_id"]
                break

        if team_id is None:
            print(f"Team '{team_name}' not found")
            return False

        # Check if user already exists
        if "users" not in self._config:
            self._config["users"] = []

        for user in self._config["users"]:
            if user["name"] == username:
                print(f"User '{username}' already exists")
                return True

        # Add user to the users list with the team_id
        self._config["users"].append({
            "name": username,
            "team_id": team_id
        })
        self._save_config()
        return False

    def remove_user(self, username: str) -> None:
        """
        Remove a user from the configuration.

        Args:
            username: Name of the user to remove
        """
        user_found = False

        if "users" in self._config:
            original_length = len(self._config["users"])
            self._config["users"] = [user for user in self._config["users"] if user["name"] != username]
            user_found = len(self._config["users"]) < original_length

        if user_found:
            self._save_config()
            print(f"User '{username}' removed successfully")
        else:
            print(f"User '{username}' not found")

    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration.

        Returns:
            Dict containing the current configuration
        """
        return self._config
