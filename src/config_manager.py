"""
Configuration Manager Module

Handles all configuration-related operations for the disk usage checker.
"""

import os
import json
import sys
from typing import Dict, List, Any, Optional, Set

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
        try:
            # Sort users alphabetically by name before saving
            if "users" in self._config:
                self._config["users"] = sorted(self._config["users"], key=lambda user: user["name"])
                
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self._config, f, indent=2)
        except IOError as e:
            print(f"Error saving configuration: {e}")
    
    def initialize_config(self, directory: str, output_file: str = "disk_usage_report.json") -> None:
        """
        Initialize a new configuration.
        
        Args:
            directory: Path to the directory to scan
            output_file: Path where the report will be saved
        """
        self._config = {
            "directory": directory,
            "output_file": output_file,
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
        
        # Add new team with next available team_ID
        if "teams" not in self._config:
            self._config["teams"] = []
        
        # Find the highest team_ID and increment by 1
        next_id = 1
        if self._config["teams"]:
            next_id = max(team.get("team_ID", 0) for team in self._config["teams"]) + 1
        
        self._config["teams"].append({
            "name": team_name,
            "team_ID": next_id
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
        # Find the team_ID for the given team_name
        team_id = None
        for team in self._config.get("teams", []):
            if team["name"] == team_name:
                team_id = team["team_ID"]
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
        
        # Add user to the users list with the team_ID
        self._config["users"].append({
            "name": username,
            "team_ID": team_id
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
    
    def get_teams(self) -> List[Dict[str, Any]]:
        """
        Get all teams from the configuration.
        
        Returns:
            List of team dictionaries
        """
        return self._config.get("teams", [])
    
    def get_users_by_team(self, team_name: str) -> List[str]:
        """
        Get all users in a specific team.
        
        Args:
            team_name: Name of the team
            
        Returns:
            List of usernames
        """
        # Find the team_ID for the given team_name
        team_id = None
        for team in self._config.get("teams", []):
            if team["name"] == team_name:
                team_id = team["team_ID"]
                break
        
        if team_id is None:
            return []
        
        # Return all usernames with matching team_ID, sorted alphabetically
        users = [user["name"] for user in self._config.get("users", []) if user["team_ID"] == team_id]
        return sorted(users)
    
    def get_all_users(self) -> List[str]:
        """
        Get all users from all teams.
        
        Returns:
            List of all usernames
        """
        users = [user["name"] for user in self._config.get("users", [])]
        return sorted(users)
    
    def get_user_team(self, username: str) -> Optional[str]:
        """
        Get the team name for a specific user.
        
        Args:
            username: Name of the user
            
        Returns:
            Team name or None if user not found
        """
        # Find the user and get their team_ID
        team_id = None
        for user in self._config.get("users", []):
            if user["name"] == username:
                team_id = user["team_ID"]
                break
        
        if team_id is None:
            return None
        
        # Find the team name for this team_ID
        for team in self._config.get("teams", []):
            if team["team_ID"] == team_id:
                return team["name"]
        
        return None
    
    def get_team_id_map(self) -> Dict[int, str]:
        """
        Get a mapping of team IDs to team names.
        
        Returns:
            Dictionary mapping team_ID to team name
        """
        return {team["team_ID"]: team["name"] for team in self._config.get("teams", [])}
    
    def add_users_batch(self, usernames: List[str], team_name: str) -> Set[str]:
        """
        Add multiple users to a team in a single operation.
        
        Args:
            usernames: List of usernames to add
            team_name: Name of the team to add users to
            
        Returns:
            Set of usernames that were already in the configuration
        """
        # Find the team_ID for the given team_name
        team_id = None
        for team in self._config.get("teams", []):
            if team["name"] == team_name:
                team_id = team["team_ID"]
                break
        
        if team_id is None:
            print(f"Team '{team_name}' not found")
            return set()
        
        # Initialize users list if needed
        if "users" not in self._config:
            self._config["users"] = []
        
        # Get existing usernames
        existing_users = {user["name"] for user in self._config["users"]}
        already_exists = set()
        
        # Add new users
        for username in usernames:
            if username in existing_users:
                already_exists.add(username)
            else:
                self._config["users"].append({
                    "name": username,
                    "team_ID": team_id
                })
        
        # Save if any users were added
        if len(already_exists) < len(usernames):
            self._save_config()
            
        return already_exists