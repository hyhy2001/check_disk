#!/usr/bin/env python3
"""
Configuration Converter Script

Converts the old configuration format to the new format.
"""

import json
import sys

def convert_config(old_config_path, new_config_path):
    """
    Convert old config format to new format.
    
    Args:
        old_config_path: Path to the old config file
        new_config_path: Path to save the new config file
    """
    try:
        # Load old config
        with open(old_config_path, 'r', encoding='utf-8') as f:
            old_config = json.load(f)
        
        # Create new config structure
        new_config = {
            "directory": old_config.get("location", ""),
            "output_file": old_config.get("output_file", "disk_usage_report.json"),
            "teams": [],
            "users": []
        }
        
        # Convert teams and assign IDs
        team_id_map = {}  # Maps team name to ID
        for i, team in enumerate(old_config.get("teams", []), 1):
            team_name = team["name"]
            team_id_map[team_name] = i
            new_config["teams"].append({
                "name": team_name,
                "team_id": i
            })
        
        # Convert users
        for team in old_config.get("teams", []):
            team_name = team["name"]
            team_id = team_id_map[team_name]
            
            for username in team.get("users", []):
                new_config["users"].append({
                    "name": username,
                    "team_id": team_id
                })
        
        # Save new config
        with open(new_config_path, 'w', encoding='utf-8') as f:
            json.dump(new_config, f, indent=2)
            
        print(f"Configuration successfully converted and saved to {new_config_path}")
        
    except Exception as e:
        print(f"Error converting configuration: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python convert_config.py <old_config_path> <new_config_path>")
        sys.exit(1)
    
    old_config_path = sys.argv[1]
    new_config_path = sys.argv[2]
    convert_config(old_config_path, new_config_path)