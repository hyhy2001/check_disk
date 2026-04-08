"""
Utilities Module

Contains common utility functions used across the disk checker application.
"""

import os
import pwd
import time
import json
import shutil
from typing import Dict, Any, List, Optional, Tuple, Set

def format_size(size_bytes: int) -> str:
    """
    Format size in bytes to human-readable format.
    Uses SI/decimal units (1 KB = 1,000 B) to match the dashboard display.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable size string
    """
    if size_bytes < 0:
        return "0 B"

    TB = 1e12; GB = 1e9; MB = 1e6; KB = 1e3
    abs_bytes = float(size_bytes)

    if abs_bytes >= TB:
        return f"{abs_bytes / TB:.2f} TB"
    if abs_bytes >= GB:
        return f"{abs_bytes / GB:.1f} GB"
    if abs_bytes >= MB:
        return f"{abs_bytes / MB:.0f} MB"
    if abs_bytes >= KB:
        return f"{abs_bytes / KB:.0f} KB"
    return f"{size_bytes} B"

def format_time_duration(seconds: float) -> str:
    """
    Format time duration in seconds to hours, minutes, and seconds.
    
    Args:
        seconds: Time duration in seconds
        
    Returns:
        Formatted time string (HH:MM:SS)
    """
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def parse_size(size_str: str) -> int:
    """
    Parse a human-readable size string to bytes.
    
    Args:
        size_str: Size string (e.g., '1TB', '500GB')
        
    Returns:
        Size in bytes
    """
    if not size_str:
        return 0
        
    size_str = size_str.upper().strip()
    multipliers = {
        'B': 1,
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4,
        'PB': 1024**5
    }
    
    # Handle numeric-only input as bytes
    if size_str.isdigit():
        return int(size_str)
        
    # Extract number and unit
    for unit, multiplier in multipliers.items():
        if size_str.endswith(unit):
            try:
                number = float(size_str[:-len(unit)])
                return int(number * multiplier)
            except ValueError:
                return 0
                
    return 0

def get_terminal_size() -> Tuple[int, int]:
    """
    Get the current terminal size.
    
    Returns:
        Tuple of (width, height)
    """
    return shutil.get_terminal_size((80, 24))

def get_username_from_uid(uid: int, uid_cache: Dict[int, str] = None) -> str:
    """
    Get username from UID with caching.
    
    Args:
        uid: User ID
        uid_cache: Optional cache dictionary to use
        
    Returns:
        Username string
    """
    if uid_cache is not None and uid in uid_cache:
        return uid_cache[uid]
        
    try:
        username = pwd.getpwuid(uid).pw_name
    except KeyError:
        username = f"uid-{uid}"
        
    if uid_cache is not None:
        uid_cache[uid] = username
        
    return username

def build_uid_cache() -> Dict[int, str]:
    """
    Build a cache of UID to username mappings.
    
    Returns:
        Dictionary mapping UIDs to usernames
    """
    uid_cache = {}
    try:
        for entry in pwd.getpwall():
            uid_cache[entry.pw_uid] = entry.pw_name
    except Exception:
        pass
    return uid_cache

def get_general_system_info(directory: str) -> Dict[str, int]:
    """
    Get general system disk information.
    
    Args:
        directory: Path to check
        
    Returns:
        Dictionary with total, used, and available space
    """
    try:
        total, used, free = shutil.disk_usage(directory)
        return {"total": total, "used": used, "available": free}
    except Exception:
        return {"total": 0, "used": 0, "available": 0}

def get_actual_disk_usage(stat_result):
    """
    Calculate the actual disk space used by a file, accounting for sparse files.

    Args:
        stat_result: Result from os.stat()

    Returns:
        int: Actual size in bytes the file occupies on disk
    """
    # st_blocks gives 512-byte blocks
    if hasattr(stat_result, 'st_blocks'):
        return stat_result.st_blocks * 512
    else:
        return stat_result.st_size


def create_usage_bar(percent: float, width: int = 20) -> str:
    """
    Create a visual ASCII bar representing a percentage.

    Args:
        percent: Percentage value (0-100)
        width: Bar width in characters

    Returns:
        String like "[########------------]"
    """
    filled = max(0, min(width, int((percent / 100) * width)))
    return f"[{'#' * filled}{'-' * (width - filled)}]"

def get_owner_from_path(path: str, uid_cache: Dict[int, str] = None) -> str:
    """
    Get the owner username of a file or directory.
    
    Args:
        path: Path to check
        uid_cache: Optional cache dictionary to use
        
    Returns:
        Username of the owner
    """
    try:
        stat_info = os.stat(path)
        uid = stat_info.st_uid
        return get_username_from_uid(uid, uid_cache)
    except (OSError, KeyError):
        return "unknown"

def format_timestamp(timestamp: int) -> str:
    """
    Format a Unix timestamp to human-readable date/time.
    
    Args:
        timestamp: Unix timestamp
        
    Returns:
        Formatted date/time string
    """
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

def _compact_json(obj, indent: int = 2) -> str:
    """
    Serialize obj to JSON where list items that are plain dicts
    are written on a single line (horizontal), everything else
    uses normal indentation.
    """
    import re

    raw = json.dumps(obj, indent=indent)

    # Find every indented block that is a plain dict: {  "k": v, ... }
    # and collapse it to a single line.
    def collapse(m: "re.Match") -> str:
        inner = m.group(0)
        # Only collapse if it contains no nested arrays/objects
        if "{" not in inner[1:] and "[" not in inner:
            flat = re.sub(r"\s+", " ", inner).strip()
            return flat
        return inner

    # Match a { ... } that spans multiple lines but has no nested braces
    pattern = re.compile(
        r"\{[^{}\[\]]*\}",
        re.DOTALL,
    )
    return pattern.sub(collapse, raw)


def save_json_report(report: Dict[str, Any], filepath: str) -> None:
    """
    Save a report to a JSON file.
    List items that are plain dicts are written compact (one line each).

    Args:
        report: Report data dictionary
        filepath: Path to save the report
    """
    try:
        output_dir = os.path.dirname(filepath)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(_compact_json(report, indent=4))
    except IOError as e:
        print(f"Error saving report to {filepath}: {e}")

def load_json_report(filepath: str) -> Dict[str, Any]:
    """
    Load a report from a JSON file.
    
    Args:
        filepath: Path to the report file
        
    Returns:
        Dictionary containing the report data
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error loading report from {filepath}: {e}")
        return {}

class ScanHelper:
    """Helper class for disk scanning operations."""
    
    @staticmethod
    def process_user_data(uid_sizes: Dict[int, int], uid_to_username: Dict[int, str], 
                         user_map: Dict[str, str]) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
        """
        Process user data from UID sizes.
        
        Args:
            uid_sizes: Dictionary mapping UIDs to sizes
            uid_to_username: Dictionary mapping UIDs to usernames
            user_map: Dictionary mapping usernames to team names
            
        Returns:
            Tuple of (user_usage, team_usage, other_usage) dictionaries
        """
        user_usage = {}
        team_usage = {}
        other_usage = {}
        
        # Initialize team usage counters
        for team_name in set(user_map.values()):
            team_usage[team_name] = 0
        
        # Process user data
        for uid, size in uid_sizes.items():
            username = get_username_from_uid(uid, uid_to_username)
            
            # Categorize by team or as "other"
            if username in user_map:
                user_usage[username] = size
                team_usage[user_map[username]] += size
            else:
                other_usage[username] = size
        
        return user_usage, team_usage, other_usage
    
    @staticmethod
    def create_user_list(user_usage: Dict[str, int], sort: bool = True) -> List[Dict[str, Any]]:
        """
        Create a list of user dictionaries from user usage data.
        
        Args:
            user_usage: Dictionary mapping usernames to sizes
            sort: Whether to sort the list by usage (descending)
            
        Returns:
            List of dictionaries with 'name' and 'used' keys
        """
        user_list = [{"name": name, "used": size} for name, size in user_usage.items()]
        if sort:
            user_list.sort(key=lambda x: x["used"], reverse=True)
        return user_list
    
    @staticmethod
    def filter_users_by_names(user_list: List[Dict[str, Any]], usernames: Set[str]) -> List[Dict[str, Any]]:
        """
        Filter users by username.
        
        Args:
            user_list: List of user dictionaries
            usernames: Set of usernames to include
            
        Returns:
            Filtered list of user dictionaries
        """
        return [user for user in user_list if user["name"] in usernames]