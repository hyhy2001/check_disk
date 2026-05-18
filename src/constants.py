"""
Centralised constants for the disk_checker package.

Filenames, directory names, and timing defaults that were previously
duplicated as string literals across modules. Update here, references
update everywhere.
"""

DEFAULT_REPORT_FILENAME = "disk_usage_report.json"
PERMISSION_ISSUES_DB_FILENAME = "permission_issues.db"
INODE_USAGE_REPORT_FILENAME = "inode_usage_report.json"
SCAN_STATUS_FILENAME = "scan_status.json"

DETAIL_USERS_DIRNAME = "detail_users"
DETAIL_USERS_DB_FILENAME = "data_detail.db"

TREE_MAP_DATA_DIRNAME = "tree_map_data"
TREE_MAP_DB_FILENAME = "treemap.db"

SIBLING_REPORT_FILENAMES = (
    INODE_USAGE_REPORT_FILENAME,
    PERMISSION_ISSUES_DB_FILENAME,
)

DEFAULT_HEARTBEAT_INTERVAL = 5.0
DEFAULT_HEARTBEAT_SYNC_INTERVAL = 30.0
