"""
migrate_permission_issues.py

Converts permission_issues JSON files from the old nested format to the
new flat format required by the web dashboard's paginated API.

Old format (nested):
    {
      "permission_issues": {
        "users": [
          {"name": "alice", "inaccessible_items": [{"path": ..., "type": ..., "error": ...}]}
        ],
        "unknown_items": [{"path": ..., "type": ..., "error": ...}]
      }
    }

New format (flat):
    {
      "permission_issues": {
        "total": 42,
        "items": [
          {"user": "alice",       "path": ..., "type": ..., "error": ...},
          {"user": "__unknown__", "path": ..., "type": ..., "error": ...}
        ]
      }
    }

Files already in the new format are detected automatically and skipped.

Usage:
    # Single file, overwrite in-place
    python scripts/migrate_permission_issues.py --report /reports/permission_issues.json

    # Batch: all matching files in a directory tree
    python scripts/migrate_permission_issues.py --reports "/reports/**/permission_issues*.json"

    # Dry-run: preview what would change without writing
    python scripts/migrate_permission_issues.py --reports "*.json" --dry-run

    # Write to a new file, keep original untouched
    python scripts/migrate_permission_issues.py --report old.json --output new.json
"""

import argparse
import glob
import json
import os
import re
import shutil
import sys
from typing import List, Tuple


def _compact_json(obj, indent: int = 4) -> str:
    """Serialize JSON with plain-dict list items on a single line each."""
    raw = json.dumps(obj, indent=indent, ensure_ascii=False)
    pattern = re.compile(r"\{[^{}\[\]]*\}", re.DOTALL)
    def collapse(m):
        inner = m.group(0)
        if "{" not in inner[1:] and "[" not in inner:
            return re.sub(r"\s+", " ", inner).strip()
        return inner
    return pattern.sub(collapse, raw)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def is_new_format(issues: dict) -> bool:
    """Return True if permission_issues block is already in flat format."""
    return "items" in issues and "total" in issues


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

def migrate_issues(issues: dict) -> Tuple[dict, int]:
    """
    Convert a permission_issues dict from nested to flat format.

    Returns:
        (new_issues_dict, fields_converted)
        fields_converted == 0 means already in new format (idempotent).
    """
    if is_new_format(issues):
        return issues, 0

    items: List[dict] = []

    # Named users — sorted alphabetically for deterministic output
    for user_entry in sorted(issues.get("users", []), key=lambda u: u.get("name", "")):
        username = user_entry.get("name", "")
        for item in user_entry.get("inaccessible_items", []):
            items.append({
                "user":  username,
                "path":  item.get("path",  ""),
                "type":  item.get("type",  ""),
                "error": item.get("error", ""),
            })

    # Unknown / orphan items last
    for item in issues.get("unknown_items", []):
        items.append({
            "user":  "__unknown__",
            "path":  item.get("path",  ""),
            "type":  item.get("type",  ""),
            "error": item.get("error", ""),
        })

    converted = len(items)
    return {"total": converted, "items": items}, converted


def migrate_file(src: str, dst: str, dry_run: bool = False) -> Tuple[str, int]:
    """
    Migrate a single permission_issues JSON file.

    Returns:
        (status_string, items_converted)
        status: 'migrated' | 'skipped' | 'error:<msg>'
    """
    try:
        with open(src, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return f"error:Cannot read file: {exc}", 0

    issues = data.get("permission_issues")
    if not isinstance(issues, dict):
        return "error:No 'permission_issues' key found", 0

    if is_new_format(issues):
        return "skipped", 0

    new_issues, count = migrate_issues(issues)
    data["permission_issues"] = new_issues

    if dry_run:
        return f"dry-run:{count} items would be migrated", count

    # Atomic write: write to temp, then rename
    tmp = dst + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(_compact_json(data))
        shutil.move(tmp, dst)
    except OSError as exc:
        if os.path.exists(tmp):
            os.remove(tmp)
        return f"error:Cannot write: {exc}", 0

    return "migrated", count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate permission_issues JSON from nested to flat format.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--report", metavar="FILE",
        help="Single permission_issues JSON file to migrate.",
    )
    group.add_argument(
        "--reports", metavar="GLOB",
        help='Glob pattern matching multiple files. Quote on shell: "path/**/*.json"',
    )
    p.add_argument(
        "--output", metavar="FILE",
        help="Write migrated result to this file instead of overwriting --report. "
             "Only valid with --report.",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done without writing anything.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.output and args.reports:
        print("Error: --output can only be used with --report, not --reports.", file=sys.stderr)
        sys.exit(1)

    # Collect files
    if args.report:
        files = [os.path.abspath(args.report)]
    else:
        files = sorted(glob.glob(args.reports, recursive=True))

    if not files:
        print("No files found.", file=sys.stderr)
        sys.exit(1)

    mode = "[DRY-RUN] " if args.dry_run else ""
    print(f"{mode}Migrating {len(files)} file(s)...\n")

    migrated = skipped = errors = total_items = 0

    for src in files:
        dst = os.path.abspath(args.output) if args.output else src
        status, count = migrate_file(src, dst, dry_run=args.dry_run)

        short = os.path.basename(src)

        if status == "skipped":
            print(f"  →  {short:50s}  already in new format")
            skipped += 1
        elif status.startswith("error:"):
            msg = status[len("error:"):]
            print(f"  ✗  {short:50s}  ERROR: {msg}", file=sys.stderr)
            errors += 1
        elif status.startswith("dry-run:"):
            detail = status[len("dry-run:"):]
            print(f"  ○  {short:50s}  {detail}")
            migrated += 1
            total_items += count
        else:
            print(f"  ✓  {short:50s}  {count} item(s) migrated")
            migrated += 1
            total_items += count

    print(f"\nDone: {migrated} migrated, {skipped} skipped, {errors} error(s). "
          f"Total items converted: {total_items}.")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
