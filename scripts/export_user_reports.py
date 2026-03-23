"""
export_user_reports.py

Reads per-user detail JSON reports (detail_report_dir_*.json and
detail_report_file_*.json) and exports one plain-text file per user.

Each text file lists every directory and file entry for that user,
sorted by size from highest to lowest.

Usage:
    python scripts/export_user_reports.py --input-dir /reports/
    python scripts/export_user_reports.py --input-dir /reports/ --output-dir /reports/txt/ --prefix sda1
"""

import os
import sys
import json
import glob
import argparse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def format_size(n: int) -> str:
    """Return human-readable size string."""
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024:
            return f"{n:.2f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.2f} EB"


def load_json(path: str) -> dict:
    """Load JSON file, return empty dict on failure."""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  [warn] Cannot read {path}: {exc}", file=sys.stderr)
        return {}


def _resolve_detail_dir(input_dir: str, prefix: str) -> str:
    """
    Return the directory that actually contains the detail JSON files.

    Handles two layouts:
      1. input_dir/detail_users/PREFIX_detail_report_dir_*.json  (standard)
      2. input_dir/PREFIX_detail_report_dir_*.json               (flat, user passed detail_users/ directly)
    """
    sub = os.path.join(input_dir, "detail_users")
    pat_prefix = f"{prefix}_" if prefix else ""
    probe = f"{pat_prefix}detail_report_dir_*.json"

    if glob.glob(os.path.join(sub, probe)):
        return sub          # standard layout
    if glob.glob(os.path.join(input_dir, probe)):
        return input_dir    # flat layout (input_dir IS detail_users/)
    return sub              # fallback — will produce a meaningful "not found" message


def find_users(input_dir: str, prefix: str) -> list:
    """
    Return sorted list of usernames discovered from detail_report_dir_*.json files.
    """
    detail_dir = _resolve_detail_dir(input_dir, prefix)

    pat_prefix = f"{prefix}_" if prefix else ""
    pattern = os.path.join(detail_dir, f"{pat_prefix}detail_report_dir_*.json")
    strip   = f"{pat_prefix}detail_report_dir_"

    print(f"  Searching: {pattern}", file=sys.stderr)

    users = []
    for path in glob.glob(pattern):
        name = os.path.basename(path)
        if name.startswith(strip) and name.endswith(".json"):
            user = name[len(strip):-len(".json")]
            users.append(user)
    return sorted(users)


def build_path(input_dir: str, prefix: str, segment: str, user: str) -> str:
    """Construct full path for a detail report file, auto-detecting layout."""
    detail_dir = _resolve_detail_dir(input_dir, prefix)
    pat_prefix = f"{prefix}_" if prefix else ""
    fname = f"{pat_prefix}{segment}_{user}.json"
    return os.path.join(detail_dir, fname)


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def build_entries(dir_data: dict, file_data: dict) -> list:
    """
    Merge directory and file entries into a single list, each item:
        {"kind": "dir"|"file", "path": str, "size": int}
    Sorted by size descending.
    """
    entries = []

    for d in dir_data.get("dirs", []):
        entries.append({"kind": "dir", "path": d["path"], "size": d["used"]})

    for f in file_data.get("files", []):
        entries.append({"kind": "file", "path": f["path"], "size": f["size"]})

    entries.sort(key=lambda x: x["size"], reverse=True)
    return entries


def export_user(user: str,
                input_dir: str,
                output_dir: str,
                prefix: str) -> str:
    """
    Load JSON reports for user, write plain-text report.
    Returns path of the written file.
    """
    dir_path  = build_path(input_dir, prefix, "detail_report_dir",  user)
    file_path = build_path(input_dir, prefix, "detail_report_file", user)

    dir_data  = load_json(dir_path)  if os.path.exists(dir_path)  else {}
    file_data = load_json(file_path) if os.path.exists(file_path) else {}

    if not dir_data and not file_data:
        print(f"  [skip] No data found for user: {user}", file=sys.stderr)
        return ""

    entries = build_entries(dir_data, file_data)

    # Build table content
    lines = [
        f"{'Type':<4}  {'User':<20}  {'Size':>12}  Path",
        "-" * 90,
    ]

    for e in entries:
        lines.append(f"{e['kind']:<4}  {user:<20}  {format_size(e['size']):>12}  {e['path']}")

    # Write output file
    parts = [p for p in [prefix, "usage", user] if p]
    out_fname = "_".join(parts) + ".txt"
    out_path  = os.path.join(output_dir, out_fname)

    os.makedirs(output_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export per-user disk usage detail reports as plain text.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--input-dir",  default=".",
                   help="Directory containing detail_report_dir/file JSON files (default: .)")
    p.add_argument("--output-dir", default=None,
                   help="Directory to write .txt files to (default: same as --input-dir)")
    p.add_argument("--prefix",     default="",
                   help="Filename prefix used when scanning (e.g. 'sda1')")
    p.add_argument("--users",      nargs="+", metavar="USER",
                   help="Only export specific user(s). Default: all discovered users.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_dir  = os.path.abspath(args.input_dir)
    output_dir = os.path.abspath(args.output_dir) if args.output_dir else input_dir
    prefix     = args.prefix

    users = args.users if args.users else find_users(input_dir, prefix)

    if not users:
        print(f"No user detail reports found in: {input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Exporting {len(users)} user(s) -> {output_dir}")
    written = 0
    for user in users:
        out = export_user(user, input_dir, output_dir, prefix)
        if out:
            print(f"  {user:20s}  {out}")
            written += 1

    print(f"Done. {written}/{len(users)} file(s) written.")


if __name__ == "__main__":
    main()
