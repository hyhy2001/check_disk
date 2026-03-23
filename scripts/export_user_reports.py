"""
export_user_reports.py

Reads per-user detail JSON reports (detail_report_dir_*.json and
detail_report_file_*.json) and exports one plain-text file per user.

Each text file lists every directory and file entry for that user,
sorted by size from highest to lowest.

Usage:
    python scripts/export_user_reports.py --input-dir /reports/
    python scripts/export_user_reports.py --input-dir /reports/ --output-dir /reports/txt/ --prefix sda1
    python scripts/export_user_reports.py --input-dir /reports/ --workers 8
"""

import os
import sys
import json
import glob
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


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


def load_json(path: str, sem: threading.Semaphore) -> dict:
    """
    Load a JSON file under a shared semaphore to cap concurrent readers.

    Only `sem` threads may hold an open file-handle at the same time,
    bounding peak memory to: max_readers × sizeof(largest JSON file).
    """
    try:
        with sem:
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

def build_entries(dir_path: str, file_path: str, sem: threading.Semaphore) -> list:
    """
    Load dir/file JSON reports and merge into one sorted entry list.

    Reads are gated by `sem` so at most N workers hold file data in
    RAM simultaneously, capping peak memory regardless of worker count.
    """
    entries: list = []

    if os.path.exists(dir_path):
        for d in load_json(dir_path, sem).get("dirs", []):
            entries.append({"kind": "dir", "path": d["path"], "size": d["used"]})

    if os.path.exists(file_path):
        for f in load_json(file_path, sem).get("files", []):
            entries.append({"kind": "file", "path": f["path"], "size": f["size"]})

    entries.sort(key=lambda x: x["size"], reverse=True)
    return entries


def export_user(user: str,
                input_dir: str,
                output_dir: str,
                prefix: str,
                sem: threading.Semaphore) -> str:
    """
    Load JSON reports for user (semaphore-gated) and write plain-text report.
    Returns path of the written file, or empty string if skipped.

    Memory strategy: `sem` limits how many workers may hold JSON data
    in RAM at the same time, so peak memory = max_readers x file_size
    instead of workers x file_size.
    """
    dir_path  = build_path(input_dir, prefix, "detail_report_dir",  user)
    file_path = build_path(input_dir, prefix, "detail_report_file", user)

    if not os.path.exists(dir_path) and not os.path.exists(file_path):
        print(f"  [skip] No data found for user: {user}", file=sys.stderr)
        return ""

    entries = build_entries(dir_path, file_path, sem)

    if not entries:
        print(f"  [skip] Empty data for user: {user}", file=sys.stderr)
        return ""

    lines = [
        f"{'Type':<4}  {'User':<20}  {'Size':>12}  Path",
        "-" * 90,
    ]

    for e in entries:
        lines.append(f"{e['kind']:<4}  {user:<20}  {format_size(e['size']):>12}  {e['path']}")

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
    p.add_argument("--input-dir",   default=".",
                   help="Directory containing detail_report_dir/file JSON files (default: .)")
    p.add_argument("--output-dir",  default=None,
                   help="Directory to write .txt files to (default: same as --input-dir)")
    p.add_argument("--prefix",      default="",
                   help="Filename prefix used when scanning (e.g. 'sda1')")
    p.add_argument("--users",       nargs="+", metavar="USER",
                   help="Only export specific user(s). Default: all discovered users.")
    p.add_argument("--workers",     type=int, default=4,
                   help="Number of parallel worker threads (default: 4). "
                        "Use 1 for sequential (legacy) behaviour.")
    p.add_argument("--max-readers", type=int, default=2,
                   help="Max workers that may read JSON files simultaneously (default: 2). "
                        "Lower = less memory, higher = faster I/O.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_dir   = os.path.abspath(args.input_dir)
    output_dir  = os.path.abspath(args.output_dir) if args.output_dir else input_dir
    prefix      = args.prefix
    workers     = max(1, args.workers)
    max_readers = max(1, args.max_readers)

    # Semaphore caps concurrent JSON readers → peak memory = max_readers × file_size
    read_sem = threading.Semaphore(max_readers)

    users = args.users if args.users else find_users(input_dir, prefix)

    if not users:
        print(f"No user detail reports found in: {input_dir}", file=sys.stderr)
        sys.exit(1)

    mode = f"parallel {workers}w (max-readers={max_readers})" if workers > 1 else "sequential"
    print(f"Exporting {len(users)} user(s) [{mode}] -> {output_dir}")

    results: list = []
    failed:  list = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(export_user, u, input_dir, output_dir, prefix, read_sem): u
            for u in users
        }
        for fut in as_completed(futures):
            user = futures[fut]
            try:
                out = fut.result()
                if out:
                    results.append((user, out))
            except Exception as exc:
                failed.append(user)
                print(f"  [warn] {user}: {exc}", file=sys.stderr)

    # Print results sorted by username for deterministic output
    for user, out in sorted(results):
        print(f"  {user:20s}  {out}")

    if failed:
        print(f"\n  [warn] {len(failed)} export(s) failed: {failed}", file=sys.stderr)

    written = len(results)
    print(f"Done. {written}/{len(users)} file(s) written.")


if __name__ == "__main__":
    main()
