"""
export_user_reports.py

Reads per-user detail JSON/NDJSON/DB reports and exports plain-text usage files per user.
This script uses a high-performance Rust core `export_rust` to parse reports and dump TXT.

Output contract note:
- Prefer canonical keys in source reports for best compatibility with disk_usage backend:
  - dirs rows: path, used
  - files rows: path, size, xt
- Legacy short keys (n/s) are still supported by parser-side normalization.
"""

import argparse
import glob
import os
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    # Ensure src package is accessible
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from src import export_rust
    HAS_RUST_EXPORTER = True
except ImportError as e:
    HAS_RUST_EXPORTER = False
    print(f"  [error] Rust exporter 'export_rust' not found ({e}). Please build it first via maturin.", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_detail_dir(input_dir: str, prefix: str) -> str:
    """Return the directory that actually contains the detail JSON files."""
    sub = os.path.join(input_dir, "detail_users")
    pat_prefix = f"{prefix}_" if prefix else ""

    if glob.glob(os.path.join(sub, f"{pat_prefix}detail_report_*.db")) or \
       glob.glob(os.path.join(sub, f"{pat_prefix}detail_report_*.ndjson")) or \
       glob.glob(os.path.join(sub, f"{pat_prefix}detail_report_*.json")):
        return sub
    if glob.glob(os.path.join(input_dir, f"{pat_prefix}detail_report_*.db")) or \
       glob.glob(os.path.join(input_dir, f"{pat_prefix}detail_report_*.ndjson")) or \
       glob.glob(os.path.join(input_dir, f"{pat_prefix}detail_report_*.json")):
        return input_dir
    return sub


def _find_data_detail_db(detail_dir: str) -> str:
    path = os.path.join(detail_dir, "data_detail.db")
    return path if os.path.exists(path) else ""


def _users_from_data_detail_db(db_path: str) -> list:
    if not db_path:
        return []
    try:
        conn = sqlite3.connect(db_path)
        try:
            table = "user_meta"
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE (type='table' OR type='view') AND name='user_meta' LIMIT 1"
            ).fetchone()
            if row is None:
                table = "users"
            rows = conn.execute(f"SELECT username FROM {table} ORDER BY username").fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        finally:
            conn.close()
    except Exception:
        return []


def find_users(input_dir: str, prefix: str) -> list:
    """Return sorted list of usernames discovered from detail reports or data_detail.db."""
    detail_dir = _resolve_detail_dir(input_dir, prefix)
    data_detail_db = _find_data_detail_db(detail_dir)
    db_users = _users_from_data_detail_db(data_detail_db)
    if db_users:
        return db_users

    pat_prefix = f"{prefix}_" if prefix else ""

    # Try looking for the unified detail_report_<user>.ndjson or .json pattern
    users = set()
    for ext in [".db", ".ndjson", ".json"]:
        unified_pattern = os.path.join(detail_dir, f"{pat_prefix}detail_report_*{ext}")
        strip_unified = f"{pat_prefix}detail_report_"

        for path in glob.glob(unified_pattern):
            name = os.path.basename(path)
            # Exclude legacy dir/file markers if mixed
            if name.startswith(strip_unified) and name.endswith(ext) and "dir_" not in name and "file_" not in name:
                user = name[len(strip_unified):-len(ext)]
                users.add(user)

        # If using legacy format fallback
        if not users:
            legacy_pattern = os.path.join(detail_dir, f"{pat_prefix}detail_report_dir_*{ext}")
            strip_legacy = f"{pat_prefix}detail_report_dir_"
            for path in glob.glob(legacy_pattern):
                name = os.path.basename(path)
                if name.startswith(strip_legacy) and name.endswith(ext):
                    user = name[len(strip_legacy):-len(ext)]
                    users.add(user)

        # New combined schema can store only detail_report_file_<user>.* with dirs in same DB.
        if not users:
            file_pattern = os.path.join(detail_dir, f"{pat_prefix}detail_report_file_*{ext}")
            strip_file = f"{pat_prefix}detail_report_file_"
            for path in glob.glob(file_pattern):
                name = os.path.basename(path)
                if name.startswith(strip_file) and name.endswith(ext):
                    user = name[len(strip_file):-len(ext)]
                    users.add(user)

        if users:
            break

    return sorted(list(users))


def _pick_existing_path(detail_dir: str, base_name: str) -> str:
    """Pick first existing path by preferred extension order."""
    for ext in [".db", ".ndjson", ".json"]:
        p = os.path.join(detail_dir, f"{base_name}{ext}")
        if os.path.exists(p):
            return p
    return ""


def _sqlite_has_dirs(path: str) -> bool:
    """True when SQLite DB contains a dirs table/view (combined schema)."""
    if not path or not path.endswith(".db") or not os.path.exists(path):
        return False
    try:
        conn = sqlite3.connect(path)
        try:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE (type='table' OR type='view') AND name='dirs' LIMIT 1"
            ).fetchone()
            return row is not None
        finally:
            conn.close()
    except Exception:
        return False


def build_paths(input_dir: str, prefix: str, user: str) -> tuple:
    """Return (unified_path, dir_path, file_path) auto-detecting layout."""
    detail_dir = _resolve_detail_dir(input_dir, prefix)
    pat_prefix = f"{prefix}_" if prefix else ""

    data_detail_db = _find_data_detail_db(detail_dir)
    if data_detail_db:
        return (data_detail_db, "", "")

    unified_path = _pick_existing_path(detail_dir, f"{pat_prefix}detail_report_{user}")
    dir_path = _pick_existing_path(detail_dir, f"{pat_prefix}detail_report_dir_{user}")
    file_path = _pick_existing_path(detail_dir, f"{pat_prefix}detail_report_file_{user}")

    return (unified_path, dir_path, file_path)


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def export_user(user: str,
                input_dir: str,
                output_dir: str,
                prefix: str,
                sem: threading.Semaphore) -> list:
    """
    Load JSON/NDJSON/DB reports for user and write plain-text report using Rust.
    Outputs TWO separate files: one for directories (dir) and one for files (file).
    """
    unified_path, dir_path, file_path = build_paths(input_dir, prefix, user)

    # Check what exists
    has_unified = os.path.exists(unified_path)
    has_dir = os.path.exists(dir_path)
    has_file = os.path.exists(file_path)
    has_combined_file_db = (not has_unified and not has_dir and has_file and _sqlite_has_dirs(file_path))

    if not has_unified and not has_dir and not has_file:
        print(f"  [skip] No data found for user: {user}", file=sys.stderr)
        return []

    results = []
    base_parts = [p for p in [prefix, "usage"] if p]

    def _collect_output(out_path: str, result_path: str) -> None:
        if result_path and os.path.exists(result_path):
            results.append(result_path)
        elif out_path and os.path.exists(out_path):
            results.append(out_path)

    with sem:
        try:
            if has_unified or has_combined_file_db:
                # Unified schema (detail_report_<user>.*) or combined file DB that contains dirs+files.
                source_path = unified_path if has_unified else file_path
                out_dir_fname = "_".join(base_parts + ["dir", user]) + ".txt"
                out_dir_path = os.path.join(output_dir, out_dir_fname)
                dir_result = export_rust.process(user, source_path, "", out_dir_path)
                _collect_output(out_dir_path, dir_result)

                out_file_fname = "_".join(base_parts + ["file", user]) + ".txt"
                out_file_path = os.path.join(output_dir, out_file_fname)
                file_result = export_rust.process(user, "", source_path, out_file_path)
                _collect_output(out_file_path, file_result)
            else:
                if has_dir:
                    out_dir_fname = "_".join(base_parts + ["dir", user]) + ".txt"
                    out_dir_path = os.path.join(output_dir, out_dir_fname)
                    dir_result = export_rust.process(user, dir_path, "", out_dir_path)
                    _collect_output(out_dir_path, dir_result)

                if has_file:
                    out_file_fname = "_".join(base_parts + ["file", user]) + ".txt"
                    out_file_path = os.path.join(output_dir, out_file_fname)
                    file_result = export_rust.process(user, "", file_path, out_file_path)
                    _collect_output(out_file_path, file_result)

            return results
        except Exception as e:
            raise RuntimeError(f"Rust export failed for {user}: {e}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export per-user disk usage detail reports as plain text using Rust core.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input-dir",   default=".",
                   help="Directory containing detail_report_dir/file JSON/NDJSON/DB files (default: .)")
    p.add_argument("--output-dir",  default=None,
                   help="Directory to write .txt files to (default: same as --input-dir)")
    p.add_argument("--prefix",      default="",
                   help="Filename prefix used when scanning (e.g. 'sda1')")
    p.add_argument("--users",       nargs="+", metavar="USER",
                   help="Only export specific user(s). Default: all discovered users.")
    p.add_argument("--workers",     type=int, default=4,
                   help="Number of parallel worker threads (default: 4).")
    p.add_argument("--max-readers", type=int, default=4,
                   help="Max workers that may read detail files simultaneously (default: 4).")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_dir   = os.path.abspath(args.input_dir)
    output_dir  = os.path.abspath(args.output_dir) if args.output_dir else input_dir
    prefix      = args.prefix
    workers     = max(1, args.workers)
    max_readers = max(1, args.max_readers)

    read_sem = threading.Semaphore(max_readers)

    users = args.users if args.users else find_users(input_dir, prefix)

    if not users:
        print(f"No user detail reports found in: {input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Exporting {len(users)} user(s) using Rust Core [parallel {workers}w] -> {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    results: list = []
    failed:  list = []

    completed = 0
    total = len(users)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(export_user, u, input_dir, output_dir, prefix, read_sem): u
            for u in users
        }
        for fut in as_completed(futures):
            user = futures[fut]
            completed += 1
            try:
                out = fut.result()
                if out:
                    results.append((user, out))
            except Exception as exc:
                failed.append(user)
                print(f"  [warn] {user}: {exc}", file=sys.stderr, flush=True)
            finally:
                # Keep console output compact on large user sets.
                print(f"\rProgress: {completed}/{total} users processed", end="", flush=True)

    print("")

    if failed:
        print(f"\n  [warn] {len(failed)} export(s) failed: {failed}", file=sys.stderr)

    written_files = sum(len(paths) for _, paths in results)
    print(f"Done. {len(results)}/{len(users)} user(s) exported, {written_files} TXT file(s) written.")


if __name__ == "__main__":
    main()
