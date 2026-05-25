"""
export_user_reports.py

Exports per-user disk usage detail reports as plain text by calling the
high-performance Rust core ``export_rust``. The Rust side reads the SQLite
databases produced by Phase 2:

  * ``detail_users/data_detail.db`` — per-user file/dir breakdown
  * ``tree_map_data/treemap.db``    — shared path dictionary + tree

This script replaces the previous NDJSON-based exporter.
"""

import argparse
import os
import sqlite3
import sys
import time

# Ensure src package is accessible.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    from src import export_rust
    from src.constants import (
        DETAIL_USERS_DB_FILENAME,
        DETAIL_USERS_DIRNAME,
        TREE_MAP_DATA_DIRNAME,
        TREE_MAP_DB_FILENAME,
    )
    HAS_RUST_EXPORTER = True
except ImportError as e:
    HAS_RUST_EXPORTER = False
    print(
        f"  [error] Rust exporter 'export_rust' not found ({e}). "
        f"Build it first via `bash src/rust_exporter/build.sh`.",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Discovery helpers
# ---------------------------------------------------------------------------

def _resolve_db_paths(input_dir: str):
    """Return (detail_db, treemap_db). `treemap_db` may be '' if missing."""
    detail = os.path.join(input_dir, DETAIL_USERS_DIRNAME, DETAIL_USERS_DB_FILENAME)
    if not os.path.isfile(detail):
        return "", ""
    tm = os.path.join(input_dir, TREE_MAP_DATA_DIRNAME, TREE_MAP_DB_FILENAME)
    return detail, tm if os.path.isfile(tm) else ""


def find_users(input_dir: str, prefix: str) -> list:
    """Return sorted list of usernames in ``data_detail.db``."""
    _ = prefix
    detail_db, _tm = _resolve_db_paths(input_dir)
    if not detail_db:
        return []
    try:
        conn = sqlite3.connect(f"file:{detail_db}?mode=ro", uri=True)
        try:
            rows = conn.execute(
                "SELECT username FROM users ORDER BY username"
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return []
    return [r[0] for r in rows]


def build_paths(input_dir: str, prefix: str, user: str):
    """Return ``(detail_db, treemap_db, '')`` so callers can detect "no data"
    via empty first slot. Third slot is unused; kept for legacy callers that
    expected a 3-tuple."""
    _ = (prefix, user)
    detail_db, treemap_db = _resolve_db_paths(input_dir)
    if not detail_db:
        return ("", "", "")
    return (detail_db, treemap_db, "")


# ---------------------------------------------------------------------------
# Export core
# ---------------------------------------------------------------------------

def build_jobs(users: list, input_dir: str, output_dir: str, prefix: str) -> list:
    """Build (user, detail_db, treemap_db, output_dir, prefix) jobs for the
    Rust pool. Skips users for which no DB exists."""
    detail_db, treemap_db = _resolve_db_paths(input_dir)
    if not detail_db:
        return []
    return [
        (user, detail_db, treemap_db, output_dir, prefix)
        for user in users
    ]


def export_user(user: str,
                input_dir: str,
                output_dir: str,
                prefix: str,
                sem=None) -> list:
    """Backward-compatible single-user export. ``sem`` is accepted but ignored
    — concurrency is handled by the Rust ``process_jobs`` thread pool."""
    _ = sem
    detail_db, treemap_db = _resolve_db_paths(input_dir)
    if not detail_db:
        print(f"  [skip] No detail.db for user: {user}", file=sys.stderr)
        return []
    try:
        return export_rust.process(user, detail_db, treemap_db, output_dir, prefix)
    except Exception as exc:
        raise RuntimeError(f"Rust export failed for {user}: {exc}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _get_rss_mb() -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0
    except Exception:
        pass
    return 0.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export per-user disk usage detail reports as plain text "
                    "by calling the Rust core (export_rust) over SQLite.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input-dir", default=".",
                   help="Directory containing detail_users/data_detail.db (default: .)")
    p.add_argument("--output-dir", default=None,
                   help="Directory to write .txt files to (default: same as --input-dir)")
    p.add_argument("--prefix", default="",
                   help="Filename prefix (e.g. disk id)")
    p.add_argument("--users", nargs="+", metavar="USER",
                   help="Only export specific user(s). Default: all users in DB.")
    p.add_argument("--workers", type=int, default=4,
                   help="Number of parallel worker threads (default: 4).")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_dir = os.path.abspath(args.input_dir)
    output_dir = os.path.abspath(args.output_dir) if args.output_dir else input_dir
    prefix = args.prefix
    workers = max(1, args.workers)

    users = args.users if args.users else find_users(input_dir, prefix)
    if not users:
        print(f"No user detail database found in: {input_dir}", file=sys.stderr)
        sys.exit(1)

    start_ts = time.time()
    rss_start = _get_rss_mb()
    print(f"Exporting {len(users)} user(s) using Rust Core "
          f"[parallel {workers}w] -> {output_dir}")
    print(f"Memory (start RSS): {rss_start:.1f} MB")

    try:
        if os.path.exists(output_dir) and not os.path.isdir(output_dir):
            raise NotADirectoryError(
                f"--output-dir must be a directory, got file path: {output_dir}"
            )
        os.makedirs(output_dir, exist_ok=True)
    except Exception as exc:
        print(f"[error] Invalid output directory: {exc}", file=sys.stderr)
        sys.exit(2)

    jobs = build_jobs(users, input_dir, output_dir, prefix)
    if not jobs:
        print("No valid export jobs found.", file=sys.stderr)
        sys.exit(1)

    try:
        written_paths = export_rust.process_jobs(jobs, workers)
    except Exception as exc:
        print(f"[error] Rust batch export failed: {exc}", file=sys.stderr)
        sys.exit(3)

    rss_now = _get_rss_mb()
    print(
        f"Done. {len(jobs)}/{len(jobs)} user job(s) processed, "
        f"{len(written_paths)} TXT file(s) written."
    )
    print(f"Memory (end RSS): {rss_now:.1f} MB")
    print(f"Elapsed: {time.time() - start_ts:.2f}s")


if __name__ == "__main__":
    main()
