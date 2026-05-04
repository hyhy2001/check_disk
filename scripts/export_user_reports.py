"""
export_user_reports.py

Reads per-user detail JSON/NDJSON reports and exports plain-text usage files per user.
This script uses a high-performance Rust core `export_rust` to parse reports and dump TXT.

Output contract note:
- Prefer canonical keys in source reports for best compatibility with disk_usage backend:
  - dirs rows: path, used
  - files rows: path, size, xt
- Legacy short keys (n/s) are still supported by parser-side normalization.
"""

import argparse
import json
import os
import sys
import time

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
    """Return the directory that contains detail_users manifest outputs."""
    _ = prefix
    sub = os.path.join(input_dir, "detail_users")
    if os.path.exists(os.path.join(sub, "manifest.json")):
        return sub
    if os.path.exists(os.path.join(input_dir, "manifest.json")):
        return input_dir
    return sub


def _find_detail_manifest(detail_dir: str) -> str:
    path = os.path.join(detail_dir, "manifest.json")
    return path if os.path.exists(path) else ""


def _users_from_detail_manifest(manifest_path: str) -> list:
    if not manifest_path:
        return []
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        users = sorted(
            str(u.get("username"))
            for u in manifest.get("users", [])
            if u.get("username") and not str(u.get("username")).startswith("uid-")
        )
        if users:
            return users

        api_users_index = os.path.join(os.path.dirname(manifest_path), "api", "users_index.min.json")
        if os.path.exists(api_users_index):
            with open(api_users_index, "r", encoding="utf-8") as fh:
                rows = json.load(fh)
            return sorted(
                str(r.get("username"))
                for r in rows
                if isinstance(r, dict)
                and r.get("username")
                and not str(r.get("username")).startswith("uid-")
            )
    except Exception:
        return []

    return []



def find_users(input_dir: str, prefix: str) -> list:
    """Return sorted list of usernames discovered from detail manifest."""
    detail_dir = _resolve_detail_dir(input_dir, prefix)
    return _users_from_detail_manifest(_find_detail_manifest(detail_dir))





def _get_rss_mb() -> float:
    """Best-effort current process RSS in MB (Linux)."""
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = float(line.split()[1])
                    return kb / 1024.0
    except Exception:
        pass
    return 0.0


def build_paths(input_dir: str, prefix: str, user: str) -> tuple:
    """Return (unified_path, dir_path, file_path) for manifest-based detail exports."""
    _ = user
    detail_dir = _resolve_detail_dir(input_dir, prefix)
    detail_manifest = _find_detail_manifest(detail_dir)
    if detail_manifest:
        return (detail_manifest, "", "")
    return ("", "", "")



# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def build_jobs(users: list, input_dir: str, output_dir: str, prefix: str) -> list:
    """Build Rust batch jobs: (user, unified, dir, file, output_dir, prefix)."""
    jobs = []
    for user in users:
        unified_path, dir_path, file_path = build_paths(input_dir, prefix, user)
        if not any([os.path.exists(unified_path), os.path.exists(dir_path), os.path.exists(file_path)]):
            print(f"  [skip] No data found for user: {user}", file=sys.stderr)
            continue
        jobs.append((user, unified_path, dir_path, file_path, output_dir, prefix))
    return jobs

def export_user(user: str,
                input_dir: str,
                output_dir: str,
                prefix: str,
                sem=None) -> list:
    """Backward-compatible single-user export wrapper over Rust batch jobs."""
    _ = sem
    unified_path, dir_path, file_path = build_paths(input_dir, prefix, user)
    if not any([os.path.exists(unified_path), os.path.exists(dir_path), os.path.exists(file_path)]):
        print(f"  [skip] No data found for user: {user}", file=sys.stderr)
        return []
    try:
        return export_rust.process_jobs([(user, unified_path, dir_path, file_path, output_dir, prefix)], 1)
    except Exception as exc:
        raise RuntimeError(f"Rust export failed for {user}: {exc}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export per-user disk usage detail reports as plain text using Rust core.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input-dir",   default=".",
                   help="Directory containing detail_users/manifest.json outputs (default: .)")
    p.add_argument("--output-dir",  default=None,
                   help="Directory to write .txt files to (default: same as --input-dir)")
    p.add_argument("--prefix",      default="",
                   help="Filename prefix used when scanning (e.g. 'sda1')")
    p.add_argument("--users",       nargs="+", metavar="USER",
                   help="Only export specific user(s). Default: all discovered users.")
    p.add_argument("--workers",     type=int, default=4,
                   help="Number of parallel worker threads (default: 4).")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    input_dir   = os.path.abspath(args.input_dir)
    output_dir  = os.path.abspath(args.output_dir) if args.output_dir else input_dir
    prefix      = args.prefix
    workers     = max(1, args.workers)

    users = args.users if args.users else find_users(input_dir, prefix)

    if not users:
        print(f"No user detail reports found in: {input_dir}", file=sys.stderr)
        sys.exit(1)

    start_ts = time.time()
    rss_start = _get_rss_mb()
    peak_rss = rss_start
    print(f"Exporting {len(users)} user(s) using Rust Core [parallel {workers}w] -> {output_dir}")
    print(f"Workers selected: --workers={workers}")
    print("Tip: change concurrency with --workers N")
    print(f"Memory (start RSS): {rss_start:.1f} MB")
    try:
        if os.path.exists(output_dir) and not os.path.isdir(output_dir):
            raise NotADirectoryError(f"--output-dir must be a directory, got file path: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
    except Exception as exc:
        print(f"[error] Invalid output directory: {exc}", file=sys.stderr)
        sys.exit(2)

    jobs = build_jobs(users, input_dir, output_dir, prefix)
    total = len(jobs)
    if not jobs:
        print("No valid export jobs found.", file=sys.stderr)
        sys.exit(1)

    try:
        written_paths = export_rust.process_jobs(jobs, workers)
    except Exception as exc:
        print(f"[error] Rust batch export failed: {exc}", file=sys.stderr)
        sys.exit(3)

    rss_now = _get_rss_mb()
    peak_rss = max(peak_rss, rss_now)
    print(
        f"\rProgress: {total}/{total} users processed | RSS: {rss_now:.1f} MB | Peak: {peak_rss:.1f} MB",
        end="",
        flush=True,
    )

    print("")

    written_files = len(written_paths)
    print(f"Done. {total}/{total} user job(s) exported, {written_files} TXT file(s) written.")
    print(f"Memory (peak RSS): {peak_rss:.1f} MB")
    print(f"Elapsed: {time.time() - start_ts:.2f}s")


if __name__ == "__main__":
    main()
