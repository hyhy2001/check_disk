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

import os
import sys
import glob
import argparse
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
    
    if glob.glob(os.path.join(sub, f"{pat_prefix}detail_report_*.ndjson")) or \
       glob.glob(os.path.join(sub, f"{pat_prefix}detail_report_*.json")):
        return sub
    if glob.glob(os.path.join(input_dir, f"{pat_prefix}detail_report_*.ndjson")) or \
       glob.glob(os.path.join(input_dir, f"{pat_prefix}detail_report_*.json")):
        return input_dir
    return sub


def find_users(input_dir: str, prefix: str) -> list:
    """Return sorted list of usernames discovered from detail_report_*.json files."""
    detail_dir = _resolve_detail_dir(input_dir, prefix)
    pat_prefix = f"{prefix}_" if prefix else ""
    
    # Try looking for the unified detail_report_<user>.ndjson or .json pattern
    users = set()
    for ext in [".ndjson", ".json"]:
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
                    
        if users:
            break

    return sorted(list(users))


def build_paths(input_dir: str, prefix: str, user: str) -> tuple:
    """Return (unified_path, dir_path, file_path) auto-detecting layout."""
    detail_dir = _resolve_detail_dir(input_dir, prefix)
    pat_prefix = f"{prefix}_" if prefix else ""
    
    # Determine extension
    ndjson_file = os.path.join(detail_dir, f"{pat_prefix}detail_report_file_{user}.ndjson")
    ext = ".ndjson" if os.path.exists(ndjson_file) else ".json"
    
    unified_path = os.path.join(detail_dir, f"{pat_prefix}detail_report_{user}{ext}")
    dir_path = os.path.join(detail_dir, f"{pat_prefix}detail_report_dir_{user}{ext}")
    file_path = os.path.join(detail_dir, f"{pat_prefix}detail_report_file_{user}{ext}")
    
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
    Load JSON reports for user and write plain-text report using Rust.
    Outputs TWO separate files: one for directories (dir) and one for files (file).
    """
    unified_path, dir_path, file_path = build_paths(input_dir, prefix, user)
    
    # Check what exists
    has_unified = os.path.exists(unified_path)
    has_dir = os.path.exists(dir_path)
    has_file = os.path.exists(file_path)

    if not has_unified and not has_dir and not has_file:
        print(f"  [skip] No data found for user: {user}", file=sys.stderr)
        return []

    results = []
    base_parts = [p for p in [prefix, "usage"] if p]

    with sem:
        try:
            if has_unified:
                # If we only have a unified file, we might still want to split by type?
                # But typically unified means one file. Let's produce one dir and one file report from it.
                out_dir_fname = "_".join(base_parts + ["dir", user]) + ".txt"
                out_dir_path = os.path.join(output_dir, out_dir_fname)
                export_rust.process(user, unified_path, "", out_dir_path)
                results.append(out_dir_path)

                out_file_fname = "_".join(base_parts + ["file", user]) + ".txt"
                out_file_path = os.path.join(output_dir, out_file_fname)
                export_rust.process(user, "", unified_path, out_file_path)
                results.append(out_file_path)
            else:
                if has_dir:
                    out_dir_fname = "_".join(base_parts + ["dir", user]) + ".txt"
                    out_dir_path = os.path.join(output_dir, out_dir_fname)
                    export_rust.process(user, dir_path, "", out_dir_path)
                    results.append(out_dir_path)
                
                if has_file:
                    out_file_fname = "_".join(base_parts + ["file", user]) + ".txt"
                    out_file_path = os.path.join(output_dir, out_file_fname)
                    export_rust.process(user, "", file_path, out_file_path)
                    results.append(out_file_path)
                    
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
                   help="Directory containing detail_report_dir/file JSON files (default: .)")
    p.add_argument("--output-dir",  default=None,
                   help="Directory to write .txt files to (default: same as --input-dir)")
    p.add_argument("--prefix",      default="",
                   help="Filename prefix used when scanning (e.g. 'sda1')")
    p.add_argument("--users",       nargs="+", metavar="USER",
                   help="Only export specific user(s). Default: all discovered users.")
    p.add_argument("--workers",     type=int, default=4,
                   help="Number of parallel worker threads (default: 4).")
    p.add_argument("--max-readers", type=int, default=4,
                   help="Max workers that may read JSON files simultaneously (default: 4).")
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
                    # In realtime ngay khi xử lý xong (flush=True để hiển thị ngay)
                    print(f"  [ok] {user:20s}  {out}", flush=True)
                    results.append((user, out))
            except Exception as exc:
                failed.append(user)
                print(f"  [warn] {user}: {exc}", file=sys.stderr, flush=True)

    if failed:
        print(f"\n  [warn] {len(failed)} export(s) failed: {failed}", file=sys.stderr)

    print(f"Done. {len(results)}/{len(users)} file(s) written.")


if __name__ == "__main__":
    main()
