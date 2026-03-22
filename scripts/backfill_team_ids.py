#!/usr/bin/env python3
"""
backfill_team_ids.py
--------------------
Reads disk_checker_config.json and one or more disk_usage_report JSON files,
then injects team_id into team_usage / user_usage entries.

Single file (overwrite in-place or to --output):
    python3 scripts/backfill_team_ids.py --report disk_usage_report.json
    python3 scripts/backfill_team_ids.py --report report.json --output patched.json
    python3 scripts/backfill_team_ids.py --report report.json --dry-run

Multiple files via glob (all overwritten in-place):
    python3 scripts/backfill_team_ids.py --reports "reports/disk_usage_report_*.json"
    python3 scripts/backfill_team_ids.py --reports r1.json r2.json r3.json
    python3 scripts/backfill_team_ids.py --reports "*.json" --dry-run
"""

import argparse
import glob
import json
import os
import sys


# ── Compact JSON serialiser (same style as the rest of the project) ──────────

def _compact_json(obj: object, indent: int = 2) -> str:
    """
    Top-level dict keys are indented normally, but list items that are plain
    dicts (e.g. team_usage, user_usage) are written on a single line each.
    """
    lines = []
    _walk(obj, lines, level=0, indent=indent)
    return "\n".join(lines)


def _walk(node, lines: list, level: int, indent: int):
    inner = " " * ((level + 1) * indent)

    if isinstance(node, dict):
        if level > 0:
            lines.append(json.dumps(node, ensure_ascii=False))
            return
        lines.append("{")
        items = list(node.items())
        for i, (k, v) in enumerate(items):
            comma = "," if i < len(items) - 1 else ""
            if isinstance(v, list) and v and isinstance(v[0], dict):
                lines.append(f'{inner}{json.dumps(k)}: [')
                for j, item in enumerate(v):
                    item_comma = "," if j < len(v) - 1 else ""
                    lines.append(f'{inner}  {json.dumps(item, ensure_ascii=False)}{item_comma}')
                lines.append(f'{inner}]{comma}')
            elif isinstance(v, dict):
                lines.append(f'{inner}{json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}{comma}')
            else:
                lines.append(f'{inner}{json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}{comma}')
        lines.append("}")
    else:
        lines.append(json.dumps(node, ensure_ascii=False))


# ── Core mapping logic ────────────────────────────────────────────────────────

def build_maps(config: dict):
    """Return (team_name→team_id, username→team_id) from config."""
    team_id_map = {
        t["name"]: t["team_ID"]
        for t in config.get("teams", [])
        if "name" in t and "team_ID" in t
    }
    user_team_id_map = {
        u["name"]: u["team_ID"]
        for u in config.get("users", [])
        if "name" in u and "team_ID" in u
    }
    return team_id_map, user_team_id_map


def inject_team_ids(report: dict, team_id_map: dict, user_team_id_map: dict):
    """
    Inject team_id into team_usage and user_usage entries.
    Returns (patched_report, number_of_fields_added).
    """
    added = 0
    for entry in report.get("team_usage", []):
        if "team_id" not in entry and entry.get("name") in team_id_map:
            entry["team_id"] = team_id_map[entry["name"]]
            added += 1
    for entry in report.get("user_usage", []):
        if "team_id" not in entry and entry.get("name") in user_team_id_map:
            entry["team_id"] = user_team_id_map[entry["name"]]
            added += 1
    return report, added


# ── Helpers ───────────────────────────────────────────────────────────────────

def resolve_default_config(report_path: str) -> str:
    """Walk up from report dir to find disk_checker_config.json."""
    candidates = [
        os.path.join(os.path.dirname(report_path), "disk_checker_config.json"),
        os.path.join(os.getcwd(), "disk_checker_config.json"),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return ""


def expand_patterns(patterns: list) -> list:
    """Expand glob patterns, deduplicate, sort."""
    paths = []
    for p in patterns:
        expanded = glob.glob(p)
        if not expanded:
            print(f"WARNING: no files matched pattern '{p}'", file=sys.stderr)
        paths.extend(expanded)
    seen = set()
    result = []
    for p in paths:
        abs_p = os.path.abspath(p)
        if abs_p not in seen:
            seen.add(abs_p)
            result.append(abs_p)
    return sorted(result)


def process_one(report_path: str, config: dict, out_path: str, dry_run: bool):
    """Patch a single report file. Returns (added_count, error_msg)."""
    try:
        with open(report_path) as f:
            report = json.load(f)
    except Exception as e:
        return 0, str(e)

    team_id_map, user_team_id_map = build_maps(config)
    report, added = inject_team_ids(report, team_id_map, user_team_id_map)
    patched = _compact_json(report)

    if dry_run:
        print(patched)
        return added, None

    try:
        with open(out_path, "w") as f:
            f.write(patched + "\n")
    except Exception as e:
        return added, str(e)

    return added, None


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Backfill team_id into disk_usage_report JSON file(s)")
    ap.add_argument("--config",  default="", help="Path to disk_checker_config.json (auto-detected if omitted)")
    # Single-file mode
    ap.add_argument("--report",  default="", help="Single report file to patch")
    ap.add_argument("--output",  default="", help="Output path for single-file mode (default: overwrite in-place)")
    # Batch mode
    ap.add_argument("--reports", nargs="+", default=[], metavar="PATTERN",
                    help="One or more report files / glob patterns (e.g. 'reports/*.json')")
    ap.add_argument("--dry-run", action="store_true", help="Print patched JSON without writing")
    args = ap.parse_args()

    # Validate mode
    batch_mode = bool(args.reports)
    single_mode = bool(args.report)

    if not batch_mode and not single_mode:
        # Default: single mode with fallback filename
        args.report = "disk_usage_report.json"
        single_mode = True

    if batch_mode and args.output:
        ap.error("--output cannot be used with --reports (batch mode overwrites each file in-place)")

    # Resolve config (use first report for auto-detection)
    anchor = args.reports[0] if batch_mode else args.report
    config_path = args.config or resolve_default_config(os.path.abspath(anchor))
    if not config_path or not os.path.isfile(config_path):
        print(f"ERROR: config not found. Tried: {config_path or '(auto-detect failed)'}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    # ── Single file ───────────────────────────────────────────────────────────
    if single_mode:
        report_path = os.path.abspath(args.report)
        if not os.path.isfile(report_path):
            print(f"ERROR: report not found: {report_path}", file=sys.stderr)
            sys.exit(1)
        out_path = os.path.abspath(args.output) if args.output else report_path
        added, err = process_one(report_path, config, out_path, args.dry_run)
        if err:
            print(f"ERROR: {err}", file=sys.stderr)
            sys.exit(1)
        if not args.dry_run:
            print(f"✅  {added} team_id field(s) added  →  {out_path}")
        else:
            print(f"\n[dry-run] {added} team_id field(s) would be added.", file=sys.stderr)
        return

    # ── Batch mode ────────────────────────────────────────────────────────────
    files = expand_patterns(args.reports)
    if not files:
        print("ERROR: no matching files found.", file=sys.stderr)
        sys.exit(1)

    total_files  = len(files)
    total_added  = 0
    total_errors = 0
    col = max(len(os.path.basename(p)) for p in files)

    print(f"Backfilling {total_files} file(s) using {os.path.basename(config_path)} …\n")
    for path in files:
        added, err = process_one(path, config, path, args.dry_run)
        name = os.path.basename(path)
        if err:
            print(f"  ✗  {name:<{col}}  ERROR: {err}")
            total_errors += 1
        else:
            verb = "would add" if args.dry_run else "added"
            print(f"  ✓  {name:<{col}}  {added:>3} field(s) {verb}")
            total_added += added

    print(f"\n{'[dry-run] ' if args.dry_run else ''}Done: {total_files} file(s), "
          f"{total_added} team_id field(s) added"
          + (f", {total_errors} error(s)" if total_errors else ""))
    if total_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

