#!/usr/bin/env python3
"""
backfill_team_ids.py
--------------------
Reads disk_checker_config.json + disk_usage_report.json and injects
team_id into team_usage / user_usage entries. Overwrites the report
in-place (or writes to --output if given).

Usage:
    python3 scripts/backfill_team_ids.py
    python3 scripts/backfill_team_ids.py --config path/to/config.json --report path/to/report.json
    python3 scripts/backfill_team_ids.py --report path/to/report.json --output patched.json
"""

import argparse
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
    pad = " " * (level * indent)
    inner = " " * ((level + 1) * indent)

    if isinstance(node, dict):
        if level > 0:
            # Inline flat dicts below the top level
            lines.append(json.dumps(node, ensure_ascii=False))
            return
        lines.append("{")
        items = list(node.items())
        for i, (k, v) in enumerate(items):
            comma = "," if i < len(items) - 1 else ""
            if isinstance(v, list) and v and isinstance(v[0], dict):
                # List of flat dicts — each item on one line
                lines.append(f'{inner}{json.dumps(k)}: [')
                for j, item in enumerate(v):
                    item_comma = "," if j < len(v) - 1 else ""
                    lines.append(f'{inner}  {json.dumps(item, ensure_ascii=False)}{item_comma}')
                lines.append(f'{inner}]{comma}')
            elif isinstance(v, dict):
                # Inline nested dict
                lines.append(f'{inner}{json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}{comma}')
            else:
                lines.append(f'{inner}{json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}{comma}')
        lines.append("}")
    else:
        lines.append(json.dumps(node, ensure_ascii=False))


# ── Core mapping logic ────────────────────────────────────────────────────────

def build_maps(config: dict):
    """Return (team_name→team_id, username→team_id) from config."""
    team_id_map: dict[str, int] = {
        t["name"]: t["team_ID"]
        for t in config.get("teams", [])
        if "name" in t and "team_ID" in t
    }
    user_team_id_map: dict[str, int] = {
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


# ── CLI ────────────────────────────────────────────────────────────────────────

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


def main():
    ap = argparse.ArgumentParser(description="Backfill team_id into disk_usage_report.json")
    ap.add_argument("--config",  default="", help="Path to disk_checker_config.json (auto-detected if omitted)")
    ap.add_argument("--report",  default="disk_usage_report.json", help="Path to disk_usage_report.json")
    ap.add_argument("--output",  default="", help="Output path (overwrites report in-place if omitted)")
    ap.add_argument("--dry-run", action="store_true", help="Print patched JSON without writing")
    args = ap.parse_args()

    # Resolve config path
    config_path = args.config or resolve_default_config(os.path.abspath(args.report))
    if not config_path or not os.path.isfile(config_path):
        print(f"ERROR: config not found. Tried: {config_path or '(auto-detect failed)'}", file=sys.stderr)
        sys.exit(1)

    # Resolve report path
    report_path = os.path.abspath(args.report)
    if not os.path.isfile(report_path):
        print(f"ERROR: report not found: {report_path}", file=sys.stderr)
        sys.exit(1)

    # Load
    with open(config_path) as f:
        config = json.load(f)
    with open(report_path) as f:
        report = json.load(f)

    team_id_map, user_team_id_map = build_maps(config)
    report, added = inject_team_ids(report, team_id_map, user_team_id_map)

    patched = _compact_json(report)

    if args.dry_run:
        print(patched)
        print(f"\n[dry-run] {added} team_id field(s) would be added.", file=sys.stderr)
        return

    out_path = os.path.abspath(args.output) if args.output else report_path
    with open(out_path, "w") as f:
        f.write(patched + "\n")

    print(f"✅  {added} team_id field(s) added → {out_path}")


if __name__ == "__main__":
    main()
