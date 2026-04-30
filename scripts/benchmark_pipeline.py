#!/usr/bin/env python3
"""Benchmark check_disk pipeline with multiple worker counts.

Runs the CLI command N times per worker value, parses debug output, and prints
median timings/rates. Default is intentionally safe: writes to /tmp.
"""

from __future__ import annotations

import argparse
import re
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "disk_checker.py"

PATTERNS = {
    "phase1_wall_s": re.compile(r"Wall time:\s+([0-9.]+)s"),
    "phase2_elapsed_s": re.compile(r"\[Phase 2\] RAM end: .* elapsed: ([0-9.]+)s"),
    "total_elapsed_s": re.compile(r"Total pipeline elapsed \(wall-clock\): ([0-9.]+)s"),
    "scan_rate": re.compile(r"Scan rate:\s+([0-9,]+) files/sec"),
    "total_files": re.compile(r"Total files:\s+([0-9,]+)"),
    "peak_rss_mb": re.compile(r"Peak RSS:\s+([0-9.]+) MB"),
}


def parse_metrics(output: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for key, pattern in PATTERNS.items():
        match = pattern.search(output)
        if not match:
            continue
        raw = match.group(1).replace(",", "")
        metrics[key] = float(raw)
    return metrics


def run_once(scan_dir: str, output_dir: Path, workers: int, level: int) -> dict[str, float]:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(CLI),
        "--run",
        "--dir",
        scan_dir,
        "--debug",
        "--output-dir",
        str(output_dir),
        "--tree-map",
        "--level",
        str(level),
        "--workers",
        str(workers),
    ]
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    combined = proc.stdout + "\n" + proc.stderr
    if proc.returncode != 0:
        print(combined, file=sys.stderr)
        raise RuntimeError(f"benchmark run failed with code {proc.returncode}")
    metrics = parse_metrics(combined)
    metrics["return_code"] = float(proc.returncode)
    return metrics


def median(values: list[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark check_disk pipeline")
    parser.add_argument("--dir", default="/www", help="Directory to scan (default: /www)")
    parser.add_argument("--output-root", default="/tmp/check_disk_bench", help="Benchmark output root")
    parser.add_argument("--workers", default="4,8,12", help="Comma-separated worker counts")
    parser.add_argument("--runs", type=int, default=3, help="Runs per worker count")
    parser.add_argument("--level", type=int, default=20, help="TreeMap level")
    args = parser.parse_args()

    workers = [int(x.strip()) for x in args.workers.split(",") if x.strip()]
    output_root = Path(args.output_root)

    print(f"Benchmark pipeline: dir={args.dir}, workers={workers}, runs={args.runs}, level={args.level}")
    print("worker,phase1_wall_s,phase2_elapsed_s,total_elapsed_s,scan_rate,total_files,peak_rss_mb")

    for worker in workers:
        samples: list[dict[str, float]] = []
        for run_idx in range(args.runs):
            out_dir = output_root / f"w{worker}_r{run_idx + 1}"
            metrics = run_once(args.dir, out_dir, worker, args.level)
            samples.append(metrics)
            print(
                f"sample w={worker} r={run_idx + 1}: "
                f"phase1={metrics.get('phase1_wall_s', 0):.2f}s "
                f"phase2={metrics.get('phase2_elapsed_s', 0):.2f}s "
                f"total={metrics.get('total_elapsed_s', 0):.2f}s "
                f"rate={metrics.get('scan_rate', 0):.0f}"
            )

        row = {
            key: median([sample[key] for sample in samples if key in sample])
            for key in PATTERNS
        }
        print(
            f"median,{worker},"
            f"{row.get('phase1_wall_s', 0):.2f},"
            f"{row.get('phase2_elapsed_s', 0):.2f},"
            f"{row.get('total_elapsed_s', 0):.2f},"
            f"{row.get('scan_rate', 0):.0f},"
            f"{row.get('total_files', 0):.0f},"
            f"{row.get('peak_rss_mb', 0):.1f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
