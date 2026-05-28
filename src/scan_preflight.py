"""
scan_preflight.py — Pre-scan directory skip detection.

Detects paths that should be excluded before the Rust scanner starts:
  - Container overlay/snapshot dirs (containerd, docker, lxc, lxd)
  - NFS .snapshot dirs (NetApp, etc.)
  - Bind mount destinations (same inode/device as another mount)
"""

import os
import subprocess
from typing import List


# Container overlay paths that duplicate host data
_CONTAINER_SKIP_PREFIXES: List[str] = [
    "/var/lib/containerd/io.containerd.snapshotter",
    "/var/lib/docker/overlay2",
    "/var/lib/docker/aufs",
    "/var/lib/lxc",
    "/var/lib/lxd/storage-pools",
]


def detect_skip_dirs(scan_root: str) -> List[str]:
    """Return a list of directory paths that should be skipped during scan.

    Combines container overlay detection, NFS snapshot detection, and
    bind mount detection. All returned paths are absolute.
    """
    skip: List[str] = []
    scan_root_abs = os.path.abspath(scan_root).rstrip("/") or "/"

    skip.extend(_detect_container_overlays(scan_root_abs))
    skip.extend(_detect_nfs_snapshots(scan_root_abs))
    skip.extend(_detect_bind_mounts(scan_root_abs))

    return skip


def _detect_container_overlays(scan_root_abs: str) -> List[str]:
    """Skip container overlay/snapshot dirs that duplicate host data."""
    return [
        p for p in _CONTAINER_SKIP_PREFIXES
        if p.startswith(scan_root_abs) or scan_root_abs == "/"
    ]


def _detect_nfs_snapshots(scan_root_abs: str) -> List[str]:
    """Find all .snapshot dirs up to depth 4 (NFS/NetApp snapshots)."""
    try:
        proc = subprocess.run(
            ["find", scan_root_abs, "-maxdepth", "4", "-type", "d", "-name", ".snapshot"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
        )
        return [p.strip() for p in proc.stdout.splitlines() if p.strip()]
    except (subprocess.TimeoutExpired, OSError):
        # Fallback: check root level only
        snapshot_path = os.path.join(scan_root_abs, ".snapshot")
        return [snapshot_path] if os.path.isdir(snapshot_path) else []


def _detect_bind_mounts(scan_root_abs: str) -> List[str]:
    """Detect bind mount destinations under scan_root by reading
    /proc/self/mountinfo. Returns paths that should be skipped to avoid
    double-counting (bind mounts share inodes with their source).
    """
    skip: List[str] = []
    try:
        seen = {}  # (dev, ino) -> first_path_seen
        with open("/proc/self/mountinfo") as f:
            mounts = []
            for line in f:
                parts = line.split()
                if len(parts) >= 5:
                    mounts.append(parts[4])

        for mp in mounts:
            try:
                st = os.stat(mp)
            except OSError:
                continue
            key = (st.st_dev, st.st_ino)
            if key in seen:
                first = seen[key]
                under_scan = []
                for p in (mp, first):
                    if p == scan_root_abs or p.startswith(scan_root_abs + "/"):
                        under_scan.append(p)
                if len(under_scan) >= 2:
                    bind_dest = max(under_scan, key=len)
                    if bind_dest not in skip:
                        skip.append(bind_dest)
                elif len(under_scan) == 1:
                    bind_dest = under_scan[0]
                    if bind_dest not in skip:
                        skip.append(bind_dest)
            else:
                seen[key] = mp
    except (IOError, OSError):
        pass
    return skip
