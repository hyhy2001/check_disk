"""
Scan status writer + heartbeat.

Writes scan_status.json so the dashboard can show progress while a long
scan is running. Optionally enqueues the status file with an
AsyncSyncPipeline so the dashboard host receives heartbeats too.

Extracted from disk_checker.py to keep the entry point focused on
argument parsing and orchestration.
"""

import json
import os
import socket
import threading
import time
from typing import Optional

from src.constants import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_HEARTBEAT_SYNC_INTERVAL,
    SCAN_STATUS_FILENAME,
)


def write_scan_status(
    out_dir: str,
    stage: str,
    running: bool,
    message: str = "",
    error: str = "",
    started_at: float = 0,
    phase_started_at: float = 0,
    tree_map_enabled: bool = False,
    sync_enabled: bool = False,
) -> Optional[str]:
    """Atomically write scan_status.json and return its path on success."""
    try:
        now = int(time.time())
        started = int(started_at) if started_at else now
        phase_started = int(phase_started_at) if phase_started_at else started
        status_path = os.path.join(out_dir, SCAN_STATUS_FILENAME)
        tmp_path = status_path + ".tmp"
        payload = {
            "running": running,
            "stage": stage,
            "started_at": started,
            "phase_started_at": phase_started,
            "phase_elapsed_sec": max(0, now - phase_started),
            "total_elapsed_sec": max(0, now - started),
            "updated_at": now,
            "finished_at": now if not running else 0,
            "pid": os.getpid(),
            "host": socket.gethostname(),
            "message": message,
            "error": error,
            "tree_map_enabled": tree_map_enabled,
            "sync_enabled": sync_enabled,
        }
        with open(tmp_path, "w") as f:
            json.dump(payload, f)
        os.replace(tmp_path, status_path)
        return status_path
    except Exception:
        return None


def update_status(
    pipeline,
    out_dir: str,
    stage: str,
    running: bool,
    message: str = "",
    error: str = "",
    started_at: float = 0,
    phase_started_at: float = 0,
    tree_map_enabled: bool = False,
    sync_enabled: bool = False,
) -> None:
    """Write status and, if a sync pipeline is given, enqueue it."""
    status_path = write_scan_status(
        out_dir,
        stage,
        running,
        message,
        error,
        started_at,
        phase_started_at,
        tree_map_enabled,
        sync_enabled,
    )
    if status_path and pipeline:
        try:
            pipeline.enqueue_file(status_path)
        except Exception:
            pass


class ScanStatusHeartbeat:
    """Background thread that touches scan_status.json so updated_at moves
    while long phases are running. Caller controls the active phase via
    set_phase(). Calling stop() halts the thread."""

    def __init__(
        self,
        out_dir: str,
        started_at: float,
        sync_pipeline=None,
        interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        sync_interval: float = DEFAULT_HEARTBEAT_SYNC_INTERVAL,
    ):
        self.out_dir = out_dir
        self.started_at = started_at
        self.sync_pipeline = sync_pipeline
        self.tree_map_enabled = False
        self.sync_enabled = sync_pipeline is not None
        self.interval = interval
        self.sync_interval = max(interval, sync_interval)
        self._last_sync = time.monotonic()
        self._stop_evt = threading.Event()
        self._lock = threading.Lock()
        self._stage = "scan"
        self._message = ""
        self._phase_started_at = started_at
        self._thread = threading.Thread(
            target=self._run, name="scan-status-heartbeat", daemon=True
        )

    def set_phase(self, stage: str, message: str = "") -> None:
        with self._lock:
            self._stage = stage
            self._message = message
            self._phase_started_at = time.time()
        update_status(
            self.sync_pipeline,
            self.out_dir,
            stage,
            True,
            message,
            started_at=self.started_at,
            phase_started_at=self._phase_started_at,
            tree_map_enabled=self.tree_map_enabled,
            sync_enabled=self.sync_enabled,
        )

    def _run(self) -> None:
        while not self._stop_evt.wait(self.interval):
            with self._lock:
                stage = self._stage
                message = self._message
                phase_started_at = self._phase_started_at
            status_path = write_scan_status(
                self.out_dir,
                stage,
                True,
                message,
                started_at=self.started_at,
                phase_started_at=phase_started_at,
                tree_map_enabled=self.tree_map_enabled,
                sync_enabled=self.sync_enabled,
            )
            now = time.monotonic()
            if (
                status_path
                and self.sync_pipeline
                and now - self._last_sync >= self.sync_interval
            ):
                try:
                    self.sync_pipeline.enqueue_file(status_path)
                    self._last_sync = now
                except Exception:
                    pass

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_evt.set()
        try:
            self._thread.join(timeout=2)
        except Exception:
            pass
