"""
TreeMap Generator Module - Ultra High Performance (Final Optimization)

Optimized for 6M+ directories:
- Integer-based hierarchy building (Massive RAM saving)
- Size-based pruning (Reduces DB noise and size)
- Binary Hash IDs for SQLite (Faster indexing)
- Multi-threaded Producer-Consumer architecture
- Zero os.stat() calls: owner resolved from Phase 1 dir_sizes_map
- O(1) depth from simple path separator count (no os.path.relpath)
"""

import os
import json
import zlib
import sqlite3
import time
import queue
import threading
import pwd
from typing import Dict, Any, List, Set, Tuple, Optional
from collections import defaultdict

try:
    from src import fast_scanner as _fast_scanner
except ImportError:
    _fast_scanner = None  # type: ignore

# Full Rust pipeline: hierarchy build + parallel JSON/compress + SQLite write.
# Requires generate_treemap_sqlite (added with flate2 + rayon).
HAS_RUST_TREEMAP_FULL = (
    _fast_scanner is not None
    and hasattr(_fast_scanner, 'generate_treemap_sqlite')
)

# Partial Rust pipeline: Python hierarchy + Rust parallel compress + SQLite.
# Fallback when generate_treemap_sqlite is unavailable.
HAS_RUST_TREEMAP_SQLITE = (
    not HAS_RUST_TREEMAP_FULL
    and _fast_scanner is not None
    and hasattr(_fast_scanner, 'compress_and_write_shards')
)

class TreeMapGenerator:
    """The ultimate TreeMap generator for massive scale filesystems."""

    def __init__(self, root_dir: str, dir_sizes: Dict[str, Dict[str, int]],
                 max_level: int = 5, max_workers: int = 4, min_size_mb: float = 0.0,
                 dir_owner_map: Optional[Dict[str, str]] = None):
        self.root_dir = os.path.abspath(root_dir)
        self.dir_sizes = dir_sizes
        self.max_level = max_level
        self.max_workers = max(1, int(max_workers))
        self.min_size_bytes = min_size_mb * 1024 * 1024

        # Queues for parallel processing — increased sizes to reduce blocking
        self.path_queue = queue.Queue(maxsize=20000)
        self.db_queue: queue.Queue = queue.Queue(maxsize=8000)

        # Internal String Table to save RAM
        self.id_to_path: List[str] = []
        self.path_to_id: Dict[str, int] = {}
        self.total_processed = 0

        # Owner resolution — prefer pre-computed map from Phase 1 (no os.stat syscalls)
        # dir_owner_map: path -> username, populated from dir_sizes_map by caller
        self._dir_owner_map: Dict[str, str] = dir_owner_map or {}
        # Fallback UID cache for paths not in dir_owner_map
        self._uid_to_owner: Dict[int, str] = {}
        self._stat_owner_cache: Dict[str, str] = {}
        self._owner_cache_cap = 50_000  # reduced — most paths served from dir_owner_map

        # Depth cache — avoid repeated string operations
        self._depth_cache: Dict[int, int] = {}  # path_id -> depth
        # Pre-compute root separator count once
        self._root_sep_count: int = self.root_dir.count(os.sep)

    def _get_id(self, path: str) -> int:
        """String interning: Returns an integer ID for a path string."""
        if path in self.path_to_id:
            return self.path_to_id[path]
        idx = len(self.id_to_path)
        self.id_to_path.append(path)
        self.path_to_id[path] = idx
        return idx

    def _get_depth(self, path: str) -> int:
        """O(1) depth via separator count — no os.path.relpath() call."""
        if path == self.root_dir:
            return 0
        # Simple separator subtraction; handles absolute paths only (which all our paths are)
        return path.count(os.sep) - self._root_sep_count

    def _get_depth_by_id(self, path_id: int) -> int:
        """Cached depth lookup by path ID."""
        cached = self._depth_cache.get(path_id)
        if cached is not None:
            return cached
        depth = self._get_depth(self.id_to_path[path_id])
        self._depth_cache[path_id] = depth
        return depth

    def _get_owner_username(self, path: str) -> str:
        """Resolve owner username — prefers Phase 1 map, falls back to os.stat()."""
        # Fast path: already known from Phase 1
        owner = self._dir_owner_map.get(path)
        if owner is not None:
            return owner

        # Slow path: stat the filesystem (only for paths missing from Phase 1 map)
        cached = self._stat_owner_cache.get(path)
        if cached is not None:
            return cached

        owner = "unknown"
        try:
            uid = os.stat(path).st_uid
            owner = self._uid_to_owner.get(uid)
            if owner is None:
                try:
                    owner = pwd.getpwuid(uid).pw_name
                except KeyError:
                    owner = f"uid-{uid}"
                self._uid_to_owner[uid] = owner
        except (OSError, PermissionError):
            owner = "unknown"

        if len(self._stat_owner_cache) >= self._owner_cache_cap:
            self._stat_owner_cache.clear()
        self._stat_owner_cache[path] = owner
        return owner

    def _serialization_worker(self, parent_to_children_ids, recursive_sizes, direct_sizes_ids):
        """Producer: Processes directory IDs and serializes them."""
        while True:
            path_id = self.path_queue.get()
            if path_id is None:
                self.path_queue.task_done()
                break

            path = self.id_to_path[path_id]
            children_nodes = []

            # Subdirectories (using IDs for speed)
            for child_id in parent_to_children_ids.get(path_id, []):
                child_path = self.id_to_path[child_id]
                child_size = recursive_sizes.get(child_id, 0)

                # Pruning by size (only active if min_size_mb > 0)
                child_depth = self._get_depth_by_id(child_id)
                if self.min_size_bytes > 0 and child_size < self.min_size_bytes and child_depth > 1:
                    continue

                children_nodes.append({
                    "name": os.path.basename(child_path) or child_path,
                    "path": child_path,
                    "value": child_size,
                    "type": "directory",
                    "owner": self._get_owner_username(child_path),
                    "shard_id": str(child_id),
                    "has_children": child_id in parent_to_children_ids and child_depth < self.max_level
                })

            # Direct files
            local_files_size = direct_sizes_ids.get(path_id, 0)
            if local_files_size > 0:
                children_nodes.append({
                    "name": f"[Files in {os.path.basename(path) or path}]",
                    "path": os.path.join(path, "__files__"),
                    "value": local_files_size,
                    "type": "file_group",
                    "owner": self._get_owner_username(path),
                    "has_children": False
                })

            if children_nodes:
                children_nodes.sort(key=lambda x: x["value"], reverse=True)
                shard_id = str(path_id)
                # Remove per-item 'path' — stored once as shard.path instead.
                items_compact = [
                    {k: v for k, v in node.items() if k != "path"}
                    for node in children_nodes
                ]
                # Consumer compresses (GIL released during zlib) so producers
                # can run their GIL-bound JSON work in parallel — natural pipeline.
                json_data = json.dumps(items_compact, separators=(',', ':'))
                self.db_queue.put((shard_id, path, json_data))

            self.path_queue.task_done()

    def _db_worker(self, db_path):
        """Consumer: Optimized SQLite writer with optional Rust fast-path."""
        if HAS_RUST_TREEMAP_SQLITE:
            self._db_worker_rust(db_path)
            return
        self._db_worker_python(db_path)

    def _db_worker_python(self, db_path: str) -> None:
        """Single-database Python sqlite3 writer.

        Consumer compresses each shard (GIL released during zlib.compress),
        which lets producer threads advance their GIL-bound JSON work in
        parallel — a natural pipeline that outperforms pre-compression in
        producers (where all 8 threads compete for GIL on dict/json work first).
        """
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA page_size = 8192")  # must be first DDL on fresh file
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA cache_size = -50000")
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE shards ("
            "id TEXT PRIMARY KEY, "
            "path TEXT NOT NULL, "
            "data BLOB NOT NULL)"
        )

        batch: list = []
        count = 0
        while True:
            item = self.db_queue.get()
            if item is None:
                if batch:
                    cursor.executemany("INSERT INTO shards VALUES (?, ?, ?)", batch)
                self.db_queue.task_done()
                break
            shard_id, shard_path, json_data = item
            compressed = sqlite3.Binary(zlib.compress(json_data.encode(), level=6))
            batch.append((shard_id, shard_path, compressed))
            count += 1
            if len(batch) >= 5000:
                cursor.executemany("INSERT INTO shards VALUES (?, ?, ?)", batch)
                batch = []
            self.db_queue.task_done()

        conn.commit()
        conn.close()
        self.total_processed = count

    def _db_worker_python_from_tsv(self, db_path, tsv_path):
        """Fallback writer: import shards from 3-column TSV (id TAB path TAB json)."""
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA page_size = 8192")
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA cache_size = -50000")
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE shards ("
            "id TEXT PRIMARY KEY, "
            "path TEXT NOT NULL, "
            "data BLOB NOT NULL)"
        )

        batch = []
        count = 0
        with open(tsv_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t", 2)
                if len(parts) != 3:
                    continue
                shard_id, shard_path, json_data = parts
                compressed = sqlite3.Binary(zlib.compress(json_data.encode(), level=6))
                batch.append((shard_id, shard_path, compressed))
                count += 1
                if len(batch) >= 5000:
                    cursor.executemany("INSERT INTO shards VALUES (?, ?, ?)", batch)
                    batch = []

        if batch:
            cursor.executemany("INSERT INTO shards VALUES (?, ?, ?)", batch)

        conn.commit()
        conn.close()
        self.total_processed = count

    def _db_worker_rust(self, db_path: str) -> None:
        """Collect all shards from db_queue, then hand off to Rust.

        - Python producers do: JSON serialization (GIL-bound) → queue.put
        - This consumer does: queue.get (lightweight) → accumulate list
        - Rust does: rayon parallel zlib compress + single-tx SQLite write
          (GIL entirely released; uses all CPU cores without Python overhead)
        """
        items: list = []
        while True:
            item = self.db_queue.get()
            if item is None:
                self.db_queue.task_done()
                break
            items.append(item)   # (shard_id, path, json_data)
            self.db_queue.task_done()

        try:
            count = _fast_scanner.compress_and_write_shards(items, db_path, 5000)
            self.total_processed = int(count)
        except Exception as e:
            print(f"  [warn] Rust compress_and_write_shards failed ({e}), falling back to Python...")
            self._write_items_python(db_path, items)

    def _write_items_python(self, db_path: str, items: list) -> None:
        """Pure-Python fallback: compress + write pre-collected shard items."""
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA page_size = 8192")
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA cache_size = -50000")
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE shards (id TEXT PRIMARY KEY, path TEXT NOT NULL, data BLOB NOT NULL)"
        )
        batch: list = []
        for shard_id, shard_path, json_data in items:
            compressed = sqlite3.Binary(zlib.compress(json_data.encode(), level=6))
            batch.append((shard_id, shard_path, compressed))
            if len(batch) >= 5000:
                cursor.executemany("INSERT INTO shards VALUES (?,?,?)", batch)
                batch = []
        if batch:
            cursor.executemany("INSERT INTO shards VALUES (?,?,?)", batch)
        conn.commit()
        conn.close()
        self.total_processed = len(items)

    def save(self, output_path: str):
        start_time = time.time()
        db_path = os.path.join(os.path.dirname(output_path), "tree_map_data.db")
        if os.path.exists(db_path):
            os.remove(db_path)

        # ─── Full Rust fast-path (hierarchy + JSON + compress + SQLite) ────────────
        if HAS_RUST_TREEMAP_FULL:
            print(f"Phase 3 [Rust]: Full pipeline for {len(self.dir_sizes):,} dirs...")
            count = _fast_scanner.generate_treemap_sqlite(
                self.root_dir,
                self.dir_sizes,
                self._dir_owner_map,
                output_path,          # json_path: tree_map_report.json
                db_path,              # db_path:   tree_map_data.db
                self.max_level,
                int(self.min_size_bytes),
            )
            elapsed = time.time() - start_time
            print(f"Completed: {count:,} shards (Rust full pipeline).")
            print(f"Total TreeMap time: {elapsed:.2f}s")
            return

        # ─── Python Phase 3.1 ──────────────────────────────────────────────────
        print(f"Phase 3.1: Building TreeMap hierarchy for {len(self.dir_sizes):,} directories...")

        # 1. Structure Analysis (Integer-based to save RAM)
        direct_sizes_ids: Dict[int, int] = {}
        all_dirs_ids: Set[int] = {self._get_id(self.root_dir)}

        for dpath, uids in self.dir_sizes.items():
            abs_path = os.path.abspath(dpath)
            depth = self._get_depth(abs_path)
            size = sum(uids.values())
            if size <= 0: continue

            target_path = abs_path
            if depth > self.max_level:
                # Clamp deep paths to max_level ancestor
                parts = abs_path[len(self.root_dir):].lstrip(os.sep).split(os.sep)
                target_path = os.path.join(self.root_dir, *parts[:self.max_level]) if parts else self.root_dir

            tid = self._get_id(target_path)
            direct_sizes_ids[tid] = direct_sizes_ids.get(tid, 0) + size

            # Trace ancestors using IDs
            curr = target_path
            while len(curr) >= len(self.root_dir):
                cid = self._get_id(curr)
                all_dirs_ids.add(cid)
                if curr == self.root_dir: break
                parent = os.path.dirname(curr)
                if parent == curr: break
                curr = parent

        parent_to_children_ids: Dict[int, List[int]] = defaultdict(list)
        for rid in all_dirs_ids:
            path = self.id_to_path[rid]
            if path == self.root_dir: continue
            parent_id = self._get_id(os.path.dirname(path))
            if parent_id in all_dirs_ids:
                parent_to_children_ids[parent_id].append(rid)

        # Pre-populate depth cache for all known IDs
        for rid in all_dirs_ids:
            self._get_depth_by_id(rid)

        # 2. Recursive Sizes (Bottom-up using IDs — sort by path length descending)
        sorted_ids = sorted(list(all_dirs_ids), key=lambda x: len(self.id_to_path[x]), reverse=True)
        recursive_sizes: Dict[int, int] = {rid: direct_sizes_ids.get(rid, 0) for rid in all_dirs_ids}
        for rid in sorted_ids:
            path = self.id_to_path[rid]
            if path == self.root_dir: continue
            parent_id = self._get_id(os.path.dirname(path))
            if parent_id in recursive_sizes:
                recursive_sizes[parent_id] += recursive_sizes[rid]

        # 3. Save Root Index
        root_id = self._get_id(self.root_dir)
        root_children = []
        for child_id in parent_to_children_ids.get(root_id, []):
            cp = self.id_to_path[child_id]
            child_depth = self._get_depth_by_id(child_id)
            root_children.append({
                "name": os.path.basename(cp) or cp,
                "path": cp,
                "value": recursive_sizes.get(child_id, 0),
                "type": "directory",
                "owner": self._get_owner_username(cp),
                "shard_id": str(child_id),
                "has_children": child_id in parent_to_children_ids and self.max_level > 1
            })

        rf_size = direct_sizes_ids.get(root_id, 0)
        if rf_size > 0:
            root_children.append({
                "name": f"[Files in {os.path.basename(self.root_dir) or self.root_dir}]",
                "path": os.path.join(self.root_dir, "__files__"),
                "value": rf_size, "type": "file_group",
                "owner": self._get_owner_username(self.root_dir), "has_children": False
            })
        root_children.sort(key=lambda x: x["value"], reverse=True)

        root_node = {
            "name": os.path.basename(self.root_dir) or self.root_dir,
            "path": self.root_dir,
            "value": recursive_sizes.get(root_id, 0),
            "type": "directory", "owner": self._get_owner_username(self.root_dir),
            "shard_id": str(root_id),
            "has_children": len(root_children) > 0, "children": root_children
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(root_node, f, separators=(',', ':'))

        # 4. Phase 3.2: Parallel serialization and streaming to SQLite
        db_path = os.path.join(os.path.dirname(output_path), "tree_map_data.db")
        if os.path.exists(db_path): os.remove(db_path)

        print(f"Phase 3.2: Processing shards with {self.max_workers} threads...")
        db_thread = threading.Thread(target=self._db_worker, args=(db_path,))
        db_thread.start()

        producers = []
        for _ in range(self.max_workers):
            p = threading.Thread(target=self._serialization_worker,
                                 args=(parent_to_children_ids, recursive_sizes, direct_sizes_ids))
            p.start()
            producers.append(p)

        # Feed directory IDs to workers
        for rid in all_dirs_ids:
            if self._get_depth_by_id(rid) < self.max_level:
                self.path_queue.put(rid)

        for _ in range(self.max_workers): self.path_queue.put(None)
        for p in producers: p.join()

        # Signal consumer that all producers are done.
        self.db_queue.put(None)
        db_thread.join()

        print(f"Completed: {self.total_processed:,} shards. RAM Strings: {len(self.id_to_path):,} unique paths.")
        print(f"Total TreeMap time: {time.time() - start_time:.2f}s")
