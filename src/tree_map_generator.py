"""
TreeMap Generator Module - Ultra High Performance (Final Optimization)

Optimized for 6M+ directories:
- Integer-based hierarchy building (Massive RAM saving)
- Size-based pruning (Reduces DB noise and size)
- Binary Hash IDs for SQLite (Faster indexing)
- Multi-threaded Producer-Consumer architecture
"""

import os
import json
import hashlib
import sqlite3
import time
import queue
import threading
import pwd
from typing import Dict, Any, List, Set, Tuple, Optional
from collections import defaultdict

try:
    from src import fast_scanner as _fast_scanner
    HAS_RUST_TREEMAP_SQLITE = hasattr(_fast_scanner, "write_shards_tsv_to_sqlite")
except ImportError:
    _fast_scanner = None  # type: ignore
    HAS_RUST_TREEMAP_SQLITE = False

class TreeMapGenerator:
    """The ultimate TreeMap generator for massive scale filesystems."""

    def __init__(self, root_dir: str, dir_sizes: Dict[str, Dict[str, int]], 
                 max_level: int = 5, max_workers: int = 4, min_size_mb: float = 1.0):
        self.root_dir = os.path.abspath(root_dir)
        self.dir_sizes = dir_sizes
        self.max_level = max_level
        self.max_workers = max_workers
        self.min_size_bytes = min_size_mb * 1024 * 1024
        
        # Queues for parallel processing
        self.path_queue = queue.Queue(maxsize=20000)
        self.db_queue = queue.Queue(maxsize=20000)
        
        # Internal String Table to save RAM
        self.id_to_path = []
        self.path_to_id = {}
        self.total_processed = 0
        self.uid_to_owner = {}
        self.path_to_owner = {}

    def _get_id(self, path: str) -> int:
        """String interning: Returns an integer ID for a path string."""
        if path in self.path_to_id:
            return self.path_to_id[path]
        idx = len(self.id_to_path)
        self.id_to_path.append(path)
        self.path_to_id[path] = idx
        return idx

    def _get_path_hash(self, path: str) -> str:
        """Hex MD5 for UI compatibility."""
        return hashlib.md5(path.encode('utf-8')).hexdigest()

    def _get_depth(self, path: str) -> int:
        if path == self.root_dir: return 0
        try:
            rel = os.path.relpath(path, self.root_dir)
            return 0 if rel == "." else rel.count(os.sep) + 1
        except ValueError: return 999

    def _get_owner_username(self, path: str) -> str:
        """Resolve owner username for a directory path with cache."""
        cached = self.path_to_owner.get(path)
        if cached is not None:
            return cached

        owner = "unknown"
        try:
            uid = os.stat(path).st_uid
            owner = self.uid_to_owner.get(uid)
            if owner is None:
                try:
                    owner = pwd.getpwuid(uid).pw_name
                except KeyError:
                    owner = f"uid-{uid}"
                self.uid_to_owner[uid] = owner
        except (OSError, PermissionError):
            owner = "unknown"

        self.path_to_owner[path] = owner
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
                
                # Pruning: skip small children in shards
                if child_size < self.min_size_bytes and self._get_depth(child_path) > 1:
                    continue

                children_nodes.append({
                    "name": os.path.basename(child_path) or child_path,
                    "path": child_path,
                    "value": child_size,
                    "type": "directory",
                    "owner": self._get_owner_username(child_path),
                    "shard_id": self._get_path_hash(child_path),
                    "has_children": child_id in parent_to_children_ids and self._get_depth(child_path) < self.max_level
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
                shard_id = self._get_path_hash(path)
                # Store shard_id as hex in JSON for web, but consumer can convert to bytes for SQLite
                json_data = json.dumps(children_nodes, separators=(',', ':'))
                self.db_queue.put((shard_id, json_data))
            
            self.path_queue.task_done()

    def _db_worker(self, db_path):
        """Consumer: Optimized SQLite writer with optional Rust fast-path."""
        if HAS_RUST_TREEMAP_SQLITE:
            self._db_worker_rust(db_path)
            return
        self._db_worker_python(db_path)

    def _db_worker_python(self, db_path):
        """Legacy Python sqlite3 writer (fallback-safe)."""
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA cache_size = -50000") # 50MB cache
        cursor = conn.cursor()
        # id is TEXT for simplicity with the current web logic, but indexed for speed
        cursor.execute("CREATE TABLE shards (id TEXT PRIMARY KEY, data TEXT)")
        
        batch = []
        count = 0
        while True:
            item = self.db_queue.get()
            if item is None:
                if batch: cursor.executemany("INSERT INTO shards VALUES (?, ?)", batch)
                self.db_queue.task_done()
                break
            
            batch.append(item)
            count += 1
            if len(batch) >= 5000: # Larger batches for high-speed I/O
                cursor.executemany("INSERT INTO shards VALUES (?, ?)", batch)
                batch = []
                if count % 500000 == 0:
                    print(f"  SQLite: Processed {count:,} shards...")
            
            self.db_queue.task_done()
        
        conn.commit()
        conn.close()
        self.total_processed = count

    def _db_worker_python_from_tsv(self, db_path, tsv_path):
        """Fallback writer: import shards from TSV into sqlite using Python."""
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA cache_size = -50000")
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE shards (id TEXT PRIMARY KEY, data TEXT)")

        batch = []
        count = 0
        with open(tsv_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                tab = line.find("\t")
                if tab < 0:
                    continue
                batch.append((line[:tab], line[tab + 1:]))
                count += 1
                if len(batch) >= 5000:
                    cursor.executemany("INSERT INTO shards VALUES (?, ?)", batch)
                    batch = []

        if batch:
            cursor.executemany("INSERT INTO shards VALUES (?, ?)", batch)

        conn.commit()
        conn.close()
        self.total_processed = count

    def _db_worker_rust(self, db_path):
        """Rust-accelerated DB writer by spooling queue items to TSV first."""
        tsv_path = db_path + ".shards.tsv"
        count = 0

        with open(tsv_path, "w", encoding="utf-8") as spool:
            while True:
                item = self.db_queue.get()
                if item is None:
                    self.db_queue.task_done()
                    break

                shard_id, json_data = item
                spool.write(shard_id)
                spool.write("\t")
                spool.write(json_data)
                spool.write("\n")
                count += 1
                self.db_queue.task_done()

        try:
            rust_count = _fast_scanner.write_shards_tsv_to_sqlite(tsv_path, db_path, 5000)
            self.total_processed = int(rust_count)
        except Exception as e:
            print(f"  [warn] Rust SQLite write failed, falling back to Python: {e}")
            self._db_worker_python_from_tsv(db_path, tsv_path)
        finally:
            try:
                os.remove(tsv_path)
            except OSError:
                pass

    def save(self, output_path: str):
        start_time = time.time()
        print(f"Phase 3.1: Building TreeMap hierarchy for {len(self.dir_sizes):,} directories...")

        # 1. Structure Analysis (Integer-based to save RAM)
        direct_sizes_ids = {}
        all_dirs_ids = {self._get_id(self.root_dir)}
        
        for dpath, uids in self.dir_sizes.items():
            abs_path = os.path.abspath(dpath)
            depth = self._get_depth(abs_path)
            size = sum(uids.values())
            if size <= 0: continue

            target_path = abs_path
            if depth > self.max_level:
                rel = os.path.relpath(abs_path, self.root_dir)
                parts = [] if rel in (".", "") else rel.split(os.sep)
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

        parent_to_children_ids = defaultdict(list)
        for rid in all_dirs_ids:
            path = self.id_to_path[rid]
            if path == self.root_dir: continue
            parent_id = self._get_id(os.path.dirname(path))
            if parent_id in all_dirs_ids:
                parent_to_children_ids[parent_id].append(rid)

        # 2. Recursive Sizes (Bottom-up using IDs)
        sorted_ids = sorted(list(all_dirs_ids), key=lambda x: len(self.id_to_path[x]), reverse=True)
        recursive_sizes = {rid: direct_sizes_ids.get(rid, 0) for rid in all_dirs_ids}
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
            root_children.append({
                "name": os.path.basename(cp) or cp,
                "path": cp,
                "value": recursive_sizes.get(child_id, 0),
                "type": "directory",
                "owner": self._get_owner_username(cp),
                "shard_id": self._get_path_hash(cp),
                "has_children": child_id in parent_to_children_ids and self.max_level > 1
            })
        
        rf_size = direct_sizes_ids.get(root_id, 0)
        if rf_size > 0:
            root_children.append({
                "name": f"[Files in {os.path.basename(self.root_dir) or self.root_dir}]",
                "path": os.path.join(self.root_dir, "__files__"),
                "value": rf_size, "type": "file_group", "owner": self._get_owner_username(self.root_dir), "has_children": False
            })
        root_children.sort(key=lambda x: x["value"], reverse=True)

        root_node = {
            "name": os.path.basename(self.root_dir) or self.root_dir,
            "path": self.root_dir,
            "value": recursive_sizes.get(root_id, 0),
            "type": "directory", "owner": self._get_owner_username(self.root_dir), "shard_id": self._get_path_hash(self.root_dir),
            "has_children": len(root_children) > 0, "children": root_children
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(root_node, f, separators=(',', ':'))

        # 4. Phase 3: Parallel serialization and streaming to SQLite
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
            if self._get_depth(self.id_to_path[rid]) < self.max_level:
                self.path_queue.put(rid)

        for _ in range(self.max_workers): self.path_queue.put(None)
        for p in producers: p.join()

        self.db_queue.put(None)
        db_thread.join()

        print(f"Completed: {self.total_processed:,} shards. RAM Strings: {len(self.id_to_path):,} unique paths.")
        print(f"Total TreeMap time: {time.time() - start_time:.2f}s")
