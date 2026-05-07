#!/usr/bin/env python3
"""
Build path_dict.seek index from existing path_dict.ndjson (PDX1 format).
Format: magic "PDX1" + version u32 + count u32 + records[gid:u32, offset:u64, len:u32]
"""

import json
import os
import struct
import sys


def build_seek_index(ndjson_path: str, seek_path: str) -> None:
    """Build binary seek index for path_dict.ndjson."""
    records = []
    with open(ndjson_path, "rb") as f:
        offset = 0
        for line in f:
            row_len = len(line)
            try:
                obj = json.loads(line.decode("utf-8", errors="ignore").rstrip())
                gid = int(obj.get("gid", -1))
                if gid >= 0:
                    records.append((gid, offset, row_len))
            except Exception:
                pass
            offset += row_len

    records.sort(key=lambda x: x[0])

    with open(seek_path, "wb") as out:
        out.write(b"PDX1")
        out.write(struct.pack("<I", 1))  # version
        out.write(struct.pack("<I", len(records)))  # count
        for gid, off, length in records:
            out.write(struct.pack("<I", gid))
            out.write(struct.pack("<Q", off))
            out.write(struct.pack("<I", length))

    print(f"Built {seek_path} with {len(records)} records from {ndjson_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path_dict.ndjson>")
        sys.exit(1)
    ndjson = sys.argv[1]
    seek = ndjson.replace(".ndjson", ".seek")
    if not os.path.exists(ndjson):
        print(f"Error: {ndjson} not found")
        sys.exit(1)
    build_seek_index(ndjson, seek)
