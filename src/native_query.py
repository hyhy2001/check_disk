"""
Native Index Query Module

Python bindings for CDX-1 native index query library.
"""

import ctypes as ct
import json
import os
from pathlib import Path
from typing import List, Optional, Tuple

# Locate native library (build via src/native_index/Makefile)
_lib_paths = [
    Path(__file__).parent / "native_index" / "libcdx1.so",
    Path(__file__).parent / "native_index" / "libcdx1.a",
]
_lib = None
for lp in _lib_paths:
    if lp.exists():
        try:
            _lib = ct.CDLL(str(lp))
            break
        except OSError:
            pass

if _lib is None:
    raise ImportError("libcdx1 not found or failed to load. Run: cd src/native_index && make")

# ── C struct definitions ────────────────────────────────────────────────────

class _CDX1Index(ct.Structure):
    _fields_ = [
        ("fd", ct.c_int),
        ("file_size", ct.c_size_t),
        ("base", ct.c_void_p),
        ("doc_count", ct.c_uint32),
        ("token_count", ct.c_uint32),
        ("ext_count", ct.c_uint32),
        ("user_count", ct.c_uint32),
        ("docs", ct.c_void_p),
        ("token_entries", ct.c_void_p),
        ("token_values", ct.POINTER(ct.c_uint32)),
        ("ext_entries", ct.c_void_p),
        ("ext_values", ct.POINTER(ct.c_uint32)),
        ("user_entries", ct.c_void_p),
        ("user_values", ct.POINTER(ct.c_uint32)),
    ]

class _CDX1Docset(ct.Structure):
    _fields_ = [
        ("doc_ids", ct.POINTER(ct.c_uint32)),
        ("count", ct.c_size_t),
    ]

class _CDX1Query(ct.Structure):
    _fields_ = [
        ("token_ids", ct.POINTER(ct.c_uint32)),
        ("token_count", ct.c_size_t),
        ("ext_ids", ct.POINTER(ct.c_uint32)),
        ("ext_count", ct.c_size_t),
        ("user_ids", ct.POINTER(ct.c_uint32)),
        ("user_count", ct.c_size_t),
        ("size_min", ct.c_uint64),
        ("size_max", ct.c_uint64),
        ("has_size_min", ct.c_int),
        ("has_size_max", ct.c_int),
    ]

# ── C API bindings ──────────────────────────────────────────────────────────

_lib.cdx1_open.argtypes = [ct.c_char_p, ct.POINTER(_CDX1Index)]
_lib.cdx1_open.restype = ct.c_int

_lib.cdx1_close.argtypes = [ct.POINTER(_CDX1Index)]
_lib.cdx1_close.restype = None

_lib.cdx1_query_docs.argtypes = [ct.POINTER(_CDX1Index), ct.POINTER(_CDX1Query), ct.POINTER(_CDX1Docset)]
_lib.cdx1_query_docs.restype = ct.c_int

_lib.cdx1_free_docset.argtypes = [ct.POINTER(_CDX1Docset)]
_lib.cdx1_free_docset.restype = None


# ── Python wrapper ──────────────────────────────────────────────────────────

class IndexQuery:
    """
    CDX-1 native index query handle.

    Usage:
        query = IndexQuery("detail_users/index")
        results = query.search(keywords=["readme"], extensions=[".txt"], size_min=1024)
        for doc_id in results:
            ...
    """

    def __init__(self, index_dir: str):
        self.index_dir = Path(index_dir)
        self.mmi_path = self.index_dir / "index.mmi"
        if not self.mmi_path.exists():
            raise FileNotFoundError(f"Index not found: {self.mmi_path}")

        self.tokens = self._load_dict("tokens.json")
        self.exts = self._load_dict("exts.json")
        self.users = self._load_dict("users.json")

        self._index = _CDX1Index()
        err = _lib.cdx1_open(str(self.mmi_path).encode("utf-8"), ct.byref(self._index))
        if err != 0:
            raise RuntimeError(f"cdx1_open failed: errno={err}")

    def _load_dict(self, filename: str) -> List[str]:
        path = self.index_dir / filename
        if not path.exists():
            return []
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def close(self):
        if hasattr(self, "_index"):
            _lib.cdx1_close(ct.byref(self._index))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def search(
        self,
        keywords: Optional[List[str]] = None,
        extensions: Optional[List[str]] = None,
        users: Optional[List[str]] = None,
        size_min: Optional[int] = None,
        size_max: Optional[int] = None,
    ) -> List[int]:
        """
        Query documents by keyword tokens, extensions, users, and size range.

        Args:
            keywords: List of path tokens to intersect (AND semantics).
            extensions: List of file extensions (e.g. [".txt", ".log"]).
            users: List of usernames to filter.
            size_min: Minimum file size in bytes.
            size_max: Maximum file size in bytes.

        Returns:
            List of document IDs matching all filters.
        """
        token_ids = self._resolve_tokens(keywords or [])
        ext_ids = self._resolve_ids(self.exts, extensions or [])
        user_ids = self._resolve_ids(self.users, users or [])

        query = _CDX1Query()
        if token_ids:
            query.token_ids = (ct.c_uint32 * len(token_ids))(*token_ids)
            query.token_count = len(token_ids)
        else:
            query.token_ids = None
            query.token_count = 0

        if ext_ids:
            query.ext_ids = (ct.c_uint32 * len(ext_ids))(*ext_ids)
            query.ext_count = len(ext_ids)
        else:
            query.ext_ids = None
            query.ext_count = 0

        if user_ids:
            query.user_ids = (ct.c_uint32 * len(user_ids))(*user_ids)
            query.user_count = len(user_ids)
        else:
            query.user_ids = None
            query.user_count = 0

        query.size_min = size_min if size_min is not None else 0
        query.size_max = size_max if size_max is not None else 0
        query.has_size_min = 1 if size_min is not None else 0
        query.has_size_max = 1 if size_max is not None else 0

        docset = _CDX1Docset()
        err = _lib.cdx1_query_docs(ct.byref(self._index), ct.byref(query), ct.byref(docset))
        if err != 0:
            raise RuntimeError(f"cdx1_query_docs failed: errno={err}")

        results = []
        if docset.count > 0 and docset.doc_ids:
            for i in range(docset.count):
                results.append(docset.doc_ids[i])

        _lib.cdx1_free_docset(ct.byref(docset))
        return results

    def _resolve_tokens(self, keywords: List[str]) -> List[int]:
        """Tokenize each keyword and resolve to token IDs."""
        token_ids = []
        token_lookup = {t: i for i, t in enumerate(self.tokens)}
        for kw in keywords:
            tokens = self._tokenize(kw)
            for t in tokens:
                tid = token_lookup.get(t)
                if tid is not None:
                    token_ids.append(tid)
        return token_ids

    def _resolve_ids(self, dict_list: List[str], values: List[str]) -> List[int]:
        lookup = {v: i for i, v in enumerate(dict_list)}
        return [lookup[v] for v in values if v in lookup]

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        """Simple alphanumeric tokenizer matching Rust phase_index logic."""
        tokens = []
        cur = []
        for ch in text:
            if ch.isalnum():
                cur.append(ch.lower())
            elif cur:
                tokens.append("".join(cur))
                cur = []
        if cur:
            tokens.append("".join(cur))
        return tokens
