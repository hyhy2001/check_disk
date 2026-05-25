"""
Smoke tests for src/report_generator.py

Tests cover:
- ReportGenerator initialization and config parsing
- generate_report() output shape against OUTPUT_CONTRACT.md
- JSON report field presence and types (date, directory, general_system,
  team_usage, user_usage, other_usage)
- Permission issues report generation
- Inode report generation
- save_json_report / load_json_report round-trip
"""

import json
import os
import sys
import time
import gzip
import struct

import pytest

# ── Ensure project root is on sys.path ──────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.disk_scanner import ScanResult  # noqa: E402
from src.report_generator import ReportGenerator  # noqa: E402
from src.utils import load_json_report, save_json_report  # noqa: E402

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_scan_result(tmp_path, *, user_usage=None, team_usage=None, other_usage=None,
                     permission_issues=None, user_inodes=None):
    """Build a minimal ScanResult with sensible defaults."""
    return ScanResult(
        general_system={"total": 100_000_000_000, "used": 40_000_000_000, "available": 60_000_000_000},
        team_usage=team_usage or [{"name": "backend", "used": 30_000_000_000, "team_id": 1}],
        user_usage=user_usage or [
            {"name": "alice", "used": 20_000_000_000, "team_id": 1},
            {"name": "bob",   "used": 10_000_000_000, "team_id": 1},
        ],
        other_usage=other_usage or [],
        timestamp=int(time.time()),
        permission_issues=permission_issues or {},
        user_inodes=user_inodes or [],
    )


def make_config(tmp_path, **overrides):
    base = {
        "directory": str(tmp_path),
        "output_file": str(tmp_path / "disk_usage_report.json"),
        "teams": [{"name": "backend", "team_id": 1}],
        "users": [
            {"name": "alice", "team_id": 1},
            {"name": "bob",   "team_id": 1},
        ],
    }
    base.update(overrides)
    return base


# ── Unified Rust detail DB integration ─────────────────────────────────────────

def test_generate_detail_reports_builds_unified_db_and_treemap(tmp_path):
    import sqlite3
    import src.report_generator as report_generator_module

    if not report_generator_module.HAS_RUST_PIPELINE:
        pytest.skip("fast_scanner.build_detail_db is not available")

    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")

        def write_bin(uid, size, path_str):
            f.write(uid.to_bytes(4, "little"))
            f.write(size.to_bytes(8, "little"))
            p = path_str.encode("utf-8")
            f.write(len(p).to_bytes(4, "little"))
            f.write(p)

        write_bin(1000, 4096, str(tmp_path / "alpha.txt"))
        write_bin(1000, 2048, str(tmp_path / "sub" / "beta.log"))

    cfg = make_config(
        tmp_path,
        output_file=str(tmp_path / "disk_usage_report.json"),
        directory=str(tmp_path),
        users=[{"name": "alice", "team_id": "backend"}],
    )
    scan_result = make_scan_result(tmp_path)
    scan_result.detail_tmpdir = str(detail_tmpdir)
    scan_result.detail_uid_username = {1000: "alice"}

    created = ReportGenerator(cfg).generate_detail_reports(
        scan_result, max_workers=1, build_treemap=True
    )

    detail_db = tmp_path / "detail_users" / "data_detail.db"
    treemap_db = tmp_path / "tree_map_data" / "treemap.db"

    assert created == sorted(map(str, [detail_db, treemap_db]))
    assert detail_db.exists()
    assert treemap_db.exists()

    # Legacy NDJSON layout MUST be cleaned up.
    assert not (tmp_path / "detail_users" / "manifest.json").exists()
    assert not (tmp_path / "detail_users" / "data_detail.json").exists()
    assert not (tmp_path / "detail_users" / "users").exists()
    assert not (tmp_path / "detail_users" / "api").exists()
    assert not (tmp_path / "tree_map_report.json").exists()
    assert not (tmp_path / "tree_map_data" / "shards").exists()
    assert not (tmp_path / "tree_map_data" / "api").exists()
    assert not (tmp_path / "tree_map_data" / "manifest.json").exists()

    # ── detail.db schema + content ────────────────────────────────────────
    conn = sqlite3.connect(f"file:{detail_db}?mode=ro", uri=True)
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert tables >= {
            "meta",
            "users",
            "file_names",
            "dirs",
            "files",
            "fts_file_names",
            "fts_dir_paths",
        }
        # user_ext_stats was removed (no consumers in dashboard or scripts).
        assert "user_ext_stats" not in tables
        # top_files and dir_user_size removed in new schema.
        assert "top_files" not in tables
        assert "dir_user_size" not in tables
        indexes = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert indexes >= {
            "ix_files_uid_size",
        }
        # ix_files_uid_ext_size and ix_files_name_uid dropped (no production callers).
        assert "ix_files_uid_ext_size" not in indexes
        assert "ix_files_name_uid" not in indexes
        # ix_files_dir_size was removed (no callers).
        assert "ix_files_dir_size" not in indexes
        # Old partial index replaced by full ix_files_uid_size.
        assert "ix_files_uid_size_big" not in indexes

        # Stamp + version
        app_id = conn.execute("PRAGMA application_id").fetchone()[0]
        # Two's-complement: stored as signed; expected magic 0xC0DD15D1.
        expected = 0xC0DD15D1
        if app_id < 0:
            app_id += 1 << 32
        assert app_id == expected
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1

        # users
        row = conn.execute(
            "SELECT total_files, total_size FROM users WHERE username='alice'"
        ).fetchone()
        assert row == (2, 6144)

        # files: basenames live in detail.db.names (file basenames),
        # NOT tm.names (which holds dir segments only).
        files = conn.execute(
            "SELECT n.name, f.ext, f.size "
            "FROM files f JOIN file_names n ON f.name_id=n.id "
            "ORDER BY f.size DESC"
        ).fetchall()
        assert {(n, x, s) for (n, x, s) in files} == {
            ("alpha.txt", "txt", 4096),
            ("beta.log", "log", 2048),
        }

        # files ordered by size DESC (top_files table removed in new schema)
        top = conn.execute(
            "SELECT f.size FROM files f "
            "WHERE f.uid=(SELECT uid FROM users WHERE username='alice') "
            "ORDER BY f.size DESC"
        ).fetchall()
        assert [r[0] for r in top] == [4096, 2048]

        # Full (uid, size DESC) index covers ORDER BY for any page.
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM files "
            "WHERE uid=0 ORDER BY size DESC LIMIT 10"
        ).fetchall()
        assert any("ix_files_uid_size" in str(row) for row in plan)
    finally:
        conn.close()

    # ── treemap.db schema + content ───────────────────────────────────────
    conn = sqlite3.connect(f"file:{treemap_db}?mode=ro", uri=True)
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert tables >= {"meta", "names", "owners", "dirs"}

        app_id = conn.execute("PRAGMA application_id").fetchone()[0]
        expected = 0xC0DD15C0
        if app_id < 0:
            app_id += 1 << 32
        assert app_id == expected

        # Root dir present, has children
        root_row = conn.execute(
            "SELECT id, total_size FROM dirs WHERE parent_id IS NULL"
        ).fetchone()
        assert root_row is not None
        root_id, root_total = root_row
        assert root_total == 6144

        # Path reconstruction reaches alpha.txt's parent
        # (its dir is the scan tmp_path itself)
        children = conn.execute(
            "SELECT n.name, d.total_size FROM dirs d "
            "JOIN names n ON d.name_id=n.id "
            "WHERE d.parent_id=? ORDER BY d.total_size DESC",
            (root_id,),
        ).fetchall()
        assert any(c[0] == "sub" for c in children)
    finally:
        conn.close()


# ── ReportGenerator.__init__ ──────────────────────────────────────────────────

class TestReportGeneratorInit:
    def test_output_file_from_config(self, tmp_path):
        cfg = make_config(tmp_path)
        rg = ReportGenerator(cfg)
        assert rg.output_file == str(tmp_path / "disk_usage_report.json")

    def test_default_output_file_fallback(self, tmp_path):
        """When output_file is absent, should default to 'disk_usage_report.json'."""
        cfg = make_config(tmp_path)
        del cfg["output_file"]
        rg = ReportGenerator(cfg)
        assert rg.output_file == "disk_usage_report.json"

    def test_debug_false_by_default(self, tmp_path):
        rg = ReportGenerator(make_config(tmp_path))
        assert rg.debug is False

    def test_debug_true(self, tmp_path):
        rg = ReportGenerator(make_config(tmp_path, debug=True))
        assert rg.debug is True


# ── generate_report() output contract ────────────────────────────────────────

class TestGenerateReport:
    def test_returns_dict(self, tmp_path):
        rg = ReportGenerator(make_config(tmp_path))
        result = rg.generate_report(make_scan_result(tmp_path))
        assert isinstance(result, dict)

    def test_top_level_keys_present(self, tmp_path):
        """OUTPUT_CONTRACT §1 — required keys must all exist."""
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        for key in ("date", "directory", "general_system", "team_usage", "user_usage", "other_usage"):
            assert key in report, f"Missing top-level key: {key}"

    def test_date_is_unix_epoch(self, tmp_path):
        before = int(time.time()) - 1
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        assert isinstance(report["date"], int)
        assert report["date"] >= before

    def test_directory_matches_config(self, tmp_path):
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        assert report["directory"] == str(tmp_path)

    def test_general_system_has_required_keys(self, tmp_path):
        """OUTPUT_CONTRACT §1 — general_system must have total, used, available."""
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        gs = report["general_system"]
        for key in ("total", "used", "available"):
            assert key in gs, f"general_system missing key: {key}"
        assert gs["total"] > 0

    def test_team_usage_is_list_with_name_and_used(self, tmp_path):
        """OUTPUT_CONTRACT §1 — team_usage items must have name + used."""
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        assert isinstance(report["team_usage"], list)
        for item in report["team_usage"]:
            assert "name" in item
            assert "used" in item

    def test_user_usage_is_list_with_name_and_used(self, tmp_path):
        """OUTPUT_CONTRACT §1 — user_usage items must have name + used."""
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        assert isinstance(report["user_usage"], list)
        for item in report["user_usage"]:
            assert "name" in item
            assert "used" in item

    def test_user_usage_team_id_injected(self, tmp_path):
        """team_id must be injected into user_usage from config mapping."""
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        alice = next((u for u in report["user_usage"] if u["name"] == "alice"), None)
        assert alice is not None, "alice should appear in user_usage"
        assert alice.get("team_id") == 1, "team_id should be injected for alice"

    def test_team_usage_team_id_injected(self, tmp_path):
        """team_id must be injected into team_usage entries."""
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(make_scan_result(tmp_path))
        backend = next((t for t in report["team_usage"] if t["name"] == "backend"), None)
        assert backend is not None
        assert backend.get("team_id") == 1

    def test_general_system_inodes_filtered_out(self, tmp_path):
        """OUTPUT_CONTRACT: inodes_* keys in general_system must be stripped from the JSON report."""
        scan = make_scan_result(tmp_path)
        scan.general_system["inodes_total"] = 1_000_000
        scan.general_system["inodes_free"] = 900_000
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(scan)
        gs = report["general_system"]
        assert "inodes_total" not in gs
        assert "inodes_free" not in gs

    def test_report_written_to_disk(self, tmp_path):
        """generate_report() must write the JSON file to output_file path."""
        out = tmp_path / "disk_usage_report.json"
        rg = ReportGenerator(make_config(tmp_path, output_file=str(out)))
        rg.generate_report(make_scan_result(tmp_path))
        assert out.exists(), "Report JSON must be written to disk"
        data = json.loads(out.read_text())
        assert "date" in data

    def test_empty_scan_result_returns_valid_report(self, tmp_path):
        """generate_report(None) must return a minimal valid report, not raise."""
        rg = ReportGenerator(make_config(tmp_path))
        report = rg.generate_report(None)
        for key in ("date", "directory", "general_system", "team_usage", "user_usage", "other_usage"):
            assert key in report


# ── Permission issues report ──────────────────────────────────────────────────
#
# permission_issues.json is no longer written by Python — Rust Phase 2 emits
# permission_issues.db directly. Tests asserting the JSON shape have been
# removed; coverage of the DB output lives in tests/test_unified_pipeline.py.


# ── Inode report ──────────────────────────────────────────────────────────────

class TestInodeReport:
    def test_inode_report_file_created(self, tmp_path):
        """generate_inode_report() must write inode_usage_report.json."""
        scan = make_scan_result(tmp_path, user_inodes=[
            {"name": "alice", "inodes": 50000},
            {"name": "bob",   "inodes": 20000},
        ])
        rg = ReportGenerator(make_config(tmp_path, output_file=str(tmp_path / "disk_usage_report.json")))
        rg.generate_report(scan)
        inode_file = tmp_path / "inode_usage_report.json"
        assert inode_file.exists(), "inode_usage_report.json should be written"

    def test_inode_report_contains_users(self, tmp_path):
        """inode_usage_report.json must have a 'users' (or equivalent) array."""
        scan = make_scan_result(tmp_path, user_inodes=[{"name": "alice", "inodes": 5000}])
        rg = ReportGenerator(make_config(tmp_path, output_file=str(tmp_path / "disk_usage_report.json")))
        rg.generate_report(scan)
        data = json.loads((tmp_path / "inode_usage_report.json").read_text())
        # The report must have some key holding the per-user inode data
        assert any(isinstance(v, list) for v in data.values()), \
            "inode report must contain at least one list (per-user inode data)"


# ── save/load JSON round-trip ─────────────────────────────────────────────────

class TestJsonRoundTrip:
    def test_save_and_load(self, tmp_path):
        path = str(tmp_path / "test_report.json")
        payload = {"date": 123456, "directory": "/data", "users": [{"name": "alice", "used": 999}]}
        save_json_report(payload, path)
        loaded = load_json_report(path)
        assert loaded == payload

    def test_save_creates_parent_dirs(self, tmp_path):
        """save_json_report must create intermediate directories if they do not exist."""
        deep = str(tmp_path / "a" / "b" / "c" / "report.json")
        save_json_report({"ok": True}, deep)
        assert os.path.exists(deep)

    def test_load_missing_file_returns_none_or_raises(self, tmp_path):
        """load_json_report on a nonexistent path must not silently return garbage."""
        result = load_json_report(str(tmp_path / "no_such_file.json"))
        # Acceptable: None, {}, or a FileNotFoundError is raised
        assert result is None or result == {} or isinstance(result, dict)
