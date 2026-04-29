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
    import src.report_generator as report_generator_module

    if not report_generator_module.HAS_RUST_UNIFIED_DB:
        pytest.skip("fast_scanner.build_unified_dbs is not available")

    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    (detail_tmpdir / "scan_t1_c1.tsv").write_text(
        f"F\t1000\t4096\t{tmp_path / 'alpha.txt'}\nF\t1000\t2048\t{tmp_path / 'sub' / 'beta.log'}\n",
        encoding="utf-8",
    )

    cfg = make_config(
        tmp_path,
        output_file=str(tmp_path / "disk_usage_report.json"),
        directory=str(tmp_path),
        users=[{"name": "alice", "team_id": "backend"}],
    )
    scan_result = make_scan_result(tmp_path)
    scan_result.detail_tmpdir = str(detail_tmpdir)
    scan_result.detail_uid_username = {1000: "alice"}

    created = ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1)

    detail_manifest = tmp_path / "detail_users" / "data_detail.json"
    treemap_json = tmp_path / "tree_map_report.json"
    treemap_manifest = tmp_path / "tree_map_data" / "manifest.json"
    assert sorted(map(str, [detail_manifest, treemap_json, treemap_manifest])) == created
    assert detail_manifest.exists()
    assert treemap_json.exists()
    assert treemap_manifest.exists()

    detail = json.loads(detail_manifest.read_text(encoding="utf-8"))
    assert detail["users"][0]["username"] == "alice"
    assert detail["users"][0]["total_files"] == 2
    assert detail["users"][0]["total_dirs"] == 2
    assert detail["users"][0]["total_used"] == 6144

    user_dir = tmp_path / "detail_users" / "users" / "alice"
    user_manifest = json.loads((user_dir / "manifest.json").read_text(encoding="utf-8"))
    part_path = user_manifest["files"]["parts"][0]["path"]
    file_rows = (user_dir / part_path).read_text(encoding="utf-8").splitlines()
    assert len(file_rows) == 2
    assert "alpha.txt" in file_rows[0]
    assert "beta.log" in file_rows[1]

    tree_manifest = json.loads(treemap_manifest.read_text(encoding="utf-8"))
    assert tree_manifest["shard_count"] >= 1


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

class TestPermissionIssuesReport:
    def test_permission_issues_file_created(self, tmp_path):
        """generate_permission_issues_report() must write permission_issues.json."""
        out_dir = tmp_path
        scan = make_scan_result(tmp_path, permission_issues={
            "users": [{"name": "alice", "inaccessible_items": [{"path": "/secret", "type": "dir", "error": "EACCES"}]}],
            "unknown_items": [],
        })
        rg = ReportGenerator(make_config(tmp_path, output_file=str(out_dir / "disk_usage_report.json")))
        # generate_report calls generate_permission_issues_report internally
        rg.generate_report(scan)
        perm_file = out_dir / "permission_issues.json"
        assert perm_file.exists(), "permission_issues.json should be written"

    def test_permission_issues_schema(self, tmp_path):
        """permission_issues.json must have 'users' and 'unknown_items' keys."""
        scan = make_scan_result(tmp_path, permission_issues={
            "users": [],
            "unknown_items": [{"path": "/lost+found", "type": "dir", "error": "?"}],
        })
        rg = ReportGenerator(make_config(tmp_path, output_file=str(tmp_path / "disk_usage_report.json")))
        rg.generate_report(scan)
        data = json.loads((tmp_path / "permission_issues.json").read_text())
        assert "users" in data["permission_issues"] or "users" in data
        assert "unknown_items" in data["permission_issues"] or "unknown_items" in data


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
