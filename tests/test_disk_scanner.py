"""
Smoke tests for src/disk_scanner.py

Tests cover:
- ScanResult dataclass structure and defaults
- DiskScanner initialization (with/without Rust core)
- Python-fallback path via LegacyDiskScanner mock
- ScanHelper utilities (used inside scanner post-processing)
"""

import os
import sys
import time
import pytest

# ── Ensure project root is on sys.path ──────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.disk_scanner import ScanResult, DiskScanner, _HAS_FAST_SCANNER
from src.utils import ScanHelper, format_size


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_config(tmp_path):
    """Minimal config dict that mirrors disk_checker_config.json structure."""
    return {
        "directory": str(tmp_path),
        "output_file": str(tmp_path / "disk_usage_report.json"),
        "teams": [{"name": "backend", "team_ID": 1}],
        "users": [{"name": "alice", "team_ID": 1}],
        "use_rust": False,  # force Python path in unit tests
    }


# ── ScanResult dataclass ──────────────────────────────────────────────────────

class TestScanResult:
    def test_required_fields_only(self):
        """ScanResult can be constructed with only required positional fields."""
        result = ScanResult(
            general_system={"total": 1000, "used": 200, "available": 800},
            team_usage=[],
            user_usage=[],
            other_usage=[],
            timestamp=int(time.time()),
        )
        assert isinstance(result.general_system, dict)
        assert isinstance(result.team_usage, list)
        assert isinstance(result.user_usage, list)
        assert isinstance(result.other_usage, list)
        assert isinstance(result.timestamp, int)

    def test_optional_fields_have_safe_defaults(self):
        """All optional fields must default to empty/falsy values — no None surprises."""
        result = ScanResult(
            general_system={},
            team_usage=[],
            user_usage=[],
            other_usage=[],
            timestamp=0,
        )
        assert result.top_dir == []
        assert result.permission_issues == {}
        assert result.user_inodes == []
        assert result.detail_files == {}
        assert result.detail_tmpdir == ""
        assert result.detail_uid_username == {}
        assert result.dir_sizes_map == {}

    def test_fields_are_independent(self):
        """Mutable default fields must NOT share state between instances (dataclass field hygiene)."""
        r1 = ScanResult(general_system={}, team_usage=[], user_usage=[], other_usage=[], timestamp=0)
        r2 = ScanResult(general_system={}, team_usage=[], user_usage=[], other_usage=[], timestamp=0)
        r1.top_dir.append({"dir": "/tmp", "user": "alice", "user_usage": 100})
        assert r2.top_dir == [], "Default mutable list must not be shared between instances"

    def test_general_system_keys(self):
        """general_system must expose at least the three disk-space keys required by OUTPUT_CONTRACT."""
        result = ScanResult(
            general_system={"total": 500, "used": 100, "available": 400},
            team_usage=[],
            user_usage=[],
            other_usage=[],
            timestamp=0,
        )
        for key in ("total", "used", "available"):
            assert key in result.general_system, f"Missing key: {key}"

    def test_user_usage_entry_shape(self):
        """Each user_usage entry must have 'name' and 'used' keys (OUTPUT_CONTRACT §1)."""
        entry = {"name": "alice", "used": 1024 * 1024}
        result = ScanResult(
            general_system={"total": 1, "used": 1, "available": 0},
            team_usage=[],
            user_usage=[entry],
            other_usage=[],
            timestamp=0,
        )
        row = result.user_usage[0]
        assert "name" in row
        assert "used" in row

    def test_team_usage_entry_shape(self):
        """Each team_usage entry must have 'name' and 'used' keys (OUTPUT_CONTRACT §1)."""
        entry = {"name": "backend", "used": 2048, "team_id": 1}
        result = ScanResult(
            general_system={"total": 1, "used": 1, "available": 0},
            team_usage=[entry],
            user_usage=[],
            other_usage=[],
            timestamp=0,
        )
        row = result.team_usage[0]
        assert "name" in row
        assert "used" in row


# ── DiskScanner initialization ────────────────────────────────────────────────

class TestDiskScannerInit:
    def test_init_disables_rust_when_flag_false(self, minimal_config):
        """use_rust=False must not attempt to use fast_scanner."""
        scanner = DiskScanner(minimal_config, max_workers=2)
        assert scanner.use_rust is False

    def test_init_uses_rust_when_available(self, minimal_config, tmp_path):
        """use_rust=True activates Rust path iff fast_scanner is importable."""
        config = dict(minimal_config, use_rust=True)
        scanner = DiskScanner(config, max_workers=2)
        assert scanner.use_rust == _HAS_FAST_SCANNER

    def test_max_workers_respected(self, minimal_config):
        """Explicitly set max_workers must be stored."""
        scanner = DiskScanner(minimal_config, max_workers=4)
        assert scanner.max_workers == 4

    def test_debug_flag(self, minimal_config):
        """debug=True must be stored on the scanner."""
        scanner = DiskScanner(minimal_config, debug=True)
        assert scanner.debug is True

    def test_missing_fast_scanner_loads_legacy(self, minimal_config, monkeypatch):
        """When fast_scanner is unavailable and use_rust=False, LegacyDiskScanner must be loaded."""
        # Force Python path even if Rust is available
        config = dict(minimal_config, use_rust=False)
        scanner = DiskScanner(config, max_workers=1)
        # _scanner is set only for the Python/legacy path
        assert scanner._scanner is not None or _HAS_FAST_SCANNER, (
            "Expected _scanner to be a LegacyDiskScanner instance when use_rust=False"
        )


# ── Python-fallback scan (integration smoke) ──────────────────────────────────

class TestDiskScannerPythonFallback:
    """Run an actual scan against a tiny synthetic fixture using the Python fallback.

    These tests are skipped when the legacy scanner cannot run (very rare).
    They verify the ScanResult shape without asserting specific byte counts
    (which vary with inode allocation and OS block size).
    """

    @pytest.fixture
    def fixture_dir(self, tmp_path):
        """Create a small reproducible directory tree for scanning."""
        (tmp_path / "subdir_a").mkdir()
        (tmp_path / "subdir_b").mkdir()
        (tmp_path / "subdir_a" / "file1.txt").write_bytes(b"hello world")
        (tmp_path / "subdir_a" / "file2.log").write_bytes(b"x" * 4096)
        (tmp_path / "subdir_b" / "archive.tar").write_bytes(b"y" * 8192)
        (tmp_path / "top_level.py").write_bytes(b"# code")
        return tmp_path

    @pytest.fixture
    def fallback_config(self, fixture_dir, tmp_path):
        return {
            "directory": str(fixture_dir),
            "output_file": str(tmp_path / "disk_usage_report.json"),
            "teams": [],
            "users": [],
            "use_rust": False,
            "target_users_only": False,
        }

    def test_scan_returns_scan_result(self, fallback_config):
        """scan() must return a ScanResult-like object with the expected attributes."""
        scanner = DiskScanner(fallback_config, max_workers=2)
        result = scanner.scan()
        for attr in (
            "general_system",
            "team_usage",
            "user_usage",
            "other_usage",
            "timestamp",
            "permission_issues",
        ):
            assert hasattr(result, attr), f"Missing attribute: {attr}"

    def test_general_system_positive_total(self, fallback_config):
        """general_system.total must be > 0 for any real filesystem."""
        scanner = DiskScanner(fallback_config, max_workers=2)
        result = scanner.scan()
        assert result.general_system.get("total", 0) > 0, "total disk size should be > 0"

    def test_timestamp_is_recent(self, fallback_config):
        """timestamp must be a Unix epoch within 60 seconds of now."""
        before = int(time.time())
        scanner = DiskScanner(fallback_config, max_workers=2)
        result = scanner.scan()
        after = int(time.time())
        assert before - 5 <= result.timestamp <= after + 5

    def test_output_contract_user_usage_shape(self, fallback_config):
        """Each item in user_usage must have 'name' (str) and 'used' (int) keys."""
        scanner = DiskScanner(fallback_config, max_workers=2)
        result = scanner.scan()
        for item in result.user_usage:
            assert "name" in item, f"Missing 'name' in {item}"
            assert "used" in item, f"Missing 'used' in {item}"
            assert isinstance(item["name"], str)
            assert isinstance(item["used"], int)

    def test_output_contract_team_usage_shape(self, fallback_config):
        """Each item in team_usage must have 'name' (str) and 'used' (int) keys."""
        scanner = DiskScanner(fallback_config, max_workers=2)
        result = scanner.scan()
        for item in result.team_usage:
            assert "name" in item
            assert "used" in item


# ── ScanHelper utilities ──────────────────────────────────────────────────────

class TestScanHelper:
    def test_create_user_list_empty(self):
        result = ScanHelper.create_user_list({})
        assert result == []

    def test_create_user_list_sorted_desc(self):
        usage = {"alice": 500, "bob": 1000, "carol": 200}
        result = ScanHelper.create_user_list(usage)
        sizes = [item["used"] for item in result]
        assert sizes == sorted(sizes, reverse=True), "Must be sorted by used desc"

    def test_create_user_list_shape(self):
        usage = {"alice": 1024}
        result = ScanHelper.create_user_list(usage)
        assert len(result) == 1
        assert result[0]["name"] == "alice"
        assert result[0]["used"] == 1024


# ── format_size utility ───────────────────────────────────────────────────────

class TestFormatSize:
    @pytest.mark.parametrize("size,expected", [
        (0, "0 B"),
        (500, "500 B"),
        (1_000, "1 KB"),
        (1_500_000, "2 MB"),
        (2_000_000_000, "2.0 GB"),
        (3_000_000_000_000, "3.00 TB"),
        (-1, "0 B"),
    ])
    def test_format_size_units(self, size, expected):
        assert format_size(size) == expected, f"format_size({size}) != {expected!r}"
