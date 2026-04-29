"""
Smoke tests for src/disk_scanner.py

Tests cover:
- ScanResult dataclass structure and defaults
- DiskScanner initialization with the Rust core
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

from src.disk_scanner import _HAS_FAST_SCANNER, DiskScanner, ScanResult  # noqa: E402
from src.utils import ScanHelper, format_size  # noqa: E402

# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def minimal_config(tmp_path):
    """Minimal config dict that mirrors disk_checker_config.json structure."""
    return {
        "directory": str(tmp_path),
        "output_file": str(tmp_path / "disk_usage_report.json"),
        "teams": [{"name": "backend", "team_ID": 1}],
        "users": [{"name": "alice", "team_ID": 1}],
        "use_rust": True,
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
    def test_init_uses_rust_when_available(self, minimal_config, tmp_path):
        """use_rust=True activates the Rust path when fast_scanner is importable."""
        if not _HAS_FAST_SCANNER:
            pytest.skip("fast_scanner is not importable in this environment")
        scanner = DiskScanner(minimal_config, max_workers=2)
        assert scanner.use_rust is True

    def test_init_requires_rust_core(self, minimal_config):
        """use_rust=False is no longer supported now that the Python fallback is removed."""
        config = dict(minimal_config, use_rust=False)
        with pytest.raises(NotImplementedError):
            DiskScanner(config, max_workers=1)

    def test_max_workers_respected(self, minimal_config):
        """Explicitly set max_workers must be stored."""
        if not _HAS_FAST_SCANNER:
            pytest.skip("fast_scanner is not importable in this environment")
        scanner = DiskScanner(minimal_config, max_workers=4)
        assert scanner.max_workers == 4

    def test_debug_flag(self, minimal_config):
        """debug=True must be stored on the scanner."""
        if not _HAS_FAST_SCANNER:
            pytest.skip("fast_scanner is not importable in this environment")
        scanner = DiskScanner(minimal_config, debug=True)
        assert scanner.debug is True


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
