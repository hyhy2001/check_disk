import json
import os
import sqlite3
import sys
import threading
import time

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import disk_checker  # noqa: E402,F401
from scripts import export_user_reports  # noqa: E402
from src.cli_interface import CLIInterface  # noqa: E402
from src.disk_scanner import ScanResult  # noqa: E402
from src.report_generator import ReportGenerator  # noqa: E402
from src.scan_status import ScanStatusHeartbeat  # noqa: E402


class _FakeSyncPipeline:
    def __init__(self):
        self.paths = []

    def enqueue_file(self, path):
        self.paths.append(path)


def test_scan_status_heartbeat_syncs_periodically(tmp_path):
    pipeline = _FakeSyncPipeline()
    heartbeat = ScanStatusHeartbeat(
        str(tmp_path),
        started_at=time.time(),
        sync_pipeline=pipeline,
        interval=0.05,
        sync_interval=0.12,
    )
    heartbeat.set_phase("scan", "Scanning")
    heartbeat.start()
    try:
        time.sleep(0.28)
    finally:
        heartbeat.stop()

    status_path = tmp_path / "scan_status.json"
    assert status_path.exists()
    assert str(status_path) in pipeline.paths
    assert len(pipeline.paths) >= 2
    assert len(pipeline.paths) <= 4


def _build_unified_fixture(tmp_path):
    import src.report_generator as report_generator_module

    if not report_generator_module.HAS_RUST_PIPELINE:
        pytest.skip("fast_scanner.build_pipeline is not available")

    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")
        def write_bin(uid, size, path_str):
            f.write(uid.to_bytes(4, 'little'))
            f.write(size.to_bytes(8, 'little'))
            p = path_str.encode('utf-8')
            f.write(len(p).to_bytes(4, 'little'))
            f.write(p)
        write_bin(1000, 4096, str(tmp_path / 'alpha.txt'))
        write_bin(1000, 2048, str(tmp_path / 'sub' / 'shared.log'))
        write_bin(1000, 512, str(tmp_path / 'sub' / 'same.bin'))
        write_bin(1001, 8192, str(tmp_path / 'other' / 'alpha.txt'))
        write_bin(1001, 1024, str(tmp_path / 'other' / 'logs' / 'shared.log'))
    (detail_tmpdir / "perm_t1.tsv").write_text(
        "\n".join(
            [
                f"P\t1000\tdirectory\tEACCES\t{tmp_path / 'secret'}",
                f"P\t1001\tfile\tENOENT\t{tmp_path / 'ghost.txt'}",
                f"P\t0\tdirectory\tEIO\t{tmp_path / 'bad_disk'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    cfg = {
        "directory": str(tmp_path),
        "output_file": str(tmp_path / "disk_usage_report.json"),
        "teams": [{"name": "backend", "team_id": "backend"}],
        "users": [
            {"name": "alice", "team_id": "backend"},
            {"name": "bob", "team_id": "backend"},
        ],
    }
    scan_result = ScanResult(
        general_system={"total": 1, "used": 1, "available": 0},
        team_usage=[],
        user_usage=[],
        other_usage=[],
        timestamp=1234567890,
        detail_tmpdir=str(detail_tmpdir),
        detail_uid_username={1000: "alice", 1001: "bob"},
    )

    created = ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=True)
    detail_db = tmp_path / "detail_users" / "data_detail.db"
    treemap_db = tmp_path / "tree_map_data" / "treemap.db"
    return cfg, created, detail_db, treemap_db


def _open_detail(detail_db, treemap_db=None):
    conn = sqlite3.connect(f"file:{detail_db}?mode=ro", uri=True)
    if treemap_db and os.path.isfile(treemap_db):
        conn.execute(f"ATTACH DATABASE 'file:{treemap_db}?mode=ro' AS tm")
    return conn


def test_unified_db_outputs_multi_user_ext_and_paths(tmp_path):
    _, _, detail_db, treemap_db = _build_unified_fixture(tmp_path)
    assert detail_db.exists()
    assert treemap_db.exists()

    conn = _open_detail(detail_db, treemap_db)
    try:
        # Per-user totals come from the `users` table.
        users = {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "SELECT username, total_files, total_size FROM users "
                "WHERE username IN ('alice','bob')"
            )
        }
        assert users["alice"] == (3, 6656)
        assert users["bob"] == (2, 9216)

        # alice has 3 files: 4096 (txt) + 2048 (log) + 512 (bin)
        rows = conn.execute(
            "SELECT n.name, f.ext, f.size FROM files f "
            "JOIN file_names n ON f.name_id=n.id "
            "WHERE f.uid=(SELECT uid FROM users WHERE username='alice') "
            "ORDER BY f.size DESC"
        ).fetchall()
        assert [r[2] for r in rows] == [4096, 2048, 512]
        assert [r for r in rows if r[1] == "log"] == [("shared.log", "log", 2048)]
        assert any(r[0] == "alpha.txt" for r in rows)

        # bob's per-dir aggregate: dirs has rows summing to bob's used
        bob_dirs = conn.execute(
            "SELECT size FROM dirs "
            "JOIN users u ON dirs.uid=u.uid "
            "WHERE u.username='bob' ORDER BY size DESC"
        ).fetchall()
        assert sorted([r[0] for r in bob_dirs]) == [1024, 8192]
    finally:
        conn.close()


def test_unified_db_meta_records_total(tmp_path):
    _, _, detail_db, treemap_db = _build_unified_fixture(tmp_path)
    conn = _open_detail(detail_db, treemap_db)
    try:
        meta = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM meta")}
        assert int(meta["total_files"]) == 5
        assert int(meta["total_size"]) == 4096 + 2048 + 512 + 8192 + 1024
        assert meta["schema_version"] == "1"
    finally:
        conn.close()

    # treemap.db meta
    conn = sqlite3.connect(f"file:{treemap_db}?mode=ro", uri=True)
    try:
        meta = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM meta")}
        assert meta["schema_version"] == "1"
        assert int(meta["max_level"]) >= 1
    finally:
        conn.close()


def test_unified_db_outputs_deterministic_for_same_input(tmp_path):
    run1 = tmp_path / "run1"
    run2 = tmp_path / "run2"
    run1.mkdir(parents=True, exist_ok=True)
    run2.mkdir(parents=True, exist_ok=True)

    _, _, db1, _ = _build_unified_fixture(run1)
    _, _, db2, _ = _build_unified_fixture(run2)

    def totals(db_path):
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            users = sorted(
                conn.execute(
                    "SELECT username, total_files, total_size FROM users ORDER BY username"
                ).fetchall()
            )
            files_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
            return users, files_count
        finally:
            conn.close()

    assert totals(db1) == totals(db2)


def test_db_application_id_and_user_version(tmp_path):
    _, _, detail_db, treemap_db = _build_unified_fixture(tmp_path)

    def stamp(path, want_app_id):
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            app_id = conn.execute("PRAGMA application_id").fetchone()[0]
            if app_id < 0:
                app_id += 1 << 32
            assert app_id == want_app_id
            assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        finally:
            conn.close()

    stamp(detail_db, 0xC0DD15D1)
    stamp(treemap_db, 0xC0DD15C0)


def test_build_pipeline_signature_accepts_db_paths(tmp_path):
    """build_pipeline FFI accepts the new (detail_db, treemap_db) arg layout."""
    from src import fast_scanner

    args = (
        str(tmp_path / "missing_tmpdir"),  # tmpdir (won't exist → empty result)
        {},                                # uids_map
        {},                                # team_map
        str(tmp_path / "detail_users" / "data_detail.db"),
        str(tmp_path / "tree_map_data" / "treemap.db"),
        str(tmp_path),                     # treemap_root
        3,                                 # max_level
        0,                                 # min_size_bytes
        0,                                 # timestamp
        1,                                 # max_workers
    )
    for call_args in (args, (*args, False), (*args, False, False)):
        try:
            fast_scanner.build_pipeline(*call_args)
        except TypeError as exc:
            pytest.fail(f"build_pipeline rejected {len(call_args)} args: {exc}")
        except RuntimeError:
            pass


def test_export_user_reports_detects_and_exports_db(tmp_path):
    _build_unified_fixture(tmp_path)
    out_dir = tmp_path / "exports"
    out_dir.mkdir()

    assert export_user_reports.find_users(str(tmp_path), "") == ["alice", "bob"]

    expected_detail = str(tmp_path / "detail_users" / "data_detail.db")
    expected_treemap = str(tmp_path / "tree_map_data" / "treemap.db")
    assert export_user_reports.build_paths(str(tmp_path), "", "alice") == (
        expected_detail,
        expected_treemap,
        "",
    )

    exported = export_user_reports.export_user(
        "alice",
        str(tmp_path),
        str(out_dir),
        "",
        threading.Semaphore(1),
    )
    assert isinstance(exported, list)
    # Rust exporter writes one file per kind (dir + file) per user.
    assert len(exported) == 2
    assert sorted(exported) == sorted([
        str(out_dir / "usage_dir_alice.txt"),
        str(out_dir / "usage_file_alice.txt"),
    ])
    for path in exported:
        assert os.path.isfile(path)
        body = open(path, encoding="utf-8").read()
        assert "alice" in body
    file_body = open(out_dir / "usage_file_alice.txt", encoding="utf-8").read()
    assert "alpha.txt" in file_body


def test_cli_check_users_uses_detail_db(tmp_path):
    _build_unified_fixture(tmp_path)

    calls = []
    cli = CLIInterface()

    def capture(users, dir_files, file_files, top, **kwargs):
        calls.append((users, dir_files, file_files, top))

    cli.report_formatter.display_user_detail_reports = capture
    cli.display_check_users(["alice", "missing"], output_dir=str(tmp_path), top=7)

    assert len(calls) == 1
    users, dir_files, file_files, top = calls[0]
    assert users == ["alice", "missing"]
    assert top == 7
    expected = str(tmp_path / "detail_users" / "data_detail.db")
    assert dir_files["alice"] == expected
    assert file_files["alice"] == expected
    assert dir_files["missing"] == expected
    assert file_files["missing"] == expected


def test_unified_outputs_permission_issues_db(tmp_path):
    """permission_issues.db must contain the expected events with owner mapping."""
    import sqlite3

    _, _, detail_db, _ = _build_unified_fixture(tmp_path)
    perm_db = detail_db.parent.parent / "permission_issues.db"
    assert perm_db.exists(), "permission_issues.db should be written by Phase 2"

    conn = sqlite3.connect(f"file:{perm_db}?mode=ro", uri=True)
    try:
        total = conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
        assert total == 3

        rows = conn.execute(
            "SELECT user, item_type, error FROM issues ORDER BY user, path"
        ).fetchall()
    finally:
        conn.close()

    by_user = {r[0]: (r[1], r[2]) for r in rows}
    assert by_user["alice"] == ("directory", "EACCES")
    assert by_user["bob"] == ("file", "ENOENT")
    assert "__unknown__" in by_user
    assert by_user["__unknown__"] == ("directory", "EIO")


# ── Negative tests ─────────────────────────────────────────────────────────────

def test_export_find_users_graceful_on_missing_db(tmp_path):
    """find_users returns [] when no detail.db exists."""
    assert export_user_reports.find_users(str(tmp_path), "") == []


def test_export_find_users_graceful_on_corrupt_db(tmp_path):
    """find_users returns [] when detail.db is not a valid SQLite file."""
    detail_dir = tmp_path / "detail_users"
    detail_dir.mkdir()
    (detail_dir / "data_detail.db").write_bytes(b"not a sqlite file")
    assert export_user_reports.find_users(str(tmp_path), "") == []


def test_export_build_paths_graceful_on_missing_db(tmp_path):
    """build_paths returns ('', '', '') when no detail.db exists."""
    assert export_user_reports.build_paths(str(tmp_path), "", "alice") == ("", "", "")


def test_unified_scan_truncated_scan_bin_reports_zero_files(tmp_path):
    """Pipeline handles truncated scan_t*.bin gracefully — total_files = 0, no crash."""
    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")
        f.write((1000).to_bytes(4, "little"))
        f.write((100).to_bytes(8, "little"))
        f.write((99).to_bytes(4, "little"))

    cfg = {
        "directory": str(tmp_path),
        "output_file": str(tmp_path / "disk_usage_report.json"),
        "teams": [{"name": "backend", "team_id": "backend"}],
        "users": [{"name": "alice", "team_id": "backend"}],
    }
    scan_result = ScanResult(
        general_system={"total": 1, "used": 1, "available": 0},
        team_usage=[], user_usage=[], other_usage=[],
        timestamp=1234567890,
        detail_tmpdir=str(detail_tmpdir),
        detail_uid_username={1000: "alice"},
    )
    ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=False)

    detail_db = tmp_path / "detail_users" / "data_detail.db"
    assert detail_db.exists()
    conn = sqlite3.connect(f"file:{detail_db}?mode=ro", uri=True)
    try:
        meta = {r[0]: r[1] for r in conn.execute("SELECT key, value FROM meta")}
        assert int(meta["total_files"]) == 0
        assert int(meta["total_size"]) == 0
        # Truncated input has no decodable user rows.
        files_count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        assert files_count == 0
    finally:
        conn.close()


def test_unified_corrupt_permission_tsv_reports_zero_permission_issues(tmp_path):
    """Pipeline handles malformed perm_t*.tsv gracefully without crashing."""
    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")
        f.write((1000).to_bytes(4, "little"))
        f.write((100).to_bytes(8, "little"))
        p = b"alpha.txt"
        f.write(len(p).to_bytes(4, "little"))
        f.write(p)
    (detail_tmpdir / "perm_t1.tsv").write_text("not\ttab\tseparated\tand\ttotally\twrong\n", encoding="utf-8")

    cfg = {
        "directory": str(tmp_path),
        "output_file": str(tmp_path / "disk_usage_report.json"),
        "teams": [{"name": "backend", "team_id": "backend"}],
        "users": [{"name": "alice", "team_id": "backend"}],
    }
    scan_result = ScanResult(
        general_system={"total": 1, "used": 1, "available": 0},
        team_usage=[], user_usage=[], other_usage=[],
        timestamp=1234567890,
        detail_tmpdir=str(detail_tmpdir),
        detail_uid_username={1000: "alice"},
    )
    ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=False)
    # Permission DB exists but contains zero rows (malformed TSV is skipped).
    import sqlite3
    perm_db = tmp_path / "permission_issues.db"
    assert perm_db.exists()
    conn = sqlite3.connect(f"file:{perm_db}?mode=ro", uri=True)
    try:
        count = conn.execute("SELECT COUNT(*) FROM issues").fetchone()[0]
    finally:
        conn.close()
    assert count == 0
