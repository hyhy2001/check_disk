import os
import sqlite3
import sys
import threading
import time

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import disk_checker  # noqa: E402
from scripts import export_user_reports  # noqa: E402
from src.cli_interface import CLIInterface  # noqa: E402
from src.disk_scanner import ScanResult  # noqa: E402
from src.report_generator import ReportGenerator  # noqa: E402


class _FakeSyncPipeline:
    def __init__(self):
        self.paths = []

    def enqueue_file(self, path):
        self.paths.append(path)


def test_scan_status_heartbeat_syncs_periodically(tmp_path):
    pipeline = _FakeSyncPipeline()
    heartbeat = disk_checker._ScanStatusHeartbeat(
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

    if not report_generator_module.HAS_RUST_UNIFIED_DB:
        pytest.skip("fast_scanner.build_unified_dbs is not available")

    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    (detail_tmpdir / "dirs_t1.tsv").write_text(
        "\n".join(
            [
                f"{tmp_path}\t1000\t4096",
                f"{tmp_path / 'sub'}\t1000\t2048",
                f"{tmp_path / 'other'}\t1001\t8192",
                f"{tmp_path / 'other' / 'logs'}\t1001\t1024",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (detail_tmpdir / "uid_1000_t1_c1.tsv").write_text(
        "\n".join(
            [
                f"4096\t{tmp_path / 'alpha.txt'}",
                f"2048\t{tmp_path / 'sub' / 'shared.log'}",
                f"512\t{tmp_path / 'sub' / 'same.bin'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (detail_tmpdir / "uid_1001_t1_c1.tsv").write_text(
        "\n".join(
            [
                f"8192\t{tmp_path / 'other' / 'alpha.txt'}",
                f"1024\t{tmp_path / 'other' / 'logs' / 'shared.log'}",
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

    created = ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1)
    detail_db = tmp_path / "detail_users" / "data_detail.db"
    return cfg, created, detail_db, tmp_path / "tree_map_report.json", tmp_path / "tree_map_data.db"


def test_unified_db_schema_queries_multi_user_ext_and_paths(tmp_path):
    _, _, detail_db, _, _ = _build_unified_fixture(tmp_path)

    conn = sqlite3.connect(detail_db)
    try:
        users = conn.execute(
            "SELECT username, total_files, total_dirs, total_used FROM users ORDER BY username"
        ).fetchall()
        assert users == [("alice", 3, 2, 6144), ("bob", 2, 2, 9216)]

        alice_id = conn.execute("SELECT user_id FROM users WHERE username='alice'").fetchone()[0]
        bob_id = conn.execute("SELECT user_id FROM users WHERE username='bob'").fetchone()[0]
        log_ext_id = conn.execute("SELECT ext_id FROM ext_dict WHERE ext='log'").fetchone()[0]

        alice_files = conn.execute(
            "SELECT size FROM file_detail WHERE user_id=? ORDER BY size DESC",
            (alice_id,),
        ).fetchall()
        assert alice_files == [(4096,), (2048,), (512,)]

        alice_logs = conn.execute(
            "SELECT size FROM file_detail WHERE user_id=? AND ext_id=? ORDER BY size DESC",
            (alice_id, log_ext_id),
        ).fetchall()
        bob_logs = conn.execute(
            "SELECT size FROM file_detail WHERE user_id=? AND ext_id=? ORDER BY size DESC",
            (bob_id, log_ext_id),
        ).fetchall()
        assert alice_logs == [(2048,)]
        assert bob_logs == [(1024,)]

        basenames = conn.execute(
            "SELECT basename FROM basename_dict WHERE basename IN ('alpha.txt', 'shared.log') ORDER BY basename"
        ).fetchall()
        assert basenames == [("alpha.txt",), ("shared.log",)]

        dirs = conn.execute(
            "SELECT used FROM dirs WHERE user='bob' ORDER BY used DESC"
        ).fetchall()
        assert dirs == [(8192,), (1024,)]
    finally:
        conn.close()


def test_build_unified_dbs_accepts_legacy_and_debug_signatures(tmp_path):
    from src import fast_scanner

    args = (
        str(tmp_path / "missing_tmpdir"),
        {},
        {},
        str(tmp_path / "detail.db"),
        str(tmp_path / "tree.json"),
        str(tmp_path / "tree.db"),
        str(tmp_path),
        3,
        0,
        0,
        1,
    )
    for call_args in (args, (*args, False)):
        try:
            fast_scanner.build_unified_dbs(*call_args)
        except TypeError as exc:
            pytest.fail(f"build_unified_dbs rejected {len(call_args)} args: {exc}")
        except RuntimeError:
            pass


def test_export_user_reports_detects_and_exports_data_detail_db(tmp_path):
    _, _, detail_db, _, _ = _build_unified_fixture(tmp_path)
    out_dir = tmp_path / "exports"
    out_dir.mkdir()

    assert export_user_reports.find_users(str(tmp_path), "") == ["alice", "bob"]
    assert export_user_reports.build_paths(str(tmp_path), "", "alice") == (str(detail_db), "", "")

    exported = export_user_reports.export_user(
        "alice",
        str(tmp_path),
        str(out_dir),
        "",
        threading.Semaphore(1),
    )
    exported_names = sorted(os.path.basename(path) for path in exported)
    assert exported_names == ["usage_dir_alice.txt", "usage_file_alice.txt"]

    dir_text = (out_dir / "usage_dir_alice.txt").read_text(encoding="utf-8")
    file_text = (out_dir / "usage_file_alice.txt").read_text(encoding="utf-8")
    assert "alice" in dir_text
    assert "alice" in file_text
    assert "alpha.txt" in file_text
    assert "other/alpha.txt" not in file_text


def test_cli_check_users_uses_data_detail_db(tmp_path):
    _, _, _, _, _ = _build_unified_fixture(tmp_path)

    calls = []
    cli = CLIInterface()

    def capture(users, dir_files, file_files, top):
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
