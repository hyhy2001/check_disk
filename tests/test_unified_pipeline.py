import json
import os
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

    if not report_generator_module.HAS_RUST_PIPELINE:
        pytest.skip("fast_scanner.build_pipeline is not available")

    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        def write_bin(uid, size, path_str):
            f.write(b'\x01')
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

    created = ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1)
    detail_manifest = tmp_path / "detail_users" / "data_detail.json"
    return cfg, created, detail_manifest, tmp_path / "tree_map_report.json", tmp_path / "tree_map_data" / "manifest.json"


def test_unified_json_outputs_multi_user_ext_and_paths(tmp_path):
    _, _, detail_manifest, _, tree_manifest = _build_unified_fixture(tmp_path)

    detail = json.loads(detail_manifest.read_text(encoding="utf-8"))
    users = sorted(
        (u["username"], u["files"], u["dirs"], u["used"])
        for u in detail["users"]
    )
    assert users == [("alice", 3, 2, 6656), ("bob", 2, 2, 9216)]

    alice_dir = detail_manifest.parent / "users" / "alice"
    alice_manifest = json.loads((alice_dir / "manifest.json").read_text(encoding="utf-8"))
    alice_part = alice_manifest["files"]["parts"][0]["path"]
    alice_files = [
        json.loads(line)
        for line in (alice_dir / alice_part).read_text(encoding="utf-8").splitlines()
    ]
    assert [row["s"] for row in alice_files] == [4096, 2048, 512]
    assert [row["s"] for row in alice_files if row["x"] == "log"] == [2048]
    assert any(row["p"].endswith("alpha.txt") for row in alice_files)

    bob_dir = detail_manifest.parent / "users" / "bob"
    bob_dirs = [
        json.loads(line)
        for line in (bob_dir / "dirs.ndjson").read_text(encoding="utf-8").splitlines()
    ]
    assert sorted(row["s"] for row in bob_dirs) == [1024, 8192]

    tree = json.loads(tree_manifest.read_text(encoding="utf-8"))
    assert tree["shard_count"] >= 1


def test_build_pipeline_accepts_legacy_and_debug_signatures(tmp_path):
    from src import fast_scanner

    args = (
        str(tmp_path / "missing_tmpdir"),
        {},
        {},
        str(tmp_path / "detail_users" / "data_detail.json"),
        str(tmp_path / "tree.json"),
        str(tmp_path / "tree_map_data"),
        str(tmp_path),
        3,
        0,
        0,
        1,
    )
    for call_args in (args, (*args, False)):
        try:
            fast_scanner.build_pipeline(*call_args)
        except TypeError as exc:
            pytest.fail(f"build_pipeline rejected {len(call_args)} args: {exc}")
        except RuntimeError:
            pass


def test_export_user_reports_detects_and_exports_data_detail_manifest(tmp_path):
    _, _, detail_manifest, _, _ = _build_unified_fixture(tmp_path)
    out_dir = tmp_path / "exports"
    out_dir.mkdir()

    assert export_user_reports.find_users(str(tmp_path), "") == ["alice", "bob"]
    assert export_user_reports.build_paths(str(tmp_path), "", "alice") == (str(detail_manifest), "", "")

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


def test_cli_check_users_uses_data_detail_manifest(tmp_path):
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
    expected = str(tmp_path / "detail_users" / "data_detail.json")
    assert dir_files["alice"] == expected
    assert file_files["alice"] == expected
    assert dir_files["missing"] == expected
    assert file_files["missing"] == expected

def test_unified_json_outputs_permission_issues(tmp_path):
    _, _, detail_manifest, _, _ = _build_unified_fixture(tmp_path)
    perm_path = detail_manifest.parent.parent / "permission_issues.json"
    assert perm_path.exists()
    
    perm_data = json.loads(perm_path.read_text(encoding="utf-8"))
    issues = perm_data["permission_issues"]
    assert issues["count"] == 3
    
    users = sorted(issues["users"], key=lambda u: u["name"])
    assert len(users) == 2
    assert users[0]["name"] == "alice"
    assert users[0]["inaccessible_items"][0]["type"] == "directory"
    assert users[0]["inaccessible_items"][0]["error"] == "EACCES"
    
    assert users[1]["name"] == "bob"
    assert users[1]["inaccessible_items"][0]["type"] == "file"
    assert users[1]["inaccessible_items"][0]["error"] == "ENOENT"
    
    unknown = issues["unknown_items"]
    assert len(unknown) == 1
    assert unknown[0]["type"] == "directory"
    assert unknown[0]["error"] == "EIO"
