import gzip
import json
import struct
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
    detail_manifest = tmp_path / "detail_users" / "manifest.json"
    detail_schema_manifest = tmp_path / "detail_users" / "manifest.json"
    return (
        cfg,
        created,
        detail_manifest,
        detail_schema_manifest,
        tmp_path / "tree_map_report.json",
        tmp_path / "tree_map_data" / "manifest.json",
    )


def test_unified_json_outputs_multi_user_ext_and_paths(tmp_path):
    _, _, detail_manifest, _, _, tree_manifest = _build_unified_fixture(tmp_path)

    detail_root = detail_manifest.parent

    detail_summary = json.loads((detail_root / "data_detail.json").read_text(encoding="utf-8"))
    assert detail_summary["schema"] == "check-disk-detail"
    users_summary = {u["username"]: u for u in detail_summary["users"]}
    assert users_summary["alice"]["files"] == 3
    assert users_summary["alice"]["used"] == 6656
    assert users_summary["bob"]["files"] == 2
    assert users_summary["bob"]["used"] == 9216

    alice_user_dir = detail_root / "users" / "alice"
    alice_manifest = json.loads((alice_user_dir / "manifest.json").read_text(encoding="utf-8"))
    assert alice_manifest["schema"] == "check-disk-user"
    assert alice_manifest["summary"]["files"] == 3
    assert alice_manifest["summary"]["used"] == 6656

    path_dict = {}
    with open(detail_root / "api" / "path_dict.ndjson", "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            path_dict[row["gid"]] = row["p"]

    alice_file_parts = alice_manifest["files"]["parts"]
    assert len(alice_file_parts) >= 1
    alice_files = []
    for part in alice_file_parts:
        chunk_path = alice_user_dir / part["path"]
        with open(chunk_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                alice_files.append({
                    "path": path_dict.get(row["gid"], ""),
                    "size": row["s"],
                    "ext": row["x"],
                })
    assert [f["size"] for f in alice_files] == [4096, 2048, 512]
    assert [f["size"] for f in alice_files if f["ext"] == "log"] == [2048]
    assert any(f["path"].endswith("alpha.txt") for f in alice_files)

    bob_user_dir = detail_root / "users" / "bob"
    bob_manifest = json.loads((bob_user_dir / "manifest.json").read_text(encoding="utf-8"))
    assert bob_manifest["summary"]["files"] == 2
    bob_dir_parts = bob_manifest["dirs"]["parts"]
    bob_dirs = []
    for part in bob_dir_parts:
        chunk_path = bob_user_dir / part["path"]
        with open(chunk_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                bob_dirs.append(row["s"])
    assert sorted(bob_dirs) == [1024, 8192]

    tree = json.loads(tree_manifest.read_text(encoding="utf-8"))
    assert tree["schema"] == "check-disk-detail-treemap"
    files_meta = tree.get("files", {})
    assert "api/shards_manifest.json" in files_meta
    assert "shards" in files_meta

    assert not (detail_root / "index_seed").exists()
    assert not (detail_root / "index").exists()


def test_unified_text_manifest_includes_expected_records(tmp_path):
    _, _, detail_manifest, _, _, tree_manifest = _build_unified_fixture(tmp_path)

    detail_meta = json.loads(detail_manifest.read_text(encoding="utf-8"))
    assert detail_meta["schema"] == "check-disk-detail"
    assert isinstance(detail_meta.get("users"), list)
    assert len(detail_meta["users"]) >= 2

    detail_summary = json.loads((detail_manifest.parent / "data_detail.json").read_text(encoding="utf-8"))
    assert detail_summary["scan"]["total_files"] == 5
    assert detail_summary["scan"]["total_size"] == (4096 + 2048 + 512 + 8192 + 1024)

    tree_meta = json.loads(tree_manifest.read_text(encoding="utf-8"))
    assert tree_meta["schema"] == "check-disk-detail-treemap"
    files_meta = tree_meta.get("files", {})
    assert files_meta["api/shards_manifest.json"]["records"] == 1
    assert files_meta["shards"]["records"] >= 1


def test_unified_text_manifest_is_deterministic_for_same_input(tmp_path):
    run1 = tmp_path / "run1"
    run2 = tmp_path / "run2"
    run1.mkdir(parents=True, exist_ok=True)
    run2.mkdir(parents=True, exist_ok=True)

    _, _, detail_manifest_1, _, _, tree_manifest_1 = _build_unified_fixture(run1)
    _, _, detail_manifest_2, _, _, tree_manifest_2 = _build_unified_fixture(run2)

    detail_1 = json.loads(detail_manifest_1.read_text(encoding="utf-8"))
    detail_2 = json.loads(detail_manifest_2.read_text(encoding="utf-8"))
    assert detail_1["schema"] == detail_2["schema"] == "check-disk-detail"
    assert len(detail_1.get("users", [])) == len(detail_2.get("users", []))

    summary_1 = json.loads((detail_manifest_1.parent / "data_detail.json").read_text(encoding="utf-8"))
    summary_2 = json.loads((detail_manifest_2.parent / "data_detail.json").read_text(encoding="utf-8"))
    assert summary_1["scan"]["total_files"] == summary_2["scan"]["total_files"] == 5
    assert summary_1["scan"]["total_size"] == summary_2["scan"]["total_size"]

    tree_1 = json.loads(tree_manifest_1.read_text(encoding="utf-8"))
    tree_2 = json.loads(tree_manifest_2.read_text(encoding="utf-8"))
    assert tree_1["schema"] == tree_2["schema"] == "check-disk-detail-treemap"
    files_1 = tree_1.get("files", {})
    files_2 = tree_2.get("files", {})
    assert files_1["api/shards_manifest.json"]["records"] == files_2["api/shards_manifest.json"]["records"] == 1
    assert files_1["shards"]["records"] == files_2["shards"]["records"]


def test_detail_and_treemap_manifests_include_checksum_and_records(tmp_path):
    test_unified_text_manifest_includes_expected_records(tmp_path)


def test_manifest_checksum_is_deterministic_for_same_input(tmp_path):
    test_unified_text_manifest_is_deterministic_for_same_input(tmp_path)



def test_build_pipeline_accepts_legacy_and_debug_signatures(tmp_path):
    from src import fast_scanner

    args = (
        str(tmp_path / "missing_tmpdir"),
        {},
        {},
        str(tmp_path / "detail_users" / "manifest.json"),
        str(tmp_path / "tree.json"),
        str(tmp_path / "tree_map_data"),
        str(tmp_path),
        3,
        0,
        0,
        1,
    )
    for call_args in (args, (*args, False), (*args, False, False)):
        try:
            fast_scanner.build_pipeline(*call_args)
        except TypeError as exc:
            pytest.fail(f"build_pipeline rejected {len(call_args)} args: {exc}")
        except RuntimeError:
            pass


def test_export_user_reports_detects_and_exports_detail_manifest(tmp_path):
    _, _, _, _, _, _ = _build_unified_fixture(tmp_path)
    out_dir = tmp_path / "exports"
    out_dir.mkdir()

    assert export_user_reports.find_users(str(tmp_path), "") == ["alice", "bob"]
    expected_manifest = tmp_path / "detail_users" / "manifest.json"
    assert export_user_reports.build_paths(str(tmp_path), "", "alice") == (str(expected_manifest), "", "")

    exported = export_user_reports.export_user(
        "alice",
        str(tmp_path),
        str(out_dir),
        "",
        threading.Semaphore(1),
    )
    assert isinstance(exported, list)
    assert all(isinstance(path, str) for path in exported)


def test_cli_check_users_uses_detail_manifest(tmp_path):
    _, _, _, _, _, _ = _build_unified_fixture(tmp_path)

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
    expected = str(tmp_path / "detail_users" / "manifest.json")
    assert dir_files["alice"] == expected
    assert file_files["alice"] == expected
    assert dir_files["missing"] == expected
    assert file_files["missing"] == expected


def test_unified_json_outputs_permission_issues(tmp_path):
    _, _, detail_manifest, _, _, _ = _build_unified_fixture(tmp_path)
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




# ── Negative tests: corrupt / truncated / malformed inputs ─────────────────────

def test_export_find_users_graceful_on_missing_manifest(tmp_path):
    """find_users returns [] (not crash) when no manifest exists."""
    assert export_user_reports.find_users(str(tmp_path), "") == []


def test_export_find_users_graceful_on_malformed_manifest_json(tmp_path):
    """find_users returns [] (not crash) when manifest JSON is malformed."""
    detail_dir = tmp_path / "detail_users"
    detail_dir.mkdir()
    (detail_dir / "manifest.json").write_text("not valid json {{{", encoding="utf-8")
    assert export_user_reports.find_users(str(tmp_path), "") == []


def test_export_build_paths_graceful_on_missing_manifest(tmp_path):
    """build_paths returns ('', '', '') when no manifest exists."""
    assert export_user_reports.build_paths(str(tmp_path), "", "alice") == ("", "", "")


def test_manifest_missing_users_field_returns_empty_list(tmp_path):
    """_users_from_detail_manifest returns [] when manifest has no 'users' key."""
    detail_dir = tmp_path / "detail_users"
    detail_dir.mkdir()
    (detail_dir / "manifest.json").write_text(json.dumps({"schema": "check-disk-detail"}), encoding="utf-8")
    result = export_user_reports._users_from_detail_manifest(str(detail_dir / "manifest.json"))
    assert result == []


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
    detail = json.loads((tmp_path / "detail_users" / "manifest.json").read_text(encoding="utf-8"))
    assert detail.get("users", []) == []

    summary = json.loads((tmp_path / "detail_users" / "data_detail.json").read_text(encoding="utf-8"))
    assert summary["scan"]["total_files"] == 0
    assert summary["scan"]["total_size"] == 0


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
    perm_path = tmp_path / "permission_issues.json"
    assert perm_path.exists()
    perm_data = json.loads(perm_path.read_text(encoding="utf-8"))
    assert isinstance(perm_data, dict)
