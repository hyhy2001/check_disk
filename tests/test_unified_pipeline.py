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

    detail = json.loads((detail_manifest.parent / "api" / "data_detail.min.json").read_text(encoding="utf-8"))
    paths_bin = detail_manifest.parent / "index_seed" / "paths.bin"
    with open(paths_bin, "rb") as fh:
        header = fh.read(12)
        count = int.from_bytes(header[8:12], "little")
        paths_dict = []
        if header[:4] == b"PTH1":
            for _ in range(count):
                n = int.from_bytes(fh.read(4), "little")
                paths_dict.append(fh.read(n).decode("utf-8"))
        else:
            assert header[:4] == b"HTAP"
            fh.read(12)
            offsets = [int.from_bytes(fh.read(8), "little") for _ in range(count + 1)]
            blob = fh.read(offsets[-1])
            for i in range(count):
                chunk = blob[offsets[i]:offsets[i + 1]]
                if chunk.endswith(b"\x00"):
                    chunk = chunk[:-1]
                paths_dict.append(chunk.decode("utf-8"))
    users = sorted(
        (u["username"], u["total_files"], u["total_dirs"], u["total_bytes"])
        for u in detail["users"]
    )
    assert users == [("alice", 3, 2, 6656), ("bob", 2, 2, 9216)]

    alice_dir = detail_manifest.parent / "users" / "alice"
    alice_manifest = json.loads((alice_dir / "manifest.json").read_text(encoding="utf-8"))
    alice_part = alice_manifest["files"]["parts"][0]["path"]
    alice_files = []
    with gzip.open(alice_dir / alice_part, "rb") as fh:
        header = fh.read(8)
        assert header[:4] == b"CDB4"
        while True:
            base = fh.read(14)
            if not base:
                break
            path_id, size, ext_len = struct.unpack("<IQH", base)
            ext = fh.read(ext_len).decode("utf-8")
            alice_files.append({"i": path_id, "s": size, "x": ext})
    assert [row["s"] for row in alice_files] == [4096, 2048, 512]
    assert [row["s"] for row in alice_files if row["x"] == "log"] == [2048]
    assert any(paths_dict[row["i"]].endswith("alpha.txt") for row in alice_files)

    bob_dir = detail_manifest.parent / "users" / "bob"
    bob_manifest = json.loads((bob_dir / "manifest.json").read_text(encoding="utf-8"))
    bob_dir_part = bob_manifest["dirs"]["parts"][0]["path"]
    bob_dirs = []
    with gzip.open(bob_dir / bob_dir_part, "rb") as fh:
        header = fh.read(8)
        assert header[:4] == b"CDB4"
        while True:
            rec = fh.read(12)
            if not rec:
                break
            path_id, used = struct.unpack("<Iq", rec)
            bob_dirs.append({"i": path_id, "s": used})
    assert sorted(row["s"] for row in bob_dirs) == [1024, 8192]

    tree = json.loads(tree_manifest.read_text(encoding="utf-8"))
    assert tree["schema"] == "check-disk-detail-treemap"
    assert tree["files"]["nodes.bin"]["records"] >= 1


def test_detail_and_treemap_manifests_include_checksum_and_records(tmp_path):
    _, _, _, detail_schema_manifest, _, tree_manifest = _build_unified_fixture(tmp_path)

    detail_meta = json.loads(detail_schema_manifest.read_text(encoding="utf-8"))
    assert detail_meta["schema"] == "check-disk-detail"
    assert detail_meta["checksum"] != "pending"
    assert detail_meta["files"]["agg/uid_totals.bin"]["records"] >= 1
    assert detail_meta["files"]["agg/dir_user_sizes.bin"]["records"] >= 1
    assert detail_meta["files"]["agg/perm_events.bin"]["records"] >= 1
    assert detail_meta["files"]["users/"]["record_encoding"] == "binary-v1"

    tree_meta = json.loads(tree_manifest.read_text(encoding="utf-8"))
    assert tree_meta["schema"] == "check-disk-detail-treemap"
    assert tree_meta["checksum"] != "pending"
    assert tree_meta["files"]["nodes.bin"]["records"] >= 1
    assert tree_meta["files"]["name_dict.bin"]["records"] >= 1


def test_manifest_checksum_is_deterministic_for_same_input(tmp_path):
    run1 = tmp_path / "run1"
    run2 = tmp_path / "run2"
    run1.mkdir(parents=True, exist_ok=True)
    run2.mkdir(parents=True, exist_ok=True)

    _, _, _, detail_schema_manifest_1, _, tree_manifest_1 = _build_unified_fixture(run1)
    _, _, _, detail_schema_manifest_2, _, tree_manifest_2 = _build_unified_fixture(run2)

    detail_1 = json.loads(detail_schema_manifest_1.read_text(encoding="utf-8"))
    detail_2 = json.loads(detail_schema_manifest_2.read_text(encoding="utf-8"))
    assert detail_1["checksum"] != "pending"
    assert detail_2["checksum"] != "pending"
    assert len(detail_1["checksum"]) == 8
    assert len(detail_2["checksum"]) == 8

    tree_1 = json.loads(tree_manifest_1.read_text(encoding="utf-8"))
    tree_2 = json.loads(tree_manifest_2.read_text(encoding="utf-8"))
    assert tree_1["checksum"] != "pending"
    assert tree_2["checksum"] != "pending"
    assert len(tree_1["checksum"]) == 8
    assert len(tree_2["checksum"]) == 8

    assert tree_1["files"]["nodes.bin"]["bytes"] == tree_2["files"]["nodes.bin"]["bytes"]
    assert detail_1["files"]["cols/doc.uid.bin"]["bytes"] == detail_2["files"]["cols/doc.uid.bin"]["bytes"]
    assert detail_1["files"]["cols/doc.uid.bin"]["records"] == detail_2["files"]["cols/doc.uid.bin"]["records"]
    assert detail_1["files"]["agg/uid_totals.bin"]["records"] == detail_2["files"]["agg/uid_totals.bin"]["records"]


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
    detail = json.loads((tmp_path / "detail_users" / "api" / "data_detail.min.json").read_text(encoding="utf-8"))
    assert detail["scan"]["total_files"] == 0


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
