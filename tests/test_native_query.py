import json
import os
import subprocess

import pytest

from src.report_generator import ReportGenerator
from src.native_query import IndexQuery
from tests.test_report_generator import make_config, make_scan_result

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CDX1_QUERY = os.path.join(PROJECT_ROOT, "src", "native_index", "query_cli")


def _run_query(detail_root, *args):
    out = subprocess.check_output([CDX1_QUERY, str(detail_root), "--json", *args], text=True)
    return json.loads(out)


def test_native_query_keyword_ext_size_user(tmp_path):
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

        write_bin(1000, 4096, str(tmp_path / "abc" / "hello_alpha.txt"))
        write_bin(1000, 2048, str(tmp_path / "abc" / "world_beta.log"))
        write_bin(1001, 8192, str(tmp_path / "def" / "hello_gamma.txt"))

    cfg = make_config(
        tmp_path,
        output_file=str(tmp_path / "disk_usage_report.json"),
        directory=str(tmp_path),
        users=[
            {"name": "alice", "team_id": "backend"},
            {"name": "bob", "team_id": "infra"},
        ],
    )
    scan_result = make_scan_result(tmp_path)
    scan_result.detail_tmpdir = str(detail_tmpdir)
    scan_result.detail_uid_username = {1000: "alice", 1001: "bob"}

    ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=False)

    detail_root = tmp_path / "detail_users"
    assert _run_query(detail_root, "--kw", "hello")["matched"] == 2
    assert _run_query(detail_root, "--kw", "hello", "--ext", ".txt")["matched"] == 2
    assert _run_query(detail_root, "--kw", "hello", "--ext", ".txt", "--user", "alice")["matched"] == 1
    assert _run_query(detail_root, "--ext", ".log", "--min", "2000", "--max", "3000")["matched"] == 1

    legacy_index = detail_root / "index" / "index.mmi"
    if legacy_index.exists():
        with IndexQuery(str(detail_root / "index")) as query:
            assert len(query.search(keywords=["hello"])) >= 1


def test_index_query_raises_when_index_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        IndexQuery(str(tmp_path / "detail_users" / "index"))


def test_text_query_graceful_on_missing_user_file_part(tmp_path):
    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")
        f.write((1000).to_bytes(4, "little"))
        f.write((10).to_bytes(8, "little"))
        p = b"abc.txt"
        f.write(len(p).to_bytes(4, "little"))
        f.write(p)

    cfg = make_config(
        tmp_path,
        output_file=str(tmp_path / "disk_usage_report.json"),
        directory=str(tmp_path),
        users=[{"name": "alice", "team_id": "backend"}],
    )
    scan_result = make_scan_result(tmp_path)
    scan_result.detail_tmpdir = str(detail_tmpdir)
    scan_result.detail_uid_username = {1000: "alice"}
    ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=False)

    detail_root = tmp_path / "detail_users"
    user_part = detail_root / "users" / "alice" / "files" / "chunk-00000" / "part-00000_files.ndjson"
    user_part.unlink()
    result = _run_query(detail_root, "--user", "alice")
    assert result["matched"] == 0


def test_text_query_graceful_on_corrupt_user_file_part(tmp_path):
    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")
        f.write((1000).to_bytes(4, "little"))
        f.write((10).to_bytes(8, "little"))
        p = b"abc.txt"
        f.write(len(p).to_bytes(4, "little"))
        f.write(p)

    cfg = make_config(
        tmp_path,
        output_file=str(tmp_path / "disk_usage_report.json"),
        directory=str(tmp_path),
        users=[{"name": "alice", "team_id": "backend"}],
    )
    scan_result = make_scan_result(tmp_path)
    scan_result.detail_tmpdir = str(detail_tmpdir)
    scan_result.detail_uid_username = {1000: "alice"}
    ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=False)

    detail_root = tmp_path / "detail_users"
    data_path = detail_root / "users" / "alice" / "files" / "chunk-00000" / "part-00000_files.ndjson"
    original = data_path.read_text(encoding="utf-8")
    data_path.write_text("{bad json\n" + original, encoding="utf-8")
    result = _run_query(detail_root, "--user", "alice")
    assert result["matched"] == 1


def test_index_query_raises_when_tokens_json_malformed(tmp_path):
    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")
        f.write((1000).to_bytes(4, "little"))
        f.write((10).to_bytes(8, "little"))
        p = b"abc.txt"
        f.write(len(p).to_bytes(4, "little"))
        f.write(p)

    cfg = make_config(
        tmp_path,
        output_file=str(tmp_path / "disk_usage_report.json"),
        directory=str(tmp_path),
        users=[{"name": "alice", "team_id": "backend"}],
    )
    scan_result = make_scan_result(tmp_path)
    scan_result.detail_tmpdir = str(detail_tmpdir)
    scan_result.detail_uid_username = {1000: "alice"}
    ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=False)

    index_dir = tmp_path / "detail_users" / "index"
    if not (index_dir / "index.mmi").exists():
        pytest.skip("legacy index.mmi not generated in text-first mode")

    (index_dir / "tokens.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        IndexQuery(str(index_dir))


def test_index_query_open_fails_on_corrupt_mmi(tmp_path):
    index_dir = tmp_path / "detail_users" / "index"
    index_dir.mkdir(parents=True)
    (index_dir / "index.mmi").write_bytes(b"BAD!")
    (index_dir / "tokens.json").write_text("[]", encoding="utf-8")
    (index_dir / "exts.json").write_text("[]", encoding="utf-8")
    (index_dir / "users.json").write_text("[]", encoding="utf-8")

    with pytest.raises(RuntimeError, match="cdx1_open failed"):
        IndexQuery(str(index_dir))


def test_index_query_open_fails_on_mismatched_doc_count_header(tmp_path):
    detail_tmpdir = tmp_path / "detail_tmp"
    detail_tmpdir.mkdir()
    with open(detail_tmpdir / "scan_t1.bin", "wb") as f:
        f.write(b"CDSKSEV1")
        f.write((1000).to_bytes(4, "little"))
        f.write((10).to_bytes(8, "little"))
        p = b"abc.txt"
        f.write(len(p).to_bytes(4, "little"))
        f.write(p)

    cfg = make_config(
        tmp_path,
        output_file=str(tmp_path / "disk_usage_report.json"),
        directory=str(tmp_path),
        users=[{"name": "alice", "team_id": "backend"}],
    )
    scan_result = make_scan_result(tmp_path)
    scan_result.detail_tmpdir = str(detail_tmpdir)
    scan_result.detail_uid_username = {1000: "alice"}
    ReportGenerator(cfg).generate_detail_reports(scan_result, max_workers=1, build_treemap=False)

    index_mmi = tmp_path / "detail_users" / "index" / "index.mmi"
    if not index_mmi.exists():
        pytest.skip("legacy index.mmi not generated in text-first mode")

    raw = bytearray(index_mmi.read_bytes())
    raw[8:12] = (0xFFFFFFFF).to_bytes(4, "little")
    index_mmi.write_bytes(raw)

    with pytest.raises(RuntimeError, match="cdx1_open failed"):
        IndexQuery(str(tmp_path / "detail_users" / "index"))
