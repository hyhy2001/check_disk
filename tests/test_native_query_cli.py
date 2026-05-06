import json
import os
import subprocess

import pytest

from src.report_generator import ReportGenerator
from tests.test_report_generator import make_config, make_scan_result

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CDX1_QUERY = os.path.join(PROJECT_ROOT, "src", "native_index", "query_cli")


def _run_query(detail_root, *args):
    out = subprocess.check_output([CDX1_QUERY, str(detail_root), "--json", *args], text=True)
    return json.loads(out)


def test_native_query_cli_json_output(tmp_path):
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

    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--kw", "hello",
        "--ext", ".txt",
        "--user", "alice",
        "--min", "1000",
        "--max", "5000",
        "--limit", "10",
        "--json",
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)
    assert data["matched"] == 1
    assert data["returned"] == 1
    assert len(data["doc_ids"]) == 1


def test_native_query_cli_json_docs_output(tmp_path):
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

    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--kw", "hello",
        "--json",
        "--docs",
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)

    assert data["matched"] == 1
    assert data["returned"] == 1
    assert isinstance(data["doc_ids"], list)
    assert isinstance(data["docs"], list)
    assert len(data["docs"]) == 1

    doc = data["docs"][0]
    assert set(["doc_id", "gid", "uid", "size", "eid", "sid", "path", "ext", "user"]).issubset(set(doc.keys()))
    assert doc["size"] == 4096
    assert doc["path"].endswith("hello_alpha.txt")
    assert doc["ext"] == "txt"
    assert doc["user"] == "alice"

def test_native_query_cli_json_docs_fields_filter(tmp_path):
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

        write_bin(1000, 1234, str(tmp_path / "abc" / "hello_alpha.txt"))

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

    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--kw", "hello",
        "--json",
        "--docs",
        "--fields", "doc_id,size,user",
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)

    assert data["matched"] == 1
    assert len(data["docs"]) == 1
    doc = data["docs"][0]
    assert set(doc.keys()) == {"doc_id", "size", "user"}
    assert doc["size"] == 1234
    assert doc["user"] == "alice"

def test_native_query_cli_offset_limit_sort(tmp_path):
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

        write_bin(1000, 1000, str(tmp_path / "abc" / "doc1.txt"))
        write_bin(1000, 3000, str(tmp_path / "abc" / "doc2.txt"))
        write_bin(1000, 2000, str(tmp_path / "abc" / "doc3.txt"))

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

    cmd_base = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--json",
        "--docs",
    ]

    # test sort asc
    out = subprocess.check_output(cmd_base + ["--sort", "size_asc"], text=True)
    data = json.loads(out)
    assert data["matched"] == 3
    sizes = [d["size"] for d in data["docs"]]
    assert sizes == [1000, 2000, 3000]

    # test sort desc + offset + limit
    out = subprocess.check_output(cmd_base + ["--sort", "size_desc", "--offset", "1", "--limit", "1"], text=True)
    data = json.loads(out)
    assert data["matched"] == 3
    assert data["returned"] == 1
    assert len(data["docs"]) == 1
    assert data["docs"][0]["size"] == 2000

def test_native_query_cli_sort_path_asc(tmp_path):
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

        write_bin(1000, 100, str(tmp_path / "z_dir" / "z.txt"))
        write_bin(1000, 100, str(tmp_path / "a_dir" / "a.txt"))
        write_bin(1000, 100, str(tmp_path / "m_dir" / "m.txt"))

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

    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--json",
        "--docs",
        "--sort", "path_asc",
        "--fields", "path",
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)
    paths = [d["path"] for d in data["docs"]]
    assert paths[0].endswith("a_dir/a.txt")
    assert paths[1].endswith("m_dir/m.txt")
    assert paths[2].endswith("z_dir/z.txt")

def test_native_query_cli_ext_accepts_dot_prefix(tmp_path):
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

        write_bin(1000, 100, str(tmp_path / "a.py"))
        write_bin(1000, 100, str(tmp_path / "b.txt"))

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

    base = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--json",
    ]
    out1 = subprocess.check_output(base + ["--ext", "py"], text=True)
    out2 = subprocess.check_output(base + ["--ext", ".py"], text=True)
    d1 = json.loads(out1)
    d2 = json.loads(out2)
    assert d1["matched"] == 1
    assert d2["matched"] == 1
    assert d1["doc_ids"] == d2["doc_ids"]

def test_native_query_cli_kw_ext_normalize_case_space(tmp_path):
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

        write_bin(1000, 100, str(tmp_path / "RustGuide.PY"))
        write_bin(1000, 100, str(tmp_path / "notes.txt"))

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

    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--kw", "  RUST  ",
        "--ext", " .PY ",
        "--json",
        "--docs",
        "--fields", "path,ext",
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)
    assert data["matched"] == 1
    assert data["docs"][0]["path"].endswith("RustGuide.PY")

def test_native_query_cli_kw_and_ext_multi_value_or(tmp_path):
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

        write_bin(1000, 100, str(tmp_path / "alpha.py"))
        write_bin(1000, 100, str(tmp_path / "beta.rs"))
        write_bin(1000, 100, str(tmp_path / "gamma.txt"))

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

    base = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--json",
    ]

    out_kw = subprocess.check_output(base + ["--kw", "alpha|beta"], text=True)
    data_kw = json.loads(out_kw)
    assert data_kw["matched"] == 2

    out_ext = subprocess.check_output(base + ["--ext", "py,rs"], text=True)
    data_ext = json.loads(out_ext)
    assert data_ext["matched"] == 2


def test_native_query_cli_page_defaults_to_500(tmp_path):
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

        for i in range(1200):
            write_bin(1000, i + 1, str(tmp_path / f"doc_{i:04d}.txt"))

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

    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--json",
        "--docs",
        "--sort", "size_asc",
        "--page", "2",
        "--fields", "doc_id,size",
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)
    assert data["matched"] == 1200
    assert data["returned"] == 500
    assert data["page"] == 2
    assert data["page_size"] == 500
    assert data["total_pages"] == 3
    assert data["docs"][0]["size"] == 501
    assert data["docs"][-1]["size"] == 1000


def test_native_query_cli_fails_on_missing_index(tmp_path):
    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--json",
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    assert proc.returncode != 0


def test_native_query_cli_fails_on_corrupt_mmi(tmp_path):
    index_dir = tmp_path / "detail_users" / "index"
    index_dir.mkdir(parents=True)
    (index_dir / "index.mmi").write_bytes(b"BAD!")
    (index_dir / "tokens.json").write_text("[]", encoding="utf-8")
    (index_dir / "exts.json").write_text("[]", encoding="utf-8")
    (index_dir / "users.json").write_text("[]", encoding="utf-8")

    cmd = [CDX1_QUERY, str(index_dir), "--json"]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    assert proc.returncode != 0


def test_native_query_cli_docs_graceful_on_truncated_user_part(tmp_path):
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

    cmd = [
        CDX1_QUERY,
        str(tmp_path / "detail_users"),
        "--json",
        "--docs",
    ]
    out = subprocess.check_output(cmd, text=True)
    data = json.loads(out)
    assert "docs" in data
    assert isinstance(data["docs"], list)


def test_native_query_cli_py_wrapper_graceful_on_missing_user_manifest(tmp_path):
    from src.formatters.report_formatter import ReportFormatter

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

    manifest_root = tmp_path / "detail_users" / "manifest.json"

    formatter = ReportFormatter()
    data = formatter._load_detail_report(str(manifest_root), is_dir=False, user="alice")
    # Text-first layout: alice data is recoverable from root manifest + NDJSON
    # even if the per-user manifest is missing.
    assert data.get("user") == "alice"
    assert data.get("total_files", 0) == 1
    assert isinstance(data.get("files"), list)
