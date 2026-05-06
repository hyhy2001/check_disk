"""
Test suite for src/sync_manager.py

Kịch bản được test:
  1. Full sync        – tar stream đồng bộ nội dung local sang remote
  2. Tar codec fallback – ưu tiên xz khi có, nếu không dùng gzip
  3. ControlMaster    – nhiều lần sync dùng chung 1 socket
  4. AsyncSyncPipeline concurrency – enqueue nhiều file/dir không race condition
  5. _should_compress – unit test compatibility (tar-only không dùng)

Yêu cầu môi trường:
  - sshd chạy trên localhost:22 (PermitRootLogin yes)
  - ssh có trên PATH
  - Chạy với quyền root (hoặc user có thể tự ghi authorized_keys)
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

# Đảm bảo import được src/
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.sync_manager as sm  # noqa: E402
from src.sync_manager import (  # noqa: E402
    _CONTROL_SOCKETS,
    AsyncSyncPipeline,
    ReportSyncer,
    _should_compress,
)


# ─────────────────────────────────────────────────────────────────────────────
# Kiểm tra nhanh xem SSH localhost có khả dụng không
# ─────────────────────────────────────────────────────────────────────────────
def _ssh_available() -> bool:
    try:
        r = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=3", "-o", "BatchMode=yes",
             "root@localhost", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5,
        )
        return r.returncode == 0
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _remote_ls(remote_dir: str, key: str) -> set:
    """Trả về set tên file (không bao gồm thư mục) trong remote_dir."""
    r = subprocess.run(
        ["ssh", "-i", key, "-o", "IdentitiesOnly=yes",
         "-o", "StrictHostKeyChecking=accept-new",
         "root@localhost",
         f"find '{remote_dir}' -maxdepth 1 -type f -printf '%f\\n' 2>/dev/null || true"],
        capture_output=True, text=True, timeout=15,
    )
    return {line.strip() for line in r.stdout.splitlines() if line.strip()}


def _remote_read(remote_path: str, key: str) -> str:
    """Đọc nội dung file trên remote."""
    r = subprocess.run(
        ["ssh", "-i", key, "-o", "IdentitiesOnly=yes",
         "-o", "StrictHostKeyChecking=accept-new",
         "root@localhost", f"cat '{remote_path}'"],
        capture_output=True, text=True, timeout=15,
    )
    return r.stdout


def _remote_mtime(remote_path: str, key: str) -> str:
    """Lấy mtime (stat -c %Y) của file trên remote."""
    r = subprocess.run(
        ["ssh", "-i", key, "-o", "IdentitiesOnly=yes",
         "-o", "StrictHostKeyChecking=accept-new",
         "root@localhost", f"stat -c '%Y' '{remote_path}' 2>/dev/null || echo MISSING"],
        capture_output=True, text=True, timeout=10,
    )
    return r.stdout.strip()


def _remote_rm(remote_dir: str, key: str):
    """Xóa thư mục trên remote."""
    subprocess.run(
        ["ssh", "-i", key, "-o", "IdentitiesOnly=yes",
         "-o", "StrictHostKeyChecking=accept-new",
         "root@localhost", f"rm -rf '{remote_dir}'"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
    )


@contextmanager
def _patched_ssh(key: str):
    """
    Context manager: patch _build_ssh_base và _get_control_socket
    để inject -i <key> -o IdentitiesOnly=yes vào mọi lệnh SSH.
    """
    extra = ["-i", key, "-o", "IdentitiesOnly=yes",
             "-o", "StrictHostKeyChecking=accept-new"]

    orig_build = sm._build_ssh_base
    orig_socket = sm._get_control_socket

    def patched_build(user, host, password=None, control_socket=""):
        cmd, env = orig_build(user, host, password, control_socket)
        # Chèn extra args sau "ssh" (hoặc sau sshpass ... ssh)
        try:
            idx = cmd.index("ssh") + 1
        except ValueError:
            idx = 1
        cmd = cmd[:idx] + extra + cmd[idx:]
        return cmd, env

    def patched_socket(user, host, password=None):
        key_idx = ["-i", key, "-o", "IdentitiesOnly=yes",
                   "-o", "StrictHostKeyChecking=accept-new"]
        # Gọi original nhưng inject key vào master_cmd
        _orig_popen = subprocess.run

        def wrapped_run(cmd, **kwargs):
            if isinstance(cmd, list) and "ssh" in cmd and "-MNf" in cmd:
                try:
                    idx = cmd.index("-MNf") + 1
                except ValueError:
                    idx = 1
                cmd = cmd[:idx] + key_idx + cmd[idx:]
            return _orig_popen(cmd, **kwargs)

        with patch("subprocess.run", side_effect=wrapped_run):
            return orig_socket(user, host, password)

    with patch.object(sm, "_build_ssh_base", side_effect=patched_build), \
         patch.object(sm, "_get_control_socket", side_effect=patched_socket):
        yield


# ─────────────────────────────────────────────────────────────────────────────
# Base class – setup/teardown SSH keypair
# ─────────────────────────────────────────────────────────────────────────────
class SyncTestBase(unittest.TestCase):
    """
    setUpClass: tạo SSH keypair, inject public key vào authorized_keys.
    tearDownClass: xóa key, cleanup temp dirs.
    """

    SSH_KEY: str = ""          # private key path (set by setUpClass)
    _KEY_TMPDIR: str = ""      # dir chứa keypair
    _AUTH_KEYS: str = os.path.expanduser("~/.ssh/authorized_keys")
    _MARKER: str = "# test_sync_manager_tmpkey"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # Tạo keypair tạm
        cls._KEY_TMPDIR = tempfile.mkdtemp(prefix="sync_test_key_")
        cls.SSH_KEY = os.path.join(cls._KEY_TMPDIR, "id_ed25519_test")
        try:
            subprocess.run(
                ["ssh-keygen", "-t", "ed25519", "-N", "", "-f", cls.SSH_KEY, "-C", "sync_test"],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise unittest.SkipTest(f"ssh-keygen unavailable for sync integration tests: {exc}") from exc
        os.chmod(cls.SSH_KEY, 0o600)

        pub_key = Path(cls.SSH_KEY + ".pub").read_text().strip()

        # Inject vào authorized_keys. Read-only envs (sandbox/CI) → skip.
        try:
            os.makedirs(os.path.dirname(cls._AUTH_KEYS), exist_ok=True)
            with open(cls._AUTH_KEYS, "a") as f:
                f.write(f"\n{pub_key} {cls._MARKER}\n")
        except OSError as exc:
            raise unittest.SkipTest(
                f"authorized_keys not writable, skipping sync integration tests: {exc}"
            ) from exc

        # Đợi sshd nhận key (thường ngay lập tức)
        deadline = time.time() + 10
        while time.time() < deadline:
            r = subprocess.run(
                ["ssh", "-i", cls.SSH_KEY, "-o", "IdentitiesOnly=yes",
                 "-o", "StrictHostKeyChecking=accept-new",
                 "-o", "ConnectTimeout=3", "-o", "BatchMode=yes",
                 "root@localhost", "echo ok"],
                capture_output=True, text=True, timeout=5,
            )
            if r.returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise unittest.SkipTest(
                "Cannot SSH into localhost with test key; skipping sync integration tests."
            )

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

        # Xóa key test khỏi authorized_keys
        try:
            if os.path.exists(cls._AUTH_KEYS):
                lines = Path(cls._AUTH_KEYS).read_text().splitlines(keepends=True)
                filtered = [line for line in lines if cls._MARKER not in line]
                Path(cls._AUTH_KEYS).write_text("".join(filtered))
        except OSError:
            pass

        # Cleanup keypair dir
        if cls._KEY_TMPDIR and os.path.isdir(cls._KEY_TMPDIR):
            shutil.rmtree(cls._KEY_TMPDIR, ignore_errors=True)

    def setUp(self):
        # Mỗi test dùng remote dir riêng
        self._remote_tmp = f"/tmp/sync_test_{os.getpid()}_{id(self)}"
        subprocess.run(
            ["ssh", "-i", self.SSH_KEY, "-o", "IdentitiesOnly=yes",
             "-o", "StrictHostKeyChecking=accept-new",
             "root@localhost", f"mkdir -p '{self._remote_tmp}'"],
            check=True, timeout=10,
        )
        # Reset ControlMaster cache
        _CONTROL_SOCKETS.clear()

    def tearDown(self):
        _remote_rm(self._remote_tmp, self.SSH_KEY)
        # Dọn ControlMaster sockets
        sm._cleanup_control_dir()
        _CONTROL_SOCKETS.clear()


# ─────────────────────────────────────────────────────────────────────────────
# 1. Delta sync
# ─────────────────────────────────────────────────────────────────────────────
class TestTarSync(SyncTestBase):
    """Verify tar stream sync behavior."""

    def _make_local(self):
        d = tempfile.mkdtemp(prefix="sync_local_")
        Path(d, "a.json").write_text('{"v": 1}')
        Path(d, "b.json").write_text('{"v": 2}')
        Path(d, "c.ndjson").write_text('{"v": 3}\n')
        return d

    def test_initial_full_sync(self):
        local = self._make_local()
        try:
            with _patched_ssh(self.SSH_KEY):
                ok = ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )
            self.assertTrue(ok, "sync_to_remote phải trả về True")
            remote_files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertIn("a.json", remote_files)
            self.assertIn("b.json", remote_files)
            self.assertIn("c.ndjson", remote_files)
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_sync_overwrites_changed_file(self):
        local = self._make_local()
        try:
            with _patched_ssh(self.SSH_KEY):
                ReportSyncer.sync_to_remote(local, "root", "localhost", self._remote_tmp)

            Path(local, "a.json").write_text('{"v": 999}')
            with _patched_ssh(self.SSH_KEY):
                ok = ReportSyncer.sync_to_remote(local, "root", "localhost", self._remote_tmp)
            self.assertTrue(ok)

            content_a = _remote_read(f"{self._remote_tmp}/a.json", self.SSH_KEY)
            self.assertIn("999", content_a)
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_deleted_file_removed_on_remote(self):
        local = self._make_local()
        try:
            with _patched_ssh(self.SSH_KEY):
                ReportSyncer.sync_to_remote(local, "root", "localhost", self._remote_tmp)

            os.remove(os.path.join(local, "b.json"))
            with _patched_ssh(self.SSH_KEY):
                ReportSyncer.sync_to_remote(local, "root", "localhost", self._remote_tmp)

            remote_files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertNotIn("b.json", remote_files)
            self.assertIn("a.json", remote_files)
            self.assertIn("c.ndjson", remote_files)
        finally:
            shutil.rmtree(local, ignore_errors=True)


class TestFallbackChain(SyncTestBase):
    """Kiểm tra tar codec và sync file đơn."""

    def _make_local(self, content: str = '{"test": true}'):
        d = tempfile.mkdtemp(prefix="sync_fallback_")
        Path(d, "report.json").write_text(content)
        return d

    def test_tar_sync_runs_without_rsync(self):
        local = self._make_local()
        try:
            with _patched_ssh(self.SSH_KEY):
                ok = ReportSyncer.sync_to_remote(local, "root", "localhost", self._remote_tmp)
            self.assertTrue(ok)
            files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertIn("report.json", files)
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_xz_preferred_over_gzip_when_available(self):
        captured_logs = []

        def capture_print(*args, **kwargs):
            captured_logs.append(" ".join(str(a) for a in args))

        local = self._make_local('{"xz_test": 1}')
        try:
            with _patched_ssh(self.SSH_KEY), \
                 patch("builtins.print", side_effect=capture_print):
                ok = ReportSyncer.sync_to_remote(local, "root", "localhost", self._remote_tmp)
            self.assertTrue(ok)
            combined = "\n".join(captured_logs)
            self.assertTrue("tar+xz" in combined or "tar+gzip" in combined)
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_sync_file_tar_only(self):
        local = tempfile.mkdtemp(prefix="sync_tar_file_")
        fpath = os.path.join(local, "single.json")
        Path(fpath).write_text('{"tar": true}')
        try:
            with _patched_ssh(self.SSH_KEY):
                ok = ReportSyncer.sync_file_to_remote(
                    file_path=fpath,
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                    _capability_cache={"control_socket": ""},
                )
            self.assertTrue(ok)
            files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertIn("single.json", files)
        finally:
            shutil.rmtree(local, ignore_errors=True)


    def test_enqueue_multiple_files_all_synced(self):
        """10 file được enqueue → tất cả phải xuất hiện trên remote."""
        local = tempfile.mkdtemp(prefix="sync_pipe_")
        expected = {}
        for i in range(10):
            name = f"report_{i:02d}.json"
            content = json.dumps({"id": i, "data": "x" * 100})
            Path(local, name).write_text(content)
            expected[name] = content

        try:
            with _patched_ssh(self.SSH_KEY):
                pipeline = AsyncSyncPipeline(
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                )
                for name in expected:
                    pipeline.enqueue_file(os.path.join(local, name))
                pipeline.wait()
                pipeline.close()

            remote_files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            for name in expected:
                self.assertIn(name, remote_files, f"File {name} phải có trên remote")
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_enqueue_directory_batch(self):
        """enqueue_directory → toàn bộ 5 file trong dir phải sync đúng."""
        local = tempfile.mkdtemp(prefix="sync_dir_")
        sub = os.path.join(local, "reports")
        os.makedirs(sub)
        for i in range(5):
            Path(sub, f"r{i}.json").write_text(f'{{"n":{i}}}')

        # remote subdir tương ứng
        remote_sub = os.path.join(self._remote_tmp, "reports")

        try:
            with _patched_ssh(self.SSH_KEY):
                pipeline = AsyncSyncPipeline(
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                )
                pipeline.enqueue_directory(sub)
                pipeline.wait()
                pipeline.close()

            # Kiểm tra file có trong remote reports/
            r = subprocess.run(
                ["ssh", "-i", self.SSH_KEY, "-o", "IdentitiesOnly=yes",
                 "-o", "StrictHostKeyChecking=accept-new",
                 "root@localhost",
                 f"find '{remote_sub}' -type f -printf '%f\\n' 2>/dev/null || true"],
                capture_output=True, text=True, timeout=15,
            )
            remote_files = {line.strip() for line in r.stdout.splitlines() if line.strip()}
            for i in range(5):
                self.assertIn(f"r{i}.json", remote_files)
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_no_race_condition_concurrent_writes(self):
        """20 file được enqueue đồng thời (4 workers) → không file nào bị corrupt."""
        local = tempfile.mkdtemp(prefix="sync_race_")
        expected = {}
        for i in range(20):
            name = f"concurrent_{i:03d}.json"
            content = json.dumps({"id": i, "payload": "=" * 500})
            Path(local, name).write_text(content)
            expected[name] = content

        try:
            with _patched_ssh(self.SSH_KEY):
                pipeline = AsyncSyncPipeline(
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                )
                for name in expected:
                    pipeline.enqueue_file(os.path.join(local, name))
                pipeline.wait()
                pipeline.close()

            remote_files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            errors = []
            for name, expected_content in expected.items():
                if name not in remote_files:
                    errors.append(f"MISSING: {name}")
                    continue
                actual = _remote_read(f"{self._remote_tmp}/{name}", self.SSH_KEY)
                if actual.strip() != expected_content.strip():
                    errors.append(f"CORRUPT: {name}")

            self.assertEqual(
                errors, [],
                "Race condition hoặc corrupt file:\n" + "\n".join(errors)
            )
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_pipeline_close_waits_for_all_pending(self):
        """close() phải đợi cho đến khi tất cả pending futures hoàn tất."""
        local = tempfile.mkdtemp(prefix="sync_close_")
        names = []
        for i in range(5):
            n = f"close_test_{i}.json"
            Path(local, n).write_text(f'{{"close": {i}}}')
            names.append(n)

        try:
            with _patched_ssh(self.SSH_KEY):
                pipeline = AsyncSyncPipeline(
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                )
                for n in names:
                    pipeline.enqueue_file(os.path.join(local, n))
                # KHÔNG gọi wait(), chỉ gọi close() — phải block đến xong
                pipeline.close()

            remote_files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            for n in names:
                self.assertIn(n, remote_files, f"{n} phải có trên remote sau close()")
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_enqueue_nonexistent_file_is_ignored(self):
        """enqueue_file với path không tồn tại → không raise exception."""
        with _patched_ssh(self.SSH_KEY):
            pipeline = AsyncSyncPipeline(
                base_dir="/tmp",
                user="root",
                host="localhost",
                dest_dir=self._remote_tmp,
            )
            # Không exception
            pipeline.enqueue_file("/tmp/this_file_does_not_exist_xyz.json")
            pipeline.close()  # phải finish sạch sẽ


# ─────────────────────────────────────────────────────────────────────────────
# 5. _should_compress – pure unit tests (không cần SSH)
# ─────────────────────────────────────────────────────────────────────────────
class TestShouldCompress(unittest.TestCase):
    """Unit test cho _should_compress logic."""

    def test_compressible_extensions(self):
        for ext in [".json", ".tsv", ".csv", ".txt", ".ndjson", ".sql"]:
            fname = f"/tmp/testfile{ext}"
            # Tạo file tạm để stat không fail
            Path(fname).touch()
            try:
                result = _should_compress(fname)
                self.assertTrue(result, f"Extension {ext} phải được compress")
            finally:
                os.unlink(fname)

    def test_skip_already_compressed(self):
        for ext in [".gz", ".xz", ".zst", ".bz2", ".lz4", ".zip", ".tar"]:
            fname = f"/tmp/testfile{ext}"
            Path(fname).touch()
            try:
                result = _should_compress(fname)
                self.assertFalse(result, f"Extension {ext} đã compressed, không double-compress")
            finally:
                os.unlink(fname)

    def test_unknown_extension_small_file_no_compress(self):
        """File nhỏ với extension lạ → không compress."""
        with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
            f.write(b"small content")
            fname = f.name
        try:
            result = _should_compress(fname)
            self.assertFalse(result, "File nhỏ không nên compress")
        finally:
            os.unlink(fname)

    def test_unknown_extension_large_file_compress(self):
        """File >= 1 MB với extension lạ → nên compress."""
        with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
            f.write(b"\x00" * (1024 * 1024 + 1))  # 1MB + 1 byte
            fname = f.name
        try:
            result = _should_compress(fname)
            self.assertTrue(result, "File >= 1MB nên compress")
        finally:
            os.unlink(fname)

    def test_nonexistent_file_returns_false(self):
        """File không tồn tại → trả về False (không raise)."""
        result = _should_compress("/tmp/this_does_not_exist_abc123.bin")
        self.assertFalse(result)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Kiểm tra SSH trước khi chạy
    if not _ssh_available():
        print(
            "\n[SKIP] SSH localhost không khả dụng.\n"
            "Đảm bảo sshd chạy và authorized_keys được cấu hình đúng.\n"
        )
        sys.exit(0)
    unittest.main(verbosity=2)
