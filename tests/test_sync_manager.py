"""
Test suite for src/sync_manager.py

Kịch bản được test:
  1. Delta sync       – chỉ sync file thay đổi
  2. Fallback chain   – rsync → tar+xz/gzip → scp
  3. ControlMaster    – nhiều lần sync dùng chung 1 socket
  4. AsyncSyncPipeline concurrency – enqueue nhiều file/dir không race condition
  5. _should_compress – unit test, không cần SSH

Yêu cầu môi trường:
  - sshd chạy trên localhost:22 (PermitRootLogin yes)
  - rsync, ssh, scp có trên PATH
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
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-N", "", "-f", cls.SSH_KEY, "-C", "sync_test"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        os.chmod(cls.SSH_KEY, 0o600)

        pub_key = Path(cls.SSH_KEY + ".pub").read_text().strip()

        # Inject vào authorized_keys
        os.makedirs(os.path.dirname(cls._AUTH_KEYS), exist_ok=True)
        with open(cls._AUTH_KEYS, "a") as f:
            f.write(f"\n{pub_key} {cls._MARKER}\n")

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
            raise RuntimeError(
                "Không thể SSH vào localhost bằng test key. "
                "Hãy kiểm tra sshd và authorized_keys."
            )

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

        # Xóa key test khỏi authorized_keys
        if os.path.exists(cls._AUTH_KEYS):
            lines = Path(cls._AUTH_KEYS).read_text().splitlines(keepends=True)
            filtered = [line for line in lines if cls._MARKER not in line]
            Path(cls._AUTH_KEYS).write_text("".join(filtered))

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
class TestDeltaSync(SyncTestBase):
    """Verify rsync chỉ ghi các file thực sự thay đổi."""

    def _make_local(self):
        d = tempfile.mkdtemp(prefix="sync_local_")
        Path(d, "a.json").write_text('{"v": 1}')
        Path(d, "b.json").write_text('{"v": 2}')
        Path(d, "c.ndjson").write_text('{"v": 3}\n')
        return d

    def test_initial_full_sync(self):
        """Lần sync đầu tiên phải đẩy đủ tất cả file."""
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
            # Kiểm tra nội dung
            self.assertIn('"v": 1', _remote_read(f"{self._remote_tmp}/a.json", self.SSH_KEY))
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_delta_only_changed_file(self):
        """Lần sync thứ 2, chỉ có a.json thay đổi → b.json và c.ndjson không bị ghi lại."""
        local = self._make_local()
        try:
            with _patched_ssh(self.SSH_KEY):
                ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )

            # Lưu mtime của b.json và c.ndjson TRƯỚC khi sync lần 2
            mtime_b_before = _remote_mtime(f"{self._remote_tmp}/b.json", self.SSH_KEY)
            mtime_c_before = _remote_mtime(f"{self._remote_tmp}/c.ndjson", self.SSH_KEY)

            # Đợi ít nhất 1 giây để mtime có thể thay đổi nếu file bị ghi lại
            time.sleep(1.2)

            # Thay đổi chỉ a.json
            Path(local, "a.json").write_text('{"v": 999}')

            with _patched_ssh(self.SSH_KEY):
                ok = ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )
            self.assertTrue(ok)

            # a.json phải được update
            content_a = _remote_read(f"{self._remote_tmp}/a.json", self.SSH_KEY)
            self.assertIn("999", content_a, "a.json phải chứa nội dung mới")

            # b.json và c.ndjson KHÔNG được thay đổi (rsync --inplace)
            mtime_b_after = _remote_mtime(f"{self._remote_tmp}/b.json", self.SSH_KEY)
            mtime_c_after = _remote_mtime(f"{self._remote_tmp}/c.ndjson", self.SSH_KEY)
            self.assertEqual(
                mtime_b_before, mtime_b_after,
                "b.json không thay đổi nên mtime không được thay đổi (delta sync)"
            )
            self.assertEqual(
                mtime_c_before, mtime_c_after,
                "c.ndjson không thay đổi nên mtime không được thay đổi (delta sync)"
            )
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_deleted_file_removed_on_remote(self):
        """File bị xóa ở local phải biến mất trên remote (--delete flag)."""
        local = self._make_local()
        try:
            with _patched_ssh(self.SSH_KEY):
                ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )
            # Xóa b.json ở local
            os.remove(os.path.join(local, "b.json"))

            with _patched_ssh(self.SSH_KEY):
                ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )

            remote_files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertNotIn(
                "b.json", remote_files,
                "b.json đã bị xóa ở local phải biến mất trên remote"
            )
            self.assertIn("a.json", remote_files)
            self.assertIn("c.ndjson", remote_files)
        finally:
            shutil.rmtree(local, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Fallback chain
# ─────────────────────────────────────────────────────────────────────────────
class TestFallbackChain(SyncTestBase):
    """Kiểm tra rsync → tar+xz/gzip → scp fallback."""

    def _make_local(self, content: str = '{"test": true}'):
        d = tempfile.mkdtemp(prefix="sync_fallback_")
        Path(d, "report.json").write_text(content)
        return d

    def test_no_rsync_falls_back_to_tar(self):
        """Khi rsync không có trên local → fallback sang tar+gzip/xz vẫn sync được."""
        local = self._make_local()
        # Lưu tham chiếu thật trước khi patch để tránh RecursionError
        _real_which = shutil.which
        try:
            with _patched_ssh(self.SSH_KEY), \
                 patch("shutil.which", side_effect=lambda x: None if x == "rsync" else _real_which(x)), \
                 patch("src.sync_manager.shutil.which", side_effect=lambda x: None if x == "rsync" else _real_which(x)):
                ok = ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )
            self.assertTrue(ok, "Fallback tar phải thành công")
            files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertIn("report.json", files)
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_xz_preferred_over_gzip_when_available(self):
        """Nếu xz có → dùng xz; nếu không có → dùng gzip. Cả hai đều sync thành công."""
        # Lưu tham chiếu thật trước khi patch để tránh RecursionError
        _real_which = shutil.which
        captured_logs = []

        def capture_print(*args, **kwargs):
            captured_logs.append(" ".join(str(a) for a in args))

        local = self._make_local('{"xz_test": 1}')
        try:
            with _patched_ssh(self.SSH_KEY), \
                 patch("src.sync_manager.shutil.which", side_effect=lambda x: None if x == "rsync" else _real_which(x)), \
                 patch("builtins.print", side_effect=capture_print):
                ok = ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )
            self.assertTrue(ok, "sync phải thành công dù không có rsync")
            combined = "\n".join(captured_logs)
            # Phải log codec được dùng (xz hoặc gzip)
            self.assertTrue(
                "tar+xz" in combined or "tar+gzip" in combined,
                f"Phải log codec được dùng, nhưng log là:\n{combined}"
            )
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_sync_file_no_rsync_falls_back_to_scp(self):
        """sync_file_to_remote: rsync không có → dùng scp.

        scp trong sync_manager không đi qua _build_ssh_base (chỉ dùng _ssh_control_args).
        Ta cần inject SSH key trực tiếp vào module-level subprocess.run bằng monkey-patch
        (không dùng unittest.mock.patch vì sync_manager bind `subprocess` trực tiếp).
        """
        local = tempfile.mkdtemp(prefix="sync_scp_")
        fpath = os.path.join(local, "single.json")
        Path(fpath).write_text('{"scp": true}')
        _real_which = shutil.which
        ssh_key = self.SSH_KEY
        key_args = ["-i", ssh_key, "-o", "IdentitiesOnly=yes",
                    "-o", "StrictHostKeyChecking=accept-new"]

        orig_run = sm.subprocess.run

        def inject_key_run(cmd, **kwargs):
            if isinstance(cmd, list) and len(cmd) > 0:
                if cmd[0] == "scp":
                    # Inject key + -O (OpenSSH >= 9 cần -O cho legacy SCP protocol)
                    cmd = ["scp", "-O"] + key_args + cmd[1:]
                elif cmd[0] == "ssh" and "-MNf" not in cmd:
                    cmd = ["ssh"] + key_args + cmd[1:]
            return orig_run(cmd, **kwargs)

        sm.subprocess.run = inject_key_run
        try:
            cap = {"has_rsync": False, "control_socket": ""}
            with patch("src.sync_manager.shutil.which",
                       side_effect=lambda x: None if x == "rsync" else _real_which(x)):
                ok = ReportSyncer.sync_file_to_remote(
                    file_path=fpath,
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                    _capability_cache=cap,
                )
            self.assertTrue(ok, "scp fallback phải thành công")
            files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertIn("single.json", files)
            content = _remote_read(f"{self._remote_tmp}/single.json", self.SSH_KEY)
            self.assertIn('"scp": true', content)
        finally:
            sm.subprocess.run = orig_run
            shutil.rmtree(local, ignore_errors=True)

    def test_rsync_available_is_used_first(self):
        """Khi rsync có sẵn, phải dùng rsync (không phải tar)."""
        if not shutil.which("rsync"):
            self.skipTest("rsync không có trên máy này")
        local = self._make_local('{"rsync_first": 1}')
        captured = []

        try:
            original_print = print
        except Exception:
            original_print = print

        def cap_print(*args, **kwargs):
            captured.append(" ".join(str(a) for a in args))
            original_print(*args, **kwargs)

        try:
            with _patched_ssh(self.SSH_KEY), \
                 patch("builtins.print", side_effect=cap_print):
                ok = ReportSyncer.sync_to_remote(
                    local, "root", "localhost", self._remote_tmp
                )
            self.assertTrue(ok)
            combined = "\n".join(captured)
            self.assertIn("rsync", combined.lower(), "Log phải đề cập rsync")
            # tar+xz/gzip không được dùng khi rsync thành công
            self.assertNotIn("tar+xz", combined)
            self.assertNotIn("tar+gzip", combined)
        finally:
            shutil.rmtree(local, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# 3. ControlMaster reuse
# ─────────────────────────────────────────────────────────────────────────────
class TestControlMasterReuse(SyncTestBase):
    """Verify nhiều lần sync dùng chung 1 SSH ControlMaster socket."""

    def test_socket_created_only_once_for_same_host(self):
        """3 lần sync file tới cùng host → _CONTROL_SOCKETS chỉ có 1 entry."""
        local = tempfile.mkdtemp(prefix="sync_cm_")
        files = []
        for i in range(3):
            p = os.path.join(local, f"f{i}.json")
            Path(p).write_text(f'{{"i": {i}}}')
            files.append(p)

        call_count = {"n": 0}
        orig_socket = sm._get_control_socket

        def counting_socket(user, host, password=None):
            call_count["n"] += 1
            return orig_socket(user, host, password)

        try:
            with _patched_ssh(self.SSH_KEY):
                # Probe capability (tạo socket lần 1)
                pipeline = AsyncSyncPipeline(
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                )
                # 3 lần enqueue
                for f in files:
                    pipeline.enqueue_file(f)
                pipeline.wait()
                pipeline.close()

            # Phải chỉ có 1 entry trong cache cho localhost
            self.assertEqual(
                len([k for k in _CONTROL_SOCKETS if k[1] == "localhost"]),
                1,
                "_CONTROL_SOCKETS phải chỉ có 1 entry cho localhost"
            )
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_ssh_master_process_started_only_once(self):
        """Nhiều lần sync tới cùng host → chỉ 1 lần start SSH master (-MNf)."""
        local = tempfile.mkdtemp(prefix="sync_master_")
        for i in range(5):
            Path(local, f"file{i}.json").write_text(f'{{"n": {i}}}')

        master_starts = {"count": 0}
        orig_run = subprocess.run

        def spy_run(cmd, **kwargs):
            if isinstance(cmd, list) and "-MNf" in cmd:
                master_starts["count"] += 1
            return orig_run(cmd, **kwargs)

        try:
            with patch("subprocess.run", side_effect=spy_run), \
                 _patched_ssh(self.SSH_KEY):
                pipeline = AsyncSyncPipeline(
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                )
                for entry in os.scandir(local):
                    pipeline.enqueue_file(entry.path)
                pipeline.close()

            self.assertLessEqual(
                master_starts["count"], 1,
                f"SSH master (-MNf) chỉ được start tối đa 1 lần, thực tế: {master_starts['count']}"
            )
        finally:
            shutil.rmtree(local, ignore_errors=True)

    def test_stale_socket_triggers_reconnect(self):
        """Socket file tồn tại nhưng master chết → tự tạo lại socket mới và sync thành công.

        Chiến lược: inject SSH key vào TẤT CẢ lệnh ssh/rsync/scp qua subprocess.run
        (không qua _patched_ssh vì ta cần _get_control_socket chạy thật để test reconnect).
        """
        local = tempfile.mkdtemp(prefix="sync_stale_")
        Path(local, "data.json").write_text('{"stale": 1}')

        # Tạo socket file rỗng để giả lập stale ControlMaster
        fake_ctl_dir = tempfile.mkdtemp(prefix="sync_fake_ctl_")
        fake_sock = os.path.join(fake_ctl_dir, "root@localhost.sock")
        Path(fake_sock).touch()  # file rỗng → "ssh -O check" sẽ trả về lỗi

        _real_run = subprocess.run
        _real_popen = subprocess.Popen
        ssh_key = self.SSH_KEY
        extra = ["-i", ssh_key, "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=accept-new"]

        def inject_key_run(cmd, **kwargs):
            if isinstance(cmd, list) and len(cmd) > 0:
                if cmd[0] in ("ssh", "scp") or (len(cmd) > 1 and cmd[1] == "ssh"):
                    try:
                        idx = next(i for i, c in enumerate(cmd) if c == "ssh") + 1
                    except StopIteration:
                        idx = 1
                    cmd = cmd[:idx] + extra + cmd[idx:]
                elif cmd[0] == "rsync":
                    # Inject SSH command với key cho rsync -e flag
                    ssh_e_val = f"ssh {' '.join(extra)}"
                    # Tìm -e arg hiện tại và update
                    new_cmd = []
                    i = 0
                    replaced = False
                    while i < len(cmd):
                        if cmd[i] == "-e" and i + 1 < len(cmd):
                            new_cmd += ["-e", ssh_e_val]
                            i += 2
                            replaced = True
                        else:
                            new_cmd.append(cmd[i])
                            i += 1
                    if not replaced:
                        new_cmd = cmd[:1] + ["-e", ssh_e_val] + cmd[1:]
                    cmd = new_cmd
            return _real_run(cmd, **kwargs)

        def inject_key_popen(cmd, **kwargs):
            if isinstance(cmd, list) and cmd and cmd[0] in ("tar", "rsync"):
                pass  # tar không cần key
            return _real_popen(cmd, **kwargs)

        try:
            _CONTROL_SOCKETS[("root", "localhost")] = fake_sock
            sm._CONTROL_DIR = fake_ctl_dir

            with patch("src.sync_manager.subprocess.run", side_effect=inject_key_run), \
                 patch("src.sync_manager.subprocess.Popen", side_effect=inject_key_popen):
                ok = ReportSyncer.sync_file_to_remote(
                    file_path=os.path.join(local, "data.json"),
                    base_dir=local,
                    user="root",
                    host="localhost",
                    dest_dir=self._remote_tmp,
                )
            # Sync phải thành công mặc dù socket cũ bị stale
            self.assertTrue(ok, "Sync phải tự recover khi socket stale")
            files = _remote_ls(self._remote_tmp, self.SSH_KEY)
            self.assertIn("data.json", files, "File phải xuất hiện trên remote sau khi recover")
        finally:
            shutil.rmtree(local, ignore_errors=True)
            shutil.rmtree(fake_ctl_dir, ignore_errors=True)
            sm._CONTROL_DIR = None


# ─────────────────────────────────────────────────────────────────────────────
# 4. AsyncSyncPipeline concurrency
# ─────────────────────────────────────────────────────────────────────────────
class TestStagedDirectorySync:
    def test_directory_rsync_uses_staging_swap_without_inplace(self, tmp_path):
        local = tmp_path / "reports"
        local.mkdir()
        (local / "data_detail.json").write_text("{}")
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch("src.sync_manager.shutil.which", return_value="/usr/bin/rsync"), \
             patch("src.sync_manager.ReportSyncer._remote_has_binary", return_value=True), \
             patch("src.sync_manager.subprocess.run", side_effect=fake_run):
            ok = ReportSyncer.sync_directory_to_remote(
                local_dir=str(local),
                base_dir=str(tmp_path),
                user="deploy",
                host="example.com",
                dest_dir="/remote",
                _capability_cache={"has_rsync": True, "control_socket": ""},
            )

        assert ok is True
        remote_commands = [cmd[-1] for cmd in calls if cmd and cmd[0] == "ssh"]
        assert any(".__staging__" in cmd and "cp -al" in cmd for cmd in remote_commands)
        assert any("mv /remote/reports.__staging__ /remote/reports" in cmd for cmd in remote_commands)
        rsync_calls = [cmd for cmd in calls if cmd and cmd[0] == "rsync"]
        assert len(rsync_calls) == 1
        assert "--inplace" not in rsync_calls[0]
        assert "--compress-level=1" in rsync_calls[0]
        assert rsync_calls[0][-1] == "deploy@example.com:/remote/reports.__staging__/"

    def test_directory_tar_fallback_extracts_to_staging_then_swaps(self, tmp_path):
        local = tmp_path / "reports"
        local.mkdir()
        (local / "summary.json").write_text("{}")
        calls = []

        class FakeStdout:
            def close(self):
                pass

        class FakePopen:
            def __init__(self, cmd, **kwargs):
                calls.append(cmd)
                self.stdout = FakeStdout()

            def wait(self, timeout=None):
                return 0

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch("src.sync_manager.shutil.which", return_value=None), \
             patch("src.sync_manager.subprocess.Popen", FakePopen), \
             patch("src.sync_manager.subprocess.run", side_effect=fake_run):
            ok = ReportSyncer.sync_directory_to_remote(
                local_dir=str(local),
                base_dir=str(tmp_path),
                user="deploy",
                host="example.com",
                dest_dir="/remote",
                _capability_cache={"has_rsync": False, "control_socket": ""},
            )

        assert ok is True
        remote_commands = [cmd[-1] for cmd in calls if cmd and cmd[0] == "ssh"]
        assert any("tar -xzf - -C /remote/reports.__staging__" in cmd for cmd in remote_commands)
        assert any("mv /remote/reports.__staging__ /remote/reports" in cmd for cmd in remote_commands)


class TestAsyncSyncPipeline(SyncTestBase):
    """Kiểm tra AsyncSyncPipeline với nhiều file/dir đồng thời."""

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
