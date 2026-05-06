import atexit
import os
import shlex
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Dict

SSH_TIMEOUT = 30
RSYNC_TIMEOUT = 300
SCP_TIMEOUT = 120

_SYNC_PRINT_LOCK = Lock()


def _sync_log(message: str) -> None:
    with _SYNC_PRINT_LOCK:
        print(message)

# ── SSH ControlMaster settings ──────────────────────────────────────────
_CONTROL_DIR = None          # lazily created temp dir for sockets
_CONTROL_SOCKETS: dict = {}  # (user, host) -> socket path


def _get_control_dir() -> str:
    """Return (and create if needed) a temp dir for SSH control sockets."""
    global _CONTROL_DIR
    if _CONTROL_DIR is None or not os.path.isdir(_CONTROL_DIR):
        _CONTROL_DIR = tempfile.mkdtemp(prefix="sync_ssh_ctl_")
        atexit.register(_cleanup_control_dir)
    return _CONTROL_DIR


def _cleanup_control_dir():
    """Remove the control socket directory on exit."""
    global _CONTROL_DIR
    if _CONTROL_DIR and os.path.isdir(_CONTROL_DIR):
        # Tear down any lingering master connections
        for key, sock in list(_CONTROL_SOCKETS.items()):
            try:
                subprocess.run(
                    ["ssh", "-O", "exit", "-o", f"ControlPath={sock}", "dummy"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                )
            except Exception:
                pass
        _CONTROL_SOCKETS.clear()
        try:
            shutil.rmtree(_CONTROL_DIR, ignore_errors=True)
        except Exception:
            pass
        _CONTROL_DIR = None


def _get_control_socket(user: str, host: str, password: str = None) -> str:
    """Get or create an SSH ControlMaster socket for (user, host).

    The master is started as a background process with ``-MNf``
    (master mode, no remote command, go to background).
    Subsequent SSH commands that specify the same ``ControlPath``
    will re-use this connection — eliminating per-command handshake.
    """
    key = (user, host)
    sock = _CONTROL_SOCKETS.get(key)
    if sock and os.path.exists(sock):
        # Verify master is still alive
        check = subprocess.run(
            ["ssh", "-O", "check", "-o", f"ControlPath={sock}", f"{user}@{host}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        if check.returncode == 0:
            return sock

    # Create new control socket
    ctl_dir = _get_control_dir()
    sock = os.path.join(ctl_dir, f"{user}@{host}.sock")

    master_cmd = [
        *(["sshpass", "-e"] if password else []),
        "ssh", "-MNf",
        "-o", f"ControlPath={sock}",
        "-o", "ControlPersist=600",       # keep alive 10 min after last use
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "StrictHostKeyChecking=accept-new",
        "-q",
        f"{user}@{host}",
    ]
    env = {**os.environ}
    if password:
        env["SSHPASS"] = password

    try:
        proc = subprocess.run(
            master_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=env,
            timeout=SSH_TIMEOUT,
        )
        if proc.returncode == 0 and os.path.exists(sock):
            _CONTROL_SOCKETS[key] = sock
            return sock
        else:
            # Master failed — return empty so callers fall back to normal SSH
            stderr_msg = proc.stderr.decode("utf-8", errors="replace").strip() if proc.stderr else ""
            if stderr_msg:
                _sync_log(f"[SYNC WARN] SSH ControlMaster failed for {user}@{host}: {stderr_msg}")
            return ""
    except (subprocess.TimeoutExpired, OSError) as e:
        _sync_log(f"[SYNC WARN] SSH ControlMaster setup failed: {e}")
        return ""


def _ssh_control_args(control_socket: str) -> list:
    """Return SSH args to use an existing ControlMaster socket."""
    if control_socket:
        return ["-o", f"ControlPath={control_socket}", "-o", "ControlMaster=auto"]
    return []


def _build_ssh_base(user: str, host: str, password: str = None, control_socket: str = ""):
    """Build SSH base command as a list (no shell=True needed).

    Returns (cmd_prefix, env_extra) where:
      - cmd_prefix is e.g. ["ssh", "-q", "user@host"]
      - env_extra  is a dict merged into os.environ for the subprocess
        (contains SSHPASS when password is set).
    """
    target = f"{user}@{host}"
    env_extra = {}
    ctl_args = _ssh_control_args(control_socket)
    if password:
        env_extra["SSHPASS"] = password
        return ["sshpass", "-e", "ssh", "-q", *ctl_args, target], env_extra
    return ["ssh", "-q", *ctl_args, target], env_extra


def _build_sshpass_env(password: str = None):
    """Return env dict with SSHPASS set (if password provided)."""
    if password:
        return {"SSHPASS": password}
    return {}


class ReportSyncer:
    """Handles syncing reports to a remote server over SSH."""

    @staticmethod
    def _remote_has_binary(ssh_base: list, env: dict, binary: str) -> bool:
        """Check whether a binary exists on remote host."""
        cmd = ssh_base + [f"command -v {binary} >/dev/null 2>&1"]
        try:
            res = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env={**os.environ, **env},
                timeout=SSH_TIMEOUT,
            )
            return res.returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            return False

    @staticmethod
    def _remote_supports_codec(ssh_base: list, env: dict, codec_bin: str) -> bool:
        """Check whether remote host supports a codec with tar --use-compress-program."""
        remote_cmd = (
            f"command -v {codec_bin} >/dev/null 2>&1 && "
            "tar --help 2>/dev/null | grep -q -- '--use-compress-program'"
        )
        cmd = ssh_base + [remote_cmd]
        try:
            res = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env={**os.environ, **env},
                timeout=SSH_TIMEOUT,
            )
            return res.returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            return False

    @staticmethod
    def sync_to_remote(output_dir: str, user: str, host: str, dest_dir: str, password: str = None) -> bool:
        """Compresses and streams the contents of output_dir to a remote server."""
        if not os.path.exists(output_dir):
            _sync_log(f"Error: Sync failed. Local report directory '{output_dir}' does not exist.")
            return False

        if not user or not host or not dest_dir:
            _sync_log("Error: Missing required SSH sync parameters (--sync-user, --sync-host, --sync-dest-dir).")
            return False

        _sync_log(f"\n[SYNC] Initiating remote sync to {user}@{host}:{dest_dir}...")

        control_socket = _get_control_socket(user, host, password)
        ssh_base, env = _build_ssh_base(user, host, password, control_socket)
        output_dir_clean = output_dir.rstrip("/")

        use_xz = shutil.which("xz") is not None and ReportSyncer._remote_supports_codec(ssh_base, env, "xz")

        if use_xz:
            compress_flag = "--use-compress-program=xz"
            remote_extract = f"rm -rf '{dest_dir}' && mkdir -p '{dest_dir}' && tar --use-compress-program=xz -xf - -C '{dest_dir}'"
        else:
            compress_flag = "-z"
            remote_extract = f"rm -rf '{dest_dir}' && mkdir -p '{dest_dir}' && tar -xzf - -C '{dest_dir}'"

        tar_create = ["tar", compress_flag, "-cf", "-", "-C", output_dir_clean, "."]
        ssh_extract = ssh_base + [remote_extract]

        try:
            tar_proc = subprocess.Popen(
                tar_create,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            ssh_proc = subprocess.run(
                ssh_extract,
                stdin=tar_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={**os.environ, **env},
                timeout=RSYNC_TIMEOUT,
            )
            tar_proc.stdout.close()
            tar_proc.wait(timeout=30)

            if ssh_proc.returncode == 0:
                codec = "xz" if use_xz else "gzip"
                _sync_log(f"[SYNC] Successfully synced reports to {host} using tar+{codec} stream.")
                return True
            _sync_log(f"[SYNC ERROR] Archive stream failed (code {ssh_proc.returncode}).")
            if ssh_proc.stderr:
                _sync_log(f"[SYNC ERROR DETAILS]:\n{ssh_proc.stderr.strip()}")
            return False
        except subprocess.TimeoutExpired:
            _sync_log("[SYNC ERROR] Archive stream timed out.")
            return False
        except Exception as e:
            _sync_log(f"[SYNC EXCEPTION] Archive stream failed: {str(e)}")
            return False

    @staticmethod
    def sync_file_to_remote(
        file_path: str,
        base_dir: str,
        user: str,
        host: str,
        dest_dir: str,
        password: str = None,
        _capability_cache: dict = None,
    ) -> bool:
        """Sync a single file to the remote server, preserving relative directory structure."""
        if not file_path or not os.path.isfile(file_path):
            return False

        file_abs = os.path.abspath(file_path)
        base_dir_abs = os.path.abspath(base_dir) if base_dir else os.path.dirname(file_abs)

        rel_path = os.path.relpath(file_abs, start=base_dir_abs)
        if rel_path.startswith(".."):
            rel_path = os.path.basename(file_abs)

        control_socket = ""
        if _capability_cache is not None:
            control_socket = _capability_cache.get("control_socket", "")

        ssh_base, env = _build_ssh_base(user, host, password, control_socket)
        merged_env = {**os.environ, **env}

        rel_dir = os.path.dirname(rel_path)
        remote_dir = f"{dest_dir}/{rel_dir}" if rel_dir else dest_dir
        remote_file = f"{dest_dir}/{rel_path}"

        extract_cmd = ssh_base + [f"mkdir -p '{remote_dir}' && tar -xzf - -C '{remote_dir}'"]
        try:
            tar_proc = subprocess.Popen(
                ["tar", "-czf", "-", "-C", os.path.dirname(file_abs), os.path.basename(file_abs)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            ssh_proc = subprocess.run(
                extract_cmd,
                stdin=tar_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=merged_env,
                timeout=SCP_TIMEOUT,
            )
            tar_proc.stdout.close()
            tar_proc.wait(timeout=30)

            if ssh_proc.returncode == 0:
                _sync_log(f"[SYNC] Synced file: {rel_path} (tar)")
                return True
            _sync_log(f"[SYNC ERROR] tar stream failed for {rel_path} (code {ssh_proc.returncode}).")
            if ssh_proc.stderr:
                _sync_log(f"[SYNC ERROR DETAILS]:\n{ssh_proc.stderr.strip()}")
            return False
        except subprocess.TimeoutExpired:
            _sync_log(f"[SYNC ERROR] tar stream timed out for {rel_path}.")
            return False
        except Exception as e:
            _sync_log(f"[SYNC EXCEPTION] tar stream failed for {rel_path}: {str(e)}")
            return False

    @staticmethod
    def sync_directory_to_remote(
        local_dir: str,
        base_dir: str,
        user: str,
        host: str,
        dest_dir: str,
        password: str = None,
        _capability_cache: dict = None,
    ) -> bool:
        """Batch-sync an entire local directory to remote using a single tar stream."""
        if not local_dir or not os.path.isdir(local_dir):
            return False

        local_abs = os.path.abspath(local_dir).rstrip("/")
        base_abs = os.path.abspath(base_dir).rstrip("/") if base_dir else os.path.dirname(local_abs)

        rel_path = os.path.relpath(local_abs, start=base_abs)
        if rel_path.startswith(".."):
            rel_path = os.path.basename(local_abs)

        remote_target = f"{dest_dir}/{rel_path}"
        remote_staging = f"{remote_target}.__staging__"
        remote_old = f"{remote_target}.__old__"

        control_socket = ""
        if _capability_cache is not None:
            control_socket = _capability_cache.get("control_socket", "")

        ssh_base, env = _build_ssh_base(user, host, password, control_socket)
        merged_env = {**os.environ, **env}

        q_staging = shlex.quote(remote_staging)
        q_target = shlex.quote(remote_target)
        q_old = shlex.quote(remote_old)

        def _run_stage(stage_name: str, remote_cmd: str, timeout: int = SSH_TIMEOUT) -> bool:
            proc = subprocess.run(
                ssh_base + [f"bash --noprofile --norc -lc {shlex.quote(remote_cmd)}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=merged_env,
                timeout=timeout,
            )
            if proc.returncode == 0:
                return True
            _sync_log(f"[SYNC ERROR] Stage failed [{stage_name}] for {rel_path}/ (code {proc.returncode}).")
            details = (proc.stderr or proc.stdout or "").strip()
            if details:
                _sync_log(f"[SYNC ERROR DETAILS][{stage_name}]:\n{details}")
            return False

        try:
            if not _run_stage("cleanup_staging", f"rm -rf {q_staging}"):
                return False
            if not _run_stage("create_staging", f"mkdir -p {q_staging}"):
                return False

            tar_proc = subprocess.Popen(
                ["tar", "-czf", "-", "-C", local_abs, "."],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            ssh_proc = subprocess.run(
                ssh_base + [f"bash --noprofile --norc -lc {shlex.quote(f'tar -xzf - -C {q_staging}')}"] ,
                stdin=tar_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=merged_env,
                timeout=RSYNC_TIMEOUT,
            )
            tar_proc.stdout.close()
            tar_proc.wait(timeout=30)

            if ssh_proc.returncode != 0:
                _sync_log(f"[SYNC ERROR] Stage failed [stream_extract] for {rel_path}/ (code {ssh_proc.returncode}).")
                details = (ssh_proc.stderr or ssh_proc.stdout or "").strip()
                if details:
                    _sync_log(f"[SYNC ERROR DETAILS][stream_extract]:\n{details}")
                return False

            if not _run_stage("cleanup_old", f"rm -rf {q_old}"):
                return False
            if not _run_stage("rotate_current", f"if [ -d {q_target} ]; then mv -T {q_target} {q_old} 2>/dev/null || rm -rf {q_old}; fi"):
                return False
            if not _run_stage("promote_staging", f"mv {q_staging} {q_target}"):
                return False
            if not _run_stage("cleanup_old_final", f"rm -rf {q_old}"):
                return False

            file_count = sum(1 for entry in os.scandir(local_abs) if entry.is_file())
            _sync_log(f"[SYNC] Batch synced directory: {rel_path}/ ({file_count} files, tar+gzip)")
            return True
        except subprocess.TimeoutExpired as e:
            _sync_log(f"[SYNC ERROR] Batch tar stage timed out for {rel_path}/: {e}")
            return False
        except Exception as e:
            _sync_log(f"[SYNC EXCEPTION] Batch tar stream failed for {rel_path}/: {e}")
            return False



def _should_compress(file_path: str) -> bool:
    """Compatibility helper retained for legacy tests; tar-only flow ignores this."""
    ext = os.path.splitext(file_path)[1].lower()
    compressible = {".json", ".tsv", ".csv", ".txt", ".ndjson", ".sql"}
    skip = {".gz", ".xz", ".zst", ".bz2", ".lz4", ".zip", ".tar"}
    if ext in skip:
        return False
    if ext in compressible:
        return True
    try:
        return os.path.getsize(file_path) >= 1_048_576
    except OSError:
        return False


class AsyncSyncPipeline:
    """Async pipeline that syncs files to remote as they are enqueued.

    Probes remote capabilities once at init (cached for all files).
    Uses SSH ControlMaster to multiplex all SSH connections through a single socket.
    Uses 4 workers for moderate parallelism (safe with ControlMaster multiplexing).
    """

    def __init__(self, base_dir: str, user: str, host: str, dest_dir: str, password: str = None):
        self.base_dir = base_dir or "."
        self.user = user
        self.host = host
        self.dest_dir = dest_dir
        self.password = password
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._lock = Lock()
        self._futures = []
        self._capability_cache = self._probe_capabilities()

    def _probe_capabilities(self) -> dict:
        """Probe remote once: establish ControlMaster socket."""
        cache: Dict[str, object] = {"control_socket": ""}
        if not self.user or not self.host:
            return cache

        # Establish SSH ControlMaster socket (shared by ALL subsequent commands)
        control_socket = _get_control_socket(self.user, self.host, self.password)
        cache["control_socket"] = control_socket

        return cache

    def enqueue_file(self, file_path: str):
        if not file_path:
            return
        path = os.path.abspath(file_path)
        if not os.path.isfile(path):
            return

        fut = self._executor.submit(
            ReportSyncer.sync_file_to_remote,
            file_path=path,
            base_dir=self.base_dir,
            user=self.user,
            host=self.host,
            dest_dir=self.dest_dir,
            password=self.password,
            _capability_cache=self._capability_cache,
        )
        with self._lock:
            self._futures.append(fut)

    def enqueue_directory(self, dir_path: str):
        """Enqueue an entire directory for batch sync (single tar stream).

        Much more efficient than enqueue_file() for each file in the directory.
        Falls back to file-by-file if batch fails.
        """
        if not dir_path:
            return
        path = os.path.abspath(dir_path)
        if not os.path.isdir(path):
            return

        fut = self._executor.submit(
            self._sync_directory_with_fallback,
            dir_path=path,
        )
        with self._lock:
            self._futures.append(fut)

    def _sync_directory_with_fallback(self, dir_path: str) -> bool:
        """Try batch directory sync; fall back to file-by-file on failure."""
        ok = ReportSyncer.sync_directory_to_remote(
            local_dir=dir_path,
            base_dir=self.base_dir,
            user=self.user,
            host=self.host,
            dest_dir=self.dest_dir,
            password=self.password,
            _capability_cache=self._capability_cache,
        )
        if ok:
            return True

        # Fallback: sync each file individually
        _sync_log(f"[SYNC] Falling back to file-by-file sync for {dir_path}")
        results = []
        try:
            for entry in os.scandir(dir_path):
                if entry.is_file():
                    r = ReportSyncer.sync_file_to_remote(
                        file_path=entry.path,
                        base_dir=self.base_dir,
                        user=self.user,
                        host=self.host,
                        dest_dir=self.dest_dir,
                        password=self.password,
                        _capability_cache=self._capability_cache,
                    )
                    results.append(r)
        except OSError as e:
            _sync_log(f"[SYNC ERROR] Could not list directory {dir_path}: {e}")
            return False
        return all(results) if results else False

    def wait(self):
        with self._lock:
            pending = list(self._futures)
        for fut in pending:
            try:
                fut.result()
            except Exception as e:
                _sync_log(f"[SYNC WARN] Async sync error: {e}")

    def close(self):
        self.wait()
        self._executor.shutdown(wait=True)
