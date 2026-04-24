import subprocess
import os
import shutil

class ReportSyncer:
    """Handles syncing reports to a remote server over SSH using Tar Stream Pipeline."""

    @staticmethod
    def _remote_supports_codec(ssh_cmd: str, codec_bin: str) -> bool:
        """Check whether remote host supports a codec with tar --use-compress-program."""
        probe = (
            f"{ssh_cmd} "
            f"\"command -v {codec_bin} >/dev/null 2>&1 && "
            "tar --help 2>/dev/null | grep -q -- '--use-compress-program'\""
        )
        res = subprocess.run(
            probe,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return res.returncode == 0

    @staticmethod
    def _remote_has_binary(ssh_cmd: str, binary: str) -> bool:
        """Check whether a binary exists on remote host."""
        probe = f"{ssh_cmd} \"command -v {binary} >/dev/null 2>&1\""
        res = subprocess.run(
            probe,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return res.returncode == 0
    
    @staticmethod
    def sync_to_remote(output_dir: str, user: str, host: str, dest_dir: str, password: str = None) -> bool:
        """
        Compresses and streams the contents of output_dir to a remote server.
        
        Args:
            output_dir (str): Local directory containing the reports.
            user (str): SSH username for the remote server.
            host (str): IP address or hostname of the remote server.
            dest_dir (str): Destination directory on the remote server.
            password (str): Optional SSH password (requires 'sshpass' installed).
            
        Returns:
            bool: True if successful, False otherwise.
        """
        if not os.path.exists(output_dir):
            print(f"Error: Sync failed. Local report directory '{output_dir}' does not exist.")
            return False
            
        if not user or not host or not dest_dir:
            print("Error: Missing required SSH sync parameters (--sync-user, --sync-host, --sync-dest-dir).")
            return False

        print(f"\n[SYNC] Initiating remote sync to {user}@{host}:{dest_dir}...")
        
        # Build the exact SSH pipeline specified
        # "rm -rf <dest> && mkdir -p <dest> && tar -xzf - -C <dest>"
        # Using -q to suppress basic ssh warnings, and no -v on tar to keep it silent.
        ssh_cmd = f"ssh -q {user}@{host}"
        if password:
            safe_pass = password.replace("'", "'\\''")
            ssh_cmd = f"sshpass -p '{safe_pass}' {ssh_cmd}"
            
        safe_dest = dest_dir.replace("'", "'\\''")
        output_dir_clean = output_dir.rstrip("/")

        # Prefer rsync when available: incremental sync avoids full re-compress every run.
        use_rsync = shutil.which("rsync") is not None and ReportSyncer._remote_has_binary(ssh_cmd, "rsync")
        if use_rsync:
            mkdir_cmd = f"{ssh_cmd} \"mkdir -p '{safe_dest}'\""
            rsync_cmd = (
                "rsync -a --delete --delete-delay --partial --inplace --no-whole-file "
                "-e \"ssh -q\" "
                f"\"{output_dir_clean}/\" "
                f"\"{user}@{host}:'{safe_dest}/'\""
            )
            if password:
                rsync_cmd = f"sshpass -p '{safe_pass}' {rsync_cmd}"

            try:
                mkdir_proc = subprocess.run(
                    mkdir_cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if mkdir_proc.returncode != 0:
                    print("[SYNC WARN] Could not prepare remote directory for rsync; falling back to archive stream.")
                else:
                    rsync_proc = subprocess.run(
                        rsync_cmd,
                        shell=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    if rsync_proc.returncode == 0:
                        print(f"[SYNC] Successfully synced reports to {host} using rsync.")
                        return True
                    print(f"[SYNC WARN] rsync failed (code {rsync_proc.returncode}); falling back to archive stream.")
                    if rsync_proc.stderr:
                        print(f"[SYNC WARN DETAILS]:\n{rsync_proc.stderr.strip()}")
            except Exception as e:
                print(f"[SYNC WARN] rsync path failed ({e}); falling back to archive stream.")

        use_xz = shutil.which("xz") is not None and ReportSyncer._remote_supports_codec(ssh_cmd, "xz")

        if use_xz:
            # xz is commonly available and typically compresses better than gzip.
            local_pack = "tar --use-compress-program='xz -9 -T0' -cf -"
            remote_unpack = "tar --use-compress-program='xz -d -T0' -xf -"
            codec_label = "xz"
        else:
            local_pack = "tar -czf -"
            remote_unpack = "tar -xzf -"
            codec_label = "gzip"

        cmd = (
            f"{local_pack} -C \"{output_dir}\" . | "
            f"{ssh_cmd} "
            f"\"rm -rf '{safe_dest}' && mkdir -p '{safe_dest}' && {remote_unpack} -C '{safe_dest}'\""
        )
        
        try:
            # We use shell=True because this relies heavily on the bash pipe (|).
            process = subprocess.run(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if process.returncode == 0:
                print(f"[SYNC] Successfully synced reports to {host} using {codec_label}.")
                return True
            else:
                print(f"[SYNC ERROR] Sync failed with return code {process.returncode}.")
                if process.stderr:
                    print(f"[SYNC ERROR DETAILS]:\n{process.stderr.strip()}")
                return False
                
        except Exception as e:
            print(f"[SYNC EXCEPTION] An error occurred during sync: {str(e)}")
            return False
