import subprocess
import os

class ReportSyncer:
    """Handles syncing reports to a remote server over SSH using Tar Stream Pipeline."""
    
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
            
        cmd = (
            f"tar -czf - -C \"{output_dir}\" . | "
            f"{ssh_cmd} "
            f"\"rm -rf {dest_dir} && mkdir -p {dest_dir} && tar -xzf - -C {dest_dir}\""
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
                print(f"[SYNC] Successfully synced reports to {host}.")
                return True
            else:
                print(f"[SYNC ERROR] Sync failed with return code {process.returncode}.")
                if process.stderr:
                    print(f"[SYNC ERROR DETAILS]:\n{process.stderr.strip()}")
                return False
                
        except Exception as e:
            print(f"[SYNC EXCEPTION] An error occurred during sync: {str(e)}")
            return False
