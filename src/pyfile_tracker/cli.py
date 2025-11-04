import argparse
import json
import os
import shutil
import time
import re
import hashlib
import tempfile
import subprocess
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class FileTrackerApp:
    METADATA_FILE = "metadata.json"
    GIT_DIR_NAME = "repo.git"
    DEBOUNCE_SECONDS = 2.0

    def __init__(self, args: Optional[argparse.Namespace] = None):
        self.args = args or self.parse_args()
        self.logger = logging.getLogger(__name__)
        self.configure_logging(self.args.log_level)

        if shutil.which("git") is None:
            raise SystemExit("git is required but not found on PATH.")

        self.input_path = self.args.input
        self.version_root = self.get_version_root(self.input_path, self.args.output)

        self.metadata = self.load_metadata(self.version_root)
        self.ensure_input_path(self.metadata, self.input_path)

    # ---------- CLI / logging ----------

    @staticmethod
    def parse_args() -> argparse.Namespace:
        p = argparse.ArgumentParser(
            description="Continuous incremental file versioning tool for a folder (git-based)."
        )
        p.add_argument(
            "-i",
            "--input",
            required=True,
            help="Path to files to track / recover.",
        )
        p.add_argument(
            "-o",
            "--output",
            help="Version store path. Defaults to per-drive app directory based on input path.",
        )
        p.add_argument(
            "-t",
            "--track",
            action='store_true',
            help=(
                "Retention for tracking mode (continuous). "
            ),
        )
        p.add_argument(
            "-p",
            "--polling-interval",
            type=float,
            default=60.0,
            help="Polling interval in seconds (default: 60.0 seconds).",
        )
        p.add_argument(
            "-r",
            "--recover",
            help=(
                "Recovery mode (one-shot): recover point, either:\n"
                "  * integer revision index (0=last, 1=previous, -1=earliest, ...), or\n"
                "  * Unix timestamp, or\n"
                "  * ISO datetime (YYYY-MM-DDTHH:MM:SS), or\n"
                "  * timedelta like '30m', '1h', '1d' (relative to now)."
            ),
        )
        p.add_argument(
            "--log-level",
            default="INFO",
            help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
        )
        args = p.parse_args()

        if bool(args.track) == bool(args.recover):
            p.error(
                "Exactly one of -k/--track (tracking) or -r/--recover (recovery) must be provided."
            )

        return args

    @staticmethod
    def configure_logging(level: str) -> None:
        numeric_level = getattr(logging, level.upper(), None)
        if not isinstance(numeric_level, int):
            numeric_level = logging.INFO
        logging.basicConfig(
            level=numeric_level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

    # ---------- Paths / storage ----------

    def get_default_base_for_input(self, input_path: str) -> str:
        abs_input = os.path.abspath(os.path.expanduser(input_path))
        drive, _ = os.path.splitdrive(abs_input)

        if os.name == "nt" and drive:
            root = drive + os.path.sep
            base = os.path.join(root, ".pyfile_tracker")
        else:
            home = os.path.expanduser("~")
            base = os.path.join(home, ".pyfile_tracker")

        os.makedirs(base, exist_ok=True)
        return base

    def get_version_root(self, input_path: str, output_path: Optional[str]) -> str:
        if output_path:
            root = os.path.abspath(os.path.expanduser(output_path))
        else:
            base = self.get_default_base_for_input(input_path)
            norm = os.path.abspath(os.path.expanduser(input_path))
            digest = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12]
            root = os.path.join(base, digest)
        os.makedirs(root, exist_ok=True)
        return root

    # ---------- Metadata ----------

    def load_metadata(self, version_root: str) -> Dict[str, Any]:
        path = os.path.join(version_root, self.METADATA_FILE)
        if not os.path.exists(path):
            return {"input_path": None, "snapshots": []}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {"input_path": None, "snapshots": []}

    def save_metadata(self) -> None:
        path = os.path.join(self.version_root, self.METADATA_FILE)
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2)
        os.replace(tmp_path, path)

    def ensure_input_path(self, metadata: Dict[str, Any], input_path: str) -> None:
        abs_in = os.path.abspath(os.path.expanduser(input_path))
        if metadata.get("input_path") is None:
            metadata["input_path"] = abs_in
        elif os.path.abspath(metadata["input_path"]) != abs_in:
            raise SystemExit(
                f"Version store already linked to a different input path: {metadata['input_path']}"
            )

    def ensure_version_root_not_in_input(self) -> None:
        abs_in = os.path.abspath(os.path.expanduser(self.input_path))
        vr = os.path.abspath(self.version_root)
        try:
            common = os.path.commonpath([abs_in, vr])
        except ValueError:
            return
        if common == abs_in:
            raise SystemExit(
                "Version directory is inside the tracked folder. "
                "Please move it outside or specify -o to a separate path."
            )

    # ---------- Git helpers ----------

    def run_git(
        self,
        git_dir: str,
        work_tree: str,
        args: List[str],
        check: bool = True,
        capture_output: bool = False,
    ) -> subprocess.CompletedProcess:
        cmd = ["git", f"--git-dir={git_dir}", f"--work-tree={work_tree}"] + args
        self.logger.debug("Running git: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
        )
        if check and result.returncode != 0:
            raise SystemExit(f"Git command failed: {' '.join(cmd)}\n{result.stderr}")
        return result

    def ensure_git_repo(self, git_dir: str, work_tree: str) -> None:
        if not os.path.isdir(git_dir) or not os.path.isfile(
            os.path.join(git_dir, "HEAD")
        ):
            os.makedirs(git_dir, exist_ok=True)
            self.run_git(git_dir, work_tree, ["init"])
            self.run_git(git_dir, work_tree, ["config", "user.name", "file-tracker"])
            self.run_git(
                git_dir, work_tree, ["config", "user.email", "file-tracker@local"]
            )

    # ---------- Snapshots ----------

    def list_snapshots(self) -> List[Dict[str, Any]]:
        snaps = self.metadata.get("snapshots", [])
        snaps.sort(key=lambda s: s["timestamp"])
        return snaps

    def next_snapshot_id(self) -> int:
        snaps = self.metadata.get("snapshots", [])
        if not snaps:
            return 1
        return max(s["id"] for s in snaps) + 1

    def create_snapshot(self) -> Optional[Dict[str, Any]]:
        abs_input = os.path.abspath(os.path.expanduser(self.input_path))
        git_dir = os.path.join(self.version_root, self.GIT_DIR_NAME)

        self.ensure_git_repo(git_dir, abs_input)

        self.run_git(git_dir, abs_input, ["add", "-A"])

        diff_res = self.run_git(
            git_dir, abs_input, ["diff", "--cached", "--quiet"], check=False
        )
        if diff_res.returncode == 0:
            self.logger.debug("No changes detected; skipping snapshot.")
            return None

        snap_id = self.next_snapshot_id()
        ts = time.time()
        iso = datetime.fromtimestamp(ts).isoformat(timespec="seconds")
        msg = f"snapshot {snap_id} {iso}"

        self.run_git(git_dir, abs_input, ["commit", "-m", msg])
        commit = self.run_git(
            git_dir, abs_input, ["rev-parse", "HEAD"], capture_output=True
        ).stdout.strip()

        snap_meta = {
            "id": snap_id,
            "timestamp": ts,
            "iso": iso,
            "commit": commit,
        }
        self.metadata.setdefault("snapshots", []).append(snap_meta)
        return snap_meta

    def parse_recover_point(self, r_value: str) -> Dict[str, Any]:
        snaps = self.list_snapshots()
        if not snaps:
            raise SystemExit("No snapshots available to recover from.")

        # 1) integer index
        try:
            idx = int(r_value)
            n = len(snaps)
            if idx >= 0:
                pos = n - 1 - idx
                if pos < 0 or pos >= n:
                    raise SystemExit(f"Revision index {idx} out of range (0..{n - 1}).")
            else:
                pos = -(idx + 1)
                if pos < 0 or pos >= n:
                    raise SystemExit(f"Negative revision index {idx} out of range.")
            return snaps[pos]
        except ValueError:
            pass

        # 2) timedelta-like string: "30m", "1h", "2d", "45s"
        m = re.fullmatch(r"(\d+)\s*([smhd])", r_value.strip(), re.IGNORECASE)
        if m:
            amount = int(m.group(1))
            unit = m.group(2).lower()
            if unit == "s":
                seconds = amount
            elif unit == "m":
                seconds = amount * 60
            elif unit == "h":
                seconds = amount * 3600
            elif unit == "d":
                seconds = amount * 86400
            else:
                raise SystemExit(f"Unsupported timedelta unit in -r: {unit}")
            ts = time.time() - seconds
        else:
            # 3) numeric timestamp or ISO datetime
            if re.fullmatch(r"\d+(\.\d+)?", r_value):
                try:
                    ts = float(r_value)
                except ValueError:
                    raise SystemExit(f"Invalid timestamp '{r_value}'.")
            else:
                s = r_value.strip()
                if " " in s and "T" not in s:
                    s = s.replace(" ", "T")
                try:
                    dt = datetime.fromisoformat(s)
                except ValueError:
                    raise SystemExit(
                        f"Invalid recover point '{r_value}'. "
                        "Use int index, Unix timestamp, ISO datetime, or timedelta like '1h'."
                    )
                ts = dt.timestamp()

        # common path for (2) and (3): find snapshot <= ts, latest
        candidate = None
        for s in snaps:
            if s["timestamp"] <= ts:
                candidate = s
            else:
                break
        if candidate is None:
            raise SystemExit(
                f"No snapshot found at or before target time "
                f"({datetime.fromtimestamp(ts).isoformat(timespec='seconds')})."
            )
        return candidate

    # ---------- Restore ----------

    @staticmethod
    def build_rel_paths(root: str) -> set:
        paths = set()
        for dirpath, dirnames, filenames in os.walk(root):
            rel_dir = os.path.relpath(dirpath, root)
            if rel_dir == ".":
                rel_dir = ""
            for name in filenames:
                rel_file = os.path.join(rel_dir, name) if rel_dir else name
                paths.add(rel_file)
        return paths

    def _do_restore_from_root(self, abs_input: str, snap_root: str) -> None:
        current_files = self.build_rel_paths(abs_input)
        snap_files = self.build_rel_paths(snap_root)

        to_delete = current_files - snap_files
        for rel in to_delete:
            path = os.path.join(abs_input, rel)
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)

        for rel in snap_files:
            src = os.path.join(snap_root, rel)
            dst = os.path.join(abs_input, rel)
            dst_dir = os.path.dirname(dst)
            os.makedirs(dst_dir, exist_ok=True)
            shutil.copy2(src, dst)

    def restore_snapshot(self, snapshot: Dict[str, Any]) -> None:
        abs_input = os.path.abspath(os.path.expanduser(self.input_path))
        self.ensure_version_root_not_in_input()

        git_dir = os.path.join(self.version_root, self.GIT_DIR_NAME)
        commit = snapshot.get("commit")
        if not commit:
            raise SystemExit(
                "Snapshot metadata has no commit hash; old format not supported here."
            )

        tmp_dir = tempfile.mkdtemp(
            prefix=f"restore_{snapshot['id']:06d}_", dir=self.version_root
        )
        try:
            self.run_git(git_dir, tmp_dir, ["checkout", commit, "--", "."])
            self._do_restore_from_root(abs_input, tmp_dir)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # ---------- Watchdog handler (inner class) ----------

    class ChangeHandler(FileSystemEventHandler):
        def __init__(self, app: "FileTrackerApp", input_root: str):
            super().__init__()
            self.app = app
            self.input_root = os.path.abspath(os.path.expanduser(input_root))
            self.pending = False
            self.last_change = 0.0

        def _mark_change(self, path: str) -> None:
            if not path:
                return
            abs_path = os.path.abspath(path)
            try:
                inside = (
                    os.path.commonpath([abs_path, self.input_root]) == self.input_root
                )
            except ValueError:
                inside = False
            if not inside:
                return
            self.pending = True
            self.last_change = time.time()
            self.app.logger.debug("Change detected at %s", abs_path)

        def on_any_event(self, event):
            if event.is_directory:
                return
            self._mark_change(getattr(event, "src_path", None))
            if hasattr(event, "dest_path"):
                self._mark_change(getattr(event, "dest_path", None))

    # ---------- Modes ----------

    def run_tracking(self) -> None:
        abs_input = os.path.abspath(os.path.expanduser(self.input_path))
        if not os.path.isdir(abs_input):
            raise SystemExit(
                f"Input path must be an existing directory: {self.input_path}"
            )

        self.ensure_version_root_not_in_input()

        if not self.metadata.get("snapshots"):
            snap = self.create_snapshot()
            if snap:
                self.save_metadata()
                self.logger.info(
                    "[init] Created baseline snapshot id=%s at %s",
                    snap["id"],
                    snap["iso"],
                )

        handler = self.ChangeHandler(self, abs_input)
        observer = Observer()
        observer.schedule(handler, abs_input, recursive=True)
        observer.start()

        self.logger.info("Tracking '%s'", abs_input)
        self.logger.info("Version store: %s", self.version_root)

        try:
            while True:
                time.sleep(self.args.polling_interval)
                if (
                    handler.pending
                    and (time.time() - handler.last_change) >= self.DEBOUNCE_SECONDS
                ):
                    handler.pending = False
                    snap = self.create_snapshot()
                    if snap:
                        self.save_metadata()
                        self.logger.info(
                            "[snapshot] id=%s at %s (total recorded: %s)",
                            snap["id"],
                            snap["iso"],
                            len(self.metadata.get("snapshots", [])),
                        )
        except KeyboardInterrupt:
            self.logger.info("Stopping tracking...")
        finally:
            observer.stop()
            observer.join()

    def run_recovery(self) -> None:
        snap = self.parse_recover_point(self.args.recover)
        self.restore_snapshot(snap)
        self.logger.info(
            "Restored '%s' to snapshot id=%s created at %s",
            self.input_path,
            snap["id"],
            snap["iso"],
        )
        self.logger.info("Version store: %s", self.version_root)

    # ---------- Entry ----------

    def run(self) -> None:
        if self.args.track:
            self.run_tracking()
        else:
            self.run_recovery()


def main() -> None:
    app = FileTrackerApp()
    app.run()


if __name__ == "__main__":
    main()
