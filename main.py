#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import sys
import time
import re
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


METADATA_FILE = "metadata.json"
SNAPSHOT_DIR = "snapshots"
DEBOUNCE_SECONDS = 2.0  # wait this long after last change before snapshot


def debug(msg: str) -> None:
    # Uncomment for debugging
    # print(f"[DEBUG] {msg}")
    pass


def get_default_base_for_input(input_path: str) -> str:
    """
    On Windows: per-drive root, e.g. D:\\.pyfile_tracker
    On POSIX:   ~/.pyfile_tracker
    """
    abs_input = os.path.abspath(os.path.expanduser(input_path))
    drive, _ = os.path.splitdrive(abs_input)

    if os.name == "nt" and drive:
        # drive like 'D:' -> 'D:\\'
        root = drive + os.path.sep
        base = os.path.join(root, ".pyfile_tracker")
    else:
        home = os.path.expanduser("~")
        base = os.path.join(home, ".pyfile_tracker")

    os.makedirs(base, exist_ok=True)
    return base


def get_version_root(input_path: str, output_path: Optional[str]) -> str:
    if output_path:
        root = os.path.abspath(os.path.expanduser(output_path))
    else:
        base = get_default_base_for_input(input_path)
        norm = os.path.abspath(os.path.expanduser(input_path))
        digest = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12]
        root = os.path.join(base, digest)
    os.makedirs(root, exist_ok=True)
    return root


def load_metadata(version_root: str) -> Dict[str, Any]:
    path = os.path.join(version_root, METADATA_FILE)
    if not os.path.exists(path):
        return {
            "input_path": None,
            "snapshots": []  # list of {id, timestamp, iso}
        }
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {
            "input_path": None,
            "snapshots": []
        }


def save_metadata(version_root: str, metadata: Dict[str, Any]) -> None:
    path = os.path.join(version_root, METADATA_FILE)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    os.replace(tmp_path, path)


def ensure_input_path(metadata: Dict[str, Any], input_path: str) -> None:
    abs_in = os.path.abspath(os.path.expanduser(input_path))
    if metadata.get("input_path") is None:
        metadata["input_path"] = abs_in
    elif os.path.abspath(metadata["input_path"]) != abs_in:
        raise SystemExit(
            f"Version store already linked to a different input path: {metadata['input_path']}"
        )


def ensure_version_root_not_in_input(input_path: str, version_root: str) -> None:
    abs_in = os.path.abspath(os.path.expanduser(input_path))
    vr = os.path.abspath(version_root)
    try:
        common = os.path.commonpath([abs_in, vr])
    except ValueError:
        # Different drives on Windows -> safe
        return
    if common == abs_in:
        raise SystemExit(
            "Version directory is inside the tracked folder. "
            "Please move it outside or specify -o to a separate path."
        )


def list_snapshots(metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    snaps = metadata.get("snapshots", [])
    snaps.sort(key=lambda s: s["timestamp"])
    return snaps


def next_snapshot_id(metadata: Dict[str, Any]) -> int:
    snaps = metadata.get("snapshots", [])
    if not snaps:
        return 1
    return max(s["id"] for s in snaps) + 1


def copy_tree(src_root: str, dst_root: str) -> None:
    for dirpath, dirnames, filenames in os.walk(src_root):
        rel_dir = os.path.relpath(dirpath, src_root)
        if rel_dir == ".":
            rel_dir = ""
        target_dir = os.path.join(dst_root, rel_dir) if rel_dir else dst_root
        os.makedirs(target_dir, exist_ok=True)
        for name in filenames:
            src_file = os.path.join(dirpath, name)
            dst_file = os.path.join(target_dir, name)
            shutil.copy2(src_file, dst_file)


def parse_retention(k_value: str) -> Tuple[str, Any]:
    """
    Returns ('count', N) or ('time', seconds)
    """
    k_value = k_value.strip()
    # Try integer N (keep last N snapshots)
    try:
        n = int(k_value)
        if n <= 0:
            raise ValueError("N must be positive")
        return "count", n
    except ValueError:
        pass

    # Try timeframe like 30m, 1h, 1d, 45s
    m = re.fullmatch(r"(\d+)\s*([smhd])", k_value, re.IGNORECASE)
    if not m:
        raise SystemExit(
            f"Invalid -k value '{k_value}'. Use integer N or timeframe like '30m', '1h', '1d'."
        )
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
        raise SystemExit(f"Unsupported timeframe unit: {unit}")
    return "time", seconds


def prune_snapshots(version_root: str, metadata: Dict[str, Any],
                    mode: str, param: Any) -> None:
    snaps = list_snapshots(metadata)
    now_ts = time.time()

    keep_ids = set()
    if mode == "count":
        n = param
        snaps_to_keep = snaps[-n:] if n < len(snaps) else snaps
        keep_ids = {s["id"] for s in snaps_to_keep}
    elif mode == "time":
        seconds = param
        cutoff = now_ts - seconds
        snaps_to_keep = [s for s in snaps if s["timestamp"] >= cutoff]
        keep_ids = {s["id"] for s in snaps_to_keep}
    else:
        raise ValueError("Unknown retention mode")

    # Delete removed snapshots from disk
    for s in snaps:
        if s["id"] not in keep_ids:
            snap_dir = os.path.join(
                version_root, SNAPSHOT_DIR, f"snapshot_{s['id']:06d}"
            )
            if os.path.isdir(snap_dir):
                shutil.rmtree(snap_dir, ignore_errors=True)

    metadata["snapshots"] = [s for s in snaps if s["id"] in keep_ids]


def create_snapshot(input_path: str, version_root: str,
                    metadata: Dict[str, Any]) -> Dict[str, Any]:
    snaps_dir = os.path.join(version_root, SNAPSHOT_DIR)
    os.makedirs(snaps_dir, exist_ok=True)

    snap_id = next_snapshot_id(metadata)
    snap_dir = os.path.join(snaps_dir, f"snapshot_{snap_id:06d}")

    if os.path.exists(snap_dir):
        raise SystemExit(f"Snapshot directory already exists: {snap_dir}")

    abs_input = os.path.abspath(os.path.expanduser(input_path))
    copy_tree(abs_input, snap_dir)

    ts = time.time()
    iso = datetime.fromtimestamp(ts).isoformat(timespec="seconds")
    snap_meta = {
        "id": snap_id,
        "timestamp": ts,
        "iso": iso,
    }
    metadata.setdefault("snapshots", []).append(snap_meta)
    return snap_meta


def parse_recover_point(r_value: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    snaps = list_snapshots(metadata)
    if not snaps:
        raise SystemExit("No snapshots available to recover from.")

    # First try integer index mode
    try:
        idx = int(r_value)
        n = len(snaps)
        if idx >= 0:
            pos = n - 1 - idx  # 0 -> last, 1 -> previous, etc.
            if pos < 0 or pos >= n:
                raise SystemExit(f"Revision index {idx} out of range (0..{n-1}).")
        else:
            pos = -(idx + 1)  # -1 -> 0 (earliest), -2 -> 1, etc.
            if pos < 0 or pos >= n:
                raise SystemExit(f"Negative revision index {idx} out of range.")
        return snaps[pos]
    except ValueError:
        pass

    # Try timestamp (all digits -> unix time)
    if re.fullmatch(r"\d+(\.\d+)?", r_value):
        try:
            ts = float(r_value)
        except ValueError:
            raise SystemExit(f"Invalid timestamp '{r_value}'.")
    else:
        # Try ISO datetime-ish
        s = r_value.strip()
        if " " in s and "T" not in s:
            s = s.replace(" ", "T")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            raise SystemExit(
                f"Invalid recover point '{r_value}'. "
                "Use int index, Unix timestamp, or ISO datetime (YYYY-MM-DDTHH:MM:SS)."
            )
        ts = dt.timestamp()

    # Find snapshot with timestamp <= ts and maximal
    candidate = None
    for s in snaps:
        if s["timestamp"] <= ts:
            candidate = s
        else:
            break
    if candidate is None:
        raise SystemExit(
            f"No snapshot found at or before timestamp {r_value} "
            f"({datetime.fromtimestamp(ts).isoformat(timespec='seconds')})."
        )
    return candidate


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


def restore_snapshot(input_path: str, version_root: str,
                     snapshot: Dict[str, Any]) -> None:
    abs_input = os.path.abspath(os.path.expanduser(input_path))
    ensure_version_root_not_in_input(abs_input, version_root)

    snaps_dir = os.path.join(version_root, SNAPSHOT_DIR)
    snap_dir = os.path.join(snaps_dir, f"snapshot_{snapshot['id']:06d}")
    if not os.path.isdir(snap_dir):
        raise SystemExit(f"Snapshot directory not found: {snap_dir}")

    # Build relative file sets
    current_files = build_rel_paths(abs_input)
    snap_files = build_rel_paths(snap_dir)

    # Delete files not present in snapshot
    to_delete = current_files - snap_files
    for rel in to_delete:
        path = os.path.join(abs_input, rel)
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)

    # Restore snapshot files
    for rel in snap_files:
        src = os.path.join(snap_dir, rel)
        dst = os.path.join(abs_input, rel)
        dst_dir = os.path.dirname(dst)
        os.makedirs(dst_dir, exist_ok=True)
        shutil.copy2(src, dst)


class ChangeHandler(FileSystemEventHandler):
    def __init__(self, input_root: str):
        super().__init__()
        self.input_root = os.path.abspath(os.path.expanduser(input_root))
        self.pending = False
        self.last_change = 0.0

    def _mark_change(self, path: str) -> None:
        if not path:
            return
        abs_path = os.path.abspath(path)
        # Ensure it's under the input root; handle cross-drive safely
        try:
            inside = os.path.commonpath([abs_path, self.input_root]) == self.input_root
        except ValueError:
            inside = False
        if not inside:
            return
        self.pending = True
        self.last_change = time.time()
        debug(f"Change detected at {abs_path}")

    def on_any_event(self, event):
        if event.is_directory:
            return
        self._mark_change(getattr(event, "src_path", None))
        if hasattr(event, "dest_path"):
            self._mark_change(getattr(event, "dest_path", None))


def run_tracking(input_path: str, version_root: str, keep_value: str, interval: float) -> None:
    abs_input = os.path.abspath(os.path.expanduser(input_path))
    if not os.path.isdir(abs_input):
        raise SystemExit(f"Input path must be an existing directory: {input_path}")

    ensure_version_root_not_in_input(abs_input, version_root)

    metadata = load_metadata(version_root)
    ensure_input_path(metadata, input_path)

    mode, param = parse_retention(keep_value)

    # Optional: ensure we have at least one baseline snapshot
    if not metadata.get("snapshots"):
        snap = create_snapshot(input_path, version_root, metadata)
        prune_snapshots(version_root, metadata, mode, param)
        save_metadata(version_root, metadata)
        print(f"[init] Created baseline snapshot id={snap['id']} at {snap['iso']}")

    handler = ChangeHandler(abs_input)
    observer = Observer()
    observer.schedule(handler, abs_input, recursive=True)
    observer.start()

    print(f"Tracking '{abs_input}'")
    print(f"Version store: {version_root}")
    print(f"Retention: mode={mode}, param={param}  (use Ctrl+C to stop)")

    try:
        while True:
            time.sleep(interval)
            if handler.pending and (time.time() - handler.last_change) >= DEBOUNCE_SECONDS:
                handler.pending = False
                snap = create_snapshot(input_path, version_root, metadata)
                prune_snapshots(version_root, metadata, mode, param)
                save_metadata(version_root, metadata)
                print(
                    f"[snapshot] id={snap['id']} at {snap['iso']} "
                    f"(total kept: {len(metadata.get('snapshots', []))})"
                )
    except KeyboardInterrupt:
        print("\nStopping tracking...")
    finally:
        observer.stop()
        observer.join()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Continuous file versioning tool for a folder."
    )
    p.add_argument(
        "-i",
        "--input",
        required=True,
        help="Path to files to track / recover."
    )
    p.add_argument(
        "-o",
        "--output",
        help="Version store path. Defaults to per-drive app directory based on input path."
    )
    p.add_argument(
        "-k",
        "--keep",
        help=(
            "Retention for tracking mode (continuous): integer N (keep last N snapshots) "
            "or timeframe like '30m', '1h', '1d'."
        ),
    )
    p.add_argument(
        "-r",
        "--recover",
        help=(
            "Recovery mode (one-shot): recover point, either integer revision index "
            "(0=last, 1=previous, -1=earliest, ...) or Unix timestamp or ISO datetime."
        ),
    )
    p.add_argument(
        "-p",
        "--polling-interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds (default: 1.0).",
    )
    args = p.parse_args()

    if bool(args.keep) == bool(args.recover):
        p.error("Exactly one of -k/--keep (tracking) or -r/--recover (recovery) must be provided.")

    return args


def main() -> None:
    args = parse_args()

    input_path = args.input
    version_root = get_version_root(input_path, args.output)

    if args.keep:
        # Continuous tracking mode
        run_tracking(input_path, version_root, args.keep, args.polling_interval)
    else:
        # One-shot recovery mode
        metadata = load_metadata(version_root)
        ensure_input_path(metadata, input_path)
        snap = parse_recover_point(args.recover, metadata)
        restore_snapshot(input_path, version_root, snap)
        print(
            f"Restored '{input_path}' to snapshot id={snap['id']} "
            f"created at {snap['iso']}"
        )
        print(f"Version store: {version_root}")


if __name__ == "__main__":
    main()