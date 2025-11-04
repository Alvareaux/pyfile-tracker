# pyfile-tracker

Continuous incremental file versioning tool for a folder, based on `git` and `watchdog`.

- Tracks a directory recursively.
- Creates **incremental snapshots** as git commits.
- Keeps either:
  - last **N** snapshots, or
  - all snapshots in a **time window** like `30m`, `1h`, `1d`.
- Restores to:
  - a **revision index** (`0` = latest, `1` = previous, `-1` = earliest, …), or
  - closest snapshot **before** a given timestamp / datetime in ISO format / timedelta (e.g. `2025-01-01T12:00:00`, `1762280492`, `2h`).

Version stores live outside the tracked folder, per drive, e.g.:

- Windows: `D:\.pyfile_tracker\<hash>`
- POSIX: `~/.pyfile_tracker/<hash>`

## Installation

```bash
pip install .
# or in editable mode
pip install -e .