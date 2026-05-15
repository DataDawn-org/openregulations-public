#!/usr/bin/env python3
"""
Daily staleness check for a backup tier.

Fails if no file matching --glob is newer than --max-age-days. Intended for
cron + hc.io ping so we learn about silent propagation failures.

Supports two backup tiers:
  --dir PATH                 Local filesystem (tier 2).
  --rclone-remote PATH       Cloud backup via rclone (tier 3, e.g. B2).

Usage:
    check_backup_freshness.py --dir PATH --max-age-days N [--glob PATTERN]
    check_backup_freshness.py --rclone-remote REMOTE_PATH --max-age-days N [--glob PATTERN]

Exit codes:
    0  fresh enough
    2  stale or missing (hc.io fires alert)
"""
from __future__ import annotations
import argparse
import fnmatch
import glob
import os
import subprocess
import sys
import time
from datetime import datetime, timezone


def list_local(dir_path: str, pattern: str) -> list[tuple[float, str]] | None:
    """Return [(mtime_epoch, basename), ...] for files matching pattern in dir.
    Returns None if dir is missing."""
    if not os.path.isdir(dir_path):
        print(f"STALE: directory missing: {dir_path}", file=sys.stderr)
        return None
    paths = [f for f in glob.glob(os.path.join(dir_path, pattern)) if os.path.isfile(f)]
    return [(os.path.getmtime(p), os.path.basename(p)) for p in paths]


def list_rclone(remote: str, pattern: str) -> list[tuple[float, str]] | None:
    """Return [(mtime_epoch, basename), ...] from `rclone lsf --format tp`.
    Returns None on rclone failure."""
    try:
        res = subprocess.run(
            ['rclone', 'lsf', remote, '--format', 'tp'],
            capture_output=True, text=True, timeout=60,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"STALE: rclone lsf {remote} failed: {e}", file=sys.stderr)
        return None
    if res.returncode != 0:
        print(f"STALE: rclone lsf {remote} exit {res.returncode}: {res.stderr.strip()}",
              file=sys.stderr)
        return None
    out: list[tuple[float, str]] = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        # Format is "YYYY-MM-DD HH:MM:SS;filename"
        try:
            ts, name = line.split(';', 1)
        except ValueError:
            continue
        if not fnmatch.fnmatch(name, pattern):
            continue
        try:
            dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        out.append((dt.timestamp(), name))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument('--dir', help='Local backup directory')
    src.add_argument('--rclone-remote', help='rclone remote path (e.g. b2:bucket/prefix)')
    ap.add_argument('--max-age-days', type=float, required=True)
    ap.add_argument('--glob', default='openregs-predeploy-*.db')
    args = ap.parse_args()

    if args.rclone_remote:
        files = list_rclone(args.rclone_remote, args.glob)
        label = args.rclone_remote
    else:
        files = list_local(args.dir, args.glob)
        label = args.dir

    if files is None:
        return 2
    if not files:
        print(f"STALE: no files match {args.glob} in {label}", file=sys.stderr)
        return 2

    newest_mtime, newest_name = max(files, key=lambda x: x[0])
    age_days = (time.time() - newest_mtime) / 86400

    if age_days > args.max_age_days:
        print(f"STALE: newest backup is {age_days:.1f} days old "
              f"(threshold: {args.max_age_days}): {newest_name}",
              file=sys.stderr)
        return 2

    print(f"fresh: newest backup is {age_days:.1f} days old "
          f"({newest_name}) in {label}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
