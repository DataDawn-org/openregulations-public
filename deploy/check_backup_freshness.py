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
import re
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


def newest_date_token(names):
    """Newest YYYYMMDD_HHMM token embedded in snapshot filenames (string-sortable)."""
    toks = [m.group(1) for n in names if (m := re.search(r'(\d{8}_\d{4})', n))]
    return max(toks) if toks else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument('--dir', help='Local backup directory')
    src.add_argument('--rclone-remote', help='rclone remote path (e.g. b2:bucket/prefix)')
    ap.add_argument('--max-age-days', type=float, required=True)
    ap.add_argument('--glob', default='openregs-predeploy-*.db')
    ap.add_argument('--cross-check-dir',
                    help='Local backup dir; STALE if its newest snapshot date is ahead '
                         'of the off-site newest (a deploy push that did not propagate). '
                         'Independent of backup_health_report.py parity by design.')
    ap.add_argument('--cross-check-grace-hours', type=float, default=6.0,
                    help='Only alarm the cross-check once the newest local snapshot is '
                         'this old -- lets an in-flight deploy push settle; a genuine '
                         'failure is still caught on the next daily run. Default 6h.')
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

    # Cross-check: off-site must not be STALER than the newest local snapshot.
    # Catches "a deploy ran + wrote a local snapshot but the off-site push silently
    # failed" — invisible to the age window alone (the prior file is still < N days).
    # Deliberately independent of backup_health_report.py's parity check (which keys on
    # the VPS live-deploy mtime over ssh): no shared code -> no correlated failure.
    #
    # INVARIANT (load-bearing): --cross-check-dir MUST be populated from the VPS
    # (deploy.sh rsync-down, currently ~L368), NEVER from B2. If a future convenience
    # edit makes that dir B2-sourced, both sides go stale together on a failed push and
    # this check silently becomes decorative (false GREEN). Keep the dir's source != B2.
    if args.cross_check_dir and args.rclone_remote:
        local = list_local(args.cross_check_dir, args.glob)
        off_tok = newest_date_token([n for _, n in files])
        loc_tok = newest_date_token([n for _, n in local]) if local else None
        if loc_tok and off_tok and loc_tok > off_tok:
            # Local has a newer-dated snapshot than off-site: either a failed push,
            # or a deploy whose push is still in flight. Only alarm once the local
            # snapshot has aged past the grace window; otherwise defer to the next
            # daily run, which catches a genuine failure cleanly (no false-RED mid-deploy).
            age_h = (time.time() - max(m for m, _ in local)) / 3600
            if age_h >= args.cross_check_grace_hours:
                print(f"STALE: off-site newest ({off_tok}) is BEHIND local newest "
                      f"({loc_tok}, written {age_h:.1f}h ago) in {label} -- a deploy "
                      f"snapshot did not propagate off-site", file=sys.stderr)
                return 2
            print(f"cross-check deferred: local {loc_tok} is newer but only {age_h:.1f}h "
                  f"old (< {args.cross_check_grace_hours}h grace) -- push may be in "
                  f"flight; re-checks next run")
        else:
            ok = bool(loc_tok and off_tok and off_tok >= loc_tok)
            print(f"cross-check: off-site {off_tok} vs local {loc_tok} "
                  f"({'ok' if ok else 'skipped (a side empty)'})")

    print(f"fresh: newest backup is {age_days:.1f} days old "
          f"({newest_name}) in {label}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
