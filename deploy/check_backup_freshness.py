#!/usr/bin/env python3
"""
Daily staleness check for a backup directory.

Fails if no file matching --glob is newer than --max-age-days. Intended for
cron + hc.io ping so we learn about silent propagation failures.

Usage:
    check_backup_freshness.py --dir PATH --max-age-days N [--glob PATTERN]

Exit codes:
    0  fresh enough
    2  stale or missing (hc.io fires alert)
"""
from __future__ import annotations
import argparse
import glob
import os
import sys
import time


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--dir', required=True)
    ap.add_argument('--max-age-days', type=float, required=True)
    ap.add_argument('--glob', default='openregs-predeploy-*.db')
    args = ap.parse_args()

    if not os.path.isdir(args.dir):
        print(f"STALE: directory missing: {args.dir}", file=sys.stderr)
        return 2

    files = [f for f in glob.glob(os.path.join(args.dir, args.glob)) if os.path.isfile(f)]
    if not files:
        print(f"STALE: no files match {args.glob} in {args.dir}", file=sys.stderr)
        return 2

    newest_file = max(files, key=os.path.getmtime)
    age_days = (time.time() - os.path.getmtime(newest_file)) / 86400

    if age_days > args.max_age_days:
        print(f"STALE: newest backup is {age_days:.1f} days old "
              f"(threshold: {args.max_age_days}): {os.path.basename(newest_file)}",
              file=sys.stderr)
        return 2

    print(f"fresh: newest backup is {age_days:.1f} days old "
          f"({os.path.basename(newest_file)})")
    return 0


if __name__ == '__main__':
    sys.exit(main())
