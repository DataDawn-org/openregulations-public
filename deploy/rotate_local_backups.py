#!/usr/bin/env python3
"""
Count-based retention for a local directory of backup files.

Lists files matching --glob in --dir, sorts by mtime descending, keeps the
newest --keep, deletes the rest. Stateless and idempotent.

Usage:
    rotate_local_backups.py --dir PATH --keep N [--glob PATTERN]

Exit codes:
    0  success (including no-op on empty dir)
    1  argument or filesystem error
    2  nothing matched the glob (distinct from "matched, nothing to delete")
"""
from __future__ import annotations
import argparse
import glob
import os
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--dir', required=True, help='Directory containing backup files')
    ap.add_argument('--keep', type=int, required=True, help='Number of most-recent files to keep (>=1)')
    ap.add_argument('--glob', default='openregs-predeploy-*.db', help='Glob pattern relative to --dir')
    args = ap.parse_args()

    if args.keep < 1:
        print(f"ERROR: --keep must be >= 1 (got {args.keep})", file=sys.stderr)
        return 1

    if not os.path.isdir(args.dir):
        print(f"ERROR: directory not found: {args.dir}", file=sys.stderr)
        return 1

    pattern = os.path.join(args.dir, args.glob)
    files = [f for f in glob.glob(pattern) if os.path.isfile(f)]
    if not files:
        print(f"no files match {pattern} (empty tier)", file=sys.stderr)
        return 2

    files.sort(key=os.path.getmtime, reverse=True)
    to_keep = files[:args.keep]
    to_delete = files[args.keep:]

    print(f"rotate: {len(files)} file(s) matched {args.glob!r} in {args.dir}")
    for f in to_keep:
        print(f"  KEEP    {os.path.basename(f)}")
    for f in to_delete:
        print(f"  DELETE  {os.path.basename(f)}")
        try:
            os.remove(f)
        except OSError as e:
            print(f"  ERROR removing {f}: {e}", file=sys.stderr)
            return 1

    # Sweep orphan SQLite sidecars (-shm/-wal/-journal) whose base .db is gone —
    # both the ones we just deleted and any pre-existing orphans. The online-backup
    # API can leave a 0-byte -wal / 32 KB -shm next to a snapshot; rotation never
    # tracked them, so they accumulated (a handful were cleaned manually 2026-05-30).
    # Sidecars of KEPT backups are left alone (they belong to a live snapshot).
    swept = 0
    for sfx in ('-shm', '-wal', '-journal'):
        for side in glob.glob(os.path.join(args.dir, args.glob + sfx)):
            base = side[: -len(sfx)]
            if not os.path.exists(base):
                try:
                    os.remove(side)
                    print(f"  SIDECAR {os.path.basename(side)} (orphan)")
                    swept += 1
                except OSError as e:
                    print(f"  WARN could not remove sidecar {side}: {e}", file=sys.stderr)

    print(f"rotate: kept {len(to_keep)}, deleted {len(to_delete)}, sidecars swept {swept}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
