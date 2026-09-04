"""Corpus-lock gate for the APHIS scripts (ratified pattern, 2026-07-11 spec §0.12).

Wired 2026-08-28. Before this, BOTH mutating scripts were unwired and the rule was applied
by feel: a session stopped before 05_build_database.py (rebuild "felt" destructive) and ran
02_extract_all.py --reset (which unlinks state and overwrites raw/) without flagging it.
A rule keyed on felt destructiveness passes the one that matters.

Acquire -> write -> release in ONE process, holding a real live pid for the whole write.
Failure to acquire is a HARD STOP (exit 3), never a warning.
Neither script is called by cron (verified 2026-08-28: 0 matches in the workstation crontab
and in weekly/monthly_update.sh), so this cannot convert a scheduled run into a silent failure.
"""
import atexit, os, subprocess, sys

LOCK_CLI = "/mnt/data/datadawn/tools/corpus_lock.py"
CORPUS = "aphis"
# #709 option (a), ruling 2026-09-04. These scripts are PUBLIC (openregulations-public);
# off the DataDawn estate the lock tool does not exist, so the gate must not turn a public
# user's run into a traceback. Two signals, on purpose, because environment detection keyed
# on ONE absent thing is how a spoofed header became a bypass the same day:
#   ESTATE_ROOT present  -> this is the estate: the lock tool AND the lock dir MUST exist,
#                           and a missing one is an ERROR (exit 3), never a no-op — the gate
#                           matters most here, and "locks/ got renamed" must not ungate it.
#   ESTATE_ROOT absent   -> off-box: log the no-op loudly and proceed.
# Residual risk, recorded: a run on the estate with /mnt/data/datadawn itself unmounted is
# indistinguishable from off-box. There is no more robust signal than the estate root that
# does not also require configuration the public copy cannot carry.
ESTATE_ROOT = "/mnt/data/datadawn"
LOCK_DIR = os.path.join(ESTATE_ROOT, "locks")


def _on_estate() -> bool:
    return os.path.isdir(ESTATE_ROOT)


def acquire(intent: str, label: str, ttl_hours: float = 6.0) -> None:
    if not _on_estate():
        sys.stderr.write(f"corpus gate: {ESTATE_ROOT} not present — off the DataDawn estate, no corpus lock to take; "
                         f"proceeding UNGATED ({label}).\n")
        return
    missing = [p for p in (LOCK_CLI, LOCK_DIR) if not os.path.exists(p)]
    if missing:
        sys.stderr.write(f"CORPUS GATE ERROR: on the estate ({ESTATE_ROOT} present) but missing {missing} — "
                         f"refusing to run ungated. Restore the lock tool/dir (~/.datadawn-tools.git) or see CLAUDE.md break-glass.\n")
        sys.exit(3)
    r = subprocess.run(
        [sys.executable, LOCK_CLI, "acquire", CORPUS,
         "--intent", intent, "--label", label,
         "--ttl-hours", str(ttl_hours), "--pid", str(os.getpid()),
         "--print-token"],
        capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(
            f"CORPUS LOCK REFUSED (rc={r.returncode}) — another writer holds '{CORPUS}'.\n"
            f"{(r.stdout or '') + (r.stderr or '')}\n"
            "HARD STOP. Investigate the holder; never clear a lock silently.\n")
        sys.exit(3)
    token = (r.stdout or "").strip().split("\n")[-1].strip()
    sys.stderr.write(f"corpus lock acquired: {CORPUS} ({label})\n")
    atexit.register(_release, token)


def _release(token: str) -> None:
    """Release by TOKEN. 2026-08-28: the first version passed --pid, which `release` does
    NOT accept — argparse exited rc=2 and the lock was left STALE-DEAD-PID after a
    successful write. It was never caught because the gate test exercised acquire twice
    (free, then held) and never exercised release at all. Testing acquire is half a test."""
    r = subprocess.run([sys.executable, LOCK_CLI, "release", CORPUS, "--token", token],
                       capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(f"WARNING: corpus lock release FAILED rc={r.returncode}: "
                         f"{(r.stdout or '')+(r.stderr or '')}\n")
