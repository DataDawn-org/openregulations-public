#!/usr/bin/env python3
"""
§4 daily Storage & Backup Health report (workstation cron).

Cross-DB (openregs + 990). Runs on the WORKSTATION — the only host with the
DataDawn [b2] rclone remote AND the local backup tier AND ssh to the VPS (the
VPS deliberately has no DataDawn B2 creds; DR is 2-hop B2->ws->VPS by design).

Read-only. Observes end-state. Pages on RED, triggers NO automated remediation:
a transient B2/ssh blip must never make the system thrash storage — that's its
own Saturday. The check's job is to make the silent loud, then a human decides.

Five sections, TWO independent verdicts (so one can't mask the other — the same
don't-multiplex principle as keeping a dedicated hc.io check off site_audit's):

  DISK + HEADROOM  -> "disk" verdict. The June-6-critical signal: does the VPS
                      have ~2x room for the next openregs deploy, and is the
                      `-mtime +5` sweep recovering it? GREEN if room; WARN if
                      tight-but-a-sweepable-backup-will-recover-it (NOT a page —
                      avoids daily false-RED for an expected sweep); RED only if
                      short AND nothing sweepable will fix it (genuinely stuck,
                      or the sweep failed to land — which is itself unmonitored).

  GENERATION-PARITY -> part of the "integrity" verdict. Per DB: is the newest
                      off-site backup from the SAME deploy as the current live
                      build, or is a generation stranded VPS-only? Cause-agnostic
                      net for the May-24/25/28 propagation-gap class. Compares the
                      newest off-site backup's timestamp to the live DB's last
                      deploy (mtime) — the predeploy snapshot is made in the same
                      deploy that swaps live, so a healthy gap is ~hours; a gap of
                      days means a deploy's snapshot never propagated.

  LEDGER INTEGRITY -> part of the "integrity" verdict. The §5a propagation ledger
                      is present; the latest entry's LOCAL copy re-hashes to the
                      recorded sha256 (cheap local-retention check). B2 server-side
                      hash-verify is a NOTED FOLLOW-UP: it needs the §5a entry to
                      also record B2's sha1 (B2 stores sha1, not sha256 — DR drill
                      F-002) so the daily check can compare server-side WITHOUT a
                      48 GB download. Same mechanism keep-1's prune-after-verify
                      (precondition b) needs for its last-local-copy delete.

  LEDGER CURRENCY  -> part of "integrity" (#146a). A live deploy NEWER than the
                      latest ledger row = a propagation that wrote no row (the
                      ledger is success-only, so a failed propagation is invisible
                      by absence — the 2026-06-13 silent-failure class). RED.

  B2 STEADY-STATE  -> part of "integrity" (#147). The keep-N rotation is holding:
                      object count bounded (rotation didn't stop -> unbounded
                      growth), every .db paired with its manifest, and zero
                      unfinished large-file sessions (interrupted uploads accruing
                      billable orphans — one sat 10 days, found by accident).

Verdict -> hc.io: pings DATADAWN_DISK_HC_URL with the disk verdict and
DATADAWN_BACKUP_INTEGRITY_HC_URL with the integrity verdict, each carrying the
full sectioned report as the ping body (hub_smoke pattern). If only one URL is
set, both verdicts fold onto it. Unset -> local-log only (vps-config monitor
convention). --no-ping forces log-only for testing.

Leak discipline: the VPS host and B2 bucket are injected from the (workstation-
private) cron env, never hardcoded, so this file mirrors public clean.

Exit code: 0 if neither verdict is RED (GREEN/WARN ok), else 2.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

SSH_OPTS = ["-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=60",
            "-o", "ServerAliveCountMax=3"]

# Injected from the workstation-private cron env — kept OUT of the script so it
# mirrors public clean (same discipline as check_backup_freshness.py's arg-driven
# remotes + deploy.sh's sanitized public copy).
VPS_HOST = os.environ.get("DATADAWN_VPS_HOST", "")        # e.g. user@<ip>
B2_BUCKET = os.environ.get("DATADAWN_B2_BUCKET", "")       # the DataDawn b2 bucket
B2_REMOTE = os.environ.get("DATADAWN_B2_REMOTE", "")       # rclone remote; REQUIRED if B2_BUCKET set (no silent fallback)

PRED_RE = re.compile(r"predeploy-(\d{8})_(\d{4})\.db$")

# Headroom: an openregs deploy needs current-live (the predeploy backup it makes)
# + the .new upload ~= 2x live. RED if free can't cover it AND nothing sweepable
# will. The daily VPS sweep removes openregs-predeploy-*.db older than this:
SWEEP_MTIME_DAYS = 5
# Healthy off-site lag: the newest off-site backup is made in the same deploy that
# swaps live, so it should trail live's deploy by hours, not days. Beyond this, a
# deploy's snapshot never propagated -> RED.
PARITY_MAX_LAG_DAYS = 2.0
# Ledger currency (#146a): the §5a row is written in the same deploy as the
# snapshot, so the latest ledger entry should trail live's deploy by hours. A live
# deploy this much newer than the newest ledger row = a propagation that wrote no
# row (success-only ledger, blind to failure-by-absence — the 2026-06-13 class).
LEDGER_MAX_LAG_DAYS = 2.0
# keep-N off-site rotation (#147): deploy.sh/update.sh retain this many predeploy
# generations on B2. More than this = rotation stopped (unbounded growth/billing).
EXPECTED_KEEP = 3

GREEN, WARN, RED = "GREEN", "WARN", "RED"
_RANK = {GREEN: 0, WARN: 1, RED: 2}


def worse(a: str, b: str) -> str:
    return a if _RANK[a] >= _RANK[b] else b


def db_configs() -> list[dict]:
    """Per-DB config. B2 remote uses the env-injected bucket (no secret here)."""
    def b2(prefix: str) -> str:
        if not B2_BUCKET:
            return ""
        if not B2_REMOTE:
            # No silent fallback to a default remote — that would check the WRONG
            # bucket (e.g. a frozen legacy off-site bucket) and report a false
            # verdict. Fail loud instead. (Gate 2, 2026-06-03.)
            raise SystemExit("backup_health_report: DATADAWN_B2_BUCKET is set but "
                             "DATADAWN_B2_REMOTE is unset — refusing to guess a remote. "
                             "Set DATADAWN_B2_REMOTE (e.g. 'hetzvps').")
        return f"{B2_REMOTE}:{B2_BUCKET}/{prefix}"
    return [
        {
            "name": "openregs",
            "live": "/opt/openregs/openregs.db",
            "b2": b2("openregs-weekly"),
            "local_dir": "/mnt/data/datadawn/openregs/backups",
            "ledger": "/mnt/data/datadawn/openregs/backups/propagation_ledger.jsonl",
            "is_headroom_db": True,   # openregs drives the disk-headroom math
        },
        {
            "name": "990",
            "live": "/opt/datasette/990data_public.db",
            "b2": b2("990-weekly"),
            "local_dir": "/mnt/data/datadawn/990project/backups",
            "ledger": "/mnt/data/datadawn/990project/backups/propagation_ledger.jsonl",
            "is_headroom_db": False,
        },
    ]


# ── VPS probe (one ssh, JSON out) ─────────────────────────────────────────────

def probe_vps() -> dict | None:
    """Single ssh round-trip: root-fs free/total, /opt sizes, live-DB stat,
    and the sweepable openregs backups (age > SWEEP_MTIME_DAYS). Returns None on
    ssh failure (caller treats an unreachable VPS as RED, not silently green)."""
    remote = r'''
import json, os, subprocess, time
out = {"disk": {}, "opt": {}, "live": {}, "openregs_backups": []}
st = os.statvfs("/")
out["disk"] = {"free_kb": st.f_bavail * st.f_frsize // 1024,
               "total_kb": st.f_blocks * st.f_frsize // 1024}
for p in ("/opt/openregs", "/opt/datasette", "/opt/openregs/backups",
          "/opt/datasette/backups"):
    try:
        r = subprocess.run(["du", "-sk", p], capture_output=True, text=True, timeout=120)
        out["opt"][p] = int(r.stdout.split()[0]) if r.returncode == 0 else None
    except Exception:
        out["opt"][p] = None
for name, p in (("openregs", "/opt/openregs/openregs.db"),
                ("990", "/opt/datasette/990data_public.db")):
    try:
        out["live"][name] = {"size": os.path.getsize(p), "mtime": int(os.path.getmtime(p))}
    except OSError:
        out["live"][name] = None
bdir = "/opt/openregs/backups"
now = time.time()
try:
    for f in os.listdir(bdir):
        if f.startswith("openregs-predeploy-") and f.endswith(".db"):
            fp = os.path.join(bdir, f)
            age = (now - os.path.getmtime(fp)) / 86400
            out["openregs_backups"].append({"name": f, "size": os.path.getsize(fp),
                                             "age_days": round(age, 2)})
except OSError:
    pass
print(json.dumps(out))
'''
    try:
        res = subprocess.run(["ssh", *SSH_OPTS, VPS_HOST, "python3 -"],
                             input=remote, capture_output=True, text=True, timeout=180)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  VPS probe failed: {e}", file=sys.stderr)
        return None
    if res.returncode != 0:
        print(f"  VPS probe ssh exit {res.returncode}: {res.stderr.strip()}", file=sys.stderr)
        return None
    try:
        return json.loads(res.stdout)
    except json.JSONDecodeError:
        print(f"  VPS probe returned non-JSON: {res.stdout[:200]}", file=sys.stderr)
        return None


# ── off-site listing ──────────────────────────────────────────────────────────

def newest_b2(remote: str) -> tuple[float, str] | None:
    """(epoch, name) of newest predeploy backup on B2, or None on failure/empty."""
    if not remote:
        return None
    try:
        res = subprocess.run(["rclone", "lsf", remote, "--format", "tp"],
                             capture_output=True, text=True, timeout=60)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  rclone lsf {remote} failed: {e}", file=sys.stderr)
        return None
    if res.returncode != 0:
        print(f"  rclone lsf {remote} exit {res.returncode}: {res.stderr.strip()}", file=sys.stderr)
        return None
    best = None
    for line in res.stdout.splitlines():
        line = line.strip()
        if ";" not in line:
            continue
        ts, name = line.split(";", 1)
        if not PRED_RE.search(name):
            continue
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if best is None or dt.timestamp() > best[0]:
            best = (dt.timestamp(), name)
    return best


def newest_local(local_dir: str) -> tuple[float, str] | None:
    if not os.path.isdir(local_dir):
        return None
    best = None
    for f in os.listdir(local_dir):
        if not PRED_RE.search(f):
            continue
        fp = os.path.join(local_dir, f)
        if not os.path.isfile(fp):
            continue
        m = os.path.getmtime(fp)
        if best is None or m > best[0]:
            best = (m, f)
    return best


def list_b2_names(remote: str) -> list[str] | None:
    """All object names directly under a B2 prefix (rclone lsf). None on failure
    (caller treats as RED — an unlistable store is not silently 'clean')."""
    if not remote:
        return None
    try:
        res = subprocess.run(["rclone", "lsf", remote],
                             capture_output=True, text=True, timeout=60)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  rclone lsf {remote} failed: {e}", file=sys.stderr)
        return None
    if res.returncode != 0:
        print(f"  rclone lsf {remote} exit {res.returncode}: {res.stderr.strip()}", file=sys.stderr)
        return None
    return [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]


def b2_unfinished_count(bucket_remote: str) -> int | None:
    """Count unfinished B2 large-file sessions (interrupted multipart uploads) —
    billing orphans nothing else watches (#147). Read-only: `rclone cleanup
    --dry-run` lists what it WOULD remove and deletes nothing; we count its
    'unfinished large file' lines. None on rclone failure (caller WARNs)."""
    if not bucket_remote:
        return None
    try:
        res = subprocess.run(["rclone", "cleanup", "--dry-run", "-v", bucket_remote],
                             capture_output=True, text=True, timeout=120)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  rclone cleanup --dry-run {bucket_remote} failed: {e}", file=sys.stderr)
        return None
    if res.returncode != 0:
        print(f"  rclone cleanup --dry-run exit {res.returncode}: {res.stderr.strip()[:200]}",
              file=sys.stderr)
        return None
    text = (res.stdout or "") + (res.stderr or "")
    return sum(1 for ln in text.splitlines() if "unfinished" in ln.lower())


# ── ledger ─────────────────────────────────────────────────────────────────────

def latest_ledger_entry(ledger_path: str) -> dict | None:
    if not os.path.isfile(ledger_path):
        return None
    last = None
    try:
        with open(ledger_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        last = json.loads(line)
                    except json.JSONDecodeError:
                        continue
    except OSError:
        return None
    return last


def sha256_file(path: str) -> str | None:
    import hashlib
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8 << 20), b""):
                h.update(chunk)
    except OSError:
        return None
    return h.hexdigest()


# ── helpers ────────────────────────────────────────────────────────────────────

def gb(kb_or_bytes: float, *, is_bytes: bool = False) -> float:
    return kb_or_bytes / (1024 ** 3 if is_bytes else 1024 ** 2)


def fdate(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d %H:%M")


# ── sections ────────────────────────────────────────────────────────────────────

def section_disk(vps: dict | None, lines: list[str]) -> str:
    lines.append("── DISK + HEADROOM " + "─" * 42)
    if vps is None:
        lines.append("  RED: VPS unreachable — cannot assess headroom (not silently green).")
        return RED
    free_gb = gb(vps["disk"]["free_kb"])
    total_gb = gb(vps["disk"]["total_kb"])
    lines.append(f"  root fs: {free_gb:.1f} G free / {total_gb:.1f} G "
                 f"({100*(1-vps['disk']['free_kb']/vps['disk']['total_kb']):.0f}% used)")
    for p, kb in vps["opt"].items():
        lines.append(f"    {p}: {gb(kb):.1f} G" if kb is not None else f"    {p}: (du failed)")

    live = vps["live"].get("openregs")
    if not live:
        lines.append("  RED: live openregs.db not stat-able — cannot compute headroom.")
        return RED
    live_gb = gb(live["size"], is_bytes=True)
    needed_gb = 2 * live_gb  # predeploy backup (= current live) + .new upload
    lines.append(f"  openregs deploy needs ~2x live = ~{needed_gb:.0f} G "
                 f"(live {live_gb:.1f} G); free = {free_gb:.1f} G")

    if free_gb >= needed_gb:
        lines.append(f"  GREEN: headroom OK for the next openregs deploy "
                     f"(+{free_gb - needed_gb:.0f} G margin).")
        return GREEN

    # Short. Is there a sweepable backup that will recover enough?
    sweepable = [b for b in vps["openregs_backups"] if b["age_days"] > SWEEP_MTIME_DAYS]
    soon = sorted([b for b in vps["openregs_backups"] if b["age_days"] <= SWEEP_MTIME_DAYS],
                  key=lambda b: b["age_days"], reverse=True)
    sweepable_gb = sum(gb(b["size"], is_bytes=True) for b in sweepable)
    if free_gb + sweepable_gb >= needed_gb:
        names = ", ".join(f"{b['name']} ({b['age_days']:.0f}d)" for b in sweepable)
        lines.append(f"  RED: short by {needed_gb - free_gb:.0f} G, but the -mtime+{SWEEP_MTIME_DAYS} "
                     f"sweep will free ~{sweepable_gb:.0f} G NOW from: {names} -> recovers headroom.")
        lines.append("       (sweep is due — if this persists past tomorrow the sweep FAILED; act.)")
        return RED  # sweepable-but-not-yet-swept on the *next* run is itself worth a look
    if soon:
        nb = soon[0]
        eta_days = SWEEP_MTIME_DAYS - nb["age_days"]
        recover_gb = sum(gb(b["size"], is_bytes=True) for b in soon)
        lines.append(f"  WARN: short by {needed_gb - free_gb:.0f} G now, but the newest backup "
                     f"({nb['name']}, age {nb['age_days']:.0f}d, ~{gb(nb['size'], is_bytes=True):.0f} G) "
                     f"sweeps in ~{eta_days:.0f}d -> recovers ~{recover_gb:.0f} G.")
        lines.append("       Off-cycle deploy blocked until then; use manual keep-1 if you must "
                     "deploy sooner. NOT paging — expected, recovering.")
        return WARN
    lines.append(f"  RED: short by {needed_gb - free_gb:.0f} G and NOTHING sweepable will recover it. "
                 f"Genuinely stuck — manual keep-1 / intervention needed before any openregs deploy.")
    return RED


def section_parity(cfg: dict, vps: dict | None, lines: list[str]) -> str:
    name = cfg["name"]
    live = (vps or {}).get("live", {}).get(name) if vps else None
    b2n = newest_b2(cfg["b2"])
    loc = newest_local(cfg["local_dir"])

    if live is None:
        lines.append(f"  [{name}] RED: live DB not stat-able (VPS unreachable or path moved).")
        return RED
    live_deploy = live["mtime"]
    lines.append(f"  [{name}] live last-deploy: {fdate(live_deploy)}")
    if b2n is None:
        lines.append(f"  [{name}] RED: no off-site (B2) backup found "
                     f"({'B2_BUCKET unset' if not cfg['b2'] else cfg['b2']}).")
        return RED
    lines.append(f"  [{name}] newest B2:    {fdate(b2n[0])}  ({b2n[1]})")
    if loc:
        lines.append(f"  [{name}] newest local: {fdate(loc[0])}  ({loc[1]})")
    else:
        lines.append(f"  [{name}] newest local: (none in {cfg['local_dir']})")

    lag_days = (live_deploy - b2n[0]) / 86400
    if lag_days <= PARITY_MAX_LAG_DAYS:
        lines.append(f"  [{name}] GREEN: off-site matches the live deploy within "
                     f"{lag_days:.1f}d (<= {PARITY_MAX_LAG_DAYS}d tolerance).")
        return GREEN
    lines.append(f"  [{name}] RED: newest off-site backup trails the live deploy by "
                 f"{lag_days:.1f}d — a generation is stranded VPS-only (propagation gap). "
                 f"Heals on the next deploy through deploy.sh/update.sh, or re-propagate manually.")
    return RED


def section_ledger(cfg: dict, lines: list[str], live_deploy_epoch: float | None = None) -> str:
    name = cfg["name"]
    entry = latest_ledger_entry(cfg["ledger"])
    if entry is None:
        lines.append(f"  [{name}] (no §5a ledger yet — activates at the first deploy through "
                     f"deploy.sh/update.sh; nothing to verify. Not an error pre-rollout.)")
        return GREEN  # absence pre-rollout is expected, not a failure
    # #146(b) FAILURE ROW: deploy.sh writes a status=FAILED row when a propagation
    # leg fails (instead of silence). A failed latest row = the last propagation did
    # NOT fully land off-site — RED, independent of currency/re-hash.
    if (entry.get("status") == "FAILED" or entry.get("b2_ok") is False
            or entry.get("local_ok") is False):
        lines.append(f"  [{name}] RED: latest ledger row is a FAILED propagation "
                     f"(stage={entry.get('stage', '?')}, rc={entry.get('return_code', '?')}, "
                     f"local_ok={entry.get('local_ok')}, b2_ok={entry.get('b2_ok')}, "
                     f"ts={entry.get('ts')}) — last backup did NOT fully land off-site.")
        return RED
    # #146(a) CURRENCY: a live deploy newer than the latest ledger row means a
    # propagation wrote NO row (success-only ledger -> failure invisible by absence,
    # the 2026-06-13 class). The re-hash below proves the latest row is INTACT; this
    # proves a newer one isn't MISSING.
    if live_deploy_epoch:
        try:
            entry_epoch = datetime.strptime(
                entry.get("ts", ""), "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc).timestamp()
        except (ValueError, TypeError):
            entry_epoch = None
        if entry_epoch is not None and (live_deploy_epoch - entry_epoch) / 86400 > LEDGER_MAX_LAG_DAYS:
            lag = (live_deploy_epoch - entry_epoch) / 86400
            lines.append(f"  [{name}] RED: live deploy ({fdate(live_deploy_epoch)}) is {lag:.1f}d "
                         f"newer than the latest ledger entry ({entry.get('ts')}) — a deploy wrote "
                         f"NO §5a row (propagation/ledger-write failure, the 2026-06-13 "
                         f"silent-absence class; off-site copy may be stale).")
            return RED
    lines.append(f"  [{name}] latest ledger: {entry.get('snapshot_file')} "
                 f"@ {entry.get('ts')} (sha256 {str(entry.get('sha256'))[:12]})")
    local_path = os.path.join(cfg["local_dir"], entry.get("snapshot_file", ""))
    if not os.path.isfile(local_path):
        lines.append(f"  [{name}] WARN: ledger's local copy not on disk "
                     f"(rotated out of keep-3?) — local re-hash skipped. "
                     f"B2 server-side hash-verify is the noted follow-up (needs sha1 in the ledger).")
        return WARN
    actual = sha256_file(local_path)
    if actual == entry.get("sha256"):
        lines.append(f"  [{name}] GREEN: local copy re-hashes clean against the ledger sha256.")
        return GREEN
    lines.append(f"  [{name}] RED: local copy sha256 MISMATCH vs ledger "
                 f"(recorded {str(entry.get('sha256'))[:12]}, now {str(actual)[:12]}) "
                 f"— retention corruption.")
    return RED


# ── B2 steady-state (#147) ───────────────────────────────────────────────────────

def section_b2_steady(cfg: dict, lines: list[str]) -> str:
    """#147 — B2 store steady-state: keep-N rotation holding (object count bounded,
    every .db paired with its manifest). Catches rotation-stopped (unbounded growth)
    and orphaned objects. Read-only rclone lsf."""
    name = cfg["name"]
    if not cfg["b2"]:
        lines.append(f"  [{name}] (B2 unset — steady-state skipped)")
        return GREEN
    names = list_b2_names(cfg["b2"])
    if names is None:
        lines.append(f"  [{name}] RED: B2 listing failed — cannot assert steady-state.")
        return RED
    dbs = sorted(n for n in names if PRED_RE.search(n))
    manifests = {n for n in names if n.endswith(".manifest.json")}
    prefix = cfg["b2"].split(":", 1)[-1]
    lines.append(f"  [{name}] {prefix}: {len(dbs)} db + {len(manifests)} manifest "
                 f"(keep-{EXPECTED_KEEP})")
    if not dbs:
        lines.append(f"  [{name}] RED: zero off-site .db backups present.")
        return RED
    verdict = GREEN
    if len(dbs) > EXPECTED_KEEP:
        lines.append(f"  [{name}] WARN: {len(dbs)} db > keep-{EXPECTED_KEEP} — rotation may have "
                     f"stopped deleting (B2 grows unbounded / billing creep).")
        verdict = worse(verdict, WARN)
    unpaired = [d for d in dbs if f"{d}.manifest.json" not in manifests]
    if unpaired:
        lines.append(f"  [{name}] WARN: {len(unpaired)} db without a paired manifest "
                     f"({', '.join(unpaired[:3])}) — orphaned-object class.")
        verdict = worse(verdict, WARN)
    if verdict == GREEN:
        lines.append(f"  [{name}] GREEN: {len(dbs)} generation(s), all manifest-paired, "
                     f"within keep-{EXPECTED_KEEP}.")
    return verdict


def section_b2_unfinished(bucket_remote: str, lines: list[str]) -> str:
    """#147 — interrupted B2 large-file uploads (billing orphans nothing watches; one
    sat 10 days, found by accident). Read-only `rclone cleanup --dry-run`."""
    n = b2_unfinished_count(bucket_remote)
    if n is None:
        lines.append("  unfinished large-file sessions: WARN — could not enumerate "
                     "(rclone cleanup --dry-run failed); not asserting clean.")
        return WARN
    if n == 0:
        lines.append("  unfinished large-file sessions: GREEN — 0 orphaned multipart uploads.")
        return GREEN
    lines.append(f"  unfinished large-file sessions: WARN — {n} interrupted upload(s) accruing "
                 f"billable orphans (#142 cleans on-run; this is the steady-state watch).")
    return WARN


# ── hc.io ───────────────────────────────────────────────────────────────────────

def pushover_notify(title: str, message: str, priority: int = 1) -> None:
    """Direct Pushover page (mirrors verify_june1_propagation.sh). hc.io's Pushover
    integration sends only the generic "<check> is DOWN" string — NOT the ping body
    (verified 2026-05-31) — so without this a real RED page says *something* broke
    but not *what*. Reads ~/.pushover_keys (PUSHOVER_APP_TOKEN / PUSHOVER_USER_KEY).
    Non-fatal: a notify failure must never fail the check — the hc.io /2 ping + the
    hc.io email channel (which DOES carry the body) are the backstops."""
    tok = usr = None
    try:
        with open(os.path.expanduser("~/.pushover_keys")) as f:
            for line in f:
                line = line.strip()
                if "PUSHOVER_APP_TOKEN" in line and "=" in line:
                    tok = line.split("=", 1)[1].strip().strip('"').strip("'")
                if "PUSHOVER_USER_KEY" in line and "=" in line:
                    usr = line.split("=", 1)[1].strip().strip('"').strip("'")
    except OSError:
        print("  pushover: ~/.pushover_keys unreadable — cannot page", file=sys.stderr)
        return
    if not (tok and usr):
        print("  pushover: token/user not found in ~/.pushover_keys", file=sys.stderr)
        return
    try:
        subprocess.run(["curl", "-fsS", "-m", "20",
                        "-F", f"token={tok}", "-F", f"user={usr}",
                        "-F", f"title={title}", "-F", f"message={message[:1000]}",
                        "-F", f"priority={priority}",
                        "https://api.pushover.net/1/messages.json"],
                       capture_output=True, text=True, timeout=30)
        print("  pushover: sent")
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  pushover: send failed (non-fatal): {e}", file=sys.stderr)


def ping(url: str, verdict: str, body: str, no_ping: bool) -> None:
    if no_ping or not url:
        return
    ec = "2" if verdict == RED else "0"
    try:
        subprocess.run(["curl", "-fsS", "-m", "15", "--retry", "3",
                        "--data-binary", "@-", f"{url.rstrip('/')}/{ec}"],
                       input=body[-9000:], capture_output=True, text=True, timeout=60)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  hc.io ping failed (non-fatal): {e}", file=sys.stderr)


# ── main ────────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="§4 daily Storage & Backup Health report")
    ap.add_argument("--no-ping", action="store_true",
                    help="never ping hc.io (testing); still prints the full report")
    ap.add_argument("--test-page", action="store_true",
                    help="fire a representative RED Pushover page (verify the real alert is legible) and exit")
    ap.add_argument("--simulate-red", action="store_true",
                    help="force a RED integrity verdict through the REAL check path — verifies main's RED branch end-to-end (extraction -> legible Pushover + hc.io /2), not a hand-sent page")
    args = ap.parse_args()

    if args.test_page:
        pushover_notify(
            "DataDawn backup-integrity RED [TEST]",
            "SYNTHETIC TEST — both DBs actually GREEN, ignore/no action.\n"
            "This is the shape of a real alert:\n"
            "VERDICT disk=GREEN integrity=RED\n"
            "[990] RED: off-site backup trails live by 14.7d — a generation is "
            "stranded VPS-only (propagation gap).",
            priority=1)
        print("test-page: fired a representative RED Pushover (synthetic; both DBs GREEN).")
        return 0

    if not VPS_HOST:
        print("FATAL: DATADAWN_VPS_HOST unset — cannot probe the VPS.", file=sys.stderr)
        return 2

    lines: list[str] = []
    lines.append(f"DataDawn Storage & Backup Health — {fdate(time.time())} UTC")
    lines.append("=" * 60)
    if not B2_BUCKET:
        lines.append("WARN: DATADAWN_B2_BUCKET unset — off-site (B2) checks degraded.")

    vps = probe_vps()

    disk_verdict = section_disk(vps, lines)

    lines.append("")
    lines.append("── GENERATION-PARITY (off-site == live deploy?) " + "─" * 13)
    parity_verdict = GREEN
    for cfg in db_configs():
        parity_verdict = worse(parity_verdict, section_parity(cfg, vps, lines))

    lines.append("")
    lines.append("── LEDGER INTEGRITY (§5a retention + currency #146a) " + "─" * 8)
    ledger_verdict = GREEN
    for cfg in db_configs():
        live = (vps or {}).get("live", {}).get(cfg["name"]) if vps else None
        ledger_verdict = worse(ledger_verdict,
                               section_ledger(cfg, lines, live["mtime"] if live else None))

    lines.append("")
    lines.append("── B2 STEADY-STATE (keep-N rotation + orphans #147) " + "─" * 9)
    steady_verdict = GREEN
    for cfg in db_configs():
        steady_verdict = worse(steady_verdict, section_b2_steady(cfg, lines))
    if B2_REMOTE and B2_BUCKET:
        steady_verdict = worse(steady_verdict,
                               section_b2_unfinished(f"{B2_REMOTE}:{B2_BUCKET}", lines))

    integrity_verdict = worse(worse(parity_verdict, ledger_verdict), steady_verdict)

    if args.simulate_red:  # verify main()'s REAL RED path end-to-end (not a hand-sent page)
        integrity_verdict = RED
        lines.append("  [990] RED: off-site backup trails live by 14.7d — a generation is "
                     "stranded VPS-only (propagation gap).  [SIMULATED — both DBs actually GREEN]")

    lines.append("")
    lines.append("=" * 60)
    lines.append(f"VERDICT  disk={disk_verdict}   backup-integrity={integrity_verdict}")
    lines.append("(RED pages a human; NO automated remediation by design.)")
    body = "\n".join(lines)
    print(body)

    disk_url = os.environ.get("DATADAWN_DISK_HC_URL", "")
    integ_url = os.environ.get("DATADAWN_BACKUP_INTEGRITY_HC_URL", "")
    if disk_url or integ_url:
        # Two checks if both URLs given; if only one is set, fold both onto it.
        if disk_url and integ_url:
            ping(disk_url, disk_verdict, body, args.no_ping)
            ping(integ_url, integrity_verdict, body, args.no_ping)
        else:
            url = disk_url or integ_url
            ping(url, worse(disk_verdict, integrity_verdict), body, args.no_ping)

    # Direct Pushover on RED — the LEGIBLE page. hc.io's Pushover sends only the
    # generic "is DOWN" string, so we page the verdict + the specific RED lines so
    # a 2am alert is self-explanatory without opening the dashboard/email.
    if not args.no_ping and RED in (disk_verdict, integrity_verdict):
        red_lines = [ln.strip() for ln in lines if "RED:" in ln]
        msg = (f"disk={disk_verdict}  integrity={integrity_verdict}\n"
               + "\n".join(red_lines[:6]) if red_lines else
               f"disk={disk_verdict}  integrity={integrity_verdict}")
        pushover_notify(
            f"DataDawn backup RED: disk={disk_verdict} integrity={integrity_verdict}",
            msg, priority=1)

    return 2 if RED in (disk_verdict, integrity_verdict) else 0


if __name__ == "__main__":
    sys.exit(main())
