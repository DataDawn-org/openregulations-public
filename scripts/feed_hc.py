#!/usr/bin/env python3
"""feed_hc.py — the restarted feeds' health checks: a verdict from a run's own facts, and the ping
(decisions_log §591, §699 step 3; relays datadawn green #1 to #3).

Why a verdict and not an exit code: under decisions_log §22 a failed download inside the weekly or monthly exits 0,
so a stopped feed is silent. Each restarted feed's loader calls report() at the end of a scheduled run with the facts
that run measured. The feed's check gets SUCCESS only when those facts show the feed did its job, and FAIL with the
reasons otherwise. A fact that is missing or malformed is a FAIL, never a success: a verdict that cannot tell must not
look like one that can (CLAUDE.md, "ask what a check emits when the check is broken").

Facts (a dict). FAIL on any of:
  listed          int >= 1. How many records the source listing returned. None or missing: the listing itself
                  failed. 0: an empty success, which is not an absence.
  min_listed      int (optional). FAIL when listed is below it, e.g. last run's count for a list that only grows
                  (a type whose listing failed part way).
  errors          int >= 0, required (absent is a FAIL). FAIL above allowed_errors.
  allowed_errors  int >= 0 (default 0). Known persistent failures, named in `note`.
  source_newest, local_newest   strings compared as text (ISO dates, or quarters like 2026Q2). When source_newest
                  is given, FAIL if local_newest is missing or older: the run left the copy behind its source.
  note            str (optional), carried into the ping body.

The ping: parity_harness/hc_ping_url_env.py under load-secrets with the feed's one-name manifest, so the process
gets the ping URL for that one check and nothing else (the dump-uptake pattern, tools/run_dump_uptake.sh). Exit 78
from load-secrets (the name is not in the store) means the check has not been created yet: each is created by
feed_hc_create.py when its feed's schedule lands (relay datadawn green #2). That is logged, not raised. report()
never raises into the loader and never changes its exit code; it returns 0 for a success verdict, 1 for a fail.
Test: selftest_feed_hc.py.
"""
import subprocess
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
PING = SCRIPTS.parent.parent / "parity_harness" / "hc_ping_url_env.py"
LOAD_SECRETS = str(Path.home() / ".local/bin/load-secrets")   # home-relative; absolute at run time, since cron's PATH lacks ~/.local/bin

DAY = 86400
# feed -> store name, manifest, check name, period and grace (seconds) of the schedule that runs it
FEEDS = {
    "hearings": ("HC_DATADAWN_FEED_HEARINGS", "hc_feed_hearings.secrets.manifest",
                 "DataDawn feed: hearings (16_committee_hearings.py, monthly)", 31 * DAY, 7 * DAY),
    "house_disbursements": ("HC_DATADAWN_FEED_HOUSE_DISBURSEMENTS", "hc_feed_house_disbursements.secrets.manifest",
                            "DataDawn feed: House disbursements (40 + 41, monthly)", 31 * DAY, 7 * DAY),
    "bills": ("HC_DATADAWN_FEED_BILLS", "hc_feed_bills.secrets.manifest",
              "DataDawn feed: bills and CBO re-fetch (09_congress_gov.py, weekly)", 7 * DAY, 2 * DAY),
    "oge": ("HC_DATADAWN_FEED_OGE", "hc_feed_oge.secrets.manifest",
            "DataDawn feed: OGE PAS index (monthly)", 31 * DAY, 7 * DAY),
    "wh_personnel": ("HC_DATADAWN_FEED_WH_PERSONNEL", "hc_feed_wh_personnel.secrets.manifest",
                     "DataDawn feed: White House personnel (33_wh_personnel_reports.py, monthly)", 31 * DAY, 7 * DAY),
}


def _int(facts, key, default=None):
    v = facts.get(key, default)
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(key)
    return v


def verdict(facts):
    """-> (ok, reasons). ok is True only when every rule above holds."""
    reasons = []
    if not isinstance(facts, dict):
        return False, ["facts are not a dict: nothing can be judged"]
    listed = facts.get("listed")
    if listed is None:
        reasons.append("the source listing was not read (listed missing): a stopped feed, not an empty one")
    elif isinstance(listed, bool) or not isinstance(listed, int):
        reasons.append(f"listed is not a count ({type(listed).__name__})")
    elif listed < 1:
        reasons.append("the source listing returned nothing: an empty success is not an absence")
    elif "min_listed" in facts:
        try:
            if listed < _int(facts, "min_listed"):
                reasons.append(f"listed {listed} is below {facts['min_listed']} (a listing that failed part way)")
        except ValueError:
            reasons.append("min_listed is not a count")
    try:
        errors = _int(facts, "errors")
        allowed = _int(facts, "allowed_errors", 0)
        if errors < 0 or allowed < 0:
            reasons.append("errors or allowed_errors is negative")
        elif errors > allowed:
            reasons.append(f"{errors} fetch or parse error(s), {allowed} allowed")
    except ValueError as e:
        reasons.append(f"{e} is missing or not a count")
    src = facts.get("source_newest")
    if src is not None:
        loc = facts.get("local_newest")
        if not isinstance(src, str) or not isinstance(loc, str) or not loc:
            reasons.append(f"cannot compare newest: source {src!r}, held {loc!r}")
        elif loc < src:
            reasons.append(f"behind the source: newest held {loc}, source {src}")
    return not reasons, reasons


def body(feed, facts, ok, reasons):
    lines = [f"feed: {feed}", f"verdict: {'SUCCESS' if ok else 'FAIL'}"]
    lines += [f"  - {r}" for r in reasons]
    if isinstance(facts, dict):
        for k in ("listed", "min_listed", "errors", "allowed_errors", "source_newest", "local_newest", "added", "note"):
            if k in facts:
                lines.append(f"{k}: {facts[k]}")
    return "\n".join(lines) + "\n"


def report(feed, facts, log=print, runner=subprocess.run):
    """Judge the run and ping the feed's check. Returns 0 (success verdict) or 1 (fail); never raises."""
    if feed not in FEEDS:
        log(f"feed_hc: REFUSED: unknown feed {feed!r}; nothing sent")
        return 1
    ok, reasons = verdict(facts)
    name, manifest, *_ = FEEDS[feed]
    cmd = [LOAD_SECRETS, "--manifest", str(SCRIPTS / manifest), "--", "python3", str(PING), name, "0" if ok else "1"]
    try:
        r = runner(cmd, input=body(feed, facts, ok, reasons).encode(), capture_output=True, timeout=90)
        rc = r.returncode
    except Exception as e:                       # never break the loader over its signal
        rc = None
        log(f"feed_hc: {feed}: verdict {'SUCCESS' if ok else 'FAIL'}; the ping could not run ({type(e).__name__})")
    if rc == 0:
        log(f"feed_hc: {feed}: verdict {'SUCCESS' if ok else 'FAIL'} pinged" + ("" if ok else f" ({'; '.join(reasons)})"))
    elif rc == 78:
        log(f"feed_hc: {feed}: verdict {'SUCCESS' if ok else 'FAIL'}; NOT PINGED: {name} is not in the store, so the "
            f"check has not been created yet (feed_hc_create.py, when the feed's schedule lands)")
    elif rc is not None:
        log(f"feed_hc: {feed}: verdict {'SUCCESS' if ok else 'FAIL'}; the ping FAILED (exit {rc})")
    return 0 if ok else 1
