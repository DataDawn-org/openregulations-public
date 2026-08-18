#!/usr/bin/env python3
"""
Daily updater for open-for-comment documents.

Builds a standalone open_comments.db with all currently open-for-comment
documents from Regulations.gov. This is a SEPARATE database from the main
openregs.db — Datasette serves both, and the explore page queries
open_comments for the "Open for Comment" tab.

This design prevents corruption of the main 26GB openregs.db by never
writing to it. The open_comments.db is rebuilt from scratch each run
(atomic write via temp file + rename).

Usage:
    python3 20_daily_open_comments.py                    # auto-detect path
    python3 20_daily_open_comments.py --db /path/to.db   # specify output path
    python3 20_daily_open_comments.py --dry-run           # preview without writing

VPS cron (6:30 AM UTC daily):
    30 6 * * * cd /opt/openregs/scripts && python3 20_daily_open_comments.py >> /opt/openregs/logs/daily_open_comments.log 2>&1
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.json"

# Output paths (auto-detected)
VPS_DIR = Path("/opt/openregs")
LOCAL_DIR = Path(__file__).resolve().parent.parent
DB_NAME = "open_comments.db"

API_BASE = "https://api.regulations.gov/v4"

PAGE_SIZE = 250  # The API max, measured 2026-08-09 (251 -> 400 "Page size
                 # parameter is greater than allowed. Maximum value is 250.").
                 # Raised from 25 on 2026-08-10 after measuring that the page
                 # cap is PAGE-INDEXED, not a record ceiling: page 40 serves at
                 # size 250 exactly as at size 25, so the per-slice ceiling is
                 # MAX_PAGES*250 = 10,000, not 1,000.
                 #
                 # ⚠ THIS FLIPS THE NORMAL PATH. The path test is
                 # `total <= MAX_PAGES * PAGE_SIZE`; at 250 that is 10,000, and
                 # the daily total is ~1,020, so the job now takes SINGLE-PASS
                 # and the windowed branch is dormant until the open-comment
                 # universe exceeds 10,000. Measured effect: 63 requests -> 6.
                 # (The earlier "~340 requests" figure was wrong — a worst-case
                 # bound quoted as a typical value. Actual: 25 windows, 13 of
                 # them empty, 61 paginated + 2 probes = 63.)
                 #
                 # Single-pass queries [ge] today with NO upper bound, so it is
                 # not subject to the window-tiling gap at all.

MAX_PAGES = 40  # API hard limit: page[number] max is 40.
                # Measured 2026-08-09 on /v4/documents: page 40 -> 200,
                # page 41 -> 400 "Page number parameter is greater than
                # allowed. Maximum value is 40." Was wrongly 20 — the same
                # pre-#246 belief that silently capped every >5K slice in
                # 03_regs_gov_comments.py at half its real depth (queue #462).

# Pagination MUST carry an explicit, total-order sort. Without one the API does
# not guarantee a stable order across pages, and the same filtered query
# silently SKIPS and REPEATS records — with nothing in the response saying so.
# Measured 2026-08-09 (queue #462): a 305-document window returned 296 / 303 /
# 296 unique ids across three IDENTICAL unsorted runs — i.e. lossy AND
# non-deterministic.
#
# NOT bare `lastModifiedDate`: it is mutable and heavily tied (38/250 rows shared
# a timestamp; largest tie group 6), and a tie group with no tiebreaker is the
# skip/repeat mechanism itself — it still lost 1 of 305 reproducibly across 3
# repeats. The composite adds `documentId`, which is unique and immutable, as a
# tiebreaker, making the whole key a TOTAL order while keeping lastModifiedDate
# usable as a resume cursor if a slice ever has to be paged past a ceiling.
#
# VERIFIED APPLIED, not merely accepted (2026-08-10) — a 200 with default
# ordering is indistinguishable from a 200 with ours, so acceptance proves
# nothing. All three signals confirm both components took effect:
#   * lastModifiedDate is monotonic across the page   (bare documentId: False)
#   * within every tie group, ids ascend              (bare lastModifiedDate: False)
#   * the ordering differs from BOTH single-key orderings
# Exactness: 405/405 unique on 3/3 repeats, matching bare documentId.
SORT_KEY = "lastModifiedDate,documentId"

# Retry policy. queue #462 follow-up: 429s previously consumed the retry budget
# and then returned None, which the caller silently turned into a short page.
MAX_ATTEMPTS = 4
RATE_LIMIT_DEFAULT_WAIT = 60      # only when the server sends no Retry-After
RATE_LIMIT_MAX_WAIT = 1800        # cap a pathological Retry-After (regs.gov has sent >1,600s)

# The date windows tile [today, today + WINDOW_DAYS]. A tail slice past the cap
# picks up everything beyond it (queue #464), so this is a paging convenience,
# not a coverage boundary.
WINDOW_DAYS = 365

# Some dockets never close: agencies park a sentinel end date far in the future
# on perpetual guidance / exemption dockets. Measured 2026-08-09/10 over the 50
# documents ending beyond the 365-day cap: 2027:2 · 2032:2 · 2050:2 · 2099:14 ·
# 2100:30 — i.e. 44 of 50 sit in 2099/2100, and the observed distribution has a
# clean gap around 2050. Flagging them rather than silently folding them into the
# corpus: capturing them unmarked would trade a silent 4.9% UNDERCOUNT for a
# silent 4.9% CATEGORY CONTAMINATION, because every downstream "open for comment"
# count would then include dockets that never close and were never actionable.
# The fetcher does NOT decide whether they belong — it labels them and lets
# consumers include or exclude.
PERPETUAL_DOCKET_YEAR = 2050

# Anticipate the path switch instead of discovering it. Since PAGE_SIZE=250 the
# job takes SINGLE-PASS and the windowed branch is dormant — dormant code that
# auto-activates on corpus growth is a latent failure, so warn while there is
# still room to act rather than at the moment it flips.
#
# Expressed as a FRACTION of the threshold rather than a bare 8000 so it cannot
# silently exceed the threshold if PAGE_SIZE is ever lowered. At the current
# threshold (MAX_PAGES*PAGE_SIZE = 10,000) this is exactly 8,000.
#
# Sizing, measured over 50 runs 2026-06-22..2026-08-10: total open documents
# min 1,015 / mean 1,146 / max 1,274, linear trend -4.68/day (DECLINING), largest
# single-day jump 79. The threshold is 7.8x the observed peak, so growth alone
# will not reach it — a structural surge (end-of-administration rulemaking, a
# post-shutdown backlog) is the only realistic route, which is precisely the case
# that arrives fast and unannounced.
PATH_SWITCH_WARN_FRACTION = 0.8

# The unreachable-population exclusion is TOLERATED, not ignored: it must stay
# bounded or it becomes a blind spot. Fire when it exceeds
# max(UNREACHABLE_FLOOR, UNREACHABLE_MEDIAN_MULT x trailing-median).
# Trailing window is the last 7 RECORDED RUNS rather than 7 calendar days, so a
# missed morning shifts the baseline instead of silently emptying it.
# FLOOR TIGHTENED 75 -> 5 on 2026-08-10, landing WITH the tail slice below.
# Rationale: once the tail slice closes the window gap, out-of-window goes to ~0
# and the trailing median goes to 0, which would pin the ceiling at the floor.
# A floor of 75 would then let the tail slice FAIL SILENTLY and swallow up to 75
# documents. Post-tail-slice, any nonzero out-of-window count means the tail
# slice broke, so the floor must be tight enough to say so.
UNREACHABLE_FLOOR = 5
UNREACHABLE_MEDIAN_MULT = 1.5
UNREACHABLE_TRAILING_RUNS = 7
UNREACHABLE_HISTORY_KEEP = 30
STATE_NAME = "open_comments_unreachable_history.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── API helpers ────────────────────────────────────────────────────────────

def load_api_key():
    """Load API key from config.json, or use env var."""
    key = os.environ.get("REGS_API_KEY")
    if key:
        return key
    if CONFIG_PATH.exists():
        cfg = json.loads(CONFIG_PATH.read_text())
        return cfg["regulations_gov_api_key"]
    raise RuntimeError("No API key found. Set REGS_API_KEY or provide config.json")


def api_get(session, endpoint, params, api_key):
    """Make an API request with retry logic. RAISES on exhaustion — never returns None.

    queue #462 follow-up: the previous version slept a flat 60s on a 429 and
    `continue`d, which CONSUMED a retry attempt. Three consecutive 429s therefore
    fell out of the loop and `return None`, and the caller turned that into a
    silent `break` — a short page indistinguishable from the tie-ordering defect
    this file was just fixed for: fewer unique documents, a clean-looking count,
    non-deterministic. At ~340 requests every morning against a 1,000/hr key that
    is a live exposure, not a hypothetical.

    Now: 429s are retried on the server's own Retry-After (which regs.gov sets
    well above 60s — CLAUDE.md records values over 1,600s), and exhausting the
    attempt budget raises. Any non-200 is either explicitly retried or fatal.
    """
    import requests
    url = f"{API_BASE}/{endpoint}"
    params = {**params, "api_key": api_key}   # copy: don't leak the key into the caller's dict
    last_err = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, timeout=60)
            if resp.status_code == 429:
                wait = RATE_LIMIT_DEFAULT_WAIT
                hdr = resp.headers.get("Retry-After")
                if hdr:
                    try:
                        wait = min(int(float(hdr)), RATE_LIMIT_MAX_WAIT)
                    except (TypeError, ValueError):
                        pass
                last_err = f"HTTP 429 rate-limited (Retry-After={hdr!r})"
                if attempt < MAX_ATTEMPTS:
                    log.warning(
                        f"Rate limited on {endpoint} (attempt {attempt}/"
                        f"{MAX_ATTEMPTS}); waiting {wait}s per Retry-After..."
                    )
                    time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            last_err = f"{type(e).__name__}: {e}"
            if attempt < MAX_ATTEMPTS:
                log.warning(
                    f"Request to {endpoint} failed ({e}); retrying in 10s "
                    f"(attempt {attempt}/{MAX_ATTEMPTS})..."
                )
                time.sleep(10)

    raise RuntimeError(
        f"API request to /{endpoint} failed after {MAX_ATTEMPTS} attempts "
        f"({last_err}). REFUSING to continue: a dropped page yields a silently "
        f"short snapshot that looks exactly like the queue-#462 pagination "
        f"defect. Failing closed leaves the live DB intact."
    )


# ── Unreachable-gap history (queue #462 follow-up) ─────────────────────────

def load_unreachable_history(state_path):
    """Read the trailing record of how many documents were out of window."""
    if not state_path or not state_path.exists():
        return []
    try:
        blob = json.loads(state_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"Unreachable-gap history unreadable ({e}); treating as empty. "
                    f"The ceiling falls back to the absolute floor this run.")
        return []
    runs = blob.get("runs") if isinstance(blob, dict) else None
    return runs if isinstance(runs, list) else []


def save_unreachable_history(state_path, runs):
    """Persist the trailing record. Never fatal — this is telemetry, not data."""
    if not state_path:
        return
    try:
        tmp = state_path.with_name(state_path.name + ".tmp")
        tmp.write_text(json.dumps({"runs": runs[-UNREACHABLE_HISTORY_KEEP:]}, indent=1))
        tmp.replace(state_path)
    except OSError as e:
        log.warning(f"Could not write unreachable-gap history ({e}).")


def unreachable_ceiling(history):
    """max(floor, mult x trailing median). Returns (ceiling, median_or_None).

    Deliberately NOT a fixed carve-out for today's 50: an alarm taught to ignore
    a specific number is where the next false negative lives. This tolerates the
    exclusion at its current size and fires if it grows.
    """
    vals = sorted(
        r["unreachable"] for r in history[-UNREACHABLE_TRAILING_RUNS:]
        if isinstance(r.get("unreachable"), (int, float))
    )
    if not vals:
        return float(UNREACHABLE_FLOOR), None
    n = len(vals)
    median = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
    return max(float(UNREACHABLE_FLOOR), UNREACHABLE_MEDIAN_MULT * median), median


def fetch_open_documents(api_key, state_path=None, record_state=True):
    """Fetch all documents currently open for comment.

    The API limits pagination to MAX_PAGES pages (MAX_PAGES * PAGE_SIZE
    results). When there are more, we split the date range into smaller
    windows and paginate each. Every paginated request carries SORT_KEY —
    see the note at the top of this file; without it the API silently skips
    and repeats records across pages.
    """
    import requests
    session = requests.Session()
    session.headers["User-Agent"] = "DataDawn-OpenRegs/1.0"

    today = datetime.now(tz=None).strftime("%Y-%m-%d")

    def fetch_pages(params_base, label=""):
        """Paginate up to MAX_PAGES pages for a given set of filters.

        Always sorts by SORT_KEY — unsorted pagination is lossy (queue #462).
        """
        docs = []
        page = 1
        total_pages = 1
        while page <= MAX_PAGES:
            params = {
                **params_base,
                "sort": SORT_KEY,
                "page[size]": PAGE_SIZE,
                "page[number]": page,
            }
            # api_get raises rather than returning None (queue #462 follow-up),
            # so the only remaining silent-short-page shape is a 200 whose body
            # has no `data` key. Treat that as fatal too, not as an empty page.
            data = api_get(session, "documents", params, api_key)
            if "data" not in data:
                raise RuntimeError(
                    f"Malformed API response for "
                    f"{label or 'slice'} page {page}: HTTP 200 with no 'data' "
                    f"key (keys: {sorted(data)[:8]}). Refusing to treat this as "
                    f"an empty page — that is how a short snapshot looks clean."
                )

            batch = data["data"]
            docs.extend(batch)
            total_pages = data.get("meta", {}).get("totalPages", 1)

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.4)

        # No silent caps: if a slice needs more pages than the API will serve,
        # say so. There is no auto-subdivision here (a previous comment claimed
        # there was — there never has been), so this is a real truncation and
        # the only thing standing between it and a silently short snapshot.
        if total_pages > MAX_PAGES:
            log.warning(
                f"TRUNCATED{' ' + label if label else ''}: slice needs "
                f"{total_pages} pages but the API serves at most {MAX_PAGES} "
                f"({MAX_PAGES * PAGE_SIZE} records). Fetched {len(docs)}; the "
                f"remainder is UNRETRIEVABLE with these filters — narrow the "
                f"date window or raise PAGE_SIZE (max 250)."
            )

        return docs

    # Probe: use just [ge] (more reliable than [ge]+[le] combo)
    all_docs = []
    probe_params = {
        "filter[commentEndDate][ge]": today,
        "page[size]": 5,
        "page[number]": 1,
    }
    probe = api_get(session, "documents", probe_params, api_key)   # raises on failure
    total = probe.get("meta", {}).get("totalElements", 0)
    log.info(f"Total open documents: {total}")

    # ABSOLUTE plausibility floor (incident 2026-06-07, queue #113): on
    # 2026-05-28 the upstream API stopped populating commentEndDate on most
    # documents and this total collapsed 1,250 -> 15 -> ... -> 0 while the
    # job stayed green for 10 days (a 0-total takes the "nothing to update"
    # early-return and exits 0). A healthy open-for-comment universe is
    # ~1,250 documents; below ABS_FLOOR the result is treated as an
    # upstream/API failure, not a quiet day. Exit 2 -> the cron's hc-ping
    # /$? reports failure -> check red -> notification. The relative >50%
    # swap guard further down cannot see this path (it only runs when a
    # write is attempted, and it ratchets if live itself already shrank);
    # this floor is absolute on purpose. Flag-and-refuse, never auto-correct.
    ABS_FLOOR = 200
    if total < ABS_FLOOR:
        log.critical(
            f"Open-document total {total} is below the plausibility floor "
            f"({ABS_FLOOR}; healthy norm ~1,250). Treating as upstream/API "
            f"failure — refusing to proceed, leaving the live DB intact."
        )
        raise SystemExit(2)

    # `expected` is the population this run can actually reach, which is NOT
    # always `total` — see the windowed branch below. `unreachable` is the
    # difference, and it is reported every run whether or not it alarms.
    expected = total
    unreachable = 0

    # Early warning on the dormant-path transition (see PATH_SWITCH_WARN_FRACTION).
    # The choice below is PROACTIVE — it reads the API's own totalElements and
    # selects a path before fetching anything — so the crossover cannot silently
    # truncate a run. This warning exists so the crossover is anticipated, not
    # merely survived: the windowed branch has been dormant since 2026-08-10 and
    # its guards are proved but unexercised in production.
    switch_warn_at = PATH_SWITCH_WARN_FRACTION * MAX_PAGES * PAGE_SIZE
    if total >= switch_warn_at:
        log.warning(
            f"PATH-SWITCH APPROACHING: {total} open documents is at or above "
            f"{switch_warn_at:.0f} ({PATH_SWITCH_WARN_FRACTION:.0%} of the "
            f"{MAX_PAGES * PAGE_SIZE:,}-record single-pass ceiling). Above that "
            f"ceiling this job switches to the WINDOWED path plus tail slice — "
            f"code that has been dormant since 2026-08-10. Exercise it before it "
            f"activates on its own."
        )

    # DIFFERENTIAL-TEST HATCH (queue #462 wrap). Since PAGE_SIZE=250 the windowed
    # branch + tail slice are DORMANT — proved only against stubs, and otherwise
    # first exercised for real during a corpus surge, i.e. the worst moment. This
    # env override forces the windowed path so both implementations can be run
    # against the SAME live source on the same morning and diffed. It deliberately
    # does NOT lower the real threshold: editing live path-selection logic to test
    # the dormant branch would change the thing under test.
    force_windowed = os.environ.get("OPENREGS_FORCE_WINDOWED") == "1"
    if force_windowed:
        log.warning("FORCED WINDOWED PATH (OPENREGS_FORCE_WINDOWED=1) — differential "
                    "test only; the real threshold is unchanged.")

    if not force_windowed and total <= MAX_PAGES * PAGE_SIZE:
        # Single pass: [ge] with no upper bound, so nothing is out of range.
        all_docs = fetch_pages({"filter[commentEndDate][ge]": today},
                               label="single-pass")
        log.info(f"Fetched {len(all_docs)}/{total} in single pass")
    else:
        # Split into fixed date windows and paginate each. NOTE: there is no
        # adaptive subdivision — a window that exceeds the per-slice ceiling is
        # truncated with a warning from fetch_pages(), not split further.
        cap = (datetime.now(tz=None) + timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
        start = datetime.strptime(today, "%Y-%m-%d")
        end = datetime.strptime(cap, "%Y-%m-%d")

        # The windows tile [today, today+WINDOW_DAYS] only, so documents whose
        # comment period ends beyond the cap are counted in `total` but are
        # structurally UNREACHABLE on this path. Measure that population rather
        # than letting it masquerade as pagination loss. Measured 2026-08-09/10:
        # 50 of 1,020 (4.9%) — and they are NOT a long tail of ordinary dates,
        # they are perpetually-open shell dockets carrying sentinel end dates
        # (44 of 50 in 2099/2100; FAA/FMCSA exemption + guidance dockets). See
        # queue #462 follow-up item 7: widening the window does not reach them,
        # an unbounded tail slice does.
        capped_probe = api_get(session, "documents", {
            "filter[commentEndDate][ge]": today,
            "filter[commentEndDate][le]": cap,
            "page[size]": 5, "page[number]": 1,
        }, api_key)
        in_range = capped_probe.get("meta", {}).get("totalElements", 0)
        if in_range:
            expected = in_range
            unreachable = max(0, total - in_range)
        else:
            # Probe returned zero in-range against a total that cleared ABS_FLOOR.
            # Don't silently compare against the wrong denominator.
            log.warning(
                f"In-range probe returned 0 against a total of {total}; cannot "
                f"establish the reachable population this run. Completeness "
                f"reporting below is against the unbounded total instead."
            )

        # Build 2-week windows (empirically well under the per-slice ceiling of
        # MAX_PAGES * PAGE_SIZE; observed max ~310 documents per window)
        windows = []
        ws = start
        while ws < end:
            we = min(ws + timedelta(days=14), end)
            windows.append((ws, we))
            ws = we + timedelta(days=1)

        for win_start, win_end in windows:
            ws = win_start.strftime("%Y-%m-%d")
            we = win_end.strftime("%Y-%m-%d")
            batch = fetch_pages({
                "filter[commentEndDate][ge]": ws,
                "filter[commentEndDate][le]": we,
            }, label=f"window {ws}..{we}")
            log.info(f"Window {ws} to {we}: {len(batch)} documents")
            all_docs.extend(batch)

        # ── TAIL SLICE (queue #464) ────────────────────────────────────────
        # The windows stop at `cap`, but comment periods do not. Measured
        # 2026-08-09/10: 50 of 1,020 open documents ended beyond the cap and were
        # therefore never fetched on ANY day — 4.9%, deterministic, and larger
        # than the pagination defect that prompted this whole review.
        #
        # Widening the window is NOT the fix: 44 of the 50 carry sentinel end
        # dates in 2099/2100 (perpetually-open FAA/FMCSA shell and exemption
        # dockets), so 365 -> 730 would capture 2 of 50 and full coverage that
        # way needs a ~74-year window. One unbounded slice reaches all of them.
        #
        # It goes through fetch_pages like any other slice, so it is sorted,
        # MAX_PAGES-bounded, and covered by the TRUNCATED warning — an unbounded
        # slice is exactly where silent truncation would hide.
        out_of_window = max(0, total - in_range) if in_range else 0
        tail = fetch_pages({"filter[commentEndDate][ge]": cap},
                           label=f"tail (>= {cap}, unbounded)")
        tail_unique = {d.get("id") for d in tail}
        log.info(f"Tail slice (comment periods ending after {cap}): "
                 f"{len(tail)} rows, {len(tail_unique)} unique, against an "
                 f"out-of-window population of {out_of_window}")
        all_docs.extend(tail)

        # Everything is now reachable, so completeness is measured against the
        # FULL total rather than the in-window subset.
        expected = total
        # ...but do NOT simply assert the gap is closed. `unreachable` is what the
        # tail slice FAILED to retrieve. If the tail breaks, this goes back to the
        # out-of-window population and UNREACHABLE-GAP fires against the tightened
        # floor. Hardcoding 0 here would make that guard vacuous — which is the
        # exact silent-failure mode the tightened floor exists to catch.
        unreachable = max(0, out_of_window - len(tail_unique))

    # Deduplicate by document ID (windows may overlap at boundaries)
    seen = set()
    unique = []
    for doc in all_docs:
        did = doc.get("id")
        if did not in seen:
            seen.add(did)
            unique.append(doc)

    # Completeness sensor for the queue-#462 defect class. Before the sort fix
    # this run silently returned fewer unique ids than the population and
    # nothing said so — three identical unsorted runs of one window gave
    # 296 / 303 / 296 out of 305. Compare against the REACHABLE population
    # (`expected`, which excludes the beyond-cap documents accounted for
    # above) so this reports pagination loss and not by-design scope.
    fetched, dropped = len(all_docs), len(all_docs) - len(unique)
    log.info(
        f"Total unique documents: {len(unique)} "
        f"(fetched {fetched} rows, {dropped} duplicate row(s) collapsed; "
        f"reachable population {expected})"
    )
    if expected and len(unique) < expected:
        log.warning(
            f"INCOMPLETE: {len(unique)} unique documents vs a reachable "
            f"population of {expected} — short by {expected - len(unique)}. "
            f"Pagination is dropping records. This is the queue-#462 defect "
            f"class; check that SORT_KEY={SORT_KEY!r} is still being sent and "
            f"that no slice logged TRUNCATED above."
        )

    # ── Bound the exclusion the INCOMPLETE guard is granted ────────────────
    # `expected` deliberately excludes the out-of-window documents, otherwise
    # this guard would false-alarm every morning. That exclusion is TOLERATED,
    # not free: an alarm taught to ignore a specific number is where the next
    # false negative lives. So the raw gap is logged every run (drift is visible
    # in the log before it is visible in an alarm), and it alarms if it grows.
    history = load_unreachable_history(state_path)
    ceiling, median = unreachable_ceiling(history)
    pct = (100.0 * unreachable / total) if total else 0.0
    log.info(
        f"Unreachable-gap sensor [windowed path only; reads 0 on single-pass, "
        f"where no upper bound is applied]: {unreachable} of {total} "
        f"({pct:.1f}%); ceiling {ceiling:.0f} "
        f"(floor {UNREACHABLE_FLOOR}, {UNREACHABLE_MEDIAN_MULT}x trailing-"
        f"{UNREACHABLE_TRAILING_RUNS}-run median "
        f"{'n/a' if median is None else format(median, '.1f')})"
    )
    if unreachable > ceiling:
        log.warning(
            f"UNREACHABLE-GAP: {unreachable} document(s) ({pct:.1f}%) end beyond "
            f"the {WINDOW_DAYS}-day window and were NOT retrieved by the tail "
            f"slice, above the ceiling of {ceiling:.0f}. Since queue #464 the "
            f"tail slice should reduce this to zero, so a nonzero value means "
            f"THE TAIL SLICE IS BROKEN — not that the corpus changed shape. "
            f"Check for a TRUNCATED warning on the tail slice above."
        )

    if record_state:
        history.append({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "total": total,
            "reachable": expected,
            "unreachable": unreachable,
            "unique": len(unique),
        })
        save_unreachable_history(state_path, history)

    return unique


def parse_document(doc):
    """Convert API document to database row dict."""
    attrs = doc.get("attributes", {})
    doc_id = doc.get("id", "")

    # Parse dates — API returns ISO format with timezone
    def parse_date(val):
        if not val:
            return None
        return val[:10]  # Just the date part (YYYY-MM-DD)

    def parse_datetime(val):
        if not val:
            return None
        return val[:19].replace("T", " ")  # YYYY-MM-DD HH:MM:SS

    posted = parse_date(attrs.get("postedDate"))
    posted_year = int(posted[:4]) if posted else None
    posted_month = int(posted[5:7]) if posted else None

    # Perpetual-docket label (queue #464) — see PERPETUAL_DOCKET_YEAR. NULL, not
    # 0, when there is no end date at all: "we don't know" is not "closes soon".
    end_date = parse_date(attrs.get("commentEndDate"))
    perpetual = None
    if end_date:
        try:
            perpetual = 1 if int(end_date[:4]) >= PERPETUAL_DOCKET_YEAR else 0
        except ValueError:
            perpetual = None

    return {
        "id": doc_id,
        "agency_id": attrs.get("agencyId"),
        "docket_id": attrs.get("docketId"),
        "title": attrs.get("title"),
        "document_type": attrs.get("documentType"),
        "subtype": attrs.get("subtype"),
        "posted_date": posted,
        "posted_year": posted_year,
        "posted_month": posted_month,
        "comment_start_date": parse_date(attrs.get("commentStartDate")),
        "comment_end_date": end_date,
        "perpetual_docket": perpetual,
        "last_modified": parse_datetime(attrs.get("lastModifiedDate")),
        "fr_doc_num": attrs.get("frDocNum"),
        "open_for_comment": 1 if attrs.get("openForComment") else 0,
        "withdrawn": 1 if attrs.get("withdrawn") else 0,
        "object_id": attrs.get("objectId"),
    }


# ── Database ───────────────────────────────────────────────────────────────

def build_database(db_path, rows):
    """Build a fresh open_comments.db with documents and stub dockets.

    Uses atomic write: builds into a .tmp file, then renames into place.
    This prevents Datasette from reading a partially-written file.
    """
    tmp_path = db_path.parent / (db_path.name + ".tmp")

    # Remove any stale temp file
    if tmp_path.exists():
        tmp_path.unlink()

    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA journal_mode=DELETE")  # No WAL — simpler for Datasette
    conn.execute("PRAGMA synchronous=FULL")

    # Create tables with same schema as openregs.db (subset of columns)
    conn.execute("""
        CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            agency_id TEXT,
            docket_id TEXT,
            title TEXT,
            document_type TEXT,
            subtype TEXT,
            posted_date TEXT,
            posted_year INTEGER,
            posted_month INTEGER,
            comment_start_date TEXT,
            comment_end_date TEXT,
            perpetual_docket INTEGER,   -- 1 = sentinel end date (queue #464); NULL = no end date
            last_modified TEXT,
            fr_doc_num TEXT,
            open_for_comment INTEGER,
            withdrawn INTEGER,
            object_id TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE dockets (
            id TEXT PRIMARY KEY,
            agency_id TEXT
        )
    """)

    # Insert documents
    sql = """
        INSERT INTO documents
        (id, agency_id, docket_id, title, document_type, subtype,
         posted_date, posted_year, posted_month,
         comment_start_date, comment_end_date, perpetual_docket, last_modified,
         fr_doc_num, open_for_comment, withdrawn, object_id)
        VALUES
        (:id, :agency_id, :docket_id, :title, :document_type, :subtype,
         :posted_date, :posted_year, :posted_month,
         :comment_start_date, :comment_end_date, :perpetual_docket, :last_modified,
         :fr_doc_num, :open_for_comment, :withdrawn, :object_id)
    """
    conn.executemany(sql, rows)

    # Create stub dockets
    docket_ids = set(r["docket_id"] for r in rows if r["docket_id"])
    conn.executemany(
        "INSERT OR IGNORE INTO dockets (id, agency_id) VALUES (?, ?)",
        [(did, did.split("-")[0] if "-" in did else None) for did in docket_ids]
    )

    # Indexes for the explore page queries
    conn.execute("CREATE INDEX idx_docs_agency ON documents(agency_id)")
    conn.execute("CREATE INDEX idx_docs_comment_end ON documents(comment_end_date)")
    conn.execute("CREATE INDEX idx_docs_withdrawn ON documents(withdrawn)")
    conn.execute("CREATE INDEX idx_docs_perpetual ON documents(perpetual_docket)")

    # ── THE FILTERED POPULATION, DEFINED ONCE (queue #476, ruling 2026-08-11) ──
    # Five consumer query-sites across three files each carried their own copy of the
    # open-comment predicate, and all five omitted `perpetual_docket` — the signature of
    # a filter living at the wrong layer. The measured cost: FMCSA's agency hub listed
    # 16 open documents of which 8 never close, and a docket hub rendered "Open for
    # comment. 26805 days left". Patch one consumer and the sixth reproduces it.
    #
    # ALL THREE predicates are load-bearing, each for a DIFFERENT reason. An earlier draft
    # of this view dropped the date predicate on the grounds that it "filters nothing" —
    # true on any day the cron is healthy, and the same point-in-time observation that
    # does not justify dropping `withdrawn = 0` one line over. review caught the asymmetry;
    # the reasoning below is why it stays.
    #
    #   comment_end_date >= date('now')
    #     THE CLIENT-SIDE STALENESS BACKSTOP, load-bearing ONLY in the failure state —
    #     which is exactly why measuring a healthy day says nothing about it. Walk it:
    #     the 06:30 cron fails for three days. WITH this predicate, closed dockets age
    #     out of every panel as their dates pass and the site quietly shrinks toward
    #     empty — visibly degraded, never wrong. WITHOUT it, the site serves the day-one
    #     population as currently-open indefinitely: banner, agency tiles, counts, all
    #     confidently stale. That is fail-OPEN on the freshness axis, introduced by a
    #     cleanup, and invisible to every measurement taken while the cron works.
    #     GRAIN MIRRORS THE BUILD, deliberately: the build fetches
    #     `filter[commentEndDate][ge] = today` with `today = strftime("%Y-%m-%d")`, i.e.
    #     >= at DAY grain, so the view decays in sync with what the build would have
    #     done. It is NOT `> datetime('now')` — the old consumer form, wrong twice over
    #     and measured 2026-08-11: `comment_end_date` is stored DATE-ONLY (986/986 rows,
    #     length exactly 10), so comparing against a datetime sorts a docket closing
    #     today BELOW 'now' from 00:00:01 and hides it for the whole (stored) final day —
    #     which includes the real period's last ~4 live hours, see UTC BOUNDARY below;
    #     and `>` rather than `>=` excludes the closing-today row that the build
    #     deliberately fetched.
    #     UTC BOUNDARY — CORRECTED 2026-08-14 (measured against the raw API; the
    #     2026-08-11 note here had the direction BACKWARDS): regulations.gov expresses
    #     the 11:59:59 PM ET close as a UTC datetime (e.g. 2026-08-15T03:59:59Z for an
    #     Aug-14 ET deadline) and parse_date stores its first 10 chars, so
    #     comment_end_date is the UTC-ROLLED date — the day AFTER the ET calendar
    #     deadline. Consequence: `>= date('now')` NEVER drops a live period early; it
    #     holds the row through the real 03:59/04:59Z close and errs ~20h late-open
    #     only. (It also means the stored/displayed date is one day after the official
    #     ET deadline — a display-contract question tracked in the queue, not a
    #     predicate defect.) The build has the same day-grain property (VPS is
    #     Etc/UTC), mirrored convention — the view decays in sync with the fetch.
    #     Changing it means changing both.
    #
    #   withdrawn = 0
    #     Not a re-derivation: the build STORES the flag (see the row mapping above) but
    #     never excludes on it, so this view is the only place it is applied. Today's
    #     snapshot holds 0 withdrawn rows — an observation, not a guarantee.
    #
    #   perpetual_docket IS NOT 1
    #     The #476 fix itself: ~46 sentinel-dated shells (2050+) that never close.
    #
    # Raw `documents` stays available and unfiltered ON PURPOSE — research access to the
    # true population, including the perpetual shells, and it ships that way in the
    # weekly R2 dump (50_generate_dumps.py, which publishes open_comments.db whole; the
    # view rides along in the schema for free). That is a feature; see the
    # `resolveDocketFromOpen` carve-out in datadawn-website/regs-shared.js, which needs
    # existence rather than open-status and must NOT use this view.
    #
    # SECOND IMPLEMENTATION EXISTS — CHANGE ONE, CHANGE BOTH.
    # `datadawn-website/regs-shared.js` -> `ocSrc()` carries an inline equivalent of the
    # WHERE clause below, as a deploy-order fallback for snapshots predating the view.
    # It is a deliberate second implementation — divergence-with-delay by design,
    # accepted as a net. Any edit to these predicates MUST be made there in the same
    # change, and the pairing comment there points back here.
    conn.execute(
        "CREATE VIEW open_comments_active AS "
        "SELECT * FROM documents "
        "WHERE comment_end_date >= date('now') "
        "AND withdrawn = 0 "
        "AND perpetual_docket IS NOT 1"
    )

    # Metadata table for tracking freshness
    conn.execute("CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO _metadata VALUES ('updated_at', ?)",
                 (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
    conn.execute("INSERT INTO _metadata VALUES ('document_count', ?)",
                 (str(len(rows)),))

    conn.commit()
    conn.close()

    # Sanity check: refuse to swap in a dataset that dropped >50% vs live,
    # which almost always means a transient API blip (partial-page returns).
    # A 0-row or near-empty open_comments.db would break the explore page.
    if db_path.exists():
        try:
            old_conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            old_count = old_conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            old_conn.close()
            if old_count > 0 and len(rows) < max(10, old_count * 0.5):
                tmp_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Refusing DB swap: new count {len(rows)} dropped >50% vs live "
                    f"{old_count}. Likely transient API issue — leaving live DB intact."
                )
        except sqlite3.DatabaseError:
            pass  # live DB unreadable, fall through and swap

    # Atomic rename into place
    if db_path.exists():
        db_path.unlink()
    tmp_path.rename(db_path)

    return len(rows), len(docket_ids)


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Daily open-for-comment updater (builds standalone open_comments.db)")
    parser.add_argument("--db", type=Path, help="Output database path (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    # Auto-detect output path
    if args.db:
        db_path = args.db
    elif VPS_DIR.exists():
        db_path = VPS_DIR / DB_NAME
    elif LOCAL_DIR.exists():
        db_path = LOCAL_DIR / DB_NAME
    else:
        log.error("No suitable output path found. Specify --db path.")
        sys.exit(1)

    log.info(f"Output: {db_path}")
    log.info(f"Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")

    # Load API key
    api_key = load_api_key()
    log.info("API key loaded")

    # Fetch open documents. The unreachable-gap history lives beside the DB so
    # it follows --db; a dry run reads the trailing baseline but must not write
    # to it, or previewing would corrupt the series it is previewing against.
    state_path = db_path.parent / STATE_NAME
    log.info("Fetching open-for-comment documents from Regulations.gov...")
    docs = fetch_open_documents(api_key, state_path=state_path,
                                record_state=not args.dry_run)
    log.info(f"Fetched {len(docs)} documents")

    if not docs:
        log.info("No open documents found. Nothing to update.")
        return

    # Parse
    rows = [parse_document(d) for d in docs]

    # Agency breakdown
    agencies = {}
    for r in rows:
        a = r["agency_id"] or "Unknown"
        agencies[a] = agencies.get(a, 0) + 1
    log.info(f"Agencies: {', '.join(f'{a}={n}' for a, n in sorted(agencies.items(), key=lambda x: -x[1])[:10])}")

    if args.dry_run:
        log.info(f"[DRY-RUN] Would build {db_path} with {len(rows)} documents")
        return

    # Build database
    doc_count, docket_count = build_database(db_path, rows)
    db_size = db_path.stat().st_size
    log.info(f"Done: {doc_count} documents, {docket_count} dockets, {db_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
