#!/usr/bin/env python3
"""
Phase 03: Download Regulations.gov comment headers for all tracked agencies.

Replaces the former three-script approach (03 + 04 + 06) with a single script
that handles all agencies and automatically subdivides by year -> month -> day
when any time window exceeds the 5,000 result API cap.

Output: regulations_gov/comments/headers/{AGENCY}/{YEAR}/{MM}/page_NNNN.json
        regulations_gov/comments/headers/{AGENCY}/{YEAR}/{MM}/d{DD}/page_NNNN.json

Usage:
    python3 03_regs_gov_comments.py                         # all agencies, all years
    python3 03_regs_gov_comments.py --agencies EPA,FDA      # specific agencies
    python3 03_regs_gov_comments.py --start-year 2020       # from 2020 onward
    python3 03_regs_gov_comments.py --api-key-2             # use secondary API key
    python3 03_regs_gov_comments.py --agency-start DOI:2018 # resume from DOI year 2018
"""

import argparse
import json
import sys
import time
import logging
import signal
from calendar import monthrange
from datetime import date
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Paths ===
PROJECT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_DIR / "scripts"
DATA_DIR = PROJECT_DIR / "regulations_gov" / "comments" / "headers"
LOG_DIR = PROJECT_DIR / "logs"
CONFIG_FILE = SCRIPTS_DIR / "config.json"
STATE_FILE = LOG_DIR / "regs_comments_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

# === Constants ===
API_BASE = "https://api.regulations.gov/v4"
PAGE_SIZE = 250
MAX_PAGE = 20           # API hard limit: 20 pages * 250 = 5,000 results
MAX_RESULTS = MAX_PAGE * PAGE_SIZE  # 5,000
MIN_INTERVAL = 3.6      # seconds between requests (1,000 req/hr)
DEFAULT_START_YEAR = 2005

DEFAULT_AGENCIES = [
    "USDA", "EPA", "FDA", "FWS", "APHIS",
    "DOT", "DOE", "HHS", "DOL", "DOI",
    "DHS", "DOJ", "ED", "HUD", "DOD",
]

# === CLI argument parsing ===
def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Regulations.gov comment headers for tracked agencies."
    )
    parser.add_argument(
        "--agencies",
        help="Comma-separated agency list (default: all tracked agencies)",
    )
    parser.add_argument(
        "--api-key-2",
        action="store_true",
        help="Use the secondary API key (regulations_gov_api_key_2)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"First year to download (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--agency-start",
        help="Resume from AGENCY:YEAR (e.g. DOI:2018). Skips earlier agencies and years.",
    )
    return parser.parse_args()


args = parse_args()

AGENCIES = args.agencies.split(",") if args.agencies else DEFAULT_AGENCIES
START_YEAR = args.start_year

# Parse --agency-start into (agency, year) tuple or None
RESUME_AGENCY = None
RESUME_YEAR = None
if args.agency_start:
    parts = args.agency_start.split(":")
    if len(parts) == 2:
        RESUME_AGENCY = parts[0]
        RESUME_YEAR = int(parts[1])
    else:
        print(f"ERROR: --agency-start must be AGENCY:YEAR (got '{args.agency_start}')")
        sys.exit(1)

# === Load API key ===
with open(CONFIG_FILE) as f:
    _cfg = json.load(f)
    API_KEY = (
        _cfg["regulations_gov_api_key_2"]
        if args.api_key_2
        else _cfg["regulations_gov_api_key"]
    )

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("regs_comments")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "regs_gov_comments.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# === Graceful shutdown ===
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received -- saving state and exiting")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# === Rate limiter ===
class RateLimiter:
    """Enforce a minimum interval between API calls."""

    def __init__(self, interval):
        self.interval = interval
        self.last = 0
        self.count = 0

    def wait(self):
        elapsed = time.time() - self.last
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last = time.time()
        self.count += 1


rate = RateLimiter(MIN_INTERVAL)


# === HTTP Session ===
def create_session():
    """Create a requests session with retry logic and API key header."""
    s = requests.Session()
    s.headers["X-Api-Key"] = API_KEY
    retry = Retry(
        total=3,
        backoff_factor=5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def api_get(session, params):
    """Rate-limited GET to the /comments endpoint with 429/403 handling."""
    rate.wait()
    url = f"{API_BASE}/comments"
    resp = session.get(url, params=params, timeout=120)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        log.warning(f"Rate limited (429). Sleeping {retry_after}s...")
        time.sleep(retry_after)
        rate.wait()
        resp = session.get(url, params=params, timeout=120)

    if resp.status_code == 403:
        log.error(f"403 Forbidden -- check API key. Body: {resp.text[:500]}")
        raise RuntimeError("API key rejected (403)")

    resp.raise_for_status()
    return resp.json()


# === State management ===
def load_state():
    """Load or initialize the state file for resume support."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "completed": {},       # {"comments:AGENCY:YEAR": count}
        "total_comments": 0,
        "api_calls": 0,
        "started_at": None,
    }


def save_state(state):
    """Atomically write state to disk."""
    state["api_calls"] = rate.count
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(STATE_FILE)


def progress(msg):
    """Append a timestamped line to the shared progress file."""
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


# === Download functions ===

def paginate_comments(session, params, output_dir):
    """
    Download all pages for a comments query and save as JSON files.

    Returns:
        Positive int: number of comments saved (query fit within pagination).
        Negative int: -totalElements when results exceed the 5,000 cap,
                      signaling the caller to subdivide the time window.
        Zero: no results.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    p = dict(params)
    p["page[size]"] = PAGE_SIZE
    p["page[number]"] = 1

    data = api_get(session, p)
    meta = data.get("meta", {})
    total_elements = meta.get("totalElements", 0)
    total_pages = min(meta.get("totalPages", 1), MAX_PAGE)

    if total_elements == 0:
        return 0

    # Save first page
    (output_dir / "page_0001.json").write_text(json.dumps(data))
    saved = len(data.get("data", []))

    # Save remaining pages
    for pg in range(2, total_pages + 1):
        if _shutdown:
            break
        p["page[number]"] = pg
        try:
            data = api_get(session, p)
            (output_dir / f"page_{pg:04d}.json").write_text(json.dumps(data))
            saved += len(data.get("data", []))
        except Exception as e:
            log.error(f"    Page {pg} failed: {e}")
            break

    # Signal truncation if results exceed the pagination cap
    if total_elements > MAX_RESULTS:
        return -total_elements

    return saved


def download_comments_day(session, agency, year, month, day):
    """Download comments for a single day. Returns saved count (negative if still truncated)."""
    params = {
        "filter[agencyId]": agency,
        "filter[postedDate][ge]": f"{year}-{month:02d}-{day:02d}",
        "filter[postedDate][le]": f"{year}-{month:02d}-{day:02d}",
        "sort": "postedDate",
    }
    out_dir = DATA_DIR / agency / str(year) / f"{month:02d}" / f"d{day:02d}"
    return paginate_comments(session, params, out_dir)


def download_comments_month(session, agency, year, month):
    """
    Download comments for a single month.

    If the month exceeds the 5,000 result cap, automatically subdivides
    into daily queries. Logs a warning if even a single day exceeds the cap.
    """
    last_day = monthrange(year, month)[1]
    params = {
        "filter[agencyId]": agency,
        "filter[postedDate][ge]": f"{year}-{month:02d}-01",
        "filter[postedDate][le]": f"{year}-{month:02d}-{last_day:02d}",
        "sort": "postedDate",
    }
    out_dir = DATA_DIR / agency / str(year) / f"{month:02d}"
    saved = paginate_comments(session, params, out_dir)

    if saved >= 0:
        return saved

    # Month exceeds pagination cap -- subdivide by day
    total_avail = -saved
    log.warning(
        f"    [{agency}] {year}-{month:02d}: {total_avail:,} comments "
        f"exceed {MAX_RESULTS:,} limit, subdividing by day"
    )
    saved = 0
    for day in range(1, last_day + 1):
        if _shutdown:
            break
        cnt = download_comments_day(session, agency, year, month, day)
        if cnt < 0:
            # Even a single day exceeds the cap -- save what we can
            log.warning(
                f"    [{agency}] {year}-{month:02d}-{day:02d}: "
                f"{-cnt:,} comments in single day, saved max {MAX_RESULTS:,}"
            )
            saved += MAX_RESULTS
        elif cnt > 0:
            saved += cnt

    log.info(f"    [{agency}] {year}-{month:02d}: {saved:,} comments (daily subdivision)")
    return saved


def download_comments_year(session, agency, year, state):
    """
    Download all comments for an agency in a given year.

    Tries the full year first. If results exceed the pagination cap,
    subdivides by month (which further subdivides by day if needed).

    Marks the agency-year as complete in the state file on success.
    """
    key = f"comments:{agency}:{year}"
    if key in state["completed"]:
        return state["completed"][key]

    # Try the whole year in one query
    last_day = monthrange(year, 12)[1]
    params = {
        "filter[agencyId]": agency,
        "filter[postedDate][ge]": f"{year}-01-01",
        "filter[postedDate][le]": f"{year}-12-{last_day:02d}",
        "sort": "postedDate",
    }
    out_dir = DATA_DIR / agency / str(year)
    saved = paginate_comments(session, params, out_dir)

    if saved < 0:
        # Year exceeds cap -- subdivide by month
        total_avail = -saved
        log.info(
            f"  [{agency}] {year}: {total_avail:,} comments -- subdividing by month"
        )
        saved = 0
        for m in range(1, 13):
            if _shutdown:
                break
            # Skip future months
            if date(year, m, 1) > date.today():
                break
            cnt = download_comments_month(session, agency, year, m)
            saved += cnt
            if cnt > 0:
                log.info(f"    [{agency}] {year}-{m:02d}: {cnt:,} comments")

    # Record completion (only if not interrupted)
    if not _shutdown:
        state["completed"][key] = saved
        state["total_comments"] += saved
        save_state(state)

    return saved


def download_agency_comments(session, agency, state, start_from_year=None):
    """
    Download all comment headers for an agency, year by year.

    Args:
        start_from_year: If set, skip years before this (for --agency-start resume).
    """
    log.info(f"  [{agency}] Downloading comment headers...")
    current_year = date.today().year
    agency_total = 0
    effective_start = start_from_year if start_from_year else START_YEAR

    for year in range(effective_start, current_year + 1):
        if _shutdown:
            break
        try:
            count = download_comments_year(session, agency, year, state)
            if count > 0:
                log.info(f"  [{agency}] {year}: {count:,} comments")
            agency_total += count
        except Exception as e:
            log.error(f"  [{agency}] {year} comments FAILED: {e}")

        progress(
            f"Comment headers: {agency} {year} done -- "
            f"{agency_total:,} agency total, {state['total_comments']:,} grand total"
        )

    log.info(f"  [{agency}] Total comment headers: {agency_total:,}")
    return agency_total


# === Main ===
def main():
    log.info("=" * 60)
    log.info("REGULATIONS.GOV COMMENT HEADERS -- Starting")
    log.info(f"Agencies: {', '.join(AGENCIES)}")
    log.info(f"Year range: {START_YEAR}-{date.today().year}")
    log.info(f"Rate limit: {MIN_INTERVAL}s between requests ({int(3600 / MIN_INTERVAL)}/hr)")
    if RESUME_AGENCY:
        log.info(f"Resuming from: {RESUME_AGENCY}:{RESUME_YEAR}")
    log.info("Auto-subdivision: year -> month -> day when results exceed 5,000")
    log.info("=" * 60)
    progress("Comment headers: STARTING")

    state = load_state()
    if not state["started_at"]:
        state["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)

    session = create_session()

    # Determine which agencies to process (handle --agency-start skip logic)
    skip_until_agency = RESUME_AGENCY is not None
    grand_total = 0

    for agency in AGENCIES:
        if _shutdown:
            break

        # Skip agencies before the resume point
        if skip_until_agency:
            if agency != RESUME_AGENCY:
                log.info(f"  [{agency}] Skipping (before --agency-start {RESUME_AGENCY})")
                continue
            else:
                skip_until_agency = False

        log.info(f"\n{'=' * 40}")
        log.info(f"Agency: {agency}")
        log.info(f"{'=' * 40}")
        progress(f"Comment headers: starting {agency}")

        # For the resume agency, start from the specified year
        start_from_year = None
        if agency == RESUME_AGENCY and RESUME_YEAR:
            start_from_year = RESUME_YEAR

        try:
            count = download_agency_comments(session, agency, state, start_from_year)
            grand_total += count
        except Exception as e:
            log.error(f"[{agency}] Comments failed entirely: {e}")

        progress(
            f"Comment headers: {agency} done -- "
            f"{state['total_comments']:,} total comments so far"
        )

    # Summary
    status = "Interrupted" if _shutdown else "Complete"
    log.info("=" * 60)
    log.info(f"REGULATIONS.GOV COMMENT HEADERS -- {status}")
    log.info(f"Total comment headers: {state['total_comments']:,}")
    log.info(f"Completed agency-years: {len(state['completed'])}")
    log.info(f"API calls this session: {rate.count}")
    log.info("=" * 60)

    progress(
        f"Comment headers: {status.upper()} -- "
        f"{state['total_comments']:,} total, "
        f"{len(state['completed'])} agency-years, "
        f"{rate.count} API calls"
    )


if __name__ == "__main__":
    main()
