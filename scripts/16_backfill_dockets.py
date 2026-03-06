#!/usr/bin/env python3
"""
Phase 16: Backfill dockets for agencies that exceeded the 5,000-record pagination cap.

Script 02 downloads dockets with simple pagination but caps at 5,000 results
(20 pages x 250/page). EPA and FDA both hit this cap. This script subdivides
by date range (year -> month -> day) to retrieve all dockets.

Raw JSON pages are saved to:
  regulations_gov/dockets_backfill/{AGENCY}/{YEAR}/page_NNNN.json
  regulations_gov/dockets_backfill/{AGENCY}/{YEAR}/{MM}/page_NNNN.json       (month subdivision)
  regulations_gov/dockets_backfill/{AGENCY}/{YEAR}/{MM}/d{DD}/page_NNNN.json (day subdivision)

State is tracked in logs/docket_backfill_state.json so the script can safely
be re-run — completed year/agency combos are skipped.

Usage:
  python3 scripts/16_backfill_dockets.py               # both EPA and FDA
  python3 scripts/16_backfill_dockets.py --agency EPA   # EPA only
  python3 scripts/16_backfill_dockets.py --agency FDA   # FDA only
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

# === Configuration ===
API_BASE = "https://api.regulations.gov/v4"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"
DATA_DIR = PROJECT_DIR / "regulations_gov/dockets_backfill"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "docket_backfill_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

DEFAULT_AGENCIES = ["EPA", "FDA"]
PAGE_SIZE = 250
MAX_PAGE = 20  # API hard limit: page[number] max is 20
MAX_RESULTS = MAX_PAGE * PAGE_SIZE  # 5,000
MIN_INTERVAL = 3.6  # seconds between requests (1,000/hr)
START_YEAR = 1994

with open(CONFIG_FILE) as f:
    API_KEY = json.load(f)["regulations_gov_api_key"]

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("docket_backfill")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "docket_backfill.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# === Graceful shutdown ===
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received — saving state and exiting")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# === Rate limiter ===
class RateLimiter:
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
    """Rate-limited GET with 429 handling."""
    rate.wait()
    url = f"{API_BASE}/dockets"
    resp = session.get(url, params=params, timeout=120)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        log.warning(f"Rate limited (429). Sleeping {retry_after}s...")
        time.sleep(retry_after)
        rate.wait()
        resp = session.get(url, params=params, timeout=120)

    if resp.status_code == 403:
        log.error(f"403 Forbidden — check API key. Response: {resp.text[:500]}")
        raise RuntimeError("API key rejected (403)")

    resp.raise_for_status()
    return resp.json()


# === State ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "completed": {},       # key -> record count (e.g. "EPA:1994" -> 123)
        "total_dockets": 0,
        "api_calls": 0,
        "started_at": None,
    }


def save_state(state):
    state["api_calls"] = rate.count
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(STATE_FILE)


def progress(msg):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


# === Download helpers ===
def paginate(session, agency, date_ge, date_le, output_dir):
    """
    Download all pages for a date-filtered docket query.
    Returns (saved_count, total_elements).

    saved_count is the number of records actually saved.
    total_elements is the total reported by the API (may exceed saved_count
    if the pagination cap is hit).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    params = {
        "filter[agencyId]": agency,
        "filter[lastModifiedDate][ge]": date_ge,
        "filter[lastModifiedDate][le]": date_le,
        "sort": "lastModifiedDate",
        "page[size]": PAGE_SIZE,
        "page[number]": 1,
    }

    data = api_get(session, params)
    meta = data.get("meta", {})
    total_elements = meta.get("totalElements", 0)
    total_pages = min(meta.get("totalPages", 1), MAX_PAGE)

    if total_elements == 0:
        return 0, 0

    (output_dir / "page_0001.json").write_text(json.dumps(data))
    saved = len(data.get("data", []))

    for pg in range(2, total_pages + 1):
        if _shutdown:
            break
        params["page[number]"] = pg
        try:
            data = api_get(session, params)
            (output_dir / f"page_{pg:04d}.json").write_text(json.dumps(data))
            saved += len(data.get("data", []))
        except Exception as e:
            log.error(f"    Page {pg} failed for {date_ge}..{date_le}: {e}")
            break

    return saved, total_elements


def download_day(session, agency, year, month, day, output_dir):
    """Download dockets for a single day."""
    date_str = f"{year}-{month:02d}-{day:02d}"
    day_dir = output_dir / f"d{day:02d}"

    saved, total = paginate(session, agency, date_str, date_str, day_dir)

    if total > MAX_RESULTS:
        log.warning(
            f"    [{agency}] {date_str}: single day exceeds cap "
            f"({total} dockets) — only {saved} saved"
        )

    return saved


def download_month(session, agency, year, month, output_dir):
    """Download dockets for a single month, subdividing by day if needed."""
    last_day = monthrange(year, month)[1]
    m_ge = f"{year}-{month:02d}-01"
    m_le = f"{year}-{month:02d}-{last_day:02d}"
    m_dir = output_dir / f"{month:02d}"

    saved, total = paginate(session, agency, m_ge, m_le, m_dir)

    if total <= MAX_RESULTS:
        return saved

    # Month exceeds cap — subdivide by day
    log.info(
        f"    [{agency}] {year}-{month:02d} has {total} dockets — "
        f"subdividing by day"
    )
    day_total = 0
    for day in range(1, last_day + 1):
        if _shutdown:
            return day_total
        if date(year, month, day) > date.today():
            break
        try:
            count = download_day(session, agency, year, month, day, m_dir)
            day_total += count
        except Exception as e:
            log.error(f"    [{agency}] {year}-{month:02d}-{day:02d} failed: {e}")

    log.info(
        f"    [{agency}] {year}-{month:02d}: {day_total} dockets via daily "
        f"subdivision (API reported {total})"
    )
    return day_total


def download_year(session, agency, year, state):
    """
    Download all dockets for an agency+year. Subdivides by month (then day)
    if the year exceeds the pagination cap.
    """
    state_key = f"{agency}:{year}"
    if state_key in state["completed"]:
        prev = state["completed"][state_key]
        log.info(f"  [{agency}] {year}: already done ({prev} dockets)")
        return prev

    last_day = monthrange(year, 12)[1]
    date_ge = f"{year}-01-01"
    date_le = f"{year}-12-{last_day:02d}"
    year_dir = DATA_DIR / agency / str(year)

    # First, try the whole year as one query
    saved, total = paginate(session, agency, date_ge, date_le, year_dir)

    if total <= MAX_RESULTS:
        # Year fits within pagination cap — done
        state["completed"][state_key] = saved
        state["total_dockets"] += saved
        save_state(state)
        if saved > 0:
            log.info(f"  [{agency}] {year}: {saved} dockets")
        return saved

    # Year exceeds cap — subdivide by month
    log.info(
        f"  [{agency}] {year} has {total} dockets — subdividing by month"
    )
    month_total = 0
    for m in range(1, 13):
        if _shutdown:
            break
        if date(year, m, 1) > date.today():
            break
        try:
            count = download_month(session, agency, year, m, year_dir)
            month_total += count
        except Exception as e:
            log.error(f"  [{agency}] {year}-{m:02d} failed: {e}")

    if not _shutdown:
        state["completed"][state_key] = month_total
        state["total_dockets"] += month_total
        save_state(state)
        log.info(
            f"  [{agency}] {year}: {month_total} dockets via subdivision "
            f"(API reported {total})"
        )

    return month_total


# === Main ===
def main():
    parser = argparse.ArgumentParser(
        description="Backfill dockets for agencies that exceeded the 5,000-record pagination cap."
    )
    parser.add_argument(
        "--agency",
        choices=["EPA", "FDA"],
        help="Only backfill this agency (default: both EPA and FDA)",
    )
    args = parser.parse_args()

    agencies = [args.agency] if args.agency else DEFAULT_AGENCIES
    current_year = date.today().year

    log.info("=" * 60)
    log.info("DOCKET BACKFILL — Recovering truncated docket lists")
    log.info(f"Agencies: {', '.join(agencies)}")
    log.info(f"Year range: {START_YEAR}–{current_year}")
    log.info(f"Rate limit: {MIN_INTERVAL}s between requests ({int(3600 / MIN_INTERVAL)}/hr)")
    log.info(f"Output: {DATA_DIR}")
    log.info("=" * 60)
    progress(f"Docket backfill: STARTING — agencies={','.join(agencies)}")

    state = load_state()
    if not state["started_at"]:
        state["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)

    session = create_session()

    for agency in agencies:
        if _shutdown:
            break
        log.info(f"\n{'=' * 40}")
        log.info(f"Agency: {agency}")
        log.info(f"{'=' * 40}")
        progress(f"Docket backfill: starting {agency}")

        agency_total = 0
        for year in range(START_YEAR, current_year + 1):
            if _shutdown:
                break
            try:
                count = download_year(session, agency, year, state)
                agency_total += count
            except Exception as e:
                log.error(f"  [{agency}] {year} FAILED: {e}")

        log.info(f"  [{agency}] Total dockets: {agency_total}")
        progress(
            f"Docket backfill: {agency} done — {agency_total} dockets"
        )

    # Summary
    log.info("=" * 60)
    status = "Interrupted" if _shutdown else "Complete"
    log.info(f"DOCKET BACKFILL — {status}")
    log.info(f"Total dockets: {state['total_dockets']}")
    log.info(f"API calls made: {rate.count}")
    log.info("=" * 60)

    progress(
        f"Docket backfill: {status.upper()} — "
        f"{state['total_dockets']} dockets, {rate.count} API calls"
    )


if __name__ == "__main__":
    main()
