#!/usr/bin/env python3
"""
Phase 3: Regulations.gov Comment Headers (metadata only)

Downloads comment metadata for priority agencies:
USDA, EPA, FDA, FWS, APHIS.

This is the slowest phase — millions of comments across these agencies.
Rate limited to 1,000 requests/hour. Extensive progress logging for resume.
"""

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
DATA_DIR = PROJECT_DIR / "regulations_gov/comments/headers"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "regs_comments_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

AGENCIES = ["USDA", "EPA", "FDA", "FWS", "APHIS"]
PAGE_SIZE = 250
MAX_PAGE = 20
MIN_INTERVAL = 3.6  # seconds (1000 req/hr)
START_YEAR = 1994

with open(CONFIG_FILE) as f:
    API_KEY = json.load(f)["regulations_gov_api_key"]

# === Logging ===
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
    """Rate-limited GET to /comments endpoint."""
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
        log.error(f"403 Forbidden — check API key. Body: {resp.text[:500]}")
        raise RuntimeError("API key rejected (403)")

    resp.raise_for_status()
    return resp.json()

# === State ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "completed": {},
        "total_comments": 0,
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

# === Download ===
def paginate_comments(session, params, output_dir):
    """Download all pages for a comments query. Returns count (negative if truncated)."""
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

    (output_dir / "page_0001.json").write_text(json.dumps(data))
    saved = len(data.get("data", []))

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

    if total_elements > MAX_PAGE * PAGE_SIZE:
        return -total_elements  # signal: needs subdivision

    return saved


def download_comments_month(session, agency, year, month):
    """Download comments for a single month."""
    last_day = monthrange(year, month)[1]
    params = {
        "filter[agencyId]": agency,
        "filter[postedDate][ge]": f"{year}-{month:02d}-01",
        "filter[postedDate][le]": f"{year}-{month:02d}-{last_day:02d}",
        "sort": "postedDate",
    }
    out_dir = DATA_DIR / agency / str(year) / f"{month:02d}"
    saved = paginate_comments(session, params, out_dir)

    if saved < 0:
        # Even a single month exceeds the limit — download what we can
        log.warning(f"    [{agency}] {year}-{month:02d}: {-saved} comments, only first {MAX_PAGE * PAGE_SIZE} saved")
        return MAX_PAGE * PAGE_SIZE

    return saved


def download_comments_year(session, agency, year, state):
    """Download all comments for an agency in a year."""
    key = f"comments:{agency}:{year}"
    if key in state["completed"]:
        return state["completed"][key]

    # First try the whole year
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
        # Year exceeds limit — subdivide by month
        total_avail = -saved
        log.info(f"  [{agency}] {year}: {total_avail} comments — subdividing by month")
        saved = 0
        for m in range(1, 13):
            if _shutdown:
                break
            if date(year, m, 1) > date.today():
                break
            cnt = download_comments_month(session, agency, year, m)
            saved += cnt
            if cnt > 0:
                log.info(f"    [{agency}] {year}-{m:02d}: {cnt} comments")

    if not _shutdown:
        state["completed"][key] = saved
        state["total_comments"] += saved
        save_state(state)

    return saved


def download_agency_comments(session, agency, state):
    """Download all comments for an agency, year by year."""
    log.info(f"  [{agency}] Downloading comment headers...")
    current_year = date.today().year
    agency_total = 0

    for year in range(START_YEAR, current_year + 1):
        if _shutdown:
            break

        try:
            count = download_comments_year(session, agency, year, state)
            if count > 0:
                log.info(f"  [{agency}] {year}: {count} comments")
            agency_total += count
        except Exception as e:
            log.error(f"  [{agency}] {year} comments FAILED: {e}")

        # Log progress every year
        progress(f"Regs.gov Phase 3: {agency} {year} done — {agency_total} comments so far, {state['total_comments']} total")

    log.info(f"  [{agency}] Total comment headers: {agency_total}")
    return agency_total


# === Main ===
def main():
    log.info("=" * 60)
    log.info("REGULATIONS.GOV COMMENT HEADERS — Starting")
    log.info(f"Agencies: {', '.join(AGENCIES)}")
    log.info(f"Rate limit: {MIN_INTERVAL}s between requests ({int(3600/MIN_INTERVAL)}/hr)")
    log.info("This phase may take several hours. Check progress.txt for status.")
    log.info("=" * 60)
    progress("Regs.gov Phase 3: STARTING (comment headers)")

    state = load_state()
    if not state["started_at"]:
        state["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)

    session = create_session()

    for agency in AGENCIES:
        if _shutdown:
            break
        log.info(f"\n{'='*40}")
        log.info(f"Agency: {agency}")
        log.info(f"{'='*40}")
        progress(f"Regs.gov Phase 3: starting {agency}")

        try:
            download_agency_comments(session, agency, state)
        except Exception as e:
            log.error(f"[{agency}] Comments failed entirely: {e}")

        progress(f"Regs.gov Phase 3: {agency} done — {state['total_comments']} total comments so far")

    # Summary
    log.info("=" * 60)
    status = "Interrupted" if _shutdown else "Complete"
    log.info(f"REGULATIONS.GOV COMMENT HEADERS — {status}")
    log.info(f"Total comment headers: {state['total_comments']}")
    log.info(f"API calls made: {rate.count}")
    log.info(f"Completed units: {len(state['completed'])}")
    log.info("=" * 60)

    progress(f"Regs.gov Phase 3: {status.upper()} — {state['total_comments']} comment headers, {rate.count} API calls")

if __name__ == "__main__":
    main()
