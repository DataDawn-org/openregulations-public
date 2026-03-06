#!/usr/bin/env python3
"""
Phase 12: Expand Regulations.gov to Additional Agencies

Downloads docket and document metadata (NOT comments) for high-priority
agencies beyond the original five (USDA, EPA, FDA, FWS, APHIS):

  DOT, DOE, HHS, DOL, DOI, DHS, DOJ, ED, HUD, DOD

Saves to the existing directory structure:
  regulations_gov/dockets/{AGENCY}/page_NNNN.json
  regulations_gov/documents/{AGENCY}/{YEAR}/page_NNNN.json
  regulations_gov/documents/{AGENCY}/{YEAR}/{MM}/page_NNNN.json  (month subdivision)

Rate limited to 1,000 requests/hour. Supports resume via state file.
Handles pagination cap (20 pages x 250 = 5,000 records) by subdividing
documents by year, then month if needed.

Usage:
  python3 scripts/12_expand_agencies.py                      # all 10 new agencies
  python3 scripts/12_expand_agencies.py --agency DOT DOE     # specific agencies only
  python3 scripts/12_expand_agencies.py --agency HHS         # single agency
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
DATA_DIR = PROJECT_DIR / "regulations_gov"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "expand_agencies_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

ALL_AGENCIES = ["DOT", "DOE", "HHS", "DOL", "DOI", "DHS", "DOJ", "ED", "HUD", "DOD"]
PAGE_SIZE = 250
MAX_PAGE = 20  # API hard limit: page[number] max is 20
MAX_RESULTS = MAX_PAGE * PAGE_SIZE  # 5,000
MIN_INTERVAL = 3.6  # seconds between requests (1,000/hr)
START_YEAR = 1994

with open(CONFIG_FILE) as f:
    API_KEY = json.load(f)["regulations_gov_api_key"]

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("expand_agencies")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "expand_agencies.log")
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


def api_get(session, endpoint, params):
    """Rate-limited GET with 429 handling."""
    rate.wait()
    url = f"{API_BASE}/{endpoint}"
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
        "completed": {},
        "total_dockets": 0,
        "total_documents": 0,
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


# === Pagination helper ===
def paginate(session, endpoint, params, output_dir):
    """
    Download all pages for a query. Returns (saved_count, total_elements).

    Skips pages that already exist on disk (resume support).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    p = dict(params)
    p["page[size]"] = PAGE_SIZE
    p["page[number]"] = 1

    # Check if page 1 already exists — if so, read meta from it to get totals
    page1_file = output_dir / "page_0001.json"
    if page1_file.exists():
        try:
            existing = json.loads(page1_file.read_text())
            meta = existing.get("meta", {})
            total_elements = meta.get("totalElements", 0)
            total_pages = min(meta.get("totalPages", 1), MAX_PAGE)
            saved = len(existing.get("data", []))
        except (json.JSONDecodeError, KeyError):
            # Corrupt file — re-download
            existing = None
    else:
        existing = None

    if existing is None:
        data = api_get(session, endpoint, p)
        meta = data.get("meta", {})
        total_elements = meta.get("totalElements", 0)
        total_pages = min(meta.get("totalPages", 1), MAX_PAGE)

        if total_elements == 0:
            return 0, 0

        page1_file.write_text(json.dumps(data))
        saved = len(data.get("data", []))
    elif total_elements == 0:
        return 0, 0

    for pg in range(2, total_pages + 1):
        if _shutdown:
            break
        page_file = output_dir / f"page_{pg:04d}.json"
        if page_file.exists():
            # Page already downloaded — count its records and skip
            try:
                existing_page = json.loads(page_file.read_text())
                saved += len(existing_page.get("data", []))
                continue
            except (json.JSONDecodeError, KeyError):
                pass  # Corrupt — re-download

        p["page[number]"] = pg
        try:
            data = api_get(session, endpoint, p)
            page_file.write_text(json.dumps(data))
            saved += len(data.get("data", []))
        except Exception as e:
            log.error(f"    Page {pg} failed: {e}")
            break

    return saved, total_elements


# === Docket downloads ===
def download_dockets(session, agency, state):
    """Download all dockets for an agency (simple pagination, no date filtering)."""
    key = f"dockets:{agency}"
    if key in state["completed"]:
        log.info(f"  [{agency}] Dockets: already done ({state['completed'][key]} records)")
        return state["completed"][key]

    log.info(f"  [{agency}] Downloading dockets...")
    out = DATA_DIR / "dockets" / agency

    saved, total = paginate(
        session, "dockets",
        {"filter[agencyId]": agency, "sort": "docketId"},
        out,
    )

    if total > MAX_RESULTS:
        log.warning(
            f"  [{agency}] Dockets exceed pagination limit "
            f"({total} total, {saved} saved). "
            f"Run 16_backfill_dockets.py to recover the rest."
        )

    state["completed"][key] = saved
    state["total_dockets"] += saved
    save_state(state)
    log.info(f"  [{agency}] Dockets: {saved} records saved")
    return saved


# === Document downloads (year -> month subdivision) ===
def download_documents_range(session, agency, date_ge, date_le, output_dir):
    """
    Download documents for a date range.
    If the range exceeds the pagination cap, subdivide by month.
    """
    saved, total = paginate(
        session, "documents",
        {
            "filter[agencyId]": agency,
            "filter[postedDate][ge]": date_ge,
            "filter[postedDate][le]": date_le,
            "sort": "postedDate",
        },
        output_dir,
    )

    if total <= MAX_RESULTS:
        return saved

    # Exceeded pagination limit — subdivide by month
    log.info(
        f"  [{agency}] Date range {date_ge}..{date_le} has {total} docs "
        f"— subdividing by month"
    )

    year = int(date_ge[:4])
    start_month = int(date_ge[5:7])
    end_month = int(date_le[5:7])
    month_total = 0

    for m in range(start_month, end_month + 1):
        if _shutdown:
            break
        if date(year, m, 1) > date.today():
            break
        last_day = monthrange(year, m)[1]
        m_ge = f"{year}-{m:02d}-01"
        m_le = f"{year}-{m:02d}-{last_day:02d}"
        m_dir = output_dir / f"{m:02d}"

        cnt, m_total = paginate(
            session, "documents",
            {
                "filter[agencyId]": agency,
                "filter[postedDate][ge]": m_ge,
                "filter[postedDate][le]": m_le,
                "sort": "postedDate",
            },
            m_dir,
        )

        if m_total > MAX_RESULTS:
            log.warning(
                f"  [{agency}] {year}-{m:02d} still exceeds limit "
                f"({m_total} docs) — saving what we can ({cnt})"
            )

        month_total += cnt

    return month_total


def download_documents(session, agency, state):
    """Download all documents for an agency, year by year."""
    log.info(f"  [{agency}] Downloading documents...")
    current_year = date.today().year
    agency_total = 0

    for year in range(START_YEAR, current_year + 1):
        if _shutdown:
            break
        key = f"documents:{agency}:{year}"
        if key in state["completed"]:
            agency_total += state["completed"][key]
            continue

        last_day = monthrange(year, 12)[1]
        date_ge = f"{year}-01-01"
        date_le = f"{year}-12-{last_day:02d}"
        out_dir = DATA_DIR / "documents" / agency / str(year)

        try:
            count = download_documents_range(
                session, agency, date_ge, date_le, out_dir
            )
            state["completed"][key] = count
            state["total_documents"] += count
            save_state(state)
            agency_total += count
            if count > 0:
                log.info(f"  [{agency}] {year}: {count} documents")
        except Exception as e:
            log.error(f"  [{agency}] {year} documents FAILED: {e}")

    log.info(f"  [{agency}] Total documents: {agency_total}")
    return agency_total


# === Main ===
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Download dockets and documents from Regulations.gov "
            "for additional high-priority agencies."
        )
    )
    parser.add_argument(
        "--agency",
        nargs="+",
        choices=ALL_AGENCIES,
        metavar="AGENCY",
        help=(
            f"One or more agencies to download "
            f"(choices: {', '.join(ALL_AGENCIES)}). "
            f"Default: all 10."
        ),
    )
    args = parser.parse_args()

    agencies = args.agency if args.agency else ALL_AGENCIES
    current_year = date.today().year

    log.info("=" * 60)
    log.info("EXPAND AGENCIES — Dockets & Documents for New Agencies")
    log.info(f"Agencies: {', '.join(agencies)}")
    log.info(f"Year range (documents): {START_YEAR}–{current_year}")
    log.info(f"Rate limit: {MIN_INTERVAL}s between requests ({int(3600 / MIN_INTERVAL)}/hr)")
    log.info(f"Output: {DATA_DIR}")
    log.info("=" * 60)
    progress(f"Expand agencies: STARTING — agencies={','.join(agencies)}")

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
        progress(f"Expand agencies: starting {agency}")

        # --- Dockets ---
        try:
            download_dockets(session, agency, state)
        except Exception as e:
            log.error(f"[{agency}] Dockets failed: {e}")

        # --- Documents ---
        if not _shutdown:
            try:
                download_documents(session, agency, state)
            except Exception as e:
                log.error(f"[{agency}] Documents failed: {e}")

        progress(
            f"Expand agencies: {agency} done — "
            f"dockets={state['total_dockets']}, docs={state['total_documents']}"
        )

    # Summary
    log.info("=" * 60)
    status = "Interrupted" if _shutdown else "Complete"
    log.info(f"EXPAND AGENCIES — {status}")
    log.info(f"Total dockets: {state['total_dockets']}")
    log.info(f"Total documents: {state['total_documents']}")
    log.info(f"API calls made: {rate.count}")
    log.info(f"Completed units: {len(state['completed'])}")
    log.info("=" * 60)

    progress(
        f"Expand agencies: {status.upper()} — "
        f"{state['total_dockets']} dockets, "
        f"{state['total_documents']} documents, "
        f"{rate.count} API calls"
    )


if __name__ == "__main__":
    main()
