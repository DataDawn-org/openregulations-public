#!/usr/bin/env python3
"""
Phase 2: Regulations.gov Docket & Document Metadata

Downloads docket and document metadata for priority agencies:
USDA, EPA, FDA, FWS, APHIS.

Rate limited to 1,000 requests/hour. Supports resume via state file.
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
DATA_DIR = PROJECT_DIR / "regulations_gov"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "regs_dockets_docs_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

DEFAULT_AGENCIES = ["USDA", "EPA", "FDA", "FWS", "APHIS"]
PAGE_SIZE = 250
MAX_PAGE = 20  # API hard limit: page[number] max is 20
MIN_INTERVAL = 3.6  # seconds between requests (1000/hr)
START_YEAR = 1994

# CLI: --agencies DOE,SEC,FAA  and  --api-key-2
_use_key2 = "--api-key-2" in sys.argv
_agency_arg = None
for i, a in enumerate(sys.argv):
    if a == "--agencies" and i + 1 < len(sys.argv):
        _agency_arg = sys.argv[i + 1]
AGENCIES = _agency_arg.split(",") if _agency_arg else DEFAULT_AGENCIES

with open(CONFIG_FILE) as f:
    _cfg = json.load(f)
    API_KEY = _cfg["regulations_gov_api_key_2"] if _use_key2 else _cfg["regulations_gov_api_key"]

# === Logging ===
log = logging.getLogger("regs_dockets_docs")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "regs_gov_dockets_docs.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
# Only add stdout handler if stdout is available (not broken pipe under nohup)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass

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

# === Download helpers ===
def paginate(session, endpoint, params, output_dir):
    """Download all pages for a query. Returns record count."""
    output_dir.mkdir(parents=True, exist_ok=True)
    p = dict(params)
    p["page[size]"] = PAGE_SIZE
    p["page[number]"] = 1

    data = api_get(session, endpoint, p)
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
            data = api_get(session, endpoint, p)
            (output_dir / f"page_{pg:04d}.json").write_text(json.dumps(data))
            saved += len(data.get("data", []))
        except Exception as e:
            log.error(f"  Page {pg} failed: {e}")
            break

    if total_elements > MAX_PAGE * PAGE_SIZE:
        log.warning(f"  Query returned {total_elements} results — only {saved} retrieved (pagination limit)")
        return -total_elements  # negative signals need for subdivision

    return saved


def download_dockets(session, agency, state):
    """Download all dockets for an agency."""
    key = f"dockets:{agency}"
    if key in state["completed"]:
        log.info(f"  [{agency}] Dockets: already done ({state['completed'][key]} records)")
        return state["completed"][key]

    log.info(f"  [{agency}] Downloading dockets...")
    out = DATA_DIR / "dockets" / agency
    saved = paginate(session, "dockets", {"filter[agencyId]": agency, "sort": "docketId"}, out)

    if saved < 0:
        log.warning(f"  [{agency}] Dockets exceed pagination limit ({-saved} total). Saved first {MAX_PAGE * PAGE_SIZE}.")
        saved = MAX_PAGE * PAGE_SIZE

    state["completed"][key] = saved
    state["total_dockets"] += saved
    save_state(state)
    log.info(f"  [{agency}] Dockets: {saved} records saved")
    return saved


def download_documents_range(session, agency, date_ge, date_le, output_dir):
    """Download documents for a date range, subdividing by month if needed."""
    params = {
        "filter[agencyId]": agency,
        "filter[postedDate][ge]": date_ge,
        "filter[postedDate][le]": date_le,
        "sort": "postedDate",
    }
    saved = paginate(session, "documents", params, output_dir)

    if saved >= 0:
        return saved

    # Exceeded pagination limit — subdivide by month
    total_available = -saved
    log.info(f"  [{agency}] Date range {date_ge}..{date_le} has {total_available} docs — subdividing by month")

    # Parse year from date_ge
    year = int(date_ge[:4])
    start_month = int(date_ge[5:7])
    end_month = int(date_le[5:7])
    total = 0

    for m in range(start_month, end_month + 1):
        if _shutdown:
            break
        if date(year, m, 1) > date.today():
            break
        last_day = monthrange(year, m)[1]
        m_ge = f"{year}-{m:02d}-01"
        m_le = f"{year}-{m:02d}-{last_day:02d}"
        m_dir = output_dir / f"{m:02d}"
        cnt = paginate(session, "documents", {
            "filter[agencyId]": agency,
            "filter[postedDate][ge]": m_ge,
            "filter[postedDate][le]": m_le,
            "sort": "postedDate",
        }, m_dir)
        if cnt < 0:
            log.warning(f"  [{agency}] {year}-{m:02d} still exceeds limit — saving what we can")
            cnt = MAX_PAGE * PAGE_SIZE
        total += cnt

    return total


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
            count = download_documents_range(session, agency, date_ge, date_le, out_dir)
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
    log.info("=" * 60)
    log.info("REGULATIONS.GOV DOCKETS & DOCUMENTS — Starting")
    log.info(f"Agencies: {', '.join(AGENCIES)}")
    log.info(f"Rate limit: {MIN_INTERVAL}s between requests ({int(3600/MIN_INTERVAL)}/hr)")
    log.info("=" * 60)
    progress("Regs.gov Phase 2: STARTING")

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
        progress(f"Regs.gov Phase 2: starting {agency}")

        try:
            download_dockets(session, agency, state)
        except Exception as e:
            log.error(f"[{agency}] Dockets failed: {e}")

        if not _shutdown:
            try:
                download_documents(session, agency, state)
            except Exception as e:
                log.error(f"[{agency}] Documents failed: {e}")

        progress(f"Regs.gov Phase 2: {agency} done — dockets={state['total_dockets']}, docs={state['total_documents']}")

    # Summary
    log.info("=" * 60)
    status = "Interrupted" if _shutdown else "Complete"
    log.info(f"REGULATIONS.GOV DOCKETS & DOCUMENTS — {status}")
    log.info(f"Total dockets: {state['total_dockets']}")
    log.info(f"Total documents: {state['total_documents']}")
    log.info(f"API calls made: {rate.count}")
    log.info("=" * 60)

    progress(f"Regs.gov Phase 2: {status.upper()} — {state['total_dockets']} dockets, {state['total_documents']} documents, {rate.count} API calls")

if __name__ == "__main__":
    main()
