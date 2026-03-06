#!/usr/bin/env python3
"""
Phase 1: Federal Register Bulk Document Ingestion

Downloads ALL documents from the Federal Register API (1994-present):
proposed rules, final rules, notices, executive orders, presidential documents.

Organized by year/month. Supports resume from interruption via state file.
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
BASE_URL = "https://www.federalregister.gov/api/v1/documents.json"
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "federal_register" / "raw"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "fr_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

PER_PAGE = 1000
START_YEAR = 1994
END_YEAR = date.today().year
REQUEST_DELAY = 0.5  # seconds between requests

# === Logging ===
log = logging.getLogger("fr_ingest")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "federal_register.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# === Graceful shutdown ===
_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received — finishing current request then saving state")
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# === HTTP Session with retries ===
def create_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

# === State management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"completed_months": {}, "total_documents": 0, "started_at": None}

def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(STATE_FILE)

def progress(msg):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
    with open(PROGRESS_FILE, "a") as f:
        f.write(line)

# === Ingestion ===
def fetch_month(session, year, month):
    """Download all document pages for a year/month. Returns doc count."""
    month_dir = DATA_DIR / str(year) / f"{month:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)

    last_day = monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    page = 1
    total_saved = 0

    while not _shutdown:
        params = {
            "per_page": PER_PAGE,
            "page": page,
            "conditions[publication_date][gte]": start_date,
            "conditions[publication_date][lte]": end_date,
            "order": "oldest",
        }

        resp = session.get(BASE_URL, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        total_pages = data.get("total_pages", 1)
        count = data.get("count", 0)

        out_file = month_dir / f"page_{page:04d}.json"
        out_file.write_text(json.dumps(data))

        total_saved += len(results)

        if page == 1:
            log.info(f"  {year}-{month:02d}: {count} docs across {total_pages} pages")

        if page >= total_pages or not results:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return total_saved

def main():
    log.info("=" * 60)
    log.info("FEDERAL REGISTER INGESTION — Starting")
    log.info(f"Range: {START_YEAR} to {END_YEAR}")
    log.info("=" * 60)
    progress("Federal Register: STARTING")

    state = load_state()
    if not state["started_at"]:
        state["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)

    session = create_session()
    today = date.today()
    errors = []
    session_docs = 0

    for year in range(START_YEAR, END_YEAR + 1):
        if _shutdown:
            break
        year_total = 0
        year_skipped = 0

        for month in range(1, 13):
            if _shutdown:
                break
            if date(year, month, 1) > today:
                break

            key = f"{year}-{month:02d}"

            if key in state["completed_months"]:
                year_skipped += state["completed_months"][key]
                year_total += state["completed_months"][key]
                continue

            try:
                count = fetch_month(session, year, month)
                if not _shutdown:
                    state["completed_months"][key] = count
                    state["total_documents"] += count
                    save_state(state)
                    year_total += count
                    session_docs += count
            except Exception as e:
                log.error(f"FAILED {year}-{month:02d}: {e}")
                errors.append(key)

            time.sleep(REQUEST_DELAY)

        if year_skipped:
            log.info(f"Year {year}: {year_total} documents ({year_skipped} previously downloaded)")
        else:
            log.info(f"Year {year}: {year_total} documents")
        progress(f"Federal Register: year {year} done — {year_total} docs, {state['total_documents']} cumulative")

    # Summary
    log.info("=" * 60)
    if _shutdown:
        log.info("FEDERAL REGISTER INGESTION — Interrupted (will resume)")
    else:
        log.info("FEDERAL REGISTER INGESTION — Complete")
    log.info(f"Total documents downloaded: {state['total_documents']}")
    log.info(f"Months completed: {len(state['completed_months'])}")
    if errors:
        log.warning(f"Failed months ({len(errors)}): {errors}")
    log.info("=" * 60)

    status = "INTERRUPTED" if _shutdown else "COMPLETE"
    progress(f"Federal Register: {status} — {state['total_documents']} total documents, {len(errors)} errors")

if __name__ == "__main__":
    main()
