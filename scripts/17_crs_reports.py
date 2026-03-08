#!/usr/bin/env python3
"""
Phase 17: Download CRS (Congressional Research Service) reports from Congress.gov API.

Downloads structured metadata for all ~13,600 CRS reports including:
  - Report ID, title, publish date, status, content type
  - Authors
  - Topics/subjects
  - Related legislation (bill references)
  - Summary text

Source: Congress.gov API (api.congress.gov/v3/crsreport)
Auth: Congress.gov API key (in config.json)

Usage:
    python3 17_crs_reports.py                    # incremental (new since last run)
    python3 17_crs_reports.py --full             # download all ~13,600 reports
    python3 17_crs_reports.py --limit 100        # stop after 100 reports
    python3 17_crs_reports.py --dry-run          # list reports without downloading details
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
CONGRESS_API = "https://api.congress.gov/v3"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"

_config = json.loads(CONFIG_FILE.read_text())
API_KEY = _config.get("congress_gov_api_key", "")
OUTPUT_DIR = PROJECT_DIR / "crs_reports"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "crs_state.json"

PAGE_SIZE = 250  # max per Congress.gov API page
REQUEST_DELAY = 0.5  # seconds between API calls

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("crs")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "crs_reports.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


# === HTTP Session ===
def create_session():
    session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=4))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (CRS reports)"})
    return session


# === State Management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_update": "1900-01-01T00:00:00Z", "reports_downloaded": 0}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# === API Functions ===
def fetch_report_list(session, since_dt=None, limit=None):
    """Fetch list of all CRS report IDs from Congress.gov API."""
    reports = []
    offset = 0

    log.info("Fetching CRS report list...")

    while True:
        if limit and len(reports) >= limit:
            log.info(f"Reached limit of {limit} reports")
            return reports[:limit]

        params = {
            "api_key": API_KEY,
            "limit": PAGE_SIZE,
            "offset": offset,
            "format": "json",
        }

        resp = session.get(f"{CONGRESS_API}/crsreport", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("CRSReports", data.get("crsReports", []))
        if not items:
            break

        for item in items:
            report_id = item.get("id")
            if not report_id:
                continue
            # Filter by update date if doing incremental
            if since_dt and item.get("updateDate", "") < since_dt:
                continue
            reports.append({
                "id": report_id,
                "title": item.get("title", ""),
                "updateDate": item.get("updateDate", ""),
                "publishDate": item.get("publishDate", ""),
                "status": item.get("status", ""),
                "contentType": item.get("contentType", ""),
            })

        total = data.get("pagination", {}).get("count", "?")
        log.info(f"  Fetched {len(reports)} / {total} report IDs")

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

        # Check if there's a next page
        if not data.get("pagination", {}).get("next"):
            break

    log.info(f"Found {len(reports)} CRS reports")
    return reports


def download_report_detail(session, report_id):
    """Download full detail for a single CRS report."""
    params = {"api_key": API_KEY, "format": "json"}
    resp = session.get(f"{CONGRESS_API}/crsreport/{report_id}", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    report = data.get("CRSReport", data.get("crsReport", data))

    # Extract structured fields
    result = {
        "id": report.get("id", report_id),
        "title": report.get("title", ""),
        "publishDate": report.get("publishDate", ""),
        "updateDate": report.get("updateDate", ""),
        "status": report.get("status", ""),
        "contentType": report.get("contentType", ""),
        "version": report.get("version"),
    }

    # Authors
    authors = report.get("authors", [])
    result["authors"] = [a.get("author", a) if isinstance(a, dict) else str(a) for a in authors]

    # Topics
    topics = report.get("topics", [])
    result["topics"] = [t.get("topic", t) if isinstance(t, dict) else str(t) for t in topics]

    # Summary
    result["summary"] = report.get("summary", "")

    # Related materials (legislation references)
    related = report.get("relatedMaterials", [])
    bill_refs = []
    for item in related:
        if isinstance(item, dict):
            bill_refs.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "congress": item.get("congress"),
                "type": item.get("type", ""),
                "number": item.get("number"),
            })
    result["relatedBills"] = bill_refs

    # Format URLs
    formats = report.get("formats", [])
    for fmt in formats:
        if isinstance(fmt, dict):
            if "pdf" in fmt.get("type", "").lower() or "pdf" in fmt.get("url", "").lower():
                result["pdf_url"] = fmt.get("url", "")
            elif "html" in fmt.get("type", "").lower() or "html" in fmt.get("url", "").lower():
                result["html_url"] = fmt.get("url", "")

    return result


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download CRS reports from Congress.gov")
    parser.add_argument("--full", action="store_true", help="Download all reports")
    parser.add_argument("--limit", type=int, help="Stop after N reports")
    parser.add_argument("--dry-run", action="store_true", help="List reports without downloading details")
    args = parser.parse_args()

    if not API_KEY:
        log.error("No congress_gov_api_key in config.json")
        sys.exit(1)

    state = load_state()
    session = create_session()

    since_dt = None if args.full else state["last_update"]
    if since_dt:
        log.info(f"=== CRS Reports Download (since {since_dt}) ===")
    else:
        log.info("=== CRS Reports Download (full) ===")

    # Fetch report list
    report_list = fetch_report_list(session, since_dt=since_dt, limit=args.limit)

    if args.dry_run:
        for r in report_list[:20]:
            log.info(f"  {r['id']}: {r['title'][:80]}")
        log.info(f"Total: {len(report_list)} reports (dry run)")
        return

    # Check existing
    existing = {f.stem for f in OUTPUT_DIR.glob("*.json")}
    to_download = [r for r in report_list if r["id"] not in existing]
    log.info(f"Already have {len(existing)} reports, {len(to_download)} new to download")

    if args.limit:
        to_download = to_download[:args.limit]

    # Download details
    downloaded = 0
    errors = 0
    max_update = state["last_update"]

    for i, report in enumerate(to_download):
        rid = report["id"]
        try:
            detail = download_report_detail(session, rid)
            out_file = OUTPUT_DIR / f"{rid}.json"
            out_file.write_text(json.dumps(detail, indent=2, ensure_ascii=False))
            downloaded += 1

            if report.get("updateDate", "") > max_update:
                max_update = report["updateDate"]

            if downloaded % 100 == 0:
                log.info(f"  Progress: {downloaded}/{len(to_download)} downloaded, {errors} errors")
                state["last_update"] = max_update
                state["reports_downloaded"] = len(existing) + downloaded
                save_state(state)

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.warning(f"  {rid}: 404 Not Found")
            else:
                log.error(f"  {rid}: HTTP error: {e}")
            errors += 1
        except Exception as e:
            log.error(f"  {rid}: Error: {e}")
            errors += 1

        time.sleep(REQUEST_DELAY)

    # Final state
    state["last_update"] = max_update
    state["reports_downloaded"] = len(existing) + downloaded
    save_state(state)

    log.info(f"=== Complete ===")
    log.info(f"Downloaded: {downloaded}")
    log.info(f"Errors: {errors}")
    log.info(f"Total on disk: {len(existing) + downloaded}")


if __name__ == "__main__":
    main()
