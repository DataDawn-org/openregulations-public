#!/usr/bin/env python3
"""
Phase 8: Download federal spending data from USAspending.gov.

Downloads grants and contracts for 20 federal agencies (toptier departments
and key regulatory subtier agencies) using the public API (no auth needed).
Data covers FY2017-present.

Stores results as JSON batches in usaspending/awards/ and builds
SQLite tables via 05_build_database.py.

Usage:
    python3 08_usaspending.py                   # all agencies, FY2017-present
    python3 08_usaspending.py --fy 2024 2025    # specific fiscal years
    python3 08_usaspending.py --agency EPA       # single agency
    python3 08_usaspending.py --agency DOE --agency NASA  # multiple agencies
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
API_BASE = "https://api.usaspending.gov/api/v2"
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "usaspending"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "usaspending_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

PAGE_SIZE = 100
MIN_INTERVAL = 0.5  # seconds between requests (no documented rate limit)

# Agency definitions: (tier, api_name)
# Toptier = whole department, subtier = specific bureau/agency within a department
AGENCIES = {
    # --- Original 4 (environment / health / agriculture) ---
    "EPA":   ("toptier", "Environmental Protection Agency"),
    "APHIS": ("subtier", "Animal and Plant Health Inspection Service"),
    "FDA":   ("subtier", "Food and Drug Administration"),
    "FWS":   ("subtier", "U.S. Fish and Wildlife Service"),
    # --- Major departments (toptier) ---
    "DOE":   ("toptier", "Department of Energy"),
    "HUD":   ("toptier", "Department of Housing and Urban Development"),
    "DOJ":   ("toptier", "Department of Justice"),
    "ED":    ("toptier", "Department of Education"),
    "VA":    ("toptier", "Department of Veterans Affairs"),
    "NASA":  ("toptier", "National Aeronautics and Space Administration"),
    "SBA":   ("toptier", "Small Business Administration"),
    "DOT":   ("toptier", "Department of Transportation"),
    "DOL":   ("toptier", "Department of Labor"),
    "DOC":   ("toptier", "Department of Commerce"),
    "DHS":   ("toptier", "Department of Homeland Security"),
    # --- Key regulatory subtier agencies ---
    "NOAA":  ("subtier", "National Oceanic and Atmospheric Administration"),
    "OSHA":  ("subtier", "Occupational Safety and Health Administration"),
    "FAA":   ("subtier", "Federal Aviation Administration"),
    "NHTSA": ("subtier", "National Highway Traffic Safety Administration"),
    "FEMA":  ("subtier", "Federal Emergency Management Agency"),
}

GRANT_CODES = ["02", "03", "04", "05"]
CONTRACT_CODES = ["A", "B", "C", "D"]

GRANT_FIELDS = [
    "Award ID", "Recipient Name", "Award Amount", "Total Outlays",
    "Description", "Award Type", "Start Date", "End Date",
    "Awarding Agency", "Awarding Sub Agency",
    "Funding Agency", "Funding Sub Agency",
    "Place of Performance State Code", "Place of Performance Zip5",
    "CFDA Number", "generated_internal_id",
]

CONTRACT_FIELDS = [
    "Award ID", "Recipient Name", "Award Amount", "Total Outlays",
    "Description", "Contract Award Type", "Award Type",
    "Start Date", "End Date",
    "Awarding Agency", "Awarding Sub Agency",
    "Funding Agency", "Funding Sub Agency",
    "Place of Performance State Code",
    "NAICS Code", "NAICS Description",
    "generated_internal_id",
]

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("usaspending")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "usaspending.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)


# === HTTP session ===
def make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=5, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def progress(msg):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


# === State management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"completed_streams": []}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(STATE_FILE)


# === Agency overview (quick summary data) ===
def fetch_agency_overview(session, toptier_code, fiscal_year):
    """Get obligations breakdown for a top-tier agency."""
    url = f"{API_BASE}/agency/{toptier_code}/obligations_by_award_category/"
    resp = session.get(url, params={"fiscal_year": fiscal_year}, timeout=60)
    resp.raise_for_status()
    return resp.json()


# === Spending over time ===
def fetch_spending_over_time(session, agency_name, tier, start_fy, end_fy):
    """Get annual spending totals for an agency."""
    url = f"{API_BASE}/search/spending_over_time/"
    payload = {
        "group": "fiscal_year",
        "filters": {
            "agencies": [{"type": "awarding", "tier": tier, "name": agency_name}],
            "time_period": [{
                "start_date": f"{start_fy - 1}-10-01",
                "end_date": f"{end_fy}-09-30",
            }],
        },
    }
    resp = session.post(url, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


# === Individual awards (paginated) ===
def fetch_awards_stream(session, agency_label, tier, agency_name, award_type,
                        type_codes, fields, fiscal_years, output_dir):
    """Download all awards for one agency+type combination, paginating through results."""
    stream_key = f"{agency_label}_{award_type}"

    all_results = []
    total_fetched = 0

    for fy in fiscal_years:
        page = 1
        fy_count = 0

        while True:
            payload = {
                "filters": {
                    "agencies": [{"type": "awarding", "tier": tier, "name": agency_name}],
                    "award_type_codes": type_codes,
                    "time_period": [{
                        "start_date": f"{fy - 1}-10-01",
                        "end_date": f"{fy}-09-30",
                    }],
                },
                "fields": fields,
                "page": page,
                "limit": PAGE_SIZE,
                "sort": "Award Amount",
                "order": "desc",
            }

            time.sleep(MIN_INTERVAL)
            retries = 0
            data = None
            while retries < 5:
                try:
                    resp = session.post(
                        f"{API_BASE}/search/spending_by_award/",
                        json=payload,
                        timeout=120,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as e:
                    retries += 1
                    wait = min(30 * retries, 120)
                    log.error(f"Error on {stream_key} FY{fy} page {page} (attempt {retries}/5): {e}")
                    if retries < 5:
                        log.info(f"  Waiting {wait}s before retry...")
                        time.sleep(wait)

            if data is None:
                log.warning(f"Skipping {stream_key} FY{fy} page {page} after 5 failures")
                break

            results = data.get("results", [])
            if not results:
                break

            # Tag each result with metadata
            for r in results:
                r["_agency"] = agency_label
                r["_award_category"] = award_type
                r["_fiscal_year"] = fy

            all_results.extend(results)
            fy_count += len(results)
            total_fetched += len(results)

            has_next = data.get("page_metadata", {}).get("hasNext", False)

            if page % 10 == 0 or not has_next:
                log.info(f"  {stream_key} FY{fy}: page {page}, {fy_count:,} awards so far")

            if not has_next:
                break

            page += 1

            # Save in chunks of 5000
            if len(all_results) >= 5000:
                _save_chunk(all_results, output_dir, stream_key)
                all_results = []

        log.info(f"  {stream_key} FY{fy}: {fy_count:,} awards")

    # Save remaining
    if all_results:
        _save_chunk(all_results, output_dir, stream_key)

    return total_fetched


_chunk_counters = {}


def _save_chunk(results, output_dir, stream_key):
    _chunk_counters[stream_key] = _chunk_counters.get(stream_key, 0) + 1
    n = _chunk_counters[stream_key]
    outfile = output_dir / f"{stream_key}_{n:04d}.json"
    outfile.write_text(json.dumps(results))
    log.info(f"  Saved {len(results):,} awards to {outfile.name}")


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download USAspending.gov data")
    parser.add_argument("--agency", action="append",
                        help="Agency to target: EPA, APHIS, FDA, FWS (can repeat)")
    parser.add_argument("--fy", type=int, nargs="+",
                        help="Fiscal years to download (default: 2017-2025)")
    parser.add_argument("--skip-overview", action="store_true",
                        help="Skip agency overview/trend data")
    args = parser.parse_args()

    agencies = [a.upper() for a in args.agency] if args.agency else list(AGENCIES.keys())
    fiscal_years = args.fy or list(range(2017, 2027))

    log.info("=" * 60)
    log.info("USASPENDING.GOV — Starting download")
    log.info(f"  Agencies: {', '.join(agencies)}")
    log.info(f"  Fiscal years: {fiscal_years[0]}-{fiscal_years[-1]}")
    log.info("=" * 60)

    session = make_session()
    state = load_state()
    completed = set(state.get("completed_streams", []))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    overview_dir = OUTPUT_DIR / "overview"
    awards_dir = OUTPUT_DIR / "awards"
    overview_dir.mkdir(exist_ok=True)
    awards_dir.mkdir(exist_ok=True)

    total_awards = 0
    start_time = time.time()

    # Top-tier agency codes for overview endpoint
    TOPTIER_CODES = {
        "EPA": "068", "USDA": "012", "HHS": "075", "DOI": "014",
        "DOE": "089", "HUD": "086", "DOJ": "015", "ED": "091",
        "VA": "036", "NASA": "080", "SBA": "073", "DOT": "069",
        "DOL": "016", "DOC": "013", "DHS": "070",
    }

    # Map subtier agencies to their parent department
    SUBTIER_TO_TOPTIER = {
        "APHIS": ("USDA", "012"), "FDA": ("HHS", "075"), "FWS": ("DOI", "014"),
        "NOAA": ("DOC", "013"), "OSHA": ("DOL", "016"),
        "FAA": ("DOT", "069"), "NHTSA": ("DOT", "069"), "FEMA": ("DHS", "070"),
    }

    # === Agency overviews & spending trends ===
    if not args.skip_overview:
        log.info("--- Agency overviews & spending trends ---")

        # Map agencies to their toptier parent for overview
        overview_agencies = set()
        for a in agencies:
            if a in SUBTIER_TO_TOPTIER:
                name, code = SUBTIER_TO_TOPTIER[a]
                overview_agencies.add((name, code))
            elif a in TOPTIER_CODES:
                overview_agencies.add((a, TOPTIER_CODES[a]))

        for name, code in overview_agencies:
            # Obligations by category for recent FYs
            for fy in fiscal_years[-3:]:
                try:
                    time.sleep(MIN_INTERVAL)
                    data = fetch_agency_overview(session, code, fy)
                    outfile = overview_dir / f"{name}_FY{fy}_overview.json"
                    outfile.write_text(json.dumps(data, indent=2))
                    total_amt = data.get("total_aggregated_amount", 0)
                    log.info(f"  {name} FY{fy}: ${total_amt:,.0f} total obligations")
                except Exception as e:
                    log.error(f"  Error fetching {name} FY{fy} overview: {e}")

            # Spending over time for all FYs
            # Map toptier label to full API name for the spending_over_time endpoint
            TOPTIER_API_NAMES = {
                "USDA": "Department of Agriculture",
                "HHS": "Department of Health and Human Services",
                "DOI": "Department of the Interior",
                "DOE": "Department of Energy",
                "HUD": "Department of Housing and Urban Development",
                "DOJ": "Department of Justice",
                "ED": "Department of Education",
                "VA": "Department of Veterans Affairs",
                "NASA": "National Aeronautics and Space Administration",
                "SBA": "Small Business Administration",
                "DOT": "Department of Transportation",
                "DOL": "Department of Labor",
                "DOC": "Department of Commerce",
                "DHS": "Department of Homeland Security",
            }
            tier, api_name = AGENCIES.get(name, ("toptier", name))
            if name not in AGENCIES:
                api_name = TOPTIER_API_NAMES.get(name, name)
                tier = "toptier"
            try:
                time.sleep(MIN_INTERVAL)
                data = fetch_spending_over_time(
                    session, api_name, tier, fiscal_years[0], fiscal_years[-1]
                )
                outfile = overview_dir / f"{name}_spending_over_time.json"
                outfile.write_text(json.dumps(data, indent=2))
                log.info(f"  {name} spending over time: {len(data.get('results', []))} years")
            except Exception as e:
                log.error(f"  Error fetching {name} spending over time: {e}")

    # === Individual awards ===
    log.info("--- Individual awards ---")

    for agency_label in agencies:
        if agency_label not in AGENCIES:
            log.warning(f"Unknown agency: {agency_label}, skipping")
            continue

        tier, api_name = AGENCIES[agency_label]

        # Grants
        stream_key = f"{agency_label}_grants"
        if stream_key in completed:
            log.info(f"  Skipping {stream_key} (already completed)")
        else:
            log.info(f"  Downloading {agency_label} grants...")
            count = fetch_awards_stream(
                session, agency_label, tier, api_name,
                "grants", GRANT_CODES, GRANT_FIELDS,
                fiscal_years, awards_dir,
            )
            total_awards += count
            completed.add(stream_key)
            state["completed_streams"] = list(completed)
            save_state(state)
            progress(f"USAspending: {stream_key} done — {count:,} awards")

        # Contracts
        stream_key = f"{agency_label}_contracts"
        if stream_key in completed:
            log.info(f"  Skipping {stream_key} (already completed)")
        else:
            log.info(f"  Downloading {agency_label} contracts...")
            count = fetch_awards_stream(
                session, agency_label, tier, api_name,
                "contracts", CONTRACT_CODES, CONTRACT_FIELDS,
                fiscal_years, awards_dir,
            )
            total_awards += count
            completed.add(stream_key)
            state["completed_streams"] = list(completed)
            save_state(state)
            progress(f"USAspending: {stream_key} done — {count:,} awards")

    # === Summary ===
    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("USASPENDING.GOV — Complete")
    log.info(f"  Total awards downloaded: {total_awards:,}")
    log.info(f"  Elapsed: {elapsed/60:.1f} minutes")
    log.info(f"  Streams completed: {len(completed)}")
    log.info("=" * 60)
    progress(f"USAspending: Complete — {total_awards:,} awards in {elapsed/60:.1f} minutes")


if __name__ == "__main__":
    main()
