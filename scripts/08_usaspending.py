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
from datetime import datetime, timedelta
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

# --- B part-2 (2026-06-23): keyset incremental drain by last_modified ---
CHUNK_SIZE = 5000                          # awards per incremental chunk file
INCREMENTAL_SUBDIR = "awards_incremental"  # write here, NEVER frozen awards/ — keeps the 471-file
                                           # baseline (manifest fdb8af10…) byte-intact = A-fallback
FREEZE_DATE = "2026-04-19"                 # USAspending one-way done-list froze here (state mtime)
OVERLAP_DAYS = 7                           # re-pull window: boundary ties + imprecise freeze cursor

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
    "Award ID", "Recipient Name", "Recipient UEI", "Recipient DUNS",
    "recipient_id",
    "Award Amount", "Total Outlays",
    "Description", "Award Type", "Start Date", "End Date",
    "Awarding Agency", "Awarding Sub Agency",
    "Funding Agency", "Funding Sub Agency",
    "Recipient Location State Code",
    "Place of Performance State Code", "Place of Performance Zip5",
    "CFDA Number", "Last Modified Date", "generated_internal_id",
]

CONTRACT_FIELDS = [
    "Award ID", "Recipient Name", "Recipient UEI", "Recipient DUNS",
    "recipient_id",
    "Award Amount", "Total Outlays",
    "Description", "Contract Award Type", "Award Type",
    "Start Date", "End Date",
    "Awarding Agency", "Awarding Sub Agency",
    "Funding Agency", "Funding Sub Agency",
    "Recipient Location State Code",
    "Place of Performance State Code",
    "NAICS Code", "NAICS Description", "Last Modified Date",
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
    s = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
    s.setdefault("stream_cursors", {})  # B part-2: stream_key -> last_modified watermark.
    return s                            # legacy "completed_streams" is ignored (kept for the record).


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


# === Individual awards — keyset drain by last_modified (B part-2, 2026-06-23) ===
# WHY keyset, not page-number: the page-number path's `hasNext` LIES at depth — measured 2026-06-23,
# it returned False at page 100 on a 183,812-record window while page 101 still served rows, and it
# 422s past page 500 (50,000). So page-number silently truncates >10K windows. Keyset pagination
# (last_record_sort_value + last_record_unique_id) is the ONLY complete drain, and the API's own 422
# prescribes it. Measured end-to-end (probe v1/v2/v3c): (sort_value, unique_id) is a TOTAL order —
# tie-safe, monotonic, drains a 63,154-record window 26% past the wall with zero dups, and terminates
# HONESTLY (hasNext=False == the true end; ascending-max == descending-newest). Drain is by
# date_type=last_modified_date so back-dated mods to old-FY awards are caught (a FY2019 award modified
# in 2026 lands in a recent last_modified window — the exact loss the freeze caused).


class DrainStalled(Exception):
    """Keyset cursor failed to advance while more remained, or a batch arrived out of order. Loud
    bail — partial progress is already durably written and the watermark holds; the observe-only
    action_date sweep flags the un-pulled remainder. This NEVER silently truncates (the bug we fix)."""


def _post_with_retry(session, payload, label, attempt_log, max_retries=5):
    """POST spending_by_award with bounded retry. Returns the JSON dict, or None after exhaustion."""
    retries = 0
    while retries < max_retries:
        try:
            resp = session.post(f"{API_BASE}/search/spending_by_award/", json=payload, timeout=120)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            retries += 1
            wait = min(30 * retries, 120)
            log.error(f"Error on {label} {attempt_log} (attempt {retries}/{max_retries}): {e}")
            if retries < max_retries:
                log.info(f"  Waiting {wait}s before retry...")
                time.sleep(wait)
    return None


def keyset_drain(fetch, tier, agency_name, type_codes, fields, start_date, end_date,
                 stream_key, max_calls=20000):
    """Yield every award with last_modified in [start_date, end_date], draining via keyset
    pagination. `fetch(payload) -> data dict | None` is injected (production: a retrying HTTP fetch;
    tests: a fixture API). Enforces the measured invariants and raises DrainStalled on a stall or an
    out-of-order batch — it does not silently stop short."""
    cursor = None     # (last_record_sort_value, last_record_unique_id); None == first page
    last_sv = None    # tail sort_value of the previous batch — monotonic guard
    calls = 0
    while calls < max_calls:
        payload = {
            "filters": {
                "agencies": [{"type": "awarding", "tier": tier, "name": agency_name}],
                "award_type_codes": type_codes,
                "time_period": [{
                    "start_date": start_date,
                    "end_date": end_date,
                    "date_type": "last_modified_date",
                }],
            },
            "fields": fields,
            "limit": PAGE_SIZE,
            "sort": "Last Modified Date",
            "order": "asc",
        }
        if cursor is not None:
            payload["last_record_sort_value"] = cursor[0]
            payload["last_record_unique_id"] = cursor[1]

        data = fetch(payload)
        calls += 1
        if data is None:
            raise DrainStalled(f"{stream_key}: API failed after retries at call {calls}")

        results = data.get("results", [])
        if not results:
            return                                  # empty window or clean end

        first_sv = results[0].get("Last Modified Date")
        if last_sv is not None and first_sv is not None and first_sv < last_sv:
            raise DrainStalled(
                f"{stream_key}: non-monotonic batch ({first_sv} < prior tail {last_sv}) at call {calls}")
        for r in results:
            yield r
        last_sv = results[-1].get("Last Modified Date")

        pm = data.get("page_metadata", {})
        if not pm.get("hasNext"):
            return                                  # honest termination (measured: == true end)
        nxt = (pm.get("last_record_sort_value"), pm.get("last_record_unique_id"))
        if nxt[0] is None or nxt == cursor:
            raise DrainStalled(f"{stream_key}: cursor failed to advance at call {calls}")
        cursor = nxt
    raise DrainStalled(f"{stream_key}: exceeded max_calls={max_calls} — undrained remainder")


def _save_chunk_incremental(results, out_dir, stream_key, run_stamp, seq):
    """Atomically write one chunk to awards_incremental/ with a collision-proof, run-stamped name.
    NEVER reuses the frozen `{stream}_{NNNN}.json` names in awards/, so the 471-file baseline
    (manifest fdb8af10…) stays byte-for-byte intact = the A-fallback. temp+rename so a crash
    mid-write can't leave a half-file the build would choke on."""
    fname = f"{stream_key}_inc_{run_stamp}_{seq:04d}.json"
    final = out_dir / fname
    tmp = out_dir / (fname + ".tmp")
    tmp.write_text(json.dumps(results))
    tmp.rename(final)
    log.info(f"  Saved {len(results):,} awards to {INCREMENTAL_SUBDIR}/{fname}")


def _overlap_start(watermark):
    """start_date for a stream's drain = (watermark or FREEZE_DATE) minus OVERLAP_DAYS, day-granular.
    The day-granular inclusive filter re-pulls the boundary day; the order-independent upsert dedups;
    the 7-day overlap covers boundary ties and the imprecise freeze cursor."""
    base = (watermark or FREEZE_DATE)[:10]
    try:
        d = datetime.strptime(base, "%Y-%m-%d").date() - timedelta(days=OVERLAP_DAYS)
    except ValueError:
        d = datetime.strptime(FREEZE_DATE, "%Y-%m-%d").date() - timedelta(days=OVERLAP_DAYS)
    return d.isoformat()


def fetch_stream_incremental(session, agency_label, tier, agency_name, award_type, type_codes,
                             fields, start_date, end_date, out_dir, state, run_stamp):
    """Keyset-drain one stream over [start_date, end_date]; write chunks to awards_incremental/;
    advance the per-stream watermark ONLY after each chunk is durably written (advance-after-durable-
    write). Returns (count, ok). On DrainStalled, persists partial progress and returns ok=False."""
    stream_key = f"{agency_label}_{award_type}"
    buf, seq, total = [], 0, 0
    watermark = state["stream_cursors"].get(stream_key)   # never regress below this

    def flush():
        nonlocal seq, buf, watermark
        if not buf:
            return
        seq += 1
        _save_chunk_incremental(buf, out_dir, stream_key, run_stamp, seq)
        chunk_max = max((r.get("Last Modified Date") for r in buf if r.get("Last Modified Date")),
                        default=None)
        if chunk_max and (watermark is None or chunk_max > watermark):
            watermark = chunk_max
        state["stream_cursors"][stream_key] = watermark
        save_state(state)                                 # persist watermark AFTER the chunk is on disk
        buf = []

    def fetch(payload):
        time.sleep(MIN_INTERVAL)
        return _post_with_retry(session, payload, stream_key, "keyset")

    try:
        for rec in keyset_drain(fetch, tier, agency_name, type_codes, fields,
                                start_date, end_date, stream_key):
            rec["_agency"] = agency_label
            rec["_award_category"] = award_type
            # No FY on the keyset path. The authoritative fiscal_year is the ACTION_DATE FY (what the
            # frozen full-pull bucketed by), and spending_by_award exposes no action_date here (not a
            # field, not sortable). MEASURED 2026-06-23: FY derived from Start Date is 46% wrong and
            # from Base Obligation Date 45% wrong vs the action_date FY (both track the BASE award, not
            # the latest action). So we carry NO FY opinion: 05_build's COALESCE preserves the
            # authoritative frozen value on updates; a keyset-only NEW award INSERTs with NULL FY
            # (honest gap — followup: FY-backfill via a bounded action_date pull).
            rec["_fiscal_year"] = None
            buf.append(rec)
            total += 1
            if len(buf) >= CHUNK_SIZE:
                flush()
        flush()                                           # final partial chunk
    except DrainStalled as e:
        flush()                                           # keep whatever drained before the stall
        log.error(f"  DRAIN STALLED {stream_key}: {e} — watermark held at {watermark}; "
                  f"action_date sweep will flag the remainder. Continuing to next stream.")
        progress(f"USAspending: {stream_key} STALLED at {watermark} ({total:,} pulled)")
        return total, False

    # Clean, complete drain to the honest end: everything modified through end_date is captured,
    # so the resume floor can move to end_date (next run re-pulls only [end_date - overlap, now]).
    state["stream_cursors"][stream_key] = end_date
    save_state(state)
    log.info(f"  {stream_key}: {total:,} awards, last_modified [{start_date}..{end_date}]")
    progress(f"USAspending: {stream_key} done — {total:,} awards through {end_date}")
    return total, True


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

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    overview_dir = OUTPUT_DIR / "overview"
    overview_dir.mkdir(exist_ok=True)

    total_awards = 0
    start_time = time.time()

    # Top-tier agency codes for overview endpoint
    TOPTIER_CODES = {
        "EPA": "068", "USDA": "012", "HHS": "075", "DOI": "014",
        "DOE": "089", "HUD": "086", "DOJ": "015", "ED": "091",
        "VA": "036", "NASA": "080", "SBA": "073", "DOT": "069",
        "DOL": "1601", "DOC": "013", "DHS": "070",
    }

    # Map subtier agencies to their parent department
    SUBTIER_TO_TOPTIER = {
        "APHIS": ("USDA", "012"), "FDA": ("HHS", "075"), "FWS": ("DOI", "014"),
        "NOAA": ("DOC", "013"), "OSHA": ("DOL", "1601"),
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

    # === Individual awards — keyset incremental drain by last_modified (B part-2) ===
    log.info("--- Individual awards (keyset incremental by last_modified) ---")
    run_stamp = time.strftime("%Y%m%dT%H%M%S")
    end_date = datetime.now().date().isoformat()
    incremental_dir = OUTPUT_DIR / INCREMENTAL_SUBDIR
    incremental_dir.mkdir(exist_ok=True)

    for agency_label in agencies:
        if agency_label not in AGENCIES:
            log.warning(f"Unknown agency: {agency_label}, skipping")
            continue
        tier, api_name = AGENCIES[agency_label]
        for award_type, type_codes, fields in (
            ("grants", GRANT_CODES, GRANT_FIELDS),
            ("contracts", CONTRACT_CODES, CONTRACT_FIELDS),
        ):
            stream_key = f"{agency_label}_{award_type}"
            start_date = _overlap_start(state["stream_cursors"].get(stream_key))
            log.info(f"  {stream_key}: keyset drain last_modified [{start_date} .. {end_date}]")
            count, ok = fetch_stream_incremental(
                session, agency_label, tier, api_name, award_type, type_codes, fields,
                start_date, end_date, incremental_dir, state, run_stamp,
            )
            total_awards += count
            if not ok:
                log.warning(f"  {stream_key} did not fully drain — see DRAIN STALLED above")

    # === Summary ===
    elapsed = time.time() - start_time
    log.info("=" * 60)
    log.info("USASPENDING.GOV — Complete")
    log.info(f"  Total awards downloaded: {total_awards:,}")
    log.info(f"  Elapsed: {elapsed/60:.1f} minutes")
    log.info(f"  Streams with cursors: {len(state['stream_cursors'])}")
    log.info("=" * 60)
    progress(f"USAspending: Complete — {total_awards:,} awards in {elapsed/60:.1f} minutes")

    # Acquisition counter (followup_queue #92, OBSERVE-ONLY). total_awards =
    # Σ fetch_awards_stream() returns = pre-dedup award records pulled this run
    # (08:232). Observation only — never breaches. ARM-TIME TODO: confirm this
    # is the exact pre-dedup layer (USAspending re-fetches full FY windows, so a
    # 0 here means dark, not quiet) before flipping to literal_zero.
    try:
        from acquisition_floor import record_acquisition
        record_acquisition("usaspending", total_awards, mode="observe")
    except ImportError:
        log.error("acquisition_floor module missing — #92 observe counter not recorded")


if __name__ == "__main__":
    main()
