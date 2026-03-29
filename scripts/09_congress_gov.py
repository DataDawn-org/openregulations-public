#!/usr/bin/env python3
"""
Phase 9: Download legislation data from Congress.gov + GovInfo BILLSTATUS.

Uses the Congress.gov API (v3) for bill listings, then downloads full bill
data from GovInfo BILLSTATUS bulk XML (no rate limits). Downloads ALL bills
regardless of policy area to support cross-agency regulatory tracking.

BILLSTATUS XML includes: sponsors, cosponsors, actions, committees,
amendments, subjects, policy areas, related bills, summaries, and more.

Requires an API key from https://api.congress.gov/sign-up/
Add to scripts/config.json: "congress_gov_api_key": "YOUR_KEY"

Usage:
    python3 09_congress_gov.py                     # current + previous congress
    python3 09_congress_gov.py --congress 118 119   # specific congresses
    python3 09_congress_gov.py --full               # all congresses (93-119)

Note: If upgrading from the previous version that filtered by policy area,
run with --reset to re-download bills that were previously skipped.
"""

import argparse
import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
API_BASE = "https://api.congress.gov/v3"
GOVINFO_BASE = "https://www.govinfo.gov/bulkdata/BILLSTATUS"
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "congress_gov"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "congress_state.json"
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"

PAGE_SIZE = 250  # max allowed by Congress.gov API
API_INTERVAL = 0.8  # seconds between API requests (~4,500/hr)
BULK_WORKERS = 8  # parallel downloads from GovInfo (static files)

# Bill types to download (all 8 types including simple resolutions)
BILL_TYPES = ["hr", "s", "hjres", "sjres", "hconres", "sconres", "hres", "sres"]

# Current congress number (119th: 2025-2027)
CURRENT_CONGRESS = 119

# GovInfo BILLSTATUS XML only available from congress 108+
# Older congresses use Congress.gov API v3 JSON (rate-limited)
MIN_BILLSTATUS_CONGRESS = 108

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("congress")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "congress.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
# Only add stdout handler if stdout is a real terminal/file (not broken pipe)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


def load_config():
    """Load API key from config.json."""
    if not CONFIG_FILE.exists():
        log.error(f"Config file not found: {CONFIG_FILE}")
        sys.exit(1)
    with open(CONFIG_FILE) as f:
        config = json.load(f)
    key = config.get("congress_gov_api_key")
    if not key or key == "YOUR_KEY":
        log.error("congress_gov_api_key not set in config.json")
        sys.exit(1)
    return key


def create_api_session(api_key):
    """Create requests session for Congress.gov API (rate-limited)."""
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.params = {"api_key": api_key, "format": "json"}
    session.headers.update({"Accept": "application/json"})
    return session


def create_bulk_session():
    """Create requests session for GovInfo bulk downloads (no rate limit)."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=BULK_WORKERS + 2))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (regulatory data project)"})
    return session


def load_state():
    """Load download progress state."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"completed_congresses": [], "completed_bills": {}}


def save_state(state):
    """Save download progress state."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def api_get(session, url, params=None):
    """Make a Congress.gov API request with rate limiting."""
    time.sleep(API_INTERVAL)
    try:
        resp = session.get(url, params=params or {}, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            log.warning(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            resp = session.get(url, params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        log.error(f"API error: {e}")
        return None


# === Phase 1: Get bill number listings from Congress.gov API ===

def download_bills_list(session, congress, bill_type):
    """Download bill number listing for a congress+type from Congress.gov API."""
    bills = []
    offset = 0
    while True:
        url = f"{API_BASE}/bill/{congress}/{bill_type}"
        params = {"limit": PAGE_SIZE, "offset": offset}
        data = api_get(session, url, params)
        if not data:
            log.error(f"Failed to fetch {bill_type} list at offset {offset}")
            break
        batch = data.get("bills", [])
        if not batch:
            break
        bills.extend(batch)
        offset += PAGE_SIZE
        total = data.get("pagination", {}).get("count", "?")
        log.info(f"  {bill_type.upper()} {congress}: {len(bills)}/{total} listed")
        if "next" not in data.get("pagination", {}):
            break
    return bills


# === Phase 2: Download BILLSTATUS XML from GovInfo ===

def xml_text(elem, path, default=""):
    """Safely get text from an XML path."""
    node = elem.find(path)
    return node.text.strip() if node is not None and node.text else default


def parse_billstatus_xml(xml_text_content):
    """Parse BILLSTATUS XML into a structured dict."""
    root = ET.fromstring(xml_text_content)
    bill = root.find(".//bill")
    if bill is None:
        return None

    result = {
        "number": xml_text(bill, "number"),
        "type": xml_text(bill, "type"),
        "congress": xml_text(bill, "congress"),
        "title": xml_text(bill, "title"),
        "originChamber": xml_text(bill, "originChamber"),
        "introducedDate": xml_text(bill, "introducedDate"),
        "updateDate": xml_text(bill, "updateDate"),
        "constitutionalAuthorityStatement": xml_text(bill, "constitutionalAuthorityStatementText"),
    }

    # Policy area
    pa = bill.find("policyArea/name")
    result["policyArea"] = pa.text.strip() if pa is not None and pa.text else None

    # Sponsors
    sponsors = []
    for sp in bill.findall(".//sponsors/item"):
        sponsors.append({
            "bioguideId": xml_text(sp, "bioguideId"),
            "fullName": xml_text(sp, "fullName"),
            "firstName": xml_text(sp, "firstName"),
            "lastName": xml_text(sp, "lastName"),
            "party": xml_text(sp, "party"),
            "state": xml_text(sp, "state"),
            "district": xml_text(sp, "district"),
        })
    result["sponsors"] = sponsors

    # Cosponsors
    cosponsors = []
    for co in bill.findall(".//cosponsors/item"):
        cosponsors.append({
            "bioguideId": xml_text(co, "bioguideId"),
            "fullName": xml_text(co, "fullName"),
            "firstName": xml_text(co, "firstName"),
            "lastName": xml_text(co, "lastName"),
            "party": xml_text(co, "party"),
            "state": xml_text(co, "state"),
            "district": xml_text(co, "district"),
            "sponsorshipDate": xml_text(co, "sponsorshipDate"),
            "isOriginalCosponsor": xml_text(co, "isOriginalCosponsor"),
        })
    result["cosponsors"] = cosponsors

    # Subjects
    subjects = []
    for subj in bill.findall(".//subjects/legislativeSubjects/item/name"):
        if subj.text:
            subjects.append(subj.text.strip())
    result["subjects"] = subjects

    # Actions
    actions = []
    for act in bill.findall(".//actions/item"):
        action = {
            "actionDate": xml_text(act, "actionDate"),
            "text": xml_text(act, "text"),
            "type": xml_text(act, "type"),
        }
        committee = act.find("committee/name")
        if committee is not None and committee.text:
            action["committee"] = committee.text.strip()
        source = act.find("sourceSystem/name")
        if source is not None and source.text:
            action["source"] = source.text.strip()
        actions.append(action)
    result["actions"] = actions

    # Summaries
    summaries = []
    for summ in bill.findall(".//summaries/summary"):
        summaries.append({
            "versionCode": xml_text(summ, "versionCode"),
            "actionDate": xml_text(summ, "actionDate"),
            "actionDesc": xml_text(summ, "actionDesc"),
            "text": xml_text(summ, "text"),
            "updateDate": xml_text(summ, "updateDate"),
        })
    result["summaries"] = summaries

    # Committees
    committees = []
    for comm in bill.findall(".//committees/item"):
        committees.append({
            "name": xml_text(comm, "name"),
            "chamber": xml_text(comm, "chamber"),
            "type": xml_text(comm, "type"),
        })
    result["committees"] = committees

    # Related bills
    related = []
    for rel in bill.findall(".//relatedBills/item"):
        related.append({
            "number": xml_text(rel, "number"),
            "type": xml_text(rel, "type"),
            "congress": xml_text(rel, "congress"),
            "title": xml_text(rel, "title"),
        })
    result["relatedBills"] = related

    # Latest action
    la = bill.find("latestAction")
    if la is not None:
        result["latestAction"] = {
            "actionDate": xml_text(la, "actionDate"),
            "text": xml_text(la, "text"),
        }

    # CBO cost estimates
    cbo = []
    for est in bill.findall(".//cboCostEstimates/item"):
        cbo.append({
            "pubDate": xml_text(est, "pubDate"),
            "title": xml_text(est, "title"),
            "url": xml_text(est, "url"),
        })
    if cbo:
        result["cboCostEstimates"] = cbo

    return result


def download_single_billstatus(bulk_session, congress, bill_type, number):
    """Download and parse a single BILLSTATUS XML file from GovInfo."""
    url = f"{GOVINFO_BASE}/{congress}/{bill_type}/BILLSTATUS-{congress}{bill_type}{number}.xml"
    try:
        resp = bulk_session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return parse_billstatus_xml(resp.text)
    except Exception as e:
        log.error(f"Failed to download BILLSTATUS-{congress}{bill_type}{number}: {e}")
        return None


def download_bill_via_api(api_session, congress, bill_type, number):
    """Download bill detail from Congress.gov API v3 (for congresses without BILLSTATUS XML).

    Returns a dict in the same format as parse_billstatus_xml for compatibility.
    Requires multiple API calls per bill: base + actions + cosponsors + subjects + summaries.
    """
    base_url = f"{API_BASE}/bill/{congress}/{bill_type}/{number}"

    # Base bill info
    data = api_get(api_session, base_url)
    if not data or "bill" not in data:
        return None
    bill = data["bill"]

    result = {
        "number": str(bill.get("number", "")),
        "type": bill.get("type", "").lower(),
        "congress": str(bill.get("congress", congress)),
        "title": bill.get("title", ""),
        "originChamber": bill.get("originChamber", ""),
        "introducedDate": bill.get("introducedDate", ""),
        "updateDate": bill.get("updateDate", ""),
        "constitutionalAuthorityStatement": bill.get("constitutionalAuthorityStatementText", ""),
    }

    # Policy area
    pa = bill.get("policyArea", {})
    result["policyArea"] = pa.get("name") if pa else None

    # Sponsors (inline in base response)
    sponsors = []
    for sp in bill.get("sponsors", []):
        sponsors.append({
            "bioguideId": sp.get("bioguideId", ""),
            "fullName": sp.get("fullName", sp.get("firstName", "") + " " + sp.get("lastName", "")),
            "firstName": sp.get("firstName", ""),
            "lastName": sp.get("lastName", ""),
            "party": sp.get("party", ""),
            "state": sp.get("state", ""),
            "district": str(sp.get("district", "")),
        })
    result["sponsors"] = sponsors

    # Latest action (inline)
    la = bill.get("latestAction", {})
    if la:
        result["latestAction"] = {
            "actionDate": la.get("actionDate", ""),
            "text": la.get("text", ""),
        }

    # Cosponsors (separate endpoint)
    cosponsors = []
    cos_url = bill.get("cosponsors", {}).get("url")
    if cos_url:
        offset = 0
        while True:
            cos_data = api_get(api_session, cos_url, {"limit": PAGE_SIZE, "offset": offset})
            if not cos_data:
                break
            for co in cos_data.get("cosponsors", []):
                cosponsors.append({
                    "bioguideId": co.get("bioguideId", ""),
                    "fullName": co.get("fullName", ""),
                    "firstName": co.get("firstName", ""),
                    "lastName": co.get("lastName", ""),
                    "party": co.get("party", ""),
                    "state": co.get("state", ""),
                    "district": str(co.get("district", "")),
                    "sponsorshipDate": co.get("sponsorshipDate", ""),
                    "isOriginalCosponsor": str(co.get("isOriginalCosponsor", "")),
                })
            if "next" not in cos_data.get("pagination", {}):
                break
            offset += PAGE_SIZE
    result["cosponsors"] = cosponsors

    # Actions (separate endpoint)
    actions = []
    act_url = bill.get("actions", {}).get("url")
    if act_url:
        offset = 0
        while True:
            act_data = api_get(api_session, act_url, {"limit": PAGE_SIZE, "offset": offset})
            if not act_data:
                break
            for act in act_data.get("actions", []):
                action = {
                    "actionDate": act.get("actionDate", ""),
                    "text": act.get("text", ""),
                    "type": act.get("type", ""),
                }
                comm = act.get("committee", {})
                if comm and comm.get("name"):
                    action["committee"] = comm["name"]
                src = act.get("sourceSystem", {})
                if src and src.get("name"):
                    action["source"] = src["name"]
                actions.append(action)
            if "next" not in act_data.get("pagination", {}):
                break
            offset += PAGE_SIZE
    result["actions"] = actions

    # Subjects (separate endpoint)
    subjects = []
    subj_url = bill.get("subjects", {}).get("url")
    if subj_url:
        subj_data = api_get(api_session, subj_url)
        if subj_data:
            for s in subj_data.get("subjects", {}).get("legislativeSubjects", []):
                name = s.get("name")
                if name:
                    subjects.append(name)
    result["subjects"] = subjects

    # Summaries (separate endpoint)
    summaries = []
    summ_url = bill.get("summaries", {}).get("url")
    if summ_url:
        summ_data = api_get(api_session, summ_url)
        if summ_data:
            for s in summ_data.get("summaries", []):
                summaries.append({
                    "versionCode": s.get("versionCode", ""),
                    "actionDate": s.get("actionDate", ""),
                    "actionDesc": s.get("actionDesc", ""),
                    "text": s.get("text", ""),
                    "updateDate": s.get("updateDate", ""),
                })
    result["summaries"] = summaries

    # Committees (inline in base)
    committees = []
    for comm in bill.get("committees", {}).get("url", []) if isinstance(bill.get("committees"), dict) else []:
        pass  # committees need separate fetch
    # Fetch committees
    comm_url = bill.get("committees", {}).get("url") if isinstance(bill.get("committees"), dict) else None
    if comm_url:
        comm_data = api_get(api_session, comm_url)
        if comm_data:
            for c in comm_data.get("committees", []):
                committees.append({
                    "name": c.get("name", ""),
                    "chamber": c.get("chamber", ""),
                    "type": c.get("type", ""),
                })
    result["committees"] = committees

    # Related bills
    related = []
    rel_url = bill.get("relatedBills", {}).get("url") if isinstance(bill.get("relatedBills"), dict) else None
    if rel_url:
        rel_data = api_get(api_session, rel_url)
        if rel_data:
            for r in rel_data.get("relatedBills", []):
                related.append({
                    "number": str(r.get("number", "")),
                    "type": r.get("type", "").lower(),
                    "congress": str(r.get("congress", "")),
                    "title": r.get("title", ""),
                })
    result["relatedBills"] = related

    return result


# === Phase 3: Orchestrate download ===

def download_congress(api_session, bulk_session, congress, state):
    """Download all bills for a congress."""
    congress_dir = OUTPUT_DIR / f"congress_{congress}"
    congress_dir.mkdir(parents=True, exist_ok=True)

    congress_key = str(congress)
    if congress_key in state.get("completed_congresses", []):
        log.info(f"Congress {congress} already completed, skipping")
        return 0

    if congress_key not in state["completed_bills"]:
        state["completed_bills"][congress_key] = []

    total_relevant = 0
    total_checked = 0
    _congress_start = time.time()

    for bill_type in BILL_TYPES:
        log.info(f"Processing {bill_type.upper()} bills for Congress {congress}...")

        # Phase 1: Get bill listing from Congress.gov API (or use cached)
        list_file = congress_dir / f"{bill_type}_list.json"
        if list_file.exists():
            with open(list_file) as f:
                bills = json.load(f)
            log.info(f"  Loaded cached list: {len(bills)} {bill_type.upper()} bills")
        else:
            bills = download_bills_list(api_session, congress, bill_type)
            if bills:
                with open(list_file, "w") as f:
                    json.dump(bills, f, indent=2)
            log.info(f"  Listed {len(bills)} {bill_type.upper()} bills from API")

        if not bills:
            continue

        # Phase 2: Download bill details
        # Filter to bills not yet completed
        completed = set(state["completed_bills"][congress_key])
        pending = []
        for bill in bills:
            number = bill.get("number")
            bill_id = f"{congress}-{bill_type}-{number}"
            if bill_id not in completed:
                pending.append((bill_type, number, bill_id))

        if not pending:
            log.info(f"  All {len(bills)} {bill_type.upper()} bills already processed")
            continue

        use_api = congress < MIN_BILLSTATUS_CONGRESS
        method = "Congress.gov API" if use_api else "BILLSTATUS XML"
        log.info(f"  Downloading {len(pending)} bills via {method} ({len(bills) - len(pending)} cached)...")

        type_relevant = 0
        batch_completed = []

        if use_api:
            # Sequential API downloads (rate-limited)
            for i, (bt, num, bid) in enumerate(pending):
                try:
                    parsed = download_bill_via_api(api_session, congress, bt, num)
                except Exception as e:
                    log.error(f"  Error processing {bid}: {e}")
                    batch_completed.append(bid)
                    total_checked += 1
                    continue

                if parsed:
                    detail_file = congress_dir / f"{bt}_{num}.json"
                    with open(detail_file, "w") as f:
                        json.dump(parsed, f, indent=2)
                    type_relevant += 1
                    total_relevant += 1

                batch_completed.append(bid)
                total_checked += 1

                if (i + 1) % 100 == 0:
                    state["completed_bills"][congress_key].extend(batch_completed)
                    batch_completed = []
                    save_state(state)
                    rate = (i + 1) / max(1, (time.time() - _congress_start) / 3600)
                    remaining = len(pending) - (i + 1)
                    eta_hr = remaining / max(1, rate)
                    log.info(f"  Progress: {i + 1}/{len(pending)}, {type_relevant} saved | ~{rate:.0f}/hr, ~{eta_hr:.1f}hr remaining")
        else:
            # Parallel GovInfo bulk XML downloads
            with ThreadPoolExecutor(max_workers=BULK_WORKERS) as pool:
                futures = {}
                for bt, num, bid in pending:
                    f = pool.submit(download_single_billstatus, bulk_session, congress, bt, num)
                    futures[f] = (bt, num, bid)

                for i, future in enumerate(as_completed(futures)):
                    bt, num, bid = futures[future]
                    try:
                        parsed = future.result()
                    except Exception as e:
                        log.error(f"  Error processing {bid}: {e}")
                        batch_completed.append(bid)
                        continue

                    if parsed:
                        detail_file = congress_dir / f"{bt}_{num}.json"
                        with open(detail_file, "w") as f:
                            json.dump(parsed, f, indent=2)
                        type_relevant += 1
                        total_relevant += 1

                    batch_completed.append(bid)
                    total_checked += 1

                    if (i + 1) % 500 == 0:
                        state["completed_bills"][congress_key].extend(batch_completed)
                        batch_completed = []
                        save_state(state)
                        log.info(f"  Progress: {i + 1}/{len(pending)} checked, {type_relevant} saved")

        # Save remaining batch
        state["completed_bills"][congress_key].extend(batch_completed)
        save_state(state)
        log.info(f"  {bill_type.upper()}: {type_relevant} saved out of {len(bills)} total")

    # Mark congress as completed
    if congress_key not in state["completed_congresses"]:
        state["completed_congresses"].append(congress_key)
    save_state(state)

    log.info(f"Congress {congress} complete: {total_relevant} bills saved from {total_checked} checked")
    return total_relevant


def main():
    parser = argparse.ArgumentParser(description="Download Congress.gov legislation data")
    parser.add_argument("--congress", type=int, nargs="+",
                        help="Congress numbers to download (default: current + previous)")
    parser.add_argument("--full", action="store_true",
                        help="Download all available congresses (93-current)")
    parser.add_argument("--reset", action="store_true",
                        help="Reset download state and start fresh")
    args = parser.parse_args()

    api_key = load_config()
    api_session = create_api_session(api_key)
    bulk_session = create_bulk_session()

    if args.reset:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        log.info("State reset")

    state = load_state()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.full:
        congresses = list(range(93, CURRENT_CONGRESS + 1))
    elif args.congress:
        congresses = args.congress
    else:
        congresses = [CURRENT_CONGRESS - 1, CURRENT_CONGRESS]

    log.info(f"{'=' * 60}")
    log.info(f"Congress.gov Bill Download (BILLSTATUS bulk XML)")
    log.info(f"Congresses: {congresses}")
    log.info(f"Policy areas: all (no filter)")
    log.info(f"Bill types: {', '.join(t.upper() for t in BILL_TYPES)}")
    log.info(f"Parallel workers: {BULK_WORKERS}")
    log.info(f"{'=' * 60}")

    # Test API connection
    test = api_get(api_session, f"{API_BASE}/bill/{CURRENT_CONGRESS}")
    if not test:
        log.error("Failed to connect to Congress.gov API. Check your API key.")
        sys.exit(1)
    log.info("API connection verified")

    # Test GovInfo connection
    test_url = f"{GOVINFO_BASE}/{CURRENT_CONGRESS}/hr/BILLSTATUS-{CURRENT_CONGRESS}hr1.xml"
    try:
        test_resp = bulk_session.get(test_url, timeout=15)
        if test_resp.status_code == 200:
            log.info("GovInfo BILLSTATUS connection verified")
        else:
            log.warning(f"GovInfo returned {test_resp.status_code} — falling back to API-only mode")
    except Exception as e:
        log.warning(f"GovInfo connection failed: {e}")

    grand_total = 0
    start_time = time.time()

    for congress in congresses:
        log.info(f"\n--- Congress {congress} ---")
        count = download_congress(api_session, bulk_session, congress, state)
        grand_total += count

    elapsed = time.time() - start_time
    log.info(f"\n{'=' * 60}")
    log.info(f"Download complete")
    log.info(f"  Total bills saved: {grand_total}")
    log.info(f"  Elapsed: {elapsed / 60:.1f} minutes")
    log.info(f"  Output: {OUTPUT_DIR}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
