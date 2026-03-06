#!/usr/bin/env python3
"""
Phase 12: Download congressional member data and stock trading disclosures.

Downloads:
1. Congress member lookup data (unitedstates/congress-legislators GitHub)
   - All current and historical members with bioguide_id, party, state
   - Master ID mapping (bioguide -> opensecrets, govtrack, fec, etc.)

2. Senate stock trading disclosures (senate-stock-watcher-data GitHub)
   - All periodic transaction reports (PTRs) as structured JSON
   - Includes ticker, transaction type, amount range, date, owner

3. House financial disclosure index (House Clerk annual ZIPs)
   - XML index of all filings with member name, state, DocID
   - PTR PDF links for individual transaction details

No API keys required — all data is publicly available.

Usage:
    python3 12_congress_stock_trades.py             # download all
    python3 12_congress_stock_trades.py --members    # just member data
    python3 12_congress_stock_trades.py --senate     # just senate trades
    python3 12_congress_stock_trades.py --house      # just house filings
"""

import argparse
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
import io
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "congress_members"
STOCK_DIR = PROJECT_DIR / "stock_trades"
LOG_DIR = PROJECT_DIR / "logs"

# Data sources
LEGISLATORS_CURRENT = "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-current.json"
LEGISLATORS_HISTORICAL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-historical.json"
SENATE_STOCK_DATA = "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data/master/aggregate/all_transactions.json"
HOUSE_FD_ZIP_BASE = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs"

# House FD years to download
HOUSE_FD_YEARS = list(range(2008, 2027))  # STOCK Act was 2012, but data goes back further

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("congress_stocks")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "congress_stocks.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


def create_session():
    """Create requests session with retry logic."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (congressional data project)"})
    return session


# ── Congress Members ─────────────────────────────────────────────────────────

def download_members(session):
    """Download congress-legislators data and build unified member lookup."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Downloading congress-legislators data...")
    all_members = []

    for url, label in [(LEGISLATORS_CURRENT, "current"), (LEGISLATORS_HISTORICAL, "historical")]:
        log.info(f"  Fetching {label} legislators...")
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        legislators = resp.json()
        log.info(f"  {label}: {len(legislators)} members")

        # Save raw
        raw_file = OUTPUT_DIR / f"legislators-{label}.json"
        with open(raw_file, "w") as f:
            json.dump(legislators, f)

        for leg in legislators:
            bio_id = leg.get("id", {}).get("bioguide", "")
            if not bio_id:
                continue

            # Get name info
            name = leg.get("name", {})
            first = name.get("first", "")
            last = name.get("last", "")
            official_full = name.get("official_full", "")
            nickname = name.get("nickname", "")

            # Get ID mappings
            ids = leg.get("id", {})

            # Get most recent term info
            terms = leg.get("terms", [])
            latest_term = terms[-1] if terms else {}
            first_term = terms[0] if terms else {}

            # Build all name variants for matching
            name_variants = set()
            name_variants.add(f"{first} {last}")
            if official_full:
                name_variants.add(official_full)
            if nickname:
                name_variants.add(f"{nickname} {last}")
            # Add middle name variant
            middle = name.get("middle", "")
            if middle:
                name_variants.add(f"{first} {middle} {last}")
                name_variants.add(f"{first} {middle[0]}. {last}")
            # Add suffix
            suffix = name.get("suffix", "")
            if suffix:
                name_variants.add(f"{first} {last} {suffix}")
                name_variants.add(f"{first} {last}, {suffix}")

            member = {
                "bioguide_id": bio_id,
                "first_name": first,
                "last_name": last,
                "full_name": official_full or f"{first} {last}",
                "nickname": nickname,
                "party": latest_term.get("party", ""),
                "state": latest_term.get("state", ""),
                "chamber": "Senate" if latest_term.get("type") == "sen" else "House",
                "district": latest_term.get("district"),
                "first_served": first_term.get("start", ""),
                "last_served": latest_term.get("end", ""),
                "is_current": label == "current",
                # ID mappings
                "opensecrets_id": ids.get("opensecrets", ""),
                "fec_ids": json.dumps(ids.get("fec", [])),
                "govtrack_id": ids.get("govtrack"),
                "thomas_id": ids.get("thomas", ""),
                "votesmart_id": ids.get("votesmart"),
                "wikipedia_id": ids.get("wikipedia", ""),
                "ballotpedia_id": ids.get("ballotpedia", ""),
                # Name variants for fuzzy matching
                "name_variants": json.dumps(sorted(name_variants)),
                # Birthday for disambiguation
                "birthday": leg.get("bio", {}).get("birthday", ""),
                "gender": leg.get("bio", {}).get("gender", ""),
            }
            all_members.append(member)

    # Deduplicate by bioguide_id (current takes priority)
    seen = {}
    for m in all_members:
        bid = m["bioguide_id"]
        if bid not in seen or m["is_current"]:
            seen[bid] = m
    members = list(seen.values())

    # Save processed
    output_file = OUTPUT_DIR / "members_lookup.json"
    with open(output_file, "w") as f:
        json.dump(members, f)

    current_count = sum(1 for m in members if m["is_current"])
    log.info(f"  Total unique members: {len(members)} ({current_count} current)")
    log.info(f"  Saved: {output_file}")

    return members


# ── Senate Stock Trades ──────────────────────────────────────────────────────

def download_senate_trades(session):
    """Download Senate stock trading data from senate-stock-watcher-data."""
    STOCK_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Downloading Senate stock trading disclosures...")
    resp = session.get(SENATE_STOCK_DATA, timeout=120)
    resp.raise_for_status()
    raw_data = resp.json()

    # Save raw
    raw_file = STOCK_DIR / "senate_raw.json"
    with open(raw_file, "w") as f:
        json.dump(raw_data, f)

    log.info(f"  Raw transactions: {len(raw_data)}")

    # Clean up ticker field (some have HTML links like <a href="...">TICKER</a>)
    ticker_html_re = re.compile(r'<a[^>]*>([^<]+)</a>')

    trades = []
    for tx in raw_data:
        # Clean ticker: strip HTML if present
        ticker = tx.get("ticker", "") or ""
        html_match = ticker_html_re.search(ticker)
        if html_match:
            ticker = html_match.group(1).strip()
        ticker = ticker.strip()
        if ticker == "--":
            ticker = ""

        # Parse senator name
        senator_name = tx.get("senator", "").strip()

        trade = {
            "member_name": senator_name,
            "chamber": "Senate",
            "transaction_date": tx.get("transaction_date", ""),
            "ticker": ticker,
            "asset_description": tx.get("asset_description", ""),
            "asset_type": tx.get("asset_type", ""),
            "transaction_type": tx.get("type", ""),
            "amount_range": tx.get("amount", ""),
            "owner": tx.get("owner", ""),
            "comment": tx.get("comment", ""),
            "source_url": tx.get("ptr_link", ""),
        }
        trades.append(trade)

    # Save processed
    output_file = STOCK_DIR / "senate_trades.json"
    with open(output_file, "w") as f:
        json.dump(trades, f)

    # Stats
    unique_senators = len(set(t["member_name"] for t in trades))
    unique_tickers = len(set(t["ticker"] for t in trades if t["ticker"]))
    log.info(f"  Senate trades: {len(trades):,} transactions")
    log.info(f"  Unique senators: {unique_senators}")
    log.info(f"  Unique tickers: {unique_tickers}")
    dated = [t for t in trades if t["transaction_date"]]
    if dated:
        log.info(f"  Date range: {min(t['transaction_date'] for t in dated)} to "
                 f"{max(t['transaction_date'] for t in dated)}")
    log.info(f"  Saved: {output_file}")

    return trades


# ── House Financial Disclosures ──────────────────────────────────────────────

def download_house_filings(session):
    """Download House financial disclosure XML indexes."""
    STOCK_DIR.mkdir(parents=True, exist_ok=True)
    house_dir = STOCK_DIR / "house"
    house_dir.mkdir(exist_ok=True)

    log.info("Downloading House financial disclosure indexes...")
    total_filings = 0

    for year in HOUSE_FD_YEARS:
        # Try FD (annual) and PTR (periodic transaction) files
        for suffix in ["FD", "PTR"]:
            url = f"{HOUSE_FD_ZIP_BASE}/{year}{suffix}.zip"
            try:
                resp = session.get(url, timeout=60)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
            except requests.exceptions.RequestException:
                continue

            try:
                zf = zipfile.ZipFile(io.BytesIO(resp.content))
            except zipfile.BadZipFile:
                log.warning(f"  Bad ZIP for {year}{suffix}")
                continue

            # Find the XML file
            xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
            if not xml_files:
                continue

            xml_data = zf.read(xml_files[0]).decode("utf-8", errors="replace")

            # Parse XML
            try:
                root = ET.fromstring(xml_data)
            except ET.ParseError:
                log.warning(f"  Parse error for {year}{suffix}")
                continue

            filings = []
            for member in root.findall(".//Member"):
                filing = {
                    "first_name": (member.findtext("First") or "").strip(),
                    "last_name": (member.findtext("Last") or "").strip(),
                    "prefix": (member.findtext("Prefix") or "").strip(),
                    "suffix": (member.findtext("Suffix") or "").strip(),
                    "filing_type": (member.findtext("FilingType") or "").strip(),
                    "state_district": (member.findtext("StateDst") or "").strip(),
                    "year": year,
                    "filing_date": (member.findtext("FilingDate") or "").strip(),
                    "doc_id": (member.findtext("DocID") or "").strip(),
                }
                # Build PDF URL for PTR filings
                if filing["doc_id"]:
                    filing["pdf_url"] = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{filing['doc_id']}.pdf"
                    filing["member_name"] = f"{filing['first_name']} {filing['last_name']}".strip()
                    filing["chamber"] = "House"
                filings.append(filing)

            if filings:
                out_file = house_dir / f"{year}{suffix}.json"
                with open(out_file, "w") as f:
                    json.dump(filings, f)
                total_filings += len(filings)
                log.info(f"  {year}{suffix}: {len(filings)} filings")

    log.info(f"  Total House filings indexed: {total_filings:,}")
    return total_filings


def main():
    parser = argparse.ArgumentParser(description="Download congressional member and stock trading data")
    parser.add_argument("--members", action="store_true", help="Download member data only")
    parser.add_argument("--senate", action="store_true", help="Download Senate stock trades only")
    parser.add_argument("--house", action="store_true", help="Download House FD indexes only")
    args = parser.parse_args()

    # If no flags, do everything
    do_all = not (args.members or args.senate or args.house)

    session = create_session()
    start_time = time.time()

    log.info("=" * 60)
    log.info("Congressional Member & Stock Trading Data Download")
    log.info("=" * 60)

    if do_all or args.members:
        members = download_members(session)

    if do_all or args.senate:
        trades = download_senate_trades(session)

    if do_all or args.house:
        house_count = download_house_filings(session)

    elapsed = time.time() - start_time
    log.info(f"\n{'=' * 60}")
    log.info(f"Download complete in {elapsed:.0f} seconds")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
