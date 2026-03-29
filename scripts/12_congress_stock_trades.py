#!/usr/bin/env python3
"""
Phase 12: Download congressional stock trade disclosures from all sources.

Consolidates the former three-script approach (12 + 13 + 14) into a single
script with subcommands. Downloads member data, Senate eFD official filings,
House PTR PDFs, and legacy third-party Senate data.

Data sources:
  - Congress members: unitedstates/congress-legislators GitHub (no auth)
  - Senate trades: efdsearch.senate.gov (CSRF auth, official, preferred)
  - Senate legacy: senate-stock-watcher-data GitHub (no auth, third-party backup)
  - House indexes: disclosures-clerk.house.gov XML (no auth)
  - House PTR PDFs: disclosures-clerk.house.gov PDFs (no auth, pdftotext required)

Output:
  congress_members/members_lookup.json
  stock_trades/senate_efd/all_transactions.json
  stock_trades/senate_trades.json (legacy)
  stock_trades/house/{year}{type}.json
  stock_trades/house_ptrs/all_transactions.json

Usage:
    python3 12_congress_stock_trades.py                # full run (all phases)
    python3 12_congress_stock_trades.py --members      # just member data
    python3 12_congress_stock_trades.py --senate-efd   # official Senate eFD trades
    python3 12_congress_stock_trades.py --house-ptr    # House PTR PDF parsing
    python3 12_congress_stock_trades.py --senate       # legacy third-party Senate aggregate
    python3 12_congress_stock_trades.py --house-index  # House FD/PTR XML indexes
"""

import argparse
import io
import json
import logging
import re
import signal
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# === Configuration ===
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "congress_members"
STOCK_DIR = PROJECT_DIR / "stock_trades"
LOG_DIR = PROJECT_DIR / "logs"

# Data sources — members
LEGISLATORS_CURRENT = "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-current.json"
LEGISLATORS_HISTORICAL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-historical.json"

# Data sources — Senate legacy (third-party)
SENATE_STOCK_DATA = "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data/master/aggregate/all_transactions.json"

# Data sources — House indexes
HOUSE_FD_ZIP_BASE = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs"
HOUSE_FD_YEARS = list(range(2008, 2027))  # STOCK Act was 2012, but data goes back further

# Data sources — Senate eFD (official)
EFD_ROOT = "https://efdsearch.senate.gov"
EFD_LANDING_URL = f"{EFD_ROOT}/search/home/"
EFD_SEARCH_URL = f"{EFD_ROOT}/search/"
EFD_REPORTS_URL = f"{EFD_ROOT}/search/report/data/"
EFD_BATCH_SIZE = 100
EFD_REPORT_TYPE_PTR = "[11]"  # Report type 11 = Periodic Transaction Report

# Data sources — House PTR PDFs
PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

# Rate limiting
RATE_LIMIT = 1.0  # seconds between requests for eFD and House PDFs
CHECKPOINT_INTERVAL = 50

# House PTR amount range normalization
AMOUNT_RANGES = [
    "$1 - $1,000",
    "$1,001 - $15,000",
    "$15,001 - $50,000",
    "$50,001 - $100,000",
    "$100,001 - $250,000",
    "$250,001 - $500,000",
    "$500,001 - $1,000,000",
    "$1,000,001 - $5,000,000",
    "$5,000,001 - $25,000,000",
    "$25,000,001 - $50,000,000",
    "$50,000,001+",
]

# House PTR owner codes
OWNER_CODES = {"SP": "Spouse", "JT": "Joint", "DC": "Dependent Child"}


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


# === Signal handling ===
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received, finishing current item...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# === Shared session ===

def create_session():
    """Create requests session with retry logic."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (congressional data project)"})
    return session


# ═══════════════════════════════════════════════════════════════════════════════
# Phase: Members
# ═══════════════════════════════════════════════════════════════════════════════

def phase_members(session):
    """Download congress-legislators data and build unified member lookup."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("─" * 50)
    log.info("PHASE: Congress Members")
    log.info("─" * 50)
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


# ═══════════════════════════════════════════════════════════════════════════════
# Phase: Senate eFD (official source, preferred)
# ═══════════════════════════════════════════════════════════════════════════════

class EFDSession:
    """Manages authenticated session with efdsearch.senate.gov."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OpenRegs/1.0 (civic data project; https://regs.datadawn.org)"
        })
        self.csrf_token = None
        self._authenticate()

    def _authenticate(self):
        """Accept the usage agreement and get CSRF token."""
        # Step 1: Get landing page
        r = self.session.get(EFD_LANDING_URL)
        r.raise_for_status()

        # Step 2: Extract form CSRF token
        m = re.search(r'csrfmiddlewaretoken" value="([^"]+)', r.text)
        if not m:
            raise RuntimeError("Could not find CSRF token on landing page")
        form_csrf = m.group(1)

        # Step 3: Accept agreement
        self.session.post(
            EFD_LANDING_URL,
            data={
                "csrfmiddlewaretoken": form_csrf,
                "prohibition_agreement": "1",
            },
            headers={"Referer": EFD_LANDING_URL},
        )

        # Step 4: Get cookie-based CSRF token for API calls
        self.csrf_token = self.session.cookies.get(
            "csrftoken", self.session.cookies.get("csrf")
        )
        if not self.csrf_token:
            raise RuntimeError("Could not get CSRF cookie after agreement")

        log.info("Authenticated with efdsearch.senate.gov")

    def search_reports(self, start=0, length=EFD_BATCH_SIZE, start_date="01/01/2012 00:00:00"):
        """Search for PTR filings. Returns (total_count, list_of_reports)."""
        r = self.session.post(
            EFD_REPORTS_URL,
            data={
                "start": str(start),
                "length": str(length),
                "report_types": EFD_REPORT_TYPE_PTR,
                "filer_types": "[]",
                "submitted_start_date": start_date,
                "submitted_end_date": "",
                "candidate_state": "",
                "senator_state": "",
                "office_id": "",
                "first_name": "",
                "last_name": "",
                "csrfmiddlewaretoken": self.csrf_token,
            },
            headers={"Referer": EFD_SEARCH_URL},
        )

        if r.status_code == 403:
            log.warning("Session expired, re-authenticating...")
            self._authenticate()
            return self.search_reports(start, length, start_date)

        r.raise_for_status()
        data = r.json()
        return data["recordsTotal"], data["data"]

    def fetch_report(self, report_url):
        """Fetch and parse a single PTR report page. Returns list of transactions."""
        full_url = f"{EFD_ROOT}{report_url}" if report_url.startswith("/") else report_url

        r = self.session.get(full_url)
        if r.status_code == 403 or r.url == EFD_LANDING_URL:
            log.warning("Session expired during report fetch, re-authenticating...")
            self._authenticate()
            r = self.session.get(full_url)

        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return []

        transactions = []
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]

        for row in table.find_all("tr")[1:]:  # Skip header row
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 7:
                continue

            tx = {}
            for i, h in enumerate(headers):
                if i < len(cells):
                    val = cells[i]
                    if val == "--":
                        val = ""
                    tx[h] = val

            transactions.append({
                "transaction_date": tx.get("transaction date", ""),
                "owner": tx.get("owner", ""),
                "ticker": tx.get("ticker", ""),
                "asset_name": tx.get("asset name", ""),
                "asset_type": tx.get("asset type", ""),
                "transaction_type": tx.get("type", ""),
                "amount": tx.get("amount", ""),
                "comment": tx.get("comment", ""),
            })

        return transactions


def _parse_efd_report_listing(row):
    """Parse a single row from the Senate eFD search results.

    Row format: [first_name, last_name, filer_name, report_link_html, date_filed]
    """
    first_name = row[0]
    last_name = row[1]
    filer_name = row[2]  # e.g., "Hagerty, Bill (Senator)"
    link_html = row[3]
    date_filed = row[4]

    # Extract URL from the <a> tag
    m = re.search(r'href="([^"]+)"', link_html)
    report_url = m.group(1) if m else ""

    # Extract report title
    m2 = re.search(r'>([^<]+)<', link_html)
    report_title = m2.group(1) if m2 else ""

    # Extract UUID from URL
    uuid_match = re.search(r'ptr/([0-9a-f-]+)/', report_url)
    report_id = uuid_match.group(1) if uuid_match else ""

    return {
        "report_id": report_id,
        "first_name": first_name,
        "last_name": last_name,
        "filer_name": filer_name,
        "report_url": report_url,
        "report_title": report_title,
        "date_filed": date_filed,
    }


def _load_efd_state():
    """Load Senate eFD download state (completed report IDs)."""
    state_file = STOCK_DIR / "senate_efd" / "state.json"
    if state_file.exists():
        with open(state_file) as f:
            return json.load(f)
    return {"completed_reports": [], "transactions": []}


def _save_efd_state(state):
    """Save Senate eFD download state."""
    state_file = STOCK_DIR / "senate_efd" / "state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w") as f:
        json.dump(state, f)


def phase_senate_efd():
    """Download Senate eFD Periodic Transaction Reports (official source).

    Authenticates via CSRF token, fetches report listings in batches,
    then downloads individual report pages and extracts transaction rows.
    Rate limited to 1s between requests. Auto-reauthenticates on 403.
    """
    efd_dir = STOCK_DIR / "senate_efd"
    efd_dir.mkdir(parents=True, exist_ok=True)

    log.info("─" * 50)
    log.info("PHASE: Senate eFD (official source)")
    log.info("─" * 50)

    efd = EFDSession()

    # Step 1: Get all PTR report listings
    log.info("Fetching PTR report listings...")
    total, first_batch = efd.search_reports(start=0)
    log.info(f"  Total PTR filings: {total}")

    all_reports = []
    for row in first_batch:
        all_reports.append(_parse_efd_report_listing(row))

    offset = EFD_BATCH_SIZE
    while offset < total:
        if _shutdown:
            log.warning("Shutdown during listing fetch, saving progress...")
            break
        time.sleep(RATE_LIMIT)
        _, batch = efd.search_reports(start=offset)
        if not batch:
            break
        for row in batch:
            all_reports.append(_parse_efd_report_listing(row))
        offset += EFD_BATCH_SIZE
        if offset % 500 == 0:
            log.info(f"  Fetched {offset}/{total} listings...")

    log.info(f"  Got {len(all_reports)} report listings")

    # Save report index
    index_file = efd_dir / "report_index.json"
    with open(index_file, "w") as f:
        json.dump(all_reports, f, indent=2)

    # Step 2: Load state and download individual reports
    state = _load_efd_state()
    completed = set(state["completed_reports"])
    all_transactions = state.get("transactions", [])

    pending = [r for r in all_reports if r["report_id"] and r["report_id"] not in completed]
    log.info(f"Downloading {len(pending)} reports ({len(completed)} already done)...")

    errors = 0
    for i, report in enumerate(pending):
        if _shutdown:
            log.warning("Shutdown signal, saving state...")
            break

        try:
            time.sleep(RATE_LIMIT)
            txs = efd.fetch_report(report["report_url"])

            # Enrich transactions with senator info
            for tx in txs:
                tx["senator_first_name"] = report["first_name"]
                tx["senator_last_name"] = report["last_name"]
                tx["senator_filer_name"] = report["filer_name"]
                tx["report_id"] = report["report_id"]
                tx["date_filed"] = report["date_filed"]
                tx["source_url"] = f"{EFD_ROOT}{report['report_url']}"

            all_transactions.extend(txs)
            completed.add(report["report_id"])

            if (i + 1) % 50 == 0:
                log.info(f"  {i + 1}/{len(pending)} reports downloaded "
                         f"({len(all_transactions)} transactions so far)")
                # Save checkpoint
                state["completed_reports"] = list(completed)
                state["transactions"] = all_transactions
                _save_efd_state(state)

        except Exception as e:
            errors += 1
            log.warning(f"  Error on {report['report_id']}: {e}")
            if errors > 20:
                log.error("Too many errors, stopping")
                break

    # Final save
    state["completed_reports"] = list(completed)
    state["transactions"] = all_transactions
    _save_efd_state(state)

    # Also save transactions as a clean JSON file
    output_file = efd_dir / "all_transactions.json"
    with open(output_file, "w") as f:
        json.dump(all_transactions, f, indent=2)

    log.info(f"  Done: {len(completed)} reports, {len(all_transactions)} transactions")
    log.info(f"  Errors: {errors}")
    log.info(f"  Output: {output_file}")

    # Summary stats
    senators = set()
    for tx in all_transactions:
        name = f"{tx.get('senator_first_name', '')} {tx.get('senator_last_name', '')}".strip()
        if name:
            senators.add(name)
    log.info(f"  Unique senators: {len(senators)}")

    # Date range
    dates = [tx["transaction_date"] for tx in all_transactions if tx.get("transaction_date")]
    if dates:
        log.info(f"  Date range: {min(dates)} to {max(dates)}")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase: House FD/PTR XML Indexes
# ═══════════════════════════════════════════════════════════════════════════════

def phase_house_index(session):
    """Download House financial disclosure XML indexes (annual ZIPs)."""
    STOCK_DIR.mkdir(parents=True, exist_ok=True)
    house_dir = STOCK_DIR / "house"
    house_dir.mkdir(exist_ok=True)

    log.info("─" * 50)
    log.info("PHASE: House FD/PTR XML Indexes")
    log.info("─" * 50)
    log.info("Downloading House financial disclosure indexes...")
    total_filings = 0

    for year in HOUSE_FD_YEARS:
        if _shutdown:
            log.warning("Shutdown signal, stopping index download...")
            break

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


# ═══════════════════════════════════════════════════════════════════════════════
# Phase: House PTR PDF Parsing
# ═══════════════════════════════════════════════════════════════════════════════

def _get_all_ptrs():
    """Load all PTR filing records from the House FD index files."""
    house_dir = STOCK_DIR / "house"
    ptrs = []
    for fd_file in sorted(house_dir.glob("*.json")):
        try:
            with open(fd_file) as f:
                filings = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        year = fd_file.stem.replace("FD", "")
        for filing in filings:
            if filing.get("filing_type") != "P":
                continue
            doc_id = str(filing.get("doc_id", ""))
            if not doc_id:
                continue
            ptrs.append({
                "doc_id": doc_id,
                "year": int(year) if year.isdigit() else 0,
                "first_name": filing.get("first_name", ""),
                "last_name": filing.get("last_name", ""),
                "member_name": filing.get("member_name", ""),
                "state_district": filing.get("state_district", ""),
                "filing_date": filing.get("filing_date", ""),
            })
    return ptrs


def _download_pdf(doc_id, year, pdf_dir):
    """Download a PTR PDF. Returns path or None."""
    pdf_path = pdf_dir / f"{doc_id}.pdf"
    if pdf_path.exists() and pdf_path.stat().st_size > 2000:
        return pdf_path

    url = PTR_PDF_URL.format(year=year, doc_id=doc_id)
    try:
        urllib.request.urlretrieve(url, str(pdf_path))
        if pdf_path.stat().st_size < 2000:
            pdf_path.unlink(missing_ok=True)
            return None
        return pdf_path
    except Exception:
        return None


def _is_scanned_pdf(pdf_path):
    """Check if a PDF is scanned (image-only, no extractable text)."""
    result = subprocess.run(
        ["pdftotext", str(pdf_path), "-"],
        capture_output=True, text=True, timeout=30,
    )
    # Scanned PDFs produce empty or near-empty text
    return len(result.stdout.strip()) < 50


def _extract_text(pdf_path):
    """Extract text from PDF using pdftotext -layout."""
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True, text=True, timeout=30,
    )
    return result.stdout


def _normalize_amount(raw):
    """Normalize dollar amount to standard range format."""
    raw = raw.strip()
    if not raw:
        return ""
    # Remove extra spaces
    raw = re.sub(r"\s+", " ", raw)
    # Handle "Over $X" format
    if raw.lower().startswith("over "):
        return raw
    # Extract the two dollar values
    vals = re.findall(r"\$[\d,]+", raw)
    if not vals:
        return raw
    if len(vals) == 1 and raw.endswith("+"):
        return vals[0] + "+"
    if len(vals) >= 2:
        return f"{vals[0]} - {vals[1]}"
    # Single value — try to find matching standard range by lower bound
    lower = vals[0]
    for std in AMOUNT_RANGES:
        if std.startswith(lower + " "):
            return std
    return raw


def _clean_text(s):
    """Remove null bytes and other PDF artifacts from text."""
    # Replace null bytes and control chars (except newline/tab)
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


def _is_metadata_line(s):
    """Check if a line is filing metadata (Filing Status, Subholding, Description)."""
    cleaned = _clean_text(s)
    # Filing Status: New / Amended
    if re.search(r"(?:FILING|Filing|FILINg|FIlINg)\s*(?:STATUS|STATuS|Status|sTaTus)", cleaned, re.IGNORECASE):
        return True
    # Subholding Of: ...
    if re.search(r"(?:SUBHOLDING|SUbHoLDINg|Subholding)\s*(?:OF|oF|Of)", cleaned, re.IGNORECASE):
        return True
    # Description: ... (CUSIP)
    if re.search(r"(?:DESCRIPTION|Description|D\w+tion)\s*:", cleaned, re.IGNORECASE):
        return True
    # Also match the garbled null-byte versions: "F S : New", "S O : ..."
    # These have null bytes between letters in the original
    if re.match(r"^\s*F\s+S\s+:\s*\w", cleaned):
        return True
    if re.match(r"^\s*S\s+O\s*:\s", cleaned):
        return True
    if re.match(r"^\s*D\s+:\s", cleaned):
        return True
    return False


def _extract_subholding(s):
    """Extract subholding value from a metadata line."""
    cleaned = _clean_text(s)
    m = re.search(r"(?:SUBHOLDING|SUbHoLDINg|Subholding)\s*(?:OF|oF|Of)\s*:\s*(.+)", cleaned, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"S\s+O\s*:\s*(.+)", cleaned)
    if m:
        return m.group(1).strip()
    return ""


def _extract_description(s):
    """Extract description/CUSIP from a metadata line."""
    cleaned = _clean_text(s)
    m = re.search(r"(?:DESCRIPTION|Description|D\w+tion)\s*:\s*(.+)", cleaned, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"D\s+:\s*(.+)", cleaned)
    if m:
        return m.group(1).strip()
    return ""


def _find_tx_type(line, dates, tx_types):
    """Find transaction type before the given date pair. Returns (tx_type, position)."""
    first_date_pos = line.find(dates[0])
    pre_date = line[:first_date_pos]

    for tt in tx_types:
        pattern = re.escape(tt)
        m = re.search(r"(?:^|\s)" + pattern + r"\s*$", pre_date)
        if m:
            return tt, m.start()

    # Try case-insensitive for older PDFs with rendering quirks
    for tt in tx_types:
        pattern = re.escape(tt)
        m = re.search(r"(?:^|\s)" + pattern + r"\s*$", pre_date, re.IGNORECASE)
        if m:
            return tt[0].upper() + tt[1:], m.start()

    return "", -1


def _parse_transactions(text):
    """Parse pdftotext -layout output to extract transactions.

    Strategy: Each transaction starts on a line containing a transaction type
    (P, S, E, or their partial variants) followed by two dates (MM/DD/YYYY).
    The asset description is to the left of the transaction type.
    The amount is to the right of the dates, possibly continuing on the next line.
    """
    if not text or len(text.strip()) < 50:
        return None  # Scanned PDF

    # Clean null bytes throughout
    text = _clean_text(text)
    lines = text.split("\n")
    transactions = []

    # Extract filer info
    filer_name = ""
    state_district = ""
    for line in lines[:25]:
        m = re.search(r"[Nn]ame:\s+(?:Hon\.\s+)?(.+?)$", line)
        if m:
            filer_name = m.group(1).strip()
        m = re.search(r"State/District:\s*(\S+)", line, re.IGNORECASE)
        if m:
            state_district = m.group(1).strip()

    date_re = r"(\d{1,2}/\d{1,2}/\d{4})"
    # Transaction type patterns — check longer patterns first
    tx_types = ["S (partial)", "E (partial)", "P (partial)", "P", "S", "E"]

    i = 0
    while i < len(lines):
        line = lines[i]

        # Skip header/footer/boilerplate lines
        if re.match(
            r"\s*(ID\s+Owner|Filing ID|Clerk of|"
            r"F\s+I$|[Nn]ame:|[Ss]tatus:|State/|"
            r"[PT]$|"
            r"\* For the|A\s+C\s+D|I\s+P\s+O|"
            r"[CI]\s+[SV]\s*$|I CERTIFY|Digitally Signed|"
            r"I\s+V\s+D|"
            r"[Cc]ertification|[Ii]nitial [Pp]ublic|[Aa]sset [Cc]lass|"
            r"Yes\s+No|"
            r"P\s+T\s+R|"  # Header "Periodic Transaction Report"
            r"P\s+erioDic)",
            line, re.IGNORECASE
        ):
            i += 1
            continue

        # Skip metadata lines that appear between transactions
        if _is_metadata_line(line):
            i += 1
            continue

        # Find lines with two dates — these are transaction lines
        all_dates = re.findall(date_re, line)
        if len(all_dates) < 2:
            i += 1
            continue

        # Try first-two dates (works for 99% of cases), then fall back
        # to last-two dates if tx type not found (handles dates in asset names,
        # e.g., PUT options with expiration dates).
        dates = all_dates[:2]
        tx_type, tx_type_pos = _find_tx_type(line, dates, tx_types)

        if not tx_type and len(all_dates) > 2:
            # Fallback: try last two dates (asset description contains a date)
            dates = all_dates[-2:]
            tx_type, tx_type_pos = _find_tx_type(line, dates, tx_types)

        # Asset description is everything before the transaction type
        if tx_type_pos > 0:
            asset_line = line[:tx_type_pos].strip()
        else:
            first_date_pos = line.find(dates[0])
            asset_line = line[:first_date_pos].strip()

        # Extract owner code from the beginning of asset_line
        owner = "Self"
        owner_m = re.match(r"^(SP|JT|DC)\s+", asset_line)
        if owner_m:
            owner = OWNER_CODES.get(owner_m.group(1), owner_m.group(1))
            asset_line = asset_line[owner_m.end():].strip()

        # Amount — everything after the second (notification) date on this line
        second_date_pos = line.find(dates[1], line.find(dates[0]) + len(dates[0]))
        if second_date_pos < 0:
            second_date_pos = line.rfind(dates[1])
        after_dates = line[second_date_pos + len(dates[1]):]
        # Strip checkbox artifacts (c, d, e, f, g) from end of line
        after_dates_clean = re.sub(r"\s+[cdefg]\s*$", "", after_dates)
        # Check for "Over $X" format (e.g., "Spouse/DC Over $1,000,000")
        over_match = re.search(r"[Oo]ver\s+(\$[\d,]+)", after_dates_clean)
        # "Over" may appear without the dollar amount (split to next line)
        amount_is_over = bool(re.search(r"[Oo]ver\s*$", after_dates_clean.strip()))
        if over_match:
            amount = "Over " + over_match.group(1)
        else:
            amount_match = re.search(r"(\$[\d,]+(?:\s*-\s*\$[\d,]+|\+)?)", after_dates_clean)
            amount = amount_match.group(1).strip() if amount_match else ""
        # Check if amount is split across lines: "$X,XXX -" without "$Y,YYY" after it
        amount_incomplete = bool(
            re.search(r"\$[\d,]+\s*-", after_dates_clean)
            and not re.search(r"\$[\d,]+\s*-\s*\$[\d,]+", after_dates_clean)
        )

        # Collect continuation lines for this transaction
        j = i + 1
        asset_continuation = []
        subholding = ""
        description = ""

        while j < len(lines):
            next_line = lines[j]
            stripped = next_line.strip()

            # Empty line = potential end of transaction block
            if not stripped:
                j += 1
                # Skip consecutive blank lines
                while j < len(lines) and not lines[j].strip():
                    j += 1
                break

            # Column header repeated on new page — skip header block
            if re.match(r"\s*ID\s+Owner", stripped, re.IGNORECASE):
                j += 1
                while j < len(lines):
                    s = lines[j].strip()
                    if not s or re.match(r"(Type|Date|\$200|Gains)", s, re.IGNORECASE):
                        j += 1
                    else:
                        break
                continue

            # Metadata lines — extract info but don't add to asset name
            if _is_metadata_line(next_line):
                sub = _extract_subholding(next_line)
                if sub:
                    subholding = sub
                desc = _extract_description(next_line)
                if desc:
                    description = desc
                j += 1
                continue

            # Check if this is a new transaction (has two dates)
            if len(re.findall(date_re, stripped)) >= 2:
                break

            # Checkbox artifacts (single letters from cap gains column)
            if re.match(r"^[cdefg](\s+[cdefg])*\s*$", stripped):
                j += 1
                continue

            # Footer/boilerplate lines
            if re.match(r"\s*(Yes|No|\*\s*For|I CERTIFY|Digitally|L\s*:\s*US|Owner:)", stripped, re.IGNORECASE):
                j += 1
                continue

            # Dollar amount on its own line — amount continuation
            dollar_only = re.match(r"^\s*(\$[\d,]+)\s*$", stripped)
            if dollar_only:
                if amount_incomplete or (amount and amount.rstrip().endswith("-")):
                    # Complete the split amount
                    amount = amount.rstrip(" -") + " - " + dollar_only.group(1)
                    amount_incomplete = False
                elif amount_is_over:
                    amount = "Over " + dollar_only.group(1)
                    amount_is_over = False
                elif not amount:
                    amount = dollar_only.group(1)
                j += 1
                continue

            # Asset continuation with embedded dollar amount (multi-line split)
            # e.g., "[ST]                                                  $100,000"
            if amount_incomplete:
                em = re.search(r"(\$[\d,]+)", stripped)
                if em:
                    amount = amount.rstrip(" -") + " - " + em.group(1)
                    amount_incomplete = False
                    # Rest of the line is asset continuation
                    rest = stripped.replace(em.group(0), "").strip()
                    if rest and not _is_metadata_line(rest):
                        asset_continuation.append(rest)
                    j += 1
                    continue

            # Asset name continuation (e.g., "[ST]" on next line, or bond details)
            asset_continuation.append(stripped)
            j += 1

        # Build full asset name from main line + continuation
        full_asset = asset_line
        for cont in asset_continuation:
            full_asset += " " + cont

        # Clean up asset name
        full_asset = re.sub(r"\s+", " ", full_asset).strip()
        # Remove trailing checkbox artifacts
        full_asset = re.sub(r"\s+[cdefg](\s+[cdefg])*\s*$", "", full_asset)

        # Extract ticker from asset name (case-insensitive for older PDFs)
        ticker = ""
        ticker_m = re.search(r"\(([A-Za-z][A-Za-z0-9.]{0,9})\)", full_asset)
        if ticker_m:
            ticker = ticker_m.group(1).upper()

        # Extract asset type code [ST], [CS], [OP], [OT], [GS], etc.
        asset_type = ""
        atype_m = re.search(r"\[([A-Z]{2})\]", full_asset)
        if atype_m:
            asset_type = atype_m.group(1)

        amount = _normalize_amount(amount)

        tx = {
            "asset_name": full_asset,
            "ticker": ticker,
            "asset_type": asset_type,
            "transaction_type": tx_type,
            "transaction_date": dates[0],
            "notification_date": dates[1],
            "amount": amount,
            "owner": owner,
            "subholding": subholding,
            "description": description,
            "filer_name": filer_name,
            "state_district": state_district,
        }
        transactions.append(tx)

        i = j

    return transactions


def _load_ptr_state():
    """Load House PTR download/parse state."""
    state_file = STOCK_DIR / "house_ptrs" / "state.json"
    if state_file.exists():
        with open(state_file) as f:
            return json.load(f)
    return {"completed": [], "failed": [], "scanned": []}


def _save_ptr_state(state):
    """Save House PTR state to disk."""
    state_file = STOCK_DIR / "house_ptrs" / "state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w") as f:
        json.dump(state, f)


def phase_house_ptr():
    """Download and parse House PTR PDFs to extract transaction data.

    Reads House PTR index files (from phase_house_index), downloads PDFs,
    extracts text via pdftotext -layout, and parses transaction lines.
    Detects and skips scanned PDFs. Rate limited to 1s between downloads.
    """
    pdf_dir = STOCK_DIR / "house_ptrs"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    output_file = pdf_dir / "all_transactions.json"

    log.info("─" * 50)
    log.info("PHASE: House PTR PDF Parsing")
    log.info("─" * 50)

    # Load all PTR filing records
    all_ptrs = _get_all_ptrs()
    log.info(f"Total PTR filings in index: {len(all_ptrs)}")

    if not all_ptrs:
        log.warning("No PTR filings found — run --house-index first")
        return

    # Load state
    state = _load_ptr_state()
    completed = set(state["completed"])
    failed = set(state.get("failed", []))
    scanned = set(state.get("scanned", []))

    # Filter to pending
    pending = [p for p in all_ptrs if p["doc_id"] not in completed
               and p["doc_id"] not in scanned]
    log.info(f"Already completed: {len(completed)}, scanned (skipped): {len(scanned)}, "
             f"pending: {len(pending)}")

    all_transactions = []
    # Load existing transactions from previous runs
    if output_file.exists():
        try:
            with open(output_file) as f:
                all_transactions = json.load(f)
            log.info(f"Loaded {len(all_transactions)} existing transactions")
        except (json.JSONDecodeError, OSError):
            pass

    errors = 0
    new_tx_count = 0
    new_completed = 0
    new_scanned = 0

    for i, ptr in enumerate(pending):
        if _shutdown:
            log.warning("Shutdown signal, saving state...")
            break

        doc_id = ptr["doc_id"]
        year = ptr["year"]

        try:
            # Download
            time.sleep(RATE_LIMIT)
            pdf_path = _download_pdf(doc_id, year, pdf_dir)

            if not pdf_path:
                failed.add(doc_id)
                errors += 1
                if errors > 50:
                    log.error("Too many download errors, stopping")
                    break
                continue

            # Check if scanned
            if _is_scanned_pdf(pdf_path):
                scanned.add(doc_id)
                new_scanned += 1
                continue

            # Extract and parse
            text = _extract_text(pdf_path)
            txs = _parse_transactions(text)

            if txs is None:
                scanned.add(doc_id)
                new_scanned += 1
                continue

            # Enrich with filing metadata
            member_name = ptr["member_name"]
            if not member_name:
                member_name = f"{ptr['first_name']} {ptr['last_name']}".strip()

            for tx in txs:
                tx["member_name"] = member_name
                tx["doc_id"] = doc_id
                tx["filing_date"] = ptr["filing_date"]
                tx["source_url"] = PTR_PDF_URL.format(year=year, doc_id=doc_id)

            all_transactions.extend(txs)
            completed.add(doc_id)
            new_completed += 1
            new_tx_count += len(txs)

            if (new_completed + new_scanned) % CHECKPOINT_INTERVAL == 0:
                log.info(f"  Progress: {new_completed + new_scanned}/{len(pending)} "
                         f"({new_completed} parsed, {new_scanned} scanned) "
                         f"— {new_tx_count} new transactions")
                state["completed"] = list(completed)
                state["failed"] = list(failed)
                state["scanned"] = list(scanned)
                _save_ptr_state(state)
                with open(output_file, "w") as f:
                    json.dump(all_transactions, f)

        except Exception as e:
            errors += 1
            log.warning(f"  Error on {doc_id}: {e}")
            if errors > 50:
                log.error("Too many errors, stopping")
                break

    # Final save
    state["completed"] = list(completed)
    state["failed"] = list(failed)
    state["scanned"] = list(scanned)
    _save_ptr_state(state)

    with open(output_file, "w") as f:
        json.dump(all_transactions, f, indent=2)

    log.info(f"  Done:")
    log.info(f"  Parsed: {len(completed)} PDFs")
    log.info(f"  Scanned (skipped): {len(scanned)} PDFs")
    log.info(f"  Failed: {len(failed)} PDFs")
    log.info(f"  Total transactions: {len(all_transactions)}")
    log.info(f"  Output: {output_file}")

    # Summary stats
    members = set()
    tickers = set()
    for tx in all_transactions:
        if tx.get("member_name"):
            members.add(tx["member_name"])
        if tx.get("ticker"):
            tickers.add(tx["ticker"])

    log.info(f"  Unique members: {len(members)}")
    log.info(f"  Unique tickers: {len(tickers)}")

    dates = [tx["transaction_date"] for tx in all_transactions
             if tx.get("transaction_date")]
    if dates:
        log.info(f"  Date range: {min(dates)} to {max(dates)}")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase: Senate Legacy (third-party backup)
# ═══════════════════════════════════════════════════════════════════════════════

def phase_senate_legacy(session):
    """Download Senate stock trading data from senate-stock-watcher-data (third-party).

    This is a backup/legacy source. The official Senate eFD (phase_senate_efd)
    is the preferred authoritative source for Senate stock trades.
    """
    STOCK_DIR.mkdir(parents=True, exist_ok=True)

    log.info("─" * 50)
    log.info("PHASE: Senate Legacy (third-party backup)")
    log.info("─" * 50)
    log.info("Downloading Senate stock trading disclosures (third-party aggregate)...")
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


# ═══════════════════════════════════════════════════════════════════════════════
# Main — dispatch based on CLI flags
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Download congressional stock trade disclosures from all sources"
    )
    parser.add_argument("--members", action="store_true",
                        help="Download congress member data only")
    parser.add_argument("--senate-efd", action="store_true",
                        help="Download official Senate eFD trades only")
    parser.add_argument("--house-index", action="store_true",
                        help="Download House FD/PTR XML indexes only")
    parser.add_argument("--house-ptr", action="store_true",
                        help="Download and parse House PTR PDFs only")
    parser.add_argument("--senate", action="store_true",
                        help="Download legacy third-party Senate aggregate only")
    args = parser.parse_args()

    # If no flags, do everything in the correct order
    do_all = not (args.members or args.senate_efd or args.house_index
                  or args.house_ptr or args.senate)

    session = create_session()
    start_time = time.time()

    log.info("=" * 60)
    log.info("Congressional Stock Trade Disclosures")
    log.info("=" * 60)

    # Full run order: members -> senate-efd -> house-index -> house-ptr -> senate-legacy
    # Members first (used by eFD for name matching in build script).
    # House index before house-ptr (PTR parsing reads index files).
    # Senate legacy last (backup source, lowest priority).

    if do_all or args.members:
        phase_members(session)
        if _shutdown:
            _report_elapsed(start_time)
            return

    if do_all or args.senate_efd:
        phase_senate_efd()
        if _shutdown:
            _report_elapsed(start_time)
            return

    if do_all or args.house_index:
        phase_house_index(session)
        if _shutdown:
            _report_elapsed(start_time)
            return

    if do_all or args.house_ptr:
        phase_house_ptr()
        if _shutdown:
            _report_elapsed(start_time)
            return

    if do_all or args.senate:
        phase_senate_legacy(session)

    _report_elapsed(start_time)


def _report_elapsed(start_time):
    elapsed = time.time() - start_time
    status = "Interrupted" if _shutdown else "Complete"
    log.info(f"\n{'=' * 60}")
    log.info(f"{status} in {elapsed:.0f} seconds")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
