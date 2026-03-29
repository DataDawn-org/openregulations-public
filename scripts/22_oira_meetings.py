#!/usr/bin/env python3
"""
Phase 22: Download OIRA EO 12866 meeting and regulatory review data from reginfo.gov.

Collects three types of data:
1. EO 12866 regulatory reviews (XML bulk, 1981-present, ~25K records)
2. OIRA meetings with outside parties (XML bulk for 2024, scrape for historical)
3. Meeting attendee details (scraped from individual meeting pages)

Data sources (no API key needed — all public):
  - reginfo.gov XML reports: EO review data + 2024 meeting summaries
  - reginfo.gov search interface: historical meetings (2001-present)
  - reginfo.gov detail pages: full attendee lists per meeting

Output: JSON files in openregs/oira_meetings/
  - reviews/       — EO 12866 review records (by year)
  - meetings/      — Meeting summary records
  - attendees/     — Attendee details per meeting

Usage:
    python3 22_oira_meetings.py                    # full run: reviews + meetings + attendees
    python3 22_oira_meetings.py --reviews-only     # just EO review XMLs
    python3 22_oira_meetings.py --meetings-only    # just meeting summaries (XML + scrape)
    python3 22_oira_meetings.py --attendees-only   # just scrape attendee detail pages
    python3 22_oira_meetings.py --year 2024        # specific year only
"""

import argparse
import json
import logging
import re
import signal
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "oira_meetings"
REVIEWS_DIR = OUTPUT_DIR / "reviews"
MEETINGS_DIR = OUTPUT_DIR / "meetings"
ATTENDEES_DIR = OUTPUT_DIR / "attendees"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "oira_state.json"
REQUEST_DELAY = 2.0  # Polite delay between scrape requests

# reginfo.gov URLs
XML_BASE = "https://www.reginfo.gov/public/do/XMLViewFileAction?f="
MEETING_SEARCH_URL = "https://www.reginfo.gov/public/do/eom12866Search"
MEETING_SEARCH_RESULTS_URL = "https://www.reginfo.gov/public/do/eom12866SearchResults"
MEETING_DETAIL_URL = "https://www.reginfo.gov/public/do/viewEO12866Meeting"

# Meeting XML files (bulk download, 2024 only)
MEETING_XML_FILES = {
    "2024_H1": "EO_12866_Meeting_Data_First_Half_2024.xml",
    "2024_H2": "EO_12866_Meeting_Data_Second_Half_2024.xml",
}

# EO review XML files: EO_RULE_COMPLETED_YYYY.xml (1981-2025)
# Plus special files for current/recent reviews
REVIEW_SPECIAL_FILES = [
    "EO_RULES_UNDER_REVIEW.xml",
    "EO_RULE_COMPLETED_YTD.xml",
]
REVIEW_YEAR_RANGE = range(1981, 2027)  # 1981-2026

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REVIEWS_DIR.mkdir(parents=True, exist_ok=True)
MEETINGS_DIR.mkdir(parents=True, exist_ok=True)
ATTENDEES_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("oira_meetings")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "oira_meetings.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
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
    log.warning("Shutdown signal received — finishing current operation")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# === State Management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "reviews_complete": False,
        "review_years_done": [],
        "meetings_xml_done": False,
        "meeting_scrape_year": None,
        "meeting_scrape_month": None,
        "meetings_scrape_complete": False,
        "attendees_index": 0,
        "attendees_complete": False,
    }


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# === HTTP Session ===
def create_session():
    session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=2))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return session


# ── Phase 1: EO 12866 Regulatory Reviews (XML bulk) ─────────────────────────

def download_review_xml(session, xml_filename):
    """Download and parse an EO review XML file. Returns list of review dicts."""
    url = f"{XML_BASE}{xml_filename}"

    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        # Check we got XML, not an HTML error page
        ct = resp.headers.get("content-type", "")
        if "html" in ct.lower():
            return []
    except Exception as e:
        log.error(f"  Failed to download {xml_filename}: {e}")
        return []

    reviews = []
    try:
        root = ET.fromstring(resp.content)
        # Root is OIRA_DATA, children are REGACT
        for row in root:
            if row.tag != "REGACT":
                continue
            review = {}
            for child in row:
                tag = child.tag.strip()
                text = (child.text or "").strip()
                if text:
                    review[tag] = text
            if review.get("RIN") or review.get("AGENCY_CODE"):
                reviews.append(review)

    except ET.ParseError as e:
        log.error(f"  XML parse error for {xml_filename}: {e}")

    return reviews


def fetch_reviews(session, state):
    """Phase 1: Download all EO 12866 review XMLs (1981-present)."""
    log.info("=== Phase 1: EO 12866 Regulatory Reviews ===")

    # Build file list: annual files + special files
    xml_files = []
    for year in REVIEW_YEAR_RANGE:
        xml_files.append(f"EO_RULE_COMPLETED_{year}.xml")
    xml_files.extend(REVIEW_SPECIAL_FILES)

    total_reviews = 0
    for xml_file in xml_files:
        if _shutdown:
            break

        out_file = REVIEWS_DIR / f"{xml_file.replace('.xml', '.json')}"
        if out_file.exists():
            existing = json.loads(out_file.read_text())
            total_reviews += len(existing)
            log.info(f"  [SKIP] {xml_file}: {len(existing)} reviews on disk")
            continue

        log.info(f"  Downloading {xml_file}...")
        reviews = download_review_xml(session, xml_file)
        if reviews:
            out_file.write_text(json.dumps(reviews, indent=2))
            total_reviews += len(reviews)
            log.info(f"  [OK] {xml_file}: {len(reviews)} reviews")
        else:
            log.info(f"  [EMPTY] {xml_file}: no reviews")
        time.sleep(0.5)

    log.info(f"  Total EO reviews: {total_reviews:,}")
    state["reviews_complete"] = True
    save_state(state)
    return total_reviews


# ── Phase 2: Meeting Summaries (XML bulk + scrape) ──────────────────────────

def parse_meeting_xml(xml_content):
    """Parse meeting XML content into list of meeting dicts."""
    meetings = []
    try:
        root = ET.fromstring(xml_content)
        # Root is RESULTS, children are ROW
        for row in root:
            if row.tag != "ROW":
                continue
            meeting = {}
            for child in row:
                tag = child.tag.strip()
                text = (child.text or "").strip()
                if text:
                    meeting[tag] = text

            # Generate a stable meeting_id if not present (H1 2024 lacks MEETING_ID)
            if not meeting.get("MEETING_ID") and meeting.get("RIN"):
                # Hash from RIN + date + requestor for uniqueness
                import hashlib
                key = f"{meeting.get('RIN', '')}|{meeting.get('MEETINGDATETIME', '')}|{meeting.get('REQUESTOR', '')}"
                meeting["MEETING_ID"] = f"gen_{hashlib.md5(key.encode()).hexdigest()[:12]}"

            if meeting.get("RIN") or meeting.get("MEETING_ID"):
                meetings.append(meeting)

    except ET.ParseError as e:
        log.error(f"  Meeting XML parse error: {e}")

    return meetings


def fetch_meeting_xmls(session, state):
    """Download 2024 meeting XML bulk files."""
    log.info("Downloading 2024 meeting XML files...")
    total = 0

    for label, xml_file in MEETING_XML_FILES.items():
        out_file = MEETINGS_DIR / f"xml_{label}.json"
        if out_file.exists():
            existing = json.loads(out_file.read_text())
            total += len(existing)
            log.info(f"  [SKIP] {label}: {len(existing)} meetings already saved")
            continue

        url = f"{XML_BASE}{xml_file}"
        log.info(f"  Downloading {xml_file}...")

        try:
            resp = session.get(url, timeout=120)
            resp.raise_for_status()
            meetings = parse_meeting_xml(resp.content)
            out_file.write_text(json.dumps(meetings, indent=2))
            total += len(meetings)
            log.info(f"  [OK] {label}: {len(meetings)} meetings")
        except Exception as e:
            log.error(f"  Failed to download {xml_file}: {e}")

        time.sleep(1.0)

    state["meetings_xml_done"] = True
    save_state(state)
    return total


def get_csrf_token(session):
    """Load the search page to get a fresh CSRF token."""
    resp = session.get(MEETING_SEARCH_URL, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    csrf_input = soup.find("input", {"name": "csrf_token"})
    return csrf_input["value"] if csrf_input else ""


def scrape_meeting_search(session, state):
    """
    Scrape historical meetings from the reginfo.gov search interface.
    Iterates by month to stay under the 1000-result limit per query.
    """
    log.info("=== Scraping historical OIRA meetings ===")

    # Get CSRF token (refresh periodically)
    try:
        csrf_token = get_csrf_token(session)
        log.info(f"  Got CSRF token: {csrf_token[:16]}...")
    except Exception as e:
        log.error(f"Failed to get CSRF token: {e}")
        return 0

    total_meetings = 0
    start_year = state.get("meeting_scrape_year") or 2001
    start_month = state.get("meeting_scrape_month") or 1
    last_csrf_refresh = time.time()

    # Month end days (handle Feb simply — 28 is fine, server accepts it)
    month_ends = {
        1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
        7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
    }

    for year in range(start_year, 2027):
        if _shutdown:
            break

        for month in range(1 if year > start_year else start_month, 13):
            if _shutdown:
                break

            out_file = MEETINGS_DIR / f"search_{year}_{month:02d}.json"
            if out_file.exists():
                existing = json.loads(out_file.read_text())
                total_meetings += len(existing)
                continue

            # Refresh CSRF token every 20 minutes
            if time.time() - last_csrf_refresh > 1200:
                try:
                    csrf_token = get_csrf_token(session)
                    last_csrf_refresh = time.time()
                    log.info(f"  Refreshed CSRF token")
                except Exception:
                    pass

            start_date = f"{month:02d}/01/{year}"
            end_date = f"{month:02d}/{month_ends[month]}/{year}"

            log.info(f"  Searching {year}-{month:02d} ({start_date} - {end_date})...")

            meetings = scrape_date_range(session, csrf_token, start_date, end_date)

            if meetings:
                out_file.write_text(json.dumps(meetings, indent=2))
                total_meetings += len(meetings)
                log.info(f"  {year}-{month:02d}: {len(meetings)} meetings")
            else:
                out_file.write_text("[]")
                log.info(f"  {year}-{month:02d}: no meetings")

            state["meeting_scrape_year"] = year
            state["meeting_scrape_month"] = month
            save_state(state)
            time.sleep(REQUEST_DELAY)

    state["meetings_scrape_complete"] = True
    save_state(state)
    log.info(f"  Total scraped meetings: {total_meetings:,}")
    return total_meetings


def scrape_date_range(session, csrf_token, start_date, end_date):
    """
    Scrape all meetings in a date range, handling pagination (10 per page).
    Uses POST for page 1, then GET with ?pagenum=N for subsequent pages.
    """
    all_meetings = []
    seen_ids = set()

    # Page 1: POST with search parameters
    form_data = {
        "csrf_token": csrf_token,
        "rin": "",
        "eomRuleStageList": "",
        "searchStartDate": start_date,
        "searchEndDate": end_date,
        "agencyCodes": "",
        "subAgencyCodes": "",
        "meetingType": "",
        "resultCount": "1",
        "sortCol": "",
        "sortOrder": "DESC",
    }

    try:
        resp = session.post(
            MEETING_SEARCH_RESULTS_URL,
            data=form_data,
            headers={
                "Referer": MEETING_SEARCH_URL,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=60,
        )
        resp.raise_for_status()
    except Exception as e:
        log.error(f"  Search failed ({start_date}-{end_date}): {e}")
        return []

    # Check for "too many results" error (>1000)
    if "allowed maximum" in resp.text:
        log.warning(f"  Too many results for {start_date}-{end_date}")
        return []

    page_meetings = parse_search_results(resp.text)
    for m in page_meetings:
        mid = m.get("meeting_id", "")
        if mid and mid not in seen_ids:
            seen_ids.add(mid)
            all_meetings.append(m)

    if not page_meetings:
        return all_meetings

    # Check for pagination — look for doPagination links
    has_next = "doPagination" in resp.text

    # Paginate through remaining pages
    page = 1
    while has_next and not _shutdown:
        time.sleep(1.0)  # Be polite between pages

        try:
            # Subsequent pages: POST with same form + pagenum query param
            resp = session.post(
                f"{MEETING_SEARCH_RESULTS_URL}?view=yes&pagenum={page}",
                data=form_data,
                headers={
                    "Referer": MEETING_SEARCH_RESULTS_URL,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                timeout=60,
            )
            resp.raise_for_status()
        except Exception as e:
            log.error(f"  Pagination failed (page {page + 1}): {e}")
            break

        page_meetings = parse_search_results(resp.text)
        new_count = 0
        for m in page_meetings:
            mid = m.get("meeting_id", "")
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                all_meetings.append(m)
                new_count += 1

        if new_count == 0:
            break  # No new results, done

        # Check if there are more pages
        has_next = f"doPagination({page + 1})" in resp.text
        page += 1

    return all_meetings


def parse_search_results(html):
    """Parse meeting search results HTML into list of meeting dicts."""
    meetings = []

    # Extract all meeting detail links with regex (more reliable than table parsing)
    links = re.findall(
        r'viewEO12866Meeting\?viewRule=false&rin=([^&]+)&meetingId=(\d+)&acronym=([^"\'&\s]+)',
        html
    )

    if not links:
        return meetings

    # Parse the HTML table for additional columns
    soup = BeautifulSoup(html, "html.parser")

    # Find all rows that contain meeting links
    for a_tag in soup.find_all("a", href=re.compile(r"viewEO12866Meeting")):
        href = a_tag.get("href", "")
        mid_match = re.search(r'meetingId=(\d+)', href)
        rin_match = re.search(r'rin=([^&]+)', href)
        acr_match = re.search(r'acronym=([^&"\']+)', href)

        if not mid_match:
            continue

        meeting = {
            "meeting_id": mid_match.group(1),
            "rin": rin_match.group(1) if rin_match else "",
            "agency_acronym": acr_match.group(1) if acr_match else "",
        }

        # Get parent row for additional data
        row = a_tag.find_parent("tr")
        if row:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            # Typical column order: Date, RIN, Agency, Title, Stage, Status
            if len(cells) >= 6:
                meeting["meeting_date"] = cells[0]
                meeting["title"] = cells[3]
                meeting["rule_stage"] = cells[4]
                meeting["meeting_type"] = cells[5]

        meetings.append(meeting)

    return meetings


# ── Phase 3: Meeting Attendee Details ────────────────────────────────────────

def collect_all_meeting_ids():
    """Gather all meeting IDs from saved search results and XML data."""
    meeting_ids = {}  # meeting_id -> {rin, acronym}

    # From search results
    for f in sorted(MEETINGS_DIR.glob("search_*.json")):
        meetings = json.loads(f.read_text())
        for m in meetings:
            mid = m.get("meeting_id")
            if mid:
                meeting_ids[mid] = {
                    "rin": m.get("rin_from_url", m.get("rin", "")),
                    "acronym": m.get("agency_acronym", ""),
                }

    # From XML data
    for f in sorted(MEETINGS_DIR.glob("xml_*.json")):
        meetings = json.loads(f.read_text())
        for m in meetings:
            mid = m.get("MEETING_ID")
            if mid:
                rin = m.get("RIN", "")
                acr = m.get("AGENCYSUBAGENCYACRONYM", "")
                # Acronym format: "0581-USDA/AMS" -> extract just the agency part
                meeting_ids[mid] = {
                    "rin": rin,
                    "acronym": acr.split("-", 1)[0] if "-" in acr else acr,
                }

    return meeting_ids


def scrape_meeting_detail(session, meeting_id, rin, acronym):
    """Scrape a single meeting detail page for attendee information."""
    params = {
        "viewRule": "false",
        "rin": rin,
        "meetingId": meeting_id,
        "acronym": acronym,
    }

    try:
        resp = session.get(MEETING_DETAIL_URL, params=params, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"  Failed to fetch meeting {meeting_id}: {e}")
        return None

    return parse_meeting_detail(resp.text, meeting_id)


def parse_meeting_detail(html, meeting_id):
    """Parse meeting detail page for attendee list and metadata.

    Page structure:
      - Metadata: <label class="generalTxt"> followed by text in same <p>
      - Attendees: <tr> rows with 3 <td>: bullet, "Name - Org", participation type
      - Documents: <table class="datatable"> with document links
    """
    soup = BeautifulSoup(html, "html.parser")

    detail = {
        "meeting_id": meeting_id,
        "attendees": [],
        "documents": [],
    }

    # --- Metadata from <label class="generalTxt"> elements ---
    field_map = {
        "rin": "rin",
        "title": "rule_title",
        "agency": "agency",
        "stage of rulemaking": "rule_stage",
        "meeting date": "meeting_date",
    }

    for label in soup.find_all("label", class_="generalTxt"):
        label_text = label.get_text(strip=True).rstrip(":").lower()

        # Special handling: Requestor and Requestor's Name share one <p>
        if "requestor" in label_text and "name" not in label_text:
            # Collect text nodes between this label and the next label
            org_parts = []
            for sib in label.next_siblings:
                if hasattr(sib, "name") and sib.name == "label":
                    break  # stop at the "Requestor's Name:" label
                text = sib.string if hasattr(sib, "string") else str(sib)
                text = text.strip().strip("\xa0")
                if text:
                    org_parts.append(text)
            detail["requestor_org"] = " ".join(org_parts).strip()
            continue

        if "requestor" in label_text and "name" in label_text:
            # Collect text after this label
            name_parts = []
            for sib in label.next_siblings:
                if hasattr(sib, "name") and sib.name == "label":
                    break
                text = sib.string if hasattr(sib, "string") else str(sib)
                text = text.strip().strip("\xa0")
                if text:
                    name_parts.append(text)
            detail["requestor_name"] = " ".join(name_parts).strip()
            continue

        # The value follows the label in the same <p> element
        p = label.find_parent("p")
        if not p:
            continue

        # Get text immediately after the label (skip the label itself)
        value_parts = []
        for sib in label.next_siblings:
            if hasattr(sib, "name") and sib.name == "label":
                break
            if hasattr(sib, "name") and sib.name == "a":
                value_parts.append(sib.get_text(strip=True))
            else:
                text = sib.string if hasattr(sib, "string") else str(sib)
                text = text.strip().strip("\xa0")
                if text:
                    value_parts.append(text)
        value = " ".join(value_parts).strip()

        if not value:
            continue

        for key, field in field_map.items():
            if key in label_text:
                detail[field] = value
                break

    # --- Attendees: rows with bullet + "Name - Org" + participation type ---
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # Check for bullet in first cell (&#8226; = •)
        first_text = cells[0].get_text(strip=True)
        if first_text not in ("•", "\u2022", ""):
            continue

        # Second cell: "Name - Organization"
        name_org = cells[1].get_text(strip=True)
        if not name_org or "-" not in name_org:
            continue

        # Third cell: participation type
        participation = cells[2].get_text(strip=True)
        if participation not in ("Teleconference", "In Person", ""):
            continue

        # Parse "Mr./Ms./Dr. Name - Organization"
        # Split on " - " (with spaces around dash)
        parts = re.split(r'\s+-\s+', name_org, maxsplit=1)
        name = parts[0].strip()
        org = parts[1].strip() if len(parts) > 1 else ""

        # Strip honorifics
        name = re.sub(r'^(Mr\.|Ms\.|Mrs\.|Dr\.|Prof\.)\s*', '', name).strip()

        if name:
            detail["attendees"].append({
                "name": name,
                "organization": org,
                "participation": participation,
            })

    # --- Documents ---
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "eoDownloadDocument" in href:
            doc_id_match = re.search(r'documentID=(\d+)', href)
            detail["documents"].append({
                "title": link.get_text(strip=True),
                "document_id": doc_id_match.group(1) if doc_id_match else None,
                "url": href if href.startswith("http") else f"https://www.reginfo.gov{href}",
            })

    return detail


def fetch_attendees(session, state):
    """Phase 3: Scrape meeting detail pages for attendee lists."""
    log.info("=== Phase 3: Meeting Attendee Details ===")

    meeting_ids = collect_all_meeting_ids()
    all_ids = sorted(meeting_ids.keys())
    log.info(f"  {len(all_ids)} meetings to fetch attendees for")

    start_idx = state.get("attendees_index", 0)
    fetched = 0
    skipped = 0

    for i, mid in enumerate(all_ids[start_idx:], start=start_idx):
        if _shutdown:
            break

        out_file = ATTENDEES_DIR / f"{mid}.json"
        if out_file.exists():
            skipped += 1
            continue

        info = meeting_ids[mid]
        detail = scrape_meeting_detail(session, mid, info["rin"], info["acronym"])

        if detail:
            out_file.write_text(json.dumps(detail, indent=2))
            attendee_count = len(detail.get("attendees", []))
            fetched += 1
            if fetched % 50 == 0:
                log.info(f"  Progress: {i + 1}/{len(all_ids)} ({fetched} fetched, {skipped} skipped)")
        else:
            # Save error marker
            out_file.write_text(json.dumps({"meeting_id": mid, "error": True}))

        state["attendees_index"] = i + 1
        if fetched % 25 == 0:
            save_state(state)

        time.sleep(REQUEST_DELAY)

    state["attendees_complete"] = True
    save_state(state)
    log.info(f"  Attendee details: {fetched} fetched, {skipped} already on disk")
    return fetched


# ── Build OIRA Database ──────────────────────────────────────────────────────

def build_oira_db():
    """
    Build a small oira.db from all collected JSON data.
    This gets imported into openregs.db by the build script.
    """
    import sqlite3

    db_path = OUTPUT_DIR / "oira.db"
    log.info(f"=== Building OIRA database: {db_path} ===")

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
        CREATE TABLE oira_reviews (
            rin TEXT,
            agency_code TEXT,
            title TEXT,
            stage TEXT,
            economically_significant TEXT,
            date_received TEXT,
            date_completed TEXT,
            decision TEXT,
            date_published TEXT,
            major TEXT,
            legal_deadline TEXT,
            legal_deadline_date TEXT,
            homeland_security TEXT,
            regulatory_flexibility TEXT,
            small_entities TEXT,
            unfunded_mandates TEXT,
            federalism TEXT,
            international_impacts TEXT,
            source_file TEXT
        );

        CREATE TABLE oira_meetings (
            meeting_id TEXT PRIMARY KEY,
            rin TEXT,
            title TEXT,
            agency_acronym TEXT,
            rule_stage TEXT,
            meeting_date TEXT,
            requestor_org TEXT,
            requestor_name TEXT,
            meeting_type TEXT,
            type_cd TEXT,
            source TEXT
        );

        CREATE TABLE oira_meeting_attendees (
            meeting_id TEXT REFERENCES oira_meetings(meeting_id),
            attendee_name TEXT,
            attendee_org TEXT,
            participation_type TEXT,
            is_government INTEGER DEFAULT 0
        );

        CREATE INDEX idx_oira_reviews_rin ON oira_reviews(rin);
        CREATE INDEX idx_oira_reviews_agency ON oira_reviews(agency_code);
        CREATE INDEX idx_oira_reviews_date ON oira_reviews(date_received);
        CREATE INDEX idx_oira_reviews_decision ON oira_reviews(decision);
        CREATE INDEX idx_oira_meetings_rin ON oira_meetings(rin);
        CREATE INDEX idx_oira_meetings_date ON oira_meetings(meeting_date);
        CREATE INDEX idx_oira_meetings_org ON oira_meetings(requestor_org);
        CREATE INDEX idx_oira_attendees_mid ON oira_meeting_attendees(meeting_id);
        CREATE INDEX idx_oira_attendees_org ON oira_meeting_attendees(attendee_org);
    """)

    # Load reviews
    review_count = 0
    for f in sorted(REVIEWS_DIR.glob("*.json")):
        reviews = json.loads(f.read_text())
        for r in reviews:
            conn.execute("""
                INSERT INTO oira_reviews
                (rin, agency_code, title, stage, economically_significant,
                 date_received, date_completed, decision, date_published, major,
                 legal_deadline, legal_deadline_date, homeland_security,
                 regulatory_flexibility, small_entities, unfunded_mandates,
                 federalism, international_impacts, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r.get("RIN"), r.get("AGENCY_CODE"), r.get("TITLE"),
                r.get("STAGE"), r.get("ECONOMICALLY_SIGNIFICANT"),
                r.get("DATE_RECEIVED"), r.get("DATE_COMPLETED"),
                r.get("DECISION"), r.get("DATE_PUBLISHED"),
                r.get("MAJOR"), r.get("LEGAL_DEADLINE"),
                r.get("LEGAL_DEADLINE_DATE"), r.get("HOMELAND_SECURITY"),
                r.get("REGULATORY_FLEXIBILITY_ANALYSIS") or r.get("REGULATORY_FLEXIBILITY"),
                r.get("SMALL_ENTITIES_AFFECTED") or r.get("SMALL_ENTITIES"),
                r.get("UNFUNDED_MANDATES"),
                r.get("FEDERALISM_IMPLICATIONS") or r.get("FEDERALISM"),
                r.get("INTERNATIONAL_IMPACTS") or r.get("INTERNATIONAL_TRADE_IMPACTS"),
                f.name,
            ))
            review_count += 1
    conn.commit()
    log.info(f"  Reviews: {review_count:,}")

    # Load meetings from XML
    meeting_count = 0
    for f in sorted(MEETINGS_DIR.glob("xml_*.json")):
        meetings = json.loads(f.read_text())
        for m in meetings:
            mid = m.get("MEETING_ID")
            if not mid:
                continue
            conn.execute("""
                INSERT OR IGNORE INTO oira_meetings
                (meeting_id, rin, title, agency_acronym, rule_stage,
                 meeting_date, requestor_org, requestor_name, meeting_type,
                 type_cd, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                mid, m.get("RIN"), m.get("TITLE"),
                m.get("AGENCYSUBAGENCYACRONYM"), m.get("RULE_STAGE_DESC"),
                m.get("MEETINGDATETIME"), m.get("REQUESTOR"),
                m.get("REQUESTOR_NAME"), m.get("MEETING_TYPE"),
                m.get("TYPE_CD"), "xml",
            ))
            meeting_count += 1

    # Load meetings from search scrape
    for f in sorted(MEETINGS_DIR.glob("search_*.json")):
        meetings = json.loads(f.read_text())
        for m in meetings:
            mid = m.get("meeting_id")
            if not mid:
                continue
            conn.execute("""
                INSERT OR IGNORE INTO oira_meetings
                (meeting_id, rin, title, agency_acronym, rule_stage,
                 meeting_date, requestor_org, requestor_name, meeting_type,
                 type_cd, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                mid,
                m.get("rin_from_url", m.get("rin", "")),
                m.get("title", m.get("rule title", "")),
                m.get("agency_acronym", m.get("agency", "")),
                m.get("rule stage", m.get("stage", "")),
                m.get("meeting date", m.get("date", "")),
                m.get("requestor", m.get("organization", "")),
                m.get("requestor name", ""),
                m.get("meeting type", m.get("status", "")),
                m.get("type", ""),
                "scrape",
            ))
            meeting_count += 1
    conn.commit()
    log.info(f"  Meetings: {meeting_count:,}")

    # Load attendees
    attendee_count = 0
    gov_keywords = {"eop", "oira", "omb", "white house", "nec", "ostp", "cea",
                    "domestic policy", "national security"}
    for f in sorted(ATTENDEES_DIR.glob("*.json")):
        detail = json.loads(f.read_text())
        if detail.get("error"):
            continue
        mid = detail.get("meeting_id")
        if not mid:
            continue

        # Update meeting record with scraped metadata if richer
        if detail.get("rule_title"):
            conn.execute("""
                UPDATE oira_meetings SET title = COALESCE(NULLIF(title, ''), ?)
                WHERE meeting_id = ?
            """, (detail["rule_title"], mid))

        for att in detail.get("attendees", []):
            name = att.get("name", "")
            org = att.get("organization", att.get("affiliation", ""))
            participation = att.get("participation", att.get("participation type", ""))

            # Detect government attendees
            is_gov = 0
            org_lower = org.lower() if org else ""
            if any(kw in org_lower for kw in gov_keywords):
                is_gov = 1

            conn.execute("""
                INSERT INTO oira_meeting_attendees
                (meeting_id, attendee_name, attendee_org, participation_type, is_government)
                VALUES (?, ?, ?, ?, ?)
            """, (mid, name, org, participation, is_gov))
            attendee_count += 1

    conn.commit()
    log.info(f"  Attendees: {attendee_count:,}")

    # Summary
    for table in ["oira_reviews", "oira_meetings", "oira_meeting_attendees"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"  {table}: {count:,}")

    conn.close()
    size_mb = db_path.stat().st_size / (1024 * 1024)
    log.info(f"  Database size: {size_mb:.1f} MB")
    return db_path


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download OIRA EO 12866 meeting and review data from reginfo.gov"
    )
    parser.add_argument("--reviews-only", action="store_true",
                        help="Only download EO review XMLs")
    parser.add_argument("--meetings-only", action="store_true",
                        help="Only download meeting summaries")
    parser.add_argument("--attendees-only", action="store_true",
                        help="Only scrape attendee detail pages")
    parser.add_argument("--build-only", action="store_true",
                        help="Only build oira.db from existing JSON")
    parser.add_argument("--year", type=int, default=None,
                        help="Limit to a specific year")
    args = parser.parse_args()

    session = create_session()
    state = load_state()
    t_start = time.time()

    if args.build_only:
        build_oira_db()
    elif args.reviews_only:
        fetch_reviews(session, state)
        build_oira_db()
    elif args.meetings_only:
        fetch_meeting_xmls(session, state)
        if not _shutdown:
            scrape_meeting_search(session, state)
        if not _shutdown:
            build_oira_db()
    elif args.attendees_only:
        fetch_attendees(session, state)
        if not _shutdown:
            build_oira_db()
    else:
        # Full run
        log.info("=== OIRA Data Collection: Full Run ===")

        # Phase 1: EO reviews
        fetch_reviews(session, state)

        # Phase 2: Meeting summaries
        if not _shutdown:
            fetch_meeting_xmls(session, state)
        if not _shutdown:
            scrape_meeting_search(session, state)

        # Phase 3: Attendee details
        if not _shutdown:
            fetch_attendees(session, state)

        # Build database
        if not _shutdown:
            build_oira_db()

    elapsed = time.time() - t_start
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    log.info(f"\n=== Done in {hours}h {minutes}m ===")


if __name__ == "__main__":
    main()
