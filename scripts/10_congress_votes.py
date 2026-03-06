#!/usr/bin/env python3
"""
Phase 9b: Download congressional roll call votes (House + Senate).

Downloads all roll call votes for congresses 106-119 (1999-2026),
including individual member votes for each roll call.

Data sources:
  - Congress.gov API v3 (house-vote endpoint) for House votes in 118-119
  - House Clerk XML (clerk.house.gov) for House votes in 106-117
  - Senate.gov XML for all Senate votes (106-119)

Produces:
  - Raw JSON files under congress_gov/votes/{congress}/{chamber}/
  - SQLite database at votes.db with tables:
      roll_call_votes  — one row per vote
      member_votes     — one row per member per vote

Usage:
    python3 09_congress_votes.py                      # default: 118-119
    python3 09_congress_votes.py --congress 116 117    # specific congresses
    python3 09_congress_votes.py --full                # all 106-119
    python3 09_congress_votes.py --build-db            # rebuild DB from saved JSON
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
API_BASE = "https://api.congress.gov/v3"
HOUSE_CLERK_BASE = "https://clerk.house.gov/evs"
SENATE_VOTE_LIST_BASE = "https://www.senate.gov/legislative/LIS/roll_call_lists"
SENATE_VOTE_BASE = "https://www.senate.gov/legislative/LIS/roll_call_votes"

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "congress_gov" / "votes"
LOG_DIR = PROJECT_DIR / "logs"
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"
DB_PATH = PROJECT_DIR / "votes.db"

PAGE_SIZE = 250  # Congress.gov API max
API_INTERVAL = 0.75  # ~4,800 req/hr, under the 5,000/hr limit
XML_INTERVAL = 0.3   # polite delay for XML downloads from clerk/senate

# Congress number -> (session_1_start_year, session_2_start_year)
# Congress N covers years (1787 + 2*N - 1) and (1787 + 2*N)
CURRENT_CONGRESS = 119
MIN_CONGRESS = 106
MAX_CONGRESS = 119

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("congress_votes")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "congress_votes.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


# === Helpers ===

def congress_years(congress):
    """Return the two calendar years for a congress number."""
    year1 = 1787 + 2 * congress - 1
    year2 = year1 + 1
    return year1, year2


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
    """Create requests session for Congress.gov API."""
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2,
                    status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.params = {"api_key": api_key, "format": "json"}
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "OpenRegs/1.0 (regulatory data project)",
    })
    return session


def create_xml_session():
    """Create requests session for XML downloads (House Clerk / Senate.gov)."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "OpenRegs/1.0 (regulatory data project)",
    })
    return session


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
        log.error(f"API error for {url}: {e}")
        return None


def xml_get(session, url):
    """Download an XML resource with polite delay."""
    time.sleep(XML_INTERVAL)
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.RequestException as e:
        log.error(f"XML download error for {url}: {e}")
        return None


def normalize_bill_id(legislation_type, legislation_number, congress):
    """
    Convert bill references to our standard bill_id format.
    e.g. ("HR", "3424", 119) -> "hr3424-119"
         ("S.", "5", 119) -> "s5-119"
         ("H.R.", "1234", 118) -> "hr1234-118"
    """
    if not legislation_type or not legislation_number:
        return None
    # Normalize the type: strip dots and spaces, lowercase
    bill_type = legislation_type.replace(".", "").replace(" ", "").lower()
    # Map common variations
    type_map = {
        "hr": "hr",
        "s": "s",
        "hjres": "hjres",
        "sjres": "sjres",
        "hconres": "hconres",
        "sconres": "sconres",
        "hres": "hres",
        "sres": "sres",
        "hamdt": "hamdt",
        "samdt": "samdt",
    }
    bill_type = type_map.get(bill_type, bill_type)
    # Strip leading zeros from number
    num = str(legislation_number).lstrip("0")
    if not num:
        return None
    return f"{bill_type}{num}-{congress}"


def parse_date_flexible(date_str):
    """Parse various date formats into YYYY-MM-DD."""
    if not date_str:
        return None
    # ISO format with timezone: "2025-09-08T18:56:00-04:00"
    if "T" in date_str:
        try:
            # Strip timezone for simple parsing
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
    # "6-Jan-1999" format
    try:
        dt = datetime.strptime(date_str.strip(), "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass
    # "January 9, 2025,  02:54 PM"
    try:
        # Remove extra whitespace and trailing time
        cleaned = re.sub(r',\s+\d{2}:\d{2}\s+[AP]M', '', date_str).strip().rstrip(",")
        dt = datetime.strptime(cleaned, "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass
    # Already YYYY-MM-DD
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    return date_str


# ===================================================================
# House Votes: Congress.gov API (congresses 118+)
# ===================================================================

def download_house_votes_api(api_session, congress, out_dir):
    """
    Download House votes using the Congress.gov API.
    Available for congresses 118+.
    Returns list of vote metadata dicts.
    """
    log.info(f"  Fetching House vote list from API for congress {congress}...")
    votes = []
    offset = 0
    while True:
        url = f"{API_BASE}/house-vote/{congress}"
        params = {"limit": PAGE_SIZE, "offset": offset}
        data = api_get(api_session, url, params)
        if not data:
            log.error(f"  Failed to fetch house vote list at offset {offset}")
            break
        batch = data.get("houseRollCallVotes", [])
        if not batch:
            break
        votes.extend(batch)
        total = data.get("pagination", {}).get("count", "?")
        log.info(f"  House votes {congress}: {len(votes)}/{total} listed")
        if "next" not in data.get("pagination", {}):
            break
        offset += PAGE_SIZE

    log.info(f"  Listed {len(votes)} House votes for congress {congress}")

    # Now fetch member votes for each roll call
    downloaded = 0
    skipped = 0
    for vote in votes:
        roll_num = vote.get("rollCallNumber")
        session_num = vote.get("sessionNumber")
        if not roll_num or not session_num:
            continue

        vote_file = out_dir / f"house_{session_num}_{roll_num}.json"
        if vote_file.exists():
            skipped += 1
            continue

        # Fetch member-level votes
        detail_url = f"{API_BASE}/house-vote/{congress}/{session_num}/{roll_num}/members"
        detail = api_get(api_session, detail_url)
        if not detail:
            log.warning(f"  Failed to fetch house vote {congress}/{session_num}/{roll_num}")
            continue

        member_data = detail.get("houseRollCallVoteMemberVotes", {})

        # Build combined record
        record = {
            "source": "congress_api",
            "chamber": "house",
            "congress": congress,
            "session": session_num,
            "roll_call_number": roll_num,
            "date": parse_date_flexible(vote.get("startDate")),
            "question": member_data.get("voteQuestion", vote.get("voteQuestion", "")),
            "vote_type": member_data.get("voteType", vote.get("voteType", "")),
            "result": member_data.get("result", vote.get("result", "")),
            "legislation_type": vote.get("legislationType"),
            "legislation_number": vote.get("legislationNumber"),
            "bill_id": normalize_bill_id(
                vote.get("legislationType"),
                vote.get("legislationNumber"),
                congress
            ),
            "description": vote.get("voteType", ""),
            "source_url": vote.get("sourceDataURL", ""),
            "members": member_data.get("results", []),
        }

        # Compute totals from members
        yea = nay = not_voting = present = 0
        for m in record["members"]:
            vc = (m.get("voteCast") or "").lower()
            if vc in ("yea", "aye"):
                yea += 1
            elif vc in ("nay", "no"):
                nay += 1
            elif vc == "present":
                present += 1
            elif vc in ("not voting", "notvoting"):
                not_voting += 1
        record["yea_count"] = yea
        record["nay_count"] = nay
        record["present_count"] = present
        record["not_voting_count"] = not_voting

        with open(vote_file, "w") as f:
            json.dump(record, f, indent=2)
        downloaded += 1

        if (downloaded + skipped) % 50 == 0:
            log.info(f"  House {congress}: {downloaded} downloaded, {skipped} skipped of {len(votes)}")

    log.info(f"  House {congress}: {downloaded} downloaded, {skipped} already cached")
    return downloaded


# ===================================================================
# House Votes: Clerk XML (congresses 106-117)
# ===================================================================

def parse_house_clerk_xml(xml_text_str, congress):
    """Parse House Clerk roll call XML into a structured dict."""
    try:
        root = ET.fromstring(xml_text_str)
    except ET.ParseError as e:
        log.error(f"  XML parse error: {e}")
        return None

    meta = root.find("vote-metadata")
    if meta is None:
        return None

    def xt(elem, tag, default=""):
        node = elem.find(tag)
        return node.text.strip() if node is not None and node.text else default

    roll_num_str = xt(meta, "rollcall-num")
    roll_num = int(roll_num_str) if roll_num_str.isdigit() else 0

    # Parse session from the text (e.g., "1st" or "2nd")
    session_text = xt(meta, "session")
    session = 1 if "1" in session_text else 2

    date_str = xt(meta, "action-date")
    question = xt(meta, "vote-question")
    vote_type = xt(meta, "vote-type")
    result = xt(meta, "vote-result")
    legis_num = xt(meta, "legis-num")
    description = xt(meta, "vote-desc")

    # Parse legislation reference from legis-num
    # Formats: "H R 1234", "H RES 123", "H J RES 45", "QUORUM", "JOURNAL", etc.
    bill_id = None
    legislation_type = None
    legislation_number = None
    if legis_num and legis_num not in ("QUORUM", "JOURNAL", "MOTION", "ADJOURN", ""):
        # Try to parse "H R 1234" or "S 1234" etc.
        m = re.match(
            r'^(H\s*R|S|H\s*J\s*RES|S\s*J\s*RES|H\s*CON\s*RES|S\s*CON\s*RES|H\s*RES|S\s*RES)\s+(\d+)',
            legis_num, re.IGNORECASE
        )
        if m:
            raw_type = m.group(1).replace(" ", "").lower()
            legislation_number = m.group(2)
            type_map = {
                "hr": "HR", "s": "S",
                "hjres": "HJRES", "sjres": "SJRES",
                "hconres": "HCONRES", "sconres": "SCONRES",
                "hres": "HRES", "sres": "SRES",
            }
            legislation_type = type_map.get(raw_type, raw_type.upper())
            bill_id = normalize_bill_id(legislation_type, legislation_number, congress)

    # Parse vote totals
    yea = nay = present = not_voting = 0
    totals = meta.find(".//totals-by-vote")
    if totals is not None:
        yea = int(xt(totals, "yea-total", "0") or 0)
        nay = int(xt(totals, "nay-total", "0") or 0)
        present = int(xt(totals, "present-total", "0") or 0)
        not_voting = int(xt(totals, "not-voting-total", "0") or 0)

    # Parse member votes
    members = []
    vote_data = root.find("vote-data")
    if vote_data is not None:
        for rv in vote_data.findall("recorded-vote"):
            legislator = rv.find("legislator")
            vote_elem = rv.find("vote")
            if legislator is not None and vote_elem is not None:
                members.append({
                    "bioguideID": legislator.get("name-id", ""),
                    "firstName": "",  # not in Clerk XML
                    "lastName": legislator.get("sort-field", legislator.text or ""),
                    "voteCast": vote_elem.text.strip() if vote_elem.text else "",
                    "voteParty": legislator.get("party", ""),
                    "voteState": legislator.get("state", ""),
                })

    return {
        "source": "house_clerk_xml",
        "chamber": "house",
        "congress": congress,
        "session": session,
        "roll_call_number": roll_num,
        "date": parse_date_flexible(date_str),
        "question": question,
        "vote_type": vote_type,
        "result": result,
        "legislation_type": legislation_type,
        "legislation_number": legislation_number,
        "bill_id": bill_id,
        "description": description,
        "source_url": "",
        "yea_count": yea,
        "nay_count": nay,
        "present_count": present,
        "not_voting_count": not_voting,
        "members": members,
    }


def download_house_votes_xml(xml_session, congress, out_dir):
    """
    Download House votes from House Clerk XML files.
    Used for congresses where the API has no data (106-117).
    """
    year1, year2 = congress_years(congress)
    log.info(f"  Downloading House votes from Clerk XML for congress {congress} (years {year1}-{year2})...")

    downloaded = 0
    skipped = 0

    for year in (year1, year2):
        session = 1 if year == year1 else 2
        roll_num = 1
        consecutive_misses = 0

        while consecutive_misses < 5:
            vote_file = out_dir / f"house_{session}_{roll_num}.json"
            if vote_file.exists():
                skipped += 1
                roll_num += 1
                consecutive_misses = 0
                continue

            url = f"{HOUSE_CLERK_BASE}/{year}/roll{roll_num:03d}.xml"
            xml_text_str = xml_get(xml_session, url)
            if xml_text_str is None:
                consecutive_misses += 1
                roll_num += 1
                continue

            consecutive_misses = 0
            record = parse_house_clerk_xml(xml_text_str, congress)
            if record:
                record["source_url"] = url
                with open(vote_file, "w") as f:
                    json.dump(record, f, indent=2)
                downloaded += 1
            else:
                log.warning(f"  Failed to parse House Clerk XML: {url}")

            roll_num += 1

            if downloaded % 100 == 0 and downloaded > 0:
                log.info(f"  House {congress} year {year}: {downloaded} downloaded so far...")

    log.info(f"  House {congress}: {downloaded} downloaded, {skipped} already cached")
    return downloaded


# ===================================================================
# Senate Votes: Senate.gov XML (all congresses)
# ===================================================================

def get_senate_vote_list(xml_session, congress, session):
    """
    Fetch the Senate vote listing XML to get all vote numbers for a
    given congress+session.
    """
    url = f"{SENATE_VOTE_LIST_BASE}/vote_menu_{congress}_{session}.xml"
    xml_text_str = xml_get(xml_session, url)
    if not xml_text_str:
        return []

    try:
        root = ET.fromstring(xml_text_str)
    except ET.ParseError:
        log.error(f"  Failed to parse Senate vote list XML for {congress}/{session}")
        return []

    vote_numbers = []
    for vote in root.findall(".//vote"):
        vn = vote.find("vote_number")
        if vn is not None and vn.text:
            num = vn.text.strip().lstrip("0")
            if num:
                vote_numbers.append(int(num))

    return sorted(vote_numbers)


def parse_senate_vote_xml(xml_text_str, congress):
    """Parse Senate roll call vote XML into a structured dict."""
    try:
        root = ET.fromstring(xml_text_str)
    except ET.ParseError as e:
        log.error(f"  XML parse error: {e}")
        return None

    def xt(tag, default=""):
        node = root.find(tag)
        return node.text.strip() if node is not None and node.text else default

    session = int(xt("session", "1") or 1)
    vote_number = int(xt("vote_number", "0").lstrip("0") or 0)
    date_str = xt("vote_date")
    question = xt("vote_question_text", xt("question"))
    result = xt("vote_result", xt("vote_result_text"))
    title = xt("vote_title")
    vote_doc_text = xt("vote_document_text")

    # Parse legislation reference from <document> element
    doc = root.find("document")
    legislation_type = None
    legislation_number = None
    bill_id = None
    if doc is not None:
        doc_type = doc.find("document_type")
        doc_num = doc.find("document_number")
        if doc_type is not None and doc_type.text and doc_num is not None and doc_num.text:
            legislation_type = doc_type.text.strip().rstrip(".")
            legislation_number = doc_num.text.strip()
            bill_id = normalize_bill_id(legislation_type, legislation_number, congress)

    # Parse totals
    count = root.find("count")
    yea = nay = present = absent = 0
    if count is not None:
        yea = int(xt("count/yeas", "0") or 0)
        nay = int(xt("count/nays", "0") or 0)
        present_str = xt("count/present", "0")
        present = int(present_str) if present_str else 0
        absent_str = xt("count/absent", "0")
        absent = int(absent_str) if absent_str else 0

    # Parse member votes
    members = []
    for member in root.findall(".//members/member"):
        def mt(tag, default=""):
            node = member.find(tag)
            return node.text.strip() if node is not None and node.text else default

        lis_id = mt("lis_member_id")
        members.append({
            "bioguideID": lis_id,  # Senate uses LIS IDs; we store as-is
            "firstName": mt("first_name"),
            "lastName": mt("last_name"),
            "voteCast": mt("vote_cast"),
            "voteParty": mt("party"),
            "voteState": mt("state"),
            "lis_member_id": lis_id,
        })

    # Build description from title + vote document text
    description = title
    if vote_doc_text and vote_doc_text != title:
        description = f"{title} -- {vote_doc_text}" if title else vote_doc_text

    return {
        "source": "senate_xml",
        "chamber": "senate",
        "congress": congress,
        "session": session,
        "roll_call_number": vote_number,
        "date": parse_date_flexible(date_str),
        "question": question,
        "vote_type": "",
        "result": result,
        "legislation_type": legislation_type,
        "legislation_number": legislation_number,
        "bill_id": bill_id,
        "description": description,
        "source_url": "",
        "yea_count": yea,
        "nay_count": nay,
        "present_count": present,
        "not_voting_count": absent,
        "members": members,
    }


def download_senate_votes(xml_session, congress, out_dir):
    """Download Senate votes from Senate.gov XML for a given congress."""
    log.info(f"  Downloading Senate votes from XML for congress {congress}...")
    downloaded = 0
    skipped = 0

    for session in (1, 2):
        # Get list of vote numbers for this session
        vote_numbers = get_senate_vote_list(xml_session, congress, session)
        if not vote_numbers:
            log.info(f"  Senate {congress} session {session}: no votes found in index")
            continue

        log.info(f"  Senate {congress} session {session}: {len(vote_numbers)} votes in index")

        for vote_num in vote_numbers:
            vote_file = out_dir / f"senate_{session}_{vote_num}.json"
            if vote_file.exists():
                skipped += 1
                continue

            url = (
                f"{SENATE_VOTE_BASE}/vote{congress}{session}/"
                f"vote_{congress}_{session}_{vote_num:05d}.xml"
            )
            xml_text_str = xml_get(xml_session, url)
            if xml_text_str is None:
                log.warning(f"  Missing Senate vote XML: {url}")
                continue

            record = parse_senate_vote_xml(xml_text_str, congress)
            if record:
                record["source_url"] = url
                with open(vote_file, "w") as f:
                    json.dump(record, f, indent=2)
                downloaded += 1
            else:
                log.warning(f"  Failed to parse Senate vote XML: {url}")

            if downloaded % 100 == 0 and downloaded > 0:
                log.info(f"  Senate {congress} session {session}: {downloaded} downloaded so far...")

    log.info(f"  Senate {congress}: {downloaded} downloaded, {skipped} already cached")
    return downloaded


# ===================================================================
# Orchestrator
# ===================================================================

def download_congress_votes(api_session, xml_session, congress):
    """Download all votes (House + Senate) for a given congress."""
    congress_dir = OUTPUT_DIR / str(congress)
    house_dir = congress_dir / "house"
    senate_dir = congress_dir / "senate"
    house_dir.mkdir(parents=True, exist_ok=True)
    senate_dir.mkdir(parents=True, exist_ok=True)

    total = 0

    # House votes
    if congress >= 118:
        # Use Congress.gov API (has member-level data)
        total += download_house_votes_api(api_session, congress, house_dir)
    else:
        # Fall back to House Clerk XML
        total += download_house_votes_xml(xml_session, congress, house_dir)

    # Senate votes (always use Senate.gov XML)
    total += download_senate_votes(xml_session, congress, senate_dir)

    return total


# ===================================================================
# SQLite Database Build
# ===================================================================

DB_SCHEMA = """
-- Roll call vote summary
CREATE TABLE IF NOT EXISTS roll_call_votes (
    congress INTEGER NOT NULL,
    chamber TEXT NOT NULL,
    roll_call_number INTEGER NOT NULL,
    session INTEGER,
    date TEXT,
    question TEXT,
    vote_type TEXT,
    description TEXT,
    result TEXT,
    bill_id TEXT,
    legislation_type TEXT,
    legislation_number TEXT,
    yea_count INTEGER,
    nay_count INTEGER,
    present_count INTEGER,
    not_voting_count INTEGER,
    source_url TEXT,
    PRIMARY KEY (congress, chamber, session, roll_call_number)
);

-- Individual member votes
CREATE TABLE IF NOT EXISTS member_votes (
    congress INTEGER NOT NULL,
    chamber TEXT NOT NULL,
    session INTEGER NOT NULL,
    roll_call_number INTEGER NOT NULL,
    bioguide_id TEXT,
    member_name TEXT,
    party TEXT,
    state TEXT,
    vote_cast TEXT,
    FOREIGN KEY (congress, chamber, session, roll_call_number)
        REFERENCES roll_call_votes(congress, chamber, session, roll_call_number)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_rcv_date ON roll_call_votes(date);
CREATE INDEX IF NOT EXISTS idx_rcv_chamber ON roll_call_votes(chamber);
CREATE INDEX IF NOT EXISTS idx_rcv_congress ON roll_call_votes(congress);
CREATE INDEX IF NOT EXISTS idx_rcv_bill ON roll_call_votes(bill_id);
CREATE INDEX IF NOT EXISTS idx_rcv_result ON roll_call_votes(result);

CREATE INDEX IF NOT EXISTS idx_mv_bioguide ON member_votes(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_mv_vote ON member_votes(congress, chamber, session, roll_call_number);
CREATE INDEX IF NOT EXISTS idx_mv_party ON member_votes(party);
CREATE INDEX IF NOT EXISTS idx_mv_state ON member_votes(state);
CREATE INDEX IF NOT EXISTS idx_mv_cast ON member_votes(vote_cast);
"""


def build_database():
    """Build (or rebuild) the votes SQLite database from saved JSON files."""
    log.info(f"Building database at {DB_PATH}...")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(DB_SCHEMA)

    # Clear existing data for a clean rebuild
    conn.execute("DELETE FROM member_votes")
    conn.execute("DELETE FROM roll_call_votes")
    conn.commit()

    vote_count = 0
    member_count = 0

    vote_files = sorted(OUTPUT_DIR.glob("*/*/*.json"))
    log.info(f"Found {len(vote_files)} vote JSON files to load")

    batch_votes = []
    batch_members = []
    BATCH_SIZE = 500

    for filepath in vote_files:
        try:
            with open(filepath) as f:
                record = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"  Skipping {filepath}: {e}")
            continue

        congress = record.get("congress")
        chamber = record.get("chamber")
        session = record.get("session")
        roll_num = record.get("roll_call_number")
        if not all([congress, chamber, roll_num is not None]):
            continue

        batch_votes.append((
            congress,
            chamber,
            roll_num,
            session,
            record.get("date"),
            record.get("question"),
            record.get("vote_type"),
            record.get("description"),
            record.get("result"),
            record.get("bill_id"),
            record.get("legislation_type"),
            record.get("legislation_number"),
            record.get("yea_count"),
            record.get("nay_count"),
            record.get("present_count"),
            record.get("not_voting_count"),
            record.get("source_url"),
        ))
        vote_count += 1

        for m in record.get("members", []):
            first = m.get("firstName", "")
            last = m.get("lastName", "")
            name = f"{first} {last}".strip() if first else last
            batch_members.append((
                congress,
                chamber,
                session,
                roll_num,
                m.get("bioguideID", ""),
                name,
                m.get("voteParty", ""),
                m.get("voteState", ""),
                m.get("voteCast", ""),
            ))
            member_count += 1

        if len(batch_votes) >= BATCH_SIZE:
            conn.executemany(
                "INSERT OR REPLACE INTO roll_call_votes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch_votes,
            )
            conn.executemany(
                "INSERT INTO member_votes VALUES (?,?,?,?,?,?,?,?,?)",
                batch_members,
            )
            conn.commit()
            batch_votes = []
            batch_members = []
            log.info(f"  Loaded {vote_count} votes, {member_count} member votes...")

    # Flush remaining
    if batch_votes:
        conn.executemany(
            "INSERT OR REPLACE INTO roll_call_votes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch_votes,
        )
        conn.executemany(
            "INSERT INTO member_votes VALUES (?,?,?,?,?,?,?,?,?)",
            batch_members,
        )
        conn.commit()

    # Analyze for query planner
    conn.execute("ANALYZE")
    conn.commit()
    conn.close()

    log.info(f"Database build complete: {vote_count} votes, {member_count} member votes")
    log.info(f"Database: {DB_PATH}")


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Download congressional roll call votes (House + Senate)"
    )
    parser.add_argument(
        "--congress", type=int, nargs="+",
        help="Congress numbers to download (default: 118-119)"
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Download all congresses 106-119"
    )
    parser.add_argument(
        "--build-db", action="store_true",
        help="Only rebuild the database from existing JSON (no downloads)"
    )
    parser.add_argument(
        "--skip-db", action="store_true",
        help="Skip database build after downloading"
    )
    args = parser.parse_args()

    if args.build_db:
        build_database()
        return

    api_key = load_config()
    api_session = create_api_session(api_key)
    xml_session = create_xml_session()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.full:
        congresses = list(range(MIN_CONGRESS, MAX_CONGRESS + 1))
    elif args.congress:
        congresses = args.congress
    else:
        congresses = [CURRENT_CONGRESS - 1, CURRENT_CONGRESS]

    log.info("=" * 60)
    log.info("Congressional Roll Call Vote Download")
    log.info(f"Congresses: {congresses}")
    log.info(f"Output: {OUTPUT_DIR}")
    log.info(f"Database: {DB_PATH}")
    log.info("=" * 60)

    # Verify API connection for congresses that need it
    if any(c >= 118 for c in congresses):
        test = api_get(api_session, f"{API_BASE}/house-vote/{CURRENT_CONGRESS}",
                       {"limit": 1})
        if test:
            log.info("Congress.gov API connection verified")
        else:
            log.warning("Congress.gov API connection failed; will use XML fallback for House")

    grand_total = 0
    start_time = time.time()

    for congress in congresses:
        log.info(f"\n--- Congress {congress} ({congress_years(congress)[0]}-{congress_years(congress)[1]}) ---")
        count = download_congress_votes(api_session, xml_session, congress)
        grand_total += count

    elapsed = time.time() - start_time
    log.info("")
    log.info("=" * 60)
    log.info("Download complete")
    log.info(f"  Total votes downloaded: {grand_total}")
    log.info(f"  Elapsed: {elapsed / 60:.1f} minutes")
    log.info("=" * 60)

    # Build database unless skipped
    if not args.skip_db:
        build_database()


if __name__ == "__main__":
    main()
