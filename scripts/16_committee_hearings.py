#!/usr/bin/env python3
"""
Phase 16: Download Congressional Committee Hearing metadata from GovInfo.

Downloads MODS XML metadata for all Congressional Hearings (CHRG collection)
from the GovInfo API. Extracts structured data including:
  - Hearing title, date, chamber, congress, committee
  - Committee members present (matchable to bioguide_id)
  - Witnesses who testified (name, title, organization)
  - Bill/legislation references
  - Statutory/USC references

This creates the connective tissue between lobbying, legislation, and members:
a lobbyist registered on issue X testifies before committee Y about bill Z.

Source: GovInfo API (api.govinfo.gov)
  - Collection listing: /collections/CHRG
  - Per-package MODS: /packages/{packageId}/mods

No API key required (DEMO_KEY works). ~46,000 hearings total.

Usage:
    python3 16_committee_hearings.py                    # incremental (new since last run)
    python3 16_committee_hearings.py --full             # download all ~46K hearings
    python3 16_committee_hearings.py --since 2020-01-01 # from a specific date
    python3 16_committee_hearings.py --congress 118     # specific congress only
"""

import argparse
import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
GOVINFO_API = "https://api.govinfo.gov"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"

# Load API key from config
_config = json.loads(CONFIG_FILE.read_text())
API_KEY = _config.get("govinfo_api_key", "DEMO_KEY")
OUTPUT_DIR = PROJECT_DIR / "hearings"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "hearings_state.json"

PAGE_SIZE = 100  # max per GovInfo API page
REQUEST_DELAY = 0.5  # seconds between API calls (be polite)
MODS_NS = {"m": "http://www.loc.gov/mods/v3"}

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("hearings")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "hearings.log")
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
    """Create requests session with retry logic."""
    session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=4))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (committee hearings project)"})
    return session


# === State Management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_modified": "1900-01-01T00:00:00Z", "packages_downloaded": 0}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# === GovInfo API ===
def fetch_package_ids(session, since_dt, limit=None):
    """Fetch all CHRG package IDs using the published endpoint.

    Uses cursor-based pagination (offsetMark) which has no 10K limit,
    unlike the collections endpoint's numeric offset pagination.
    """
    package_ids = []
    seen_ids = set()

    # Convert since_dt to date for the published endpoint
    since_date = since_dt.split("T")[0]
    log.info(f"Fetching CHRG package list (published since {since_date})...")

    url = (
        f"{GOVINFO_API}/published/{since_date}"
        f"?collection=CHRG&pageSize={PAGE_SIZE}&offsetMark=*&api_key={API_KEY}"
    )

    while url:
        if limit and len(package_ids) >= limit:
            log.info(f"Reached limit of {limit} packages")
            return package_ids

        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        packages = data.get("packages", [])
        if not packages:
            break

        for pkg in packages:
            pid = pkg.get("packageId")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                package_ids.append({
                    "packageId": pid,
                    "lastModified": pkg.get("lastModified", ""),
                    "title": pkg.get("title", ""),
                })

        total = data.get("count", "?")
        log.info(f"  Fetched {len(package_ids)} / {total} package IDs")

        # Cursor-based pagination — nextPage URL includes the offsetMark
        next_page = data.get("nextPage")
        if next_page:
            url = f"{next_page}&api_key={API_KEY}"
        else:
            url = None

        time.sleep(REQUEST_DELAY)

    log.info(f"Found {len(package_ids)} total hearing packages")
    return package_ids


def parse_mods(xml_text):
    """Parse MODS XML into structured hearing data."""
    root = ET.fromstring(xml_text)
    hearing = {}

    # Title
    title_el = root.find(".//m:titleInfo/m:title", MODS_NS)
    hearing["title"] = title_el.text.strip() if title_el is not None and title_el.text else ""

    # Date
    date_el = root.find(".//m:originInfo/m:dateIssued", MODS_NS)
    hearing["date_issued"] = date_el.text.strip() if date_el is not None and date_el.text else ""

    # Extension fields (congress, session, chamber, etc.)
    for ext in root.findall(".//m:extension", MODS_NS):
        for child in ext:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child.text and child.text.strip():
                if tag == "congress":
                    hearing["congress"] = int(child.text.strip())
                elif tag == "session":
                    hearing["session"] = child.text.strip()
                elif tag == "chamber":
                    hearing["chamber"] = child.text.strip()
                elif tag == "collectionCode":
                    hearing["collection"] = child.text.strip()

    # Chamber from docClass if not in extension
    if "chamber" not in hearing:
        for ext in root.findall(".//m:extension", MODS_NS):
            for child in ext:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag == "waisDatabaseName" and child.text:
                    if "senate" in child.text.lower():
                        hearing["chamber"] = "Senate"
                    elif "house" in child.text.lower():
                        hearing["chamber"] = "House"

    # Pages
    for phys in root.findall(".//m:physicalDescription/m:extent", MODS_NS):
        if phys.text and "p." in phys.text:
            try:
                hearing["pages"] = int(phys.text.replace("p.", "").strip())
            except ValueError:
                pass

    # Committee — corporate name with role "associated name"
    committees = []
    for name in root.findall(".//m:name[@type='corporate']", MODS_NS):
        roles = [r.text for r in name.findall(".//m:roleTerm[@type='text']", MODS_NS) if r.text]
        if "associated name" in roles:
            parts = [p.text for p in name.findall("m:namePart", MODS_NS) if p.text]
            # Filter out "United States" and "Congress" to get committee name
            committee_parts = [p for p in parts if p not in ("United States", "Congress", "Senate", "House of Representatives", "House")]
            if committee_parts:
                # Also capture the chamber from the hierarchy
                chamber_from_parts = None
                if "Senate" in parts:
                    chamber_from_parts = "Senate"
                elif "House of Representatives" in parts or "House" in parts:
                    chamber_from_parts = "House"
                committees.append({
                    "name": ", ".join(committee_parts),
                    "chamber": chamber_from_parts,
                    "full_hierarchy": " > ".join(parts),
                })
    hearing["committees"] = committees

    # Members — personal names with description "United States Congressional Member"
    members = []
    for name in root.findall(".//m:name[@type='personal']", MODS_NS):
        desc = name.find("m:description", MODS_NS)
        if desc is not None and desc.text == "United States Congressional Member":
            parts = [p.text for p in name.findall("m:namePart", MODS_NS) if p.text]
            roles = [r.text for r in name.findall(".//m:roleTerm[@type='text']", MODS_NS) if r.text]
            members.append({
                "name": " ".join(parts),
                "role": roles[0] if roles else "committee member",
            })
    hearing["members"] = members

    # Witnesses — personal names with description "Hearing Witness"
    witnesses = []
    for name in root.findall(".//m:name[@type='personal']", MODS_NS):
        desc = name.find("m:description", MODS_NS)
        if desc is not None and desc.text == "Hearing Witness":
            parts = [p.text for p in name.findall("m:namePart", MODS_NS) if p.text]
            # The namePart often contains "LastName, FirstName, title, org, location"
            raw = ", ".join(parts)
            witnesses.append({"raw": raw})
    hearing["witnesses"] = witnesses

    # Bill references — relatedItem with identifier matching bill patterns
    bills = []
    statutes = []
    for related in root.findall(".//m:relatedItem", MODS_NS):
        id_el = related.find(".//m:identifier", MODS_NS)
        if id_el is not None and id_el.text:
            ref = id_el.text.strip()
            # Bill pattern: S. 1234, H.R. 5678, S.J.Res. 12, etc.
            if any(ref.startswith(p) for p in ("S.", "H.R.", "H.J.Res", "S.J.Res", "H.Con.Res", "S.Con.Res", "H.Res", "S.Res")):
                bills.append(ref)
            elif "U.S.C." in ref or "Public Law" in ref:
                statutes.append(ref)
    hearing["bill_references"] = bills
    hearing["statute_references"] = statutes

    # URLs
    for loc in root.findall(".//m:location/m:url", MODS_NS):
        label = loc.get("displayLabel", "")
        if "PDF" in label and loc.text:
            hearing["pdf_url"] = loc.text
        elif "HTML" in label and loc.text:
            hearing["html_url"] = loc.text
        elif "Content Detail" in label and loc.text:
            hearing["detail_url"] = loc.text

    return hearing


def download_hearing(session, package_id):
    """Download and parse MODS XML for a single hearing."""
    url = f"{GOVINFO_API}/packages/{package_id}/mods?api_key={API_KEY}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    hearing = parse_mods(resp.text)
    hearing["package_id"] = package_id
    return hearing


def parse_witness(raw):
    """Parse a witness raw string into name, title, org, location.

    Two MODS formats exist:
      Senate: "LastName, FirstName" (just name, sometimes with "Prepared statement")
      House:  "FirstName LastName, Credentials, Title, Organization, City, STATE"

    Detection heuristic: if the first comma-segment contains a space, it's
    "FirstName LastName" (House format). If not, it's "LastName" (Senate format)
    and the second segment is the first name.
    """
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 2:
        return {"name": raw, "title": "", "organization": "", "location": ""}

    # Detect format: does the first segment contain a space? (House = full name)
    first_has_space = " " in parts[0].strip()

    if not first_has_space and len(parts) >= 2:
        # Senate format: "LastName, FirstName [Prepared statement]"
        first_name = parts[1].replace("Prepared statement", "").strip()
        name = f"{first_name} {parts[0]}".strip()
        rest = parts[2:]
    else:
        # House format: "FirstName LastName, credentials, title, org..."
        name = parts[0]
        rest = parts[1:]

    # Classify remaining parts into credentials, title, org, location
    title_parts = []
    org_parts = []
    location_parts = []

    # Check if last part looks like a US state code (2 uppercase letters)
    if len(rest) >= 2 and len(rest[-1]) == 2 and rest[-1].isupper():
        location_parts = rest[-2:]  # city, STATE
        rest = rest[:-2]

    # Credentials: short items like "M.D.", "Ph.D.", "M.B.A.", "M.P.H.", "Esq."
    # Title: contains role keywords
    # Org suffixes: "Inc.", "LLC", "Ltd.", "Corp.", "Co." — attach to org, not title
    # Everything else: organization
    _role_kw = ("president", "director", "chairman", "chairwoman", "chief",
                "professor", "secretary", "commissioner", "vice", "officer",
                "counsel", "manager", "administrator", "executive", "founder",
                "co-founder", "ceo", "cfo", "coo", "cto", "former", "acting",
                "deputy", "assistant", "associate", "senior", "head", "member",
                "superintendent", "sheriff", "captain", "lieutenant", "sergeant",
                "inspector", "analyst", "advocate", "coordinator")
    _org_suffixes = ("inc.", "inc", "llc", "llc.", "ltd.", "ltd", "corp.", "corp",
                     "co.", "l.p.", "p.c.", "p.a.", "l.l.c.", "l.l.p.")

    for part in rest:
        p_lower = part.lower().strip()
        # Org suffix: attach to previous org part or start org
        if p_lower in _org_suffixes:
            if org_parts:
                org_parts[-1] = org_parts[-1] + ", " + part
            else:
                org_parts.append(part)
        # Credential: short, contains dots (but not org suffixes)
        elif len(part) <= 10 and ("." in part or p_lower in ("esq", "ret", "usn", "usa", "usaf", "usmc")):
            title_parts.append(part)
        elif any(kw in p_lower for kw in _role_kw):
            title_parts.append(part)
        else:
            org_parts.append(part)

    return {
        "name": name,
        "title": ", ".join(title_parts),
        "organization": ", ".join(org_parts),
        "location": ", ".join(location_parts),
    }


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download committee hearing metadata from GovInfo")
    parser.add_argument("--full", action="store_true", help="Download all hearings from the beginning")
    parser.add_argument("--since", type=str, help="Download hearings modified since this date (YYYY-MM-DD)")
    parser.add_argument("--congress", type=int, help="Only download hearings from this congress number")
    parser.add_argument("--limit", type=int, help="Stop after downloading this many hearings (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="List packages without downloading")
    args = parser.parse_args()

    state = load_state()
    session = create_session()

    # Determine start date
    if args.full:
        since_dt = "1900-01-01T00:00:00Z"
    elif args.since:
        since_dt = f"{args.since}T00:00:00Z"
    else:
        since_dt = state["last_modified"]

    log.info(f"=== Committee Hearings Download ===")
    log.info(f"Since: {since_dt}")

    # Fetch package list
    package_ids = fetch_package_ids(session, since_dt, limit=args.limit)

    if args.congress:
        # Filter by congress — package IDs contain congress number (e.g., CHRG-118shrg12345)
        filtered = []
        for pkg in package_ids:
            pid = pkg["packageId"]
            # Extract congress number from package ID: CHRG-{congress}{chamber}hrg{number}
            try:
                congress_str = pid.split("-")[1][:3]
                congress_num = int(congress_str.rstrip("shHSr"))
                if congress_num == args.congress:
                    filtered.append(pkg)
            except (IndexError, ValueError):
                filtered.append(pkg)  # keep if we can't parse
        log.info(f"Filtered to {len(filtered)} packages for congress {args.congress}")
        package_ids = filtered

    if args.dry_run:
        for pkg in package_ids[:20]:
            log.info(f"  {pkg['packageId']}: {pkg['title'][:80]}")
        log.info(f"Total: {len(package_ids)} packages (dry run, not downloading)")
        return

    # Check which packages we already have
    existing = set()
    for f in OUTPUT_DIR.glob("*.json"):
        existing.add(f.stem)

    to_download = [p for p in package_ids if p["packageId"] not in existing]
    log.info(f"Already have {len(existing)} hearings, {len(to_download)} new to download")

    if args.limit:
        to_download = to_download[:args.limit]
        log.info(f"Limited to {args.limit} downloads")

    # Download and parse each hearing
    downloaded = 0
    errors = 0
    max_modified = state["last_modified"]

    for i, pkg in enumerate(to_download):
        pid = pkg["packageId"]
        try:
            hearing = download_hearing(session, pid)

            # Parse witnesses
            parsed_witnesses = []
            for w in hearing.get("witnesses", []):
                parsed = parse_witness(w["raw"])
                parsed["raw"] = w["raw"]
                parsed_witnesses.append(parsed)
            hearing["witnesses"] = parsed_witnesses

            # Save
            out_file = OUTPUT_DIR / f"{pid}.json"
            out_file.write_text(json.dumps(hearing, indent=2, ensure_ascii=False))

            downloaded += 1

            # Track latest modification time
            if pkg["lastModified"] > max_modified:
                max_modified = pkg["lastModified"]

            if downloaded % 50 == 0:
                log.info(f"  Progress: {downloaded}/{len(to_download)} downloaded, {errors} errors")
                state["last_modified"] = max_modified
                state["packages_downloaded"] = len(existing) + downloaded
                save_state(state)

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.warning(f"  {pid}: 404 Not Found (skipping)")
            else:
                log.error(f"  {pid}: HTTP error: {e}")
            errors += 1
        except Exception as e:
            log.error(f"  {pid}: Error: {e}")
            errors += 1

        time.sleep(REQUEST_DELAY)

    # Final state save
    state["last_modified"] = max_modified
    state["packages_downloaded"] = len(existing) + downloaded
    save_state(state)

    log.info(f"=== Complete ===")
    log.info(f"Downloaded: {downloaded}")
    log.info(f"Errors: {errors}")
    log.info(f"Total on disk: {len(existing) + downloaded}")


if __name__ == "__main__":
    main()
