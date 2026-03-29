#!/usr/bin/env python3
"""
Phase 10: Download and parse eCFR (Electronic Code of Federal Regulations).

Downloads the current regulatory text for 19 CFR titles relevant to
EPA, FDA, USDA/APHIS, FWS, DOE, HUD, DOJ, ED, VA, NASA, SBA, DOT,
DOL, DOC, DHS, NOAA, OSHA, FAA, NHTSA, FEMA and related agencies
from GovInfo bulk data. Parses the XML hierarchy into structured JSON
for database import.

No API key required — direct file downloads from govinfo.gov.

Usage:
    python3 10_ecfr.py              # download all 19 titles
    python3 10_ecfr.py --titles 9   # download specific title(s)
    python3 10_ecfr.py --parse-only # skip download, just re-parse
"""

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from xml.etree.ElementTree import iterparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
GOVINFO_BASE = "https://www.govinfo.gov/bulkdata/ECFR"
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "ecfr"
LOG_DIR = PROJECT_DIR / "logs"

# CFR titles relevant to our agencies
TITLES = {
    7: "Agriculture",
    9: "Animals and Animal Products",
    10: "Energy",
    14: "Aeronautics and Space",
    15: "Commerce and Foreign Trade",
    17: "Commodity and Securities Exchanges",
    20: "Employees' Benefits",
    21: "Food and Drugs",
    24: "Housing and Urban Development",
    28: "Judicial Administration",
    29: "Labor",
    33: "Navigation and Navigable Waters",
    34: "Education",
    38: "Pensions, Bonuses, and Veterans' Relief",
    40: "Protection of Environment",
    44: "Emergency Management and Assistance",
    46: "Shipping",
    49: "Transportation",
    50: "Wildlife and Fisheries",
}

# Agency mapping by CFR title and chapter
AGENCY_MAP = {
    # Title 7: Agriculture (USDA sub-agencies)
    (7, None):       "USDA",    # Subtitle A / Office of the Secretary
    (7, "I"):        "AMS",     # Agricultural Marketing Service
    (7, "II"):       "FNS",     # Food and Nutrition Service
    (7, "III"):      "APHIS",   # Animal and Plant Health Inspection Service
    (7, "IV"):       "RMA",     # Risk Management Agency
    (7, "V"):        "ARS",     # Agricultural Research Service
    (7, "VI"):       "NRCS",    # Natural Resources Conservation Service
    (7, "VII"):      "FSA",     # Farm Service Agency
    (7, "VIII"):     "AMS",     # Grain Inspection (now under AMS)
    (7, "IX"):       "AMS",     # Marketing Orders
    (7, "X"):        "AMS",     # Federal Milk Orders
    (7, "XI"):       "AMS",     # Research & Promotion
    (7, "XIV"):      "CCC",     # Commodity Credit Corporation
    (7, "XV"):       "FAS",     # Foreign Agricultural Service
    (7, "XVII"):     "RUS",     # Rural Utilities Service
    (7, "XVIII"):    "RHS",     # Rural Housing Service
    (7, "XXV"):      "USDA",    # Office of Advocacy and Outreach
    (7, "XXVI"):     "USDA",    # Office of Budget and Program Analysis
    (7, "XXVII"):    "USDA",    # Office of Procurement and Property Management
    (7, "XXVIII"):   "USDA",    # Office of Operations
    (7, "XXIX"):     "USDA",    # Office of Energy Policy
    (7, "XXX"):      "USDA",    # Office of the Chief Economist
    (7, "XXXI"):     "USDA",    # Office of Civil Rights
    (7, "XXXII"):    "USDA",    # Office of the Chief Financial Officer
    (7, "XXXIII"):   "USDA",    # Office of Transportation
    (7, "XXXIV"):    "NIFA",    # National Institute of Food and Agriculture
    (7, "XXXV"):     "RHS",     # Rural Housing Service
    (7, "XXXVI"):    "NASS",    # National Agricultural Statistics Service
    (7, "XXXVII"):   "ERS",     # Economic Research Service
    (7, "XXXVIII"):  "USDA",    # World Agricultural Outlook Board
    (7, "XLII"):     "RBS",     # Rural Business-Cooperative Service
    (7, "L"):        "RBS",     # Rural Business-Cooperative Service
    # Title 9: Animals and Animal Products
    (9, "I"):        "APHIS",
    (9, "II"):       "AMS",     # Packers & Stockyards (now under AMS)
    (9, "III"):      "FSIS",    # Food Safety and Inspection Service
    # Title 10: Energy
    (10, "I"):       "NRC",     # Nuclear Regulatory Commission
    (10, "II"):      "DOE",     # Department of Energy
    (10, "III"):     "DOE",     # Department of Energy
    (10, "X"):       "DOE",     # DOE (includes FERC)
    (10, "XVII"):    "DNFSB",   # Defense Nuclear Facilities Safety Board
    # Title 14: Aeronautics and Space
    (14, "I"):       "FAA",     # Federal Aviation Administration
    (14, "II"):      "FAA",     # Office of Space Transportation
    (14, "III"):     "FAA",     # Commercial Space Transportation
    (14, "V"):       "NASA",    # National Aeronautics and Space Administration
    # Title 15: Commerce and Foreign Trade
    (15, None):      "DOC",     # Department of Commerce (default)
    (15, "I"):       "BIS",     # Bureau of Industry and Security
    (15, "II"):      "NIST",    # National Institute of Standards and Technology
    (15, "III"):     "ITA",     # International Trade Administration
    (15, "IV"):      "FTZ",     # Foreign-Trade Zones Board
    (15, "VII"):     "BIS",     # Bureau of Industry and Security
    (15, "IX"):      "NOAA",    # National Oceanic and Atmospheric Administration
    (15, "XX"):      "NIST",    # Office of NIST
    # Title 17: Commodity and Securities Exchanges
    (17, "I"):       "CFTC",    # Commodity Futures Trading Commission
    (17, "II"):      "SEC",     # Securities and Exchange Commission
    (17, "IV"):      "SEC",     # SEC (specific regulations)
    # Title 20: Employees' Benefits
    (20, "I"):       "SSA",     # Social Security Administration
    (20, "II"):      "SSA",     # Social Security Administration (Office of Hearings)
    (20, "III"):     "SSA",     # Social Security Administration
    (20, "V"):       "DOL",     # Employment and Training Administration
    (20, "VI"):      "DOL",     # Office of Workers' Compensation Programs
    (20, "VII"):     "DOL",     # Benefits Review Board
    (20, "VIII"):    "JBEA",    # Joint Board for Enrollment of Actuaries
    (20, "IX"):      "SSA-OIG", # Office of the Inspector General, SSA
    # Title 21: Food and Drugs
    (21, "I"):       "FDA",
    (21, "II"):      "DEA",     # Drug Enforcement Administration
    (21, "III"):     "ONDCP",   # Office of National Drug Control Policy
    # Title 24: Housing and Urban Development
    (24, None):      "HUD",     # HUD owns all of Title 24
    # Title 28: Judicial Administration
    (28, "I"):       "DOJ",     # Department of Justice
    (28, "III"):     "BOP",     # Federal Prison Industries (Bureau of Prisons)
    (28, "V"):       "BOP",     # Bureau of Prisons
    (28, "VIII"):    "CSOSA",   # Court Services and Offender Supervision Agency
    # Title 29: Labor
    (29, None):      "DOL",     # Department of Labor (default)
    (29, "I"):       "OWCP",    # Office of Workers' Compensation Programs
    (29, "IV"):      "OFCCP",   # Office of Federal Contract Compliance Programs
    (29, "V"):       "DOL-WHD", # Wage and Hour Division
    (29, "X"):       "NMB",     # National Mediation Board
    (29, "XII"):     "FMCS",    # Federal Mediation and Conciliation Service
    (29, "XIV"):     "EEOC",    # Equal Employment Opportunity Commission
    (29, "XVII"):    "OSHA",    # Occupational Safety and Health Administration
    (29, "XX"):      "OSHA",    # OSH Review Commission
    (29, "XXV"):     "PBGC",    # Pension Benefit Guaranty Corporation
    (29, "XXVII"):   "FLRA",    # Federal Labor Relations Authority
    (29, "XL"):      "PBGC",    # Pension Benefit Guaranty Corporation
    # Title 33: Navigation and Navigable Waters
    (33, "I"):       "USCG",    # United States Coast Guard
    (33, "II"):      "USACE",   # US Army Corps of Engineers
    (33, "IV"):      "USACE",   # Army Corps of Engineers (Civil Works)
    # Title 34: Education
    (34, None):      "ED",      # Department of Education
    # Title 38: Pensions, Bonuses, and Veterans' Relief
    (38, "I"):       "VA",      # Department of Veterans Affairs
    # Title 40: Protection of Environment
    (40, None):      "EPA",     # EPA owns all of Title 40
    # Title 44: Emergency Management and Assistance
    (44, "I"):       "FEMA",    # Federal Emergency Management Agency
    (44, "IV"):      "DHS",     # Department of Homeland Security
    # Title 46: Shipping
    (46, "I"):       "USCG",    # United States Coast Guard
    (46, "II"):      "FMC",     # Federal Maritime Commission
    (46, "III"):     "USCG",    # Coast Guard (Proceedings)
    (46, "IV"):      "FMC",     # Federal Maritime Commission
    # Title 49: Transportation
    (49, "A"):       "DOT",     # DOT Office of the Secretary (Subtitle A)
    (49, "I"):       "PHMSA",   # Pipeline and Hazardous Materials Safety Admin
    (49, "II"):      "FRA",     # Federal Railroad Administration
    (49, "III"):     "FHWA",    # Federal Highway Admin / FMCSA
    (49, "V"):       "NHTSA",   # National Highway Traffic Safety Admin
    (49, "VI"):      "FTA",     # Federal Transit Administration
    (49, "VIII"):    "NTSB",    # National Transportation Safety Board
    (49, "X"):       "STB",     # Surface Transportation Board
    (49, "XII"):     "TSA",     # Transportation Security Administration
    # Title 50: Wildlife and Fisheries
    (50, "I"):       "FWS",
    (50, "II"):      "NOAA",    # National Marine Fisheries Service
    (50, "III"):     "NOAA",    # International Fishing Regulations
    (50, "IV"):      "FWS",
    (50, "V"):       "MMC",     # Marine Mammal Commission
    (50, "VI"):      "FWS",
}

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("ecfr")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "ecfr.log")
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
    session.headers.update({"User-Agent": "OpenRegs/1.0 (regulatory data project)"})
    return session


def download_title(session, title_num):
    """Download a single eCFR title XML file."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    url = f"{GOVINFO_BASE}/title-{title_num}/ECFR-title{title_num}.xml"
    outfile = OUTPUT_DIR / f"title{title_num}.xml"

    log.info(f"Downloading Title {title_num} ({TITLES[title_num]})...")
    log.info(f"  URL: {url}")

    resp = session.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    total = int(resp.headers.get("Content-Length", 0))
    downloaded = 0

    with open(outfile, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded * 100 // total
                if pct % 20 == 0 and downloaded > 0:
                    log.info(f"  {downloaded / 1024 / 1024:.1f} MB / {total / 1024 / 1024:.1f} MB ({pct}%)")

    size_mb = outfile.stat().st_size / 1024 / 1024
    log.info(f"  Saved: {outfile} ({size_mb:.1f} MB)")
    return outfile


def get_text(elem):
    """Get all text content from an element, including tail text of children."""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(get_text(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(parts)


def clean_text(text):
    """Clean extracted text: normalize whitespace."""
    if not text:
        return ""
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_section_text(section_elem):
    """Extract readable text from a section element."""
    parts = []
    for elem in section_elem.iter():
        if elem.tag in ('HEAD', 'SECTNO'):
            continue  # Skip headings, we store them separately
        if elem.tag in ('P', 'FP', 'P-1', 'P-2', 'P-3', 'FP-1', 'FP-2'):
            text = get_text(elem)
            if text.strip():
                parts.append(text.strip())
        elif elem.tag == 'EXTRACT':
            text = get_text(elem)
            if text.strip():
                parts.append(text.strip())
    return "\n\n".join(parts)


def parse_title(xml_path, title_num):
    """Parse an eCFR title XML file into structured section records."""
    log.info(f"Parsing Title {title_num} ({TITLES[title_num]})...")

    sections = []
    # Track the current hierarchy as we parse
    hierarchy = {
        "subtitle": "", "chapter": "", "subchapter": "",
        "part": "", "part_name": "", "subpart": "", "subpart_name": "",
    }

    # Use iterparse for memory efficiency (Title 40 is 153 MB)
    context = iterparse(str(xml_path), events=("start", "end"))

    current_path = []
    section_count = 0
    part_auth = ""
    part_source = ""

    for event, elem in context:
        if event == "start":
            current_path.append(elem.tag)
            div_type = elem.get("TYPE", "")

            if elem.tag.startswith("DIV") and div_type:
                identifier = elem.get("N", "")
                node = elem.get("NODE", "")

                if div_type == "SUBTITLE":
                    hierarchy["subtitle"] = identifier
                elif div_type == "CHAPTER":
                    hierarchy["chapter"] = identifier
                    hierarchy["subchapter"] = ""
                elif div_type == "SUBCHAP":
                    hierarchy["subchapter"] = identifier
                elif div_type == "PART":
                    hierarchy["part"] = identifier
                    hierarchy["subpart"] = ""
                    hierarchy["subpart_name"] = ""
                    part_auth = ""
                    part_source = ""
                    # Get part heading
                    head = elem.find("HEAD")
                    hierarchy["part_name"] = clean_text(head.text) if head is not None and head.text else ""
                elif div_type == "SUBPART":
                    hierarchy["subpart"] = identifier
                    head = elem.find("HEAD")
                    hierarchy["subpart_name"] = clean_text(head.text) if head is not None and head.text else ""

        elif event == "end":
            div_type = elem.get("TYPE", "") if elem.tag.startswith("DIV") else ""

            # Capture part-level authority and source
            if div_type == "PART":
                auth_elem = elem.find("AUTH")
                if auth_elem is not None:
                    pspace = auth_elem.find("PSPACE")
                    if pspace is not None:
                        part_auth = clean_text(get_text(pspace))
                source_elem = elem.find("SOURCE")
                if source_elem is not None:
                    pspace = source_elem.find("PSPACE")
                    if pspace is not None:
                        part_source = clean_text(get_text(pspace))

            # Process sections (DIV8 TYPE="SECTION")
            if div_type == "SECTION":
                section_num_elem = elem.find("SECTNO")
                section_num = clean_text(section_num_elem.text) if section_num_elem is not None and section_num_elem.text else ""
                # Strip § symbol
                section_num = section_num.replace("§", "").strip()

                head_elem = elem.find("HEAD")
                heading = clean_text(head_elem.text) if head_elem is not None and head_elem.text else ""

                # Section-level authority
                sec_auth_elem = elem.find(".//SECAUTH")
                sec_auth = ""
                if sec_auth_elem is not None:
                    pspace = sec_auth_elem.find("PSPACE")
                    if pspace is not None:
                        sec_auth = clean_text(get_text(pspace))

                # Amendment citations
                cita_elem = elem.find(".//CITA")
                amendments = clean_text(get_text(cita_elem)) if cita_elem is not None else ""

                # Full text
                full_text = extract_section_text(elem)

                # Determine agency
                chapter = hierarchy["chapter"]
                agency = AGENCY_MAP.get((title_num, chapter))
                if agency is None:
                    agency = AGENCY_MAP.get((title_num, None), "")

                section_id = f"{title_num}:{section_num}" if section_num else f"{title_num}:{elem.get('NODE', '')}"

                sections.append({
                    "section_id": section_id,
                    "title_number": title_num,
                    "title_name": TITLES[title_num],
                    "chapter": hierarchy["chapter"],
                    "subchapter": hierarchy["subchapter"],
                    "part_number": hierarchy["part"],
                    "part_name": hierarchy["part_name"],
                    "subpart": hierarchy["subpart"],
                    "subpart_name": hierarchy["subpart_name"],
                    "section_number": section_num,
                    "section_heading": heading,
                    "agency": agency,
                    "authority": sec_auth or part_auth,
                    "source_citation": part_source,
                    "amendment_citations": amendments,
                    "full_text": full_text,
                })

                section_count += 1
                if section_count % 5000 == 0:
                    log.info(f"  Parsed {section_count} sections...")

                # Free memory
                elem.clear()

            if current_path:
                current_path.pop()

    log.info(f"  Done: {len(sections)} sections from Title {title_num}")

    # Save parsed JSON
    output_file = OUTPUT_DIR / f"title{title_num}_parsed.json"
    with open(output_file, "w") as f:
        json.dump(sections, f)
    log.info(f"  Saved: {output_file}")

    return sections


def main():
    parser = argparse.ArgumentParser(description="Download and parse eCFR regulatory text")
    parser.add_argument("--titles", type=int, nargs="+",
                        help=f"Title numbers to download (default: all {list(TITLES.keys())})")
    parser.add_argument("--parse-only", action="store_true",
                        help="Skip download, just re-parse existing XML files")
    args = parser.parse_args()

    titles = args.titles or list(TITLES.keys())

    # Validate titles
    for t in titles:
        if t not in TITLES:
            log.error(f"Unknown title {t}. Available: {list(TITLES.keys())}")
            sys.exit(1)

    session = create_session()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"{'=' * 60}")
    log.info(f"eCFR Download & Parse")
    log.info(f"Titles: {[f'{t} ({TITLES[t]})' for t in titles]}")
    log.info(f"{'=' * 60}")

    start_time = time.time()
    total_sections = 0

    for title_num in titles:
        xml_path = OUTPUT_DIR / f"title{title_num}.xml"

        # Download
        if not args.parse_only:
            xml_path = download_title(session, title_num)
        elif not xml_path.exists():
            log.error(f"XML file not found: {xml_path}")
            continue

        # Parse
        sections = parse_title(xml_path, title_num)
        total_sections += len(sections)

    elapsed = time.time() - start_time
    log.info(f"\n{'=' * 60}")
    log.info(f"eCFR complete")
    log.info(f"  Total sections: {total_sections:,}")
    log.info(f"  Elapsed: {elapsed / 60:.1f} minutes")
    log.info(f"  Output: {OUTPUT_DIR}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
