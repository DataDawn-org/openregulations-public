#!/usr/bin/env python3
"""
Phase 19: Download GAO (Government Accountability Office) reports from GovInfo.

Downloads MODS XML metadata for ~16,500 GAO reports and Comptroller General
decisions (1994-2008) from the GAOREPORTS collection.

Extracts: title, date, report number, abstract, subjects, document type,
legal references (USC, public laws, statutes at large), PDF/HTML URLs.

Source: GovInfo API (api.govinfo.gov), collection GAOREPORTS
Auth: GovInfo API key (in config.json), or DEMO_KEY

Usage:
    python3 19_gao_reports.py                    # incremental
    python3 19_gao_reports.py --full             # download all ~16,500
    python3 19_gao_reports.py --limit 100        # stop after 100
    python3 19_gao_reports.py --dry-run          # list without downloading
"""

import argparse
import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
GOVINFO_API = "https://api.govinfo.gov"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"

_config = json.loads(CONFIG_FILE.read_text())
API_KEY = _config.get("govinfo_api_key", "DEMO_KEY")
OUTPUT_DIR = PROJECT_DIR / "gao_reports"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "gao_state.json"

PAGE_SIZE = 100
REQUEST_DELAY = 0.5
MODS_NS = {"m": "http://www.loc.gov/mods/v3"}

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("gao")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "gao_reports.log")
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
        status_forcelist=[429, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=4))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (GAO reports)"})
    return session


# === State Management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_modified": "1900-01-01T00:00:00Z", "packages_downloaded": 0}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# === GovInfo API ===
def fetch_package_ids(session, since_date, limit=None):
    """Fetch all GAOREPORTS package IDs using cursor-based pagination."""
    package_ids = []
    seen_ids = set()

    log.info(f"Fetching GAOREPORTS package list (published since {since_date})...")

    url = (
        f"{GOVINFO_API}/published/{since_date}"
        f"?collection=GAOREPORTS&pageSize={PAGE_SIZE}&offsetMark=*&api_key={API_KEY}"
    )

    while url:
        if limit and len(package_ids) >= limit:
            log.info(f"Reached limit of {limit} packages")
            return package_ids[:limit]

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

        next_page = data.get("nextPage")
        if next_page:
            url = f"{next_page}&api_key={API_KEY}"
        else:
            url = None

        time.sleep(REQUEST_DELAY)

    log.info(f"Found {len(package_ids)} total GAO packages")
    return package_ids


def parse_mods(xml_text):
    """Parse MODS XML into structured GAO report data."""
    root = ET.fromstring(xml_text)
    report = {}

    # Title
    title_el = root.find(".//m:titleInfo/m:title", MODS_NS)
    report["title"] = title_el.text.strip() if title_el is not None and title_el.text else ""

    # Date
    date_el = root.find(".//m:originInfo/m:dateIssued", MODS_NS)
    report["date_issued"] = date_el.text.strip() if date_el is not None and date_el.text else ""

    # Abstract
    abstract_el = root.find(".//m:abstract", MODS_NS)
    report["abstract"] = abstract_el.text.strip() if abstract_el is not None and abstract_el.text else ""

    # Pages
    for phys in root.findall(".//m:physicalDescription/m:extent", MODS_NS):
        if phys.text and "p." in phys.text:
            try:
                report["pages"] = int(phys.text.replace("p.", "").strip().rstrip("+"))
            except ValueError:
                pass

    # Extension fields
    for ext in root.findall(".//m:extension", MODS_NS):
        for child in ext:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child.text and child.text.strip():
                if tag == "reportNumber":
                    report["report_number"] = child.text.strip()
                elif tag == "type":
                    report["document_type"] = child.text.strip()
                elif tag == "docClass":
                    report["doc_class"] = child.text.strip()
                elif tag == "accountNo":
                    report["account_number"] = child.text.strip()

    # Subject topics
    subjects = []
    for subj in root.findall(".//m:subject/m:topic", MODS_NS):
        if subj.text and subj.text.strip():
            subjects.append(subj.text.strip())
    # Also check extension subjects
    for ext in root.findall(".//m:extension", MODS_NS):
        for child in ext:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "subject" and child.text and child.text.strip():
                subj_text = child.text.strip()
                if subj_text not in subjects:
                    subjects.append(subj_text)
    report["subjects"] = subjects

    # Legal references
    laws = []
    usc_refs = []
    statute_refs = []

    for ext in root.findall(".//m:extension", MODS_NS):
        for child in ext:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "law":
                congress = child.get("congress", "")
                number = child.get("number", "")
                if congress and number:
                    laws.append(f"Public Law {congress}-{number}")
            elif tag == "USCode":
                title_num = child.get("title", "")
                for section in child:
                    sec_tag = section.tag.split("}")[-1] if "}" in section.tag else section.tag
                    if sec_tag == "section":
                        sec_num = section.get("number", "")
                        sec_detail = section.get("detail", "")
                        usc_refs.append(f"{title_num} U.S.C. {sec_num}{sec_detail}")
            elif tag == "statuteAtLarge":
                vol = child.get("volume", "")
                for pages in child:
                    p = pages.get("pages", "")
                    if vol and p:
                        statute_refs.append(f"{vol} Stat. {p}")

    # Also from relatedItem
    for related in root.findall(".//m:relatedItem", MODS_NS):
        for ident in related.findall(".//m:identifier", MODS_NS):
            id_type = ident.get("type", "")
            if ident.text:
                if "public law" in id_type.lower():
                    if ident.text not in laws:
                        laws.append(ident.text)
                elif "usc" in id_type.lower():
                    if ident.text not in usc_refs:
                        usc_refs.append(ident.text)
                elif "statute" in id_type.lower():
                    if ident.text not in statute_refs:
                        statute_refs.append(ident.text)

    report["public_laws"] = laws
    report["usc_references"] = usc_refs
    report["statute_references"] = statute_refs

    # Preferred citation
    for ident in root.findall(".//m:identifier", MODS_NS):
        if ident.get("type") == "preferred citation" and ident.text:
            report["citation"] = ident.text.strip()

    # URLs
    for loc in root.findall(".//m:location/m:url", MODS_NS):
        label = loc.get("displayLabel", "")
        if "PDF" in label and loc.text:
            report["pdf_url"] = loc.text
        elif "HTML" in label and loc.text:
            report["html_url"] = loc.text
        elif "Content Detail" in label and loc.text:
            report["detail_url"] = loc.text

    # SuDoc number
    for cls in root.findall(".//m:classification", MODS_NS):
        if cls.get("authority") == "sudocs" and cls.text:
            report["sudocs"] = cls.text.strip()

    return report


def download_report(session, package_id):
    """Download and parse MODS XML for a single GAO report."""
    url = f"{GOVINFO_API}/packages/{package_id}/mods?api_key={API_KEY}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    report = parse_mods(resp.text)
    report["package_id"] = package_id
    return report


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download GAO reports from GovInfo")
    parser.add_argument("--full", action="store_true", help="Download all reports")
    parser.add_argument("--limit", type=int, help="Stop after N reports")
    parser.add_argument("--dry-run", action="store_true", help="List without downloading")
    args = parser.parse_args()

    state = load_state()
    session = create_session()

    since_date = "1900-01-01" if args.full else state["last_modified"].split("T")[0]

    log.info(f"=== GAO Reports Download ===")
    log.info(f"Since: {since_date}")

    # Fetch package list
    package_ids = fetch_package_ids(session, since_date, limit=args.limit)

    if args.dry_run:
        for pkg in package_ids[:20]:
            log.info(f"  {pkg['packageId']}: {pkg['title'][:80]}")
        log.info(f"Total: {len(package_ids)} packages (dry run)")
        return

    # Check existing
    existing = {f.stem for f in OUTPUT_DIR.glob("*.json")}
    to_download = [p for p in package_ids if p["packageId"] not in existing]
    log.info(f"Already have {len(existing)} reports, {len(to_download)} new to download")

    if args.limit:
        to_download = to_download[:args.limit]

    # Download
    downloaded = 0
    errors = 0
    max_modified = state["last_modified"]

    for i, pkg in enumerate(to_download):
        pid = pkg["packageId"]
        try:
            report = download_report(session, pid)
            out_file = OUTPUT_DIR / f"{pid}.json"
            out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False))
            downloaded += 1

            if pkg["lastModified"] > max_modified:
                max_modified = pkg["lastModified"]

            if downloaded % 100 == 0:
                log.info(f"  Progress: {downloaded}/{len(to_download)} downloaded, {errors} errors")
                state["last_modified"] = max_modified
                state["packages_downloaded"] = len(existing) + downloaded
                save_state(state)

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.warning(f"  {pid}: 404")
            else:
                log.error(f"  {pid}: HTTP error: {e}")
            errors += 1
        except Exception as e:
            log.error(f"  {pid}: Error: {e}")
            errors += 1

        time.sleep(REQUEST_DELAY)

    # Final state
    state["last_modified"] = max_modified
    state["packages_downloaded"] = len(existing) + downloaded
    save_state(state)

    log.info(f"=== Complete ===")
    log.info(f"Downloaded: {downloaded}")
    log.info(f"Errors: {errors}")
    log.info(f"Total on disk: {len(existing) + downloaded}")


if __name__ == "__main__":
    main()
