#!/usr/bin/env python3
"""
Phase 11: Download the Congressional Record (CREC) from GovInfo.

Downloads daily Congressional Record packages (1994-present) from GovInfo.
Extracts HTML text and MODS XML metadata for each granule (speech, debate,
procedural action). Skips PDF files to save bandwidth (~95% size reduction).

This is the core dataset for the "Open Floor" product — full text of
everything said on the floor of Congress.

No API key required — direct ZIP downloads from govinfo.gov.

Usage:
    python3 11_congressional_record.py                     # current + previous congress
    python3 11_congressional_record.py --years 2024 2025   # specific years
    python3 11_congressional_record.py --full               # all years (1994-present)
    python3 11_congressional_record.py --since 2020-01-01   # from a date forward
"""

import argparse
import io
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
GOVINFO_BASE = "https://www.govinfo.gov"
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "congressional_record"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "crec_state.json"

# Download settings
WORKERS = 4  # parallel ZIP downloads
SITEMAP_YEARS = list(range(1994, 2027))

# Congress dates for mapping years to congress numbers
# Each congress spans 2 years starting January 3 of odd years
def year_to_congress(year):
    return (year - 1789) // 2 + 1

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("crec")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "crec.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


class GovInfoRetry(Retry):
    """Custom Retry that handles GovInfo's malformed 'Retry-After: 30, 15' header.

    GovInfo (via Cloudflare) sometimes sends a Retry-After header with two
    comma-separated values like '30, 15', which violates RFC 7231 and causes
    urllib3 to raise InvalidHeader. This subclass parses the first value instead.
    """

    def parse_retry_after(self, retry_after):
        if retry_after is None:
            return None
        # GovInfo sometimes sends "30, 15" — take the first (larger) value
        if "," in str(retry_after):
            retry_after = str(retry_after).split(",")[0].strip()
        return super().parse_retry_after(retry_after)


def create_session():
    """Create requests session with retry logic."""
    session = requests.Session()
    retries = GovInfoRetry(
        total=5, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=WORKERS + 2))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (congressional record project)"})
    return session


def load_state():
    """Load download progress state."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"completed_dates": [], "completed_years": []}


def save_state(state):
    """Save download progress state."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def discover_packages_from_sitemap(session, year):
    """Get all CREC package IDs for a year from the GovInfo sitemap.

    Returns a list of (date_str, package_id) tuples. Most dates have a simple
    package ID like 'CREC-2025-01-07', but some have variant suffixes:
      - Volume variants:  CREC-2012-01-03-v157, CREC-2012-01-03-v158
      - Issue variants:   CREC-2009-12-18-i193, CREC-2009-12-18-i194
    These variant packages do NOT exist at the plain date URL, so we must
    use the full package ID for the download.
    """
    url = f"{GOVINFO_BASE}/sitemap/CREC_{year}_sitemap.xml"
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Sitemap error for {year}: {e}")
        return []

    # Parse sitemap XML for package URLs
    # Capture full package ID including any variant suffix (-v123, -i45, -pt1)
    packages = {}  # date_str -> list of package_ids
    pattern = re.compile(r'CREC-(\d{4}-\d{2}-\d{2})(-[a-z]\w+)?')

    def _extract(text):
        for match in pattern.finditer(text):
            date_str = match.group(1)
            suffix = match.group(2) or ""
            pkg_id = f"CREC-{date_str}{suffix}"
            packages.setdefault(date_str, set()).add(pkg_id)

    try:
        root = ET.fromstring(resp.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for loc in root.findall(".//sm:loc", ns):
            _extract(loc.text or "")
    except ET.ParseError:
        _extract(resp.text)

    # Build result: for each date, list its package IDs
    result = []
    for date_str in sorted(packages):
        for pkg_id in sorted(packages[date_str]):
            result.append((date_str, pkg_id))
    return result


def parse_mods_metadata(mods_xml):
    """Parse MODS metadata from a CREC package."""
    ns = {"mods": "http://www.loc.gov/mods/v3"}

    try:
        root = ET.fromstring(mods_xml)
    except ET.ParseError:
        return None, []

    # Package-level metadata
    package = {}
    date_elem = root.find(".//mods:dateIssued", ns)
    if date_elem is not None and date_elem.text:
        package["date"] = date_elem.text
        # Derive congress/session from date
        try:
            year = int(date_elem.text[:4])
            package["congress"] = str(year_to_congress(year))
            package["session"] = "1" if year % 2 == 1 else "2"
        except (ValueError, IndexError):
            pass

    # Per-granule metadata (relatedItem type="constituent")
    granules = []
    for item in root.findall(".//mods:relatedItem[@type='constituent']", ns):
        granule = {}

        # Title
        title_elem = item.find("mods:titleInfo/mods:title", ns)
        granule["title"] = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

        # Chamber (partName)
        part_elem = item.find("mods:titleInfo/mods:partName", ns)
        granule["chamber_label"] = part_elem.text.strip() if part_elem is not None and part_elem.text else ""

        # Extension fields (all children are in MODS namespace)
        item_ext = item.find("mods:extension", ns)
        if item_ext is not None:
            granule["granule_class"] = getattr(item_ext.find("mods:granuleClass", ns), "text", "") or ""
            granule["sub_granule_class"] = getattr(item_ext.find("mods:subGranuleClass", ns), "text", "") or ""
            granule["page_prefix"] = getattr(item_ext.find("mods:pagePrefix", ns), "text", "") or ""
            granule["chamber"] = getattr(item_ext.find("mods:chamber", ns), "text", "") or ""
            granule["granule_date"] = getattr(item_ext.find("mods:granuleDate", ns), "text", "") or ""

            # Bill references
            bills = []
            for bill_elem in item_ext.findall("mods:bill", ns):
                bills.append({
                    "congress": bill_elem.get("congress", ""),
                    "type": bill_elem.get("type", ""),
                    "number": bill_elem.get("number", ""),
                })
            if bills:
                granule["bills"] = bills

            # Congressional member references (bioguide_id linkage)
            cong_members = []
            for cm in item_ext.findall("mods:congMember", ns):
                member = {
                    "bioguide_id": cm.get("bioGuideId", ""),
                    "chamber": cm.get("chamber", ""),
                    "congress": cm.get("congress", ""),
                    "party": cm.get("party", ""),
                    "role": cm.get("role", ""),
                    "state": cm.get("state", ""),
                }
                # Extract name forms
                for name_elem in cm.findall("mods:name", ns):
                    ntype = name_elem.get("type", "")
                    if ntype == "parsed":
                        member["parsed_name"] = (name_elem.text or "").strip()
                    elif ntype == "authority-fnf":
                        member["authority_name"] = (name_elem.text or "").strip()
                    elif ntype == "authority-lnf":
                        member["authority_lnf"] = (name_elem.text or "").strip()
                if member["bioguide_id"]:
                    cong_members.append(member)
            if cong_members:
                granule["cong_members"] = cong_members

        # Page range
        page_ext = item.find("mods:part[@type='article']/mods:extent[@unit='pages']", ns)
        if page_ext is not None:
            start_elem = page_ext.find("mods:start", ns)
            end_elem = page_ext.find("mods:end", ns)
            granule["page_start"] = start_elem.text if start_elem is not None else ""
            granule["page_end"] = end_elem.text if end_elem is not None else ""

        # Speakers
        speakers = []
        for name_elem in item.findall("mods:name[@type='personal']", ns):
            name_part = name_elem.find("mods:namePart", ns)
            role = name_elem.find("mods:role/mods:roleTerm", ns)
            if name_part is not None and name_part.text:
                speaker = {"name": name_part.text.strip()}
                if role is not None and role.text:
                    speaker["role"] = role.text.strip()
                speakers.append(speaker)
        if speakers:
            granule["speakers"] = speakers

        # Granule ID — prefer ID attribute on relatedItem, fallback to URI
        rid = item.get("ID", "")
        if rid.startswith("id-"):
            granule["granule_id"] = rid[3:]

        # Identifier fields (citation, URI fallback for granule_id)
        for id_elem in item.findall("mods:identifier", ns):
            id_type = id_elem.get("type", "")
            if "citation" in id_type:
                granule["citation"] = id_elem.text
            elif id_type == "uri" and "granule_id" not in granule:
                uri = id_elem.text or ""
                match = re.search(r'/(?:details|granule)/CREC-[\d-]+/(CREC-[^/]+)', uri)
                if match:
                    granule["granule_id"] = match.group(1)

        # Location (link to HTML content)
        for loc in item.findall("mods:location/mods:url", ns):
            if loc.get("displayLabel") == "Content Detail":
                granule["detail_url"] = loc.text

        granules.append(granule)

    return package, granules


def extract_html_text(html_content):
    """Extract text from CREC HTML file (text is in <pre> tags)."""
    if not html_content:
        return ""
    # Strip HTML tags but preserve line breaks from <pre>
    text = re.sub(r'<[^>]+>', '', html_content)
    # Clean up whitespace but preserve paragraph structure
    lines = text.split('\n')
    cleaned = []
    for line in lines:
        stripped = line.rstrip()
        if stripped:
            cleaned.append(stripped)
        elif cleaned and cleaned[-1] != "":
            cleaned.append("")  # Preserve paragraph breaks
    return "\n".join(cleaned).strip()


def download_and_process_date(session, date_str, output_dir, package_id=None):
    """Download and process a single CREC daily package."""
    if package_id is None:
        package_id = f"CREC-{date_str}"

    zip_url = f"{GOVINFO_BASE}/content/pkg/{package_id}.zip"

    try:
        resp = session.get(zip_url, timeout=120)
        if resp.status_code == 404:
            return None
        # GovInfo redirects to /error for missing packages (302 -> 200 HTML)
        if "/error" in resp.url or resp.headers.get("content-type", "").startswith("text/html"):
            log.warning(f"Package not found (redirected to error): {package_id}")
            return None
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Download error for {package_id}: {e}")
        return None

    # Process ZIP in memory (don't save the whole ZIP)
    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        log.error(f"Bad ZIP for {date_str}")
        return None

    result = {
        "date": date_str,
        "package_id": package_id,
        "granules": [],
    }

    # Find and parse MODS metadata
    mods_files = [n for n in zf.namelist() if n.endswith("mods.xml")]
    package_meta = {}
    granule_metadata = {}

    for mods_file in mods_files:
        mods_xml = zf.read(mods_file).decode("utf-8", errors="replace")
        pkg, granules = parse_mods_metadata(mods_xml)
        if pkg:
            package_meta = pkg
        for g in granules:
            gid = g.get("granule_id", "")
            if gid:
                granule_metadata[gid] = g

    result["congress"] = package_meta.get("congress")
    result["session"] = package_meta.get("session")
    result["volume"] = package_meta.get("volume")
    result["issue"] = package_meta.get("issue")

    # Extract HTML content (skip PDFs)
    html_files = [n for n in zf.namelist() if n.endswith(".htm")]

    for html_file in html_files:
        # Extract granule ID from filename
        basename = Path(html_file).stem  # e.g., CREC-2025-01-07-pt1-PgH51-6
        html_content = zf.read(html_file).decode("utf-8", errors="replace")
        text = extract_html_text(html_content)

        # Match with MODS metadata
        meta = granule_metadata.get(basename, {})

        granule = {
            "granule_id": basename,
            "date": date_str,
            "congress": result.get("congress"),
            "session": result.get("session"),
            "volume": result.get("volume"),
            "issue": result.get("issue"),
            "title": meta.get("title", ""),
            "chamber": meta.get("chamber", meta.get("granule_class", "")),
            "granule_class": meta.get("granule_class", ""),
            "sub_granule_class": meta.get("sub_granule_class", ""),
            "page_start": meta.get("page_start", ""),
            "page_end": meta.get("page_end", ""),
            "speakers": meta.get("speakers", []),
            "bills": meta.get("bills", []),
            "citation": meta.get("citation", ""),
            "text": text,
        }
        result["granules"].append(granule)

    return result


def process_year(session, year, state):
    """Download and process all CREC packages for a year."""
    year_dir = OUTPUT_DIR / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    year_str = str(year)
    if year_str in state.get("completed_years", []):
        log.info(f"Year {year} already completed, skipping")
        return 0

    # Discover packages from sitemap (includes variant suffixes)
    packages = discover_packages_from_sitemap(session, year)
    if not packages:
        log.warning(f"No packages found for {year}")
        return 0

    unique_dates = sorted(set(d for d, _ in packages))
    log.info(f"Year {year}: {len(unique_dates)} session days, {len(packages)} packages found")

    # Filter to incomplete packages (track by package_id, not just date)
    completed_pkgs = set(state.get("completed_packages", []))
    # Also check legacy completed_dates for backward compat
    completed_dates = set(state.get("completed_dates", []))
    pending = [(d, pid) for d, pid in packages
               if pid not in completed_pkgs and d not in completed_dates]

    if not pending:
        log.info(f"Year {year}: all {len(packages)} packages already processed")
        if year_str not in state.get("completed_years", []):
            state.setdefault("completed_years", []).append(year_str)
            save_state(state)
        return 0

    log.info(f"Year {year}: {len(pending)} packages to download ({len(packages) - len(pending)} cached)")

    total_granules = 0
    successful_pkgs = []
    failed_count = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for date_str, pkg_id in pending:
            f = pool.submit(download_and_process_date, session, date_str, year_dir, package_id=pkg_id)
            futures[f] = (date_str, pkg_id)

        for i, future in enumerate(as_completed(futures)):
            date_str, pkg_id = futures[future]
            try:
                result = future.result()
            except Exception as e:
                log.error(f"Error processing {pkg_id}: {e}")
                failed_count += 1
                continue  # Do NOT mark as completed — will retry next run

            if result and result["granules"]:
                # Save as JSON (one file per day, or per package for variants)
                if pkg_id.startswith(f"CREC-{date_str}") and pkg_id != f"CREC-{date_str}":
                    # Variant package — save with full package ID
                    out_file = year_dir / f"{pkg_id}.json"
                else:
                    out_file = year_dir / f"{date_str}.json"
                with open(out_file, "w") as f:
                    json.dump(result, f)
                total_granules += len(result["granules"])
                successful_pkgs.append(pkg_id)
            elif result is not None:
                # Downloaded OK but no granules (empty package) — mark complete
                successful_pkgs.append(pkg_id)
            # result is None means download failed silently — do NOT mark complete

            if (i + 1) % 10 == 0:
                state.setdefault("completed_packages", []).extend(successful_pkgs)
                successful_pkgs = []
                save_state(state)
                log.info(f"  {year}: {i + 1}/{len(pending)} packages processed, "
                         f"{total_granules} granules, {failed_count} failed")

    # Save remaining successful
    if successful_pkgs:
        state.setdefault("completed_packages", []).extend(successful_pkgs)
        save_state(state)

    # Mark year complete only if ALL packages succeeded (none failed)
    if failed_count == 0:
        all_pkg_ids = set(pid for _, pid in packages)
        done_pkgs = set(state.get("completed_packages", []))
        done_dates = set(state.get("completed_dates", []))
        all_done = all(pid in done_pkgs or d in done_dates for d, pid in packages)
        if all_done and year_str not in state.get("completed_years", []):
            state.setdefault("completed_years", []).append(year_str)
            save_state(state)

    log.info(f"Year {year}: {total_granules} granules from {len(pending)} packages"
             f" ({failed_count} failed)")
    return total_granules


def main():
    parser = argparse.ArgumentParser(description="Download Congressional Record from GovInfo")
    parser.add_argument("--years", type=int, nargs="+",
                        help="Years to download (default: current + previous year)")
    parser.add_argument("--full", action="store_true",
                        help="Download all years (1994-present)")
    parser.add_argument("--since", type=str,
                        help="Download from a date forward (YYYY-MM-DD)")
    parser.add_argument("--reset", action="store_true",
                        help="Reset download state")
    args = parser.parse_args()

    session = create_session()

    if args.reset:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        log.info("State reset")

    state = load_state()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Determine years to download
    current_year = datetime.now().year
    if args.full:
        years = list(range(1994, current_year + 1))
    elif args.since:
        start_year = int(args.since[:4])
        years = list(range(start_year, current_year + 1))
    elif args.years:
        years = args.years
    else:
        years = [current_year - 1, current_year]

    log.info(f"{'=' * 60}")
    log.info(f"Congressional Record (CREC) Download")
    log.info(f"Years: {years[0]}-{years[-1]} ({len(years)} years)")
    log.info(f"Workers: {WORKERS}")
    log.info(f"Output: {OUTPUT_DIR}")
    log.info(f"{'=' * 60}")

    grand_total = 0
    start_time = time.time()

    for year in years:
        log.info(f"\n--- Year {year} ---")
        count = process_year(session, year, state)
        grand_total += count

    elapsed = time.time() - start_time
    log.info(f"\n{'=' * 60}")
    log.info(f"Download complete")
    log.info(f"  Total granules: {grand_total:,}")
    log.info(f"  Elapsed: {elapsed / 60:.1f} minutes")
    log.info(f"  Output: {OUTPUT_DIR}")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
