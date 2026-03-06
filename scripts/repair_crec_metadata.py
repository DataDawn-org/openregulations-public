#!/usr/bin/env python3
"""
Repair CREC metadata: download MODS XML for each existing day and
merge parsed metadata into the existing JSON files.

This fixes the bug where parse_mods_metadata() failed to use the MODS
namespace prefix for extension elements, resulting in empty metadata fields
(title, chamber, granule_class, speakers, bills, etc.).

The text content in the JSON files is correct — only metadata needs repair.
Downloads only the ~1 MB MODS XML per day (not the full 30+ MB ZIP).

Usage:
    python3 repair_crec_metadata.py           # repair all years
    python3 repair_crec_metadata.py --years 2020 2021  # specific years
    python3 repair_crec_metadata.py --dry-run  # show what would be repaired
"""

import argparse
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_DIR = Path(__file__).resolve().parent.parent
CREC_DIR = PROJECT_DIR / "congressional_record"
LOG_DIR = PROJECT_DIR / "logs"
WORKERS = 8

# Logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("crec_repair")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "crec_repair.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


def year_to_congress(year):
    return (year - 1789) // 2 + 1


class GovInfoRetry(Retry):
    def parse_retry_after(self, retry_after):
        if retry_after is None:
            return None
        if "," in str(retry_after):
            retry_after = str(retry_after).split(",")[0].strip()
        return super().parse_retry_after(retry_after)


def create_session():
    session = requests.Session()
    retries = GovInfoRetry(
        total=5, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=WORKERS + 2))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (crec metadata repair)"})
    return session


def parse_mods_metadata(mods_xml, date_str):
    """Parse MODS metadata — fixed version with proper namespace handling."""
    ns = {"mods": "http://www.loc.gov/mods/v3"}

    try:
        root = ET.fromstring(mods_xml)
    except ET.ParseError:
        return None, {}

    # Package-level metadata
    package = {}
    date_elem = root.find(".//mods:dateIssued", ns)
    if date_elem is not None and date_elem.text:
        package["date"] = date_elem.text
    else:
        package["date"] = date_str

    # Derive congress/session from date
    try:
        year = int(date_str[:4])
        package["congress"] = str(year_to_congress(year))
        package["session"] = "1" if year % 2 == 1 else "2"
    except (ValueError, IndexError):
        pass

    # Per-granule metadata keyed by granule_id
    granule_meta = {}
    for item in root.findall(".//mods:relatedItem[@type='constituent']", ns):
        granule = {}

        # Granule ID from ID attribute
        rid = item.get("ID", "")
        gid = rid[3:] if rid.startswith("id-") else ""
        if not gid:
            # Fallback: extract from URI
            for id_elem in item.findall("mods:identifier", ns):
                if id_elem.get("type") == "uri":
                    uri = id_elem.text or ""
                    match = re.search(r'/(?:details|granule)/CREC-[\d-]+/(CREC-[^/]+)', uri)
                    if match:
                        gid = match.group(1)
                        break
        if not gid:
            continue

        # Title
        title_elem = item.find("mods:titleInfo/mods:title", ns)
        granule["title"] = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

        # Chamber label (partName)
        part_elem = item.find("mods:titleInfo/mods:partName", ns)
        granule["chamber_label"] = part_elem.text.strip() if part_elem is not None and part_elem.text else ""

        # Extension fields (all in MODS namespace)
        item_ext = item.find("mods:extension", ns)
        if item_ext is not None:
            granule["granule_class"] = getattr(item_ext.find("mods:granuleClass", ns), "text", "") or ""
            granule["sub_granule_class"] = getattr(item_ext.find("mods:subGranuleClass", ns), "text", "") or ""
            granule["chamber"] = getattr(item_ext.find("mods:chamber", ns), "text", "") or ""

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

        # Citation
        for id_elem in item.findall("mods:identifier", ns):
            if "citation" in (id_elem.get("type") or ""):
                granule["citation"] = id_elem.text

        granule_meta[gid] = granule

    return package, granule_meta


def repair_file(session, json_path):
    """Download MODS XML and merge metadata into existing JSON file."""
    try:
        data = json.load(open(json_path))
    except (json.JSONDecodeError, FileNotFoundError):
        return None, "bad json"

    date_str = data.get("date", "")
    package_id = data.get("package_id", f"CREC-{date_str}")
    if not date_str:
        return None, "no date"

    # Check if already has metadata AND cong_members (skip if fully repaired)
    granules = data.get("granules", [])
    has_metadata = granules and granules[0].get("title") and granules[0].get("chamber")
    has_bioguide = any(g.get("cong_members") for g in granules)
    if has_metadata and has_bioguide:
        return None, "already has metadata"

    # Download MODS XML directly (much smaller than full ZIP)
    mods_url = f"https://www.govinfo.gov/metadata/pkg/{package_id}/mods.xml"
    try:
        resp = session.get(mods_url, timeout=60)
        if resp.status_code == 404:
            return None, "mods 404"
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        return None, f"download error: {e}"

    mods_xml = resp.text
    package_meta, granule_meta = parse_mods_metadata(mods_xml, date_str)
    if not granule_meta:
        return None, "no granules in mods"

    # Merge metadata into existing granules
    updated = 0
    for granule in granules:
        gid = granule.get("granule_id", "")
        meta = granule_meta.get(gid)
        if meta:
            granule["title"] = meta.get("title", granule.get("title", ""))
            granule["chamber"] = meta.get("chamber", granule.get("chamber", ""))
            granule["granule_class"] = meta.get("granule_class", granule.get("granule_class", ""))
            granule["sub_granule_class"] = meta.get("sub_granule_class", granule.get("sub_granule_class", ""))
            granule["page_start"] = meta.get("page_start", granule.get("page_start", ""))
            granule["page_end"] = meta.get("page_end", granule.get("page_end", ""))
            granule["speakers"] = meta.get("speakers", granule.get("speakers", []))
            granule["bills"] = meta.get("bills", granule.get("bills", []))
            granule["citation"] = meta.get("citation", granule.get("citation", ""))
            if "cong_members" in meta:
                granule["cong_members"] = meta["cong_members"]
            updated += 1

    # Update package-level metadata
    if package_meta:
        data["congress"] = package_meta.get("congress", data.get("congress"))
        data["session"] = package_meta.get("session", data.get("session"))

    # Write back
    with open(json_path, "w") as f:
        json.dump(data, f)

    return updated, "ok"


def main():
    parser = argparse.ArgumentParser(description="Repair CREC metadata by downloading MODS XML")
    parser.add_argument("--years", type=int, nargs="+", help="Years to repair (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be repaired")
    args = parser.parse_args()

    session = create_session()

    # Find all JSON files
    all_files = []
    for year_dir in sorted(CREC_DIR.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)
        if args.years and year not in args.years:
            continue
        for json_file in sorted(year_dir.glob("*.json")):
            all_files.append((year, json_file))

    log.info(f"Found {len(all_files)} JSON files to check")

    if args.dry_run:
        # Just count files needing repair
        needs_repair = 0
        for year, path in all_files:
            try:
                data = json.load(open(path))
                granules = data.get("granules", [])
                if granules and not granules[0].get("title") and not granules[0].get("chamber"):
                    needs_repair += 1
            except Exception:
                pass
        log.info(f"Files needing repair: {needs_repair} of {len(all_files)}")
        return

    start_time = time.time()
    total_updated = 0
    total_granules = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for year, path in all_files:
            f = pool.submit(repair_file, session, path)
            futures[f] = (year, path)

        for i, future in enumerate(as_completed(futures)):
            year, path = futures[future]
            try:
                count, status = future.result()
            except Exception as e:
                log.error(f"Error repairing {path.name}: {e}")
                errors += 1
                continue

            if count is not None:
                total_updated += 1
                total_granules += count
            elif status not in ("already has metadata",):
                if status != "mods 404":
                    errors += 1

            if (i + 1) % 50 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed * 60
                log.info(f"  Progress: {i + 1}/{len(all_files)} files "
                         f"({total_updated} repaired, {total_granules:,} granules, "
                         f"{errors} errors, {rate:.0f} files/min)")

    elapsed = time.time() - start_time
    log.info(f"\nRepair complete in {elapsed / 60:.1f} minutes")
    log.info(f"  Files repaired: {total_updated}")
    log.info(f"  Granules updated: {total_granules:,}")
    log.info(f"  Errors: {errors}")


if __name__ == "__main__":
    main()
