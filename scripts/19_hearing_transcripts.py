#!/usr/bin/env python3
"""
19_hearing_transcripts.py — Download Congressional committee hearing transcripts from GovInfo.

Source: GovInfo CHRG collection (api.govinfo.gov)
  - Enumerate all hearing packages via collections endpoint
  - Download MODS XML for structured metadata (committee, witnesses, dates)
  - Download HTML for full transcript text

Output: hearings_raw/
  - metadata/*.json (package metadata)
  - mods/*.xml (MODS structured metadata)
  - text/*.html (full transcript text)

Usage:
  python3 scripts/19_hearing_transcripts.py                    # enumerate + download all
  python3 scripts/19_hearing_transcripts.py --congress 118     # specific congress only
  python3 scripts/19_hearing_transcripts.py --download-only    # skip enumeration, download pending
  python3 scripts/19_hearing_transcripts.py --build-db         # build database from downloaded files
"""

import json
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import quote, unquote

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
HEARINGS_DIR = BASE_DIR / "hearings_raw"
METADATA_DIR = HEARINGS_DIR / "metadata"
MODS_DIR = HEARINGS_DIR / "mods"
TEXT_DIR = HEARINGS_DIR / "text"
DB_PATH = HEARINGS_DIR / "hearings.db"
CONFIG_PATH = BASE_DIR / "scripts" / "config.json"

# GovInfo API
API_BASE = "https://api.govinfo.gov"
RATE_LIMIT_DELAY = 0.1  # 10 req/sec (well under 40/sec limit)

# Load API key
config = json.loads(CONFIG_PATH.read_text())
API_KEY = config.get("govinfo_api_key", "")


def api_get(url, params=None):
    """Make a GovInfo API request with rate limiting."""
    if params is None:
        params = {}
    params["api_key"] = API_KEY
    time.sleep(RATE_LIMIT_DELAY)
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def enumerate_packages(congress=None):
    """Enumerate all CHRG packages via the collections endpoint."""
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    index_file = HEARINGS_DIR / "package_index.json"

    # Load existing index
    if index_file.exists():
        existing = json.loads(index_file.read_text())
        existing_ids = {p["packageId"] for p in existing}
        print(f"  Existing index: {len(existing)} packages")
    else:
        existing = []
        existing_ids = set()

    # Use published endpoint for date-range enumeration
    # Start from earliest date
    if existing:
        # Get the most recent date we've seen and start from there
        last_date = max(p.get("dateIssued", "1990-01-01") for p in existing)
        start_date = last_date[:10]
        print(f"  Resuming from {start_date}")
    else:
        start_date = "1957-01-01"  # GovInfo has some hearings back to 85th Congress

    end_date = datetime.now().strftime("%Y-%m-%d")

    # The collections endpoint is better for full enumeration
    url = f"{API_BASE}/collections/CHRG/{start_date}T00:00:00Z"
    offset_mark = "*"
    page_size = 1000
    total_new = 0

    consecutive_errors = 0
    while True:
        data = None
        for attempt in range(5):
            try:
                data = api_get(url, {
                    "pageSize": page_size,
                    "offsetMark": offset_mark,
                })
                consecutive_errors = 0
                break
            except Exception as e:
                wait = 5 * (attempt + 1)
                print(f"  Error fetching page (attempt {attempt+1}/5): {e}, retrying in {wait}s...")
                time.sleep(wait)

        if data is None:
            consecutive_errors += 1
            if consecutive_errors >= 3:
                print(f"  Too many consecutive failures, saving progress...")
                break
            print(f"  Skipping page, will retry on next run...")
            continue

        packages = data.get("packages", [])
        if not packages:
            break

        new_count = 0
        for pkg in packages:
            pid = pkg.get("packageId", "")
            if pid and pid not in existing_ids:
                # Filter by congress if specified
                if congress:
                    # Package IDs look like CHRG-118shrg53641
                    m = re.match(r"CHRG-(\d+)", pid)
                    if m and int(m.group(1)) != congress:
                        continue

                existing.append(pkg)
                existing_ids.add(pid)
                new_count += 1

        total_new += new_count

        next_url = data.get("nextPage")
        if not next_url:
            break

        # Extract offsetMark from nextPage URL (must URL-decode to avoid double-encoding by requests)
        if "offsetMark=" in next_url:
            offset_mark = unquote(next_url.split("offsetMark=")[1].split("&")[0])
        else:
            break

        if total_new % 5000 < page_size:
            print(f"  Enumerated {len(existing)} packages ({total_new} new)...")

    # Save index
    index_file.write_text(json.dumps(existing, indent=2))
    print(f"  Total packages: {len(existing)} ({total_new} new)")
    return existing


def download_package_data(packages, max_downloads=None):
    """Download MODS metadata and HTML text for each package."""
    MODS_DIR.mkdir(parents=True, exist_ok=True)
    TEXT_DIR.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped = 0
    errors = 0

    for i, pkg in enumerate(packages):
        pid = pkg.get("packageId", "")
        if not pid:
            continue

        mods_file = MODS_DIR / f"{pid}.xml"
        text_file = TEXT_DIR / f"{pid}.htm"

        # Skip if already downloaded
        if mods_file.exists() and text_file.exists():
            skipped += 1
            continue

        try:
            # Download MODS XML
            if not mods_file.exists():
                mods_url = f"{API_BASE}/packages/{pid}/mods"
                resp = requests.get(mods_url, params={"api_key": API_KEY}, timeout=60)
                if resp.status_code == 200:
                    mods_file.write_bytes(resp.content)
                else:
                    print(f"  MODS {pid}: HTTP {resp.status_code}")
                time.sleep(RATE_LIMIT_DELAY)

            # Download HTML text
            if not text_file.exists():
                htm_url = f"https://www.govinfo.gov/content/pkg/{pid}/html/{pid}.htm"
                resp = requests.get(htm_url, timeout=60)
                if resp.status_code == 200:
                    text_file.write_bytes(resp.content)
                elif resp.status_code == 404:
                    # Try without .htm extension
                    text_file.write_text("")  # Mark as attempted
                else:
                    print(f"  HTML {pid}: HTTP {resp.status_code}")
                time.sleep(RATE_LIMIT_DELAY)

            downloaded += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error {pid}: {e}")

        if downloaded % 100 == 0 and downloaded > 0:
            print(f"  Downloaded {downloaded} (skipped {skipped}, errors {errors})...")

        if max_downloads and downloaded >= max_downloads:
            print(f"  Reached max downloads ({max_downloads})")
            break

    print(f"  Done: {downloaded} downloaded, {skipped} skipped, {errors} errors")
    return downloaded


class TextExtractor(HTMLParser):
    """Extract plain text from HTML hearing transcripts."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.in_body = False
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag == "body":
            self.in_body = True
        if tag in ("script", "style"):
            self.skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip = False
        if tag in ("p", "br", "div", "tr", "li", "h1", "h2", "h3", "h4"):
            self.text_parts.append("\n")

    def handle_data(self, data):
        if self.in_body and not self.skip:
            self.text_parts.append(data)

    def get_text(self):
        return "".join(self.text_parts).strip()


def parse_mods(mods_path):
    """Parse MODS XML to extract structured metadata."""
    try:
        tree = ET.parse(mods_path)
    except ET.ParseError:
        return None

    root = tree.getroot()
    ns = {"mods": "http://www.loc.gov/mods/v3"}

    def find_text(xpath, default=""):
        el = root.find(xpath, ns)
        return el.text.strip() if el is not None and el.text else default

    def find_all_text(xpath):
        return [el.text.strip() for el in root.findall(xpath, ns) if el.text]

    # Extract basic metadata
    title = find_text(".//mods:titleInfo/mods:title")
    date_issued = find_text(".//mods:originInfo/mods:dateIssued")

    # Congress and chamber from classification
    congress = None
    chamber = None
    for classification in root.findall(".//mods:classification", ns):
        auth = classification.get("authority", "")
        text = (classification.text or "").strip()
        if auth == "congNum":
            try:
                congress = int(text)
            except ValueError:
                pass
        elif auth == "chamber":
            chamber = text

    # Committee from name elements
    committee = None
    for name in root.findall(".//mods:name", ns):
        name_type = name.get("type", "")
        if name_type == "corporate":
            role = name.find("mods:role/mods:roleTerm", ns)
            if role is not None and role.text and "committee" in role.text.lower():
                name_part = name.find("mods:namePart", ns)
                if name_part is not None and name_part.text:
                    committee = name_part.text.strip()
                    break

    # Witnesses from personal name elements
    witnesses = []
    for name in root.findall(".//mods:name", ns):
        if name.get("type") == "personal":
            role = name.find("mods:role/mods:roleTerm", ns)
            if role is not None and role.text and "witness" in role.text.lower():
                name_part = name.find("mods:namePart", ns)
                affiliation = name.find("mods:affiliation", ns)
                if name_part is not None and name_part.text:
                    witnesses.append({
                        "name": name_part.text.strip(),
                        "affiliation": affiliation.text.strip() if affiliation is not None and affiliation.text else None
                    })

    # Subjects
    subjects = []
    for subject in root.findall(".//mods:subject/mods:topic", ns):
        if subject.text:
            subjects.append(subject.text.strip())

    # Page count
    extent = find_text(".//mods:physicalDescription/mods:extent")
    pages = None
    if extent:
        m = re.search(r"(\d+)\s*p", extent)
        if m:
            pages = int(m.group(1))

    return {
        "title": title,
        "date_issued": date_issued,
        "congress": congress,
        "chamber": chamber,
        "committee": committee,
        "witnesses": witnesses,
        "subjects": subjects,
        "pages": pages,
    }


def build_database(packages):
    """Build hearings.db from downloaded MODS and text files."""
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    conn.execute("""
        CREATE TABLE hearings (
            package_id TEXT PRIMARY KEY,
            title TEXT,
            congress INTEGER,
            chamber TEXT,
            committee TEXT,
            date_issued TEXT,
            pages INTEGER,
            subjects TEXT,
            full_text TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE hearing_witnesses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            package_id TEXT NOT NULL REFERENCES hearings(package_id),
            witness_name TEXT,
            affiliation TEXT
        )
    """)

    hearing_count = 0
    witness_count = 0
    text_count = 0
    extractor = TextExtractor()

    for pkg in packages:
        pid = pkg.get("packageId", "")
        if not pid:
            continue

        mods_file = MODS_DIR / f"{pid}.xml"
        text_file = TEXT_DIR / f"{pid}.htm"

        if not mods_file.exists():
            continue

        meta = parse_mods(mods_file)
        if not meta:
            continue

        # Extract text
        full_text = None
        if text_file.exists() and text_file.stat().st_size > 0:
            try:
                html = text_file.read_text(errors="replace")
                extractor.__init__()
                extractor.feed(html)
                full_text = extractor.get_text()
                if full_text:
                    text_count += 1
            except Exception:
                pass

        conn.execute("""
            INSERT OR REPLACE INTO hearings (package_id, title, congress, chamber, committee,
                date_issued, pages, subjects, full_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pid,
            meta["title"],
            meta["congress"],
            meta["chamber"],
            meta["committee"],
            meta["date_issued"],
            meta["pages"],
            json.dumps(meta["subjects"]) if meta["subjects"] else None,
            full_text,
        ))
        hearing_count += 1

        # Insert witnesses
        for w in meta.get("witnesses", []):
            conn.execute("""
                INSERT INTO hearing_witnesses (package_id, witness_name, affiliation)
                VALUES (?, ?, ?)
            """, (pid, w["name"], w["affiliation"]))
            witness_count += 1

        if hearing_count % 1000 == 0:
            conn.commit()
            print(f"  Processed {hearing_count} hearings...")

    # Indexes
    conn.execute("CREATE INDEX idx_hearings_congress ON hearings(congress)")
    conn.execute("CREATE INDEX idx_hearings_chamber ON hearings(chamber)")
    conn.execute("CREATE INDEX idx_hearings_committee ON hearings(committee)")
    conn.execute("CREATE INDEX idx_hearings_date ON hearings(date_issued)")
    conn.execute("CREATE INDEX idx_witnesses_pkg ON hearing_witnesses(package_id)")
    conn.execute("CREATE INDEX idx_witnesses_name ON hearing_witnesses(witness_name)")

    # FTS on hearings
    conn.execute("""
        CREATE VIRTUAL TABLE hearings_fts USING fts5(
            title, committee, full_text, subjects,
            content='hearings', content_rowid='rowid'
        )
    """)
    conn.execute("""
        INSERT INTO hearings_fts(rowid, title, committee, full_text, subjects)
        SELECT rowid, title, committee, full_text, subjects FROM hearings
    """)

    # FTS on witnesses
    conn.execute("""
        CREATE VIRTUAL TABLE witnesses_fts USING fts5(
            witness_name, affiliation,
            content='hearing_witnesses', content_rowid='id'
        )
    """)
    conn.execute("""
        INSERT INTO witnesses_fts(rowid, witness_name, affiliation)
        SELECT id, witness_name, affiliation FROM hearing_witnesses
    """)

    conn.commit()

    print(f"\n=== Hearings Database Built ===")
    print(f"  Hearings: {hearing_count:,}")
    print(f"  With full text: {text_count:,}")
    print(f"  Witnesses: {witness_count:,}")

    # Congress breakdown
    for row in conn.execute("""
        SELECT congress, COUNT(*), SUM(CASE WHEN full_text IS NOT NULL THEN 1 ELSE 0 END)
        FROM hearings WHERE congress IS NOT NULL
        GROUP BY congress ORDER BY congress DESC LIMIT 10
    """).fetchall():
        print(f"  Congress {row[0]}: {row[1]} hearings ({row[2]} with text)")

    db_size = DB_PATH.stat().st_size / 1024 / 1024
    print(f"\n  Database: {DB_PATH} ({db_size:.1f} MB)")
    conn.close()


def main():
    args = sys.argv[1:]
    congress = None
    download_only = "--download-only" in args
    build_db_only = "--build-db" in args

    for i, arg in enumerate(args):
        if arg == "--congress" and i + 1 < len(args):
            congress = int(args[i + 1])

    max_downloads = None
    for i, arg in enumerate(args):
        if arg == "--max" and i + 1 < len(args):
            max_downloads = int(args[i + 1])

    print("=== Congressional Hearing Transcripts Pipeline ===\n")

    if build_db_only:
        index_file = HEARINGS_DIR / "package_index.json"
        if index_file.exists():
            packages = json.loads(index_file.read_text())
            print(f"Loaded {len(packages)} packages from index")
            build_database(packages)
        else:
            print("ERROR: No package index found. Run enumeration first.")
        return

    if not download_only:
        print("1. Enumerating hearing packages from GovInfo...")
        packages = enumerate_packages(congress)
        print()
    else:
        index_file = HEARINGS_DIR / "package_index.json"
        if index_file.exists():
            packages = json.loads(index_file.read_text())
            print(f"Loaded {len(packages)} packages from index\n")
        else:
            print("ERROR: No package index found. Run enumeration first.")
            return

    print("2. Downloading MODS metadata and HTML transcripts...")
    download_package_data(packages, max_downloads=max_downloads)
    print()

    print("3. Building database...")
    build_database(packages)


if __name__ == "__main__":
    main()
