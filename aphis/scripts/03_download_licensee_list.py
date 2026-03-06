#!/usr/bin/env python3
"""
Step 3: Download and parse the APHIS active licensee/registrant list.

Downloads from:
- https://www.aphis.usda.gov/sites/default/files/List-of-Active-Licensees-and-Registrants.pdf
- Tries .xlsx variant at same path

Parses into structured JSON for database import.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.aura_client import RAW_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

LICENSEE_URLS = {
    "pdf": "https://www.aphis.usda.gov/sites/default/files/List-of-Active-Licensees-and-Registrants.pdf",
    "xlsx": "https://www.aphis.usda.gov/sites/default/files/List-of-Active-Licensees-and-Registrants.xlsx",
    "xlsx_alt": "https://www.aphis.usda.gov/animal_welfare/downloads/List-of-Active-Licensees-and-Registrants.xlsx",
}

OUTPUT_DIR = RAW_DIR / "licensees"


def download_file(url: str, dest: Path, session: requests.Session) -> bool:
    """Download a file if it exists. Returns True on success."""
    try:
        resp = session.get(url, timeout=60, stream=True)
        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "")
            # Check it's actually a file, not an error page
            if "text/html" in content_type and dest.suffix != ".html":
                logger.info(f"  URL returned HTML (likely error page), skipping: {url}")
                return False

            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            size_mb = dest.stat().st_size / (1024 * 1024)
            logger.info(f"  Downloaded {dest.name} ({size_mb:.1f} MB)")
            return True
        else:
            logger.info(f"  HTTP {resp.status_code} for {url}")
            return False
    except requests.RequestException as e:
        logger.warning(f"  Failed to download {url}: {e}")
        return False


def parse_xlsx(filepath: Path) -> list[dict]:
    """Parse the licensee Excel file into records."""
    try:
        import openpyxl
    except ImportError:
        logger.error("openpyxl not installed. Install with: pip install openpyxl")
        return []

    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    # First row is headers
    headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(rows[0])]
    logger.info(f"  Excel headers: {headers}")

    records = []
    for row in rows[1:]:
        if not any(row):
            continue
        record = {}
        for i, val in enumerate(row):
            if i < len(headers):
                key = headers[i]
                record[key] = str(val).strip() if val is not None else ""
        records.append(record)

    return records


def parse_pdf_with_tabula(filepath: Path) -> list[dict]:
    """Parse the licensee PDF using tabula-py."""
    try:
        import tabula
    except ImportError:
        logger.error("tabula-py not installed. Install with: pip install tabula-py")
        return []

    try:
        dfs = tabula.read_pdf(str(filepath), pages="all", multiple_tables=True)
        records = []
        for df in dfs:
            for _, row in df.iterrows():
                record = {col: str(val).strip() if val else "" for col, val in row.items()}
                records.append(record)
        return records
    except Exception as e:
        logger.error(f"tabula-py failed: {e}")
        return []


def parse_pdf_with_pdfplumber(filepath: Path) -> list[dict]:
    """Parse the licensee PDF using pdfplumber (fallback)."""
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber not installed. Install with: pip install pdfplumber")
        return []

    records = []
    headers = None

    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table:
                    continue
                for row in table:
                    if not any(row):
                        continue
                    cells = [str(c).strip() if c else "" for c in row]

                    # Detect header row
                    if headers is None and any(
                        kw in " ".join(cells).lower()
                        for kw in ["certificate", "name", "state", "license", "type"]
                    ):
                        headers = cells
                        continue

                    if headers:
                        record = {}
                        for i, val in enumerate(cells):
                            if i < len(headers):
                                record[headers[i]] = val
                        records.append(record)
                    else:
                        records.append({"raw": cells})

    return records


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "openregs-aphis-extractor/1.0 (public interest research)"

    downloaded_files = {}

    # Try downloading each format
    for fmt, url in LICENSEE_URLS.items():
        ext = fmt.split("_")[0]  # xlsx_alt → xlsx
        dest = OUTPUT_DIR / f"licensee_list.{ext}"
        if dest.exists():
            logger.info(f"Already have {dest.name}")
            downloaded_files[fmt] = dest
            continue

        logger.info(f"Trying {fmt}: {url}")
        if download_file(url, dest, session):
            downloaded_files[fmt] = dest

    if not downloaded_files:
        logger.error("Could not download any licensee list file")
        sys.exit(1)

    # Parse in order of preference: xlsx > pdf
    records = []

    if "xlsx" in downloaded_files or "xlsx_alt" in downloaded_files:
        xlsx_path = downloaded_files.get("xlsx") or downloaded_files.get("xlsx_alt")
        logger.info(f"Parsing Excel file: {xlsx_path}")
        records = parse_xlsx(xlsx_path)
        logger.info(f"  Parsed {len(records)} records from Excel")

    if not records and "pdf" in downloaded_files:
        pdf_path = downloaded_files["pdf"]
        logger.info(f"Parsing PDF file: {pdf_path}")

        # Try tabula first, then pdfplumber
        records = parse_pdf_with_tabula(pdf_path)
        if not records:
            logger.info("  tabula failed, trying pdfplumber...")
            records = parse_pdf_with_pdfplumber(pdf_path)
        logger.info(f"  Parsed {len(records)} records from PDF")

    if records:
        output_file = OUTPUT_DIR / "all_results.json"
        with open(output_file, "w") as f:
            json.dump({
                "category": "licensees",
                "source": "aphis_download",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_records": len(records),
                "results": records,
            }, f, indent=2)
        logger.info(f"Saved {len(records)} licensee records to {output_file}")
    else:
        logger.warning("No records parsed from any file. Manual inspection may be needed.")
        logger.info("Downloaded files are available for manual parsing in: %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
