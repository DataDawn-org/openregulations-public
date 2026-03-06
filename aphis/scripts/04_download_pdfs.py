#!/usr/bin/env python3
"""
Step 4: Download inspection report and enforcement action PDFs.

Reads extracted metadata from raw/{category}/all_results.json,
extracts PDF URLs, and downloads each to pdfs/{category}/{hash_id}.pdf.

Features:
- Resume capability (skips already-downloaded files)
- Rate limiting (~2 req/sec)
- Progress tracking
- Parallel downloads with thread pool
"""

import hashlib
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.aura_client import BASE_DIR, RAW_DIR, STATE_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

PDFS_DIR = BASE_DIR / "pdfs"
MAX_WORKERS = 4
RATE_LIMIT = 0.5  # seconds between requests per worker
MAX_RETRIES_PER_PDF = 3

# Categories that have downloadable PDFs
PDF_CATEGORIES = ["inspections", "enforcement"]

# Possible field names containing PDF URLs
URL_FIELDS = [
    "reportUrl",
    "pdfUrl",
    "documentUrl",
    "url",
    "reportLink",
    "pdfLink",
    "web_reportUrl",
    "inspectionReportUrl",
]


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "openregs-aphis-extractor/1.0 (public interest research)"
    return session


def extract_pdf_url(record: dict) -> str | None:
    """Find a PDF URL in a record by checking known field names."""
    for field in URL_FIELDS:
        url = record.get(field)
        if url and isinstance(url, str) and ("http" in url or url.startswith("/")):
            return url
    # Check all string values for URL patterns
    for key, val in record.items():
        if isinstance(val, str) and (".pdf" in val.lower() or "report" in val.lower()):
            if val.startswith("http"):
                return val
    return None


def hash_id(url: str) -> str:
    """Generate a short hash ID from a URL."""
    # Try to extract a Salesforce record ID
    match = re.search(r'[?&]id=([^&]+)', url) or re.search(r'/([a-zA-Z0-9]{15,18})(?:\?|$)', url)
    if match:
        return hashlib.sha1(match.group(1).encode()).hexdigest()[:12]
    return hashlib.sha1(url.encode()).hexdigest()[:12]


def download_pdf(
    url: str,
    dest: Path,
    session: requests.Session,
) -> bool:
    """Download a single PDF. Returns True on success."""
    if dest.exists() and dest.stat().st_size > 0:
        return True

    for attempt in range(MAX_RETRIES_PER_PDF):
        try:
            time.sleep(RATE_LIMIT)
            resp = session.get(url, timeout=60, stream=True)

            if resp.status_code == 200:
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)

                if dest.stat().st_size > 0:
                    return True
                else:
                    dest.unlink()
                    logger.warning(f"Empty file downloaded: {url}")
            else:
                logger.debug(f"HTTP {resp.status_code} for {url} (attempt {attempt + 1})")

        except requests.RequestException as e:
            logger.debug(f"Download error for {url} (attempt {attempt + 1}): {e}")

    return False


def load_pdf_urls(category: str) -> list[tuple[str, str]]:
    """
    Load records for a category and extract (url, hash_id) pairs.
    Returns list of (pdf_url, file_hash) tuples.
    """
    results_file = RAW_DIR / category / "all_results.json"
    if not results_file.exists():
        logger.warning(f"No results file for {category}: {results_file}")
        return []

    with open(results_file) as f:
        data = json.load(f)

    records = data.get("results", [])
    pairs = []
    missing_url = 0

    for record in records:
        url = extract_pdf_url(record)
        if url:
            hid = record.get("hash_id") or hash_id(url)
            pairs.append((url, hid))
        else:
            missing_url += 1

    if missing_url:
        logger.info(f"  {missing_url} records had no extractable PDF URL")

    return pairs


def process_category(category: str, max_workers: int = MAX_WORKERS):
    """Download all PDFs for a single category."""
    logger.info(f"\n--- Downloading PDFs for: {category} ---")

    pdf_dir = PDFS_DIR / category
    pdf_dir.mkdir(parents=True, exist_ok=True)

    pairs = load_pdf_urls(category)
    if not pairs:
        logger.info(f"No PDF URLs found for {category}")
        return

    # Filter out already-downloaded files
    to_download = []
    already_have = 0
    for url, hid in pairs:
        dest = pdf_dir / f"{hid}.pdf"
        if dest.exists() and dest.stat().st_size > 0:
            already_have += 1
        else:
            to_download.append((url, hid, dest))

    logger.info(f"  Total: {len(pairs)}, already downloaded: {already_have}, to download: {len(to_download)}")

    if not to_download:
        logger.info(f"  All PDFs for {category} already downloaded")
        return

    # Download with thread pool
    session = build_session()
    success = 0
    failed = 0
    failed_urls = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_pdf, url, dest, session): (url, hid)
            for url, hid, dest in to_download
        }

        for i, future in enumerate(as_completed(futures)):
            url, hid = futures[future]
            try:
                if future.result():
                    success += 1
                else:
                    failed += 1
                    failed_urls.append(url)
            except Exception as e:
                failed += 1
                failed_urls.append(url)
                logger.debug(f"Download exception for {hid}: {e}")

            if (i + 1) % 100 == 0:
                logger.info(f"  Progress: {i + 1}/{len(to_download)} ({success} ok, {failed} failed)")

    logger.info(f"  Done: {success} downloaded, {failed} failed")

    # Save failed URLs for retry
    if failed_urls:
        failed_file = STATE_DIR / f"failed_pdfs_{category}.json"
        failed_file.parent.mkdir(parents=True, exist_ok=True)
        with open(failed_file, "w") as f:
            json.dump(failed_urls, f, indent=2)
        logger.info(f"  Failed URLs saved to {failed_file}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download APHIS inspection/enforcement PDFs")
    parser.add_argument("--categories", nargs="+", default=PDF_CATEGORIES, help="Categories to download PDFs for")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Number of download threads")
    args = parser.parse_args()

    num_workers = args.workers

    for category in args.categories:
        try:
            process_category(category, max_workers=num_workers)
        except Exception as e:
            logger.error(f"Failed to process {category}: {e}")

    logger.info("\nPDF download complete.")


if __name__ == "__main__":
    main()
