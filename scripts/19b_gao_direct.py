#!/usr/bin/env python3
"""
Phase 19b: Download GAO reports directly from gao.gov.

Supplements 19_gao_reports.py (GovInfo, 1989-2008) with direct scraping
of gao.gov to get all ~59,000 reports including 2009-present.

Two-phase approach:
  Phase 1 (listing): Scrape /reports-testimonies pages to discover all reports
  Phase 2 (detail):  Scrape individual /products/{gao-number} pages for full metadata

Source: gao.gov (Government Accountability Office)
Auth: None (public data, Akamai CDN requires browser-like headers)
Rate: ~3 seconds between requests (Akamai-friendly)

Usage:
    python3 19b_gao_direct.py --phase listing       # Phase 1: discover reports
    python3 19b_gao_direct.py --phase detail         # Phase 2: full metadata
    python3 19b_gao_direct.py --phase all            # Both phases sequentially
    python3 19b_gao_direct.py --phase listing --limit 5   # Test with 5 pages
    python3 19b_gao_direct.py --dry-run              # Preview without saving

Timing estimates:
  Phase 1 (listing): ~2,950 pages × 3s = ~2.5 hours → ~59K report stubs
  Phase 2 (detail):  ~59K pages × 3s = ~49 hours → full metadata + highlights
"""

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "gao_direct"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "gao_direct_state.json"
REQUEST_DELAY = 3.0  # seconds between requests (Akamai-friendly)
LISTING_URL = "https://www.gao.gov/reports-testimonies"
PRODUCT_BASE = "https://www.gao.gov"
ITEMS_PER_PAGE = 20  # fixed by gao.gov

# Browser-like headers required by Akamai CDN
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "identity",
}

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("gao_direct")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "gao_direct.log")
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
        total=5, backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=2))
    session.headers.update(BROWSER_HEADERS)
    return session


# === State Management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "listing_page": 0,
        "listing_complete": False,
        "listing_total": 0,
        "detail_index": 0,
        "detail_complete": False,
    }


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# === Phase 1: Listing Pages ===
def parse_listing_page(html):
    """Parse a /reports-testimonies page and extract report stubs."""
    soup = BeautifulSoup(html, "lxml")
    reports = []

    for row in soup.select("div.views-row"):
        report = {}

        # Title and URL
        title_link = row.select_one("h4.c-search-result__header a")
        if not title_link:
            continue
        report["title"] = title_link.get_text(strip=True)
        href = title_link.get("href", "")
        report["url"] = href
        # Extract GAO number from URL: /products/gao-26-109069 -> GAO-26-109069
        m = re.search(r"/products/([\w-]+)$", href)
        report["gao_number"] = m.group(1).upper() if m else ""

        # Subheading: GAO number text, dates
        subheading = row.select_one("div.teaser-search--subheading")
        if subheading:
            # GAO number (text form)
            num_span = subheading.select_one("span.d-block.text-small")
            if num_span:
                gao_num_text = num_span.get_text(strip=True)
                if gao_num_text and not report["gao_number"]:
                    report["gao_number"] = gao_num_text

            # Dates
            times = subheading.select("time[datetime]")
            if len(times) >= 1:
                report["published_date"] = times[0].get("datetime", "")[:10]
            if len(times) >= 2:
                report["released_date"] = times[1].get("datetime", "")[:10]

        # Summary snippet
        summary = row.select_one("div.c-search-result__summary")
        if summary:
            report["summary"] = summary.get_text(strip=True)

        if report.get("gao_number"):
            reports.append(report)

    # Total count from page
    total = 0
    count_div = soup.select_one("div.result-count strong")
    if count_div:
        m = re.search(r"of\s+([\d,]+)", count_div.get_text())
        if m:
            total = int(m.group(1).replace(",", ""))

    # Last page from pagination
    last_page = 0
    last_link = soup.select_one('a[aria-label="Last page"]')
    if last_link:
        href = last_link.get("href", "")
        m = re.search(r"page=(\d+)", href)
        if m:
            last_page = int(m.group(1))

    return reports, total, last_page


def run_listing_phase(session, state, limit=None, dry_run=False, delay=REQUEST_DELAY):
    """Scrape all listing pages to discover report URLs."""
    start_page = state.get("listing_page", 0)

    if state.get("listing_complete"):
        log.info("Listing phase already complete. Use --full to re-run.")
        return

    log.info(f"=== Phase 1: Listing (starting at page {start_page}) ===")
    run_start = time.time()

    # First request to get total count
    resp = session.get(f"{LISTING_URL}?page=0", timeout=60)
    resp.raise_for_status()
    _, total, last_page = parse_listing_page(resp.text)
    log.info(f"Total reports: {total:,} across {last_page + 1} pages")
    state["listing_total"] = total
    time.sleep(delay)

    all_stubs = []
    existing_stubs = {}

    # Load existing listing stubs
    listing_file = OUTPUT_DIR / "_listing_stubs.json"
    if listing_file.exists():
        existing_stubs = {r["gao_number"]: r for r in json.loads(listing_file.read_text())}
        log.info(f"Loaded {len(existing_stubs)} existing stubs")

    page_limit = last_page + 1
    if limit:
        page_limit = min(start_page + limit, last_page + 1)

    for page_num in range(start_page, page_limit):
        try:
            url = f"{LISTING_URL}?page={page_num}"
            log.info(f"  Fetching page {page_num}...")
            sys.stdout.flush()
            resp = session.get(url, timeout=90)
            resp.raise_for_status()

            reports, _, _ = parse_listing_page(resp.text)

            if dry_run:
                for r in reports:
                    log.info(f"  {r.get('gao_number', '?')}: {r.get('title', '')[:70]}")
            else:
                for r in reports:
                    if r.get("gao_number"):
                        existing_stubs[r["gao_number"]] = r
                all_stubs.extend(reports)

            if (page_num + 1) % 10 == 0 or page_num == start_page:
                total_found = len(existing_stubs)
                elapsed_pages = page_num - start_page + 1
                avg_per_page = (time.time() - run_start) / max(elapsed_pages, 1)
                remaining = page_limit - page_num - 1
                eta_hrs = (remaining * avg_per_page) / 3600
                log.info(f"  Page {page_num + 1}/{page_limit}: {len(reports)} reports "
                         f"(total unique: {total_found:,}, "
                         f"~{avg_per_page:.0f}s/page, ETA: {eta_hrs:.1f}h)")
                if not dry_run:
                    state["listing_page"] = page_num + 1
                    save_state(state)
                    # Save stubs periodically
                    listing_file.write_text(json.dumps(
                        list(existing_stubs.values()), indent=2, ensure_ascii=False
                    ))

        except requests.exceptions.HTTPError as e:
            log.error(f"  Page {page_num}: HTTP {e.response.status_code if e.response else '?'}")
            # On 403, back off longer (Akamai block)
            if e.response is not None and e.response.status_code == 403:
                log.warning("  Got 403 — backing off 60 seconds")
                time.sleep(60)
        except Exception as e:
            log.error(f"  Page {page_num}: {e}")

        time.sleep(delay)

    if not dry_run:
        # Final save
        listing_file.write_text(json.dumps(
            list(existing_stubs.values()), indent=2, ensure_ascii=False
        ))
        if page_num >= last_page:
            state["listing_complete"] = True
        state["listing_page"] = page_num + 1
        save_state(state)

    log.info(f"Listing phase: {len(existing_stubs):,} unique reports found")


# === Phase 2: Detail Pages ===
def parse_detail_page(html):
    """Parse a /products/{gao-number} page for full metadata."""
    soup = BeautifulSoup(html, "lxml")
    detail = {}

    # Title
    h1 = soup.select_one("h1.split-headings")
    if h1:
        detail["title"] = h1.get_text(" ", strip=True)

    # Post-title metadata block
    meta_block = soup.select_one("section.block-post-title-info")
    if meta_block:
        # GAO number
        num_el = meta_block.select_one("span.text-small strong")
        if num_el:
            detail["gao_number"] = num_el.get_text(strip=True)
        # Date text
        date_text = meta_block.get_text(" ", strip=True)
        # Extract published date
        m = re.search(r"Published:\s*(\w+ \d+, \d{4})", date_text)
        if m:
            detail["published_text"] = m.group(1)
        m = re.search(r"Released:\s*(\w+ \d+, \d{4})", date_text)
        if m:
            detail["released_text"] = m.group(1)

    # Fast Facts
    fast_facts = soup.select_one("div.field--name-field-fast-facts-description")
    if fast_facts:
        detail["fast_facts"] = fast_facts.get_text("\n", strip=True)

    # Highlights (What GAO Found / Why GAO Did This Study)
    highlights = soup.select_one("div.field--name-product-highlights-custom, "
                                 "div.js-endpoint-highlights")
    if highlights:
        detail["highlights"] = highlights.get_text("\n", strip=True)

    # Recommendations
    recs = soup.select_one("div.field--name-field-recommendations-intro, "
                           "div.js-endpoint-recommendations")
    if recs:
        detail["recommendations"] = recs.get_text("\n", strip=True)

    # Full report section — PDF links, page count
    report_section = soup.select_one("section.js-endpoint-full-report, "
                                     "section.full-reports-group")
    if report_section:
        for link in report_section.select("a[href]"):
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()
            if "highlights" in text and href.endswith(".pdf"):
                detail["highlights_pdf_url"] = href if href.startswith("http") else PRODUCT_BASE + href
            elif href.endswith(".pdf"):
                detail["pdf_url"] = href if href.startswith("http") else PRODUCT_BASE + href
                # Try to extract page count from link text
                m = re.search(r"\((\d+)\s*pages?\)", link.get_text())
                if m:
                    detail["pages"] = int(m.group(1))
            elif "files.gao.gov" in href:
                detail["html_report_url"] = href

    # Topics
    topics_div = soup.select_one("div.views-field-field-topic")
    if topics_div:
        detail["topics"] = [a.get_text(strip=True) for a in topics_div.select("a")]

    # Subject terms
    subjects_div = soup.select_one("div.views-field-field-subject-term")
    if subjects_div:
        detail["subjects"] = [s.get_text(strip=True)
                              for s in subjects_div.select("span, a")
                              if s.get_text(strip=True)]

    # Agencies
    agencies_div = soup.select_one("div.views-field-field-agency-name")
    if agencies_div:
        detail["agencies"] = [a.get_text(strip=True) for a in agencies_div.select("a")]

    # GAO contacts
    contacts = []
    for contact_block in soup.select("div.staff-contact"):
        contact = {}
        name_el = contact_block.select_one("span.node-title")
        if name_el:
            contact["name"] = name_el.get_text(strip=True)
        title_el = contact_block.select_one("div.field--name-field-staff-contact-title")
        if title_el:
            contact["title"] = title_el.get_text(strip=True)
        team_el = contact_block.select_one("span.field--name-field-staff-team")
        if team_el:
            contact["team"] = team_el.get_text(strip=True)
        email_el = contact_block.select_one("a[href^='mailto:']")
        if email_el:
            contact["email"] = email_el.get_text(strip=True)
        phone_el = contact_block.select_one("a[href^='tel:']")
        if phone_el:
            contact["phone"] = phone_el.get_text(strip=True)
        if contact.get("name"):
            contacts.append(contact)
    if contacts:
        detail["contacts"] = contacts

    # OG meta tags as fallback
    og_desc = soup.select_one('meta[property="og:description"]')
    if og_desc and not detail.get("highlights"):
        detail["og_description"] = og_desc.get("content", "")

    return detail


def run_detail_phase(session, state, limit=None, dry_run=False, delay=REQUEST_DELAY):
    """Scrape individual product pages for full metadata."""
    listing_file = OUTPUT_DIR / "_listing_stubs.json"
    if not listing_file.exists():
        log.error("No listing stubs found. Run --phase listing first.")
        return

    stubs = json.loads(listing_file.read_text())
    log.info(f"=== Phase 2: Detail ({len(stubs):,} reports) ===")

    start_index = state.get("detail_index", 0)

    # Check which ones already have detail files
    existing = {f.stem for f in OUTPUT_DIR.glob("*.json") if not f.name.startswith("_")}
    log.info(f"Already have {len(existing):,} detail files")

    # Build work list: skip ones we already have
    work_list = []
    for stub in stubs:
        gao_num = stub.get("gao_number", "")
        if gao_num and gao_num not in existing:
            work_list.append(stub)

    if limit:
        work_list = work_list[:limit]

    log.info(f"Need to fetch {len(work_list):,} detail pages")

    fetched = 0
    errors = 0

    for i, stub in enumerate(work_list):
        gao_num = stub.get("gao_number", "")
        url_path = stub.get("url", f"/products/{gao_num.lower()}")
        url = f"{PRODUCT_BASE}{url_path}" if url_path.startswith("/") else url_path

        try:
            resp = session.get(url, timeout=90)
            resp.raise_for_status()

            detail = parse_detail_page(resp.text)

            # Merge stub data with detail
            merged = {**stub, **detail}

            if dry_run:
                log.info(f"  {gao_num}: {merged.get('title', '')[:70]}")
                log.info(f"    Topics: {merged.get('topics', [])}")
                log.info(f"    PDF: {merged.get('pdf_url', 'none')}")
            else:
                out_file = OUTPUT_DIR / f"{gao_num}.json"
                out_file.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
                fetched += 1

            if fetched % 100 == 0 and fetched > 0:
                log.info(f"  Progress: {fetched:,}/{len(work_list):,} fetched, {errors} errors")
                state["detail_index"] = start_index + i + 1
                save_state(state)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            if status == 403:
                log.warning(f"  {gao_num}: 403 — backing off 60s")
                time.sleep(60)
            elif status == 404:
                log.warning(f"  {gao_num}: 404 — skipping")
                # Save a minimal stub so we don't retry
                if not dry_run:
                    out_file = OUTPUT_DIR / f"{gao_num}.json"
                    out_file.write_text(json.dumps({**stub, "_error": "404"}, indent=2))
            else:
                log.error(f"  {gao_num}: HTTP {status}")
            errors += 1
        except Exception as e:
            log.error(f"  {gao_num}: {e}")
            errors += 1

        time.sleep(delay)

    if not dry_run:
        state["detail_index"] = start_index + len(work_list)
        if not work_list or errors == 0:
            state["detail_complete"] = True
        save_state(state)

    log.info(f"Detail phase complete: {fetched:,} fetched, {errors} errors")


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download GAO reports from gao.gov")
    parser.add_argument("--phase", choices=["listing", "detail", "all"],
                        default="all", help="Which phase to run")
    parser.add_argument("--limit", type=int, help="Limit pages (listing) or reports (detail)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    parser.add_argument("--full", action="store_true", help="Reset state and start fresh")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY,
                        help=f"Seconds between requests (default: {REQUEST_DELAY})")
    args = parser.parse_args()

    state = load_state()
    if args.full:
        state = {
            "listing_page": 0,
            "listing_complete": False,
            "listing_total": 0,
            "detail_index": 0,
            "detail_complete": False,
        }
        save_state(state)

    session = create_session()

    delay = args.delay
    log.info(f"=== GAO Direct Download ===")
    log.info(f"Phase: {args.phase}, Delay: {delay}s")

    if args.phase in ("listing", "all"):
        run_listing_phase(session, state, limit=args.limit, dry_run=args.dry_run, delay=delay)

    if args.phase in ("detail", "all"):
        run_detail_phase(session, state, limit=args.limit, dry_run=args.dry_run, delay=delay)


if __name__ == "__main__":
    main()
