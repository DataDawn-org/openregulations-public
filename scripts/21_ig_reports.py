#!/usr/bin/env python3
"""
Phase 21: Download Inspector General reports from oversight.gov.

Scrapes ~35,000 IG reports from oversight.gov (CIGIE's central repository
of Inspector General reports from 73+ federal OIGs).

Two-phase approach:
  Phase 1 (listing): Scrape report listing pages for core metadata + accordion data
  Phase 2 (detail):  Scrape individual report pages for financial data, PDFs, recommendations

Source: oversight.gov (Council of Inspectors General on Integrity & Efficiency)
Auth: None (public data)
Rate: 5 seconds between requests (per robots.txt Crawl-delay)

Usage:
    python3 21_ig_reports.py --phase listing        # Phase 1: listing pages (~30 min)
    python3 21_ig_reports.py --phase detail          # Phase 2: detail pages (~48 hrs)
    python3 21_ig_reports.py --phase all             # Both phases sequentially
    python3 21_ig_reports.py --phase listing --limit 5   # Test with 5 listing pages
    python3 21_ig_reports.py --dry-run               # Preview without saving

Timing estimates:
  Phase 1 (listing): ~351 pages × 5s = ~30 minutes → ~35K report stubs
  Phase 2 (detail):  ~35K pages × 5s = ~48 hours → full metadata + recommendations
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
OUTPUT_DIR = PROJECT_DIR / "ig_reports"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "ig_reports_state.json"
REQUEST_DELAY = 5.0  # per robots.txt Crawl-delay
LISTING_URL = "https://www.oversight.gov/reports/federal"
SITE_BASE = "https://www.oversight.gov"
ITEMS_PER_PAGE = 100

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("ig_reports")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "ig_reports.log")
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
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
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
    """Parse a listing page table and extract report stubs including accordion data."""
    soup = BeautifulSoup(html, "lxml")
    reports = []

    table = soup.select_one("table.listing-table")
    if not table:
        return reports, 0, 0

    # All rows are in a single <tbody>. Each report = 3 rows:
    #   1. tr.listing-table__row (main data)
    #   2. tr.listing-table__accordion-toggle (button)
    #   3. tr.listing-table__container (hidden highlights)
    main_rows = table.select("tr.listing-table__row.table-row")
    container_rows = table.select("tr.listing-table__container")

    # Build a lookup from main row index to its container row
    # They appear in the same order, so zip them
    containers = list(container_rows)

    for idx, main_row in enumerate(main_rows):
        report = {}

        # Date
        date_td = main_row.select_one("td.views-field-field-report-date-issued")
        if date_td:
            time_el = date_td.select_one("time[datetime]")
            if time_el:
                report["date_issued"] = time_el.get("datetime", "")[:10]
            else:
                report["date_issued"] = date_td.get_text(strip=True)

        # Agency reviewed
        agency_td = main_row.select_one("td.views-field-field-report-agency-reviewed")
        if agency_td:
            report["agency_reviewed"] = agency_td.get_text(strip=True)

        # Title
        title_td = main_row.select_one("td.views-field-title")
        if title_td:
            report["title"] = title_td.get_text(strip=True)

        # Type
        type_td = main_row.select_one("td.views-field-field-report-type")
        if type_td:
            report["report_type"] = type_td.get_text(strip=True)

        # Location
        loc_td = main_row.select_one("td.views-field-field-report-location")
        if loc_td:
            locality = loc_td.select_one("span.locality")
            state_el = loc_td.select_one("span.administrative-area")
            parts = []
            if locality:
                parts.append(locality.get_text(strip=True))
            if state_el:
                parts.append(state_el.get_text(strip=True))
            report["location"] = ", ".join(parts) if parts else loc_td.get_text(strip=True)

        # Detail URL
        action_td = main_row.select_one("td.action-cell a[href]")
        if action_td:
            href = action_td.get("href", "")
            report["detail_url"] = href

        # Accordion data (hidden highlights row)
        if idx < len(containers):
            accordion_row = containers[idx]

            # Submitting OIG
            oig_div = accordion_row.select_one("div.field--name-field-report-submitting-oig")
            if oig_div:
                item = oig_div.select_one("div.field__item")
                if item:
                    report["submitting_oig"] = item.get_text(strip=True)

            # Report description
            body_div = accordion_row.select_one("div.field--name-body")
            if body_div:
                item = body_div.select_one("div.field__item")
                if item:
                    report["description"] = item.get_text("\n", strip=True)

            # Number of recommendations
            recs_div = accordion_row.select_one("div.field--name-field-report-number-of-recs")
            if recs_div:
                item = recs_div.select_one("div.field__item")
                if item:
                    try:
                        report["num_recommendations"] = int(item.get_text(strip=True))
                    except ValueError:
                        report["num_recommendations_text"] = item.get_text(strip=True)

            # Report number
            num_div = accordion_row.select_one("div.field--name-field-report-number")
            if num_div:
                item = num_div.select_one("div.field__item")
                if item:
                    report["report_number"] = item.get_text(strip=True)

        if report.get("title"):
            reports.append(report)

    # Total count from footer
    total = 0
    footer = soup.select_one("div.view-footer")
    if footer:
        m = re.search(r"of\s+([\d,]+)", footer.get_text())
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
    """Scrape all listing pages to discover IG reports."""
    start_page = state.get("listing_page", 0)

    if state.get("listing_complete"):
        log.info("Listing phase already complete. Use --full to re-run.")
        return

    log.info(f"=== Phase 1: Listing (starting at page {start_page}) ===")

    # First request to get total count
    resp = session.get(f"{LISTING_URL}?items_per_page={ITEMS_PER_PAGE}&page=0", timeout=30)
    resp.raise_for_status()
    _, total, last_page = parse_listing_page(resp.text)
    # Calculate last_page from total since pagination links may be truncated
    if total > 0:
        last_page = max(last_page, (total - 1) // ITEMS_PER_PAGE)
    log.info(f"Total reports: {total:,} across {last_page + 1} pages")
    state["listing_total"] = total
    time.sleep(delay)

    # Load existing stubs
    listing_file = OUTPUT_DIR / "_listing_stubs.json"
    existing_stubs = []
    seen_urls = set()
    if listing_file.exists():
        existing_stubs = json.loads(listing_file.read_text())
        seen_urls = {r.get("detail_url", "") for r in existing_stubs if r.get("detail_url")}
        log.info(f"Loaded {len(existing_stubs)} existing stubs")

    page_limit = last_page + 1
    if limit:
        page_limit = min(start_page + limit, last_page + 1)

    for page_num in range(start_page, page_limit):
        try:
            url = f"{LISTING_URL}?items_per_page={ITEMS_PER_PAGE}&page={page_num}"
            resp = session.get(url, timeout=30)
            resp.raise_for_status()

            reports, _, _ = parse_listing_page(resp.text)

            if dry_run:
                for r in reports:
                    log.info(f"  [{r.get('date_issued', '?')}] {r.get('agency_reviewed', '?')}: "
                             f"{r.get('title', '')[:60]}")
            else:
                for r in reports:
                    detail_url = r.get("detail_url", "")
                    if detail_url and detail_url not in seen_urls:
                        seen_urls.add(detail_url)
                        existing_stubs.append(r)

            if (page_num + 1) % 10 == 0 or page_num == start_page:
                log.info(f"  Page {page_num + 1}/{page_limit}: {len(reports)} reports "
                         f"(total unique: {len(existing_stubs):,})")
                if not dry_run:
                    state["listing_page"] = page_num + 1
                    save_state(state)
                    listing_file.write_text(json.dumps(
                        existing_stubs, indent=2, ensure_ascii=False
                    ))

        except requests.exceptions.HTTPError as e:
            log.error(f"  Page {page_num}: HTTP {e.response.status_code if e.response else '?'}")
            if e.response is not None and e.response.status_code == 403:
                log.warning("  Got 403 — backing off 60 seconds")
                time.sleep(60)
        except Exception as e:
            log.error(f"  Page {page_num}: {e}")

        time.sleep(delay)

    if not dry_run:
        listing_file.write_text(json.dumps(
            existing_stubs, indent=2, ensure_ascii=False
        ))
        if page_num >= last_page:
            state["listing_complete"] = True
        state["listing_page"] = page_num + 1
        save_state(state)

    log.info(f"Listing phase: {len(existing_stubs):,} unique reports found")


# === Phase 2: Detail Pages ===
def parse_detail_page(html):
    """Parse an individual report detail page for full metadata."""
    soup = BeautifulSoup(html, "lxml")
    detail = {}

    article = soup.select_one("article.node--type-report")
    if not article:
        # Try broader selector
        article = soup

    # Helper to extract field value
    def get_field(field_name):
        div = article.select_one(f"div.field--name-{field_name}")
        if div:
            item = div.select_one("div.field__item")
            if item:
                return item.get_text(strip=True)
        return None

    def get_field_html(field_name):
        div = article.select_one(f"div.field--name-{field_name}")
        if div:
            item = div.select_one("div.field__item")
            if item:
                return item.get_text("\n", strip=True)
        return None

    def get_field_items(field_name):
        """Get all field__item values for multi-value fields."""
        div = article.select_one(f"div.field--name-{field_name}")
        if div:
            return [item.get_text(strip=True) for item in div.select("div.field__item")]
        return []

    def get_field_int(field_name):
        """Get a field value, trying the content attribute first for formatted numbers."""
        div = article.select_one(f"div.field--name-{field_name}")
        if div:
            item = div.select_one("div.field__item")
            if item:
                # Check content attribute (raw integer for currency fields)
                content = item.get("content")
                if content is not None:
                    try:
                        return int(content)
                    except (ValueError, TypeError):
                        pass
                # Fall back to text
                text = item.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    return int(text)
                except ValueError:
                    return None
        return None

    # Core fields
    detail["title_full"] = get_field("field-title-full")
    detail["date_issued"] = None
    date_div = article.select_one("div.field--name-field-report-date-issued")
    if date_div:
        time_el = date_div.select_one("time[datetime]")
        if time_el:
            detail["date_issued"] = time_el.get("datetime", "")[:10]

    detail["submitting_oig"] = get_field("field-report-submitting-oig")
    detail["agencies_reviewed"] = get_field_items("field-report-agency-reviewed")
    detail["components"] = get_field_items("field-report-components")
    detail["report_number"] = get_field("field-report-number")
    detail["report_type"] = get_field("field-report-type")
    detail["description"] = get_field_html("body")
    detail["external_entity"] = get_field("field-report-external-entity")
    detail["agency_wide"] = get_field("field-report-agency-wide")

    # Location
    loc_div = article.select_one("div.field--name-field-report-location")
    if loc_div:
        locality = loc_div.select_one("span.locality")
        state_el = loc_div.select_one("span.administrative-area")
        parts = []
        if locality:
            parts.append(locality.get_text(strip=True))
        if state_el:
            parts.append(state_el.get_text(strip=True))
        detail["location"] = ", ".join(parts) if parts else None

    # Financial fields
    detail["questioned_costs"] = get_field_int("field-net-questioned-costs")
    detail["funds_for_better_use"] = get_field_int("field-net-funds-for-better-use")
    detail["num_recommendations"] = get_field_int("field-report-number-of-recs")

    # PDF download link
    pdf_div = article.select_one("div.field--name-field-report-file")
    if pdf_div:
        pdf_link = pdf_div.select_one("a[href$='.pdf']")
        if pdf_link:
            href = pdf_link.get("href", "")
            detail["pdf_url"] = href if href.startswith("http") else SITE_BASE + href
    # Also check media field
    if not detail.get("pdf_url"):
        media_div = article.select_one("div.field--name-field-media-document")
        if media_div:
            pdf_link = media_div.select_one("a[href$='.pdf']")
            if pdf_link:
                href = pdf_link.get("href", "")
                detail["pdf_url"] = href if href.startswith("http") else SITE_BASE + href

    # External link (to originating OIG website)
    ext_link_div = article.select_one("div.field--name-field-report-link")
    if ext_link_div:
        link = ext_link_div.select_one("a[href]")
        if link:
            detail["external_url"] = link.get("href", "")

    # NDAA 5274
    detail["ndaa_5274"] = get_field("field-report-ndaa-5274")

    # Open recommendations table
    recs_block = soup.select_one("div#block-oversight-views-block-report-recommendations-block-1, "
                                 "div.view-report-recommendations")
    if recs_block:
        recommendations = []
        # Each recommendation is 2 rows: data row + text row
        rows = recs_block.select("tbody tr.listing-table__row")
        i = 0
        while i < len(rows):
            row = rows[i]
            rec = {}

            # Recommendation number
            num_td = row.select_one("td.views-field-field-rec-number")
            if num_td:
                rec["rec_number"] = num_td.get_text(strip=True)

            # Significant
            sig_td = row.select_one("td.views-field-field-rec-significant")
            if sig_td:
                rec["significant"] = sig_td.get_text(strip=True)

            # Questioned costs
            qc_td = row.select_one("td.views-field-field-net-questioned-costs")
            if qc_td:
                text = qc_td.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    rec["questioned_costs"] = int(text)
                except ValueError:
                    rec["questioned_costs_text"] = qc_td.get_text(strip=True)

            # Funds for better use
            fbu_td = row.select_one("td.views-field-field-net-funds-for-better-use")
            if fbu_td:
                text = fbu_td.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    rec["funds_for_better_use"] = int(text)
                except ValueError:
                    rec["funds_for_better_use_text"] = fbu_td.get_text(strip=True)

            # Check next row for recommendation text
            if i + 1 < len(rows):
                next_row = rows[i + 1]
                text_td = next_row.select_one("td[colspan]")
                if text_td:
                    rec["text"] = text_td.get_text("\n", strip=True)
                    i += 1  # Skip the text row

            if rec.get("rec_number"):
                recommendations.append(rec)
            i += 1

        if recommendations:
            detail["recommendations"] = recommendations

    # Remove None values
    return {k: v for k, v in detail.items() if v is not None}


def make_report_id(stub):
    """Generate a filesystem-safe ID from a report stub."""
    # Use detail_url path as the unique ID
    url = stub.get("detail_url", "")
    if url:
        # /reports/audit/audit-office-justice-programs... -> audit-office-justice-programs...
        slug = url.rstrip("/").split("/")[-1]
        # Truncate very long slugs
        if len(slug) > 120:
            slug = slug[:120]
        return slug
    # Fallback to report number
    num = stub.get("report_number", "")
    if num:
        return re.sub(r"[^\w\-]", "_", num)
    return None


def run_detail_phase(session, state, limit=None, dry_run=False, delay=REQUEST_DELAY):
    """Scrape individual report detail pages for full metadata."""
    listing_file = OUTPUT_DIR / "_listing_stubs.json"
    if not listing_file.exists():
        log.error("No listing stubs found. Run --phase listing first.")
        return

    stubs = json.loads(listing_file.read_text())
    log.info(f"=== Phase 2: Detail ({len(stubs):,} reports) ===")

    # Check which ones already have detail files
    existing = {f.stem for f in OUTPUT_DIR.glob("*.json") if not f.name.startswith("_")}
    log.info(f"Already have {len(existing):,} detail files")

    # Build work list
    work_list = []
    for stub in stubs:
        report_id = make_report_id(stub)
        if report_id and report_id not in existing:
            work_list.append((report_id, stub))

    if limit:
        work_list = work_list[:limit]

    log.info(f"Need to fetch {len(work_list):,} detail pages")

    fetched = 0
    errors = 0

    for i, (report_id, stub) in enumerate(work_list):
        detail_url = stub.get("detail_url", "")
        url = f"{SITE_BASE}{detail_url}" if detail_url.startswith("/") else detail_url

        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()

            detail = parse_detail_page(resp.text)

            # Merge stub + detail
            merged = {**stub, **detail}
            merged["_report_id"] = report_id

            if dry_run:
                log.info(f"  {report_id}: {merged.get('title', '')[:60]}")
                log.info(f"    QC: ${merged.get('questioned_costs', 0):,} | "
                         f"FBU: ${merged.get('funds_for_better_use', 0):,} | "
                         f"Recs: {merged.get('num_recommendations', 0)}")
            else:
                out_file = OUTPUT_DIR / f"{report_id}.json"
                out_file.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
                fetched += 1

            if fetched % 100 == 0 and fetched > 0:
                log.info(f"  Progress: {fetched:,}/{len(work_list):,} fetched, {errors} errors")
                state["detail_index"] = i + 1
                save_state(state)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            if status == 403:
                log.warning(f"  {report_id}: 403 — backing off 60s")
                time.sleep(60)
            elif status == 404:
                log.warning(f"  {report_id}: 404 — skipping")
                if not dry_run:
                    out_file = OUTPUT_DIR / f"{report_id}.json"
                    out_file.write_text(json.dumps({**stub, "_error": "404"}, indent=2))
            else:
                log.error(f"  {report_id}: HTTP {status}")
            errors += 1
        except Exception as e:
            log.error(f"  {report_id}: {e}")
            errors += 1

        time.sleep(delay)

    if not dry_run:
        state["detail_index"] = len(work_list)
        if not work_list or errors == 0:
            state["detail_complete"] = True
        save_state(state)

    log.info(f"Detail phase complete: {fetched:,} fetched, {errors} errors")


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download IG reports from oversight.gov")
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
    log.info(f"=== IG Reports Download ===")
    log.info(f"Phase: {args.phase}, Delay: {delay}s")

    if args.phase in ("listing", "all"):
        run_listing_phase(session, state, limit=args.limit, dry_run=args.dry_run, delay=delay)

    if args.phase in ("detail", "all"):
        run_detail_phase(session, state, limit=args.limit, dry_run=args.dry_run, delay=delay)


if __name__ == "__main__":
    main()
