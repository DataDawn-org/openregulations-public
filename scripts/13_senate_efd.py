#!/usr/bin/env python3
"""
Script 13: Download Senate Electronic Financial Disclosures (eFD)

Downloads Periodic Transaction Reports (PTRs) from efdsearch.senate.gov,
the official government source for Senate stock trading disclosures.

Data covers 2012-present. Each PTR contains individual stock transactions
with ticker, date, buy/sell type, and dollar amount range.

This replaces the third-party senate-stock-watcher-data source with
authoritative government data.

Rate-limited to ~1 request/second to be respectful of the Senate server.
"""

import json
import logging
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("senate_efd")

# === Paths ===
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "stock_trades" / "senate_efd"
STATE_FILE = OUTPUT_DIR / "state.json"

# === eFD Configuration ===
ROOT = "https://efdsearch.senate.gov"
LANDING_URL = f"{ROOT}/search/home/"
SEARCH_URL = f"{ROOT}/search/"
REPORTS_URL = f"{ROOT}/search/report/data/"

BATCH_SIZE = 100
RATE_LIMIT = 1.0  # seconds between requests

# Report type 11 = Periodic Transaction Report (PTR)
# These contain individual stock trades
REPORT_TYPE_PTR = "[11]"


class EFDSession:
    """Manages authenticated session with efdsearch.senate.gov."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "OpenRegs/1.0 (civic data project; https://regs.datadawn.org)"
        })
        self.csrf_token = None
        self._authenticate()

    def _authenticate(self):
        """Accept the usage agreement and get CSRF token."""
        # Step 1: Get landing page
        r = self.session.get(LANDING_URL)
        r.raise_for_status()

        # Step 2: Extract form CSRF token
        m = re.search(r'csrfmiddlewaretoken" value="([^"]+)', r.text)
        if not m:
            raise RuntimeError("Could not find CSRF token on landing page")
        form_csrf = m.group(1)

        # Step 3: Accept agreement
        self.session.post(
            LANDING_URL,
            data={
                "csrfmiddlewaretoken": form_csrf,
                "prohibition_agreement": "1",
            },
            headers={"Referer": LANDING_URL},
        )

        # Step 4: Get cookie-based CSRF token for API calls
        self.csrf_token = self.session.cookies.get(
            "csrftoken", self.session.cookies.get("csrf")
        )
        if not self.csrf_token:
            raise RuntimeError("Could not get CSRF cookie after agreement")

        log.info("Authenticated with efdsearch.senate.gov")

    def search_reports(self, start=0, length=BATCH_SIZE, start_date="01/01/2012 00:00:00"):
        """Search for PTR filings. Returns (total_count, list_of_reports)."""
        r = self.session.post(
            REPORTS_URL,
            data={
                "start": str(start),
                "length": str(length),
                "report_types": REPORT_TYPE_PTR,
                "filer_types": "[]",
                "submitted_start_date": start_date,
                "submitted_end_date": "",
                "candidate_state": "",
                "senator_state": "",
                "office_id": "",
                "first_name": "",
                "last_name": "",
                "csrfmiddlewaretoken": self.csrf_token,
            },
            headers={"Referer": SEARCH_URL},
        )

        if r.status_code == 403:
            log.warning("Session expired, re-authenticating...")
            self._authenticate()
            return self.search_reports(start, length, start_date)

        r.raise_for_status()
        data = r.json()
        return data["recordsTotal"], data["data"]

    def fetch_report(self, report_url):
        """Fetch and parse a single PTR report page. Returns list of transactions."""
        full_url = f"{ROOT}{report_url}" if report_url.startswith("/") else report_url

        r = self.session.get(full_url)
        if r.status_code == 403 or r.url == LANDING_URL:
            log.warning("Session expired during report fetch, re-authenticating...")
            self._authenticate()
            r = self.session.get(full_url)

        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find("table")
        if not table:
            return []

        transactions = []
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]

        for row in table.find_all("tr")[1:]:  # Skip header row
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 7:
                continue

            tx = {}
            for i, h in enumerate(headers):
                if i < len(cells):
                    val = cells[i]
                    if val == "--":
                        val = ""
                    tx[h] = val

            transactions.append({
                "transaction_date": tx.get("transaction date", ""),
                "owner": tx.get("owner", ""),
                "ticker": tx.get("ticker", ""),
                "asset_name": tx.get("asset name", ""),
                "asset_type": tx.get("asset type", ""),
                "transaction_type": tx.get("type", ""),
                "amount": tx.get("amount", ""),
                "comment": tx.get("comment", ""),
            })

        return transactions


def parse_report_listing(row):
    """Parse a single row from the search results.

    Row format: [first_name, last_name, filer_name, report_link_html, date_filed]
    """
    first_name = row[0]
    last_name = row[1]
    filer_name = row[2]  # e.g., "Hagerty, Bill (Senator)"
    link_html = row[3]
    date_filed = row[4]

    # Extract URL from the <a> tag
    m = re.search(r'href="([^"]+)"', link_html)
    report_url = m.group(1) if m else ""

    # Extract report title
    m2 = re.search(r'>([^<]+)<', link_html)
    report_title = m2.group(1) if m2 else ""

    # Extract UUID from URL
    uuid_match = re.search(r'ptr/([0-9a-f-]+)/', report_url)
    report_id = uuid_match.group(1) if uuid_match else ""

    return {
        "report_id": report_id,
        "first_name": first_name,
        "last_name": last_name,
        "filer_name": filer_name,
        "report_url": report_url,
        "report_title": report_title,
        "date_filed": date_filed,
    }


def load_state():
    """Load download state (completed report IDs)."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"completed_reports": [], "transactions": []}


def save_state(state):
    """Save download state."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("SENATE eFD: Downloading Periodic Transaction Reports")
    log.info("=" * 60)

    efd = EFDSession()

    # Phase 1: Get all PTR report listings
    log.info("Phase 1: Fetching PTR report listings...")
    total, first_batch = efd.search_reports(start=0)
    log.info(f"  Total PTR filings: {total}")

    all_reports = []
    for row in first_batch:
        all_reports.append(parse_report_listing(row))

    offset = BATCH_SIZE
    while offset < total:
        time.sleep(RATE_LIMIT)
        _, batch = efd.search_reports(start=offset)
        if not batch:
            break
        for row in batch:
            all_reports.append(parse_report_listing(row))
        offset += BATCH_SIZE
        if offset % 500 == 0:
            log.info(f"  Fetched {offset}/{total} listings...")

    log.info(f"  Got {len(all_reports)} report listings")

    # Save report index
    index_file = OUTPUT_DIR / "report_index.json"
    with open(index_file, "w") as f:
        json.dump(all_reports, f, indent=2)

    # Phase 2: Load state and download individual reports
    state = load_state()
    completed = set(state["completed_reports"])
    all_transactions = state.get("transactions", [])

    pending = [r for r in all_reports if r["report_id"] and r["report_id"] not in completed]
    log.info(f"Phase 2: Downloading {len(pending)} reports ({len(completed)} already done)...")

    errors = 0
    for i, report in enumerate(pending):
        try:
            time.sleep(RATE_LIMIT)
            txs = efd.fetch_report(report["report_url"])

            # Enrich transactions with senator info
            for tx in txs:
                tx["senator_first_name"] = report["first_name"]
                tx["senator_last_name"] = report["last_name"]
                tx["senator_filer_name"] = report["filer_name"]
                tx["report_id"] = report["report_id"]
                tx["date_filed"] = report["date_filed"]
                tx["source_url"] = f"{ROOT}{report['report_url']}"

            all_transactions.extend(txs)
            completed.add(report["report_id"])

            if (i + 1) % 50 == 0:
                log.info(f"  {i + 1}/{len(pending)} reports downloaded "
                         f"({len(all_transactions)} transactions so far)")
                # Save checkpoint
                state["completed_reports"] = list(completed)
                state["transactions"] = all_transactions
                save_state(state)

        except Exception as e:
            errors += 1
            log.warning(f"  Error on {report['report_id']}: {e}")
            if errors > 20:
                log.error("Too many errors, stopping")
                break

    # Final save
    state["completed_reports"] = list(completed)
    state["transactions"] = all_transactions
    save_state(state)

    # Also save transactions as a clean JSON file
    output_file = OUTPUT_DIR / "all_transactions.json"
    with open(output_file, "w") as f:
        json.dump(all_transactions, f, indent=2)

    log.info(f"\nDone: {len(completed)} reports, {len(all_transactions)} transactions")
    log.info(f"  Errors: {errors}")
    log.info(f"  Output: {output_file}")

    # Summary stats
    senators = set()
    for tx in all_transactions:
        name = f"{tx.get('senator_first_name', '')} {tx.get('senator_last_name', '')}".strip()
        if name:
            senators.add(name)
    log.info(f"  Unique senators: {len(senators)}")

    # Date range
    dates = [tx["transaction_date"] for tx in all_transactions if tx.get("transaction_date")]
    if dates:
        log.info(f"  Date range: {min(dates)} to {max(dates)}")


if __name__ == "__main__":
    main()
