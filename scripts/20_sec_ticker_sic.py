#!/usr/bin/env python3
"""
Download SIC codes for stock tickers from SEC EDGAR.

1. Fetches company_tickers.json (all SEC-registered companies with tickers)
2. Matches against tickers in stock_trades table
3. Looks up SIC code for each matched CIK via EDGAR submissions API
4. Saves ticker_sic.json for use in database build

SEC EDGAR rate limit: 10 requests/second with User-Agent header.
~2,000 lookups takes ~4 minutes.
"""

import json
import logging
import sqlite3
import time
import urllib.request
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "openregs.db"
OUTPUT_DIR = BASE_DIR / "sec_tickers"
OUTPUT_FILE = OUTPUT_DIR / "ticker_sic.json"
STATE_FILE = OUTPUT_DIR / "state.json"

USER_AGENT = "DataDawn data@datadawn.org"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"

# Rate limit: 10 req/sec
MIN_INTERVAL = 0.11  # slightly over 100ms


def fetch_json(url):
    """Fetch JSON from URL with proper User-Agent."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def get_traded_tickers():
    """Get unique tickers from stock_trades table."""
    if not DB_PATH.exists():
        log.error(f"Database not found: {DB_PATH}")
        return set()

    db = sqlite3.connect(str(DB_PATH))
    rows = db.execute(
        "SELECT DISTINCT ticker FROM stock_trades WHERE ticker IS NOT NULL AND ticker <> ''"
    ).fetchall()
    db.close()

    # Clean: strip leading dashes (PDF parsing artifacts), uppercase
    cleaned = set()
    for (ticker,) in rows:
        t = ticker.strip().lstrip("-").upper()
        if t and len(t) <= 5 and "." not in t:
            cleaned.add(t)

    return cleaned


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Load state for resume
    completed = {}
    if STATE_FILE.exists():
        state = json.loads(STATE_FILE.read_text())
        completed = state.get("completed", {})
        log.info(f"Resuming: {len(completed)} CIKs already looked up")

    # Load existing results
    results = {}
    if OUTPUT_FILE.exists():
        results = json.loads(OUTPUT_FILE.read_text())
        log.info(f"Loaded {len(results)} existing ticker→SIC mappings")

    # Step 1: Get SEC tickers
    log.info("Fetching SEC company tickers...")
    sec_data = fetch_json(SEC_TICKERS_URL)
    sec_tickers = {}
    for entry in sec_data.values():
        ticker = entry["ticker"].upper()
        sec_tickers[ticker] = {
            "cik": int(entry["cik_str"]),
            "name": entry["title"],
        }
    log.info(f"  SEC has {len(sec_tickers):,} tickers")

    # Step 2: Match against our traded tickers
    our_tickers = get_traded_tickers()
    log.info(f"  We trade {len(our_tickers):,} unique tickers")

    matched = {t: sec_tickers[t] for t in our_tickers if t in sec_tickers}
    log.info(f"  Matched: {len(matched):,} ({100*len(matched)/max(len(our_tickers),1):.1f}%)")

    # Step 3: Look up SIC for each unique CIK
    cik_to_tickers = {}
    for ticker, info in matched.items():
        cik = info["cik"]
        if cik not in cik_to_tickers:
            cik_to_tickers[cik] = []
        cik_to_tickers[cik].append(ticker)

    # Filter out already-completed CIKs
    pending_ciks = [cik for cik in cik_to_tickers if str(cik) not in completed]
    log.info(f"  Unique CIKs: {len(cik_to_tickers):,} ({len(pending_ciks):,} pending)")

    if not pending_ciks:
        log.info("All CIKs already looked up!")
    else:
        log.info(f"Looking up SIC codes for {len(pending_ciks):,} CIKs...")
        last_request = 0
        errors = 0

        for i, cik in enumerate(pending_ciks):
            # Rate limit
            elapsed = time.time() - last_request
            if elapsed < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - elapsed)

            url = SEC_SUBMISSIONS_URL.format(cik=cik)
            try:
                last_request = time.time()
                data = fetch_json(url)
                sic = data.get("sic", "")
                sic_desc = data.get("sicDescription", "")
                name = data.get("name", "")
                exchanges = data.get("exchanges", [])

                # Store result for all tickers with this CIK
                for ticker in cik_to_tickers[cik]:
                    results[ticker] = {
                        "ticker": ticker,
                        "cik": str(cik),
                        "company_name": name,
                        "sic_code": sic,
                        "sic_description": sic_desc,
                        "exchange": exchanges[0] if exchanges else "",
                    }

                completed[str(cik)] = sic

            except Exception as e:
                errors += 1
                log.warning(f"  Error for CIK {cik}: {e}")
                if errors > 20:
                    log.error("Too many errors, stopping")
                    break

            # Progress
            if (i + 1) % 200 == 0:
                log.info(f"  {i+1}/{len(pending_ciks)} CIKs processed ({len(results):,} tickers mapped)")
                # Checkpoint
                OUTPUT_FILE.write_text(json.dumps(results, indent=2))
                STATE_FILE.write_text(json.dumps({"completed": completed}))

        # Final save
        OUTPUT_FILE.write_text(json.dumps(results, indent=2))
        STATE_FILE.write_text(json.dumps({"completed": completed}))

    # Stats
    sic_counts = {}
    for r in results.values():
        sic = r.get("sic_code", "")
        if sic:
            sic_counts[sic] = sic_counts.get(sic, 0) + 1

    log.info(f"\nResults: {len(results):,} tickers with SIC codes")
    log.info(f"Unique SIC codes: {len(sic_counts):,}")
    log.info(f"Errors: {errors if 'errors' in dir() else 0}")

    # Show top SIC codes
    log.info("\nTop SIC codes:")
    for sic, count in sorted(sic_counts.items(), key=lambda x: -x[1])[:15]:
        desc = ""
        for r in results.values():
            if r.get("sic_code") == sic:
                desc = r.get("sic_description", "")
                break
        log.info(f"  {sic} ({desc}): {count} tickers")


if __name__ == "__main__":
    main()
