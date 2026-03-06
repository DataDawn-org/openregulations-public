#!/usr/bin/env python3
"""
Phase 3b: Backfill truncated comment months with daily date windows.

The Phase 3 download hit the regulations.gov pagination cap (5,000 results)
on 72 EPA months, missing ~273K comment headers. This script re-queries
those months day-by-day, where daily volumes safely fit under the cap.

Daily results are saved to subdirectories within each month:
  {AGENCY}/{YYYY}/{MM}/d{DD}/page_{NNNN}.json

The original truncated monthly pages (page_0001–0020.json) are left in
place. When processing, use the daily files for backfilled months.
"""

import json
import sys
import time
import logging
import re
import signal
from calendar import monthrange
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
API_BASE = "https://api.regulations.gov/v4"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"
DATA_DIR = PROJECT_DIR / "regulations_gov/comments/headers"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "regs_comments_backfill_state.json"
COMMENTS_LOG = LOG_DIR / "regs_gov_comments.log"
PROGRESS_FILE = LOG_DIR / "progress.txt"

PAGE_SIZE = 250
MAX_PAGE = 20
MIN_INTERVAL = 3.6  # 1000 req/hr

with open(CONFIG_FILE) as f:
    API_KEY = json.load(f)["regulations_gov_api_key"]

# === Logging ===
log = logging.getLogger("backfill")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "regs_comments_backfill.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# === Graceful shutdown ===
_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received — saving state")
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# === Rate limiter ===
class RateLimiter:
    def __init__(self, interval):
        self.interval = interval
        self.last = 0
        self.count = 0

    def wait(self):
        elapsed = time.time() - self.last
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last = time.time()
        self.count += 1

rate = RateLimiter(MIN_INTERVAL)

# === HTTP Session ===
def create_session():
    s = requests.Session()
    s.headers["X-Api-Key"] = API_KEY
    retry = Retry(
        total=3,
        backoff_factor=5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

def api_get(session, params):
    rate.wait()
    url = f"{API_BASE}/comments"
    resp = session.get(url, params=params, timeout=120)
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        log.warning(f"Rate limited (429). Sleeping {retry_after}s...")
        time.sleep(retry_after)
        rate.wait()
        resp = session.get(url, params=params, timeout=120)
    if resp.status_code == 403:
        log.error(f"403 Forbidden: {resp.text[:500]}")
        raise RuntimeError("API key rejected (403)")
    resp.raise_for_status()
    return resp.json()

# === State ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "completed_days": {},
        "completed_months": [],
        "total_backfilled": 0,
        "total_new": 0,
        "started_at": None,
    }

def save_state(state):
    state["api_calls"] = rate.count
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(STATE_FILE)

def progress(msg):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

# === Find truncated months ===
def find_truncated_months():
    """Parse Phase 3 log for truncation warnings."""
    truncated = []
    pattern = re.compile(
        r"\[(\w+)\] (\d{4})-(\d{2}): (\d+) comments, only first 5000 saved"
    )
    with open(COMMENTS_LOG) as f:
        for line in f:
            m = pattern.search(line)
            if m:
                agency, year, month, actual = m.groups()
                truncated.append({
                    "agency": agency,
                    "year": int(year),
                    "month": int(month),
                    "actual": int(actual),
                    "saved": 5000,
                    "missed": int(actual) - 5000,
                })
    return truncated

# === Download ===
def download_day(session, agency, year, month, day, output_dir):
    """Download all comment headers for a single day."""
    date_str = f"{year}-{month:02d}-{day:02d}"
    params = {
        "filter[agencyId]": agency,
        "filter[postedDate][ge]": date_str,
        "filter[postedDate][le]": date_str,
        "sort": "postedDate",
        "page[size]": PAGE_SIZE,
        "page[number]": 1,
    }

    data = api_get(session, params)
    meta = data.get("meta", {})
    total_elements = meta.get("totalElements", 0)
    total_pages = min(meta.get("totalPages", 1), MAX_PAGE)

    if total_elements == 0:
        return 0

    day_dir = output_dir / f"d{day:02d}"
    day_dir.mkdir(parents=True, exist_ok=True)

    (day_dir / "page_0001.json").write_text(json.dumps(data))
    saved = len(data.get("data", []))

    for pg in range(2, total_pages + 1):
        if _shutdown:
            break
        params["page[number]"] = pg
        try:
            data = api_get(session, params)
            (day_dir / f"page_{pg:04d}.json").write_text(json.dumps(data))
            saved += len(data.get("data", []))
        except Exception as e:
            log.error(f"    {date_str} page {pg} failed: {e}")
            break

    if total_elements > MAX_PAGE * PAGE_SIZE:
        log.warning(f"    {date_str}: single day still exceeds cap ({total_elements} comments)")

    return saved


def backfill_month(session, entry, state):
    """Backfill one truncated month using daily windows."""
    agency = entry["agency"]
    year = entry["year"]
    month = entry["month"]
    month_key = f"{agency}:{year}-{month:02d}"

    if month_key in state["completed_months"]:
        log.info(f"  {month_key}: already backfilled, skipping")
        return 0

    last_day = monthrange(year, month)[1]
    output_dir = DATA_DIR / agency / str(year) / f"{month:02d}"
    month_total = 0

    for day in range(1, last_day + 1):
        if _shutdown:
            return month_total

        day_key = f"{agency}:{year}-{month:02d}-{day:02d}"

        if day_key in state["completed_days"]:
            month_total += state["completed_days"][day_key]
            continue

        try:
            count = download_day(session, agency, year, month, day, output_dir)
            state["completed_days"][day_key] = count
            state["total_backfilled"] += count
            month_total += count
            save_state(state)
        except Exception as e:
            log.error(f"    Failed {day_key}: {e}")

    if not _shutdown:
        state["completed_months"].append(month_key)
        new_recovered = max(0, month_total - entry["saved"])
        state["total_new"] += new_recovered
        save_state(state)
        log.info(
            f"  {month_key}: {month_total:,} total via daily "
            f"(+{new_recovered:,} new, was {entry['saved']:,}/{entry['actual']:,})"
        )

    return month_total


# === Main ===
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--agency", help="Only backfill this agency (e.g. EPA)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("COMMENT BACKFILL — Recovering truncated months")
    if args.agency:
        log.info(f"Filtering to agency: {args.agency}")
    log.info("=" * 60)

    truncated = find_truncated_months()
    if args.agency:
        truncated = [e for e in truncated if e["agency"] == args.agency]
    if not truncated:
        log.info("No truncated months found. Nothing to backfill.")
        return

    total_missed = sum(e["missed"] for e in truncated)
    est_days = sum(monthrange(e["year"], e["month"])[1] for e in truncated)
    est_hours = est_days * MIN_INTERVAL / 3600

    log.info(f"Truncated months: {len(truncated)}")
    log.info(f"Comments to recover: ~{total_missed:,}")
    log.info(f"Estimated API calls: ~{est_days:,} (one per day)")
    log.info(f"Estimated runtime: ~{est_hours:.1f} hours")
    log.info("=" * 60)
    progress(f"Backfill: STARTING — {len(truncated)} months, ~{total_missed:,} comments to recover")

    state = load_state()
    if not state["started_at"]:
        state["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)

    session = create_session()

    for i, entry in enumerate(truncated):
        if _shutdown:
            break
        log.info(
            f"\n[{i + 1}/{len(truncated)}] {entry['agency']} "
            f"{entry['year']}-{entry['month']:02d} "
            f"(need {entry['actual']:,}, have {entry['saved']:,}, "
            f"missing ~{entry['missed']:,})"
        )
        try:
            backfill_month(session, entry, state)
        except Exception as e:
            log.error(
                f"Failed {entry['agency']} "
                f"{entry['year']}-{entry['month']:02d}: {e}"
            )

        progress(
            f"Backfill: {i + 1}/{len(truncated)} months — "
            f"{state['total_new']:,} new comments recovered so far"
        )

    # Summary
    log.info("=" * 60)
    status = "Interrupted" if _shutdown else "Complete"
    log.info(f"COMMENT BACKFILL — {status}")
    log.info(f"Total comments via daily queries: {state['total_backfilled']:,}")
    log.info(f"New comments recovered: {state['total_new']:,}")
    log.info(f"Months backfilled: {len(state['completed_months'])}")
    log.info(f"API calls: {rate.count}")
    log.info("=" * 60)

    progress(
        f"Backfill: {status.upper()} — "
        f"{state['total_new']:,} new comments recovered, "
        f"{len(state['completed_months'])}/{len(truncated)} months, "
        f"{rate.count} API calls"
    )

if __name__ == "__main__":
    main()
