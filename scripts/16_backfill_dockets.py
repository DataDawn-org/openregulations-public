#!/usr/bin/env python3
"""
Phase 16: Backfill stub dockets by fetching each one by ID from the Regulations.gov API.

The database has ~72K stub dockets (id + agency_id only) that were backfilled from
documents/comments referencing dockets not yet downloaded. This script fetches the
full docket record for each stub.

Raw JSON saved to: regulations_gov/dockets_backfill/{docket_id}.json
State tracked in:  logs/docket_backfill_state.json (resumable)

Usage:
  python3 scripts/16_backfill_dockets.py                    # all stubs
  python3 scripts/16_backfill_dockets.py --agency EPA       # EPA stubs only
  python3 scripts/16_backfill_dockets.py --agency FDA       # FDA stubs only
  python3 scripts/16_backfill_dockets.py --api-key-2        # use second API key
  python3 scripts/16_backfill_dockets.py --limit 1000       # stop after N
"""

import argparse
import json
import logging
import signal
import sqlite3
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
API_BASE = "https://api.regulations.gov/v4"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"
DB_PATH = PROJECT_DIR / "openregs.db"
DATA_DIR = PROJECT_DIR / "regulations_gov" / "dockets_backfill"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "docket_backfill_state.json"

MIN_INTERVAL = 3.6  # seconds between requests (~1,000/hr)

with open(CONFIG_FILE) as f:
    _cfg = json.load(f)

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("docket_backfill")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "docket_backfill.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# === Graceful shutdown ===
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received — saving state and exiting")
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


# === HTTP Session ===
def create_session(api_key):
    s = requests.Session()
    s.headers["X-Api-Key"] = api_key
    retry = Retry(
        total=3,
        backoff_factor=5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


# === State ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"completed": [], "errors": [], "total_fetched": 0, "started_at": None}


def save_state(state):
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.rename(STATE_FILE)


# === Main ===
def get_stub_dockets(agency=None):
    """Get docket IDs that are stubs (no title) from the database."""
    if not DB_PATH.exists():
        log.error(f"Database not found: {DB_PATH}")
        return []

    db = sqlite3.connect(str(DB_PATH))
    query = "SELECT id, agency_id FROM dockets WHERE title IS NULL OR title = ''"
    if agency:
        query += f" AND agency_id = '{agency}'"
    query += " ORDER BY agency_id, id"
    rows = db.execute(query).fetchall()
    db.close()
    return rows


def fetch_docket(session, rate, docket_id):
    """Fetch a single docket by ID."""
    rate.wait()
    url = f"{API_BASE}/dockets/{docket_id}"
    resp = session.get(url, timeout=120)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        log.warning(f"Rate limited (429). Sleeping {retry_after}s...")
        time.sleep(retry_after)
        rate.wait()
        resp = session.get(url, timeout=120)

    if resp.status_code == 404:
        return None  # docket doesn't exist in API

    if resp.status_code == 403:
        log.error(f"403 Forbidden — check API key. Response: {resp.text[:500]}")
        raise RuntimeError("API key rejected (403)")

    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(
        description="Backfill stub dockets by fetching each by ID from Regulations.gov."
    )
    parser.add_argument("--agency", help="Only backfill this agency (e.g., EPA, FDA)")
    parser.add_argument("--api-key-2", action="store_true", help="Use second API key")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N dockets")
    args = parser.parse_args()

    api_key = _cfg["regulations_gov_api_key_2"] if args.api_key_2 else _cfg["regulations_gov_api_key"]

    # Get stub docket list
    stubs = get_stub_dockets(args.agency)
    log.info(f"Found {len(stubs):,} stub dockets" + (f" for {args.agency}" if args.agency else ""))

    if not stubs:
        log.info("No stub dockets to backfill!")
        return

    # Load state for resume
    state = load_state()
    if not state["started_at"]:
        state["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

    completed_set = set(state["completed"])
    pending = [(did, aid) for did, aid in stubs if did not in completed_set]
    log.info(f"Already completed: {len(completed_set):,}, pending: {len(pending):,}")

    if args.limit:
        pending = pending[:args.limit]
        log.info(f"Limited to {args.limit} dockets")

    if not pending:
        log.info("All stubs already backfilled!")
        return

    # Setup
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rate = RateLimiter(MIN_INTERVAL)
    session = create_session(api_key)
    errors = 0
    fetched = 0
    start_time = time.time()

    log.info(f"Starting backfill: {len(pending):,} dockets at ~{int(3600/MIN_INTERVAL)}/hr")
    log.info(f"Estimated time: {len(pending) * MIN_INTERVAL / 3600:.1f} hours")

    for i, (docket_id, agency_id) in enumerate(pending):
        if _shutdown:
            break

        try:
            data = fetch_docket(session, rate, docket_id)

            if data is None:
                # 404 — docket doesn't exist
                state["errors"].append(docket_id)
                errors += 1
            else:
                # Save the raw JSON
                outfile = DATA_DIR / f"{docket_id}.json"
                outfile.write_text(json.dumps(data, indent=2))
                fetched += 1

            state["completed"].append(docket_id)
            state["total_fetched"] = fetched

        except Exception as e:
            errors += 1
            state["errors"].append(docket_id)
            log.warning(f"Error for {docket_id}: {e}")
            if errors > 50:
                log.error("Too many consecutive errors, stopping")
                break

        # Progress every 200
        if (i + 1) % 200 == 0:
            elapsed = time.time() - start_time
            rate_hr = (i + 1) / (elapsed / 3600) if elapsed > 0 else 0
            remaining = (len(pending) - i - 1) / rate_hr if rate_hr > 0 else 0
            log.info(
                f"  {i+1:,}/{len(pending):,} processed | "
                f"{fetched:,} fetched, {errors} errors | "
                f"{rate_hr:.0f}/hr | ~{remaining:.1f}hr remaining"
            )
            save_state(state)

    # Final save
    save_state(state)

    elapsed = time.time() - start_time
    status = "Interrupted" if _shutdown else "Complete"
    log.info(f"\nDocket backfill: {status}")
    log.info(f"  Fetched: {fetched:,}")
    log.info(f"  Errors/404s: {errors}")
    log.info(f"  API calls: {rate.count}")
    log.info(f"  Duration: {elapsed/3600:.1f} hours")


if __name__ == "__main__":
    main()
