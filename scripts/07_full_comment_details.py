#!/usr/bin/env python3
"""
Phase 7: Download full comment details from Regulations.gov.

The header-only download (Phase 3/6) gives us: id, title, postedDate, agencyId.
The detail endpoint gives us everything: full comment text, firstName, lastName,
organization, city, state, zip, country, docketId, commentOnDocumentId, category,
duplicateComments, trackingNbr, attachments, and more.

Rate limited to 1,000 requests/hour (3.6s between requests) to stay within
the Regulations.gov API cap. Supports resume via state file.

Usage:
    python3 07_full_comment_details.py                        # all agencies, all types
    python3 07_full_comment_details.py --types organization   # orgs only (~180K)
    python3 07_full_comment_details.py --types organization --types unknown
    python3 07_full_comment_details.py --agency EPA --types organization
    python3 07_full_comment_details.py --workers 2            # more parallelism
    python3 07_full_comment_details.py --limit 1000           # stop after N
    python3 07_full_comment_details.py --skip-anonymous       # skip anonymous
"""

import argparse
import json
import logging
import signal
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
API_BASE = "https://api.regulations.gov/v4"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts/config.json"
DB_PATH = PROJECT_DIR / "openregs.db"
HEADERS_DIR = PROJECT_DIR / "regulations_gov/comments/headers"
DETAILS_DIR = PROJECT_DIR / "regulations_gov/comments/details"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "full_comments_state.json"
PROGRESS_FILE = LOG_DIR / "progress.txt"

MIN_INTERVAL = 3.6  # seconds between ANY two requests (1,000 req/hr)
BATCH_SIZE = 100     # comments per output file
DEFAULT_WORKERS = 1

with open(CONFIG_FILE) as f:
    API_KEY = json.load(f)["regulations_gov_api_key"]

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("full_comments")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "full_comment_details.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# === Graceful shutdown ===
_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received — finishing in-flight requests then saving...")
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# === Thread-safe rate limiter ===
class ThreadSafeRateLimiter:
    """Ensures at least `interval` seconds between any two API calls across all threads."""

    def __init__(self, interval):
        self.interval = interval
        self.next_allowed = 0.0
        self.count = 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            if now < self.next_allowed:
                wait_time = self.next_allowed - now
            else:
                wait_time = 0
            self.next_allowed = max(now, self.next_allowed) + self.interval
            self.count += 1
        if wait_time > 0:
            time.sleep(wait_time)


rate = ThreadSafeRateLimiter(MIN_INTERVAL)


# === Thread-local HTTP sessions ===
_thread_local = threading.local()

def get_session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers["X-Api-Key"] = API_KEY
        retry = Retry(
            total=3,
            backoff_factor=10,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _thread_local.session = s
    return _thread_local.session


def fetch_comment_detail(comment_id):
    """Fetch full details for a single comment. Called from worker threads."""
    if _shutdown:
        return comment_id, None, "shutdown"

    rate.wait()

    if _shutdown:
        return comment_id, None, "shutdown"

    session = get_session()
    url = f"{API_BASE}/comments/{comment_id}"

    try:
        resp = session.get(url, params={
            "include": "attachments",
            "api_key": API_KEY,
        }, timeout=120)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            log.warning(f"Rate limited (429). Sleeping {retry_after}s...")
            time.sleep(retry_after)
            rate.wait()
            resp = session.get(url, params={
                "include": "attachments",
                "api_key": API_KEY,
            }, timeout=120)

        if resp.status_code == 404:
            return comment_id, None, "not_found"

        if resp.status_code == 403:
            log.error(f"403 Forbidden on {comment_id}")
            return comment_id, None, "forbidden"

        resp.raise_for_status()
        return comment_id, resp.json(), "ok"

    except Exception as e:
        log.error(f"Error fetching {comment_id}: {e}")
        return comment_id, None, "error"


# === State management ===
def load_state_downloaded():
    """Load the set of already-downloaded comment IDs."""
    if STATE_FILE.exists():
        data = json.loads(STATE_FILE.read_text())
        return set(data.get("downloaded", []))
    return set()


def save_state(downloaded, failed_count):
    state = {
        "downloaded": list(downloaded),
        "total_fetched": len(downloaded),
        "failed_count": failed_count,
        "api_calls": rate.count,
        "saved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.rename(STATE_FILE)


# === Collect comment IDs ===
def collect_comment_ids_from_db(agencies=None, types=None, skip_anonymous=False):
    """Query the SQLite database for comment IDs, filtered by agency and/or submitter_type."""
    if not DB_PATH.exists():
        log.error(f"Database not found: {DB_PATH}. Run 05_build_database.py first.")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))

    where = []
    params = []

    if agencies:
        placeholders = ",".join("?" for _ in agencies)
        where.append(f"agency_id IN ({placeholders})")
        params.extend(agencies)

    if types:
        placeholders = ",".join("?" for _ in types)
        where.append(f"submitter_type IN ({placeholders})")
        params.extend(types)

    if skip_anonymous:
        where.append("submitter_type != 'anonymous'")
        where.append("title NOT LIKE '%anonymous%'")

    where_clause = " AND ".join(where) if where else "1=1"

    query = f"""
        SELECT id, COALESCE(title, ''), agency_id, submitter_type
        FROM comments
        WHERE {where_clause}
        ORDER BY
            CASE submitter_type
                WHEN 'organization' THEN 0
                WHEN 'unknown' THEN 1
                WHEN 'individual' THEN 2
                WHEN 'anonymous' THEN 3
                ELSE 4
            END,
            agency_id, posted_date DESC
    """

    rows = conn.execute(query, params).fetchall()
    conn.close()

    log.info(f"Found {len(rows):,} comments in database matching filters")

    type_counts = {}
    for _, _, _, stype in rows:
        type_counts[stype] = type_counts.get(stype, 0) + 1
    for stype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        log.info(f"  {stype}: {count:,}")

    return [(cid, title, agency) for cid, title, agency, _ in rows]


def collect_comment_ids_from_files(agencies=None, skip_anonymous=False):
    """Scan header JSON files and collect comment IDs. Fallback when --types is not used."""
    comments = []

    for json_file in sorted(HEADERS_DIR.rglob("page_*.json")):
        if agencies:
            parts = json_file.relative_to(HEADERS_DIR).parts
            if parts and parts[0] not in agencies:
                continue

        try:
            with open(json_file) as f:
                data = json.load(f)
            for rec in data.get("data", []):
                cid = rec.get("id")
                attrs = rec.get("attributes", {})
                title = attrs.get("title", "")
                agency = attrs.get("agencyId", "")
                if cid:
                    comments.append((cid, title, agency))
        except (json.JSONDecodeError, OSError):
            continue

    seen = set()
    unique = []
    for cid, title, agency in comments:
        if cid not in seen:
            seen.add(cid)
            unique.append((cid, title, agency))

    if skip_anonymous:
        unique = [(c, t, a) for c, t, a in unique if "anonymous" not in t.lower()]

    def sort_key(item):
        if "anonymous" in item[1].lower():
            return 2
        return 0

    unique.sort(key=sort_key)
    return unique


def progress(msg):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download full comment details (parallel)")
    parser.add_argument("--agency", action="append", help="Agency to target (can repeat)")
    parser.add_argument("--types", action="append",
                        help="Submitter types: organization, individual, anonymous, unknown (can repeat)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--limit", type=int, default=0, help="Max comments to fetch")
    parser.add_argument("--skip-anonymous", action="store_true", help="Skip anonymous comments")
    args = parser.parse_args()

    agencies = [a.upper() for a in args.agency] if args.agency else None
    types = [t.lower() for t in args.types] if args.types else None
    agency_label = ", ".join(agencies) if agencies else "ALL"
    types_label = ", ".join(types) if types else "ALL"
    workers = args.workers

    log.info("=" * 60)
    log.info("FULL COMMENT DETAILS — Starting (parallel)")
    log.info(f"  Agencies: {agency_label}")
    log.info(f"  Submitter types: {types_label}")
    log.info(f"  Workers: {workers}")
    log.info(f"  Rate limit: {MIN_INTERVAL}s between requests ({int(3600/MIN_INTERVAL)}/hr max)")
    if args.limit:
        log.info(f"  Limit: {args.limit} comments")
    if args.skip_anonymous:
        log.info("  Skipping anonymous comments")
    log.info("=" * 60)

    # Load already-downloaded IDs
    downloaded = load_state_downloaded()
    log.info(f"Already downloaded: {len(downloaded):,} comments")

    # Collect comment IDs
    if types:
        log.info("Querying database for comment IDs...")
        all_comments = collect_comment_ids_from_db(agencies, types, args.skip_anonymous)
    else:
        log.info("Scanning header files for comment IDs...")
        all_comments = collect_comment_ids_from_files(agencies, args.skip_anonymous)
    log.info(f"Total comment IDs to consider: {len(all_comments):,}")

    # Filter out already downloaded
    to_fetch = [cid for cid, _, _ in all_comments if cid not in downloaded]
    log.info(f"Remaining to fetch: {len(to_fetch):,}")

    if not to_fetch:
        log.info("Nothing to fetch — all done!")
        return

    if args.limit:
        to_fetch = to_fetch[:args.limit]
        log.info(f"Will fetch {len(to_fetch):,} (limited)")

    est_hours = len(to_fetch) / (min(workers * 300, 1000))
    log.info(f"Estimated time: ~{est_hours:.0f} hours at ~{min(workers * 300, 1000)}/hr")
    progress(f"Full details: starting {len(to_fetch):,} comments for {agency_label} ({workers} workers)")

    # Create output directory
    DETAILS_DIR.mkdir(parents=True, exist_ok=True)

    # Tracking
    batch = []
    batch_num = len(list(DETAILS_DIR.glob("batch_*.json")))
    fetched = 0
    failed = 0
    start_time = time.time()
    batch_lock = threading.Lock()

    def save_batch():
        """Save current batch to disk. Must be called with batch_lock held."""
        nonlocal batch, batch_num
        if not batch:
            return
        batch_num += 1
        outfile = DETAILS_DIR / f"batch_{batch_num:06d}.json"
        outfile.write_text(json.dumps(batch, indent=None))
        batch = []

    # Run parallel fetchers
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit work in chunks to allow early shutdown
        CHUNK = 500
        idx = 0

        while idx < len(to_fetch) and not _shutdown:
            chunk = to_fetch[idx : idx + CHUNK]
            futures = {executor.submit(fetch_comment_detail, cid): cid for cid in chunk}

            for future in as_completed(futures):
                if _shutdown:
                    # Cancel pending futures
                    for f in futures:
                        f.cancel()
                    break

                comment_id, result, status = future.result()

                with batch_lock:
                    if status == "ok" and result:
                        batch.append(result)
                        downloaded.add(comment_id)
                        fetched += 1

                        if len(batch) >= BATCH_SIZE:
                            save_batch()

                            elapsed = time.time() - start_time
                            rate_actual = fetched / (elapsed / 3600) if elapsed > 0 else 0
                            remaining = len(to_fetch) - idx - fetched % CHUNK
                            eta_hours = remaining / rate_actual if rate_actual > 0 else 0
                            log.info(
                                f"Batch {batch_num}: {fetched:,}/{len(to_fetch):,} "
                                f"({fetched*100/len(to_fetch):.1f}%), "
                                f"{failed} failed, {rate_actual:.0f}/hr, "
                                f"~{eta_hours:.0f}h remaining"
                            )

                            # Save state every 10 batches
                            if batch_num % 10 == 0:
                                save_state(downloaded, failed)

                    elif status == "forbidden":
                        log.error("API key rejected — stopping all workers")
                        _handle_signal(None, None)
                        failed += 1
                    elif status == "shutdown":
                        pass
                    else:
                        failed += 1

            idx += CHUNK

            # Progress log per chunk
            elapsed = time.time() - start_time
            rate_actual = fetched / (elapsed / 3600) if elapsed > 0 else 0
            remaining = len(to_fetch) - idx
            if remaining > 0 and rate_actual > 0:
                eta_hours = remaining / rate_actual
                progress(
                    f"Full details: {fetched:,}/{len(to_fetch):,} "
                    f"({fetched*100/len(to_fetch):.1f}%), "
                    f"{rate_actual:.0f}/hr, ~{eta_hours:.0f}h remaining"
                )

    # Save remaining batch
    with batch_lock:
        save_batch()

    # Final state save
    save_state(downloaded, failed)

    # Summary
    elapsed = time.time() - start_time
    status_label = "Interrupted" if _shutdown else "Complete"
    rate_actual = fetched / (elapsed / 3600) if elapsed > 0 else 0

    log.info("=" * 60)
    log.info(f"FULL COMMENT DETAILS — {status_label}")
    log.info(f"  Fetched: {fetched:,} | Failed: {failed} | Batches: {batch_num}")
    log.info(f"  Total downloaded (all time): {len(downloaded):,}")
    log.info(f"  Elapsed: {elapsed/3600:.1f} hours")
    log.info(f"  Actual rate: {rate_actual:.0f}/hr")
    log.info(f"  API calls: {rate.count}")
    log.info("=" * 60)
    progress(f"Full details: {status_label} — {fetched:,} fetched, {len(downloaded):,} total, {rate_actual:.0f}/hr")


if __name__ == "__main__":
    main()
