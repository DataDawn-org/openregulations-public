#!/usr/bin/env python3
"""
Daily updater for open-for-comment documents.

Builds a standalone open_comments.db with all currently open-for-comment
documents from Regulations.gov. This is a SEPARATE database from the main
openregs.db — Datasette serves both, and the explore page queries
open_comments for the "Open for Comment" tab.

This design prevents corruption of the main 26GB openregs.db by never
writing to it. The open_comments.db is rebuilt from scratch each run
(atomic write via temp file + rename).

Usage:
    python3 20_daily_open_comments.py                    # auto-detect path
    python3 20_daily_open_comments.py --db /path/to.db   # specify output path
    python3 20_daily_open_comments.py --dry-run           # preview without writing

VPS cron (6:30 AM UTC daily):
    30 6 * * * cd /opt/openregs/scripts && python3 20_daily_open_comments.py >> /opt/openregs/logs/daily_open_comments.log 2>&1
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.json"

# Output paths (auto-detected)
VPS_DIR = Path("/opt/openregs")
LOCAL_DIR = Path(__file__).resolve().parent.parent
DB_NAME = "open_comments.db"

API_BASE = "https://api.regulations.gov/v4"
PAGE_SIZE = 25  # API max per page
MAX_PAGES = 20  # API hard limit: page[number] max is 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── API helpers ────────────────────────────────────────────────────────────

def load_api_key():
    """Load API key from config.json, or use env var."""
    key = os.environ.get("REGS_API_KEY")
    if key:
        return key
    if CONFIG_PATH.exists():
        cfg = json.loads(CONFIG_PATH.read_text())
        return cfg["regulations_gov_api_key"]
    raise RuntimeError("No API key found. Set REGS_API_KEY or provide config.json")


def api_get(session, endpoint, params, api_key):
    """Make an API request with retry logic."""
    import requests
    url = f"{API_BASE}/{endpoint}"
    params["api_key"] = api_key
    for attempt in range(3):
        try:
            resp = session.get(url, params=params, timeout=60)
            if resp.status_code == 429:
                log.warning("Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            if attempt < 2:
                log.warning(f"Request failed ({e}), retrying in 10s...")
                time.sleep(10)
            else:
                raise
    return None


def fetch_open_documents(api_key):
    """Fetch all documents currently open for comment.

    The API limits pagination to 20 pages (500 results). When there are more,
    we split the date range into smaller windows and paginate each.
    """
    import requests
    session = requests.Session()
    session.headers["User-Agent"] = "DataDawn-OpenRegs/1.0"

    today = datetime.now(tz=None).strftime("%Y-%m-%d")

    def fetch_pages(params_base):
        """Paginate up to MAX_PAGES (20) pages for a given set of filters."""
        docs = []
        page = 1
        while page <= MAX_PAGES:
            params = {**params_base, "page[size]": PAGE_SIZE, "page[number]": page}
            data = api_get(session, "documents", params, api_key)
            if not data or "data" not in data:
                break

            batch = data["data"]
            docs.extend(batch)
            total_pages = data.get("meta", {}).get("totalPages", 1)

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.4)

        return docs

    # Probe: use just [ge] (more reliable than [ge]+[le] combo)
    all_docs = []
    probe_params = {
        "filter[commentEndDate][ge]": today,
        "page[size]": 5,
        "page[number]": 1,
    }
    probe = api_get(session, "documents", probe_params, api_key)
    total = probe.get("meta", {}).get("totalElements", 0) if probe else 0
    log.info(f"Total open documents: {total}")

    if total <= MAX_PAGES * PAGE_SIZE:
        # Can fetch in one pass with just [ge]
        all_docs = fetch_pages({"filter[commentEndDate][ge]": today})
        log.info(f"Fetched {len(all_docs)}/{total} in single pass")
    else:
        # Split into windows, subdividing if any window hits the 500-doc limit
        cap = (datetime.now(tz=None) + timedelta(days=365)).strftime("%Y-%m-%d")
        start = datetime.strptime(today, "%Y-%m-%d")
        end = datetime.strptime(cap, "%Y-%m-%d")

        # Build initial 2-week windows (small enough to stay under 500)
        windows = []
        ws = start
        while ws < end:
            we = min(ws + timedelta(days=14), end)
            windows.append((ws, we))
            ws = we + timedelta(days=1)

        for win_start, win_end in windows:
            ws = win_start.strftime("%Y-%m-%d")
            we = win_end.strftime("%Y-%m-%d")
            batch = fetch_pages({
                "filter[commentEndDate][ge]": ws,
                "filter[commentEndDate][le]": we,
            })
            log.info(f"Window {ws} to {we}: {len(batch)} documents")
            all_docs.extend(batch)

    # Deduplicate by document ID (windows may overlap at boundaries)
    seen = set()
    unique = []
    for doc in all_docs:
        did = doc.get("id")
        if did not in seen:
            seen.add(did)
            unique.append(doc)

    log.info(f"Total unique documents: {len(unique)}")
    return unique


def parse_document(doc):
    """Convert API document to database row dict."""
    attrs = doc.get("attributes", {})
    doc_id = doc.get("id", "")

    # Parse dates — API returns ISO format with timezone
    def parse_date(val):
        if not val:
            return None
        return val[:10]  # Just the date part (YYYY-MM-DD)

    def parse_datetime(val):
        if not val:
            return None
        return val[:19].replace("T", " ")  # YYYY-MM-DD HH:MM:SS

    posted = parse_date(attrs.get("postedDate"))
    posted_year = int(posted[:4]) if posted else None
    posted_month = int(posted[5:7]) if posted else None

    return {
        "id": doc_id,
        "agency_id": attrs.get("agencyId"),
        "docket_id": attrs.get("docketId"),
        "title": attrs.get("title"),
        "document_type": attrs.get("documentType"),
        "subtype": attrs.get("subtype"),
        "posted_date": posted,
        "posted_year": posted_year,
        "posted_month": posted_month,
        "comment_start_date": parse_date(attrs.get("commentStartDate")),
        "comment_end_date": parse_date(attrs.get("commentEndDate")),
        "last_modified": parse_datetime(attrs.get("lastModifiedDate")),
        "fr_doc_num": attrs.get("frDocNum"),
        "open_for_comment": 1 if attrs.get("openForComment") else 0,
        "withdrawn": 1 if attrs.get("withdrawn") else 0,
        "object_id": attrs.get("objectId"),
    }


# ── Database ───────────────────────────────────────────────────────────────

def build_database(db_path, rows):
    """Build a fresh open_comments.db with documents and stub dockets.

    Uses atomic write: builds into a .tmp file, then renames into place.
    This prevents Datasette from reading a partially-written file.
    """
    tmp_path = db_path.parent / (db_path.name + ".tmp")

    # Remove any stale temp file
    if tmp_path.exists():
        tmp_path.unlink()

    conn = sqlite3.connect(str(tmp_path))
    conn.execute("PRAGMA journal_mode=DELETE")  # No WAL — simpler for Datasette
    conn.execute("PRAGMA synchronous=FULL")

    # Create tables with same schema as openregs.db (subset of columns)
    conn.execute("""
        CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            agency_id TEXT,
            docket_id TEXT,
            title TEXT,
            document_type TEXT,
            subtype TEXT,
            posted_date TEXT,
            posted_year INTEGER,
            posted_month INTEGER,
            comment_start_date TEXT,
            comment_end_date TEXT,
            last_modified TEXT,
            fr_doc_num TEXT,
            open_for_comment INTEGER,
            withdrawn INTEGER,
            object_id TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE dockets (
            id TEXT PRIMARY KEY,
            agency_id TEXT
        )
    """)

    # Insert documents
    sql = """
        INSERT INTO documents
        (id, agency_id, docket_id, title, document_type, subtype,
         posted_date, posted_year, posted_month,
         comment_start_date, comment_end_date, last_modified,
         fr_doc_num, open_for_comment, withdrawn, object_id)
        VALUES
        (:id, :agency_id, :docket_id, :title, :document_type, :subtype,
         :posted_date, :posted_year, :posted_month,
         :comment_start_date, :comment_end_date, :last_modified,
         :fr_doc_num, :open_for_comment, :withdrawn, :object_id)
    """
    conn.executemany(sql, rows)

    # Create stub dockets
    docket_ids = set(r["docket_id"] for r in rows if r["docket_id"])
    conn.executemany(
        "INSERT OR IGNORE INTO dockets (id, agency_id) VALUES (?, ?)",
        [(did, did.split("-")[0] if "-" in did else None) for did in docket_ids]
    )

    # Indexes for the explore page queries
    conn.execute("CREATE INDEX idx_docs_agency ON documents(agency_id)")
    conn.execute("CREATE INDEX idx_docs_comment_end ON documents(comment_end_date)")
    conn.execute("CREATE INDEX idx_docs_withdrawn ON documents(withdrawn)")

    # Metadata table for tracking freshness
    conn.execute("CREATE TABLE _metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO _metadata VALUES ('updated_at', ?)",
                 (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
    conn.execute("INSERT INTO _metadata VALUES ('document_count', ?)",
                 (str(len(rows)),))

    conn.commit()
    conn.close()

    # Sanity check: refuse to swap in a dataset that dropped >50% vs live,
    # which almost always means a transient API blip (partial-page returns).
    # A 0-row or near-empty open_comments.db would break the explore page.
    if db_path.exists():
        try:
            old_conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            old_count = old_conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            old_conn.close()
            if old_count > 0 and len(rows) < max(10, old_count * 0.5):
                tmp_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"Refusing DB swap: new count {len(rows)} dropped >50% vs live "
                    f"{old_count}. Likely transient API issue — leaving live DB intact."
                )
        except sqlite3.DatabaseError:
            pass  # live DB unreadable, fall through and swap

    # Atomic rename into place
    if db_path.exists():
        db_path.unlink()
    tmp_path.rename(db_path)

    return len(rows), len(docket_ids)


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Daily open-for-comment updater (builds standalone open_comments.db)")
    parser.add_argument("--db", type=Path, help="Output database path (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    # Auto-detect output path
    if args.db:
        db_path = args.db
    elif VPS_DIR.exists():
        db_path = VPS_DIR / DB_NAME
    elif LOCAL_DIR.exists():
        db_path = LOCAL_DIR / DB_NAME
    else:
        log.error("No suitable output path found. Specify --db path.")
        sys.exit(1)

    log.info(f"Output: {db_path}")
    log.info(f"Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")

    # Load API key
    api_key = load_api_key()
    log.info("API key loaded")

    # Fetch open documents
    log.info("Fetching open-for-comment documents from Regulations.gov...")
    docs = fetch_open_documents(api_key)
    log.info(f"Fetched {len(docs)} documents")

    if not docs:
        log.info("No open documents found. Nothing to update.")
        return

    # Parse
    rows = [parse_document(d) for d in docs]

    # Agency breakdown
    agencies = {}
    for r in rows:
        a = r["agency_id"] or "Unknown"
        agencies[a] = agencies.get(a, 0) + 1
    log.info(f"Agencies: {', '.join(f'{a}={n}' for a, n in sorted(agencies.items(), key=lambda x: -x[1])[:10])}")

    if args.dry_run:
        log.info(f"[DRY-RUN] Would build {db_path} with {len(rows)} documents")
        return

    # Build database
    doc_count, docket_count = build_database(db_path, rows)
    db_size = db_path.stat().st_size
    log.info(f"Done: {doc_count} documents, {docket_count} dockets, {db_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
