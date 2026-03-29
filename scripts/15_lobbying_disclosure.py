#!/usr/bin/env python3
"""
Phase 15: Lobbying Disclosure Data Pull

Downloads ALL lobbying disclosure data from the Senate LDA REST API
(lda.gov/api/v1/) and stores in a separate SQLite database.

Data includes:
- LD-1 registrations and LD-2 activity reports (filings endpoint)
- LD-203 political contributions (contributions endpoint)
- Reference constants (filing types, issue codes, government entities)

The old lda.senate.gov API migrated to lda.gov on June 30, 2026.

Expected volume: ~2-3 million filings (1999-present), 8,000-12,000 API pages.
At 2 requests/second: ~1.5-2 hours for the full pull.

Usage:
    python3 15_lobbying_disclosure.py                    # full pull
    python3 15_lobbying_disclosure.py --contributions    # contributions only
    python3 15_lobbying_disclosure.py --constants        # constants only
"""

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
CONFIG_FILE = Path(__file__).parent / "config.json"
with open(CONFIG_FILE) as f:
    CONFIG = json.load(f)

API_KEY = CONFIG["lda_api_key"]
BASE_URL = "https://lda.gov/api/v1"

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "lobbying.db"
RAW_DIR = PROJECT_DIR / "lobbying_raw"
FILINGS_RAW_DIR = RAW_DIR / "filings"
CONTRIBUTIONS_RAW_DIR = RAW_DIR / "contributions"
LOG_DIR = PROJECT_DIR / "logs"
PROGRESS_FILE = LOG_DIR / "progress.txt"

PAGE_SIZE = 250
MIN_INTERVAL = 0.5  # 120 requests/minute = 2/sec, stay under with 0.5s gap

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("lobbying")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "lobbying_pull.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)

# === Graceful shutdown ===
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received — finishing current request then stopping")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# === HTTP session ===
def make_session():
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Token {API_KEY}",
        "Accept": "application/json",
    })
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


def progress(msg):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


# === Rate-limited request with 429 backoff ===
def api_get(session, url, params=None):
    """Make a GET request with rate limiting and 429 exponential backoff."""
    backoff = 1
    max_backoff = 120

    while True:
        time.sleep(MIN_INTERVAL)
        try:
            resp = session.get(url, params=params, timeout=120)

            if resp.status_code == 429:
                wait = min(backoff * 2, max_backoff)
                log.warning(f"429 rate limited — waiting {wait}s before retry")
                time.sleep(wait)
                backoff = wait
                continue

            if resp.status_code >= 500:
                log.warning(f"Server error {resp.status_code} — waiting 30s before retry")
                time.sleep(30)
                continue

            if resp.status_code == 400:
                log.warning(f"400 Bad Request for {url} params={params} — returning None")
                return None

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.ConnectionError as e:
            log.warning(f"Connection error: {e} — waiting 30s before retry")
            time.sleep(30)
            continue


# === Database setup ===
def create_database():
    """Create the lobbying database and all tables."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        -- Reference tables
        CREATE TABLE IF NOT EXISTS lobbying_issue_codes (
            code TEXT PRIMARY KEY,
            description TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS lobbying_gov_entities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS lobbying_filing_types (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT
        );

        -- Raw filing metadata (both LD-1 and LD-2, one row per filing)
        CREATE TABLE IF NOT EXISTS lobbying_filings_raw (
            filing_uuid TEXT PRIMARY KEY,
            filing_type TEXT NOT NULL,
            registrant_id INTEGER,
            registrant_name TEXT,
            client_id INTEGER,
            client_name TEXT,
            filing_year INTEGER,
            filing_period TEXT,
            received_date TEXT,
            amount_reported REAL,
            is_amendment INTEGER DEFAULT 0,
            is_no_activity INTEGER DEFAULT 0,
            is_termination INTEGER DEFAULT 0,
            raw_json TEXT
        );

        -- LD-1 registrations
        CREATE TABLE IF NOT EXISTS lobbying_registrations (
            filing_uuid TEXT PRIMARY KEY,
            filing_type TEXT NOT NULL,
            registrant_id INTEGER,
            registrant_name TEXT NOT NULL,
            registrant_description TEXT,
            registrant_address TEXT,
            registrant_country TEXT,
            registrant_ppb_country TEXT,
            client_id INTEGER,
            client_name TEXT NOT NULL,
            client_description TEXT,
            client_country TEXT,
            client_ppb_country TEXT,
            client_state TEXT,
            effective_date TEXT,
            received_date TEXT,
            general_issue_codes TEXT,
            is_amendment INTEGER DEFAULT 0,
            termination_date TEXT
        );

        -- LD-2 activity reports
        CREATE TABLE IF NOT EXISTS lobbying_activities (
            id INTEGER PRIMARY KEY,
            filing_uuid TEXT NOT NULL,
            filing_type TEXT NOT NULL,
            registrant_name TEXT NOT NULL,
            registrant_id INTEGER,
            client_name TEXT NOT NULL,
            filing_year INTEGER NOT NULL,
            filing_period TEXT NOT NULL,
            issue_code TEXT,
            specific_issues TEXT,
            government_entities TEXT,
            income_amount INTEGER,
            expense_amount INTEGER,
            is_no_activity INTEGER DEFAULT 0,
            is_termination INTEGER DEFAULT 0,
            received_date TEXT,
            CONSTRAINT fk_activity_filing FOREIGN KEY (filing_uuid)
                REFERENCES lobbying_filings_raw(filing_uuid)
        );

        -- Lobbyists listed on filings
        CREATE TABLE IF NOT EXISTS lobbying_lobbyists (
            id INTEGER PRIMARY KEY,
            filing_uuid TEXT NOT NULL,
            lobbyist_name TEXT NOT NULL,
            covered_position TEXT,
            is_new INTEGER DEFAULT 0,
            CONSTRAINT fk_lobbyist_filing FOREIGN KEY (filing_uuid)
                REFERENCES lobbying_filings_raw(filing_uuid)
        );

        -- LD-203 contributions
        CREATE TABLE IF NOT EXISTS lobbying_contributions (
            id INTEGER PRIMARY KEY,
            filing_uuid TEXT NOT NULL,
            lobbyist_name TEXT,
            contributor_name TEXT,
            payee_name TEXT,
            recipient_name TEXT,
            contribution_type TEXT,
            amount INTEGER,
            contribution_date TEXT,
            filing_year INTEGER,
            filing_period TEXT,
            registrant_name TEXT,
            received_date TEXT
        );
    """)

    # Create indexes (IF NOT EXISTS is implicit for CREATE INDEX IF NOT EXISTS)
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_reg_client_name ON lobbying_registrations(client_name COLLATE NOCASE)",
        "CREATE INDEX IF NOT EXISTS idx_reg_registrant_name ON lobbying_registrations(registrant_name COLLATE NOCASE)",
        "CREATE INDEX IF NOT EXISTS idx_act_client_name ON lobbying_activities(client_name COLLATE NOCASE)",
        "CREATE INDEX IF NOT EXISTS idx_act_issue_code ON lobbying_activities(issue_code)",
        "CREATE INDEX IF NOT EXISTS idx_act_filing_year ON lobbying_activities(filing_year)",
        "CREATE INDEX IF NOT EXISTS idx_act_filing_uuid ON lobbying_activities(filing_uuid)",
        "CREATE INDEX IF NOT EXISTS idx_lob_filing_uuid ON lobbying_lobbyists(filing_uuid)",
        "CREATE INDEX IF NOT EXISTS idx_lob_name ON lobbying_lobbyists(lobbyist_name COLLATE NOCASE)",
        "CREATE INDEX IF NOT EXISTS idx_raw_filing_type ON lobbying_filings_raw(filing_type)",
        "CREATE INDEX IF NOT EXISTS idx_raw_client ON lobbying_filings_raw(client_name COLLATE NOCASE)",
        "CREATE INDEX IF NOT EXISTS idx_raw_year ON lobbying_filings_raw(filing_year)",
        "CREATE INDEX IF NOT EXISTS idx_contrib_lobbyist ON lobbying_contributions(lobbyist_name COLLATE NOCASE)",
        "CREATE INDEX IF NOT EXISTS idx_contrib_recipient ON lobbying_contributions(recipient_name COLLATE NOCASE)",
        "CREATE INDEX IF NOT EXISTS idx_contrib_year ON lobbying_contributions(filing_year)",
    ]
    for idx_sql in indexes:
        conn.execute(idx_sql)

    conn.commit()
    conn.close()
    log.info(f"Database ready: {DB_PATH}")


# === Constants pull ===
def pull_constants(session):
    """Pull reference/lookup tables: filing types, issue codes, government entities."""
    conn = sqlite3.connect(str(DB_PATH))

    # Filing types
    log.info("Pulling filing types...")
    data = api_get(session, f"{BASE_URL}/constants/filing/filingtypes/")
    if isinstance(data, list):
        items = data
    else:
        items = data.get("results", data) if isinstance(data, dict) else []
    count = 0
    for item in items:
        if isinstance(item, dict):
            conn.execute(
                "INSERT OR REPLACE INTO lobbying_filing_types (id, name, description) VALUES (?, ?, ?)",
                (item.get("id"), item.get("name", ""), item.get("description")),
            )
            count += 1
    conn.commit()
    log.info(f"  Filing types: {count} records")

    # Issue codes
    log.info("Pulling lobbying activity issue codes...")
    data = api_get(session, f"{BASE_URL}/constants/filing/lobbyingactivityissues/")
    if isinstance(data, list):
        items = data
    else:
        items = data.get("results", data) if isinstance(data, dict) else []
    count = 0
    for item in items:
        if isinstance(item, dict):
            code = item.get("code") or item.get("id") or item.get("value")
            desc = item.get("description") or item.get("name") or ""
            if code:
                conn.execute(
                    "INSERT OR REPLACE INTO lobbying_issue_codes (code, description) VALUES (?, ?)",
                    (str(code), desc),
                )
                count += 1
    conn.commit()
    log.info(f"  Issue codes: {count} records")

    # Government entities
    log.info("Pulling government entities...")
    data = api_get(session, f"{BASE_URL}/constants/filing/governmententities/")
    if isinstance(data, list):
        items = data
    else:
        items = data.get("results", data) if isinstance(data, dict) else []
    count = 0
    for item in items:
        if isinstance(item, dict):
            conn.execute(
                "INSERT OR REPLACE INTO lobbying_gov_entities (id, name) VALUES (?, ?)",
                (item.get("id"), item.get("name", "")),
            )
            count += 1
    conn.commit()
    log.info(f"  Government entities: {count} records")

    conn.close()
    log.info("Constants pull complete")


# === Filing parsing ===
def safe_str(val):
    """Safely convert a value to trimmed string, or None."""
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def safe_int(val):
    """Safely convert to integer, or None."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def safe_float(val):
    """Safely convert to float, or None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def extract_nested_id(obj):
    """Extract ID from a nested object like {'id': 123, 'name': 'foo'}."""
    if isinstance(obj, dict):
        return obj.get("id")
    return obj


def extract_nested_name(obj):
    """Extract name from a nested object."""
    if isinstance(obj, dict):
        return safe_str(obj.get("name"))
    return safe_str(obj)


def get_filing_type_name(filing):
    """Get the filing type string (e.g., 'LD-1', 'LD-2') from a filing dict."""
    ft = filing.get("filing_type")
    if isinstance(ft, dict):
        return safe_str(ft.get("name")) or safe_str(ft.get("id"))
    return safe_str(ft)


def parse_filing(filing, conn):
    """Parse a single filing JSON object and insert into appropriate tables."""
    filing_uuid = safe_str(filing.get("filing_uuid"))
    if not filing_uuid:
        return

    filing_type = get_filing_type_name(filing)
    if not filing_type:
        filing_type = "UNKNOWN"

    # Registrant info
    registrant = filing.get("registrant", {}) or {}
    registrant_id = extract_nested_id(registrant) or safe_int(registrant.get("id"))
    registrant_name = safe_str(registrant.get("name")) or safe_str(registrant.get("registrant_name")) or ""
    registrant_desc = safe_str(registrant.get("description"))
    registrant_address = safe_str(registrant.get("address"))
    registrant_country = safe_str(registrant.get("country"))
    registrant_ppb_country = safe_str(registrant.get("ppb_country"))

    # Client info
    client = filing.get("client", {}) or {}
    client_id = extract_nested_id(client) or safe_int(client.get("id"))
    client_name = safe_str(client.get("name")) or safe_str(client.get("client_name")) or ""
    client_desc = safe_str(client.get("general_description")) or safe_str(client.get("description"))
    client_country = safe_str(client.get("country"))
    client_ppb_country = safe_str(client.get("ppb_country"))
    client_state = safe_str(client.get("state"))

    # Filing metadata
    filing_year = safe_int(filing.get("filing_year"))
    filing_period = safe_str(filing.get("filing_period"))
    received_date = safe_str(filing.get("dt_posted")) or safe_str(filing.get("received_date"))
    effective_date = safe_str(filing.get("effective_date"))
    termination_date = safe_str(filing.get("termination_date"))

    # Amount
    amount_reported = safe_float(filing.get("income")) or safe_float(filing.get("expenses"))

    # Flags
    is_amendment = 1 if filing_type and "A" in filing_type.upper() and filing_type.upper() != "LD-2" else 0
    # More precise: LD-1A, LD-2A are amendments
    if filing_type and filing_type.upper().endswith("A"):
        is_amendment = 1
    is_no_activity = 1 if filing.get("is_no_activity") else 0
    is_termination = 1 if filing.get("is_termination") or termination_date else 0

    # Insert into lobbying_filings_raw
    raw_json = json.dumps(filing)
    conn.execute("""
        INSERT OR REPLACE INTO lobbying_filings_raw
        (filing_uuid, filing_type, registrant_id, registrant_name, client_id,
         client_name, filing_year, filing_period, received_date, amount_reported,
         is_amendment, is_no_activity, is_termination, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        filing_uuid, filing_type, registrant_id, registrant_name, client_id,
        client_name, filing_year, filing_period, received_date, amount_reported,
        is_amendment, is_no_activity, is_termination, raw_json,
    ))

    # Determine if this is an LD-1 (registration) or LD-2 (activity report)
    ft_upper = filing_type.upper() if filing_type else ""

    if ft_upper.startswith("LD-1") or ft_upper in ("RR", "RA"):
        # Registration — insert into lobbying_registrations
        # Collect issue area codes
        lobbying_activities = filing.get("lobbying_activities", []) or []
        issue_codes = set()
        for act in lobbying_activities:
            if isinstance(act, dict):
                issue = act.get("general_issue_code")
                if isinstance(issue, dict):
                    code = issue.get("code") or issue.get("value")
                elif issue:
                    code = issue
                else:
                    code = None
                if code:
                    issue_codes.add(str(code))

        general_issue_codes = ",".join(sorted(issue_codes)) if issue_codes else None

        conn.execute("""
            INSERT OR REPLACE INTO lobbying_registrations
            (filing_uuid, filing_type, registrant_id, registrant_name,
             registrant_description, registrant_address, registrant_country,
             registrant_ppb_country, client_id, client_name, client_description,
             client_country, client_ppb_country, client_state, effective_date,
             received_date, general_issue_codes, is_amendment, termination_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            filing_uuid, filing_type, registrant_id, registrant_name,
            registrant_desc, registrant_address, registrant_country,
            registrant_ppb_country, client_id, client_name, client_desc,
            client_country, client_ppb_country, client_state, effective_date,
            received_date, general_issue_codes, is_amendment, termination_date,
        ))

    if not (ft_upper.startswith("LD-1") or ft_upper in ("RR", "RA")):
        # Activity report — one row per lobbying activity within the filing
        lobbying_activities = filing.get("lobbying_activities", []) or []

        if not lobbying_activities:
            # Filing with no specific activities (e.g., no-activity report)
            conn.execute("""
                INSERT INTO lobbying_activities
                (filing_uuid, filing_type, registrant_name, registrant_id,
                 client_name, filing_year, filing_period, issue_code,
                 specific_issues, government_entities, income_amount,
                 expense_amount, is_no_activity, is_termination, received_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                filing_uuid, filing_type, registrant_name, registrant_id,
                client_name, filing_year, filing_period or "", None,
                None, None, safe_int(filing.get("income")),
                safe_int(filing.get("expenses")), is_no_activity,
                is_termination, received_date,
            ))
        else:
            for act in lobbying_activities:
                if not isinstance(act, dict):
                    continue

                # Extract issue code
                issue = act.get("general_issue_code")
                if isinstance(issue, dict):
                    issue_code = safe_str(issue.get("code")) or safe_str(issue.get("value"))
                else:
                    issue_code = safe_str(issue)

                specific_issues = safe_str(act.get("description")) or safe_str(act.get("specific_issues"))

                # Government entities contacted
                gov_entities_list = act.get("government_entities", []) or []
                gov_names = []
                for ge in gov_entities_list:
                    if isinstance(ge, dict):
                        name = ge.get("name")
                    else:
                        name = ge
                    if name:
                        gov_names.append(str(name).strip())
                government_entities = ",".join(gov_names) if gov_names else None

                conn.execute("""
                    INSERT INTO lobbying_activities
                    (filing_uuid, filing_type, registrant_name, registrant_id,
                     client_name, filing_year, filing_period, issue_code,
                     specific_issues, government_entities, income_amount,
                     expense_amount, is_no_activity, is_termination, received_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    filing_uuid, filing_type, registrant_name, registrant_id,
                    client_name, filing_year, filing_period or "", issue_code,
                    specific_issues, government_entities,
                    safe_int(filing.get("income")),
                    safe_int(filing.get("expenses")),
                    is_no_activity, is_termination, received_date,
                ))

    # Lobbyists — nested inside lobbying_activities[].lobbyists[]
    seen_lobbyists = set()
    for act in (filing.get("lobbying_activities", []) or []):
        if not isinstance(act, dict):
            continue
        for lob in (act.get("lobbyists", []) or []):
            if not isinstance(lob, dict):
                continue
            lobbyist = lob.get("lobbyist", lob)
            if isinstance(lobbyist, dict):
                first = safe_str(lobbyist.get("first_name")) or ""
                last = safe_str(lobbyist.get("last_name")) or ""
                name = f"{first} {last}".strip() if (first or last) else safe_str(lobbyist.get("name")) or ""
            else:
                name = safe_str(lobbyist) or ""

            if not name:
                continue

            # Deduplicate within a filing (same lobbyist may appear in multiple activities)
            lob_key = (filing_uuid, name.lower())
            if lob_key in seen_lobbyists:
                continue
            seen_lobbyists.add(lob_key)

            covered = safe_str(lob.get("covered_position")) or safe_str(lob.get("covered_official_position"))
            is_new = 1 if lob.get("is_new") or lob.get("new") else 0

            conn.execute("""
                INSERT INTO lobbying_lobbyists
                (filing_uuid, lobbyist_name, covered_position, is_new)
                VALUES (?, ?, ?, ?)
            """, (filing_uuid, name, covered, is_new))


# === Filings pull ===
def get_resume_date(table, date_column="received_date"):
    """Get the most recent received_date from a table for resume support."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        row = conn.execute(
            f"SELECT MAX({date_column}) FROM {table}"
        ).fetchone()
        return row[0] if row and row[0] else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def pull_filings(session):
    """Pull all filings with pagination by year (API requires a filter param)."""
    FILINGS_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Check which years are already complete via state file
    state_file = LOG_DIR / "lobbying_filings_state.json"
    if state_file.exists():
        state = json.loads(state_file.read_text())
    else:
        state = {"completed_years": []}
    completed = set(state.get("completed_years", []))

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    total_fetched = 0
    total_parsed = 0
    parse_errors = 0
    filing_types_seen = {}
    global_page = 0

    # The API requires at least one filter; iterate by filing_year (1999-present)
    import datetime
    current_year = datetime.date.today().year
    years = list(range(1999, current_year + 1))

    for year in years:
        if _shutdown:
            break
        if str(year) in completed:
            log.info(f"  Year {year}: already complete, skipping")
            continue

        log.info(f"  Pulling filings for year {year}...")
        page = 1
        year_fetched = 0
        year_dir = FILINGS_RAW_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        while not _shutdown:
            params = {
                "page_size": PAGE_SIZE,
                "page": page,
                "filing_year": year,
                "ordering": "dt_posted",
            }

            data = api_get(session, f"{BASE_URL}/filings/", params=params)
            if data is None:
                log.error(f"Failed to fetch filings year {year} page {page}, stopping")
                break

            results = data.get("results", [])
            total_count = data.get("count", 0)
            next_url = data.get("next")

            # Save raw JSON to disk
            raw_file = year_dir / f"page_{page:04d}.json"
            raw_file.write_text(json.dumps(data))
            global_page += 1

            # Parse each filing
            for filing in results:
                try:
                    parse_filing(filing, conn)
                    total_parsed += 1

                    ft = get_filing_type_name(filing) or "UNKNOWN"
                    filing_types_seen[ft] = filing_types_seen.get(ft, 0) + 1

                except Exception as e:
                    parse_errors += 1
                    filing_uuid = filing.get("filing_uuid", "???")
                    log.error(f"Error parsing filing {filing_uuid}: {e}")

            year_fetched += len(results)
            total_fetched += len(results)

            conn.commit()

            if page % 10 == 0 or page == 1 or not next_url:
                pct = (year_fetched / total_count * 100) if total_count else 0
                log.info(
                    f"    Year {year} page {page}: {year_fetched:,}/{total_count:,} "
                    f"({pct:.1f}%)"
                )

            if not next_url or not results:
                break

            page += 1

        # Mark year as complete
        if not _shutdown:
            completed.add(str(year))
            state["completed_years"] = sorted(completed)
            tmp = state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, indent=2))
            tmp.rename(state_file)

        log.info(f"  Year {year}: {year_fetched:,} filings in {page} pages")
        progress(f"Lobbying filings: year {year} done — {year_fetched:,} filings, "
                 f"running total {total_fetched:,}")

    conn.close()

    types_str = ", ".join(f"{k}:{v}" for k, v in sorted(filing_types_seen.items()))
    log.info(f"Filings pull complete: {total_fetched:,} fetched, "
             f"{total_parsed:,} parsed, {parse_errors} errors across {global_page} pages")
    log.info(f"  Filing types: {types_str}")
    progress(f"Lobbying filings: {'INTERRUPTED' if _shutdown else 'COMPLETE'} — "
             f"{total_fetched:,} fetched, {total_parsed:,} parsed")

    return total_fetched, total_parsed, parse_errors


# === Contributions pull ===
def parse_contribution(filing, conn):
    """Parse a single LD-203 contribution filing and insert rows."""
    filing_uuid = safe_str(filing.get("filing_uuid"))
    if not filing_uuid:
        return

    registrant = filing.get("registrant", {}) or {}
    registrant_name = safe_str(registrant.get("name")) or ""

    filing_year = safe_int(filing.get("filing_year"))
    filing_period = safe_str(filing.get("filing_period"))
    received_date = safe_str(filing.get("dt_posted")) or safe_str(filing.get("received_date"))

    # Each filing may contain multiple contribution items
    contribution_items = filing.get("contribution_items", []) or []
    contributions = filing.get("contributions", contribution_items) or []

    if not contributions:
        # Some filings have no contribution items (no-activity LD-203)
        # Still record a row with NULLs for tracking
        conn.execute("""
            INSERT INTO lobbying_contributions
            (filing_uuid, lobbyist_name, contributor_name, payee_name,
             recipient_name, contribution_type, amount, contribution_date,
             filing_year, filing_period, registrant_name, received_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            filing_uuid, None, None, None, None, None, None, None,
            filing_year, filing_period, registrant_name, received_date,
        ))
        return

    # Lobbyist info lives at the filing level, not per-item
    filing_lobbyist = filing.get("lobbyist", {}) or {}
    if isinstance(filing_lobbyist, dict):
        first = safe_str(filing_lobbyist.get("first_name")) or ""
        last = safe_str(filing_lobbyist.get("last_name")) or ""
        filing_lobbyist_name = f"{first} {last}".strip() if (first or last) else safe_str(filing_lobbyist.get("name"))
    else:
        filing_lobbyist_name = safe_str(filing_lobbyist)

    for item in contributions:
        if not isinstance(item, dict):
            continue

        # Per-item lobbyist (rare) falls back to filing-level lobbyist
        lobbyist = item.get("lobbyist", {})
        if isinstance(lobbyist, dict) and lobbyist:
            first = safe_str(lobbyist.get("first_name")) or ""
            last = safe_str(lobbyist.get("last_name")) or ""
            lobbyist_name = f"{first} {last}".strip() if (first or last) else safe_str(lobbyist.get("name"))
        else:
            item_name = safe_str(lobbyist) if lobbyist else None
            lobbyist_name = item_name or filing_lobbyist_name

        contributor_name = safe_str(item.get("contributor_name"))
        payee_name = safe_str(item.get("payee_name"))
        recipient_name = safe_str(item.get("honoree_name")) or safe_str(item.get("recipient_name")) or safe_str(item.get("destination_name"))

        contrib_type = item.get("contribution_type")
        if isinstance(contrib_type, dict):
            contribution_type = safe_str(contrib_type.get("name")) or safe_str(contrib_type.get("code"))
        else:
            # Use display name if available, fall back to raw code
            contribution_type = safe_str(item.get("contribution_type_display")) or safe_str(contrib_type)

        amount = safe_int(item.get("amount"))
        contribution_date = safe_str(item.get("date")) or safe_str(item.get("contribution_date"))

        conn.execute("""
            INSERT INTO lobbying_contributions
            (filing_uuid, lobbyist_name, contributor_name, payee_name,
             recipient_name, contribution_type, amount, contribution_date,
             filing_year, filing_period, registrant_name, received_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            filing_uuid, lobbyist_name, contributor_name, payee_name,
            recipient_name, contribution_type, amount, contribution_date,
            filing_year, filing_period, registrant_name, received_date,
        ))


def pull_contributions(session):
    """Pull all LD-203 contribution filings with pagination by year."""
    CONTRIBUTIONS_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Check which years are already complete via state file
    state_file = LOG_DIR / "lobbying_contributions_state.json"
    if state_file.exists():
        state = json.loads(state_file.read_text())
    else:
        state = {"completed_years": []}
    completed = set(state.get("completed_years", []))

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    total_fetched = 0
    total_parsed = 0
    parse_errors = 0

    import datetime
    current_year = datetime.date.today().year
    # LD-203 contribution filings started in 2008
    years = list(range(2008, current_year + 1))

    for year in years:
        if _shutdown:
            break
        if str(year) in completed:
            log.info(f"  Contributions year {year}: already complete, skipping")
            continue

        log.info(f"  Pulling contributions for year {year}...")
        page = 1
        year_fetched = 0
        year_dir = CONTRIBUTIONS_RAW_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        while not _shutdown:
            params = {
                "page_size": PAGE_SIZE,
                "page": page,
                "filing_year": year,
                "ordering": "dt_posted",
            }

            data = api_get(session, f"{BASE_URL}/contributions/", params=params)
            if data is None:
                log.error(f"Failed to fetch contributions year {year} page {page}, stopping")
                break

            results = data.get("results", [])
            total_count = data.get("count", 0)
            next_url = data.get("next")

            raw_file = year_dir / f"page_{page:04d}.json"
            raw_file.write_text(json.dumps(data))

            for filing in results:
                try:
                    parse_contribution(filing, conn)
                    total_parsed += 1
                except Exception as e:
                    parse_errors += 1
                    filing_uuid = filing.get("filing_uuid", "???")
                    log.error(f"Error parsing contribution {filing_uuid}: {e}")

            year_fetched += len(results)
            total_fetched += len(results)
            conn.commit()

            if page % 10 == 0 or page == 1 or not next_url:
                pct = (year_fetched / total_count * 100) if total_count else 0
                log.info(
                    f"    Contributions year {year} page {page}: "
                    f"{year_fetched:,}/{total_count:,} ({pct:.1f}%)"
                )

            if not next_url or not results:
                break

            page += 1

        if not _shutdown:
            completed.add(str(year))
            state["completed_years"] = sorted(completed)
            tmp = state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, indent=2))
            tmp.rename(state_file)

        log.info(f"  Contributions year {year}: {year_fetched:,} in {page} pages")
        progress(f"Lobbying contributions: year {year} done — {year_fetched:,}, "
                 f"running total {total_fetched:,}")

    conn.close()

    log.info(f"Contributions pull complete: {total_fetched:,} fetched, "
             f"{total_parsed:,} parsed, {parse_errors} errors")
    progress(f"Lobbying contributions: {'INTERRUPTED' if _shutdown else 'COMPLETE'} — "
             f"{total_fetched:,} fetched, {total_parsed:,} parsed")

    return total_fetched, total_parsed, parse_errors


# === Summary ===
def print_summary():
    """Print database summary counts."""
    conn = sqlite3.connect(str(DB_PATH))

    tables = [
        "lobbying_filings_raw",
        "lobbying_registrations",
        "lobbying_activities",
        "lobbying_lobbyists",
        "lobbying_contributions",
        "lobbying_issue_codes",
        "lobbying_gov_entities",
        "lobbying_filing_types",
    ]

    log.info("=" * 60)
    log.info("DATABASE SUMMARY")
    log.info("-" * 40)
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(f"  {table}: {count:,} rows")
        except sqlite3.OperationalError:
            log.info(f"  {table}: (table not found)")
    log.info("=" * 60)

    conn.close()


# === Main ===
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download lobbying disclosure data from Senate LDA API")
    parser.add_argument("--constants", action="store_true", help="Pull constants only")
    parser.add_argument("--contributions", action="store_true", help="Pull contributions only")
    parser.add_argument("--filings", action="store_true", help="Pull filings only")
    parser.add_argument("--summary", action="store_true", help="Print summary only")
    args = parser.parse_args()

    # If no specific flag, run everything
    run_all = not (args.constants or args.contributions or args.filings or args.summary)

    log.info("=" * 60)
    log.info("LOBBYING DISCLOSURE DATA PULL — Starting")
    log.info(f"  API base: {BASE_URL}")
    log.info(f"  Database: {DB_PATH}")
    log.info(f"  Raw JSON: {RAW_DIR}")
    log.info("=" * 60)
    progress("Lobbying disclosure: STARTING")

    start_time = time.time()

    # Create database and tables
    create_database()

    session = make_session()

    if args.summary:
        print_summary()
        return

    # Step 1: Constants
    if run_all or args.constants:
        log.info("--- Step 1: Pulling constants ---")
        try:
            pull_constants(session)
        except Exception as e:
            log.error(f"Failed to pull constants: {e}")

    # Step 2: Filings
    if (run_all or args.filings) and not _shutdown:
        log.info("--- Step 2: Pulling filings ---")
        filings_fetched, filings_parsed, filings_errors = pull_filings(session)

    # Step 3: Contributions
    if (run_all or args.contributions) and not _shutdown:
        log.info("--- Step 3: Pulling contributions ---")
        contrib_fetched, contrib_parsed, contrib_errors = pull_contributions(session)

    # Summary
    elapsed = time.time() - start_time
    log.info(f"Elapsed: {elapsed / 60:.1f} minutes")
    print_summary()

    status = "INTERRUPTED" if _shutdown else "COMPLETE"
    progress(f"Lobbying disclosure: {status} in {elapsed / 60:.1f} minutes")


if __name__ == "__main__":
    main()
