#!/usr/bin/env python3
"""
Step 5: Build normalized SQLite database from raw extracted JSON.

Parses raw JSON from all categories and builds:
- facilities table (central entity)
- inspections table
- enforcement_actions table
- teachable_moments table
- annual_reports table
- FTS5 virtual table for full-text search
- All indexes for efficient querying
"""

import hashlib
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.aura_client import BASE_DIR, RAW_DIR, hash_id_from_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DB_DIR = BASE_DIR / "db"
DB_PATH = DB_DIR / "aphis.db"

# Inspection IDs (owner queue 1015; decisions_log §856, the ruling on followup #672: each report is its own row,
# keyed on its USDA document link; relay datadawn green #5 item 1, the drafting chat's option B). The 110,400 IDs
# served before this change stay as they are: inspection_id_pin.tsv maps each served report's link (sha256, first 32
# hex characters) to its served ID, with "nolink" rows for the served reports that have no link. A report whose link
# is not pinned gets an ID from its link (hash_id_from_url). The build refuses, before it removes the old aphis.db,
# when the pin is missing or differs from PIN_SHA256 (selftest_aphis_inspection_ids.py, legs R1 and R2). The pin is
# written once by make_inspection_id_pin.py from the served aphis.db and never regenerated.
PIN_PATH = Path(__file__).resolve().parent / "inspection_id_pin.tsv"
PIN_SHA256 = "da4578394612ac08aab33d672a071ee081b433a7c619c334a30626becbfb5117"
PIN_REFUSED_EXIT = 3
# The previous build (decisions_log §898 item 2, option (a), the drafting chat's call; relay datadawn green #2 item 1).
# A served report the extract no longer carries (USDA took it down, or now lists it under a new link) stays served:
# its row is copied, every column unchanged, from the aphis.db this build replaces, with its facility row when this
# build has none for its certificate. The monthly refresh (owner queue 1016) marks such reports later, under §859. The
# build refuses, before it removes the old aphis.db, when that file is missing, can't be read as a database, lacks a
# table or column the carry reads, or lacks a pinned served ID (selftest_aphis_inspection_ids.py, legs P1-P4):
# building without it would drop served reports.
PREVIOUS_REFUSED_EXIT = 4
INSPECTION_COLUMNS = ("id", "cert_number", "inspection_date", "inspection_type", "critical_count",
                      "noncritical_count", "direct_count", "teachable_moment_count", "pdf_url", "pdf_local_path")
FACILITY_COLUMNS = ("cert_number", "customer_number", "legal_name", "dba_name", "site_name", "site_address",
                    "site_city", "site_state", "site_zip", "license_type", "status", "first_seen_date",
                    "last_seen_date")
LINK_FIELDS = ("reportUrl", "reportLink", "pdfUrl", "documentUrl", "web_reportUrl")
CERT_FIELDS = ("web_certNumber", "certNumber", "cert_number")
DATE_FIELDS = ("web_inspectionDate", "inspectionDate", "inspection_date")
CUSTOMER_FIELDS = ("web_customerNumber", "customerNumber", "customer_number")

SCHEMA = """
-- Central entity: every AWA-regulated facility
CREATE TABLE IF NOT EXISTS facilities (
    cert_number TEXT PRIMARY KEY,
    customer_number TEXT,
    legal_name TEXT,
    dba_name TEXT,
    site_name TEXT,
    site_address TEXT,
    site_city TEXT,
    site_state TEXT,
    site_zip TEXT,
    license_type TEXT,
    status TEXT,
    first_seen_date TEXT,
    last_seen_date TEXT
);

-- Inspection reports
CREATE TABLE IF NOT EXISTS inspections (
    id TEXT PRIMARY KEY,
    cert_number TEXT REFERENCES facilities(cert_number),
    inspection_date TEXT,
    inspection_type TEXT,
    critical_count INTEGER,
    noncritical_count INTEGER,
    direct_count INTEGER,
    teachable_moment_count INTEGER,
    pdf_url TEXT,
    pdf_local_path TEXT
);

-- Enforcement actions
CREATE TABLE IF NOT EXISTS enforcement_actions (
    id TEXT PRIMARY KEY,
    cert_number TEXT REFERENCES facilities(cert_number),
    action_date TEXT,
    action_type TEXT,
    penalty_amount REAL,
    description TEXT,
    outcome TEXT,
    pdf_url TEXT,
    pdf_local_path TEXT
);

-- Teachable moments
CREATE TABLE IF NOT EXISTS teachable_moments (
    id TEXT PRIMARY KEY,
    cert_number TEXT REFERENCES facilities(cert_number),
    inspection_date TEXT,
    category TEXT,
    description TEXT
);

-- Research facility annual reports
CREATE TABLE IF NOT EXISTS annual_reports (
    id TEXT PRIMARY KEY,
    cert_number TEXT REFERENCES facilities(cert_number),
    report_year INTEGER,
    species TEXT,
    total_animals INTEGER,
    animals_pain_cat_c INTEGER,
    animals_pain_cat_d INTEGER,
    animals_pain_cat_e INTEGER
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_facilities_state ON facilities(site_state);
CREATE INDEX IF NOT EXISTS idx_facilities_type ON facilities(license_type);
CREATE INDEX IF NOT EXISTS idx_facilities_name ON facilities(legal_name);
CREATE INDEX IF NOT EXISTS idx_inspections_date ON inspections(inspection_date);
CREATE INDEX IF NOT EXISTS idx_inspections_cert ON inspections(cert_number);
CREATE INDEX IF NOT EXISTS idx_enforcement_cert ON enforcement_actions(cert_number);
CREATE INDEX IF NOT EXISTS idx_enforcement_date ON enforcement_actions(action_date);
CREATE INDEX IF NOT EXISTS idx_annual_cert_year ON annual_reports(cert_number, report_year);
CREATE INDEX IF NOT EXISTS idx_teachable_cert ON teachable_moments(cert_number);
"""

FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS facilities_fts USING fts5(
    legal_name, dba_name, site_name, site_city, site_state,
    content="facilities", content_rowid='rowid'
);
"""


def get_field(record: dict, *candidates: str, default=None):
    """Get first matching field from a record, trying multiple possible names."""
    for name in candidates:
        val = record.get(name)
        if val is not None and str(val).strip():
            return str(val).strip()
    return default


def safe_int(val, default=None) -> int | None:
    """Convert a value to int, returning default on failure."""
    if val is None:
        return default
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return default


def safe_float(val, default=None) -> float | None:
    """Convert a value to float, returning default on failure."""
    if val is None:
        return default
    try:
        # Remove $ and commas
        cleaned = re.sub(r'[$,]', '', str(val).strip())
        return float(cleaned)
    except (ValueError, TypeError):
        return default


def normalize_date(val: str | None) -> str | None:
    """Try to normalize a date string to YYYY-MM-DD format."""
    if not val:
        return None

    val = str(val).strip()

    # Already YYYY-MM-DD
    if re.match(r'^\d{4}-\d{2}-\d{2}$', val):
        return val

    # MM/DD/YYYY
    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', val)
    if match:
        return f"{match.group(3)}-{match.group(1).zfill(2)}-{match.group(2).zfill(2)}"

    # Epoch ms
    if val.isdigit() and len(val) > 10:
        try:
            dt = datetime.fromtimestamp(int(val) / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass

    return val


def generate_id(record: dict, *field_groups) -> str:
    """Generate a stable ID from record fields.

    Each argument can be a single field name or a tuple of candidate names
    (tried in order, like get_field).
    """
    import hashlib
    parts = []
    for fg in field_groups:
        if isinstance(fg, (list, tuple)):
            val = get_field(record, *fg) or ""
        else:
            val = str(record.get(fg, "")).strip()
        parts.append(val)
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:16]


def link_key(url: str) -> str:
    """The pin's key for a report link: its sha256, first 32 hex characters."""
    return hashlib.sha256(url.encode()).hexdigest()[:32]


def load_pin(path: Path, expected_sha256: str):
    """Read the pinned served IDs. Returns (link key -> ID, every pinned ID).

    Refuses (SystemExit PIN_REFUSED_EXIT, the reason on stderr) when the file is missing or unreadable, or when its
    sha256 is not expected_sha256. Called before the old aphis.db is removed, so a refusal leaves it in place.
    """
    try:
        data = Path(path).read_bytes()
    except OSError as e:
        print(f"REFUSED: the inspection ID pin {path} can't be read ({e.__class__.__name__}); aphis.db is left as it "
              f"was. Restore the file from the orchestrators repository.", file=sys.stderr)
        raise SystemExit(PIN_REFUSED_EXIT)
    got = hashlib.sha256(data).hexdigest()
    if got != expected_sha256:
        print(f"REFUSED: the inspection ID pin {path} has sha256 {got[:16]}…, not the pinned {expected_sha256[:16]}…; "
              f"aphis.db is left as it was. Restore the file from the orchestrators repository.", file=sys.stderr)
        raise SystemExit(PIN_REFUSED_EXIT)
    by_link = {}
    pinned = set()
    for line in data.decode().splitlines()[1:]:
        key, rec_id = line.split("\t")
        pinned.add(rec_id)
        if key != "nolink":
            by_link[key] = rec_id
    logger.info(f"Inspection ID pin: {len(pinned):,} served IDs, {len(by_link):,} with a link")
    return by_link, pinned


def load_previous_build(path: Path, pinned: set):
    """Read, from the aphis.db this build replaces, the row of every pinned served ID and every facility row.
    Returns (id -> inspection row, certificate -> facility row), in INSPECTION_COLUMNS / FACILITY_COLUMNS order.

    Refuses (SystemExit PREVIOUS_REFUSED_EXIT, the reason on stderr) when the file is missing, can't be read as a
    database (SQLite's quick_check included), lacks a table or column the carry reads, or lacks a pinned ID. Called
    before connect() removes the old aphis.db, so a refusal leaves it in place.
    """
    def refuse(why):
        print(f"REFUSED: the previous build {path} {why}, so served reports the extract no longer carries would be "
              f"dropped (decisions_log §859, §898 item 2); aphis.db is left as it was. Put the served aphis.db back "
              f"(run_aphis.sh keeps a copy before each rebuild; the server holds the served file) and run the build "
              f"again.", file=sys.stderr)
        raise SystemExit(PREVIOUS_REFUSED_EXIT)

    path = Path(path)
    if not path.is_file():
        refuse("is missing")
    try:
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
        try:
            check = conn.execute("PRAGMA quick_check").fetchone()[0]
            if check != "ok":
                refuse(f"fails SQLite's quick_check ({str(check)[:80]})")
            insp = {r[0]: r for r in conn.execute(f"SELECT {', '.join(INSPECTION_COLUMNS)} FROM inspections")
                    if r[0] in pinned}
            fac = {r[0]: r for r in conn.execute(f"SELECT {', '.join(FACILITY_COLUMNS)} FROM facilities")}
        finally:
            conn.close()
    except sqlite3.Error as e:
        refuse(f"can't be read as a build ({e.__class__.__name__}: {str(e)[:80]})")
    missing = pinned - set(insp)
    if missing:
        refuse(f"lacks {len(missing):,} pinned served ID(s), for example {sorted(missing)[0]}")
    logger.info(f"Previous build: {len(insp):,} pinned served reports and {len(fac):,} facility rows read from {path}")
    return insp, fac


def load_results(category: str) -> list[dict]:
    """Load results for a category from all_results.json or prefix files."""
    cat_dir = RAW_DIR / category

    # Try all_results.json first
    all_path = cat_dir / "all_results.json"
    if all_path.exists():
        with open(all_path) as f:
            data = json.load(f)
        results = data.get("results", []) if isinstance(data, dict) else data
        logger.info(f"Loaded {len(results)} records for {category} (all_results.json)")
        return results

    # Fall back to prefix_*.json files
    if not cat_dir.exists():
        logger.warning(f"No results directory for {category}")
        return []

    results = []
    prefix_files = sorted(cat_dir.glob("prefix_*.json"))
    if not prefix_files:
        logger.warning(f"No results files for {category}")
        return []

    for pf in prefix_files:
        with open(pf) as f:
            data = json.load(f)
        chunk = data.get("results", []) if isinstance(data, dict) else data
        results.extend(chunk)

    logger.info(f"Loaded {len(results)} records for {category} ({len(prefix_files)} prefix files)")
    return results


class DatabaseBuilder:
    def __init__(self, db_path: Path, pin=None):
        self.db_path = db_path
        self.conn = None
        self.facilities_seen = set()
        self.pin = pin if pin is not None else ({}, set())

    def connect(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.db_path.exists():
            self.db_path.unlink()
        # Side files go with the database (decisions_log §847 A1; relay datadawn green #3 item 3; the same lines as
        # white's fix for builders 05, 11 and 22, which have no shared helper). Measured 2026-11-05 on SQLite 3.45.1:
        # while another process still holds the old aphis.db open, its -wal and -shm stay behind, and the new file's
        # first statement fails with "disk I/O error" (the file stays unreadable after that reader closes); removed
        # first, the new file builds normally. Unconditional: side files can outlive their database, and SQLite
        # discards them beside a new, empty file anyway, so nothing it would have kept is lost here.
        for _side in ("-wal", "-shm", "-journal"):
            self.db_path.with_name(self.db_path.name + _side).unlink(missing_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        logger.info(f"Database created at {self.db_path}")

    def close(self):
        if self.conn:
            self.conn.close()

    def upsert_facility(self, record: dict):
        """Extract facility info from any record type and upsert."""
        cert = get_field(record,
            "web_certNumber", "certNumber", "cert_number",
            "certificateNumber", "CertificateNumber",
        )
        if not cert:
            return

        if cert in self.facilities_seen:
            # Update last_seen_date if we have a date
            date = get_field(record,
                "web_inspectionDate", "inspectionDate", "inspection_date",
                "actionDate", "action_date", "reportDate",
            )
            if date:
                date = normalize_date(date)
                self.conn.execute(
                    "UPDATE facilities SET last_seen_date = MAX(COALESCE(last_seen_date, ''), ?) WHERE cert_number = ?",
                    (date, cert),
                )
            return

        self.facilities_seen.add(cert)

        self.conn.execute("""
            INSERT OR IGNORE INTO facilities
            (cert_number, customer_number, legal_name, dba_name, site_name,
             site_address, site_city, site_state, site_zip, license_type, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            cert,
            get_field(record, "web_customerNumber", "customerNumber", "customer_number"),
            get_field(record, "web_legalName", "legalName", "legal_name", "Legal Name"),
            get_field(record, "web_dbaName", "dbaName", "dba_name", "DBA"),
            get_field(record, "web_siteName", "siteName", "site_name", "Site Name"),
            get_field(record, "web_siteAddress", "siteAddress", "site_address", "Address"),
            get_field(record, "web_siteCity", "siteCity", "site_city", "city", "City"),
            get_field(record, "web_siteState", "siteState", "site_state", "state", "State"),
            get_field(record, "web_siteZip", "siteZip", "site_zip", "zip", "Zip"),
            get_field(record, "web_licenseType", "licenseType", "license_type", "License Type", "Type", "certType"),
            get_field(record, "web_status", "status", "Status"),
        ))

    def import_inspections(self, records: list[dict]):
        """Import inspection report records, one row per report (owner queue 1015; §856; option B).

        Reports with a link go first, each keyed on its link: its pinned ID if the link is pinned, otherwise an ID
        from the link. Then reports with no link, under the fallback ID (certificate | date | customer) they have
        always had, unless a report with a link holds that ID already. Nothing is replaced: a row whose ID is taken
        is not added, and the log counts it.
        """
        logger.info(f"Importing {len(records)} inspection records...")
        by_link, pinned = self.pin
        linked = [r for r in records if get_field(r, *LINK_FIELDS)]
        unlinked = [r for r in records if not get_field(r, *LINK_FIELDS)]
        count = n_pinned = n_from_link = n_fallback = 0
        not_added = {"same link": 0, "other link, same ID": 0, "no link, ID held by a linked report": 0,
                     "no link, ID taken": 0}
        link_of = {}
        used = set()

        for r in linked + unlinked:
            self.upsert_facility(r)

            cert = get_field(r, *CERT_FIELDS)
            date = normalize_date(get_field(r, *DATE_FIELDS))

            # Find PDF URL
            pdf_url = get_field(r, *LINK_FIELDS)
            if pdf_url:
                rec_id = by_link.get(link_key(pdf_url))
                is_pinned = rec_id is not None
                if rec_id is None:
                    rec_id = hash_id_from_url(pdf_url)
            else:
                rec_id = generate_id(r, CERT_FIELDS, DATE_FIELDS, CUSTOMER_FIELDS)

            pdf_local = None
            if pdf_url and r.get("hash_id"):
                pdf_path = BASE_DIR / "pdfs" / "inspections" / f"{r['hash_id']}.pdf"
                if pdf_path.exists():
                    pdf_local = str(pdf_path.relative_to(BASE_DIR))

            try:
                self.conn.execute("""
                    INSERT INTO inspections
                    (id, cert_number, inspection_date, inspection_type,
                     critical_count, noncritical_count, direct_count,
                     teachable_moment_count, pdf_url, pdf_local_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec_id,
                    cert,
                    date,
                    get_field(r, "web_inspectionType", "inspectionType", "inspection_type"),
                    safe_int(get_field(r, "web_critical", "critical", "criticalCount")),
                    safe_int(get_field(r, "web_nonCritical", "nonCritical", "nonCriticalCount")),
                    safe_int(get_field(r, "web_direct", "direct", "directCount")),
                    safe_int(get_field(r, "web_teachableMoment", "teachableMoment", "teachableMomentCount", "teachableMoments")),
                    pdf_url,
                    pdf_local,
                ))
                count += 1
                used.add(rec_id)
                if pdf_url:
                    link_of[rec_id] = pdf_url
                    if is_pinned:
                        n_pinned += 1
                    else:
                        n_from_link += 1
                else:
                    n_fallback += 1
            except sqlite3.IntegrityError:
                if pdf_url and link_of.get(rec_id) == pdf_url:
                    not_added["same link"] += 1
                elif pdf_url:
                    not_added["other link, same ID"] += 1
                elif rec_id in link_of:
                    not_added["no link, ID held by a linked report"] += 1
                else:
                    not_added["no link, ID taken"] += 1
                logger.warning(f"Inspection not added, ID {rec_id} already in this build "
                               f"({'link' if pdf_url else 'no link'}, cert {cert or '-'}, date {date or '-'})")

        self.conn.commit()
        logger.info(f"  Inserted {count} inspection records: {n_pinned:,} under a pinned served ID, "
                    f"{n_from_link:,} under an ID from their link, {n_fallback:,} with no link under their fallback ID")
        logger.info(f"  {not_added['same link']} report(s) not added: same link as a report already in this build")
        logger.info(f"  {not_added['other link, same ID']} report(s) not added: a different link gave the same ID")
        logger.info(f"  {not_added['no link, ID held by a linked report']} report(s) with no link not added: "
                    f"their fallback ID belongs to a linked report")
        logger.info(f"  {not_added['no link, ID taken']} report(s) with no link not added: their fallback ID is "
                    f"another such report's")
        logger.info(f"  {len(pinned - used):,} pinned served ID(s) carried by no report in this extract")

    def carry_forward_absent_reports(self, previous):
        """Carry forward, unchanged, every pinned served report that no report in this extract carries (decisions_log
        §898 item 2, option (a)), with its facility row when this build has none for its certificate. Reads the built
        tables, not the import's own counts, and re-reads them after: a pinned ID still absent stops the build."""
        prev_insp, prev_fac = previous
        _by_link, pinned = self.pin
        present = {r[0] for r in self.conn.execute("SELECT id FROM inspections")}
        absent = sorted(pinned - present)
        have_fac = {r[0] for r in self.conn.execute("SELECT cert_number FROM facilities")}
        n_fac = n_no_fac = 0
        for rec_id in absent:
            row = prev_insp[rec_id]
            self.conn.execute(f"INSERT INTO inspections ({', '.join(INSPECTION_COLUMNS)}) "
                              f"VALUES ({', '.join('?' * len(INSPECTION_COLUMNS))})", row)
            cert = row[1]
            if cert and cert not in have_fac:
                if cert in prev_fac:
                    self.conn.execute(f"INSERT INTO facilities ({', '.join(FACILITY_COLUMNS)}) "
                                      f"VALUES ({', '.join('?' * len(FACILITY_COLUMNS))})", prev_fac[cert])
                    have_fac.add(cert)
                    n_fac += 1
                else:
                    n_no_fac += 1
        self.conn.commit()
        still = pinned - {r[0] for r in self.conn.execute("SELECT id FROM inspections")}
        if still:
            raise RuntimeError(f"carry-forward post-state: {len(still):,} pinned served ID(s) still absent, for "
                               f"example {sorted(still)[0]}")
        logger.info(f"  {len(absent):,} pinned served report(s) carried forward unchanged from the previous build "
                    f"(absent from this extract; decisions_log §898 item 2), with {n_fac:,} facility row(s) this build "
                    f"had none for")
        if n_no_fac:
            logger.warning(f"  {n_no_fac:,} carried report(s) have a certificate with no facility row in either build")

    def import_enforcement(self, records: list[dict]):
        """Import enforcement action records."""
        logger.info(f"Importing {len(records)} enforcement records...")
        count = 0

        for r in records:
            self.upsert_facility(r)

            cert = get_field(r, "web_certNumber", "certNumber", "cert_number")
            rec_id = r.get("hash_id") or generate_id(r,
                ("web_certNumber", "certNumber", "cert_number"),
                ("web_actionDate", "actionDate", "action_date"),
                ("web_actionType", "actionType", "action_type"),
            )

            pdf_url = get_field(r, "reportUrl", "pdfUrl", "documentUrl")
            pdf_local = None
            if pdf_url and r.get("hash_id"):
                pdf_path = BASE_DIR / "pdfs" / "enforcement" / f"{r['hash_id']}.pdf"
                if pdf_path.exists():
                    pdf_local = str(pdf_path.relative_to(BASE_DIR))

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO enforcement_actions
                    (id, cert_number, action_date, action_type,
                     penalty_amount, description, outcome,
                     pdf_url, pdf_local_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec_id,
                    cert,
                    normalize_date(get_field(r, "web_actionDate", "actionDate", "action_date")),
                    get_field(r, "web_actionType", "actionType", "action_type"),
                    safe_float(get_field(r, "web_penaltyAmount", "penaltyAmount", "penalty_amount")),
                    get_field(r, "web_description", "description"),
                    get_field(r, "web_outcome", "outcome"),
                    pdf_url,
                    pdf_local,
                ))
                count += 1
            except sqlite3.IntegrityError as e:
                logger.debug(f"Duplicate enforcement {rec_id}: {e}")

        self.conn.commit()
        logger.info(f"  Inserted {count} enforcement records")

    def import_teachable_moments(self, records: list[dict]):
        """Import teachable moment records."""
        logger.info(f"Importing {len(records)} teachable moment records...")
        count = 0

        for r in records:
            self.upsert_facility(r)

            cert = get_field(r, "web_certNumber", "certNumber", "cert_number")
            rec_id = r.get("hash_id") or generate_id(r,
                ("web_certNumber", "certNumber", "cert_number"),
                ("web_inspectionDate", "inspectionDate", "inspection_date"),
                ("web_category", "category", "tmCategory"),
            )

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO teachable_moments
                    (id, cert_number, inspection_date, category, description)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    rec_id,
                    cert,
                    normalize_date(get_field(r, "web_inspectionDate", "inspectionDate", "inspection_date")),
                    get_field(r, "web_category", "category", "tmCategory"),
                    get_field(r, "web_description", "description", "tmDescription"),
                ))
                count += 1
            except sqlite3.IntegrityError as e:
                logger.debug(f"Duplicate teachable moment {rec_id}: {e}")

        self.conn.commit()
        logger.info(f"  Inserted {count} teachable moment records")

    def import_annual_reports(self, records: list[dict]):
        """Import annual report records."""
        logger.info(f"Importing {len(records)} annual report records...")
        count = 0

        for r in records:
            self.upsert_facility(r)

            cert = get_field(r, "web_certNumber", "certNumber", "cert_number")
            rec_id = r.get("hash_id") or generate_id(r,
                ("web_certNumber", "certNumber", "cert_number"),
                ("web_reportYear", "reportYear", "report_year", "Year"),
                ("web_species", "species", "Species"),
            )

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO annual_reports
                    (id, cert_number, report_year, species, total_animals,
                     animals_pain_cat_c, animals_pain_cat_d, animals_pain_cat_e)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec_id,
                    cert,
                    safe_int(get_field(r, "web_reportYear", "reportYear", "report_year", "Year")),
                    get_field(r, "web_species", "species", "Species"),
                    safe_int(get_field(r, "web_totalAnimals", "totalAnimals", "total_animals", "Total")),
                    safe_int(get_field(r, "web_painCatC", "painCatC", "animals_pain_cat_c", "Cat C")),
                    safe_int(get_field(r, "web_painCatD", "painCatD", "animals_pain_cat_d", "Cat D")),
                    safe_int(get_field(r, "web_painCatE", "painCatE", "animals_pain_cat_e", "Cat E")),
                ))
                count += 1
            except sqlite3.IntegrityError as e:
                logger.debug(f"Duplicate annual report {rec_id}: {e}")

        self.conn.commit()
        logger.info(f"  Inserted {count} annual report records")

    def import_licensees(self, records: list[dict]):
        """Import licensee records (from downloaded PDF/Excel)."""
        logger.info(f"Importing {len(records)} licensee records...")
        count = 0

        for r in records:
            cert = get_field(r,
                "Certificate Number", "cert_number", "CertificateNumber",
                "Certificate", "Cert Number", "CERTIFICATE NUMBER",
            )
            if not cert:
                continue

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO facilities
                    (cert_number, customer_number, legal_name, dba_name, site_name,
                     site_address, site_city, site_state, site_zip, license_type, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    cert,
                    get_field(r, "Customer Number", "customer_number", "CustomerNumber"),
                    get_field(r, "Legal Name", "legal_name", "LegalName", "Name", "LEGAL NAME"),
                    get_field(r, "DBA", "dba_name", "DBAName", "DBA Name"),
                    get_field(r, "Site Name", "site_name", "SiteName"),
                    get_field(r, "Address", "site_address", "Street Address"),
                    get_field(r, "City", "site_city"),
                    get_field(r, "State", "site_state", "ST"),
                    get_field(r, "Zip", "site_zip", "Zip Code", "ZIP"),
                    get_field(r, "Type", "license_type", "License Type", "Category"),
                    get_field(r, "Status", "status", default="Active"),
                ))
                count += 1
                self.facilities_seen.add(cert)
            except sqlite3.IntegrityError as e:
                logger.debug(f"Duplicate licensee {cert}: {e}")

        self.conn.commit()
        logger.info(f"  Inserted {count} licensee/facility records")

    def build_fts(self):
        """Build full-text search index."""
        logger.info("Building FTS5 index...")
        self.conn.executescript(FTS_SCHEMA)
        self.conn.execute("""
            INSERT INTO facilities_fts(facilities_fts) VALUES('rebuild')
        """)
        self.conn.commit()

        count = self.conn.execute("SELECT COUNT(*) FROM facilities_fts").fetchone()[0]
        logger.info(f"  FTS index built with {count} entries")

    def print_stats(self):
        """Print database statistics."""
        tables = ["facilities", "inspections", "enforcement_actions", "teachable_moments", "annual_reports"]
        logger.info("\n=== Database Statistics ===")
        for table in tables:
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                logger.info(f"  {table}: {count:,} records")
            except sqlite3.OperationalError:
                logger.info(f"  {table}: (table not created)")

        # DB file size
        size_mb = self.db_path.stat().st_size / (1024 * 1024)
        logger.info(f"  Database size: {size_mb:.1f} MB")


def main():
    from lib.corpus_gate import acquire as _corpus_acquire
    _corpus_acquire("rebuild aphis.db from raw/ extraction output", "aphis-scripts")

    DB_DIR.mkdir(parents=True, exist_ok=True)

    # Before connect(), which removes the old aphis.db: a refusal here leaves it in place.
    pin = load_pin(PIN_PATH, PIN_SHA256)
    # Also before connect(): the previous build's pinned reports, for the carry-forward (refuses when it can't
    # supply them).
    previous = load_previous_build(DB_PATH, pin[1])

    builder = DatabaseBuilder(DB_PATH, pin=pin)
    builder.connect()

    try:
        # Import in order: licensees first (establishes facility records),
        # then inspections, enforcement, teachable moments (annual reports are skipped, below)
        licensee_records = load_results("licensees")
        if licensee_records:
            builder.import_licensees(licensee_records)

        inspection_records = load_results("inspections")
        if inspection_records:
            builder.import_inspections(inspection_records)

        enforcement_records = load_results("enforcement")
        if enforcement_records:
            builder.import_enforcement(enforcement_records)

        tm_records = load_results("teachable_moments")
        if tm_records:
            builder.import_teachable_moments(tm_records)

        # Annual reports are NOT imported (relay datadawn green #3 item 2, the drafter's call; the importer's fix is
        # its own item). import_annual_reports reads field names the extract (pulled 2026-03-03 UTC) doesn't have
        # (it has `year` and a column per species), so every record's ID came from its certificate alone: measured
        # 11-09, 12,044 records built 1,598 rows (the distinct certificates), every field empty but the certificate,
        # plus a facility row with no name for each of 196 certificates seen only there. annual_reports stays in the
        # schema, built empty, as served today.
        ar_records = load_results("annual_reports")
        logger.warning(f"Annual reports: {len(ar_records):,} annual-report record(s) in the extract NOT imported "
                       f"(relay datadawn green #3 item 2); annual_reports is built empty, as served")

        # Served reports the extract no longer carries, carried forward unchanged (decisions_log §898 item 2)
        builder.carry_forward_absent_reports(previous)

        # Build FTS index
        builder.build_fts()

        # Print stats
        builder.print_stats()

    finally:
        builder.close()

    # Past the try/finally: an exception in the build propagates and never reaches here,
    # so this line is the success point.
    # In-file build stamp (data-history audit 2026-09-06): a published dump is a bare SQLite
    # file and the R2 manifest does not survive a download, so build_metadata is the only
    # vintage a consumer retains. Shared helper — one implementation, three callers.
    # Non-gating: warns and proceeds. Reached ONLY on a genuinely successful build.
    sys.path.insert(0, "/mnt/data/datadawn/tools")
    from stamp_db import stamp_build_metadata
    stamp_build_metadata(DB_PATH, log=logger.info)

    logger.info(f"\nDatabase ready at: {DB_PATH}")


if __name__ == "__main__":
    main()
