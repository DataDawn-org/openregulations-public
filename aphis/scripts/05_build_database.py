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

import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.aura_client import BASE_DIR, RAW_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DB_DIR = BASE_DIR / "db"
DB_PATH = DB_DIR / "aphis.db"

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
    content='facilities', content_rowid='rowid'
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
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = None
        self.facilities_seen = set()

    def connect(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.db_path.exists():
            self.db_path.unlink()
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
        """Import inspection report records."""
        logger.info(f"Importing {len(records)} inspection records...")
        count = 0

        for r in records:
            self.upsert_facility(r)

            cert = get_field(r, "web_certNumber", "certNumber", "cert_number")
            date = normalize_date(get_field(r, "web_inspectionDate", "inspectionDate", "inspection_date"))

            rec_id = r.get("hash_id") or generate_id(r,
                ("web_certNumber", "certNumber", "cert_number"),
                ("web_inspectionDate", "inspectionDate", "inspection_date"),
                ("web_customerNumber", "customerNumber", "customer_number"),
            )

            # Find PDF URL
            pdf_url = get_field(r, "reportUrl", "reportLink", "pdfUrl", "documentUrl", "web_reportUrl")
            pdf_local = None
            if pdf_url and r.get("hash_id"):
                pdf_path = BASE_DIR / "pdfs" / "inspections" / f"{r['hash_id']}.pdf"
                if pdf_path.exists():
                    pdf_local = str(pdf_path.relative_to(BASE_DIR))

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO inspections
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
            except sqlite3.IntegrityError as e:
                logger.debug(f"Duplicate inspection {rec_id}: {e}")

        self.conn.commit()
        logger.info(f"  Inserted {count} inspection records")

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
    DB_DIR.mkdir(parents=True, exist_ok=True)

    builder = DatabaseBuilder(DB_PATH)
    builder.connect()

    try:
        # Import in order: licensees first (establishes facility records),
        # then inspections, enforcement, teachable moments, annual reports
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

        ar_records = load_results("annual_reports")
        if ar_records:
            builder.import_annual_reports(ar_records)

        # Build FTS index
        builder.build_fts()

        # Print stats
        builder.print_stats()

    finally:
        builder.close()

    logger.info(f"\nDatabase ready at: {DB_PATH}")


if __name__ == "__main__":
    main()
