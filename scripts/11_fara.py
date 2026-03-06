#!/usr/bin/env python3
"""
Phase 11: FARA (Foreign Agents Registration Act) Data

Downloads all FARA bulk data from the Department of Justice efile system
and loads it into a SQLite database.

Four datasets (all as CSV ZIPs):
  - Registrants: firms/entities registered as foreign agents
  - Foreign Principals: the foreign governments/entities they represent
  - Short Forms: individual people working as foreign agents
  - Registrant Docs: links to all filed PDF documents

Source: https://efile.fara.gov (no API key required, updated daily)
Encoding: ISO-8859-1 (not UTF-8)

Usage:
    python3 scripts/11_fara.py                  # download + build DB
    python3 scripts/11_fara.py --download-only  # just download ZIPs
    python3 scripts/11_fara.py --build-only     # just rebuild DB from existing CSVs
"""

import argparse
import csv
import io
import logging
import sqlite3
import sys
import time
import zipfile
from pathlib import Path

import requests

# === Configuration ===
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "fara"
DB_PATH = PROJECT_DIR / "fara.db"
LOG_DIR = PROJECT_DIR / "logs"
PROGRESS_FILE = LOG_DIR / "progress.txt"

BULK_BASE = "https://efile.fara.gov/bulk/zip"
BULK_FILES = {
    "registrants": "FARA_All_Registrants.csv.zip",
    "foreign_principals": "FARA_All_ForeignPrincipals.csv.zip",
    "short_forms": "FARA_All_ShortForms.csv.zip",
    "registrant_docs": "FARA_All_RegistrantDocs.csv.zip",
}

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("fara")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "fara.log")
_fh.setFormatter(_fmt)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
log.addHandler(_fh)
log.addHandler(_sh)


def progress(msg):
    with open(PROGRESS_FILE, "a") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


# === Schema ===
SCHEMA = {
    "fara_registrants": """
        CREATE TABLE IF NOT EXISTS fara_registrants (
            registration_number TEXT PRIMARY KEY,
            registration_date TEXT,
            termination_date TEXT,
            name TEXT,
            business_name TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state TEXT,
            zip TEXT
        )
    """,
    "fara_foreign_principals": """
        CREATE TABLE IF NOT EXISTS fara_foreign_principals (
            registration_number TEXT,
            registrant_date TEXT,
            registrant_name TEXT,
            foreign_principal TEXT,
            fp_registration_date TEXT,
            fp_termination_date TEXT,
            country TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state TEXT,
            zip TEXT
        )
    """,
    "fara_short_forms": """
        CREATE TABLE IF NOT EXISTS fara_short_forms (
            registration_number TEXT,
            registration_date TEXT,
            registrant_name TEXT,
            short_form_date TEXT,
            short_form_termination_date TEXT,
            last_name TEXT,
            first_name TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state TEXT,
            zip TEXT
        )
    """,
    "fara_registrant_docs": """
        CREATE TABLE IF NOT EXISTS fara_registrant_docs (
            registration_number TEXT,
            registrant_name TEXT,
            date_stamped TEXT,
            document_type TEXT,
            short_form_name TEXT,
            foreign_principal_name TEXT,
            foreign_principal_country TEXT,
            url TEXT
        )
    """,
}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fp_reg ON fara_foreign_principals(registration_number)",
    "CREATE INDEX IF NOT EXISTS idx_fp_country ON fara_foreign_principals(country)",
    "CREATE INDEX IF NOT EXISTS idx_fp_principal ON fara_foreign_principals(foreign_principal)",
    "CREATE INDEX IF NOT EXISTS idx_sf_reg ON fara_short_forms(registration_number)",
    "CREATE INDEX IF NOT EXISTS idx_sf_name ON fara_short_forms(last_name, first_name)",
    "CREATE INDEX IF NOT EXISTS idx_rd_reg ON fara_registrant_docs(registration_number)",
    "CREATE INDEX IF NOT EXISTS idx_rd_type ON fara_registrant_docs(document_type)",
    "CREATE INDEX IF NOT EXISTS idx_reg_name ON fara_registrants(name)",
]

# Column mappings: CSV header → DB column (keyed by CSV header name)
HEADER_MAP = {
    "registrants": {
        "Registration Number": "registration_number",
        "Registration Date": "registration_date",
        "Termination Date": "termination_date",
        "Name": "name",
        "Business Name": "business_name",
        "Address 1": "address_1",
        "Address 2": "address_2",
        "City": "city",
        "State": "state",
        "Zip": "zip",
    },
    "foreign_principals": {
        "Registration Number": "registration_number",
        "Registrant Date": "registrant_date",
        "Registrant Name": "registrant_name",
        "Foreign Principal": "foreign_principal",
        "Foreign Principal Registration Date": "fp_registration_date",
        "Foreign Principal Termination Date": "fp_termination_date",
        "Country/Location Represented": "country",
        "Address 1": "address_1",
        "Address 2": "address_2",
        "City": "city",
        "State": "state",
        "Zip": "zip",
    },
    "short_forms": {
        "Registration Number": "registration_number",
        "Registration Date": "registration_date",
        "Registrant Name": "registrant_name",
        "Short Form Date": "short_form_date",
        "Short Form Termination Date": "short_form_termination_date",
        "Short Form Last Name": "last_name",
        "Short Form First Name": "first_name",
        "Address 1": "address_1",
        "Address 2": "address_2",
        "City": "city",
        "State": "state",
        "Zip": "zip",
    },
    "registrant_docs": {
        "Registration Number": "registration_number",
        "Registrant Name": "registrant_name",
        "Date Stamped": "date_stamped",
        "Document Type": "document_type",
        "Short Form Name": "short_form_name",
        "Foreign Principal Name": "foreign_principal_name",
        "Foreign Principal Country": "foreign_principal_country",
        "URL": "url",
    },
}

TABLE_NAMES = {
    "registrants": "fara_registrants",
    "foreign_principals": "fara_foreign_principals",
    "short_forms": "fara_short_forms",
    "registrant_docs": "fara_registrant_docs",
}


# === Download ===
def download_bulk_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()

    for key, filename in BULK_FILES.items():
        url = f"{BULK_BASE}/{filename}"
        out = DATA_DIR / filename
        log.info(f"Downloading {filename}...")

        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        out.write_bytes(resp.content)

        size_mb = len(resp.content) / 1024 / 1024
        log.info(f"  {filename}: {size_mb:.1f} MB")

    log.info("All bulk files downloaded")
    progress("FARA: bulk files downloaded")


# === Build database ===
def build_database():
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-512000")

    # Create tables
    for table, ddl in SCHEMA.items():
        conn.execute(ddl)

    for key, filename in BULK_FILES.items():
        zip_path = DATA_DIR / filename
        if not zip_path.exists():
            log.warning(f"  {filename} not found — skipping")
            continue

        table = TABLE_NAMES[key]
        hmap = HEADER_MAP[key]

        log.info(f"Loading {filename} → {table}...")

        with zipfile.ZipFile(zip_path) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                log.warning(f"  No CSV found in {filename}")
                continue

            with zf.open(csv_names[0]) as f:
                # ISO-8859-1 encoding
                text = io.TextIOWrapper(f, encoding="latin-1")
                reader = csv.reader(text)
                header = next(reader, None)

                if header is None:
                    log.warning(f"  Empty CSV in {filename}")
                    continue

                log.info(f"  CSV columns: {header}")

                # Build column index mapping: CSV position → DB column order
                db_columns = list(hmap.values())
                col_indices = []
                for csv_col in header:
                    csv_col = csv_col.strip()
                    if csv_col in hmap:
                        col_indices.append((header.index(csv_col), hmap[csv_col]))

                # Order by DB schema column order
                ordered = sorted(col_indices, key=lambda x: db_columns.index(x[1]))
                pick_indices = [i for i, _ in ordered]
                insert_cols = [c for _, c in ordered]
                placeholders = ",".join(["?"] * len(insert_cols))
                col_list = ",".join(insert_cols)
                sql = f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"

                batch = []
                count = 0
                for row in reader:
                    mapped = []
                    for idx in pick_indices:
                        mapped.append(row[idx].strip() if idx < len(row) else "")
                    batch.append(mapped)

                    if len(batch) >= 10000:
                        conn.executemany(sql, batch)
                        count += len(batch)
                        batch = []

                if batch:
                    conn.executemany(sql, batch)
                    count += len(batch)

                conn.commit()
                log.info(f"  {table}: {count:,} rows loaded")

    # Create indexes
    log.info("Creating indexes...")
    for idx_sql in INDEXES:
        conn.execute(idx_sql)

    # Create FTS
    log.info("Creating FTS indexes...")
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fara_registrants_fts USING fts5(
            name, business_name, city, state,
            content='fara_registrants',
            content_rowid='rowid'
        )
    """)
    conn.execute("""
        INSERT INTO fara_registrants_fts(rowid, name, business_name, city, state)
        SELECT rowid, COALESCE(name,''), COALESCE(business_name,''),
               COALESCE(city,''), COALESCE(state,'')
        FROM fara_registrants
    """)

    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fara_foreign_principals_fts USING fts5(
            registrant_name, foreign_principal, country,
            content='fara_foreign_principals',
            content_rowid='rowid'
        )
    """)
    conn.execute("""
        INSERT INTO fara_foreign_principals_fts(rowid, registrant_name, foreign_principal, country)
        SELECT rowid, COALESCE(registrant_name,''), COALESCE(foreign_principal,''),
               COALESCE(country,'')
        FROM fara_foreign_principals
    """)

    conn.commit()

    # Stats
    log.info("=== FARA Database Stats ===")
    for table in TABLE_NAMES.values():
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"  {table}: {count:,}")

    db_size = DB_PATH.stat().st_size / 1024 / 1024
    log.info(f"  Database size: {db_size:.1f} MB")

    conn.close()
    progress(f"FARA: database built — {db_size:.1f} MB")


def main():
    parser = argparse.ArgumentParser(description="Download and build FARA database")
    parser.add_argument("--download-only", action="store_true", help="Only download bulk files")
    parser.add_argument("--build-only", action="store_true", help="Only build DB from existing CSVs")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("FARA DATA — Foreign Agents Registration Act")
    log.info("=" * 60)
    progress("FARA: starting")

    t0 = time.time()

    if not args.build_only:
        download_bulk_files()

    if not args.download_only:
        build_database()

    elapsed = time.time() - t0
    log.info(f"Total time: {elapsed:.0f}s")
    log.info("=" * 60)
    progress(f"FARA: COMPLETE in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
