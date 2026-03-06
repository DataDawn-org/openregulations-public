#!/usr/bin/env python3
"""
Phase 08-FEC: Download FEC campaign finance bulk data and build SQLite database.

Downloads bulk CSV files from the FEC (Federal Election Commission) and loads
them into a SQLite database with proper tables and indexes.

Data sources (no API key needed — all public bulk downloads):
  - Committee master (cm)
  - Candidate master (cn)
  - Candidate-committee linkages (ccl)
  - Individual contributions (indiv) — LARGE, multi-GB per cycle
  - Committee contributions to candidates (pas2)
  - Committee-to-committee transactions (oth)

Also builds a bioguide_id crosswalk using congress-legislators data from
unitedstates/congress-legislators, mapping FEC candidate IDs to bioguide IDs.

Covers election cycles 2010-2026 (matching lobbying disclosure coverage).

Usage:
    python3 08_fec_campaign_finance.py                  # download all, build DB
    python3 08_fec_campaign_finance.py --download-only  # just download bulk ZIPs
    python3 08_fec_campaign_finance.py --build-only     # just build DB from existing files
    python3 08_fec_campaign_finance.py --crosswalk-only # just build bioguide crosswalk
    python3 08_fec_campaign_finance.py --cycles 2024 2022  # specific cycles only
"""

import argparse
import csv
import io
import json
import logging
import os
import signal
import sqlite3
import sys
import time
import zipfile
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
PROJECT_DIR = Path(__file__).resolve().parent.parent
BULK_DIR = PROJECT_DIR / "fec" / "bulk"
DB_PATH = PROJECT_DIR / "fec.db"
LOG_DIR = PROJECT_DIR / "logs"

# Election cycles to download (even years, 2010-2026)
ALL_CYCLES = list(range(2010, 2028, 2))  # [2010, 2012, ..., 2026]

# FEC bulk data base URL
FEC_BULK_BASE = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads"

# congress-legislators for bioguide crosswalk (JSON, not YAML — no PyYAML needed)
LEGISLATORS_CURRENT_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-current.json"
LEGISLATORS_HISTORICAL_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-historical.json"

# === Bulk data file definitions ===
# Each entry: (file_prefix, description, column_names)
BULK_FILES = {
    "cn": {
        "description": "Candidate Master",
        "columns": [
            "CAND_ID", "CAND_NAME", "CAND_PTY_AFFILIATION",
            "CAND_ELECTION_YR", "CAND_OFFICE_ST", "CAND_OFFICE",
            "CAND_OFFICE_DISTRICT", "CAND_ICI", "CAND_STATUS",
            "CAND_PCC", "CAND_ST1", "CAND_ST2", "CAND_CITY",
            "CAND_ST", "CAND_ZIP",
        ],
    },
    "cm": {
        "description": "Committee Master",
        "columns": [
            "CMTE_ID", "CMTE_NM", "TRES_NM", "CMTE_ST1", "CMTE_ST2",
            "CMTE_CITY", "CMTE_ST", "CMTE_ZIP", "CMTE_DSGN", "CMTE_TP",
            "CMTE_PTY_AFFILIATION", "CMTE_FILING_FREQ", "ORG_TP",
            "CONNECTED_ORG_NM", "CAND_ID",
        ],
    },
    "ccl": {
        "description": "Candidate-Committee Linkage",
        "columns": [
            "CAND_ID", "CAND_ELECTION_YR", "FEC_ELECTION_YR",
            "CMTE_ID", "CMTE_TP", "CMTE_DSGN", "LINKAGE_ID",
        ],
    },
    "pas2": {
        "description": "Committee Contributions to Candidates",
        "columns": [
            "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI",
            "IMAGE_NUM", "TRANSACTION_TP", "ENTITY_TP", "NAME",
            "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION",
            "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", "CAND_ID",
            "TRAN_ID", "FILE_NUM", "MEMO_CD", "MEMO_TEXT", "SUB_ID",
        ],
    },
    "indiv": {
        "description": "Individual Contributions",
        "columns": [
            "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI",
            "IMAGE_NUM", "TRANSACTION_TP", "ENTITY_TP", "NAME",
            "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION",
            "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", "TRAN_ID",
            "FILE_NUM", "MEMO_CD", "MEMO_TEXT", "SUB_ID",
        ],
    },
    "oth": {
        "description": "Committee-to-Committee Transactions",
        "columns": [
            "CMTE_ID", "AMNDT_IND", "RPT_TP", "TRANSACTION_PGI",
            "IMAGE_NUM", "TRANSACTION_TP", "ENTITY_TP", "NAME",
            "CITY", "STATE", "ZIP_CODE", "EMPLOYER", "OCCUPATION",
            "TRANSACTION_DT", "TRANSACTION_AMT", "OTHER_ID", "TRAN_ID",
            "FILE_NUM", "MEMO_CD", "MEMO_TEXT", "SUB_ID",
        ],
    },
}

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("fec_download")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "fec_download.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass

# === Graceful shutdown ===
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.warning("Shutdown signal received - finishing current operation then stopping")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# === Helpers ===

def create_session():
    """Create requests session with retry logic."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "OpenRegs/1.0 (campaign finance data project)"
    })
    return session


def cycle_to_suffix(cycle: int) -> str:
    """Convert a 4-digit cycle year to 2-digit suffix: 2024 -> '24', 2010 -> '10'."""
    return str(cycle)[-2:]


def build_download_url(file_prefix: str, cycle: int) -> str:
    """Build the FEC bulk data download URL for a given file type and cycle."""
    suffix = cycle_to_suffix(cycle)
    return f"{FEC_BULK_BASE}/{cycle}/{file_prefix}{suffix}.zip"


# ── Download Phase ───────────────────────────────────────────────────────────

def download_bulk_file(session, file_prefix: str, cycle: int) -> Path:
    """
    Download a single FEC bulk data ZIP file.

    Returns the path to the downloaded ZIP, or None if download failed.
    Skips download if file already exists (resume capability).
    """
    url = build_download_url(file_prefix, cycle)
    suffix = cycle_to_suffix(cycle)
    zip_filename = f"{file_prefix}{suffix}.zip"
    cycle_dir = BULK_DIR / str(cycle)
    cycle_dir.mkdir(parents=True, exist_ok=True)
    zip_path = cycle_dir / zip_filename

    # Resume: skip if already downloaded
    if zip_path.exists() and zip_path.stat().st_size > 0:
        log.info(f"  [SKIP] {zip_filename} already exists ({zip_path.stat().st_size:,} bytes)")
        return zip_path

    log.info(f"  Downloading {url} ...")
    try:
        resp = session.get(url, timeout=600, stream=True)
        resp.raise_for_status()

        # Stream to disk to handle large files
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        tmp_path = zip_path.with_suffix(".zip.tmp")

        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                if _shutdown:
                    log.warning(f"  Shutdown during download of {zip_filename}")
                    tmp_path.unlink(missing_ok=True)
                    return None
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0 and downloaded % (50 * 1024 * 1024) < (1024 * 1024):
                    pct = downloaded / total * 100
                    log.info(f"    {downloaded:,} / {total:,} bytes ({pct:.0f}%)")

        # Rename temp to final
        tmp_path.rename(zip_path)
        log.info(f"  [OK] {zip_filename}: {zip_path.stat().st_size:,} bytes")
        return zip_path

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            log.warning(f"  [MISS] {zip_filename} not available (404) - cycle {cycle} may not have this data")
        else:
            log.error(f"  [ERROR] Failed to download {zip_filename}: {e}")
        return None
    except Exception as e:
        log.error(f"  [ERROR] Failed to download {zip_filename}: {e}")
        return None


def download_all(session, cycles: list[int]):
    """Download all bulk data files for all requested cycles."""
    log.info(f"=== FEC Bulk Data Download ===")
    log.info(f"Cycles: {cycles}")
    log.info(f"File types: {', '.join(BULK_FILES.keys())}")
    log.info(f"Output dir: {BULK_DIR}")

    BULK_DIR.mkdir(parents=True, exist_ok=True)

    total_files = len(cycles) * len(BULK_FILES)
    completed = 0
    failed = 0

    for cycle in cycles:
        if _shutdown:
            break
        log.info(f"\n--- Cycle {cycle} ---")
        for prefix, info in BULK_FILES.items():
            if _shutdown:
                break
            completed += 1
            log.info(f"[{completed}/{total_files}] {info['description']} ({prefix}{cycle_to_suffix(cycle)}.zip)")
            result = download_bulk_file(session, prefix, cycle)
            if result is None:
                failed += 1
            time.sleep(0.5)  # Be polite to FEC servers

    log.info(f"\nDownload complete: {completed - failed} succeeded, {failed} failed/missing")


# ── Database Build Phase ─────────────────────────────────────────────────────

# SQL table creation statements
CREATE_TABLES_SQL = """
-- Candidates: one row per candidate per cycle
CREATE TABLE IF NOT EXISTS fec_candidates (
    cand_id TEXT NOT NULL,
    cand_name TEXT,
    cand_pty_affiliation TEXT,
    cand_election_yr INTEGER,
    cand_office_st TEXT,
    cand_office TEXT,
    cand_office_district TEXT,
    cand_ici TEXT,
    cand_status TEXT,
    cand_pcc TEXT,
    cand_city TEXT,
    cand_st TEXT,
    cand_zip TEXT,
    cycle INTEGER NOT NULL,
    PRIMARY KEY (cand_id, cycle)
);

-- Committees: one row per committee per cycle
CREATE TABLE IF NOT EXISTS fec_committees (
    cmte_id TEXT NOT NULL,
    cmte_nm TEXT,
    tres_nm TEXT,
    cmte_city TEXT,
    cmte_st TEXT,
    cmte_zip TEXT,
    cmte_dsgn TEXT,
    cmte_tp TEXT,
    cmte_pty_affiliation TEXT,
    cmte_filing_freq TEXT,
    org_tp TEXT,
    connected_org_nm TEXT,
    cand_id TEXT,
    cycle INTEGER NOT NULL,
    PRIMARY KEY (cmte_id, cycle)
);

-- Candidate-committee linkages
CREATE TABLE IF NOT EXISTS fec_candidate_committee_linkages (
    cand_id TEXT NOT NULL,
    cand_election_yr INTEGER,
    fec_election_yr INTEGER,
    cmte_id TEXT NOT NULL,
    cmte_tp TEXT,
    cmte_dsgn TEXT,
    linkage_id INTEGER,
    cycle INTEGER NOT NULL
);

-- Committee contributions to candidates (PAC-to-candidate, party-to-candidate, etc.)
CREATE TABLE IF NOT EXISTS fec_contributions_to_candidates (
    cmte_id TEXT,
    amndt_ind TEXT,
    rpt_tp TEXT,
    transaction_pgi TEXT,
    transaction_tp TEXT,
    entity_tp TEXT,
    name TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    employer TEXT,
    occupation TEXT,
    transaction_dt TEXT,
    transaction_amt REAL,
    other_id TEXT,
    cand_id TEXT,
    tran_id TEXT,
    file_num INTEGER,
    memo_cd TEXT,
    memo_text TEXT,
    sub_id INTEGER,
    cycle INTEGER NOT NULL
);

-- Individual contributions (large table)
CREATE TABLE IF NOT EXISTS fec_individual_contributions (
    cmte_id TEXT,
    amndt_ind TEXT,
    rpt_tp TEXT,
    transaction_pgi TEXT,
    transaction_tp TEXT,
    entity_tp TEXT,
    name TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    employer TEXT,
    occupation TEXT,
    transaction_dt TEXT,
    transaction_amt REAL,
    other_id TEXT,
    tran_id TEXT,
    file_num INTEGER,
    memo_cd TEXT,
    memo_text TEXT,
    sub_id INTEGER,
    cycle INTEGER NOT NULL
);

-- Committee-to-committee transactions
CREATE TABLE IF NOT EXISTS fec_committee_transactions (
    cmte_id TEXT,
    amndt_ind TEXT,
    rpt_tp TEXT,
    transaction_pgi TEXT,
    transaction_tp TEXT,
    entity_tp TEXT,
    name TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    employer TEXT,
    occupation TEXT,
    transaction_dt TEXT,
    transaction_amt REAL,
    other_id TEXT,
    tran_id TEXT,
    file_num INTEGER,
    memo_cd TEXT,
    memo_text TEXT,
    sub_id INTEGER,
    cycle INTEGER NOT NULL
);

-- Bioguide crosswalk: map FEC candidate IDs to bioguide IDs
CREATE TABLE IF NOT EXISTS fec_candidate_crosswalk (
    fec_candidate_id TEXT NOT NULL,
    bioguide_id TEXT NOT NULL,
    full_name TEXT,
    first_name TEXT,
    last_name TEXT,
    party TEXT,
    state TEXT,
    chamber TEXT,
    PRIMARY KEY (fec_candidate_id, bioguide_id)
);
"""

CREATE_INDEXES_SQL = """
-- Candidate indexes
CREATE INDEX IF NOT EXISTS idx_fec_candidates_name ON fec_candidates(cand_name);
CREATE INDEX IF NOT EXISTS idx_fec_candidates_state ON fec_candidates(cand_office_st);
CREATE INDEX IF NOT EXISTS idx_fec_candidates_office ON fec_candidates(cand_office);
CREATE INDEX IF NOT EXISTS idx_fec_candidates_party ON fec_candidates(cand_pty_affiliation);
CREATE INDEX IF NOT EXISTS idx_fec_candidates_cycle ON fec_candidates(cycle);

-- Committee indexes
CREATE INDEX IF NOT EXISTS idx_fec_committees_name ON fec_committees(cmte_nm);
CREATE INDEX IF NOT EXISTS idx_fec_committees_type ON fec_committees(cmte_tp);
CREATE INDEX IF NOT EXISTS idx_fec_committees_dsgn ON fec_committees(cmte_dsgn);
CREATE INDEX IF NOT EXISTS idx_fec_committees_cand ON fec_committees(cand_id);
CREATE INDEX IF NOT EXISTS idx_fec_committees_connected ON fec_committees(connected_org_nm);
CREATE INDEX IF NOT EXISTS idx_fec_committees_cycle ON fec_committees(cycle);

-- Linkage indexes
CREATE INDEX IF NOT EXISTS idx_fec_ccl_cand ON fec_candidate_committee_linkages(cand_id);
CREATE INDEX IF NOT EXISTS idx_fec_ccl_cmte ON fec_candidate_committee_linkages(cmte_id);
CREATE INDEX IF NOT EXISTS idx_fec_ccl_cycle ON fec_candidate_committee_linkages(cycle);

-- Contributions to candidates indexes
CREATE INDEX IF NOT EXISTS idx_fec_pas2_cmte ON fec_contributions_to_candidates(cmte_id);
CREATE INDEX IF NOT EXISTS idx_fec_pas2_cand ON fec_contributions_to_candidates(cand_id);
CREATE INDEX IF NOT EXISTS idx_fec_pas2_cycle ON fec_contributions_to_candidates(cycle);
CREATE INDEX IF NOT EXISTS idx_fec_pas2_amt ON fec_contributions_to_candidates(transaction_amt);
CREATE INDEX IF NOT EXISTS idx_fec_pas2_date ON fec_contributions_to_candidates(transaction_dt);
CREATE INDEX IF NOT EXISTS idx_fec_pas2_state ON fec_contributions_to_candidates(state);

-- Individual contributions indexes
CREATE INDEX IF NOT EXISTS idx_fec_indiv_cmte ON fec_individual_contributions(cmte_id);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_name ON fec_individual_contributions(name);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_employer ON fec_individual_contributions(employer);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_occupation ON fec_individual_contributions(occupation);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_cycle ON fec_individual_contributions(cycle);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_amt ON fec_individual_contributions(transaction_amt);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_date ON fec_individual_contributions(transaction_dt);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_state ON fec_individual_contributions(state);
CREATE INDEX IF NOT EXISTS idx_fec_indiv_zip ON fec_individual_contributions(zip_code);

-- Committee-to-committee indexes
CREATE INDEX IF NOT EXISTS idx_fec_oth_cmte ON fec_committee_transactions(cmte_id);
CREATE INDEX IF NOT EXISTS idx_fec_oth_other ON fec_committee_transactions(other_id);
CREATE INDEX IF NOT EXISTS idx_fec_oth_cycle ON fec_committee_transactions(cycle);
CREATE INDEX IF NOT EXISTS idx_fec_oth_amt ON fec_committee_transactions(transaction_amt);
CREATE INDEX IF NOT EXISTS idx_fec_oth_date ON fec_committee_transactions(transaction_dt);

-- Crosswalk indexes
CREATE INDEX IF NOT EXISTS idx_fec_xwalk_bioguide ON fec_candidate_crosswalk(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_fec_xwalk_fec ON fec_candidate_crosswalk(fec_candidate_id);
"""

# Map file prefix -> (table_name, columns_to_insert)
# We strip address fields from candidates and committees to save space,
# and strip IMAGE_NUM from transaction files (it's an internal FEC reference).
TABLE_MAP = {
    "cn": {
        "table": "fec_candidates",
        "insert_cols": [
            "cand_id", "cand_name", "cand_pty_affiliation",
            "cand_election_yr", "cand_office_st", "cand_office",
            "cand_office_district", "cand_ici", "cand_status",
            "cand_pcc", "cand_city", "cand_st", "cand_zip", "cycle",
        ],
        "source_indices": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14],
        # Indices into the BULK_FILES columns: skip CAND_ST1, CAND_ST2 (street addresses)
    },
    "cm": {
        "table": "fec_committees",
        "insert_cols": [
            "cmte_id", "cmte_nm", "tres_nm", "cmte_city", "cmte_st",
            "cmte_zip", "cmte_dsgn", "cmte_tp", "cmte_pty_affiliation",
            "cmte_filing_freq", "org_tp", "connected_org_nm", "cand_id", "cycle",
        ],
        "source_indices": [0, 1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
        # Skip CMTE_ST1, CMTE_ST2 (street addresses, indices 3 & 4)
    },
    "ccl": {
        "table": "fec_candidate_committee_linkages",
        "insert_cols": [
            "cand_id", "cand_election_yr", "fec_election_yr",
            "cmte_id", "cmte_tp", "cmte_dsgn", "linkage_id", "cycle",
        ],
        "source_indices": [0, 1, 2, 3, 4, 5, 6],
    },
    "pas2": {
        "table": "fec_contributions_to_candidates",
        "insert_cols": [
            "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi",
            "transaction_tp", "entity_tp", "name", "city", "state",
            "zip_code", "employer", "occupation", "transaction_dt",
            "transaction_amt", "other_id", "cand_id", "tran_id",
            "file_num", "memo_cd", "memo_text", "sub_id", "cycle",
        ],
        "source_indices": [0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
        # Skip IMAGE_NUM (index 4)
    },
    "indiv": {
        "table": "fec_individual_contributions",
        "insert_cols": [
            "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi",
            "transaction_tp", "entity_tp", "name", "city", "state",
            "zip_code", "employer", "occupation", "transaction_dt",
            "transaction_amt", "other_id", "tran_id", "file_num",
            "memo_cd", "memo_text", "sub_id", "cycle",
        ],
        "source_indices": [0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        # Skip IMAGE_NUM (index 4)
    },
    "oth": {
        "table": "fec_committee_transactions",
        "insert_cols": [
            "cmte_id", "amndt_ind", "rpt_tp", "transaction_pgi",
            "transaction_tp", "entity_tp", "name", "city", "state",
            "zip_code", "employer", "occupation", "transaction_dt",
            "transaction_amt", "other_id", "tran_id", "file_num",
            "memo_cd", "memo_text", "sub_id", "cycle",
        ],
        "source_indices": [0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        # Skip IMAGE_NUM (index 4)
    },
}


def parse_row(row_fields: list[str], source_indices: list[int], cycle: int) -> tuple:
    """
    Extract selected fields from a parsed pipe-delimited row and append the cycle.

    Handles rows with fewer fields than expected by padding with empty strings.
    Converts TRANSACTION_AMT fields to float where applicable.
    """
    # Pad if row is shorter than expected
    max_idx = max(source_indices) if source_indices else 0
    while len(row_fields) < max_idx + 1:
        row_fields.append("")

    values = []
    for idx in source_indices:
        val = row_fields[idx].strip() if idx < len(row_fields) else ""
        values.append(val if val else None)
    values.append(cycle)
    return tuple(values)


def stream_zip_csv(zip_path: Path, file_prefix: str, cycle: int):
    """
    Generator that yields parsed rows from a ZIP file containing a pipe-delimited
    text file. Streams the data to keep memory usage manageable.

    The FEC bulk files are pipe-delimited with no header row.
    Inside the ZIP, the text file is named like 'itcont.txt' (indiv),
    'itpas2.txt' (pas2), 'itoth.txt' (oth), 'cn.txt' (candidates),
    'cm.txt' (committees), 'ccl.txt' (linkages).
    """
    suffix = cycle_to_suffix(cycle)

    # Known inner file name patterns
    inner_names = {
        "cn": f"cn.txt",
        "cm": f"cm.txt",
        "ccl": f"ccl.txt",
        "pas2": f"itpas2.txt",
        "indiv": f"itcont.txt",
        "oth": f"itoth.txt",
    }

    expected_name = inner_names.get(file_prefix, f"{file_prefix}.txt")

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            # Find the data file inside the zip
            names = zf.namelist()
            target = None

            # First try exact match
            if expected_name in names:
                target = expected_name
            else:
                # Try case-insensitive and partial matching
                for name in names:
                    if name.lower() == expected_name.lower():
                        target = name
                        break
                if target is None:
                    # Look for any .txt file
                    txt_files = [n for n in names if n.lower().endswith(".txt")]
                    if len(txt_files) == 1:
                        target = txt_files[0]
                    elif txt_files:
                        # Pick the largest one
                        target = max(txt_files, key=lambda n: zf.getinfo(n).file_size)

            if target is None:
                log.error(f"  No data file found in {zip_path}. Contents: {names}")
                return

            log.info(f"  Reading {target} from {zip_path.name}...")

            # Stream the file line by line
            with zf.open(target) as f:
                text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                for line in text_stream:
                    line = line.rstrip("\n\r")
                    if not line:
                        continue
                    fields = line.split("|")
                    yield fields

    except zipfile.BadZipFile:
        log.error(f"  Bad ZIP file: {zip_path}")
    except Exception as e:
        log.error(f"  Error reading {zip_path}: {e}")


def load_file_to_db(db: sqlite3.Connection, file_prefix: str, cycle: int) -> int:
    """
    Load a single FEC bulk data file into the database.

    Returns number of rows inserted.
    """
    suffix = cycle_to_suffix(cycle)
    zip_path = BULK_DIR / str(cycle) / f"{file_prefix}{suffix}.zip"

    if not zip_path.exists():
        log.warning(f"  ZIP not found: {zip_path}")
        return 0

    mapping = TABLE_MAP[file_prefix]
    table = mapping["table"]
    insert_cols = mapping["insert_cols"]
    source_indices = mapping["source_indices"]

    placeholders = ", ".join(["?"] * len(insert_cols))
    col_list = ", ".join(insert_cols)
    insert_sql = f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"

    row_count = 0
    batch = []
    batch_size = 10000  # Insert in batches for performance

    for fields in stream_zip_csv(zip_path, file_prefix, cycle):
        if _shutdown:
            break

        try:
            row = parse_row(fields, source_indices, cycle)
            batch.append(row)
        except Exception as e:
            log.debug(f"  Skipping malformed row: {e}")
            continue

        if len(batch) >= batch_size:
            try:
                db.executemany(insert_sql, batch)
                db.commit()
                row_count += len(batch)
                if row_count % 500000 == 0:
                    log.info(f"    {table}: {row_count:,} rows loaded...")
            except sqlite3.Error as e:
                log.error(f"  DB error on batch insert into {table}: {e}")
                # Try row-by-row for this batch
                for single_row in batch:
                    try:
                        db.execute(insert_sql, single_row)
                    except sqlite3.Error:
                        pass
                db.commit()
                row_count += len(batch)
            batch = []

    # Final batch
    if batch:
        try:
            db.executemany(insert_sql, batch)
            db.commit()
            row_count += len(batch)
        except sqlite3.Error as e:
            log.error(f"  DB error on final batch insert into {table}: {e}")
            for single_row in batch:
                try:
                    db.execute(insert_sql, single_row)
                except sqlite3.Error:
                    pass
            db.commit()
            row_count += len(batch)

    return row_count


def delete_cycle_data(db: sqlite3.Connection, cycle: int):
    """Delete all data for a given cycle from all tables (for clean reload)."""
    tables = [
        "fec_candidates", "fec_committees", "fec_candidate_committee_linkages",
        "fec_contributions_to_candidates", "fec_individual_contributions",
        "fec_committee_transactions",
    ]
    for table in tables:
        try:
            db.execute(f"DELETE FROM {table} WHERE cycle = ?", (cycle,))
        except sqlite3.OperationalError:
            pass  # Table may not exist yet
    db.commit()


def build_database(cycles: list[int]):
    """Build the SQLite database from downloaded bulk files."""
    log.info(f"=== Building FEC Database ===")
    log.info(f"Database: {DB_PATH}")
    log.info(f"Cycles: {cycles}")

    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-512000")  # 512MB cache
    db.execute("PRAGMA temp_store=MEMORY")

    # Create tables
    log.info("Creating tables...")
    db.executescript(CREATE_TABLES_SQL)

    # Load order: candidates and committees first (reference data),
    # then linkages, then transaction tables
    load_order = [p for p in ["cn", "cm", "ccl", "pas2", "oth", "indiv"] if p in BULK_FILES]

    for cycle in cycles:
        if _shutdown:
            break
        log.info(f"\n--- Loading cycle {cycle} ---")

        # Delete existing data for this cycle (clean reload)
        log.info(f"  Clearing existing data for cycle {cycle}...")
        delete_cycle_data(db, cycle)

        for prefix in load_order:
            if _shutdown:
                break
            info = BULK_FILES[prefix]
            log.info(f"  Loading {info['description']} ({prefix})...")

            t0 = time.time()
            count = load_file_to_db(db, prefix, cycle)
            elapsed = time.time() - t0

            if count > 0:
                log.info(f"  [OK] {info['description']}: {count:,} rows in {elapsed:.1f}s")
            else:
                log.info(f"  [SKIP] {info['description']}: no data")

    # Create indexes (after all data is loaded, for speed)
    log.info("\nCreating indexes (this may take a while for large tables)...")
    t0 = time.time()
    for statement in CREATE_INDEXES_SQL.strip().split(";"):
        statement = statement.strip()
        if statement and not statement.startswith("--"):
            try:
                db.execute(statement)
                db.commit()
            except sqlite3.Error as e:
                log.error(f"  Index error: {e}")
    elapsed = time.time() - t0
    log.info(f"  Indexes created in {elapsed:.1f}s")

    # Summary
    log.info("\n=== Database Summary ===")
    tables = [
        "fec_candidates", "fec_committees", "fec_candidate_committee_linkages",
        "fec_contributions_to_candidates", "fec_individual_contributions",
        "fec_committee_transactions", "fec_candidate_crosswalk",
    ]
    for table in tables:
        try:
            count = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(f"  {table}: {count:,} rows")
        except sqlite3.OperationalError:
            log.info(f"  {table}: (not yet created)")

    db.close()
    log.info(f"\nDatabase size: {DB_PATH.stat().st_size / (1024**3):.2f} GB")


# ── Bioguide Crosswalk ───────────────────────────────────────────────────────

def build_crosswalk(session):
    """
    Build the FEC candidate ID -> bioguide ID crosswalk.

    Downloads congress-legislators JSON data and extracts FEC IDs from the
    id section of each legislator entry. Each legislator can have multiple
    FEC IDs (one per campaign).
    """
    log.info("=== Building Bioguide Crosswalk ===")

    crosswalk = []  # List of (fec_id, bioguide_id, full_name, first, last, party, state, chamber)

    for url, label in [
        (LEGISLATORS_CURRENT_URL, "current"),
        (LEGISLATORS_HISTORICAL_URL, "historical"),
    ]:
        log.info(f"  Downloading {label} legislators...")
        try:
            resp = session.get(url, timeout=120)
            resp.raise_for_status()
            legislators = resp.json()
            log.info(f"  {label}: {len(legislators)} legislators")
        except Exception as e:
            log.error(f"  Failed to download {label} legislators: {e}")
            continue

        for leg in legislators:
            ids = leg.get("id", {})
            bioguide = ids.get("bioguide")
            fec_ids = ids.get("fec", [])

            if not bioguide or not fec_ids:
                continue

            name = leg.get("name", {})
            first_name = name.get("first", "")
            last_name = name.get("last", "")
            full_name = name.get("official_full", "") or f"{first_name} {last_name}"

            # Get most recent term info
            terms = leg.get("terms", [])
            latest = terms[-1] if terms else {}
            party = latest.get("party", "")
            state = latest.get("state", "")
            term_type = latest.get("type", "")
            chamber = "Senate" if term_type == "sen" else "House"

            for fec_id in fec_ids:
                crosswalk.append((
                    fec_id, bioguide, full_name, first_name,
                    last_name, party, state, chamber,
                ))

    log.info(f"  Total crosswalk entries: {len(crosswalk)}")

    # Insert into database
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")

    # Ensure table exists
    db.executescript(CREATE_TABLES_SQL)

    # Clear existing crosswalk
    db.execute("DELETE FROM fec_candidate_crosswalk")

    insert_sql = """
        INSERT OR IGNORE INTO fec_candidate_crosswalk
        (fec_candidate_id, bioguide_id, full_name, first_name, last_name, party, state, chamber)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    db.executemany(insert_sql, crosswalk)
    db.commit()

    count = db.execute("SELECT COUNT(*) FROM fec_candidate_crosswalk").fetchone()[0]
    log.info(f"  Crosswalk table: {count} rows")

    # Create indexes
    db.execute("CREATE INDEX IF NOT EXISTS idx_fec_xwalk_bioguide ON fec_candidate_crosswalk(bioguide_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_fec_xwalk_fec ON fec_candidate_crosswalk(fec_candidate_id)")
    db.commit()
    db.close()

    log.info("  Crosswalk build complete")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download FEC campaign finance bulk data and build SQLite database"
    )
    parser.add_argument(
        "--cycles", nargs="+", type=int, default=None,
        help=f"Election cycles to process (default: {ALL_CYCLES})"
    )
    parser.add_argument(
        "--download-only", action="store_true",
        help="Only download bulk files, don't build database"
    )
    parser.add_argument(
        "--build-only", action="store_true",
        help="Only build database from existing files"
    )
    parser.add_argument(
        "--crosswalk-only", action="store_true",
        help="Only build the bioguide crosswalk"
    )
    parser.add_argument(
        "--skip-indiv", action="store_true",
        help="Skip individual contributions (the largest dataset)"
    )
    args = parser.parse_args()

    cycles = args.cycles or ALL_CYCLES
    session = create_session()

    # If --skip-indiv, remove 'indiv' from BULK_FILES
    if args.skip_indiv:
        log.info("Skipping individual contributions (--skip-indiv)")
        BULK_FILES.pop("indiv", None)
        TABLE_MAP.pop("indiv", None)

    t_start = time.time()

    if args.crosswalk_only:
        build_crosswalk(session)
    elif args.download_only:
        download_all(session, cycles)
    elif args.build_only:
        build_database(cycles)
        build_crosswalk(session)
    else:
        # Full run: download, then build DB
        download_all(session, cycles)
        if not _shutdown:
            build_database(cycles)
        if not _shutdown:
            build_crosswalk(session)

    elapsed = time.time() - t_start
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    log.info(f"\n=== Done in {hours}h {minutes}m ===")


if __name__ == "__main__":
    main()
