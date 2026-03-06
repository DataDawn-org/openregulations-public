#!/usr/bin/env python3
"""
Build FEC employer aggregate tables from the raw fec.db.

Produces fec_employers.db with NO individual names or personal info —
just employer-level rollups of campaign contributions.

Tables created:
  fec_employer_totals        — overall giving by employer (10+ donations)
  fec_employer_to_candidate  — employer × candidate × cycle (5+ donations), with bioguide_id
  fec_employer_to_party      — employer × party × cycle
  fec_top_occupations        — occupation breakdown per employer

Usage:
    python3 09_fec_employer_aggregates.py
"""

import re
import sqlite3
import time
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
FEC_DB = PROJECT_DIR / "fec.db"
OUT_DB = PROJECT_DIR / "fec_employers.db"

# Employers to exclude (self-reported non-employers / junk)
EXCLUDE_EMPLOYERS = {
    'NONE', 'N/A', 'NA', 'RETIRED', 'SELF-EMPLOYED', 'SELF EMPLOYED', 'SELF',
    'NOT EMPLOYED', 'NOT-EMPLOYED', 'UNEMPLOYED', 'HOMEMAKER', 'STUDENT',
    'INFORMATION REQUESTED', 'INFORMATION REQUESTED PER BEST EFFORTS',
    'REQUESTED', 'REFUSED', 'DISABLED', 'ENTREPRENEUR',
    'CORPORATION', 'COMPANY', 'BUSINESS', 'EMPLOYER', 'PRIVATE',
    'VARIOUS', 'MULTIPLE', 'OTHER', 'SAME', 'SAME AS ABOVE',
}

MIN_DONATIONS_EMPLOYER = 10   # min donations for employer_totals
MIN_DONATIONS_CANDIDATE = 5   # min donations for employer_to_candidate

# ── Employer name normalization ──────────────────────────────────────────
# Strips suffixes and punctuation variants so "FAHR LLC" == "FAHR, LLC"
_SUFFIX_PATTERN = re.compile(
    r',?\s*\b(LLC|L\.L\.C\.|INC|INC\.|INCORPORATED|CORP|CORP\.|CORPORATION|'
    r'CO|CO\.|COMPANY|LTD|LTD\.|LIMITED|LP|L\.P\.|LLP|L\.L\.P\.|'
    r'PLC|P\.L\.C\.|NA|N\.A\.|PC|P\.C\.|PLLC|PA|P\.A\.)\s*\.?\s*$',
    re.IGNORECASE
)
_CLEAN_PATTERN = re.compile(r'[.,\s]+$')


def normalize_employer(name):
    """Normalize employer name: uppercase, strip suffixes, collapse whitespace."""
    n = name.upper().strip()
    # Strip common suffixes (may need multiple passes: "FAHR, LLC." → "FAHR")
    for _ in range(2):
        n = _SUFFIX_PATTERN.sub('', n)
    n = _CLEAN_PATTERN.sub('', n)
    # Collapse internal whitespace
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    if not FEC_DB.exists():
        print(f"ERROR: {FEC_DB} not found. Run 08_fec_campaign_finance.py first.")
        sys.exit(1)

    log(f"Source: {FEC_DB} ({FEC_DB.stat().st_size / 1e9:.1f} GB)")

    # Open source DB read-only
    src = sqlite3.connect(f"file:{FEC_DB}?mode=ro", uri=True)
    src.execute("PRAGMA cache_size = -2000000")  # 2GB cache

    # Register the normalization function so SQLite can use it
    src.create_function("normalize_employer", 1, normalize_employer)

    # Load bioguide crosswalk into memory
    crosswalk = {}  # fec_candidate_id → bioguide_id
    for row in src.execute("SELECT fec_candidate_id, bioguide_id FROM fec_candidate_crosswalk"):
        crosswalk[row[0]] = row[1]
    log(f"Loaded {len(crosswalk):,} FEC→bioguide mappings")

    # Create output DB
    if OUT_DB.exists():
        OUT_DB.unlink()
    out = sqlite3.connect(str(OUT_DB))
    out.execute("PRAGMA journal_mode = WAL")
    out.execute("PRAGMA synchronous = NORMAL")
    out.execute("PRAGMA cache_size = -500000")

    # ── Step 1: Create indexes on source if missing ──────────────────────
    log("Checking indexes on fec_individual_contributions...")
    existing = {r[1] for r in src.execute("PRAGMA index_list(fec_individual_contributions)").fetchall()}

    needed_indexes = {
        "idx_indiv_employer": "CREATE INDEX IF NOT EXISTS idx_indiv_employer ON fec_individual_contributions(UPPER(employer))",
        "idx_indiv_cmte_cycle": "CREATE INDEX IF NOT EXISTS idx_indiv_cmte_cycle ON fec_individual_contributions(cmte_id, cycle)",
    }

    missing = [k for k in needed_indexes if k not in existing]
    if missing:
        src.close()
        log(f"Creating {len(missing)} indexes (this may take a while on 104M rows)...")
        src_rw = sqlite3.connect(str(FEC_DB))
        src_rw.execute("PRAGMA journal_mode = WAL")
        src_rw.execute("PRAGMA cache_size = -2000000")
        for name in missing:
            t0 = time.time()
            log(f"  Creating {name}...")
            src_rw.execute(needed_indexes[name])
            src_rw.commit()
            log(f"  {name} done ({time.time()-t0:.0f}s)")
        src_rw.close()
        src = sqlite3.connect(f"file:{FEC_DB}?mode=ro", uri=True)
        src.execute("PRAGMA cache_size = -2000000")
        src.create_function("normalize_employer", 1, normalize_employer)
    else:
        log("Indexes already exist")

    # ── Step 2: Build employer totals (with normalization) ───────────────
    log("Building fec_employer_totals (with employer name normalization)...")
    out.execute("""
        CREATE TABLE fec_employer_totals (
            employer TEXT PRIMARY KEY,
            donation_count INTEGER,
            total_amount INTEGER,
            avg_amount INTEGER,
            unique_states INTEGER,
            min_cycle INTEGER,
            max_cycle INTEGER
        )
    """)

    exclude_sql = ",".join(f"'{e}'" for e in EXCLUDE_EMPLOYERS)
    t0 = time.time()

    # First pass: get raw employer → normalized mapping via SQL + Python
    log("  Pass 1: Aggregating with normalized employer names...")
    rows = src.execute(f"""
        SELECT
            normalize_employer(employer) AS norm_emp,
            COUNT(*) AS donation_count,
            CAST(SUM(transaction_amt) AS INTEGER) AS total_amount,
            CAST(AVG(transaction_amt) AS INTEGER) AS avg_amount,
            COUNT(DISTINCT state) AS unique_states,
            MIN(cycle) AS min_cycle,
            MAX(cycle) AS max_cycle
        FROM fec_individual_contributions
        WHERE employer IS NOT NULL AND employer != ''
          AND UPPER(employer) NOT IN ({exclude_sql})
        GROUP BY normalize_employer(employer)
        HAVING COUNT(*) >= {MIN_DONATIONS_EMPLOYER}
        ORDER BY total_amount DESC
    """).fetchall()

    # Filter out any that normalized to empty or to an excluded name
    clean_rows = []
    for r in rows:
        emp = r[0]
        if emp and emp not in EXCLUDE_EMPLOYERS and len(emp) > 1:
            clean_rows.append(r)

    out.executemany(
        "INSERT OR IGNORE INTO fec_employer_totals VALUES (?,?,?,?,?,?,?)", clean_rows
    )
    out.commit()
    log(f"  {len(clean_rows):,} employers ({time.time()-t0:.0f}s)")

    # ── Step 3: Build employer → candidate (with bioguide_id) ────────────
    log("Building fec_employer_to_candidate (with bioguide_id linkage)...")
    out.execute("""
        CREATE TABLE fec_employer_to_candidate (
            employer TEXT,
            cand_id TEXT,
            bioguide_id TEXT,
            cand_name TEXT,
            party TEXT,
            office TEXT,
            state TEXT,
            cycle INTEGER,
            donation_count INTEGER,
            total_amount INTEGER
        )
    """)

    t0 = time.time()
    rows = src.execute(f"""
        SELECT
            normalize_employer(i.employer) AS employer,
            l.cand_id,
            c.cand_name,
            c.cand_pty_affiliation AS party,
            c.cand_office AS office,
            c.cand_office_st AS state,
            i.cycle,
            COUNT(*) AS donation_count,
            CAST(SUM(i.transaction_amt) AS INTEGER) AS total_amount
        FROM fec_individual_contributions i
        JOIN fec_candidate_committee_linkages l
            ON i.cmte_id = l.cmte_id AND i.cycle = l.cycle
        JOIN fec_candidates c
            ON l.cand_id = c.cand_id AND l.cycle = c.cycle
        WHERE i.employer IS NOT NULL AND i.employer != ''
          AND UPPER(i.employer) NOT IN ({exclude_sql})
        GROUP BY normalize_employer(i.employer), l.cand_id, i.cycle
        HAVING COUNT(*) >= {MIN_DONATIONS_CANDIDATE}
        ORDER BY total_amount DESC
    """).fetchall()

    # Add bioguide_id from crosswalk
    enriched = []
    for r in rows:
        emp, cand_id, cand_name, party, office, state, cycle, cnt, total = r
        if emp and emp not in EXCLUDE_EMPLOYERS and len(emp) > 1:
            bio_id = crosswalk.get(cand_id)
            enriched.append((emp, cand_id, bio_id, cand_name, party, office, state, cycle, cnt, total))

    out.executemany(
        "INSERT INTO fec_employer_to_candidate VALUES (?,?,?,?,?,?,?,?,?,?)", enriched
    )
    out.commit()
    log(f"  {len(enriched):,} employer-candidate-cycle combos ({time.time()-t0:.0f}s)")

    bio_linked = sum(1 for r in enriched if r[2] is not None)
    log(f"  {bio_linked:,} linked to bioguide_id ({bio_linked*100/len(enriched):.1f}%)")

    # ── Step 4: Build employer → party ───────────────────────────────────
    log("Building fec_employer_to_party...")
    out.execute("""
        CREATE TABLE fec_employer_to_party (
            employer TEXT,
            party TEXT,
            cycle INTEGER,
            donation_count INTEGER,
            total_amount INTEGER,
            candidate_count INTEGER
        )
    """)

    t0 = time.time()
    rows = out.execute("""
        SELECT employer, party, cycle,
               SUM(donation_count) AS donation_count,
               SUM(total_amount) AS total_amount,
               COUNT(DISTINCT cand_id) AS candidate_count
        FROM fec_employer_to_candidate
        GROUP BY employer, party, cycle
        ORDER BY total_amount DESC
    """).fetchall()

    out.executemany(
        "INSERT INTO fec_employer_to_party VALUES (?,?,?,?,?,?)", rows
    )
    out.commit()
    log(f"  {len(rows):,} employer-party-cycle combos ({time.time()-t0:.0f}s)")

    # ── Step 5: Top occupations per employer ─────────────────────────────
    log("Building fec_top_occupations...")
    out.execute("""
        CREATE TABLE fec_top_occupations (
            employer TEXT,
            occupation TEXT,
            donation_count INTEGER,
            total_amount INTEGER
        )
    """)

    t0 = time.time()
    rows = src.execute(f"""
        SELECT
            normalize_employer(employer) AS employer,
            UPPER(occupation) AS occupation,
            COUNT(*) AS donation_count,
            CAST(SUM(transaction_amt) AS INTEGER) AS total_amount
        FROM fec_individual_contributions
        WHERE employer IS NOT NULL AND employer != ''
          AND occupation IS NOT NULL AND occupation != ''
          AND UPPER(employer) NOT IN ({exclude_sql})
        GROUP BY normalize_employer(employer), UPPER(occupation)
        HAVING COUNT(*) >= 10
        ORDER BY employer, donation_count DESC
    """).fetchall()

    clean_occ = [(r[0], r[1], r[2], r[3]) for r in rows
                 if r[0] and r[0] not in EXCLUDE_EMPLOYERS and len(r[0]) > 1]

    out.executemany(
        "INSERT INTO fec_top_occupations VALUES (?,?,?,?)", clean_occ
    )
    out.commit()
    log(f"  {len(clean_occ):,} employer-occupation combos ({time.time()-t0:.0f}s)")

    # ── Step 6: Create indexes ───────────────────────────────────────────
    log("Creating indexes...")
    out.execute("CREATE INDEX idx_emp_cand_employer ON fec_employer_to_candidate(employer)")
    out.execute("CREATE INDEX idx_emp_cand_cand ON fec_employer_to_candidate(cand_id)")
    out.execute("CREATE INDEX idx_emp_cand_bioguide ON fec_employer_to_candidate(bioguide_id)")
    out.execute("CREATE INDEX idx_emp_party_employer ON fec_employer_to_party(employer)")
    out.execute("CREATE INDEX idx_emp_occ_employer ON fec_top_occupations(employer)")

    # FTS on employer names
    out.execute("""
        CREATE VIRTUAL TABLE fec_employer_totals_fts USING fts5(
            employer,
            content='fec_employer_totals',
            content_rowid='rowid'
        )
    """)
    out.execute("INSERT INTO fec_employer_totals_fts(fec_employer_totals_fts) VALUES('rebuild')")
    out.commit()

    # ── Summary ──────────────────────────────────────────────────────────
    log("=== Summary ===")
    for table in ['fec_employer_totals', 'fec_employer_to_candidate', 'fec_employer_to_party', 'fec_top_occupations']:
        ct = out.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log(f"  {table}: {ct:,}")

    # Bioguide linkage stats
    total_cands = out.execute("SELECT COUNT(DISTINCT cand_id) FROM fec_employer_to_candidate").fetchone()[0]
    bio_cands = out.execute("SELECT COUNT(DISTINCT cand_id) FROM fec_employer_to_candidate WHERE bioguide_id IS NOT NULL").fetchone()[0]
    log(f"  Candidates with bioguide_id: {bio_cands}/{total_cands} ({bio_cands*100/total_cands:.1f}%)")

    db_size = OUT_DB.stat().st_size / 1e6
    log(f"Output: {OUT_DB} ({db_size:.0f} MB)")
    log("Done — zero individual names or personal data in output.")

    src.close()
    out.close()


if __name__ == "__main__":
    main()
