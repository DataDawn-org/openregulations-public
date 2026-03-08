#!/usr/bin/env python3
"""Resume the 05_build_database.py build from where it crashed.

Last successful step: spending_agency_summary
Remaining: 7 summary tables, FTS indexes, stats, build report.
"""

import sqlite3
import logging
import time
import json
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "openregs.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(BASE_DIR / "logs" / "resume_build_20260308.log"),
    ],
)
log = logging.getLogger(__name__)


def build_remaining_summary_tables(conn):
    """Build the 7 summary tables that were not completed."""

    # Pre-compute normalized names for fast joins
    log.info("Pre-computing normalized lobby client names...")
    conn.execute("DROP TABLE IF EXISTS _tmp_lobby_clients")
    conn.execute("""
        CREATE TEMP TABLE _tmp_lobby_clients AS
        SELECT UPPER(TRIM(client_name)) AS norm_name,
            COUNT(DISTINCT filing_uuid) AS lobby_filings,
            SUM(income_amount) AS total_lobby_income,
            GROUP_CONCAT(DISTINCT issue_code) AS lobby_issues
        FROM lobbying_activities
        WHERE client_name IS NOT NULL AND TRIM(client_name) != ''
        GROUP BY UPPER(TRIM(client_name))
    """)
    conn.execute("CREATE INDEX _tmp_lc_idx ON _tmp_lobby_clients(norm_name)")
    log.info(f"  Lobby client lookup: {conn.execute('SELECT COUNT(*) FROM _tmp_lobby_clients').fetchone()[0]:,} unique names")

    # 1. witness_lobby_overlap (optimized: join on indexed temp table)
    log.info("Building witness_lobby_overlap...")
    conn.execute("DROP TABLE IF EXISTS witness_lobby_overlap")
    conn.execute("""
        CREATE TABLE witness_lobby_overlap AS
        SELECT
            hw.organization,
            COUNT(DISTINCT hw.package_id) AS hearings_testified,
            lc.lobby_filings,
            lc.total_lobby_income,
            MIN(h.date_issued) AS first_hearing,
            MAX(h.date_issued) AS last_hearing,
            lc.lobby_issues
        FROM hearing_witnesses hw
        JOIN hearings h ON hw.package_id = h.package_id
        JOIN _tmp_lobby_clients lc ON UPPER(TRIM(hw.organization)) = lc.norm_name
        WHERE hw.organization IS NOT NULL AND hw.organization != ''
        GROUP BY UPPER(TRIM(hw.organization))
        ORDER BY hearings_testified DESC
    """)
    conn.execute("CREATE INDEX idx_wlo_org ON witness_lobby_overlap(organization)")
    conn.execute("CREATE INDEX idx_wlo_hearings ON witness_lobby_overlap(hearings_testified)")
    conn.commit()
    log.info(f"  witness_lobby_overlap: {conn.execute('SELECT COUNT(*) FROM witness_lobby_overlap').fetchone()[0]:,} rows")

    # 2. commenter_lobby_overlap (optimized: join on indexed temp table)
    log.info("Building commenter_lobby_overlap...")
    conn.execute("DROP TABLE IF EXISTS commenter_lobby_overlap")
    conn.execute("""
        CREATE TABLE commenter_lobby_overlap AS
        SELECT
            cd.organization,
            COUNT(DISTINCT cd.id) AS comments_filed,
            COUNT(DISTINCT cd.gov_agency) AS agencies_commented,
            lc.lobby_filings,
            lc.total_lobby_income,
            lc.lobby_issues
        FROM comment_details cd
        JOIN _tmp_lobby_clients lc ON UPPER(TRIM(cd.organization)) = lc.norm_name
        WHERE cd.organization IS NOT NULL AND cd.organization != ''
        GROUP BY UPPER(TRIM(cd.organization))
        ORDER BY comments_filed DESC
    """)
    conn.execute("CREATE INDEX idx_clo_org ON commenter_lobby_overlap(organization)")
    conn.execute("CREATE INDEX idx_clo_comments ON commenter_lobby_overlap(comments_filed)")
    conn.commit()
    log.info(f"  commenter_lobby_overlap: {conn.execute('SELECT COUNT(*) FROM commenter_lobby_overlap').fetchone()[0]:,} rows")

    # 3. lobbying_bill_summary
    log.info("Building lobbying_bill_summary...")
    conn.execute("DROP TABLE IF EXISTS lobbying_bill_summary")
    conn.execute("""
        CREATE TABLE lobbying_bill_summary AS
        SELECT lb.bill_id, l.title AS bill_title, l.policy_area,
            COUNT(DISTINCT lb.filing_uuid) AS lobby_filings,
            COUNT(DISTINCT lb.client_name) AS unique_clients,
            GROUP_CONCAT(DISTINCT lb.issue_code) AS issue_codes,
            GROUP_CONCAT(DISTINCT lb.client_name) AS clients
        FROM lobbying_bills lb
        LEFT JOIN legislation l ON lb.bill_id = l.bill_id
        WHERE lb.bill_id IS NOT NULL
        GROUP BY lb.bill_id
        ORDER BY lobby_filings DESC
    """)
    conn.execute("CREATE UNIQUE INDEX idx_lbs_bill ON lobbying_bill_summary(bill_id)")
    conn.commit()
    log.info(f"  lobbying_bill_summary: {conn.execute('SELECT COUNT(*) FROM lobbying_bill_summary').fetchone()[0]:,} rows")

    # 4. speeches_near_trades
    log.info("Building speeches_near_trades...")
    conn.execute("DROP TABLE IF EXISTS speeches_near_trades")
    conn.execute("""
        CREATE TABLE speeches_near_trades AS
        SELECT cm.full_name, cm.party, cm.state, cm.bioguide_id,
            st.transaction_date AS trade_date, st.ticker, st.asset_description,
            st.transaction_type, st.amount_range,
            cr.date AS speech_date, cr.title AS speech_title,
            ABS(CAST(julianday(st.transaction_date) - julianday(cr.date) AS INTEGER)) AS days_apart
        FROM stock_trades st
        JOIN crec_speakers cs ON st.bioguide_id = cs.bioguide_id
        JOIN congressional_record cr ON cs.granule_id = cr.granule_id
        JOIN congress_members cm ON st.bioguide_id = cm.bioguide_id
        WHERE st.ticker IS NOT NULL AND st.ticker != ''
            AND st.transaction_date IS NOT NULL AND cr.date IS NOT NULL
            AND ABS(julianday(st.transaction_date) - julianday(cr.date)) <= 7
        ORDER BY cm.full_name, st.transaction_date DESC
    """)
    conn.execute("CREATE INDEX idx_snt_member ON speeches_near_trades(bioguide_id)")
    conn.execute("CREATE INDEX idx_snt_ticker ON speeches_near_trades(ticker)")
    conn.commit()
    log.info(f"  speeches_near_trades: {conn.execute('SELECT COUNT(*) FROM speeches_near_trades').fetchone()[0]:,} rows")

    # 5. committee_donor_summary
    log.info("Building committee_donor_summary...")
    conn.execute("DROP TABLE IF EXISTS committee_donor_summary")
    conn.execute("""
        CREATE TABLE committee_donor_summary AS
        SELECT c.name AS committee_name, cm.full_name AS member_name,
            cm.party, cm.state, cm.bioguide_id,
            fc.cmte_nm AS donor_committee, fc.cmte_id AS donor_cmte_id,
            SUM(fcon.transaction_amt) AS total_donated,
            COUNT(*) AS contribution_count
        FROM committee_memberships cmem
        JOIN committees c ON cmem.committee_id = c.committee_id
        JOIN congress_members cm ON cmem.bioguide_id = cm.bioguide_id
        JOIN fec_candidate_crosswalk xw ON xw.bioguide_id = cm.bioguide_id
        JOIN fec_contributions fcon ON fcon.cand_id = xw.fec_candidate_id
        JOIN fec_committees fc ON fc.cmte_id = fcon.cmte_id
        WHERE cm.is_current = 1
        GROUP BY cmem.committee_id, cmem.bioguide_id, fcon.cmte_id
        HAVING total_donated >= 10000
        ORDER BY total_donated DESC
    """)
    conn.execute("CREATE INDEX idx_cds_member ON committee_donor_summary(bioguide_id)")
    conn.execute("CREATE INDEX idx_cds_committee ON committee_donor_summary(committee_name)")
    conn.commit()
    log.info(f"  committee_donor_summary: {conn.execute('SELECT COUNT(*) FROM committee_donor_summary').fetchone()[0]:,} rows")

    # 6. committee_trade_conflicts
    log.info("Building committee_trade_conflicts...")
    conn.execute("DROP TABLE IF EXISTS committee_trade_conflicts")
    try:
        has_sic = conn.execute("SELECT COUNT(*) FROM ticker_sic").fetchone()[0] > 0
        has_ranges = conn.execute("SELECT COUNT(*) FROM committee_sic_ranges").fetchone()[0] > 0
    except Exception:
        has_sic = False
        has_ranges = False

    try:
        if has_sic and has_ranges:
            log.info("  Using SIC-range matching...")
            conn.execute("""
                CREATE TABLE committee_trade_conflicts AS
                SELECT cm.full_name, cm.party, cm.state, cm.bioguide_id,
                    cj.committee_name, cj.jurisdiction_desc, cj.jurisdiction_tier,
                    ts.sic_code, ts.sic_description,
                    st.transaction_date, st.ticker, st.asset_description,
                    st.transaction_type, st.amount_range
                FROM committee_memberships cmem
                JOIN congress_members cm ON cmem.bioguide_id = cm.bioguide_id
                JOIN committee_sic_ranges csr ON cmem.committee_id = csr.committee_id
                JOIN committee_jurisdiction cj ON cmem.committee_id = cj.committee_id
                    AND cj.jurisdiction_tier = csr.jurisdiction_tier
                JOIN stock_trades st ON cmem.bioguide_id = st.bioguide_id
                JOIN ticker_sic ts ON UPPER(st.ticker) = ts.ticker
                WHERE CAST(ts.sic_code AS INTEGER) BETWEEN csr.sic_start AND csr.sic_end
                ORDER BY cm.full_name, st.transaction_date DESC
            """)
        else:
            log.info("  Using ticker matching (no SIC data)...")
            conn.execute("""
                CREATE TABLE committee_trade_conflicts AS
                SELECT cm.full_name, cm.party, cm.state, cm.bioguide_id,
                    cs.committee_name, cs.regulated_sectors AS jurisdiction_desc,
                    'primary' AS jurisdiction_tier,
                    '' AS sic_code, '' AS sic_description,
                    st.transaction_date, st.ticker, st.asset_description,
                    st.transaction_type, st.amount_range
                FROM committee_memberships cmem
                JOIN congress_members cm ON cmem.bioguide_id = cm.bioguide_id
                JOIN committee_sectors cs ON cmem.committee_id = cs.committee_id
                JOIN stock_trades st ON cmem.bioguide_id = st.bioguide_id
                WHERE st.ticker IS NOT NULL AND st.ticker != ''
                    AND (',' || cs.example_tickers || ',') LIKE ('%,' || st.ticker || ',%')
                ORDER BY cm.full_name, st.transaction_date DESC
            """)
        conn.execute("CREATE INDEX idx_ctc_member ON committee_trade_conflicts(bioguide_id)")
        conn.execute("CREATE INDEX idx_ctc_ticker ON committee_trade_conflicts(ticker)")
        conn.execute("CREATE INDEX idx_ctc_committee ON committee_trade_conflicts(committee_name)")
        conn.commit()
        log.info(f"  committee_trade_conflicts: {conn.execute('SELECT COUNT(*) FROM committee_trade_conflicts').fetchone()[0]:,} rows")
    except Exception as e:
        log.warning(f"  committee_trade_conflicts: skipped ({e})")

    # 7. revolving_door
    log.info("Building revolving_door...")
    conn.execute("DROP TABLE IF EXISTS revolving_door")
    try:
        conn.execute("""
            CREATE TABLE revolving_door AS
            WITH position_filter AS (
                SELECT DISTINCT lobbyist_name, filing_uuid, covered_position
                FROM lobbying_lobbyists
                WHERE covered_position IS NOT NULL AND covered_position <> ''
                AND (
                    UPPER(covered_position) LIKE '%MEMBER OF CONGRESS%'
                    OR UPPER(covered_position) LIKE 'U.S. REPRESENTATIVE%'
                    OR UPPER(covered_position) LIKE 'U.S. SENATOR%'
                    OR UPPER(covered_position) LIKE 'US SENATOR%'
                    OR UPPER(covered_position) LIKE 'US REPRESENTATIVE%'
                    OR UPPER(covered_position) LIKE 'U.S. CONGRESSMAN%'
                    OR UPPER(covered_position) LIKE 'UNITED STATES REP%'
                    OR UPPER(covered_position) LIKE 'UNITED STATES SEN%'
                    OR UPPER(covered_position) LIKE 'FORMER MEMBER%'
                    OR UPPER(covered_position) LIKE 'MEMBER, U.S.%'
                    OR UPPER(covered_position) LIKE 'MEMBER U.S.%'
                    OR UPPER(covered_position) LIKE 'MEMBER OF THE U.S.%'
                )
            ),
            best_member AS (
                SELECT bioguide_id, full_name, party, state, chamber,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(full_name)
                        ORDER BY bioguide_id DESC
                    ) AS rn
                FROM congress_members
            )
            SELECT
                bm.bioguide_id,
                bm.full_name,
                bm.party,
                bm.state,
                bm.chamber AS congress_chamber,
                COUNT(DISTINCT la.filing_uuid) AS lobbying_filing_count,
                COUNT(DISTINCT la.client_name) AS client_count,
                COUNT(DISTINCT la.registrant_name) AS firm_count,
                MIN(la.filing_year) AS first_lobbying_year,
                MAX(la.filing_year) AS last_lobbying_year,
                SUM(COALESCE(la.income_amount, 0)) AS total_reported_income,
                GROUP_CONCAT(DISTINCT la.registrant_name) AS lobbying_firms,
                MIN(pf.covered_position) AS covered_position_sample
            FROM position_filter pf
            JOIN best_member bm ON UPPER(bm.full_name) = pf.lobbyist_name AND bm.rn = 1
            JOIN lobbying_activities la ON pf.filing_uuid = la.filing_uuid
            GROUP BY bm.bioguide_id
            ORDER BY lobbying_filing_count DESC
        """)
        conn.execute("CREATE INDEX idx_rd_bioguide ON revolving_door(bioguide_id)")
        conn.commit()
        log.info(f"  revolving_door: {conn.execute('SELECT COUNT(*) FROM revolving_door').fetchone()[0]:,} rows")
    except Exception as e:
        log.warning(f"  revolving_door: skipped ({e})")

    log.info("Summary tables complete.")


FTS_SCHEMA = """
-- Full-text search on Federal Register
CREATE VIRTUAL TABLE IF NOT EXISTS federal_register_fts USING fts5(
    title, abstract, agency_names, excerpts,
    content='federal_register', content_rowid='rowid'
);

-- Full-text search on dockets (standalone)
CREATE VIRTUAL TABLE IF NOT EXISTS dockets_fts USING fts5(
    title, agency_id, summary
);

-- Full-text search on documents
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    title, agency_id, document_type,
    content='documents', content_rowid='rowid'
);

-- Full-text search on comments
CREATE VIRTUAL TABLE IF NOT EXISTS comments_fts USING fts5(
    title, submitter_name, agency_id, submitter_type, docket_id,
    content='comments', content_rowid='rowid'
);

-- Full-text search on eCFR regulatory text
CREATE VIRTUAL TABLE IF NOT EXISTS cfr_fts USING fts5(
    section_number, section_heading, part_name, agency, full_text,
    content='cfr_sections', content_rowid='rowid'
);

-- Full-text search on Congressional Record
CREATE VIRTUAL TABLE IF NOT EXISTS crec_fts USING fts5(
    title, chamber, full_text,
    content='congressional_record', content_rowid='rowid'
);

-- Full-text search on lobbying activities
CREATE VIRTUAL TABLE IF NOT EXISTS lobbying_fts USING fts5(
    client_name, registrant_name, specific_issues, government_entities, issue_code,
    content='lobbying_activities', content_rowid='rowid'
);

-- Full-text search on spending awards
CREATE VIRTUAL TABLE IF NOT EXISTS spending_awards_fts USING fts5(
    recipient_name, agency, sub_agency,
    content='spending_awards', content_rowid='rowid'
);

-- Full-text search on legislation
CREATE VIRTUAL TABLE IF NOT EXISTS legislation_fts USING fts5(
    title, policy_area, bill_id,
    content='legislation', content_rowid='rowid'
);

CREATE VIRTUAL TABLE IF NOT EXISTS fara_registrants_fts USING fts5(
    name, business_name, city, state,
    content='fara_registrants', content_rowid='rowid'
);

CREATE VIRTUAL TABLE IF NOT EXISTS fara_foreign_principals_fts USING fts5(
    registrant_name, foreign_principal, country,
    content='fara_foreign_principals', content_rowid='rowid'
);

CREATE VIRTUAL TABLE IF NOT EXISTS fec_employer_fts USING fts5(
    employer,
    content='fec_employer_totals', content_rowid='rowid'
);

-- Full-text search on hearings
CREATE VIRTUAL TABLE IF NOT EXISTS hearings_fts USING fts5(
    title, chamber, committees,
    content='hearings', content_rowid='rowid'
);

-- Full-text search on CRS reports
CREATE VIRTUAL TABLE IF NOT EXISTS crs_reports_fts USING fts5(
    title, authors, topics, summary,
    content='crs_reports', content_rowid='rowid'
);

-- Full-text search on nominations
CREATE VIRTUAL TABLE IF NOT EXISTS nominations_fts USING fts5(
    description, organization, citation, status,
    content='nominations', content_rowid='rowid'
);

-- Full-text search on GAO reports
CREATE VIRTUAL TABLE IF NOT EXISTS gao_reports_fts USING fts5(
    title, abstract, subjects, report_number,
    content='gao_reports', content_rowid='rowid'
);

-- Full-text search on earmarks
CREATE VIRTUAL TABLE IF NOT EXISTS earmarks_fts USING fts5(
    recipient, project_description, member_name, recipient_address,
    content='earmarks', content_rowid='id'
);
"""


def build_fts(conn):
    """Build full-text search indexes."""
    log.info("Building FTS5 indexes...")
    conn.executescript(FTS_SCHEMA)

    for table in ["federal_register_fts", "documents_fts", "comments_fts",
                   "cfr_fts", "crec_fts", "lobbying_fts",
                   "spending_awards_fts", "legislation_fts",
                   "fara_registrants_fts", "fara_foreign_principals_fts",
                   "fec_employer_fts",
                   "hearings_fts", "crs_reports_fts", "nominations_fts",
                   "gao_reports_fts", "earmarks_fts"]:
        try:
            source_map = {
                'cfr_fts': 'cfr_sections', 'crec_fts': 'congressional_record',
                'lobbying_fts': 'lobbying_activities', 'spending_awards_fts': 'spending_awards',
                'legislation_fts': 'legislation', 'fara_registrants_fts': 'fara_registrants',
                'fara_foreign_principals_fts': 'fara_foreign_principals',
                'fec_employer_fts': 'fec_employer_totals', 'hearings_fts': 'hearings',
                'crs_reports_fts': 'crs_reports', 'nominations_fts': 'nominations',
                'gao_reports_fts': 'gao_reports', 'earmarks_fts': 'earmarks'
            }
            source_table = source_map.get(table, table.replace('_fts', ''))
            count = conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
            if count > 0:
                log.info(f"  Rebuilding {table}...")
                conn.execute(f"INSERT INTO {table}({table}) VALUES('rebuild')")
                conn.commit()
        except sqlite3.OperationalError as e:
            log.warning(f"  {table}: skipped ({e})")

    # dockets_fts is standalone — populate with JOIN to docket_summary
    log.info("  Populating dockets_fts with summaries...")
    conn.execute("""
        INSERT INTO dockets_fts(rowid, title, agency_id, summary)
        SELECT d.rowid, d.title, d.agency_id, COALESCE(ds.summary, '')
        FROM dockets d
        LEFT JOIN docket_summary ds ON ds.docket_id = d.id
    """)
    conn.commit()

    log.info("  FTS indexes built")


def print_stats(conn):
    """Print database statistics."""
    log.info("\n" + "=" * 50)
    log.info("DATABASE STATISTICS")
    log.info("=" * 50)

    tables = [
        ("federal_register", "Federal Register docs"),
        ("agencies", "Agencies"),
        ("dockets", "Dockets"),
        ("documents", "Documents"),
        ("comments", "Comment headers"),
        ("comment_details", "Comment details (full text)"),
        ("fr_regs_crossref", "FR <-> Regs.gov links"),
        ("presidential_documents", "Presidential documents"),
        ("spending_awards", "Spending awards"),
        ("legislation", "Legislation bills"),
        ("cfr_sections", "CFR sections"),
        ("congressional_record", "Congressional Record"),
        ("crec_speakers", "CREC speakers"),
        ("crec_bills", "CREC bill references"),
        ("congress_members", "Congress members"),
        ("stock_trades", "Stock trades"),
        ("lobbying_filings", "Lobbying filings"),
        ("lobbying_activities", "Lobbying activities"),
        ("lobbying_bills", "Lobbying bill references"),
        ("roll_call_votes", "Roll call votes"),
        ("member_votes", "Member votes"),
        ("fec_candidates", "FEC candidates"),
        ("fec_contributions", "FEC contributions"),
        ("hearings", "Committee hearings"),
        ("crs_reports", "CRS reports"),
        ("nominations", "Executive nominations"),
        ("treaties", "Treaties"),
        ("gao_reports", "GAO reports"),
        ("earmarks", "Earmarks/CDS"),
        ("witness_lobby_overlap", "Witness-lobbyist overlap"),
        ("commenter_lobby_overlap", "Commenter-lobbyist overlap"),
        ("lobbying_bill_summary", "Lobbying bill summary"),
        ("speeches_near_trades", "Speeches near trades"),
        ("committee_donor_summary", "Committee donor summary"),
        ("committee_trade_conflicts", "Committee trade conflicts"),
        ("revolving_door", "Revolving door"),
    ]

    for table, label in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(f"  {label}: {count:,}")
        except sqlite3.OperationalError:
            log.info(f"  {label}: (not populated)")

    size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    log.info(f"\nDatabase size: {size_mb:.1f} MB")


def save_build_report(conn, elapsed_seconds):
    """Save a build report."""
    report_dir = BASE_DIR / "build_reports"
    report_dir.mkdir(exist_ok=True)

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = now.strftime("%Y%m%d_%H%M%S")

    table_counts = {}
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ):
        table_name = row[0]
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
            table_counts[table_name] = count
        except Exception:
            table_counts[table_name] = -1

    db_size_bytes = DB_PATH.stat().st_size
    total_rows = sum(c for c in table_counts.values() if c > 0)

    report = {
        "timestamp": timestamp,
        "build_duration_seconds": round(elapsed_seconds, 1),
        "build_duration_minutes": round(elapsed_seconds / 60, 1),
        "database_size_bytes": db_size_bytes,
        "database_size_gb": round(db_size_bytes / (1024**3), 2),
        "total_tables": len(table_counts),
        "total_rows": total_rows,
        "tables": table_counts,
        "note": "Resumed build — summary tables + FTS only",
    }

    # Load previous report for deltas
    prev_reports = sorted(report_dir.glob("*.json"))
    prev_report = None
    if prev_reports:
        try:
            prev_report = json.loads(prev_reports[-1].read_text())
        except Exception:
            pass

    if prev_report and "tables" in prev_report:
        prev_tables = prev_report["tables"]
        deltas = {}
        for table, count in table_counts.items():
            if count < 0:
                continue
            prev_count = prev_tables.get(table, 0)
            if prev_count < 0:
                prev_count = 0
            delta = count - prev_count
            if delta != 0:
                deltas[table] = {"previous": prev_count, "current": count, "delta": delta}
        new_tables = [t for t in table_counts if t not in prev_tables and table_counts[t] > 0]
        removed_tables = [t for t in prev_tables if t not in table_counts]

        report["deltas"] = deltas
        report["new_tables"] = new_tables
        report["removed_tables"] = removed_tables
        report["previous_build"] = prev_report.get("timestamp", "unknown")

    json_path = report_dir / f"{filename}.json"
    json_path.write_text(json.dumps(report, indent=2))
    log.info(f"Build report saved: {json_path}")


def main():
    log.info("=" * 60)
    log.info("RESUMING OPENREGS BUILD — summary tables + FTS")
    log.info(f"Database: {DB_PATH}")
    log.info("=" * 60)

    start = time.time()
    conn = sqlite3.connect(str(DB_PATH), timeout=300)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-2000000")  # 2GB cache

    try:
        build_remaining_summary_tables(conn)
        build_fts(conn)
        print_stats(conn)
        elapsed = time.time() - start
        save_build_report(conn, elapsed)
    finally:
        conn.close()

    elapsed = time.time() - start
    log.info(f"\nResume complete in {elapsed/60:.1f} minutes")
    log.info(f"Database ready: {DB_PATH}")


if __name__ == "__main__":
    main()
