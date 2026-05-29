"""
2026-05-22 S-C1 lobbying spend inflation fix — openregs.db side.

Companion to 2026_05_22_lobbying_s_c1.py which migrated lobbying.db.

The build script (05_build_database.py) was updated with the new schema. This
migration brings openregs.db's existing tables into compatibility so a regular
incremental build can re-import lobbying data without schema conflict, and
rebuilds the affected precomputed tables.

Approach: DROP lobbying_filings + lobbying_activities + summary/derived tables.
Re-create with new schema and re-import from lobbying.db (now canonical). Then
run build_summary_tables + build_revolving_door + build_witness_lobby_overlap
+ build_commenter_lobby_overlap which derive from the re-imported tables.

Idempotent. Safe to re-run.
"""

import sqlite3
import sys
import time
from pathlib import Path


def run(openregs_path, lobbying_path):
    print(f"=== Lobbying S-C1 openregs migration ===")
    print(f"  openregs.db: {openregs_path}")
    print(f"  lobbying.db: {lobbying_path}")

    conn = sqlite3.connect(str(openregs_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")

    try:
        # Verify lobbying.db has the new schema
        sc = sqlite3.connect(str(lobbying_path))
        ldb_cols = {r[1] for r in sc.execute("PRAGMA table_info(lobbying_filings_raw)")}
        sc.close()
        assert "income_amount" in ldb_cols and "expense_amount" in ldb_cols, \
            "lobbying.db hasn't been migrated yet — run 2026_05_22_lobbying_s_c1.py first"
        assert "amount_reported" not in ldb_cols, \
            "lobbying.db still has amount_reported — run the migration"
        print("  Source lobbying.db schema: OK (income+expense present, amount_reported gone)")

        # ── Step 1: drop dependent precomputed tables ──
        for t in ("revolving_door", "witness_lobby_overlap", "commenter_lobby_overlap",
                  "lobbying_issue_summary", "lobbying_by_year", "top_lobbying_clients"):
            n_before = conn.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE name=? AND type='table'", (t,)).fetchone()[0]
            conn.execute(f"DROP TABLE IF EXISTS {t}")
            if n_before:
                print(f"  Dropped {t}")

        # ── Step 2: drop lobbying_filings + lobbying_activities (will rebuild from lobbying.db) ──
        # Also need to drop dependent indexes; SQLite drops them with the table.
        n_filings_before = conn.execute("SELECT COUNT(*) FROM lobbying_filings").fetchone()[0]
        n_act_before = conn.execute("SELECT COUNT(*) FROM lobbying_activities").fetchone()[0]
        print(f"  Pre-drop: lobbying_filings={n_filings_before:,} lobbying_activities={n_act_before:,}")

        # Drop FTS table for lobbying activities — it references the activities table
        for t in ("lobbying_fts", "lobbying_fts_data", "lobbying_fts_idx",
                  "lobbying_fts_content", "lobbying_fts_docsize", "lobbying_fts_config"):
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        print("  Dropped lobbying_fts and shadow tables")

        conn.execute("DROP TABLE IF EXISTS lobbying_filings")
        conn.execute("DROP TABLE IF EXISTS lobbying_activities")
        conn.commit()
        print("  Dropped lobbying_filings + lobbying_activities")

        # ── Step 3: recreate tables with new schema ──
        # Pull schema from 05_build_database.py — using the new column layout
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS lobbying_filings (
                filing_uuid TEXT PRIMARY KEY,
                filing_type TEXT NOT NULL,
                registrant_id INTEGER,
                registrant_name TEXT,
                registrant_state TEXT,
                registrant_country TEXT,
                registrant_house_id INTEGER,
                client_id INTEGER,
                client_name TEXT,
                client_state TEXT,
                client_ppb_state TEXT,
                client_country TEXT,
                client_ppb_country TEXT,
                client_general_description TEXT,
                client_government_entity INTEGER,
                filing_year INTEGER,
                filing_period TEXT,
                received_date TEXT,
                income_amount REAL,
                expense_amount REAL,
                is_amendment INTEGER DEFAULT 0,
                is_no_activity INTEGER DEFAULT 0,
                is_termination INTEGER DEFAULT 0,
                affiliated_org_count INTEGER
            );

            CREATE TABLE IF NOT EXISTS lobbying_activities (
                id INTEGER PRIMARY KEY,
                filing_uuid TEXT NOT NULL,
                filing_type TEXT NOT NULL,
                registrant_name TEXT NOT NULL,
                registrant_id INTEGER,
                client_name TEXT NOT NULL,
                client_name_normalized TEXT,
                filing_year INTEGER NOT NULL,
                filing_period TEXT NOT NULL,
                issue_code TEXT,
                specific_issues TEXT,
                government_entities TEXT,
                is_no_activity INTEGER DEFAULT 0,
                is_termination INTEGER DEFAULT 0,
                received_date TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_lobby_filing_type ON lobbying_filings(filing_type);
            CREATE INDEX IF NOT EXISTS idx_lobby_client ON lobbying_filings(client_name);
            CREATE INDEX IF NOT EXISTS idx_lobby_year ON lobbying_filings(filing_year);
            CREATE INDEX IF NOT EXISTS idx_lobby_client_state ON lobbying_filings(client_state);
            CREATE INDEX IF NOT EXISTS idx_lobby_registrant_state ON lobbying_filings(registrant_state);
            CREATE INDEX IF NOT EXISTS idx_lobby_house_id ON lobbying_filings(registrant_house_id) WHERE registrant_house_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_lobby_govt_entity ON lobbying_filings(client_government_entity) WHERE client_government_entity = 1;
            CREATE INDEX IF NOT EXISTS idx_lobby_act_client ON lobbying_activities(client_name);
            CREATE INDEX IF NOT EXISTS idx_lobby_act_client_norm ON lobbying_activities(client_name_normalized);
            CREATE INDEX IF NOT EXISTS idx_lobby_act_issue ON lobbying_activities(issue_code);
            CREATE INDEX IF NOT EXISTS idx_lobby_act_year ON lobbying_activities(filing_year);
            CREATE INDEX IF NOT EXISTS idx_lobby_act_uuid ON lobbying_activities(filing_uuid);
            CREATE INDEX IF NOT EXISTS idx_lobby_act_registrant ON lobbying_activities(registrant_name);
        """)
        conn.commit()
        print("  Recreated lobbying_filings + lobbying_activities with new schema")

        # ── Step 4: re-import from lobbying.db ──
        t0 = time.time()
        conn.execute(f"ATTACH DATABASE '{lobbying_path}' AS ldb")
        n_filings = conn.execute("""
            INSERT OR IGNORE INTO lobbying_filings
            (filing_uuid, filing_type, registrant_id, registrant_name,
             registrant_state, registrant_country, registrant_house_id,
             client_id, client_name,
             client_state, client_ppb_state, client_country, client_ppb_country,
             client_general_description, client_government_entity,
             filing_year, filing_period,
             received_date, income_amount, expense_amount,
             is_amendment, is_no_activity, is_termination,
             affiliated_org_count)
            SELECT filing_uuid, filing_type, registrant_id, registrant_name,
                   registrant_state, registrant_country, registrant_house_id,
                   client_id, client_name,
                   client_state, client_ppb_state, client_country, client_ppb_country,
                   client_general_description, client_government_entity,
                   filing_year, filing_period,
                   received_date, income_amount, expense_amount,
                   is_amendment, is_no_activity, is_termination,
                   affiliated_org_count
            FROM ldb.lobbying_filings_raw
        """).rowcount
        conn.commit()
        print(f"  Imported {n_filings:,} filings in {time.time()-t0:.0f}s")

        t0 = time.time()
        n_acts = conn.execute("""
            INSERT OR IGNORE INTO lobbying_activities
            (id, filing_uuid, filing_type, registrant_name, registrant_id, client_name,
             filing_year, filing_period, issue_code, specific_issues,
             government_entities,
             is_no_activity, is_termination, received_date)
            SELECT id, filing_uuid, filing_type, registrant_name, registrant_id, client_name,
                   filing_year, filing_period, issue_code, specific_issues,
                   government_entities,
                   is_no_activity, is_termination, received_date
            FROM ldb.lobbying_activities
        """).rowcount
        conn.commit()
        print(f"  Imported {n_acts:,} activities in {time.time()-t0:.0f}s")

        # ── Step 5: client_name_normalized backfill ──
        t0 = time.time()
        conn.execute("UPDATE lobbying_activities SET client_name_normalized = UPPER(TRIM(client_name)) WHERE client_name_normalized IS NULL")
        conn.commit()
        print(f"  Normalized client_name in {time.time()-t0:.0f}s")

        conn.execute("DETACH DATABASE ldb")

        # ── Step 6: spot-check ──
        print()
        print("=== Spot-check: top 5 LD-2 clients (post-fix) ===")
        for row in conn.execute("""
            SELECT client_name,
                   COUNT(*) AS filings,
                   CAST(SUM(income_amount) AS INTEGER) AS total
            FROM lobbying_filings
            WHERE filing_type GLOB '[1234Q]*' AND income_amount > 0
            GROUP BY client_name
            ORDER BY total DESC LIMIT 10
        """):
            print(f"  {row[0]:<55} filings={row[1]:>5}  total=${row[2]:>15,}")

        print()
        for name in ['AT&T SERVICES, INC.', 'GOOGLE INC.', 'META PLATFORMS, INC.']:
            row = conn.execute("""
                SELECT COUNT(*), CAST(SUM(income_amount) AS INTEGER)
                FROM lobbying_filings
                WHERE client_name=? AND filing_type GLOB '[1234Q]*' AND income_amount > 0
            """, (name,)).fetchone()
            print(f"  {name}: {row[0]} filings, total=${row[1]:,}")
    finally:
        conn.close()
    print()
    print("Migration complete. Next: rebuild FTS + precomputed tables.")


if __name__ == "__main__":
    base = Path(__file__).resolve().parents[2]
    openregs = Path(sys.argv[1]) if len(sys.argv) > 1 else base / "openregs.db"
    lobbying = Path(sys.argv[2]) if len(sys.argv) > 2 else base / "lobbying.db"
    run(openregs, lobbying)
