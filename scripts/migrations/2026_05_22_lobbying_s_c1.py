"""
2026-05-22 S-C1 lobbying spend inflation fix — one-shot migration.

Bug: lobbying_activities.income_amount was filing-level data replicated across
activity rows. SUM queries inflated by activity count (2-3x for major lobbyists).
A second bug: lobbying_filings_raw.amount_reported conflated income (outside
firms) with expense (in-house lobbyists) under a single column name.

Fix:
  - Add income_amount + expense_amount to lobbying_filings_raw (filing-level,
    XOR-populated, both nullable).
  - Backfill from raw_json (single transaction).
  - DROP amount_reported from lobbying_filings_raw.
  - DROP income_amount + expense_amount from lobbying_activities (grain
    mismatch — these are filing-level facts, not activity-level).
  - Rebuild 4 summary tables via 15_lobbying_disclosure.py build_summary_tables.

References:
  - bestpractices/audit_2026-05-22_schema_fit.md (S-C1, S-L4)
  - bestpractices/decisions_log.md §64-66
  - bestpractices/incident_log.md 2026-05-22

Usage:
  python3 migrations/2026_05_22_lobbying_s_c1.py [DB_PATH]
  DB_PATH defaults to openregs/lobbying.db (relative to script's parent).

Idempotent: safe to re-run. Checks column existence before each ALTER.
"""

import json
import sqlite3
import sys
import time
from pathlib import Path


def safe_float(x):
    """Parse to float; return None on '', None, or unparseable."""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def has_column(conn, table, col):
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    return col in cols


def run(db_path):
    print(f"=== Lobbying S-C1 migration on {db_path} ===")
    print(f"SQLite version: {sqlite3.sqlite_version}")
    assert sqlite3.sqlite_version >= "3.35", "Need SQLite 3.35+ for DROP COLUMN"

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")  # we're rewriting both ends; FK noise

    try:
        # ── Step 1: add new columns to lobbying_filings_raw if missing ──
        t0 = time.time()
        if has_column(conn, "lobbying_filings_raw", "income_amount"):
            print("Step 1: income_amount already exists, skipping ADD")
        else:
            print("Step 1: adding income_amount, expense_amount to lobbying_filings_raw")
            conn.execute("ALTER TABLE lobbying_filings_raw ADD COLUMN income_amount REAL")
            conn.execute("ALTER TABLE lobbying_filings_raw ADD COLUMN expense_amount REAL")
            conn.commit()
            print(f"  added in {time.time()-t0:.1f}s")

        # ── Step 2: backfill income_amount + expense_amount from raw_json ──
        # Single-statement SQL backfill via json_extract. Idempotent: only touches
        # rows where both new columns are still NULL. Empty-string and missing-key
        # cases produce NULL via NULLIF. Non-numeric values would CAST to 0 — we
        # verified at audit time that no such values exist in this dataset.
        #
        # Earlier version used a Python LIMIT loop without OFFSET; the "neither
        # populated" rows kept matching the WHERE clause and got re-processed,
        # inflating the progress counter and terminating early. Pure SQL avoids
        # the loop-state-vs-WHERE-clause coupling entirely.
        n_total = conn.execute("SELECT COUNT(*) FROM lobbying_filings_raw").fetchone()[0]
        n_to_backfill = conn.execute("""
            SELECT COUNT(*) FROM lobbying_filings_raw
            WHERE income_amount IS NULL AND expense_amount IS NULL
        """).fetchone()[0]
        print(f"Step 2: backfilling {n_to_backfill:,} of {n_total:,} rows via SQL json_extract")

        t0 = time.time()
        conn.execute("""
            UPDATE lobbying_filings_raw
            SET income_amount = CAST(NULLIF(json_extract(raw_json, '$.income'), '') AS REAL),
                expense_amount = CAST(NULLIF(json_extract(raw_json, '$.expenses'), '') AS REAL)
            WHERE income_amount IS NULL AND expense_amount IS NULL
        """)
        conn.commit()

        income_set = conn.execute("SELECT COUNT(*) FROM lobbying_filings_raw WHERE income_amount IS NOT NULL").fetchone()[0]
        expense_set = conn.execute("SELECT COUNT(*) FROM lobbying_filings_raw WHERE expense_amount IS NOT NULL").fetchone()[0]
        neither = conn.execute("SELECT COUNT(*) FROM lobbying_filings_raw WHERE income_amount IS NULL AND expense_amount IS NULL").fetchone()[0]
        print(f"  Backfill done in {time.time()-t0:.0f}s: "
              f"income_populated={income_set:,} expense_populated={expense_set:,} "
              f"neither={neither:,} ({100*neither/n_total:.1f}% of total)")

        # ── Step 3: sanity check against existing amount_reported (if column exists) ──
        if has_column(conn, "lobbying_filings_raw", "amount_reported"):
            print("Step 3: sanity-check amount_reported = COALESCE(income_amount, expense_amount)")
            mismatch = conn.execute("""
                SELECT COUNT(*) FROM lobbying_filings_raw
                WHERE amount_reported IS NOT NULL
                  AND COALESCE(income_amount, expense_amount) IS NOT amount_reported
            """).fetchone()[0]
            print(f"  Mismatches: {mismatch:,}")
            # Show sample mismatches if any
            if mismatch:
                samples = conn.execute("""
                    SELECT filing_uuid, filing_type, amount_reported, income_amount, expense_amount
                    FROM lobbying_filings_raw
                    WHERE amount_reported IS NOT NULL
                      AND COALESCE(income_amount, expense_amount) IS NOT amount_reported
                    LIMIT 5
                """).fetchall()
                for s in samples:
                    print(f"    {s}")
                # tolerate small floating-point rounding mismatches; abort if widespread
                if mismatch > n_total * 0.001:  # >0.1% mismatch is a red flag
                    raise SystemExit(f"ABORT: {mismatch} mismatches > 0.1% of {n_total}")
                print(f"  WARN: {mismatch} mismatches but under 0.1% threshold — continuing")
        else:
            print("Step 3: amount_reported already dropped, skipping sanity check")

        # ── Step 4: DROP amount_reported from lobbying_filings_raw ──
        if has_column(conn, "lobbying_filings_raw", "amount_reported"):
            print("Step 4: DROP COLUMN amount_reported FROM lobbying_filings_raw")
            t0 = time.time()
            conn.execute("ALTER TABLE lobbying_filings_raw DROP COLUMN amount_reported")
            conn.commit()
            print(f"  dropped in {time.time()-t0:.0f}s")
        else:
            print("Step 4: amount_reported already dropped, skipping")

        # ── Step 5: DROP income_amount + expense_amount from lobbying_activities ──
        for col in ("income_amount", "expense_amount"):
            if has_column(conn, "lobbying_activities", col):
                print(f"Step 5: DROP COLUMN {col} FROM lobbying_activities")
                t0 = time.time()
                conn.execute(f"ALTER TABLE lobbying_activities DROP COLUMN {col}")
                conn.commit()
                print(f"  dropped in {time.time()-t0:.0f}s")
            else:
                print(f"Step 5: lobbying_activities.{col} already dropped, skipping")

        # ── Step 6: drop the 4 old summary tables so they get rebuilt cleanly ──
        # 15_lobbying_disclosure.py build_summary_tables() drops + recreates,
        # but we drop here too so a partial state isn't visible if the rebuild
        # is deferred.
        for t in ("lobbying_issue_summary", "lobbying_client_summary",
                  "lobbying_registrant_summary", "lobbying_year_summary"):
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        conn.commit()
        print("Step 6: dropped 4 stale summary tables (will be rebuilt by build_summary_tables)")

        # ── Step 7: VACUUM to reclaim space from dropped columns ──
        # SQLite's DROP COLUMN marks columns as dropped but doesn't shrink the
        # file; VACUUM rebuilds. Skip on first dry-run to save time; required
        # for prod.
        print("Step 7: skipping VACUUM (caller should run separately)")

        # ── Final summary ──
        print()
        print("=== Final state ===")
        for t in ("lobbying_filings_raw", "lobbying_activities"):
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({t})")]
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t} ({n:,} rows): {cols}")

        # Spot-check: top-5 clients by new income_amount aggregate, LD-2 only
        print()
        print("=== Spot-check: top 5 LD-2 clients by income_amount (canonical) ===")
        for row in conn.execute("""
            SELECT client_name,
                   COUNT(*) AS filings,
                   CAST(SUM(income_amount) AS INTEGER) AS total
            FROM lobbying_filings_raw
            WHERE filing_type GLOB '[1234Q]*' AND income_amount > 0
            GROUP BY client_name
            ORDER BY total DESC
            LIMIT 10
        """):
            print(f"  {row[0]:<55} filings={row[1]:>5}  total=${row[2]:>15,}")
    finally:
        conn.close()
    print()
    print("Migration complete.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        db = Path(sys.argv[1])
    else:
        db = Path(__file__).resolve().parents[2] / "lobbying.db"
    run(db)
