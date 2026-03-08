#!/usr/bin/env python3
"""
qc_database.py — Quality check for openregs.db before deployment.

Compares new build against previous build report, checks for regressions,
validates new tables, and runs spot-check queries.

Usage:
  python3 scripts/qc_database.py                    # full QC
  python3 scripts/qc_database.py --quick             # row counts only
"""

import json
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "openregs.db"
REPORT_DIR = BASE_DIR / "build_reports"
LATEST_REPORT = REPORT_DIR / "latest.json"

# Tables that should never decrease in row count (append-only data)
APPEND_ONLY = {
    "federal_register", "comments", "documents", "dockets",
    "legislation", "congress_members", "congressional_record",
    "stock_trades", "spending_awards", "lobbying_filings",
    "lobbying_activities", "lobbying_lobbyists", "fec_contributions",
    "fec_candidates", "fec_committees", "fara_registrants",
    "fara_foreign_principals", "fara_short_forms", "fara_registrant_docs",
    "roll_call_votes", "member_votes", "cfr_sections",
    "presidential_documents", "committees", "committee_memberships",
}

# New tables expected in this build (won't be in baseline)
EXPECTED_NEW = {
    "earmarks", "nominations", "nomination_actions",
    "treaties", "treaty_actions", "hearings", "hearing_witnesses",
    "hearing_members", "crs_reports", "crs_report_bills",
    "gao_reports",
}

# Minimum expected row counts for key tables
MIN_ROWS = {
    "federal_register": 990_000,
    "comments": 3_700_000,
    "documents": 700_000,
    "legislation": 160_000,
    "congress_members": 12_000,
    "congressional_record": 870_000,
    "stock_trades": 60_000,  # Was 95K before PTR-only filter removed non-trade filings
    "spending_awards": 860_000,
    "lobbying_filings": 1_900_000,
    "fec_contributions": 4_300_000,
    "member_votes": 8_300_000,
    "earmarks": 70_000,
    "nominations": 39_000,
    "hearings": 45_000,
    "crs_reports": 13_000,
    "gao_reports": 16_000,
}

# Spot-check queries that should return results
SPOT_CHECKS = [
    ("Earmarks with bioguide_id", "SELECT COUNT(*) FROM earmarks WHERE bioguide_id IS NOT NULL"),
    ("Earmarks by fiscal year", "SELECT fiscal_year, COUNT(*) FROM earmarks GROUP BY fiscal_year ORDER BY fiscal_year"),
    ("Nominations confirmed", "SELECT COUNT(*) FROM nominations WHERE status = 'Confirmed'"),
    ("Nominations with votes", "SELECT COUNT(*) FROM nominations WHERE vote_yea IS NOT NULL"),
    ("Treaties in force", "SELECT COUNT(*) FROM treaties WHERE in_force_date IS NOT NULL"),
    ("Hearings with witnesses", "SELECT COUNT(DISTINCT package_id) FROM hearing_witnesses"),
    ("CRS reports with bill refs", "SELECT COUNT(DISTINCT report_id) FROM crs_report_bills"),
    ("GAO reports with abstracts", "SELECT COUNT(*) FROM gao_reports WHERE abstract IS NOT NULL"),
    ("FTS earmarks test", "SELECT COUNT(*) FROM earmarks_fts WHERE earmarks_fts MATCH 'infrastructure'"),
    ("Cross-ref: earmarks members in congress_members",
     "SELECT COUNT(DISTINCT e.bioguide_id) FROM earmarks e JOIN congress_members cm ON e.bioguide_id = cm.bioguide_id"),
    ("Revolving door count", "SELECT COUNT(*) FROM revolving_door"),
    ("Lobbying bill refs", "SELECT COUNT(*) FROM lobbying_bills"),
    ("Speeches near trades", "SELECT COUNT(*) FROM speeches_near_trades"),
]


def load_baseline():
    """Load the previous build report for comparison."""
    if LATEST_REPORT.exists():
        return json.loads(LATEST_REPORT.read_text())
    return None


def run_qc(quick=False):
    if not DB_PATH.exists():
        print("ERROR: openregs.db not found!")
        return False

    conn = sqlite3.connect(str(DB_PATH))
    baseline = load_baseline()

    print("=" * 60)
    print("OPENREGS DATABASE — QUALITY CHECK")
    print("=" * 60)

    db_size = DB_PATH.stat().st_size
    print(f"\nDatabase: {DB_PATH}")
    print(f"Size: {db_size / 1024**3:.2f} GB")
    if baseline:
        old_size = baseline.get("database_size_bytes", 0)
        delta = db_size - old_size
        print(f"Previous: {old_size / 1024**3:.2f} GB ({'+' if delta >= 0 else ''}{delta / 1024**2:.0f} MB)")

    # Get all tables
    tables = {row[0]: row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()}

    print(f"\nTables: {len(tables)}")

    # === Row count comparison ===
    print(f"\n{'TABLE':<40} {'NEW':>12} {'OLD':>12} {'DELTA':>12} {'STATUS'}")
    print("-" * 88)

    issues = []
    warnings = []
    all_counts = {}

    old_tables = baseline.get("tables", {}) if baseline else {}

    for table_name in sorted(tables.keys()):
        # Skip FTS internal tables
        if any(table_name.endswith(s) for s in ("_config", "_data", "_docsize", "_idx", "_content")):
            continue

        try:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
        except sqlite3.Error:
            count = -1

        all_counts[table_name] = count
        old_count = old_tables.get(table_name)

        if old_count is not None:
            delta = count - old_count
            delta_str = f"{'+' if delta >= 0 else ''}{delta:,}"

            if table_name in APPEND_ONLY and delta < 0:
                status = "REGRESSION!"
                issues.append(f"{table_name}: lost {abs(delta):,} rows ({old_count:,} → {count:,})")
            elif delta < 0:
                status = "decreased"
                warnings.append(f"{table_name}: {delta:,} rows")
            elif delta > 0:
                status = "+"
            else:
                status = "ok"
        elif table_name in EXPECTED_NEW:
            delta_str = "NEW"
            status = "new"
        else:
            delta_str = "—"
            status = ""

        print(f"{table_name:<40} {count:>12,} {old_count if old_count is not None else '—':>12} {delta_str:>12} {status}")

    # === Minimum row checks ===
    print(f"\n{'MINIMUM ROW CHECKS':}")
    print("-" * 60)
    for table, min_count in sorted(MIN_ROWS.items()):
        actual = all_counts.get(table, 0)
        if actual >= min_count:
            print(f"  {table:<35} {actual:>12,} >= {min_count:>12,}  OK")
        else:
            print(f"  {table:<35} {actual:>12,} <  {min_count:>12,}  FAIL")
            issues.append(f"{table}: {actual:,} rows below minimum {min_count:,}")

    if quick:
        print("\n(--quick mode: skipping spot checks)")
    else:
        # === Spot-check queries ===
        print(f"\nSPOT-CHECK QUERIES:")
        print("-" * 60)
        for label, sql in SPOT_CHECKS:
            try:
                rows = conn.execute(sql).fetchall()
                if len(rows) == 1 and len(rows[0]) == 1:
                    result = f"{rows[0][0]:,}"
                else:
                    result = "; ".join(str(r) for r in rows[:8])
                    if len(rows) > 8:
                        result += f" ... ({len(rows)} total)"
                print(f"  {label:<50} {result}")
            except sqlite3.Error as e:
                print(f"  {label:<50} ERROR: {e}")
                issues.append(f"Spot check failed: {label} — {e}")

    # === Summary ===
    print(f"\n{'=' * 60}")
    if issues:
        print(f"ISSUES ({len(issues)}):")
        for i in issues:
            print(f"  - {i}")
    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  - {w}")
    if not issues and not warnings:
        print("ALL CHECKS PASSED")

    print("=" * 60)

    conn.close()
    return len(issues) == 0


if __name__ == "__main__":
    quick = "--quick" in sys.argv
    ok = run_qc(quick)
    sys.exit(0 if ok else 1)
