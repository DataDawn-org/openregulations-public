#!/usr/bin/env python3
"""
Refresh member_stats table without a full database rebuild.

Run monthly (or after new data pulls) to update trade/speech/bill counts.
Fast — takes ~10 seconds on the full database.

Usage:
    python3 scripts/refresh_member_stats.py
"""

import sqlite3
import sys
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "openregs.db"


def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    print("Refreshing member_stats...")
    start = time.time()

    conn.execute("DROP TABLE IF EXISTS member_stats")
    conn.execute("""
        CREATE TABLE member_stats AS
        SELECT
            cm.bioguide_id,
            (SELECT COUNT(*) FROM stock_trades WHERE bioguide_id = cm.bioguide_id) AS trade_count,
            (SELECT COUNT(*) FROM crec_speakers WHERE bioguide_id = cm.bioguide_id) AS speech_count,
            (SELECT COUNT(*) FROM legislation WHERE sponsor_bioguide_id = cm.bioguide_id) AS bills_sponsored
        FROM congress_members cm
    """)
    conn.execute("CREATE UNIQUE INDEX idx_member_stats_bio ON member_stats(bioguide_id)")
    conn.execute("CREATE TABLE IF NOT EXISTS build_metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT OR REPLACE INTO build_metadata VALUES ('member_stats_updated', date('now'))")
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM member_stats").fetchone()[0]
    elapsed = time.time() - start
    print(f"  {count:,} members updated in {elapsed:.1f}s")
    ts = conn.execute("SELECT value FROM build_metadata WHERE key = 'member_stats_updated'").fetchone()[0]
    print(f"  Timestamp: {ts}")

    conn.close()


if __name__ == "__main__":
    main()
