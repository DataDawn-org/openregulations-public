#!/usr/bin/env python3
"""Build entity_prominence table — finder prominence signal (followup_queue #36).

For each entity with an EIN that has at least one 990 filing in 990data.db,
record a size_band (0-5) and peak_revenue. The finder uses size_band DESC as
the primary tiebreaker before name-position + revenue, replacing the prior
LENGTH-ASC tiebreaker that surfaced micro-orgs for federated names ("golf"
returned GOLFHER ahead of United States Golf Association, etc.).

Standalone / wired into 05_build_database.py. ATTACHes 990data.db read-only
via WAL snapshot so the monthly 990 build is not blocked (same pattern as the
4 staging-DB attaches; see bestpractices/pipeline_verification.md and the
2026-04-21 unqualified-DROP incident).

Bands (spec from followup_queue #36, tunable):
    5  $1B+
    4  $100M-1B
    3  $10M-100M
    2  $1M-10M
    1  $100K-1M
    0  <$100K or no 990 ingested

Uses MAX(total_revenue) across all 990/990EZ/990PF filings (NOT just the
latest year) to absorb the known NULL-financial-field issue (#7) — peak
revenue is a more stable prominence signal than any single year and matches
"this org is/was a major filer" semantics.
"""
import sqlite3
import sys
import time
from pathlib import Path

OPENREGS = Path('/mnt/data/datadawn/openregs/openregs.db')
DATA990 = Path('/mnt/data/datadawn/990project/990data.db')

DDL = """
DROP TABLE IF EXISTS main.entity_prominence;
CREATE TABLE main.entity_prominence (
    entity_id INTEGER PRIMARY KEY,
    ein TEXT NOT NULL,
    peak_revenue REAL,
    size_band INTEGER NOT NULL
);
"""

POPULATE = """
INSERT INTO main.entity_prominence (entity_id, ein, peak_revenue, size_band)
WITH peak AS (
    SELECT ein, MAX(total_revenue) AS peak_revenue
    FROM data990.returns
    WHERE return_type IN ('990','990EZ','990PF')
      AND total_revenue IS NOT NULL
    GROUP BY ein
)
SELECT
    e.entity_id,
    e.ein,
    p.peak_revenue,
    CASE
        WHEN p.peak_revenue >= 1e9 THEN 5
        WHEN p.peak_revenue >= 1e8 THEN 4
        WHEN p.peak_revenue >= 1e7 THEN 3
        WHEN p.peak_revenue >= 1e6 THEN 2
        WHEN p.peak_revenue >= 1e5 THEN 1
        ELSE 0
    END AS size_band
FROM main.entities e
INNER JOIN peak p ON e.ein = p.ein
WHERE e.ein IS NOT NULL;
"""

INDEX = "CREATE INDEX main.idx_entity_prominence_band ON entity_prominence(size_band);"


def main():
    if not OPENREGS.exists():
        sys.exit(f"missing openregs.db at {OPENREGS}")
    if not DATA990.exists():
        sys.exit(f"missing 990data.db at {DATA990}")

    t0 = time.time()
    db = sqlite3.connect(OPENREGS)
    db.execute(f"ATTACH DATABASE 'file:{DATA990}?mode=ro' AS data990")
    try:
        db.executescript(DDL)
        db.execute(POPULATE)
        db.commit()
        db.execute(INDEX)
        db.commit()
        n, = db.execute("SELECT COUNT(*) FROM entity_prominence").fetchone()
        bands = db.execute(
            "SELECT size_band, COUNT(*) FROM entity_prominence GROUP BY size_band ORDER BY size_band DESC"
        ).fetchall()
    finally:
        try:
            db.execute("DETACH DATABASE data990")
        except sqlite3.OperationalError:
            pass
        db.close()

    print(f"entity_prominence built in {time.time()-t0:.1f}s — {n:,} rows")
    print("Band distribution (5=$1B+ ... 0=<$100K):")
    for band, count in bands:
        print(f"  band {band}: {count:>10,}")


if __name__ == "__main__":
    main()
