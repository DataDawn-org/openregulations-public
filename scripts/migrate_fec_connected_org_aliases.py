#!/usr/bin/env python3
"""One-time migration: fec_connected_org PAC aliases -> entity_relationships.

Audit-confirmed identity bug (2026-06-11): FEC connected-org (sponsor) names
were stored as ALIASES of their PACs, so matcher Tier 2 resolved sponsor names
to the PAC ahead of the sponsor's own exact name match (PhRMA -> PhRMA Better
Government Cmte). The relationship row — relationship_type='affiliated_pac',
source='fec_connected_org', exactly the enums the entity_relationships DDL
documented — is the correct home. Producer fixed in entity_population.py the
same day; this migrates the existing rows.

Per alias row:
  1. snapshot to JSON next to this script (reversibility)
  2. INSERT entity_relationships (parent = sponsor entity when its name is
     UNIQUE among entities, else NULL; child = PAC; raw sponsor name preserved
     in notes; FEC committee id in source_ref) — NOT EXISTS-guarded, idempotent
  3. DELETE the alias row

Usage:
    python3 scripts/migrate_fec_connected_org_aliases.py <db_path> [--dry-run]

Run against BOTH /mnt/data/datadawn/staging/entities_phase_a.db (canonical;
monthly Phase A re-runs land there) and openregs.db (derived; aligns the local
build artifact with what the next 05_build import will produce anyway).
Regression fixtures: test_entity_matcher_fixtures.py (F1-F4 green after this).
"""
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

def main():
    args = [a for a in sys.argv[1:] if a != '--dry-run']
    dry = '--dry-run' in sys.argv
    if len(args) != 1:
        sys.exit(__doc__)
    db_path = Path(args[0]).resolve()
    if not db_path.exists():
        sys.exit(f"no such db: {db_path}")

    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT a.entity_id AS pac_id, a.alias_name, a.alias_normalized
        FROM entity_aliases a WHERE a.alias_source='fec_connected_org'
    """).fetchall()
    print(f"{db_path.name}: {len(rows):,} fec_connected_org alias rows")
    if not rows:
        print("nothing to migrate"); return

    snap = Path(__file__).parent / (
        f"fec_connected_org_alias_snapshot_{db_path.stem}_{time.strftime('%Y%m%d')}.json")
    if not dry:
        snap.write_text(json.dumps(
            [{'pac_entity_id': r[0], 'alias_name': r[1], 'alias_normalized': r[2]}
             for r in rows], indent=1))
        print(f"snapshot: {snap}")

    inserted = parent_resolved = deleted = 0
    for pac_id, alias_name, alias_norm in rows:
        cmte = conn.execute(
            "SELECT fec_committee_id FROM entities WHERE entity_id=?", (pac_id,)).fetchone()
        source_ref = cmte[0] if cmte and cmte[0] else f"entity:{pac_id}"
        parents = conn.execute(
            "SELECT entity_id FROM entities WHERE name_normalized=? LIMIT 2",
            (alias_norm,)).fetchall()
        parent_id = parents[0][0] if (len(parents) == 1 and parents[0][0] != pac_id) else None
        if parent_id is not None:
            parent_resolved += 1
        if dry:
            continue
        cur = conn.execute("""
            INSERT INTO entity_relationships
                (parent_entity_id, child_entity_id, relationship_type,
                 source, source_ref, notes, created_at)
            SELECT ?, ?, 'affiliated_pac', 'fec_connected_org', ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM entity_relationships
                WHERE child_entity_id=? AND relationship_type='affiliated_pac'
                  AND source='fec_connected_org' AND source_ref=?)
        """, (parent_id, pac_id, source_ref,
              f"connected_org_name={alias_name}", now, pac_id, source_ref))
        inserted += cur.rowcount
        cur = conn.execute(
            "DELETE FROM entity_aliases WHERE entity_id=? AND alias_normalized=? AND alias_source='fec_connected_org'",
            (pac_id, alias_norm))
        deleted += cur.rowcount

    if not dry:
        conn.commit()
    left = conn.execute(
        "SELECT COUNT(*) FROM entity_aliases WHERE alias_source='fec_connected_org'").fetchone()[0]
    rel = conn.execute(
        "SELECT COUNT(*) FROM entity_relationships WHERE relationship_type='affiliated_pac'").fetchone()[0]
    print(f"{'DRY-RUN: would insert' if dry else 'inserted'} {inserted if not dry else len(rows):,} relationships "
          f"({parent_resolved:,} with resolved sponsor parent), "
          f"{'would delete' if dry else 'deleted'} {deleted if not dry else len(rows):,} aliases")
    print(f"post-state: {left:,} fec_connected_org aliases remain, "
          f"{rel:,} affiliated_pac relationships exist")
    conn.close()


if __name__ == "__main__":
    main()
