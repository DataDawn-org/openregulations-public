#!/usr/bin/env python3
"""Build comment_org_entities table — commenter-name → entity match for ranking.

Maps the raw organization-name strings that appear on regulations.gov comments
(comments.submitter_name where submitter_type='organization', plus
comment_details.organization) to canonical entities, so public surfaces can
rank/badge comments from organizations DataDawn already covers (company hub
/org/cik/, nonprofit hub /org/ein/).

Resolution goes through the central EntityMatcher (entity_matcher.py) —
unique-match-or-miss, no arbitrary picks on ambiguous names (Design-B
doctrine, decisions_log §88). Name-only resolution (no state hint): the table
is keyed by the raw string, and one string must map to at most one entity.

Key semantics: name_key = UPPER(TRIM(raw name)). Query-time consumers join
    ... LEFT JOIN comment_org_entities coe
        ON coe.name_key = UPPER(TRIM(COALESCE(NULLIF(TRIM(cd.organization),''),
                                              c.submitter_name)))
hub: 'company' when the entity has a CIK (company hub page exists),
     else 'nonprofit' when it has an EIN, else NULL (matched, no hub page).
Entities with both (e.g. Pfizer) get 'company' — the richer hub.

Standalone / wired into 05_build_database.py (same dual-home pattern as
build_entity_prominence.py). Consumers LEFT JOIN and feature-detect, so a
missing or empty table degrades to unranked display, never an error.

Expected scale: ~200K distinct name keys, ~25-60K matched (match rate varies
by docket mix; many submitter_name values are "Person, Org" mashups or
coalitions that correctly fail unique resolution).
"""
import sqlite3
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from entity_matcher import EntityMatcher  # noqa: E402

OPENREGS = SCRIPT_DIR.parent / "openregs.db"

DDL = """
DROP TABLE IF EXISTS main.comment_org_entities;
CREATE TABLE main.comment_org_entities (
    name_key TEXT PRIMARY KEY,          -- UPPER(TRIM(raw name)) as seen in comment tables
    entity_id INTEGER NOT NULL REFERENCES entities(entity_id),
    canonical_name TEXT NOT NULL,
    entity_type TEXT,
    ein TEXT,
    cik INTEGER,
    hub TEXT,                           -- 'company' | 'nonprofit' | NULL (no hub page)
    match_method TEXT NOT NULL          -- EntityMatcher method ('normalized_name' etc.)
);
"""

NAME_SOURCES = """
SELECT DISTINCT UPPER(TRIM(submitter_name)) AS name_key
FROM comments
WHERE submitter_type = 'organization'
  AND submitter_name IS NOT NULL AND TRIM(submitter_name) != ''
UNION
SELECT DISTINCT UPPER(TRIM(organization)) AS name_key
FROM comment_details
WHERE organization IS NOT NULL AND TRIM(organization) != ''
"""


def build(conn: sqlite3.Connection) -> dict:
    """Build the table on an open openregs.db connection. Returns stats."""
    conn.executescript(DDL)
    matcher = EntityMatcher(conn)
    names = [r[0] for r in conn.execute(NAME_SOURCES)]

    rows = []
    for name_key in names:
        r = matcher.resolve(name=name_key, source_context="comment_org")
        if r.entity_id is None:
            continue  # unmatched, ambiguous, or resolved to gov_unit/actor — skip
        ent = conn.execute(
            "SELECT canonical_name, entity_type, ein, cik FROM entities WHERE entity_id = ?",
            (r.entity_id,),
        ).fetchone()
        if ent is None:
            continue
        canonical_name, entity_type, ein, cik = ent
        hub = "company" if cik is not None else ("nonprofit" if ein else None)
        rows.append((name_key, r.entity_id, canonical_name, entity_type,
                     ein, cik, hub, r.match_method))

    conn.executemany(
        "INSERT INTO main.comment_org_entities "
        "(name_key, entity_id, canonical_name, entity_type, ein, cik, hub, match_method) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.execute(
        "CREATE INDEX main.idx_coe_entity ON comment_org_entities(entity_id)"
    )
    conn.commit()

    hubs = dict(conn.execute(
        "SELECT COALESCE(hub, 'matched_no_hub'), COUNT(*) "
        "FROM comment_org_entities GROUP BY hub"
    ).fetchall())
    return {"names_seen": len(names), "matched": len(rows), "hubs": hubs}


def main():
    if not OPENREGS.exists():
        sys.exit(f"missing openregs.db at {OPENREGS}")
    t0 = time.time()
    db = sqlite3.connect(OPENREGS)
    try:
        stats = build(db)
    finally:
        db.close()
    print(f"comment_org_entities built in {time.time()-t0:.1f}s — "
          f"{stats['matched']:,} of {stats['names_seen']:,} distinct names matched")
    for hub, n in sorted(stats["hubs"].items()):
        print(f"  {hub}: {n:,}")


if __name__ == "__main__":
    main()
