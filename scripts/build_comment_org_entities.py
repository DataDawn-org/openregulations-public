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


# ============================================================================
# Launch gate (2026-06-11, pre-launch verification ruling): the table ships
# ONLY on the maintainer's affirmative GO after the verification-packet review. Absence
# of a GO file means HOLD — the build drops/skips the table (the explore page
# feature-detects, so a hold is user-invisible) and the criticality floor
# check skips it with a loud GATE_HOLD log (deliberate hold != silent loss).
#
# GO file: openregs/comment_org_entities.GO — `key=value` lines:
#     mode=pinned | rebuild
#     snapshot=<path to reviewed snapshot .db>     (pinned mode)
#     sha256=<table_hash of the reviewed table>    (pinned mode)
#     reviewed_by=... date=... seed=...            (provenance, free-form)
# pinned: ship the reviewed artifact VERBATIM, hash-verified (what the reviewer
#   reviewed is provably what deploys — a fresh rebuild would differ because
#   the weekly update pulls new comments first). Hash mismatch = loud abort
#   of this step (table dropped -> floor fires -> deploy aborts -> pager).
# rebuild: steady-state after launch week — build fresh from current data.
# ============================================================================

GO_PATH = SCRIPT_DIR.parent / "comment_org_entities.GO"


def table_hash(conn: sqlite3.Connection) -> str:
    """Deterministic content hash of comment_org_entities (ordered dump).
    Single implementation shared by the verification packet and the gated
    build — the reviewed hash and the shipped hash must come from one place.
    """
    import hashlib
    h = hashlib.sha256()
    for row in conn.execute(
            "SELECT name_key, entity_id, canonical_name, entity_type, "
            "COALESCE(ein,''), COALESCE(cik,''), COALESCE(hub,''), match_method "
            "FROM comment_org_entities ORDER BY name_key"):
        # tuple() first: on a row_factory=sqlite3.Row connection, repr(row)
        # includes a memory address — non-deterministic. Caught by the packet
        # generator's snapshot assert on first run.
        h.update(repr(tuple(row)).encode())
    return h.hexdigest()


def snapshot_table(conn: sqlite3.Connection, dest: Path) -> str:
    """Copy comment_org_entities into a standalone snapshot DB; return hash."""
    dest.unlink(missing_ok=True)
    conn.execute(f"ATTACH DATABASE '{dest}' AS snap")
    conn.execute("CREATE TABLE snap.comment_org_entities AS SELECT * FROM main.comment_org_entities")
    conn.commit()
    conn.execute("DETACH DATABASE snap")
    return table_hash(conn)


def read_gate(go_path: Path = GO_PATH) -> dict | None:
    """None = HOLD. Otherwise dict of GO-file keys (mode required)."""
    if not go_path.exists():
        return None
    kv = {}
    for line in go_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            kv[k.strip()] = v.strip()
    if kv.get('mode') not in ('pinned', 'rebuild'):
        raise ValueError(f"GO file {go_path} has invalid/missing mode= (need pinned|rebuild)")
    return kv


def build_gated(conn: sqlite3.Connection, go_path: Path = GO_PATH, log=print) -> dict | None:
    """Gate-aware build. Returns build stats, {'shipped': 'pinned'} or None on hold."""
    gate = read_gate(go_path)
    if gate is None:
        log("GATE_HOLD: comment_org_entities — no affirmative GO "
            f"({go_path.name} absent); table NOT shipped (page degrades to "
            "unranked via feature-detect). Floor check will skip with the same marker.")
        conn.execute("DROP TABLE IF EXISTS main.comment_org_entities")
        conn.commit()
        return None
    if gate['mode'] == 'rebuild':
        log("GATE_GO mode=rebuild: building comment_org_entities fresh")
        return build(conn)
    # pinned: ship the reviewed artifact verbatim, verify hash
    snap = Path(gate['snapshot'])
    if not snap.exists():
        raise FileNotFoundError(f"GATE pinned snapshot missing: {snap}")
    conn.execute("DROP TABLE IF EXISTS main.comment_org_entities")
    conn.execute(f"ATTACH DATABASE 'file:{snap}?mode=ro' AS snap")
    conn.execute("CREATE TABLE main.comment_org_entities AS SELECT * FROM snap.comment_org_entities")
    conn.execute("DETACH DATABASE snap")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_coe_entity ON comment_org_entities(entity_id)")
    conn.commit()
    got = table_hash(conn)
    want = gate.get('sha256', '')
    if got != want:
        conn.execute("DROP TABLE IF EXISTS main.comment_org_entities")
        conn.commit()
        raise ValueError(
            f"GATE_PIN_HASH_MISMATCH: reviewed sha256={want[:16]}... but snapshot "
            f"restored as {got[:16]}... — table dropped; floor will abort deploy.")
    n = conn.execute("SELECT COUNT(*) FROM comment_org_entities").fetchone()[0]
    log(f"GATE_SHIPPED_PINNED: comment_org_entities = reviewed artifact "
        f"({n:,} rows, sha256={got[:16]}…, reviewed_by={gate.get('reviewed_by','?')})")
    return {'shipped': 'pinned', 'rows': n, 'sha256': got}


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
