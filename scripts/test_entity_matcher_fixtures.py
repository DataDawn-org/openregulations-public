#!/usr/bin/env python3
"""Entity-matcher regression fixtures (red-first, 2026-06-11).

Born from the PAC-misattribution audit (see memory
session_2026_06_11_detailed_comments_ranking.md round 4): FEC connected-org
sponsor names were absorbed as PAC *aliases* (alias_source='fec_connected_org'),
and matcher Tier 2 (alias) preempts the exact-name tier, so PhRMA/ACC/API
resolved to their own PACs. Written FAILING before the source fix landed, per
the red-first ruling.

Expected state by phase:
  after step 1 (alias->relationship migration):  F1-F4 GREEN
  after step 2 (tier-2 hardening, held for design sign-off):  F5 GREEN
  F6 must be GREEN at every phase (manual-alias authority regression guard).

Run: python3 scripts/test_entity_matcher_fixtures.py   (exit 1 on any FAIL)
"""
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from entity_matcher import EntityMatcher  # noqa: E402

DB = SCRIPT_DIR.parent / "openregs.db"

results = []


def check(fid, desc, ok, detail):
    results.append((fid, desc, ok, detail))
    print(f"  {'GREEN' if ok else 'RED  '}  {fid}: {desc}\n         -> {detail}")


def main():
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    m = EntityMatcher(conn)

    def ent(eid):
        if eid is None:
            return None
        return conn.execute(
            "SELECT canonical_name, entity_type, ein FROM entities WHERE entity_id=?",
            (eid,)).fetchone()

    print("== PAC-misattribution fixtures (expect GREEN after step-1 migration) ==")

    # F1: PhRMA — unique legitimate name-tier candidate exists (the c6, EIN
    # 530241211). Must resolve to IT, never to the Better Government Committee.
    r = m.resolve(name="PHARMACEUTICAL RESEARCH AND MANUFACTURERS OF AMERICA (PHRMA)",
                  source_context="fixture")
    e = ent(r.entity_id)
    check("F1", "PhRMA -> the 501(c)(6), not its PAC",
          bool(e) and e[2] == '530241211',
          f"resolved {e[0][:60] + ' type=' + e[1] if e else 'MISS'} (method={r.match_method})")

    # F2-F4: name tier has a genuine multi-entity collision (3 / 2 / 23
    # candidates). Resolve-or-disclose: the matcher must MISS — a PAC alias
    # must not convert a collision into a confident wrong answer.
    for fid, raw, n_collide in (
            ("F2", "AMERICAN CHEMISTRY COUNCIL (ACC)", 3),
            ("F3", "AMERICAN PETROLEUM INSTITUTE (API)", 2),
            ("F4", "AMERICAN VETERINARY MEDICAL ASSOCIATION", 23)):
        r = m.resolve(name=raw, source_context="fixture")
        e = ent(r.entity_id)
        check(fid, f"{raw.split(' (')[0].title()} -> MISS ({n_collide}-way name collision)",
              r.entity_id is None,
              f"resolved {e[0][:55] + ' type=' + e[1] if e else 'MISS (correct)'}")

    print("== Tier-2 defect fixtures ==")

    # F5: false-miss — two same-normalized alias rows pointing at ONE entity
    # must resolve (uniqueness must count DISTINCT entities, not rows).
    # Discover a real instance: duplicate-alias entity with no name-tier escape
    # (no entity carries the string as name_normalized).
    row = conn.execute("""
        SELECT a.alias_normalized, MIN(a.entity_id)
        FROM entity_aliases a
        WHERE NOT EXISTS (SELECT 1 FROM entities e WHERE e.name_normalized = a.alias_normalized)
        GROUP BY a.alias_normalized
        HAVING COUNT(*) > 1 AND COUNT(DISTINCT a.entity_id) = 1
        LIMIT 1""").fetchone()
    if row:
        nn, eid = row
        raw = conn.execute(
            "SELECT alias_name FROM entity_aliases WHERE alias_normalized=? LIMIT 1", (nn,)
        ).fetchone()[0]
        r = m.resolve(name=raw, source_context="fixture")
        check("F5", f"duplicate-alias unique entity resolves ({raw[:40]!r})",
              r.entity_id == eid,
              f"expected entity {eid}, got {r.entity_id} (method={r.match_method}) "
              "(GREEN expected only after step-2 hardening)")
    else:
        check("F5", "duplicate-alias case", False, "no real instance found — construct synthetic")

    # F6: manual-alias authority guard — a manual alias with NO name-tier
    # candidate must resolve, and must KEEP resolving after step-2 hardening
    # (manual is authoritative; the verification ledger is its safeguard).
    row = conn.execute("""
        SELECT a.alias_name, a.alias_normalized, a.entity_id, a.alias_source
        FROM entity_aliases a
        WHERE a.alias_source LIKE 'manual%'
          AND NOT EXISTS (SELECT 1 FROM entities e WHERE e.name_normalized = a.alias_normalized)
          AND (SELECT COUNT(DISTINCT entity_id) FROM entity_aliases a2
               WHERE a2.alias_normalized = a.alias_normalized) = 1
        LIMIT 1""").fetchone()
    if row:
        raw, nn, eid, src = row
        r = m.resolve(name=raw, source_context="fixture")
        check("F6", f"manual alias resolves ({src}, {raw[:40]!r})",
              r.entity_id == eid,
              f"expected entity {eid}, got {r.entity_id} (method={r.match_method})")
    else:
        check("F6", "manual-alias guard", False, "no qualifying manual alias found")

    # F7: Sunshine-Village class — a MACHINE alias whose string IS another
    # entity's name (non-empty disjoint name-tier set, here with a unique
    # name-tier owner). The alias must be refused and resolution must fall
    # through to the name-tier owner — never the alias owner.
    row = conn.execute("""
        SELECT a.alias_name, a.alias_normalized, a.entity_id AS alias_owner,
               (SELECT e.entity_id FROM entities e
                WHERE e.name_normalized = a.alias_normalized) AS name_owner
        FROM entity_aliases a
        WHERE a.alias_source NOT LIKE 'manual%'
          AND (SELECT COUNT(DISTINCT a2.entity_id) FROM entity_aliases a2
               WHERE a2.alias_normalized = a.alias_normalized) = 1
          AND (SELECT COUNT(*) FROM entities e
               WHERE e.name_normalized = a.alias_normalized) = 1
          AND NOT EXISTS (SELECT 1 FROM entities e
               WHERE e.name_normalized = a.alias_normalized
                 AND e.entity_id = a.entity_id)
        ORDER BY a.alias_normalized LIMIT 1""").fetchone()
    if row:
        raw, nn, alias_owner, name_owner = row
        r = m.resolve(name=raw, source_context="fixture")
        check("F7", f"machine alias refused when string IS another entity's name ({raw[:35]!r})",
              r.entity_id == name_owner and r.entity_id != alias_owner,
              f"alias_owner={alias_owner}, name_owner={name_owner}, resolved={r.entity_id} "
              f"(method={r.match_method}) (GREEN expected only after step-2 hardening)")
    else:
        check("F7", "Sunshine-Village class", False, "no real instance found")

    # F8: manual-alias collision — two manual rows, same normalized form,
    # DIFFERENT entities. Must refuse loudly (no first-row-wins, no fall-
    # through to a name tier the curators contest). Synthetic minimal DB:
    # this state is a curation error and should never exist in real data,
    # so the fixture constructs it rather than hoping to find it.
    syn = sqlite3.connect(":memory:")
    syn.executescript("""
        CREATE TABLE entities (entity_id INTEGER PRIMARY KEY, name_normalized TEXT,
                               primary_state TEXT);
        CREATE TABLE entity_aliases (entity_id INTEGER, alias_name TEXT,
                                     alias_normalized TEXT, alias_source TEXT);
        INSERT INTO entities VALUES (1, 'SOME OTHER NAME A', NULL),
                                    (2, 'SOME OTHER NAME B', NULL),
                                    (3, 'CONTESTED COALITION', NULL);
        INSERT INTO entity_aliases VALUES
            (1, 'Contested Coalition', 'CONTESTED COALITION', 'manual_pinned'),
            (2, 'Contested Coalition', 'CONTESTED COALITION', 'manual_chapter_national');
    """)
    r = EntityMatcher(syn).resolve(name="CONTESTED COALITION", source_context="fixture")
    check("F8", "conflicting manual aliases -> loud MISS, no fall-through",
          r.entity_id is None,
          f"resolved={r.entity_id} (entity 3 carries the name at name-tier; a fall-through "
          f"would have returned it — hard stop must prevent that)")
    syn.close()

    conn.close()
    n_red = sum(1 for _, _, ok, _ in results if not ok)
    print(f"\n{len(results) - n_red} GREEN / {n_red} RED")
    sys.exit(1 if n_red else 0)


if __name__ == "__main__":
    main()
