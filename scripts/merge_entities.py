#!/usr/bin/env python3
"""Entity dedup-merge tool (first uses: F-1 AdvaMed GLEIF-twin, F-2 NAM
FEC-shadow — 2026-06-12 verification-review remediation).

Merge semantics (the F-1 template ruling):
  - survivor inherits the loser's registry identifiers where survivor has
    NULL (LEI/CIK/EIN/UEI/FEC/DUNS/ticker) — cross-registry attributes on one
    record immunize against re-duplication at the next ingest
  - loser's aliases re-point to the survivor; loser's canonical name becomes
    a manual_legal_name alias of the survivor when its normalized form
    differs (keeps every string that matched the loser matching the survivor)
  - loser's entity_relationships re-point (self-references dropped)
  - loser: status='merged', merged_into_entity_id=survivor (row KEPT)
  - dd_id_redirects row (reason='merge') — public dd_id permanence
  - matcher excludes merged entities from every tier (entity_matcher.py,
    same-day change), so post-merge resolution lands on the survivor

Usage:
  python3 merge_entities.py <db_path> <loser_dd_id> <survivor_dd_id> "<note>" [--dry-run]

Run against BOTH /mnt/data/datadawn/staging/entities_phase_a.db (canonical)
and openregs.db (derived; aligns the local artifact pre-rebuild).
"""
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ID_COLS = ('ein', 'cik', 'uei', 'lei', 'fec_committee_id', 'duns', 'ticker')


def merge(conn: sqlite3.Connection, loser_dd: str, survivor_dd: str,
          note: str, dry: bool = False) -> dict:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from entity_matcher import normalize_name

    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).isoformat()
    L = conn.execute("SELECT * FROM entities WHERE dd_id=?", (loser_dd,)).fetchone()
    S = conn.execute("SELECT * FROM entities WHERE dd_id=?", (survivor_dd,)).fetchone()
    if not L or not S:
        raise SystemExit(f"missing entity: loser={bool(L)} survivor={bool(S)}")
    if L['merged_into_entity_id'] is not None:
        raise SystemExit(f"{loser_dd} already merged")
    lid, sid = L['entity_id'], S['entity_id']
    report = {'loser': loser_dd, 'survivor': survivor_dd, 'inherited': {}, 'aliases_repointed': 0}

    for col in ID_COLS:
        if L[col] is not None and S[col] is None:
            report['inherited'][col] = L[col]
            if not dry:
                # unique partial indexes: clear the loser's value first
                conn.execute(f"UPDATE entities SET {col}=NULL WHERE entity_id=?", (lid,))
                conn.execute(f"UPDATE entities SET {col}=? WHERE entity_id=?", (L[col], sid))

    if not dry:
        # provenance union + audit note on survivor
        prov = set(filter(None, (S['source_provenance'] or '').split(','))) | \
               set(filter(None, (L['source_provenance'] or '').split(',')))
        merged_note = ((S['notes'] + ' | ') if S['notes'] else '') + f"merge {loser_dd}: {note}"
        conn.execute("UPDATE entities SET source_provenance=?, notes=?, updated_at=? WHERE entity_id=?",
                     (','.join(sorted(prov)), merged_note, now, sid))

        # re-point aliases (PK entity_id+alias_name+alias_source: insert-or-ignore then delete)
        for a in conn.execute("SELECT alias_name, alias_normalized, alias_source FROM entity_aliases WHERE entity_id=?", (lid,)).fetchall():
            conn.execute("""INSERT OR IGNORE INTO entity_aliases
                (entity_id, alias_name, alias_normalized, alias_source) VALUES (?,?,?,?)""",
                         (sid, a['alias_name'], a['alias_normalized'], a['alias_source']))
            report['aliases_repointed'] += 1
        conn.execute("DELETE FROM entity_aliases WHERE entity_id=?", (lid,))

        # loser's canonical name keeps matching the survivor
        l_norm = normalize_name(L['canonical_name'])
        if l_norm and l_norm != normalize_name(S['canonical_name']):
            conn.execute("""INSERT OR IGNORE INTO entity_aliases
                (entity_id, alias_name, alias_normalized, alias_source)
                VALUES (?,?,?,'manual_legal_name')""", (sid, L['canonical_name'], l_norm))
            report['name_alias_added'] = L['canonical_name']

        # re-point relationships, dropping would-be self-references
        conn.execute("DELETE FROM entity_relationships WHERE parent_entity_id=? AND child_entity_id=?", (lid, sid))
        conn.execute("DELETE FROM entity_relationships WHERE parent_entity_id=? AND child_entity_id=?", (sid, lid))
        conn.execute("UPDATE entity_relationships SET parent_entity_id=? WHERE parent_entity_id=?", (sid, lid))
        conn.execute("UPDATE entity_relationships SET child_entity_id=? WHERE child_entity_id=?", (sid, lid))

        # re-point entity_dominance — it feeds the phase_c6 tiebreaker
        # attachments under the #75 silent-pick invariant; a merged entity_id
        # left here would be a silent arbitrary pick downstream. (Checked
        # clean for the first two merges; structural for the gleif program.)
        try:
            conn.execute("UPDATE entity_dominance SET entity_id=? WHERE entity_id=?", (sid, lid))
        except sqlite3.OperationalError:
            pass  # table absent in some scratch DBs

        conn.execute("""UPDATE entities SET status='merged', merged_into_entity_id=?, updated_at=?
                        WHERE entity_id=?""", (sid, now, lid))
        conn.execute("""INSERT OR REPLACE INTO dd_id_redirects
                (old_dd_id, new_dd_id, reason, merged_at, resolution_note)
                VALUES (?,?,'merge',?,?)""", (loser_dd, survivor_dd, now, note))
        conn.commit()
    return report


def main():
    argv = [a for a in sys.argv[1:] if a != '--dry-run']
    dry = '--dry-run' in sys.argv
    if len(argv) != 4:
        raise SystemExit(__doc__)
    db_path, loser_dd, survivor_dd, note = argv
    conn = sqlite3.connect(db_path)
    # reversibility snapshot of both rows + loser aliases
    conn.row_factory = sqlite3.Row
    snap = {
        'ts': time.strftime('%Y%m%d-%H%M%S'),
        'rows': {dd: dict(conn.execute("SELECT * FROM entities WHERE dd_id=?", (dd,)).fetchone() or {})
                 for dd in (loser_dd, survivor_dd)},
        'loser_aliases': [dict(r) for r in conn.execute(
            "SELECT * FROM entity_aliases WHERE entity_id=(SELECT entity_id FROM entities WHERE dd_id=?)",
            (loser_dd,))],
    }
    if not dry:
        out = Path(__file__).parent / f"merge_snapshot_{loser_dd.replace('/','_')}_{snap['ts']}.json"
        out.write_text(json.dumps(snap, indent=1, default=str))
        print(f"snapshot: {out.name}")
    r = merge(conn, loser_dd, survivor_dd, note, dry=dry)
    print(("DRY-RUN " if dry else "") + json.dumps(r, indent=1))


if __name__ == "__main__":
    main()
