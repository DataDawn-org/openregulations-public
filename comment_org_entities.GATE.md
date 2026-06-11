# comment_org_entities launch gate

Ruling (maintainer, 2026-06-11): the ranking table ships ONLY on an affirmative GO
after human review of the pre-launch verification packet
(`working-docs/commenter_curation/verification_packet/`). **Absence of a GO
means HOLD** — the build drops/skips the table (explore page feature-detects →
unranked, user-invisible), and `validate_critical_tables` skips this table's
floor with a loud `GATE_HOLD` log line (deliberate hold ≠ silent loss; with a
GO present the floor enforces normally).

## To ship: create `openregs/comment_org_entities.GO`
```
mode=pinned
snapshot=/mnt/data/datadawn/working-docs/commenter_curation/verification_packet/coe_reviewed_snapshot_<date>_<hash8>.db
sha256=<full table hash printed in the packet README>
reviewed_by=<reviewer>
date=<review date>
seed=<the stratified-draw seed>
```
`mode=pinned` ships the reviewed snapshot VERBATIM, hash-verified — what was
reviewed is provably what deploys (a fresh rebuild would differ: the weekly
update pulls new comments before building). Hash mismatch → table dropped →
floor fires → deploy aborts → Pushover pager.

After launch week, switch to steady-state: replace the file with `mode=rebuild`
(fresh build from current data each run — the matching logic is what the
review validated; per-build pinning is launch-only).

## Mechanics
- Gate logic + shared hash: `scripts/build_comment_org_entities.py`
  (`read_gate` / `build_gated` / `table_hash` / `snapshot_table`)
- Build call site: `05_build_database.py` `build_comment_org_entities(conn)`
- Floor gate-awareness: `05_build_database.py` `validate_critical_tables`
- All four paths (hold / pinned / tampered / bad-mode) test-verified
  2026-06-11 on a temp DB.
