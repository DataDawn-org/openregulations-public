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

## Exit plan — pinned mode has a documented END, not an indefinite default
Pinned solves LAUNCH provenance only. Persisting it would let the surface go
stale against weekly rebuilds; the transition is part of the design:

1. **Launch**: `mode=pinned` ships the reviewed artifact (this file's main
   flow). Applies to the first deploy only.
2. **Steady state — flip to `mode=rebuild` at the first weekly build after a
   clean launch week** (set a date in the GO file when the GO is written).
   Post-launch tables rebuild fresh and unreviewed — acceptable BECAUSE the
   ranking asserts no identities. The protections that remain: the
   criticality floor (now enforcing, gate-aware), the matcher fixture suite
   (9 regression cases), and generator-time invariants on the curation side.
3. **Hard gate independent of mode**: any surface that ASSERTS identity
   (verified per-comment entity links/badges) requires the larger
   verification round first (n≈50–100 random entities clean; rule of three
   bounds the name-level error rate at ~3/n). Rankings may drift unreviewed;
   identity claims may not.

## Mechanics
- Gate logic + shared hash: `scripts/build_comment_org_entities.py`
  (`read_gate` / `build_gated` / `table_hash` / `snapshot_table`)
- Build call site: `05_build_database.py` `build_comment_org_entities(conn)`
- Floor gate-awareness: `05_build_database.py` `validate_critical_tables`
- All four paths (hold / pinned / tampered / bad-mode) test-verified
  2026-06-11 on a temp DB.
