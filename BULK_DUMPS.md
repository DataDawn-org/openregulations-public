# Bulk dumps — what persists, what doesn't, and how to cite

`dumps.datadawn.org` publishes the full databases as gzipped SQLite files. This page exists
because a bulk artifact makes a promise the serving layer does not: that someone can come back
later and get **the same bytes**. Below is exactly which parts of that promise we keep.

> ⚠ **STATUS, 2026-09-06 — the monthly tier is NOT LIVE YET.** The bucket currently carries a
> **bucket-wide 28-day delete rule**, so today everything is reaped at 28 days regardless of
> prefix, and only `manifest_history.jsonl` (rewritten weekly, so its age resets) and the git
> copy of it persist. The layout and retention below describe the committed design and take
> effect when that rule is re-scoped to the `weekly/` prefix. **Until then, do not rely on a
> monthly archive being present.** This file ships with the rule change, not ahead of it.

## The two retention tiers, and why they differ

| tier | prefix | retained | citable? |
|---|---|---|---|
| **Monthly archive** | `YYYY-MM/` | **12 months** | ✅ **cite this** |
| Weekly snapshot | `YYYY-Www/` | ~3 weeks, then deleted | ❌ ephemeral by design |
| Manifest history | `manifest_history.jsonl` | **every week, forever** | identification only |

**Cite the monthly archive.** A weekly prefix is a convenience for people who want the freshest
possible extract; it is deleted within about three weeks and **will not be there when a reader
follows your citation**.

> **Why this is spelled out.** The manifest history records every weekly vintage, so you can
> always *prove* which copy you hold. If we stopped there we would have built a citation trap:
> you could demonstrate you had `2026-W37` and no one — including us — could ever obtain it
> again. The monthly tier exists so that a citation resolves to something a reader can actually
> download.

## Citing a vintage

    DataDawn OpenRegs database, monthly archive 2026-09.
    https://dumps.datadawn.org/2026-09/openregs.db.gz
    sha256 (gzipped): <from 2026-09/manifest.json>

Cite the **month**, the **filename**, and the **sha256 of the gzipped artifact**. The sha256 is
what makes the citation checkable: it is stable, it is in the manifest, and it is the identifier
that survives a download.

## Verifying a copy you already have

    sha256sum openregs.db.gz

Match that against `manifest_history.jsonl` — one JSON line per week, cumulative, never pruned:

    curl -s https://dumps.datadawn.org/manifest_history.jsonl \
      | grep -F "<your sha256>"

**This works even for a weekly vintage whose files are long gone.** Identification and
availability are separate guarantees here: we can always tell you *what* you hold; we can only
re-supply the monthly ones.

## Layout

    manifest.json                 current pointer — the most recent weekly
    (pre-2026-09 entries in manifest_history.jsonl predate the prefix restructure
     and used a FLAT layout: <week_id>/ rather than weekly/<week_id>/. The sha256
     identifies them either way; only the URL shape changed.)
    manifest_history.jsonl        every week ever published (cumulative)
    manifests/<week_id>.json      per-week manifest, kept outside the reaped prefix
    YYYY-MM/                      MONTHLY ARCHIVE — retained 12 months, cite these
    YYYY-Www/                     weekly — deleted after ~3 weeks

⚠ **Artifacts live under a prefix, not at the root.** `dumps.datadawn.org/openregs.db.gz`
is a 404; the file is at `dumps.datadawn.org/2026-09/openregs.db.gz`. The manifest lists
`filename` only, so prepend the archive you want.

## What is in a dump

Whatever the database held at build time — no additional filtering beyond what the served
surfaces already apply. Two artifacts carry documented divergences from the served copy:
`lobbying.db.gz` withholds entity-attribution columns, and `open_comments.db.gz` carries a
`grain_note` in the manifest describing a mid-correction column. Read the manifest entry for the
artifact you are using.

`build_metadata` inside each database records when that database was built. Where a source has
its own vintage (the IRS Business Master File, for instance), that vintage is recorded in its own
table — `bmf_source_meta` — because a corpus build date and a source as-of date are different
facts and conflating them is how a stale figure gets cited as current.
