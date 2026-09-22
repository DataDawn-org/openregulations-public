# Bulk dumps — what persists, what doesn't, and how to cite

`dumps.datadawn.org` publishes the full databases as gzipped SQLite files. This page exists
because a bulk artifact makes a promise the serving layer does not: that someone can come back
later and get **the same bytes**. Below is exactly which parts of that promise we keep.

> **Every archive is a snapshot as of its build date, and our practice is not to reissue it.** If a
> comment is withdrawn or removed at regulations.gov *after* an archive was built, that archive
> still contains it. On the live site, its Datasette and API, and the MCP, we hide a comment once
> our own checks confirm it was withdrawn or removed at the source. The practice exists because a
> citation is only worth making if the thing cited stays put: an archive that could be rewritten
> later is one a reader cannot check anything against, including our own published numbers. So a
> dated archive is left as a record of what the corpus looked like on its date. Prefer a recent
> archive if you need the current picture.

## The two retention tiers, and why they differ

| tier | prefix | retained | citable? |
|---|---|---|---|
| **Monthly archive** | `monthly/YYYY-MM/` | **not rotated — retained** | ✅ **cite this** |
| Weekly snapshot | `weekly/YYYY-Www/` | 28 days, then deleted | ❌ ephemeral by design |
| *(legacy weeklies)* | `YYYY-Www/`, no prefix | kept indefinitely | ⚠ see below |
| Manifest history | `manifest_history.jsonl` | **every week, forever** | identification only |

**Monthly archives are not rotated.** The bucket's rotation rule is scoped to the `weekly/`
prefix and does not reach `monthly/`, so a monthly archive stays where a citation can find it.
Weekly snapshots, which are published under `weekly/`, are deleted 28 days after upload.

**A few older weekly archives sit at a bare `YYYY-Www/` path**, from before weekly snapshots moved
under the `weekly/` prefix. The rotation rule does not reach those either, so they are still there
and we are not deleting them. They are not citable in the sense this page means — nothing promises
they will stay — but if you already hold a reference to one, it currently resolves.

**Cite the monthly archive.** A weekly prefix is a convenience for people who want the freshest
possible extract; it is deleted 28 days after upload and **will not be there when a reader
follows your citation**. The handful of legacy bare-path weeklies are an accident of an old
layout, not a promise — do not build a citation on one.

> **Why this is spelled out.** The manifest history records every weekly vintage, so you can
> always *prove* which copy you hold. **Treat the bucket copy as a convenience mirror and the
> public git repository as the record** — the git copy is the one with a history.
> If we stopped at identification we would have built a citation trap:
> you could demonstrate you had `2026-W37` and no one — including us — could ever obtain it
> again. The monthly tier exists so that a citation resolves to something a reader can actually
> download.

## Citing a vintage

    DataDawn OpenRegs database, monthly archive 2026-09.
    https://dumps.datadawn.org/monthly/2026-09/openregs.db.gz
    sha256 (gzipped): <from monthly/2026-09/manifest.json>

Cite the **month**, the **filename**, and the **sha256 of the gzipped artifact**. The sha256 is
what makes the citation checkable: it is stable, it is in the manifest, and it is the identifier
that survives a download.

## Verifying a copy you already have

    sha256sum openregs.db.gz

Match that against `manifest_history.jsonl` — one JSON line per week, cumulative, never pruned:

    curl -s https://dumps.datadawn.org/manifest_history.jsonl \
      | grep -F "<your sha256>"

**This works even for a weekly vintage whose files are long gone.** Identification and
availability are different things here: we can always tell you *what* you hold, because the
history is permanent; we can only still be **serving** the monthly ones. That is a separate matter
from reissuing — serving is handing back the same bytes that have been there all along, which is
what a monthly archive is for.

## Layout

    manifest.json                 current pointer — the most recent weekly
    (pre-2026-09 entries in manifest_history.jsonl predate the prefix restructure
     and used a FLAT layout: <week_id>/ rather than weekly/<week_id>/. The sha256
     identifies them either way; only the URL shape changed.)
    manifest_history.jsonl        every week ever published (cumulative)
    manifests/<week_id>.json      per-week manifest, kept outside the reaped prefix
    monthly/YYYY-MM/              MONTHLY ARCHIVE — not rotated, cite these
    weekly/YYYY-Www/              weekly — deleted 28 days after upload
    YYYY-Www/                     legacy weekly, no prefix — reached by no rule, kept indefinitely

⚠ **Artifacts live under a prefix, not at the root.** `dumps.datadawn.org/openregs.db.gz`
is a 404; the file is at `dumps.datadawn.org/monthly/2026-09/openregs.db.gz`. The manifest
lists `filename` only, so prepend the archive you want.

**A monthly archive's `manifest.json` names monthly paths.** Each entry's `key` and `url` point at
that artifact's own `monthly/<YYYY-MM>/` location, so you can follow them directly; a top-level
`promoted_from` records the `weekly/<week_id>/` prefix the month was promoted from, which is
provenance rather than a place to fetch from — that weekly prefix is deleted 28 days after upload.

Earlier monthly manifests were byte copies of the week's and named `weekly/` paths in `key` and
`url`. **2026-09 was rewritten on 2026-09-22 and every month since is written this way.** Only
`key` and `url` changed: each artifact's `sha256_gzipped` and `size_bytes` are exactly as first
published, and the artifacts themselves were never touched.

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
