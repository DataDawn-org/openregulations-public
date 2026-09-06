# OpenRegs Changelog

Public changelog for the OpenRegs database and serving layer at `regs.datadawn.org`. New entries on top.

---

## 2026-09-06 — Comment deadlines: one-day overstatement, and a filter that kept closed dockets

### Summary

Comment-period deadlines on `regs.datadawn.org` were shown **one day later than the official
deadline**, and the "currently open for comment" filter kept dockets that had **already
closed** for up to ~20 hours. Both are fixed. If you have custom SQL against
`documents.comment_end_date`, or you copied the query pattern from our API documentation,
**your query is affected** — see "What changed for SQL users".

Overstating a deadline is the dangerous direction: someone relying on the displayed date
files on what looks like the last day, and the window has already shut.

### The bug

Regulations.gov expresses an 11:59:59 PM **Eastern** deadline as a **UTC instant**:
`2026-08-29T03:59:59Z` is 11:59:59 PM ET on **August 28**. Two separate defects followed
from that, in two different databases.

**1. The UTC calendar date is one day later than the ET deadline.** Every end-of-day close
crosses midnight UTC, so reading the date portion gives tomorrow. This is not occasional:
of 444,690 documents with a comment end date, **444,689 close at end-of-ET-day** (03:59:59Z
under EDT, 04:59:59Z under EST — the same 11:59:59 PM wall clock on both sides of the
daylight-saving boundary). The single exception is a 1753 sentinel value, not a real
deadline.

**2. `comment_end_date > datetime('now')` is a comparison between two different text
formats.** The column holds `2026-08-29T03:59:59Z`; `datetime('now')` produces
`2026-08-24 08:49:21`. SQLite compares these as text, and `'T'` (0x54) sorts after `' '`
(0x20) at position 11 — so a document whose comment period had already closed kept matching
until UTC rolled past its own calendar date. **This pattern was published in our own API
documentation**, so queries built against that guidance inherit it.

Worked example, measured: at 2026-08-25 12:00Z (8:00 AM Eastern on a Tuesday), the old
predicate returned **15 documents, all of which closed the previous night at 11:59:59 PM
Eastern**.

### What we fixed

1. **`open_for_comment` now compares instants, not text.** The predicate is
   `julianday(comment_end_date) > julianday('now')`. `julianday()` parses both the
   ISO-with-`Z` stored form and SQLite's own datetime form correctly, so the comparison is
   between two moments rather than two strings.

2. **The raw UTC instant KEEPS its existing name in this release — the rename was deferred.**
   Three canned queries return the full instant under the name `comment_end_date`, which
   invites reading it as a date. Renaming it to `comment_end_utc` was prepared and then held
   back deliberately: it is a consumer-visible change to result-set column names, and it will
   ship **together with** the corrected Eastern-date column rather than on its own, so that
   consumers face one naming change instead of two. **No result-set column name changes here.**
   The `open_for_comment` description now states the grain explicitly instead.

3. **Default sort on the open-comments `documents` table moved to
   `comment_end_datetime`.** A calendar date cannot order two closes on the same day.
   Ordering is equivalent in practice — every close is end-of-day — but the column named in
   the sort changed, so a consumer pinned to the old name gets an error rather than a
   silently different order.

4. **Displayed dates convert to Eastern.** Surfaces render the close date by timezone
   conversion from the full instant, never by subtracting a day. A `-1 day` shortcut would
   hard-code the current all-closes-at-end-of-day distribution and break on the first
   mid-day close.

5. **Our own documentation stopped teaching the broken pattern.** The API schema map and the
   AI-surface schema file (`llms_openregs.txt`) documented `WHERE > datetime('now')`. Both
   now state the grain and the correct predicate.

### What changed for SQL users

**If you filter on `comment_end_date` with a text comparison** — this silently includes
already-closed dockets:

```sql
-- ❌ WRONG: text comparison between 'YYYY-MM-DDTHH:MM:SSZ' and 'YYYY-MM-DD HH:MM:SS'.
--    Keeps documents that closed up to ~20 hours ago.
SELECT id, title FROM documents
WHERE comment_end_date > datetime('now') AND withdrawn = 0;

-- ✅ RIGHT: compare instants.
SELECT id, title FROM documents
WHERE julianday(comment_end_date) > julianday('now') AND withdrawn = 0;
```

**If you display `comment_end_date` as a deadline** — it is a UTC instant, and its date
portion is one day after the Eastern deadline:

```sql
-- The official ET deadline as a calendar date. Convert; do not subtract a day.
SELECT id,
       comment_end_date                                   AS close_utc_instant,
       date(comment_end_date, '-4 hours')                 AS close_et_date_EDT_ONLY
FROM documents
WHERE comment_end_date IS NOT NULL;
```

⚠️ **The `-4 hours` form above is correct only during EDT, and it is wrong on 36% of this
corpus.** SQLite has no timezone database, so there is no DST-aware conversion available in
pure SQL. Of 444,690 documents with a comment end date, **161,866 (36.4%) are EST-era and
need `-5`**.

**And the failure is silent.** Applying `-4` to an EST-era row is off by one hour, which
looks fine almost every time — until the close instant sits within that hour, and then the
date rolls and you are back to reporting a deadline one day late. That is the same
invisible, off-by-a-day failure this entry exists to fix, reintroduced by the workaround.

**So: if your consumer can convert timezones — any application language can — pass the raw
UTC instant out of SQL and convert it there.** That is what our own surfaces do, and it is
why they convert by timezone rather than by subtracting a fixed offset.

**If you select `comment_end_date` from `open_for_comment`, `search_documents`, or
`explore_docket_documents`** — **your result-set column names are UNCHANGED.** You still get
`comment_end_date`, and it still carries the full UTC instant. What changed is the *filter*:
`open_for_comment` no longer returns comment periods that have already closed. A rename to
`comment_end_utc` is coming in a later release, alongside a corrected Eastern-date column;
this entry will be updated when it does.

### If you use the bulk dumps

This section is for people holding a `.db.gz` from `dumps.datadawn.org`. **You will not see
any of the above** — a dump is a database file with no metadata, no column notes and no
release notes attached, so nothing in the file itself can tell you this.

**Affected: `open_comments.db`, table `documents`, column `comment_end_date`.**

**The grain change, and its direction.** In dumps generated **before 2026-09-06**, that
column holds the UTC-rolled calendar day **AFTER** the official Eastern deadline — it is
**one day LATE** on every end-of-day close. In dumps generated **on or after 2026-09-06**, it
holds the **Eastern calendar date**, which is the official deadline. Nothing else about the
column changes: same name, same type, same position. **The values simply differ by one day,
and no field in the file marks which grain you have.**

**Dumps published before that date keep the old grain permanently.** We do not and will not
rewrite published archives — a dated dump is a fixed record of what we served that week, and
silently editing one would be worse than the defect. If you are holding an older dump and
need the correct deadline, either re-download a current one or derive it yourself:

```sql
-- Correct in EVERY dump, old grain or new. comment_end_datetime was never truncated.
SELECT id, comment_end_datetime AS close_utc_instant
FROM documents
WHERE comment_end_datetime IS NOT NULL;
-- then convert close_utc_instant to America/New_York in your application language
-- and take the date. Do not subtract a fixed offset (see the EDT/EST warning above).
```

**`comment_end_datetime` is authoritative in every dump we have ever published**, before and
after this change. If you built on that column, you are unaffected and no action is needed.

**How to tell which grain your dump has**, without trusting the filename: pick any row with
both columns set and compare `substr(comment_end_datetime, 1, 10)` to `comment_end_date`. If
they are **equal**, you have the **old** grain (the date is the UTC day). If `comment_end_date`
is **one day earlier**, you have the **corrected** grain.

**The equivalent defect on the `openregs.db` side is different and needs no dump action** —
there `comment_end_date` always held the full UTC instant and still does; only the served
query predicate and the rendering were wrong, and neither ships inside a dump.

**If you rely on the open-comments table's default ordering** — it is now
`comment_end_datetime` rather than `comment_end_date`.

### Not changed

`comment_window_analysis` uses `comment_end_date` only inside `julianday()` arithmetic, which
was already correct, and is untouched. Previously-published dated bulk archives at
`dumps.datadawn.org` are immutable by design and retain the original column grain; this
change takes effect from the next dump forward.

---

## 2026-05-22 — Lobbying spending: schema redesign + correctness fix

### Summary

Lobbying spending totals on `regs.datadawn.org` were inflated 1–3× depending on client. Largest clients (AT&T, Google, Meta, Boeing, Comcast and similar) were the most-distorted. **Fixed.** This required a schema change to two tables; if you have custom SQL queries against the lobbying tables, see "What changed for SQL users" below.

### The bug

`lobbying_activities.income_amount` and `lobbying_activities.expense_amount` were filing-level values (one number per filing) replicated across every activity row for that filing. A filing with 5 activities had the same income amount appearing 5 times. Aggregating queries (`SUM(income_amount) GROUP BY client_name`) summed the same dollar 5 times, inflating headline totals by activity-count.

Top-line examples (canonical vs published, cumulative through 2026-05-22):

| Client | Canonical | Published (pre-fix) | Inflation |
|---|---|---|---|
| AT&T Services, Inc. | $29.6M | $58.8M | 1.98× |
| Google Inc. | $13.4M | $36.8M | 2.75× |
| Meta Platforms, Inc. | $18.8M | $50.9M | 2.71× |

Affected surfaces: `top_lobbying_clients_by_spending` canned query, `top_lobbying_firms`, `lobbying_by_issue_area`, `lobbying_spending_over_time`, `revolving_door`, all explore-page tabs under "Lobbying", and the `search_lobbying` MCP tool.

### What we fixed

1. **Moved income and expense columns to the right table.** `lobbying_filings` now carries `income_amount REAL` and `expense_amount REAL` (one row per filing, no replication). Both are nullable and XOR-populated: outside-firm filings report income from the client; in-house lobbyists report their own expenses; LD-1 registrations and many LD-203 contribution reports have neither.

2. **Removed the legacy `amount_reported` column.** It was `COALESCE(income, expenses)` which collapsed two different concepts (client→firm payment vs in-house spend) under one name. Switch to `income_amount`, `expense_amount`, or `COALESCE(income_amount, expense_amount)` depending on your intent.

3. **Removed `income_amount` and `expense_amount` from `lobbying_activities`.** Those columns lived at the wrong grain. The activity table now carries only per-activity attributes (issue codes, specific issues, government entities). For amounts, JOIN to `lobbying_filings` by `filing_uuid`.

4. **All public aggregators now filter to LD-2 quarterly activity reports.** "Lobbying spending" publicly means quarterly client-firm activity reports (the LD-2 family: `filing_type GLOB '[1234Q]*'`). LD-203 contribution reports include their own income field that semantically isn't client→firm lobbying payment; summing it alongside LD-2 double-counts for legacy clients. The `income_amount` column on `lobbying_filings` still carries LD-203 values faithfully; if you want to include LD-203 in a custom query, remove the `filing_type` filter.

### What changed for SQL users

**If you reference `lobbying_filings.amount_reported`** — switch to:

```sql
-- For client→firm payment totals (the "lobbying spending" concept):
SELECT client_name, SUM(income_amount) AS total
FROM lobbying_filings
WHERE filing_type GLOB '[1234Q]*' AND income_amount > 0
GROUP BY client_name;

-- For in-house lobbying expenses:
SELECT client_name, SUM(expense_amount) AS total
FROM lobbying_filings
WHERE filing_type GLOB '[1234Q]*' AND expense_amount > 0
GROUP BY client_name;

-- For the union (both classes combined):
SELECT client_name, SUM(COALESCE(income_amount, expense_amount)) AS total
FROM lobbying_filings
WHERE filing_type GLOB '[1234Q]*'
GROUP BY client_name;
```

**If you reference `lobbying_activities.income_amount` or `lobbying_activities.expense_amount`** — these columns no longer exist. JOIN to `lobbying_filings`:

```sql
SELECT la.client_name, la.issue_code, f.income_amount, la.specific_issues
FROM lobbying_activities la
JOIN lobbying_filings f ON f.filing_uuid = la.filing_uuid
WHERE la.client_name LIKE '%Boeing%' AND f.filing_type GLOB '[1234Q]*';
```

**If you have analyses that cited specific lobbying-spending totals from before 2026-05-22**, those totals were inflated 1–3×. The largest clients (AT&T, Google, Meta, Boeing, Comcast and similar) were the most-distorted. Re-run with the new schema for canonical figures.

### Why we made this change (rather than papering over)

The simpler patch would have been to NULL the activity-level columns on all but the first row per filing, leaving the table shape unchanged. We rejected that because the underlying bug is a **grain mismatch** — income is a filing-level fact, not an activity-level fact. Patching it in place would preserve the broken mental model and invite the same bug to recur. Dropping the misplaced columns and moving the amounts to the right table is the more disruptive fix today but the less drift-prone fix going forward.

### Audit and detection

The bug was found in a 2026-05-22 schema-fit + value-correctness audit (finding S-C1). The audit's initial enumeration identified 5 downstream consumer sites; the fix-session re-grep found 38 sites across 8 surfaces (scripts, MCP, canned queries, explore HTML, docs). That gap prompted a methodology commitment: audit reports must include grep-based blast-radius enumeration, and fix sessions must independently re-enumerate against a canonical surface list. Process improvement applies to future schema-fit audits across the project.

The bug was publicly present since at least the first commit of this public repository (2026-03-06); the broken schema predates that and may have been present in private precursor scripts.

### References

- `scripts/migrations/2026_05_22_lobbying_s_c1.py` (lobbying.db migration)
- `scripts/migrations/2026_05_22_lobbying_s_c1_openregs.py` (openregs.db migration)
- See PR #17 commit message + this file for the canonical narrative.

---
