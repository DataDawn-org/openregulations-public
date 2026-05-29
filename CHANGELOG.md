# OpenRegs Changelog

Public changelog for the OpenRegs database and serving layer at `regs.datadawn.org`. New entries on top.

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
