# OpenRegulations

A comprehensive database of U.S. federal government data: regulations, legislation, congressional floor speeches, committee hearings, CRS reports, executive nominations, treaties, GAO reports, lobbying disclosures, campaign finance, stock trades, foreign agent registrations, and federal spending. All from official government sources. All public domain.

Built by a human, [Claude](https://www.anthropic.com/claude) (Anthropic), and DJ Crabdaddy ([Claude Code](https://docs.anthropic.com/en/docs/claude-code)) 🦀

**Live instance**: https://regs.datadawn.org/
**Explore pages**: https://regs.datadawn.org/explore/
**Build command**: `python3 scripts/05_build_database.py`
**Deploy command**: `bash deploy/deploy.sh`

## Data at a Glance

| Dataset | Records | Source |
|---------|---------|--------|
| Federal Register documents | 993,703 | Federal Register API |
| Regulatory dockets | 254,397 | Regulations.gov API + derived from documents/comments |
| Regulatory documents | 1,703,711 | Regulations.gov API |
| Public comment headers | 9,669,365 | Regulations.gov API |
| Comment full details | 423,837 | Regulations.gov API (4.4% of headers, in progress) |
| Presidential documents | 5,918 | Federal Register API |
| Congressional legislation | 364,559 | GovInfo BILLSTATUS + Congress.gov API |
| Legislation actions | 1,100,541 | GovInfo BILLSTATUS |
| Legislation subjects | 1,513,256 | GovInfo BILLSTATUS |
| Legislation cosponsors | 2,070,889 | GovInfo BILLSTATUS |
| CFR regulatory sections | 123,480 | GovInfo eCFR bulk XML |
| Congressional Record entries | 878,583 | GovInfo CREC packages |
| CREC speakers (bioguide-linked) | 944,216 | Derived from MODS XML |
| CREC bill references | 1,561,719 | Derived from MODS XML |
| Roll call votes | 26,359 | Congress.gov API |
| Member vote records | 8,315,224 | Congress.gov API |
| Congressional committees | 233 | congress-legislators GitHub |
| Committee memberships | 3,908 | congress-legislators GitHub |
| Congress members | 12,763 | congress-legislators GitHub |
| Stock trading disclosures | 61,148 | Senate eFD + House FD PTR PDFs (gov). PTR transactions only |
| Lobbying filings | 1,908,114 | Senate LDA API |
| Lobbying lobbyists | 4,376,087 | Senate LDA API |
| Lobbying activities | 3,528,264 | Senate LDA API |
| Lobbying contributions | 3,492,672 | Senate LDA API |
| FARA registrants | 7,035 | FARA.gov |
| FARA foreign principals | 17,627 | FARA.gov |
| FARA registrant documents | 151,348 | FARA.gov |
| FARA short forms | 44,363 | FARA.gov |
| FEC candidates | 64,679 | FEC bulk data |
| FEC committees | 154,967 | FEC bulk data |
| FEC contributions | 4,395,926 | FEC bulk data |
| FEC operating expenditures | 15,358,447 | FEC bulk data |
| FEC independent expenditures | 666,910 | FEC bulk data |
| FEC PAC summary | 98,614 | FEC bulk data |
| Federal spending awards | 863,632 | USAspending.gov API |
| FR agencies | 444 | Federal Register API |
| APHIS facilities | 15,119 | APHIS Salesforce API |
| APHIS inspections | 110,400 | APHIS Salesforce API |
| Committee hearings | 46,177 | GovInfo CHRG collection |
| Hearing witnesses | 109,242 | GovInfo CHRG collection |
| Hearing member attendance | 1,244,920 | GovInfo CHRG collection |
| CRS reports | 13,629 | Congress.gov API |
| CRS report–bill cross-references | 135,890 | Congress.gov API |
| Executive nominations | 40,067 | Congress.gov API |
| Nomination actions | 189,972 | Congress.gov API |
| Treaties | 777 | Congress.gov API |
| Treaty actions | 4,286 | Congress.gov API |
| GAO reports | 73,725 | GovInfo GAOREPORTS + gao.gov direct (1989–present) |
| OIRA regulatory reviews | 48,434 | Reginfo.gov |
| OIRA review meetings | 8,663 | Reginfo.gov |
| OIRA meeting attendees | 90,711 | Reginfo.gov |
| IG reports | 34,880 | oversight.gov |
| IG recommendations | 11,999 | oversight.gov |
| Earmarks | 70,826 | House/Senate Appropriations |
| Lobbying bills (parsed) | 3,483,171 | Derived from lobbying specific_issues text |
| CBO cost estimates | ~17,200 | Congress.gov API (from bill data) |
| FR ↔ Regs.gov cross-references | 185,900 | Derived |

**Total**: ~120 million rows across 188 tables.

**Comment coverage by agency**: 9.7M comment headers across EPA, FDA, USDA, FWS, APHIS, and expansion agencies.

---

## Interactive Explore Pages

The live instance includes 24 interactive explore pages at `regs.datadawn.org/explore/`, powered by a shared utility library (`shared.js`) for consistent styling and data formatting:

| Page | URL | Description |
|------|-----|-------------|
| Explore Home | `/explore/index.html` | Overview dashboard with search and data stats |
| Members | `/explore/member.html` | 12,700+ Congress members: stock trades, floor speeches, legislation, votes, donors, committees |
| Regulations | `/explore/regulation.html` | Federal rulemaking: dockets, documents, comments, CFR text, cross-references |
| Legislation | `/explore/legislation.html` | 364K+ bills: sponsors, cosponsors, actions, subjects, related floor speeches |
| Lobbying | `/explore/lobbying.html` | 1.9M+ lobbying filings: clients, registrants, issue areas, government entities |
| Contributions | `/explore/contributions.html` | Campaign finance: employer donations, candidate fundraising, party flows |
| FARA | `/explore/fara.html` | Foreign agent registrations: principals, countries, registrant documents |
| Hearings | `/explore/hearings.html` | Committee hearings: witnesses, member attendance, hearing search |
| Nominations | `/explore/nominations.html` | Executive nominations: confirmation rates, agency positions, vote tallies |
| Research | `/explore/research.html` | Cross-domain research: speeches near trades, committee trade conflicts, revolving door, lobbying-legislation overlap |
| Entity | `/explore/entity.html` | Cross-domain entity search: find any person/org across all datasets |
| Speech | `/explore/speech.html` | Congressional Record full text: speakers, referenced bills, same-day entries |
| API | `/explore/api.html` | API documentation and interactive query builder |
| Witness-Lobby Overlap | `/explore/witness-lobby.html` | Organizations that testify AND lobby Congress |
| Commenter-Lobby Overlap | `/explore/commenter-lobby.html` | Organizations that comment on rules AND lobby |
| Committee Stock Trading | `/explore/committee-trades.html` | Stock trades by committee members |
| Committee Donors | `/explore/committee-donors.html` | PAC donations to committee members |
| Speeches Near Trades | `/explore/speeches-trades.html` | Floor speeches within 7 days of trades |
| Committee Trade Conflicts | `/explore/trade-conflicts.html` | Trading in sectors committees regulate |
| Most Lobbied Bills | `/explore/lobbied-bills.html` | Bills with most lobbying activity |
| The Revolving Door | `/explore/revolving-door.html` | Former members who became lobbyists |
| CBO Cost Estimates | `/explore/cbo.html` | CBO cost estimates linked to legislation |
| FARA Hearings Overlap | `/explore/fara-hearings.html` | Foreign agents who testified at congressional hearings |
| Meta Search | `/explore/search.html` | Cross-dataset search across all tables and explore pages |
| Treaties | `/explore/treaties.html` | Treaty metadata, countries, Senate actions |

All pages use the same DataDawn dark theme and share a common `shared.js` utility library (15 functions for formatting, search, pagination, and chart rendering). Data is queried live from the Datasette API.

---

## Source

**Federal Register API**
- URL: https://www.federalregister.gov/developers/api/v1
- Free, public, no API key needed.
- All document types: Rules, Proposed Rules, Notices, Presidential Documents.
- Coverage: 1994–2026.
- Accessed: March 1–2, 2026.

**Regulations.gov API v4**
- URL: https://api.regulations.gov/v4
- Requires a free API key (1,000 requests/hour rate limit).
- Provides dockets, documents, and comment headers.
- Accessed: March 1–2, 2026.

**GovInfo Bulk Data**
- URL: https://www.govinfo.gov/bulkdata
- Free, public, no API key needed.
- eCFR: Current regulatory text for 19 CFR titles (7, 9, 10, 14, 15, 17, 20, 21, 24, 28, 29, 33, 34, 38, 40, 44, 46, 49, 50).
- Congressional Record (CREC): Floor proceedings, 1994–present.
- BILLSTATUS: Congressional legislation metadata.

**Congressional Financial Disclosures**
- Senate eFD: https://efdsearch.senate.gov — Periodic Transaction Reports (PTRs).
- House FD: https://disclosures-clerk.house.gov — PTR PDFs parsed for transaction-level data.
- Free, public, no API key needed.

**Senate Lobbying Disclosure Act (LDA)**
- URL: https://lda.senate.gov/api/
- Requires a free API key.
- Lobbying filings, lobbyists, activities by issue area, contributions.
- Coverage: 1999–present.

**FARA (Foreign Agents Registration Act)**
- URL: https://efile.fara.gov/
- Free, public, no API key needed.
- Registrants, foreign principals, registration documents, short forms.

**FEC Campaign Finance**
- URL: https://www.fec.gov/data/browse-data/
- Free bulk data downloads, no API key needed.
- Candidates, committees, individual contributions.
- Coverage: 2004–2024 election cycles.

**Congress.gov Votes API**
- URL: https://api.congress.gov/v3/
- Requires a free API key.
- Roll call votes with individual member positions.
- Coverage: 101st–119th Congress (1989–present).

**GovInfo CHRG Collection (Committee Hearings)**
- URL: https://api.govinfo.gov/published?collection=CHRG
- Free, public, requires a free GovInfo API key.
- Committee hearing metadata, witness lists, member attendance.
- Coverage: 1993–present.

**Congress.gov CRS Reports API**
- URL: https://api.congress.gov/v3/crsreport
- Requires a free API key.
- CRS report metadata, summaries, author info, related legislation.

**Congress.gov Nominations API**
- URL: https://api.congress.gov/v3/nomination
- Requires a free API key.
- Executive nominations: nominee info, position, agency, confirmation status, action history.
- Coverage: Congresses 100–119.

**Congress.gov Treaties API**
- URL: https://api.congress.gov/v3/treaty
- Requires a free API key.
- Treaty metadata, countries/parties, index terms, Senate resolution text, action history.
- Coverage: Congresses 89–119.

**GovInfo GAOREPORTS Collection (GAO Reports)**
- URL: https://api.govinfo.gov/published?collection=GAOREPORTS
- Free, public, requires a free GovInfo API key.
- GAO report metadata, abstracts, legal references (USC, public laws, statutes at large), subject classifications.
- Coverage: 1989–2008 (GovInfo); 2009–present (gao.gov direct).

**OIRA Regulatory Reviews (Reginfo.gov)**
- URL: https://www.reginfo.gov/
- Free, public, no API key needed.
- Executive Order 12866 regulatory review data: reviews, meetings, attendees.
- Coverage: 1981–present.

**Inspector General Reports (oversight.gov)**
- URL: https://www.oversight.gov/
- Free, public, no API key needed.
- IG report metadata, recommendations, questioned costs.
- Coverage: varies by agency (generally 2010–present).

**SEC EDGAR (Ticker-SIC Mapping)**
- URL: https://www.sec.gov/files/company_tickers.json
- Free, public, requires `User-Agent` header.
- Maps stock tickers to SIC industry codes via EDGAR company filings. Used for committee trade conflict detection.

**License**
- All U.S. government data. Public domain. No copyright restrictions.

---

## Prerequisites & Setup

**Python dependencies** (all stdlib except `requests`):
```
pip install requests
```

**API keys**:
1. **Regulations.gov**: Register at https://open.gsa.gov/api/regulationsgov/ (free, instant).
2. **Congress.gov**: Sign up at https://api.congress.gov/sign-up/ (free, instant).
3. **Senate LDA** (lobbying): Register at https://lda.senate.gov/api/ (free).
4. Save all keys in `scripts/config.json`:
   ```json
   {
     "regulations_gov_api_key": "YOUR_KEY_HERE",
     "congress_gov_api_key": "YOUR_KEY_HERE",
     "lda_api_key": "YOUR_LDA_KEY_HERE"
   }
   ```
   The Federal Register, USAspending.gov, FARA, and FEC APIs require no key.

**Deployment** (optional):
- See `deploy/deploy.sh` for the deployment template (Datasette + Caddy reverse proxy).
- Set `REMOTE_HOST` in the script to your server's address.
- SSH key required for deploy; not needed for data collection or database builds.

---

## Pipeline: Running from Scratch

Scripts are numbered in execution order. Run from the project root:

```
1. python3 scripts/01_federal_register.py           # ~30 min, no API key needed
2. python3 scripts/02_regs_gov_dockets_docs.py      # ~4-6 hours, needs API key
3. python3 scripts/03_regs_gov_comments.py          # ~30-50 hours (all agencies — EPA/FDA/USDA/FWS/APHIS; consolidated from old 03+04+06 on 2026-03-27)
4. python3 scripts/07_full_comment_details.py       # weeks (optional, full text)
5. python3 scripts/08_usaspending.py                # ~2 hours (federal spending)
6. python3 scripts/09_congress_gov.py --full         # ~50 min (congressional legislation, congresses 93-119)
7. python3 scripts/10_ecfr.py                       # ~30 sec (eCFR regulatory text, 5 CFR titles)
8. python3 scripts/11_congressional_record.py       # ~6-8 hours (Congressional Record, 1994-present)
9. python3 scripts/12_congress_stock_trades.py      # ~3 hours (Senate eFD + House PTR + FD indexes; consolidated from old 12+13+14 on 2026-03-27)
10. python3 scripts/15_lobbying_disclosure.py       # ~2 hours (lobbying disclosures from Senate LDA API)
11. python3 scripts/17_crs_reports.py               # CRS reports from Congress.gov API
12. python3 scripts/18_nominations_treaties.py      # executive nominations + treaties from Congress.gov API
13. python3 scripts/19_gao_reports.py               # GAO reports from GovInfo GAOREPORTS
14. python3 scripts/05_build_database.py            # ~25 min (builds from raw JSON + APIs)
15. bash deploy/deploy.sh                           # ~30 min (uploads to server)
```

Steps 1-13 download raw data and are idempotent (safe to re-run; state files track progress). Step 14 builds the database from whatever raw data exists. You can run step 14 at any point to get a database from partial data.

**Note (2026-03-27 consolidation)**: Scripts 04 and 06 were merged into `03_regs_gov_comments.py`. Scripts 13 and 14 were merged into `12_congress_stock_trades.py`. The weekly and monthly update orchestrators (`weekly_update.sh`, `monthly_update.sh`) also run additional sources (FARA, FEC, OIRA, IG reports, GAO direct). See `PIPELINE.md` for the canonical pipeline manifest.

**Key architectural constraint**: The Regulations.gov API returns a maximum of **5,000 results per query** (20 pages × 250 per page). This drives the entire download strategy:
- Script 03 queries by year, then subdivides to months if a year exceeds 5,000
- Script 04 further subdivides to days for months that still exceed 5,000
- Script 06 does the same but also handles day-level subdivision inline
- Without this subdivision, large agencies like EPA (which has months with 10,000+ comments) would silently lose data

**The full-detail endpoint** (`/v4/comments/{id}`) returns 45+ fields per comment (full text, structured name, location, organization, attachments) vs. the search endpoint's 8 fields. Scripts 03/06 use the fast search endpoint to get headers in bulk; script 07 fetches full details one-by-one. At 1,000 req/hr, full details for 3M+ comments would take months — so headers-first is the practical strategy.

### Resetting State / Re-downloading

State files in `logs/` track completed work. To re-download data:

- **Re-download a specific agency's comments**: Delete matching entries from `logs/regs_comments_state.json`. Entries are keyed as `"comments:AGENCY:YEAR": count`. Delete the entries for the years you want to re-pull, then re-run script 03 or 06.
- **Re-run backfill**: Delete `logs/regs_comments_backfill_state.json` entirely.
- **Re-download full details**: Delete `logs/full_comments_state.json` and the batch files in `regulations_gov/comments/details/`.
- **Full reset**: Delete all state files and raw data directories, then re-run from step 1.

---

## Monitoring (Synthetic Site Audit)

`scripts/site_audit.py` runs a weekly synthetic-monitoring pass against the live deployment. It's what catches:

- Broken external links to `govinfo.gov`, `federalregister.gov`, `regulations.gov`, etc. — sampled randomly from recent DB rows each week, so coverage rotates
- FTS tables that became empty or missing after a deploy (the class of bug that keeps silent pipelines silent)
- Detail-page template regressions (random sample of rows per table, verifies the page renders with >2 KB of HTML)
- Datasette health (both DBs attached, canned queries return non-empty results)
- 990 form viewer renders on Cloudflare R2
- TLS cert expiry (<14 d = warning, <7 d = critical)
- Stale Federal Register data (latest published doc older than expected)

Runs in ~2 minutes end-to-end. Tiered severity so transient external flakiness (regulations.gov sometimes 403s automated requests, govinfo occasionally 5xxs) doesn't page anyone — only genuinely user-visible breakage turns the check red.

Driven by `scripts/run_site_audit.sh` from cron:

```bash
0 9 * * 0 /path/to/scripts/run_site_audit.sh <healthchecks.io-uuid>
```

The wrapper tees output to a rolling log and posts the summary body to healthchecks.io so pass/fail counts plus the specific failing URLs appear in the hc.io dashboard — no SSH needed to triage. See `scripts/site_audit.py --help` for the full list of checks.

---

## APHIS Animal Welfare Sub-Project

A separate extraction pipeline for the USDA APHIS Animal Care Public Search Tool, which uses a Salesforce Aura API (not Regulations.gov). Lives in `aphis/`.

**What it extracts**: Inspection reports (~93K records) and annual research facility reports (~10K records) from APHIS's Salesforce-based search tool at `https://aphis.my.site.com/PublicSearchTool/s/`.

**Scripts** (in `aphis/scripts/`):

| Script | Purpose |
|--------|---------|
| `lib/aura_client.py` | Shared Salesforce Aura API client (fwuid extraction, pagination, rate limiting) |
| `01_discover_actions.py` | Probe API to find action names for each record type |
| `02_extract_all.py` | Bulk extraction by character prefix (a-z, 0-9) with subdivision |
| `03_download_licensee_list.py` | Download APHIS active licensee list (PDF/Excel) |
| `04_download_pdfs.py` | Download inspection/enforcement report PDFs |
| `05_build_database.py` | Build `aphis/db/aphis.db` from raw JSON |
| `run_aphis.sh` | Master runner with `--skip-pdfs`, `--pdfs-only`, `--db-only` flags |

**Key technical detail**: The Aura API has a 2,100 result limit per query. The extraction script works around this by searching letter-by-letter on customer name (a, b, c...), and recursively subdividing to two-character prefixes (aa, ab, ac...) when a single letter returns >2,100 results.

**Status**: Inspection and annual report extraction confirmed working. Enforcement actions, teachable moments, and licensee search action names were not discovered (the API probe found `doIRSearch_UI` for inspections and `doARSearch` for annual reports, but enforcement/teachable moments use unknown action names).

---

## Raw Data File Locations

All raw data lives under the project root. The database builder reads from these directories:

| Directory | Contents | Size |
|-----------|----------|------|
| `federal_register/raw/` | Paginated JSON from FR API (monthly date windows, 1994–2026) | 1.5 GB |
| `regulations_gov/dockets/` | Docket JSON by agency | 6.3 MB |
| `regulations_gov/documents/` | Document JSON by agency and year | 669 MB |
| `regulations_gov/comments/headers/` | Comment header JSON by agency/year/month | 1.1 GB |
| `regulations_gov/comments/details/` | Full comment detail JSON (batch files, in progress) | — |
| `congress_gov/` | BILLSTATUS XML and bill listing cache | ~200 MB |
| `ecfr/` | eCFR title XML files and parsed JSON | ~500 MB |
| `congressional_record/` | CREC daily packages (HTML text + MODS XML) | ~10 GB |

**Directory layout for comment headers:**
```
regulations_gov/comments/headers/
├── EPA/
│   ├── 2009/
│   │   ├── 09/
│   │   │   ├── page_0001.json        # Normal month (≤5,000 results)
│   │   │   ├── d01/page_0001.json    # Daily backfill for truncated months
│   │   │   └── d02/page_0001.json
│   │   └── 10/
│   │       └── page_0001.json
│   └── 2020/
│       └── ...
├── FDA/
│   └── ...
├── USDA/
│   └── ...
├── FWS/
│   └── ...
└── APHIS/
    └── ...
```

**State files** (track download progress for resume):
- `logs/regs_comments_state.json` — completed year/agency combos for Phase 3 headers
- `logs/regs_comments_backfill_state.json` — completed daily backfills
- `logs/full_comments_state.json` — downloaded comment detail IDs

---

## Processing

### Federal Register
- Paginated through monthly date windows, January 1994 through 2026.
- 994,487 documents total.
- No records dropped.

### Dockets
- Downloaded for multiple agencies via Regulations.gov API + expansion.
- 254,397 dockets total (API downloads + stub dockets derived from documents/comments).
- The Regulations.gov docket endpoint caps at 5,000 results (20 pages × 250/page). Stub records (id + agency_id) are backfilled during the database build step to ensure referential integrity.

### Documents
- Downloaded by agency and year.
- 1,703,711 documents across all covered agencies.

### Comment Headers
- Downloaded for EPA, FDA, USDA, FWS, APHIS, and expansion agencies.
- 9,669,365 headers in current build (up from ~3.9M before expansion).
- Rate limited at 1,000 requests/hour per API key.
- EPA months exceeding 5,000 results were backfilled using daily date windows, recovering ~308,000 comments across 78 truncated months.

### Enrichments Applied During Database Build
- **Submitter name parsing**: Names parsed from comment titles using regex patterns (e.g., "Comment from X", "Comment submitted by X"). Successfully parsed submitter names for the majority of comments.
- **Submitter type classification**: Each comment classified as `individual`, `organization`, `anonymous`, or `unknown` based on name heuristics.
- **Docket ID extraction**: Docket ID derived from comment IDs by stripping the last numeric segment.
- **Cross-reference table** (5-phase):
  1. Direct match on `fr_doc_num` = `document_number` (61,059 links)
  2. Normalized match — strip spaces, convert en-dashes/em-dashes to hyphens (+283 links)
  3. Leading-zero strip — match `E5-05178` to `E5-5178` format differences (+6,515 links)
  4. Date + title match for page-number-style `fr_doc_num` values (+535 links)
  5. Case-insensitive date + title match for documents without `fr_doc_num` (+1,638 links)
  6. Total: 70,030 cross-references
- **Agency mapping table**: Maps Regulations.gov agency codes (EPA, FDA, USDA, FWS, APHIS) to Federal Register agency IDs.
- **Date decomposition**: Year and month columns extracted from dates for time-series queries.
- **Analytical views**: `docket_stats`, `comments_by_year`, `fr_by_year`, `top_dockets`, `regulatory_pipeline`, `comments_monthly`, `agency_overview`, `top_submitters`.

### Deduplication
- `INSERT OR IGNORE` on primary keys throughout. No explicit deduplication needed; IDs are unique from source.

---

## Schema

### federal_register

Every document published in the Federal Register, 1994–2026.

| Column | Description |
|---|---|
| `document_number` (PK) | Unique FR document number (e.g., `2016-20442`) |
| `title` | Document title |
| `type` | `Rule`, `Proposed Rule`, `Notice`, or `Presidential Document` |
| `abstract` | Summary text |
| `publication_date` | Publication date (`YYYY-MM-DD`) |
| `pub_year` | Extracted integer year for filtering |
| `pub_month` | Extracted integer month for filtering |
| `html_url` | Link to full text on federalregister.gov |
| `pdf_url` | Link to PDF on govinfo.gov |
| `agency_names` | Semicolon-separated agency names |
| `agency_ids` | Comma-separated FR agency IDs |
| `excerpts` | Search excerpt text |

### agencies

Federal agencies from FR data (444 agencies).

| Column | Description |
|---|---|
| `id` (PK) | FR agency ID |
| `name` | Display name |
| `raw_name` | Official name from the Federal Register |
| `slug` | URL slug |
| `parent_id` | Parent agency ID (null for top-level agencies) |

### federal_register_agencies

Link table associating Federal Register documents with agencies. Many FR documents have multiple agencies.

### agency_map

Maps Regulations.gov short codes to Federal Register agencies.

| Column | Description |
|---|---|
| `regs_code` (PK) | Regulations.gov agency code (e.g., `EPA`, `FDA`) |
| `fr_agency_id` | Matching FR agency ID |
| `fr_agency_name` | Matching FR agency display name |
| `fr_raw_name` | Matching FR agency official name |

### dockets

Regulatory dockets (proceedings and rulemaking containers).

| Column | Description |
|---|---|
| `id` (PK) | Docket ID (e.g., `EPA-HQ-OA-2002-0001`) |
| `agency_id` | Agency short code |
| `title` | Docket title |
| `docket_type` | `Rulemaking` or `Nonrulemaking` |
| `last_modified` | ISO timestamp of last modification |
| `object_id` | Internal regulations.gov ID |

### documents

Regulatory documents filed in dockets.

| Column | Description |
|---|---|
| `id` (PK) | Document ID (e.g., `USDA_FRDOC_0001-1524`) |
| `agency_id` | Agency short code |
| `docket_id` | Parent docket ID |
| `title` | Document title |
| `document_type` | `Notice`, `Rule`, `Proposed Rule`, `Supporting & Related Material`, or `Other` |
| `posted_date` | Date posted (`YYYY-MM-DD`) |
| `posted_year` | Extracted integer year |
| `posted_month` | Extracted integer month |
| `comment_start_date` | Start of public comment window |
| `comment_end_date` | End of public comment window |
| `fr_doc_num` | Federal Register document number (links to `federal_register` table) |
| `open_for_comment` | `0` or `1` |
| `withdrawn` | `0` or `1` |

### comments

Public comment headers (metadata only, not full text).

| Column | Description |
|---|---|
| `id` (PK) | Comment ID (e.g., `EPA-HQ-OA-2002-0001-0039`) |
| `agency_id` | Agency short code |
| `docket_id` | Derived from comment ID |
| `title` | Original title (often `Comment from [Name]`) |
| `submitter_name` | Parsed from title (null if anonymous or unparseable) |
| `submitter_type` | `individual`, `organization`, `anonymous`, or `unknown` |
| `posted_date` | Date posted (`YYYY-MM-DD`) |
| `posted_year` | Extracted integer year |
| `posted_month` | Extracted integer month |
| `withdrawn` | `0` or `1` |

### fr_regs_crossref

Cross-reference table linking Federal Register documents to Regulations.gov documents. Built in 5 phases: direct match, normalized match, leading-zero strip, date+title for page-number docs, and case-insensitive date+title.

### cfr_sections

Current regulatory text from the Electronic Code of Federal Regulations (19 titles).

| Column | Description |
|---|---|
| `section_id` (PK) | Title:section identifier (e.g., `40:63.1`) |
| `title_number` | CFR title number (7, 9, 21, 40, or 50) |
| `title_name` | Title name (e.g., "Protection of Environment") |
| `chapter` | Roman numeral chapter |
| `part_number` | Part number |
| `part_name` | Part heading |
| `section_number` | Section number (e.g., `63.1`) |
| `section_heading` | Section heading text |
| `agency` | Agency code (EPA, FDA, APHIS, FWS, etc.) |
| `authority` | Legal authority citation |
| `full_text` | Full regulatory text of the section |

### congressional_record

Floor proceedings from the Congressional Record (1994–present).

| Column | Description |
|---|---|
| `granule_id` (PK) | Unique GovInfo granule ID |
| `date` | Date of proceedings (`YYYY-MM-DD`) |
| `congress` | Congress number (e.g., 118) |
| `session` | Session number (1 or 2) |
| `volume` / `issue` | Congressional Record volume and issue |
| `title` | Entry title |
| `chamber` | `Senate`, `House`, `Extensions of Remarks`, or `Daily Digest` |
| `granule_class` | Content type (e.g., `SPEECHESSTATEMENTS`, `BILLS`) |
| `page_start` / `page_end` | Congressional Record page range |
| `speakers` | JSON array of speaker names |
| `bills` | JSON array of referenced bill numbers |
| `full_text` | Full text of the entry |

### crec_speakers

Normalized speaker data from CREC, linked to congress_members via `bioguide_id`.

| Column | Description |
|---|---|
| `granule_id` (PK1) | References congressional_record |
| `speaker_name` (PK2) | Speaker name as it appears in the record |
| `speaker_role` | Role (e.g., "SPEAKING") |
| `bioguide_id` | Bioguide ID linking to congress_members (99.6% coverage) |
| `party` | Party at time of speech |
| `state` | State at time of speech |

### crec_bills

Bill references extracted from CREC floor proceedings.

| Column | Description |
|---|---|
| `granule_id` (PK1) | References congressional_record |
| `congress` | Congress number |
| `bill_type` | Type (hr, s, hjres, etc.) |
| `bill_number` | Bill number |
| `bill_id` | Formatted as `{congress}-{type}-{number}` — matches legislation table (93rd-119th Congress) |

### congress_members

All current and historical members of Congress (12,700+).

| Column | Description |
|---|---|
| `bioguide_id` (PK) | Universal Biographical Directory ID |
| `full_name` | Official full name |
| `party` | Most recent party affiliation |
| `state` | Most recent state |
| `chamber` | Senate or House (most recent term) |
| `is_current` | 1 if currently serving |
| `opensecrets_id` | OpenSecrets (CRP) ID for campaign finance data |
| `govtrack_id` | GovTrack.us ID |
| `fec_ids` | FEC candidate IDs (JSON array) |

### stock_trades

Congressional stock trading disclosures from 100% government sources. Senate: efdsearch.senate.gov (15K+ transactions, 2012-present). House: disclosures-clerk.house.gov PTR PDFs parsed for transaction-level data (46K+ transactions with tickers, amounts, buy/sell, 2013-present). Both chambers linked to congress_members via bioguide_id (85% coverage). All dates normalized to ISO format (YYYY-MM-DD).

| Column | Description |
|---|---|
| `member_name` | Name as it appears in disclosure |
| `bioguide_id` | Linked to congress_members (85.5% coverage) |
| `chamber` | Senate or House |
| `transaction_date` | Date of trade |
| `ticker` | Stock ticker symbol (86% coverage for parsed transactions) |
| `asset_description` | Description of asset traded |
| `transaction_type` | Purchase, Sale, Sale (Full), Sale (Partial), Exchange, etc. |
| `amount_range` | Dollar range (e.g., "$1,001 - $15,000") |
| `owner` | Self, Spouse, Joint, Child, etc. |
| `source_url` | Link to original disclosure document |
| `filing_type` | P (PTR), A (Annual), H (Amendment), etc. |

### committees

Congressional committees and subcommittees.

| Column | Description |
|---|---|
| `committee_id` | Committee identifier (e.g., SSEV, HSAG) |
| `name` | Full committee name |
| `chamber` | Senate, House, or Joint |
| `url` | Committee website URL |
| `parent_committee_id` | Parent committee (for subcommittees) |

### committee_memberships

Current committee assignments with leadership roles.

| Column | Description |
|---|---|
| `bioguide_id` | Member's bioguide ID (links to congress_members) |
| `committee_id` | Committee identifier (links to committees) |
| `member_name` | Member name as listed in committee data |
| `party` | Party affiliation |
| `title` | Leadership title (Chairman, Ranking Member, etc.) |
| `rank` | Seniority rank on committee |

### legislation

Congressional bills and resolutions (93rd–119th Congress, 364K+ bills).

| Column | Description |
|---|---|
| `bill_id` (PK) | Bill identifier (e.g., `119-hr-1234`) |
| `congress` | Congress number |
| `bill_type` | hr, s, hjres, sjres, hres, sres, hconres, sconres |
| `bill_number` | Bill number |
| `title` | Bill title |
| `policy_area` | Policy area classification |
| `introduced_date` | Date introduced |
| `latest_action_date` | Date of most recent action |
| `latest_action_text` | Description of most recent action |
| `origin_chamber` | Originating chamber |
| `sponsor_name` | Primary sponsor |
| `sponsor_bioguide_id` | Sponsor bioguide ID (links to congress_members) |
| `cosponsor_count` | Number of cosponsors |
| `summary_text` | Bill summary |

### legislation_cosponsors

Cosponsors for tracked legislation, distinguishing original from later cosponsors.

| Column | Description |
|---|---|
| `bill_id` | Bill identifier (links to legislation) |
| `bioguide_id` | Cosponsor's bioguide ID (links to congress_members) |
| `full_name` | Cosponsor's full name |
| `party` | Party affiliation |
| `state` | State |
| `sponsorship_date` | Date they cosponsored |
| `is_original_cosponsor` | 1 if signed on at introduction, 0 if added later |

### roll_call_votes

Congressional roll call votes (101st–119th Congress, 26K+ votes).

| Column | Description |
|---|---|
| `congress` (PK1) | Congress number |
| `chamber` (PK2) | Senate or House |
| `session` (PK3) | Session number |
| `roll_call_number` (PK4) | Roll call number |
| `date` | Vote date |
| `question` | Question being voted on |
| `vote_type` | Type of vote |
| `description` | Vote description |
| `result` | Passed, Failed, Agreed to, etc. |
| `bill_id` | Related bill (links to legislation) |
| `yea_count` / `nay_count` | Vote tallies |
| `source_url` | Link to official vote record |

### member_votes

Individual member positions on each roll call vote (8.3M+ records).

| Column | Description |
|---|---|
| `congress` | Congress number |
| `chamber` | Senate or House |
| `session` | Session number |
| `roll_call_number` | Roll call number (links to roll_call_votes) |
| `bioguide_id` | Member bioguide ID (links to congress_members) |
| `member_name` | Member name |
| `party` | Party at time of vote |
| `state` | State |
| `vote_cast` | Yea, Nay, Not Voting, Present |

### comment_details

Full comment details from Regulations.gov (in progress — 423K of 9.7M downloaded).

| Column | Description |
|---|---|
| `id` (PK) | Comment ID (links to comments) |
| `comment_text` | Full text of the comment |
| `organization` | Organization name |
| `first_name` / `last_name` | Submitter name |
| `city` / `state_province` / `zip` / `country` | Location |
| `subtype` | Comment subtype |
| `duplicate_comments` | Count of duplicate submissions |
| `attachment_count` | Number of attached files |
| `attachment_urls` | URLs to attachment files |

### lobbying_filings

Lobbying disclosure filings from the Senate LDA (1.9M+ filings, 1999–present).

| Column | Description |
|---|---|
| `filing_uuid` (PK) | Unique filing identifier |
| `filing_type` | Filing type code |
| `registrant_id` | Registrant ID |
| `registrant_name` | Lobbying firm or self-filing organization |
| `client_id` | Client ID |
| `client_name` | Client being represented |
| `filing_year` | Year of filing |
| `filing_period` | Filing period (Q1, Q2, mid-year, etc.) |
| `received_date` | Date received by Senate |
| `amount_reported` | Dollar amount reported |
| `is_no_activity` | 1 if no lobbying activity |
| `is_termination` | 1 if terminating registration |

### lobbying_lobbyists

Individual lobbyists listed on filings (3.5M+ records).

| Column | Description |
|---|---|
| `filing_uuid` | Filing reference |
| `lobbyist_name` | Lobbyist name |
| `covered_position` | Former government position (revolving door) |
| `is_new` | 1 if newly added on this filing |

### lobbying_activities

Lobbying activities by issue area (2.8M+ records).

| Column | Description |
|---|---|
| `filing_uuid` | Filing reference |
| `issue_code` | General issue area code (links to lobbying_issue_codes) |
| `specific_issues` | Detailed description of issues lobbied on |
| `government_entities` | Government bodies contacted |
| `income_amount` / `expense_amount` | Amounts reported |

### lobbying_contributions

Lobbyist contributions to federal candidates and officeholders.

| Column | Description |
|---|---|
| `filing_uuid` | Filing reference |
| `lobbyist_name` | Contributing lobbyist |
| `recipient_name` | Recipient candidate/officeholder |
| `amount` | Contribution amount |
| `contribution_date` | Date of contribution |

### fara_registrants

Foreign agent registrations under FARA (7K+ registrants).

| Column | Description |
|---|---|
| `registration_number` (PK) | FARA registration number |
| `registration_date` | Date of registration |
| `termination_date` | Date of termination (null if active) |
| `name` | Registrant name |
| `business_name` | Business name |
| `city` / `state` / `zip` | Location |

### fara_foreign_principals

Foreign governments, parties, and entities represented by FARA registrants (18K+ records).

| Column | Description |
|---|---|
| `registration_number` | Links to fara_registrants |
| `registrant_name` | Registrant name |
| `foreign_principal` | Foreign principal name |
| `country` | Country of foreign principal |
| `fp_registration_date` | Date principal was added |
| `fp_termination_date` | Date relationship terminated |

### fara_registrant_docs

Documents filed by FARA registrants (151K+ documents).

| Column | Description |
|---|---|
| `registration_number` | Links to fara_registrants |
| `registrant_name` | Registrant name |
| `date_stamped` | Date stamped by DOJ |
| `document_type` | Supplemental Statement, Amendment, etc. |
| `foreign_principal_name` | Related foreign principal |
| `foreign_principal_country` | Country |
| `url` | URL to document |

### fara_short_forms

Individual employees/associates of FARA registrants (44K+ records).

| Column | Description |
|---|---|
| `registration_number` | Links to fara_registrants |
| `registrant_name` | Registrant name |
| `first_name` / `last_name` | Individual name |
| `short_form_date` | Date of short form |
| `short_form_termination_date` | Termination date |

### fec_candidates

FEC candidate records (65K+ records, 2004–2024 cycles).

| Column | Description |
|---|---|
| `cand_id` (PK1) | FEC candidate ID |
| `cycle` (PK2) | Election cycle year |
| `cand_name` | Candidate name |
| `cand_pty_affiliation` | Party affiliation |
| `cand_office` | Office sought (H, S, P) |
| `cand_office_st` | State |
| `cand_ici` | Incumbent/Challenger/Open (I/C/O) |

### fec_committees

FEC political committees (155K+ records).

| Column | Description |
|---|---|
| `cmte_id` (PK1) | Committee ID |
| `cycle` (PK2) | Election cycle year |
| `cmte_nm` | Committee name |
| `cmte_tp` | Committee type |
| `connected_org_nm` | Connected organization name |
| `cand_id` | Associated candidate ID |

### fec_contributions

Individual contributions to federal candidates (4.4M+ records, aggregated from 103.9M raw records).

| Column | Description |
|---|---|
| `cmte_id` | Receiving committee |
| `cand_id` | Candidate |
| `transaction_dt` | Transaction date |
| `transaction_amt` | Dollar amount |
| `employer` | Contributor's employer |
| `occupation` | Contributor's occupation |
| `state` | Contributor's state |
| `cycle` | Election cycle |

### fec_operating_expenditures

FEC operating expenditures by political committees (15.4M+ records).

| Column | Description |
|---|---|
| `cmte_id` | Spending committee ID |
| `recipient_nm` | Payee/recipient name |
| `purpose` | Purpose of expenditure |
| `transaction_dt` | Transaction date |
| `transaction_amt` | Dollar amount |
| `city` / `state` / `zip` | Payee location |
| `cycle` | Election cycle |

### fec_independent_expenditures

FEC independent expenditures for/against candidates (667K+ records).

| Column | Description |
|---|---|
| `cmte_id` | Spending committee ID |
| `cand_id` | Target candidate ID |
| `sup_opp` | Support (S) or Oppose (O) |
| `purpose` | Purpose of expenditure |
| `transaction_dt` | Transaction date |
| `transaction_amt` | Dollar amount |
| `cycle` | Election cycle |

### fec_pac_summary

FEC PAC financial summary data (99K+ records).

| Column | Description |
|---|---|
| `cmte_id` | Committee ID |
| `cmte_nm` | Committee name |
| `cmte_tp` | Committee type |
| `total_receipts` | Total receipts |
| `total_disbursements` | Total disbursements |
| `contributions_to_candidates` | Contributions to federal candidates |
| `independent_expenditures` | Independent expenditures |
| `cycle` | Election cycle |

### hearings

Committee hearings from GovInfo CHRG collection (46,177 hearings, 1993–present).

| Column | Description |
|---|---|
| `package_id` (PK) | GovInfo package identifier |
| `title` | Hearing title |
| `committee` | Committee name |
| `chamber` | Senate, House, or Joint |
| `congress` | Congress number |
| `session` | Session number |
| `date` | Hearing date |
| `jacket_number` | GPO jacket number |
| `url` | Link to hearing on GovInfo |

### hearing_witnesses

Witnesses who testified at committee hearings.

| Column | Description |
|---|---|
| `package_id` | Links to hearings |
| `witness_name` | Witness name |
| `organization` | Witness's organization or affiliation |
| `position` | Witness's title or position |

### hearing_members

Congress members who participated in committee hearings.

| Column | Description |
|---|---|
| `package_id` | Links to hearings |
| `bioguide_id` | Links to congress_members |
| `member_name` | Member name |
| `party` | Party affiliation |
| `state` | State |

### crs_reports

Congressional Research Service reports (13,629 reports).

| Column | Description |
|---|---|
| `report_number` (PK) | CRS report number (e.g., "R12345") |
| `title` | Report title |
| `summary` | Report summary text |
| `authors` | Report author(s) |
| `published_date` | Publication date |
| `updated_date` | Most recent update date |
| `url` | Link to report |

### crs_report_bills

Cross-references between CRS reports and legislation.

| Column | Description |
|---|---|
| `report_number` | Links to crs_reports |
| `bill_id` | Links to legislation |
| `congress` | Congress number |
| `bill_type` | Bill type (hr, s, etc.) |
| `bill_number` | Bill number |

### nominations

Executive nominations submitted to the Senate (40,067 nominations, Congresses 100–119).

| Column | Description |
|---|---|
| `nomination_id` (PK) | Nomination identifier |
| `congress` | Congress number |
| `nominee_name` | Name of nominee |
| `position` | Position nominated for |
| `agency` | Agency or department |
| `received_date` | Date received by Senate |
| `latest_action_date` | Date of most recent action |
| `latest_action_text` | Description of most recent action |
| `confirmed` | 1 if confirmed, 0 otherwise |
| `vote_yea` | Confirmation vote yea count |
| `vote_nay` | Confirmation vote nay count |

### nomination_actions

Action history for executive nominations.

| Column | Description |
|---|---|
| `nomination_id` | Links to nominations |
| `action_date` | Date of action |
| `action_text` | Description of action |

### treaties

Treaties submitted to the Senate (785 treaties, Congresses 89–119).

| Column | Description |
|---|---|
| `treaty_id` (PK) | Treaty identifier |
| `congress` | Congress number |
| `treaty_number` | Treaty document number |
| `title` | Treaty title |
| `country` | Countries or parties involved |
| `transmitted_date` | Date transmitted to Senate |
| `index_terms` | Subject index terms |
| `resolution_text` | Senate resolution text |
| `latest_action_date` | Date of most recent action |
| `latest_action_text` | Description of most recent action |

### treaty_actions

Action history for treaties.

| Column | Description |
|---|---|
| `treaty_id` | Links to treaties |
| `action_date` | Date of action |
| `action_text` | Description of action |

### gao_reports

Government Accountability Office reports (73,725 reports, 1989–present). Includes GovInfo GAOREPORTS collection (1989–2008) plus direct gao.gov scrape (2009–present).

| Column | Description |
|---|---|
| `package_id` (PK) | GovInfo package identifier |
| `title` | Report title |
| `report_number` | GAO report number |
| `published_date` | Publication date |
| `abstract` | Report abstract/summary |
| `subject_terms` | Subject classifications |
| `usc_references` | U.S. Code citations |
| `public_law_references` | Public law citations |
| `statute_references` | Statutes at Large citations |
| `url` | Link to report on GovInfo |

### oira_reviews

OIRA (Office of Information and Regulatory Affairs) regulatory reviews (48,434 reviews).

| Column | Description |
|---|---|
| `rin` (PK) | Regulation Identifier Number |
| `title` | Rule title |
| `agency` | Issuing agency |
| `stage` | Review stage (Proposed Rule, Final Rule, etc.) |
| `received_date` | Date received by OIRA |
| `concluded_date` | Date review concluded |
| `conclusion` | Review conclusion (Consistent with Change, Withdrawn, etc.) |
| `legal_authority` | Legal authority citations |

### oira_meetings

OIRA review meetings with external parties (8,663 meetings).

| Column | Description |
|---|---|
| `meeting_id` (PK) | Meeting identifier |
| `rin` | Regulation Identifier Number (links to oira_reviews) |
| `date` | Meeting date |
| `subject` | Meeting subject |
| `attendee_count` | Number of attendees |

### oira_attendees

Individual attendees at OIRA review meetings (90,711 records).

| Column | Description |
|---|---|
| `meeting_id` | Links to oira_meetings |
| `name` | Attendee name |
| `organization` | Attendee organization |
| `title` | Attendee title |

### ig_reports

Inspector General reports from oversight.gov (34,880 reports).

| Column | Description |
|---|---|
| `report_id` (PK) | Report identifier |
| `title` | Report title |
| `agency` | Issuing IG office |
| `published_date` | Publication date |
| `type` | Report type (Audit, Inspection, Investigation, etc.) |
| `url` | Link to report |
| `questioned_costs` | Dollar amount of questioned costs |
| `unsupported_costs` | Dollar amount of unsupported costs |

### ig_recommendations

Recommendations from IG reports (11,999 recommendations).

| Column | Description |
|---|---|
| `report_id` | Links to ig_reports |
| `recommendation_number` | Recommendation number within report |
| `recommendation_text` | Recommendation text |
| `status` | Open, Closed, etc. |

### cbo_cost_estimates

Congressional Budget Office cost estimates linked to legislation (~17,200 estimates).

| Column | Description |
|---|---|
| `bill_id` | Links to legislation |
| `title` | Cost estimate title |
| `pub_date` | Publication date |
| `description` | Cost estimate description |
| `url` | Link to CBO cost estimate |

### Views

| View | Description |
|---|---|
| `docket_stats` | Comment count, document count, and date ranges per docket |
| `docket_summary` | Docket overview with linked documents and comment counts |
| `comments_by_year` | Comment counts by agency and year with submitter type breakdown |
| `fr_by_year` | Federal Register document counts by year and type |
| `top_dockets` | Most commented dockets with submitter breakdown |
| `regulatory_pipeline` | Dockets with both proposed and final rules, showing timeline |
| `comments_monthly` | Comment volume by agency/year/month for time-series charts |
| `agency_overview` | Comprehensive per-agency stats (dockets, docs, comments, rules) |
| `top_submitters` | Most active organizational commenters across agencies |
| `speaker_activity` | CREC speaker summary with bioguide linkage and party info |
| `bills_floor_time` | Bills discussed on the floor with mention counts |
| `member_overview` | Current members with trade counts, speech counts, bills sponsored |
| `member_stats` | Per-member counts: trades, speeches, bills, votes, FEC totals |
| `stock_trades_by_ticker` | Aggregate trading activity per ticker symbol |
| `committee_member_trades` | Cross-reference of committee memberships with stock trades |
| `lobbying_by_year` | Lobbying spending aggregated by year |
| `top_lobbying_clients` | Top lobbying clients by total spending |
| `spending_by_agency` | Federal spending aggregated by agency |
| `nomination_rates` | Confirmation rates by congress (total, confirmed, withdrawn, returned) |
| `hearing_activity` | Hearing counts and unique witnesses by congress and chamber |
| `fec_employer_totals` | Top employers by total campaign contributions |
| `fec_employer_to_candidate` | Employer-to-candidate contribution flows |
| `fec_employer_to_party` | Employer-to-party contribution flows |
| `fec_top_occupations` | Top occupations by total contributions |

### Pre-computed Research Tables

These materialized tables pre-compute expensive cross-domain joins for the Research explore page:

| Table | Description |
|-------|-------------|
| `speeches_near_trades` | Floor speeches within 7 days of a stock trade by the same member |
| `committee_trade_conflicts` | Trades in stocks whose SIC code falls within the member's committee jurisdiction |
| `committee_donor_summary` | PAC donations to current committee members |
| `lobbying_bill_summary` | Bills most frequently referenced in lobbying filings |
| `witness_lobby_overlap` | Organizations that both testified at hearings and lobbied |
| `commenter_lobby_overlap` | Organizations that both submitted regulatory comments and lobbied |
| `revolving_door` | Former members of Congress who became registered lobbyists |
| `lobbying_issue_summary` | Lobbying activity counts per issue code |
| `legislation_policy_summary` | Bill counts per policy area |
| `legislation_sponsor_summary` | Bill counts per sponsor |
| `spending_agency_summary` | Spending totals per agency |
| `docket_summary` | Best FR abstract per docket (enriches dockets_fts) |

### FTS5 Tables

Full-text search indexes (SQLite FTS5):

- `federal_register_fts` — title, abstract, agency_names, excerpts
- `dockets_fts` — title, agency_id
- `documents_fts` — title, agency_id, document_type
- `comments_fts` — title, submitter_name, agency_id
- `cfr_fts` — section_number, section_heading, part_name, agency, full_text
- `crec_fts` — title, chamber, full_text
- `legislation_fts` — title, sponsor_name, policy_area
- `spending_awards_fts` — recipient_name, description, agency
- `lobbying_fts` — registrant_name, client_name, specific_issues
- `fara_registrants_fts` — name, business_name
- `fara_foreign_principals_fts` — foreign_principal, registrant_name, country
- `fec_employer_fts` — employer names for contribution search
- `hearings_fts` — title, chamber, committees
- `crs_reports_fts` — title, authors, topics, summary
- `nominations_fts` — description, organization, citation, status
- `gao_reports_fts` — title, abstract, subjects, report_number
- `ig_reports_fts` — title, agency, type
- `oira_reviews_fts` — title, agency, legal_authority

---

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/01_federal_register.py` | Download all Federal Register documents via the FR API |
| `scripts/02_regs_gov_dockets_docs.py` | Download dockets and documents from Regulations.gov |
| `scripts/03_regs_gov_comments.py` | Download comment headers for all agencies (EPA/FDA/USDA/FWS/APHIS). Consolidated 2026-03-27 from old 03+04+06 — includes daily-window backfill for truncated months and FWS/APHIS targeted fetch. |
| `scripts/05_build_database.py` | Build SQLite database from all raw JSON with enrichments |
| `scripts/07_full_comment_details.py` | Download full comment details (text, names, location) per comment |
| `scripts/08_usaspending.py` | Download federal spending (grants + contracts) for 20 agencies from USAspending.gov |
| `scripts/08_fec_campaign_finance.py` | Download FEC bulk data (candidates, committees, contributions) |
| `scripts/09_congress_gov.py` | Download congressional legislation via GovInfo BILLSTATUS bulk XML + Congress.gov API |
| `scripts/09_fec_employer_aggregates.py` | Build employer-level FEC contribution aggregates |
| `scripts/10_ecfr.py` | Download and parse eCFR regulatory text from GovInfo bulk data |
| `scripts/10_congress_votes.py` | Download roll call votes and member positions from Congress.gov API |
| `scripts/11_congressional_record.py` | Download Congressional Record floor proceedings (1994–present) from GovInfo |
| `scripts/11_fara.py` | Download FARA registrations, foreign principals, documents, short forms |
| `scripts/12_congress_stock_trades.py` | Congress member data + stock trades from Senate eFD, House PTR, and FD indexes. Consolidated 2026-03-27 from old 12+13+14. |
| `scripts/15_lobbying_disclosure.py` | Download lobbying disclosure data from Senate LDA API (filings, contributions, lobbyists) |
| `scripts/16_committee_hearings.py` | Download committee hearing metadata, witnesses, and member attendance from GovInfo CHRG |
| `scripts/17_crs_reports.py` | Download CRS reports and related legislation from Congress.gov API |
| `scripts/18_nominations_treaties.py` | Download executive nominations and treaties from Congress.gov API |
| `scripts/19_gao_reports.py` | Download GAO reports from GovInfo GAOREPORTS collection |
| `scripts/19b_gao_direct.py` | Download GAO reports directly from gao.gov (2009–present) |
| `scripts/20_sec_ticker_sic.py` | Download SIC codes for stock tickers from SEC EDGAR for committee trade conflict analysis |
| `scripts/21_ig_reports.py` | Download IG reports and recommendations from oversight.gov |
| `scripts/16_backfill_dockets.py` | Backfill truncated docket lists for EPA/FDA from Regulations.gov |
| `scripts/build_lis_crosswalk.py` | Build LIS-to-bioguide crosswalk for vote linkage |
| `scripts/repair_crec_metadata.py` | Repair CREC metadata (one-time fix for namespace bug) |
| `deploy/deploy.sh` | Deploy database + APHIS DB + templates + explore pages to Datasette instance |
| `deploy/post_deploy_qc.sh` | Post-deployment quality checks (row counts, FTS, canned queries, explore pages) |

---

## Datasette Configuration

The live instance at regs.datadawn.org includes 80+ pre-built SQL queries across all tables, accessible from the database page. Post-deploy quality checks are run via `deploy/post_deploy_qc.sh`. Key query categories:

- **Regulatory data**: Most commented dockets, rulemaking pipeline, FR documents by agency, recently published rules
- **Public comments**: Search comments, comment volume by year, top organizational commenters, monthly activity
- **CFR regulations**: Search regulatory text, browse by agency, part listing, section detail
- **Congressional Record**: Search floor proceedings, speeches by member (with bioguide linkage), most active speakers, bills on floor
- **Congress members**: Search members, member profiles (trades + speeches + bills + votes + donors), current members overview
- **Committees**: Committee members, member committee assignments, committee leadership, committee trade activity
- **Cosponsors**: Bill cosponsors (original vs. later), most prolific cosponsors, bipartisan bills
- **Stock trades**: Trades by member, most active traders, most traded stocks, trades by ticker, member activity timelines
- **Legislation**: Search bills, browse by policy area, bill detail with actions/subjects
- **Lobbying**: Search filings, top clients, lobbying by issue area, revolving door (covered positions), bills referenced in lobbying
- **FARA**: Search registrants, foreign principals by country, registrant documents
- **Campaign finance**: Contributions by employer, top donors, candidate fundraising, party flows
- **Hearings**: Committee hearings, witness search, member attendance, witness-lobbyist overlap
- **Nominations**: Executive nominations, confirmation rates by congress, agency positions
- **Research**: Speeches near trades, committee trade conflicts (SIC-based), revolving door (former members lobbying), lobbying-legislation overlap, witness-lobby overlap, commenter-lobby overlap
- **Votes**: Roll call votes, member voting records, party-line analysis
- **Spending**: Search awards, agency spending summary
- **Cross-references**: FR ↔ Regulations.gov document links, CRS reports ↔ legislation, lobbying ↔ legislation

These are defined in `deploy/metadata.json`. The metadata also configures facets (agency, year, submitter type, document type, chamber) and sort orders for each table.

---

## Update Plan

- **Federal Register**: Updated daily by GPO. Can re-run monthly to stay current.
- **Regulations.gov**: API data updates continuously. Comment downloads are incremental (state files track completed year/month combinations).
- **Recommended cycle**: Monthly for Federal Register, quarterly for Regulations.gov.
- **Rebuild command**: `python3 scripts/05_build_database.py` rebuilds the entire database from raw JSON files (~37 minutes for ~120M rows).
- **Deploy command**: `bash deploy/deploy.sh` uploads the database and metadata to regs.datadawn.org.

---

## Next Rebuild TODO

These items should be addressed in the next `05_build_database.py` rebuild:

1. **Normalize `transaction_type` upstream** — House PTR data uses abbreviated codes (`P`, `S`, `E`, `S (partial)`) while Senate uses full names (`Purchase`, `Sale (Full)`, etc.). Normalize all to full names during build so explore pages don't need client-side translation. Affects: `stock_trades` table, `speeches_near_trades`, `committee_trade_conflicts`.

2. **Fix `committee_donor_summary` subcommittee duplication** — Members who sit on both a parent committee and its subcommittees appear multiple times in the summary table, inflating donation counts. The build should deduplicate by (bioguide_id, fec_committee_id) pair.

3. **Remove "Independent" party filter from revolving-door page** — Zero matches (one Whig member displays as Independent). Either remove the filter button or fix the underlying party mapping.

---

## Known Limitations

1. **Comment details download in progress** — Full comment text requires a separate API call per comment. Script `07_full_comment_details.py` is actively downloading at ~780 req/hr. 423K details downloaded so far. At current rates, the full 9.7M comments would take months — the prioritized approach (organizations first) is the practical strategy.

2. **Comment expansion coverage** — Comment headers expanded from ~3.9M to 9.7M across additional agencies. Coverage completeness varies by agency and time period.

3. **House PTR PDF parsing** — ~30% of House PTR filings (2,408 of 8,073) are scanned/image PDFs that cannot be text-parsed. These are predominantly older filings (pre-2017). Newer filings are 95%+ text-extractable. Filing indexes are still available for all filings.

4. **EPA truncation partially recovered** — 78 months exceeded the 5,000-result API pagination cap. All 78 were backfilled via daily date windows, recovering ~308,000 comments. **One day (2020-12-30) still exceeds the daily cap** with 5,144 comments but only 5,000 retrieved — an estimated **~144 comments are missing** for that single day. No other days are known to be truncated.

5. **Submitter name parsing is heuristic** — Regex-based extraction from comment titles. Approximately 60% of comments have a parsed submitter name. The rest are classified as `anonymous` or `unknown`. Organization detection uses keyword matching and may misclassify some orgs as individuals or vice versa.

6. **Docket ID derivation** — Extracted by stripping the last numeric segment from comment IDs (e.g., `EPA-HQ-OA-2002-0001-0039` → `EPA-HQ-OA-2002-0001`). Works for standard ID formats but may produce incorrect docket IDs for non-standard formats.

7. **No document full text** — Federal Register abstracts and excerpts are included, but full document text is not stored. Full text is available via `html_url` links.

8. **Agency coverage varies** — Core agencies (EPA, FDA, USDA, FWS, APHIS) have the deepest coverage. Expansion agencies have comment headers and documents but may lack full docket metadata. The Federal Register table covers all 444 agencies.

9. **Cross-reference coverage** — 395,621 FR↔Regs.gov crossref rows, covering ~23% of the 1,703,711 Regulations.gov documents. The 5-phase matching (direct, normalized, leading-zero strip, date+title, case-insensitive) recovers what's possible from the metadata; the remainder lack a `fr_doc_num` field in their source records.

---

## License

This project is licensed under [Creative Commons Zero v1.0 Universal](LICENSE). All U.S. government-sourced data is in the public domain.

Built by a human, [Claude](https://www.anthropic.com/claude) (Anthropic), and DJ Crabdaddy ([Claude Code](https://docs.anthropic.com/en/docs/claude-code)) 🦀

A [DataDawn](https://datadawn.org) project.
