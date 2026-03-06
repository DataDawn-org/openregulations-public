# OpenRegs Data Dictionary

> Comprehensive U.S. federal government data: regulations, legislation, congressional floor speeches, lobbying disclosures, campaign finance, stock trades, foreign agent registrations, and federal spending.
> Updated: 2026-03-06

---

## Data Sources & APIs

| Source | API / URL | Auth | Rate Limit |
|--------|-----------|------|------------|
| Federal Register | `federalregister.gov/api/v1` | None | None (polite 0.5s delay) |
| Regulations.gov | `api.regulations.gov/v4` | API key (`X-Api-Key`) | 1,000 req/hour |
| GovInfo (BILLSTATUS, CREC, eCFR) | `govinfo.gov/bulkdata` | None | None |
| Congress.gov (votes, legislation) | `api.congress.gov/v3` | API key | 5,000 req/hour |
| USAspending.gov | `api.usaspending.gov/api/v2` | None | None |
| Senate LDA (lobbying) | `lda.senate.gov/api/v1` | API key | Throttled (~1 req/sec) |
| Senate eFD (stock trades) | `efdsearch.senate.gov` | None | Throttled |
| House FD (stock trades) | `disclosures-clerk.house.gov` | None | None (PDF downloads) |
| FEC (campaign finance) | `fec.gov/data/browse-data` | None (bulk files) | N/A |
| FARA (foreign agents) | `efile.fara.gov` | None | None |
| APHIS (animal welfare) | `aphis.my.site.com` (Salesforce Aura) | None | Throttled |

API keys are stored in `scripts/config.json` (never committed to git).

---

## Databases

| Database | Path | Size | Tables | Purpose |
|----------|------|------|--------|---------|
| openregs.db | `openregs.db` | 22 GB | 40+ | Primary database (deployed to Datasette) |
| lobbying.db | `lobbying.db` | 13 GB | 8 | Full lobbying data with raw JSON |
| fec.db | `fec.db` | 47 GB | 7 | Full FEC data (104M individual contributions) |
| fara.db | `fara.db` | 42 MB | 4 | Full FARA data |
| votes.db | `votes.db` | 985 MB | 2 | Full vote records |
| aphis.db | `aphis/db/aphis.db` | 53 MB | 5 | APHIS animal welfare |

The `openregs.db` is the production database deployed to Datasette. It contains curated subsets from all sources. The standalone databases (`lobbying.db`, `fec.db`, `fara.db`, `votes.db`) hold the complete raw downloads and are used as build sources.

---

## Tables in openregs.db

### Federal Register

#### `federal_register` -- 993,703 rows

All documents published in the Federal Register, 1994--present.

| Column | Type | Description |
|--------|------|-------------|
| `document_number` | TEXT PK | Unique FR document number (e.g., `2016-20442`) |
| `title` | TEXT | Document title |
| `type` | TEXT | `Rule`, `Proposed Rule`, `Notice`, `Presidential Document` |
| `abstract` | TEXT | Summary text |
| `publication_date` | TEXT | ISO date |
| `pub_year` / `pub_month` | INTEGER | Extracted date parts for filtering |
| `html_url` / `pdf_url` | TEXT | Links to full text |
| `agency_names` | TEXT | Semicolon-separated agency names |
| `agency_ids` | TEXT | Comma-separated FR agency IDs |

#### `agencies` -- 444 rows

Federal agency reference table from FR data.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | FR agency ID |
| `name` | TEXT | Display name |
| `slug` | TEXT | URL slug |
| `parent_id` | INTEGER | Parent agency (null for top-level) |

#### `federal_register_agencies` -- 1,531,312 rows

Junction table: FR documents to agencies (many-to-many).

| Column | Type | Description |
|--------|------|-------------|
| `document_number` | TEXT PK | FR document number |
| `agency_id` | INTEGER PK | FR agency ID |

#### `presidential_documents` -- 5,904 rows

Executive orders and proclamations extracted from FR data.

| Column | Type | Description |
|--------|------|-------------|
| `document_number` | TEXT PK | FR document number |
| `title` | TEXT | Document title |
| `document_type` | TEXT | `Executive Order` or `Proclamation` |
| `executive_order_number` | INTEGER | EO number (null for proclamations) |
| `signing_date` | TEXT | ISO date signed |

---

### Regulations.gov

#### `dockets` -- 86,706 rows

Regulatory dockets from 5 agencies (EPA, FDA, USDA, FWS, APHIS). Includes 72K stub records backfilled from documents/comments for referential integrity.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Docket ID (e.g., `EPA-HQ-OAR-2024-0001`) |
| `agency_id` | TEXT | Agency code |
| `title` | TEXT | Docket title (null for stubs) |
| `docket_type` | TEXT | `Rulemaking` or `Nonrulemaking` |

#### `docket_summary` -- 26,195 rows

AI-generated docket summaries for dockets with titles.

| Column | Type | Description |
|--------|------|-------------|
| `docket_id` | TEXT | Docket ID |
| `summary` | TEXT | Plain-language summary |

#### `documents` -- 727,510 rows

Regulatory documents filed in dockets (rules, proposed rules, notices, supporting materials).

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Document ID |
| `agency_id` | TEXT | Agency code |
| `docket_id` | TEXT | Parent docket ID |
| `title` | TEXT | Document title |
| `document_type` | TEXT | `Rule`, `Proposed Rule`, `Notice`, `Supporting & Related Material`, `Other` |
| `posted_date` | TEXT | ISO date |
| `posted_year` / `posted_month` | INTEGER | Extracted date parts |
| `fr_doc_num` | TEXT | FR document number (cross-reference) |
| `comment_start_date` / `comment_end_date` | TEXT | Comment period window |
| `open_for_comment` / `withdrawn` | INTEGER | Boolean flags |

#### `comments` -- 3,713,961 rows

Public comment headers (metadata, not full text).

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Comment ID |
| `agency_id` | TEXT | Agency code |
| `docket_id` | TEXT | Derived from comment ID |
| `title` | TEXT | Original title (often "Comment from [Name]") |
| `submitter_name` | TEXT | Parsed from title (~62% coverage) |
| `submitter_type` | TEXT | `individual`, `organization`, `anonymous`, `unknown` |
| `posted_date` | TEXT | ISO date |
| `posted_year` / `posted_month` | INTEGER | Extracted date parts |

Coverage: FWS 1.6M, EPA 1.2M, APHIS 491K, FDA 433K, USDA 22K.

#### `comment_details` -- 41,807 rows

Full comment details (text, structured name, location). Downloaded one-by-one from the per-comment API endpoint. In progress.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Comment ID (links to `comments`) |
| `comment_text` | TEXT | Full text of the comment |
| `organization` | TEXT | Organization name |
| `first_name` / `last_name` | TEXT | Submitter name fields |
| `city` / `state_province` / `country` | TEXT | Location |
| `duplicate_comments` | INTEGER | Count of duplicate submissions |
| `attachment_count` | INTEGER | Number of attached files |
| `attachment_urls` | TEXT | URLs to attachment files |

#### `fr_regs_crossref` -- 70,030 rows

Cross-reference linking FR documents to Regulations.gov documents. Built via 5-phase matching (direct, normalized, leading-zero strip, date+title, case-insensitive).

| Column | Type | Description |
|--------|------|-------------|
| `fr_document_number` | TEXT PK | FR document number |
| `regs_document_id` | TEXT PK | Regulations.gov document ID |

#### `agency_map` -- 5 rows

Maps Regulations.gov agency codes to FR agency IDs.

| Column | Type | Description |
|--------|------|-------------|
| `regs_code` | TEXT PK | Regulations.gov code (EPA, FDA, USDA, FWS, APHIS) |
| `fr_agency_id` | INTEGER | FR agency ID |

---

### Code of Federal Regulations

#### `cfr_sections` -- 123,480 rows

Current regulatory text from eCFR bulk XML. 19 CFR titles (7, 9, 10, 14, 15, 17, 20, 21, 24, 28, 29, 33, 34, 38, 40, 44, 46, 49, 50).

| Column | Type | Description |
|--------|------|-------------|
| `section_id` | TEXT PK | e.g., `40:63.1` |
| `title_number` | INTEGER | CFR title number |
| `part_number` / `section_number` | TEXT | Part and section identifiers |
| `section_heading` | TEXT | Section heading text |
| `agency` | TEXT | Responsible agency code |
| `full_text` | TEXT | Full regulatory text |

---

### Congressional Record

#### `congressional_record` -- 878,583 rows

Floor proceedings from GovInfo CREC packages, 1994--present.

| Column | Type | Description |
|--------|------|-------------|
| `granule_id` | TEXT PK | GovInfo granule ID |
| `date` | TEXT | ISO date |
| `congress` | INTEGER | Congress number |
| `chamber` | TEXT | `Senate`, `House`, `Extensions of Remarks`, `Daily Digest` |
| `title` | TEXT | Entry title |
| `granule_class` | TEXT | Content type (e.g., `SPEECHESSTATEMENTS`, `BILLS`) |
| `speakers` | TEXT | JSON array of speaker names |
| `bills` | TEXT | JSON array of referenced bill numbers |
| `full_text` | TEXT | Full text of the proceeding |

#### `crec_speakers` -- 944,216 rows

Normalized speaker-to-speech linkage with bioguide matching (99.6% coverage).

| Column | Type | Description |
|--------|------|-------------|
| `granule_id` | TEXT PK | References `congressional_record` |
| `speaker_name` | TEXT PK | Speaker name as it appears |
| `bioguide_id` | TEXT | Links to `congress_members` |
| `party` / `state` | TEXT | Party and state at time of speech |

#### `crec_bills` -- 1,561,719 rows

Bill references extracted from CREC MODS XML.

| Column | Type | Description |
|--------|------|-------------|
| `granule_id` | TEXT PK | References `congressional_record` |
| `congress` | INTEGER PK | Congress number |
| `bill_type` | TEXT PK | hr, s, hjres, etc. |
| `bill_number` | INTEGER PK | Bill number |
| `bill_id` | TEXT | Formatted as `{congress}-{type}-{number}` (links to `legislation`) |

---

### Legislation

#### `legislation` -- 167,507 rows

Congressional bills and resolutions (93rd--119th Congress).

| Column | Type | Description |
|--------|------|-------------|
| `bill_id` | TEXT PK | e.g., `119-hr-1234` |
| `congress` | INTEGER | Congress number |
| `bill_type` | TEXT | hr, s, hjres, sjres, hres, sres, hconres, sconres |
| `bill_number` | INTEGER | Bill number |
| `title` | TEXT | Bill title |
| `policy_area` | TEXT | Policy area classification |
| `introduced_date` | TEXT | ISO date |
| `sponsor_bioguide_id` | TEXT | Links to `congress_members` |
| `cosponsor_count` | INTEGER | Number of cosponsors |
| `summary_text` | TEXT | CRS summary |

#### `legislation_actions` -- 1,100,541 rows

Bill action history (introduced, referred, passed, signed, etc.).

| Column | Type | Description |
|--------|------|-------------|
| `bill_id` | TEXT | Links to `legislation` |
| `action_date` | TEXT | ISO date |
| `action_text` | TEXT | Description of action |
| `chamber` | TEXT | Senate or House |

#### `legislation_cosponsors` -- 2,070,889 rows

Bill cosponsors with original vs. later cosponsor distinction.

| Column | Type | Description |
|--------|------|-------------|
| `bill_id` | TEXT PK | Links to `legislation` |
| `bioguide_id` | TEXT PK | Links to `congress_members` |
| `full_name` | TEXT | Cosponsor name |
| `party` / `state` | TEXT | Party and state |
| `is_original_cosponsor` | INTEGER | 1 if signed on at introduction |

#### `legislation_subjects` -- 1,513,256 rows

Subject tags on bills (many-to-many).

| Column | Type | Description |
|--------|------|-------------|
| `bill_id` | TEXT PK | Links to `legislation` |
| `subject` | TEXT PK | Subject tag |

---

### Congress Members & Committees

#### `congress_members` -- 12,763 rows

All current and historical members of Congress. The universal linkage table.

| Column | Type | Description |
|--------|------|-------------|
| `bioguide_id` | TEXT PK | Universal identifier (links everywhere) |
| `full_name` | TEXT | Display name |
| `party` | TEXT | Most recent party |
| `state` | TEXT | Most recent state |
| `chamber` | TEXT | Senate or House (most recent) |
| `is_current` | INTEGER | Currently serving |
| `opensecrets_id` | TEXT | CRP ID |
| `fec_ids` | TEXT | FEC candidate IDs (JSON array) |
| `govtrack_id` | INTEGER | GovTrack ID |
| `lis_id` | TEXT | LIS ID (Senate votes) |
| `trade_count` / `speech_count` / `bills_sponsored` / `vote_count` | INTEGER | Pre-computed stats |

#### `committees` -- 233 rows

Congressional committees and subcommittees.

| Column | Type | Description |
|--------|------|-------------|
| `committee_id` | TEXT PK | e.g., SSEV, HSAG |
| `name` | TEXT | Full committee name |
| `chamber` | TEXT | Senate, House, or Joint |
| `parent_committee_id` | TEXT | Parent (for subcommittees) |

#### `committee_memberships` -- 3,908 rows

Current committee assignments.

| Column | Type | Description |
|--------|------|-------------|
| `bioguide_id` | TEXT PK | Links to `congress_members` |
| `committee_id` | TEXT PK | Links to `committees` |
| `title` | TEXT | Leadership title (Chairman, Ranking Member, etc.) |
| `rank` | INTEGER | Seniority rank |

---

### Votes

#### `roll_call_votes` -- 26,359 rows

Congressional roll call votes (101st--119th Congress, 1989--present).

| Column | Type | Description |
|--------|------|-------------|
| `congress` | INTEGER PK | Congress number |
| `chamber` | TEXT PK | Senate or House |
| `session` | INTEGER PK | Session number |
| `roll_call_number` | INTEGER PK | Roll call number |
| `date` | TEXT | Vote date |
| `question` | TEXT | Question being voted on |
| `result` | TEXT | Passed, Failed, Agreed to, etc. |
| `bill_id` | TEXT | Related bill (links to `legislation`) |
| `yea_count` / `nay_count` | INTEGER | Vote tallies |

#### `member_votes` -- 8,315,224 rows

Individual member positions on each roll call.

| Column | Type | Description |
|--------|------|-------------|
| `congress` / `chamber` / `session` / `roll_call_number` | | Composite key to `roll_call_votes` |
| `bioguide_id` | TEXT | Links to `congress_members` |
| `party` / `state` | TEXT | At time of vote |
| `vote_cast` | TEXT | Yea, Nay, Not Voting, Present |

#### `lis_to_bioguide` -- 326 rows

Crosswalk for Senate vote linkage (LIS IDs to bioguide IDs).

---

### Stock Trades

#### `stock_trades` -- 95,621 rows

Congressional financial disclosures. Senate from efdsearch.senate.gov (15K+), House from clerk PTR PDFs (80K+).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `member_name` | TEXT | Name as filed |
| `bioguide_id` | TEXT | Links to `congress_members` (85.5% coverage) |
| `chamber` | TEXT | Senate or House |
| `transaction_date` | TEXT | ISO date of trade |
| `ticker` | TEXT | Stock ticker (86% coverage) |
| `asset_description` | TEXT | Full description |
| `transaction_type` | TEXT | Purchase, Sale, Sale (Full), Sale (Partial), Exchange |
| `amount_range` | TEXT | Dollar range (e.g., "$1,001 - $15,000") |
| `owner` | TEXT | Self, Spouse, Joint, Child |
| `source_url` | TEXT | Link to original disclosure |

---

### Lobbying Disclosures

#### `lobbying_filings` -- 1,507,321 rows

Senate LDA filings (LD-1 registrations, LD-2 activity reports), 1999--present.

| Column | Type | Description |
|--------|------|-------------|
| `filing_uuid` | TEXT PK | Unique filing ID |
| `filing_type` | TEXT | Filing type code |
| `registrant_name` | TEXT | Lobbying firm or self-filing org |
| `client_name` | TEXT | Client being represented |
| `filing_year` | INTEGER | Year |
| `filing_period` | TEXT | Q1, Q2, mid-year, etc. |
| `amount_reported` | REAL | Dollar amount |
| `is_no_activity` / `is_termination` | INTEGER | Boolean flags |

#### `lobbying_lobbyists` -- 3,464,421 rows

Individual lobbyists listed on filings.

| Column | Type | Description |
|--------|------|-------------|
| `filing_uuid` | TEXT | Filing reference |
| `lobbyist_name` | TEXT | Lobbyist name |
| `covered_position` | TEXT | Former government position (revolving door) |

#### `lobbying_activities` -- 2,754,579 rows

Activities by issue area, with specific issues lobbied and government entities contacted.

| Column | Type | Description |
|--------|------|-------------|
| `filing_uuid` | TEXT | Filing reference |
| `issue_code` | TEXT | General issue area (links to `lobbying_issue_codes`) |
| `specific_issues` | TEXT | Detailed description |
| `government_entities` | TEXT | Bodies contacted |
| `income_amount` / `expense_amount` | INTEGER | Amounts reported |

#### `lobbying_contributions` -- 0 rows (schema present)

LD-203 political contribution reports. Schema exists but not yet populated.

#### `lobbying_issue_codes` -- 79 rows

Reference table mapping issue codes (AGR, ENV, TAX, etc.) to descriptions.

---

### FARA (Foreign Agents)

#### `fara_registrants` -- 7,035 rows

Foreign agent registrations under FARA.

| Column | Type | Description |
|--------|------|-------------|
| `registration_number` | TEXT PK | FARA registration number |
| `name` / `business_name` | TEXT | Registrant identity |
| `registration_date` / `termination_date` | TEXT | Active period |
| `city` / `state` | TEXT | Location |

#### `fara_foreign_principals` -- 17,627 rows

Foreign governments, parties, and entities represented.

| Column | Type | Description |
|--------|------|-------------|
| `registration_number` | TEXT | Links to `fara_registrants` |
| `foreign_principal` | TEXT | Name of foreign principal |
| `country` | TEXT | Country |
| `fp_registration_date` / `fp_termination_date` | TEXT | Period of representation |

#### `fara_registrant_docs` -- 151,348 rows

Documents filed by FARA registrants (supplemental statements, amendments, etc.).

| Column | Type | Description |
|--------|------|-------------|
| `registration_number` | TEXT | Links to `fara_registrants` |
| `date_stamped` | TEXT | Date stamped by DOJ |
| `document_type` | TEXT | Document type |
| `url` | TEXT | URL to document |

#### `fara_short_forms` -- 44,363 rows

Individual employees/associates of FARA registrants.

| Column | Type | Description |
|--------|------|-------------|
| `registration_number` | TEXT | Links to `fara_registrants` |
| `first_name` / `last_name` | TEXT | Individual name |
| `short_form_date` / `short_form_termination_date` | TEXT | Active period |

---

### FEC Campaign Finance

#### `fec_candidates` -- 64,679 rows

FEC candidate records, 2004--2024 election cycles.

| Column | Type | Description |
|--------|------|-------------|
| `cand_id` | TEXT PK | FEC candidate ID |
| `cycle` | INTEGER PK | Election cycle year |
| `cand_name` | TEXT | Candidate name |
| `cand_pty_affiliation` | TEXT | Party |
| `cand_office` | TEXT | H (House), S (Senate), P (President) |
| `cand_office_st` | TEXT | State |
| `cand_ici` | TEXT | Incumbent/Challenger/Open |

#### `fec_committees` -- 154,967 rows

Political committees registered with the FEC.

| Column | Type | Description |
|--------|------|-------------|
| `cmte_id` | TEXT PK | Committee ID |
| `cycle` | INTEGER PK | Election cycle |
| `cmte_nm` | TEXT | Committee name |
| `cmte_tp` | TEXT | Committee type |
| `connected_org_nm` | TEXT | Connected organization |
| `cand_id` | TEXT | Associated candidate ID |

#### `fec_contributions` -- 4,395,926 rows

Contributions to federal candidates (subset from FEC bulk data).

| Column | Type | Description |
|--------|------|-------------|
| `cmte_id` | TEXT | Receiving committee |
| `cand_id` | TEXT | Candidate |
| `transaction_dt` | TEXT | Transaction date |
| `transaction_amt` | REAL | Dollar amount |
| `employer` / `occupation` | TEXT | Contributor employment |
| `state` | TEXT | Contributor state |
| `cycle` | INTEGER | Election cycle |

#### `fec_candidate_crosswalk` -- 1,711 rows

Maps FEC candidate IDs to bioguide IDs for Congress member linkage.

| Column | Type | Description |
|--------|------|-------------|
| `fec_candidate_id` | TEXT PK | FEC candidate ID |
| `bioguide_id` | TEXT PK | Links to `congress_members` |

#### Pre-computed FEC aggregates

| Table | Rows | Description |
|-------|------|-------------|
| `fec_employer_totals` | 352,103 | Employers ranked by total contributions |
| `fec_employer_to_candidate` | 476,534 | Employer-to-candidate contribution flows |
| `fec_employer_to_party` | 286,908 | Employer-to-party contribution flows |
| `fec_top_occupations` | 666,254 | Occupations by total contributions per employer |

---

### Federal Spending

#### `spending_awards` -- 863,632 rows

Federal grants and contracts from USAspending.gov (20 agencies, FY2017--2026).

| Column | Type | Description |
|--------|------|-------------|
| `generated_internal_id` | TEXT PK | USAspending internal ID |
| `award_id` | TEXT | Award identifier |
| `agency` / `sub_agency` | TEXT | Awarding agency |
| `award_category` | TEXT | `grants` or `contracts` |
| `recipient_name` | TEXT | Award recipient |
| `award_amount` | REAL | Total dollars |
| `description` | TEXT | Award description |
| `fiscal_year` | INTEGER | Federal fiscal year |
| `cfda_number` | TEXT | CFDA program number (grants) |
| `naics_code` | TEXT | NAICS code (contracts) |

---

## Standalone Databases

### lobbying.db (13 GB)

Full lobbying data including raw JSON and additional tables not in openregs.db.

| Table | Rows | Description |
|-------|------|-------------|
| `lobbying_filings_raw` | 1,908,114 | All filings with `raw_json` column |
| `lobbying_registrations` | 137,404 | LD-1 registration details (client/registrant descriptions, addresses, countries) |
| `lobbying_activities` | 3,528,264 | Activities (more complete than openregs.db subset) |
| `lobbying_lobbyists` | 4,376,087 | Individual lobbyists |
| `lobbying_contributions` | 0 | LD-203 contributions (schema ready) |
| `lobbying_issue_codes` | 79 | Reference: issue area codes |
| `lobbying_gov_entities` | 257 | Reference: government entities |
| `lobbying_filing_types` | 150 | Reference: filing type codes |

### fec.db (47 GB)

Full FEC bulk data with complete individual contribution records.

| Table | Rows | Description |
|-------|------|-------------|
| `fec_candidates` | 64,679 | Same as openregs.db |
| `fec_committees` | 154,967 | Same as openregs.db |
| `fec_individual_contributions` | 103,897,425 | ALL individual contributions (full dataset) |
| `fec_contributions_to_candidates` | 4,395,926 | Contributions routed to candidates |
| `fec_committee_transactions` | 44,030,642 | Committee-to-committee transactions |
| `fec_candidate_committee_linkages` | 61,606 | Candidate-committee relationships |
| `fec_candidate_crosswalk` | 1,711 | FEC-to-bioguide mapping |

### fara.db (42 MB)

Complete FARA data (same tables as in openregs.db, with FTS indexes).

### votes.db (985 MB)

Complete vote records (same tables as in openregs.db).

| Table | Rows | Description |
|-------|------|-------------|
| `roll_call_votes` | 26,359 | Roll call votes |
| `member_votes` | 8,315,224 | Individual vote positions |

### aphis.db (53 MB)

APHIS animal welfare data from Salesforce API.

| Table | Rows | Description |
|-------|------|-------------|
| `facilities` | 15,119 | Licensed/registered animal facilities |
| `inspections` | 110,400 | Inspection records with violation counts |
| `annual_reports` | 0 | Annual reports (not yet populated) |
| `enforcement_actions` | 0 | Enforcement actions (not yet populated) |
| `teachable_moments` | 0 | Teachable moments (not yet populated) |

---

## Materialized Views (openregs.db)

| View | Description |
|------|-------------|
| `agency_overview` | Per-agency summary stats (dockets, docs, comments, rules) |
| `member_overview` | Current members with trade/speech/bill/vote counts |
| `member_stats` | Per-member stats across all domains |
| `regulatory_pipeline` | Dockets with both proposed and final rules |
| `top_dockets` | Most-commented dockets with submitter breakdown |
| `top_submitters` | Most active organizational commenters |
| `docket_stats` | Per-docket comment/document counts |
| `comments_by_year` | Annual comment volume by agency |
| `comments_monthly` | Monthly comment trends |
| `fr_by_year` | Annual FR publication volume by type |
| `speaker_activity` | CREC speaker summary with party info |
| `bills_floor_time` | Bills with most floor discussion |
| `stock_trades_by_ticker` | Aggregate trading by ticker |
| `committee_member_trades` | Committee memberships cross-referenced with trades |
| `lobbying_by_year` | Annual lobbying spending |
| `top_lobbying_clients` | Clients ranked by total spending |
| `spending_by_agency` | Spending totals per agency/category |

---

## Full-Text Search Indexes (FTS5)

| Index | Indexed Columns |
|-------|----------------|
| `federal_register_fts` | title, abstract, agency_names, excerpts |
| `dockets_fts` | title, agency_id, summary |
| `documents_fts` | title, agency_id, document_type |
| `comments_fts` | title, submitter_name, agency_id, submitter_type, docket_id |
| `cfr_fts` | section_number, section_heading, part_name, agency, full_text |
| `crec_fts` | title, chamber, full_text |
| `legislation_fts` | title, policy_area, bill_id |
| `spending_awards_fts` | recipient_name, agency, sub_agency |
| `lobbying_fts` | client_name, registrant_name, specific_issues, government_entities, issue_code |
| `fara_registrants_fts` | name, business_name, city, state |
| `fara_foreign_principals_fts` | registrant_name, foreign_principal, country |
| `fec_employer_fts` | employer |

---

## Cross-References

### Primary linkage keys

| Key | Links | Coverage |
|-----|-------|----------|
| `bioguide_id` | `congress_members` to `crec_speakers`, `stock_trades`, `legislation` (sponsor), `legislation_cosponsors`, `committee_memberships`, `member_votes`, `fec_candidate_crosswalk` | Universal member key |
| `bill_id` | `legislation` to `legislation_actions`, `legislation_cosponsors`, `legislation_subjects`, `crec_bills`, `roll_call_votes` | Format: `{congress}-{type}-{number}` |
| `docket_id` | `dockets` to `documents`, `comments` | Regulations.gov docket ID |
| `document_number` | `federal_register` to `fr_regs_crossref`, `presidential_documents`, `federal_register_agencies` | FR document number |
| `registration_number` | `fara_registrants` to `fara_foreign_principals`, `fara_registrant_docs`, `fara_short_forms` | FARA registration number |
| `filing_uuid` | `lobbying_filings` to `lobbying_lobbyists`, `lobbying_activities`, `lobbying_contributions` | Lobbying filing ID |
| `cand_id` | `fec_candidates` to `fec_committees`, `fec_contributions`, `fec_candidate_crosswalk` | FEC candidate ID |
| `cmte_id` | `fec_committees` to `fec_contributions` | FEC committee ID |
| `committee_id` | `committees` to `committee_memberships` | Congressional committee ID |
| `granule_id` | `congressional_record` to `crec_speakers`, `crec_bills` | GovInfo granule ID |
| `cert_number` | `facilities` to `inspections` (aphis.db) | APHIS certificate number |

### Cross-dataset joins

| From | To | Via |
|------|----|-----|
| FR documents | Regs.gov documents | `fr_regs_crossref` (70K links) |
| Regs.gov documents | FR documents | `fr_doc_num` = `document_number` |
| Congress members | FEC candidates | `fec_candidate_crosswalk` (1,711 links) |
| Congress members | Stock trades | `stock_trades.bioguide_id` (85.5% coverage) |
| Congress members | Floor speeches | `crec_speakers.bioguide_id` (99.6% coverage) |
| Congress members | Vote records | `member_votes.bioguide_id` |
| Congress members | Sponsored bills | `legislation.sponsor_bioguide_id` |
| Congress members | Cosponsored bills | `legislation_cosponsors.bioguide_id` |
| Congress members | Committees | `committee_memberships.bioguide_id` |
| Bills | Floor speeches | `crec_bills.bill_id` |
| Bills | Roll call votes | `roll_call_votes.bill_id` |
| Senate votes | Bioguide | `lis_to_bioguide` crosswalk (326 senators) |

---

## Directory Structure

```

├── openregs.db                     # Primary database (22 GB, deployed)
├── lobbying.db                     # Full lobbying data (13 GB)
├── fec.db                          # Full FEC data (47 GB)
├── fara.db                         # Full FARA data (42 MB)
├── votes.db                        # Full vote records (985 MB)
├── federal_register/
│   └── raw/{YYYY}/{MM}/page_{NNNN}.json          # FR API responses (1.5 GB)
├── regulations_gov/
│   ├── dockets/{AGENCY}/page_{NNNN}.json
│   ├── documents/{AGENCY}/{YYYY}/page_{NNNN}.json
│   └── comments/
│       ├── headers/{AGENCY}/{YYYY}/{MM}/page_{NNNN}.json
│       └── details/                               # Full comment detail batches
├── congress_gov/                                  # BILLSTATUS XML + bill listings (3.2 GB)
├── ecfr/                                          # eCFR title XML + parsed JSON (834 MB)
├── congressional_record/                          # CREC daily packages (8.1 GB)
├── usaspending/                                   # USAspending JSON (2.1 GB)
├── lobbying_raw/                                  # Senate LDA raw JSON (8.4 GB)
├── fec/                                           # FEC bulk data files (17 GB)
├── fara/                                          # FARA downloads (4.9 MB)
├── congress_members/                              # congress-legislators YAML
├── stock_trades/
│   ├── senate_efd/                                # Senate eFD data
│   ├── house/                                     # House filing indexes
│   ├── house_ptrs/                                # House PTR PDFs
│   ├── senate_raw.json                            # Raw Senate data
│   └── senate_trades.json                         # Parsed Senate trades
├── aphis/
│   ├── scripts/                                   # APHIS extraction pipeline
│   └── db/aphis.db                                # APHIS database (53 MB)
├── scripts/                                       # Pipeline scripts (see below)
│   └── config.json                                # API keys (NEVER commit)
├── deploy/
│   ├── deploy.sh                                  # Deployment script
│   └── metadata.json                              # Datasette configuration
├── logs/                                          # State files + logs
└── DATA_DICTIONARY.md                             # This file
```

---

## Pipeline Scripts

Scripts are numbered in execution order. Run from ``.

### Data download scripts

| Script | Source | Auth | Time |
|--------|--------|------|------|
| `01_federal_register.py` | FR API | None | ~30 min |
| `02_regs_gov_dockets_docs.py` | Regulations.gov | API key | ~4-6 hr |
| `03_regs_gov_comments.py` | Regulations.gov | API key | ~20-40 hr |
| `04_backfill_comments.py` | Regulations.gov | API key | ~4 hr |
| `06_fws_aphis_headers.py` | Regulations.gov | API key | ~12-15 hr |
| `07_full_comment_details.py` | Regulations.gov | API key | Weeks |
| `08_usaspending.py` | USAspending.gov | None | ~2 hr |
| `08_fec_campaign_finance.py` | FEC bulk data | None | ~1 hr |
| `09_congress_gov.py` | GovInfo + Congress.gov | API key | ~50 min |
| `09_fec_employer_aggregates.py` | Local (from fec.db) | None | ~5 min |
| `10_ecfr.py` | GovInfo bulk XML | None | ~30 sec |
| `10_congress_votes.py` | Congress.gov API | API key | ~2 hr |
| `11_congressional_record.py` | GovInfo CREC | None | ~6-8 hr |
| `11_fara.py` | FARA.gov | None | ~5 min |
| `12_congress_stock_trades.py` | GitHub + House/Senate | None | ~5 sec |
| `12_expand_agencies.py` | Local | None | ~1 sec |
| `13_senate_efd.py` | efdsearch.senate.gov | None | ~30 min |
| `14_house_fd_ptr.py` | House clerk PDFs | None | ~2.5 hr |
| `15_lobbying_disclosure.py` | Senate LDA API | API key | ~2 hr |
| `16_backfill_dockets.py` | Regulations.gov | API key | ~1 hr |

### Build and deploy

| Script | Purpose | Time |
|--------|---------|------|
| `05_build_database.py` | Build openregs.db from all raw data | ~5 min |
| `build_lis_crosswalk.py` | Build Senate LIS-to-bioguide crosswalk | ~1 sec |
| `refresh_member_stats.py` | Refresh pre-computed member stats | ~1 min |
| `deploy/deploy.sh` | Upload DB + templates to server | ~15 min |

All download scripts are idempotent. State files in `logs/` track completed work. Safe to re-run at any time. `05_build_database.py` rebuilds the entire database from whatever raw data exists.

---

## Rate Limits & Resilience

| API | Limit | Script behavior |
|-----|-------|-----------------|
| Federal Register | None | 0.5s polite delay between requests |
| Regulations.gov | 1,000 req/hr | 3.6s minimum interval; HTTP 429 exponential backoff |
| Congress.gov | 5,000 req/hr | Polite delay; retries on server errors |
| Senate LDA | ~1 req/sec (throttled) | Adaptive delay; retries with backoff |
| USAspending.gov | None | Standard retry on errors |
| FEC | N/A (bulk files) | Direct download |
| FARA | None | Standard retry |
| Senate eFD | Throttled | Session-based; retries |
| House FD | None | PDF download with retries |

All scripts: retry up to 3-5 times with exponential backoff on server errors. Graceful shutdown on SIGINT/SIGTERM (saves state before exiting).

**Key constraint**: Regulations.gov returns max 5,000 results per query (20 pages x 250/page). Scripts subdivide by year, then month, then day to avoid truncation. EPA months exceeding 5,000 were backfilled via daily windows, recovering ~308K comments.

---

## Known Limitations

1. **Comment details in progress** -- 42K of 3.7M downloaded. Full text requires per-comment API calls at ~780 req/hr.
2. **FDA comments incomplete** -- Only through 2022. 2023-2026 missing (~400K-800K estimated).
3. **House PTR parsing** -- ~30% of House PTR PDFs are scanned images (mostly pre-2017). Filing indexes available for all.
4. **EPA truncation** -- One day (2020-12-30) still exceeds daily API cap; ~144 comments missing.
5. **Submitter name parsing** -- Regex-based, ~62% coverage. Organization detection is heuristic.
6. **5 agencies only** -- Dockets/documents/comments cover EPA, FDA, USDA, FWS, APHIS. Federal Register covers all 444 agencies.
7. **FR cross-reference coverage** -- 70K of 728K documents linked (9.6%). Most documents lack `fr_doc_num`.
8. **Lobbying contributions** -- Schema exists but LD-203 data not yet loaded.
9. **APHIS sub-tables** -- Annual reports, enforcement actions, and teachable moments have schemas but no data yet.
10. **FEC in openregs.db** -- Contains 4.4M contributions to candidates. Full individual contributions (104M rows) only in standalone fec.db.
