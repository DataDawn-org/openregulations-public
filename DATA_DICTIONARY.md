# OpenRegs Data Dictionary

> Comprehensive U.S. federal government data: regulations, legislation, congressional floor speeches, committee hearings, CRS reports, executive nominations, treaties, GAO reports, lobbying disclosures, campaign finance, stock trades, foreign agent registrations, and federal spending.
> Updated: 2026-03-08

---

## Data Sources & APIs

| Source | API / URL | Auth | Rate Limit |
|--------|-----------|------|------------|
| Federal Register | `federalregister.gov/api/v1` | None | None (polite 0.5s delay) |
| Regulations.gov | `api.regulations.gov/v4` | API key (`X-Api-Key`) | 1,000 req/hour |
| GovInfo (BILLSTATUS, CREC, eCFR) | `govinfo.gov/bulkdata` | None | None |
| Congress.gov (votes, legislation) | `api.congress.gov/v3` | API key | 5,000 req/hour |
| USAspending.gov | `api.usaspending.gov/api/v2` | None | None |
| LDA.gov (lobbying) | `lda.gov/api/v1` | API key | Throttled (~1 req/sec) |
| Senate eFD (stock trades) | `efdsearch.senate.gov` | None | Throttled |
| House FD (stock trades) | `disclosures-clerk.house.gov` | None | None (PDF downloads) |
| FEC (campaign finance) | `fec.gov/data/browse-data` | None (bulk files) | N/A |
| FARA (foreign agents) | `efile.fara.gov` | None | None |
| APHIS (animal welfare) | `aphis.my.site.com` (Salesforce Aura) | None | Throttled |
| GovInfo CHRG (hearings) | `api.govinfo.gov` (collection CHRG) | API key | None |
| Congress.gov CRS Reports | `api.congress.gov/v3/crsreport` | API key | 5,000 req/hour |
| Congress.gov Nominations | `api.congress.gov/v3/nomination` | API key | 5,000 req/hour |
| Congress.gov Treaties | `api.congress.gov/v3/treaty` | API key | 5,000 req/hour |
| GovInfo GAOREPORTS (GAO) | `api.govinfo.gov` (collection GAOREPORTS) | API key | None |
| SEC EDGAR (ticker SIC) | `sec.gov/cgi-bin/browse-edgar` | None (User-Agent) | 10 req/sec |

API keys are stored in `scripts/config.json` (never committed to git).

---

## Databases

| Database | Path | Size | Tables | Purpose |
|----------|------|------|--------|---------|
| openregs.db | `openregs.db` | 22 GB | 60+ | Primary database (deployed to Datasette) |
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

#### `presidential_documents` -- 5,905 rows

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

#### `dockets` -- 164,759 rows

Regulatory dockets from 5 agencies (EPA, FDA, USDA, FWS, APHIS). Includes 72K stub records backfilled from documents/comments for referential integrity.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Docket ID (e.g., `EPA-HQ-OAR-2024-0001`) |
| `agency_id` | TEXT | Agency code |
| `title` | TEXT | Docket title (null for stubs) |
| `docket_type` | TEXT | `Rulemaking` or `Nonrulemaking` |

#### `docket_summary` -- 49,900 rows

AI-generated docket summaries for dockets with titles.

| Column | Type | Description |
|--------|------|-------------|
| `docket_id` | TEXT | Docket ID |
| `summary` | TEXT | Plain-language summary |

#### `documents` -- 1,213,019 rows

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

#### `comments` -- 3,907,894 rows

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

Coverage: FWS 1.6M, EPA 1.2M, FDA 611K, APHIS 507K, USDA 22K.

#### `comment_details` -- 86,507 rows

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

#### `fr_regs_crossref` -- 185,900 rows

Cross-reference linking FR documents to Regulations.gov documents. Built via 5-phase matching (direct, normalized, leading-zero strip, date+title, case-insensitive).

| Column | Type | Description |
|--------|------|-------------|
| `fr_document_number` | TEXT PK | FR document number |
| `regs_document_id` | TEXT PK | Regulations.gov document ID |

#### `agency_map` -- 15 rows

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

#### `stock_trades` -- 64,220 rows

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

#### `lobbying_filings` -- 1,908,114 rows

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

#### `lobbying_lobbyists` -- 4,376,087 rows

Individual lobbyists listed on filings.

| Column | Type | Description |
|--------|------|-------------|
| `filing_uuid` | TEXT | Filing reference |
| `lobbyist_name` | TEXT | Lobbyist name |
| `covered_position` | TEXT | Former government position (revolving door) |

#### `lobbying_activities` -- 3,528,264 rows

Activities by issue area, with specific issues lobbied and government entities contacted.

| Column | Type | Description |
|--------|------|-------------|
| `filing_uuid` | TEXT | Filing reference |
| `issue_code` | TEXT | General issue area (links to `lobbying_issue_codes`) |
| `specific_issues` | TEXT | Detailed description |
| `government_entities` | TEXT | Bodies contacted |
| `income_amount` / `expense_amount` | INTEGER | Amounts reported |

#### `lobbying_contributions` -- 3,492,672 rows

LD-203 political contribution reports (lobbyist contributions to federal candidates/officeholders).

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

### Committee Hearings

#### `hearings` -- 46,177 rows

Committee hearing metadata from GovInfo CHRG collection (1993--present).

| Column | Type | Description |
|--------|------|-------------|
| `package_id` | TEXT PK | GovInfo package identifier (e.g., `CHRG-118shrg12345`) |
| `title` | TEXT | Hearing title |
| `chamber` | TEXT | `Senate`, `House`, or `Joint` |
| `congress` | INTEGER | Congress number |
| `session` | TEXT | Session number |
| `date_issued` | TEXT | ISO date of hearing |
| `committees` | TEXT | Committee name(s) |
| `detail_url` | TEXT | GovInfo detail URL |
| `html_url` | TEXT | Link to HTML transcript |
| `pdf_url` | TEXT | Link to PDF transcript |

#### `hearing_witnesses` -- ~100,000+ rows

Witnesses who testified at committee hearings. Extracted from MODS XML metadata.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `package_id` | TEXT | Links to `hearings` |
| `name` | TEXT | Witness name |
| `title` | TEXT | Witness title/position |
| `organization` | TEXT | Witness organization or affiliation |
| `location` | TEXT | Location |

#### `hearing_members` -- ~200,000+ rows

Congress members who participated in committee hearings. Includes bioguide linkage.

| Column | Type | Description |
|--------|------|-------------|
| `package_id` | TEXT PK | Links to `hearings` |
| `name` | TEXT PK | Member name |
| `role` | TEXT | Role (e.g., Chair) |
| `bioguide_id` | TEXT | Links to `congress_members` |

---

### CRS Reports

#### `crs_reports` -- 13,629 rows

Congressional Research Service reports from Congress.gov API.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | CRS report ID (e.g., `R12345`, `RL33456`) |
| `title` | TEXT | Report title |
| `publish_date` | TEXT | ISO date of publication |
| `update_date` | TEXT | Most recent update date |
| `status` | TEXT | Report status |
| `content_type` | TEXT | Content type |
| `authors` | TEXT | Report authors |
| `topics` | TEXT | Topic/subject classifications |
| `summary` | TEXT | Report summary text |
| `pdf_url` | TEXT | Link to PDF |
| `html_url` | TEXT | Link to HTML |

#### `crs_report_bills` -- ~20,000+ rows

Cross-references between CRS reports and legislation.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `report_id` | TEXT | Links to `crs_reports` |
| `bill_title` | TEXT | Bill title |
| `congress` | INTEGER | Congress number |
| `bill_type` | TEXT | hr, s, hjres, etc. |
| `bill_number` | INTEGER | Bill number |
| `bill_id` | TEXT | Formatted as `{congress}-{type}-{number}` (links to `legislation`) |

---

### Executive Nominations

#### `nominations` -- 40,067 rows

Executive nominations submitted to the Senate (Congresses 100--119).

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Nomination identifier |
| `congress` | INTEGER | Congress number |
| `number` | INTEGER | Nomination number |
| `part_number` | TEXT | Part number (for multi-part nominations) |
| `citation` | TEXT | Nomination citation |
| `description` | TEXT | Description (nominee name, position, agency) |
| `organization` | TEXT | Agency or department |
| `received_date` | TEXT | Date received by Senate |
| `authority_date` | TEXT | Authority date |
| `is_civilian` | INTEGER | Boolean: civilian nomination |
| `is_military` | INTEGER | Boolean: military nomination |
| `status` | TEXT | `Confirmed`, `Withdrawn`, `Returned`, `Rejected`, `Pending` |
| `vote_yea` | INTEGER | Confirmation vote yea count |
| `vote_nay` | INTEGER | Confirmation vote nay count |

#### `nomination_actions` -- ~50,000+ rows

Action history for executive nominations.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `nomination_id` | TEXT | Links to `nominations` |
| `action_date` | TEXT | ISO date of action |
| `action_text` | TEXT | Description of action |
| `action_type` | TEXT | Action type code |

---

### Treaties

#### `treaties` -- 777 rows

Treaties submitted to the Senate (Congresses 89--119).

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Treaty identifier |
| `congress` | INTEGER | Congress number |
| `number` | INTEGER | Treaty document number |
| `title` | TEXT | Treaty title |
| `topic` | TEXT | Treaty topic |
| `transmitted_date` | TEXT | Date transmitted to Senate |
| `in_force_date` | TEXT | Date treaty entered into force |
| `countries` | TEXT | Countries or parties involved |
| `index_terms` | TEXT | Subject index terms |
| `resolution_text` | TEXT | Senate resolution text |

#### `treaty_actions` -- ~2,000+ rows

Action history for treaties.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `treaty_id` | TEXT | Links to `treaties` |
| `action_date` | TEXT | ISO date of action |
| `action_text` | TEXT | Description of action |
| `action_type` | TEXT | Action type code |

---

### GAO Reports

#### `gao_reports` -- 16,569 rows

Government Accountability Office reports and Comptroller General decisions from GovInfo (1994--2008).

| Column | Type | Description |
|--------|------|-------------|
| `package_id` | TEXT PK | GovInfo package identifier |
| `title` | TEXT | Report title |
| `date_issued` | TEXT | ISO date |
| `report_number` | TEXT | GAO report number |
| `document_type` | TEXT | Report type |
| `doc_class` | TEXT | Document classification |
| `abstract` | TEXT | Report abstract/summary |
| `subjects` | TEXT | Subject classifications |
| `public_laws` | TEXT | Referenced public laws |
| `usc_references` | TEXT | U.S. Code citations |
| `statute_references` | TEXT | Statutes at Large citations |
| `citation` | TEXT | Citation |
| `pdf_url` | TEXT | Link to PDF |
| `html_url` | TEXT | Link to HTML |
| `detail_url` | TEXT | GovInfo detail URL |
| `sudocs` | TEXT | SuDoc classification number |

---

### CBO Cost Estimates

#### `cbo_cost_estimates` -- ~17,200 rows

Congressional Budget Office cost estimates linked to legislation. Extracted from Congress.gov bill data.

| Column | Type | Description |
|--------|------|-------------|
| `bill_id` | TEXT | Links to `legislation` |
| `pub_date` | TEXT | Publication date |
| `title` | TEXT | Cost estimate title |
| `description` | TEXT | Cost estimate description |
| `url` | TEXT | Link to CBO cost estimate |

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

### Lobbying Cross-References

#### `lobbying_bills` -- ~500,000+ rows

Cross-reference linking lobbying filings to specific legislation. Bill references are extracted from the `specific_issues` text in `lobbying_activities` using regex pattern matching.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `filing_uuid` | TEXT | Links to `lobbying_filings` |
| `bill_congress` | INTEGER | Congress number (derived from filing year) |
| `bill_type` | TEXT | hr, s, hjres, sjres, etc. |
| `bill_number` | INTEGER | Bill number |
| `bill_id` | TEXT | Formatted bill ID (links to `legislation`) |
| `client_name` | TEXT | Lobbying client |
| `issue_code` | TEXT | Issue area code |

#### `lobbying_issue_agencies` -- 79 rows

Reference table mapping lobbying issue codes to relevant federal agencies and congressional committees.

| Column | Type | Description |
|--------|------|-------------|
| `issue_code` | TEXT PK | Issue area code (links to `lobbying_issue_codes`) |
| `issue_description` | TEXT | Human-readable description |
| `primary_agencies` | TEXT | Primary federal agencies involved |
| `secondary_agencies` | TEXT | Secondary agencies |
| `regs_gov_agency_prefixes` | TEXT | Regulations.gov docket ID prefixes |
| `federal_register_agency_slugs` | TEXT | FR agency slugs |
| `related_committee_ids` | TEXT | Congressional committee IDs |
| `notes` | TEXT | Additional notes |

---

### Committee-Sector Reference

#### `committee_sectors` -- ~30 rows

Maps congressional committees to the industries they regulate. Used for identifying potential conflicts of interest in stock trades.

| Column | Type | Description |
|--------|------|-------------|
| `committee_id` | TEXT PK | Links to `committees` |
| `committee_name` | TEXT | Committee name |
| `chamber` | TEXT | Senate, House, or Joint |
| `regulated_sectors` | TEXT | Description of regulated sectors |
| `example_tickers` | TEXT | Representative stock tickers |
| `gics_sectors` | TEXT | GICS sector classifications |
| `sic_ranges` | TEXT | SIC code ranges |
| `notes` | TEXT | Additional notes |

#### `committee_jurisdiction` -- ~100+ rows

Tiered committee jurisdiction mapping with SIC codes. Each committee can have primary, secondary, and tertiary jurisdiction over different industry sectors.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `committee_id` | TEXT | Links to `committees` |
| `committee_name` | TEXT | Committee name |
| `chamber` | TEXT | Senate, House, or Joint |
| `jurisdiction_desc` | TEXT | Description of jurisdiction |
| `sic_codes` | TEXT | Comma-separated SIC codes or ranges |
| `jurisdiction_tier` | TEXT | `primary`, `secondary`, or `tertiary` |
| `notes` | TEXT | Additional notes |

#### `committee_sic_ranges` -- ~300+ rows

Expanded SIC code ranges from `committee_jurisdiction`. Each range maps a committee to a contiguous block of SIC codes for efficient matching against `ticker_sic`.

| Column | Type | Description |
|--------|------|-------------|
| `committee_id` | TEXT | Links to `committees` |
| `sic_start` | INTEGER | Start of SIC range |
| `sic_end` | INTEGER | End of SIC range |
| `jurisdiction_tier` | TEXT | `primary`, `secondary`, or `tertiary` |

#### `ticker_sic` -- ~2,000 rows

Maps stock tickers to SIC industry codes from SEC EDGAR. Only includes tickers that appear in `stock_trades`.

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | TEXT PK | Stock ticker symbol |
| `cik` | TEXT | SEC CIK number |
| `company_name` | TEXT | Company name from EDGAR |
| `sic_code` | TEXT | SIC industry code |
| `sic_description` | TEXT | SIC code description |
| `exchange` | TEXT | Stock exchange |

---

### Pre-computed Summary Tables

These materialized tables replace expensive GROUP BY queries for the explore pages.

#### `docket_summary` -- ~27,000 rows

Best Federal Register abstract per docket, used to enrich `dockets_fts`.

| Column | Type | Description |
|--------|------|-------------|
| `docket_id` | TEXT | Links to `dockets` |
| `summary` | TEXT | FR abstract text for the docket |

#### `lobbying_issue_summary` -- 80 rows

Pre-computed lobbying activity counts per issue code.

| Column | Type | Description |
|--------|------|-------------|
| `issue_code` | TEXT | Issue area code |
| `description` | TEXT | Issue description |
| `filing_count` | INTEGER | Number of filings |
| `client_count` | INTEGER | Unique clients |
| `total_income` | INTEGER | Total reported income |

#### `legislation_policy_summary` -- ~34 rows

Pre-computed bill counts per policy area.

| Column | Type | Description |
|--------|------|-------------|
| `policy_area` | TEXT | Policy area classification |
| `bill_count` | INTEGER | Number of bills |
| `sponsor_count` | INTEGER | Unique sponsors |
| `from_congress` | INTEGER | Earliest congress |
| `to_congress` | INTEGER | Latest congress |

#### `legislation_sponsor_summary` -- ~1,400 rows

Pre-computed bill counts per sponsor.

| Column | Type | Description |
|--------|------|-------------|
| `sponsor_name` | TEXT | Sponsor name |
| `sponsor_party` | TEXT | Party |
| `sponsor_state` | TEXT | State |
| `sponsor_bioguide_id` | TEXT | Links to `congress_members` |
| `bill_count` | INTEGER | Number of bills sponsored |

#### `spending_agency_summary` -- ~15 rows

Pre-computed spending totals per agency.

| Column | Type | Description |
|--------|------|-------------|
| `name` | TEXT | Agency name |
| `award_count` | INTEGER | Number of awards |
| `total_spending` | INTEGER | Total award dollars |
| `sub_count` | INTEGER | Number of sub-agencies |

---

### Research / Cross-Domain Tables

These materialized tables pre-compute expensive cross-domain joins for the Research explore page.

#### `speeches_near_trades` -- varies

Floor speeches delivered within 7 days of a stock trade by the same member. Pre-computed because the live join takes 8+ seconds.

| Column | Type | Description |
|--------|------|-------------|
| `full_name` | TEXT | Member name |
| `party` | TEXT | Party |
| `state` | TEXT | State |
| `bioguide_id` | TEXT | Links to `congress_members` |
| `trade_date` | TEXT | Stock trade date |
| `ticker` | TEXT | Stock ticker traded |
| `asset_description` | TEXT | Asset description |
| `transaction_type` | TEXT | Purchase, Sale, etc. |
| `amount_range` | TEXT | Dollar range |
| `speech_date` | TEXT | Floor speech date |
| `speech_title` | TEXT | Speech title |
| `days_apart` | INTEGER | Days between trade and speech |

#### `committee_trade_conflicts` -- varies

Trades by committee members in stocks whose SIC code falls within the committee's jurisdictional SIC ranges. Uses `ticker_sic` and `committee_sic_ranges` for industry-level matching.

| Column | Type | Description |
|--------|------|-------------|
| `full_name` | TEXT | Member name |
| `party` | TEXT | Party |
| `state` | TEXT | State |
| `bioguide_id` | TEXT | Links to `congress_members` |
| `committee_name` | TEXT | Committee name |
| `jurisdiction_desc` | TEXT | Jurisdiction description |
| `jurisdiction_tier` | TEXT | primary, secondary, or tertiary |
| `sic_code` | TEXT | Stock's SIC code |
| `sic_description` | TEXT | SIC description |
| `transaction_date` | TEXT | Trade date |
| `ticker` | TEXT | Stock ticker |
| `asset_description` | TEXT | Asset description |
| `transaction_type` | TEXT | Purchase, Sale, etc. |
| `amount_range` | TEXT | Dollar range |

#### `committee_donor_summary` -- varies

PAC donations to current committee members, pre-computed because the live join takes 95+ seconds.

| Column | Type | Description |
|--------|------|-------------|
| `committee_name` | TEXT | Congressional committee name |
| `member_name` | TEXT | Member name |
| `party` | TEXT | Party |
| `state` | TEXT | State |
| `bioguide_id` | TEXT | Links to `congress_members` |
| `donor_committee` | TEXT | Donating PAC/committee name |
| `donor_cmte_id` | TEXT | FEC committee ID |
| `total_donated` | REAL | Total contribution amount |
| `contribution_count` | INTEGER | Number of contributions |

#### `lobbying_bill_summary` -- varies

Bills most frequently referenced in lobbying filings. Aggregated from `lobbying_bills`.

| Column | Type | Description |
|--------|------|-------------|
| `bill_id` | TEXT | Links to `legislation` |
| `bill_title` | TEXT | Bill title |
| `policy_area` | TEXT | Policy area |
| `lobby_filings` | INTEGER | Number of lobbying filings referencing this bill |
| `unique_clients` | INTEGER | Number of unique lobbying clients |
| `issue_codes` | TEXT | Issue area codes |
| `clients` | TEXT | Client names (concatenated) |

#### `witness_lobby_overlap` -- varies

Organizations that both testified at committee hearings and were lobbying clients. Matched by organization name.

| Column | Type | Description |
|--------|------|-------------|
| `organization` | TEXT | Organization name |
| `hearings_testified` | INTEGER | Number of hearings where they testified |
| `lobby_filings` | INTEGER | Number of lobbying filings as client |
| `total_lobby_income` | INTEGER | Total lobbying income reported |
| `first_hearing` | TEXT | Date of first hearing |
| `last_hearing` | TEXT | Date of last hearing |
| `lobby_issues` | TEXT | Lobbying issue codes |

#### `commenter_lobby_overlap` -- varies

Organizations that both submitted regulatory comments and were lobbying clients. Matched by organization name from `comment_details`.

| Column | Type | Description |
|--------|------|-------------|
| `organization` | TEXT | Organization name |
| `comments_filed` | INTEGER | Number of comments filed |
| `agencies_commented` | INTEGER | Number of agencies commented to |
| `lobby_filings` | INTEGER | Number of lobbying filings as client |
| `total_lobby_income` | INTEGER | Total lobbying income reported |
| `lobby_issues` | TEXT | Lobbying issue codes |

#### `revolving_door` -- varies

Former members of Congress who became registered lobbyists. Matched by name between `congress_members` and `lobbying_lobbyists` where `covered_position` indicates former Congressional service.

| Column | Type | Description |
|--------|------|-------------|
| `bioguide_id` | TEXT | Links to `congress_members` |
| `full_name` | TEXT | Member name |
| `party` | TEXT | Party |
| `state` | TEXT | State |
| `congress_chamber` | TEXT | Senate or House (when serving) |
| `lobbying_filing_count` | INTEGER | Number of lobbying filings |
| `client_count` | INTEGER | Unique clients lobbied for |
| `firm_count` | INTEGER | Lobbying firms worked at |
| `first_lobbying_year` | INTEGER | First year of lobbying activity |
| `last_lobbying_year` | INTEGER | Most recent lobbying year |
| `total_reported_income` | INTEGER | Total lobbying income reported |
| `lobbying_firms` | TEXT | Names of lobbying firms |
| `covered_position_sample` | TEXT | Sample covered position description |

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
| `nomination_rates` | Confirmation rates by congress (total, confirmed, withdrawn, returned, rejected, pending) |
| `hearing_activity` | Hearing counts and unique witnesses by congress and chamber |

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
| `hearings_fts` | title, chamber, committees |
| `crs_reports_fts` | title, authors, topics, summary |
| `nominations_fts` | description, organization, citation, status |
| `gao_reports_fts` | title, abstract, subjects, report_number |

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
| `package_id` | `hearings` to `hearing_witnesses`, `hearing_members` | GovInfo package ID |
| `report_id` | `crs_reports` to `crs_report_bills` | CRS report ID |
| `nomination_id` | `nominations` to `nomination_actions` | Nomination ID |
| `treaty_id` | `treaties` to `treaty_actions` | Treaty ID |
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
| Bills | CRS reports | `crs_report_bills.bill_id` |
| Bills | Lobbying filings | `lobbying_bills.bill_id` |
| Hearing witnesses | Lobbying clients | `witness_lobby_overlap` (name matching) |
| Comment orgs | Lobbying clients | `commenter_lobby_overlap` (name matching) |
| Committee members | Trades in jurisd. sectors | `committee_trade_conflicts` (SIC matching) |
| Former members | Lobbying activity | `revolving_door` (name matching) |
| Senate votes | Bioguide | `lis_to_bioguide` crosswalk (326 senators) |

---

## Directory Structure

```
openregs/
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
├── hearings/                                      # GovInfo CHRG hearing metadata JSON
├── crs_reports/                                   # Congress.gov CRS report JSON
├── nominations/                                   # Congress.gov nomination JSON
├── treaties/                                      # Congress.gov treaty JSON
├── gao_reports/                                   # GovInfo GAOREPORTS metadata JSON
├── sec_tickers/                                   # SEC EDGAR ticker-SIC mapping
├── reference/                                     # Committee sector/jurisdiction CSVs
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

Scripts are numbered in execution order. Run from the project root directory.

### Data download scripts

| Script | Source | Auth | Time |
|--------|--------|------|------|
| `01_federal_register.py` | FR API | None | ~30 min |
| `02_regs_gov_dockets_docs.py` | Regulations.gov | API key | ~4-6 hr |
| `03_regs_gov_comments.py` | Regulations.gov | API key | ~30-50 hr (all agencies; consolidated from old 03+04+06 on 2026-03-27) |
| `07_full_comment_details.py` | Regulations.gov | API key | Weeks |
| `08_usaspending.py` | USAspending.gov | None | ~2 hr |
| `08_fec_campaign_finance.py` | FEC bulk data | None | ~1 hr |
| `09_congress_gov.py` | GovInfo + Congress.gov | API key | ~50 min |
| `09_fec_employer_aggregates.py` | Local (from fec.db) | None | ~5 min |
| `10_ecfr.py` | GovInfo bulk XML | None | ~30 sec |
| `10_congress_votes.py` | Congress.gov API | API key | ~2 hr |
| `11_congressional_record.py` | GovInfo CREC | None | ~6-8 hr |
| `11_fara.py` | FARA.gov | None | ~5 min |
| `12_congress_stock_trades.py` | Senate eFD + House PTR + FD indexes | None | ~3 hr (consolidated from old 12+13+14 on 2026-03-27) |
| `15_lobbying_disclosure.py` | Senate LDA API | API key | ~2 hr |
| `16_committee_hearings.py` | GovInfo CHRG API | API key | ~4 hr |
| `17_crs_reports.py` | Congress.gov API | API key | ~30 min |
| `18_nominations_treaties.py` | Congress.gov API | API key | ~1 hr |
| `19_gao_reports.py` | GovInfo GAOREPORTS API | API key | ~2 hr |
| `20_sec_ticker_sic.py` | SEC EDGAR | None | ~4 min |
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
8. **Lobbying contributions loaded** -- 3.5M LD-203 contribution records now populated.
9. **APHIS sub-tables** -- Annual reports, enforcement actions, and teachable moments have schemas but no data yet.
10. **FEC in openregs.db** -- Contains 4.4M contributions to candidates. Full individual contributions (104M rows) only in standalone fec.db.
