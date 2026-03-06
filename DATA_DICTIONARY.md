# OpenRegs Data Dictionary

> Federal regulatory data archive — bulk download from Federal Register and Regulations.gov APIs.
> Created: 2026-03-01

---

## Data Sources

| Source | API | Auth | Rate Limit |
|--------|-----|------|------------|
| Federal Register | `federalregister.gov/api/v1` | None | None (polite 0.5s delay) |
| Regulations.gov | `api.regulations.gov/v4` | API key (`X-Api-Key` header) | 1,000 requests/hour |

---

## Tables / Datasets

### 1. Federal Register Documents

| Attribute | Value |
|-----------|-------|
| **Path** | `./federal_register/raw/{YYYY}/{MM}/page_{NNNN}.json` |
| **Source** | `GET https://www.federalregister.gov/api/v1/documents.json` |
| **Coverage** | 1994–present, all agencies, all document types |
| **Created** | 2026-03-01 (Phase 1 ingestion) |
| **Format** | JSON — paginated API responses, up to 1,000 documents per page |
| **Supports** | Full-text regulatory analysis, rulemaking timeline tracking, agency activity trends, cross-reference with Regulations.gov via `docket_ids` and `document_number` |

**Document types included:**
- `Rule` — Final rules
- `Proposed Rule` — Notices of Proposed Rulemaking (NPRMs)
- `Notice` — Agency notices, meetings, petitions, announcements
- `Presidential Document` — Executive orders, proclamations, memoranda

**Key fields per document:**

| Field | Type | Description |
|-------|------|-------------|
| `document_number` | string | Unique Federal Register document ID |
| `type` | string | Rule, Proposed Rule, Notice, Presidential Document |
| `title` | string | Document title |
| `abstract` | string | Brief description/summary |
| `publication_date` | string | Date published (YYYY-MM-DD) |
| `agencies` | array | List of responsible agencies with names and IDs |
| `html_url` | string | Link to full text on federalregister.gov |
| `pdf_url` | string | Link to official PDF |
| `docket_ids` | array | Related Regulations.gov docket IDs |
| `regulation_id_numbers` | array | RIN identifiers (link to Unified Agenda) |
| `cfr_references` | array | Code of Federal Regulations citations affected |
| `effective_on` | string | Date rule takes effect |
| `comments_close_on` | string | Public comment deadline |
| `action` | string | Regulatory action description |
| `dates` | string | Key dates in free text |
| `page_length` | int | Number of Federal Register pages |
| `start_page` | int | Starting FR page number |
| `end_page` | int | Ending FR page number |

**Response envelope fields:**

| Field | Type | Description |
|-------|------|-------------|
| `count` | int | Total matching documents for the query |
| `total_pages` | int | Total pages available |
| `results` | array | Array of document objects |

---

### 2. Regulations.gov Dockets

| Attribute | Value |
|-----------|-------|
| **Path** | `./regulations_gov/dockets/{AGENCY}/page_{NNNN}.json` |
| **Source** | `GET https://api.regulations.gov/v4/dockets` |
| **Coverage** | Agencies: USDA, EPA, FDA, FWS, APHIS |
| **Created** | 2026-03-01 (Phase 2 ingestion) |
| **Format** | JSON API spec — `data[]` array with `meta{}` pagination info |
| **Supports** | Docket inventory, rulemaking tracking, linking documents and comments to regulatory proceedings |

**Key fields per docket (in `data[].attributes`):**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Docket ID (e.g., `EPA-HQ-OAR-2024-0001`) |
| `agencyId` | string | Agency acronym |
| `title` | string | Docket title |
| `docketType` | string | `Rulemaking` or `Nonrulemaking` |
| `lastModifiedDate` | string | Last modification timestamp |
| `objectId` | string | Internal object identifier |
| `highlightedContent` | string | Search-highlighted content (if searched) |

---

### 3. Regulations.gov Documents

| Attribute | Value |
|-----------|-------|
| **Path** | `./regulations_gov/documents/{AGENCY}/{YYYY}/page_{NNNN}.json` |
| **Subdivided** | If a year exceeds 5,000 results: `{AGENCY}/{YYYY}/{MM}/page_{NNNN}.json` |
| **Source** | `GET https://api.regulations.gov/v4/documents` |
| **Coverage** | Agencies: USDA, EPA, FDA, FWS, APHIS — all years available |
| **Created** | 2026-03-01 (Phase 2 ingestion) |
| **Format** | JSON API spec |
| **Supports** | Document inventory, cross-reference with Federal Register, rulemaking participation analysis |

**Key fields per document (in `data[].attributes`):**

| Field | Type | Description |
|-------|------|-------------|
| `documentId` | string | Document ID |
| `agencyId` | string | Agency acronym |
| `docketId` | string | Parent docket ID |
| `documentType` | string | Rule, Proposed Rule, Notice, Supporting & Related Material, Other |
| `title` | string | Document title |
| `postedDate` | string | Date posted to Regulations.gov |
| `lastModifiedDate` | string | Last modification date |
| `frDocNum` | string | Federal Register document number (cross-reference to FR data) |
| `objectId` | string | Internal object ID |
| `withdrawn` | boolean | Whether document was withdrawn |
| `openForComment` | boolean | Whether currently accepting comments |
| `commentEndDate` | string | Comment period deadline |
| `commentStartDate` | string | Comment period start |

---

### 4. Regulations.gov Comment Headers

| Attribute | Value |
|-----------|-------|
| **Path** | `./regulations_gov/comments/headers/{AGENCY}/{YYYY}/page_{NNNN}.json` |
| **Subdivided** | If a year exceeds 5,000 results: `{AGENCY}/{YYYY}/{MM}/page_{NNNN}.json` |
| **Source** | `GET https://api.regulations.gov/v4/comments` |
| **Coverage** | Agencies: USDA, EPA, FDA, FWS, APHIS — all years available |
| **Created** | 2026-03-01 (Phase 3 ingestion) |
| **Format** | JSON API spec — metadata only, NOT full comment text |
| **Supports** | Comment volume analysis, public participation metrics, identifying high-engagement rulemakings |

**Key fields per comment (in `data[].attributes`):**

| Field | Type | Description |
|-------|------|-------------|
| `commentId` | string | Comment ID |
| `agencyId` | string | Agency acronym |
| `docketId` | string | Parent docket ID |
| `documentId` | string | Parent document ID (the document being commented on) |
| `title` | string | Comment title/subject line |
| `postedDate` | string | Date posted |
| `lastModifiedDate` | string | Last modification date |
| `objectId` | string | Internal object ID |
| `withdrawn` | boolean | Whether comment was withdrawn |

**Note:** Full comment text and attachments require individual document detail API calls (`/v4/comments/{commentId}`) which are not included in this download. This dataset contains headers/metadata only.

---

## Directory Structure

```
./
├── federal_register/
│   └── raw/                                  # Raw FR API responses
│       └── {YYYY}/
│           └── {MM}/
│               └── page_{NNNN}.json          # Up to 1,000 docs per page
├── regulations_gov/
│   ├── dockets/
│   │   └── {AGENCY}/
│   │       └── page_{NNNN}.json              # Up to 250 dockets per page
│   ├── documents/
│   │   └── {AGENCY}/
│   │       └── {YYYY}/
│   │           └── page_{NNNN}.json          # Up to 250 docs per page
│   └── comments/
│       └── headers/
│           └── {AGENCY}/
│               └── {YYYY}/
│                   └── page_{NNNN}.json      # Up to 250 comments per page
├── scripts/
│   ├── config.json                           # API keys (DO NOT COMMIT)
│   ├── 01_federal_register.py                # Phase 1 ingestion
│   ├── 02_regs_gov_dockets_docs.py           # Phase 2 ingestion
│   ├── 03_regs_gov_comments.py               # Phase 3 ingestion
│   └── run_all.sh                            # Master runner
├── logs/
│   ├── progress.txt                          # Human-readable status updates
│   ├── federal_register.log                  # Phase 1 detailed log
│   ├── regs_gov_dockets_docs.log             # Phase 2 detailed log
│   ├── regs_gov_comments.log                 # Phase 3 detailed log
│   ├── fr_state.json                         # Phase 1 resume state
│   ├── regs_dockets_docs_state.json          # Phase 2 resume state
│   └── regs_comments_state.json              # Phase 3 resume state
└── DATA_DICTIONARY.md                        # This file
```

---

## Cross-References Between Datasets

| From | To | Join Key |
|------|----|----------|
| FR Documents → Regs.gov Dockets | `docket_ids` → docket `id` |
| FR Documents → Regs.gov Documents | `document_number` → `frDocNum` |
| Regs.gov Documents → Dockets | `docketId` → docket `id` |
| Regs.gov Comments → Documents | `documentId` → document `documentId` |
| Regs.gov Comments → Dockets | `docketId` → docket `id` |

---

## Rate Limits & Resilience

- **Federal Register:** No API key, no enforced rate limit. Scripts use 0.5s polite delay.
- **Regulations.gov:** 1,000 requests/hour hard limit. Scripts enforce 3.6s minimum interval and handle HTTP 429 with exponential backoff.
- **Resume:** All scripts save state after each completed unit (month or agency-year). Re-running skips completed work.
- **Retries:** HTTP requests retry up to 5 times (FR) or 3 times (Regs.gov) with exponential backoff on server errors.
- **Graceful shutdown:** SIGINT/SIGTERM saves state before exiting.

---

## Monitoring

Check ingestion status:
```bash
# Live progress
tail -f ./logs/progress.txt

# Phase 1 status
cat ./logs/fr_state.json | python3 -m json.tool

# Phase 2 status
cat ./logs/regs_dockets_docs_state.json | python3 -m json.tool

# Phase 3 status
cat ./logs/regs_comments_state.json | python3 -m json.tool

# Disk usage
du -sh ./federal_register/ ./regulations_gov/
```
