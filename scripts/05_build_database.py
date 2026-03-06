#!/usr/bin/env python3
"""
Phase 5: Build normalized SQLite database from all extracted data.

Parses raw JSON from:
- Federal Register (994K+ documents)
- Regulations.gov dockets (14K+)
- Regulations.gov documents (727K+)
- Regulations.gov comments (1.6M+ headers)

Enrichments:
- Parse submitter names from comment titles
- Extract docket_id from comment IDs
- Cross-link Federal Register ↔ Regulations.gov via fr_doc_num
- Agency normalization table mapping regs.gov codes to FR agencies
- Year/month columns for time-series queries
- Docket-level stats view (comment counts, date ranges)

Produces a single SQLite database with proper schema, indexes, and FTS5.
"""

import json
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("build_db")

# === Paths ===
BASE_DIR = Path(__file__).resolve().parent.parent
FR_DIR = BASE_DIR / "federal_register" / "raw"
REGS_DIR = BASE_DIR / "regulations_gov"
DOCKETS_DIR = REGS_DIR / "dockets"
DOCS_DIR = REGS_DIR / "documents"
COMMENTS_DIR = REGS_DIR / "comments" / "headers"
DETAILS_DIR = REGS_DIR / "comments" / "details"
DB_PATH = BASE_DIR / "openregs.db"
MEMBERS_DIR = BASE_DIR / "congress_members"
STOCK_DIR = BASE_DIR / "stock_trades"

# === Schema ===
SCHEMA = """
-- Federal Register documents
CREATE TABLE IF NOT EXISTS federal_register (
    document_number TEXT PRIMARY KEY,
    title TEXT,
    type TEXT,
    abstract TEXT,
    publication_date TEXT,
    pub_year INTEGER,
    pub_month INTEGER,
    html_url TEXT,
    pdf_url TEXT,
    agency_names TEXT,
    agency_ids TEXT,
    excerpts TEXT
);

-- Agencies (extracted from Federal Register records)
CREATE TABLE IF NOT EXISTS agencies (
    id INTEGER PRIMARY KEY,
    name TEXT,
    raw_name TEXT,
    slug TEXT,
    parent_id INTEGER
);

-- Federal Register <-> Agency link table
CREATE TABLE IF NOT EXISTS federal_register_agencies (
    document_number TEXT REFERENCES federal_register(document_number),
    agency_id INTEGER REFERENCES agencies(id),
    PRIMARY KEY (document_number, agency_id)
);

-- Agency code mapping: regulations.gov short codes <-> FR agency names
CREATE TABLE IF NOT EXISTS agency_map (
    regs_code TEXT PRIMARY KEY,
    fr_agency_id INTEGER,
    fr_agency_name TEXT,
    fr_raw_name TEXT
);

-- Regulations.gov dockets
CREATE TABLE IF NOT EXISTS dockets (
    id TEXT PRIMARY KEY,
    agency_id TEXT,
    title TEXT,
    docket_type TEXT,
    last_modified TEXT,
    object_id TEXT
);

-- Regulations.gov documents
CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    agency_id TEXT,
    docket_id TEXT REFERENCES dockets(id),
    title TEXT,
    document_type TEXT,
    subtype TEXT,
    posted_date TEXT,
    posted_year INTEGER,
    posted_month INTEGER,
    comment_start_date TEXT,
    comment_end_date TEXT,
    last_modified TEXT,
    fr_doc_num TEXT,
    open_for_comment INTEGER,
    withdrawn INTEGER,
    object_id TEXT
);

-- Regulations.gov comment headers
CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    agency_id TEXT,
    docket_id TEXT,
    title TEXT,
    submitter_name TEXT,
    submitter_type TEXT,
    document_type TEXT,
    posted_date TEXT,
    posted_year INTEGER,
    posted_month INTEGER,
    last_modified TEXT,
    withdrawn INTEGER,
    object_id TEXT
);

-- Regulations.gov comment full details (from detail API)
CREATE TABLE IF NOT EXISTS comment_details (
    id TEXT PRIMARY KEY REFERENCES comments(id),
    comment_text TEXT,
    organization TEXT,
    first_name TEXT,
    last_name TEXT,
    city TEXT,
    state_province TEXT,
    zip TEXT,
    country TEXT,
    subtype TEXT,
    category TEXT,
    tracking_number TEXT,
    duplicate_comments INTEGER,
    comment_on_document_id TEXT,
    receive_date TEXT,
    postmark_date TEXT,
    gov_agency TEXT,
    gov_agency_type TEXT,
    page_count INTEGER,
    attachment_count INTEGER,
    attachment_urls TEXT
);

CREATE INDEX IF NOT EXISTS idx_cd_org ON comment_details(organization) WHERE organization IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cd_state ON comment_details(state_province) WHERE state_province IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cd_subtype ON comment_details(subtype);
CREATE INDEX IF NOT EXISTS idx_cd_comment_on ON comment_details(comment_on_document_id);

-- Cross-reference: Federal Register <-> Regulations.gov documents
CREATE TABLE IF NOT EXISTS fr_regs_crossref (
    fr_document_number TEXT REFERENCES federal_register(document_number),
    regs_document_id TEXT REFERENCES documents(id),
    PRIMARY KEY (fr_document_number, regs_document_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fr_date ON federal_register(publication_date);
CREATE INDEX IF NOT EXISTS idx_fr_year ON federal_register(pub_year);
CREATE INDEX IF NOT EXISTS idx_fr_type ON federal_register(type);
CREATE INDEX IF NOT EXISTS idx_fr_agencies ON federal_register(agency_names);
CREATE INDEX IF NOT EXISTS idx_fra_agency ON federal_register_agencies(agency_id);

CREATE INDEX IF NOT EXISTS idx_dockets_agency ON dockets(agency_id);
CREATE INDEX IF NOT EXISTS idx_dockets_type ON dockets(docket_type);

CREATE INDEX IF NOT EXISTS idx_docs_agency ON documents(agency_id);
CREATE INDEX IF NOT EXISTS idx_docs_docket ON documents(docket_id);
CREATE INDEX IF NOT EXISTS idx_docs_date ON documents(posted_date);
CREATE INDEX IF NOT EXISTS idx_docs_year ON documents(posted_year);
CREATE INDEX IF NOT EXISTS idx_docs_type ON documents(document_type);
CREATE INDEX IF NOT EXISTS idx_docs_frnum ON documents(fr_doc_num);
CREATE INDEX IF NOT EXISTS idx_docs_comment_end ON documents(comment_end_date) WHERE comment_end_date IS NOT NULL AND withdrawn = 0;

CREATE INDEX IF NOT EXISTS idx_comments_agency ON comments(agency_id);
CREATE INDEX IF NOT EXISTS idx_comments_docket ON comments(docket_id);
CREATE INDEX IF NOT EXISTS idx_comments_date ON comments(posted_date);
CREATE INDEX IF NOT EXISTS idx_comments_year ON comments(posted_year);
CREATE INDEX IF NOT EXISTS idx_comments_submitter ON comments(submitter_name);
CREATE INDEX IF NOT EXISTS idx_comments_subtype ON comments(submitter_type);

CREATE INDEX IF NOT EXISTS idx_crossref_fr ON fr_regs_crossref(fr_document_number);
CREATE INDEX IF NOT EXISTS idx_crossref_regs ON fr_regs_crossref(regs_document_id);

-- Presidential documents (Executive Orders, Proclamations)
CREATE TABLE IF NOT EXISTS presidential_documents (
    document_number TEXT PRIMARY KEY,
    title TEXT,
    document_type TEXT,
    executive_order_number INTEGER,
    signing_date TEXT,
    publication_date TEXT,
    html_url TEXT,
    abstract TEXT
);

CREATE INDEX IF NOT EXISTS idx_presdoc_type ON presidential_documents(document_type);
CREATE INDEX IF NOT EXISTS idx_presdoc_eo ON presidential_documents(executive_order_number);
CREATE INDEX IF NOT EXISTS idx_presdoc_date ON presidential_documents(signing_date);

-- Federal spending awards (from USAspending.gov)
CREATE TABLE IF NOT EXISTS spending_awards (
    generated_internal_id TEXT PRIMARY KEY,
    award_id TEXT,
    agency TEXT,
    sub_agency TEXT,
    award_category TEXT,
    award_type TEXT,
    recipient_name TEXT,
    award_amount REAL,
    total_outlays REAL,
    description TEXT,
    start_date TEXT,
    end_date TEXT,
    fiscal_year INTEGER,
    state_code TEXT,
    cfda_number TEXT,
    naics_code TEXT,
    naics_description TEXT
);

CREATE INDEX IF NOT EXISTS idx_spending_agency ON spending_awards(agency);
CREATE INDEX IF NOT EXISTS idx_spending_category ON spending_awards(award_category);
CREATE INDEX IF NOT EXISTS idx_spending_fy ON spending_awards(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_spending_recipient ON spending_awards(recipient_name);
CREATE INDEX IF NOT EXISTS idx_spending_state ON spending_awards(state_code);

-- Lobbying disclosure filings (from Senate LDA API)
CREATE TABLE IF NOT EXISTS lobbying_filings (
    filing_uuid TEXT PRIMARY KEY,
    filing_type TEXT NOT NULL,
    registrant_id INTEGER,
    registrant_name TEXT,
    client_id INTEGER,
    client_name TEXT,
    filing_year INTEGER,
    filing_period TEXT,
    received_date TEXT,
    amount_reported REAL,
    is_amendment INTEGER DEFAULT 0,
    is_no_activity INTEGER DEFAULT 0,
    is_termination INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS lobbying_activities (
    id INTEGER PRIMARY KEY,
    filing_uuid TEXT NOT NULL,
    filing_type TEXT NOT NULL,
    registrant_name TEXT NOT NULL,
    client_name TEXT NOT NULL,
    filing_year INTEGER NOT NULL,
    filing_period TEXT NOT NULL,
    issue_code TEXT,
    specific_issues TEXT,
    government_entities TEXT,
    income_amount INTEGER,
    expense_amount INTEGER,
    is_no_activity INTEGER DEFAULT 0,
    is_termination INTEGER DEFAULT 0,
    received_date TEXT
);

CREATE TABLE IF NOT EXISTS lobbying_lobbyists (
    id INTEGER PRIMARY KEY,
    filing_uuid TEXT NOT NULL,
    lobbyist_name TEXT NOT NULL,
    covered_position TEXT,
    is_new INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS lobbying_contributions (
    id INTEGER PRIMARY KEY,
    filing_uuid TEXT NOT NULL,
    lobbyist_name TEXT,
    contributor_name TEXT,
    payee_name TEXT,
    recipient_name TEXT,
    contribution_type TEXT,
    amount INTEGER,
    contribution_date TEXT,
    filing_year INTEGER,
    filing_period TEXT,
    registrant_name TEXT,
    received_date TEXT
);

CREATE TABLE IF NOT EXISTS lobbying_issue_codes (
    code TEXT PRIMARY KEY,
    description TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lobby_filing_type ON lobbying_filings(filing_type);
CREATE INDEX IF NOT EXISTS idx_lobby_client ON lobbying_filings(client_name);
CREATE INDEX IF NOT EXISTS idx_lobby_year ON lobbying_filings(filing_year);
CREATE INDEX IF NOT EXISTS idx_lobby_act_client ON lobbying_activities(client_name);
CREATE INDEX IF NOT EXISTS idx_lobby_act_issue ON lobbying_activities(issue_code);
CREATE INDEX IF NOT EXISTS idx_lobby_act_year ON lobbying_activities(filing_year);
CREATE INDEX IF NOT EXISTS idx_lobby_act_uuid ON lobbying_activities(filing_uuid);
CREATE INDEX IF NOT EXISTS idx_lobby_act_registrant ON lobbying_activities(registrant_name);
CREATE INDEX IF NOT EXISTS idx_lobby_lob_uuid ON lobbying_lobbyists(filing_uuid);
CREATE INDEX IF NOT EXISTS idx_lobby_lob_name ON lobbying_lobbyists(lobbyist_name);
CREATE INDEX IF NOT EXISTS idx_lobby_lob_covered ON lobbying_lobbyists(covered_position) WHERE covered_position IS NOT NULL AND covered_position != '';
CREATE INDEX IF NOT EXISTS idx_lobby_contrib_year ON lobbying_contributions(filing_year);
CREATE INDEX IF NOT EXISTS idx_lobby_contrib_recipient ON lobbying_contributions(recipient_name);

-- Congressional legislation (from Congress.gov API)
CREATE TABLE IF NOT EXISTS legislation (
    bill_id TEXT PRIMARY KEY,
    congress INTEGER,
    bill_type TEXT,
    bill_number INTEGER,
    title TEXT,
    policy_area TEXT,
    introduced_date TEXT,
    latest_action_date TEXT,
    latest_action_text TEXT,
    origin_chamber TEXT,
    sponsor_name TEXT,
    sponsor_state TEXT,
    sponsor_party TEXT,
    sponsor_bioguide_id TEXT,
    cosponsor_count INTEGER DEFAULT 0,
    summary_text TEXT,
    update_date TEXT,
    url TEXT
);

CREATE INDEX IF NOT EXISTS idx_leg_congress ON legislation(congress);
CREATE INDEX IF NOT EXISTS idx_leg_type ON legislation(bill_type);
CREATE INDEX IF NOT EXISTS idx_leg_policy ON legislation(policy_area);
CREATE INDEX IF NOT EXISTS idx_leg_date ON legislation(introduced_date);
CREATE INDEX IF NOT EXISTS idx_leg_sponsor ON legislation(sponsor_name);
CREATE INDEX IF NOT EXISTS idx_leg_sponsor_bioguide ON legislation(sponsor_bioguide_id);

-- Legislation actions (legislative history)
CREATE TABLE IF NOT EXISTS legislation_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_id TEXT REFERENCES legislation(bill_id),
    action_date TEXT,
    action_text TEXT,
    action_type TEXT,
    chamber TEXT,
    action_code TEXT
);

CREATE INDEX IF NOT EXISTS idx_legact_bill ON legislation_actions(bill_id);
CREATE INDEX IF NOT EXISTS idx_legact_date ON legislation_actions(action_date);

-- Legislation subjects (CRS subject terms)
CREATE TABLE IF NOT EXISTS legislation_subjects (
    bill_id TEXT REFERENCES legislation(bill_id),
    subject TEXT,
    PRIMARY KEY (bill_id, subject)
);

CREATE INDEX IF NOT EXISTS idx_legsub_subject ON legislation_subjects(subject);

-- eCFR regulatory text (current Code of Federal Regulations)
CREATE TABLE IF NOT EXISTS cfr_sections (
    section_id TEXT PRIMARY KEY,
    title_number INTEGER,
    title_name TEXT,
    chapter TEXT,
    subchapter TEXT,
    part_number TEXT,
    part_name TEXT,
    subpart TEXT,
    subpart_name TEXT,
    section_number TEXT,
    section_heading TEXT,
    agency TEXT,
    authority TEXT,
    source_citation TEXT,
    amendment_citations TEXT,
    full_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_cfr_title ON cfr_sections(title_number);
CREATE INDEX IF NOT EXISTS idx_cfr_part ON cfr_sections(part_number);
CREATE INDEX IF NOT EXISTS idx_cfr_agency ON cfr_sections(agency);

-- Congressional Record (floor proceedings)
CREATE TABLE IF NOT EXISTS congressional_record (
    granule_id TEXT PRIMARY KEY,
    date TEXT,
    congress INTEGER,
    session INTEGER,
    volume TEXT,
    issue TEXT,
    title TEXT,
    chamber TEXT,
    granule_class TEXT,
    sub_granule_class TEXT,
    page_start TEXT,
    page_end TEXT,
    speakers TEXT,
    bills TEXT,
    citation TEXT,
    full_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_crec_date ON congressional_record(date);
CREATE INDEX IF NOT EXISTS idx_crec_chamber ON congressional_record(chamber);
CREATE INDEX IF NOT EXISTS idx_crec_congress ON congressional_record(congress);
CREATE INDEX IF NOT EXISTS idx_crec_class ON congressional_record(granule_class);

-- Congressional Record speakers (normalized from JSON, with bioguide linkage)
CREATE TABLE IF NOT EXISTS crec_speakers (
    granule_id TEXT NOT NULL REFERENCES congressional_record(granule_id),
    speaker_name TEXT NOT NULL,
    speaker_role TEXT,
    bioguide_id TEXT,
    party TEXT,
    state TEXT,
    PRIMARY KEY (granule_id, speaker_name)
);

CREATE INDEX IF NOT EXISTS idx_crec_speaker_name ON crec_speakers(speaker_name);
CREATE INDEX IF NOT EXISTS idx_crec_speaker_bioguide ON crec_speakers(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_crec_speaker_granule ON crec_speakers(granule_id);

-- Congressional Record bill references (normalized from JSON)
CREATE TABLE IF NOT EXISTS crec_bills (
    granule_id TEXT NOT NULL REFERENCES congressional_record(granule_id),
    congress INTEGER,
    bill_type TEXT,
    bill_number INTEGER,
    bill_id TEXT,
    PRIMARY KEY (granule_id, congress, bill_type, bill_number)
);

CREATE INDEX IF NOT EXISTS idx_crec_bill_id ON crec_bills(bill_id);
CREATE INDEX IF NOT EXISTS idx_crec_bill_type ON crec_bills(bill_type, bill_number);

-- Congress members (from congress-legislators GitHub)
CREATE TABLE IF NOT EXISTS congress_members (
    bioguide_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    nickname TEXT,
    party TEXT,
    state TEXT,
    chamber TEXT,
    district INTEGER,
    first_served TEXT,
    last_served TEXT,
    is_current INTEGER,
    opensecrets_id TEXT,
    fec_ids TEXT,
    govtrack_id INTEGER,
    thomas_id TEXT,
    votesmart_id INTEGER,
    lis_id TEXT,
    wikipedia_id TEXT,
    ballotpedia_id TEXT,
    birthday TEXT,
    gender TEXT,
    terms_count INTEGER,
    served_until TEXT
);

CREATE INDEX IF NOT EXISTS idx_cm_name ON congress_members(last_name, first_name);
CREATE INDEX IF NOT EXISTS idx_cm_party ON congress_members(party);
CREATE INDEX IF NOT EXISTS idx_cm_state ON congress_members(state);
CREATE INDEX IF NOT EXISTS idx_cm_chamber ON congress_members(chamber);
CREATE INDEX IF NOT EXISTS idx_cm_current ON congress_members(is_current);

-- Stock trading disclosures
CREATE TABLE IF NOT EXISTS stock_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_name TEXT,
    bioguide_id TEXT,
    chamber TEXT,
    transaction_date TEXT,
    disclosure_date TEXT,
    ticker TEXT,
    asset_description TEXT,
    asset_type TEXT,
    transaction_type TEXT,
    amount_range TEXT,
    owner TEXT,
    comment TEXT,
    source_url TEXT,
    filing_type TEXT,
    state_district TEXT,
    doc_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_st_bioguide ON stock_trades(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_st_ticker ON stock_trades(ticker);
CREATE INDEX IF NOT EXISTS idx_st_date ON stock_trades(transaction_date);
CREATE INDEX IF NOT EXISTS idx_st_member ON stock_trades(member_name);
CREATE INDEX IF NOT EXISTS idx_st_type ON stock_trades(transaction_type);
CREATE INDEX IF NOT EXISTS idx_st_chamber ON stock_trades(chamber);

-- Congressional committees
CREATE TABLE IF NOT EXISTS committees (
    committee_id TEXT PRIMARY KEY,
    name TEXT,
    chamber TEXT,
    url TEXT,
    parent_committee_id TEXT
);

-- Committee membership (current)
CREATE TABLE IF NOT EXISTS committee_memberships (
    bioguide_id TEXT NOT NULL,
    committee_id TEXT NOT NULL,
    member_name TEXT,
    party TEXT,
    title TEXT,
    rank INTEGER,
    PRIMARY KEY (bioguide_id, committee_id)
);

CREATE INDEX IF NOT EXISTS idx_cm_committee ON committee_memberships(committee_id);
CREATE INDEX IF NOT EXISTS idx_cm_bioguide ON committee_memberships(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_cm_title ON committee_memberships(title);

-- Legislation cosponsors
CREATE TABLE IF NOT EXISTS legislation_cosponsors (
    bill_id TEXT NOT NULL REFERENCES legislation(bill_id),
    bioguide_id TEXT NOT NULL,
    full_name TEXT,
    party TEXT,
    state TEXT,
    sponsorship_date TEXT,
    is_original_cosponsor INTEGER,
    PRIMARY KEY (bill_id, bioguide_id)
);

CREATE INDEX IF NOT EXISTS idx_lc_bioguide ON legislation_cosponsors(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_lc_bill ON legislation_cosponsors(bill_id);
CREATE INDEX IF NOT EXISTS idx_lc_original ON legislation_cosponsors(is_original_cosponsor);

-- Roll call votes (from Congress.gov / House Clerk / Senate.gov)
CREATE TABLE IF NOT EXISTS roll_call_votes (
    congress INTEGER NOT NULL,
    chamber TEXT NOT NULL,
    session INTEGER,
    roll_call_number INTEGER NOT NULL,
    date TEXT,
    question TEXT,
    vote_type TEXT,
    description TEXT,
    result TEXT,
    bill_id TEXT,
    legislation_type TEXT,
    legislation_number TEXT,
    yea_count INTEGER,
    nay_count INTEGER,
    present_count INTEGER,
    not_voting_count INTEGER,
    source_url TEXT,
    PRIMARY KEY (congress, chamber, session, roll_call_number)
);

CREATE INDEX IF NOT EXISTS idx_rcv_date ON roll_call_votes(date);
CREATE INDEX IF NOT EXISTS idx_rcv_bill ON roll_call_votes(bill_id);
CREATE INDEX IF NOT EXISTS idx_rcv_congress ON roll_call_votes(congress);
CREATE INDEX IF NOT EXISTS idx_rcv_result ON roll_call_votes(result);
CREATE INDEX IF NOT EXISTS idx_rcv_compound ON roll_call_votes(congress, chamber, session, roll_call_number);

-- Individual member votes on roll calls
CREATE TABLE IF NOT EXISTS member_votes (
    congress INTEGER NOT NULL,
    chamber TEXT NOT NULL,
    session INTEGER NOT NULL,
    roll_call_number INTEGER NOT NULL,
    bioguide_id TEXT,
    member_name TEXT,
    party TEXT,
    state TEXT,
    vote_cast TEXT,
    FOREIGN KEY (congress, chamber, session, roll_call_number)
        REFERENCES roll_call_votes(congress, chamber, session, roll_call_number)
);

CREATE INDEX IF NOT EXISTS idx_mv_bioguide ON member_votes(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_mv_vote ON member_votes(congress, chamber, session, roll_call_number);
CREATE INDEX IF NOT EXISTS idx_mv_party ON member_votes(party);

-- Senate LIS ID to bioguide crosswalk (for Senate vote records)
CREATE TABLE IF NOT EXISTS lis_to_bioguide (
    lis_id TEXT PRIMARY KEY,
    bioguide_id TEXT NOT NULL
);

-- FEC campaign finance: candidates
CREATE TABLE IF NOT EXISTS fec_candidates (
    cand_id TEXT NOT NULL,
    cand_name TEXT,
    cand_pty_affiliation TEXT,
    cand_election_yr INTEGER,
    cand_office_st TEXT,
    cand_office TEXT,
    cand_office_district TEXT,
    cand_ici TEXT,
    cand_status TEXT,
    cand_pcc TEXT,
    cycle INTEGER NOT NULL,
    PRIMARY KEY (cand_id, cycle)
);

CREATE INDEX IF NOT EXISTS idx_fec_cand_name ON fec_candidates(cand_name);
CREATE INDEX IF NOT EXISTS idx_fec_cand_office ON fec_candidates(cand_office);
CREATE INDEX IF NOT EXISTS idx_fec_cand_state ON fec_candidates(cand_office_st);

-- FEC campaign finance: committees (PACs, party committees, etc.)
CREATE TABLE IF NOT EXISTS fec_committees (
    cmte_id TEXT NOT NULL,
    cmte_nm TEXT,
    cmte_tp TEXT,
    cmte_dsgn TEXT,
    cmte_pty_affiliation TEXT,
    org_tp TEXT,
    connected_org_nm TEXT,
    cand_id TEXT,
    cycle INTEGER NOT NULL,
    PRIMARY KEY (cmte_id, cycle)
);

CREATE INDEX IF NOT EXISTS idx_fec_cmte_name ON fec_committees(cmte_nm);
CREATE INDEX IF NOT EXISTS idx_fec_cmte_org ON fec_committees(connected_org_nm) WHERE connected_org_nm IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fec_cmte_cand ON fec_committees(cand_id) WHERE cand_id IS NOT NULL;

-- FEC: committee contributions to candidates (PAC money to candidates)
CREATE TABLE IF NOT EXISTS fec_contributions (
    cmte_id TEXT,
    cmte_name TEXT,
    cand_id TEXT,
    cand_name TEXT,
    transaction_dt TEXT,
    transaction_amt REAL,
    entity_tp TEXT,
    state TEXT,
    employer TEXT,
    occupation TEXT,
    cycle INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fec_contrib_cand ON fec_contributions(cand_id);
CREATE INDEX IF NOT EXISTS idx_fec_contrib_cmte ON fec_contributions(cmte_id);
CREATE INDEX IF NOT EXISTS idx_fec_contrib_cycle ON fec_contributions(cycle);
CREATE INDEX IF NOT EXISTS idx_fec_contrib_amt ON fec_contributions(transaction_amt);

-- FEC: candidate to bioguide crosswalk
CREATE TABLE IF NOT EXISTS fec_candidate_crosswalk (
    fec_candidate_id TEXT NOT NULL,
    bioguide_id TEXT NOT NULL,
    full_name TEXT,
    party TEXT,
    state TEXT,
    chamber TEXT,
    PRIMARY KEY (fec_candidate_id, bioguide_id)
);

CREATE INDEX IF NOT EXISTS idx_fec_xwalk_bio ON fec_candidate_crosswalk(bioguide_id);

-- FEC: employer aggregate tables (from fec_employers.db, no PII)
CREATE TABLE IF NOT EXISTS fec_employer_totals (
    employer TEXT PRIMARY KEY,
    donation_count INTEGER,
    total_amount INTEGER,
    avg_amount INTEGER,
    unique_states INTEGER,
    min_cycle INTEGER,
    max_cycle INTEGER
);

CREATE TABLE IF NOT EXISTS fec_employer_to_candidate (
    employer TEXT,
    cand_id TEXT,
    bioguide_id TEXT,
    cand_name TEXT,
    party TEXT,
    office TEXT,
    state TEXT,
    cycle INTEGER,
    donation_count INTEGER,
    total_amount INTEGER
);

CREATE INDEX IF NOT EXISTS idx_emp_totals_amount ON fec_employer_totals(total_amount);
CREATE INDEX IF NOT EXISTS idx_emp_cand_employer ON fec_employer_to_candidate(employer);
CREATE INDEX IF NOT EXISTS idx_emp_cand_cand ON fec_employer_to_candidate(cand_id);
CREATE INDEX IF NOT EXISTS idx_emp_cand_bioguide ON fec_employer_to_candidate(bioguide_id);

CREATE TABLE IF NOT EXISTS fec_employer_to_party (
    employer TEXT,
    party TEXT,
    cycle INTEGER,
    donation_count INTEGER,
    total_amount INTEGER,
    candidate_count INTEGER
);

CREATE INDEX IF NOT EXISTS idx_emp_party_employer ON fec_employer_to_party(employer);

CREATE TABLE IF NOT EXISTS fec_top_occupations (
    employer TEXT,
    occupation TEXT,
    donation_count INTEGER,
    total_amount INTEGER
);

CREATE INDEX IF NOT EXISTS idx_emp_occ_employer ON fec_top_occupations(employer);

-- FARA (Foreign Agents Registration Act)
CREATE TABLE IF NOT EXISTS fara_registrants (
    registration_number TEXT PRIMARY KEY,
    registration_date TEXT,
    termination_date TEXT,
    name TEXT,
    business_name TEXT,
    address_1 TEXT,
    address_2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT
);
CREATE INDEX IF NOT EXISTS idx_fara_reg_name ON fara_registrants(name);

CREATE TABLE IF NOT EXISTS fara_foreign_principals (
    rowid INTEGER PRIMARY KEY,
    registration_number TEXT,
    registrant_date TEXT,
    registrant_name TEXT,
    foreign_principal TEXT,
    fp_registration_date TEXT,
    fp_termination_date TEXT,
    country TEXT,
    address_1 TEXT,
    address_2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT
);
CREATE INDEX IF NOT EXISTS idx_fara_fp_reg ON fara_foreign_principals(registration_number);
CREATE INDEX IF NOT EXISTS idx_fara_fp_country ON fara_foreign_principals(country);
CREATE INDEX IF NOT EXISTS idx_fara_fp_principal ON fara_foreign_principals(foreign_principal);

CREATE TABLE IF NOT EXISTS fara_short_forms (
    rowid INTEGER PRIMARY KEY,
    registration_number TEXT,
    registration_date TEXT,
    registrant_name TEXT,
    short_form_date TEXT,
    short_form_termination_date TEXT,
    last_name TEXT,
    first_name TEXT,
    address_1 TEXT,
    address_2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT
);
CREATE INDEX IF NOT EXISTS idx_fara_sf_reg ON fara_short_forms(registration_number);
CREATE INDEX IF NOT EXISTS idx_fara_sf_name ON fara_short_forms(last_name, first_name);

CREATE TABLE IF NOT EXISTS fara_registrant_docs (
    rowid INTEGER PRIMARY KEY,
    registration_number TEXT,
    registrant_name TEXT,
    date_stamped TEXT,
    document_type TEXT,
    short_form_name TEXT,
    foreign_principal_name TEXT,
    foreign_principal_country TEXT,
    url TEXT
);
CREATE INDEX IF NOT EXISTS idx_fara_rd_reg ON fara_registrant_docs(registration_number);
CREATE INDEX IF NOT EXISTS idx_fara_rd_type ON fara_registrant_docs(document_type);
"""

VIEWS = """
-- Docket-level statistics
CREATE VIEW IF NOT EXISTS docket_stats AS
SELECT
    d.id,
    d.agency_id,
    d.title,
    d.docket_type,
    COUNT(DISTINCT c.id) AS comment_count,
    COUNT(DISTINCT doc.id) AS document_count,
    MIN(c.posted_date) AS earliest_comment,
    MAX(c.posted_date) AS latest_comment,
    MIN(doc.posted_date) AS earliest_document,
    MAX(doc.posted_date) AS latest_document
FROM dockets d
LEFT JOIN comments c ON c.docket_id = d.id
LEFT JOIN documents doc ON doc.docket_id = d.id
GROUP BY d.id;

-- Comments per year per agency
CREATE VIEW IF NOT EXISTS comments_by_year AS
SELECT
    agency_id,
    posted_year,
    COUNT(*) AS comment_count,
    COUNT(DISTINCT docket_id) AS docket_count,
    SUM(CASE WHEN submitter_type = 'organization' THEN 1 ELSE 0 END) AS org_comments,
    SUM(CASE WHEN submitter_type = 'individual' THEN 1 ELSE 0 END) AS individual_comments,
    SUM(CASE WHEN submitter_type = 'anonymous' THEN 1 ELSE 0 END) AS anonymous_comments
FROM comments
WHERE posted_year IS NOT NULL
GROUP BY agency_id, posted_year
ORDER BY agency_id, posted_year;

-- Federal Register documents per year per type
CREATE VIEW IF NOT EXISTS fr_by_year AS
SELECT
    pub_year,
    type,
    COUNT(*) AS doc_count
FROM federal_register
WHERE pub_year IS NOT NULL
GROUP BY pub_year, type
ORDER BY pub_year, type;

-- Top commented dockets (materialized for performance)
CREATE VIEW IF NOT EXISTS top_dockets AS
SELECT
    c.docket_id,
    c.agency_id,
    d.title AS docket_title,
    d.docket_type,
    COUNT(*) AS comment_count,
    COUNT(DISTINCT c.submitter_name) AS unique_submitters,
    SUM(CASE WHEN c.submitter_type = 'organization' THEN 1 ELSE 0 END) AS org_comments,
    SUM(CASE WHEN c.submitter_type = 'individual' THEN 1 ELSE 0 END) AS individual_comments,
    SUM(CASE WHEN c.submitter_type = 'anonymous' THEN 1 ELSE 0 END) AS anonymous_comments,
    MIN(c.posted_date) AS first_comment,
    MAX(c.posted_date) AS last_comment
FROM comments c
LEFT JOIN dockets d ON d.id = c.docket_id
WHERE c.docket_id IS NOT NULL
GROUP BY c.docket_id
ORDER BY comment_count DESC;

-- Regulatory pipeline: documents that went from proposed rule -> final rule
CREATE VIEW IF NOT EXISTS regulatory_pipeline AS
SELECT
    d.id AS docket_id,
    d.agency_id,
    d.title AS docket_title,
    proposed.id AS proposed_rule_id,
    proposed.title AS proposed_rule_title,
    proposed.posted_date AS proposed_date,
    proposed.comment_start_date,
    proposed.comment_end_date,
    final.id AS final_rule_id,
    final.title AS final_rule_title,
    final.posted_date AS final_date,
    julianday(final.posted_date) - julianday(proposed.posted_date) AS days_proposed_to_final,
    (SELECT COUNT(*) FROM comments c WHERE c.docket_id = d.id) AS total_comments
FROM dockets d
JOIN documents proposed ON proposed.docket_id = d.id AND proposed.document_type = 'Proposed Rule'
JOIN documents final ON final.docket_id = d.id AND final.document_type = 'Rule'
WHERE d.docket_type = 'Rulemaking';

-- Comment volume by month (time series for charting)
CREATE VIEW IF NOT EXISTS comments_monthly AS
SELECT
    agency_id,
    posted_year,
    posted_month,
    posted_year || '-' || printf('%02d', posted_month) AS year_month,
    COUNT(*) AS comment_count,
    COUNT(DISTINCT docket_id) AS active_dockets
FROM comments
WHERE posted_year IS NOT NULL AND posted_month IS NOT NULL
GROUP BY agency_id, posted_year, posted_month
ORDER BY agency_id, posted_year, posted_month;

-- Agency overview: comprehensive agency stats
CREATE VIEW IF NOT EXISTS agency_overview AS
SELECT
    am.regs_code AS agency_code,
    am.fr_agency_name AS agency_name,
    (SELECT COUNT(*) FROM dockets WHERE agency_id = am.regs_code) AS docket_count,
    (SELECT COUNT(*) FROM documents WHERE agency_id = am.regs_code) AS document_count,
    (SELECT COUNT(*) FROM comments WHERE agency_id = am.regs_code) AS comment_count,
    (SELECT COUNT(*) FROM documents WHERE agency_id = am.regs_code AND document_type = 'Rule') AS final_rules,
    (SELECT COUNT(*) FROM documents WHERE agency_id = am.regs_code AND document_type = 'Proposed Rule') AS proposed_rules,
    (SELECT COUNT(*) FROM documents WHERE agency_id = am.regs_code AND document_type = 'Notice') AS notices,
    (SELECT MIN(posted_date) FROM comments WHERE agency_id = am.regs_code) AS earliest_comment,
    (SELECT MAX(posted_date) FROM comments WHERE agency_id = am.regs_code) AS latest_comment
FROM agency_map am;

-- Top submitters: most active commenters across all agencies
CREATE VIEW IF NOT EXISTS top_submitters AS
SELECT
    submitter_name,
    submitter_type,
    COUNT(*) AS comment_count,
    COUNT(DISTINCT agency_id) AS agencies_commented,
    COUNT(DISTINCT docket_id) AS dockets_commented,
    MIN(posted_date) AS first_comment,
    MAX(posted_date) AS last_comment,
    GROUP_CONCAT(DISTINCT agency_id) AS agency_list
FROM comments
WHERE submitter_name IS NOT NULL AND submitter_type = 'organization'
GROUP BY submitter_name
HAVING comment_count >= 5
ORDER BY comment_count DESC;

-- Speaker activity summary (with bioguide linkage)
CREATE VIEW IF NOT EXISTS speaker_activity AS
SELECT
    cs.speaker_name,
    cs.bioguide_id,
    cm.full_name AS official_name,
    cm.party,
    cm.state,
    COUNT(*) AS total_speeches,
    COUNT(DISTINCT cr.date) AS active_days,
    MIN(cr.date) AS first_appearance,
    MAX(cr.date) AS last_appearance,
    GROUP_CONCAT(DISTINCT cr.chamber) AS chambers,
    COUNT(DISTINCT cr.congress) AS congresses_active
FROM crec_speakers cs
JOIN congressional_record cr ON cs.granule_id = cr.granule_id
LEFT JOIN congress_members cm ON cs.bioguide_id = cm.bioguide_id
GROUP BY cs.speaker_name
ORDER BY total_speeches DESC;

-- Member activity: stock trades + legislative activity combined
CREATE VIEW IF NOT EXISTS member_overview AS
SELECT
    cm.bioguide_id,
    cm.full_name,
    cm.party,
    cm.state,
    cm.chamber,
    cm.is_current,
    (SELECT COUNT(*) FROM stock_trades st WHERE st.bioguide_id = cm.bioguide_id) AS trade_count,
    (SELECT COUNT(*) FROM crec_speakers cs WHERE cs.bioguide_id = cm.bioguide_id) AS speech_count,
    (SELECT COUNT(*) FROM legislation l WHERE l.sponsor_bioguide_id = cm.bioguide_id) AS bills_sponsored,
    (SELECT MIN(st.transaction_date) FROM stock_trades st WHERE st.bioguide_id = cm.bioguide_id) AS first_trade,
    (SELECT MAX(st.transaction_date) FROM stock_trades st WHERE st.bioguide_id = cm.bioguide_id) AS last_trade,
    (SELECT GROUP_CONCAT(DISTINCT st.ticker) FROM stock_trades st
     WHERE st.bioguide_id = cm.bioguide_id AND st.ticker != '' LIMIT 20) AS top_tickers
FROM congress_members cm
WHERE cm.is_current = 1
ORDER BY trade_count DESC;

-- Committee-trade cross-reference: did committee members trade related stocks?
CREATE VIEW IF NOT EXISTS committee_member_trades AS
SELECT
    cm2.committee_id,
    c.name AS committee_name,
    cm2.bioguide_id,
    cm2.member_name,
    cm2.title AS committee_role,
    st.ticker,
    st.asset_description,
    st.transaction_type,
    st.transaction_date,
    st.amount_range
FROM committee_memberships cm2
JOIN committees c ON cm2.committee_id = c.committee_id
JOIN stock_trades st ON cm2.bioguide_id = st.bioguide_id
WHERE st.ticker IS NOT NULL AND st.ticker != ''
ORDER BY cm2.committee_id, st.transaction_date DESC;

-- Stock trading by ticker: aggregate trading activity per ticker
CREATE VIEW IF NOT EXISTS stock_trades_by_ticker AS
SELECT
    st.ticker,
    st.asset_description,
    COUNT(*) AS trade_count,
    COUNT(DISTINCT st.bioguide_id) AS trader_count,
    GROUP_CONCAT(DISTINCT cm.full_name) AS traders,
    SUM(CASE WHEN st.transaction_type LIKE '%Purchase%' THEN 1 ELSE 0 END) AS purchases,
    SUM(CASE WHEN st.transaction_type LIKE '%Sale%' THEN 1 ELSE 0 END) AS sales,
    MIN(st.transaction_date) AS first_trade,
    MAX(st.transaction_date) AS last_trade
FROM stock_trades st
LEFT JOIN congress_members cm ON st.bioguide_id = cm.bioguide_id
WHERE st.ticker IS NOT NULL AND st.ticker != ''
GROUP BY st.ticker
ORDER BY trade_count DESC;

-- Bills with floor time
CREATE VIEW IF NOT EXISTS bills_floor_time AS
SELECT
    cb.bill_id,
    cb.congress,
    cb.bill_type,
    cb.bill_number,
    l.title AS bill_title,
    l.policy_area,
    l.sponsor_name,
    COUNT(*) AS floor_mentions,
    COUNT(DISTINCT cr.date) AS days_discussed,
    MIN(cr.date) AS first_discussed,
    MAX(cr.date) AS last_discussed
FROM crec_bills cb
JOIN congressional_record cr ON cb.granule_id = cr.granule_id
LEFT JOIN legislation l ON cb.bill_id = l.bill_id
GROUP BY cb.bill_id
HAVING floor_mentions >= 3
ORDER BY floor_mentions DESC;

-- Lobbying spending summary by year
CREATE VIEW IF NOT EXISTS lobbying_by_year AS
SELECT
    filing_year,
    COUNT(DISTINCT filing_uuid) AS filing_count,
    COUNT(DISTINCT client_name) AS unique_clients,
    COUNT(DISTINCT registrant_name) AS unique_registrants,
    SUM(income_amount) AS total_income_reported,
    SUM(expense_amount) AS total_expense_reported
FROM lobbying_activities
WHERE filing_year IS NOT NULL
GROUP BY filing_year
ORDER BY filing_year;

-- Top lobbying clients
CREATE VIEW IF NOT EXISTS top_lobbying_clients AS
SELECT
    client_name,
    COUNT(DISTINCT filing_uuid) AS filing_count,
    COUNT(DISTINCT registrant_name) AS firms_hired,
    COUNT(DISTINCT issue_code) AS issue_areas,
    SUM(income_amount) AS total_reported_income,
    MIN(filing_year) AS first_year,
    MAX(filing_year) AS last_year,
    GROUP_CONCAT(DISTINCT issue_code) AS issue_codes
FROM lobbying_activities
WHERE income_amount > 0
GROUP BY client_name
ORDER BY total_reported_income DESC;

-- Spending overview by agency (for the expanded 20-agency data)
CREATE VIEW IF NOT EXISTS spending_by_agency AS
SELECT
    agency,
    sub_agency,
    award_category,
    COUNT(*) AS award_count,
    SUM(award_amount) AS total_amount,
    AVG(award_amount) AS avg_amount,
    MIN(start_date) AS earliest,
    MAX(start_date) AS latest
FROM spending_awards
GROUP BY agency, sub_agency, award_category
ORDER BY total_amount DESC;
"""

FTS_SCHEMA = """
-- Full-text search on Federal Register
CREATE VIRTUAL TABLE IF NOT EXISTS federal_register_fts USING fts5(
    title, abstract, agency_names, excerpts,
    content='federal_register', content_rowid='rowid'
);

-- Full-text search on dockets (standalone — populated with summary from docket_summary)
CREATE VIRTUAL TABLE IF NOT EXISTS dockets_fts USING fts5(
    title, agency_id, summary
);

-- Full-text search on documents
CREATE VIRTUAL TABLE IF NOT EXISTS documents_fts USING fts5(
    title, agency_id, document_type,
    content='documents', content_rowid='rowid'
);

-- Full-text search on comments (includes submitter name + type + docket)
CREATE VIRTUAL TABLE IF NOT EXISTS comments_fts USING fts5(
    title, submitter_name, agency_id, submitter_type, docket_id,
    content='comments', content_rowid='rowid'
);

-- Full-text search on eCFR regulatory text
CREATE VIRTUAL TABLE IF NOT EXISTS cfr_fts USING fts5(
    section_number, section_heading, part_name, agency, full_text,
    content='cfr_sections', content_rowid='rowid'
);

-- Full-text search on Congressional Record
CREATE VIRTUAL TABLE IF NOT EXISTS crec_fts USING fts5(
    title, chamber, full_text,
    content='congressional_record', content_rowid='rowid'
);

-- Full-text search on lobbying activities (specific issues + client/registrant names)
CREATE VIRTUAL TABLE IF NOT EXISTS lobbying_fts USING fts5(
    client_name, registrant_name, specific_issues, government_entities, issue_code,
    content='lobbying_activities', content_rowid='rowid'
);

-- Full-text search on spending awards
CREATE VIRTUAL TABLE IF NOT EXISTS spending_awards_fts USING fts5(
    recipient_name, agency, sub_agency,
    content='spending_awards', content_rowid='rowid'
);

-- Full-text search on legislation
CREATE VIRTUAL TABLE IF NOT EXISTS legislation_fts USING fts5(
    title, policy_area, bill_id,
    content='legislation', content_rowid='rowid'
);

CREATE VIRTUAL TABLE IF NOT EXISTS fara_registrants_fts USING fts5(
    name, business_name, city, state,
    content='fara_registrants', content_rowid='rowid'
);

CREATE VIRTUAL TABLE IF NOT EXISTS fara_foreign_principals_fts USING fts5(
    registrant_name, foreign_principal, country,
    content='fara_foreign_principals', content_rowid='rowid'
);

CREATE VIRTUAL TABLE IF NOT EXISTS fec_employer_fts USING fts5(
    employer,
    content='fec_employer_totals', content_rowid='rowid'
);
"""


# === Comment title parsing ===

# Patterns for extracting submitter name from comment title
NAME_PATTERNS = [
    re.compile(r'^Comment (?:from|by|submitted by|of) (.+?)(?:\s*\[.*\])?$', re.IGNORECASE),
    re.compile(r'^(.+?) - Comment$', re.IGNORECASE),
    re.compile(r'^Comment (?:from|by) (.+?),\s*(?:NA|N/A)$', re.IGNORECASE),
    re.compile(r'^Public Comment (?:from|by|submitted by) (.+?)$', re.IGNORECASE),
]

ANONYMOUS_PATTERNS = [
    re.compile(r'anonymous', re.IGNORECASE),
    re.compile(r'^Form Letter', re.IGNORECASE),
    re.compile(r'^Mass Mail', re.IGNORECASE),
]

# Heuristic: if the extracted name looks like an org
ORG_INDICATORS = [
    'inc', 'inc.', 'corp', 'corp.', 'llc', 'llp', 'ltd', 'co.',
    'company', 'corporation', 'association', 'institute', 'foundation',
    'council', 'society', 'commission', 'department', 'university',
    'college', 'board', 'committee', 'coalition', 'alliance',
    'group', 'partners', 'international', 'national', 'federal',
    'chamber', 'union', 'district', 'authority', 'center', 'centre',
]


def parse_comment_title(title: str) -> tuple[str | None, str]:
    """
    Extract submitter name and type from comment title.
    Returns (submitter_name, submitter_type).
    submitter_type is 'individual', 'organization', 'anonymous', or 'unknown'.
    """
    if not title:
        return None, "unknown"

    # Check anonymous
    for pat in ANONYMOUS_PATTERNS:
        if pat.search(title):
            return None, "anonymous"

    # Try name extraction patterns
    for pat in NAME_PATTERNS:
        m = pat.match(title)
        if m:
            name = m.group(1).strip().rstrip(',').strip()
            if not name or len(name) < 2:
                continue

            # Classify as org or individual
            name_lower = name.lower()
            if any(ind in name_lower.split() for ind in ORG_INDICATORS):
                return name, "organization"
            # Names with commas like "Smith, John" are individuals
            # Names with lots of words and caps are likely orgs
            if ',' not in name and len(name.split()) > 4:
                return name, "organization"
            return name, "individual"

    return None, "unknown"


def extract_docket_id(comment_id: str) -> str | None:
    """
    Extract docket ID from a comment ID.
    e.g. 'EPA-HQ-OA-2002-0001-0039' → 'EPA-HQ-OA-2002-0001'
    Strip the last numeric segment.
    """
    if not comment_id:
        return None
    # Find the last '-NNNN' segment and remove it
    m = re.match(r'^(.+)-\d+$', comment_id)
    return m.group(1) if m else None


def extract_year_month(date_str: str | None) -> tuple[int | None, int | None]:
    """Extract year and month from an ISO date string."""
    if not date_str:
        return None, None
    m = re.match(r'(\d{4})-(\d{2})', date_str)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def iter_json_pages(directory: Path):
    """Iterate over all page_XXXX.json files in a directory tree, yielding parsed data."""
    if not directory.exists():
        return

    for json_file in sorted(directory.rglob("page_*.json")):
        try:
            with open(json_file) as f:
                data = json.load(f)
            yield json_file, data
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"  Skipping {json_file}: {e}")


def import_federal_register(conn: sqlite3.Connection):
    """Import Federal Register documents."""
    log.info("Importing Federal Register documents...")
    count = 0
    agencies_seen = {}

    for json_file, data in iter_json_pages(FR_DIR):
        results = data.get("results", [])
        for doc in results:
            doc_num = doc.get("document_number")
            if not doc_num:
                continue

            pub_date = doc.get("publication_date")
            pub_year, pub_month = extract_year_month(pub_date)

            doc_agencies = doc.get("agencies") or []
            agency_names = "; ".join(a.get("name", "") for a in doc_agencies if a.get("name"))
            agency_ids = ",".join(str(a.get("id", "")) for a in doc_agencies if a.get("id"))

            for a in doc_agencies:
                aid = a.get("id")
                if aid and aid not in agencies_seen:
                    agencies_seen[aid] = a

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO federal_register
                    (document_number, title, type, abstract, publication_date,
                     pub_year, pub_month, html_url, pdf_url, agency_names, agency_ids, excerpts)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    doc_num,
                    doc.get("title"),
                    doc.get("type"),
                    doc.get("abstract"),
                    pub_date,
                    pub_year,
                    pub_month,
                    doc.get("html_url"),
                    doc.get("pdf_url"),
                    agency_names,
                    agency_ids,
                    doc.get("excerpts"),
                ))
                count += 1

                for a in doc_agencies:
                    aid = a.get("id")
                    if aid:
                        conn.execute(
                            "INSERT OR IGNORE INTO federal_register_agencies VALUES (?, ?)",
                            (doc_num, aid),
                        )
            except sqlite3.Error as e:
                log.debug(f"  FR insert error for {doc_num}: {e}")

        if count % 50000 == 0 and count > 0:
            conn.commit()
            log.info(f"  {count:,} Federal Register documents...")

    # Insert agencies
    for aid, a in agencies_seen.items():
        conn.execute("""
            INSERT OR IGNORE INTO agencies (id, name, raw_name, slug, parent_id)
            VALUES (?, ?, ?, ?, ?)
        """, (aid, a.get("name"), a.get("raw_name"), a.get("slug"), a.get("parent_id")))

    conn.commit()
    log.info(f"  Done: {count:,} Federal Register documents, {len(agencies_seen)} agencies")
    return count


def import_dockets(conn: sqlite3.Connection):
    """Import regulations.gov dockets."""
    log.info("Importing dockets...")
    count = 0

    for json_file, data in iter_json_pages(DOCKETS_DIR):
        records = data.get("data", [])
        for rec in records:
            attrs = rec.get("attributes", {})
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO dockets
                    (id, agency_id, title, docket_type, last_modified, object_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    rec.get("id"),
                    attrs.get("agencyId"),
                    attrs.get("title"),
                    attrs.get("docketType"),
                    attrs.get("lastModifiedDate"),
                    attrs.get("objectId"),
                ))
                count += 1
            except sqlite3.Error as e:
                log.debug(f"  Docket insert error: {e}")

    conn.commit()
    log.info(f"  Done: {count:,} dockets")
    return count


def backfill_orphan_dockets(conn: sqlite3.Connection):
    """Create stub docket records for docket IDs referenced in documents/comments but missing from dockets table."""
    log.info("Backfilling orphan dockets...")
    count = conn.execute("""
        INSERT OR IGNORE INTO dockets (id, agency_id, title, docket_type)
        SELECT DISTINCT docket_id, agency_id, NULL, NULL
        FROM (
            SELECT docket_id, agency_id FROM documents
            WHERE docket_id IS NOT NULL AND docket_id != ''
            AND docket_id NOT IN (SELECT id FROM dockets)
            UNION
            SELECT docket_id, agency_id FROM comments
            WHERE docket_id IS NOT NULL AND docket_id != ''
            AND docket_id NOT IN (SELECT id FROM dockets)
        )
    """).rowcount
    conn.commit()
    log.info(f"  Done: {count:,} orphan dockets backfilled")
    return count


def import_documents(conn: sqlite3.Connection):
    """Import regulations.gov documents."""
    log.info("Importing documents...")
    count = 0

    for json_file, data in iter_json_pages(DOCS_DIR):
        records = data.get("data", [])
        for rec in records:
            attrs = rec.get("attributes", {})
            posted_date = attrs.get("postedDate")
            posted_year, posted_month = extract_year_month(posted_date)
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO documents
                    (id, agency_id, docket_id, title, document_type, subtype,
                     posted_date, posted_year, posted_month,
                     comment_start_date, comment_end_date,
                     last_modified, fr_doc_num, open_for_comment, withdrawn, object_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rec.get("id"),
                    attrs.get("agencyId"),
                    attrs.get("docketId"),
                    attrs.get("title"),
                    attrs.get("documentType"),
                    attrs.get("subtype"),
                    posted_date,
                    posted_year,
                    posted_month,
                    attrs.get("commentStartDate"),
                    attrs.get("commentEndDate"),
                    attrs.get("lastModifiedDate"),
                    attrs.get("frDocNum"),
                    1 if attrs.get("openForComment") else 0,
                    1 if attrs.get("withdrawn") else 0,
                    attrs.get("objectId"),
                ))
                count += 1
            except sqlite3.Error as e:
                log.debug(f"  Document insert error: {e}")

        if count % 50000 == 0 and count > 0:
            conn.commit()
            log.info(f"  {count:,} documents...")

    conn.commit()
    log.info(f"  Done: {count:,} documents")
    return count


def import_comments(conn: sqlite3.Connection):
    """Import regulations.gov comment headers with enrichments."""
    log.info("Importing comments (with name parsing + docket extraction)...")
    count = 0
    name_parsed = 0
    docket_linked = 0

    for json_file, data in iter_json_pages(COMMENTS_DIR):
        records = data.get("data", [])
        for rec in records:
            attrs = rec.get("attributes", {})
            comment_id = rec.get("id")
            title = attrs.get("title", "")

            # Enrichment: parse submitter name
            submitter_name, submitter_type = parse_comment_title(title)
            if submitter_name:
                name_parsed += 1

            # Enrichment: extract docket_id from comment ID
            docket_id = extract_docket_id(comment_id)
            if docket_id:
                docket_linked += 1

            posted_date = attrs.get("postedDate")
            posted_year, posted_month = extract_year_month(posted_date)

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO comments
                    (id, agency_id, docket_id, title, submitter_name, submitter_type,
                     document_type, posted_date, posted_year, posted_month,
                     last_modified, withdrawn, object_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    comment_id,
                    attrs.get("agencyId"),
                    docket_id,
                    title,
                    submitter_name,
                    submitter_type,
                    attrs.get("documentType"),
                    posted_date,
                    posted_year,
                    posted_month,
                    attrs.get("lastModifiedDate"),
                    1 if attrs.get("withdrawn") else 0,
                    attrs.get("objectId"),
                ))
                count += 1
            except sqlite3.Error as e:
                log.debug(f"  Comment insert error: {e}")

        if count % 100000 == 0 and count > 0:
            conn.commit()
            log.info(f"  {count:,} comments...")

    conn.commit()
    log.info(f"  Done: {count:,} comments ({name_parsed:,} names parsed, {docket_linked:,} docket-linked)")
    return count


def import_comment_details(conn: sqlite3.Connection):
    """Import full comment details from downloaded batch files."""
    if not DETAILS_DIR.exists():
        log.info("No comment details directory found — skipping")
        return 0

    batch_files = sorted(DETAILS_DIR.glob("batch_*.json"))
    if not batch_files:
        log.info("No comment detail batch files found — skipping")
        return 0

    log.info(f"Importing comment details from {len(batch_files)} batch files...")
    count = 0

    for bf in batch_files:
        try:
            with open(bf) as f:
                batch = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.debug(f"  Skipping {bf.name}: {e}")
            continue

        for rec in batch:
            data = rec.get("data", rec)
            comment_id = data.get("id")
            if not comment_id:
                continue

            attrs = data.get("attributes", {})

            # Extract attachment info
            included = rec.get("included", [])
            attachment_count = len(included)
            attachment_urls = []
            for att in included:
                att_attrs = att.get("attributes", {})
                for fmt in (att_attrs.get("fileFormats") or []):
                    url = fmt.get("fileUrl")
                    if url:
                        attachment_urls.append(url)

            comment_text = attrs.get("comment")
            if comment_text and comment_text.strip().lower() in ("see attached file(s)", "see attached"):
                comment_text = None  # Not useful text

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO comment_details
                    (id, comment_text, organization, first_name, last_name,
                     city, state_province, zip, country, subtype, category,
                     tracking_number, duplicate_comments, comment_on_document_id,
                     receive_date, postmark_date, gov_agency, gov_agency_type,
                     page_count, attachment_count, attachment_urls)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    comment_id,
                    comment_text,
                    attrs.get("organization") or None,
                    attrs.get("firstName") or None,
                    attrs.get("lastName") or None,
                    attrs.get("city") or None,
                    attrs.get("stateProvinceRegion") or None,
                    attrs.get("zip") or None,
                    attrs.get("country") or None,
                    attrs.get("subtype") or None,
                    attrs.get("category") or None,
                    attrs.get("trackingNbr") or None,
                    attrs.get("duplicateComments"),
                    attrs.get("commentOnDocumentId") or None,
                    attrs.get("receiveDate") or None,
                    attrs.get("postmarkDate") or None,
                    attrs.get("govAgency") or None,
                    attrs.get("govAgencyType") or None,
                    attrs.get("pageCount"),
                    attachment_count,
                    json.dumps(attachment_urls) if attachment_urls else None,
                ))
                count += 1
            except sqlite3.Error as e:
                log.debug(f"  Detail insert error: {e}")

        if count % 10000 == 0 and count > 0:
            conn.commit()
            log.info(f"  {count:,} comment details...")

    conn.commit()
    log.info(f"  Done: {count:,} comment details imported")
    return count


def import_presidential_documents(conn: sqlite3.Connection):
    """Import presidential documents from Federal Register API data."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    log.info("Importing presidential documents from Federal Register API...")

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=5, status_forcelist=[500, 502, 503])
    session.mount("https://", HTTPAdapter(max_retries=retry))

    count = 0
    for doc_type in ["executive_order", "proclamation"]:
        page = 1
        while True:
            url = "https://www.federalregister.gov/api/v1/documents.json"
            params = {
                "conditions[presidential_document_type]": doc_type,
                "conditions[type]": "PRESDOCU",
                "fields[]": [
                    "document_number", "title", "signing_date",
                    "publication_date", "html_url", "abstract",
                    "executive_order_number",
                ],
                "per_page": 1000,
                "page": page,
            }
            try:
                resp = session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"  Error fetching {doc_type} page {page}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for rec in results:
                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO presidential_documents
                        (document_number, title, document_type, executive_order_number,
                         signing_date, publication_date, html_url, abstract)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        rec.get("document_number"),
                        rec.get("title"),
                        doc_type,
                        rec.get("executive_order_number"),
                        rec.get("signing_date"),
                        rec.get("publication_date"),
                        rec.get("html_url"),
                        rec.get("abstract"),
                    ))
                    count += 1
                except sqlite3.Error:
                    pass

            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

        conn.commit()
        type_count = conn.execute(
            "SELECT COUNT(*) FROM presidential_documents WHERE document_type = ?",
            (doc_type,)
        ).fetchone()[0]
        log.info(f"  {doc_type}: {type_count:,}")

    log.info(f"  Done: {count:,} presidential documents")
    return count


SPENDING_DIR = BASE_DIR / "usaspending" / "awards"


def import_spending(conn: sqlite3.Connection):
    """Import USAspending.gov awards from downloaded JSON files."""
    if not SPENDING_DIR.exists():
        log.info("Skipping spending data (no usaspending/awards/ directory)")
        return 0

    log.info("Importing USAspending.gov awards...")
    count = 0

    for json_file in sorted(SPENDING_DIR.glob("*.json")):
        try:
            with open(json_file) as f:
                records = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"  Skipping {json_file.name}: {e}")
            continue

        for rec in records:
            internal_id = rec.get("generated_internal_id")
            if not internal_id:
                continue

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO spending_awards
                    (generated_internal_id, award_id, agency, sub_agency,
                     award_category, award_type, recipient_name,
                     award_amount, total_outlays, description,
                     start_date, end_date, fiscal_year,
                     state_code, cfda_number, naics_code, naics_description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    internal_id,
                    rec.get("Award ID"),
                    rec.get("Awarding Agency") or rec.get("_agency"),
                    rec.get("Awarding Sub Agency"),
                    rec.get("_award_category"),
                    rec.get("Award Type") or rec.get("Contract Award Type"),
                    rec.get("Recipient Name"),
                    rec.get("Award Amount"),
                    rec.get("Total Outlays"),
                    rec.get("Description"),
                    rec.get("Start Date"),
                    rec.get("End Date"),
                    rec.get("_fiscal_year"),
                    rec.get("Place of Performance State Code"),
                    rec.get("CFDA Number"),
                    rec.get("NAICS Code"),
                    rec.get("NAICS Description"),
                ))
                count += 1
            except sqlite3.Error as e:
                log.debug(f"  Spending insert error: {e}")

        if count % 10000 == 0 and count > 0:
            conn.commit()

    conn.commit()
    log.info(f"  Done: {count:,} spending awards")
    return count


LOBBYING_DB = BASE_DIR / "lobbying.db"


def import_lobbying(conn: sqlite3.Connection):
    """Import lobbying disclosure data from separate lobbying.db."""
    if not LOBBYING_DB.exists():
        log.info("Skipping lobbying data (lobbying.db not found)")
        return 0

    log.info("Importing lobbying disclosure data...")
    conn.execute(f"ATTACH DATABASE '{LOBBYING_DB}' AS ldb")

    # Copy filings (skip raw_json column to save space)
    count = conn.execute("""
        INSERT OR IGNORE INTO lobbying_filings
        (filing_uuid, filing_type, registrant_id, registrant_name,
         client_id, client_name, filing_year, filing_period,
         received_date, amount_reported, is_amendment, is_no_activity, is_termination)
        SELECT filing_uuid, filing_type, registrant_id, registrant_name,
               client_id, client_name, filing_year, filing_period,
               received_date, amount_reported, is_amendment, is_no_activity, is_termination
        FROM ldb.lobbying_filings_raw
    """).rowcount
    log.info(f"  Filings: {count:,}")

    count = conn.execute("""
        INSERT OR IGNORE INTO lobbying_activities
        SELECT id, filing_uuid, filing_type, registrant_name, client_name,
               filing_year, filing_period, issue_code, specific_issues,
               government_entities, income_amount, expense_amount,
               is_no_activity, is_termination, received_date
        FROM ldb.lobbying_activities
    """).rowcount
    log.info(f"  Activities: {count:,}")

    count = conn.execute("""
        INSERT OR IGNORE INTO lobbying_lobbyists
        SELECT id, filing_uuid, lobbyist_name, covered_position, is_new
        FROM ldb.lobbying_lobbyists
    """).rowcount
    log.info(f"  Lobbyists: {count:,}")

    count = conn.execute("""
        INSERT OR IGNORE INTO lobbying_contributions
        SELECT id, filing_uuid, lobbyist_name, contributor_name, payee_name,
               recipient_name, contribution_type, amount, contribution_date,
               filing_year, filing_period, registrant_name, received_date
        FROM ldb.lobbying_contributions
    """).rowcount
    log.info(f"  Contributions: {count:,}")

    count = conn.execute("""
        INSERT OR IGNORE INTO lobbying_issue_codes
        SELECT code, description FROM ldb.lobbying_issue_codes
    """).rowcount
    log.info(f"  Issue codes: {count:,}")

    conn.commit()
    conn.execute("DETACH DATABASE ldb")
    log.info("  Lobbying import complete")


FARA_DB = BASE_DIR / "fara.db"
FEC_DB = BASE_DIR / "fec.db"
FEC_EMPLOYERS_DB = BASE_DIR / "fec_employers.db"
VOTES_DB = BASE_DIR / "votes.db"
CONGRESS_DIR = BASE_DIR / "congress_gov"
ECFR_DIR = BASE_DIR / "ecfr"
CREC_DIR = BASE_DIR / "congressional_record"


def import_fara(conn: sqlite3.Connection):
    """Import FARA (Foreign Agents Registration Act) data from separate fara.db."""
    if not FARA_DB.exists():
        log.info("Skipping FARA data (fara.db not found)")
        return 0

    log.info("Importing FARA data...")
    conn.execute(f"ATTACH DATABASE '{FARA_DB}' AS faradb")

    # fara_registrants has no rowid column in schema, copy directly
    conn.execute("INSERT OR IGNORE INTO fara_registrants SELECT * FROM faradb.fara_registrants")
    log.info(f"  fara_registrants: {conn.execute('SELECT COUNT(*) FROM fara_registrants').fetchone()[0]:,}")

    # These tables have explicit rowid in openregs schema but not in fara.db — list columns
    fp_cols = "registration_number, registrant_date, registrant_name, foreign_principal, fp_registration_date, fp_termination_date, country, address_1, address_2, city, state, zip"
    conn.execute(f"INSERT INTO fara_foreign_principals ({fp_cols}) SELECT {fp_cols} FROM faradb.fara_foreign_principals")
    log.info(f"  fara_foreign_principals: {conn.execute('SELECT COUNT(*) FROM fara_foreign_principals').fetchone()[0]:,}")

    sf_cols = "registration_number, registration_date, registrant_name, short_form_date, short_form_termination_date, last_name, first_name, address_1, address_2, city, state, zip"
    conn.execute(f"INSERT INTO fara_short_forms ({sf_cols}) SELECT {sf_cols} FROM faradb.fara_short_forms")
    log.info(f"  fara_short_forms: {conn.execute('SELECT COUNT(*) FROM fara_short_forms').fetchone()[0]:,}")

    rd_cols = "registration_number, registrant_name, date_stamped, document_type, short_form_name, foreign_principal_name, foreign_principal_country, url"
    conn.execute(f"INSERT INTO fara_registrant_docs ({rd_cols}) SELECT {rd_cols} FROM faradb.fara_registrant_docs")
    log.info(f"  fara_registrant_docs: {conn.execute('SELECT COUNT(*) FROM fara_registrant_docs').fetchone()[0]:,}")

    conn.commit()
    conn.execute("DETACH DATABASE faradb")
    log.info("  FARA import complete")


def import_fec(conn: sqlite3.Connection):
    """Import FEC campaign finance data from separate fec.db."""
    if not FEC_DB.exists():
        log.info("Skipping FEC data (fec.db not found)")
        return 0

    log.info("Importing FEC campaign finance data...")
    conn.execute(f"ATTACH DATABASE '{FEC_DB}' AS fecdb")

    # Candidates (all cycles)
    count = conn.execute("""
        INSERT OR IGNORE INTO fec_candidates
        (cand_id, cand_name, cand_pty_affiliation, cand_election_yr,
         cand_office_st, cand_office, cand_office_district, cand_ici,
         cand_status, cand_pcc, cycle)
        SELECT cand_id, cand_name, cand_pty_affiliation, cand_election_yr,
               cand_office_st, cand_office, cand_office_district, cand_ici,
               cand_status, cand_pcc, cycle
        FROM fecdb.fec_candidates
    """).rowcount
    log.info(f"  Candidates: {count:,}")

    # Committees (all cycles)
    count = conn.execute("""
        INSERT OR IGNORE INTO fec_committees
        (cmte_id, cmte_nm, cmte_tp, cmte_dsgn, cmte_pty_affiliation,
         org_tp, connected_org_nm, cand_id, cycle)
        SELECT cmte_id, cmte_nm, cmte_tp, cmte_dsgn, cmte_pty_affiliation,
               org_tp, connected_org_nm, cand_id, cycle
        FROM fecdb.fec_committees
    """).rowcount
    log.info(f"  Committees: {count:,}")

    # Contributions to candidates — join with committee names for readability
    count = conn.execute("""
        INSERT INTO fec_contributions
        (cmte_id, cmte_name, cand_id, cand_name, transaction_dt,
         transaction_amt, entity_tp, state, employer, occupation, cycle)
        SELECT c.cmte_id,
               (SELECT cm.cmte_nm FROM fecdb.fec_committees cm
                WHERE cm.cmte_id = c.cmte_id AND cm.cycle = c.cycle LIMIT 1),
               c.cand_id,
               (SELECT cn.cand_name FROM fecdb.fec_candidates cn
                WHERE cn.cand_id = c.cand_id AND cn.cycle = c.cycle LIMIT 1),
               c.transaction_dt, c.transaction_amt, c.entity_tp,
               c.state, c.employer, c.occupation, c.cycle
        FROM fecdb.fec_contributions_to_candidates c
    """).rowcount
    log.info(f"  Contributions to candidates: {count:,}")

    # Candidate crosswalk
    count = conn.execute("""
        INSERT OR IGNORE INTO fec_candidate_crosswalk
        (fec_candidate_id, bioguide_id, full_name, party, state, chamber)
        SELECT fec_candidate_id, bioguide_id, full_name, party, state, chamber
        FROM fecdb.fec_candidate_crosswalk
    """).rowcount
    log.info(f"  Candidate crosswalk: {count:,}")

    conn.commit()
    conn.execute("DETACH DATABASE fecdb")
    log.info("  FEC import complete")
    return count


def import_fec_employers(conn: sqlite3.Connection):
    """Import FEC employer aggregates (no PII) from fec_employers.db."""
    if not FEC_EMPLOYERS_DB.exists():
        log.info("Skipping FEC employer data (fec_employers.db not found — run 09_fec_employer_aggregates.py)")
        return

    log.info("Importing FEC employer aggregates...")
    conn.execute(f"ATTACH DATABASE '{FEC_EMPLOYERS_DB}' AS empdb")

    for table in ['fec_employer_totals', 'fec_employer_to_candidate',
                   'fec_employer_to_party', 'fec_top_occupations']:
        conn.execute(f"INSERT INTO {table} SELECT * FROM empdb.{table}")
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"  {table}: {count:,}")

    conn.commit()
    conn.execute("DETACH DATABASE empdb")
    log.info("  FEC employer import complete")


def import_votes(conn: sqlite3.Connection):
    """Import roll call votes from separate votes.db."""
    if not VOTES_DB.exists():
        log.info("Skipping votes data (votes.db not found)")
        return 0

    log.info("Importing roll call votes...")
    conn.execute(f"ATTACH DATABASE '{VOTES_DB}' AS vdb")

    # Roll call votes
    count = conn.execute("""
        INSERT OR IGNORE INTO roll_call_votes
        (congress, chamber, session, roll_call_number, date, question,
         vote_type, description, result, bill_id, legislation_type,
         legislation_number, yea_count, nay_count, present_count,
         not_voting_count, source_url)
        SELECT congress, chamber, session, roll_call_number, date, question,
               vote_type, description, result, bill_id, legislation_type,
               legislation_number, yea_count, nay_count, present_count,
               not_voting_count, source_url
        FROM vdb.roll_call_votes
    """).rowcount
    log.info(f"  Roll call votes: {count:,}")

    # Member votes — resolve Senate LIS IDs to bioguide_ids
    # Load LIS crosswalk
    lis_crosswalk_file = MEMBERS_DIR / "lis_crosswalk.json"
    lis_to_bio = {}
    if lis_crosswalk_file.exists():
        with open(lis_crosswalk_file) as f:
            lis_to_bio = json.load(f)
        log.info(f"  LIS crosswalk loaded: {len(lis_to_bio)} senators")

    # First insert all member votes as-is
    count = conn.execute("""
        INSERT INTO member_votes
        (congress, chamber, session, roll_call_number,
         bioguide_id, member_name, party, state, vote_cast)
        SELECT congress, chamber, session, roll_call_number,
               bioguide_id, member_name, party, state, vote_cast
        FROM vdb.member_votes
    """).rowcount
    log.info(f"  Member votes: {count:,}")

    conn.commit()

    # Now fix Senate LIS IDs → bioguide IDs
    if lis_to_bio:
        fixed = 0
        for lis_id, bioguide_id in lis_to_bio.items():
            result = conn.execute("""
                UPDATE member_votes SET bioguide_id = ?
                WHERE bioguide_id = ? AND chamber = 'senate'
            """, (bioguide_id, lis_id))
            fixed += result.rowcount
        conn.commit()
        log.info(f"  Senate LIS IDs resolved: {fixed:,} vote records updated")

    # Also populate lis_to_bioguide table for reference
    for lis_id, bioguide_id in lis_to_bio.items():
        conn.execute("INSERT OR IGNORE INTO lis_to_bioguide VALUES (?, ?)",
                     (lis_id, bioguide_id))
    conn.commit()

    conn.execute("DETACH DATABASE vdb")

    # Normalize bill_id format: votes use "hr2638-110", legislation uses "110-hr-2638"
    log.info("  Normalizing vote bill_id format...")
    bills = conn.execute(
        "SELECT DISTINCT bill_id FROM roll_call_votes WHERE bill_id IS NOT NULL"
    ).fetchall()
    normalized = 0
    for (bid,) in bills:
        m = re.match(r'([a-z]+)(\d+)-(\d+)', bid)
        if m:
            new_bid = f'{m.group(3)}-{m.group(1)}-{m.group(2)}'
            conn.execute('UPDATE roll_call_votes SET bill_id = ? WHERE bill_id = ?',
                         (new_bid, bid))
            normalized += 1
    conn.commit()
    log.info(f"  Normalized {normalized:,} bill_ids")

    log.info("  Votes import complete")
    return count


def import_legislation(conn: sqlite3.Connection):
    """Import congressional legislation from BILLSTATUS-parsed JSON data."""
    if not CONGRESS_DIR.exists():
        log.info("Skipping legislation (no congress_gov/ directory)")
        return 0

    log.info("Importing congressional legislation...")
    bill_count = 0
    action_count = 0
    subject_count = 0

    for congress_dir in sorted(CONGRESS_DIR.glob("congress_*")):
        congress_num = congress_dir.name.split("_")[1]

        # Load individual bill detail files
        for detail_file in sorted(congress_dir.glob("*_*.json")):
            # Skip list files
            if detail_file.name.endswith("_list.json"):
                continue

            try:
                with open(detail_file) as f:
                    detail = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                log.debug(f"  Skipping {detail_file.name}: {e}")
                continue

            # BILLSTATUS format: fields at top level (no "bill" wrapper)
            # Also handles legacy API format where data is under "bill" key
            bill = detail.get("bill", detail) if "bill" in detail else detail
            if not bill.get("number"):
                continue

            congress = bill.get("congress", congress_num)
            bill_type = bill.get("type", "").lower()
            number = bill.get("number")
            bill_id = f"{congress}-{bill_type}-{number}"

            # Extract sponsor info
            sponsors = bill.get("sponsors", [])
            sponsor = sponsors[0] if sponsors else {}

            # Extract policy area (string in BILLSTATUS, dict in API format)
            policy_area = bill.get("policyArea") or detail.get("policyArea")
            if isinstance(policy_area, dict):
                policy_area = policy_area.get("name", "")

            # Latest action
            latest_action = bill.get("latestAction", {})

            # Summary (first available)
            summaries = bill.get("summaries", detail.get("summaries", []))
            summary_text = None
            if summaries:
                for s in reversed(summaries):
                    text = s.get("text", "")
                    if text:
                        summary_text = re.sub(r'<[^>]+>', '', text).strip()
                        break

            # Cosponsor count
            cosponsors = bill.get("cosponsors", detail.get("cosponsors", []))

            try:
                conn.execute("""
                    INSERT OR REPLACE INTO legislation
                    (bill_id, congress, bill_type, bill_number, title, policy_area,
                     introduced_date, latest_action_date, latest_action_text,
                     origin_chamber, sponsor_name, sponsor_state, sponsor_party,
                     sponsor_bioguide_id, cosponsor_count, summary_text, update_date, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bill_id,
                    congress,
                    bill_type,
                    number,
                    bill.get("title"),
                    policy_area,
                    bill.get("introducedDate"),
                    latest_action.get("actionDate") if latest_action else None,
                    latest_action.get("text") if latest_action else None,
                    bill.get("originChamber"),
                    sponsor.get("fullName") or f"{sponsor.get('firstName', '')} {sponsor.get('lastName', '')}".strip() if sponsor else None,
                    sponsor.get("state"),
                    sponsor.get("party"),
                    sponsor.get("bioguideId"),
                    len(cosponsors),
                    summary_text,
                    bill.get("updateDate"),
                    bill.get("url"),
                ))
                bill_count += 1
            except sqlite3.Error as e:
                log.debug(f"  Bill insert error for {bill_id}: {e}")
                continue

            # Import actions
            actions = bill.get("actions", detail.get("actions", []))
            for action in actions:
                try:
                    conn.execute("""
                        INSERT INTO legislation_actions
                        (bill_id, action_date, action_text, action_type, chamber, action_code)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        bill_id,
                        action.get("actionDate"),
                        action.get("text"),
                        action.get("type"),
                        action.get("committee", action.get("actionCode", "")),
                        action.get("source"),
                    ))
                    action_count += 1
                except sqlite3.Error:
                    pass

            # Import subjects (BILLSTATUS: list of strings; API: nested dict)
            subjects_data = bill.get("subjects", detail.get("subjects", []))
            if isinstance(subjects_data, dict):
                subject_list = subjects_data.get("legislativeSubjects", [])
            elif isinstance(subjects_data, list):
                subject_list = subjects_data
            else:
                subject_list = []

            for subj in subject_list:
                subj_name = subj.get("name") if isinstance(subj, dict) else str(subj)
                if subj_name:
                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO legislation_subjects VALUES (?, ?)",
                            (bill_id, subj_name),
                        )
                        subject_count += 1
                    except sqlite3.Error:
                        pass

        conn.commit()

    log.info(f"  Done: {bill_count:,} bills, {action_count:,} actions, {subject_count:,} subjects")
    return bill_count


def import_ecfr(conn: sqlite3.Connection):
    """Import eCFR regulatory text from parsed JSON files."""
    if not ECFR_DIR.exists():
        log.info("Skipping eCFR (no ecfr/ directory)")
        return 0

    log.info("Importing eCFR regulatory text...")
    section_count = 0

    for parsed_file in sorted(ECFR_DIR.glob("*_parsed.json")):
        try:
            with open(parsed_file) as f:
                sections = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.debug(f"  Skipping {parsed_file.name}: {e}")
            continue

        for section in sections:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO cfr_sections
                    (section_id, title_number, title_name, chapter, subchapter,
                     part_number, part_name, subpart, subpart_name,
                     section_number, section_heading, agency,
                     authority, source_citation, amendment_citations, full_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    section["section_id"],
                    section["title_number"],
                    section["title_name"],
                    section.get("chapter", ""),
                    section.get("subchapter", ""),
                    section.get("part_number", ""),
                    section.get("part_name", ""),
                    section.get("subpart", ""),
                    section.get("subpart_name", ""),
                    section.get("section_number", ""),
                    section.get("section_heading", ""),
                    section.get("agency", ""),
                    section.get("authority", ""),
                    section.get("source_citation", ""),
                    section.get("amendment_citations", ""),
                    section.get("full_text", ""),
                ))
                section_count += 1
            except sqlite3.Error as e:
                log.debug(f"  CFR insert error: {e}")

        conn.commit()
        log.info(f"  {parsed_file.name}: {len(sections):,} sections")

    log.info(f"  Done: {section_count:,} CFR sections")
    return section_count


def import_congressional_record(conn: sqlite3.Connection):
    """Import Congressional Record from parsed JSON files."""
    if not CREC_DIR.exists():
        log.info("Skipping Congressional Record (no congressional_record/ directory)")
        return 0

    log.info("Importing Congressional Record...")
    granule_count = 0

    for year_dir in sorted(CREC_DIR.iterdir()):
        if not year_dir.is_dir():
            continue

        year_count = 0
        for day_file in sorted(year_dir.glob("*.json")):
            try:
                with open(day_file) as f:
                    day_data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            # Package-level metadata (fallback for granules missing these)
            pkg_congress = day_data.get("congress")
            pkg_session = day_data.get("session")
            pkg_volume = day_data.get("volume")
            pkg_issue = day_data.get("issue")

            for granule in day_data.get("granules", []):
                try:
                    speakers = json.dumps(granule.get("speakers", [])) if granule.get("speakers") else None
                    bills = json.dumps(granule.get("bills", [])) if granule.get("bills") else None

                    conn.execute("""
                        INSERT OR REPLACE INTO congressional_record
                        (granule_id, date, congress, session, volume, issue,
                         title, chamber, granule_class, sub_granule_class,
                         page_start, page_end, speakers, bills, citation, full_text)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        granule["granule_id"],
                        granule.get("date"),
                        granule.get("congress") or pkg_congress,
                        granule.get("session") or pkg_session,
                        granule.get("volume") or pkg_volume,
                        granule.get("issue") or pkg_issue,
                        granule.get("title", ""),
                        granule.get("chamber", ""),
                        granule.get("granule_class", ""),
                        granule.get("sub_granule_class", ""),
                        granule.get("page_start", ""),
                        granule.get("page_end", ""),
                        speakers,
                        bills,
                        granule.get("citation", ""),
                        granule.get("text", ""),
                    ))
                    granule_count += 1
                    year_count += 1
                except sqlite3.Error:
                    pass

        if year_count:
            conn.commit()
            log.info(f"  {year_dir.name}: {year_count:,} granules")

    log.info(f"  Done: {granule_count:,} congressional record entries")
    return granule_count


def import_congress_members(conn: sqlite3.Connection):
    """Import congress member data from congress-legislators download."""
    members_file = MEMBERS_DIR / "members_lookup.json"
    if not members_file.exists():
        log.info("Skipping congress members (no congress_members/members_lookup.json)")
        return 0

    log.info("Importing congress members...")
    with open(members_file) as f:
        members = json.load(f)

    # Load LIS crosswalk if available (Senate LIS ID -> bioguide)
    lis_crosswalk_file = MEMBERS_DIR / "lis_crosswalk.json"
    lis_to_bio = {}
    if lis_crosswalk_file.exists():
        with open(lis_crosswalk_file) as f:
            lis_to_bio = json.load(f)
    # Reverse: bioguide -> lis_id
    bio_to_lis = {v: k for k, v in lis_to_bio.items()}
    if bio_to_lis:
        log.info(f"  LIS crosswalk: {len(bio_to_lis)} senators mapped")

    count = 0
    for m in members:
        try:
            bid = m["bioguide_id"]
            # Compute terms_count from first/last served years
            first_yr = m.get("first_served", "")[:4]
            last_yr = m.get("last_served", "")[:4]
            terms_count = None
            if first_yr and last_yr and first_yr.isdigit() and last_yr.isdigit():
                term_len = 6 if m.get("chamber") == "Senate" else 2
                terms_count = max(1, (int(last_yr) - int(first_yr)) // term_len + 1)
            # served_until: null for current, last_served for former
            served_until = None if m.get("is_current") else m.get("last_served", "")

            conn.execute("""
                INSERT OR REPLACE INTO congress_members
                (bioguide_id, first_name, last_name, full_name, nickname,
                 party, state, chamber, district, first_served, last_served,
                 is_current, opensecrets_id, fec_ids, govtrack_id, thomas_id,
                 votesmart_id, lis_id, wikipedia_id, ballotpedia_id, birthday, gender,
                 terms_count, served_until)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bid,
                m.get("first_name", ""),
                m.get("last_name", ""),
                m.get("full_name", ""),
                m.get("nickname", ""),
                m.get("party", ""),
                m.get("state", ""),
                m.get("chamber", ""),
                m.get("district"),
                m.get("first_served", ""),
                m.get("last_served", ""),
                1 if m.get("is_current") else 0,
                m.get("opensecrets_id", ""),
                m.get("fec_ids", ""),
                m.get("govtrack_id"),
                m.get("thomas_id", ""),
                m.get("votesmart_id"),
                bio_to_lis.get(bid),
                m.get("wikipedia_id", ""),
                m.get("ballotpedia_id", ""),
                m.get("birthday", ""),
                m.get("gender", ""),
                terms_count,
                served_until,
            ))
            count += 1
        except sqlite3.Error:
            pass

    conn.commit()
    current = sum(1 for m in members if m.get("is_current"))
    log.info(f"  Done: {count:,} members ({current} current)")
    return count


def import_stock_trades(conn: sqlite3.Connection):
    """Import stock trading disclosures (Senate trades + House FD indexes)."""
    if not STOCK_DIR.exists():
        log.info("Skipping stock trades (no stock_trades/ directory)")
        return 0

    log.info("Importing stock trades...")

    # Build name->bioguide lookup from congress_members
    name_to_bioguide = {}
    members_file = MEMBERS_DIR / "members_lookup.json"
    if members_file.exists():
        with open(members_file) as f:
            members = json.load(f)
        for m in members:
            bid = m["bioguide_id"]
            # Add all name variants for matching (normalized)
            variants = json.loads(m.get("name_variants", "[]"))
            for v in variants:
                name_to_bioguide[v.lower().strip()] = bid
            full = m.get("full_name", "")
            if full:
                name_to_bioguide[full.lower().strip()] = bid
            # Also add first+last without middle name
            first = m.get("first_name", "").lower().strip()
            last = m.get("last_name", "").lower().strip()
            if first and last:
                name_to_bioguide[f"{first} {last}"] = bid
            # Nickname + last
            nick = m.get("nickname", "").lower().strip()
            if nick and last:
                name_to_bioguide[f"{nick} {last}"] = bid

    # Manual overrides for names the fuzzy matcher can't resolve
    _name_overrides = {
        "daniel goldman": "G000599",
        "craig goldman": "G000601",
        "nicholas van taylor": "T000479",
        "richard w. allen": "A000372",
        "christopher l. jacobs": "J000020",
        "jerry moran,": "M000934",
        "william cassidy": "C001075",
        "daniel s sullivan": "S001198",
        "elizabeth fletcher": "F000468",
        "james french hill": "H001072",
        "michael garcia": "G000061",
        "daniel crenshaw": "C001120",
        "michael john gallagher": "G000579",
        "james e hon banks": "B001299",
        "a. mitchell mcconnell, jr.": "M000355",
        "luis v. gutierrez": "G000535",
        "james e. banks": "B001299",
        "john p ricketts": "R000618",
        "kenneth r. buck": "B001297",
        "ashley hinson arenholz": "H001091",
        "nanette barragan": "B001300",
        # House legal-name / variant-name overrides
        "felix barry moore": "M001212",
        "theodore p. budd": "B001305",
        "theodore budd": "B001305",
        "james d jordan": "J000289",
        "james d. jordan": "J000289",
        "bradley mark walker": "W000819",
        "angela dawn craig": "C001119",
        "alfred lawson": "L000586",
        "william hurd": "H001073",
        "neal patrick dunn": "D000628",
        "benjamin lee cline": "C001118",
        "elizabeth cheney": "C001109",
        "greg francis murphy": "M001210",
        "gregory j. pence": "P000615",
        "jenniffer gonzalez colon": "G000582",
        "jenniffer gonzalez-colon": "G000582",
        "jenniffer gonzalez": "G000582",
        "john kevin ellzey": "E000071",
        "james hagedorn": "H001088",
        "anderson drew ferguson": "F000465",
        "katherine porter": "P000618",
        "terrycina andrea sewell": "S001185",
        "steven dane russell": "R000604",
        "donald john bacon": "B001298",
        "donald j. bacon": "B001298",
        "don davis": "D000230",
        "sydney kamlager": "K000400",
        "jeff van drew": "V000133",
        "monica de la cruz hernandez": "D000594",
        "zach nunn": "N000193",
        "michael d. bishop": "B001293",
        "nydia m. velasquez": "V000081",
        "douglas allen collins": "C001093",
        "thomas bryant cotton": "C001095",
        "c.w. bill young": "Y000031",
        "sheila jackson-lee": "J000032",
        "shelia jackson lee": "J000032",
        "m shontel brown": "B001313",
        "aumua amata": "R000600",
        "matthew james salmon": "S000018",
        "dennis lynn heck": "H001064",
    }
    for override_name, override_bid in _name_overrides.items():
        name_to_bioguide[override_name] = override_bid

    # Common suffixes and titles to strip
    _suffixes = {" jr", " jr.", " sr", " sr.", " ii", " iii", " iv",
                 ", jr", ", jr.", ", sr", ", sr.", ", ii", ", iii", ", iv",
                 " md", ", md", " facs", ", facs", " md, facs", ", md, facs"}

    def _normalize(name):
        """Normalize a name for matching: lowercase, strip suffixes/titles."""
        import unicodedata
        n = name.lower().strip()
        # Strip Unicode accents (é->e, á->a, etc.)
        n = unicodedata.normalize('NFKD', n).encode('ascii', 'ignore').decode('ascii')
        # Normalize curly quotes
        n = n.replace('\u2019', "'").replace('\u2018', "'")
        n = re.sub(r'\s+', ' ', n)
        # Strip trailing suffixes
        for suf in _suffixes:
            if n.endswith(suf):
                n = n[:-len(suf)].strip().rstrip(',')
        # Strip leading "Hon." etc.
        n = re.sub(r'^(?:hon\.?|the honorable|dr\.?|mr\.?|mrs\.?|ms\.?|captain|capt\.?)\s+', '', n)
        return n.strip()

    def resolve_bioguide(name):
        """Try to resolve a name to a bioguide_id with slightly fuzzy matching."""
        if not name:
            return ""
        key = _normalize(name)
        if key in name_to_bioguide:
            return name_to_bioguide[key]

        # Try first + last only (skip middle names/initials)
        parts = key.split()
        if len(parts) >= 2:
            simple = f"{parts[0]} {parts[-1]}"
            if simple in name_to_bioguide:
                return name_to_bioguide[simple]

        # Try with middle initial removed: "John A Smith" -> "John Smith"
        if len(parts) == 3 and len(parts[1]) <= 2:
            no_middle = f"{parts[0]} {parts[2]}"
            if no_middle in name_to_bioguide:
                return name_to_bioguide[no_middle]

        # Try last name only if unique in the lookup
        if len(parts) >= 2:
            last = parts[-1]
            last_matches = [bid for k, bid in name_to_bioguide.items()
                           if k.endswith(f" {last}")]
            if len(set(last_matches)) == 1:
                return last_matches[0]

        return ""

    def normalize_date(d):
        """Convert MM/DD/YYYY to YYYY-MM-DD. Pass through other formats."""
        if not d:
            return ""
        m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', d.strip())
        if m:
            month, day, year = m.groups()
            if int(year) > 2030:  # Filter bad dates like 2049
                return ""
            return f"{year}-{int(month):02d}-{int(day):02d}"
        return d  # Already ISO or other format

    total = 0

    # Senate trades — prefer eFD (government source) over third-party
    efd_file = STOCK_DIR / "senate_efd" / "all_transactions.json"
    legacy_file = STOCK_DIR / "senate_trades.json"

    senate_file = efd_file if efd_file.exists() else legacy_file
    source_label = "eFD (government)" if efd_file.exists() else "legacy (third-party)"

    if senate_file.exists():
        with open(senate_file) as f:
            trades = json.load(f)
        for t in trades:
            # Handle both eFD format and legacy format
            if "senator_filer_name" in t:
                # eFD format: senator_first_name, senator_last_name, etc.
                member_name = f"{t.get('senator_first_name', '')} {t.get('senator_last_name', '')}".strip()
                ticker = t.get("ticker", "")
                asset_desc = t.get("asset_name", "")
                asset_type = t.get("asset_type", "")
                tx_type = t.get("transaction_type", "")
                amount = t.get("amount", "")
                owner = t.get("owner", "")
                comment = t.get("comment", "")
                tx_date = t.get("transaction_date", "")
                source_url = t.get("source_url", "")
            else:
                # Legacy format
                member_name = t.get("member_name", "")
                ticker = t.get("ticker", "")
                asset_desc = t.get("asset_description", "")
                asset_type = t.get("asset_type", "")
                tx_type = t.get("transaction_type", "")
                amount = t.get("amount_range", "")
                owner = t.get("owner", "")
                comment = t.get("comment", "")
                tx_date = t.get("transaction_date", "")
                source_url = t.get("source_url", "")

            bioguide = resolve_bioguide(member_name)
            try:
                conn.execute("""
                    INSERT INTO stock_trades
                    (member_name, bioguide_id, chamber, transaction_date,
                     ticker, asset_description, asset_type, transaction_type,
                     amount_range, owner, comment, source_url, filing_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    member_name, bioguide, "Senate",
                    normalize_date(tx_date),
                    ticker, asset_desc, asset_type, tx_type,
                    amount, owner, comment, source_url, "P",
                ))
                total += 1
            except sqlite3.Error:
                pass
        conn.commit()
        log.info(f"  Senate trades ({source_label}): {len(trades):,}")

    # House — prefer parsed PTR transactions over filing indexes
    house_ptr_file = STOCK_DIR / "house_ptrs" / "all_transactions.json"
    house_dir = STOCK_DIR / "house"

    if house_ptr_file.exists():
        # Parsed transaction-level data from PTR PDFs
        with open(house_ptr_file) as f:
            house_txs = json.load(f)
        house_tx_count = 0
        # Track doc_ids that have parsed transactions
        parsed_doc_ids = set()
        for tx in house_txs:
            member_name = tx.get("member_name", "").strip()
            if not member_name:
                member_name = tx.get("filer_name", "").strip()
            bioguide = resolve_bioguide(member_name)
            doc_id = tx.get("doc_id", "")
            parsed_doc_ids.add(doc_id)
            try:
                conn.execute("""
                    INSERT INTO stock_trades
                    (member_name, bioguide_id, chamber, transaction_date,
                     disclosure_date, ticker, asset_description, asset_type,
                     transaction_type, amount_range, owner, source_url,
                     filing_type, state_district, doc_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    member_name, bioguide, "House",
                    normalize_date(tx.get("transaction_date", "")),
                    normalize_date(tx.get("filing_date", "")),
                    tx.get("ticker", ""),
                    tx.get("asset_name", ""),
                    tx.get("asset_type", ""),
                    tx.get("transaction_type", ""),
                    tx.get("amount", ""),
                    tx.get("owner", ""),
                    tx.get("source_url", ""),
                    "P",  # All parsed transactions are PTRs
                    tx.get("state_district", ""),
                    doc_id,
                ))
                house_tx_count += 1
            except sqlite3.Error:
                pass
        conn.commit()
        total += house_tx_count
        log.info(f"  House PTR transactions (parsed): {house_tx_count:,}")

        # Also import filing indexes for non-PTR filings and PTRs we couldn't parse
        if house_dir.exists():
            index_count = 0
            for fd_file in sorted(house_dir.glob("*.json")):
                try:
                    with open(fd_file) as f:
                        filings = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                for filing in filings:
                    doc_id = str(filing.get("doc_id", ""))
                    # Skip PTR filings that we already have parsed transactions for
                    if filing.get("filing_type") == "P" and doc_id in parsed_doc_ids:
                        continue
                    member_name = filing.get("member_name", "").strip()
                    if not member_name:
                        member_name = f"{filing.get('first_name', '')} {filing.get('last_name', '')}".strip()
                    bioguide = resolve_bioguide(member_name)
                    try:
                        conn.execute("""
                            INSERT INTO stock_trades
                            (member_name, bioguide_id, chamber, disclosure_date,
                             filing_type, state_district, doc_id, source_url)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            member_name, bioguide, "House",
                            normalize_date(filing.get("filing_date", "")),
                            filing.get("filing_type", ""),
                            filing.get("state_district", ""),
                            doc_id,
                            filing.get("pdf_url", ""),
                        ))
                        index_count += 1
                    except sqlite3.Error:
                        pass
                conn.commit()
            total += index_count
            log.info(f"  House filing indexes (non-PTR + unparsed): {index_count:,}")
    elif house_dir.exists():
        # Fallback: filing indexes only (no parsed PTR data)
        house_count = 0
        for fd_file in sorted(house_dir.glob("*.json")):
            try:
                with open(fd_file) as f:
                    filings = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
            for filing in filings:
                member_name = filing.get("member_name", "").strip()
                if not member_name:
                    member_name = f"{filing.get('first_name', '')} {filing.get('last_name', '')}".strip()
                bioguide = resolve_bioguide(member_name)
                try:
                    conn.execute("""
                        INSERT INTO stock_trades
                        (member_name, bioguide_id, chamber, disclosure_date,
                         filing_type, state_district, doc_id, source_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        member_name, bioguide, "House",
                        normalize_date(filing.get("filing_date", "")),
                        filing.get("filing_type", ""),
                        filing.get("state_district", ""),
                        filing.get("doc_id", ""),
                        filing.get("pdf_url", ""),
                    ))
                    house_count += 1
                except sqlite3.Error:
                    pass
            conn.commit()
        total += house_count
        log.info(f"  House filings (index only): {house_count:,}")

    # Stats
    matched = conn.execute(
        "SELECT COUNT(*) FROM stock_trades WHERE bioguide_id != ''"
    ).fetchone()[0]
    log.info(f"  Done: {total:,} stock trades ({matched:,} matched to bioguide_id)")
    return total


def import_committees(conn: sqlite3.Connection):
    """Import committee and membership data from congress-legislators."""
    committees_file = MEMBERS_DIR / "committees-current.json"
    membership_file = MEMBERS_DIR / "committee-membership-current.json"

    if not committees_file.exists() or not membership_file.exists():
        log.info("Skipping committees (no committee data files)")
        return 0

    log.info("Importing committee data...")

    # Load committees
    with open(committees_file) as f:
        committees_raw = json.load(f)

    committee_count = 0
    for c in committees_raw:
        chamber = c.get("type", "")  # "house", "senate", "joint"
        # Build committee ID from thomas_id or house/senate_committee_id
        cid = c.get("thomas_id", "")
        if not cid:
            cid = c.get("house_committee_id", c.get("senate_committee_id", ""))
        if not cid:
            continue

        try:
            conn.execute(
                "INSERT OR REPLACE INTO committees (committee_id, name, chamber, url) "
                "VALUES (?, ?, ?, ?)",
                (cid, c.get("name", ""), chamber, c.get("url", ""))
            )
            committee_count += 1
        except sqlite3.Error:
            pass

        # Subcommittees
        for sub in c.get("subcommittees", []):
            sub_id = f"{cid}{sub.get('thomas_id', '')}"
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO committees "
                    "(committee_id, name, chamber, url, parent_committee_id) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (sub_id, sub.get("name", ""), chamber, sub.get("url", ""), cid)
                )
                committee_count += 1
            except sqlite3.Error:
                pass

    conn.commit()
    log.info(f"  Committees: {committee_count}")

    # Load memberships
    with open(membership_file) as f:
        memberships_raw = json.load(f)

    member_count = 0
    for committee_id, members in memberships_raw.items():
        for m in members:
            bioguide = m.get("bioguide", "")
            if not bioguide:
                continue
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO committee_memberships "
                    "(bioguide_id, committee_id, member_name, party, title, rank) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (bioguide, committee_id, m.get("name", ""),
                     m.get("party", ""), m.get("title", ""),
                     m.get("rank"))
                )
                member_count += 1
            except sqlite3.Error:
                pass
    conn.commit()

    unique_members = conn.execute(
        "SELECT COUNT(DISTINCT bioguide_id) FROM committee_memberships"
    ).fetchone()[0]
    chairs = conn.execute(
        "SELECT COUNT(*) FROM committee_memberships WHERE title != ''"
    ).fetchone()[0]
    log.info(f"  Memberships: {member_count:,} ({unique_members} unique members, {chairs} with titles)")
    return member_count


def import_cosponsors(conn: sqlite3.Connection):
    """Import legislation cosponsors from parsed BILLSTATUS JSON files."""
    congress_dir = BASE_DIR / "congress_gov"
    if not congress_dir.exists():
        log.info("Skipping cosponsors (no congress_gov/ directory)")
        return 0

    log.info("Importing legislation cosponsors...")
    total = 0

    for congress_subdir in sorted(congress_dir.iterdir()):
        if not congress_subdir.is_dir():
            continue
        # Extract congress number from dir name (e.g., "congress_118" -> 118)
        congress_match = re.search(r'(\d+)', congress_subdir.name)
        if not congress_match:
            continue
        congress_num = int(congress_match.group(1))

        for bill_file in congress_subdir.glob("*.json"):
            try:
                with open(bill_file) as f:
                    bill_data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            # Skip list files (e.g., hconres_list.json)
            if not isinstance(bill_data, dict):
                continue

            # Build bill_id
            bill_type = (bill_data.get("type") or bill_data.get("bill_type") or "").lower()
            bill_number = bill_data.get("number") or bill_data.get("bill_number")
            if not (bill_type and bill_number):
                continue
            bill_id = f"{congress_num}-{bill_type}-{bill_number}"

            for cs in bill_data.get("cosponsors", []):
                bioguide = cs.get("bioguideId", "")
                if not bioguide:
                    continue
                is_original = 1 if cs.get("isOriginalCosponsor") == "True" else 0
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO legislation_cosponsors "
                        "(bill_id, bioguide_id, full_name, party, state, "
                        "sponsorship_date, is_original_cosponsor) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (bill_id, bioguide, cs.get("fullName", ""),
                         cs.get("party", ""), cs.get("state", ""),
                         cs.get("sponsorshipDate", ""), is_original)
                    )
                    total += 1
                except sqlite3.Error:
                    pass

        conn.commit()

    original = conn.execute(
        "SELECT COUNT(*) FROM legislation_cosponsors WHERE is_original_cosponsor = 1"
    ).fetchone()[0]
    unique_bills = conn.execute(
        "SELECT COUNT(DISTINCT bill_id) FROM legislation_cosponsors"
    ).fetchone()[0]
    log.info(f"  Done: {total:,} cosponsors across {unique_bills:,} bills "
             f"({original:,} original cosponsors)")
    return total


def build_crec_junction_tables(conn: sqlite3.Connection):
    """Build normalized speaker and bill tables from CREC JSON columns,
    enriched with bioguide_id from cong_members data in the JSON files."""
    log.info("Building CREC junction tables...")

    # Phase 1: Build speakers from DB column
    cursor = conn.execute(
        "SELECT granule_id, speakers FROM congressional_record "
        "WHERE speakers IS NOT NULL AND length(speakers) > 5"
    )
    speaker_count = 0
    for granule_id, speakers_json in cursor:
        try:
            speakers = json.loads(speakers_json)
        except (json.JSONDecodeError, TypeError):
            continue
        for s in speakers:
            name = s.get("name", "").strip()
            if not name:
                continue
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO crec_speakers "
                    "(granule_id, speaker_name, speaker_role) "
                    "VALUES (?, ?, ?)",
                    (granule_id, name, s.get("role", ""))
                )
                speaker_count += 1
            except sqlite3.Error:
                pass
    conn.commit()
    log.info(f"  Speakers: {speaker_count:,} entries")

    # Phase 2: Enrich speakers with bioguide_id from cong_members in JSON files
    log.info("  Enriching speakers with bioguide_id from MODS congMember data...")
    bioguide_updates = 0
    if CREC_DIR.exists():
        for year_dir in sorted(CREC_DIR.iterdir()):
            if not year_dir.is_dir():
                continue
            for json_file in year_dir.glob("*.json"):
                try:
                    with open(json_file) as f:
                        day_data = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                for granule in day_data.get("granules", []):
                    cong_members = granule.get("cong_members", [])
                    if not cong_members:
                        continue
                    gid = granule.get("granule_id", "")
                    for cm in cong_members:
                        bioguide = cm.get("bioguide_id", "")
                        if not bioguide:
                            continue
                        # Match by parsed name (e.g., "Mr. REID")
                        # or by authority name to speaker_name
                        parsed = cm.get("parsed_name", "")
                        authority = cm.get("authority_name", "")
                        party = cm.get("party", "")
                        state = cm.get("state", "")
                        # Try to update matching speaker rows
                        for name_try in [parsed, authority]:
                            if not name_try:
                                continue
                            try:
                                result = conn.execute(
                                    "UPDATE crec_speakers SET bioguide_id=?, party=?, state=? "
                                    "WHERE granule_id=? AND speaker_name=? AND bioguide_id IS NULL",
                                    (bioguide, party, state, gid, name_try)
                                )
                                if result.rowcount > 0:
                                    bioguide_updates += result.rowcount
                                    break
                            except sqlite3.Error:
                                pass
        conn.commit()
    log.info(f"  Bioguide enrichment: {bioguide_updates:,} speakers linked")

    # Phase 3: Bills
    cursor = conn.execute(
        "SELECT granule_id, bills FROM congressional_record "
        "WHERE bills IS NOT NULL AND length(bills) > 5"
    )
    bill_count = 0
    for granule_id, bills_json in cursor:
        try:
            bills = json.loads(bills_json)
        except (json.JSONDecodeError, TypeError):
            continue
        for b in bills:
            congress = b.get("congress", "")
            bill_type = b.get("type", "").lower()
            number = b.get("number", "")
            if not (congress and bill_type and number):
                continue
            try:
                congress_int = int(congress)
                number_int = int(number)
            except (ValueError, TypeError):
                continue
            bill_id = f"{congress_int}-{bill_type}-{number_int}"
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO crec_bills "
                    "(granule_id, congress, bill_type, bill_number, bill_id) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (granule_id, congress_int, bill_type, number_int, bill_id)
                )
                bill_count += 1
            except sqlite3.Error:
                pass
    conn.commit()
    log.info(f"  Bill references: {bill_count:,} entries")

    # Count cross-references
    crossref = conn.execute(
        "SELECT COUNT(DISTINCT cb.bill_id) FROM crec_bills cb "
        "JOIN legislation l ON cb.bill_id = l.bill_id"
    ).fetchone()[0]
    log.info(f"  CREC bills matching legislation table: {crossref:,}")

    # Stats on bioguide coverage
    total_speakers = conn.execute("SELECT COUNT(*) FROM crec_speakers").fetchone()[0]
    linked_speakers = conn.execute(
        "SELECT COUNT(*) FROM crec_speakers WHERE bioguide_id IS NOT NULL"
    ).fetchone()[0]
    unique_bioguides = conn.execute(
        "SELECT COUNT(DISTINCT bioguide_id) FROM crec_speakers WHERE bioguide_id IS NOT NULL"
    ).fetchone()[0]
    log.info(f"  Bioguide coverage: {linked_speakers:,}/{total_speakers:,} "
             f"speaker entries ({unique_bioguides} unique members)")

    return speaker_count, bill_count


def build_crossref(conn: sqlite3.Connection):
    """Build cross-reference between Federal Register and Regulations.gov documents."""
    log.info("Building FR <-> Regulations.gov cross-reference...")

    # Phase 1: Direct match on fr_doc_num = document_number
    conn.execute("""
        INSERT OR IGNORE INTO fr_regs_crossref (fr_document_number, regs_document_id)
        SELECT fr.document_number, d.id
        FROM federal_register fr
        JOIN documents d ON d.fr_doc_num = fr.document_number
        WHERE d.fr_doc_num IS NOT NULL AND d.fr_doc_num != ''
    """)
    count1 = conn.execute("SELECT COUNT(*) FROM fr_regs_crossref").fetchone()[0]
    log.info(f"  Phase 1 (direct match): {count1:,}")

    # Phase 2: Normalized match — strip spaces, convert en-dashes/em-dashes to hyphens
    conn.execute("""
        INSERT OR IGNORE INTO fr_regs_crossref (fr_document_number, regs_document_id)
        SELECT fr.document_number, d.id
        FROM federal_register fr
        JOIN documents d
            ON fr.document_number = REPLACE(REPLACE(REPLACE(d.fr_doc_num, ' ', ''), '–', '-'), '—', '-')
        WHERE d.fr_doc_num IS NOT NULL AND d.fr_doc_num != ''
        AND NOT EXISTS (
            SELECT 1 FROM fr_regs_crossref x WHERE x.regs_document_id = d.id
        )
    """)
    count2 = conn.execute("SELECT COUNT(*) FROM fr_regs_crossref").fetchone()[0]
    log.info(f"  Phase 2 (normalized): +{count2 - count1:,} = {count2:,}")

    # Phase 2b: Leading-zero normalization — Regulations.gov zero-pads the numeric
    # suffix (e.g. "E5-05178", "2010-00345", "06-00087") but the Federal Register
    # API does not (e.g. "E5-5178", "2010-345", "06-87").  After converting dashes,
    # strip leading zeros from the portion after the first hyphen.
    conn.execute("""
        INSERT OR IGNORE INTO fr_regs_crossref (fr_document_number, regs_document_id)
        SELECT fr.document_number, d.id
        FROM documents d
        JOIN federal_register fr
            ON fr.document_number =
                SUBSTR(
                    REPLACE(REPLACE(REPLACE(d.fr_doc_num, ' ', ''), '–', '-'), '—', '-'),
                    1,
                    INSTR(REPLACE(REPLACE(REPLACE(d.fr_doc_num, ' ', ''), '–', '-'), '—', '-'), '-')
                )
                || CAST(
                    CAST(
                        SUBSTR(
                            REPLACE(REPLACE(REPLACE(d.fr_doc_num, ' ', ''), '–', '-'), '—', '-'),
                            INSTR(REPLACE(REPLACE(REPLACE(d.fr_doc_num, ' ', ''), '–', '-'), '—', '-'), '-') + 1
                        ) AS INTEGER
                    ) AS TEXT
                )
        WHERE d.fr_doc_num IS NOT NULL AND d.fr_doc_num != ''
        AND d.fr_doc_num LIKE '%-%'
        AND NOT EXISTS (
            SELECT 1 FROM fr_regs_crossref x WHERE x.regs_document_id = d.id
        )
    """)
    count2b = conn.execute("SELECT COUNT(*) FROM fr_regs_crossref").fetchone()[0]
    log.info(f"  Phase 2b (leading-zero strip): +{count2b - count2:,} = {count2b:,}")

    # Phase 2c: Case-insensitive title + date match for documents whose fr_doc_num
    # is a short FR page number (not a document number) that cannot match directly.
    # Only accept unique matches to avoid false positives.
    conn.execute("""
        INSERT OR IGNORE INTO fr_regs_crossref (fr_document_number, regs_document_id)
        SELECT fr_doc, regs_doc FROM (
            SELECT fr.document_number AS fr_doc, d.id AS regs_doc,
                   COUNT(*) OVER (PARTITION BY d.id) AS match_count
            FROM documents d
            JOIN federal_register fr
                ON date(d.posted_date) = fr.publication_date
                AND LOWER(d.title) = LOWER(fr.title)
            WHERE d.fr_doc_num IS NOT NULL AND d.fr_doc_num != ''
            AND NOT EXISTS (
                SELECT 1 FROM fr_regs_crossref x WHERE x.regs_document_id = d.id
            )
            AND length(d.title) > 20
        )
        WHERE match_count = 1
    """)
    count2c = conn.execute("SELECT COUNT(*) FROM fr_regs_crossref").fetchone()[0]
    log.info(f"  Phase 2c (title+date for page-nums): +{count2c - count2b:,} = {count2c:,}")

    # Phase 3: Date + Title match for documents WITHOUT any fr_doc_num
    # Find documents where exactly one FR record matches on both date and title
    conn.execute("""
        INSERT OR IGNORE INTO fr_regs_crossref (fr_document_number, regs_document_id)
        SELECT fr_doc, regs_doc FROM (
            SELECT fr.document_number AS fr_doc, d.id AS regs_doc,
                   COUNT(*) OVER (PARTITION BY d.id) AS match_count
            FROM documents d
            JOIN federal_register fr
                ON date(d.posted_date) = fr.publication_date
                AND d.title = fr.title
            WHERE (d.fr_doc_num IS NULL OR d.fr_doc_num = '')
            AND NOT EXISTS (
                SELECT 1 FROM fr_regs_crossref x WHERE x.regs_document_id = d.id
            )
            AND length(d.title) > 20
        )
        WHERE match_count = 1
    """)
    count3 = conn.execute("SELECT COUNT(*) FROM fr_regs_crossref").fetchone()[0]
    log.info(f"  Phase 3 (date+title, no fr_doc_num): +{count3 - count2c:,} = {count3:,}")

    conn.commit()
    log.info(f"  Done: {count3:,} total cross-references")
    return count3


def build_agency_map(conn: sqlite3.Connection):
    """Build mapping between regulations.gov agency codes and FR agencies."""
    log.info("Building agency code mapping...")

    # Get regs.gov agency codes from documents
    regs_agencies = conn.execute(
        "SELECT DISTINCT agency_id FROM documents WHERE agency_id IS NOT NULL"
    ).fetchall()

    mapped = 0
    for (code,) in regs_agencies:
        # Try to find a matching FR agency by name/raw_name containing the code
        # or by looking at documents that have both a regs agency_id and fr_doc_num
        row = conn.execute("""
            SELECT a.id, a.name, a.raw_name
            FROM agencies a
            JOIN federal_register_agencies fra ON fra.agency_id = a.id
            JOIN fr_regs_crossref xr ON xr.fr_document_number = fra.document_number
            JOIN documents d ON d.id = xr.regs_document_id
            WHERE d.agency_id = ?
            GROUP BY a.id
            ORDER BY COUNT(*) DESC
            LIMIT 1
        """, (code,)).fetchone()

        if row:
            conn.execute("""
                INSERT OR IGNORE INTO agency_map (regs_code, fr_agency_id, fr_agency_name, fr_raw_name)
                VALUES (?, ?, ?, ?)
            """, (code, row[0], row[1], row[2]))
            mapped += 1

    conn.commit()
    log.info(f"  Done: {mapped} agency codes mapped")
    return mapped


def build_views(conn: sqlite3.Connection):
    """Create analytical views."""
    log.info("Creating views...")
    conn.executescript(VIEWS)
    conn.commit()
    log.info("  Views created")


def build_member_stats(conn: sqlite3.Connection):
    """Denormalize activity counts into congress_members for fast single-table queries."""
    log.info("Building member stats (denormalized into congress_members)...")

    # Add stats columns to congress_members if they don't exist
    existing = {row[1] for row in conn.execute("PRAGMA table_info(congress_members)")}
    for col in ["trade_count", "speech_count", "bills_sponsored", "vote_count", "fec_total_received"]:
        if col not in existing:
            conn.execute(f"ALTER TABLE congress_members ADD COLUMN {col} INTEGER DEFAULT 0")

    # Compute and update stats in-place
    conn.execute("""
        UPDATE congress_members SET
            trade_count = (SELECT COUNT(*) FROM stock_trades WHERE bioguide_id = congress_members.bioguide_id),
            speech_count = (SELECT COUNT(*) FROM crec_speakers WHERE bioguide_id = congress_members.bioguide_id),
            bills_sponsored = (SELECT COUNT(*) FROM legislation WHERE sponsor_bioguide_id = congress_members.bioguide_id),
            vote_count = (SELECT COUNT(*) FROM member_votes WHERE bioguide_id = congress_members.bioguide_id),
            fec_total_received = (SELECT COALESCE(SUM(fc.transaction_amt), 0) FROM fec_contributions fc
                JOIN fec_candidate_crosswalk xw ON xw.fec_candidate_id = fc.cand_id
                WHERE xw.bioguide_id = congress_members.bioguide_id)
    """)

    # Composite index for the default sort (current members by most trades)
    conn.execute("DROP INDEX IF EXISTS idx_cm_current_trades")
    conn.execute("CREATE INDEX idx_cm_current_trades ON congress_members(is_current DESC, trade_count DESC)")

    # Keep member_stats as a view for backward compatibility
    conn.execute("DROP TABLE IF EXISTS member_stats")
    conn.execute("DROP VIEW IF EXISTS member_stats")
    conn.execute("""
        CREATE VIEW member_stats AS
        SELECT bioguide_id, trade_count, speech_count, bills_sponsored, vote_count, fec_total_received
        FROM congress_members
    """)

    conn.execute("CREATE TABLE IF NOT EXISTS build_metadata (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT OR REPLACE INTO build_metadata VALUES ('member_stats_updated', date('now'))")
    conn.commit()
    log.info(f"  member stats denormalized for {conn.execute('SELECT COUNT(*) FROM congress_members WHERE trade_count > 0').fetchone()[0]:,} members with trades")


def build_docket_summary(conn: sqlite3.Connection):
    """Build docket summary table from FR doc abstracts for enriched FTS."""
    log.info("Building docket_summary table...")
    conn.execute("DROP TABLE IF EXISTS docket_summary")
    conn.execute("""
        CREATE TABLE docket_summary AS
        SELECT doc.docket_id, fr.abstract AS summary
        FROM documents doc
        JOIN federal_register fr ON fr.document_number = doc.fr_doc_num
        WHERE fr.abstract IS NOT NULL AND fr.abstract != '' AND doc.docket_id IS NOT NULL
        GROUP BY doc.docket_id
        HAVING LENGTH(fr.abstract) = MAX(LENGTH(fr.abstract))
    """)
    conn.execute("CREATE UNIQUE INDEX idx_docket_summary ON docket_summary(docket_id)")
    conn.commit()
    log.info(f"  docket_summary: {conn.execute('SELECT COUNT(*) FROM docket_summary').fetchone()[0]:,} rows")


def build_summary_tables(conn: sqlite3.Connection):
    """Build pre-computed summary tables for explore page browse tabs."""
    log.info("Building pre-computed summary tables...")

    # Lobbying issue summary (replaces ~29s GROUP BY on lobbying_activities)
    conn.execute("DROP TABLE IF EXISTS lobbying_issue_summary")
    conn.execute("""
        CREATE TABLE lobbying_issue_summary AS
        SELECT la.issue_code, ic.description,
            COUNT(*) AS filing_count,
            COUNT(DISTINCT la.client_name) AS client_count,
            CAST(SUM(la.income_amount) AS INTEGER) AS total_income
        FROM lobbying_activities la
        LEFT JOIN lobbying_issue_codes ic ON la.issue_code = ic.code
        GROUP BY la.issue_code
        ORDER BY filing_count DESC
    """)
    conn.execute("CREATE UNIQUE INDEX idx_lis_code ON lobbying_issue_summary(issue_code)")
    conn.commit()
    log.info(f"  lobbying_issue_summary: {conn.execute('SELECT COUNT(*) FROM lobbying_issue_summary').fetchone()[0]:,} rows")

    # Legislation policy area summary (replaces ~15s GROUP BY on legislation)
    conn.execute("DROP TABLE IF EXISTS legislation_policy_summary")
    conn.execute("""
        CREATE TABLE legislation_policy_summary AS
        SELECT policy_area, COUNT(*) AS bill_count,
               COUNT(DISTINCT sponsor_bioguide_id) AS sponsor_count,
               MIN(congress) AS from_congress, MAX(congress) AS to_congress
        FROM legislation
        WHERE policy_area IS NOT NULL AND policy_area != ''
        GROUP BY policy_area
        ORDER BY bill_count DESC
    """)
    conn.execute("CREATE UNIQUE INDEX idx_lps_area ON legislation_policy_summary(policy_area)")
    conn.commit()
    log.info(f"  legislation_policy_summary: {conn.execute('SELECT COUNT(*) FROM legislation_policy_summary').fetchone()[0]:,} rows")

    # Legislation sponsor summary (replaces ~12s GROUP BY on legislation)
    conn.execute("DROP TABLE IF EXISTS legislation_sponsor_summary")
    conn.execute("""
        CREATE TABLE legislation_sponsor_summary AS
        SELECT sponsor_name, sponsor_party, sponsor_state, sponsor_bioguide_id,
               COUNT(*) AS bill_count
        FROM legislation
        WHERE sponsor_name IS NOT NULL AND sponsor_name != ''
        GROUP BY sponsor_bioguide_id
        ORDER BY bill_count DESC
    """)
    conn.execute("CREATE UNIQUE INDEX idx_lss_bioguide ON legislation_sponsor_summary(sponsor_bioguide_id)")
    conn.commit()
    log.info(f"  legislation_sponsor_summary: {conn.execute('SELECT COUNT(*) FROM legislation_sponsor_summary').fetchone()[0]:,} rows")

    # Spending agency summary (replaces ~7s GROUP BY on spending_awards)
    conn.execute("DROP TABLE IF EXISTS spending_agency_summary")
    conn.execute("""
        CREATE TABLE spending_agency_summary AS
        SELECT agency AS name,
            COUNT(*) AS award_count,
            CAST(SUM(award_amount) AS INTEGER) AS total_spending,
            COUNT(DISTINCT sub_agency) AS sub_count
        FROM spending_awards
        GROUP BY agency
        ORDER BY total_spending DESC
    """)
    conn.execute("CREATE UNIQUE INDEX idx_sas_agency ON spending_agency_summary(name)")
    conn.commit()
    log.info(f"  spending_agency_summary: {conn.execute('SELECT COUNT(*) FROM spending_agency_summary').fetchone()[0]:,} rows")

    log.info("  Summary tables built")


def build_fts(conn: sqlite3.Connection):
    """Build full-text search indexes."""
    log.info("Building FTS5 indexes...")
    conn.executescript(FTS_SCHEMA)

    for table in ["federal_register_fts", "documents_fts", "comments_fts",
                   "cfr_fts", "crec_fts", "lobbying_fts",
                   "spending_awards_fts", "legislation_fts",
                   "fara_registrants_fts", "fara_foreign_principals_fts",
                   "fec_employer_fts"]:
        try:
            source_map = {'cfr_fts': 'cfr_sections', 'crec_fts': 'congressional_record', 'lobbying_fts': 'lobbying_activities', 'spending_awards_fts': 'spending_awards', 'legislation_fts': 'legislation', 'fara_registrants_fts': 'fara_registrants', 'fara_foreign_principals_fts': 'fara_foreign_principals', 'fec_employer_fts': 'fec_employer_totals'}
            source_table = source_map.get(table, table.replace('_fts', ''))
            count = conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
            if count > 0:
                log.info(f"  Rebuilding {table}...")
                conn.execute(f"INSERT INTO {table}({table}) VALUES('rebuild')")
                conn.commit()
        except sqlite3.OperationalError:
            pass

    # dockets_fts is standalone (not content-linked) — populate with JOIN to docket_summary
    log.info("  Populating dockets_fts with summaries...")
    conn.execute("""
        INSERT INTO dockets_fts(rowid, title, agency_id, summary)
        SELECT d.rowid, d.title, d.agency_id, COALESCE(ds.summary, '')
        FROM dockets d
        LEFT JOIN docket_summary ds ON ds.docket_id = d.id
    """)
    conn.commit()

    log.info("  FTS indexes built")


def print_stats(conn: sqlite3.Connection):
    """Print database statistics."""
    log.info("\n" + "=" * 50)
    log.info("DATABASE STATISTICS")
    log.info("=" * 50)

    tables = [
        ("federal_register", "Federal Register docs"),
        ("agencies", "Agencies"),
        ("agency_map", "Agency code mappings"),
        ("dockets", "Dockets"),
        ("documents", "Documents"),
        ("comments", "Comment headers"),
        ("comment_details", "Comment details (full text)"),
        ("fr_regs_crossref", "FR <-> Regs.gov links"),
        ("presidential_documents", "Presidential documents"),
        ("spending_awards", "Spending awards"),
        ("legislation", "Legislation bills"),
        ("cfr_sections", "CFR sections"),
        ("congressional_record", "Congressional Record"),
        ("crec_speakers", "CREC speakers"),
        ("crec_bills", "CREC bill references"),
        ("congress_members", "Congress members"),
        ("committees", "Committees"),
        ("committee_memberships", "Committee memberships"),
        ("legislation_cosponsors", "Legislation cosponsors"),
        ("stock_trades", "Stock trades"),
        ("lobbying_filings", "Lobbying filings"),
        ("lobbying_activities", "Lobbying activities"),
        ("lobbying_lobbyists", "Lobbying lobbyists"),
        ("lobbying_contributions", "Lobbying contributions"),
        ("roll_call_votes", "Roll call votes"),
        ("member_votes", "Member votes"),
        ("fec_candidates", "FEC candidates"),
        ("fec_committees", "FEC committees"),
        ("fec_contributions", "FEC contributions to candidates"),
        ("fec_candidate_crosswalk", "FEC-bioguide crosswalk"),
        ("fec_employer_totals", "FEC employer totals"),
        ("fec_employer_to_candidate", "FEC employer→candidate"),
        ("fec_employer_to_party", "FEC employer→party"),
        ("fec_top_occupations", "FEC top occupations"),
        ("fara_registrants", "FARA registrants"),
        ("fara_foreign_principals", "FARA foreign principals"),
        ("fara_short_forms", "FARA short forms (agents)"),
        ("fara_registrant_docs", "FARA registrant documents"),
    ]

    for table, label in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(f"  {label}: {count:,}")
        except sqlite3.OperationalError:
            log.info(f"  {label}: (not populated)")

    # Comment submitter breakdown
    log.info("\nComment submitter types:")
    for row in conn.execute(
        "SELECT submitter_type, COUNT(*) as cnt FROM comments GROUP BY submitter_type ORDER BY cnt DESC"
    ):
        log.info(f"  {row[0]}: {row[1]:,}")

    # Comments by agency
    log.info("\nComments by agency:")
    for row in conn.execute(
        "SELECT agency_id, COUNT(*) as cnt FROM comments GROUP BY agency_id ORDER BY cnt DESC"
    ):
        log.info(f"  {row[0]}: {row[1]:,}")

    # Top dockets by comment count
    log.info("\nTop 10 dockets by comment count:")
    for row in conn.execute("""
        SELECT docket_id, agency_id, COUNT(*) as cnt
        FROM comments
        WHERE docket_id IS NOT NULL
        GROUP BY docket_id
        ORDER BY cnt DESC
        LIMIT 10
    """):
        log.info(f"  {row[0]} ({row[1]}): {row[2]:,} comments")

    # Document types
    log.info("\nDocuments by type:")
    for row in conn.execute(
        "SELECT document_type, COUNT(*) as cnt FROM documents GROUP BY document_type ORDER BY cnt DESC LIMIT 10"
    ):
        log.info(f"  {row[0]}: {row[1]:,}")

    size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    log.info(f"\nDatabase size: {size_mb:.1f} MB")


def main():
    log.info("=" * 60)
    log.info("BUILDING OPENREGS DATABASE (with enrichments)")
    log.info(f"Output: {DB_PATH}")
    log.info("=" * 60)

    start = time.time()

    if DB_PATH.exists():
        DB_PATH.unlink()
        log.info("Removed existing database")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-256000")  # 256MB cache
    conn.executescript(SCHEMA)

    try:
        import_federal_register(conn)
        import_dockets(conn)
        import_documents(conn)
        import_comments(conn)
        import_comment_details(conn)
        backfill_orphan_dockets(conn)
        import_presidential_documents(conn)
        import_spending(conn)
        import_lobbying(conn)
        import_fec(conn)
        import_fec_employers(conn)
        import_votes(conn)
        import_fara(conn)
        import_legislation(conn)
        import_ecfr(conn)
        import_congressional_record(conn)
        import_congress_members(conn)
        import_committees(conn)
        import_stock_trades(conn)
        import_cosponsors(conn)
        build_crec_junction_tables(conn)
        build_crossref(conn)
        build_agency_map(conn)
        build_views(conn)
        build_member_stats(conn)
        build_docket_summary(conn)
        build_summary_tables(conn)
        build_fts(conn)
        print_stats(conn)
    finally:
        conn.close()

    elapsed = time.time() - start
    log.info(f"\nDone in {elapsed/60:.1f} minutes")
    log.info(f"Database ready: {DB_PATH}")


if __name__ == "__main__":
    main()
