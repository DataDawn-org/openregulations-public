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
import shutil
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
COMMENTS_DIR = REGS_DIR / "comments" / "headers"     # Original 5-agency headers
COMMENTS_EXPANSION_DIR = REGS_DIR / "comments"        # Expansion agencies (direct under comments/)
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
    excerpts TEXT,
    regulation_id_numbers TEXT
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
    organization_normalized TEXT,
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
CREATE INDEX IF NOT EXISTS idx_cd_org_norm ON comment_details(organization_normalized) WHERE organization_normalized IS NOT NULL;
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
    recipient_name_normalized TEXT,
    recipient_uei TEXT,
    recipient_duns TEXT,
    recipient_id TEXT,
    recipient_location_state TEXT,
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
CREATE INDEX IF NOT EXISTS idx_spending_recipient_norm ON spending_awards(recipient_name_normalized);
CREATE INDEX IF NOT EXISTS idx_spending_uei ON spending_awards(recipient_uei) WHERE recipient_uei IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_spending_duns ON spending_awards(recipient_duns) WHERE recipient_duns IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_spending_state ON spending_awards(state_code);

-- Lobbying disclosure filings (from Senate LDA API)
CREATE TABLE IF NOT EXISTS lobbying_filings (
    filing_uuid TEXT PRIMARY KEY,
    filing_type TEXT NOT NULL,
    registrant_id INTEGER,
    registrant_name TEXT,
    registrant_state TEXT,
    registrant_country TEXT,
    registrant_house_id INTEGER,
    client_id INTEGER,
    client_name TEXT,
    client_state TEXT,
    client_ppb_state TEXT,
    client_country TEXT,
    client_ppb_country TEXT,
    client_general_description TEXT,
    client_government_entity INTEGER,
    filing_year INTEGER,
    filing_period TEXT,
    received_date TEXT,
    amount_reported REAL,
    is_amendment INTEGER DEFAULT 0,
    is_no_activity INTEGER DEFAULT 0,
    is_termination INTEGER DEFAULT 0,
    affiliated_org_count INTEGER
);

CREATE TABLE IF NOT EXISTS lobbying_affiliated_orgs (
    id INTEGER PRIMARY KEY,
    filing_uuid TEXT NOT NULL,
    org_name TEXT,
    country TEXT,
    country_code TEXT
);
CREATE INDEX IF NOT EXISTS idx_lobby_aff_filing ON lobbying_affiliated_orgs(filing_uuid);
CREATE INDEX IF NOT EXISTS idx_lobby_aff_name ON lobbying_affiliated_orgs(org_name);

CREATE TABLE IF NOT EXISTS lobbying_activities (
    id INTEGER PRIMARY KEY,
    filing_uuid TEXT NOT NULL,
    filing_type TEXT NOT NULL,
    registrant_name TEXT NOT NULL,
    registrant_id INTEGER,
    client_name TEXT NOT NULL,
    client_name_normalized TEXT,
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
CREATE INDEX IF NOT EXISTS idx_lobby_client_state ON lobbying_filings(client_state);
CREATE INDEX IF NOT EXISTS idx_lobby_registrant_state ON lobbying_filings(registrant_state);
CREATE INDEX IF NOT EXISTS idx_lobby_house_id ON lobbying_filings(registrant_house_id) WHERE registrant_house_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_lobby_govt_entity ON lobbying_filings(client_government_entity) WHERE client_government_entity = 1;
CREATE INDEX IF NOT EXISTS idx_lobby_act_client ON lobbying_activities(client_name);
CREATE INDEX IF NOT EXISTS idx_lobby_act_client_norm ON lobbying_activities(client_name_normalized);
CREATE INDEX IF NOT EXISTS idx_lobby_act_issue ON lobbying_activities(issue_code);
CREATE INDEX IF NOT EXISTS idx_lobby_act_year ON lobbying_activities(filing_year);
CREATE INDEX IF NOT EXISTS idx_lobby_act_uuid ON lobbying_activities(filing_uuid);
CREATE INDEX IF NOT EXISTS idx_lobby_act_registrant ON lobbying_activities(registrant_name);
CREATE INDEX IF NOT EXISTS idx_lobby_lob_uuid ON lobbying_lobbyists(filing_uuid);
CREATE INDEX IF NOT EXISTS idx_lobby_lob_name ON lobbying_lobbyists(lobbyist_name);
CREATE INDEX IF NOT EXISTS idx_lobby_lob_covered ON lobbying_lobbyists(covered_position) WHERE covered_position IS NOT NULL AND covered_position != '';
CREATE INDEX IF NOT EXISTS idx_lobby_contrib_year ON lobbying_contributions(filing_year);
CREATE INDEX IF NOT EXISTS idx_lobby_contrib_recipient ON lobbying_contributions(recipient_name);

-- Lobbying-to-legislation cross-reference (bill refs extracted from specific_issues text)
CREATE TABLE IF NOT EXISTS lobbying_bills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_uuid TEXT NOT NULL,
    bill_congress INTEGER,
    bill_type TEXT,
    bill_number INTEGER,
    bill_id TEXT,
    client_name TEXT,
    issue_code TEXT
);
CREATE INDEX IF NOT EXISTS idx_lb_bill ON lobbying_bills(bill_id);
CREATE INDEX IF NOT EXISTS idx_lb_filing ON lobbying_bills(filing_uuid);
CREATE INDEX IF NOT EXISTS idx_lb_client ON lobbying_bills(client_name);

-- Committee-sector reference (maps committees to regulated industries and tickers)
CREATE TABLE IF NOT EXISTS committee_sectors (
    committee_id TEXT NOT NULL,
    committee_name TEXT,
    chamber TEXT,
    regulated_sectors TEXT,
    example_tickers TEXT,
    gics_sectors TEXT,
    sic_ranges TEXT,
    notes TEXT,
    PRIMARY KEY (committee_id)
);

-- Committee jurisdiction with tiered SIC mapping (primary/secondary/tertiary)
CREATE TABLE IF NOT EXISTS committee_jurisdiction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    committee_id TEXT NOT NULL,
    committee_name TEXT,
    chamber TEXT,
    jurisdiction_desc TEXT,
    sic_codes TEXT,
    jurisdiction_tier TEXT DEFAULT 'primary',
    notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_cj_committee ON committee_jurisdiction(committee_id);
CREATE INDEX IF NOT EXISTS idx_cj_tier ON committee_jurisdiction(jurisdiction_tier);

-- Expanded SIC ranges from committee_jurisdiction (for efficient range matching)
CREATE TABLE IF NOT EXISTS committee_sic_ranges (
    committee_id TEXT NOT NULL,
    sic_start INTEGER NOT NULL,
    sic_end INTEGER NOT NULL,
    jurisdiction_tier TEXT DEFAULT 'primary'
);
CREATE INDEX IF NOT EXISTS idx_csr_sic ON committee_sic_ranges(sic_start, sic_end);
CREATE INDEX IF NOT EXISTS idx_csr_committee ON committee_sic_ranges(committee_id);

-- Ticker-to-SIC mapping from SEC EDGAR
CREATE TABLE IF NOT EXISTS ticker_sic (
    ticker TEXT PRIMARY KEY,
    cik TEXT,
    company_name TEXT,
    sic_code TEXT,
    sic_description TEXT,
    exchange TEXT
);
CREATE INDEX IF NOT EXISTS idx_ts_sic ON ticker_sic(sic_code);

-- Lobbying issue-to-agency reference (maps lobbying issue codes to agencies and committees)
CREATE TABLE IF NOT EXISTS lobbying_issue_agencies (
    issue_code TEXT NOT NULL,
    issue_description TEXT,
    primary_agencies TEXT,
    secondary_agencies TEXT,
    regs_gov_agency_prefixes TEXT,
    federal_register_agency_slugs TEXT,
    related_committee_ids TEXT,
    notes TEXT,
    PRIMARY KEY (issue_code)
);

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
    cik INTEGER,              -- SEC EDGAR CIK, populated from ticker_sic+sec_companies
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
CREATE INDEX IF NOT EXISTS idx_st_cik ON stock_trades(cik) WHERE cik IS NOT NULL;
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

-- CBO cost estimates (extracted from Congress.gov bill data)
CREATE TABLE IF NOT EXISTS cbo_cost_estimates (
    bill_id TEXT NOT NULL,
    pub_date TEXT,
    title TEXT,
    description TEXT,
    url TEXT,
    FOREIGN KEY (bill_id) REFERENCES legislation(bill_id)
);
CREATE INDEX IF NOT EXISTS idx_cbo_bill ON cbo_cost_estimates(bill_id);

-- Committee hearings (from GovInfo CHRG collection)
CREATE TABLE IF NOT EXISTS hearings (
    package_id TEXT PRIMARY KEY,
    title TEXT,
    chamber TEXT,
    congress INTEGER,
    session TEXT,
    date_issued TEXT,
    committees TEXT,
    detail_url TEXT,
    html_url TEXT,
    pdf_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_hear_chamber ON hearings(chamber);
CREATE INDEX IF NOT EXISTS idx_hear_congress ON hearings(congress);
CREATE INDEX IF NOT EXISTS idx_hear_date ON hearings(date_issued);

-- Hearing witnesses (normalized from hearings JSON)
CREATE TABLE IF NOT EXISTS hearing_witnesses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    package_id TEXT NOT NULL REFERENCES hearings(package_id),
    name TEXT,
    title TEXT,
    organization TEXT,
    organization_normalized TEXT,
    location TEXT
);

CREATE INDEX IF NOT EXISTS idx_hw_package ON hearing_witnesses(package_id);
CREATE INDEX IF NOT EXISTS idx_hw_name ON hearing_witnesses(name);
CREATE INDEX IF NOT EXISTS idx_hw_org ON hearing_witnesses(organization) WHERE organization IS NOT NULL AND organization != '';
CREATE INDEX IF NOT EXISTS idx_hw_org_norm ON hearing_witnesses(organization_normalized) WHERE organization_normalized IS NOT NULL;

-- Hearing members (committee members present)
CREATE TABLE IF NOT EXISTS hearing_members (
    package_id TEXT NOT NULL REFERENCES hearings(package_id),
    name TEXT NOT NULL,
    role TEXT,
    bioguide_id TEXT,
    PRIMARY KEY (package_id, name)
);

CREATE INDEX IF NOT EXISTS idx_hm_name ON hearing_members(name);
CREATE INDEX IF NOT EXISTS idx_hm_bioguide ON hearing_members(bioguide_id);

-- CRS reports (from Congress.gov API)
CREATE TABLE IF NOT EXISTS crs_reports (
    id TEXT PRIMARY KEY,
    title TEXT,
    publish_date TEXT,
    update_date TEXT,
    status TEXT,
    content_type TEXT,
    authors TEXT,
    topics TEXT,
    summary TEXT,
    pdf_url TEXT,
    html_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_crs_date ON crs_reports(publish_date);
CREATE INDEX IF NOT EXISTS idx_crs_status ON crs_reports(status);

-- CRS report related bills
CREATE TABLE IF NOT EXISTS crs_report_bills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id TEXT NOT NULL REFERENCES crs_reports(id),
    bill_title TEXT,
    congress INTEGER,
    bill_type TEXT,
    bill_number INTEGER,
    bill_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_crb_report ON crs_report_bills(report_id);
CREATE INDEX IF NOT EXISTS idx_crb_bill ON crs_report_bills(bill_id);

-- Executive nominations (from Congress.gov API)
CREATE TABLE IF NOT EXISTS nominations (
    id TEXT PRIMARY KEY,
    congress INTEGER,
    number INTEGER,
    part_number TEXT,
    citation TEXT,
    description TEXT,
    organization TEXT,
    received_date TEXT,
    authority_date TEXT,
    is_civilian INTEGER,
    is_military INTEGER,
    status TEXT,
    vote_yea INTEGER,
    vote_nay INTEGER
);

CREATE INDEX IF NOT EXISTS idx_nom_congress ON nominations(congress);
CREATE INDEX IF NOT EXISTS idx_nom_date ON nominations(received_date);
CREATE INDEX IF NOT EXISTS idx_nom_org ON nominations(organization) WHERE organization IS NOT NULL AND organization != '';
CREATE INDEX IF NOT EXISTS idx_nom_status_date ON nominations(status, received_date DESC);
CREATE INDEX IF NOT EXISTS idx_nom_civ_status ON nominations(is_civilian, status, received_date DESC);

-- Nomination actions
CREATE TABLE IF NOT EXISTS nomination_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nomination_id TEXT NOT NULL REFERENCES nominations(id),
    action_date TEXT,
    action_text TEXT,
    action_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_nomact_nom ON nomination_actions(nomination_id);

-- Treaties (from Congress.gov API)
CREATE TABLE IF NOT EXISTS treaties (
    id TEXT PRIMARY KEY,
    congress INTEGER,
    number INTEGER,
    title TEXT,
    topic TEXT,
    transmitted_date TEXT,
    in_force_date TEXT,
    countries TEXT,
    index_terms TEXT,
    resolution_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_treaty_congress ON treaties(congress);
CREATE INDEX IF NOT EXISTS idx_treaty_date ON treaties(transmitted_date);

-- Treaty actions
CREATE TABLE IF NOT EXISTS treaty_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    treaty_id TEXT NOT NULL REFERENCES treaties(id),
    action_date TEXT,
    action_text TEXT,
    action_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_tact_treaty ON treaty_actions(treaty_id);

-- GAO reports (from GovInfo GAOREPORTS collection)
CREATE TABLE IF NOT EXISTS gao_reports (
    package_id TEXT PRIMARY KEY,
    title TEXT,
    date_issued TEXT,
    report_number TEXT,
    document_type TEXT,
    doc_class TEXT,
    abstract TEXT,
    subjects TEXT,
    public_laws TEXT,
    usc_references TEXT,
    statute_references TEXT,
    citation TEXT,
    pdf_url TEXT,
    html_url TEXT,
    detail_url TEXT,
    sudocs TEXT
);

CREATE INDEX IF NOT EXISTS idx_gao_date ON gao_reports(date_issued);
CREATE INDEX IF NOT EXISTS idx_gao_type ON gao_reports(document_type);
CREATE INDEX IF NOT EXISTS idx_gao_report_num ON gao_reports(report_number);

-- Inspector General reports (from oversight.gov)
CREATE TABLE IF NOT EXISTS ig_reports (
    report_id TEXT PRIMARY KEY,
    title TEXT,
    date_issued TEXT,
    report_number TEXT,
    report_type TEXT,
    agency_reviewed TEXT,
    submitting_oig TEXT,
    location TEXT,
    description TEXT,
    num_recommendations INTEGER DEFAULT 0,
    questioned_costs INTEGER DEFAULT 0,
    funds_for_better_use INTEGER DEFAULT 0,
    pdf_url TEXT,
    detail_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_ig_date ON ig_reports(date_issued);
CREATE INDEX IF NOT EXISTS idx_ig_agency ON ig_reports(agency_reviewed);
CREATE INDEX IF NOT EXISTS idx_ig_type ON ig_reports(report_type);
CREATE INDEX IF NOT EXISTS idx_ig_oig ON ig_reports(submitting_oig);

CREATE TABLE IF NOT EXISTS ig_recommendations (
    report_id TEXT REFERENCES ig_reports(report_id),
    rec_number TEXT,
    significant TEXT,
    text TEXT,
    questioned_costs INTEGER DEFAULT 0,
    funds_for_better_use INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ig_rec_report ON ig_recommendations(report_id);
CREATE INDEX IF NOT EXISTS idx_ig_rec_sig ON ig_recommendations(significant);

-- Earmarks / Congressionally Directed Spending
CREATE TABLE IF NOT EXISTS earmarks (
    id INTEGER PRIMARY KEY,
    fiscal_year INTEGER NOT NULL,
    chamber TEXT NOT NULL,
    member_name TEXT,
    member_last TEXT,
    member_first TEXT,
    bioguide_id TEXT,
    party TEXT,
    state TEXT,
    district TEXT,
    subcommittee TEXT,
    recipient TEXT,
    recipient_normalized TEXT,
    project_description TEXT,
    recipient_address TEXT,
    amount_requested INTEGER
);

CREATE INDEX IF NOT EXISTS idx_earmarks_fy ON earmarks(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_earmarks_chamber ON earmarks(chamber);
CREATE INDEX IF NOT EXISTS idx_earmarks_bioguide ON earmarks(bioguide_id);
CREATE INDEX IF NOT EXISTS idx_earmarks_state ON earmarks(state);
CREATE INDEX IF NOT EXISTS idx_earmarks_amount ON earmarks(amount_requested);
CREATE INDEX IF NOT EXISTS idx_earmarks_recip_norm ON earmarks(recipient_normalized);

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

-- FEC: operating expenditures (PAC/party/candidate disbursements at payee level)
CREATE TABLE IF NOT EXISTS fec_operating_expenditures (
    cmte_id TEXT,
    cmte_name TEXT,
    form_tp_cd TEXT,
    name TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    transaction_dt TEXT,
    transaction_amt REAL,
    purpose TEXT,
    category TEXT,
    category_desc TEXT,
    entity_tp TEXT,
    memo_cd TEXT,
    memo_text TEXT,
    cycle INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fec_oppexp_cmte ON fec_operating_expenditures(cmte_id);
CREATE INDEX IF NOT EXISTS idx_fec_oppexp_name ON fec_operating_expenditures(name);
CREATE INDEX IF NOT EXISTS idx_fec_oppexp_amt ON fec_operating_expenditures(transaction_amt);
CREATE INDEX IF NOT EXISTS idx_fec_oppexp_date ON fec_operating_expenditures(transaction_dt);
CREATE INDEX IF NOT EXISTS idx_fec_oppexp_cycle ON fec_operating_expenditures(cycle);
CREATE INDEX IF NOT EXISTS idx_fec_oppexp_purpose ON fec_operating_expenditures(purpose);
CREATE INDEX IF NOT EXISTS idx_fec_oppexp_entity ON fec_operating_expenditures(entity_tp);
CREATE INDEX IF NOT EXISTS idx_fec_oppexp_form ON fec_operating_expenditures(form_tp_cd);

-- FEC: PAC & party summary (one row per committee per cycle)
CREATE TABLE IF NOT EXISTS fec_pac_summary (
    cmte_id TEXT NOT NULL,
    cmte_nm TEXT,
    cmte_tp TEXT,
    cmte_dsgn TEXT,
    ttl_receipts REAL,
    indv_contrib REAL,
    other_pol_cmte_contrib REAL,
    ttl_disb REAL,
    contrib_to_other_cmte REAL,
    ind_exp REAL,
    pty_coord_exp REAL,
    coh_cop REAL,
    debts_owed_by REAL,
    cvg_end_dt TEXT,
    cycle INTEGER NOT NULL,
    PRIMARY KEY (cmte_id, cycle)
);

CREATE INDEX IF NOT EXISTS idx_fec_pac_summary_name ON fec_pac_summary(cmte_nm);
CREATE INDEX IF NOT EXISTS idx_fec_pac_summary_type ON fec_pac_summary(cmte_tp);
CREATE INDEX IF NOT EXISTS idx_fec_pac_summary_disb ON fec_pac_summary(ttl_disb);
CREATE INDEX IF NOT EXISTS idx_fec_pac_summary_ie ON fec_pac_summary(ind_exp);

-- FEC: independent expenditures (enriched with support/oppose)
CREATE TABLE IF NOT EXISTS fec_independent_expenditures (
    cand_id TEXT,
    cand_name TEXT,
    spe_id TEXT,
    spe_nam TEXT,
    can_office TEXT,
    can_office_state TEXT,
    cand_pty_aff TEXT,
    exp_amo REAL,
    exp_date TEXT,
    agg_amo REAL,
    sup_opp TEXT,
    pur TEXT,
    pay TEXT,
    dissem_dt TEXT,
    fec_election_yr INTEGER,
    cycle INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fec_ie_cand ON fec_independent_expenditures(cand_id);
CREATE INDEX IF NOT EXISTS idx_fec_ie_spender ON fec_independent_expenditures(spe_id);
CREATE INDEX IF NOT EXISTS idx_fec_ie_supopp ON fec_independent_expenditures(sup_opp);
CREATE INDEX IF NOT EXISTS idx_fec_ie_amt ON fec_independent_expenditures(exp_amo);
CREATE INDEX IF NOT EXISTS idx_fec_ie_date ON fec_independent_expenditures(exp_date);
CREATE INDEX IF NOT EXISTS idx_fec_ie_cycle ON fec_independent_expenditures(cycle);

-- FEC: electioneering communications
CREATE TABLE IF NOT EXISTS fec_electioneering (
    cmte_id TEXT,
    cmte_nm TEXT,
    cand_id TEXT,
    cand_name TEXT,
    cand_office TEXT,
    disb_amt REAL,
    disb_dt TEXT,
    comm_dt TEXT,
    payee_name TEXT,
    cycle INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fec_elec_cand ON fec_electioneering(cand_id);
CREATE INDEX IF NOT EXISTS idx_fec_elec_cmte ON fec_electioneering(cmte_id);

-- FEC: communication costs
CREATE TABLE IF NOT EXISTS fec_communication_costs (
    cmte_id TEXT,
    cmte_nm TEXT,
    cand_id TEXT,
    cand_name TEXT,
    cand_office TEXT,
    transaction_dt TEXT,
    transaction_amt REAL,
    communication_tp TEXT,
    purpose TEXT,
    sup_opp TEXT,
    cycle INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fec_commcost_cand ON fec_communication_costs(cand_id);
CREATE INDEX IF NOT EXISTS idx_fec_commcost_cmte ON fec_communication_costs(cmte_id);
CREATE INDEX IF NOT EXISTS idx_fec_commcost_supopp ON fec_communication_costs(sup_opp);

-- OIRA: EO 12866 regulatory reviews
CREATE TABLE IF NOT EXISTS oira_reviews (
    rin TEXT,
    agency_code TEXT,
    title TEXT,
    stage TEXT,
    economically_significant TEXT,
    date_received TEXT,
    date_completed TEXT,
    decision TEXT,
    date_published TEXT,
    major TEXT
);

CREATE INDEX IF NOT EXISTS idx_oira_reviews_rin ON oira_reviews(rin);
CREATE INDEX IF NOT EXISTS idx_oira_reviews_agency ON oira_reviews(agency_code);
CREATE INDEX IF NOT EXISTS idx_oira_reviews_date ON oira_reviews(date_received);
CREATE INDEX IF NOT EXISTS idx_oira_reviews_decision ON oira_reviews(decision);

-- OIRA: EO 12866 meetings with outside parties
CREATE TABLE IF NOT EXISTS oira_meetings (
    meeting_id TEXT PRIMARY KEY,
    rin TEXT,
    title TEXT,
    agency_acronym TEXT,
    rule_stage TEXT,
    meeting_date TEXT,
    requestor_org TEXT,
    requestor_name TEXT,
    meeting_type TEXT,
    type_cd TEXT,
    source TEXT
);

CREATE INDEX IF NOT EXISTS idx_oira_meetings_rin ON oira_meetings(rin);
CREATE INDEX IF NOT EXISTS idx_oira_meetings_date ON oira_meetings(meeting_date);
CREATE INDEX IF NOT EXISTS idx_oira_meetings_org ON oira_meetings(requestor_org);

-- OIRA: meeting attendees
CREATE TABLE IF NOT EXISTS oira_meeting_attendees (
    meeting_id TEXT REFERENCES oira_meetings(meeting_id),
    attendee_name TEXT,
    attendee_org TEXT,
    participation_type TEXT,
    is_government INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_oira_attendees_mid ON oira_meeting_attendees(meeting_id);
CREATE INDEX IF NOT EXISTS idx_oira_attendees_org ON oira_meeting_attendees(attendee_org);

-- FARA (Foreign Agents Registration Act)
CREATE TABLE IF NOT EXISTS fara_registrants (
    registration_number TEXT PRIMARY KEY,
    registration_date TEXT,
    termination_date TEXT,
    name TEXT,
    name_normalized TEXT,
    business_name TEXT,
    address_1 TEXT,
    address_2 TEXT,
    city TEXT,
    state TEXT,
    zip TEXT
);
CREATE INDEX IF NOT EXISTS idx_fara_reg_name ON fara_registrants(name);
CREATE INDEX IF NOT EXISTS idx_fara_reg_name_norm ON fara_registrants(name_normalized);

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

-- ==========================================================================
-- Entity resolution tables (entities / public_actors / entity_relationships)
-- Architecture: /mnt/data/datadawn/bestpractices/entity_architecture.md
-- Policy:       /mnt/data/datadawn/bestpractices/data_sourcing_policy.md
-- All-government-data only. Matchers built in-house from primary sources.
-- ==========================================================================

-- entities: organizations only (corporations, nonprofits, PACs, foreign).
-- Not people. Not governments. The FK target for org-carrying source rows.
CREATE TABLE IF NOT EXISTS entities (
    entity_id INTEGER PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    display_name TEXT,                   -- lazy; NULL falls back to canonical_name
    entity_type TEXT,                    -- 'public_company' | 'private_company' | 'nonprofit'
                                         -- | 'foreign_entity' | 'pac' | 'trade_association'
                                         -- | 'union' | 'political_committee' | 'unknown'
    ein TEXT,                            -- IRS (9 digits)
    cik INTEGER,                         -- SEC Central Index Key
    uei TEXT,                            -- SAM.gov Unique Entity Identifier (12 chars)
    lei TEXT,                            -- GLEIF Legal Entity Identifier (20 chars)
    ticker TEXT,                         -- SEC primary ticker, UPPERCASE
    fec_committee_id TEXT,               -- FEC 9-char C00XXXXXX (for PACs/parties)
    duns TEXT,                           -- legacy D&B; federal-adopted subset only
    sic_code TEXT,
    naics_code TEXT,
    ntee_code TEXT,
    state_of_incorporation TEXT,
    primary_state TEXT,                  -- 2-letter USPS (country='US') or ISO-3166-2 subdivision code
                                         -- without country prefix; country lives in country col
    country TEXT,                        -- 2-letter ISO code
    status TEXT,                         -- 'active' | 'merged' | 'defunct' | 'unknown'
    merged_into_entity_id INTEGER,
    source_provenance TEXT,              -- comma-separated set from {'sec', 'bmf', 'gleif',
                                         -- 'fec', 'sam', 'manual'}; tracks which sources
                                         -- contributed to this entity. Unordered set semantics.
    -- Historical names preserved for attribution. JSON array:
    --   [{"name": "Wyeth LLC", "until": "2009-10-15", "reason": "acquired_by_pfizer"},
    --    {"name": "American Home Products", "until": "2002-03-11", "reason": "rebranded"}]
    predecessor_names TEXT,              -- JSON array (added 2026-04-19 per acronym_curation doc)
    -- Industry taxonomy tags (see bestpractices/datadawn_industry_taxonomy.md).
    -- Rule: industry = policy area affected, not corporate-structural classification.
    -- Foreign parents, advocacy orgs, think tanks tagged by US policy area they influence.
    industry_codes TEXT,                 -- JSON array of industry slugs: ["pharmaceuticals", "big_tech"]
    primary_industry TEXT,               -- single most-representative slug (for default queries)
    first_seen_date TEXT,
    last_seen_date TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    notes TEXT,
    -- Canonical DataDawn identifier (virtual — no storage). Format: DD-E-{entity_id}.
    -- See entity_architecture.md §"Future: canonical DataDawn identifiers (DD-IDs)".
    dd_id TEXT GENERATED ALWAYS AS ('DD-E-' || entity_id) VIRTUAL,
    FOREIGN KEY (merged_into_entity_id) REFERENCES entities(entity_id),
    CHECK (merged_into_entity_id IS NULL OR merged_into_entity_id != entity_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_ein ON entities(ein) WHERE ein IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_cik ON entities(cik) WHERE cik IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_uei ON entities(uei) WHERE uei IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_lei ON entities(lei) WHERE lei IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_fec_cmte ON entities(fec_committee_id) WHERE fec_committee_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entity_name_norm ON entities(name_normalized);
CREATE INDEX IF NOT EXISTS idx_entity_name_state ON entities(name_normalized, primary_state);
CREATE INDEX IF NOT EXISTS idx_entity_ticker ON entities(ticker) WHERE ticker IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_entity_type ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entity_provenance ON entities(source_provenance) WHERE source_provenance IS NOT NULL;

-- entity_aliases: preserves source-specific name variants for reverse lookup.
CREATE TABLE IF NOT EXISTS entity_aliases (
    entity_id INTEGER NOT NULL,
    alias_name TEXT NOT NULL,
    alias_normalized TEXT NOT NULL,
    alias_source TEXT NOT NULL,          -- '990_filer' | 'sec_former_name' | 'lobbying_client'
                                         -- | 'fara_registrant' | 'sam_legal_name' | 'manual'
    effective_start TEXT,
    effective_end TEXT,
    FOREIGN KEY (entity_id) REFERENCES entities(entity_id),
    PRIMARY KEY (entity_id, alias_name, alias_source)
);
CREATE INDEX IF NOT EXISTS idx_alias_norm ON entity_aliases(alias_normalized);
CREATE INDEX IF NOT EXISTS idx_alias_norm_src ON entity_aliases(alias_normalized, alias_source);

-- public_actors: named individuals in public-role contexts, Model 2.
-- One row per (person, role-context); never fuzzy-merged across sources.
-- source_id format for composite keys: '{id_part}:{normalized_name}' using a
-- single colon. normalize_name() strips all non-alphanumeric+space, so colons
-- never appear in the name part. See entity_architecture.md §"Table: public_actors".
CREATE TABLE IF NOT EXISTS public_actors (
    public_actor_id INTEGER PRIMARY KEY,
    role_context TEXT NOT NULL,          -- 'congress_member' | 'fara_agent' | 'fec_candidate'
                                         -- | 'oge_pas_filer' | 'sec_form4_reporter'
                                         -- | 'nonprofit_officer' | 'lobbyist_lda'
    source_id TEXT NOT NULL,             -- '{id_part}:{normalized_name}' or bare id — see doc
    name_full TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    name_first TEXT,
    name_last TEXT,

    -- Canonical IDs (populated where applicable)
    bioguide_id TEXT,
    fec_cand_id TEXT,
    fara_registration_number TEXT,
    oge_filer_slug TEXT,
    sec_reporter_cik INTEGER,

    -- Employer/organization
    employer_entity_id INTEGER,
    employer_entity_name TEXT,
    employer_ein TEXT,
    employer_cik INTEGER,
    employer_lda_registrant_id INTEGER,

    -- Role details
    role_title TEXT,
    role_start_date TEXT,
    role_end_date TEXT,
    is_current INTEGER,

    -- Materiality (two-pass: Pass 1 sets defaults; Pass 2 refines)
    materiality_tier INTEGER NOT NULL DEFAULT 2,
    materiality_reason TEXT,

    -- Merge tracking (Model 2 — deterministic evidence only)
    merged_into_public_actor_id INTEGER,
    merge_method TEXT,                   -- 'bioguide_match' | 'fec_crosswalk' | 'cik_match' | 'manual'
    merge_confidence REAL,

    -- Provenance
    first_seen_date TEXT,
    last_seen_date TEXT,
    source_table TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    notes TEXT,

    -- Canonical DataDawn identifier (virtual). Format: DD-P-{public_actor_id}.
    dd_id TEXT GENERATED ALWAYS AS ('DD-P-' || public_actor_id) VIRTUAL,

    UNIQUE(role_context, source_id),
    FOREIGN KEY (employer_entity_id) REFERENCES entities(entity_id),
    FOREIGN KEY (merged_into_public_actor_id) REFERENCES public_actors(public_actor_id),
    CHECK (merged_into_public_actor_id IS NULL OR merged_into_public_actor_id != public_actor_id)
);

CREATE INDEX IF NOT EXISTS idx_pa_name_norm ON public_actors(name_normalized);
CREATE INDEX IF NOT EXISTS idx_pa_role_context ON public_actors(role_context);
CREATE INDEX IF NOT EXISTS idx_pa_bioguide ON public_actors(bioguide_id) WHERE bioguide_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pa_fec_cand ON public_actors(fec_cand_id) WHERE fec_cand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pa_fara ON public_actors(fara_registration_number) WHERE fara_registration_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pa_employer ON public_actors(employer_entity_id) WHERE employer_entity_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pa_merged ON public_actors(merged_into_public_actor_id) WHERE merged_into_public_actor_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pa_tier ON public_actors(materiality_tier);

-- entity_relationships: parent/subsidiary/merger/successor/affiliate graph.
-- Unified table across all source systems.
CREATE TABLE IF NOT EXISTS entity_relationships (
    relationship_id INTEGER PRIMARY KEY,
    parent_entity_id INTEGER,            -- NULL when parent is unknown (e.g., BMF GEN with no resolved central)
    child_entity_id INTEGER NOT NULL,
    relationship_type TEXT NOT NULL,     -- 'subsidiary_of' | 'parent_of' | 'affiliated_pac'
                                         -- | 'founder_family' | 'foreign_controlled' | 'joint_venture'
                                         -- | 'predecessor_to' | 'successor_to'
                                         -- | 'bmf_group_exemption' | 'lobbying_affiliate'
    source TEXT NOT NULL,                -- 'bmf' | 'sec_exhibit_21' | 'lobbying_affiliated_orgs'
                                         -- | 'gleif_successor' | 'fec_connected_org' | 'manual'
    source_ref TEXT,                     -- filing_uuid / accession_number / GEN / etc.
    gen TEXT,                            -- IRS Group Exemption Number when source='bmf'
    effective_start TEXT,
    effective_end TEXT,
    confidence_score REAL NOT NULL DEFAULT 1.0,
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (parent_entity_id) REFERENCES entities(entity_id),
    FOREIGN KEY (child_entity_id) REFERENCES entities(entity_id),
    CHECK (parent_entity_id IS NULL OR parent_entity_id != child_entity_id)
);

CREATE INDEX IF NOT EXISTS idx_er_parent ON entity_relationships(parent_entity_id) WHERE parent_entity_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_er_child ON entity_relationships(child_entity_id);
CREATE INDEX IF NOT EXISTS idx_er_type ON entity_relationships(relationship_type);
CREATE INDEX IF NOT EXISTS idx_er_gen ON entity_relationships(gen) WHERE gen IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_er_source ON entity_relationships(source);
"""

VIEWS = """
-- NOTE: docket_stats, comments_by_year, fr_by_year, top_dockets, comments_monthly,
-- speaker_activity, bills_floor_time, lobbying_by_year, top_lobbying_clients, spending_by_agency
-- are now MATERIALIZED as tables in materialize_slow_views() for performance.
-- Their SQL is preserved there. Do NOT recreate them as views here.

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

-- Nomination confirmation rates by congress
CREATE VIEW IF NOT EXISTS nomination_rates AS
SELECT
    congress,
    COUNT(*) AS total,
    SUM(CASE WHEN status = 'Confirmed' THEN 1 ELSE 0 END) AS confirmed,
    SUM(CASE WHEN status = 'Withdrawn' THEN 1 ELSE 0 END) AS withdrawn,
    SUM(CASE WHEN status = 'Returned' THEN 1 ELSE 0 END) AS returned,
    SUM(CASE WHEN status = 'Rejected' THEN 1 ELSE 0 END) AS rejected,
    SUM(CASE WHEN status = 'Pending' THEN 1 ELSE 0 END) AS pending,
    ROUND(100.0 * SUM(CASE WHEN status = 'Confirmed' THEN 1 ELSE 0 END) / COUNT(*), 1) AS confirm_pct
FROM nominations
GROUP BY congress
ORDER BY congress;

-- Hearing activity by congress and chamber
CREATE VIEW IF NOT EXISTS hearing_activity AS
SELECT
    congress,
    chamber,
    COUNT(*) AS hearing_count,
    COUNT(DISTINCT hw.name) AS unique_witnesses,
    MIN(h.date_issued) AS earliest,
    MAX(h.date_issued) AS latest
FROM hearings h
LEFT JOIN hearing_witnesses hw ON hw.package_id = h.package_id
GROUP BY congress, chamber
ORDER BY congress DESC, chamber;

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

-- Full-text search on hearings
CREATE VIRTUAL TABLE IF NOT EXISTS hearings_fts USING fts5(
    title, chamber, committees,
    content='hearings', content_rowid='rowid'
);

-- Full-text search on CRS reports
CREATE VIRTUAL TABLE IF NOT EXISTS crs_reports_fts USING fts5(
    title, authors, topics, summary,
    content='crs_reports', content_rowid='rowid'
);

-- Full-text search on nominations
CREATE VIRTUAL TABLE IF NOT EXISTS nominations_fts USING fts5(
    description, organization, citation, status,
    content='nominations', content_rowid='rowid'
);

-- Full-text search on GAO reports
CREATE VIRTUAL TABLE IF NOT EXISTS gao_reports_fts USING fts5(
    title, abstract, subjects, report_number,
    content='gao_reports', content_rowid='rowid'
);

-- Full-text search on earmarks
CREATE VIRTUAL TABLE IF NOT EXISTS earmarks_fts USING fts5(
    recipient, project_description, member_name, recipient_address,
    content='earmarks', content_rowid='id'
);

-- Staging imports — FTS on entity resolution + disbursements + WH visits + OGE PAS.
-- All content-linked so they rebuild cleanly from source tables.
CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(
    canonical_name, display_name, ticker, ein, cik, uei, lei,
    content='entities', content_rowid='entity_id'
);
CREATE VIRTUAL TABLE IF NOT EXISTS entity_aliases_fts USING fts5(
    alias_name, alias_source,
    content='entity_aliases', content_rowid='rowid'
);
CREATE VIRTUAL TABLE IF NOT EXISTS public_actors_fts USING fts5(
    name_full, role_context, role_title, bioguide_id, fec_cand_id,
    employer_entity_name,
    content='public_actors', content_rowid='public_actor_id'
);
CREATE VIRTUAL TABLE IF NOT EXISTS senate_office_totals_fts USING fts5(
    office_name, line_category, report_id,
    content='senate_office_totals', content_rowid='id'
);
CREATE VIRTUAL TABLE IF NOT EXISTS house_pre2016_office_totals_fts USING fts5(
    office_name, category, quarter,
    content='house_pre2016_office_totals', content_rowid='id'
);
CREATE VIRTUAL TABLE IF NOT EXISTS house_disbursements_nonpersonnel_fts USING fts5(
    organization, program, vendor_name, description,
    content='house_disbursements_nonpersonnel', content_rowid='id'
);
CREATE VIRTUAL TABLE IF NOT EXISTS wh_visits_fts USING fts5(
    visitor_name_full, visitee_name_last, visitee_name_first,
    visitee_full, meeting_loc, meeting_room, description,
    content='wh_visits', content_rowid='visit_id'
);
CREATE VIRTUAL TABLE IF NOT EXISTS oge_pas_filings_fts USING fts5(
    filer_name_full, department, position, doc_type,
    content='oge_pas_filings', content_rowid='filing_id'
);
CREATE VIRTUAL TABLE IF NOT EXISTS oge_pas_transactions_fts USING fts5(
    filer_last, filer_first, asset_description, asset_name, ticker,
    tx_type, department, position,
    content='oge_pas_transactions', content_rowid='transaction_id'
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
                     pub_year, pub_month, html_url, pdf_url, agency_names, agency_ids, excerpts,
                     regulation_id_numbers)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ", ".join(doc.get("regulation_id_numbers") or []) or None,
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

    enrich_regulation_id_numbers(conn)
    return count


def enrich_regulation_id_numbers(conn: sqlite3.Connection):
    """Backfill regulation_id_numbers from the FR API for rows that are still NULL.

    Only fetches Rules and Proposed Rules (the document types that carry RINs).
    Uses minimal fields to keep bandwidth low. Idempotent — skips if already populated.
    """
    null_count = conn.execute(
        "SELECT COUNT(*) FROM federal_register "
        "WHERE regulation_id_numbers IS NULL AND type IN ('Rule', 'Proposed Rule')"
    ).fetchone()[0]
    if null_count == 0:
        log.info("  RIN enrichment: all Rules/Proposed Rules already have regulation_id_numbers")
        return

    log.info(f"  RIN enrichment: {null_count:,} Rules/Proposed Rules missing regulation_id_numbers, fetching from FR API...")

    import requests
    session = requests.Session()
    session.headers["User-Agent"] = "DataDawn/1.0 (data@datadawn.org)"

    updated = 0
    api_types = {"RULE": "Rule", "PRORULE": "Proposed Rule"}

    # Query year-by-year to avoid the FR API's 10,000-result cap per query.
    min_year = conn.execute(
        "SELECT MIN(pub_year) FROM federal_register WHERE type IN ('Rule', 'Proposed Rule')"
    ).fetchone()[0] or 1994
    max_year = conn.execute(
        "SELECT MAX(pub_year) FROM federal_register WHERE type IN ('Rule', 'Proposed Rule')"
    ).fetchone()[0] or 2026

    for year in range(min_year, max_year + 1):
        for api_type, label in api_types.items():
            page = 1
            while True:
                try:
                    resp = session.get(
                        "https://www.federalregister.gov/api/v1/documents.json",
                        params={
                            "per_page": 1000,
                            "page": page,
                            "conditions[type]": api_type,
                            "conditions[publication_date][gte]": f"{year}-01-01",
                            "conditions[publication_date][lte]": f"{year}-12-31",
                            "fields[]": ["document_number", "regulation_id_numbers"],
                            "order": "oldest",
                        },
                        timeout=60,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    log.warning(f"  RIN enrichment: API error on {year} page {page} ({label}): {e}")
                    break

                results = data.get("results", [])
                if not results:
                    break

                for doc in results:
                    rins = doc.get("regulation_id_numbers") or []
                    if rins:
                        rin_str = ", ".join(rins)
                        cur = conn.execute(
                            "UPDATE federal_register SET regulation_id_numbers = ? "
                            "WHERE document_number = ? AND regulation_id_numbers IS NULL",
                            (rin_str, doc["document_number"]),
                        )
                        updated += cur.rowcount

                total_pages = data.get("total_pages", 1)
                if page >= total_pages:
                    break
                page += 1
                time.sleep(0.3)
        conn.commit()
        if updated and year % 5 == 0:
            log.info(f"  RIN enrichment: through {year}, {updated:,} updated so far")

    conn.commit()
    log.info(f"  RIN enrichment: {updated:,} documents updated with regulation_id_numbers")


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

    # Scan both old headers dir and expansion dir for comment pages
    comment_sources = [COMMENTS_DIR]
    # Add expansion agency dirs (skip 'headers' and 'details' subdirs)
    if COMMENTS_EXPANSION_DIR.exists():
        for entry in sorted(COMMENTS_EXPANSION_DIR.iterdir()):
            if entry.is_dir() and entry.name not in ("headers", "details"):
                comment_sources.append(entry)

    log.info(f"  Scanning {len(comment_sources)} comment source directories...")

    for source_dir in comment_sources:
        for json_file, data in iter_json_pages(source_dir):
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

    batch_files = sorted(DETAILS_DIR.glob("batch_*.json")) + sorted(DETAILS_DIR.glob("rev_*.json"))
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

    conn.execute("UPDATE comment_details SET organization_normalized = UPPER(TRIM(organization)) WHERE organization IS NOT NULL AND organization_normalized IS NULL")
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
            except requests.RequestException as e:
                raise RuntimeError(
                    f"Federal Register API fetch failed for {doc_type} page {page}: {e}. "
                    f"Aborting build rather than silently producing partial data."
                ) from e

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
                     recipient_uei, recipient_duns, recipient_id,
                     recipient_location_state,
                     award_amount, total_outlays, description,
                     start_date, end_date, fiscal_year,
                     state_code, cfda_number, naics_code, naics_description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    internal_id,
                    rec.get("Award ID"),
                    rec.get("Awarding Agency") or rec.get("_agency"),
                    rec.get("Awarding Sub Agency"),
                    rec.get("_award_category"),
                    rec.get("Award Type") or rec.get("Contract Award Type"),
                    rec.get("Recipient Name"),
                    rec.get("Recipient UEI"),
                    rec.get("Recipient DUNS"),
                    rec.get("recipient_id"),
                    rec.get("Recipient Location State Code"),
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

    conn.execute("UPDATE spending_awards SET recipient_name_normalized = UPPER(TRIM(recipient_name)) WHERE recipient_name IS NOT NULL AND recipient_name_normalized IS NULL")
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
         registrant_state, registrant_country, registrant_house_id,
         client_id, client_name,
         client_state, client_ppb_state, client_country, client_ppb_country,
         client_general_description, client_government_entity,
         filing_year, filing_period,
         received_date, amount_reported, is_amendment, is_no_activity, is_termination,
         affiliated_org_count)
        SELECT filing_uuid, filing_type, registrant_id, registrant_name,
               registrant_state, registrant_country, registrant_house_id,
               client_id, client_name,
               client_state, client_ppb_state, client_country, client_ppb_country,
               client_general_description, client_government_entity,
               filing_year, filing_period,
               received_date, amount_reported, is_amendment, is_no_activity, is_termination,
               affiliated_org_count
        FROM ldb.lobbying_filings_raw
    """).rowcount
    log.info(f"  Filings: {count:,}")

    count = conn.execute("""
        INSERT OR IGNORE INTO lobbying_affiliated_orgs (id, filing_uuid, org_name, country, country_code)
        SELECT id, filing_uuid, org_name, country, country_code
        FROM ldb.lobbying_affiliated_orgs
    """).rowcount
    log.info(f"  Affiliated orgs: {count:,}")

    count = conn.execute("""
        INSERT OR IGNORE INTO lobbying_activities
        (id, filing_uuid, filing_type, registrant_name, registrant_id, client_name,
         filing_year, filing_period, issue_code, specific_issues,
         government_entities, income_amount, expense_amount,
         is_no_activity, is_termination, received_date)
        SELECT id, filing_uuid, filing_type, registrant_name, registrant_id, client_name,
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

    conn.execute("UPDATE lobbying_activities SET client_name_normalized = UPPER(TRIM(client_name)) WHERE client_name_normalized IS NULL")
    conn.commit()
    conn.execute("DETACH DATABASE ldb")
    log.info("  Lobbying import complete")


FARA_DB = BASE_DIR / "fara.db"
FEC_DB = BASE_DIR / "fec.db"
FEC_EMPLOYERS_DB = BASE_DIR / "fec_employers.db"
OIRA_DB = BASE_DIR / "oira_meetings" / "oira.db"
GOVERNMENT_UNITS_DB = BASE_DIR / "government_units" / "government_units.db"
BMF_GROUPS_DB = BASE_DIR / "bmf_groups" / "bmf_groups.db"
VOTES_DB = BASE_DIR / "votes.db"
CONGRESS_DIR = BASE_DIR / "congress_gov"
ECFR_DIR = BASE_DIR / "ecfr"
CREC_DIR = BASE_DIR / "congressional_record"
HEARINGS_DIR = BASE_DIR / "hearings"
CRS_DIR = BASE_DIR / "crs_reports"
NOMINATIONS_DIR = BASE_DIR / "nominations"
TREATIES_DIR = BASE_DIR / "treaties"
GAO_DIR = BASE_DIR / "gao_reports"
GAO_DIRECT_DIR = BASE_DIR / "gao_direct"
IG_DIR = BASE_DIR / "ig_reports"
EARMARKS_DB = BASE_DIR / "earmarks" / "earmarks.db"


def import_earmarks(conn: sqlite3.Connection):
    """Import earmarks from earmarks.db via ATTACH."""
    if not EARMARKS_DB.exists():
        log.info("Skipping earmarks (no earmarks/earmarks.db)")
        return 0

    log.info("Importing earmarks...")
    conn.execute(f"ATTACH DATABASE '{EARMARKS_DB}' AS edb")

    count = conn.execute("""
        INSERT OR REPLACE INTO earmarks
        (id, fiscal_year, chamber, member_name, member_last, member_first,
         bioguide_id, party, state, district, subcommittee,
         recipient, project_description, recipient_address, amount_requested)
        SELECT id, fiscal_year, chamber, member_name, member_last, member_first,
               bioguide_id, party, state, district, subcommittee,
               recipient, project_description, recipient_address, amount_requested
        FROM edb.earmarks
    """).rowcount
    log.info(f"  Earmarks: {count:,}")

    conn.execute("UPDATE earmarks SET recipient_normalized = UPPER(TRIM(recipient)) WHERE recipient IS NOT NULL AND recipient_normalized IS NULL")
    conn.commit()
    conn.execute("DETACH DATABASE edb")
    return count


def import_fara(conn: sqlite3.Connection):
    """Import FARA (Foreign Agents Registration Act) data from separate fara.db."""
    if not FARA_DB.exists():
        log.info("Skipping FARA data (fara.db not found)")
        return 0

    log.info("Importing FARA data...")
    conn.execute(f"ATTACH DATABASE '{FARA_DB}' AS faradb")

    # fara_registrants — list columns explicitly (destination has extra name_normalized)
    conn.execute("""INSERT OR IGNORE INTO fara_registrants
        (registration_number, registration_date, termination_date, name, business_name,
         address_1, address_2, city, state, zip)
        SELECT registration_number, registration_date, termination_date, name, business_name,
               address_1, address_2, city, state, zip
        FROM faradb.fara_registrants""")
    conn.execute("UPDATE fara_registrants SET name_normalized = UPPER(TRIM(name)) WHERE name IS NOT NULL AND name_normalized IS NULL")
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
    """Import all FEC campaign finance data: candidates, committees, contributions,
    crosswalk, disbursements (from fec.db), and employer aggregates (from fec_employers.db)."""
    if not FEC_DB.exists():
        log.info("Skipping FEC data (fec.db not found)")
        return 0

    log.info("Importing FEC campaign finance data...")
    conn.execute(f"ATTACH DATABASE '{FEC_DB}' AS fecdb")

    # Check which tables exist in fec.db (disbursement tables may not be present in older builds)
    fec_tables = {r[0] for r in conn.execute(
        "SELECT name FROM fecdb.sqlite_master WHERE type='table'"
    ).fetchall()}

    total = 0

    # --- Core tables (always present) ---

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
    total += count

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
    total += count

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
    total += count

    # Candidate crosswalk
    count = conn.execute("""
        INSERT OR IGNORE INTO fec_candidate_crosswalk
        (fec_candidate_id, bioguide_id, full_name, party, state, chamber)
        SELECT fec_candidate_id, bioguide_id, full_name, party, state, chamber
        FROM fecdb.fec_candidate_crosswalk
    """).rowcount
    log.info(f"  Candidate crosswalk: {count:,}")
    total += count

    # --- Disbursement tables (may not exist in older fec.db builds) ---

    # Operating expenditures — select key columns, join with committee name
    if "fec_operating_expenditures" in fec_tables:
        count = conn.execute("""
            INSERT INTO fec_operating_expenditures
            (cmte_id, cmte_name, form_tp_cd, name, city, state, zip_code,
             transaction_dt, transaction_amt, purpose, category, category_desc,
             entity_tp, memo_cd, memo_text, cycle)
            SELECT o.cmte_id,
                   (SELECT cm.cmte_nm FROM fecdb.fec_committees cm
                    WHERE cm.cmte_id = o.cmte_id AND cm.cycle = o.cycle LIMIT 1),
                   o.form_tp_cd, o.name, o.city, o.state, o.zip_code,
                   o.transaction_dt, o.transaction_amt, o.purpose,
                   o.category, o.category_desc, o.entity_tp,
                   o.memo_cd, o.memo_text, o.cycle
            FROM fecdb.fec_operating_expenditures o
        """).rowcount
        log.info(f"  Operating expenditures: {count:,}")
        total += count
    else:
        log.info("  fec_operating_expenditures: not yet in fec.db (run 08 with --disbursements-only)")

    # PAC & party summary — select key financial columns
    if "fec_pac_summary" in fec_tables:
        count = conn.execute("""
            INSERT OR IGNORE INTO fec_pac_summary
            (cmte_id, cmte_nm, cmte_tp, cmte_dsgn, ttl_receipts,
             indv_contrib, other_pol_cmte_contrib, ttl_disb,
             contrib_to_other_cmte, ind_exp, pty_coord_exp,
             coh_cop, debts_owed_by, cvg_end_dt, cycle)
            SELECT cmte_id, cmte_nm, cmte_tp, cmte_dsgn, ttl_receipts,
                   indv_contrib, other_pol_cmte_contrib, ttl_disb,
                   contrib_to_other_cmte, ind_exp, pty_coord_exp,
                   coh_cop, debts_owed_by, cvg_end_dt, cycle
            FROM fecdb.fec_pac_summary
        """).rowcount
        log.info(f"  PAC summary: {count:,}")
        total += count
    else:
        log.info("  fec_pac_summary: not yet in fec.db")

    # Independent expenditures — select key columns
    if "fec_independent_expenditures" in fec_tables:
        count = conn.execute("""
            INSERT INTO fec_independent_expenditures
            (cand_id, cand_name, spe_id, spe_nam, can_office,
             can_office_state, cand_pty_aff, exp_amo, exp_date,
             agg_amo, sup_opp, pur, pay, dissem_dt, fec_election_yr, cycle)
            SELECT cand_id, cand_name, spe_id, spe_nam, can_office,
                   can_office_state, cand_pty_aff, exp_amo, exp_date,
                   agg_amo, sup_opp, pur, pay, dissem_dt, fec_election_yr, cycle
            FROM fecdb.fec_independent_expenditures
        """).rowcount
        log.info(f"  Independent expenditures: {count:,}")
        total += count
    else:
        log.info("  fec_independent_expenditures: not yet in fec.db")

    # Electioneering communications
    if "fec_electioneering" in fec_tables:
        count = conn.execute("""
            INSERT INTO fec_electioneering
            (cmte_id, cmte_nm, cand_id, cand_name, cand_office,
             disb_amt, disb_dt, comm_dt, payee_name, cycle)
            SELECT cmte_id, cmte_nm, cand_id, cand_name, cand_office,
                   disb_amt, disb_dt, comm_dt, payee_name, cycle
            FROM fecdb.fec_electioneering
        """).rowcount
        log.info(f"  Electioneering communications: {count:,}")
        total += count
    else:
        log.info("  fec_electioneering: not yet in fec.db")

    # Communication costs
    if "fec_communication_costs" in fec_tables:
        count = conn.execute("""
            INSERT INTO fec_communication_costs
            (cmte_id, cmte_nm, cand_id, cand_name, cand_office,
             transaction_dt, transaction_amt, communication_tp,
             purpose, sup_opp, cycle)
            SELECT cmte_id, cmte_nm, cand_id, cand_name, cand_office,
                   transaction_dt, transaction_amt, communication_tp,
                   purpose, sup_opp, cycle
            FROM fecdb.fec_communication_costs
        """).rowcount
        log.info(f"  Communication costs: {count:,}")
        total += count
    else:
        log.info("  fec_communication_costs: not yet in fec.db")

    conn.commit()
    conn.execute("DETACH DATABASE fecdb")

    # --- Employer aggregates (separate DB) ---
    if FEC_EMPLOYERS_DB.exists():
        log.info("  Importing FEC employer aggregates...")
        conn.execute(f"ATTACH DATABASE '{FEC_EMPLOYERS_DB}' AS empdb")

        for table in ['fec_employer_totals', 'fec_employer_to_candidate',
                       'fec_employer_to_party', 'fec_top_occupations']:
            conn.execute(f"INSERT INTO {table} SELECT * FROM empdb.{table}")
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            log.info(f"  {table}: {count:,}")
            total += count

        conn.commit()
        conn.execute("DETACH DATABASE empdb")
    else:
        log.info("  Skipping employer aggregates (fec_employers.db not found — run 09_fec_employer_aggregates.py)")

    log.info(f"  FEC import complete ({total:,} total rows)")
    return total


def import_bmf_groups(conn: sqlite3.Connection):
    """Import IRS BMF group exemption membership (subordinate→GEN) from bmf_groups.db."""
    if not BMF_GROUPS_DB.exists():
        log.info("Skipping bmf_groups (bmf_groups.db not found — run 26_bmf_group_exemptions.py)")
        return 0

    log.info("Importing BMF group exemption data...")
    # ATTACH read-only + qualify DROPs with main. (same latent bug class as
    # import_government_units — fixed 2026-04-21.)
    conn.execute(f"ATTACH DATABASE 'file:{BMF_GROUPS_DB}?mode=ro' AS bgdb")
    conn.executescript("""
        DROP TABLE IF EXISTS main.bmf_group_members;
        DROP TABLE IF EXISTS main.bmf_group_exemptions;
        CREATE TABLE main.bmf_group_members AS SELECT * FROM bgdb.bmf_group_members;
        CREATE TABLE main.bmf_group_exemptions AS SELECT * FROM bgdb.bmf_group_exemptions;
        CREATE INDEX IF NOT EXISTS idx_bgm_gen ON bmf_group_members(gen);
        CREATE INDEX IF NOT EXISTS idx_bgm_ein ON bmf_group_members(ein);
        CREATE INDEX IF NOT EXISTS idx_bgm_state_gen ON bmf_group_members(state, gen);
    """)
    conn.execute("DETACH DATABASE bgdb")
    n = conn.execute("SELECT COUNT(*) FROM bmf_group_members").fetchone()[0]
    log.info(f"  BMF group members: {n:,}")
    return n


def import_government_units(conn: sqlite3.Connection):
    """Import Census-derived government units registry (~92K local govts + 56 state/territory)."""
    if not GOVERNMENT_UNITS_DB.exists():
        log.info("Skipping government_units (government_units.db not found — run 25_government_units.py)")
        return 0

    log.info("Importing government_units registry...")
    # ATTACH with mode=ro so a stray DROP/DELETE can't wipe the source DB.
    # (Fixed 2026-04-21: un-qualified DROP TABLE would fall through to the
    # attached schema when main had no matching table, destroying the
    # government_units.db source file on fresh builds.)
    conn.execute(f"ATTACH DATABASE 'file:{GOVERNMENT_UNITS_DB}?mode=ro' AS gudb")
    conn.executescript("""
        DROP TABLE IF EXISTS main.government_units;
        CREATE TABLE main.government_units AS SELECT * FROM gudb.government_units;
        -- Canonical DataDawn identifier (virtual). Format: DD-G-{gov_unit_id}.
        ALTER TABLE main.government_units ADD COLUMN dd_id TEXT
            GENERATED ALWAYS AS ('DD-G-' || gov_unit_id) VIRTUAL;
        CREATE INDEX IF NOT EXISTS idx_gu_name_norm ON government_units(name_normalized);
        CREATE INDEX IF NOT EXISTS idx_gu_name_state ON government_units(name_normalized, state);
        CREATE INDEX IF NOT EXISTS idx_gu_state ON government_units(state);
        CREATE INDEX IF NOT EXISTS idx_gu_level ON government_units(level);
        CREATE INDEX IF NOT EXISTS idx_gu_pid6 ON government_units(census_pid6) WHERE census_pid6 IS NOT NULL;
    """)
    conn.execute("DETACH DATABASE gudb")
    n = conn.execute("SELECT COUNT(*) FROM government_units").fetchone()[0]
    log.info(f"  Government units: {n:,}")
    return n


def import_oira(conn: sqlite3.Connection):
    """Import OIRA meeting and review data from oira.db."""
    if not OIRA_DB.exists():
        log.info("Skipping OIRA data (oira.db not found — run 22_oira_meetings.py)")
        return 0

    log.info("Importing OIRA data...")
    conn.execute(f"ATTACH DATABASE '{OIRA_DB}' AS oiradb")

    # Check which tables exist
    oira_tables = {r[0] for r in conn.execute(
        "SELECT name FROM oiradb.sqlite_master WHERE type='table'"
    ).fetchall()}

    total = 0

    if "oira_reviews" in oira_tables:
        count = conn.execute("""
            INSERT INTO oira_reviews
            (rin, agency_code, title, stage, economically_significant,
             date_received, date_completed, decision, date_published, major)
            SELECT rin, agency_code, title, stage, economically_significant,
                   date_received, date_completed, decision, date_published, major
            FROM oiradb.oira_reviews
        """).rowcount
        log.info(f"  OIRA reviews: {count:,}")
        total += count

    if "oira_meetings" in oira_tables:
        count = conn.execute("""
            INSERT OR IGNORE INTO oira_meetings
            (meeting_id, rin, title, agency_acronym, rule_stage,
             meeting_date, requestor_org, requestor_name, meeting_type,
             type_cd, source)
            SELECT meeting_id, rin, title, agency_acronym, rule_stage,
                   meeting_date, requestor_org, requestor_name, meeting_type,
                   type_cd, source
            FROM oiradb.oira_meetings
        """).rowcount
        log.info(f"  OIRA meetings: {count:,}")
        total += count

    if "oira_meeting_attendees" in oira_tables:
        count = conn.execute("""
            INSERT INTO oira_meeting_attendees
            (meeting_id, attendee_name, attendee_org, participation_type, is_government)
            SELECT meeting_id, attendee_name, attendee_org, participation_type, is_government
            FROM oiradb.oira_meeting_attendees
        """).rowcount
        log.info(f"  OIRA meeting attendees: {count:,}")
        total += count

    conn.commit()
    conn.execute("DETACH DATABASE oiradb")
    log.info(f"  OIRA import complete ({total:,} total rows)")
    return total


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
    cbo_count = 0

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

            # Import CBO cost estimates
            cbo_estimates = bill.get("cboCostEstimates", [])
            for est in cbo_estimates:
                if est.get("url"):
                    try:
                        conn.execute(
                            "INSERT INTO cbo_cost_estimates (bill_id, pub_date, title, description, url) VALUES (?, ?, ?, ?, ?)",
                            (bill_id, est.get("pubDate", "")[:10], est.get("title"), est.get("description"), est.get("url")),
                        )
                        cbo_count += 1
                    except sqlite3.Error:
                        pass

        conn.commit()

    log.info(f"  Done: {bill_count:,} bills, {action_count:,} actions, {subject_count:,} subjects, {cbo_count:,} CBO estimates")
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

        # Skip unparsed PTR filings (scanned PDFs with no extractable trade data).
        # These create blank rows with only filing metadata — no ticker, amount, or dates.
        # 3,072 such rows across 220 members as of 2026-03. If we improve the PDF parser
        # later, those transactions will come in via all_transactions.json instead.
        unparsed_count = 0
        if house_dir.exists():
            for fd_file in sorted(house_dir.glob("*.json")):
                try:
                    with open(fd_file) as f:
                        filings = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue
                for filing in filings:
                    if filing.get("filing_type") == "P" and str(filing.get("doc_id", "")) not in parsed_doc_ids:
                        unparsed_count += 1
        if unparsed_count:
            log.info(f"  House unparsed PTRs (skipped, no trade data): {unparsed_count:,}")
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

    # Normalize transaction_type: House uses abbreviated codes (P, S, E, S (partial))
    # while Senate uses full names (Purchase, Sale, Exchange, Sale (Partial)).
    # Normalize all to full names so explore pages don't need client-side translation.
    txn_map = {
        'P': 'Purchase',
        'S': 'Sale (Full)',
        'S (partial)': 'Sale (Partial)',
        'S (Partial)': 'Sale (Partial)',
        'E': 'Exchange',
    }
    normalized = 0
    for abbrev, full_name in txn_map.items():
        cur = conn.execute(
            "UPDATE stock_trades SET transaction_type = ? WHERE transaction_type = ?",
            (full_name, abbrev)
        )
        normalized += cur.rowcount
    conn.commit()
    if normalized:
        log.info(f"  Normalized {normalized:,} abbreviated transaction_type values to full names")

    # Stats
    matched = conn.execute(
        "SELECT COUNT(*) FROM stock_trades WHERE bioguide_id != ''"
    ).fetchone()[0]
    log.info(f"  Done: {total:,} stock trades ({matched:,} matched to bioguide_id)")

    return total


def enrich_stock_trades_cik(conn: sqlite3.Connection):
    """Backfill stock_trades.cik from ticker_sic. Must run AFTER ticker_sic is loaded."""
    if conn.execute("SELECT COUNT(*) FROM ticker_sic").fetchone()[0] == 0:
        log.info("Skipping stock_trades CIK enrichment (ticker_sic empty)")
        return 0
    total = conn.execute("SELECT COUNT(*) FROM stock_trades").fetchone()[0]
    if total == 0:
        return 0
    conn.execute("""
        UPDATE stock_trades SET cik = (
            SELECT CAST(ts.cik AS INTEGER) FROM ticker_sic ts
            WHERE UPPER(ts.ticker) = UPPER(stock_trades.ticker)
            LIMIT 1
        )
        WHERE cik IS NULL AND ticker IS NOT NULL
    """)
    conn.commit()
    n_with_cik = conn.execute("SELECT COUNT(*) FROM stock_trades WHERE cik IS NOT NULL").fetchone()[0]
    log.info(f"stock_trades CIK enrichment: {n_with_cik:,} of {total:,} trades linked to SEC CIK ({100*n_with_cik/total:.1f}%)")
    return n_with_cik


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


def import_hearings(conn: sqlite3.Connection):
    """Import committee hearings from GovInfo CHRG MODS JSON."""
    if not HEARINGS_DIR.exists():
        log.info("Skipping hearings (no hearings/ directory)")
        return 0

    log.info("Importing committee hearings...")
    hearing_count = 0
    witness_count = 0
    member_count = 0

    for jf in sorted(HEARINGS_DIR.glob("*.json")):
        try:
            with open(jf) as f:
                h = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        pid = h.get("package_id", jf.stem)

        # Validate congress — some MODS metadata has wrong values.
        # Authoritative source is the package_id prefix: CHRG-{congress}{chamber}...
        congress_val = h.get("congress")
        if congress_val is None or not (50 <= congress_val <= 200):
            m = re.match(r'CHRG-(\d+)', pid)
            if m:
                congress_val = int(m.group(1))

        # Committees as JSON string
        committees = json.dumps(h.get("committees", [])) if h.get("committees") else None

        try:
            conn.execute("""
                INSERT OR REPLACE INTO hearings
                (package_id, title, chamber, congress, session, date_issued,
                 committees, detail_url, html_url, pdf_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pid,
                h.get("title"),
                h.get("chamber"),
                congress_val,
                h.get("session"),
                h.get("date_issued"),
                committees,
                h.get("detail_url"),
                h.get("html_url"),
                h.get("pdf_url"),
            ))
            hearing_count += 1
        except sqlite3.Error:
            continue

        # Witnesses
        for w in h.get("witnesses", []):
            if isinstance(w, dict):
                name = w.get("name", "")
                title = w.get("title", "")
                org = w.get("organization", "")
                loc = w.get("location", "")
            else:
                name = str(w)
                title = org = loc = ""
            if name:
                try:
                    conn.execute(
                        "INSERT INTO hearing_witnesses (package_id, name, title, organization, location) VALUES (?, ?, ?, ?, ?)",
                        (pid, name, title, org, loc),
                    )
                    witness_count += 1
                except sqlite3.Error:
                    pass

        # Members
        for m in h.get("members", []):
            mname = m.get("name", str(m)) if isinstance(m, dict) else str(m)
            mrole = m.get("role", "") if isinstance(m, dict) else ""
            if mname:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO hearing_members (package_id, name, role) VALUES (?, ?, ?)",
                        (pid, mname, mrole),
                    )
                    member_count += 1
                except sqlite3.Error:
                    pass

        if hearing_count % 5000 == 0 and hearing_count > 0:
            conn.commit()
            log.info(f"  Progress: {hearing_count:,} hearings")

    conn.execute("UPDATE hearing_witnesses SET organization_normalized = UPPER(TRIM(organization)) WHERE organization IS NOT NULL AND organization != '' AND organization_normalized IS NULL")
    conn.commit()
    log.info(f"  Done: {hearing_count:,} hearings, {witness_count:,} witnesses, {member_count:,} members")
    return hearing_count


def import_crs_reports(conn: sqlite3.Connection):
    """Import CRS reports from Congress.gov JSON."""
    if not CRS_DIR.exists():
        log.info("Skipping CRS reports (no crs_reports/ directory)")
        return 0

    log.info("Importing CRS reports...")
    report_count = 0
    bill_ref_count = 0

    for jf in sorted(CRS_DIR.glob("*.json")):
        try:
            with open(jf) as f:
                r = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        rid = r.get("id", jf.stem)
        authors = ", ".join(r.get("authors", [])) if r.get("authors") else None
        topics = ", ".join(r.get("topics", [])) if r.get("topics") else None

        try:
            conn.execute("""
                INSERT OR REPLACE INTO crs_reports
                (id, title, publish_date, update_date, status, content_type,
                 authors, topics, summary, pdf_url, html_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rid,
                r.get("title"),
                r.get("publishDate"),
                r.get("updateDate"),
                r.get("status"),
                r.get("contentType"),
                authors,
                topics,
                r.get("summary"),
                r.get("pdf_url"),
                r.get("html_url"),
            ))
            report_count += 1
        except sqlite3.Error:
            continue

        # Related bills
        for bill in r.get("relatedBills", []):
            congress = bill.get("congress")
            btype = (bill.get("type") or "").lower()
            bnumber = bill.get("number")
            bill_id = f"{congress}-{btype}-{bnumber}" if congress and btype and bnumber else None
            try:
                conn.execute(
                    "INSERT INTO crs_report_bills (report_id, bill_title, congress, bill_type, bill_number, bill_id) VALUES (?, ?, ?, ?, ?, ?)",
                    (rid, bill.get("title"), congress, btype, bnumber, bill_id),
                )
                bill_ref_count += 1
            except sqlite3.Error:
                pass

    conn.commit()
    log.info(f"  Done: {report_count:,} CRS reports, {bill_ref_count:,} bill references")
    return report_count


def import_nominations(conn: sqlite3.Connection):
    """Import executive nominations from Congress.gov JSON."""
    if not NOMINATIONS_DIR.exists():
        log.info("Skipping nominations (no nominations/ directory)")
        return 0

    log.info("Importing executive nominations...")
    nom_count = 0
    action_count = 0

    for jf in sorted(NOMINATIONS_DIR.glob("*.json")):
        try:
            with open(jf) as f:
                n = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        congress = n.get("congress")
        number = n.get("number")
        part = n.get("partNumber", "")
        nom_id = f"{congress}-{number}"
        if part:
            nom_id = f"{congress}-{number}-{part}"

        description = n.get("description") or ""
        organization = n.get("organization") or ""

        # For nominees with data, pull organization from first nominee if top-level is empty
        nominees = n.get("nominees", [])
        if not organization and nominees:
            organization = nominees[0].get("organization", "") or ""

        # For military nominations with no description, construct one from the
        # referral action (e.g., "Military nomination — Armed Services Committee")
        if not description and n.get("isMilitary"):
            committee = ""
            for act in n.get("actions", []):
                if act.get("type") == "IntroReferral":
                    # Extract committee name from "referred to the Committee on X"
                    txt = act.get("text", "")
                    m = re.search(r"Committee on (.+?)\.?$", txt)
                    if m:
                        committee = m.group(1).rstrip(".")
                    break
            if committee:
                description = f"Military nomination — {committee}"
                if not organization:
                    organization = committee
            else:
                description = "Military nomination"

        try:
            conn.execute("""
                INSERT OR REPLACE INTO nominations
                (id, congress, number, part_number, citation, description,
                 organization, received_date, authority_date,
                 is_civilian, is_military, status, vote_yea, vote_nay)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                nom_id,
                congress,
                number,
                part,
                n.get("citation"),
                description,
                organization,
                n.get("receivedDate"),
                n.get("authorityDate"),
                1 if n.get("isCivilian") else 0,
                1 if n.get("isMilitary") else 0,
                n.get("status"),
                n.get("vote_yea"),
                n.get("vote_nay"),
            ))
            nom_count += 1
        except sqlite3.Error:
            continue

        # Actions
        for act in n.get("actions", []):
            try:
                conn.execute(
                    "INSERT INTO nomination_actions (nomination_id, action_date, action_text, action_type) VALUES (?, ?, ?, ?)",
                    (nom_id, act.get("actionDate"), act.get("text"), act.get("type")),
                )
                action_count += 1
            except sqlite3.Error:
                pass

        if nom_count % 5000 == 0 and nom_count > 0:
            conn.commit()
            log.info(f"  Progress: {nom_count:,} nominations")

    conn.commit()
    log.info(f"  Done: {nom_count:,} nominations, {action_count:,} actions")
    return nom_count


def import_treaties(conn: sqlite3.Connection):
    """Import treaties from Congress.gov JSON."""
    if not TREATIES_DIR.exists():
        log.info("Skipping treaties (no treaties/ directory)")
        return 0

    log.info("Importing treaties...")
    treaty_count = 0
    action_count = 0

    for jf in sorted(TREATIES_DIR.glob("*.json")):
        try:
            with open(jf) as f:
                t = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        congress = t.get("congress")
        number = t.get("number")
        treaty_id = jf.stem  # Already formatted as congress-number[-suffix]

        countries = ", ".join(t.get("countries", [])) if t.get("countries") else None
        index_terms = ", ".join(t.get("indexTerms", [])) if t.get("indexTerms") else None

        try:
            conn.execute("""
                INSERT OR REPLACE INTO treaties
                (id, congress, number, title, topic, transmitted_date,
                 in_force_date, countries, index_terms, resolution_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                treaty_id,
                congress,
                number,
                t.get("title"),
                t.get("topic"),
                t.get("transmittedDate"),
                t.get("inForceDate"),
                countries,
                index_terms,
                t.get("resolutionText"),
            ))
            treaty_count += 1
        except sqlite3.Error:
            continue

        # Actions
        for act in t.get("actions", []):
            try:
                conn.execute(
                    "INSERT INTO treaty_actions (treaty_id, action_date, action_text, action_type) VALUES (?, ?, ?, ?)",
                    (treaty_id, act.get("actionDate"), act.get("text"), act.get("type")),
                )
                action_count += 1
            except sqlite3.Error:
                pass

    conn.commit()
    log.info(f"  Done: {treaty_count:,} treaties, {action_count:,} actions")
    return treaty_count


def import_gao_reports(conn: sqlite3.Connection):
    """Import GAO reports from GovInfo MODS JSON."""
    if not GAO_DIR.exists():
        log.info("Skipping GAO reports (no gao_reports/ directory)")
        return 0

    log.info("Importing GAO reports...")
    count = 0

    for jf in sorted(GAO_DIR.glob("*.json")):
        try:
            with open(jf) as f:
                g = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        pid = g.get("package_id", jf.stem)
        subjects = ", ".join(g.get("subjects", [])) if g.get("subjects") else None
        public_laws = ", ".join(g.get("public_laws", [])) if g.get("public_laws") else None
        usc_refs = ", ".join(g.get("usc_references", [])) if g.get("usc_references") else None
        statute_refs = ", ".join(g.get("statute_references", [])) if g.get("statute_references") else None

        try:
            conn.execute("""
                INSERT OR REPLACE INTO gao_reports
                (package_id, title, date_issued, report_number, document_type,
                 doc_class, abstract, subjects, public_laws, usc_references,
                 statute_references, citation, pdf_url, html_url, detail_url, sudocs)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pid,
                g.get("title"),
                g.get("date_issued"),
                g.get("report_number"),
                g.get("document_type"),
                g.get("doc_class"),
                g.get("abstract"),
                subjects,
                public_laws,
                usc_refs,
                statute_refs,
                g.get("citation"),
                g.get("pdf_url"),
                g.get("html_url"),
                g.get("detail_url"),
                g.get("sudocs"),
            ))
            count += 1
        except sqlite3.Error:
            continue

        if count % 5000 == 0 and count > 0:
            conn.commit()
            log.info(f"  Progress: {count:,} GAO reports")

    conn.commit()
    log.info(f"  Done: {count:,} GAO reports (GovInfo)")

    # --- GAO Direct (gao.gov listing stubs, 2009-present) ---
    stubs_file = GAO_DIRECT_DIR / "_listing_stubs.json"
    if stubs_file.exists():
        log.info("  Loading GAO direct listing stubs...")
        try:
            stubs = json.loads(stubs_file.read_text())
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"  Failed to read GAO direct stubs: {e}")
            stubs = []

        direct_count = 0
        for s in stubs:
            gao_num = s.get("gao_number")
            if not gao_num:
                continue
            url = s.get("url", "")
            if url and not url.startswith("http"):
                url = f"https://www.gao.gov{url}"
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO gao_reports
                    (package_id, title, date_issued, report_number, abstract, detail_url)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    gao_num,
                    s.get("title"),
                    s.get("published_date"),
                    gao_num,
                    s.get("summary"),
                    url,
                ))
                direct_count += 1
            except sqlite3.Error:
                continue

        conn.commit()
        log.info(f"  Done: {direct_count:,} GAO direct stubs (INSERT OR IGNORE)")
        count += direct_count
    else:
        log.info("  Skipping GAO direct (gao_direct/_listing_stubs.json not found)")

    return count


def import_ig_reports(conn: sqlite3.Connection):
    """Import Inspector General reports from oversight.gov JSON files."""
    if not IG_DIR.exists():
        log.info("Skipping IG reports (ig_reports/ not found — run 21_ig_reports.py)")
        return 0

    log.info("Importing IG reports...")
    count = 0
    rec_count = 0

    for jf in sorted(IG_DIR.glob("*.json")):
        if jf.name.startswith("_"):
            continue  # skip _listing_stubs.json and similar
        try:
            data = json.loads(jf.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        if not isinstance(data, dict) or not data.get("title"):
            continue

        report_id = data.get("_report_id", jf.stem)
        detail_url = data.get("detail_url", "")
        if detail_url and not detail_url.startswith("http"):
            detail_url = f"https://www.oversight.gov{detail_url}"

        try:
            conn.execute("""
                INSERT OR IGNORE INTO ig_reports
                (report_id, title, date_issued, report_number, report_type,
                 agency_reviewed, submitting_oig, location, description,
                 num_recommendations, questioned_costs, funds_for_better_use,
                 pdf_url, detail_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                report_id,
                data.get("title"),
                data.get("date_issued"),
                data.get("report_number"),
                data.get("report_type"),
                data.get("agency_reviewed"),
                data.get("submitting_oig"),
                data.get("location"),
                data.get("description"),
                data.get("num_recommendations", 0),
                data.get("questioned_costs", 0),
                data.get("funds_for_better_use", 0),
                data.get("pdf_url"),
                detail_url,
            ))
            count += 1

            for rec in data.get("recommendations", []):
                conn.execute("""
                    INSERT INTO ig_recommendations
                    (report_id, rec_number, significant, text,
                     questioned_costs, funds_for_better_use)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    report_id,
                    rec.get("rec_number"),
                    rec.get("significant"),
                    rec.get("text"),
                    rec.get("questioned_costs", 0),
                    rec.get("funds_for_better_use", 0),
                ))
                rec_count += 1

        except sqlite3.Error as e:
            log.debug(f"  IG insert error for {report_id}: {e}")

        if count % 5000 == 0 and count > 0:
            conn.commit()
            log.info(f"  {count:,} IG reports...")

    conn.commit()
    log.info(f"  IG reports: {count:,}, recommendations: {rec_count:,}")
    return count


# ═════════════════════════════════════════════════════════════════════════════
# Staging-DB imports (entities, disbursements, OGE PAS, WH visits).
#
# These DBs live under /mnt/data/datadawn/staging/ and are built by a separate
# pipeline (scripts 24/40-47, entity_population_phase_*). We attach them
# read-only and copy tables + indexes into openregs.db, preserving original
# table names (no collisions with the rest of the schema).
#
# WAL snapshot-read: ATTACH ...?mode=ro gives us SQLite's read-transaction
# snapshot. Any concurrent writers (Form 4 backfill, NAICS enrichment) keep
# writing to the staging WAL; our read sees a consistent point-in-time view.
# ═════════════════════════════════════════════════════════════════════════════

STAGING_ENTITIES_DB = Path('/mnt/data/datadawn/staging/entities_phase_a.db')
STAGING_DISBURSEMENTS_DB = Path('/mnt/data/datadawn/staging/disbursements/disbursements.db')
STAGING_OGE_PAS_DB = Path('/mnt/data/datadawn/staging/oge_pas.db')
STAGING_WH_VISITS_DB = Path('/mnt/data/datadawn/staging/wh_visits.db')


def _attach_ro(conn: sqlite3.Connection, db_path: Path, alias: str):
    """ATTACH staging DB read-only (so concurrent writers don't affect us).
    Uses SQLite URI syntax; caller's sqlite3 connection must have uri=True."""
    conn.execute(f"ATTACH DATABASE 'file:{db_path}?mode=ro' AS {alias}")


def _copy_table_with_schema(conn: sqlite3.Connection, src_alias: str,
                             src_table: str, dest_table: str | None = None):
    """Copy a table + its indexes from attached alias into main DB.
    Uses PRAGMA table_info (not sqlite_master.sql) to resolve columns, since
    ALTER TABLE ADD COLUMN doesn't update the stored DDL string."""
    dest_table = dest_table or src_table
    cols = list(conn.execute(f'PRAGMA {src_alias}.table_info("{src_table}")'))
    if not cols:
        log.warning(f"  {src_alias}.{src_table}: table not found, skipping")
        return 0
    # Read original DDL to preserve PRIMARY KEY / AUTOINCREMENT / NOT NULL intent.
    ddl_row = conn.execute(
        f"SELECT sql FROM {src_alias}.sqlite_master WHERE type='table' AND name=?",
        (src_table,)
    ).fetchone()
    sql = ddl_row[0] if ddl_row else None
    # Qualify with main. so SQLite doesn't resolve to the (readonly) attached DB.
    conn.execute(f'DROP TABLE IF EXISTS main."{dest_table}"')
    if sql:
        # Use the original DDL as the baseline (preserves constraints).
        if src_table != dest_table:
            sql = sql.replace(f'TABLE "{src_table}"', f'TABLE "{dest_table}"')
            sql = sql.replace(f'TABLE {src_table}', f'TABLE {dest_table}')
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            # Fall back to synthesized DDL if original fails (e.g. reserved-word clash)
            col_defs = ', '.join(f'"{c[1]}" {c[2]}' for c in cols)
            conn.execute(f'CREATE TABLE main."{dest_table}" ({col_defs})')
    else:
        col_defs = ', '.join(f'"{c[1]}" {c[2]}' for c in cols)
        conn.execute(f'CREATE TABLE main."{dest_table}" ({col_defs})')
    # If the dest table's column set doesn't fully match the source (e.g. later
    # ALTER TABLE ADD COLUMN added cols to source that aren't in stored DDL),
    # add the missing ones before INSERT.
    dest_cols = {c[1] for c in conn.execute(f'PRAGMA main.table_info("{dest_table}")')}
    for c in cols:
        if c[1] not in dest_cols:
            conn.execute(f'ALTER TABLE main."{dest_table}" ADD COLUMN "{c[1]}" {c[2]}')
    # Explicit column list so ordering is unambiguous
    col_list = ', '.join(f'"{c[1]}"' for c in cols)
    conn.execute(
        f'INSERT INTO main."{dest_table}" ({col_list}) '
        f'SELECT {col_list} FROM {src_alias}."{src_table}"'
    )
    count = conn.execute(f'SELECT COUNT(*) FROM main."{dest_table}"').fetchone()[0]
    # Copy indexes (skip auto-indexes sqlite creates for PKs)
    for (idx_sql,) in conn.execute(
        f"SELECT sql FROM {src_alias}.sqlite_master "
        f"WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (src_table,)
    ):
        # Indexes reference table name; if we renamed, adjust
        s = idx_sql
        if src_table != dest_table:
            s = s.replace(f'ON "{src_table}"', f'ON "{dest_table}"')
            s = s.replace(f'ON {src_table}', f'ON {dest_table}')
        try:
            conn.execute(s)
        except sqlite3.OperationalError as e:
            log.warning(f"    index skipped ({e}): {s[:80]}")
    return count


def import_staging_entities(conn: sqlite3.Connection):
    """Copy entities_phase_a.db — the entity-resolution master layer."""
    if not STAGING_ENTITIES_DB.exists():
        log.info("Skipping entities staging (entities_phase_a.db not found)")
        return 0
    log.info("Importing staging entities_phase_a...")
    _attach_ro(conn, STAGING_ENTITIES_DB, 'ent_stg')
    tables = [
        'entities', 'public_actors', 'entity_aliases', 'entity_relationships',
        'entity_dominance', 'industries', 'industries_sic_map',
        'industries_ntee_map', 'uei_registry',
    ]
    total = 0
    for t in tables:
        n = _copy_table_with_schema(conn, 'ent_stg', t)
        log.info(f"  {t}: {n:,}")
        total += n
    conn.commit()
    conn.execute("DETACH DATABASE ent_stg")
    log.info(f"  entities staging import complete: {total:,} rows")
    return total


def import_staging_disbursements(conn: sqlite3.Connection):
    """Copy disbursements.db — House CSV + PDF + Senate PDF office spending."""
    if not STAGING_DISBURSEMENTS_DB.exists():
        log.info("Skipping disbursements staging")
        return 0
    log.info("Importing staging disbursements...")
    _attach_ro(conn, STAGING_DISBURSEMENTS_DB, 'dis_stg')
    tables = [
        'house_disbursements_nonpersonnel',
        'house_disbursements_personnel_agg',
        'house_disbursements_summary',
        'house_pre2016_office_totals',
        'senate_office_totals',
    ]
    total = 0
    for t in tables:
        n = _copy_table_with_schema(conn, 'dis_stg', t)
        log.info(f"  {t}: {n:,}")
        total += n
    # Also copy the dedup view (qualify main. so SQLite doesn't resolve to attached)
    conn.execute('DROP VIEW IF EXISTS main.house_pre2016_office_totals_dedup')
    view_sql = conn.execute(
        "SELECT sql FROM dis_stg.sqlite_master WHERE type='view' AND name=?",
        ('house_pre2016_office_totals_dedup',)
    ).fetchone()
    if view_sql and view_sql[0]:
        conn.execute(view_sql[0])
    conn.commit()
    conn.execute("DETACH DATABASE dis_stg")
    log.info(f"  disbursements import complete: {total:,} rows + dedup view")
    return total


def import_staging_oge_pas(conn: sqlite3.Connection):
    """Copy OGE PAS (senior executive ethics disclosures)."""
    if not STAGING_OGE_PAS_DB.exists():
        log.info("Skipping OGE PAS staging")
        return 0
    log.info("Importing staging OGE PAS...")
    _attach_ro(conn, STAGING_OGE_PAS_DB, 'oge_stg')
    total = 0
    for t in ('oge_pas_filings', 'oge_pas_transactions'):
        n = _copy_table_with_schema(conn, 'oge_stg', t)
        log.info(f"  {t}: {n:,}")
        total += n
    conn.commit()
    conn.execute("DETACH DATABASE oge_stg")
    log.info(f"  OGE PAS import complete: {total:,} rows")
    return total


def import_staging_wh_visits(conn: sqlite3.Connection):
    """Copy WH visitor logs (tour PII-redacted + policy visits with name)."""
    if not STAGING_WH_VISITS_DB.exists():
        log.info("Skipping WH visits staging")
        return 0
    log.info("Importing staging WH visits...")
    _attach_ro(conn, STAGING_WH_VISITS_DB, 'wh_stg')
    total = 0
    for t in ('wh_visits', 'wh_visits_daily_agg'):
        n = _copy_table_with_schema(conn, 'wh_stg', t)
        log.info(f"  {t}: {n:,}")
        total += n
    conn.commit()
    conn.execute("DETACH DATABASE wh_stg")
    log.info(f"  WH visits import complete: {total:,} rows")
    return total


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


def link_hearing_members(conn: sqlite3.Connection):
    """Match hearing member names to congress_members bioguide IDs."""
    log.info("Linking hearing members to congress_members...")

    # Build a temp lookup table with all name variants for fast JOIN matching
    # This replaces slow correlated subqueries (1.2M x 12K = billions of comparisons)
    conn.execute("DROP TABLE IF EXISTS _tmp_member_names")
    conn.execute("""
        CREATE TEMP TABLE _tmp_member_names AS
        SELECT bioguide_id, full_name AS name FROM congress_members
        UNION
        SELECT bioguide_id, first_name || ' ' || last_name FROM congress_members
        UNION
        SELECT bioguide_id, first_name || ' ' || last_name FROM congress_members
            WHERE last_name != full_name
    """)
    conn.execute("CREATE INDEX _tmp_mn_name ON _tmp_member_names(name)")
    log.info("  Built name lookup table")

    # Fast JOIN-based update: exact name match
    conn.execute("""
        UPDATE hearing_members SET bioguide_id = (
            SELECT t.bioguide_id FROM _tmp_member_names t
            WHERE t.name = hearing_members.name
            LIMIT 1
        )
        WHERE bioguide_id IS NULL
    """)
    log.info("  Pass 1 (exact match) complete")

    # Pass 2: strip middle initial from hearing name (e.g. "Benjamin L. Cardin" -> "Benjamin Cardin")
    conn.execute("""
        UPDATE hearing_members SET bioguide_id = (
            SELECT t.bioguide_id FROM _tmp_member_names t
            WHERE t.name = TRIM(
                SUBSTR(hearing_members.name, 1, INSTR(hearing_members.name, ' ') - 1) || ' ' ||
                CASE
                    WHEN INSTR(SUBSTR(hearing_members.name, INSTR(hearing_members.name, ' ') + 1), ' ') > 0
                    THEN SUBSTR(SUBSTR(hearing_members.name, INSTR(hearing_members.name, ' ') + 1),
                         INSTR(SUBSTR(hearing_members.name, INSTR(hearing_members.name, ' ') + 1), ' ') + 1)
                    ELSE SUBSTR(hearing_members.name, INSTR(hearing_members.name, ' ') + 1)
                END
            )
            LIMIT 1
        )
        WHERE bioguide_id IS NULL
    """)
    log.info("  Pass 2 (strip middle initial) complete")

    conn.execute("DROP TABLE IF EXISTS _tmp_member_names")
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM hearing_members").fetchone()[0]
    linked = conn.execute("SELECT COUNT(*) FROM hearing_members WHERE bioguide_id IS NOT NULL").fetchone()[0]
    log.info(f"  Linked {linked:,} / {total:,} hearing members ({100*linked/total:.1f}%) to bioguide IDs")


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


def materialize_slow_views(conn: sqlite3.Connection):
    """Materialize slow aggregate views as tables for instant queries.

    These were previously VIEWs that aggregated millions of rows on every query,
    causing 6-30+ second response times or outright timeouts. Building them as
    tables during the ~20 min build adds minimal time but makes queries instant.
    """
    log.info("Materializing slow views as tables...")

    # Pre-aggregate comment and document counts into temp tables for fast JOINs
    # This avoids the catastrophic LEFT JOIN of 14M comments x 2M documents x 111K dockets
    log.info("  Pre-aggregating comment and document counts...")
    conn.execute("DROP TABLE IF EXISTS _tmp_comment_counts")
    conn.execute("""
        CREATE TEMP TABLE _tmp_comment_counts AS
        SELECT docket_id,
            COUNT(*) AS comment_count,
            MIN(posted_date) AS earliest_comment,
            MAX(posted_date) AS latest_comment
        FROM comments
        WHERE docket_id IS NOT NULL
        GROUP BY docket_id
    """)
    conn.execute("CREATE INDEX _tmp_cc_docket ON _tmp_comment_counts(docket_id)")

    conn.execute("DROP TABLE IF EXISTS _tmp_doc_counts")
    conn.execute("""
        CREATE TEMP TABLE _tmp_doc_counts AS
        SELECT docket_id,
            COUNT(*) AS document_count,
            MIN(posted_date) AS earliest_document,
            MAX(posted_date) AS latest_document
        FROM documents
        WHERE docket_id IS NOT NULL
        GROUP BY docket_id
    """)
    conn.execute("CREATE INDEX _tmp_dc_docket ON _tmp_doc_counts(docket_id)")
    log.info("  Pre-aggregation done")

    tables = [
        ("docket_stats", """
            SELECT
                d.id, d.agency_id, d.title, d.docket_type,
                COALESCE(cc.comment_count, 0) AS comment_count,
                COALESCE(dc.document_count, 0) AS document_count,
                cc.earliest_comment,
                cc.latest_comment,
                dc.earliest_document,
                dc.latest_document
            FROM dockets d
            LEFT JOIN _tmp_comment_counts cc ON cc.docket_id = d.id
            LEFT JOIN _tmp_doc_counts dc ON dc.docket_id = d.id
        """, [
            "CREATE INDEX idx_ds_agency ON docket_stats(agency_id)",
            "CREATE INDEX idx_ds_comments ON docket_stats(comment_count DESC)",
        ]),
        ("comments_by_year", """
            SELECT
                agency_id, posted_year,
                COUNT(*) AS comment_count,
                COUNT(DISTINCT docket_id) AS docket_count,
                SUM(CASE WHEN submitter_type = 'organization' THEN 1 ELSE 0 END) AS org_comments,
                SUM(CASE WHEN submitter_type = 'individual' THEN 1 ELSE 0 END) AS individual_comments,
                SUM(CASE WHEN submitter_type = 'anonymous' THEN 1 ELSE 0 END) AS anonymous_comments
            FROM comments
            WHERE posted_year IS NOT NULL
            GROUP BY agency_id, posted_year
        """, [
            "CREATE INDEX idx_cby_agency ON comments_by_year(agency_id)",
        ]),
        ("fr_by_year", """
            SELECT pub_year, type, COUNT(*) AS doc_count
            FROM federal_register
            WHERE pub_year IS NOT NULL
            GROUP BY pub_year, type
        """, []),
        ("top_dockets", """
            SELECT
                c.docket_id, c.agency_id,
                d.title AS docket_title, d.docket_type,
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
        """, [
            "CREATE INDEX idx_td_comments ON top_dockets(comment_count DESC)",
            "CREATE INDEX idx_td_agency ON top_dockets(agency_id)",
        ]),
        ("comments_monthly", """
            SELECT
                agency_id, posted_year, posted_month,
                posted_year || '-' || printf('%02d', posted_month) AS year_month,
                COUNT(*) AS comment_count,
                COUNT(DISTINCT docket_id) AS active_dockets
            FROM comments
            WHERE posted_year IS NOT NULL AND posted_month IS NOT NULL
            GROUP BY agency_id, posted_year, posted_month
        """, [
            "CREATE INDEX idx_cm_agency ON comments_monthly(agency_id)",
        ]),
        ("top_submitters", """
            SELECT
                submitter_name, submitter_type,
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
        """, [
            "CREATE INDEX idx_ts_count ON top_submitters(comment_count DESC)",
        ]),
        ("speaker_activity", """
            SELECT
                cs.speaker_name, cs.bioguide_id,
                cm.full_name AS official_name, cm.party, cm.state,
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
        """, [
            "CREATE INDEX idx_sa_bioguide ON speaker_activity(bioguide_id)",
            "CREATE INDEX idx_sa_speeches ON speaker_activity(total_speeches DESC)",
        ]),
        ("bills_floor_time", """
            SELECT
                cb.bill_id, cb.congress, cb.bill_type, cb.bill_number,
                l.title AS bill_title, l.policy_area, l.sponsor_name,
                COUNT(*) AS floor_mentions,
                COUNT(DISTINCT cr.date) AS days_discussed,
                MIN(cr.date) AS first_discussed,
                MAX(cr.date) AS last_discussed
            FROM crec_bills cb
            JOIN congressional_record cr ON cb.granule_id = cr.granule_id
            LEFT JOIN legislation l ON cb.bill_id = l.bill_id
            GROUP BY cb.bill_id
            HAVING floor_mentions >= 3
        """, [
            "CREATE INDEX idx_bft_mentions ON bills_floor_time(floor_mentions DESC)",
            "CREATE INDEX idx_bft_bill ON bills_floor_time(bill_id)",
        ]),
        ("lobbying_by_year", """
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
        """, []),
        ("top_lobbying_clients", """
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
        """, [
            "CREATE INDEX idx_tlc_income ON top_lobbying_clients(total_reported_income DESC)",
        ]),
        ("spending_by_agency", """
            SELECT
                agency, sub_agency, award_category,
                COUNT(*) AS award_count,
                SUM(award_amount) AS total_amount,
                AVG(award_amount) AS avg_amount,
                MIN(start_date) AS earliest,
                MAX(start_date) AS latest
            FROM spending_awards
            GROUP BY agency, sub_agency, award_category
        """, [
            "CREATE INDEX idx_sba_amount ON spending_by_agency(total_amount DESC)",
        ]),
        ("stock_trades_by_ticker", """
            SELECT
                st.ticker, st.asset_description,
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
        """, [
            "CREATE INDEX idx_stbt_count ON stock_trades_by_ticker(trade_count DESC)",
        ]),
    ]

    for name, sql, indexes in tables:
        conn.execute(f"DROP TABLE IF EXISTS {name}")
        conn.execute(f"DROP VIEW IF EXISTS {name}")
        conn.execute(f"CREATE TABLE {name} AS {sql}")
        for idx in indexes:
            conn.execute(idx)
        conn.commit()
        count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        log.info(f"  {name}: {count:,} rows")

    log.info("  Slow views materialized")


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


def import_reference_tables(conn: sqlite3.Connection):
    """Import committee-sector and lobbying-issue reference CSVs."""
    import csv
    ref_dir = BASE_DIR / "reference"

    # Committee sectors
    csv_path = ref_dir / "committee_sectors.csv"
    if csv_path.exists():
        conn.execute("DELETE FROM committee_sectors")
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [(r['committee_id'], r['committee_name'], r['chamber'],
                     r['regulated_sectors'], r['example_tickers'],
                     r['gics_sectors'], r['sic_ranges'], r['notes']) for r in reader]
        conn.executemany(
            "INSERT OR REPLACE INTO committee_sectors VALUES (?,?,?,?,?,?,?,?)", rows)
        conn.commit()
        log.info(f"  committee_sectors: {len(rows)} rows")
    else:
        log.warning(f"  {csv_path} not found, skipping")

    # Lobbying issue-to-agency mapping
    csv_path = ref_dir / "lobbying_issue_agencies.csv"
    if csv_path.exists():
        conn.execute("DELETE FROM lobbying_issue_agencies")
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [(r['issue_code'], r['issue_description'], r['primary_agencies'],
                     r['secondary_agencies'], r['regs_gov_agency_prefixes'],
                     r['federal_register_agency_slugs'], r['related_committee_ids'],
                     r['notes']) for r in reader]
        conn.executemany(
            "INSERT OR REPLACE INTO lobbying_issue_agencies VALUES (?,?,?,?,?,?,?,?)", rows)
        conn.commit()
        log.info(f"  lobbying_issue_agencies: {len(rows)} rows")
    else:
        log.warning(f"  {csv_path} not found, skipping")

    # Committee jurisdiction (tiered SIC mapping)
    csv_path = ref_dir / "committee_jurisdiction.csv"
    if csv_path.exists():
        conn.execute("DELETE FROM committee_jurisdiction")
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [(r['committee_id'], r['committee_name'], r['chamber'],
                     r['jurisdiction_desc'], r['sic_codes'],
                     r['jurisdiction_tier'], r['notes']) for r in reader]
        conn.executemany(
            "INSERT INTO committee_jurisdiction (committee_id, committee_name, chamber, jurisdiction_desc, sic_codes, jurisdiction_tier, notes) VALUES (?,?,?,?,?,?,?)",
            rows)
        conn.commit()
        log.info(f"  committee_jurisdiction: {len(rows)} rows")

        # Expand SIC code strings into committee_sic_ranges for efficient matching
        conn.execute("DELETE FROM committee_sic_ranges")
        range_count = 0
        for committee_id, _, _, _, sic_codes, tier, _ in rows:
            if not sic_codes:
                continue
            for part in sic_codes.split(","):
                part = part.strip()
                if not part:
                    continue
                if "-" in part:
                    start, end = part.split("-", 1)
                    try:
                        conn.execute(
                            "INSERT INTO committee_sic_ranges VALUES (?,?,?,?)",
                            (committee_id, int(start), int(end), tier))
                        range_count += 1
                    except ValueError:
                        pass
                else:
                    try:
                        sic_int = int(part)
                        conn.execute(
                            "INSERT INTO committee_sic_ranges VALUES (?,?,?,?)",
                            (committee_id, sic_int, sic_int, tier))
                        range_count += 1
                    except ValueError:
                        pass
        conn.commit()
        log.info(f"  committee_sic_ranges: {range_count} ranges expanded")
    else:
        log.warning(f"  {csv_path} not found, skipping")

    # Ticker → SIC mapping from SEC EDGAR
    sic_path = BASE_DIR / "sec_tickers" / "ticker_sic.json"
    if sic_path.exists():
        conn.execute("DELETE FROM ticker_sic")
        sic_data = json.loads(sic_path.read_text())
        sic_rows = [(r['ticker'], r.get('cik', ''), r.get('company_name', ''),
                     r.get('sic_code', ''), r.get('sic_description', ''),
                     r.get('exchange', '')) for r in sic_data.values()]
        conn.executemany("INSERT OR REPLACE INTO ticker_sic VALUES (?,?,?,?,?,?)", sic_rows)
        conn.commit()
        log.info(f"  ticker_sic: {len(sic_rows)} tickers with SIC codes")
    else:
        log.warning(f"  {sic_path} not found, skipping (run 20_sec_ticker_sic.py first)")


def build_lobbying_bill_refs(conn: sqlite3.Connection):
    """Extract bill references from lobbying activity specific_issues text."""
    log.info("Extracting bill references from lobbying activities...")

    # Pattern matches H.R. 1234, HR 1234, HR1234, S. 1234, S 1234, S1234,
    # H.J.Res. 56, S.J.Res. 56, H.Con.Res. 14, S.Con.Res. 14, H.Res. 99, S.Res. 99
    # and variants without dots/spaces
    bill_pattern = re.compile(
        r'\b('
        r'H\.?\s*J\.?\s*Res\.?\s*(\d+)'      # H.J.Res. / HJRes
        r'|S\.?\s*J\.?\s*Res\.?\s*(\d+)'      # S.J.Res. / SJRes
        r'|H\.?\s*Con\.?\s*Res\.?\s*(\d+)'    # H.Con.Res. / HConRes
        r'|S\.?\s*Con\.?\s*Res\.?\s*(\d+)'    # S.Con.Res. / SConRes
        r'|H\.?\s*Res\.?\s*(\d+)'             # H.Res. / HRes
        r'|S\.?\s*Res\.?\s*(\d+)'             # S.Res. / SRes
        r'|H\.?\s*R\.?\s*(\d+)'               # H.R. / HR
        r'|S\.?\s*(\d+)'                       # S. / S (must be last)
        r')',
        re.IGNORECASE
    )

    # Map group index to bill_type
    group_type_map = {
        2: 'hjres', 3: 'sjres', 4: 'hconres', 5: 'sconres',
        6: 'hres', 7: 'sres', 8: 'hr', 9: 's'
    }

    conn.execute("DELETE FROM lobbying_bills")
    conn.commit()

    cursor = conn.execute("""
        SELECT filing_uuid, filing_year, client_name, issue_code, specific_issues
        FROM lobbying_activities
        WHERE specific_issues IS NOT NULL
    """)

    batch = []
    total = 0
    rows_scanned = 0

    for row in cursor:
        filing_uuid, filing_year, client_name, issue_code, text = row
        rows_scanned += 1

        # Determine congress from filing year: 2023-2024 = 118th
        congress = (filing_year - 1789) // 2 + 1 if filing_year else None

        seen = set()  # deduplicate within this row
        for match in bill_pattern.finditer(text):
            bill_type = None
            bill_number = None
            for grp_idx, btype in group_type_map.items():
                val = match.group(grp_idx)
                if val:
                    bill_type = btype
                    bill_number = int(val)
                    break

            if bill_type and bill_number:
                bill_id = f"{congress}-{bill_type}-{bill_number}" if congress else None
                dedup_key = (bill_type, bill_number)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                batch.append((filing_uuid, congress, bill_type, bill_number,
                              bill_id, client_name, issue_code))

        if len(batch) >= 10000:
            conn.executemany(
                "INSERT INTO lobbying_bills (filing_uuid, bill_congress, bill_type, bill_number, bill_id, client_name, issue_code) VALUES (?,?,?,?,?,?,?)",
                batch
            )
            conn.commit()
            total += len(batch)
            batch = []

    if batch:
        conn.executemany(
            "INSERT INTO lobbying_bills (filing_uuid, bill_congress, bill_type, bill_number, bill_id, client_name, issue_code) VALUES (?,?,?,?,?,?,?)",
            batch
        )
        conn.commit()
        total += len(batch)

    log.info(f"  Scanned {rows_scanned:,} activity rows, extracted {total:,} bill references")


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

    # Legislation subject summary (replaces ~8.5s GROUP BY on legislation_subjects)
    conn.execute("DROP TABLE IF EXISTS legislation_subject_summary")
    conn.execute("""
        CREATE TABLE legislation_subject_summary AS
        SELECT subject, COUNT(*) AS bill_count
        FROM legislation_subjects
        GROUP BY subject
        ORDER BY bill_count DESC
    """)
    conn.commit()
    log.info(f"  legislation_subject_summary: {conn.execute('SELECT COUNT(*) FROM legislation_subject_summary').fetchone()[0]:,} rows")

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

    # GAO-Legislation crossref (links GAO reports to bills via public law numbers)
    conn.execute("DROP TABLE IF EXISTS gao_legislation")
    conn.execute("""
        CREATE TABLE gao_legislation AS
        SELECT DISTINCT
            g.package_id AS gao_package_id,
            g.title AS gao_title,
            g.date_issued AS gao_date,
            la.bill_id,
            REPLACE(TRIM(law_ref.value), 'Public Law ', '') AS public_law_number
        FROM gao_reports g,
            json_each('["' || REPLACE(g.public_laws, ', ', '","') || '"]') AS law_ref
        JOIN legislation_actions la
            ON la.action_text = 'Became Public Law No: ' || REPLACE(TRIM(law_ref.value), 'Public Law ', '') || '.'
        WHERE g.public_laws IS NOT NULL AND g.public_laws != ''
    """)
    conn.execute("CREATE INDEX idx_gao_leg_gao ON gao_legislation(gao_package_id)")
    conn.execute("CREATE INDEX idx_gao_leg_bill ON gao_legislation(bill_id)")
    conn.commit()
    log.info(f"  gao_legislation: {conn.execute('SELECT COUNT(*) FROM gao_legislation').fetchone()[0]:,} rows")

    # Earmark-Spending crossref (matches earmark recipients to federal spending awards)
    # Pre-compute normalized names with indexes for fast matching (15-20 min → <1 min)
    # Uses pre-computed recipient_normalized columns + indexes (no temp tables needed)
    conn.execute("DROP TABLE IF EXISTS earmark_spending_crossref")
    conn.execute("""
        CREATE TABLE earmark_spending_crossref AS
        SELECT
            e.id AS earmark_id,
            e.bioguide_id,
            e.member_name,
            e.fiscal_year AS earmark_fy,
            e.recipient AS earmark_recipient,
            e.amount_requested,
            e.project_description,
            s.generated_internal_id AS spending_id,
            s.recipient_name AS spending_recipient,
            s.award_amount,
            s.agency AS spending_agency,
            s.description AS spending_description,
            s.fiscal_year AS spending_fy,
            CASE
                WHEN e.amount_requested = CAST(s.award_amount AS INTEGER) THEN 'exact_amount'
                ELSE 'name_fy'
            END AS match_type
        FROM earmarks e
        JOIN spending_awards s
            ON e.recipient_normalized = s.recipient_name_normalized
            AND e.fiscal_year = s.fiscal_year
        WHERE e.recipient_normalized IS NOT NULL AND LENGTH(e.recipient_normalized) > 3
    """)
    conn.execute("CREATE INDEX idx_esc_earmark ON earmark_spending_crossref(earmark_id)")
    conn.execute("CREATE INDEX idx_esc_spending ON earmark_spending_crossref(spending_id)")
    conn.execute("CREATE INDEX idx_esc_bioguide ON earmark_spending_crossref(bioguide_id)")
    conn.execute("CREATE INDEX idx_esc_match ON earmark_spending_crossref(match_type)")
    conn.commit()
    log.info(f"  earmark_spending_crossref: {conn.execute('SELECT COUNT(*) FROM earmark_spending_crossref').fetchone()[0]:,} rows")

    # Pre-compute normalized lobby client aggregates for fast joins
    # Uses pre-computed client_name_normalized column
    log.info("  Pre-computing normalized lobby client names...")
    conn.execute("DROP TABLE IF EXISTS _tmp_lobby_clients")
    conn.execute("""
        CREATE TEMP TABLE _tmp_lobby_clients AS
        SELECT client_name_normalized AS norm_name,
            COUNT(DISTINCT filing_uuid) AS lobby_filings,
            SUM(income_amount) AS total_lobby_income,
            GROUP_CONCAT(DISTINCT issue_code) AS lobby_issues
        FROM lobbying_activities
        WHERE client_name_normalized IS NOT NULL AND client_name_normalized != ''
        GROUP BY client_name_normalized
    """)
    conn.execute("CREATE INDEX _tmp_lc_idx ON _tmp_lobby_clients(norm_name)")
    log.info(f"  Lobby client lookup: {conn.execute('SELECT COUNT(*) FROM _tmp_lobby_clients').fetchone()[0]:,} unique names")

    # Witness-lobbyist overlap (hearing witnesses who also lobby)
    conn.execute("DROP TABLE IF EXISTS witness_lobby_overlap")
    conn.execute("""
        CREATE TABLE witness_lobby_overlap AS
        SELECT
            hw.organization,
            COUNT(DISTINCT hw.package_id) AS hearings_testified,
            lc.lobby_filings,
            lc.total_lobby_income,
            MIN(h.date_issued) AS first_hearing,
            MAX(h.date_issued) AS last_hearing,
            lc.lobby_issues
        FROM hearing_witnesses hw
        JOIN hearings h ON hw.package_id = h.package_id
        JOIN _tmp_lobby_clients lc ON hw.organization_normalized = lc.norm_name
        WHERE hw.organization_normalized IS NOT NULL
        GROUP BY hw.organization_normalized
        ORDER BY hearings_testified DESC
    """)
    conn.execute("CREATE INDEX idx_wlo_org ON witness_lobby_overlap(organization)")
    conn.execute("CREATE INDEX idx_wlo_hearings ON witness_lobby_overlap(hearings_testified)")
    conn.commit()
    log.info(f"  witness_lobby_overlap: {conn.execute('SELECT COUNT(*) FROM witness_lobby_overlap').fetchone()[0]:,} rows")

    # Commenter-lobbyist overlap (orgs that comment on regs AND lobby)
    conn.execute("DROP TABLE IF EXISTS commenter_lobby_overlap")
    conn.execute("""
        CREATE TABLE commenter_lobby_overlap AS
        SELECT
            cd.organization,
            COUNT(DISTINCT cd.id) AS comments_filed,
            COUNT(DISTINCT cd.gov_agency) AS agencies_commented,
            lc.lobby_filings,
            lc.total_lobby_income,
            lc.lobby_issues
        FROM comment_details cd
        JOIN _tmp_lobby_clients lc ON cd.organization_normalized = lc.norm_name
        WHERE cd.organization_normalized IS NOT NULL
        GROUP BY cd.organization_normalized
        ORDER BY comments_filed DESC
    """)
    conn.execute("CREATE INDEX idx_clo_org ON commenter_lobby_overlap(organization)")
    conn.execute("CREATE INDEX idx_clo_comments ON commenter_lobby_overlap(comments_filed)")
    conn.commit()
    log.info(f"  commenter_lobby_overlap: {conn.execute('SELECT COUNT(*) FROM commenter_lobby_overlap').fetchone()[0]:,} rows")

    # Lobbying-to-legislation summary (bills most referenced in lobbying filings)
    conn.execute("DROP TABLE IF EXISTS lobbying_bill_summary")
    conn.execute("""
        CREATE TABLE lobbying_bill_summary AS
        SELECT lb.bill_id, l.title AS bill_title, l.policy_area,
            COUNT(DISTINCT lb.filing_uuid) AS lobby_filings,
            COUNT(DISTINCT lb.client_name) AS unique_clients,
            GROUP_CONCAT(DISTINCT lb.issue_code) AS issue_codes,
            GROUP_CONCAT(DISTINCT lb.client_name) AS clients
        FROM lobbying_bills lb
        LEFT JOIN legislation l ON lb.bill_id = l.bill_id
        WHERE lb.bill_id IS NOT NULL
        GROUP BY lb.bill_id
        ORDER BY lobby_filings DESC
    """)
    conn.execute("CREATE UNIQUE INDEX idx_lbs_bill ON lobbying_bill_summary(bill_id)")
    conn.commit()
    log.info(f"  lobbying_bill_summary: {conn.execute('SELECT COUNT(*) FROM lobbying_bill_summary').fetchone()[0]:,} rows")

    # Speeches near trades (pre-computed because live query takes 8s+)
    conn.execute("DROP TABLE IF EXISTS speeches_near_trades")
    conn.execute("""
        CREATE TABLE speeches_near_trades AS
        SELECT cm.full_name, cm.party, cm.state, cm.bioguide_id,
            st.transaction_date AS trade_date, st.ticker, st.asset_description,
            st.transaction_type, st.amount_range,
            cr.date AS speech_date, cr.title AS speech_title,
            ABS(CAST(julianday(st.transaction_date) - julianday(cr.date) AS INTEGER)) AS days_apart
        FROM stock_trades st
        JOIN crec_speakers cs ON st.bioguide_id = cs.bioguide_id
        JOIN congressional_record cr ON cs.granule_id = cr.granule_id
        JOIN congress_members cm ON st.bioguide_id = cm.bioguide_id
        WHERE st.ticker IS NOT NULL AND st.ticker != ''
            AND st.transaction_date IS NOT NULL AND cr.date IS NOT NULL
            AND ABS(julianday(st.transaction_date) - julianday(cr.date)) <= 7
        ORDER BY cm.full_name, st.transaction_date DESC
    """)
    conn.execute("CREATE INDEX idx_snt_member ON speeches_near_trades(bioguide_id)")
    conn.execute("CREATE INDEX idx_snt_ticker ON speeches_near_trades(ticker)")
    conn.commit()
    log.info(f"  speeches_near_trades: {conn.execute('SELECT COUNT(*) FROM speeches_near_trades').fetchone()[0]:,} rows")

    # Committee donor summary (pre-computed because live query takes 95s)
    # Deduplicate by (bioguide_id, donor committee) — members on parent + subcommittees
    # were previously counted multiple times for the same donation.
    conn.execute("DROP TABLE IF EXISTS committee_donor_summary")
    conn.execute("""
        CREATE TABLE committee_donor_summary AS
        SELECT GROUP_CONCAT(DISTINCT c.name) AS committee_name,
            cm.full_name AS member_name,
            cm.party, cm.state, cm.bioguide_id,
            fc.cmte_nm AS donor_committee, fc.cmte_id AS donor_cmte_id,
            SUM(fcon.transaction_amt) AS total_donated,
            COUNT(*) AS contribution_count
        FROM (SELECT DISTINCT bioguide_id, committee_id FROM committee_memberships) cmem
        JOIN committees c ON cmem.committee_id = c.committee_id
        JOIN congress_members cm ON cmem.bioguide_id = cm.bioguide_id
        JOIN fec_candidate_crosswalk xw ON xw.bioguide_id = cm.bioguide_id
        JOIN fec_contributions fcon ON fcon.cand_id = xw.fec_candidate_id
        JOIN fec_committees fc ON fc.cmte_id = fcon.cmte_id
        WHERE cm.is_current = 1
        GROUP BY cm.bioguide_id, fcon.cmte_id
        HAVING total_donated >= 10000
        ORDER BY total_donated DESC
    """)
    conn.execute("CREATE INDEX idx_cds_member ON committee_donor_summary(bioguide_id)")
    conn.execute("CREATE INDEX idx_cds_committee ON committee_donor_summary(committee_name)")
    conn.commit()
    log.info(f"  committee_donor_summary: {conn.execute('SELECT COUNT(*) FROM committee_donor_summary').fetchone()[0]:,} rows")

    # Committee-sector trade conflicts (members trading stocks in sectors their committee regulates)
    # Uses SIC-range matching via ticker_sic + committee_sic_ranges when available,
    # falls back to example_tickers matching from committee_sectors
    conn.execute("DROP TABLE IF EXISTS committee_trade_conflicts")
    try:
        has_sic = conn.execute("SELECT COUNT(*) FROM ticker_sic").fetchone()[0] > 0
        has_ranges = conn.execute("SELECT COUNT(*) FROM committee_sic_ranges").fetchone()[0] > 0
    except Exception:
        has_sic = False
        has_ranges = False

    try:
        if has_sic and has_ranges:
            # SIC-based matching (broad coverage)
            log.info("  Building committee_trade_conflicts via SIC-range matching...")
            conn.execute("""
                CREATE TABLE committee_trade_conflicts AS
                SELECT cm.full_name, cm.party, cm.state, cm.bioguide_id,
                    cj.committee_name, cj.jurisdiction_desc, cj.jurisdiction_tier,
                    ts.sic_code, ts.sic_description,
                    st.transaction_date, st.ticker, st.asset_description,
                    st.transaction_type, st.amount_range
                FROM committee_memberships cmem
                JOIN congress_members cm ON cmem.bioguide_id = cm.bioguide_id
                JOIN committee_sic_ranges csr ON cmem.committee_id = csr.committee_id
                JOIN committee_jurisdiction cj ON cmem.committee_id = cj.committee_id
                    AND cj.jurisdiction_tier = csr.jurisdiction_tier
                JOIN stock_trades st ON cmem.bioguide_id = st.bioguide_id
                JOIN ticker_sic ts ON UPPER(st.ticker) = ts.ticker
                WHERE CAST(ts.sic_code AS INTEGER) BETWEEN csr.sic_start AND csr.sic_end
                ORDER BY cm.full_name, st.transaction_date DESC
            """)
        else:
            # Fallback: example_tickers matching (limited coverage)
            log.info("  Building committee_trade_conflicts via ticker matching (no SIC data)...")
            conn.execute("""
                CREATE TABLE committee_trade_conflicts AS
                SELECT cm.full_name, cm.party, cm.state, cm.bioguide_id,
                    cs.committee_name, cs.regulated_sectors AS jurisdiction_desc,
                    'primary' AS jurisdiction_tier,
                    '' AS sic_code, '' AS sic_description,
                    st.transaction_date, st.ticker, st.asset_description,
                    st.transaction_type, st.amount_range
                FROM committee_memberships cmem
                JOIN congress_members cm ON cmem.bioguide_id = cm.bioguide_id
                JOIN committee_sectors cs ON cmem.committee_id = cs.committee_id
                JOIN stock_trades st ON cmem.bioguide_id = st.bioguide_id
                WHERE st.ticker IS NOT NULL AND st.ticker != ''
                    AND (',' || cs.example_tickers || ',') LIKE ('%,' || st.ticker || ',%')
                ORDER BY cm.full_name, st.transaction_date DESC
            """)
        conn.execute("CREATE INDEX idx_ctc_member ON committee_trade_conflicts(bioguide_id)")
        conn.execute("CREATE INDEX idx_ctc_ticker ON committee_trade_conflicts(ticker)")
        conn.execute("CREATE INDEX idx_ctc_committee ON committee_trade_conflicts(committee_name)")
        conn.commit()
        log.info(f"  committee_trade_conflicts: {conn.execute('SELECT COUNT(*) FROM committee_trade_conflicts').fetchone()[0]:,} rows")
    except Exception as e:
        log.warning(f"  committee_trade_conflicts: skipped ({e})")

    # Revolving door: former members of Congress who became lobbyists
    conn.execute("DROP TABLE IF EXISTS revolving_door")
    try:
        conn.execute("""
            CREATE TABLE revolving_door AS
            WITH position_filter AS (
                SELECT DISTINCT lobbyist_name, filing_uuid, covered_position
                FROM lobbying_lobbyists
                WHERE covered_position IS NOT NULL AND covered_position <> ''
                AND (
                    UPPER(covered_position) LIKE '%MEMBER OF CONGRESS%'
                    OR UPPER(covered_position) LIKE 'U.S. REPRESENTATIVE%'
                    OR UPPER(covered_position) LIKE 'U.S. SENATOR%'
                    OR UPPER(covered_position) LIKE 'US SENATOR%'
                    OR UPPER(covered_position) LIKE 'US REPRESENTATIVE%'
                    OR UPPER(covered_position) LIKE 'U.S. CONGRESSMAN%'
                    OR UPPER(covered_position) LIKE 'UNITED STATES REP%'
                    OR UPPER(covered_position) LIKE 'UNITED STATES SEN%'
                    OR UPPER(covered_position) LIKE 'FORMER MEMBER%'
                    OR UPPER(covered_position) LIKE 'MEMBER, U.S.%'
                    OR UPPER(covered_position) LIKE 'MEMBER U.S.%'
                    OR UPPER(covered_position) LIKE 'MEMBER OF THE U.S.%'
                )
            ),
            best_member AS (
                SELECT bioguide_id, full_name, party, state, chamber,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(full_name)
                        ORDER BY bioguide_id DESC
                    ) AS rn
                FROM congress_members
            )
            SELECT
                bm.bioguide_id,
                bm.full_name,
                bm.party,
                bm.state,
                bm.chamber AS congress_chamber,
                COUNT(DISTINCT la.filing_uuid) AS lobbying_filing_count,
                COUNT(DISTINCT la.client_name) AS client_count,
                COUNT(DISTINCT la.registrant_name) AS firm_count,
                MIN(la.filing_year) AS first_lobbying_year,
                MAX(la.filing_year) AS last_lobbying_year,
                SUM(COALESCE(la.income_amount, 0)) AS total_reported_income,
                GROUP_CONCAT(DISTINCT la.registrant_name) AS lobbying_firms,
                MIN(pf.covered_position) AS covered_position_sample
            FROM position_filter pf
            JOIN best_member bm ON UPPER(bm.full_name) = pf.lobbyist_name AND bm.rn = 1
            JOIN lobbying_activities la ON pf.filing_uuid = la.filing_uuid
            GROUP BY bm.bioguide_id
            ORDER BY lobbying_filing_count DESC
        """)
        conn.execute("CREATE INDEX idx_rd_bioguide ON revolving_door(bioguide_id)")
        conn.commit()
        log.info(f"  revolving_door: {conn.execute('SELECT COUNT(*) FROM revolving_door').fetchone()[0]:,} rows")
    except Exception as e:
        log.warning(f"  revolving_door: skipped ({e})")

    # FARA-Hearings overlap (FARA registrants who testified at congressional hearings)
    conn.execute("DROP TABLE IF EXISTS fara_hearing_overlap")
    try:
        conn.execute("""
            CREATE TABLE fara_hearing_overlap AS
            SELECT
                fr.name AS registrant_name,
                fr.registration_number,
                COUNT(DISTINCT hw.package_id) AS hearings_testified,
                COUNT(DISTINCT fp.country) AS countries_represented,
                GROUP_CONCAT(DISTINCT fp.country) AS countries,
                GROUP_CONCAT(DISTINCT fp.foreign_principal) AS foreign_principals,
                MIN(h.date_issued) AS first_hearing,
                MAX(h.date_issued) AS last_hearing,
                fr.registration_date,
                fr.termination_date
            FROM fara_registrants fr
            JOIN hearing_witnesses hw
                ON fr.name_normalized = hw.organization_normalized
            JOIN hearings h ON hw.package_id = h.package_id
            LEFT JOIN fara_foreign_principals fp
                ON fr.registration_number = fp.registration_number
            WHERE fr.name IS NOT NULL AND fr.name != ''
            GROUP BY fr.registration_number
            ORDER BY hearings_testified DESC
        """)
        conn.execute("CREATE INDEX idx_fho_reg ON fara_hearing_overlap(registration_number)")
        conn.execute("CREATE INDEX idx_fho_name ON fara_hearing_overlap(registrant_name)")
        conn.commit()
        log.info(f"  fara_hearing_overlap: {conn.execute('SELECT COUNT(*) FROM fara_hearing_overlap').fetchone()[0]:,} rows")
    except Exception as e:
        log.warning(f"  fara_hearing_overlap: skipped ({e})")

    # Committee trade counts (pre-computed for "Most Trades" sort on committee-trades page)
    # The live correlated subquery exceeds 30s SQL limit; this table makes it instant.
    conn.execute("DROP TABLE IF EXISTS committee_trade_counts")
    try:
        conn.execute("""
            CREATE TABLE committee_trade_counts AS
            SELECT c.name AS committee_name, c.committee_id,
                cm.full_name AS member_name, cm.party, cm.state, cm.bioguide_id,
                COUNT(*) AS trade_count,
                COUNT(DISTINCT st.ticker) AS unique_tickers,
                SUM(CASE WHEN st.transaction_type LIKE '%Purchase%' THEN 1 ELSE 0 END) AS purchases,
                SUM(CASE WHEN st.transaction_type LIKE '%Sale%' THEN 1 ELSE 0 END) AS sales,
                MIN(st.transaction_date) AS first_trade,
                MAX(st.transaction_date) AS last_trade
            FROM committee_memberships cmem
            JOIN committees c ON cmem.committee_id = c.committee_id
            JOIN congress_members cm ON cmem.bioguide_id = cm.bioguide_id
            JOIN stock_trades st ON cmem.bioguide_id = st.bioguide_id
            WHERE st.ticker IS NOT NULL AND st.ticker != ''
            GROUP BY cmem.committee_id, cmem.bioguide_id
            ORDER BY trade_count DESC
        """)
        conn.execute("CREATE INDEX idx_ctcnt_committee ON committee_trade_counts(committee_id)")
        conn.execute("CREATE INDEX idx_ctcnt_member ON committee_trade_counts(bioguide_id)")
        conn.execute("CREATE INDEX idx_ctcnt_trades ON committee_trade_counts(trade_count DESC)")
        conn.commit()
        log.info(f"  committee_trade_counts: {conn.execute('SELECT COUNT(*) FROM committee_trade_counts').fetchone()[0]:,} rows")
    except Exception as e:
        log.warning(f"  committee_trade_counts: skipped ({e})")

    # Docket importance score — uses docket_stats (already materialized) + doc type stats
    conn.execute("DROP TABLE IF EXISTS docket_importance")
    try:
        # Pre-aggregate document type stats (fast since documents table has docket_id index)
        conn.execute("DROP TABLE IF EXISTS _tmp_doc_type_stats")
        conn.execute("""
            CREATE TEMP TABLE _tmp_doc_type_stats AS
            SELECT docket_id,
                COUNT(*) as total_docs,
                COUNT(DISTINCT document_type) as doc_types,
                SUM(CASE WHEN document_type = 'Proposed Rule' THEN 1 ELSE 0 END) as proposed_rules,
                SUM(CASE WHEN document_type = 'Rule' THEN 1 ELSE 0 END) as final_rules
            FROM documents WHERE docket_id IS NOT NULL GROUP BY docket_id
        """)
        conn.execute("CREATE INDEX _tmp_dts_docket ON _tmp_doc_type_stats(docket_id)")

        conn.execute("DROP TABLE IF EXISTS _tmp_fr_stats")
        conn.execute("""
            CREATE TEMP TABLE _tmp_fr_stats AS
            SELECT doc.docket_id, COUNT(DISTINCT xref.fr_document_number) as fr_links
            FROM fr_regs_crossref xref
            JOIN documents doc ON doc.id = xref.regs_document_id
            WHERE doc.docket_id IS NOT NULL
            GROUP BY doc.docket_id
        """)
        conn.execute("CREATE INDEX _tmp_fs_docket ON _tmp_fr_stats(docket_id)")

        conn.execute("""
            CREATE TABLE docket_importance AS
            SELECT d.id AS docket_id, d.agency_id, d.title,
                COALESCE(ds.total_docs, 0) AS total_docs,
                COALESCE(ds.doc_types, 0) AS doc_type_count,
                COALESCE(ds.proposed_rules, 0) AS proposed_rules,
                COALESCE(ds.final_rules, 0) AS final_rules,
                COALESCE(dst.comment_count, 0) AS comment_count,
                COALESCE(fs.fr_links, 0) AS fr_crossref_count,
                CAST(
                    CASE WHEN COALESCE(ds.proposed_rules,0) > 0 AND COALESCE(ds.final_rules,0) > 0 THEN 10 ELSE 0 END
                    + CASE WHEN COALESCE(ds.final_rules,0) > 0 THEN 5 ELSE 0 END
                    + ROUND(LOG10(COALESCE(dst.comment_count,0) + 1) * 3, 1)
                    + ROUND(LOG10(COALESCE(ds.total_docs,0) + 1) * 2, 1)
                    + COALESCE(ds.doc_types, 0) * 2
                    + CASE WHEN COALESCE(fs.fr_links,0) > 0 THEN 3 ELSE 0 END
                AS INTEGER) AS importance_score
            FROM dockets d
            LEFT JOIN _tmp_doc_type_stats ds ON ds.docket_id = d.id
            LEFT JOIN docket_stats dst ON dst.id = d.id
            LEFT JOIN _tmp_fr_stats fs ON fs.docket_id = d.id
            WHERE d.title IS NOT NULL
        """)
        conn.execute("DROP TABLE IF EXISTS _tmp_doc_type_stats")
        conn.execute("DROP TABLE IF EXISTS _tmp_fr_stats")
        conn.execute("CREATE INDEX idx_di_score ON docket_importance(importance_score DESC)")
        conn.execute("CREATE INDEX idx_di_agency ON docket_importance(agency_id)")
        conn.execute("CREATE INDEX idx_di_docket ON docket_importance(docket_id)")
        conn.commit()
        log.info(f"  docket_importance: {conn.execute('SELECT COUNT(*) FROM docket_importance').fetchone()[0]:,} rows")
    except Exception as e:
        log.warning(f"  docket_importance: skipped ({e})")

    log.info("  Summary tables built")


def build_fts(conn: sqlite3.Connection):
    """Build full-text search indexes."""
    log.info("Building FTS5 indexes...")
    conn.executescript(FTS_SCHEMA)

    for table in ["federal_register_fts", "documents_fts", "comments_fts",
                   "cfr_fts", "crec_fts", "lobbying_fts",
                   "spending_awards_fts", "legislation_fts",
                   "fara_registrants_fts", "fara_foreign_principals_fts",
                   "fec_employer_fts",
                   "hearings_fts", "crs_reports_fts", "nominations_fts",
                   "gao_reports_fts", "earmarks_fts",
                   "entities_fts", "entity_aliases_fts", "public_actors_fts",
                   "senate_office_totals_fts", "house_pre2016_office_totals_fts",
                   "house_disbursements_nonpersonnel_fts",
                   "wh_visits_fts",
                   "oge_pas_filings_fts", "oge_pas_transactions_fts"]:
        try:
            source_map = {'cfr_fts': 'cfr_sections', 'crec_fts': 'congressional_record', 'lobbying_fts': 'lobbying_activities', 'spending_awards_fts': 'spending_awards', 'legislation_fts': 'legislation', 'fara_registrants_fts': 'fara_registrants', 'fara_foreign_principals_fts': 'fara_foreign_principals', 'fec_employer_fts': 'fec_employer_totals', 'hearings_fts': 'hearings', 'crs_reports_fts': 'crs_reports', 'nominations_fts': 'nominations', 'gao_reports_fts': 'gao_reports', 'earmarks_fts': 'earmarks'}
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


def validate_critical_tables(conn: sqlite3.Connection):
    """Fail-loud post-build check that every critical table exists and has
    at least a lower-bound row count. Catches silent failures where a build
    swallows an error in one phase but deploys a mutilated DB anyway.

    Uncovered history: from ~2026-04-19 through 2026-04-21, every fresh
    openregs.db build silently lost `government_units` + `bmf_group_*` tables
    due to an unqualified `DROP TABLE IF EXISTS` that fell through to the
    (read-write-attached) source DB, destroying the source. `executescript`
    then failed, but `weekly_update.sh` runs with `set -uo pipefail` (no -e)
    so the deploy shipped the broken DB. This check makes that class of
    failure impossible to miss.

    Raises SystemExit(5) on any missing/undersized critical table so the
    build aborts and deploy is skipped.
    """
    log.info("\n" + "=" * 60)
    log.info("CRITICAL TABLES VALIDATION")
    log.info("=" * 60)
    # (table_name, min_rows) — thresholds set at ~70% of current production
    # so natural data-pull variance doesn't trip them.
    critical = [
        ("federal_register", 900_000),
        ("dockets", 200_000),
        ("documents", 1_500_000),
        ("comments", 9_000_000),
        ("cfr_sections", 100_000),
        ("legislation", 370_000),
        ("congress_members", 12_000),
        ("committees", 200),
        ("spending_awards", 800_000),
        ("congressional_record", 800_000),
        ("fec_operating_expenditures", 15_000_000),
        ("oira_reviews", 40_000),
        ("lobbying_filings", 1_500_000),
        ("government_units", 80_000),     # wiped silently pre-2026-04-21
        ("bmf_group_members", 350_000),   # same bug class
        # Staging-imports (2026-04-21+)
        ("entities", 2_000_000),
        ("public_actors", 10_000_000),
        ("entity_aliases", 400_000),
        ("wh_visits", 7_000_000),
        ("senate_office_totals", 20_000),
        ("house_pre2016_office_totals", 200_000),
        ("oge_pas_filings", 700),
    ]
    failures = []
    for table, min_rows in critical:
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        ).fetchone()[0]
        if not exists:
            failures.append(f"MISSING: {table}")
            log.error(f"  ✗ {table}: table does not exist")
            continue
        try:
            n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        except sqlite3.OperationalError as e:
            failures.append(f"UNREADABLE: {table} ({e})")
            log.error(f"  ✗ {table}: {e}")
            continue
        if n < min_rows:
            failures.append(f"UNDERCOUNT: {table} has {n:,} rows (expected ≥ {min_rows:,})")
            log.error(f"  ✗ {table}: {n:,} rows (expected ≥ {min_rows:,})")
        else:
            log.info(f"  ✓ {table}: {n:,}")
    if failures:
        log.error("\n" + "=" * 60)
        log.error(f"CRITICAL TABLES VALIDATION FAILED: {len(failures)} issue(s)")
        for f in failures:
            log.error(f"  - {f}")
        log.error("Aborting before save_build_report/deploy — do NOT ship this DB.")
        log.error("=" * 60)
        raise SystemExit(5)
    log.info(f"  All {len(critical)} critical tables present and sized OK.")


def validate_dates(conn: sqlite3.Connection):
    """Post-build sanity check on date columns.

    Warns (does not fail) on:
    - Dates outside [1900, 2050] in any table/column (catches typos like 2205
      postmark or 2999 comment date, plus non-date strings in date columns
      like "UKRAINE" that landed in fara_foreign_principals.fp_registration_date)
    - `policy_area` coverage for current-year bills below 70% (catches
      Congress.gov pipeline regressions like the 2026-04-19 audit found)

    Rationale: caught dozens of source-side data errors (1905, 1940, 2029 typos)
    during the expansion-pull audit and keeps future runs from silently ingesting
    malformed dates. Separate from generate_audit_report() which is comprehensive;
    this is a targeted sanity-guard.
    """
    log.info("\n" + "=" * 60)
    log.info("DATE SANITY + COVERAGE VALIDATION")
    log.info("=" * 60)

    # 1. Date-range outliers across all tables with date-like columns
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%_fts%' "
        "AND name NOT LIKE '%_data' AND name NOT LIKE '%_idx' AND name NOT LIKE '%_content' "
        "AND name NOT LIKE '%_config' AND name NOT LIKE '%_docsize'"
    ).fetchall()]

    outlier_findings = []
    for t in tables:
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{t}")').fetchall()]
        for col in cols:
            if 'date' not in col.lower() or col.endswith('_iso'):
                continue
            # Sample to determine format (YYYY-prefix vs MM/DD/YYYY)
            try:
                sample = conn.execute(
                    f'SELECT "{col}" FROM "{t}" WHERE "{col}" IS NOT NULL AND "{col}" != "" LIMIT 1'
                ).fetchone()
                if not sample:
                    continue
                s = str(sample[0])
                # Pick extract based on format
                if s[:4].isdigit() and len(s) >= 4:
                    extract = f'SUBSTR("{col}", 1, 4)'
                elif len(s) >= 10 and s[2] in '/-' and s[5] in '/-':
                    extract = f'SUBSTR("{col}", -4, 4)'
                else:
                    continue  # unknown format — skip
                # Count outliers
                n_outlier = conn.execute(
                    f'SELECT COUNT(*) FROM "{t}" WHERE "{col}" IS NOT NULL AND "{col}" != "" '
                    f'AND ({extract} NOT GLOB "[0-9][0-9][0-9][0-9]" OR '
                    f'CAST({extract} AS INTEGER) < 1900 OR CAST({extract} AS INTEGER) > 2050)'
                ).fetchone()[0]
                if n_outlier > 0:
                    # Get samples
                    examples = conn.execute(
                        f'SELECT DISTINCT "{col}" FROM "{t}" WHERE "{col}" IS NOT NULL AND "{col}" != "" '
                        f'AND ({extract} NOT GLOB "[0-9][0-9][0-9][0-9]" OR '
                        f'CAST({extract} AS INTEGER) < 1900 OR CAST({extract} AS INTEGER) > 2050) LIMIT 3'
                    ).fetchall()
                    outlier_findings.append((t, col, n_outlier, [e[0] for e in examples]))
            except Exception:
                pass

    if outlier_findings:
        log.warning(f"\n  ⚠  {len(outlier_findings)} (table, column) pairs have date outliers:")
        for t, col, n, samples in outlier_findings:
            log.warning(f"    {t}.{col}: {n:,} rows outside [1900, 2050]. e.g. {samples}")
    else:
        log.info("  ✓ No date outliers outside [1900, 2050] across any table.")

    # 2. policy_area coverage for current-year bills (catches Congress.gov pipeline breaks)
    from datetime import datetime
    current_year = str(datetime.now().year)
    try:
        totals = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN policy_area IS NOT NULL AND policy_area != '' THEN 1 ELSE 0 END) "
            "FROM legislation WHERE substr(introduced_date, 1, 4) = ?", (current_year,)
        ).fetchone()
        total, with_pa = totals[0] or 0, totals[1] or 0
        if total == 0:
            log.warning(f"  ⚠  No bills in legislation for introduced_date year {current_year} — pipeline may be stalled")
        else:
            pct = with_pa / total * 100
            if pct < 70:
                log.warning(f"  ⚠  policy_area coverage for {current_year} bills: {with_pa:,}/{total:,} ({pct:.0f}%) — below 70% threshold")
            else:
                log.info(f"  ✓ policy_area coverage for {current_year} bills: {with_pa:,}/{total:,} ({pct:.0f}%)")
    except sqlite3.Error as e:
        log.warning(f"  policy_area coverage check failed: {e}")


def validate_freshness(conn: sqlite3.Connection):
    """Post-build freshness check: surface tables whose newest row is too old.

    Warn-only (never aborts the build) — old data is still useful, the build
    succeeded structurally, and aborting would prevent the rest of the build
    from shipping. The job here is to surface that ingestion broke at the
    source, so weekly_update.sh can ping a dedicated "operational health"
    hc.io check (separate from the deploy-broken main check).

    History: added 2026-04-25 after `01_federal_register.py` silently went
    14+ days stale due to a state-file bug that marked the current month
    "complete" too aggressively. The bug had been live since at least
    Apr 11; nobody noticed until a manual site audit on Apr 25 surfaced it.
    A freshness check at build time would have flagged it the next Saturday
    after the bug landed. See bestpractices/pipeline_verification.md
    "## The 2026-04-25 incident" for context.

    Threshold rationale: each table's window is generous enough to absorb
    a single missed Saturday + normal upstream cadence quirks. A table
    crossing its threshold doesn't mean prod is broken — it means we should
    investigate the upstream ingest before next Saturday. If a table is
    genuinely catastrophically stale (e.g., FR > 60 days), the
    "FRESHNESS_WARNING" log marker is still emitted and aggregated by the
    operational-health check; the build never aborts on freshness alone.
    """
    log.info("\n" + "=" * 60)
    log.info("FRESHNESS VALIDATION")
    log.info("=" * 60)

    # (table, date_column, max_age_days, why_this_window)
    expectations = [
        ("federal_register", "publication_date", 14,
         "FR publishes M-F; 14d covers a missed Saturday + 1 week"),
        ("comments", "posted_date", 14,
         "Regs.gov comments daily; 14d covers a missed Saturday"),
        ("congressional_record", "date", 30,
         "session days only; 30d covers normal recess + 1 missed Saturday"),
        ("legislation_actions", "action_date", 14,
         "actions during session; 14d covers brief recess + 1 missed Sat"),
        ("spending_awards", "start_date", 90,
         "USAspending: lagging indicator, 90d typical lag from action to publish"),
        # fec_operating_expenditures excluded: transaction_dt is MM/DD/YYYY
        # not ISO, so SQLite's julianday() can't parse it for the age math.
        # Source also contains clearly-bad dates (1416, 2114). Re-add when
        # we either normalize to ISO at ingest or add MDY-aware comparison.
        ("stock_trades", "transaction_date", 60,
         "STOCK Act 45-day disclosure window + 15d safety"),
        ("legislation", "introduced_date", 14,
         "Bill introductions; 14d covers a missed Saturday + recess week"),
        ("presidential_documents", "publication_date", 30,
         "Sporadic; 30d covers normal cadence"),
    ]

    findings = []
    for table, date_col, max_age_days, _why in expectations:
        try:
            # Clamp to today: future-dated entries (e.g., FEC dates the week
            # after the filing, USAspending action dates set forward by months,
            # SEC stock_trades with mis-typed years) would otherwise let MAX
            # return a future date and the freshness check pass falsely.
            # We want "what is the freshest *real* observation we have" —
            # if every recent ingest landed only future-dated rows, that's
            # still a freshness gap.
            row = conn.execute(
                f"SELECT MAX({date_col}), COUNT(*) FROM {table} "
                f"WHERE {date_col} <= date('now')"
            ).fetchone()
            latest, n = row[0], row[1]
            if not latest or n == 0:
                findings.append((table, date_col, "NO_DATA", n, max_age_days))
                continue
            age_days = conn.execute(
                "SELECT CAST(julianday('now') - julianday(?) AS INTEGER)",
                (latest,)
            ).fetchone()[0]
            if age_days is None:
                # Date string unparseable by julianday (e.g. MM/DD/YYYY).
                # validate_dates handles outliers; here we log so future-Tony
                # knows the freshness check silently skipped this table.
                log.info(
                    f"  - {table}.{date_col} skipped (date format not "
                    f"julianday-parseable; latest raw value was {latest!r})"
                )
                continue
            if age_days > max_age_days:
                findings.append((table, date_col, latest, age_days, max_age_days))
                log.warning(
                    f"  ⚠  FRESHNESS_WARNING: {table}.{date_col} latest={latest} "
                    f"({age_days}d old, threshold={max_age_days}d)"
                )
            else:
                log.info(
                    f"  ✓ {table}.{date_col} latest={latest} "
                    f"({age_days}d old, threshold={max_age_days}d)"
                )
        except sqlite3.Error as e:
            log.warning(f"  freshness check failed for {table}.{date_col}: {e}")

    if findings:
        log.warning(
            f"\n  FRESHNESS_WARNING: {len(findings)} table(s) past expected freshness window."
        )
        log.warning(
            "  This is non-fatal. Investigate upstream ingest before next Saturday."
        )
    else:
        log.info(f"\n  ✓ All {len(expectations)} freshness expectations met.")


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
        ("cbo_cost_estimates", "CBO cost estimates"),
        ("stock_trades", "Stock trades"),
        ("lobbying_filings", "Lobbying filings"),
        ("lobbying_activities", "Lobbying activities"),
        ("lobbying_bills", "Lobbying bill references"),
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
        ("fec_operating_expenditures", "FEC operating expenditures"),
        ("fec_pac_summary", "FEC PAC summary"),
        ("fec_independent_expenditures", "FEC independent expenditures"),
        ("fec_electioneering", "FEC electioneering communications"),
        ("fec_communication_costs", "FEC communication costs"),
        ("oira_reviews", "OIRA regulatory reviews"),
        ("oira_meetings", "OIRA meetings"),
        ("oira_meeting_attendees", "OIRA meeting attendees"),
        ("hearings", "Committee hearings"),
        ("hearing_witnesses", "Hearing witnesses"),
        ("hearing_members", "Hearing members"),
        ("crs_reports", "CRS reports"),
        ("crs_report_bills", "CRS report bill references"),
        ("nominations", "Executive nominations"),
        ("nomination_actions", "Nomination actions"),
        ("treaties", "Treaties"),
        ("treaty_actions", "Treaty actions"),
        ("gao_reports", "GAO reports"),
        ("ig_reports", "IG reports"),
        ("ig_recommendations", "IG recommendations"),
        ("earmarks", "Earmarks/CDS"),
        ("fara_registrants", "FARA registrants"),
        ("fara_foreign_principals", "FARA foreign principals"),
        ("fara_short_forms", "FARA short forms (agents)"),
        ("fara_registrant_docs", "FARA registrant documents"),
        ("witness_lobby_overlap", "Witness-lobbyist overlap"),
        ("commenter_lobby_overlap", "Commenter-lobbyist overlap"),
        ("lobbying_bill_summary", "Lobbying bill summary"),
        ("speeches_near_trades", "Speeches near trades"),
        ("committee_donor_summary", "Committee donor summary"),
        ("committee_trade_conflicts", "Committee trade conflicts"),
        ("revolving_door", "Revolving door (members→lobbyists)"),
        ("fara_hearing_overlap", "FARA-hearings overlap"),
        ("committee_trade_counts", "Committee trade counts"),
        ("committee_sectors", "Committee sector reference"),
        ("committee_jurisdiction", "Committee jurisdiction (tiered)"),
        ("committee_sic_ranges", "Committee SIC ranges"),
        ("ticker_sic", "Ticker SIC codes (SEC EDGAR)"),
        ("lobbying_issue_agencies", "Lobbying issue-agency reference"),
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


def save_build_report(conn: sqlite3.Connection, elapsed_seconds: float):
    """Save a build report with table counts, deltas from previous build, and metadata."""
    import json
    from datetime import datetime, timezone

    report_dir = BASE_DIR / "build_reports"
    report_dir.mkdir(exist_ok=True)

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = now.strftime("%Y%m%d_%H%M%S")

    # Collect all table counts
    table_counts = {}
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ):
        table_name = row[0]
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
            table_counts[table_name] = count
        except Exception:
            table_counts[table_name] = -1

    db_size_bytes = DB_PATH.stat().st_size
    total_rows = sum(c for c in table_counts.values() if c > 0)

    report = {
        "timestamp": timestamp,
        "build_duration_seconds": round(elapsed_seconds, 1),
        "build_duration_minutes": round(elapsed_seconds / 60, 1),
        "database_size_bytes": db_size_bytes,
        "database_size_gb": round(db_size_bytes / (1024**3), 2),
        "total_tables": len(table_counts),
        "total_rows": total_rows,
        "tables": table_counts,
    }

    # Load previous report for deltas
    prev_reports = sorted(report_dir.glob("*.json"))
    prev_report = None
    if prev_reports:
        try:
            prev_report = json.loads(prev_reports[-1].read_text())
        except Exception:
            pass

    if prev_report and "tables" in prev_report:
        prev_tables = prev_report["tables"]
        deltas = {}
        for table, count in table_counts.items():
            if count < 0:
                continue
            prev_count = prev_tables.get(table, 0)
            if prev_count < 0:
                prev_count = 0
            delta = count - prev_count
            if delta != 0:
                deltas[table] = {"previous": prev_count, "current": count, "delta": delta}
        # New tables
        new_tables = [t for t in table_counts if t not in prev_tables and table_counts[t] > 0]
        # Removed tables
        removed_tables = [t for t in prev_tables if t not in table_counts]

        report["deltas"] = deltas
        report["new_tables"] = new_tables
        report["removed_tables"] = removed_tables
        report["previous_build"] = prev_report.get("timestamp", "unknown")
        report["previous_total_rows"] = prev_report.get("total_rows", 0)
        report["row_delta"] = total_rows - prev_report.get("total_rows", 0)
        report["size_delta_gb"] = round(
            (db_size_bytes - prev_report.get("database_size_bytes", 0)) / (1024**3), 2
        )

    # Save JSON
    json_path = report_dir / f"{filename}.json"
    json_path.write_text(json.dumps(report, indent=2))
    log.info(f"\nBuild report saved: {json_path}")

    # Save human-readable summary
    lines = [
        f"# OpenRegs Build Report — {now.strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"**Build time**: {report['build_duration_minutes']} minutes",
        f"**Database size**: {report['database_size_gb']} GB",
        f"**Total tables**: {report['total_tables']}",
        f"**Total rows**: {total_rows:,}",
        "",
    ]

    if prev_report:
        lines.append(f"**Previous build**: {report.get('previous_build', 'unknown')}")
        row_delta = report.get("row_delta", 0)
        sign = "+" if row_delta >= 0 else ""
        lines.append(f"**Row change**: {sign}{row_delta:,}")
        lines.append(f"**Size change**: {sign}{report.get('size_delta_gb', 0)} GB")
        lines.append("")

        if report.get("new_tables"):
            lines.append("## New Tables")
            for t in report["new_tables"]:
                lines.append(f"- `{t}`: {table_counts[t]:,} rows")
            lines.append("")

        if report.get("removed_tables"):
            lines.append("## Removed Tables")
            for t in report["removed_tables"]:
                lines.append(f"- `{t}`")
            lines.append("")

        changed = {k: v for k, v in report.get("deltas", {}).items()
                   if k not in report.get("new_tables", [])}
        if changed:
            lines.append("## Changed Tables")
            lines.append("| Table | Previous | Current | Delta |")
            lines.append("|-------|----------|---------|-------|")
            for t, d in sorted(changed.items(), key=lambda x: abs(x[1]["delta"]), reverse=True):
                sign = "+" if d["delta"] >= 0 else ""
                lines.append(f"| `{t}` | {d['previous']:,} | {d['current']:,} | {sign}{d['delta']:,} |")
            lines.append("")

    lines.append("## All Tables")
    lines.append("| Table | Rows |")
    lines.append("|-------|------|")
    for t, c in sorted(table_counts.items()):
        if c >= 0:
            lines.append(f"| `{t}` | {c:,} |")
        else:
            lines.append(f"| `{t}` | (error) |")

    summary_path = report_dir / f"{filename}.md"
    summary_path.write_text("\n".join(lines))
    log.info(f"Build summary saved: {summary_path}")

    # Also save as "latest" for easy access
    (report_dir / "latest.json").write_text(json.dumps(report, indent=2))
    (report_dir / "latest.md").write_text("\n".join(lines))


def generate_audit_report(conn: sqlite3.Connection):
    """Generate a comprehensive data quality audit report after build.

    Runs coverage checks, linkage quality, date ranges, and data integrity
    queries across all tables. Produces a graded markdown report with rubric,
    gaps & roadmap, latest record dates, and coverage percentages.
    Saved to build_reports/audit_YYYYMMDD.md and build_reports/audit_latest.md.
    """
    import time as _time
    from datetime import datetime, timezone

    audit_start = _time.time()

    log.info("\n" + "=" * 60)
    log.info("GENERATING DATABASE AUDIT REPORT")
    log.info("=" * 60)

    report_dir = BASE_DIR / "build_reports"
    report_dir.mkdir(exist_ok=True)
    now = datetime.now(timezone.utc)

    GRADING_RUBRIC = (
        "## Grading Rubric\n\n"
        "| Grade | Meaning |\n"
        "|-------|---------|\n"
        "| **A+** | Comprehensive coverage, clean data, no known gaps, actively maintained |\n"
        "| **A** | Clean and well-structured; minor coverage gaps that don't undermine utility |\n"
        "| **B+** | Solid dataset with known structural limitations (missing time periods, partial coverage) |\n"
        "| **B** | Usable but with significant coverage gaps or staleness |\n"
        "| **C** | Collection in progress — data quality is fine but coverage incomplete due to external constraints |\n"
    )

    CONNECTION_DESCRIPTIONS = {
        "Speeches Near Trades": "Floor speeches within 7 days of a member's stock trade",
        "Committee Donor Summary": "Campaign contributions to members by committee jurisdiction",
        "Lobbying Bill Summary": "Bills referenced in lobbying activity reports with spending totals",
        "Witness-Lobby Overlap": "Hearing witnesses whose organizations also appear in lobbying disclosures",
        "Committee Trade Conflicts": "Stock trades by committee members in sectors they oversee",
        "Commenter-Lobby Overlap": "Regulatory commenters whose organizations also lobby on the same issues",
        "Revolving Door": "Individuals appearing in both government roles and lobbying disclosures",
    }

    def q1(sql):
        """Return single value."""
        return conn.execute(sql).fetchone()[0]

    def qrow(sql):
        """Return single row as dict."""
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
        return dict(zip(cols, row)) if row else {}

    def qall(sql):
        """Return all rows as list of dicts."""
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def safe_date(val):
        if not val:
            return ""
        return str(val)[:10]

    db_size_bytes = DB_PATH.stat().st_size
    db_size_gb = round(db_size_bytes / (1024**3), 1)
    total_tables = q1("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")

    # ── Collect all metrics ──────────────────────────────────────────

    sections = []  # list of (title, grade, rows, coverage, notes, latest_record)

    # --- Federal Register ---
    fr = qrow("SELECT COUNT(*) AS cnt, MIN(pub_year) AS mn, MAX(pub_year) AS mx, MAX(publication_date) AS latest FROM federal_register")
    fr_nulltype = q1("SELECT COUNT(*) FROM federal_register WHERE type IS NULL OR type = ''")
    sections.append((
        "Federal Register", "A+", f"{fr['cnt']:,}",
        f"{fr['mn']}–{fr['mx']} ({fr['mx'] - fr['mn'] + 1} years)",
        f"{fr_nulltype} records with null type" if fr_nulltype > 0 else "Complete",
        safe_date(fr['latest']),
    ))

    # --- Regulatory dockets ---
    dk = qrow("SELECT COUNT(*) AS cnt, SUM(CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END) AS titled, MAX(last_modified) AS latest FROM dockets")
    dk_pct = round(100 * dk['titled'] / dk['cnt'], 1) if dk['cnt'] else 0
    sections.append((
        "Regulatory Dockets", "A",
        f"{dk['cnt']:,}",
        f"{dk_pct}% have full metadata",
        f"{dk['cnt'] - dk['titled']:,} stub records (backfilled from comments/docs)",
        safe_date(dk['latest']),
    ))

    # --- Regulatory documents ---
    doc = qrow("SELECT COUNT(*) AS cnt, MAX(posted_date) AS latest FROM documents")
    sections.append((
        "Regulatory Documents", "A", f"{doc['cnt']:,}",
        "5 agencies (FWS, EPA, FDA, APHIS, USDA — selected for volume and policy relevance)",
        "",
        safe_date(doc['latest']),
    ))

    # --- Comments ---
    cm = qall("SELECT agency_id, COUNT(*) AS cnt FROM comments GROUP BY agency_id ORDER BY cnt DESC")
    cm_total = sum(r['cnt'] for r in cm)
    cm_detail = q1("SELECT COUNT(*) FROM comment_details")
    cm_pct = round(100 * cm_detail / cm_total, 2) if cm_total else 0
    cm_agencies = ", ".join(f"{r['agency_id']} {r['cnt']:,}" for r in cm)
    cm_latest = q1("SELECT MAX(posted_date) FROM comments")
    sections.append((
        "Public Comment Headers", "A",
        f"{cm_total:,}",
        f"{len(cm)} agencies: {cm_agencies}",
        "",
        safe_date(cm_latest),
    ))
    sections.append((
        "Comment Full Text", "C",
        f"{cm_detail:,}",
        f"{cm_pct}% of headers",
        "Rate-limited at ~780 req/hr. Expanding.",
        "",
    ))

    # --- Presidential Documents ---
    pd = qrow("""
        SELECT COUNT(*) AS cnt, MIN(signing_date) AS mn, MAX(signing_date) AS mx,
               SUM(CASE WHEN document_type='executive_order' THEN 1 ELSE 0 END) AS eos,
               SUM(CASE WHEN document_type='proclamation' THEN 1 ELSE 0 END) AS procs
        FROM presidential_documents
    """)
    sections.append((
        "Presidential Documents", "A+",
        f"{pd['cnt']:,}",
        f"{pd['mn'][:4]}–{pd['mx'][:4]} ({pd['eos']:,} EOs + {pd['procs']:,} proclamations)",
        "Complete and current",
        safe_date(pd['mx']),
    ))

    # --- CFR ---
    cfr = qrow("SELECT COUNT(*) AS cnt, COUNT(DISTINCT title_number) AS titles FROM cfr_sections")
    sections.append((
        "CFR Regulatory Text", "A",
        f"{cfr['cnt']:,} sections",
        f"{cfr['titles']} of 50 CFR titles ({round(100*cfr['titles']/50)}%)",
        "",
        "",
    ))

    # --- Cross-references ---
    xref = q1("SELECT COUNT(*) FROM fr_regs_crossref")
    sections.append(("FR ↔ Regs.gov Cross-Refs", "A", f"{xref:,}", "", "", ""))

    # --- Congressional Record ---
    crec = qrow("SELECT COUNT(*) AS cnt, MIN(date) AS mn, MAX(date) AS mx FROM congressional_record")
    spk = qrow("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN bioguide_id IS NOT NULL THEN 1 ELSE 0 END) AS linked
        FROM crec_speakers
    """)
    spk_pct = round(100 * spk['linked'] / spk['total'], 1) if spk['total'] else 0
    crec_bills = q1("SELECT COUNT(*) FROM crec_bills")
    sections.append((
        "Congressional Record", "A+",
        f"{crec['cnt']:,} entries, {spk['total']:,} speakers, {crec_bills:,} bill refs",
        f"{crec['mn']}–{crec['mx']}",
        f"{spk_pct}% speaker bioguide linkage",
        safe_date(crec['mx']),
    ))

    # --- Congress Members ---
    mem = qrow("SELECT COUNT(*) AS cnt, SUM(is_current) AS current FROM congress_members")
    sections.append((
        "Congress Members", "A+",
        f"{mem['cnt']:,} ({mem['current']} current)",
        "Historical + current",
        "Universal bioguide_id linkage across all tables",
        "",
    ))

    # --- Legislation ---
    leg = qrow("SELECT COUNT(*) AS cnt, MIN(congress) AS mn, MAX(congress) AS mx FROM legislation")
    cosp = q1("SELECT COUNT(*) FROM legislation_cosponsors")
    subj = q1("SELECT COUNT(*) FROM legislation_subjects")
    act = q1("SELECT COUNT(*) FROM legislation_actions")
    leg_span = leg['mx'] - leg['mn'] + 1
    sections.append((
        "Legislation", "B+",
        f"{leg['cnt']:,} bills, {cosp:,} cosponsors, {subj:,} subjects, {act:,} actions",
        f"Congress {leg['mn']}–{leg['mx']} ({leg_span} of 27 available congresses)",
        f"Gap: congresses 93–{leg['mn']-1} not loaded" if leg['mn'] > 93 else "Complete",
        "",
    ))

    # --- Roll Call Votes ---
    rv = qrow("SELECT COUNT(*) AS cnt, MIN(congress) AS mn, MAX(congress) AS mx, MAX(date) AS latest FROM roll_call_votes")
    mv = q1("SELECT COUNT(*) FROM member_votes")
    sections.append((
        "Roll Call Votes", "A",
        f"{rv['cnt']:,} votes, {mv:,} member votes",
        f"Congress {rv['mn']}–{rv['mx']}",
        "",
        safe_date(rv['latest']),
    ))

    # --- Committees ---
    com = q1("SELECT COUNT(*) FROM committees")
    cmem = qrow("SELECT COUNT(*) AS cnt, COUNT(DISTINCT bioguide_id) AS members FROM committee_memberships")
    sections.append((
        "Committees", "A",
        f"{com} committees, {cmem['cnt']:,} memberships ({cmem['members']} members)",
        "Current snapshot",
        "Historical memberships not tracked",
        "",
    ))

    # --- Stock Trades ---
    st = qrow("""
        SELECT COUNT(*) AS cnt,
               SUM(CASE WHEN bioguide_id IS NOT NULL THEN 1 ELSE 0 END) AS linked,
               SUM(CASE WHEN transaction_date IS NULL OR transaction_date = '' THEN 1 ELSE 0 END) AS null_dates,
               SUM(CASE WHEN transaction_type IS NULL OR transaction_type = '' THEN 1 ELSE 0 END) AS null_types,
               MIN(transaction_date) AS mn, MAX(transaction_date) AS mx
        FROM stock_trades
    """)
    st_pct = round(100 * st['linked'] / st['cnt'], 1) if st['cnt'] else 0
    st_by_chamber = qall("SELECT chamber, COUNT(*) AS cnt FROM stock_trades GROUP BY chamber")
    st_chambers = ", ".join(f"{r['chamber']} {r['cnt']:,}" for r in st_by_chamber)
    txn_types = q1("SELECT COUNT(DISTINCT transaction_type) FROM stock_trades")
    st_issues = []
    if st['null_dates'] and st['null_dates'] > 0:
        st_issues.append(f"{st['null_dates']:,} empty dates")
    if st['null_types'] and st['null_types'] > 0:
        st_issues.append(f"{st['null_types']:,} empty types")
    if txn_types > 5:
        st_issues.append(f"{txn_types} distinct type values (needs normalization)")
    sections.append((
        "Stock Trades", "A",
        f"{st['cnt']:,} ({st_chambers})",
        f"{st['mn'] or '?'}–{st['mx'] or '?'}, {st_pct}% bioguide-linked",
        "; ".join(st_issues) if st_issues else "Clean",
        safe_date(st['mx']),
    ))

    # --- Spending ---
    sp = qrow("SELECT COUNT(*) AS cnt, ROUND(SUM(award_amount)/1e9, 1) AS total_b FROM spending_awards")
    sp_cats = qall("SELECT award_category, COUNT(*) AS cnt, ROUND(SUM(award_amount)/1e9,1) AS b FROM spending_awards GROUP BY award_category")
    sp_detail = ", ".join(f"{r['award_category']} {r['cnt']:,} (${r['b']}B)" for r in sp_cats)
    sections.append((
        "Federal Spending", "A",
        f"{sp['cnt']:,} awards (${sp['total_b']}B total)",
        f"20 agencies, FY2017–present. {sp_detail}",
        "",
        "",
    ))

    # --- Lobbying ---
    lb = qrow("SELECT COUNT(*) AS cnt, MIN(filing_year) AS mn, MAX(filing_year) AS mx FROM lobbying_filings")
    la = q1("SELECT COUNT(*) FROM lobbying_activities")
    ll = q1("SELECT COUNT(*) FROM lobbying_lobbyists")
    lc = q1("SELECT COUNT(*) FROM lobbying_contributions")
    lbills = q1("SELECT COUNT(*) FROM lobbying_bills")
    sections.append((
        "Lobbying Disclosures", "A+",
        f"{lb['cnt']:,} filings, {la:,} activities, {ll:,} lobbyists, {lc:,} contributions, {lbills:,} bill refs",
        f"{lb['mn']}–{lb['mx']}",
        "Complete Senate LDA data",
        "",
    ))

    # --- FEC ---
    fec_cand = q1("SELECT COUNT(*) FROM fec_candidates")
    fec_comm = q1("SELECT COUNT(*) FROM fec_committees")
    fec_cont = q1("SELECT COUNT(*) FROM fec_contributions")
    fec_xwalk = q1("SELECT COUNT(*) FROM fec_candidate_crosswalk")
    fec_oppexp = q1("SELECT COUNT(*) FROM fec_operating_expenditures")
    fec_pac = q1("SELECT COUNT(*) FROM fec_pac_summary")
    fec_ie = q1("SELECT COUNT(*) FROM fec_independent_expenditures")
    fec_elec = q1("SELECT COUNT(*) FROM fec_electioneering")
    fec_cc = q1("SELECT COUNT(*) FROM fec_communication_costs")
    sections.append((
        "FEC Campaign Finance", "A+",
        f"{fec_cont:,} contributions, {fec_cand:,} candidates, {fec_comm:,} committees, "
        f"{fec_oppexp:,} operating expenditures, {fec_ie:,} independent expenditures, "
        f"{fec_pac:,} PAC summaries",
        f"{fec_xwalk:,} bioguide crosswalk links, {fec_elec:,} electioneering, {fec_cc:,} comm costs",
        "",
        "",
    ))

    # --- OIRA ---
    oira_rev = q1("SELECT COUNT(*) FROM oira_reviews")
    oira_mtg = q1("SELECT COUNT(*) FROM oira_meetings")
    oira_att = q1("SELECT COUNT(*) FROM oira_meeting_attendees")
    sections.append((
        "OIRA Regulatory Review", "A",
        f"{oira_rev:,} EO reviews, {oira_mtg:,} meetings, {oira_att:,} attendees",
        "1981–present (reviews), 2001–present (meetings)",
        "reginfo.gov bulk XML + scrape",
        "",
    ))

    # --- FARA ---
    fa_reg = q1("SELECT COUNT(*) FROM fara_registrants")
    fa_fp = q1("SELECT COUNT(*) FROM fara_foreign_principals")
    fa_sf = q1("SELECT COUNT(*) FROM fara_short_forms")
    fa_doc = q1("SELECT COUNT(*) FROM fara_registrant_docs")
    sections.append((
        "FARA Foreign Agents", "A+",
        f"{fa_reg:,} registrants, {fa_fp:,} principals, {fa_sf:,} agents, {fa_doc:,} documents",
        "Historical–present",
        "Complete DOJ bulk data",
        "",
    ))

    # --- Hearings ---
    hr = qrow("SELECT COUNT(*) AS cnt FROM hearings")
    hw = qrow("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN organization IS NOT NULL AND organization != '' THEN 1 ELSE 0 END) AS with_org
        FROM hearing_witnesses
    """)
    hw_pct = round(100 * hw['with_org'] / hw['total'], 1) if hw['total'] else 0
    hm = q1("SELECT COUNT(*) FROM hearing_members")
    # Check for congress field anomalies
    hr_bad_congress = q1("SELECT COUNT(*) FROM hearings WHERE congress < 50 OR congress > 200")
    hr_note = f"{hr_bad_congress:,} records with suspect congress values" if hr_bad_congress > 0 else ""
    hr_latest = q1("SELECT MAX(date_issued) FROM hearings")
    sections.append((
        "Committee Hearings", "B+",
        f"{hr['cnt']:,} hearings, {hw['total']:,} witnesses ({hw_pct}% with org), {hm:,} member attendance",
        "",
        hr_note,
        safe_date(hr_latest),
    ))

    # --- CRS ---
    crs = qrow("SELECT COUNT(*) AS cnt, MIN(publish_date) AS mn, MAX(publish_date) AS mx FROM crs_reports")
    crs_bills = q1("SELECT COUNT(*) FROM crs_report_bills")
    sections.append((
        "CRS Reports", "A",
        f"{crs['cnt']:,} reports, {crs_bills:,} bill cross-refs",
        f"{crs['mn'][:4]}–{crs['mx'][:4]}",
        "",
        safe_date(crs['mx']),
    ))

    # --- Nominations ---
    nom = qrow("SELECT COUNT(*) AS cnt FROM nominations")
    nom_act = q1("SELECT COUNT(*) FROM nomination_actions")
    nom_status = qall("SELECT status, COUNT(*) AS cnt FROM nominations GROUP BY status ORDER BY cnt DESC")
    nom_breakdown = ", ".join(f"{r['status']} {r['cnt']:,}" for r in nom_status)
    nom_civ = qrow("SELECT SUM(is_civilian) AS civ, SUM(is_military) AS mil FROM nominations")
    nom_votes = q1("SELECT COUNT(*) FROM nominations WHERE vote_yea IS NOT NULL")
    nom_range = qrow("SELECT MIN(congress) AS mn, MAX(congress) AS mx FROM nominations")
    nom_latest = q1("SELECT MAX(received_date) FROM nominations")
    sections.append((
        "Executive Nominations", "B+",
        f"{nom['cnt']:,} nominations, {nom_act:,} actions",
        f"Congress {nom_range['mn']}–{nom_range['mx']}: {nom_breakdown}",
        f"Civilian {nom_civ['civ']:,} / Military {nom_civ['mil']:,}; {nom_votes:,} with recorded votes. "
        f"Note: 'Returned' = sent back to President without action (functionally rejected/withdrawn)",
        safe_date(nom_latest),
    ))

    # --- Treaties ---
    tr = qrow("SELECT COUNT(*) AS cnt FROM treaties")
    tr_act = q1("SELECT COUNT(*) FROM treaty_actions")
    sections.append(("Treaties", "B", f"{tr['cnt']:,} treaties, {tr_act:,} actions", "", "Small but complete", ""))

    # --- GAO ---
    gao = qrow("SELECT COUNT(*) AS cnt, MIN(date_issued) AS mn, MAX(date_issued) AS mx FROM gao_reports")
    sections.append((
        "GAO Reports", "B",
        f"{gao['cnt']:,}",
        f"{gao['mn'][:4]}–{gao['mx'][:4]}",
        "Coverage ends 2008 (GovInfo collection limitation)",
        safe_date(gao['mx']),
    ))

    # --- IG Reports ---
    ig = q1("SELECT COUNT(*) FROM ig_reports")
    ig_rec = q1("SELECT COUNT(*) FROM ig_recommendations")
    sections.append((
        "IG Reports", "A",
        f"{ig:,} reports, {ig_rec:,} recommendations",
        "Historical–present",
        "oversight.gov scrape",
        "",
    ))

    # --- Earmarks ---
    ear = qrow("""
        SELECT COUNT(*) AS cnt,
               SUM(CASE WHEN bioguide_id IS NOT NULL AND bioguide_id != '' THEN 1 ELSE 0 END) AS linked
        FROM earmarks
    """)
    ear_pct = round(100 * ear['linked'] / ear['cnt'], 1) if ear['cnt'] else 0
    sections.append((
        "Earmarks/CDS", "A",
        f"{ear['cnt']:,}",
        f"FY2022–present, {ear_pct}% bioguide-linked",
        "",
        "",
    ))

    # ── Cross-dataset connections ────────────────────────────────────

    connection_tables = [
        ("speeches_near_trades", "Speeches Near Trades"),
        ("committee_donor_summary", "Committee Donor Summary"),
        ("lobbying_bill_summary", "Lobbying Bill Summary"),
        ("witness_lobby_overlap", "Witness-Lobby Overlap"),
        ("committee_trade_conflicts", "Committee Trade Conflicts"),
        ("commenter_lobby_overlap", "Commenter-Lobby Overlap"),
        ("revolving_door", "Revolving Door"),
        ("fara_hearing_overlap", "FARA-Hearings Overlap"),
    ]
    conn_rows = []
    for table, label in connection_tables:
        try:
            cnt = q1(f"SELECT COUNT(*) FROM [{table}]")
            conn_rows.append((label, f"{cnt:,}"))
        except Exception:
            conn_rows.append((label, "(not built)"))

    # ── FTS verification ─────────────────────────────────────────────

    fts_tables = []
    for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_fts' ORDER BY name"
    ):
        tname = row[0]
        try:
            cnt = conn.execute(
                f"SELECT COUNT(*) FROM [{tname}] WHERE [{tname}] MATCH 'government'"
            ).fetchone()[0]
            fts_tables.append((tname, cnt, "OK"))
        except Exception as e:
            fts_tables.append((tname, 0, f"ERROR: {e}"))

    fts_ok = sum(1 for _, _, s in fts_tables if s == "OK")
    fts_total = len(fts_tables)

    # ── Build the report ─────────────────────────────────────────────

    audit_duration = round(_time.time() - audit_start)

    lines = [
        f"# OpenRegs Database — Full Audit Report",
        f"",
        f"**Generated**: {now.strftime('%Y-%m-%d %H:%M UTC')} (auto-generated by build script, {audit_duration}s)",
        f"**Database**: {db_size_gb} GB, {total_tables} tables, {fts_total} FTS indexes ({fts_ok} verified OK)",
        f"",
        GRADING_RUBRIC,
        f"---",
        f"",
        f"## Data Source Grades",
        f"",
        f"| Dataset | Grade | Records | Coverage | Notes | Latest |",
        f"|---------|-------|---------|----------|-------|--------|",
    ]

    for title, grade, rows, coverage, notes, latest in sections:
        lines.append(f"| {title} | **{grade}** | {rows} | {coverage} | {notes} | {latest} |")

    lines.extend([
        "",
        "---",
        "",
        "## Cross-Dataset Connections",
        "",
        "| Connection | Records | Description |",
        "|------------|---------|-------------|",
    ])
    for label, cnt in conn_rows:
        desc = CONNECTION_DESCRIPTIONS.get(label, "")
        lines.append(f"| {label} | {cnt} | {desc} |")

    lines.extend([
        "",
        "---",
        "",
        "## Full-Text Search Indexes",
        "",
        "| Index | Test Matches ('government') | Status |",
        "|-------|----------------------------|--------|",
    ])
    for tname, cnt, status in fts_tables:
        lines.append(f"| `{tname}` | {cnt:,} | {status} |")

    # ── Known gaps & roadmap ─────────────────────────────────────────

    flags = []

    # Stock trade type normalization (should be clean after build-time normalization)
    txn_distinct = qall("SELECT transaction_type, COUNT(*) AS cnt FROM stock_trades GROUP BY transaction_type ORDER BY cnt DESC")
    abbreviated = [r for r in txn_distinct if r['transaction_type'] in ('P', 'S', 'E', 'S (partial)', '', None)]
    if abbreviated:
        flags.append({
            "gap": "Stock trade transaction_type still has abbreviated codes after normalization",
            "impact": "Low",
            "status": "Investigate",
            "plan": "Check import_stock_trades normalization step",
        })

    # Hearings congress anomalies
    if hr_bad_congress > 0:
        flags.append({
            "gap": f"Hearings: {hr_bad_congress} records with suspect congress values",
            "impact": "Low",
            "status": "Next rebuild",
            "plan": "Data cleanup",
        })

    # Legislation congress gap
    if leg['mn'] > 93:
        flags.append({
            "gap": f"Legislation missing congresses 93–{leg['mn']-1} ({leg_span} of 27 loaded)",
            "impact": "Medium",
            "status": "Paused",
            "plan": "GovInfo BILLSTATUS available back to 93rd",
        })

    # GAO coverage
    if gao['mx'] and gao['mx'][:4] < '2020':
        flags.append({
            "gap": f"GAO reports end at {gao['mx'][:4]}",
            "impact": "Medium",
            "status": "Source-limited",
            "plan": "Monitor GovInfo for collection updates",
        })

    # Comment detail coverage
    if cm_pct < 10:
        flags.append({
            "gap": f"Comment full text at {cm_pct}%: {cm_detail:,} of {cm_total:,} headers",
            "impact": "High",
            "status": "Expanding",
            "plan": "Ongoing API collection (~780 req/hr)",
        })

    # FTS failures
    fts_failures = [t for t, _, s in fts_tables if s != "OK"]
    if fts_failures:
        flags.append({
            "gap": f"FTS index errors: {', '.join(fts_failures)}",
            "impact": "High",
            "status": "Fix required",
            "plan": "Investigate and rebuild affected indexes",
        })

    if flags:
        lines.extend([
            "",
            "---",
            "",
            "## Known Gaps & Roadmap",
            "",
            "| Gap | Impact | Status | Plan |",
            "|-----|--------|--------|------|",
        ])
        for f in flags:
            lines.append(f"| {f['gap']} | {f['impact']} | {f['status']} | {f['plan']} |")

    lines.extend([
        "",
        "---",
        "",
        f"*Auto-generated by `05_build_database.py` — {now.strftime('%Y-%m-%d')}*",
    ])

    report_text = "\n".join(lines)

    # Save timestamped + latest
    filename = now.strftime("%Y%m%d_%H%M%S")
    (report_dir / f"audit_{filename}.md").write_text(report_text)
    (report_dir / "audit_latest.md").write_text(report_text)

    log.info(f"Audit report saved: {report_dir / f'audit_{filename}.md'}")
    log.info(f"Audit latest saved: {report_dir / 'audit_latest.md'}")

    # Print summary to console
    grade_counts = {}
    for _, grade, _, _, _, _ in sections:
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
    grade_summary = ", ".join(f"{g}: {c}" for g, c in sorted(grade_counts.items()))
    log.info(f"Grades: {grade_summary}")
    if flags:
        log.info(f"Known gaps: {len(flags)}")
        for f in flags:
            log.info(f"  ⚠ [{f['impact']}] {f['gap']} — {f['status']}")
    else:
        log.info("No known gaps — all clean!")


def create_performance_indexes(conn: sqlite3.Connection):
    """Create performance-critical indexes for explore page queries.

    These give 100-1000x speedups on member profile and cross-reference queries.
    """
    log.info("Creating performance indexes...")
    indexes = [
        # Member profile queries (votes) — idx_mv_bioguide covers bioguide_id
        "CREATE INDEX IF NOT EXISTS idx_member_votes_vote ON member_votes(vote_id)",
        # Hearing queries — idx_hw_package and idx_hm_bioguide cover witnesses/members
        "CREATE INDEX IF NOT EXISTS idx_hearing_members_package ON hearing_members(package_id)",
        # Lobbying contributions
        "CREATE INDEX IF NOT EXISTS idx_lobby_contrib_filing ON lobbying_contributions(filing_uuid)",
    ]
    for idx_sql in indexes:
        try:
            conn.execute(idx_sql)
        except sqlite3.OperationalError as e:
            log.warning(f"  Index skipped: {e}")
    conn.commit()
    log.info(f"  {len(indexes)} performance indexes created")


def validate_pipeline():
    """
    Pre-build validation checks. Warns about missing data, orphan scripts,
    and stale sources. Never blocks the build — just logs warnings.

    See PIPELINE.md "Build-Time Validation" section for specification.
    """
    warnings = []

    # -- Check 1: Orphan script detection --
    # Every numbered script should correspond to data that flows into the build.
    # If a new script is added without wiring it in, we want to know.
    known_scripts = {
        "01_federal_register", "02_regs_gov_dockets_docs",
        "03_regs_gov_comments",  # consolidated: replaces 03+04+06
        "05_build_database",
        "07_full_comment_details",
        "08_fec_campaign_finance", "08_usaspending",
        "09_congress_gov", "09_fec_employer_aggregates",
        "10_congress_votes", "10_ecfr",
        "11_congressional_record", "11_fara",
        "12_congress_stock_trades",  # consolidated: replaces 12+13+14
        "12_expand_agencies",
        "15_lobbying_disclosure", "16_backfill_dockets",
        "16_committee_hearings", "17_crs_reports",
        "18_nominations_treaties",
        "19_gao_reports", "19_hearing_transcripts", "19b_gao_direct",
        "20_daily_open_comments", "20_sec_ticker_sic",
        "21_ig_reports", "22_oira_meetings",
        # Staging-integration + entity-resolution scripts (feeds staging DBs
        # merged in build). These run asynchronously from the weekly pipeline
        # but their outputs land in openregs.db via the import_staging_* fns.
        "15b_enrich_lobbying",
        "23_cbo_backfill",
        "24_sec_form4_backfill",
        "25_government_units",
        "26_bmf_group_exemptions",
        "27_tribes_and_federal_agencies",
        "28_state_agencies_and_universities",
        "29_acronym_curation_ingest",
        "30_industry_classifier",
        "31_trade_association_tagging",
        "32_data_cleanup_pass",
        "33_uei_entity_creation",
        "36_form4_to_public_actors",
        "39_naics_from_usaspending_detail",
        "40_congressional_disbursements_download",
        "41_parse_house_disbursements",
        "42_wh_visitors_integrate",
        "43_oge_pas_integrate",
        "44_faca_integrate",
        "45_wh_visitor_to_actor",
        "46_parse_senate_disbursements",
        "47_parse_house_pre2016_disbursements",
    }
    scripts_dir = BASE_DIR / "scripts"
    for py_file in scripts_dir.glob("[0-9][0-9]*.py"):
        stem = py_file.stem
        if stem not in known_scripts:
            warnings.append(f"ORPHAN SCRIPT: {py_file.name} is not in the known scripts list — update validate_pipeline() and PIPELINE.md")

    # -- Check 2: Source path existence --
    expected_sources = {
        "Federal Register": FR_DIR,
        "Dockets": DOCKETS_DIR,
        "Documents": DOCS_DIR,
        "Comments (headers)": COMMENTS_DIR,
        "Comments (details)": DETAILS_DIR,
        "Spending": SPENDING_DIR,
        "Lobbying": LOBBYING_DB,
        "FEC": FEC_DB,
        "OIRA": OIRA_DB,
        "Votes": VOTES_DB,
        "FARA": FARA_DB,
        "Legislation": CONGRESS_DIR,
        "eCFR": ECFR_DIR,
        "Congressional Record": CREC_DIR,
        "Congress Members": MEMBERS_DIR,
        "Stock Trades": STOCK_DIR,
        "Hearings": HEARINGS_DIR,
        "CRS Reports": CRS_DIR,
        "Nominations": NOMINATIONS_DIR,
        "Treaties": TREATIES_DIR,
        "GAO Reports": GAO_DIR,
        "GAO Direct": GAO_DIRECT_DIR,
        "IG Reports": IG_DIR,
        "Earmarks": EARMARKS_DB,
    }
    for name, path in expected_sources.items():
        if not path.exists():
            warnings.append(f"MISSING SOURCE: {name} — expected at {path}")

    # -- Check 3: Stale data detection --
    stale_thresholds = {
        FR_DIR: ("Federal Register", 7),
        COMMENTS_DIR: ("Comments", 7),
        CONGRESS_DIR: ("Legislation", 30),
        LOBBYING_DB: ("Lobbying", 90),
        FEC_DB: ("FEC", 90),
    }
    for path, (name, max_days) in stale_thresholds.items():
        if path.exists():
            if path.is_dir():
                # Use directory mtime
                mtime = path.stat().st_mtime
            else:
                mtime = path.stat().st_mtime
            age_days = (time.time() - mtime) / 86400
            if age_days > max_days:
                warnings.append(f"STALE DATA: {name} last modified {age_days:.0f} days ago (threshold: {max_days} days)")

    # -- Log results --
    if warnings:
        log.warning(f"Pipeline validation: {len(warnings)} warning(s)")
        for w in warnings:
            log.warning(f"  ⚠ {w}")
    else:
        log.info("Pipeline validation: all checks passed")

    return warnings


def main():
    log.info("=" * 60)
    log.info("BUILDING OPENREGS DATABASE (with enrichments)")
    log.info(f"Output: {DB_PATH}")
    log.info("=" * 60)

    validate_pipeline()

    start = time.time()

    if DB_PATH.exists():
        backup_path = DB_PATH.with_name("openregs_prev.db")
        log.info(f"Backing up existing database → {backup_path}")
        shutil.copy2(str(DB_PATH), str(backup_path))
        log.info(f"Backup complete ({DB_PATH.stat().st_size / (1024**3):.1f} GB)")
        DB_PATH.unlink()
        log.info("Removed existing database")

    conn = sqlite3.connect(str(DB_PATH), uri=True)
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
        import_oira(conn)
        import_votes(conn)
        import_fara(conn)
        import_legislation(conn)
        import_ecfr(conn)
        import_congressional_record(conn)
        import_congress_members(conn)
        import_committees(conn)
        import_stock_trades(conn)
        import_hearings(conn)
        import_crs_reports(conn)
        import_nominations(conn)
        import_treaties(conn)
        import_gao_reports(conn)
        import_ig_reports(conn)
        import_earmarks(conn)
        import_government_units(conn)
        import_bmf_groups(conn)
        # Staging-phase imports — wrap in try/except and emit a phase-marker
        # line so downstream (weekly_update.sh) can ping hc.io staging check
        # distinctly from the overall build status.
        staging_failures = []
        for fn in (import_staging_entities, import_staging_disbursements,
                   import_staging_oge_pas, import_staging_wh_visits):
            try:
                fn(conn)
            except Exception as e:
                staging_failures.append(f"{fn.__name__}: {e}")
                log.error(f"STAGING PHASE FAIL: {fn.__name__}: {e}", exc_info=True)
        if staging_failures:
            log.error(f"STAGING_IMPORT_FAILED: {len(staging_failures)} phase(s) errored: "
                      + "; ".join(staging_failures))
        else:
            log.info("STAGING_IMPORT_OK: all 4 staging DBs imported cleanly")
        import_cosponsors(conn)
        build_crec_junction_tables(conn)
        link_hearing_members(conn)
        build_crossref(conn)
        build_agency_map(conn)
        create_performance_indexes(conn)
        build_views(conn)
        materialize_slow_views(conn)
        build_member_stats(conn)
        build_docket_summary(conn)
        import_reference_tables(conn)
        enrich_stock_trades_cik(conn)
        build_lobbying_bill_refs(conn)
        build_summary_tables(conn)
        build_fts(conn)
        print_stats(conn)
        validate_critical_tables(conn)  # fail-loud; aborts on missing/undersized
        validate_dates(conn)
        validate_freshness(conn)  # warn-only; emits FRESHNESS_WARNING markers for hc.io
        elapsed = time.time() - start
        save_build_report(conn, elapsed)
        generate_audit_report(conn)
    finally:
        conn.close()

    elapsed = time.time() - start
    log.info(f"\nDone in {elapsed/60:.1f} minutes")
    log.info(f"Database ready: {DB_PATH}")


if __name__ == "__main__":
    main()
