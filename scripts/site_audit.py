#!/usr/bin/env python3
"""
site_audit.py -- Weekly synthetic monitoring for DataDawn.

Runs ~150 checks against the live user-facing surfaces (regs.datadawn.org,
data.datadawn.org, forms.datadawn.org, datadawn.org) and the databases
behind them. Designed to catch:

  - Broken external links (govinfo, federalregister.gov, regulations.gov)
  - Stale / missing FTS tables (the class of bug that hit on 2026-04-11)
  - Detail-page template regressions
  - Datasette health (both DBs attached, canned queries non-empty)
  - Form viewer renders
  - TLS cert expiry (<14 days = warning, <7 days = critical)
  - Static files still served (llms.txt, robots.txt, openapi.json)

Severity:
  CRITICAL -- turns the hc.io check red (user-visible breakage)
  WARNING  -- logged + in ping body, does NOT turn the check red
  INFO     -- just a passing note

Transient failures (5xx, timeout) retry once before being flagged.

Usage:
    python3 site_audit.py               # full audit
    python3 site_audit.py --quick       # smaller samples (~30 checks, for testing)
    python3 site_audit.py --dry-run     # print planned checks, no HTTP
    python3 site_audit.py --no-color    # disable ANSI colors in stdout

Exit codes:
    0 -- all critical checks passed (warnings allowed)
    1 -- one or more critical checks failed
"""

import argparse
import datetime as dt
import json
import logging
import os
import random
import socket
import sqlite3
import ssl
import sys
import time
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
PROJECT_DIR = Path(__file__).resolve().parent.parent  # openregs/
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

OPENREGS_DB = PROJECT_DIR / "openregs.db"
# 990 DB lives in a sibling project; override with NINETY_DB env var if your layout differs.
NINETY_DB = Path(os.environ.get("NINETY_DB",
                                str(PROJECT_DIR.parent / "990project" / "990data_public.db")))

OPENREGS_BASE = "https://regs.datadawn.org"
NINETY_BASE = "https://data.datadawn.org"
FORMS_BASE = "https://forms.datadawn.org"
WEBSITE_BASE = "https://datadawn.org"

USER_AGENT = "DataDawn-SiteAudit/1.0 (+https://datadawn.org)"
REQ_TIMEOUT = 15  # seconds per request
FTS_TIMEOUT = 60  # longer timeout for COUNT(*) on large FTS tables (crec_fts has ~900K rows)
DOMAIN_THROTTLE = 1.0  # min seconds between requests to same external domain

# Status codes that indicate the server responded (not a broken link), even if it blocked us.
# Regulations.gov returns 403 to automated requests — the link is valid for browser users.
LINK_OK_CODES = {200, 201, 202, 203, 204, 301, 302, 303, 304, 307, 308, 401, 403}
TLS_WARN_DAYS = 14
TLS_CRIT_DAYS = 7

SAMPLE_SIZE = 5       # external link samples per source, detail page samples per type
QUICK_SAMPLE_SIZE = 2

# Exit codes
EXIT_OK = 0
EXIT_CRITICAL = 1

# FTS tables (must exist + be non-empty on the LIVE deployment)
OPENREGS_FTS = [
    "federal_register_fts", "dockets_fts", "documents_fts", "comments_fts",
    "cfr_fts", "crec_fts", "lobbying_fts", "spending_awards_fts",
    "legislation_fts", "fara_registrants_fts", "fara_foreign_principals_fts",
    "fec_employer_fts", "hearings_fts", "crs_reports_fts",
    "nominations_fts", "gao_reports_fts", "earmarks_fts",
]

NINETY_FTS = [
    "fts_returns", "fts_grants", "fts_daf",
    "fts_si990", "fts_bmf", "fts_officers",
]

# Explore pages (relative to their Datasette base)
OPENREGS_EXPLORE_PAGES = [
    "index.html", "api.html", "cbo.html", "commenter-lobby.html",
    "committee-donors.html", "committee-trades.html", "contributions.html",
    "entity.html", "fara-hearings.html", "fara.html", "hearings.html",
    "legislation.html", "lobbied-bills.html", "lobbying.html", "member.html",
    "nominations.html", "regulation.html", "research.html",
    "revolving-door.html", "search.html", "speech.html", "speeches-trades.html",
    "trade-conflicts.html", "treaties.html", "witness-lobby.html",
]

NINETY_EXPLORE_PAGES = [
    "990.html", "bmf.html", "daf.html", "grants.html", "results.html",
]

# Canned queries to test (name, optional params dict)
OPENREGS_CANNED_QUERIES = [
    ("most_commented_dockets", {}),
    ("search_federal_register", {"search": "regulation"}),
    ("search_comments", {"search": "public"}),
    ("search_documents", {"search": "rule"}),
    ("search_dockets", {"search": "proposed"}),
]

# === Severity / Result model ===

class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Result:
    category: str
    name: str
    passed: bool
    severity: Severity
    detail: str = ""
    elapsed_ms: int = 0


# === Logging setup ===
log = logging.getLogger("site_audit")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "site_audit.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(_sh)
except Exception:
    pass


# === HTTP client with polite throttling + retry-once ===

class PoliteClient:
    """Domain-aware throttled HTTP client with retry-once on transient errors."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._last_hit: dict[str, float] = defaultdict(float)
        self._session = requests.Session()
        retry = Retry(
            total=0,  # we do our own single retry on 5xx/timeout
            connect=0, read=0, status=0,
            backoff_factor=0,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        self._session.headers.update({"User-Agent": USER_AGENT})

    def _throttle(self, url: str):
        host = urllib.parse.urlparse(url).netloc
        now = time.monotonic()
        wait = self._last_hit[host] + DOMAIN_THROTTLE - now
        if wait > 0:
            time.sleep(wait)
        self._last_hit[host] = time.monotonic()

    def request(self, method: str, url: str, **kwargs) -> tuple[int, str, int]:
        """Return (status_code, error_or_empty, elapsed_ms). 0 status_code means connection error."""
        if self.dry_run:
            return 200, "", 0
        kwargs.setdefault("timeout", REQ_TIMEOUT)
        kwargs.setdefault("allow_redirects", True)
        for attempt in (1, 2):
            self._throttle(url)
            t0 = time.monotonic()
            try:
                resp = self._session.request(method, url, **kwargs)
                elapsed = int((time.monotonic() - t0) * 1000)
                if resp.status_code >= 500 and attempt == 1:
                    time.sleep(2)
                    continue
                return resp.status_code, "", elapsed
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt == 1:
                    time.sleep(2)
                    continue
                elapsed = int((time.monotonic() - t0) * 1000)
                return 0, f"{type(e).__name__}: {e}", elapsed
            except requests.RequestException as e:
                elapsed = int((time.monotonic() - t0) * 1000)
                return 0, f"{type(e).__name__}: {e}", elapsed
        return 0, "exhausted retries", 0

    def head_or_get(self, url: str) -> tuple[int, str, int]:
        """Try HEAD first (cheap), fall back to GET if HEAD returns 405/501."""
        code, err, ms = self.request("HEAD", url)
        if code in (405, 501, 403):
            # Some sites (govinfo among them) don't support HEAD cleanly
            return self.request("GET", url, stream=True)
        return code, err, ms

    def get(self, url: str, **kwargs) -> tuple[int, str, int, bytes]:
        """GET returning (code, err, elapsed_ms, body_bytes). Body is None on error."""
        if self.dry_run:
            return 200, "", 0, b""
        kwargs.setdefault("timeout", REQ_TIMEOUT)
        for attempt in (1, 2):
            self._throttle(url)
            t0 = time.monotonic()
            try:
                resp = self._session.get(url, **kwargs)
                elapsed = int((time.monotonic() - t0) * 1000)
                if resp.status_code >= 500 and attempt == 1:
                    time.sleep(2)
                    continue
                return resp.status_code, "", elapsed, resp.content
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt == 1:
                    time.sleep(2)
                    continue
                return 0, f"{type(e).__name__}: {e}", 0, b""
            except requests.RequestException as e:
                return 0, f"{type(e).__name__}: {e}", 0, b""
        return 0, "exhausted retries", 0, b""


# === Audit runner ===

class Audit:
    def __init__(self, dry_run: bool = False, quick: bool = False, no_color: bool = False):
        self.client = PoliteClient(dry_run=dry_run)
        self.dry_run = dry_run
        self.quick = quick
        self.no_color = no_color
        self.results: list[Result] = []
        self.started = dt.datetime.now()
        self.sample_size = QUICK_SAMPLE_SIZE if quick else SAMPLE_SIZE

    def record(self, category: str, name: str, passed: bool, severity: Severity,
               detail: str = "", elapsed_ms: int = 0):
        r = Result(category=category, name=name, passed=passed, severity=severity,
                   detail=detail, elapsed_ms=elapsed_ms)
        self.results.append(r)
        icon = "OK" if passed else ("CRIT" if severity == Severity.CRITICAL else "WARN")
        log.info(f"  [{icon:4}] {category}/{name}  {detail}")

    # ---- Category 1: top-level site health ----
    def check_site_health(self):
        log.info("== Site health ==")
        # forms.datadawn.org has no index page (R2-backed static filings), so we can only
        # verify the host is up via DNS + TLS handshake — that check lives under check_tls_expiry.
        targets = [
            ("regs.datadawn.org", OPENREGS_BASE + "/", Severity.CRITICAL),
            ("data.datadawn.org", NINETY_BASE + "/", Severity.CRITICAL),
            ("datadawn.org", WEBSITE_BASE + "/", Severity.CRITICAL),
        ]
        for name, url, sev in targets:
            code, err, ms = self.client.head_or_get(url)
            passed = 200 <= code < 400
            detail = f"HTTP {code}" if code else err
            self.record("site_health", name, passed, sev, detail, ms)

    # ---- Category 2: Datasette internals ----
    def check_datasette_internals(self):
        log.info("== Datasette internals ==")
        for label, base, expected_dbs in [
            ("openregs", OPENREGS_BASE, ["openregs", "aphis"]),
            ("990", NINETY_BASE, ["990data_public"]),
        ]:
            # /-/versions
            code, err, ms = self.client.head_or_get(f"{base}/-/versions")
            self.record("datasette", f"{label}/versions", 200 <= code < 400,
                        Severity.WARNING, f"HTTP {code}" if code else err, ms)
            # /-/databases should include the expected DB name
            code, err, ms, body = self.client.get(f"{base}/-/databases.json")
            if 200 <= code < 400 and body:
                try:
                    dbs = json.loads(body)
                    names = [d.get("name", "") for d in dbs] if isinstance(dbs, list) else []
                    missing = [d for d in expected_dbs if d not in names]
                    ok = not missing
                    self.record("datasette", f"{label}/databases",
                                ok, Severity.CRITICAL,
                                f"found={names}" if ok else f"missing={missing}, got={names}", ms)
                except Exception as e:
                    self.record("datasette", f"{label}/databases", False,
                                Severity.WARNING, f"parse error: {e}", ms)
            else:
                self.record("datasette", f"{label}/databases", False,
                            Severity.CRITICAL, f"HTTP {code} {err}", ms)

    # ---- Category 3: static files ----
    def check_static_files(self):
        log.info("== Static files ==")
        targets = [
            (OPENREGS_BASE + "/llms.txt", "openregs/llms.txt", Severity.WARNING, 500),
            (NINETY_BASE + "/llms.txt", "990/llms.txt", Severity.WARNING, 500),
            (OPENREGS_BASE + "/robots.txt", "openregs/robots.txt", Severity.WARNING, 20),
            (WEBSITE_BASE + "/robots.txt", "website/robots.txt", Severity.WARNING, 20),
            (OPENREGS_BASE + "/api/openapi.json", "openregs/api/openapi.json", Severity.WARNING, 300),
            (NINETY_BASE + "/api/openapi.json", "990/api/openapi.json", Severity.WARNING, 300),
        ]
        for url, name, sev, min_size in targets:
            code, err, ms, body = self.client.get(url)
            size = len(body) if body else 0
            passed = 200 <= code < 400 and size >= min_size
            detail = f"HTTP {code}, {size} bytes (min {min_size})" if code else err
            self.record("static", name, passed, sev, detail, ms)

    # ---- Category 4: explore pages ----
    def check_explore_pages(self):
        log.info("== Explore pages ==")
        for page in OPENREGS_EXPLORE_PAGES:
            code, err, ms = self.client.head_or_get(f"{OPENREGS_BASE}/explore/{page}")
            passed = 200 <= code < 400
            # A single page breaking is a warning; if many break it bubbles up in the summary.
            self.record("explore", f"openregs/{page}", passed, Severity.WARNING,
                        f"HTTP {code}" if code else err, ms)
        for page in NINETY_EXPLORE_PAGES:
            code, err, ms = self.client.head_or_get(f"{NINETY_BASE}/explore/{page}")
            passed = 200 <= code < 400
            self.record("explore", f"990/{page}", passed, Severity.WARNING,
                        f"HTTP {code}" if code else err, ms)

    # ---- Category 5: canned queries ----
    def check_canned_queries(self):
        log.info("== Canned queries ==")
        for qname, params in OPENREGS_CANNED_QUERIES:
            qs = {"_shape": "array", "_size": "1", **params}
            url = f"{OPENREGS_BASE}/openregs/{qname}.json?{urllib.parse.urlencode(qs)}"
            code, err, ms, body = self.client.get(url)
            if not (200 <= code < 400) or not body:
                self.record("canned", qname, False, Severity.CRITICAL,
                            f"HTTP {code} {err}", ms)
                continue
            try:
                rows = json.loads(body)
                n = len(rows) if isinstance(rows, list) else 0
                self.record("canned", qname, n > 0, Severity.CRITICAL,
                            f"{n} rows", ms)
            except Exception as e:
                self.record("canned", qname, False, Severity.WARNING,
                            f"parse error: {e}", ms)

    # ---- Category 6: FTS sanity (query LIVE VPS to catch stale deployment) ----
    def check_fts_sanity(self):
        log.info("== FTS sanity (live VPS) ==")
        def fts_nonempty(base: str, db: str, table: str) -> tuple[int, str, int]:
            """Return (row_sentinel, error, ms). row_sentinel is 1 if FTS has >=1 row, 0 if empty, -1 on error.

            Uses SELECT rowid ... LIMIT 1 instead of COUNT(*) -- on large FTS tables like crec_fts
            (~900K rows) COUNT(*) can exceed the 15s request timeout.
            """
            sql = f"SELECT rowid FROM {table} LIMIT 1"
            url = f"{base}/{db}.json?sql={urllib.parse.quote(sql)}&_shape=array"
            code, err, ms, body = self.client.get(url, timeout=FTS_TIMEOUT)
            if not (200 <= code < 400) or not body:
                return -1, f"HTTP {code} {err}", ms
            try:
                rows = json.loads(body)
                return (1 if rows else 0), "", ms
            except Exception as e:
                return -1, f"parse error: {e}", ms

        for table in OPENREGS_FTS:
            n, err, ms = fts_nonempty(OPENREGS_BASE, "openregs", table)
            passed = n == 1
            detail = "non-empty" if n == 1 else ("EMPTY" if n == 0 else err)
            self.record("fts", f"openregs/{table}", passed, Severity.CRITICAL, detail, ms)

        for table in NINETY_FTS:
            n, err, ms = fts_nonempty(NINETY_BASE, "990data_public", table)
            passed = n == 1
            detail = "non-empty" if n == 1 else ("EMPTY" if n == 0 else err)
            self.record("fts", f"990/{table}", passed, Severity.CRITICAL, detail, ms)

    # ---- Category 7: detail pages (render sample rows via Datasette) ----
    def check_detail_pages(self):
        log.info("== Detail pages ==")
        try:
            oc = sqlite3.connect(f"file:{OPENREGS_DB}?mode=ro", uri=True)
        except Exception as e:
            self.record("detail", "openregs-db", False, Severity.CRITICAL,
                        f"cannot open {OPENREGS_DB}: {e}")
            return

        try:
            samples = [
                # (table, pk column, label)
                ("federal_register", "document_number", "federal_register"),
                ("documents", "id", "regs_document"),
                ("dockets", "id", "regs_docket"),
                ("legislation", "bill_id", "legislation"),
                ("congress_members", "bioguide_id", "member"),
                ("gao_reports", "package_id", "gao_report"),
            ]
            for table, pk, label in samples:
                try:
                    rows = oc.execute(
                        f"SELECT {pk} FROM {table} WHERE {pk} IS NOT NULL ORDER BY random() LIMIT ?",
                        (self.sample_size,),
                    ).fetchall()
                except sqlite3.Error as e:
                    self.record("detail", label, False, Severity.WARNING,
                                f"db query failed: {e}")
                    continue
                for (val,) in rows:
                    url = f"{OPENREGS_BASE}/openregs/{table}/{urllib.parse.quote(str(val), safe='')}"
                    code, err, ms, body = self.client.get(url)
                    size = len(body) if body else 0
                    passed = 200 <= code < 400 and size > 2000
                    self.record("detail", f"{label}:{val}", passed, Severity.WARNING,
                                f"HTTP {code}, {size} bytes" if code else err, ms)
        finally:
            oc.close()

    # ---- Category 8: external link sampling ----
    def check_external_links(self):
        log.info("== External links (sampled) ==")
        try:
            oc = sqlite3.connect(f"file:{OPENREGS_DB}?mode=ro", uri=True)
        except Exception as e:
            self.record("external", "openregs-db", False, Severity.CRITICAL,
                        f"cannot open {OPENREGS_DB}: {e}")
            return

        try:
            # CREC → govinfo
            rows = oc.execute(
                "SELECT granule_id FROM congressional_record "
                "WHERE date >= date('now', '-180 days') AND granule_id IS NOT NULL "
                "ORDER BY random() LIMIT ?", (self.sample_size,),
            ).fetchall()
            for (gid,) in rows:
                url = f"https://www.govinfo.gov/app/details/{gid}"
                code, err, ms = self.client.head_or_get(url)
                self.record("external", f"govinfo/{gid}", code in LINK_OK_CODES,
                            Severity.WARNING, f"HTTP {code}" if code else err, ms)

            # Federal Register
            rows = oc.execute(
                "SELECT document_number FROM federal_register "
                "WHERE publication_date >= date('now', '-180 days') "
                "ORDER BY random() LIMIT ?", (self.sample_size,),
            ).fetchall()
            for (dn,) in rows:
                url = f"https://www.federalregister.gov/d/{dn}"
                code, err, ms = self.client.head_or_get(url)
                self.record("external", f"federalregister/{dn}", code in LINK_OK_CODES,
                            Severity.WARNING, f"HTTP {code}" if code else err, ms)

            # Regulations.gov documents
            rows = oc.execute(
                "SELECT id FROM documents "
                "WHERE posted_date >= date('now', '-180 days') "
                "ORDER BY random() LIMIT ?", (self.sample_size,),
            ).fetchall()
            for (doc_id,) in rows:
                url = f"https://www.regulations.gov/document/{doc_id}"
                code, err, ms = self.client.head_or_get(url)
                self.record("external", f"regulations_document/{doc_id}",
                            code in LINK_OK_CODES, Severity.WARNING,
                            f"HTTP {code}" if code else err, ms)

            # Regulations.gov dockets
            rows = oc.execute(
                "SELECT id FROM dockets ORDER BY random() LIMIT ?",
                (self.sample_size,),
            ).fetchall()
            for (dk,) in rows:
                url = f"https://www.regulations.gov/docket/{dk}"
                code, err, ms = self.client.head_or_get(url)
                self.record("external", f"regulations_docket/{dk}",
                            code in LINK_OK_CODES, Severity.WARNING,
                            f"HTTP {code}" if code else err, ms)
        finally:
            oc.close()

    # ---- Category 9: form viewer (990 filings on R2) ----
    def check_form_viewer(self):
        log.info("== Form viewer ==")
        try:
            nc = sqlite3.connect(f"file:{NINETY_DB}?mode=ro", uri=True)
        except Exception as e:
            self.record("forms", "990-db", False, Severity.CRITICAL,
                        f"cannot open {NINETY_DB}: {e}")
            return
        try:
            rows = nc.execute(
                "SELECT object_id FROM returns WHERE object_id IS NOT NULL "
                "ORDER BY random() LIMIT ?", (self.sample_size,),
            ).fetchall()
            for (oid,) in rows:
                url = f"{FORMS_BASE}/{oid}.html"
                code, err, ms, body = self.client.get(url)
                size = len(body) if body else 0
                passed = 200 <= code < 400 and size > 5000
                self.record("forms", f"{oid}.html", passed, Severity.WARNING,
                            f"HTTP {code}, {size:,} bytes" if code else err, ms)
        finally:
            nc.close()

    # ---- Category 10: REST APIs ----
    def check_rest_apis(self):
        log.info("== REST APIs ==")
        targets = [
            (f"{OPENREGS_BASE}/api/search/members?q=pelosi", "api/members"),
            (f"{OPENREGS_BASE}/api/search/legislation?q=climate", "api/legislation"),
            (f"{OPENREGS_BASE}/api/search/comments?q=regulation", "api/comments"),
        ]
        for url, name in targets:
            code, err, ms, body = self.client.get(url)
            size = len(body) if body else 0
            passed = 200 <= code < 400 and size > 50
            self.record("api", name, passed, Severity.WARNING,
                        f"HTTP {code}, {size} bytes" if code else err, ms)

    # ---- Category 11: TLS expiry ----
    def check_tls_expiry(self):
        log.info("== TLS expiry ==")
        hosts = ["regs.datadawn.org", "data.datadawn.org",
                 "forms.datadawn.org", "datadawn.org"]
        if self.dry_run:
            for h in hosts:
                self.record("tls", h, True, Severity.INFO, "dry-run skipped")
            return
        for host in hosts:
            try:
                ctx = ssl.create_default_context()
                with socket.create_connection((host, 443), timeout=10) as sock:
                    with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                        cert = ssock.getpeercert()
                not_after = dt.datetime.strptime(
                    cert["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=dt.timezone.utc)
                days_left = (not_after - dt.datetime.now(dt.timezone.utc)).days
                if days_left < TLS_CRIT_DAYS:
                    sev, passed = Severity.CRITICAL, False
                elif days_left < TLS_WARN_DAYS:
                    sev, passed = Severity.WARNING, False
                else:
                    sev, passed = Severity.INFO, True
                self.record("tls", host, passed, sev,
                            f"expires {not_after:%Y-%m-%d} ({days_left}d left)")
            except Exception as e:
                self.record("tls", host, False, Severity.WARNING,
                            f"check failed: {type(e).__name__}: {e}")

    # ---- Category 12: data freshness (local DB sanity) ----
    def check_freshness(self):
        log.info("== Data freshness ==")
        try:
            oc = sqlite3.connect(f"file:{OPENREGS_DB}?mode=ro", uri=True)
            row = oc.execute(
                "SELECT MAX(publication_date) FROM federal_register"
            ).fetchone()
            oc.close()
            latest = row[0] if row else None
            if latest:
                latest_dt = dt.datetime.fromisoformat(latest)
                age_days = (dt.datetime.now() - latest_dt).days
                # Weekly rebuild on Saturday; checking Sunday expect age <= ~3 days of FR publication lag.
                if age_days > 10:
                    self.record("freshness", "federal_register", False, Severity.WARNING,
                                f"latest FR doc is {age_days} days old ({latest})")
                else:
                    self.record("freshness", "federal_register", True, Severity.INFO,
                                f"latest FR doc {age_days} days old ({latest})")
            else:
                self.record("freshness", "federal_register", False, Severity.CRITICAL,
                            "no FR docs in local DB")
        except Exception as e:
            self.record("freshness", "federal_register", False, Severity.WARNING,
                        f"check failed: {e}")

    # ---- Orchestration ----
    def run(self):
        log.info(f"=== DataDawn Site Audit {self.started:%Y-%m-%d %H:%M:%S} "
                 f"{'(quick)' if self.quick else ''}{'(dry-run)' if self.dry_run else ''} ===")
        checks: list[tuple[str, Callable]] = [
            ("site_health", self.check_site_health),
            ("datasette_internals", self.check_datasette_internals),
            ("static_files", self.check_static_files),
            ("fts_sanity", self.check_fts_sanity),
            ("canned_queries", self.check_canned_queries),
            ("explore_pages", self.check_explore_pages),
            ("detail_pages", self.check_detail_pages),
            ("external_links", self.check_external_links),
            ("form_viewer", self.check_form_viewer),
            ("rest_apis", self.check_rest_apis),
            ("tls_expiry", self.check_tls_expiry),
            ("freshness", self.check_freshness),
        ]
        for name, fn in checks:
            try:
                fn()
            except Exception as e:
                log.exception(f"Check '{name}' crashed: {e}")
                self.record(name, "_crashed", False, Severity.CRITICAL,
                            f"{type(e).__name__}: {e}")

    # ---- Summary / exit ----
    def summarize(self) -> tuple[str, int]:
        by_cat: dict[str, list[Result]] = defaultdict(list)
        for r in self.results:
            by_cat[r.category].append(r)
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        crits = [r for r in self.results if not r.passed and r.severity == Severity.CRITICAL]
        warns = [r for r in self.results if not r.passed and r.severity == Severity.WARNING]

        lines = []
        lines.append(f"DataDawn Site Audit — {self.started:%Y-%m-%d %H:%M %Z}")
        lines.append(f"Results: {passed}/{total} passed, "
                     f"{len(crits)} critical, {len(warns)} warnings")
        lines.append("")
        for cat in sorted(by_cat.keys()):
            cat_passed = sum(1 for r in by_cat[cat] if r.passed)
            lines.append(f"  {cat}: {cat_passed}/{len(by_cat[cat])}")
        lines.append("")
        if crits:
            lines.append("CRITICAL FAILURES:")
            for r in crits:
                lines.append(f"  - {r.category}/{r.name}: {r.detail}")
            lines.append("")
        if warns:
            lines.append("WARNINGS:")
            for r in warns[:50]:  # cap at 50 to respect hc.io 10KB body limit
                lines.append(f"  - {r.category}/{r.name}: {r.detail}")
            if len(warns) > 50:
                lines.append(f"  ... and {len(warns) - 50} more warnings (see log)")
            lines.append("")
        elapsed = (dt.datetime.now() - self.started).total_seconds()
        lines.append(f"Elapsed: {elapsed:.1f}s")
        exit_code = EXIT_CRITICAL if crits else EXIT_OK
        lines.append(f"Exit: {exit_code}")

        summary = "\n".join(lines)

        # Also write full JSON for later inspection
        json_path = LOG_DIR / "site_audit_latest.json"
        payload = {
            "started": self.started.isoformat(),
            "elapsed_seconds": elapsed,
            "total": total, "passed": passed,
            "critical": len(crits), "warnings": len(warns),
            "results": [
                {
                    "category": r.category, "name": r.name, "passed": r.passed,
                    "severity": r.severity.value, "detail": r.detail,
                    "elapsed_ms": r.elapsed_ms,
                }
                for r in self.results
            ],
        }
        try:
            json_path.write_text(json.dumps(payload, indent=2))
        except Exception as e:
            log.warning(f"Could not write JSON summary: {e}")

        return summary, exit_code


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--quick", action="store_true", help="Smaller sample sizes for fast local testing")
    p.add_argument("--dry-run", action="store_true", help="Plan checks, skip all HTTP")
    p.add_argument("--no-color", action="store_true", help="Disable ANSI color in output")
    p.add_argument("--seed", type=int, default=None, help="Random seed (for reproducible sampling)")
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    audit = Audit(dry_run=args.dry_run, quick=args.quick, no_color=args.no_color)
    audit.run()
    summary, exit_code = audit.summarize()
    print()
    print(summary)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
