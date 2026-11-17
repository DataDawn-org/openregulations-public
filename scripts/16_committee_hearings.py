#!/usr/bin/env python3
"""
Phase 16: Download Congressional Committee Hearing metadata from GovInfo.

Downloads MODS XML metadata for all Congressional Hearings (CHRG collection)
from the GovInfo API. Extracts structured data including:
  - Hearing title, date, chamber, congress, committee
  - Committee members present (matchable to bioguide_id)
  - Witnesses who testified (name, title, organization)
  - Bill/legislation references
  - Statutory/USC references

This creates the connective tissue between lobbying, legislation, and members:
a lobbyist registered on issue X testifies before committee Y about bill Z.

Source: GovInfo API (api.govinfo.gov)
  - Collection listing: /collections/CHRG
  - Per-package MODS: /packages/{packageId}/mods

API key: GOVINFO_API_KEY from the environment, else govinfo_api_key in scripts/config.json, sent only in the
X-Api-Key header. With neither, the script refuses before sending anything: no DEMO_KEY fallback (owner queue
298; decisions_log §699). ~46,000 hearings total.

Default runs list EVERY package (about 490 listing requests) and download only those not on disk. govinfo's
/published/{date} listing filters by ISSUE date, and govinfo publishes old hearings late, often decades after
issue, so no issue-date window is complete: on 2026-10-26 a full listing found 2,537 packages missing from disk,
most published Sept-Oct 2026 (owner queue 808; the drafter's call B, relay datadawn green #3). Before 2026-10-26
the run passed the newest lastModified TIME as the issue date and skipped even recent packages. The state keeps
max_date_issued as a record only. Test: selftest_16_hearings_key_watermark.py.

Hearings the source removed (decisions_log §844, owner queue 997): after a complete scheduled listing,
a hearing on disk that govinfo no longer lists is a candidate only (§144, §499 (3): listing absence is not evidence).
It is confirmed removed only by two per-ID summary lookups, at least a minute apart, that both answer 404, with both
gate arms passing before and after (a listed hearing answers 200; an impossible ID answers 404). Confirmations go to
hearing_source_confirmations.json, which 05_build_database.py reads to mark those hearings (kept up, the note in source_status: decisions_log §844 A1,
§862); nothing local is deleted.
Test: selftest_16_hearings_source_removed.py.

Usage:
    python3 16_committee_hearings.py                    # list everything, download what is missing
    python3 16_committee_hearings.py --full             # the same listing (kept for existing callers)
    python3 16_committee_hearings.py --since 2020-01-01 # issued on or after a specific date
    python3 16_committee_hearings.py --congress 118     # specific congress only
"""

import argparse
import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, str(Path(__file__).resolve().parent))
import feed_hc  # noqa: E402  (the feed's health check: decisions_log §591, §699)

# === Configuration ===
GOVINFO_API = "https://api.govinfo.gov"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"

OUTPUT_DIR = PROJECT_DIR / "hearings"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "hearings_state.json"

PAGE_SIZE = 100  # max per GovInfo API page
REQUEST_DELAY = 0.5  # seconds between API calls (be polite)
MODS_NS = {"m": "http://www.loc.gov/mods/v3"}

# Hearings the source removed (decisions_log §844). The registry sits beside hearings/, never inside it: 05 imports
# every *.json under hearings/ as a hearing.
REMOVED_REGISTRY = PROJECT_DIR / "hearing_source_confirmations.json"
SUMMARY_URL = GOVINFO_API + "/packages/{}/summary"
IMPOSSIBLE_ID = "CHRG-999hhrg99999999"  # well-formed, cannot exist: govinfo answers 404 (measured 2026-11-05)
MAX_NEW_CANDIDATES = 1000   # more unlisted hearings needing a lookup than this means a short listing: refuse, look up none
CONFIRM_SPACING_S = 60      # at least this long between an ID's two lookups
NEW_REGISTRY_MAX_AGE_DAYS = 120  # §151 R5's ruled value, written into a NEW registry; an existing file keeps its own
REGISTRY_DOC = ("§844 hearings govinfo no longer posts: per-ID confirmations. Written by "
                "scripts/16_committee_hearings.py on its scheduled run, READ by 05_build_database.py "
                "(mark_source_removed_hearings), which reads `confirmations` only. `relisted` keeps the record of "
                "each confirmation cleared because govinfo posted the hearing again. decisions_log §844, §144, "
                "§146, §151 R5.")

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("hearings")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "hearings.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


# === API key ===
def load_api_key():
    """GOVINFO_API_KEY from the environment, else govinfo_api_key in config.json. Neither (or the literal
    DEMO_KEY): refuse with exit 2 before any request is sent (owner queue 298; decisions_log §699)."""
    key = os.environ.get("GOVINFO_API_KEY", "").strip()
    if not key and CONFIG_FILE.exists():
        try:
            key = (json.loads(CONFIG_FILE.read_text()).get("govinfo_api_key") or "").strip()
        except (OSError, ValueError) as e:
            log.error(f"cannot read {CONFIG_FILE}: {type(e).__name__}")
    if not key or key == "DEMO_KEY":
        log.error("REFUSED: no govinfo API key (GOVINFO_API_KEY in the environment, or govinfo_api_key in "
                  "scripts/config.json); nothing sent. DEMO_KEY is not a fallback (owner queue 298).")
        sys.exit(2)
    return key


# === HTTP Session ===
def create_session(api_key):
    """Create requests session with retry logic; the key travels only in the X-Api-Key header."""
    session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=4))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (committee hearings project)",
                            "X-Api-Key": api_key})
    return session


# === State Management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"last_modified": "1900-01-01T00:00:00Z", "packages_downloaded": 0}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def newest_issued_on_disk():
    """Newest date_issued among the hearing files already downloaded ('' if none)."""
    newest = ""
    for f in OUTPUT_DIR.glob("*.json"):
        try:
            d = (json.loads(f.read_text()).get("date_issued") or "")[:10]
        except (OSError, ValueError):
            continue
        if d > newest:
            newest = d
    return newest


# === GovInfo API ===
def fetch_package_ids(session, since_dt, limit=None):
    """Fetch all CHRG package IDs using the published endpoint.

    Uses cursor-based pagination (offsetMark) which has no 10K limit,
    unlike the collections endpoint's numeric offset pagination.
    """
    package_ids = []
    seen_ids = set()

    # Convert since_dt to date for the published endpoint (which filters by dateIssued)
    since_date = since_dt.split("T")[0]
    log.info(f"Fetching CHRG package list (issued since {since_date})...")

    url = (
        f"{GOVINFO_API}/published/{since_date}"
        f"?collection=CHRG&pageSize={PAGE_SIZE}&offsetMark=*"
    )

    while url:
        if limit and len(package_ids) >= limit:
            log.info(f"Reached limit of {limit} packages")
            return package_ids

        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        packages = data.get("packages", [])
        if not packages:
            break

        for pkg in packages:
            pid = pkg.get("packageId")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                package_ids.append({
                    "packageId": pid,
                    "lastModified": pkg.get("lastModified", ""),
                    "title": pkg.get("title", ""),
                })

        total = data.get("count", "?")
        log.info(f"  Fetched {len(package_ids)} / {total} package IDs")

        # Cursor-based pagination — nextPage URL includes the offsetMark
        next_page = data.get("nextPage")
        if next_page:
            url = next_page
        else:
            url = None

        time.sleep(REQUEST_DELAY)

    log.info(f"Found {len(package_ids)} total hearing packages")
    return package_ids


def parse_mods(xml_text):
    """Parse MODS XML into structured hearing data."""
    root = ET.fromstring(xml_text)
    hearing = {}

    # Title
    title_el = root.find(".//m:titleInfo/m:title", MODS_NS)
    hearing["title"] = title_el.text.strip() if title_el is not None and title_el.text else ""

    # Date
    date_el = root.find(".//m:originInfo/m:dateIssued", MODS_NS)
    hearing["date_issued"] = date_el.text.strip() if date_el is not None and date_el.text else ""

    # Extension fields (congress, session, chamber, etc.)
    for ext in root.findall(".//m:extension", MODS_NS):
        for child in ext:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child.text and child.text.strip():
                if tag == "congress":
                    hearing["congress"] = int(child.text.strip())
                elif tag == "session":
                    hearing["session"] = child.text.strip()
                elif tag == "chamber":
                    hearing["chamber"] = child.text.strip()
                elif tag == "collectionCode":
                    hearing["collection"] = child.text.strip()

    # Chamber from docClass if not in extension
    if "chamber" not in hearing:
        for ext in root.findall(".//m:extension", MODS_NS):
            for child in ext:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if tag == "waisDatabaseName" and child.text:
                    if "senate" in child.text.lower():
                        hearing["chamber"] = "Senate"
                    elif "house" in child.text.lower():
                        hearing["chamber"] = "House"

    # Pages
    for phys in root.findall(".//m:physicalDescription/m:extent", MODS_NS):
        if phys.text and "p." in phys.text:
            try:
                hearing["pages"] = int(phys.text.replace("p.", "").strip())
            except ValueError:
                pass

    # Committee — corporate name with role "associated name"
    committees = []
    for name in root.findall(".//m:name[@type='corporate']", MODS_NS):
        roles = [r.text for r in name.findall(".//m:roleTerm[@type='text']", MODS_NS) if r.text]
        if "associated name" in roles:
            parts = [p.text for p in name.findall("m:namePart", MODS_NS) if p.text]
            # Filter out "United States" and "Congress" to get committee name
            committee_parts = [p for p in parts if p not in ("United States", "Congress", "Senate", "House of Representatives", "House")]
            if committee_parts:
                # Also capture the chamber from the hierarchy
                chamber_from_parts = None
                if "Senate" in parts:
                    chamber_from_parts = "Senate"
                elif "House of Representatives" in parts or "House" in parts:
                    chamber_from_parts = "House"
                committees.append({
                    "name": ", ".join(committee_parts),
                    "chamber": chamber_from_parts,
                    "full_hierarchy": " > ".join(parts),
                })
    hearing["committees"] = committees

    # Members — personal names with description "United States Congressional Member"
    members = []
    for name in root.findall(".//m:name[@type='personal']", MODS_NS):
        desc = name.find("m:description", MODS_NS)
        if desc is not None and desc.text == "United States Congressional Member":
            parts = [p.text for p in name.findall("m:namePart", MODS_NS) if p.text]
            roles = [r.text for r in name.findall(".//m:roleTerm[@type='text']", MODS_NS) if r.text]
            members.append({
                "name": " ".join(parts),
                "role": roles[0] if roles else "committee member",
            })
    hearing["members"] = members

    # Witnesses — personal names with description "Hearing Witness"
    witnesses = []
    for name in root.findall(".//m:name[@type='personal']", MODS_NS):
        desc = name.find("m:description", MODS_NS)
        if desc is not None and desc.text == "Hearing Witness":
            parts = [p.text for p in name.findall("m:namePart", MODS_NS) if p.text]
            # The namePart often contains "LastName, FirstName, title, org, location"
            raw = ", ".join(parts)
            witnesses.append({"raw": raw})
    hearing["witnesses"] = witnesses

    # Bill references — relatedItem with identifier matching bill patterns
    bills = []
    statutes = []
    for related in root.findall(".//m:relatedItem", MODS_NS):
        id_el = related.find(".//m:identifier", MODS_NS)
        if id_el is not None and id_el.text:
            ref = id_el.text.strip()
            # Bill pattern: S. 1234, H.R. 5678, S.J.Res. 12, etc.
            if any(ref.startswith(p) for p in ("S.", "H.R.", "H.J.Res", "S.J.Res", "H.Con.Res", "S.Con.Res", "H.Res", "S.Res")):
                bills.append(ref)
            elif "U.S.C." in ref or "Public Law" in ref:
                statutes.append(ref)
    hearing["bill_references"] = bills
    hearing["statute_references"] = statutes

    # URLs
    for loc in root.findall(".//m:location/m:url", MODS_NS):
        label = loc.get("displayLabel", "")
        if "PDF" in label and loc.text:
            hearing["pdf_url"] = loc.text
        elif "HTML" in label and loc.text:
            hearing["html_url"] = loc.text
        elif "Content Detail" in label and loc.text:
            hearing["detail_url"] = loc.text

    return hearing


def download_hearing(session, package_id):
    """Download and parse MODS XML for a single hearing."""
    url = f"{GOVINFO_API}/packages/{package_id}/mods"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    hearing = parse_mods(resp.text)
    hearing["package_id"] = package_id
    return hearing


def parse_witness(raw):
    """Parse a witness raw string into name, title, org, location.

    Two MODS formats exist:
      Senate: "LastName, FirstName" (just name, sometimes with "Prepared statement")
      House:  "FirstName LastName, Credentials, Title, Organization, City, STATE"

    Detection heuristic: if the first comma-segment contains a space, it's
    "FirstName LastName" (House format). If not, it's "LastName" (Senate format)
    and the second segment is the first name.
    """
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) < 2:
        return {"name": raw, "title": "", "organization": "", "location": ""}

    # Detect format: does the first segment contain a space? (House = full name)
    first_has_space = " " in parts[0].strip()

    if not first_has_space and len(parts) >= 2:
        # Senate format: "LastName, FirstName [Prepared statement]"
        first_name = parts[1].replace("Prepared statement", "").strip()
        name = f"{first_name} {parts[0]}".strip()
        rest = parts[2:]
    else:
        # House format: "FirstName LastName, credentials, title, org..."
        name = parts[0]
        rest = parts[1:]

    # Classify remaining parts into credentials, title, org, location
    title_parts = []
    org_parts = []
    location_parts = []

    # Check if last part looks like a US state code (2 uppercase letters)
    if len(rest) >= 2 and len(rest[-1]) == 2 and rest[-1].isupper():
        location_parts = rest[-2:]  # city, STATE
        rest = rest[:-2]

    # Credentials: short items like "M.D.", "Ph.D.", "M.B.A.", "M.P.H.", "Esq."
    # Title: contains role keywords
    # Org suffixes: "Inc.", "LLC", "Ltd.", "Corp.", "Co." — attach to org, not title
    # Everything else: organization
    _role_kw = ("president", "director", "chairman", "chairwoman", "chief",
                "professor", "secretary", "commissioner", "vice", "officer",
                "counsel", "manager", "administrator", "executive", "founder",
                "co-founder", "ceo", "cfo", "coo", "cto", "former", "acting",
                "deputy", "assistant", "associate", "senior", "head", "member",
                "superintendent", "sheriff", "captain", "lieutenant", "sergeant",
                "inspector", "analyst", "advocate", "coordinator")
    _org_suffixes = ("inc.", "inc", "llc", "llc.", "ltd.", "ltd", "corp.", "corp",
                     "co.", "l.p.", "p.c.", "p.a.", "l.l.c.", "l.l.p.")

    for part in rest:
        p_lower = part.lower().strip()
        # Org suffix: attach to previous org part or start org
        if p_lower in _org_suffixes:
            if org_parts:
                org_parts[-1] = org_parts[-1] + ", " + part
            else:
                org_parts.append(part)
        # Credential: short, contains dots (but not org suffixes)
        elif len(part) <= 10 and ("." in part or p_lower in ("esq", "ret", "usn", "usa", "usaf", "usmc")):
            title_parts.append(part)
        elif any(kw in p_lower for kw in _role_kw):
            title_parts.append(part)
        else:
            org_parts.append(part)

    return {
        "name": name,
        "title": ", ".join(title_parts),
        "organization": ", ".join(org_parts),
        "location": ", ".join(location_parts),
    }


# === Hearings the source removed (decisions_log §844; owner queue 997) ===
def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _summary_status(session, package_id):
    """HTTP status of govinfo's per-package summary, or None when the lookup itself failed (never a 404)."""
    try:
        return session.get(SUMMARY_URL.format(package_id), timeout=30).status_code
    except Exception as e:
        log.warning(f"  removed-check: lookup of {package_id} failed ({type(e).__name__}); not confirmable this run")
        return None


def _write_registry(reg):
    """Write the registry whole (temp file, fsync, rename), then re-read it: the file is the record, not this call."""
    tmp = REMOVED_REGISTRY.with_name(REMOVED_REGISTRY.name + ".tmp")
    with open(tmp, "w") as f:
        f.write(json.dumps(reg, indent=1) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, REMOVED_REGISTRY)
    back = json.loads(REMOVED_REGISTRY.read_text())
    if ([e["id"] for e in back["confirmations"]] != [e["id"] for e in reg["confirmations"]]
            or [r["id"] for r in back.get("relisted", [])] != [r["id"] for r in reg.get("relisted", [])]):
        raise RuntimeError(f"{REMOVED_REGISTRY.name} re-read does not match what was written")


def confirm_removed(session, listed_ids, on_disk_ids):
    """Confirm, by ID, which hearings on disk govinfo has removed, and record them for 05 to mark (§844, §844 A1).

    Called only after a COMPLETE scheduled listing. A hearing on disk that the listing lacks is a candidate; it is
    recorded as not_posted only when two summary lookups at least CONFIRM_SPACING_S apart both answer 404 and the
    gate passes before and after (a listed hearing answers 200, an impossible ID 404). §146: the build applies
    what this writes with no approval step. §151 R5: an entry older than the file's max_age_days is looked up again
    while its hearing stays unlisted; only a fresh 404 pair renews it, and anything else leaves it as it was (the
    hearing stays marked: staleness fails toward marking). A confirmed hearing govinfo lists again (HEARING_RELISTED)
    is looked up the same way: two 200s at least CONFIRM_SPACING_S apart, gate passing before and after, move its
    record from `confirmations` to `relisted` (kept, never deleted), so the next build serves it again, as §144's
    comment path does on a confirmed lookup (relay datadawn green #3 item 2); anything else keeps it marked, and the
    next scheduled run looks again. The gate's live control is never a hearing the registry holds. Each ID's second
    lookup waits until CONFIRM_SPACING_S after its own first. Returns counts for the health check's note; writes
    nothing on any refusal."""
    res = {"unlisted": 0, "looked_up": 0, "confirmed_new": 0, "renewed": 0, "unconfirmed": 0, "relisted": 0,
           "relisted_cleared": 0, "relisted_held": 0, "entries": 0, "written": False, "refused": None,
           "needs_person": False}
    listed, on_disk = set(listed_ids), set(on_disk_ids)
    reg = {"_doc": REGISTRY_DOC, "max_age_days": NEW_REGISTRY_MAX_AGE_DAYS, "confirmations": [], "relisted": []}
    if REMOVED_REGISTRY.exists():
        try:
            reg = json.loads(REMOVED_REGISTRY.read_text())
            if not (isinstance(reg, dict) and isinstance(reg.get("confirmations"), list)
                    and isinstance(reg.get("max_age_days"), int) and not isinstance(reg.get("max_age_days"), bool)
                    and reg["max_age_days"] > 0
                    and all(isinstance(e, dict) and e.get("id") for e in reg["confirmations"])
                    and isinstance(reg.get("relisted", []), list)
                    and all(isinstance(r, dict) and r.get("id") for r in reg.get("relisted", []))):
                raise ValueError("unexpected shape")
        except (OSError, ValueError) as e:
            log.error(f"removed-check REFUSED: {REMOVED_REGISTRY.name} is unreadable or malformed "
                      f"({type(e).__name__}); left as it is, nothing looked up")
            res["refused"] = "registry unreadable"
            # Only a person can repair the file, so this refusal turns the health check red (main() counts it as an
            # error); the source's refusals below (the cap, the gate) stay notes (relay datadawn green #1 item 3, N2).
            res["needs_person"] = True
            return res
    by_id = {e["id"]: e for e in reg["confirmations"]}
    relisted_log = list(reg.get("relisted", []))
    res["entries"] = len(by_id)
    max_age = reg["max_age_days"]
    now = datetime.now(timezone.utc)

    def stale(e):
        try:
            return (now - datetime.fromisoformat(str(e.get("probed")).replace("Z", "+00:00"))).days > max_age
        except (TypeError, ValueError):
            return True

    relist = sorted(i for i in by_id if i in listed)
    res["relisted"] = len(relist)
    unlisted = sorted(on_disk - listed)
    res["unlisted"] = len(unlisted)
    to_check = [p for p in unlisted if p not in by_id or stale(by_id[p])]
    if not to_check and not relist:
        log.info(f"removed-check: {len(unlisted)} on disk but unlisted, all already confirmed; nothing to look up")
        return res
    if len(to_check) + len(relist) > MAX_NEW_CANDIDATES:
        log.error(f"removed-check REFUSED: {len(to_check):,} unlisted and {len(relist):,} re-listed hearings need a "
                  f"lookup (cap {MAX_NEW_CANDIDATES:,}); a listing that short is more likely broken than that many "
                  f"changes. Nothing looked up, registry unchanged")
        res["refused"] = f"{len(to_check) + len(relist)} candidates over the cap"
        return res
    # The live control must be a hearing nothing here would mark: never one the registry holds.
    held_ids = set(by_id)
    live_pool = sorted((listed & on_disk) - held_ids) or sorted(listed - held_ids)
    if not live_pool:
        res["refused"] = "no listed hearing for the live control"
        log.error("removed-check REFUSED: no listed hearing to use as the live control")
        return res
    live_id = live_pool[0]

    def gate():
        return {"live_id": live_id, "live_http": _summary_status(session, live_id),
                "impossible_id": IMPOSSIBLE_ID, "impossible_http": _summary_status(session, IMPOSSIBLE_ID)}

    def gate_ok(g):
        return g["live_http"] == 200 and g["impossible_http"] == 404

    g_before = gate()
    if not gate_ok(g_before):
        log.error(f"removed-check REFUSED: the gate failed before the lookups ({g_before}); nothing written")
        res["refused"] = "gate failed before"
        return res
    first, second, first_t = {}, {}, {}
    relist_set = set(relist)
    probe = to_check + relist          # disjoint: to_check is unlisted, relist is listed
    for pid in probe:
        first_t[pid] = time.monotonic()
        first[pid] = {"at": _now_iso(), "http": _summary_status(session, pid)}
        time.sleep(REQUEST_DELAY)
    for pid in probe:
        if first[pid]["http"] == (200 if pid in relist_set else 404):
            wait = first_t[pid] + CONFIRM_SPACING_S - time.monotonic()   # per ID: never less than the spacing
            if wait > 0:
                time.sleep(wait)
            second[pid] = {"at": _now_iso(), "http": _summary_status(session, pid)}
            time.sleep(REQUEST_DELAY)
    g_after = gate()
    if not gate_ok(g_after):
        log.error(f"removed-check REFUSED: the gate failed after the lookups ({g_after}); nothing written")
        res["refused"] = "gate failed after"
        return res
    res["looked_up"] = len(to_check)
    for pid in to_check:
        a, b = first[pid], second.get(pid)
        if a["http"] == 404 and b is not None and b["http"] == 404:
            res["renewed" if pid in by_id else "confirmed_new"] += 1
            by_id[pid] = {"id": pid, "verdict": "not_posted", "probed": b["at"], "gets": [a, b],
                          "gate": g_after, "gate_before": g_before}
        else:
            res["unconfirmed"] += 1
            log.info(f"  removed-check: {pid} is unlisted but not confirmed removed (lookups {a['http']}, "
                     f"{b['http'] if b else '-'}){'; its earlier confirmation is kept' if pid in by_id else ''}")
    for pid in relist:
        a, b = first[pid], second.get(pid)
        if a["http"] == 200 and b is not None and b["http"] == 200:
            res["relisted_cleared"] += 1
            relisted_log.append({"id": pid, "verdict": "posted_again", "probed": b["at"], "gets": [a, b],
                                 "gate": g_after, "gate_before": g_before, "was": by_id.pop(pid)})
            log.warning(f"HEARING_RELISTED id={pid} CLEARED: govinfo lists it again and two lookups at least "
                        f"{CONFIRM_SPACING_S}s apart answered 200; its record moves to `relisted`, so the next build "
                        f"serves it unmarked")
        else:
            res["relisted_held"] += 1
            log.warning(f"HEARING_RELISTED id={pid} HELD: govinfo lists it again, but its lookups answered "
                        f"{a['http']}, {b['http'] if b else '-'}; its confirmation is kept (still marked), and the "
                        f"next scheduled run looks again")
    reg["confirmations"] = [by_id[k] for k in sorted(by_id)]
    reg["relisted"] = relisted_log
    _write_registry(reg)
    res["written"] = True
    res["entries"] = len(by_id)
    log.info(f"removed-check: {len(unlisted)} unlisted; looked up {len(to_check)}; {res['confirmed_new']} newly "
             f"confirmed, {res['renewed']} renewed, {res['unconfirmed']} not confirmed; registry holds "
             f"{len(by_id)}; {res['relisted']} listed again ({res['relisted_cleared']} unmarked again, "
             f"{res['relisted_held']} still marked)")
    return res


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download committee hearing metadata from GovInfo")
    parser.add_argument("--full", action="store_true", help="Download all hearings from the beginning")
    parser.add_argument("--since", type=str, help="Download hearings modified since this date (YYYY-MM-DD)")
    parser.add_argument("--congress", type=int, help="Only download hearings from this congress number")
    parser.add_argument("--limit", type=int, help="Stop after downloading this many hearings (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="List packages without downloading")
    args = parser.parse_args()

    session = create_session(load_api_key())
    state = load_state()

    # Newest issue date held: a record only (the state's, or on the first run after 2026-10-26, the newest on disk).
    watermark = state.get("max_date_issued") or newest_issued_on_disk()

    # Start date (an ISSUE date: /published/ filters by dateIssued). The default lists everything, because old
    # hearings are published late; --since narrows a hand run.
    if args.since and not args.full:
        since_dt = f"{args.since}T00:00:00Z"
    else:
        since_dt = "1900-01-01T00:00:00Z"

    log.info(f"=== Committee Hearings Download ===")
    log.info(f"Since: {since_dt}")

    # The feed's health check (feed_hc.py; decisions_log §591, §699): only a complete run speaks for the feed, so a
    # hand run narrowed by --since, --limit or --congress, or a dry run, does not ping.
    scheduled = not (args.dry_run or args.limit or args.congress or (args.since and not args.full))

    # Fetch package list
    try:
        package_ids = fetch_package_ids(session, since_dt, limit=args.limit)
    except Exception:
        if scheduled:
            feed_hc.report("hearings", {"listed": None, "errors": 1, "note": "the govinfo listing raised"},
                           log=log.info)
        raise

    if args.congress:
        # Filter by congress — package IDs contain congress number (e.g., CHRG-118shrg12345)
        filtered = []
        for pkg in package_ids:
            pid = pkg["packageId"]
            # Extract congress number from package ID: CHRG-{congress}{chamber}hrg{number}
            try:
                congress_str = pid.split("-")[1][:3]
                congress_num = int(congress_str.rstrip("shHSr"))
                if congress_num == args.congress:
                    filtered.append(pkg)
            except (IndexError, ValueError):
                filtered.append(pkg)  # keep if we can't parse
        log.info(f"Filtered to {len(filtered)} packages for congress {args.congress}")
        package_ids = filtered

    if args.dry_run:
        for pkg in package_ids[:20]:
            log.info(f"  {pkg['packageId']}: {pkg['title'][:80]}")
        log.info(f"Total: {len(package_ids)} packages (dry run, not downloading)")
        return

    # Check which packages we already have
    existing = set()
    for f in OUTPUT_DIR.glob("*.json"):
        existing.add(f.stem)

    to_download = [p for p in package_ids if p["packageId"] not in existing]
    log.info(f"Already have {len(existing)} hearings, {len(to_download)} new to download")

    if args.limit:
        to_download = to_download[:args.limit]
        log.info(f"Limited to {args.limit} downloads")

    # Download and parse each hearing
    downloaded = 0
    errors = 0
    max_modified = state["last_modified"]

    for i, pkg in enumerate(to_download):
        pid = pkg["packageId"]
        try:
            hearing = download_hearing(session, pid)

            # Parse witnesses
            parsed_witnesses = []
            for w in hearing.get("witnesses", []):
                parsed = parse_witness(w["raw"])
                parsed["raw"] = w["raw"]
                parsed_witnesses.append(parsed)
            hearing["witnesses"] = parsed_witnesses

            # Save
            out_file = OUTPUT_DIR / f"{pid}.json"
            out_file.write_text(json.dumps(hearing, indent=2, ensure_ascii=False))

            downloaded += 1

            # Track latest modification time (a record only; the listing never starts from it)
            if pkg["lastModified"] > max_modified:
                max_modified = pkg["lastModified"]
            # ... and the newest issue date, the listing's watermark
            issued = (hearing.get("date_issued") or "")[:10]
            if issued > watermark:
                watermark = issued

            if downloaded % 50 == 0:
                log.info(f"  Progress: {downloaded}/{len(to_download)} downloaded, {errors} errors")
                state["last_modified"] = max_modified
                state["max_date_issued"] = watermark
                state["packages_downloaded"] = len(existing) + downloaded
                save_state(state)

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.warning(f"  {pid}: 404 Not Found (skipping)")
            else:
                log.error(f"  {pid}: HTTP error: {e}")
            errors += 1
        except Exception as e:
            log.error(f"  {pid}: Error: {e}")
            errors += 1

        time.sleep(REQUEST_DELAY)

    # Final state save
    state["last_modified"] = max_modified
    state["max_date_issued"] = watermark
    state["packages_downloaded"] = len(existing) + downloaded
    save_state(state)

    # Hearings the source removed (§844): only a complete scheduled listing can show a drop.
    removed_note = ""
    if scheduled:
        try:
            rc = confirm_removed(session, {p["packageId"] for p in package_ids}, existing)
            removed_note = (f"; removed-check: {rc['unlisted']} unlisted, {rc['confirmed_new']} newly confirmed, "
                            f"{rc['entries']} in registry, {rc['relisted']} listed again "
                            f"({rc['relisted_cleared']} unmarked again, {rc['relisted_held']} still marked)"
                            + (f", REFUSED ({rc['refused']})" if rc["refused"] else ""))
            if rc.get("needs_person"):
                errors += 1
                removed_note += "; a person must repair the registry"
        except Exception as e:
            log.error(f"removed-check raised {type(e).__name__}: {e}; the registry is as the last good run left it")
            removed_note = f"; removed-check raised {type(e).__name__}"
            errors += 1

    log.info(f"=== Complete ===")
    log.info(f"Downloaded: {downloaded}")
    log.info(f"Errors: {errors}")
    log.info(f"Total on disk: {len(existing) + downloaded}")

    if scheduled:
        feed_hc.report("hearings", {"listed": len(package_ids), "errors": errors, "added": downloaded,
                                    "note": f"{len(existing) + downloaded} on disk{removed_note}"}, log=log.info)
    else:
        log.info("feed_hc: not pinged (a hand run narrowed by --since, --limit or --congress)")


if __name__ == "__main__":
    main()
