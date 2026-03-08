#!/usr/bin/env python3
"""
Phase 18: Download executive nominations and treaties from Congress.gov API.

Downloads:
  - Executive nominations (~44,700): nominee info, position, agency, actions, votes
  - Treaties (~785): parties, topics, resolution text, actions

Source: Congress.gov API (api.congress.gov/v3)
Auth: Congress.gov API key (in config.json)

Usage:
    python3 18_nominations_treaties.py                     # incremental
    python3 18_nominations_treaties.py --full               # download all
    python3 18_nominations_treaties.py --nominations-only   # just nominations
    python3 18_nominations_treaties.py --treaties-only      # just treaties
    python3 18_nominations_treaties.py --limit 100          # stop after 100
    python3 18_nominations_treaties.py --dry-run            # list without downloading
"""

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Configuration ===
CONGRESS_API = "https://api.congress.gov/v3"
PROJECT_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_DIR / "scripts" / "config.json"

_config = json.loads(CONFIG_FILE.read_text())
API_KEY = _config.get("congress_gov_api_key", "")
NOMINATIONS_DIR = PROJECT_DIR / "nominations"
TREATIES_DIR = PROJECT_DIR / "treaties"
LOG_DIR = PROJECT_DIR / "logs"
STATE_FILE = LOG_DIR / "nominations_treaties_state.json"

PAGE_SIZE = 250
REQUEST_DELAY = 0.5

# Congress range for nominations (100-119)
CONGRESS_MIN = 100
CONGRESS_MAX = 119

# === Logging ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
NOMINATIONS_DIR.mkdir(parents=True, exist_ok=True)
TREATIES_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("nominations")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh = logging.FileHandler(LOG_DIR / "nominations_treaties.log")
_fh.setFormatter(_fmt)
log.addHandler(_fh)
try:
    _sh = logging.StreamHandler(sys.stdout)
    _sh.setFormatter(_fmt)
    log.addHandler(_sh)
except Exception:
    pass


# === HTTP Session ===
def create_session():
    session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=4))
    session.headers.update({"User-Agent": "OpenRegs/1.0 (nominations and treaties)"})
    return session


# === State Management ===
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "nominations_last_congress": CONGRESS_MIN - 1,
        "treaties_last_congress": 88,
        "nominations_downloaded": 0,
        "treaties_downloaded": 0,
    }


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# === Nominations ===
def fetch_nominations_for_congress(session, congress, limit=None):
    """Fetch all nomination list entries for a given congress."""
    nominations = []
    offset = 0

    while True:
        if limit and len(nominations) >= limit:
            return nominations[:limit]

        params = {
            "api_key": API_KEY,
            "limit": PAGE_SIZE,
            "offset": offset,
            "format": "json",
        }
        resp = session.get(f"{CONGRESS_API}/nomination/{congress}", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("nominations", [])
        if not items:
            break

        for item in items:
            number = item.get("number")
            part = item.get("partNumber", "")
            nom_id = f"{congress}-{number}"
            if part:
                nom_id = f"{congress}-{number}-{part}"

            nominations.append({
                "id": nom_id,
                "congress": congress,
                "number": number,
                "partNumber": part,
                "citation": item.get("citation", ""),
                "description": item.get("description", ""),
                "organization": item.get("organization", ""),
                "receivedDate": item.get("receivedDate", ""),
                "nominationType": item.get("nominationType", {}),
                "latestAction": item.get("latestAction", {}),
                "url": item.get("url", ""),
            })

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

        if not data.get("pagination", {}).get("next"):
            break

    return nominations


def download_nomination_detail(session, congress, number, part=None):
    """Download detail for a single nomination, including actions and nominees."""
    # Base detail
    nom_path = f"{congress}/{number}"
    if part:
        nom_path = f"{congress}/{number}-{part}"

    params = {"api_key": API_KEY, "format": "json"}
    resp = session.get(f"{CONGRESS_API}/nomination/{nom_path}", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    nom = data.get("nomination", data)

    result = {
        "congress": congress,
        "number": number,
        "partNumber": part or "",
        "citation": nom.get("citation", ""),
        "description": nom.get("description", ""),
        "organization": nom.get("organization", ""),
        "receivedDate": nom.get("receivedDate", ""),
        "authorityDate": nom.get("authorityDate", ""),
        "isCivilian": nom.get("nominationType", {}).get("isCivilian", False),
        "isMilitary": nom.get("nominationType", {}).get("isMilitary", False),
        "latestAction": nom.get("latestAction", {}),
    }

    # Parse status from latest action
    action_text = (result["latestAction"].get("text") or "").lower()
    if "confirmed" in action_text:
        result["status"] = "Confirmed"
    elif "withdrawn" in action_text:
        result["status"] = "Withdrawn"
    elif "returned to" in action_text:
        result["status"] = "Returned"
    elif "rejected" in action_text or "not confirmed" in action_text:
        result["status"] = "Rejected"
    else:
        result["status"] = "Pending"

    # Parse vote tally from action text if present
    vote_match = re.search(r'(\d+)\s*-\s*(\d+)', result["latestAction"].get("text", ""))
    if vote_match and "vote" in action_text.lower():
        result["vote_yea"] = int(vote_match.group(1))
        result["vote_nay"] = int(vote_match.group(2))

    # Fetch actions
    time.sleep(REQUEST_DELAY)
    try:
        resp = session.get(f"{CONGRESS_API}/nomination/{nom_path}/actions",
                          params={"api_key": API_KEY, "format": "json", "limit": 250}, timeout=30)
        resp.raise_for_status()
        actions_data = resp.json()
        result["actions"] = actions_data.get("actions", [])
    except Exception:
        result["actions"] = []

    # Fetch nominees (only for civilian nominations, not bulk military)
    if result["isCivilian"] and not nom.get("isList"):
        time.sleep(REQUEST_DELAY)
        try:
            nominees_info = nom.get("nominees", {})
            if isinstance(nominees_info, dict) and nominees_info.get("url"):
                resp = session.get(nominees_info["url"],
                                  params={"api_key": API_KEY, "format": "json"}, timeout=30)
                resp.raise_for_status()
                nominees_data = resp.json()
                result["nominees"] = nominees_data.get("nominees", [])
            elif isinstance(nominees_info, list):
                result["nominees"] = nominees_info
            else:
                result["nominees"] = []
        except Exception:
            result["nominees"] = []
    else:
        result["nominees"] = []

    return result


# === Treaties ===
def fetch_treaties_for_congress(session, congress, limit=None):
    """Fetch all treaty list entries for a given congress."""
    treaties = []
    offset = 0

    while True:
        if limit and len(treaties) >= limit:
            return treaties[:limit]

        params = {
            "api_key": API_KEY,
            "limit": PAGE_SIZE,
            "offset": offset,
            "format": "json",
        }
        resp = session.get(f"{CONGRESS_API}/treaty/{congress}", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("treaties", [])
        if not items:
            break

        for item in items:
            treaty_id = f"{congress}-{item.get('number', '')}"
            if item.get("suffix"):
                treaty_id += f"-{item['suffix']}"

            treaties.append({
                "id": treaty_id,
                "congress": congress,
                "number": item.get("number"),
                "suffix": item.get("suffix", ""),
                "topic": item.get("topic", ""),
                "transmittedDate": item.get("transmittedDate", ""),
                "url": item.get("url", ""),
            })

        offset += PAGE_SIZE
        time.sleep(REQUEST_DELAY)

        if not data.get("pagination", {}).get("next"):
            break

    return treaties


def download_treaty_detail(session, congress, number):
    """Download detail for a single treaty."""
    params = {"api_key": API_KEY, "format": "json"}
    resp = session.get(f"{CONGRESS_API}/treaty/{congress}/{number}", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    treaty = data.get("treaty", data)
    # API returns treaty as a list — take first element
    if isinstance(treaty, list):
        treaty = treaty[0] if treaty else {}

    result = {
        "congress": congress,
        "number": number,
        "topic": treaty.get("topic", ""),
        "transmittedDate": treaty.get("transmittedDate", ""),
        "inForceDate": treaty.get("inForceDate"),
        "congressConsidered": treaty.get("congressConsidered"),
        "congressReceived": treaty.get("congressReceived"),
    }

    # Titles
    titles = treaty.get("titles", [])
    result["titles"] = [{"title": t.get("title", ""), "type": t.get("titleType", "")} for t in titles]
    result["title"] = titles[0].get("title", "") if titles else treaty.get("topic", "")

    # Countries/parties
    countries = treaty.get("countriesParties", [])
    result["countries"] = [c.get("name", c) if isinstance(c, dict) else str(c) for c in countries]

    # Index terms
    terms = treaty.get("indexTerms", [])
    result["indexTerms"] = [t.get("name", t) if isinstance(t, dict) else str(t) for t in terms]

    # Resolution text (can be very long HTML)
    result["resolutionText"] = treaty.get("resolutionText", "")

    # Fetch actions
    time.sleep(REQUEST_DELAY)
    try:
        actions_ref = treaty.get("actions", {})
        if isinstance(actions_ref, dict) and actions_ref.get("url"):
            resp = session.get(actions_ref["url"],
                              params={"api_key": API_KEY, "format": "json", "limit": 250}, timeout=30)
            resp.raise_for_status()
            result["actions"] = resp.json().get("actions", [])
        elif isinstance(actions_ref, list):
            result["actions"] = actions_ref
        else:
            result["actions"] = []
    except Exception:
        result["actions"] = []

    return result


# === Main ===
def main():
    parser = argparse.ArgumentParser(description="Download nominations and treaties from Congress.gov")
    parser.add_argument("--full", action="store_true", help="Download all from beginning")
    parser.add_argument("--nominations-only", action="store_true", help="Only download nominations")
    parser.add_argument("--treaties-only", action="store_true", help="Only download treaties")
    parser.add_argument("--limit", type=int, help="Stop after N items per type")
    parser.add_argument("--dry-run", action="store_true", help="List without downloading details")
    args = parser.parse_args()

    if not API_KEY:
        log.error("No congress_gov_api_key in config.json")
        sys.exit(1)

    state = load_state()
    session = create_session()

    do_nominations = not args.treaties_only
    do_treaties = not args.nominations_only

    # === Nominations ===
    if do_nominations:
        log.info("=== Nominations Download ===")
        start_congress = CONGRESS_MIN if args.full else max(state["nominations_last_congress"], CONGRESS_MIN)
        existing = {f.stem for f in NOMINATIONS_DIR.glob("*.json")}
        total_downloaded = 0
        total_errors = 0

        for congress in range(start_congress, CONGRESS_MAX + 1):
            log.info(f"  Congress {congress}...")
            try:
                nom_list = fetch_nominations_for_congress(session, congress, limit=args.limit)
            except Exception as e:
                log.warning(f"  Congress {congress}: error listing nominations: {e}")
                continue

            if not nom_list:
                log.info(f"  Congress {congress}: 0 nominations")
                continue

            log.info(f"  Congress {congress}: {len(nom_list)} nominations")

            if args.dry_run:
                for n in nom_list[:5]:
                    log.info(f"    {n['citation']}: {n['description'][:70]}")
                continue

            to_download = [n for n in nom_list if n["id"] not in existing]
            if args.limit:
                remaining = args.limit - total_downloaded
                if remaining <= 0:
                    break
                to_download = to_download[:remaining]

            for nom in to_download:
                try:
                    detail = download_nomination_detail(
                        session, nom["congress"], nom["number"],
                        part=nom.get("partNumber") or None
                    )
                    out_file = NOMINATIONS_DIR / f"{nom['id']}.json"
                    out_file.write_text(json.dumps(detail, indent=2, ensure_ascii=False))
                    total_downloaded += 1
                    existing.add(nom["id"])

                    if total_downloaded % 100 == 0:
                        log.info(f"    Progress: {total_downloaded} downloaded, {total_errors} errors")

                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 404:
                        log.debug(f"    {nom['id']}: 404")
                    else:
                        log.error(f"    {nom['id']}: {e}")
                    total_errors += 1
                except Exception as e:
                    log.error(f"    {nom['id']}: {e}")
                    total_errors += 1

                time.sleep(REQUEST_DELAY)

            state["nominations_last_congress"] = congress
            state["nominations_downloaded"] = len(existing)
            save_state(state)

        log.info(f"  Nominations complete: {total_downloaded} downloaded, {total_errors} errors, {len(existing)} total on disk")

    # === Treaties ===
    if do_treaties:
        log.info("=== Treaties Download ===")
        start_congress = 89 if args.full else max(state.get("treaties_last_congress", 88), 89)
        existing = {f.stem for f in TREATIES_DIR.glob("*.json")}
        total_downloaded = 0
        total_errors = 0

        for congress in range(start_congress, CONGRESS_MAX + 1):
            log.info(f"  Congress {congress}...")
            try:
                treaty_list = fetch_treaties_for_congress(session, congress, limit=args.limit)
            except Exception as e:
                log.warning(f"  Congress {congress}: error listing treaties: {e}")
                continue

            if not treaty_list:
                log.info(f"  Congress {congress}: 0 treaties")
                continue

            log.info(f"  Congress {congress}: {len(treaty_list)} treaties")

            if args.dry_run:
                for t in treaty_list[:5]:
                    log.info(f"    {t['id']}: {t['topic'][:70]}")
                continue

            to_download = [t for t in treaty_list if t["id"] not in existing]
            if args.limit:
                remaining = args.limit - total_downloaded
                if remaining <= 0:
                    break
                to_download = to_download[:remaining]

            for treaty in to_download:
                try:
                    detail = download_treaty_detail(session, treaty["congress"], treaty["number"])
                    out_file = TREATIES_DIR / f"{treaty['id']}.json"
                    out_file.write_text(json.dumps(detail, indent=2, ensure_ascii=False))
                    total_downloaded += 1
                    existing.add(treaty["id"])

                except requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 404:
                        log.debug(f"    {treaty['id']}: 404")
                    else:
                        log.error(f"    {treaty['id']}: {e}")
                    total_errors += 1
                except Exception as e:
                    log.error(f"    {treaty['id']}: {e}")
                    total_errors += 1

                time.sleep(REQUEST_DELAY)

            state["treaties_last_congress"] = congress
            state["treaties_downloaded"] = len(existing)
            save_state(state)

        log.info(f"  Treaties complete: {total_downloaded} downloaded, {total_errors} errors, {len(existing)} total on disk")

    log.info("=== All done ===")


if __name__ == "__main__":
    main()
