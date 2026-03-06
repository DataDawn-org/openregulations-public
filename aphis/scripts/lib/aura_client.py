"""
Shared Aura API client for the USDA APHIS Public Search Tool.

Handles:
- fwuid extraction from page HTML
- Aura POST payload construction for any action
- Pagination (index 0→20, 100 rows/page)
- 2,100 result limit detection
- Character-based search subdivision (a→z, 0→9, then aa, ab...)
- Retry with exponential backoff
- Polite rate limiting (~1 req/sec)
- State tracking for resume capability
"""

import hashlib
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "raw"
STATE_DIR = BASE_DIR / "logs"

# Both domains point to the same Salesforce instance
CANDIDATE_BASES = [
    "https://efile.aphis.usda.gov/PublicSearchTool",
    "https://aphis.my.site.com/PublicSearchTool",
]

ROWS_PER_PAGE = 100
MAX_PAGES = 21  # 21 pages × 100 = 2,100 max results per query
MAX_RESULTS = MAX_PAGES * ROWS_PER_PAGE

# Characters used for search subdivision
SEARCH_CHARS = list("abcdefghijklmnopqrstuvwxyz0123456789")

# Rate limiting
MIN_REQUEST_INTERVAL = 1.0  # seconds between requests

# Retry config
MAX_RETRIES = 10
RETRY_DELAY = 30  # seconds


class TooManyResultsError(Exception):
    """Raised when a query returns more than 2,100 results."""
    def __init__(self, prefix: str, total_count: int):
        self.prefix = prefix
        self.total_count = total_count
        super().__init__(f"Query prefix '{prefix}' returned {total_count} results (max {MAX_RESULTS})")


class BadResponseError(Exception):
    """Raised when the API returns an unparseable or error response."""
    pass


class AuraClient:
    """Client for the APHIS Public Search Tool's Salesforce Aura API."""

    def __init__(self, rate_limit: float = MIN_REQUEST_INTERVAL):
        self.base_url: Optional[str] = None
        self.fwuid: Optional[str] = None
        self.session = self._build_session()
        self.rate_limit = rate_limit
        self._last_request_time = 0.0

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "openregs-aphis-extractor/1.0 (public interest research)",
            "Accept": "application/json",
        })
        return session

    def _throttle(self):
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def detect_base_url(self) -> str:
        """Try candidate base URLs and return the first one that responds."""
        if self.base_url:
            return self.base_url

        for base in CANDIDATE_BASES:
            try:
                resp = self.session.get(f"{base}/s/", timeout=15, allow_redirects=True)
                if resp.status_code == 200:
                    self.base_url = base
                    logger.info(f"Using base URL: {base}")
                    return base
            except requests.RequestException as e:
                logger.debug(f"Base URL {base} failed: {e}")
                continue

        raise ConnectionError(f"None of the candidate URLs responded: {CANDIDATE_BASES}")

    def fetch_fwuid(self, max_attempts: int = 3) -> str:
        """Extract the Salesforce framework UID from the page HTML."""
        if self.fwuid:
            return self.fwuid

        base = self.detect_base_url()
        url = f"{base}/s/"

        for attempt in range(max_attempts):
            try:
                self._throttle()
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()

                # fwuid appears URL-encoded in the HTML
                # Pattern: %22fwuid%22%3A%22<value>%22
                match = re.search(r'%22fwuid%22%3A%22([^%]+)%22', resp.text)
                if match:
                    self.fwuid = match.group(1)
                    logger.info(f"Extracted fwuid: {self.fwuid}")
                    return self.fwuid

                # Also try unencoded JSON pattern
                match = re.search(r'"fwuid"\s*:\s*"([^"]+)"', resp.text)
                if match:
                    self.fwuid = match.group(1)
                    logger.info(f"Extracted fwuid (unencoded): {self.fwuid}")
                    return self.fwuid

                logger.warning(f"fwuid not found in page HTML (attempt {attempt + 1}/{max_attempts})")

            except requests.RequestException as e:
                logger.warning(f"Failed to fetch page for fwuid (attempt {attempt + 1}): {e}")

            if attempt < max_attempts - 1:
                time.sleep(RETRY_DELAY)

        raise BadResponseError("Could not extract fwuid after all attempts")

    def build_aura_context(self) -> str:
        """Build the aura.context JSON string."""
        if not self.fwuid:
            self.fetch_fwuid()

        return json.dumps({
            "mode": "PROD",
            "fwuid": self.fwuid,
            "app": "siteforce:communityApp",
            "loaded": {},
            "dn": [],
            "globals": {},
            "uad": False,
        }, separators=(",", ":"))

    def build_message(self, action_name: str, params: dict) -> str:
        """Build the Aura message JSON for a given action."""
        return json.dumps({
            "actions": [{
                "id": "1",
                "descriptor": f"apex://EFL_PSTController/ACTION${action_name}",
                "callingDescriptor": "UNKNOWN",
                "params": params,
            }]
        }, separators=(",", ":"))

    def call_action(self, action_name: str, params: dict) -> dict:
        """
        Make a single Aura API call and return the parsed response.

        Returns the returnValue from the first action in the response.
        """
        base = self.detect_base_url()
        endpoint = f"{base}/s/sfsites/aura"

        message = self.build_message(action_name, params)
        context = self.build_aura_context()

        payload = urlencode({
            "message": message,
            "aura.context": context,
            "aura.token": "null",
        })

        for attempt in range(MAX_RETRIES):
            try:
                self._throttle()
                resp = self.session.post(
                    endpoint,
                    data=payload,
                    headers={"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"},
                    timeout=60,
                )

                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} on attempt {attempt + 1}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                    continue

                data = resp.json()

                # Check for Aura framework errors
                if "exceptionEvent" in data:
                    error_msg = data.get("exceptionEvent", {}).get("descriptor", "unknown error")
                    # Framework update — refetch fwuid
                    if "clientOutOfSync" in str(data):
                        logger.warning("Framework out of sync, refetching fwuid...")
                        self.fwuid = None
                        self.fetch_fwuid()
                        continue
                    raise BadResponseError(f"Aura exception: {error_msg}")

                actions = data.get("actions", [])
                if not actions:
                    raise BadResponseError("No actions in response")

                action_resp = actions[0]
                state = action_resp.get("state")
                if state == "ERROR":
                    errors = action_resp.get("error", [])
                    error_msg = errors[0].get("message", "unknown") if errors else "unknown"
                    raise BadResponseError(f"Action error: {error_msg}")

                if state != "SUCCESS":
                    raise BadResponseError(f"Unexpected action state: {state}")

                return action_resp.get("returnValue", {})

            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    raise BadResponseError(f"All {MAX_RETRIES} attempts failed: {e}") from e

        raise BadResponseError("Exhausted all retry attempts")

    def search(self, action_name: str, search_criteria: dict, get_count: bool = True) -> dict:
        """
        Perform a search with the given criteria.

        Returns dict with 'totalCount' and 'results' keys.
        """
        params = {
            "searchCriteria": search_criteria,
            "getCount": get_count,
        }
        return self.call_action(action_name, params)

    def fetch_all_pages(self, action_name: str, base_criteria: dict) -> tuple[int, list[dict]]:
        """
        Fetch all pages for a given search query.

        Returns (total_count, all_results).
        Raises TooManyResultsError if totalCount > MAX_RESULTS.
        """
        all_results = []
        total_count = 0

        for page_index in range(MAX_PAGES + 1):
            criteria = {**base_criteria, "index": page_index, "numberOfRows": ROWS_PER_PAGE}
            get_count = (page_index == 0)

            result = self.search(action_name, criteria, get_count=get_count)

            if page_index == 0:
                total_count = result.get("totalCount", 0)
                if total_count > MAX_RESULTS:
                    raise TooManyResultsError(
                        base_criteria.get("customerName", ""),
                        total_count,
                    )

            page_results = result.get("results") or []
            if not page_results:
                break

            all_results.extend(page_results)
            logger.debug(f"  Page {page_index}: {len(page_results)} results (total so far: {len(all_results)})")

            if len(page_results) < ROWS_PER_PAGE:
                break

        return total_count, all_results

    def fetch_by_prefix(
        self,
        action_name: str,
        base_criteria: dict,
        prefix: str = "",
        search_field: str = "customerName",
        max_depth: int = 4,
    ) -> list[dict]:
        """
        Recursively fetch results using character-based subdivision.

        Starts with the given prefix. If a query returns >2,100 results,
        subdivides into longer prefixes (e.g., "a" → "aa", "ab", ...).
        """
        criteria = {**base_criteria, search_field: prefix}

        try:
            total_count, results = self.fetch_all_pages(action_name, criteria)
            logger.info(f"Prefix '{prefix}': {total_count} total, {len(results)} fetched")
            return results

        except TooManyResultsError as e:
            if len(prefix) >= max_depth:
                logger.error(f"Prefix '{prefix}' still has {e.total_count} results at max depth {max_depth}")
                # Fetch what we can
                criteria = {**base_criteria, search_field: prefix}
                _, results = self.fetch_all_pages.__wrapped__(self, action_name, criteria) if hasattr(self.fetch_all_pages, '__wrapped__') else (0, [])
                return results

            logger.info(f"Prefix '{prefix}' has {e.total_count} results, subdividing...")
            all_results = []
            for char in SEARCH_CHARS:
                sub_prefix = prefix + char
                sub_results = self.fetch_by_prefix(
                    action_name, base_criteria, sub_prefix, search_field, max_depth
                )
                all_results.extend(sub_results)

            return all_results

    def fetch_all_letters(
        self,
        action_name: str,
        base_criteria: Optional[dict] = None,
        search_field: str = "customerName",
    ) -> list[dict]:
        """
        Fetch all results by iterating through single-character prefixes.

        This is the main entry point for full extraction of a category.
        """
        if base_criteria is None:
            base_criteria = {}

        all_results = []
        for char in SEARCH_CHARS:
            logger.info(f"Searching prefix '{char}'...")
            results = self.fetch_by_prefix(action_name, base_criteria, char, search_field)
            all_results.extend(results)
            logger.info(f"  Got {len(results)} results for '{char}' (total: {len(all_results)})")

        # Also try empty prefix for records that might not match any letter
        try:
            criteria = {**base_criteria, search_field: ""}
            total_count, empty_results = self.fetch_all_pages(action_name, criteria)
            if empty_results:
                logger.info(f"Empty prefix returned {len(empty_results)} results")
                all_results.extend(empty_results)
        except TooManyResultsError:
            pass  # Expected — empty prefix matches everything

        return all_results


def deduplicate(results: list[dict], key_fields: Optional[list[str]] = None) -> list[dict]:
    """
    Remove duplicate records based on key fields.

    If no key_fields specified, uses all fields for comparison.
    """
    if not results:
        return []

    seen = set()
    unique = []

    for record in results:
        if key_fields:
            key = tuple(record.get(f) for f in key_fields)
        else:
            key = tuple(sorted(record.items()))

        if key not in seen:
            seen.add(key)
            unique.append(record)

    removed = len(results) - len(unique)
    if removed:
        logger.info(f"Deduplicated: {len(results)} → {len(unique)} ({removed} duplicates removed)")

    return unique


def hash_id_from_url(url: str) -> str:
    """Generate a truncated SHA-1 hash from a URL's record ID."""
    # Extract the ID parameter from the URL
    match = re.search(r'[?&]id=([^&]+)', url) or re.search(r'/([a-zA-Z0-9]{15,18})(?:\?|$)', url)
    if match:
        record_id = match.group(1)
    else:
        record_id = url

    return hashlib.sha1(record_id.encode()).hexdigest()[:12]


def add_hash_ids(results: list[dict], url_field: str = "reportUrl") -> list[dict]:
    """Add hash_id field to each result based on a URL field."""
    for record in results:
        url = record.get(url_field, "")
        if url:
            record["hash_id"] = hash_id_from_url(url)
        else:
            # Fallback: hash from all fields
            record["hash_id"] = hashlib.sha1(
                json.dumps(record, sort_keys=True).encode()
            ).hexdigest()[:12]
    return results


class StateTracker:
    """Tracks extraction progress for resume capability."""

    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state = self._load()

    def _load(self) -> dict:
        if self.state_file.exists():
            with open(self.state_file) as f:
                return json.load(f)
        return {"completed": {}, "counts": {}}

    def save(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(self.state, f, indent=2)

    def is_completed(self, category: str, prefix: str = "__all__") -> bool:
        return prefix in self.state.get("completed", {}).get(category, [])

    def mark_completed(self, category: str, prefix: str = "__all__", count: int = 0):
        if category not in self.state["completed"]:
            self.state["completed"][category] = []
        if prefix not in self.state["completed"][category]:
            self.state["completed"][category].append(prefix)
        if count:
            if category not in self.state["counts"]:
                self.state["counts"][category] = {}
            self.state["counts"][category][prefix] = count
        self.save()

    def get_count(self, category: str) -> int:
        return sum(self.state.get("counts", {}).get(category, {}).values())
