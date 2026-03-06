#!/usr/bin/env python3
"""
Step 1: Discover which Aura action names work for each record type.

Probes the APHIS Public Search Tool API with candidate action names
and saves discovered actions + sample response schemas to raw/api_discovery.json.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.aura_client import AuraClient, BadResponseError, RAW_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Candidate action names to probe per category
CANDIDATES = {
    "inspections": [
        "doIRSearch_UI",
    ],
    "enforcement": [
        "doEASearch_UI",
        "doEnforcementSearch_UI",
        "doEASearch",
    ],
    "teachable_moments": [
        "doTMSearch_UI",
        "doTeachableMomentSearch_UI",
        "doTMSearch",
    ],
    "annual_reports": [
        "doARSearch_UI",
        "doAnnualReportSearch_UI",
        "doARSearch",
        "doRASearch_UI",
    ],
    "licensees": [
        "doCustomerSearch_UI",
        "doCSSearch_UI",
        "doLicenseeSearch_UI",
        "doLRSearch_UI",
    ],
}

# Minimal test criteria — search for "a" to get a small result set
TEST_CRITERIA = {
    "index": 0,
    "numberOfRows": 5,
    "customerName": "a",
}


def probe_action(client: AuraClient, action_name: str) -> dict:
    """
    Try calling an action and return the result info.

    Returns dict with keys: works, error, totalCount, sample_fields, sample_record
    """
    result = {
        "action_name": action_name,
        "works": False,
        "error": None,
        "totalCount": None,
        "sample_fields": [],
        "sample_record": None,
    }

    try:
        resp = client.search(action_name, TEST_CRITERIA, get_count=True)

        total = resp.get("totalCount", 0)
        records = resp.get("results") or []

        result["works"] = True
        result["totalCount"] = total

        if records:
            sample = records[0]
            result["sample_fields"] = sorted(sample.keys())
            result["sample_record"] = sample

        logger.info(f"  OK: {action_name} → {total} total results, {len(records)} returned, fields: {result['sample_fields']}")

    except BadResponseError as e:
        result["error"] = str(e)
        logger.info(f"  FAIL: {action_name} → {e}")

    except Exception as e:
        result["error"] = str(e)
        logger.info(f"  ERROR: {action_name} → {e}")

    return result


def main():
    client = AuraClient(rate_limit=1.5)

    logger.info("Initializing connection and fetching fwuid...")
    try:
        client.fetch_fwuid()
    except Exception as e:
        logger.error(f"Failed to initialize: {e}")
        sys.exit(1)

    logger.info(f"Connected to {client.base_url} with fwuid={client.fwuid}")

    discovery = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "base_url": client.base_url,
        "fwuid": client.fwuid,
        "categories": {},
    }

    for category, action_names in CANDIDATES.items():
        logger.info(f"\n--- Probing category: {category} ---")
        category_results = []

        for action_name in action_names:
            result = probe_action(client, action_name)
            category_results.append(result)

            # If we found a working action, no need to try more for this category
            if result["works"]:
                break

        discovery["categories"][category] = {
            "probes": category_results,
            "confirmed_action": next(
                (r["action_name"] for r in category_results if r["works"]),
                None,
            ),
            "total_count_estimate": next(
                (r["totalCount"] for r in category_results if r["works"]),
                None,
            ),
        }

    # Summary
    logger.info("\n=== Discovery Summary ===")
    for category, info in discovery["categories"].items():
        action = info["confirmed_action"]
        count = info["total_count_estimate"]
        if action:
            logger.info(f"  {category}: {action} (est. {count:,} records)")
        else:
            logger.warning(f"  {category}: NO WORKING ACTION FOUND")

    # Save results
    output_path = RAW_DIR / "api_discovery.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(discovery, f, indent=2, default=str)
    logger.info(f"\nDiscovery results saved to {output_path}")

    # Exit with error if any category has no working action
    failed = [c for c, i in discovery["categories"].items() if not i["confirmed_action"]]
    if failed:
        logger.warning(f"Categories with no working action: {failed}")
        logger.info("You may need to inspect the tool's JavaScript to find the correct action names.")
        # Don't exit with error — partial discovery is still useful


if __name__ == "__main__":
    main()
