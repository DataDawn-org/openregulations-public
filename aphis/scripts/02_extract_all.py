#!/usr/bin/env python3
"""
Step 2: Main bulk extraction — all categories, letter-by-letter.

For each confirmed record type:
1. Search letter-by-letter (a-z, 0-9) on customer name
2. If any letter returns >2,100, subdivide into two-char prefixes
3. Continue subdividing recursively until all results fit
4. Deduplicate across letter batches
5. Save raw results to raw/{category}/ as JSON

Uses multiprocessing (6 workers) for letter searches within each category.
Categories run sequentially to be polite to the API.
Supports resume via state file.
"""

import json
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.aura_client import (
    AuraClient,
    BadResponseError,
    RAW_DIR,
    STATE_DIR,
    SEARCH_CHARS,
    StateTracker,
    TooManyResultsError,
    deduplicate,
    add_hash_ids,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(processName)s] %(message)s",
)
logger = logging.getLogger(__name__)

MAX_WORKERS = 6

# Default action names — updated by discovery results if available
DEFAULT_ACTIONS = {
    "inspections": "doIRSearch_UI",
    "enforcement": "doEASearch_UI",
    "teachable_moments": "doTMSearch_UI",
    "annual_reports": "doARSearch_UI",
    "licensees": "doCustomerSearch_UI",
}

# Category-specific deduplication keys
DEDUP_KEYS = {
    "inspections": None,  # Will use hash_id if available, else all fields
    "enforcement": None,
    "teachable_moments": None,
    "annual_reports": None,
    "licensees": None,
}


def load_discovery() -> dict[str, str]:
    """Load confirmed action names from discovery results."""
    discovery_path = RAW_DIR / "api_discovery.json"
    if not discovery_path.exists():
        logger.warning("No discovery file found, using default action names")
        return DEFAULT_ACTIONS

    with open(discovery_path) as f:
        discovery = json.load(f)

    actions = {}
    for category, info in discovery.get("categories", {}).items():
        confirmed = info.get("confirmed_action")
        if confirmed:
            actions[category] = confirmed
        elif category in DEFAULT_ACTIONS:
            logger.warning(f"No confirmed action for {category}, using default: {DEFAULT_ACTIONS[category]}")
            actions[category] = DEFAULT_ACTIONS[category]

    return actions


def fetch_prefix_worker(args: tuple) -> tuple[str, str, list[dict]]:
    """
    Worker function for multiprocessing.

    Args: (category, prefix, action_name, base_criteria)
    Returns: (category, prefix, results)
    """
    category, prefix, action_name, base_criteria = args

    client = AuraClient(rate_limit=1.0)
    try:
        client.fetch_fwuid()
    except Exception as e:
        logger.error(f"Worker failed to get fwuid: {e}")
        return category, prefix, []

    try:
        results = client.fetch_by_prefix(action_name, base_criteria or {}, prefix)
        logger.info(f"[{category}] Prefix '{prefix}': {len(results)} results")
        return category, prefix, results
    except Exception as e:
        logger.error(f"[{category}] Prefix '{prefix}' failed: {e}")
        return category, prefix, []


def extract_category(
    category: str,
    action_name: str,
    state: StateTracker,
    parallel: bool = True,
) -> list[dict]:
    """Extract all results for a single category."""

    if state.is_completed(category):
        logger.info(f"Category '{category}' already completed, skipping")
        return []

    logger.info(f"\n{'='*60}")
    logger.info(f"Extracting category: {category} (action: {action_name})")
    logger.info(f"{'='*60}")

    output_dir = RAW_DIR / category
    output_dir.mkdir(parents=True, exist_ok=True)

    all_results = []
    pending_prefixes = []

    for char in SEARCH_CHARS:
        if state.is_completed(category, char):
            # Load previously saved results
            prefix_file = output_dir / f"prefix_{char}.json"
            if prefix_file.exists():
                with open(prefix_file) as f:
                    saved = json.load(f)
                all_results.extend(saved)
                logger.info(f"  Loaded {len(saved)} cached results for prefix '{char}'")
            continue
        pending_prefixes.append(char)

    if not pending_prefixes:
        logger.info(f"All prefixes for '{category}' already completed")
        # Load all and deduplicate
        all_results = deduplicate(all_results)
        return all_results

    logger.info(f"Pending prefixes: {len(pending_prefixes)} of {len(SEARCH_CHARS)}")

    if parallel and len(pending_prefixes) > 1:
        # Parallel extraction
        work_items = [
            (category, prefix, action_name, {})
            for prefix in pending_prefixes
        ]

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_prefix_worker, item): item[1]
                for item in work_items
            }

            for future in as_completed(futures):
                prefix = futures[future]
                try:
                    _, _, results = future.result()

                    # Save prefix results
                    prefix_file = output_dir / f"prefix_{prefix}.json"
                    with open(prefix_file, "w") as f:
                        json.dump(results, f)

                    state.mark_completed(category, prefix, len(results))
                    all_results.extend(results)

                except Exception as e:
                    logger.error(f"[{category}] Prefix '{prefix}' worker failed: {e}")
    else:
        # Sequential extraction
        client = AuraClient(rate_limit=1.0)
        client.fetch_fwuid()

        for prefix in pending_prefixes:
            try:
                results = client.fetch_by_prefix(action_name, {}, prefix)

                # Save prefix results
                prefix_file = output_dir / f"prefix_{prefix}.json"
                with open(prefix_file, "w") as f:
                    json.dump(results, f)

                state.mark_completed(category, prefix, len(results))
                all_results.extend(results)

                logger.info(f"  Prefix '{prefix}': {len(results)} results (total: {len(all_results)})")

            except Exception as e:
                logger.error(f"  Prefix '{prefix}' failed: {e}")

    # Deduplicate
    pre_dedup = len(all_results)
    all_results = deduplicate(all_results)
    logger.info(f"Category '{category}': {pre_dedup} raw → {len(all_results)} deduplicated")

    # Save combined results
    combined_file = output_dir / "all_results.json"
    with open(combined_file, "w") as f:
        json.dump({
            "category": category,
            "action_name": action_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_records": len(all_results),
            "results": all_results,
        }, f, indent=2, default=str)

    state.mark_completed(category, "__all__", len(all_results))
    logger.info(f"Saved {len(all_results)} results to {combined_file}")

    return all_results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract APHIS Public Search Tool data")
    parser.add_argument("--categories", nargs="+", help="Specific categories to extract")
    parser.add_argument("--sequential", action="store_true", help="Disable parallel extraction")
    parser.add_argument("--reset", action="store_true", help="Reset state and re-extract everything")
    args = parser.parse_args()

    state_file = STATE_DIR / "extraction_state.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)

    if args.reset and state_file.exists():
        state_file.unlink()
        logger.info("State file reset")

    state = StateTracker(state_file)
    actions = load_discovery()

    categories_to_extract = args.categories or list(actions.keys())

    logger.info(f"Categories to extract: {categories_to_extract}")
    logger.info(f"Actions: {json.dumps({c: actions.get(c, 'UNKNOWN') for c in categories_to_extract}, indent=2)}")

    summary = {}
    for category in categories_to_extract:
        if category not in actions:
            logger.warning(f"No action name for category '{category}', skipping")
            continue

        try:
            results = extract_category(
                category,
                actions[category],
                state,
                parallel=not args.sequential,
            )
            summary[category] = len(results)
        except Exception as e:
            logger.error(f"Category '{category}' failed: {e}")
            summary[category] = f"ERROR: {e}"

    logger.info(f"\n{'='*60}")
    logger.info("Extraction Summary")
    logger.info(f"{'='*60}")
    for category, count in summary.items():
        logger.info(f"  {category}: {count}")


if __name__ == "__main__":
    main()
