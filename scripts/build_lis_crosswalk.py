#!/usr/bin/env python3
"""
Build a crosswalk mapping Senate LIS IDs to bioguide IDs.

Downloads legislators-current.json and legislators-historical.json from the
unitedstates/congress-legislators repo, extracts bioguide_id and lis_id for
every legislator who has both, and saves the mapping as lis_crosswalk.json.
"""

import json
import os
import urllib.request
from pathlib import Path

URLS = [
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-current.json",
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-historical.json",
]

# Also check local copies if available
PROJECT_DIR = Path(__file__).resolve().parent.parent
LOCAL_FILES = [
    str(PROJECT_DIR / "congress_members" / "legislators-current.json"),
    str(PROJECT_DIR / "congress_members" / "legislators-historical.json"),
]

OUTPUT_PATH = str(PROJECT_DIR / "congress_members" / "lis_crosswalk.json")


def load_legislators():
    """Load from local files if available, otherwise download."""
    all_legislators = []
    for local_path, url in zip(LOCAL_FILES, URLS):
        if os.path.exists(local_path):
            print(f"Loading from local file: {local_path}")
            with open(local_path) as f:
                data = json.load(f)
        else:
            print(f"Downloading: {url}")
            with urllib.request.urlopen(url) as resp:
                data = json.loads(resp.read().decode())
        all_legislators.extend(data)
        print(f"  -> {len(data)} legislators loaded")
    return all_legislators


def build_crosswalk(legislators):
    """Extract lis_id -> bioguide_id mapping for all legislators with both."""
    # lis_id -> { bioguide_id, name }
    crosswalk = {}
    missing_lis = 0
    missing_bioguide = 0

    for leg in legislators:
        ids = leg.get("id", {})
        bioguide = ids.get("bioguide")
        lis = ids.get("lis")
        name = leg.get("name", {})
        full_name = name.get("official_full") or f"{name.get('first', '')} {name.get('last', '')}"

        if not bioguide:
            missing_bioguide += 1
            continue
        if not lis:
            missing_lis += 1
            continue

        crosswalk[lis] = {
            "bioguide_id": bioguide,
            "name": full_name.strip(),
        }

    return crosswalk, missing_lis, missing_bioguide


def main():
    legislators = load_legislators()
    print(f"\nTotal legislators loaded: {len(legislators)}")

    crosswalk, missing_lis, missing_bioguide = build_crosswalk(legislators)

    print(f"\n=== Results ===")
    print(f"Legislators with both bioguide + LIS IDs: {len(crosswalk)}")
    print(f"Legislators missing LIS ID (expected for House members): {missing_lis}")
    print(f"Legislators missing bioguide ID: {missing_bioguide}")

    # Show LIS ID format analysis
    lis_ids = sorted(crosswalk.keys())
    print(f"\n=== LIS ID Format ===")
    print(f"First 10 (sorted): {lis_ids[:10]}")
    print(f"Last 10 (sorted):  {lis_ids[-10:]}")

    # Check format pattern
    all_s_prefix = all(lid.startswith("S") for lid in lis_ids)
    print(f"All start with 'S': {all_s_prefix}")
    numeric_parts = [lid[1:] for lid in lis_ids if lid.startswith("S")]
    lengths = set(len(n) for n in numeric_parts)
    print(f"Numeric part lengths: {lengths}")

    # Show some examples
    print(f"\n=== Examples ===")
    examples = ["S238", "S127", "S313", "S275", "S010"]
    for lis_id in examples:
        if lis_id in crosswalk:
            entry = crosswalk[lis_id]
            print(f"  {lis_id} -> {entry['bioguide_id']} ({entry['name']})")

    # Save the crosswalk - simple lis_id -> bioguide_id mapping
    output = {}
    for lis_id in sorted(crosswalk.keys()):
        output[lis_id] = crosswalk[lis_id]["bioguide_id"]

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved {len(output)} mappings to {OUTPUT_PATH}")

    # Also save a detailed version with names for debugging
    detailed_path = OUTPUT_PATH.replace(".json", "_detailed.json")
    with open(detailed_path, "w") as f:
        json.dump(
            {lis: crosswalk[lis] for lis in sorted(crosswalk.keys())},
            f,
            indent=2,
        )
    print(f"Saved detailed version to {detailed_path}")


if __name__ == "__main__":
    main()
