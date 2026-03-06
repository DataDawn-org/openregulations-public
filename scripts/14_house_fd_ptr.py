#!/usr/bin/env python3
"""
Script 14: Download and parse House Financial Disclosure PTR PDFs

Downloads Periodic Transaction Report (PTR) PDFs from
disclosures-clerk.house.gov and extracts transaction-level data
using pdftotext.

Each PTR contains individual stock/bond transactions with:
- Asset name (with optional ticker symbol)
- Transaction type (Purchase/Sale/Exchange)
- Transaction date
- Dollar amount range
- Owner (Self/Spouse/Joint/Dependent Child)

Text-based PDFs (~70% of filings, increasing to ~95% for recent years)
are parsed directly. Scanned/image PDFs are skipped.

Rate-limited to ~1 request/second to be respectful of House servers.
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("house_fd_ptr")

# === Paths ===
BASE_DIR = Path(__file__).resolve().parent.parent
HOUSE_DIR = BASE_DIR / "stock_trades" / "house"
PDF_DIR = BASE_DIR / "stock_trades" / "house_ptrs"
STATE_FILE = PDF_DIR / "state.json"
OUTPUT_FILE = PDF_DIR / "all_transactions.json"

# === Configuration ===
RATE_LIMIT = 1.0  # seconds between downloads
CHECKPOINT_INTERVAL = 50

# PDF URL patterns
PTR_PDF_URL = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"

# Owner codes
OWNER_CODES = {"SP": "Spouse", "JT": "Joint", "DC": "Dependent Child"}

# Amount range normalization
AMOUNT_RANGES = [
    "$1 - $1,000",
    "$1,001 - $15,000",
    "$15,001 - $50,000",
    "$50,001 - $100,000",
    "$100,001 - $250,000",
    "$250,001 - $500,000",
    "$500,001 - $1,000,000",
    "$1,000,001 - $5,000,000",
    "$5,000,001 - $25,000,000",
    "$25,000,001 - $50,000,000",
    "$50,000,001+",
]


def load_state():
    """Load download/parse state."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"completed": [], "failed": [], "scanned": []}


def save_state(state):
    """Save state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def get_all_ptrs():
    """Load all PTR filing records from the House FD index files."""
    ptrs = []
    for fd_file in sorted(HOUSE_DIR.glob("*.json")):
        try:
            with open(fd_file) as f:
                filings = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        year = fd_file.stem.replace("FD", "")
        for filing in filings:
            if filing.get("filing_type") != "P":
                continue
            doc_id = str(filing.get("doc_id", ""))
            if not doc_id:
                continue
            ptrs.append({
                "doc_id": doc_id,
                "year": int(year) if year.isdigit() else 0,
                "first_name": filing.get("first_name", ""),
                "last_name": filing.get("last_name", ""),
                "member_name": filing.get("member_name", ""),
                "state_district": filing.get("state_district", ""),
                "filing_date": filing.get("filing_date", ""),
            })
    return ptrs


def download_pdf(doc_id, year):
    """Download a PTR PDF. Returns path or None."""
    pdf_path = PDF_DIR / f"{doc_id}.pdf"
    if pdf_path.exists() and pdf_path.stat().st_size > 2000:
        return pdf_path

    url = PTR_PDF_URL.format(year=year, doc_id=doc_id)
    try:
        urllib.request.urlretrieve(url, str(pdf_path))
        if pdf_path.stat().st_size < 2000:
            pdf_path.unlink(missing_ok=True)
            return None
        return pdf_path
    except Exception:
        return None


def is_scanned_pdf(pdf_path):
    """Check if a PDF is scanned (image-only, no extractable text)."""
    result = subprocess.run(
        ["pdftotext", str(pdf_path), "-"],
        capture_output=True, text=True, timeout=30,
    )
    # Scanned PDFs produce empty or near-empty text
    return len(result.stdout.strip()) < 50


def extract_text(pdf_path):
    """Extract text from PDF using pdftotext -layout."""
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True, text=True, timeout=30,
    )
    return result.stdout


def normalize_amount(raw):
    """Normalize dollar amount to standard range format."""
    raw = raw.strip()
    if not raw:
        return ""
    # Remove extra spaces
    raw = re.sub(r"\s+", " ", raw)
    # Handle "Over $X" format
    if raw.lower().startswith("over "):
        return raw
    # Extract the two dollar values
    vals = re.findall(r"\$[\d,]+", raw)
    if not vals:
        return raw
    if len(vals) == 1 and raw.endswith("+"):
        return vals[0] + "+"
    if len(vals) >= 2:
        return f"{vals[0]} - {vals[1]}"
    # Single value — try to find matching standard range by lower bound
    lower = vals[0]
    for std in AMOUNT_RANGES:
        if std.startswith(lower + " "):
            return std
    return raw


def _clean_text(s):
    """Remove null bytes and other PDF artifacts from text."""
    # Replace null bytes and control chars (except newline/tab)
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


def _is_metadata_line(s):
    """Check if a line is filing metadata (Filing Status, Subholding, Description)."""
    cleaned = _clean_text(s)
    # Filing Status: New / Amended
    if re.search(r"(?:FILING|Filing|FILINg|FIlINg)\s*(?:STATUS|STATuS|Status|sTaTus)", cleaned, re.IGNORECASE):
        return True
    # Subholding Of: ...
    if re.search(r"(?:SUBHOLDING|SUbHoLDINg|Subholding)\s*(?:OF|oF|Of)", cleaned, re.IGNORECASE):
        return True
    # Description: ... (CUSIP)
    if re.search(r"(?:DESCRIPTION|Description|D\w+tion)\s*:", cleaned, re.IGNORECASE):
        return True
    # Also match the garbled null-byte versions: "F S : New", "S O : ..."
    # These have null bytes between letters in the original
    if re.match(r"^\s*F\s+S\s+:\s*\w", cleaned):
        return True
    if re.match(r"^\s*S\s+O\s*:\s", cleaned):
        return True
    if re.match(r"^\s*D\s+:\s", cleaned):
        return True
    return False


def _extract_subholding(s):
    """Extract subholding value from a metadata line."""
    cleaned = _clean_text(s)
    m = re.search(r"(?:SUBHOLDING|SUbHoLDINg|Subholding)\s*(?:OF|oF|Of)\s*:\s*(.+)", cleaned, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"S\s+O\s*:\s*(.+)", cleaned)
    if m:
        return m.group(1).strip()
    return ""


def _extract_description(s):
    """Extract description/CUSIP from a metadata line."""
    cleaned = _clean_text(s)
    m = re.search(r"(?:DESCRIPTION|Description|D\w+tion)\s*:\s*(.+)", cleaned, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"D\s+:\s*(.+)", cleaned)
    if m:
        return m.group(1).strip()
    return ""


def _find_tx_type(line, dates, tx_types):
    """Find transaction type before the given date pair. Returns (tx_type, position)."""
    first_date_pos = line.find(dates[0])
    pre_date = line[:first_date_pos]

    for tt in tx_types:
        pattern = re.escape(tt)
        m = re.search(r"(?:^|\s)" + pattern + r"\s*$", pre_date)
        if m:
            return tt, m.start()

    # Try case-insensitive for older PDFs with rendering quirks
    for tt in tx_types:
        pattern = re.escape(tt)
        m = re.search(r"(?:^|\s)" + pattern + r"\s*$", pre_date, re.IGNORECASE)
        if m:
            return tt[0].upper() + tt[1:], m.start()

    return "", -1


def parse_transactions(text):
    """Parse pdftotext -layout output to extract transactions.

    Strategy: Each transaction starts on a line containing a transaction type
    (P, S, E, or their partial variants) followed by two dates (MM/DD/YYYY).
    The asset description is to the left of the transaction type.
    The amount is to the right of the dates, possibly continuing on the next line.
    """
    if not text or len(text.strip()) < 50:
        return None  # Scanned PDF

    # Clean null bytes throughout
    text = _clean_text(text)
    lines = text.split("\n")
    transactions = []

    # Extract filer info
    filer_name = ""
    state_district = ""
    for line in lines[:25]:
        m = re.search(r"[Nn]ame:\s+(?:Hon\.\s+)?(.+?)$", line)
        if m:
            filer_name = m.group(1).strip()
        m = re.search(r"State/District:\s*(\S+)", line, re.IGNORECASE)
        if m:
            state_district = m.group(1).strip()

    date_re = r"(\d{2}/\d{2}/\d{4})"
    # Transaction type patterns — check longer patterns first
    tx_types = ["S (partial)", "E (partial)", "P (partial)", "P", "S", "E"]

    i = 0
    while i < len(lines):
        line = lines[i]

        # Skip header/footer/boilerplate lines
        if re.match(
            r"\s*(ID\s+Owner|Filing ID|Clerk of|"
            r"F\s+I$|[Nn]ame:|[Ss]tatus:|State/|"
            r"[PT]$|"
            r"\* For the|A\s+C\s+D|I\s+P\s+O|"
            r"[CI]\s+[SV]\s*$|I CERTIFY|Digitally Signed|"
            r"I\s+V\s+D|"
            r"[Cc]ertification|[Ii]nitial [Pp]ublic|[Aa]sset [Cc]lass|"
            r"Yes\s+No|"
            r"P\s+T\s+R|"  # Header "Periodic Transaction Report"
            r"P\s+erioDic)",
            line, re.IGNORECASE
        ):
            i += 1
            continue

        # Skip metadata lines that appear between transactions
        if _is_metadata_line(line):
            i += 1
            continue

        # Find lines with two dates — these are transaction lines
        all_dates = re.findall(date_re, line)
        if len(all_dates) < 2:
            i += 1
            continue

        # Try first-two dates (works for 99% of cases), then fall back
        # to last-two dates if tx type not found (handles dates in asset names,
        # e.g., PUT options with expiration dates).
        dates = all_dates[:2]
        tx_type, tx_type_pos = _find_tx_type(line, dates, tx_types)

        if not tx_type and len(all_dates) > 2:
            # Fallback: try last two dates (asset description contains a date)
            dates = all_dates[-2:]
            tx_type, tx_type_pos = _find_tx_type(line, dates, tx_types)

        # Asset description is everything before the transaction type
        if tx_type_pos > 0:
            asset_line = line[:tx_type_pos].strip()
        else:
            first_date_pos = line.find(dates[0])
            asset_line = line[:first_date_pos].strip()

        # Extract owner code from the beginning of asset_line
        owner = "Self"
        owner_m = re.match(r"^(SP|JT|DC)\s+", asset_line)
        if owner_m:
            owner = OWNER_CODES.get(owner_m.group(1), owner_m.group(1))
            asset_line = asset_line[owner_m.end():].strip()

        # Amount — everything after the second (notification) date on this line
        second_date_pos = line.find(dates[1], line.find(dates[0]) + len(dates[0]))
        if second_date_pos < 0:
            second_date_pos = line.rfind(dates[1])
        after_dates = line[second_date_pos + len(dates[1]):]
        # Strip checkbox artifacts (c, d, e, f, g) from end of line
        after_dates_clean = re.sub(r"\s+[cdefg]\s*$", "", after_dates)
        # Check for "Over $X" format (e.g., "Spouse/DC Over $1,000,000")
        over_match = re.search(r"[Oo]ver\s+(\$[\d,]+)", after_dates_clean)
        # "Over" may appear without the dollar amount (split to next line)
        amount_is_over = bool(re.search(r"[Oo]ver\s*$", after_dates_clean.strip()))
        if over_match:
            amount = "Over " + over_match.group(1)
        else:
            amount_match = re.search(r"(\$[\d,]+(?:\s*-\s*\$[\d,]+|\+)?)", after_dates_clean)
            amount = amount_match.group(1).strip() if amount_match else ""
        # Check if amount is split across lines: "$X,XXX -" without "$Y,YYY" after it
        amount_incomplete = bool(
            re.search(r"\$[\d,]+\s*-", after_dates_clean)
            and not re.search(r"\$[\d,]+\s*-\s*\$[\d,]+", after_dates_clean)
        )

        # Collect continuation lines for this transaction
        j = i + 1
        asset_continuation = []
        subholding = ""
        description = ""

        while j < len(lines):
            next_line = lines[j]
            stripped = next_line.strip()

            # Empty line = potential end of transaction block
            if not stripped:
                j += 1
                # Skip consecutive blank lines
                while j < len(lines) and not lines[j].strip():
                    j += 1
                break

            # Column header repeated on new page — skip header block
            if re.match(r"\s*ID\s+Owner", stripped, re.IGNORECASE):
                j += 1
                while j < len(lines):
                    s = lines[j].strip()
                    if not s or re.match(r"(Type|Date|\$200|Gains)", s, re.IGNORECASE):
                        j += 1
                    else:
                        break
                continue

            # Metadata lines — extract info but don't add to asset name
            if _is_metadata_line(next_line):
                sub = _extract_subholding(next_line)
                if sub:
                    subholding = sub
                desc = _extract_description(next_line)
                if desc:
                    description = desc
                j += 1
                continue

            # Check if this is a new transaction (has two dates)
            if len(re.findall(date_re, stripped)) >= 2:
                break

            # Checkbox artifacts (single letters from cap gains column)
            if re.match(r"^[cdefg](\s+[cdefg])*\s*$", stripped):
                j += 1
                continue

            # Footer/boilerplate lines
            if re.match(r"\s*(Yes|No|\*\s*For|I CERTIFY|Digitally|L\s*:\s*US|Owner:)", stripped, re.IGNORECASE):
                j += 1
                continue

            # Dollar amount on its own line — amount continuation
            dollar_only = re.match(r"^\s*(\$[\d,]+)\s*$", stripped)
            if dollar_only:
                if amount_incomplete or (amount and amount.rstrip().endswith("-")):
                    # Complete the split amount
                    amount = amount.rstrip(" -") + " - " + dollar_only.group(1)
                    amount_incomplete = False
                elif amount_is_over:
                    amount = "Over " + dollar_only.group(1)
                    amount_is_over = False
                elif not amount:
                    amount = dollar_only.group(1)
                j += 1
                continue

            # Asset continuation with embedded dollar amount (multi-line split)
            # e.g., "[ST]                                                  $100,000"
            if amount_incomplete:
                em = re.search(r"(\$[\d,]+)", stripped)
                if em:
                    amount = amount.rstrip(" -") + " - " + em.group(1)
                    amount_incomplete = False
                    # Rest of the line is asset continuation
                    rest = stripped.replace(em.group(0), "").strip()
                    if rest and not _is_metadata_line(rest):
                        asset_continuation.append(rest)
                    j += 1
                    continue

            # Asset name continuation (e.g., "[ST]" on next line, or bond details)
            asset_continuation.append(stripped)
            j += 1

        # Build full asset name from main line + continuation
        full_asset = asset_line
        for cont in asset_continuation:
            full_asset += " " + cont

        # Clean up asset name
        full_asset = re.sub(r"\s+", " ", full_asset).strip()
        # Remove trailing checkbox artifacts
        full_asset = re.sub(r"\s+[cdefg](\s+[cdefg])*\s*$", "", full_asset)

        # Extract ticker from asset name (case-insensitive for older PDFs)
        ticker = ""
        ticker_m = re.search(r"\(([A-Za-z][A-Za-z0-9.]{0,9})\)", full_asset)
        if ticker_m:
            ticker = ticker_m.group(1).upper()

        # Extract asset type code [ST], [CS], [OP], [OT], [GS], etc.
        asset_type = ""
        atype_m = re.search(r"\[([A-Z]{2})\]", full_asset)
        if atype_m:
            asset_type = atype_m.group(1)

        amount = normalize_amount(amount)

        tx = {
            "asset_name": full_asset,
            "ticker": ticker,
            "asset_type": asset_type,
            "transaction_type": tx_type,
            "transaction_date": dates[0],
            "notification_date": dates[1],
            "amount": amount,
            "owner": owner,
            "subholding": subholding,
            "description": description,
            "filer_name": filer_name,
            "state_district": state_district,
        }
        transactions.append(tx)

        i = j

    return transactions


def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("HOUSE FD: Downloading and Parsing PTR PDFs")
    log.info("=" * 60)

    # Load all PTR filing records
    all_ptrs = get_all_ptrs()
    log.info(f"Total PTR filings in index: {len(all_ptrs)}")

    # Load state
    state = load_state()
    completed = set(state["completed"])
    failed = set(state.get("failed", []))
    scanned = set(state.get("scanned", []))

    # Filter to pending
    pending = [p for p in all_ptrs if p["doc_id"] not in completed
               and p["doc_id"] not in scanned]
    log.info(f"Already completed: {len(completed)}, scanned (skipped): {len(scanned)}, "
             f"pending: {len(pending)}")

    all_transactions = []
    # Load existing transactions from previous runs
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE) as f:
                all_transactions = json.load(f)
            log.info(f"Loaded {len(all_transactions)} existing transactions")
        except (json.JSONDecodeError, OSError):
            pass

    errors = 0
    new_tx_count = 0
    new_completed = 0
    new_scanned = 0

    for i, ptr in enumerate(pending):
        doc_id = ptr["doc_id"]
        year = ptr["year"]

        try:
            # Download
            time.sleep(RATE_LIMIT)
            pdf_path = download_pdf(doc_id, year)

            if not pdf_path:
                failed.add(doc_id)
                errors += 1
                if errors > 50:
                    log.error("Too many download errors, stopping")
                    break
                continue

            # Check if scanned
            if is_scanned_pdf(pdf_path):
                scanned.add(doc_id)
                new_scanned += 1
                continue

            # Extract and parse
            text = extract_text(pdf_path)
            txs = parse_transactions(text)

            if txs is None:
                scanned.add(doc_id)
                new_scanned += 1
                continue

            # Enrich with filing metadata
            member_name = ptr["member_name"]
            if not member_name:
                member_name = f"{ptr['first_name']} {ptr['last_name']}".strip()

            for tx in txs:
                tx["member_name"] = member_name
                tx["doc_id"] = doc_id
                tx["filing_date"] = ptr["filing_date"]
                tx["source_url"] = PTR_PDF_URL.format(year=year, doc_id=doc_id)

            all_transactions.extend(txs)
            completed.add(doc_id)
            new_completed += 1
            new_tx_count += len(txs)

            if (new_completed + new_scanned) % CHECKPOINT_INTERVAL == 0:
                log.info(f"  Progress: {new_completed + new_scanned}/{len(pending)} "
                         f"({new_completed} parsed, {new_scanned} scanned) "
                         f"— {new_tx_count} new transactions")
                state["completed"] = list(completed)
                state["failed"] = list(failed)
                state["scanned"] = list(scanned)
                save_state(state)
                with open(OUTPUT_FILE, "w") as f:
                    json.dump(all_transactions, f)

        except Exception as e:
            errors += 1
            log.warning(f"  Error on {doc_id}: {e}")
            if errors > 50:
                log.error("Too many errors, stopping")
                break

    # Final save
    state["completed"] = list(completed)
    state["failed"] = list(failed)
    state["scanned"] = list(scanned)
    save_state(state)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_transactions, f, indent=2)

    log.info(f"\nDone:")
    log.info(f"  Parsed: {len(completed)} PDFs")
    log.info(f"  Scanned (skipped): {len(scanned)} PDFs")
    log.info(f"  Failed: {len(failed)} PDFs")
    log.info(f"  Total transactions: {len(all_transactions)}")
    log.info(f"  Output: {OUTPUT_FILE}")

    # Summary stats
    members = set()
    tickers = set()
    for tx in all_transactions:
        if tx.get("member_name"):
            members.add(tx["member_name"])
        if tx.get("ticker"):
            tickers.add(tx["ticker"])

    log.info(f"  Unique members: {len(members)}")
    log.info(f"  Unique tickers: {len(tickers)}")

    dates = [tx["transaction_date"] for tx in all_transactions
             if tx.get("transaction_date")]
    if dates:
        log.info(f"  Date range: {min(dates)} to {max(dates)}")


if __name__ == "__main__":
    main()
