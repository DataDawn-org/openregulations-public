#!/usr/bin/env bash
#
# Master runner for APHIS Public Search Tool extraction.
#
# Phases:
#   1. Discover API action names
#   2. Extract all categories (metadata)
#   3. Download licensee list
#   4. Download PDFs (long-running, can run separately)
#   5. Build SQLite database
#
# Usage:
#   ./run_aphis.sh              # Run all phases
#   ./run_aphis.sh --skip-pdfs  # Skip PDF downloads (fastest)
#   ./run_aphis.sh --pdfs-only  # Only download PDFs
#   ./run_aphis.sh --db-only    # Only rebuild database
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$BASE_DIR/logs"

mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/run_${TIMESTAMP}.log"

# Parse arguments
SKIP_PDFS=false
PDFS_ONLY=false
DB_ONLY=false

for arg in "$@"; do
    case $arg in
        --skip-pdfs) SKIP_PDFS=true ;;
        --pdfs-only) PDFS_ONLY=true ;;
        --db-only) DB_ONLY=true ;;
        --help)
            echo "Usage: $0 [--skip-pdfs|--pdfs-only|--db-only]"
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

run_script() {
    local name="$1"
    local script="$2"
    shift 2

    log "=== Starting: $name ==="
    local start_time=$(date +%s)

    if python3 "$script" "$@" 2>&1 | tee -a "$LOG_FILE"; then
        local end_time=$(date +%s)
        local duration=$(( end_time - start_time ))
        log "=== Completed: $name (${duration}s) ==="
    else
        local exit_code=$?
        log "=== FAILED: $name (exit code $exit_code) ==="
        return $exit_code
    fi
}

log "APHIS extraction starting"
log "Base directory: $BASE_DIR"
log "Log file: $LOG_FILE"

if $DB_ONLY; then
    run_script "Build Database" "$SCRIPT_DIR/05_build_database.py"
    log "Done (database only)."
    exit 0
fi

if $PDFS_ONLY; then
    run_script "Download PDFs" "$SCRIPT_DIR/04_download_pdfs.py"
    log "Done (PDFs only)."
    exit 0
fi

# Phase 1: Discover actions
run_script "Action Discovery" "$SCRIPT_DIR/01_discover_actions.py"

# Phase 2: Extract all categories
run_script "Bulk Extraction" "$SCRIPT_DIR/02_extract_all.py"

# Phase 3: Download licensee list
run_script "Licensee List" "$SCRIPT_DIR/03_download_licensee_list.py"

# Phase 4: Download PDFs (optional)
if ! $SKIP_PDFS; then
    run_script "PDF Downloads" "$SCRIPT_DIR/04_download_pdfs.py"
fi

# Phase 5: Build database
run_script "Build Database" "$SCRIPT_DIR/05_build_database.py"

log ""
log "=== All phases complete ==="
log "Database: $BASE_DIR/db/aphis.db"
log "Raw data: $BASE_DIR/raw/"
if ! $SKIP_PDFS; then
    log "PDFs: $BASE_DIR/pdfs/"
fi
log "Logs: $LOG_FILE"
