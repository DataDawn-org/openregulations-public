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

# Phase 5 and after: 05 deletes aphis.db and rebuilds it without pdf_sha256 or cert_verification, so every rebuild
# runs the certificate checker (new PDFs only; it resumes) and the populate step after it. aphis.db is what deploy.sh
# uploads as it is, so a copy is taken first and put back if 05, the checker or the populate step fails: the values
# already served are kept (owner queue 944; yellow 26875742's plan step 8; relay datadawn green #2 item 6).
# Test: selftest_aphis_cert_packet.py.
DB_FILE="$BASE_DIR/db/aphis.db"

# Put the kept copy back after a failed rebuild (purple 56fbf00a's AC1). The failed build's file and its -wal, -shm
# and -journal are moved aside together as aphis.db.failed-<time>-<pid>* (a consistent set, kept for a look, never
# deleted; one per failure, N4): a -wal
# left beside the restored file would be read into it, and the failed build would be served. "Put back" counts only
# once the file reads as the copy: the copy's bytes, no sidecar beside it, integrity ok.
FAILED_SET=""
put_back() {
    local kept="$1" want="$2" s base taken n=1
    # Each failed set is named by its own time and pid (purple 56fbf00a's N4), so a later failure never lands beside
    # an older set's -wal; every set is kept. A name already taken (any member) moves to the next free suffix, -2, -3
    # and on, and the copy is still put back (purple 74b06e3f's S1). mv -n never overwrites, and each move is checked.
    base="${DB_FILE:?}.failed-$(date +%Y%m%dT%H%M%S)-$$"
    FAILED_SET="$base"
    while :; do
        taken=""
        for s in "" "-wal" "-shm" "-journal"; do
            if [[ -e "${FAILED_SET:?}$s" ]]; then taken="${FAILED_SET}$s"; fi
        done
        if [[ -z "$taken" ]]; then break; fi
        n=$((n + 1))
        if (( n > 99 )); then
            log "PUT BACK NOT CONFIRMED: no free failed-set name after 99 tries; nothing moved"
            return 1
        fi
        log "Failed-set name taken ($(basename "$taken")); using suffix -$n"
        FAILED_SET="$base-$n"
    done
    for s in "" "-wal" "-shm" "-journal"; do
        if [[ -e "${DB_FILE:?}$s" ]]; then
            mv -n "${DB_FILE:?}$s" "${FAILED_SET:?}$s"
            if [[ -e "${DB_FILE:?}$s" ]]; then
                log "PUT BACK NOT CONFIRMED: ${DB_FILE}$s could not be moved aside without overwriting"
                return 1
            fi
        fi
    done
    mv -f "${kept:?}" "${DB_FILE:?}"
    for s in "-wal" "-shm" "-journal"; do
        if [[ -e "${DB_FILE:?}$s" ]]; then
            log "PUT BACK NOT CONFIRMED: ${DB_FILE}$s is beside the restored file"
            return 1
        fi
    done
    if [[ "$(sha256sum "$DB_FILE" | cut -d' ' -f1)" != "$want" ]]; then
        log "PUT BACK NOT CONFIRMED: aphis.db's bytes are not the kept copy's"
        return 1
    fi
    python3 - "$DB_FILE" <<'PY' || { log "PUT BACK NOT CONFIRMED: aphis.db does not read as a sound database"; return 1; }
import sqlite3, sys
c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro&immutable=1", uri=True)
ok = c.execute("PRAGMA integrity_check").fetchone()[0]
c.close()
sys.exit(0 if ok == "ok" else 1)
PY
}

rebuild_database() {
    local kept="" want=""
    if [[ -f "$DB_FILE" ]]; then
        # sqlite's backup API: a consistent copy even with a WAL beside the file
        python3 - "$DB_FILE" "$DB_FILE.prev" <<'PY'
import sqlite3, sys
src = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
dst = sqlite3.connect(sys.argv[2])
src.backup(dst)
dst.close()
src.close()
PY
        kept="$DB_FILE.prev"
        want="$(sha256sum "$kept" | cut -d' ' -f1)"
        log "Kept a copy of aphis.db before the rebuild: $kept (sha256 $want)"
    fi
    local rc=0
    run_script "Build Database" "$SCRIPT_DIR/05_build_database.py" || rc=$?
    if [[ $rc -eq 0 ]]; then
        run_script "Certificate check" "$SCRIPT_DIR/verify_pdf_cert_match.py" || rc=$?
    fi
    if [[ $rc -eq 0 ]]; then
        run_script "Populate PDF paths, fingerprints and certificate verdicts" "$SCRIPT_DIR/populate_pdf_paths.py" || rc=$?
    fi
    if [[ $rc -ne 0 ]]; then
        if [[ -n "$kept" ]]; then
            if put_back "$kept" "$want"; then
                log "=== REBUILD FAILED (exit $rc): aphis.db put back as it was before the rebuild, and checked; the failed build is aside as $(basename "$FAILED_SET")* ==="
            else
                log "=== REBUILD FAILED (exit $rc), AND aphis.db COULD NOT BE CONFIRMED PUT BACK: do not deploy until it is checked ==="
            fi
        else
            log "=== REBUILD FAILED (exit $rc): there was no aphis.db before it to put back ==="
        fi
        exit "$rc"
    fi
}

log "APHIS extraction starting"
log "Base directory: $BASE_DIR"
log "Log file: $LOG_FILE"

if $DB_ONLY; then
    rebuild_database
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

# Phase 5: Build database, then the certificate check and the populate step
rebuild_database

log ""
log "=== All phases complete ==="
log "Database: $BASE_DIR/db/aphis.db"
log "Raw data: $BASE_DIR/raw/"
if ! $SKIP_PDFS; then
    log "PDFs: $BASE_DIR/pdfs/"
fi
log "Logs: $LOG_FILE"
