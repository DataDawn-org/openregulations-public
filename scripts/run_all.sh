#!/usr/bin/env bash
#
# Master runner: executes all three ingestion phases in order.
# Phase 1 (Federal Register) has no rate limit and runs fast.
# Phases 2 and 3 (Regulations.gov) share a rate limit and run sequentially.
#
# Usage:
#   ./run_all.sh              # run all phases
#   ./run_all.sh 2            # start from phase 2
#   ./run_all.sh 3            # start from phase 3
#
# Check progress:
#   tail -f ./logs/progress.txt
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
START_PHASE="${1:-1}"

echo "=============================================="
echo "  OpenRegs Data Ingestion"
echo "  Start time: $(date)"
echo "  Starting from phase: $START_PHASE"
echo "  Progress:  tail -f $LOG_DIR/progress.txt"
echo "=============================================="
echo ""

if [ "$START_PHASE" -le 1 ]; then
    echo ">>> Phase 1: Federal Register API"
    echo "    Log: $LOG_DIR/federal_register.log"
    python3 "$SCRIPT_DIR/01_federal_register.py" 2>&1
    echo ""
    echo ">>> Phase 1 finished at $(date)"
    echo ""
fi

if [ "$START_PHASE" -le 2 ]; then
    echo ">>> Phase 2: Regulations.gov Dockets & Documents"
    echo "    Log: $LOG_DIR/regs_gov_dockets_docs.log"
    python3 "$SCRIPT_DIR/02_regs_gov_dockets_docs.py" 2>&1
    echo ""
    echo ">>> Phase 2 finished at $(date)"
    echo ""
fi

if [ "$START_PHASE" -le 3 ]; then
    echo ">>> Phase 3: Regulations.gov Comment Headers"
    echo "    Log: $LOG_DIR/regs_gov_comments.log"
    python3 "$SCRIPT_DIR/03_regs_gov_comments.py" 2>&1
    echo ""
    echo ">>> Phase 3 finished at $(date)"
    echo ""
fi

echo "=============================================="
echo "  All phases complete"
echo "  End time: $(date)"
echo "=============================================="

# Print summary from state files
echo ""
echo "=== SUMMARY ==="
if [ -f "$LOG_DIR/fr_state.json" ]; then
    echo "Federal Register: $(python3 -c "import json; d=json.load(open('$LOG_DIR/fr_state.json')); print(f\"{d['total_documents']} documents, {len(d['completed_months'])} months\")")"
fi
if [ -f "$LOG_DIR/regs_dockets_docs_state.json" ]; then
    echo "Regs.gov Dockets/Docs: $(python3 -c "import json; d=json.load(open('$LOG_DIR/regs_dockets_docs_state.json')); print(f\"{d['total_dockets']} dockets, {d['total_documents']} documents\")")"
fi
if [ -f "$LOG_DIR/regs_comments_state.json" ]; then
    echo "Regs.gov Comments: $(python3 -c "import json; d=json.load(open('$LOG_DIR/regs_comments_state.json')); print(f\"{d['total_comments']} comment headers\")")"
fi
echo "==============="
