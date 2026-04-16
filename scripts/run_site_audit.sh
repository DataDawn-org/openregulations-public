#!/bin/bash
# run_site_audit.sh -- Cron wrapper for site_audit.py.
#
# Runs the audit, logs output to the rolling log, saves the last summary for
# debugging, and pings healthchecks.io with the summary body so we can see
# pass/fail counts and the specific failing URLs in the hc.io dashboard
# without having to SSH in.
#
# Usage (cron):
#     0 9 * * 0 /mnt/data/datadawn/openregs/scripts/run_site_audit.sh <HC_UUID>
#
# Usage (manual test, no hc.io ping):
#     bash /mnt/data/datadawn/openregs/scripts/run_site_audit.sh
#
# Exit code mirrors the audit's exit code (0 = all criticals passed, 1 = at
# least one critical failure). Cron translates this into the hc.io success
# or fail ping.

set -uo pipefail

UUID="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
AUDIT="$SCRIPT_DIR/site_audit.py"
LOG_DIR="$PROJECT_DIR/logs"
LOG="$LOG_DIR/site_audit_cron.log"
BODY="$LOG_DIR/site_audit_latest.txt"

mkdir -p "$LOG_DIR"

{
    echo
    echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
} >> "$LOG"

# tee the audit output to both the body file (for hc.io) and the rolling log.
python3 "$AUDIT" 2>&1 | tee "$BODY" >> "$LOG"
ec=${PIPESTATUS[0]}

if [[ -n "$UUID" ]]; then
    # hc.io accepts up to 10 KB body via POST --data-binary.
    curl -fsS -m 10 --retry 3 --data-binary "@$BODY" \
        "https://hc-ping.com/${UUID}/${ec}" > /dev/null || true
fi

exit "$ec"
