#!/usr/bin/env bash
#
# Wait for FDA 2022 to finish downloading, then gracefully stop Phase 3
# and launch EPA backfill.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PID=913519
LOG="$PROJECT_DIR/logs/regs_gov_comments.log"
STATE="$PROJECT_DIR/logs/regs_comments_state.json"
PROGRESS="$PROJECT_DIR/logs/progress.txt"

echo "[$(date)] Watching for FDA 2022 completion..."
echo "[$(date)] Phase 3 PID: $PID"

# Wait for the FDA 2022 year-summary line to appear in the log
# Pattern: "[FDA] 2022: NNNNN comments"
while true; do
    # Match year-summary line (no "subdividing" suffix)
    if grep '\[FDA\] 2022: [0-9]* comments' "$LOG" 2>/dev/null | grep -qv 'subdividing'; then
        echo "[$(date)] FDA 2022 complete! Sending SIGTERM to PID $PID..."
        break
    fi

    # Check process is still alive
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "[$(date)] Process $PID is no longer running. Checking if FDA 2022 finished..."
        break
    fi

    sleep 10
done

# Send graceful shutdown signal
if kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID"
    echo "[$(date)] SIGTERM sent. Waiting for graceful shutdown..."
    # Wait for process to exit (up to 60s)
    for i in $(seq 1 60); do
        if ! kill -0 "$PID" 2>/dev/null; then
            echo "[$(date)] Process exited cleanly."
            break
        fi
        sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
        echo "[$(date)] WARNING: Process still running after 60s"
    fi
fi

# Mark FDA 2023-2026 + FWS/APHIS years as completed in state
# so Phase 3 won't re-download them if restarted
echo "[$(date)] Marking FDA 2023-2026 and FWS/APHIS as skipped in state..."
python3 -c "
import json
from pathlib import Path

state_file = Path('$STATE')
state = json.loads(state_file.read_text())

# Mark remaining FDA years as skipped (0 comments)
for year in range(2023, 2027):
    key = f'comments:FDA:{year}'
    if key not in state['completed']:
        state['completed'][key] = 0
        print(f'  Marked {key} as skipped')

# Mark FWS and APHIS as skipped (not a priority)
for agency in ['FWS', 'APHIS']:
    for year in range(1994, 2027):
        key = f'comments:{agency}:{year}'
        if key not in state['completed']:
            state['completed'][key] = 0
    print(f'  Marked all {agency} years as skipped')

tmp = state_file.with_suffix('.tmp')
tmp.write_text(json.dumps(state, indent=2))
tmp.rename(state_file)
print('State updated.')
"

echo "[$(date)] Phase 3 stopped after FDA 2022."
echo "[$(date)] Launching EPA backfill (Phase 3b)..."

# Log to progress
echo "[$(date)] Phase 3 stopped after FDA 2022. Starting EPA backfill." >> "$PROGRESS"

# Launch backfill
cd "$PROJECT_DIR"
python3 "$PROJECT_DIR/scripts/04_backfill_comments.py --agency EPA

echo "[$(date)] EPA backfill complete."
