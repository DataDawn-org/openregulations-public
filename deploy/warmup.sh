#!/usr/bin/env bash
#
# warmup.sh — Pre-warm key SQLite pages into OS buffer cache
# Run after Datasette starts to avoid cold-cache timeouts
#
# Usage: bash warmup.sh [host:port]
#
set -euo pipefail

BASE="${1:-http://localhost:8002}"
API="$BASE/openregs.json"

echo "[$(date '+%H:%M:%S')] Warming up OpenRegs database..."

# Core tables (touch index + data pages)
tables=(
    congress_members
    stock_trades
    crec_speakers
    congressional_record
    member_votes
    roll_call_votes
    legislation
    lobbying_activities
    spending_awards
    comments
    dockets
    documents
    federal_register
    fara_registrants
    fec_employer_totals
    committee_memberships
    committees
)

for t in "${tables[@]}"; do
    result=$(curl -s -m 120 "$API?sql=SELECT+COUNT(*)+as+cnt+FROM+${t}&_shape=objects" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d[\"rows\"][0][\"cnt\"]:>12,} rows  {d[\"query_ms\"]:>8.0f}ms')" 2>/dev/null || echo "         TIMEOUT")
    printf "  %-30s %s\n" "$t" "$result"
done

# FTS indexes (these are separate B-trees)
fts_tables=(
    dockets_fts
    federal_register_fts
    comments_fts
    legislation_fts
    spending_awards_fts
    lobbying_fts
    fara_registrants_fts
    fec_employer_fts
)

echo ""
echo "[$(date '+%H:%M:%S')] Warming FTS indexes..."
for t in "${fts_tables[@]}"; do
    result=$(curl -s -m 120 "$API?sql=SELECT+COUNT(*)+as+cnt+FROM+${t}&_shape=objects" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d[\"rows\"][0][\"cnt\"]:>12,} rows  {d[\"query_ms\"]:>8.0f}ms')" 2>/dev/null || echo "         TIMEOUT")
    printf "  %-30s %s\n" "$t" "$result"
done

echo ""
echo "[$(date '+%H:%M:%S')] Warmup complete"
