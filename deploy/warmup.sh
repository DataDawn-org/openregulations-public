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

# --- First reads, timed, BEFORE the COUNTs below --------------------------------------------------------------------
# The row: working-docs/dated_obligations_2026-10-07_orange_relay15.md (relay datadawn orange #15; the hand install
# approved 2026-10-07, decisions_log §346). One comment search, one docket query and one comment-text read run
# first after each restart, so every deploy records the first read of each kind, timed, before any reader.
# Each read's query_ms, and the warm-up's total, go to the JOURNAL (tag openregs-warmup): the unit's own output file is
# under PrivateTmp and is discarded at each restart. Read them with: journalctl -t openregs-warmup
# A read that fails or times out is logged as such and the warm-up carries on; nothing here can stop the script (the
# unit already backgrounds it, so it never blocks the service either).
# Test: openregs/deploy/selftest_warmup_first_reads.py
JTAG="openregs-warmup"
READ_TIMEOUT="${WARMUP_READ_TIMEOUT:-120}"
T0_MS=$(date +%s%3N)

jlog() {
    # One line to stdout and to the journal: systemd-cat (the journal's stream socket), else logger (/dev/log).
    echo "$1"
    if printf '%s\n' "$1" | systemd-cat -t "$JTAG" -p info >/dev/null 2>&1; then return 0; fi
    if logger -t "$JTAG" -- "$1" >/dev/null 2>&1; then return 0; fi
    echo "  (journal write failed: $JTAG)"
    return 0
}

first_read() {
    # $1 = name, $2 = SQL. Journals "first-read NAME status=ok query_ms=N rows=N wall_ms=N", or status=timeout, or
    # status=fail with the HTTP code (Datasette's 10 s limit, its HTTP 400 "SQL query took too long", is named
    # limit=sql_time_limit, so a cold read that ran out of time is told apart from a 429 or a 5xx) or curl's exit.
    local name="$1" q t0 t1 rc=0 body="" out="" code="" why=""
    q=$(python3 -c 'import sys, urllib.parse as u; print(u.quote(sys.argv[1], safe=""))' "$2" 2>/dev/null) || q=""
    if [ -z "$q" ]; then jlog "first-read $name status=fail sql_not_encoded"; return 0; fi
    t0=$(date +%s%3N)
    body=$(curl -s -m "$READ_TIMEOUT" -w '\n%{http_code}' "$API?sql=${q}&_shape=objects" 2>/dev/null) || rc=$?
    t1=$(date +%s%3N)
    code="${body##*$'\n'}"; body="${body%$'\n'*}"
    if [ "$rc" -eq 28 ]; then
        jlog "first-read $name status=timeout limit_s=$READ_TIMEOUT wall_ms=$((t1 - t0))"; return 0
    elif [ "$rc" -ne 0 ]; then
        jlog "first-read $name status=fail curl_exit=$rc wall_ms=$((t1 - t0))"; return 0
    elif [ "$code" != "200" ]; then
        case "$body" in *"SQL query took too long"*) why=" limit=sql_time_limit" ;; esac
        jlog "first-read $name status=fail http=${code}${why} wall_ms=$((t1 - t0))"; return 0
    fi
    out=$(printf '%s' "$body" | python3 -c 'import sys, json; d = json.load(sys.stdin); print("query_ms=%.0f rows=%d" % (float(d["query_ms"]), len(d["rows"])))' 2>/dev/null) || out=""
    if [ -z "$out" ]; then
        jlog "first-read $name status=fail unreadable_answer wall_ms=$((t1 - t0))"
    else
        jlog "first-read $name status=ok $out wall_ms=$((t1 - t0))"
    fi
    return 0
}

jlog "warm-up start base=$BASE"
# The comment search: the MCP's search_comments shape (datadawn-mcp/server.py), the reader path for comment search.
first_read search "SELECT c.id, c.docket_id, c.posted_date FROM comments c WHERE c.rowid IN (SELECT rowid FROM comments_fts WHERE comments_fts MATCH 'nutrition') ORDER BY c.posted_date DESC LIMIT 20"
# The docket query: comments by docket_id (FMCSA-2025-0886, the first docket page read after the 10-10 restart).
first_read docket "SELECT id, title, posted_date FROM comments WHERE docket_id = 'FMCSA-2025-0886' ORDER BY posted_date DESC LIMIT 50"
# The comment-text read: comment_details.comment_text for one id.
first_read text "SELECT id, comment_text FROM comment_details WHERE id = 'AMS-2006-0079-0006'"
jlog "first reads done after_ms=$(( $(date +%s%3N) - T0_MS ))"
# --- end of the first reads ------------------------------------------------------------------------------------------

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
jlog "warm-up done total_ms=$(( $(date +%s%3N) - T0_MS ))"
