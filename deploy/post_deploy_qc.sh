#!/bin/bash
#
# post_deploy_qc.sh — Comprehensive post-deploy quality check for OpenRegs
#
# Run after every deploy to verify:
#   1. All services are running
#   2. All HTTPS endpoints respond
#   3. All explore pages load
#   4. All FTS indexes are working
#   5. New/critical tables are queryable
#   6. Cross-table joins work
#   7. APIs and llms.txt are accessible
#   8. No slow queries (>5s timeout)
#
# Usage:
#   bash deploy/post_deploy_qc.sh                    # default (regs.datadawn.org)
#   bash deploy/post_deploy_qc.sh http://localhost:8002  # test local
#
# Exit codes:
#   0 = all checks pass
#   1 = one or more checks failed
#
set -uo pipefail
# Note: no set -e — individual checks should continue on failure

BASE_REGS="${1:-https://regs.datadawn.org}"
BASE_990="https://data.datadawn.org"
API_REGS="${BASE_REGS}/openregs.json"
API_990="${BASE_990}/990data_public.json"
REMOTE_HOST="${OPENREGS_REMOTE_HOST:?Set OPENREGS_REMOTE_HOST (e.g. user@server)}"
TIMEOUT=30
PASS=0
FAIL=0
WARN=0

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass() { ((PASS++)); echo -e "  ${GREEN}PASS${NC}  $1"; }
fail() { ((FAIL++)); echo -e "  ${RED}FAIL${NC}  $1"; }
warn() { ((WARN++)); echo -e "  ${YELLOW}WARN${NC}  $1"; }

# Helper: query Datasette and return rows as JSON
query_regs() {
    curl -s --max-time "$TIMEOUT" "${API_REGS}?sql=$1&_shape=objects" 2>/dev/null
}

query_990() {
    curl -s --max-time "$TIMEOUT" "${API_990}?sql=$1&_shape=objects" 2>/dev/null
}

# Helper: extract count and ms from query result
check_query() {
    local label="$1"
    local sql="$2"
    local min_count="${3:-0}"
    local max_ms="${4:-5000}"

    local result
    result=$(query_regs "$sql")
    local count ms
    count=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['rows'][0]['cnt'])" 2>/dev/null)
    ms=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['query_ms']))" 2>/dev/null)

    if [ -z "$count" ]; then
        fail "$label: no response or error"
        return
    fi

    if [ "$count" -lt "$min_count" ]; then
        fail "$label: ${count} rows (expected >=${min_count})"
    elif [ "$ms" -gt "$max_ms" ]; then
        warn "$label: ${count} rows but SLOW (${ms}ms > ${max_ms}ms threshold)"
    else
        pass "$label: ${count} rows (${ms}ms)"
    fi
}

echo "============================================================"
echo "POST-DEPLOY QC — $(date '+%Y-%m-%d %H:%M:%S')"
echo "OpenRegs: ${BASE_REGS}"
echo "990:      ${BASE_990}"
echo "============================================================"
echo ""

# ── 1. SERVICE HEALTH ────────────────────────────────────────────
echo "=== 1. Service Health ==="
if [ "${BASE_REGS}" != "http://localhost:8002" ]; then
    for svc in openregs datasette 990-api openregs-api datadawn-mcp caddy; do
        status=$(ssh -o ConnectTimeout=5 "$REMOTE_HOST" "sudo systemctl is-active $svc" 2>/dev/null)
        if [ "$status" = "active" ]; then
            pass "$svc"
        else
            fail "$svc: $status"
        fi
    done
else
    echo "  (skipping remote service checks for localhost)"
fi
echo ""

# ── 2. HTTPS ENDPOINTS ──────────────────────────────────────────
echo "=== 2. HTTPS Endpoints ==="
for url in "${BASE_REGS}/" "${BASE_990}/" "${BASE_REGS}/llms.txt" "${BASE_990}/llms.txt"; do
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 "$url")
    if [ "$code" = "200" ]; then
        pass "$url ($code)"
    else
        fail "$url ($code)"
    fi
done

# REST APIs
for url in "${BASE_REGS}/api/" "${BASE_990}/api/"; do
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 "$url")
    if [ "$code" = "200" ]; then
        pass "API: $url ($code)"
    else
        fail "API: $url ($code)"
    fi
done
echo ""

# ── 3. EXPLORE PAGES ────────────────────────────────────────────
echo "=== 3. Explore Pages ==="
explore_pass=0
explore_fail=0
for page in search legislation member regulation lobbying contributions fara hearings nominations speeches-trades trade-conflicts committee-trades committee-donors revolving-door witness-lobby commenter-lobby lobbied-bills entity research speech api treaties cbo fara-hearings; do
    code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "${BASE_REGS}/explore/${page}.html")
    if [ "$code" = "200" ]; then
        ((explore_pass++))
    else
        fail "explore/${page}.html ($code)"
        ((explore_fail++))
    fi
done
if [ "$explore_fail" -eq 0 ]; then
    pass "All ${explore_pass} explore pages return 200"
fi
echo ""

# ── 4. FTS INDEX TESTS ──────────────────────────────────────────
echo "=== 4. FTS Search Tests ==="
check_query "Federal Register FTS" "SELECT+COUNT(*)+as+cnt+FROM+federal_register_fts+WHERE+federal_register_fts+MATCH+'climate'" 100
check_query "Comments FTS" "SELECT+COUNT(*)+as+cnt+FROM+comments_fts+WHERE+comments_fts+MATCH+'pollution'" 100
check_query "Legislation FTS" "SELECT+COUNT(*)+as+cnt+FROM+legislation_fts+WHERE+legislation_fts+MATCH+'agriculture'" 100
check_query "Dockets FTS" "SELECT+COUNT(*)+as+cnt+FROM+dockets_fts+WHERE+dockets_fts+MATCH+'EPA'" 100
check_query "Spending FTS" "SELECT+COUNT(*)+as+cnt+FROM+spending_awards_fts+WHERE+spending_awards_fts+MATCH+'wildlife'" 100
check_query "Lobbying FTS" "SELECT+COUNT(*)+as+cnt+FROM+lobbying_fts+WHERE+lobbying_fts+MATCH+'pharmaceutical'" 100
check_query "FARA FTS" "SELECT+COUNT(*)+as+cnt+FROM+fara_registrants_fts+WHERE+fara_registrants_fts+MATCH+'japan'" 1
check_query "FEC Employer FTS" "SELECT+COUNT(*)+as+cnt+FROM+fec_employer_fts+WHERE+fec_employer_fts+MATCH+'google'" 1
check_query "CREC FTS" "SELECT+COUNT(*)+as+cnt+FROM+crec_fts+WHERE+crec_fts+MATCH+'trade'" 100
check_query "GAO FTS" "SELECT+COUNT(*)+as+cnt+FROM+gao_reports_fts+WHERE+gao_reports_fts+MATCH+'cybersecurity'" 10
check_query "Hearings FTS" "SELECT+COUNT(*)+as+cnt+FROM+hearings_fts+WHERE+hearings_fts+MATCH+'oversight'" 100
check_query "Earmarks FTS" "SELECT+COUNT(*)+as+cnt+FROM+earmarks_fts+WHERE+earmarks_fts+MATCH+'highway'" 10
check_query "CFR FTS" "SELECT+COUNT(*)+as+cnt+FROM+cfr_fts+WHERE+cfr_fts+MATCH+'hazardous'" 100
echo ""

# ── 5. CORE TABLE ROW COUNTS ────────────────────────────────────
echo "=== 5. Core Table Row Counts ==="
check_query "Federal Register" "SELECT+COUNT(*)+as+cnt+FROM+federal_register" 900000
check_query "Comments" "SELECT+COUNT(*)+as+cnt+FROM+comments" 9000000
check_query "Documents" "SELECT+COUNT(*)+as+cnt+FROM+documents" 1500000
check_query "Dockets" "SELECT+COUNT(*)+as+cnt+FROM+dockets" 200000
check_query "Legislation" "SELECT+COUNT(*)+as+cnt+FROM+legislation" 300000
check_query "Congressional Record" "SELECT+COUNT(*)+as+cnt+FROM+congressional_record" 800000
check_query "Congress Members" "SELECT+COUNT(*)+as+cnt+FROM+congress_members" 12000
check_query "Lobbying Filings" "SELECT+COUNT(*)+as+cnt+FROM+lobbying_filings" 1800000
check_query "FEC Contributions" "SELECT+COUNT(*)+as+cnt+FROM+fec_contributions" 4000000
check_query "Stock Trades" "SELECT+COUNT(*)+as+cnt+FROM+stock_trades" 50000
check_query "GAO Reports" "SELECT+COUNT(*)+as+cnt+FROM+gao_reports" 70000
echo ""

# ── 6. NEW TABLE VERIFICATION ───────────────────────────────────
echo "=== 6. New Tables (2026-03-29 build) ==="
check_query "OIRA Reviews" "SELECT+COUNT(*)+as+cnt+FROM+oira_reviews" 40000
check_query "OIRA Meetings" "SELECT+COUNT(*)+as+cnt+FROM+oira_meetings" 8000
check_query "OIRA Attendees" "SELECT+COUNT(*)+as+cnt+FROM+oira_meeting_attendees" 80000
check_query "FEC Operating Expenditures" "SELECT+COUNT(*)+as+cnt+FROM+fec_operating_expenditures" 15000000
check_query "FEC PAC Summary" "SELECT+COUNT(*)+as+cnt+FROM+fec_pac_summary" 90000
check_query "FEC Independent Expenditures" "SELECT+COUNT(*)+as+cnt+FROM+fec_independent_expenditures" 600000
check_query "FEC Electioneering" "SELECT+COUNT(*)+as+cnt+FROM+fec_electioneering" 1000
check_query "FEC Communication Costs" "SELECT+COUNT(*)+as+cnt+FROM+fec_communication_costs" 20000
check_query "IG Reports" "SELECT+COUNT(*)+as+cnt+FROM+ig_reports" 30000
check_query "IG Recommendations" "SELECT+COUNT(*)+as+cnt+FROM+ig_recommendations" 10000
echo ""

# ── 7. CROSS-TABLE JOINS ────────────────────────────────────────
echo "=== 7. Cross-Table Joins ==="
check_query "FR↔Regs crossref" "SELECT+COUNT(*)+as+cnt+FROM+fr_regs_crossref" 300000
check_query "Earmark-spending crossref" "SELECT+COUNT(*)+as+cnt+FROM+earmark_spending_crossref" 100000
check_query "Witness-lobby overlap" "SELECT+COUNT(*)+as+cnt+FROM+witness_lobby_overlap" 1000
check_query "Speeches near trades" "SELECT+COUNT(*)+as+cnt+FROM+speeches_near_trades" 50000
check_query "Revolving door" "SELECT+COUNT(*)+as+cnt+FROM+revolving_door" 100
check_query "Docket importance" "SELECT+COUNT(*)+as+cnt+FROM+docket_importance" 100000
echo ""

# ── 8. 990 DATABASE SPOT CHECK ──────────────────────────────────
echo "=== 8. 990 Database ==="
result=$(query_990 "SELECT+COUNT(*)+as+cnt+FROM+returns")
count=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['rows'][0]['cnt'])" 2>/dev/null)
ms=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['query_ms']))" 2>/dev/null)
if [ -n "$count" ] && [ "$count" -gt 5000000 ]; then
    pass "990 returns: ${count} rows (${ms}ms)"
else
    fail "990 returns: ${count:-no response}"
fi

result=$(query_990 "SELECT+COUNT(*)+as+cnt+FROM+fts_bmf+WHERE+fts_bmf+MATCH+'american+cancer'")
count=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['rows'][0]['cnt'])" 2>/dev/null)
if [ -n "$count" ] && [ "$count" -gt 0 ]; then
    pass "990 FTS (fts_bmf): ${count} results"
else
    fail "990 FTS (fts_bmf): ${count:-no response}"
fi
echo ""

# ── SUMMARY ─────────────────────────────────────────────────────
echo "============================================================"
echo "QC SUMMARY"
echo "============================================================"
echo -e "  ${GREEN}PASS: ${PASS}${NC}"
if [ "$WARN" -gt 0 ]; then
    echo -e "  ${YELLOW}WARN: ${WARN}${NC}"
fi
if [ "$FAIL" -gt 0 ]; then
    echo -e "  ${RED}FAIL: ${FAIL}${NC}"
    echo ""
    echo "Deploy has issues — review failures above."
    exit 1
else
    echo ""
    echo "All checks passed. Deploy is healthy."
    exit 0
fi
