#!/usr/bin/env bash
#
# deploy.sh — Deploy OpenRegs database to Digital Ocean Datasette instance.
#
# This mirrors the DataDawn (990project) deployment pattern:
#   - Caddy reverse proxy on port 443
#   - Datasette serving SQLite on localhost:8002
#   - systemd service for process management
#
# Usage:
#   ./deploy.sh              # full deploy
#   ./deploy.sh --setup      # first-time server setup
#   ./deploy.sh --db-only    # just upload new database
#   ./deploy.sh --dry-run    # show what would happen
#
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────
PROJECT_DIR="/mnt/data/datadawn/openregs"
DB="$PROJECT_DIR/openregs.db"
APHIS_DB="$PROJECT_DIR/aphis/db/aphis.db"
LOBBYING_DB="$PROJECT_DIR/lobbying.db"
FARA_DB="$PROJECT_DIR/fara.db"
REMOTE_HOST="${OPENREGS_REMOTE_HOST:?Set OPENREGS_REMOTE_HOST (e.g. user@your-server)}"
REMOTE_DIR="/opt/openregs"
REMOTE_DB="$REMOTE_DIR/openregs.db"
REMOTE_APHIS_DB="$REMOTE_DIR/aphis.db"
REMOTE_LOBBYING_DB="$REMOTE_DIR/lobbying.db"
REMOTE_FARA_DB="$REMOTE_DIR/fara.db"
DATASETTE_PORT=8002  # DataDawn uses 8001
DOMAIN="regs.datadawn.org"  # Subdomain for the regs data

DRY_RUN=0
SETUP=0
DB_ONLY=0

for arg in "$@"; do
    case $arg in
        --dry-run) DRY_RUN=1 ;;
        --setup) SETUP=1 ;;
        --db-only) DB_ONLY=1 ;;
        --help)
            echo "Usage: $0 [--setup|--db-only|--dry-run]"
            exit 0
            ;;
    esac
done

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# ── Backup propagation to local + B2 (post-deploy, non-critical) ─────────
#
# Called after a successful deploy. Takes the pre-deploy snapshot that was
# written to the VPS (cp $REMOTE_DB → $BACKUP_DIR/$BACKUP_FILE) and:
#   1. Quick-checks it on VPS (refuse to propagate structural corruption)
#   2. Rsyncs down to $PROJECT_DIR/backups/
#   3. Rotates local to last 3 (via deploy/rotate_local_backups.py)
#   4. rclone copies to b2:someones-backup/openregs-weekly/
#   5. Rotates B2 to last 3 (inline)
#
# We use PRAGMA quick_check (not integrity_check) here. quick_check verifies
# B-tree links, page-header consistency, and freelist — catches the failure
# modes that actually occur during a cp+rsync (torn pages, truncation,
# header mismatch). The full integrity_check adds a cell-level scan that's
# ~10× slower and protects against scenarios we've never hit in this project's
# history. See bestpractices/pipeline_verification.md "2026-04-25 incident"
# for context on why we made this trade. Use full integrity_check at rollback
# time (per ROLLBACK.md) — that's where the extra rigor matters.
#
# Any failure here does NOT abort the deploy — the VPS snapshot is still in
# place, and deploy/check_backup_freshness.py (run daily via cron) alerts
# if propagation silently stops working over time.
#
# Returns 0 on success, non-zero on any failure (logged).
propagate_backup_to_local_and_b2() {
    local backup_file="$1"
    if [[ -z "$backup_file" ]]; then
        log "  (no backup was made — skipping propagation)"
        return 0
    fi
    local remote_backup="$BACKUP_DIR/$backup_file"
    local local_backup_dir="$PROJECT_DIR/backups"
    local b2_remote="b2:someones-backup/openregs-weekly"
    local helper="$PROJECT_DIR/deploy/rotate_local_backups.py"

    log "=== Backup propagation (local + B2) ==="

    # 1. Quick-check the VPS snapshot before replicating it.
    log "Quick-checking VPS snapshot (structural integrity)..."
    if ! ssh "$REMOTE_HOST" "python3 -c \"
import sqlite3, sys
r = sqlite3.connect('file:$remote_backup?mode=ro', uri=True).execute('PRAGMA quick_check').fetchone()[0]
sys.exit(0 if r == 'ok' else 2)
\""; then
        log "  WARNING: quick_check FAILED on VPS snapshot — refusing to propagate corruption"
        return 2
    fi
    log "  quick_check: ok"

    # 2. Pull the VPS snapshot down to the local backups dir.
    mkdir -p "$local_backup_dir"
    log "Pulling $remote_backup → $local_backup_dir/"
    if ! rsync -a --partial-dir=.rsync-partials --timeout=600 \
            "$REMOTE_HOST:$remote_backup" "$local_backup_dir/$backup_file"; then
        log "  WARNING: rsync down failed; local tier NOT updated this run"
        return 3
    fi
    log "  local copy: $local_backup_dir/$backup_file"

    # 3. Rotate local to last 3.
    if ! python3 "$helper" --dir "$local_backup_dir" --keep 3; then
        log "  WARNING: local rotation helper failed"
        return 4
    fi

    # 4. Push to B2.
    log "Pushing to $b2_remote/"
    if ! rclone copy "$local_backup_dir/$backup_file" "$b2_remote/"; then
        log "  WARNING: B2 push failed"
        return 5
    fi

    # 5. Rotate B2 to last 3 (inline — sort lexically on filename works
    # because our format encodes timestamp in the name).
    log "Rotating B2 backups (keep 3)..."
    local b2_excess
    b2_excess=$(rclone lsf "$b2_remote/" --files-only 2>/dev/null \
                | grep -E '^openregs-predeploy-.*\.db$' \
                | sort | head -n -3 || true)
    if [[ -n "$b2_excess" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            log "  rclone delete $b2_remote/$f"
            rclone delete "$b2_remote/$f" || log "    (delete failed for $f, continuing)"
        done <<< "$b2_excess"
    else
        log "  B2 already at or under 3 backups, nothing to rotate"
    fi

    log "Backup propagation complete"
    return 0
}

# ── Preflight checks ──────────────────────────────────────────────────────
if [[ ! -f "$DB" ]]; then
    log "ERROR: Database not found: $DB"
    log "Run scripts/05_build_database.py first."
    exit 1
fi

DB_SIZE_MB=$(du -m "$DB" | cut -f1)
log "Database: $DB (${DB_SIZE_MB}MB)"

# ── First-time server setup ───────────────────────────────────────────────
if [[ $SETUP -eq 1 ]]; then
    log "=== First-time server setup ==="

    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would set up $REMOTE_HOST"
    else
        log "Creating remote directories..."
        ssh "$REMOTE_HOST" "sudo mkdir -p $REMOTE_DIR && sudo chown \$(whoami):\$(whoami) $REMOTE_DIR"

        log "Installing Datasette (if not already installed)..."
        ssh "$REMOTE_HOST" 'sudo pip install datasette datasette-cors 2>/dev/null || sudo pip3 install datasette datasette-cors'

        log "Creating systemd service..."
        ssh "$REMOTE_HOST" "sudo tee /etc/systemd/system/openregs.service > /dev/null" <<SERVICE_EOF
[Unit]
Description=OpenRegs Datasette
After=network.target

[Service]
Type=simple
User=${REMOTE_HOST%%@*}
WorkingDirectory=$REMOTE_DIR
ExecStart=$(ssh "$REMOTE_HOST" 'which datasette') serve $REMOTE_DB $REMOTE_APHIS_DB $REMOTE_LOBBYING_DB $REMOTE_FARA_DB $REMOTE_DIR/open_comments.db \\
    --host 127.0.0.1 \\
    --port $DATASETTE_PORT \\
    --metadata $REMOTE_DIR/metadata.json \\
    --template-dir $REMOTE_DIR/templates \\
    --static explore:$REMOTE_DIR/explore \\
    --setting sql_time_limit_ms 30000 \\
    --setting max_returned_rows 1000 \\
    --setting default_allow_sql 1
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE_EOF

        ssh "$REMOTE_HOST" 'sudo systemctl daemon-reload'
        ssh "$REMOTE_HOST" 'sudo systemctl enable openregs'

        log "Adding Caddy route for $DOMAIN..."
        log ""
        log ">>> MANUAL STEP: Add this to /etc/caddy/Caddyfile on the server:"
        log ""
        log "  $DOMAIN {"
        log "      reverse_proxy localhost:$DATASETTE_PORT"
        log "  }"
        log ""
        log "Then run: systemctl reload caddy"
        log ""
        log "Also add a DNS A record for $DOMAIN → $(ssh "$REMOTE_HOST" 'curl -s ifconfig.me')"
    fi
fi

# ── Pre-deploy disk-space check ──────────────────────────────────────────
#
# Peak disk usage during deploy = live + new_backup + .new (rsync) during
# transfer, then atomic `mv .new → live` frees the old live. The VPS tier
# keeps pre-deploy snapshots for up to 5 days (swept by a separate cron);
# local + B2 tiers are rotated count-based (last 3) via the post-deploy
# propagation hook below. See deploy/ROLLBACK.md for the 3-tier restore
# runbook and bestpractices/cron_inventory.md for sweep/monitor cron UUIDs.
#
# Minimum safe free-space threshold is **2× new-DB size** (covers new_backup +
# .new together). We abort below 2×, warn between 2× and 2.5×, proceed above.
log "=== Pre-deploy disk-space check ==="
NEW_DB_BYTES=$(stat -c%s "$DB")
NEW_DB_GB=$(awk -v b=$NEW_DB_BYTES 'BEGIN{printf "%.2f", b/1024/1024/1024}')
REQUIRED_MIN_GB=$(awk -v b=$NEW_DB_BYTES 'BEGIN{printf "%.2f", (b*2)/1024/1024/1024}')
REQUIRED_SAFE_GB=$(awk -v b=$NEW_DB_BYTES 'BEGIN{printf "%.2f", (b*2.5)/1024/1024/1024}')
if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Would check remote disk against 2× (${REQUIRED_MIN_GB}G) / 2.5× (${REQUIRED_SAFE_GB}G) of new DB (${NEW_DB_GB}G)"
else
    REMOTE_FREE_KB=$(ssh "$REMOTE_HOST" "df -P / | awk 'NR==2 {print \$4}'")
    REMOTE_FREE_GB=$(awk -v k=$REMOTE_FREE_KB 'BEGIN{printf "%.2f", k/1024/1024}')
    log "Local new DB: ${NEW_DB_GB} GB   VPS free: ${REMOTE_FREE_GB} GB"
    log "Thresholds — min safe (2×): ${REQUIRED_MIN_GB} GB   recommended (2.5×): ${REQUIRED_SAFE_GB} GB"
    # Compare free vs required minimum (in KB to avoid float math in bash)
    REQUIRED_MIN_KB=$(awk -v b=$NEW_DB_BYTES 'BEGIN{printf "%d", (b*2)/1024}')
    REQUIRED_SAFE_KB=$(awk -v b=$NEW_DB_BYTES 'BEGIN{printf "%d", (b*2.5)/1024}')
    if [[ $REMOTE_FREE_KB -lt $REQUIRED_MIN_KB ]]; then
        log "ERROR: insufficient disk headroom — need at least ${REQUIRED_MIN_GB} GB free but only ${REMOTE_FREE_GB} GB available"
        log "Free space on VPS by removing an old pre-deploy snapshot (only do this if"
        log "local + B2 tiers have a copy — see deploy/ROLLBACK.md Tier 2/3):"
        log "    ssh $REMOTE_HOST 'ls -lht $BACKUP_DIR/openregs-predeploy-*.db'"
        log "    ssh $REMOTE_HOST 'rm $BACKUP_DIR/openregs-predeploy-<oldest-timestamp>.db'"
        log "Aborting deploy."
        exit 2
    elif [[ $REMOTE_FREE_KB -lt $REQUIRED_SAFE_KB ]]; then
        log "WARNING: free space ${REMOTE_FREE_GB} GB is below recommended ${REQUIRED_SAFE_GB} GB (2.5× new DB)"
        log "Deploy will proceed but margin is tight. Consider removing an old backup to free ~$(awk -v b=$NEW_DB_BYTES 'BEGIN{printf "%.0f", b/1024/1024/1024}') GB."
    else
        log "OK: ${REMOTE_FREE_GB} GB free comfortably exceeds ${REQUIRED_SAFE_GB} GB safe threshold"
    fi
fi

# ── Backup existing databases on VPS ─────────────────────────────────────
log "=== Backing up existing databases on VPS ==="
BACKUP_DIR="$REMOTE_DIR/backups"

if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Would backup existing DB to $BACKUP_DIR/openregs-predeploy-<ts>.db"
    TIMESTAMP=$(date '+%Y%m%d_%H%M')
    BACKUP_FILE="openregs-predeploy-${TIMESTAMP}.db"
else
    ssh "$REMOTE_HOST" "mkdir -p $BACKUP_DIR"
    TIMESTAMP=$(date '+%Y%m%d_%H%M')
    BACKUP_FILE="openregs-predeploy-${TIMESTAMP}.db"
    # Snapshot of the live DB before we overwrite it. Swept on VPS by a
    # separate daily cron (`find -mtime +5 -delete`) so deploy.sh doesn't
    # have to own cleanup — see deploy/ROLLBACK.md Tier 1 and
    # bestpractices/cron_inventory.md.
    if ssh "$REMOTE_HOST" "test -f $REMOTE_DB"; then
        log "Backing up existing openregs.db → backups/$BACKUP_FILE"
        ssh "$REMOTE_HOST" "cp $REMOTE_DB $BACKUP_DIR/$BACKUP_FILE"
        log "VPS backup complete (cleanup owned by daily sweep cron, 5-day retention)"
    else
        log "No existing DB to backup"
        BACKUP_FILE=""
    fi
fi

# ── Upload databases ──────────────────────────────────────────────────────
#
# Upload strategy: write to "${REMOTE_DB}.new", then atomic mv on success.
#
# WHY: rsync's `--partial` option (implied by `-P`) means that on transfer
# interrupt, rsync renames its temp file to the destination filename. If the
# destination is the live DB, this CORRUPTS the live DB by overwriting it
# with a half-written file. (Discovered 2026-04-11 — see notes in
# bestpractices/best_practices.md and bestpractices/deploy_guide.md.)
#
# The `.new` filename ensures the live DB is never touched until the upload
# is complete and verified. `--partial-dir` keeps any partial files in a
# separate directory so they can resume safely without overwriting anything.
#
log "=== Uploading databases ==="

if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Would upload ${DB_SIZE_MB}MB to $REMOTE_HOST:${REMOTE_DB}.new, then atomic mv to $REMOTE_DB"
else
    log "Uploading $DB → $REMOTE_HOST:${REMOTE_DB}.new (${DB_SIZE_MB}MB) via rsync..."
    rsync -a --partial-dir=.rsync-partials --progress --timeout=600 "$DB" "$REMOTE_HOST:${REMOTE_DB}.new"
    log "Upload complete — atomically replacing live database..."
    ssh "$REMOTE_HOST" "mv ${REMOTE_DB}.new ${REMOTE_DB} && sudo chown datasette:datasette ${REMOTE_DB} && sudo chmod 664 ${REMOTE_DB}"
    log "openregs.db swap complete"
fi

if [[ -f "$APHIS_DB" ]]; then
    APHIS_SIZE_MB=$(du -m "$APHIS_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload APHIS DB (${APHIS_SIZE_MB}MB) to $REMOTE_HOST:${REMOTE_APHIS_DB}.new, then atomic mv"
    else
        log "Uploading $APHIS_DB → $REMOTE_HOST:${REMOTE_APHIS_DB}.new (${APHIS_SIZE_MB}MB)..."
        rsync -a --partial-dir=.rsync-partials --progress --timeout=600 "$APHIS_DB" "$REMOTE_HOST:${REMOTE_APHIS_DB}.new"
        ssh "$REMOTE_HOST" "mv ${REMOTE_APHIS_DB}.new ${REMOTE_APHIS_DB} && sudo chown datasette:datasette ${REMOTE_APHIS_DB} && sudo chmod 664 ${REMOTE_APHIS_DB}"
        log "APHIS swap complete"
    fi
else
    log "NOTE: APHIS database not found at $APHIS_DB, skipping"
fi

if [[ -f "$LOBBYING_DB" ]]; then
    LOBBYING_SIZE_MB=$(du -m "$LOBBYING_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload Lobbying DB (${LOBBYING_SIZE_MB}MB) to $REMOTE_HOST:${REMOTE_LOBBYING_DB}.new, then atomic mv"
    else
        log "Uploading $LOBBYING_DB → $REMOTE_HOST:${REMOTE_LOBBYING_DB}.new (${LOBBYING_SIZE_MB}MB) via rsync..."
        rsync -a --partial-dir=.rsync-partials --progress --timeout=600 "$LOBBYING_DB" "$REMOTE_HOST:${REMOTE_LOBBYING_DB}.new"
        ssh "$REMOTE_HOST" "mv ${REMOTE_LOBBYING_DB}.new ${REMOTE_LOBBYING_DB} && sudo chown datasette:datasette ${REMOTE_LOBBYING_DB} && sudo chmod 664 ${REMOTE_LOBBYING_DB}"
        log "Lobbying swap complete"
    fi
else
    log "NOTE: Lobbying database not found at $LOBBYING_DB, skipping"
fi

if [[ -f "$FARA_DB" ]]; then
    FARA_SIZE_MB=$(du -m "$FARA_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload FARA DB (${FARA_SIZE_MB}MB) to $REMOTE_HOST:${REMOTE_FARA_DB}.new, then atomic mv"
    else
        log "Uploading $FARA_DB → $REMOTE_HOST:${REMOTE_FARA_DB}.new (${FARA_SIZE_MB}MB)..."
        rsync -a --partial-dir=.rsync-partials --progress --timeout=600 "$FARA_DB" "$REMOTE_HOST:${REMOTE_FARA_DB}.new"
        ssh "$REMOTE_HOST" "mv ${REMOTE_FARA_DB}.new ${REMOTE_FARA_DB} && sudo chown datasette:datasette ${REMOTE_FARA_DB} && sudo chmod 664 ${REMOTE_FARA_DB}"
        log "FARA swap complete"
    fi
else
    log "NOTE: FARA database not found at $FARA_DB, skipping"
fi

# ── Upload templates ─────────────────────────────────────────────────────
TEMPLATES_DIR="$PROJECT_DIR/deploy/templates"
if [[ -d "$TEMPLATES_DIR" ]]; then
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload templates to $REMOTE_HOST:$REMOTE_DIR/templates"
    else
        log "Uploading templates..."
        ssh "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/templates"
        scp -rq "$TEMPLATES_DIR/"* "$REMOTE_HOST:$REMOTE_DIR/templates/"
        log "Templates uploaded"
    fi
fi

# ── Upload explore pages ────────────────────────────────────────────────
EXPLORE_DIR="$PROJECT_DIR/deploy/explore"
if [[ -d "$EXPLORE_DIR" ]]; then
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload explore pages to $REMOTE_HOST:$REMOTE_DIR/explore"
    else
        log "Uploading explore pages..."
        ssh "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/explore"
        scp -rq "$EXPLORE_DIR/"* "$REMOTE_HOST:$REMOTE_DIR/explore/"
        log "Explore pages uploaded"
    fi
fi

# ── Update metadata ──────────────────────────────────────────────────────
if [[ $DB_ONLY -eq 0 ]]; then
    log "=== Updating metadata ==="

    # Get counts from local DB for the description
    FR_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM federal_register" 2>/dev/null || echo "0")
    DOCKET_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM dockets" 2>/dev/null || echo "0")
    DOC_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM documents" 2>/dev/null || echo "0")
    COMMENT_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM comments" 2>/dev/null || echo "0")

    fmt_k() { awk "BEGIN {v=$1; if (v>=1000000) printf \"%.1fM\", v/1000000; else if (v>=1000) printf \"%.0fK\", v/1000; else printf \"%d\", v}"; }
    FR_FMT=$(echo | fmt_k "$FR_COUNT")
    DK_FMT=$(echo | fmt_k "$DOCKET_COUNT")
    DC_FMT=$(echo | fmt_k "$DOC_COUNT")
    CM_FMT=$(echo | fmt_k "$COMMENT_COUNT")

    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would update metadata: $FR_FMT FR docs, $DK_FMT dockets, $DC_FMT docs, $CM_FMT comments"
    else
        # Update description with live counts in the metadata file, then upload
        METADATA_FILE="$PROJECT_DIR/deploy/metadata.json"
        if [[ -f "$METADATA_FILE" ]]; then
            # Inject live counts into description
            DESCRIPTION="<p>Federal regulatory data: <strong>${FR_FMT} Federal Register documents</strong>, <strong>${DK_FMT} dockets</strong>, <strong>${DC_FMT} regulatory documents</strong>, and <strong>${CM_FMT} public comments</strong> with full-text search.</p><p>Built by <a href=\\\"https://datadawn.org\\\">DataDawn</a>.</p>"
            python3 -c "
import json, sys
with open('$METADATA_FILE') as f:
    meta = json.load(f)
meta['description_html'] = '''$DESCRIPTION'''
json.dump(meta, sys.stdout, indent=2)
" | ssh "$REMOTE_HOST" "cat > $REMOTE_DIR/metadata.json"
            log "Metadata uploaded (with canned queries)"
        else
            log "WARNING: metadata.json not found at $METADATA_FILE, using inline"
            ssh "$REMOTE_HOST" "cat > $REMOTE_DIR/metadata.json" <<METADATA_EOF
{
    "title": "OpenRegs — Federal Regulatory Data",
    "description_html": "<p>Federal regulatory data: <strong>${FR_FMT} Federal Register documents</strong>, <strong>${DK_FMT} dockets</strong>, <strong>${DC_FMT} regulatory documents</strong>, and <strong>${CM_FMT} public comments</strong>.</p><p>Data sourced from the Federal Register API and Regulations.gov.</p>",
    "license": "Public Domain (U.S. Government data)",
    "license_url": "https://www.regulations.gov/faq",
    "source": "Federal Register API & Regulations.gov API",
    "source_url": "https://www.federalregister.gov/developers/api/v1",
    "plugins": {
        "datasette-cors": {
            "allow_all": true
        }
    }
}
METADATA_EOF
            log "Metadata updated (inline fallback)"
        fi
    fi
fi

# ── Restart service ───────────────────────────────────────────────────────
log "=== Restarting Datasette ==="

if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Would restart openregs service"
else
    ssh "$REMOTE_HOST" 'sudo systemctl restart openregs'
    sleep 2
    if ssh "$REMOTE_HOST" 'sudo systemctl is-active openregs' >/dev/null 2>&1; then
        log "OpenRegs Datasette is running on port $DATASETTE_PORT"
    else
        log "WARNING: Service may not have started correctly"
        ssh "$REMOTE_HOST" 'sudo journalctl -u openregs --no-pager -n 10'
    fi
fi

# ── Post-deploy backup propagation (non-critical) ────────────────────────
# Runs after service restart succeeded. Failures here do not abort the
# script — the VPS snapshot still exists, and hc.io + daily freshness
# monitor alert if propagation silently breaks. See deploy/ROLLBACK.md.
if [[ $DRY_RUN -eq 0 ]] && [[ $DB_ONLY -eq 0 ]]; then
    if ! propagate_backup_to_local_and_b2 "${BACKUP_FILE:-}"; then
        log "WARNING: backup propagation exited non-zero (see above). Deploy continues."
    fi
fi

log ""
log "=== Deploy complete ==="
log "Database: ${DB_SIZE_MB}MB"
log "URL: https://$DOMAIN/"
