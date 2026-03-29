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
PROJECT_DIR="${OPENREGS_DIR:-$(dirname "$(dirname "$(readlink -f "$0")")")}"
DB="$PROJECT_DIR/openregs.db"
APHIS_DB="$PROJECT_DIR/aphis/db/aphis.db"
LOBBYING_DB="$PROJECT_DIR/lobbying.db"
FARA_DB="$PROJECT_DIR/fara.db"
REMOTE_HOST="${OPENREGS_REMOTE_HOST:?Set OPENREGS_REMOTE_HOST (e.g. user@your-server)}"
REMOTE_DIR="${OPENREGS_REMOTE_DIR:-/opt/openregs}"
REMOTE_DB="$REMOTE_DIR/openregs.db"
REMOTE_APHIS_DB="$REMOTE_DIR/aphis.db"
REMOTE_LOBBYING_DB="$REMOTE_DIR/lobbying.db"
REMOTE_FARA_DB="$REMOTE_DIR/fara.db"
DATASETTE_PORT="${OPENREGS_PORT:-8002}"
DOMAIN="${OPENREGS_DOMAIN:-regs.datadawn.org}"

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

# ── Backup existing databases on VPS ─────────────────────────────────────
log "=== Backing up existing databases on VPS ==="
BACKUP_DIR="$REMOTE_DIR/backups"

if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Would backup existing DB to $BACKUP_DIR/"
else
    ssh "$REMOTE_HOST" "mkdir -p $BACKUP_DIR"
    TIMESTAMP=$(date '+%Y%m%d_%H%M')
    # Backup main DB (only keep last 2 backups to save disk space)
    if ssh "$REMOTE_HOST" "test -f $REMOTE_DB"; then
        log "Backing up existing openregs.db → backups/openregs_${TIMESTAMP}.db"
        ssh "$REMOTE_HOST" "cp $REMOTE_DB $BACKUP_DIR/openregs_${TIMESTAMP}.db"
        # Clean old backups (keep last 2)
        ssh "$REMOTE_HOST" "ls -t $BACKUP_DIR/openregs_*.db 2>/dev/null | tail -n +3 | xargs -r rm"
        log "Backup complete (keeping last 2 versions)"
    else
        log "No existing DB to backup"
    fi
fi

# ── Upload databases ──────────────────────────────────────────────────────
log "=== Uploading databases ==="

if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Would upload ${DB_SIZE_MB}MB to $REMOTE_HOST:$REMOTE_DB"
else
    log "Uploading $DB → $REMOTE_HOST:$REMOTE_DB (${DB_SIZE_MB}MB) via rsync..."
    rsync -aP --inplace --timeout=600 "$DB" "$REMOTE_HOST:$REMOTE_DB"
    log "Upload complete"
fi

if [[ -f "$APHIS_DB" ]]; then
    APHIS_SIZE_MB=$(du -m "$APHIS_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload APHIS DB (${APHIS_SIZE_MB}MB) to $REMOTE_HOST:$REMOTE_APHIS_DB"
    else
        log "Uploading $APHIS_DB → $REMOTE_HOST:$REMOTE_APHIS_DB (${APHIS_SIZE_MB}MB)..."
        scp -q "$APHIS_DB" "$REMOTE_HOST:$REMOTE_APHIS_DB"
        log "APHIS upload complete"
    fi
else
    log "NOTE: APHIS database not found at $APHIS_DB, skipping"
fi

if [[ -f "$LOBBYING_DB" ]]; then
    LOBBYING_SIZE_MB=$(du -m "$LOBBYING_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload Lobbying DB (${LOBBYING_SIZE_MB}MB) to $REMOTE_HOST:$REMOTE_LOBBYING_DB"
    else
        log "Uploading $LOBBYING_DB → $REMOTE_HOST:$REMOTE_LOBBYING_DB (${LOBBYING_SIZE_MB}MB) via rsync..."
        rsync -aP --inplace --timeout=600 "$LOBBYING_DB" "$REMOTE_HOST:$REMOTE_LOBBYING_DB"
        log "Lobbying upload complete"
    fi
else
    log "NOTE: Lobbying database not found at $LOBBYING_DB, skipping"
fi

if [[ -f "$FARA_DB" ]]; then
    FARA_SIZE_MB=$(du -m "$FARA_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload FARA DB (${FARA_SIZE_MB}MB) to $REMOTE_HOST:$REMOTE_FARA_DB"
    else
        log "Uploading $FARA_DB → $REMOTE_HOST:$REMOTE_FARA_DB (${FARA_SIZE_MB}MB)..."
        scp -q "$FARA_DB" "$REMOTE_HOST:$REMOTE_FARA_DB"
        log "FARA upload complete"
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

log ""
log "=== Deploy complete ==="
log "Database: ${DB_SIZE_MB}MB"
log "URL: https://$DOMAIN/"
