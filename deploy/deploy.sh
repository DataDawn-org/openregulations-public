#!/usr/bin/env bash
#
# deploy.sh — Deploy OpenRegs database to the production Datasette instance.
#
# This mirrors the DataDawn (990project) deployment pattern:
#   - Caddy reverse proxy on port 443
#   - Datasette serving SQLite on localhost:8002
#   - systemd service for process management
#
# Usage:
#   ./deploy.sh              # full deploy
#   ./deploy.sh --db-only    # just upload new database
#   ./deploy.sh --dry-run    # show what would happen
#
# (--setup was deprecated 2026-05-10 — for fresh-server provisioning, see
# bestpractices/disaster_recovery.md instead.)
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

# SSH keepalive + timeout options applied to every ssh/scp/rsync call below.
# Codified 2026-05-02 after the propagation hook's quick_check ssh hung
# indefinitely when the underlying TCP connection went silent during a long
# (~17 min) remote PRAGMA quick_check. Without ServerAliveInterval the local
# ssh client never gives up; deploy.sh blocks forever and a kill-9 is the
# only way out. With these:
#   - ConnectTimeout 15     fail fast if VPS is unreachable
#   - ServerAliveInterval 60 + Max 3 → 3 missed keepalives over 3 minutes
#                                       severs a silent connection
# See bestpractices/incident_log.md "2026-05-02 deploy hang" for context.
SSH_OPTS="-o ConnectTimeout=15 -o ServerAliveInterval=60 -o ServerAliveCountMax=3"

DRY_RUN=0
DB_ONLY=0

for arg in "$@"; do
    case $arg in
        --dry-run) DRY_RUN=1 ;;
        --db-only) DB_ONLY=1 ;;
        --setup)
            # Deprecated 2026-05-10 — kept as an error path so anyone using
            # muscle memory or stale docs gets a clear redirect instead of
            # silently falling through into a full deploy.
            echo "ERROR: --setup is no longer supported (the path was last current in March 2026)."
            echo "For fresh-server provisioning, follow bestpractices/disaster_recovery.md instead."
            echo "For incremental config edits, use the vps-config/ local mirror + caddy-reload."
            exit 1
            ;;
        --help)
            echo "Usage: $0 [--db-only|--dry-run]"
            exit 0
            ;;
    esac
done

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# ── Helper: skip rsync when local and remote are byte-identical ──────────
#
# rsync against a `${path}.new` destination filename has no existing file
# to baseline against, so it transfers the whole source even when the
# parallel `${path}` (the live filename) is identical. For our static
# auxiliary DBs (aphis, lobbying, fara — populated by separate pipelines,
# often unchanged across openregs builds) this means burning 14+ GB of
# bandwidth on a no-op transfer that can also push disk usage past the
# headroom guard if the target is large (regression observed 2026-05-12 —
# lobbying.db rsync failed with rsync code 11 partway through, despite
# local and VPS being byte-identical: size 15201697792, same mtime to
# the nanosecond).
#
# Compare cheap size+mtime fingerprints before each rsync; skip if identical.
# Returns 0 (no upload needed) on match, 1 (upload) otherwise. Falls back
# to "upload" on any stat failure — safe default.
#
# NOT applied to openregs.db because that DB is rebuilt every run, so its
# mtime always changes — the check would just add latency.
remote_matches_local() {
    local local_path="$1"
    local remote_path="$2"
    local local_sig remote_sig
    local_sig=$(stat -c '%s.%Y' "$local_path" 2>/dev/null) || return 1
    remote_sig=$(ssh $SSH_OPTS "$REMOTE_HOST" "stat -c '%s.%Y' '$remote_path' 2>/dev/null" || true)
    [[ -z "$remote_sig" ]] && return 1
    [[ "$local_sig" == "$remote_sig" ]] && return 0
    return 1
}

# ── Backup propagation to local + B2 (post-deploy, non-critical) ─────────
#
# Called after a successful deploy. Takes the pre-deploy snapshot that was
# written to the VPS (cp $REMOTE_DB → $BACKUP_DIR/$BACKUP_FILE) and:
#   1. Quick-checks it on VPS (refuse to propagate structural corruption)
#   1b. Writes a sidecar manifest.json on the VPS next to the backup (DR
#      drill F-001/F-003 follow-up — row counts + schema fingerprint so we
#      can tell what's in a backup without opening it; lets future DR runs
#      detect "wrong backup" or "schema drift since last run" cheaply).
#   2. Rsyncs both files down to $PROJECT_DIR/backups/
#   3. Rotates local to last 3 (via deploy/rotate_local_backups.py)
#   4. rclone copies both files to b2:someones-backup/openregs-weekly/
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
    local crit_path="$PROJECT_DIR/criticality.json"

    log "=== Backup propagation (local + B2) ==="

    # 1. Quick-check the VPS snapshot before replicating it.
    log "Quick-checking VPS snapshot (structural integrity)..."
    if ! ssh $SSH_OPTS "$REMOTE_HOST" "python3 -c \"
import sqlite3, sys
r = sqlite3.connect('file:$remote_backup?mode=ro', uri=True).execute('PRAGMA quick_check').fetchone()[0]
sys.exit(0 if r == 'ok' else 2)
\""; then
        log "  WARNING: quick_check FAILED on VPS snapshot — refusing to propagate corruption"
        return 2
    fi
    log "  quick_check: ok"

    # 1b. Write manifest sidecar on the VPS. Non-fatal: a missing manifest
    # doesn't invalidate the backup, just means this DR check is unavailable
    # for this run. We log a WARNING and keep propagating.
    if [[ -f "$crit_path" ]]; then
        local crit_tables_json
        crit_tables_json=$(python3 -c "import json,sys; print(json.dumps(list(json.load(open('$crit_path'))['tables'].keys())))")
        log "Writing manifest sidecar on VPS..."
        if ssh $SSH_OPTS "$REMOTE_HOST" \
                "REMOTE_BACKUP='$remote_backup' CRIT_TABLES='$crit_tables_json' SOURCE_SCRIPT=openregs/deploy/deploy.sh python3 -" \
                <<'PYEOF'
import sqlite3, json, os, hashlib, time, sys
backup = os.environ['REMOTE_BACKUP']
tables = json.loads(os.environ['CRIT_TABLES'])
conn = sqlite3.connect(f'file:{backup}?mode=ro', uri=True)
schema_rows = conn.execute(
    "SELECT type, name, sql FROM sqlite_schema "
    "WHERE sql IS NOT NULL ORDER BY type, name"
).fetchall()
schema_text = "\n".join(f"{t}\t{n}\t{s}" for t, n, s in schema_rows)
schema_fp = hashlib.sha256(schema_text.encode()).hexdigest()
# schema_objects enables table-level diff in validate_schema_fingerprint
# (audit M5 follow-up). Keys are "{type}:{name}" so 'table:returns' won't
# collide with a hypothetical 'index:returns'. Adds ~100-300 KB to the
# manifest — negligible.
schema_objects = {f"{t}:{n}": s for t, n, s in schema_rows}
existing = {r[0] for r in conn.execute(
    "SELECT name FROM sqlite_schema WHERE type='table'").fetchall()}
row_counts = {}
for t in tables:
    if t in existing:
        try:
            row_counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.Error:
            row_counts[t] = None
manifest = {
    "schema_version": 1,
    "backup_file": os.path.basename(backup),
    "manifest_timestamp_utc": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    "backup_mtime_utc": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(os.path.getmtime(backup))),
    "db_size_bytes": os.path.getsize(backup),
    "quick_check": "ok",
    "schema_fingerprint_sha256": schema_fp,
    "schema_objects": schema_objects,
    "sqlite_version": sqlite3.sqlite_version,
    "row_counts": row_counts,
    "row_count_tables_present": sum(1 for v in row_counts.values() if v is not None),
    "row_count_tables_missing": sum(1 for v in row_counts.values() if v is None),
    "source": os.environ.get('SOURCE_SCRIPT', ''),
}
out = backup + '.manifest.json'
tmp = out + '.tmp'
with open(tmp, 'w') as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
os.rename(tmp, out)
print(f"manifest: {out} ({manifest['row_count_tables_present']} tables, fp={schema_fp[:12]})", file=sys.stderr)
PYEOF
        then
            log "  manifest written"
        else
            log "  WARNING: manifest generation failed (non-fatal — backup itself is intact)"
        fi
    else
        log "  (criticality.json missing locally — skipping manifest sidecar)"
    fi

    # 2. Pull the VPS snapshot + manifest down to the local backups dir.
    mkdir -p "$local_backup_dir"
    log "Pulling $remote_backup → $local_backup_dir/"
    if ! rsync -a --partial-dir=.rsync-partials -e "ssh $SSH_OPTS" --timeout=600 \
            "$REMOTE_HOST:$remote_backup" "$local_backup_dir/$backup_file"; then
        log "  WARNING: rsync down failed; local tier NOT updated this run"
        return 3
    fi
    log "  local copy: $local_backup_dir/$backup_file"
    # Manifest pull is best-effort — pre-manifest backups won't have one.
    rsync -a -e "ssh $SSH_OPTS" --timeout=60 \
        "$REMOTE_HOST:${remote_backup}.manifest.json" \
        "$local_backup_dir/${backup_file}.manifest.json" 2>/dev/null \
        && log "  local manifest: $local_backup_dir/${backup_file}.manifest.json" \
        || log "  (no manifest on VPS to pull — pre-manifest backup or generation failed)"

    # 3. Rotate local to last 3 (both .db and .db.manifest.json families).
    if ! python3 "$helper" --dir "$local_backup_dir" --keep 3; then
        log "  WARNING: local rotation helper failed"
        return 4
    fi
    # Manifest sidecars: keep last 3 too. Exit 2 ("nothing matched") is fine
    # during the rollout period when no manifests exist yet.
    local rc=0
    python3 "$helper" --dir "$local_backup_dir" --keep 3 \
        --glob 'openregs-predeploy-*.db.manifest.json' || rc=$?
    if [[ $rc -ne 0 && $rc -ne 2 ]]; then
        log "  WARNING: manifest rotation helper failed (exit $rc)"
    fi

    # 4. Push to B2 (db + manifest together).
    log "Pushing to $b2_remote/"
    if ! rclone copy "$local_backup_dir/$backup_file" "$b2_remote/"; then
        log "  WARNING: B2 push failed"
        return 5
    fi
    if [[ -f "$local_backup_dir/${backup_file}.manifest.json" ]]; then
        rclone copy "$local_backup_dir/${backup_file}.manifest.json" "$b2_remote/" \
            || log "  WARNING: B2 manifest push failed (non-fatal)"
    fi

    # 5. Rotate B2 to last 3 (inline — sort lexically on filename works
    # because our format encodes timestamp in the name). Sweeps both .db
    # backups and any orphaned .manifest.json files in one pass.
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
            # Sweep the paired manifest if present.
            rclone delete "$b2_remote/${f}.manifest.json" 2>/dev/null || true
        done <<< "$b2_excess"
    else
        log "  B2 already at or under 3 backups, nothing to rotate"
    fi
    # Also sweep any manifest orphans (e.g. from rollout transitions).
    local b2_manifest_orphans
    b2_manifest_orphans=$(rclone lsf "$b2_remote/" --files-only 2>/dev/null \
                | grep -E '^openregs-predeploy-.*\.db\.manifest\.json$' \
                | sort | head -n -3 || true)
    if [[ -n "$b2_manifest_orphans" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            log "  rclone delete $b2_remote/$f (manifest orphan)"
            rclone delete "$b2_remote/$f" || log "    (delete failed for $f, continuing)"
        done <<< "$b2_manifest_orphans"
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

# BACKUP_DIR is referenced by the disk-check error message and the
# backup step further down. Define it once up top so the disk-check abort
# path can print a useful "rm this snapshot" hint without crashing on an
# unbound variable (regression observed 2026-05-02).
BACKUP_DIR="$REMOTE_DIR/backups"

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
    REMOTE_FREE_KB=$(ssh $SSH_OPTS "$REMOTE_HOST" "df -P / | awk 'NR==2 {print \$4}'")
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
# (BACKUP_DIR defined above the disk check so the abort-path hint works.)

if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Would backup existing DB to $BACKUP_DIR/openregs-predeploy-<ts>.db"
    TIMESTAMP=$(date '+%Y%m%d_%H%M')
    BACKUP_FILE="openregs-predeploy-${TIMESTAMP}.db"
else
    ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $BACKUP_DIR"
    TIMESTAMP=$(date '+%Y%m%d_%H%M')
    BACKUP_FILE="openregs-predeploy-${TIMESTAMP}.db"
    # Snapshot of the live DB before we overwrite it. Swept on VPS by a
    # separate daily cron (`find -mtime +5 -delete`) so deploy.sh doesn't
    # have to own cleanup — see deploy/ROLLBACK.md Tier 1 and
    # bestpractices/cron_inventory.md.
    if ssh $SSH_OPTS "$REMOTE_HOST" "test -f $REMOTE_DB"; then
        log "Backing up existing openregs.db → backups/$BACKUP_FILE"
        ssh $SSH_OPTS "$REMOTE_HOST" "cp $REMOTE_DB $BACKUP_DIR/$BACKUP_FILE"
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
    rsync -a --partial-dir=.rsync-partials -e "ssh $SSH_OPTS" --progress --timeout=600 "$DB" "$REMOTE_HOST:${REMOTE_DB}.new"
    log "Upload complete — atomically replacing live database..."
    ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_DB}.new ${REMOTE_DB} && sudo chown datasette:datasette ${REMOTE_DB} && sudo chmod 664 ${REMOTE_DB}"
    log "openregs.db swap complete"
fi

if [[ -f "$APHIS_DB" ]]; then
    APHIS_SIZE_MB=$(du -m "$APHIS_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload APHIS DB (${APHIS_SIZE_MB}MB) to $REMOTE_HOST:${REMOTE_APHIS_DB}.new, then atomic mv"
    elif remote_matches_local "$APHIS_DB" "$REMOTE_APHIS_DB"; then
        log "APHIS DB byte-identical to VPS (size+mtime match), skipping upload"
    else
        log "Uploading $APHIS_DB → $REMOTE_HOST:${REMOTE_APHIS_DB}.new (${APHIS_SIZE_MB}MB)..."
        rsync -a --partial-dir=.rsync-partials -e "ssh $SSH_OPTS" --progress --timeout=600 "$APHIS_DB" "$REMOTE_HOST:${REMOTE_APHIS_DB}.new"
        ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_APHIS_DB}.new ${REMOTE_APHIS_DB} && sudo chown datasette:datasette ${REMOTE_APHIS_DB} && sudo chmod 664 ${REMOTE_APHIS_DB}"
        log "APHIS swap complete"
    fi
else
    log "NOTE: APHIS database not found at $APHIS_DB, skipping"
fi

if [[ -f "$LOBBYING_DB" ]]; then
    LOBBYING_SIZE_MB=$(du -m "$LOBBYING_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload Lobbying DB (${LOBBYING_SIZE_MB}MB) to $REMOTE_HOST:${REMOTE_LOBBYING_DB}.new, then atomic mv"
    elif remote_matches_local "$LOBBYING_DB" "$REMOTE_LOBBYING_DB"; then
        log "Lobbying DB byte-identical to VPS (size+mtime match), skipping upload (saved ${LOBBYING_SIZE_MB}MB transfer)"
    else
        log "Uploading $LOBBYING_DB → $REMOTE_HOST:${REMOTE_LOBBYING_DB}.new (${LOBBYING_SIZE_MB}MB) via rsync..."
        rsync -a --partial-dir=.rsync-partials -e "ssh $SSH_OPTS" --progress --timeout=600 "$LOBBYING_DB" "$REMOTE_HOST:${REMOTE_LOBBYING_DB}.new"
        ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_LOBBYING_DB}.new ${REMOTE_LOBBYING_DB} && sudo chown datasette:datasette ${REMOTE_LOBBYING_DB} && sudo chmod 664 ${REMOTE_LOBBYING_DB}"
        log "Lobbying swap complete"
    fi
else
    log "NOTE: Lobbying database not found at $LOBBYING_DB, skipping"
fi

if [[ -f "$FARA_DB" ]]; then
    FARA_SIZE_MB=$(du -m "$FARA_DB" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload FARA DB (${FARA_SIZE_MB}MB) to $REMOTE_HOST:${REMOTE_FARA_DB}.new, then atomic mv"
    elif remote_matches_local "$FARA_DB" "$REMOTE_FARA_DB"; then
        log "FARA DB byte-identical to VPS (size+mtime match), skipping upload"
    else
        log "Uploading $FARA_DB → $REMOTE_HOST:${REMOTE_FARA_DB}.new (${FARA_SIZE_MB}MB)..."
        rsync -a --partial-dir=.rsync-partials -e "ssh $SSH_OPTS" --progress --timeout=600 "$FARA_DB" "$REMOTE_HOST:${REMOTE_FARA_DB}.new"
        ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_FARA_DB}.new ${REMOTE_FARA_DB} && sudo chown datasette:datasette ${REMOTE_FARA_DB} && sudo chmod 664 ${REMOTE_FARA_DB}"
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
        ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/templates"
        scp $SSH_OPTS -rq "$TEMPLATES_DIR/"* "$REMOTE_HOST:$REMOTE_DIR/templates/"
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
        ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/explore"
        scp $SSH_OPTS -rq "$EXPLORE_DIR/"* "$REMOTE_HOST:$REMOTE_DIR/explore/"
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
" | ssh $SSH_OPTS "$REMOTE_HOST" "cat > $REMOTE_DIR/metadata.json"
            log "Metadata uploaded (with canned queries)"
        else
            log "WARNING: metadata.json not found at $METADATA_FILE, using inline"
            ssh $SSH_OPTS "$REMOTE_HOST" "cat > $REMOTE_DIR/metadata.json" <<METADATA_EOF
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
    ssh $SSH_OPTS "$REMOTE_HOST" 'sudo systemctl restart openregs'
    sleep 2
    if ssh $SSH_OPTS "$REMOTE_HOST" 'sudo systemctl is-active openregs' >/dev/null 2>&1; then
        log "OpenRegs Datasette is running on port $DATASETTE_PORT"
    else
        log "WARNING: Service may not have started correctly"
        ssh $SSH_OPTS "$REMOTE_HOST" 'sudo journalctl -u openregs --no-pager -n 10'
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
