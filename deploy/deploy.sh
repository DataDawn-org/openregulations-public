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
# (--setup was deprecated 2026-05-10 — for fresh-server provisioning,
# follow bestpractices/disaster_recovery.md instead.)
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

# Predeploy snapshots live on NVMe ($REMOTE_DIR/backups), keep-1 newest + the
# daily -mtime+5 sweep — the months-proven config. Snapshot-on-volume is PARKED
# (2026-06-03): SQLite .backup()'s buffered 50 GB write to the ~70 MB/s network
# volume intermittently wedged the deploy in writeback-throttle
# (folio_wait_bit_common, 45 min D-state, indefinite). Deliberate fix — NVMe
# snapshot, then ASYNC post-deploy `dd oflag=direct` to the volume, off the
# synchronous path where a slow write is invisible — is deferred (decisions_log
# §94). KEPT here on NVMe: gated/verified snapshot (.tmp→quick_check→mv),
# .corrupt quarantine, stale-.tmp sweep. Gate 2 FLIPPED 2026-06-03: backups now go to
# the current off-site bucket via a dedicated rclone remote (new key, post-2026-05-30
# exposure); the prior bucket retained read-only as legacy history (not deleted).
B2_RCLONE_REMOTE="${OPENREGS_B2_REMOTE:?Set OPENREGS_B2_REMOTE (rclone remote name for off-site backups)}"
B2_BUCKET="${OPENREGS_B2_BUCKET:?Set OPENREGS_B2_BUCKET (off-site backup bucket name)}"

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
PROPAGATE_ONLY=0

for arg in "$@"; do
    case $arg in
        --dry-run) DRY_RUN=1 ;;
        --db-only) DB_ONLY=1 ;;
        --propagate-only) PROPAGATE_ONLY=1 ;;
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
            echo "Usage: $0 [--db-only|--dry-run|--propagate-only]"
            echo "  --propagate-only  Re-propagate the newest existing VPS predeploy"
            echo "                    backup to local + B2 (+ §5a ledger). No build/"
            echo "                    upload/swap. Closes an off-site propagation gap;"
            echo "                    also the §5b 'heal-the-refusal' re-run path."
            exit 0
            ;;
    esac
done

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Log a fatal message and exit non-zero. It was CALLED at the pre-swap quick_check guard
# (the #19 gate, 2026-05-25) but never DEFINED — latent since: a pre-swap-quick_check
# failure would have printed "die: command not found" / exit 127 instead of the intended
# message (the deploy would still abort under `set -e`, but unclearly). Found + defined
# 2026-06-01 while adding upload_db_with_retry (which also relies on it).
die() {
    log "FATAL: $*"
    exit 1
}

# Upload a DB to its `.new` destination with up to 3 retries. --partial-dir resumes
# (not restarts) each attempt, so a transient broken pipe / socket IO doesn't waste the
# whole transfer. Added 2026-06-01, mirroring 990 update.sh after a single broken pipe
# killed a clean build's deploy there (incident_log 2026-06-01); openregs.db (~48GB) is
# the most blip-exposed upload on the box. Live DB is never at risk — uploads target a
# `.new` path and the atomic mv runs only on success; die() after 3 fails leaves live
# untouched. Args: $1=local src, $2=remote dst (.new path), $3=label for logs.
upload_db_with_retry() {
    local src="$1" dst="$2" label="$3" ok=0 attempt
    for attempt in 1 2 3; do
        if rsync -a --partial-dir=.rsync-partials -e "ssh $SSH_OPTS" --progress --timeout=600 "$src" "$dst"; then
            ok=1; break
        fi
        log "  WARNING: $label upload attempt $attempt/3 failed (transient transport?); retrying (resumes via --partial-dir)..."
        sleep 10
    done
    [[ $ok -eq 1 ]] || die "rsync upload of $label → $dst failed after 3 attempts — live DB untouched (.new pattern)."
}

# ── Helpers: skip rsync when local and remote are byte-identical ─────────
#
# rsync against a `${path}.new` destination filename has no existing file
# to baseline against, so it transfers the whole source even when the
# parallel `${path}` (the live filename) is identical. For our static
# auxiliary DBs (aphis, lobbying, fara — populated by separate pipelines,
# often unchanged across openregs builds) this means burning 14+ GB of
# bandwidth on a no-op transfer that can also push disk usage past the
# headroom guard.
#
# Strategy (cheap to expensive):
#   1. Compare sizes — if different, content differs, upload.
#   2. Sizes match: compare mtime fast-path — if equal, skip with confidence.
#   3. Sizes match, mtimes drift: compute a sample hash (first + last 1 MB)
#      and compare. Catches the case where a separate pipeline rebuilt the
#      DB to byte-identical content (mtime drifted, content unchanged).
#
# Why the sample hash exists: 2026-05-23 deploy failed mid-rsync of
# lobbying.db because the previous mtime+size-only check returned "differs"
# under genuine mtime drift (size identical, mtime 57 sec apart from prior
# pipeline re-run). The unnecessary 14 GB upload exhausted VPS headroom.
# A 2 MB sample hash is ~50 ms vs ~14 min for an unnecessary rsync.
#
# Reliability for SQLite specifically: the first 1 MB includes the SQLite
# header (page size, schema cookie, file change counter — touched on every
# write). The last 1 MB usually includes recent journal/freelist pages. A
# real content change that leaves both windows untouched on a multi-GB
# SQLite DB is vanishingly rare.
#
# Returns 0 (no upload needed) on match, 1 (upload) otherwise. Falls back
# to "upload" on any stat/ssh failure — safe default.
#
# NOT applied to openregs.db because that DB is rebuilt every run, so its
# mtime always changes — the check would just add latency.
sample_hash_local() {
    python3 - "$1" <<'PYEOF'
import hashlib, os, sys
path = sys.argv[1]
size = os.path.getsize(path)
h = hashlib.sha256()
with open(path, 'rb') as f:
    h.update(f.read(1024 * 1024))  # first 1 MB
    if size > 2 * 1024 * 1024:
        f.seek(size - 1024 * 1024)
        h.update(f.read(1024 * 1024))  # last 1 MB
print(h.hexdigest())
PYEOF
}

sample_hash_remote() {
    ssh $SSH_OPTS "$REMOTE_HOST" "REMOTE_PATH='$1' python3 -" <<'PYEOF'
import hashlib, os, sys
path = os.environ['REMOTE_PATH']
try:
    size = os.path.getsize(path)
except OSError:
    sys.exit(1)
h = hashlib.sha256()
with open(path, 'rb') as f:
    h.update(f.read(1024 * 1024))
    if size > 2 * 1024 * 1024:
        f.seek(size - 1024 * 1024)
        h.update(f.read(1024 * 1024))
print(h.hexdigest())
PYEOF
}

remote_matches_local() {
    local local_path="$1"
    local remote_path="$2"
    local local_size remote_size local_mtime remote_mtime
    local_size=$(stat -c%s "$local_path" 2>/dev/null) || return 1
    remote_size=$(ssh $SSH_OPTS "$REMOTE_HOST" "stat -c%s '$remote_path' 2>/dev/null" || true)
    [[ -z "$remote_size" ]] && return 1
    [[ "$local_size" != "$remote_size" ]] && return 1

    # Sizes match: try mtime fast-path (no hash needed in the happy case).
    local_mtime=$(stat -c%Y "$local_path" 2>/dev/null) || return 1
    remote_mtime=$(ssh $SSH_OPTS "$REMOTE_HOST" "stat -c%Y '$remote_path' 2>/dev/null" || true)
    [[ -n "$remote_mtime" && "$local_mtime" == "$remote_mtime" ]] && return 0

    # Size matches but mtime drifted. Fall back to sample hash to confirm
    # whether contents actually differ. See 2026-05-23 incident in the
    # header comment above this function.
    log "  mtime drift on $(basename "$local_path") (size match); computing sample hash..."
    local local_hash remote_hash
    local_hash=$(sample_hash_local "$local_path" 2>/dev/null) || return 1
    remote_hash=$(sample_hash_remote "$remote_path" 2>/dev/null) || return 1
    [[ "$local_hash" == "$remote_hash" ]] && return 0
    return 1
}

# ── Backup propagation to local + B2 (post-deploy, non-critical) ─────────
#
# Called after a successful deploy. Takes the pre-deploy snapshot that was
# written to the VPS (.backup() $REMOTE_DB → $BACKUP_DIR/$BACKUP_FILE) and:
#   1. Quick-checks it on VPS (refuse to propagate structural corruption)
#   1b. Writes a sidecar manifest.json on the VPS next to the backup (DR
#      drill F-001/F-003 follow-up — row counts + schema fingerprint so we
#      can tell what's in a backup without opening it; lets future DR runs
#      detect "wrong backup" or "schema drift since last run" cheaply).
#   2. Rsyncs both files down to $PROJECT_DIR/backups/
#   3. Rotates local to last 3 (via deploy/rotate_local_backups.py)
#   4. rclone copies both files to $B2_RCLONE_REMOTE:$B2_BUCKET/openregs-weekly/
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
ledger_record_fail() {
    # #146(b) (decisions_log §103): a FAILED/REFUSED propagation must leave a
    # durable trace, not silence — the success-only §5a ledger was blind to
    # failure-by-absence (the 2026-06-13 class). Writes a status=FAILED row so the
    # §4 daily backup-health check (section_ledger) REDs on it and a human sees the
    # last propagation did NOT fully land off-site. The write itself is non-fatal.
    local ledger_dir="$1" snap="$2" b2r="$3" stage="$4" code="$5" local_ok="$6" b2_ok="$7"
    LEDGER="$ledger_dir/propagation_ledger.jsonl" SNAP="$snap" B2R="$b2r" \
        STAGE="$stage" CODE="$code" LOCAL_OK="$local_ok" B2_OK="$b2_ok" python3 - <<'PYEOF' \
        || log "  WARNING: failure-row ledger write failed (non-fatal)"
import json, os, time
entry = {
    "ts": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    "db": "openregs",
    "snapshot_file": os.environ['SNAP'],
    "b2_remote": os.environ['B2R'],
    "status": "FAILED",
    "stage": os.environ['STAGE'],
    "return_code": int(os.environ['CODE']),
    "local_ok": os.environ['LOCAL_OK'] == '1',
    "b2_ok": os.environ['B2_OK'] == '1',
}
with open(os.environ['LEDGER'], 'a') as f:
    f.write(json.dumps(entry, sort_keys=True) + "\n")
PYEOF
    log "  §5a FAILURE row recorded (stage=$stage rc=$code) — the daily backup-health check will RED on it"
}

propagate_backup_to_local_and_b2() {
    local backup_file="$1"
    if [[ -z "$backup_file" ]]; then
        log "  (no backup was made — skipping propagation)"
        return 0
    fi
    local remote_backup="$BACKUP_DIR/$backup_file"
    local local_backup_dir="$PROJECT_DIR/backups"
    local b2_remote="${B2_RCLONE_REMOTE}:${B2_BUCKET}/openregs-weekly"
    local helper="$PROJECT_DIR/deploy/rotate_local_backups.py"
    local crit_path="$PROJECT_DIR/criticality.json"

    log "=== Backup propagation (local + B2) ==="

    # 1. Quick-check the VPS snapshot before replicating it. SECONDARY tripwire:
    # the snapshot was already quick_check-gated at birth (see snapshot block), so
    # a failure here means post-birth/post-swap corruption (bit-rot, fs fault). On
    # fail: quarantine to .corrupt (de-heads it — the *.db rollback glob no longer
    # matches it, so ls -t falls back to gen-1) and signal a non-zero exit so the
    # cron's hc.io fires. Clean heredoc w/ try/except (matches the birth check) so a
    # truly-unreadable file exits cleanly, not via traceback.
    log "Quick-checking VPS snapshot (structural integrity)..."
    if ! ssh $SSH_OPTS "$REMOTE_HOST" "python3 - '$remote_backup'" <<'PYQC'
import sqlite3, sys
try:
    r = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True).execute("PRAGMA quick_check").fetchone()[0]
except Exception as e:
    print("quick_check error:", e); sys.exit(1)
sys.exit(0 if r == "ok" else 1)
PYQC
    then
        log "  WARNING: quick_check FAILED post-birth — quarantining to .corrupt and de-heading"
        ssh $SSH_OPTS "$REMOTE_HOST" "mv '$remote_backup' '$remote_backup.corrupt'" || true
        SNAPSHOT_CORRUPT=1   # forces non-zero exit at end (hc.io alerts); NOT local
        ledger_record_fail "$local_backup_dir" "$backup_file" "$b2_remote" "vps_quick_check" 2 0 0
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
# (M5 — 2026-05-15). Keys are "{type}:{name}" so 'table:returns' won't
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
# Content markers (2026-05-30): the filename carries the DEPLOY date but the
# content of the PRIOR build (predeploy snapshot = the build being overwritten).
# These fields make the actual content generation machine-checkable so reading
# the backup dir by filename can't mislead — the off-by-one that burned a read
# on 2026-05-30. federal_register max pub date = a weekly-cadence content marker;
# presidential_documents_last_refresh surfaces FR-carry-forward staleness (#2).
content_markers = {}
try:
    content_markers["federal_register_max_pub_date"] = conn.execute(
        "SELECT MAX(publication_date) FROM federal_register").fetchone()[0]
except sqlite3.Error:
    content_markers["federal_register_max_pub_date"] = None
try:
    _r = conn.execute("SELECT value FROM build_metadata "
                      "WHERE key='presidential_documents_last_refresh'").fetchone()
    content_markers["presidential_documents_last_refresh"] = _r[0] if _r else None
except sqlite3.Error:
    content_markers["presidential_documents_last_refresh"] = None
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
    "content_markers": content_markers,
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
    # INVARIANT — keep this a VPS pull. check_backup_freshness.py --cross-check-dir
    # asserts off-site (B2) >= this local Tier-2 copy to catch a failed deploy push;
    # if you ever repoint this to source from B2, that cross-check goes circular
    # (both sides stale together on a failed push -> false GREEN). Source must != B2.
    mkdir -p "$local_backup_dir"
    log "Pulling $remote_backup → $local_backup_dir/"
    if ! rsync -a --partial-dir=.rsync-partials -e "ssh $SSH_OPTS" --timeout=600 \
            "$REMOTE_HOST:$remote_backup" "$local_backup_dir/$backup_file"; then
        log "  WARNING: rsync down failed; local tier NOT updated this run"
        ledger_record_fail "$local_backup_dir" "$backup_file" "$b2_remote" "rsync_local" 3 0 0
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
        ledger_record_fail "$local_backup_dir" "$backup_file" "$b2_remote" "rotate_local" 4 1 0
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
        ledger_record_fail "$local_backup_dir" "$backup_file" "$b2_remote" "b2_push" 5 1 0
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

    # 6. Propagation ledger (§5a, 2026-05-30). Append a durable record that THIS
    # generation was hashed and propagated to BOTH local and B2. This is the
    # spine of the §4 daily parity check: it proves transfer + retention
    # integrity (is what we shipped still safely off-site, uncorrupted),
    # orthogonal to the manifest's build-content fingerprint. Append-only JSONL,
    # workstation-local, single-writer (deploys are serialized by the §6.2
    # deploy-lock). FULLY NON-FATAL: the deploy + propagation already succeeded
    # above; a ledger-write failure must never fail a deploy — it is
    # observability, not a gate.
    #
    # SCOPE NOTE: this records the SUCCESS path. The REFUSED / FAILED propagation
    # paths (return 2/3/4/5 above) now ALSO write a row — a status=FAILED entry via
    # ledger_record_fail() — so a failed propagation is no longer invisible by
    # absence (#146(b), decisions_log §103; closed the 2026-05-30 follow-up). The §4
    # daily section_ledger REDs on a FAILED latest row or a missing-row currency gap.
    local ledger="$local_backup_dir/propagation_ledger.jsonl"
    log "Writing propagation ledger entry → $ledger"
    LEDGER="$ledger" LOCAL_COPY="$local_backup_dir/$backup_file" \
        MANIFEST="$local_backup_dir/${backup_file}.manifest.json" \
        SNAP_NAME="$backup_file" B2_REMOTE="$b2_remote" python3 - <<'PYEOF' \
        || log "  WARNING: ledger write failed (non-fatal — backup itself is safe)"
import json, os, hashlib, time, sys
ledger = os.environ['LEDGER']
local_copy = os.environ['LOCAL_COPY']
# Full-file sha256 of the local copy (byte-identical to the B2 copy rclone just
# pushed) — recorded NOW so §4 can re-hash the off-site copy later and assert it
# still matches, catching truncation / bit-rot / interrupted transfer.
h = hashlib.sha256()
with open(local_copy, 'rb') as f:
    for chunk in iter(lambda: f.read(8 << 20), b''):
        h.update(chunk)
sha = h.hexdigest()
# Generation identity comes from the manifest's content_markers + schema fp
# (#6). content_markers is what §4 generation-parity compares live-vs-offsite.
fp, markers = None, None
try:
    with open(os.environ['MANIFEST']) as mf:
        m = json.load(mf)
    fp = m.get('schema_fingerprint_sha256')
    markers = m.get('content_markers')
except (OSError, ValueError):
    pass
entry = {
    "ts": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    "db": "openregs",
    "snapshot_file": os.environ['SNAP_NAME'],
    "sha256": sha,
    "size_bytes": os.path.getsize(local_copy),
    "build_fingerprint": fp,
    "content_markers": markers,
    "b2_remote": os.environ['B2_REMOTE'],
    "local_ok": True,
    "b2_ok": True,
}
with open(ledger, 'a') as f:
    f.write(json.dumps(entry, sort_keys=True) + "\n")
print(f"ledger: {entry['snapshot_file']} sha={sha[:12]} fp={(fp or '')[:12]}",
      file=sys.stderr)
PYEOF

    log "Backup propagation complete"
    return 0
}

# ── N2 snapshot sidecar backup (post-deploy, non-critical) ────────────────
#
# `openregs/state/dd_id_snapshot.db` is the N2 reconciliation baseline that
# validate_dd_id_stability.py reads on every Saturday rebuild. It lives
# workstation-local (~500 MB), is NOT deployed to the VPS, and is the
# single point of failure for the N2 gate: corruption/loss means the next
# rebuild halts with "no snapshot DB" and recovery requires either
# restoring the sidecar or running `--force-rebaseline` against fresh
# scratch (the bootstrap guard added 2026-05-21 refuses silent re-baseline
# from stale scratch — louder halt, but the failure mode without a backup
# is harder to recover from).
#
# This function (added 2026-05-21 per `decisions_log.md` §60 hygiene
# follow-up) propagates the workstation sidecar to the local backups dir
# and B2 on every successful non-dry-run deploy. Same rotate-3 cadence as
# the main openregs.db backup family, separate B2 prefix to keep listings
# clean.
#
# Failures here do NOT abort the deploy.
backup_n2_sidecar_to_local_and_b2() {
    local sidecar="$PROJECT_DIR/state/dd_id_snapshot.db"
    local local_backup_dir="$PROJECT_DIR/backups"
    local b2_remote="${B2_RCLONE_REMOTE}:${B2_BUCKET}/openregs-dd-id-snapshot"
    local helper="$PROJECT_DIR/deploy/rotate_local_backups.py"

    if [[ ! -f "$sidecar" ]]; then
        log "  (no N2 sidecar at $sidecar — skipping snapshot backup)"
        return 0
    fi

    log "=== N2 snapshot sidecar backup (local + B2) ==="
    local ts
    ts=$(date -u +'%Y%m%d-%H%M%S')
    local snap_name="dd_id_snapshot-${ts}.db"
    local local_path="$local_backup_dir/$snap_name"

    mkdir -p "$local_backup_dir"
    log "Copying $sidecar → $local_path"
    if ! cp "$sidecar" "$local_path"; then
        log "  WARNING: local cp of sidecar failed"
        return 2
    fi

    # Quick-check the copy (same rationale as the main propagation function).
    if ! python3 -c "
import sqlite3, sys
r = sqlite3.connect('file:$local_path?mode=ro', uri=True).execute('PRAGMA quick_check').fetchone()[0]
sys.exit(0 if r == 'ok' else 2)
"; then
        log "  WARNING: quick_check FAILED on snapshot copy — removing and skipping B2 push"
        rm -f "$local_path"
        return 3
    fi
    log "  quick_check: ok"

    # Rotate local snapshot copies to last 3.
    local rc=0
    python3 "$helper" --dir "$local_backup_dir" --keep 3 \
        --glob 'dd_id_snapshot-*.db' || rc=$?
    if [[ $rc -ne 0 && $rc -ne 2 ]]; then
        log "  WARNING: local snapshot rotation helper failed (exit $rc)"
    fi

    # Push to B2.
    log "Pushing $snap_name → $b2_remote/"
    if ! rclone copy "$local_path" "$b2_remote/"; then
        log "  WARNING: B2 snapshot push failed"
        return 5
    fi

    # Rotate B2 to last 3.
    local b2_excess
    b2_excess=$(rclone lsf "$b2_remote/" --files-only 2>/dev/null \
                | grep -E '^dd_id_snapshot-.*\.db$' \
                | sort | head -n -3 || true)
    if [[ -n "$b2_excess" ]]; then
        while IFS= read -r f; do
            [[ -z "$f" ]] && continue
            log "  rclone delete $b2_remote/$f"
            rclone delete "$b2_remote/$f" || log "    (delete failed for $f, continuing)"
        done <<< "$b2_excess"
    else
        log "  B2 already at or under 3 sidecar copies"
    fi

    log "N2 sidecar backup complete"
    return 0
}

# ── --propagate-only: re-propagate an existing VPS backup (no build/swap) ──
#
# Added 2026-05-31. Re-runs propagate_backup_to_local_and_b2() against the
# newest existing VPS predeploy backup — the safe way to CLOSE an off-site
# propagation gap (a generation stranded VPS-only because a manual deploy
# bypassed the workstation->B2 push) WITHOUT a full rebuild/deploy. Reuses the
# tested propagation function (quick_check gate, manifest, rotate-3, §5a ledger)
# so there is no hand-rolled divergence. Also the §5b "re-run after a refusal"
# mechanism. The function's own quick_check refuses to propagate a torn snapshot.
# Early-exits before any preflight/build/upload/swap.
if [[ $PROPAGATE_ONLY -eq 1 ]]; then
    BACKUP_DIR="$REMOTE_DIR/backups"
    log "=== --propagate-only: locating newest VPS backup in $BACKUP_DIR ==="
    # awk NR==1 (not head -1) so the remote ls is fully drained — avoids the
    # pipefail+SIGPIPE early-exit class (feedback_pipefail_grep_early_exit).
    PROP_PATH=$(ssh $SSH_OPTS "$REMOTE_HOST" \
        "ls -1t '$BACKUP_DIR'/openregs-predeploy-*.db 2>/dev/null | awk 'NR==1'")
    PROP_FILE=$(basename "$PROP_PATH" 2>/dev/null || true)
    if [[ -z "$PROP_FILE" ]]; then
        log "ERROR: no openregs-predeploy-*.db found on the VPS to propagate."
        exit 1
    fi
    log "  newest VPS backup: $PROP_FILE"
    if [[ $DRY_RUN -eq 1 ]]; then
        log "  [dry-run] would propagate $PROP_FILE to local + B2 (+ ledger), then exit."
        exit 0
    fi
    if propagate_backup_to_local_and_b2 "$PROP_FILE"; then
        log "--propagate-only: done."
        exit 0
    else
        rc=$?
        log "--propagate-only: propagation FAILED (rc=$rc) — see warnings above."
        exit "$rc"
    fi
fi

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

# ── Pre-flight: decide which DBs to upload ────────────────────────────────
#
# Auxiliary DBs (aphis, lobbying, fara) are produced by independent pipelines
# and frequently unchanged between Saturday openregs rebuilds. We decide
# upfront which ones need re-upload so the disk-headroom check below can
# size against the actual transfer total (the original check sized against
# openregs alone and ignored auxiliary `.new` files — that's how the
# 2026-05-23 deploy ran out of VPS disk mid-rsync of lobbying.db). The same
# remote_matches_local check runs again JIT in the upload blocks further
# down; double-call cost is ~3 ssh round-trips, negligible vs rsync.
log "=== Pre-flight: deciding which DBs to upload ==="
NEW_DB_BYTES=$(stat -c%s "$DB")
TRANSFER_BYTES=$NEW_DB_BYTES  # openregs.db is always uploaded (rebuilt every run)
log "  openregs.db (${DB_SIZE_MB} MB): will upload (rebuilt every run)"
for entry in "APHIS|$APHIS_DB|$REMOTE_APHIS_DB" \
             "Lobbying|$LOBBYING_DB|$REMOTE_LOBBYING_DB" \
             "FARA|$FARA_DB|$REMOTE_FARA_DB"; do
    IFS='|' read -r aux_label aux_local aux_remote <<< "$entry"
    if [[ ! -f "$aux_local" ]]; then
        log "  $aux_label: not found locally ($aux_local), nothing to upload"
        continue
    fi
    aux_mb=$(du -m "$aux_local" | cut -f1)
    if [[ $DRY_RUN -eq 1 ]]; then
        log "  $aux_label (${aux_mb} MB): [DRY-RUN] assuming upload"
        TRANSFER_BYTES=$((TRANSFER_BYTES + $(stat -c%s "$aux_local")))
    elif remote_matches_local "$aux_local" "$aux_remote"; then
        log "  $aux_label (${aux_mb} MB): byte-identical to VPS, will skip"
    else
        log "  $aux_label (${aux_mb} MB): differs from VPS, will upload"
        TRANSFER_BYTES=$((TRANSFER_BYTES + $(stat -c%s "$aux_local")))
    fi
done
TRANSFER_GB=$(awk -v b=$TRANSFER_BYTES 'BEGIN{printf "%.2f", b/1024/1024/1024}')
log "  Total transfer: ${TRANSFER_GB} GB"

# ── Pre-deploy disk-space check ──────────────────────────────────────────
#
# Snapshot is born on NVMe (.backup() → $BACKUP_DIR) and the .new upload also
# lands on NVMe before the atomic swap, so peak free we need on / =
#   snapshot_overhead   (~BACKUP_BYTES, one openregs.db snapshot)
# + transfer_overhead   (sum of .new files, TRANSFER_BYTES from preflight)
# After the atomic `mv .new → live` the old live frees; the snapshot is trimmed
# to keep-1 post-promote in the backup step + swept by the daily -mtime+5 cron.
# Abort below min; warn between min and +10% margin.
log "=== Pre-deploy disk-space check ==="
if [[ $DRY_RUN -eq 1 ]]; then
    BACKUP_BYTES=$NEW_DB_BYTES  # dry-run estimate (no ssh)
    log "[DRY-RUN] Would size against snapshot (~${DB_SIZE_MB} MB) + transfer (${TRANSFER_GB} GB)"
else
    BACKUP_BYTES=$(ssh $SSH_OPTS "$REMOTE_HOST" "stat -c%s '$REMOTE_DB' 2>/dev/null" || echo 0)
fi
NEEDED_MIN_BYTES=$((BACKUP_BYTES + TRANSFER_BYTES))
NEEDED_SAFE_BYTES=$((NEEDED_MIN_BYTES + NEEDED_MIN_BYTES / 10))
BACKUP_GB=$(awk -v b=$BACKUP_BYTES 'BEGIN{printf "%.2f", b/1024/1024/1024}')
NEEDED_MIN_GB=$(awk -v b=$NEEDED_MIN_BYTES 'BEGIN{printf "%.2f", b/1024/1024/1024}')
NEEDED_SAFE_GB=$(awk -v b=$NEEDED_SAFE_BYTES 'BEGIN{printf "%.2f", b/1024/1024/1024}')
if [[ $DRY_RUN -eq 1 ]]; then
    log "[DRY-RUN] Required: ≥${NEEDED_MIN_GB} GB (snapshot ${BACKUP_GB} GB + transfer ${TRANSFER_GB} GB); recommended ${NEEDED_SAFE_GB} GB"
else
    REMOTE_FREE_KB=$(ssh $SSH_OPTS "$REMOTE_HOST" "df -P / | awk 'NR==2 {print \$4}'")
    REMOTE_FREE_GB=$(awk -v k=$REMOTE_FREE_KB 'BEGIN{printf "%.2f", k/1024/1024}')
    log "Required: ≥${NEEDED_MIN_GB} GB (snapshot ${BACKUP_GB} GB + transfer ${TRANSFER_GB} GB)"
    log "Recommended: ${NEEDED_SAFE_GB} GB (with 10% margin); VPS free: ${REMOTE_FREE_GB} GB"
    NEEDED_MIN_KB=$((NEEDED_MIN_BYTES / 1024))
    NEEDED_SAFE_KB=$((NEEDED_SAFE_BYTES / 1024))
    if [[ $REMOTE_FREE_KB -lt $NEEDED_MIN_KB ]]; then
        SHORTFALL_GB=$(awk -v n=$NEEDED_MIN_BYTES -v f=$((REMOTE_FREE_KB * 1024)) 'BEGIN{printf "%.2f", (n-f)/1024/1024/1024}')
        log "ERROR: insufficient NVMe headroom (short ${SHORTFALL_GB} GB)"
        log "Free space by removing an old pre-deploy snapshot (local + B2 hold copies — ROLLBACK.md Tier 2/3):"
        log "    ssh $REMOTE_HOST 'ls -lht $BACKUP_DIR/openregs-predeploy-*.db'"
        log "    ssh $REMOTE_HOST 'rm $BACKUP_DIR/openregs-predeploy-<oldest>.db'"
        die "Aborting deploy — insufficient NVMe headroom."
    elif [[ $REMOTE_FREE_KB -lt $NEEDED_SAFE_KB ]]; then
        log "WARNING: free space ${REMOTE_FREE_GB} GB below recommended ${NEEDED_SAFE_GB} GB (need +10% margin)"
    else
        log "OK: ${REMOTE_FREE_GB} GB free exceeds ${NEEDED_SAFE_GB} GB safe threshold"
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
    if ssh $SSH_OPTS "$REMOTE_HOST" "test -f $REMOTE_DB"; then
        # Block-start cleanup: orphan *.db.tmp from a hard-killed prior .backup()
        # (OOM/dropped-ssh/power) matches neither the rollback nor keep-1 glob, so it
        # would accrue ~48 GB/kill — remove all. Also bound any .corrupt quarantine
        # files to newest-1 (one forensic sample; the exit-6 page already alerted a
        # human). awk-drained per feedback_pipefail_grep_early_exit.
        ssh $SSH_OPTS "$REMOTE_HOST" "rm -f $BACKUP_DIR/openregs-predeploy-*.db.tmp"
        ssh $SSH_OPTS "$REMOTE_HOST" "DIR='$BACKUP_DIR' bash -s" <<'PYCORRUPT'
cd "$DIR" || exit 0
ls -1t openregs-predeploy-*.db.corrupt 2>/dev/null | awk 'NR>1' | while IFS= read -r f; do
    echo "  rm old quarantine $f"; rm -f -- "$f"
done
PYCORRUPT

        log "Snapshot → backups/$BACKUP_FILE.tmp (WAL-safe .backup() API, NVMe)"
        # WAL-safe SQLite online-backup API — NOT cp (cp of a live WAL DB races
        # checkpoints → torn copy; hit 2026-05-24). .backup() → .tmp, then the mv to
        # the final (rollback-head) name is GATED on (1) .backup() success and
        # (2) quick_check — verify BEFORE head-promotion. On .backup() error: rm .tmp; die.
        if ! ssh $SSH_OPTS "$REMOTE_HOST" "python3 - '$REMOTE_DB' '$BACKUP_DIR/$BACKUP_FILE.tmp'" <<'PYBACKUP'
import sqlite3, sys
src = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
dst = sqlite3.connect(sys.argv[2])
try:
    with dst:
        src.backup(dst)
    dst.execute("PRAGMA journal_mode=DELETE")  # single-file snapshot: no -wal/-shm orphans on later opens
finally:
    src.close(); dst.close()
PYBACKUP
        then
            ssh $SSH_OPTS "$REMOTE_HOST" "rm -f '$BACKUP_DIR/$BACKUP_FILE.tmp'" || true
            die "Snapshot .backup() failed — removed partial .tmp; live untouched; prior snapshot remains rollback head."
        fi
        # Verify the .tmp BEFORE the mv. quick_check (project standard, decisions_log
        # §21) — catches torn/truncated/header from a write error; full integrity_check
        # stays at rollback (ROLLBACK.md). On fail: quarantine to .corrupt + die.
        if ! ssh $SSH_OPTS "$REMOTE_HOST" "python3 - '$BACKUP_DIR/$BACKUP_FILE.tmp'" <<'PYCHECK'
import sqlite3, sys
try:
    r = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True).execute("PRAGMA quick_check").fetchone()[0]
except Exception as e:
    print("quick_check error:", e); sys.exit(1)
sys.exit(0 if r == "ok" else 1)
PYCHECK
        then
            ssh $SSH_OPTS "$REMOTE_HOST" "mv '$BACKUP_DIR/$BACKUP_FILE.tmp' '$BACKUP_DIR/$BACKUP_FILE.corrupt'" || true
            die "Snapshot quick_check FAILED at birth — quarantined to .corrupt; live untouched; prior snapshot remains rollback head."
        fi
        # Complete + verified → atomic promotion to the final (rollback-head) name.
        ssh $SSH_OPTS "$REMOTE_HOST" "mv '$BACKUP_DIR/$BACKUP_FILE.tmp' '$BACKUP_DIR/$BACKUP_FILE'"
        # Part B (2026-06-04): the snapshot is journal_mode=DELETE (single file), but
        # defensively sweep any .tmp-wal/.tmp-shm a WAL-era open could leave so the backup
        # dir is EXACTLY <snapshot>.db + <snapshot>.db.manifest.json — nothing else.
        ssh $SSH_OPTS "$REMOTE_HOST" "rm -f '$BACKUP_DIR/$BACKUP_FILE.tmp-wal' '$BACKUP_DIR/$BACKUP_FILE.tmp-shm'"
        # keep-1 (GATED — option iii, 2026-06-04): trim an older snapshot ONLY after
        # confirming it is off-box (workstation Tier-2 OR B2 Tier-3). One that ISN'T (a prior
        # propagation silently failed) is NOT deleted — warn + flag exit 7 so the cron pages,
        # leaving the only copy intact. Runs workstation-side (the VPS can't see the off-box
        # tiers). The daily -mtime+5 cron still backstops NVMe disk.
        TRIM_B2="${B2_RCLONE_REMOTE}:${B2_BUCKET}/openregs-weekly"
        OLD_SNAPS=$(ssh $SSH_OPTS "$REMOTE_HOST" "ls -1t '$BACKUP_DIR'/openregs-predeploy-*.db 2>/dev/null | awk 'NR>1'") || { log "  WARNING: keep-1: could not list VPS snapshots (ssh failure) — skipping trim this run; daily -mtime+5 cron backstops disk."; OLD_SNAPS=""; }
        # B2 (Tier-3) listing fetched ONCE for exact-name membership tests below.
        B2_LIST=$(rclone lsf --files-only "$TRIM_B2/" 2>/dev/null) || B2_LIST=""
        while IFS= read -r REMOTE_OLD; do
            [[ -z "$REMOTE_OLD" ]] && continue
            OB=$(basename "$REMOTE_OLD")
            OFF_BOX=0
            [[ -f "$PROJECT_DIR/backups/$OB" ]] && OFF_BOX=1
            # B2 (Tier-3) fallback: exact-name membership in the once-fetched listing. NOT
            # `rclone lsf "$TRIM_B2/$OB"` — that lists $OB as a *directory* and exits 0/empty
            # for a NON-existent file → false "present" (sandbox-caught 2026-06-04). Herestring
            # (not a pipe) avoids the pipefail + grep-q SIGPIPE masking class.
            if [[ $OFF_BOX -eq 0 ]] && grep -Fxq "$OB" <<< "$B2_LIST"; then OFF_BOX=1; fi
            if [[ $OFF_BOX -eq 1 ]]; then
                log "  keep-1 trim: $OB present off-box (Tier-2/3) → rm on VPS"
                ssh $SSH_OPTS "$REMOTE_HOST" "rm -f -- '$BACKUP_DIR/$OB' '$BACKUP_DIR/$OB.manifest.json'"
            else
                log "  WARNING: keep-1 trim SKIPPED $OB — NOT on workstation OR B2; refusing to delete the only copy (heal via deploy.sh --propagate-only)."
                TRIM_UNSAFE=1
            fi
        done <<< "$OLD_SNAPS"
        log "VPS snapshot complete+verified on NVMe (keep-1; deeper history → workstation+B2)"
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
    upload_db_with_retry "$DB" "$REMOTE_HOST:${REMOTE_DB}.new" "openregs.db"
    # Pre-swap integrity gate (#19, 2026-05-25): quick_check the uploaded .new
    # BEFORE it goes live (a corrupt build/transfer can't replace a good live DB).
    # Companion to the WAL-safe backup fix. Gates the primary DB (openregs.db);
    # aphis/lobbying/fara could get the same via a small helper (noted follow-up).
    if ! ssh $SSH_OPTS "$REMOTE_HOST" "python3 - '${REMOTE_DB}.new'" <<'PYCHECK'
import sqlite3, sys
try:
    r = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True).execute("PRAGMA quick_check").fetchone()[0]
except Exception as e:
    print("quick_check error:", e); sys.exit(1)
sys.exit(0 if r == "ok" else 1)
PYCHECK
    then
        die "Pre-swap quick_check FAILED on ${REMOTE_DB}.new — refusing to swap (live DB untouched; .new left in place for inspection)"
    fi
    log "Pre-swap quick_check passed — openregs.db is structurally sound"
    log "Upload complete — atomically replacing live database..."
    # rm -wal/-shm post-mv: orphan WAL companions from the pre-swap DB can replay against
    # the new file on the next SQLite checkpoint, clobbering pages the new file thought it owned.
    # See decisions_log §83 + incident_log 2026-05-28. Belt-and-suspenders for the live-DB-readonly
    # doctrine; closes the class even if a future workaround violates the rule.
    ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_DB}.new ${REMOTE_DB} && rm -f ${REMOTE_DB}-wal ${REMOTE_DB}-shm && sudo chown datasette:datasette ${REMOTE_DB} && sudo chmod 664 ${REMOTE_DB}"
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
        upload_db_with_retry "$APHIS_DB" "$REMOTE_HOST:${REMOTE_APHIS_DB}.new" "aphis.db"
        ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_APHIS_DB}.new ${REMOTE_APHIS_DB} && rm -f ${REMOTE_APHIS_DB}-wal ${REMOTE_APHIS_DB}-shm && sudo chown datasette:datasette ${REMOTE_APHIS_DB} && sudo chmod 664 ${REMOTE_APHIS_DB}"
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
        upload_db_with_retry "$LOBBYING_DB" "$REMOTE_HOST:${REMOTE_LOBBYING_DB}.new" "lobbying.db"
        ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_LOBBYING_DB}.new ${REMOTE_LOBBYING_DB} && rm -f ${REMOTE_LOBBYING_DB}-wal ${REMOTE_LOBBYING_DB}-shm && sudo chown datasette:datasette ${REMOTE_LOBBYING_DB} && sudo chmod 664 ${REMOTE_LOBBYING_DB}"
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
        upload_db_with_retry "$FARA_DB" "$REMOTE_HOST:${REMOTE_FARA_DB}.new" "fara.db"
        ssh $SSH_OPTS "$REMOTE_HOST" "mv ${REMOTE_FARA_DB}.new ${REMOTE_FARA_DB} && rm -f ${REMOTE_FARA_DB}-wal ${REMOTE_FARA_DB}-shm && sudo chown datasette:datasette ${REMOTE_FARA_DB} && sudo chmod 664 ${REMOTE_FARA_DB}"
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

# ── Upload bulk-dumps script + dumps subdomain robots.txt ────────────────
# Added with bulk_dumps_phase1 (decisions_log §74, 2026-05-24).
# Script runs on VPS via ssh from weekly_update.sh Phase 5 after smoke pass;
# sources from /opt/openregs/*.db + /opt/datasette/990data_public.db per the
# PII guard. robots.txt is uploaded to R2 by the script itself each run.
DUMPS_SCRIPT="$PROJECT_DIR/scripts/50_generate_dumps.py"
DUMPS_ROBOTS="$PROJECT_DIR/dumps/robots.txt"
if [[ -f "$DUMPS_SCRIPT" ]]; then
    if [[ $DRY_RUN -eq 1 ]]; then
        log "[DRY-RUN] Would upload bulk-dumps script + robots to $REMOTE_HOST:$REMOTE_DIR/{scripts,dumps}/"
    else
        log "Uploading bulk-dumps script + robots.txt..."
        ssh $SSH_OPTS "$REMOTE_HOST" "mkdir -p $REMOTE_DIR/scripts $REMOTE_DIR/dumps"
        scp $SSH_OPTS -q "$DUMPS_SCRIPT" "$REMOTE_HOST:$REMOTE_DIR/scripts/"
        if [[ -f "$DUMPS_ROBOTS" ]]; then
            scp $SSH_OPTS -q "$DUMPS_ROBOTS" "$REMOTE_HOST:$REMOTE_DIR/dumps/"
        fi
        log "Bulk-dumps assets uploaded"
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

# ── Post-deploy smoke test ────────────────────────────────────────────────
# Verify prod returns the same row counts as the DBs we just deployed.
# Catches Datasette serving the old file via a stale handle (see incident_log
# 2026-05-22 — Datasette held open fd on deleted inode after ad-hoc atomic
# rename, served pre-PF-dedup data for ~30 min until restart), mid-transfer
# corruption (2026-04-11 incident), atomic-rename races, or any other
# "deploy ran but prod is wrong" failure mode. Mirrors the 990project/update.sh
# smoke test added 2026-05-10. Smoke set lives in criticality.json — adding
# a table means flipping `smoke: true` there, no code edit here.
#
# Failures here do NOT abort the script — backup propagation still runs —
# but SMOKE_FAILED is set so the final exit is non-zero and the cron's
# hc.io ping alerts. Added 2026-05-23 (parallel to 990 path).
if [[ $DRY_RUN -eq 0 ]]; then
    sleep 15  # let Datasette/WAL warmup after restart
    log "=== Post-deploy smoke test (prod vs local row counts) ==="
    SMOKE_FAILED=0
    # Output one "table,db" line per smoke=true entry in criticality.json.
    SMOKE_LIST=$(python3 -c "
import json
c = json.load(open('$PROJECT_DIR/criticality.json'))
for t, info in c['tables'].items():
    if info.get('smoke'):
        print(f\"{t},{info.get('db','openregs')}\")
")
    while IFS=, read -r table db; do
        [[ -z "$table" ]] && continue
        case "$db" in
            openregs) local_db="$DB" ;;
            lobbying) local_db="$LOBBYING_DB" ;;
            aphis)    local_db="$APHIS_DB" ;;
            fara)     local_db="$FARA_DB" ;;
            *)        log "  SKIP  $table: unknown db='$db'"; SMOKE_FAILED=1; continue ;;
        esac
        # Retry 3× with 5s/10s backoff for transient post-restart 503s.
        PROD=""
        for attempt in 1 2 3; do
            PROD=$(curl -fsS --max-time 15 \
                "https://$DOMAIN/${db}.json?sql=SELECT+COUNT(*)+AS+n+FROM+${table}&_shape=array" \
                2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['n'])" 2>/dev/null)
            [[ -n "$PROD" ]] && break
            [[ $attempt -lt 3 ]] && sleep $((attempt * 5))
        done
        LOCAL=$(python3 -c "import sqlite3; print(sqlite3.connect('$local_db').execute('SELECT COUNT(*) FROM $table').fetchone()[0])" 2>/dev/null)
        if [[ -z "$PROD" ]]; then
            log "  WARN  $table ($db): prod query failed or returned no data (local=$LOCAL)"
            SMOKE_FAILED=1
        elif [[ "$PROD" != "$LOCAL" ]]; then
            log "  FAIL  $table ($db): prod=$PROD local=$LOCAL"
            SMOKE_FAILED=1
        else
            log "  OK    $table ($db): $PROD"
        fi
    done <<< "$SMOKE_LIST"
    if [[ "$SMOKE_FAILED" -eq 1 ]]; then
        log "WARNING: post-deploy smoke test failed — prod row counts don't match local."
        log "Backup propagation will still run; script will exit non-zero at end so the cron's hc.io ping alerts."
    else
        log "Post-deploy smoke test passed"
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
    if ! backup_n2_sidecar_to_local_and_b2; then
        log "WARNING: N2 sidecar backup exited non-zero (see above). Deploy continues."
    fi
fi

log ""
log "=== Deploy complete ==="
log "Database: ${DB_SIZE_MB}MB"
log "URL: https://$DOMAIN/"

# Surface failures as a non-zero exit so the cron's hc.io ping alerts. By this
# point the deploy is LIVE — these mean "investigate the backup/data," not "redo
# the deploy." Corruption checked FIRST (more severe than a smoke mismatch): a
# snapshot failed quick_check post-birth and was quarantined, so the rollback head
# reverted to gen-1. The deploy itself succeeded (live .new passed its own pre-swap
# quick_check); the off-site backup chain for THIS generation is what's suspect.
if [[ "${SNAPSHOT_CORRUPT:-0}" -eq 1 ]]; then
    log "EXITING with status 6: a snapshot failed quick_check and was quarantined to .corrupt — rollback head reverted to gen-1; investigate."
    exit 6
fi
if [[ "${TRIM_UNSAFE:-0}" -eq 1 ]]; then
    log "EXITING with status 7: keep-1 trim skipped an un-propagated snapshot (not on workstation OR B2) — the deploy itself succeeded; investigate + deploy.sh --propagate-only."
    exit 7
fi
if [[ "${SMOKE_FAILED:-0}" -eq 1 ]]; then
    log "EXITING with status 4 due to smoke-test failure (see WARNING above)"
    exit 4
fi
