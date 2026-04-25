# OpenRegs deploy rollback procedure

Three-tier backup system. Pick the tier that matches the time since the last
known-good deploy.

## When to roll back

- Datasette `/openregs` returns 500s or times out persistently after a deploy.
- `PRAGMA integrity_check` on the live DB returns anything but `ok`.
- A canned query or explore page that worked before the deploy now 500s.
- Users report missing data (tables empty, rows zero) that existed pre-deploy.

**Do NOT roll back for**: slow queries (could be ANALYZE/stats issue — run
`sqlite3 /opt/openregs/openregs.db "ANALYZE;"` first), minor template glitches
(templates roll back separately), or single missing queries (`metadata.json`
only).

## Backup tiers

| Tier | Retention | Location | Restore time |
|---|---|---|---|
| 1. VPS (hot) | ≤ 5 days | `user@your-vps:/opt/openregs/backups/` | ~3 min |
| 2. Workstation (warm) | last 3 deploys (~21 days) | `/mnt/data/datadawn/openregs/backups/` | ~15 min |
| 3. B2 offsite (cold) | last 3 deploys (~21 days) | `b2:someones-backup/openregs-weekly/` | ~25 min |

File naming: `openregs-predeploy-YYYYMMDD_HHMM.db`. Each is a pre-deploy
snapshot of what was live at that timestamp.

**Picking a tier:**
- Regression discovered within 5 days → **Tier 1**.
- Longer than 5 days (Tier 1 already swept) → **Tier 2**.
- Workstation compromised / offline / local backup dir lost → **Tier 3**.

**Limit**: the system covers roughly the last 21 days. A regression discovered
>21 days after deploy has no clean rollback path; recovery requires rebuilding
from source data.

## Tier 1 — Restore from VPS backup (≤ 5 days since deploy)

```bash
# 1. Verify backup exists + pick most-recent pre-deploy snapshot
ssh user@your-vps 'ls -lth /opt/openregs/backups/openregs-predeploy-*.db'

# Then SSH in:
ssh user@your-vps
```

On the VPS:

```bash
LATEST=$(ls -t /opt/openregs/backups/openregs-predeploy-*.db | head -n 1)
echo "Rolling back to $LATEST"

# Stop datasette (prevents reads mid-swap)
sudo systemctl stop openregs

# Preserve the broken live DB for forensics
sudo mv /opt/openregs/openregs.db /opt/openregs/openregs.db.broken.$(date +%s)

# Copy backup into place (NOT mv — keep backup intact for future rollbacks)
sudo cp "$LATEST" /opt/openregs/openregs.db
sudo chown datasette:datasette /opt/openregs/openregs.db
sudo chmod 664 /opt/openregs/openregs.db

# Integrity-check before starting the service
sudo -u datasette sqlite3 /opt/openregs/openregs.db 'PRAGMA integrity_check;'
# Must print 'ok'. If not, try an older backup or escalate to Tier 2.

# Bring it up
sudo systemctl start openregs
sudo systemctl status openregs --no-pager

# Smoke-check via Caddy
curl -sI https://regs.datadawn.org/ | head -3
curl -s 'https://regs.datadawn.org/openregs/congress_members?_size=1' | head -30
```

## Tier 2 — Restore from workstation backup (5-21 days since deploy)

When the regression is older than 5 days, the VPS-tier snapshot has been
swept. Restore from the workstation's local backup directory.

On the workstation:

```bash
# 1. List available backups (newest first)
ls -lth /mnt/data/datadawn/openregs/backups/openregs-predeploy-*.db
# Expect up to 3 files

# 2. Pick the pre-regression backup and integrity-check it FIRST
BACKUP=/mnt/data/datadawn/openregs/backups/openregs-predeploy-YYYYMMDD_HHMM.db
python3 -c "
import sqlite3
r = sqlite3.connect(f'file:$BACKUP?mode=ro', uri=True).execute('PRAGMA integrity_check').fetchone()[0]
assert r == 'ok', f'Backup integrity fail: {r}'
print('ok')
"

# 3. Rsync backup up to VPS under a temp name (never over live)
rsync -a --partial-dir=.rsync-partials --progress --timeout=600 \
    "$BACKUP" user@your-vps:/opt/openregs/openregs.db.rollback
# ~36 GB over home pipe: typically 10-15 min
```

On the VPS:

```bash
sudo systemctl stop openregs
sudo mv /opt/openregs/openregs.db /opt/openregs/openregs.db.broken.$(date +%s)
sudo mv /opt/openregs/openregs.db.rollback /opt/openregs/openregs.db
sudo chown datasette:datasette /opt/openregs/openregs.db
sudo chmod 664 /opt/openregs/openregs.db
sudo -u datasette sqlite3 /opt/openregs/openregs.db 'PRAGMA integrity_check;'
sudo systemctl start openregs
sudo systemctl status openregs --no-pager
```

Smoke-check:

```bash
curl -sI https://regs.datadawn.org/ | head -3
curl -s 'https://regs.datadawn.org/openregs/congress_members?_size=1' | head -30
```

## Tier 3 — Restore from B2 offsite (last-line fallback)

Use when the workstation is compromised, offline, or the local backup
directory has been lost. Works from any machine with `rclone` + the B2 remote
configured.

```bash
# 1. List available B2 backups (newest first via sort -r)
rclone lsf b2:someones-backup/openregs-weekly/ | sort -r

# 2. Download the pre-regression backup
mkdir -p /tmp/b2-restore
rclone copy -P b2:someones-backup/openregs-weekly/openregs-predeploy-YYYYMMDD_HHMM.db \
    /tmp/b2-restore/
# ~36 GB from B2: typically 20-25 min

# 3. Integrity-check
BACKUP=/tmp/b2-restore/openregs-predeploy-YYYYMMDD_HHMM.db
python3 -c "
import sqlite3
r = sqlite3.connect(f'file:$BACKUP?mode=ro', uri=True).execute('PRAGMA integrity_check').fetchone()[0]
assert r == 'ok'
print('ok')
"

# 4. Continue as Tier 2 step 3+ (rsync up, swap on VPS)
```

## Post-rollback triage

1. Inspect `/opt/openregs/openregs.db.broken.*` — run `PRAGMA integrity_check`,
   count rows on canonical tables, grep the build log for errors.
2. Review `/tmp/build_*.log` or `openregs/logs/weekly_update*.log` on the
   workstation for root cause.
3. If the failure was disk-space mid-deploy, the `.new` file may be at
   `/opt/openregs/openregs.db.new` — remove it.
4. Once root cause is fixed locally, re-run the deploy. The `.broken.*` file
   can be deleted after confirming stability.

## Notes on specific deploys

### 2026-04-25 staging merge (first post-hook deploy)

If the 2026-04-25 weekly deploy needs rollback, the Apr 18 backup is
pre-staging-merge and lacks entities, disbursements, oge_pas, wh_visits,
government_units, and bmf_group_* tables. Canned queries or explore pages
targeting those will 500 until a re-deploy with the fix lands. Non-staging
features remain functional.

### Workstation-side `openregs_prev.db`

`05_build_database.py` renames `openregs.db` → `openregs_prev.db` at the
start of each local build. This is the DEVELOPER'S previous successful
build, NOT last-week's production. Use for local debugging; don't scp it up
for rollback without verifying.

## Rollback time budget

| Scenario | Typical time |
|---|---|
| Tier 1 (VPS, in-place cp) | ~3 min |
| Tier 2 (Workstation rsync up + swap) | ~15 min |
| Tier 3 (B2 rclone down + rsync up + swap) | ~25 min |

## FTS rebuild (rare)

Backed-up DBs contain pre-built FTS tables. If FTS shows empty results after
rollback (usually indicates a corrupted backup or incomplete WAL checkpoint
at backup time):

```bash
sudo -u datasette sqlite3 /opt/openregs/openregs.db <<'SQL'
INSERT INTO federal_register_fts(federal_register_fts) VALUES('rebuild');
INSERT INTO documents_fts(documents_fts) VALUES('rebuild');
-- repeat for any FTS table showing missing results
SQL
```

## Related runbooks

- `deploy/deploy.sh` — deploy logic (including the post-swap backup-propagation
  hook that populates Tiers 2 and 3)
- `deploy/rotate_local_backups.py` — count-based retention for Tier 2
- `deploy/check_backup_freshness.py` — daily staleness monitor (alerts if
  propagation has silently stopped working)
- `bestpractices/cron_inventory.md` — hc.io UUIDs for backup-tier monitors
