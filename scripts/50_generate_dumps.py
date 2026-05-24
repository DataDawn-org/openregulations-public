#!/usr/bin/env python3
"""
50_generate_dumps.py — Generate weekly gzipped SQLite snapshots of the 6
Datasette-served databases and upload to Cloudflare R2 at dumps.datadawn.org.

Spec:   bestpractices/bulk_dumps_phase1_spec.md
Origin: bestpractices/decisions_log.md §74 (close §58 + bulk dumps as
        leverage extension, 2026-05-24)

Runs ON THE VPS (not the workstation). Sources from /opt/openregs/ and
/opt/datasette/ — the deployed Datasette serving roots that already passed
the PII redaction in 05_build_database.py. Invoked from weekly_update.sh
via ssh after deploy + smoke-test pass; smoke pass is the gating signal
per spec.

Uses boto3 (not rclone) for R2 operations: the VPS's apt-installed
rclone 1.60.1-DEV silently 403s on R2 writes (auth header mismatch).
boto3 hits the same R2 S3 endpoint cleanly. Credentials come from the
[r2-dumps] section of ~/.config/rclone/rclone.conf to avoid duplicating
secret storage.

Pipeline:
  1. Pre-flight PII guards: allowlist match + source under deployed root
  2. Per-file: pigz --best → sha256 → upload to _pending_{week_id}/ → delete
     local. Caps peak local disk at one compressed artifact (~15 GB) instead
     of ~30 GB cumulative — mitigation for the 2026-05-23 disk-full class.
  3. Build manifest.json, upload to _pending_{week_id}/.
  4. Atomic dated-archive promotion: list+copy+delete each object under
     _pending_{week_id}/ to {week_id}/. R2 metadata ops, fast. Failure
     semantics: mid-run crash leaves _pending_* operator-visible (24h R2
     lifecycle cleans it), consumers never see a half-populated dated archive.
  5. Update current-pointer manifest: copy {week_id}/manifest.json to root.
  6. Upload dumps.datadawn.org/robots.txt (idempotent each run).

Log markers (for weekly_update.sh grep):
  DUMPS_OK       — successful run
  DUMPS_FAILED   — any fatal error (PII guard, compression, upload)
"""

import argparse
import configparser
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("[FATAL] boto3 not installed (apt-get install python3-boto3 or pip install boto3)",
          flush=True)
    print("DUMPS_FAILED", flush=True)
    sys.exit(1)

# ─── Allowlist (PII guard #1) ───────────────────────────────────────────────
#
# Exact-string match. Any filename outside this set is a hard error. Tracks
# vps-config/systemd/openregs.service + datasette.service ExecStart args,
# verified 2026-05-24. Re-verify before any change. Future Datasette DB
# additions must be added here explicitly — silent omission is the intended
# failure mode for unknown files.
CANONICAL_DEPLOYED_DUMPS = frozenset({
    "openregs.db",
    "990data_public.db",  # NOT 990data.db. The unsanitized 990data.db holds
                          # target_orgs, funder_bench, op_retreat_*, and
                          # other private analysis tables that must NEVER
                          # be published. Datasette serves only
                          # 990data_public.db. Same-prefix-different-suffix
                          # is a known footgun.
    "aphis.db",
    "lobbying.db",
    "fara.db",
    "open_comments.db",
})

# ─── Allowed source roots (PII guard #2) ───────────────────────────────────
#
# Source path must resolve under one of these. Tracks systemd ExecStart
# args. Any path resolving to a staging directory (`staging/`), acquisition
# output, or anywhere else is rejected. Hard error.
ALLOWED_SOURCE_ROOTS = (
    "/opt/openregs/",
    "/opt/datasette/",
)

# Phase 1: every artifact at schema_version 1. S-C1 (2026-05-22 lobbying
# schema redesign) backfilled retroactively in the Phase 2 public changelog.
DEFAULT_SCHEMA_VERSION = 1
# TODO(phase-2): emit schema_version_note alongside any schema_version > 1
# at the manifest-entry construction site below.

# ─── R2 configuration ──────────────────────────────────────────────────────
#
# Bucket bound to dumps.datadawn.org via Cloudflare CNAME.
# Credentials live in ~/.config/rclone/rclone.conf [r2-dumps] section.
R2_BUCKET = os.environ.get("DUMPS_R2_BUCKET", "datadump")
RCLONE_CONFIG_PATH = Path(os.environ.get(
    "RCLONE_CONFIG", "~/.config/rclone/rclone.conf")).expanduser()
RCLONE_SECTION = os.environ.get("DUMPS_R2_REMOTE", "r2-dumps")

# Staging directory for compressed artifacts before R2 upload. Cleaned
# after each per-file upload; size never exceeds one compressed artifact.
STAGING_DIR = Path(os.environ.get("DUMPS_STAGING_DIR", "/var/tmp/dumps_staging"))

# Static dumps.datadawn.org/robots.txt source path. Uploaded to bucket root
# on each run for idempotency (kept in version control, not bucket-managed).
DUMPS_ROBOTS_TXT = Path(os.environ.get(
    "DUMPS_ROBOTS_TXT", "/opt/openregs/dumps/robots.txt"))

# ─── Logging ───────────────────────────────────────────────────────────────


def _ts():
    return datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"


def log(msg, level="INFO"):
    print(f"[{_ts()}] [{level}] {msg}", flush=True)


def die(msg):
    log(msg, level="FATAL")
    print("DUMPS_FAILED", flush=True)
    sys.exit(1)


# ─── PII guards ────────────────────────────────────────────────────────────


def resolve_and_check_source(filename):
    """Apply PII guards #1+#2. Return the resolved absolute Path."""
    if filename not in CANONICAL_DEPLOYED_DUMPS:
        die(
            f"PII guard #1: {filename!r} not in allowlist. "
            f"Allowlist: {sorted(CANONICAL_DEPLOYED_DUMPS)}. "
            f"Add explicitly if intentional."
        )

    for root in ALLOWED_SOURCE_ROOTS:
        candidate = Path(root) / filename
        if candidate.exists():
            resolved = candidate.resolve()
            if not any(str(resolved).startswith(r) for r in ALLOWED_SOURCE_ROOTS):
                die(
                    f"PII guard #2: {filename!r} at {candidate} "
                    f"resolves to {resolved} outside {ALLOWED_SOURCE_ROOTS} "
                    f"(symlink escape)."
                )
            return resolved

    die(
        f"PII guard #2: {filename!r} not found under {ALLOWED_SOURCE_ROOTS}. "
        f"Source must be the deployed Datasette serving root; "
        f"staging/acquisition/analysis paths are forbidden."
    )


# ─── Compression + hashing ─────────────────────────────────────────────────


def compress_with_pigz(source_path: Path, output_path: Path) -> int:
    """pigz --best (parallel gzip, gzip-compatible output). Returns bytes."""
    log(f"compress: {source_path} → {output_path.name} (pigz --best)")
    with open(output_path, "wb") as out:
        proc = subprocess.run(
            ["pigz", "--best", "-c", str(source_path)],
            stdout=out,
            check=False,
        )
    if proc.returncode != 0:
        die(f"pigz failed (returncode {proc.returncode}) for {source_path}")
    size = output_path.stat().st_size
    log(f"  → {output_path.name}: {size:,} bytes ({size / 1024**3:.2f} GiB)")
    return size


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


# ─── R2 ops via boto3 ──────────────────────────────────────────────────────

_s3_client = None


def get_s3_client():
    """Lazy-init boto3 S3 client with credentials from rclone.conf."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    if not RCLONE_CONFIG_PATH.exists():
        die(f"rclone config not found at {RCLONE_CONFIG_PATH}")

    cfg = configparser.RawConfigParser()
    cfg.read(RCLONE_CONFIG_PATH)
    if RCLONE_SECTION not in cfg:
        die(f"[{RCLONE_SECTION}] section missing from {RCLONE_CONFIG_PATH}")
    section = cfg[RCLONE_SECTION]

    _s3_client = boto3.session.Session().client(
        "s3",
        endpoint_url=section["endpoint"],
        aws_access_key_id=section["access_key_id"],
        aws_secret_access_key=section["secret_access_key"],
        region_name=section.get("region", "auto"),
    )
    return _s3_client


def s3_upload_file(local_path: Path, key: str):
    log(f"upload: {local_path.name} → s3://{R2_BUCKET}/{key}")
    try:
        get_s3_client().upload_file(str(local_path), R2_BUCKET, key)
    except ClientError as e:
        die(f"S3 upload failed: {local_path} → {key} ({e})")


def s3_list_prefix(prefix: str):
    """Return list of object keys under prefix."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    try:
        for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except ClientError as e:
        die(f"S3 list failed: prefix={prefix} ({e})")
    return keys


def s3_copy_object(src_key: str, dst_key: str):
    log(f"copy: {src_key} → {dst_key}")
    try:
        get_s3_client().copy_object(
            CopySource={"Bucket": R2_BUCKET, "Key": src_key},
            Bucket=R2_BUCKET,
            Key=dst_key,
        )
    except ClientError as e:
        die(f"S3 copy failed: {src_key} → {dst_key} ({e})")


def s3_delete_object(key: str):
    try:
        get_s3_client().delete_object(Bucket=R2_BUCKET, Key=key)
    except ClientError as e:
        die(f"S3 delete failed: {key} ({e})")


def s3_move_prefix(src_prefix: str, dst_prefix: str):
    """Atomic-ish promotion: copy each object under src_prefix to dst_prefix,
    then delete from src_prefix. R2 metadata ops, fast for small object counts.
    """
    log(f"promote: {src_prefix} → {dst_prefix}")
    keys = s3_list_prefix(src_prefix)
    if not keys:
        die(f"No objects found under {src_prefix} to promote")
    for src_key in sorted(keys):
        suffix = src_key[len(src_prefix):]
        dst_key = dst_prefix + suffix
        s3_copy_object(src_key, dst_key)
        s3_delete_object(src_key)


# ─── Pipeline ──────────────────────────────────────────────────────────────


def iso_week_id(now=None) -> str:
    """ISO-8601 year-week identifier (e.g. 2026-W22)."""
    now = now or datetime.datetime.utcnow()
    iso_year, iso_week, _ = now.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def iso_now() -> str:
    return datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"


def main():
    parser = argparse.ArgumentParser(
        description="Generate weekly bulk dumps for dumps.datadawn.org"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Resolve sources + log paths but do not compress/upload.",
    )
    parser.add_argument(
        "--week-id", default=None,
        help="Override ISO week id (default: current UTC week).",
    )
    args = parser.parse_args()

    week_id = args.week_id or iso_week_id()
    pending_prefix = f"_pending_{week_id}/"
    dated_prefix = f"{week_id}/"

    log("=" * 70)
    log("Bulk dumps run starting")
    log(f"  week_id={week_id}")
    log(f"  dry_run={args.dry_run}")
    log(f"  bucket=s3://{R2_BUCKET}")
    log(f"  pending={pending_prefix}")
    log("=" * 70)

    # Tool availability.
    if not shutil.which("pigz"):
        die("pigz not installed (apt-get install pigz)")

    # Pre-flight: resolve + apply PII guards 1+2 for every allowlisted file.
    resolved = []
    for filename in sorted(CANONICAL_DEPLOYED_DUMPS):
        path = resolve_and_check_source(filename)
        size_bytes = path.stat().st_size
        log(f"  ✓ {filename:24s} → {path}  ({size_bytes / 1024**3:.2f} GiB)")
        resolved.append((filename, path))

    if args.dry_run:
        log("Dry-run complete — no compression/upload performed.")
        print("DUMPS_OK", flush=True)
        return 0

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    # Per-file: compress → hash → upload → delete local → next.
    artifacts = []
    for filename, source_path in resolved:
        compressed_name = f"{filename}.gz"
        compressed_path = STAGING_DIR / compressed_name

        if compressed_path.exists():
            log(f"  removing stale staging file: {compressed_path}")
            compressed_path.unlink()

        size_bytes = compress_with_pigz(source_path, compressed_path)
        sha256 = sha256_file(compressed_path)
        log(f"  sha256({compressed_name}) = {sha256}")

        # Audit log (PII guard #3): source path + sha256 in traceable form.
        log(
            f"AUDIT source={source_path} target={compressed_name} "
            f"size={size_bytes} sha256={sha256}"
        )

        s3_upload_file(compressed_path, f"{pending_prefix}{compressed_name}")

        compressed_path.unlink()
        log(f"  → local {compressed_name} deleted (push-and-delete)")

        artifacts.append({
            "filename": compressed_name,
            "size_bytes": size_bytes,
            "sha256_gzipped": sha256,
            "generated_at": iso_now(),
            "schema_version": DEFAULT_SCHEMA_VERSION,
        })

    # Build manifest + upload under pending prefix.
    manifest = {
        "manifest_generated_at": iso_now(),
        "schema_version_format": "integer-monotonic",
        "sha256_target": "gzipped_artifact",
        "current_week": week_id,
        "artifacts": artifacts,
    }
    manifest_path = STAGING_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    s3_upload_file(manifest_path, f"{pending_prefix}manifest.json")
    manifest_path.unlink()

    # Atomic promotion: pending → dated archive.
    s3_move_prefix(pending_prefix, dated_prefix)

    # Update current-pointer manifest (last write before consumers see it).
    s3_copy_object(f"{dated_prefix}manifest.json", "manifest.json")

    # Upload dumps subdomain robots.txt (idempotent each run).
    if DUMPS_ROBOTS_TXT.exists():
        s3_upload_file(DUMPS_ROBOTS_TXT, "robots.txt")
    else:
        log(f"WARNING: {DUMPS_ROBOTS_TXT} not found; skipped robots.txt upload.")

    log("=" * 70)
    log(f"Bulk dumps run complete — week_id={week_id}")
    log(f"  Current pointer: https://dumps.datadawn.org/manifest.json")
    log(f"  Dated archive:   https://dumps.datadawn.org/{week_id}/manifest.json")
    log("=" * 70)
    print("DUMPS_OK", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
