"""Where an archived APHIS PDF is, and its text, for the certificate checker and the populate step.

Owner queue 944; relay datadawn green #2 item 6 (with yellow's plan step 8). Test: selftest_aphis_cert_packet.py.

resolve(): the manifest (pdf_archive_manifest.jsonl) keeps each PDF's path as fetched. On 2026-08-28,
rename_nocert_pdfs.py renamed 476 of them from None_* to nocert_* and repointed aphis.db, not the manifest (its own
event line says so: "DB is the live pointer"). So a path that is gone under None_* is looked for under nocert_*.

pdf_text(): pdftotext's text, or ExtractFailed. A missing file, a non-zero exit, a timeout or a crash is a failure,
never an empty text: an empty text reads as "no certificate", which is a verdict, not a failure.
"""
import subprocess
from pathlib import Path

OLD_PREFIX, NEW_PREFIX = "None_", "nocert_"


class ExtractFailed(Exception):
    pass


def resolve(base, manifest_path):
    """The PDF's path now, or None when it is under neither its manifest name nor its 08-28 rename."""
    p = Path(base) / manifest_path
    if p.is_file():
        return p
    if p.name.startswith(OLD_PREFIX):
        q = p.with_name(NEW_PREFIX + p.name[len(OLD_PREFIX):])
        if q.is_file():
            return q
    return None


def pdf_text(path, timeout=60):
    """pdftotext -q PATH - , or raise ExtractFailed naming why."""
    if path is None or not Path(path).is_file():
        raise ExtractFailed("file missing")
    try:
        r = subprocess.run(["pdftotext", "-q", str(path), "-"], capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise ExtractFailed(f"pdftotext timed out after {timeout}s")
    except OSError as e:
        raise ExtractFailed(f"pdftotext could not run ({type(e).__name__})")
    if r.returncode != 0:
        raise ExtractFailed(f"pdftotext exit {r.returncode}")
    return r.stdout.decode("utf-8", errors="replace")
