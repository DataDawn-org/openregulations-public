#!/usr/bin/env python3
"""Per-row verdict: does each archived PDF's own Certificate field match its DB row?

THREE states, never two. `no_cert` is NOT a pass — it is a row where the verification is
INCAPABLE OF FIRING, which is a different thing from firing clean, and folding it into a
denominator with the matches is the same error as reporting 100% against a stale denominator.

Method provenance (both discarded attempts recorded deliberately):
  1. A hand-rolled zlib/regex extractor returned IDENTICAL digit strings for three unrelated
     documents (a font/encoding table, not text) and reported 38.6% match. DISCARDED — it
     measured the parser, not the archive. Tell: identical output for different inputs.
  2. pdftotext + whole-text substring search gave 3.5%; inspection showed most misses were
     layout artifacts. DISCARDED as over-counting.
  3. THIS: parse the PDF's own \\d{2}-[A-Z]-\\d{4} certificate tokens and compare.
Local CPU only. Zero requests to the source.

A PDF whose text can't be extracted (missing, pdftotext failing, timing out or crashing) gets NO verdict: it is
listed, and the run exits 1 at the end, so a rebuild stops before the populate step and the values already served
are kept (owner queue 944; relay datadawn green #2 item 6, the drafter's call). Before this, any failure read as
no_cert. Paths go through lib/pdf_paths.resolve(), which follows the 08-28 None_ -> nocert_ rename. Test:
selftest_aphis_cert_packet.py.
"""
import json, re, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.pdf_paths import ExtractFailed, pdf_text, resolve

B = Path("/mnt/data/datadawn/openregs/aphis")
OUT = B / "pdf_cert_verdicts.jsonl"
CERT = re.compile(r'\b\d{2}-[A-Z]-\d{4}\b')

def main():
    byid = {}
    for l in open(B / "pdf_archive_manifest.jsonl"):
        r = json.loads(l)
        if r.get("ok"):
            byid[r["id"]] = r
    c = sqlite3.connect(f"file:{B}/db/aphis.db?mode=ro", uri=True)
    meta = {r[0]: (r[1], r[2]) for r in c.execute(
        "SELECT id,cert_number,inspection_date FROM inspections")}

    done = set()
    if OUT.exists():
        for l in open(OUT):
            try: done.add(json.loads(l)["id"])
            except Exception: pass
    todo = [i for i in byid if i not in done]
    print(f"total={len(byid)} done={len(done)} todo={len(todo)}", flush=True)

    failed = []
    with open(OUT, "a") as fh:
        for n, i in enumerate(todo, 1):
            r = byid[i]
            dbc, dbd = meta.get(i, (None, None))
            try:
                t = pdf_text(resolve(B, r["path"]))
            except ExtractFailed as e:
                failed.append(f"{i} ({r['path']}): {e}")
                continue
            found = sorted(set(CERT.findall(t)))
            if not found:
                v = "no_cert"
            elif dbc and dbc in found:
                v = "match"
            else:
                v = "mismatch"
            fh.write(json.dumps({"id": i, "db_cert": dbc, "db_date": dbd,
                                 "pdf_certs": found, "verdict": v,
                                 "bytes": r["bytes"], "sha256": r["sha256"],
                                 "text_chars": len(t)}) + "\n")
            if n % 5000 == 0:
                fh.flush(); print(f"  {n}/{len(todo)}", flush=True)
    if failed:
        print(f"REFUSED: {len(failed)} PDF(s) could not be read, so they have no verdict (never no_cert):",
              flush=True)
        for f in failed:
            print(f"  {f}", flush=True)
        sys.exit(1)
    print("verification finished", flush=True)

if __name__ == "__main__":
    main()
