#!/usr/bin/env python3
"""Populate inspections.pdf_local_path from the archive manifest, WITH verification state.

Decision (2026-08-28; record: orchestrators 20e9b31): FLAG rather than exclude. A row carrying its document plus a
cert_mismatch marker is more useful downstream than a row silently missing one, and it keeps
the finding visible in the data rather than only in an audit file.

cert_verification states (four, never two):
  match        the PDF's own Certificate field equals the row's cert_number
  mismatch     the PDF names a DIFFERENT certificate  -> CONFIRMED WRONG POINTER (340)
  no_cert_ok   pre/re-license, new-site, attempted: no certificate EXISTS yet (benign, ~3,377)
  no_cert_flag routine/focused inspection with no certificate printed: UNRESOLVED (~459)
Runs under the aphis corpus lock.

Refuses before writing anything (exit 1, every value already in aphis.db kept) when a manifest id has no verdict
(the checker didn't finish: no "unknown" is ever served) or a no_cert PDF's text can't be extracted (owner queue 944;
relay datadawn green #2 item 6, the drafter's call). Paths follow the 08-28 None_ -> nocert_ rename
(lib/pdf_paths.resolve), so the 476 renamed rows get the path that exists. run_aphis.sh runs this after every
rebuild (05 drops pdf_sha256 and cert_verification). Test: selftest_aphis_cert_packet.py.
"""
import json, sqlite3, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.corpus_gate import acquire
from lib.pdf_paths import ExtractFailed, pdf_text, resolve

B = Path("/mnt/data/datadawn/openregs/aphis")
DB = B / "db" / "aphis.db"
BENIGN = ("PRE-LICENSE", "PRELICENSE", "RE-LICENSE", "RELICENSE", "NEW SITE", "ATTEMPTED")
TY = re.compile(r'Type:\s*([A-Z][A-Z \-]{3,40})')

def main():
    acquire("populate inspections.pdf_local_path + cert_verification from the archive manifest",
            "aphis-populate")
    man = {}
    for l in open(B / "pdf_archive_manifest.jsonl"):
        r = json.loads(l)
        if r.get("ok"): man[r["id"]] = r
    ver = {}
    for l in open(B / "pdf_cert_verdicts.jsonl"):
        v = json.loads(l); ver[v["id"]] = v["verdict"]

    # Work out every value first; write nothing unless all of them could be.
    rows, refused = [], []
    for rid, r in man.items():
        p = resolve(B, r["path"])
        v = ver.get(rid)
        if v is None:
            refused.append(f"{rid}: no verdict (the certificate checker has not finished)")
            continue
        if p is None:
            refused.append(f"{rid}: PDF missing ({r['path']})")
            continue
        if v == "no_cert":
            try:
                t = pdf_text(p)
            except ExtractFailed as e:
                refused.append(f"{rid} ({r['path']}): {e}")
                continue
            m = TY.search(t)
            ty = (m.group(1).strip().upper() if m else "")
            v = "no_cert_ok" if any(b in ty for b in BENIGN) else "no_cert_flag"
        rows.append((p.relative_to(B).as_posix(), r["sha256"], v, rid))
    if refused:
        print(f"REFUSED: {len(refused)} row(s) can't be populated; nothing written, aphis.db kept as it is:",
              flush=True)
        for x in refused[:50]:
            print(f"  {x}", flush=True)
        if len(refused) > 50:
            print(f"  ... and {len(refused) - 50} more", flush=True)
        sys.exit(1)

    c = sqlite3.connect(DB)
    cols = {r[1] for r in c.execute("PRAGMA table_info(inspections)")}
    if "cert_verification" not in cols:
        c.execute("ALTER TABLE inspections ADD COLUMN cert_verification TEXT")
    if "pdf_sha256" not in cols:
        c.execute("ALTER TABLE inspections ADD COLUMN pdf_sha256 TEXT")
    c.executemany("UPDATE inspections SET pdf_local_path=?, pdf_sha256=?, cert_verification=? WHERE id=?", rows)
    c.commit()
    print("populated %d rows" % len(rows), flush=True)
    for row in c.execute("SELECT cert_verification, COUNT(*) FROM inspections "
                         "WHERE pdf_local_path IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"):
        print("  %-14s %s" % (row[0], f"{row[1]:,}"), flush=True)
    c.close()

if __name__ == "__main__":
    main()
