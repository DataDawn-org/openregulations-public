#!/usr/bin/env python3
"""selftest_aphis_cert_packet.py — the APHIS certificate checker, the populate step and the rebuild runner
(relay datadawn green #2 item 6; owner queue 944; yellow 26875742's plan step 8; decisions_log §837 decision 5).

The defects (M: working-docs/receipts/2026-11-01_orange_5117a9c2/item3_pdf_cert_rerun/, yellow's plan §0):
  - verify_pdf_cert_match.py turns any pdftotext failure, a missing file included, into "no_cert";
  - populate_pdf_paths.py runs pdftotext with no failure check too, and writes "unknown" for an id with no verdict;
  - both read the manifest's paths, still "None_*" for the 476 PDFs renamed "nocert_*" on 08-28;
  - run_aphis.sh runs 01-05 only: 05 deletes and rebuilds aphis.db without pdf_sha256 and cert_verification, and
    nothing restores them.
The rule (the drafter's call, relay #2 item 6): on a pdftotext failure the checker and the populate step refuse and
keep the values already served; no new served value. The PDFs stay byte for byte under their nocert_ names.

Fixtures are seven real archived APHIS PDFs (copied into a scratch tree; the originals are only read), one per
verdict and rename state, with today's manifest, verdict and database values as the expected answers, plus a
missing file and an unreadable one. --real adds the populate step over the whole real archive (read-only) into a
scratch copy of aphis.db, compared row for row with today's; only a populate that refuses before writing and takes
its database path from DB may be run with it.

  python3 selftest_aphis_cert_packet.py                 # the live scripts in openregs/aphis/scripts
  python3 selftest_aphis_cert_packet.py --dir CANDIDATE  # a candidate scripts dir (with lib/)
  python3 selftest_aphis_cert_packet.py --dir CANDIDATE --real
Exit 0 = every case passed; 1 = a case failed.
"""
import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REAL = Path("/mnt/data/datadawn/openregs/aphis")
LIVE_DIR = REAL / "scripts"
# id -> what today's database and verdict file say (M, 2026-11-04)
FIX = {
    "0000291c435fd1b8": "match",
    "0094e948a0236b0f": "mismatch",
    "01463ad0657c36f3": "mismatch",       # renamed None_ -> nocert_; one of the 25 that a rerun would turn no_cert
    "005a7d7970183d54": "no_cert_flag",
    "000f7b1336dd0719": "no_cert_flag",   # renamed
    "000489c4d7850fc6": "no_cert_ok",
    "0385e6eb6eb727a9": "no_cert_ok",     # renamed
}
MISSING, BROKEN = "ffffffffffff0001", "ffffffffffff0002"


def real_rows():
    man, ver = {}, {}
    for line in open(REAL / "pdf_archive_manifest.jsonl"):
        r = json.loads(line)
        if r.get("ok") and r["id"] in FIX:
            man[r["id"]] = r
    for line in open(REAL / "pdf_cert_verdicts.jsonl"):
        v = json.loads(line)
        if v["id"] in FIX:
            ver[v["id"]] = v
    c = sqlite3.connect(f"file:{REAL}/db/aphis.db?mode=ro", uri=True)
    db = {r[0]: r[1:] for r in c.execute(
        "SELECT id, cert_number, inspection_date, pdf_local_path, pdf_sha256, cert_verification FROM inspections")
        if r[0] in FIX}
    c.close()
    return man, ver, db


def make_tree(*, with_columns, verdicts_for=None, extra=()):
    """A scratch aphis tree: pdfs/ (the seven, under today's names, plus extras), the manifest (today's paths, None_
    for the renamed), the verdict file (for verdicts_for ids), db/aphis.db (as 05 leaves it, or as served today)."""
    man, ver, db = real_rows()
    t = Path(tempfile.mkdtemp(prefix="staphis_"))
    (t / "pdfs").mkdir()
    (t / "db").mkdir()
    for i in FIX:
        served_path = db[i][2]                                # pdfs/nocert_... for the renamed
        shutil.copy2(REAL / served_path, t / served_path)
    lines = [json.dumps(man[i]) for i in FIX]
    for i, kind in extra:
        rec = {"id": i, "ok": True, "path": f"pdfs/99-X-0001_2026-01-01_{i}.pdf", "bytes": 10, "sha256": "0" * 64}
        if kind == "broken":
            (t / rec["path"]).write_bytes(b"this is not a PDF\n" * 4)
        lines.append(json.dumps(rec))
    (t / "pdf_archive_manifest.jsonl").write_text("\n".join(lines) + "\n")
    vf = [json.dumps(ver[i]) for i in (verdicts_for if verdicts_for is not None else FIX)]
    (t / "pdf_cert_verdicts.jsonl").write_text("".join(x + "\n" for x in vf))
    c = sqlite3.connect(t / "db" / "aphis.db")
    if with_columns:
        c.execute("CREATE TABLE inspections (id TEXT, cert_number TEXT, inspection_date TEXT, pdf_local_path TEXT, "
                  "cert_verification TEXT, pdf_sha256 TEXT)")
        c.executemany("INSERT INTO inspections VALUES (?,?,?,?,?,?)",
                      [(i, db[i][0], db[i][1], db[i][2], db[i][4], db[i][3]) for i in FIX])
    else:   # as 05 leaves it: no fingerprint or verdict column; the path as the manifest has it
        c.execute("CREATE TABLE inspections (id TEXT, cert_number TEXT, inspection_date TEXT, pdf_local_path TEXT)")
        c.executemany("INSERT INTO inspections VALUES (?,?,?,?)",
                      [(i, db[i][0], db[i][1], man[i]["path"]) for i in FIX])
    for i, _ in extra:
        c.execute("INSERT INTO inspections (id, cert_number, inspection_date, pdf_local_path) VALUES (?,?,?,?)",
                  (i, "99-X-0001", "2026-01-01", f"pdfs/99-X-0001_2026-01-01_{i}.pdf"))
    c.commit()
    c.close()
    return t, man, ver, db


def load(path, tree, name):
    for k in [k for k in sys.modules if k == "lib" or k.startswith("lib.")]:
        del sys.modules[k]
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.B = tree
    if hasattr(m, "OUT"):
        m.OUT = tree / "pdf_cert_verdicts.jsonl"
    if hasattr(m, "DB"):
        m.DB = tree / "db" / "aphis.db"
    if hasattr(m, "acquire"):
        m.acquire = lambda *a, **k: None          # never the real aphis corpus lock
    return m


def call(m):
    out = io.StringIO()
    code = 0
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            m.main()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    return code, out.getvalue()


def verdicts(tree):
    return {json.loads(x)["id"]: json.loads(x)["verdict"] for x in (tree / "pdf_cert_verdicts.jsonl").read_text().split("\n") if x}


def db_rows(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    cols = {r[1] for r in c.execute("PRAGMA table_info(inspections)")}
    sel = ", ".join(x if x in cols else "NULL" for x in ("pdf_local_path", "pdf_sha256", "cert_verification"))
    rows = {r[0]: r[1:] for r in c.execute(f"SELECT id, {sel} FROM inspections")}
    c.close()
    return rows


STUB = '''import os, sys, sqlite3
from pathlib import Path
name = Path(__file__).name
base = Path(__file__).resolve().parent.parent
with open(base / "calls.txt", "a") as f:
    f.write(name + "\\n")
if os.environ.get("APHIS_STUB_FAIL") == name:
    sys.exit(4)
if name == "05_build_database.py" and os.environ.get("APHIS_STUB_KILL05") == "1":
    # purple 56fbf00a's AC1 setup: unlink, rebuild in WAL, commit a failed build into the WAL, then die by SIGKILL
    import signal
    db = base / "db" / "aphis.db"
    if db.exists():
        db.unlink()
    c = sqlite3.connect(db); c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA wal_autocheckpoint=0")
    c.execute("CREATE TABLE marker (v)")
    c.executemany("INSERT INTO marker VALUES (?)", [("FAILED BUILD",)] * 500)
    c.commit()
    os.kill(os.getpid(), signal.SIGKILL)
if name == "05_build_database.py":
    db = base / "db" / "aphis.db"
    if db.exists():
        db.unlink()
    c = sqlite3.connect(db); c.execute("CREATE TABLE marker (v)"); c.execute("INSERT INTO marker VALUES ('rebuilt')")
    c.commit(); c.close()
if name == "populate_pdf_paths.py":
    c = sqlite3.connect(base / "db" / "aphis.db"); c.execute("INSERT INTO marker VALUES ('populated')"); c.commit(); c.close()
'''


def run_runner(script_dir, fail=None, kill05=False, detail=None):
    """run_aphis.sh --db-only from script_dir, with every Python step stubbed. Returns (exit, calls, markers);
    fills `detail` (a dict) with what lies beside the restored file and how it reads."""
    t = Path(tempfile.mkdtemp(prefix="staphisr_"))
    s = t / "scripts"
    s.mkdir()
    (t / "db").mkdir()
    shutil.copy2(Path(script_dir) / "run_aphis.sh", s / "run_aphis.sh")
    for n in ("01_discover_actions.py", "02_extract_all.py", "03_download_licensee_list.py", "04_download_pdfs.py",
              "05_build_database.py", "verify_pdf_cert_match.py", "populate_pdf_paths.py"):
        (s / n).write_text(STUB)
    c = sqlite3.connect(t / "db" / "aphis.db")
    c.execute("CREATE TABLE marker (v)")
    c.execute("INSERT INTO marker VALUES ('previous, as served')")
    c.commit()
    c.close()
    env = {k: v for k, v in os.environ.items() if k != "APHIS_STUB_FAIL"}
    env.pop("APHIS_STUB_KILL05", None)
    if fail:
        env["APHIS_STUB_FAIL"] = fail
    if kill05:
        env["APHIS_STUB_KILL05"] = "1"
    r = subprocess.run(["bash", str(s / "run_aphis.sh"), "--db-only"], capture_output=True, text=True, env=env,
                       timeout=120)
    calls = (t / "calls.txt").read_text().split() if (t / "calls.txt").exists() else []
    if detail is not None:
        detail["beside"] = sorted(p.name for p in (t / "db").iterdir() if p.name.startswith("aphis.db-"))
        detail["aside"] = sorted(p.name for p in (t / "db").iterdir() if p.name.startswith("aphis.db.failed"))
        detail["said_put_back"] = "put back as it was" in (r.stdout + r.stderr)
    try:
        c = sqlite3.connect(f"file:{t}/db/aphis.db?mode=ro", uri=True)   # as a reader opens it: any -wal is read
        markers = [x[0] for x in c.execute("SELECT v FROM marker")]
        if detail is not None:
            detail["integrity"] = c.execute("PRAGMA integrity_check").fetchone()[0]
        c.close()
    except sqlite3.Error as e:
        markers = [f"unreadable: {e}"]
    shutil.rmtree(t, ignore_errors=True)
    return r.returncode, calls, markers


def run_runner_twice(script_dir):
    """Purple 56fbf00a's N4: run 1 kills 05 after committing a failed build into its WAL; run 2 fails cleanly at the
    populate step. Returns (exits, served markers, {failed set main file: its marker counts as read})."""
    t = Path(tempfile.mkdtemp(prefix="staphis2_"))
    s = t / "scripts"
    s.mkdir()
    (t / "db").mkdir()
    shutil.copy2(Path(script_dir) / "run_aphis.sh", s / "run_aphis.sh")
    for n in ("01_discover_actions.py", "02_extract_all.py", "03_download_licensee_list.py", "04_download_pdfs.py",
              "05_build_database.py", "verify_pdf_cert_match.py", "populate_pdf_paths.py"):
        (s / n).write_text(STUB)
    c = sqlite3.connect(t / "db" / "aphis.db")
    c.execute("CREATE TABLE marker (v)")
    c.execute("INSERT INTO marker VALUES ('previous, as served')")
    c.commit()
    c.close()
    base = {k: v for k, v in os.environ.items() if k not in ("APHIS_STUB_FAIL", "APHIS_STUB_KILL05")}
    exits = []
    for extra in ({"APHIS_STUB_KILL05": "1"}, {"APHIS_STUB_FAIL": "populate_pdf_paths.py"}):
        r = subprocess.run(["bash", str(s / "run_aphis.sh"), "--db-only"], capture_output=True, text=True,
                           env={**base, **extra}, timeout=120)
        exits.append(r.returncode)
        time.sleep(1.1)        # a later failure, at least a second later
    sets = {}
    for p in sorted((t / "db").iterdir()):
        if p.name.startswith("aphis.db.failed") and not p.name.endswith(("-wal", "-shm", "-journal")):
            try:
                cc = sqlite3.connect(f"file:{p}?mode=ro", uri=True)   # as anyone opening it would: its -wal is read
                rows = [x[0] for x in cc.execute("SELECT v FROM marker")]
                cc.close()
                sets[p.name] = {v: rows.count(v) for v in set(rows)}
            except sqlite3.Error as e:
                sets[p.name] = {f"unreadable: {e}": 1}
    cc = sqlite3.connect(f"file:{t}/db/aphis.db?mode=ro", uri=True)
    served = [x[0] for x in cc.execute("SELECT v FROM marker")]
    cc.close()
    shutil.rmtree(t, ignore_errors=True)
    return exits, served, sets


COLLIDE_STUB = '''import sys, sqlite3, time
from pathlib import Path
name = Path(__file__).name
base = Path(__file__).resolve().parent.parent
if name == "05_build_database.py":
    for _ in range(200):
        if (base / "go").exists():
            break
        time.sleep(0.05)
    db = base / "db" / "aphis.db"
    if db.exists():
        db.unlink()
    c = sqlite3.connect(db); c.execute("CREATE TABLE marker (v)"); c.execute("INSERT INTO marker VALUES ('FAILED BUILD')")
    c.commit(); c.close()
    sys.exit(4)
'''


def run_runner_collision(script_dir, suffix):
    """Purple 74b06e3f's probe, as a leg (its S2): a PATH stub for `date` pins the set's time, Popen's pid is the
    runner's $$, and a file holding the set's name (or its -wal name) is made before 05 fails. Returns (exit, the
    taken file unchanged?, served markers, files in db/)."""
    t = Path(tempfile.mkdtemp(prefix="staphisc_"))
    s = t / "scripts"
    s.mkdir()
    (t / "db").mkdir()
    (t / "bin").mkdir()
    shutil.copy2(Path(script_dir) / "run_aphis.sh", s / "run_aphis.sh")
    for n in ("01_discover_actions.py", "02_extract_all.py", "03_download_licensee_list.py", "04_download_pdfs.py",
              "05_build_database.py", "verify_pdf_cert_match.py", "populate_pdf_paths.py"):
        (s / n).write_text(COLLIDE_STUB)
    (t / "bin" / "date").write_text('#!/bin/bash\nif [[ "${1:-}" == "+%Y%m%dT%H%M%S" ]]; then echo 20261104T120000; '
                                    'else exec /usr/bin/date "$@"; fi\n')
    os.chmod(t / "bin" / "date", 0o755)
    c = sqlite3.connect(t / "db" / "aphis.db")
    c.execute("CREATE TABLE marker (v)")
    c.execute("INSERT INTO marker VALUES ('previous, as served')")
    c.commit()
    c.close()
    env = {k: v for k, v in os.environ.items() if k not in ("APHIS_STUB_FAIL", "APHIS_STUB_KILL05")}
    env["PATH"] = f"{t / 'bin'}:{env.get('PATH', '')}"
    p = subprocess.Popen(["bash", str(s / "run_aphis.sh"), "--db-only"], stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, env=env)
    taken = t / "db" / f"aphis.db.failed-20261104T120000-{p.pid}{suffix}"
    taken.write_bytes(b"someone else's file\n")
    before = taken.read_bytes()
    (t / "go").touch()
    p.communicate(timeout=60)
    unchanged = taken.exists() and taken.read_bytes() == before
    cc = sqlite3.connect(f"file:{t}/db/aphis.db?mode=ro", uri=True)
    served = [x[0] for x in cc.execute("SELECT v FROM marker")]
    cc.close()
    files = sorted(x.name.replace(str(p.pid), "<pid>") for x in (t / "db").iterdir())
    shutil.rmtree(t, ignore_errors=True)
    return p.returncode, unchanged, served, files


def real_allowed(m):
    """--real may run a populate only when it has a DB setting of its own, pointed outside the real folder (purple
    56fbf00a's AC2): a populate without one writes REAL/db/aphis.db, the served file."""
    if not hasattr(m, "DB"):
        return False, "this populate has no DB setting of its own, so it would write the served aphis.db"
    try:
        Path(m.DB).resolve().relative_to(REAL.resolve())
        return False, f"its DB ({m.DB}) is inside {REAL}"
    except ValueError:
        return True, ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=str(LIVE_DIR))
    ap.add_argument("--real", action="store_true")
    a = ap.parse_args()
    d = Path(a.dir).resolve()
    print(f"scripts under test: {d}")
    results = []

    def case(name, ok, detail):
        results.append(bool(ok))
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")

    # ---- the checker ----
    t, man, ver, db = make_tree(with_columns=True, verdicts_for=[], extra=[(MISSING, "missing"), (BROKEN, "broken")])
    code, text = call(load(d / "verify_pdf_cert_match.py", t, "chk1"))
    v = verdicts(t)
    case("REFUSE checker: a missing PDF gets no verdict (never no_cert)", MISSING not in v, f"verdict {v.get(MISSING)}")
    case("REFUSE checker: an unreadable PDF gets no verdict (never no_cert)", BROKEN not in v, f"verdict {v.get(BROKEN)}")
    case("REFUSE checker: a run with an extraction failure exits non-zero", code not in (0, None), f"exit {code}")
    wrong = {i: (v.get(i), FIX[i].split("_ok")[0].split("_flag")[0]) for i in FIX
             if v.get(i) != FIX[i].replace("no_cert_ok", "no_cert").replace("no_cert_flag", "no_cert")}
    case("CLEAN checker: the seven real PDFs, the three renamed ones included, get today's verdicts", not wrong,
         f"differing: {wrong}")
    shutil.rmtree(t, ignore_errors=True)

    t, man, ver, db = make_tree(with_columns=True)
    before = (t / "pdf_cert_verdicts.jsonl").read_bytes()
    code, text = call(load(d / "verify_pdf_cert_match.py", t, "chk2"))
    case("CLEAN checker: verdicts already held are kept as they are, and a run with nothing to do exits 0",
         code in (0, None) and (t / "pdf_cert_verdicts.jsonl").read_bytes() == before, f"exit {code}")
    shutil.rmtree(t, ignore_errors=True)

    # ---- the populate step ----
    t, man, ver, db = make_tree(with_columns=True, extra=[(BROKEN, "broken")])
    vf = t / "pdf_cert_verdicts.jsonl"
    vf.write_text(vf.read_text() + json.dumps({"id": BROKEN, "verdict": "no_cert"}) + "\n")
    before = db_rows(t / "db" / "aphis.db")
    code, text = call(load(d / "populate_pdf_paths.py", t, "pop1"))
    after = db_rows(t / "db" / "aphis.db")
    case("REFUSE populate: an unreadable no_cert PDF stops it, exit non-zero", code not in (0, None), f"exit {code}")
    case("REFUSE populate: on that refusal every value already held is kept", after == before,
         f"{sum(1 for k in before if before[k] != after.get(k))} row(s) changed")
    shutil.rmtree(t, ignore_errors=True)

    ids = list(FIX)
    t, man, ver, db = make_tree(with_columns=True, verdicts_for=ids[1:])
    before = db_rows(t / "db" / "aphis.db")
    code, text = call(load(d / "populate_pdf_paths.py", t, "pop2"))
    after = db_rows(t / "db" / "aphis.db")
    case("REFUSE populate: an id with no verdict stops it before anything is written (no 'unknown' served)",
         code not in (0, None) and after == before and "unknown" not in {r[2] for r in after.values()},
         f"exit {code}; cert_verification values {sorted({str(r[2]) for r in after.values()})}")
    shutil.rmtree(t, ignore_errors=True)

    t, man, ver, db = make_tree(with_columns=False)
    code, text = call(load(d / "populate_pdf_paths.py", t, "pop3"))
    after = db_rows(t / "db" / "aphis.db")
    want = {i: (db[i][2], db[i][3], db[i][4]) for i in FIX}
    diff = {i: (after.get(i), want[i]) for i in FIX if after.get(i) != want[i]}
    case("CLEAN populate after a rebuild: paths (nocert_ for the renamed), fingerprints and verdicts equal today's",
         code in (0, None) and not diff, f"exit {code}; differing: {diff}")
    files_ok = all(hashlib.sha256((t / want[i][0]).read_bytes()).hexdigest() == man[i]["sha256"] for i in FIX)
    case("CLEAN the PDFs stay byte for byte under their names", files_ok, "sha256 of each against the manifest")
    shutil.rmtree(t, ignore_errors=True)

    # ---- the runner ----
    code, calls, markers = run_runner(d)
    case("CLEAN run_aphis.sh --db-only: 05, then the checker, then populate",
         code == 0 and calls == ["05_build_database.py", "verify_pdf_cert_match.py", "populate_pdf_paths.py"],
         f"exit {code}; steps {calls}; db {markers}")
    for step in ("verify_pdf_cert_match.py", "populate_pdf_paths.py", "05_build_database.py"):
        code, calls, markers = run_runner(d, fail=step)
        case(f"REFUSE run_aphis.sh: {step} failing after the rebuild began fails the run and restores aphis.db",
             code != 0 and markers == ["previous, as served"], f"exit {code}; steps {calls}; db {markers}")
    det = {}
    code, calls, markers = run_runner(d, kill05=True, detail=det)
    case("REFUSE run_aphis.sh: 05 killed after committing a failed build into its WAL: the run fails, and aphis.db "
         "reads as the copy (no -wal beside it; the failed set kept aside)",
         code != 0 and markers == ["previous, as served"] and not det.get("beside") and det.get("aside")
         and det.get("integrity") == "ok" and det.get("said_put_back"),
         f"exit {code}; db {markers[:1]}{'…' if len(markers) > 1 else ''} ({len(markers)} rows); beside "
         f"{det.get('beside')}; aside {det.get('aside')}; integrity {det.get('integrity')}; "
         f"said put back {det.get('said_put_back')}")

    exits, served, sets = run_runner_twice(d)
    contents = sorted(sets.values(), key=lambda x: sorted(x))
    case("REFUSE run_aphis.sh: two failures in a row keep two separate failed sets, each reading as its own build "
         "(never an older -wal beside a newer set), and the served file is the copy both times",
         all(x != 0 for x in exits) and served == ["previous, as served"] and len(sets) == 2
         and {"FAILED BUILD": 500} in contents and {"rebuilt": 1} in contents,
         f"exits {exits}; served {served}; sets {sets}")

    for suffix in ("", "-wal"):
        code, unchanged, served, files = run_runner_collision(d, suffix)
        case(f"REFUSE run_aphis.sh: a failed-set name already taken ({'its main file' if not suffix else suffix}) "
             "is left untouched, the set goes under the next suffix, and the copy is still put back",
             code != 0 and unchanged and served == ["previous, as served"]
             and any(f.endswith("-<pid>-2") for f in files),
             f"exit {code}; taken file unchanged {unchanged}; served {served}; files {files}")

    # ---- the --real guard (purple 56fbf00a's AC2) ----
    import types
    no_db = real_allowed(types.SimpleNamespace(B=REAL))
    in_real = real_allowed(types.SimpleNamespace(B=REAL, DB=REAL / "db" / "aphis.db"))
    scratch = real_allowed(types.SimpleNamespace(B=REAL, DB=Path(tempfile.gettempdir()) / "x" / "aphis.db"))
    case("REFUSE --real is refused for a populate with no DB of its own, or one pointed inside the real folder",
         no_db[0] is False and in_real[0] is False and scratch[0] is True,
         f"no DB: {no_db}; DB in the real folder: {in_real}; DB in scratch: {scratch}")

    # ---- REAL (only with --real): the populate step over the real archive into a scratch copy ----
    m_real = load(d / "populate_pdf_paths.py", REAL, "popR") if a.real else None
    if a.real and not hasattr(m_real, "DB"):      # load() points an existing DB at the folder; none exists here
        case("REAL refused before anything ran", False, real_allowed(m_real)[1])
    elif a.real:
        t = Path(tempfile.mkdtemp(prefix="staphisR_"))
        (t / "db").mkdir()
        src = sqlite3.connect(f"file:{REAL}/db/aphis.db?mode=ro", uri=True)
        dst = sqlite3.connect(t / "db" / "aphis.db")
        src.backup(dst)
        src.close()
        dst.execute("ALTER TABLE inspections DROP COLUMN cert_verification")
        dst.execute("ALTER TABLE inspections DROP COLUMN pdf_sha256")
        dst.commit()
        dst.close()
        m = m_real
        m.DB = t / "db" / "aphis.db"
        assert real_allowed(m)[0], "the real leg's DB must be outside the real folder"
        code, text = call(m)
        got, want = db_rows(t / "db" / "aphis.db"), db_rows(REAL / "db" / "aphis.db")
        diff = [k for k in want if got.get(k) != want[k]]
        case("REAL populate over the real archive equals today's aphis.db row for row",
             code in (0, None) and not diff and len(got) == len(want),
             f"exit {code}; rows {len(got)}; differing {len(diff)} {diff[:3]}")
        shutil.rmtree(t, ignore_errors=True)

    failed = results.count(False)
    print(f"{len(results) - failed}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
