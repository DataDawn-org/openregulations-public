#!/usr/bin/env python3
"""selftest_aphis_builder_side_files.py — the APHIS builder removes aphis.db's side files with it before it builds
(decisions_log §847 A1; relay datadawn green #2 item 3 and #3 item 3; white's legs for builders 05, 11 and 22, applied
to openregs/aphis/scripts/05_build_database.py).

The failure (measured 2026-11-05, SQLite 3.45.1): while another process still holds the OLD aphis.db open in a read
transaction, its -wal and -shm stay behind when the builder unlinks the main file. The new file's first statement
then fails with "disk I/O error", and the file stays unreadable after that reader closes.

Each leg runs the builder's own DatabaseBuilder.connect() (the unlink, the connect and the schema), imported from the
script under test, on a scratch aphis.db, then writes one row and commits, as the build's first import would. The
builder's call to sqlite3.connect on that path is watched, to record which side files are on disk at that moment.
  R1  must hold: a reader of the old file is open in another process (main file present)
  N1  must hold: the same, with the old main file already gone (a build that died after its unlink), so the side files
      are removed even when there is no database to remove
  C1-C4 checks that pass before and after the change: a stale -wal and -shm, a -wal alone, a -shm alone, a hot
      -journal, with no reader (SQLite discards each beside a new, empty file; none is replayed)
A leg passes when the build connects and commits, no side file of the path is on disk at the builder's connect (R1,
N1), and afterwards a fresh connection reads the new schema with the one row, no old table, integrity_check ok.

  python3 selftest_aphis_builder_side_files.py                       # the live builder beside this file
  python3 selftest_aphis_builder_side_files.py --script OLD.py --lib DIR
--lib is the folder holding the builder's `lib/` package (default: this file's folder). Exit 0 = every leg passed.
"""
import argparse
import importlib.util
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
SIDES = ("-wal", "-shm", "-journal")
READER = r'''
import sqlite3, sys
c = sqlite3.connect(sys.argv[1], isolation_level=None)
c.execute("BEGIN")
c.execute("SELECT COUNT(*) FROM old_t").fetchone()
print("ready", flush=True)
sys.stdin.readline()
c.execute("COMMIT")
c.close()
print("closed", flush=True)
'''


def load_builder(script, lib):
    sys.path.insert(0, str(lib))
    spec = importlib.util.spec_from_file_location(f"aphis05_{os.getpid()}_{id(script)}", script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def clean(p):
    for s in ("",) + SIDES:
        try:
            os.remove(str(p) + s)
        except FileNotFoundError:
            pass


def watch_connect(mod, target, seen):
    """Point the builder's `sqlite3` at a copy of the module whose connect records the side files present when the
    builder opens TARGET. Only the builder's own name changes; this file's sqlite3 is untouched."""
    shim = types.ModuleType("sqlite3_watched")
    shim.__dict__.update(sqlite3.__dict__)

    def connect(path, *a, **k):
        if str(path) == str(target):
            seen.append([s for s in SIDES if os.path.exists(str(target) + s)])
        return sqlite3.connect(path, *a, **k)

    shim.connect = connect
    mod.sqlite3 = shim


def build(mod, p):
    """The builder's connect() (unlink, connect, schema), then one row and a commit. Returns an error repr or None."""
    b = mod.DatabaseBuilder(Path(p))
    try:
        b.connect()
        b.conn.execute("INSERT INTO facilities (cert_number, legal_name) VALUES ('99-T-0001', 'selftest')")
        b.conn.commit()
        return None
    except sqlite3.Error as e:
        return repr(e)
    finally:
        b.close()


def read_after(p):
    c = sqlite3.connect(p)
    try:
        tabs = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        n = c.execute("SELECT COUNT(*) FROM facilities").fetchone()[0] if "facilities" in tabs else None
        ic = c.execute("PRAGMA integrity_check").fetchone()[0]
        return {"facilities": n, "old_table": "old_t" in tabs, "integrity": ic, "error": None}
    except sqlite3.Error as e:
        return {"error": repr(e)}
    finally:
        c.close()


def old_db_with_reader(p, main_gone):
    """An old aphis.db in WAL mode whose -wal holds frames a reader in another process still needs, so the -wal and
    -shm stay beside it. With main_gone, the main file is then removed (the side files stay)."""
    clean(p)
    w = sqlite3.connect(p)
    w.execute("PRAGMA journal_mode=WAL")
    w.execute("PRAGMA wal_autocheckpoint=0")
    w.execute("CREATE TABLE old_t(x)")
    w.executemany("INSERT INTO old_t VALUES(?)", [(i,) for i in range(5000)])
    w.commit()
    rd = subprocess.Popen([sys.executable, "-I", "-c", READER, str(p)], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, text=True)
    if rd.stdout.readline().strip() != "ready":
        raise RuntimeError("the reader did not start")
    w.executemany("INSERT INTO old_t VALUES(?)", [(-i,) for i in range(1, 2001)])
    w.commit()
    w.close()
    if main_gone:
        os.remove(p)
    present = [s for s in SIDES if os.path.exists(str(p) + s)]
    return rd, present


def stale_side_files(p, scratch, which):
    """An old aphis.db with stale side files beside it and no reader: frames saved from a writer (-wal, -shm) or a hot
    -journal saved mid-transaction."""
    src = scratch / "src.db"
    clean(src)
    if which == "-journal":
        c = sqlite3.connect(src, isolation_level=None)
        c.execute("PRAGMA journal_mode=DELETE")
        c.execute("CREATE TABLE old_t(x)")
        c.execute("PRAGMA cache_size=1")
        c.execute("BEGIN")
        c.executemany("INSERT INTO old_t VALUES(?)", [(i,) for i in range(20000)])
        have = os.path.exists(str(src) + "-journal")
        if have:
            shutil.copy(str(src) + "-journal", scratch / "saved-journal")
        c.execute("ROLLBACK")
        c.close()
        if not have:
            return None
        saved = {"-journal": scratch / "saved-journal"}
    else:
        w = sqlite3.connect(src)
        w.execute("PRAGMA journal_mode=WAL")
        w.execute("PRAGMA wal_autocheckpoint=0")
        w.execute("CREATE TABLE old_t(x)")
        w.executemany("INSERT INTO old_t VALUES(?)", [(i,) for i in range(5000)])
        w.commit()
        shutil.copy(str(src) + "-wal", scratch / "saved-wal")
        shutil.copy(str(src) + "-shm", scratch / "saved-shm")
        w.close()
        saved = {s: scratch / f"saved{s}" for s in which}
    clean(p)
    o = sqlite3.connect(p)
    o.execute("CREATE TABLE old_t(x)")
    o.commit()
    o.close()
    for s, f in saved.items():
        shutil.copy(f, str(p) + s)
    return sorted(saved)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", default=str(HERE / "05_build_database.py"))
    ap.add_argument("--lib", default=str(HERE))
    a = ap.parse_args()
    script = Path(a.script).resolve()
    print(f"script under test: {script}\nsqlite {sqlite3.sqlite_version}")
    mod = load_builder(script, Path(a.lib).resolve())
    scratch = Path(tempfile.mkdtemp(prefix="st_aphis_side_"))
    p = scratch / "aphis.db"
    results = []

    def leg(name, ok, detail):
        results.append(bool(ok))
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")

    try:
        for code, label, main_gone in (("R1", "a reader of the old file open in another process", False),
                                       ("N1", "a reader open and the old main file already gone", True)):
            seen = []
            watch_connect(mod, p, seen)
            rd, before = old_db_with_reader(p, main_gone)
            err = build(mod, p)
            rd.stdin.write("go\n")
            rd.stdin.flush()
            rd.wait(timeout=30)
            after = read_after(p)
            at_connect = seen[0] if seen else None
            ok = (err is None and at_connect == [] and after.get("error") is None and after.get("facilities") == 1
                  and after.get("old_table") is False and after.get("integrity") == "ok")
            leg(f"{code} {label}: the build connects and commits, and the new file reads clean",
                ok, f"side files before the build {before}; at the builder's connect {at_connect}; build error {err}; "
                    f"after the reader closed {after}")
        for code, which in (("C1", ("-wal", "-shm")), ("C2", ("-wal",)), ("C3", ("-shm",)), ("C4", "-journal")):
            seen = []
            watch_connect(mod, p, seen)
            planted = stale_side_files(p, scratch, which)
            if planted is None:
                leg(f"{code} a stale hot -journal", False, "SETUP: no hot journal could be captured")
                continue
            err = build(mod, p)
            after = read_after(p)
            ok = (err is None and after.get("error") is None and after.get("facilities") == 1
                  and after.get("old_table") is False and after.get("integrity") == "ok")
            leg(f"{code} stale {'+'.join(planted)}, no reader: nothing of the old file reaches the new one",
                ok, f"at the builder's connect {seen[0] if seen else None}; build error {err}; after {after}")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    n_fail = results.count(False)
    print(f"{len(results) - n_fail}/{len(results)} passed")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
