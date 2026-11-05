#!/usr/bin/env python3
"""selftest_09_bill_listing.py — a failed Congress.gov listing page in 09_congress_gov.py (relay datadawn green #2
item 1; owner queue 792; decisions_log §827).

The defect (measured 2026-11-02, §827): in the 10-31 weekly one House-resolution list page (offset 250) failed with
"Response ended prematurely". download_bills_list() logged it and returned the 250 it had; download_congress() wrote
that partial list over hres_list.json; the run exited 0; and the bills check, on its first run, passed and stored the
short count (18,432) as its baseline. 37 resolutions stayed stale because the re-fetch never saw them.

The rule this test holds the script to:
  - a listing page that still fails after its retries fails the run (exit non-zero) and the bills check (FAIL,
    naming the listing);
  - a partial list never overwrites the saved one, and is never written as one;
  - the check's baseline (state "bills_listed") is set only from a full fetch, so a partial count never becomes it.

Runs 09's main() in a scratch tree with Congress.gov, govinfo, the ping, the acquisition counter and config replaced
by fakes: nothing reaches the network, the real state, the real lists or the hc account.

  python3 selftest_09_bill_listing.py                   # the live script (and the feed_hc.py beside it)
  python3 selftest_09_bill_listing.py --script X.py     # a candidate; it imports the feed_hc.py beside it
Exit 0 = every case passed; 1 = a case failed. Legs marked REFUSE are the must-refuse half; CLEAN legs are the
clean half (a correct run must produce no failure).
"""
import argparse
import contextlib
import importlib.util
import io
import json
import logging
import shutil
import sys
import tempfile
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
LIVE = Path("/mnt/data/datadawn/openregs/scripts/09_congress_gov.py")
PAGE = 2                      # tiny pages, so a 7-bill listing spans four pages


def numbers(n0, k):
    return [str(n) for n in range(n0, n0 + k)]


class FakeResp:
    status_code = 200


class FakeBulk:
    def get(self, url, timeout=None):
        return FakeResp()


def run(script, *, listings, fail, prior_lists=None, state_extra=None, empty=None):
    """listings: {(congress, type): [numbers]}; fail: {(congress, type, offset): n} -- that page fails n times
    (a large n: every try). prior_lists: {(congress, type): [numbers]} saved list files before the run.
    Returns a dict of what the run left behind."""
    tree = Path(tempfile.mkdtemp(prefix="st09l_"))
    (tree / "logs").mkdir()
    for (c, t), nums in (prior_lists or {}).items():
        d = tree / "congress_gov" / f"congress_{c}"
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{t}_list.json").write_text(json.dumps([{"number": n, "updateDate": "2026-01-01"} for n in nums],
                                                     indent=2))
    state = {"completed_congresses": [], "completed_bills": {}}
    state.update(state_extra or {})
    (tree / "logs" / "congress_state.json").write_text(json.dumps(state))
    before = {p.relative_to(tree).as_posix(): p.read_bytes() for p in (tree / "congress_gov").rglob("*_list.json")}

    sys.modules.pop("feed_hc", None)
    sys.modules["acquisition_floor"] = types.SimpleNamespace(record_acquisition=lambda *a, **k: None)
    spec = importlib.util.spec_from_file_location(f"c09l_{tree.name}", script)
    m = importlib.util.module_from_spec(spec)
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        spec.loader.exec_module(m)
    for h in list(m.log.handlers):          # keep the test's lines out of the real congress.log
        m.log.removeHandler(h)
        h.close()
    m.log.addHandler(logging.StreamHandler(out))
    m.OUTPUT_DIR = tree / "congress_gov"
    m.LOG_DIR = tree / "logs"
    m.STATE_FILE = tree / "logs" / "congress_state.json"
    m.BILL_TYPES = ["hr", "hres"]
    m.PAGE_SIZE = PAGE
    if hasattr(m, "LIST_RETRY_WAITS"):
        m.LIST_RETRY_WAITS = tuple(0 for _ in m.LIST_RETRY_WAITS)
    m.load_config = lambda: "test-key"
    m.create_api_session = lambda key: object()
    m.create_bulk_session = lambda: FakeBulk()
    tries = {}

    def fake_api_get(session, url, params=None):
        if params is None:                              # the connection test
            return {"bills": [], "pagination": {"count": 0}}
        parts = url.rstrip("/").split("/")
        c, t, off = int(parts[-2]), parts[-1], params.get("offset", 0)
        key = (c, t, off)
        if key in fail:
            tries[key] = tries.get(key, 0) + 1
            if tries[key] <= fail[key]:
                return None
        nums = listings.get((c, t), [])
        if key in (empty or {}):                        # purple's N2: an HTTP 200 page with no bills, short of count
            ekey = ("empty",) + key
            tries[ekey] = tries.get(ekey, 0) + 1
            if tries[ekey] <= empty[key]:
                return {"bills": [], "pagination": {"count": len(nums)}}
        page = nums[off:off + params.get("limit", PAGE)]
        pag = {"count": len(nums)}
        if off + PAGE < len(nums):
            pag["next"] = "next"
        return {"bills": [{"number": n, "updateDate": "2026-10-01", "type": t.upper()} for n in page],
                "pagination": pag}

    def fake_billstatus(bulk_session, congress, bill_type, number):
        return {"number": str(number), "type": bill_type.upper(), "congress": congress, "updateDate": "2026-10-01",
                "actions": [], "latestAction": {"actionDate": "2026-10-01", "text": "x"}}

    reported = []

    def fake_report(feed, facts, log=print, runner=None):
        reported.append((feed, json.loads(json.dumps(facts))))
        ok, _ = m.feed_hc.verdict(facts)
        return 0 if ok else 1

    m.api_get = fake_api_get
    m.download_single_billstatus = fake_billstatus
    m.feed_hc.report = fake_report
    argv = sys.argv
    sys.argv = ["09_congress_gov.py"]
    code = 0
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            m.main()
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    finally:
        sys.argv = argv
        for h in list(m.log.handlers):
            m.log.removeHandler(h)
    after = {p.relative_to(tree).as_posix(): p.read_bytes() for p in (tree / "congress_gov").rglob("*_list.json")}
    saved = json.loads(m.STATE_FILE.read_text())
    res = {"code": code, "before": before, "after": after, "state": saved, "reported": reported, "m": m,
           "log": out.getvalue(), "tree": tree}
    return res


def verdict_of(res):
    bills = [f for feed, f in res["reported"] if feed == "bills"]
    if not bills:
        return None, ["no bills report was sent"], {}
    ok, reasons = res["m"].feed_hc.verdict(bills[-1])
    return ok, reasons, bills[-1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", default=str(LIVE))
    script = Path(ap.parse_args().script).resolve()
    print(f"script under test: {script}")
    results = []

    def case(name, ok, detail):
        results.append(bool(ok))
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")

    full119 = {(119, "hr"): numbers(1, 7), (119, "hres"): numbers(1, 7)}
    full = {**full119, (118, "hr"): numbers(1, 3), (118, "hres"): numbers(1, 3)}
    always = 99
    hres_p2 = {(119, "hres", PAGE): always}         # the 10-31 shape: the second page fails every try
    prior = {(119, "hres"): numbers(1, 7)}          # last week's full list, saved

    # REFUSE 1: a failed page never overwrites the saved list.
    r = run(script, listings=full, fail=hres_p2, prior_lists=prior,
            state_extra={"bills_listed": {"119": 14}})
    k = "congress_gov/congress_119/hres_list.json"
    case("REFUSE a partial list never overwrites the saved one",
         r["after"].get(k) == r["before"].get(k),
         f"saved list {'unchanged' if r['after'].get(k) == r['before'].get(k) else 'OVERWRITTEN'} "
         f"({len(json.loads(r['after'][k])) if k in r['after'] else 'none'} entries after, 7 before)")
    # REFUSE 2: the run fails.
    case("REFUSE a failed listing page fails the run (exit non-zero)", r["code"] not in (0, None),
         f"exit {r['code']}")
    # REFUSE 3: the bills check fails, naming the listing.
    ok, reasons, facts = verdict_of(r)
    named = any("hres" in x.lower() or "incomplete" in x.lower() for x in reasons)
    case("REFUSE the bills check says FAIL and names the incomplete listing", ok is False and named,
         f"verdict {'SUCCESS' if ok else 'FAIL'}; reasons {reasons}")
    # CLEAN guard: a good baseline is not lowered by the partial run.
    case("REFUSE a partial run leaves the earlier full baseline as it was",
         r["state"].get("bills_listed", {}).get("119") == 14,
         f"bills_listed now {r['state'].get('bills_listed')}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # REFUSE 4 and 5: no saved list and no baseline yet (the 10-31 weekly was the check's first run).
    r = run(script, listings=full, fail=hres_p2)
    k = "congress_gov/congress_119/hres_list.json"
    case("REFUSE a partial list is never written as the saved list", k not in r["after"],
         f"{k} {'absent' if k not in r['after'] else 'WRITTEN with ' + str(len(json.loads(r['after'][k]))) + ' entries'}")
    case("REFUSE a partial count never becomes the check's baseline",
         "119" not in r["state"].get("bills_listed", {}),
         f"bills_listed now {r['state'].get('bills_listed')}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # REFUSE 6: the verdict itself (feed_hc.py beside the script).
    fh = r["m"].feed_hc
    ok6, why6 = fh.verdict({"listed": 14, "min_listed": 14, "errors": 0, "incomplete": ["119 hres: page at offset 2"]})
    case("REFUSE feed_hc: an incomplete listing is a FAIL even when the counts look whole", ok6 is False,
         f"verdict {'SUCCESS' if ok6 else 'FAIL'} {why6}")
    ok7, why7 = fh.verdict({"listed": 14, "errors": 0, "incomplete": "hres"})
    case("REFUSE feed_hc: a malformed incomplete fact is a FAIL, never a success", ok7 is False,
         f"verdict {'SUCCESS' if ok7 else 'FAIL'} {why7}")

    # CLEAN 1: a full fetch writes the full list, exits 0, the check succeeds, and its count is the baseline.
    r = run(script, listings=full, fail={}, prior_lists=prior)
    k = "congress_gov/congress_119/hres_list.json"
    ok, reasons, facts = verdict_of(r)
    case("CLEAN a full fetch: list written whole, exit 0, check SUCCESS, baseline = its count",
         r["code"] in (0, None) and len(json.loads(r["after"][k])) == 7 and ok is True
         and r["state"].get("bills_listed", {}).get("119") == 14,
         f"exit {r['code']}; {len(json.loads(r['after'][k]))} entries; verdict {ok} {reasons}; "
         f"bills_listed {r['state'].get('bills_listed')}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # CLEAN 2: the baseline a partial run left (18,432 on 10-31; 9 here) is replaced by the next full fetch.
    r = run(script, listings=full, fail={}, state_extra={"bills_listed": {"119": 9}})
    ok, reasons, _ = verdict_of(r)
    case("CLEAN the next full fetch resets a short baseline to its own count",
         r["state"].get("bills_listed", {}).get("119") == 14 and ok is True,
         f"bills_listed {r['state'].get('bills_listed')}; verdict {ok} {reasons}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # CLEAN 3: a page that fails once and then answers is a full fetch (the bounded retry).
    r = run(script, listings=full, fail={(119, "hres", PAGE): 1}, prior_lists=prior)
    k = "congress_gov/congress_119/hres_list.json"
    ok, reasons, _ = verdict_of(r)
    n_after = len(json.loads(r["after"][k])) if k in r["after"] else 0
    case("CLEAN a page that fails once and answers on retry: full list, exit 0, check SUCCESS",
         r["code"] in (0, None) and n_after == 7 and ok is True,
         f"exit {r['code']}; {n_after} entries; verdict {ok} {reasons}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # CLEAN 4: other feeds send no incomplete fact; their verdicts are unchanged.
    ok8, why8 = fh.verdict({"listed": 5, "errors": 0})
    ok9, why9 = fh.verdict({"listed": 5, "errors": 0, "incomplete": []})
    case("CLEAN feed_hc: no incomplete fact, or an empty one, changes nothing", ok8 is True and ok9 is True,
         f"{ok8} {why8}; {ok9} {why9}")

    # ---- relay datadawn green #3 item 3: purple 56fbf00a's N2 and N1 ----
    # REFUSE N2: a page that answers with no bills, short of the listing's own count, every try, is a failed listing.
    r = run(script, listings=full, fail={}, empty={(119, "hres", PAGE): always}, prior_lists=prior,
            state_extra={"bills_listed": {"119": 14}})
    k = "congress_gov/congress_119/hres_list.json"
    ok, reasons, _ = verdict_of(r)
    case("REFUSE N2: an empty page short of the listing's count fails the run and the check, the saved list kept",
         r["code"] not in (0, None) and ok is False and r["after"].get(k) == r["before"].get(k)
         and any("incomplete" in x.lower() for x in reasons),
         f"exit {r['code']}; verdict {ok} {reasons}; saved list {'kept' if r['after'].get(k) == r['before'].get(k) else 'CHANGED'}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # CLEAN N2: a transient empty page (once, then the page answers) is a full fetch.
    r = run(script, listings=full, fail={}, empty={(119, "hres", PAGE): 1}, prior_lists=prior)
    ok, reasons, _ = verdict_of(r)
    n_after = len(json.loads(r["after"][k])) if k in r["after"] else 0
    case("CLEAN N2: a page empty once that then answers gives the whole list, exit 0, check SUCCESS",
         r["code"] in (0, None) and n_after == 7 and ok is True, f"exit {r['code']}; {n_after} entries; verdict {ok} {reasons}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # CLEAN N2: a listing that is truly empty (its count is 0) is not a failure.
    r = run(script, listings={**full, (119, "hres"): []}, fail={})
    ok, reasons, _ = verdict_of(r)
    case("CLEAN N2: a listing whose own count is 0 is not a failure", r["code"] in (0, None) and ok is True,
         f"exit {r['code']}; verdict {ok} {reasons}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # REFUSE N1: a prior Congress with no saved list, whose listing fails, is not marked complete (a rerun retries it).
    r = run(script, listings=full, fail={(118, "hres", PAGE): always})
    done = r["state"].get("completed_congresses", [])
    case("REFUSE N1: a prior Congress whose listing failed is not marked complete", "118" not in done,
         f"completed_congresses {done}; exit {r['code']}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    # REFUSE S2 (purple 74b06e3f's S2 on the N2/N1 change; relay datadawn green #3's S1 accepted, #5 item 3): page drift,
    # one number read twice across a page boundary and another never read, so the entries reach the count but the
    # distinct numbers fall short. That is an incomplete listing, and the shortfall must be counted by distinct number.
    drift = ["1", "2", "2", "4", "5", "6", "7"]          # "3" skipped, "2" read twice; Congress.gov's count stays 7
    r = run(script, listings={**full, (119, "hres"): drift}, fail={}, prior_lists=prior,
            state_extra={"bills_listed": {"119": 14}})
    k = "congress_gov/congress_119/hres_list.json"
    ok, reasons, _ = verdict_of(r)
    case("REFUSE S2: a listing whose entries reach the count but whose distinct numbers fall short is incomplete",
         r["code"] not in (0, None) and ok is False and r["after"].get(k) == r["before"].get(k)
         and any("incomplete" in x.lower() for x in reasons),
         f"exit {r['code']}; verdict {ok} {reasons}; saved list "
         f"{'kept' if r['after'].get(k) == r['before'].get(k) else 'CHANGED'}")
    shutil.rmtree(r["tree"], ignore_errors=True)

    failed = results.count(False)
    print(f"{len(results) - failed}/{len(results)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
