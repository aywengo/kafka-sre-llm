#!/usr/bin/env python3
"""M7 KB phase 2, I2: the independent check of the gated entries (runs/m7/reports/kb2_design.md section 4). One call per window.

The checker model (GLM-5.3-Flash-oQ8e-mtp, the judge build, not the writer; thinking off, temperature 0) sees the window's
chunks as the model receives them and the window's gated entries (title, statement, applies_when, quotes); not the writer's prompt.
It gives one verdict per entry: ok, partial, wrong or unsupported, with a short reason. An unparsable or incomplete reply is
retried once (temperature 0.3); entries still without a verdict are "unchecked". Only "ok" entries enter the index.

  .venv/bin/python scripts/run_guarded.py --stall-min 20 --log L -- .venv/bin/python scripts/kb_check_entries.py \\
      --slice work/m7/kb/slice.jsonl --write-dir work/m7/kb/sanity --out-dir work/m7/kb/sanity \\
      --model GLM-5.3-Flash-oQ8e-mtp --done runs/m7/logs/m7_kb2.sanity.check.done

Outputs in --out-dir: checks.jsonl (one row per entry: id, window, verdict, reason, attempts, versions, checker) and
check_windows.jsonl (one row per window that has gated entries: counts, seconds, tokens; the resume key). A line per window:
  [check] 12/50 <target id> entries=4 ok=3 partial=1 wrong=0 unsupported=0 unchecked=0 3.2s tokens=210
Refused: a writer folder that is not finished (no window row for every window of its done marker) or holds another entry
prompt version; an output folder with rows of another check prompt version or checker model. A second run with every window
done writes nothing. --reuse DIR copies the rows of an earlier check folder (the sanity run) for windows whose check
version, checker and entry ids are the same. --backend stub: an in-process stub server (plants from the environment: KB_STUB_CHECK_BAD target ids
whose replies never parse; KB_STUB_CHECK_PARTIAL target ids whose first entry is rated partial; an entry id ending in 0 is
rated partial).
Exit: 0 done; 1 a server or input error (resumable); 2 refused (also: the server answered with another model; never retried).
"""
import argparse
import concurrent.futures as cf
import json
import os
import re
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import kb_entry as K, prompts as P  # noqa: E402
import kb_write_entries as W  # noqa: E402

CHECKER_MODEL = "GLM-5.3-Flash-oQ8e-mtp"
FALLBACK_MODEL = "GLM-5.3-Flash-oQ4e"      # only by the conductor's note before the run (--fallback-note)
# One request at a time (W21: oMLX server requests stay sequential).
MAX_TOKENS, CONCURRENCY = 4000, 1
VERDICTS = ("ok", "partial", "wrong", "unsupported")
FILES = ("check_windows.jsonl", "checks.jsonl")


def entry_lines(entries):
    return "\n".join(json.dumps({"id": e["id"], "title": e["title"], "statement": e["statement"], **({"applies_when": e["applies_when"]} if e.get("applies_when") else {}),
                                 "quotes": e["quotes"]}, ensure_ascii=False) for e in entries)


def parse_verdicts(obj, ids):
    """{id: (verdict, reason)} of the rows that name an id of this window with a known verdict."""
    out = {}
    if not isinstance(obj, dict) or not isinstance(obj.get("verdicts"), list):
        return out
    for r in obj["verdicts"]:
        if isinstance(r, dict) and r.get("id") in ids and r.get("verdict") in VERDICTS and r["id"] not in out:
            out[r["id"]] = (r["verdict"], str(r.get("reason") or "")[:300])
    return out


def stub_checker():
    bad, partial = W._env_ids("KB_STUB_CHECK_BAD"), W._env_ids("KB_STUB_CHECK_PARTIAL")

    def handler(body):
        user = body["messages"][-1]["content"]
        ids = re.findall(r'"id": "(kbe-[0-9a-f]{16})"', user)
        targets = re.findall(r"=== chunk (\S+) ===", user)
        if set(targets) & bad:
            return "no verdicts here"
        rows = []
        for n, i in enumerate(ids):
            v = "partial" if (n == 0 and set(targets) & partial) or i.endswith("0") else "ok"     # about 1 in 16 rated partial
            rows.append({"id": i, "verdict": v, "reason": "stub"})
        return json.dumps({"verdicts": rows})
    return handler


def check_window(client, win, entries, max_tokens):
    t0 = time.monotonic()
    msgs = P.kb_check_messages(K.window_text(win, None), entry_lines(entries))
    ids = {e["id"] for e in entries}
    got, attempts, tokens = {}, 0, 0
    for temp in (0.0, W.RETRY_TEMPERATURE):
        attempts += 1
        r = client.chat(msgs, max_tokens, temperature=temp)
        tokens += r["tokens"]
        v = parse_verdicts(W.parse_json(r["text"]), ids)
        got = {**v, **got}     # a verdict of the first reply is never replaced by the retry's; the retry only fills gaps
        if len(got) == len(ids):
            break
    return got, attempts, round(time.monotonic() - t0, 2), tokens


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--slice", required=True)
    ap.add_argument("--record", default="runs/v3/INDEX_FROZEN.json")
    ap.add_argument("--write-dir", required=True, help="the writer's output folder")
    ap.add_argument("--write-done", default=None, help="the writer's done marker (required to be present when given)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--model", default=CHECKER_MODEL)
    ap.add_argument("--fallback-note", default=None, help=f"the conductor's note that allows {FALLBACK_MODEL}")
    ap.add_argument("--backend", choices=["openai", "stub"], default="openai")
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY)
    ap.add_argument("--max-tokens", type=int, default=MAX_TOKENS)
    ap.add_argument("--reuse", default=None, help="an earlier check folder whose rows of these windows are copied when the check version, checker and the window's entry ids are the same")
    ap.add_argument("--done", default=None)
    a = ap.parse_args(argv)
    if a.model == W.WRITER_MODEL:
        print("REFUSED: the checker must not be the writer's model", file=sys.stderr)
        return 2
    if a.model != CHECKER_MODEL and not (a.model == FALLBACK_MODEL and (a.fallback_note or "").strip()):
        print(f"REFUSED: the checker is {CHECKER_MODEL}; {FALLBACK_MODEL} only with --fallback-note (the conductor's note), no other model", file=sys.stderr)
        return 2
    if a.write_done and not Path(a.write_done).is_file():
        print(f"REFUSED: the writer's done marker {a.write_done} is missing (the writer has not finished)", file=sys.stderr)
        return 2
    try:
        slice_chunks, _, _ = W.load_slice_chunks(a.slice, a.record)
    except SystemExit as e:
        print(e, file=sys.stderr)
        return 2
    pos = {c["id"]: i for i, c in enumerate(slice_chunks)}
    wd, out = Path(a.write_dir), Path(a.out_dir)
    wrows = W.load_rows(wd / "windows.jsonl")
    ents = W.load_rows(wd / "entries_raw.jsonl")
    if not wrows:
        print(f"REFUSED: {wd / 'windows.jsonl'} has no rows", file=sys.stderr)
        return 2
    try:
        W.check_versions(wrows, "prompt_version", P.KB_ENTRY_PROMPT_VERSION, "writer", wrows[0]["writer"], wd / "windows.jsonl")
        W.check_versions(ents, "prompt_version", P.KB_ENTRY_PROMPT_VERSION, "writer", wrows[0]["writer"], wd / "entries_raw.jsonl")
    except SystemExit as e:
        print(e, file=sys.stderr)
        return 2
    if wrows[0]["writer"] == a.model:
        print("REFUSED: the checker must not be the writer's model", file=sys.stderr)
        return 2
    by_win = {}
    for e in ents:
        by_win.setdefault(e["window"]["target"], []).append(e)
    wins = sorted(by_win, key=lambda w: pos[w])
    out.mkdir(parents=True, exist_ok=True)
    for f in FILES:
        (out / f).touch()
    crow = W.load_rows(out / "check_windows.jsonl")
    try:
        W.check_versions(crow, "check_prompt_version", P.KB_CHECK_PROMPT_VERSION, "checker", a.model, out / "check_windows.jsonl")
        W.check_versions(W.load_rows(out / "checks.jsonl"), "check_prompt_version", P.KB_CHECK_PROMPT_VERSION, "checker", a.model,
                         out / "checks.jsonl")
    except SystemExit as e:
        print(e, file=sys.stderr)
        return 2
    stray = [r["window"] for r in crow if r["window"] not in by_win]
    if stray:
        print(f"REFUSED: {out} holds check rows of {len(stray)} window(s) without gated entries in {wd} (first {stray[0]})", file=sys.stderr)
        return 2
    done = {r["window"] for r in crow}
    removed = W.restore(out, done, FILES)
    if removed:
        print(f"[check] resume: removed {removed} line(s) of windows that had no window row")
    lock = threading.Lock()
    n_total, counter = len(wins), [len(done)]

    def append(rows, wrow):
        with lock:
            for name, rr in (("checks.jsonl", rows), ("check_windows.jsonl", [wrow])):
                with open(out / name, "a", encoding="utf-8") as f:
                    for r in rr:
                        f.write(K.dumps(r) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            counter[0] += 1
            return counter[0]
    if a.reuse:
        rdir = Path(a.reuse)
        rchecks = W.load_rows(rdir / "checks.jsonl")
        skipped = 0
        for r in sorted(W.load_rows(rdir / "check_windows.jsonl"), key=lambda r: pos.get(r["window"], -1)):
            w = r["window"]
            if w not in by_win or w in done:
                continue
            rows = [x for x in rchecks if x["window"] == w]
            if (r.get("check_prompt_version") != P.KB_CHECK_PROMPT_VERSION or r.get("checker") != a.model
                    or sorted(x["id"] for x in rows) != sorted(e["id"] for e in by_win[w])):
                skipped += 1
                continue
            k = append(rows, {**r, "reused_from": str(rdir)})
            done.add(w)
            print(f"[check] {k}/{n_total} {w} entries={len(rows)} reused", flush=True)
        if skipped:
            print(f"[check] reuse: {skipped} window(s) of {rdir} differ (version, checker or entries) and are checked again")
    todo = [w for w in wins if w not in done]
    if todo:
        if a.backend == "stub":
            transport, url, key = W.stub_transport(stub_checker()), "http://stub.invalid/v1", None
        else:
            transport = None
            url, key = W.server_config(a.base_url)
        try:
            client = W.Client(url, key, a.model, transport=transport)
        except W.ServerError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1
        print(f"[check] {len(todo)} window(s) to check of {n_total} ({len(done)} done); model {a.model}, prompt {P.KB_CHECK_PROMPT_VERSION}, "
              f"concurrency {a.concurrency}", flush=True)

        stop = threading.Event()     # set on a server error: windows not yet started return at once; futures are never cancelled
        # (shutdown(cancel_futures=True) leaves as_completed waiting forever: the hang of 2026-11-24)

        def job(w):
            if stop.is_set():
                return None
            _, win = K.window_of(slice_chunks, pos[w])
            return check_window(client, win, by_win[w], a.max_tokens)
        err = None
        with cf.ThreadPoolExecutor(max_workers=max(1, a.concurrency)) as ex:
            futs = {ex.submit(job, w): w for w in todo}
            for fu in cf.as_completed(futs):
                w = futs[fu]
                try:
                    res = fu.result()
                    if res is None:
                        continue                 # not started after a server error; checked on the resume
                    got, attempts, secs, tokens = res
                except W.ServerError as e:
                    err = e if err is None or (isinstance(e, W.WrongModel) and not isinstance(err, W.WrongModel)) else err
                    stop.set()                                        # no new call after a server error
                    print(f"[check] -/{n_total} {w} SERVER ERROR ({e}); not written", flush=True)
                    continue
                rows = []
                for e in by_win[w]:
                    v, why = got.get(e["id"], ("unchecked", "no verdict after the retry"))
                    rows.append({"id": e["id"], "window": w, "verdict": v, "reason": why, "attempts": attempts, "kind": e["kind"],
                                 "entry_prompt_version": e["prompt_version"], "check_prompt_version": P.KB_CHECK_PROMPT_VERSION,
                                 "checker": a.model})
                cnt = {k: sum(1 for r in rows if r["verdict"] == k) for k in VERDICTS + ("unchecked",)}
                wrow = {"window": w, "entries": len(rows), **cnt, "attempts": attempts, "seconds": secs, "tokens": tokens,
                        "check_prompt_version": P.KB_CHECK_PROMPT_VERSION, "checker": a.model, "created": W.now()}
                k = append(rows, wrow)
                print(f"[check] {k}/{n_total} {w} entries={len(rows)} " + " ".join(f"{x}={cnt[x]}" for x in cnt)
                      + f" {secs}s tokens={tokens}" + (" retried" if attempts > 1 else ""), flush=True)
        if isinstance(err, W.WrongModel):
            print(f"REFUSED: {err}; another client changed the server; the windows checked so far are kept (the chain's server gate decides)", file=sys.stderr)
            return 2
        if err:
            print(f"ERROR: {err}; the windows checked so far are kept (resume by running again)", file=sys.stderr)
            return 1
    rows = W.load_rows(out / "checks.jsonl")
    cnt = {k: sum(1 for r in rows if r["verdict"] == k) for k in VERDICTS + ("unchecked",)}
    if len(rows) != len(ents):
        print(f"ERROR: {len(rows)} check rows for {len(ents)} gated entries", file=sys.stderr)
        return 1
    info = {"stage": "check", "windows": n_total, "entries": len(rows), **cnt, "checker": a.model,
            "check_prompt_version": P.KB_CHECK_PROMPT_VERSION, "entry_prompt_version": P.KB_ENTRY_PROMPT_VERSION,
            **({"fallback_note": a.fallback_note} if a.model == FALLBACK_MODEL else {})}
    if a.done:
        new = W.write_marker(a.done, out, FILES, info)
        print(f"[check] done marker {'written' if new else 'unchanged (nothing new)'}: {a.done}")
    print(f"[check] summary: {n_total} windows, {len(rows)} entries: " + ", ".join(f"{k} {v}" for k, v in cnt.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
