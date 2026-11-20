#!/usr/bin/env python3
"""E4 timing through the service: time to the first streamed words and time to the complete answer, on open items.

  scripts/run_guarded.py --log out/e2_first_words/run.log -- \
      .venv/bin/python scripts/eval2_first_words.py run --out out/e2_first_words/<name>
  .venv/bin/python scripts/eval2_first_words.py summary --out out/e2_first_words/<name>
  .venv/bin/python scripts/eval2_first_words.py run --stub --out <tmp>      # tests: no model, no server

Starts `kafka_sre_rag serve` (default mode) through scripts/laptop_bench.py (the service start-up and the streaming
client are its RealRunner; nothing is copied), asks each open item through the streamed endpoint, and writes one JSONL
row per item: seconds to the first words, seconds to the complete answer, completion tokens, whether the answer ended at
the token limit, and the split the service reports (search terms, retrieval, answering). The summary gives, per tier,
medians and 90th percentiles, the share at the limit, seconds per 1,000 completion tokens, and the two bars of E4.
The server address comes from the environment (OMLX_URL) as the runtime reads it and is never printed.

Default items: the 60 judged open scenarios (eval/m4/b1/items.jsonl, eval/m4/b2/items.jsonl) and the 60 open
mechanical ids of eval/m4/laptop/ids.json. Hidden files are refused (.enc, or anything under eval/hidden).
Resumable: a finished item (a row without error) is skipped; failed items are asked again.
"""
import argparse
import datetime
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import laptop_bench as lb  # noqa: E402

ROOT = lb.ROOT
JUDGED_DEFAULT = ["eval/m4/b1/items.jsonl", "eval/m4/b2/items.jsonl"]
BARS = {"median_wall_s": 60.0, "first_words_s": 10.0}
CONFIG_KEYS = ("backend", "model", "k", "max_tokens", "extra_body", "catalogues")


class HiddenRefused(ValueError):
    pass


def refuse_hidden(path):
    """Refuse an encrypted file or anything under eval/hidden. Names the argument, never opens the file."""
    p = Path(path)
    parts = {x.lower() for x in p.parts}
    if p.name.lower().endswith(".enc") or ".enc" in {s.lower() for s in p.suffixes} or "hidden" in parts:
        raise HiddenRefused(f"refused: {p.name} is a hidden file or under a hidden folder; this tool takes open items only")
    return p


def resolve_path(rel):
    refuse_hidden(rel)
    p = Path(rel)
    p = p if p.is_absolute() else ROOT / p
    refuse_hidden(p.resolve())
    return p


def load_questions(judged_files=None, ids_file=None):
    """[(tier, id, question)] for the judged tier (all rows of the files) then the mechanical tier (ids of ids.json)."""
    out = []
    for rel in (judged_files or JUDGED_DEFAULT):
        for r in lb.read_jsonl(resolve_path(rel)):
            out.append(("judged", r["id"], lb.full_question(r)))
    ids_path = resolve_path(ids_file) if ids_file else lb.IDS_FILE
    ids = json.loads(ids_path.read_text(encoding="utf-8"))["score"]
    items = {r["id"]: r for r in lb.read_jsonl(resolve_path(ids["file"]))}
    for i in ids["ids"]:
        out.append(("mechanical", i, lb.full_question(items[i])))
    seen = set()
    for t, i, _ in out:
        if (t, i) in seen:
            raise ValueError(f"duplicate id {i} in tier {t}")
        seen.add((t, i))
    return out


def row_from(ask, tier, iid, n, cold):
    return {"id": iid, "tier": tier, "i": n, "cold": cold,
            "first_words_s": ask.get("ttft_s"), "wall_s": ask.get("wall_s"), "tokens": ask.get("tokens"),
            "at_limit": bool(ask.get("truncated")), "search_s": ask.get("search_s"),
            "retrieve_s": ask.get("retrieve_s"), "answer_s": ask.get("answer_s"), "error": ask.get("error")}


def cmd_run(a):
    out = Path(a.out)
    out = out if out.is_absolute() else ROOT / out
    qs = load_questions(a.judged_items, a.ids_file)
    if a.tier != "all":
        qs = [q for q in qs if q[0] == a.tier]
    if a.n:
        qs = [q for q in qs if q[0] == "judged"][:a.n] + [q for q in qs if q[0] == "mechanical"][:a.n]
    cfg = dict(lb.DEFAULTS)
    for k in ("backend", "model", "extra_body", "k", "max_tokens", "catalogues", "port"):
        v = getattr(a, k, None)
        if v is not None:
            cfg[k] = v
    cfg["stub"] = bool(a.stub)
    out.mkdir(parents=True, exist_ok=True)
    stored = lb.read_json(out / "config.json")
    now_cfg = {k: cfg.get(k) for k in CONFIG_KEYS} | {"stub": cfg["stub"]}
    if stored is not None and stored != now_cfg:
        diff = sorted(k for k in now_cfg if stored.get(k) != now_cfg[k])
        print(f"refused: the stored config.json differs in {diff}; use a new --out folder", flush=True)
        return 2
    lb.write_json(out / "config.json", now_cfg)
    rows_file = out / "rows.jsonl"
    done = {(r["tier"], r["id"]) for r in lb.read_jsonl(rows_file) if not r.get("error")}
    todo = [q for q in qs if (q[0], q[1]) not in done]
    print(f"[first_words] {len(qs)} items, {len(qs) - len(todo)} done, {len(todo)} to ask", flush=True)
    if todo:
        runner = lb.make_runner("service", cfg, out, "service")
        info = runner.start()
        try:
            for n, (tier, iid, q) in enumerate(todo, 1):
                ask = runner.ask(q, "default", False, iid=iid)
                row = row_from(ask, tier, iid, len(qs) - len(todo) + n, n == 1)
                if n == 1 and info.get("startup_s") is not None:
                    row["startup_s"] = info["startup_s"]
                lb.append_jsonl(rows_file, row)
                print(f"[first_words] {row['i']}/{len(qs)} {tier} id={iid} first_words={lb.fmt(row['first_words_s'])}s "
                      f"wall={lb.fmt(row['wall_s'])}s tokens={row['tokens']} at_limit={row['at_limit']} "
                      f"{'FAILED ' + str(row['error']) if row['error'] else 'ok'}", flush=True)
        finally:
            runner.stop()
    write_summary(out)
    print("[first_words] done", flush=True)
    return 0


def percentile(xs, p):
    """Nearest-rank percentile of a non-empty list."""
    xs = sorted(xs)
    return xs[max(0, math.ceil(p / 100 * len(xs)) - 1)]


def stats(xs):
    xs = [x for x in xs if x is not None]
    return {"n": len(xs), "median": lb.med(xs), "p90": percentile(xs, 90) if xs else None}


def tier_summary(rows):
    ok = [r for r in rows if not r.get("error")]
    wall, fw = [r["wall_s"] for r in ok], [r["first_words_s"] for r in ok]
    wall_sum = sum(r["wall_s"] for r in ok if r["tokens"])
    tok_sum = sum(r["tokens"] for r in ok if r["tokens"])
    mw, mf = stats(wall), stats(fw)
    within = sum(1 for x in fw if x is not None and x <= BARS["first_words_s"])
    return {"asked": len(rows), "ok": len(ok), "failed": len(rows) - len(ok), "wall_s": mw, "first_words_s": mf,
            "tokens": stats([r["tokens"] for r in ok]), "at_limit": sum(1 for r in ok if r["at_limit"]),
            "s_per_1000_tokens": round(wall_sum / tok_sum * 1000, 2) if tok_sum else None,
            "first_words_within_10s": within,
            "bar_median_under_60s": {"met": mw["median"] is not None and mw["median"] < BARS["median_wall_s"],
                                     "median": mw["median"], "n": mw["n"]},
            "bar_first_words_within_10s": {"met": mf["median"] is not None and mf["median"] <= BARS["first_words_s"],
                                           "median": mf["median"], "n": mf["n"], "items_within": within}}


def summarise(rows):
    last = {}
    for r in rows:  # the last row of an item wins (a failed item is asked again)
        last[(r["tier"], r["id"])] = r
    by = {}
    for r in last.values():
        by.setdefault(r["tier"], []).append(r)
    res = {t: tier_summary(v) for t, v in sorted(by.items())}
    return res


def summary_md(s, cfg):
    L = ["# E4 timing through the service (open items)", "",
         f"Configuration: backend {cfg.get('backend')}, model {cfg.get('model') or 'service default'}, k {cfg.get('k')}, "
         f"max_tokens {cfg.get('max_tokens') or 'service default'}, stub {cfg.get('stub')}.", "",
         "| tier | ok/asked | wall median s | wall p90 s | first words median s | first words p90 s | at limit | s per 1,000 tokens |",
         "|---|---|---|---|---|---|---|---|"]
    for t, v in s.items():
        L.append(f"| {t} | {v['ok']}/{v['asked']} | {lb.fmt(v['wall_s']['median'])} | {lb.fmt(v['wall_s']['p90'])} | "
                 f"{lb.fmt(v['first_words_s']['median'])} | {lb.fmt(v['first_words_s']['p90'])} | "
                 f"{v['at_limit']}/{v['ok']} | {lb.fmt(v['s_per_1000_tokens'])} |")
    L += ["", "Bars of E4 (median of the complete answer under 60 s; first words within 10 s, read as the median):", ""]
    for t, v in s.items():
        b1, b2 = v["bar_median_under_60s"], v["bar_first_words_within_10s"]
        L.append(f"- {t}: median under 60 s {'met' if b1['met'] else 'not met'} (median {lb.fmt(b1['median'])} s, n={b1['n']}); "
                 f"first words within 10 s {'met' if b2['met'] else 'not met'} (median {lb.fmt(b2['median'])} s, n={b2['n']}, "
                 f"{b2['items_within']} of {b2['n']} items within 10 s)")
    return "\n".join(L) + "\n"


def write_summary(out):
    out = Path(out)
    s = summarise(lb.read_jsonl(out / "rows.jsonl"))
    lb.write_json(out / "summary.json", s)
    (out / "summary.md").write_text(summary_md(s, lb.read_json(out / "config.json", {})), encoding="utf-8")
    return s


def cmd_summary(a):
    out = Path(a.out)
    out = out if out.is_absolute() else ROOT / out
    write_summary(out)
    print((out / "summary.md").read_text(encoding="utf-8"), end="")
    return 0


def build_parser():
    ap = argparse.ArgumentParser(prog="eval2_first_words.py", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run")
    p.add_argument("--out", default=f"out/e2_first_words/{datetime.date.today().isoformat()}")
    p.add_argument("--stub", action="store_true", help="fake backend, for tests")
    p.add_argument("--backend", choices=["openai", "mlx"])
    p.add_argument("--model")
    p.add_argument("--extra-body")
    p.add_argument("--k", choices=["k6", "k3"])
    p.add_argument("--max-tokens", type=int)
    p.add_argument("--catalogues")
    p.add_argument("--port", type=int)
    p.add_argument("--judged-items", action="append", help="items file of the judged tier (repeatable; default B1 and B2)")
    p.add_argument("--ids-file", help="ids file of the mechanical tier (default eval/m4/laptop/ids.json)")
    p.add_argument("--tier", choices=["all", "judged", "mechanical"], default="all")
    p.add_argument("--n", type=int, help="only the first N items of each tier")
    p = sub.add_parser("summary")
    p.add_argument("--out", required=True)
    return ap


def main(argv=None):
    a = build_parser().parse_args(argv)
    try:
        return {"run": cmd_run, "summary": cmd_summary}[a.cmd](a)
    except HiddenRefused as e:
        print(str(e), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
