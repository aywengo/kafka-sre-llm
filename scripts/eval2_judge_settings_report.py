#!/usr/bin/env python3
"""Comparison of judge settings on a fixed subset of 16 answers, pass 1 (scripts/m4_s5a_judge_settings.sh).

  scripts/eval2_judge_settings_report.py report --results glm-think-12k=work/m4/pilot/judge_settings/glm-think-12k.jsonl \
      glm-think-0k=... [--noeffect glm-nothink-12k] > runs/m4/S5A_JUDGE_SETTINGS.md
  scripts/eval2_judge_settings_report.py check-switch --cache CACHE --setting glm-nothink-12k   # exit 0: thinking is off

--results are --out files of scripts/eval2_judge.py, one per setting. The report states numbers only and no winner.
Against the reader it uses the same definitions as scripts/eval2_pilot_report.py: a labelled wrong item is caught when the
same answer has a must-not point present, a must point contradicted, or an other_errors entry whose quote overlaps the
label's quote; an essential omission is marked when the answer's essential-miss flag is set.
"""
import argparse
import json
import math
import statistics as st
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eval2_pilot_report import READER_OMISSIONS, cohen_kappa, fmt, norm_ws, quotes_overlap  # noqa: E402

SUBSET = [("real_default", f"real-{n}") for n in (1, 3, 5, 31, 39)] \
    + [("real_strict", f"real-{n}") for n in (3, 36)] \
    + [("real_v2_own_terms", f"real-{n}") for n in (31, 36, 39)] \
    + [("real_v2_base_excerpts", f"real-{n}") for n in (2, 39)] \
    + [("holdout_default", f"holdout-{n}") for n in (13, 15, 34)] \
    + [("holdout_strict", "holdout-3")]
CONCURRENCY = 3
FULL_PILOT_CALLS, JUDGED_TIER_CALLS = 220, 240
TOKENS_PER_CHAR = 0.3   # rough: a JSON reply of C characters is about 0.3 C tokens


def rd(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def thinking_still_on(entry):
    """True if the cache entry of a call made with thinking switched off shows thinking: a reasoning field with text,
    a <think> block in the reply, or completion tokens far above what the reply itself needs. None if unknown."""
    if (entry.get("reasoning_chars") or 0) > 0:
        return True
    reply = entry.get("reply")
    if isinstance(reply, str) and "<think>" in reply:
        return True
    toks = (entry.get("usage") or {}).get("completion_tokens")
    if toks is None or not isinstance(reply, str):
        return None
    return toks > 3 * TOKENS_PER_CHAR * len(reply) + 300


def median(xs):
    return st.median(xs) if xs else float("nan")


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return float("nan")
    mx, my = sum(xs) / n, sum(ys) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if sx == 0 or sy == 0:
        return float("nan")
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (sx * sy)


def pass1(rows):
    return {(r["set"], r["id"]): r for r in rows if r.get("pass") == 1}


def caught(row, label):
    if row is None or not row.get("valid_reply"):
        return None
    return bool([k for k, e in row["must_not"].items() if e["verdict"] == "present"]
                or [k for k, e in row["must"].items() if e["verdict"] == "contradicted"]
                or [1 for e in row["other_errors"] if quotes_overlap(e["quote"], label["quote"])])


def verdict_map(row):
    return {(kind, pid): e["verdict"] for kind in ("must", "must_not") for pid, e in row[kind].items()}


def hours(n_calls, seconds):
    return n_calls * seconds / CONCURRENCY / 3600


def build_report(results, labels, noeffect=(), omissions=None):
    """results: {setting: [result rows]} in the order to show. -> text."""
    omissions = READER_OMISSIONS if omissions is None else omissions
    sub = set(SUBSET)
    L, out = [], None
    out = L.append
    results = {**results, **{n: [] for n in noeffect if n not in results}}
    by = {name: pass1([r for r in rows if (r["set"], r["id"]) in sub]) for name, rows in results.items()}
    labs = [x for x in labels if (f"{x['question_set']}_{x['answer_set']}", f"{x['question_set']}-{x['question']}") in sub]
    oms = [o for o in omissions if tuple(o) in sub]
    out("# S5a: judge settings on 16 answers, pass 1")
    out("")
    out(f"Subset: {len(SUBSET)} answers, pass 1 only ({len(labs)} of the reader's labelled wrong items and {len(oms)} of the "
        f"reader's essential omissions fall in it). Calls ran {CONCURRENCY} in parallel. This report gives numbers and names no winner. "
        "It leans on `questions_real` and the holdout; it compares judging settings and is not a claim about the system. "
        "Sixteen answers is a small sample: a difference of one or two items is not a finding.")
    out("")
    out("## Per setting")
    out("")
    out("| setting | calls done | valid replies | median s | max s | median completion tokens (n) | median reasoning chars (n) | quote failures | mean S |")
    out("|---|---|---|---|---|---|---|---|---|")
    stats = {}
    for name, d in by.items():
        if name in noeffect:
            out(f"| {name} | switch had no effect | | | | | | | |")
            continue
        rows = list(d.values())
        valid = [r for r in rows if r.get("valid_reply")]
        secs = [r["seconds"] for r in rows if isinstance(r.get("seconds"), (int, float))]
        toks = [r["completion_tokens"] for r in rows if isinstance(r.get("completion_tokens"), (int, float))]
        reas = [r["reasoning_chars"] for r in rows if isinstance(r.get("reasoning_chars"), (int, float))]
        claims, failed = sum(r["quote_claims"] for r in valid), sum(r["quote_failed"] for r in valid)
        s = [r["S"] for r in valid]
        stats[name] = {"secs": secs, "median": median(secs), "mean": sum(secs) / len(secs) if secs else float("nan")}
        out(f"| {name} | {len(rows)} of {len(SUBSET)} | {len(valid)} | {fmt(median(secs), 0)} | {fmt(max(secs) if secs else float('nan'), 0)} "
            f"| {fmt(median(toks), 0)} ({len(toks)}) | {fmt(median(reas), 0) if reas else 'none in replies'} ({len(reas)}) "
            f"| {failed} of {claims} | {fmt(sum(s) / len(s) if s else float('nan'))} |")
    out("")
    out("Entries made before the cache recorded usage have no token or reasoning figures; n is the number of calls with a figure. "
        "Seconds of the first calls of a setting include the model load when the model had to be loaded.")
    if noeffect:
        out("")
        out("Switch had no effect: " + ", ".join(noeffect) + " (the first reply still showed thinking, so the remaining calls were not made).")
    live = [n for n in by if n not in noeffect and by[n]]
    out("")
    out("## Against the reader")
    out("")
    out(f"Labelled wrong items in the subset ({len(labs)}): caught = same answer has a must-not present, a must point contradicted, "
        "or an other_errors entry whose quote overlaps the label's quote. Essential omissions in the subset "
        f"({len(oms)}): marked = the answer's essential-miss flag is set.")
    out("")
    out("| setting | labelled wrong items caught | not judged | essential omissions marked | not judged |")
    out("|---|---|---|---|---|")
    for name in live:
        cs = [caught(by[name].get((f"{x['question_set']}_{x['answer_set']}", f"{x['question_set']}-{x['question']}")), x) for x in labs]
        ms = [(by[name].get(tuple(o)) or {}) for o in oms]
        mv = [m for m in ms if m.get("valid_reply")]
        out(f"| {name} | {sum(1 for c in cs if c)} of {len(labs)} | {sum(1 for c in cs if c is None)} "
            f"| {sum(1 for m in mv if m['essential_miss'])} of {len(oms)} | {len(oms) - len(mv)} |")
    out("")
    out("Labelled items in the subset: " + "; ".join(
        f"{x['question_set']}_{x['answer_set']} {x['question_set']}-{x['question']} ({norm_ws(x['quote'])[:40]})" for x in labs) + ".")
    out("")
    out("## Agreement between settings")
    out("")
    out("Over answers with a valid reply in both settings: share of point verdicts (must and must-not pooled; present, absent, "
        "contradicted) that are equal, Cohen's kappa over the same verdicts, and the Pearson correlation of S across the answers.")
    out("")
    out("| pair | answers | verdicts compared | equal | kappa | correlation of S |")
    out("|---|---|---|---|---|---|")
    for i, a in enumerate(live):
        for b in live[i + 1:]:
            keys = [k for k in by[a] if k in by[b] and by[a][k].get("valid_reply") and by[b][k].get("valid_reply")]
            pairs = []
            for k in keys:
                va, vb = verdict_map(by[a][k]), verdict_map(by[b][k])
                pairs += [(va[p], vb[p]) for p in va if p in vb]
            kappa, po, _, n, _ = cohen_kappa(pairs)
            r = pearson([by[a][k]["S"] for k in keys], [by[b][k]["S"] for k in keys])
            out(f"| {a} vs {b} | {len(keys)} | {n} | {fmt(100 * po, 1) + ' %' if n else 'n/a'} | {fmt(kappa)} | {fmt(r)} |")
    out("")
    out("## Projected judge time")
    out("")
    out(f"Calls divided by {CONCURRENCY} in parallel, at the setting's median and mean seconds per call measured here; "
        f"{FULL_PILOT_CALLS} calls = the full pilot (110 answers, two passes), {JUDGED_TIER_CALLS} calls = the judged tier "
        "(120 items, two passes) for one configuration. A projection from 16 calls, not a measurement of the full run.")
    out("")
    out("| setting | full pilot, median-based | full pilot, mean-based | judged tier, median-based | judged tier, mean-based |")
    out("|---|---|---|---|---|")
    for name in live:
        s = stats[name]
        out(f"| {name} | {fmt(hours(FULL_PILOT_CALLS, s['median']), 1)} h | {fmt(hours(FULL_PILOT_CALLS, s['mean']), 1)} h "
            f"| {fmt(hours(JUDGED_TIER_CALLS, s['median']), 1)} h | {fmt(hours(JUDGED_TIER_CALLS, s['mean']), 1)} h |")
    return "\n".join(L) + "\n"


def check_switch(cache, setting):
    """-> 0 thinking is off in the first entry of the setting, 1 it is still on, 2 no entry or no way to tell."""
    for c in rd(cache):
        if (c.get("setting") or "glm-think-12k") == setting:
            on = thinking_still_on(c)
            print({True: "thinking still on", False: "thinking off", None: "cannot tell"}[on])
            return {True: 1, False: 0, None: 2}[on]
    print("no entry for the setting")
    return 2


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("report")
    r.add_argument("--results", nargs="+", required=True, metavar="SETTING=PATH", help="in the order to show; a missing file is an empty setting")
    r.add_argument("--labels", default="runs/m4/reports/wp2a_labels_checked.jsonl")
    r.add_argument("--noeffect", nargs="*", default=[], help="settings whose switch had no effect")
    c = sub.add_parser("check-switch")
    c.add_argument("--cache", required=True)
    c.add_argument("--setting", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "check-switch":
        return check_switch(a.cache, a.setting)
    results = {}
    for spec in a.results:
        name, _, path = spec.partition("=")
        results[name] = rd(path) if path and Path(path).exists() else []
    sys.stdout.write(build_report(results, rd(a.labels), a.noeffect))
    return 0


if __name__ == "__main__":
    sys.exit(main())
