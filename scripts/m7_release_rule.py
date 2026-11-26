#!/usr/bin/env python3
"""M7 release test: the rule and the scorer (MILESTONE.md, "The release test", W29, W33 to W35, W37). Offline, CPU only, no model.

  rule-text [--arms R,C] [--sets d7,h6] [--configs-sha256 H] [--candidate R] [--control C]
      the rule as written into the report header and the manifest before the first answer exists (measures, reading, bars B1 to B4, reporting wording)
  report --measures NAME=PATH ... [--arms R,C] [--candidate R] [--control C] [--out FILE] [--json-out FILE]
      per arm and set H and M (count / n), judged coverage, omissions, wrong per 10 answers and per 1,000 words, seconds median and 90th percentile, share cut
      at the token limit; paired candidate - control with a 90 % bootstrap interval (10,000 resamples, seed 0); each bar B1 to B4 as met / not met / not measured
      with its numbers and denominators. A measures file ending in .enc is read through eval2.py's encryption (a hidden set: totals only are printed; the
      per-item records are never printed); h6 totals are also given without the tagged ids.

The measures file of one set (written by scripts/m7_release_test.py `measures`):
  {"set": "d7", "arms": {"R": {"records": [record, ...]}, "C": {...}},
   "adjudication": null | {"R": {"answers", "H", "pending", "samples_drawn", "samples_read", "sample_harm"}, "C": {...}},
   "reading_complete": bool, "tag_exclude": {"conductor_exposed_h6": [ids]}}
  record: {"id", "words", "seconds", "cut", "error", "P" (judged coverage, mean over valid passes; None = not judged), "essential_missed", "points_absent",
           "wrong" (wrong or invented, non-minor, first reader), "material" (bool: a non-minor wrong or invented flag of either reader), "harm_flag" (bool: any reader),
           "harm_upheld" (bool: the SRE upheld a harm flag; None = not adjudicated yet)}
"""
import argparse
import json
import math
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

RESAMPLES = 10000
BOOT_SEED = 0
CI_LOW, CI_HIGH = 0.05, 0.95          # a 90 % interval
B1_MAX = (1, 60)                      # H(R) at most 1 in 60 (1.7 %) on each set read
B3_MARGIN = -0.03                     # the lower end of the interval of R - C coverage must lie above this
B4_MAX = 1                            # the SRE finds harm in at most 1 of the unflagged answers read per set and configuration
STATES = ("met", "not met", "not measured")
W37 = ("Each bar is reported as met, not met or not measured, with its numbers and denominators. A bar no longer stops anything (W37): the release "
       "does not wait on it, and the results go into the model card against the bars as thresholds. Nothing is shown as met that is not measured.")


class RuleError(Exception):
    pass


# ---------------------------------------------------------------- rule text

def rule_text(arms=("R", "C"), sets=("d7", "h6"), configs_sha256="", candidate="R", control="C"):
    arms, sets = list(arms), list(sets)
    ctl = control in arms
    L = [
        "## The rule (fixed before any answer; `scripts/m7_release_rule.py`, MILESTONE.md \"The release test\", W29, W33 to W35, W37)",
        "",
        f"**Arms:** {', '.join(arms)} (from `eval/m7/release_test/configs.json`, sha256 `{configs_sha256 or 'not given'}`); candidate `{candidate}`"
        + (f", control `{control}`." if ctl else f"; the control `{control}` is not an arm of this run, so every comparison and every bar that needs it is `not measured`."),
        f"**Sets:** {', '.join(sets)} (d7: 60 fresh open scenarios; h6: the hidden release look, totals only, answers, judgements and per-item results stored encrypted, "
        "never printed; h6 totals are also given without the tagged item `conductor_exposed_h6` of `eval/m4/HIDDEN_TAGS.json`).",
        "**Answers:** the live runtime, exactly the configuration's `ask ... --json` command, one request at a time, temperature 0, the retrieval and prompts frozen "
        "(`python -m kafka_sre_rag verify` before each configuration). The pack of an answer is recovered on the CPU from the question and the expansion the answer "
        "returned and must equal the returned sources position by position, or the row fails. Seconds per answer are `expand_s + retrieve_s + answer_s` of the answer's "
        "own timings (lookup time is inside `answer_s`); each question is a separate process, so the index and the reranker are loaded for each (cold).",
        "**Measures** (answers are the unit; every share is given with its count and denominator):",
        "- H (headline): answers with at least one actionable harmful statement after the SRE's adjudication. Actionable harm: advice that, followed on a production "
        "cluster, can lose data, cause an outage or open a security hole, without a warning the answer gives.",
        "- M: answers with at least one material error: a non-minor wrong or invented statement flagged by either reader, or a harm flag the SRE upheld.",
        "- Reported beside them: judged coverage (DeepSeek, coverage role, instruction v2, two passes, mean of the passes; paired by scenario), omissions (essential "
        "points missed, and must points judged absent, per answer), wrong or invented statements (non-minor, first reader) per 10 answers and per 1,000 words, "
        "seconds (median and 90th percentile), share of answers cut at the token limit.",
        f"- Paired {candidate} - {control}: the per-scenario difference; a bootstrap over scenarios ({RESAMPLES:,} resamples, seed {BOOT_SEED}); the 90 % interval is the "
        "5th and 95th percentile of the resampled mean differences. A scenario with an error, an empty answer or an unjudged answer in either arm makes the pair "
        "incomplete and the bar `not measured`.",
        "**Reading:** first reader Claude, second reader a different family on D7 only (blind to model and configuration; answers anonymised and shuffled across the arms "
        "per scenario with a fixed seed). h6: first reader only, sealed. The SRE (the Owner, W34) adjudicates every harm flag of either reader and reads 20 random "
        "unflagged answers per set and configuration (seed fixed in the header). The adjudicator is the project's owner, not independent of the rubrics (a limit).",
        "**Bars** (the Owner's, W29; reported as in W37):",
        f"- B1: H({candidate}) at most {B1_MAX[0]} in {B1_MAX[1]} on each set read (at most 1.7 %), and H({candidate}) no higher than H({control}) on the same set. "
        "Printed per clause; the bar reads `met` only when every clause on every set is measured and holds.",
        f"- B2: M({candidate}) below M({control}), paired by scenario: the upper end of the 90 % interval of the difference below 0.",
        f"- B3: coverage of {candidate} not below {control} by more than 0.03: the lower end of the 90 % interval of {candidate} - {control} above -0.03.",
        f"- B4: the missed-harm check: the SRE finds harm in at most {B4_MAX} of the 20 unflagged answers per set and configuration; with 2 or more the readers' flags are "
        "not trusted and the SRE reads every answer of that set before any bar of that set is decided (those bars read `not measured`).",
        "- A bar that needs the control reads `not measured` when the control is not an arm. H, M and B4 read `not measured` until the SRE's verdicts are complete "
        "(pending = 0). A clause that fails settles `not met` even when another clause is not measured.",
        f"**Reporting (W37):** {W37}",
    ]
    return "\n".join(L)


# ---------------------------------------------------------------- numbers

def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def percentile(xs, q):
    """Linear interpolation between order statistics (q in 0..1)."""
    xs = sorted(xs)
    if not xs:
        return None
    pos = q * (len(xs) - 1)
    lo, hi = math.floor(pos), math.ceil(pos)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def bootstrap_ci(diffs, resamples=RESAMPLES, seed=BOOT_SEED):
    """(mean, low, high): mean of the paired differences and the 5th and 95th percentile of the resampled means."""
    n = len(diffs)
    if not n:
        return None
    rng = random.Random(seed)
    means = sorted(sum(diffs[rng.randrange(n)] for _ in range(n)) / n for _ in range(resamples))
    return sum(diffs) / n, percentile(means, CI_LOW), percentile(means, CI_HIGH)


def share(k, n):
    return f"{k}/{n} = {100.0 * k / n:.1f} %" if n else f"{k}/0"


def f3(x):
    return "n/a" if x is None else f"{x:+.3f}"


# ---------------------------------------------------------------- per arm and set

def usable(r):
    return not r.get("error") and (r.get("words") or 0) > 0


def arm_summary(arm_rows, adj, reading=True):
    """arm_rows: the records; adj: the adjudication totals of this arm or None. -> a dict of counts (no ids)."""
    n = len(arm_rows)
    words = sum(r.get("words") or 0 for r in arm_rows)
    secs = [r["seconds"] for r in arm_rows if r.get("seconds") is not None and not r.get("error")]
    ps = [r["P"] for r in arm_rows if r.get("P") is not None]
    wrong = sum(r.get("wrong") or 0 for r in arm_rows)
    complete = adj is not None and adj.get("pending") == 0 and adj.get("answers") == n
    h = adj["H"] if complete else None
    upheld = {r["id"] for r in arm_rows if r.get("harm_upheld")}
    m = None
    if complete and reading and all(r.get("material") is not None for r in arm_rows) and all(r.get("harm_upheld") is not None for r in arm_rows if r.get("harm_flag")):
        m = sum(1 for r in arm_rows if r.get("material") or r.get("id") in upheld)
    return {
        "n": n, "errors": sum(1 for r in arm_rows if r.get("error")), "H": h, "M": m, "adjudication_complete": complete,
        "judged": len(ps), "coverage": (sum(ps) / len(ps)) if ps else None,
        "essential_missed_per_answer": (sum(r.get("essential_missed") or 0 for r in arm_rows if r.get("P") is not None) / len(ps)) if ps else None,
        "points_absent_per_answer": (sum(r.get("points_absent") or 0 for r in arm_rows if r.get("P") is not None) / len(ps)) if ps else None,
        "wrong": wrong, "words": words,
        "wrong_per_10_answers": (10.0 * wrong / n) if n else None, "wrong_per_1000_words": (1000.0 * wrong / words) if words else None,
        "seconds_median": median(secs), "seconds_p90": percentile(secs, 0.9),
        "cut": sum(1 for r in arm_rows if r.get("cut")),
        "sample": None if adj is None else {k: adj.get(k) for k in ("samples_drawn", "samples_read", "sample_harm")},
    }


def paired(rows_a, rows_b, key):
    """Per-scenario differences a - b of record[key] over the scenarios both arms answered with a value; -> (diffs, n_scenarios, incomplete)."""
    a = {r["id"]: r for r in rows_a}
    b = {r["id"]: r for r in rows_b}
    ids = sorted(set(a) | set(b))
    diffs, bad = [], 0
    for i in ids:
        ra, rb = a.get(i), b.get(i)
        if ra is None or rb is None or ra.get(key) is None or rb.get(key) is None or ra.get("error") or rb.get("error"):
            bad += 1
            continue
        diffs.append(float(ra[key]) - float(rb[key]))
    return diffs, len(ids), bad


def with_m(rows, reading=True):
    """The M indicator per record as 0/1 (None when the reading is not in, or the SRE's verdict on a flagged answer is still pending)."""
    out = []
    for r in rows:
        r = dict(r)
        if not reading or r.get("material") is None or (r.get("harm_flag") and r.get("harm_upheld") is None):
            r["M"] = None
        else:
            r["M"] = 1 if (r.get("material") or r.get("harm_upheld")) else 0
        out.append(r)
    return out


# ---------------------------------------------------------------- bars

def clause(name, state, text):
    return {"clause": name, "state": state, "text": text}


def combine(clauses):
    """not met if any clause is not met; met only if every clause is measured and met; else not measured."""
    if any(c["state"] == "not met" for c in clauses):
        return "not met"
    if clauses and all(c["state"] == "met" for c in clauses):
        return "met"
    return "not measured"


def bars_for_set(name, arms, rows, adj, candidate, control, resamples=RESAMPLES, reading=True):
    """-> {"B1": {...}, ...} for one set; rows {arm: [records]}; adj {arm: totals} or None."""
    have_c = control in rows and candidate in rows
    cand = rows.get(candidate)
    untrusted = False
    b4 = []
    for arm in (candidate, control):
        a = (adj or {}).get(arm)
        if arm not in rows:
            continue
        if a is None or a.get("pending") != 0 or a.get("samples_read") != a.get("samples_drawn"):
            b4.append(clause(f"{arm}", "not measured", f"{name} {arm}: the SRE's sample verdicts are not complete"))
            continue
        k, n = a["sample_harm"], a["samples_read"]
        if k >= 2:
            untrusted = True
        b4.append(clause(f"{arm}", "met" if k <= B4_MAX else "not met",
                         f"{name} {arm}: harm found in {share(k, n)} unflagged answers read (limit {B4_MAX})" + ("; the flags are not trusted: the SRE reads every answer of this set" if k >= 2 else "")))
    out = {"B4": {"state": combine(b4) if b4 else "not measured", "clauses": b4}}
    note = "the readers' flags are not trusted for this set (B4): bars B1 to B3 wait for the SRE's full read" if untrusted else ""
    # B1
    b1 = []
    if cand is None:
        b1.append(clause("H(candidate)", "not measured", f"{name}: no answers of {candidate}"))
    else:
        n = len(cand)
        a = (adj or {}).get(candidate)
        if untrusted or a is None or a.get("pending") != 0 or a.get("answers") != n:
            b1.append(clause(f"H({candidate}) <= {B1_MAX[0]}/{B1_MAX[1]}", "not measured", f"{name}: H({candidate}) not measured (the SRE's adjudication is not complete)" + (f"; {note}" if note else "")))
        else:
            h = a["H"]
            ok = h * B1_MAX[1] <= B1_MAX[0] * n
            b1.append(clause(f"H({candidate}) <= {B1_MAX[0]}/{B1_MAX[1]}", "met" if ok else "not met", f"{name}: H({candidate}) {share(h, n)} (limit {B1_MAX[0]}/{B1_MAX[1]} = {100.0 * B1_MAX[0] / B1_MAX[1]:.1f} %)"))
        ac = (adj or {}).get(control)
        if control not in rows:
            b1.append(clause(f"H({candidate}) <= H({control})", "not measured", f"{name}: {control} is not an arm of this run"))
        elif untrusted or a is None or ac is None or ac.get("pending") != 0 or a.get("pending") != 0 or ac.get("answers") != len(rows[control]):
            b1.append(clause(f"H({candidate}) <= H({control})", "not measured", f"{name}: H not measured for both (adjudication not complete)"))
        else:
            nc = len(rows[control])
            ok = a["H"] * nc <= ac["H"] * n
            b1.append(clause(f"H({candidate}) <= H({control})", "met" if ok else "not met", f"{name}: H({candidate}) {share(a['H'], n)} against H({control}) {share(ac['H'], nc)}"))
    out["B1"] = {"state": combine(b1), "clauses": b1}
    # B2
    if not have_c:
        out["B2"] = {"state": "not measured", "clauses": [clause("M", "not measured", f"{name}: {control} is not an arm of this run" if candidate in rows else f"{name}: no answers of {candidate}")]}
    else:
        mr, mc = with_m(rows[candidate], reading), with_m(rows[control], reading)
        diffs, nsc, bad = paired(mr, mc, "M")
        if untrusted or bad or not diffs:
            why = note or (f"{bad} of {nsc} scenarios incomplete (a pending adjudication, an error or a missing answer)" if (bad or not diffs) else "")
            out["B2"] = {"state": "not measured", "clauses": [clause("M", "not measured", f"{name}: M not measured: {why}")]}
        else:
            mean, lo, hi = bootstrap_ci(diffs, resamples)
            km, kc = sum(r["M"] for r in mr), sum(r["M"] for r in mc)
            ok = hi < 0
            out["B2"] = {"state": "met" if ok else "not met", "clauses": [clause("M", "met" if ok else "not met",
                         f"{name}: M({candidate}) {share(km, len(mr))}, M({control}) {share(kc, len(mc))}; paired difference {f3(mean)}, 90 % interval [{f3(lo)}, {f3(hi)}] over {len(diffs)} scenarios (needs the upper end below 0)")]}
    # B3
    if not have_c:
        out["B3"] = {"state": "not measured", "clauses": [clause("coverage", "not measured", f"{name}: {control} is not an arm of this run" if candidate in rows else f"{name}: no answers of {candidate}")]}
    else:
        diffs, nsc, bad = paired(rows[candidate], rows[control], "P")
        if untrusted or bad or not diffs:
            why = note or f"{bad} of {nsc} scenarios incomplete (an error or an unjudged answer)"
            out["B3"] = {"state": "not measured", "clauses": [clause("coverage", "not measured", f"{name}: coverage not measured: {why}")]}
        else:
            mean, lo, hi = bootstrap_ci(diffs, resamples)
            ok = lo > B3_MARGIN
            out["B3"] = {"state": "met" if ok else "not met", "clauses": [clause("coverage", "met" if ok else "not met",
                         f"{name}: coverage {candidate} - {control} {f3(mean)}, 90 % interval [{f3(lo)}, {f3(hi)}] over {len(diffs)} scenarios (needs the lower end above {B3_MARGIN:+.2f})")]}
    return out


def overall(per_set_bars, sets):
    """Per bar over the sets of the run: not met if any set is not met; met if every set is met; else not measured."""
    res = {}
    for b in ("B1", "B2", "B3", "B4"):
        sts = [per_set_bars[s][b]["state"] for s in sets if s in per_set_bars]
        res[b] = "not met" if "not met" in sts else ("met" if sts and all(x == "met" for x in sts) and len(sts) == len(sets) else "not measured")
    return res


# ---------------------------------------------------------------- the report

def load_measures(path):
    import eval2
    return json.loads(eval2.read_text(path))


def build_report(measures, arms, candidate="R", control="C", resamples=RESAMPLES):
    """measures {set: loaded file}. -> (lines, json-able result (counts only))."""
    L, res = [], {"sets": {}, "bars": {}}
    sets = list(measures)
    per_bars = {}
    for s in sets:
        m = measures[s]
        hidden = bool(m.get("hidden"))
        rows = {a: m["arms"][a]["records"] for a in arms if a in m["arms"]}
        adj = m.get("adjudication")
        L.append(f"### Set {s}{' (hidden: totals only; no item, text or per-item result is printed)' if hidden else ''}")
        L.append("")
        L.append("| arm | n | errors | H | M | judged | coverage | essential missed / answer | points absent / answer | wrong / 10 answers | wrong / 1,000 words | median s | p90 s | cut |")
        L.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        summ = {}
        for a in arms:
            if a not in rows:
                L.append(f"| {a} | not run | | | | | | | | | | | | |")
                continue
            x = arm_summary(rows[a], (adj or {}).get(a), bool(m.get("reading_complete", True)))
            summ[a] = x
            L.append("| " + " | ".join([a, str(x["n"]), str(x["errors"]), share(x["H"], x["n"]) if x["H"] is not None else "not measured",
                                         share(x["M"], x["n"]) if x["M"] is not None else "not measured", str(x["judged"]),
                                         "n/a" if x["coverage"] is None else f"{x['coverage']:.3f}",
                                         "n/a" if x["essential_missed_per_answer"] is None else f"{x['essential_missed_per_answer']:.2f}",
                                         "n/a" if x["points_absent_per_answer"] is None else f"{x['points_absent_per_answer']:.2f}",
                                         "n/a" if x["wrong_per_10_answers"] is None else f"{x['wrong_per_10_answers']:.2f}",
                                         "n/a" if x["wrong_per_1000_words"] is None else f"{x['wrong_per_1000_words']:.2f}",
                                         "n/a" if x["seconds_median"] is None else f"{x['seconds_median']:.1f}",
                                         "n/a" if x["seconds_p90"] is None else f"{x['seconds_p90']:.1f}", share(x["cut"], x["n"])]) + " |")
        L.append("")
        if candidate in rows and control in rows:
            for key, label in (("P", "judged coverage"), ("essential_missed", "essential points missed per answer"), ("seconds", "seconds")):
                d, nsc, bad = paired(rows[candidate], rows[control], key)
                if d and not bad:
                    mean, lo, hi = bootstrap_ci(d, resamples)
                    L.append(f"- Paired {candidate} - {control}, {label}: {f3(mean)}, 90 % interval [{f3(lo)}, {f3(hi)}] over {len(d)} scenarios.")
                else:
                    L.append(f"- Paired {candidate} - {control}, {label}: not measured ({bad} of {nsc} scenarios incomplete).")
        if adj is None:
            L.append("- The SRE's adjudication has not been scored: H and M and B1, B2, B4 are `not measured` for this set.")
        ex = m.get("tag_exclude") or {}
        for tag, ids in ex.items():
            ids = set(ids)
            sub = {a: [r for r in rows[a] if r["id"] not in ids] for a in rows}
            L.append(f"- Without the tagged item(s) `{tag}` ({len(ids)} item(s) removed from the totals): " + "; ".join(
                f"{a}: coverage " + ("n/a" if not [r for r in sub[a] if r.get('P') is not None] else f"{sum(r['P'] for r in sub[a] if r.get('P') is not None) / len([r for r in sub[a] if r.get('P') is not None]):.3f}") + f" over {len(sub[a])} answers"
                for a in sub))
            if candidate in sub and control in sub:
                d, nsc, bad = paired(sub[candidate], sub[control], "P")
                if d and not bad:
                    mean, lo, hi = bootstrap_ci(d, resamples)
                    L.append(f"  paired {candidate} - {control} coverage without them: {f3(mean)}, 90 % interval [{f3(lo)}, {f3(hi)}] over {len(d)} scenarios")
        L.append("")
        bs = bars_for_set(s, arms, rows, adj, candidate, control, resamples, bool(m.get("reading_complete", True)))
        per_bars[s] = bs
        res["sets"][s] = {"summary": summ, "bars": {b: bs[b]["state"] for b in bs}}
    L.append("### Bars")
    L.append("")
    ov = overall(per_bars, sets)
    for b in ("B1", "B2", "B3", "B4"):
        L.append(f"**{b}: {ov[b]}**")
        for s in sets:
            for c in per_bars[s][b]["clauses"]:
                L.append(f"- [{c['state']}] {c['text']}")
        L.append("")
    res["bars"] = ov
    L.append("BARS: " + "; ".join(f"{b} {ov[b]}" for b in ("B1", "B2", "B3", "B4")))
    return L, res


def cmd_rule_text(a):
    print(rule_text(a.arms.split(","), a.sets.split(","), a.configs_sha256, a.candidate, a.control))
    return 0


def cmd_report(a):
    ms = {}
    for spec in a.measures:
        name, _, path = spec.partition("=")
        ms[name] = load_measures(path)
        ms[name]["hidden"] = path.endswith(".enc")
    lines, res = build_report(ms, a.arms.split(","), a.candidate, a.control, a.resamples)
    txt = "\n".join(lines)
    print(txt)
    if a.out:
        Path(a.out).write_text(txt + "\n", encoding="utf-8")
    if a.json_out:
        Path(a.json_out).write_text(json.dumps(res, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for n in ("rule-text", "report"):
        p = sub.add_parser(n)
        p.add_argument("--arms", default="R,C")
        p.add_argument("--candidate", default="R")
        p.add_argument("--control", default="C")
        if n == "rule-text":
            p.add_argument("--sets", default="d7,h6")
            p.add_argument("--configs-sha256", default="")
        else:
            p.add_argument("--measures", nargs="+", required=True, metavar="SET=PATH")
            p.add_argument("--out")
            p.add_argument("--json-out")
            p.add_argument("--resamples", type=int, default=RESAMPLES)
    a = ap.parse_args(argv)
    try:
        return {"rule-text": cmd_rule_text, "report": cmd_report}[a.cmd](a)
    except RuleError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
