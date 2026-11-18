#!/usr/bin/env python3
"""Rubric pilot report (agents/m4-evaluation.md, "Rubric pilot"): the four go or no-go criteria and the extras.

  scripts/eval2_pilot_report.py --results work/m4/pilot/judge_results.jsonl > runs/m4/S5_JUDGE_PILOT.md

--results is the --out file of scripts/eval2_judge.py (to rebuild it from a judge cache without calls use
`eval2_judge.py run ... --rescore`). Answer sets are named "<question set>_<answer set>" (real_default, real_strict,
real_v2_own_terms, real_v2_base_excerpts, holdout_default, holdout_strict) and "generic" for the generic-answer probe;
the generic set is left out of all four criteria and of the statistics.

The four criteria (all four must hold):
  1. Cohen's kappa between the two passes over all per-point verdicts (present / absent / contradicted, must and
     must-not pooled; answers with a valid reply in both passes) is at least 0.7.
  2. ICC of the item score S between the passes is higher than the ICC of the old 1 to 5 mark between its two passes
     on the same answers. Form: ICC(2,1), two-way random effects, absolute agreement, single measure (Shrout and
     Fleiss), k = 2 passes. Old marks come from work/v3/judge/results_h2h_*_p1.jsonl and _p2.jsonl: an answer's text is
     looked up on the base side and on the tuned side (score_base, score_tuned; compare.py already maps the flipped
     pass back), the first file in sorted order that has a mark in both passes is used; an answer without an old mark
     in both passes is left out of this comparison and counted.
  3. At least 80 % of the reader's essential omissions are marked absent on an essential point (the essential-miss flag),
     taken over omissions x valid passes. The omissions are read from the three v3 reading reports by hand and listed
     in READER_OMISSIONS below (override with --omissions JSONL of {"set","id"}).
  4. At most 5 % of the verdicts "present" or "contradicted" have a quote that is not found in the answer.
"""
import argparse
import difflib
import json
import math
import re
import statistics as st
import sys
from collections import Counter, OrderedDict, defaultdict
from pathlib import Path

# Essential omissions found by the reader (runs/v3/reports/v2rag_invention_reading.md, per-question table, and
# strict_mode_invention_reading.md, "Strict drops an essential step"). The holdout reading found none ("Essential
# omissions 0"; partial omissions are not counted here).
READER_OMISSIONS = [
    ("real_v2_own_terms", "real-1"), ("real_v2_own_terms", "real-3"), ("real_v2_own_terms", "real-26"),
    ("real_v2_own_terms", "real-36"),
    ("real_v2_base_excerpts", "real-1"), ("real_v2_base_excerpts", "real-3"), ("real_v2_base_excerpts", "real-18"),
    ("real_strict", "real-3"), ("real_strict", "real-36"),
]
THRESHOLD_KAPPA, THRESHOLD_OMISSION, THRESHOLD_QUOTES = 0.7, 0.8, 0.05
MDE_FACTOR = 2.80


def rd(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def norm_ws(s):
    return re.sub(r"\s+", " ", s or "").strip()


# ---------------------------------------------------------------- statistics

def cohen_kappa(pairs):
    """pairs of category labels -> (kappa, observed agreement, expected agreement, n, confusion Counter)."""
    n = len(pairs)
    if not n:
        return float("nan"), float("nan"), float("nan"), 0, Counter()
    conf = Counter(pairs)
    po = sum(c for (a, b), c in conf.items() if a == b) / n
    cats = {a for a, _ in pairs} | {b for _, b in pairs}
    pe = sum((sum(c for (a, _), c in conf.items() if a == k) / n) * (sum(c for (_, b), c in conf.items() if b == k) / n)
             for k in cats)
    kappa = float("nan") if pe == 1 else (po - pe) / (1 - pe)
    return kappa, po, pe, n, conf


def icc_2_1(pairs):
    """ICC(2,1) for n subjects rated twice: (MSR - MSE) / (MSR + (k-1) MSE + k (MSC - MSE) / n)."""
    n, k = len(pairs), 2
    if n < 2:
        return float("nan")
    gm = sum(a + b for a, b in pairs) / (n * k)
    rows = [(a + b) / k for a, b in pairs]
    cols = [sum(a for a, _ in pairs) / n, sum(b for _, b in pairs) / n]
    ssr = k * sum((m - gm) ** 2 for m in rows)
    ssc = n * sum((m - gm) ** 2 for m in cols)
    sst = sum((x - gm) ** 2 for p in pairs for x in p)
    sse = sst - ssr - ssc
    msr, msc, mse = ssr / (n - 1), ssc / (k - 1), sse / ((n - 1) * (k - 1))
    den = msr + (k - 1) * mse + k * (msc - mse) / n
    return float("nan") if den == 0 else (msr - mse) / den


def sd(xs):
    return st.stdev(xs) if len(xs) > 1 else float("nan")


def mde(sd_d, n):
    return MDE_FACTOR * sd_d / math.sqrt(n)


def quotes_overlap(a, b):
    """Two quotes overlap if one contains the other, or share a common run of >= 25 characters or >= 60 % of the shorter."""
    a, b = norm_ws(a).lower(), norm_ws(b).lower()
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    m = difflib.SequenceMatcher(None, a, b, autojunk=False).find_longest_match(0, len(a), 0, len(b))
    return m.size >= min(25, len(a), len(b)) or m.size >= 0.6 * min(len(a), len(b))


def fmt(x, d=2):
    return "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.{d}f}"


def pf(ok):
    return "PASS" if ok else "FAIL"


# ---------------------------------------------------------------- old marks

def old_marks(old_dir, answers):
    """answers: [(set, id, question, text)]. -> ({(set, id): (mark_p1, mark_p2, file)}, files used Counter, missing list)."""
    d = Path(old_dir)
    sides = []  # per file: {(question, text): (p1, p2)}
    for p1 in sorted(d.glob("results_h2h_*_p1.jsonl")):
        p2 = p1.with_name(p1.name.replace("_p1.jsonl", "_p2.jsonl"))
        if not p2.exists():
            continue
        m = {}
        for r1, r2 in zip(rd(p1), rd(p2)):
            if r1.get("question") != r2.get("question"):
                continue
            for side, key in (("base", "score_base"), ("tuned", "score_tuned")):
                if isinstance(r1.get(key), (int, float)) and isinstance(r2.get(key), (int, float)) and r1.get(side):
                    m[(norm_ws(r1["question"]), norm_ws(r1[side]))] = (r1[key], r2[key])
        sides.append((p1.name.replace("_p1.jsonl", "").replace("results_", ""), m))
    out, used, missing = {}, Counter(), []
    for s, i, q, t in answers:
        for name, m in sides:
            hit = m.get((norm_ws(q), norm_ws(t)))
            if hit:
                out[(s, i)] = (hit[0], hit[1], name)
                used[name] += 1
                break
        else:
            missing.append((s, i))
    return out, used, missing


# ---------------------------------------------------------------- report

def mean_s(rows, key="S"):
    v = [r[key] for r in rows]
    return sum(v) / len(v) if v else float("nan")


def build_report(results, rubrics, labels, old_dir, omissions=None):
    L = []
    out = L.append
    omissions = READER_OMISSIONS if omissions is None else omissions
    rub = {r["id"]: r for r in rubrics}
    all_rows = [r for r in results]
    generic = [r for r in all_rows if r["set"] == "generic"]
    rows = [r for r in all_rows if r["set"] != "generic"]
    invalid = [r for r in all_rows if not r["valid_reply"]]
    valid = [r for r in rows if r["valid_reply"]]
    by = defaultdict(dict)  # (set, id) -> {pass: row}
    for r in valid:
        by[(r["set"], r["id"])][r["pass"]] = r
    both = {k: v for k, v in by.items() if 1 in v and 2 in v}
    sets = list(OrderedDict.fromkeys(r["set"] for r in rows))

    out("# Rubric pilot: judge step results")
    out("")
    out(f"Judgments: {len(all_rows)} ({len(rows)} on the six answer sets, {len(generic)} on the generic probe); "
        f"{len(invalid)} without a valid JSON reply (left out). Answers judged in both passes: {len(both)} of "
        f"{len({(r['set'], r['id']) for r in rows})}.")
    out("Answers per set (valid in both passes): " + ", ".join(
        f"{s} {sum(1 for k in both if k[0] == s)}" for s in sets) + ".")
    out("")

    # ---- 1. kappa
    pairs, by_kind = [], defaultdict(list)
    for k, v in both.items():
        for kind in ("must", "must_not"):
            for pid in v[1][kind]:
                pr = (v[1][kind][pid]["verdict"], v[2][kind][pid]["verdict"])
                pairs.append(pr)
                by_kind[kind].append(pr)
    kappa, po, pe, n, conf = cohen_kappa(pairs)
    c1 = not math.isnan(kappa) and kappa >= THRESHOLD_KAPPA
    out("## The four criteria")
    out("")
    out(f"1. **Per-point agreement between passes: kappa = {fmt(kappa)}** (threshold at least {THRESHOLD_KAPPA}): **{pf(c1)}**")
    out(f"   - {n} point verdicts pairs (must and must-not pooled, three categories); observed agreement {fmt(po, 3)}, "
        f"expected by chance {fmt(pe, 3)}.")
    for kind in ("must", "must_not"):
        k2 = cohen_kappa(by_kind[kind])
        out(f"   - {kind}: kappa {fmt(k2[0])}, {k2[3]} pairs, agreement {fmt(k2[1], 3)}.")
    cats = ["present", "absent", "contradicted"]
    out("   - counts (rows pass 1, columns pass 2):")
    out("")
    out("     | pass 1 \\ pass 2 | " + " | ".join(cats) + " |")
    out("     |---|" + "---|" * len(cats))
    for a in cats:
        out(f"     | {a} | " + " | ".join(str(conf.get((a, b), 0)) for b in cats) + " |")
    out("")

    # ---- 2. ICC
    olds, used, missing = old_marks(old_dir, [(r["set"], r["id"], r["question"], r["answer"]) for r in valid if r["pass"] == 1])
    matched = [k for k in both if k in olds]
    s_pairs_all = [(v[1]["S"], v[2]["S"]) for v in both.values()]
    s_pairs = [(both[k][1]["S"], both[k][2]["S"]) for k in matched]
    o_pairs = [(olds[k][0], olds[k][1]) for k in matched]
    icc_s, icc_o = icc_2_1(s_pairs), icc_2_1(o_pairs)
    c2 = not math.isnan(icc_s) and not math.isnan(icc_o) and icc_s > icc_o
    out(f"2. **Reliability between passes, S against the old mark: ICC(2,1) of S = {fmt(icc_s)}, of the 1 to 5 mark = "
        f"{fmt(icc_o)}** on {len(matched)} answers (S must be higher): **{pf(c2)}**")
    out(f"   - Form: ICC(2,1), two-way random effects, absolute agreement, single measure, two passes. Same answers for both.")
    out(f"   - Old marks: `work/v3/judge/results_h2h_*_p1.jsonl` with `_p2.jsonl`; the answer text is looked up on the base "
        f"and the tuned side (`score_base`, `score_tuned`), first file in sorted order with a mark in both passes. "
        f"{len(matched)} of {len(both)} judged answers matched; {len([k for k in both if k not in olds])} have no old mark in "
        f"both passes and are left out. Files used (answers): "
        + (", ".join(f"{n} {c}" for n, c in sorted(used.items())) or "none") + ".")
    out(f"   - For reference, ICC(2,1) of S on all {len(s_pairs_all)} judged answers: {fmt(icc_2_1(s_pairs_all))}.")
    out(f"   - Mean absolute pass-to-pass difference: S {fmt(sum(abs(a - b) for a, b in s_pairs) / len(s_pairs) if s_pairs else float('nan'))}"
        f" (scale about 0 to 1), old mark {fmt(sum(abs(a - b) for a, b in o_pairs) / len(o_pairs) if o_pairs else float('nan'))} (scale 1 to 5).")
    out("")

    # ---- 3. omissions
    trials = []
    for s, i in omissions:
        for p in (1, 2):
            r = by.get((s, i), {}).get(p)
            if r is not None:
                trials.append((s, i, p, r["essential_miss"]))
    absent_n = sum(1 for t in trials if t[3])
    share = absent_n / len(trials) if trials else float("nan")
    c3 = not math.isnan(share) and share >= THRESHOLD_OMISSION
    out(f"3. **Reader's essential omissions marked absent on an essential point: {fmt(100 * share, 1)} %** "
        f"({absent_n} of {len(trials)} judgments; threshold at least {int(100 * THRESHOLD_OMISSION)} %): **{pf(c3)}**")
    out(f"   - {len(omissions)} omissions listed by the reader (answer set, question), two passes each; judged in pilot results: "
        f"{len({(t[0], t[1]) for t in trials})}.")
    for s, i in omissions:
        ps = [by.get((s, i), {}).get(p) for p in (1, 2)]
        out(f"   - {s} {i}: " + ", ".join(f"p{p} " + ("not judged" if r is None else ("absent" if r['essential_miss'] else "NOT marked")) for p, r in zip((1, 2), ps)))
    out("")

    # ---- 4. quotes
    claims = sum(r["quote_claims"] for r in valid)
    failed = sum(r["quote_failed"] for r in valid)
    qshare = failed / claims if claims else float("nan")
    c4 = not math.isnan(qshare) and qshare <= THRESHOLD_QUOTES
    out(f"4. **Quotes that failed verification: {fmt(100 * qshare, 1)} %** ({failed} of {claims} present or contradicted "
        f"verdicts, all judgments on the six sets; threshold at most {int(100 * THRESHOLD_QUOTES)} %): **{pf(c4)}**")
    kinds = Counter(e.split(":")[0] for r in valid for e in r["judge_errors"])
    out("   - other judge errors: " + (", ".join(f"{k} {c}" for k, c in sorted(kinds.items())) or "none") + ".")
    out("")
    go = c1 and c2 and c3 and c4
    out(f"**Go or no-go: {'GO' if go else 'NO-GO'}** (criteria passed: {sum([c1, c2, c3, c4])} of 4). "
        "This leans on `questions_real` and the holdout; it calibrates the judging method and is not a claim about the system.")
    out("")

    # ---- extras
    out("## Beyond the four")
    out("")
    gv = defaultdict(dict)
    for r in generic:
        if r["valid_reply"]:
            gv[r["id"]][r["pass"]] = r
    gmeans = [sum(x["S"] for x in v.values()) / len(v) for v in gv.values()]
    out(f"### Generic-answer probe")
    out("")
    out(f"- {len(gv)} generic answers with a valid reply; mean S {fmt(sum(gmeans) / len(gmeans) if gmeans else float('nan'))}, "
        f"mean P {fmt(sum(x['P'] for v in gv.values() for x in v.values()) / max(sum(len(v) for v in gv.values()), 1))} "
        f"(a good rubric gives about 0).")
    sat = Counter()
    for v in gv.values():
        for r in v.values():
            for pid, e in r["must"].items():
                if e["verdict"] == "present":
                    sat[(r["id"], pid)] += 1
    out("- must points a generic answer satisfied (rubric, point, judgments present of "
        f"{sum(len(v) for v in gv.values())}):")
    if not sat:
        out("  - none")
    for (rid, pid), c in sorted(sat.items()):
        txt = next((p["text"] for p in rub.get(rid, {}).get("must", []) if p["id"] == pid), "")
        out(f"  - {rid} {pid} ({c}): {txt[:140]}")
    mn = Counter()
    for v in gv.values():
        for r in v.values():
            for pid, e in r["must_not"].items():
                if e["verdict"] == "present":
                    mn[(r["id"], pid)] += 1
    if mn:
        out("- must-not points marked present in a generic answer: " + ", ".join(f"{k[0]} {k[1]} ({c})" for k, c in sorted(mn.items())))
    out("")

    # labelled wrong items
    out("### The reader's 30 labelled wrong items: did the judging catch them")
    out("")
    out("Caught = in the same answer a must-not point present, a must point contradicted, or an entry in other_errors whose "
        "quote overlaps the label's quote (one contains the other, or a common run of at least 25 characters or 60 % of the shorter). "
        "A must-not present or a contradiction counts without checking that it is the same statement as the label.")
    out("")
    out("| # | set | question | label quote (start) | pass 1 | pass 2 |")
    out("|---|---|---|---|---|---|")
    caught_any = caught_both = judged = 0
    for lab in labels:
        s, rid = f"{lab['question_set']}_{lab['answer_set']}", f"{lab['question_set']}-{lab['question']}"
        cells, hit = [], []
        for p in (1, 2):
            r = by.get((s, rid), {}).get(p)
            if r is None:
                cells.append("not judged")
                hit.append(None)
                continue
            how = [f"must_not {k}" for k, e in r["must_not"].items() if e["verdict"] == "present"] \
                + [f"contradicted {k}" for k, e in r["must"].items() if e["verdict"] == "contradicted"] \
                + ["other_errors" for e in r["other_errors"] if quotes_overlap(e["quote"], lab["quote"])][:1]
            cells.append(", ".join(how) if how else "missed")
            hit.append(bool(how))
        if all(h is not None for h in hit):
            judged += 1
            caught_any += any(hit)
            caught_both += all(hit)
        out(f"| {lab['item']} | {s} | {rid} | {norm_ws(lab['quote'])[:60]} | {cells[0]} | {cells[1]} |")
    out("")
    out(f"Caught in at least one pass: {caught_any} of {judged} labelled items judged in both passes; in both passes: {caught_both}; "
        f"labelled items with no judged answer: {len(labels) - judged}.")
    out("")

    # pair order against the reader
    out("### Order of S between paired answers against the reader")
    out("")
    bad = Counter()
    for lab in labels:
        if lab.get("verdict") in ("confirmed", "partly"):
            bad[(f"{lab['question_set']}_{lab['answer_set']}", f"{lab['question_set']}-{lab['question']}")] += 1
    for s, i in omissions:
        bad[(s, i)] += 1
    mean_by = {k: sum(r["S"] for r in v.values()) / len(v) for k, v in by.items()}
    agree = disagree = tie = 0
    detail = []
    qids = sorted({k[1] for k in mean_by})
    for qid in qids:
        present = [s for s in sets if (s, qid) in mean_by]
        for i, a in enumerate(present):
            for b in present[i + 1:]:
                ba, bb = bad[(a, qid)], bad[(b, qid)]
                if ba == bb:
                    continue
                worse, better = (a, b) if ba > bb else (b, a)
                d = mean_by[(better, qid)] - mean_by[(worse, qid)]
                res = "agrees" if d > 1e-9 else ("tie" if abs(d) <= 1e-9 else "disagrees")
                agree += res == "agrees"
                tie += res == "tie"
                disagree += res == "disagrees"
                detail.append(f"{qid}: reader worse {worse} ({max(ba, bb)}) vs {better} ({min(ba, bb)}), S {fmt(mean_by[(worse, qid)])} vs {fmt(mean_by[(better, qid)])}: {res}")
    out("Reader's count per answer = labelled wrong items (confirmed or partly) plus essential omissions. Pairs of answer sets on the "
        "same question where the counts differ: the S of the answer the reader found worse should be lower.")
    out("")
    out(f"- {agree + disagree + tie} pairs: S agrees {agree}, tie {tie}, disagrees {disagree}.")
    for d in detail:
        out(f"  - {d}")
    out("")

    # statistics
    out("### Spread of S and the smallest visible difference")
    out("")
    sets_means = {s: {k[1]: v for k, v in mean_by.items() if k[0] == s} for s in sets}
    out("| answer set | answers | mean S | SD of S across items |")
    out("|---|---|---|---|")
    for s in sets:
        v = list(sets_means[s].values())
        out(f"| {s} | {len(v)} | {fmt(sum(v) / len(v) if v else float('nan'))} | {fmt(sd(v))} |")
    allv = [v for s in sets for v in sets_means[s].values()]
    out(f"| all | {len(allv)} | {fmt(sum(allv) / len(allv) if allv else float('nan'))} | {fmt(sd(allv))} |")
    out("")
    out(f"MDE = {MDE_FACTOR} x SD_d / sqrt(n) (paired, two-sided alpha 0.05, 80 % power).")
    out("")
    out("| pair of answer sets (paired by question) | n | SD_d | MDE at 120 items | MDE at 200 items |")
    out("|---|---|---|---|---|")
    sds = []
    for i, a in enumerate(sets):
        for b in sets[i + 1:]:
            qs = sorted(set(sets_means[a]) & set(sets_means[b]))
            if len(qs) < 3:
                continue
            d = [sets_means[a][q] - sets_means[b][q] for q in qs]
            sds.append(sd(d))
            out(f"| {a} vs {b} | {len(qs)} | {fmt(sd(d))} | {fmt(mde(sd(d), 120))} | {fmt(mde(sd(d), 200))} |")
    if sds:
        out(f"| median of the pairs | | {fmt(st.median(sds))} | {fmt(mde(st.median(sds), 120))} | {fmt(mde(st.median(sds), 200))} |")
    pp = [a - b for a, b in s_pairs_all]
    out("")
    out(f"Judge noise alone (S pass 1 minus S pass 2, {len(pp)} answers): SD_d {fmt(sd(pp))}, MDE at 120 items {fmt(mde(sd(pp), 120))}, "
        f"at 200 items {fmt(mde(sd(pp), 200))}. This is the judging noise on the same answer; it is not an A/A test of the system.")
    out("")
    out(f"Other rates over the {len(both)} answers judged twice: essential-miss rate "
        f"{fmt(100 * sum(r['essential_miss'] for v in both.values() for r in v.values()) / (2 * len(both)) if both else float('nan'), 1)} %, "
        f"harmful rate {fmt(100 * sum(r['harmful'] for v in both.values() for r in v.values()) / (2 * len(both)) if both else float('nan'), 1)} %, "
        f"other_errors per answer {fmt(sum(r['other_errors_n'] for v in both.values() for r in v.values()) / (2 * len(both)) if both else float('nan'))}.")
    out("")

    # constant points
    out("### Points present in every answer or in none (candidates for rewriting)")
    out("")
    tally = defaultdict(lambda: [0, 0])  # (rubric, kind, point) -> [present, rows]
    for r in valid:
        for kind in ("must", "must_not"):
            for pid, e in r[kind].items():
                t = tally[(r["id"], kind, pid)]
                t[1] += 1
                t[0] += e["verdict"] == "present"
    groups = {"must present in every judgment": [], "must present in none": [],
              "must-not present in every judgment": [], "must-not present in none": []}
    for (rid, kind, pid), (pres, tot) in sorted(tally.items()):
        key = f"{kind.replace('_', '-')} present in " + ("every judgment" if pres == tot else "none" if pres == 0 else "")
        if key in groups:
            groups[key].append(f"{rid} {pid} ({tot})")
    for k, v in groups.items():
        out(f"- {k}: {len(v)}" + (f" ({', '.join(v)})" if v else ""))
    out("")
    out("Counts in brackets are the judgments (answers x passes) the point was seen in. A must point that no answer satisfies "
        "may be too hard or wrongly worded; a must-not point that no answer triggers is ordinary and only unused.")
    return "\n".join(L) + "\n", {"kappa": kappa, "icc_s": icc_s, "icc_old": icc_o, "omission_share": share,
                                  "quote_fail_share": qshare, "go": go, "matched_old": len(matched)}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True, help="--out file of eval2_judge.py")
    ap.add_argument("--rubrics", default="eval/m4/pilot/rubrics.jsonl")
    ap.add_argument("--labels", default="runs/m4/reports/wp2a_labels_checked.jsonl")
    ap.add_argument("--old-dir", default="work/v3/judge", help="directory with results_h2h_*_p1.jsonl and _p2.jsonl")
    ap.add_argument("--omissions", help="JSONL of {set, id}: the reader's essential omissions (default: the built-in list)")
    a = ap.parse_args(argv)
    om = [(r["set"], r["id"]) for r in rd(a.omissions)] if a.omissions else None
    text, _ = build_report(rd(a.results), rd(a.rubrics), rd(a.labels), a.old_dir, om)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
