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

Option --same-statement (first pilot): a labelled wrong item is caught only by an entry of the same answer whose quote overlaps the
label's quote (a must-not present, a contradicted must point or an other-errors entry), not by any must-not or contradiction on the
answer. Without the option the first pilot's published numbers are reproduced.

Version 2 (--v2): the criteria of the second pilot, fixed in advance (runs/m4/reports/s5_pilot_analysis.md, design change 5), on the
results of eval2_judge.py run --instruction v2:
  1. kappa between the passes at least 0.7.
  2. The judge's share of the variance of a paired difference between two configurations at most 1/3. Formula (judge_variance_share):
     per item i and configuration c the score is the mean of the k passes; d_i = mean_A,i - mean_B,i; Var(d) is the sample variance of
     d_i over the items; the judge noise of one answer is s2 = sample variance of its k pass scores, averaged over the items for each
     configuration (s2_A, s2_B); the judge's part of Var(d) is (s2_A + s2_B) / k; share = that / Var(d). The criterion is on S
     (as in the analysis), the median over all pairs of configurations; P is reported beside it.
  3. At most 5 % of the verdicts "present" or "contradicted" have a quote not found under the fair matching.
  4. Raw flag precision at least 0.5 and semantic-error recall at least 0.7 in at least one pass (the same pass), from the reviewer's
     files. Precision of a pass = its candidate errors that the reviewer confirmed (confirmed 'yes' or 'harmful') over all its candidates.
     Recall of a pass = the reviewer's wrong/harmful findings that a candidate of that pass catches over all of them. A candidate catches a
     finding only if it concerns the same statement: its quotes overlap the finding's quote, or the finding names the candidate's flag_id.
  5. Supported essential omissions: at least 80 % of the reviewer's omission findings are flagged by the judge (judged answers x passes),
     and at least 60 % of the judge's supported essential-miss flags (answer x pass x point) are confirmed by an omission finding.
  Ordering of paired answers against the reviewer: descriptive, not a criterion.
Reviewer's files: the review file written by `eval2_judge.py flags` with 'confirmed' (yes, harmful, no, unclear) and 'note' filled in
(--review), and a findings file (--findings), JSONL, one row per finding the reviewer made on an answer:
  {"set", "id", "kind": "wrong"|"harmful"|"invented"|"omission", "quote": the statement (omission: not needed), "point": the must point id
   of an omission, "flag": optional flag_id(s) of the review file that the finding is about, "supported": false if the answer's own
   excerpts did not contain the omitted point (default true)}.
Criteria 4 and 5 and the ordering wait for these files; the verdict is GO only when every criterion has passed.

Coverage role (--coverage; decision J1 of MILESTONE.md, runs/m4/reports/s5b_pilot2_analysis.md sections 1, 3, 6, 7): the local judge reports
rubric coverage only; wrong, invented and harmful statements come from blind readers and the identifier check. Inputs: --results (the
--out file of `eval2_judge.py run --coverage`; rows made without it are read as well: a demoted failed-quote verdict is turned back into
"unverified"), --rubrics (with chunk ids: `eval2_judge.py fill-chunks`), --findings (the readers' file: the format above with a "reader"
field, optional "minor": true, and optional marker rows {"set","id","reader","kind":"read"} for an answer read with no finding; --reads
adds a JSONL of such markers; --all-read says every answer was read by the reader named on its rows). Criteria, each PASS, FAIL or WAITING:
  C1 essential-miss units, judge against reader, in each pass, on at least 100 weight-2 units: kappa >= 0.6, precision >= 0.75, recall >= 0.75.
     A unit is a weight-2 point or an any_of group with weight 2. A reader's omission row names a point ("m4") or several ("m4|m5"); several
     must equal one any_of group's members; a row that does not map to a unit is listed and not used. The reader of an answer is the first
     reader who has a row or marker on it (--primary-reader NAME forces one). A judge's unverified unit is not a miss.
  C2 at least 60 % of the judge's supported essential-miss units (answers x passes) are confirmed by that reader's omission with supported true.
  C3 unverified verdicts at most 3 % of all must verdicts, and none stored as absent (the rows were made with --coverage).
  C4 kappa between the judge's two passes at least 0.7 (must and must-not verdicts pooled, unverified a category of its own).
  C5 on answers read by two readers: the totals of wrong + invented + harmful findings differ by at most 30 % of the larger, otherwise
     "readings not usable for a bar"; both totals and the per-answer table are printed.
  C6 identifier check: the number of false "invented" findings on the stored answers, from the identifier check's own report
     (--idcheck-false-invented N; WAITING without it); PASS only for 0.
Go only if all six pass. Descriptive, not criteria: the readers' wrong + invented + harmful per 10 answers and harmful count per answer
set (between-item standard error), essential omissions per set, the judge's measures, and for each measure the paired difference between
sets with its standard error.
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2_judge as ej  # noqa: E402  (version 2 measures)

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


class TagError(Exception):
    """A tag file problem; the message names ids and the file name only."""


UNION_TAG = "union of all tags"   # the key of the block without the items of every tag together (only when the file has two or more tags)


def load_tag_file(path, known_ids):
    """eval/m4/HIDDEN_TAGS.json -> {tag name: sorted list of ids}. None -> {}. An id that is not in the data is not an error: the
    reports count the tagged items that are present (tag_blocks, tag_head). known_ids is kept for the callers and not used.
    Ids are not secret; the message carries ids, never item text."""
    if not path:
        return {}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        tags = data["tags"]
        if not isinstance(tags, dict) or any(not isinstance(v, dict) or not isinstance(v.get("ids"), list) for v in tags.values()):
            raise TypeError
        out = OrderedDict((str(name), sorted({str(i) for i in v["ids"]})) for name, v in tags.items())
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        raise TagError(f"{Path(path).name}: not a tag file ({{\"tags\": {{name: {{\"ids\": [...]}}}}}})") from None
    for name, v in tags.items():
        ids = [str(i) for i in v["ids"]]
        dup = sorted({i for i in ids if ids.count(i) > 1})
        if dup:
            raise TagError(f"tag {name}: duplicate id(s): {', '.join(dup)}")
    return out


def tag_blocks(tags, present_ids):
    """[(name, ids tagged, ids tagged and present in the data)]: one block per tag in file order, then, with two or more tags, the
    union of all tags under UNION_TAG. A tag whose ids are all absent has an empty present set."""
    present_ids = set(present_ids)
    blocks = [(n, list(ids), set(ids) & present_ids) for n, ids in tags.items()]
    if len(blocks) > 1:
        every = sorted({i for _, ids, _ in blocks for i in ids})
        blocks.append((UNION_TAG, every, set(every) & present_ids))
    return blocks


def tag_label(name, tagged, present):
    """('the items tagged X', '4 items'); when some tagged ids are not in the data: ('...', '1 of 2 tagged items present')."""
    subject = "the union of all tagged items" if name == UNION_TAG else f"the items tagged {name}"
    return subject, (f"{present} items" if present == tagged else f"{present} of {tagged} tagged items present")


def tag_head(name, tagged, present, remain):
    """'without the items tagged X (4 items; 16 remain)'."""
    subject, counts = tag_label(name, tagged, present)
    return f"without {subject} ({counts}; {remain} remain)"


def rd(path):
    if ej.is_hidden(path):   # .enc: decrypted in memory (scripts/eval2.py)
        return ej.rd(path)
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


def build_report(results, rubrics, labels, old_dir, omissions=None, same_statement=False):
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
    if same_statement:
        out("Caught = in the same answer a must-not point present, a must point contradicted or an entry in other_errors whose "
            "quote overlaps the label's quote (one contains the other, or a common run of at least 25 characters or 60 % of the shorter): "
            "the same statement. A must-not or a contradiction elsewhere on the answer does not count.")
    else:
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
            same = (lambda e: quotes_overlap(e["quote"], lab["quote"])) if same_statement else (lambda e: True)
            how = [f"must_not {k}" for k, e in r["must_not"].items() if e["verdict"] == "present" and same(e)] \
                + [f"contradicted {k}" for k, e in r["must"].items() if e["verdict"] == "contradicted" and same(e)] \
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


# ---------------------------------------------------------------- version 2 (second pilot)

THRESHOLD_VARIANCE_SHARE = 1 / 3
THRESHOLD_PRECISION, THRESHOLD_RECALL = 0.5, 0.7
THRESHOLD_OMISSION_FLAGGED, THRESHOLD_OMISSION_CONFIRMED = 0.8, 0.6
WAITING = "WAITING"


def judge_variance_share(rows, set_a, set_b, key="S"):
    """Share of the variance of a paired difference between two configurations that is judge noise (see the module docstring).
    rows: result rows; only answers valid in both passes in both sets, on items that both sets have. Needs at least 3 items.
    -> (share, var_d, var_judge, n_items) or None. Hand-computed in tests/test_eval2_pilot_report.py."""
    pas = defaultdict(dict)
    for r in rows:
        if r.get("valid_reply") and r["set"] in (set_a, set_b):
            pas[(r["set"], r["id"])][r["pass"]] = r[key]
    ids = sorted(i for i in {k[1] for k in pas} if all(len(pas.get((s, i), {})) >= 2 for s in (set_a, set_b)))
    if len(ids) < 3:
        return None
    ks = {s: [sorted(pas[(s, i)].items()) for i in ids] for s in (set_a, set_b)}
    mean = lambda xs: sum(xs) / len(xs)
    d = [mean([v for _, v in ks[set_a][n]]) - mean([v for _, v in ks[set_b][n]]) for n in range(len(ids))]
    var_d = sd(d) ** 2
    k = min(len(x) for s in (set_a, set_b) for x in ks[s])
    s2 = {s: mean([sd([v for _, v in x]) ** 2 for x in ks[s]]) for s in (set_a, set_b)}
    var_j = (s2[set_a] + s2[set_b]) / k
    return (float("nan") if var_d == 0 else var_j / var_d), var_d, var_j, len(ids)


def _status(ok):
    return "PASS" if ok else "FAIL"


def precision_recall(review, findings):
    """Per pass: precision of the judge's candidate errors and recall of the reviewer's wrong/harmful/invented findings.
    -> {pass: {"flags", "confirmed", "precision", "findings", "caught", "recall"}}; and the number of undecided flags."""
    undecided = sum(1 for x in review if x.get("confirmed", "") not in ("yes", "harmful", "no", "unclear"))
    passes = sorted({p for x in review for p, _ in x["members"]}) or [1, 2]
    sem = [f for f in findings if f.get("kind") in ("wrong", "harmful", "invented")]
    out = {}
    for p in passes:
        flags = [(x, i) for x in review for q, i in x["members"] if q == p]
        conf = sum(1 for x, _ in flags if x.get("confirmed") in ("yes", "harmful"))
        caught = 0
        for f in sem:
            names = f.get("flag") if isinstance(f.get("flag"), list) else [f.get("flag")]
            if any(x["set"] == f["set"] and x["item"] == f["id"] and any(q == p for q, _ in x["members"])
                   and (x["flag_id"] in names or any(quotes_overlap(q, f.get("quote", "")) for q in x["quotes"]))
                   for x in review):
                caught += 1
        out[p] = {"flags": len(flags), "confirmed": conf, "precision": conf / len(flags) if flags else float("nan"),
                  "findings": len(sem), "caught": caught, "recall": caught / len(sem) if sem else float("nan")}
    return out, undecided


def omission_shares(rows, findings):
    """(share of the reviewer's supported omission findings that the judge flagged as supported essential misses, over answers x
    passes; trials; share of the judge's supported essential-miss flags that an omission finding confirms; flags)."""
    om = [f for f in findings if f.get("kind") == "omission" and f.get("supported", True)]
    flagged = trials = 0
    for f in om:
        for r in rows:
            if r.get("valid_reply") and r["set"] == f["set"] and r["id"] == f["id"]:
                trials += 1
                flagged += any(e["supported"] and (not f.get("point") or e["id"] == f["point"]) for e in r["essential_points"])
    flags = conf = 0
    for r in rows:
        if not r.get("valid_reply"):
            continue
        for e in r["essential_points"]:
            if e["supported"]:
                flags += 1
                conf += any(f["set"] == r["set"] and f["id"] == r["id"] and (not f.get("point") or f["point"] == e["id"]) for f in om)
    return (flagged / trials if trials else float("nan")), trials, (conf / flags if flags else float("nan")), flags


def probe_results(rows, rubrics):
    """Rubric probes (eval2_judge.py gen-probes, judged in one pass): per probe set the expectations met and the ones not met.
      probe_generic   every must point NOT present (the item's generic answer names nothing specific);
      probe_alt       every weight-2 point satisfied (present, or a member of its any_of group present) and no must-not present
                      (the documented correct alternative);
      probe_ref       every weight-2 point present and no must-not present (the item's reference answer);
      probe_nearmiss  the targeted must-not point NOT present (reference + that point's near_miss sentence; ids '<item>~<point>').
    An answer without a valid reply or without a result row leaves its expectations unmet (note 'no valid reply' / 'not judged').
    -> {set: {"expected": int, "met": int, "unmet": [(item, point, note)], "answers": int}}."""
    rub = {r["id"]: r for r in rubrics}
    by = {}
    for r in rows:
        by.setdefault(r["set"], {})[r["id"]] = r
    out = {}

    def quote_of(e):
        return (e.get("quote") or "")[:100]

    def tally(name, expectations):
        out[name] = {"expected": len(expectations), "met": sum(1 for _, _, ok, _ in expectations if ok),
                     "unmet": [(i, p, note) for i, p, ok, note in expectations if not ok],
                     "answers": len(by.get(name, {}))}

    for name in ("probe_generic", "probe_alt", "probe_ref"):
        exp, seen_groups = [], set()
        for rid, rb in rub.items():
            row = by.get(name, {}).get(rid)
            groups = ej.groups_of(rb)
            targets = [("must", p) for p in rb["must"]] if name == "probe_generic" else \
                [("must", p) for p in rb["must"] if p["weight"] == 2]
            if name != "probe_generic":
                targets += [("must_not", p) for p in rb.get("must_not") or []]
            for kind, p in targets:
                if row is None or not row.get("valid_reply"):
                    exp.append((rid, p["id"], False, "not judged" if row is None else "no valid reply"))
                    continue
                v = row[kind][p["id"]]
                if kind == "must_not" or name == "probe_generic":
                    ok = v["verdict"] != "present"
                    exp.append((rid, p["id"], ok, "" if ok else f"judge: {v['verdict']}, quote: {quote_of(v)}"))
                    continue
                mv = {k: x["verdict"] for k, x in row["must"].items()}
                grp = next((g for g in groups if p["id"] in g), {p["id"]})
                if name == "probe_alt" and len(grp) > 1:
                    # the alternative has to satisfy the essential point through its any_of group: one expectation per group
                    if (rid, sorted(grp)[0]) in seen_groups:
                        continue
                    seen_groups.add((rid, sorted(grp)[0]))
                    ok = any(mv.get(i) == "present" for i in grp)
                    exp.append((rid, "+".join(sorted(grp)), ok, "" if ok else "judge: no member of the any_of group present (" + ", ".join(f"{i}: {mv.get(i)}" for i in sorted(grp)) + ")"))
                    continue
                ok = mv.get(p["id"]) == "present" or (name == "probe_alt" and any(mv.get(i) == "present" for i in grp))
                exp.append((rid, p["id"], ok, "" if ok else f"judge: {mv.get(p['id'])}"))
        tally(name, exp)
    exp = []
    for rid, rb in rub.items():
        for pt in rb.get("must_not") or []:
            pid = f"{rid}{ej.PROBE_SEP}{pt['id']}"
            row = by.get("probe_nearmiss", {}).get(pid)
            if row is None or not row.get("valid_reply"):
                exp.append((rid, pt["id"], False, "not judged" if row is None else "no valid reply"))
                continue
            v = row["must_not"][pt["id"]]
            ok = v["verdict"] != "present"
            exp.append((rid, pt["id"], ok, "" if ok else f"judge: present, quote: {quote_of(v)}"))
    tally("probe_nearmiss", exp)
    return out


PROBE_TEXT = (("probe_generic", "generic answer", "no must point present"),
              ("probe_alt", "correct alternative", "every essential point satisfied through any_of, no must-not present"),
              ("probe_ref", "reference answer", "every weight-2 point present, no must-not present"),
              ("probe_nearmiss", "reference + near-miss sentence", "the targeted must-not point not present"))


def probe_section(rows, rubrics):
    res = probe_results(rows, rubrics)
    L = ["## Rubric probes", "",
         "One pass each, judged like any answer (instruction v2, no pack, so ANSWER EXCERPTS is '(not available)'). A point not met is a "
         "defect of the rubric or of the judge to look at; these are not go or no-go criteria.", ""]
    for name, what, expect in PROBE_TEXT:
        r = res[name]
        L.append(f"- **{name}** ({what}; {r['answers']} answers; expected: {expect}): **{r['met']} of {r['expected']} expectations met**.")
        for i, p, note in r["unmet"]:
            L.append(f"  - not met: {i} {p}" + (f" ({note})" if note else ""))
    L.append("")
    return L, res


def source_section(rubrics, chunks):
    """How the must points' `source` fields map to the index: by file path against the chunks' `path` (a path that is relative to its
    source's root gets 'raw/<source>/' in front; the paths of the frozen index already start with raw/ or data/, so the match is
    exact). Points without a file path in the source ('searched ...', 'inferred', 'not covered') have no source to match and can never
    be a supported essential miss; neither can points whose only file is a tool source of the catalogues (raw_catalogue/...)."""
    idx = ej.index_paths_of(chunks)
    kinds = Counter()
    ess = Counter()
    for rb in rubrics:
        for p in rb["must"]:
            k = ej.point_source_kind(p, idx)
            kinds[k] += 1
            if p["weight"] == 2:
                ess[k] += 1
    n, ne = sum(kinds.values()), sum(ess.values())
    L = ["## Sources of the must points", "",
         "Matching: the file paths in a point's `source` (the first word of each ';' segment that starts with raw/ or data/, ':lines' removed) "
         "against the chunks' canonical `path` (the index paths start with raw/ or data/ already, otherwise 'raw/<source>/' is put in "
         "front); a chunk carries no line numbers, so the match is file level: every chunk of the file counts as a place the point may be.", "",
         "| source of the point | all must points | weight-2 points |", "|---|---|---|"]
    for k, label in (("index", "file path with chunks in the index"), ("not_indexed", "raw/ or data/ path without a chunk"),
                     ("catalogue", "only a raw_catalogue file (tool source, not in the index)"),
                     ("none", "no file path ('searched', 'inferred', 'not covered')")):
        L.append(f"| {label} | {kinds[k]} | {ess[k]} |")
    L.append(f"| total | {n} | {ne} |")
    L += ["", f"Points that can never be a supported essential miss: {n - kinds['index']} of {n} ({ne - ess['index']} of {ne} with weight 2); "
          f"of these, {kinds['none']} have no source that is a file path.", ""]
    return L


def build_report_v2(results, review=None, findings=None, title="Judge pilot 2", probes=None, rubrics=None, chunks=None):
    """The second pilot's report: measures, the five criteria, the ordering against the reviewer, the verdict.
    results: result rows (instruction v2); rows given a reference answer (arm 3) are shown apart and left out of the criteria.
    review, findings: the reviewer's files or None. -> (text, numbers)."""
    L = []
    out = L.append
    ref_rows = [r for r in results if r.get("reference_given")]
    rows = [r for r in results if not r.get("reference_given")]
    valid = [r for r in rows if r["valid_reply"]]
    by = defaultdict(dict)
    for r in valid:
        by[(r["set"], r["id"])][r["pass"]] = r
    both = {k: v for k, v in by.items() if 1 in v and 2 in v}
    sets = list(OrderedDict.fromkeys(r["set"] for r in rows))
    ins = sorted({r.get("instruction", "v1") for r in rows})
    out(f"# {title}: judge step version 2, results")
    out("")
    out(f"Judgments: {len(rows)} ({len(valid)} with a valid reply, {len(rows) - len(valid)} without, left out); answers judged in both "
        f"passes: {len(both)} of {len({(r['set'], r['id']) for r in rows})}; answer sets: {', '.join(sets)}; instruction version(s): "
        f"{', '.join(ins)}. Judge settings: {', '.join(sorted({r.get('setting', '?') for r in rows}))}.")
    out("")

    def measure_table(rs, head):
        m = ejm(rs)
        out(head)
        out("")
        out("| answer set | pass | answers | coverage P | essential misses (per 10 answers, count) | supported essential misses (per 10, count) "
            "| candidate errors (per 10, count) | of them harmful (count) | S (secondary) |")
        out("|---|---|---|---|---|---|---|---|---|")
        for name, per in m.items():
            for p, x in sorted(per.items()):
                out(f"| {name} | {p} | {x['n']} | {fmt(x['mean_P'])} | {fmt(x['essential_miss_per10'], 1)} ({x['essential_miss']}) "
                    f"| {fmt(x['supported_essential_miss_per10'], 1)} ({x['supported_essential_miss']}) "
                    f"| {fmt(x['candidates_per10'], 1)} ({x['candidates']}) | {x['harmful_candidates']} | {fmt(x['mean_S'])} |")
        out("")
        return m

    ejm = ej.measures
    measure_table(valid, "## Measures per answer set and pass (what needs no reviewer)")
    out("Candidate errors are raw: must-not present, must contradicted, and other errors with a verified `source_quote`, deduplicated "
        "across channels by overlapping quotes. They count as errors only once a reviewer has confirmed them "
        "(`eval2_judge.py flags`, then `confirmed`). Supported essential miss: a weight-2 must point absent, no any_of alternative "
        "present, and its source among the answer's own excerpts (by chunk id when the point gives chunk ids, otherwise by source "
        "file path).")
    out("")
    rules = Counter(e["rule"] for r in valid for e in r["essential_points"])
    out("Rule used for the essential-miss points: " + (", ".join(f"{k} {v}" for k, v in sorted(rules.items())) or "none") + ".")
    out("")
    if ref_rows:
        measure_table([r for r in ref_rows if r["valid_reply"]], "### Arm with the reference answer in the judge's input (not in the criteria)")

    out("## The five criteria")
    out("")
    # 1 kappa
    pairs = [(v[1][k][pid]["verdict"], v[2][k][pid]["verdict"]) for v in both.values() for k in ("must", "must_not") for pid in v[1][k]]
    kappa, po, pe, n, _ = cohen_kappa(pairs)
    c1 = not math.isnan(kappa) and kappa >= THRESHOLD_KAPPA
    out(f"1. **Per-point kappa between the passes: {fmt(kappa)}** ({n} point pairs, agreement {fmt(po, 3)}; at least {THRESHOLD_KAPPA}): **{_status(c1)}**")
    out("")
    # 2 variance share
    shares = {}
    for key in ("S", "P"):
        for i, a in enumerate(sets):
            for b in sets[i + 1:]:
                r = judge_variance_share(valid, a, b, key)
                if r:
                    shares[(key, a, b)] = r
    out("2. **The judge's share of the variance of a paired difference** = ((s2_A + s2_B) / k) / Var(d), with d the per-item difference of "
        "the pass means, s2 the within-answer variance of the k = 2 pass scores, averaged over the items (formula in the module docstring).")
    med = {}
    for key in ("S", "P"):
        v = [x[0] for k2, x in shares.items() if k2[0] == key and not math.isnan(x[0])]
        med[key] = st.median(v) if v else float("nan")
    c2 = not math.isnan(med["S"]) and med["S"] <= THRESHOLD_VARIANCE_SHARE
    out(f"   - median over the pairs of configurations: S {fmt(med['S'])}, P {fmt(med['P'])}; criterion on S, at most "
        f"{fmt(THRESHOLD_VARIANCE_SHARE)}: **{_status(c2)}**")
    for (key, a, b), (sh, vd, vj, ni) in shares.items():
        out(f"   - {key}: {a} vs {b}: share {fmt(sh)}, Var(d) {fmt(vd, 4)}, judge part {fmt(vj, 4)}, {ni} items")
    out("")
    # 3 quotes
    claims, failed = sum(r["quote_claims"] for r in valid), sum(r["quote_failed"] for r in valid)
    qs = failed / claims if claims else float("nan")
    c3 = not math.isnan(qs) and qs <= THRESHOLD_QUOTES
    dropped = sum(r.get("other_dropped", 0) for r in valid)
    kept = sum(r["other_errors_n"] for r in valid)
    out(f"3. **Quotes not found (fair matching): {fmt(100 * qs, 1)} %** ({failed} of {claims} present or contradicted verdicts; at most "
        f"{int(100 * THRESHOLD_QUOTES)} %): **{_status(c3)}**")
    out(f"   - other-errors entries dropped for a `source_quote` not found in the passages or the excerpts: {dropped}; kept: {kept}.")
    out("")
    # 4, 5 and ordering
    c4 = c5 = WAITING
    pr_by_pass, undecided = {}, 0
    if review is not None and findings is not None:
        pr_by_pass, undecided = precision_recall(review, findings)
        ok = [p for p, x in pr_by_pass.items() if x["precision"] >= THRESHOLD_PRECISION and x["recall"] >= THRESHOLD_RECALL]
        c4 = WAITING if undecided else _status(bool(ok))
    if findings is not None:
        f1, t1, f2, t2 = omission_shares(valid, findings)
        c5 = "N/A" if math.isnan(f1) and math.isnan(f2) else _status(
            not math.isnan(f1) and f1 >= THRESHOLD_OMISSION_FLAGGED and not math.isnan(f2) and f2 >= THRESHOLD_OMISSION_CONFIRMED)
    out(f"4. **Raw flag precision at least {THRESHOLD_PRECISION} and semantic-error recall at least {THRESHOLD_RECALL} in at least one pass** "
        f"(from the reviewer's files): **{c4}**")
    if pr_by_pass:
        for p, x in pr_by_pass.items():
            out(f"   - pass {p}: precision {fmt(x['precision'])} ({x['confirmed']} of {x['flags']} flags confirmed), recall {fmt(x['recall'])} "
                f"({x['caught']} of {x['findings']} wrong, harmful or invented findings caught, same statement only)")
        out(f"   - flags without a decision in the review file: {undecided}; 'unclear' counts as not confirmed.")
    else:
        out("   - waits for the review file (`eval2_judge.py flags`, `confirmed` and `note` filled in) and the findings file.")
    out("")
    out(f"5. **Supported essential omissions: at least {int(100 * THRESHOLD_OMISSION_FLAGGED)} % of the reviewer's flagged by the judge, and at least "
        f"{int(100 * THRESHOLD_OMISSION_CONFIRMED)} % of the judge's flags confirmed by the reviewer**: **{c5}**")
    if findings is not None:
        out(f"   - reviewer's supported omissions flagged by the judge: {fmt(100 * f1, 1)} % ({t1} answer x pass trials); "
            f"judge's supported essential-miss flags confirmed: {fmt(100 * f2, 1)} % ({t2} flags).")
    else:
        out("   - waits for the findings file.")
    out("")
    out("### Order of paired answers against the reviewer (descriptive, not a criterion)")
    out("")
    if findings is None:
        out("Waits for the findings file.")
    else:
        bad = Counter((f["set"], f["id"]) for f in findings)
        meas = {}
        for k, v in by.items():
            meas[k] = {"S": mean_s(list(v.values()), "S"), "P": mean_s(list(v.values()), "P"),
                       "flags": mean_s([{"x": len(x["candidates"]) + sum(e["supported"] for e in x["essential_points"])} for x in v.values()], "x")}
        for key, worse_is in (("S", "low"), ("P", "low"), ("flags", "high")):
            ag = ti = di = 0
            for i in sorted({k[1] for k in meas}):
                pres = [s for s in sets if (s, i) in meas]
                for x, a in enumerate(pres):
                    for b in pres[x + 1:]:
                        if bad[(a, i)] == bad[(b, i)]:
                            continue
                        w, g = (a, b) if bad[(a, i)] > bad[(b, i)] else (b, a)
                        d = meas[(g, i)][key] - meas[(w, i)][key]
                        d = d if worse_is == "low" else -d
                        ag += d > 1e-9
                        ti += abs(d) <= 1e-9
                        di += d < -1e-9
            out(f"- {key} ({'lower' if worse_is == 'low' else 'higher'} for the worse answer expected): agrees {ag}, tie {ti}, disagrees {di}.")
    out("")
    cs = {"1 kappa": _status(c1), "2 variance share": _status(c2), "3 quote failures": _status(c3),
          "4 flag precision and recall": c4, "5 supported omissions": c5}
    failed_c = [k for k, v in cs.items() if v == "FAIL"]
    waiting = [k for k, v in cs.items() if v not in ("PASS", "FAIL")]   # waiting for the reviewer, or no data (N/A)
    go = not failed_c and not waiting
    out("## Result")
    out("")
    out("| criterion | result |")
    out("|---|---|")
    for k, v in cs.items():
        out(f"| {k} | {v} |")
    out("")
    tail = "" if not waiting else (f" Criteria that wait for the reviewer's files: "
                                   f"{', '.join(k for k in waiting if cs[k] == WAITING) or 'none'}."
                                   + "".join(f" {k}: no data (N/A), not decided." for k in waiting if cs[k] == "N/A"))
    out(f"**Go or no-go: {'GO' if go else 'NO-GO'}** ({sum(v == 'PASS' for v in cs.values())} of 5 criteria passed, "
        f"{len(failed_c)} failed, {len(waiting)} open).{tail} Not a GO until every criterion has passed.")
    probe_res = None
    if probes is not None and rubrics is not None:
        out("")
        pl, probe_res = probe_section(probes, rubrics)
        L += pl
    if rubrics is not None and chunks:
        out("")
        L += source_section(rubrics, chunks)
    return "\n".join(L) + "\n", {"kappa": kappa, "probes": probe_res, "variance_share_S": med["S"], "variance_share_P": med["P"], "quote_fail_share": qs,
                                  "criteria": cs, "go": go, "precision_recall": pr_by_pass}


# ---------------------------------------------------------------- coverage role

COV_KAPPA, COV_PRECISION, COV_RECALL, COV_MIN_UNITS = 0.6, 0.75, 0.75, 100
COV_CONFIRMED, COV_UNVERIFIED, COV_PASS_KAPPA, COV_READER_DIFF = 0.6, 0.03, 0.7, 0.30
WIH = ("wrong", "invented", "harmful")


def split_point_ids(point):
    return [x.strip() for x in re.split(r"[|,+/ ]+", point or "") if x.strip()]


def reader_table(findings, reads=None, all_read=False, answers=(), primary=None):
    """-> ({(set, id): [readers who read it, first first]}, {(set, id, reader): [finding rows]}).
    An answer is read by a reader with a row or a marker (kind 'read') on it, or in `reads`; rows without a "reader" field belong to
    reader "r1". all_read: every answer of `answers` that no reader has on it is read by "r1" (no finding)."""
    readers, rows = OrderedDict(), defaultdict(list)
    for f in list(findings) + list(reads or []):
        k = (f["set"], f["id"])
        who = f.get("reader") or "r1"
        if who not in readers.setdefault(k, []):
            readers[k].append(who)
        if f.get("kind") != "read":
            rows[(f["set"], f["id"], who)].append(f)
    if all_read:
        for k in answers:
            readers.setdefault(k, ["r1"])
    if primary:
        for k, v in readers.items():
            if primary in v:
                v.remove(primary)
                v.insert(0, primary)
    return readers, rows


def map_omissions(rows, units):
    """Reader omission rows of one answer and reader -> ({unit index: [rows]}, [unmapped rows with a reason]). A row naming one point
    maps to the unit that holds it; a row naming several must equal the member set of one unit (an any_of group)."""
    mapped, bad = defaultdict(list), []
    for f in rows:
        if f.get("kind") != "omission":
            continue
        ids = split_point_ids(f.get("point"))
        if not ids:
            bad.append((f, "no point named"))
            continue
        hit = [i for i, u in enumerate(units) if set(ids) <= set(u["ids"])]
        if not hit:
            known = all(any(x in u["ids"] for u in units) for x in ids)
            bad.append((f, "several points that are not one any_of group" if len(ids) > 1 and known else "not a weight-2 unit"))
        elif len(ids) > 1 and set(ids) != set(units[hit[0]]["ids"]):
            bad.append((f, "several points that are not one any_of group"))
        else:
            mapped[hit[0]].append(f)
    return mapped, bad


def _mean_se(xs):
    n = len(xs)
    if not n:
        return float("nan"), float("nan")
    m = sum(xs) / n
    return m, (st.stdev(xs) / math.sqrt(n) if n > 1 else float("nan"))


BY_KIND_MIN = 5   # a kind with fewer items is merged into "other"; "other" itself is not shown below this size (hidden rows)


def kind_section(valid, rubrics, items, min_n=BY_KIND_MIN):
    """Totals per item kind (the 'kind' of the items file) as lines of a table; kinds with fewer than min_n items go to 'other', and
    'other' is left out if it too has fewer than min_n. Totals only: no ids. -> list of lines."""
    kind_of = {x["id"]: (x.get("kind") or "other") for x in items}
    ids_of = defaultdict(set)
    for r in valid:
        ids_of[kind_of.get(r["id"], "other")].add(r["id"])
    group = {k: (k if len(v) >= min_n else "other") for k, v in ids_of.items()}
    members = defaultdict(set)
    for k, v in ids_of.items():
        members[group[k]] |= v
    L = ["## Totals per kind (kinds with fewer than %d items are merged into 'other'; 'other' is shown only with at least %d items)" % (min_n, min_n), "",
         "| kind | answer set | pass | answers | coverage P | essential-miss units per 10 (count) | supported essential-miss units per 10 (count) | must-not present | unverified share (count) |",
         "|---|---|---|---|---|---|---|---|---|"]
    for g in sorted(members, key=lambda g: (g == "other", g)):
        if len(members[g]) < min_n:
            continue
        m = ej.coverage_measures([r for r in valid if r["id"] in members[g]], rubrics)
        for name, per in m.items():
            for p, x in sorted(per.items()):
                L.append(f"| {g} | {name} | {p} | {x['n']} | {fmt(x['mean_P'])} | {fmt(x['missed_per10'], 1)} ({x['missed']}) | "
                         f"{fmt(x['supported_missed_per10'], 1)} ({x['supported_missed']}) | {x['must_not_present']} | "
                         f"{fmt(100 * x['unverified_share'], 1)} % ({x['unverified']}) |")
    L.append("")
    return L


def build_report_coverage(results, rubrics, findings, reads=None, all_read=False, primary=None, idcheck_false=None,
                          title="Coverage role: batch report", hidden=False, items=None):
    """The batch report of the coverage role (module docstring). -> (text, numbers).
    hidden: totals only: no list of reader rows that map to no unit and no per-answer table of the two readers (both name ids);
    items (with 'kind'): adds the totals per kind."""
    L = []
    out = L.append
    rub = {r["id"]: r for r in rubrics}
    valid = [r for r in results if r.get("valid_reply") and not r.get("reference_given") and r["id"] in rub]
    sets = list(OrderedDict.fromkeys(r["set"] for r in valid))
    cov = {(r["set"], r["id"], r["pass"]): ej.coverage_row(r, rub[r["id"]]) for r in valid}
    answers = sorted({(r["set"], r["id"]) for r in valid})
    both = [k for k in answers if (k[0], k[1], 1) in cov and (k[0], k[1], 2) in cov]
    passes = sorted({r["pass"] for r in valid})
    readers, frows = reader_table(findings, reads, all_read, answers, primary)
    read_answers = [k for k in answers if readers.get(k)]
    with_chunks = sum(1 for r in rubrics if ej.rubric_has_chunks(r))
    rules = Counter(c["rule"] for c in cov.values())
    out(f"# {title}")
    out("")
    out(f"Judgments: {len(valid)} valid rows on {len(answers)} answers ({len(both)} in both passes); answer sets: {', '.join(sets)}. "
        f"Answers with a reader: {len(read_answers)}; with two readers: {sum(1 for k in answers if len(readers.get(k, [])) >= 2)}.")
    if rules.get("file"):
        out(f"**Supported rule: for {rules['file']} of {len(cov)} judgments (rubrics without chunk ids) the old file-level rule is used (source "
            f"file against the excerpts' files); for {rules.get('chunk', 0)} the rule is by chunk id.** Rubrics with chunk ids: {with_chunks} of {len(rubrics)}.")
    else:
        out(f"Supported rule: by chunk id for all {len(cov)} judgments (rubrics with chunk ids: {with_chunks} of {len(rubrics)}); a point without chunk ids is never supported.")
    out("")

    # ---- measures
    m = ej.coverage_measures(valid, rubrics)
    out("## Judge measures per answer set and pass")
    out("")
    out("| answer set | pass | answers | coverage P | essential-miss units per 10 (count) | supported essential-miss units per 10 (count) | must-not present | unverified share (count) |")
    out("|---|---|---|---|---|---|---|---|")
    for name, per in m.items():
        for p, x in sorted(per.items()):
            out(f"| {name} | {p} | {x['n']} | {fmt(x['mean_P'])} | {fmt(x['missed_per10'], 1)} ({x['missed']}) | "
                f"{fmt(x['supported_missed_per10'], 1)} ({x['supported_missed']}) | {x['must_not_present']} | {fmt(100 * x['unverified_share'], 1)} % ({x['unverified']}) |")
    out("")
    out("Essential-miss unit: a weight-2 point, or an any_of group of weight 2 counted once; missed = no member present and none unverified. "
        "Unverified (the judge said present or contradicted, the quote was not found) is not present and not a miss; P counts it as not present. "
        "The judge's other-errors output is not used here.")
    out("")

    # ---- unit-level agreement with the reader
    unmapped, unit_rows = [], {p: [] for p in passes}
    conf_ok = defaultdict(lambda: [0, 0])   # per pass: [confirmed, flags]
    confirmed_flags = []
    per_answer_reader = {}                  # (set, id) -> {"omissions": n, "wih": n, "harmful": n, "minor": n}
    for k in answers:
        rd_ = readers.get(k)
        if not rd_:
            continue
        who = rd_[0]
        fr = frows.get((k[0], k[1], who), [])
        per_answer_reader[k] = {"reader": who, "wih": sum(1 for f in fr if f.get("kind") in WIH), "harmful": sum(1 for f in fr if f.get("kind") == "harmful"),
                                "minor": sum(1 for f in fr if f.get("kind") in WIH and f.get("minor")),
                                "omissions": 0}
        for p in passes:
            c = cov.get((k[0], k[1], p))
            if c is None:
                continue
            mapped, bad = map_omissions(fr, c["units"])
            if p == passes[0]:
                unmapped += [(k, who, f, why) for f, why in bad]
                per_answer_reader[k]["omissions"] = len(mapped)
            for i, u in enumerate(c["units"]):
                rows_u = mapped.get(i, [])
                unit_rows[p].append((u["status"] == "missed", bool(rows_u)))
                if u["status"] == "missed" and u["supported"]:
                    conf_ok[p][1] += 1
                    ok = any(f.get("supported", True) for f in rows_u)
                    conf_ok[p][0] += ok
    out("## Criteria")
    out("")
    cs, numbers = OrderedDict(), {}

    def status3(ok_list, enough=True):
        if not enough:
            return WAITING
        return "PASS" if all(ok_list) else "FAIL"

    # C1
    c1_ok, c1_lines, enough = [], [], True
    for p in passes:
        pairs = [("miss" if j else "ok", "miss" if r else "ok") for j, r in unit_rows[p]]
        kappa, po, pe, n, conf = cohen_kappa(pairs)
        both_n, jo, ro = conf[("miss", "miss")], conf[("miss", "ok")], conf[("ok", "miss")]
        prec = both_n / (both_n + jo) if both_n + jo else float("nan")
        rec = both_n / (both_n + ro) if both_n + ro else float("nan")
        ok = all(not math.isnan(x) for x in (kappa, prec, rec)) and kappa >= COV_KAPPA and prec >= COV_PRECISION and rec >= COV_RECALL
        c1_ok.append(ok)
        enough = enough and n >= COV_MIN_UNITS
        numbers[f"c1_pass{p}"] = {"units": n, "kappa": kappa, "precision": prec, "recall": rec, "both": both_n, "judge_only": jo, "reader_only": ro}
        c1_lines.append(f"   - pass {p}: {n} weight-2 units, both miss {both_n}, judge only {jo}, reader only {ro}; kappa {fmt(kappa)} "
                        f"(at least {COV_KAPPA}), precision {fmt(prec)} (at least {COV_PRECISION}), recall {fmt(rec)} (at least {COV_RECALL}): "
                        + (WAITING if not read_answers else pf(ok) if n >= COV_MIN_UNITS else "not decided, fewer than %d units" % COV_MIN_UNITS))
    cs["C1 essential-miss units, judge against reader"] = status3(c1_ok, enough) if passes and read_answers else WAITING
    out(f"**C1 essential-miss units, judge against reader (kappa, precision, recall in each pass, at least {COV_MIN_UNITS} units): {cs['C1 essential-miss units, judge against reader']}**"
        + (" (no answer has been read by a reader)" if not read_answers else "" if enough else f" (fewer than {COV_MIN_UNITS} units in a pass: not decided)")
        + (f"; computed on the {len(read_answers)} read answers of {len(answers)}" if read_answers else ""))
    L.extend(c1_lines)
    out(f"   - reader omission rows that do not map to a unit (not used): {len(unmapped)}")
    for k, who, f, why in ([] if hidden else unmapped):
        out(f"     - {k[0]} {k[1]} ({who}) point {f.get('point')!r}: {why}")
    out("")

    # C2
    tot_c, tot_f = sum(v[0] for v in conf_ok.values()), sum(v[1] for v in conf_ok.values())
    share = tot_c / tot_f if tot_f else float("nan")
    cs["C2 supported misses confirmed by the reader"] = WAITING if not tot_f else _status(share >= COV_CONFIRMED)
    numbers["c2"] = {"confirmed": tot_c, "flags": tot_f, "share": share}
    out(f"**C2 supported essential-miss units confirmed by a reader's omission with supported true: {fmt(100 * share, 1)} %** ({tot_c} of {tot_f}, "
        f"answers x passes; at least {int(100 * COV_CONFIRMED)} %): **{cs['C2 supported misses confirmed by the reader']}**")
    for p in passes:
        out(f"   - pass {p}: {conf_ok[p][0]} of {conf_ok[p][1]}")
    out("")

    # C3
    must_n = sum(c["must_verdicts"] for c in cov.values())
    unv = sum(c["unverified"] for c in cov.values())
    claims = sum(r["quote_claims"] for r in valid)
    raw_demoted = sum(1 for r in valid for e in list(r["must"].values()) + list(r["must_not"].values())
                      if e["verdict"] == "absent" and e.get("quote_ok") is False and e.get("judge_verdict") in ("present", "contradicted"))
    ush = unv / must_n if must_n else float("nan")
    ok3 = not math.isnan(ush) and ush <= COV_UNVERIFIED and raw_demoted == 0
    cs["C3 unverified verdicts"] = _status(ok3)
    numbers["c3"] = {"unverified": unv, "must_verdicts": must_n, "share": ush, "raw_demoted": raw_demoted}
    out(f"**C3 unverified verdicts: {fmt(100 * ush, 1)} % of must verdicts** ({unv} of {must_n}; {unv} of {claims} present or contradicted claims; at most "
        f"{int(100 * COV_UNVERIFIED)} %), verdicts stored as absent although the judge said present or contradicted: {raw_demoted} (must be 0: run with --coverage): **{cs['C3 unverified verdicts']}**")
    out("")

    # C4
    byk = {(r["set"], r["id"], r["pass"]): r for r in valid}
    pairs4, pairs4m = [], []
    for k in both:
        r1, r2 = byk[(k[0], k[1], 1)], byk[(k[0], k[1], 2)]
        for kind in ("must", "must_not"):
            for pid in r1[kind]:
                pr_ = (_view_verdict(r1[kind][pid]), _view_verdict(r2[kind][pid]))
                pairs4.append(pr_)
                if kind == "must":
                    pairs4m.append(pr_)
    k4, po4, _, n4, _ = cohen_kappa(pairs4)
    k4m = cohen_kappa(pairs4m)[0]
    cs["C4 kappa between the passes"] = WAITING if not n4 else _status(k4 >= COV_PASS_KAPPA)
    numbers["c4"] = {"kappa": k4, "kappa_must": k4m, "pairs": n4}
    out(f"**C4 kappa between the judge's two passes: {fmt(k4)}** ({n4} point verdict pairs, must and must-not pooled, agreement {fmt(po4, 3)}; must only: {fmt(k4m)}; "
        f"at least {COV_PASS_KAPPA}): **{cs['C4 kappa between the passes']}**")
    out("")

    # C5
    two = [k for k in answers if len(readers.get(k, [])) >= 2]
    out_c5 = WAITING
    if two:
        ra, rb_ = None, None
        names = Counter(w for k in two for w in readers[k][:2])
        ra, rb_ = [n for n, _ in names.most_common(2)] if len(names) >= 2 else (None, None)
        tab, ta, tb, ta_nm, tb_nm = [], 0, 0, 0, 0
        for k in two:
            fa = [f for f in frows.get((k[0], k[1], ra), []) if f.get("kind") in WIH]
            fb = [f for f in frows.get((k[0], k[1], rb_), []) if f.get("kind") in WIH]
            tab.append((k, len(fa), len(fb)))
            ta, tb = ta + len(fa), tb + len(fb)
            ta_nm += sum(1 for f in fa if not f.get("minor"))
            tb_nm += sum(1 for f in fb if not f.get("minor"))
        big = max(ta, tb)
        diff = abs(ta - tb) / big if big else 0.0
        ok5 = diff <= COV_READER_DIFF
        out_c5 = "PASS" if ok5 else "FAIL"
        numbers["c5"] = {"answers": len(two), "reader_a": ra, "reader_b": rb_, "total_a": ta, "total_b": tb, "difference": diff}
        out(f"**C5 two readers on the same answers: {ra} {ta}, {rb_} {tb} wrong + invented + harmful findings on {len(two)} answers "
            f"({fmt(100 * len(two) / len(answers), 0)} % of the answers; the plan asks for at least 25 %); difference {fmt(100 * diff, 0)} % of the larger "
            f"(at most {int(100 * COV_READER_DIFF)} %): {out_c5}**" + ("" if ok5 else " Readings not usable for a bar."))
        out(f"   - without findings marked minor: {ra} {ta_nm}, {rb_} {tb_nm} (not the criterion).")
        out("")
        if not hidden:
            out(f"   | set | answer | {ra} | {rb_} |")
            out("   |---|---|---|---|")
            for k, x, y in tab:
                out(f"   | {k[0]} | {k[1]} | {x} | {y} |")
    else:
        out("**C5 two readers on the same answers: WAITING** (no answer has two readers).")
    cs["C5 two readers agree on totals"] = out_c5
    out("")

    # C6
    if idcheck_false is None:
        cs["C6 identifier check"] = WAITING
        out("**C6 identifier check: WAITING** (no number given; pass the false 'invented' count of the identifier check's report with --idcheck-false-invented N).")
    else:
        cs["C6 identifier check"] = _status(idcheck_false == 0)
        out(f"**C6 identifier check: {idcheck_false} false 'invented' findings** (from the identifier check's own report; must be 0): **{cs['C6 identifier check']}**")
    out("")
    out("| criterion | result |")
    out("|---|---|")
    for k, v in cs.items():
        out(f"| {k} | {v} |")
    out("")
    failed_c = [k for k, v in cs.items() if v == "FAIL"]
    waiting = [k for k, v in cs.items() if v == WAITING]
    go = not failed_c and not waiting
    counts = f"{sum(v == 'PASS' for v in cs.values())} of {len(cs)} criteria passed, {len(failed_c)} failed, {len(waiting)} waiting"
    if waiting:
        out(f"**Waiting for the readers' files and the identifier check's number** ({counts}). No go or no-go while a criterion is waiting.")
    else:
        out(f"**Go or no-go: {'GO' if go else 'NO-GO'}** ({counts}). Go only if all six hold.")
    out("")

    # descriptive
    out("## Descriptive, not criteria")
    out("")
    out("Readers (first reader of each answer; findings marked minor are included): wrong + invented + harmful, harmful and essential omissions "
        "(mapped units), mean per answer, shown per 10 answers with the between-item standard error. Judge: mean over the passes.")
    out("")
    out("| answer set | answers read | reader w+i+h per 10 (SE) | reader harmful (count) | reader omissions per 10 (SE) | judge P (SE) | judge essential-miss units per 10 (SE) | judge supported units per 10 (SE) |")
    out("|---|---|---|---|---|---|---|---|")
    per = {}   # (measure, set) -> {id: value}
    for k in answers:
        for name, val in (("judge_P", sum(cov[(k[0], k[1], p)]["P"] for p in passes if (k[0], k[1], p) in cov) / max(1, sum(1 for p in passes if (k[0], k[1], p) in cov))),
                          ("judge_missed", sum(sum(1 for u in cov[(k[0], k[1], p)]["units"] if u["status"] == "missed") for p in passes if (k[0], k[1], p) in cov)
                           / max(1, sum(1 for p in passes if (k[0], k[1], p) in cov))),
                          ("judge_supported", sum(sum(1 for u in cov[(k[0], k[1], p)]["units"] if u["status"] == "missed" and u["supported"]) for p in passes if (k[0], k[1], p) in cov)
                           / max(1, sum(1 for p in passes if (k[0], k[1], p) in cov)))):
            per.setdefault((name, k[0]), {})[k[1]] = val
        if k in per_answer_reader:
            x = per_answer_reader[k]
            for name, val in (("reader_wih", x["wih"]), ("reader_harmful", x["harmful"]), ("reader_omissions", x["omissions"])):
                per.setdefault((name, k[0]), {})[k[1]] = val
    for s_ in sets:
        def cell(name, scale=10):
            mm, se = _mean_se(list(per.get((name, s_), {}).values()))
            return f"{fmt(scale * mm, 1 if scale == 10 else 2)} ({fmt(scale * se, 1 if scale == 10 else 2)})"
        hv = sum(per.get(("reader_harmful", s_), {}).values())
        out(f"| {s_} | {len(per.get(('reader_wih', s_), {}))} | {cell('reader_wih')} | {int(hv)} | {cell('reader_omissions')} | {cell('judge_P', 1)} | {cell('judge_missed')} | {cell('judge_supported')} |")
    out("")
    if len(sets) >= 2:
        out("Paired difference between answer sets, per item (first minus second), mean (standard error), items in both sets with a value:")
        out("")
        out("| measure | pair | items | mean difference (SE) |")
        out("|---|---|---|---|")
        for name in ("reader_wih", "reader_harmful", "reader_omissions", "judge_P", "judge_missed", "judge_supported"):
            for i, a in enumerate(sets):
                for b in sets[i + 1:]:
                    da, db = per.get((name, a), {}), per.get((name, b), {})
                    d = [da[q] - db[q] for q in sorted(set(da) & set(db))]
                    mm, se = _mean_se(d)
                    out(f"| {name} | {a} - {b} | {len(d)} | {fmt(mm, 3)} ({fmt(se, 3)}) |")
        out("")
    if items is not None:
        L.extend(kind_section(valid, rubrics, items))
    return "\n".join(L) + "\n", {"criteria": dict(cs), "go": go, **numbers}


def _view_verdict(e):
    """A per-point verdict as the coverage role reads it: a demoted failed-quote verdict is 'unverified'."""
    if e["verdict"] == "absent" and e.get("quote_ok") is False and e.get("judge_verdict") in ("present", "contradicted"):
        return "unverified"
    return e["verdict"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True, help="--out file of eval2_judge.py")
    ap.add_argument("--rubrics", default="eval/m4/pilot/rubrics.jsonl", help="first pilot only")
    ap.add_argument("--labels", default="runs/m4/reports/wp2a_labels_checked.jsonl")
    ap.add_argument("--old-dir", default="work/v3/judge", help="directory with results_h2h_*_p1.jsonl and _p2.jsonl")
    ap.add_argument("--omissions", help="JSONL of {set, id}: the reader's essential omissions (default: the built-in list)")
    ap.add_argument("--same-statement", action="store_true",
                    help="first pilot: a labelled item counts as caught only by a flag with an overlapping quote (default: any flag on the answer)")
    ap.add_argument("--v2", action="store_true", help="second pilot: the version 2 report (criteria of the analysis, design change 5)")
    ap.add_argument("--review", help="--v2: review file of eval2_judge.py flags, with confirmed and note filled in")
    ap.add_argument("--findings", help="--v2: the reviewer's findings per answer (JSONL, see the module docstring)")
    ap.add_argument("--title", default="Judge pilot 2")
    ap.add_argument("--probes", help="--v2: --out file of eval2_judge.py run on the probe sets (needs --rubrics, the ORIGINAL rubrics file)")
    ap.add_argument("--chunks", help="--v2: chunk index (JSONL); with --rubrics adds the section on the sources of the must points")
    ap.add_argument("--coverage", action="store_true", help="the coverage role's batch report (criteria C1 to C6; module docstring)")
    ap.add_argument("--reads", help="--coverage: JSONL of {set, id, reader} for answers read with no finding")
    ap.add_argument("--all-read", action="store_true", help="--coverage: every answer was read (by reader r1 unless rows say otherwise)")
    ap.add_argument("--primary-reader", help="--coverage: the reader whose findings are used on answers with two readers")
    ap.add_argument("--idcheck-false-invented", type=int, help="--coverage: false 'invented' findings in the identifier check's report (C6)")
    ap.add_argument("--items", help="--coverage --by-kind: items file (id, kind), open or .enc as the results")
    ap.add_argument("--by-kind", action="store_true",
                    help="--coverage: add totals per item kind (needs --items); kinds with fewer than %d items are merged into 'other'" % BY_KIND_MIN)
    ap.add_argument("--tag-file", help="--coverage: eval/m4/HIDDEN_TAGS.json; every total is printed again without each tag's items")
    a = ap.parse_args(argv)
    if a.tag_file and not a.coverage:
        ap.error("--tag-file needs --coverage")
    if a.coverage:
        if not a.findings:
            ap.error("--coverage needs --findings")
        if a.by_kind and not a.items:
            ap.error("--by-kind needs --items")
        paths = [a.results, a.rubrics, a.findings] + [x for x in (a.reads, a.items) if x]
        if len({ej.is_hidden(x) for x in paths}) > 1:
            print("open and hidden (.enc) files are never mixed in one report (results, rubrics, findings, reads, items)", file=sys.stderr)
            return 2
        hidden = ej.is_hidden(a.results)
        title = a.title if a.title != "Judge pilot 2" else "Coverage role: batch report"
        try:
            results, rubrics, findings = rd(a.results), rd(a.rubrics), rd(a.findings)
            reads = rd(a.reads) if a.reads else None
            items = rd(a.items) if a.by_kind else None
            args = (a.all_read, a.primary_reader, a.idcheck_false_invented)
            data_ids = {r["id"] for r in results} | {r["id"] for r in rubrics}
            try:
                tags = load_tag_file(a.tag_file, data_ids)
            except TagError as e:
                print(f"error: {e}", file=sys.stderr)
                return 2
            text, _ = build_report_coverage(results, rubrics, findings, reads, args[0], args[1], args[2], title, hidden=hidden, items=items)
            result_ids = {r["id"] for r in results}
            for name, tagged, ex in tag_blocks(tags, data_ids):

                def keep(rows):
                    return None if rows is None else [r for r in rows if r["id"] not in ex]
                head = tag_head(name, len(tagged), len(ex), len(result_ids - ex))
                t2, _ = build_report_coverage(keep(results), keep(rubrics), keep(findings), keep(reads), args[0], args[1], args[2], head,
                                              hidden=hidden, items=keep(items))
                text += "\n" + t2
        except Exception as e:
            if not hidden:
                raise
            print(f"hidden report stopped: {type(e).__name__} (details are not shown for hidden input)", file=sys.stderr)
            return 1
        sys.stdout.write(text)
        return 0
    if a.v2:
        rubs = rd(a.rubrics) if (a.probes or a.chunks) else None
        chunks = {c["id"]: c for c in rd(a.chunks)} if a.chunks else None
        text, _ = build_report_v2(rd(a.results), rd(a.review) if a.review else None, rd(a.findings) if a.findings else None, a.title,
                                  rd(a.probes) if a.probes else None, rubs, chunks)
        sys.stdout.write(text)
        return 0
    om = [(r["set"], r["id"]) for r in rd(a.omissions)] if a.omissions else None
    text, _ = build_report(rd(a.results), rd(a.rubrics), rd(a.labels), a.old_dir, om, a.same_statement)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
