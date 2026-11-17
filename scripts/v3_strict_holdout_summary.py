#!/usr/bin/env python3
"""Strict mode against default mode on the runbook holdout (42 questions): markdown report on stdout.
  v3_strict_holdout_summary.py > runs/v3/STRICT_HOLDOUT_CHECK.md
Difference = strict minus default (default is the baseline slot of the head-to-head file), GLM judge, two passes (the second flipped).
The result line states the measured difference and its standard error only; it declares no winner.
Pure helpers (result_line, shortest, gap_count, mean_tokens) are tested in tests/test_v3_strict_holdout_summary.py."""
import json, statistics as st, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v3_ship_summary import aggregate, both_passes, pass_deltas  # noqa: E402
from v3_v2rag_summary import parse_invention  # noqa: E402
from pack_filters import gap_sentence_count  # noqa: E402

W = Path("work/v3")
J = W / "judge"
S = W / "strict"
NAME = "holdout_strict_vs_default"
HOLDOUT = "eval/questions_runbooks_holdout.jsonl"
DEFAULT_ANS = W / "ans_rag_holdout_s1.jsonl"
STRICT_ANS = S / "ans_rag_holdout_strict.jsonl"


def result_line(agg, n_expected):
    if agg["n"] != n_expected:
        return f"result: INCOMPLETE, {agg['n']} of {n_expected} questions scored in both passes"
    return (f"result: strict minus default, two passes, {agg['n']} holdout questions: mean {agg['mean']:+.2f}, "
            f"standard error {agg['se']:.2f}, wins / ties / losses {agg['wins']} / {agg['ties']} / {agg['losses']}")


def mean_tokens(rows):
    return st.mean(r.get("tokens") or 0 for r in rows) if rows else None


def gap_count(rows):
    """Number of answers containing at least one 'The retrieved documentation does not cover ...' sentence."""
    return sum(gap_sentence_count(r.get("text")) > 0 for r in rows)


def shortest(rows, num, limit=10):
    """[(question number, tokens)] of the `limit` shortest answers, shortest first (ties by question number)."""
    return sorted(((num[r["question"]], r.get("tokens") or 0) for r in rows if r["question"] in num), key=lambda t: (t[1], t[0]))[:limit]


def _rows(path):
    p = Path(path)
    return [json.loads(l) for l in p.open() if l.strip()] if p.exists() else []


def _line(label, d):
    a = aggregate(d.values() if isinstance(d, dict) else d)
    if not a["n"]:
        return f"| {label} | 0 | - | - | - |"
    return f"| {label} | {a['n']} | {a['mean']:+.2f} | {a['se']:.2f} | {a['wins']} / {a['ties']} / {a['losses']} |"


def _inv(name):
    f = S / f"invention_holdout_{name}.txt"
    return parse_invention(f.read_text()) if f.exists() else (None, None)


def _fmt(x):
    return "?" if x is None else (f"{x:.1f}" if isinstance(x, float) else str(x))


def main():
    qs = [r["question"] for r in _rows(HOLDOUT)]
    num = {q: i for i, q in enumerate(qs, 1)}
    p = {i: pass_deltas(_rows(J / f"judge_h2h_{NAME}_p{i}.cache.jsonl"), flip) for i, flip in ((1, 0), (2, 1))}
    both = both_passes(p[1], p[2])
    agg = aggregate(both.values())
    dflt, strict = _rows(DEFAULT_ANS), _rows(STRICT_ANS)
    out = [result_line(agg, len(qs)) + "\n", "# Strict mode against default mode on the runbook holdout\n"]
    out.append("Judge GLM-5.3-Flash-oQ8e-mtp, two passes (the second flipped), 42 runbook-holdout questions, frozen stage-1 index, k6, "
               "same packs; difference = strict minus default. Only questions scored in both passes count in the two-pass row. "
               "Unscored items: " + ", ".join(f"pass {i} {len(qs) - len(p[i])}" for i in (1, 2)) + ".\n")
    out.append("| Set | n | mean | standard error | wins / ties / losses |\n|---|---|---|---|---|")
    out += [_line("Pass 1", p[1]), _line("Pass 2", p[2]), _line("Both passes", both)]
    out.append("\nMean answer tokens: default " + _fmt(mean_tokens(dflt)) + f" ({len(dflt)} answers), strict " + _fmt(mean_tokens(strict))
               + f" ({len(strict)} answers).")
    out.append("\nMechanical check (scripts/invention_count.py, answers against the packs they were written from; "
               "answers with an identifier absent from their pack / absent from the whole index):")
    for label, key in (("default", "default"), ("strict", "strict")):
        a, b = _inv(key)
        out.append(f"- {label}: {_fmt(a)} / {_fmt(b)}")
    out.append(f"\nAnswers with a 'The retrieved documentation does not cover ...' statement: default {gap_count(dflt)} of {len(dflt)}, "
               f"strict {gap_count(strict)} of {len(strict)}.")
    out.append("\nTen shortest strict answers (over-refusal candidates; a reviewer reads these), question number: tokens:")
    out.append(", ".join(f"Q{n}: {t}" for n, t in shortest(strict, num)) or "none")
    out.append("\nA reviewer still has to read answers for invented or wrong items; the mechanical count only sees identifiers absent "
               "from the pack or the index. The decision on the default mode is the Owner's. Differences under about 0.2 are noise "
               "(standard error about 0.2 on 40 questions). The holdout is not questions_real.")
    print("\n".join(out))


if __name__ == "__main__":
    main()
