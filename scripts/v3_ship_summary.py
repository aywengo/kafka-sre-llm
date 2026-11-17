#!/usr/bin/env python3
"""Shipping checks of v3 (retrieval on the base model, frozen stage-1 index): markdown report on stdout.
  v3_ship_summary.py > runs/v3/SHIP_CHECKS.md
(a) judging of base + retrieval against the plain base model, two passes; (b) the smoke test of the runtime
(scripts/v3_ship_smoke.py output) against the stored search terms, packs and answers; (c) the result line.
The pure functions (decode_cache_row, aggregate, first_diff) are imported by tests/test_v3_ship_summary.py."""
import hashlib, json, math, statistics as st
from pathlib import Path

J = Path("work/v3/judge")
S = Path("work/v3/ship")
NAME = "ragbase_s1_vs_base"
SET_ASIDE = {29, 37, 38, 39}   # question numbers (1-based, eval/questions_real.jsonl) the corpus additions were aimed at
# Phase 0 on the old index, RAG base k6 against plain base, two passes, 40 questions: +1.19, wins/ties/losses 26/8/6
# (source: runs/v3/PHASE0_REPORT.md, table row "RAG base k6" and decision point 2).
PHASE0 = {"mean": 1.19, "wins": 26, "ties": 8, "losses": 6}
N_SMOKE = 3


def decode_cache_row(row, flip):
    """(base_score, variant_score) of one judge-cache row, or None if unscored. Same decoding as
    scripts/s1_ceiling_summary.py: the variant is shown as A when md5(question) is odd, XOR flip (pass 2)."""
    j = row.get("j")
    if not j:
        return None
    swap = (int(hashlib.md5(row["question"].encode()).hexdigest(), 16) % 2) ^ int(bool(flip))
    return (j["b"], j["a"]) if swap else (j["a"], j["b"])


def aggregate(deltas):
    """n, mean, se (population stdev / sqrt n, as in the ceiling summary), wins, ties, losses."""
    v = list(deltas)
    if not v:
        return {"n": 0, "mean": float("nan"), "se": float("nan"), "wins": 0, "ties": 0, "losses": 0}
    return {"n": len(v), "mean": st.mean(v), "se": st.pstdev(v) / math.sqrt(len(v)),
            "wins": sum(x > 0 for x in v), "ties": sum(x == 0 for x in v), "losses": sum(x < 0 for x in v)}


def first_diff(a, b):
    """Index of the first differing character of two strings, or None if equal (a shorter prefix differs at len)."""
    if a == b:
        return None
    n = min(len(a), len(b))
    return next((i for i in range(n) if a[i] != b[i]), n)


def pass_deltas(rows, flip):
    """{question: variant - base} for the scored rows of one pass."""
    out = {}
    for r in rows:
        d = decode_cache_row(r, flip)
        if d is not None:
            out[r["question"]] = d[1] - d[0]
    return out


def both_passes(p1, p2):
    """Mean delta per question, only for questions scored in both passes."""
    return {q: (p1[q] + p2[q]) / 2 for q in p1 if q in p2}


def _rows(path):
    p = Path(path)
    return [json.loads(l) for l in p.open() if l.strip()] if p.exists() else []


def _line(label, d):
    a = aggregate(d.values() if isinstance(d, dict) else d)
    if not a["n"]:
        return f"| {label} | 0 | - | - | - |"
    return f"| {label} | {a['n']} | {a['mean']:+.2f} | {a['se']:.2f} | {a['wins']} / {a['ties']} / {a['losses']} |"


def main():
    real = [json.loads(l) for l in open("eval/questions_real.jsonl") if l.strip()]
    num = {q["question"]: i for i, q in enumerate(real, 1)}
    cat = {q["question"]: q["category"] for q in real}
    reasons = []
    out = []
    p = {}
    for i, flip in ((1, 0), (2, 1)):
        p[i] = pass_deltas(_rows(J / f"judge_h2h_{NAME}_p{i}.cache.jsonl"), flip)
        if len(p[i]) != len(real):
            reasons.append(f"judge pass {i} scored {len(p[i])} of {len(real)}")
    both = both_passes(p[1], p[2])
    out.append("## (a) Judging: base + retrieval (frozen stage-1 index, k6) minus plain base\n")
    out.append("GLM-5.3-Flash-oQ8e-mtp, difference = base + retrieval minus plain base, 40 real questions; "
               "pass 2 has the answer order flipped. Only questions scored in both passes count in the two-pass rows.\n")
    out.append("| Set | n | mean | standard error | wins / ties / losses |\n|---|---|---|---|---|")
    out.append(_line("Pass 1", p[1]))
    out.append(_line("Pass 2", p[2]))
    out.append(_line("Both passes (mean per question)", both))
    out.append(_line("Both passes without 29, 37, 38, 39", {q: v for q, v in both.items() if num.get(q) not in SET_ASIDE}))
    out.append(f"| Phase 0, old index, RAG base k6, both passes (runs/v3/PHASE0_REPORT.md) | 40 | {PHASE0['mean']:+.2f} | - | "
               f"{PHASE0['wins']} / {PHASE0['ties']} / {PHASE0['losses']} |")
    out.append("\nPer category, both passes (n, mean, wins / ties / losses):")
    for c in sorted(set(cat.values())):
        a = aggregate(v for q, v in both.items() if cat.get(q) == c)
        out.append(f"- {c}: n={a['n']}, " + (f"{a['mean']:+.2f}, {a['wins']} / {a['ties']} / {a['losses']}" if a["n"] else "-"))

    out.append("\n## (b) Smoke test: the runtime's real path (mlx, base model, frozen index, k6, default mode)\n")
    exp = {r["question"]: r.get("expansion") for r in _rows("work/v3/real_exp_base.jsonl")}
    pack = {r["question"]: r.get("k6") for r in _rows("work/v3/packs_real_s1.jsonl")}
    ans = {r["question"]: r.get("text") for r in _rows("work/v3/ans_rag_real_s1.jsonl")}
    out.append("| question | search terms equal | chunk ids equal (stored k6) | answer equal | expand s | retrieve s | answer s |\n|---|---|---|---|---|---|---|")
    notes = []
    for n in range(1, N_SMOKE + 1):
        f = S / f"smoke_{n}.json"
        if not f.exists():
            out.append(f"| {n} | missing | missing | missing | - | - | - |")
            reasons.append(f"smoke test {n}: no result file")
            continue
        r = json.loads(f.read_text())
        q = r["question"]
        e_ok = r.get("expansion") == exp.get(q)
        c_ok = r.get("chunk_ids") == pack.get(q)
        a_ok = r.get("text") == ans.get(q)
        t = r.get("timings", {})
        out.append(f"| {n} | {'yes' if e_ok else 'NO'} | {'yes' if c_ok else 'NO'} | {'yes' if a_ok else 'NO'} | "
                   f"{t.get('expand_s', float('nan')):.1f} | {t.get('retrieve_s', float('nan')):.1f} | {t.get('answer_s', float('nan')):.1f} |")
        if q not in ans:
            reasons.append(f"smoke test {n}: question not in the stored answers")
        if not e_ok:
            reasons.append(f"smoke test {n}: search terms differ")
            notes.append(f"- question {n} search terms: runtime {len(r.get('expansion') or '')} characters, stored {len(exp.get(q) or '')}; "
                         f"first difference at {first_diff(r.get('expansion') or '', exp.get(q) or '')}")
        if not c_ok:
            reasons.append(f"smoke test {n}: chunk ids differ")
            notes.append(f"- question {n} chunk ids: runtime {r.get('chunk_ids')}, stored {pack.get(q)}")
        if not a_ok:
            reasons.append(f"smoke test {n}: answer text differs")
            a, b = r.get("text") or "", ans.get(q) or ""
            notes.append(f"- question {n} answer: first difference at character {first_diff(a, b)}; runtime {len(a)} characters, stored {len(b)}")
    if notes:
        out.append("\nDifferences:\n" + "\n".join(notes))
    out.append("\nNote: 34 of the 40 stored answers were generated on the old index with identical packs and 6 were regenerated, "
               "all greedy, so an exact match is expected.")
    head = "result: PASS" if not reasons else "result: CHECK (" + "; ".join(reasons) + ")"
    print("# Shipping checks of v3\n")
    print(head + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
