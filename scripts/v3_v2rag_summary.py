#!/usr/bin/env python3
"""Can the v2 adapter be released as a retrieval-only model? Markdown report on stdout.
  v3_v2rag_summary.py > runs/v3/V2RAG_CHECK.md
Contestants against base + retrieval on the frozen stage-1 index (work/v3/chunks_s1.jsonl), 40 real questions:
  (a) v2 with its own search terms and packs, (b) v2 answering from the base model's packs.
Rule (the Owner's go, 2026-11-17): HOLDS if (a) is at -0.2 or better over both judge passes AND its count of answers
with an identifier absent from the pack is not higher than base + retrieval's. The decoding and aggregation
functions come from v3_ship_summary.py; the pure parts here (holds_rule, recall_scores, count_identical, ...)
are tested in tests/test_v3_v2rag_summary.py."""
import json, re, statistics as st, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v3_ship_summary import SET_ASIDE, aggregate, both_passes, pass_deltas  # noqa: E402

W = Path("work/v3")
J = W / "judge"
V = W / "v2rag"
INDEX = W / "chunks_s1.jsonl"
BASE_ANS = W / "ans_rag_real_s1.jsonl"
BASE_PACKS = W / "packs_real_s1.jsonl"
OWN_PACKS = V / "packs_v2exp.jsonl"
THRESHOLD = -0.2   # a tie or better, given a standard error of about 0.2
EPS = 1e-9


def holds_rule(mean, own_count, base_count, n=40, n_expected=40):
    """(holds, reason). mean = two-pass mean difference of v2 (own search terms) minus base + retrieval;
    counts = answers with an identifier absent from the pack (None = unknown)."""
    if own_count is None or base_count is None:
        return False, "the mechanical count is missing"
    if n != n_expected or mean != mean:
        return False, f"judging incomplete: {n} of {n_expected} questions scored in both passes"
    reasons = []
    if mean < THRESHOLD - EPS:
        reasons.append(f"two-pass mean {mean:+.2f} is below {THRESHOLD:+.1f}")
    if own_count > base_count:
        reasons.append(f"answers with an identifier absent from the pack: {own_count} against {base_count} for base + retrieval")
    if reasons:
        return False, "; ".join(reasons)
    return True, (f"two-pass mean {mean:+.2f} (at {THRESHOLD:+.1f} or better), answers with an identifier absent from the pack "
                  f"{own_count} against {base_count} for base + retrieval")


def recall_scores(packs, facts, answerable, chunks, questions, k="k6"):
    """{question number: share of the question's regexes found in the lowercased title + full text of the pack's chunks}
    for the answerable questions. A hit is a share of 1.0, a miss 0.0, anything between is partial.
    packs: {question text: pack row}; chunks: {id: chunk row}; questions: list of question texts (number = position + 1)."""
    out = {}
    for n in answerable:
        pack = packs.get(questions[n - 1])
        if pack is None:
            continue
        text = " ".join(((chunks[i].get("title") or "") + " " + chunks[i]["text"]) for i in pack[k] if i in chunks).lower()
        rx = facts[str(n)]
        out[n] = sum(bool(re.search(x, text)) for x in rx) / len(rx)
    return out


def recall_counts(scores):
    v = list(scores.values())
    return {"hits": sum(x >= 1 - EPS for x in v), "partial": sum(EPS < x < 1 - EPS for x in v), "miss": sum(x <= EPS for x in v), "n": len(v)}


def count_identical(a, b, k="k6"):
    """Number of questions whose k-list is the same in both pack sets (same chunks in the same order), and the questions compared."""
    both = [q for q in a if q in b]
    return sum(a[q][k] == b[q][k] for q in both), len(both)


def parse_invention(text):
    """(answers with an identifier absent from the pack, absent from the corpus) from scripts/invention_count.py output, or None."""
    a = re.search(r"answers with >=1 token absent from pack: (\d+)", text)
    b = re.search(r"answers with >=1 token absent from corpus: (\d+)", text)
    return (int(a.group(1)) if a else None, int(b.group(1)) if b else None)


def big_differences(per_question, num, limit=2):
    """[(question number, difference)] with |difference| >= limit, sorted by number."""
    return sorted((num[q], v) for q, v in per_question.items() if abs(v) >= limit - EPS)


def median_chars(rows):
    return int(st.median(len(r.get("text") or "") for r in rows)) if rows else None


def _rows(path):
    p = Path(path)
    return [json.loads(l) for l in p.open() if l.strip()] if p.exists() else []


def _by_question(path):
    return {r["question"]: r for r in _rows(path)}


def _line(label, d):
    a = aggregate(d.values() if isinstance(d, dict) else d)
    if not a["n"]:
        return f"| {label} | 0 | - | - | - |"
    return f"| {label} | {a['n']} | {a['mean']:+.2f} | {a['se']:.2f} | {a['wins']} / {a['ties']} / {a['losses']} |"


def _judged(name):
    p = {i: pass_deltas(_rows(J / f"judge_h2h_{name}_p{i}.cache.jsonl"), flip) for i, flip in ((1, 0), (2, 1))}
    return p, both_passes(p[1], p[2])


def _invention(name):
    f = V / f"invention_real_{name}.txt"
    return parse_invention(f.read_text()) if f.exists() else (None, None)


def main():
    real = _rows("eval/questions_real.jsonl")
    questions = [r["question"] for r in real]
    num = {q: i for i, q in enumerate(questions, 1)}
    cat = {r["question"]: r["category"] for r in real}
    out = []

    own_p, own = _judged("v2ownexp_s1")
    bas_p, bas = _judged("v2baseexp_s1")
    own_mean = st.mean(own.values()) if own else float("nan")
    inv = {k: _invention(k) for k in ("ragbase", "v2ownexp", "v2baseexp")}
    holds, reason = holds_rule(own_mean, inv["v2ownexp"][0], inv["ragbase"][0], n=len(own), n_expected=len(real))
    head = "result: HOLDS" if holds else "result: DOES NOT HOLD"

    out.append("Judge GLM-5.3-Flash-oQ8e-mtp, two passes (the second flipped), 40 real questions, difference = contestant minus base + retrieval "
               "(frozen stage-1 index, k6, base model's search terms). Only questions scored in both passes count in the two-pass rows.\n")
    out.append("| Contestant and set | n | mean | standard error | wins / ties / losses |\n|---|---|---|---|---|")
    for label, p, b in (("(a) v2, own search terms", own_p, own), ("(b) v2, base model's search terms", bas_p, bas)):
        out.append(_line(f"{label}, pass 1", p[1]))
        out.append(_line(f"{label}, pass 2", p[2]))
        out.append(_line(f"{label}, both passes", b))
        out.append(_line(f"{label}, both passes without 29, 37, 38, 39", {q: v for q, v in b.items() if num.get(q) not in SET_ASIDE}))
    out.append("\nPer category, both passes (n, mean (a), mean (b)):")
    for c in sorted(set(cat.values())):
        av = [v for q, v in own.items() if cat.get(q) == c]
        bv = [v for q, v in bas.items() if cat.get(q) == c]
        out.append(f"- {c}: n={len(av)}, (a) " + (f"{st.mean(av):+.2f}" if av else "-") + ", (b) " + (f"{st.mean(bv):+.2f}" if bv else "-"))

    base_rows, own_rows, bas_rows = _rows(BASE_ANS), _rows(V / "ans_v2_ownexp.jsonl"), _rows(V / "ans_v2_baseexp.jsonl")
    out.append("\nMedian answer length (characters, 40 answers each) and the mechanical check "
               "(answers with an identifier absent from their own pack / absent from the whole index; scripts/invention_count.py):")
    for label, rows, key in (("base + retrieval", base_rows, "ragbase"), ("v2, own search terms", own_rows, "v2ownexp"),
                             ("v2, base model's search terms", bas_rows, "v2baseexp")):
        a, b = inv[key]
        out.append(f"- {label}: {median_chars(rows)} characters; {'?' if a is None else a} / {'?' if b is None else b}")

    chunks = {r["id"]: r for r in _rows(INDEX)}
    facts = json.loads((W / "phase0_facts.json").read_text())
    answerable = json.loads((W / "phase0_answerable.json").read_text())["answerable"]
    bp, op = _by_question(BASE_PACKS), _by_question(OWN_PACKS)
    sb = recall_scores(bp, facts, answerable, chunks, questions)
    so = recall_scores(op, facts, answerable, chunks, questions)
    cb, co = recall_counts(sb), recall_counts(so)
    ident, compared = count_identical(bp, op)
    out.append(f"\nObjective recall of the packs (k6, frozen index), answerable questions ({len(answerable)}): a hit is every fact regex found "
               "in the lowercased title + full text of the pack's chunks.")
    out.append(f"- base model's search terms: {cb['hits']} hits, {cb['partial']} partial, {cb['miss']} miss (of {cb['n']})")
    out.append(f"- v2's own search terms: {co['hits']} hits, {co['partial']} partial, {co['miss']} miss (of {co['n']})")
    stored = Path(W / "recall_objective_s1.json")
    note = "method not confirmed"
    if stored.exists():
        ref = json.loads(stored.read_text()).get("k6", {})
        pq = ref.get("per_question", {})
        same = sum(abs(round(sb.get(int(n), -1), 2) - v) < 0.011 for n, v in pq.items())
        note = (f"method check: recomputing the base model's packs reproduces {same} of {len(pq)} stored per-question values and "
                f"{cb['hits']} hits (stored {ref.get('hits', '?')}); the method text itself is not in that file, it was inferred from the values")
    out.append(f"- {note}")
    out.append(f"- packs identical between the two pack sets (same k6 chunk ids in the same order): {ident} of {compared}")

    big = big_differences(own, num)
    out.append("\nQuestions where v2 with its own search terms differs from base + retrieval by 2 or more (question number: difference): "
               + (", ".join(f"Q{n}: {v:+.1f}" for n, v in big) or "none"))
    out.append("\nA reviewer still has to read 10 answers for invented identifiers before a release decision; the mechanical count only "
               "sees identifiers absent from the pack or the index.")
    out.append("\nNote: questions_real has been used many times; this result leans on it again. Differences under about 0.2 are noise.")

    print(head + "\n")
    print("# v2 adapter as a retrieval-only model: check on the frozen index\n")
    print(f"Reason: {reason}\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
