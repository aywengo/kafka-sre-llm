#!/usr/bin/env python3
"""Summary of the stage-1 ceiling check (markdown on stdout). Applies the rule fixed in agents/train-v3-stage1.md:
stop if the teacher is under +0.3 on the real questions, or its wins do not exceed its losses, or the pooled
real + holdout result is under +0.3; go if it is at least +0.4 on the real questions and at least 0 on the
holdout; otherwise in between (Roman decides). The prompted student at +0.2 or more means: ship the prompt."""
import collections, hashlib, json, re, statistics as st
from pathlib import Path

J = Path("work/v3/judge")
SET_ASIDE = {29, 37, 38, 39}   # questions whose topics the corpus additions were aimed at


def deltas(name):
    """{question: [variant - base+retrieval per pass]}, and per-pass (n, mean, wins, ties, losses)."""
    per, passes = collections.defaultdict(list), []
    for p, flip in (("p1", 0), ("p2", 1)):
        f = J / f"judge_h2h_{name}_{p}.cache.jsonl"
        d = []
        for l in f.open() if f.exists() else []:
            r = json.loads(l)
            j = r.get("j")
            if not j:
                continue
            swap = (int(hashlib.md5(r["question"].encode()).hexdigest(), 16) % 2) ^ flip
            b, t = (j["b"], j["a"]) if swap else (j["a"], j["b"])
            d.append(t - b)
            per[r["question"]].append(t - b)
        passes.append((len(d), st.mean(d) if d else float("nan"), sum(x > 0 for x in d), sum(x == 0 for x in d), sum(x < 0 for x in d)))
    return per, passes


def both(per):
    return {q: st.mean(v) for q, v in per.items() if len(v) == 2}


def line(label, d):
    v = list(d.values())
    if not v:
        return f"| {label} | 0 | - | - | - |"
    se = st.pstdev(v) / len(v) ** 0.5
    return f"| {label} | {len(v)} | {st.mean(v):+.2f} | {se:.2f} | {sum(x > 0 for x in v)} / {sum(x == 0 for x in v)} / {sum(x < 0 for x in v)} |"


real = [json.loads(l) for l in open("eval/questions_real.jsonl") if l.strip()]
num = {q["question"]: i for i, q in enumerate(real, 1)}
cat = {q["question"]: q["category"] for q in real}
tp, tpass = deltas("teacher_real"); t = both(tp)
sp, spass = deltas("student_real"); s = both(sp)
hp, hpass = deltas("teacher_holdout"); h = both(hp)
pooled = list(t.values()) + list(h.values())
tm = st.mean(t.values()) if t else float("nan")
hm = st.mean(h.values()) if h else float("nan")
pm = st.mean(pooled) if pooled else float("nan")
wins, losses = sum(x > 0 for x in t.values()), sum(x < 0 for x in t.values())
if tm < 0.3 or wins <= losses or pm < 0.3:
    verdict = "stop"
elif tm >= 0.4 and hm >= 0:
    verdict = "go"
else:
    verdict = "in between (Roman decides)"
sm = st.mean(s.values()) if s else float("nan")
print("# Stage 1 ceiling check: teacher and prompted student against base + retrieval\n")
print(f"verdict: {verdict}")
print(f"\nRule inputs: teacher on the real questions {tm:+.2f} (wins {wins}, losses {losses}); holdout {hm:+.2f}; pooled {pm:+.2f} over {len(pooled)} questions.")
print(f"Prompted student on the real questions: {sm:+.2f}" + (" (at or above +0.2: the rule says ship the prompt)" if sm >= 0.2 else " (under +0.2)") + ".")
print("\nGLM-5.3-Flash-oQ8e-mtp, two passes (second flipped), difference = contestant minus base + retrieval, same packs. Only questions scored in both passes are counted.\n")
print("| Contestant and set | n | mean | standard error | wins / ties / losses |\n|---|---|---|---|---|")
print(line("Teacher, real questions", t))
print(line("Teacher, real questions without 29, 37, 38, 39", {q: v for q, v in t.items() if num[q] not in SET_ASIDE}))
print(line("Teacher, runbook holdout", h))
print(line("Prompted student, real questions", s))
print(line("Prompted student, real questions without 29, 37, 38, 39", {q: v for q, v in s.items() if num[q] not in SET_ASIDE}))
print("\nPer pass (n scored, mean, wins / ties / losses):")
for label, ps in (("teacher real", tpass), ("student real", spass), ("teacher holdout", hpass)):
    print(f"- {label}: " + "; ".join(f"pass {i}: n={n}, {m:+.2f}, {w}/{ti}/{lo}" for i, (n, m, w, ti, lo) in enumerate(ps, 1)))
print("\nPer category on the real questions (n, teacher, prompted student):")
for c in sorted(set(cat.values())):
    tv = [v for q, v in t.items() if cat[q] == c]; sv = [v for q, v in s.items() if cat[q] == c]
    print(f"- {c}: n={len(tv)}, teacher {st.mean(tv):+.2f}" if tv else f"- {c}: n=0", end="")
    print(f", student {st.mean(sv):+.2f}" if sv else "")
big = sorted((v, num[q]) for q, v in t.items() if abs(v) >= 2)
print("\nTeacher differences of 2 or more (question number: difference): " + (", ".join(f"Q{n}: {v:+.1f}" for v, n in big) or "none"))
big = sorted((v, num[q]) for q, v in s.items() if abs(v) >= 2)
print("Prompted-student differences of 2 or more: " + (", ".join(f"Q{n}: {v:+.1f}" for v, n in big) or "none"))
print("\nAnswer length (median characters) and the mechanical check (answers with an identifier absent from their pack / absent from the whole index, of 40):")
for label, ans, inv in (("base + retrieval", "work/v3/ans_rag_real_s1.jsonl", "ragbase"), ("teacher", "work/v3/ceiling/teacher_real_answers.jsonl", "teacher"),
                        ("prompted student", "work/v3/ceiling/ans_student_rules_real.jsonl", "student")):
    rows = [json.loads(l) for l in open(ans)]
    txt = Path(f"work/v3/ceiling/invention_real_{inv}.txt").read_text() if Path(f"work/v3/ceiling/invention_real_{inv}.txt").exists() else ""
    a = re.search(r"absent from pack: (\d+)", txt); b = re.search(r"absent from corpus: (\d+)", txt)
    print(f"- {label}: {int(st.median(len(r['text']) for r in rows))} characters; {a.group(1) if a else '?'} / {b.group(1) if b else '?'}")
cov = collections.Counter(json.loads(l).get("coverage") for l in open("work/v3/ceiling/teacher_real.jsonl"))
print(f"\nTeacher coverage labels on the real questions: {dict(cov)}.")
print("\nNote: on the holdout, base + retrieval already scores about 4.5 of 5, so that set has little room above the baseline; the pooled figure is pulled towards it.")
