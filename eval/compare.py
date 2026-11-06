#!/usr/bin/env python3
"""Answer eval questions with the base model and the fine-tuned adapter, then
optionally have a judge model (any OpenAI-compatible server) score both.

  python eval/compare.py --model Qwen/Qwen3-14B --adapter adapters/kafka-sre-v1 \
      --questions eval/questions.jsonl --judge-url http://localhost:8080/v1

Writes eval/results.jsonl and prints mean scores per category.
"""
import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

import httpx
from mlx_lm import generate, load

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from build_dataset import SYSTEM_PROMPT  # noqa: E402

JUDGE_SYSTEM = """You grade answers from an SRE assistant on Kafka, Strimzi, Kubernetes and \
GitOps. Score each answer 1-5: 5 = correct, specific, safe and actionable; 3 = partly correct \
or vague; 1 = wrong or dangerous. Invented config keys, flags or CRD fields cap the score at 2. \
Use the reference notes if given. Reply ONLY with JSON: {"a": <int>, "b": <int>, "notes": "..."}"""


def answer(model, tok, question, max_tokens):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": question}]
    prompt = tok.apply_chat_template(msgs, add_generation_prompt=True, tokenize=False,
                                     enable_thinking=False)
    out = generate(model, tok, prompt=prompt, max_tokens=max_tokens)
    return re.sub(r"<think>.*?</think>", "", out, flags=re.S).strip()


def judge(url, model, q, ref, a, b):
    user = f"Question:\n{q}\n\nReference notes:\n{ref or '(none)'}\n\nAnswer A:\n{a}\n\nAnswer B:\n{b}"
    r = httpx.post(f"{url.rstrip('/')}/chat/completions", timeout=600, json={
        "model": model, "temperature": 0,
        "messages": [{"role": "system", "content": JUDGE_SYSTEM}, {"role": "user", "content": user}],
    })
    r.raise_for_status()
    txt = re.sub(r"<think>.*?</think>", "", r.json()["choices"][0]["message"]["content"], flags=re.S)
    m = re.search(r"\{.*\}", txt, flags=re.S)
    return json.loads(m.group(0)) if m else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--adapter", required=True)
    ap.add_argument("--questions", nargs="+", default=["eval/questions.jsonl"],
                    help="one or more question files; each is reported separately")
    ap.add_argument("--out", default="eval/results.jsonl")
    ap.add_argument("--max-tokens", type=int, default=1024)
    ap.add_argument("--judge-url")
    ap.add_argument("--judge-model", default="default")
    args = ap.parse_args()

    qs = [dict(json.loads(l), set=Path(p).stem) for p in args.questions
          for l in Path(p).open() if l.strip()]

    rows = []
    for label, adapter in (("base", None), ("tuned", args.adapter)):
        model, tok = load(args.model, adapter_path=adapter)
        for i, q in enumerate(qs):
            if label == "base":
                rows.append(dict(q))
            rows[i][label] = answer(model, tok, q["question"], args.max_tokens)
            print(f"[{label}] {i + 1}/{len(qs)}")
        del model

    scores = defaultdict(lambda: {"base": [], "tuned": []})  # keyed by (set, category)
    for r in rows:
        if args.judge_url:
            # randomise which answer is shown first to reduce position bias
            swap = hash(r["question"]) % 2
            a, b = (r["tuned"], r["base"]) if swap else (r["base"], r["tuned"])
            j = judge(args.judge_url, args.judge_model, r["question"], r.get("reference"), a, b)
            if j:
                r["score_base"], r["score_tuned"] = (j["b"], j["a"]) if swap else (j["a"], j["b"])
                r["judge_notes"] = j.get("notes")
                cat = (r["set"], r.get("category", "all"))
                scores[cat]["base"].append(r["score_base"])
                scores[cat]["tuned"].append(r["score_tuned"])

    with open(args.out, "w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"wrote {args.out}")
    for qset in dict.fromkeys(r["set"] for r in rows):
        cats = {c: s for (st, c), s in sorted(scores.items()) if st == qset}
        allb = [x for s in cats.values() for x in s["base"]]
        allt = [x for s in cats.values() for x in s["tuned"]]
        print(f"== {qset} ==")
        for cat, s in cats.items():
            print(f"{cat:20s} base {statistics.mean(s['base']):.2f}  tuned {statistics.mean(s['tuned']):.2f}  (n={len(s['base'])})")
        if allb:
            print(f"{'OVERALL':20s} base {statistics.mean(allb):.2f}  tuned {statistics.mean(allt):.2f}  (n={len(allb)})")


if __name__ == "__main__":
    main()
