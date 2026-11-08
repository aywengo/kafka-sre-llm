#!/usr/bin/env python3
"""Answer eval questions with the base model and the fine-tuned adapter, then
optionally have a judge model (any OpenAI-compatible server) score both.

  python eval/compare.py --model Qwen/Qwen3-14B --adapter adapters/kafka-sre-v1 \
      --questions eval/questions.jsonl --judge-url http://localhost:8080/v1

Writes eval/results.jsonl and prints mean scores per category.
"""
import argparse
import hashlib
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

import httpx
from mlx_lm import load, stream_generate

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from build_dataset import SYSTEM_PROMPT  # noqa: E402

JUDGE_SYSTEM = """You grade answers from an SRE assistant on Kafka, Strimzi, Kubernetes and \
GitOps. Score each answer 1-5: 5 = correct, specific, safe and actionable; 3 = partly correct \
or vague; 1 = wrong or dangerous. Invented config keys, flags or CRD fields cap the score at 2. \
Score content quality only. Some answers may be cut off at the end because of a length \
limit; do not penalise an answer for being cut off. \
Use the reference notes if given. Reply ONLY with JSON: {"a": <int>, "b": <int>, "notes": "..."}"""


def answer(model, tok, question, max_tokens):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": question}]
    prompt = tok.apply_chat_template(msgs, add_generation_prompt=True, tokenize=False,
                                     enable_thinking=False)
    text, last = "", None
    for last in stream_generate(model, tok, prompt=prompt, max_tokens=max_tokens):
        text += last.text
    return {"text": re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip(),
            "tokens": last.generation_tokens, "truncated": last.finish_reason == "length"}


def judge(url, model, q, ref, a, b, api_key=None, extra_body=None):
    user = f"Question:\n{q}\n\nReference notes:\n{ref or '(none)'}\n\nAnswer A:\n{a}\n\nAnswer B:\n{b}"
    r = httpx.post(f"{url.rstrip('/')}/chat/completions", timeout=600,
                   headers={"Authorization": f"Bearer {api_key}"} if api_key else {}, json={
        **(extra_body or {}),
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
    ap.add_argument("--judge-api-key")
    ap.add_argument("--judge-extra-body", type=json.loads, default=None,
                    help='JSON merged into judge requests, e.g. \'{"chat_template_kwargs": {"thinking_mode": "chat"}}\'')
    ap.add_argument("--cache", help="JSONL of generated answers; written as they are produced and "
                                    "reused on rerun, so generation can resume and judging can run later")
    args = ap.parse_args()

    qs = [dict(json.loads(l), set=Path(p).stem) for p in args.questions
          for l in Path(p).open() if l.strip()]

    cache = {}
    if args.cache and Path(args.cache).exists():
        for l in Path(args.cache).open():
            c = json.loads(l)
            cache[(c["label"], c["question"])] = c
    rows = [dict(q) for q in qs]
    for label, adapter in (("base", None), ("tuned", args.adapter)):
        todo = [i for i, q in enumerate(qs) if (label, q["question"]) not in cache]
        if todo:
            model, tok = load(args.model, adapter_path=adapter)
            for n, i in enumerate(todo):
                entry = {"label": label, "question": qs[i]["question"],
                         **answer(model, tok, qs[i]["question"], args.max_tokens)}
                cache[(label, qs[i]["question"])] = entry
                if args.cache:
                    with open(args.cache, "a") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                print(f"[{label}] {n + 1}/{len(todo)}", flush=True)
            del model
        for i, q in enumerate(qs):
            e = cache[(label, q["question"])]
            rows[i][label], rows[i][f"{label}_tokens"], rows[i][f"{label}_truncated"] = \
                e["text"], e["tokens"], e["truncated"]

    scores = defaultdict(lambda: {"base": [], "tuned": []})  # keyed by (set, category)
    clean = defaultdict(lambda: {"base": [], "tuned": []})   # pairs where neither answer was truncated
    for r in rows:
        if args.judge_url:
            # randomise which answer is shown first to reduce position bias
            swap = int(hashlib.md5(r["question"].encode()).hexdigest(), 16) % 2
            a, b = (r["tuned"], r["base"]) if swap else (r["base"], r["tuned"])
            j = judge(args.judge_url, args.judge_model, r["question"], r.get("reference"), a, b,
                      args.judge_api_key, args.judge_extra_body)
            if j:
                r["score_base"], r["score_tuned"] = (j["b"], j["a"]) if swap else (j["a"], j["b"])
                r["judge_notes"] = j.get("notes")
                cat = (r["set"], r.get("category", "all"))
                scores[cat]["base"].append(r["score_base"])
                scores[cat]["tuned"].append(r["score_tuned"])
                if not (r["base_truncated"] or r["tuned_truncated"]):
                    clean[cat]["base"].append(r["score_base"])
                    clean[cat]["tuned"].append(r["score_tuned"])

    with open(args.out, "w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"wrote {args.out}")
    for qset in dict.fromkeys(r["set"] for r in rows):
        rs = [r for r in rows if r["set"] == qset]
        print(f"== {qset} == truncated: base {sum(r['base_truncated'] for r in rs)}/{len(rs)}, "
              f"tuned {sum(r['tuned_truncated'] for r in rs)}/{len(rs)}")
        for title, sc in (("all pairs", scores), ("pairs with no truncated answer", clean)):
            cats = {c: v for (st, c), v in sorted(sc.items()) if st == qset}
            allb = [x for v in cats.values() for x in v["base"]]
            allt = [x for v in cats.values() for x in v["tuned"]]
            print(f"-- {title}")
            for cat, v in cats.items():
                print(f"{cat:20s} base {statistics.mean(v['base']):.2f}  tuned {statistics.mean(v['tuned']):.2f}  (n={len(v['base'])})")
            if allb:
                print(f"{'OVERALL':20s} base {statistics.mean(allb):.2f}  tuned {statistics.mean(allt):.2f}  (n={len(allb)})")


if __name__ == "__main__":
    main()
