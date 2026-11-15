#!/usr/bin/env python3
"""GPU steps of the v3 retrieval eval. Writes JSONL resumed by question text.

  rag_answers.py expand --model M [--adapter A] --questions Q.jsonl --out expansions.jsonl
  rag_answers.py answer --model M [--adapter A] --questions Q.jsonl --out answers.jsonl --label tuned
                        [--packs packs.json --k k6|k3] [--thorough] [--thinking]

answers.jsonl rows are in eval/compare.py cache format (label, question, text, tokens, truncated).
Packs come from scripts/retrieve.py (CPU); expansions are written by the model under test.
"""
import argparse, json, re, sys
from pathlib import Path
from mlx_lm import load, stream_generate

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_dataset import SYSTEM_PROMPT
from retrieve import EXPAND_SYSTEM, index

THOROUGH = ("Answer thoroughly: give every likely cause the situation allows, how to check each one "
            "(exact command, metric or field) and the fix, then what to do if none of them fit.")


def gen(model, tok, msgs, max_tokens, thinking=False):
    prompt = tok.apply_chat_template(msgs, add_generation_prompt=True, tokenize=False, enable_thinking=thinking)
    text, last = "", None
    for last in stream_generate(model, tok, prompt=prompt, max_tokens=max_tokens):
        text += last.text
    return re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip(), last.generation_tokens, last.finish_reason == "length"


def pack_text(chunk_ids):
    chunks = {c["id"]: c for c in index()[0]}
    parts = []
    for cid in chunk_ids:
        c = chunks[cid]
        parts.append(f"{c['source']} \u2014 {c['title'] or c['path'].rsplit('/', 1)[-1]}\n{c['text']}")
    return "\n\n".join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["expand", "answer"])
    ap.add_argument("--model", required=True)
    ap.add_argument("--adapter")
    ap.add_argument("--questions", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="tuned")
    ap.add_argument("--packs")
    ap.add_argument("--k", choices=["k6", "k3"])
    ap.add_argument("--thorough", action="store_true")
    ap.add_argument("--thinking", action="store_true")
    ap.add_argument("--max-tokens", type=int, default=2048)
    a = ap.parse_args()
    qs = [json.loads(l)["question"] for l in open(a.questions) if l.strip()]
    out = Path(a.out)
    done = {json.loads(l)["question"] for l in out.open()} if out.exists() else set()
    todo = [q for q in qs if q not in done]
    if not todo:
        return
    packs = {p["question"]: p[a.k] for p in json.load(open(a.packs))} if a.packs else {}
    model, tok = load(a.model, adapter_path=a.adapter)
    for n, q in enumerate(todo, 1):
        if a.mode == "expand":
            text, toks, trunc = gen(model, tok, [{"role": "system", "content": EXPAND_SYSTEM}, {"role": "user", "content": q}], 300)
            row = {"question": q, "expansion": text}
        else:
            user = q
            if packs:
                user = ("Use these documentation excerpts where relevant.\n\n" + pack_text(packs[q]) + f"\n\nQuestion: {q}")
            if a.thorough:
                user += "\n\n" + THOROUGH
            text, toks, trunc = gen(model, tok, [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}],
                                    a.max_tokens, a.thinking)
            row = {"label": a.label, "question": q, "text": text, "tokens": toks, "truncated": trunc}
        with open(out, "a") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"[{a.mode}] {n}/{len(todo)}", flush=True)


if __name__ == "__main__":
    main()
