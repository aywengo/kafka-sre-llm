#!/usr/bin/env python3
"""GPU steps of the v3 retrieval eval. Writes JSONL resumed by question text.

  rag_answers.py expand --model M [--adapter A] --questions Q.jsonl --out expansions.jsonl
  rag_answers.py answer --model M [--adapter A] --questions Q.jsonl --out answers.jsonl --label tuned
                        [--packs packs.json|packs.jsonl --k k6|k3] [--thorough] [--thinking]

answers.jsonl rows are in eval/compare.py cache format (label, question, text, tokens, truncated).
Packs come from scripts/retrieve.py (CPU); expansions are written by the model under test.
"""
import argparse, json, re, sys
from pathlib import Path
from mlx_lm import load, stream_generate

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from kafka_sre_rag.prompts import answer_messages, expand_messages
from retrieve import CHUNKS, index


def gen(model, tok, msgs, max_tokens, thinking=False):
    prompt = tok.apply_chat_template(msgs, add_generation_prompt=True, tokenize=False, enable_thinking=thinking)
    text, last = "", None
    for last in stream_generate(model, tok, prompt=prompt, max_tokens=max_tokens):
        text += last.text
    return re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip(), last.generation_tokens, last.finish_reason == "length"


def chunks_for(chunk_ids, index_path=None):
    """Full chunk dicts by id (the prompt uses the full chunk text); layout lives in kafka_sre_rag.prompts."""
    chunks = {c["id"]: c for c in index(index_path or CHUNKS)[0]}
    return [chunks[cid] for cid in chunk_ids]


def load_packs(path, k):
    """Packs as {question: chunk ids}: a JSON array, or JSONL (one object per line) from build_packs_v3.py."""
    text = Path(path).read_text()
    if text.lstrip().startswith("["):
        rows = json.loads(text)
    else:
        rows = [json.loads(l) for l in text.splitlines() if l.strip()]
    return {p["question"]: p[k] for p in rows}


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
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--rules", action="store_true",
                    help="add the teacher's pack-answer rules to the instruction (prompted-student probe)")
    ap.add_argument("--index", help="chunks.jsonl the packs were built on (default: retrieve.CHUNKS)")
    ap.add_argument("--thinking", action="store_true")
    ap.add_argument("--max-tokens", type=int, default=2048)
    a = ap.parse_args()
    qs = [json.loads(l)["question"] for l in open(a.questions) if l.strip()]
    out = Path(a.out)
    done = {json.loads(l)["question"] for l in out.open()} if out.exists() else set()
    todo = [q for q in qs if q not in done]
    if not todo:
        return
    packs = load_packs(a.packs, a.k) if a.packs else {}
    model, tok = load(a.model, adapter_path=a.adapter)
    for n, q in enumerate(todo, 1):
        if a.mode == "expand":
            text, toks, trunc = gen(model, tok, expand_messages(q), 300)
            row = {"question": q, "expansion": text}
        else:
            msgs = answer_messages(q, chunks_for(packs[q], a.index) if packs else None, strict=a.strict, thorough=a.thorough, rules=a.rules)
            text, toks, trunc = gen(model, tok, msgs, a.max_tokens, a.thinking)
            row = {"label": a.label, "question": q, "text": text, "tokens": toks, "truncated": trunc}
        with open(out, "a") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"[{a.mode}] {n}/{len(todo)}", flush=True)


if __name__ == "__main__":
    main()
