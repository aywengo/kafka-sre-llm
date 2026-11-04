#!/usr/bin/env python3
"""Generate grounded SRE training examples from doc chunks using a local
OpenAI-compatible server (mlx_lm.server, LM Studio, Ollama, vLLM...).

Resumable: chunks already present in the output file are skipped.

Example:
  mlx_lm.server --model mlx-community/<big-instruct-model> --port 8080
  python scripts/generate_qa.py --url http://localhost:8080/v1 --model default \
      --chunks work/chunks.jsonl --out work/raw_qa.jsonl --verify
"""
import argparse
import asyncio
import json
import random
import re
from pathlib import Path

import httpx

GEN_SYSTEM = """You write training data for an assistant that is a senior Site Reliability \
Engineer specialised in Apache Kafka, Strimzi, Kubernetes, GitOps (Argo CD, Flux) and data \
streaming. You are given one documentation excerpt. Produce realistic questions an engineer \
would ask, with expert answers.

Rules:
- Every fact in an answer must be supported by the excerpt. Do not invent config keys, \
CRD fields, CLI flags, metrics or defaults.
- Write questions the way people actually ask: incident descriptions, Slack-style questions, \
"how do I", "why does", "what's the difference", config review requests.
- Answers are practical: short explanation first, then commands or YAML in fenced code \
blocks where useful, then caveats (version differences, risks, what to check next).
- Never refer to "the excerpt", "the document", "the text" or "the passage". The answer must \
stand on its own.
- If the excerpt is navigation, a changelog, a list of links or has no technical substance, \
return [].

Return ONLY a JSON array. Each item: {"type": "<concept|howto|troubleshooting|config|comparison>", \
"question": "...", "answer": "..."}"""

GEN_USER = """Source: {source} — {title}

<excerpt>
{text}
</excerpt>

Write {n} diverse items. Mix types; include at least one troubleshooting or config item if the \
excerpt allows it."""

VERIFY_SYSTEM = """You check training data for factual grounding. Given an excerpt and a \
question/answer pair, reply with ONLY a JSON object: {"grounded": true|false, "reason": "..."}. \
grounded=false if the answer states anything not supported by the excerpt, contains a wrong \
config key/flag/default, or would mislead an operator."""


def strip_think(s: str) -> str:
    return re.sub(r"<think>.*?</think>", "", s, flags=re.S).strip()


def extract_json(s: str, kind="["):
    s = strip_think(s)
    s = re.sub(r"^```(?:json)?|```$", "", s.strip(), flags=re.M).strip()
    close = "]" if kind == "[" else "}"
    start, end = s.find(kind), s.rfind(close)
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(s[start:end + 1])
    except json.JSONDecodeError:
        return None


class LLM:
    def __init__(self, url, model, api_key, temperature, max_tokens):
        self.client = httpx.AsyncClient(base_url=url.rstrip("/"), timeout=600,
                                        headers={"Authorization": f"Bearer {api_key}"})
        self.model, self.temperature, self.max_tokens = model, temperature, max_tokens

    async def chat(self, system, user, temperature=None):
        for attempt in range(3):
            try:
                r = await self.client.post("/chat/completions", json={
                    "model": self.model,
                    "messages": [{"role": "system", "content": system},
                                 {"role": "user", "content": user}],
                    "temperature": self.temperature if temperature is None else temperature,
                    "max_tokens": self.max_tokens,
                })
                r.raise_for_status()
                return r.json()["choices"][0]["message"]["content"]
            except (httpx.HTTPError, KeyError) as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
                print(f"  retry after {e!r}")


async def process(chunk, llm, args, sem, out_f, lock, stats):
    async with sem:
        n = max(1, min(args.max_per_chunk, len(chunk["text"]) // 700))
        try:
            raw = await llm.chat(GEN_SYSTEM, GEN_USER.format(n=n, **chunk))
        except Exception as e:  # keep going on single failures
            print(f"  gen failed {chunk['id']}: {e!r}")
            stats["failed"] += 1
            return
        items = extract_json(raw) or []
        kept = []
        for it in items:
            if not isinstance(it, dict) or not it.get("question") or not it.get("answer"):
                continue
            if args.verify:
                try:
                    v = extract_json(await llm.chat(
                        VERIFY_SYSTEM,
                        f"<excerpt>\n{chunk['text']}\n</excerpt>\n\nQ: {it['question']}\n\nA: {it['answer']}",
                        temperature=0.0), kind="{")
                except Exception:
                    v = None
                if not v or not v.get("grounded"):
                    stats["rejected"] += 1
                    continue
            kept.append(it)
        async with lock:
            # Always write a record per chunk (even empty) so reruns skip it.
            out_f.write(json.dumps({
                "chunk_id": chunk["id"], "source": chunk["source"], "license": chunk["license"],
                "path": chunk["path"], "title": chunk["title"], "items": kept,
            }) + "\n")
            out_f.flush()
            stats["done"] += 1
            stats["items"] += len(kept)
            if stats["done"] % 20 == 0:
                print(f"  {stats}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080/v1")
    ap.add_argument("--model", default="default")
    ap.add_argument("--api-key", default="local")
    ap.add_argument("--chunks", default="work/chunks.jsonl")
    ap.add_argument("--out", default="work/raw_qa.jsonl")
    ap.add_argument("--concurrency", type=int, default=2)
    ap.add_argument("--max-per-chunk", type=int, default=4)
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--max-tokens", type=int, default=4096)
    ap.add_argument("--limit", type=int, help="process at most N new chunks (for test runs)")
    ap.add_argument("--sources", nargs="*", help="only these source names")
    ap.add_argument("--verify", action="store_true", help="second pass to reject ungrounded items")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if out.exists():
        done = {json.loads(l)["chunk_id"] for l in out.open() if l.strip()}

    chunks = [json.loads(l) for l in Path(args.chunks).open() if l.strip()]
    chunks = [c for c in chunks if c["id"] not in done
              and (not args.sources or c["source"] in args.sources)]
    random.Random(args.seed).shuffle(chunks)  # spread sources across a partial run
    if args.limit:
        chunks = chunks[: args.limit]
    print(f"{len(done)} chunks already done, {len(chunks)} to process")

    llm = LLM(args.url, args.model, args.api_key, args.temperature, args.max_tokens)
    sem, lock = asyncio.Semaphore(args.concurrency), asyncio.Lock()
    stats = {"done": 0, "items": 0, "rejected": 0, "failed": 0}
    with out.open("a") as f:
        await asyncio.gather(*(process(c, llm, args, sem, f, lock, stats) for c in chunks))
    print(f"finished: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
