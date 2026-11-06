#!/usr/bin/env python3
"""Generate grounded SRE training examples from doc chunks using a local
OpenAI-compatible server (mlx_lm.server, LM Studio, Ollama, vLLM...).

Resumable: chunks already present in the output file are skipped.

Example:
  mlx_lm.server --model mlx-community/<big-instruct-model> --port 8080
  python scripts/generate_qa.py --url http://localhost:8080/v1 --model <id> \
      --chunks work/chunks.jsonl --out work/raw_qa.jsonl --verify

To turn off reasoning, pass the chat-template switch your model uses, e.g.
  --extra-body '{"chat_template_kwargs": {"thinking": false}}'
(check the variable name in the model's chat template; the progress line shows
whether reasoning tokens per chunk actually dropped).
"""
import argparse
import asyncio
import json
import random
import re
import time
from pathlib import Path

import httpx

GEN_SYSTEM = """You write training data for an assistant that is a senior Site Reliability \
Engineer specialised in Apache Kafka, Strimzi, Kubernetes, GitOps (Argo CD) and data \
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
- Use only API versions (apiVersion), kinds and field paths that appear in the excerpt. Never \
fill them in from memory, even if you know another version.
- Say where each setting lives: broker, topic, client (producer/consumer/admin), Connect worker \
or connector, or the exact custom resource field path. Never move a setting to another scope.
- If the question's premise is wrong (wrong component, wrong version, wrong scope), the answer \
must correct it first instead of going along with it.
- Label early-access, preview or deprecated features as such, using the excerpt's wording.
- Never output unresolved placeholders such as {SomeAttribute}; if the excerpt has one, leave \
that detail out.
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

STRICT_RULES = """grounded=false if the answer states anything not supported by the excerpt, \
contains a wrong config key/flag/default, would mislead an operator, contains an unresolved \
placeholder such as {SomeAttribute}, uses an apiVersion that is not in the excerpt, places a \
setting at the wrong scope (broker vs topic vs client vs Connect worker vs connector vs a \
specific custom resource field), or accepts a false premise in the question instead of \
correcting it."""

VERIFY_SYSTEM = """You check training data for factual grounding. Given an excerpt and a \
question/answer pair, reply with ONLY a JSON object: {"grounded": true|false, "reason": "..."}. \
""" + STRICT_RULES

VERIFY_BATCH_SYSTEM = """You check training data for factual grounding. You get one excerpt \
and several numbered question/answer pairs generated from it. For each pair decide whether it \
is grounded. """ + STRICT_RULES + """ Reply with ONLY a JSON \
array, one object per pair: [{"id": 1, "grounded": true|false, "reason": "<few words>"}, ...]"""


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
    def __init__(self, url, model, api_key, temperature, max_tokens, extra_body, usage):
        self.client = httpx.AsyncClient(base_url=url.rstrip("/"), timeout=1800,
                                        headers={"Authorization": f"Bearer {api_key}"})
        self.model, self.temperature, self.max_tokens = model, temperature, max_tokens
        self.extra_body = extra_body or {}
        self.usage = usage  # shared counters, reported in progress lines

    async def chat(self, system, user, temperature=None, max_tokens=None, extra_body=None):
        body = {
            "model": self.model,
            "messages": [{"role": "system", "content": system},
                         {"role": "user", "content": user}],
            "temperature": self.temperature if temperature is None else temperature,
            "max_tokens": max_tokens or self.max_tokens,
        }
        body.update(self.extra_body if extra_body is None else extra_body)
        for attempt in range(3):
            try:
                r = await self.client.post("/chat/completions", json=body)
                r.raise_for_status()
                data = r.json()
                u = data.get("usage") or {}
                self.usage["calls"] += 1
                self.usage["prompt_tokens"] += u.get("prompt_tokens", 0) or 0
                self.usage["completion_tokens"] += u.get("completion_tokens", 0) or 0
                details = u.get("completion_tokens_details") or {}
                self.usage["reasoning_tokens"] += details.get("reasoning_tokens", 0) or 0
                return data["choices"][0]["message"]["content"] or ""
            except (httpx.HTTPError, KeyError) as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
                print(f"  retry after {e!r}")


async def process(chunk, llm, args, sem, out_f, lock, stats):
    async with sem:
        if args.existing is not None:
            items = args.existing[chunk["id"]]
        else:
            n = max(1, min(args.max_per_chunk, len(chunk["text"]) // 700))
            try:
                raw = await llm.chat(GEN_SYSTEM, GEN_USER.format(n=n, **chunk))
            except Exception as e:  # keep going on single failures
                print(f"  gen failed {chunk['id']}: {e!r}")
                stats["failed"] += 1
                return
            items = [it for it in (extract_json(raw) or [])
                     if isinstance(it, dict) and it.get("question") and it.get("answer")]
        if args.verify == "batch" and items:
            kept = await verify_batch(chunk, items, llm, args, stats)
        elif args.verify == "each":
            kept = [it for it in items if await verify_one(chunk, it, llm, args, stats)]
        else:
            kept = items
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
                report(stats, llm.usage)


async def verify_one(chunk, it, llm, args, stats):
    try:
        v = extract_json(await llm.chat(
            VERIFY_SYSTEM,
            f"<excerpt>\n{chunk['text']}\n</excerpt>\n\nQ: {it['question']}\n\nA: {it['answer']}",
            temperature=0.0, max_tokens=args.verify_max_tokens,
            extra_body=args.verify_extra_body), kind="{")
    except Exception:
        v = None
    if not v or not v.get("grounded"):
        stats["rejected"] += 1
        return False
    return True


async def verify_batch(chunk, items, llm, args, stats):
    """One call checks every item of a chunk, so the excerpt is sent once."""
    pairs = "\n\n".join(f"[{i}]\nQ: {it['question']}\nA: {it['answer']}"
                        for i, it in enumerate(items, 1))
    try:
        v = extract_json(await llm.chat(
            VERIFY_BATCH_SYSTEM,
            f"<excerpt>\n{chunk['text']}\n</excerpt>\n\n{pairs}",
            temperature=0.0, max_tokens=args.verify_max_tokens,
            extra_body=args.verify_extra_body))
    except Exception:
        v = None
    ok = set()
    for r in v or []:
        if isinstance(r, dict) and r.get("grounded") is True:
            try:
                ok.add(int(r.get("id")))
            except (TypeError, ValueError):
                pass
    if v is None:
        stats["verify_parse_fail"] += 1
    kept = [it for i, it in enumerate(items, 1) if i in ok]  # unparsed verdict = reject
    stats["rejected"] += len(items) - len(kept)
    return kept


def report(stats, usage):
    mins = (time.monotonic() - stats["_t0"]) / 60
    calls = max(1, usage["calls"])
    done = max(1, stats["done"])
    print(f"  done={stats['done']} items={stats['items']} rejected={stats['rejected']} "
          f"failed={stats['failed']} verify_parse_fail={stats['verify_parse_fail']} | "
          f"{stats['done'] / max(mins, 1e-9):.2f} chunks/min | "
          f"per chunk: {usage['completion_tokens'] / done:.0f} out tok "
          f"({usage['reasoning_tokens'] / done:.0f} reasoning), "
          f"{usage['prompt_tokens'] / done:.0f} in tok, {usage['calls'] / done:.1f} calls | "
          f"{usage['completion_tokens'] / calls:.0f} out tok/call", flush=True)


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
    ap.add_argument("--verify", nargs="?", const="batch", default="none",
                    choices=["none", "batch", "each"],
                    help="grounding check: 'batch' = one call per chunk (default when the flag "
                         "is given), 'each' = one call per item (slower)")
    ap.add_argument("--verify-max-tokens", type=int, default=2048)
    ap.add_argument("--extra-body", type=json.loads, default={},
                    help='JSON merged into every request, e.g. '
                         '\'{"chat_template_kwargs": {"thinking": false}}\'')
    ap.add_argument("--verify-extra-body", type=json.loads, default=None,
                    help="JSON for verify requests only (defaults to --extra-body)")
    ap.add_argument("--reverify", metavar="RAW_QA",
                    help="skip generation: run the verifier over the items in this JSONL and "
                         "write survivors to --out. Only chunks whose id is in --chunks are "
                         "checked, i.e. chunks whose text did not change.")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()
    args.existing = None
    if args.reverify:
        args.existing = {r["chunk_id"]: r["items"] for r in map(json.loads, Path(args.reverify).open())
                         if r["items"] and (not args.sources or r["source"] in args.sources)}
    if args.verify_extra_body is None:
        args.verify_extra_body = args.extra_body

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    done = set()
    if out.exists():
        done = {json.loads(l)["chunk_id"] for l in out.open() if l.strip()}

    chunks = [json.loads(l) for l in Path(args.chunks).open() if l.strip()]
    chunks = [c for c in chunks if c["id"] not in done
              and (not args.sources or c["source"] in args.sources)
              and (args.existing is None or c["id"] in args.existing)]
    random.Random(args.seed).shuffle(chunks)  # spread sources across a partial run
    if args.limit:
        chunks = chunks[: args.limit]
    print(f"{len(done)} chunks already done, {len(chunks)} to process")

    usage = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "reasoning_tokens": 0}
    llm = LLM(args.url, args.model, args.api_key, args.temperature, args.max_tokens,
              args.extra_body, usage)
    sem, lock = asyncio.Semaphore(args.concurrency), asyncio.Lock()
    stats = {"done": 0, "items": 0, "rejected": 0, "failed": 0, "verify_parse_fail": 0,
             "_t0": time.monotonic()}
    with out.open("a") as f:
        await asyncio.gather(*(process(c, llm, args, sem, f, lock, stats) for c in chunks))
    print("finished:")
    report(stats, usage)


if __name__ == "__main__":
    asyncio.run(main())
