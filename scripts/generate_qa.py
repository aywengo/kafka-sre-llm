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
import os
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
blocks where useful, then caveats only if the excerpt states them.
- Never refer to "the excerpt", "the document", "the text" or "the passage". The answer must \
stand on its own.
- Use only API versions (apiVersion), kinds and field paths that appear in the excerpt. Never \
fill them in from memory, even if you know another version.
- Say where each setting lives: broker, topic, client (producer/consumer/admin), Connect worker \
or connector, or the exact custom resource field path. Never move a setting to another scope.
- If the question's premise is wrong (wrong component, wrong version, wrong scope), the answer \
must correct it first instead of going along with it.
- Label early-access, preview or deprecated features as such, using the excerpt's wording.
- Caveats, side effects, alternatives (other modes, flags, endpoints), examples, defaults and \
deprecations are facts too. Include one only if the excerpt states it. If the excerpt gives no \
caveat, write none.
- Troubleshooting answers are a checklist: the likely causes the excerpt names, most likely \
first; for each, how to check it (command or metric from the excerpt) and the fix. Never add a \
cause the excerpt does not name. If it names only one, give that one well.
- If the excerpt says something is not covered by official documentation, is a compiler's \
note, or cannot be completed from the docs, keep that warning in the answer in plain words.
- This dataset targets Kafka 4.3 and Strimzi 1.3.1. Do not mention other versions unless the \
excerpt does.
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

GEN_USER_TYPES = """Source: {source} — {title}

<excerpt>
{text}
</excerpt>

Write {n} diverse items, all of type "{types}". For troubleshooting: a realistic symptom as an \
engineer would report it, then the causes the excerpt names in order of likelihood, how to \
check each, and the fix. Skip the excerpt (return []) if it names no failure mode."""

GEN_SYSTEM_CONFIG_REF = """You write training data for an assistant that is a senior Site Reliability \
Engineer specialised in Apache Kafka. You are given configuration reference entries \
(one setting each: description, Type, Default, Valid Values, Importance, Update Mode).

Rules:
- Answer ONLY with what the entry states: what the setting does, its scope (broker, topic, \
producer, consumer, admin, Connect, ...), type, default, valid values, importance, update mode. \
Quote key names and values exactly.
- No advice, no recommendations, no troubleshooting causes, no restart or retry caveats, no \
examples or YAML/properties snippets beyond what the entry itself contains, no other versions.
- Write exactly one item per setting, in the order the settings appear. Use the question style \
"what is the default of X / can I change it at runtime (update mode)" ONLY for a setting whose \
entry has an "Update Mode:" line. For every other setting use "what is X and what does it do". \
Where valid values are listed you may use "what values does X accept" instead, but never rotate \
into a style whose facts the entry lacks.
- Questions are phrased like an engineer would ask. Answers are short and stand on their own: \
never refer to "the entry", "the excerpt" or "the documentation".
- If an entry has no technical substance, return [].

Return ONLY a JSON array. Each item: {"type": "config", "question": "...", "answer": "..."}"""

GEN_USER_CONFIG_REF = """Source: {source} — {title}

<excerpt>
{text}
</excerpt>

Write exactly {n} items, one per setting in the excerpt, following the rules."""

STRICT_RULES = """grounded=false if the answer states anything not supported by the excerpt, \
contains a wrong config key/flag/default, would mislead an operator, contains an unresolved \
placeholder such as {SomeAttribute}, uses an apiVersion that is not in the excerpt, places a \
setting at the wrong scope (broker vs topic vs client vs Connect worker vs connector vs a \
specific custom resource field), or accepts a false premise in the question instead of \
correcting it, adds a caveat, alternative, example, endpoint, default or deprecation that is \
not in the excerpt, lists a troubleshooting cause that is not in the excerpt, or drops a \
not-covered-by-the-docs warning that the excerpt has. First restate the premise of the \
question in a few words and check it against the excerpt (key vs value, component, scope, \
version)."""

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
            if args.profile == "config-ref":
                n = max(1, chunk["text"].count("Type:"))  # one item per setting
            try:
                user = (GEN_USER_TYPES.format(n=n, types=args.types, **chunk) if args.types
                        else GEN_USER.format(n=n, **chunk))
                if args.profile == "config-ref":
                    system, user = GEN_SYSTEM_CONFIG_REF, GEN_USER_CONFIG_REF.format(n=n, **chunk)
                else:
                    system = GEN_SYSTEM
                raw = await llm.chat(system, user)
            except Exception as e:  # keep going on single failures
                print(f"  gen failed {chunk['id']}: {e!r}")
                stats["failed"] += 1
                return
            items = [it for it in (extract_json(raw) or [])
                     if isinstance(it, dict) and it.get("question") and it.get("answer")]
        rejected = []
        if args.verify == "batch" and items:
            kept = await verify_batch(chunk, items, llm, args, stats, rejected)
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
            src = stats["by_source"].setdefault(chunk["source"], {"checked": 0, "rejected": 0})
            src["checked"] += len(items)
            src["rejected"] += len(items) - len(kept)
            if rejected:
                with open(str(out_f.name) + ".rejects.jsonl", "a") as rf:
                    for it in rejected:
                        rf.write(json.dumps({"chunk_id": chunk["id"], "source": chunk["source"],
                                             **it}, ensure_ascii=False) + "\n")
            stats["done"] += 1
            stats["items"] += len(kept)
            if args.existing is not None and src["checked"] >= args.guard_min_items \
                    and src["rejected"] / src["checked"] > args.max_reject_rate:
                print(f"GUARD: {chunk['source']} reject rate {src['rejected']}/{src['checked']} "
                      f"exceeds {args.max_reject_rate:.0%}; stopping", flush=True)
                report(stats, llm.usage)
                os._exit(3)
            if stats["done"] % 20 == 0:
                report(stats, llm.usage)


async def verify_one(chunk, it, llm, args, stats):
    try:
        v = extract_json(await llm.chat(
            VERIFY_SYSTEM,
            f"Source: {chunk['source']} — {chunk['title']}\n\n<excerpt>\n{chunk['text']}\n</excerpt>\n\nQ: {it['question']}\n\nA: {it['answer']}",
            temperature=0.0, max_tokens=args.verify_max_tokens,
            extra_body=args.verify_extra_body), kind="{")
    except Exception:
        v = None
    if not v or not v.get("grounded"):
        stats["rejected"] += 1
        return False
    return True


async def verify_batch(chunk, items, llm, args, stats, rejected=None):
    """One call checks every item of a chunk, so the excerpt is sent once."""
    pairs = "\n\n".join(f"[{i}]\nQ: {it['question']}\nA: {it['answer']}"
                        for i, it in enumerate(items, 1))
    try:
        v = extract_json(await llm.chat(
            VERIFY_BATCH_SYSTEM,
            f"Source: {chunk['source']} — {chunk['title']}\n\n<excerpt>\n{chunk['text']}\n</excerpt>\n\n{pairs}",
            temperature=0.0, max_tokens=args.verify_max_tokens,
            extra_body=args.verify_extra_body))
    except Exception:
        v = None
    ok, reasons = set(), {}
    for r in v or []:
        if isinstance(r, dict):
            try:
                i = int(r.get("id"))
            except (TypeError, ValueError):
                continue
            if r.get("grounded") is True:
                ok.add(i)
            else:
                reasons[i] = r.get("reason")
    if v is None:
        stats["verify_parse_fail"] += 1
    kept = [it for i, it in enumerate(items, 1) if i in ok]  # unparsed verdict = reject
    if rejected is not None:
        rejected += [{**it, "reason": reasons.get(i, "no verdict")}
                     for i, it in enumerate(items, 1) if i not in ok]
    stats["rejected"] += len(items) - len(kept)
    return kept


def report(stats, usage):
    mins = (time.monotonic() - stats["_t0"]) / 60
    calls = max(1, usage["calls"])
    done = max(1, stats["done"])
    rates = {s: f"{v['rejected']}/{v['checked']}" for s, v in stats["by_source"].items()}
    print(f"  reject by source: {rates}", flush=True)
    print(f"  done={stats['done']} items={stats['items']} rejected={stats['rejected']} "
          f"failed={stats['failed']} verify_parse_fail={stats['verify_parse_fail']} | "
          f"{stats['done'] / max(mins, 1e-9):.2f} chunks/min | "
          f"per chunk: {usage['completion_tokens'] / done:.0f} out tok "
          f"({usage['reasoning_tokens'] / done:.0f} reasoning), "
          f"{usage['prompt_tokens'] / done:.0f} in tok, {usage['calls'] / done:.1f} calls | "
          f"{usage['completion_tokens'] / calls:.0f} out tok/call", flush=True)


CONTEXT_MAX_CHARS = 12000


def add_context(chunks, n):
    """Replace each chunk's text by it plus n neighbours each side (same file, file order),
    dropping the farthest neighbours until the excerpt fits CONTEXT_MAX_CHARS."""
    by_path = {}
    for c in chunks:
        by_path.setdefault(c["path"], []).append(c)
    out = []
    for group in by_path.values():
        for i, c in enumerate(group):
            k = n
            while True:
                parts = group[max(0, i - k): i + k + 1]
                text = "\n\n".join(p["text"] for p in parts)
                if len(text) <= CONTEXT_MAX_CHARS or k == 0:
                    break
                k -= 1
            out.append({**c, "text": text[:CONTEXT_MAX_CHARS]})
    return out


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080/v1")
    ap.add_argument("--model", default="default")
    ap.add_argument("--api-key", default=os.environ.get("OMLX_API_KEY", "local"))
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
    ap.add_argument("--max-reject-rate", type=float, default=0.25,
                    help="--reverify only: stop when a source's reject rate exceeds this")
    ap.add_argument("--guard-min-items", type=int, default=100)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--context-neighbours", type=int, default=0, metavar="N",
                    help="show each chunk with N previous and N next chunks of the same file")
    ap.add_argument("--paths", nargs="*", help="only chunks whose path contains one of these")
    ap.add_argument("--profile", choices=["config-ref"],
                    help="config-ref: single-setting reference entries, answer only what the entry states")
    ap.add_argument("--text-contains", help="only chunks whose text contains this string")
    ap.add_argument("--types", help="ask only for items of this type, e.g. troubleshooting")
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
    if args.context_neighbours:
        chunks = add_context(chunks, args.context_neighbours)
    chunks = [c for c in chunks if c["id"] not in done
              and (not args.sources or c["source"] in args.sources)
              and (not args.paths or any(p in c["path"] for p in args.paths))
              and (not args.text_contains or args.text_contains in c["text"])
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
             "_t0": time.monotonic(), "by_source": {}}
    with out.open("a") as f:
        await asyncio.gather(*(process(c, llm, args, sem, f, lock, stats) for c in chunks))
    print("finished:")
    report(stats, usage)


if __name__ == "__main__":
    asyncio.run(main())
