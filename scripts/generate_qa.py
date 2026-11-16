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
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from kafka_sre_rag.prompts import pack_text  # noqa: E402  (single source for how a pack is rendered)

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pack_filters  # noqa: E402

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
check each, and the fix. Do not repeat general setup text from the excerpt (e.g. how to \
enable remote JMX or its security) unless the question is about it. Skip the excerpt (return \
[]) if it names no failure mode."""

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

PACK_GEN_SYSTEM = """You write training data for an assistant that is a senior Site Reliability \
Engineer specialised in Apache Kafka, Strimzi, Kubernetes, GitOps (Argo CD) and data streaming. \
At run time the assistant answers with a set of retrieved documentation excerpts in front of it. \
You are given a question and those excerpts (the material). Write the answer the assistant should give.

Rules:
- Every fact in the answer must be supported by the material. Every command, flag, configuration \
key, metric name, resource kind, field path, annotation, endpoint and error string in the answer \
must appear in the material, written exactly as it appears there. Never complete one from memory, \
even if you know it.
- Be complete. Answer like a senior SRE answering a colleague: use every cause, check, step and \
caveat that the material supports and that bears on the question, and nothing it does not support.
- Shape. Start with one or two sentences that answer the question directly. Then: for \
troubleshooting, the likely causes the material names, most likely first, numbered; for each cause \
a **Check** (the exact command, metric or field from the material) and a **Fix**; then what to do \
if none of them fit, if the material says. Number only causes that can produce the symptom asked \
about. A numbered cause needs a Check naming a command, metric, field, log or status from the \
material; if the material gives none, mention the cause in prose instead of numbering it. Do not \
force the troubleshooting shape onto a question that is not about a symptom. Check and Fix labels \
are for causes of a symptom only; steps of a procedure are plain numbered steps. For how-to, the steps in order with the exact commands \
or YAML from the material. For config review and concept questions, the facts, with the scope of \
each setting. No generic sections, no recap, no closing summary. Stop when the relevant material \
is used up.
- Ignore excerpts that are not about the question. Do not combine settings or steps from different \
products or versions unless the material itself connects them. An identifier that appears in the \
material for one component must not be used for another.
- Use only API versions (apiVersion), kinds and field paths that appear in the material. Say where \
each setting lives: broker, topic, client (producer/consumer/admin), Connect worker or connector, \
or the exact custom resource field path. Never move a setting to another scope.
- If the material contradicts the question's premise (wrong component, wrong version, wrong \
scope), correct it first. A premise the material does not mention is not wrong; it is a gap.
- Never say or imply that a field, flag, metric, feature or behaviour does not exist, is not \
supported, is not exposed, does not apply, does not propagate or cannot be done unless the \
material says so in those terms, including hedged forms ("there is no ... that I can point to", \
"not ... here"). If the question names something the material does not mention, say so only in \
the gap sentence.
- Label early-access, preview or deprecated features as such, using the material's wording. Do not \
mention product versions unless the material does. Never output unresolved placeholders such as \
{SomeAttribute}.
- Coverage. Decide how far the material answers the question. "full": it supports a complete \
answer. "partial": it answers part of it; answer that part fully, then add one sentence in exactly \
this form: "The retrieved documentation does not cover <what is missing>." A partial or none \
answer without that sentence is wrong, and a full answer must not contain it. Before writing the \
gap sentence, check that nothing in your answer or in the material, including comments inside code \
blocks, gives what it names as missing. Every statement about what is missing goes in that one \
sentence and nowhere else, and the sentence appears exactly once. "none": it does not answer the \
question; start with that same sentence form, then continue with the facts that do help, if any, \
stated as facts without mentioning the documentation again. Keep the same layout as a covered answer: short paragraphs, numbered items \
and code blocks where they help, never one block of text.
- After that sentence you may add at most two sentences of general direction, starting with "In \
general,", in plain words, naming no command, flag, key, metric, field, tool or other identifier and stating no technical \
fact that the material does not support; they give direction only (where to look, what kind of \
thing to check). Never write an answer that only says something is not covered.
- Apart from that one sentence form, never refer to "the material", "the excerpts", "the \
documentation", "the docs", "the source", "the sources", "the context", "described here", \
"given here". The answer must read as the assistant's own.
- Keep the answer under about 1,200 words.
- Return {"skip": true} only if the question is malformed or not a real question. A real question \
the material does not answer gets coverage "none"; a question with a wrong premise is answered by \
correcting the premise from the material.

Return ONLY a JSON object: {"coverage": "full|partial|none", "not_covered": "<what is missing; \
empty for full>", "answer": "..."}"""

PACK_VERIFY_SYSTEM = """You check training data for factual grounding. You get the material (the \
same documentation excerpts the writer saw), a question and an answer. Reply with ONLY a JSON object:
{"claims": [{"c": "<claim in a few words>", "ok": true|false, "src": <number of the supporting \
excerpt, 0 if none>}], "identifiers_not_in_material": ["..."], "causes_with_check_and_fix": \
<integer>, "coverage": "full|partial|none", "gap_ok": true|false, "grounded": true|false, \
"reason": "<few words>"}
- List each factual claim of the answer, at most 25 (merge trivial ones). ok is true only if the \
material supports it.
- identifiers_not_in_material: every command, flag, configuration key, metric name, kind, field \
path, annotation, endpoint or error string in the answer that does not appear in the material.
- causes_with_check_and_fix: for a troubleshooting answer, how many of its causes have both a \
check and a fix; otherwise 0.
- causes_with_check_and_fix counts only causes that can produce the symptom asked about, whose \
check names something concrete from the material and whose fix is an action; a cause with "no fix \
available" or a check like "review the configuration" does not count. For an answer that is not \
about a symptom it is 0.
- coverage: your own judgement of how far the material answers the question.
- gap_ok is false if the answer says something is not covered that the material does cover, or if \
the material lacks part of the answer and the answer does not say so, or if the sentences after \
"In general," contain an identifier, or if the answer only says that something is not covered, or \
if the coverage is partial or none and the answer does not contain a sentence of the form "The \
retrieved documentation does not cover ...".
- Sentences starting with "In general," after the gap sentence are not claims; only check that \
they name no command, flag, key, metric, field, tool or other identifier and that they state no \
technical fact that the material does not support.
- grounded is false if any claim is not ok, if identifiers_not_in_material is not empty, if a \
setting is placed at the wrong scope, if a false premise in the question is accepted, if an \
identifier that the material gives for one component is used for another, or if gap_ok is false.
- grounded is false if the answer says that something does not exist, is not supported, is not \
exposed, is not governed by, does not apply, does not propagate to or cannot be done, including \
hedged forms, and the material does not say so.
- grounded is false if any sentence other than the gap sentence refers to the material, the \
documentation, a table or a source, or to what is or is not documented or described.
- gap_ok is false if what the gap sentence names as missing appears anywhere in the answer or in \
the material, including comments inside code blocks, or if the answer contains more than one gap \
sentence.
- grounded is false if a numbered cause cannot produce the symptom asked about.
- grounded is false if a command or setting of one product is offered to change a resource that \
another product manages, unless the material connects them.
- If a <withheld> block is present, it holds an excerpt the writer did NOT see: grounded is false \
if the answer states any fact that is supported only by the withheld block.
Excerpts are numbered in the order they appear, starting at 1."""

PACK_PROMPT_VERSION = "pack/3"   # pack/1 (the pilot) had no constant; written into every pack row
PACK_VERIFY_MAX_TOKENS = 4096   # pack mode default; the old modes keep 2048
OLD_VERIFY_MAX_TOKENS = 2048


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
        return (await self.chat_full(system, user, temperature, max_tokens, extra_body))[0]

    async def chat_full(self, system, user, temperature=None, max_tokens=None, extra_body=None):
        """-> (content, finish_reason or None). chat() returns only the content, as before."""
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
                choice = data["choices"][0]
                content = choice["message"]["content"] or ""
                u = data.get("usage") or {}
                self.usage["calls"] += 1
                self.usage["prompt_tokens"] += u.get("prompt_tokens", 0) or 0
                self.usage["completion_tokens"] += u.get("completion_tokens", 0) or 0
                details = u.get("completion_tokens_details") or {}
                self.usage["reasoning_tokens"] += details.get("reasoning_tokens", 0) or 0
                return content, choice.get("finish_reason")
            except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError, AttributeError) as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)
                # class name (and status code) only: httpx error text contains the server address
                what = type(e).__name__
                if isinstance(e, httpx.HTTPStatusError):
                    what += f" {e.response.status_code}"
                print(f"  retry after {what}")


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


# ---------------------------------------------------------------- pack mode

def read_jsonl(path):
    """Complete JSON lines only: a half-written last line (file still growing) is ignored."""
    out = []
    with open(path) as f:
        for line in f:
            if line.strip():
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def load_index(path):
    return {c["id"]: c for c in read_jsonl(path)}


def pack_gen_user(chunks, typ, question):
    return "<material>\n" + pack_text(chunks) + "\n</material>\n\nQuestion type: " \
        f"{typ}\nQuestion: {question}"


def pack_verify_user(chunks, withheld_chunks, question, answer):
    user = "<material>\n" + pack_text(chunks) + "\n</material>\n"
    if withheld_chunks:
        user += "<withheld>\n" + pack_text(withheld_chunks) + "\n</withheld>\n"
    return user + f"\nQuestion: {question}\n\nAnswer:\n{answer}"


def valid_verdict(v):
    return isinstance(v, dict) and "grounded" in v


async def verify_pack_item(vllm, args, chunks, withheld_chunks, question, answer):
    """-> (verdict dict or None, None or "parse" / "request"). One retry on an unparsable verdict."""
    user = pack_verify_user(chunks, withheld_chunks, question, answer)
    err = "parse"
    for _ in range(2):
        try:
            raw = await vllm.chat(PACK_VERIFY_SYSTEM, user, temperature=0.0,
                                  max_tokens=args.verify_max_tokens,
                                  extra_body=args.verify_extra_body)
        except Exception as e:
            print(f"  verify request failed: {type(e).__name__}", flush=True)
            err = "request"
            continue
        v = extract_json(raw, kind="{")
        if valid_verdict(v):
            return v, None
        err = "parse"
    return None, err


def chunks_of(index, ids):
    missing = [i for i in ids if i not in index]
    if missing:
        raise KeyError(f"chunk ids not in the index: {missing[:3]}")
    return [index[i] for i in ids]


def length_stats(values):
    return f"{statistics.mean(values):.0f} / {statistics.median(values):.0f}" if values else "-"


def pack_report(rows, secs=None, n_run=None):
    """Markdown summary of pack-mode rows (items with a "verify" key are the verified ones)."""
    L = []
    items = rows
    skips = [r for r in items if r.get("skip")]
    answered = [r for r in items if not r.get("skip") and r.get("answer")]
    L.append(f"- items: {len(items)}; skips: {len(skips)}"
             f" ({len(skips) / max(1, len(items)):.0%})")
    L.append("- writer coverage: " + ", ".join(
        f"{k}={v}" for k, v in sorted(Counter(r.get("coverage") for r in answered).items(),
                                      key=lambda kv: str(kv[0]))))
    ver = [r for r in answered if isinstance(r.get("verify"), dict)]
    L.append(f"- verified with a verdict: {len(ver)}")
    agree = sum(1 for r in ver if r["verify"].get("coverage") == r.get("coverage"))
    L.append(f"- verifier coverage agreement: {agree}/{len(ver)}"
             + (f" ({agree / len(ver):.0%})" if ver else ""))
    g = sum(1 for r in ver if r["verify"].get("grounded") is True)
    go = sum(1 for r in ver if r["verify"].get("gap_ok") is True)
    L.append(f"- grounded rate: {g}/{len(ver)}" + (f" ({g / len(ver):.0%})" if ver else ""))
    L.append(f"- gap_ok rate: {go}/{len(ver)}" + (f" ({go / len(ver):.0%})" if ver else ""))
    ids = sum(1 for r in ver if r["verify"].get("identifiers_not_in_material"))
    L.append(f"- items with identifiers_not_in_material: {ids}")
    L.append(f"- verifier parse failures (verify null, not rejected): "
             f"{sum(1 for r in answered if r.get('verify_error') == 'parse')}")
    mech = {r["id"]: pack_filters.mechanical_problems(r) for r in items}
    bad = [r for r in items if mech[r["id"]] and mech[r["id"]] != ["skip"]]
    codes = Counter(c.split(":", 1)[0] for r in bad for c in mech[r["id"]])
    L.append(f"- rows with a mechanical problem: {len(bad)}"
             + (" (" + ", ".join(f"{k}={v}" for k, v in sorted(codes.items())) + ")" if codes else ""))
    both = sum(1 for r in ver if r["verify"].get("grounded") is True and not mech[r["id"]])
    L.append(f"- grounded and mechanical filter passed: {both} "
             f"({both / max(1, len(items)):.0%} of {len(items)} items)")
    L.append(f"- generation truncations (finish_reason length): "
             f"{sum(1 for r in items if r.get('gen_finish') == 'length')}")
    L.append(f"- generation parse errors: {sum(1 for r in items if r.get('gen_error'))}")
    lens = [len(r["answer"]) for r in answered]
    L.append(f"- answer chars, mean / median: {length_stats(lens)} (n={len(lens)})")
    for field in ("type", "coverage"):
        for k in sorted({str(r.get(field)) for r in answered}):
            v = [len(r["answer"]) for r in answered if str(r.get(field)) == k]
            L.append(f"  - {field}={k}: {length_stats(v)} (n={len(v)})")
    if secs is not None and n_run:
        L.append(f"- seconds per item (this run, {n_run} items): {secs / n_run:.1f}")
    return "\n".join(L)


def emit_report(rows, args, secs=None, n_run=None):
    text = pack_report(rows, secs, n_run)
    print("report:\n" + text, flush=True)
    if args.stats:
        Path(args.stats).write_text("# Pack run report\n\n"
                                    f"prompt_version: {PACK_PROMPT_VERSION}\n\n" + text + "\n")


async def pack_item(q, pk, index, llm, vllm, args, sems, out_f, lock, stats):
    ids = pk[args.k]
    wids = pk.get("withheld") or []
    try:
        chunks, wchunks = chunks_of(index, ids), chunks_of(index, wids)
    except KeyError as e:
        print(f"  no pack for {q['id']}: {e}", flush=True)
        stats["failed"] += 1
        return
    gen_finish, gen_error, skip, cov, nc, answer = None, None, False, None, "", ""
    async with sems[0]:
        try:
            raw, gen_finish = await llm.chat_full(PACK_GEN_SYSTEM,
                                                  pack_gen_user(chunks, q.get("type", ""), q["question"]))
        except Exception as e:  # not written: a rerun retries it
            print(f"  gen failed {q['id']}: {type(e).__name__}", flush=True)
            stats["failed"] += 1
            return
    obj = extract_json(raw, kind="{")
    if not isinstance(obj, dict):
        gen_error = "parse"
    elif obj.get("skip") is True:
        skip = True
    elif not isinstance(obj.get("answer"), str) or not obj["answer"].strip():
        gen_error = "parse"
    else:
        cov, nc, answer = obj.get("coverage"), obj.get("not_covered") or "", obj["answer"].strip()
    verify, verify_error = None, None
    if args.verify != "none" and answer and not gen_error:
        async with sems[1]:
            verify, verify_error = await verify_pack_item(vllm, args, chunks, wchunks,
                                                          q["question"], answer)
    row = {"id": q["id"], "question": q["question"], "type": q.get("type"),
           "category": q.get("category"), "origin": q.get("origin"),
           "treatment": pk.get("treatment", q.get("treatment")), "split": q.get("split"),
           "k": args.k, "chunk_ids": ids, "withheld": wids, "coverage": cov, "not_covered": nc,
           "answer": answer, "answer_chars": len(answer), "gen_finish": gen_finish,
           "skip": skip, "verify": verify, "prompt_version": PACK_PROMPT_VERSION}
    if gen_error:
        row["gen_error"] = gen_error
    row["mech"] = pack_filters.mechanical_problems(row)
    if verify_error:
        row["verify_error"] = verify_error
    async with lock:
        out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
        out_f.flush()
        stats["done"] += 1
        if stats["done"] % 20 == 0:
            print(f"  done={stats['done']} failed={stats['failed']}", flush=True)


def make_llms(args):
    usage = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "reasoning_tokens": 0}
    llm = LLM(args.url, args.model, args.api_key, args.temperature, args.max_tokens,
              args.extra_body, usage)
    vllm = llm
    if args.verify_model or args.verify_url:
        vllm = LLM(args.verify_url or args.url, args.verify_model or args.model, args.api_key,
                   0.0, args.verify_max_tokens, args.verify_extra_body,
                   {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "reasoning_tokens": 0})
    return llm, vllm


async def run_pack(args, llm=None, vllm=None):
    if not (args.questions and args.packs and args.index):
        raise SystemExit("--mode pack needs --questions, --packs and --index")
    index = load_index(args.index)
    packs = {r["id"]: r for r in read_jsonl(args.packs)}
    bank = read_jsonl(args.questions)
    only = None
    if args.ids:
        only = {l.strip() for l in Path(args.ids).read_text().splitlines() if l.strip()}
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    existing = read_jsonl(out) if out.exists() else []
    stale = {r.get("prompt_version") for r in existing} - {PACK_PROMPT_VERSION}
    if stale:
        raise SystemExit(f"{out} has rows with prompt_version {sorted(map(str, stale))} but this "
                         f"run is {PACK_PROMPT_VERSION}; use a new --out file, nothing was written")
    done = {r["id"] for r in existing}
    todo = [q for q in bank if q["id"] not in done and (only is None or q["id"] in only)]
    nopack = [q for q in todo if q["id"] not in packs]
    todo = [q for q in todo if q["id"] in packs]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(done)} already done, {len(todo)} to process, {len(nopack)} without a pack", flush=True)
    if llm is None:
        llm, vllm = make_llms(args)
    sems = (asyncio.Semaphore(args.concurrency), asyncio.Semaphore(args.concurrency))
    lock, stats = asyncio.Lock(), {"done": 0, "failed": 0}
    t0 = time.monotonic()
    with out.open("a") as f:
        await asyncio.gather(*(pack_item(q, packs[q["id"]], index, llm, vllm, args, sems, f, lock, stats)
                               for q in todo))
    print(f"finished: {stats['done']} written, {stats['failed']} failed (not written)", flush=True)
    emit_report(read_jsonl(out), args, time.monotonic() - t0, stats["done"])


async def verify_one_row(item, index, vllm, args, sem, out_f, lock, stats):
    try:
        chunks, wchunks = chunks_of(index, item["chunk_ids"]), chunks_of(index, item.get("withheld") or [])
    except KeyError as e:
        print(f"  no pack for {item['id']}: {e}", flush=True)
        stats["failed"] += 1
        return
    async with sem:
        verify, err = await verify_pack_item(vllm, args, chunks, wchunks, item["question"], item["answer"])
    row = {"id": item["id"], "verify": verify}
    if err:
        row["verify_error"] = err
    async with lock:
        out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
        out_f.flush()
        stats["done"] += 1
        stats["records"].append({**item, **row})


async def run_pack_verify(args, vllm=None):
    if not (args.in_path and args.index and args.out):
        raise SystemExit("--mode pack-verify needs --in, --index and --out")
    if vllm is None:
        if not args.verify_model:
            raise SystemExit("--mode pack-verify needs --verify-model")
        _, vllm = make_llms(args)
    index = load_index(args.index)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    done = {r["id"] for r in read_jsonl(out)} if out.exists() else set()
    seen = set(done)  # ids already handled or being handled
    sem, lock = asyncio.Semaphore(args.concurrency), asyncio.Lock()
    stats = {"done": 0, "failed": 0, "records": []}
    t0 = time.monotonic()
    with out.open("a") as f:
        while True:
            # look for the marker BEFORE reading, so a stop is never based on a stale read
            finished = bool(args.until_file) and Path(args.until_file).exists()
            pending = []
            for it in read_jsonl(args.in_path):
                if it["id"] in seen or it.get("skip") or not it.get("answer") or it.get("gen_error"):
                    continue
                if args.only_grounded and not (isinstance(it.get("verify"), dict)
                                               and it["verify"].get("grounded") is True):
                    continue
                seen.add(it["id"])
                pending.append(it)
            if args.limit:
                pending = pending[: max(0, args.limit - stats["done"] - stats["failed"])]
            if pending:
                await asyncio.gather(*(verify_one_row(it, index, vllm, args, sem, f, lock, stats)
                                       for it in pending))
            if not args.follow or args.limit and stats["done"] + stats["failed"] >= args.limit:
                break
            if not pending:
                if finished:
                    break
                await asyncio.sleep(args.follow)
    print(f"finished: {stats['done']} verified, {stats['failed']} failed (not written)", flush=True)
    emit_report(stats["records"], args, time.monotonic() - t0, stats["done"])


def build_parser():
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
    ap.add_argument("--verify-max-tokens", type=int, default=None,
                    help=f"default {OLD_VERIFY_MAX_TOKENS}; {PACK_VERIFY_MAX_TOKENS} in "
                         "--mode pack / pack-verify")
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
    ap.add_argument("--mode", choices=["pack", "pack-verify"],
                    help="pack: answer bank questions from their retrieval packs (needs --questions, "
                         "--packs, --index); pack-verify: second verifier over a pack-mode output "
                         "(needs --in, --index, --verify-model). Without --mode: chunk mode.")
    ap.add_argument("--questions", help="pack: question bank JSONL (gen_questions.py)")
    ap.add_argument("--packs", help="pack: packs JSONL (build_packs_v3.py --questions)")
    ap.add_argument("--index", default="work/v3/chunks.jsonl", help="pack: chunks JSONL, loaded once")
    ap.add_argument("--k", default="k6", help="pack: which pack list of the packs row to use")
    ap.add_argument("--ids", help="pack: only the question ids listed in this file, one per line")
    ap.add_argument("--verify-model", help="pack: model for the verifier (default --model)")
    ap.add_argument("--verify-url", help="pack: server for the verifier (default --url)")
    ap.add_argument("--stats", help="pack: write the end-of-run report to this markdown file")
    ap.add_argument("--in", dest="in_path", help="pack-verify: pack-mode output to verify")
    ap.add_argument("--only-grounded", action="store_true",
                    help="pack-verify: only items whose first verdict has grounded true")
    ap.add_argument("--follow", type=float, default=0, metavar="SECONDS",
                    help="pack-verify: when --in is exhausted, re-read it after this pause "
                         "(0 = do not follow)")
    ap.add_argument("--until-file", help="pack-verify with --follow: stop when this file exists "
                                         "and no unverified items remain")
    return ap


def parse_args(argv=None):
    args = build_parser().parse_args(argv)
    if args.verify_max_tokens is None:
        args.verify_max_tokens = PACK_VERIFY_MAX_TOKENS if args.mode else OLD_VERIFY_MAX_TOKENS
    args.existing = None
    if args.verify_extra_body is None:
        args.verify_extra_body = args.extra_body
    return args


async def main():
    args = parse_args()
    if args.mode == "pack":
        return await run_pack(args)
    if args.mode == "pack-verify":
        return await run_pack_verify(args)
    if args.reverify:
        args.existing = {r["chunk_id"]: r["items"] for r in map(json.loads, Path(args.reverify).open())
                         if r["items"] and (not args.sources or r["source"] in args.sources)}

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
