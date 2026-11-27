#!/usr/bin/env python3
"""M7 step S0 (agents/m7 KB2 design, section 6; task I7): the search terms of D7, made once.

For each of the 60 scenarios of eval/m7/d7/items.jsonl the runtime's own search-terms call is made, as
pipeline.ask() and evalhooks.run_item() make it before retrieval:

    gen.chat(prompts.expand_messages(question), cfg["expansion"]["max_tokens"])

with an OpenAIChatGenerator (temperature 0 as the runtime sends it, the frozen expansion limit of 300 tokens,
thinking off through chat_template_kwargs.enable_thinking=false in the request body). `question` is
eval2.full_question(item): the question plus the "Evidence:" block, the same string eval2.py hands to the runtime.
One request at a time. The server address comes from --url or OMLX_URL and the key from OMLX_API_KEY; neither is
written to the output, the log or an error line (errors are reported by their class name only).

Output, one JSON line per scenario in items order (default work/m7/d7/search_terms.jsonl), consumed by the D7 packs
step (task I5):

  id                  scenario id
  search_terms        the reply as the runtime uses it: the text gen.chat returns (think block stripped). This is
                      the string that retrieval.select(question, search_terms) and kb_lookup.answer_block replay.
  terms               list of terms from kafka_sre_rag.profiles.clean_terms(search_terms) (the runtime's own parser
                      of a reply into terms: fenced blocks, ANSWER/bullet/YAML lines dropped, duplicates removed)
  terms_malformed     the second value of clean_terms (fewer than 3 terms)
  raw_reply           the message content as the server sent it, before the think block is stripped
  prompt_sha256       sha256 of the call's messages in eval2.messages_fingerprint's canonical form (the value eval2
                      stores as search_terms_prompt_sha256); prompt_chars is the character count
  model, settings     the model id; {max_tokens, temperature, extra_body, expansion (frozen.json)}
  items_sha256        sha256 of the items file
  tokens, truncated, seconds   what the call reported
  step, stub          "S0"; true only in a --stub row

Resumable: rows already in the output are kept untouched and skipped; a row whose model, settings, prompt hash or
items hash differs from this run makes the run refuse. The first failed call stops the run (exit 1), so the file is
always a prefix of the items order. With 60 good rows the done marker <out>.done is written and the file's sha256 is
printed (it goes into runs/m7/D7_LEDGER.md).

--stub runs the same code on an in-process fake server (no network, no model); its rows are marked "stub": true, and
it refuses to write to the real default path.
"""
import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

ITEMS = ROOT / "eval/m7/d7/items.jsonl"
OUT = ROOT / "work/m7/d7/search_terms.jsonl"
EXTRA_THINKING_OFF = {"chat_template_kwargs": {"enable_thinking": False}}
N_EXPECTED = 60
STUB_LOG = []   # the request bodies a --stub run sent (for tests)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def read_rows(path):
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def stub_transport():
    """A fake /v1/chat/completions: a deterministic comma line per question; the first call also carries a think block."""
    import httpx

    def handler(request):
        if request.method == "GET":
            return httpx.Response(404, json={})
        body = json.loads(request.content)
        STUB_LOG.append(body)
        q = body["messages"][-1]["content"]
        h = hashlib.sha256(q.encode()).hexdigest()
        reply = ", ".join(f"term-{h[i:i + 4]}" for i in range(0, 40, 4)) + ", ``fenced``"
        if len(STUB_LOG) == 1:
            reply = "<think>planning</think>" + reply
        return httpx.Response(200, json={"model": body["model"], "choices": [{"message": {"content": reply}, "finish_reason": "stop"}],
                                         "usage": {"completion_tokens": 40}})
    return httpx.MockTransport(handler)


def make_generator(a, extra_body):
    from kafka_sre_rag.generators import OpenAIChatGenerator
    if a.stub:
        return OpenAIChatGenerator(base_url="http://127.0.0.1:9", model=a.model, extra_body=extra_body, transport=stub_transport())
    url = a.url or os.environ.get("OMLX_URL")
    if not url:
        raise SystemExit("error: the server URL is missing: set OMLX_URL or pass --url")
    return OpenAIChatGenerator(base_url=url, model=a.model, extra_body=extra_body)   # the key is read from OMLX_API_KEY


def record_raw(gen):
    """A response hook that keeps the unstripped message content of the last call (the generator returns it stripped)."""
    box = {"raw": None}

    def hook(response):
        try:
            response.read()
            box["raw"] = (response.json()["choices"][0]["message"].get("content")) if response.status_code < 400 else None
        except (ValueError, KeyError, IndexError, TypeError):
            box["raw"] = None
    hooks = gen.client.event_hooks
    gen.client.event_hooks = {"request": list(hooks.get("request", [])), "response": list(hooks.get("response", [])) + [hook]}
    return box


def one_row(gen, box, item, settings, model, items_sha, stub):
    from kafka_sre_rag import profiles, prompts
    import eval2
    question = eval2.full_question(item)
    messages = prompts.expand_messages(question)
    sha, chars = eval2.messages_fingerprint(messages)
    t0 = time.monotonic()
    box["raw"] = None
    text, toks, trunc = gen.chat(messages, settings["max_tokens"])
    seconds = round(time.monotonic() - t0, 3)
    terms, malformed = profiles.clean_terms(text)
    row = {"id": item["id"], "search_terms": text, "terms": terms, "terms_malformed": malformed,
           "raw_reply": box["raw"] if box["raw"] is not None else text, "prompt_sha256": sha, "prompt_chars": chars,
           "model": model, "settings": settings, "items_sha256": items_sha, "tokens": toks or None,
           "truncated": bool(trunc), "seconds": seconds, "step": "S0"}
    if stub:
        row["stub"] = True
    return row, messages, sha


def run(a):
    from kafka_sre_rag import config
    out = Path(a.out) if a.out else OUT
    items_f = Path(a.items) if a.items else ITEMS
    if a.stub and out.resolve() == OUT.resolve():
        print("error: --stub refuses the real output path; give --out", file=sys.stderr)
        return 2
    extra = json.loads(a.extra_body) if a.extra_body else EXTRA_THINKING_OFF
    if (extra.get("chat_template_kwargs") or {}).get("enable_thinking") is not False:
        print("error: thinking must be off: --extra-body must set chat_template_kwargs.enable_thinking to false", file=sys.stderr)
        return 2
    items = [json.loads(l) for l in items_f.read_text(encoding="utf-8").splitlines() if l.strip()]
    if len(items) != N_EXPECTED and not a.stub_any_size:
        print(f"error: {items_f.name} holds {len(items)} items, expected {N_EXPECTED}", file=sys.stderr)
        return 2
    items_sha = sha256_file(items_f)
    cfg = config.load()
    gen = make_generator(a, extra)
    from kafka_sre_rag import prompts
    import eval2
    body = gen.request_body(prompts.expand_messages("q"), cfg["expansion"]["max_tokens"])
    settings = {"max_tokens": cfg["expansion"]["max_tokens"], "temperature": body["temperature"], "extra_body": extra,
                "expansion": cfg["expansion"]}
    rows = read_rows(out)
    for i, r in enumerate(rows):
        bad = (i >= len(items) or r.get("id") != items[i]["id"] or r.get("model") != a.model or r.get("settings") != settings
               or r.get("items_sha256") != items_sha or bool(r.get("stub")) != bool(a.stub)
               or r.get("prompt_sha256") != eval2.messages_fingerprint(prompts.expand_messages(eval2.full_question(items[i])))[0])
        if bad:
            print(f"error: row {i + 1} of {out.name} does not match this run (id, model, settings, prompt or items hash, or stub flag); "
                  "not resuming", file=sys.stderr)
            return 2
    box = record_raw(gen)
    todo = items[len(rows):]
    print(f"[d7-terms] {len(items)} scenarios, {len(rows)} already done, {len(todo)} to do; model {a.model}", flush=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    for n, it in enumerate(todo, len(rows) + 1):
        try:
            row, _, _ = one_row(gen, box, it, settings, a.model, items_sha, a.stub)
        except Exception as e:   # the class only: a server error text may carry the address
            print(f"[d7-terms] {n}/{len(items)} id={it['id']} FAILED ({type(e).__name__}); stopping, run again to resume", flush=True)
            return 1
        with open(out, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        print(f"[d7-terms] {n}/{len(items)} id={it['id']} terms={len(row['terms'])} tokens={row['tokens']} "
              f"{row['seconds']}s{' TRUNCATED' if row['truncated'] else ''}", flush=True)
    rows = read_rows(out)
    if len(rows) != len(items):
        return 1
    digest = sha256_file(out)
    marker = Path(str(out) + ".done")
    if not marker.exists():
        marker.write_text(json.dumps({"rows": len(rows), "sha256": digest, "model": a.model, "items_sha256": items_sha,
                                      "stub": bool(a.stub)}, indent=1) + "\n", encoding="utf-8")
    print(f"[d7-terms] done: {len(rows)} rows, {len(todo)} new; sha256 {digest}", flush=True)
    return 0


def build_parser():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", required=True, help="model name on the server (the design names the Qwen 4-bit build)")
    ap.add_argument("--url", help="server URL (default: environment variable OMLX_URL); never written anywhere")
    ap.add_argument("--items", help="default eval/m7/d7/items.jsonl")
    ap.add_argument("--out", help="default work/m7/d7/search_terms.jsonl")
    ap.add_argument("--extra-body", help='JSON; default {"chat_template_kwargs": {"enable_thinking": false}}')
    ap.add_argument("--stub", action="store_true", help="fake in-process server; rows marked stub; refuses the real output path")
    ap.add_argument("--stub-any-size", action="store_true", help=argparse.SUPPRESS)   # tests with a small items file
    return ap


def main(argv=None):
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
