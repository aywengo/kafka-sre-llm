#!/usr/bin/env python3
"""Independent grounding audit: sample kept items, re-check them with another model
using the generator's verifier rules, report flag rate per source.

  python scripts/audit_items.py --raw work/v2/raw_qa.jsonl work/v2/raw_qa_runbooks2.jsonl \
      --chunks work/v2/chunks.jsonl --url http://127.0.0.1:8000/v1 --model GLM-5.3-Flash-oQ4e
Key from JUDGE_API_KEY. Resumes from --cache (one line per audited item).
"""
import argparse
import asyncio
import collections
import hashlib
import json
import os
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from generate_qa import LLM, VERIFY_BATCH_SYSTEM, add_context, extract_json


def key(chunk_id, it):
    return hashlib.sha1(f"{chunk_id}\0{it['question']}\0{it['answer']}".encode()).hexdigest()


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", nargs="+", required=True)
    ap.add_argument("--chunks", default="work/v2/chunks.jsonl")
    ap.add_argument("--url", default="http://127.0.0.1:8000/v1")
    ap.add_argument("--model", default="GLM-5.3-Flash-oQ4e")
    ap.add_argument("--sample", type=int, default=400)
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--max-tokens", type=int, default=32768)
    ap.add_argument("--cache", default="work/v2/audit_cache.jsonl")
    ap.add_argument("--min-per-stratum", type=int, default=30)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    chunks = {c["id"]: c for c in map(json.loads, Path(args.chunks).open())}
    # stratum = source, except the redo and incidents files, which are audited as their own strata
    def stratum(p, r):
        return ("config-redo" if "config_redo" in p else "incidents" if "incidents" in p else r["source"])
    pool = [(stratum(p, r), r["chunk_id"], it) for p in args.raw for r in map(json.loads, Path(p).open())
            for it in r["items"]]
    ctx = {c["id"]: c for c in add_context(list(chunks.values()), 1)}  # incidents were generated with +-1 neighbour
    rng = random.Random(args.seed)
    by_src = collections.defaultdict(list)
    for x in pool:
        by_src[x[0]].append(x)
    sample = []
    for src, xs in sorted(by_src.items()):
        n = max(args.min_per_stratum, round(args.sample * len(xs) / len(pool)))
        sample += rng.sample(xs, min(n, len(xs)))

    cache = {}
    if Path(args.cache).exists():
        cache = {c["key"]: c for c in map(json.loads, Path(args.cache).open())}
    groups = collections.defaultdict(list)
    for src, cid, it in sample:
        if key(cid, it) not in cache:
            groups[(src, cid)].append((src, it))

    llm = LLM(args.url, args.model, os.environ["JUDGE_API_KEY"], 0.0, args.max_tokens, {},
              collections.Counter())
    sem, lock = asyncio.Semaphore(args.concurrency), asyncio.Lock()

    async def audit(sc, items):
        cid = sc[1]
        c = (ctx if sc[0] == "incidents" else chunks)[cid]
        async with sem:
            pairs = "\n\n".join(f"[{i}]\nQ: {it['question']}\nA: {it['answer']}"
                                for i, (_, it) in enumerate(items, 1))
            try:
                v = extract_json(await llm.chat(
                    VERIFY_BATCH_SYSTEM, f"Source: {c['source']} — {c['title']}\n\n<excerpt>\n{c['text']}\n</excerpt>\n\n{pairs}"))
            except Exception as e:
                print(f"  audit failed {cid}: {e!r}")
                return
            verdict = {}
            for r in v or []:
                if isinstance(r, dict) and str(r.get("id", "")).isdigit():
                    verdict[int(r["id"])] = r
            async with lock:
                with open(args.cache, "a") as f:
                    for i, (src, it) in enumerate(items, 1):
                        r = verdict.get(i)
                        if r is None:
                            continue
                        f.write(json.dumps({"key": key(cid, it), "source": src, "chunk_id": cid,
                                            "flagged": r.get("grounded") is not True,
                                            "reason": r.get("reason"), "question": it["question"],
                                            "answer": it["answer"]}, ensure_ascii=False) + "\n")

    await asyncio.gather(*(audit(sc, xs) for sc, xs in groups.items()))

    cache = {c["key"]: c for c in map(json.loads, Path(args.cache).open())}
    rows = [cache[key(cid, it)] for _, cid, it in sample if key(cid, it) in cache]
    print(f"audited {len(rows)}/{len(sample)} sampled items")
    tot = collections.Counter(r["source"] for r in rows)
    bad = collections.Counter(r["source"] for r in rows if r["flagged"])
    for s in sorted(tot):
        print(f"{s:22s} {bad[s]:3d}/{tot[s]:3d} = {bad[s] / tot[s]:.1%}  (gate 15%)")
    print(f"{'OVERALL':22s} {sum(bad.values()):3d}/{len(rows):3d} = {sum(bad.values()) / max(1, len(rows)):.1%}  (gate 10%)")
    flagged = [r for r in rows if r["flagged"]]
    for r in rng.sample(flagged, min(10, len(flagged))):
        print(f"\n[{r['source']}] Q: {r['question'][:200]}\n  reason: {r['reason']}")


if __name__ == "__main__":
    asyncio.run(main())
