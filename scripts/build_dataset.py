#!/usr/bin/env python3
"""Filter, deduplicate and split generated items into mlx-lm chat datasets.

Writes data/train.jsonl, data/valid.jsonl, data/test.jsonl with rows like
{"messages": [{"role": "system", ...}, {"role": "user", ...}, {"role": "assistant", ...}]}

Splits are done per source chunk, so near-duplicate questions generated from the
same excerpt never land in both train and valid/test.
"""
import argparse
import hashlib
import json
import random
import re
from collections import Counter
from pathlib import Path

SYSTEM_PROMPT = (
    "You are an expert Site Reliability Engineer for Apache Kafka, Strimzi, Kubernetes, "
    "GitOps and data streaming platforms. Give precise, practical answers with commands or "
    "YAML where useful, and call out risks and version-specific behaviour. If you are not "
    "sure about an exact config key or flag, say so."
)

LEAK_PATTERNS = re.compile(
    r"\b(the|this) (excerpt|passage|document|text|documentation snippet)\b|as mentioned above",
    re.I,
)


PLACEHOLDER = re.compile(r"(?<![${\w])\{[A-Z]\w*\}")
STRIMZI_API = re.compile(r"kafka\.strimzi\.io/v\w+")


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def shingles(s: str, k=5):
    w = norm(s).split()
    return {" ".join(w[i:i + k]) for i in range(max(1, len(w) - k + 1))}


def bucket(key: str, salt: str) -> float:
    return int(hashlib.sha1((salt + key).encode()).hexdigest()[:8], 16) / 0xFFFFFFFF


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", nargs="+", default=["work/raw_qa.jsonl"])
    ap.add_argument("--out", default="data")
    ap.add_argument("--valid-frac", type=float, default=0.03)
    ap.add_argument("--test-frac", type=float, default=0.02)
    ap.add_argument("--min-answer", type=int, default=120)
    ap.add_argument("--max-answer", type=int, default=7000)
    ap.add_argument("--near-dup", type=float, default=0.7, help="Jaccard threshold on questions")
    ap.add_argument("--general", help="optional JSONL of general chat data (messages format)")
    ap.add_argument("--general-ratio", type=float, default=0.15)
    ap.add_argument("--no-system", action="store_true")
    ap.add_argument("--skip-sources", nargs="*", default=[], help="drop records from these sources")
    ap.add_argument("--drop-pattern", action="append", default=[],
                    help="regex (case-insensitive); drop items whose question or answer matches")
    ap.add_argument("--cap-path", action="append", default=[], metavar="PATTERN=N",
                    help="keep at most N random items from chunks whose path matches PATTERN")
    ap.add_argument("--strimzi-api-group-version", default="kafka.strimzi.io/v1",
                    help="drop items that use any other kafka.strimzi.io/<version>")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    drop_res = [re.compile(p, re.I) for p in args.drop_pattern]
    caps = []
    for c in args.cap_path:
        pat, _, n = c.rpartition("=")
        caps.append((re.compile(pat), int(n)))

    rnd = random.Random(args.seed)
    seen_exact, seen_sh = set(), []
    drop = Counter()
    splits = {"train": [], "valid": [], "test": []}
    by_source = Counter()

    by_file = Counter()
    pattern_drops = Counter()

    def dropped(it, count=True):
        for r in drop_res:
            if r.search(it["question"]) or r.search(it["answer"]):
                if count:
                    pattern_drops[r.pattern] += 1
                return True
        return False

    skip_cap = set()  # (chunk_id, item index) removed by --cap-path
    for pat, n in caps:
        cand = [(rec["chunk_id"], i)
                for raw in args.raw for rec in map(json.loads, Path(raw).open())
                if pat.search(rec.get("path", "")) and rec.get("source") not in args.skip_sources
                for i, it in enumerate(rec["items"]) if not dropped(it, count=False)]
        keep = set(random.Random(args.seed).sample(cand, min(n, len(cand))))
        skip_cap |= set(cand) - keep
        drop["cap_path"] += len(cand) - len(keep)

    for raw in args.raw:
        for line in Path(raw).open():
            rec = json.loads(line)
            if rec.get("source") in args.skip_sources:
                drop["skipped_source"] += 1
                continue
            b = bucket(rec["chunk_id"], "split")
            split = "test" if b < args.test_frac else "valid" if b < args.test_frac + args.valid_frac else "train"
            for idx, it in enumerate(rec["items"]):
                if dropped(it):
                    drop["drop_pattern"] += 1
                    continue
                if (rec["chunk_id"], idx) in skip_cap:
                    continue
                text = it["question"] + "\n" + it["answer"]
                if PLACEHOLDER.search(text):
                    drop["placeholder"] += 1
                    continue
                if any(v != args.strimzi_api_group_version for v in STRIMZI_API.findall(text)):
                    drop["strimzi_api_version"] += 1
                    continue
                q, a = it["question"].strip(), it["answer"].strip()
                if not (args.min_answer <= len(a) <= args.max_answer) or len(q) < 15:
                    drop["length"] += 1
                    continue
                if LEAK_PATTERNS.search(q) or LEAK_PATTERNS.search(a):
                    drop["leak"] += 1
                    continue
                if a.count("```") % 2:
                    drop["broken_fence"] += 1
                    continue
                key = norm(q)
                if key in seen_exact:
                    drop["dup"] += 1
                    continue
                sh = shingles(q)
                # cheap near-dup check against recent questions from any source
                if any(len(sh & o) / max(1, len(sh | o)) > args.near_dup for o in seen_sh[-3000:]):
                    drop["near_dup"] += 1
                    continue
                seen_exact.add(key)
                seen_sh.append(sh)
                msgs = [] if args.no_system else [{"role": "system", "content": SYSTEM_PROMPT}]
                msgs += [{"role": "user", "content": q}, {"role": "assistant", "content": a}]
                splits[split].append({"messages": msgs})
                by_source[rec["source"]] += 1
                by_file[(Path(raw).name, rec["source"])] += 1

    if args.general:
        gen = [json.loads(l) for l in Path(args.general).open() if l.strip()]
        rnd.shuffle(gen)
        want = int(len(splits["train"]) * args.general_ratio / (1 - args.general_ratio))
        splits["train"] += gen[:want]
        print(f"mixed in {min(want, len(gen))} general examples")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, rows in splits.items():
        rnd.shuffle(rows)
        with (out / f"{name}.jsonl").open("w") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"{name}: {len(rows)}")
    print(f"per source: {dict(by_source)}")
    print(f"dropped: {dict(drop)}")
    print(f"drop_pattern by pattern: {dict(pattern_drops)}")
    print("per file:", {f"{f}:{s}": n for (f, s), n in sorted(by_file.items())})
    n = len(splits["train"])
    print(f"hint: iters for 2 epochs at batch 4 ≈ {max(1, 2 * n // 4)}")


if __name__ == "__main__":
    main()
