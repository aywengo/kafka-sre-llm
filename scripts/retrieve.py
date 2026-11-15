#!/usr/bin/env python3
"""BM25 retrieval over work/v2/chunks.jsonl (title indexed TITLE_W times, path once).

    retrieve(question, k=6, max_chars=14000) -> [chunk dicts, best first]
CLI: retrieve.py QUESTIONS.jsonl [--k 6 --max-chars 14000] > packs.jsonl
"""
import argparse, json, math, re, sys
from collections import Counter
from functools import lru_cache

CHUNKS = "work/v2/chunks.jsonl"
K1, B, TITLE_W = 1.2, 0.75, 2
STOP = set("a an and are as at be but by can do does for from has have how i if in is it its my of on or our so that the "
           "their then there these they this to was we what when where which why will with you your should would not no "
           "any all one two into than also about after before during while each other".split())


def tokens(text):
    out = []
    for w in re.findall(r"[A-Za-z0-9_.\-]+", text):
        w = w.strip(".-_").lower()
        if not w:
            continue
        parts = [p for p in re.split(r"[._\-]+", w) if p]
        if len(parts) > 1:
            out.append(w)
        out += [p for p in parts if p not in STOP and len(p) > 1]
    return out


@lru_cache(maxsize=1)
def index():
    chunks = [json.loads(l) for l in open(CHUNKS)]
    docs = []
    for c in chunks:
        path_words = c["path"].rsplit("/", 2)[-2:] if c["path"] else []
        docs.append(Counter(tokens(c["text"]) + tokens(c["title"]) * TITLE_W + tokens(" ".join(path_words))))
    df = Counter(t for d in docs for t in d)
    n = len(docs)
    idf = {t: math.log(1 + (n - f + 0.5) / (f + 0.5)) for t, f in df.items()}
    lens = [sum(d.values()) for d in docs]
    inv = {}
    for i, d in enumerate(docs):
        for t in d:
            inv.setdefault(t, []).append(i)
    return chunks, docs, idf, lens, sum(lens) / n, inv


def scores(question):
    chunks, docs, idf, lens, avg, inv = index()
    s = Counter()
    for t in set(tokens(question)):
        for i in inv.get(t, ()):
            tf = docs[i][t]
            s[i] += idf[t] * tf * (K1 + 1) / (tf + K1 * (1 - B + B * lens[i] / avg))
    return s


def retrieve(question, k=6, max_chars=14000):
    chunks = index()[0]
    out, used = [], 0
    for i, _ in scores(question).most_common(k):
        c = dict(chunks[i])
        room = max_chars - used
        if room < 500:
            break
        c["text"] = c["text"][:room]
        out.append(c)
        used += len(c["text"])
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("questions")
    ap.add_argument("--k", type=int, default=6)
    ap.add_argument("--max-chars", type=int, default=14000)
    a = ap.parse_args()
    for l in open(a.questions):
        if l.strip():
            q = json.loads(l)["question"]
            print(json.dumps({"question": q, "chunk_ids": [c["id"] for c in retrieve(q, a.k, a.max_chars)]}))
