#!/usr/bin/env python3
"""Retrieval over work/v2/chunks.jsonl: LLM query expansion -> BM25 top 40 -> cross-encoder rerank (CPU) ->
top k, fused with the BM25 rank (RRF). BM25 indexes the title TITLE_W times, the path once.

    retrieve(question, k=6, max_chars=14000) -> [chunk dicts, best first]
CLI: retrieve.py QUESTIONS.jsonl [--k 6 --max-chars 14000] > packs.jsonl
"""
import argparse, json, math, re, sys
from collections import Counter
from functools import lru_cache

CHUNKS = "work/v2/chunks.jsonl"
EXPAND_SYSTEM = """You help search technical documentation (Apache Kafka 4.3, Strimzi, Kubernetes, Argo CD). Given an on-call engineer's question, list the exact terms the relevant documentation pages would contain: configuration keys, metric and log names, CLI tools with flags, Kubernetes/Strimzi/Argo CD resource kinds and fields, error and exception names, and the names of the concepts involved. Output one comma-separated line of 15 to 30 terms, most important first. No sentences, no explanations."""
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


@lru_cache(maxsize=1)
def reranker():
    from sentence_transformers import CrossEncoder
    return CrossEncoder("BAAI/bge-reranker-v2-m3", device="cpu", max_length=512)


def rank(question, expansion=None, pool=40, rerank_query=None, chunk_chars=1500, rrf=True):
    """Chunk indices, best first: BM25 on question + expansion -> top `pool` -> cross-encoder rerank (CPU)."""
    chunks = index()[0]
    cand = [i for i, _ in scores(question + " " + expansion if expansion else question).most_common(pool)]
    if rerank_query is None and not expansion:
        rerank_query = question
    rq = rerank_query or question
    ce = reranker().predict([(rq, (chunks[i]["title"] + "\n" + chunks[i]["text"])[:chunk_chars]) for i in cand], batch_size=8)
    order = sorted(range(len(cand)), key=lambda j: -ce[j])
    if rrf:
        pos = {j: r for r, j in enumerate(order)}
        order = sorted(range(len(cand)), key=lambda j: -(1 / (60 + j) + 1 / (60 + pos[j])))
    return [cand[j] for j in order]


def retrieve(question, k=6, max_chars=14000, expansion=None, rerank=True, **kw):
    chunks = index()[0]
    out, used = [], 0
    ranked = rank(question, expansion, **kw) if rerank else [i for i, _ in scores(question + " " + expansion if expansion else question).most_common(k)]
    for i in ranked[:k]:
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
