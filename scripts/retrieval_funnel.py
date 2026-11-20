#!/usr/bin/env python3
"""Retrieval funnel: at which stage does retrieval lose the chunk a need depends on, for named variants.

Needs file: eval/m4/retrieval_needs_dev.jsonl (scripts/retrieval_needs.py). Row types: point (must point of a
scenario), scenario (its covering chunks), fact (mechanical item), header. Stored answers give the search terms and
the pack of the measured run; items give the question text. The frozen variant must reproduce the stored packs.

Stages per need and variant (the first stage that loses the chunk):
  not_in_index   the chunk id is not in the index file
  outside_pool   not among the candidates (BM25 pool, or the lists a fusion draws on)
  capped_runbook removed by the runbook cap
  below_cut      a candidate, but ranked after the first k
  cut_by_chars   within the first k, but cut by the character limit
  in_pack        delivered
Point rows with status not_indexed, no_source or unmapped keep that status as their stage.

The index file is read through scripts/retrieve.py (index(), scores(), tokens(), reranker()) without changing it; the
frozen index path is passed explicitly and its sha256 is checked against runs/v3/INDEX_FROZEN.json. Reranker scores
are cached under work/m5/funnel_cache/ (git-ignored); the 40 frozen candidates are always scored in one predict()
call in pool order, as retrieve.rank() does, so the frozen variant reproduces its floating-point scores. CPU only.

    .venv/bin/python scripts/retrieval_funnel.py replay --answers A.jsonl ... --items I.jsonl ... [--needs N]
    .venv/bin/python scripts/retrieval_funnel.py run --needs N --answers ... --items ... --presets frozen bm25 r1 ...
    .venv/bin/python scripts/retrieval_funnel.py full          # replay of all stored packs + the standard variants
"""
import argparse
import collections
import dataclasses
import hashlib
import json
import os
import re
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
os.environ.setdefault("HF_HUB_OFFLINE", "1")
import retrieve as RT  # noqa: E402  (read-only use; its defaults are not changed)

INDEX = "work/v3/chunks_s1.jsonl"
FROZEN = "runs/v3/INDEX_FROZEN.json"
CACHE_DIR = "work/m5/funnel_cache"
NEEDS = "eval/m4/retrieval_needs_dev.jsonl"
ANSWERS = ["work/m4/pilot2/ans_default.jsonl", "work/m4/s6/ans_A1.jsonl"]
ITEMS = ["eval/m4/pilot2/items.jsonl", "eval/m4/open/tier_a.jsonl"]
STAGES = ["not_indexed", "no_source", "unmapped", "not_in_index", "outside_pool", "capped_runbook", "below_cut",
          "cut_by_chars", "in_pack"]
BEST_FIRST = ["in_pack", "cut_by_chars", "below_cut", "capped_runbook", "outside_pool", "not_in_index"]
LOOKUP_SCORE_DEPTH = 1000      # a lookup hit's BM25 score counts only inside the top 1000 (else 0)
FROZEN_EXACT = 40              # candidates scored in one predict() call, as retrieve.rank() does


# ---------------------------------------------------------------- variants
@dataclasses.dataclass(frozen=True)
class Variant:
    pool: int = 40                 # BM25 candidates handed to the ordering
    order: str = "frozen"          # frozen (RRF of BM25 order and reranker order) | bm25 | rerank | fusion
    bm25_weight: float = 1.0       # weight of the BM25 list in the frozen fusion
    fuse: tuple = ()               # lists of a fusion: bm25, terms, frozen, rerank
    fuse_weights: tuple = ()       # one weight per list (default 1)
    bm25_depth: int = 200          # depth of the main BM25 list inside a fusion
    per_term_n: int = 3            # chunks taken from the BM25 query of each search term
    max_terms: int = 40            # search terms used
    rerank_query: str = "question"  # question | terms | question_terms | short_question
    lookup: int = 0                # exact-heading lookup: slots reserved at the front
    runbook_cap: int = -1          # at most this many runbook chunks (-1: no cap)
    k: int = 6
    chars: int = 14000
    additive: bool = False         # r3: the frozen pack (6 / 14,000) exactly, then lookup hits, then the rest of the
                                   # frozen order; the runbook cap and k / chars only limit what is added

    @property
    def uses_reranker(self):
        return self.order in ("frozen", "rerank") or (self.order == "fusion" and bool({"frozen", "rerank"} & set(self.fuse)))


FROZEN_V = Variant()
R2_BASE = dict(order="fusion", fuse=("bm25", "terms"), lookup=2, k=8, chars=20000)
PRESETS = {
    "frozen": FROZEN_V,
    "k8": Variant(k=8, chars=20000),
    "k10": Variant(k=10, chars=24000),
    "bm25": Variant(order="bm25"),
    "rerank": Variant(order="rerank"),
    "wbm2": Variant(bm25_weight=2),
    "terms": Variant(order="fusion", fuse=("bm25", "terms")),
    "lookup2": Variant(lookup=2),
    "runbook1": Variant(runbook_cap=1),
    "r1": Variant(**{**R2_BASE, "fuse": ("bm25", "terms", "frozen"), "runbook_cap": 1}),
    "r2": Variant(**R2_BASE),
    "r3": Variant(additive=True, lookup=2, runbook_cap=1, k=10, chars=24000),
    "pool100": Variant(pool=100),
    "pool200": Variant(pool=200),
    "rq_terms": Variant(rerank_query="terms"),
    "rq_question_terms": Variant(rerank_query="question_terms"),
    "rq_short": Variant(rerank_query="short_question"),
    "oracle40": Variant(order="bm25", k=40, chars=10 ** 7),
}
STANDARD = ["frozen", "k8", "k10", "bm25", "rerank", "wbm2", "terms", "lookup2", "runbook1", "r1", "r2", "r3", "oracle40"]
EXTENDED = STANDARD + ["pool100", "pool200", "rq_terms", "rq_question_terms", "rq_short"]


def parse_variant(spec):
    """'name' or 'name:key=value,key=value' (a preset, then overrides) -> (label, Variant)."""
    name, _, rest = spec.partition(":")
    v = PRESETS[name] if name in PRESETS else Variant()
    if name not in PRESETS and name != "custom":
        raise SystemExit(f"unknown preset {name!r}; presets: {', '.join(PRESETS)} (or custom:key=value,...)")
    over = {}
    for kv in filter(None, rest.split(",")):
        key, _, val = kv.partition("=")
        f = {x.name: x for x in dataclasses.fields(Variant)}.get(key)
        if f is None:
            raise SystemExit(f"unknown variant parameter {key!r}")
        if key in ("fuse", "fuse_weights"):
            over[key] = tuple((float(x) if key == "fuse_weights" else x) for x in val.split("+") if x)
        else:
            over[key] = type(getattr(FROZEN_V, key))(val)
    return spec, dataclasses.replace(v, **over)


def say_flush(*args):
    """print() that flushes: stdout redirected to a log file is block-buffered, and a guard that watches the log
    kills a job whose log does not grow (the 2026-11-19 stall was this, not a hang)."""
    print(*args, flush=True)


HEARTBEAT_S = 60               # a scoring call that runs longer prints a progress line this often


# ---------------------------------------------------------------- reranker scores with a disk cache
class CEScorer:
    """Reranker scores keyed by (query, chunk id), appended to a jsonl file as they are computed. `model` is anything
    with predict(pairs, batch_size=8) -> scores (default: the pinned reranker of scripts/retrieve.py, loaded lazily)."""

    def __init__(self, chunks_by_id, cache_file, model=None, chunk_chars=1500, batch_size=8):
        self.byid, self.path, self._model = chunks_by_id, Path(cache_file), model
        self.chunk_chars, self.batch = chunk_chars, batch_size
        self.mem, self.computed = {}, 0
        self.say, self.heartbeat = say_flush, HEARTBEAT_S
        if self.path.exists():
            for l in self.path.read_text().splitlines():
                if l.strip():
                    r = json.loads(l)
                    self.mem[(r["q"], r["c"])] = r["s"]

    @property
    def model(self):
        if self._model is None:
            self._model = RT.reranker()
        return self._model

    def _predict(self, qh, query, ids):
        pairs = [(query, (self.byid[c]["title"] + "\n" + self.byid[c]["text"])[:self.chunk_chars]) for c in ids]
        done, t0 = threading.Event(), time.time()

        def beat():
            while not done.wait(self.heartbeat):
                self.say(f"  scoring {len(pairs)} pairs, still running after {time.time() - t0:.0f}s")

        if self.heartbeat:
            threading.Thread(target=beat, daemon=True).start()
        try:
            sc = self.model.predict(pairs, batch_size=self.batch)   # one call, as retrieve.rank(): same scores
        finally:
            done.set()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            for c, s in zip(ids, sc):
                s = float(s)
                self.mem[(qh, c)] = s
                f.write(json.dumps({"q": qh, "c": c, "s": s}) + "\n")
        self.computed += len(ids)

    def scores(self, query, cand_ids, exact=FROZEN_EXACT):
        """{chunk id: score} for the candidates (best BM25 first). The first `exact` are scored in one call, in order
        (rescored together if any is missing); later ones are scored when missing."""
        qh = hashlib.sha1(query.encode()).hexdigest()[:20]
        head, tail = list(cand_ids[:exact]), list(cand_ids[exact:])
        if head and any((qh, c) not in self.mem for c in head):
            self._predict(qh, query, head)
        miss = [c for c in tail if (qh, c) not in self.mem]
        if miss:
            self._predict(qh, query, miss)
        return {c: self.mem[(qh, c)] for c in cand_ids}


# ---------------------------------------------------------------- pure ordering rules
# DRIFT GUARD: kafka_sre_rag/profiles.py holds a copy of the rules below (select_ranked, rrf, cap_runbooks, Lookup,
# ranking). tests/test_profiles_drift.py asserts identical packs for r1 and r2; change both files together.
def select_ranked(ranked, lens, k, max_chars):
    """The selection rule of retrieve.retrieve(): the first k that fit, stop at less than 500 characters of room."""
    out, used = [], 0
    for c in ranked[:k]:
        room = max_chars - used
        if room < 500:
            break
        out.append(c)
        used += min(lens[c], room)
    return out


def frozen_order(cand, ce, bm25_weight=1):
    """RRF of the BM25 order and the reranker order over the pool, as retrieve.rank() with rrf=True."""
    o = sorted(range(len(cand)), key=lambda j: -ce[cand[j]])
    pos = {j: r for r, j in enumerate(o)}
    return [cand[j] for j in sorted(range(len(cand)), key=lambda j: -(bm25_weight / (60 + j) + 1 / (60 + pos[j])))]


def rrf(lists, weights=None):
    sc = collections.Counter()
    for w, L in zip(weights or [1] * len(lists), lists):
        for r, c in enumerate(L):
            sc[c] += w / (60 + r)
    return [c for c, _ in sc.most_common()]


def cap_runbooks(ranked, source_of, cap):
    out, n = [], 0
    for c in ranked:
        if source_of[c] == "runbooks":
            n += 1
            if n > cap:
                continue
        out.append(c)
    return out


# ---------------------------------------------------------------- exact-heading lookup
_ERR = re.compile(r"^([A-Z][A-Z0-9_]{3,})\n-?\d+\n(True|False)\n", re.M)
_KEYHEAD = re.compile(r"^## (.+)$", re.M)
_IDRE = re.compile(r"^(--?[a-z][\w-]+|[\w-]+(\.[\w-]+)+|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+|([A-Z][a-z0-9]+){2,}|[\w.-]+/[\w./-]+)$")


def _norm(s):
    return s.strip().strip("`").strip().lower()


class Lookup:
    """Names (key, error, endpoint, kind) -> chunks whose title or inline '## heading' (or error-table row) is that name."""

    def __init__(self, chunks):
        self.keys = collections.defaultdict(set)
        for c in chunks:
            t = c["title"].split(" > ")[-1] if c["title"] else ""
            t = re.sub(r"\s*(schema reference|schema properties|command reference)$", "", _norm(t))
            if t:
                self.keys[_norm(t)].add(c["id"])
            for h in _KEYHEAD.findall(c["text"]):
                self.keys[_norm(h.split(" > ")[-1])].add(c["id"])
            if c["path"].endswith("protocol_errors.html"):
                for m in _ERR.finditer(c["text"]):
                    self.keys[m.group(1).lower()].add(c["id"])

    @staticmethod
    def identifiers(q, exp):
        out = [_norm(s) for s in re.findall(r"`([^`\n]{3,80})`", q) + re.findall(r"[\w./-]+", q) if _IDRE.match(s.strip())]
        out += [_norm(t) for t in exp.split(",") if _IDRE.match(t.strip())]
        return list(dict.fromkeys(out))

    def hits(self, q, exp, n, score_of):
        """Up to n chunk ids; ambiguous names (more than 12 chunks) are skipped; ties go to the smaller chunk id."""
        out = []
        for ident in self.identifiers(q, exp):
            cands = self.keys.get(ident, ())
            if not cands or len(cands) > 12:
                continue
            best = max(sorted(cands), key=lambda c: score_of(c))
            if best not in out:
                out.append(best)
            if len(out) >= n:
                break
        return out


# ---------------------------------------------------------------- per-item state and ranking
class Index:
    def __init__(self, path=INDEX):
        want = json.loads((ROOT / FROZEN).read_text())["sha256"]
        p = Path(path) if Path(path).is_absolute() else ROOT / path
        self.sha = hashlib.sha256(p.read_bytes()).hexdigest()
        self.frozen_ok = self.sha == want
        self.path = str(p)
        self.chunks = RT.index(self.path)[0]
        self.byid = {c["id"]: c for c in self.chunks}
        self.ids = [c["id"] for c in self.chunks]
        self.len = {c["id"]: len(c["text"]) for c in self.chunks}
        self.source = {c["id"]: c["source"] for c in self.chunks}
        self._lookup = None

    @property
    def lookup(self):
        if self._lookup is None:
            self._lookup = Lookup(self.chunks)
        return self._lookup


class ItemState:
    """Question, stored search terms and the BM25 ranking of one item (computed once, reused by every variant)."""

    def __init__(self, ix, q, exp, stored_pack=None):
        self.ix, self.q, self.exp, self.stored = ix, q, exp or "", stored_pack
        sc = RT.scores(q + " " + self.exp if self.exp else q, ix.path)
        self._sc = sc
        self._ranked = {}
        self.bm = [ix.ids[i] for i, _ in sc.most_common()]          # all chunks with a nonzero score, best first
        self.score = {ix.ids[i]: s for i, s in sc.items()}
        self.rank = {c: r + 1 for r, c in enumerate(self.bm)}
        self.terms = [x.strip() for x in self.exp.split(",") if x.strip()]
        self._per_term = {}

    def head(self, pool):
        """The pool exactly as retrieve.rank() takes it (Counter.most_common(pool))."""
        if pool not in self._ranked:
            self._ranked[pool] = [self.ix.ids[i] for i, _ in self._sc.most_common(pool)]
        return self._ranked[pool]

    def term_list(self, n, max_terms):
        key = (n, max_terms)
        if key not in self._per_term:
            tr = {}
            for t in self.terms[:max_terms]:
                for r, i in enumerate(RT.scores(t, self.ix.path).most_common(n)):
                    c = self.ix.ids[i[0]]
                    tr[c] = min(tr.get(c, 99), r)
            self._per_term[key] = sorted(tr, key=lambda c: tr[c])
        return self._per_term[key]

    def rerank_query_text(self, kind):
        if kind == "question":
            return self.q
        if kind == "terms":
            return self.exp[:600] or self.q
        if kind == "question_terms":
            return self.q + "\n" + self.exp
        if kind == "short_question":
            return self.q.split("\n\nEvidence:")[0][:400]
        raise ValueError(kind)


def ranking(st, v, scorer):
    """(ordered chunk ids, set of candidate chunk ids) of variant v for one item."""
    ix = st.ix
    pool = st.head(v.pool)
    ce = None
    if v.uses_reranker:
        ce = scorer.scores(st.rerank_query_text(v.rerank_query), st.head(max(v.pool, FROZEN_EXACT)) if v.order == "fusion" else pool)
    if v.order == "bm25":
        rk, cands = list(pool), set(pool)
    elif v.order == "rerank":
        rk, cands = sorted(pool, key=lambda c: -ce[c]), set(pool)
    elif v.order == "frozen":
        rk, cands = frozen_order(pool, ce, v.bm25_weight), set(pool)
    elif v.order == "fusion":
        lists = []
        for comp in v.fuse:
            if comp == "bm25":
                lists.append(st.bm[:v.bm25_depth])
            elif comp == "terms":
                lists.append(st.term_list(v.per_term_n, v.max_terms))
            elif comp == "frozen":
                lists.append(frozen_order(pool, ce, v.bm25_weight))
            elif comp == "rerank":
                lists.append(sorted(pool, key=lambda c: -ce[c]))
            else:
                raise ValueError(comp)
        rk = rrf(lists, list(v.fuse_weights) or None)
        cands = set(rk)
    else:
        raise ValueError(v.order)
    if v.additive:
        return additive_ranking(st, v, rk, set(pool))
    if v.lookup:
        lk = ix.lookup.hits(st.q, st.exp, v.lookup,
                            lambda c: st.score.get(c, 0.0) if st.rank.get(c, 10 ** 9) <= LOOKUP_SCORE_DEPTH else 0.0)
        rk = lk + [c for c in rk if c not in lk]
        cands |= set(lk)
    capped = set()
    if v.runbook_cap >= 0:
        kept = cap_runbooks(rk, ix.source, v.runbook_cap)
        capped = set(rk) - set(kept)
        rk = kept
    st.capped = capped
    return rk, cands


def additive_ranking(st, v, frozen_rk, pool):
    """Variant r3. DRIFT GUARD: a copy of profiles.additive_pack (tests/test_profile_r3.py asserts identical packs).
    Returns (the pack followed by the remaining candidates, candidates); st.final_n is the pack length."""
    ix = st.ix
    lens = ix.len
    pack = select_ranked(frozen_rk, lens, 6, 14000)           # the frozen pack, exactly
    used = 0
    for c in pack:
        used += min(lens[c], 14000 - used)
    lk = ix.lookup.hits(st.q, st.exp, v.lookup,
                        lambda c: st.score.get(c, 0.0) if st.rank.get(c, 10 ** 9) <= LOOKUP_SCORE_DEPTH else 0.0)
    inpack = set(pack)
    runbooks = 0                                              # runbook chunks added so far
    capped, rest, full = set(), [], False
    for c in dict.fromkeys(lk + frozen_rk):
        if c in inpack:
            continue
        if full or len(pack) >= v.k or v.chars - used < 500:
            full = True
            rest.append(c)
            continue
        if ix.source[c] == "runbooks":
            if runbooks >= v.runbook_cap:
                capped.add(c)
                continue
            runbooks += 1
        pack.append(c)
        inpack.add(c)
        used += min(lens[c], v.chars - used)
    st.capped = capped
    st.final_n = len(pack)
    return pack + rest, set(frozen_rk) | set(lk)


def selected(st, v, rk):
    """The pack a ranking yields: select_ranked for the others, the first final_n for the additive variant."""
    return rk[:st.final_n] if v.additive else select_ranked(rk, st.ix.len, v.k, v.chars)


def pack_of(st, v, scorer):
    rk, _ = ranking(st, v, scorer)
    return selected(st, v, rk)


def chunk_stage(st, v, scorer, cid):
    """Stage at which chunk `cid` is lost (or in_pack) for this item and variant."""
    if cid not in st.ix.byid:
        return "not_in_index"
    cache = st.__dict__.setdefault("_rk_cache", {})
    if v not in cache:
        rk, cands = ranking(st, v, scorer)
        cache[v] = (rk, cands, st.capped, set(selected(st, v, rk)))
    rk, cands, capped, sel = cache[v]
    if cid in capped:
        return "capped_runbook"
    if cid not in cands:
        return "outside_pool"
    if cid in sel:
        return "in_pack"
    return "cut_by_chars" if rk.index(cid) < v.k else "below_cut"


def best_stage(stages):
    return min(stages, key=BEST_FIRST.index)


# ---------------------------------------------------------------- inputs
def rd(path):
    p = Path(path)
    p = p if p.is_absolute() else ROOT / p
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def full_question(it):
    q = it["question"]
    return q + "\n\nEvidence:\n" + it["evidence"] if it.get("evidence") else q


def load_inputs(needs_path, answers_paths, items_paths):
    needs = [r for r in rd(needs_path) if r.get("row_type") != "header"]
    answers = {}
    for p in answers_paths:
        for a in rd(p):
            answers.setdefault(a["id"], a)
    items = {}
    for p in items_paths:
        for it in rd(p):
            items.setdefault(it["id"], it)
    return needs, answers, items


def make_states(ix, ids, answers, items, log=None):
    out = {}
    for n, i in enumerate(ids):
        if i in answers and i in items:
            a = answers[i]
            out[i] = ItemState(ix, full_question(items[i]), a.get("search_terms") or "", a.get("pack"))
    return out


# ---------------------------------------------------------------- replay: frozen variant against stored packs
def compare_pack(new, old):
    if list(new) == list(old):
        return "exact"
    if sorted(new) == sorted(old):
        return "order_only"
    return "content"


def replay(states, scorer, say=say_flush):
    res, detail = collections.Counter(), {}
    for n, (i, st) in enumerate(states.items()):
        t = time.time()
        new = pack_of(st, FROZEN_V, scorer)
        r = compare_pack(new, st.stored or [])
        res[r] += 1
        detail[i] = r
        say(f"[replay {n + 1}/{len(states)}] {i} {r} {time.time() - t:.1f}s")
    return res, detail


# ---------------------------------------------------------------- the funnel
def evaluate(ix, needs, states, variants, scorer, say=say_flush):
    """stages[label][(row_type, item, point)] = stage; one line per item."""
    stages = {lab: {} for lab, _ in variants}
    by_item = collections.defaultdict(list)
    for r in needs:
        by_item[r["item"]].append(r)
    skipped = sorted(set(by_item) - set(states))
    for n, (i, st) in enumerate(states.items()):
        if i not in by_item:
            continue
        t, c0 = time.time(), scorer.computed
        st.__dict__.pop("_rk_cache", None)
        for lab, v in variants:
            for r in by_item[i]:
                key = (r["row_type"], i, r.get("point"))
                if r["row_type"] == "point" and r["status"] != "mapped":
                    stages[lab][key] = r["status"]
                    continue
                stages[lab][key] = best_stage([chunk_stage(st, v, scorer, c) for c in r["chunks"]]) if r["chunks"] else "not_in_index"
        say(f"[{n + 1}/{len(states)}] {i} variants={len(variants)} reranked={scorer.computed - c0} {time.time() - t:.1f}s")
    return stages, skipped


def totals(needs, stages, base_label):
    """Per variant: served counts with gained/lost against base, and stage counts."""
    pts = [r for r in needs if r["row_type"] == "point"]
    mapped = [r for r in pts if r["status"] == "mapped"]
    scen = [r for r in needs if r["row_type"] == "scenario"]
    all_facts = [r for r in needs if r["row_type"] == "fact"]
    # facts of items tagged not_in_index (the a3 set) are reported apart; without such rows nothing changes
    nii = [r for r in all_facts if "not_in_index" in r.get("tags", [])]
    facts = [r for r in all_facts if "not_in_index" not in r.get("tags", [])]
    key = lambda r: (r["row_type"], r["item"], r.get("point"))  # noqa: E731
    groups = {"points": mapped, "weight2": [r for r in mapped if r["weight"] == 2], "scenarios": scen,
              "facts": facts, "facts_C1": [r for r in facts if "C1" in r.get("tags", [])],
              "facts_other": [r for r in facts if "C1" not in r.get("tags", [])]}
    if nii:
        groups["facts_not_in_index"] = nii
    out = {}
    for lab, st in stages.items():
        row = {}
        for g, rows in groups.items():
            served = [st.get(key(r)) == "in_pack" for r in rows]
            b = [stages[base_label].get(key(r)) == "in_pack" for r in rows]
            row[g] = {"served": sum(served), "of": len(rows), "gained": sum(1 for x, y in zip(served, b) if x and not y),
                      "lost": sum(1 for x, y in zip(served, b) if y and not x)}
        row["point_stages"] = dict(collections.Counter(st[key(r)] for r in pts if key(r) in st))
        row["point_stages_weight2"] = dict(collections.Counter(st[key(r)] for r in pts if r["weight"] == 2 and key(r) in st))
        row["fact_stages"] = dict(collections.Counter(st[key(r)] for r in facts if key(r) in st))
        if nii:
            row["not_in_index_fact_stages"] = dict(collections.Counter(st[key(r)] for r in nii if key(r) in st))
        out[lab] = row
    return out


def chunkref_stages(needs, states, v, scorer):
    """Covering-chunk references of the scenario rows: stage counts (the 119 of the plan's first funnel table)."""
    c = collections.Counter()
    for r in needs:
        if r["row_type"] == "scenario" and r["item"] in states:
            for cid in r["chunks"]:
                c[chunk_stage(states[r["item"]], v, scorer, cid)] += 1
    return dict(c)


def table(tot):
    extra = any("facts_not_in_index" in row for row in tot.values())
    lines = ["| variant | points | weight-2 | scenarios >=1 chunk | facts | C1 | other facts |"
             + (" facts not in the index (apart) |" if extra else ""), "|---|---|---|---|---|---|---|" + ("---|" if extra else "")]
    for lab, row in tot.items():
        cells = [f"{row[g]['served']}/{row[g]['of']} (+{row[g]['gained']}/-{row[g]['lost']})"
                 for g in ("points", "weight2", "scenarios", "facts", "facts_C1", "facts_other")
                 + (("facts_not_in_index",) if extra else ())]
        lines.append(f"| {lab} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def stage_table(tot, which="point_stages", title="points"):
    lines = [f"{title} by stage", "| variant | " + " | ".join(STAGES) + " |", "|---|" + "---|" * len(STAGES)]
    for lab, row in tot.items():
        lines.append(f"| {lab} | " + " | ".join(str(row[which].get(s, 0)) for s in STAGES) + " |")
    return "\n".join(lines)


# ---------------------------------------------------------------- CLI
def open_scorer(ix, cache_dir=CACHE_DIR, model=None):
    d = Path(cache_dir)
    d = d if d.is_absolute() else ROOT / d
    return CEScorer(ix.byid, d / f"ce_{ix.sha[:12]}_1500.jsonl", model=model)


def cmd_replay(a, say=say_flush):
    ix = Index(a.index)
    if not ix.frozen_ok:
        raise SystemExit(f"index sha256 {ix.sha} is not the frozen one in {FROZEN}")
    needs, answers, items = load_inputs(a.needs, a.answers, a.items)
    ids = list(answers) if a.all_answers else sorted({r["item"] for r in needs})
    states = make_states(ix, ids, answers, items)
    res, detail = replay(states, open_scorer(ix, a.cache_dir), say)
    say(f"replay of {len(states)} stored packs with the frozen variant: " + ", ".join(f"{k} {v}" for k, v in sorted(res.items())))
    bad = [i for i, r in detail.items() if r != "exact"]
    if bad:
        say("not exact: " + " ".join(f"{i}({detail[i]})" for i in bad))
    return res, detail


def cmd_run(a, say=say_flush):
    ix = Index(a.index)
    if not ix.frozen_ok:
        raise SystemExit(f"index sha256 {ix.sha} is not the frozen one in {FROZEN}")
    needs, answers, items = load_inputs(a.needs, a.answers, a.items)
    labels = ["frozen"] + [p for p in (a.presets or []) if p != "frozen"] + [s for s in (a.variant or [])]
    variants = [parse_variant(s) for s in labels]
    say("development set (WP5): numbers below are fitted to these items; not evidence of improvement")
    states = make_states(ix, sorted({r["item"] for r in needs}), answers, items)
    scorer = open_scorer(ix, a.cache_dir)
    stages, skipped = evaluate(ix, needs, states, variants, scorer, say)
    tot = totals(needs, stages, "frozen")
    say(table(tot))
    say(stage_table(tot))
    say(stage_table(tot, "fact_stages", "mechanical facts"))
    refs = chunkref_stages(needs, states, FROZEN_V, scorer)
    say(f"covering-chunk references (frozen), by stage: {refs}")
    if skipped:
        say(f"needs without stored answer or item (skipped): {len(skipped)}: {' '.join(skipped[:10])}")
    if a.json:
        p = Path(a.json)
        if p.exists():
            raise SystemExit(f"{p} exists; not overwritten")
        p.parent.mkdir(parents=True, exist_ok=True)
        per_need = {lab: {"|".join(str(x) for x in k if x is not None): v for k, v in st.items()} for lab, st in stages.items()}
        p.write_text(json.dumps({"totals": tot, "per_need_stage": per_need, "chunk_refs_frozen": refs, "skipped": skipped,
                                 "variants": {lab: dataclasses.asdict(v) for lab, v in variants}}, indent=1, default=list))
    return tot


def cmd_full(a):
    out = Path(a.out_dir)
    if out.exists():
        raise SystemExit(f"{out} exists; give a new --out-dir")
    out.mkdir(parents=True)
    a.all_answers = True
    cmd_replay(a)
    a.presets = EXTENDED if a.extended else STANDARD
    a.variant = None
    a.json = str(out / "funnel.json")
    cmd_run(a)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("replay", "run", "full"):
        p = sub.add_parser(name)
        p.add_argument("--needs", default=NEEDS)
        p.add_argument("--answers", nargs="+", default=ANSWERS)
        p.add_argument("--items", nargs="+", default=ITEMS)
        p.add_argument("--index", default=INDEX)
        p.add_argument("--cache-dir", default=CACHE_DIR)
        if name == "replay":
            p.add_argument("--all-answers", action="store_true", help="every stored answer, not only the needs' items")
        if name == "run":
            p.add_argument("--presets", nargs="*", help=" ".join(PRESETS))
            p.add_argument("--variant", action="append", help="name[:key=value,...], e.g. r2:k=10,chars=24000")
            p.add_argument("--json", help="write the totals to this new file")
        if name == "full":
            p.add_argument("--out-dir", default="work/m5/funnel_full")
            p.add_argument("--extended", action="store_true", help="also pool 100/200 and the other reranker queries")
    a = ap.parse_args(argv)
    {"replay": cmd_replay, "run": cmd_run, "full": cmd_full}[a.cmd](a)


if __name__ == "__main__":
    main()
