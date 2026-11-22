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

Profile r4 (M5; design runs/m5/reports/r0_r4_design.md): presets r4g1 to r4g8, one scored call per item and setting
cached whole (ce_<sha12>_<L>_<C>_frozenfmt_r4.jsonl), and the commands
    r4-split    build / check the S and C halves (runs/m5/r4_split.json) by the hash rule
    time-r4     the 20-item live timing pass of the r4 reranker call (uncached)
    run ... --split runs/m5/r4_split.json    the funnel with the per-half tables of section 5
    equal-r4    runtime equals simulation (stand-in reranker, or --live on a few items), pack rules 3
    choose-r4   the rule of section 3 as code; the verdict is printed first
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
STAGES = ["not_indexed", "no_source", "unmapped", "not_in_index", "outside_pool", "outside_rerank_set", "capped_runbook",
          "below_cut", "cut_by_chars", "in_pack"]
BEST_FIRST = ["in_pack", "cut_by_chars", "below_cut", "capped_runbook", "outside_rerank_set", "outside_pool", "not_in_index"]
R4_SPLIT = "runs/m5/r4_split.json"
R4_SPLIT_SALT = "m5-r4-split:"
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
    r4: bool = False               # r4: the frozen pack exactly, then a scored set ordered by one second reranker call;
                                   # runbook_cap is the added-runbook cap, k / chars the pack limits (lookup: question only)
    terms_source: str = "E"        # r4 terms: E (cleaned expansion) | EI (question identifiers + E) | I (identifiers)
    rerank_set: int = 24           # r4: size P of the scored set
    ce_max_length: int = 512       # r4: reranker window (tokens)
    ce_chunk_chars: int = 1500     # r4: passage length (characters)
    ce_batch_size: int = 8
    query: str = "Qf"              # r4 reranker query: Qf (question) | Qa (question head + terms) | Qb (terms only)
    q_head_chars: int = 800
    terms_chars: int = 600

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
R4_BASE = dict(r4=True, lookup=2, runbook_cap=1, k=10, chars=24000, per_term_n=3, max_terms=40, bm25_depth=200,
               ce_batch_size=8, q_head_chars=800, terms_chars=600)
R4_GRID = {   # name: (query, window L, passage C, scored set P, terms source); candidate.json r4_grid holds the same
    "r4g1": ("Qf", 512, 1500, 24, "E"), "r4g2": ("Qb", 512, 1500, 24, "E"), "r4g3": ("Qb", 512, 1500, 40, "E"),
    "r4g4": ("Qa", 1024, 3000, 24, "E"), "r4g5": ("Qa", 1024, 3000, 40, "E"), "r4g6": ("Qb", 1024, 3000, 24, "E"),
    "r4g7": ("Qa", 1024, 3000, 24, "EI"), "r4g8": ("Qa", 1024, 3000, 24, "I"),
}
for _n, (_q, _L, _C, _P, _t) in R4_GRID.items():
    PRESETS[_n] = Variant(**R4_BASE, query=_q, ce_max_length=_L, ce_chunk_chars=_C, rerank_set=_P, terms_source=_t)
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
        self.id = None
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
    if v.r4:
        return r4_ranking(st, v, rk, scorer)
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


# ---------------------------------------------------------------- profile r4
# DRIFT GUARD: a copy of kafka_sre_rag/profiles.py (clean_terms, question_identifiers, r4_terms, term_order,
# r4_rerank_set, r4_query, r4_pack); tests/test_profile_r4.py asserts identical packs. Change both files together.
_FENCE = re.compile(r"```.*?(```|\Z)", re.S)
_DROP_LINE = (re.compile(r"^\s*ANSWER\s*:", re.I), re.compile(r"^\s*[-#>]"), re.compile(r"^\s*[A-Za-z_][\w.\-]*\s*:(\s|$)"))


def clean_terms(exp):
    """(terms, malformed); see kafka_sre_rag.profiles.clean_terms."""
    text = _FENCE.sub("", exp or "")
    lines = [l for l in text.split("\n") if not any(p.match(l) for p in _DROP_LINE)]
    out, seen = [], set()
    for t in ", ".join(lines).split(","):
        t = t.strip(" `'\"\t\r\n")
        if not t or len(t) > 80 or len(t.split()) > 8 or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    out = out[:40]
    return out, len(out) < 3


def question_identifiers(q):
    return Lookup.identifiers(q, "")


def _dedupe_ci(seq, n):
    out, seen = [], set()
    for t in seq:
        if t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)
    return out[:n]


def r4_terms(v, question, exp):
    n = v.max_terms
    ex, bad = clean_terms(exp)
    ids = question_identifiers(question)
    if v.terms_source == "E":
        return (_dedupe_ci(ids, n) if bad else ex[:n]), bad
    if v.terms_source == "EI":
        return _dedupe_ci(ids + ex, n), bad
    if v.terms_source == "I":
        return _dedupe_ci(ids, n), bad
    raise ValueError(v.terms_source)


def _head(s, n):
    if len(s) <= n:
        return s
    cut = s[:n]
    i = max(cut.rfind(ch) for ch in " \n\t\r")
    return cut[:i] if i > 0 else cut


def _join_terms(terms, n):
    out, used = [], 0
    for t in terms:
        add = len(t) + (2 if out else 0)
        if used + add > n:
            break
        out.append(t)
        used += add
    return ", ".join(out)


def r4_query(v, question, terms):
    if v.query == "Qf":
        return question
    head = _head(question, v.q_head_chars)
    joined = _join_terms(terms, v.terms_chars)
    if v.query == "Qa":
        return head + "\nSearch terms: " + joined if joined else head
    if v.query == "Qb":
        return joined or head
    raise ValueError(v.query)


def r4_ranking(st, v, frozen_rk, scorer):
    """Variant r4g*. Returns (pack followed by the chunks not reached, candidates); st.final_n is the pack length,
    st.capped the chunks refused by the runbook cap, st.r4info the details (scored set, stop, terms, call)."""
    ix, lens = st.ix, st.ix.len
    frozen_pack = select_ranked(frozen_rk, lens, 6, 14000)
    used = 0
    for c in frozen_pack:
        used += min(lens[c], 14000 - used)
    terms, bad = r4_terms(v, st.q, st.exp)
    tl = {}
    for t in terms:
        for r, i in enumerate(RT.scores(t, ix.path).most_common(v.per_term_n)):
            c = ix.ids[i[0]]
            tl[c] = min(tl.get(c, 99), r)
    term_list = sorted(tl, key=lambda c: tl[c])
    lk = ix.lookup.hits(st.q, "", v.lookup,
                        lambda c: st.score.get(c, 0.0) if st.rank.get(c, 10 ** 9) <= LOOKUP_SCORE_DEPTH else 0.0)
    pre = rrf([st.bm[:v.bm25_depth], term_list, frozen_rk])
    inpack = set(frozen_pack)
    R = [c for c in dict.fromkeys(lk + pre) if c not in inpack][:v.rerank_set]
    query = r4_query(v, st.q, terms)
    sc, how, secs = scorer.r4.scores(v, query, R, st.id) if R else ([], "none", 0.0)
    order = [R[j] for j in sorted(range(len(R)), key=lambda j: -sc[j])]
    pack = list(frozen_pack)
    runbooks, capped, rest, stop = 0, [], [], "end"
    for j, c in enumerate(order):
        if c in inpack:
            continue
        if len(pack) >= v.k:
            stop = "k"
        elif v.chars - used < 500:
            stop = "chars"
        if stop != "end":
            rest = [x for x in order[j:] if x not in inpack]
            break
        if ix.source[c] == "runbooks":
            if runbooks >= v.runbook_cap:
                capped.append(c)
                continue
            runbooks += 1
        pack.append(c)
        inpack.add(c)
        used += min(lens[c], v.chars - used)
    st.capped = set(capped)
    st.final_n = len(pack)
    st.r4info = {"R": R, "order": order, "rest": rest, "stop": stop, "terms": terms, "malformed": bad, "pairs": len(R),
                 "query": query, "frozen_pack": frozen_pack, "how": how, "seconds": secs,
                 "pack_chars": sum(lens[c] for c in pack), "frozen_chars": sum(lens[c] for c in frozen_pack)}
    cands = set(pre) | set(lk) | set(frozen_rk) | set(frozen_pack)
    return pack + rest, cands


class R4Scorer:
    """Scores of ONE reranker call per (query, ordered id list), cached whole in
    ce_<sha12>_<L>_<C>_frozenfmt_r4.jsonl: {"q": query hash, "ids": hash of the ordered ids, "n": pairs, "s": scores}.
    Never piecemeal: the same pairs in the same order as the runtime's single predict() call."""

    def __init__(self, byid, cache_file, max_chars, batch_size=8, model=None, model_factory=None):
        self.byid, self.path, self.chars, self.batch = byid, Path(cache_file), max_chars, batch_size
        self._model, self._factory = model, model_factory
        self.mem, self.say, self.heartbeat, self.last = {}, say_flush, HEARTBEAT_S, None
        if self.path.exists():
            for l in self.path.read_text().splitlines():
                if l.strip():
                    r = json.loads(l)
                    self.mem[(r["q"], r["ids"])] = r["s"]

    @property
    def model(self):
        if self._model is None:
            self._model = self._factory()
        return self._model

    @staticmethod
    def key(query, ids):
        return hashlib.sha1(query.encode()).hexdigest()[:20], hashlib.sha1("\n".join(ids).encode()).hexdigest()[:20]

    def pairs(self, query, ids):
        return [(query, (self.byid[c]["title"] + "\n" + self.byid[c]["text"])[:self.chars]) for c in ids]

    def predict(self, query, ids):
        pairs = self.pairs(query, ids)
        done, t0 = threading.Event(), time.time()

        def beat():
            while not done.wait(self.heartbeat):
                self.say(f"  scoring {len(pairs)} pairs, still running after {time.time() - t0:.0f}s")

        if self.heartbeat:
            threading.Thread(target=beat, daemon=True).start()
        try:
            sc = [float(x) for x in self.model.predict(pairs, batch_size=self.batch)]   # ONE call
        finally:
            done.set()
        return sc, time.time() - t0

    def scores(self, query, ids):
        """(scores in the order of ids, 'cached' | 'computed', seconds of the call)."""
        k = self.key(query, ids)
        self.last = (query, tuple(ids))
        if k in self.mem:
            return self.mem[k], "cached", 0.0
        sc, secs = self.predict(query, ids)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            f.write(json.dumps({"q": k[0], "ids": k[1], "n": len(ids), "s": sc}) + "\n")
        self.mem[k] = sc
        return sc, "computed", secs


def _default_model(max_length):
    if max_length == 512:
        return RT.reranker()
    if max_length not in _MODELS:
        from sentence_transformers import CrossEncoder
        _MODELS[max_length] = CrossEncoder(RT.RERANKER_MODEL, revision=RT.RERANKER_REVISION, device="cpu",
                                           max_length=max_length)
    return _MODELS[max_length]


_MODELS = {}


class R4Scorers:
    """One R4Scorer per (window L, passage C); `model_of(L)` gives the reranker of a window (default: the pinned model)."""

    def __init__(self, ix, cache_dir, model_of=None, say=say_flush):
        d = Path(cache_dir)
        self.dir = d if d.is_absolute() else ROOT / d
        self.ix, self.model_of, self.say, self.by = ix, model_of or _default_model, say, {}
        self.computed = 0

    def of(self, v):
        key = (v.ce_max_length, v.ce_chunk_chars)
        if key not in self.by:
            L, C = key
            sc = R4Scorer(self.ix.byid, self.dir / f"ce_{self.ix.sha[:12]}_{L}_{C}_frozenfmt_r4.jsonl", C, v.ce_batch_size,
                          model_factory=lambda L=L: self.model_of(L))
            sc.say = self.say
            self.by[key] = sc
        return self.by[key]

    def scores(self, v, query, ids, item=None):
        sc, how, secs = self.of(v).scores(query, ids)
        if how == "computed":
            self.computed += len(ids)
        self.say(f"  r4 {item} {len(ids)} pairs L={v.ce_max_length} {how} {secs:.1f}s")
        return sc, how, secs

    def last(self, v):
        return self.of(v).last


def selected(st, v, rk):
    """The pack a ranking yields: select_ranked for the others, the first final_n for the additive variant."""
    return rk[:st.final_n] if (v.additive or v.r4) else select_ranked(rk, st.ix.len, v.k, v.chars)


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
        cache[v] = (rk, cands, st.capped, set(selected(st, v, rk)), dict(st.r4info) if v.r4 else None)
    rk, cands, capped, sel, r4i = cache[v]
    if v.r4:
        if cid in sel:
            return "in_pack"
        if cid in capped:
            return "capped_runbook"
        if cid in r4i["rest"]:
            cut = r4i["stop"] == "chars" and r4i["rest"].index(cid) < v.k - len(sel)
            return "cut_by_chars" if cut else "below_cut"
        return "outside_rerank_set" if cid in cands else "outside_pool"
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
    paths = [needs_path] if isinstance(needs_path, (str, Path)) else list(needs_path)
    needs = [r for p in paths for r in rd(p) if r.get("row_type") != "header"]
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
            out[i].id = i
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
def evaluate(ix, needs, states, variants, scorer, say=say_flush, info=None):
    """stages[label][(row_type, item, point)] = stage; one line per item. info (a dict) is filled with
    info[label][item] = the r4 details of the item (r4 variants only)."""
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
            if info is not None and v.r4 and (st.__dict__.get("_rk_cache") or {}).get(v):
                info.setdefault(lab, {})[i] = (st.__dict__["_rk_cache"][v][4])
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


# ---------------------------------------------------------------- r4: halves, tables, the choice rule
def split_key(item_id):
    return hashlib.sha256((R4_SPLIT_SALT + item_id).encode()).hexdigest()


def build_split(items_by_batch):
    """items_by_batch: {batch: [(item id, kind), ...]}. Within each batch and kind, ids sorted by the hash of
    "m5-r4-split:" + id, alternating S, C, S, C, starting with S."""
    S, C = [], []
    for batch, rows in items_by_batch.items():
        kinds = collections.defaultdict(list)
        for i, k in rows:
            kinds[k].append(i)
        for k, ids in kinds.items():
            for n, i in enumerate(sorted(ids, key=split_key)):
                (S if n % 2 == 0 else C).append(i)
    return sorted(S), sorted(C)


def pctl(vals, q):
    """q-th percentile (0-100) by linear interpolation; None for an empty list."""
    v = sorted(vals)
    if not v:
        return None
    x = (len(v) - 1) * q / 100
    lo = int(x)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (x - lo)


def group_rows(needs, group):
    mapped = [r for r in needs if r["row_type"] == "point" and r["status"] == "mapped"]
    if group == "points":
        return mapped
    if group == "weight2":
        return [r for r in mapped if r["weight"] == 2]
    if group == "facts":
        return [r for r in needs if r["row_type"] == "fact" and "not_in_index" not in r.get("tags", [])]
    raise ValueError(group)


def half_stats(needs, stages, label, base_label, items, group, info=None, time_row=None):
    """The figures of one table row: indexed points (or facts) of `items` (None: all), served, gain with an
    item-clustered standard error, stage counts, pack and added characters, pairs, malformed expansions."""
    rows = [r for r in group_rows(needs, group) if items is None or r["item"] in items]
    key = lambda r: (r["row_type"], r["item"], r.get("point"))  # noqa: E731
    per = collections.defaultdict(lambda: [0, 0, 0])      # item -> [points, served, base]
    gained = lost = 0
    for r in rows:
        a = stages[label].get(key(r)) == "in_pack"
        b = stages[base_label].get(key(r)) == "in_pack"
        x = per[r["item"]]
        x[0] += 1
        x[1] += a
        x[2] += b
        gained += a and not b
        lost += b and not a
    N = sum(x[0] for x in per.values())
    served, base = sum(x[1] for x in per.values()), sum(x[2] for x in per.values())
    m = len(per)
    g = (served - base) / N if N else 0.0
    var = sum(((x[1] - x[2]) - g * x[0]) ** 2 for x in per.values()) * m / (m - 1) / N ** 2 if m > 1 and N else 0.0
    st = collections.Counter(stages[label][key(r)] for r in rows if key(r) in stages[label])
    allrows = [r for r in needs if r["row_type"] == ("fact" if group == "facts" else "point")
               and (items is None or r["item"] in items)]
    apart = sum(1 for r in allrows if stages[label].get(key(r)) in ("not_indexed", "no_source", "unmapped", "not_in_index"))
    out = {"items": m, "points": N, "served": served, "base": base, "gained": gained, "lost": lost,
           "gain_pp": 100 * g, "se_pp": 100 * var ** 0.5, "stages": dict(st), "not_indexed_apart": apart,
           "per_item": {i: x for i, x in per.items()}}
    if info and label in info:
        its = [i for i in per if i in info[label]]
        out.update(pack_chars=[info[label][i]["pack_chars"] for i in its],
                   added_chars=[info[label][i]["pack_chars"] - info[label][i]["frozen_chars"] for i in its],
                   pairs=[info[label][i]["pairs"] for i in its],
                   malformed=sum(1 for i in its if info[label][i]["malformed"]), info_items=len(its))
    return out


R4_COLS = ["setting", "half", "items", "indexed points", "served (%)", "gain pp (SE)", "gained", "lost", "below the pack",
           "outside the scored set", "never found (outside the candidates)", "cut by characters", "capped runbook",
           "not indexed (apart)", "pack characters median / p90 (real)", "added characters median / p90",
           "reranker seconds added median / p90 (time-r4)", "scored pairs median", "malformed expansions"]


def fmt(x, d=0):
    return "-" if x is None else f"{x:.{d}f}"


def r4_table(table, time_s=None, title=""):
    """table[setting][half] = half_stats(...) (+ pack_chars ...); one markdown table (section 5)."""
    lines = [title] if title else []
    lines += ["| " + " | ".join(R4_COLS) + " |", "|" + "---|" * len(R4_COLS)]
    for lab, halves in table.items():
        for half, h in halves.items():
            ts = (time_s or {}).get(lab)
            stg = h["stages"]
            pc, ac = h.get("pack_chars", []), h.get("added_chars", [])
            lines.append("| " + " | ".join([
                lab, half, str(h["items"]), str(h["points"]), f"{h['served']} ({100 * h['served'] / h['points']:.1f} %)" if h["points"] else "-",
                f"{h['gain_pp']:+.1f} ({h['se_pp']:.1f})", str(h["gained"]), str(h["lost"]), str(stg.get("below_cut", 0)),
                str(stg.get("outside_rerank_set", 0)), str(stg.get("outside_pool", 0)), str(stg.get("cut_by_chars", 0)),
                str(stg.get("capped_runbook", 0)), str(h["not_indexed_apart"]),
                f"{fmt(pctl(pc, 50))} / {fmt(pctl(pc, 90))}", f"{fmt(pctl(ac, 50))} / {fmt(pctl(ac, 90))}",
                f"{fmt(pctl(ts, 50), 1)} / {fmt(pctl(ts, 90), 1)}" if ts else "-", fmt(pctl(h.get("pairs", []), 50)),
                f"{h.get('malformed', 0)} of {h.get('info_items', 0)}"]) + " |")
    return "\n".join(lines)


E1_NOTE = "0 frozen-served points lost on S, C and the 140 facts"
BAR_S, BAR_C = 25, 18        # percentage points, the stop bar of section 3
TIME_MEDIAN, TIME_P90 = 6.0, 9.0
CHARS_MEDIAN, CHARS_P90 = 11000, 16000
TIE_POINTS = 4


def choose_r4(table, time_s, equal):
    """The rule of section 3 of runs/m5/reports/r0_r4_design.md. table[setting] = {"S": stats, "C": stats,
    "facts": stats}; time_s[setting] = seconds of the r4 call per timed item; equal[setting] = True when runtime
    equals simulation on every item. Returns {"verdict": "CONTINUE" | "STOP", "chosen", "reasons", "settings"}.
    Eligible: (e1) no loss on S, C and the facts, (e2) runtime equals simulation, (e3) time median <= 6.0 s and p90
    <= 9.0 s, (e4) added real characters on S median <= 11,000 and p90 <= 16,000. Objective: served points on S.
    Tie-break: among eligible settings within 4 points of the best eligible, the smallest median added characters,
    then the lowest median seconds. Stop when the chosen setting gains under +25 pp on S or +18 pp on C (net, in
    points: net * 100 < bar * points), loses anything, or no setting is eligible."""
    rows = {}
    for lab, t in table.items():
        S, C, Fa = t["S"], t["C"], t.get("facts", {"lost": 0})
        ts = (time_s or {}).get(lab)
        ac = S.get("added_chars", [])
        why = []
        if S["lost"] or C["lost"] or Fa["lost"]:
            why.append(f"e1 loss (S {S['lost']}, C {C['lost']}, facts {Fa['lost']})")
        if not (equal or {}).get(lab):
            why.append("e2 runtime equals simulation not shown")
        if not ts:
            why.append("e3 no timing")
        elif pctl(ts, 50) > TIME_MEDIAN or pctl(ts, 90) > TIME_P90:
            why.append(f"e3 time median {pctl(ts, 50):.1f} / p90 {pctl(ts, 90):.1f} s")
        if not ac:
            why.append("e4 no character data")
        elif pctl(ac, 50) > CHARS_MEDIAN or pctl(ac, 90) > CHARS_P90:
            why.append(f"e4 added characters median {pctl(ac, 50):.0f} / p90 {pctl(ac, 90):.0f}")
        rows[lab] = {"eligible": not why, "why_not": why, "S_served": S["served"], "S_net": S["served"] - S["base"],
                     "C_net": C["served"] - C["base"], "add_med": pctl(ac, 50) if ac else None,
                     "sec_med": pctl(ts, 50) if ts else None}
    elig = [l for l, r in rows.items() if r["eligible"]]
    if not elig:
        return {"verdict": "STOP", "chosen": None, "reasons": ["no setting is eligible"], "settings": rows}
    best = max(rows[l]["S_served"] for l in elig)
    near = [l for l in elig if rows[l]["S_served"] >= best - TIE_POINTS]
    chosen = min(near, key=lambda l: (rows[l]["add_med"], rows[l]["sec_med"], -rows[l]["S_served"], l))
    S, C, Fa = table[chosen]["S"], table[chosen]["C"], table[chosen].get("facts", {"lost": 0})
    reasons = []
    if (S["served"] - S["base"]) * 100 < BAR_S * S["points"]:
        reasons.append(f"gain on S {S['served'] - S['base']} of {S['points']} points ({100 * (S['served'] - S['base']) / S['points']:+.1f} pp) is under +{BAR_S} pp")
    if (C["served"] - C["base"]) * 100 < BAR_C * C["points"]:
        reasons.append(f"gain on C {C['served'] - C['base']} of {C['points']} points ({100 * (C['served'] - C['base']) / C['points']:+.1f} pp) is under +{BAR_C} pp")
    if S["lost"] or C["lost"] or Fa["lost"]:
        reasons.append("a loss appears")
    return {"verdict": "STOP" if reasons else "CONTINUE", "chosen": chosen, "reasons": reasons, "settings": rows,
            "near": near}


def choose_report(res, table):
    """Text of the verdict (first line), then the per-setting reading and the shrinkage figures."""
    if res["verdict"] == "STOP":
        head = "VERDICT: STOP AND REPORT, NO B4: " + "; ".join(res["reasons"])
    else:
        head = f"VERDICT: CONTINUE, chosen {res['chosen']}: the stop bar is not hit"
    out = [head, ""]
    if res.get("chosen"):
        out.append(f"chosen by the tie-break among {', '.join(res.get('near', []))} (within {TIE_POINTS} points of the best eligible)")
    for lab, r in res["settings"].items():
        out.append(f"- {lab}: " + ("eligible" if r["eligible"] else "NOT eligible: " + "; ".join(r["why_not"]))
                   + f"; served on S {r['S_served']}, net gain S {r['S_net']}, C {r['C_net']}")
    c = res.get("chosen")
    if c:
        S, C = table[c]["S"], table[c]["C"]
        gS, gC = S["served"] - S["base"], C["served"] - C["base"]
        out += ["", "reported, deciding nothing:",
                f"- f = gain on C / gain on S = {gC} / {gS} = {gC / gS:.2f}" if gS else "- f: gain on S is 0",
                f"- projected B4 gain {0.6 * C['gain_pp']:+.1f} to {C['gain_pp']:+.1f} pp (gain on C x 0.6 to x 1.0)",
                f"- item-clustered standard error of the gain on C: {C['se_pp']:.1f} pp ({C['items']} items)"]
    return "\n".join(out)


def check_pack_rules(pack, stored):
    """Acceptance 3 for one item: starts with the stored frozen pack, no duplicates, at most 10, loses nothing."""
    return {"prefix": list(pack[:len(stored)]) == list(stored), "unique": len(pack) == len(set(pack)),
            "max10": len(pack) <= 10, "loses_0": set(stored) <= set(pack)}


class StandIn:
    """Reranker stand-in for the equality check: a pair already scored in the funnel's frozen cache gets its stored
    score (so the frozen call is the real one), any other pair a deterministic hash score. Maps passages back to
    chunk ids and records every call as (query, ids)."""

    def __init__(self, ix, frozen_mem):
        self.mem, self.calls, self.ix, self.hint = frozen_mem, [], ix, None
        self.by_text = {}
        for c in ix.chunks:
            for C in (1500, 3000):
                t = (c["title"] + "\n" + c["text"])[:C]
                self.by_text.setdefault(t, set()).add(c["id"])

    def predict(self, pairs, batch_size=8):
        """Duplicate passages (several chunks with the same first characters) map to several ids. `hint`, when set, is
        the list of chunk ids the next call is expected to carry (the frozen pool, in pool order): its ids settle the
        duplicates, so the frozen call returns exactly the stored scores. Without a hint a duplicate gets the smallest
        stored score among its ids (floating-point jitter of identical text), else a hash of the passage."""
        out, ids = [], []
        hint = self.hint if self.hint is not None and len(self.hint) == len(pairs) else None
        for n, (q, t) in enumerate(pairs):
            cands = sorted(self.by_text.get(t, ()))
            if not cands:
                raise RuntimeError("passage matches no chunk")
            ids.append(tuple(cands))
            qh = hashlib.sha1(q.encode()).hexdigest()[:20]
            if hint and hint[n] in cands and (qh, hint[n]) in self.mem:
                out.append(self.mem[(qh, hint[n])])
                continue
            known = {self.mem[(qh, c)] for c in cands if (qh, c) in self.mem}
            out.append(min(known) if known else int(hashlib.sha1((q + "\0" + t).encode()).hexdigest()[:8], 16) / 2 ** 32)
        self.calls.append((pairs[0][0] if pairs else "", tuple(ids)))
        return out

    @staticmethod
    def same_call(call, last):
        """call: (query, per-pair candidate ids) seen by the stand-in; last: (query, ids) of the funnel's scorer."""
        return call[0] == last[0] and len(call[1]) == len(last[1]) and all(c in cs for cs, c in zip(call[1], last[1]))


# ---------------------------------------------------------------- CLI
def open_scorer(ix, cache_dir=CACHE_DIR, model=None):
    d = Path(cache_dir)
    d = d if d.is_absolute() else ROOT / d
    sc = CEScorer(ix.byid, d / f"ce_{ix.sha[:12]}_1500.jsonl", model=model)
    sc.r4 = R4Scorers(ix, d)
    return sc


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
    info = {}
    stages, skipped = evaluate(ix, needs, states, variants, scorer, say, info)
    tot = totals(needs, stages, "frozen")
    say(table(tot))
    say(stage_table(tot))
    say(stage_table(tot, "fact_stages", "mechanical facts"))
    refs = chunkref_stages(needs, states, FROZEN_V, scorer)
    say(f"covering-chunk references (frozen), by stage: {refs}")
    if skipped:
        say(f"needs without stored answer or item (skipped): {len(skipped)}: {' '.join(skipped[:10])}")
    r4 = None
    r4labels = [lab for lab, v in variants if v.r4]
    if a.split and r4labels:
        split = load_split(a.split)
        time_s = None
        if a.time:
            time_s = {lab: x["seconds"] for lab, x in json.loads(Path(a.time).read_text())["settings"].items()}
        r4 = r4_tables(needs, stages, info, r4labels, split, time_s)
        for grp, title in (("points", "indexed must points"), ("weight2", "weight-2 points"), ("facts", "mechanical facts")):
            say("")
            say(r4_table(r4[grp], time_s, f"r4: {title} (S = choice half, C = check half; frozen is the base)"))
    if a.json:
        p = Path(a.json)
        if p.exists():
            raise SystemExit(f"{p} exists; not overwritten")
        p.parent.mkdir(parents=True, exist_ok=True)
        per_need = {lab: {"|".join(str(x) for x in k if x is not None): v for k, v in st.items()} for lab, st in stages.items()}
        p.write_text(json.dumps({"totals": tot, "per_need_stage": per_need, "chunk_refs_frozen": refs, "skipped": skipped, "r4": r4,
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


# ---------------------------------------------------------------- r4: commands
R4_ITEMS = {"pilot": "eval/m4/pilot2/items.jsonl", "b1": "eval/m4/b1/items.jsonl", "b2": "eval/m4/b2/items.jsonl"}
R4_NEEDS = [NEEDS, "work/m5/b1_needs.jsonl", "work/m4/b2/b2_needs.jsonl"]
R4_ANSWERS = ["work/m4/pilot2/ans_default.jsonl", "work/m4/s6/ans_A1.jsonl", "work/m4/b1/ans_default.jsonl",
              "work/m4/b2/ans_default.jsonl"]
R4_ITEM_FILES = ["eval/m4/pilot2/items.jsonl", "eval/m4/open/tier_a.jsonl", "eval/m4/b1/items.jsonl", "eval/m4/b2/items.jsonl"]
ALL_ANSWERS = R4_ANSWERS + ["work/m4/e2/ans_O_Q4D_a3.jsonl"]
ALL_ITEM_FILES = R4_ITEM_FILES + ["eval/m4/open/tier_a3.jsonl"]


def new_json(path, obj):
    p = Path(path)
    p = p if p.is_absolute() else ROOT / p
    if p.exists():
        raise SystemExit(f"{p} exists; not overwritten")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=1, default=list) + "\n")


def load_split(path):
    p = Path(path)
    p = p if p.is_absolute() else ROOT / p
    d = json.loads(p.read_text())
    return set(d["S"]), set(d["C"])


def timing_ids(S, tier_a_ids):
    """The 20 items of the time screen: the first 10 S scenario ids and the first 10 tier-A ids by hash order."""
    return sorted(S, key=split_key)[:10] + sorted(tier_a_ids, key=split_key)[:10]


def cmd_r4_split(a, say=say_flush):
    by_batch = {b: [(it["id"], it["kind"]) for it in rd(p)] for b, p in R4_ITEMS.items()}
    S, C = build_split(by_batch)
    needs, answers, _ = load_inputs(R4_NEEDS, R4_ANSWERS, [])
    for lab, half in (("S", S), ("C", C)):
        pts = [r for r in needs if r["row_type"] == "point" and r["status"] == "mapped" and r["item"] in set(half)]
        served = sum(1 for r in pts if r["item"] in answers and set(r["chunks"]) & set(answers[r["item"]].get("pack") or []))
        say(f"{lab}: {len(half)} items, {len(pts)} indexed points, frozen serves {served} ({100 * served / len(pts):.1f} %) "
            f"(stored packs)")
    say("S: " + " ".join(S))
    if a.out:
        new_json(a.out, {"rule": "within each batch (pilot, B1, B2) and item kind, ids sorted by sha256(\"m5-r4-split:\" + id), "
                         "alternating S, C, S, C starting with S", "salt": R4_SPLIT_SALT, "S": S, "C": C})
        say(f"written {a.out}")


def r4_tables(needs, stages, info, labels, split, time_s):
    S, C = split
    out = {}
    for grp in ("points", "weight2", "facts"):
        out[grp] = {}
        for lab in labels:
            if grp == "facts":
                out[grp][lab] = {"facts": half_stats(needs, stages, lab, "frozen", None, grp, info)}
            else:
                out[grp][lab] = {"S": half_stats(needs, stages, lab, "frozen", S, grp, info),
                                 "C": half_stats(needs, stages, lab, "frozen", C, grp, info),
                                 "S+C": half_stats(needs, stages, lab, "frozen", S | C, grp, info)}
    return out


def cmd_time_r4(a, say=say_flush):
    ix = Index(a.index)
    if not ix.frozen_ok:
        raise SystemExit("index is not the frozen one")
    S, _ = load_split(a.split)
    tier = [it["id"] for it in rd("eval/m4/open/tier_a.jsonl")]
    ids = timing_ids(S, tier)
    needs, answers, items = load_inputs(a.needs, a.answers, a.items)
    states = make_states(ix, ids, answers, items)
    scorer = open_scorer(ix, a.cache_dir)
    labels = a.settings or list(R4_GRID)

    class Timed:     # the r4 call alone, live, never from the cache and never stored
        def __init__(self, base):
            self.base = base

        def scores(self, v, query, ids, item=None):
            sc, secs = self.base.of(v).predict(query, ids)
            say(f"  r4 {item} {len(ids)} pairs L={v.ce_max_length} timed {secs:.1f}s")
            return sc, "timed", secs

    for L in sorted({PRESETS[l].ce_max_length for l in labels}):        # load and warm each window, not timed
        m = _default_model(L)
        m.predict([("warm up", "warm up passage")] * 2, batch_size=2)
    scorer.r4 = Timed(scorer.r4)
    res = {l: {"seconds": [], "pairs": [], "items": []} for l in labels}
    for n, i in enumerate(ids):
        st = states[i]
        for lab in labels:
            st.__dict__.pop("_rk_cache", None)
            ranking(st, PRESETS[lab], scorer)
            r = res[lab]
            r["seconds"].append(st.r4info["seconds"])
            r["pairs"].append(st.r4info["pairs"])
            r["items"].append(i)
            say(f"[time {n + 1}/{len(ids)}] {i} {lab} {st.r4info['pairs']} pairs {st.r4info['seconds']:.1f}s computed")
    for lab in labels:
        sec = res[lab]["seconds"]
        say(f"{lab}: median {pctl(sec, 50):.1f} s, p90 {pctl(sec, 90):.1f} s over {len(sec)} items "
            f"(stays if median <= {TIME_MEDIAN} and p90 <= {TIME_P90})")
    new_json(a.out, {"items": ids, "settings": res})


def cmd_equal_r4(a, say=say_flush):
    import tempfile
    import shutil
    from unittest import mock
    sys.path.insert(0, str(ROOT))
    from kafka_sre_rag import profiles, retrieval
    ix = Index(a.index)
    if not ix.frozen_ok:
        raise SystemExit("index is not the frozen one")
    needs, answers, items = load_inputs(a.needs, a.answers, a.items)
    labels = a.settings or list(R4_GRID)
    if a.live:
        S, _ = load_split(a.split)
        ids = sorted(S, key=split_key)[:4] + sorted((i["id"] for i in rd("eval/m4/open/tier_a.jsonl")), key=split_key)[:4]
    else:
        ids = [i for i in answers if i in items]
    states = make_states(ix, ids, answers, items)
    tmp = tempfile.mkdtemp()
    try:
        scorer = open_scorer(ix, a.cache_dir)
        patches = []
        if a.live:
            scorer.r4 = R4Scorers(ix, tmp, say=lambda *x: None)
            model = None
        else:
            model = StandIn(ix, scorer.mem)
            scorer._model = model
            scorer.path = Path(tmp) / "frozen_standin.jsonl"      # a stand-in score never reaches the real cache
            scorer.r4 = R4Scorers(ix, tmp, model_of=lambda L: model, say=lambda *x: None)
            patches = [mock.patch.object(RT, "reranker", lambda: model),
                       mock.patch.object(retrieval, "_reranker_at", lambda L: model)]
        for p in patches:
            p.start()
        tally = {l: collections.Counter() for l in labels}
        bad = []
        try:
            for n, i in enumerate(ids):
                st, t0, ok = states[i], time.time(), 0
                for lab in labels:
                    v = PRESETS[lab]
                    grid = profiles.grid_settings(lab)
                    if model:
                        model.calls.clear()
                        model.hint = st.head(FROZEN_EXACT)
                    got = [c["id"] for c in retrieval.select(st.q, st.exp, profile="r4", index_path=ix.path, settings=grid)]
                    call = list(model.calls) if model else None
                    st.__dict__.pop("_rk_cache", None)
                    want = pack_of(st, v, scorer)
                    t = tally[lab]
                    t["items"] += 1
                    t["packs_equal"] += got == want
                    if call is not None:
                        same = bool(st.r4info["pairs"] == 0 and len(call) == 1) or (
                            len(call) == 2 and StandIn.same_call(call[1], scorer.r4.last(v)))
                        t["calls_equal"] += same
                    else:
                        t["calls_equal"] += 1
                    rules = check_pack_rules(want, st.stored or [])
                    for k, val in rules.items():
                        t[k] += val
                    ok += got == want
                    if got != want or not all(rules.values()):
                        bad.append((i, lab, got == want, rules))
                say(f"[equal {n + 1}/{len(ids)}] {i} settings={len(labels)} runtime==simulation {ok}/{len(labels)} "
                    f"{time.time() - t0:.1f}s {'cached' if not a.live else 'computed'}")
        finally:
            for p in patches:
                p.stop()
    finally:
        shutil.rmtree(tmp, True)
    res = {}
    for lab, t in tally.items():
        n = t["items"]
        res[lab] = {"items": n, "packs_equal": t["packs_equal"], "calls_equal": t["calls_equal"], "prefix": t["prefix"],
                    "unique": t["unique"], "max10": t["max10"], "loses_0": t["loses_0"],
                    "runtime_equals_sim": bool(n) and all(t[k] == n for k in ("packs_equal", "calls_equal", "prefix", "unique", "max10", "loses_0"))}
        say(f"{lab}: {n} items; packs equal {t['packs_equal']}; r4 call equal {t['calls_equal']}; starts with the stored frozen "
            f"pack {t['prefix']}; unique {t['unique']}; at most 10 {t['max10']}; loses 0 {t['loses_0']}")
    for b in bad[:20]:
        say(f"MISMATCH {b}")
    if a.out:
        new_json(a.out, {"mode": "live" if a.live else "stand-in", "items": len(ids), "settings": res})


def cmd_choose_r4(a, say=say_flush):
    f = json.loads(Path(a.funnel).read_text())
    table = {lab: {"S": h["S"], "C": h["C"], "facts": f["r4"]["facts"][lab]["facts"]}
             for lab, h in f["r4"]["points"].items()}
    t = json.loads(Path(a.time).read_text())
    time_s = {lab: x["seconds"] for lab, x in t["settings"].items()}
    e = json.loads(Path(a.equal).read_text())
    equal = {lab: x["runtime_equals_sim"] for lab, x in e["settings"].items()}
    res = choose_r4(table, time_s, equal)
    say(choose_report(res, table))
    say("")
    say(r4_table({l: {"S": table[l]["S"], "C": table[l]["C"]} for l in table}, time_s, "points, per half"))
    say("")
    say(r4_table({l: {"facts": table[l]["facts"]} for l in table}, time_s, "the 140 facts"))
    for grp, title in (("weight2", "weight-2 points"),):
        say("")
        say(r4_table(f["r4"][grp], time_s, title + " (reported, decides nothing)"))
    say("")
    say(r4_table({l: {"S+C": f["r4"]["points"][l]["S+C"]} for l in table}, time_s, "S and C together (reported, decides nothing)"))
    if a.out:
        new_json(a.out, res)
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("replay", "run", "full"):
        p = sub.add_parser(name)
        p.add_argument("--needs", nargs="+", default=NEEDS)
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
            p.add_argument("--split", help="the S / C halves (runs/m5/r4_split.json): per-half tables of the r4 variants")
            p.add_argument("--time", help="time-r4 output (seconds of the r4 call), shown in the r4 tables")
        if name == "full":
            p.add_argument("--out-dir", default="work/m5/funnel_full")
            p.add_argument("--extended", action="store_true", help="also pool 100/200 and the other reranker queries")
    p = sub.add_parser("r4-split", help="build the S / C halves by the hash rule and print their counts")
    p.add_argument("--out", help="write the split to this new file (runs/m5/r4_split.json)")
    for name in ("time-r4", "equal-r4"):
        p = sub.add_parser(name)
        p.add_argument("--needs", nargs="+", default=R4_NEEDS)
        p.add_argument("--answers", nargs="+", default=ALL_ANSWERS if name == "equal-r4" else R4_ANSWERS)
        p.add_argument("--items", nargs="+", default=ALL_ITEM_FILES if name == "equal-r4" else R4_ITEM_FILES)
        p.add_argument("--index", default=INDEX)
        p.add_argument("--cache-dir", default=CACHE_DIR)
        p.add_argument("--split", default=R4_SPLIT)
        p.add_argument("--settings", nargs="*", help=" ".join(R4_GRID))
        p.add_argument("--out", help="write the result to this new file")
        if name == "equal-r4":
            p.add_argument("--live", action="store_true", help="real reranker on 8 items (4 S scenarios, 4 tier-A), not the stand-in")
    p = sub.add_parser("choose-r4", help="the rule of section 3 of the r4 design; the verdict is printed first")
    p.add_argument("--funnel", required=True, help="run --json output made with --split")
    p.add_argument("--time", required=True, help="time-r4 output")
    p.add_argument("--equal", required=True, help="equal-r4 output (stand-in, all items)")
    p.add_argument("--out", help="write the verdict to this new file")
    a = ap.parse_args(argv)
    {"replay": cmd_replay, "run": cmd_run, "full": cmd_full, "r4-split": cmd_r4_split, "time-r4": cmd_time_r4,
     "equal-r4": cmd_equal_r4, "choose-r4": cmd_choose_r4}[a.cmd](a)


if __name__ == "__main__":
    main()
