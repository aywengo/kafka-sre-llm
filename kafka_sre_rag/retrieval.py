"""Thin layer over the frozen retrieval in scripts/retrieve.py (imported by path, logic not copied)."""
import sys

from . import config, profiles

_SCRIPTS = str(config.ROOT / "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)
import retrieve as RT  # noqa: E402
import build_packs_v3 as BP  # noqa: E402  (pure helper select_ranked only)

SIZES = {"k6": "k6", "k3": "k3"}


def _capture_ce():
    """Make the cached reranker remember the scores of its latest predict() call (same trick as
    build_packs_v3.capture_ce, but applied once so repeated calls do not stack wrappers)."""
    model = RT.reranker()
    if not hasattr(model, "_last_ce"):
        predict = model.predict
        model._last_ce = {}

        def wrapped(*a, **kw):
            model._last_ce["ce"] = predict(*a, **kw)
            return model._last_ce["ce"]
        model.predict = wrapped
    return model._last_ce


_RERANKERS = {}


def _reranker_at(max_length):
    """The pinned reranker with a window of max_length tokens: the frozen instance (RT.reranker(), 512) or, for
    another length, a second cached CrossEncoder of the same pinned model and revision (CPU, offline)."""
    if max_length == config.load()["retrieval"]["reranker"]["max_length"]:
        return RT.reranker()
    if max_length not in _RERANKERS:
        from sentence_transformers import CrossEncoder
        _RERANKERS[max_length] = CrossEncoder(RT.RERANKER_MODEL, revision=RT.RERANKER_REVISION, device="cpu",
                                              max_length=max_length)
    return _RERANKERS[max_length]


def _select_candidate(question, expansion, profile, index_path, rerank, settings=None):
    """Pack of a candidate profile (r1 to r4; candidate.json). Unmeasured. Chunk dicts as select() returns them;
    bm25_rank is the rank in the whole BM25 list (0: not in it, a lookup hit), ce_score only for r1's pool.
    settings: a dict replacing the profile's own settings (r4 grid settings r4g1 to r4g8)."""
    cfg = config.load()
    prof = profiles.load_candidate()["profiles"][profile]
    if settings is not None:
        prof = settings
    path = str(index_path or profiles.index_file(profile, cfg))
    if prof["order"] == "additive":
        return _select_additive(question, expansion, prof, path, rerank, cfg)
    if prof["order"] == "r4":
        return _select_r4(question, expansion, prof, path, rerank, cfg)
    chunks = RT.index(path)[0]
    ids = [c["id"] for c in chunks]
    byid = {c["id"]: c for c in chunks}
    exp = expansion or ""
    sc = RT.scores(question + " " + exp if exp else question, path)
    bm = [ids[i] for i, _ in sc.most_common()]
    score = {ids[i]: s for i, s in sc.items()}
    pool = [ids[i] for i, _ in sc.most_common(cfg["retrieval"]["pool"])]
    if not pool:
        return []
    terms = [t.strip() for t in exp.split(",") if t.strip()]
    term_lists = [[ids[i] for i, _ in RT.scores(t, path).most_common(prof["per_term_n"])]
                  for t in terms[:prof["max_terms"]]]
    frozen_list, ce_of = None, {}
    if "frozen" in prof["fuse"] and rerank:
        last = _capture_ce()
        last.pop("ce", None)
        ranked = RT.rank(question, exp, pool=cfg["retrieval"]["pool"], index_path=path)  # the frozen ordering
        ce = last.get("ce", [])
        if sorted(ranked) != sorted(i for i, _ in sc.most_common(cfg["retrieval"]["pool"])) or len(ce) != len(pool):
            raise RuntimeError("retrieve.rank() no longer matches the candidate pool; retrieval changed")
        frozen_list = [ids[i] for i in ranked]
        ce_of = {c: round(float(ce[j]), 6) for j, c in enumerate(pool)}
    lookup = _lookup(path)
    rk = profiles.order(prof, question, exp, bm=bm, score=score, term_lists=term_lists, frozen_list=frozen_list,
                        source_of={c["id"]: c["source"] for c in chunks}, lookup=lookup)
    sel = profiles.select_ranked(rk, {c: len(byid[c]["text"]) for c in rk[:prof["k"]]}, prof["k"], prof["max_chars"])
    rank = {c: r + 1 for r, c in enumerate(bm)}
    return [{**byid[c], "bm25_rank": rank.get(c, 0), "ce_score": ce_of.get(c)} for c in sel]


def _select_additive(question, expansion, prof, path, rerank, cfg):
    """Profile r3: the frozen pack exactly, then lookup hits and the rest of the frozen order (profiles.additive_pack).
    rerank=False: the pool in BM25 order stands in for the frozen ordering (unmeasured, already flagged)."""
    chunks = RT.index(path)[0]
    ids = [c["id"] for c in chunks]
    byid = {c["id"]: c for c in chunks}
    exp = expansion or ""
    sc = RT.scores(question + " " + exp if exp else question, path)
    bm = [ids[i] for i, _ in sc.most_common()]
    score = {ids[i]: s for i, s in sc.items()}
    pool = [ids[i] for i, _ in sc.most_common(cfg["retrieval"]["pool"])]
    if not pool:
        return []
    frozen_list, ce_of = list(pool), {}
    if rerank:
        last = _capture_ce()
        last.pop("ce", None)
        ranked = RT.rank(question, exp, index_path=path)  # the frozen ordering
        ce = last.get("ce", [])
        if sorted(ranked) != sorted(i for i, _ in sc.most_common(cfg["retrieval"]["pool"])) or len(ce) != len(pool):
            raise RuntimeError("retrieve.rank() no longer matches the candidate pool; retrieval changed")
        frozen_list = [ids[i] for i in ranked]
        ce_of = {c: round(float(ce[j]), 6) for j, c in enumerate(pool)}
    source_of = {c["id"]: c["source"] for c in chunks}
    lens = {c["id"]: len(c["text"]) for c in chunks}
    pack, _ = profiles.additive_pack(prof, question, exp, frozen_list=frozen_list, bm=bm, score=score,
                                     source_of=source_of, lens=lens, lookup=_lookup(path))
    rank = {c: r + 1 for r, c in enumerate(bm)}
    return [{**byid[c], "bm25_rank": rank.get(c, 0), "ce_score": ce_of.get(c)} for c in pack]


def _select_r4(question, expansion, prof, path, rerank, cfg):
    """Profile r4: the frozen pack exactly, then up to k - 6 chunks from a scored set (profiles.r4_rerank_set) ordered
    by ONE second reranker call (profiles.r4_query, profiles.r4_passage; window ce_max_length), appended by
    profiles.r4_pack. rerank=False: the fused order stands in for both reranker calls (unmeasured, already flagged).
    ce_score: the frozen call's score for frozen-pool chunks, the r4 call's score for the others."""
    chunks = RT.index(path)[0]
    ids = [c["id"] for c in chunks]
    byid = {c["id"]: c for c in chunks}
    exp = expansion or ""
    sc = RT.scores(question + " " + exp if exp else question, path)
    bm = [ids[i] for i, _ in sc.most_common()]
    score = {ids[i]: s for i, s in sc.items()}
    pool = [ids[i] for i, _ in sc.most_common(cfg["retrieval"]["pool"])]
    if not pool:
        return []
    frozen_list, ce_of = list(pool), {}
    if rerank:
        last = _capture_ce()
        last.pop("ce", None)
        ranked = RT.rank(question, exp, index_path=path)  # the frozen ordering
        ce = last.get("ce", [])
        if sorted(ranked) != sorted(i for i, _ in sc.most_common(cfg["retrieval"]["pool"])) or len(ce) != len(pool):
            raise RuntimeError("retrieve.rank() no longer matches the candidate pool; retrieval changed")
        frozen_list = [ids[i] for i in ranked]
        ce_of = {c: round(float(ce[j]), 6) for j, c in enumerate(pool)}
    source_of = {c["id"]: c["source"] for c in chunks}
    lens = {c["id"]: len(c["text"]) for c in chunks}
    frozen_pack = profiles.select_ranked(frozen_list, lens, prof["base_k"], prof["base_chars"])
    terms, _ = profiles.r4_terms(prof, question, exp)
    term_list = profiles.term_order([[ids[i] for i, _ in RT.scores(t, path).most_common(prof["per_term_n"])]
                                     for t in terms])
    rank = {c: r + 1 for r, c in enumerate(bm)}
    depth = prof["lookup_score_depth"]
    lk = _lookup(path).hits(question, "", prof["lookup"],
                            lambda c: score.get(c, 0.0) if rank.get(c, 10 ** 9) <= depth else 0.0,
                            prof["lookup_max_chunks"])
    r = profiles.r4_rerank_set(prof, bm=bm, term_list=term_list, frozen_list=frozen_list, lookup_hits=lk,
                               frozen_pack=frozen_pack)
    order_r = r
    if rerank and r:
        query = profiles.r4_query(prof, question, terms)
        pairs = [(query, profiles.r4_passage(prof, byid[c])) for c in r]
        s4 = _reranker_at(prof["ce_max_length"]).predict(pairs, batch_size=prof["ce_batch_size"])   # one call
        order_r = [r[j] for j in sorted(range(len(r)), key=lambda j: -float(s4[j]))]   # ties: position in R
        for j, c in enumerate(r):
            ce_of.setdefault(c, round(float(s4[j]), 6))
    pack, _, _, _ = profiles.r4_pack(prof, frozen_list=frozen_list, order_r=order_r, source_of=source_of, lens=lens)
    return [{**byid[c], "bm25_rank": rank.get(c, 0), "ce_score": ce_of.get(c)} for c in pack]


_LOOKUPS = {}


def _lookup(path):
    if path not in _LOOKUPS:
        _LOOKUPS[path] = profiles.Lookup(RT.index(path)[0])
    return _LOOKUPS[path]


def select(question, expansion, *, k="k6", index_path=None, rerank=True, profile="frozen", settings=None):
    """profile r1 / r2 (candidate.json, unmeasured) take their own pack size; k must stay k6 then. rerank=False skips the cross-encoder (unmeasured). Chunk dicts with full text, best first; the ids are exactly those retrieve.retrieve() returns for k
    (k6 = 6 / 14,000 chars, k3 = 3 / 6,000). Each dict also has bm25_rank (1 = best) and ce_score."""
    if profiles.check(profile) != profiles.FROZEN:
        if k != "k6":
            raise ValueError(f"profile {profile} sets its own pack size; k must be 'k6', got {k!r}")
        return _select_candidate(question, expansion, profile, index_path, rerank, settings)
    if settings is not None:
        raise ValueError("settings apply to candidate profiles only")
    cfg = config.load()
    if k not in ("k6", "k3"):
        raise ValueError(f"k must be 'k6' or 'k3', got {k!r}")
    size = cfg["retrieval"][k]
    path = str(index_path or config.index_file(cfg))
    chunks = RT.index(path)[0]
    exp = expansion or ""
    cand = [i for i, _ in RT.scores(question + " " + exp if exp else question, path).most_common(cfg["retrieval"]["pool"])]
    if not cand:
        return []
    if not rerank:  # BM25 order only, no cross-encoder (dry runs and tests; not the measured retrieval)
        sel = BP.select(cand, chunks, size["k"], size["max_chars"])
        return [{**chunks[i], "bm25_rank": cand.index(i) + 1, "ce_score": None} for i in sel]
    last = _capture_ce()
    last.pop("ce", None)
    ranked = RT.rank(question, exp, index_path=path)
    ce = last.get("ce", [])
    if sorted(ranked) != sorted(cand) or len(ce) != len(cand):
        raise RuntimeError("retrieve.rank() no longer matches the candidate pool; retrieval changed")
    pos = {i: j for j, i in enumerate(cand)}
    sel = BP.select(ranked, chunks, size["k"], size["max_chars"])
    return [{**chunks[i], "bm25_rank": pos[i] + 1, "ce_score": round(float(ce[pos[i]]), 6)} for i in sel]
