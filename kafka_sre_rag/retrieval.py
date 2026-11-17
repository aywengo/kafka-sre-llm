"""Thin layer over the frozen retrieval in scripts/retrieve.py (imported by path, logic not copied)."""
import sys

from . import config

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


def select(question, expansion, *, k="k6", index_path=None, rerank=True):
    """rerank=False skips the cross-encoder (unmeasured). Chunk dicts with full text, best first; the ids are exactly those retrieve.retrieve() returns for k
    (k6 = 6 / 14,000 chars, k3 = 3 / 6,000). Each dict also has bm25_rank (1 = best) and ce_score."""
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
