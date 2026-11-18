"""Evaluation seam: the same steps as pipeline.ask() (expansion -> retrieval -> answer) but with a stored pack
accepted and every stage timed. It composes the existing functions and changes no prompt or retrieval setting.
Used by scripts/eval2.py only."""
import time

from . import config, prompts


def chunks_by_ids(ids, index_path=None):
    """Full chunk dicts for the given ids, in the given order. Raises KeyError for an id the index lacks."""
    from . import retrieval
    path = str(index_path or config.index_file())
    by_id = {c["id"]: c for c in retrieval.RT.index(path)[0]}
    return [by_id[i] for i in ids]


def run_item(question, gen, cfg=None, *, k="k6", mode="default", rerank=True, max_tokens=None,
             pack_ids=None, index_path=None):
    """One answer. pack_ids None: expansion + retrieval as pipeline.ask(). pack_ids given: no expansion, no
    retrieval; the model gets exactly those chunks in that order. Returns a dict with text, tokens, truncated,
    search_terms (None on replay), pack (chunk ids), seconds {search_terms, retrieval, answer, total}, messages."""
    cfg = cfg or config.load()
    if mode not in ("default", "strict"):
        raise ValueError(f"mode must be 'default' or 'strict', got {mode!r}")
    t0 = time.monotonic()
    if pack_ids is None:
        from . import retrieval
        exp, _, _ = gen.chat(prompts.expand_messages(question), max_tokens or cfg["expansion"]["max_tokens"])
        t1 = time.monotonic()
        chunks = retrieval.select(question, exp, k=k, index_path=index_path, rerank=rerank)
    else:
        exp = None
        t1 = time.monotonic()
        chunks = chunks_by_ids(pack_ids, index_path)
    t2 = time.monotonic()
    msgs = prompts.answer_messages(question, chunks, strict=(mode == "strict"))
    text, toks, trunc = gen.chat(msgs, max_tokens or cfg["answer"]["max_tokens"], None)
    t3 = time.monotonic()
    return {"text": text, "tokens": toks, "truncated": bool(trunc), "search_terms": exp,
            "pack": [c["id"] for c in chunks],
            "seconds": {"search_terms": t1 - t0, "retrieval": t2 - t1, "answer": t3 - t2, "total": t3 - t0},
            "messages": msgs}
