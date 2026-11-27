"""Evaluation seam: the same steps as pipeline.ask() (expansion -> retrieval -> answer) but with a stored pack
accepted and every stage timed. It composes the existing functions and changes no prompt or retrieval setting.
Used by scripts/eval2.py only."""
import time

from . import config, prompts

GROUNDED_MODES = tuple(prompts.GROUNDED)  # EXPERIMENTAL: grounded-g1, grounded-g2


def chunks_by_ids(ids, index_path=None):
    """Full chunk dicts for the given ids, in the given order. Raises KeyError for an id the index lacks."""
    if any(str(i).startswith("kbe:") for i in ids):  # EXPERIMENTAL (M7 I4): entries of a stored K pack; chunk-only packs are as before
        from . import kb_entries
        return kb_entries.resolve_pack(ids, kb_entries.default_kb(), index_path)
    from . import retrieval
    path = str(index_path or config.index_file())
    by_id = {c["id"]: c for c in retrieval.RT.index(path)[0]}
    return [by_id[i] for i in ids]


def _accepts_info(gen):
    """True when the generator's chat takes the `info` record argument (the server generators do; stubs may not)."""
    import inspect
    try:
        return "info" in inspect.signature(getattr(gen, "_gen", gen).chat).parameters
    except (TypeError, ValueError):
        return False


def _timing_fields(lookup, info):
    """M7 KA1: prompt_tokens (the server's count) and ttft_s (streamed calls only) of the answer call, next to the cards'
    count and characters, so the cost of the longer prompt can be compared paired. None when the call did not report them."""
    info = info or {}
    lookup["prompt_tokens"] = info.get("prompt_tokens")
    lookup["ttft_s"] = info.get("ttft_s")


def run_item(question, gen, cfg=None, *, k="k6", mode="default", rerank=True, max_tokens=None,
             pack_ids=None, index_path=None, profile="frozen", thorough=None, thorough_checker=None, kb=None, lookup_kb=None,
             kb_entries=None):
    """One answer. pack_ids None: expansion + retrieval as pipeline.ask(). pack_ids given: no expansion, no
    retrieval; the model gets exactly those chunks in that order. Returns a dict with text, tokens, truncated,
    search_terms (None on replay), pack (chunk ids), seconds {search_terms, retrieval, answer, total}, messages.
    thorough: a verify_pass.Profile (EXPERIMENTAL answer profile; thorough_checker a service.AnswerChecker); the result
    then also has "thorough" (see verify_pass.draft_and_check) and seconds draft, thinking and check. None: as before.
    lookup_kb: a kb_lookup.KbLookup (EXPERIMENTAL, M7 KA1): the lookup cards of the question and the search terms (none
    on a replayed pack: no search terms) follow the excerpts; the result then also has "lookup". None: as before.
    kb_entries: a kb_entries.KbEntries (EXPERIMENTAL, M7 I4): up to 4 fact entries follow the frozen pack; the result then also has
    "kb_entries" (the merge's record). Not with a stored pack (replay it with kb_entries.resolve_pack), a profile or closed book."""
    cfg = cfg or config.load()
    if thorough is not None and (mode == "closed" or thorough_checker is None):
        raise ValueError("the thorough answer profile needs excerpts and an answer checker")
    if pack_ids is not None and profile != "frozen":
        raise ValueError("a stored pack (pack_ids) is replayed as it is; it is not made by a retrieval profile")
    if mode not in ("default", "strict", "closed") + GROUNDED_MODES:
        raise ValueError(f"mode must be 'default', 'strict', 'closed' or a grounded mode, got {mode!r}")
    if kb_entries is not None and (pack_ids is not None or profile != "frozen" or mode == "closed"):
        raise ValueError("the entries option needs a live frozen retrieval (no stored pack, candidate profile or closed book)")
    t0 = time.monotonic()
    if mode == "closed":
        # Closed book (configuration P0): no search-terms call, no retrieval, no pack. The model gets SYSTEM_PROMPT and
        # the bare question, as the v3 plain-base measurements did (eval/compare.py answer()).
        if pack_ids is not None or profile != "frozen":
            raise ValueError("closed book uses no pack and no retrieval profile")
        msgs = prompts.answer_messages(question, None)
        text, toks, trunc = gen.chat(msgs, max_tokens or cfg["answer"]["max_tokens"], None)
        t3 = time.monotonic()
        return {"text": text, "tokens": toks, "truncated": bool(trunc), "search_terms": None, "pack": [],
                "seconds": {"search_terms": 0.0, "retrieval": 0.0, "answer": t3 - t0, "total": t3 - t0},
                "messages": msgs}
    if pack_ids is None:
        from . import retrieval
        if thorough is None:
            exp, _, _ = gen.chat(prompts.expand_messages(question), max_tokens or cfg["expansion"]["max_tokens"])
        else:  # the search-terms call stays off thinking under the profile
            exp, _, _ = gen.chat(prompts.expand_messages(question), max_tokens or cfg["expansion"]["max_tokens"],
                                 thinking=False)
        t1 = time.monotonic()
        sel_kw = {} if profile == "frozen" else {"profile": profile}
        chunks = retrieval.select(question, exp, k=k, index_path=index_path, rerank=rerank, **sel_kw)
    else:
        exp = None
        t1 = time.monotonic()
        chunks = chunks_by_ids(pack_ids, index_path)
    kb_info = None
    if kb_entries is not None:  # off: the pack stays exactly as the retrieval made it
        chunks, kb_info = kb_entries.merge(chunks, question, exp, rerank=rerank)
    t2 = time.monotonic()
    cards_text, lookup = None, None
    if lookup_kb is not None:  # off: the prompt stays byte for byte as it was
        from . import kb_lookup
        cards_text, lookup = kb_lookup.answer_block(question, exp, kb=lookup_kb)
    if mode in GROUNDED_MODES:
        msgs = prompts.grounded_answer_messages(question, chunks, mode, cards_text=cards_text or None)
    else:
        msgs = prompts.answer_messages(question, chunks, strict=(mode == "strict"), cards_text=cards_text or None)
    info = None
    if lookup is not None and thorough is None and _accepts_info(gen):  # a record only: the request is not changed
        info = {}
    extra = {} if lookup is None else {"lookup": lookup}
    if kb_info is not None:
        extra["kb_entries"] = kb_info
    if thorough is not None:  # EXPERIMENTAL: the draft (thinking as the profile says), then the checking pass
        from . import verify_pass
        th = verify_pass.draft_and_check(question, msgs, chunks, gen, max_tokens or cfg["answer"]["max_tokens"],
                                         thorough, thorough_checker, kb=kb, t_start=t0)
        t3 = time.monotonic()
        if lookup is not None:
            _timing_fields(lookup, th["draft_info"])
        return {"text": th["text"], "tokens": th["tokens"], "truncated": th["truncated"], "search_terms": exp,
                "pack": [c["id"] for c in chunks],
                "seconds": {"search_terms": t1 - t0, "retrieval": t2 - t1, "answer": t3 - t2, "total": t3 - t0,
                            **th["seconds"]},
                "messages": msgs, "thorough": th, **extra}
    text, toks, trunc = gen.chat(msgs, max_tokens or cfg["answer"]["max_tokens"], None,
                                 **({} if info is None else {"info": info}))
    t3 = time.monotonic()
    if lookup is not None:
        _timing_fields(lookup, info)
    return {"text": text, "tokens": toks, "truncated": bool(trunc), "search_terms": exp,
            "pack": [c["id"] for c in chunks],
            "seconds": {"search_terms": t1 - t0, "retrieval": t2 - t1, "answer": t3 - t2, "total": t3 - t0},
            "messages": msgs, **extra}
