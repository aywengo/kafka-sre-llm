"""ask(): expansion -> retrieval -> answer, with the measured flag."""
import time
from dataclasses import dataclass, field

from . import config, profiles, prompts

GROUNDED_MODES = tuple(prompts.GROUNDED)  # EXPERIMENTAL: grounded-g1, grounded-g2
LOOKUP_REASON = ("lookup cards (off by default; reference cards of the question's names follow the excerpts; measured in v4.0.0 only "
                 "inside the release configuration RE, Gemma 4 31B 4-bit: their check Z3 passed on that model and failed on "
                 "Qwen3.8-27B; every other combination is unmeasured)")
KB_ENTRIES_REASON = ("knowledge-base entries (off by default; whole fact entries follow the frozen excerpts while the pack stays "
                     "within 10 items and 24,000 characters; measured in v4.0.0 only inside the release configuration RE: "
                     "Gemma 4 31B 4-bit, lookup cards, entries; audited 178 of 200 ok, 22 partial, 0 wrong, admitted under an "
                     "80 % bar set after the audit; every other combination is unmeasured)")


@dataclass
class Answer:
    text: str
    sources: list
    expansion: str
    timings: dict
    measured: bool
    tokens: int = 0
    truncated: bool = False
    unmeasured_reasons: list = field(default_factory=list)
    messages: list = field(default_factory=list)
    profile_fields: dict = field(default_factory=dict)  # {} for frozen; retrieval_profile, settings hash, measured False
    thorough: dict = None  # EXPERIMENTAL answer profile only: first_pass_text, record, draft_info, seconds; else None
    lookup: dict = None  # EXPERIMENTAL lookup cards only: cards_found, cards, chars, seconds, catalogue_sha256; else None
    kb_entries: dict = None  # EXPERIMENTAL entries only: added ids, skipped counts, candidates, pack_chars, seconds; else None


def is_reference(gen, cfg):
    ref = cfg["reference_generator"]
    ident = getattr(gen, "identity", None) or {}
    if ident.get("backend") != ref["backend"] or ident.get("adapter"):
        return False
    model = str(ident.get("model") or "")
    return ref["snapshot"] in model or model.rstrip("/").endswith(ref["local_wrapper"].rsplit("/", 1)[-1])


def ask(question, gen, cfg=None, *, k="k6", mode="default", index_path=None, on_token=None, rerank=True,
        max_tokens=None, profile="frozen", thorough=None, thorough_checker=None, kb=None, lookup_cards=False, lookup_kb=None,
        kb_entries=None):
    """lookup_cards: EXPERIMENTAL (M7 KA1), off by default: the reference cards of the names in the question and the search
    terms follow the excerpts in the prompt (lookup_kb: a kb_lookup.KbLookup, or None for the shared one).
    thorough: a verify_pass.Profile (EXPERIMENTAL answer profile, off by default) with thorough_checker a
    service.AnswerChecker; the answer is then not streamed (on_token must be None).
    kb_entries: EXPERIMENTAL (M7 I4), off by default: a kb_entries.KbEntries; up to 4 fact entries follow the frozen pack (frozen
    profile only). None: the pack and the prompt stay exactly as retrieval.select made them."""
    t_start = time.monotonic()
    if thorough is not None and (on_token is not None or thorough_checker is None):
        raise ValueError("the thorough answer profile needs an answer checker and is not streamed")
    cfg = cfg or config.load()
    profiles.check(profile)
    if mode not in ("default", "strict") + GROUNDED_MODES:
        raise ValueError(f"mode must be 'default', 'strict' or a grounded mode, got {mode!r}")
    from . import retrieval  # lazy: pulls in scripts/retrieve.py
    reasons = []
    if mode != "default":
        reasons.append(f"mode {mode}")
    if k != "k6":
        reasons.append(f"k {k}")
    if not is_reference(gen, cfg):
        reasons.append("generator is not the reference model")
    if profile != "frozen":
        reasons.append(profiles.unmeasured_reason(profile))
    if index_path and str(index_path) != str(profiles.index_file(profile, cfg)):
        reasons.append("index override")
    if not rerank:
        reasons.append("no reranker")
    if max_tokens is not None:  # command-line override; the default (frozen) limits stay otherwise
        reasons.append(f"max_tokens override {max_tokens}")
    if thorough is not None:
        reasons.append(thorough.unmeasured_reason())
    if lookup_cards:
        reasons.append(LOOKUP_REASON)
    if kb_entries is not None:
        if profile != "frozen":
            raise ValueError("the entries option composes with the frozen profile only (not measured with a candidate profile)")
        reasons.append(KB_ENTRIES_REASON)
    t0 = time.monotonic()
    if thorough is None:
        exp, _, _ = gen.chat(prompts.expand_messages(question), max_tokens or cfg["expansion"]["max_tokens"])
    else:  # the search-terms call stays off thinking under the profile
        exp, _, _ = gen.chat(prompts.expand_messages(question), max_tokens or cfg["expansion"]["max_tokens"],
                             thinking=False)
    t1 = time.monotonic()
    sel_kw = {} if profile == "frozen" else {"profile": profile}  # frozen call stays exactly as it was
    chunks = retrieval.select(question, exp, k=k, index_path=index_path, rerank=rerank, **sel_kw)
    kb_info = None
    if kb_entries is not None:  # off: the pack stays exactly as the retrieval made it
        chunks, kb_info = kb_entries.merge(chunks, question, exp, rerank=rerank)
    t2 = time.monotonic()
    cards_text, lookup = None, None
    if lookup_cards:  # off: the prompt stays byte for byte as it was
        from . import kb_lookup
        cards_text, lookup = kb_lookup.answer_block(question, exp, kb=lookup_kb)
    if mode in GROUNDED_MODES:
        msgs = prompts.grounded_answer_messages(question, chunks, mode, cards_text=cards_text or None)
    else:
        msgs = prompts.answer_messages(question, chunks, strict=(mode == "strict"), cards_text=cards_text or None)
    th = None
    if thorough is None:
        text, toks, trunc = gen.chat(msgs, max_tokens or cfg["answer"]["max_tokens"], on_token)
    else:  # EXPERIMENTAL: draft (thinking as the profile says), then the checking pass; nothing is streamed
        from . import verify_pass
        th = verify_pass.draft_and_check(question, msgs, chunks, gen, max_tokens or cfg["answer"]["max_tokens"],
                                         thorough, thorough_checker, kb=kb, t_start=t_start)
        text, toks, trunc = th["text"], th["tokens"], th["truncated"]
    t3 = time.monotonic()
    sources = [{"source": c["source"], "title": c["title"], "path": c["path"],
                "bm25_rank": c.get("bm25_rank"), "ce_score": c.get("ce_score")} for c in chunks]
    timings = {"expand_s": t1 - t0, "retrieve_s": t2 - t1, "answer_s": t3 - t2}
    if lookup is not None:
        timings["lookup_s"] = lookup["seconds"]
    if th is not None:
        timings.update(draft_s=th["seconds"]["draft"], thinking_s=th["seconds"]["thinking"],
                       check_s=th["seconds"]["check"])
    return Answer(text, sources, exp, timings,
                  not reasons, toks, trunc, reasons, msgs, profiles.manifest_fields(profile), th, lookup, kb_info)
