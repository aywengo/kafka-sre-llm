"""ask(): expansion -> retrieval -> answer, with the measured flag."""
import time
from dataclasses import dataclass, field

from . import config, prompts


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


def is_reference(gen, cfg):
    ref = cfg["reference_generator"]
    ident = getattr(gen, "identity", None) or {}
    if ident.get("backend") != ref["backend"] or ident.get("adapter"):
        return False
    model = str(ident.get("model") or "")
    return ref["snapshot"] in model or model.rstrip("/").endswith(ref["local_wrapper"].rsplit("/", 1)[-1])


def ask(question, gen, cfg=None, *, k="k6", mode="default", index_path=None, on_token=None, rerank=True):
    cfg = cfg or config.load()
    if mode not in ("default", "strict"):
        raise ValueError(f"mode must be 'default' or 'strict', got {mode!r}")
    from . import retrieval  # lazy: pulls in scripts/retrieve.py
    reasons = []
    if mode != "default":
        reasons.append(f"mode {mode}")
    if k != "k6":
        reasons.append(f"k {k}")
    if not is_reference(gen, cfg):
        reasons.append("generator is not the reference model")
    if index_path and str(index_path) != str(config.index_file(cfg)):
        reasons.append("index override")
    if not rerank:
        reasons.append("no reranker")
    t0 = time.monotonic()
    exp, _, _ = gen.chat(prompts.expand_messages(question), cfg["expansion"]["max_tokens"])
    t1 = time.monotonic()
    chunks = retrieval.select(question, exp, k=k, index_path=index_path, rerank=rerank)
    t2 = time.monotonic()
    msgs = prompts.answer_messages(question, chunks, strict=(mode == "strict"))
    text, toks, trunc = gen.chat(msgs, cfg["answer"]["max_tokens"], on_token)
    t3 = time.monotonic()
    sources = [{"source": c["source"], "title": c["title"], "path": c["path"],
                "bm25_rank": c.get("bm25_rank"), "ce_score": c.get("ce_score")} for c in chunks]
    return Answer(text, sources, exp, {"expand_s": t1 - t0, "retrieve_s": t2 - t1, "answer_s": t3 - t2},
                  not reasons, toks, trunc, reasons, msgs)
