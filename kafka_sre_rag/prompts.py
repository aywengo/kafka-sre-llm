"""Single source of truth for every prompt the retrieval pipeline sends to a model.

Stdlib only. The text and whitespace here are frozen (measured in Phase 0); tests/golden/prompts_phase0.json
holds the hashes. Do not reword, and keep the em dash in pack_text.
"""

SYSTEM_PROMPT = (
    "You are an expert Site Reliability Engineer for Apache Kafka, Strimzi, Kubernetes, "
    "GitOps and data streaming platforms. Give precise, practical answers with commands or "
    "YAML where useful, and call out risks and version-specific behaviour. If you are not "
    "sure about an exact config key or flag, say so."
)

EXPAND_SYSTEM = """You help search technical documentation (Apache Kafka 4.3, Strimzi, Kubernetes, Argo CD). Given an on-call engineer's question, list the exact terms the relevant documentation pages would contain: configuration keys, metric and log names, CLI tools with flags, Kubernetes/Strimzi/Argo CD resource kinds and fields, error and exception names, and the names of the concepts involved. Output one comma-separated line of 15 to 30 terms, most important first. No sentences, no explanations."""

INSTRUCTION = "Use these documentation excerpts where relevant."
STRICT = ("Use only commands, flags, configuration keys, resource fields and annotations that appear in the "
          "excerpts. If the excerpts do not cover part of the question, say so in one sentence instead of "
          "filling the gap.")
THOROUGH = ("Answer thoroughly: give every likely cause the situation allows, how to check each one "
            "(exact command, metric or field) and the fix, then what to do if none of them fit.")


def pack_text(chunks):
    """chunks: dicts with source, title, path, text, best first."""
    parts = []
    for c in chunks:
        parts.append(f"{c['source']} \u2014 {c['title'] or c['path'].rsplit('/', 1)[-1]}\n{c['text']}")
    return "\n\n".join(parts)


def user_message(question, chunks=None, *, strict=False, thorough=False):
    """chunks None = closed-book (the bare question). strict only applies with chunks, as in Phase 0."""
    user = question
    if chunks is not None:
        user = (INSTRUCTION + (" " + STRICT if strict else "") + "\n\n" + pack_text(chunks) + f"\n\nQuestion: {question}")
    if thorough:
        user += "\n\n" + THOROUGH
    return user


def answer_messages(question, chunks, *, strict=False, thorough=False):
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message(question, chunks, strict=strict, thorough=thorough)}]


def expand_messages(question):
    return [{"role": "system", "content": EXPAND_SYSTEM}, {"role": "user", "content": question}]
