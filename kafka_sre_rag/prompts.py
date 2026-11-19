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


# The teacher's rules for pack answers (scripts/generate_qa.py, prompt version pack/4), addressed to the
# assistant itself. Used only by the "prompted student" probe of the stage-1 ceiling check: does the base
# model, given these rules as instructions, already write what an adapter would be trained to write?
RULES = (
    "Follow these rules. Every fact in your answer must be supported by the excerpts. Every command, flag, "
    "configuration key, metric name, resource kind, field path, annotation, endpoint and error string must "
    "appear in the excerpts, written exactly as it appears there; never complete one from memory. Be complete: "
    "use every cause, check, step and caveat that the excerpts support and that bears on the question, and "
    "nothing they do not support. Start with one or two sentences that answer the question directly. For a "
    "symptom, number the likely causes the excerpts name, most likely first, each with a **Check** (the exact "
    "command, metric or field) and a **Fix**; number only causes that can produce the symptom. For a how-to, "
    "give the steps in order with the exact commands or YAML. For configuration and concept questions, give "
    "the facts and say where each setting lives. No generic sections, no recap, no closing summary. Ignore "
    "excerpts that are not about the question, and do not combine settings or steps of different products "
    "unless the excerpts connect them. If the excerpts contradict the question's premise, correct it first; "
    "something the excerpts do not mention is a gap, not an error: never say that it does not exist or is not "
    "supported. If the excerpts do not cover the question or part of it, answer what they do cover, then add "
    "one sentence in exactly this form: \"The retrieved documentation does not cover <what is missing>.\" "
    "After that sentence you may add at most two sentences starting with \"In general,\" that give direction "
    "only and name no command, flag, key, metric, field or tool. Apart from that one sentence, do not mention "
    "the excerpts or the documentation. Keep the answer under about 1,200 words.")


def pack_text(chunks):
    """chunks: dicts with source, title, path, text, best first."""
    parts = []
    for c in chunks:
        parts.append(f"{c['source']} \u2014 {c['title'] or c['path'].rsplit('/', 1)[-1]}\n{c['text']}")
    return "\n\n".join(parts)


def user_message(question, chunks=None, *, strict=False, thorough=False, rules=False):
    """chunks None = closed-book (the bare question). strict and rules only apply with chunks."""
    user = question
    if chunks is not None:
        user = (INSTRUCTION + (" " + STRICT if strict else "") + (" " + RULES if rules else "")
                + "\n\n" + pack_text(chunks) + f"\n\nQuestion: {question}")
    if thorough:
        user += "\n\n" + THOROUGH
    return user


def answer_messages(question, chunks, *, strict=False, thorough=False, rules=False):
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message(question, chunks, strict=strict, thorough=thorough, rules=rules)}]


def expand_messages(question):
    return [{"role": "system", "content": EXPAND_SYSTEM}, {"role": "user", "content": question}]


# WP2b R2 (kafka_sre_rag/revise.py): the one extra call that replaces the lines of an answer the catalogue check
# flagged. Added after the measured prompts above; nothing above it changed. Placeholders are filled with str.replace:
# <<EXCERPTS>> (pack_text of the excerpts), <<ANSWER>> (the answer, one numbered line per line) and <<FINDINGS>> (one
# JSON object per finding: line, item, problem, evidence, suggestion). It carries no text of any evaluation item.
REVISE_PROMPT = (
    "Below are documentation excerpts, an answer written from them with numbered lines, and findings from a check "
    "of that answer against catalogues of what exists in Apache Kafka, Strimzi, Kubernetes and Argo CD. Each finding "
    "names a line and an item on it (a flag, configuration key, field, apiVersion, value, tool or error name) that "
    "the catalogues contradict, with the evidence from the catalogue.\n\n"
    "Correct only the lines the findings name. For each such line give the line as it should read, using only the "
    "real names in the evidence or in the excerpts (the valid options of the tool, the served apiVersion, the "
    "values the key accepts, the scope the key belongs to). If no correct form is known from the evidence or the "
    "excerpts, delete the line (text null) instead of guessing. Do not add any command, flag, key, field or name "
    "that is in neither the evidence nor the excerpts. Keep the indentation, the comment marker and a trailing "
    "backslash of the line. Give more than one line in one replacement only when the command is wrapped over "
    "lines with a trailing backslash. If a finding is wrong (the item is correct as written), leave its line out. "
    "Do not change any line that no finding names.\n\n"
    "Reply with JSON only, no other text, in exactly this form: "
    "{\"replacements\": [{\"line\": 12, \"text\": \"the new text of line 12\"}, {\"line\": 30, \"text\": null}]}. "
    "An empty list means nothing is to be changed.\n\n"
    "Documentation excerpts:\n\n<<EXCERPTS>>\n\n"
    "Answer with numbered lines:\n\n<<ANSWER>>\n\n"
    "Findings, one JSON object per line:\n\n<<FINDINGS>>\n"
)
