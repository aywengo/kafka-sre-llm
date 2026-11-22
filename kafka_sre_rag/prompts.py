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
# JSON object per finding: line, lines, item, problem, evidence, suggestion). It carries no text of any evaluation item.
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
    "lines with a trailing backslash. An item may stand on several lines (the finding lists them in \"lines\"): "
    "correct or remove every one of them. A name in a finding's \"nearest real\" list is a replacement only where "
    "the excerpts support that name in that place; otherwise remove the line or reword it without any name that is "
    "not in the excerpts. Do not change any line that no finding names.\n\n"
    "Reply with JSON only: your whole reply is one JSON object and nothing else (no text before or after it, no "
    "code fence), in exactly this form: "
    "{\"replacements\": [{\"line\": 12, \"text\": \"the new text of line 12\"}, {\"line\": 30, \"text\": null}]}. "
    "An empty list means nothing is to be changed.\n\n"
    "Documentation excerpts:\n\n<<EXCERPTS>>\n\n"
    "Answer with numbered lines:\n\n<<ANSWER>>\n\n"
    "Findings, one JSON object per line:\n\n<<FINDINGS>>\n"
)


# M5 G1 (EXPERIMENTAL): the grounded modes. Added after the measured prompts above; nothing above it changed.
# The text goes after the question (INSTRUCTION, the pack, the question, then GROUNDED_Gx). The two variants differ
# in one rule: what happens when the excerpts lack a fact (G1: not from memory, except a single name, value or
# verdict; G2: labelled general-knowledge lines). The sha256 of each constant is in frozen.json (prompts_sha256).
# The gap sentence contains the literal characters <what is missing>.
GROUNDED_G1 = (
    "Start with the direct answer in one to three sentences. "
    "Every command, flag, configuration key, field, annotation, metric and error name you write must appear in the "
    "excerpts or in the question, written as it appears there, and so must every statement of what a setting, command "
    "or component does and what its default is. "
    "An excerpt counts when it states the fact in other words or shows it in an example. "
    "If the excerpts do not cover something the question needs, answer what they do cover and add one sentence in "
    "exactly this form: \"The retrieved documentation does not cover <what is missing>.\" "
    "Do not fill that gap from memory, with one exception: if the question asks for a single name, value or verdict "
    "that the excerpts do not give, still give your best answer after that sentence, in the form the question asks for. "
    "Never say that something does not exist or is not supported only because the excerpts do not mention it. "
    "No background, no options or alternatives the question did not ask for, no generic advice, no summary at the end. "
    "Keep the answer under about 600 words, code included.")

GROUNDED_G2 = (
    "Start with the direct answer in one to three sentences. "
    "Base every statement on the excerpts or the question, and write every command, flag, configuration key, field, "
    "annotation, metric and error name as it appears there. "
    "An excerpt counts when it states the fact in other words or shows it in an example. "
    "If the excerpts do not cover something the question needs, answer what they do cover and add one sentence in "
    "exactly this form: \"The retrieved documentation does not cover <what is missing>.\" "
    "After that sentence you may add what you know from general knowledge, on lines that begin with "
    "\"General knowledge (not in the excerpts):\"; name in them only things you are sure exist. "
    "Never say that something does not exist or is not supported only because the excerpts do not mention it. "
    "No background, no options or alternatives the question did not ask for, no generic advice, no summary at the end. "
    "Keep the answer under about 600 words, code included.")

GROUNDED = {"grounded-g1": GROUNDED_G1, "grounded-g2": GROUNDED_G2}

# G3 (EXPERIMENTAL), based on G2; text from runs/m5/reports/g_dev_analysis.md section 4. Registered below, after GROUNDED.
GROUNDED_G3 = (
    "Start with the direct answer in one to three sentences. When the question asks what is wrong, name the cause that "
    "fits all the evidence the question gives, not only a cause one excerpt happens to describe. "
    "Base every statement on the excerpts or the question, and write every command, flag, configuration key, field, "
    "annotation, metric and error name as it appears there. "
    "An excerpt counts when it states the fact in other words or shows it in an example. "
    "Answer every part of the question: when it asks what is wrong, give the read-only checks that confirm the cause; "
    "when it asks for a change, give the change and how to verify it; in a review, name every problem you find, not only the first. "
    "If the excerpts do not cover something the question needs, answer what they do cover and add one sentence in "
    "exactly this form: \"The retrieved documentation does not cover <what is missing>.\" "
    "After that sentence you may add what you know from general knowledge, on lines that begin with "
    "\"General knowledge (not in the excerpts):\"; name in them only things you are sure exist. "
    "Never say that something does not exist or is not supported only because the excerpts do not mention it. "
    "No background, no generic advice, no summary at the end; offer an alternative only when the question asks for one "
    "or the evidence leaves the cause open. "
    "Keep the answer under about 700 words, code included.")
GROUNDED["grounded-g3"] = GROUNDED_G3


def grounded_user_message(question, chunks, variant):
    """INSTRUCTION, the pack, the question, then the grounded text. Needs excerpts (no closed-book form)."""
    if chunks is None:
        raise ValueError("a grounded mode needs excerpts; it has no closed-book form")
    if variant not in GROUNDED:
        raise ValueError(f"variant must be one of {sorted(GROUNDED)}, got {variant!r}")
    return (INSTRUCTION + "\n\n" + pack_text(chunks) + f"\n\nQuestion: {question}" + "\n\n" + GROUNDED[variant])


def grounded_answer_messages(question, chunks, variant):
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": grounded_user_message(question, chunks, variant)}]
