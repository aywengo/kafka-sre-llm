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


def user_message(question, chunks=None, *, strict=False, thorough=False, rules=False, cards_text=None):
    """chunks None = closed-book (the bare question). strict and rules only apply with chunks.
    cards_text (M7 KA1, EXPERIMENTAL): the rendered lookup cards, one block after the excerpts; None or "" = nothing."""
    user = question
    if chunks is not None:
        user = (INSTRUCTION + (" " + STRICT if strict else "") + (" " + RULES if rules else "")
                + "\n\n" + pack_text(chunks) + ("\n\n" + cards_text if cards_text else "")
                + f"\n\nQuestion: {question}")
    if thorough:
        user += "\n\n" + THOROUGH
    return user


def answer_messages(question, chunks, *, strict=False, thorough=False, rules=False, cards_text=None):
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message(question, chunks, strict=strict, thorough=thorough, rules=rules,
                                                     cards_text=cards_text)}]


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


def grounded_user_message(question, chunks, variant, cards_text=None):
    """INSTRUCTION, the pack, (the lookup cards,) the question, then the grounded text. Needs excerpts (no closed-book form)."""
    if chunks is None:
        raise ValueError("a grounded mode needs excerpts; it has no closed-book form")
    if variant not in GROUNDED:
        raise ValueError(f"variant must be one of {sorted(GROUNDED)}, got {variant!r}")
    return (INSTRUCTION + "\n\n" + pack_text(chunks) + ("\n\n" + cards_text if cards_text else "")
            + f"\n\nQuestion: {question}" + "\n\n" + GROUNDED[variant])


def grounded_answer_messages(question, chunks, variant, cards_text=None):
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": grounded_user_message(question, chunks, variant, cards_text)}]


# M7 P1 (EXPERIMENTAL, unmeasured): the checking pass of the thorough profile (kafka_sre_rag/verify_pass.py). Added after
# the prompts above; nothing above it changed. Text verbatim from the design runs/m7/reports/k1_check_design.md section 2.
# Placeholders are filled with one re.sub: <<QUESTION>>, <<EXCERPTS>> (pack_text), <<ANSWER>> (revise.numbered),
# <<FINDINGS>> (one JSON object per flagged finding, as revise.build_messages lays them out) and, in VERIFY_PROMPT_CARDS
# only, <<CARDS>> (kb_lookup.render of the draft's cards). The hashes are in tests/golden/prompts_verify.json; these
# constants are not in frozen.json (they are no part of the measured configuration). No text of any evaluation item.
VERIFY_SYSTEM = (
    "You check a draft answer to an operations question about Apache Kafka, Strimzi, Kubernetes or Argo CD against the "
    "documentation excerpts it was written from. You do not answer the question and you do not improve wording, order or "
    "completeness. You only correct lines that the excerpts or the catalogue findings show to be wrong.")

_VERIFY_HEAD = (
    "Question:\n\n<<QUESTION>>\n\n"
    "Documentation excerpts:\n\n<<EXCERPTS>>\n\n"
    "Draft answer, one numbered line per line:\n\n<<ANSWER>>\n\n"
    "Findings of a mechanical check against catalogues of what exists (one JSON object per line; empty if none):\n\n"
    "<<FINDINGS>>\n\n")
_VERIFY_CASES = "Name a line of the draft only in one of two cases.\n"
_VERIFY_CASE1 = (
    "1. Contradicted: a sentence in the excerpts cannot be true together with what the line states (a different default, "
    "name, value or field for the same purpose, or the opposite behaviour). Copy the deciding words of that sentence "
    "exactly into \"quote\". A line the excerpts do not mention is not contradicted, even if you believe it is wrong.\n")
_VERIFY_CASE1_CARDS = (
    "1. Contradicted: a sentence in the excerpts or a fact on a reference card cannot be true together with what the line "
    "states (a different default, name, value or field for the same purpose, or the opposite behaviour). Copy the deciding "
    "words of that sentence or fact exactly into \"quote\", from the excerpt or the card. A line that neither the "
    "excerpts nor a reference card mention is not contradicted, even if you believe it is wrong.\n")
_VERIFY_TAIL = (
    "2. Named by a finding: the line holds the item a finding names. Copy the deciding words of the finding's evidence or "
    "of an excerpt exactly into \"quote\".\n"
    "For each line you name, give in \"text\" the line as it should read. Change only the words that are wrong; keep the "
    "rest of the line, its indentation, list or comment marker, code formatting and trailing backslash. Take every new "
    "name, value or number from the quote. Do not add a step, command, flag, warning or sentence. Do not remove a flag, "
    "option, namespace or warning from a command unless that exact item is what is wrong. Never change a heading, a line "
    "that repeats the user's own configuration or output from the question, or a line that begins with \"General "
    "knowledge (not in the excerpts):\". If the wrong item stands on several lines, name each of them. Give \"text\": "
    "null only when the whole line is contradicted and the excerpts give no correct form; never delete part of a "
    "command.\n"
    "Name at most 6 lines, most important first. Most answers need no change; then reply with an empty list.\n\n"
    "Reply with JSON only, one object and nothing else (no code fence, no text before or after it), in exactly this form: "
    "{\"replacements\": [{\"line\": 12, \"quote\": \"words copied from an excerpt or a finding\", \"text\": \"the "
    "corrected line 12\"}]}")

VERIFY_PROMPT = _VERIFY_HEAD + _VERIFY_CASES + _VERIFY_CASE1 + _VERIFY_TAIL
VERIFY_PROMPT_CARDS = (
    _VERIFY_HEAD
    + "Reference cards from the pinned catalogues for names the draft uses (exact facts; empty if none):\n\n<<CARDS>>\n\n"
    + _VERIFY_CASES + _VERIFY_CASE1_CARDS + _VERIFY_TAIL)
del _VERIFY_HEAD, _VERIFY_CASES, _VERIFY_CASE1, _VERIFY_CASE1_CARDS, _VERIFY_TAIL


# M7 KA1 (EXPERIMENTAL, unmeasured, off by default): the lookup cards of an answer prompt (kafka_sre_rag/kb_lookup.py,
# answer_block). One block after the excerpts, under this heading; the cards are catalogue facts with their pin. The block is
# left out when there is no card. The hash of this constant is in tests/test_lookup_cards.py; it is no part of
# the measured configuration (not in frozen.json). No text of any evaluation item.
LOOKUP_CARDS_HEADER = (
    "REFERENCE CARDS from the pinned catalogues (exact names, defaults and values; these are not documentation "
    "excerpts). Use them for the exact name, default or value of what they list. A name without a card is not "
    "thereby wrong: the catalogues can be incomplete.")

# M7 KB phase 2 (EXPERIMENTAL, unmeasured): the fact entries (runs/m7/reports/kb2_design.md sections 2 to 5). Added after
# the prompts above; nothing above it changed. Writer (scripts/kb_write_entries.py, DeepSeek), checker
# (scripts/kb_check_entries.py, GLM) and the rendering of an entry in a pack (kafka_sre_rag/kb_entry.render). Placeholders
# are filled with one re.sub: <<WINDOW>> (kb_entry.window_text), <<TARGET>> (the target chunk id), <<ENTRIES>> (one JSON
# object per entry). Every output row carries its version; a changed text needs a new version and a new sanity run. These
# constants are not in frozen.json and no golden hash covers them (they are no part of the measured configuration). They
# are written for and tried only on the slice's sanity chunks; no text of any evaluation item.
KB_ENTRY_PROMPT_VERSION = "kbe/4"
KB_CHECK_PROMPT_VERSION = "kbc/3"

KB_WRITER_SYSTEM = (
    "You turn pages of the Strimzi and Apache Kafka documentation into short, exact fact entries for operators. You "
    "state only what the given text says, in its own terms. You never add knowledge from memory, never infer beyond "
    "the text, never calculate, and never invent an example, value, flag, field or name.")

KB_WRITER_USER = (
    "Below are consecutive chunks of one documentation page, each under a header line with its id. The chunk marked "
    "TARGET is the one you write entries for; the others are its neighbours on the same page and are there only so "
    "that a fact split over a chunk boundary can be written once.\n\n"
    "<<WINDOW>>\n\n"
    "Write 0 to 12 entries for the target chunk <<TARGET>>. Prefer facts an operator needs: what a setting does, "
    "defaults and limits, valid values and units, procedures and the order of their steps, warnings, preconditions and "
    "\"do not\" rules, compatibility and versions. Write no entry for a table of contents, boilerplate, a list of links "
    "or text that states no fact; then reply with an empty list.\n\n"
    "Rules for every entry:\n"
    "- \"kind\": one of \"fact\", \"value\" (a default, limit, unit or set of valid values), \"procedure\" (the ordered "
    "steps of one section), \"warning\" (a risk, a precondition or a \"do not\"), \"cross\" (a fact joined from two or "
    "three of the chunks shown).\n"
    "- \"title\": at most 100 characters; names the subject (resource kind, setting, tool or operation).\n"
    "- \"statement\": at most 400 characters, one to three sentences, self-contained: name the product, resource, field "
    "or tool instead of writing \"it\". Keep every condition, negation, version and unit the text gives. Write every "
    "number, setting, field, command, flag and other name exactly as the text writes it, with the same characters.\n"
    "- \"applies_when\": optional, at most 200 characters: the condition or mode the text ties the fact to; leave it "
    "out when the text ties it to none.\n"
    "- \"sources\": the ids of the 1 to 3 chunks that state the entry; the target chunk <<TARGET>> is always one of "
    "them. A \"cross\" entry has two or three.\n"
    "- \"quotes\": 1 to 4 objects {\"chunk\": id, \"text\": span}; each span is copied character for character from "
    "that chunk as shown (its first line or its text); every source has at least one quote, "
    "and at least one quote is from the target chunk. Keep each quote under 250 characters: quote one sentence or one "
    "table row per quote. When the entry's subject is named by a heading line inside the chunk, quote that heading line as well. Every claim of the statement and of \"applies_when\" is stated "
    "by the quotes, and every number and name in them and in the title appears in a quote.\n"
    "- A \"cross\" entry only when every part of its statement is quoted from the chunk that states it.\n"
    "- Count characters. A quote over 300 characters is rejected: use two shorter quotes from the same chunk. A statement "
    "over 400 characters is rejected: write two entries.\n"
    "- When a heading or an earlier sentence limits where a statement applies (a version, a feature, a component, a mode), "
    "carry that condition into the statement or its applies-when field; a statement true only under a condition must name it.\n"
    "- The heading trail of the entry (the page title and the headings above the quote, and a table's caption) is added to "
    "the entry automatically; do not repeat it. The statement must still name every condition stated in the sentence's "
    "own paragraph or in the immediately preceding sentence (\"only at debug level\", \"if the Topic Operator is not "
    "running\", \"only when the CA type is cert-manager\"), because the trail does not carry those.\n"
    "- For a table row, quote the whole row from its first cell, so the name and its value are in one quote.\n"
    "- Write each name exactly as a quote or the chunk's title line writes it: same case, no added class, prefix, path or "
    "plural. If neither contains it, leave it out. Do not make up hyphenated words.\n"
    "- File each quote under the id of the chunk it comes from, and list that chunk in sources.\n\n"
    "Reply with JSON only, one object and nothing else (no code fence, no text before or after it), in exactly this "
    "form: {\"entries\": [{\"kind\": \"value\", \"title\": \"...\", \"statement\": \"...\", \"applies_when\": \"...\", "
    "\"sources\": [\"<<TARGET>>\"], \"quotes\": [{\"chunk\": \"<<TARGET>>\", \"text\": \"...\"}]}]}")

KB_CHECK_SYSTEM = (
    "You check short fact entries against the documentation chunks they quote. You judge only whether each entry says "
    "what its quoted text says. You do not rewrite entries and you do not use knowledge from memory.")

KB_CHECK_USER = (
    "Documentation chunks, each under a header line with its id:\n\n"
    "<<WINDOW>>\n\n"
    "Entries, one JSON object per line, each with its title, its statement, the condition it applies to (if any), its "
    "heading trail (the page title and the headings above the quote, added from the chunk by code; it is part of the entry "
    "and is shown with it to the reader) and its quotes:"
    "\n\n<<ENTRIES>>\n\n"
    "For each entry give one verdict:\n"
    "- \"ok\" only when all three hold: (1) every claim of the title, of the statement and of \"applies_when\" is "
    "stated by the quoted spans; (2) nothing is added: no extra claim, no stronger or weaker wording, no dropped condition, negation, "
    "version or scope; (3) read in its chunk, each quote means what the statement says.\n"
    "- \"partial\": part of it is supported, or something is added or dropped; so is an entry when a condition from the "
    "chunk (a heading, the preceding sentence, a note or warning nearby) is neither in the statement (or applies_when) nor "
    "in the heading trail.\n"
    "- \"wrong\": the chunks contradict it.\n"
    "- \"unsupported\": the quotes do not state it.\n"
    "Give one short reason per entry.\n\n"
    "Reply with JSON only, one object and nothing else (no code fence, no text before or after it), with one row per "
    "entry id, in exactly this form: {\"verdicts\": [{\"id\": \"kbe-...\", \"verdict\": \"ok\", \"reason\": \"...\"}]}")

# The rendered form of an entry in a pack (kb_entry.render): an item for pack_text, whose first line is
# "<source> — <title line>". <<TITLE>>, <<PRODUCT>>, <<VERSION>>, <<PATHS>>, <<STATEMENT>>, <<APPLIES>> (empty, or the
# applies line), <<CONTEXT>> (kbe/4: empty, or the heading-trail line) and <<QUOTES>> (one quote line per quote) are filled with one re.sub. At most 1,500 characters in all.
KB_ENTRY_RENDER = {
    "title": "Fact entry: <<TITLE>> (<<PRODUCT>> <<VERSION>>; <<PATHS>>)",
    "text": "<<STATEMENT>><<APPLIES>><<CONTEXT>>\nSource text:\n<<QUOTES>>",
    "applies": "\nApplies when: <<APPLIES_WHEN>>",
    "context": "\nContext: <<TRAIL>>",
    "quote": "- \"<<QUOTE>>\"",
}


def kb_writer_messages(window, target):
    import re
    fill = {"WINDOW": window, "TARGET": target}
    user = re.sub(r"<<(WINDOW|TARGET)>>", lambda m: fill[m.group(1)], KB_WRITER_USER)
    return [{"role": "system", "content": KB_WRITER_SYSTEM}, {"role": "user", "content": user}]


def kb_check_messages(window, entries):
    import re
    fill = {"WINDOW": window, "ENTRIES": entries}
    user = re.sub(r"<<(WINDOW|ENTRIES)>>", lambda m: fill[m.group(1)], KB_CHECK_USER)
    return [{"role": "system", "content": KB_CHECK_SYSTEM}, {"role": "user", "content": user}]
