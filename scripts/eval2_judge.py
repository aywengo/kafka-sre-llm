#!/usr/bin/env python3
"""Tier B judge step of the new evaluation (agents/m4-evaluation.md, "Tier B: judged" and "Rubric pilot").

One call to the judge per answer and pass. The judge gets the instruction of the design, the question, any
evidence, the rubric points with their ids and the item's source passages. It never gets a reference answer.
The reply is validated (every point has a verdict; a "present" or "contradicted" verdict needs a quote that
occurs in the answer after whitespace normalisation, otherwise it becomes "absent" and is counted as a judge
error) and turned into the item score of the design.

  scripts/eval2_judge.py run --rubrics eval/m4/pilot/rubrics.jsonl \
      --qmap real=eval/questions_real.jsonl holdout=eval/questions_runbooks_holdout.jsonl \
      --chunks work/v3/chunks_s1.jsonl \
      --answers real_default=work/v3/ans_rag_real_s1.jsonl holdout_default=... \
      --cache work/m4/pilot/judge_cache.jsonl --out work/m4/pilot/judge_results.jsonl \
      --judge-url "$OMLX_URL/v1" --judge-model GLM-5.3-Flash-oQ8e-mtp --extra-body '{"max_tokens": 32768}'
  scripts/eval2_judge.py run ... --stub            # dry run: no server, stub replies, prints counts and an estimate
  scripts/eval2_judge.py run ... --rescore         # no calls: rebuild --out from --cache
  scripts/eval2_judge.py gen-generic --rubrics ... --qmap ... --out eval/m4/pilot/generic_answers.jsonl

Data formats (so that tier B items of the new evaluation use the same code):
  rubric row   {"id", "question" (optional if the id is "<prefix>-<n>" and --qmap has the prefix), "evidence" (optional),
                "must": [{"id","text","weight","first"}], "must_not": [{"id","text","severity"}], "any_of": [[ids]],
                "covered_chunks": [chunk ids] and/or "source_text": str or [str]}
  answer file  JSONL rows with "question" and "text"; rows are matched to rubrics by the question text
               (whitespace-normalised). An answer set is given as NAME=PATH.
  chunks file  JSONL rows with "id" and "text" (--chunks); optional, only for rubrics that give chunk ids.

Scoring (per answer and pass), exactly as the design states:
  P = (weights of must points present) / (all must weights); W = must-not "wrong" present + must "contradicted";
  H = must-not "harmful" present; S = max(-1, P - 0.25 W - 0.5 H); S with the W penalty 0 and 0.5 as well.
  An any_of group counts as ONE point: weight = the highest weight among its members, present if any member is
  present. Groups that share a member are merged. A contradicted member still counts in W.
  Essential miss: a weight-2 must point is absent and no member of its any_of group is present.

Judge call: OpenAI-compatible chat completions; URL and key from --judge-url / OMLX_URL and JUDGE_API_KEY /
OMLX_API_KEY / ~/.config/kafka-sre-llm/. Busy statuses (429, 503, 507) are waited out as in scripts/generate_qa.py.
Neither URL nor key is ever printed.

Version 2 of the judge step (runs/m4/reports/s5_pilot_analysis.md, design changes 1 to 3; runs/m4/reports/s5b_judge_v2.md):
  --instruction v1|v2   instruction text (default v2; v1 reproduces the first pilot's prompts and cache keys exactly)
  quote matching        fair for both versions: markdown emphasis marks, backticks, code-fence tags and list markers are removed from
                        quote and text, whitespace is normalised; a quote with "..." (or the ellipsis character) is accepted if each
                        fragment has at least 12 characters and the fragments occur in order
  ANSWER EXCERPTS       v2 only: the chunks of the answer's own pack, looked up in --chunks (the frozen index). The pack comes from the
                        answer row ("pack": chunk ids, as the harness writes it) or from --packs NAME=PATH (JSONL with "question" and
                        "pack" or "k6"). Answers are matched to rubrics by question text, or by id when the row has no question.
  source_quote          v2: an other-errors entry needs a "source_quote" found in SOURCE PASSAGES or ANSWER EXCERPTS; others are dropped
  --with-reference      adds the rubric's "reference" answer to the judge's input (an arm of its own; off by default)
  flags                 write the candidate errors of a results file to a review file (columns confirmed, note left empty)
  gen-probes            second pilot: the probe answer sets (generic, correct_alternative, reference, near-miss) and their rubrics file
  confirmed             read a filled review file back: confirmed wrong and harmful items per 10 answers per answer set

Coverage role (decision J1 of MILESTONE.md; the judge reports rubric coverage only, its other-errors output is never counted):
  run --coverage        a present/contradicted verdict whose quote is not found becomes "unverified" (not present, not a miss); rows carry
                        the answer's pack; cache keys are unchanged, so `run --rescore --coverage` reuses a cache
  supported             rubrics that carry chunk ids (a must point with "chunks" or "not_indexed"): a point is supported if and only if one of its
                        "chunks" is in the answer's pack; an any_of group if any member is; a point without "chunks" never. The file-level rule
                        remains only for rubrics without chunk ids (the two pilots); a report that uses it says so
  essential-miss unit   one per weight-2 point, an any_of group counting once (essential_units, coverage_row, coverage_measures)
  chunks-for --chunks IDX [--root .] "raw/x.md:120-130; raw/y.html:40"
                        ids of the index chunks whose text contains those lines (word 4-grams, so whitespace and markup do not matter; a chunk
                        counts with at least 30 % of a cited range); "not in the index" with the best partial match when none does
  fill-chunks --rubrics R --chunks IDX --out R2 [--check] [--overwrite]
                        fills "chunks" of every must point from its "source" (a point that places nothing gets "not_indexed": true) and lists
                        the points it could not place; --check only verifies recorded ids and exits 1 on a problem
"""
import argparse
import hashlib
import json
import math
import os
import re
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

JUDGE_INSTRUCTION_V1 = """You check one answer from an assistant for Kafka, Strimzi, Kubernetes and Argo CD operators against a list of points. Judge each point on its own. Do not grade style, length or tone.
For each MUST point, the verdict is "present" only if the answer states the point explicitly and specifically enough that an engineer following the answer would act on it correctly. A setting, flag, field, annotation, metric, API path or HTTP method counts only if it is named as in the point. A vague mention ("check the configuration", "look at the logs") is "absent". If the answer states the point and contradicts it elsewhere, the verdict is "contradicted". If the point says it must be the main or first recommendation and the answer gives it only as one option among several, the verdict is "absent".
For each MUST-NOT point, the verdict is "present" if the answer states or recommends it, even in passing or as an alternative, and "absent" otherwise. A statement the answer explicitly rejects is "absent".
For every "present" or "contradicted" verdict, quote the shortest passage of the answer that shows it. If you cannot quote it, the verdict is "absent".
Then list any other statement in the answer that contradicts the source passages below, with a quote. Do not list statements that are only missing from the sources.
Reply only with JSON: {"must":[{"id":"","verdict":"present|absent|contradicted","quote":""}],"must_not":[{"id":"","verdict":"present|absent","quote":""}],"other_errors":[{"quote":"","why":""}]}"""

JUDGE_INSTRUCTION = JUDGE_INSTRUCTION_V1   # version 1, as measured in the first pilot

# Version 2: the text of version 1 with exactly three changes (s5_pilot_analysis.md, "Changes to the design text", 3).
_V1_MUST_END = 'the verdict is "absent".\nFor each MUST-NOT point'
_V1_OTHER = ('Then list any other statement in the answer that contradicts the source passages below, with a quote. '
             'Do not list statements that are only missing from the sources.')
_V2_OTHER = ('List another statement only if a sentence in SOURCE PASSAGES or ANSWER EXCERPTS contradicts it. '
             'Quote that sentence in "source_quote". Do not list statements that are absent from them.')
_V1_MUSTNOT = ('For each MUST-NOT point, the verdict is "present" if the answer states or recommends it, even in passing or as an alternative, '
               'and "absent" otherwise. A statement the answer explicitly rejects is "absent".')
_V2_MUSTNOT = ('For each MUST-NOT point, the verdict is "present" only if the answer states or recommends it and the point\'s "Applies" note, '
               'if it has one, holds for what the answer says; where a point has no "Applies" note, a statement made in passing or as an '
               'alternative counts. A statement the answer explicitly rejects, or mentions only to warn against, is "absent".')
_V1_FORMAT = '"other_errors":[{"quote":"","why":""}]'
_V2_FORMAT = '"other_errors":[{"quote":"","source_quote":"","why":""}]'
assert _V1_MUSTNOT in JUDGE_INSTRUCTION_V1 and _V1_MUST_END in JUDGE_INSTRUCTION_V1 and _V1_OTHER in JUDGE_INSTRUCTION_V1 and _V1_FORMAT in JUDGE_INSTRUCTION_V1
JUDGE_INSTRUCTION_V2 = (JUDGE_INSTRUCTION_V1
                        .replace(_V1_MUST_END, 'the verdict is "absent". A definition of a setting is not a recommendation to change it; '
                                 'a default value is not a change.\nFor each MUST-NOT point')
                        .replace(_V1_MUSTNOT, _V2_MUSTNOT).replace(_V1_OTHER, _V2_OTHER).replace(_V1_FORMAT, _V2_FORMAT))
INSTRUCTIONS = {"v1": JUDGE_INSTRUCTION_V1, "v2": JUDGE_INSTRUCTION_V2}
DEFAULT_INSTRUCTION = "v2"   # the command line; the functions below default to "v1" so that old callers are unchanged

# Without source passages (--source-chars 0) the judge cannot check the answer against them: the sentences about
# listing statements that contradict the source passages are replaced by an instruction to leave other_errors empty.
_SRC_SENTENCES = ('Then list any other statement in the answer that contradicts the source passages below, with a quote. '
                  'Do not list statements that are only missing from the sources.')
assert _SRC_SENTENCES in JUDGE_INSTRUCTION
JUDGE_INSTRUCTION_NO_SOURCES = JUDGE_INSTRUCTION.replace(_SRC_SENTENCES, 'Leave "other_errors" as an empty list.')
JUDGE_INSTRUCTION_V2_NO_SOURCES = JUDGE_INSTRUCTION_V2.replace(_V2_OTHER, 'Leave "other_errors" as an empty list.')

# Entries of the cache without a "setting" label were made under this setting (GLM oQ8e, thinking on, 32768 tokens, 12000
# characters of sources); its cache keys are the keys that were in use before settings had names.
DEFAULT_SETTING = "glm-think-12k"

BUSY_STATUS = (429, 503, 507)
BUSY_RETRIES, BUSY_WAIT_S = 20, 60
MUST_VERDICTS = ("present", "absent", "contradicted")
MUSTNOT_VERDICTS = ("present", "absent")
DEFAULT_SOURCE_CHARS = 12000
DEFAULT_EXCERPT_CHARS = 24000    # a k6 pack of the frozen index is 17 000 to 19 500 characters at most in the 82 old packs
MIN_FRAGMENT = 12                # characters of each fragment of a quote joined by "..."
SECONDS_PER_ITEM_DESIGN = 90      # design, "Cost of a run": 1.5 minutes per item with three in parallel
SECONDS_PER_CALL_V3_MEDIAN = 160  # v3: median per head-to-head call (sequential; divide by the concurrency)


def rd(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def norm_ws(s):
    return re.sub(r"\s+", " ", s or "").strip()


_LIST_MARK = re.compile(r"^[ \t]*(?:[-*+\u2022]|\d{1,3}[.)])[ \t]+", re.M)
_FENCE_TAG = re.compile(r"```[A-Za-z0-9_+.-]*")
_ELLIPSIS = re.compile(r"\.\.\.|\u2026")


def fair_norm(s):
    """Text for quote matching: list markers at the start of a line, code-fence tags, backticks and asterisks (emphasis) removed,
    whitespace normalised. Applied to the quote and to the text it is looked up in."""
    s = _LIST_MARK.sub("", s or "")
    s = _FENCE_TAG.sub("", s).replace("`", "").replace("*", "")
    return norm_ws(s)


def quote_found(quote, hay_norm):
    """Is `quote` in `hay_norm` (a fair_norm text)? A quote that occurs as it is, is found. A quote with "..." (or the ellipsis
    character) is found if every fragment has at least MIN_FRAGMENT characters and the fragments occur in order."""
    q = fair_norm(quote)
    if not q:
        return False
    if q in hay_norm:
        return True
    if not _ELLIPSIS.search(q):
        return False
    frags = [f for f in (p.strip(" []") for p in _ELLIPSIS.split(q)) if f]
    if not frags or any(len(f) < MIN_FRAGMENT for f in frags):
        return False
    pos = 0
    for f in frags:
        i = hay_norm.find(f, pos)
        if i < 0:
            return False
        pos = i + len(f)
    return True


def quotes_overlap(a, b):
    """Two quotes overlap if one contains the other, or share a common run of >= 25 characters or >= 60 % of the shorter
    (the rule of scripts/eval2_pilot_report.py, on fair_norm text)."""
    import difflib
    a, b = fair_norm(a).lower(), fair_norm(b).lower()
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    m = difflib.SequenceMatcher(None, a, b, autojunk=False).find_longest_match(0, len(a), 0, len(b))
    return m.size >= min(25, len(a), len(b)) or m.size >= 0.6 * min(len(a), len(b))


# ---------------------------------------------------------------- items

def load_qmap(specs):
    """['real=eval/questions_real.jsonl', ...] -> {'real': [question, ...]} (1-based positions)."""
    out = {}
    for spec in specs or []:
        name, _, path = spec.partition("=")
        out[name] = [r["question"] for r in rd(path)]
    return out


def rubric_question(rubric, qmap):
    if rubric.get("question"):
        return rubric["question"]
    m = re.fullmatch(r"(.+)-(\d+)", rubric["id"])
    if m and m.group(1) in qmap and 1 <= int(m.group(2)) <= len(qmap[m.group(1)]):
        return qmap[m.group(1)][int(m.group(2)) - 1]
    raise ValueError(f"rubric {rubric['id']}: no question text (no 'question' field and no --qmap entry)")


def source_text(rubric, chunks, cap):
    """Source passages of an item: 'covered_chunks' ids looked up in the chunk index, plus 'source_text'.
    Total length capped at `cap` characters: each passage gets an equal share of the cap, shares that a short
    passage does not use go to the longer ones, a cut passage ends with ' [...]'."""
    parts = []
    for cid in rubric.get("covered_chunks") or []:
        if cid not in chunks:
            raise ValueError(f"rubric {rubric['id']}: chunk {cid} not in the chunk file")
        c = chunks[cid]
        head = c.get("path") or c.get("title") or cid
        parts.append((f"[{cid}] {head}", c["text"].strip()))
    st = rubric.get("source_text")
    if st:
        for i, t in enumerate([st] if isinstance(st, str) else st):
            parts.append((f"[text {i + 1}]", t.strip()))
    return render_parts(parts, cap)


def render_parts(parts, cap):
    """[(head, text)] -> one string, capped at `cap` characters in total (shares as in source_text)."""
    if not parts:
        return ""
    budget = cap
    order = sorted(range(len(parts)), key=lambda i: len(parts[i][1]))
    share = {}
    left = len(parts)
    for i in order:
        s = max(budget // left, 0)
        share[i] = min(len(parts[i][1]), s)
        budget -= share[i]
        left -= 1
    out = []
    for i, (head, text) in enumerate(parts):
        t = text if share[i] >= len(text) else text[:max(share[i] - 6, 0)].rstrip() + " [...]"
        out.append(f"{head}\n{t}")
    return "\n\n".join(out)


def excerpt_text(ids, chunks, cap):
    """ANSWER EXCERPTS: the chunks of the answer's pack, in pack order, same layout and cap as the source passages."""
    parts = []
    for cid in ids:
        if cid not in chunks:
            raise ValueError(f"pack chunk {cid} not in the chunk file (the frozen index)")
        c = chunks[cid]
        parts.append((f"[{cid}] {c.get('path') or c.get('title') or cid}", c["text"].strip()))
    return render_parts(parts, cap)


def pack_ids_of(row):
    """Chunk ids of an answer row's pack (the harness writes 'pack': [ids]; a retrieval file has 'pack' or 'k6'), or None."""
    for k in ("pack", "k6"):
        v = row.get(k)
        if isinstance(v, list) and v:
            return [x["id"] if isinstance(x, dict) else x for x in v]
    return None


def prepare_items(items, chunks, cap, excerpt_cap=DEFAULT_EXCERPT_CHARS, instruction="v1"):
    """Fix, once per item, the texts the judge is shown and that quotes are checked against: item['source'] (cap > 0),
    item['excerpts'] (v2 and cap > 0; None without a pack) and item['excerpt_paths']. Values already set are kept."""
    for it in items:
        if cap > 0 and it.get("source") is None:
            it["source"] = source_text(it["rubric"], chunks, cap)
        ids = it.get("pack")
        if ids and "excerpt_paths" not in it:
            it["excerpt_paths"] = [canon_path(chunks[c]) for c in ids if c in chunks]
        if chunks and "index_paths" not in it:
            it["index_paths"] = index_paths_of(chunks)
        if instruction != "v1" and cap > 0 and it.get("excerpts") is None:
            it["excerpts"] = excerpt_text(ids, chunks, excerpt_cap) if ids else None
    return items


FIRST_NOTE = "(This must be the main or first recommendation, not one option among several.)"


def point_text(p, instruction="v1"):
    """Text of a point as the judge sees it. A point with first: true. v1 (first pilot, kept as it was): the note is added unless
    the text already contains 'first' or 'main'. Later versions: the note is added unless the text already says 'must be the main
    or first recommendation' (a cause point that begins 'Makes the main diagnosis' needs it). A must-not point that gives a
    'condition' shows it as '(Applies: <condition>)'; its 'near_miss' is never shown (it is a test of the rubric)."""
    t = p["text"].strip()
    if p.get("first"):
        low = t.lower()
        if (("first" not in low and "main" not in low) if instruction == "v1" else "must be the main or first recommendation" not in low):
            t += " " + FIRST_NOTE
    if (p.get("condition") or "").strip():
        t += f" (Applies: {p['condition'].strip()})"
    return t


EXCERPTS_LABEL = ("ANSWER EXCERPTS (the excerpts the answer was written from; a statement in the answer about what its excerpts "
                  "contain or lack is checked against these only):")


def build_prompt(item, pass_no, chunks, cap, instruction="v1", excerpt_cap=DEFAULT_EXCERPT_CHARS):
    """-> (system, user). Pass 2 lists the points (must and must-not) in reverse order.
    cap 0: no source passages at all, no SOURCE PASSAGES block, and the instruction without the sentences about them.
    instruction v2: an ANSWER EXCERPTS block follows the source passages ('(not available)' when the answer has no pack);
    item['reference'] (set only when the run asks for it) is added after it as REFERENCE ANSWER."""
    r = item["rubric"]
    must, must_not = list(r["must"]), list(r.get("must_not") or [])
    if pass_no == 2:
        must, must_not = must[::-1], must_not[::-1]
    lines = [f"QUESTION:\n{item['question'].strip()}"]
    if r.get("evidence"):
        lines.append(f"EVIDENCE (pasted by the operator):\n{r['evidence'].strip()}")
    lines.append("MUST points:\n" + "\n".join(f"- id {p['id']}: {point_text(p, instruction)}" for p in must))
    lines.append("MUST-NOT points:\n" + ("\n".join(f"- id {p['id']}: {point_text(p, instruction)}" for p in must_not) or "(none)"))
    if cap > 0:
        src = item.get("source")
        if src is None:
            src = source_text(r, chunks, cap)
        lines.append("SOURCE PASSAGES:\n" + (src or "(none)"))
        if instruction != "v1":
            exc = item.get("excerpts")
            if exc is None and item.get("pack"):
                exc = excerpt_text(item["pack"], chunks, excerpt_cap)
            lines.append(EXCERPTS_LABEL + "\n" + (exc or "(not available)"))
    if item.get("reference"):
        lines.append("REFERENCE ANSWER:\n" + item["reference"].strip())
    lines.append("ANSWER TO CHECK:\n<<<\n" + item["answer"].strip() + "\n>>>")
    if instruction == "v1":
        system = JUDGE_INSTRUCTION_V1 if cap > 0 else JUDGE_INSTRUCTION_NO_SOURCES
    else:
        system = JUDGE_INSTRUCTION_V2 if cap > 0 else JUDGE_INSTRUCTION_V2_NO_SOURCES
    return system, "\n\n".join(lines)


def match_items(rubrics, answer_sets, qmap, only=None, packs=None, with_reference=False):
    """Match the answers of each set to the rubrics by question text (a row without a question: by its id).
    -> (items, counts per set). only: optional collection of (set, id) pairs; the other answers are left out.
    Each item gets 'pack' (chunk ids of the answer's own excerpts, or None): from the answer row, else from
    packs[set] = {normalised question: ids}. with_reference: item['reference'] = the rubric's reference answer (required)."""
    items, counts = [], OrderedDict()
    for name, rows in answer_sets.items():
        by_q, by_id = {}, {}
        for row in rows:
            if row.get("question"):
                by_q.setdefault(norm_ws(row["question"]), row)
            elif row.get("id") is not None:
                by_id.setdefault(str(row["id"]), row)
        counts[name] = 0
        for r in rubrics:
            q = rubric_question(r, qmap)
            row = by_q.get(norm_ws(q)) or by_id.get(str(r["id"]))
            if row is None or not (row.get("text") or "").strip():
                continue
            if only is not None and (name, r["id"]) not in only:
                continue
            it = {"set": name, "id": r["id"], "question": q, "answer": row["text"], "rubric": r,
                  "pack": pack_ids_of(row) or (packs or {}).get(name, {}).get(norm_ws(q))}
            if with_reference:
                if not (r.get("reference") or "").strip():
                    raise ValueError(f"rubric {r['id']}: --with-reference but the rubric has no 'reference'")
                it["reference"] = r["reference"]
            items.append(it)
            counts[name] += 1
    return items, counts


# ---------------------------------------------------------------- scoring

def groups_of(rubric):
    """any_of groups with overlapping groups merged: list of sets of must ids."""
    groups = []
    for g in rubric.get("any_of") or []:
        g = set(g)
        for old in [x for x in groups if x & g]:
            g |= old
            groups.remove(old)
        groups.append(g)
    return groups


def score_item(rubric, must_v, mustnot_v):
    """must_v: {id: 'present'|'absent'|'contradicted'}; mustnot_v: {id: 'present'|'absent'}. -> dict of numbers."""
    weight = {p["id"]: p["weight"] for p in rubric["must"]}
    groups = groups_of(rubric)
    in_group = {i: g for g in groups for i in g if i in weight}
    units = []  # (weight, present)
    for p in rubric["must"]:
        if p["id"] not in in_group:
            units.append((p["weight"], must_v.get(p["id"]) == "present"))
    for g in groups:
        members = [i for i in g if i in weight]
        if members:
            units.append((max(weight[i] for i in members), any(must_v.get(i) == "present" for i in members)))
    total = sum(w for w, _ in units)
    P = sum(w for w, pres in units if pres) / total if total else 0.0
    sev = {p["id"]: p["severity"] for p in rubric.get("must_not") or []}
    W = sum(1 for i, v in mustnot_v.items() if v == "present" and sev.get(i) == "wrong") \
        + sum(1 for i, v in must_v.items() if v == "contradicted")
    H = sum(1 for i, v in mustnot_v.items() if v == "present" and sev.get(i) == "harmful")
    s = lambda pen: max(-1.0, P - pen * W - 0.5 * H)
    essential_miss = bool(essential_points(rubric, must_v))
    return {"P": P, "W": W, "H": H, "S": s(0.25), "S_w0": s(0.0), "S_w05": s(0.5),
            "essential_miss": essential_miss, "harmful": H > 0}


def essential_points(rubric, must_v):
    """Ids of the weight-2 must points that are absent while no member of their any_of group is present."""
    in_group = {i: g for g in groups_of(rubric) for i in g}
    out = []
    for p in rubric["must"]:
        if p["weight"] == 2 and must_v.get(p["id"]) == "absent":
            g = in_group.get(p["id"])
            if g and any(must_v.get(i) in ("present", "unverified") for i in g if i != p["id"]):
                continue
            out.append(p["id"])
    return out


def essential_units(rubric, must_v):
    """Essential-miss units of one answer (coverage role): one per weight-2 must point that is in no any_of group, and one per any_of
    group whose weight (the highest of its members) is 2. -> [{"ids": [member ids in rubric order], "status": s}] with s
      "present"     the point, or any member of the group, is present;
      "unverified"  no member present, but the judge said present or contradicted for one and its quote was not found (kind of
                    verdict "unverified": it is neither present nor a miss);
      "missed"      no member present or unverified, and at least one member absent;
      "other"       nothing present, nothing absent (contradicted only).
    must_v: {id: 'present'|'absent'|'contradicted'|'unverified'}."""
    weight = {p["id"]: p["weight"] for p in rubric["must"]}
    order = [p["id"] for p in rubric["must"]]
    groups = groups_of(rubric)
    in_group = {i for g in groups for i in g}
    sets = [{i} for i in order if i not in in_group and weight[i] == 2]
    sets += [{i for i in g if i in weight} for g in groups if g and max(weight.get(i, 0) for i in g) == 2]
    out = []
    for g in sorted(sets, key=lambda g: min(order.index(i) for i in g)):
        vs = [must_v.get(i) for i in g]
        if "present" in vs:
            st = "present"
        elif "unverified" in vs:
            st = "unverified"
        elif "absent" in vs:
            st = "missed"
        else:
            st = "other"
        out.append({"ids": [i for i in order if i in g], "status": st})
    return out


def canon_path(chunk):
    """Path of a chunk as the rubrics name files: 'raw/...' or 'data/...' as it is; a path relative to its source's root
    ('docs/x.md') gets 'raw/<source>/' in front."""
    p = chunk.get("path") or ""
    return p if p.startswith(("raw/", "data/")) else f"raw/{chunk.get('source', '')}/{p}"


def index_paths_of(chunks):
    """{canonical file path: [chunk ids]} of the chunk index (cached on the dict object's id for the run)."""
    key = id(chunks)
    if _INDEX_CACHE.get("key") != key:
        m = {}
        for cid, c in chunks.items():
            m.setdefault(canon_path(c), []).append(cid)
        _INDEX_CACHE.update(key=key, map=m)
    return _INDEX_CACHE["map"]


_INDEX_CACHE = {}


def _source_tokens(point, prefixes):
    """First word of each ';'-separated segment of the point's 'source' that starts with one of `prefixes`, without ':lines'.
    A segment that starts with a word such as 'searched', 'inferred' or 'not covered' names no file."""
    out = []
    for seg in (point.get("source") or "").split(";"):
        words = seg.split()
        if words and words[0].startswith(prefixes):
            out.append(re.sub(r":\d[\d,-]*$", "", words[0]))
    return out


def point_paths(point):
    """Index file paths named in a must point's 'source' ('raw/x/y.md:12; data/z.md:40-50' -> ['raw/x/y.md', 'data/z.md'])."""
    return _source_tokens(point, ("raw/", "data/"))


def point_catalogue_paths(point):
    """Tool source files of the catalogues ('raw_catalogue/...'); they are not in the chunk index."""
    return _source_tokens(point, ("raw_catalogue/",))


def point_source_kind(point, index_paths):
    """'index' (a file path with chunks in the index), 'not_indexed' (a raw/ or data/ path without chunks), 'catalogue'
    (only a raw_catalogue path), 'none' (no file path at all: 'searched ...', 'inferred', 'not covered')."""
    paths = point_paths(point)
    if paths:
        return "index" if any(x in index_paths for x in paths) else "not_indexed"
    return "catalogue" if point_catalogue_paths(point) else "none"


def point_chunks(point, index_paths):
    """Chunk ids of the files named in the point's source (file level: the sources carry line numbers of the original file,
    the chunks do not)."""
    return sorted({c for x in point_paths(point) for c in index_paths.get(x, [])})


def rubric_has_chunks(rubric):
    """Does the rubric carry chunk ids (written by the rubric author before any answer exists)? Yes if any must point has a 'chunks'
    or a 'not_indexed' field."""
    return any("chunks" in p or "not_indexed" in p for p in rubric["must"])


def supported_points(rubric, point_ids, pack, excerpt_paths, index_paths=None):
    """For essential-miss points: is the point's source among the answer's own excerpts?
    Rubrics that carry chunk ids (rubric_has_chunks): a point is supported if and only if one of its 'chunks' is in the pack; a point
    without 'chunks' is never supported ('not_indexed' when it says so, otherwise 'no_chunks'). Rubrics without chunk ids (the two
    pilots): the old file-level rule, by source file path against the excerpts' canonical paths.
    -> [{"id", "supported": bool, "rule": "chunk"|"not_indexed"|"no_chunks"|"path"|"none"|"no_pack"|"catalogue"}]. File-level rule: 'none':
    the point names no file; with index_paths given: 'not_indexed' (a raw/ or data/ path that has no chunk in the index) and 'catalogue'
    (only a raw_catalogue path): such a point can never be a supported miss."""
    pts = {p["id"]: p for p in rubric["must"]}
    by_chunk = rubric_has_chunks(rubric)
    out = []
    for pid in point_ids:
        p = pts[pid]
        if not pack:
            out.append({"id": pid, "supported": False, "rule": "no_pack"})
        elif p.get("chunks"):
            out.append({"id": pid, "supported": any(c in set(pack) for c in p["chunks"]), "rule": "chunk"})
        elif by_chunk:
            out.append({"id": pid, "supported": False, "rule": "not_indexed" if p.get("not_indexed") else "no_chunks"})
        elif point_paths(p):
            if index_paths is not None and not any(x in index_paths for x in point_paths(p)):
                out.append({"id": pid, "supported": False, "rule": "not_indexed"})
            else:
                out.append({"id": pid, "supported": any(x in set(excerpt_paths or []) for x in point_paths(p)), "rule": "path"})
        elif index_paths is not None and point_catalogue_paths(p):
            out.append({"id": pid, "supported": False, "rule": "catalogue"})
        else:
            out.append({"id": pid, "supported": False, "rule": "none"})
    return out


def validate_reply(rubric, answer, j, instruction="v1", source=None, excerpts=None, unverified=False):
    """Check a parsed reply against the rubric and the answer.
    -> (must {id: {verdict, quote, quote_ok, judge_verdict}}, must_not {...}, other_errors [...], errors [str], n_claims, n_failed).
    A point without a usable verdict, or a present/contradicted verdict whose quote is not in the answer (fair matching, see
    quote_found), is "absent". Instruction v2: an other-errors entry whose "source_quote" is not found in the source passages
    and the answer excerpts (the texts the judge was shown) is dropped and logged as "source_quote_not_found:other".
    unverified=True (coverage role): a present/contradicted verdict whose quote is not found becomes "unverified" instead of "absent"
    (for must and must-not points; judge_verdict keeps what the judge said, quote_ok is False); it is still counted in n_failed."""
    ans = fair_norm(answer)
    errors, n_claims, n_failed = [], 0, 0

    def one(points, reply_list, allowed, kind):
        nonlocal n_claims, n_failed
        got = {}
        for e in reply_list if isinstance(reply_list, list) else []:
            if isinstance(e, dict) and isinstance(e.get("id"), str):
                got.setdefault(e["id"], e)
        for e in got:
            if e not in {p["id"] for p in points}:
                errors.append(f"unknown_id:{e}")
        out = {}
        for p in points:
            e = got.get(p["id"])
            if e is None:
                errors.append(f"missing:{p['id']}")
                out[p["id"]] = {"verdict": "absent", "quote": "", "quote_ok": None, "judge_verdict": None}
                continue
            v, q = e.get("verdict"), e.get("quote") if isinstance(e.get("quote"), str) else ""
            if v not in allowed:
                errors.append(f"bad_verdict:{p['id']}")
                out[p["id"]] = {"verdict": "absent", "quote": q, "quote_ok": None, "judge_verdict": v}
                continue
            if v == "absent":
                out[p["id"]] = {"verdict": "absent", "quote": q, "quote_ok": None, "judge_verdict": v}
                continue
            n_claims += 1
            ok = quote_found(q, ans)
            if not ok:
                n_failed += 1
                errors.append(f"quote_not_found:{p['id']}")
            out[p["id"]] = {"verdict": v if ok else ("unverified" if unverified else "absent"), "quote": q, "quote_ok": ok, "judge_verdict": v}
        return out

    must = one(rubric["must"], j.get("must"), MUST_VERDICTS, "must")
    must_not = one(rubric.get("must_not") or [], j.get("must_not"), MUSTNOT_VERDICTS, "must_not")
    other = []
    shown = fair_norm((source or "") + "\n\n" + (excerpts or ""))
    for e in j.get("other_errors") or []:
        if isinstance(e, dict) and isinstance(e.get("quote"), str):
            entry = {"quote": e["quote"], "why": e.get("why") if isinstance(e.get("why"), str) else "",
                     "quote_ok": quote_found(e["quote"], ans)}
            if instruction != "v1":
                sq = e.get("source_quote") if isinstance(e.get("source_quote"), str) else ""
                entry["source_quote"] = sq
                entry["source_ok"] = quote_found(sq, shown)
                if not entry["source_ok"]:
                    errors.append("source_quote_not_found:other")
                    continue
            other.append(entry)
    return must, must_not, other, errors, n_claims, n_failed


SEVERITY_RANK = {"other": 0, "wrong": 1, "harmful": 2}


def candidate_errors(rubric, must, must_not, other):
    """Candidate errors of one judged answer, deduplicated across channels by overlapping quotes:
    must-not present (severity from the rubric), must contradicted ('wrong'), other errors that kept a verified source_quote
    (severity 'other'; in version 1 every entry, as there is no source_quote). -> [{channels, quotes, severity, reasons, points}]."""
    mn_pts = {p["id"]: p for p in rubric.get("must_not") or []}
    m_pts = {p["id"]: p for p in rubric["must"]}
    raw = []
    for pid, e in must_not.items():
        if e["verdict"] == "present":
            raw.append(("must_not", pid, e["quote"], mn_pts[pid].get("severity", "wrong"), mn_pts[pid]["text"]))
    for pid, e in must.items():
        if e["verdict"] == "contradicted":
            raw.append(("contradicted", pid, e["quote"], "wrong", m_pts[pid]["text"]))
    for e in other:
        raw.append(("other_errors", None, e["quote"], "other", e.get("why") or ""))
    groups = []
    for ch, pid, quote, sev, why in raw:
        hit = next((g for g in groups if any(quotes_overlap(quote, q) for q in g["quotes"])), None)
        if hit is None:
            hit = {"channels": [], "quotes": [], "severity": "other", "reasons": [], "points": []}
            groups.append(hit)
        if ch not in hit["channels"]:
            hit["channels"].append(ch)
        if quote and quote not in hit["quotes"]:
            hit["quotes"].append(quote)
        if SEVERITY_RANK[sev] > SEVERITY_RANK[hit["severity"]]:
            hit["severity"] = sev
        if why and why not in hit["reasons"]:
            hit["reasons"].append(why)
        if pid:
            hit["points"].append(pid)
    return groups


def result_row(item, pass_no, entry, coverage=False):
    """One output row from a cache entry (reply parsed or None). coverage=True: the coverage role's rules (failed-quote verdicts are
    "unverified"; the row also carries 'pack', the answer's chunk ids, so that supported misses can be rescored by chunk id)."""
    usage = entry.get("usage") if isinstance(entry.get("usage"), dict) else None
    base = {"set": item["set"], "id": item["id"], "question": item["question"], "pass": pass_no,
            "answer": item["answer"], "key": entry["key"], "seconds": entry.get("seconds"),
            "retried": bool(entry.get("retried")), "setting": entry.get("setting") or DEFAULT_SETTING,
            "instruction": entry.get("instruction") or "v1", "reference_given": bool(item.get("reference")),
            "prompt_chars": entry.get("prompt_chars"), "usage": usage,
            "completion_tokens": (usage or {}).get("completion_tokens"),
            "reasoning_chars": entry.get("reasoning_chars"), "finish_reason": entry.get("finish_reason"),
            "reply_chars": len(entry["reply"]) if isinstance(entry.get("reply"), str) else None}
    j = entry.get("j")
    if not isinstance(j, dict):
        return {**base, "valid_reply": False, "judge_errors": ["invalid_json"]}
    ins = base["instruction"]
    must, must_not, other, errors, n_claims, n_failed = validate_reply(
        item["rubric"], item["answer"], j, ins, item.get("source"), item.get("excerpts"), unverified=coverage)
    must_v = {k: v["verdict"] for k, v in must.items()}
    sc = score_item(item["rubric"], must_v, {k: v["verdict"] for k, v in must_not.items()})
    ess = supported_points(item["rubric"], essential_points(item["rubric"], must_v), item.get("pack"), item.get("excerpt_paths"), item.get("index_paths"))
    cands = candidate_errors(item["rubric"], must, must_not, other)
    return {**base, "valid_reply": True, "must": must, "must_not": must_not, "other_errors": other,
            "other_errors_n": len(other), "other_dropped": errors.count("source_quote_not_found:other"),
            "judge_errors": errors, "quote_claims": n_claims, "quote_failed": n_failed, **sc,
            "pack_n": len(item["pack"]) if item.get("pack") else None, "essential_points": ess,
            "supported_essential_miss": any(x["supported"] for x in ess),
            "candidates": cands, "n_candidates": len(cands), **({"pack": item.get("pack"), "coverage_rules": True} if coverage else {})}


# ---------------------------------------------------------------- judge call

def parse_reply(content):
    txt = re.sub(r"<think>.*?</think>", "", content or "", flags=re.S)
    m = re.search(r"\{.*\}", txt, flags=re.S)
    if not m:
        return None
    try:
        j = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    return j if isinstance(j, dict) else None


class Judge:
    def __init__(self, url, model, api_key=None, extra_body=None, temperature=0.0, client=None,
                 busy_wait_s=BUSY_WAIT_S, busy_retries=BUSY_RETRIES):
        import httpx
        self.httpx = httpx
        self.url, self.model, self.extra_body, self.temperature = url.rstrip("/"), model, extra_body or {}, temperature
        self.client = client or httpx.Client(timeout=1800, headers={"Authorization": f"Bearer {api_key}"} if api_key else {})
        self.busy_wait_s, self.busy_retries = busy_wait_s, busy_retries

    def chat(self, system, user, temperature):
        body = {**self.extra_body, "model": self.model, "temperature": temperature,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]}
        attempt = busy = 0
        while True:
            try:
                r = self.client.post(f"{self.url}/chat/completions", json=body)
                r.raise_for_status()
                data = r.json()
                choice = data["choices"][0]
                msg = choice["message"]
                reasoning = None   # length of the reasoning text when the reply carries it in a field of its own
                for k in ("reasoning_content", "reasoning"):
                    if k in msg:
                        reasoning = len(msg[k] or "") if isinstance(msg[k] or "", str) else None
                        break
                usage = data.get("usage")
                meta = {"usage": usage if isinstance(usage, dict) else None, "reasoning_chars": reasoning,
                        "finish_reason": choice.get("finish_reason")}
                return msg.get("content") or "", meta
            except (self.httpx.HTTPError, ValueError, KeyError, IndexError, TypeError, AttributeError) as e:
                what = type(e).__name__   # class and status only: the error text carries the server address
                code = e.response.status_code if isinstance(e, self.httpx.HTTPStatusError) else None
                if code:
                    what += f" {code}"
                if code in BUSY_STATUS and busy < self.busy_retries:
                    busy += 1
                    print(f"  server busy ({what}), waiting {self.busy_wait_s}s ({busy}/{self.busy_retries})", flush=True)
                    time.sleep(self.busy_wait_s)
                    continue
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt if self.busy_wait_s >= 1 else 0)
                attempt += 1
                print(f"  retry after {what}", flush=True)

    def __call__(self, system, user):
        """-> (parsed reply or None, raw content, retried, meta of the last call: usage as returned, reasoning_chars,
        finish_reason; after a retry also first_usage). A reply that is not valid JSON is retried once at temperature 0."""
        content, meta = self.chat(system, user, self.temperature)
        j = parse_reply(content)
        if j is not None:
            return j, content, False, meta
        content2, meta2 = self.chat(system, user, 0)
        return parse_reply(content2), content2, True, {**meta2, "first_usage": meta.get("usage")}


def stub_judge(system, user):
    """Dry run: every point absent. Reads the ids from the prompt."""
    must = re.findall(r"^- id (\S+):", user.split("MUST-NOT points:")[0], flags=re.M)
    mn = re.findall(r"^- id (\S+):", re.split(r"SOURCE PASSAGES:|ANSWER EXCERPTS|REFERENCE ANSWER:|ANSWER TO CHECK:", user.split("MUST-NOT points:")[1])[0], flags=re.M)
    j = {"must": [{"id": i, "verdict": "absent", "quote": ""} for i in must],
         "must_not": [{"id": i, "verdict": "absent", "quote": ""} for i in mn], "other_errors": []}
    return j, json.dumps(j), False, {"usage": {"completion_tokens": 0}, "reasoning_chars": None, "finish_reason": "stub"}


def cache_key(model, pass_no, item, system, user, setting=DEFAULT_SETTING, instruction="v1"):
    """The default setting hashes as before settings had names, and instruction v1 as before versions existed, so that old
    cache entries are found. Any other instruction version is part of the key."""
    tail = "" if setting == DEFAULT_SETTING else f"\0{setting}"
    if instruction != "v1":
        tail += f"\0instruction={instruction}"
    return hashlib.md5(f"{model}\0{pass_no}\0{item['set']}\0{item['id']}\0{system}\0{user}{tail}".encode()).hexdigest()


def load_cache(path, retry_invalid=False):
    cache = {}
    if path and Path(path).exists():
        for line in Path(path).read_text().splitlines():
            if line.strip():
                c = json.loads(line)
                if retry_invalid and not isinstance(c.get("j"), dict):
                    continue
                cache[c["key"]] = c
    return cache


def job_keys(items, passes, chunks, cap, model, setting=DEFAULT_SETTING, instruction="v1", excerpt_cap=DEFAULT_EXCERPT_CHARS):
    """-> [(item, pass, system, user, key)] in a fixed order."""
    prepare_items(items, chunks, cap, excerpt_cap, instruction)
    jobs = []
    for it in items:
        for p in passes:
            system, user = build_prompt(it, p, chunks, cap, instruction, excerpt_cap)
            jobs.append((it, p, system, user, cache_key(model, p, it, system, user, setting, instruction)))
    return jobs


def run(items, passes, judge, model, chunks, cap, cache_path, concurrency=1, retry_invalid=False, calls=True,
        say=lambda s: print(s, flush=True), setting=DEFAULT_SETTING, write_cache=True, instruction="v1",
        excerpt_cap=DEFAULT_EXCERPT_CHARS, coverage=False):
    """Judge every (item, pass) not in the cache. -> list of result rows in a fixed order.
    Every progress line is flushed. write_cache False: the cache is read but not written (dry run)."""
    cache = load_cache(cache_path, retry_invalid)
    jobs = job_keys(items, passes, chunks, cap, model, setting, instruction, excerpt_cap)
    lock = threading.Lock()

    def do(job):
        it, p, system, user, key = job
        t0 = time.time()
        res = judge(system, user)
        j, raw, retried = res[:3]
        meta = res[3] if len(res) > 3 and isinstance(res[3], dict) else {}
        entry = {"key": key, "set": it["set"], "id": it["id"], "pass": p, "setting": setting, "instruction": instruction, "j": j, "reply": raw,
                 "retried": retried, "seconds": round(time.time() - t0, 1), "prompt_chars": len(system) + len(user),
                 "usage": meta.get("usage"), "reasoning_chars": meta.get("reasoning_chars"),
                 "finish_reason": meta.get("finish_reason")}
        if "first_usage" in meta:
            entry["first_usage"] = meta["first_usage"]
        with lock:
            cache[key] = entry
            if cache_path and write_cache:
                Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, "a") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            row = result_row(it, p, entry, coverage)
            say(f"[judge] {len(cache)} cached  {it['set']} {it['id']} p{p} valid={row['valid_reply']}"
                + (f" quotes_failed={row['quote_failed']}/{row['quote_claims']} S={row['S']:.2f} candidates={row['n_candidates']}"
                   if row["valid_reply"] else "")
                + f" {entry['seconds']}s tokens={row['completion_tokens']}")

    todo = [j for j in jobs if j[4] not in cache]
    if calls and todo:
        with ThreadPoolExecutor(concurrency) as ex:
            list(ex.map(do, todo))
    rows = []
    for it, p, _, _, key in jobs:
        if key in cache:
            rows.append(result_row(it, p, cache[key], coverage))
    return rows, len(todo) if calls else 0, len(jobs)


# ---------------------------------------------------------------- generic answers

GENERIC_TEMPLATE = (
    "This is a common situation and it usually has a straightforward explanation. First, review the relevant "
    "configuration and the recent changes to your environment, and check the logs of the components involved for "
    "errors or warnings. Make sure all components are healthy and that the settings are appropriate for your "
    "workload. If something looks wrong, adjust the configuration carefully, test the change in a non-production "
    "environment first, and monitor the system afterwards. Consult the official documentation for the exact steps "
    "for your version, and consider contacting your platform team or support if the problem continues. Keeping "
    "good monitoring, regular backups and a tested rollback plan will help to prevent similar issues in the future."
)


def generic_answers(rubrics, qmap):
    """One bland answer per rubric question, from a fixed template (it names nothing specific)."""
    return [{"label": "generic", "question": rubric_question(r, qmap), "text": GENERIC_TEMPLATE} for r in rubrics]


PROBE_SETS = (("probe_generic", "generic"), ("probe_alt", "correct_alternative"), ("probe_ref", "reference"))
PROBE_NEARMISS = "probe_nearmiss"
PROBE_SEP = "~"


def make_probes(rubrics, items_by_id):
    """The probe answer sets of a rubric file (second pilot), built from the rubric only.
    probe_generic / probe_alt / probe_ref: the rubric's 'generic', 'correct_alternative' and 'reference' answer, one per item.
    probe_nearmiss: for each must-not point, one answer = the item's reference (whitespace collapsed to one paragraph) with the
    point's 'near_miss' sentence appended, under the id '<item>~<point>' with a copy of the item's rubric of that id.
    -> (probe rubrics: the originals with question and evidence filled in, then the near-miss copies; {set: [answer rows]}).
    Raises ValueError for a rubric without one of the fields (nothing is guessed)."""
    out_rubrics, sets = [], {name: [] for name, _ in PROBE_SETS}
    sets[PROBE_NEARMISS] = []
    near = []
    for r in rubrics:
        it = items_by_id.get(r["id"])
        if it is None:
            raise ValueError(f"no item for rubric {r['id']}")
        base = {**r, "question": r.get("question") or it["question"]}
        if (it.get("evidence") or "").strip() and not base.get("evidence"):
            base["evidence"] = it["evidence"]
        out_rubrics.append(base)
        for name, field in PROBE_SETS:
            text = (r.get(field) or "").strip()
            if not text:
                raise ValueError(f"rubric {r['id']}: no '{field}' for the probe")
            sets[name].append({"id": r["id"], "label": name, "text": text})
        for pt in r.get("must_not") or []:
            nm = (pt.get("near_miss") or "").strip()
            if not nm or not (r.get("reference") or "").strip():
                raise ValueError(f"rubric {r['id']}: must-not {pt['id']} has no near_miss, or the rubric has no reference")
            pid = f"{r['id']}{PROBE_SEP}{pt['id']}"
            near.append({**base, "id": pid, "probe_of": r["id"]})
            sets[PROBE_NEARMISS].append({"id": pid, "label": PROBE_NEARMISS, "text": norm_ws(r["reference"]) + " " + norm_ws(nm)})
    return out_rubrics + near, sets


# ---------------------------------------------------------------- coverage role

def coverage_row(row, rubric):
    """The coverage role's view of one result row (valid reply). Verdicts: a present or contradicted verdict whose quote was not found
    is "unverified", whether the row was made with coverage rules (it says "unverified" already) or without (it says "absent" with
    judge_verdict present/contradicted and quote_ok False: the old demotion, undone here). Unverified is not present and not a miss.
    Supported: rubrics with chunk ids: by chunk id against the row's 'pack' (rows made with --coverage carry it; without it nothing is
    supported); rubrics without chunk ids: the file-level result stored in the row's essential_points (members that were not listed
    there count as unsupported). A unit is supported if any member is.
    -> {"P", "units": [{"ids", "status", "supported"}], "must_not_present", "unverified", "must_verdicts", "demoted_raw", "rule"}."""
    must_v, demoted = {}, 0
    for pid, e in row["must"].items():
        v = e["verdict"]
        if v == "absent" and e.get("quote_ok") is False and e.get("judge_verdict") in ("present", "contradicted"):
            v, demoted = "unverified", demoted + 1
        must_v[pid] = v
    mn_v = {pid: ("unverified" if e["verdict"] == "absent" and e.get("quote_ok") is False and e.get("judge_verdict") == "present"
                  else e["verdict"]) for pid, e in row["must_not"].items()}
    sc = score_item(rubric, must_v, mn_v)
    units = essential_units(rubric, must_v)
    by_chunk = rubric_has_chunks(rubric)
    if by_chunk:
        sup = {x["id"]: x["supported"] for x in supported_points(rubric, [p["id"] for p in rubric["must"]], row.get("pack"), None)}
    else:
        sup = {x["id"]: x["supported"] for x in row.get("essential_points") or []}
    for u in units:
        u["supported"] = any(sup.get(i) for i in u["ids"])
    return {"P": sc["P"], "units": units, "must_not_present": sum(1 for v in mn_v.values() if v == "present"),
            "unverified": sum(1 for v in must_v.values() if v == "unverified"), "must_verdicts": len(must_v),
            "demoted_raw": demoted + sum(1 for pid, e in row["must_not"].items() if mn_v[pid] == "unverified" and e["verdict"] == "absent"),
            "rule": "chunk" if by_chunk else "file"}


def coverage_measures(rows, rubrics):
    """Per answer set and pass, from result rows: answers, mean coverage P, essential-miss units (count, per 10 answers), supported
    essential-miss units (count, per 10), must-not points present, unverified must verdicts and their share of all must verdicts.
    -> {set: {pass: {...}}}."""
    rub = {r["id"]: r for r in rubrics}
    out = {}
    for r in rows:
        if not r.get("valid_reply"):
            continue
        c = coverage_row(r, rub[r["id"]])
        m = out.setdefault(r["set"], {}).setdefault(r["pass"], {"n": 0, "P": 0.0, "units": 0, "missed": 0, "supported_missed": 0,
                                                                  "must_not_present": 0, "unverified": 0, "must_verdicts": 0})
        m["n"] += 1
        m["P"] += c["P"]
        m["units"] += len(c["units"])
        m["missed"] += sum(1 for u in c["units"] if u["status"] == "missed")
        m["supported_missed"] += sum(1 for u in c["units"] if u["status"] == "missed" and u["supported"])
        m["must_not_present"] += c["must_not_present"]
        m["unverified"] += c["unverified"]
        m["must_verdicts"] += c["must_verdicts"]
    for per in out.values():
        for m in per.values():
            m["mean_P"] = m["P"] / m["n"]
            m["missed_per10"] = 10 * m["missed"] / m["n"]
            m["supported_missed_per10"] = 10 * m["supported_missed"] / m["n"]
            m["unverified_share"] = m["unverified"] / m["must_verdicts"] if m["must_verdicts"] else float("nan")
    return out


# ---------------------------------------------------------------- chunk ids of rubric points (helper for rubric authors)

import html as _html

PLACE_MIN_OVERLAP = 0.3   # a chunk is placed for a source range if it holds at least this share of the range's text (word 4-grams)
_SHINGLE = 4
_TAGS = re.compile(r"<[^>]+>")
_MD_LINK_URL = re.compile(r"\]\([^)]*\)")


def _tokens(text):
    """Words of a text for matching: markup (html tags, markdown link targets, emphasis, backticks), case and whitespace do not matter."""
    t = _html.unescape(_TAGS.sub(" ", _MD_LINK_URL.sub("]", text or ""))).lower()
    return re.findall(r"[a-z0-9]+", t)


def _shingles(toks, n=_SHINGLE):
    n = max(1, min(n, len(toks)))
    return {tuple(toks[i:i + n]) for i in range(len(toks) - n + 1)}, n


def parse_source_ref(ref):
    """'raw/a/b.md:120-130; data/x.md:5,9-12; inferred ...' -> [{"ref", "path", "lines": [(a, b)] or None, "kind"}] with kind
    'index' (raw/ or data/), 'catalogue' (raw_catalogue/) or 'none' (a segment that names no file)."""
    out = []
    for seg in (ref or "").split(";"):
        seg = seg.strip()
        if not seg:
            continue
        w = seg.split()[0].rstrip(",.)")
        m = re.fullmatch(r"(.+?):(\d[\d,\-]*)", w)
        path, spec = (m.group(1), m.group(2)) if m else (w, None)
        kind = "catalogue" if path.startswith("raw_catalogue/") else "index" if path.startswith(("raw/", "data/")) else "none"
        lines = None
        if spec:
            lines = []
            for part in spec.split(","):
                a, _, b = part.partition("-")
                if a.isdigit():
                    lines.append((int(a), int(b) if b.isdigit() else int(a)))
        out.append({"ref": seg, "path": path if kind != "none" else None, "lines": lines or None, "kind": kind})
    return out


def place_ref(ref, chunks, root=".", min_overlap=PLACE_MIN_OVERLAP):
    """Index chunks whose text contains the lines a source reference cites. chunks: {id: chunk} of the frozen index. The cited lines of
    the file under `root` are matched against the text of the chunks of that file (word 4-grams, so whitespace, case, markup and chunk
    boundaries do not matter); a chunk is placed if it holds at least `min_overlap` of the range's 4-grams.
    -> [{"ref", "path", "status", "chunks": [ids], "overlap": {id: share}, "best": (id, share) or None, "note"}] one per segment;
    status 'placed', 'not in the index' (nothing reaches min_overlap, or the file has no chunk), 'catalogue' (a tool source, not in
    the index), 'no file' (a segment without a file path), 'no lines' or 'file not found' (cannot be matched)."""
    by_path = index_paths_of(chunks)
    out = []
    for seg in parse_source_ref(ref):
        res = {"ref": seg["ref"], "path": seg["path"], "status": None, "chunks": [], "overlap": {}, "best": None, "note": ""}
        out.append(res)
        if seg["kind"] == "none":
            res["status"] = "no file"
            continue
        if seg["kind"] == "catalogue":
            res["status"] = "catalogue"
            continue
        if not seg["lines"]:
            res["status"], res["note"] = "no lines", "the reference gives no line numbers"
            continue
        f = Path(root) / seg["path"]
        if not f.is_file():
            res["status"] = "file not found"
            continue
        lines = f.read_text(errors="replace").splitlines()
        ranges = []   # one target per cited range (a list like 146,289,311 are separate places, not one text)
        for a, b in seg["lines"]:
            toks = _tokens("\n".join(lines[max(a - 1, 0):b]))
            if toks:
                ranges.append(_shingles(toks))
        ids = by_path.get(seg["path"], [])
        if not ranges:
            res["status"], res["note"] = "no lines", "the cited lines have no text"
            continue
        if not ids:
            res["status"], res["note"] = "not in the index", "the file has no chunk in the index"
            continue
        for cid in ids:
            ctoks = _tokens(chunks[cid]["text"])
            ov = max(len(sh & _shingles(ctoks, n)[0]) / len(sh) for sh, n in ranges)
            if ov > 0:
                res["overlap"][cid] = round(ov, 3)
        if res["overlap"]:
            res["best"] = max(res["overlap"].items(), key=lambda kv: kv[1])
        res["chunks"] = sorted(c for c, ov in res["overlap"].items() if ov >= min_overlap)
        res["status"] = "placed" if res["chunks"] else "not in the index"
        short = min(n for _, n in ranges) < _SHINGLE
        if res["chunks"] and (len(res["chunks"]) > 3 or short):
            res["note"] = (f"{len(res['chunks'])} chunks" if len(res["chunks"]) > 3 else "a cited range of fewer than 4 words") + \
                ": a short or common text can be in chunks that do not hold the point; check by hand"
    return out


def describe_placement(places):
    L = []
    for r in places:
        if r["status"] == "placed":
            L.append(f"{r['ref']}: " + ", ".join(f"{c} ({r['overlap'][c]:.2f})" for c in r["chunks"]) + (f" [{r['note']}]" if r["note"] else ""))
        else:
            b = f"; best partial match {r['best'][0]} with overlap {r['best'][1]:.2f}" if r["best"] else ""
            L.append(f"{r['ref']}: " + ("not in the index" if r["status"] == "not in the index" else r["status"]) + (f" ({r['note']})" if r["note"] else "") + b)
    return L


def fill_chunks(rubrics, chunks, root=".", overwrite=False, check=False):
    """Fill 'chunks' of every must point from its 'source' (or, with check=True, only check). A point with placed segments gets the union
    of their chunk ids; a point with none gets 'not_indexed': true and no 'chunks'. A point that already has 'chunks' keeps them unless
    overwrite. Check: a recorded chunk id that is not in the index, or that is not among the placed chunks of the point's source, is a
    problem; so is a point with neither 'chunks' nor 'not_indexed'.
    -> (rubrics (filled copies; the input in check mode), unplaced [(rubric id, point id, reason)], problems [(rubric id, point id, text)])."""
    out, unplaced, problems = [], [], []
    for r in rubrics:
        r2 = json.loads(json.dumps(r))
        for p in r2["must"]:
            places = place_ref(p.get("source"), chunks, root)
            ids = sorted({c for x in places for c in x["chunks"]})
            if check:
                have = p.get("chunks") or []
                if not have and not p.get("not_indexed"):
                    problems.append((r["id"], p["id"], "neither 'chunks' nor 'not_indexed'"))
                for c in have:
                    if c not in chunks:
                        problems.append((r["id"], p["id"], f"chunk {c} is not in the index"))
                    elif ids and c not in ids:
                        problems.append((r["id"], p["id"], f"chunk {c} is not among the placed chunks {ids}"))
                if have and not ids:
                    problems.append((r["id"], p["id"], "has chunks but its source places none: " + "; ".join(describe_placement(places))))
                if p.get("not_indexed") and ids:
                    problems.append((r["id"], p["id"], f"says not_indexed but its source places chunks {ids}"))
                continue
            if p.get("chunks") and not overwrite:
                continue
            if ids:
                p["chunks"] = ids
                p.pop("not_indexed", None)
            else:
                p.pop("chunks", None)
                p["not_indexed"] = True
                unplaced.append((r["id"], p["id"], "; ".join(describe_placement(places)) or "no source"))
        out.append(r2)
    return (rubrics if check else out), unplaced, problems


# ---------------------------------------------------------------- measures, review file

def measures(rows):
    """Per answer set and pass: answers with a valid reply, mean P (coverage), essential-miss rate, supported essential-miss
    rate, candidate errors, mean S, quote failures, source quotes dropped. Rates and totals are also given per 10 answers.
    -> {set: {pass: {...}}} (insertion order of the rows)."""
    out = {}
    for r in rows:
        if not r.get("valid_reply"):
            continue
        m = out.setdefault(r["set"], {}).setdefault(r["pass"], {
            "n": 0, "P": 0.0, "S": 0.0, "essential_miss": 0, "supported_essential_miss": 0, "candidates": 0,
            "harmful_candidates": 0, "quote_claims": 0, "quote_failed": 0, "other_dropped": 0})
        m["n"] += 1
        m["P"] += r["P"]
        m["S"] += r["S"]
        m["essential_miss"] += bool(r["essential_miss"])
        m["supported_essential_miss"] += bool(r.get("supported_essential_miss"))
        m["candidates"] += len(r.get("candidates") or [])
        m["harmful_candidates"] += sum(1 for c in r.get("candidates") or [] if c["severity"] == "harmful")
        m["quote_claims"] += r["quote_claims"]
        m["quote_failed"] += r["quote_failed"]
        m["other_dropped"] += r.get("other_dropped", 0)
    for per_pass in out.values():
        for m in per_pass.values():
            n = m["n"]
            m["mean_P"], m["mean_S"] = m["P"] / n, m["S"] / n
            for k in ("essential_miss", "supported_essential_miss", "candidates", "harmful_candidates"):
                m[k + "_per10"] = 10 * m[k] / n
    return out


def review_rows(rows):
    """Candidate errors of a results file as review rows: one row per candidate, merged across the passes and the channels of
    one answer when the quotes overlap. Columns 'confirmed' and 'note' are empty for the reviewer. 'members' lists
    [pass, index of the candidate in that pass's row] so that the flags of each pass can be found again."""
    by = OrderedDict()
    for r in rows:
        if r.get("valid_reply"):
            by.setdefault((r["set"], r["id"]), []).append(r)
    out = []
    for (name, rid), rs in by.items():
        groups = []
        for r in sorted(rs, key=lambda x: x["pass"]):
            for idx, c in enumerate(r.get("candidates") or []):
                g = next((g for g in groups if any(quotes_overlap(q, q2) for q in c["quotes"] for q2 in g["quotes"])), None)
                if g is None:
                    g = {"passes": [], "members": [], "channels": [], "quotes": [], "severity": "other", "reasons": [], "points": []}
                    groups.append(g)
                if r["pass"] not in g["passes"]:
                    g["passes"].append(r["pass"])
                g["members"].append([r["pass"], idx])
                for k, key in (("channels", "channels"), ("quotes", "quotes"), ("reasons", "reasons"), ("points", "points")):
                    g[k] += [x for x in c[key] if x not in g[k]]
                if SEVERITY_RANK[c["severity"]] > SEVERITY_RANK[g["severity"]]:
                    g["severity"] = c["severity"]
        for n, g in enumerate(groups, 1):
            out.append({"flag_id": f"{name}:{rid}:{n}", "item": rid, "set": name, **g, "confirmed": "", "note": ""})
    return out


CONFIRMED_VALUES = ("yes", "harmful", "no", "unclear", "")


def confirmed_summary(review, rows):
    """From a filled review file: per answer set, the answers judged (valid in at least one pass), confirmed wrong items (confirmed
    'yes' or 'harmful'), confirmed harmful items ('harmful', or 'yes' on a flag the rubric rates harmful), and both per 10
    answers; flags not yet decided are counted. A flag merged over two passes is one item."""
    n_answers = {}
    for r in rows:
        if r.get("valid_reply"):
            n_answers.setdefault(r["set"], set()).add(r["id"])
    out = {}
    for name, ids in n_answers.items():
        mine = [x for x in review if x["set"] == name]
        wrong = [x for x in mine if x["confirmed"] in ("yes", "harmful")]
        harmful = [x for x in wrong if x["confirmed"] == "harmful" or x["severity"] == "harmful"]
        n = len(ids)
        out[name] = {"answers": n, "flags": len(mine), "undecided": sum(1 for x in mine if x["confirmed"] not in ("yes", "harmful", "no", "unclear")),
                     "unclear": sum(1 for x in mine if x["confirmed"] == "unclear"),
                     "confirmed_wrong": len(wrong), "confirmed_harmful": len(harmful),
                     "wrong_per10": 10 * len(wrong) / n, "harmful_per10": 10 * len(harmful) / n}
    return out


# ---------------------------------------------------------------- main

def api_key():
    k = os.environ.get("JUDGE_API_KEY") or os.environ.get("OMLX_API_KEY")
    if not k:
        p = Path.home() / ".config/kafka-sre-llm/judge_key"
        k = p.read_text().strip() if p.exists() else None
    return k


def estimate(n_calls, concurrency):
    lo = n_calls * SECONDS_PER_CALL_V3_MEDIAN / max(concurrency, 1) / 3600
    design = n_calls * SECONDS_PER_ITEM_DESIGN / 3600
    return lo, design


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--rubrics", required=True)
    common.add_argument("--qmap", nargs="*", default=[], help="PREFIX=questions.jsonl; rubric id PREFIX-N is the Nth question")
    fl = sub.add_parser("flags", help="write the candidate errors of a results file to a review file")
    fl.add_argument("--results", nargs="+", required=True, help="--out file(s) of the run command")
    fl.add_argument("--out", required=True)
    cf = sub.add_parser("confirmed", help="confirmed wrong and harmful items per 10 answers from a filled review file")
    cf.add_argument("--review", required=True)
    cf.add_argument("--results", nargs="+", required=True)
    cfor = sub.add_parser("chunks-for", help="index chunk ids whose text contains the lines of a source reference ('raw/x.md:120-130; ...')")
    cfor.add_argument("ref")
    cfor.add_argument("--chunks", required=True)
    cfor.add_argument("--root", default=".", help="directory that holds raw/ and data/")
    cfor.add_argument("--min-overlap", type=float, default=PLACE_MIN_OVERLAP)
    fc = sub.add_parser("fill-chunks", parents=[common], help="fill (or --check) 'chunks' of every must point of a rubrics file from its 'source'")
    fc.add_argument("--chunks", required=True)
    fc.add_argument("--root", default=".")
    fc.add_argument("--out", help="the filled rubrics file (not written with --check)")
    fc.add_argument("--check", action="store_true", help="only check the recorded chunk ids; exit 1 on a problem")
    fc.add_argument("--overwrite", action="store_true", help="replace existing 'chunks' too")
    g = sub.add_parser("gen-generic", parents=[common], help="write the generic-answer probe file")
    g.add_argument("--out", required=True)
    gp = sub.add_parser("gen-probes", parents=[common], help="write the probe answer sets and the probe rubrics file (second pilot)")
    gp.add_argument("--items", required=True, help="JSONL of items (id, question, optional evidence)")
    gp.add_argument("--out-dir", required=True, help="writes probe_rubrics.jsonl and one <set>.jsonl per probe set here")
    r = sub.add_parser("run", parents=[common], help="judge answers against rubrics")
    r.add_argument("--answers", nargs="+", required=True, metavar="NAME=PATH")
    r.add_argument("--chunks", help="JSONL of chunks (id, text) for rubrics that give covered_chunks")
    r.add_argument("--source-chars", type=int, default=DEFAULT_SOURCE_CHARS,
                   help="cap on the source passages of one item, in characters; 0 = no source passages at all")
    r.add_argument("--settings-name", default=DEFAULT_SETTING,
                   help="label of the judge settings, stored in each cache entry and in the cache key (the default is the "
                        "setting of entries that have no label)")
    r.add_argument("--only", nargs="+", metavar="SET:ID", help="judge only these answers (answer set name and rubric id)")
    r.add_argument("--instruction", choices=sorted(INSTRUCTIONS), default=DEFAULT_INSTRUCTION,
                   help="instruction text (v1: the first pilot's; v2: with ANSWER EXCERPTS and source_quote); part of the cache key")
    r.add_argument("--items", help="JSONL of items ('id', 'question', optional 'evidence'): fills the question and evidence of rubrics "
                                   "that have none (the harness's answer rows carry only the item id)")
    r.add_argument("--packs", nargs="*", default=[], metavar="NAME=PATH",
                   help="JSONL of retrieval packs for an answer set ('question' and 'pack' or 'k6' chunk ids) when its answer rows "
                        "carry no 'pack'")
    r.add_argument("--excerpt-chars", type=int, default=DEFAULT_EXCERPT_CHARS, help="cap on the answer excerpts of one item")
    r.add_argument("--with-reference", action="store_true",
                   help="add the rubric's 'reference' answer to the judge's input (off by default; use another --settings-name)")
    r.add_argument("--passes", type=int, nargs="+", default=[1, 2], choices=[1, 2])
    r.add_argument("--cache", help="JSONL of judge replies; written per call and reused on rerun")
    r.add_argument("--out", help="JSONL of validated per-point results and scores, rewritten from the cache at the end")
    r.add_argument("--judge-url", default=None, help="default $OMLX_URL/v1")
    r.add_argument("--judge-model", default="GLM-5.3-Flash-oQ8e-mtp")
    r.add_argument("--judge-extra-body", type=json.loads, default=None, help="JSON merged into the request as given")
    r.add_argument("--temperature", type=float, default=0.0)
    r.add_argument("--concurrency", type=int, default=3)
    r.add_argument("--retry-invalid", action="store_true", help="redo cached calls whose reply was not valid JSON")
    r.add_argument("--stub", action="store_true", help="dry run: stub judge, no server; prints matches, largest prompt, estimate")
    r.add_argument("--rescore", action="store_true", help="no calls: rebuild --out from --cache")
    r.add_argument("--coverage", action="store_true",
                   help="coverage role: a failed-quote verdict becomes 'unverified' (not absent); rows carry the answer's pack; the cache key is unchanged")
    a = ap.parse_args(argv)

    if a.cmd == "flags":
        rows = [x for f in a.results for x in rd(f)]
        rev = review_rows(rows)
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rev))
        print(f"wrote {len(rev)} candidate errors on {len({(x['set'], x['item']) for x in rev})} answers to {a.out}")
        return 0
    if a.cmd == "confirmed":
        rev, rows = rd(a.review), [x for f in a.results for x in rd(f)]
        bad = [x.get("flag_id") for x in rev if x.get("confirmed", "") not in CONFIRMED_VALUES]
        if bad:
            print(f"{a.review}: 'confirmed' must be one of yes, harmful, no, unclear or empty; wrong in {bad[:5]}", file=sys.stderr)
            return 1
        print("| answer set | answers | flags | undecided | confirmed wrong | per 10 answers | confirmed harmful | per 10 answers |")
        print("|---|---|---|---|---|---|---|---|")
        for name, m in confirmed_summary(rev, rows).items():
            print(f"| {name} | {m['answers']} | {m['flags']} | {m['undecided']} | {m['confirmed_wrong']} | {m['wrong_per10']:.1f} | "
                  f"{m['confirmed_harmful']} | {m['harmful_per10']:.1f} |")
        return 0
    if a.cmd == "chunks-for":
        idx = {c["id"]: c for c in rd(a.chunks)}
        places = place_ref(a.ref, idx, a.root, a.min_overlap)
        for line in describe_placement(places):
            print(line)
        ids = sorted({c for x in places for c in x["chunks"]})
        print("chunk ids: " + (" ".join(ids) if ids else "none"))
        return 0 if ids else 1
    if a.cmd == "fill-chunks":
        idx = {c["id"]: c for c in rd(a.chunks)}
        rubs = rd(a.rubrics)
        new, unplaced, problems = fill_chunks(rubs, idx, a.root, a.overwrite, a.check)
        npts = sum(len(r["must"]) for r in rubs)
        if a.check:
            for rid, pid, t in problems:
                print(f"problem: {rid} {pid}: {t}")
            print(f"checked {npts} must points: {len(problems)} problems")
            return 1 if problems else 0
        if not a.out:
            ap.error("fill-chunks needs --out (or --check)")
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in new))
        for rid, pid, why in unplaced:
            print(f"could not place: {rid} {pid}: {why}")
        print(f"{npts} must points: {sum(1 for r in new for p in r['must'] if p.get('chunks'))} with chunks, "
              f"{len(unplaced)} could not be placed (marked not_indexed); wrote {a.out}")
        return 0
    rubrics = rd(a.rubrics)
    qmap = load_qmap(a.qmap)
    if a.cmd == "gen-generic":
        rows = generic_answers(rubrics, qmap)
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rows))
        print(f"wrote {len(rows)} generic answers to {a.out}")
        return 0

    if a.cmd == "gen-probes":
        try:
            prubs, psets = make_probes(rubrics, {x["id"]: x for x in rd(a.items)})
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1
        d = Path(a.out_dir)
        d.mkdir(parents=True, exist_ok=True)
        (d / "probe_rubrics.jsonl").write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in prubs))
        for name, rows in psets.items():
            (d / f"{name}.jsonl").write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rows))
        print(f"wrote {len(prubs)} probe rubrics and " + ", ".join(f"{name} {len(rows)}" for name, rows in psets.items()) + f" answers to {d}")
        return 0

    sets = OrderedDict()
    for spec in a.answers:
        name, _, path = spec.partition("=")
        if not path:
            ap.error(f"--answers {spec!r}: expected NAME=PATH")
        sets[name] = rd(path)
    chunks = {c["id"]: c for c in rd(a.chunks)} if a.chunks else {}
    if a.source_chars < 0:
        ap.error("--source-chars must be 0 or more")
    only = None
    if a.only:
        only = set()
        for spec in a.only:
            name, _, rid = spec.partition(":")
            if not rid or name not in sets:
                ap.error(f"--only {spec!r}: expected SET:ID with SET one of {list(sets)}")
            only.add((name, rid))
    if a.items:
        by_id = {x["id"]: x for x in rd(a.items)}
        for rb in rubrics:
            it = by_id.get(rb.get("probe_of") or rb["id"])
            if it is None:
                ap.error(f"--items {a.items}: no item for rubric {rb['id']}")
            rb.setdefault("question", it["question"])
            if it.get("evidence"):
                rb.setdefault("evidence", it["evidence"])
    packs = {}
    for spec in a.packs:
        name, _, path = spec.partition("=")
        if not path or name not in sets:
            ap.error(f"--packs {spec!r}: expected NAME=PATH with NAME one of {list(sets)}")
        packs[name] = {norm_ws(x["question"]): pack_ids_of(x) for x in rd(path) if x.get("question")}
    try:
        items, counts = match_items(rubrics, sets, qmap, only, packs, a.with_reference)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    if only is not None and {(it["set"], it["id"]) for it in items} != only:
        missing = sorted(only - {(it["set"], it["id"]) for it in items})
        print(f"--only: no matched answer for {missing}", file=sys.stderr)
        return 1
    for name, n in counts.items():
        print(f"matched {name}: {n} answers", flush=True)
    if not items:
        print("no answer matched a rubric", file=sys.stderr)
        return 1
    if a.stub or a.rescore:
        url = None
        judge = stub_judge
    else:
        url = a.judge_url or (os.environ.get("OMLX_URL", "").rstrip("/") + "/v1" if os.environ.get("OMLX_URL") else None)
        if not url:
            print("no judge URL: pass --judge-url or set OMLX_URL", file=sys.stderr)
            return 2
        judge = Judge(url, a.judge_model, api_key(), a.judge_extra_body, a.temperature)
    cache_path = a.cache   # a dry run reads it (cached entries are reused) and never writes it
    try:
        prepare_items(items, chunks, a.source_chars, a.excerpt_chars, a.instruction)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1
    if a.instruction != "v1":
        with_pack = sum(1 for it in items if it.get("pack"))
        print(f"instruction {a.instruction}: {with_pack} of {len(items)} answers have their own excerpts"
              + (f"; largest excerpts block {max(len(it['excerpts'] or '') for it in items)} characters" if a.source_chars else ""), flush=True)
    sizes = [len(s) + len(u) for it in items for p in a.passes
             for s, u in [build_prompt(it, p, chunks, a.source_chars, a.instruction, a.excerpt_chars)]]
    n_calls = len(items) * len(a.passes)
    lo, design = estimate(n_calls, a.concurrency)
    print(f"items {len(items)}, passes {a.passes}, judge calls {n_calls}, largest prompt {max(sizes)} characters "
          f"(system + user), mean {sum(sizes) // len(sizes)}", flush=True)
    print(f"judge time estimate for {n_calls} calls: {lo:.1f} h (v3 median {SECONDS_PER_CALL_V3_MEDIAN} s per call, "
          f"{a.concurrency} in parallel) to {design:.1f} h (design: {SECONDS_PER_ITEM_DESIGN} s per item)", flush=True)
    cached = load_cache(cache_path, a.retry_invalid)
    reused = sum(1 for *_, k in job_keys(items, a.passes, chunks, a.source_chars, a.judge_model, a.settings_name,
                                                 a.instruction, a.excerpt_chars) if k in cached)
    print(f"settings {a.settings_name}, instruction {a.instruction}: {reused} of {n_calls} judgments already in the cache", flush=True)
    rows, n_new, n_jobs = run(items, a.passes, judge, a.judge_model, chunks, a.source_chars, cache_path,
                              a.concurrency, a.retry_invalid, calls=not a.rescore, setting=a.settings_name,
                              write_cache=not a.stub, instruction=a.instruction, excerpt_cap=a.excerpt_chars, coverage=a.coverage)
    invalid = sum(not r["valid_reply"] for r in rows)
    print(f"{n_jobs} judgments, {n_new} called now, {len(rows) - invalid} valid replies, {invalid} unscored", flush=True)
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        tmp = a.out + ".tmp"
        Path(tmp).write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rows))
        os.replace(tmp, a.out)
        print(f"wrote {a.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
