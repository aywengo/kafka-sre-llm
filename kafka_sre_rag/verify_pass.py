"""M7 P1 (EXPERIMENTAL, unmeasured): the checking pass of the thorough profile, and the profile's draft step.

The binding design is runs/m7/reports/k1_check_design.md (sections 2 and 3) with the conductor's decisions of 2026-11-23
in MILESTONE.md. One extra call to the same model, thinking off, temperature 0, at most 600 tokens: the question, the
excerpts (prompts.pack_text), the draft with numbered lines (revise.numbered), the checker's invented and wrong_use
findings laid out as revise.build_messages lays them out (revise.finding_view), and with cards also the reference cards
of the draft's names (kb_lookup.cards on the draft, kb_lookup.render within its budget). The prompt is
prompts.VERIFY_PROMPT or prompts.VERIFY_PROMPT_CARDS under prompts.VERIFY_SYSTEM. The reply names at most 6 lines, each
with a word-for-word quote and the corrected line. Each proposed change is accepted or rejected on its own:

  A1  each entry is parsed on its own; a bad entry drops only that entry; a reply that is not JSON or was cut at the
      token limit changes nothing. revise.parse_reply cannot be used: it refuses any key besides line and text, so the
      "quote" this reply must carry would make every reply invalid.
  A2  the quote (whitespace collapsed, ** and backticks removed) is a substring of pack_text(chunks), title lines
      included (at least 4 words), or of the evidence of a finding on that very line (at least 3 words), or, with
      cards, of the cards as rendered (at least 3 words).
  A3  the quote shares with the old line (its command unit inside a fence) a Checker.extract item or two content words;
      a quote from the evidence of a finding on the line itself passes (the finding names the line's item), and so
      does a quote from a card whose name stands on the line on identifier boundaries and is the only carded name
      there or the one nearest to the changed words (a card's facts rarely repeat its name).
  A4  the line is not a heading, a table separator, a fence marker, the last line of a draft cut at the token limit, a
      "General knowledge (not in the excerpts):" line, or a line that copies the question: 40 or more characters in a
      row, the whole line, or a key=value / key: value pair, word for word.
  A5  at most two lines per replacement and no fence marker (revise.validate_patches); outside a fence a line is its own
      command unit (revise.command_unit), so a prose line cannot become two.
  A6  every new item (Checker.extract) and every new number in the new text is in the quote or found in the excerpts
      (Excerpts.has; with cards also in the cards).
  A7  inside a fence, every flag (FLAG_RE) and namespace of the old command unit that no finding names is still there,
      and no token of the unit (split on whitespace, values included) is removed or shortened and no flag replaced,
      unless a finding names it; on a prose line the same for the tokens of its inline code spans.
  A8  a line with a warning (_NEG_BEFORE, _NEG_AFTER, "data loss", "irreversible") keeps every warning phrase and is not
      deleted; a deletion only for a prose line or a whole command unit.

Stage 2: the changes that passed stage 1 are added one at a time in the model's order (changes to the occurrences of
one flagged item go in together, see "group" in the record); a change is dropped when, with the changes already kept,
any of these fail: revise.apply_patches and revise.only_allowed_changed; revise.structure_problems and
revise.yaml_regressions; the re-check by the checker (no new invented or wrong_use item, revise.item_keys; the flagged
item no longer on a changed line; revise.surviving empty for the items the changes touch); checker.find_destructive
(no new destructive match that is not skipped, none turned from skipped to not skipped, counted per pattern).

Failures: nothing after the draft can lose it; an exception gives status error and the draft stands. The checking
call starts before DEADLINE_S and must end within TOTAL_S of the question (its timeout is the time left); otherwise
the draft stands, "not checked". A thinking draft cut by the token limit before its </think> is no draft: the
answer says so and nothing is checked.

Limits: at most 6 changed lines (counted as written: a change may write two), 25 % of the draft's lines and 15 % of its characters; over that nothing changes. A
rejected change leaves the draft's line as it is, with no new mark (decision 4 of 2026-11-23).

Items that prompted the rules (open development data B1 and B2 of M4, never D5, the hidden part or the legacy sets;
the prompt carries no text of any of them): b2-19 (A1: a reply cut at 1,500 tokens while arguing; A2), b1-09 (A2: the
model disputing true findings from memory), b2-25 (A4: the user's own setting changed), b1-24 run 2 (A7: a scope flag
deleted, the reset then hit every partition), b2-02, b1-20 and b1-25 S (stage 2 structure and YAML checks, an
indentation change), b1-23 S and b1-02 D (stage 2 re-check and surviving names), b1-25 D (one bad line rejected six
correct changes: hence each change by itself), b2-20 (a claim repeated in prose stays open: surviving covers names
only; a known limit, not a rule). They are fitted to these items; the pass is judged on other data.
The rules were fitted on B1, B2, pilot2 and T; D5 was not read.

Thinking per call (generators.OpenAIChatGenerator.chat(thinking=...)): the draft may be written with thinking on, the
search-terms call and this call stay off. What the server returns as reasoning stays out of the answer text.

Command (stored answers with their packs; resumable by id; one line per item):
  python -m kafka_sre_rag.verify_pass run --answers IN.jsonl --items ITEMS.jsonl --out OUT.jsonl [--cards] [--limit N]
      [--backend openai --model M [--base-url URL] [--extra-body JSON]] [--dry-run] [--catalogues DIR]
Open files only: hidden (.enc) files and eval/hidden/ are refused.
"""
import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import prompts
from .generators import CallTimeout, EmptyAnswerError
from .checker import FLAG_RE, _NEG_AFTER, _NEG_BEFORE, Excerpts, find_destructive
from .revise import (FENCE_OPEN_RE, FLAGGED, apply_patches, command_unit, fence_lines, finding_view, flagged,
                     item_keys, numbered, occurrences, only_allowed_changed, structure_problems, surviving,
                     validate_patches, yaml_regressions)

VERSION = "verify/1"
LAYOUT = "verify-layout/1"   # question, excerpts, numbered draft, findings (, cards): the text is VERIFY_PROMPT(_CARDS)
MAX_TOKENS = 600
MAX_ENTRIES = 6
MAX_LINES = 6
MAX_LINE_SHARE = 0.25
MAX_CHAR_SHARE = 0.15
DEADLINE_S = 240.0
TOTAL_S = 300.0               # the whole answer: the checking call gets the time left of it as its timeout
THINKING_TOKENS = 1500        # added to the answer's token limit when the draft is written with thinking on
MIN_WORDS = {"excerpts": 4, "finding": 3, "card": 3}
QUESTION_COPY_CHARS = 40
GENERAL = "General knowledge (not in the excerpts):"
NOT_CHECKED = "not checked"

_HEADING = re.compile(r"^\s{0,3}#{1,6}\s")
_TABLE_SEP = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)*\|?\s*$")
_WARN = re.compile(r"\b(?:data loss|irreversibl[ey])\b", re.I)
_NS = re.compile(r"(?:(?<![\w-])-n|(?<![\w-])--namespace)(?:\s+|=)([^\s\\'\"`]+)")
_NS_YAML = re.compile(r"^\s*-?\s*namespace:\s*([^\s#]+)")
_NUM = re.compile(r"(?<![\w.])\d+(?:[.,]\d+)*")
_WORD = re.compile(r"[a-z0-9][a-z0-9_.\-/]*[a-z0-9]")
STOP = frozenset("""the and for that this with from are was were has have had not but you your can will would should
could may might must its into onto than then them they their there here when what which while where who how why all any
each every some such only also more most less least very just use used uses using set sets value values line lines
default defaults""".split())


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def prompt_sha256(cards: bool = False) -> Dict[str, str]:
    return {"system": sha(prompts.VERIFY_SYSTEM),
            "prompt": sha(prompts.VERIFY_PROMPT_CARDS if cards else prompts.VERIFY_PROMPT)}


def norm(s: str) -> str:
    """The form quotes and haystacks are compared in: ** and backticks removed, whitespace collapsed."""
    return " ".join((s or "").replace("**", "").replace("`", "").split())


def content_words(s: str) -> set:
    return {w for w in _WORD.findall(norm(s).lower()) if len(w) >= 3 and w not in STOP}


# ----------------------------------------------------------------------------- prompt
def findings_block(findings: List[Dict[str, Any]]) -> str:
    """The flagged findings, one JSON object per line, in the layout of revise.build_messages."""
    return "\n".join(json.dumps({"line": f["line"], "lines": occurrences(f), "item": f["text"], "problem": f["level"],
                                 "evidence": finding_view(f)[0], "suggestion": finding_view(f)[1]},
                                ensure_ascii=False) for f in findings if f["level"] in FLAGGED)


def build_messages(question: str, text: str, chunks, findings: List[Dict[str, Any]], cards_text: Optional[str] = None):
    """cards_text None: VERIFY_PROMPT; a string (also ""): VERIFY_PROMPT_CARDS with that block."""
    vals = {"QUESTION": question, "EXCERPTS": prompts.pack_text(chunks), "ANSWER": numbered(text),
            "FINDINGS": findings_block(findings), "CARDS": cards_text or ""}
    tpl = prompts.VERIFY_PROMPT if cards_text is None else prompts.VERIFY_PROMPT_CARDS
    user = re.sub(r"<<(QUESTION|EXCERPTS|ANSWER|FINDINGS|CARDS)>>", lambda m: vals[m.group(1)], tpl)
    return [{"role": "system", "content": prompts.VERIFY_SYSTEM}, {"role": "user", "content": user}]


# ----------------------------------------------------------------------------- A1: the reply
def parse_verify_reply(reply: str, n_lines: int, truncated: bool = False):
    """-> (entries or None, reason, dropped). entries: [{"line", "quote", "text"}] in the model's order; dropped:
    [{"entry", "reason"}] for entries refused one by one. None means the whole reply changes nothing."""
    if truncated:
        return None, "reply cut at the token limit", []
    s = (reply or "").strip()
    s = re.sub(r"^```[A-Za-z]*\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    start = min([i for i in (s.find("{"), s.find("[")) if i >= 0], default=-1)
    if start < 0:
        return None, "reply is not JSON", []
    try:
        obj, _ = json.JSONDecoder().raw_decode(s[start:])
    except ValueError:
        return None, "reply is not JSON", []
    rep = obj.get("replacements") if isinstance(obj, dict) else obj
    if not isinstance(rep, list):
        return None, "no replacements list", []
    out, dropped, seen = [], [], set()
    for r in rep:
        why = None
        if not isinstance(r, dict) or not {"line", "quote", "text"} <= set(r):
            why = "entry lacks line, quote or text"
        elif set(r) - {"line", "quote", "text"}:
            why = "entry has keys other than line, quote and text"
        elif isinstance(r["line"], bool) or not isinstance(r["line"], int) or not 1 <= r["line"] <= n_lines:
            why = "entry names a line outside the draft"
        elif not isinstance(r["quote"], str) or not r["quote"].strip():
            why = "quote is not a non-empty string"
        elif r["text"] is not None and not isinstance(r["text"], str):
            why = "text is not a string or null"
        elif r["line"] in seen:
            why = "line %d named twice" % r["line"]
        elif len(out) >= MAX_ENTRIES:
            why = "more than %d entries" % MAX_ENTRIES
        if why:
            dropped.append({"entry": r, "reason": why})
            continue
        seen.add(r["line"])
        out.append({"line": r["line"], "quote": r["quote"], "text": r["text"]})
    return out, "", dropped


# ----------------------------------------------------------------------------- helpers
def new_positions(text: str, patches: List[Dict[str, Any]]) -> Dict[int, List[int]]:
    """Old line number -> the new line numbers it became under apply_patches (empty for a deleted line)."""
    by = {p["line"]: p["text"] for p in patches}
    out, k = {}, 0
    for n in range(1, len(text.split("\n")) + 1):
        if n not in by:
            k += 1
            out[n] = [k]
        elif by[n] is None:
            out[n] = []
        else:
            m = len(by[n].split("\n"))
            out[n] = list(range(k + 1, k + m + 1))
            k += m
    return out


def _warnings(s: str) -> List[str]:
    found = [m.group(0).lower() for rx in (_NEG_BEFORE, _NEG_AFTER, _WARN) for m in rx.finditer(s or "")]
    return sorted(set(found))


def _namespaces(s: str) -> set:
    out = set(_NS.findall(s or ""))
    for line in (s or "").split("\n"):
        m = _NS_YAML.match(line)
        if m:
            out.add(m.group(1).strip("\"'"))
    return out


def _destructive_live(text: str) -> Counter:
    return Counter(m["id"] for m in find_destructive(text) if not m["skipped"])


class Context:
    """What the rules of one draft need, computed once."""

    def __init__(self, question, text, chunks, first, extractor, truncated, cards_text, cards=()):
        self.question, self.text, self.chunks, self.first = question, text, chunks, first
        self.lines = text.split("\n")
        self.n = len(self.lines)
        self.infence = fence_lines(text)
        self.extract = extractor
        self.truncated = bool(truncated)
        self.flagged = flagged(first)
        self.pack = norm(prompts.pack_text(chunks))
        self.excerpts = Excerpts([c["text"] for c in chunks if c.get("text")])
        self.cards_raw = cards_text
        self.cards = norm(cards_text) if cards_text else ""
        # (name, normalised text) of each card as rendered (a card that did not fit the budget is not in the prompt)
        self.card_texts = [(c.ident.split()[-1], norm(c.text())) for c in cards if norm(c.text()) in self.cards]
        self.items_by_line: Dict[int, set] = {}
        for it in extractor(text):
            self.items_by_line.setdefault(it.line, set()).add(it.text)
        last = [i for i, l in enumerate(self.lines, 1) if l.strip()]
        self.last_line = last[-1] if last else self.n

    def unit(self, n: int) -> Tuple[int, int]:
        return command_unit(self.lines, n, self.infence)

    def findings_on(self, n: int) -> List[Dict[str, Any]]:
        a, z = self.unit(n)
        return [f for f in self.flagged if any(a <= o <= z for o in occurrences(f))]

    def findings_on_line(self, n: int) -> List[Dict[str, Any]]:
        """The flagged findings with an occurrence on line n itself (review S3: not merely in its command unit)."""
        return [f for f in self.flagged if n in occurrences(f)]


def _name_spans(name: str, line: str) -> List[Tuple[int, int]]:
    """Where a card's name stands in a line, on identifier boundaries (review S4: `acks` is not in `backs`, a key is not
    in a longer key)."""
    rx = re.compile(r"(?<![\w.\-/])%s(?![\w\-]|\.\w)" % re.escape(name))
    return [m.span() for m in rx.finditer(line)]


def _changed_span(old: str, new: Optional[str]) -> Tuple[int, int]:
    """The part of the old line that the change touches (first to last changed character)."""
    if new is None:
        return 0, len(old)
    ops = [(i1, i2) for tag, i1, i2, _, _ in SequenceMatcher(None, old, new, autojunk=False).get_opcodes()
           if tag != "equal"]
    if not ops:
        return 0, 0
    return ops[0][0], max(i2 for _, i2 in ops)


def card_ties_line(ctx: "Context", names: set, old: str, new: Optional[str]) -> bool:
    """A3's card path (review S4): a card the quote comes from is tied to the line when its name stands on the line on
    identifier boundaries and is the only carded name there, or the carded name nearest to the changed words. Nearest
    is read in the order a value follows its key: a carded name the change touches, else the last one that ends
    before the change, else the first one after it; a tie is not enough."""
    on_line = {name: _name_spans(name, old) for name, _ in ctx.card_texts}
    on_line = {k: v for k, v in on_line.items() if v}
    if not on_line or not names & set(on_line):
        return False
    if len(on_line) == 1:
        return True
    a, z = _changed_span(old, new)
    spans = [(s, e, k) for k, v in on_line.items() for s, e in v]
    touched = {k for s, e, k in spans if s < z and a < e}
    if touched:
        nearest = touched
    else:
        before = [(e, k) for s, e, k in spans if e <= a]
        after = [(s, k) for s, e, k in spans if s >= z]
        if before:
            m = max(e for e, _ in before)
            nearest = {k for e, k in before if e == m}
        else:
            m = min(s for s, _ in after)
            nearest = {k for s, k in after if s == m}
    return len(nearest) == 1 and next(iter(nearest)) in names


_CODE_SPAN = re.compile(r"`([^`\n]+)`")


def _tokens(s: str) -> List[str]:
    return [t for t in (s or "").split() if t != "\\"]


def _named(tok: str, names: set) -> bool:
    t = tok.strip("\"'`,;()")
    return any(n and (n in t or t == n or (len(t) >= 4 and t in n)) for n in names)


def lost_tokens(old: str, new: str, names: set) -> List[str]:
    """A7 (review S5): the tokens of a command (split on whitespace, values included) that the change removes or
    shortens, or flags it replaces by another token, unless a finding names them. A value replaced by a different
    value that is not a part of it is allowed (that is what a correction does); A6 checks where the new value comes
    from."""
    a, b = _tokens(old), _tokens(new)
    lost: List[str] = []
    for tag, i1, i2, j1, j2 in SequenceMatcher(None, a, b, autojunk=False).get_opcodes():
        if tag == "equal" or tag == "insert":
            continue
        if tag == "delete":
            lost += a[i1:i2]
            continue
        olds, news = a[i1:i2], b[j1:j2]
        for k, t in enumerate(olds):
            if k >= len(news):
                lost.append(t)
                continue
            u = news[k]
            if (u != t and u in t) or (t.startswith("-") and u != t):
                lost.append(t)
    return [t for t in lost if not _named(t, names)]


_KV = re.compile(r"(?<![\w.\-])([A-Za-z][\w.\-/]*)\s*(=|:)\s*([^\s,;`'\"]+)")
_LIST_MARK = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")


def question_copy(old: str, question: str) -> Optional[str]:
    """A4's question rule (review S6): the reason when the line copies the question, else None: 40 or more characters
    in a row, the whole line (list marker and backticks removed), or a key=value / key: value pair, word for word."""
    if not (question and old.strip()):
        return None
    m = SequenceMatcher(None, old, question, autojunk=False).find_longest_match(0, len(old), 0, len(question))
    if m.size >= QUESTION_COPY_CHARS:
        return "A4: the line copies %d characters of the question" % m.size
    qn = " ".join(question.replace("`", "").split())
    line = " ".join(_LIST_MARK.sub("", old).replace("`", "").replace("**", "").split())
    if re.search(r"[A-Za-z0-9]", line) and line in qn:
        return "A4: the line is in the question word for word"
    for k, _, v in _KV.findall(old.replace("`", "")):
        if re.search(r"(?<![\w.\-])%s\s*[=:]\s*%s(?![\w.\-])" % (re.escape(k), re.escape(v)), qn):
            return "A4: the line carries the question's setting %s word for word" % k
    return None


# ----------------------------------------------------------------------------- stage 1
def where_quoted(ctx: Context, e: Dict[str, Any]) -> Tuple[Optional[str], str]:
    """A2: (where the quote was found or None, reason)."""
    q = norm(e["quote"])
    words = len(q.split())
    if words and q in ctx.pack and words >= MIN_WORDS["excerpts"]:
        return "excerpts", ""
    for f in ctx.findings_on_line(e["line"]):
        if words >= MIN_WORDS["finding"] and q in norm(finding_view(f)[0]):
            return "finding", ""
    if ctx.cards and words >= MIN_WORDS["card"] and q in ctx.cards:
        return "card", ""
    if q in ctx.pack or (ctx.cards and q in ctx.cards):
        return None, "A2: quote too short (%d words)" % words
    return None, "A2: quote not found in the excerpts%s or the evidence of a finding on the line" % (
        ", the cards" if ctx.cards_raw is not None else "")


def stage1(ctx: Context, e: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
    """Reasons the change is refused by A2 to A8 ([] when it passes) and where its quote was found."""
    why: List[str] = []
    n, new = e["line"], e["text"]
    old = ctx.lines[n - 1]
    a, z = ctx.unit(n)
    infence = n in ctx.infence
    found, r = where_quoted(ctx, e)
    if found is None:
        why.append(r)
    q = norm(e["quote"])
    # A3
    unit_text = "\n".join(ctx.lines[a - 1:z])
    items_here = set().union(*[ctx.items_by_line.get(i, set()) for i in range(a, z + 1)])
    # a quote from a card is tied to the line through the card's name (a card's facts rarely repeat the name; S4: on
    # identifier boundaries, the only or the nearest carded name), and a quote from the evidence of a finding on this
    # very line (S3: where_quoted looks on the line only) is about the item that finding names on it
    card_names = {name for name, t in ctx.card_texts if q in t} if found == "card" else set()
    if found != "finding" and not any(norm(t) and norm(t) in q for t in items_here) \
            and len(content_words(unit_text) & content_words(q)) < 2 \
            and not (card_names and card_ties_line(ctx, card_names, old, new)):
        why.append("A3: the quote shares no checked item and fewer than two content words with the line")
    # A4
    if not infence and _HEADING.match(old):
        why.append("A4: the line is a heading")
    if _TABLE_SEP.match(old) and "-" in old:
        why.append("A4: the line is a table separator")
    if re.match(FENCE_OPEN_RE, old):
        why.append("A4: the line is a fence marker")
    if ctx.truncated and n == ctx.last_line:
        why.append("A4: the last line of a draft cut at the token limit")
    if GENERAL in old:
        why.append("A4: a general-knowledge line")
    qc = question_copy(old, ctx.question)
    if qc:
        why.append(qc)
    # A5
    patch = {"line": n, "text": new}
    why += ["A5: " + w for w in validate_patches(ctx.text, [patch], set(range(1, ctx.n + 1)))]
    if new is not None and len(new.split("\n")) > 1 and (a, z) == (n, n) and not infence:
        why.append("A5: a prose line cannot become two lines")
    # A6
    if new is not None:
        patched = apply_patches(ctx.text, [patch])
        span = new_positions(ctx.text, [patch])[n]
        new_items = {it.text for it in ctx.extract(patched) if it.line in span}
        old_items = ctx.items_by_line.get(n, set())
        for t in sorted(new_items - old_items):
            if not (norm(t) in q or ctx.excerpts.has(t) or (ctx.cards and norm(t) in ctx.cards)):
                why.append("A6: new item %s is in neither the quote nor the excerpts" % t)
        old_nums = set(_NUM.findall(old))
        hay = q + "\n" + ctx.pack + "\n" + ctx.cards
        for num in sorted(set(_NUM.findall(new)) - old_nums):
            if not re.search(r"(?<![\w.])%s(?![\d])" % re.escape(num), hay):
                why.append("A6: new number %s is in neither the quote nor the excerpts" % num)
    # A7
    if infence and new is not None:
        new_unit = "\n".join(ctx.lines[a - 1:n - 1] + new.split("\n") + ctx.lines[n:z])
        named = {f["text"] for f in ctx.findings_on(n)}
        lost = sorted(set(FLAG_RE.findall(unit_text)) - set(FLAG_RE.findall(new_unit)) - named)
        if lost:
            why.append("A7: flag removed from the command: " + ", ".join(lost))
        lost_ns = sorted(_namespaces(unit_text) - _namespaces(new_unit) - named)
        if lost_ns:
            why.append("A7: namespace removed from the command: " + ", ".join(lost_ns))
        gone = lost_tokens(unit_text, new_unit, named)
        if gone:
            why.append("A7: part of the command removed or shortened: " + ", ".join(gone))
    elif new is not None:  # S5: a command in inline code on a prose line
        named = {f["text"] for f in ctx.findings_on_line(n)}
        gone = lost_tokens(" ".join(_CODE_SPAN.findall(old)), " ".join(_CODE_SPAN.findall(new)), named)
        if gone:
            why.append("A7: part of the inline code removed or shortened: " + ", ".join(gone))
    # A8
    warn = _warnings(old)
    if warn and new is None:
        why.append("A8: a line with a warning is not deleted")
    elif warn:
        gone = [w for w in warn if w not in _warnings(new)]
        if gone:
            why.append("A8: warning dropped: " + ", ".join(gone))
    if new is None and infence and (a, z) != (n, n):
        why.append("A8: deletion of part of a command (lines %d-%d)" % (a, z))
    return why, found


# ----------------------------------------------------------------------------- the pass
def new_record(text: str, truncated: bool, cards: bool, ident: Dict[str, Any], max_tokens: int) -> Dict[str, Any]:
    """The record of one pass before anything ran: the draft is the final text."""
    return {
        "verify_version": VERSION, "verify_layout": LAYOUT, "cards_enabled": bool(cards),
        "verify_prompt_sha256": prompt_sha256(cards), "model": ident.get("model"),
        "max_tokens": max_tokens, "thinking": False, "first_pass_text": text, "check_first": None,
        "flagged": None, "draft_truncated": bool(truncated), "cards": None, "patches": [],
        "proposals": [], "dropped": [], "r2": {"status": "not_started", "reasons": []},
        "revised_text": text, "final_text": text, "check_after": None, "limits": None,
        "destructive_before": None, "destructive_after": None,
        "timings": {"check_s": 0.0, "cards_s": 0.0, "call_s": 0.0, "rules_s": 0.0}}


class Verifier:
    """checker: service.AnswerChecker (check(text, chunks) -> dict; .checker.extract for items) or any object with those.
    gen: chat(messages, max_tokens, on_token=None, *, thinking=None, info=None) -> (text, tokens, truncated).
    cards: put the draft's reference cards into the prompt (kb: a kb_lookup.KbLookup; built from the checker's
    catalogues when not given)."""

    def __init__(self, checker, gen, *, cards: bool = False, kb=None, max_tokens: Optional[int] = None,
                 card_chars: Optional[int] = None, extract: Optional[Callable] = None):
        self.checker, self.gen, self.cards = checker, gen, bool(cards)
        self.max_tokens = max_tokens or MAX_TOKENS
        self._kb = kb
        self.card_chars = card_chars
        self._extract = extract

    def extract(self, text):
        if self._extract is not None:
            return self._extract(text)
        return self.checker.checker.extract(text)

    @property
    def kb(self):
        if self._kb is None:
            from .kb_lookup import KbLookup
            self._kb = KbLookup(self.checker.cats, self.checker.directory, verify=True)
        return self._kb

    def cards_for(self, text: str):
        """(the rendered block, the cards of the draft's names) within the budget of kb_lookup.render."""
        from . import kb_lookup
        cs = self.kb.cards(text)
        return kb_lookup.render(cs, self.card_chars or kb_lookup.DEFAULT_MAX_CHARS), cs

    def run(self, question: str, text: str, chunks, *, truncated: bool = False, first: Optional[Dict[str, Any]] = None,
            deadline_at: Optional[float] = None, call_deadline_at: Optional[float] = None) -> Dict[str, Any]:
        """deadline_at: the checking call starts only before it. call_deadline_at: the call itself must end by then (a
        per-call timeout; on timeout the draft stands, status not_checked). Nothing after the draft can lose it
        (review S2): any exception in the check, the cards, the call, the rules or stage 2 gives status error with the
        reason recorded, and the draft is the final text."""
        t0 = time.monotonic()
        rec = new_record(text, truncated, self.cards, getattr(self.gen, "identity", None) or {}, self.max_tokens)
        try:
            self._run(rec, t0, question, text, chunks, truncated, first, deadline_at, call_deadline_at)
        except Exception as e:  # noqa: BLE001 - the draft is kept whatever failed
            rec.update(patches=[], revised_text=text, check_after=None, destructive_after=None)
            for p in rec.get("proposals") or []:
                p["accepted"] = False
            rec["r2"].update(status="error", reasons=["the checking pass failed: " + " ".join(
                ("%s: %s" % (type(e).__name__, e)).split())[:200]])
        return self._done(rec, t0)

    def _run(self, rec, t0, question, text, chunks, truncated, first, deadline_at, call_deadline_at):
        rec["destructive_before"] = dict(_destructive_live(text))
        if first is None:
            first = self.checker.check(text, chunks)
        rec.update(check_first=first, flagged=len(flagged(first)))
        rec["timings"]["check_s"] = time.monotonic() - t0
        call = rec["r2"]   # named as in revise's Reviser record, so revise.public() applies to this record too
        if deadline_at is not None and time.monotonic() > deadline_at:
            call.update(status="not_checked", reasons=["the deadline had passed before the checking call"])
            return
        cards_text, card_list = None, []
        if self.cards:
            t = time.monotonic()
            cards_text, card_list = self.cards_for(text)
            rec["timings"]["cards_s"] = time.monotonic() - t
            rec["cards"] = {"count": len(card_list), "chars": len(cards_text), "sha256": sha(cards_text),
                            "text": cards_text}
        msgs = build_messages(question, text, chunks, first["findings"], cards_text)
        rec["prompt_chars"] = sum(len(m["content"]) for m in msgs)
        kw: Dict[str, Any] = {}
        if call_deadline_at is not None:
            left = call_deadline_at - time.monotonic()
            if left <= 0:
                call.update(status="not_checked", reasons=["no time was left for the checking call"])
                return
            kw["timeout"] = left
        t = time.monotonic()
        info: Dict[str, Any] = {}
        try:
            reply, toks, trunc = self.gen.chat(msgs, self.max_tokens, None, thinking=False, info=info, **kw)
        except CallTimeout:
            rec["timings"]["call_s"] = time.monotonic() - t
            call.update(status="not_checked", reasons=["the checking call did not finish within the time left"])
            return
        except Exception as e:  # the draft is not lost because the check failed; the type only (no server text)
            rec["timings"]["call_s"] = time.monotonic() - t
            call.update(status="error", reasons=["model call failed: " + type(e).__name__])
            return
        rec["timings"]["call_s"] = time.monotonic() - t
        call.update(reply=reply, tokens=toks, truncated=bool(trunc), info=info)
        t = time.monotonic()
        entries, why, dropped = parse_verify_reply(reply, len(text.split("\n")), bool(trunc))
        rec["dropped"] = dropped
        if entries is None:
            call.update(status="cut" if trunc else "not_json", reasons=[why])
        elif not entries:
            call.update(status="no_patch", reasons=["the model named no line" if not dropped else "every entry was malformed"])
        else:
            self._rules(rec, Context(question, text, chunks, first, self.extract, truncated, cards_text, card_list),
                        entries)
        rec["timings"]["rules_s"] = time.monotonic() - t

    def _done(self, rec, t0):
        rec["final_text"] = rec["revised_text"]
        rec["timings"]["total_s"] = time.monotonic() - t0
        return rec

    def _rules(self, rec, ctx: Context, entries):
        call = rec["r2"]
        props = []
        for i, e in enumerate(entries):
            why, found = stage1(ctx, e)
            props.append({"line": e["line"], "was": ctx.lines[e["line"] - 1], "quote": e["quote"], "quote_found": found,
                          "text": e["text"], "stage1": why, "stage2": [], "group": None, "accepted": False})
        rec["proposals"] = props
        passed = [i for i, p in enumerate(props) if not p["stage1"]]
        # changes to the occurrences of one flagged item form one group (added together)
        groups: List[List[int]] = []
        member: Dict[int, int] = {}
        for i in passed:
            keys = {(f["type"], f["text"]) for f in ctx.findings_on(props[i]["line"])}
            g = next((member[j] for j in passed if j in member and keys &
                      {(f["type"], f["text"]) for f in ctx.findings_on(props[j]["line"])}), None)
            if g is None:
                g = len(groups)
                groups.append([])
            groups[g].append(i)
            member[i] = g
        kept: List[int] = []
        after = None
        for g, idx in enumerate(groups):
            for i in idx:
                props[i]["group"] = g
            cand = [props[i] for i in kept + idx]
            patches = [{"line": p["line"], "text": p["text"]} for p in cand]
            reasons, res = self._stage2(ctx, patches)
            if reasons:
                for i in idx:
                    props[i]["stage2"] = reasons
                continue
            kept += idx
            after = res
        if not kept:
            call.update(status="rejected", reasons=["no proposed change passed the rules"])
            return
        patches = [{"line": props[i]["line"], "text": props[i]["text"]} for i in kept]
        new = apply_patches(ctx.text, patches)
        changed_chars = sum(max(len(props[i]["was"]), len(props[i]["text"] or "")) for i in kept)
        n_lines = sum(max(1, len((props[i]["text"] or "").split("\n"))) for i in kept)   # a change may write two
        lim = {"lines": n_lines, "line_share": n_lines / max(ctx.n, 1),
               "char_share": changed_chars / max(len(ctx.text), 1),
               "max": {"lines": MAX_LINES, "line_share": MAX_LINE_SHARE, "char_share": MAX_CHAR_SHARE}}
        rec["limits"] = lim
        over = []
        if lim["lines"] > MAX_LINES:
            over.append("%d lines changed (at most %d)" % (lim["lines"], MAX_LINES))
        if lim["line_share"] > MAX_LINE_SHARE:
            over.append("%.1f %% of the lines changed (at most %.0f %%)" % (100 * lim["line_share"], 100 * MAX_LINE_SHARE))
        if lim["char_share"] > MAX_CHAR_SHARE:
            over.append("%.1f %% of the characters changed (at most %.0f %%)" % (100 * lim["char_share"], 100 * MAX_CHAR_SHARE))
        if over:
            for i in kept:
                props[i]["stage2"] = ["over the limits: " + "; ".join(over)]
            call.update(status="over_limit", reasons=over)
            return
        for i in kept:
            props[i]["accepted"] = True
        rec["patches"] = [{"line": props[i]["line"], "was": props[i]["was"], "text": props[i]["text"],
                           "quote": props[i]["quote"], "quote_found": props[i]["quote_found"]} for i in kept]
        rec["revised_text"] = new
        rec["check_after"] = after
        rec["destructive_after"] = dict(_destructive_live(new))
        call.update(status="accepted", reasons=[])

    def _stage2(self, ctx: Context, patches) -> Tuple[List[str], Optional[Dict[str, Any]]]:
        text = ctx.text
        why: List[str] = []
        new = apply_patches(text, patches)
        changed = {p["line"] for p in patches}
        if not only_allowed_changed(text, new, changed):
            why.append("a line that was not named changed")
        why += structure_problems(text, patches) + yaml_regressions(text, new)
        if why:
            return why, None
        after = self.checker.check(new, ctx.chunks)
        fresh = item_keys(after) - item_keys(ctx.first)
        if fresh:
            why.append("new invented or wrong_use: " + ", ".join(sorted(t for _, t in fresh)))
        pos = new_positions(text, patches)
        moved = {q for n in changed for q in pos[n]}
        touched = [f for f in ctx.flagged if any(o in changed for o in occurrences(f))]
        keys = {(f["type"], f["text"]) for f in touched}
        for f in flagged(after):
            if (f["type"], f["text"]) in keys and moved & set(occurrences(f)):
                why.append("flag remains on a changed line: " + f["text"])
        left = surviving(new, touched)
        if left:
            why.append("flagged name still in the answer: " + ", ".join(left))
        before_d, after_d = _destructive_live(text), _destructive_live(new)
        more = sorted(k for k in after_d if after_d[k] > before_d.get(k, 0))
        if more:
            why.append("new destructive step not warned against: " + ", ".join(more))
        return why, after


_BIG = ("first_pass_text", "revised_text", "final_text", "check_first", "check_after")


def compact(rec: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The record without its texts and full check results (the reply is kept); counts before and after."""
    if rec is None:
        return None
    d = {k: v for k, v in rec.items() if k not in _BIG}
    if d.get("cards"):
        d["cards"] = {k: v for k, v in d["cards"].items() if k != "text"}
    d["counts_first"] = (rec.get("check_first") or {}).get("counts")
    d["counts_after"] = (rec.get("check_after") or {}).get("counts")
    return d


def summary_line(rec: Optional[Dict[str, Any]]) -> str:
    """One line for the output of ask: what the pass did (no item text)."""
    if rec is None:
        return "thorough profile (EXPERIMENTAL, unmeasured): not checked (the checking pass is switched off)"
    st = rec["r2"]["status"]
    props = rec.get("proposals") or []
    if st == "no_draft":
        return ("thorough profile (EXPERIMENTAL, unmeasured): no draft: the model was cut off by the token limit while "
                "thinking; nothing was checked")
    if st == "not_checked":
        why = (rec["r2"].get("reasons") or [""])[0]
        if "deadline" in why:
            return "thorough profile (EXPERIMENTAL, unmeasured): %s: the %d s deadline had passed before the checking call" % (
                NOT_CHECKED, int(DEADLINE_S))
        return "thorough profile (EXPERIMENTAL, unmeasured): %s: the checking call did not finish within the %d s limit" % (
            NOT_CHECKED, int(TOTAL_S))
    if st == "error" and (rec["r2"].get("reasons") or [""])[0].startswith("the checking pass failed"):
        return "thorough profile (EXPERIMENTAL, unmeasured): %s: the checking pass failed; the draft is shown as written" % (
            NOT_CHECKED)
    if st in ("error", "cut", "not_json"):
        return "thorough profile (EXPERIMENTAL, unmeasured): %s: the checking call gave no usable reply (%s)" % (
            NOT_CHECKED, st)
    n_ok = sum(1 for p in props if p["accepted"])
    return ("thorough profile (EXPERIMENTAL, unmeasured): checked against the excerpts%s; %d change%s proposed, %d accepted"
            % (" and reference cards" if rec.get("cards_enabled") else "", len(props), "" if len(props) == 1 else "s", n_ok))


# ----------------------------------------------------------------------------- the profile's draft and check
@dataclass
class Profile:
    """The thorough profile's switches (EXPERIMENTAL, unmeasured). The defaults are the profile itself; thinking only
    (verify=False), check only (thinking=False) and cards off are the separate switches of the evaluation's arms."""
    thinking: bool = True
    verify: bool = True
    cards: bool = True
    deadline_s: float = DEADLINE_S
    thinking_tokens: int = THINKING_TOKENS

    def parts(self) -> str:
        return "thinking %s, checking pass %s%s" % ("on" if self.thinking else "off", "on" if self.verify else "off",
                                                    (", cards %s" % ("on" if self.cards else "off")) if self.verify else "")

    def unmeasured_reason(self) -> str:
        return "answer profile thorough (EXPERIMENTAL, unmeasured: %s)" % self.parts()

    def manifest(self) -> Dict[str, Any]:
        return {"name": "thorough", "thinking": self.thinking, "verify": self.verify,
                "cards": self.cards if self.verify else None, "deadline_s": self.deadline_s,
                "thinking_tokens": self.thinking_tokens if self.thinking else 0, "verify_version": VERSION,
                "verify_max_tokens": MAX_TOKENS,
                "verify_prompt_sha256": prompt_sha256(self.cards) if self.verify else None}


NO_DRAFT = ("No answer: the model was cut off by the token limit while thinking, before it wrote the answer "
            "(thorough profile, EXPERIMENTAL). Nothing was checked. Ask again, or without --answer-profile.")


def no_draft_record(profile: "Profile", gen, truncated: bool) -> Dict[str, Any]:
    """The record of a draft cut while thinking (review S1): no draft, nothing checked, no reasoning text kept."""
    rec = new_record(NO_DRAFT, truncated, profile.cards and profile.verify, getattr(gen, "identity", None) or {},
                     MAX_TOKENS)
    rec["first_pass_text"] = ""
    rec["r2"].update(status="no_draft", reasons=["the draft was cut by the token limit while thinking"])
    return rec


def draft_and_check(question: str, msgs, chunks, gen, answer_max_tokens: int, profile: Profile, checker, *, kb=None,
                    t_start: Optional[float] = None) -> Dict[str, Any]:
    """The draft (thinking as the profile says) and, when switched on, the checking pass (thinking off). t_start: the
    moment the question arrived, for the wall-clock deadline (now when None); the checking call must end within TOTAL_S
    of it. Returns text (final), first_pass_text, tokens, truncated, record (None without the pass), draft_info (what
    the server reported), seconds, max_tokens, cut_while_thinking, draft_streamed.

    The draft is streamed internally in every arm of the profile, thinking on or off (review S7: the arms differ only
    in their switch), so the seconds of thinking can be measured; nothing is shown. A draft cut by the token limit
    while thinking is no draft (review S1): the answer says so and nothing is checked.

    Two limits with thinking on (M7 K4, review K2): the total token limit is the answer limit plus the thinking cap (a draft whose
    reasoning uses it all is cut while thinking), and the ANSWER part, counted after the reasoning ended, is stopped at the answer limit
    itself (generators answer_cap), so an answer is never longer than the answer of an arm without thinking; a draft stopped there is
    marked truncated, as such an arm's is. Without thinking no cap is passed: the request and the result are as before."""
    t_start = time.monotonic() if t_start is None else t_start
    info: Dict[str, Any] = {}
    mt = answer_max_tokens + (profile.thinking_tokens if profile.thinking else 0)
    t0 = time.monotonic()
    cut, text, toks, trunc = False, None, 0, False
    try:
        kw = {"answer_cap": answer_max_tokens} if profile.thinking else {}   # M7 K4: the answer part is cut where an arm without thinking is cut
        text, toks, trunc = gen.chat(msgs, mt, lambda s: None, thinking=bool(profile.thinking), info=info, **kw)
        cut = bool(profile.thinking and info.get("cut_while_thinking"))
    except EmptyAnswerError:
        if not (profile.thinking and info.get("cut_while_thinking")):
            raise
        cut, toks, trunc = True, info.get("completion_tokens") or 0, True
    t1 = time.monotonic()
    rec = None
    if cut:
        rec = no_draft_record(profile, gen, trunc)
        text = NO_DRAFT
    elif profile.verify:
        try:
            ver = Verifier(checker, gen, cards=profile.cards, kb=kb)
        except Exception as e:  # noqa: BLE001 - the draft stands (review S2)
            ver = None
            rec = new_record(text, trunc, profile.cards, getattr(gen, "identity", None) or {}, MAX_TOKENS)
            rec["r2"].update(status="error", reasons=["the checking pass failed: " + " ".join(
                ("%s: %s" % (type(e).__name__, e)).split())[:200]])
        if ver is not None:
            rec = ver.run(question, text, chunks, truncated=trunc, deadline_at=t_start + profile.deadline_s,
                          call_deadline_at=t_start + TOTAL_S)
    t2 = time.monotonic()
    return {"text": rec["final_text"] if rec else text, "first_pass_text": "" if cut else text, "tokens": toks,
            "truncated": bool(trunc), "record": rec, "draft_info": info, "max_tokens": mt,
            "cut_while_thinking": cut, "draft_streamed": True,
            "seconds": {"draft": t1 - t0, "thinking": info.get("think_s"), "check": t2 - t1}}


# ----------------------------------------------------------------------------- run over stored answers
class StubGenerator:
    """--dry-run: no model; every checking call gets an empty list."""
    backend = "stub"
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def chat(self, messages, max_tokens, on_token=None, **kw):
        user = messages[-1]["content"] if messages else ""   # M7 K4, dry runs: a failure planted by the six-arm chain's --plant-check-fail
        if any(x and x in user for x in os.environ.get("M7_STUB_CHECK_FAIL", "").split(",")):
            raise RuntimeError("planted failure of the checking call (dry run)")
        return '{"replacements": []}', 0, False


def _refuse_hidden(path: str):
    p = str(path)
    if p.endswith(".enc") or "eval/hidden" in Path(p).resolve().as_posix():
        raise SystemExit("error: verify_pass run reads and writes open files only (%s)" % p)


def _read_jsonl(path) -> List[Dict[str, Any]]:
    if not Path(path).exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def _questions(items_path) -> Dict[str, str]:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    import eval2  # the harness's own question form (question, then "Evidence:" when the item carries it)
    return {it["id"]: eval2.full_question(it) for it in _read_jsonl(items_path)}


STORED_SECONDS = "the stored row's seconds.total (search terms, retrieval and draft), before this check"


def _file_sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _answers_run(path) -> Optional[str]:
    """The run id in the eval2 manifest next to the answers file (None when there is none)."""
    m = Path(str(path) + ".manifest.json")
    try:
        return json.loads(m.read_text(encoding="utf-8")).get("run")
    except (OSError, ValueError, AttributeError):
        return None


def run_command(a, out=sys.stdout) -> int:
    from . import config, evalhooks, service
    from .__main__ import make_generator
    for p in (a.answers, a.items, a.out):
        _refuse_hidden(p)
    if Path(a.out).resolve() == Path(a.answers).resolve():
        raise SystemExit("error: --out is the stored answers file; it is never changed")
    cfg = config.load()
    if not a.dry_run:
        config.verify(cfg=cfg)
    gen = StubGenerator() if a.dry_run else make_generator(argparse.Namespace(
        dry_run=False, backend=a.backend, model=a.model, adapter=a.adapter, base_url=a.base_url, extra_body=a.extra_body))
    try:
        checker = service.AnswerChecker(a.catalogues)
    except service.CatalogueError as e:
        raise SystemExit("error: %s" % e)
    ident = getattr(gen, "identity", None) or {}
    conf = {"verify_version": VERSION, "cards": bool(a.cards), "verify_prompt_sha256": prompt_sha256(bool(a.cards)),
            "max_tokens": MAX_TOKENS, "backend": ident.get("backend"), "model": ident.get("model"),
            "catalogue_hash": checker.catalogue_hash, "answers_file": Path(a.answers).name,
            "answers_sha256": _file_sha256(a.answers), "answers_run": _answers_run(a.answers),
            "deadline": {"deadline_s": DEADLINE_S, "total_s": TOTAL_S, "counted_from": STORED_SECONDS}}
    questions = _questions(a.items)
    rows = [r for r in _read_jsonl(a.answers) if not r.get("error") and r.get("text") and isinstance(r.get("pack"), list)]
    if a.limit:
        rows = rows[: a.limit]
    done_rows = _read_jsonl(a.out)
    for r in done_rows:
        if r.get("config") != conf:
            raise SystemExit("error: %s was written with another configuration; not resuming" % a.out)
    keep = [r for r in done_rows if not r.get("error")]
    if len(keep) != len(done_rows):
        Path(a.out).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in keep), encoding="utf-8")
    done = {r["id"] for r in keep}
    todo = [r for r in rows if r["id"] not in done]
    print("[verify] %d answers, %d already done, %d to do (cards %s)" % (len(rows), len(done), len(todo),
                                                                     "on" if a.cards else "off"), file=out, flush=True)
    ver = Verifier(checker, gen, cards=bool(a.cards))
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    for i, r in enumerate(todo, 1):
        t0 = time.monotonic()
        row = {"id": r["id"], "config": conf, "error": None}
        try:
            if r["id"] not in questions:
                raise KeyError("no item with this id in --items")
            chunks = evalhooks.chunks_by_ids(r["pack"])
            # the stored row's own seconds count against the deadline, as in the in-line profile (review S7)
            spent = (r.get("seconds") or {}).get("total")
            row["stored_seconds"] = spent
            kw = {}
            if isinstance(spent, (int, float)):
                kw = {"deadline_at": t0 + DEADLINE_S - spent, "call_deadline_at": t0 + TOTAL_S - spent}
            else:
                row["deadline"] = "none: the stored row has no seconds.total"
            row.update(ver.run(questions[r["id"]], r["text"], chunks, truncated=bool(r.get("truncated")), **kw))
        except Exception as e:  # recorded; the run continues
            row["error"] = " ".join(("%s: %s" % (type(e).__name__, e)).split())[:300]
        row["seconds"] = {"total": time.monotonic() - t0}
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        props = row.get("proposals") or []
        print("[verify] %d/%d id=%s status=%s proposed=%d accepted=%d total=%.1fs %s" % (
            i, len(todo), r["id"], (row.get("r2") or {}).get("status", "-"), len(props),
            sum(1 for p in props if p["accepted"]), row["seconds"]["total"],
            "ok" if not row["error"] else "FAILED " + row["error"]), file=out, flush=True)
    print("[verify] done", file=out, flush=True)
    return 0


def build_parser():
    ap = argparse.ArgumentParser(prog="python -m kafka_sre_rag.verify_pass",
                                 description="EXPERIMENTAL checking pass of the thorough profile (unmeasured).")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run the checking pass over stored answers with their packs (resumable by id)")
    r.add_argument("--answers", required=True, help="answers file of eval2.py answer (text, pack, truncated per id)")
    r.add_argument("--items", required=True, help="the items file of those answers (for the questions)")
    r.add_argument("--out", required=True, help="records file (JSON Lines; appended, resumable by id)")
    r.add_argument("--cards", action="store_true", help="reference cards of the draft's names in the prompt")
    r.add_argument("--limit", type=int, default=None, help="first N answers only")
    r.add_argument("--catalogues", default=None, help="default: work/m4/catalogues")
    r.add_argument("--backend", choices=["mlx", "openai"], default="openai")
    r.add_argument("--model")
    r.add_argument("--adapter")
    r.add_argument("--base-url", help="openai: server URL (default: environment variable OMLX_URL)")
    r.add_argument("--extra-body", type=json.loads, default=None, metavar="JSON")
    r.add_argument("--dry-run", action="store_true", help="no model: a stub that proposes nothing")
    return ap


def main(argv=None) -> int:
    a = build_parser().parse_args(argv)
    return run_command(a)


if __name__ == "__main__":
    sys.exit(main())
