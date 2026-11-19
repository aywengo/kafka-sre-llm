"""WP2b revise step: R1 marks flagged items in place, R2 asks the same model for replacement lines and re-checks them.

The checker (kafka_sre_rag.checker) only flags. This module acts on the flags of one answer:

  R1  for each `invented` or `wrong_use` finding, the evidence is written at the line: a comment line under the
      flagged line inside a code fence (after the whole command when it is wrapped with a trailing backslash; a
      fence whose language has no comments, such as json, gets a bracketed note after the closing fence), a
      bracketed note after the sentence in prose (after the table for a table row). `unverified` findings are not
      marked. No model call, about the time of one check.
  R2  only when at least one invented or wrong_use finding exists and the caller asked for it: one call to the same
      model with the excerpts, the answer with numbered lines and each finding with its catalogue evidence. The reply
      is JSON replacements for the named lines only. The patch is applied and the result checked again; it is accepted
      only if every flagged item is gone, nothing new is invented or wrong_use, and only the named lines changed (one
      line more or less for a wrapped command). Otherwise R1 stands. The prompt is prompts.REVISE_PROMPT.

The output record holds the first-pass text, the findings, the patches, the revised and the final text, both check
results, the timings and the hash of the revise prompt. The first-pass text is never changed in place.

Streaming (decision O2, option c): FenceGate lets prose through as it is written and holds each code fence, from its
opening line, until its check (about 9 ms) has run; the fence is then shown with its R1 marks. Prose findings and R2
patches follow the answer (the note and the corrections block).

Development data of the design (runs/m4/reports/wp2b_revise_design.md section 7), used to build and tune this step and
never to measure it: batch B1 (work/m4/b1/ans_default.jsonl, ans_strict.jsonl, findings_unblinded.jsonl, eval/m4/b1/),
the second pilot (eval/m4/pilot2/, runs/m4/S5B_FINDINGS.jsonl, 48 answers), the 244 stored v3 answers (only to see that
no new false `invented` appears), runs/m4/reports/wp2a_labels_checked.jsonl, and the open half of the mechanical tier
(identifier-precision regression). The measurement (E3) uses the hidden half after E2. The R2 prompt contains no text
of any of these sets.

Stdlib only. Imports kafka_sre_rag.checker (split_markdown) and kafka_sre_rag.prompts.
"""
import difflib
import hashlib
import json
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import prompts
from .checker import INVENTED, WRONG_USE, split_markdown

VERSION = "revise/1"
LAYOUT = "revise-layout/1"   # how the answer and the findings are laid out in the prompt (the text is REVISE_PROMPT)
FLAGGED = (INVENTED, WRONG_USE)
REVISE_MAX_TOKENS = 1500
MARK_MAX = 300

NO_COMMENT_LANGS = frozenset({"json", "jsonc", "xml", "html"})
COMMENT_PREFIX = {"java": "//", "scala": "//", "kotlin": "//", "go": "//", "js": "//", "javascript": "//",
                  "groovy": "//", "sql": "--"}
FENCE_OPEN_RE = r"^\s*(`{3,}|~{3,})"


def prompt_sha256() -> str:
    return hashlib.sha256(prompts.REVISE_PROMPT.encode("utf-8")).hexdigest()


def flagged(check: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [f for f in check.get("findings", []) if f["level"] in FLAGGED]


# ----------------------------------------------------------------------------- R1: marks
def _evidence(f: Dict[str, Any]) -> str:
    ev, lead = f.get("evidence") or "", "`%s` " % f["text"]
    if ev.startswith(lead):
        ev = ev[len(lead):]
    s = "`%s` (%s)" % (f["text"], ev)
    sug = f.get("suggestion") or ""
    if sug and sug not in ev and not (sug.startswith("did you mean") and "nearest real" in ev):
        s += "; " + sug
    s = " ".join(s.split())
    return s if len(s) <= MARK_MAX else s[:MARK_MAX - 3] + "..."


class Marks:
    """Where R1 puts its evidence: `after[n]` are lines inserted after original line n; `inline[n]` are
    (position, text) insertions into original line n."""

    def __init__(self):
        self.after: Dict[int, List[str]] = {}
        self.inline: Dict[int, List[Tuple[int, str]]] = {}

    def __len__(self):
        return sum(len(v) for v in self.after.values()) + sum(len(v) for v in self.inline.values())


def _continued(line: str) -> bool:
    return line.rstrip().endswith("\\")


def plan_marks(text: str, findings: List[Dict[str, Any]]) -> Marks:
    """R1 for the invented and wrong_use findings in `findings` (dicts as the checker's to_dict() gives them)."""
    marks, lines = Marks(), text.split("\n")
    fences = {}
    for seg in split_markdown(text):
        if seg.kind != "fence" or not seg.lines:
            continue
        last = seg.lines[-1][0]
        closed = last < len(lines) and lines[last].strip()[:3] in ("```", "~~~")
        for no, _ in seg.lines:
            fences[no] = (last, last + 1 if closed else last, seg.lang)
    prose: Dict[Tuple[int, int], List[str]] = {}
    for f in findings:
        if f["level"] not in FLAGGED:
            continue
        n = min(max(int(f["line"]), 1), len(lines))
        mark = _evidence(f)
        if n in fences:
            last, close, lang = fences[n]
            if lang in NO_COMMENT_LANGS:
                marks.after.setdefault(close, []).append("[not confirmed: %s]" % mark)
                continue
            end = n
            while end < last and _continued(lines[end - 1]):  # a wrapped command: the comment goes after all of it
                end += 1
            indent = lines[n - 1][:len(lines[n - 1]) - len(lines[n - 1].lstrip())]
            marks.after.setdefault(end, []).append(
                "%s%s not confirmed: %s" % (indent, COMMENT_PREFIX.get(lang, "#"), mark.replace("`", "'")))
            continue
        line = lines[n - 1]
        if line.lstrip().startswith("|"):  # a table row: the note goes under the table
            e = n
            while e < len(lines) and lines[e].lstrip().startswith("|"):
                e += 1
            marks.after.setdefault(e, []).append("[not confirmed: %s]" % mark)
            continue
        sentence = f.get("sentence") or ""
        idx = line.find(sentence) if sentence else -1
        pos = idx + len(sentence) if idx >= 0 else len(line.rstrip())
        prose.setdefault((n, pos), []).append(mark)
    for (n, pos), ms in prose.items():
        marks.inline.setdefault(n, []).append((pos, " [not confirmed: %s]" % "; ".join(ms)))
    return marks


def render_range(lines: List[str], marks: Marks, lo: int = 1, hi: Optional[int] = None) -> List[str]:
    """Original lines lo..hi (1-based, inclusive) with the marks applied."""
    hi = len(lines) if hi is None else min(hi, len(lines))
    out = []
    for n in range(lo, hi + 1):
        line = lines[n - 1]
        for pos, s in sorted(marks.inline.get(n, []), reverse=True):
            line = line[:pos] + s + line[pos:]
        out.append(line)
        out.extend(marks.after.get(n, []))
    return out


def mark_in_place(text: str, findings: List[Dict[str, Any]]) -> str:
    """R1: the text with the evidence of each invented or wrong_use finding at its line. Unchanged when there is none."""
    marks = plan_marks(text, findings)
    if not len(marks):
        return text
    return "\n".join(render_range(text.split("\n"), marks))


# ----------------------------------------------------------------------------- R2: patch
def numbered(text: str) -> str:
    return "\n".join("%d| %s" % (i, l) for i, l in enumerate(text.split("\n"), 1))


def build_messages(text: str, chunks, findings: List[Dict[str, Any]]):
    fl = "\n".join(json.dumps({"line": f["line"], "item": f["text"], "problem": f["level"],
                               "evidence": f.get("evidence") or "", "suggestion": f.get("suggestion") or ""},
                              ensure_ascii=False) for f in findings if f["level"] in FLAGGED)
    vals = {"EXCERPTS": prompts.pack_text(chunks), "ANSWER": numbered(text), "FINDINGS": fl}
    user = re.sub(r"<<(EXCERPTS|ANSWER|FINDINGS)>>", lambda m: vals[m.group(1)], prompts.REVISE_PROMPT)
    return [{"role": "system", "content": prompts.SYSTEM_PROMPT}, {"role": "user", "content": user}]


def parse_reply(reply: str, n_lines: int) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """-> (patches, "") or (None, reason). patches: [{"line": int, "text": str or None}], one per line at most."""
    s = (reply or "").strip()
    s = re.sub(r"^```[A-Za-z]*\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    start = min([i for i in (s.find("{"), s.find("[")) if i >= 0], default=-1)
    if start < 0:
        return None, "reply is not JSON"
    try:
        obj, _ = json.JSONDecoder().raw_decode(s[start:])
    except ValueError:
        return None, "reply is not JSON"
    rep = obj.get("replacements") if isinstance(obj, dict) else obj
    if not isinstance(rep, list):
        return None, "no replacements list"
    out, seen = [], set()
    for r in rep:
        if not isinstance(r, dict) or set(r) - {"line", "text"} or "line" not in r or "text" not in r:
            return None, "replacement entry is malformed"
        ln, tx = r["line"], r["text"]
        if isinstance(ln, bool) or not isinstance(ln, int) or not (1 <= ln <= n_lines):
            return None, "replacement names a line outside the answer"
        if tx is not None and not isinstance(tx, str):
            return None, "replacement text is not a string or null"
        if ln in seen:
            return None, "line %d replaced twice" % ln
        seen.add(ln)
        out.append({"line": ln, "text": tx})
    return out, ""


def allowed_lines(text: str, findings: List[Dict[str, Any]]) -> set:
    """The lines R2 may change: the lines the findings name, and for a wrapped command (trailing backslash) the one
    line before or after that belongs to the same command."""
    lines, allowed = text.split("\n"), set()
    for f in findings:
        if f["level"] not in FLAGGED:
            continue
        n = f["line"]
        if not 1 <= n <= len(lines):
            continue
        allowed.add(n)
        if n < len(lines) and _continued(lines[n - 1]):
            allowed.add(n + 1)
        if n > 1 and _continued(lines[n - 2]):
            allowed.add(n - 1)
    return allowed


def apply_patches(text: str, patches: List[Dict[str, Any]]) -> str:
    by = {p["line"]: p["text"] for p in patches}
    out = []
    for n, line in enumerate(text.split("\n"), 1):
        if n not in by:
            out.append(line)
        elif by[n] is not None:
            out.extend(by[n].split("\n"))
    return "\n".join(out)


def validate_patches(text: str, patches, allowed: set) -> List[str]:
    """Reasons the patches are refused before they are applied; [] when they are fine."""
    why, lines = [], text.split("\n")
    for p in patches:
        if p["line"] not in allowed:
            why.append("line %d is not named by a finding" % p["line"])
        if p["text"] is not None:
            new = p["text"].split("\n")
            if len(new) > 2:
                why.append("line %d: replacement longer than two lines" % p["line"])
            if any(re.match(FENCE_OPEN_RE, l) for l in new):
                why.append("line %d: replacement contains a code fence marker" % p["line"])
    return why


def only_allowed_changed(old: str, new: str, allowed: set) -> bool:
    a, b = old.split("\n"), new.split("\n")
    for tag, i1, i2, _, _ in difflib.SequenceMatcher(None, a, b, autojunk=False).get_opcodes():
        if tag == "equal":
            continue
        if i1 == i2:  # a pure insertion must sit next to a named line
            if i1 not in allowed and i1 + 1 not in allowed:
                return False
        elif any(n not in allowed for n in range(i1 + 1, i2 + 1)):
            return False
    return True


def item_keys(check: Dict[str, Any]) -> set:
    return {(f["type"], f["text"]) for f in flagged(check)}


def corrections_block(patches: List[Dict[str, Any]]) -> str:
    """The text that follows a streamed answer when R2's patch was accepted ('' without patches)."""
    if not patches:
        return ""
    out = ["Corrections to the answer above (a name the catalogues contradict was replaced; each change was checked again):"]
    for p in patches:
        out.append("Line %d was:  %s" % (p["line"], p["was"]))
        out.append("Line %d now:  %s" % (p["line"], "(removed)" if p["text"] is None else p["text"].replace("\n", " / ")))
    return "\n".join(out)


class Reviser:
    """check(text, chunks) -> the checker's result as a dict with `findings` and `note` (service.AnswerChecker.check).
    gen is the model for R2 (chat(messages, max_tokens) -> (text, tokens, truncated)); None means R1 only."""

    def __init__(self, check: Callable, gen=None, max_tokens: Optional[int] = None):
        self.check, self.gen = check, gen
        self.max_tokens = max_tokens or REVISE_MAX_TOKENS

    def revise(self, text: str, chunks, *, r2: bool = False, first: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if r2 and self.gen is None:
            raise ValueError("R2 needs a model")
        t0 = time.monotonic()
        if first is None:
            first = self.check(text, chunks)
        t_first = time.monotonic() - t0
        bad = flagged(first)
        rec: Dict[str, Any] = {
            "revise_version": VERSION, "r2_enabled": bool(r2), "first_pass_text": text, "check_first": first,
            "flagged": len(bad), "patches": [], "r2": {"status": "off" if not r2 else "not_needed", "reasons": []},
            "revised_text": text, "check_after": None, "revise_prompt_sha256": prompt_sha256(), "revise_layout": LAYOUT,
            "timings": {"check_s": t_first, "r2_s": 0.0, "recheck_s": 0.0}}
        accepted = False
        if r2 and bad:
            accepted = self._r2(text, chunks, first, bad, rec)
        if accepted:
            rec["final_text"] = rec["revised_text"]
            rec["marks"] = 0
        else:
            marks = plan_marks(text, first["findings"])
            rec["marks"] = len(marks)
            rec["final_text"] = "\n".join(render_range(text.split("\n"), marks)) if len(marks) else text
        t = rec["timings"]
        t["total_s"] = time.monotonic() - t0
        return rec

    def _r2(self, text, chunks, first, bad, rec) -> bool:
        r2 = rec["r2"]
        n_lines = len(text.split("\n"))
        t = time.monotonic()
        try:
            reply, toks, trunc = self.gen.chat(build_messages(text, chunks, bad), self.max_tokens)
        except Exception as e:  # the answer is not lost because the second call failed; the type only (no server text)
            rec["timings"]["r2_s"] = time.monotonic() - t
            r2.update(status="error", reasons=["model call failed: " + type(e).__name__])
            return False
        rec["timings"]["r2_s"] = time.monotonic() - t
        r2.update(reply=reply, tokens=toks, truncated=bool(trunc))
        patches, why = parse_reply(reply, n_lines)
        if patches is None:
            r2.update(status="rejected", reasons=[why])
            return False
        if not patches:
            r2.update(status="no_patch", reasons=["the model named no line to change"])
            return False
        lines = text.split("\n")
        rec["patches"] = [{"line": p["line"], "was": lines[p["line"] - 1], "text": p["text"]} for p in patches]
        allowed = allowed_lines(text, bad)
        reasons = validate_patches(text, patches, allowed)
        new = apply_patches(text, patches) if not reasons else None
        if new is not None and not only_allowed_changed(text, new, allowed):
            reasons.append("a line that no finding names changed")
        if not reasons:
            t1 = time.monotonic()
            after = self.check(new, chunks)
            rec["timings"]["recheck_s"] = time.monotonic() - t1
            rec["check_after"] = after
            left = item_keys(first) & item_keys(after)
            fresh = item_keys(after) - item_keys(first)
            if left:
                reasons.append("flag remains: " + ", ".join(sorted(t for _, t in left)))
            if fresh:
                reasons.append("new invented or wrong_use: " + ", ".join(sorted(t for _, t in fresh)))
        if reasons:
            r2.update(status="rejected", reasons=reasons)
            return False
        r2.update(status="accepted", reasons=[])
        rec["revised_text"] = new
        return True


_BIG = ("first_pass_text", "revised_text", "final_text", "check_first", "check_after")


def public(rec: Dict[str, Any]) -> Dict[str, Any]:
    """The record without its texts and full check results (for a response that already carries the text)."""
    d = {k: v for k, v in rec.items() if k not in _BIG}
    d["r2"] = {k: v for k, v in rec["r2"].items() if k != "reply"}
    d["counts_first"] = rec["check_first"].get("counts")
    d["counts_after"] = (rec["check_after"] or {}).get("counts")
    return d


# ----------------------------------------------------------------------------- streaming
class FenceGate:
    """Between the model's stream and the client: prose passes at once, a code fence is held, from its opening line
    to its closing line (or the end of the answer), until the checker has run on the text so far; it is then shown
    with its R1 marks. `emit(piece)` gets what may be shown. Fence recognition is that of the checker
    (split_markdown), so the lines of a fence here are the lines it judges. A failure of the check withholds the
    fence (a notice is shown in its place); an unchecked fence is never shown."""
    WITHHELD = "[a code block was withheld because the answer check could not run]\n"

    def __init__(self, emit: Callable[[str], None], check: Callable, chunks_fn: Callable[[], Any]):
        self.emit, self.check, self.chunks_fn = emit, check, chunks_fn
        self.raw: List[str] = []       # every character received
        self.out: List[str] = []       # everything shown
        self.nl = 0                    # newlines received so far
        self.state = "undecided"       # undecided: start of a line not yet known; prose; fence
        self.cur = ""                  # the undecided line so far
        self.fence = ""                # the fence marker of the open fence
        self.fline = ""                # the current line inside the fence
        self.flines: List[str] = []    # complete lines of the open fence
        self.fstart = 0

    def _show(self, s: str):
        if s:
            self.out.append(s)
            self.emit(s)

    def feed(self, piece: str):
        buf = []
        for ch in piece or "":
            self.raw.append(ch)
            if self.state == "fence":
                self.fline += ch
                if ch == "\n":
                    line = self.fline[:-1]
                    self.flines.append(line)
                    self.fline = ""
                    self.nl += 1
                    if len(self.flines) > 1 and line.strip().startswith(self.fence):
                        self._release(buf, terminated=True)
                continue
            if self.state == "prose":
                buf.append(ch)
                if ch == "\n":
                    self.state, self.nl = "undecided", self.nl + 1
                continue
            self.cur += ch
            if ch == "\n":
                buf.append(self.cur)
                self.cur, self.nl = "", self.nl + 1
                continue
            s = self.cur.lstrip()
            if s == "" or (len(s) < 3 and set(s) in ({"`"}, {"~"})):
                continue  # could still turn out to be a fence marker
            if s.startswith("```") or s.startswith("~~~"):
                self.state, self.fence = "fence", s[0] * 3
                self.fstart = self.nl + 1
                self.fline = self.cur
                self.flines, self.cur = [], ""
                continue
            buf.append(self.cur)
            self.cur, self.state = "", "prose"
        self._show("".join(buf))

    def _release(self, buf: List[str], terminated: bool):
        self._show("".join(buf))  # the prose before the fence is out before the check runs
        buf.clear()
        lines = self.flines + ([self.fline] if self.fline else [])
        text = "".join(self.raw)
        lo, hi = self.fstart, self.fstart + len(lines) - 1
        try:
            res = self.check(text, self.chunks_fn())
            mine = [f for f in res["findings"] if lo <= f["line"] <= hi]
            marks = plan_marks(text, mine)
            shown = "\n".join(render_range(text.split("\n"), marks, lo, hi)) + ("\n" if terminated else "")
        except Exception:
            shown = self.WITHHELD
        buf.append(shown)
        self.state, self.fence, self.fline, self.flines = "undecided", "", "", []

    def finish(self):
        buf = []
        if self.state == "fence":
            self._release(buf, terminated=False)
        elif self.cur:
            buf.append(self.cur)
            self.cur = ""
        self._show("".join(buf))

    def text(self) -> str:
        return "".join(self.out)
