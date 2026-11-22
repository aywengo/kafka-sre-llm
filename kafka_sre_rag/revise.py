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

WP2b T2b changes (acceptance rule with wrapped commands as one unit, repeated names, the finding's own line, the
"nearest real" filter, the REVISE_PROMPT wording) were prompted by these open development items: b1-02, b1-09, b1-20,
b1-23, b1-24, b1-25, b2-02, b2-19. They are fitted to them and are judged on the hidden half, not on these items.

Stdlib only (PyYAML is used for the YAML check only when installed). Imports kafka_sre_rag.checker (split_markdown) and kafka_sre_rag.prompts.
"""
import difflib
import hashlib
import json
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import prompts
from .checker import INVENTED, WRONG_USE, find_destructive, split_markdown

VERSION = "revise/2"
LAYOUT = "revise-layout/2"   # how the answer and the findings are laid out in the prompt (the text is REVISE_PROMPT)
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
NEAR_RE = re.compile(r";? nearest real: .*$")


def finding_view(f: Dict[str, Any]) -> Tuple[str, str]:
    """(evidence, suggestion) of a finding as R1 and R2 show them. A finding that carries `near` (the checker's names of
    the same kind and scope, close in spelling) shows a "nearest real" list only from it; with none it says only that the
    name does not exist. A finding without `near` is shown as the checker wrote it."""
    ev, sug = f.get("evidence") or "", f.get("suggestion") or ""
    near = f.get("near")
    if near is None:
        return ev, sug
    ev = NEAR_RE.sub("", ev)
    if near:
        ev += "; nearest real: " + ", ".join("`%s`" % n for n in near)
        sug = "did you mean `%s`?" % near[0]
    else:
        sug = "" if sug.startswith("did you mean") else sug
    return ev, sug


def _evidence(f: Dict[str, Any]) -> str:
    ev, sug = finding_view(f)
    lead = "`%s` " % f["text"]
    if ev.startswith(lead):
        ev = ev[len(lead):]
    s = "`%s` (%s)" % (f["text"], ev)
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


def occurrences(f: Dict[str, Any]) -> List[int]:
    """Every line the flagged item occurs on (the checker's `lines`; `line` alone for an older record)."""
    return list(f.get("lines") or [f["line"]])


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
        mark = _evidence(f)
        for ln in occurrences(f):   # every occurrence is marked, not only the first
            n = min(max(int(ln), 1), len(lines))
            if n in fences:
                last, close, lang = fences[n]
                if lang in NO_COMMENT_LANGS:
                    text_ = "[not confirmed: %s]" % mark
                    if text_ not in marks.after.setdefault(close, []):
                        marks.after[close].append(text_)
                    continue
                end = n
                while end < last and _continued(lines[end - 1]):  # a wrapped command: the comment goes after all of it
                    end += 1
                indent = lines[n - 1][:len(lines[n - 1]) - len(lines[n - 1].lstrip())]
                text_ = "%s%s not confirmed: %s" % (indent, COMMENT_PREFIX.get(lang, "#"), mark.replace("`", "'"))
                if text_ not in marks.after.setdefault(end, []):
                    marks.after[end].append(text_)
                continue
            line = lines[n - 1]
            if line.lstrip().startswith("|"):  # a table row: the note goes under the table
                e = n
                while e < len(lines) and lines[e].lstrip().startswith("|"):
                    e += 1
                text_ = "[not confirmed: %s]" % mark
                if text_ not in marks.after.setdefault(e, []):
                    marks.after[e].append(text_)
                continue
            sentence = f.get("sentence") or ""
            idx = line.find(sentence) if sentence else -1
            pos = idx + len(sentence) if idx >= 0 else len(line.rstrip())
            if mark not in prose.setdefault((n, pos), []):
                prose[(n, pos)].append(mark)
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


DESTRUCTIVE_TAG = "destructive step"   # the words every mark of the destructive-step option starts with


def _destructive_text(m: Dict[str, Any]) -> str:
    return "%s: %s; %s" % (DESTRUCTIVE_TAG, m["name"], m["consequence"])


def plan_destructive_marks(text: str, matches: Optional[List[Dict[str, Any]]] = None) -> Marks:
    """Marks (same placement as R1) for the destructive steps that find_destructive() lists and does not skip.
    EXPERIMENTAL; nothing is removed or reworded."""
    matches = find_destructive(text) if matches is None else matches
    marks, lines = Marks(), text.split("\n")
    prose: Dict[Tuple[int, int], List[str]] = {}
    for m in matches:
        if m["skipped"]:
            continue
        note = _destructive_text(m)
        n = m["line"]
        if m["kind"] == "fence":
            lang, end = m["lang"], m["end"]
            seg_last = end
            while seg_last < len(lines) and not lines[seg_last].strip()[:3] in ("```", "~~~"):
                seg_last += 1
            if lang in NO_COMMENT_LANGS:
                close = seg_last + 1 if seg_last < len(lines) else seg_last
                t = "[%s]" % note
                if t not in marks.after.setdefault(close, []):
                    marks.after[close].append(t)
                continue
            indent = lines[n - 1][:len(lines[n - 1]) - len(lines[n - 1].lstrip())]
            t = "%s%s %s" % (indent, COMMENT_PREFIX.get(lang, "#"), note.replace("`", "'"))
            if t not in marks.after.setdefault(end, []):
                marks.after[end].append(t)
        elif m["kind"] == "table":
            e = n
            while e < len(lines) and lines[e].lstrip().startswith("|"):
                e += 1
            t = "[%s]" % note
            if t not in marks.after.setdefault(e, []):
                marks.after[e].append(t)
        else:
            if note not in prose.setdefault((n, m["pos"]), []):
                prose[(n, m["pos"])].append(note)
    for (n, pos), ms in prose.items():
        marks.inline.setdefault(n, []).append((pos, " [%s]" % "; ".join(ms)))
    return marks


def mark_destructive(text: str) -> Tuple[str, int, List[Dict[str, Any]]]:
    """(text with the marks, how many marks were written, the matches). Unchanged text when there is no mark."""
    matches = find_destructive(text)
    marks = plan_destructive_marks(text, matches)
    if not len(marks):
        return text, 0, matches
    return "\n".join(render_range(text.split("\n"), marks)), len(marks), matches


def strip_destructive_marks(text: str) -> str:
    """The inverse of mark_destructive (used by the tests): the text without any destructive-step mark."""
    out = []
    for line in text.split("\n"):
        if re.match(r"^\s*(?:#|//|--)\s%s: " % DESTRUCTIVE_TAG, line) or re.match(r"^\[%s: .*\]$" % DESTRUCTIVE_TAG, line):
            continue
        out.append(re.sub(r" \[%s: [^\]]*\]" % DESTRUCTIVE_TAG, "", line))
    return "\n".join(out)


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
    fl = "\n".join(json.dumps({"line": f["line"], "lines": occurrences(f), "item": f["text"], "problem": f["level"],
                               "evidence": finding_view(f)[0], "suggestion": finding_view(f)[1]},
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


def fence_lines(text: str) -> set:
    """Line numbers (1-based) that lie inside a fenced block."""
    return {no for seg in split_markdown(text) if seg.kind == "fence" for no, _ in seg.lines}


def command_unit(lines: List[str], n: int, infence: set) -> Tuple[int, int]:
    """(first, last) line of the command that line n belongs to: lines joined by a trailing backslash form one unit.
    Outside a fenced block a line is a unit of its own."""
    if n not in infence:
        return n, n
    a, z = n, n
    while a > 1 and (a - 1) in infence and _continued(lines[a - 2]):
        a -= 1
    while z < len(lines) and (z + 1) in infence and _continued(lines[z - 1]):
        z += 1
    return a, z


def allowed_lines(text: str, findings: List[Dict[str, Any]]) -> set:
    """The lines R2 may change: every line that holds a flagged item (the same (type, text) on several lines counts
    each of them), and all lines of a wrapped command (trailing backslash) that holds one."""
    lines, allowed, infence = text.split("\n"), set(), fence_lines(text)
    for f in findings:
        if f["level"] not in FLAGGED:
            continue
        for n in occurrences(f):
            if not 1 <= n <= len(lines):
                continue
            a, z = command_unit(lines, n, infence)
            allowed.update(range(a, z + 1))
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


def _indent(line: str) -> str:
    return line[:len(line) - len(line.lstrip())]


def structure_problems(text: str, patches: List[Dict[str, Any]]) -> List[str]:
    """Reasons a patch damages the shape of a fenced block: a wrapped command left with an orphan continuation line or a
    dangling backslash (its first line removed, its last line removed or given a backslash), the leading whitespace of a
    line changed, or a YAML block that parsed before and does not now. [] when the shape is kept."""
    lines, infence, why = text.split("\n"), fence_lines(text), []
    by = {p["line"]: p["text"] for p in patches}
    units = {}
    for n in by:
        if n in infence:
            units[command_unit(lines, n, infence)] = None
    for (a, z) in units:
        new, first_gone = [], False
        for i in range(a, z + 1):
            if i not in by:
                new.append(lines[i - 1])
            elif by[i] is None:
                first_gone = first_gone or i == a
            else:
                new.extend(by[i].split("\n"))
        if not new:
            continue   # the whole command was removed
        if first_gone:
            why.append("lines %d-%d: the first line of the command was removed and the rest stays" % (a, z))
        elif any(not _continued(l) for l in new[:-1]) or (_continued(new[-1]) and not _continued(lines[z - 1])):
            why.append("lines %d-%d: the command is left with an orphan continuation line or a dangling backslash" % (a, z))
    for n, t in by.items():
        if t is None or n not in infence:
            continue
        a, z = command_unit(lines, n, infence)
        okws = {_indent(lines[i - 1]) for i in range(a, z + 1)}
        new = t.split("\n")
        if _indent(new[0]) != _indent(lines[n - 1]) or any(_indent(l) not in okws for l in new[1:]):
            why.append("line %d: the leading whitespace changed" % n)
    return why


def yaml_regressions(old: str, new: str) -> List[str]:
    """A fenced YAML block that parsed in `old` and does not in `new` (blocks paired in order). Needs PyYAML; without it
    the rule cannot be applied and nothing is reported."""
    try:
        import yaml
    except ImportError:
        return []

    def blocks(t):
        return ["\n".join(l for _, l in seg.lines) for seg in split_markdown(t) if seg.kind == "fence" and seg.lang in ("yaml", "yml")]

    def parses(b):
        try:
            list(yaml.safe_load_all(b))
            return True
        except Exception:
            return False
    o, n = blocks(old), blocks(new)
    if len(o) != len(n):
        return ["the number of YAML blocks changed"]
    return ["YAML block %d parsed before the patch and does not now" % (i + 1)
            for i, (a, b) in enumerate(zip(o, n)) if parses(a) and not parses(b)]


NAME_TYPES = frozenset({"cli_flag", "cli_tool", "config_key", "field_path", "annotation", "label", "type_name", "error_name"})


def surviving(new: str, flagged_findings: List[Dict[str, Any]]) -> List[str]:
    """Flagged names (type, text) that still stand anywhere in `new`, on a line the checker saw or not. Only names are
    searched (a value such as `true` is left to the re-check); a longer name that starts with it (`--topic` in
    `--topic-list`) is not an occurrence. A flag is a flag of one tool, so inside a code fence it is left to the
    re-check, which knows the tool of each command (`--topic` is real for kafka-topics.sh); in prose it is searched."""
    out, lines, infence = [], new.split("\n"), None
    for f in flagged_findings:
        if f["type"] not in NAME_TYPES or not f["text"]:
            continue
        pat = re.compile(r"(?<![\w.\-/])" + re.escape(f["text"]) + r"(?![\w\-]|\.\w)")
        if f["type"] == "cli_flag":
            infence = fence_lines(new) if infence is None else infence
            hay = "\n".join(l for n, l in enumerate(lines, 1) if n not in infence)
        else:
            hay = new
        if pat.search(hay) and f["text"] not in out:
            out.append(f["text"])
    return out


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
        if new is not None:
            reasons += structure_problems(text, patches) + yaml_regressions(text, new)
            left_txt = surviving(new, bad)
            if left_txt:
                reasons.append("flagged name still in the answer: " + ", ".join(left_txt))
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
            mine = []
            for f in res["findings"]:   # every occurrence inside this fence is marked
                here = [n for n in occurrences(f) if lo <= n <= hi]
                if here:
                    mine.append(dict(f, line=here[0], lines=here))
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
