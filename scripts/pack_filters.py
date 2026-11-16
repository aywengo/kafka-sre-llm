"""Mechanical filters for pack-mode answers (stdlib only)."""
import re

GAP_SENTENCE_RE = re.compile(r"The retrieved documentation does not cover [^\n]*?\.")
LEAK_RE = re.compile(r"(?i)\bthe (material|excerpts?|docs)\b|\bthe (documentation|sources?) (has|have|says|does|do|lists?|gives?|shows?|describes?)\b|\b(documented|undocumented|nothing here|described here|given here|in what (is|was) (described|given|shown)|what I can point you to|from the material)\b|\bthe [\w-]+ documentation\b")


def gap_sentence_count(answer):
    return len(GAP_SENTENCE_RE.findall(answer or ""))


def gap_problem(coverage, answer):
    """None if the gap sentence is used correctly for this coverage, else a problem code."""
    answer = answer or ""
    n = gap_sentence_count(answer)
    if coverage in ("partial", "none"):
        if n == 0:
            return "gap_sentence_missing"
        if n > 1:
            return "gap_sentence_repeated"
        if coverage == "none" and not GAP_SENTENCE_RE.match(answer.lstrip()):
            return "gap_sentence_not_first"
        return None
    if coverage == "full" and n:
        return "gap_sentence_in_full"
    return None


def leak_problem(answer):
    """The matched text of a reference to the material outside the gap sentence, else None."""
    m = LEAK_RE.search(GAP_SENTENCE_RE.sub("", answer or ""))
    return m.group(0) if m else None


def mechanical_problems(row):
    if row.get("skip"):
        return ["skip"]
    if row.get("gen_error"):
        return ["gen_error"]
    answer = row.get("answer") or ""
    problems = []
    g = gap_problem(row.get("coverage"), answer)
    if g:
        problems.append(g)
    leak = leak_problem(answer)
    if leak:
        problems.append("leak:" + leak)
    return problems
