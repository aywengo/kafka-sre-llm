#!/usr/bin/env python3
"""Score tier A answers of the M4 evaluation (programmatic checks, no model).

Input : an items file (scripts/eval2_items_a1.py writes eval/m4/build/tier_a1.jsonl) and an answers
        file with rows {id, text, ...}; other keys are ignored. If an id occurs twice the later row
        wins. An item without an answer counts as failed and is reported as `missing`.
Output: a score file, one row per item:
          {id, checks_passed, checks_total, all_pass, format_ok, results: [...], identifiers: {...}}
        and a summary (printed, and written with --summary): the all-pass rate is the main score,
        the share of checks passed is secondary; both per kind, domain, failure class and overlap
        band, with the count and a Wilson 95 % interval. Format failures (no `ANSWER:` line, no
        command line) are counted separately.

Check types of slice a3 (scripts/eval2_checks_a3.py, version 3 of this scorer): semantic_rule (oracles netpol_allows,
restart_annotation, config_rule, properties_parse, pdb_evicts), cmd_order, number_within, declines, no_invented_value,
forbidden_keys. Items without them score as before.

Check types of slice A2 (write a resource, fix the manifest, write the command, version trap):
  yaml_document, yaml_one_doc (exactly one document with a kind, of the requested kind and apiVersion),
  yaml_strict (strict validation of that document by Catalogues.validate_resource: an unknown field is a
  failure unless the schema preserves unknown fields at that path), yaml_fields_equal, yaml_fields_absent,
  yaml_config_keys (the keys of a free-form config map must exist for the Kafka configuration scope; a key the
  catalogue cannot classify is not counted as wrong), old_value_forbidden, has_kafka_command, kcmd_spec (a
  command line for the tool with the required flags and values, none of the forbidden flags), kcmd_known_flags
  (no flag absent from the tool's complete option list), kcmd_constraints (the `available only with` and
  `cannot go with` rules of the tool's code, for the options spelled as flags).

Identifier precision is computed over the whole answer text, independent of the item's own
checks: every code-like token that can be classified is looked up in the catalogues:
  Kafka configuration keys (dotted names, also the keys under `config:` in a YAML manifest),
  Connect REST method and path pairs, `argocd` commands and flags, Kafka protocol error names,
  Strimzi, Argo CD and Kubernetes field paths in YAML manifests and dotted spec paths, `strimzi.io/` annotations.
The summary reports known, unknown and not covered. "unknown" comes only from a catalogue (or a
tool) that is marked complete; everything else is "not covered" and is never called invented.

Decline outcome (decision J5): beside the unchanged all-pass rate every answer gets one of pass / correct_decline /
wrong_decline / wrong / abstained, from the covering chunks of the item in the frozen index (work/v3/chunks_s1.jsonl,
read-only, its sha256 checked against runs/v3/INDEX_FROZEN.json; a mismatch skips this part) and the answer's pack.

This scorer shares only the catalogue files, kafka_sre_rag/catalogue.py and, for the decline outcome, the frozen
index file with anything else. It
does not import from a runtime answer checker, and nothing in kafka_sre_rag/ imports it.

Usage: .venv/bin/python scripts/eval2_score.py --items ITEMS.jsonl --answers ANSWERS.jsonl
           [--scores OUT.jsonl] [--summary OUT.json] [--catalogues DIR]
"""
import argparse
import difflib
import json
import math
import re
import shlex
import sys
from collections import defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues  # noqa: E402

VERSION = "eval2_score/3"
FORMAT_CHECKS = ("answer_line", "has_command", "yaml_document", "has_kafka_command")
ANSWER_RE = re.compile(r"^[ \t>*_`#-]*ANSWER[ \t]*(?:\*\*|__)?[ \t]*:[ \t]*(?:\*\*|__)?[ \t]*(.+?)[ \t]*$", re.M)


# ------------------------------------------------------------------ parsing helpers
def parse_answer_line(text):
    """The value of the last `ANSWER: value` line, or None."""
    m = ANSWER_RE.findall(text or "")
    if not m:
        return None
    v = m[-1].strip()
    v = re.sub(r"(?:\*\*|__)$", "", v).strip()  # a closing bold marker, not a value that ends in `*`
    return v


def norm_val(v):
    v = (v or "").strip()
    v = re.sub(r"^[`'\"“”]+|[`'\"“”]+$", "", v).strip()
    v = re.sub(r"[.;]+$", "", v).strip()
    v = re.sub(r"^[`'\"“”]+|[`'\"“”]+$", "", v).strip()
    return re.sub(r"\s+", " ", v)


def norm_cmp(v, ci):
    v = norm_val(v)
    v = re.sub(r"(?<=\d),(?=\d{3}(?!\d))", "", v)
    return v.lower() if ci else v


def value_equals(parsed, accepted, ci=False):
    """Equal after normalisation; `300000 (5 minutes)` equals 300000 (a gloss in brackets is allowed)."""
    p = norm_cmp(parsed, ci)
    for a in accepted:
        a = norm_cmp(a, ci)
        if p == a or re.fullmatch(re.escape(a) + r"\s*\(.*\)", p):
            return True
    return False


def split_set(v):
    parts = re.split(r"\s*(?:,|;|\||\bor\b|\band\b|/)\s*", norm_val(v).strip("[](){} "))
    return {norm_val(x).lower() for x in parts if norm_val(x)}


VERDICTS = {
    "exists": {"exists", "exist", "it exists", "does exist"},
    "does not exist": {"does not exist", "doesn't exist", "doesnt exist", "not exist", "it does not exist",
                       "does not exists", "nonexistent", "non-existent", "no such key"},
    "not in my sources": {"not in my sources", "not in sources", "not in the sources", "not in my source",
                          "not in my sources."},
}


def norm_verdict(v):
    v = re.sub(r"\s+", " ", norm_val(v).lower())
    for k, syn in VERDICTS.items():
        if v in syn:
            return k
    return None


def upper_snake(v):
    return re.sub(r"[\s\-]+", "_", norm_val(v)).upper()


# ------------------------------------------------------------------ argocd command lines
FENCE_RE = re.compile(r"```[^\n]*\n(.*?)```", re.S)
INLINE_RE = re.compile(r"`([^`\n]*\bargocd\s[^`\n]*)`")
SPLIT_RE = re.compile(r"\s*(?:\|\||&&|\||;|>|<)\s*")


def argocd_lines(text):
    """Command lines starting with `argocd` (code blocks, `$` prompts, inline code), continuation lines joined."""
    cands = []
    text = re.sub(r"\\[ \t]*\n[ \t]*", " ", text or "")  # shell line continuations, in or out of a code block
    for block in FENCE_RE.findall(text):
        cur = ""
        for ln in block.split("\n"):
            s = ln.strip()
            if not s or s.startswith("#"):
                if not cur:
                    continue
            cur = (cur + " " + s).strip() if cur else s
            if cur.endswith("\\"):
                cur = cur[:-1].rstrip()
                continue
            cands.append(cur)
            cur = ""
        if cur:
            cands.append(cur)
    for ln in text.split("\n"):
        s = ln.strip().lstrip("$ ").strip()
        if s.startswith("argocd "):
            cands.append(s)
    cands += INLINE_RE.findall(text)
    out = []
    for c in cands:
        c = c.strip().lstrip("$ ").strip()
        for part in SPLIT_RE.split(c):
            part = part.strip()
            if part.startswith("argocd ") or part == "argocd":
                if part not in out:
                    out.append(part)
    return out


def tokenize(line):
    try:
        return shlex.split(line, comments=True)
    except ValueError:
        return line.split()


def resolve_argocd(line, cats):
    """-> dict(command, positionals, flags, bad_subcommand). `command` is the longest known command prefix."""
    toks = tokenize(line)
    words, rest = [], []
    for t in toks:
        (rest if t.startswith("-") or rest else words).append(t)
    # words: leading non-flag tokens ('argocd', 'app', 'sync', 'my-app'); flags and what follows go to rest
    flags, positionals = [], []
    for t in toks[1:]:
        if t.startswith("--"):
            flags.append(t.split("=")[0])
    cmd = "argocd"
    nonflag = [t for t in toks[1:] if not t.startswith("-")]
    best = 0
    cmds = set(cats.argocd_commands())
    for i in range(1, min(4, len(nonflag)) + 1):
        cand = " ".join(["argocd"] + nonflag[:i])
        if cand in cmds:
            cmd, best = cand, i
    positionals = nonflag[best:]
    group = any(c.startswith(cmd + " ") for c in cmds)
    bad = None
    if group and positionals and cmd != "argocd":
        # `argocd app foo`: the next word is read as a sub-command position
        nxt = positionals[0]
        if not any(c == cmd + " " + nxt for c in cmds):
            bad = cmd + " " + nxt
    elif cmd == "argocd" and nonflag:
        bad = "argocd " + nonflag[0]
    return {"command": cmd, "positionals": positionals, "flags": flags, "tokens": toks, "bad_subcommand": bad}


# ------------------------------------------------------------------ the checks
def check_answer_line(chk, text, cats):
    v = parse_answer_line(text)
    return v is not None, "ANSWER line: %r" % v if v is not None else "no ANSWER: line"


def check_answer_equals(chk, text, cats):
    v = parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    mode = chk.get("norm", "exact")
    if mode == "upper_snake":
        ok = upper_snake(v) in {upper_snake(a) for a in chk["accepted"]}
    else:
        ok = value_equals(v, chk["accepted"], ci=(mode == "ci"))
    return ok, "answer %r, accepted %s" % (v, chk["accepted"][:4])


def check_answer_set(chk, text, cats):
    v = parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    got, want = split_set(v), {norm_val(a).lower() for a in chk["accepted"]}
    return got == want, "answer set %s, wanted %s" % (sorted(got), sorted(want))


def check_answer_verdict(chk, text, cats):
    v = parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    got = norm_verdict(v)
    return got == chk["expected"], "verdict %r (read as %s), expected %s" % (v, got, chk["expected"])


def check_answer_letter(chk, text, cats):
    v = parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    m = re.match(r"^\(?([A-Za-z])\)?(?:[.):\s]|$)", norm_val(v))
    got = m.group(1).upper() if m else None
    return got == chk["expected"], "letter %s, expected %s" % (got, chk["expected"])


def parse_rest_answer(v):
    m = re.match(r"^`?\s*(GET|POST|PUT|DELETE|PATCH|HEAD)\s+(\S+?)`?\s*$", norm_val(v).replace("`", ""), re.I)
    return (m.group(1).upper(), m.group(2)) if m else (None, None)


def check_rest_method(chk, text, cats):
    v = parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    method, _ = parse_rest_answer(v)
    return method == chk["method"], "method %s, expected %s" % (method, chk["method"])


def check_rest_path(chk, text, cats):
    v = parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    _, path = parse_rest_answer(v)
    if path is None:
        return False, "no METHOD path in the ANSWER line"
    tpl = cats.rest_template(path)
    return tpl == chk["template"], "path matches %s, expected %s" % (tpl, chk["template"])


def check_has_command(chk, text, cats):
    n = len(argocd_lines(text))
    return n > 0, "%d argocd command line(s)" % n


def check_cmdline_spec(chk, text, cats):
    """At least one command line has the command, every required flag, every positional token."""
    best = "no argocd command line"
    for line in argocd_lines(text):
        r = resolve_argocd(line, cats)
        miss = []
        if r["command"] != chk["command"]:
            miss.append("command is %s" % r["command"])
        alts = chk.get("flag_alternatives") or {}
        short = {t.split("=")[0] for t in r["tokens"][1:] if re.match(r"^-[A-Za-z]", t) and not t.startswith("--")}
        miss += ["flag %s missing" % f for f in chk.get("required_flags", [])
                 if f not in r["flags"] and not (set(alts.get(f, [])) & short)]
        toks = set(r["tokens"]) | {t.split("=", 1)[-1] for t in r["tokens"]}
        miss += ["%s missing" % p for p in chk.get("tokens", []) if p not in toks]
        for f, val in (chk.get("flag_values") or {}).items():
            if not any(t == f + "=" + val or (t == f and i + 1 < len(r["tokens"]) and r["tokens"][i + 1] == val)
                       for i, t in enumerate(r["tokens"])):
                miss.append("%s %s missing" % (f, val))
        if not miss:
            return True, "line meets the specification: %s" % line[:100]
        best = "closest line %r: %s" % (line[:80], "; ".join(miss))
    return False, best


def check_no_invented_cli(chk, text, cats):
    bad = []
    for line in argocd_lines(text):
        r = resolve_argocd(line, cats)
        if r["bad_subcommand"]:
            if cats.argocd_command(r["bad_subcommand"]).status == UNKNOWN:
                bad.append("command " + r["bad_subcommand"])
        for f in r["flags"]:
            if cats.cli_option(r["command"], f).status == UNKNOWN:
                bad.append("%s %s" % (r["command"], f))
    return not bad, ("nonexistent: " + ", ".join(bad)) if bad else "no nonexistent command or flag"


# ------------------------------------------------------------------ YAML documents (write a resource, fix the manifest)
def answer_yaml_docs(text):
    """Mapping documents of the answer: those in code fences, else the whole text read as a YAML stream."""
    docs = []
    blocks = FENCE_RE.findall(text or "")
    if not blocks and re.search(r"^\s*apiVersion\s*:", text or "", re.M):
        blocks = [text]
    for block in blocks:
        try:
            for d in yaml.safe_load_all(block):
                if isinstance(d, dict):
                    docs.append(d)
        except yaml.YAMLError:
            continue
    return docs


def doc_for(docs, chk):
    kind = chk.get("kind")
    hits = [d for d in docs if d.get("kind") == kind] if kind else docs
    return hits[0] if hits else None


def path_get(doc, path):
    """(found, value) for a path given as a list of keys and list indexes."""
    cur = doc
    for seg in path:
        if isinstance(seg, int):
            if not isinstance(cur, list) or seg >= len(cur):
                return False, None
            cur = cur[seg]
        else:
            if not isinstance(cur, dict) or seg not in cur:
                return False, None
            cur = cur[seg]
    return True, cur


def _loose(v):
    return ("true" if v else "false") if isinstance(v, bool) else str(v).strip().lower()


def same_value(got, want, loose=False, as_set=False):
    if as_set and isinstance(got, list) and isinstance(want, list):
        return len(got) == len(want) and all(any(same_value(g, w) for g in got) for w in want) \
            and all(any(same_value(g, w) for w in want) for g in got)
    if loose and not isinstance(got, (dict, list)) and not isinstance(want, (dict, list)):
        return _loose(got) == _loose(want)
    if isinstance(want, bool) or isinstance(got, bool):
        return isinstance(got, bool) and isinstance(want, bool) and got == want
    if isinstance(want, (int, float)) and isinstance(got, (int, float)):
        return got == want
    return got == want and type(got) is type(want)


def path_str(path):
    return ".".join(("[%d]" % s if isinstance(s, int) else str(s)) for s in path).replace(".[", "[")


def check_yaml_document(chk, text, cats):
    n = len(answer_yaml_docs(text))
    return n > 0, "%d YAML document(s)" % n


def check_yaml_one_doc(chk, text, cats):
    """Exactly one document with a `kind` in the answer, and it has the requested kind and apiVersion."""
    docs = [d for d in answer_yaml_docs(text) if "kind" in d]
    if len(docs) != 1:
        return False, "%d documents with a kind (exactly one wanted)" % len(docs)
    d = docs[0]
    ok = d.get("kind") == chk["kind"] and d.get("apiVersion") == chk["api_version"]
    return ok, "kind %r apiVersion %r, wanted %s %s" % (d.get("kind"), d.get("apiVersion"), chk["kind"], chk["api_version"])


def check_yaml_strict(chk, text, cats):
    """The document of the requested kind passes the strict schema validation of the catalogues."""
    d = doc_for(answer_yaml_docs(text), chk)
    if d is None:
        return False, "no document of kind %s" % chk.get("kind")
    r = cats.validate_resource(d)
    if not r.covered:
        return False, "no complete schema covers %s" % chk.get("kind")
    if r.ok:
        return True, "valid against the %s schema" % r.source
    return False, "; ".join("%s %s" % (v.code, v.path) for v in r.violations[:4]) + (
        " (+%d more)" % (len(r.violations) - 4) if len(r.violations) > 4 else "")


def check_yaml_fields_equal(chk, text, cats):
    """Every listed field is present with the listed value. fields: [{path: [...], value: v, loose: bool}]."""
    d = doc_for(answer_yaml_docs(text), chk)
    if d is None:
        return False, "no document of kind %s" % chk.get("kind")
    bad = []
    for f in chk["fields"]:
        found, got = path_get(d, f["path"])
        if not found:
            bad.append("%s missing" % path_str(f["path"]))
        elif not same_value(got, f["value"], f.get("loose", False), f.get("as_set", False)):
            bad.append("%s is %r, wanted %r" % (path_str(f["path"]), got, f["value"]))
    return not bad, "; ".join(bad[:4]) if bad else "%d field(s) as specified" % len(chk["fields"])


def check_yaml_fields_absent(chk, text, cats):
    d = doc_for(answer_yaml_docs(text), chk)
    if d is None:
        return False, "no document of kind %s" % chk.get("kind")
    bad = [path_str(p) for p in chk["paths"] if path_get(d, p)[0]]
    return not bad, ("present but excluded: " + ", ".join(bad)) if bad else "%d excluded field(s) absent" % len(chk["paths"])


def check_yaml_config_keys(chk, text, cats):
    """Keys of a free-form config map must exist in the Kafka configuration catalogue for the scope.
    A key the catalogue cannot classify (not covered) is not counted as wrong."""
    d = doc_for(answer_yaml_docs(text), chk)
    if d is None:
        return False, "no document of kind %s" % chk.get("kind")
    found, cfg = path_get(d, chk["path"])
    if not found:
        return False, "%s missing" % path_str(chk["path"])
    if not isinstance(cfg, dict):
        return False, "%s is not a map" % path_str(chk["path"])
    bad = [str(k) for k in cfg if cats.config_key(str(k), chk["scope"]).status == UNKNOWN]
    return not bad, ("not %s settings: %s" % (chk["scope"], ", ".join(bad))) if bad else "all keys are %s settings" % chk["scope"]


def check_old_value_forbidden(chk, text, cats):
    v = parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    hit = value_equals(v, chk["forbidden"], ci=(chk.get("norm", "ci") == "ci"))
    return not hit, "answer %r is the old value" % v if hit else "answer %r is not the old value" % v


# ------------------------------------------------------------------ Kafka command lines (write the command)
KTOOL_RE = re.compile(r"^(?:\S*/)?((?:kafka|connect)-[a-z0-9-]+?|trogdor)(?:\.sh)?$")


def kafka_lines(text):
    """Token lists of command lines for a Kafka tool (kafka-*.sh, bin/kafka-*.sh, without .sh too): code
    blocks, `$` prompts and inline code; continuation lines are joined; pipes and `&&` split commands."""
    text = re.sub(r"\\[ \t]*\n[ \t]*", " ", text or "")
    cands = []
    for block in FENCE_RE.findall(text):
        cands += [ln for ln in block.split("\n")]
    cands += text.split("\n")
    cands += re.findall(r"`([^`\n]+)`", text)
    out = []
    for c in cands:
        c = c.strip().lstrip("$ ").strip()
        for part in SPLIT_RE.split(c):
            toks = tokenize(part.strip())
            # an environment prefix such as `KAFKA_HEAP_OPTS=-Xmx1g bin/kafka-...`
            while toks and re.match(r"^[A-Z_][A-Z0-9_]*=", toks[0]):
                toks = toks[1:]
            if toks and KTOOL_RE.match(toks[0]) and toks not in out:
                out.append(toks)
    return out


def kafka_tool_name(tok):
    m = KTOOL_RE.match(tok)
    return (m.group(1) + ".sh") if m else tok


def kafka_flags(toks):
    """-> list of (flag, value or None). A value is the next token unless that is another `--flag`."""
    out = []
    for i, t in enumerate(toks[1:], 1):
        if not t.startswith("--"):
            continue
        if "=" in t:
            f, v = t.split("=", 1)
            out.append((f, v))
        else:
            nxt = toks[i + 1] if i + 1 < len(toks) else None
            out.append((t, nxt if nxt is not None and not nxt.startswith("--") else None))
    return out


def check_has_kafka_command(chk, text, cats):
    n = len(kafka_lines(text))
    return n > 0, "%d Kafka command line(s)" % n


def check_kcmd_spec(chk, text, cats):
    """A command line for the tool with every required flag (and value, when one is given) and none of the
    forbidden flags. required_flags is {flag: value or null}; a value may be given as `--flag v` or `--flag=v`."""
    best = "no command line for %s" % chk["tool"]
    for toks in kafka_lines(text):
        if kafka_tool_name(toks[0]) != chk["tool"]:
            continue
        flags = kafka_flags(toks)
        have = {}
        for f, v in flags:
            have.setdefault(f, []).append(v)
        miss = []
        for f, v in chk.get("required_flags", {}).items():
            if f not in have:
                miss.append("%s missing" % f)
            elif v is not None and not any(x is not None and norm_val(x) == v for x in have[f]):
                miss.append("%s should be %s" % (f, v))
        miss += ["%s is forbidden" % f for f in chk.get("forbidden_flags", []) if f in have]
        if not miss:
            return True, "line meets the specification: %s" % " ".join(toks)[:100]
        best = "closest line %r: %s" % (" ".join(toks)[:80], "; ".join(miss))
    return False, best


def check_kcmd_known_flags(chk, text, cats):
    """No flag on a command line of the tool is absent from the tool's complete option list. A tool without a
    complete list gives no verdict, so a flag there is not called wrong."""
    bad, seen = [], 0
    for toks in kafka_lines(text):
        if kafka_tool_name(toks[0]) != chk["tool"]:
            continue
        seen += 1
        for f, _ in kafka_flags(toks):
            if cats.cli_option(chk["tool"], f).status == UNKNOWN:
                bad.append(f)
    if not seen:
        return False, "no command line for %s" % chk["tool"]
    return not bad, ("nonexistent flag(s): " + ", ".join(sorted(set(bad)))) if bad else "every flag exists for %s" % chk["tool"]


def constraint_problems(tool, flags, cats):
    """Violations of what the tool's code states, for the flags given. Only constraints whose options are all
    spelled as flags are applied: availableIf (an option needs one of the others) and checkInvalidArgs
    (the first option cannot go with the rest). Constraints written as expressions are skipped."""
    present = set(flags)
    out = []
    for c in cats.cli_constraints(tool):
        if c["kind"] == "availableIf":
            others = [o for o in c["others"] if o.startswith("--")]
            if c["option"] in present and others and len(others) == len(c["others"]) and not (present & set(others)):
                out.append("%s needs one of %s" % (c["option"], ", ".join(others)))
        elif c["kind"] == "checkInvalidArgs":
            args = c["args"]
            if args and all(a.startswith("--") for a in args) and args[0] in present and present & set(args[1:]):
                out.append("%s cannot go with %s" % (args[0], ", ".join(sorted(present & set(args[1:])))))
    return out


def check_kcmd_constraints(chk, text, cats):
    probs, seen = [], 0
    for toks in kafka_lines(text):
        if kafka_tool_name(toks[0]) != chk["tool"]:
            continue
        seen += 1
        probs += constraint_problems(chk["tool"], [f for f, _ in kafka_flags(toks)], cats)
    if not seen:
        return False, "no command line for %s" % chk["tool"]
    return not probs, "; ".join(sorted(set(probs))) if probs else "the combination of options is allowed"


CHECKS = {
    "answer_line": check_answer_line,
    "answer_equals": check_answer_equals,
    "answer_set": check_answer_set,
    "answer_verdict": check_answer_verdict,
    "answer_letter": check_answer_letter,
    "rest_method": check_rest_method,
    "rest_path": check_rest_path,
    "has_command": check_has_command,
    "cmdline_spec": check_cmdline_spec,
    "no_invented_cli": check_no_invented_cli,
    "yaml_document": check_yaml_document,
    "yaml_one_doc": check_yaml_one_doc,
    "yaml_strict": check_yaml_strict,
    "yaml_fields_equal": check_yaml_fields_equal,
    "yaml_fields_absent": check_yaml_fields_absent,
    "yaml_config_keys": check_yaml_config_keys,
    "old_value_forbidden": check_old_value_forbidden,
    "has_kafka_command": check_has_kafka_command,
    "kcmd_spec": check_kcmd_spec,
    "kcmd_known_flags": check_kcmd_known_flags,
    "kcmd_constraints": check_kcmd_constraints,
}


# the a3 checks (semantic_rule, cmd_order, number_within, declines, no_invented_value, forbidden_keys): per item, opt-in
from eval2_checks_a3 import CHECKS_A3  # noqa: E402
assert not set(CHECKS_A3) & set(CHECKS), "a check name is defined twice"
CHECKS.update(CHECKS_A3)


def run_checks(item, text, cats):
    res = []
    for i, chk in enumerate(item["checks"]):
        fn = CHECKS.get(chk["type"])
        if fn is None:
            raise ValueError("unknown check type %r in item %s" % (chk["type"], item.get("id")))
        ok, detail = fn(chk, text or "", cats)
        res.append({"check": i, "type": chk["type"], "pass": bool(ok), "detail": detail})
    return res


# ------------------------------------------------------------------ identifier precision
CFG_RE = re.compile(r"(?<![\w.\-/@:$])([a-z][a-z0-9_]*(?:\.[a-z0-9][a-z0-9_-]*)+)(?![\w\-/(@:]|\.\w)")
FILE_EXT = {"properties", "sh", "yaml", "yml", "json", "jar", "md", "html", "log", "txt", "xml", "conf", "py",
            "java", "cfg", "crt", "pem", "jks", "p12", "tar", "gz", "zip", "sql", "csv", "adoc", "svc", "local",
            "com", "org", "io", "net", "dev"}
ANN_RE = re.compile(r"(?<![\w./-])strimzi\.io/([A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])")
NOT_ANN = {"docs", "downloads", "documentation", "quickstarts", "contributing", "charts", "community", "blog",
           "install", "examples"}
REST_RE = re.compile(r"(?<![\w])(GET|POST|PUT|DELETE|PATCH)\s+`?((?:https?://[^\s/`]+)?/(?:connectors|connector-plugins|admin/loggers)\S*?)(?=[`\s'\")]|$)")
CURL_RE = re.compile(r"curl\b[^\n]*", re.I)
KTOOL_LINE_RE = re.compile(r"(?<![\w-])(kafka-[a-z0-9-]+)(\.sh)?\b([^\n]*?)(?=(?<![\w-])kafka-[a-z0-9-]+(?:\.sh)?\b|\n|$)")
SNAKE_RE = re.compile(r"(?<![\w])([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)(?![\w])")
STRIMZI_KINDS = ("KafkaBridge", "KafkaConnect", "KafkaConnector", "KafkaMirrorMaker2", "KafkaNodePool",
                 "KafkaRebalance", "KafkaTopic", "KafkaUser", "StrimziPodSet", "Kafka")
SPEC_PATH_RE = re.compile(r"(?<![\w.\-/])((?:spec|status)(?:\.[A-Za-z0-9_]+(?:\[(?:\d+|\*)?\])?)+)")


# The Kafka configuration scope of the free-form config maps of the Strimzi kinds: (kind, path) -> scope
CONFIG_SCOPE = {("Kafka", "spec.kafka.config"): "broker", ("KafkaTopic", "spec.config"): "topic",
                ("KafkaConnect", "spec.config"): "connect"}


class Idents:
    """Collects classified identifiers of one answer: (type, token, status, reason, similar)."""

    def __init__(self):
        self.rows = {}

    def add(self, typ, token, look, extra=None):
        key = (typ, token)
        if key in self.rows:
            return
        similar = (look.data or {}).get("similar") or (look.data or {}).get("allowed_methods") or []
        self.rows[key] = {"type": typ, "token": token, "status": look.status, "reason": look.reason,
                          "similar": similar, **(extra or {})}

    def summary(self):
        out = {KNOWN: [], UNKNOWN: [], NOT_COVERED: []}
        for r in self.rows.values():
            out[r["status"]].append(r)
        return out


def key_prefixes(cats):
    return {e["key"].split(".")[0] for e in (cats.data["kafka_config"] or {}).get("entries", [])}


def flatten_yaml(obj, prefix=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = prefix + "." + str(k) if prefix else str(k)
            yield p
            yield from flatten_yaml(v, p)
    elif isinstance(obj, list):
        for v in obj:
            yield from flatten_yaml(v, prefix + "[]")


def yaml_docs(text):
    for block in FENCE_RE.findall(text or ""):
        try:
            for d in yaml.safe_load_all(block):
                if isinstance(d, dict):
                    yield d
        except yaml.YAMLError:
            continue


def classify_identifiers(text, cats):
    ids = Idents()
    text = text or ""
    # annotations first, and remove them so that the dotted-key scan does not see them
    for name in sorted(set(ANN_RE.findall(text))):
        if name not in NOT_ANN:
            ids.add("strimzi_annotation", "strimzi.io/" + name, cats.annotation("strimzi.io/" + name))
    scan = ANN_RE.sub(" ", text)
    # Connect REST pairs (prose, code, curl lines)
    pairs = {(m, p.rstrip(".,;:!?)")) for m, p in REST_RE.findall(scan)}
    for ln in CURL_RE.findall(scan):
        mm = re.search(r"-X\s*(GET|POST|PUT|DELETE|PATCH)", ln)
        pm = re.search(r"(?:https?://[^\s/'\"]+)?(/(?:connectors|connector-plugins|admin/loggers)[^\s'\"]*)", ln)
        if pm:
            pairs.add(((mm.group(1) if mm else "GET"), pm.group(1).rstrip(".,;:!?)")))
    for method, path in sorted(pairs):
        ids.add("connect_rest", "%s %s" % (method, cats.rest_segments(path) and "/" + "/".join(cats.rest_segments(path))),
                cats.rest_operation(method, path))
    # argocd commands and flags
    for line in argocd_lines(text):
        r = resolve_argocd(line, cats)
        if r["bad_subcommand"]:
            ids.add("argocd_command", r["bad_subcommand"], cats.argocd_command(r["bad_subcommand"]))
        else:
            ids.add("argocd_command", r["command"], cats.argocd_command(r["command"]))
        for f in r["flags"]:
            ids.add("argocd_flag", "%s %s" % (r["command"], f), cats.cli_option(r["command"], f))
    # protocol error names
    names = [e["name"] for e in (cats.data["protocol_errors"] or {}).get("entries", [])]
    for tok in sorted(set(SNAKE_RE.findall(scan))):
        look = cats.error_name(tok)
        if look.known:
            ids.add("protocol_error", tok, look)
            continue
        words = set(tok.split("_"))
        near = any(len(words & set(n.split("_"))) / len(words | set(n.split("_"))) >= 0.6 for n in names) or \
            bool(difflib.get_close_matches(tok, names, n=1, cutoff=0.85))
        if near:
            ids.add("protocol_error", tok, look)
    # YAML manifests of Strimzi kinds: field paths, and the keys under config maps
    kinds = {k for k in (e["kind"] for e in (cats.data["strimzi_resources"] or {}).get("entries", []))}
    other_kinds = set(cats.kinds()) - kinds  # Argo CD and Kubernetes kinds with a schema: fields are checked too
    prefixes = key_prefixes(cats)
    mentioned = set()
    for d in yaml_docs(text):
        kind = d.get("kind")
        if kind in other_kinds:
            for p in flatten_yaml({k: v for k, v in d.items() if k not in ("apiVersion", "kind", "metadata")}):
                look = cats.field_path(kind, p, d.get("apiVersion") if isinstance(d.get("apiVersion"), str) else None)
                if look.known or look.unknown:
                    ids.add("manifest_field", "%s %s" % (kind, p), look)
        if kind in kinds:
            mentioned.add(kind)
            for p in flatten_yaml({k: v for k, v in d.items() if k not in ("apiVersion", "kind", "metadata")}):
                look = cats.field_path(kind, p)
                if look.known or look.unknown:
                    ids.add("strimzi_field", "%s %s" % (kind, p), look)
                elif look.data.get("field", "").endswith("config"):
                    # the key under a free-form `config` map is a Kafka (or Connect) configuration key
                    leaf = p[len(look.data["field"]) + 1:]
                    if re.fullmatch(r"[a-z][a-z0-9_]*(?:\.[a-z0-9][a-z0-9_-]*)+", leaf):
                        ids.add("kafka_config_key", leaf, cats.config_key(leaf, CONFIG_SCOPE.get((kind, look.data["field"]))))
    # dotted spec paths in prose, only when the answer names Strimzi kinds
    for m in re.finditer(r"kind:\s*(\w+)", text):
        if m.group(1) in kinds:
            mentioned.add(m.group(1))
    for k in STRIMZI_KINDS[:-1]:
        if re.search(r"\b%s\b" % k, text):
            mentioned.add(k)
    if re.search(r"\bKafka (?:custom resource|resource|CR)\b|kind:\s*Kafka\b", text):
        mentioned.add("Kafka")
    if mentioned:
        for p in sorted(set(SPEC_PATH_RE.findall(scan))):
            looks = [cats.field_path(k, p) for k in sorted(mentioned)]
            pick = next((l for l in looks if l.known), None) or next((l for l in looks if l.not_covered), None) \
                or looks[0]
            if pick.known or pick.unknown:
                ids.add("strimzi_field", p, pick)
    # Kafka command-line tools: options are looked up, a tool without a full options list gives "not covered"
    for tool, sh, rest in KTOOL_LINE_RE.findall(scan):
        tl = cats.cli_tool(tool + ".sh")
        if tl.unknown and sh:  # only a name written as a script (`kafka-x.sh`): `kafka-config` may be a repository
            ids.add("kafka_cli_tool", tool + ".sh", tl)
        for opt in sorted(set(re.findall(r"(?<![\w-])(--[a-zA-Z][\w-]*)", rest.split("|")[0]))):
            ids.add("kafka_cli_flag", "%s %s" % (tool + ".sh", opt), cats.cli_option(tool + ".sh", opt))
    # dotted configuration keys
    for tok in sorted(set(CFG_RE.findall(scan))):
        last = tok.rsplit(".", 1)[-1]
        if last in FILE_EXT or re.fullmatch(r"\d+(\.\d+)*", tok):
            continue
        if tok.startswith(("spec.", "status.")):
            continue
        look = cats.config_key(tok)
        if look.known or tok.split(".")[0] in prefixes or look.not_covered and tok.startswith(("spring.", "quarkus.")):
            ids.add("kafka_config_key", tok, look)
    return ids.summary()


# ------------------------------------------------------------------ scoring
def wilson(k, n, z=1.96):
    if n == 0:
        return (None, None)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(max(0.0, c - h), 4), round(min(1.0, c + h), 4))


def score_item(item, text, cats):
    res = run_checks(item, text, cats)
    fmt = [r["pass"] for r, c in zip(res, item["checks"]) if c["type"] in FORMAT_CHECKS]
    ident = classify_identifiers(text, cats)
    return {"id": item["id"], "checks_passed": sum(r["pass"] for r in res), "checks_total": len(res),
            "all_pass": all(r["pass"] for r in res), "format_ok": all(fmt) if fmt else True,
            "results": res, "identifiers": ident}


def group_stats(rows, items_by_id, field):
    groups = defaultdict(list)
    for r in rows:
        groups[str(items_by_id[r["id"]].get(field))].append(r)
    out = {}
    for g, rs in sorted(groups.items()):
        k, n = sum(r["all_pass"] for r in rs), len(rs)
        lo, hi = wilson(k, n)
        cp, ct = sum(r["checks_passed"] for r in rs), sum(r["checks_total"] for r in rs)
        out[g] = {"n": n, "all_pass": k, "all_pass_rate": round(k / n, 4), "wilson95": [lo, hi],
                  "checks_share": round(cp / ct, 4) if ct else None,
                  "format_failures": sum(not r["format_ok"] for r in rs)}
    return out


def summarise(rows, items, missing=0, extra=0, catalogue_hash=None):
    by_id = {i["id"]: i for i in items}
    n = len(rows)
    k = sum(r["all_pass"] for r in rows)
    lo, hi = wilson(k, n)
    cp, ct = sum(r["checks_passed"] for r in rows), sum(r["checks_total"] for r in rows)
    tally = {"known": 0, "unknown": 0, "not_covered": 0}
    by_type = defaultdict(lambda: {"known": 0, "unknown": 0, "not_covered": 0})
    unknown_list = []
    answers_with_unknown = 0
    for r in rows:
        ident = r.get("identifiers") or {}
        for st in tally:
            for x in ident.get(st, []):
                tally[st] += 1
                by_type[x["type"]][st] += 1
        if ident.get("unknown"):
            answers_with_unknown += 1
        for x in ident.get("unknown", []):
            unknown_list.append({"id": r["id"], "type": x["type"], "token": x["token"], "reason": x["reason"],
                                 "similar": x["similar"]})
    classified = tally["known"] + tally["unknown"]
    return {
        "scorer": VERSION,
        "catalogue_hash": catalogue_hash,
        "items_scored": n, "missing_answers": missing, "answers_for_unknown_ids": extra,
        "all_pass": k, "all_pass_rate": round(k / n, 4) if n else None, "wilson95": [lo, hi],
        "checks_share": round(cp / ct, 4) if ct else None,
        "format_failures": sum(not r["format_ok"] for r in rows),
        "by_kind": group_stats(rows, by_id, "kind"),
        "by_domain": group_stats(rows, by_id, "domain"),
        "by_failure_class": group_stats(rows, by_id, "failure_class"),
        "by_overlap_band": group_stats(rows, by_id, "overlap_band"),
        "by_anchored": group_stats(rows, by_id, "anchored"),
        "identifier_precision": {
            "known": tally["known"], "unknown": tally["unknown"], "not_covered": tally["not_covered"],
            "precision_known_of_classified": round(tally["known"] / classified, 4) if classified else None,
            "answers_with_an_unknown_identifier": answers_with_unknown,
            "by_type": dict(sorted(by_type.items())),
            "unknown_identifiers": unknown_list,
            "note": "unknown = absent from a catalogue marked complete; not_covered is never counted as invented",
        },
    }


def load_jsonl(path):
    return [json.loads(l) for l in Path(path).read_text().splitlines() if l.strip()]


def score_files(items, answers, cats):
    ans = {}
    for a in answers:
        if "id" in a and "text" in a:
            ans[a["id"]] = a["text"]
    rows, missing = [], 0
    ids = {i["id"] for i in items}
    for it in items:
        if it["id"] not in ans:
            missing += 1
            rows.append({"id": it["id"], "checks_passed": 0, "checks_total": len(it["checks"]), "all_pass": False,
                         "format_ok": False, "results": [], "identifiers": {}, "missing": True})
            continue
        rows.append(score_item(it, ans[it["id"]], cats))
    return rows, missing, len([i for i in ans if i not in ids])


# ------------------------------------------------------------------ decline outcome (decision J5)
# Reported beside the all-pass rate, which it never changes. Per item and answer one outcome:
#   pass              all checks pass
#   correct_decline   not pass, declined, and the fact the failed check needs is in no chunk of the frozen index
#                     (or not in the answer's pack)
#   wrong_decline     not pass, declined, and the excerpts of the pack contained that fact
#   wrong             not pass, not declined
#   abstained         is_real only: expected "does not exist", answer declined ("not in my sources")
# The covering chunks of an item come from the item's own recorded `sources` (the entry of each source, in the file
# the source names when that file is in the index, else anywhere in the index). Details: runs/m4/reports/j5_decline_outcome.md.
import hashlib  # noqa: E402

OUTCOMES = ("pass", "correct_decline", "wrong_decline", "wrong", "abstained")
DEFAULT_INDEX = "work/v3/chunks_s1.jsonl"
DEFAULT_FROZEN = "runs/v3/INDEX_FROZEN.json"
UNIT_KINDS = ("write_command", "argocd_command", "write_resource", "fix_manifest")  # the question supplies what it names
GENERIC_LEAVES = {"apiVersion", "kind", "metadata", "name", "namespace", "spec", "status"}


class IndexMismatch(RuntimeError):
    pass


class ChunkIndex:
    """The frozen index file, read-only: chunks by id and by path."""

    def __init__(self, rows):
        self.chunks = {r["id"]: r for r in rows}
        self.by_path = defaultdict(list)
        for r in rows:
            self.by_path[r["path"]].append(r)
        self.rows = rows
        self.memo = {}

    @classmethod
    def load(cls, index_path=DEFAULT_INDEX, frozen_path=DEFAULT_FROZEN):
        """Refuses (IndexMismatch) when the file's sha256 differs from the one recorded in INDEX_FROZEN.json."""
        want = json.loads(Path(frozen_path).read_text())["sha256"]
        raw = Path(index_path).read_bytes()
        got = hashlib.sha256(raw).hexdigest()
        if got != want:
            raise IndexMismatch("index %s has sha256 %s, frozen record says %s" % (index_path, got, want))
        return cls([json.loads(l) for l in raw.decode("utf-8").splitlines() if l.strip()])


def _word(tok):
    return re.compile(r"(?<![\w.-])" + re.escape(tok) + r"(?![\w-]|\.\w)")


def _flag(tok):
    return re.compile(r"(?<![\w-])" + re.escape(tok) + r"(?![\w-])")


def _tool(tok):
    return re.compile(r"(?<![\w-])" + re.escape(re.sub(r"\.sh$", "", tok)) + r"(?:\.sh)?(?![\w-])")


def _cmd(cmd):
    return re.compile(r"(?<![\w-])" + r"\s+".join(re.escape(t) for t in cmd.split()) + r"(?![\w-])")


def _in_question(tok, question):
    return re.search(r"(?<![\w-])" + re.escape(tok) + r"(?![\w-])", question or "") is not None


def derive_units(item):
    """The facts the excerpts must carry for this item, from its recorded sources. A unit:
    {label, path (file in the index or None), strict: [regex], loose: [regex], tokens: [str] (to match a
    failed check's detail and a gap statement), decisive: bool}. A unit whose name the question already gives
    (write and manifest kinds) is not decisive. is_real items keep units only when the verdict expected is
    `exists`: nothing in the index can cover a name that does not exist."""
    kind, q = item.get("kind"), item.get("question") or ""
    expected = next((c.get("expected") for c in item["checks"] if c["type"] == "answer_verdict"), None)
    if kind == "is_real" and expected != "exists":
        return []
    units, seen = [], set()
    for src in item.get("sources", []):
        entry, cat = src.get("entry"), src.get("catalogue")
        if not entry:
            continue
        label = "%s:%s" % (cat, entry)
        if label in seen:
            continue
        seen.add(label)
        u = {"label": label, "path": src.get("file"), "strict": [], "loose": [], "tokens": [], "decisive": True,
             "heading": None}
        accepted = [norm_val(a).lower() for c in item["checks"] if c["type"] in ("answer_equals", "answer_set")
                    for a in c.get("accepted", [])]
        if cat in ("kafka_config", "kafka_config_history"):
            key = entry.split("/", 1)[-1]
            u["heading"] = key
            u["strict"] = [re.compile(r"(?m)^## " + re.escape(key) + r"\s*$")]
            u["loose"] = [_word(key)] if key.lower() in accepted else []   # the answer is the key itself
            u["tokens"] = [key]
        elif cat == "protocol_errors":
            u["strict"] = [re.compile(r"(?m)^" + re.escape(entry) + r"\s*$")]
            u["loose"] = [_word(entry)] if kind == "is_real" else []         # existence: any mention shows it
            u["tokens"] = [entry]
        elif cat == "connect_rest":
            u["heading"] = entry
            u["loose"] = [re.compile(r"(?<![\w/-])" + r"\s+".join(re.escape(t) for t in entry.split(" ", 1))
                                     + r"(?![\w/{}-])")] if kind == "is_real" else []
            u["strict"] = [re.compile(r"(?m)(?:^|: )" + re.escape(entry) + r"\s*$")]
            u["tokens"] = [entry, entry.split(" ", 1)[-1]]
        elif cat == "kafka_cli":
            toks = entry.split()
            u["path"] = None
            u["strict"] = [_tool(toks[0])] + ([_flag(toks[1])] if len(toks) > 1 and toks[1].startswith("--") else [])
            u["tokens"] = [toks[1]] if len(toks) > 1 else [toks[0]]
            if kind in UNIT_KINDS and all(_in_question(t, q) for t in (toks[0],) + tuple(toks[1:2])):
                u["decisive"] = False
        elif cat == "argocd" and entry.startswith("argocd "):
            toks = entry.split()
            i = next((n for n, t in enumerate(toks) if t.startswith("--")), len(toks))
            cmd = " ".join(toks[:i])
            u["path"] = None
            u["strict"] = [_cmd(cmd)] + ([_flag(toks[i])] if i < len(toks) else [])
            u["tokens"] = [toks[i]] if i < len(toks) else [cmd]
            if kind in UNIT_KINDS and _in_question(cmd, q):
                u["decisive"] = False
        elif cat == "strimzi_annotations":
            u["path"] = None
            u["strict"] = [re.compile(r"(?<![\w./-])" + re.escape(entry) + r"(?![\w-])")]
            u["tokens"] = [entry]
            if kind in UNIT_KINDS and _in_question(entry, q):
                u["decisive"] = False
        elif cat in ("kubernetes", "strimzi_resources", "argocd") and " " in entry:
            kd, path = entry.split(" ", 1)
            leaf = re.sub(r"\[.*?\]", "", path.split(".")[-1])
            u["path"] = None
            u["strict"] = [_word(leaf), re.compile(r"(?<![\w-])" + re.escape(kd) + r"(?![\w-])")]
            u["tokens"] = [leaf]
            if kind in UNIT_KINDS and (leaf in GENERIC_LEAVES or _in_question(leaf, q)):
                u["decisive"] = False
        else:
            continue
        units.append(u)
    # options listed beside the answer are not what the excerpts must carry: when the answer is one of the
    # configuration keys of the sources, only that key is decisive
    accepted = {norm_val(a).lower() for c in item["checks"] if c["type"] in ("answer_equals", "answer_set")
                for a in c.get("accepted", [])}
    cfg = [u for u in units if u["label"].startswith("kafka_config:")]
    hit = [u for u in cfg if u["label"].split("/", 1)[-1].lower() in accepted]
    if hit:
        for u in cfg:
            u["decisive"] = u in hit
    return units


def cover_unit(unit, idx):
    """Ids of the chunks of the index holding the unit's fact, with how they were found: strict (the entry's own
    heading or line in the source file; for reference entries the chunks naming the entry are added), mention
    (only chunks naming the entry), global (the patterns together in any chunk, for a source file that is not
    in the index), none."""
    key = (unit["label"], unit["path"])
    if key in idx.memo:
        return idx.memo[key]
    idx.memo[key] = res = _cover_unit(unit, idx)
    return res


def _cover_unit(unit, idx):
    in_file = idx.by_path.get(unit["path"]) if unit["path"] else None
    strict = []
    if in_file:
        strict = [c["id"] for c in in_file if (unit["heading"] is not None and c.get("title") == unit["heading"])
                  or all(p.search(c["text"]) for p in unit["strict"])]
    if unit["loose"]:
        # the entry's own section, plus every chunk of the index that names the entry: the excerpt then carries
        # the answer value (a related key, the same key in another scope, an endpoint in an example)
        mention = [c["id"] for c in idx.rows if all(p.search(c["text"]) for p in unit["loose"])]
        hit = list(dict.fromkeys(strict + mention))
        return hit, ("strict" if strict else "mention" if mention else "none")
    if strict:
        return strict, "strict"
    pats = unit["strict"]
    hit = [c["id"] for c in idx.rows if all(p.search(c["text"]) for p in pats)] if pats else []
    return hit, ("global" if hit else "none")


# -- the decline detector
_SRC = r"(?:excerpts?|documentation|docs\b|sources?|context|provided (?:text|material|information)|retrieved|reference)"
_NEG = (r"(?:do(?:es)? not|don'?t|doesn'?t|did not|cannot|can'?t|could not|couldn'?t|unable to|not|no)\s+"
        r"(?:\w+\s+){0,3}?(?:cover|contain|include|mention|provide|specify|describe|list|state|document|show|say|"
        r"define|address|explain|identify|find|determine|confirm|verify|name|give|answer|support|have|distinguish|"
        r"differentiate|clarify|indicate|resolve|settle)")
GAP_PATTERNS = [
    re.compile(r"not in (?:my|the) sources?", re.I),
    re.compile(r"(?:not|never) (?:found|covered|mentioned|documented|listed|specified|included|present|stated|defined)\b"
               r"[^.\n]{0,40}?\b(?:in|by)\b[^.\n]{0,30}?" + _SRC, re.I),
    re.compile(r"(?:retrieved|provided) documentation does not cover", re.I),
    re.compile(r"\b(?:excerpts?|documentation|sources?)\b[^.\n]{0,80}?" + _NEG, re.I),
    re.compile(r"\b(?:" + _NEG + r")\b[^.\n]{0,100}?" + _SRC, re.I),
    re.compile(r"\bno (?:information|mention|details?|reference|excerpts?|documentation|text)\b"
               r"[^.\n]{0,60}?(?:about|on|of|regarding|for|in|that)\b", re.I),
    re.compile(r"\b(?:cannot|can'?t|could not|unable to)\s+(?:answer|find|determine|provide|confirm|verify|construct|"
               r"give|write|identify)\b[^.\n]{0,80}?(?:excerpts?|documentation|sources?|provided|given)", re.I),
    re.compile(r"\bnot covered\b", re.I),
]
EMPTY_ANSWER = {"null", "unknown", "n/a", "na", "not specified", "not provided", "not available", "undetermined"}


def gap_sentences(text):
    """Sentences (split at line ends and full stops outside backticks) that state a gap in the excerpts."""
    out = []
    for para in re.split(r"\n+", text or ""):
        for sent in re.split(r"(?<=[.!?])\s+(?=[A-Z*`\"])", para):
            if any(p.search(sent) for p in GAP_PATTERNS):
                out.append(sent.strip())
    return out


GAP_VALUE = re.compile(r"^(?:not (?:found|covered|specified|provided|available|mentioned|in)|cannot|can'?t|unable|"
                       r"no information|insufficient|the (?:provided|retrieved) )", re.I)


def detect_decline(text):
    """(declined, gap_text). With an ANSWER line the answer is a decline when the value itself is empty (null,
    unknown, ...), says that the sources do not cover it, or is the verdict `not in my sources`: a concrete value
    beside a remark about another gap is an answer. Without an ANSWER line (a command, a manifest, prose) the
    answer is a decline when a sentence names a gap in the excerpts or sources (the prompts' form 'The retrieved
    documentation does not cover X' and its natural variants)."""
    gaps = gap_sentences(text)
    v = parse_answer_line(text)
    if v is not None:
        nv = norm_val(v).lower()
        return (nv in EMPTY_ANSWER or norm_verdict(v) == "not in my sources" or bool(GAP_VALUE.search(nv))
                or any(p.search(v) for p in GAP_PATTERNS)), " ".join(gaps)
    return bool(gaps), " ".join(gaps)


def _mentions(token, text):
    if not token or not text:
        return False
    if token in text:
        return True
    bare = token.lstrip("-")
    return bare != token and bare.replace("-", " ") in text.lower().replace("-", " ")


def has_payload(text):
    """The answer carries something to check besides a gap statement: a code block, or an ANSWER line that
    is not itself empty or a verdict of not covered."""
    if FENCE_RE.search(text or ""):
        return True
    v = parse_answer_line(text)
    return v is not None and norm_val(v).lower() not in EMPTY_ANSWER and norm_verdict(v) != "not in my sources" \
        and not any(p.search(v) for p in GAP_PATTERNS)


def classify_answer(item, row, text, pack, idx_cache):
    """One outcome for a scored row. idx_cache: {unit label: (cover ids, via)} for this item's units."""
    all_units = derive_units(item)
    units = [u for u in all_units if u["decisive"]]
    declined, gap = detect_decline(text or "")
    failed = [r for r in row["results"] if not r["pass"]]
    detail = " ".join(r["detail"] for r in failed)
    rel = [u for u in units if any(_mentions(t, detail) for t in u["tokens"])]
    given = [u for u in all_units if not u["decisive"] and any(_mentions(t, detail) for t in u["tokens"])]
    mapped = bool(rel) and len(rel) < len(units)
    rel_found = bool(rel)
    rel = rel or units
    undecided = False
    if declined and failed and has_payload(text) and mapped:
        said = [u for u in units if any(_mentions(t, gap) for t in u["tokens"])]
        if said and not set(u["label"] for u in said) & set(u["label"] for u in rel):
            declined = False          # the gap statement is about another element than the failed check
        elif not said:
            undecided = True          # cannot decide mechanically: counted as declined
    info = {"declined": declined, "decline_undecided": undecided, "units": len(units),
            "relevant_units": [u["label"] for u in rel], "via": {u["label"]: idx_cache[u["label"]][1] for u in units}}
    pack = set(pack or [])
    in_pack = [bool(set(idx_cache[u["label"]][0]) & pack) for u in rel]
    info["covering_in_pack"] = any(in_pack)
    if row["all_pass"]:
        return "pass", info
    if not declined:
        return "wrong", info
    expected = next((c.get("expected") for c in item["checks"] if c["type"] == "answer_verdict"), None)
    if item.get("kind") == "is_real" and expected == "does not exist":
        return "abstained", info
    if not units or (given and not rel_found):
        # nothing to look up, or the failed element is one the question itself names: the excerpts were not needed
        info["no_units_derived" if not units else "failed_element_in_question"] = True
        return "wrong_decline", info
    if any(not idx_cache[u["label"]][0] for u in rel):
        return "correct_decline", info
    return ("wrong_decline" if all(in_pack) else "correct_decline"), info


def item_coverage(item, idx):
    """Static coverage of an item: per-unit covering chunk ids and how they were found, and not_in_index
    (some decisive unit is in no chunk of the index; an item without decisive units is never not_in_index)."""
    units = derive_units(item)
    cache = {u["label"]: cover_unit(u, idx) for u in units}
    dec = [u for u in units if u["decisive"]]
    cover = sorted({c for u in dec for c in cache[u["label"]][0]})
    return {"units": [{"label": u["label"], "decisive": u["decisive"], "chunks": len(cache[u["label"]][0]),
                       "via": cache[u["label"]][1]} for u in units],
            "covering_chunks": cover, "not_in_index": bool(dec) and any(not cache[u["label"]][0] for u in dec),
            "coverage_derived": bool(dec)}, cache


def add_outcomes(rows, items, answers, idx):
    """Adds to each score row (in place): outcome, declined, coverage. Existing keys are not touched."""
    by_id = {i["id"]: i for i in items}
    ans = {a["id"]: a for a in answers if "id" in a and "text" in a}
    for r in rows:
        it = by_id[r["id"]]
        cov, cache = item_coverage(it, idx)
        a = ans.get(r["id"])
        if a is None:
            r["outcome"], r["declined"] = "wrong", False
            r["coverage"] = dict(cov, missing_answer=True)
            continue
        outcome, info = classify_answer(it, r, a["text"], a.get("pack"), cache)
        r["outcome"], r["declined"] = outcome, info["declined"]
        r["coverage"] = dict(cov, **{k: v for k, v in info.items() if k not in ("declined", "units")})
    return rows


def outcome_counts(rows, items, field="kind"):
    by_id = {i["id"]: i for i in items}
    out = {"total": {o: 0 for o in OUTCOMES}}
    out["total"]["n"] = 0
    out["total"]["not_in_index"] = 0
    for r in rows:
        if "outcome" not in r:
            continue
        for g in ("total", str(by_id[r["id"]].get(field))):
            d = out.setdefault(g, {"n": 0, "not_in_index": 0, **{o: 0 for o in OUTCOMES}})
            d["n"] += 1
            d[r["outcome"]] += 1
            d["not_in_index"] += bool(r["coverage"]["not_in_index"])
    for g, d in out.items():
        d["shares"] = {o: round(d[o] / d["n"], 4) if d["n"] else None for o in OUTCOMES}
        d["all_pass_rate"] = d["shares"]["pass"]
    return out


def format_outcomes(counts, name=""):
    """Plain text lines: counts and shares per kind and in total (no item ids)."""
    L = ["outcomes %s(all-pass rate unchanged = pass share)" % (name + " " if name else "")]
    L.append("%-18s%5s" % ("kind", "n") + "".join("%17s" % o for o in OUTCOMES) + "%14s" % "not_in_index")
    for g in sorted(k for k in counts if k != "total") + ["total"]:
        d = counts[g]
        L.append("%-18s%5d" % (g, d["n"]) + "".join("%9d (%4.1f%%)" % (d[o], 100 * d["shares"][o]) if d["n"] else "%17s" % "-"
                                                    for o in OUTCOMES) + "%14d" % d["not_in_index"])
    return L


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--items", required=True)
    ap.add_argument("--answers", required=True)
    ap.add_argument("--scores", help="write the per-item score rows here (jsonl)")
    ap.add_argument("--summary", help="write the summary here (json)")
    ap.add_argument("--catalogues", help="catalogue directory (default work/m4/catalogues)")
    ap.add_argument("--index", default=DEFAULT_INDEX, help="frozen index file for the decline outcome")
    ap.add_argument("--frozen", default=DEFAULT_FROZEN, help="record of the frozen index (sha256)")
    ap.add_argument("--no-outcomes", action="store_true", help="skip the decline outcome (covering chunks need the index)")
    args = ap.parse_args(argv)
    cats = Catalogues.load(args.catalogues)
    items = load_jsonl(args.items)
    answers = load_jsonl(args.answers)
    rows, missing, extra = score_files(items, answers, cats)
    from kafka_sre_rag.catalogue import catalogue_hash
    summ = summarise(rows, items, missing, extra, catalogue_hash(args.catalogues))
    if not args.no_outcomes:
        try:
            add_outcomes(rows, items, answers, ChunkIndex.load(args.index, args.frozen))
            summ["outcomes"] = outcome_counts(rows, items)
        except (IndexMismatch, OSError) as e:
            summ["outcomes"] = {"refused": str(e)}
            print("outcomes skipped: %s" % e, file=sys.stderr)
    if args.scores:
        Path(args.scores).write_text("".join(json.dumps(r, sort_keys=True, ensure_ascii=False) + "\n" for r in rows))
    text = json.dumps(summ, indent=1, sort_keys=True, ensure_ascii=False)
    if args.summary:
        Path(args.summary).write_text(text + "\n")
    print(text)
    if "total" in summ.get("outcomes", {}):
        print("\n".join(format_outcomes(summ["outcomes"])), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
