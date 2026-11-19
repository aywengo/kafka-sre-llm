#!/usr/bin/env python3
"""Identifier check of the M4 evaluation for free-text answers (no model, no server, CPU only).

Every identifier an answer uses that a catalogue can decide is classified:
  exists       the catalogue lists it
  nonexistent  the catalogue is complete for that kind of name in that context and the name is not in it
  undecided    the catalogue cannot decide (incomplete list, a name that may belong to another project,
               a context that is not certain); the reason is kept (`why` code and `reason` text)
A finding of `nonexistent` must be right: whenever there is doubt the identifier is `undecided`. This includes a
line that says the name does not exist, is a typo or was removed (the answer is not using the name), a snippet with
an elision marker, a Kubernetes core kind (the OpenAPI part of the catalogue is not trusted for nonexistence),
an Argo CD sub-command (CLI plugins can add commands) and a key close to nothing in the reference.

Kinds of identifiers and where they are read (code blocks, YAML manifests, command lines, inline code):
  cli_tool         `kafka-x.sh` written as a script, against bin_tools of kafka_cli (bin_complete)
  cli_flag         long option of a Kafka tool on a command line, against the tool's option list
                   (only when the catalogue marks that tool's list complete)
  argocd_command / argocd_flag   `argocd ...` command lines against the generated cobra reference
  api_version      apiVersion of a Strimzi or Argo CD kind in a manifest, against the served versions of the CRD
  manifest_field   field path in a manifest of a kind with a complete schema (CRD or Kubernetes OpenAPI)
  enum_value       scalar under a field whose schema has an enum
  prose_path       `spec.` or `status.` path in inline code, when the answer names the resource kind
  config_key       dotted Kafka configuration key (config maps of manifests, properties blocks, inline code,
                   --add-config / --config) against kafka_config; nonexistent only when it is also close to a
                   listed key and is in no Kafka version of the history
  config_value     value of a key whose reference enumerates the valid values
  rest_operation   Connect REST method and path against the generated OpenAPI document
  protocol_error   UPPER_SNAKE name in a sentence about an error that is close to, but not, a protocol error
  annotation       strimzi.io/ names (the catalogue is not complete: exists or undecided only)

Shares only the catalogue files (and kafka_sre_rag/catalogue.py, as eval2_score.py does) with the runtime
answer checker; nothing is imported from kafka_sre_rag/checker.py.

Usage: .venv/bin/python scripts/eval2_idcheck.py --answers ANSWERS.jsonl [--catalogues DIR]
           [--totals-only] [--show-undecided] [--json NEW.json]

A path ending in .enc is a hidden run: it is read through scripts/eval2.py (key from the environment) and only
the totals are ever printed; --json and --show-undecided are refused for it. The totals carry no answer id,
item id or identifier.
"""
import argparse
import difflib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues, catalogue_hash, default_dir  # noqa: E402
from eval2_score import argocd_lines, tokenize  # noqa: E402

VERSION = "eval2_idcheck/1"
EXISTS, NONEXISTENT, UNDECIDED = "exists", "nonexistent", "undecided"
ENVELOPE = ("apiVersion", "kind", "metadata")
KINDS = ("cli_tool", "cli_flag", "argocd_command", "argocd_flag", "api_version", "manifest_field", "enum_value",
         "prose_path", "config_key", "config_value", "rest_operation", "protocol_error", "annotation")

# ------------------------------------------------------------------ reading an answer


def parse_answer(text):
    """-> (lines, blocks, prose). blocks: dicts {lang, start, lines [(no, text)], pre}; prose: [(no, text)]."""
    lines = (text or "").split("\n")
    blocks, prose, cur = [], [], None
    for no, ln in enumerate(lines, 1):
        s = ln.lstrip()
        if cur is None:
            if s.startswith("```") or s.startswith("~~~"):
                rest = s[3:].strip()
                cur = {"lang": (rest.split()[0].lower() if rest else ""), "fence": s[:3], "start": no,
                       "lines": [], "pre": " ".join(t for _, t in prose[-3:]).lower()}
            else:
                prose.append((no, ln))
        elif s.startswith(cur["fence"]):
            blocks.append(cur)
            cur = None
        else:
            cur["lines"].append((no, ln))
    if cur is not None:  # an answer cut at the token limit leaves the last block open
        blocks.append(cur)
    return lines, blocks, prose


SPAN_RE = re.compile(r"(`+)([^`\n]+?)\1")


def inline_spans(prose):
    return [(no, m.group(2).strip()) for no, ln in prose for m in SPAN_RE.finditer(ln)]


def logical_lines(block_lines):
    """Join shell continuation lines: [(first line no, text)]."""
    out, cur, first = [], "", None
    for no, ln in block_lines:
        if cur == "":
            first = no
        s = ln.rstrip()
        if s.endswith("\\"):
            cur += s[:-1] + " "
            continue
        out.append((first, cur + s))
        cur = ""
    if cur:
        out.append((first, cur))
    return out


# a line that says the name does not exist, is a typo, was removed or is a question about it: the answer is not
# using the name, so a nonexistent finding on that line would count the answer for pointing out the error
DENIAL_RE = re.compile(
    r"\b(?:no such|there is no|there's no|there are no|does not exist|doesn't exist|do not exist|don't exist|did not exist|"
    r"not exist|nonexistent|non-existent|not a (?:real|valid|known|standard|documented|supported|actual)|"
    r"is not (?:a|an|an actual|listed|documented|supported|defined|available|present|real|valid)|"
    r"isn't (?:a|an|listed|documented|supported|defined|available|real|valid)|"
    r"not (?:listed|documented|supported|defined|found|present) (?:in|for|by|as)|typo|misspell\w*|invented|made[- ]up|"
    r"hallucinat\w*|fictional|no longer|was removed|were removed|been removed|not a flag|not an option|not an error|"
    r"incorrect\w*|wrong\w*|invalid|mistake\w*|not the (?:correct|right|real|actual|exact)|"
    r"(?:correct|right|real|actual|exact) (?:key|name|option|flag|spelling|tool|script|path|field|value|error)|"
    r"instead of|rather than|should be|missing the|is missing|do(?:es)? not contain|do(?:es)? not cover|"
    r"no reference to|not explicitly|is it possible|is there an?|was it)\b", re.I)
NOT_NAME_RE = r"\bnot\s+[*_]*[`'\"]*%s"


class Found:
    """Identifiers of one answer, deduplicated by (kind, ident); nonexistent beats exists beats undecided."""

    ORDER = {NONEXISTENT: 2, EXISTS: 1, UNDECIDED: 0}

    def __init__(self, lines):
        self.lines, self.rows = lines, {}

    def add(self, kind, ident, status, lineno, reason="", why=""):
        if status == NONEXISTENT and lineno and 0 < lineno <= len(self.lines) and (
                DENIAL_RE.search(self.lines[lineno - 1]) or
                re.search(NOT_NAME_RE % re.escape(ident.split()[-1].split("=")[0]), self.lines[lineno - 1], re.I)):
            status, why = UNDECIDED, "mentioned_as_nonexistent"
            reason = "the line says the name does not exist or is a typo or was removed; " + reason
        key = (kind, ident)
        old = self.rows.get(key)
        if old and self.ORDER[old["status"]] >= self.ORDER[status]:
            return
        text = self.lines[lineno - 1].strip() if lineno and 0 < lineno <= len(self.lines) else ""
        self.rows[key] = {"kind": kind, "ident": ident, "status": status, "reason": reason, "why": why,
                          "line_no": lineno, "line": text[:200]}

    def result(self):
        rows = sorted(self.rows.values(), key=lambda r: (r["line_no"] or 0, r["kind"], r["ident"]))
        c = Counter(r["status"] for r in rows)
        return {"exists": c[EXISTS], "nonexistent": c[NONEXISTENT], "undecided": c[UNDECIDED], "items": rows}


# ------------------------------------------------------------------ catalogue helpers

class Ctx:
    def __init__(self, cats):
        self.cats = cats
        d = cats.data
        self.crd_kinds = {e["kind"] for e in (d.get("strimzi_resources") or {}).get("entries", [])}
        self.crd_kinds |= {e["kind"] for e in (d.get("argocd") or {}).get("resources", []) if e.get("complete")}
        self.all_kinds = set(cats.kinds())
        self.kind_re = re.compile(r"\b(" + "|".join(sorted(map(re.escape, self.all_kinds), key=len, reverse=True)) +
                                  r")\b") if self.all_kinds else None
        # every path of every kind (array brackets removed) -> kinds that have it
        self.path_kinds = defaultdict(set)
        for name in ("strimzi_resources", "argocd", "kubernetes"):
            for e in (d.get(name) or {}).get("entries", []) + (d.get(name) or {}).get("resources", []):
                for p in e["paths"]:
                    self.path_kinds[p.replace("[]", "")].add(e["kind"])
        self.prefixes = {e["key"].split(".")[0] for e in (d.get("kafka_config") or {}).get("entries", [])}
        self.error_names = [e["name"] for e in (d.get("protocol_errors") or {}).get("entries", [])]

    def kinds_in(self, text):
        return set(self.kind_re.findall(text)) if self.kind_re else set()

    def named_kinds(self, text):
        """CRD kinds (Strimzi, Argo CD) the text names as resources: written as a word (not the bare word Kafka
        or Application), in backticks, as `Kafka` custom resource, or as `kind: Kafka`. Core Kubernetes kinds
        are not used to call a path nonexistent: `ConfigMap` in a sentence about `spec.logging` is not the
        resource that has the path."""
        out = {k for k in self.kinds_in(text) if k in self.crd_kinds and k not in ("Kafka", "Application")}
        out |= {k for k in re.findall(r"`([A-Z][A-Za-z0-9]+)`", text) if k in self.crd_kinds}
        for a, b in re.findall(r"\b(Kafka)\s+(?:custom resource|resource|CR|CRD)\b|kind:\s*(Kafka)\b", text):
            out.add(a or b)
        return out


# prefixed or per-component forms that the reference does not list (never decided)
OPEN_PREFIXES = ("producer.", "consumer.", "admin.", "listener.name.", "config.providers.", "transforms.",
                 "predicates.", "key.converter.", "value.converter.", "header.converter.", "producer.override.",
                 "consumer.override.", "admin.override.", "mirror.", "log4j", "spring.", "quarkus.", "camel.",
                 "mp.messaging.", "kafka.", "strimzi.", "cruise.control.", "sasl.login.callback")
FILE_EXT = {"properties", "sh", "yaml", "yml", "json", "jar", "md", "html", "log", "txt", "xml", "conf", "py", "java",
            "cfg", "crt", "pem", "jks", "p12", "tar", "gz", "zip", "sql", "csv", "adoc", "svc", "local", "com", "org",
            "io", "net", "dev", "key", "password"}
KEY_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*(?:\.[a-z0-9][a-z0-9_-]*)+$")
PLACEHOLDER_RE = re.compile(r"[<>{}$*|\[\]()\s]|^\.\.\.|…")


def decide_config_key(ctx, key, scope=None):
    """-> (status, why, reason, note) or None when the token is not recognisably a Kafka config key."""
    cats = ctx.cats
    if not KEY_RE.match(key) or key.split(".")[-1] in FILE_EXT or re.fullmatch(r"\d+(\.\d+)*", key):
        return None
    look = cats.config_key(key)
    if look.known:
        scopes = look.data.get("scopes", [])
        if scope == "topic" and "topic" not in scopes:
            # the topic scope of the reference lists every topic-level configuration
            return NONEXISTENT, "", "listed for %s only; not a topic configuration (the topic scope is complete), " \
                                    "and KafkaTopic spec.config takes topic configurations" % ",".join(scopes), None
        if scope == "broker" and set(scopes) <= {"topic"}:
            return NONEXISTENT, "", "a topic-level configuration only; not a broker configuration (its broker form, " \
                                    "if any, has a log. prefix)", None
        return EXISTS, "", look.reason, None
    if key.split(".")[0] not in ctx.prefixes or key.split(".")[0] in ("spec", "status", "metadata"):
        return None
    if key.startswith(OPEN_PREFIXES):
        return UNDECIDED, "prefixed_form", "prefixed or per-component form that the reference does not list", None
    if look.status == NOT_COVERED:
        return UNDECIDED, "catalogue_not_complete", look.reason, None
    hist = cats.config_history(key)
    if hist.known and any(v is not None for e in hist.entries for v in e["versions"].values()):
        return UNDECIDED, "older_version", "key exists in an earlier Kafka version (4.1 or 4.2)", None
    near = look.data.get("similar") or []
    if near:
        return NONEXISTENT, "", "not in the reference of any scope (4.3, 4.2, 4.1); close to " + ", ".join(near), None
    return UNDECIDED, "no_near_key", "not in the reference but not close to a listed key: may be a plugin or " \
                                      "other project's key", None


def split_values(v):
    v = v.strip().strip("`'\"").rstrip(",.;")
    return v


def decide_config_value(ctx, key, value):
    """-> (status, reason, why) or None when nothing is decidable. Only enumerated valid values."""
    value = split_values(str(value))
    if not value or PLACEHOLDER_RE.search(value) or ":" in value or value.startswith(("#", "/")):
        return None
    cats = ctx.cats
    look = cats.config_key(key)
    if not look.known:
        return None
    lists = [e["valid_values_list"] for e in look.entries if e.get("valid_values_list")]
    if not lists:
        return None
    parts = [p.strip() for p in value.split(",")] if any(e.get("type") == "list" for e in look.entries) else [value]
    for p in parts:
        if any(p in l for l in lists):
            continue
        if any(p.lower() in [x.lower() for x in l] for l in lists):
            return UNDECIDED, "case_only", "value differs from a valid value only in case"
        return NONEXISTENT, "", "%r is not among the valid values %s" % (p, lists[0])
    return EXISTS, "", "value is among the valid values"


# ------------------------------------------------------------------ scripts and flags

SCRIPT_RE = re.compile(r"(?<![\w.-])(kafka-[a-z0-9]+(?:-[a-z0-9]+)*\.sh)(?![\w-])")
USER_SCRIPT_RE = re.compile(r"(?:>>?|\btee|\btouch|\bnano|\bvim?|\bchmod|\bcp|\bmv|\bln)\s+\S*kafka-|"
                            r"\b(?:create|created|save|saved|name[ds]?|called|write|written|custom|wrapper|your own|"
                            r"example script|sample script)\b", re.I)


def find_scripts(ctx, found, lines):
    for no, ln in enumerate(lines, 1):
        for m in SCRIPT_RE.finditer(ln):
            name = m.group(1)
            look = ctx.cats.cli_tool(name)
            if look.known:
                found.add("cli_tool", name, EXISTS, no, look.reason)
            elif look.unknown:
                if USER_SCRIPT_RE.search(ln):
                    found.add("cli_tool", name, UNDECIDED, no, "the line may define or create this script",
                              "possible_user_script")
                else:
                    near = look.data.get("similar") or []
                    found.add("cli_tool", name, NONEXISTENT, no,
                              "not one of the %d scripts of bin/ (Kafka 4.3.2, list complete)%s" % (
                                  len(ctx.cats.data["kafka_cli"]["bin_tools"]),
                                  ("; close to " + ", ".join(near)) if near else ""))
            else:
                found.add("cli_tool", name, UNDECIDED, no, look.reason, "tool_not_listed")


WRAP_ANY = {"sudo", "env", "time", "nohup", "watch", "timeout", "xargs", "exec"}
TOOL_TOK_RE = re.compile(r"^(?:\S*/)?((?:kafka|connect)-[a-z0-9-]+?|trogdor)(\.sh)?$")
# command separators; a `<` or `>` inside a word is a placeholder (<broker>), not a redirect
SPLIT_RE = re.compile(r"\s*(?:\|\||&&)\s*|\s*;\s+|\s+\|\s+|\s+(?:\d?>>?&?\d?|<)\s+")


def tool_in(toks, ctx):
    """-> (index, 'kafka-x.sh') of the Kafka tool this command runs, or None."""
    for i, t in enumerate(toks):
        m = TOOL_TOK_RE.match(t)
        if not m:
            continue
        name = m.group(1) + ".sh"
        known = ctx.cats.cli_tool(name).known
        if not (m.group(2) or known):
            continue
        if i == 0:
            return i, name
        w = toks[0]
        before = toks[:i]
        if w in ("kubectl", "oc") and "--" in before:
            return i, name
        if w in ("docker", "podman") and ("exec" in before or "run" in before):
            return i, name
        if w in WRAP_ANY or w in ("bash", "sh"):
            return i, name
        return None
    return None


def commands(blocks, prose, ctx):
    """[(line no, tokens)] of the shell commands of code blocks, `$` lines and inline code."""
    cands = []
    for b in blocks:
        for no, ln in logical_lines(b["lines"]):
            cands.append((no, ln))
    for no, ln in prose:
        s = ln.strip()
        if s.startswith("$ "):
            cands.append((no, s))
    for no, sp in inline_spans(prose):
        cands.append((no, sp))
    out = []
    for no, c in cands:
        c = c.strip().lstrip("$ ").strip()
        if not c or c.startswith("#"):
            continue
        for part in SPLIT_RE.split(c):
            toks = tokenize(part.strip())
            while toks and re.match(r"^[A-Z_][A-Z0-9_]*=", toks[0]):
                toks = toks[1:]
            if toks:
                out.append((no, toks))
    return out


def long_flags(toks):
    """-> [(flag, value or None)] after the tool name; stops at a bare `--`."""
    out = []
    for i, t in enumerate(toks):
        if t == "--":
            break
        if not t.startswith("--") or not re.match(r"^--[A-Za-z][\w.-]*", t):
            continue
        if "=" in t:
            f, v = t.split("=", 1)
        else:
            f, v = t, (toks[i + 1] if i + 1 < len(toks) and not toks[i + 1].startswith("-") else None)
        out.append((f, v))
    return out


def find_kafka_commands(ctx, found, cmds):
    cats = ctx.cats
    for no, toks in cmds:
        ti = tool_in(toks, ctx)
        if ti is None:
            continue
        i, tool = ti
        for flag, val in long_flags(toks[i + 1:]):
            look = cats.cli_option(tool, flag)
            ident = "%s %s" % (tool, flag)
            if look.known:
                found.add("cli_flag", ident, EXISTS, no, look.reason)
            elif look.unknown:
                near = look.data.get("similar") or []
                found.add("cli_flag", ident, NONEXISTENT, no,
                          "not in the complete option list of %s (read from its source)%s" % (
                              tool, ("; close to " + ", ".join(near)) if near else ""))
            else:
                found.add("cli_flag", ident, UNDECIDED, no, look.reason,
                          "tool_options_incomplete" if cats.cli_tool(tool).known else "tool_not_listed")
            if flag in ("--add-config", "--config", "--add-config-file") and val and tool.startswith("kafka-"):
                config_arg(ctx, found, val, no)


def config_arg(ctx, found, val, no):
    """`--add-config 'a=1,b=2'` or `[a=1,b=2]`: keys and enumerated values."""
    val = val.strip().strip("'\"").strip("[]")
    for part in re.split(r",(?=[a-z][\w.-]*=)", val):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        add_config(ctx, found, k.strip(), v, no)


OTHER_CLIENT_RE = re.compile(r"\bkcat\b|kafkacat|librdkafka|confluent_kafka|sarama|kafka-python|\s-X\s|"
                             r"\bspring\b|\bquarkus\b|\bflink\b|\bspark\b|\bcruise.?control\b", re.I)


def add_config(ctx, found, key, value, no, scope=None, with_value=True):
    r = decide_config_key(ctx, key, scope)
    if r is None:
        return
    st, why, reason, note = r
    if st == NONEXISTENT and OTHER_CLIENT_RE.search(found.lines[no - 1] if 0 < no <= len(found.lines) else ""):
        st, why, reason = UNDECIDED, "other_client_line", "the line is about another client or product (kcat, Spring, Flink, ...)"
    if note:
        reason = (reason + "; " + note) if reason else note
    found.add("config_key", key, st, no, reason, why or ("scope_mismatch" if note else ""))
    if st == EXISTS and with_value and value is not None:
        v = decide_config_value(ctx, key, value)
        if v:
            found.add("config_value", "%s=%s" % (key, split_values(str(value))), v[0], no, v[1], v[2] if v[0] == UNDECIDED else "")


def resolve_argocd_line(toks, cats):
    """-> (command, flags, bad_subcommand). The command words are the run of non-flag words right after `argocd`;
    the longest known command prefix of that run is the command. A group command followed by a word that is not
    one of its sub-commands gives bad_subcommand."""
    words = []
    for t in toks[1:]:
        if t.startswith("-"):
            break
        words.append(t)
    cmds = set(cats.argocd_commands())
    cmd, n = "argocd", 0
    for i in range(1, len(words) + 1):
        cand = " ".join(["argocd"] + words[:i])
        if cand in cmds:
            cmd, n = cand, i
    flags = [t.split("=")[0] for t in toks[1:] if t.startswith("--") and re.match(r"^--[A-Za-z]", t)]
    bad = None
    if n < len(words) and any(c.startswith(cmd + " ") for c in cmds) and re.match(r"^[a-z][a-z0-9-]*$", words[n]):
        bad = cmd + " " + words[n]
    return cmd, flags, bad


def find_argocd(ctx, found, text, lines):
    cats = ctx.cats

    def line_of(frag):
        for no, ln in enumerate(lines, 1):
            if frag in ln:
                return no
        return 0

    for line in argocd_lines(text):
        toks = tokenize(line)
        no = line_of(line[:40]) or line_of(line.split()[-1])
        cmd, flags, bad = resolve_argocd_line(toks, cats)
        if bad:
            look = cats.argocd_command(bad)
            if look.unknown:
                # the CLI can be extended by plugins (`argocd-<name>` binaries on the PATH): not decidable
                found.add("argocd_command", bad, UNDECIDED, no,
                          "not a command of the generated CLI reference; Argo CD CLI plugins can add commands",
                          "argocd_plugins_possible")
            else:
                found.add("argocd_command", bad, UNDECIDED, no, look.reason, "argocd_cli_not_complete")
            continue
        if cmd == "argocd" and len(toks) > 1 and not toks[1].startswith("-"):
            continue
        found.add("argocd_command", cmd, EXISTS, no, "command of the generated CLI reference")
        group = cmd != "argocd" and any(c.startswith(cmd + " ") for c in cats.argocd_commands())
        for f in flags:
            look = cats.cli_option(cmd, f)
            ident = "%s %s" % (cmd, f)
            if group and not look.known:
                found.add("argocd_flag", ident, UNDECIDED, no,
                          "the line stops at a group command; its sub-command (and so the flag's owner) is not written",
                          "argocd_group_command")
            elif look.known:
                found.add("argocd_flag", ident, EXISTS, no, look.reason)
            elif look.unknown:
                found.add("argocd_flag", ident, NONEXISTENT, no,
                          "not in the complete option list of the command (own and inherited)")
            else:
                found.add("argocd_flag", ident, UNDECIDED, no, look.reason, "argocd_cli_not_complete")


# ------------------------------------------------------------------ manifests

OLD_CONTEXT_RE = re.compile(r"\b(old|older|previous|previously|legacy|deprecated|before|migrat\w*|upgrad\w*|"
                            r"replace\w*|instead of|no longer|removed|was|were|until|prior|from \S*v1beta)\b", re.I)


def find_api_versions(ctx, found, blocks):
    for b in blocks:
        body = b["lines"]
        for idx, (no, ln) in enumerate(body):
            m = re.match(r"^\s*-?\s*apiVersion:\s*['\"]?([A-Za-z0-9./_-]+)['\"]?\s*(?:#.*)?$", ln)
            if not m:
                continue
            kind = None
            for _, other in body[max(0, idx - 4): idx + 5]:
                k = re.match(r"^\s*kind:\s*['\"]?([A-Za-z0-9]+)['\"]?\s*(?:#.*)?$", other)
                if k:
                    kind = k.group(1)
                    break
            if not kind:
                continue
            api = m.group(1)
            look = ctx.cats.served_api_versions(kind, api)
            ident = "%s %s" % (kind, api)
            if look.known:
                found.add("api_version", ident, EXISTS, no, look.reason)
            elif look.unknown:
                old = OLD_CONTEXT_RE.search(b["pre"]) or OLD_CONTEXT_RE.search(ln) or \
                    any(re.match(r"^\s*#", o) and OLD_CONTEXT_RE.search(o) for _, o in body)
                if old:
                    found.add("api_version", ident, UNDECIDED, no,
                              "not served by the pinned CRD (%s), but the text around the block speaks of an "
                              "old or changed version" % ", ".join(look.data["served"]), "old_version_context")
                else:
                    found.add("api_version", ident, NONEXISTENT, no,
                              "the pinned CRD serves only %s" % ", ".join(look.data["served"]))
            else:
                found.add("api_version", ident, UNDECIDED, no, look.reason, "apiversion_not_covered")


def clean_yaml(body):
    return "\n".join("" if ln.strip() in ("...", "…") or ln.strip().startswith("# ...") else ln for ln in body.split("\n"))


def find_line(b, key, value=None):
    pats = []
    if value is not None:
        pats.append(re.compile(r"^\s*-?\s*['\"]?%s['\"]?\s*:\s*['\"]?%s\b" % (re.escape(str(key)), re.escape(str(value)))))
    pats.append(re.compile(r"^\s*-?\s*['\"]?%s['\"]?\s*:" % re.escape(str(key))))
    for pat in pats:
        for no, ln in b["lines"]:
            if pat.match(ln):
                return no
    return b["start"]


def find_manifests(ctx, found, blocks):
    cats = ctx.cats
    for b in blocks:
        body = "\n".join(t for _, t in b["lines"])
        if "kind:" not in body and not re.search(r"^\s*spec:\s*$", body, re.M):
            continue
        b["elided"] = bool(re.search(r"^\s*#\s*\.\.\.|^\s*\.\.\.\s*$|^\s*…|#\s*(?:omitted|elided|snip)", body, re.M | re.I))
        try:
            docs = [d for d in yaml.safe_load_all(clean_yaml(body)) if isinstance(d, dict)]
        except yaml.YAMLError:
            found.add("manifest_field", "block@%d" % b["start"], UNDECIDED, b["start"],
                      "the YAML block did not parse (partial snippet or placeholders)", "yaml_parse")
            continue
        for d in docs:
            kind, api = d.get("kind"), d.get("apiVersion")
            if kind is None and api is None and isinstance(d.get("spec"), dict):
                kind, api = infer_kind(ctx, d["spec"])
            if not isinstance(kind, str) or not isinstance(api, str) or kind not in ctx.all_kinds:
                continue
            srv = cats.served_api_versions(kind, api)
            if srv.unknown:
                found.add("manifest_field", "%s fields" % kind, UNDECIDED, find_line(b, "apiVersion"),
                          "apiVersion %s is not served; the catalogue holds no schema for it" % api,
                          "apiversion_not_served")
                # the keys of a config map do not depend on the schema version
                spec = d.get("spec") if isinstance(d.get("spec"), dict) else {}
                cfg, scope = {"Kafka": (spec.get("kafka", {}).get("config") if isinstance(spec.get("kafka"), dict) else None, "broker"),
                              "KafkaTopic": (spec.get("config"), "topic")}.get(kind, (None, None))
                for ck, cv in (cfg.items() if isinstance(cfg, dict) else ()):
                    add_config(ctx, found, str(ck), cv if isinstance(cv, (str, int)) and not isinstance(cv, bool) else None,
                               find_line(b, ck), scope)
                continue
            walk(ctx, found, b, kind, api, {k: v for k, v in d.items() if k not in ENVELOPE}, "")


def infer_kind(ctx, spec):
    """A snippet that starts at `spec:` and has no kind: the one CRD kind that has every top-level key of it."""
    kinds = None
    for k in spec:
        ks = set(ctx.path_kinds.get("spec." + str(k), set()))
        kinds = ks if kinds is None else kinds & ks
    if kinds and len(kinds) == 1 and next(iter(kinds)) in ctx.crd_kinds:
        kind = next(iter(kinds))
        served = sorted({e["api_version"] for e in ctx.cats._kinds.get(kind, []) if e.get("source") == "crd"})
        if len(served) == 1:
            return kind, served[0]
    return None, None


def walk(ctx, found, b, kind, api, node, prefix):
    cats = ctx.cats
    if isinstance(node, dict):
        for k, v in node.items():
            p = "%s.%s" % (prefix, k) if prefix else str(k)
            look = cats.field_path(kind, p, api)
            no = find_line(b, k)
            ident = "%s %s" % (kind, re.sub(r"\[\]", "[]", p))
            if look.known:
                found.add("manifest_field", ident, EXISTS, no, look.reason)
                info = look.data.get("info") or {}
                types = info.get("type") or []
                if (isinstance(v, dict) and types and "object" not in types) or \
                        (isinstance(v, list) and types and "array" not in types):
                    continue  # a placeholder such as {{ value }} read as a flow mapping: not a structure
                if info.get("open"):
                    if isinstance(v, dict) and p.endswith(".config") or p == "spec.config":
                        scope = {("Kafka", "spec.kafka.config"): "broker", ("KafkaTopic", "spec.config"): "topic"}.get((kind, p))
                        for ck, cv in v.items() if isinstance(v, dict) else ():
                            add_config(ctx, found, str(ck), cv if isinstance(cv, (str, int)) and not isinstance(cv, bool) else None,
                                       find_line(b, ck), scope)
                    continue
                walk(ctx, found, b, kind, api, v, p)
            elif look.unknown and look.evidence == "openapi":
                found.add("manifest_field", ident, UNDECIDED, no,
                          "not in the Kubernetes OpenAPI schema of the catalogue, which is not trusted for "
                          "nonexistence (the pinned docs show fields it lacks)", "kubernetes_schema")
                continue
            elif look.unknown:
                near = look.data.get("similar") or []
                if b.get("elided"):
                    found.add("manifest_field", ident, UNDECIDED, no,
                              "not a field of %s %s, but the snippet has an elision marker, so its structure may be "
                              "incomplete" % (kind, api), "elided_snippet")
                    continue
                found.add("manifest_field", ident, NONEXISTENT, no,
                          "not a field of %s %s in the complete schema%s" % (kind, api, ("; close to " + ", ".join(near)) if near else ""))
            elif look.reason.startswith("below the free-form"):
                continue
            else:
                found.add("manifest_field", "%s fields" % kind, UNDECIDED, no, look.reason, "schema_not_complete")
                return
    elif isinstance(node, list):
        for item in node:
            walk(ctx, found, b, kind, api, item, prefix + "[]")
    elif isinstance(node, str):
        enum = cats.field_enum(kind, prefix, api)
        if not enum or PLACEHOLDER_RE.search(node) or node.startswith(("#", "/")):
            return
        no = find_line(b, prefix.split(".")[-1].replace("[]", ""), node)
        ident = "%s %s=%s" % (kind, prefix, node)
        if node in enum:
            found.add("enum_value", ident, EXISTS, no, "listed in the enum of the schema")
        elif node.lower() in [e.lower() for e in enum]:
            found.add("enum_value", ident, UNDECIDED, no, "differs from an enum value only in case", "case_only")
        elif b.get("elided"):
            found.add("enum_value", ident, UNDECIDED, no, "not in the schema enum, but the snippet has an elision marker",
                      "elided_snippet")
        elif (cats.schema_entry(kind, api) or {}).get("source") != "crd":
            found.add("enum_value", ident, UNDECIDED, no, "Kubernetes schema is not trusted for nonexistence",
                      "kubernetes_schema")
        else:
            found.add("enum_value", ident, NONEXISTENT, no, "not in the schema enum %s" % enum)


# ------------------------------------------------------------------ paths in prose

PATH_RE = re.compile(r"^\.?(?:([A-Z][A-Za-z0-9]+)\.)?((?:spec|status)(?:\.[A-Za-z0-9_-]+(?:\[(?:\d+|\*)?\])?)+)$")


def find_prose_paths(ctx, found, prose, spans, all_text):
    cats = ctx.cats
    prose_txt = dict(prose)
    nos = [no for no, _ in prose]
    crd_in_answer = {k for k in ctx.kinds_in(all_text) if k in ctx.crd_kinds}
    answer_kinds = ctx.kinds_in(all_text)
    for no, sp in spans:
        m = PATH_RE.match(sp)
        if not m:
            continue
        kind_prefix, path = m.group(1), re.sub(r"\[(?:\d+|\*)?\]", "[]", m.group(2))
        bare = path.replace("[]", "")
        i = nos.index(no)
        heading = next((prose_txt[n] for n in reversed(nos[:i + 1]) if prose_txt[n].lstrip().startswith("#")), "")
        strong = ({kind_prefix} if kind_prefix in ctx.all_kinds else set()) | ctx.named_kinds(prose_txt[no]) | \
            ctx.named_kinds(heading)
        weak = ctx.kinds_in(" ".join(prose_txt[n] for n in nos[max(0, i - 3): i]))
        ident = path
        hits = [k for k in sorted(answer_kinds | strong | weak) if cats.field_path(k, path).known]
        if hits:
            found.add("prose_path", ident, EXISTS, no, "field of " + ", ".join(hits))
            continue
        if strong:
            looks = [(k, cats.field_path(k, path)) for k in sorted(strong)]
            if all(l.unknown for _, l in looks):
                found.add("prose_path", ident, NONEXISTENT, no,
                          "not a field of %s in the complete schema (kind named in the line or its heading)" %
                          ", ".join(k for k, _ in looks))
            else:
                found.add("prose_path", ident, UNDECIDED, no, "; ".join("%s: %s" % (k, l.reason) for k, l in looks[:2]),
                          "schema_not_complete")
            continue
        if bare in ctx.path_kinds:
            found.add("prose_path", ident, UNDECIDED, no,
                      "the path exists for %s, which the answer does not name" % ", ".join(sorted(ctx.path_kinds[bare])[:3]),
                      "path_in_other_kind")
            continue
        pool = weak or crd_in_answer
        if not pool:
            found.add("prose_path", ident, UNDECIDED, no, "the answer names no resource kind of a catalogue", "kind_not_named")
            continue
        looks = [(k, cats.field_path(k, path)) for k in sorted(pool)]
        if not all(l.unknown for _, l in looks):
            found.add("prose_path", ident, UNDECIDED, no, "; ".join("%s: %s" % (k, l.reason) for k, l in looks[:2]),
                      "schema_not_complete")
            continue
        parent = path.rsplit(".", 1)[0]
        if parent.count(".") < 1 or not any(cats.field_path(k, parent).known for k, _ in looks):
            found.add("prose_path", ident, UNDECIDED, no, "no kind in the line; the parent path is not a field of the "
                      "kinds named nearby either", "kind_not_named")
            continue
        found.add("prose_path", ident, NONEXISTENT, no,
                  "not a field of %s in the complete schema (parent path is one)" % ", ".join(k for k, _ in looks))


# ------------------------------------------------------------------ config keys in blocks and inline code

PROP_RE = re.compile(r"^\s*([a-z][a-z0-9]*(?:[._-][a-z0-9]+)*\.[a-z0-9][a-z0-9_.-]*)\s*=\s*(.*?)\s*$")
SPAN_KEY_RE = re.compile(r"^([a-z][a-z0-9_.-]*)(?:\s*=\s*(\S+))?$")


def find_config_text(ctx, found, blocks, spans):
    for b in blocks:
        if b["lang"] in ("yaml", "yml", "json", "xml", "python", "java", "go"):
            continue
        for no, ln in b["lines"]:
            m = PROP_RE.match(ln)
            if m and not ln.lstrip().startswith(("#", "//")):
                add_config(ctx, found, m.group(1), m.group(2), no)
    for no, sp in spans:
        m = SPAN_KEY_RE.match(sp)
        if m and "." in m.group(1):
            add_config(ctx, found, m.group(1), m.group(2), no)


# ------------------------------------------------------------------ REST

REST_PROSE_RE = re.compile(r"(?<![\w])(GET|POST|PUT|DELETE|PATCH)\s+`?((?:https?://[^\s/`]+)?/(?:connectors|connector-plugins|admin/loggers)[^\s`'\"|)]*)")
REST_URL_RE = re.compile(r"(?:https?://[^\s/'\"`]+)?(/(?:connectors|connector-plugins|admin/loggers)[^\s'\"`|)]*)")
DATA_FLAGS = {"-d", "--data", "--data-raw", "--data-binary", "--data-urlencode", "-F", "--form", "--json"}


def rest_decide(ctx, found, method, path, no):
    path = path.rstrip(".,;:!?)")
    segs = ctx.cats.rest_segments(path)
    if not segs:
        return
    look = ctx.cats.rest_operation(method, path)
    tpl = ctx.cats.rest_template(path) or "/" + "/".join(segs)
    ident = "%s %s" % (method, tpl)
    if look.known:
        found.add("rest_operation", ident, EXISTS, no, look.reason)
    elif look.unknown:
        allowed = look.data.get("allowed_methods")
        why = "path exists, served methods: %s" % ", ".join(allowed) if allowed else "no such path in the Connect REST document"
        found.add("rest_operation", ident, NONEXISTENT, no, why)
    else:
        found.add("rest_operation", ident, UNDECIDED, no, look.reason, "rest_not_complete")


def find_rest(ctx, found, blocks, prose):
    for no, ln in [x for b in blocks for x in logical_lines(b["lines"])] + prose:
        if re.search(r"\bcurl\b", ln):
            toks = tokenize(ln)
            if "curl" not in toks:
                toks = ln.split()
            method, data, upload = None, False, False
            for i, t in enumerate(toks):
                if t in ("-X", "--request") and i + 1 < len(toks):
                    method = toks[i + 1].upper()
                elif re.match(r"^-X[A-Za-z]+$", t):
                    method = t[2:].upper()
                elif t.startswith("--request="):
                    method = t.split("=", 1)[1].upper()
                elif t in DATA_FLAGS or any(t.startswith(f + "=") for f in DATA_FLAGS if f.startswith("--")):
                    data = True
                elif t in ("-T", "--upload-file"):
                    upload = True
            url = next((m for t in toks for m in REST_URL_RE.findall(t)), None)
            if not url:
                continue
            if method is None:
                if upload:
                    continue
                method = "POST" if data else "GET"
            if method in ("GET", "POST", "PUT", "DELETE", "PATCH"):
                rest_decide(ctx, found, method, url, no)
        for m, p in REST_PROSE_RE.findall(ln):
            rest_decide(ctx, found, m, REST_URL_RE.search(p).group(1) if REST_URL_RE.search(p) else p, no)


# ------------------------------------------------------------------ protocol errors, annotations

SNAKE_RE = re.compile(r"(?<![\w])([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)(?![\w])")
ERR_WORD_RE = re.compile(r"\b(?:error|errors|exception|exceptions)\b", re.I)
ANN_RE = re.compile(r"(?<![\w./-])strimzi\.io/([a-z][a-z0-9-]*[a-z0-9])(?![\w/])")
NOT_ANN = {"docs", "downloads", "documentation", "quickstarts", "contributing", "charts", "community", "blog",
           "install", "examples"}


def find_error_names(ctx, found, prose):
    names = ctx.error_names
    if not names or not ctx.cats.complete("protocol_errors"):
        return
    for no, ln in prose:
        if not ERR_WORD_RE.search(ln):
            continue
        for tok in set(SNAKE_RE.findall(ln)):
            if tok in names:
                found.add("protocol_error", tok, EXISTS, no, "listed in the protocol reference")
                continue
            words = set(tok.split("_"))
            jac = max((len(words & set(n.split("_"))) / len(words | set(n.split("_"))) for n in names), default=0)
            close = difflib.get_close_matches(tok, names, n=2, cutoff=0.85)
            if close or (jac >= 0.67 and len(words) >= 3):
                found.add("protocol_error", tok, NONEXISTENT, no,
                          "not an error of the protocol reference; close to " + ", ".join(close or [
                              n for n in names if len(words & set(n.split("_"))) / len(words | set(n.split("_"))) >= 0.67][:2]))


def find_annotations(ctx, found, lines):
    for no, ln in enumerate(lines, 1):
        for name in ANN_RE.findall(ln):
            if name in NOT_ANN:
                continue
            look = ctx.cats.annotation("strimzi.io/" + name)
            if look.known:
                found.add("annotation", "strimzi.io/" + name, EXISTS, no, look.reason)
            else:
                found.add("annotation", "strimzi.io/" + name, UNDECIDED, no,
                          "the annotation catalogue lists names the docs mention; it is not complete", "annotation_not_complete")


# ------------------------------------------------------------------ one answer and the totals

def check_text(text, ctx):
    lines, blocks, prose = parse_answer(text)
    found = Found(lines)
    spans = inline_spans(prose)
    find_scripts(ctx, found, lines)
    cmds = commands(blocks, prose, ctx)
    find_kafka_commands(ctx, found, cmds)
    find_argocd(ctx, found, "\n".join(lines), lines)
    find_api_versions(ctx, found, blocks)
    find_manifests(ctx, found, blocks)
    find_prose_paths(ctx, found, prose, spans, "\n".join(lines))
    find_config_text(ctx, found, blocks, spans)
    find_rest(ctx, found, blocks, prose)
    find_error_names(ctx, found, prose)
    find_annotations(ctx, found, lines)
    return found.result()


def check_rows(rows, ctx):
    """-> list of (id, result or None). A row with an error or without text is skipped (result None)."""
    out = []
    for i, r in enumerate(rows):
        rid = r.get("id") or "#%d" % (i + 1)
        if r.get("error") or not (r.get("text") or "").strip():
            out.append((rid, None))
        else:
            out.append((rid, check_text(r["text"], ctx)))
    return out


def totals(results):
    done = [r for _, r in results if r is not None]
    n = len(done)
    ne = sum(r["nonexistent"] for r in done)
    ex = sum(r["exists"] for r in done)
    by = {k: Counter() for k in KINDS}
    why = Counter()
    for r in done:
        for it in r["items"]:
            by[it["kind"]][it["status"]] += 1
            if it["status"] == UNDECIDED:
                why[it["why"] or "other"] += 1
    return {"answers": len(results), "answers_checked": n, "skipped": len(results) - n,
            "answers_with_nonexistent": sum(1 for r in done if r["nonexistent"]),
            "nonexistent": ne, "nonexistent_per_10_answers": round(10 * ne / n, 2) if n else None,
            "exists": ex, "undecided": sum(r["undecided"] for r in done),
            "share_exists_of_decided": round(ex / (ex + ne), 4) if ex + ne else None,
            "by_kind": {k: {s: by[k][s] for s in (EXISTS, NONEXISTENT, UNDECIDED)} for k in KINDS if sum(by[k].values())},
            "undecided_why": dict(sorted(why.items()))}


def format_totals(t, name, hidden=False):
    L = ["== idcheck %s%s  (%s)" % (name, " (hidden: totals only)" if hidden else "", VERSION),
         "answers %d  checked %d  skipped (error or no text) %d" % (t["answers"], t["answers_checked"], t["skipped"]),
         "answers with at least one nonexistent identifier: %d of %d" % (t["answers_with_nonexistent"], t["answers_checked"]),
         "nonexistent identifiers: %d  (%s per 10 answers)" % (t["nonexistent"], t["nonexistent_per_10_answers"]),
         "decided identifiers that exist: %d of %d  (share %s)  undecided %d" % (
             t["exists"], t["exists"] + t["nonexistent"], t["share_exists_of_decided"], t["undecided"]),
         "%-16s %7s %12s %10s" % ("by kind", "exists", "nonexistent", "undecided")]
    for k, c in t["by_kind"].items():
        L.append("%-16s %7d %12d %10d" % (k, c[EXISTS], c[NONEXISTENT], c[UNDECIDED]))
    if t["undecided_why"]:
        L.append("undecided because: " + ", ".join("%s %d" % kv for kv in t["undecided_why"].items()))
    return "\n".join(L)


def report_block(rows, hidden, catalogues=None):
    """Totals text for the harness report. For a hidden run it is totals only, by construction."""
    ctx = Ctx(Catalogues.load(catalogues))
    return format_totals(totals(check_rows(rows, ctx)), "free-text answers", hidden=bool(hidden))


def format_answer(rid, res, show_undecided=False):
    L = ["-- %s  nonexistent %d  exists %d  undecided %d" % (rid, res["nonexistent"], res["exists"], res["undecided"])]
    for it in res["items"]:
        if it["status"] == NONEXISTENT or (show_undecided and it["status"] == UNDECIDED):
            L.append("   %s [%s] %s  line %s: %s\n      %s%s" % (
                it["status"].upper(), it["kind"], it["ident"], it["line_no"], it["line"][:120], it["reason"],
                ("  (" + it["why"] + ")") if it["why"] else ""))
    return "\n".join(L)


def load_rows(path):
    p = Path(path)
    if str(p).endswith(".enc"):
        import eval2  # noqa: E402  (decrypts with the hidden key; only the hidden path needs it)
        return eval2.read_jsonl(p), True
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()], False


def main(argv=None, out=None):
    out = out or sys.stdout
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--answers", required=True, help="answers file written by eval2.py answer (or a stored answers file)")
    ap.add_argument("--catalogues", default=None, help="catalogues folder (default work/m4/catalogues)")
    ap.add_argument("--totals-only", action="store_true", help="print the totals and nothing per answer or per item")
    ap.add_argument("--show-undecided", action="store_true", help="also list undecided identifiers per answer")
    ap.add_argument("--json", default=None, help="write the full per-answer result to this new file")
    a = ap.parse_args(argv)
    rows, hidden = load_rows(a.answers)
    if hidden and (a.json or a.show_undecided):
        print("hidden answers: totals only (--json and --show-undecided are refused)", file=sys.stderr)
        return 2
    cdir = Path(a.catalogues) if a.catalogues else default_dir()
    ctx = Ctx(Catalogues.load(cdir))
    results = check_rows(rows, ctx)
    totals_only = a.totals_only or hidden
    if not totals_only:
        for rid, res in results:
            print(format_answer(rid, res, a.show_undecided) if res else "-- %s  skipped (error or no text)" % rid, file=out)
        print(file=out)
    name = "%s" % ("hidden run" if hidden else Path(a.answers).name)
    print(format_totals(totals(results), name, hidden), file=out)
    try:
        print("catalogues %s" % catalogue_hash(cdir)[:12], file=out)
    except Exception:
        pass
    if a.json:
        p = Path(a.json)
        if p.exists():
            print("refusing to overwrite %s" % p, file=sys.stderr)
            return 2
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"version": VERSION, "answers": [{"id": i, **r} for i, r in results if r],
                                 "totals": totals(results)}, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
