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

Identifier precision is computed over the whole answer text, independent of the item's own
checks: every code-like token that can be classified is looked up in the catalogues:
  Kafka configuration keys (dotted names, also the keys under `config:` in a YAML manifest),
  Connect REST method and path pairs, `argocd` commands and flags, Kafka protocol error names,
  Strimzi field paths in YAML manifests and dotted spec paths, `strimzi.io/` annotations.
The summary reports known, unknown and not covered. "unknown" comes only from a catalogue (or a
tool) that is marked complete; everything else is "not covered" and is never called invented.

This scorer shares only the catalogue files and kafka_sre_rag/catalogue.py with anything else. It
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

VERSION = "eval2_score/1"
FORMAT_CHECKS = ("answer_line", "has_command")
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
}


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
KTOOL_LINE_RE = re.compile(r"(?<![\w-])(kafka-[a-z0-9-]+)(?:\.sh)?\b([^\n]*)")
SNAKE_RE = re.compile(r"(?<![\w])([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)(?![\w])")
STRIMZI_KINDS = ("KafkaBridge", "KafkaConnect", "KafkaConnector", "KafkaMirrorMaker2", "KafkaNodePool",
                 "KafkaRebalance", "KafkaTopic", "KafkaUser", "StrimziPodSet", "Kafka")
SPEC_PATH_RE = re.compile(r"(?<![\w.\-/])((?:spec|status)(?:\.[A-Za-z0-9_]+(?:\[(?:\d+|\*)?\])?)+)")


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
    prefixes = key_prefixes(cats)
    mentioned = set()
    for d in yaml_docs(text):
        kind = d.get("kind")
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
                        sc = "broker" if look.data["field"].endswith("kafka.config") else None
                        ids.add("kafka_config_key", leaf, cats.config_key(leaf, sc))
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
    for tool, rest in KTOOL_LINE_RE.findall(scan):
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


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--items", required=True)
    ap.add_argument("--answers", required=True)
    ap.add_argument("--scores", help="write the per-item score rows here (jsonl)")
    ap.add_argument("--summary", help="write the summary here (json)")
    ap.add_argument("--catalogues", help="catalogue directory (default work/m4/catalogues)")
    args = ap.parse_args(argv)
    cats = Catalogues.load(args.catalogues)
    items = load_jsonl(args.items)
    answers = load_jsonl(args.answers)
    rows, missing, extra = score_files(items, answers, cats)
    from kafka_sre_rag.catalogue import catalogue_hash
    summ = summarise(rows, items, missing, extra, catalogue_hash(args.catalogues))
    if args.scores:
        Path(args.scores).write_text("".join(json.dumps(r, sort_keys=True, ensure_ascii=False) + "\n" for r in rows))
    text = json.dumps(summ, indent=1, sort_keys=True, ensure_ascii=False)
    if args.summary:
        Path(args.summary).write_text(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
