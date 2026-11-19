#!/usr/bin/env python3
"""Checks of the a3 slice of tier A (WP1c): semantic rules, command order, numbers with units, declines, forbidden keys.

`CHECKS_A3` is merged into `CHECKS` of scripts/eval2_score.py (version eval2_score/3). A check is per item and
opt-in: an item without one of these check types is scored exactly as before. Every check has the signature
`fn(chk, text, cats) -> (ok, detail)` of the existing checks. No model, no network, nothing is executed (the one
exception is the test of `parse_properties` against a real JDK, in tests/test_eval2_checks_a3.py).

The rules each oracle applies come from the pinned sources under raw/ (the line numbers are those of the pinned
files; WP1c T1a's semantic_rules.json anchors the same ones):

  netpol_allows        raw/kubernetes/content/en/docs/concepts/services-networking/network-policies.md
                       l.105-107 (an empty podSelector selects all pods of the policy's namespace), l.147-160
                       (podSelector alone = the policy's own namespace; namespaceSelector alone = all pods of the
                       matching namespaces; both in one entry = pods in namespaces, AND), l.171-186 (two entries =
                       OR), l.355 (the label kubernetes.io/metadata.name is set on every namespace), l.87-91 and
                       l.77-85 (ports, endPort), l.209-212 ("By default, if no policies exist ... allowed").
                       NOT in the pinned text: that an empty or missing `from` matches every source. The page
                       describes it only through a `code_sample` file (network-policy-allow-all-ingress.yaml)
                       that is not in raw/. The oracle implements it as the brief says and reports it
                       (RULES: netpol.empty_from_all, anchored False). The Kubernetes API
                       reference of NetworkPolicyIngressRule is not in raw/ (only content/en/docs), so the rule stays unanchored.
  restart_annotation   raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc l.30-46
                       (strimzi.io/restart on KafkaConnector: "true", includeTasks, onlyFailed, both default false;
                       example value "includeTasks,onlyFailed"), proc-manual-restart-connector-task.adoc l.35-40
                       (strimzi.io/restart-task="<task id>", ids are non-negative integers),
                       proc-manual-restart-mirrormaker2-connector.adoc l.30-58 (strimzi.io/restart-connector=
                       "<name>[:includeTasks,onlyFailed]" on KafkaMirrorMaker2),
                       proc-manual-restart-mirrormaker2-connector-task.adoc l.31-45
                       (strimzi.io/restart-connector-task="<name>:<task id>").
  config_rule          the entries of raw/apache-kafka-config/content/en/43/generated/consumer_config.html
                       l.63 (heartbeat.interval.ms: "lower than session.timeout.ms, but typically ... no higher
                       than 1/3 of that value") and l.103 (session.timeout.ms), topic_config.html l.245
                       (min.insync.replicas: with acks=all the write fails when the ISR holds fewer than
                       min.insync.replicas members). The rule engine only evaluates numeric relations an item
                       states; the relation itself is the item's (the generator cites the source).
  properties_parse     java.util.Properties#load(Reader) as documented by the JDK Javadoc; not in raw/. The parser
                       is validated against the installed JDK by tests/test_eval2_checks_a3.py (skipped without
                       javac/java). T1a's table may cite Utils.loadProps of the sparse clone; if that is absent
                       the rule stays "JDK behaviour, unanchored in the index".
  pdb_evicts           raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md l.84-102 (integers
                       and percentages, rounded up), l.123 (only one of the two fields), l.129-141 (the four
                       examples), l.155-160 (maxUnavailable 0 or minAvailable 100% / the number of replicas =
                       zero voluntary evictions); concepts/workloads/pods/disruptions.md l.114-117.
  forbidden_keys       work/m4/catalogues_a3/strimzi_forbidden.json, read only through
                       kafka_sre_rag.catalogue_a3.forbidden (T1a, commit 34d9c2e).

Every rule an oracle applies is listed in RULES with `anchored` and its source, in step with
work/m4/catalogues_a3/semantic_rules.json (mechanism V1, V2, V4 ... of T1a). Generators build no item on a rule with
`anchored: False`; the scorer still evaluates it, and a verdict that used one carries "[unanchored rule ...]" in its
detail.
"""
import importlib
import ipaddress
import json
import math
import os
import re
import shlex
from fractions import Fraction
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


class _Lazy:
    """eval2_score imports this module to merge CHECKS_A3, so its helpers are looked up at call time."""

    def __getattr__(self, name):
        return getattr(importlib.import_module("eval2_score"), name)


sc = _Lazy()
NP = "raw/kubernetes/content/en/docs/concepts/services-networking/network-policies.md"
RS = "raw/strimzi/documentation/modules/configuring/"
PDB = "raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md"
KC = "raw/apache-kafka-config/content/en/43/generated/"
RULES = {
    "netpol.pod_selector_own_namespace": {"mechanism": "V1", "anchored": True, "source": NP + ":147"},
    "netpol.namespace_selector_all_pods": {"mechanism": "V1", "anchored": True, "source": NP + ":150"},
    "netpol.both_in_one_entry_is_and": {"mechanism": "V1", "anchored": True, "source": NP + ":153"},
    "netpol.separate_entries_are_or": {"mechanism": "V1", "anchored": True, "source": NP + ":187"},
    "netpol.empty_selector_selects_all": {"mechanism": "V1", "anchored": True, "source": NP + ":106"},
    "netpol.empty_from_all": {"mechanism": "V1", "anchored": False,
                              "source": "none: network-policies.md l.224-232 describes allow-all only through the "
                                        "code sample network-policy-allow-all-ingress.yaml, which is not in raw/; "
                                        "no API reference of NetworkPolicyIngressRule in raw/"},
    "restart.connector_true_or_params": {"mechanism": "V2", "anchored": True, "source": RS + "proc-manual-restart-connector.adoc:33,35-39,46"},
    "restart.task_id": {"mechanism": "V2", "anchored": True, "source": RS + "proc-manual-restart-connector-task.adoc:39-40"},
    "restart.mm2_connector": {"mechanism": "V2", "anchored": True, "source": RS + "proc-manual-restart-mirrormaker2-connector.adoc:36,50-58"},
    "restart.mm2_connector_task": {"mechanism": "V2", "anchored": True, "source": RS + "proc-manual-restart-mirrormaker2-connector-task.adoc:38,45"},
    "config.heartbeat_below_session_third": {"mechanism": "V5", "anchored": True, "source": KC + "consumer_config.html:64"},
    "config.min_isr_below_replicas": {"mechanism": "V6", "anchored": True, "derived": True,
                                      "source": KC + "kafka_config.html:521; " + KC + "topic_config.html:246"},
    "pdb.zero_evictions": {"mechanism": "V7", "anchored": True, "source": PDB + ":155-157"},
    "pdb.percent_rounds_up": {"mechanism": "V7", "anchored": True, "source": PDB + ":97-102,138-141"},
    "pdb.one_field_only": {"mechanism": "V7", "anchored": True, "source": PDB + ":123"},
    "properties.load_semantics": {"mechanism": "V4", "anchored": False,
                                  "source": "java.util.Properties#load Javadoc; checked by running JDK 19 "
                                            "(tests); T1b anchors it to Utils.loadProps"},
}


def rule_anchored(rule_id):
    return RULES[rule_id]["anchored"]


def unanchored_rules():
    return sorted(k for k, v in RULES.items() if not v["anchored"])


LANG_FENCE_RE = re.compile(r"```[ \t]*([A-Za-z0-9_+-]*)[^\n]*\n(.*?)```", re.S)


def fenced(text, langs=None):
    """[(language, body)] of the code fences of an answer, in order; langs filters by label (lower case)."""
    out = [(m.group(1).lower(), m.group(2)) for m in LANG_FENCE_RE.finditer(text or "")]
    return [b for b in out if not langs or b[0] in langs]


# =============================================================== semantic_rule: label selectors and NetworkPolicy
def selector_matches(sel, labels):
    """A Kubernetes label selector against a label map. None or {} selects everything (an empty selector selects
    all: network-policies.md l.105-107, configure-pdb.md l.118-120). matchLabels is an AND of equalities;
    matchExpressions: In, NotIn, Exists, DoesNotExist (assign-pod-node.md l.138)."""
    if sel is None:
        return True
    if not isinstance(sel, dict):
        return False
    labels = {str(k): str(v) for k, v in (labels or {}).items()}
    for k, v in (sel.get("matchLabels") or {}).items():
        if labels.get(str(k)) != str(v):
            return False
    for ex in sel.get("matchExpressions") or []:
        key, op, vals = str(ex.get("key")), ex.get("operator"), [str(v) for v in (ex.get("values") or [])]
        if op == "In":
            ok = key in labels and labels[key] in vals
        elif op == "NotIn":
            ok = key not in labels or labels[key] not in vals
        elif op == "Exists":
            ok = key in labels
        elif op == "DoesNotExist":
            ok = key not in labels
        else:
            return False
        if not ok:
            return False
    return True


def _port_ok(ports, port, protocol):
    """No ports list (or an empty one) allows every port; an entry has `port` (number or name), optional `endPort`
    (>= port, numeric: network-policies.md l.87-91) and `protocol` (TCP by default)."""
    if not ports:
        return True
    if port is None:
        return True
    for p in ports:
        if not isinstance(p, dict):
            continue
        if str(p.get("protocol", "TCP")).upper() != str(protocol or "TCP").upper():
            continue
        want = p.get("port")
        if want is None:
            return True
        if isinstance(want, int) and isinstance(port, int):
            end = p.get("endPort")
            if (want <= port <= end) if isinstance(end, int) else want == port:
                return True
        elif str(want) == str(port):
            return True
    return False


def _peer_matches(peer, src, policy_ns, namespaces):
    """One entry of `from`. podSelector alone: pods of the policy's own namespace; namespaceSelector alone: all
    pods of the matching namespaces; both: the pods that match podSelector within matching namespaces (AND);
    ipBlock: the source address in the CIDR and not in `except`."""
    if not isinstance(peer, dict) or not ({"podSelector", "namespaceSelector", "ipBlock"} & set(peer)):
        return False  # an entry without any selector is not valid; it selects nothing
    if "ipBlock" in peer:
        if len(peer) > 1 or not src.get("ip"):
            return False
        blk = peer["ipBlock"] or {}
        try:
            ip = ipaddress.ip_address(src["ip"])
            if ip not in ipaddress.ip_network(blk.get("cidr"), strict=False):
                return False
            return not any(ip in ipaddress.ip_network(x, strict=False) for x in blk.get("except") or [])
        except (ValueError, TypeError):
            return False
    ns = src.get("namespace")
    pod = src.get("pod_labels") or {}
    if "namespaceSelector" in peer:
        nslabels = {"kubernetes.io/metadata.name": ns, **(namespaces.get(ns) or {})}  # l.355
        if not selector_matches(peer["namespaceSelector"], nslabels):
            return False
        return selector_matches(peer["podSelector"], pod) if "podSelector" in peer else True
    return ns == policy_ns and selector_matches(peer["podSelector"], pod)


def netpol_allows(policy, source, target, namespaces=None, port=None, protocol="TCP"):
    """Does this NetworkPolicy let `source` reach `target` (ingress)?
    -> (allowed, reason). `source`: {namespace, pod_labels, ip?}; `target`: {namespace, pod_labels};
    `namespaces`: {name: labels}. A policy that does not select the target pod, or whose policyTypes lack
    Ingress, does not decide the question: the reason says so and `allowed` is None."""
    namespaces = namespaces or {}
    meta, spec = policy.get("metadata") or {}, policy.get("spec") or {}
    policy_ns = meta.get("namespace") or target.get("namespace")
    if policy_ns != target.get("namespace"):
        return None, "the policy is in namespace %s, the target pod in %s" % (policy_ns, target.get("namespace"))
    if not selector_matches(spec.get("podSelector"), target.get("pod_labels")):
        return None, "podSelector does not select the target pod"
    if "Ingress" not in (spec.get("policyTypes") or ["Ingress"]):
        return None, "policyTypes has no Ingress"
    for rule in spec.get("ingress") or []:   # no rule (or none at all) = deny all ingress
        rule = rule or {}
        if not _port_ok(rule.get("ports"), port, protocol):
            continue
        frm = rule.get("from")
        if not frm:                          # empty or missing `from`: every source (UNANCHORED, see the module doc)
            return True, "a rule without `from` allows every source [unanchored rule netpol.empty_from_all]"
        for peer in frm:                     # separate entries are OR
            if _peer_matches(peer, source, policy_ns, namespaces):
                return True, "an entry of `from` matches the source"
    return False, "no rule matches the source"


def _first_doc(text, kind):
    for d in sc.answer_yaml_docs(text):
        if d.get("kind") == kind:
            return d
    return None


def oracle_netpol_allows(chk, text):
    pol = _first_doc(text, "NetworkPolicy")
    if pol is None:
        return False, "no NetworkPolicy document in the answer"
    got, why = netpol_allows(pol, chk["source"], chk["target"], chk.get("namespaces"), chk.get("port"),
                             chk.get("protocol", "TCP"))
    if got is None:
        return False, why
    return got == bool(chk["expect"]), "policy %s the source (%s); expected %s" % (
        "allows" if got else "denies", why, "allow" if chk["expect"] else "deny")


# =============================================================== semantic_rule: Strimzi restart annotations
CANON_KINDS = {"kafkaconnector": "KafkaConnector", "kafkamirrormaker2": "KafkaMirrorMaker2"}
PARAMS = {"includeTasks", "onlyFailed"}


def _restart_params(s, allow_true):
    """The value forms of the pinned text -> (include_tasks, only_failed), or None.
    KafkaConnector `strimzi.io/restart`: "true" alone (proc-manual-restart-connector.adoc l.33), or the two
    parameters includeTasks / onlyFailed (each defaults to false, l.35-39), a comma list of them as in l.46.
    `true` combined with parameters is not shown anywhere and is rejected. MirrorMaker 2 has no `true`:
    `<connector>:includeTasks,onlyFailed` (proc-manual-restart-mirrormaker2-connector.adoc l.50-58)."""
    toks = [t.strip() for t in s.split(",")] if s else []
    if allow_true and toks == ["true"]:
        return (False, False)
    if not toks or any(t not in PARAMS for t in toks) or len(set(toks)) != len(toks):
        return None
    return ("includeTasks" in toks, "onlyFailed" in toks)


def parse_restart_annotation(kind, name, value):
    """The effect of one annotation on a KafkaConnector or KafkaMirrorMaker2 -> dict with `valid` and, when valid,
    restart_connector, include_tasks, only_failed, task_id, connector. A kind/name pair the pinned docs do not
    define is not valid (no effect)."""
    value = "" if value is None else str(value).strip()
    bad = {"valid": False, "restart_connector": False, "include_tasks": False, "only_failed": False,
           "task_id": None, "connector": None}
    out = dict(bad, valid=True)
    if kind == "KafkaConnector" and name == "strimzi.io/restart":
        p = _restart_params(value, True)
        if p is None:
            return bad
        return dict(out, restart_connector=True, include_tasks=p[0], only_failed=p[1])
    if kind == "KafkaConnector" and name == "strimzi.io/restart-task":
        return dict(out, task_id=int(value)) if re.fullmatch(r"\d+", value) else bad
    if kind == "KafkaMirrorMaker2" and name == "strimzi.io/restart-connector":
        con, _, par = value.rpartition(":") if re.search(r":(?:includeTasks|onlyFailed)(?:,|$)", value) \
            else (value, "", "")
        if not con:
            return bad
        p = _restart_params(par, False) if par else (False, False)
        if p is None:
            return bad
        return dict(out, restart_connector=True, include_tasks=p[0], only_failed=p[1], connector=con)
    if kind == "KafkaMirrorMaker2" and name == "strimzi.io/restart-connector-task":
        con, _, tid = value.rpartition(":")
        return dict(out, connector=con, task_id=int(tid)) if con and re.fullmatch(r"\d+", tid) else bad
    return bad


KUBECTL_VALUE_FLAGS = {"-n", "--namespace", "-l", "--selector", "--context", "--kubeconfig", "--field-manager",
                       "-f", "--filename", "-o", "--output"}


def answer_annotations(text):
    """[(kind, annotation, value)] set by the answer: `kubectl annotate <Kind> <name> key=value` lines and
    metadata.annotations of fenced manifests."""
    out = []
    flat = re.sub(r"\\[ \t]*\n[ \t]*", " ", text or "")
    for line in flat.split("\n") + re.findall(r"`([^`\n]+)`", flat):
        line = line.strip().lstrip("$ ").strip()
        toks = sc.tokenize(line)
        if "annotate" not in toks or "kubectl" not in toks:
            continue
        rest = toks[toks.index("annotate") + 1:]
        pos, i = [], 0
        while i < len(rest):
            t = rest[i]
            if t in KUBECTL_VALUE_FLAGS:
                i += 2
                continue
            if not t.startswith("-"):
                pos.append(t)
            i += 1
        if not pos:
            continue
        res = pos[0]
        kind = CANON_KINDS.get(res.split(".")[0].split("/")[0].lower())
        for t in pos:
            if re.match(r"^[\w.-]+/[\w.-]+=", t):
                k, v = t.split("=", 1)
                out.append((kind, k, v))
    for d in sc.answer_yaml_docs(text):
        anns = (d.get("metadata") or {}).get("annotations") or {}
        for k, v in anns.items():
            out.append((d.get("kind"), str(k), v))
    return out


def oracle_restart_annotation(chk, text):
    want = chk["expect"]
    seen = []
    for kind, ann, val in answer_annotations(text):
        if kind != chk["kind"] or not str(ann).startswith("strimzi.io/restart"):
            continue
        eff = parse_restart_annotation(kind, ann, val)
        seen.append("%s=%s -> %s" % (ann, val, "valid" if eff["valid"] else "no effect"))
        if all(eff.get(k) == v for k, v in want.items()) and (eff["valid"] or want.get("valid") is False):
            return True, "annotation %s=%r has the wanted effect" % (ann, val)
    return False, ("no annotation with the wanted effect %s; found: %s" % (want, "; ".join(seen) or "none"))


# =============================================================== semantic_rule: numeric relations between config values
def _num(v):
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return Fraction(v).limit_denominator(10 ** 9)
    if isinstance(v, str) and re.fullmatch(r"\s*[-+]?\d+(?:\.\d+)?\s*", v):
        return Fraction(v.strip())
    return None


def parse_properties(text):
    """java.util.Properties#load(Reader) semantics -> dict (a later key replaces an earlier one).
    Natural lines end at \\n, \\r or \\r\\n; a line whose first non-blank character is # or ! is a comment (never
    continued); a line ending in an odd number of backslashes continues on the next line, whose leading blanks are
    dropped. Blank = space, tab, form feed. The key runs to the first unescaped =, : or blank; then blanks, at most
    one = or :, and blanks again are skipped; the rest of the line is the value, with trailing blanks and any `#`
    kept. Escapes: \\t \\n \\r \\f, \\uXXXX (a malformed one raises ValueError), any other \\c is c."""
    lines = re.split(r"\r\n|\r|\n", text)
    if lines and lines[-1] == "":
        lines.pop()
    BL = " \t\f"
    out, i = {}, 0
    while i < len(lines):
        ln = lines[i].lstrip(BL)
        i += 1
        if not ln or ln[0] in "#!":
            continue
        while True:  # continuation: an odd number of trailing backslashes
            n = len(ln) - len(ln.rstrip("\\"))
            if n % 2 == 0:
                break
            ln = ln[:-1]
            if i >= len(lines):
                break
            ln += lines[i].lstrip(BL)
            i += 1
        k, back = 0, False
        while k < len(ln):
            c = ln[k]
            if not back and c in "=:":
                break
            if not back and c in BL:
                break
            back = (not back) if c == "\\" else False
            k += 1
        v = k + 1
        sep = k < len(ln) and ln[k] in "=:"
        while v < len(ln):
            c = ln[v]
            if c not in BL:
                if not sep and c in "=:":
                    sep = True
                else:
                    break
            v += 1
        out[_unescape(ln[:k])] = _unescape(ln[v:] if k < len(ln) else "")
    return out


def _unescape(s):
    out, i = [], 0
    while i < len(s):
        c = s[i]
        i += 1
        if c != "\\":
            out.append(c)
            continue
        if i >= len(s):
            break  # a lone trailing backslash is dropped
        c = s[i]
        i += 1
        if c == "u":
            hx = s[i:i + 4]
            if len(hx) < 4 or not re.fullmatch(r"[0-9a-fA-F]{4}", hx):
                raise ValueError("malformed \\uxxxx encoding")
            out.append(chr(int(hx, 16)))
            i += 4
        else:
            out.append({"t": "\t", "n": "\n", "r": "\r", "f": "\f"}.get(c, c))
    return "".join(out)


def artifact_values(text):
    """Config values an answer sets: the Properties blocks (language properties/ini/conf or unlabelled blocks that
    are not YAML) and the config maps of YAML documents (spec.config, spec.kafka.config, nested config
    mappings flattened with dots), plus the documents themselves for `spec.` paths."""
    props, docs = {}, sc.answer_yaml_docs(text)
    for lang, body in fenced(text):
        if lang in ("yaml", "yml", "json", "bash", "sh", "shell", "java", "python"):
            continue
        if lang not in ("properties", "ini", "conf", "cfg", "") or (lang == "" and re.search(r"^\s*apiVersion\s*:", body, re.M)):
            continue
        try:
            props.update(parse_properties(body))
        except ValueError:
            continue
    flat = {}
    for d in docs:
        for path in (("spec", "config"), ("spec", "kafka", "config")):
            ok, cfg = sc.path_get(d, list(path))
            if ok and isinstance(cfg, dict):
                for k, v in _flatten(cfg):
                    flat[k] = v
    return props, flat, docs


def _flatten(m, prefix=""):
    for k, v in m.items():
        key = prefix + str(k)
        if isinstance(v, dict):
            yield from _flatten(v, key + ".")
        else:
            yield key, v


def _operand(spec, props, flat, docs, defaults):
    """-> (Fraction or None, label). spec: a number, {"const": n}, {"path": "spec.replicas"} or a config key."""
    if isinstance(spec, dict) and "const" in spec:
        return _num(spec["const"]), str(spec["const"])
    if isinstance(spec, dict) and "path" in spec:
        for d in docs:
            ok, v = sc.path_get(d, [int(s) if s.isdigit() else s for s in spec["path"].split(".")])
            if ok and _num(v) is not None:
                return _num(v), spec["path"]
        return _num(defaults.get(spec["path"])), spec["path"]
    if isinstance(spec, (int, float)):
        return _num(spec), str(spec)
    key = str(spec)
    for src in (props, flat):
        if key in src and _num(src[key]) is not None:
            return _num(src[key]), key
    return _num(defaults.get(key)), key


OPS = {"lt": lambda a, b: a < b, "le": lambda a, b: a <= b, "gt": lambda a, b: a > b, "ge": lambda a, b: a >= b,
       "eq": lambda a, b: a == b, "ne": lambda a, b: a != b}


def oracle_config_rule(chk, text):
    """chk["rules"]: [{"a": operand, "op": lt|le|gt|ge|eq|ne, "b": operand, "scale": "1/3"}] meaning
    a op b*scale (scale is a Fraction string, default 1). Example, heartbeat below session and at most a third:
    [{"a": "heartbeat.interval.ms", "op": "lt", "b": "session.timeout.ms"},
     {"a": "heartbeat.interval.ms", "op": "le", "b": "session.timeout.ms", "scale": "1/3"}].
    `defaults` gives the value of a key the answer leaves out. All rules must hold."""
    props, flat, docs = artifact_values(text)
    defaults = chk.get("defaults") or {}
    bad = []
    for r in chk["rules"]:
        a, la = _operand(r["a"], props, flat, docs, defaults)
        b, lb = _operand(r["b"], props, flat, docs, defaults)
        if a is None or b is None:
            bad.append("%s or %s not set to a number" % (la, lb))
            continue
        scale = Fraction(r.get("scale", 1))
        if not OPS[r["op"]](a, b * scale):
            bad.append("%s=%s is not %s %s=%s%s" % (la, a, r["op"], lb, b, " x %s" % scale if scale != 1 else ""))
    return not bad, "; ".join(bad) if bad else "every numeric relation holds"


def oracle_properties_parse(chk, text):
    """The answer's Properties block, read with Java semantics, must give `expect` {key: value} (and, with
    `exact`, no other keys). The block is the last one labelled properties, else the last unlabelled one."""
    blocks = fenced(text, ("properties", "ini", "conf", "cfg")) or fenced(text, ("",))
    if not blocks:
        return False, "no properties block"
    try:
        got = parse_properties(blocks[-1][1])
    except ValueError as e:
        return False, "the block does not load: %s" % e
    bad = ["%s=%r, expected %r" % (k, got.get(k), v) for k, v in chk["expect"].items() if got.get(k) != v]
    if chk.get("exact") and set(got) - set(chk["expect"]):
        bad.append("extra keys %s" % sorted(set(got) - set(chk["expect"])))
    return not bad, "; ".join(bad) if bad else "Properties.load gives the expected values [unanchored rule properties.load_semantics]"


# =============================================================== semantic_rule: PodDisruptionBudget
def _pdb_count(v, replicas):
    """int or 'N%' -> number of pods, percentages rounded up (configure-pdb.md l.97-102)."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    m = re.fullmatch(r"\s*(\d+)\s*%\s*", str(v))
    if m:
        return math.ceil(Fraction(int(m.group(1)) * replicas, 100))
    return int(v) if isinstance(v, str) and re.fullmatch(r"\s*\d+\s*", v) else None


def pdb_allowed_disruptions(pdb, replicas):
    """Voluntary evictions the budget allows with `replicas` healthy pods -> (n or None, reason).
    minAvailable m: replicas - m; maxUnavailable k: k, never more than replicas. Only one of the two fields may be
    set (l.123). Neither field: no verdict (not stated in the pinned text)."""
    spec = pdb.get("spec") or {}
    has_min, has_max = "minAvailable" in spec, "maxUnavailable" in spec
    if has_min and has_max:
        return None, "both minAvailable and maxUnavailable are set; only one is allowed"
    if not (has_min or has_max):
        return None, "neither minAvailable nor maxUnavailable is set"
    if has_min:
        m = _pdb_count(spec["minAvailable"], replicas)
        return (None, "minAvailable %r is not a number or a percentage" % spec["minAvailable"]) if m is None \
            else (max(0, replicas - m), "minAvailable %s of %d" % (m, replicas))
    k = _pdb_count(spec["maxUnavailable"], replicas)
    return (None, "maxUnavailable %r is not a number or a percentage" % spec["maxUnavailable"]) if k is None \
        else (min(max(0, k), replicas), "maxUnavailable %s of %d" % (k, replicas))


def oracle_pdb_evicts(chk, text):
    pdb = _first_doc(text, "PodDisruptionBudget")
    if pdb is None:
        return False, "no PodDisruptionBudget document in the answer"
    n, why = pdb_allowed_disruptions(pdb, int(chk["replicas"]))
    if n is None:
        return False, why
    allows = n > 0
    return allows == bool(chk["expect"]), "%s: %d eviction(s) allowed with %d replicas; expected %s" % (
        why, n, chk["replicas"], "an eviction" if chk["expect"] else "no eviction")


ORACLES = {"netpol_allows": oracle_netpol_allows, "restart_annotation": oracle_restart_annotation,
           "config_rule": oracle_config_rule, "properties_parse": oracle_properties_parse,
           "pdb_evicts": oracle_pdb_evicts}


def check_semantic_rule(chk, text, cats):
    fn = ORACLES.get(chk.get("oracle"))
    if fn is None:
        raise ValueError("unknown oracle %r" % chk.get("oracle"))
    return fn(chk, text)


# =============================================================== cmd_order
def ordered_commands(text, tools):
    """Command lines of the answer in the order they appear: [(tool, words, flags)]. `tools`: names a spec uses.
    Lines are the lines of the text (code blocks included) and inline code; `\\` continuations are joined; pipes
    and `&&` split commands; an environment prefix is skipped. words = the non-flag tokens before the first flag;
    flags = {flag: [values]} for `--flag`, `--flag=v`, `--flag v` and short flags."""
    text = re.sub(r"\\[ \t]*\n[ \t]*", " ", text or "")

    def parse(c):
        found = []
        c = c.strip().lstrip("$ ").strip()
        for part in sc.SPLIT_RE.split(c):
            toks = sc.tokenize(part.strip())
            while toks and re.match(r"^[A-Z_][A-Z0-9_]*=", toks[0]):
                toks = toks[1:]
            if toks and _tool_name(toks[0]) in tools:
                found.append((_tool_name(toks[0]), _words(toks), _flags(toks)))
        return found

    out = []
    for line in text.split("\n"):
        ents = parse(line)
        if not ents:  # prose around inline code
            for seg in re.findall(r"`([^`\n]+)`", line):
                ents += parse(seg)
        out += ents
    return out


def _tool_name(tok):
    base = tok.rsplit("/", 1)[-1]
    return sc.kafka_tool_name(tok) if sc.KTOOL_RE.match(tok) else base


def _words(toks):
    w = []
    for t in toks[1:]:
        if t.startswith("-"):
            break
        w.append(t)
    return w


def _flags(toks):
    have = {}
    for f, v in sc.kafka_flags(toks):
        have.setdefault(f, []).append(v)
    for t in toks[1:]:
        if re.match(r"^-[A-Za-z]", t) and not t.startswith("--"):
            have.setdefault(t.split("=")[0], []).append(t.split("=", 1)[1] if "=" in t else None)
    return have


def _step_matches(step, cmd):
    tool, words, flags = cmd
    if _tool_name(step["tool"]) != tool:
        return False
    if step.get("words") and words[:len(step["words"])] != step["words"]:
        return False
    if any(f not in flags for f in step.get("flags_all", [])):
        return False
    for f, v in (step.get("values") or {}).items():
        if not any(x is not None and sc.norm_val(x) == v for x in flags.get(f, [])):
            return False
    if step.get("flags_any") and not any(f in flags for f in step["flags_any"]):
        return False
    return not any(f in flags for f in step.get("flags_forbidden", []))


def check_cmd_order(chk, text, cats):
    """chk["steps"]: [{role: safe|mutate|verify, tool, words?, flags_all?, flags_any?, flags_forbidden?, values?}].
    Passes when a mutating line is present, a safe line comes before the first mutating line, and, when a verify
    step is defined and any verify line occurs, one comes after the first mutating line (chk["require_verify"]
    makes that line mandatory). A line that matches a mutate step counts as mutating, whatever else it matches."""
    steps = chk["steps"]
    cmds = ordered_commands(text, {_tool_name(s["tool"]) for s in steps})
    role_steps = {r: [s for s in steps if s["role"] == r] for r in ("safe", "mutate", "verify")}
    roles = []
    for c in cmds:
        for r in ("mutate", "safe", "verify"):
            if any(_step_matches(s, c) for s in role_steps[r]):
                roles.append(r)
                break
        else:
            roles.append(None)
    if "mutate" not in roles:
        return False, "no mutating line (roles in order: %s)" % [r for r in roles if r]
    first_mut = roles.index("mutate")
    if "safe" not in roles[:first_mut]:
        return False, "no safe line before the first mutating line (roles in order: %s)" % [r for r in roles if r]
    if role_steps["verify"]:
        verifies = [i for i, r in enumerate(roles) if r == "verify"]
        if chk.get("require_verify") and not any(i > first_mut for i in verifies):
            return False, "no verify line after the mutating line"
        if verifies and not any(i > first_mut for i in verifies):
            return False, "the verify line comes before the mutating line"
    return True, "order is %s" % [r for r in roles if r]


# =============================================================== number_within
SIZE_UNITS = {"b": 1, "byte": 1, "bytes": 1,
              "kib": 2 ** 10, "mib": 2 ** 20, "gib": 2 ** 30, "tib": 2 ** 40,
              "ki": 2 ** 10, "mi": 2 ** 20, "gi": 2 ** 30, "ti": 2 ** 40,   # Kubernetes quantity suffixes
              "kb": 10 ** 3, "mb": 10 ** 6, "gb": 10 ** 9, "tb": 10 ** 12}
TIME_UNITS = {"ms": Fraction(1, 1000), "msec": Fraction(1, 1000), "millisecond": Fraction(1, 1000),
              "milliseconds": Fraction(1, 1000), "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
              "min": 60, "mins": 60, "minute": 60, "minutes": 60, "h": 3600, "hr": 3600, "hrs": 3600,
              "hour": 3600, "hours": 3600, "d": 86400, "day": 86400, "days": 86400}


def parse_unit(u):
    """'GiB', 'ms', 'MiB/s' -> (dimension, factor to the base unit) or None. Units are case-insensitive, KB (1000)
    and KiB (1024) stay distinct."""
    u = re.sub(r"\s+", "", (u or "").lower().replace("per", "/"))
    if "/" in u:
        a, _, b = u.partition("/")
        pa, pb = parse_unit(a), parse_unit(b) if b else None
        if pa and pb and pa[0] == "size" and pb[0] == "time":
            return "size/time", Fraction(pa[1]) / Fraction(pb[1])
        return None
    if u in SIZE_UNITS:
        return "size", Fraction(SIZE_UNITS[u])
    if u in TIME_UNITS:
        return "time", Fraction(TIME_UNITS[u])
    return None


NUM_RE = re.compile(r"^([-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*([A-Za-z/ ]*?)\s*$")


def parse_quantity(v):
    """'12.5 GiB' -> (Fraction, unit string) or None. Parenthesised glosses, `~`, `≈`, bold and code marks are dropped."""
    v = re.sub(r"\([^)]*\)", "", v or "")
    v = re.sub(r"[`*_~≈]|^\s*(?:about|approximately|around|roughly)\s+", "", v, flags=re.I).strip().rstrip(".")
    m = NUM_RE.match(v)
    if not m:
        return None
    return Fraction(m.group(1).replace(",", "")), m.group(2).strip()


def check_number_within(chk, text, cats):
    """chk: {value, unit, tolerance (relative, default 0.01), abs_tolerance (in `unit`, optional)}. The ANSWER line
    holds one number with an optional unit; no unit = the asked unit; a unit of another dimension fails."""
    v = sc.parse_answer_line(text)
    if v is None:
        return False, "no ANSWER: line"
    q = parse_quantity(v)
    if q is None:
        return False, "ANSWER %r is not a number with an optional unit" % v
    num, unit = q
    want_u = parse_unit(chk["unit"])
    got_u = parse_unit(unit) if unit else want_u
    if want_u is None:
        raise ValueError("unknown unit %r in the check" % chk["unit"])
    if got_u is None:
        return False, "unknown unit %r" % unit
    if got_u[0] != want_u[0]:
        return False, "unit %r is a %s, the question asks for %s" % (unit, got_u[0], want_u[0])
    got, want = num * got_u[1], Fraction(chk["value"]).limit_denominator(10 ** 9) * want_u[1]
    tol = max(abs(want) * Fraction(str(chk.get("tolerance", 0.01))),
              Fraction(str(chk.get("abs_tolerance", 0))) * want_u[1])
    ok = abs(got - want) <= tol
    return ok, "answer %s %s = %.6g %s; wanted %s %s within %s" % (
        v, "" if unit else "(asked unit)", float(got / want_u[1]), chk["unit"], chk["value"], chk["unit"],
        "%.3g%%" % (100 * chk.get("tolerance", 0.01)))


# =============================================================== declines, no_invented_value
VERSION_OR_NUM_RE = re.compile(r"(?<![\w.-])\d[\d,]*(?:\.\d+)*(?![\w-])")


def check_declines(chk, text, cats):
    declined, gap = sc.detect_decline(text)
    return declined, ("declines: %r" % (gap[:100] or sc.parse_answer_line(text))) if declined else "the answer does not decline"


def check_no_invented_value(chk, text, cats):
    """The answer gives no value. It fails on: an ANSWER line or code block that carries a payload
    (has_payload), a concrete number (any number or version in the text other than those in chk["allowed_numbers"],
    which the generator fills with the numbers the question itself contains), or an identifier the catalogues
    call unknown (other than chk["allowed_identifiers"], the question's own names)."""
    bad = []
    if sc.has_payload(text):
        bad.append("an answer value or code block")
    allowed = {str(x) for x in chk.get("allowed_numbers", [])}
    nums = [n for n in VERSION_OR_NUM_RE.findall(text or "") if n.strip(",") not in allowed]
    if nums:
        bad.append("number(s) %s" % sorted(set(nums))[:5])
    allowed_ids = set(chk.get("allowed_identifiers", []))
    unk = [r["token"] for r in sc.classify_identifiers(text, cats)["unknown"] if r["token"] not in allowed_ids]
    if unk:
        bad.append("unknown identifier(s) %s" % sorted(set(unk))[:5])
    return not bad, ("invents: " + "; ".join(bad)) if bad else "no value, number or unknown identifier"


# =============================================================== forbidden_keys
def _config_maps(doc, path):
    """The mappings at a config_path of the table; `[]` after a segment walks every element of that list
    (`spec.mirrors[].source.config`)."""
    cur = [doc]
    for seg in path.split("."):
        nxt = []
        for c in cur:
            if seg.endswith("[]"):
                v = c.get(seg[:-2]) if isinstance(c, dict) else None
                nxt += [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []
            elif isinstance(c, dict) and seg in c:
                nxt.append(c[seg])
        cur = nxt
    return [c for c in cur if isinstance(c, dict)]


def check_forbidden_keys(chk, text, cats):
    """A manifest of a kind in the table must not set a forbidden key under any of that kind's config maps
    (the primary one and the `<Kind>:<config_path>` ones). The table is read only by
    kafka_sre_rag.catalogue_a3.forbidden (exact keys, prefixes, exceptions, exception prefixes ending in `.`,
    exception globs); chk["catalogues_a3"] names another table directory (tests), chk["kinds"] limits the kinds.
    A manifest of no covered kind, or without any of its config maps, fails: the check is attached only to items
    that carry a config map."""
    from kafka_sre_rag import catalogue_a3
    dirpath = chk.get("catalogues_a3")
    res = catalogue_a3.resources(dirpath)
    kinds = set(chk.get("kinds") or [k.split(":")[0] for k in res])
    seen, bad = 0, []
    for d in sc.answer_yaml_docs(text):
        kind = d.get("kind")
        if kind not in kinds:
            continue
        for name, entry in res.items():
            if name != kind and not name.startswith(kind + ":"):
                continue
            for cfg in _config_maps(d, entry["config_path"]):
                seen += 1
                for key, _ in _flatten(cfg):
                    hit = catalogue_a3.forbidden(kind, key, entry["config_path"], dirpath)
                    if hit:
                        bad.append("%s %s: %s is forbidden (%s %s, %s:%s)" % (
                            kind, entry["config_path"], key, hit["rule"], hit["match"], hit["file"], hit["line"]))
    if not seen:
        return False, "no manifest with a config map of a covered kind"
    return not bad, "; ".join(bad[:4]) if bad else "no forbidden key under the config maps"


CHECKS_A3 = {
    "semantic_rule": check_semantic_rule,
    "cmd_order": check_cmd_order,
    "number_within": check_number_within,
    "declines": check_declines,
    "no_invented_value": check_no_invented_value,
    "forbidden_keys": check_forbidden_keys,
}
