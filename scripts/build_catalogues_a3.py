#!/usr/bin/env python3
"""Build the a3 catalogues (WP1c T1a) from the pinned sources in `raw/`.

Output: work/m4/catalogues_a3/ (git-ignored), separate from work/m4/catalogues/ (never touched):

  strimzi_forbidden.json   config keys and prefixes Strimzi manages and a user cannot set, per kind,
                           parsed from the pinned text ("cannot be set" sentences of appendix_crds.adoc,
                           the broker list from the KafkaClusterSpec API module)
  semantic_rules.json      the source sentences the valid-but-wrong mechanisms V1..V8 rest on, and the
                           Kubernetes memory-quantity suffix rule; each with file and line
  MANIFEST.json            sha256 of each file and the combined hash

Every entry cites file and line, and the build re-reads that line and fails (exit 2) if the text does
not contain the entry. Nothing is written from memory: a statement whose sentence is not in the pinned
sources is marked "anchored": false with the reason. Reads only files on disk; no network, no git.
Deterministic: two runs give byte-identical files.

Usage: .venv/bin/python scripts/build_catalogues_a3.py [--out DIR]
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

VERSION = "build_catalogues_a3/1"
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "work" / "m4" / "catalogues_a3"
FILES = ("strimzi_forbidden", "semantic_rules")      # order of the combined hash
MANIFEST_NAME = "MANIFEST.json"

DOC = "raw/strimzi/documentation"
APPENDIX = DOC + "/modules/appendix_crds.adoc"
CLUSTER_SPEC = DOC + "/api/io.strimzi.api.kafka.model.kafka.KafkaClusterSpec.adoc"
MANAGED = DOC + "/modules/managing/con-managed-broker-config-properties.adoc"
SSL_DOC = DOC + "/modules/con-common-configuration-properties.adoc"
K8S = "raw/kubernetes/content/en/docs"
KCFG = "raw/apache-kafka-config/content/en/43/generated"


class BuildError(Exception):
    pass


_cache = {}


def lines_of(rel):
    if rel not in _cache:
        p = ROOT / rel
        if not p.is_file():
            raise BuildError("source file is absent: " + rel)
        _cache[rel] = p.read_text(encoding="utf-8").split("\n")
    return _cache[rel]


def line_text(rel, n):
    ls = lines_of(rel)
    if not 1 <= n <= len(ls):
        raise BuildError("%s has no line %d" % (rel, n))
    return ls[n - 1]


def find_line(rel, quote):
    """The single line number whose text contains `quote`; BuildError when none or several."""
    hits = [i + 1 for i, t in enumerate(lines_of(rel)) if quote in t]
    if len(hits) != 1:
        raise BuildError("%s: quote found on %d lines %s: %r" % (rel, len(hits), hits[:6], quote))
    return hits[0]


# ------------------------------------------------------------------ forbidden-keys table
# type heading of appendix_crds.adoc -> (resource key, config path). The first path of a kind is the
# primary one (the key "<Kind>"); further paths of the same kind are keyed "<Kind>:<config_path>".
TYPE_MAP = {
    "KafkaClusterSpec": [("Kafka", "spec.kafka.config")],
    "CruiseControlSpec": [("CruiseControl", "spec.cruiseControl.config")],
    "KafkaConnectSpec": [("KafkaConnect", "spec.config")],
    "KafkaConnectorSpec": [("KafkaConnector", "spec.config")],
    "KafkaMirrorMaker2TargetClusterSpec": [("KafkaMirrorMaker2", "spec.target.config")],
    "KafkaMirrorMaker2ClusterSpec": [("KafkaMirrorMaker2", "spec.mirrors[].source.config")],
    "KafkaMirrorMaker2ConnectorSpec": [("KafkaMirrorMaker2", "spec.mirrors[].sourceConnector.config"),
                                       ("KafkaMirrorMaker2", "spec.mirrors[].checkpointConnector.config")],
    "KafkaBridgeSpec": [("KafkaBridge", "spec.config")],
    "KafkaBridgeConsumerSpec": [("KafkaBridge", "spec.consumer.config")],
    "KafkaBridgeProducerSpec": [("KafkaBridge", "spec.producer.config")],
    "KafkaBridgeHttpTls": [("KafkaBridge", "spec.http.tls.config")],
}
HEADING = re.compile(r"^(?:==? `(\w+)` schema (?:properties|reference))$")
TOKEN = re.compile(r"^[A-Za-z0-9._-]+$")


def split_list(s):
    toks = [t.strip() for t in s.split(",")]
    for t in toks:
        if not TOKEN.match(t):
            raise BuildError("unparsable token %r in %r" % (t, s))
    return toks


def parse_sentences(text):
    """(forbidden_prefixes, forbidden_keys, exceptions) of one table cell of appendix_crds.adoc."""
    prefixes, keys, exceptions = [], [], []
    for m in re.finditer(r"(prefixes|properties|options) cannot be set: ", text):
        rest = text[m.end():]
        cut = re.search(r" \(with the exception of: |\s+The following|$", rest)
        seg = rest[:cut.start()]
        if m.group(1) == "prefixes":
            prefixes += split_list(seg)
        else:
            keys += split_list(seg.rstrip(".") if seg.endswith(".") else seg)
    for m in re.finditer(r"\(with the exception of: ([^)]*)\)", text):
        exceptions += split_list(m.group(1))
    return prefixes, keys, exceptions


def bullets_after(rel, start_line, stop_blank_after_bullet=True):
    """(line number, text between backticks, whole line) of the bullet lines that follow `start_line`."""
    out, seen = [], False
    ls = lines_of(rel)
    for i in range(start_line, len(ls)):
        t = ls[i]
        if t.startswith("*"):
            seen = True
            m = re.match(r"^(\*+) (.*)$", t)
            q = re.search(r"`([^`]+)`", m.group(2))
            out.append((i + 1, len(m.group(1)), q.group(1) if q else None, t))
        elif seen and t.strip() == "":
            break
    return out


def new_entry(kind, path):
    return {"config_path": path, "forbidden_prefixes": [], "forbidden_keys": [], "exceptions": [],
            "exception_globs": [], "source": "", "anchors": []}


def add_anchor(e, role, rel, n, items):
    text = line_text(rel, n)
    for it in items:
        if it not in text:
            raise BuildError("%s:%d does not contain %r" % (rel, n, it))
    e["anchors"].append({"role": role, "file": rel, "line": n, "items": list(items)})


def build_forbidden():
    res = {}
    entries = {}

    def entry(kind, path):
        if (kind, path) in entries:
            return entries[(kind, path)]
        primary = not any(k == kind for k, _ in entries)
        e = new_entry(kind, path)
        entries[(kind, path)] = e
        res[kind if primary else "%s:%s" % (kind, path)] = e
        return e

    # 1. every "cannot be set" cell of the appendix, mapped by the type heading above it
    ls = lines_of(APPENDIX)
    heading = None
    seen_types = set()
    for i, t in enumerate(ls):
        m = HEADING.match(t)
        if m:
            heading = m.group(1)
        if t.startswith("|") and "cannot be set" in t:
            if heading not in TYPE_MAP:
                raise BuildError("%s:%d: 'cannot be set' under unmapped type %r" % (APPENDIX, i + 1, heading))
            n = i + 1
            pre, keys, exc = parse_sentences(t)
            seen_types.add(heading)
            for kind, path in TYPE_MAP[heading]:
                e = entry(kind, path)
                for role, vals in (("forbidden_prefixes", pre), ("forbidden_keys", keys), ("exceptions", exc)):
                    if vals:
                        add_anchor(e, role, APPENDIX, n, vals)
                        e[role] += [v for v in vals if v not in e[role]]
                e["source"] += ("," if e["source"] else APPENDIX + ":") + str(n)
    for need in TYPE_MAP:
        if need != "KafkaClusterSpec" and need not in seen_types:
            raise BuildError("no 'cannot be set' sentence found for type " + need)

    # 2. the broker: the appendix line only points to other documentation (line of the pointer is kept)
    ptr = find_line(APPENDIX, "Kafka broker config properties with certain prefixes cannot be set unless")
    kafka = entry("Kafka", "spec.kafka.config")
    start = find_line(CLUSTER_SPEC, "Properties with the following prefixes cannot be set:")
    for n, depth, item, _ in bullets_after(CLUSTER_SPEC, start):
        add_anchor(kafka, "forbidden_prefixes", CLUSTER_SPEC, n, [item])
        kafka["forbidden_prefixes"].append(item)
    managed = find_line(MANAGED, "you cannot configure the following options through the `config` property:")
    for n, depth, item, _ in bullets_after(MANAGED, managed):
        add_anchor(kafka, "forbidden_keys", MANAGED, n, [item])
        kafka["forbidden_keys"].append(item)
    exc_start = find_line(CLUSTER_SPEC, "including the following exceptions to the options configured by Strimzi:")
    group = None
    for n, depth, item, text in bullets_after(CLUSTER_SPEC, exc_start):
        if depth == 1:
            group = "listener" if "Listener properties" in text else "other"
            continue                       # a heading of a nested list (or the ssl bullet, handled below)
        if group == "listener":
            kafka["exception_globs"].append("listener.name.*." + item)
            add_anchor(kafka, "exception_globs", CLUSTER_SPEC, n, [item])
        else:
            kafka["exceptions"].append(item)
            add_anchor(kafka, "exceptions", CLUSTER_SPEC, n, [item])
    # the ssl exception ("Any ssl configuration for supported TLS versions and cipher suites"), keys from the
    # section it links to (con-common-configuration-ssl-reference); ssl.endpoint.identification.algorithm is
    # there said to be configurable only in components based on Kafka clients, so it is not a broker exception
    for key, quote in (("ssl.enabled.protocols", "The `ssl.enabled.protocols` property specifies"),
                       ("ssl.protocol", "The `ssl.protocol` property sets"),
                       ("ssl.cipher.suites", "  ssl.cipher.suites: TLS_AES_256_GCM_SHA384")):
        add_anchor(kafka, "exceptions", SSL_DOC, find_line(SSL_DOC, quote), [key])
        kafka["exceptions"].append(key)
    kafka["source"] = "%s:%d" % (CLUSTER_SPEC, start)
    kafka["pointer"] = "%s:%d" % (APPENDIX, ptr)

    for e in res.values():
        for k in ("forbidden_prefixes", "forbidden_keys", "exceptions", "exception_globs"):
            seen = []
            for v in e[k]:
                if v not in seen:
                    seen.append(v)
            e[k] = seen
        e["exception_note"] = ("an exception equal to a key allows exactly that key; one ending in '.' allows "
                               "every key with that prefix; exception_globs use '*' for one listener name")
    return {"schema": 1, "builder": VERSION, "resources": res}


# ------------------------------------------------------------------ semantic rules
def anchor(rel, quote, _unused=None):
    """File, the line found by searching for the quote (never taken from a plan), and the quote."""
    return {"file": rel, "line": find_line(rel, quote), "quote": quote}


def build_semantic():
    NP = K8S + "/concepts/services-networking/network-policies.md"
    PDB = K8S + "/tasks/run-application/configure-pdb.md"
    MEM = K8S + "/concepts/configuration/manage-resources-containers.md"
    RESTART = DOC + "/modules/configuring/proc-manual-restart-connector.adoc"
    REST = "raw/apache-kafka/docs/kafka-connect/user-guide.md"
    KTOP = KCFG + "/topic_config.html"
    KBRK = KCFG + "/kafka_config.html"
    KCON = KCFG + "/consumer_config.html"

    m = {}
    m["V1"] = {
        "title": "NetworkPolicy from: podSelector only selects pods of the policy's own namespace",
        "oracle": "np_allows(policy, source_ns, labels)",
        "anchored": True,
        "anchors": [
            anchor(NP, "**podSelector**: This selects particular Pods in the same namespace as the NetworkPolicy which", 147),
            anchor(NP, "**namespaceSelector**: This selects particular namespaces for which all Pods should be allowed as", 150),
            anchor(NP, "**namespaceSelector** *and* **podSelector**: A single `to`/`from` entry that specifies both", 153),
            anchor(NP, "`role=client` in namespaces with the label `user=alice`. But the following policy is different:", 169),
            anchor(NP, "Namespace with the label `role=client`, *or* from any Pod in any namespace with the label", 183),
            anchor(NP, "which the policy applies. The example policy selects pods with the label \"role=db\". An empty", 105),
        ],
    }
    m["V2"] = {
        "title": "strimzi.io/restart=\"true\" restarts the connector instance only, not its tasks",
        "oracle": "restart annotation value parser (true, includeTasks, onlyFailed, combinations)",
        "anchored": True,
        "anchors": [
            anchor(RESTART, "The `restart` annotation is set to `true`.", 33),
            anchor(RESTART, "You can refine the restart behavior using the `includeTasks` and `onlyFailed` parameters. Both default to `false`.", 35),
            anchor(RESTART, "* `includeTasks` restarts both the connector instance and its task instances.", 38),
            anchor(RESTART, "* `onlyFailed` restarts only instances with a `FAILED` status when set to `true`.", 39),
            anchor(RESTART, "strimzi.io/restart=\"includeTasks,onlyFailed\"", 46),
            anchor(REST, "the \"includeTasks\" parameter specifies whether to restart the connector instance and task instances", 308),
        ],
    }
    m["V3"] = {
        "title": "a setting equal to its default is set as if it changed behaviour",
        "oracle": "catalogue default equals the value set (kafka_config 'Default' row of the generated reference)",
        "anchored": True,
        "note": "generic mechanism: the Default row of every key in the generated *_config.html; the anchors "
                "are the example min.insync.replicas, topic scope and broker scope",
        "anchors": [
            anchor(KTOP, "<tr><th>Server Default Property:</th><td>min.insync.replicas</td></tr>"),
            anchor(KTOP, "<h4><a id=\"min.insync.replicas\"></a><a id=\"topicconfigs_min.insync.replicas\""),
            anchor(KBRK, "<h4><a id=\"min.insync.replicas\"></a><a id=\"brokerconfigs_min.insync.replicas\""),
        ],
        "default_rows": {"topic_config.html:min.insync.replicas": "<tr><th>Default:</th><td>1</td></tr>"},
    }
    # the 'Default' rows follow the heading of the key; record their lines
    for fname, key_id in ((KTOP, "topicconfigs_min.insync.replicas"), (KBRK, "brokerconfigs_min.insync.replicas")):
        h = find_line(fname, 'id="%s"' % key_id)
        for k in range(h, h + 12):
            if line_text(fname, k).startswith("<tr><th>Default:</th><td>1</td></tr>"):
                m["V3"]["anchors"].append({"file": fname, "line": k, "quote": "<tr><th>Default:</th><td>1</td></tr>"})
                break
        else:
            raise BuildError("no Default row after " + key_id)
    del m["V3"]["default_rows"]
    m["V4"] = {
        "title": "'#' comment after a value in a .properties file becomes part of the value",
        "oracle": "java.util.Properties parser semantics",
        "anchored": False,
        "reason": "no sentence about comments or trailing '#' in a .properties value exists in the pinned "
                  "documentation (raw/apache-kafka, raw/apache-kafka-config, raw/strimzi/documentation: grepped); "
                  "the rule lives in the JDK Properties.load javadoc and Kafka's Utils.loadProps, neither of which "
                  "is a pinned source (sparse clone is D1, pending)",
        "anchors": [],
    }
    m["V5"] = {
        "title": "heartbeat.interval.ms must be lower than session.timeout.ms and typically no higher than 1/3 of it",
        "oracle": "numeric rule on the two values (classic group protocol)",
        "anchored": True,
        "anchors": [
            anchor(KCON, "the value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher than 1/3 of that value", 64),
        ],
    }
    m["V6"] = {
        "title": "min.insync.replicas not below replicas: acks=all writes fail once a replica is lost",
        "oracle": "numeric rule on the KafkaTopic manifest",
        "anchored": True,
        "scope": "limited to the documented mechanism (acks=all fails when the ISR has fewer than "
                 "min.insync.replicas members) plus one arithmetic step: min.insync.replicas equal to replicas "
                 "fails after one broker is lost. The 'replicas above the broker count' branch is not part of "
                 "the rule: no pinned sentence supports it",
        "anchors": [
            anchor(KBRK, "If <code>acks=all</code> and the current ISR set contains fewer than <code>min.insync.replicas</code> members, then the producer will raise an exception"),
            anchor(KBRK, "A typical scenario would be to create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2"),
            anchor(KTOP, "If <code>acks=all</code> and the current ISR set contains fewer than <code>min.insync.replicas</code> members, then the producer will raise an exception"),
        ],
    }
    m["V7"] = {
        "title": "PodDisruptionBudget with maxUnavailable 0 or minAvailable 100% allows no voluntary eviction",
        "oracle": "pdb_allows_eviction(replicas, spec)",
        "anchored": True,
        "anchors": [
            anchor(PDB, "If you set `maxUnavailable` to 0% or 0, or you set `minAvailable` to 100% or the number of replicas,"),
            anchor(PDB, "you are requiring zero voluntary evictions. When you set zero voluntary evictions for a workload"),
            anchor(PDB, "object such as ReplicaSet, then you cannot successfully drain a Node running one of those Pods."),
            anchor(PDB, "You can specify only one of `maxUnavailable` and `minAvailable` in a single `PodDisruptionBudget`."),
        ],
    }
    m["V8"] = {
        "title": "a key real for the broker but not for the topic scope inside a KafkaTopic config map",
        "oracle": "membership of the key in the topic scope of kafka_config (topic_config.html)",
        "anchored": False,
        "reason": "no pinned sentence says the Topic Operator or the broker accepts a broker-scope key in "
                  "KafkaTopic spec.config; Kafka's own topic-config name validation may reject it, which would "
                  "make the item 'rejected', not 'valid but wrong'",
        "note": "generators build no items on an unanchored rule. T1b may re-anchor it from the Kafka source in "
                "the sparse clone (topic config name validation); the item kind would then change",
        "anchors": [],
        "context_anchors": [
            anchor(APPENDIX, "|The topic configuration."),
            anchor(KTOP, "<h4><a id=\"min.insync.replicas\"></a><a id=\"topicconfigs_min.insync.replicas\""),
        ],
    }
    quantity = {
        "title": "Kubernetes memory quantity suffixes (computed-sizing template, plan 5.4)",
        "anchored": True,
        "anchors": [
            anchor(MEM, "a plain integer or as a fixed-point number using one of these", 181),
            anchor(MEM, "E, P, T, G, M, k. You can also use the power-of-two equivalents: Ei, Pi, Ti, Gi,", 183),
            anchor(MEM, "Mi, Ki. The Kubernetes API also allows m as a suffix (for millibytes: 1/1000 of a byte),", 184),
            anchor(MEM, "128974848, 129e6, 129M,  128974848000m, 123Mi", 189),
            anchor(MEM, "Pay attention to the case of the suffixes. \"M\" means megabytes, while \"m\" means millibytes.", 191),
        ],
        "suffixes_in_text": ["E", "P", "T", "G", "M", "k", "Ei", "Pi", "Ti", "Gi", "Mi", "Ki", "m"],
        "multipliers": {
            "derived": True,
            "note": "the text names the suffixes and says the examples are 'roughly the same value'; the "
                    "multipliers are the SI (k=10^3 .. E=10^18) and power-of-two (Ki=2^10 .. Ei=2^60) readings, "
                    "checked at build time against the example line: 123Mi = 128974848 bytes and 129M = 129e6",
            "decimal": {"k": 10 ** 3, "M": 10 ** 6, "G": 10 ** 9, "T": 10 ** 12, "P": 10 ** 15, "E": 10 ** 18},
            "binary": {"Ki": 2 ** 10, "Mi": 2 ** 20, "Gi": 2 ** 30, "Ti": 2 ** 40, "Pi": 2 ** 50, "Ei": 2 ** 60},
            "milli": {"m": "1/1000 of a byte; never meant for memory sizes (line of the 'M' versus 'm' warning)"},
        },
    }
    if 123 * 2 ** 20 != 128974848 or 129 * 10 ** 6 != 129000000:
        raise BuildError("quantity multipliers do not reproduce the example line")
    return {"schema": 1, "builder": VERSION, "mechanisms": m, "memory_quantity": quantity}


# ------------------------------------------------------------------ verify every cited line
def verify_semantic(sem):
    def check(a):
        if a["quote"] not in line_text(a["file"], a["line"]):
            raise BuildError("%s:%d does not contain %r" % (a["file"], a["line"], a["quote"]))
    for mech in sem["mechanisms"].values():
        for a in mech["anchors"]:
            check(a)
    for mech in sem["mechanisms"].values():
        for a in mech.get("context_anchors", []):
            check(a)
    for a in sem["memory_quantity"]["anchors"]:
        check(a)


def dumps(obj):
    return json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


def manifest_for(directory):
    d = Path(directory)
    files = {}
    for n in FILES:
        f = d / (n + ".json")
        if f.is_file():
            files[n + ".json"] = hashlib.sha256(f.read_bytes()).hexdigest()
    whole = hashlib.sha256("".join("%s %s\n" % kv for kv in files.items()).encode()).hexdigest()
    return {"manifest": "catalogues_a3/1", "files": files, "catalogue_hash": whole}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    a = ap.parse_args(argv)
    out = Path(a.out)
    try:
        forb = build_forbidden()
        sem = build_semantic()
        verify_semantic(sem)
    except BuildError as e:
        print("build_catalogues_a3: FAILED: %s" % e, file=sys.stderr)
        return 2
    out.mkdir(parents=True, exist_ok=True)
    (out / "strimzi_forbidden.json").write_text(dumps(forb), encoding="utf-8")
    (out / "semantic_rules.json").write_text(dumps(sem), encoding="utf-8")
    man = manifest_for(out)
    (out / MANIFEST_NAME).write_text(dumps(man), encoding="utf-8")
    for k, e in forb["resources"].items():
        print("forbidden  %-62s prefixes %2d keys %2d exceptions %2d globs %d"
              % (k + " " + e["config_path"], len(e["forbidden_prefixes"]), len(e["forbidden_keys"]),
                 len(e["exceptions"]), len(e["exception_globs"])))
    for k, v in sem["mechanisms"].items():
        print("semantic   %s anchored=%s anchors=%d" % (k, v["anchored"], len(v["anchors"])))
    print("catalogue_hash", man["catalogue_hash"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
