#!/usr/bin/env python3
"""Build the a3 catalogues (WP1c T1a) from the pinned sources in `raw/`.

Output: work/m4/catalogues_a3/ (git-ignored), separate from work/m4/catalogues/ (never touched):

  strimzi_forbidden.json   config keys and prefixes Strimzi manages and a user cannot set, per kind,
                           parsed from the pinned text ("cannot be set" sentences of appendix_crds.adoc,
                           the broker list from the KafkaClusterSpec API module)
  semantic_rules.json      the source sentences the valid-but-wrong mechanisms V1..V8 rest on, and the
                           Kubernetes memory-quantity suffix rule; each with file and line
  java_api.json            (T1b) public classes, methods with parameter types and public constants of the
                           pinned kafka-clients and kafka-streams jars, read with javap; the client config
                           names (ConsumerConfig.*_CONFIG ...) with their string values; skipped, with a
                           message, when the jars or the sparse clone are not fetched (scripts/fetch_maven_jars.py)
  python_symbols.json      (T2) the public names of the pinned confluent-kafka wheel (modules, classes, their
                           methods with parameter names) and the librdkafka configuration property names its
                           compiled table accepts; read by importing the wheel in a THROWAWAY venv outside the
                           repository (env CK_PYTHON or --ck-python, the venv's python); skipped, with a message,
                           when the wheel or that python is absent
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
import os
import re
import subprocess
import sys
import zipfile
from pathlib import Path

VERSION = "build_catalogues_a3/3"
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "work" / "m4" / "catalogues_a3"
FILES = ("strimzi_forbidden", "semantic_rules", "java_api", "python_symbols")      # order of the combined hash
MANIFEST_NAME = "MANIFEST.json"

DOC = "raw/strimzi/documentation"
APPENDIX = DOC + "/modules/appendix_crds.adoc"
CLUSTER_SPEC = DOC + "/api/io.strimzi.api.kafka.model.kafka.KafkaClusterSpec.adoc"
MANAGED = DOC + "/modules/managing/con-managed-broker-config-properties.adoc"
SSL_DOC = DOC + "/modules/con-common-configuration-properties.adoc"
K8S = "raw/kubernetes/content/en/docs"
KCFG = "raw/apache-kafka-config/content/en/43/generated"
KAFKA_SRC = "raw_catalogue/apache-kafka-api"


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
    if not (ROOT / KAFKA_SRC / "LICENSE").is_file():
        raise BuildError("the sparse clone %s is absent: run scripts/fetch_maven_jars.py --clone" % KAFKA_SRC)
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
                  "documentation (raw/apache-kafka, raw/apache-kafka-config, raw/strimzi/documentation: grepped). "
                  "Utils.loadProps (T1b, sparse clone) only calls java.util.Properties.load on the file; the "
                  "comment rule is in the JDK, not in a pinned source, so the Kafka source does not settle it",
        "anchors": [],
        "context_anchors": [
            {"file": KAFKA_SRC + "/clients/src/main/java/org/apache/kafka/common/utils/Utils.java", "line": None,
             "quote": "props.load(propStream);"},
        ],
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
        "anchored": True,
        "valid_but_wrong": False,
        "outcome": "rejected",
        "item_kind": "rejected, not valid but wrong",
        "reason": "the Kafka source (T1b, sparse clone at the pinned commit) rejects an unknown topic config name "
                  "on topic create and on alter: ControllerConfigurationValidator.validate (TOPIC) calls "
                  "LogConfig.validate, which calls LogConfig.validateNames, which throws InvalidConfigurationException "
                  "'Unknown topic config name' for any name not in the LogConfig definition (the topic scope); "
                  "ConfigurationControlManager turns it into an INVALID_CONFIG error, and create reaches it through "
                  "incrementalAlterConfig. A broker-only key in KafkaTopic spec.config is therefore refused by the "
                  "broker, not accepted: the mechanism is NOT valid-but-wrong, generators must not use it in that "
                  "kind. It can serve a 'rejected' item. The documentation says only 'The topic configuration.' "
                  "(context anchor)",
        "anchors": [
            {"file": KAFKA_SRC + "/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java",
             "line": None, "quote": 'throw new InvalidConfigurationException("Unknown topic config name: " + name);'},
            {"file": KAFKA_SRC + "/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java",
             "line": None, "quote": "public static void validateNames(Map<String, String> props) {"},
            {"file": KAFKA_SRC + "/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java",
             "line": None, "quote": "        validateNames(props);"},
            {"file": KAFKA_SRC + "/core/src/main/scala/kafka/server/ControllerConfigurationValidator.scala",
             "line": None, "quote": "LogConfig.validate(oldConfigs, filteredConfigs, kafkaConfig.extractLogConfigMap,"},
            {"file": KAFKA_SRC + "/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java",
             "line": None, "quote": "validator.validate(configResource, allConfigs, existingConfigsMap);"},
            {"file": KAFKA_SRC + "/metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java",
             "line": None, "quote": "return new ApiError(INVALID_CONFIG, e.getMessage());"},
            {"file": KAFKA_SRC + "/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java",
             "line": None, "quote": "configurationControl.incrementalAlterConfig(configResource, keyToOps, true, forwarded);"},
        ],
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


# ------------------------------------------------------------------ Java API catalogue (T1b)
MAVEN = ROOT / "raw_catalogue" / "maven"
CLONE = ROOT / KAFKA_SRC
JAR_ROOTS = {"kafka-clients-4.3.2.jar": "clients", "kafka-streams-4.3.2.jar": "streams"}
# packages (exact, or with sub-packages for the streams ones) whose public classes are catalogued;
# any package with an "internal" or "internals" segment is left out
EXACT_PKGS = {"org.apache.kafka.clients", "org.apache.kafka.clients.consumer", "org.apache.kafka.clients.producer",
              "org.apache.kafka.clients.admin", "org.apache.kafka.common", "org.apache.kafka.common.config",
              "org.apache.kafka.common.serialization", "org.apache.kafka.common.errors",
              "org.apache.kafka.common.header", "org.apache.kafka.common.acl", "org.apache.kafka.common.resource"}
TREE_PKGS = ("org.apache.kafka.streams",)
CONFIG_CLASSES = {"org.apache.kafka.clients.consumer.ConsumerConfig": "consumer",
                  "org.apache.kafka.clients.producer.ProducerConfig": "producer",
                  "org.apache.kafka.clients.admin.AdminClientConfig": "admin",
                  "org.apache.kafka.streams.StreamsConfig": "streams",
                  "org.apache.kafka.clients.CommonClientConfigs": "common"}
MODS = {"public", "protected", "private", "static", "final", "abstract", "default", "synchronized", "native",
        "strictfp", "transient", "volatile", "sealed", "non-sealed"}
KIND_RE = re.compile(r"\b(class|interface|enum|@interface)\s+([\w.$]+)")


def pkg_wanted(pkg):
    if "internal" in pkg.split(".") or "internals" in pkg.split("."):
        return False
    return pkg in EXACT_PKGS or any(pkg == t or pkg.startswith(t + ".") for t in TREE_PKGS)


def split_top(text, sep):
    """Split on `sep` outside angle brackets and parentheses."""
    out, depth, cur = [], 0, ""
    for ch in text:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == sep and depth == 0:
            if cur.strip():
                out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def split_mods(tokens):
    mods = []
    while tokens and tokens[0] in MODS:
        mods.append(tokens.pop(0))
    return mods, tokens


def literal(raw):
    raw = raw.strip()
    if raw.startswith('"') and raw.endswith('"'):
        try:
            return json.loads(raw)
        except ValueError:
            return raw[1:-1]
    return raw


def parse_javap(text):
    """{fqcn: record} for the public classes in javap -public -constants output."""
    classes, cur = {}, None
    for line in text.split("\n"):
        if line.startswith("Compiled from") or not line.strip():
            continue
        if not line.startswith(" ") and line.rstrip().endswith("{"):
            m = KIND_RE.search(line)
            head = line[:m.start()].split()
            cur = None
            if "public" in head:
                name = m.group(2)
                cur = {"kind": m.group(1), "modifiers": [h for h in head if h in MODS], "constructors": [],
                       "methods": [], "fields": [], "constants": {}}
                classes[name] = cur
            continue
        if cur is None or not line.startswith("  ") or not line.rstrip().endswith(";"):
            continue
        body = line.strip()[:-1]
        if body.startswith("static {"):
            continue
        if "(" in body and " = " not in body.split("(")[0]:
            body = re.sub(r"\s+throws\s+[\w.$, <>?]+$", "", body)
            i = body.index("(")
            params = split_top(body[i + 1:body.rindex(")")], ",")
            before = split_top(body[:i], " ")
            mods, rest = split_mods(before)
            if rest and rest[0].startswith("<"):
                rest = rest[1:]                      # method type parameters
            name = rest[-1]
            rec = {"name": name.split(".")[-1], "params": params, "modifiers": mods}
            if len(rest) == 1:
                cur["constructors"].append(rec)
            else:
                rec["returns"] = " ".join(rest[:-1])
                cur["methods"].append(rec)
        else:
            left, _, val = body.partition(" = ")
            mods, rest = split_mods(split_top(left, " "))
            if len(rest) < 2:
                continue
            f = {"name": rest[-1], "type": " ".join(rest[:-1]), "modifiers": mods}
            if val and "static" in mods and "final" in mods:
                if not f["name"].endswith("_DOC"):        # the long doc strings are not catalogued
                    cur["constants"][f["name"]] = {"type": f["type"], "value": literal(val)}
            else:
                cur["fields"].append(f)
    return classes


def jar_classes(jar):
    names = []
    with zipfile.ZipFile(jar) as z:
        for n in z.namelist():
            if not n.endswith(".class") or n.endswith("module-info.class") or n.endswith("package-info.class"):
                continue
            fq = n[:-6].replace("/", ".")
            if any(p.isdigit() for p in fq.split("$")[1:]):
                continue                                   # anonymous classes
            if pkg_wanted(fq.rsplit(".", 1)[0]):
                names.append(fq)
    return sorted(names)


def source_of(root, fqcn):
    """(relative path in the sparse clone, text lines) of the top-level class file, or (None, None)."""
    top = fqcn.split("$")[0].replace(".", "/") + ".java"
    rel = "%s/%s/src/main/java/%s" % (KAFKA_SRC, root, top)
    return (rel, lines_of(rel)) if (ROOT / rel).is_file() else (None, None)


def build_java():
    """The Java API catalogue, or None when the jars or the clone are not fetched."""
    sys.path.insert(0, str(ROOT / "scripts"))
    import fetch_maven_jars as fj
    jars = {n: MAVEN / n for n in JAR_ROOTS}
    if not all(p.is_file() for p in jars.values()) or not (CLONE / "LICENSE").is_file():
        return None
    for n, p in jars.items():
        why = fj.check(p, fj.JARS[n])
        if why:
            raise BuildError("%s: %s" % (n, why))
    cp = ":".join(str(p) for p in jars.values())
    classes, sources = {}, {}
    for n, p in jars.items():
        names = jar_classes(p)
        r = subprocess.run(["javap", "-public", "-constants", "-cp", cp] + names, capture_output=True, text=True)
        if r.returncode:
            raise BuildError("javap failed: " + r.stderr.strip()[-300:])
        for fq, rec in parse_javap(r.stdout).items():
            rec["jar"] = n
            classes[fq] = rec
    for fq in sorted(classes):
        rec = classes[fq]
        rel, ls = source_of(JAR_ROOTS[rec["jar"]], fq)
        rec["source"] = rel
        rec["line"] = None
        if ls is not None:
            simple = fq.split("$")[-1].rsplit(".", 1)[-1]
            decl = re.compile(r"^\s*(?:@\w+\s+)*(?:(?:public|protected|private|static|final|abstract|sealed)\s+)*"
                              r"(?:class|interface|enum|@interface|record)\s+%s\b" % re.escape(simple))
            hits = [i + 1 for i, t in enumerate(ls) if decl.match(t)]
            rec["line"] = hits[0] if len(hits) == 1 else None
            for cname, c in rec["constants"].items():
                ch = [i + 1 for i, t in enumerate(ls) if re.search(r"\b%s\s*=" % re.escape(cname), t)]
                c["line"] = ch[0] if len(ch) == 1 else None
        for k in ("constructors", "methods"):
            rec[k].sort(key=lambda m: (m["name"], m["params"], m.get("returns", "")))
        rec["fields"].sort(key=lambda f: f["name"])
        rec["modifiers"] = sorted(rec["modifiers"])
    cfg = {}
    for fq, scope in CONFIG_CLASSES.items():
        if fq not in classes:
            raise BuildError("config class %s is not in the jars" % fq)
        cfg[scope] = {}
        for cname, c in sorted(classes[fq]["constants"].items()):
            if cname.endswith("_CONFIG") and c["type"] == "java.lang.String":
                cfg[scope][c["value"]] = {"constant": fq + "." + cname, "source": classes[fq]["source"],
                                          "line": c.get("line")}
    return {"schema": 1, "builder": VERSION, "javap": "javap -public -constants (JDK on PATH)",
            "jars": {n: {"sha256": fj.JARS[n]["sha256"], "bytes": fj.JARS[n]["bytes"]} for n in jars},
            "clone_commit": fj.COMMIT, "classes": classes, "client_config_names": cfg}


# ------------------------------------------------------------------ Python client symbols (T2)
PYPI_DIR = ROOT / "raw_catalogue" / "pypi"
CK_WHEEL = {
    "file": "confluent_kafka-2.16.1-cp313-cp313-macosx_13_0_arm64.whl",
    "bytes": 4507494,
    "sha256": "c3bc184e79395eee26ffe73c9a041b5c68dd38ee1f90e0e7fffba89e02ab2ce0",
    "version": "2.16.1",
    "url": "https://files.pythonhosted.org/packages/96/9f/74347d6b48b36a8e610384ff660a4ddb4840cb09032d3e47d7cf89a16789/"
           "confluent_kafka-2.16.1-cp313-cp313-macosx_13_0_arm64.whl",
    "uploaded": "2026-10-28T05:36:58Z",
    "licence": "Apache-2.0 (LICENSE file of the wheel; PyPI classifier Apache Software License)",
    "why": "newest final release uploaded at least 14 days before 2026-11-19 (2.16.2 is of 2026-11-18, 4.4.0b1 is a beta)",
}
CK_SUBMODULES = ("confluent_kafka.admin", "confluent_kafka.error", "confluent_kafka.serialization",
                 "confluent_kafka.schema_registry", "confluent_kafka.avro", "confluent_kafka.cimpl")

# Run with `python -I -c` inside the throwaway venv. It imports the package (a C extension and its Python files; no
# user code), lists names, reads parameter names with inspect or from the wheel's cimpl.pyi, and tests candidate
# property names against librdkafka's own configuration parser by building a Producer from {name: ""}: the parser
# answers "No such configuration property" for a name it does not know, and anything else for a name it knows.
PY_DUMP = r"""
import ast, importlib, inspect, json, os, re, sys, types
import confluent_kafka as ck
pkgdir = os.path.dirname(ck.__file__)
subs = %(subs)r
out = {"modules": {}, "classes": {}, "unavailable_modules": {}}

def public(name):
    if name.startswith("__"):
        return False
    return not name.startswith("_") or name.lstrip("_").replace("_", "").isupper()

pyi = {}
pyi_path = os.path.join(pkgdir, "cimpl.pyi")
if os.path.isfile(pyi_path):
    for node in ast.parse(open(pyi_path, encoding="utf-8").read()).body:
        if isinstance(node, ast.ClassDef):
            for f in node.body:
                if isinstance(f, ast.FunctionDef):
                    a = f.args
                    names = [x.arg for x in a.posonlyargs + a.args + a.kwonlyargs if x.arg not in ("self", "cls")]
                    k = (node.name, f.name)
                    prev = pyi.get(k, {"params": [], "var_pos": False, "var_kw": False})   # @overload: union
                    pyi[k] = {"params": prev["params"] + [x for x in names if x not in prev["params"]], "pos_only": [], "src": "pyi",
                              "var_pos": prev["var_pos"] or a.vararg is not None,
                              "var_kw": prev["var_kw"] or a.kwarg is not None}

def sig(cls, name, attr):
    try:
        s = inspect.signature(attr)
    except (TypeError, ValueError):
        return pyi.get((cls.__name__, name))
    names, posonly, vp, vk = [], [], False, False
    for p in s.parameters.values():
        if p.name in ("self", "cls"):
            continue
        if p.kind == p.VAR_POSITIONAL:
            vp = True
        elif p.kind == p.VAR_KEYWORD:
            vk = True
        elif p.kind == p.POSITIONAL_ONLY:
            posonly.append(p.name)
        else:
            names.append(p.name)
    return {"params": names, "pos_only": posonly, "var_pos": vp, "var_kw": vk, "src": "inspect"}

def record_module(modname, mod):
    names = sorted(n for n in dir(mod) if public(n) and not isinstance(getattr(mod, n, None), types.ModuleType))
    out["modules"][modname] = {"names": names}
    for n in names:
        obj = getattr(mod, n, None)
        if inspect.isclass(obj) and ((getattr(obj, "__module__", "") or "").startswith("confluent_kafka")
                                     or getattr(obj, "__module__", "") == "cimpl"):
            members, methods = [], {}
            for m in sorted(dir(obj)):
                if not public(m):
                    continue
                try:
                    a = inspect.getattr_static(obj, m)
                except AttributeError:
                    continue
                members.append(m)
                if callable(a) or isinstance(a, (staticmethod, classmethod)):
                    raw = a.__func__ if isinstance(a, (staticmethod, classmethod)) else a
                    methods[m] = sig(obj, m, raw)
            out["classes"][modname + "." + n] = {"members": members, "methods": methods}

record_module("confluent_kafka", ck)
for sm in subs:
    try:
        m = importlib.import_module(sm)
    except Exception as e:
        out["unavailable_modules"][sm] = type(e).__name__ + ": " + str(e)[:120]
        continue
    record_module(sm, m)

cands = set()
tok = re.compile(rb"[a-z][a-z0-9_.]{1,63}")
for f in (os.path.join(pkgdir, ".dylibs", "librdkafka.dylib"), *[os.path.join(pkgdir, x) for x in os.listdir(pkgdir) if x.endswith(".so")]):
    if os.path.isfile(f):
        for m in tok.finditer(open(f, "rb").read()):
            cands.add(m.group(0).decode())
props = []
for c in sorted(cands):
    try:
        ck.Producer({c: ""})
    except ck.KafkaException as e:
        if "No such configuration property" not in str(e):
            props.append(c)
    except BaseException:
        props.append(c)
    else:
        props.append(c)
out["config_properties"] = props
out["candidates_tested"] = len(cands)
out["version"] = ck.version()
out["librdkafka"] = ck.libversion()[0]
out["python"] = "%%d.%%d.%%d" %% sys.version_info[:3]
sys.stdout.write(json.dumps(out, sort_keys=True))
"""


def build_python(ck_python):
    """The Python client symbol list, or None when the wheel or the throwaway venv's python is not given."""
    whl = PYPI_DIR / CK_WHEEL["file"]
    if not whl.is_file() or not ck_python:
        return None
    data = whl.read_bytes()
    if len(data) != CK_WHEEL["bytes"] or hashlib.sha256(data).hexdigest() != CK_WHEEL["sha256"]:
        raise BuildError("%s: size or sha256 differs from the pin" % whl.name)
    if not Path(ck_python).is_file():
        raise BuildError("ck python %s does not exist" % ck_python)
    r = subprocess.run([str(ck_python), "-I", "-c", PY_DUMP % {"subs": CK_SUBMODULES}], capture_output=True, text=True,
                       timeout=600, stdin=subprocess.DEVNULL)
    if r.returncode:
        raise BuildError("symbol dump failed: " + r.stderr.strip()[-300:])
    d = json.loads(r.stdout)
    if d["version"] != CK_WHEEL["version"]:
        raise BuildError("the venv has confluent-kafka %s, the pin is %s" % (d["version"], CK_WHEEL["version"]))
    if not d["config_properties"]:
        raise BuildError("no configuration property names found")
    return {"schema": 1, "builder": VERSION, "package": "confluent-kafka", "version": d["version"],
            "librdkafka": d["librdkafka"], "python": d["python"],
            "wheel": {k: CK_WHEEL[k] for k in ("file", "bytes", "sha256", "url", "uploaded", "licence", "why")},
            "config_properties_source": "names found in the strings of librdkafka.dylib and cimpl.so and accepted by "
                                        "librdkafka's configuration parser (Producer({name: ''}) does not answer "
                                        "'No such configuration property'); candidates tested: %d" % d["candidates_tested"],
            "modules": d["modules"], "classes": d["classes"], "unavailable_modules": d["unavailable_modules"],
            "config_properties": d["config_properties"]}


# ------------------------------------------------------------------ verify every cited line
def verify_semantic(sem):
    def check(a):
        if a["line"] is None:                      # source-anchored entry: the line is found, not written
            a["line"] = find_line(a["file"], a["quote"])
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
    ap.add_argument("--ck-python", default=os.environ.get("CK_PYTHON"),
                    help="python of the throwaway venv holding the pinned confluent-kafka wheel (env CK_PYTHON)")
    a = ap.parse_args(argv)
    out = Path(a.out)
    try:
        forb = build_forbidden()
        sem = build_semantic()
        verify_semantic(sem)
        jav = build_java()
        pys = build_python(a.ck_python)
    except BuildError as e:
        print("build_catalogues_a3: FAILED: %s" % e, file=sys.stderr)
        return 2
    out.mkdir(parents=True, exist_ok=True)
    (out / "strimzi_forbidden.json").write_text(dumps(forb), encoding="utf-8")
    (out / "semantic_rules.json").write_text(dumps(sem), encoding="utf-8")
    if jav is not None:
        (out / "java_api.json").write_text(dumps(jav), encoding="utf-8")
        print("java_api   %d classes, %d config names" % (len(jav["classes"]),
              sum(len(v) for v in jav["client_config_names"].values())))
    else:
        print("java_api   SKIPPED: jars or sparse clone not fetched (scripts/fetch_maven_jars.py --clone)")
    if pys is not None:
        (out / "python_symbols.json").write_text(dumps(pys), encoding="utf-8")
        print("python_symbols  confluent-kafka %s: %d modules, %d classes, %d config properties"
              % (pys["version"], len(pys["modules"]), len(pys["classes"]), len(pys["config_properties"])))
    else:
        print("python_symbols  SKIPPED: wheel in raw_catalogue/pypi/ or --ck-python / CK_PYTHON missing")
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
