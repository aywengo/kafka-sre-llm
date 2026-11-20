#!/usr/bin/env python3
"""WP1c T5: the valid-but-wrong generator of the a3 slice of tier A.  No model, no network, no file written under eval/.

`generate()` returns the items (the item format of scripts/eval2_items_a1.py plus a `vbw` block and
`catalogue_hash_a3`). Each item shows an artifact (a Strimzi or Kubernetes manifest, or a properties block) that is
valid and asks "does this do what is wanted?". The answer ends with `ANSWER: works` or `ANSWER: does not work` and,
for "does not work", gives the corrected artifact in one fenced block.

Mechanisms (the rules of work/m4/catalogues_a3/semantic_rules.json, mechanisms V1..V8, plus the forbidden-keys table
work/m4/catalogues_a3/strimzi_forbidden.json, mechanism FK):
  V1 NetworkPolicy from.podSelector       V2 strimzi.io/restart value        V3 a setting equal to its default
  V4 '#' after a value in .properties     V5 heartbeat / session timeout     V6 min.insync.replicas = replicas
  V7 PodDisruptionBudget, no eviction     V8 broker-only key in a KafkaTopic FK forbidden key in a Strimzi config map
Per mechanism: 3 wrong items and 1 control (an artifact that does what is wanted; its answer is `works`).
A mechanism is used only when the table marks it `anchored: true` and not `valid_but_wrong: false`, and every rule of
the scorer module (eval2_checks_a3.RULES) it applies is anchored. V4 (unanchored) and V8 (the Kafka source rejects the
key, so the artifact is not valid) are therefore skipped today; the builders stay in this module, and a rebuilt table
that anchors them makes `generate()` pick them up with no code change. FK takes the places the skipped mechanisms leave,
so the build keeps about 32 items.

Per item, asserted in code (generate() raises AssertionError otherwise):
  - the shown artifact is valid: it parses and, for a manifest, passes Catalogues.validate_resource (covered, no
    violation); for a properties block every key is a known consumer setting;
  - for a wrong item the oracle checks (semantic_rule, forbidden_keys, yaml_config_keys) fail the shown artifact and
    pass the corrected one; for a control item they pass the shown artifact;
  - the reference passes every check; the perturbed answer (the shown artifact with the wrong verdict) fails the verdict;
    for a wrong item the shown artifact with `does not work` fails an oracle check (a verdict alone is not enough).
Deterministic: every template has its own random.Random seeded by a string; pools are sorted lists; no set order.
Version 3 (fix round 2, N1, N2, N8): `FRESH` holds further drafts that were never in an open file (a fault and a control as a
new pair for V1, V2, V3 and V7; a control for V5 on the open side and for V6 on the hidden side); their ids are `-v3-NNN`. The
V6 fault `sessions:5` (never published) is dropped so that the hidden V6 pair is a fault and a control; its number is not
reused. Every template now has controls in two split groups, so the split can put a control on each side. The
connector.plugin.version item carries a note (N8). Items of version 2 keep their id and text.
Usage: .venv/bin/python scripts/eval2_items_a3_vbw.py [--out FILE]   (prints the counts; writes only to --out)
"""
import argparse
import copy
import hashlib
import json
import random
import re
import sys
from collections import Counter
from fractions import Fraction
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2_checks_a3 as c3  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag import catalogue_a3  # noqa: E402
from kafka_sre_rag.catalogue import KNOWN, Catalogues, catalogue_hash  # noqa: E402

VERSION = "eval2_items_a3_vbw/3"   # revision 3: see R3 below
SEED = "m4-a3-vbw-1"
TARGET_TOTAL = 32
ORACLE_TYPES = ("semantic_rule", "forbidden_keys", "yaml_config_keys")
FMT = ("Finish with a final line `ANSWER: works` or `ANSWER: does not work`. If it does not work, give the corrected "
       "%s in one fenced code block before that line and change only what is needed.")

# rules (eval2_checks_a3.RULES) each mechanism applies
MECH_RULES = {
    "V1": ["netpol.pod_selector_own_namespace", "netpol.namespace_selector_all_pods",
           "netpol.both_in_one_entry_is_and", "netpol.separate_entries_are_or"],
    "V2": ["restart.connector_true_or_params", "restart.mm2_connector"],
    "V3": [], "V4": ["properties.load_semantics"], "V5": ["config.heartbeat_below_session_third"],
    "V6": ["config.min_isr_below_replicas"], "V7": ["pdb.zero_evictions"], "V8": [],
}
TEMPLATES = {"V1": "vbw_netpol_from", "V2": "vbw_restart_annotation", "V3": "vbw_default_value",
             "V4": "vbw_properties_comment", "V5": "vbw_heartbeat_session", "V6": "vbw_min_isr_replicas",
             "V7": "vbw_pdb_no_eviction", "V8": "vbw_topic_broker_key", "FK": "vbw_forbidden_key"}
TEMPLATE_ORDER = [TEMPLATES[m] for m in ("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "FK")]
DOMAIN = {"V1": "kubernetes", "V7": "kubernetes", "V2": "connect", "V3": "strimzi", "V4": "kafka", "V5": "kafka",
          "V6": "strimzi", "V8": "strimzi", "FK": "strimzi"}
VERSIONS = {"V1": {"kubernetes": "v1.37.2"}, "V7": {"kubernetes": "v1.37.2"}, "V4": {"kafka": "4.3"},
            "V5": {"kafka": "4.3"}}
STRIMZI_V = {"kafka": "4.3", "strimzi": "1.3.1"}


def dump(doc):
    return yaml.safe_dump(doc, sort_keys=False, default_flow_style=False).rstrip("\n")


def fence(body, lang):
    return "```%s\n%s\n```" % (lang, body.rstrip("\n"))


def pick(rng, pool, n):
    return rng.sample(sorted(pool), n)


def num_of(default):
    m = re.match(r"\s*(-?\d+(?:\.\d+)?)", str(default))
    return Fraction(m.group(1)) if m else None


# ------------------------------------------------------------------ source records (file, line, phrase on that line)
def anchors_of(mech, table):
    out = []
    for a in table["mechanisms"][mech].get("anchors", []):
        out.append({"catalogue": "catalogues_a3", "entry": mech, "file": a["file"], "line": a["line"],
                    "phrase": a["quote"]})
    return out


def entry_source(cats, key, scope):
    e = next(x for x in cats._config[key] if x["scope"] == scope)
    return {"catalogue": "kafka_config", "entry": "%s (%s)" % (key, scope), "file": e["source_file"],
            "line": e["source_line"], "phrase": key}


def default_source(cats, key, scope):
    """The `Default:` row of the key's own entry in the generated reference (the first one after its heading)."""
    e = next(x for x in cats._config[key] if x["scope"] == scope)
    lines = (ROOT / e["source_file"]).read_text(errors="replace").split("\n")
    for n in range(e["source_line"], min(len(lines), e["source_line"] + 10) + 1):
        if "<th>Default:</th>" in lines[n - 1]:
            return {"catalogue": "kafka_config", "entry": "%s (%s) Default" % (key, scope), "file": e["source_file"],
                    "line": n, "phrase": re.sub(r"\s+", " ", lines[n - 1]).strip()}
    raise AssertionError("no Default row for %s" % key)


def forbidden_source(kind, key, config_path=None, exception=False):
    res = catalogue_a3.resources()
    name = kind if config_path is None else "%s:%s" % (kind, config_path)
    e = res[name]
    if exception:
        for a in e["anchors"]:
            if a["role"] == "exceptions" and key in a["items"]:
                return {"catalogue": "catalogues_a3", "entry": "%s exception" % name, "file": a["file"],
                        "line": a["line"], "phrase": key}
        raise AssertionError("no exception anchor for %s %s" % (name, key))
    hit = catalogue_a3.forbidden(kind, key, config_path)
    assert hit, "%s %s is not forbidden" % (name, key)
    f, ln = hit["file"], hit["line"]
    if ln is None:
        f, _, ln = e["source"].rpartition(":")
        ln = int(ln.split(",")[0])
    return {"catalogue": "catalogues_a3", "entry": "%s forbids %s" % (name, hit["match"]), "file": f, "line": ln,
            "phrase": hit["match"]}


# ------------------------------------------------------------------ drafts
def draft(mech, role, group, intro, artifact, shown, why, lang="yaml", fix=None, struct=None, oracle=None,
          sources=None, rules=None, noun="manifest", extra=None):
    return {"mech": mech, "role": role, "group": group, "intro": intro, "artifact": artifact, "shown": shown,
            "why": why, "lang": lang, "fix": fix, "struct": struct or [], "oracle": oracle or [],
            "sources": sources or [], "rules": rules or MECH_RULES.get(mech, []), "noun": noun,
            "extra": extra or {}}


def yaml_struct(kind, api, fields):
    return [{"type": "yaml_one_doc", "kind": kind, "api_version": api}, {"type": "yaml_strict", "kind": kind},
            {"type": "yaml_fields_equal", "kind": kind,
             "fields": [{"path": p, "value": v} for p, v in fields]}]


# ---- V1 NetworkPolicy
def b_v1(rng, cats, table, fresh=None):
    names = fresh["pool"] if fresh else pick(rng, [("orders-kafka", "kafka", "payments", "orders-api", "reporting"),
                       ("events", "streaming", "checkout", "checkout-svc", "analytics"),
                       ("audit-log", "messaging", "fraud", "fraud-scorer", "ingest"),
                       ("clickstream", "data-platform", "ingest", "ingest-gateway", "billing"),
                       ("telemetry", "kafka-prod", "analytics", "report-exporter", "payments")], 4)
    nsl = lambda n: {"kubernetes.io/metadata.name": n}  # noqa: E731

    def pol(cluster, kns, app, frm):
        return {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                "metadata": {"name": "allow-%s-to-%s" % (app, cluster), "namespace": kns},
                "spec": {"podSelector": {"matchLabels": {"strimzi.io/cluster": cluster}},
                         "policyTypes": ["Ingress"],
                         "ingress": [{"ports": [{"protocol": "TCP", "port": 9092}], "from": frm}]}}

    def chks(cluster, kns, cns, app, other_ns, other_app):
        """One allow probe and three deny probes (version 2): the same label in another namespace, another pod of
        the source namespace, the same label in the target (policy) namespace."""
        tgt = {"namespace": kns, "pod_labels": {"strimzi.io/cluster": cluster}}
        allns = {n: nsl(n) for n in (kns, cns, other_ns)}
        mk = lambda src, exp: {"type": "semantic_rule", "oracle": "netpol_allows", "rule": "V1", "port": 9092,  # noqa
                               "source": src, "target": tgt, "namespaces": allns, "expect": exp}
        return [mk({"namespace": cns, "pod_labels": {"app": app}}, True),
                mk({"namespace": other_ns, "pod_labels": {"app": app}}, False),
                mk({"namespace": cns, "pod_labels": {"app": other_app}}, False),
                mk({"namespace": kns, "pod_labels": {"app": app}}, False)]

    def fields(p):
        s, m = p["spec"], p["metadata"]
        return [(["metadata", "name"], m["name"]), (["metadata", "namespace"], m["namespace"]),
                (["spec", "podSelector"], s["podSelector"]), (["spec", "policyTypes"], s["policyTypes"]),
                (["spec", "ingress", 0, "ports"], s["ingress"][0]["ports"])]

    srcs = anchors_of("V1", table)
    out = []
    kinds = fresh["kinds"] if fresh else ["pod_only", "or_entries", "ns_only", "control"]
    for (cluster, kns, cns, app, other), kind in zip(names, kinds):
        pod = {"podSelector": {"matchLabels": {"app": app}}}
        nsel = {"namespaceSelector": {"matchLabels": nsl(cns)}}
        both = {**nsel, **pod}
        ctx = "The brokers of the Kafka cluster `%s` run in namespace `%s`, where ingress is denied unless a NetworkPolicy allows it. " % (cluster, kns)
        if kind == "pod_only":
            shown = pol(cluster, kns, app, [pod])
            intro = ctx + "Only the pods labelled `app: %s` in namespace `%s` must reach the brokers on TCP 9092; nothing from other namespaces. Does this NetworkPolicy do that?" % (app, cns)
            fixd = pol(cluster, kns, app, [both])
            why = "The entry has only a podSelector, which selects pods of the policy's own namespace `%s`, not of `%s`; the selectors must be in one entry." % (kns, cns)
            oc = chks(cluster, kns, cns, app, other, "other-" + app)
        elif kind == "or_entries":
            shown = pol(cluster, kns, app, [nsel, pod])
            intro = ctx + "Only the pods labelled `app: %s` in namespace `%s` must reach the brokers on TCP 9092. Does this NetworkPolicy do that?" % (app, cns)
            fixd = pol(cluster, kns, app, [both])
            why = "Two entries in `from` are ORed: every pod of `%s` is allowed, and so is any `app: %s` pod of `%s`. One entry with both selectors is an AND." % (cns, app, kns)
            oc = chks(cluster, kns, cns, app, other, "other-" + app)
        elif kind == "ns_only":
            shown = pol(cluster, kns, app, [nsel])
            intro = ctx + "Only the pods labelled `app: %s` in namespace `%s` must reach the brokers on TCP 9092, not the other pods of that namespace. Does this NetworkPolicy do that?" % (app, cns)
            fixd = pol(cluster, kns, app, [both])
            why = "A namespaceSelector alone selects all pods of the matching namespace; the podSelector has to be in the same entry."
            oc = chks(cluster, kns, cns, app, other, "other-" + app)
        else:
            shown = pol(cluster, kns, app, [both])
            intro = ctx + "Only the pods labelled `app: %s` in namespace `%s` must reach the brokers on TCP 9092. Does this NetworkPolicy do that?" % (app, cns)
            fixd = None
            why = "Both selectors are in one entry, so only `app: %s` pods of `%s` match."
            why = why % (app, cns)
            oc = chks(cluster, kns, cns, app, other, "other-" + app)
        out.append(draft("V1", "control" if kind == "control" else "wrong", "%s:%s" % (kind, cluster), intro,
                         "NetworkPolicy", dump(shown), why, fix=dump(fixd) if fixd else None,
                         struct=yaml_struct("NetworkPolicy", "networking.k8s.io/v1", fields(shown)), oracle=oc,
                         sources=srcs))
    return out


# ---- V2 restart annotation
def b_v2(rng, cats, table, fresh=None):
    conns = fresh["pool"] if fresh else pick(rng, [("orders-sink", "orders-connect", "streaming", "org.apache.kafka.connect.file.FileStreamSinkConnector", {"file": "/data/orders.txt", "topics": "orders"}),
                       ("audit-source", "audit-connect", "platform", "org.apache.kafka.connect.file.FileStreamSourceConnector", {"file": "/var/log/audit.log", "topic": "audit"}),
                       ("metrics-source", "metrics-connect", "observability", "org.apache.kafka.connect.file.FileStreamSourceConnector", {"file": "/var/log/metrics.log", "topic": "metrics"}),
                       ("ledger-sink", "ledger-connect", "finance", "org.apache.kafka.connect.file.FileStreamSinkConnector", {"file": "/data/ledger.txt", "topics": "ledger"})], 4)
    srcs = anchors_of("V2", table)
    api = "kafka.strimzi.io/v1"

    def conn(name, cluster, ns, cls, cfg, value):
        return {"apiVersion": api, "kind": "KafkaConnector",
                "metadata": {"name": name, "namespace": ns, "labels": {"strimzi.io/cluster": cluster},
                             "annotations": {"strimzi.io/restart": value}},
                "spec": {"class": cls, "tasksMax": 2, "config": cfg}}

    def mm2(name, ns, ann):
        return {"apiVersion": api, "kind": "KafkaMirrorMaker2",
                "metadata": {"name": name, "namespace": ns, "annotations": {"strimzi.io/restart-connector": ann}},
                "spec": {"replicas": 1,
                         "target": {"alias": "target", "bootstrapServers": "target-kafka-bootstrap:9092",
                                    "groupId": "%s-group" % name, "configStorageTopic": "%s-configs" % name,
                                    "offsetStorageTopic": "%s-offsets" % name, "statusStorageTopic": "%s-status" % name},
                         "mirrors": [{"source": {"alias": "source", "bootstrapServers": "source-kafka-bootstrap:9092"},
                                      "sourceConnector": {"tasksMax": 2}, "topicsPattern": "orders.*"}]}}

    def fields_conn(d):
        return [(["metadata", "name"], d["metadata"]["name"]), (["spec", "class"], d["spec"]["class"]),
                (["spec", "config"], d["spec"]["config"])]

    out = []
    for i, (name, cluster, ns, cls, cfg) in zip(fresh["positions"] if fresh else range(4), conns):
        if i == 0:
            shown, fixd = conn(name, cluster, ns, cls, cfg, "true"), conn(name, cluster, ns, cls, cfg, "includeTasks")
            intro = "A Kafka Connect cluster `%s` runs the `KafkaConnector` below. To restart the connector instance and all of its tasks, I apply this manifest. Does it do that?" % cluster
            why = "`strimzi.io/restart: \"true\"` restarts the connector instance only; `includeTasks` restarts the tasks as well."
            exp = {"restart_connector": True, "include_tasks": True, "only_failed": False}
            kind_name = "KafkaConnector"
            fl = fields_conn(shown)
        elif i == 1:
            shown, fixd = conn(name, cluster, ns, cls, cfg, "true"), conn(name, cluster, ns, cls, cfg, "onlyFailed")
            intro = "The `KafkaConnector` below runs on the Connect cluster `%s`. I want Strimzi to restart the connector instance only if it is in the FAILED state, so I apply this manifest. Does it do that?" % cluster
            why = "`true` restarts the instance whatever its state; `onlyFailed` restarts only instances with a FAILED status."
            exp = {"restart_connector": True, "include_tasks": False, "only_failed": True}
            kind_name = "KafkaConnector"
            fl = fields_conn(shown)
        elif i == 2:
            cname = "source->target.MirrorSourceConnector"
            shown, fixd = mm2(name, ns, cname), mm2(name, ns, cname + ":includeTasks")
            intro = "A `KafkaMirrorMaker2` resource named `%s` mirrors `source` to `target`. To restart the connector `%s` together with its tasks, I apply this manifest. Does it do that?" % (name, cname)
            why = "`strimzi.io/restart-connector=<name>` restarts the connector instance only; `<name>:includeTasks` restarts its tasks too."
            exp = {"restart_connector": True, "include_tasks": True, "only_failed": False, "connector": cname}
            kind_name = "KafkaMirrorMaker2"
            fl = [(["metadata", "name"], name), (["spec", "mirrors", 0, "sourceConnector", "tasksMax"], 2)]
        else:
            shown = conn(name, cluster, ns, cls, cfg, "includeTasks,onlyFailed")
            fixd = None
            intro = "The `KafkaConnector` below runs on the Connect cluster `%s`. I want Strimzi to restart the connector instance and its tasks, but only the ones in the FAILED state, so I apply this manifest. Does it do that?" % cluster
            why = "`includeTasks` adds the tasks and `onlyFailed` limits the restart to FAILED instances; the value combines both."
            exp = {"restart_connector": True, "include_tasks": True, "only_failed": True}
            kind_name = "KafkaConnector"
            fl = fields_conn(shown)
        oc = [{"type": "semantic_rule", "oracle": "restart_annotation", "rule": "V2", "kind": kind_name, "expect": exp}]
        out.append(draft("V2", "control" if fixd is None else "wrong", "%s:%s" % (kind_name, name), intro, kind_name,
                         dump(shown), why, fix=dump(fixd) if fixd else None,
                         struct=yaml_struct(kind_name, api, fl), oracle=oc, sources=srcs))
    return out


# ---- V3 value equal to the default
COMPACTED_KEYS = ("delete.retention.ms", "min.cleanable.dirty.ratio")  # act only on topics with cleanup.policy=compact
V3_POOL = [("min.insync.replicas", "a producer with `acks=all` must be refused unless at least 2 replicas hold the record", 2),
           ("retention.ms", "records must be kept for 3 days (259200000 ms)", 259200000),
           ("segment.bytes", "log segments must roll at 256 MiB (268435456 bytes)", 268435456),
           ("max.message.bytes", "the topic must accept record batches of up to 2 MiB (2097152 bytes)", 2097152),
           ("delete.retention.ms", "delete tombstones must be kept for 12 hours (43200000 ms)", 43200000),
           ("min.cleanable.dirty.ratio", "log compaction must start once 10 % of the log is dirty (ratio 0.1)", Fraction(1, 10))]


def b_v3(rng, cats, table, fresh=None):
    picks = fresh["keys"] if fresh else pick(rng, [p[0] for p in V3_POOL], 4)
    out = []
    names = fresh["names"] if fresh else pick(rng, ["orders", "payments-events", "inventory-updates", "session-log", "audit-trail", "page-views"], 4)
    for i, (key, name) in enumerate(zip(picks, names)):
        _, need, want = next(p for p in V3_POOL if p[0] == key)
        ent = next(e for e in cats._config[key] if e["scope"] == "topic")
        dflt = num_of(ent["default"])
        wantv = float(want) if isinstance(want, Fraction) else want
        control = i == (len(picks) - 1 if fresh else 3)
        val = wantv if control else (float(dflt) if "." in str(ent["default"]) else int(dflt))

        compacted = key in COMPACTED_KEYS

        def topic(v):
            cfg = {"cleanup.policy": "compact", key: v} if compacted else {key: v}
            return {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaTopic",
                    "metadata": {"name": name, "namespace": "kafka", "labels": {"strimzi.io/cluster": "events"}},
                    "spec": {"partitions": 6, "replicas": 3, "config": cfg}}
        shown = topic(val)
        fixd = None if control else topic(wantv)
        intro = "In the Strimzi cluster `events`, %s. I apply this `KafkaTopic`. Does it do that?" % need
        if control:
            why = "`%s` is set to %s, which is not the default (%s), and it is what the requirement asks for." % (key, val, ent["default"])
        else:
            why = "`%s: %s` is the default of this setting (%s), so the topic behaves as if nothing was set; the requirement needs %s." % (key, val, ent["default"], wantv)
        oc = [{"type": "semantic_rule", "oracle": "config_rule", "rule": "V3",
               "rules": [{"a": key, "op": "eq", "b": {"const": wantv}}]}]
        fl = [(["metadata", "name"], name), (["spec", "partitions"], 6), (["spec", "replicas"], 3)]
        if compacted:
            fl.append((["spec", "config", "cleanup.policy"], "compact"))
        out.append(draft("V3", "control" if control else "wrong", "%s:%s" % (key, name), intro, "KafkaTopic",
                         dump(shown), why, fix=dump(fixd) if fixd else None,
                         struct=yaml_struct("KafkaTopic", "kafka.strimzi.io/v1", fl), oracle=oc,
                         sources=[entry_source(cats, key, "topic"), default_source(cats, key, "topic")],
                         extra={"default": str(ent["default"])}))
    return out


# ---- V4 comment after a value (only when anchored)
def b_v4(rng, cats, table):
    keys = pick(rng, [("group.id", "billing-consumers"), ("client.id", "ledger-reader"), ("auto.offset.reset", "earliest"),
                      ("isolation.level", "read_committed"), ("client.rack", "eu-west-1a")], 4)
    out = []
    for i, (key, val) in enumerate(keys):
        ctl = i == 3
        base = ["bootstrap.servers=broker-1:9092", "enable.auto.commit=false"]
        lines = lambda body: "\n".join(base + body)  # noqa: E731
        shown = lines(["# the consumer settings", "%s=%s" % (key, val)]) if ctl else lines(["%s=%s   # keep" % (key, val)])
        fixd = None if ctl else lines(["# keep", "%s=%s" % (key, val)])
        intro = "A consumer reads its settings from the properties file below. `%s` must be exactly `%s`. Does the file do that?" % (key, val)
        why = "In a .properties file a `#` after a value is part of the value; the comment must be on its own line." if not ctl else "The comment is on its own line, so the value is exactly `%s`." % val
        oc = [{"type": "semantic_rule", "oracle": "properties_parse", "rule": "V4", "expect": {key: val}}]
        out.append(draft("V4", "control" if ctl else "wrong", "%s" % key, intro, "properties block", shown, why,
                         lang="properties", fix=fixd, oracle=oc, sources=anchors_of("V4", table), noun="properties block"))
    return out


# ---- V5 heartbeat and session timeout
def b_v5(rng, cats, table, fresh=None):
    pool = fresh["pool"] if fresh else pick(rng, [("billing-consumers", 30000), ("ledger-readers", 60000), ("search-indexer", 30000),
                                                  ("fraud-scorer", 45000), ("report-builder", 90000)], 4)
    out = []
    for i, (gid, sess) in enumerate(pool):
        third = sess // 3
        variant = (fresh["variants"] if fresh else ["equal", "above_third", "session_default", "control"])[i]
        body = ["bootstrap.servers=broker-1:9092", "group.id=%s" % gid, "enable.auto.commit=false"]
        if variant == "equal":
            hb, shown_sess = sess, sess
        elif variant == "above_third":
            hb, shown_sess = sess // 2, sess
        elif variant == "session_default":
            sess, third = 45000, 15000
            hb, shown_sess = 30000, None
        else:
            hb, shown_sess = third, sess
        sess_line = ["session.timeout.ms=%d" % shown_sess] if shown_sess is not None else []
        shown = "\n".join(body + sess_line + ["heartbeat.interval.ms=%d" % hb])
        fixd = "\n".join(body + sess_line + ["heartbeat.interval.ms=%d" % third])
        intro = ("The consumer group `%s` keeps `session.timeout.ms` at %d ms%s. Does the consumer configuration below "
                 "set `heartbeat.interval.ms` the way the Kafka consumer documentation asks for?") % (gid, sess, " (the file leaves it unset)" if shown_sess is None else "")
        why = ("The heartbeat interval is %d ms against a session timeout of %d ms; one third is %d ms." % (hb, sess, sess // 3)) \
            if variant != "control" else "The heartbeat interval %d ms is one third of the session timeout %d ms." % (hb, sess)
        oc = [{"type": "semantic_rule", "oracle": "config_rule", "rule": "V5", "defaults": {"session.timeout.ms": 45000},
               "rules": [{"a": "heartbeat.interval.ms", "op": "lt", "b": "session.timeout.ms"},
                         {"a": "heartbeat.interval.ms", "op": "le", "b": "session.timeout.ms", "scale": "1/3"},
                         {"a": "session.timeout.ms", "op": "eq", "b": {"const": sess}}]}]
        out.append(draft("V5", "control" if variant == "control" else "wrong", "%s:%s" % (variant, gid), intro,
                         "properties block", shown, why, lang="properties", fix=None if variant == "control" else fixd,
                         oracle=oc, sources=anchors_of("V5", table) + [entry_source(cats, "heartbeat.interval.ms", "consumer")],
                         noun="properties block"))
    return out


# ---- V6 min.insync.replicas equal to replicas
def b_v6(rng, cats, table, fresh=None):
    pool = fresh["pool"] if fresh else pick(rng, [("orders", 3), ("payments", 3), ("ledger", 2), ("shipments", 4), ("sessions", 5)], 4)
    out = []
    for i, (name, rep) in enumerate(pool):
        ctl = fresh["ctl"][i] if fresh and "ctl" in fresh else i == (len(pool) - 1 if fresh else 3)
        minisr = rep - 1 if ctl else rep
        if rep == 2 and ctl:
            rep, minisr = 3, 2
        if rep == 2:
            floor, need = 2, "at least 2 replicas must hold every acknowledged write"
        else:
            floor, need = 2, "at least 2 replicas must hold every acknowledged write"

        def topic(r, m):
            return {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaTopic",
                    "metadata": {"name": name, "namespace": "kafka", "labels": {"strimzi.io/cluster": "events"}},
                    "spec": {"partitions": 6, "replicas": r, "config": {"min.insync.replicas": m}}}
        shown = topic(rep, minisr)
        fixd = None if ctl else topic(3 if rep == 2 else rep, 2 if rep <= 3 else rep - 1)
        intro = ("Producers write to the topic `%s` of the cluster `events` with `acks=all`; %s, and writes must keep "
                 "succeeding while one broker is down. Does this `KafkaTopic` do that?") % (name, need)
        why = ("With `min.insync.replicas` equal to `replicas` (%d), one lost replica leaves the in-sync set below the minimum and `acks=all` writes fail." % rep) if not ctl \
            else "With %d replicas and `min.insync.replicas: %d`, one replica can be lost and writes still succeed." % (rep, minisr)
        oc = [{"type": "semantic_rule", "oracle": "config_rule", "rule": "V6",
               "rules": [{"a": "min.insync.replicas", "op": "lt", "b": {"path": "spec.replicas"}},
                         {"a": "min.insync.replicas", "op": "ge", "b": {"const": floor}}]}]
        fl = [(["metadata", "name"], name), (["spec", "partitions"], 6)]
        out.append(draft("V6", "control" if ctl else "wrong", "%s:%d" % (name, rep), intro, "KafkaTopic", dump(shown), why,
                         fix=dump(fixd) if fixd else None, struct=yaml_struct("KafkaTopic", "kafka.strimzi.io/v1", fl),
                         oracle=oc, sources=anchors_of("V6", table) + [entry_source(cats, "min.insync.replicas", "topic")]))
    return out


# ---- V7 PodDisruptionBudget
def b_v7(rng, cats, table, fresh=None):
    pool = fresh["pool"] if fresh else pick(rng, [("orders-kafka", 3), ("events-kafka", 5), ("audit-kafka", 3), ("ledger-kafka", 4), ("search-kafka", 6)], 4)
    out = []
    for i, (name, rep) in enumerate(pool):
        variant = (fresh["variants"] if fresh else ["max0", "min100", "minall", "control"])[i]
        spec = {"max0": {"maxUnavailable": 0}, "min100": {"minAvailable": "100%"}, "minall": {"minAvailable": rep},
                "control": {"maxUnavailable": 1}}[variant]

        def pdb(s):
            return {"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                    "metadata": {"name": "%s-pdb" % name, "namespace": "kafka"},
                    "spec": {**s, "selector": {"matchLabels": {"app": name}}}}
        shown = pdb(spec)
        fixd = None if variant == "control" else pdb({"maxUnavailable": 1})
        intro = "The StatefulSet `%s` runs %d broker pods. Node drains must be able to evict one broker pod at a time. Does this PodDisruptionBudget allow that?" % (name, rep)
        why = ("The budget requires zero voluntary evictions (%s), so a node drain cannot evict a broker pod." % json.dumps(spec)) \
            if variant != "control" else "`maxUnavailable: 1` allows one voluntary eviction at a time."
        oc = [{"type": "semantic_rule", "oracle": "pdb_evicts", "rule": "V7", "replicas": rep, "expect": True,
               "allowed": 1}]
        fl = [(["metadata", "name"], "%s-pdb" % name), (["spec", "selector"], {"matchLabels": {"app": name}})]
        out.append(draft("V7", "control" if variant == "control" else "wrong", "%s:%s" % (variant, name), intro,
                         "PodDisruptionBudget", dump(shown), why, fix=dump(fixd) if fixd else None,
                         struct=yaml_struct("PodDisruptionBudget", "policy/v1", fl), oracle=oc,
                         sources=anchors_of("V7", table)))
    return out


# ---- V8 broker-only key in a KafkaTopic (only when the table marks it valid_but_wrong)
V8_POOL = [("log.retention.hours", 72, "retention.ms", 259200000, "keep records for 3 days"),
           ("log.segment.bytes", 268435456, "segment.bytes", 268435456, "roll log segments at 256 MiB"),
           ("log.cleanup.policy", "compact", "cleanup.policy", "compact", "compact the topic"),
           ("log.message.timestamp.type", "LogAppendTime", "message.timestamp.type", "LogAppendTime", "stamp records with the broker's append time")]


def b_v8(rng, cats, table):
    pool = pick(rng, V8_POOL, 4)
    out = []
    for i, (bk, bv, tk, tv, need) in enumerate(pool):
        ctl = i == 3

        def topic(k, v):
            return {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaTopic",
                    "metadata": {"name": "t-%d" % i, "namespace": "kafka", "labels": {"strimzi.io/cluster": "events"}},
                    "spec": {"partitions": 3, "replicas": 3, "config": {k: v}}}
        shown = topic(tk, tv) if ctl else topic(bk, bv)
        fixd = None if ctl else topic(tk, tv)
        intro = "In the cluster `events` I want to %s for the topic below. Does this `KafkaTopic` do that?" % need
        why = "`%s` is a broker setting; the topic setting is `%s`." % (bk, tk)
        oc = [{"type": "yaml_config_keys", "kind": "KafkaTopic", "path": ["spec", "config"], "scope": "topic"}]
        fl = [(["spec", "config", tk], tv)]
        out.append(draft("V8", "control" if ctl else "wrong", "%s" % tk, intro, "KafkaTopic", dump(shown), why,
                         fix=dump(fixd) if fixd else None, struct=yaml_struct("KafkaTopic", "kafka.strimzi.io/v1", fl),
                         oracle=oc, sources=anchors_of("V8", table)))
    return out


# ---- FK forbidden keys
def _kafka(cfg):
    return {"apiVersion": "kafka.strimzi.io/v1", "kind": "Kafka", "metadata": {"name": "events", "namespace": "kafka"},
            "spec": {"kafka": {"listeners": [{"name": "plain", "port": 9092, "type": "internal", "tls": False}],
                               "config": cfg},
                     "entityOperator": {"topicOperator": {}, "userOperator": {}}}}


def _connect(cfg):
    return {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaConnect", "metadata": {"name": "events-connect", "namespace": "kafka"},
            "spec": {"replicas": 1, "bootstrapServers": "events-kafka-bootstrap:9092", "groupId": "events-connect",
                     "configStorageTopic": "events-connect-configs", "offsetStorageTopic": "events-connect-offsets",
                     "statusStorageTopic": "events-connect-status", "config": cfg}}


def _connector(cfg, **spec):
    return {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaConnector",
            "metadata": {"name": "orders-sink", "namespace": "kafka", "labels": {"strimzi.io/cluster": "events-connect"}},
            "spec": {**{"class": "org.apache.kafka.connect.file.FileStreamSinkConnector", "tasksMax": 2}, **spec, "config": cfg}}


def _mm2(src_cfg=None, tgt_cfg=None, conn_cfg=None):
    d = {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaMirrorMaker2", "metadata": {"name": "mirror", "namespace": "kafka"},
         "spec": {"replicas": 1,
                  "target": {"alias": "target", "bootstrapServers": "target-kafka-bootstrap:9092", "groupId": "mirror",
                             "configStorageTopic": "mirror-configs", "offsetStorageTopic": "mirror-offsets",
                             "statusStorageTopic": "mirror-status"},
                  "mirrors": [{"source": {"alias": "source", "bootstrapServers": "source-kafka-bootstrap:9092"},
                               "sourceConnector": {"tasksMax": 2}, "topicsPattern": "orders.*"}]}}
    if tgt_cfg:
        d["spec"]["target"]["config"] = tgt_cfg
    if src_cfg:
        d["spec"]["mirrors"][0]["source"]["config"] = src_cfg
    if conn_cfg:
        d["spec"]["mirrors"][0]["sourceConnector"]["config"] = conn_cfg
    return d


N8_NOTE = ("The reference only removes the entry from `config`. The KafkaConnector resource has a field for the plugin "
           "version, `spec.version` (appendix_crds.adoc:3473-3475); an answer that moves the version there is also right "
           "and passes the structure checks (review round 2, N8: left as it is).")
FK_KAFKA = [("log.dirs", "/var/lib/kafka/data-0/kafka-log0", "put the log directory of the brokers on `/var/lib/kafka/data-0/kafka-log0`"),
            ("listeners", "PLAINTEXT://:9092", "bind the broker listener to port 9092"),
            ("inter.broker.listener.name", "REPLICATION-9091", "use the listener `REPLICATION-9091` between brokers"),
            ("sasl.enabled.mechanisms", "SCRAM-SHA-512", "enable SCRAM-SHA-512 on the brokers"),
            ("authorizer.class.name", "org.apache.kafka.metadata.authorizer.StandardAuthorizer", "use the standard authorizer"),
            ("super.users", "User:admin", "make `User:admin` a super user")]
FK_KAFKA_OK = [("ssl.enabled.protocols", "TLSv1.3", "allow only TLS 1.3"),
               ("cruise.control.metrics.topic.min.insync.replicas", 2, "set the Cruise Control metrics topic to 2 in-sync replicas"),
               ("controller.quorum.fetch.timeout.ms", 3000, "shorten the controller quorum fetch timeout to 3 seconds")]
FK_CONNECT = [("config.storage.topic", "events-connect-configs", "store the connector configurations in `events-connect-configs`"),
              ("plugin.path", "/opt/kafka/plugins", "load plugins from `/opt/kafka/plugins`"),
              ("bootstrap.servers", "events-kafka-bootstrap:9092", "connect to `events-kafka-bootstrap:9092`")]
FK_CONNECT_OK = [("ssl.protocol", "TLSv1.3", "use TLS 1.3"), ("ssl.cipher.suites", "TLS_AES_256_GCM_SHA384", "restrict the cipher suites")]
FK_BASE = {"num.partitions": 6, "default.replication.factor": 3}
FK_CONNECT_BASE = {"offset.flush.interval.ms": 10000}


def b_fk(rng, cats, table, quota):
    n_ctl = max(1, quota // 4)
    n_wrong = quota - n_ctl
    wrong_slots = ["kafka", "kafka2", "connect", "tasks", "plugin", "mm2", "bridge", "kafka3"][:n_wrong]
    ctl_slots = ["kafka_ok", "connect_ok", "kafka_ok2", "connect_ok2"][:n_ctl]
    kk, kk2 = pick(rng, FK_KAFKA, 2)
    rest = [x for x in FK_KAFKA if x not in (kk, kk2)]
    kk3 = rng.choice(sorted(rest))
    cc = rng.choice(sorted(FK_CONNECT))
    kok, kok2 = pick(rng, FK_KAFKA_OK, 2)
    cok, cok2 = pick(rng, FK_CONNECT_OK, 2)
    out = []

    def add(role, group, kind, intro, shown, fixd, why, fl, path, key, exc=False, cpath=None):
        note = {"note": N8_NOTE} if key == "connector.plugin.version" else {}
        if role == "wrong":
            src = [forbidden_source(kind, key, cpath)]
        else:
            src = [forbidden_source(kind, key, cpath, exception=True)]
        struct = yaml_struct(kind, "kafka.strimzi.io/v1", fl) + [{"type": "forbidden_keys", "kinds": [kind]}]
        # forbidden_keys is an oracle check, structure checks keep their place before it
        oc = [struct.pop()]
        out.append(draft("FK", role, group, intro, kind, dump(shown), why, fix=dump(fixd) if fixd else None,
                         struct=struct, oracle=oc, sources=src, extra={"config_path": path, "key": key, **note}))

    def cfgq(path):
        return "Will Strimzi apply every entry of `%s` below as written?" % path

    for slot in wrong_slots:
        if slot in ("kafka", "kafka2", "kafka3"):
            k, v, need = {"kafka": kk, "kafka2": kk2, "kafka3": kk3}[slot]
            shown = _kafka({**FK_BASE, k: v})
            fixd = _kafka(dict(FK_BASE))
            add("wrong", "Kafka:" + k, "Kafka", "I want to %s on the cluster `events`. %s If not, return the manifest without the entries it does not take from `config`." % (need, cfgq("spec.kafka.config")),
                shown, fixd, "`%s` is a key Strimzi manages itself; it is forbidden in `spec.kafka.config`." % k,
                [(["spec", "kafka", "config", a], b) for a, b in FK_BASE.items()], "spec.kafka.config", k)
        elif slot == "connect":
            k, v, need = cc
            shown = _connect({**FK_CONNECT_BASE, k: v})
            fixd = _connect(dict(FK_CONNECT_BASE))
            add("wrong", "KafkaConnect:" + k, "KafkaConnect", "On the Connect cluster below I want to %s. %s If not, return the manifest without the entries it does not take from `config`." % (need, cfgq("spec.config")),
                shown, fixd, "`%s` is forbidden in the `config` of a KafkaConnect resource; Strimzi sets it." % k,
                [(["spec", "config", a], b) for a, b in FK_CONNECT_BASE.items()], "spec.config", k)
        elif slot == "tasks":
            k, field, v = rng.choice(sorted([("tasks.max", "tasksMax", 3), ("connector.class", "class", "org.apache.kafka.connect.file.FileStreamSourceConnector")]))
            cfg = {"file": "/data/orders.txt", "topics": "orders", k: v}
            shown = _connector(cfg)
            if field == "tasksMax":
                shown["spec"].pop("tasksMax")
            else:
                shown["spec"].pop("class")
                shown["spec"]["tasksMax"] = 2
            fixd = copy.deepcopy(shown)
            fixd["spec"][field] = v
            fixd["spec"]["config"] = {"file": "/data/orders.txt", "topics": "orders"}
            what = "run 3 tasks" if field == "tasksMax" else "use the connector class `%s`" % v
            add("wrong", "KafkaConnector:" + k, "KafkaConnector", "The connector below should %s. %s If not, return the manifest with the setting where Strimzi takes it." % (what, cfgq("spec.config")),
                shown, fixd, "`%s` cannot be set in a KafkaConnector `config`; the resource has the field `spec.%s`." % (k, field),
                [(["spec", field], v), (["spec", "config", "file"], "/data/orders.txt")], "spec.config", k)
        elif slot == "plugin":
            cfg = {"file": "/data/orders.txt", "topics": "orders", "connector.plugin.version": "2.1.0"}
            shown = _connector(cfg)
            fixd = _connector({"file": "/data/orders.txt", "topics": "orders"})
            add("wrong", "KafkaConnector:connector.plugin.version", "KafkaConnector", "The connector below is meant to use version 2.1.0 of its plugin. %s If not, return the manifest without the entries it does not take from `config`." % cfgq("spec.config"),
                shown, fixd, "`connector.plugin.version` is in the list of properties that cannot be set in a KafkaConnector `config`.",
                [(["spec", "config", "file"], "/data/orders.txt")], "spec.config", "connector.plugin.version")
        elif slot == "mm2":
            shown = _mm2(conn_cfg={"replication.factor": 3, "tasks.max": 4})
            fixd = _mm2(conn_cfg={"replication.factor": 3})
            fixd["spec"]["mirrors"][0]["sourceConnector"]["tasksMax"] = 4
            add("wrong", "KafkaMirrorMaker2:tasks.max", "KafkaMirrorMaker2", "The mirror source connector below should run 4 tasks. %s If not, return the manifest with the setting where Strimzi takes it." % cfgq("spec.mirrors[].sourceConnector.config"),
                shown, fixd, "`tasks.max` cannot be set in the connector `config`; the field is `tasksMax`.",
                [(["spec", "mirrors", 0, "sourceConnector", "tasksMax"], 4),
                 (["spec", "mirrors", 0, "sourceConnector", "config", "replication.factor"], 3)],
                "spec.mirrors[].sourceConnector.config", "tasks.max", cpath="spec.mirrors[].sourceConnector.config")
        elif slot == "bridge":
            def bridge(cfg):
                return {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaBridge", "metadata": {"name": "events-bridge", "namespace": "kafka"},
                        "spec": {"replicas": 1, "bootstrapServers": "events-kafka-bootstrap:9092", "http": {"port": 8080}, "config": cfg}}
            add("wrong", "KafkaBridge:bridge.id", "KafkaBridge", "The bridge below should identify itself as `events-bridge-1`. %s If not, return the manifest without the entries it does not take from `config`." % cfgq("spec.config"),
                bridge({"bridge.id": "events-bridge-1"}), bridge({}), "`bridge.id` is forbidden in the bridge `config`.",
                [(["spec", "http", "port"], 8080)], "spec.config", "bridge.id")
    for slot in ctl_slots:
        if slot.startswith("kafka"):
            k, v, need = kok if slot == "kafka_ok" else kok2
            add("control", "Kafka:" + k, "Kafka", "I want to %s on the cluster `events`. %s" % (need, cfgq("spec.kafka.config")),
                _kafka({**FK_BASE, k: v}), None,
                "`%s` starts with a forbidden prefix but is one of the documented exceptions, so Strimzi applies it." % k,
                [], "spec.kafka.config", k, exc=True)
        else:
            k, v, need = cok if slot == "connect_ok" else cok2
            add("control", "KafkaConnect:" + k, "KafkaConnect", "On the Connect cluster below I want to %s. %s" % (need, cfgq("spec.config")),
                _connect({**FK_CONNECT_BASE, k: v}), None,
                "`%s` has a forbidden prefix but is a documented exception." % k, [], "spec.config", k, exc=True)
    return out


# Round 2 (N1, N2): further drafts that were never in an open file. A template that was left without a "works" control on
# one side gets one (V5 open, V6 hidden), a template whose hidden items were all in an open file gets a new fault and a new
# control as a pair (V1, V2, V3, V7). `pair` joins an existing pair where the control belongs to its faults.
FRESH = {
    "V1": {"kinds": ["or_entries", "control"],
           "pool": [("inventory-kafka", "stock", "warehouse", "stock-sync", "audit"),
                    ("returns-kafka", "commerce", "refunds", "refund-api", "finance")]},
    "V2": {"positions": [1, 3],
           "pool": [("refunds-source", "refunds-connect", "support", "org.apache.kafka.connect.file.FileStreamSourceConnector",
                     {"file": "/var/log/refunds.log", "topic": "refunds"}),
                    ("inventory-sink", "inventory-connect", "logistics", "org.apache.kafka.connect.file.FileStreamSinkConnector",
                     {"file": "/data/inventory.txt", "topics": "inventory"})]},
    "V3": {"keys": ["segment.bytes", "min.insync.replicas"], "names": ["payments-events", "inventory-updates"]},
    "V5": {"variants": ["control"], "pool": [("shipment-tracker", 60000)]},
    "V6": {"pool": [("inventory", 4)]},
    "V7": {"variants": ["minall", "control"], "pool": [("billing-kafka", 4), ("fraud-kafka", 3)]},
}
FRESH_PAIR = {"V1": "vbw:V1:c", "V2": "vbw:V2:c", "V3": "vbw:V3:c", "V5": "vbw:V5:b", "V6": "vbw:V6:b", "V7": "vbw:V7:c"}
# A hidden fault of version 2 that is not used any more: V6 keeps the fault `orders:3` and gets the new control, so that the
# hidden side has a pair (fault, control); the fault `sessions:5` is dropped (it was never in an open file). Its number is not reused.
SKIP = {("V6", "sessions:5"), ("V6", "orders:3")}   # revision 3: orders:3 repeated the published fault `payments:3`
# Revision 3: version-3 drafts (position k, 1-based) replaced because their variant was published.
SKIP_FRESH = {("V1", 1), ("V1", 2), ("V2", 1), ("V2", 2), ("V7", 1), ("V7", 2)}

# ------------------------------------------------------------------ revision 3 (Owner's decision L7, 2026-11-20)
# The hidden valid-but-wrong items whose fault or control variant (eval2_items_a3.variant_key) was published in an open file
# under other resource names are redrawn with variants that were never published; the new items are `-v3-1NN`. Every variant
# is built from the same pinned sources and judged by the same oracles; the checks module is not touched.
SOURCE_LINES = {
    "restart_task": ("raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector-task.adoc", 35,
                     "Restart the connector task by adding the `strimzi.io/restart-task` annotation to the `KafkaConnector` resource:"),
    "restart_task_id": ("raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector-task.adoc", 33,
                        "Task IDs are non-negative integers, starting from 0."),
    "pdb_round_min": ("raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md", 99,
                      "rounds up to the nearest integer, so in this case, 4 Pods must be available. When you"),
    "pdb_round_max": ("raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md", 100,
                      "specify the value `maxUnavailable` as a percentage, Kubernetes rounds up the number of"),
}


def line_source(key, entry):
    f, ln, phrase = SOURCE_LINES[key]
    assert phrase in (ROOT / f).read_text(errors="replace").split("\n")[ln - 1], "%s:%d does not hold the phrase" % (f, ln)
    return {"catalogue": "raw", "entry": entry, "file": f, "line": ln, "phrase": phrase}


def r3_v1(table):
    """Intent: every pod of one namespace may reach the brokers. Wrong: both selectors in one entry (too narrow, only one app);
    control: the namespaceSelector alone (documented to select all pods of the matching namespaces)."""
    cluster, kns, cns, app, other = "ledger-kafka", "accounting", "treasury", "fx-rates", "audit"
    nsl = lambda n: {"kubernetes.io/metadata.name": n}  # noqa: E731

    def pol(frm):
        return {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                "metadata": {"name": "allow-%s-to-%s" % (cns, cluster), "namespace": kns},
                "spec": {"podSelector": {"matchLabels": {"strimzi.io/cluster": cluster}}, "policyTypes": ["Ingress"],
                         "ingress": [{"ports": [{"protocol": "TCP", "port": 9092}], "from": frm}]}}
    tgt = {"namespace": kns, "pod_labels": {"strimzi.io/cluster": cluster}}
    allns = {n: nsl(n) for n in (kns, cns, other)}
    mk = lambda src, exp: {"type": "semantic_rule", "oracle": "netpol_allows", "rule": "V1", "port": 9092,  # noqa: E731
                           "source": src, "target": tgt, "namespaces": allns, "expect": exp}
    oc = [mk({"namespace": cns, "pod_labels": {"app": app}}, True), mk({"namespace": cns, "pod_labels": {"app": "other-" + app}}, True),
          mk({"namespace": other, "pod_labels": {"app": app}}, False), mk({"namespace": kns, "pod_labels": {"app": app}}, False)]
    nsel = {"namespaceSelector": {"matchLabels": nsl(cns)}}
    both = {**nsel, "podSelector": {"matchLabels": {"app": app}}}
    ctx = ("The brokers of the Kafka cluster `%s` run in namespace `%s`, where ingress is denied unless a NetworkPolicy allows it. "
           "Every pod of namespace `%s`, whatever its labels, must reach the brokers on TCP 9092; nothing from other namespaces. "
           "Does this NetworkPolicy do that?") % (cluster, kns, cns)
    fl = lambda p: [(["metadata", "name"], p["metadata"]["name"]), (["metadata", "namespace"], kns),  # noqa: E731
                    (["spec", "podSelector"], p["spec"]["podSelector"]), (["spec", "policyTypes"], ["Ingress"]),
                    (["spec", "ingress", 0, "ports"], p["spec"]["ingress"][0]["ports"])]
    srcs = anchors_of("V1", table)
    out = []
    for kind, shown, fixd, why in (
            ("and_narrow", pol([both]), pol([nsel]),
             "The entry has a namespaceSelector and a podSelector together, which selects only the pods labelled `app: %s` of `%s`; "
             "the namespaceSelector alone selects all pods of the namespace." % (app, cns)),
            ("ns_all", pol([nsel]), None,
             "A namespaceSelector alone selects all pods of the matching namespace `%s`, and nothing from other namespaces." % cns)):
        out.append(draft("V1", "control" if fixd is None else "wrong", "%s:%s" % (kind, cluster), ctx, "NetworkPolicy",
                         dump(shown), why, fix=dump(fixd) if fixd else None,
                         struct=yaml_struct("NetworkPolicy", "networking.k8s.io/v1", fl(shown)), oracle=oc, sources=srcs))
    return out


def r3_v2(table):
    """Intent: restart one task. Wrong: `strimzi.io/restart: "true"` (restarts the connector instance); control: restart-task."""
    name, cluster, ns = "billing-sink", "billing-connect", "finance"
    cls = "org.apache.kafka.connect.file.FileStreamSinkConnector"
    cfg = {"file": "/data/billing.txt", "topics": "billing"}
    api = "kafka.strimzi.io/v1"

    def conn(ann):
        return {"apiVersion": api, "kind": "KafkaConnector",
                "metadata": {"name": name, "namespace": ns, "labels": {"strimzi.io/cluster": cluster}, "annotations": ann},
                "spec": {"class": cls, "tasksMax": 3, "config": cfg}}
    fl = [(["metadata", "name"], name), (["spec", "class"], cls), (["spec", "config"], cfg)]
    srcs = anchors_of("V2", table) + [line_source("restart_task", "V2 restart-task"), line_source("restart_task_id", "V2 restart-task")]
    out = []
    for tid, bad in ((2, True), (1, False)):
        shown = conn({"strimzi.io/restart": "true"} if bad else {"strimzi.io/restart-task": str(tid)})
        fixd = conn({"strimzi.io/restart-task": str(tid)}) if bad else None
        intro = ("The `KafkaConnector` below runs on the Connect cluster `%s`. Task %d of it is stuck and I want Strimzi to restart "
                 "that task only, not the connector instance, so I apply this manifest. Does it do that?") % (cluster, tid)
        why = ("`strimzi.io/restart: \"true\"` restarts the connector instance; a single task is restarted with `strimzi.io/restart-task` "
               "and its task id.") if bad else "`strimzi.io/restart-task: \"%d\"` restarts the task with id %d only." % (tid, tid)
        oc = [{"type": "semantic_rule", "oracle": "restart_annotation", "rule": "V2", "kind": "KafkaConnector",
               "expect": {"restart_connector": False, "task_id": tid}}]
        out.append(draft("V2", "wrong" if bad else "control", "restart_task:%s-task%d" % (name, tid), intro, "KafkaConnector", dump(shown), why,
                         fix=dump(fixd) if fixd else None, struct=yaml_struct("KafkaConnector", api, fl), oracle=oc, sources=srcs,
                         rules=MECH_RULES["V2"] + ["restart.task_id"]))
    return out


def r3_v6():
    """A wrong item with five replicas (the number was never published); the control `inventory:4` of version 3 stays."""
    return {"pool": [("telemetry", 5)], "ctl": [False]}


def r3_v7(table):
    """Percentages round up (configure-pdb.md l.97-102): minAvailable "90%" of 5 pods is 5, so no eviction is allowed;
    maxUnavailable "25%" of 4 pods is 1."""
    out = []
    for name, rep, spec, bad in (("search-kafka", 5, {"minAvailable": "90%"}, True), ("stream-kafka", 4, {"maxUnavailable": "25%"}, False)):
        def pdb(s):
            return {"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
                    "metadata": {"name": "%s-pdb" % name, "namespace": "kafka"},
                    "spec": {**s, "selector": {"matchLabels": {"app": name}}}}
        shown = pdb(spec)
        fixd = pdb({"maxUnavailable": 1}) if bad else None
        intro = ("The StatefulSet `%s` runs %d broker pods. Node drains must be able to evict one broker pod at a time. "
                 "Does this PodDisruptionBudget allow that?") % (name, rep)
        why = ("A percentage of `minAvailable` is rounded up: 90 %% of %d pods is %d, so all pods must stay available and a node drain "
               "cannot evict a broker pod." % (rep, rep)) if bad else \
            "A percentage of `maxUnavailable` is rounded up: 25 %% of %d pods is 1, so one voluntary eviction at a time is allowed." % rep
        oc = [{"type": "semantic_rule", "oracle": "pdb_evicts", "rule": "V7", "replicas": rep, "expect": True, "allowed": 1}]
        fl = [(["metadata", "name"], "%s-pdb" % name), (["spec", "selector"], {"matchLabels": {"app": name}})]
        out.append(draft("V7", "wrong" if bad else "control", "%s:%s" % ("minpct" if bad else "maxpct", name), intro,
                         "PodDisruptionBudget", dump(shown), why, fix=dump(fixd) if fixd else None,
                         struct=yaml_struct("PodDisruptionBudget", "policy/v1", fl), oracle=oc,
                         sources=anchors_of("V7", table) + [line_source("pdb_round_min", "V7 percentage rounds up"),
                                                            line_source("pdb_round_max", "V7 percentage rounds up")],
                         rules=MECH_RULES["V7"] + ["pdb.percent_rounds_up"]))
    return out


BUILDERS = {"V1": b_v1, "V2": b_v2, "V3": b_v3, "V4": b_v4, "V5": b_v5, "V6": b_v6, "V7": b_v7, "V8": b_v8}


def mechanism_status(directory=None):
    """{mech: (used, reason)} for V1..V8: used only when the table says anchored true and valid_but_wrong not false and
    every rule the mechanism applies is anchored in the scorer module."""
    table = catalogue_a3.semantic_rules(directory)
    out = {}
    for m in sorted(BUILDERS):
        e = table["mechanisms"].get(m)
        if e is None:
            out[m] = (False, "not in the table")
        elif e.get("anchored") is not True:
            out[m] = (False, "unanchored in the table")
        elif e.get("valid_but_wrong") is False:
            out[m] = (False, "valid_but_wrong is false: " + str(e.get("item_kind", "")))
        elif any(not c3.rule_anchored(r) for r in MECH_RULES[m]):
            out[m] = (False, "a rule it applies is unanchored in eval2_checks_a3.RULES")
        else:
            out[m] = (True, "anchored")
    return out


# ------------------------------------------------------------------ assembly and the per-item assertions
def parse_ok(d, cats):
    """The shown artifact is valid: parses; a manifest passes the schema, a properties block names known keys."""
    if d["lang"] == "properties":
        props = c3.parse_properties(d["shown"])
        for k in props:
            assert cats.config_key(k, "consumer").status == KNOWN, "unknown consumer key %s" % k
        return "properties block parses; keys are known consumer settings"
    docs = list(yaml.safe_load_all(d["shown"]))
    assert len(docs) == 1 and isinstance(docs[0], dict), "not one mapping"
    r = cats.validate_resource(docs[0])
    assert r.covered and r.ok, "shown %s does not validate: %s" % (docs[0].get("kind"), [(v.code, v.path) for v in r.violations][:3])
    return "validates against the %s schema" % r.source


def build_item(d, n, cats, chash, chash_a3, ver="v2"):
    tpl = TEMPLATES[d["mech"]]
    wrong = d["role"] == "wrong"
    lang = d["lang"]
    q = d["intro"] + "\n\n" + fence(d["shown"], lang) + "\n\n" + FMT % d["noun"]
    fmt_chk = {"format": True, "type": "answer_line"}
    verdict = {"type": "answer_equals", "accepted": ["does not work" if wrong else "works"], "norm": "ci"}
    checks = [fmt_chk, verdict] + (d["struct"] + d["oracle"] if wrong else [])
    item_id = "a3-%s-%s-%03d" % (tpl, ver, n)
    item = {"id": item_id, "tier": "A", "kind": "valid_but_wrong", "family": "%s:%s" % (tpl, d["group"]),
            "domain": DOMAIN[d["mech"]], "question": q, "checks": checks,
            "sources": d["sources"], "versions": VERSIONS.get(d["mech"], STRIMZI_V), "template": tpl, "anchored": True,
            "failure_class": "valid_but_wrong" if wrong else "valid_control", "exposed": False, "overlap": None,
            "overlap_band": "none", "catalogue_hash": chash, "catalogue_hash_a3": chash_a3, "seed": SEED,
            "generator": VERSION}
    shown_t = fence(d["shown"], lang)
    if wrong:
        ref = "%s\n\n%s\nANSWER: does not work" % (d["why"], fence(d["fix"], lang))
        pert = "The artifact does what is wanted.\n\n%s\nANSWER: works" % shown_t
    else:
        ref = "%s\nANSWER: works" % d["why"]
        pert = "%s\nANSWER: does not work" % d["why"]
    item["reference"], item["perturbed_reference"], item["perturbed_violates"] = ref, pert, 1
    # ---- the halves, asserted
    valid_note = parse_ok(d, cats)
    oracle_item = {"id": item_id, "checks": d["oracle"]}
    shown_res = sc.run_checks(oracle_item, shown_t, cats) if d["oracle"] else []
    if wrong:
        assert any(not r["pass"] for r in shown_res), "%s: the shown artifact passes its oracle" % item_id
        fixed_res = sc.run_checks(oracle_item, fence(d["fix"], lang), cats)
        assert all(r["pass"] for r in fixed_res), "%s: the corrected artifact fails: %s" % (item_id, fixed_res)
        r2 = sc.run_checks(item, shown_t + "\nANSWER: does not work", cats)
        assert any(not r["pass"] for r, c in zip(r2, checks) if c["type"] in ORACLE_TYPES), "%s: perturbed 2 passes the oracle" % item_id
    else:
        assert shown_res and all(r["pass"] for r in shown_res), "%s: the control fails its oracle: %s" % (item_id, shown_res)
    full = checks if wrong else checks + d["oracle"]
    ref_res = sc.run_checks({"id": item_id, "checks": checks}, ref, cats)
    assert all(r["pass"] for r in ref_res), "%s: reference fails %s" % (item_id, [r for r in ref_res if not r["pass"]])
    pres = sc.run_checks({"id": item_id, "checks": checks}, pert, cats)
    assert pres[0]["pass"] and not pres[1]["pass"], "%s: perturbed answer is not a verdict failure" % item_id
    assert full
    item["vbw"] = {"mechanism": d["mech"], "role": d["role"], "rules": d["rules"], "shown_valid": True,
                   "shown_valid_note": valid_note, "shown_fails_oracle": wrong,
                   "oracle_checks": [i for i, c in enumerate(checks) if c["type"] in ORACLE_TYPES],
                   "control_artifact": d["fix"] if wrong else d["shown"], "artifact_language": lang,
                   "oracle_checks_on_control": d["oracle"]}
    item.update(d["extra"])
    item["dedup"] = None
    return item


def assign_pairs(m, drafts):
    """`pair` (version 2): an item and its control share a value, the split keeps a pair on one side. V1..V8: the
    first wrong item with the control, the other two wrong items together. FK: a control with the first wrong item of
    its resource kind, every other wrong item alone."""
    if m == "FK":
        first = {}
        for d in drafts:
            if d["role"] == "wrong":
                first.setdefault(d["group"].split(":")[0], d)
        used = set()
        for d in drafts:
            if d["role"] == "wrong":
                d["extra"]["pair"] = "vbw:FK:%s" % d["group"]
        for d in drafts:
            if d["role"] == "control":
                kind = d["group"].split(":")[0]
                cand = [w for w in drafts if w["role"] == "wrong" and w["group"].split(":")[0] == kind
                        and w["group"] not in used]
                tgt = cand[0] if cand else first[kind]
                used.add(tgt["group"])
                d["extra"]["pair"] = tgt["extra"]["pair"]
        return
    wrong = [d for d in drafts if d["role"] == "wrong"]
    for d in drafts:
        d["extra"]["pair"] = "vbw:%s:%s" % (m, "a" if d["role"] == "control" or d is wrong[0] else "b")


def generate(catalogues_dir=None, catalogues_a3_dir=None, dedup=True, seed=SEED):
    """The items, in a fixed order; no file is written. Raises AssertionError when an item breaks an invariant."""
    cats = Catalogues.load(catalogues_dir)
    chash = catalogue_hash(catalogues_dir)
    a3d = catalogues_a3_dir
    chash_a3 = catalogue_a3.manifest(a3d)["catalogue_hash"]
    table = catalogue_a3.semantic_rules(a3d)
    status = mechanism_status(a3d)
    drafts = {}
    for m in sorted(BUILDERS):
        if status[m][0]:
            drafts[m] = BUILDERS[m](random.Random("%s|%s|%s" % (seed, chash, TEMPLATES[m])), cats, table)
    quota = max(4, TARGET_TOTAL - 4 * len(drafts))
    drafts["FK"] = b_fk(random.Random("%s|%s|%s" % (seed, chash, TEMPLATES["FK"])), cats, table, quota)
    items, counter = [], Counter()
    corpus = None
    if dedup:
        import eval2_items_a1 as a1
        corpus, _ = a1.corpus_texts()
        for line in (ROOT / "eval" / "m4" / "open" / "tier_a.jsonl").read_text().splitlines():
            if line.strip():
                corpus.append(("eval/m4/open/tier_a.jsonl", a1.tokens(json.loads(line)["question"])))
    for m, ds in drafts.items():
        assign_pairs(m, ds)
    generate.dropped = []
    for m in ["V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "FK"]:
        for d in drafts.get(m, []):
            counter[m] += 1
            if (m, d["group"]) in SKIP:
                continue
            it = build_item(d, counter[m], cats, chash, chash_a3)
            if dedup:
                hit, dd = a1.near_duplicate(it["question"], corpus)
                it["dedup"] = dd
                if hit:
                    generate.dropped.append((it["id"], dd))
                    continue
            items.append(it)
    for m in ["V1", "V2", "V3", "V5", "V6", "V7"]:
        if not status[m][0]:
            continue
        ds = BUILDERS[m](random.Random("%s|%s|%s|fresh" % (seed, chash, TEMPLATES[m])), cats, table, fresh=FRESH[m])
        for k, d in enumerate(ds, 1):
            if (m, k) in SKIP_FRESH:
                continue
            d["extra"]["pair"] = FRESH_PAIR[m]
            it = build_item(d, k, cats, chash, chash_a3, ver="v3")
            if dedup:
                hit, dd = a1.near_duplicate(it["question"], corpus)
                it["dedup"] = dd
                if hit:
                    generate.dropped.append((it["id"], dd))
                    continue
            items.append(it)
    r3 = {"V1": r3_v1(table), "V2": r3_v2(table), "V6": BUILDERS["V6"](random.Random("r3"), cats, table, fresh=r3_v6()),
          "V7": r3_v7(table)}
    for m in ["V1", "V2", "V6", "V7"]:
        if not status[m][0]:
            continue
        for k, d in enumerate(r3[m], 101):
            d["extra"]["pair"] = FRESH_PAIR[m]
            it = build_item(d, k, cats, chash, chash_a3, ver="v3")
            if dedup:
                hit, dd = a1.near_duplicate(it["question"], corpus)
                it["dedup"] = dd
                if hit:
                    generate.dropped.append((it["id"], dd))
                    continue
            items.append(it)
    return items


generate.dropped = []


def counts(items):
    c = Counter(i["template"] for i in items)
    return {t: c[t] for t in TEMPLATE_ORDER}


def digest(items):
    return hashlib.sha256("".join(json.dumps(i, sort_keys=True, ensure_ascii=False) + "\n" for i in items).encode()).hexdigest()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", help="write the items here (a new path; nothing is written without it)")
    args = ap.parse_args(argv)
    items = generate()
    st = mechanism_status()
    print("rule status:")
    for m in sorted(st):
        print("  %s %-5s %s" % (m, "used" if st[m][0] else "SKIP", st[m][1][:110]))
    roles = Counter((i["template"], i["vbw"]["role"]) for i in items)
    print("items per rule:")
    for t in TEMPLATE_ORDER:
        print("  %-26s %2d  (wrong %d, control %d)" % (t, counts(items)[t], roles[(t, "wrong")], roles[(t, "control")]))
    print("total %d (plan: 32)   dropped as near-duplicates: %d" % (len(items), len(generate.dropped)))
    best = max((max(i["dedup"]["max_jaccard"], i["dedup"]["max_containment"]), i["id"], i["dedup"]["nearest"]) for i in items)
    print("highest similarity to eval/*.jsonl, tier_a.jsonl, data and work/v3 questions: %.3f (%s, nearest %s)" % best)
    print("digest", digest(items))
    if args.out:
        Path(args.out).write_text("".join(json.dumps(i, sort_keys=True, ensure_ascii=False) + "\n" for i in items))
        print("wrote", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
