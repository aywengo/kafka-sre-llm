#!/usr/bin/env python3
"""Draft builders for the four new mechanical kinds of tier A (M4, slice A2). No model is involved.

Used by scripts/eval2_items_a1.py, which keeps the caps, the checks of the references and the output file.
Every function returns a list of drafts (dicts, the shape of eval2_items_a1.mk) for Gen.fill.

Kinds (truth comes from the catalogues only):
  write_resource  a specification assembled from the schema of a resource kind (field names, types,
                  enumerations; the values of free-form config maps are checked against the Kafka
                  configuration catalogue for the right scope). Checks: one YAML document of the kind and
                  apiVersion, strict schema validation, every field has the value asked for, the fields the
                  question excludes are absent, config keys exist for the scope.
  fix_manifest    a valid generated manifest with exactly one injected fault of a recorded class (near-miss
                  field name, value outside an enumeration, field at the wrong nesting level, apiVersion not
                  served for the kind, config key of the wrong scope). The corrected document must validate,
                  the fault must be gone and every other value unchanged.
  write_command   a Kafka command-line task assembled from the options of a tool whose option list is
                  complete. Every flag, every `available only with` and `cannot go with` rule is read from the
                  catalogue (build time: a task whose derivation fails is left out).
  version_trap    keys whose default, valid values or existence changed between Kafka 4.1 and 4.3: the answer
                  is the 4.3 value, the 4.1 value is forbidden.
"""
import copy
import re

import yaml

import eval2_score as sc
from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN

NAMES = ["orders-events", "payments", "inventory-updates", "clickstream", "audit-log", "user-signups", "shipping",
         "notifications", "metrics-raw", "billing-events", "search-index", "fraud-alerts"]
CLUSTERS = ["prod-cluster", "my-cluster", "data-platform", "events-kafka"]
NAMESPACES = ["kafka", "streaming", "prod", "data"]
ROUND = [1, 2, 3, 4, 5, 8, 10, 16, 20, 50, 100, 500, 1000, 3000, 5000, 10000, 30000, 60000, 300000, 600000,
         3600000, 86400000, 604800000]
MS_VALUES = [1000, 5000, 10000, 30000, 60000, 300000, 600000, 3600000, 86400000, 604800000]
BYTES_VALUES = [1048576, 5242880, 10485760, 104857600, 536870912, 1073741824]
# Strimzi does not accept these in `spec.kafka.config`; the prefixes are left out so that no generated
# manifest depends on a rule the catalogues do not hold.
BROKER_SKIP = ("listeners", "advertised.", "broker.", "listener.", "host.name", "port", "inter.broker.", "sasl.",
               "ssl.", "security.", "password.", "log.dir", "zookeeper.", "authorizer.", "super.user", "cruise.control.",
               "node.id", "process.roles", "controller.", "metadata.log.dir", "early.start.", "config.providers",
               "principal.builder", "create.topic.policy", "alter.config.policy", "metric.reporters", "remote.log.",
               "kafka.metrics", "cordoned.", "initial.broker.registration")
VERSION_TRAP_NOTE = "kafka_config_history"


def mk(kind, template, group, domain, fc, core, fmt, checks, refs, perturbed, violates, sources, versions,
       anchored, desc=None, **extra):
    d = {"kind": kind, "template": template, "group": group, "domain": domain, "failure_class": fc, "core": core,
         "fmt": fmt, "checks": checks, "refs": refs, "perturbed": perturbed, "violates": violates,
         "sources": sources, "versions": versions, "anchored": anchored, "desc": desc}
    d.update(extra)
    return d


def dump(doc):
    return yaml.safe_dump(doc, sort_keys=False, default_flow_style=False).rstrip("\n")


def block(doc):
    return "```yaml\n%s\n```" % dump(doc)


def sp(path):
    """Schema path of a document path: list indexes become []."""
    out = ""
    for s in path:
        if isinstance(s, int):
            out += "[]"
        else:
            out += ("." if out else "") + str(s)
    return out


def pstr(path):
    return sc.path_str(path)


# ------------------------------------------------------------------ schema helpers
class Schema:
    def __init__(self, cats, kind):
        self.cats = cats
        self.kind = kind
        self.e = cats.schema_entry(kind)
        assert self.e, kind
        self.api = self.e["api_version"]
        self.paths = self.e["paths"]
        self.catalogue = self.e["_catalogue"]
        self.kids = {}
        for q in self.paths:
            if q.endswith("[]"):
                continue
            parent, _, leaf = q.rpartition(".")
            self.kids.setdefault(parent, []).append(leaf)

    def info(self, path):
        return self.paths.get(sp(path), {})

    def enum(self, path):
        return self.info(path).get("enum")

    def children(self, path):
        return sorted(self.kids.get(sp(path), []))

    def types(self, path):
        return self.info(path).get("type", ["any"])

    def open_at(self, path):
        """Is there a free-form map (or preserved unknown fields) at the path or above it?"""
        segs = [s for s in path if not isinstance(s, int)]
        for i in range(len(path), 0, -1):
            if self.paths.get(sp(path[:i]), {}).get("open"):
                return True
        return False

    def preserves(self, path):
        if not path:
            return bool(self.e.get("preserve_unknown_root"))
        return bool(self.info(path).get("open"))

    def src(self, path, fact=None):
        path = list(path)
        while path and sp(path) not in self.paths:
            path = path[:-1]
        info = self.info(path)
        bits = ["type " + "/".join(info.get("type", ["any"]))]
        if info.get("enum"):
            bits.append("one of " + ", ".join(info["enum"]))
        if info.get("required"):
            bits.append("required")
        return {"catalogue": self.catalogue, "entry": "%s %s" % (self.kind, sp(path) or "(root)"),
                "file": self.e["source_file"], "line": None, "fact": fact or "; ".join(bits)}


def cfg_source(e, fact):
    return {"catalogue": "kafka_config", "entry": "%s/%s" % (e["scope"], e["key"]), "file": e["source_file"],
            "line": e["source_line"], "fact": fact}


# ------------------------------------------------------------------ values of config entries
BOUND = re.compile(r"^\[(-?\d+),\.\.\.(?:,(-?\d+))?\]")


def pick_value(rng, e):
    """A valid value different from the default for a configuration entry, from its type and its stated
    range or list of valid values, or None when the entry does not give enough to choose one."""
    t, dv = e.get("type"), e.get("default_value")
    vl = e.get("valid_values_list")
    if vl:
        ch = [v for v in vl if v != dv and re.fullmatch(r"[\w.\-]+", v) and len(v) <= 24]
        return rng.choice(sorted(ch)) if ch else None
    if t == "boolean":
        return {"true": False, "false": True}.get(dv)
    if t in ("int", "long", "short"):
        vv = e.get("valid_values") or ""
        m = BOUND.match(vv)
        if vv and not m:
            return None
        lo = int(m.group(1)) if m else 0
        hi = int(m.group(2)) if m and m.group(2) else (32767 if t == "short" else 2 ** 31 - 1 if t == "int" else 2 ** 40)
        k = e["key"]
        pool = MS_VALUES if k.endswith(".ms") else BYTES_VALUES if k.endswith(".bytes") else ROUND
        cand = [v for v in pool if lo <= v <= hi and str(v) != dv]
        return rng.choice(cand) if cand else None
    return None


def config_pool(cats, scope):
    out = []
    for e in cats.data["kafka_config"]["entries"]:
        if e["scope"] != scope or e.get("type") not in ("int", "long", "short", "boolean", "string"):
            continue
        k = e["key"]
        if scope == "broker" and k.startswith(BROKER_SKIP):
            continue
        if scope in ("sink_connector", "source_connector") and (k.startswith("tasks.") or k in ("name", "connector.class")):
            continue
        if "deprecated" in (e.get("description") or "").lower()[:80]:
            continue
        out.append(e)
    return sorted(out, key=lambda e: e["key"])


# ------------------------------------------------------------------ a resource under construction
class Res:
    def __init__(self, cats, kind, name, namespace=None):
        self.cats = cats
        self.s = Schema(cats, kind)
        self.kind = kind
        self.api = self.s.api
        self.doc = {"apiVersion": self.api, "kind": kind, "metadata": {"name": name}}
        self.leaves = []     # dicts: path, value, phrase, loose, as_set, spec
        self.absent = []     # dicts: path, phrase
        self.cfg = []        # (path, scope, [entries])
        self.sources = []
        self.name = name
        self.namespace = namespace
        self.meta_phrases = ["named `%s`" % name]
        self.leaves.append({"path": ["metadata", "name"], "value": name, "loose": False, "as_set": False, "spec": False})
        if namespace:
            self.doc["metadata"]["namespace"] = namespace
            self.meta_phrases.append("in the namespace `%s`" % namespace)
            self.leaves.append({"path": ["metadata", "namespace"], "value": namespace, "loose": False, "as_set": False,
                                "spec": False})

    def label(self, key, value):
        self.doc["metadata"].setdefault("labels", {})[key] = value
        self.meta_phrases.append("labelled `%s: %s`" % (key, value))
        self.leaves.append({"path": ["metadata", "labels", key], "value": value, "loose": False, "as_set": False,
                            "spec": False})
        a = self.cats.annotation(key)
        if a.known:
            e = a.entries[0]
            self.sources.append({"catalogue": "strimzi_annotations", "entry": key, "file": e["source_file"], "line": None,
                                 "fact": "the label name is in the Strimzi documentation"})

    def set(self, path, value, phrase, loose=False, as_set=False):
        cur = self.doc
        for i, seg in enumerate(path[:-1]):
            nxt = path[i + 1]
            if isinstance(seg, int):
                while len(cur) <= seg:
                    cur.append({} if not isinstance(nxt, int) else [])
                cur = cur[seg]
            else:
                if seg not in cur:
                    cur[seg] = [] if isinstance(nxt, int) else {}
                cur = cur[seg]
        last = path[-1]
        if isinstance(last, int):
            while len(cur) <= last:
                cur.append(None)
            cur[last] = value
        else:
            cur[last] = value
        self.leaves.append({"path": list(path), "value": value, "loose": loose, "as_set": as_set, "spec": True,
                            "phrase": phrase})
        self.sources.append(self.s.src(path))

    def config(self, rng, path, scope, n, phrase_fmt="`%s` = %s", exclude_one=True, force=()):
        """n config entries of the scope, chosen from the catalogue with valid values; one more key is excluded."""
        pool = config_pool(self.cats, scope)
        forced = [e for e in pool if e["key"] in force]
        rest = [e for e in pool if e["key"] not in force]
        rng.shuffle(rest)
        got = list(forced)
        tried = 0
        for e in rest:
            if len(got) >= n:
                break
            tried += 1
            if pick_value(rng, e) is not None:
                got.append(e)
        picked = []
        for e in got:
            v = pick_value(rng, e)
            if v is None:
                continue
            picked.append((e, v))
        if not picked:
            return None
        for e, v in picked:
            self.leaves.append({"path": list(path) + [e["key"]], "value": v, "loose": True, "as_set": False,
                                "spec": True, "phrase": "`%s` set to %s" % (e["key"], str(v).lower() if isinstance(v, bool) else v),
                                "config": True})
            self.sources.append(cfg_source(e, "key exists in scope %s; type %s%s" % (
                scope, e["type"], "; valid values " + e["valid_values"] if e.get("valid_values") else "")))
        cur = self.doc
        for seg in path:
            cur = cur.setdefault(seg, {})
        for e, v in picked:
            cur[e["key"]] = v
        self.sources.append(self.s.src(path, "free-form map (the schema keeps unknown keys); keys are checked "
                                             "against the %s configuration catalogue" % scope))
        self.cfg.append((list(path), scope, [e for e, _ in picked]))
        if exclude_one:
            spare = [e for e in rest if e["key"] not in {x["key"] for x, _ in picked}]
            if spare:
                x = spare[rng.randrange(len(spare))]
                self.absent.append({"path": list(path) + [x["key"]], "phrase": "`%s` (leave it at its default)" % x["key"],
                                    "src": cfg_source(x, "key exists in scope %s; the item excludes it" % scope)})
        return picked

    def exclude(self, path, what):
        info = self.s.paths.get(sp(path))
        if not info and self.s.open_at(path):
            src = self.s.src(path, "free-form map: the item excludes the key `%s`" % path[-1])
            self.absent.append({"path": list(path), "phrase": what, "src": src})
            return
        src = self.s.src(path) if info else {"catalogue": self.s.catalogue, "entry": "%s %s" % (self.kind, sp(path)),
                                              "file": self.s.e["source_file"], "line": None,
                                              "absent": sp(path), "fact": "not a field of the schema"}
        self.absent.append({"path": list(path), "phrase": what, "src": src})

    # ------------------------------------------------ outputs
    def spec_leaf_count(self):
        return sum(1 for l in self.leaves if l["spec"])

    def groups(self):
        out = set()
        for l in self.leaves:
            if l["spec"]:
                p = l["path"]
                out.add(str(p[1]) if len(p) > 1 and p[0] == "spec" else str(p[0]))
        return sorted(out)

    def preserves(self):
        seen, out = set(), []
        for l in self.leaves:
            if not l["spec"]:
                continue
            parent = [s for s in l["path"][:-1]]
            key = sp(parent)
            if key in seen:
                continue
            seen.add(key)
            out.append({"path": key or "(root)", "preserves_unknown_fields": self.s.preserves(parent)})
        return out

    def note(self):
        opens = [c for c in self.cfg]
        txt = "The schema is strict: a field it does not define is an error."
        if opens:
            p, scope, _ = opens[0]
            txt += " `%s` is a free-form map (the schema keeps unknown keys there), but its keys must be real Kafka %s settings." % (
                sp(p), {"topic": "topic-level", "broker": "broker", "connect": "Connect worker",
                        "sink_connector": "Connect sink connector", "source_connector": "Connect source connector"}[scope])
        return txt


def doc_leaves(doc, prefix=()):
    """(path, value) for every scalar and every list of scalars in a document."""
    if isinstance(doc, dict):
        for k, v in doc.items():
            yield from doc_leaves(v, prefix + (k,))
    elif isinstance(doc, list) and doc and all(not isinstance(x, (dict, list)) for x in doc):
        yield list(prefix), doc
    elif isinstance(doc, list):
        for i, v in enumerate(doc):
            yield from doc_leaves(v, prefix + (i,))
    else:
        yield list(prefix), doc


# ------------------------------------------------------------------ the builders of valid resources
def b_topic(g, rng, cats, force=()):
    r = Res(cats, "KafkaTopic", rng.choice(NAMES), rng.choice(NAMESPACES))
    r.label("strimzi.io/cluster", rng.choice(CLUSTERS))
    p, rep = rng.choice([3, 6, 12, 24, 48]), rng.choice([1, 2, 3])
    r.set(["spec", "partitions"], p, "%d partitions" % p)
    r.set(["spec", "replicas"], rep, "%d replica%s" % (rep, "" if rep == 1 else "s"))
    if not r.config(rng, ["spec", "config"], "topic", rng.choice([2, 3]), force=force):
        return None
    r.exclude(["spec", "topicName"], "`spec.topicName` (the resource name is the topic name)")
    return r


def b_user(g, rng, cats, force=()):
    r = Res(cats, "KafkaUser", rng.choice(NAMES) + "-app", rng.choice(NAMESPACES))
    r.label("strimzi.io/cluster", rng.choice(CLUSTERS))
    at = rng.choice(r.s.enum(["spec", "authentication", "type"]))
    r.set(["spec", "authentication", "type"], at, "`%s` authentication" % at)
    qs = [("consumerByteRate", [1048576, 5242880, 10485760]), ("producerByteRate", [1048576, 5242880, 10485760]),
          ("requestPercentage", [10, 25, 50])]
    rng.shuffle(qs)
    for name, vals in qs[:rng.choice([1, 2])]:
        assert "spec.quotas." + name in r.s.paths
        v = rng.choice(vals)
        r.set(["spec", "quotas", name], v, "quota `%s` of %d" % (name, v))
    r.set(["spec", "authorization", "type"], "simple", "`simple` authorization")
    rt = rng.choice([t for t in r.s.enum(["spec", "authorization", "acls", 0, "resource", "type"]) if t in ("topic", "group")])
    pt = rng.choice(r.s.enum(["spec", "authorization", "acls", 0, "resource", "patternType"]))
    ops = rng.sample(r.s.enum(["spec", "authorization", "acls", 0, "operations", 0]), rng.choice([1, 2]))
    ops = [o for o in ops if o != "All"] or ["Read"]
    rn = rng.choice(NAMES)
    r.set(["spec", "authorization", "acls", 0, "resource", "type"], rt, "an ACL rule on a %s" % rt)
    r.set(["spec", "authorization", "acls", 0, "resource", "name"], rn, "resource name `%s`" % rn)
    r.set(["spec", "authorization", "acls", 0, "resource", "patternType"], pt, "pattern type `%s`" % pt)
    r.set(["spec", "authorization", "acls", 0, "operations"], ops, "operations %s" % " and ".join("`%s`" % o for o in ops),
          as_set=True)
    r.exclude(["spec", "authorization", "acls", 0, "host"], "`host` in the ACL rule (no host restriction)")
    return r


SINKS = ["org.apache.kafka.connect.file.FileStreamSinkConnector", "com.example.orders.OrdersSinkConnector",
         "io.example.warehouse.WarehouseSinkConnector"]
SOURCES = ["org.apache.kafka.connect.file.FileStreamSourceConnector", "com.example.cdc.OrdersSourceConnector"]


def b_connector(g, rng, cats, force=()):
    r = Res(cats, "KafkaConnector", rng.choice(NAMES) + "-connector", rng.choice(NAMESPACES))
    r.label("strimzi.io/cluster", rng.choice(["my-connect", "data-connect"]))
    sink = rng.random() < 0.6
    cls = rng.choice(SINKS if sink else SOURCES)
    scope = "sink_connector" if sink else "source_connector"
    tm = rng.choice([1, 2, 4, 8])
    st = rng.choice(r.s.enum(["spec", "state"]))
    r.set(["spec", "class"], cls, "connector class `%s`" % cls)
    r.set(["spec", "tasksMax"], tm, "`tasksMax` %d" % tm)
    r.set(["spec", "state"], st, "state `%s`" % st)
    if not r.config(rng, ["spec", "config"], scope, rng.choice([1, 2]),
                    force=[k for k in force if k]):
        return None
    r.exclude(["spec", "autoRestart"], "`spec.autoRestart` (no automatic restarts)")
    return r


def b_nodepool(g, rng, cats, force=()):
    r = Res(cats, "KafkaNodePool", rng.choice(["dual-role", "brokers", "controllers", "pool-a", "pool-b"]),
            rng.choice(NAMESPACES))
    r.label("strimzi.io/cluster", rng.choice(CLUSTERS))
    roles = rng.choice([["broker"], ["controller"], ["broker", "controller"]])
    n = rng.choice([1, 3, 5])
    r.set(["spec", "roles"], roles, "roles %s" % " and ".join("`%s`" % x for x in roles), as_set=True)
    r.set(["spec", "replicas"], n, "%d replicas" % n)
    r.set(["spec", "storage", "type"], "persistent-claim", "`persistent-claim` storage")
    size = rng.choice(["100Gi", "250Gi", "500Gi", "1Ti"])
    r.set(["spec", "storage", "size"], size, "a volume of `%s`" % size)
    dc = rng.choice([True, False])
    r.set(["spec", "storage", "deleteClaim"], dc, "`deleteClaim: %s`" % str(dc).lower())
    if rng.random() < 0.6:
        h = rng.choice(["1g", "2g", "4g"])
        r.set(["spec", "jvmOptions", "-Xmx"], h, "`-Xmx` `%s`" % h)
    r.exclude(["spec", "storage", "class"], "`spec.storage.class` (use the cluster's default storage class)")
    r.exclude(["spec", "storage", "volumes"], "`spec.storage.volumes` (a single volume, not JBOD)")
    return r


def b_rebalance(g, rng, cats, force=()):
    r = Res(cats, "KafkaRebalance", rng.choice(["rebalance", "scale-up", "scale-down", "full-rebalance"]), rng.choice(NAMESPACES))
    r.label("strimzi.io/cluster", rng.choice(CLUSTERS))
    mode = rng.choice(r.s.enum(["spec", "mode"]))
    r.set(["spec", "mode"], mode, "mode `%s`" % mode)
    if mode in ("add-brokers", "remove-brokers"):
        bs = sorted(rng.sample([3, 4, 5, 6, 7], rng.choice([1, 2])))
        r.set(["spec", "brokers"], bs, "brokers %s" % ", ".join(str(b) for b in bs), as_set=True)
    opts = [("concurrentPartitionMovementsPerBroker", [2, 5, 10], "`concurrentPartitionMovementsPerBroker` %d"),
            ("concurrentLeaderMovements", [500, 1000, 2000], "`concurrentLeaderMovements` %d"),
            ("replicationThrottle", [10485760, 52428800], "`replicationThrottle` %d"),
            ("skipHardGoalCheck", [True, False], "`skipHardGoalCheck: %s`")]
    rng.shuffle(opts)
    for name, vals, ph in opts[:rng.choice([3, 4])]:
        v = rng.choice(vals)
        r.set(["spec", name], v, ph % (str(v).lower() if isinstance(v, bool) else v))
    r.exclude(["spec", "goals"], "`spec.goals` (use the default goals)")
    return r


def b_argo(g, rng, cats, force=()):
    r = Res(cats, "Application", rng.choice(NAMES) + "-app", "argocd")
    proj = rng.choice(["default", "team-a", "platform"])
    r.set(["spec", "project"], proj, "project `%s`" % proj)
    repo = rng.choice(["https://git.example.com/org/%s.git" % rng.choice(["kafka-config", "platform", "apps"])])
    r.set(["spec", "source", "repoURL"], repo, "source repository `%s`" % repo)
    rev = rng.choice(["main", "release-1.4", "v2.1.0"])
    r.set(["spec", "source", "targetRevision"], rev, "target revision `%s`" % rev)
    path = rng.choice(["k8s", "deploy/prod", "manifests"])
    r.set(["spec", "source", "path"], path, "path `%s`" % path)
    ns = rng.choice(["prod", "kafka", "staging"])
    r.set(["spec", "destination", "server"], "https://kubernetes.default.svc", "destination cluster `https://kubernetes.default.svc`")
    r.set(["spec", "destination", "namespace"], ns, "destination namespace `%s`" % ns)
    if rng.random() < 0.65:
        prune, heal = rng.choice([True, False]), rng.choice([True, False])
        r.set(["spec", "syncPolicy", "automated", "prune"], prune, "automated sync with `prune: %s`" % str(prune).lower())
        r.set(["spec", "syncPolicy", "automated", "selfHeal"], heal, "`selfHeal: %s`" % str(heal).lower())
        r.exclude(["spec", "source", "helm"], "`spec.source.helm` (plain manifests, no Helm)")
    else:
        r.set(["spec", "syncPolicy", "syncOptions"], ["CreateNamespace=true"], "the sync option `CreateNamespace=true`", as_set=True)
        r.exclude(["spec", "syncPolicy", "automated"], "`spec.syncPolicy.automated` (sync manually)")
    return r


def b_netpol(g, rng, cats, force=()):
    r = Res(cats, "NetworkPolicy", rng.choice(["allow-clients", "kafka-ingress", "allow-from-apps", "restrict-brokers"]),
            rng.choice(NAMESPACES))
    app = rng.choice(["kafka", "kafka-connect", "schema-registry"])
    src = rng.choice(["orders-api", "payments", "web-frontend", "billing"])
    port = rng.choice([9092, 9093, 8083, 8081])
    r.set(["spec", "podSelector", "matchLabels", "app"], app, "pods labelled `app: %s`" % app)
    r.set(["spec", "policyTypes"], ["Ingress"], "policy type `Ingress` only", as_set=True)
    r.set(["spec", "ingress", 0, "from", 0, "podSelector", "matchLabels", "app"], src, "ingress allowed from pods labelled `app: %s`" % src)
    r.set(["spec", "ingress", 0, "ports", 0, "port"], port, "TCP port %d" % port)
    r.set(["spec", "ingress", 0, "ports", 0, "protocol"], "TCP", "protocol `TCP`")
    r.exclude(["spec", "egress"], "`spec.egress` (egress traffic is not restricted)")
    return r


def b_pdb(g, rng, cats, force=()):
    r = Res(cats, "PodDisruptionBudget", rng.choice(["kafka-pdb", "connect-pdb", "broker-budget", "zk-budget"]),
            rng.choice(NAMESPACES))
    app = rng.choice(["kafka", "kafka-connect", "schema-registry"])
    comp = rng.choice(["broker", "worker", "controller"])
    r.set(["spec", "selector", "matchLabels", "app"], app, "pods labelled `app: %s`" % app)
    r.set(["spec", "selector", "matchLabels", "component"], comp, "also the label `component: %s`" % comp)
    if rng.random() < 0.5:
        v = rng.choice([1, 2])
        r.set(["spec", "maxUnavailable"], v, "`maxUnavailable` %d" % v)
        r.exclude(["spec", "minAvailable"], "`spec.minAvailable` (the two budget fields are mutually exclusive)")
    else:
        v = rng.choice([2, 3, 5])
        r.set(["spec", "minAvailable"], v, "`minAvailable` %d" % v)
        r.exclude(["spec", "maxUnavailable"], "`spec.maxUnavailable` (the two budget fields are mutually exclusive)")
    pol = rng.choice(["AlwaysAllow", "IfHealthyBudget"])
    r.set(["spec", "unhealthyPodEvictionPolicy"], pol, "`unhealthyPodEvictionPolicy: %s`" % pol)
    return r


def b_deploy(g, rng, cats, force=()):
    r = Res(cats, "Deployment", rng.choice(["web", "orders-api", "payments", "billing", "search"]), rng.choice(NAMESPACES))
    app = r.name
    reps = rng.choice([2, 3, 5])
    img = "registry.example.com/%s:%s" % (app, rng.choice(["1.4.2", "2.0.1", "3.1.0"]))
    r.set(["spec", "replicas"], reps, "%d replicas" % reps)
    r.set(["spec", "selector", "matchLabels", "app"], app, "the selector label `app: %s`" % app)
    r.set(["spec", "template", "metadata", "labels", "app"], app, "the same label on the pods")
    cont = rng.choice([app, "app"])
    r.set(["spec", "template", "spec", "containers", 0, "name"], cont, "one container `%s`" % cont)
    r.set(["spec", "template", "spec", "containers", 0, "image"], img, "image `%s`" % img)
    cpu, mem, lim = rng.choice(["250m", "500m", "1"]), rng.choice(["256Mi", "512Mi", "1Gi"]), rng.choice(["512Mi", "1Gi", "2Gi"])
    base = ["spec", "template", "spec", "containers", 0, "resources"]
    r.set(base + ["requests", "cpu"], cpu, "requests of `%s` CPU" % cpu)
    r.set(base + ["requests", "memory"], mem, "requests of `%s` memory" % mem)
    r.set(base + ["limits", "memory"], lim, "a memory limit of `%s`" % lim)
    r.exclude(base + ["limits", "cpu"], "a CPU limit (`resources.limits.cpu`)")
    return r


def b_kafka(g, rng, cats, force=()):
    r = Res(cats, "Kafka", rng.choice(CLUSTERS), rng.choice(NAMESPACES))
    lt = rng.choice(r.s.enum(["spec", "kafka", "listeners", 0, "type"]))
    port = rng.choice([9092, 9093, 9094])
    tls = rng.choice([True, False])
    lname = rng.choice(["plain", "tls", "external"])
    r.set(["spec", "kafka", "listeners", 0, "name"], lname, "one listener named `%s`" % lname)
    r.set(["spec", "kafka", "listeners", 0, "port"], port, "on port %d" % port)
    r.set(["spec", "kafka", "listeners", 0, "type"], lt, "of type `%s`" % lt)
    r.set(["spec", "kafka", "listeners", 0, "tls"], tls, "`tls: %s`" % str(tls).lower())
    if not r.config(rng, ["spec", "kafka", "config"], "broker", rng.choice([2, 3]), force=force):
        return None
    r.exclude(["spec", "zookeeper"], "`spec.zookeeper` (the cluster uses KRaft with node pools)")
    return r


BUILDERS = [("topic", b_topic, "KafkaTopic"), ("user", b_user, "KafkaUser"), ("connector", b_connector, "KafkaConnector"),
            ("nodepool", b_nodepool, "KafkaNodePool"), ("rebalance", b_rebalance, "KafkaRebalance"),
            ("argo", b_argo, "Application"), ("netpol", b_netpol, "NetworkPolicy"), ("pdb", b_pdb, "PodDisruptionBudget"),
            ("deploy", b_deploy, "Deployment"), ("kafka", b_kafka, "Kafka")]
DOMAIN = {"KafkaTopic": "strimzi", "KafkaUser": "strimzi", "KafkaConnector": "strimzi", "KafkaNodePool": "strimzi",
          "KafkaRebalance": "strimzi", "Kafka": "strimzi", "Application": "argocd", "NetworkPolicy": "kubernetes",
          "PodDisruptionBudget": "kubernetes", "Deployment": "kubernetes"}


# ------------------------------------------------------------------ write a resource
def resource_checks(r, doc_kind_api=True):
    fields = [{"path": l["path"], "value": l["value"], **({"loose": True} if l["loose"] else {}),
               **({"as_set": True} if l["as_set"] else {})} for l in r.leaves]
    checks = [{"type": "yaml_document", "format": True},
              {"type": "yaml_one_doc", "kind": r.kind, "api_version": r.api},
              {"type": "yaml_strict", "kind": r.kind},
              {"type": "yaml_fields_equal", "kind": r.kind, "fields": fields}]
    if r.absent:
        checks.append({"type": "yaml_fields_absent", "kind": r.kind, "paths": [a["path"] for a in r.absent]})
    for path, scope, _ in r.cfg:
        checks.append({"type": "yaml_config_keys", "kind": r.kind, "path": path, "scope": scope})
    return checks


def join_phrases(ps):
    ps = [p for p in ps if p]
    return ps[0] if len(ps) == 1 else ", ".join(ps[:-1]) + " and " + ps[-1]


def write_core(r, variant):
    parts = join_phrases([l["phrase"] for l in r.leaves if l.get("phrase")])
    meta = " ".join(r.meta_phrases)
    excl = join_phrases([a["phrase"] for a in r.absent])
    kind_txt = {"Application": "Argo CD `Application`", "Deployment": "Kubernetes `Deployment`",
                "NetworkPolicy": "Kubernetes `NetworkPolicy`", "PodDisruptionBudget": "Kubernetes `PodDisruptionBudget`"}.get(
        r.kind, "Strimzi `%s`" % r.kind)
    art = "an" if kind_txt[0] in "AEIOU" else "a"
    if variant == "a":
        return "Write %s %s resource (apiVersion `%s`) %s, with %s. Do not set %s. %s Give exactly one YAML document." % (
            art, kind_txt, r.api, meta, parts, excl, r.note())
    return ("I need the manifest of %s %s %s, apiVersion `%s`: %s. Leave out %s. %s Reply with exactly one YAML document." % (
        art, kind_txt, meta, r.api, parts, excl, r.note()))


def skeleton(r):
    return block({"apiVersion": r.api, "kind": r.kind, "metadata": {"name": r.name}})


def perturb_write(r):
    """The reference with one specified value changed (the first non-metadata leaf): fails the field check."""
    d = copy.deepcopy(r.doc)
    for l in r.leaves:
        if l["spec"] and not l["as_set"] and not l.get("config"):
            cur = d
            for seg in l["path"][:-1]:
                cur = cur[seg]
            v = l["value"]
            cur[l["path"][-1]] = (not v) if isinstance(v, bool) else (v + 1) if isinstance(v, int) else v + "x"
            return block(d)
    return None


def drafts_write(g, rng, cats, which, variant, n_try=14):
    """Drafts for one resource kind; the item count per template is capped by Gen.fill."""
    name, fn, kind = [b for b in BUILDERS if b[0] == which][0]
    out, seen = [], set()
    for _ in range(n_try):
        r = fn(g, rng, cats)
        if r is None or not 4 <= r.spec_leaf_count() <= 8:
            continue
        core = write_core(r, variant)
        if core in seen:
            continue
        seen.add(core)
        pert = perturb_write(r)
        if pert is None:
            continue
        srcs = []
        seenk = set()
        for s in r.sources + [a["src"] for a in r.absent]:
            k = (s["catalogue"], s["entry"], s["fact"])
            if k not in seenk:
                seenk.add(k)
                srcs.append(s)
        checks = resource_checks(r)
        out.append(mk("write_resource", "wr_%s_%s" % (which, variant), "%s:%s" % (kind, "+".join(r.groups())),
                      DOMAIN[kind], "negation", core,
                      "Put the manifest in one fenced code block.", checks, [block(r.doc)], pert, 3, srcs,
                      {"kafka": "4.3", "strimzi": "1.3.1"} if DOMAIN[kind] == "strimzi" else
                      {"argo-cd": "v3.6.1"} if kind == "Application" else {"kubernetes": "v1.37.2"},
                      True, None, resource_kind=kind, preserves_unknown=r.preserves(), baseline=skeleton(r)))
    return out


# ------------------------------------------------------------------ fix the manifest
def swap_adjacent(k):
    for i in range(1, len(k) - 2):
        if k[i] != k[i + 1] and k[i].isalpha() and k[i + 1].isalpha():
            return k[:i] + k[i + 1] + k[i] + k[i + 2:]
    return None


def near_names(k):
    out = []
    if len(k) >= 5 and k.endswith("s") and not k.endswith("ss"):
        out.append(k[:-1])
    elif k.isalpha():
        out.append(k + "s")
    if k != k.lower() and k.isalpha():
        out.append(k.lower())
    if k[0].islower() and k.isalpha():
        out.append(k[0].upper() + k[1:])
    sw = swap_adjacent(k)
    if sw:
        out.append(sw)
    if "-" in k:
        out.append(k.replace("-", "_"))
    return [x for x in out if x != k]


def parent_dict(doc, path):
    cur = doc
    for seg in path[:-1]:
        cur = cur[seg]
    return cur


def schema_keys_ok(res, path):
    """True when `path` (a list) is a path inside the schema that is not below a free-form map."""
    return sp(path) in res.s.paths and not res.s.open_at(path[:-1])


def fault_near_miss(rng, r):
    cands = []
    for path, val in doc_leaves(r.doc):
        if path[0] in ("metadata", "apiVersion", "kind") or not isinstance(path[-1], str):
            continue
        if not schema_keys_ok(r, path) or r.s.open_at(path):
            continue
        parent = path[:-1]
        sib = set(r.s.children(parent))
        for fake in near_names(path[-1]):
            if fake in sib or fake in parent_dict(r.doc, path):
                continue
            cands.append((path, fake, val))
    if not cands:
        return None
    path, fake, val = cands[rng.randrange(len(cands))]
    d = copy.deepcopy(r.doc)
    par = parent_dict(d, path)
    d_new = {}
    for k, v in par.items():
        d_new[fake if k == path[-1] else k] = v
    par.clear()
    par.update(d_new)
    new_path = path[:-1] + [fake]
    return d, {"class": "near_miss_name", "faulty_path": new_path, "original_path": path,
               "detail": "`%s` for `%s`" % (fake, path[-1])}


def fault_enum(rng, r):
    cands = []
    for path, val in doc_leaves(r.doc):
        if path[0] in ("metadata", "apiVersion", "kind"):
            continue
        if isinstance(val, str):
            en = r.s.enum(path)
            if en and val in en:
                for fake in (val.upper(), val.capitalize(), val.replace("-", "_"), val + "s", val[:-1], val.replace("-", " ")):
                    if fake != val and fake not in en and len(fake) > 2:
                        cands.append((path, None, fake))
                        break
        elif isinstance(val, list) and val and isinstance(val[0], str):
            en = r.s.enum(path + [0])
            if en and all(x in en for x in val):
                i = rng.randrange(len(val))
                for fake in (val[i].upper(), val[i].capitalize(), val[i] + "s", val[i][:-1]):
                    if fake not in en and fake != val[i] and len(fake) > 2:
                        cands.append((path, i, fake))
                        break
    if not cands:
        return None
    path, i, fake = cands[rng.randrange(len(cands))]
    d = copy.deepcopy(r.doc)
    par = parent_dict(d, path)
    if i is None:
        old = par[path[-1]]
        par[path[-1]] = fake
    else:
        old = par[path[-1]][i]
        par[path[-1]][i] = fake
    return d, {"class": "value_type_trap", "faulty_path": path + ([i] if i is not None else []), "original_path": path,
               "detail": "`%s` for `%s` at `%s`" % (fake, old, pstr(path))}


def fault_nest(rng, r):
    cands = []
    for path, val in doc_leaves(r.doc):
        if path[0] in ("metadata", "apiVersion", "kind") or isinstance(val, list) or not isinstance(path[-1], str):
            continue
        if len(path) < 2 or r.s.open_at(path[:-1]) or sp(path) not in r.s.paths:
            continue
        k, par = path[-1], path[:-1]
        # one level up (next to its parent); a path that ends in an index step is not moved
        upper = par[:-1]
        if isinstance(par[-1], str) and (not upper or isinstance(upper[-1], str) or isinstance(upper[-1], int)):
            tgt = upper + [k]
            if sp(tgt) not in r.s.paths and not r.s.open_at(upper) and k not in parent_dict(r.doc, tgt) \
                    and (len(upper) > 0 or k not in ("apiVersion", "kind", "metadata")):
                cands.append((path, tgt, "up"))
        # one level down, into a sibling object of the parent
        pd = parent_dict(r.doc, path)
        for sib, sv in pd.items():
            if isinstance(sv, dict) and sib != k and not r.s.open_at(par + [sib]):
                tgt = par + [sib, k]
                if sp(tgt) not in r.s.paths and k not in sv:
                    cands.append((path, tgt, "down"))
    if not cands:
        return None
    path, tgt, how = cands[rng.randrange(len(cands))]
    d = copy.deepcopy(r.doc)
    par = parent_dict(d, path)
    v = par.pop(path[-1])
    cur = d
    for seg in tgt[:-1]:
        cur = cur[seg]
    cur[tgt[-1]] = v
    return d, {"class": "wrong_nesting", "faulty_path": tgt, "original_path": path,
               "detail": "`%s` moved %s, to `%s`" % (pstr(path), how, pstr(tgt))}


def fault_api(rng, r):
    group, _, ver = r.api.rpartition("/")
    cands = ["%s/%s" % (group, v) if group else v for v in ("v1beta2", "v1beta1", "v1alpha1", "v2")]
    if group:
        cands.append("v1")
    cands = [c for c in cands if c != r.api]
    ok = []
    for c in cands:
        d = copy.deepcopy(r.doc)
        d["apiVersion"] = c
        v = r.cats.validate_resource(d)
        if [x.code for x in v.violations] == ["unknown_api_version"]:
            ok.append(c)
    if not ok:
        return None
    c = ok[rng.randrange(len(ok))]
    d = copy.deepcopy(r.doc)
    d["apiVersion"] = c
    return d, {"class": "wrong_api_version", "faulty_path": ["apiVersion"], "original_path": ["apiVersion"],
               "detail": "apiVersion `%s` for %s" % (c, r.kind)}


def scope_pairs(cats):
    """(topic key, broker key): the broker form of a topic-level setting is `log.` + the key."""
    top = {e["key"]: e for e in cats.data["kafka_config"]["entries"] if e["scope"] == "topic"}
    brk = {e["key"]: e for e in cats.data["kafka_config"]["entries"] if e["scope"] == "broker"}
    return sorted((t, "log." + t) for t in top if "log." + t in brk and t not in brk and "log." + t not in top)


def fault_scope(rng, r, pairs):
    if not r.cfg:
        return None
    path, scope, ents = r.cfg[0]
    keys = {e["key"] for e in ents}
    if scope == "topic":
        m = {t: b for t, b in pairs}
    elif scope == "broker":
        m = {b: t for t, b in pairs}
    else:
        return None
    hit = [k for k in sorted(keys) if k in m]
    if not hit:
        return None
    k = hit[rng.randrange(len(hit))]
    d = copy.deepcopy(r.doc)
    cfgd = d
    for seg in path:
        cfgd = cfgd[seg]
    nd = {}
    for kk, v in cfgd.items():
        nd[m[k] if kk == k else kk] = v
    cfgd.clear()
    cfgd.update(nd)
    return d, {"class": "wrong_scope", "faulty_path": path + [m[k]], "original_path": path + [k],
               "detail": "the %s key `%s` where the %s key `%s` belongs" % (
                   "broker" if scope == "topic" else "topic-level", m[k], scope, k)}


FAULTS = {"near": ("near_miss_name", fault_near_miss), "enum": ("value_type_trap", fault_enum),
          "nest": ("wrong_nesting", fault_nest), "api": ("wrong_api_version", fault_api),
          "scope": ("wrong_scope", fault_scope)}


def fix_core(r, faulty, variant):
    ask = {"a": "`kubectl apply` (or the Argo CD / Strimzi validation) rejects the manifest below. It has exactly one "
                "mistake. Return the corrected manifest in full as one YAML document and change nothing that is "
                "not wrong.\n\n%s",
           "b": "A reviewer says this manifest has exactly one error. Fix it and give the complete corrected manifest "
                "(one YAML document); every other value must stay as it is.\n\n%s",
           "c": "This manifest fails validation against the schema of its kind, and the cause is a single mistake. "
                "Return the whole manifest with that mistake corrected and everything else unchanged (one YAML "
                "document).\n\n%s"}[variant]
    return ask % block(faulty)


def fix_checks(r, fault):
    cfg_paths = [c[0] for c in r.cfg]
    fields = []
    for p, v in doc_leaves(r.doc):
        f = {"path": p, "value": v}
        if any(p[:len(c)] == c and len(p) > len(c) for c in cfg_paths):
            f["loose"] = True
        fields.append(f)
    checks = [{"type": "yaml_document", "format": True},
              {"type": "yaml_one_doc", "kind": r.kind, "api_version": r.api},
              {"type": "yaml_strict", "kind": r.kind},
              {"type": "yaml_fields_equal", "kind": r.kind, "fields": fields}]
    if fault["class"] in ("near_miss_name", "wrong_nesting", "wrong_scope"):
        checks.append({"type": "yaml_fields_absent", "kind": r.kind, "paths": [fault["faulty_path"]]})
    for path, scope, _ in r.cfg:
        checks.append({"type": "yaml_config_keys", "kind": r.kind, "path": path, "scope": scope})
    return checks


def drafts_fix(g, rng, cats, fault_key, variant, pairs, n_try=40):
    fc, fn = FAULTS[fault_key]
    out, seen = [], set()
    kinds = [b for b in BUILDERS]
    for i in range(n_try):
        name, bfn, kind = kinds[i % len(kinds)]
        if fault_key == "scope" and name not in ("topic", "kafka"):
            continue
        if fault_key == "scope":
            force = [rng.choice([t for t, _ in pairs])] if name == "topic" else [rng.choice([b for _, b in pairs])]
            r = bfn(g, rng, cats, force=force)
        else:
            r = bfn(g, rng, cats)
        if r is None:
            continue
        res = fn(rng, r, pairs) if fault_key == "scope" else fn(rng, r)
        if res is None:
            continue
        faulty, fault = res
        v = cats.validate_resource(faulty)
        if not v.covered or (v.ok and fault["class"] != "wrong_scope"):
            continue
        core = fix_core(r, faulty, variant)
        if core in seen:
            continue
        seen.add(core)
        checks = fix_checks(r, fault)
        if fault["class"] == "wrong_api_version":
            fsrc = {"catalogue": r.s.catalogue, "entry": "%s apiVersion" % r.kind, "file": r.s.e["source_file"],
                    "line": None, "fact": "the schema serves %s only" % r.api}
        else:
            fsrc = r.s.src(fault["original_path"])
        srcs, seenk = [], set()
        for s in r.sources + [fsrc]:
            k = (s["catalogue"], s["entry"], s["fact"])
            if k not in seenk:
                seenk.add(k)
                srcs.append(s)
        # the check the faulty manifest must fail: the strict validation, or for a wrong-scope key the key check
        viol = [i for i, c in enumerate(checks) if c["type"] == ("yaml_config_keys" if fault["class"] == "wrong_scope"
                                                                 else "yaml_strict")][0]
        out.append(mk("fix_manifest", "fm_%s_%s" % (fault_key, variant), "%s:%s" % (kind, "+".join(r.groups())),
                      DOMAIN[kind], fc, core, "Give the corrected manifest in one fenced code block.", checks,
                      [block(r.doc)], block(faulty), viol, srcs,
                      {"kafka": "4.3", "strimzi": "1.3.1"} if DOMAIN[kind] == "strimzi" else
                      {"argo-cd": "v3.6.1"} if kind == "Application" else {"kubernetes": "v1.37.2"},
                      True, None, resource_kind=kind, fault=fault, preserves_unknown=r.preserves(),
                      baseline=block(faulty),
                      validator_codes=v.codes()))
    return out


# ------------------------------------------------------------------ write the command
BS = "kafka:9092"


def S(tool, phrase, required, forbidden=(), derive=(), params=None, group=None):
    return {"tool": tool, "phrase": phrase, "required": required, "forbidden": list(forbidden), "derive": list(derive),
            "params": params or {}, "group": group or tool}


# {x} placeholders are filled from `params` (a list of dicts, tried in turn); a value of None means a bare flag
CMD_SPECS = [
    S("kafka-consumer-groups.sh", "describe the consumer group `{g}` with its members and its state",
      {"--describe": None, "--group": "{g}", "--members": None, "--state": None}, ["--delete", "--reset-offsets"],
      [("avail", "--members", ["--describe"]), ("avail", "--state", ["--describe", "--list"])],
      [{"g": "orders"}, {"g": "payments-consumers"}]),
    S("kafka-consumer-groups.sh", "show the committed offsets and lag of the consumer group `{g}`",
      {"--describe": None, "--group": "{g}", "--offsets": None}, ["--delete", "--execute"],
      [("avail", "--offsets", ["--describe"])], [{"g": "billing"}, {"g": "search-indexer"}]),
    S("kafka-consumer-groups.sh",
      "see what resetting the offsets of the group `{g}` on the topic `{t}` to the time `{d}` would do, without changing anything",
      {"--reset-offsets": None, "--group": "{g}", "--topic": "{t}", "--to-datetime": "{d}", "--dry-run": None}, ["--execute"],
      [("option", "--reset-offsets"), ("option", "--dry-run"), ("option", "--execute"), ("takes", "--to-datetime", "required")],
      [{"g": "orders", "t": "orders-events", "d": "2026-11-01T00:00:00.000"},
       {"g": "payments-consumers", "t": "payments", "d": "2026-10-15T12:30:00.000"}]),
    S("kafka-consumer-groups.sh", "reset the offsets of the group `{g}` on the topic `{t}` to the earliest offset and apply the change",
      {"--reset-offsets": None, "--group": "{g}", "--topic": "{t}", "--to-earliest": None, "--execute": None}, ["--dry-run"],
      [("option", "--reset-offsets"), ("option", "--to-earliest"), ("option", "--execute")],
      [{"g": "orders", "t": "orders-events"}, {"g": "billing", "t": "billing-events"}]),
    S("kafka-consumer-groups.sh", "move the offsets of the group `{g}` on the topic `{t}` back by {n} offsets, as a dry run",
      {"--reset-offsets": None, "--group": "{g}", "--topic": "{t}", "--shift-by": "-{n}", "--dry-run": None}, ["--execute"],
      [("takes", "--shift-by", "required"), ("option", "--dry-run")],
      [{"g": "orders", "t": "orders-events", "n": "100"}, {"g": "search-indexer", "t": "search-index", "n": "500"}]),
    S("kafka-consumer-groups.sh", "list the consumer groups that are in the state `{s}`",
      {"--list": None, "--state": "{s}"}, ["--describe", "--delete"],
      [("avail", "--state", ["--describe", "--list"])], [{"s": "Stable"}, {"s": "Empty"}]),
    S("kafka-topics.sh", "list the partitions of all topics that are under-replicated",
      {"--describe": None, "--under-replicated-partitions": None}, ["--create", "--delete", "--alter"],
      [("desc", "--under-replicated-partitions", "under-replicated"), ("option", "--describe")], [{}]),
    S("kafka-topics.sh", "list the partitions of the topic `{t}` whose in-sync replica count is below the configured minimum",
      {"--describe": None, "--topic": "{t}", "--under-min-isr-partitions": None}, ["--create", "--delete"],
      [("desc", "--under-min-isr-partitions", "isr count is less than")], [{"t": "orders-events"}, {"t": "payments"}]),
    S("kafka-topics.sh", "show only the partitions whose leader is not available",
      {"--describe": None, "--unavailable-partitions": None}, ["--delete"],
      [("desc", "--unavailable-partitions", "leader is not available")], [{}]),
    S("kafka-topics.sh", "create the topic `{t}` with {p} partitions and replication factor {r}",
      {"--create": None, "--topic": "{t}", "--partitions": "{p}", "--replication-factor": "{r}"}, ["--delete", "--alter"],
      [("desc", "--replication-factor", "being created"), ("desc", "--partitions", "created or altered")],
      [{"t": "orders-events", "p": "12", "r": "3"}, {"t": "audit-log", "p": "6", "r": "3"}, {"t": "clickstream", "p": "24", "r": "2"}]),
    S("kafka-topics.sh", "create the topic `{t}` with {p} partitions and the topic setting `retention.ms` set to {v}",
      {"--create": None, "--topic": "{t}", "--partitions": "{p}", "--config": "retention.ms={v}"}, ["--alter"],
      [("desc", "--config", "topic being created")],
      [{"t": "metrics-raw", "p": "6", "v": "86400000"}, {"t": "notifications", "p": "3", "v": "604800000"}]),
    S("kafka-topics.sh", "increase the partitions of the topic `{t}` to {p}",
      {"--alter": None, "--topic": "{t}", "--partitions": "{p}"}, ["--replication-factor", "--create"],
      [("desc", "--partitions", "created or altered"), ("invalid", "--replication-factor", [])], [{"t": "orders-events", "p": "24"}, {"t": "payments", "p": "12"}]),
    S("kafka-topics.sh", "delete the topic `{t}` only if it exists",
      {"--delete": None, "--topic": "{t}", "--if-exists": None}, ["--create"],
      [("desc", "--if-exists", "only execute if the topic exist")], [{"t": "tmp-load-test"}, {"t": "old-events"}]),
    S("kafka-configs.sh", "set the topic setting `retention.ms` of the topic `{t}` to {v}",
      {"--alter": None, "--entity-type": "topics", "--entity-name": "{t}", "--add-config": "retention.ms={v}"}, ["--describe"],
      [("option", "--add-config"), ("desc", "--entity-type", "topics")],
      [{"t": "orders-events", "v": "86400000"}, {"t": "audit-log", "v": "2592000000"}]),
    S("kafka-configs.sh", "show all configuration of the topic `{t}`, including the defaults",
      {"--describe": None, "--entity-type": "topics", "--entity-name": "{t}", "--all": None}, ["--alter"],
      [("desc", "--all", "including static configs")], [{"t": "orders-events"}, {"t": "payments"}]),
    S("kafka-configs.sh", "remove the override `retention.ms` from the topic `{t}`",
      {"--alter": None, "--entity-type": "topics", "--entity-name": "{t}", "--delete-config": "retention.ms"}, ["--add-config"],
      [("option", "--delete-config")], [{"t": "orders-events"}, {"t": "clickstream"}]),
    S("kafka-configs.sh", "set the dynamic broker setting `log.cleaner.threads` to {v} on broker {b}",
      {"--alter": None, "--entity-type": "brokers", "--entity-name": "{b}", "--add-config": "log.cleaner.threads={v}"}, ["--describe"],
      [("desc", "--entity-type", "brokers")], [{"b": "1", "v": "2"}, {"b": "0", "v": "4"}]),
    S("kafka-leader-election.sh", "trigger a preferred leader election for partition {p} of the topic `{t}` only",
      {"--election-type": "preferred", "--topic": "{t}", "--partition": "{p}"}, ["--all-topic-partitions"],
      [("desc", "--partition", "REQUIRED if --topic"), ("desc", "--election-type", "preferred")],
      [{"t": "orders-events", "p": "3"}, {"t": "payments", "p": "0"}]),
    S("kafka-leader-election.sh", "trigger a preferred leader election for every eligible partition of every topic",
      {"--election-type": "preferred", "--all-topic-partitions": None}, ["--topic", "--partition"],
      [("desc", "--topic", "Not allowed if"), ("desc", "--all-topic-partitions", "all of the eligible")], [{}]),
    S("kafka-leader-election.sh", "run an unclean leader election for partition {p} of the topic `{t}`",
      {"--election-type": "unclean", "--topic": "{t}", "--partition": "{p}"}, ["--all-topic-partitions"],
      [("desc", "--election-type", "unclean"), ("desc", "--partition", "REQUIRED if --topic")],
      [{"t": "orders-events", "p": "1"}, {"t": "billing-events", "p": "2"}]),
    S("kafka-get-offsets.sh", "get the latest offset of every partition of the topic `{t}`",
      {"--topic": "{t}", "--time": "latest"}, ["--partitions"],
      [("desc", "--time", "latest"), ("option", "--topic")], [{"t": "orders-events"}, {"t": "payments"}]),
    S("kafka-get-offsets.sh", "get the earliest offset of partitions {p} of the topic `{t}`",
      {"--topic": "{t}", "--partitions": "{p}", "--time": "earliest"}, [],
      [("desc", "--time", "earliest"), ("desc", "--partitions", "Comma separated list of partition ids")],
      [{"t": "orders-events", "p": "0,1"}, {"t": "audit-log", "p": "2,3"}]),
    S("kafka-log-dirs.sh", "describe the log directories of brokers {b} for the topic `{t}`",
      {"--describe": None, "--broker-list": "{b}", "--topic-list": "{t}"}, [],
      [("desc", "--broker-list", "form"), ("desc", "--topic-list", "queried")], [{"b": "1,2", "t": "orders-events"}, {"b": "0", "t": "payments"}]),
    S("kafka-reassign-partitions.sh", "verify the partition reassignment described in `{f}`",
      {"--verify": None, "--reassignment-json-file": "{f}"}, ["--execute", "--generate"],
      [("desc", "--verify", "Verify if the reassignment completed")], [{"f": "reassign.json"}, {"f": "move-orders.json"}]),
    S("kafka-reassign-partitions.sh", "execute the reassignment in `{f}` with the movement throttled to {v} bytes per second",
      {"--execute": None, "--reassignment-json-file": "{f}", "--throttle": "{v}"}, ["--generate", "--verify"],
      [("desc", "--throttle", "bytes/sec"), ("desc", "--execute", "Kick off")],
      [{"f": "reassign.json", "v": "50000000"}, {"f": "move-orders.json", "v": "100000000"}]),
    S("kafka-reassign-partitions.sh", "generate a candidate reassignment that moves the topics listed in `{f}` to brokers {b}",
      {"--generate": None, "--topics-to-move-json-file": "{f}", "--broker-list": "{b}"}, ["--execute"],
      [("desc", "--generate", "candidate"), ("desc", "--broker-list", "brokers to which")],
      [{"f": "topics.json", "b": "1,2,3"}, {"f": "move-topics.json", "b": "4,5,6"}]),
    S("kafka-delete-records.sh", "delete the records before the offsets listed in `{f}`",
      {"--offset-json-file": "{f}"}, [], [("desc", "--offset-json-file", "offset per partition")],
      [{"f": "offsets.json"}, {"f": "delete-before.json"}]),
    S("kafka-acls.sh", "give the user `{u}` the producer role on the topic `{t}`",
      {"--add": None, "--allow-principal": "User:{u}", "--producer": None, "--topic": "{t}"}, ["--operation", "--deny-principal"],
      [("invalid", "--producer", ["--operation", "--deny-principal", "--deny-host"]), ("desc", "--producer", "Convenience option")],
      [{"u": "alice", "t": "orders-events"}, {"u": "billing-app", "t": "billing-events"}]),
    S("kafka-acls.sh", "give the user `{u}` the consumer role on the topic `{t}` for the group `{g}`",
      {"--add": None, "--allow-principal": "User:{u}", "--consumer": None, "--topic": "{t}", "--group": "{g}"},
      ["--operation", "--deny-principal"],
      [("invalid", "--consumer", ["--operation", "--deny-principal", "--deny-host"]), ("desc", "--consumer", "Convenience option")],
      [{"u": "bob", "t": "orders-events", "g": "orders"}, {"u": "search-app", "t": "search-index", "g": "search-indexer"}]),
    S("kafka-acls.sh", "list the ACLs of the topic `{t}`",
      {"--list": None, "--topic": "{t}"}, ["--add", "--remove"],
      [("desc", "--list", "List ACLs"), ("option", "--topic")], [{"t": "orders-events"}, {"t": "payments"}]),
    S("kafka-console-consumer.sh", "read the first {n} messages of the topic `{t}` from the beginning and then exit",
      {"--topic": "{t}", "--from-beginning": None, "--max-messages": "{n}"}, ["--group"],
      [("option", "--from-beginning"), ("option", "--max-messages")], [{"t": "orders-events", "n": "10"}, {"t": "audit-log", "n": "5"}]),
    S("kafka-producer-perf-test.sh", "measure the producer throughput of the topic `{t}` with {n} records of {s} bytes and no throttling",
      {"--topic": "{t}", "--num-records": "{n}", "--record-size": "{s}", "--throughput": "-1"}, [],
      [("option", "--throughput"), ("option", "--record-size"), ("option", "--num-records")],
      [{"t": "perf-test", "n": "100000", "s": "1024"}, {"t": "perf-test", "n": "500000", "s": "512"}]),
    S("kafka-streams-groups.sh", "describe the streams group `{g}` with its members",
      {"--describe": None, "--group": "{g}", "--members": None}, ["--delete"],
      [("option", "--describe"), ("option", "--members")], [{"g": "orders-streams"}, {"g": "fraud-streams"}]),
]


def cmd_template_core(i, tool, phrase):
    t = i % 5
    cap = phrase[0].upper() + phrase[1:]
    return [
        "Write the command with `%s` to %s. Bootstrap server: `%s`." % (tool, phrase, BS),
        "On a broker host with Kafka 4.3 installed, which single `%s` command would %s? Bootstrap server: `%s`." % (tool, phrase, BS),
        "%s with the Kafka command-line tools. Give the command (bootstrap server `%s`)." % (cap, BS),
        "I need to %s. What exact command do I run? Kafka 4.3, bootstrap server `%s`." % (phrase, BS),
        "Give me the Kafka CLI invocation that will %s (broker address `%s`)." % (phrase, BS),
    ][t], t


def derive_check(cats, tool, d):
    """Verify one derivation against the catalogue; returns (ok, source dict or reason)."""
    ent = [e for e in cats.data["kafka_cli"]["entries"] if e["tool"] == tool][0]
    kind = d[0]
    o = ent["options"].get(d[1])
    if o is None:
        return False, "option %s not in %s" % (d[1], tool)
    base = {"catalogue": "kafka_cli", "entry": "%s %s" % (tool, d[1]), "file": o["files"][0], "line": None}
    if kind == "option":
        return True, dict(base, fact="option of the complete parser")
    if kind == "takes":
        return o.get("takes_argument") == d[2], dict(base, fact="takes an argument: %s" % o.get("takes_argument"))
    if kind == "desc":
        txt = ((o.get("description") or "") + " " + (o.get("argument_name") or "")).lower()
        return d[2].lower() in txt, dict(base, fact="description: %s" % (o.get("description") or "")[:110].replace("\n", " "))
    if kind == "avail":
        for c in cats.cli_constraints(tool):
            if c["kind"] == "availableIf" and c["option"] == d[1] and set(d[2]) <= set(c["others"]):
                line = c["source"].rsplit(":", 1)
                return True, {"catalogue": "kafka_cli", "entry": "%s %s" % (tool, d[1]), "file": line[0],
                              "line": int(line[1]) if line[-1].isdigit() else None,
                              "fact": "available only with %s" % ", ".join(c["others"])}
        return False, "no availableIf for %s" % d[1]
    if kind == "invalid":
        for c in cats.cli_constraints(tool):
            if c["kind"] == "checkInvalidArgs" and c["args"] and c["args"][0] == d[1] and set(d[2]) <= set(c["args"][1:]):
                line = c["source"].rsplit(":", 1)
                return True, {"catalogue": "kafka_cli", "entry": "%s %s" % (tool, d[1]), "file": line[0],
                              "line": int(line[1]) if line[-1].isdigit() else None,
                              "fact": "cannot go with %s" % ", ".join(c["args"][1:])}
        if d[2] == []:
            return True, dict(base, fact="option of the complete parser")
        return False, "no checkInvalidArgs for %s" % d[1]
    return False, "unknown derivation"


def cmd_line(tool, req, params, with_forbidden=None, drop=None):
    toks = ["bin/" + tool, "--bootstrap-server", BS]
    for f, v in req.items():
        if f == drop:
            continue
        toks.append(f)
        if v is not None:
            toks.append(v.format(**params))
    if with_forbidden:
        toks.append(with_forbidden)
    return " ".join(toks)


def drafts_cmd(g, rng, cats, stats):
    out = []
    n = 0
    for spec in CMD_SPECS:
        tool = spec["tool"]
        ent = [e for e in cats.data["kafka_cli"]["entries"] if e["tool"] == tool]
        if not ent or ent[0].get("complete") is not True:
            stats["tool_incomplete"] += 1
            continue
        srcs, ok = [], True
        flags = list(spec["required"]) + spec["forbidden"] + ["--bootstrap-server"]
        for f in flags:
            if cats.cli_option(tool, f).status != KNOWN:
                ok = False
                stats["flag_unknown:%s %s" % (tool, f)] += 1
        for d in spec["derive"]:
            good, src = derive_check(cats, tool, d)
            if not good:
                ok = False
                stats["derivation_failed:%s %s" % (tool, d[1])] += 1
            else:
                srcs.append(src)
        if not ok:
            continue
        # the allowed-combination rules of the catalogue hold for the reference
        for params in spec["params"]:
            req = {f: (v.format(**params) if v is not None else None) for f, v in spec["required"].items()}
            allf = ["--bootstrap-server"] + list(req)
            if sc.constraint_problems(tool, allf, cats):
                stats["constraint_violated"] += 1
                continue
            n += 1
            core, tmpl = cmd_template_core(n, tool, spec["phrase"].format(**params))
            reqall = dict(req)
            reqall["--bootstrap-server"] = BS
            checks = [{"type": "has_kafka_command", "format": True},
                      {"type": "kcmd_spec", "tool": tool, "required_flags": reqall, "forbidden_flags": spec["forbidden"]},
                      {"type": "kcmd_known_flags", "tool": tool},
                      {"type": "kcmd_constraints", "tool": tool}]
            line = cmd_line(tool, spec["required"], params)
            if spec["forbidden"]:
                pert = cmd_line(tool, spec["required"], params, with_forbidden=spec["forbidden"][0])
            else:
                last = [f for f in spec["required"]][-1]
                pert = cmd_line(tool, spec["required"], params, drop=last)
            fc = "negation" if spec["forbidden"] and any(
                f in ("--execute", "--dry-run", "--all-topic-partitions", "--topic", "--operation") for f in spec["forbidden"]) \
                else "multi_flag" if len(spec["required"]) >= 3 else "plain_lookup"
            sources = [dict(s) for s in srcs] or [{"catalogue": "kafka_cli", "entry": tool, "file": ent[0]["source_file"],
                                                   "line": None, "fact": "complete option list"}]
            sources.append({"catalogue": "kafka_cli", "entry": tool, "file": ent[0]["source_file"], "line": None,
                            "fact": "the tool's option parser is complete; every flag on the line is checked against it"})
            descs = [ent[0]["options"][d[1]].get("description") or "" for d in spec["derive"] if d[0] == "desc"]
            out.append(mk("write_command", "wc_%s" % "abcde"[tmpl], "%s:%s" % (tool.replace(".sh", ""), next(iter(spec["required"]))),
                          "kafka", fc, core,
                          "Give the command in a code block; use only options that exist for the tool.",
                          checks, ["```bash\n%s\n```" % line], "```bash\n%s\n```" % pert, 1, sources, {"kafka": "4.3.2"},
                          True, " ".join(descs) or None,
                          baseline="```bash\nbin/%s --bootstrap-server %s\n```" % (tool, BS)))
    return out


# ------------------------------------------------------------------ version traps (Kafka 4.1 against 4.3)
def norm_default(v):
    v = (v or "").strip()
    return "" if v in ("null", '""', "") else v


def hist_src(e, fact):
    return {"catalogue": "kafka_config_history", "entry": "%s/%s" % (e["scope"], e["key"]), "file": e["source_files"][0],
            "line": None, "fact": fact}


def drafts_vtrap(g, rng, cats):
    h = cats.data["kafka_config_history"]
    out = []
    cfg = {(x["scope"], x["key"]): x for x in cats.data["kafka_config"]["entries"]}
    SH = {"broker": "broker", "group": "group coordinator", "connect": "Connect worker", "streams": "Kafka Streams",
          "topic": "topic-level", "mirror_source": "MirrorMaker 2 source connector"}
    answer = "Finish with a final line `ANSWER: <value>`."
    for e in h["entries"]:
        v = e["versions"]
        v1, v3 = v.get("4.1"), v.get("4.3")
        sh = SH.get(e["scope"])
        if not sh:
            continue
        key = e["key"]
        if not (v1 and v3):
            continue
        # default changed
        if "default" in e["changes"] and v1.get("default_value") is not None and v3.get("default_value") is not None:
            old, new = norm_default(v1["default_value"]), norm_default(v3["default_value"])
            if old != new and old and new and len(new) <= 20 and len(old) <= 20 and new not in ("null",) and \
                    cats.config_key(key, e["scope"]).status == KNOWN:
                for var, core in (("a", "What is the default value of the %s setting `%s` in Kafka 4.3?" % (sh, key)),
                                  ("b", "Kafka 4.3: which value does `%s` (%s configuration) take when you do not set it?" % (key, sh))):
                    out.append(mk("version_trap", "vtrap_default_" + var, "%s:%s" % (e["scope"], key.split(".")[0]), "kafka",
                                  "version", core, answer + " Give the bare value.",
                                  [{"type": "answer_line", "format": True},
                                   {"type": "answer_equals", "accepted": [new], "norm": "ci"},
                                   {"type": "old_value_forbidden", "forbidden": [old], "norm": "ci"}],
                                  [ans2("The 4.3 default of `%s` is %s." % (key, new), new)],
                                  ans2("The default of `%s` is %s." % (key, old), old), 1,
                                  [hist_src(e, "default %s in 4.1, %s in 4.3" % (old, new))], {"kafka": "4.3", "compared_with": "4.1"},
                                  True, (cfg.get((e["scope"], key)) or {}).get("description"), chance=0.0, old_value=old))
        # valid range changed (lower bound)
        b1, b3 = BOUND.match(v1.get("valid_values") or ""), BOUND.match(v3.get("valid_values") or "")
        if "valid_values" in e["changes"] and b1 and b3 and b1.group(1) != b3.group(1) and \
                cats.config_key(key, e["scope"]).status == KNOWN:
            new, old = b3.group(1), b1.group(1)
            for var, core in (("a", "What is the smallest value the %s setting `%s` accepts in Kafka 4.3?" % (sh, key)),):
                out.append(mk("version_trap", "vtrap_range_" + var, "%s:%s" % (e["scope"], key.split(".")[0]), "kafka",
                              "version", core, answer, [{"type": "answer_line", "format": True},
                                                       {"type": "answer_equals", "accepted": [new], "norm": "ci"},
                                                       {"type": "old_value_forbidden", "forbidden": [old], "norm": "ci"}],
                              [ans2("In 4.3 the minimum of `%s` is %s." % (key, new), new)],
                              ans2("The minimum of `%s` is %s." % (key, old), old), 1,
                              [hist_src(e, "valid values %s in 4.1, %s in 4.3" % (v1["valid_values"], v3["valid_values"]))],
                              {"kafka": "4.3", "compared_with": "4.1"}, True, (cfg.get((e["scope"], key)) or {}).get("description"), chance=0.0, old_value=old))
        # list of valid values changed: the highest value
        l1 = re.match(r"^\[(.*)\]$", v1.get("valid_values") or "")
        l3 = re.match(r"^\[(.*)\]$", v3.get("valid_values") or "")
        if "valid_values" in e["changes"] and l1 and l3 and "," in l3.group(1) and ".." not in l3.group(1) and \
                cats.config_key(key, e["scope"]).status == KNOWN:
            a1 = [x.strip() for x in l1.group(1).split(",")]
            a3 = [x.strip() for x in l3.group(1).split(",")]
            if a1[-1] != a3[-1]:
                core = "Which is the highest version string the %s setting `%s` accepts in Kafka 4.3?" % (sh, key)
                out.append(mk("version_trap", "vtrap_values_a", "%s:%s" % (e["scope"], key.split(".")[0]), "kafka", "version",
                              core, answer, [{"type": "answer_line", "format": True},
                                            {"type": "answer_equals", "accepted": [a3[-1]], "norm": "ci"},
                                            {"type": "old_value_forbidden", "forbidden": [a1[-1]], "norm": "ci"}],
                              [ans2("The last accepted value in 4.3 is %s." % a3[-1], a3[-1])],
                              ans2("The last accepted value is %s." % a1[-1], a1[-1]), 1,
                              [hist_src(e, "valid values end with %s in 4.1, %s in 4.3" % (a1[-1], a3[-1]))],
                              {"kafka": "4.3", "compared_with": "4.1"}, True, (cfg.get((e["scope"], key)) or {}).get("description"), chance=0.0, old_value=a1[-1]))
    # settings added in 4.2 or 4.3: from which release do they exist
    added = [e for e in h["entries"] if any(c.startswith("added_in_") for c in e["changes"]) and e["scope"] in SH
             and cats.config_key(e["key"], e["scope"]).status == KNOWN]
    for e in added:
        first = [c for c in e["changes"] if c.startswith("added_in_")][0].split("_")[-1]
        sh, key = SH[e["scope"]], e["key"]
        core = ("In which Kafka release does the %s setting `%s` first appear in the configuration reference? Choose one of: "
                "4.1 (it is already there), 4.2, 4.3." % (sh, key))
        wrong = "4.1"
        out.append(mk("version_trap", "vtrap_added_a", "%s:%s" % (e["scope"], key.split(".")[0]), "kafka", "version", core,
                      answer, [{"type": "answer_line", "format": True},
                               {"type": "answer_equals", "accepted": [first], "norm": "ci"},
                               {"type": "old_value_forbidden", "forbidden": [wrong], "norm": "ci"}],
                      [ans2("`%s` first appears in %s." % (key, first), first)],
                      ans2("`%s` is already in 4.1." % key, wrong), 1,
                      [hist_src(e, "absent from the 4.1 reference, present from %s" % first)],
                      {"kafka": "4.3", "compared_with": "4.1"}, True, (cfg.get((e["scope"], key)) or {}).get("description"), chance=1 / 3.0, old_value=wrong,
                      bucket=first))
    return out


def ans2(expl, value):
    return "%s\nANSWER: %s" % (expl, value)
