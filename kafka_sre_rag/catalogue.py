"""Lookups against the catalogues of real identifiers built by scripts/build_catalogues.py.

Every lookup returns a `Lookup` whose `status` is one of three distinct values:

  KNOWN        the catalogue lists the item (see `evidence` for how strong that is)
  UNKNOWN      the catalogue is complete for this question and the item is not in it
  NOT_COVERED  no catalogue can answer: it is not loaded, the kind/tool/scope is not in it, the
               catalogue (or the tool or resource kind) is not marked `complete`, or the path is
               below a free-form field

Only UNKNOWN may be reported as "invented". NOT_COVERED must never be. A catalogue or tool that
does not say `complete: true` is treated as incomplete, so a missing flag can only make a
lookup more cautious.

Every entry has `provenance`: "exists" (schema, source code or generated reference) and/or
"documented" (seen in documentation prose or an example). Every catalogue has `complete`; in
kafka_cli, argocd and kubernetes each tool or resource kind has its own flag.

Completeness, by catalogue:
  kafka_config         complete per scope (generated reference). A key that is absent from a scope
                       is UNKNOWN there; pass `scope` whenever the context is known, because
                       connector-specific keys and prefixed forms are NOT_COVERED, not UNKNOWN.
  strimzi_resources    complete for the ten Strimzi kinds: the schema of the 1.3.1 custom resource
                       definitions (source "crd", provenance exists, served versions only) next to the
                       entries of the documentation reference (source "api_reference", documented);
                       below free-form fields (config, metadata labels and annotations) NOT_COVERED.
  strimzi_annotations  names the documentation mentions; not complete, so never UNKNOWN.
  kafka_cli            complete per tool, only when the option parser was read from that tool's
                       source (Kafka 4.3.2, tools of bin/); for the rest an absent option is
                       NOT_COVERED (see `reason` of the entry).
  argocd               CLI complete (generated reference); Application, ApplicationSet and AppProject
                       complete from the v3.6.1 definitions; any other kind is examples only.
  kubernetes           every kind of the v1.37.2 API schema complete; kubectl part examples only,
                       never UNKNOWN; kinds only in docs examples are incomplete.
  kafka_config_history keys whose default, type, valid values or existence differ between Kafka
                       4.1, 4.2 and 4.3.
  protocol_errors      complete (generated protocol reference, Kafka 4.3).
  connect_rest         complete (generated OpenAPI of the Connect worker, Kafka 4.3).

Stdlib only; reads JSON; no network.
"""
import difflib
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

KNOWN = "known"
UNKNOWN = "unknown"
NOT_COVERED = "not_covered"
STATUSES = (KNOWN, UNKNOWN, NOT_COVERED)

CATALOGUE_NAMES = ("kafka_config", "strimzi_resources", "strimzi_annotations",
                   "kafka_cli", "argocd", "kubernetes", "protocol_errors", "connect_rest",
                   "kafka_config_history")

MANIFEST_NAME = "CATALOGUES.json"

# Forms of config keys the Kafka reference does not list one by one.
OPEN_KEY_PREFIXES = ("producer.", "consumer.", "admin.", "listener.name.", "config.providers.")

# Keys of other projects (Spring Boot and similar): no source of this project documents them.
FOREIGN_KEY_PREFIXES = ("spring.", "quarkus.", "mp.messaging.", "camel.")

# Wrapper fields every Kubernetes object has; the catalogues do not describe them.
ENVELOPE = ("apiVersion", "kind", "metadata")


def default_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "work" / "m4" / "catalogues"


@dataclass(frozen=True)
class Lookup:
    status: str
    reason: str
    catalogue: Optional[str] = None
    evidence: Optional[str] = None
    entries: tuple = ()
    data: Dict[str, Any] = field(default_factory=dict)

    @property
    def known(self) -> bool:
        return self.status == KNOWN

    @property
    def unknown(self) -> bool:
        return self.status == UNKNOWN

    @property
    def not_covered(self) -> bool:
        return self.status == NOT_COVERED


def manifest_for(directory) -> Dict[str, Any]:
    """sha256 of every catalogue file in `directory` and of the whole set (the catalogue hash).
    The set hash is the sha256 of the lines "<name>.json <sha256>\\n" in the order of CATALOGUE_NAMES;
    a catalogue that is absent is left out. No timestamps: the same files give the same manifest."""
    d = Path(directory)
    files = {}
    for n in CATALOGUE_NAMES:
        f = d / (n + ".json")
        if f.is_file():
            files[n + ".json"] = hashlib.sha256(f.read_bytes()).hexdigest()
    whole = hashlib.sha256("".join("%s %s\n" % kv for kv in files.items()).encode()).hexdigest()
    return {"manifest": "catalogues/1", "files": files, "catalogue_hash": whole}


def catalogue_hash(directory=None) -> str:
    return manifest_for(directory or default_dir())["catalogue_hash"]


def _nc(reason, catalogue=None, **data):
    return Lookup(NOT_COVERED, reason, catalogue, data=data)


def normalise_path(path: str) -> str:
    """spec.kafka.listeners[0].name, listeners[*].name -> spec.kafka.listeners[].name"""
    return re.sub(r"\[(?:\d+|\*)?\]", "[]", path.strip().strip("."))


def _tool_name(tool: str) -> str:
    t = tool.strip()
    if t.startswith(("kafka-", "bin/kafka-", "./bin/kafka-", "/opt/kafka/bin/kafka-")) or \
            re.match(r"^(?:\S*/)?kafka-[a-z0-9-]+(?:\.sh)?$", t):
        base = t.split("/")[-1]
        return base if base.endswith(".sh") else base + ".sh"
    return " ".join(t.split())


class Catalogues:
    def __init__(self, data: Dict[str, Optional[dict]]):
        self.data = {n: data.get(n) for n in CATALOGUE_NAMES}
        self._config: Dict[str, List[dict]] = {}
        self._scopes = set()
        c = self.data["kafka_config"]
        if c:
            for e in c["entries"]:
                self._config.setdefault(e["key"], []).append(e)
                self._scopes.add(e["scope"])
        self._kinds: Dict[str, List[dict]] = {}
        for name, key in (("strimzi_resources", "entries"), ("argocd", "resources"),
                          ("kubernetes", "resources")):
            c = self.data[name]
            if c:
                for e in c[key] + (c.get("reference_entries", []) if name == "strimzi_resources" else []):
                    self._kinds.setdefault(e["kind"], []).append(dict(e, _catalogue=name))
        self._types = set()
        sr = self.data["strimzi_resources"] or {}
        for e in sr.get("entries", []) + sr.get("reference_entries", []):
            self._types.add(e["kind"])
            for p in e["paths"].values():
                self._types.update(p.get("declared_in", []))
                self._types.update(t.split("object:")[1] for t in p["type"] if "object:" in t)
        self._ann = {e["name"]: e for e in (self.data["strimzi_annotations"] or {}).get("entries", [])}
        self._tools: Dict[str, List[dict]] = {}
        for name, key in (("kafka_cli", "entries"), ("argocd", "cli"), ("kubernetes", "cli")):
            c = self.data[name]
            if c:
                for e in c[key]:
                    self._tools.setdefault(e["tool"], []).append(dict(e, _catalogue=name))

    @classmethod
    def load(cls, directory=None) -> "Catalogues":
        d = Path(directory) if directory else default_dir()
        data = {}
        for n in CATALOGUE_NAMES:
            f = d / (n + ".json")
            data[n] = json.loads(f.read_text()) if f.is_file() else None
        return cls(data)

    def loaded(self) -> List[str]:
        return [n for n in CATALOGUE_NAMES if self.data[n]]

    def complete(self, name: str) -> bool:
        """Is catalogue `name` loaded and marked complete? (A missing flag means no.)"""
        c = self.data.get(name)
        return bool(c and c.get("complete") is True)

    # -------------------------------------------------------------- Kafka configuration keys
    def scopes(self) -> List[str]:
        return sorted(self._scopes)

    def config_key(self, key: str, scope: Optional[str] = None) -> Lookup:
        """Is `key` a real Kafka configuration key (in `scope`, or in any scope)?"""
        if not self.data["kafka_config"]:
            return _nc("kafka_config catalogue not loaded")
        if scope is not None and scope not in self._scopes:
            return _nc("scope %r is not in the catalogue" % scope, "kafka_config", scopes=self.scopes())
        complete = self.complete("kafka_config")
        key = key.strip()
        hits = self._config.get(key, [])
        in_scope = [e for e in hits if scope is None or e["scope"] == scope]
        if in_scope:
            return Lookup(KNOWN, "listed in the generated reference", "kafka_config", "reference",
                          tuple(in_scope), {"scopes": sorted({e["scope"] for e in in_scope})})
        if key.startswith(FOREIGN_KEY_PREFIXES):
            return _nc("key of another project (%s); not documented by any source here" % key.split(".")[0],
                       "kafka_config")
        if key.startswith(OPEN_KEY_PREFIXES):
            return _nc("prefixed or per-listener form that the reference does not list", "kafka_config")
        if not complete:
            return _nc("the kafka_config catalogue is not marked complete", "kafka_config")
        data = {"also_in": sorted({e["scope"] for e in hits})} if hits else {}
        near = difflib.get_close_matches(
            key, [k for k, es in self._config.items() if scope is None or any(e["scope"] == scope for e in es)],
            n=3, cutoff=0.85)
        if near:
            data["similar"] = near
        return Lookup(UNKNOWN, "not in the reference for %s" % (("scope " + scope) if scope else "any scope"),
                      "kafka_config", "reference", (), data)

    def config_default(self, key: str, scope: Optional[str] = None) -> Lookup:
        r = self.config_key(key, scope)
        if not r.known:
            return r
        by = {e["scope"]: {"default": e.get("default"), "default_value": e.get("default_value")}
              for e in r.entries}
        data = dict(r.data, by_scope=by)
        vals = {v["default_value"] for v in by.values()}
        if len(vals) == 1:
            data["default_value"] = next(iter(vals))
            data["default"] = next(iter(by.values()))["default"]
        return Lookup(KNOWN, r.reason, r.catalogue, r.evidence, r.entries, data)

    def config_valid_values(self, key: str, scope: Optional[str] = None) -> Lookup:
        r = self.config_key(key, scope)
        if not r.known:
            return r
        by = {e["scope"]: {"valid_values": e.get("valid_values"), "list": e.get("valid_values_list")}
              for e in r.entries}
        data = dict(r.data, by_scope=by)
        firsts = {json.dumps(v, sort_keys=True) for v in by.values()}
        if len(firsts) == 1:
            data.update(next(iter(by.values())))
        return Lookup(KNOWN, r.reason, r.catalogue, r.evidence, r.entries, data)

    def config_value(self, key: str, value: str, scope: Optional[str] = None) -> Lookup:
        """KNOWN when `value` is in the enumerated valid values, UNKNOWN when the key has an
        enumeration and `value` is not in it, NOT_COVERED when the key is unknown to the
        catalogue or has no enumeration (ranges and free text are not checked)."""
        r = self.config_key(key, scope)
        if not r.known:
            return _nc("key not confirmed: " + r.reason, "kafka_config", key_status=r.status)
        lists = [e["valid_values_list"] for e in r.entries if e.get("valid_values_list")]
        if not lists:
            return _nc("the reference gives no enumeration for this key", "kafka_config")
        if any(value in l for l in lists):
            return Lookup(KNOWN, "value is in the valid values", "kafka_config", "reference", r.entries,
                          {"valid": lists[0]})
        return Lookup(UNKNOWN, "value is not among the valid values", "kafka_config", "reference",
                      r.entries, {"valid": lists[0]})

    # -------------------------------------------------------------- resource field paths
    def kinds(self) -> List[str]:
        return sorted(self._kinds)

    def field_path(self, kind: str, path: str, api_version: Optional[str] = None) -> Lookup:
        """Is `path` (from the resource root, e.g. spec.quotas.consumerByteRate) a field of `kind`?"""
        cands = [e for e in self._kinds.get(kind, []) if api_version in (None, e["api_version"])]
        if not cands:
            return _nc("kind %r (or that apiVersion) is in no catalogue" % kind)
        p = normalise_path(path)
        top = p.split(".")[0].split("[")[0]
        if top in ENVELOPE:
            return _nc("apiVersion, kind and metadata are not described by the catalogues")
        if top not in ("spec", "status") and \
                not any(top == q.split(".")[0].split("[")[0] for e in cands for q in e["paths"]):
            p = "spec." + p
        results = []
        for e in cands:
            paths = e["paths"]
            cat = e["_catalogue"]
            if p in paths:
                results.append(Lookup(KNOWN, "listed in the " + e["source"].replace("_", " "), cat,
                                      e["source"], (), {"info": paths[p], "api_version": e["api_version"]}))
                continue
            bare = p.replace("[]", "")
            alt = [q for q in paths if q.replace("[]", "") == bare]
            if alt:
                results.append(Lookup(KNOWN, "listed, but array brackets differ: " + alt[0], cat,
                                      e["source"], (), {"info": paths[alt[0]], "api_version": e["api_version"]}))
                continue
            segs = p.split(".")
            blocked = None
            for i in range(len(segs) - 1, 0, -1):
                anc = ".".join(segs[:i])
                if anc in paths and paths[anc].get("open"):
                    blocked = anc
                    break
            if blocked:
                results.append(_nc("below the free-form field %s" % blocked, cat, field=blocked))
            elif not e.get("complete"):
                results.append(_nc("catalogue holds examples only (%s)" % e["source"], cat))
            else:
                near = difflib.get_close_matches(p, list(paths), n=3, cutoff=0.8)
                same_leaf = [q for q in paths if q.split(".")[-1] == segs[-1]][:3]
                results.append(Lookup(UNKNOWN, "not a field of %s in the API reference" % kind, cat,
                                      e["source"], (), {"similar": near or same_leaf}))
        for st in (KNOWN, UNKNOWN, NOT_COVERED):
            for r in results:
                if r.status == st:
                    return r
        return _nc("no result")

    def strimzi_type(self, name: str) -> Lookup:
        """Is `name` a kind or a nested type of the Strimzi API (KafkaUserQuotas, GenericKafkaListener)?
        UNKNOWN only means the Strimzi API reference has no such type."""
        if not self.data["strimzi_resources"]:
            return _nc("strimzi_resources catalogue not loaded")
        if name in self._types:
            return Lookup(KNOWN, "type of the Strimzi API reference", "strimzi_resources", "api_reference")
        if not self.complete("strimzi_resources"):
            return _nc("the strimzi_resources catalogue is not marked complete", "strimzi_resources")
        return Lookup(UNKNOWN, "no such type in the Strimzi API reference", "strimzi_resources", "api_reference",
                      (), {"similar": difflib.get_close_matches(name, sorted(self._types), n=3, cutoff=0.8)})

    # -------------------------------------------------------------- annotations and labels
    def annotation(self, name: str, value: Optional[str] = None) -> Lookup:
        """Is `name` (strimzi.io/...) defined in the documentation? With `value`, data['value_status']
        is 'seen' or 'not_seen' (values come from examples, so 'not_seen' is weak evidence)."""
        if not self.data["strimzi_annotations"]:
            return _nc("strimzi_annotations catalogue not loaded")
        name = name.strip()
        if not name.startswith("strimzi.io/"):
            return _nc("only the strimzi.io/ prefix is catalogued", "strimzi_annotations")
        e = self._ann.get(name) or (self._ann.get(name[:-1]) if name.endswith("-") else None)
        if not e:
            similar = difflib.get_close_matches(name, list(self._ann), n=3, cutoff=0.8)
            if not self.complete("strimzi_annotations"):
                return _nc("not mentioned in the Strimzi documentation, and the catalogue of annotations is "
                           "not complete (the operator sets names the docs never show)",
                           "strimzi_annotations", similar=similar)
            return Lookup(UNKNOWN, "not mentioned in the Strimzi documentation", "strimzi_annotations",
                          "docs_mentions", (), {"similar": similar})
        data = {"kind": e["kind"], "values_seen": e["values_seen"]}
        if value is not None:
            data["value_status"] = "seen" if value in e["values_seen"] else (
                "not_seen" if e["values_seen"] else "no_values_documented")
        return Lookup(KNOWN, "mentioned in the Strimzi documentation", "strimzi_annotations",
                      "docs_mentions", (e,), data)

    # -------------------------------------------------------------- command line options
    def tools(self) -> List[str]:
        return sorted(self._tools)

    def cli_option(self, tool: str, option: str) -> Lookup:
        """Does `tool` (kafka-topics.sh, "argocd app sync", "kubectl get") have `option`?
        `evidence` on a KNOWN result is source_code, options_table, options_table_inherited or seen_in_example.
        An option absent from a tool without a full options list is NOT_COVERED, not UNKNOWN."""
        tname = _tool_name(tool)
        ents = self._tools.get(tname)
        if not ents:
            return _nc("tool %r is in no catalogue" % tname)
        opt = option.strip().split("=")[0]
        for e in ents:
            o = e["options"].get(opt)
            if o:
                data = {"files": o["files"], "tool_complete": bool(e.get("complete"))}
                for k in ("takes_argument", "description", "required", "commands", "aliases"):
                    if k in o:
                        data[k] = o[k]
                return Lookup(KNOWN, "option listed (%s)" % o["evidence"], e["_catalogue"], o["evidence"],
                              (), data)
        e = ents[0]
        if e.get("complete") is True:
            near = difflib.get_close_matches(opt, list(e["options"]), n=3, cutoff=0.7)
            return Lookup(UNKNOWN, "not in the full options list of %s" % tname, e["_catalogue"],
                          "source_code" if "source_code" in {o.get("evidence") for o in e["options"].values()}
                          else "options_table", (), {"similar": near})
        return _nc("no full options list for %s on disk (%d options seen in examples)" % (tname, len(e["options"])),
                   e["_catalogue"], options_seen=sorted(e["options"]))

    def cli_tool(self, tool: str) -> Lookup:
        """Is `tool` a script of Kafka's bin/ directory (4.3.2)? UNKNOWN only for a kafka-*.sh name that
        bin/ does not have (kafka-clusters.sh); other names are NOT_COVERED."""
        c = self.data["kafka_cli"]
        if not c:
            return _nc("kafka_cli catalogue not loaded")
        tname = _tool_name(tool)
        ents = [e for e in c["entries"] if e["tool"] == tname]
        if ents and ents[0].get("in_bin"):
            return Lookup(KNOWN, "script of bin/ in Kafka 4.3.2", "kafka_cli", "source_code", (ents[0],),
                          {"complete": bool(ents[0].get("complete")), "reason": ents[0].get("reason")})
        if c.get("bin_complete") and tname.startswith("kafka-") and tname.endswith(".sh"):
            near = difflib.get_close_matches(tname, c.get("bin_tools", []), n=3, cutoff=0.8)
            return Lookup(UNKNOWN, "not a script of bin/ in Kafka 4.3.2", "kafka_cli", "source_code", (),
                          {"similar": near})
        return _nc("tool %r is not in the bin/ listing" % tname, "kafka_cli")

    def cli_constraints(self, tool: str) -> List[dict]:
        """What the tool's code states about options that are required, available only with others or
        mutually exclusive (as the code wrote them, variables replaced by option names)."""
        return [c for e in self._tools.get(_tool_name(tool), []) for c in e.get("constraints", [])]

    # -------------------------------------------------------------- argocd sub-commands
    def argocd_commands(self) -> List[str]:
        return sorted(t for t, es in self._tools.items() if any(e["_catalogue"] == "argocd" for e in es))

    def argocd_command(self, command: str) -> Lookup:
        """Is `command` ("argocd app sync") a command of the argocd CLI? UNKNOWN only when the
        argocd CLI part of the catalogue is complete (the generated reference is)."""
        c = self.data["argocd"]
        if not c:
            return _nc("argocd catalogue not loaded")
        cmd = " ".join(command.split())
        if cmd in self._tools and any(e["_catalogue"] == "argocd" for e in self._tools[cmd]):
            return Lookup(KNOWN, "command of the generated CLI reference", "argocd", "options_table")
        if (c.get("complete_parts") or {}).get("cli") is not True:
            return _nc("the argocd CLI is not marked complete", "argocd")
        near = difflib.get_close_matches(cmd, self.argocd_commands(), n=3, cutoff=0.8)
        return Lookup(UNKNOWN, "not a command of the argocd CLI reference", "argocd", "options_table", (),
                      {"similar": near})

    # -------------------------------------------------------------- protocol error names
    def error_name(self, name: str) -> Lookup:
        """Is `name` (NOT_ENOUGH_REPLICAS_AFTER_APPEND) an error of the Kafka protocol?"""
        c = self.data["protocol_errors"]
        if not c:
            return _nc("protocol_errors catalogue not loaded")
        name = name.strip()
        for e in c["entries"]:
            if e["name"] == name:
                return Lookup(KNOWN, "listed in the generated protocol reference", "protocol_errors",
                              "reference", (e,), {"code": e["code"], "retriable": e["retriable"]})
        if not self.complete("protocol_errors"):
            return _nc("the protocol_errors catalogue is not marked complete", "protocol_errors")
        near = difflib.get_close_matches(name, [e["name"] for e in c["entries"]], n=3, cutoff=0.8)
        return Lookup(UNKNOWN, "not an error of the protocol reference", "protocol_errors", "reference", (),
                      {"similar": near})

    # -------------------------------------------------------------- Connect REST
    @staticmethod
    def rest_segments(path: str) -> List[str]:
        """'http://h:8083/connectors/x/restart?forward=true' -> ['connectors', 'x', 'restart']"""
        p = re.sub(r"^https?://[^/\s]+", "", path.strip())
        p = p.split("?")[0].split("#")[0]
        return [x for x in p.split("/") if x]

    @staticmethod
    def _is_placeholder(seg: str) -> bool:
        return bool(re.fullmatch(r"\{[^}]*\}|<[^>]*>|\$\{?\w+\}?|:\w+|\[[^\]]*\]", seg))

    def rest_template(self, path: str) -> Optional[str]:
        """The template of the REST catalogue that `path` matches ('/connectors/{connector}/status'), or None."""
        c = self.data["connect_rest"]
        if not c:
            return None
        segs = self.rest_segments(path)
        for tpl in sorted({e["path"] for e in c["entries"]}):
            ts = [x for x in tpl.split("/") if x]
            if len(ts) != len(segs):
                continue
            if all(t.startswith("{") or t == s for t, s in zip(ts, segs)):
                return tpl
        return None

    def rest_operation(self, method: str, path: str) -> Lookup:
        """Does the Connect REST interface serve `method` on `path`? A placeholder or a concrete name
        matches a {parameter} segment. UNKNOWN carries data['allowed_methods'] when the path exists."""
        c = self.data["connect_rest"]
        if not c:
            return _nc("connect_rest catalogue not loaded")
        m = method.strip().upper()
        tpl = self.rest_template(path)
        if tpl:
            ops = [e for e in c["entries"] if e["path"] == tpl]
            for e in ops:
                if e["method"] == m:
                    return Lookup(KNOWN, "operation of the generated Connect REST document", "connect_rest",
                                  "reference", (e,), {"template": tpl, "operation_id": e["operation_id"]})
        if not self.complete("connect_rest"):
            return _nc("the connect_rest catalogue is not marked complete", "connect_rest")
        if tpl:
            return Lookup(UNKNOWN, "the path exists but not with this method", "connect_rest", "reference", (),
                          {"template": tpl, "allowed_methods": sorted(e["method"] for e in c["entries"] if e["path"] == tpl)})
        return Lookup(UNKNOWN, "no such path in the Connect REST document", "connect_rest", "reference", (), {})

    # -------------------------------------------------------------- Kafka configuration history
    def config_history(self, key: str, scope: Optional[str] = None) -> Lookup:
        """Did the default, type, valid values or existence of `key` change between Kafka 4.1, 4.2 and 4.3?
        KNOWN: yes (data['entries'] have the values of each version and the list of changes).
        UNKNOWN: no difference is recorded (the key may also not exist)."""
        c = self.data["kafka_config_history"]
        if not c:
            return _nc("kafka_config_history catalogue not loaded")
        hits = [e for e in c["entries"] if e["key"] == key.strip() and (scope is None or e["scope"] == scope)]
        if hits:
            return Lookup(KNOWN, "differs between Kafka versions: " + ", ".join(sorted({x for e in hits for x in e["changes"]})),
                          "kafka_config_history", "reference", tuple(hits), {"changes": hits[0]["changes"]})
        if not self.complete("kafka_config_history"):
            return _nc("the kafka_config_history catalogue is not marked complete", "kafka_config_history")
        return Lookup(UNKNOWN, "no difference between Kafka 4.1, 4.2 and 4.3 is recorded", "kafka_config_history",
                      "reference")

    # -------------------------------------------------------------- Strimzi Cluster Operator install files
    def operator_deployment(self) -> Optional[dict]:
        c = self.data["strimzi_resources"]
        return (c or {}).get("operator_deployment")

    def operator_pod_labels(self) -> Dict[str, str]:
        """Labels of the Cluster Operator pod in the 1.3.1 install files (name, strimzi.io/kind)."""
        d = self.operator_deployment()
        return dict(d["pod_labels"]) if d else {}

    def operator_label(self, key: str, value: Optional[str] = None) -> Lookup:
        """Is `key` a label of the Cluster Operator pod in the install files? With `value`, data['value_ok']
        says whether it is the value there. Only these labels are known: other labels are NOT_COVERED."""
        d = self.operator_deployment()
        if not d:
            return _nc("strimzi_resources has no operator_deployment")
        labels = d["pod_labels"]
        if key not in labels:
            return _nc("not a label of the operator pod in the install files", "strimzi_resources")
        data = {"value": labels[key]}
        if value is not None:
            data["value_ok"] = labels[key] == value
        return Lookup(KNOWN, "label of the operator pod in packaging/install/cluster-operator", "strimzi_resources",
                      "install_files", (), data)

    def operator_env(self, name: str) -> Lookup:
        """Is `name` set in the Cluster Operator Deployment of the install files? data['value'] is the
        default value there. The operator reads more variables than the install files set: absent
        names are NOT_COVERED."""
        d = self.operator_deployment()
        if not d:
            return _nc("strimzi_resources has no operator_deployment")
        e = d["env"].get(name)
        if e is None:
            return _nc("not set in the Deployment of the install files", "strimzi_resources")
        return Lookup(KNOWN, "set in the Deployment of the install files", "strimzi_resources", "install_files",
                      (), dict(e))

    # -------------------------------------------------------------- schema validation of a manifest
    def schema_entry(self, kind: str, api_version: Optional[str] = None) -> Optional[dict]:
        """The entry (CRD or OpenAPI schema, complete) for `kind` and `api_version`, or None."""
        for e in self._kinds.get(kind, []):
            if e.get("complete") and e.get("source") in SCHEMA_SOURCES and api_version in (None, e["api_version"]):
                return e
        return None

    def _schema_groups(self) -> Dict[str, set]:
        """group (or '' for the core group) -> versions served, over every complete schema entry."""
        out: Dict[str, set] = {}
        for es in self._kinds.values():
            for e in es:
                if e.get("complete") and e.get("source") in SCHEMA_SOURCES:
                    g, _, v = e["api_version"].rpartition("/")
                    out.setdefault(g, set()).add(v)
        return out

    def validate_resource(self, doc: Any, kind: Optional[str] = None, api_version: Optional[str] = None) -> "Validation":
        """Validate a parsed YAML document against the schema of its kind and apiVersion (the CRD of
        Strimzi or Argo CD, the OpenAPI schema of Kubernetes). Strict: a field the schema does not
        define is an error unless the schema preserves unknown fields at that point (free-form map,
        x-kubernetes-preserve-unknown-fields); a value of the wrong type, a string outside an
        enumeration, a missing required field and a oneOf of required fields that does not hold exactly
        once are errors too. `metadata` is not checked and the CEL rules of the CRDs are not evaluated.
        When no complete schema covers the kind, `covered` is False and there are no violations."""
        if not isinstance(doc, dict):
            return Validation(kind, api_version, False, [Violation("", "not_a_mapping", "the document is not a mapping")])
        kind = kind or doc.get("kind")
        api = api_version or doc.get("apiVersion")
        v: List[Violation] = []
        if not isinstance(kind, str) or not isinstance(api, str):
            return Validation(kind, api, False, [Violation("", "missing_kind", "kind and apiVersion are needed")])
        group = api.rpartition("/")[0]
        e = self.schema_entry(kind, api)
        if e is None:
            served = sorted(x["api_version"] for x in self._kinds.get(kind, [])
                            if x.get("complete") and x.get("source") in SCHEMA_SOURCES)
            if served:
                v.append(Violation("apiVersion", "unknown_api_version",
                                   "%s is not served for %s; served: %s" % (api, kind, ", ".join(served))))
                return Validation(kind, api, True, v)
            if group in self._schema_groups():
                v.append(Violation("kind", "unknown_kind", "%s is not a kind of %s in the schemas%s" % (
                    kind, api, self._sim(kind, self.kinds_of(api)))))
                return Validation(kind, api, True, v)
            return Validation(kind, api, False, v)
        paths = e["paths"]
        kids = self._children(e)
        self._check_object(paths, kids, "", doc, v, root=True, root_open=bool(e.get("preserve_unknown_root")))
        return Validation(kind, api, True, v, e["source"])

    def kinds_of(self, api_version: str) -> List[str]:
        return sorted(k for k, es in self._kinds.items()
                      if any(x.get("complete") and x.get("source") in SCHEMA_SOURCES and x["api_version"] == api_version
                             for x in es))

    @staticmethod
    def _sim(name, pool):
        near = difflib.get_close_matches(name, pool, n=3, cutoff=0.8)
        return "; similar: " + ", ".join(near) if near else ""

    @staticmethod
    def _children(entry) -> Dict[str, List[str]]:
        cache = entry.setdefault("_children", {})
        if not cache:
            for q in entry["paths"]:
                if q.endswith("[]"):
                    continue
                parent, _, leaf = q.rpartition(".")
                cache.setdefault(parent, []).append(leaf)
        return cache

    def _check_object(self, paths, kids, path, value, out, root=False, root_open=False):
        info = paths.get(path, {}) if path else {}
        names = kids.get(path, [])
        open_ = root_open if root else bool(info.get("open"))
        for key in sorted(value, key=str):
            val = value[key]
            if not isinstance(key, str):
                out.append(Violation(_join(path, str(key)), "unknown_field", "key %r is not a string" % (key,)))
                continue
            if root and key in ENVELOPE:
                continue
            cp = _join(path, key)
            cinfo = paths.get(cp)
            if cinfo is None:
                if not open_:
                    near = difflib.get_close_matches(key, names, n=3, cutoff=0.6)
                    out.append(Violation(cp, "unknown_field", "%s is not a field of the schema%s" % (
                        cp, "; similar: " + ", ".join(near) if near else ""), tuple(near)))
                continue
            self._check_value(paths, kids, cp, val, out)
        for name in names:
            if name not in value and paths.get(_join(path, name), {}).get("required") and not (root and name in ENVELOPE):
                out.append(Violation(_join(path, name), "missing_required", "%s is required" % _join(path, name)))
        alts = info.get("one_of_required")
        if alts:
            ok = [a for a in alts if all(x in value and value[x] is not None for x in a)]
            if len(ok) != 1:
                out.append(Violation(path, "one_of", "%s needs exactly one of: %s (found %d)" % (
                    path or "the document", " | ".join(",".join(a) for a in alts), len(ok))))
        mv = info.get("map_values")
        if mv and not names:
            for key, val in value.items():
                if val is not None and not _type_ok(val, mv):
                    out.append(Violation(_join(path, str(key)), "wrong_type", "%s should be %s, found %s" % (
                        _join(path, str(key)), "/".join(mv), _py_type(val))))

    def _check_value(self, paths, kids, path, val, out):
        info = paths[path]
        if val is None:
            return
        types = info.get("type", ["any"])
        if not _type_ok(val, types):
            out.append(Violation(path, "wrong_type", "%s should be %s, found %s" % (path, "/".join(types), _py_type(val))))
            return
        if isinstance(val, dict):
            if "object" in types or "any" in types:
                self._check_object(paths, kids, path, val, out)
        elif isinstance(val, list):
            ipath = path + "[]"
            if ipath in paths:
                for i, item in enumerate(val):
                    ip = "%s[%d]" % (path, i)
                    if item is None:
                        continue
                    if not _type_ok(item, paths[ipath].get("type", ["any"])):
                        out.append(Violation(ip, "wrong_type", "%s should be %s, found %s" % (
                            ip, "/".join(paths[ipath]["type"]), _py_type(item))))
                    elif isinstance(item, dict):
                        for x in self._sub(paths, kids, ipath, item):
                            out.append(Violation(x.path.replace(ipath, ip, 1) if x.path.startswith(ipath) else x.path,
                                                 x.code, x.message.replace(ipath, ip), x.similar))
                    elif paths[ipath].get("enum") and item not in paths[ipath]["enum"]:
                        out.append(Violation(ip, "bad_enum", "%s: %r is not one of %s" % (ip, item, paths[ipath]["enum"])))
        elif info.get("enum") is not None and val not in info["enum"] and str(val) not in [str(x) for x in info["enum"]]:
            out.append(Violation(path, "bad_enum", "%s: %r is not one of %s" % (path, val, info["enum"])))

    def _sub(self, paths, kids, path, item):
        out: List[Violation] = []
        self._check_object(paths, kids, path, item, out)
        return out


SCHEMA_SOURCES = ("crd", "openapi")


@dataclass(frozen=True)
class Violation:
    path: str
    code: str       # unknown_field wrong_type bad_enum missing_required one_of unknown_kind unknown_api_version ...
    message: str
    similar: tuple = ()


@dataclass
class Validation:
    kind: Optional[str]
    api_version: Optional[str]
    covered: bool                      # a complete schema was found for the kind (and the group)
    violations: List[Violation] = field(default_factory=list)
    source: Optional[str] = None       # "crd" or "openapi"

    @property
    def ok(self) -> bool:
        return not self.violations

    def codes(self) -> List[str]:
        return sorted({x.code for x in self.violations})


def _join(path: str, key: str) -> str:
    return path + "." + key if path else key


def _py_type(v) -> str:
    return {bool: "boolean", int: "integer", float: "number", str: "string", dict: "object", list: "array"}.get(type(v), type(v).__name__)


def _type_ok(v, types) -> bool:
    if "any" in types:
        return True
    if isinstance(v, bool):
        return "boolean" in types
    if isinstance(v, int):
        return "integer" in types or "number" in types
    if isinstance(v, float):
        return "number" in types or ("integer" in types and v.is_integer())
    if isinstance(v, str):
        return "string" in types
    if isinstance(v, dict):
        return "object" in types
    if isinstance(v, list):
        return "array" in types
    return False

