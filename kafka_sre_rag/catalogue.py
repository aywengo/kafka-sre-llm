"""Lookups against the catalogues of real identifiers built by scripts/build_catalogues.py.

Every lookup returns a `Lookup` whose `status` is one of three distinct values:

  KNOWN        the catalogue lists the item (see `evidence` for how strong that is)
  UNKNOWN      the catalogue is complete for this question and the item is not in it
  NOT_COVERED  no catalogue can answer: it is not loaded, the kind/tool/scope is not in it, the
               catalogue is only a list of examples, or the path is below a free-form field

Only UNKNOWN may be reported as "invented". NOT_COVERED must never be.

Completeness, by catalogue:
  kafka_config         complete per scope (generated reference). A key that is absent from a scope
                       is UNKNOWN there; pass `scope` whenever the context is known, because
                       connector-specific keys and prefixed forms are NOT_COVERED, not UNKNOWN.
  strimzi_resources    complete for the ten Strimzi kinds (generated API reference), except below
                       free-form fields (config, metadata annotations, Kubernetes types such as
                       resources or affinity), which are NOT_COVERED.
  strimzi_annotations  names the documentation mentions; absence means "not documented".
  kafka_cli            complete only for tools with an options table (`complete`); for the rest an
                       absent option is NOT_COVERED.
  argocd               CLI complete (generated reference); resources are examples only.
  kubernetes           examples only; never UNKNOWN.

Stdlib only; reads JSON; no network.
"""
import difflib
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
                   "kafka_cli", "argocd", "kubernetes")

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
                for e in c[key]:
                    self._kinds.setdefault(e["kind"], []).append(dict(e, _catalogue=name))
        self._types = set()
        for e in (self.data["strimzi_resources"] or {}).get("entries", []):
            self._types.add(e["kind"])
            for p in e["paths"].values():
                self._types.update(p["declared_in"])
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

    # -------------------------------------------------------------- Kafka configuration keys
    def scopes(self) -> List[str]:
        return sorted(self._scopes)

    def config_key(self, key: str, scope: Optional[str] = None) -> Lookup:
        """Is `key` a real Kafka configuration key (in `scope`, or in any scope)?"""
        if not self.data["kafka_config"]:
            return _nc("kafka_config catalogue not loaded")
        if scope is not None and scope not in self._scopes:
            return _nc("scope %r is not in the catalogue" % scope, "kafka_config", scopes=self.scopes())
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
            elif not e["complete"]:
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
            return Lookup(UNKNOWN, "not mentioned in the Strimzi documentation", "strimzi_annotations",
                          "docs_mentions", (), {"similar": difflib.get_close_matches(name, list(self._ann), n=3, cutoff=0.8)})
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
        `evidence` on a KNOWN result is options_table, options_table_inherited or seen_in_example.
        An option absent from a tool without a full options list is NOT_COVERED, not UNKNOWN."""
        tname = _tool_name(tool)
        ents = self._tools.get(tname)
        if not ents:
            return _nc("tool %r is in no catalogue" % tname)
        opt = option.strip().split("=")[0]
        for e in ents:
            o = e["options"].get(opt)
            if o:
                return Lookup(KNOWN, "option listed (%s)" % o["evidence"], e["_catalogue"], o["evidence"],
                              (), {"files": o["files"], "tool_complete": e["complete"]})
        e = ents[0]
        if e["complete"]:
            near = difflib.get_close_matches(opt, list(e["options"]), n=3, cutoff=0.7)
            return Lookup(UNKNOWN, "not in the full options list of %s" % tname, e["_catalogue"],
                          "options_table", (), {"similar": near})
        return _nc("no full options list for %s on disk (%d options seen in examples)" % (tname, len(e["options"])),
                   e["_catalogue"], options_seen=sorted(e["options"]))
