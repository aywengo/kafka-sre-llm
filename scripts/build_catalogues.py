#!/usr/bin/env python3
"""Build catalogues of real identifiers from the pinned sources in `raw/`.

Output: JSON files in work/m4/catalogues/ (git-ignored), one per catalogue:

  kafka_config.json        config keys per scope (generated reference, kafka-site)
  strimzi_resources.json   field paths per Strimzi kind (generated API reference, appendix_crds.adoc)
  strimzi_annotations.json strimzi.io/ annotations and labels as the documentation uses them
  kafka_cli.json           kafka-*.sh options: options table or seen in an example
  argocd.json              argocd CLI (generated reference) and example manifests
  kubernetes.json          kubectl commands and manifest fields seen in the docs examples

  protocol_errors.json     Kafka protocol error names (generated reference, protocol_errors.html)
  connect_rest.json        Kafka Connect REST interface: method and path (connect_rest.yaml)
  CATALOGUES.json          manifest: sha256 of every catalogue file and of the whole set

Every entry carries `provenance` (a list: "exists" = schema, source code or generated reference,
"documented" = seen in documentation prose or an example). Every catalogue carries `complete`;
in kafka_cli.json every tool does too. Complete means the source is a full, generated list, so an
identifier that is absent from it does not exist. Anything else is "not covered".

Reads only files already on disk. No network, no git commands, nothing under raw/ is changed.
Deterministic: running it twice gives byte-identical files. Fails loudly (exit 2) when a
source file it expects is absent. Pins come from sources.yaml and kafka_sre_rag/frozen.json (runs/v3/INDEX_FROZEN.json, if present, must agree).

Usage: .venv/bin/python scripts/build_catalogues.py [--out DIR]
"""
import argparse
import hashlib
import html
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from kafka_sre_rag.catalogue import manifest_for  # noqa: E402

VERSION = "build_catalogues/3"
ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw"
DEFAULT_OUT = ROOT / "work" / "m4" / "catalogues"
RAWC = ROOT / "raw_catalogue"   # fetched by scripts/fetch_catalogue_sources.py (catalogue_sources.yaml)


class MissingSource(Exception):
    pass


# ------------------------------------------------------------------ pins and helpers
def load_pins():
    src = yaml.safe_load((ROOT / "sources.yaml").read_text())["sources"]
    commits = json.loads((ROOT / "kafka_sre_rag" / "frozen.json").read_text())["index"]["source_commits"]
    old = ROOT / "runs" / "v3" / "INDEX_FROZEN.json"   # not tracked; when present it must agree
    if old.is_file():
        other = json.loads(old.read_text())["source_commits"]
        if other != commits:
            diff = sorted(k for k in set(commits) | set(other) if commits.get(k) != other.get(k))
            raise MissingSource("runs/v3/INDEX_FROZEN.json and kafka_sre_rag/frozen.json disagree on source_commits: %s"
                                % ", ".join(diff))
    pins = {}
    for s in src:
        name = s["name"]
        if name not in commits:
            continue
        pins[name] = {
            "source": name,
            "ref": str(s.get("ref", "")),
            "commit": commits[name],
            "license": s.get("license", ""),
        }
    return add_fetched_pins(pins)


def load_fetched():
    """raw_catalogue/FETCHED.json (commit and sha256 of every fetched file), or MissingSource."""
    f = RAWC / "FETCHED.json"
    if not f.is_file():
        raise MissingSource("raw_catalogue/FETCHED.json is absent: run scripts/fetch_catalogue_sources.py")
    return json.loads(f.read_text())


def add_fetched_pins(pins):
    listed = {s["name"]: s for s in yaml.safe_load((ROOT / "catalogue_sources.yaml").read_text())["sources"]}
    fetched = load_fetched()["sources"]
    for name, s in listed.items():
        if name not in fetched:
            raise MissingSource("catalogue source %s was not fetched (raw_catalogue/FETCHED.json)" % name)
        rec = fetched[name]
        if rec["commit"] != s["commit"] or rec["ref"] != str(s["ref"]):
            raise MissingSource("%s: FETCHED.json has %s@%s, catalogue_sources.yaml says %s@%s" % (
                name, rec["ref"], rec["commit"], s["ref"], s["commit"]))
        pins[name] = {"source": name, "ref": str(s["ref"]), "commit": rec["commit"], "license": rec["license"],
                      "repo": s["repo"]}
    return pins


def check_fetched(name):
    """Every fetched file of `name` is still the file that FETCHED.json hashed."""
    rec = load_fetched()["sources"][name]
    for f, h in rec["files"].items():
        p = RAWC / name / f
        if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest() != h:
            raise MissingSource("%s: %s is missing or changed since it was fetched" % (name, p))


def rawc(name, *parts):
    return need(RAWC.joinpath(name, *parts))


def pin_str(pins, name):
    p = pins[name]
    return "%s@%s#%s" % (name, p["ref"], p["commit"][:12])


def check_checkout(pins, name):
    """The checkout on disk must be the pinned commit (read .git/HEAD, no git command).
    A source of catalogue_sources.yaml is checked against the hashes in FETCHED.json instead."""
    if name in load_fetched()["sources"]:
        check_fetched(name)
        return pins[name]["commit"]
    head = RAW / name / ".git" / "HEAD"
    if not head.is_file():
        raise MissingSource("no checkout of %s: %s" % (name, head))
    h = head.read_text().strip()
    if re.fullmatch(r"[0-9a-f]{40}", h) and h != pins[name]["commit"]:
        raise MissingSource("%s is at %s, pin is %s" % (name, h, pins[name]["commit"]))
    return h


def need(path):
    if not Path(path).is_file():
        raise MissingSource("expected source file is absent: %s" % path)
    return Path(path)


def rel(path):
    # Lexical, not resolved: the path must not depend on where raw/ really sits (symlinks).
    try:
        return str(Path(os.path.abspath(path)).relative_to(ROOT))
    except ValueError:      # a file outside the repository (tests)
        return str(path)


def dump(path, obj):
    path.write_text(json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


def header(name, pins, used, note, complete, complete_reason, **extra):
    h = {
        "catalogue": name,
        "generator": VERSION,
        "note": note,
        "complete": complete,
        "complete_reason": complete_reason,
        "sources": {n: pins[n] for n in sorted(used)},
    }
    h.update(extra)
    return h


def line_of(text, pos):
    return text.count("\n", 0, pos) + 1


# ------------------------------------------------------------------ OpenAPI / CRD schemas
ENVELOPE_FIELDS = ("apiVersion", "kind", "metadata")
QUANTITY_DEFS = ("io.k8s.apimachinery.pkg.api.resource.Quantity",)


def _scalar_types(node, defs, stack=()):
    """Types of a map's values when additionalProperties is a scalar schema, else None."""
    ap = node.get("additionalProperties")
    if not isinstance(ap, dict):
        return None
    n, _ = _deref(ap, defs, stack)
    if n is None:
        return None
    if n.get("x-quantity"):
        return ["integer", "number", "string"]
    t = n.get("type")
    if t in ("string", "integer", "number", "boolean") and not n.get("x-kubernetes-int-or-string"):
        return [t]
    return None


def _deref(node, defs, stack):
    """Follow $ref and a single-element allOf. -> (node, stack) or (None, name) on a cycle."""
    while True:
        ref = node.get("$ref")
        if ref is None and len(node.get("allOf") or []) == 1 and "properties" not in node:
            node = node["allOf"][0]
            continue
        if ref is None:
            return node, stack
        name = ref.split("/")[-1]
        if name in stack:
            return None, name
        if defs is None or name not in defs:
            raise MissingSource("schema reference %s is not defined" % ref)
        stack = stack + (name,)
        node = defs[name]
        if name in QUANTITY_DEFS:
            return dict(node, **{"x-quantity": True}), stack


def flatten_schema(root, defs=None, skip_envelope=True):
    """Flatten an OpenAPI v3 / swagger 2 schema (a CRD's openAPIV3Schema, or a definition of
    swagger.json) into {field path: info}. Paths are written spec.a.b[].c (arrays as [] after the
    array's own path). info keys: type (list), enum, required (true when the parent lists the field
    as required), open (children not enumerated: free-form map or object, preserved unknown fields,
    recursive type), preserve_unknown, map_values (scalar value types of a map), one_of_required
    (alternatives of `required` lists at an object: exactly one must hold), recursive.
    Strings the schema only describes (descriptions, examples) are dropped. Deterministic."""
    out = {}

    def types_of(node):
        if node.get("x-quantity"):
            return ["integer", "number", "string"]
        if node.get("x-kubernetes-int-or-string") or node.get("format") == "int-or-string":
            return ["integer", "string"]
        t = node.get("type")
        if isinstance(t, list):
            return sorted(t)
        if t:
            return [t]
        alts = sorted({a.get("type") for a in node.get("anyOf", []) if isinstance(a, dict) and a.get("type")})
        if alts:
            return alts
        return ["object"] if node.get("properties") else ["any"]

    def walk(node, path, stack, required):
        n, stack2 = _deref(node, defs, stack)
        if n is None:                      # recursive type: stop here
            if path:
                info = out.setdefault(path, {})
                info.update(type=info.get("type", ["object"]), open=True, recursive=True)
                if required:
                    info["required"] = True
            return
        node, stack = n, stack2
        types = types_of(node)
        props = node.get("properties") or {}
        info = {"type": types}
        if node.get("enum"):
            info["enum"] = sorted(node["enum"], key=str)
        if required:
            info["required"] = True
        if node.get("x-kubernetes-preserve-unknown-fields"):
            info["preserve_unknown"] = True
            info["open"] = True
        if "object" in types or "any" in types:
            if isinstance(node.get("additionalProperties"), (dict, bool)) and node.get("additionalProperties") is not False:
                info["open"] = True
                mv = _scalar_types(node, defs, stack)
                if mv:
                    info["map_values"] = mv
            if not props and "any" not in types and not info.get("open"):
                info["open"] = True        # an object that declares no fields: free-form
            if "any" in types:
                info["open"] = True
        alts = [sorted(a["required"]) for a in node.get("oneOf", []) if isinstance(a, dict) and a.get("required")]
        if alts and len(alts) == len(node.get("oneOf", [])):
            info["one_of_required"] = alts
        if path:
            out[path] = info
        if props:
            req = set(node.get("required") or [])
            for name in sorted(props):
                if not path and skip_envelope and name in ENVELOPE_FIELDS:
                    continue
                walk(props[name], (path + "." if path else "") + name, stack, name in req)
        if "array" in types and isinstance(node.get("items"), dict):
            walk(node["items"], path + "[]", stack, False)

    walk(root, "", (), False)
    return out


def crd_entries(files, pin):
    """Entries (one per kind and served version) from CRD YAML files [(path, parsed doc)]."""
    res = []
    for f, d in files:
        if not isinstance(d, dict) or d.get("kind") != "CustomResourceDefinition":
            continue
        spec = d["spec"]
        kind = spec["names"]["kind"]
        for v in spec["versions"]:
            if not v.get("served"):
                continue
            schema = v["schema"]["openAPIV3Schema"]
            paths = flatten_schema(schema)
            res.append({
                "kind": kind, "api_version": "%s/%s" % (spec["group"], v["name"]),
                "complete": True, "provenance": ["exists"], "source": "crd",
                "source_file": rel(f), "pin": pin, "storage_version": bool(v.get("storage")),
                "scope": spec.get("scope"), "plural": spec["names"].get("plural"),
                "preserve_unknown_root": bool(schema.get("x-kubernetes-preserve-unknown-fields")),
                "paths": dict(sorted(paths.items())),
            })
    res.sort(key=lambda e: (e["kind"], e["api_version"]))
    return res


PROV_OF_EVIDENCE = {"source_code": ["exists"], "options_table": ["exists"], "options_table_inherited": ["exists"],
                    "seen_in_example": ["documented"]}


# ------------------------------------------------------------------ kafka_config
CONFIG_FILES = {
    "admin_client_config.html": "admin",
    "connect_config.html": "connect",
    "consumer_config.html": "consumer",
    "group_config.html": "group",
    "kafka_config.html": "broker",
    "mirror_checkpoint_config.html": "mirror_checkpoint",
    "mirror_connector_config.html": "mirror_connector",
    "mirror_heartbeat_config.html": "mirror_heartbeat",
    "mirror_source_config.html": "mirror_source",
    "producer_config.html": "producer",
    "remote_log_manager_config.html": "remote_log_manager",
    "remote_log_metadata_manager_config.html": "remote_log_metadata_manager",
    "sink_connector_config.html": "sink_connector",
    "source_connector_config.html": "source_connector",
    "streams_config.html": "streams",
    "topic_config.html": "topic",
}
PLUGIN_FILES = {  # h6 keys under a <div id="class">: Connect transformations and predicates
    "connect_transforms.html": "connect_transform",
    "connect_predicates.html": "connect_predicate",
}
ROW_FIELDS = {
    "Type": "type",
    "Default": "default",
    "Valid Values": "valid_values",
    "Importance": "importance",
    "Update Mode": "update_mode",
    "Server Default Property": "server_default_property",
}
LI_RE = re.compile(r"<li>\s*<h([46])>(.*?)</h\1>(.*?)</li>", re.S)
TAG_RE = re.compile(r"<[^>]+>")
ROW_RE = re.compile(r"<tr><th>(.*?):</th><td>(.*?)</td></tr>", re.S)
DIV_RE = re.compile(r'<div id="([^"]+)">')


def clean(s):
    return html.unescape(TAG_RE.sub("", s)).strip()


def enum_list(valid):
    m = re.fullmatch(r"\[(.*)\]", valid or "")
    if not m or "..." in m.group(1):
        return None
    return [v.strip() for v in m.group(1).split(",") if v.strip()]


def parse_config_html(text, scope, source_file, pin):
    entries = []
    divs = [(m.start(), m.group(1)) for m in DIV_RE.finditer(text)]
    for m in LI_RE.finditer(text):
        anchors = re.findall(r"<a [^>]*>([^<]+)</a>", m.group(2))
        if not anchors:
            continue
        key = html.unescape(anchors[-1]).strip()
        e = {"scope": scope, "key": key, "source_file": source_file, "pin": pin,
             "source_line": line_of(text, m.start(2)), "provenance": ["exists"]}
        e["description"] = html.unescape(" ".join(clean(x) for x in re.findall(r"<p>(.*?)</p>", m.group(3), re.S))).strip()
        for k, v in ROW_RE.findall(m.group(3)):
            if k in ROW_FIELDS:
                e[ROW_FIELDS[k]] = clean(v)
        e["valid_values_list"] = enum_list(e.get("valid_values"))
        # "300000 (5 minutes)": the reference adds a human-readable gloss after the value
        dm = re.fullmatch(r"(.*?)\s+\(\d[^()]*\)", e.get("default", ""))
        e["default_value"] = dm.group(1) if dm else e.get("default")
        if scope in ("connect_transform", "connect_predicate"):
            owner = [d for pos, d in divs if pos < m.start()]
            e["owner"] = owner[-1] if owner else None
        entries.append(e)
    return entries


def build_kafka_config(pins):
    base = RAW / "apache-kafka-config" / "content" / "en" / "43" / "generated"
    pin = pin_str(pins, "apache-kafka-config")
    entries = []
    for fname, scope in sorted({**CONFIG_FILES, **PLUGIN_FILES}.items()):
        f = need(base / fname)
        got = parse_config_html(f.read_text(), scope, rel(f), pin)
        if not got and scope not in ("connect_predicate",):
            raise MissingSource("no config keys parsed from %s" % f)
        entries += got
    entries.sort(key=lambda e: (e["scope"], e.get("owner") or "", e["key"]))
    out = header("kafka_config", pins, ["apache-kafka-config"],
                 "Complete per scope. Metrics and protocol pages are not catalogued. "
                 "Prefixed forms (producer.override.*, listener.name.*) are not listed.",
                 True, "generated configuration reference of Kafka 4.3, one page per scope; "
                 "connector-specific keys and prefixed forms are not listed, so pass a scope",
                 versions=["4.3"])
    out["entries"] = entries
    counts = defaultdict(int)
    for e in entries:
        counts[e["scope"]] += 1
    return out, "%d keys in %d scopes (%s)" % (
        len(entries), len(counts), ", ".join("%s %d" % kv for kv in sorted(counts.items())))


# ------------------------------------------------------------------ kafka_config_history
HISTORY_DIRS = {"4.1": ("kafka-config-pages", "static/41/generated"),
                "4.2": ("kafka-config-pages", "content/en/42/generated")}
HISTORY_FIELDS = ("type", "default_value", "valid_values", "importance")


def config_entries_of(base, pin, ver):
    got = {}
    for fname, scope in sorted({**CONFIG_FILES, **PLUGIN_FILES}.items()):
        f = base / fname
        if not f.is_file():
            continue
        for e in parse_config_html(f.read_text(), scope, rel(f), pin):
            got[(e["scope"], e.get("owner") or "", e["key"])] = e
    return got


def diff_config_versions(versions, order):
    """versions: {version: {(scope, owner, key): entry}} -> entries for the keys that differ."""
    keys = sorted(set().union(*[set(v) for v in versions.values()]))
    entries = []
    for k in keys:
        row = {}
        for ver in order:
            e = versions[ver].get(k)
            row[ver] = None if e is None else {f: e.get(f) for f in HISTORY_FIELDS}
        changes = []
        present = [v for v in order if row[v]]
        if len(present) < len(order):
            first, last = present[0], present[-1]
            if order.index(first) > 0:
                changes.append("added_in_" + first)
            if order.index(last) < len(order) - 1:
                changes.append("removed_after_" + last)
        for f, label in (("default_value", "default"), ("type", "type"), ("valid_values", "valid_values")):
            vals = [row[v][f] for v in order if row[v]]
            if len(set(vals)) > 1:
                changes.append(label)
        if changes:
            entries.append({"scope": k[0], "key": k[2], "owner": k[1] or None, "changes": changes,
                            "versions": row, "provenance": ["exists"],
                            "source_files": sorted({versions[v][k]["source_file"] for v in order if k in versions[v]})})
    return entries


def build_kafka_config_history(pins):
    versions = {}
    pin43 = pin_str(pins, "apache-kafka-config")
    versions["4.3"] = config_entries_of(RAW / "apache-kafka-config" / "content" / "en" / "43" / "generated", pin43, "4.3")
    for ver, (src, sub) in sorted(HISTORY_DIRS.items()):
        base = RAWC / src / sub
        if not base.is_dir():
            raise MissingSource("no configuration pages for %s under %s" % (ver, base))
        versions[ver] = config_entries_of(base, pin_str(pins, src), ver)
        if len(versions[ver]) < 500:
            raise MissingSource("only %d keys parsed for Kafka %s" % (len(versions[ver]), ver))
    order = ["4.1", "4.2", "4.3"]
    entries = diff_config_versions(versions, order)
    out = header("kafka_config_history", pins, ["apache-kafka-config", "kafka-config-pages"],
                 "Keys of the generated configuration reference whose default, type, valid values or existence "
                 "differ between Kafka 4.1, 4.2 and 4.3, with the values of each version (null = the key is not "
                 "on that version's page). Same page set and parser as kafka_config. A change is a difference "
                 "in the text of the reference: a default shown with a different unit or spelling is listed as "
                 "changed. importance is stored but a change of it alone is not listed.",
                 True, "the generated reference pages of the three versions, all scopes the kafka_config catalogue has",
                 versions=["4.1", "4.2", "4.3"])
    out["entries"] = entries
    cnt = defaultdict(int)
    for e in entries:
        for c in e["changes"]:
            cnt[c.split("_in_")[0].split("_after_")[0]] += 1
    return out, "%d keys changed over 4.1-4.3 (%s); %d keys in 4.3, %d in 4.2, %d in 4.1" % (
        len(entries), ", ".join("%s %d" % kv for kv in sorted(cnt.items())),
        len(versions["4.3"]), len(versions["4.2"]), len(versions["4.1"]))


# ------------------------------------------------------------------ strimzi resources
SECTION_RE = re.compile(r"^\[id='type-(.+?)-\{context\}'\]\n", re.M)
XREF_RE = re.compile(r"xref:type-([A-Za-z0-9]+)-\{context\}\[`[^`]*`\]")
ENUM_RE = re.compile(r"\((?:one|one or more) of \[([^\]]*)\]\)")
REQUIRED_RE = re.compile(r"(?:^|\s)Required\.|This is a required property|Required in the `v1`")
PRIMS = {"string", "integer", "boolean", "number", "map", "object", "long", "double"}


def parse_prop_type(raw):
    """-> dict(targets=[type names], base='string'|..., array=bool, enum=[...])"""
    raw = raw.strip()
    enum = None
    m = ENUM_RE.search(raw)
    if m:
        enum = [v.strip() for v in m.group(1).split(",") if v.strip()]
    targets = XREF_RE.findall(raw)
    array = raw.rstrip().endswith(" array")
    base = None
    if not targets:
        base = re.sub(r"\s*\((?:one|one or more) of \[[^\]]*\]\)", "", raw)
        base = re.sub(r" array$", "", base).strip()
        lm = re.fullmatch(r"https?://\S+\[([^\]]+)\]", base)
        if lm:  # a Kubernetes (or other) type documented elsewhere: fields are not enumerated
            base = "external:" + lm.group(1)
    return {"targets": targets, "base": base, "array": array, "enum": enum}


def parse_appendix(text):
    """-> (types, roots). types[name] = {props: {name: {...}}, used_in: bool, disc: value|None}"""
    parts = SECTION_RE.split(text)
    types = {}
    # parts = [preamble, id1, body1, id2, body2, ...]
    for i in range(1, len(parts), 2):
        sid, body = parts[i], parts[i + 1]
        name = sid[:-len("-schema")] if sid.endswith("-schema") else sid
        t = types.setdefault(name, {"props": {}, "used_in": False, "disc": None})
        if "Used in:" in body:
            t["used_in"] = True
        m = re.search(r"It must have the value `([^`]+)` for the type", body)
        if m:
            t["disc"] = m.group(1)
        tm = re.search(r"^\|Property \|Property type \|Description\n(.*?)^\|====", body, re.S | re.M)
        if not tm:
            continue
        lines = tm.group(1).split("\n")
        rows, cur = [], None
        for ln in lines:
            if ln.startswith("|") and (cur is None or len(cur) < 3 or True):
                # a new cell starts with '|' at line start; a row has exactly 3 cells
                if cur is None or len(cur) == 3:
                    cur = []
                    rows.append(cur)
                cur.append(ln[1:])
            elif cur:
                cur[-1] += "\n" + ln
        for r in rows:
            if len(r) != 3:
                raise ValueError("table row with %d cells in type %s: %r" % (len(r), name, r[:1]))
            pname, ptype, desc = r[0].strip(), r[1].strip(), r[2].strip()
            p = parse_prop_type(ptype)
            p["required_hint"] = bool(REQUIRED_RE.search(desc))
            t["props"][pname] = p
    roots = sorted(n for n, t in types.items() if not t["used_in"] and "spec" in t["props"])
    return types, roots


def flatten(types, tname, prefix, stack, out, declared=None):
    t = types[tname]
    for pname, p in t["props"].items():
        path = prefix + pname
        rec = out.setdefault(path, {"type": set(), "enum": set(), "required_hint": False,
                                    "declared_in": set(), "open": False})
        rec["declared_in"].add(tname)
        rec["required_hint"] = rec["required_hint"] or p["required_hint"]
        arr = "[]" if p["array"] else ""
        if p["targets"]:
            for tg in p["targets"]:
                rec["type"].add(("array of " if p["array"] else "") + "object:" + tg)
        else:
            base = p["base"]
            rec["type"].add(("array of " if p["array"] else "") + base)
            if base in ("map", "object") or base.startswith("external:"):
                rec["open"] = True
        if p["enum"]:
            rec["enum"].update(p["enum"])
        # the discriminator value of a subtype is a valid enum value of "type"
        if pname == "type" and t["disc"]:
            rec["enum"].add(t["disc"])
        for tg in p["targets"]:
            if tg not in types:
                rec["open"] = True
                continue
            if tg in stack:
                rec["recursive"] = True
                continue
            flatten(types, tg, path + arr + ".", stack | {tg}, out)


def build_strimzi_resources(pins):
    f = need(RAW / "strimzi" / "documentation" / "modules" / "appendix_crds.adoc")
    pin = pin_str(pins, "strimzi")
    types, roots = parse_appendix(f.read_text())
    # api versions: the appendix names none; take what example manifests in the docs show
    seen = defaultdict(set)
    for adoc in sorted((RAW / "strimzi" / "documentation").rglob("*.adoc")):
        txt = adoc.read_text(errors="replace")
        for m in re.finditer(r"^apiVersion: ([a-z.]*strimzi\.io/[a-z0-9]+)\s*\nkind: (\w+)", txt, re.M):
            seen[m.group(2)].add(m.group(1))
    kinds = []
    for k in roots:
        out = {}
        flatten(types, k, "", {k}, out)
        paths = {}
        for path, r in sorted(out.items()):
            d = {"type": sorted(r["type"]), "declared_in": sorted(r["declared_in"]),
                 "provenance": ["documented"]}
            if r["enum"]:
                d["enum"] = sorted(r["enum"])
            if r["required_hint"]:
                d["required_hint"] = True
            if r["open"]:
                d["open"] = True
            if r.get("recursive"):
                d["recursive"] = True
            paths[path] = d
        group = "core.strimzi.io" if k == "StrimziPodSet" else "kafka.strimzi.io"
        kinds.append({
            "kind": k,
            "api_version": group + "/v1",
            "api_version_evidence": (
                "seen in example manifests: " + ", ".join(sorted(seen[k])) if seen.get(k) else
                "not stated in the reference; every apiVersion line of the checked-out docs is v1"),
            "root_type": k,
            "complete": True,
            "provenance": ["documented"],
            "source": "api_reference",
            "source_file": rel(f),
            "pin": pin,
            "paths": paths,
        })
    crd_dir = RAWC / "strimzi-install" / "packaging" / "install" / "cluster-operator"
    crds = []
    for cf in sorted(crd_dir.glob("04*-Crd-*.yaml")):
        crds.append((cf, yaml.safe_load(cf.read_text())))
    cpin = pin_str(pins, "strimzi-install")
    entries = crd_entries(crds, cpin)
    if not entries:
        raise MissingSource("no CRD under %s" % crd_dir)
    served = defaultdict(list)
    for e in entries:
        served[e["kind"]].append(e["api_version"])
    docs_only = {}                    # paths the reference lists and the CRD does not
    crd_only = {}
    by_kind = {e["kind"]: e for e in entries}
    for k in kinds:
        c = by_kind.get(k["kind"])
        if c:
            a = {p.replace("[]", "") for p in k["paths"]}
            b = {p.replace("[]", "") for p in c["paths"]}
            docs_only[k["kind"]] = len(a - b)
            crd_only[k["kind"]] = len(b - a)
    operator = operator_deployment(crd_dir, cpin)
    out = header("strimzi_resources", pins, ["strimzi", "strimzi-install"],
                 "Two lists of entries. `entries`, source=crd (provenance exists): the full schema of every served "
                 "version from the 1.3.1 custom resource definitions (packaging/install/cluster-operator, "
                 "04*-Crd-*.yaml): type, enum, required (the parent lists it), open (free-form map or object, "
                 "preserved unknown fields: the children are not enumerated), one_of_required. Only the served "
                 "versions are listed: 1.3.1 serves v1 only, so apiVersion v1beta2 is not served. The CEL rules "
                 "(x-kubernetes-validations) are not evaluated. `reference_entries`, source=api_reference (provenance documented): "
                 "the generated reference in the documentation, as before, now under `reference_entries` (provenance documented). "
                 "operator_deployment: labels, selector, "
                 "environment variables and ConfigMap keys of the Cluster Operator install files.",
                 True, "the custom resource definitions are the schema the API server enforces; the reference "
                 "entries are kept as documented. Paths below free-form or external-typed fields are not covered",
                 versions=["strimzi 1.3.1"])
    # `entries` holds one entry per kind and served version (the CRDs); the entries of the documentation
    # reference are kept apart, so that code that lists the kinds from `entries` sees each kind once
    out["entries"] = entries
    out["reference_entries"] = sorted(kinds, key=lambda e: e["kind"])
    out["operator_deployment"] = operator
    total = sum(len(k["paths"]) for k in entries)
    return out, ("%d kinds from CRDs (versions %s), %d schema paths; %d documented entries kept; paths in the reference "
                 "but not in the CRD %s, in the CRD but not in the reference %s; operator: %d env vars, %d pod labels" % (
                     len(entries), ",".join(sorted({v.split("/")[1] for vs in served.values() for v in vs})), total, len(kinds),
                     sum(docs_only.values()), sum(crd_only.values()),
                     len(operator["env"]), len(operator["pod_labels"])))


def operator_deployment(crd_dir, pin):
    dep = yaml.safe_load(need(crd_dir / "060-Deployment-strimzi-cluster-operator.yaml").read_text())
    cm = yaml.safe_load(need(crd_dir / "050-ConfigMap-strimzi-cluster-operator.yaml").read_text())
    tpl = dep["spec"]["template"]
    c = tpl["spec"]["containers"][0]
    env = {}
    for e in c.get("env", []):
        rec = {}
        if "value" in e:
            rec["value"] = e["value"]
        if "valueFrom" in e:
            rec["value_from"] = e["valueFrom"]
        env[e["name"]] = rec
    return {
        "provenance": ["exists"], "pin": pin,
        "source_files": [rel(crd_dir / "060-Deployment-strimzi-cluster-operator.yaml"),
                         rel(crd_dir / "050-ConfigMap-strimzi-cluster-operator.yaml")],
        "deployment_name": dep["metadata"]["name"],
        "deployment_labels": dep["metadata"].get("labels", {}),
        "selector": dep["spec"]["selector"].get("matchLabels", {}),
        "pod_labels": tpl["metadata"].get("labels", {}),
        "service_account": tpl["spec"].get("serviceAccountName"),
        "container": {"name": c["name"], "image": c["image"], "args": c.get("args", []),
                      "ports": c.get("ports", [])},
        "env": dict(sorted(env.items())),
        "configmap": {"name": cm["metadata"]["name"], "labels": cm["metadata"].get("labels", {}),
                      "data_keys": sorted(cm.get("data", {}))},
    }


# ------------------------------------------------------------------ strimzi annotations
ANN_RE = re.compile(r"(?<![A-Za-z0-9._/-])strimzi\.io/([A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])")
VAL_RE = re.compile(r"""^\s*(?:=|:\s)\s*(?:"([^"]*)"|'([^']*)'|([^\s"',;)]+))""")
NOT_ANN = {"docs", "downloads", "documentation", "quickstarts", "contributing", "charts",
           "community", "blog", "install", "examples"}


def context_kind(lines, i, line, pos):
    low = line.lower()
    if "kubectl annotate" in low or "annotation" in low[:pos + 40] or "annotate" in low:
        return "annotation"
    if "kubectl label" in low or " -l " in line or "--selector" in low or "label" in low:
        return "label"
    ind = len(line) - len(line.lstrip())
    for j in range(i - 1, max(i - 25, -1), -1):
        l2 = lines[j]
        if not l2.strip():
            continue
        i2 = len(l2) - len(l2.lstrip())
        s = l2.strip()
        if i2 < ind and s in ("annotations:", "labels:", "matchLabels:"):
            return "annotation" if s == "annotations:" else "label"
        if i2 < ind:
            ind = i2
    return "unspecified"


def build_strimzi_annotations(pins):
    root = RAW / "strimzi" / "documentation"
    need(root / "modules" / "appendix_crds.adoc")
    pin = pin_str(pins, "strimzi")
    ents = {}
    for f in sorted(root.rglob("*.adoc")):
        lines = f.read_text(errors="replace").split("\n")
        for i, line in enumerate(lines):
            for m in ANN_RE.finditer(line):
                before = line[:m.start()]
                if before.rstrip().endswith("//") or before.endswith("www.") or "link:" in before[-12:]:
                    continue
                name = m.group(1)
                if name in NOT_ANN:
                    continue
                key = "strimzi.io/" + name
                e = ents.setdefault(key, {"name": key, "kind_counts": defaultdict(int),
                                          "values_seen": set(), "files": set(), "removal_form": False})
                e["kind_counts"][context_kind(lines, i, line, m.start())] += 1
                e["files"].add(rel(f))
                vm = VAL_RE.match(line[m.end():])
                if vm:
                    v = next(g for g in vm.groups() if g is not None)
                    v = v.strip().strip("`.").strip()
                    if v and not v.startswith(("|", "<")):
                        e["values_seen"].add(v)
    # "key-" is the kubectl removal syntax: fold into the key
    out_entries = []
    for key, e in sorted(ents.items()):
        if key.endswith("-") and key[:-1] in ents:
            continue
        kc = dict(e["kind_counts"])
        decided = {k: v for k, v in kc.items() if k != "unspecified"}
        kind = max(sorted(decided), key=lambda k: decided[k]) if decided else "unspecified"
        out_entries.append({
            "name": key,
            "kind": kind,
            "kind_counts": dict(sorted(kc.items())),
            "values_seen": sorted(e["values_seen"]),
            "values_evidence": "values seen in example commands and manifests, not a closed list",
            "source_files": sorted(e["files"]),
            "source_file": sorted(e["files"])[0],
            "pin": pin,
            "provenance": ["documented"],
        })
    out = header("strimzi_annotations", pins, ["strimzi"],
                 "Names with the prefix strimzi.io/ found in the Strimzi documentation. 'kind' is "
                 "decided from the surrounding text and is a heuristic. A name that is absent here "
                 "is not proof of invention: operator-applied labels and annotations are only "
                 "listed when the docs mention them.",
                 False, "names the documentation mentions, not a generated list")
    out["entries"] = out_entries
    return out, "%d names" % len(out_entries)


# ------------------------------------------------------------------ CLI helpers
OPT_RE = re.compile(r"(?<![\w-])(--[a-zA-Z][\w-]*)")
CMD_CUT_RE = re.compile(r"\s(?:\||&&|;|>|2>|<<)\s?")


def code_lines(text):
    """Yield logical command lines from code blocks (fenced or indented) joined on trailing backslash."""
    raw = text.split("\n")
    in_fence = False
    block = []
    blocks = []
    for ln in raw:
        if ln.lstrip().startswith("```") or ln.strip() in ("----", "...."):
            if in_fence:
                blocks.append(block)
                block = []
            in_fence = not in_fence
            continue
        if in_fence:
            block.append(ln)
        elif ln.startswith("    ") or ln.startswith("\t"):
            block.append(ln)
        else:
            if block:
                blocks.append(block)
                block = []
    if block:
        blocks.append(block)
    for b in blocks:
        cur = ""
        for ln in b:
            s = ln.rstrip()
            if cur:
                cur += " " + s.strip()
            else:
                cur = s.strip()
            if s.endswith("\\"):
                cur = cur[:-1].rstrip()
                continue
            yield cur
            cur = ""
        if cur:
            yield cur


def options_in_command(cmd_after_tool):
    cut = CMD_CUT_RE.split(" " + cmd_after_tool, maxsplit=1)[0]
    # drop quoted values so that --foo inside a quoted string is not read as an option
    cut = re.sub(r'"[^"]*"|\'[^\']*\'', '""', cut)
    return OPT_RE.findall(cut)


def add_opt(tools, tool, opt, evidence, source_file):
    t = tools.setdefault(tool, {"options": {}})
    o = t["options"].setdefault(opt, {"evidence": evidence, "files": set(), "seen": 0})
    order = {"options_table": 3, "options_table_inherited": 2, "seen_in_example": 1}
    if order[evidence] > order[o["evidence"]]:
        o["evidence"] = evidence
    o["files"].add(source_file)
    o["seen"] += 1


def finish_tools(tools, pin, complete_when):
    res = []
    for tool, t in sorted(tools.items()):
        opts = {}
        for opt, o in sorted(t["options"].items()):
            opts[opt] = {"evidence": o["evidence"], "files": sorted(o["files"]), "seen": o["seen"],
                         "provenance": PROV_OF_EVIDENCE[o["evidence"]]}
        has_table = any(o["evidence"] in ("options_table",) for o in opts.values())
        res.append({
            "tool": tool,
            "complete": complete_when(t, has_table),
            "options": opts,
            "provenance": sorted({p for o in opts.values() for p in o["provenance"]}) or ["documented"],
            "source_files": sorted({f for o in opts.values() for f in o["files"]}),
            "pin": pin,
        })
    return res


# ------------------------------------------------------------------ kafka_cli from the tools' source
def mask_comments(src):
    """Same length as `src`: comments blanked (newlines kept), string and char literals kept."""
    out = []
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if src.startswith('"""', i):
            j = src.find('"""', i + 3)
            j = n if j < 0 else j + 3
            out.append(src[i:j])
            i = j
        elif c == '"':
            j = i + 1
            while j < n and src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            out.append(src[i:j + 1])
            i = j + 1
        elif c == "'" and i + 2 < n and (src[i + 2] == "'" or (src[i + 1] == "\\" and "'" in src[i + 2:i + 5])):
            j = src.index("'", i + 2 if src[i + 1] != "\\" else i + 3)
            out.append(src[i:j + 1])
            i = j + 1
        elif src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif src.startswith("/*", i):
            j = src.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append("".join(ch if ch == "\n" else " " for ch in src[i:j]))
            i = j
        else:
            out.append(c)
            i += 1
    return "".join(out)


def close_of(src, open_idx):
    """Index of the bracket that closes src[open_idx] (strings are skipped)."""
    pairs = {"(": ")", "[": "]", "{": "}"}
    depth, i, n = 0, open_idx, len(src)
    while i < n:
        c = src[i]
        if c == '"':
            if src.startswith('"""', i):
                i = src.index('"""', i + 3) + 3
                continue
            i += 1
            while i < n and src[i] != '"':
                i += 2 if src[i] == "\\" else 1
        elif c in pairs:
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise ValueError("unbalanced brackets")


def split_args(src, open_idx):
    """-> ([(start, end)] of the top-level arguments, index of the closing bracket)."""
    end = close_of(src, open_idx)
    args, depth, start, i = [], 0, open_idx + 1, open_idx + 1
    while i < end:
        c = src[i]
        if c == '"':
            if src.startswith('"""', i):
                i = src.index('"""', i + 3) + 3
                continue
            i += 1
            while i < end and src[i] != '"':
                i += 2 if src[i] == "\\" else 1
        elif c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            args.append((start, i))
            start = i + 1
        i += 1
    if src[start:end].strip():
        args.append((start, end))
    return args, end


STR_RE = re.compile(r'"""(.*?)"""|"((?:[^"\\]|\\.)*)"', re.S)
ESC = {"n": "\n", "t": "\t", '"': '"', "'": "'", "\\": "\\", "r": "\r"}


def unescape(s):
    return re.sub(r"\\(.)", lambda m: ESC.get(m.group(1), m.group(1)), s)


def literals(text):
    return [unescape(m.group(1) if m.group(1) is not None else m.group(2)) for m in STR_RE.finditer(text)]


def only_literals(text):
    """True when `text` is string literals joined by + and nothing else (so the text is complete)."""
    rest = STR_RE.sub("", text)
    return not re.sub(r"[\s+()]", "", rest) and not re.search(r"String\s*\.\s*format|\.formatted|\.join", text)


def statement_end(src, i):
    """End of the statement that continues from src[i]: a `;`, or a newline that is not followed by
    (or preceded by) a continuing `.` at bracket depth 0 (Scala has no `;`)."""
    n, depth = len(src), 0
    while i < n:
        c = src[i]
        if c == '"':
            if src.startswith('"""', i):
                i = src.index('"""', i + 3) + 3
                continue
            i += 1
            while i < n and src[i] != '"':
                i += 2 if src[i] == "\\" else 1
        elif c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth < 0:
                return i
        elif c == ";" and depth == 0:
            return i
        elif c == "\n" and depth == 0:
            nxt = re.match(r"\s*(.)", src[i:])
            prev = src[:i].rstrip()[-1:]
            if not (nxt and nxt.group(1) == ".") and prev != ".":
                return i
        i += 1
    return n


JOPT_RE = re.compile(r"(?:(\w+)\s*=\s*)?(?:[\w.]*?)[pP]arser\s*\.\s*(accepts|acceptsAll)\s*\(")
ARGP_RE = re.compile(r"(\w+)\s*\.\s*addArgument\s*\(")
SUBP_RE = re.compile(r"(\w+)\s*=\s*(\w+)\s*\.\s*addParser\s*\(\s*\"([^\"]+)\"")
GROUP_RE = re.compile(r"(\w+)\s*=\s*(\w+)\s*\.\s*(?:addMutuallyExclusiveGroup|addArgumentGroup)\s*\(")
CONST_RE = re.compile(r"(?:static\s+final\s+String|val)\s+([A-Za-z_]\w*)\s*(?::\s*String)?\s*=\s*\"((?:[^\"\\]|\\.)*)\"\s*;?")
CONSTRAINT_JOPT_RE = re.compile(r"\.\s*(requiredIf|requiredUnless|availableIf|availableUnless)\s*\(")
CHECK_RE = re.compile(r"CommandLineUtils\s*\.\s*(checkRequiredArgs|checkInvalidArgs|checkInvalidArgsSet)\s*\(")
ARG_NOARG_ACTIONS = ("storeTrue", "storeFalse", "count", "version", "help", "storeConst")


def dash(n):
    """joptsimple names have no dashes: one letter is -x, anything longer is --name."""
    return n if n.startswith(("-", "<")) else ("-" + n if len(n) == 1 else "--" + n)


def parse_option_source(path, src, consts):
    """Options defined in one Java or Scala source file.
    -> (options, constraints, problems). options: {name: {takes_argument, description, ...}}"""
    m = mask_comments(src)
    srel = rel(path)
    opts, cons, problems = {}, [], []
    var2opt = {}

    def line(pos):
        return m.count("\n", 0, pos) + 1

    def resolve_name(text):
        lits = literals(text)
        if only_literals(text) and lits:
            return ["".join(lits)] if len(lits) == 1 or "+" in text else lits
        ident = text.strip()
        if re.fullmatch(r"[A-Za-z_][\w.]*", ident):
            key = ident.split(".")[-1]
            if key in consts:
                return [consts[key]]
        return None

    # joptsimple: parser.accepts("name", "description").withRequiredArg()...
    for jm in JOPT_RE.finditer(m):
        args, close = split_args(m, jm.end() - 1)
        if not args:
            continue
        first = m[args[0][0]:args[0][1]]
        if jm.group(2) == "acceptsAll":
            names = literals(first)
            if not names:
                names = resolve_name(re.sub(r"^.*?\(", "", first, count=1).rstrip(") ")) or []
        else:
            names = resolve_name(first)
        if not names:
            problems.append("%s:%d option name is not a literal: %s" % (srel, line(jm.start()), first.strip()[:60]))
            continue
        names = [dash(x) for x in names]
        desc_text = m[args[1][0]:args[1][1]] if len(args) > 1 else ""
        chain = m[close + 1:statement_end(m, close + 1)]
        takes = "required" if "withRequiredArg" in chain else ("optional" if "withOptionalArg" in chain else "none")
        rec = {"takes_argument": takes, "description": " ".join("".join(literals(desc_text)).split()),
               "files": [srel], "line": line(jm.start()), "evidence": "source_code"}
        if desc_text and not only_literals(desc_text):
            rec["description_partial"] = True
        if re.search(r"\.\s*required\s*\(\s*\)", chain):
            rec["required"] = True
        if "forHelp" in chain:
            rec["for_help"] = True
        if "withValuesSeparatedBy" in chain:
            rec["list_separator"] = (literals(chain[chain.index("withValuesSeparatedBy"):]) or [None])[0]
        dm = re.search(r"describedAs\s*\(\s*\"((?:[^\"\\]|\\.)*)\"", chain)
        if dm:
            rec["argument_name"] = unescape(dm.group(1))
        dflt = re.search(r"defaultsTo\s*\(", chain)
        if dflt:
            _, dc = split_args(chain, dflt.end() - 1)
            rec["default"] = " ".join(chain[dflt.end():dc].split())[:120]
        for cm in CONSTRAINT_JOPT_RE.finditer(chain):
            a, _ = split_args(chain, cm.end() - 1)
            others = [dash(literals(chain[x:y])[0]) if STR_RE.fullmatch(chain[x:y].strip()) else chain[x:y].strip()
                      for x, y in a]
            cons.append({"option": names[0], "kind": cm.group(1), "others": others, "raw_others": True,
                         "source": "%s:%d" % (srel, line(jm.start()))})
        for nme in names:
            opts[nme] = dict(rec, aliases=[x for x in names if x != nme])
        if jm.group(1):
            var2opt[jm.group(1)] = names[0]
    # argparse4j: receiver.addArgument("--name", "-n").help("...").action(storeTrue())
    sub_of = {}
    events = []
    for sm in SUBP_RE.finditer(m):
        events.append((sm.start(), "sub", sm.group(1), sm.group(2), sm.group(3)))
    for gm in GROUP_RE.finditer(m):
        events.append((gm.start(), "group", gm.group(1), gm.group(2), None))
    for am in ARGP_RE.finditer(m):
        events.append((am.start(), "arg", am.group(1), None, am))
    events.sort(key=lambda e: e[0])
    for ev in events:
        if ev[1] == "sub":
            sub_of[ev[2]] = ev[4]
        elif ev[1] == "group":
            sub_of[ev[2]] = sub_of.get(ev[3])
        else:
            am = ev[4]
            args, close = split_args(m, am.end() - 1)
            texts = [m[x:y] for x, y in args]
            names = []
            for t in texts:
                names += [x for x in literals(t)]
            long_or_short = [x for x in names if x.startswith("-")]
            chain = m[close + 1:statement_end(m, close + 1)]
            if not long_or_short:
                if names:
                    opts.setdefault("<%s>" % names[0], {"takes_argument": "required", "positional": True,
                                                        "description": "", "files": [srel], "line": line(am.start()),
                                                        "evidence": "source_code", "aliases": []})
                continue
            act = re.search(r"\.\s*action\s*\(\s*(?:Arguments\s*\.\s*)?(\w+)", chain)
            takes = "none" if act and act.group(1) in ARG_NOARG_ACTIONS else "required"
            hm = re.search(r"\.\s*help\s*\(", chain)
            desc, partial = "", False
            if hm:
                ha, _ = split_args(chain, hm.end() - 1)
                htxt = chain[ha[0][0]:ha[0][1]] if ha else ""
                desc = " ".join("".join(literals(htxt)).split())
                partial = not only_literals(htxt)
            rec = {"takes_argument": takes, "description": desc, "files": [srel], "line": line(am.start()),
                   "evidence": "source_code"}
            if partial:
                rec["description_partial"] = True
            if re.search(r"\.\s*required\s*\(\s*true\s*\)", chain):
                rec["required"] = True
            cmd = sub_of.get(ev[2])
            for nme in long_or_short:
                o = opts.setdefault(nme, dict(rec, aliases=[x for x in long_or_short if x != nme]))
                if cmd:
                    o.setdefault("commands", [])
                    if cmd not in o["commands"]:
                        o["commands"].append(cmd)
                else:
                    o.pop("commands", None)
                    o["global"] = True
    # the checks the code makes
    for cm in CHECK_RE.finditer(m):
        args, close = split_args(m, cm.end() - 1)
        texts = [" ".join(m[x:y].split()) for x, y in args][2:]
        cons.append({"kind": cm.group(1), "args": texts, "source": "%s:%d" % (srel, line(cm.start()))})
    return opts, cons, problems, var2opt, "CommandDefaultOptions" in m and re.search(r"extends\s+CommandDefaultOptions", m) is not None, \
        bool(events) and bool(re.search(r"newArgumentParser", m)) and not re.search(r"addHelp\s*\(\s*false", m)


def cls_index(root):
    """fully qualified class name -> file, for every Java and Scala file under `root`."""
    idx = {}
    for f in sorted(root.rglob("*")):
        if f.suffix in (".java", ".scala"):
            rp = f.relative_to(root).as_posix()
            mm = re.search(r"src/main/(?:java|scala)/(.+)\.(?:java|scala)$", rp)
            if mm:
                idx[mm.group(1).replace("/", ".")] = f
    return idx


RUN_CLASS_RE = re.compile(r'kafka-run-class\.sh\s+(?:\$EXTRA_ARGS\s+)?"?\$?\{?([\w.]+)\}?"?')
CASE_CLASS_RE = re.compile(r'^\s*([\w-]+)\)\s*CLASS="([\w.]+)"', re.M)


def tool_classes(script_text):
    """[(command or None, fully qualified class)] that bin/<tool>.sh runs through kafka-run-class.sh."""
    cases = CASE_CLASS_RE.findall(script_text)
    if cases:
        return [(a, c) for a, c in cases]
    return [(None, mm.group(1)) for mm in RUN_CLASS_RE.finditer(script_text) if "." in mm.group(1)]


def parse_tool(script, root, idx, consts, default_opts):
    """-> dict(tool entry fields) for one bin/*.sh, with `complete` only when the option parser was read."""
    text = script.read_text()
    classes = tool_classes(text)
    res = {"tool": script.name, "main_classes": [c for _, c in classes], "options": {}, "constraints": [],
           "files": [], "problems": [], "complete": False}
    if not classes:
        res["reason"] = "the script does not run a Java class through kafka-run-class.sh with a literal class name"
        return res
    ok = True
    for command, cls in classes:
        f = idx.get(cls)
        if f is None:
            res["problems"].append("main class %s is not in the fetched source" % cls)
            ok = False
            continue
        files = [f]
        simple = cls.split(".")[-1]
        for suffix in ("Options",):
            g = idx.get(cls + suffix)
            if g is not None:
                files.append(g)
        got_any = False
        for ff in files:
            opts, cons, problems, var2opt, inherits, argp = parse_option_source(ff, ff.read_text(), consts)
            res["problems"] += problems
            for c in cons:
                if "option" in c:
                    c["others"] = [var2opt.get(x, x) for x in c["others"]]
                else:
                    c["args"] = [var2opt.get(x, x) for x in c["args"]]
                res["constraints"].append(c)
            if opts:
                got_any = True
            for name, rec in opts.items():
                cur = res["options"].get(name)
                if command:
                    rec = dict(rec, commands=sorted(set(rec.get("commands", [])) | {command}))
                if cur is None:
                    res["options"][name] = rec
                else:
                    cur["files"] = sorted(set(cur["files"]) | set(rec["files"]))
                    if "commands" in cur or "commands" in rec:
                        cur["commands"] = sorted(set(cur.get("commands", [])) | set(rec.get("commands", [])))
            if inherits:
                for name, rec in default_opts.items():
                    res["options"].setdefault(name, dict(rec))
            if argp:
                for name, desc in (("--help", "argparse4j adds -h/--help"), ("-h", "argparse4j adds -h/--help")):
                    res["options"].setdefault(name, {"takes_argument": "none", "description": desc, "files": [rel(ff)],
                                                     "evidence": "source_code", "aliases": [], "for_help": True})
            res["files"] += [rel(ff)]
        if not got_any:
            ok = False
            res["problems"].append("no option definition found in %s" % ", ".join(rel(x) for x in files))
    res["files"] = sorted(set(res["files"]))
    if res["problems"]:
        ok = False
    res["complete"] = ok and bool(res["options"])
    if not res["complete"] and "reason" not in res:
        res["reason"] = "; ".join(res["problems"]) or "no option was parsed"
    return res


# tools that the parser cannot call complete, with the reason read from their script and source
SPECIAL_TOOLS = {
    "kafka-run-class.sh": "runs any Java class: the options are those of the class named on the command line",
    "kafka-server-stop.sh": "shell script with no option parser: it greps --process-role and --node-id from its arguments and reads the SIGNAL variable",
    "kafka-server-start.sh": "wrapper script: -daemon, -name and -loggc are read by the shell scripts, the file name is positional; Kafka.scala parses only --override and --version",
    "connect-distributed.sh": "no option parser: ConnectDistributed takes a properties file and --help only by reading args",
    "connect-standalone.sh": "no option parser: ConnectStandalone takes a worker properties file and connector properties files as positional arguments",
}


# ------------------------------------------------------------------ kafka_cli
KTOOL_RE = re.compile(r"(?<![\w-])(kafka-[a-z0-9-]+)\.sh\b")
TABLE_START_RE = re.compile(r"^\s*\*?\s*Option\b.*\bDescription\b")
TABLE_OPT_RE = re.compile(r"^\s{0,12}\*?\s*(--[a-zA-Z][\w-]*)")


def build_kafka_cli(pins):
    root = RAW / "apache-kafka" / "docs"
    need(root / "streams" / "developer-guide" / "app-reset-tool.md")
    pin = pin_str(pins, "apache-kafka")
    spin = pin_str(pins, "strimzi")
    tools = {}
    sources = [(root, "apache-kafka", "**/*.md", pin),
               (RAW / "strimzi" / "documentation", "strimzi", "**/*.adoc", spin)]
    for base, _name, glob, p in sources:
        for f in sorted(base.glob(glob)):
            text = f.read_text(errors="replace")
            srel = rel(f)
            # 1. options tables (help output pasted into the docs)
            lines = text.split("\n")
            last_tool = None
            i = 0
            while i < len(lines):
                tm = KTOOL_RE.search(lines[i])
                if tm and not TABLE_START_RE.match(lines[i]):
                    last_tool = tm.group(1) + ".sh"
                if TABLE_START_RE.match(lines[i]) and last_tool:
                    j = i + 1
                    while j < len(lines) and (lines[j].strip() == "" or lines[j].startswith(" ") or lines[j].startswith("\t")):
                        om = TABLE_OPT_RE.match(lines[j])
                        if om:
                            add_opt(tools, last_tool, om.group(1), "options_table", srel)
                        j += 1
                    i = j
                    continue
                i += 1
            # 2. options seen in example commands
            for cmd in code_lines(text):
                for tm in KTOOL_RE.finditer(cmd):
                    tool = tm.group(1) + ".sh"
                    for opt in options_in_command(cmd[tm.end():]):
                        add_opt(tools, tool, opt, "seen_in_example", srel)
                    tools.setdefault(tool, {"options": {}})
    entries = finish_tools(tools, pin + " ; " + spin, lambda t, has_table: has_table)
    for e in entries:
        e["source_file"] = e["source_files"][0] if e["source_files"] else None
    entries, summary = merge_source_options(entries, pins)
    out = header("kafka_cli", pins, ["apache-kafka", "strimzi", "apache-kafka-tools"],
                 "One entry per tool of bin/ in Kafka 4.3.2 (bin_tools lists them all; bin_complete) plus the "
                 "tools the docs mention that are not in bin/ (in_bin=false, never complete). For a tool of "
                 "bin/ whose option parser was read from its source (joptsimple accepts(), argparse4j "
                 "addArgument()), complete is true and options are those of the source: takes_argument, "
                 "description, required, aliases, commands (sub-commands of argparse tools), plus "
                 "constraints (requiredIf/availableIf and the CommandLineUtils checks as the code states them, "
                 "variables replaced by option names). Options the docs show that the source does not define "
                 "are listed in documented_not_in_source. A tool whose parser could not be read stays "
                 "incomplete with `reason`; its options are those seen in docs and examples. Prefix "
                 "abbreviations of joptsimple options (--bootstrap) are switched off by CommandDefaultOptions "
                 "(new OptionParser(false)); the catalogue lists full names only.",
                 False, "per tool: complete only when the option parser was read from that tool's source; "
                 "see the per-tool complete flag and reason")
    out["entries"] = entries
    out["bin_tools"] = sorted(summary["bin"])
    out["bin_complete"] = True
    full = [e["tool"] for e in entries if e["complete"]]
    inc = [(e["tool"], e.get("reason", "")) for e in entries if not e["complete"] and e.get("in_bin")]
    return out, "%d tools, %d options; %d complete from source; incomplete in bin/: %s; %d documented-only options listed" % (
        len(entries), sum(len(e["options"]) for e in entries), len(full),
        "; ".join("%s (%s)" % (a, b[:60]) for a, b in inc) or "-", summary["doc_only"])


def merge_source_options(doc_entries, pins):
    base = RAWC / "apache-kafka-tools"
    root = base / "bin"
    if not root.is_dir():
        raise MissingSource("no bin/ under %s" % base)
    srcroot = base
    idx = cls_index(srcroot)
    consts = {}
    for f in sorted(srcroot.rglob("*")):
        if f.suffix in (".java", ".scala"):
            for cm in CONST_RE.finditer(mask_comments(f.read_text())):
                consts.setdefault(cm.group(1), unescape(cm.group(2)))
    cdo = idx["org.apache.kafka.server.util.CommandDefaultOptions"]
    default_opts, _, _, _, _, _ = parse_option_source(cdo, cdo.read_text(), consts)
    kpin = pin_str(pins, "apache-kafka-tools")
    by = {e["tool"]: e for e in doc_entries}
    scripts = sorted(root.glob("*.sh"))
    bin_names = [s.name for s in scripts]
    out = []
    doc_only_total = 0
    for sc in scripts:
        r = parse_tool(sc, base, idx, consts, default_opts)
        doc = by.pop(sc.name, None)
        complete, reason = r["complete"], r.get("reason")
        if sc.name in SPECIAL_TOOLS:
            complete, reason = False, SPECIAL_TOOLS[sc.name]
        opts = {}
        doc_opts = (doc or {}).get("options", {})
        for name, rec in sorted(r["options"].items()):
            o = {k: v for k, v in rec.items() if v not in (None, [], "") or k in ("description",)}
            o["provenance"] = ["exists"] + (["documented"] if name in doc_opts else [])
            if name in doc_opts:
                o["doc_files"] = doc_opts[name]["files"]
            opts[name] = o
        only = sorted(n for n in doc_opts if n not in opts)
        if complete:
            doc_only_total += len(only)
        else:
            for n in only:   # a tool we could not read: keep what the docs show
                opts[n] = doc_opts[n]
        e = {"tool": sc.name, "in_bin": True, "complete": complete,
             "main_classes": r["main_classes"], "options": dict(sorted(opts.items())),
             "provenance": sorted({p for o in opts.values() for p in o.get("provenance", ["documented"])}) or ["exists"],
             "source_files": sorted(set(r["files"]) | {"raw_catalogue/apache-kafka-tools/bin/" + sc.name}),
             "pin": kpin + (" ; " + doc["pin"] if doc else ""), "constraints": r["constraints"]}
        e["source_file"] = e["source_files"][0]
        if not complete:
            e["reason"] = reason
        if complete and only:
            e["documented_not_in_source"] = only
        out.append(e)
    for tool, doc in sorted(by.items()):   # tools of the docs that bin/ does not have
        doc = dict(doc, in_bin=False, complete=False,
                   reason="not a script of bin/ in Kafka 4.3.2; known only from the documentation")
        out.append(doc)
    out.sort(key=lambda e: e["tool"])
    return out, {"bin": bin_names, "doc_only": doc_only_total}


# ------------------------------------------------------------------ argocd
H_OPT_RE = re.compile(r"^\s+(?:-\w, )?(--[a-zA-Z][\w-]*)")


def parse_cobra_md(text):
    """-> (command, own options, inherited options)"""
    m = re.search(r"^## (.+)$", text, re.M)
    cmd = m.group(1).strip() if m else None
    sec = {}
    for name in ("Options", "Options inherited from parent commands"):
        sm = re.search(r"^### %s\n+```\n(.*?)```" % re.escape(name), text, re.M | re.S)
        sec[name] = []
        if sm:
            for ln in sm.group(1).split("\n"):
                om = H_OPT_RE.match(ln)
                if om:
                    sec[name].append(om.group(1))
    return cmd, sec["Options"], sec["Options inherited from parent commands"]


def flatten_yaml(obj, prefix, out):
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = prefix + "." + str(k) if prefix else str(k)
            out.setdefault(p, set()).add(type_name(v))
            flatten_yaml(v, p, out)
    elif isinstance(obj, list):
        for v in obj:
            p = prefix + "[]"
            out.setdefault(p, set()).add(type_name(v))
            flatten_yaml(v, p, out)


def type_name(v):
    return {dict: "object", list: "array", str: "string", bool: "boolean", int: "integer",
            float: "number", type(None): "null"}.get(type(v), "other")


def manifest_entries(docs_by_file, pin):
    """docs_by_file: list of (source_file, [parsed documents]) -> list of kind entries (examples)."""
    kinds = {}
    for sf, docs in docs_by_file:
        for d in docs:
            if not isinstance(d, dict) or "kind" not in d or "apiVersion" not in d:
                continue
            key = (str(d["apiVersion"]), str(d["kind"]))
            e = kinds.setdefault(key, {"paths": {}, "files": set()})
            e["files"].add(sf)
            paths = {}
            flatten_yaml({k: v for k, v in d.items() if k not in ("apiVersion", "kind")}, "", paths)
            for p, tys in paths.items():
                rec = e["paths"].setdefault(p, {"type": set(), "files": set()})
                rec["type"] |= tys
                rec["files"].add(sf)
    res = []
    for (av, kind), e in sorted(kinds.items()):
        res.append({
            "kind": kind, "api_version": av, "complete": False, "source": "example_manifests",
            "provenance": ["documented"],
            "source_file": sorted(e["files"])[0], "source_files": sorted(e["files"]), "pin": pin,
            "paths": {p: {"type": sorted(r["type"]), "files": sorted(r["files"]),
                          "provenance": ["documented"]}
                      for p, r in sorted(e["paths"].items())},
        })
    return res


def build_argocd(pins):
    root = RAW / "argo-cd" / "docs"
    pin = pin_str(pins, "argo-cd")
    cmd_dir = root / "user-guide" / "commands"
    srv_dir = root / "operator-manual" / "server-commands"
    cmd_files = sorted(cmd_dir.glob("*.md"))
    srv_files = sorted(srv_dir.glob("*.md"))
    if not cmd_files:
        raise MissingSource("no argocd command reference under %s" % cmd_dir)
    need(cmd_dir / "argocd_app_sync.md")
    tools = {}
    shorts = {}
    for f in cmd_files + srv_files:
        cmd, own, inh = parse_cobra_md(f.read_text())
        if not cmd:
            continue
        sm = re.search(r"^## .+\n+(.+)$", f.read_text(), re.M)
        shorts[cmd] = sm.group(1).strip() if sm else ""
        t = tools.setdefault(cmd, {"options": {}})
        for o in own:
            add_opt(tools, cmd, o, "options_table", rel(f))
        for o in inh:
            add_opt(tools, cmd, o, "options_table_inherited", rel(f))
    cli = finish_tools(tools, pin, lambda t, has_table: True)
    for e in cli:
        e["source_file"] = e["source_files"][0] if e["source_files"] else None
        e["description"] = shorts.get(e["tool"], "")
    # example manifests
    mf = [need(root / "operator-manual" / n) for n in ("application.yaml", "project.yaml", "applicationset.yaml")]
    docs = []
    for f in mf:
        try:
            docs.append((rel(f), [d for d in yaml.safe_load_all(f.read_text())]))
        except yaml.YAMLError as ex:
            raise MissingSource("cannot parse %s: %s" % (f, ex))
    examples = manifest_entries(docs, pin)
    cdir = RAWC / "argocd-crds" / "manifests" / "crds"
    crds = [(f, yaml.safe_load(f.read_text())) for f in sorted(cdir.glob("*-crd.yaml"))]
    cpin = pin_str(pins, "argo-cd-crds") if "argo-cd-crds" in pins else pin_str(pins, "argocd-crds")
    schema_entries = crd_entries(crds, cpin)
    if sorted(e["kind"] for e in schema_entries) != ["AppProject", "Application", "ApplicationSet"]:
        raise MissingSource("unexpected Argo CD CRDs: %s" % [e["kind"] for e in schema_entries])
    have = {(e["kind"], e["api_version"]): e for e in schema_entries}
    ex_only = 0
    kept = []
    for e in examples:
        c = have.get((e["kind"], e["api_version"]))
        if c:   # the schema wins over the example; the example paths the schema lacks are counted
            ex_only += len({q.replace("[]", "") for q in e["paths"]} - {q.replace("[]", "") for q in c["paths"]})
        else:
            kept.append(e)
    res = sorted(schema_entries + kept, key=lambda e: (e["kind"], e["api_version"]))
    out = header("argocd", pins, ["argo-cd", "argocd-crds"],
                 "cli: every command of the generated reference, options complete (own and "
                 "inherited). resources: the Application, ApplicationSet and AppProject definitions "
                 "(manifests/crds, v3.6.1) flattened to field paths with type, enum, required and open "
                 "(complete=true, provenance exists); any other kind is kept from the example manifests "
                 "under docs/operator-manual (complete=false). Example paths that the schema does not "
                 "define are dropped for the three kinds.",
                 True, "the CLI part is complete (generated cobra reference); every resource kind is one of the "
                 "three definitions, complete from the schema",
                 complete_parts={"cli": True, "resources": all(e["complete"] for e in res),
                                 "resource_kinds_complete": sorted(e["kind"] for e in schema_entries)},
                 versions=["argo-cd v3.6.1"])
    out["cli"] = cli
    out["resources"] = res
    nopt = len({o for e in cli for o in e["options"]})
    return out, "%d commands (%d distinct flags), %d resource kinds (%s), %d schema paths, %d example-only paths dropped" % (
        len(cli), nopt, len(res), ", ".join("%s%s" % (r["kind"], "" if r["complete"] else "*") for r in res),
        sum(len(r["paths"]) for r in schema_entries), ex_only)


# ------------------------------------------------------------------ kubernetes
FENCE_YAML_RE = re.compile(r"^```ya?ml\s*\n(.*?)^```", re.S | re.M)
KUBECTL_RE = re.compile(r"(?<![\w-])kubectl\s+((?:[a-z][a-z-]*)(?:\s+(?:[a-z][a-z-]*))?)")
KSUB_RE = re.compile(r"(?<![\w-])kubectl\s+([a-z][a-z-]*)\b")


def build_kubernetes(pins):
    root = RAW / "kubernetes" / "content" / "en" / "docs"
    need(root / "_index.md")
    pin = pin_str(pins, "kubernetes")
    files = sorted(list((root / "concepts").rglob("*.md")) + list((root / "tasks").rglob("*.md")))
    docs = []
    tools = {}
    for f in files:
        text = f.read_text(errors="replace")
        srel = rel(f)
        got = []
        for m in FENCE_YAML_RE.finditer(text):
            body = m.group(1)
            if "{{" in body:
                continue
            try:
                got += [d for d in yaml.safe_load_all(body)]
            except yaml.YAMLError:
                continue
        if got:
            docs.append((srel, got))
        for cmd in code_lines(text):
            for km in KSUB_RE.finditer(cmd):
                sub = km.group(1)
                tool = "kubectl " + sub
                tools.setdefault(tool, {"options": {}})
                for opt in options_in_command(cmd[km.end():]):
                    add_opt(tools, tool, opt, "seen_in_example", srel)
    examples = manifest_entries(docs, pin)
    swf = rawc("kubernetes-api", "api", "openapi-spec", "swagger.json")
    spin = pin_str(pins, "kubernetes-api")
    defs = json.loads(swf.read_text())["definitions"]
    schema_entries = []
    for dname in sorted(defs):
        for g in defs[dname].get("x-kubernetes-group-version-kind", []):
            if g["kind"].endswith("List"):
                continue
            paths = flatten_schema(defs[dname], defs)
            schema_entries.append({
                "kind": g["kind"], "api_version": (g["group"] + "/" if g["group"] else "") + g["version"],
                "group": g["group"], "version": g["version"], "definition": dname,
                "complete": True, "provenance": ["exists"], "source": "openapi",
                "source_file": rel(swf), "pin": spin, "paths": dict(sorted(paths.items())),
            })
    schema_entries.sort(key=lambda e: (e["kind"], e["api_version"]))
    have = {(e["kind"], e["api_version"]): e for e in schema_entries}
    ex_only = 0
    kept = []
    for e in examples:
        c = have.get((e["kind"], e["api_version"]))
        if c:
            def below_open(q, c=c):
                segs = q.replace("[]", "").split(".")
                return any(c["paths"].get(".".join(segs[:i]), {}).get("open") or
                           c["paths"].get(".".join(segs[:i]) + "[]", {}).get("open") for i in range(1, len(segs)))
            ex_only += len([q for q in {q.replace("[]", "") for q in e["paths"]}
                            if q not in {x.replace("[]", "") for x in c["paths"]} and not below_open(q)
                            and not q.startswith("metadata")])
        else:
            kept.append(e)
    res = sorted(schema_entries + kept, key=lambda e: (e["kind"], e["api_version"], e["source"]))
    cli = finish_tools(tools, pin, lambda t, has_table: False)
    for e in cli:
        e["source_file"] = e["source_files"][0] if e["source_files"] else None
    out = header("kubernetes", pins, ["kubernetes", "kubernetes-api"],
                 "resources: every kind with a group-version-kind in api/openapi-spec/swagger.json of "
                 "Kubernetes v1.37.2 (the release the documentation pin describes), flattened to field paths "
                 "with type, enum, required and open (complete=true, provenance exists); *List kinds are left "
                 "out. A kind that the docs examples show but the schema lacks is kept from the examples "
                 "(complete=false). For a kind in the schema the examples are dropped. cli: kubectl "
                 "sub-commands with the flags seen in the docs examples; no generated kubectl reference "
                 "exists as files at the Kubernetes tag, so this part is incomplete (complete=false per command).",
                 False, "resources are complete for every kind of the v1.37.2 API schema (custom resources are "
                 "not in it); the kubectl part is examples only", 
                 complete_parts={"resources": True, "cli": False}, versions=["kubernetes v1.37.2"])
    out["resources"] = res
    out["cli"] = cli
    return out, "%d resource kinds/versions from the schema (%d paths), %d example kinds kept (incomplete), %d example-only paths dropped, %d kubectl sub-commands, %d flags seen" % (
        len(schema_entries), sum(len(r["paths"]) for r in schema_entries), len(kept), ex_only, len(cli),
        sum(len(c["options"]) for c in cli))


# ------------------------------------------------------------------ protocol errors
ERR_ROW_RE = re.compile(r"<tr><td>([A-Z0-9_]+)</td><td>(-?\d+)</td><td>(True|False)</td><td>(.*?)</td></tr>", re.S)


def parse_protocol_errors(text, source_file, pin):
    ents = []
    for m in ERR_ROW_RE.finditer(text):
        ents.append({"name": m.group(1), "code": int(m.group(2)), "retriable": m.group(3) == "True",
                     "description": clean(m.group(4)), "source_file": source_file,
                     "source_line": line_of(text, m.start()), "pin": pin, "provenance": ["exists"]})
    return ents


def build_protocol_errors(pins):
    f = need(RAW / "apache-kafka-config" / "content" / "en" / "43" / "generated" / "protocol_errors.html")
    pin = pin_str(pins, "apache-kafka-config")
    text = f.read_text()
    ents = parse_protocol_errors(text, rel(f), pin)
    rows = text.count("<tr><td>")
    if not ents or len(ents) != rows:
        raise MissingSource("protocol_errors.html: parsed %d of %d rows" % (len(ents), rows))
    ents.sort(key=lambda e: e["code"])
    out = header("protocol_errors", pins, ["apache-kafka-config"],
                 "Error names of the Kafka protocol as listed in the generated reference "
                 "(name, code, retriable, description). Java exception class names are not listed.",
                 True, "generated protocol reference of Kafka 4.3: every error code the protocol defines",
                 versions=["4.3"])
    out["entries"] = ents
    return out, "%d error names" % len(ents)


# ------------------------------------------------------------------ connect rest
HTTP_METHODS = ("get", "put", "post", "delete", "patch", "head", "options")


def parse_connect_rest(text, source_file, pin):
    doc = yaml.safe_load(text)
    lines = text.split("\n")
    ents = []
    for path, ops in sorted(doc["paths"].items()):
        pi = next(i for i, l in enumerate(lines) if l == "  %s:" % path)
        for method in HTTP_METHODS:
            if method not in ops:
                continue
            mi = next(i for i in range(pi + 1, len(lines)) if lines[i] == "    %s:" % method)
            op = ops[method]
            ents.append({"method": method.upper(), "path": path, "operation_id": op.get("operationId"),
                         "summary": " ".join(str(op.get("summary", "")).split()),
                         "path_params": [p["name"] for p in op.get("parameters", []) if p.get("in") == "path"],
                         "source_file": source_file, "source_line": mi + 1, "pin": pin,
                         "provenance": ["exists"]})
    return ents


def build_connect_rest(pins):
    f = need(RAW / "apache-kafka-config" / "content" / "en" / "43" / "generated" / "connect_rest.yaml")
    pin = pin_str(pins, "apache-kafka-config")
    ents = parse_connect_rest(f.read_text(), rel(f), pin)
    if not ents:
        raise MissingSource("no operations parsed from %s" % f)
    out = header("connect_rest", pins, ["apache-kafka-config"],
                 "Method and path pairs of the Kafka Connect REST interface (OpenAPI generated from the "
                 "worker's resources).", True,
                 "generated OpenAPI document of the Connect worker: every path and method it serves",
                 versions=["4.3"])
    out["entries"] = ents
    return out, "%d operations on %d paths" % (len(ents), len({e["path"] for e in ents}))


# ------------------------------------------------------------------ main
BUILDERS = [
    ("kafka_config", build_kafka_config, ["apache-kafka-config"]),
    ("strimzi_resources", build_strimzi_resources, ["strimzi", "strimzi-install"]),
    ("strimzi_annotations", build_strimzi_annotations, ["strimzi"]),
    ("kafka_cli", build_kafka_cli, ["apache-kafka", "strimzi"]),
    ("argocd", build_argocd, ["argo-cd", "argocd-crds"]),
    ("kubernetes", build_kubernetes, ["kubernetes", "kubernetes-api"]),
    ("kafka_config_history", build_kafka_config_history, ["apache-kafka-config", "kafka-config-pages"]),
    ("protocol_errors", build_protocol_errors, ["apache-kafka-config"]),
    ("connect_rest", build_connect_rest, ["apache-kafka-config"]),
]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", default=str(DEFAULT_OUT), help="output directory")
    args = ap.parse_args(argv)
    out_dir = Path(args.out)
    try:
        pins = load_pins()
        for _, _, srcs in BUILDERS:
            for s in srcs:
                check_checkout(pins, s)
        results = [(n, fn(pins)) for n, fn, _ in BUILDERS]
    except (MissingSource, FileNotFoundError) as e:
        print("FAIL: %s" % e, file=sys.stderr)
        return 2
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, (obj, summary) in results:
        dump(out_dir / (name + ".json"), obj)
        print("%-20s %s" % (name + ".json", summary))
    man = manifest_for(out_dir)
    dump(out_dir / "CATALOGUES.json", man)
    print("%-20s catalogue hash %s" % ("CATALOGUES.json", man["catalogue_hash"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
