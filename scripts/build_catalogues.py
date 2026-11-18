#!/usr/bin/env python3
"""Build catalogues of real identifiers from the pinned sources in `raw/`.

Output: JSON files in work/m4/catalogues/ (git-ignored), one per catalogue:

  kafka_config.json        config keys per scope (generated reference, kafka-site)
  strimzi_resources.json   field paths per Strimzi kind (generated API reference, appendix_crds.adoc)
  strimzi_annotations.json strimzi.io/ annotations and labels as the documentation uses them
  kafka_cli.json           kafka-*.sh options: options table or seen in an example
  argocd.json              argocd CLI (generated reference) and example manifests
  kubernetes.json          kubectl commands and manifest fields seen in the docs examples

Reads only files already on disk. No network, no git commands, nothing under raw/ is changed.
Deterministic: running it twice gives byte-identical files. Fails loudly (exit 2) when a
source file it expects is absent. Pins come from sources.yaml and runs/v3/INDEX_FROZEN.json.

Usage: .venv/bin/python scripts/build_catalogues.py [--out DIR]
"""
import argparse
import html
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

VERSION = "build_catalogues/1"
ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw"
DEFAULT_OUT = ROOT / "work" / "m4" / "catalogues"


class MissingSource(Exception):
    pass


# ------------------------------------------------------------------ pins and helpers
def load_pins():
    src = yaml.safe_load((ROOT / "sources.yaml").read_text())["sources"]
    frozen = json.loads((ROOT / "runs" / "v3" / "INDEX_FROZEN.json").read_text())
    commits = frozen["source_commits"]
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
    return pins


def pin_str(pins, name):
    p = pins[name]
    return "%s@%s#%s" % (name, p["ref"], p["commit"][:12])


def check_checkout(pins, name):
    """The checkout on disk must be the pinned commit (read .git/HEAD, no git command)."""
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
    return str(Path(path).resolve().relative_to(ROOT))


def dump(path, obj):
    path.write_text(json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


def header(name, pins, used, note):
    return {
        "catalogue": name,
        "generator": VERSION,
        "note": note,
        "sources": {n: pins[n] for n in sorted(used)},
    }


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
        e = {"scope": scope, "key": key, "source_file": source_file, "pin": pin}
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
                 "Prefixed forms (producer.override.*, listener.name.*) are not listed.")
    out["entries"] = entries
    counts = defaultdict(int)
    for e in entries:
        counts[e["scope"]] += 1
    return out, "%d keys in %d scopes (%s)" % (
        len(entries), len(counts), ", ".join("%s %d" % kv for kv in sorted(counts.items())))


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
            d = {"type": sorted(r["type"]), "declared_in": sorted(r["declared_in"])}
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
            "source": "api_reference",
            "source_file": rel(f),
            "pin": pin,
            "paths": paths,
        })
    out = header("strimzi_resources", pins, ["strimzi"],
                 "From the generated API reference (appendix_crds.adoc): the CRD YAML files are "
                 "NOT on disk (only documentation/ is checked out). required_hint is true when the "
                 "description says 'Required.'; the reference has no machine-readable required list. "
                 "Subtype fields (discriminated by 'type') are merged into one set per path. "
                 "Paths below an 'open' field (free-form map, e.g. config) are not enumerated.")
    out["entries"] = kinds
    total = sum(len(k["paths"]) for k in kinds)
    return out, "%d kinds, %d field paths (%s)" % (
        len(kinds), total, ", ".join("%s %d" % (k["kind"], len(k["paths"])) for k in kinds))


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
        })
    out = header("strimzi_annotations", pins, ["strimzi"],
                 "Names with the prefix strimzi.io/ found in the Strimzi documentation. 'kind' is "
                 "decided from the surrounding text and is a heuristic. A name that is absent here "
                 "is not proof of invention: operator-applied labels and annotations are only "
                 "listed when the docs mention them.")
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
            opts[opt] = {"evidence": o["evidence"], "files": sorted(o["files"]), "seen": o["seen"]}
        has_table = any(o["evidence"] in ("options_table",) for o in opts.values())
        res.append({
            "tool": tool,
            "complete": complete_when(t, has_table),
            "options": opts,
            "source_files": sorted({f for o in opts.values() for f in o["files"]}),
            "pin": pin,
        })
    return res


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
    out = header("kafka_cli", pins, ["apache-kafka", "strimzi"],
                 "complete is true only for a tool whose full options table is on disk; for the "
                 "others an option that is not listed is NOT proof that it does not exist.")
    out["entries"] = entries
    full = [e["tool"] for e in entries if e["complete"]]
    return out, "%d tools, %d options (%d with an options table: %s)" % (
        len(entries), sum(len(e["options"]) for e in entries), len(full), ", ".join(full) or "-")


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
            "source_file": sorted(e["files"])[0], "source_files": sorted(e["files"]), "pin": pin,
            "paths": {p: {"type": sorted(r["type"]), "files": sorted(r["files"])}
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
    for f in cmd_files + srv_files:
        cmd, own, inh = parse_cobra_md(f.read_text())
        if not cmd:
            continue
        t = tools.setdefault(cmd, {"options": {}})
        for o in own:
            add_opt(tools, cmd, o, "options_table", rel(f))
        for o in inh:
            add_opt(tools, cmd, o, "options_table_inherited", rel(f))
    cli = finish_tools(tools, pin, lambda t, has_table: True)
    for e in cli:
        e["source_file"] = e["source_files"][0] if e["source_files"] else None
    # example manifests
    mf = [need(root / "operator-manual" / n) for n in ("application.yaml", "project.yaml", "applicationset.yaml")]
    docs = []
    for f in mf:
        try:
            docs.append((rel(f), [d for d in yaml.safe_load_all(f.read_text())]))
        except yaml.YAMLError as ex:
            raise MissingSource("cannot parse %s: %s" % (f, ex))
    res = manifest_entries(docs, pin)
    out = header("argocd", pins, ["argo-cd"],
                 "cli: every command of the generated reference, options complete (own and "
                 "inherited). resources: field paths seen in the fully commented example manifests "
                 "under docs/operator-manual (not a schema; complete=false). The CRD YAML files "
                 "are not on disk.")
    out["cli"] = cli
    out["resources"] = res
    nopt = len({o for e in cli for o in e["options"]})
    return out, "%d commands (%d distinct flags), %d resource kinds (%s), %d example paths" % (
        len(cli), nopt, len(res), ", ".join(r["kind"] for r in res), sum(len(r["paths"]) for r in res))


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
    res = manifest_entries(docs, pin)
    cli = finish_tools(tools, pin, lambda t, has_table: False)
    for e in cli:
        e["source_file"] = e["source_files"][0] if e["source_files"] else None
    out = header("kubernetes", pins, ["kubernetes"],
                 "Only the docs prose (concepts, tasks) is on disk; the API reference, the OpenAPI "
                 "schema and the kubectl reference are not. This catalogue holds what the examples "
                 "show: kinds with the field paths seen, and kubectl sub-commands with the flags "
                 "seen. complete=false everywhere: a missing item is not proof of invention.")
    out["resources"] = res
    out["cli"] = cli
    return out, "%d resource kinds, %d example paths, %d kubectl sub-commands, %d flags seen" % (
        len(res), sum(len(r["paths"]) for r in res), len(cli), sum(len(c["options"]) for c in cli))


# ------------------------------------------------------------------ main
BUILDERS = [
    ("kafka_config", build_kafka_config, ["apache-kafka-config"]),
    ("strimzi_resources", build_strimzi_resources, ["strimzi"]),
    ("strimzi_annotations", build_strimzi_annotations, ["strimzi"]),
    ("kafka_cli", build_kafka_cli, ["apache-kafka", "strimzi"]),
    ("argocd", build_argocd, ["argo-cd"]),
    ("kubernetes", build_kubernetes, ["kubernetes"]),
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
