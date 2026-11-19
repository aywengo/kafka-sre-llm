"""Read-only loader of the a3 catalogues (work/m4/catalogues_a3/, built by scripts/build_catalogues_a3.py).

  forbidden(kind, key, config_path=None)   the rule that forbids `key` in the config map of `kind`, or None
  semantic_rules()                          the semantic-rule table (mechanisms V1..V8, memory quantities)
  verify(directory=None)                    recompute the hashes against MANIFEST.json
  java_api(directory=None)                  (T1b) the Java API of the pinned kafka-clients / kafka-streams jars
  java_class(fqcn), java_methods(fqcn, name, params=None), java_constant(fqcn, name),
  client_config_names(scope)                lookups in it; None / [] / {} when absent
  python_symbols(directory=None)            (T2) public names of the pinned confluent-kafka wheel, or None
  py_module_names(module), py_class(fq), py_method(fq, name), py_config_properties()
                                            lookups in it; None / {} / [] when absent

A key is forbidden under a config path when it equals a forbidden key, or starts with a forbidden prefix
and is not an exception. An exception equals the key, or ends with "." and is a prefix of the key; an
exception glob ("listener.name.*.connections.max.reauth.ms") has "*" for any run of characters.
Without `config_path` the primary path of the kind is used (Kafka: spec.kafka.config; KafkaBridge:
spec.config; KafkaMirrorMaker2: spec.target.config). Other paths are given as `config_path`. An unknown
kind returns None: the table says nothing about it. The catalogue is not part of work/m4/catalogues/.
"""
import fnmatch
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DIR = ROOT / "work" / "m4" / "catalogues_a3"
FILES = ("strimzi_forbidden", "semantic_rules", "java_api", "python_symbols")

_loaded = {}


def default_dir():
    return DEFAULT_DIR


def _read(directory, name):
    d = Path(directory)
    k = (str(d), name)
    if k not in _loaded:
        _loaded[k] = json.loads((d / (name + ".json")).read_text(encoding="utf-8"))
    return _loaded[k]


def manifest(directory=None):
    d = Path(directory or DEFAULT_DIR)
    files = {}
    for n in FILES:
        f = d / (n + ".json")
        if f.is_file():
            files[n + ".json"] = hashlib.sha256(f.read_bytes()).hexdigest()
    whole = hashlib.sha256("".join("%s %s\n" % kv for kv in files.items()).encode()).hexdigest()
    return {"manifest": "catalogues_a3/1", "files": files, "catalogue_hash": whole}


def verify(directory=None):
    """True when the files on disk have the hashes recorded in MANIFEST.json."""
    d = Path(directory or DEFAULT_DIR)
    rec = json.loads((d / "MANIFEST.json").read_text(encoding="utf-8"))
    return manifest(d) == rec


def semantic_rules(directory=None):
    return _read(directory or DEFAULT_DIR, "semantic_rules")


def resources(directory=None):
    return _read(directory or DEFAULT_DIR, "strimzi_forbidden")["resources"]


def _entry(res, kind, config_path):
    if config_path is None:
        return res.get(kind)
    prim = res.get(kind)
    if prim is not None and prim["config_path"] == config_path:
        return prim
    return res.get("%s:%s" % (kind, config_path))


def _anchor_for(e, role, item):
    for a in e["anchors"]:
        if a["role"] == role and item in a["items"]:
            return a["file"], a["line"]
    return e["source"].rsplit(":", 1)[0], None


def _excepted(e, key):
    for x in e["exceptions"]:
        if key == x or (x.endswith(".") and key.startswith(x)):
            return x
    for g in e.get("exception_globs", []):
        if fnmatch.fnmatchcase(key, g):
            return g
    return None


def forbidden(kind, key, config_path=None, directory=None):
    """The matching rule as a dict, or None when `key` may be set (or the kind is not in the table).
    Keys: kind, config_path, rule ("key" or "prefix"), match, file, line, source."""
    e = _entry(resources(directory), kind, config_path)
    if e is None:
        return None
    hit = None
    if key in e["forbidden_keys"]:
        hit = ("key", key, "forbidden_keys")
    else:
        for p in e["forbidden_prefixes"]:
            if key.startswith(p):
                hit = ("prefix", p, "forbidden_prefixes")
                break
    if hit is None or _excepted(e, key) is not None:
        return None
    f, ln = _anchor_for(e, hit[2], hit[1])
    return {"kind": kind, "config_path": e["config_path"], "rule": hit[0], "match": hit[1],
            "file": f, "line": ln, "source": e["source"]}


# ------------------------------------------------------------------ Java API (T1b), additive
def java_api(directory=None):
    """The java_api.json table, or None when it was not built (jars not fetched)."""
    d = Path(directory or DEFAULT_DIR)
    if not (d / "java_api.json").is_file():
        return None
    return _read(d, "java_api")


def java_class(fqcn, directory=None):
    """The record of a public class (nested classes use '$': 'a.b.Outer$Inner'), or None."""
    j = java_api(directory)
    return None if j is None else j["classes"].get(fqcn)


def java_methods(fqcn, name, params=None, directory=None):
    """The public methods `name` of the class; `params` (a list of type strings) narrows to one signature."""
    c = java_class(fqcn, directory)
    if c is None:
        return []
    return [m for m in c["methods"] if m["name"] == name and (params is None or m["params"] == list(params))]


def java_constant(fqcn, name, directory=None):
    """The value of a public compile-time constant (string constants as str), or None."""
    c = java_class(fqcn, directory)
    if c is None or name not in c["constants"]:
        return None
    return c["constants"][name]["value"]


def client_config_names(scope, directory=None):
    """{config name: {constant, source, line}} for scope consumer, producer, admin, streams or common."""
    j = java_api(directory)
    return {} if j is None else j["client_config_names"].get(scope, {})


# ------------------------------------------------------------------ Python client symbols (T2), additive
def python_symbols(directory=None):
    """The python_symbols.json table (confluent-kafka wheel), or None when it was not built."""
    d = Path(directory or DEFAULT_DIR)
    if not (d / "python_symbols.json").is_file():
        return None
    return _read(d, "python_symbols")


def py_module_names(module, directory=None):
    """The public names of a module ('confluent_kafka', 'confluent_kafka.admin', ...), or None when unknown."""
    p = python_symbols(directory)
    m = None if p is None else p["modules"].get(module)
    return None if m is None else m["names"]


def py_class(fq, directory=None):
    """{'members': [...], 'methods': {name: {params, pos_only, var_pos, var_kw, src} or None}} of a class, or None."""
    p = python_symbols(directory)
    return None if p is None else p["classes"].get(fq)


def py_method(fq, name, directory=None):
    """The signature record of a method of a class (None when the class or the method is unknown, or the
    signature is not known: the record is then {} for a known method without a signature)."""
    c = py_class(fq, directory)
    if c is None or name not in c["members"]:
        return None
    return c["methods"].get(name) or {}


def py_config_properties(directory=None):
    """The librdkafka configuration property names the wheel accepts (sorted list), or [] when absent."""
    p = python_symbols(directory)
    return [] if p is None else p["config_properties"]
