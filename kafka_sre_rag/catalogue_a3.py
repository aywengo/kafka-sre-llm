"""Read-only loader of the a3 catalogues (work/m4/catalogues_a3/, built by scripts/build_catalogues_a3.py).

  forbidden(kind, key, config_path=None)   the rule that forbids `key` in the config map of `kind`, or None
  semantic_rules()                          the semantic-rule table (mechanisms V1..V8, memory quantities)
  verify(directory=None)                    recompute the hashes against MANIFEST.json

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
FILES = ("strimzi_forbidden", "semantic_rules")

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
