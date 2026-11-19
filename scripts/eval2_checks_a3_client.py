#!/usr/bin/env python3
"""Client-code checks of the a3 slice of tier A (WP1c T2): Java and Python answers, checked as text.

`CHECKS_A3_CLIENT` is merged into `CHECKS_A3` of scripts/eval2_checks_a3.py (one line at its end) and so into
`CHECKS` of scripts/eval2_score.py. Every check has the signature `fn(chk, text, cats) -> (ok, detail)` of the
existing checks. A detail that is a failure of the environment or of the template, not of the answer, starts with a
distinct reason word (below).

NOTHING THE ANSWER CONTAINS IS EVER RUN. Java source is only compiled (`javac -proc:none`, temporary folder, a
timeout) and attributed (`scripts/javacheck/ListCalls.java`, the Compiler Tree API, which reads the source and the
two jars and lists the resolved calls); no class file is loaded or executed. Python source is only parsed (`ast`).

Java checks (jars: raw_catalogue/maven/kafka-clients-4.3.2.jar and kafka-streams-4.3.2.jar, size and sha256 checked
once per process against scripts/fetch_maven_jars.py; no other jar, no network):
  java_compiles    the java fences of the answer compile together against the two jars.
                   spec: {"timeout": 30}. Reason words of a failure: java_no_block, java_no_class, java_timeout,
                   java_template_rocksdb (the Streams snippet needs org.rocksdb, which is not in the two jars: a problem
                   of the template, the generator must avoid it, not an answer failure), java_compile_error,
                   java_unavailable (no javac or no jars on this machine).
  java_calls       calls the item requires are present, resolved to the right class and signature, and calls it
                   forbids are absent. spec: {"required": [E...], "forbidden": [E...]} with E =
                   {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitSync",
                    "params": ["java.time.Duration"]}; "class" matches the declaring class or the static type of the
                   receiver, "method" "<init>" is a constructor, "params" (erased binary names, optional) is the
                   resolved signature. A call javac could not resolve matches nothing.
  java_call_order spec: {"order": [E, E, ...]}: the calls occur in this order (a subsequence of the calls in
                   evaluation order: receiver chain, arguments, the call).
  java_config      the configuration the program sets through put / setProperty / putIfAbsent on a Properties or
                   Map, Map.of and Map.entry: spec {"keys": {"enable.auto.commit": "false", "group.id": null},
                   "forbidden": {"enable.auto.commit": "true"}, "scope": ["consumer"]}. A key may be a `*Config.*_CONFIG`
                   constant (javac resolves it) or a string. A key not in the Java catalogue's names (work/m4/catalogues_a3/
                   java_api.json: consumer, producer, admin, streams, common and topic names; Streams prefixes producer.,
                   consumer., main.consumer., restore.consumer., global.consumer., admin. stripped) is an invented key; an
                   invented `*_CONFIG` constant is reported from javac's "cannot find symbol".

Python checks (symbols: work/m4/catalogues_a3/python_symbols.json, the confluent-kafka wheel, WP1c T2 / D2):
  py_syntax        every python fence parses with ast.parse.
  py_imports       names imported from confluent_kafka (and its submodules) exist in the symbol list; attribute
                   chains on the module (confluent_kafka.Consumer) too. spec: {"require": ["Consumer"]}.
  py_calls         required calls present (and in order), forbidden calls absent; a method called on a variable
                   whose class is known (x = Consumer(...)) must exist in that class and take the keywords and the
                   number of positional arguments given. spec: {"required": [{"name": "poll", "class": "Consumer",
                   "kwargs": ["timeout"], "min_args": 0}], "forbidden": [...], "order": [...], "unknown_methods": true}.
                   A receiver of unknown class matches an entry that names a class (it cannot be disproved).
  py_config        the librdkafka-style configuration dicts passed to the client classes: spec {"keys": {...},
                   "forbidden": {...}}; a key the wheel's property table does not know is an invented key. The
                   wheel carries no list file: the table is the one compiled into librdkafka, read by asking its
                   parser (scripts/build_catalogues_a3.py). When python_symbols.json has no property table only names
                   of imports and calls are checked and the detail says so.
"""
import ast
import hashlib
import importlib.util
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from kafka_sre_rag import catalogue_a3  # noqa: E402

MAVEN = ROOT / "raw_catalogue" / "maven"
JAR_NAMES = ("kafka-clients-4.3.2.jar", "kafka-streams-4.3.2.jar")
LISTCALLS = ROOT / "scripts" / "javacheck" / "ListCalls.java"
CACHE = ROOT / "work" / "m4" / "javacheck_cache"
JAVAC = "javac"            # tests replace these to show the timeout and the unavailable reasons
JAVA = "java"
DEFAULT_TIMEOUT = 30
RELEASE = "17"

FENCE_RE = re.compile(r"```[ \t]*([A-Za-z0-9_+-]*)[^\n]*\n(.*?)```", re.S)
TYPE_DECL_RE = re.compile(r"\b(?:class|interface|enum|record)\s+([A-Za-z_]\w*)")
PUBLIC_TYPE_RE = re.compile(r"\bpublic\s+(?:(?:final|abstract|sealed|strictfp)\s+)*(?:class|interface|enum|record)\s+([A-Za-z_]\w*)")


def blocks(text, langs, fallback_unlabeled=True):
    """Bodies of the code fences labelled with one of `langs`; if there is none, the unlabeled fences."""
    fences = [(m.group(1).lower(), m.group(2)) for m in FENCE_RE.finditer(text or "")]
    out = [b for lang, b in fences if lang in langs]
    if not out and fallback_unlabeled:
        out = [b for lang, b in fences if lang == ""]
    return out


# ============================================================== Java: the jars, javac, ListCalls
_jar_state = {}


def _fetch_module():
    spec = importlib.util.spec_from_file_location("fetch_maven_jars", ROOT / "scripts" / "fetch_maven_jars.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def jar_paths():
    """(paths, None) when both jars are present with the pinned size and sha256, else (None, reason). Cached."""
    if "r" not in _jar_state:
        fj = _fetch_module()
        paths = []
        for n in JAR_NAMES:
            p = MAVEN / n
            if not p.is_file():
                _jar_state["r"] = (None, "java_unavailable: %s is not fetched (scripts/fetch_maven_jars.py)" % n)
                break
            why = fj.check(p, fj.JARS[n])
            if why:
                _jar_state["r"] = (None, "java_unavailable: %s: %s" % (n, why))
                break
            paths.append(str(p))
        else:
            _jar_state["r"] = (paths, None)
    return _jar_state["r"]


def _run(cmd, cwd, timeout):
    """Run a command with a minimal environment in its own process group; kill the group on timeout.
    Returns (returncode, stdout, stderr) or raises subprocess.TimeoutExpired."""
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(cwd), "LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"}
    if os.environ.get("JAVA_HOME"):
        env["JAVA_HOME"] = os.environ["JAVA_HOME"]
    proc = subprocess.Popen(cmd, cwd=str(cwd), env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
        raise
    return proc.returncode, out, err


def java_sources(text, tmp):
    """Write the java fences of an answer as source files into tmp. Returns (paths, None) or ([], reason)."""
    bl = blocks(text, ("java",))
    if not bl:
        return [], "java_no_block: no java code fence in the answer"
    paths = []
    for i, b in enumerate(bl):
        if not TYPE_DECL_RE.search(b):
            return [], "java_no_class: the java block %d declares no class, interface, enum or record" % (i + 1)
        m = PUBLIC_TYPE_RE.search(b)
        name = m.group(1) if m else "Snippet%d" % (i + 1)
        p = Path(tmp) / "src" / (name + ".java")
        p.parent.mkdir(exist_ok=True)
        if p.exists():
            return [], "java_compile_error: two java blocks declare the public type %s" % name
        p.write_text(b, encoding="utf-8")
        paths.append(str(p))
    return paths, None


def javac_command(jars, outdir, files):
    return [JAVAC, "-proc:none", "--release", RELEASE, "-nowarn", "-Xlint:none", "-implicit:none", "-encoding", "UTF-8",
            "-sourcepath", "", "-cp", os.pathsep.join(jars), "-d", str(outdir)] + list(files)


def _first_error(stderr):
    lines = (stderr or "").splitlines()
    idx = next((i for i, l in enumerate(lines) if ": error:" in l or l.startswith("error:")), None)
    if idx is None:
        first = (stderr or "").strip()[:200]
    else:
        first = lines[idx].strip()
        sym = next((l.strip() for l in lines[idx + 1:idx + 4] if l.strip().startswith("symbol:")), None)
        if sym:
            first += " (" + re.sub(r"\s+", " ", sym) + ")"
    return re.sub(r"^.*?([A-Za-z0-9_]+\.java:)", r"\1", first)          # no temporary folder in the detail


def check_java_compiles(chk, text, cats):
    jars, why = jar_paths()
    if why:
        return False, why
    timeout = float(chk.get("timeout", DEFAULT_TIMEOUT))
    with tempfile.TemporaryDirectory(prefix="a3java_") as tmp:
        files, why = java_sources(text, tmp)
        if why:
            return False, why
        out = Path(tmp) / "classes"
        out.mkdir()
        try:
            rc, _, err = _run(javac_command(jars, out, files), tmp, timeout)
        except subprocess.TimeoutExpired:
            return False, "java_timeout: javac did not finish in %g s" % timeout
        except FileNotFoundError:
            return False, "java_unavailable: %s not found" % JAVAC
    if rc == 0:
        return True, "compiles against kafka-clients and kafka-streams 4.3.2 (%d file%s)" % (len(files), "" if len(files) == 1 else "s")
    if re.search(r"cannot access (?:org\.rocksdb|\w*Rocks\w*)|class file for org\.rocksdb", err):
        return False, ("java_template_rocksdb: the snippet needs org.rocksdb, which the two jars do not carry; "
                       "a problem of the template, not of the answer (%s)" % _first_error(err))
    return False, "java_compile_error: " + _first_error(err)


# ---- ListCalls, compiled once into work/m4/javacheck_cache/<sha256 of the source>/
def listcalls_dir():
    """Directory holding ListCalls.class, compiled on first use. Returns (dir, None) or (None, reason)."""
    sha = hashlib.sha256(LISTCALLS.read_bytes()).hexdigest()
    d = CACHE / sha
    if (d / "ListCalls.class").is_file():
        return d, None
    CACHE.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="lc_", dir=str(CACHE)))
    try:
        try:
            rc, _, err = _run([JAVAC, "-proc:none", "-d", str(tmp), str(LISTCALLS)], tmp, 120)
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            return None, "java_unavailable: cannot compile ListCalls (%s)" % type(e).__name__
        if rc:
            return None, "java_unavailable: ListCalls does not compile: " + _first_error(err)
        try:
            tmp.rename(d)
        except OSError:                                  # another process was first
            shutil.rmtree(tmp, ignore_errors=True)
    finally:
        if tmp.exists() and not d.exists():
            shutil.rmtree(tmp, ignore_errors=True)
    return d, None


def list_calls(text, timeout=None):
    """(info, None) with info = {"calls": [...], "errors": [...], "unresolved": [...]}, or (None, reason)."""
    import json
    jars, why = jar_paths()
    if why:
        return None, why
    cdir, why = listcalls_dir()
    if why:
        return None, why
    timeout = float(timeout or DEFAULT_TIMEOUT)
    with tempfile.TemporaryDirectory(prefix="a3java_") as tmp:
        files, why = java_sources(text, tmp)
        if why:
            return None, why
        try:
            rc, out, err = _run([JAVA, "-cp", str(cdir), "ListCalls", os.pathsep.join(jars)] + files, tmp, timeout)
        except subprocess.TimeoutExpired:
            return None, "java_timeout: ListCalls did not finish in %g s" % timeout
        except FileNotFoundError:
            return None, "java_unavailable: %s not found" % JAVA
    if rc:
        return None, "java_unavailable: ListCalls failed: " + (err or "").strip()[:200]
    info = {"calls": [], "errors": [], "unresolved": []}
    for line in out.splitlines():
        rec = json.loads(line)
        t = rec.pop("t")
        info[{"call": "calls", "err": "errors", "unresolved": "unresolved"}[t]].append(rec)
    # the implicit super() of a constructor is not a call the answer wrote
    info["calls"] = [c for c in info["calls"] if not (c["name"] == "<init>" and c["owner"] == "java.lang.Object")]
    return info, None


def _erase(t):
    """Erase generics and normalise varargs in a catalogue or spec type string."""
    t = t.strip().replace("...", "[]")
    prev = None
    while prev != t:
        prev = t
        t = re.sub(r"<[^<>]*>", "", t)
    return t


def _class_matches(cls, c, directory=None):
    """The call is on the class `cls`: it is the declaring class or the static type of the receiver, or the call
    resolved to a supertype of `cls` (an interface or superclass the class implements; `Consumer<K, V> c = new
    KafkaConsumer<>(..); c.poll(..)` is a call of KafkaConsumer.poll). Subtypes come from the Java catalogue."""
    if cls in (c["owner"], c["receiver"]):
        return True
    if c["name"] == "<init>":
        return False
    return any(t != "java.lang.Object" and catalogue_a3.java_is_subtype(cls, t, directory) for t in (c["owner"], c["receiver"]))


def _entry_matches(e, c, directory=None):
    cls = e.get("class")
    if cls and not _class_matches(cls, c, directory):
        return False
    if e.get("method") and e["method"] != c["name"]:
        return False
    if e.get("params") is not None and [_erase(p) for p in e["params"]] != c["params"]:
        return False
    return True


def _describe(e):
    return "%s.%s(%s)" % ((e.get("class") or "*").rsplit(".", 1)[-1], e.get("method", "*"),
                          "*" if e.get("params") is None else ", ".join(_erase(p).rsplit(".", 1)[-1] for p in e["params"]))


def _java_info(chk, text):
    info, why = list_calls(text, chk.get("timeout"))
    if why:
        return None, (False, why)
    return info, None


def _note_errors(info):
    return " [%d compile error%s]" % (len(info["errors"]), "" if len(info["errors"]) == 1 else "s") if info["errors"] else ""


def check_java_calls(chk, text, cats):
    info, bad = _java_info(chk, text)
    if bad:
        return bad
    calls = info["calls"]
    d = chk.get("catalogues_a3")
    missing = [_describe(e) for e in chk.get("required", []) if not any(_entry_matches(e, c, d) for c in calls)]
    present = [_describe(e) for e in chk.get("forbidden", []) if any(_entry_matches(e, c, d) for c in calls)]
    if missing or present:
        hint = ""
        if info["unresolved"]:
            hint = "; javac could not resolve: " + ", ".join(sorted({u["name"] for u in info["unresolved"]})[:4])
        return False, ("missing call %s" % ", ".join(missing) if missing else "") + \
            ("; " if missing and present else "") + ("forbidden call present %s" % ", ".join(present) if present else "") + hint
    return True, "%d required call%s present, resolved%s" % (len(chk.get("required", [])), "" if len(chk.get("required", [])) == 1 else "s",
                                                           _note_errors(info))


def ordered_subsequence(entries, calls, matches):
    """(ok, detail): the entries match calls at increasing positions (earliest match each)."""
    pos = -1
    for i, e in enumerate(entries):
        nxt = next((j for j in range(pos + 1, len(calls)) if matches(e, calls[j])), None)
        if nxt is None:
            anywhere = any(matches(e, c) for c in calls)
            name = _describe(e) if matches is not _py_entry_matches else str(e.get("name"))
            return False, ("%s is called, but not after %s" % (name, _describe(entries[i - 1]) if matches is not _py_entry_matches
                                                                 else str(entries[i - 1].get("name")))
                           if anywhere and i else "%s is never called" % name)
        pos = nxt
    return True, "calls occur in the required order"


def check_java_call_order(chk, text, cats):
    info, bad = _java_info(chk, text)
    if bad:
        return bad
    d = chk.get("catalogues_a3")
    ok, detail = ordered_subsequence(chk.get("order", []), info["calls"], lambda e, c: _entry_matches(e, c, d))
    return ok, detail + (_note_errors(info) if ok else "")


# ---- java_config
PUT_NAMES = {"put", "setProperty", "putIfAbsent"}
STREAMS_PREFIXES = ("main.consumer.", "restore.consumer.", "global.consumer.", "producer.", "consumer.", "admin.")
TOPIC_CONFIG = "org.apache.kafka.common.config.TopicConfig"


def _argval(a):
    k = a.get("k")
    if k in ("s", "n", "b", "c"):
        return a["v"]
    return None


def java_config_pairs(calls):
    """[(key, value, line, resolvable)] in evaluation order. Keys and values are constant strings or None."""
    pairs = []
    for c in calls:
        o = c["owner"]
        if c["name"] in PUT_NAMES and len(c["args"]) == 2 and (o.startswith("java.util.") or o.startswith("java.lang.")):
            pairs.append((_argval(c["args"][0]), _argval(c["args"][1]), c["line"]))
        elif o == "java.util.Map" and c["name"] == "of" and len(c["args"]) % 2 == 0 and c["args"]:
            for i in range(0, len(c["args"]), 2):
                pairs.append((_argval(c["args"][i]), _argval(c["args"][i + 1]), c["line"]))
        elif o == "java.util.Map" and c["name"] == "entry" and len(c["args"]) == 2:
            pairs.append((_argval(c["args"][0]), _argval(c["args"][1]), c["line"]))
    return pairs


def java_config_names(scopes, directory=None):
    names = set()
    for s in scopes:
        if s == "topic":
            c = catalogue_a3.java_class(TOPIC_CONFIG, directory)
            for cn, v in ((c or {}).get("constants") or {}).items():
                if cn.endswith("_CONFIG") and v.get("type") == "java.lang.String":
                    names.add(v["value"])
        else:
            names.update(catalogue_a3.client_config_names(s, directory))
    return names


PREFIX_SCOPE = {"main.consumer.": "consumer", "restore.consumer.": "consumer", "global.consumer.": "consumer",
                "producer.": "producer", "consumer.": "consumer", "admin.": "admin"}


def _known_key(k, names, streams, directory=None):
    if k in names:
        return True
    if streams:
        for p, scope in PREFIX_SCOPE.items():
            if k.startswith(p) and k[len(p):] in java_config_names([scope, "common"], directory):
                return True
        if k.startswith("topic.") and k[len("topic."):] in java_config_names(["topic"], directory):
            return True
    return False


def _same(a, b):
    return str(a).strip().lower() == str(b).strip().lower()


def check_java_config(chk, text, cats):
    info, bad = _java_info(chk, text)
    if bad:
        return bad
    directory = chk.get("catalogues_a3")
    if catalogue_a3.java_api(directory) is None:
        return False, "java_unavailable: java_api.json is not built (scripts/build_catalogues_a3.py)"
    scopes = chk.get("scope") or ["consumer", "producer", "admin", "streams", "common", "topic"]
    names = java_config_names(scopes + ["common"] if "common" not in scopes else scopes, directory)
    streams = "streams" in scopes
    problems = []
    invented_consts = sorted({m for e in info["errors"] for m in re.findall(r"symbol:\s+variable\s+(\w+_CONFIG)\b", e["msg"])})
    if invented_consts:
        problems.append("invented config constant %s" % ", ".join(invented_consts))
    final = {}
    for k, v, line in java_config_pairs(info["calls"]):
        if k is None:
            continue
        final[k] = v
        if re.fullmatch(r"[a-z][a-z0-9_.]*", k) and not _known_key(k, names, streams, directory):
            problems.append("unknown config key %r (line %s)" % (k, line))
    for k, want in (chk.get("keys") or {}).items():
        if k not in final:
            problems.append("config key %s is not set" % k)
        elif want is not None and (final[k] is None or not _same(final[k], want)):
            problems.append("config key %s is %s, wanted %s" % (k, "not a constant" if final[k] is None else repr(final[k]), want))
    for k, bad_v in (chk.get("forbidden") or {}).items():
        if k in final and (bad_v is None or (final[k] is not None and _same(final[k], bad_v))):
            problems.append("config key %s must not be %s" % (k, "set" if bad_v is None else repr(bad_v)))
    if problems:
        return False, "; ".join(dict.fromkeys(problems))[:400]
    return True, "%d config key%s set, all known%s" % (len(final), "" if len(final) == 1 else "s", _note_errors(info))


# ============================================================== Python
PY_LANGS = ("python", "py", "python3")
CK = "confluent_kafka"


def py_source(text):
    """(sources, reason): the python fences of the answer."""
    bl = blocks(text, PY_LANGS)
    return (bl, None) if bl else ([], "py_no_block: no python code fence in the answer")


def check_py_syntax(chk, text, cats):
    bl, why = py_source(text)
    if why:
        return False, why
    for i, b in enumerate(bl):
        try:
            ast.parse(b, filename="<block %d>" % (i + 1))
        except SyntaxError as e:
            return False, "py_syntax_error: block %d line %s: %s" % (i + 1, e.lineno, e.msg)
    return True, "%d python block%s parse" % (len(bl), "" if len(bl) == 1 else "s")


class PyInfo(ast.NodeVisitor):
    """Imports, calls in evaluation order, receiver classes of simple variables, configuration dicts."""

    def __init__(self):
        self.imports = []           # (module, name or None, alias, lineno)
        self.alias_name = {}        # local name -> dotted origin ("confluent_kafka.Consumer" or module "confluent_kafka.admin")
        self.var_class = {}         # "x" / "self.x" -> origin of its class
        self.dicts = {}             # variable name -> {key: value-or-None}
        self.calls = []             # dicts: name, class, recv_class, nargs, kwargs, line, origin, node
        self.attr_uses = []         # (origin dotted path, lineno) of attribute chains on module aliases
        self.ctor_confs = []        # list of (dict key->value, line) passed to client classes
        self._pending_conf = []     # (Name id, line)

    # --- imports
    def visit_Import(self, n):
        for a in n.names:
            self.imports.append((a.name, None, a.asname, n.lineno))
            if a.asname:
                self.alias_name[a.asname] = a.name
            else:
                self.alias_name[a.name.split(".")[0]] = a.name.split(".")[0]

    def visit_ImportFrom(self, n):
        mod = ("." * n.level) + (n.module or "")
        for a in n.names:
            self.imports.append((mod, a.name, a.asname, n.lineno))
            self.alias_name[a.asname or a.name] = mod + "." + a.name

    # --- helpers
    def origin_of(self, node):
        """Dotted origin of a Name or an attribute chain whose root is an import alias, else None."""
        if isinstance(node, ast.Name):
            return self.alias_name.get(node.id)
        if isinstance(node, ast.Attribute):
            base = self.origin_of(node.value)
            return None if base is None else base + "." + node.attr
        return None

    @staticmethod
    def key_of(node):
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            return node.value.id + "." + node.attr
        return None

    @staticmethod
    def const_str(v):
        if isinstance(v, ast.Constant):
            if isinstance(v.value, bool):
                return "true" if v.value else "false"
            return str(v.value)
        return None

    def dict_of(self, node):
        if isinstance(node, ast.Dict):
            d = {}
            for k, v in zip(node.keys, node.values):
                if k is None:                                   # **base
                    if isinstance(v, ast.Name) and v.id in self.dicts:
                        d.update(self.dicts[v.id])
                elif isinstance(k, ast.Constant) and isinstance(k.value, str):
                    d[k.value] = self.const_str(v)
            return d
        if isinstance(node, ast.Name):
            return self.dicts.get(node.id)
        return None

    # --- assignments
    def visit_Assign(self, n):
        self.visit(n.value)
        d = self.dict_of(n.value) if isinstance(n.value, ast.Dict) else None
        cls = self.ctor_class(n.value)
        for t in n.targets:
            k = self.key_of(t)
            if k and d is not None:
                self.dicts[k] = dict(d)
            if k and cls:
                self.var_class[k] = cls
            if isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant) and isinstance(t.slice.value, str):
                base = self.key_of(t.value)
                if base in self.dicts:
                    self.dicts[base][t.slice.value] = self.const_str(n.value)

    def visit_AnnAssign(self, n):
        if n.value is not None:
            self.visit(n.value)
        k = self.key_of(n.target)
        if k and n.value is not None:
            if isinstance(n.value, ast.Dict):
                self.dicts[k] = self.dict_of(n.value)
            cls = self.ctor_class(n.value)
            if cls:
                self.var_class[k] = cls

    def visit_With(self, n):
        for it in n.items:
            self.visit(it.context_expr)
            cls = self.ctor_class(it.context_expr)
            if cls and it.optional_vars is not None and self.key_of(it.optional_vars):
                self.var_class[self.key_of(it.optional_vars)] = cls
        for s in n.body:
            self.visit(s)

    def ctor_class(self, node):
        if isinstance(node, ast.Call):
            o = self.origin_of(node.func)
            if o and o.split(".")[0] == CK:
                return o
        return None

    def visit_Attribute(self, n):
        o = self.origin_of(n)
        if o and o.split(".")[0] == CK:
            self.attr_uses.append((o, n.lineno))
        self.generic_visit(n)

    def visit_Call(self, n):
        self.visit(n.func)
        for a in n.args:
            self.visit(a)
        for k in n.keywords:
            self.visit(k.value)
        rec = {"line": n.lineno, "nargs": len([a for a in n.args if not isinstance(a, ast.Starred)]),
               "star": any(isinstance(a, ast.Starred) for a in n.args),
               "kwargs": [k.arg for k in n.keywords if k.arg], "dstar": any(k.arg is None for k in n.keywords)}
        f = n.func
        if isinstance(f, ast.Attribute):
            rec["name"] = f.attr
            o = self.origin_of(f)
            recv = self.key_of(f.value)
            rec["origin"] = o
            rec["recv_class"] = self.var_class.get(recv) if recv else self.ctor_class(f.value)
            if rec["recv_class"] is None and o and o.split(".")[0] == CK:
                rec["recv_class"] = None
        elif isinstance(f, ast.Name):
            rec["name"] = f.id
            rec["origin"] = self.alias_name.get(f.id)
            rec["recv_class"] = None
        else:
            return
        origin = rec["origin"]
        rec["is_client_class"] = bool(origin and origin.split(".")[0] == CK and rec["recv_class"] is None
                                      and rec["name"][:1].isupper())
        rec["class"] = origin if rec["is_client_class"] else rec["recv_class"]
        self.calls.append(rec)
        # configuration passed to a client class
        if rec["is_client_class"]:
            arg = n.args[0] if n.args else next((k.value for k in n.keywords if k.arg in ("conf", "config")), None)
            d = self.dict_of(arg) if arg is not None else None
            if d is not None:
                self.ctor_confs.append((d, n.lineno))


def _pyinfo(text):
    bl, why = py_source(text)
    if why:
        return None, (False, why)
    info = PyInfo()
    for i, b in enumerate(bl):
        try:
            tree = ast.parse(b, filename="<block %d>" % (i + 1))
        except SyntaxError as e:
            return None, (False, "py_syntax_error: block %d line %s: %s" % (i + 1, e.lineno, e.msg))
        info.visit(tree)
    return info, None


def _symbols(chk):
    d = chk.get("catalogues_a3")
    if catalogue_a3.python_symbols(d) is None:
        return None, (False, "py_symbols_unavailable: python_symbols.json is not built (scripts/build_catalogues_a3.py)")
    return d, None


def check_py_imports(chk, text, cats):
    d, bad = _symbols(chk)
    if bad:
        return bad
    info, bad = _pyinfo(text)
    if bad:
        return bad
    problems, seen = [], set()
    for mod, name, alias, line in info.imports:
        if mod.split(".")[0] != CK:
            continue
        if mod.startswith("."):
            continue
        names = catalogue_a3.py_module_names(mod, d)
        if names is None:
            problems.append("no module %s (line %d)" % (mod, line))
            continue
        if name is None:
            seen.add(mod)
        elif name == "*":
            pass
        elif name in names or catalogue_a3.py_module_names(mod + "." + name, d) is not None:
            seen.add(name)
        else:
            problems.append("%s has no name %s (line %d)" % (mod, name, line))
    for dotted, line in info.attr_uses:
        parts = dotted.split(".")
        for i in range(1, len(parts)):
            mod, name = ".".join(parts[:i]), parts[i]
            names = catalogue_a3.py_module_names(mod, d)
            if names is None:
                break
            if name in names:
                break                                           # a class or function: deeper parts are members
            if catalogue_a3.py_module_names(mod + "." + name, d) is None:
                problems.append("%s has no name %s (line %d)" % (mod, name, line))
                break
    if not seen and not problems:
        problems.append("no import of confluent_kafka")
    for r in chk.get("require", []):
        if r not in seen:
            problems.append("%s is not imported" % r)
    if problems:
        return False, "; ".join(dict.fromkeys(problems))[:400]
    return True, "imports from confluent_kafka exist in the %s symbol list" % catalogue_a3.python_symbols(d)["version"]


def _py_entry_matches(e, c):
    if e.get("name") and e["name"] != c["name"]:
        return False
    cls = e.get("class")
    if cls:
        full = cls if "." in cls else None
        got = c.get("class")
        if got is not None:
            if full:
                if got != full:
                    return False
            elif got.rsplit(".", 1)[-1] != cls:
                return False
    if e.get("min_args") is not None and c["nargs"] < e["min_args"] and not c["star"]:
        return False
    for k in e.get("kwargs", []):
        if k not in c["kwargs"] and not c["dstar"]:
            return False
    return True


def _py_describe(e):
    return "%s%s" % ((e["class"] + ".") if e.get("class") else "", e.get("name", "*"))


def _py_method_problems(c, d):
    """Problems of a call on a variable of a known client class: unknown method, unknown keyword, too many args."""
    cls = c.get("recv_class")
    if not cls or cls == c.get("origin"):
        return []
    sym = catalogue_a3.py_class(cls, d)
    if sym is None:
        return []
    short = cls.rsplit(".", 1)[-1]
    if c["name"] not in sym["members"]:
        return ["%s has no method %s (line %d)" % (short, c["name"], c["line"])]
    sig = catalogue_a3.py_method(cls, c["name"], d)
    if not sig:
        return []
    out = []
    allowed = set(sig.get("params", [])) | ({"*"} if sig.get("var_kw") else set())
    if "*" not in allowed:
        for k in c["kwargs"]:
            if k not in allowed:
                out.append("%s.%s has no parameter %s (line %d)" % (short, c["name"], k, c["line"]))
    if not sig.get("var_pos") and not c["star"] and c["nargs"] > len(sig.get("params", [])) + len(sig.get("pos_only", [])):
        out.append("%s.%s takes at most %d positional argument%s, %d given (line %d)" % (
            short, c["name"], len(sig.get("params", [])) + len(sig.get("pos_only", [])),
            "" if len(sig.get("params", [])) + len(sig.get("pos_only", [])) == 1 else "s", c["nargs"], c["line"]))
    return out


def check_py_calls(chk, text, cats):
    d, bad = _symbols(chk)
    if bad:
        return bad
    info, bad = _pyinfo(text)
    if bad:
        return bad
    problems = []
    for e in chk.get("required", []):
        if not any(_py_entry_matches(e, c) for c in info.calls):
            problems.append("missing call %s" % _py_describe(e))
    for e in chk.get("forbidden", []):
        if any(_py_entry_matches(e, c) for c in info.calls):
            problems.append("forbidden call %s present" % _py_describe(e))
    if chk.get("order"):
        ok, detail = ordered_subsequence(chk["order"], info.calls, _py_entry_matches)
        if not ok:
            problems.append(detail)
    if chk.get("unknown_methods", True):
        for c in info.calls:
            problems.extend(_py_method_problems(c, d))
    if problems:
        return False, "; ".join(dict.fromkeys(problems))[:400]
    return True, "%d call%s as required, methods and keywords exist in the wheel" % (
        len(chk.get("required", [])) + len(chk.get("order", [])), "s" if len(chk.get("required", [])) + len(chk.get("order", [])) != 1 else "")


def check_py_config(chk, text, cats):
    d, bad = _symbols(chk)
    if bad:
        return bad
    info, bad = _pyinfo(text)
    if bad:
        return bad
    if not info.ctor_confs:
        return False, "py_no_config: no configuration dict passed to a confluent_kafka client class"
    props = set(catalogue_a3.py_config_properties(d))
    confs = [c for c, _ in info.ctor_confs]
    problems = []
    if props:
        for k in dict.fromkeys(k for c in confs for k in c):
            if k not in props:
                problems.append("unknown config key %r" % k)
    for k, want in (chk.get("keys") or {}).items():       # some client's configuration sets it (to the value)
        have = [c[k] for c in confs if k in c]
        if not have:
            problems.append("config key %s is not set" % k)
        elif want is not None and not any(v is not None and _same(v, want) for v in have):
            problems.append("config key %s is %s, wanted %s" % (k, "not a constant" if have[-1] is None else repr(have[-1]), want))
    for k, bad_v in (chk.get("forbidden") or {}).items():  # no client's configuration may set it (to the value)
        for c in confs:
            if k in c and (bad_v is None or (c[k] is not None and _same(c[k], bad_v))):
                problems.append("config key %s must not be %s" % (k, "set" if bad_v is None else repr(bad_v)))
    if problems:
        return False, "; ".join(dict.fromkeys(problems))[:400]
    note = "" if props else " (no property table in python_symbols.json: only names of imports and calls were checked)"
    nkeys = len({k for c in confs for k in c})
    return True, "%d config key%s set, all known%s" % (nkeys, "" if nkeys == 1 else "s", note)


CHECKS_A3_CLIENT = {
    "java_compiles": check_java_compiles,
    "java_calls": check_java_calls,
    "java_call_order": check_java_call_order,
    "java_config": check_java_config,
    "py_syntax": check_py_syntax,
    "py_imports": check_py_imports,
    "py_calls": check_py_calls,
    "py_config": check_py_config,
}
