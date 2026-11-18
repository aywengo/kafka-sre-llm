"""Answer checker: inspects an answer for names, values and uses the catalogues cannot confirm.

The runtime answers from retrieved documentation excerpts, and its answers sometimes carry an
invented or wrongly used item (a field that does not exist, a flag the tool does not have, a method
the REST interface does not accept). This module extracts the items an operator would type from an
answer in Markdown and decides each one against, in this order:

  (a) the excerpts the answer was written from (when the caller passes them): the exact token there
      makes the item `in_excerpts`, and it is never reported as invented (third-party names live there);
  (b) the catalogues (kafka_sre_rag.catalogue): `known`, `unknown` (only from a catalogue or tool marked
      complete) or `not_covered`;
  (c) value and usage checks where the catalogue has the data: a value outside the valid values of a
      key, an HTTP method the path does not accept, a field path that does not exist for the stated
      kind (with the nearest real names), a config key in a scope it does not belong to when the scope
      is clear from the context, a label or annotation value that is a near miss of a documented one.

Findings have a level: `invented` (unknown in a complete catalogue), `wrong_use` (a known name with a
wrong value, method or scope), `unverified` (no catalogue covers it and the excerpts do not contain it;
information, not an accusation). Nothing here rewrites the answer.

Shape rules that keep the checker from judging what it cannot know (each is a class, not a string):
  * a dotted token is a Kafka-style config key only when its first word starts some catalogued key (or
    it carries a foreign prefix such as spring.), and it is not a file name, domain, URL or version;
  * a key that is in no scope of the Kafka reference is `invented` only when it resembles a real key (shares two
    words with one, one of them distinctive, e.g. retention.hours ~ log.retention.hours); a key that resembles
    none (auto.register.schemas) is `unverified`: it may belong to another component. The start of a real key
    (min.insync) is an abbreviation and is not reported. A scope is taken only from a YAML path, a properties file
    named in the sentence or just before the block, or (for `key=value` only) a section heading about settings;
    never from the words of a sentence;
  * a field that is not in the Strimzi reference but is written under an older apiVersion than the one the
    reference describes is `invented` only when a real sibling has a similar name, else `unverified`; what lies
    below a field already reported is not reported again;
  * a flag counts as in the excerpts when an excerpt that names its tool contains it;
  * a config key inside a connector, a MirrorMaker 2 or a prefixed (producer.*) block is judged only
    as "known somewhere"; a key unknown there is `not_covered` (connector-specific keys are not listed);
  * a path under an HTTP method is judged against the Connect REST interface only when its first
    segment is one the Connect REST interface has; other REST interfaces are `not_covered`;
  * a Kafka-prefixed CamelCase name is judged as a Strimzi type only where it is used as a kind
    (a `kind:` line, "the X resource"); elsewhere it may be a Java class and is `not_covered`;
  * a CamelCase or UPPER_SNAKE name is judged as a protocol error only when it is that shape of a real
    error name with a small spelling difference, or the sentence speaks of an error.

Stdlib only. Imports only kafka_sre_rag.catalogue. Reads nothing under eval/.
"""
import difflib
import re
import shlex
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .catalogue import FOREIGN_KEY_PREFIXES, KNOWN, NOT_COVERED, OPEN_KEY_PREFIXES, UNKNOWN, Catalogues, normalise_path

VERSION = "checker/1"

INVENTED = "invented"
WRONG_USE = "wrong_use"
UNVERIFIED = "unverified"
LEVELS = (INVENTED, WRONG_USE, UNVERIFIED)

IN_EXCERPTS = "in_excerpts"
SKIPPED = "skipped"   # extracted, but the context needed to judge it is missing

HTTP_METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD")

FILE_EXTS = frozenset("""yaml yml json md txt sh properties conf cfg log xml html htm py js go java jar csv ini toml tar
gz zip tgz crt pem jks p12 sql adoc env""".split())
DOMAIN_SUFFIXES = frozenset("local svc com org io net dev example internal cluster".split())

# a section heading that names a scope and says it is about settings ("Consumer configuration")
HEADING_SCOPE = (
    (re.compile(r"\bconsumer\b.*\b(config|propert|setting)", re.I), "consumer"),
    (re.compile(r"\bproducer\b.*\b(config|propert|setting)", re.I), "producer"),
    (re.compile(r"\bbroker\b.*\b(config|propert|setting)", re.I), "broker"),
    (re.compile(r"\btopic\b.*\b(config|propert|setting)", re.I), "topic"),
)
PROPS_FILE_SCOPE = (
    (re.compile(r"\bserver\.properties\b|\bbroker\.properties\b"), "broker"),
    (re.compile(r"\bconsumer\.properties\b"), "consumer"),
    (re.compile(r"\bproducer\.properties\b"), "producer"),
    (re.compile(r"\bconnect-(?:distributed|standalone)\.properties\b"), "connect"),
)
ENTITY_SCOPE = {"topics": "topic", "brokers": "broker", "broker-loggers": None, "clients": None, "users": None}

CONFIG_PATH_SCOPE = {  # (kind, normalised path of the config map) -> scope; "open" = connector-specific
    ("Kafka", "spec.kafka.config"): "broker",
    ("KafkaConnect", "spec.config"): "connect",
    ("KafkaTopic", "spec.config"): "topic",
    ("KafkaBridge", "spec.consumer.config"): "consumer",
    ("KafkaBridge", "spec.producer.config"): "producer",
    ("KafkaBridge", "spec.adminClient.config"): "admin",
    ("KafkaConnector", "spec.config"): "open",
    ("KafkaMirrorMaker2", "spec.mirrors[].sourceConnector.config"): "open",
    ("KafkaMirrorMaker2", "spec.mirrors[].checkpointConnector.config"): "open",
    ("KafkaMirrorMaker2", "spec.mirrors[].heartbeatConnector.config"): "open",
}

CAMEL_RE = re.compile(r"(?<![\w.\-/])(?:[A-Z][a-z0-9]+){2,}(?![\w\-])")
UPPER_SNAKE_RE = re.compile(r"(?<![\w.\-/])[A-Z][A-Z0-9]*(?:_[A-Z0-9]+){1,}(?![\w\-])")
DOTTED_RE = re.compile(r"(?<![\w.\-/@:=])[a-z][a-z0-9_-]*(?:\.[a-z0-9_-]+)+(?![\w\-/@:])")
ANNOT_RE = re.compile(r"(?<![\w./-])([a-z0-9][a-z0-9.-]*\.(?:io|dev|com|org|net)/[A-Za-z0-9._-]*[A-Za-z0-9_])(?![\w/-])")
FLAG_RE = re.compile(r"(?<![\w-])(--[a-z][a-z0-9]*(?:-[a-z0-9]+)*)(?![\w-])")
TOOL_RE = re.compile(r"(?<![\w./-])((?:[\w./-]*/)?kafka-[a-z0-9]+(?:-[a-z0-9]+)*(?:\.sh)?)(?![\w-])")
HTTP_RE = re.compile(r"\b(GET|POST|PUT|DELETE|PATCH|HEAD)\s+((?:https?://[^/\s`]+)?/[A-Za-z0-9_{}<>:.\-$]*(?:/[A-Za-z0-9_{}<>:.\-$?=&]*)*)")
FIELD_PATH_RE = re.compile(
    r"(?<![\w.\-/])(?:([A-Z][A-Za-z0-9]+)\.)?\.?((?:spec|status)(?:\.[A-Za-z0-9_\-]+(?:\[[^\]\s]*\])?|\[[^\]\s]*\])+)")
INLINE_CODE_RE = re.compile(r"(``[^\n]+?``|`[^`\n]+`)")
KV_RE = re.compile(r"^([a-z][a-z0-9_.\-]*[a-z0-9])\s*(?:=|:\s)\s*(.*)$")


# ----------------------------------------------------------------------------- data classes
@dataclass
class Item:
    text: str                 # the token as written ("--unparsed", "PUT /connectors/{name}/restart")
    type: str                 # config_key, config_value, field_path, cli_flag, cli_tool, http_op, error_name,
    #                           annotation, label, type_name
    line: int                 # 1-based line of the answer
    sentence: str             # the sentence or code line around it
    source: str = "prose"     # fence, inline, prose
    context: Dict[str, Any] = field(default_factory=dict)
    status: str = ""          # in_excerpts, known, unknown, not_covered, skipped (set by the decision)
    count: int = 1

    def to_dict(self):
        return {"text": self.text, "type": self.type, "line": self.line, "sentence": self.sentence,
                "source": self.source, "context": self.context, "status": self.status, "count": self.count}


@dataclass
class Finding:
    level: str
    item: Item
    rule: str                 # which decision produced it
    evidence: str
    suggestion: str = ""
    catalogue: Optional[str] = None

    def to_dict(self):
        return {"level": self.level, "text": self.item.text, "type": self.item.type, "line": self.item.line,
                "sentence": self.item.sentence, "rule": self.rule, "evidence": self.evidence,
                "suggestion": self.suggestion, "catalogue": self.catalogue}


@dataclass
class CheckResult:
    items: List[Item]
    findings: List[Finding]
    catalogue_hash: Optional[str] = None
    unchecked_kinds: Dict[str, int] = field(default_factory=dict)
    unknown_paths: set = field(default_factory=set)     # (kind, path) already reported, so children are not

    def by_level(self, level):
        return [f for f in self.findings if f.level == level]

    def counts(self):
        c = {lv: len(self.by_level(lv)) for lv in LEVELS}
        c["items"] = len(self.items)
        c["in_excerpts"] = sum(1 for i in self.items if i.status == IN_EXCERPTS)
        c["known"] = sum(1 for i in self.items if i.status == KNOWN)
        return c

    def to_dict(self):
        return {"checker": VERSION, "catalogue_hash": self.catalogue_hash, "counts": self.counts(),
                "findings": [f.to_dict() for f in self.findings], "items": [i.to_dict() for i in self.items],
                "unchecked_kinds": self.unchecked_kinds}


# ----------------------------------------------------------------------------- markdown segments
@dataclass
class Seg:
    kind: str                       # "text" or "fence"
    lines: List[Tuple[int, str]]
    lang: str = ""
    heading: str = ""
    before: str = ""                # text of up to three lines before this segment (context)


def split_markdown(text: str) -> List[Seg]:
    segs: List[Seg] = []
    heading = ""
    recent: List[str] = []
    fence = None
    buf: List[Tuple[int, str]] = []
    lang = ""
    start_before = ""
    for no, raw in enumerate(text.split("\n"), 1):
        m = re.match(r"^\s*(`{3,}|~{3,})\s*([^\s`]*)", raw)
        if fence is None:
            if m:
                fence = m.group(1)[0] * 3
                lang = m.group(2).lower()
                buf = []
                start_before = "\n".join(recent[-3:])
                continue
            h = re.match(r"^\s{0,3}#{1,6}\s+(.*)$", raw)
            if h:
                heading = h.group(1).strip()
            segs.append(Seg("text", [(no, raw)], heading=heading, before="\n".join(recent[-2:])))
            if raw.strip():
                recent.append(raw)
        else:
            if raw.strip().startswith(fence):
                segs.append(Seg("fence", buf, lang=lang, heading=heading, before=start_before))
                fence = None
                buf = []
            else:
                buf.append((no, raw))
    if fence is not None and buf:
        segs.append(Seg("fence", buf, lang=lang, heading=heading, before=start_before))
    return segs


# ----------------------------------------------------------------------------- helpers
def _sentence_of(line: str, pos: int) -> str:
    """The sentence of a prose line around position pos (sentences end at '. ' '! ' '? ')."""
    start = 0
    for m in re.finditer(r"[.!?](?=\s)|[:;](?=\s)", line):
        if m.end() <= pos:
            start = m.end()
        else:
            return line[start:m.end()].strip(" *-#>")
    return line[start:].strip(" *-#>")


def _first_words(cats: Catalogues) -> set:
    return {k.split(".")[0] for k in cats._config}


def _strimzi_kinds(cats: Catalogues) -> List[str]:
    return sorted((e["kind"] for e in (cats.data["strimzi_resources"] or {}).get("entries", [])), key=len, reverse=True)


def kinds_in(text: str, kinds: List[str]) -> List[str]:
    """Strimzi kinds named in `text`. The bare word Kafka counts only as a kind (code span, `Kafka resource`,
    `Kafka CR`, `Kafka.spec`), not as the name of the product."""
    found = []
    for k in kinds:
        if k == "Kafka":
            if re.search(r"`Kafka`|\bKafka\s+(?:resource|CR|custom resource|CRD)\b|\bKafka\.(?:spec|status)\b|kind:\s*Kafka\b", text):
                found.append(k)
        elif re.search(r"(?<![\w])%s(?![\w])" % re.escape(k), text):
            found.append(k)
    return found


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


class Excerpts:
    def __init__(self, excerpts):
        if excerpts is None:
            self.text = None
            self.chunks: List[str] = []
        elif isinstance(excerpts, str):
            self.text = excerpts
            self.chunks = [excerpts]
        else:
            self.chunks = list(excerpts)
            self.text = "\n\n".join(self.chunks)

    @property
    def present(self) -> bool:
        return self.text is not None

    def has(self, token: str) -> bool:
        if not self.present or not token:
            return False
        pat = r"(?<![\w-])(?<!\w\.)" + re.escape(token) + r"(?![\w-]|\.\w)"
        return re.search(pat, self.text) is not None

    def has_flag(self, flag: str, tool: str) -> bool:
        """A flag is a flag of one tool: it counts as in the excerpts when it occurs in an excerpt that names
        the tool; when no excerpt names the tool, an occurrence anywhere counts."""
        if not self.present:
            return False
        base = tool.replace(".sh", "")
        pat = r"(?<![\w-])" + re.escape(flag) + r"(?![\w-])"
        named = [c for c in self.chunks if base in c]
        if not named:
            return re.search(pat, self.text) is not None
        return any(re.search(pat, c) for c in named)

    def has_path(self, path: str) -> bool:
        """A field path: the dotted path itself, or its last two keys one after the other as YAML keys."""
        if not self.present:
            return False
        bare = normalise_path(path)
        for cand in {path, bare, bare.replace("[]", "")}:
            if cand and cand in self.text:
                return True
        segs = [s for s in re.split(r"\.|\[\]", bare) if s]
        if len(segs) >= 2:
            parent, leaf = segs[-2], segs[-1]
            if re.search(r"(?<![\w])%s\s*:[^\n]*\n(?:[^\n]*\n){0,40}?\s*-?\s*%s\s*:" % (re.escape(parent), re.escape(leaf)), self.text):
                return True
        elif segs:
            return bool(re.search(r"(?<![\w.])%s\s*:" % re.escape(segs[0]), self.text))
        return False


# ----------------------------------------------------------------------------- YAML (indentation reader)
def _strip_comment(s: str) -> str:
    q = None
    for i, ch in enumerate(s):
        if q:
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
        elif ch == "#" and (i == 0 or s[i - 1] in " \t"):
            return s[:i]
    return s


_YKEY = re.compile(r"""^("(?:[^"\\]|\\.)*"|'[^']*'|[^\s:#\-\[{][^:]*?|-[^\s:][^:]*?)\s*:(?:\s+(.*))?$""")


def yaml_entries(lines: List[Tuple[int, str]]):
    """Yield documents as dicts {kind, apiVersion, entries:[(segments, key, value, lineno)]} from the lines of a
    YAML block. Tolerant: reads what it can from partial or slightly wrong YAML; flow values are skipped."""
    docs = []
    cur = {"kind": None, "apiVersion": None, "entries": [], "start": lines[0][0] if lines else 0}
    stack: List[Tuple[int, str]] = []
    skip_indent = None
    for no, raw in lines:
        if raw.strip() == "---":
            docs.append(cur)
            cur = {"kind": None, "apiVersion": None, "entries": [], "start": no}
            stack = []
            skip_indent = None
            continue
        s = _strip_comment(raw.rstrip())
        if not s.strip() or s.strip() in ("...", "…"):
            continue
        indent = len(s) - len(s.lstrip(" "))
        if skip_indent is not None:
            if indent > skip_indent:
                continue
            skip_indent = None
        body = s.lstrip(" ")
        is_item = False
        key_indent = indent
        if body.startswith("- ") or body == "-":
            is_item = True
            while stack and stack[-1][0] > indent:
                stack.pop()
            if not (stack and stack[-1][0] == indent and stack[-1][1] == "[]"):
                stack.append((indent, "[]"))
            rest = body[1:].lstrip(" ")
            key_indent = indent + (len(body) - len(rest))
            body = rest
            if not body:
                continue
        else:
            while stack and stack[-1][0] >= indent:
                stack.pop()
        m = _YKEY.match(body)
        if not m:
            continue
        key = m.group(1).strip().strip("\"'")
        val = (m.group(2) or "").strip()
        if is_item:
            while stack and stack[-1][0] >= key_indent and stack[-1][1] != "[]":
                stack.pop()
        segs = [e[1] for e in stack] + [key]
        if not stack and key in ("kind", "apiVersion") and val:
            cur[key] = val.strip("\"'")
        cur["entries"].append((segs, key, val.strip("\"'") if val and val[0] in "\"'" else val, no))
        if val in ("|", ">", "|-", ">-", "|+", ">+"):
            skip_indent = key_indent
        elif not val or val in ("{}", "[]"):
            stack.append((key_indent, key))
        elif val.startswith(("{", "[")) or val.startswith(("&", "*")):
            stack.append((key_indent, key))
        # scalar value: the key is a leaf, nothing is pushed
    docs.append(cur)
    return docs


def segs_to_path(segs: List[str]) -> str:
    out = ""
    for s in segs:
        if s == "[]":
            out += "[]"
        else:
            out += ("." if out else "") + s
    return out


def prose_s_with_code(line: str, pos: int) -> str:
    return line[pos:]


# ----------------------------------------------------------------------------- the checker
class Checker:
    def __init__(self, catalogues: Optional[Catalogues] = None):
        self.cats = catalogues or Catalogues.load()
        self._first = _first_words(self.cats)
        self._kinds = _strimzi_kinds(self.cats)
        self._errors = {e["name"]: e for e in (self.cats.data["protocol_errors"] or {}).get("entries", [])}
        self._error_norm = {_norm(n): n for n in self._errors}
        self._rest_first = {self.cats.rest_segments(e["path"])[0] for e in (self.cats.data["connect_rest"] or {}).get("entries", [])
                            if self.cats.rest_segments(e["path"])}
        self._unchecked_now: Dict[str, int] = {}
        self._seg_df: Dict[str, int] = {}
        for k in self.cats._config:
            for g in set(k.split(".")):
                self._seg_df[g] = self._seg_df.get(g, 0) + 1
        self._kubectl_opts = set()
        for t, es in self.cats._tools.items():
            if t.startswith("kubectl"):
                for e in es:
                    self._kubectl_opts.update(e["options"])

    # ------------------------------------------------------------------ public
    def check(self, text: str, excerpts=None) -> CheckResult:
        ex = Excerpts(excerpts)
        result = CheckResult([], [], self._chash())
        raw_items = self.extract(text, result)
        seen: Dict[tuple, Item] = {}
        for it in raw_items:
            key = (it.type, it.text, tuple(sorted((k, str(v)) for k, v in it.context.items() if k in ("scope", "tool", "kind", "open"))))
            if key in seen:
                seen[key].count += 1
                continue
            seen[key] = it
        for it in seen.values():
            result.items.append(it)
            self._decide(it, ex, result)
        return result

    def _chash(self):
        try:
            from .catalogue import catalogue_hash
            return catalogue_hash()
        except Exception:
            return None

    # ------------------------------------------------------------------ extraction
    def extract(self, text: str, result: Optional[CheckResult] = None) -> List[Item]:
        items: List[Item] = []
        segs = split_markdown(text)
        recent_tool = (None, -99)
        for seg in segs:
            if seg.kind == "fence":
                items += self._extract_fence(seg, result)
                continue
            no, line = seg.lines[0]
            if not line.strip():
                continue
            items_line, tool = self._extract_text_line(no, line, seg, recent_tool)
            items += items_line
            if tool:
                recent_tool = (tool, no)
        return items

    # -------- fences
    def _extract_fence(self, seg: Seg, result) -> List[Item]:
        lines = seg.lines
        body = "\n".join(l for _, l in lines)
        lang = seg.lang
        out: List[Item] = []
        if lang in ("json", "xml", "java", "python", "py", "go", "diff", "text", "log", "sql", "javascript"):
            if lang not in ("text", "log"):
                return out
        # heredocs inside shell blocks: the body is YAML
        shell_lines: List[Tuple[int, str]] = []
        i = 0
        while i < len(lines):
            no, l = lines[i]
            hm = re.search(r"<<-?\s*['\"]?(\w+)['\"]?", l)
            if hm and lang in ("bash", "sh", "shell", "console", "zsh", ""):
                shell_lines.append((no, l))
                end = hm.group(1)
                j = i + 1
                here = []
                while j < len(lines) and lines[j][1].strip() != end:
                    here.append(lines[j])
                    j += 1
                if here:
                    out += self._extract_yaml(here, seg, hm.group(0))
                i = j + 1
                continue
            shell_lines.append((no, l))
            i += 1
        lines = shell_lines
        body = "\n".join(l for _, l in lines)
        looks_yaml = bool(re.search(r"^\s*apiVersion:\s*\S", body, re.M) and re.search(r"^\s*kind:\s*\S", body, re.M))
        partial_yaml = bool(re.match(r"^\s*(spec|metadata|status):\s*$", body, re.M)) and lang in ("yaml", "yml", "")
        if lang in ("yaml", "yml") or looks_yaml or partial_yaml:
            out += self._extract_yaml(lines, seg, "")
            return out
        first = next((l.strip() for _, l in lines if l.strip() and not l.strip().startswith("#")), "")
        shellish = lang in ("bash", "sh", "shell", "console", "zsh") or re.match(
            r"^(\$\s*)?(kubectl|kafka-|bin/kafka|\./bin/kafka|/opt/kafka|argocd|curl|oc |helm|docker|sudo|export |echo |cat |ssh)", first)
        propsish = lang in ("properties", "ini", "conf", "cfg") or (
            not shellish and sum(1 for _, l in lines if re.match(r"^\s*[A-Za-z][\w.\-]*\s*=", l)) >= 1)
        httpish = lang in ("http",) or re.match(r"^(GET|POST|PUT|DELETE|PATCH|HEAD)\s+/", first)
        if shellish:
            out += self._extract_shell(lines, seg)
        elif httpish:
            for no, l in lines:
                out += self._http_in(l, no, l.strip(), "fence")
        elif propsish:
            out += self._extract_properties(lines, seg)
        return out

    def _scope_from_text(self, *texts) -> Optional[str]:
        t = " ".join(x for x in texts if x)
        for rx, sc in PROPS_FILE_SCOPE:
            if rx.search(t):
                return sc
        return None

    def _scope_from_heading(self, heading: str) -> Optional[str]:
        found = {sc for rx, sc in HEADING_SCOPE if rx.search(heading or "")}
        return next(iter(found)) if len(found) == 1 else None

    _OTHER_SCOPE_WORDS = ((re.compile(r"\bconsumers?\b", re.I), "consumer"), (re.compile(r"\bproducers?\b", re.I), "producer"),
                          (re.compile(r"\bbrokers?\b", re.I), "broker"), (re.compile(r"\btopics?\b", re.I), "topic"))

    def _scope_hint(self, sentence: str, seg, heading_ok: bool = False) -> Optional[str]:
        """The scope of a key, when the context states it: a properties file named in the sentence or the
        text before the block; for a `key=value` setting also a section heading about one kind of settings,
        unless the sentence itself speaks of another scope."""
        sc = self._scope_from_text(sentence, seg.before)
        if sc or not heading_ok:
            return sc
        sc = self._scope_from_heading(seg.heading)
        if sc and any(rx.search(sentence) and name != sc for rx, name in self._OTHER_SCOPE_WORDS):
            return None
        return sc

    def _extract_properties(self, lines, seg) -> List[Item]:
        out = []
        scope = self._scope_hint("\n".join(l for _, l in lines), seg, heading_ok=True)
        for no, l in lines:
            s = l.strip()
            if not s or s.startswith("#"):
                continue
            m = re.match(r"^([A-Za-z][\w.\-]*)\s*=\s*(.*)$", s)
            if not m:
                continue
            key, val = m.group(1), re.sub(r"\s+#.*$", "", m.group(2)).strip()
            out += self._config_items(key, val, no, s, "fence", {"scope": scope} if scope else {})
        return out

    def _config_items(self, key, value, no, sentence, source, ctx) -> List[Item]:
        out = []
        if not self._is_config_key(key):
            return out
        out.append(Item(key, "config_key", no, sentence, source, dict(ctx)))
        v = (value or "").strip().strip("\"'`")
        if v and not re.search(r"[<>${}\[\]]", v) and len(v) < 60:
            out.append(Item("%s=%s" % (key, v), "config_value", no, sentence, source, dict(ctx, key=key, value=v)))
        return out

    def _is_config_key(self, tok: str) -> bool:
        if not re.fullmatch(r"[a-z][a-z0-9_-]*(?:\.[a-z0-9_-]+)+", tok):
            return False
        last = tok.rsplit(".", 1)[1]
        if last in FILE_EXTS or last in DOMAIN_SUFFIXES or re.fullmatch(r"[\d.]+", tok.replace(".", "")):
            return False
        if re.fullmatch(r"v?\d+(\.\d+)*", tok):
            return False
        first = tok.split(".")[0]
        return first in self._first or tok.startswith(FOREIGN_KEY_PREFIXES)

    # -------- YAML
    def _extract_yaml(self, lines, seg, _hd) -> List[Item]:
        out: List[Item] = []
        docs = yaml_entries(lines)
        ctx_kinds = kinds_in(seg.before + "\n" + seg.heading, self._kinds)
        for d in docs:
            if not d["entries"]:
                continue
            kind = d["kind"]
            partial = False
            if not kind:
                top = d["entries"][0][0][0]
                if top in ("spec", "metadata", "status") and len(ctx_kinds) == 1:
                    kind, partial = ctx_kinds[0], True
                else:
                    continue
            if kind not in self._kinds:
                out += self._yaml_labels_only(d, seg)
                self._unchecked(kind)
                continue
            seen_paths = set()
            api = d["apiVersion"]
            for segs, key, val, no in d["entries"]:
                top = segs[0]
                if top not in ("spec", "status", "metadata"):
                    continue
                path = segs_to_path(segs)
                ctx = {"kind": kind, "path": path}
                if api:
                    ctx["api_version"] = api
                line_text = "%s: %s" % (key, val) if val else "%s:" % key
                if top == "metadata":
                    out += self._yaml_meta(segs, key, val, no, line_text, kind)
                    continue
                # inside a free-form config map?
                cfg_i = None
                for i, s in enumerate(segs[:-1]):
                    if s == "config" and CONFIG_PATH_SCOPE.get((kind, normalise_path(segs_to_path(segs[:i + 1])))):
                        cfg_i = i
                        break
                if cfg_i is not None:
                    cpath = normalise_path(segs_to_path(segs[:cfg_i + 1]))
                    scope = CONFIG_PATH_SCOPE[(kind, cpath)]
                    if len(segs) == cfg_i + 2:
                        c2 = {"scope": None if scope == "open" else scope, "open": scope == "open", "kind": kind}
                        out += self._config_items(key, val, no, line_text, "fence", c2)
                    if cpath not in seen_paths:
                        seen_paths.add(cpath)
                        out.append(Item(cpath, "field_path", no, line_text, "fence", dict(ctx, path=cpath)))
                    continue
                if any(s.endswith(("annotations", "labels")) and s != "matchLabels" for s in segs[:-1]) and "/" in key:
                    out += self._yaml_meta(segs, key, val, no, line_text, kind)
                    continue
                np_ = normalise_path(path)
                if np_ in seen_paths:
                    continue
                seen_paths.add(np_)
                out.append(Item(np_, "field_path", no, line_text, "fence", ctx))
        return out

    def _unchecked(self, kind):
        self._unchecked_now[kind] = self._unchecked_now.get(kind, 0) + 1

    def _yaml_labels_only(self, d, seg) -> List[Item]:
        out = []
        for segs, key, val, no in d["entries"]:
            if segs[0] == "metadata" and any(s in ("labels", "annotations") for s in segs[:-1]) and "/" in key:
                out += self._yaml_meta(segs, key, val, no, "%s: %s" % (key, val), d["kind"])
        return out

    def _yaml_meta(self, segs, key, val, no, line_text, kind) -> List[Item]:
        if "/" not in key or not any(s.endswith(("labels", "annotations")) for s in segs[:-1]):
            return []
        typ = "label" if any(s.endswith("labels") for s in segs[:-1]) else "annotation"
        out = [Item(key, "annotation", no, line_text, "fence", {"kind": kind, "role": typ})]
        v = val.strip("\"'")
        if v and not re.search(r"[<>$]", v):
            out.append(Item("%s=%s" % (key, v), "annotation_value", no, line_text, "fence",
                            {"kind": kind, "name": key, "value": v, "role": typ}))
        return out

    # -------- shell
    def _extract_shell(self, lines, seg) -> List[Item]:
        out: List[Item] = []
        joined: List[Tuple[int, str]] = []
        buf, start = "", None
        for no, l in lines:
            s = l.rstrip()
            if start is None:
                start = no
            if s.endswith("\\"):
                buf += s[:-1] + " "
                continue
            buf += s
            joined.append((start, buf))
            buf, start = "", None
        if buf:
            joined.append((start, buf))
        for no, l in joined:
            s = l.strip()
            if not s or s.startswith("#"):
                continue
            s = re.sub(r"^\$\s+", "", s)
            for cmd in re.split(r"\s*(?:\|\||&&|\||;)\s*", s):
                out += self.parse_command(cmd, no, s, "fence")
        return out

    def parse_command(self, cmd: str, no: int, sentence: str, source: str) -> List[Item]:
        out: List[Item] = []
        cmd = cmd.strip()
        if not cmd:
            return out
        try:
            toks = shlex.split(cmd, posix=True)
        except ValueError:
            toks = cmd.split()
        while toks and (toks[0] in ("sudo", "exec", "time", "$") or re.match(r"^[A-Z_][A-Z0-9_]*=", toks[0])):
            toks = toks[1:]
        if "--" in toks and toks and toks[0] in ("kubectl", "oc"):
            k = toks.index("--")
            head, tail = toks[:k], toks[k + 1:]
            out += self._cmd_tokens(head, no, sentence, source)
            if tail:
                out += self.parse_command(" ".join(shlex.quote(t) if " " in t else t for t in tail), no, sentence, source)
            return out
        return out + self._cmd_tokens(toks, no, sentence, source)

    def _cmd_tokens(self, toks, no, sentence, source) -> List[Item]:
        out: List[Item] = []
        if not toks:
            return out
        base = toks[0].split("/")[-1]
        if base.startswith("kafka-"):
            return out + self._kafka_tool(toks, no, sentence, source)
        if base in ("kubectl", "oc"):
            return out + self._kubectl(toks, no, sentence, source)
        if base == "argocd":
            return out + self._argocd(toks, no, sentence, source)
        if base == "curl":
            return out + self._curl(toks, no, sentence, source)
        return out

    def _kafka_tool(self, toks, no, sentence, source) -> List[Item]:
        out = []
        tool = toks[0].split("/")[-1]
        tool = tool if tool.endswith(".sh") else tool + ".sh"
        out.append(Item(tool, "cli_tool", no, sentence, source, {"tool": tool}))
        entity = None
        for i, t in enumerate(toks[1:], 1):
            if t == "--entity-type" and i + 1 < len(toks):
                entity = toks[i + 1]
        for i, t in enumerate(toks[1:], 1):
            m = FLAG_RE.fullmatch(t.split("=")[0])
            if not m:
                continue
            flag = m.group(1)
            out.append(Item(flag, "cli_flag", no, sentence, source, {"tool": tool}))
            val = t.split("=", 1)[1] if "=" in t else (toks[i + 1] if i + 1 < len(toks) else "")
            if flag == "--add-config" and val:
                scope = ENTITY_SCOPE.get(entity) if entity else None
                for kv in re.split(r",(?![^\[]*\])", val):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        out += self._config_items(k.strip("[]"), v, no, sentence, source, {"scope": scope} if scope else {})
            elif flag in ("--producer-property", "--consumer-property") and "=" in val:
                k, v = val.split("=", 1)
                out += self._config_items(k, v, no, sentence, source, {"scope": flag[2:].split("-")[0]})
            elif flag in ("--property",) and "=" in val and tool.startswith(("kafka-console-consumer", "kafka-console-producer")):
                pass
        return out

    def _kubectl(self, toks, no, sentence, source) -> List[Item]:
        out = []
        verb = None
        rest = toks[1:]
        skip = False
        for t in rest:
            if skip:
                skip = False
                continue
            if t in ("-n", "--namespace", "-c", "--context", "-f", "-l", "--selector", "-o", "--output", "-A"):
                skip = t not in ("-A",)
                continue
            if t.startswith("-"):
                continue
            verb = t
            break
        tool = "kubectl %s" % verb if verb else "kubectl"
        out_flags = []
        for i, t in enumerate(rest):
            name = t.split("=")[0]
            if t in ("-l",) or name in ("--selector", "-l"):
                val = t.split("=", 1)[1] if "=" in t else (rest[i + 1] if i + 1 < len(rest) else "")
                out += self._selector_items(val, no, sentence, source)
            elif name.startswith("--") and FLAG_RE.fullmatch(name):
                out.append(Item(name, "cli_flag", no, sentence, source, {"tool": tool}))
        if verb in ("annotate", "label"):
            seen_res = False
            for t in rest[rest.index(verb) + 1:]:
                if t.startswith("-"):
                    continue
                if "=" in t or (t.endswith("-") and "/" in t):
                    k, _, v = t.partition("=")
                    k = k.rstrip("-")
                    if "/" in k:
                        out.append(Item(k, "annotation", no, sentence, source, {"role": verb}))
                        if v and not re.search(r"[<>$]", v):
                            out.append(Item("%s=%s" % (k, v), "annotation_value", no, sentence, source,
                                            {"name": k, "value": v, "role": verb}))
        return out

    def _selector_items(self, val, no, sentence, source) -> List[Item]:
        out = []
        for part in val.strip("'\"").split(","):
            k, eq, v = part.partition("=")
            k = k.strip()
            if eq and "/" in k:
                out.append(Item(k, "annotation", no, sentence, source, {"role": "label"}))
                v = v.strip().lstrip("=").strip("'\"")
                if v and not re.search(r"[<>$]", v):
                    out.append(Item("%s=%s" % (k, v), "annotation_value", no, sentence, source,
                                    {"name": k, "value": v, "role": "label"}))
        return out

    def _argocd(self, toks, no, sentence, source) -> List[Item]:
        out = []
        words = []
        for t in toks:
            if t.startswith("-"):
                break
            words.append(t)
        # longest prefix that is a command
        cmd = None
        for n in range(len(words), 0, -1):
            c = " ".join(words[:n])
            if self.cats.argocd_command(c).known:
                cmd = c
                break
        if cmd is None and len(words) >= 2:
            cmd = " ".join(words[:2])
        if cmd is None:
            return out
        out.append(Item(cmd, "cli_tool", no, sentence, source, {"tool": cmd, "argocd": True}))
        for t in toks:
            name = t.split("=")[0]
            if FLAG_RE.fullmatch(name):
                out.append(Item(name, "cli_flag", no, sentence, source, {"tool": cmd}))
        return out

    def _curl(self, toks, no, sentence, source) -> List[Item]:
        method, url = None, None
        for i, t in enumerate(toks):
            if t in ("-X", "--request") and i + 1 < len(toks):
                method = toks[i + 1].upper()
            elif t.startswith("-X") and len(t) > 2:
                method = t[2:].upper()
            elif re.match(r"^https?://|^/", t) and url is None:
                url = t
        if method is None and any(t in ("-d", "--data", "--data-raw", "--json") for t in toks):
            method = "POST"
        method = method or "GET"
        if url and method in HTTP_METHODS:
            return [Item("%s %s" % (method, url), "http_op", no, sentence, source,
                         {"method": method, "path": url})]
        return []

    def _http_in(self, text, no, sentence, source) -> List[Item]:
        out = []
        for m in HTTP_RE.finditer(text):
            method, path = m.group(1), m.group(2).rstrip(".,;:)")
            out.append(Item("%s %s" % (method, path), "http_op", no, sentence, source, {"method": method, "path": path}))
        return out

    def _error_shape(self, tok: str, camel: bool, sentence: str) -> bool:
        """Does `tok` have the shape of a protocol error name? CamelCase or UPPER_SNAKE spelling of a real error,
        or a near miss of one; an UPPER_SNAKE name also when the sentence speaks of an error."""
        norm = _norm(tok[:-9] if tok.endswith("Exception") else tok)
        if norm in self._error_norm:
            return True
        if len(norm) >= 12 and difflib.get_close_matches(norm, list(self._error_norm), n=1, cutoff=0.9):
            return True
        return (not camel) and bool(re.search(r"\b(errors?|exceptions?|raises?|returns?)\b", sentence, re.I))

    def _error_item(self, tok, no, sent, source, camel) -> List[Item]:
        if not self._error_shape(tok, camel, sent):
            return []
        return [Item(tok, "error_name", no, sent, source, {"camel": True} if camel else {})]

    # -------- prose and inline code
    def _extract_text_line(self, no, line, seg, recent_tool) -> Tuple[List[Item], Optional[str]]:
        out: List[Item] = []
        last_tool = None
        # inline code spans
        spans = [(m.start(), m.end(), m.group(0).strip("`").strip()) for m in INLINE_CODE_RE.finditer(line)]
        prose = list(line)
        for a, b, _ in spans:
            for k in range(a, b):
                prose[k] = " "
        prose_s = "".join(prose)
        prose_s = re.sub(r"[*_]{2,}", "  ", prose_s)
        for a, b, content in spans:
            sent = _sentence_of(line, a)
            its, tool = self._span_items(content, no, sent, line, seg, recent_tool if last_tool is None else (last_tool, no))
            out += its
            if tool:
                last_tool = tool
        # bare prose tokens
        for m in TOOL_RE.finditer(prose_s):
            t = m.group(1).split("/")[-1]
            t = t if t.endswith(".sh") else t + ".sh"
            out.append(Item(t, "cli_tool", no, _sentence_of(line, m.start()), "prose", {"tool": t}))
            last_tool = t
        for m in HTTP_RE.finditer(prose_s):
            out.append(Item("%s %s" % (m.group(1), m.group(2).rstrip(".,;:)")), "http_op", no, _sentence_of(line, m.start()),
                            "prose", {"method": m.group(1), "path": m.group(2).rstrip(".,;:)")}))
        tool_here = last_tool or (recent_tool[0] if no - recent_tool[1] <= 2 else None)
        for m in ANNOT_RE.finditer(prose_s):
            tok = m.group(1)
            sent = _sentence_of(line, m.start())
            out += self._annotation_items(tok, prose_s[m.end():], no, sent, "prose")
        for m in DOTTED_RE.finditer(prose_s):
            tok = m.group(0).rstrip(".")
            if not self._is_config_key(tok) or "/" in prose_s[max(0, m.start() - 1):m.start()]:
                continue
            sent = _sentence_of(line, m.start())
            ctx = {}
            sc = self._scope_hint(sent, seg)
            if sc:
                ctx["scope"] = sc
            out.append(Item(tok, "config_key", no, sent, "prose", ctx))
        for m in UPPER_SNAKE_RE.finditer(prose_s):
            out += self._error_item(m.group(0), no, _sentence_of(line, m.start()), "prose", False)
        for m in CAMEL_RE.finditer(prose_s):
            out += self._error_item(m.group(0), no, _sentence_of(line, m.start()), "prose", True)
        # field paths written bare in prose
        for m in FIELD_PATH_RE.finditer(prose_s):
            sent = _sentence_of(line, m.start())
            out += self._path_item(m.group(1), m.group(2), no, sent, "prose", line, seg)
        # flags after a tool in the same line
        if last_tool or tool_here:
            tool = last_tool or tool_here
            for m in FLAG_RE.finditer(prose_s):
                if tool and (last_tool is None or prose_s.find(last_tool.replace(".sh", "")) < m.start()):
                    out.append(Item(m.group(1), "cli_flag", no, _sentence_of(line, m.start()), "prose", {"tool": tool}))
        # `key` = `value` in prose, and alternative values after a key ("to `all` or `class`")
        out += self._prose_values(line, no, seg)
        for a, b, content in spans:
            km = KV_RE.match(content)
            key, given = (km.group(1), True) if (km and "=" in content) else (content, False)
            if self._is_config_key(key):
                sent = _sentence_of(line, a)
                out += self._alt_items(key, self._following_values(line[b:], given), no, sent, "inline", self._scope_hint(sent, seg))
        for m in DOTTED_RE.finditer(prose_s):
            tok = m.group(0).rstrip(".")
            if self._is_config_key(tok):
                sent = _sentence_of(line, m.start())
                out += self._alt_items(tok, self._following_values(prose_s_with_code(line, m.end()), False), no, sent, "prose", self._scope_hint(sent, seg))
        return out, last_tool

    _FIRST_VALUE = re.compile(r"\s*(?:=|:|\bto\b|\bas\b)\s*`([^`\n]+)`")
    _ALT_VALUE = re.compile(r"\s*\)?\s*(?:,|/|\bor\b|\(or\b|\band\b)\s*`([^`\n]+)`")

    def _following_values(self, rest: str, first_given: bool) -> List[str]:
        """Values written after a key: "key to `a` or `b`" (first_given False) or "key=a (or `b`)" (True)."""
        vals, pos = [], 0
        if not first_given:
            m = self._FIRST_VALUE.match(rest)
            if not m:
                return vals
            vals.append(m.group(1))
            pos = m.end()
        while True:
            m = self._ALT_VALUE.match(rest[pos:])
            if not m:
                return vals
            vals.append(m.group(1))
            pos += m.end()

    def _alt_items(self, key, vals, no, sent, source, scope) -> List[Item]:
        out = []
        for v in vals:
            v = v.strip().strip("\"'")
            if v and not re.search(r"[<>${}\[\]\s]", v) and len(v) < 40:
                ctx = {"key": key, "value": v}
                if scope:
                    ctx["scope"] = scope
                out.append(Item("%s=%s" % (key, v), "config_value", no, sent, source, ctx))
        return out

    def _prose_values(self, line, no, seg) -> List[Item]:
        out = []
        for m in re.finditer(r"`?([a-z][a-z0-9_.\-]*[a-z0-9])`?\s*(?:=|:)\s*`([^`\n]+)`", line):
            key, val = m.group(1), m.group(2).strip()
            if self._is_config_key(key) and not re.search(r"[<>${}\[\]]", val):
                sent = _sentence_of(line, m.start())
                ctx = {"key": key, "value": val}
                sc = self._scope_hint(sent, seg)
                if sc:
                    ctx["scope"] = sc
                out.append(Item("%s=%s" % (key, val), "config_value", no, sent, "inline", ctx))
        for m in re.finditer(r"(?<![`\w.\-])([a-z][a-z0-9_.\-]*[a-z0-9])\s*=\s*([^\s`,;)]+)", line):
            key, val = m.group(1), m.group(2).strip("`.'\"")
            if self._is_config_key(key) and "`" not in line[m.start() - 1:m.start()] and not re.search(r"[<>${}\[\]]", val):
                sent = _sentence_of(line, m.start())
                ctx = {"key": key, "value": val}
                sc = self._scope_hint(sent, seg)
                if sc:
                    ctx["scope"] = sc
                out.append(Item("%s=%s" % (key, val), "config_value", no, sent, "prose", ctx))
        return out

    def _annotation_items(self, tok, after, no, sent, source) -> List[Item]:
        out = [Item(tok, "annotation", no, sent, source, {"role": "annotation"})]
        m = re.match(r"\s*[=:]\s*[\"'`]?([A-Za-z0-9,._\-]+)", after)
        if m and "/" in tok:
            v = m.group(1)
            out.append(Item("%s=%s" % (tok, v), "annotation_value", no, sent, source, {"name": tok, "value": v, "role": "annotation"}))
        return out

    def _path_item(self, kind, path, no, sent, source, line, seg) -> List[Item]:
        kinds = [kind] if kind and kind in self._kinds else kinds_in(sent, self._kinds)
        if not kinds:
            kinds = kinds_in(line + "\n" + seg.heading, self._kinds)
        if kind and kind not in self._kinds:
            return []
        if len(kinds) != 1:
            return []
        p = normalise_path(path)
        return [Item(p, "field_path", no, sent, source, {"kind": kinds[0], "path": p})]

    def _span_items(self, content, no, sent, line, seg, recent_tool) -> Tuple[List[Item], Optional[str]]:
        out: List[Item] = []
        c = content.strip()
        if not c:
            return out, None
        tool = None
        # http
        hm = HTTP_RE.search(c)
        if hm and hm.start() == 0:
            return self._http_in(c, no, sent, "inline"), None
        base = c.split()[0].split("/")[-1]
        if base.startswith("kafka-") or base in ("kubectl", "argocd", "curl", "oc"):
            its = []
            for cmd in re.split(r"\s*(?:\|\||&&|\||;)\s*", c):
                its += self.parse_command(cmd, no, sent, "inline")
            tl = next((i.context["tool"] for i in its if i.type == "cli_tool"), None)
            return its, tl
        fm = re.fullmatch(r"(?:([A-Z][A-Za-z0-9]+)\.)?\.?((?:spec|status)(?:\.[A-Za-z0-9_\-]+(?:\[[^\]\s]*\])?|\[[^\]\s]*\])*)", c)
        if fm and (fm.group(2).count(".") >= 1 or fm.group(1)):
            return self._path_item(fm.group(1), fm.group(2), no, sent, "inline", line, seg), None
        am = ANNOT_RE.match(c)
        if am and (len(am.group(1)) == len(c.split("=")[0].split(":")[0].strip())):
            return self._annotation_items(am.group(1), c[am.end():], no, sent, "inline"), None
        fl = re.match(r"^(--[a-z][a-z0-9-]*)(?:[= ].*)?$", c)
        if fl:
            flag_tool = recent_tool[0] if recent_tool and recent_tool[0] and (no - recent_tool[1] <= 2) else None
            if flag_tool:
                its = [Item(fl.group(1), "cli_flag", no, sent, "inline", {"tool": flag_tool})]
                return its, None
            return out, None
        kv = KV_RE.match(c)
        if kv and self._is_config_key(kv.group(1)):
            key, val = kv.group(1), kv.group(2).strip().strip("\"'`")
            ctx = {}
            if "=" in c:
                hs = self._scope_hint(sent, seg, heading_ok=True)
                return self._config_items(key, val, no, sent, "inline", {"scope": hs} if hs else {}), None
        if self._is_config_key(c):
            ctx = {}
            sc = self._scope_hint(sent, seg)
            if sc:
                ctx["scope"] = sc
            return [Item(c, "config_key", no, sent, "inline", ctx)], None
        if re.fullmatch(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+", c):
            return self._error_item(c, no, sent, "inline", False), None
        if re.fullmatch(r"(?:[A-Z][a-z0-9]+){2,}", c):
            ctx = {"camel": True}
            if c.startswith(("Kafka", "Strimzi")) and re.search(
                    r"`%s`\W{0,3}(?:custom\s+)?(?:resources?|CRD|CR|object)\b|\b(?:kind|resource)\W{0,3}`%s`" % (re.escape(c), re.escape(c)), line):
                ctx["as_kind"] = True
            return self._error_item(c, no, sent, "inline", True) + (
                [Item(c, "type_name", no, sent, "inline", {"as_kind": True})] if ctx.get("as_kind") else []), None
        return out, tool

    # ------------------------------------------------------------------ decisions
    def _decide(self, it: Item, ex: Excerpts, result: CheckResult):
        fn = getattr(self, "_d_" + it.type, None)
        if fn is None:
            it.status = SKIPPED
            return
        fn(it, ex, result)

    def _add(self, result, level, it, rule, evidence, suggestion="", catalogue=None):
        result.findings.append(Finding(level, it, rule, evidence, suggestion, catalogue))

    def _sim(self, similar):
        return ("; nearest real: " + ", ".join("`%s`" % s for s in similar)) if similar else ""

    # config keys
    GENERIC_SEGMENT_DF = 30     # a word that begins or ends this many catalogued keys says little

    def _near_keys(self, key: str) -> List[str]:
        """Real keys that share at least two words with `key`, one of them a distinctive word."""
        segs = set(key.split("."))
        out = []
        for k in self.cats._config:
            shared = segs & set(k.split("."))
            if len(shared) >= 2 and any(self._seg_df.get(g, 0) < self.GENERIC_SEGMENT_DF for g in shared):
                out.append((len(shared) / len(segs | set(k.split("."))), k))
        out.sort(key=lambda t: (-t[0], t[1]))
        return [k for _, k in out[:3]]

    def _is_abbreviation(self, key: str) -> bool:
        return any(k.startswith(key + ".") for k in self.cats._config)

    def _d_config_key(self, it, ex, result):
        key, scope = it.text, it.context.get("scope")
        open_ctx = it.context.get("open")
        in_ex = ex.has(key)
        r = self.cats.config_key(key, scope)
        if r.known:
            it.status = IN_EXCERPTS if in_ex else KNOWN
            return
        if scope and r.not_covered and "scope" in r.reason:
            r = self.cats.config_key(key, None)
            scope = None
        if in_ex:
            it.status = IN_EXCERPTS
            if r.unknown and scope and r.data.get("also_in"):
                self._add(result, WRONG_USE, it, "config_scope", "`%s` is a %s setting, not a %s setting (listed for: %s)" % (
                    key, "/".join(r.data["also_in"]), scope, ", ".join(r.data["also_in"])), "check where it belongs", "kafka_config")
            return
        if r.unknown:
            if open_ctx:
                it.status = NOT_COVERED
                self._add(result, UNVERIFIED, it, "config_open_scope",
                          "`%s` is not in the Kafka reference; connector configs hold connector-specific keys the reference does not list" % key,
                          "", "kafka_config")
                return
            if scope and r.data.get("also_in"):
                it.status = UNKNOWN
                self._add(result, WRONG_USE, it, "config_scope", "`%s` is not a %s setting; the reference lists it for: %s" % (
                    key, scope, ", ".join(r.data["also_in"])), "use it in the right place", "kafka_config")
                return
            if self._is_abbreviation(key):
                it.status = SKIPPED       # "min.insync=2": the start of a real key, written short
                return
            near = r.data.get("similar") or self._near_keys(key)
            if not near:
                # not in the Kafka reference and not near any Kafka key: another component's setting is likely
                it.status = NOT_COVERED
                self._add(result, UNVERIFIED, it, "config_far_from_any_key",
                          "`%s` is not in the Kafka reference and resembles no Kafka key; it may belong to another component" % key,
                          "", "kafka_config")
                return
            it.status = UNKNOWN
            where = ("scope " + scope) if scope else "any scope"
            self._add(result, INVENTED, it, "config_unknown", "not a key of the Kafka 4.3 reference for %s%s" % (
                where, self._sim(near)), "did you mean `%s`?" % near[0], "kafka_config")
            return
        it.status = NOT_COVERED
        if key.startswith(FOREIGN_KEY_PREFIXES):
            self._add(result, UNVERIFIED, it, "foreign_key", "`%s` is a key of another project (%s); no source here documents it" % (
                key, key.split(".")[0]), "", "kafka_config")
        else:
            self._add(result, UNVERIFIED, it, "config_not_covered", r.reason, "", "kafka_config")

    def _d_config_value(self, it, ex, result):
        key, value = it.context["key"], it.context["value"]
        scope = it.context.get("scope")
        if ex.has(it.text):
            it.status = IN_EXCERPTS
            return
        parts = [v.strip() for v in value.split(",") if v.strip()] if "," in value else [value]
        rs = [self.cats.config_value(key, v, scope) for v in parts]
        r = next((x for x in rs if x.unknown), rs[0])
        value = next((v for v, x in zip(parts, rs) if x.unknown), value)
        if all(x.known for x in rs):
            it.status = KNOWN
        elif r.unknown:
            it.status = UNKNOWN
            self._add(result, WRONG_USE, it, "config_value", "`%s` accepts %s, not `%s`" % (
                key, ", ".join("`%s`" % v for v in r.data["valid"]), value),
                "use one of: " + ", ".join(r.data["valid"]), "kafka_config")
        else:
            it.status = SKIPPED

    # field paths
    def _api_versions(self, kind):
        return {e["api_version"] for e in self.cats._kinds.get(kind, []) if e.get("api_version")}

    def _is_ancestor(self, kind, path):
        return any(q.startswith(path + ".") or q.startswith(path + "[")
                   for e in self.cats._kinds.get(kind, []) for q in e["paths"])

    def _near_sibling(self, kind, path):
        """Real fields next to `path` whose name starts with, or is started by, its last word (consumer -> consumerByteRate)
        or is a spelling variant of it."""
        parent, _, leaf = path.rpartition(".")
        leaf = leaf.replace("[]", "").lower()
        out = []
        for e in self.cats._kinds.get(kind, []):
            for q in e["paths"]:
                qp, _, ql = q.rpartition(".")
                ql = ql.replace("[]", "").lower()
                if qp == parent and ql != leaf and (ql.startswith(leaf) or leaf.startswith(ql)
                                                    or difflib.SequenceMatcher(None, ql, leaf).ratio() >= 0.85):
                    out.append(q)
        return sorted(set(out))[:3]

    def _d_field_path(self, it, ex, result):
        kind, path = it.context["kind"], it.context["path"]
        gone = result.unknown_paths
        if any(k == kind and path.startswith(g) and path[len(g):len(g) + 1] in (".", "[") for k, g in gone):
            it.status = SKIPPED          # below a field that was already reported
            return
        r = self.cats.field_path(kind, path)
        in_ex = ex.has_path(path)
        if r.known:
            it.status = IN_EXCERPTS if in_ex else KNOWN
            return
        if in_ex:
            it.status = IN_EXCERPTS
            if r.unknown:
                gone.add((kind, path))      # the excerpts vouch for it: what is below it is not judged against the catalogue
            return
        if r.unknown and self._is_ancestor(kind, path):
            it.status = KNOWN            # a node on the way to listed fields (the catalogue lists the leaves)
            return
        if r.unknown:
            it.status = UNKNOWN
            gone.add((kind, path))
            sim = r.data.get("similar") or []
            api = it.context.get("api_version")
            if api and api not in self._api_versions(kind):
                near = self._near_sibling(kind, path)
                if not near:
                    it.status = NOT_COVERED
                    self._add(result, UNVERIFIED, it, "field_other_api_version",
                              "`%s` is not a field of %s in %s, the version the Strimzi reference describes; the answer uses %s, "
                              "where it may have existed" % (path, kind, "/".join(sorted(self._api_versions(kind))), api), "", r.catalogue)
                    return
                sim = near
            self._add(result, INVENTED, it, "field_unknown", "`%s` is not a field of %s in the Strimzi API reference%s" % (
                path, kind, self._sim(sim)), ("did you mean `%s`?" % sim[0]) if sim else "", r.catalogue)
            return
        it.status = NOT_COVERED
        if "free-form" in r.reason:
            it.status = SKIPPED
            return
        self._add(result, UNVERIFIED, it, "field_not_covered", r.reason, "", r.catalogue)

    # tools and flags
    def _d_cli_tool(self, it, ex, result):
        tool = it.text
        if it.context.get("argocd"):
            r = self.cats.argocd_command(tool)
            if r.known:
                it.status = KNOWN
            elif r.unknown:
                it.status = UNKNOWN
                self._add(result, INVENTED, it, "argocd_command", "not a command of the argocd CLI reference%s" % self._sim(r.data.get("similar")),
                          "", "argocd")
            else:
                it.status = NOT_COVERED
            return
        if ex.has(tool) or ex.has(tool.replace(".sh", "")):
            it.status = IN_EXCERPTS
            return
        if tool in self.cats.tools():
            it.status = KNOWN
            return
        it.status = NOT_COVERED
        near = difflib.get_close_matches(tool, [t for t in self.cats.tools() if t.startswith("kafka-")], n=2, cutoff=0.85)
        self._add(result, UNVERIFIED, it, "tool_not_covered", "`%s` is not among the Kafka tools the documentation shows (the list of tools is not marked complete)%s" % (tool, self._sim(near)),
                  "", "kafka_cli")

    def _d_cli_flag(self, it, ex, result):
        flag, tool = it.text, it.context["tool"]
        if ex.has_flag(flag, tool.split()[0] if tool.startswith("kubectl") else tool):
            it.status = IN_EXCERPTS
            return
        r = self.cats.cli_option(tool, flag)
        if r.known:
            it.status = KNOWN
            return
        if tool.startswith("kubectl") and flag in self._kubectl_opts:
            it.status = KNOWN
            return
        if r.unknown:
            it.status = UNKNOWN
            sim = r.data.get("similar") or []
            self._add(result, INVENTED, it, "flag_unknown", "not a known option of %s in the %s reference%s" % (
                tool, "Kafka 4.3" if tool.startswith("kafka") else "generated", self._sim(sim)),
                ("did you mean `%s`?" % sim[0]) if sim else "", r.catalogue)
            return
        it.status = NOT_COVERED
        if "is in no catalogue" in r.reason:
            self._add(result, UNVERIFIED, it, "flag_tool_not_covered", "tool `%s` is in no catalogue" % tool, "", None)
            return
        self._add(result, UNVERIFIED, it, "flag_not_covered",
                  "not a known option of %s in Kafka 4.3 as far as the documentation shows (its option list is not complete)" % tool, "",
                  r.catalogue)

    # HTTP
    def _d_http_op(self, it, ex, result):
        method, path = it.context["method"], it.context["path"]
        segs = self.cats.rest_segments(path)
        if ex.has(it.text):
            it.status = IN_EXCERPTS
            return
        if segs and segs[0] not in self._rest_first and not (len(segs) == 1 and segs[0] in ("",)):
            it.status = SKIPPED          # another REST interface (metrics, Schema Registry, Cruise Control): not catalogued
            return
        r = self.cats.rest_operation(method, path)
        if r.known:
            it.status = KNOWN
        elif r.unknown and r.data.get("allowed_methods"):
            it.status = UNKNOWN
            am = r.data["allowed_methods"]
            self._add(result, WRONG_USE, it, "http_method", "the Connect REST interface serves `%s` with %s, not %s" % (
                r.data["template"], "/".join(am), method), "use %s" % "/".join(am), "connect_rest")
        elif r.unknown:
            it.status = UNKNOWN
            self._add(result, INVENTED, it, "http_path", "no such path in the Connect REST interface", "", "connect_rest")
        else:
            it.status = NOT_COVERED

    # protocol errors (and the CamelCase shape)
    def _d_error_name(self, it, ex, result):
        tok = it.text
        camel = it.context.get("camel")
        norm = _norm(tok[:-9] if tok.endswith("Exception") else tok)
        if ex.has(tok):
            it.status = IN_EXCERPTS
            return
        if norm in self._error_norm:
            it.status = KNOWN
            return
        near = difflib.get_close_matches(norm, list(self._error_norm), n=1, cutoff=0.9)
        sentence_error = bool(re.search(r"\b(error|errors|exception|raises?|returns?)\b", it.sentence, re.I))
        if near and len(norm) >= 12:
            name = self._error_norm[near[0]]
            it.status = UNKNOWN
            self._add(result, INVENTED, it, "error_name", "`%s` is not a protocol error name; the nearest real one is `%s`" % (tok, name),
                      "use `%s`" % name, "protocol_errors")
            return
        if not camel and sentence_error and self.cats.data["protocol_errors"]:
            r = self.cats.error_name(tok)
            if r.unknown:
                it.status = UNKNOWN
                self._add(result, INVENTED, it, "error_name", "not an error of the Kafka protocol reference%s" % self._sim(r.data.get("similar")),
                          "", "protocol_errors")
                return
        it.status = SKIPPED

    def _d_type_name(self, it, ex, result):
        if ex.has(it.text):
            it.status = IN_EXCERPTS
            return
        r = self.cats.strimzi_type(it.text)
        if r.known:
            it.status = KNOWN
        elif r.unknown and it.context.get("as_kind"):
            it.status = UNKNOWN
            self._add(result, INVENTED, it, "strimzi_type", "no such kind or type in the Strimzi API reference%s" % self._sim(r.data.get("similar")), "", "strimzi_resources")
        else:
            it.status = SKIPPED

    # annotations and labels
    def _d_annotation(self, it, ex, result):
        name = it.text
        if ex.has(name):
            it.status = IN_EXCERPTS
            return
        r = self.cats.annotation(name)
        if r.known:
            it.status = KNOWN
        elif r.unknown:
            it.status = UNKNOWN
            self._add(result, INVENTED, it, "annotation_unknown", "`%s` is not mentioned in the Strimzi documentation" % name, "", "strimzi_annotations")
        elif name.startswith("strimzi.io/"):
            it.status = NOT_COVERED
            sim = r.data.get("similar") or []
            self._add(result, UNVERIFIED, it, "annotation_not_covered", "`%s` is not among the names the Strimzi documentation mentions (that list is not complete)%s" % (name, self._sim(sim)), "", "strimzi_annotations")
        else:
            it.status = SKIPPED

    def _d_annotation_value(self, it, ex, result):
        name, value = it.context["name"], it.context["value"]
        if ex.has(it.text) or ex.has(value) and ex.has(name):
            it.status = IN_EXCERPTS
            return
        r = self.cats.annotation(name, value)
        if not r.known:
            it.status = SKIPPED
            return
        seen = r.data["values_seen"]
        if value in seen or not seen:
            it.status = KNOWN
            return
        near = [s for s in seen if _norm(s) == _norm(value) or difflib.SequenceMatcher(None, s.lower(), value.lower()).ratio() >= 0.85]
        booleanish = all(s in ("true", "false", "0", "1") for s in seen)
        if near:
            it.status = UNKNOWN
            self._add(result, WRONG_USE, it, "annotation_value", "documented values of `%s` are %s; `%s` differs only in spelling or case" % (
                name, ", ".join("`%s`" % s for s in seen), value), "use `%s`" % near[0], "strimzi_annotations")
        elif booleanish and re.fullmatch(r"[A-Za-z]+", value) and value not in seen:
            it.status = UNKNOWN
            self._add(result, WRONG_USE, it, "annotation_value", "documented values of `%s` are %s, not `%s`" % (
                name, ", ".join("`%s`" % s for s in seen), value), "use one of the documented values", "strimzi_annotations")
        else:
            it.status = SKIPPED


# ----------------------------------------------------------------------------- module-level API
_default: Optional[Checker] = None


def check_answer(text: str, excerpts=None, catalogues: Optional[Catalogues] = None) -> CheckResult:
    """Check one answer. `excerpts` is the text (or a list of texts) the answer was written from, or None."""
    global _default
    if catalogues is not None:
        ck = Checker(catalogues)
    else:
        if _default is None:
            _default = Checker()
        ck = _default
    ck._unchecked_now = {}
    res = ck.check(text, excerpts)
    res.unchecked_kinds = dict(ck._unchecked_now)
    return res


def _group_unverified(findings: List[Finding]) -> List[Tuple[str, int]]:
    """Unverified findings as (label, number of names): keys of another project that share a prefix are one
    group (`spring.kafka.consumer.*`), every other name stands alone. Order of first appearance."""
    order: Dict[str, int] = {}
    members: Dict[str, set] = {}
    for f in findings:
        t = f.item.text
        label = t
        if f.rule == "foreign_key" and t.count(".") >= 2:
            label = t.rsplit(".", 1)[0] + ".*"
        order.setdefault(label, len(order))
        members.setdefault(label, set()).add(t)
    return [(l, len(members[l])) for l in sorted(order, key=order.get)]


def render_note(result: CheckResult, max_unverified: int = 5) -> str:
    """A short note for the user: what could not be confirmed. Empty when there is nothing to say.
    invented and wrong_use are listed one by one, each with its evidence. Unverified names are information,
    not an accusation: they come on one separate line, grouped by shared prefix, at most `max_unverified`
    groups, then "and N more"."""
    lines = []
    for f in result.by_level(INVENTED) + result.by_level(WRONG_USE):
        ev = f.evidence
        lead = "`%s` " % f.item.text
        if ev.startswith(lead):
            ev = ev[len(lead):]
        s = "Could not confirm: `%s` (%s)" % (f.item.text, ev)
        if f.suggestion and f.suggestion not in ev and not (
                f.suggestion.startswith("did you mean") and "nearest real" in ev):
            s += "; " + f.suggestion
        lines.append(s)
    groups = _group_unverified(result.by_level(UNVERIFIED))
    if groups:
        shown = ["`%s`%s" % (l, " (%d keys)" % n if n > 1 else "") for l, n in groups[:max_unverified]]
        more = len(groups) - max_unverified
        lines.append("Not checked, no reference covers them: " + ", ".join(shown) + (" and %d more" % more if more > 0 else ""))
    return "\n".join(lines)


def checked_count(result: CheckResult) -> int:
    """How many distinct names were looked up and confirmed (in the excerpts or in a catalogue). Items that were
    reported, skipped or not covered are not counted. Added for `ask --check`; no decision depends on it."""
    return sum(1 for i in result.items if i.status in (IN_EXCERPTS, KNOWN))
