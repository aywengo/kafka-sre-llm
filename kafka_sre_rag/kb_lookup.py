"""M7 K7: exact lookup from the pinned catalogues, as short reference cards (not wired into the runtime).

    cards(text, sources=("checker", "profiles"))  -> [Card]   identifiers of `text` resolved against the catalogues
    render(cards, max_chars=DEFAULT_MAX_CHARS)     -> str      one text block for a prompt ("" when nothing fits)

Identifiers come from the two existing extractors and nothing is parsed here a second time:
  "checker"   checker.Checker.extract: typed items of an answer-like text (config keys, flags with their tool,
              Strimzi / Argo CD / Kubernetes field paths with their kind, annotations, error names, REST lines);
  "profiles"  profiles.Lookup.identifiers: bare names of a question (lower-cased by that function; matched here
              without regard to case). It cannot tell the tool of a flag or the kind of a path, so those give no card.
The catalogues are read through catalogue.Catalogues (not a second parser) and their recorded hash is checked.

A name the catalogues do not hold gives no card and no statement (the checker's job; catalogues can be incomplete,
the tools catalogue is marked so). A key known in several scopes gives ONE merged card: the scopes are listed and
the facts that differ per scope get a line per scope, so one name costs one card of the budget.
No network, no model, no file written.
"""
import json
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .catalogue import Catalogues, catalogue_hash, default_dir, normalise_path
from .checker import Checker
from .profiles import Lookup as _ProfileLookup

SOURCES = ("checker", "profiles")
DEFAULT_MAX_CHARS = 4000          # see runs/m7/reports/k7_lookup.md for the measurements behind it
MAX_CARD_CHARS = 1100             # one card is cut to whole lines within this
DESC_CHARS = 280                  # description of a key or option, cut at a sentence end within this
HEADER = ("REFERENCE CARDS from the pinned catalogues (exact names, defaults and values; these are not documentation "
          "excerpts). A name without a card is not thereby wrong: the catalogues can be incomplete.")

_PIN_COMMIT = re.compile(r"@[0-9a-f]{40}#")


class CatalogueHashError(RuntimeError):
    pass


@dataclass(frozen=True)
class Card:
    ident: str                          # exactly as catalogued (kind-specific: key, "tool --flag", "Kind path", ...)
    kind: str                           # config_key cli_option cli_tool field_path resource_kind annotation error_name rest_path
    product: str                        # "Kafka 4.3", "Strimzi 1.3.1", "Argo CD 3.6.1", "Kubernetes 1.37.2"
    pin: str                            # pin of the catalogue entry (commit hash shortened)
    facts: Tuple[Tuple[str, str], ...]  # (label, text) in display order

    def text(self) -> str:
        head = "- %s `%s` [%s; pin %s]" % (self.kind.replace("_", " "), self.ident, self.product, self.pin)
        lines = [head] + ["    %s: %s" % kv for kv in self.facts]
        return "\n".join(lines)


def cut_sentence(s: str, limit: int) -> str:
    """`s` whole when it fits, else cut at the last sentence end within `limit`, else at a word with an ellipsis."""
    s = " ".join((s or "").split())
    if len(s) <= limit:
        return s
    head = s[:limit]
    ends = [m.end() for m in re.finditer(r"[.!?](?=\s|$)", head)]
    if ends and ends[-1] >= limit // 3:
        return head[:ends[-1]]
    return head.rsplit(" ", 1)[0].rstrip(",;:") + " ..."


def _pin(p) -> str:
    return _PIN_COMMIT.sub("#", (p or "").split(" ; ")[0])


def _cat_version(cats: Catalogues, name: str, default: str) -> str:
    v = (cats.data.get(name) or {}).get("versions") or []
    return v[0] if v else default


def _cap_lines(card: Card, limit: int = MAX_CARD_CHARS) -> Card:
    """Drop trailing facts until the card fits `limit` (a card is cut by fact lines, never inside one)."""
    facts = list(card.facts)
    while facts and len(Card(card.ident, card.kind, card.product, card.pin, tuple(facts)).text()) > limit:
        facts.pop()
    return Card(card.ident, card.kind, card.product, card.pin, tuple(facts))


class KbLookup:
    def __init__(self, cats: Catalogues, directory=None, verify: bool = True):
        self.catalogue_sha256 = self._verify(Path(directory) if directory else default_dir()) if verify else None
        self.cats = cats
        self.checker = Checker(cats)
        self._lc_keys = {k.lower(): k for k in cats._config}
        self._lc_kinds: Dict[str, str] = {k.lower(): k for k in cats._kinds}
        self._lc_ann = {k.lower(): k for k in cats._ann}
        self._lc_err = {e["name"].lower(): e["name"] for e in (cats.data["protocol_errors"] or {}).get("entries", [])}
        self._hist: Dict[str, List[dict]] = {}
        for e in (cats.data["kafka_config_history"] or {}).get("entries", []):
            self._hist.setdefault(e["key"], []).append(e)

    @classmethod
    def load(cls, directory=None) -> "KbLookup":
        d = Path(directory) if directory else default_dir()
        return cls(Catalogues.load(d), d, verify=True)

    @staticmethod
    def _verify(directory: Path):
        """The recorded hash (CATALOGUES.json) must equal the hash of the files on disk, as the checker's report does."""
        rec = json.loads((directory / "CATALOGUES.json").read_text())["catalogue_hash"]
        now = catalogue_hash(directory)
        if rec != now:
            raise CatalogueHashError("catalogue hash %s is not the recorded %s" % (now[:12], rec[:12]))
        return now

    # ------------------------------------------------------------------ public
    def cards(self, text: str, sources: Sequence[str] = SOURCES, terms: str = "") -> List[Card]:
        """terms: the comma-separated search terms of the question (profiles source only; as profiles.Lookup
        reads them). "" changes nothing."""
        out: Dict[tuple, Tuple[int, Card]] = {}
        low = (text + "\n" + terms).lower() if terms else text.lower()

        def at(*names):
            ps = [low.find(str(n).lower()) for n in names if n]
            ps = [p for p in ps if p >= 0]
            return min(ps) if ps else len(text)

        def add(key, card, *names):
            if card is not None and key not in out:
                out[key] = (at(*names), card)

        if "checker" in sources:
            for it in self.checker.extract(text):
                self._from_item(it, add)
        if "profiles" in sources:
            for name in _ProfileLookup.identifiers(text, terms):
                self._from_name(name, add)
        # order of first mention in the text (stable: ties keep the extractor order)
        return [c for _, c in sorted(out.values(), key=lambda x: x[0])]

    # ------------------------------------------------------------------ resolution
    def _from_item(self, it, add):
        t, c = it.type, it.context
        if t == "config_key":
            k = self._lc_keys.get(it.text.lower())
            if k:
                add(("config_key", k), self.config_card(k), it.text)
        elif t == "cli_flag":
            tool = c.get("tool")
            if tool:
                add(("cli_option", tool, it.text), self.option_card(tool, it.text), it.text)
        elif t == "cli_tool":
            add(("cli_tool", it.text), self.tool_card(it.text), it.text)
        elif t == "field_path":
            kind, path = c.get("kind"), c.get("path")
            if kind and path:
                add(("kind", kind), self.kind_card(kind), kind)
                add(("field_path", kind, normalise_path(path)), self.field_card(kind, path), path, it.text)
        elif t == "api_version":
            if c.get("kind"):
                add(("kind", c["kind"]), self.kind_card(c["kind"]), c["kind"])
        elif t == "annotation":
            add(("annotation", it.text), self.annotation_card(it.text), it.text)
        elif t == "error_name":
            add(("error_name", it.text), self.error_card(it.text), it.text)
        elif t == "http_op":
            add(("rest", c.get("path")), self.rest_card(c.get("path", ""), c.get("method")), c.get("path"))

    def _from_name(self, name, add):
        n = name.lower()
        if n in self._lc_keys:
            add(("config_key", self._lc_keys[n]), self.config_card(self._lc_keys[n]), name)
        elif n in self._lc_ann:
            add(("annotation", self._lc_ann[n]), self.annotation_card(self._lc_ann[n]), name)
        elif n in self._lc_err:
            add(("error_name", self._lc_err[n]), self.error_card(self._lc_err[n]), name)
        elif n in self._lc_kinds:
            add(("kind", self._lc_kinds[n]), self.kind_card(self._lc_kinds[n]), name)
        elif n.endswith(".sh") or n in self.cats._tools:
            add(("cli_tool", n), self.tool_card(n), name)
        # a bare flag, a dotted name that is no key and anything else: no card (tool or kind unknown)

    # ------------------------------------------------------------------ cards
    def config_card(self, key: str) -> Optional[Card]:
        ents = self.cats._config.get(key)
        if not ents:
            return None
        ents = sorted(ents, key=lambda e: e["scope"])
        scopes = [e["scope"] for e in ents]
        ver = _cat_version(self.cats, "kafka_config", "?")
        facts = [("scopes", ", ".join(scopes))]
        base = ents[0]
        for label, f in (("type", lambda e: e.get("type")),
                         ("default", lambda e: e.get("default")),
                         ("valid values", lambda e: e.get("valid_values"))):
            vals = {s: f(e) for s, e in zip(scopes, ents)}
            uniq = {v for v in vals.values() if v not in (None, "")}
            if len(uniq) == 1 and len({str(v) for v in vals.values()}) == 1:
                facts.append((label, cut_sentence(str(next(iter(uniq))), 160)))
            elif uniq:
                facts.append((label, "; ".join("%s: %s" % (s, cut_sentence(str(v), 120) if v not in (None, "") else "-")
                                               for s, v in vals.items())))
            elif label == "default":
                facts.append((label, "(empty)"))
        descs = {e.get("description") for e in ents}
        if len(descs) == 1:
            facts.append(("description", cut_sentence(base.get("description", ""), DESC_CHARS)))
        else:
            for s, e in zip(scopes[:3], ents[:3]):
                facts.append(("description (%s)" % s, cut_sentence(e.get("description", ""), DESC_CHARS // 2)))
        for h in sorted(self._hist.get(key, []), key=lambda e: e["scope"]):
            facts.append(("history (%s)" % h["scope"], _history_line(h)))
        return _cap_lines(Card(key, "config_key", "Kafka " + ver, _pin(base.get("pin")), tuple(facts)))

    def option_card(self, tool: str, option: str) -> Optional[Card]:
        r = self.cats.cli_option(tool, option)
        if not r.known:
            return None
        d = r.data
        facts = []
        if d.get("takes_argument") is not None:
            facts.append(("argument", str(d["takes_argument"])))
        if d.get("required"):
            facts.append(("required", "yes"))
        if d.get("aliases"):
            facts.append(("aliases", ", ".join(d["aliases"])))
        if d.get("description"):
            facts.append(("description", cut_sentence(d["description"], DESC_CHARS)))
        facts.append(("evidence", r.evidence or "-"))
        cat = r.catalogue
        ent = next((e for e in self.cats._tools.get(_norm_tool(self.cats, tool), []) if e["_catalogue"] == cat), None)
        prod = {"kafka_cli": "Kafka 4.3.2", "argocd": "Argo CD " + _cat_version(self.cats, "argocd", "?").split()[-1].lstrip("v"),
                "kubernetes": "Kubernetes " + _cat_version(self.cats, "kubernetes", "?").split()[-1].lstrip("v")}.get(cat, cat)
        return _cap_lines(Card("%s %s" % (_norm_tool(self.cats, tool), option.split("=")[0]), "cli_option", prod,
                               _pin((ent or {}).get("pin")), tuple(facts)))

    def tool_card(self, tool: str) -> Optional[Card]:
        name = _norm_tool(self.cats, tool)
        ents = self.cats._tools.get(name)
        if not ents:
            return None
        e = ents[0]
        cat = e["_catalogue"]
        facts = []
        if e.get("description"):
            facts.append(("description", cut_sentence(e["description"], DESC_CHARS)))
        opts = sorted(e["options"])
        if opts:
            shown, used = [], 0
            for o in opts:
                if used + len(o) + 2 > 520:
                    break
                shown.append(o)
                used += len(o) + 2
            more = " (+%d more)" % (len(opts) - len(shown)) if len(shown) < len(opts) else ""
            facts.append(("options" + ("" if e.get("complete") else ", list incomplete"), ", ".join(shown) + more))
        elif e.get("reason"):
            facts.append(("note", cut_sentence(e["reason"], 160)))
        if not facts:
            return None
        prod = {"kafka_cli": "Kafka 4.3.2", "argocd": "Argo CD " + _cat_version(self.cats, "argocd", "?").split()[-1].lstrip("v"),
                "kubernetes": "Kubernetes " + _cat_version(self.cats, "kubernetes", "?").split()[-1].lstrip("v")}.get(cat, cat)
        return _cap_lines(Card(name, "cli_tool", prod, _pin(e.get("pin")), tuple(facts)))

    def _entry(self, kind: str):
        """The schema entry of a kind: CRD or OpenAPI first (complete), then the documentation reference."""
        es = self.cats._kinds.get(kind, [])
        for e in es:
            if e.get("source") in ("crd", "openapi") and e.get("complete"):
                return e
        return es[0] if es else None

    def _product(self, e) -> str:
        cat = e["_catalogue"]
        v = _cat_version(self.cats, cat, "?").split()[-1].lstrip("v")
        return {"strimzi_resources": "Strimzi ", "argocd": "Argo CD ", "kubernetes": "Kubernetes "}.get(cat, cat + " ") + v

    def field_card(self, kind: str, path: str) -> Optional[Card]:
        p = normalise_path(path)
        if p in ("spec", "status") or p.split(".")[0] in ("apiVersion", "kind", "metadata"):
            return None
        # direct lookup (Catalogues.field_path builds close-match lists for unknown paths: 0.8 s on a long answer)
        e = self._entry(self._lc_kinds.get(kind.lower(), kind))
        if e is None:
            return None
        paths = e["paths"]
        if p not in paths and not p.startswith(("spec.", "status.")) and "spec." + p in paths:
            p = "spec." + p
        info = paths.get(p)
        if info is None:
            return None
        facts = [("apiVersion", e["api_version"]),
                 ("type", ", ".join(info.get("type", ["?"])))]
        if info.get("enum"):
            facts.append(("enum", ", ".join(str(v) for v in info["enum"])))
        if info.get("required"):
            facts.append(("required", "yes"))
        if info.get("open"):
            facts.append(("note", "free-form; names below it are not catalogued"))
        return Card("%s %s" % (e["kind"], p), "field_path", self._product(e), _pin(e.get("pin")), tuple(facts))

    def kind_card(self, kind: str) -> Optional[Card]:
        k = self._lc_kinds.get(kind.lower())
        e = self._entry(k) if k else None
        if not e:
            return None
        paths = e["paths"]
        tops = [q for q in paths if q.startswith("spec.") and q.count(".") == 1 and "[" not in q]
        if not tops:
            tops = [q for q in paths if "." not in q and "[" not in q]
        names = []
        for q in sorted(tops):
            n = q.split(".")[-1]
            names.append(n + ("*" if paths[q].get("required") else ""))
        shown, used = [], 0
        for n in names:
            if used + len(n) + 2 > 600:
                break
            shown.append(n)
            used += len(n) + 2
        more = " (+%d more)" % (len(names) - len(shown)) if len(shown) < len(names) else ""
        vers = sorted({x["api_version"] for x in self.cats._kinds[k] if x.get("source") in ("crd", "openapi")}
                      or {x["api_version"] for x in self.cats._kinds[k]})
        facts = [("apiVersion served", ", ".join(vers)),
                 ("fields below %s (* required)" % ("spec" if any(q.startswith("spec.") for q in tops) else "the root"),
                  ", ".join(shown) + more)]
        return _cap_lines(Card(k, "resource_kind", self._product(e), _pin(e.get("pin")), tuple(facts)))

    def annotation_card(self, name: str) -> Optional[Card]:
        r = self.cats.annotation(name)
        if not r.known:
            return None
        e = r.entries[0]
        facts = [("role", e.get("kind", "?"))]
        facts.append(("values seen", ", ".join(map(str, e["values_seen"][:8])) if e.get("values_seen")
                      else "none documented; the catalogue lists names the docs mention, not every name the operator reads"))
        return Card(e["name"], "annotation", "Strimzi " + _cat_version(self.cats, "strimzi_resources", "?").split()[-1],
                    _pin(e.get("pin")), tuple(facts))

    def error_card(self, name: str) -> Optional[Card]:
        r = self.cats.error_name(self._lc_err.get(name.lower(), name))
        if not r.known:
            return None
        e = r.entries[0]
        return Card(e["name"], "error_name", "Kafka " + _cat_version(self.cats, "protocol_errors", "?"), _pin(e.get("pin")),
                    (("code", str(e["code"])), ("retriable", "yes" if e["retriable"] else "no"),
                     ("description", cut_sentence(e.get("description", ""), DESC_CHARS))))

    def rest_card(self, path: str, method: Optional[str] = None) -> Optional[Card]:
        tpl = self.cats.rest_template(path)
        if not tpl:
            return None
        ops = sorted((e for e in self.cats.data["connect_rest"]["entries"] if e["path"] == tpl), key=lambda e: e["method"])
        facts = [("operations", "; ".join("%s (%s)" % (e["method"], cut_sentence(e.get("summary", ""), 110)) for e in ops))]
        return _cap_lines(Card(tpl, "rest_path", "Kafka " + _cat_version(self.cats, "connect_rest", "?"),
                               _pin(ops[0].get("pin")), tuple(facts)))


def _norm_tool(cats: Catalogues, tool: str) -> str:
    from .catalogue import _tool_name
    t = _tool_name(tool)
    return t


def _hist_vals(v) -> str:
    if v is None:
        return "absent"
    return ", ".join("%s=%s" % (k, cut_sentence(str(x), 60)) for k, x in v.items() if k != "importance" and x not in (None, ""))


def _history_line(h: dict) -> str:
    parts = ["changes: " + ", ".join(h["changes"])]
    for ver in ("4.1", "4.2", "4.3"):
        if ver in h["versions"]:
            parts.append("%s %s" % (ver, _hist_vals(h["versions"][ver])))
    return "; ".join(parts)


# ---------------------------------------------------------------------------------------- module level
def render_counted(cards: Sequence[Card], max_chars: int = DEFAULT_MAX_CHARS, header: str = HEADER) -> Tuple[str, int]:
    """(block, number of cards in it). The block is the header, then whole cards in the given order (order of first
    mention). A card that does not fit in what is left is skipped and later, smaller cards may still fit. ("", 0)
    when no card fits: nothing, not even the header, is then produced."""
    out, used = [], len(header) + 1
    for c in cards:
        t = c.text()
        if used + len(t) + 1 > max_chars:
            continue
        out.append(t)
        used += len(t) + 1
    return (header + "\n" + "\n".join(out), len(out)) if out else ("", 0)


def render(cards: Sequence[Card], max_chars: int = DEFAULT_MAX_CHARS) -> str:
    return render_counted(cards, max_chars)[0]


def answer_block(question: str, terms: str = "", *, kb: Optional[KbLookup] = None,
                 max_chars: int = DEFAULT_MAX_CHARS, header: Optional[str] = None) -> Tuple[str, dict]:
    """M7 KA1: the lookup cards of an ANSWER prompt: the identifiers of the question and of the frozen search terms
    (`profiles` source), rendered under the header of prompts.LOOKUP_CARDS_HEADER within `max_chars` (header
    included). Returns (block, info); block is "" when no card was found or fits (no heading then). info:
    cards_found, cards (in the block), chars, seconds (the whole lookup), catalogue_recorded_sha256 (the catalogue hash
    recorded in CATALOGUES.json and checked at load; not the hash of the directory files that eval2 puts in its manifest)."""
    from . import prompts
    t0 = time.monotonic()
    kb = kb or get()
    found = kb.cards(question, ("profiles",), terms or "")
    block, n = render_counted(found, max_chars, header if header is not None else prompts.LOOKUP_CARDS_HEADER)
    return block, {"cards_found": len(found), "cards": n, "chars": len(block),
                   "seconds": round(time.monotonic() - t0, 6), "catalogue_recorded_sha256": kb.catalogue_sha256}


_kb: Optional[KbLookup] = None
_lock = threading.Lock()


def get() -> KbLookup:
    """The catalogues are loaded once (hash checked) and shared."""
    global _kb
    with _lock:
        if _kb is None:
            _kb = KbLookup.load()
        return _kb


def cards(text: str, *, sources: Sequence[str] = SOURCES, kb: Optional[KbLookup] = None) -> List[Card]:
    return (kb or get()).cards(text, sources)
