"""M7 KB phase 2 (EXPERIMENTAL, unmeasured): the fact entries' schema, rendering and mechanical gate.

Spec: runs/m7/reports/kb2_design.md sections 1 to 3 (and 5 for the rendered form). Stdlib only; no model, no server.

  window_of(slice_chunks, i)     the window of the i-th slice chunk: it, plus the previous and next chunk of the same file
  window_text(window, target)    the window as the writer and the checker see it (a header line per chunk, then the chunk
                                 as the model receives it: pack_text's title line, a newline, the text)
  check_reply(obj, ...)          the reply's schema (retried when broken) and the gate of each entry
  gate_entry(e, window, target)  the reasons an entry fails the mechanical gate ([] = passes)
  finish_entry(...)              the code's fields: id, scope, pinned sources, provenance
  render(entry)                  the item an entry is in a pack (pack_text input), at most RENDER_MAX characters

The gate (section 3): schema and size limits; every quote a substring of its chunk after whitespace normalisation; quote
ids within the window and within the sources; every source quoted; the target chunk quoted; every number and every
identifier-like token of the title, the statement and applies_when occurs in a quote (kbe/2: or, for a name with a letter,
in the first title line of a source chunk, which finish_entry then adds as a quote); the rendered form within its cap.
"""
import hashlib
import json
import re

from . import prompts

KINDS = ("fact", "value", "procedure", "warning", "cross")
TITLE_MAX, STATEMENT_MAX, APPLIES_MAX, QUOTE_MAX = 100, 400, 200, 300
QUOTES_MIN, QUOTES_MAX, SOURCES_MIN, SOURCES_MAX = 1, 4, 1, 3
ENTRIES_MAX = 8                      # per window
RENDER_MAX = 1500                    # the rendered entry, as a reranker passage
SCOPE = {"strimzi": {"product": "Strimzi", "version": "1.3.1"},
         "apache-kafka": {"product": "Apache Kafka", "version": "4.3.2"}}
SLICE_SOURCES = tuple(SCOPE)
SLICE_EXCLUDE_PREFIX = "raw/strimzi/documentation/contributing/"
# words with a dot that are prose, not identifiers (the gate would otherwise ask a quote to contain them)
ABBREVIATIONS = {"e.g", "i.e", "etc", "vs", "cf"}
_ENTRY_KEYS = {"kind", "title", "statement", "applies_when", "sources", "quotes"}


def in_slice(chunk):
    """The slice rule of section 1: Strimzi and Apache Kafka chunks, except Strimzi's contributor guides."""
    return chunk.get("source") in SLICE_SOURCES and not str(chunk.get("path", "")).startswith(SLICE_EXCLUDE_PREFIX)


def norm_ws(s):
    return re.sub(r"\s+", " ", s or "").strip()


def norm_statement(s):
    return norm_ws(s).lower()


def chunk_view(c):
    """A chunk as the model receives it in a pack: the title line, a newline, the text."""
    return prompts.pack_text([c])


def window_of(chunks, i):
    """chunks: the slice in index order (dicts). The window of chunks[i]: the previous and the next chunk of the same
    file, when they are next to it in index order and in the slice (0 to 2 neighbours). Returns (target id, [chunks])."""
    c = chunks[i]
    out = []
    if i > 0 and chunks[i - 1]["path"] == c["path"]:
        out.append(chunks[i - 1])
    out.append(c)
    if i + 1 < len(chunks) and chunks[i + 1]["path"] == c["path"]:
        out.append(chunks[i + 1])
    return c["id"], out


def window_text(window, target):
    parts = []
    for c in window:
        mark = " (TARGET)" if c["id"] == target else ""
        parts.append(f"=== chunk {c['id']}{mark} ===\n{chunk_view(c)}")
    return "\n\n".join(parts)


def entry_id(prompt_version, sources, statement):
    key = "\n".join([prompt_version, ",".join(sorted(sources)), norm_statement(statement)])
    return "kbe-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


_STRIP = "\"'()[]{}<>,;:!?*“”‘’"


def identifier_tokens(text):
    """The numbers and identifier-like tokens of a text that a quote must contain: backticked spans, and words with a
    digit, an inner '.', '_', '-' or '/', or camelCase. Surrounding punctuation and a sentence's final dot are not part
    of a word; a few prose abbreviations (e.g., i.e., etc.) are not identifiers."""
    text = text or ""
    out = [norm_ws(m) for m in re.findall(r"`([^`]+)`", text) if norm_ws(m)]
    rest = re.sub(r"`[^`]*`", " ", text)
    for w in rest.split():
        w = w.strip(_STRIP)
        while w.endswith("."):
            w = w[:-1]
        w = w.strip(_STRIP)
        if w.endswith("'s") or w.endswith("’s"):
            w = w[:-2]
        if not w or w.lower() in ABBREVIATIONS:
            continue
        if re.search(r"\d", w) or re.search(r"\w[._/\-]\w", w) or re.search(r"[a-z][A-Z]", w):
            out.append(w)
    return out


def _schema_entry(e):
    """Field types and size limits of one entry as the model wrote it."""
    bad = []
    if not isinstance(e, dict):
        return ["entry is not an object"]
    extra = set(e) - _ENTRY_KEYS
    if extra:
        bad.append(f"unknown field(s) {sorted(extra)}")
    if e.get("kind") not in KINDS:
        bad.append(f"kind {e.get('kind')!r} not one of {', '.join(KINDS)}")
    for k, lim in (("title", TITLE_MAX), ("statement", STATEMENT_MAX)):
        v = e.get(k)
        if not isinstance(v, str) or not v.strip():
            bad.append(f"{k} missing or empty")
        elif len(v) > lim:
            bad.append(f"{k} longer than {lim} characters ({len(v)})")
    aw = e.get("applies_when")
    if aw is not None and not isinstance(aw, str):
        bad.append("applies_when is not a string")
    elif aw and len(aw) > APPLIES_MAX:
        bad.append(f"applies_when longer than {APPLIES_MAX} characters ({len(aw)})")
    src = e.get("sources")
    if not isinstance(src, list) or not all(isinstance(s, str) and s for s in src):
        bad.append("sources is not a list of ids")
    elif not SOURCES_MIN <= len(src) <= SOURCES_MAX:
        bad.append(f"{len(src)} sources (1 to 3)")
    elif len(set(src)) != len(src):
        bad.append("a source id is repeated")
    q = e.get("quotes")
    if not isinstance(q, list) or not all(isinstance(x, dict) and isinstance(x.get("chunk"), str)
                                          and isinstance(x.get("text"), str) and x["text"].strip() for x in q):
        bad.append("quotes is not a list of {chunk, text}")
    elif not QUOTES_MIN <= len(q) <= QUOTES_MAX:
        bad.append(f"{len(q)} quotes (1 to 4)")
    else:
        for x in q:
            if len(x["text"]) > QUOTE_MAX:
                bad.append(f"a quote of {x['chunk']} is longer than {QUOTE_MAX} characters ({len(x['text'])})")
    if not bad and e["kind"] == "cross" and len(src) < 2:
        bad.append("a cross entry has one source (needs two or three)")
    return bad


def gate_entry(e, window, target):
    """window: {chunk id: chunk dict} of the window; target: the target chunk id. Returns the list of reasons the entry
    fails the gate (empty: it passes)."""
    bad = _schema_entry(e)
    if bad:
        return bad
    src = e["sources"]
    for s in src:
        if s not in window:
            bad.append(f"source {s} is not in the window")
    if target not in src:
        bad.append("the target chunk is not a source")
    views = {cid: norm_ws(chunk_view(c)) for cid, c in window.items()}
    quoted = set()
    for x in e["quotes"]:
        cid = x["chunk"]
        if cid not in window:
            bad.append(f"quote of {cid}: not a chunk of the window")
            continue
        if cid not in src:
            bad.append(f"quote of {cid}: not one of the sources")
        if norm_ws(x["text"]) not in views[cid]:
            bad.append(f"quote of {cid} is not verbatim: {norm_ws(x['text'])[:60]!r}")
            continue
        quoted.add(cid)
    for s in src:
        if s in window and not any(x["chunk"] == s for x in e["quotes"]):
            bad.append(f"source {s} has no quote")
    if target not in {x["chunk"] for x in e["quotes"]}:
        bad.append("no quote from the target chunk")
    for field, t in _support(e, window)[0]:
        bad.append(f"{field}: {t!r} is in no quote or title line")
    return bad


def _title_line(c):
    return chunk_view(c).split("\n", 1)[0]


def _title_norm(s):
    """A title line for the name comparison: backticks removed, '.', '/' and ' > ' as separators (one space)."""
    return norm_ws(re.sub(r"\s>\s|[./]", " ", (s or "").replace("`", "")))


_WORD_NAME = re.compile(r"^[A-Za-z]+(-[A-Za-z]+)+$")


def _in_title(token, line):
    if not re.search(r"[A-Za-z]", token):          # numbers and versions are not matched in a title line
        return False
    t, l = _title_norm(token), _title_norm(line)
    return bool(t) and re.search(r"(?<![\w\-])" + re.escape(t) + r"(?![\w\-])", l, re.I if _WORD_NAME.match(token) else 0) is not None


def _support(e, window):
    """Returns (unsupported [(field, token)], title chunk ids): the tokens that no quote contains and no source chunk's title
    line contains, and the ids of the source chunks whose title line is what supports a token (in source order)."""
    qtext = " \n ".join(norm_ws(x["text"]) for x in e["quotes"])
    bad, titles = [], []
    for field in ("title", "statement", "applies_when"):     # the title goes into the pack too (review of f45dd13)
        for t in identifier_tokens(e.get(field) or ""):
            if token_in(t, qtext):
                continue
            cid = next((s for s in e["sources"] if s in window and _in_title(t, _title_line(window[s]))), None)
            if cid is None:
                bad.append((field, t))
            elif cid not in titles:
                titles.append(cid)
    return bad, [s for s in e["sources"] if s in titles]


def title_support(e, window):
    """The ids of the source chunks whose title line supports a name of the entry (no quote has it): finish_entry records
    each such line as an extra quote."""
    return _support(e, window)[1]


def token_in(token, text):
    """The token occurs in the text as a whole token: not part of a longer name or number (`spec.kafka.replica` is not in
    `spec.kafka.replicas`, 3 is not in 30 or 1.3.1); a sentence's final dot after it is allowed. A path slash before it is
    fine (kbe/2: `kafka-features.sh` in bin/kafka-features.sh); a name of letters and hyphens only is compared without case."""
    flags = re.I if _WORD_NAME.match(token) else 0
    return re.search(r"(?<![\w.\-])" + re.escape(token) + r"(?![\w/\-])(?!\.\w)", text, flags) is not None


def check_reply(obj, window_chunks, target):
    """obj: the parsed reply. Returns (schema_error or None, passed entries as written, rejects [(entry, reasons)]).
    A schema error (not an object with a list "entries" of at most 8) makes the reply invalid (retried, then failed)."""
    if not isinstance(obj, dict) or not isinstance(obj.get("entries"), list):
        return "the reply is not an object with a list \"entries\"", [], []
    if len(obj["entries"]) > ENTRIES_MAX:
        return f"{len(obj['entries'])} entries (at most {ENTRIES_MAX})", [], []
    window = {c["id"]: c for c in window_chunks}
    ok, rej = [], []
    seen = set()
    for e in obj["entries"]:
        reasons = gate_entry(e, window, target)
        if not reasons:
            key = (tuple(sorted(e["sources"])), norm_statement(e["statement"]))
            if key in seen:
                reasons = ["an exact duplicate of an earlier entry of this window"]
            seen.add(key)
        (rej.append((e, reasons)) if reasons else ok.append(e))
    return None, ok, rej


def finish_entry(e, window_chunks, target, *, prompt_version, writer, created, commits):
    """The entry with the code's fields: id, scope (from the target's source), each source pinned with its source,
    path and commit (commits: INDEX_FROZEN.json source_commits), and the provenance."""
    byid = {c["id"]: c for c in window_chunks}
    out = {"id": entry_id(prompt_version, e["sources"], e["statement"]), "kind": e["kind"], "title": e["title"].strip(),
           "statement": e["statement"].strip()}
    if (e.get("applies_when") or "").strip():
        out["applies_when"] = e["applies_when"].strip()
    out["scope"] = dict(SCOPE[byid[target]["source"]])
    out["sources"] = [{"id": s, "source": byid[s]["source"], "path": byid[s]["path"], "commit": commits.get(byid[s]["source"])}
                      for s in e["sources"]]
    out["quotes"] = [{"chunk": x["chunk"], "text": x["text"]} for x in e["quotes"]]
    for cid in title_support(e, byid):         # a name only the chunk's title line has: that line is shown as a quote
        out["quotes"].append({"chunk": cid, "text": _title_line(byid[cid]), "title_line": True})
    out.update({"prompt_version": prompt_version, "writer": writer,
                "window": {"target": target, "chunks": [c["id"] for c in window_chunks]}, "created": created})
    return out


def render(entry):
    """The pack item of an entry (pack_text input: source, title, path, text), filled from prompts.KB_ENTRY_RENDER."""
    R = prompts.KB_ENTRY_RENDER
    paths = []
    for s in entry["sources"]:
        if s["path"] not in paths:
            paths.append(s["path"])
    fill = {"TITLE": entry["title"], "PRODUCT": entry["scope"]["product"], "VERSION": entry["scope"]["version"],
            "PATHS": ", ".join(paths), "STATEMENT": entry["statement"],
            "APPLIES": (re.sub(r"<<APPLIES_WHEN>>", lambda m: entry["applies_when"], R["applies"])
                        if entry.get("applies_when") else ""),
            "QUOTES": "\n".join(re.sub(r"<<QUOTE>>", lambda m, t=norm_ws(q["text"]): t, R["quote"]) for q in entry["quotes"])}
    sub = lambda tpl: re.sub(r"<<([A-Z_]+)>>", lambda m: fill[m.group(1)], tpl)  # noqa: E731
    return {"source": entry["sources"][0]["source"], "title": sub(R["title"]), "path": paths[0], "text": sub(R["text"]),
            "entry_id": entry["id"]}


def render_text(entry):
    return prompts.pack_text([render(entry)])


def render_check(entry):
    """The rendered-size part of the gate (needs the code's fields): reasons, empty when within RENDER_MAX."""
    n = len(render_text(entry))
    return [] if n <= RENDER_MAX else [f"the rendered entry has {n} characters (at most {RENDER_MAX})"]


def dumps(row):
    return json.dumps(row, ensure_ascii=False, sort_keys=True)
