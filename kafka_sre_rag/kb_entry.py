"""M7 KB phase 2 (EXPERIMENTAL, unmeasured): the fact entries' schema, rendering and mechanical gate.

Spec: runs/m7/reports/kb2_design.md sections 1 to 3 (and 5 for the rendered form). Stdlib only; no model, no server.

  window_of(slice_chunks, i)     the window of the i-th slice chunk: it, plus the previous and next chunk of the same file
  window_text(window, target)    the window as the writer and the checker see it (a header line per chunk, then the chunk
                                 as the model receives it: pack_text's title line, a newline, the text)
  check_reply(obj, ...)          the reply's schema (retried when broken) and the gate of each entry
  gate_entry(e, window, target)  the reasons an entry fails the mechanical gate ([] = passes)
  finish_entry(...)              the code's fields: id, scope, pinned sources, provenance
  context_notes(...)             kbe/5: the lead-in sentence and nearby admonitions, derived from the chunks (not by a model)
  heading_trail(...)             kbe/4: the source headings an entry stands under, derived from the chunks (not by a model)
  render(entry)                  the item an entry is in a pack (pack_text input), at most RENDER_MAX characters

The gate (section 3): schema and size limits; every quote a substring of its chunk after whitespace normalisation; quote
ids within the window and within the sources; every source quoted; the target chunk quoted; every number and every
identifier-like token of the title, the statement and applies_when occurs in a quote (kbe/2: or, for a name with a letter,
in the first title line of a source chunk; kbe/3: or in a heading line inside a source chunk, a number together with its
neighbouring word; backticks and emphasis underscores are ignored; finish_entry then adds that line as a quote); the rendered form within its cap.
"""
import hashlib
import json
import re

from . import prompts

KINDS = ("fact", "value", "procedure", "warning", "cross")
TITLE_MAX, STATEMENT_MAX, APPLIES_MAX, QUOTE_MAX = 100, 400, 200, 300
HEADING_MAX = 120                    # a heading line inside a chunk that counts as support (kbe/3)
QUOTES_MIN, QUOTES_MAX, SOURCES_MIN, SOURCES_MAX = 1, 4, 1, 3
ENTRIES_MAX = 12                     # per window (kbe/4: 12; kbe/3 and before: 8)
TRAIL_MAX = 200                      # the heading trail of an entry, in characters (kbe/4)
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


# ---------------------------------------------------------------- the heading trail (kbe/4, mechanical, not written by a model)
_HEAD_MARK = re.compile(r"^\s*(#{1,6}|={1,6})\s+(\S.*?)\s*$")
_CAPTION = re.compile(r"^\.(?=[A-Za-z0-9`*_])(\S.*?)\s*$")
_TABLE_FENCE = re.compile(r"^\s*\|===+\s*$")
_ANCHOR = re.compile(r"\s*\{#[^}]*\}\s*$")
TRAIL_SEP = " > "
ELLIPSIS = "\u2026"


def _norm_map(text):
    """(normalised text, index map): the text with white space collapsed (as norm_ws does) and, per character of it, its
    index in the original; leading white space is dropped."""
    out, idx, prev_space = [], [], True
    for i, ch in enumerate(text or ""):
        if ch.isspace():
            if not prev_space:
                out.append(" ")
                idx.append(i)
            prev_space = True
        else:
            out.append(ch)
            idx.append(i)
            prev_space = False
    return "".join(out), idx


def trail_before(text, pos):
    """The in-chunk headings (a stack by level: markdown '#', AsciiDoc '=', not inside a code block) in force at character
    pos of text, outermost first, and the caption (an AsciiDoc '.Caption' line) of the table the position is in, or None."""
    stack, fence, in_table, caption, pending, off = [], False, False, None, None, 0
    for line in (text or "").split("\n"):
        if off > pos:
            break
        end = off + len(line)
        if _FENCE.match(line):
            fence = not fence
        elif not fence:
            if _TABLE_FENCE.match(line):
                if in_table:
                    in_table, caption = False, None
                else:
                    in_table, caption, pending = True, pending, None
            elif not in_table:
                m = _HEAD_MARK.match(line)
                if m:
                    lvl = len(m.group(1))
                    name = _ANCHOR.sub("", m.group(2)).strip()          # a trailing {#anchor} is no part of the name
                    stack = [(l, t) for l, t in stack if l < lvl] + [(lvl, name)]
                    pending = None
                else:
                    c = _CAPTION.match(line)
                    if c:
                        pending = c.group(1)
                    elif line.strip() and not line.lstrip().startswith("["):
                        pending = None
        off = end + 1
    return [t for _, t in stack], (caption if in_table else None)


def _mid(s, n):
    """s cut to at most n characters by removing the middle (an ellipsis in its place)."""
    if len(s) <= n:
        return s
    if n <= 1:
        return ELLIPSIS[:n]
    a = n // 2
    return s[:a] + ELLIPSIS + s[len(s) - (n - 1 - a):]


def _fit(crumb, rest, n):
    """One trail part within n characters: the crumb (the chunk's title, segments split by ' > ') then the rest (the in-chunk
    headings and the table caption, nearest last). The middle of the crumb gives way first (middle segments become an
    ellipsis, then characters), then the outermost headings; the nearest heading stays (cut in the middle only when it alone
    is too long)."""
    segs = [x for x in crumb.split(TRAIL_SEP) if x] if crumb else []
    rest = list(rest)
    join = lambda a, b: TRAIL_SEP.join(a + b)  # noqa: E731
    if len(join(segs, rest)) <= n:
        return join(segs, rest)
    for k in range(len(segs) - 2, 0, -1):                       # first segment, an ellipsis, the last k segments
        cand = segs[:1] + [ELLIPSIS] + segs[len(segs) - k:]
        if len(join(cand, rest)) <= n:
            return join(cand, rest)
    tail = TRAIL_SEP.join(rest)
    room = n - len(tail) - (len(TRAIL_SEP) if tail else 0)
    if segs and room >= 12:                                      # the crumb is cut in the middle
        head = _mid(TRAIL_SEP.join(segs), room)
        return TRAIL_SEP.join([head] + rest)
    while len(rest) > 1 and len(TRAIL_SEP.join(rest)) > n:      # the crumb is gone: drop the outermost headings
        rest = rest[1:]
    return _mid(TRAIL_SEP.join(rest), n) if rest else _mid(TRAIL_SEP.join(segs), n)


def heading_trail(sources, quotes, byid, cap=TRAIL_MAX):
    """kbe/4: the heading trail of an entry, derived from its source chunks and its quotes (not by a model): per source chunk,
    the chunk's title line without the source prefix (a breadcrumb such as 'Doc > Section'), the in-chunk headings in force
    where each quote of that chunk stands, and the caption of the table the quote is a row of; parts of several chunks (or of
    quotes under different headings) joined by ' | '; at most cap characters. Quotes that are a title or heading line are
    not positions."""
    parts = []
    for cid in sources:
        c = byid.get(cid)
        if c is None:
            continue
        crumb = _ANCHOR.sub("", (c.get("title") or str(c.get("path", "")).rsplit("/", 1)[-1] or "").strip())
        ntext, idx = _norm_map(c.get("text") or "")
        variants = []
        for x in quotes:
            if x["chunk"] != cid or x.get("title_line") or x.get("heading_line"):
                continue
            q = norm_ws(x["text"])
            at = ntext.find(q) if q else -1
            if at < 0:
                continue
            heads, cap_ = trail_before(c.get("text") or "", idx[at])
            rest = heads + ([cap_] if cap_ else [])
            if rest not in variants:
                variants.append(rest)
        for rest in (variants or [[]]):
            parts.append((crumb, rest))
    uniq = []
    for p in parts:
        if p not in uniq:
            uniq.append(p)
    if not uniq:
        return ""
    sep = " | "
    n = max(20, (cap - len(sep) * (len(uniq) - 1)) // len(uniq))
    out = sep.join(_fit(cr, rest, n) for cr, rest in uniq)
    return out if len(out) <= cap else _mid(out, cap)


# ---------------------------------------------------------------- the lead-in and nearby admonitions (kbe/5, mechanical)
CONTEXT_MAX = 400                    # trail + lead-in + admonition, in characters (about; kbe/5)
LEAD_MIN = 24                        # the least a part keeps when the room is short
_LIST_LINE = re.compile(r"^\s*(?:\*{1,5}|[\-+]|\d+[.)]|[a-z][.)]|\.{1,5})\s+\S")
_MARKER_ONLY = re.compile(r"^\s*(?:\*{1,5}|-|\d+[.)]|[a-z][.)]|\.{1,5})\s*$")
_ATTR_LINE = re.compile(r"^\s*\[([^\]\n]*)\]\s*$")
_ADM_NAMES = ("NOTE", "WARNING", "IMPORTANT", "CAUTION")
_ADM_ATTR = re.compile(r"^\s*(" + "|".join(_ADM_NAMES) + r")\b")
_ADM_PARA = re.compile(r"^\s*(?:(NOTE|WARNING|IMPORTANT|CAUTION)\s*:\s*"                       # AsciiDoc  NOTE: text
                       r"|>\s*\[!(NOTE|WARNING|IMPORTANT|CAUTION)\]\s*"                         # GitHub    > [!NOTE]
                       r"|>?\s*\*{1,2}(Note|Warning|Important|Caution)\s*[:.]?\*{1,2}\s*[:.]?\s*)")   # **Note:** text, > **Note**
_DELIM = re.compile(r"^={4,}\s*$")
_HTML_TABLE = re.compile(r"^\s*<table\b", re.I)
SHORT_SENTENCE = 80                  # a lead-in shorter than this takes the sentence before it as well
_ABBREV_END = re.compile(r"(?:^|\s)(?:" + "|".join(re.escape(a) for a in sorted(ABBREVIATIONS)) + r")\.$", re.I)


def _blocks(text):
    """The chunk text as blocks (kind, start, end, body): kind is heading, code, table, list, admonition (body = its name and
    text) or para; offsets are into text. Captions and attribute lines belong to no block."""
    lines = (text or "").split("\n")
    offs, o = [], 0
    for ln in lines:
        offs.append(o)
        o += len(ln) + 1
    n, i, out, adm = len(lines), 0, [], None

    def end_of(j):
        return offs[j] + len(lines[j])

    def stop(ln):
        return (not ln.strip() or _HEAD_MARK.match(ln) or _FENCE.match(ln) or _TABLE_FENCE.match(ln) or _DELIM.match(ln)
                or _HTML_TABLE.match(ln) or ln.lstrip().startswith("|"))

    while i < n:
        ln = lines[i]
        if not ln.strip():
            i += 1
            continue
        if _FENCE.match(ln):                                      # a code block, to the closing fence
            j = i + 1
            while j < n and not (lines[j].strip() == ln.strip()):
                j += 1
            out.append(("code", offs[i], end_of(min(j, n - 1)), ""))
            i, adm = j + 1, None
            continue
        if _HTML_TABLE.match(ln):                                 # an HTML table, to the line with </table>
            j = i
            while j < n and "</table>" not in lines[j].lower():
                j += 1
            out.append(("table", offs[i], end_of(min(j, n - 1)), ""))
            i, adm = j + 1, None
            continue
        if ln.lstrip().startswith("|") and not _TABLE_FENCE.match(ln):     # a markdown table
            j = i + 1
            while j < n and lines[j].lstrip().startswith("|") and not _TABLE_FENCE.match(lines[j]):
                j += 1
            out.append(("table", offs[i], end_of(j - 1), ""))
            i, adm = j, None
            continue
        if _TABLE_FENCE.match(ln):
            j = i + 1
            while j < n and not _TABLE_FENCE.match(lines[j]):
                j += 1
            out.append(("table", offs[i], end_of(min(j, n - 1)), ""))
            i, adm = j + 1, None
            continue
        if _DELIM.match(ln):                                      # ==== : an admonition block when [NOTE] came before, else ignored
            if adm:
                j = i + 1
                while j < n and not _DELIM.match(lines[j]):
                    j += 1
                body = " ".join(x.strip() for x in lines[i + 1:j] if x.strip())
                out.append(("admonition", offs[i], end_of(min(j, n - 1)), adm + "\0" + norm_ws(body)))
                i, adm = j + 1, None
            else:
                i += 1
            continue
        a = _ATTR_LINE.match(ln)
        if a:
            m = _ADM_ATTR.match(a.group(1))
            if m:
                adm = m.group(1)
            i += 1
            continue
        if _CAPTION.match(ln) and not _LIST_LINE.match(ln):
            i += 1
            continue
        if _HEAD_MARK.match(ln):
            out.append(("heading", offs[i], end_of(i), ""))
            i, adm = i + 1, None
            continue
        m = _ADM_PARA.match(ln)
        if m or adm:
            j = i + 1
            while j < n and not stop(lines[j]):
                j += 1
            name = (m.group(1) or m.group(2) or m.group(3) or "").upper() if m else adm
            first = ln[m.end():] if m else ln
            body = " ".join([first.strip()] + [re.sub(r"^\s*>\s?", "", x).strip() for x in lines[i + 1:j]])
            out.append(("admonition", offs[i], end_of(j - 1), name + "\0" + norm_ws(body)))
            i, adm = j, None
            continue
        if _LIST_LINE.match(ln):
            j = i + 1
            while j < n:
                if lines[j].strip():
                    if (_HEAD_MARK.match(lines[j]) or _FENCE.match(lines[j]) or _TABLE_FENCE.match(lines[j]) or _DELIM.match(lines[j])
                            or _HTML_TABLE.match(lines[j])):
                        break
                    if not (_LIST_LINE.match(lines[j]) or lines[j].startswith((" ", "\t")) or lines[j].strip() == "+"
                            or lines[j - 1].strip() == "+"):          # a flush-left line that continues nothing ends the list
                        break
                    j += 1
                else:                                             # a blank line ends the list unless more of it follows
                    k = j + 1
                    while k < n and not lines[k].strip():
                        k += 1
                    if k < n and (_LIST_LINE.match(lines[k]) or lines[k].startswith((" ", "\t", "+"))) and not _HEAD_MARK.match(lines[k]):
                        j = k
                    else:
                        break
            out.append(("list", offs[i], end_of(j - 1), ""))
            i, adm = j, None
            continue
        j = i + 1
        while j < n and not stop(lines[j]):
            j += 1
        out.append(("para", offs[i], end_of(j - 1), ""))
        i, adm = j, None
    return out


def last_sentence(text):
    """The last sentence of text (white space normalised); a sentence ends at . ! or ? and a space, never after e.g. / i.e. / etc.;
    a trailing ':' stays part of it. '' when there is none."""
    t = norm_ws(text)
    if not t:
        return ""
    pieces = re.split(r"(?<=[.!?])\s+", t)
    merged = []
    for p in pieces:
        if merged and _ABBREV_END.search(merged[-1]):
            merged[-1] += " " + p
        else:
            merged.append(p)
    return merged[-1]


def lead_sentences(text):
    """The sentence before the quote: the last sentence of text, and the one before it when the last is shorter than
    SHORT_SENTENCE characters ('Such a record is called a tombstone.' alone says nothing about what the record is)."""
    t = norm_ws(text)
    last = last_sentence(t)
    if last and len(last) < SHORT_SENTENCE and len(t) > len(last):
        prev = last_sentence(t[:len(t) - len(last)])
        if prev:
            return prev + " " + last
    return last


def _item_before(text, s):
    """The text of the list item the position s is in, from its marker to s, with its code blocks and '+' lines left out; '' when
    s is not in a list item (an item goes on over '+' lines, code blocks and indented lines, not over a blank line without '+')."""
    item, fence, blank, plus = None, False, False, False
    for ln in text[:s].split("\n")[:-1] + [text[text.rfind("\n", 0, s) + 1:s]]:
        if _FENCE.match(ln):
            fence = not fence
            if item is not None:
                plus = False
            continue
        if fence:
            continue
        if not ln.strip():
            blank = True
            continue
        if _LIST_LINE.match(ln) or _MARKER_ONLY.match(ln):
            item, blank, plus = [ln], False, False
        elif ln.strip() == "+":
            plus, blank = True, False
        elif (item is not None and not _HEAD_MARK.match(ln)
              and (plus or ln.startswith((" ", "\t")) or (not blank and ln[:1].islower() and not item[-1].rstrip().endswith((".", "!", "?", ":"))))):
            item.append(ln)
            blank, plus = False, False
        else:
            item, blank, plus = None, False, False
    if not item or fence or (blank and not plus):             # a blank line (without '+') between the item and the position
        return ""
    if s == 0 or text[s - 1] == "\n":                          # the position starts a line: does that line continue the item?
        end = text.find("\n", s)
        ln = text[s:end if end >= 0 else len(text)]
        if not (plus or ln.startswith((" ", "\t")) or (ln[:1].islower() and not item[-1].rstrip().endswith((".", "!", "?", ":")))):
            return ""
    return _strip_list_marker(" ".join(item))


def _strip_list_marker(s):
    return re.sub(r"^\s*(?:\*{1,5}|[\-+]|\d+[.)]|[a-z][.)]|\.{1,5})\s+", "", s)


def _adm_text(body):
    name, _, txt = body.partition("\0")
    return (name + ": " + txt).strip() if txt else name


def lead_parts(chunk_text, qtext):
    """(lead_in, [admonition, ...]) of the quote in the chunk text: lead_in is the sentence just before the quote in its paragraph
    (or list item) or, when the quote starts a paragraph, list or table, the last sentence of the paragraph above it (not when
    that is a heading, a code block, a table or an admonition); the admonitions are the NOTE / WARNING / IMPORTANT / CAUTION
    blocks directly above and directly below the quote's block (one lead-in paragraph may stand between). All '' / []
    when the quote is not found or is a title or heading line."""
    text = chunk_text or ""
    ntext, idx = _norm_map(text)
    q = norm_ws(qtext)
    at = ntext.find(q) if q else -1
    if at < 0:
        return "", []
    s = idx[at]
    blocks = _blocks(text)
    k = next((b for b, x in enumerate(blocks) if x[1] <= s <= x[2]), None)
    if k is None or blocks[k][0] in ("heading", "admonition", "code"):
        return "", []
    kind, start = blocks[k][0], blocks[k][1]
    lead, lead_block = "", None
    before = ""
    item = _item_before(text, s)                              # the list item the quote is in (or continues), up to the quote
    if kind == "para" and not item:
        before = text[start:s]
    elif kind in ("para", "list"):
        before = item
    if norm_ws(before):
        lead = lead_sentences(before)
    elif k > 0 and blocks[k - 1][0] == "para":
        prev = blocks[k - 1]
        lead, lead_block = lead_sentences(text[prev[1]:prev[2]]), k - 1
    elif k > 1 and kind == "para" and blocks[k - 1][0] == "list" and blocks[k - 2][0] == "para":
        prev = blocks[k - 2]                                  # a paragraph right after a list: the lead-in of that list
        lead, lead_block = lead_sentences(text[prev[1]:prev[2]]), k - 2
    adms = []
    if k + 1 < len(blocks) and blocks[k + 1][0] == "admonition":
        adms.append(_adm_text(blocks[k + 1][3]))
    up = (lead_block if lead_block is not None else k) - 1
    if up >= 0 and blocks[up][0] == "admonition":
        adms.insert(0, _adm_text(blocks[up][3]))
    return lead, adms


def _cut(s, n, keep):
    """s cut to at most n characters with an ellipsis; keep 'end' keeps the end (the nearest text), 'start' the start."""
    if len(s) <= n:
        return s
    if n <= 1:
        return ELLIPSIS[:n]
    return ELLIPSIS + s[len(s) - (n - 1):] if keep == "end" else s[:n - 1].rstrip() + ELLIPSIS


def _shares(lens, room):
    """Water-filling: each part gets what it needs when the room allows, else an equal share of what is left (at least LEAD_MIN)."""
    out, left, order = [0] * len(lens), room, sorted(range(len(lens)), key=lambda i: lens[i])
    for r, i in enumerate(order):
        fair = max(LEAD_MIN, left // (len(lens) - r))
        out[i] = min(lens[i], fair)
        left -= out[i]
    return out


def context_notes(sources, quotes, byid, trail, cap=CONTEXT_MAX):
    """kbe/5: (lead_in, admonition) of an entry, derived from its source chunks and its quotes (not by a model). lead_in: per
    quote, the sentence before it (see lead_parts), skipped when it already stands inside a quote of the entry; admonition: the
    NOTE / WARNING / IMPORTANT / CAUTION blocks next to a quote, 'NAME: text'. Several are joined by ' | '. Trail, lead_in and
    admonition together stay within cap characters (the trail keeps its own text; the nearest text of the others is kept, a
    cut is marked with an ellipsis)."""
    qnorm = [norm_ws(x["text"]) for x in quotes]
    leads, adms = [], []
    for cid in sources:
        c = byid.get(cid)
        if c is None:
            continue
        for x in quotes:
            if x["chunk"] != cid or x.get("title_line") or x.get("heading_line"):
                continue
            lead, ad = lead_parts(c.get("text") or "", x["text"])
            if lead and not any(lead in q for q in qnorm) and lead not in leads:
                leads.append(lead)
            adms.extend(a for a in ad if a not in adms and not any(a.partition(": ")[2] in q for q in qnorm))
    parts = [("l", t) for t in leads] + [("a", t) for t in adms]
    if not parts:
        return "", ""
    room = max(0, cap - len(trail or "")) - 3 * (len(parts) - 1)
    room = max(room, LEAD_MIN * len(parts))
    shares = _shares([len(t) for _, t in parts], room)
    cut = [_cut(t, n, "end" if k == "l" else "start") for (k, t), n in zip(parts, shares)]
    return (" | ".join(t for (k, _), t in zip(parts, cut) if k == "l"),
            " | ".join(t for (k, _), t in zip(parts, cut) if k == "a"))


def _title_line(c):
    return chunk_view(c).split("\n", 1)[0]


_HEADING = re.compile(r"^\s*(?:#{1,6}|={1,6})\s+\S")
_FENCE = re.compile(r"^\s*(?:```|~~~|----+\s*$|\.\.\.\.+\s*$)")


def heading_lines(c):
    """The heading lines inside a chunk's text (markdown '#', AsciiDoc '='), not those in a code block, each at most
    HEADING_MAX characters (kbe/3: a heading names the subject of what follows and counts as support like the title line)."""
    out, fence = [], False
    for line in (c.get("text") or "").split("\n"):
        if _FENCE.match(line):
            fence = not fence
            continue
        if not fence and _HEADING.match(line) and len(line.strip()) <= HEADING_MAX:
            out.append(line.strip())
    return out


def unmark(s):
    """kbe/3, verbatim-safe: backticks and AsciiDoc emphasis underscores (an underscore that is not between two letters or
    digits, as in _&lt;cluster_name&gt;_-cluster-ca-cert) removed, for comparing a name with a text; inner underscores stay."""
    return re.sub(r"(?<![A-Za-z0-9])_|_(?![A-Za-z0-9])", "", (s or "").replace("`", ""))


def _title_norm(s):
    """A title line for the name comparison: backticks and emphasis underscores removed, '.', '/' and ' > ' as separators (one space)."""
    return norm_ws(re.sub(r"\s>\s|[./]", " ", unmark(s)))


_WORD_NAME = re.compile(r"^[A-Za-z]+(-[A-Za-z]+)+$")


def _in_title(token, line):
    if not re.search(r"[A-Za-z]", token):          # a bare number is matched with its neighbouring word (_number_in_title)
        return False
    t, l = _title_norm(token), _title_norm(line)
    return bool(t) and re.search(r"(?<![\w\-])" + re.escape(t) + r"(?![\w\-])", l, re.I if _WORD_NAME.match(token) else 0) is not None


def _number_in_title(token, field_text, line):
    """kbe/3: a number (no letter) is supported by a title or heading line when the line has it together with a word that
    stands next to it in the field's text ("Step 1", "MirrorMaker 2")."""
    l = _title_norm(line)
    ft = norm_ws(unmark(field_text))
    for m in re.finditer(r"(?<![\w.\-])" + re.escape(token) + r"(?![\w\-])(?!\.\w)", ft):
        before = re.search(r"([A-Za-z][\w\-]*)\s$", ft[:m.start()])
        after = re.match(r"\s([A-Za-z][\w\-]*)", ft[m.end():])
        for phrase in ([f"{before.group(1)} {token}"] if before else []) + ([f"{token} {after.group(1)}"] if after else []):
            if re.search(r"(?<![\w\-])" + re.escape(norm_ws(phrase)) + r"(?![\w\-])", l):
                return True
    return False


def _line_supports(token, field_text, line):
    return _number_in_title(token, field_text, line) if not re.search(r"[A-Za-z]", token) else _in_title(token, line)


def _support(e, window):
    """Returns (unsupported [(field, token)], lines [(chunk id, line, is_title)]): the tokens that no quote contains and no
    source chunk's title line or heading line contains, and the lines that support a token (in source order, title line first;
    kbe/3: heading lines inside the chunk count like the title line)."""
    qtext = " \n ".join(norm_ws(x["text"]) for x in e["quotes"])
    qun = unmark(qtext)
    bad, found = [], set()
    for field in ("title", "statement", "applies_when"):     # the title goes into the pack too (review of f45dd13)
        ftext = e.get(field) or ""
        for t in identifier_tokens(ftext):
            if token_in(t, qtext) or (unmark(t) and token_in(unmark(t), qun)):
                continue
            hit = None
            for s in e["sources"]:
                if s not in window:
                    continue
                cands = [(_title_line(window[s]), True)] + [(h, False) for h in heading_lines(window[s])]
                hit = next(((s, ln, ti) for ln, ti in cands if _line_supports(t, ftext, ln)), None)
                if hit:
                    break
            if hit is None:
                bad.append((field, t))
            else:
                found.add(hit)
    order = {s: i for i, s in enumerate(e["sources"])}
    return bad, sorted(found, key=lambda h: (order[h[0]], not h[2], h[1]))


def title_support(e, window):
    """The ids of the source chunks whose title line supports a name of the entry (no quote has it)."""
    out = []
    for cid, _, ti in _support(e, window)[1]:
        if ti and cid not in out:
            out.append(cid)
    return out


def line_support(e, window):
    """[(chunk id, line, is_title)]: the title lines and heading lines that support a name of the entry; finish_entry records
    each as an extra quote."""
    return _support(e, window)[1]


def token_in(token, text):
    """The token occurs in the text as a whole token: not part of a longer name or number (`spec.kafka.replica` is not in
    `spec.kafka.replicas`, 3 is not in 30 or 1.3.1); a sentence's final dot after it is allowed. A path slash before it is
    fine (kbe/2: `kafka-features.sh` in bin/kafka-features.sh); a name of letters and hyphens only is compared without case."""
    flags = re.I if _WORD_NAME.match(token) else 0
    return re.search(r"(?<![\w.\-])" + re.escape(token) + r"(?![\w/\-])(?!\.\w)", text, flags) is not None


def check_reply(obj, window_chunks, target):
    """obj: the parsed reply. Returns (schema_error or None, passed entries as written, rejects [(entry, reasons)]).
    A schema error (not an object with a list "entries" of at most ENTRIES_MAX = 12) makes the reply invalid (retried, then failed)."""
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
    trail = heading_trail(e["sources"], out["quotes"], byid)     # kbe/4: from the chunks, before the support lines are added
    if trail:
        out["heading_trail"] = trail
    lead, adm = context_notes(e["sources"], out["quotes"], byid, trail)     # kbe/5: the sentence before the quote, a nearby note
    if lead:
        out["lead_in"] = lead
    if adm:
        out["admonition"] = adm
    for cid, line, is_title in line_support(e, byid):     # a name only a title line or a heading line has: that line is shown as a quote
        out["quotes"].append({"chunk": cid, "text": line, ("title_line" if is_title else "heading_line"): True})
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
            "CONTEXT": (re.sub(r"<<TRAIL>>", lambda m: entry["heading_trail"], R["context"]) if entry.get("heading_trail") else ""),
            "BEFORE": (re.sub(r"<<LEAD_IN>>", lambda m: entry["lead_in"], R["before"]) if entry.get("lead_in") else ""),
            "NOTE": (re.sub(r"<<ADMONITION>>", lambda m: entry["admonition"], R["note"]) if entry.get("admonition") else ""),
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
