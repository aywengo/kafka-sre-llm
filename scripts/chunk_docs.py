#!/usr/bin/env python3
"""Convert Markdown / AsciiDoc / reStructuredText / HTML docs into clean, heading-aware chunks.

Output: one JSON object per line with id, source, license, path, title, text.
"""
import argparse
import fnmatch
import hashlib
import json
import re
from pathlib import Path

import yaml

try:
    from bs4 import BeautifulSoup
except ImportError:  # HTML sources need beautifulsoup4
    BeautifulSoup = None

MIN_CHARS = 400
MAX_CHARS = 4000

ADOC_ATTR_DEF = re.compile(r"^:([\w-]+):\s*(.*)$")
ADOC_ATTR_REF = re.compile(r"\{([\w-]+)\}")                        # names may contain hyphens
ADOC_PLACEHOLDER = re.compile(r"(?<![$\w])\{([A-Za-z][\w-]*)\}")
ADOC_COND = re.compile(r"^if(n?)def::([\w-]+)\[\]$")
RST_ADORN = re.compile(r"^([=\-~^\"'`#*+])\1{2,}$")


def parse_adoc_text(text: str, known=()) -> dict:
    """`:name: value` definitions. Definitions inside ifdef::x[] / ifndef::x[] blocks count only
    when the condition holds for the attributes defined so far (`known` plus this text)."""
    found, skip = {}, []
    for line in text.splitlines():
        s = line.strip()
        if m := ADOC_COND.match(s):
            skip.append((m.group(2) in found or m.group(2) in known) == bool(m.group(1)))
        elif s.startswith("endif::") and skip:
            skip.pop()
        elif not any(skip) and (m := ADOC_ATTR_DEF.match(s)):
            found[m.group(1)] = m.group(2)
    return found


def parse_adoc_attributes(path: Path, attrs=None) -> dict:
    """Read `:name: value` definitions from an AsciiDoc file, following include:: lines."""
    attrs = {} if attrs is None else attrs
    attrs.update(parse_adoc_text(path.read_text(errors="ignore"), attrs))
    for line in path.read_text(errors="ignore").splitlines():
        m = re.match(r"^include::([^\[]+)\[", line.strip())
        if m and (path.parent / m.group(1)).is_file():
            parse_adoc_attributes(path.parent / m.group(1), attrs)
    return attrs


def resolve_attributes(attrs: dict) -> dict:
    """Expand attributes that refer to other attributes."""
    sub = lambda v: ADOC_ATTR_REF.sub(lambda m: attrs.get(m.group(1), m.group(0)), v)
    for _ in range(5):
        attrs = {k: sub(v) for k, v in attrs.items()}
    return attrs


# ---------- format cleaners ---------------------------------------------------

def clean_markdown(text: str) -> str:
    text = re.sub(r"^---\n.*?\n---\n", "", text, flags=re.S)            # front matter
    text = re.sub(r'{{<\s*glossary_tooltip[^>]*?text="([^"]+)"[^>]*>}}', r"\1", text)
    text = re.sub(r"{{[<%].*?[%>]}}", "", text, flags=re.S)                # other Hugo shortcodes
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)                       # images
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)                   # links -> text
    return text


def clean_adoc(text: str, attrs: dict, keep=()) -> str:
    attrs = resolve_attributes({**attrs, **parse_adoc_text(text, attrs), **{k: "{%s}" % k for k in keep}})
    out, in_listing, in_comment = [], False, False
    for line in text.splitlines():
        s = line.strip()
        if s == "////":
            in_comment = not in_comment
            continue
        if in_comment:
            continue
        if s in ("----", "...."):
            in_listing = not in_listing
            out.append("```")
            continue
        if not in_listing:
            if s.startswith(("//", "include::", "ifdef::", "ifndef::", "endif::", "image::")):
                continue
            if re.match(r"^:[\w-]+:", s):                                  # attribute definitions
                continue
            if re.match(r"^\[(id|role|source|discrete|options|cols|%|NOTE|IMPORTANT|WARNING|TIP|\.)", s):
                continue
            m = re.match(r"^(=+)\s+(.*)", line)
            if m:
                line = "#" * len(m.group(1)) + " " + m.group(2)
            line = re.sub(r"xref:[^\[]+\[([^\]]*)\]", r"\1", line)
            line = re.sub(r"link:[^\[]+\[([^\]]*)\]", r"\1", line)
        line = ADOC_ATTR_REF.sub(lambda m: attrs.get(m.group(1), m.group(0)), line)
        if not in_listing:                                                 # an attribute value can be an xref
            line = re.sub(r"xref:([^\[]+)\[([^\]]*)\]",
                          lambda m: m.group(2) or m.group(1).rsplit("_", 1)[0].replace("-", " "), line)
        out.append(line)
    return "\n".join(out)


def clean_html(text: str) -> str:
    if BeautifulSoup is None:
        raise SystemExit("pip install beautifulsoup4 to process HTML sources")
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    soup = BeautifulSoup(text, "html.parser")
    for tag in soup(["script", "style", "nav"]):
        tag.decompose()
    for pre in soup.find_all("pre"):
        pre.replace_with("\n```\n" + pre.get_text() + "\n```\n")
    for level in range(1, 5):
        for h in soup.find_all(f"h{level}"):
            h.replace_with("\n" + "#" * level + " " + h.get_text(" ", strip=True) + "\n")
    for li in soup.find_all("li"):
        li.insert(0, "- ")
    txt = soup.get_text("\n")
    txt = re.sub(r"\{\{\s*\w+\s*\}\}", "<version>", txt)                  # Kafka doc placeholders
    return txt


def clean_rst(text: str) -> str:
    """reStructuredText -> markdown: underlined titles become # headings (level = order in which
    the underline characters first appear), `::` literal blocks become fenced code, directive
    and option lines are dropped (list-table rows stay as nested lists)."""
    text = re.sub(r"`([^`<]+?)\s*<[^>]+>`__?", r"\1", text)                 # `text <url>`_ -> text
    text = re.sub(r"(?<!`)`([^`\n]+)`_", r"\1", text)                       # `named reference`_
    text = re.sub(r"``([^`\n]+)``", r"`\1`", text)                          # ``literal`` -> `literal`
    lines, out, levels, fence = text.splitlines(), [], [], None
    i = 0
    while i < len(lines):
        line, nxt = lines[i], lines[i + 1] if i + 1 < len(lines) else ""
        i += 1
        indent = len(line) - len(line.lstrip())
        if fence is not None:
            if not line.strip() or indent > fence:
                out.append(line)
                continue
            out.append("```")                                              # literal block ends on a dedent
            fence = None
        s = line.strip()
        if s and not RST_ADORN.match(line) and RST_ADORN.match(nxt) and len(nxt) >= len(line.rstrip()):
            if nxt[0] not in levels:
                levels.append(nxt[0])
            out.append("#" * (levels.index(nxt[0]) + 1) + " " + s)
            i += 1
        elif re.match(r"\.\. (code|code-block|sourcecode)::", s) or (s.endswith("::") and not s.startswith(".. ")):
            if not s.startswith(".. ") and s != "::":
                out.append(line.rstrip()[:-1])                             # "text::" reads "text:"
            out.append("```")
            fence = indent
        elif RST_ADORN.match(line) or s.startswith(".. ") or re.match(r"^\s+:[\w-]+:( |$)", line) \
                or re.fullmatch(r"(\|[^|]+\|\s*)+", s):                      # overlines, directives, options, |substitutions|
            continue
        else:
            out.append(line)
    if fence is not None:
        out.append("```")
    return "\n".join(out)


CLEANERS = {".md": clean_markdown, ".html": clean_html, ".rst": clean_rst, ".txt": lambda t: t}


# ---------- chunking ----------------------------------------------------------

def split_sections(text: str):
    """Yield (title_path, body) split on markdown headings, outside code fences."""
    titles, buf, in_code = [], [], False

    def flush():
        body = "\n".join(buf).strip()
        if body:
            yield " > ".join(t for t in titles if t), body

    for line in text.splitlines():
        if line.strip().startswith("```"):
            in_code = not in_code
        m = None if in_code else re.match(r"^(#{1,4})\s+(.*)", line)
        if m:
            yield from flush()
            buf = []
            level = len(m.group(1))
            titles = titles[: level - 1] + [""] * max(0, level - 1 - len(titles)) + [m.group(2).strip()]
        else:
            buf.append(line)
    yield from flush()


def split_long(body: str, max_chars: int = MAX_CHARS):
    MAX_CHARS = max_chars
    if len(body) <= MAX_CHARS:
        return [body]
    # A single paragraph (table, list, generated metrics dump) can exceed
    # MAX_CHARS, so break oversized paragraphs at line/sentence boundaries first.
    paras = []
    for para in re.split(r"\n\s*\n", body):
        while len(para) > MAX_CHARS:
            cut = para.rfind("\n", 0, MAX_CHARS)
            if cut < MAX_CHARS // 2:
                cut = para.rfind(". ", 0, MAX_CHARS)
            if cut < MAX_CHARS // 2:
                cut = MAX_CHARS
            paras.append(para[:cut].strip())
            para = para[cut:].strip()
        if para:
            paras.append(para)
    parts, cur = [], ""
    for para in paras:
        if cur and len(cur) + len(para) + 2 > MAX_CHARS:
            parts.append(cur)
            cur = ""
        cur = (cur + "\n\n" + para).strip()
    if cur:
        parts.append(cur)
    return parts


def chunk_text(text: str, max_chars: int = MAX_CHARS):
    if max_chars != MAX_CHARS and len(text) <= max_chars:   # per-source limit: keep small docs whole
        m = re.search(r"^#{1,4}\s+(.*)", text, re.M)
        yield (m.group(1).strip() if m else ""), text.strip()
        return
    pending_title, pending = None, ""
    for title, body in split_sections(text):
        body = re.sub(r"\n{3,}", "\n\n", body)
        if pending and len(pending) < MIN_CHARS:          # merge tiny sections forward
            body = pending + "\n\n" + (f"## {title}\n" if title else "") + body
            title = pending_title or title
            pending = ""
        for part in split_long(body, max_chars):
            if len(part) < MIN_CHARS:
                pending_title, pending = title, part
                continue
            yield title, part
    if pending and len(pending) >= 150:
        yield pending_title, pending


# ---------- OpenAPI -----------------------------------------------------------

def _schema_text(schema: dict, components: dict, depth: int = 0) -> str:
    """One-line description of a schema; named schemas are expanded one level."""
    if not schema:
        return ""
    ref = schema.get("$ref")
    if ref:
        name = ref.rsplit("/", 1)[-1]
        target = components.get(name, {})
        if depth == 0 and target.get("properties"):
            props = ", ".join(f"{k}: {_schema_text(v, components, 1)}" for k, v in target["properties"].items())
            return f"{name} {{{props}}}"
        return name
    if schema.get("type") == "array":
        return f"array of {_schema_text(schema.get('items', {}), components, depth)}"
    if schema.get("type") == "object" and schema.get("properties"):
        return "{" + ", ".join(f"{k}: {_schema_text(v, components, 1)}" for k, v in schema["properties"].items()) + "}"
    if schema.get("additionalProperties"):
        return f"object with {_schema_text(schema['additionalProperties'], components, 1) or 'any'} values"
    return schema.get("type", "") + (f" (default {schema['default']})" if "default" in schema else "")


def chunk_openapi(text: str):
    """One chunk per path and method: method + path as the title, then summary, parameters,
    request body and responses."""
    spec = yaml.safe_load(text)
    components = spec.get("components", {}).get("schemas", {})
    info = spec.get("info", {})
    for path, ops in spec.get("paths", {}).items():
        for method, op in ops.items():
            if not isinstance(op, dict):
                continue
            lines = [f"{info.get('title', 'API')} {info.get('version', '')}: {method.upper()} {path}".strip(),
                     op.get("summary") or op.get("description", "")]
            if op.get("operationId"):
                lines.append(f"operationId: {op['operationId']}")
            for p in op.get("parameters", []):
                desc = f" - {p['description']}" if p.get("description") else ""
                req = ", required" if p.get("required") else ""
                lines.append(f"parameter {p['name']} (in {p['in']}{req}): {_schema_text(p.get('schema', {}), components)}{desc}")
            body = op.get("requestBody", {})
            for mime, content in body.get("content", {}).items():
                lines.append(f"request body ({mime}): {_schema_text(content.get('schema', {}), components) or 'any'}")
            for code, resp in op.get("responses", {}).items():
                schemas = "; ".join(_schema_text(c.get("schema", {}), components) for c in resp.get("content", {}).values())
                lines.append(f"response {code}: {resp.get('description', '')}" + (f" - {schemas}" if schemas.strip("; ") else ""))
            yield f"{method.upper()} {path}", "\n".join(l for l in lines if l)


# ---------- main --------------------------------------------------------------

def iter_files(src, raw_dir: Path):
    if "local" in src:
        roots = [Path(src["local"])]
    else:
        roots = [raw_dir / src["name"] / p for p in src["paths"]]
    excludes = src.get("exclude", [])
    for f in src.get("files", []):
        yield raw_dir / src["name"] / f
    for root in roots:
        if not root.exists():
            print(f"  skip missing {root}")
            continue
        for g in src["globs"]:
            for p in root.glob(g):
                rel = "/" + p.relative_to(root).as_posix()
                if any(fnmatch.fnmatch(rel, pat) for pat in excludes):
                    continue
                yield p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="sources.yaml")
    ap.add_argument("--raw", default="raw")
    ap.add_argument("--out", default="work/chunks.jsonl")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out.open("w") as f:
        for src in cfg["sources"]:
            count = dropped = 0
            attrs = {}
            keep = set(src.get("keep_placeholders", []))   # literal {name} text (REST path parameters), not attributes
            for rel in src.get("attribute_files", []):
                parse_adoc_attributes(Path(args.raw) / src["name"] / rel, attrs)
            for path in sorted(set(iter_files(src, Path(args.raw)))):
                raw_text = path.read_text(errors="ignore")
                if path.suffix == ".adoc":
                    text = clean_adoc(raw_text, attrs, keep)
                elif path.suffix in CLEANERS:
                    text = CLEANERS[path.suffix](raw_text)
                elif path.suffix in (".yaml", ".yml"):
                    text = raw_text
                else:
                    continue
                pieces = chunk_openapi(text) if path.suffix in (".yaml", ".yml") else chunk_text(text, src.get("max_chars", MAX_CHARS))
                for i, (title, body) in enumerate(pieces):
                    if path.suffix == ".adoc" and any(m.group(1) not in keep for m in ADOC_PLACEHOLDER.finditer(body)):
                        dropped += 1
                        continue
                    cid = hashlib.sha1(f"{path}:{i}:{title}:{body}".encode()).hexdigest()[:16]
                    f.write(json.dumps({
                        "id": cid, "source": src["name"], "license": src.get("license"),
                        "path": str(path), "title": title, "text": body,
                    }) + "\n")
                    count += 1
            print(f"{src['name']}: {count} chunks" + (f", dropped {dropped} with unresolved placeholders" if dropped else ""))
            n += count
    print(f"total {n} chunks -> {out}")


if __name__ == "__main__":
    main()
