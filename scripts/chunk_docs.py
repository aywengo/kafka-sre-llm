#!/usr/bin/env python3
"""Convert Markdown / AsciiDoc / HTML docs into clean, heading-aware chunks.

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

# Strimzi AsciiDoc attributes worth resolving; unknown ones are left as-is.
ADOC_ATTRS = {
    "ProductName": "Strimzi",
    "ProductLongName": "Strimzi",
    "ProductVersion": "<strimzi-version>",
    "DefaultKafkaVersion": "<kafka-version>",
}


# ---------- format cleaners ---------------------------------------------------

def clean_markdown(text: str) -> str:
    text = re.sub(r"^---\n.*?\n---\n", "", text, flags=re.S)            # front matter
    text = re.sub(r'{{<\s*glossary_tooltip[^>]*?text="([^"]+)"[^>]*>}}', r"\1", text)
    text = re.sub(r"{{[<%].*?[%>]}}", "", text, flags=re.S)                # other Hugo shortcodes
    text = re.sub(r"<!--.*?-->", "", text, flags=re.S)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)                       # images
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)                   # links -> text
    return text


def clean_adoc(text: str) -> str:
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
            if re.match(r"^\[(id|role|source|discrete|options|cols|NOTE|IMPORTANT|WARNING|TIP|\.)", s):
                continue
            m = re.match(r"^(=+)\s+(.*)", line)
            if m:
                line = "#" * len(m.group(1)) + " " + m.group(2)
            line = re.sub(r"xref:[^\[]+\[([^\]]*)\]", r"\1", line)
            line = re.sub(r"link:[^\[]+\[([^\]]*)\]", r"\1", line)
        line = re.sub(r"\{(\w+)\}", lambda m: ADOC_ATTRS.get(m.group(1), m.group(0)), line)
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


CLEANERS = {".md": clean_markdown, ".adoc": clean_adoc, ".html": clean_html, ".txt": lambda t: t}


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


def split_long(body: str):
    if len(body) <= MAX_CHARS:
        return [body]
    parts, cur = [], ""
    for para in re.split(r"\n\s*\n", body):
        if cur and len(cur) + len(para) > MAX_CHARS:
            parts.append(cur)
            cur = ""
        cur = (cur + "\n\n" + para).strip()
    if cur:
        parts.append(cur)
    return parts


def chunk_text(text: str):
    pending_title, pending = None, ""
    for title, body in split_sections(text):
        body = re.sub(r"\n{3,}", "\n\n", body)
        if pending and len(pending) < MIN_CHARS:          # merge tiny sections forward
            body = pending + "\n\n" + (f"## {title}\n" if title else "") + body
            title = pending_title or title
            pending = ""
        for part in split_long(body):
            if len(part) < MIN_CHARS:
                pending_title, pending = title, part
                continue
            yield title, part
    if pending and len(pending) >= 150:
        yield pending_title, pending


# ---------- main --------------------------------------------------------------

def iter_files(src, raw_dir: Path):
    if "local" in src:
        roots = [Path(src["local"])]
    else:
        roots = [raw_dir / src["name"] / p for p in src["paths"]]
    excludes = src.get("exclude", [])
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
            count = 0
            for path in sorted(set(iter_files(src, Path(args.raw)))):
                cleaner = CLEANERS.get(path.suffix)
                if not cleaner:
                    continue
                text = cleaner(path.read_text(errors="ignore"))
                for i, (title, body) in enumerate(chunk_text(text)):
                    cid = hashlib.sha1(f"{path}:{i}:{title}:{body}".encode()).hexdigest()[:16]
                    f.write(json.dumps({
                        "id": cid, "source": src["name"], "license": src.get("license"),
                        "path": str(path), "title": title, "text": body,
                    }) + "\n")
                    count += 1
            print(f"{src['name']}: {count} chunks")
            n += count
    print(f"total {n} chunks -> {out}")


if __name__ == "__main__":
    main()
