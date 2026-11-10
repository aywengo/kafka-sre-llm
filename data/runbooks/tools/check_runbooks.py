#!/usr/bin/env python3
"""Mechanical checks for data/runbooks against the pinned sources.

Usage:
    python3 -I tools/check_runbooks.py --sources <cache_dir> [check ...]

<cache_dir> is the directory filled by tools/fetch_sources.sh. Checks:

    code       extract fenced code blocks and inline code spans from the
               runbooks and match them against the pinned sources: a block
               must appear line for line inside one source code element,
               commands must equal a complete source command, and anything
               else must be listed in tools/allowed_unmatched.txt; an
               unterminated fence fails
    citations  check every cited anchor, GitHub path and backtick repo path
    steps      check every top-level list step in the Diagnosis, Remediation,
               Verification and Rollback sections for a Source line or a
               NO OFFICIAL SOURCE marker
    coverage   count coverage.md rows by status, compare with the Totals
               table and the eval counts, and flag "covered" rows whose
               runbook says NO OFFICIAL SOURCE in a required section
    eval       check eval/*.jsonl: fields, runbook path, source_url cited in
               the runbook

With no check named, all of them run. Exit status is 1 if any check reports
a failure. Use -v to list every item that is not found.

Only the Python standard library is used.
"""

import argparse
import glob
import hashlib
import html
import html.parser
import json
import os
import re
import sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)  # data/runbooks

STRIMZI_GUIDE = "https://strimzi.io/docs/operators/1.3.1/deploying"
KAFKA_PAGES = {
    "https://kafka.apache.org/43/operations/basic-kafka-operations/": "k-basic.html",
    "https://kafka.apache.org/43/operations/kraft/": "k-kraft.html",
    "https://kafka.apache.org/43/operations/monitoring/": "k-monitoring.html",
    "https://kafka.apache.org/43/operations/geo-replication-cross-cluster-data-mirroring/": "k-geo.html",
    "https://kafka.apache.org/43/getting-started/zk2kraft/": "k-zk2kraft.html",
}
STRIMZI_REPO = "https://github.com/strimzi/strimzi-kafka-operator/"
DRAIN_REPO = "https://github.com/strimzi/drain-cleaner/"
DRAIN_SHA = "70f1bc25bc14aacd15eb478768db6566bde783e3"
STRIMZI_SHA = "555c3e0f07c203911bc3cb7d13ac085bfaa81b50"
KAFKA_SHA = "0a6c96d3b61167f1141df2032f25f10fd6e118fe"

STEP_SECTIONS = ("diagnosis", "remediation", "verification", "rollback")
NOSRC = re.compile(r"NO OFFICIAL SOURCE", re.I)
SRCLINE = re.compile(r"\*Source:\*")
URL_RE = re.compile(r"https?://[^\s)<>\]|`\"']+")


# ---------------------------------------------------------------- helpers

class _Text(html.parser.HTMLParser):
    """Collect page text and the set of element ids."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.ids = set()
        self.code = []  # text of each <pre> or <code> element
        self._skip = 0
        self._code_depth = 0
        self._buf = []

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1
        if tag in ("pre", "code"):
            if self._code_depth == 0:
                self._buf = []
            self._code_depth += 1
        for k, v in attrs:
            if k in ("id", "name") and v:
                self.ids.add(v)

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self._skip:
            self._skip -= 1
        if tag in ("pre", "code") and self._code_depth:
            self._code_depth -= 1
            if self._code_depth == 0:
                self.code.append("".join(self._buf))

    def handle_data(self, data):
        if not self._skip:
            self.parts.append(data)
            if self._code_depth:
                self._buf.append(data)


def parse_html(path, with_code=False):
    p = _Text()
    with open(path, encoding="utf-8", errors="replace") as f:
        p.feed(f.read())
    if with_code:
        return "".join(p.parts), p.ids, p.code
    return "".join(p.parts), p.ids


def squash(s):
    """Normalise for matching: unescape, unify dashes and quotes, drop
    markdown backslash escapes and all whitespace."""
    s = html.unescape(s)
    s = s.replace("–", "--").replace("—", "--")
    s = s.replace("‘", "'").replace("’", "'")
    s = s.replace("“", '"').replace("”", '"')
    s = re.sub(r"\\([-_*.#])", r"\1", s)
    return re.sub(r"\s+", "", s)


def _unify(s):
    s = html.unescape(s)
    s = s.replace("\u2013", "--").replace("\u2014", "--")
    s = s.replace("\u2018", "'").replace("\u2019", "'")
    s = s.replace("\u201c", '"').replace("\u201d", '"')
    return re.sub(r"\\([-_*.#])", r"\1", s)


def spaced(s):
    """Normalise for command matching: like squash(), but runs of whitespace
    become one space instead of nothing, so 'secret my-x' != 'secretmy-x'."""
    return re.sub(r"\s+", " ", _unify(s)).strip()


CMD_RE = re.compile(r"^\s*(\$ )?(kubectl|bin/|oc |openssl|helm )")


def is_cmd(line):
    return bool(CMD_RE.match(line))


def cmd_key(cmd):
    """Normalised form used to compare complete commands."""
    s = spaced(cmd)
    s = re.sub(r"^\$ ", "", s)
    s = re.sub(r"\s*(<\d+>|\(\d+\))$", "", s)  # trailing callout
    return s


CALLOUT = re.compile(r"\s*(#\s*)?(<\d+>|\(\d+\))\s*$")


def block_lines(s):
    """Lines of a code block for exact comparison: whitespace collapsed (not
    removed), callout markers and blank lines dropped."""
    out = []
    for line in s.split("\n"):
        line = CALLOUT.sub("", spaced(line))
        if line:
            out.append(re.sub(r"^\$ ", "", line))
    return out


def load_unmatched():
    """Code items that are knowingly not verbatim (abridged blocks,
    placeholders, composed identifiers). One per line:
    runbook path, TAB, key, TAB, reason. The key of a span is its text; the
    key of a block is 'block:' plus the first 12 hex chars of the SHA-256 of
    its normalised lines, so any edit to the block invalidates the entry."""
    out = {}
    path = os.path.join(HERE, "allowed_unmatched.txt")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            if line.strip() and not line.startswith("#"):
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 3:
                    out[(parts[0], parts[1])] = parts[2]
    return out


def block_key(content):
    return "block:" + hashlib.sha256("\n".join(block_lines(content)).encode()).hexdigest()[:12]


def _open_quote(s):
    """True if s ends inside a double- or single-quoted string."""
    q = None
    i = 0
    while i < len(s):
        c = s[i]
        if c == "\\" and q != "'":
            i += 2
            continue
        if q is None and c in "\"'":
            q = c
        elif c == q:
            q = None
        i += 1
    return q is not None


def logical_commands(content):
    """Yield each command in a block or span with its continuation lines
    (trailing backslash) and multi-line quoted arguments joined on."""
    lines = content.split("\n")
    i = 0
    while i < len(lines):
        if not is_cmd(lines[i]):
            i += 1
            continue
        buf = [lines[i]]
        while i + 1 < len(lines) and (buf[-1].rstrip().endswith("\\") or _open_quote("\n".join(buf))):
            i += 1
            buf.append(lines[i])
        yield "\n".join(buf)
        i += 1


def load_abbreviated():
    """Inline command mentions that are deliberately shortened in prose.
    One per line: the normalised command, a tab, the reason."""
    out = {}
    path = os.path.join(HERE, "abbreviated_commands.txt")
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            if line.strip() and not line.startswith("#"):
                cmd, _, why = line.rstrip("\n").partition("\t")
                out[cmd.strip()] = why.strip()
    return out


def runbook_files():
    return sorted(glob.glob(os.path.join(ROOT, "runbooks", "*", "*.md")))


def rel(p):
    return os.path.relpath(p, ROOT)


class Sources:
    def __init__(self, cache):
        self.cache = cache
        need = ["strimzi", "drain-cleaner", "kafka43", "web"]
        missing = [d for d in need if not os.path.isdir(os.path.join(cache, d))]
        if missing:
            sys.exit("missing in %s: %s (run tools/fetch_sources.sh)" % (cache, ", ".join(missing)))
        self.strimzi = os.path.join(cache, "strimzi")
        self.drain = os.path.join(cache, "drain-cleaner")
        self.kafka = os.path.join(cache, "kafka43")
        self.verify_page_hashes()
        self.verify_repos()
        self.pages = {}
        self.page_code = []
        for url, f in list(KAFKA_PAGES.items()) + [(STRIMZI_GUIDE, "strimzi.html")]:
            text, ids, code = parse_html(os.path.join(cache, "web", f), with_code=True)
            self.pages[url] = (text, ids)
            self.page_code.extend(code)
        self._corpus = None

    def verify_page_hashes(self):
        """Refuse to run against cached pages whose bytes differ from the pins."""
        bad = []
        with open(os.path.join(HERE, "pinned_pages.txt"), encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) != 3 or parts[0].startswith("#"):
                    continue
                name, _, want = parts
                path = os.path.join(self.cache, "web", name)
                if not os.path.isfile(path):
                    bad.append("%s missing" % name)
                    continue
                with open(path, "rb") as pf:
                    got = hashlib.sha256(pf.read()).hexdigest()[:16]
                if got != want:
                    bad.append("%s sha256 prefix %s, pinned %s" % (name, got, want))
        if bad:
            sys.exit("cached pages do not match tools/pinned_pages.txt:\n  " + "\n  ".join(bad))

    def verify_repos(self):
        """Refuse to run against a repo cache that is not exactly the pinned
        commit: wrong HEAD, modified tracked files or untracked files."""
        import subprocess
        bad = []
        for d, want in ((self.strimzi, STRIMZI_SHA), (self.drain, DRAIN_SHA), (self.kafka, KAFKA_SHA)):
            def git(*a):
                return subprocess.run(["git", "-C", d] + list(a), capture_output=True, text=True, check=True).stdout
            try:
                head = git("rev-parse", "HEAD").strip()
                dirty = git("status", "--porcelain", "--untracked-files=all", "--ignored=matching")
            except (OSError, subprocess.CalledProcessError) as e:
                bad.append("%s: git failed (%s)" % (d, e))
                continue
            if head != want:
                bad.append("%s: HEAD %s, pinned %s" % (d, head, want))
            if dirty.strip():
                bad.append("%s: working tree differs from the pinned commit:\n    %s" % (
                    d, "\n    ".join(dirty.strip().split("\n")[:10])))
        if bad:
            sys.exit("source cache is not the pinned commits:\n  " + "\n  ".join(bad))

    def source_commands(self):
        """Complete logical commands found in code elements of the sources:
        <pre>/<code> on the pages, fenced/delimited blocks and inline code in
        the .adoc and .md files."""
        if getattr(self, "_cmds", None) is not None:
            return self._cmds
        cmds = set()
        for e in self.code_elements():
            for cmd in logical_commands(e):
                cmds.add(cmd_key(cmd))
        self._cmds = cmds
        return cmds

    def code_elements(self):
        """Raw text of every code element in the sources, plus each example
        file under packaging/examples as one element."""
        if getattr(self, "_elems", None) is not None:
            return self._elems
        elems = list(self.page_code)
        for c in self._raw_chunks()[len(self.pages):]:
            # AsciiDoc ---- / .... blocks and Markdown ``` fences
            elems += re.findall(r"(?ms)^(?:----|\.\.\.\.)\s*\n(.*?)^(?:----|\.\.\.\.)\s*$", c)
            elems += re.findall(r"(?ms)^\s*```[^\n]*\n(.*?)^\s*```", c)
            # AsciiDoc one-line literal after [source,...] with no delimiter
            # (an undelimited block runs to the next blank line)
            elems += re.findall(r"(?m)^\[source[^\]]*\]\n((?!----)(?:[^\n]+\n?)+)", c)
            elems += re.findall(r"`([^`\n]+)`", c)
        for pat in ("packaging/examples/**/*.yaml", "packaging/examples/**/*.json"):
            for f in glob.glob(os.path.join(self.strimzi, pat), recursive=True):
                elems.append(open(f, encoding="utf-8", errors="replace").read())
        self._elems = elems
        return elems

    def block_index(self):
        """Normalised line lists of all code elements, indexed by line."""
        if getattr(self, "_bidx", None) is None:
            seqs = [block_lines(e) for e in self.code_elements()]
            idx = defaultdict(list)
            for si, s in enumerate(seqs):
                for li, line in enumerate(s):
                    idx[line].append((si, li))
            self._bidx = (seqs, idx)
        return self._bidx

    def block_in_one_element(self, content):
        """True if the block's lines appear, in order and contiguous, inside a
        single source code element."""
        want = block_lines(content)
        if not want:
            return True
        seqs, idx = self.block_index()
        for si, li in idx.get(want[0], ()):
            if seqs[si][li:li + len(want)] == want:
                return True
        return False

    def spaced_corpus(self):
        """Like corpus(), but whitespace is collapsed, not removed, so that
        argument boundaries in commands are kept."""
        if getattr(self, "_spaced", None) is None:
            self._spaced = "\x00".join(spaced(c) for c in self._raw_chunks())
        return self._spaced

    def _raw_chunks(self):
        if getattr(self, "_raw", None) is None:
            chunks = [t for t, _ in self.pages.values()]
            pats = [
                (self.strimzi, "documentation/**/*.adoc"),
                (self.strimzi, "packaging/examples/**/*.yaml"),
                (self.strimzi, "packaging/examples/**/*.json"),
                (self.strimzi, "packaging/install/**/*.yaml"),
                (self.strimzi, "kafka-versions.yaml"),
                (self.kafka, "docs/**/*.md"),
                (self.drain, "README.md"),
            ]
            for base, pat in pats:
                for p in glob.glob(os.path.join(base, pat), recursive=True):
                    with open(p, encoding="utf-8", errors="replace") as f:
                        chunks.append(f.read())
            self._raw = chunks
        return self._raw

    def corpus(self):
        if self._corpus is None:
            self._corpus = "\x00".join(squash(c) for c in self._raw_chunks())
        return self._corpus

    def repo_path_exists(self, path):
        """A repo-relative path or glob in strimzi-kafka-operator 1.3.1.
        Bare file names are searched for under documentation/."""
        path = re.sub(r":\d+(-\d+)?$", "", path.strip())
        if path.startswith("./"):
            path = path[2:]
        if path.startswith("docs/"):
            return bool(glob.glob(os.path.join(self.kafka, path)))
        if "/" not in path:
            return bool(glob.glob(os.path.join(self.strimzi, "documentation", "**", path), recursive=True))
        for base in (self.strimzi, os.path.join(self.strimzi, "documentation")):
            if glob.glob(os.path.join(base, path)):
                return True
        return False


# ---------------------------------------------------------------- markdown

FENCE = re.compile(r"^(\s*)```")


def code_items(text):
    """Yield (kind, line_no, content) for fenced blocks and inline spans."""
    lines = text.split("\n")
    in_block = False
    buf, start = [], 0
    for i, line in enumerate(lines, 1):
        if FENCE.match(line):
            if in_block:
                yield ("block", start, "\n".join(buf))
                in_block, buf = False, []
            else:
                in_block, start = True, i
            continue
        if in_block:
            buf.append(line)
            continue
        for m in re.finditer(r"`([^`\n]+)`", line):
            yield ("span", i, m.group(1))
    if in_block:
        # an unclosed fence: report it, and still check what it contains
        yield ("unterminated", start, "")
        yield ("block", start, "\n".join(buf))


def strip_fences(text):
    out, in_block = [], False
    for line in text.split("\n"):
        if FENCE.match(line):
            in_block = not in_block
            out.append("")
            continue
        out.append("" if in_block else line)
    return "\n".join(out)


LIST_ITEM = re.compile(r"^(\d+\.|[-*])\s+")


def step_units(text):
    """Top-level list items inside the step sections.
    Returns a list of (section, line_no, body)."""
    lines = strip_fences(text).split("\n")
    raw = text.split("\n")
    section = None
    units = []
    cur = None
    for i, line in enumerate(lines):
        if line.startswith("## "):
            if cur:
                units.append(cur)
                cur = None
            name = line[3:].strip().lower()
            section = next((s for s in STEP_SECTIONS if name.startswith(s)), None)
            continue
        if section is None:
            continue
        if line.startswith("#"):
            if cur:
                units.append(cur)
                cur = None
            continue
        if LIST_ITEM.match(line):
            if cur:
                units.append(cur)
            cur = [section, i + 1, [raw[i]]]
            continue
        if cur is not None:
            if line.strip() == "" or line.startswith((" ", "\t")) or raw[i].startswith((" ", "\t")):
                cur[2].append(raw[i])
            else:
                units.append(cur)
                cur = None
    if cur:
        units.append(cur)
    return [(s, n, "\n".join(b)) for s, n, b in units]


def sections(text):
    """Map lower-case h2 title -> body text."""
    out, name, buf = {}, None, []
    for line in text.split("\n"):
        if line.startswith("## "):
            if name is not None:
                out[name] = "\n".join(buf)
            name, buf = line[3:].strip().lower(), []
        elif name is not None:
            buf.append(line)
    if name is not None:
        out[name] = "\n".join(buf)
    return out


# ---------------------------------------------------------------- checks

REPOPATH = re.compile(r"^(\./)?(documentation|packaging|install|examples|docs)/[\w./*{}()\-]+(:\d+(-\d+)?)?$|^[\w.\-]+\.(adoc|yaml|json)(:\d+(-\d+)?)?$")


def check_code(src, verbose):
    corpus = src.corpus()
    total = Counter()
    found = Counter()
    paths_ok, paths_bad, notfound = [], [], []
    cmd_total = cmd_bad = 0
    cmd_bad_items = []
    cmd_abbrev = 0
    source_cmds = src.source_commands()
    abbreviated = load_abbreviated()
    abbreviated_used = set()
    blocks_contig = 0
    unmatched = load_unmatched()
    allowed_used = set()
    allowed_n = 0
    unterminated = []
    for p in runbook_files():
        text = open(p, encoding="utf-8").read()
        for kind, ln, content in code_items(text):
            if kind == "unterminated":
                unterminated.append((rel(p), ln))
                continue
            total[kind] += 1
            sq = squash(content)
            if not sq:
                found[kind] += 1
                continue
            # commands are checked as complete logical commands (with their
            # continuation lines and multi-line quoted arguments), whitespace kept
            for cmd in logical_commands(content):
                cmd_total += 1
                key = cmd_key(cmd)
                if key in source_cmds:
                    continue
                if kind == "span" and key in abbreviated:
                    cmd_abbrev += 1
                    abbreviated_used.add(key)
                    continue
                cmd_bad += 1
                cmd_bad_items.append((rel(p), ln, key[:140]))
            if kind == "block":
                # a block must sit, line for line, inside one source element
                if src.block_in_one_element(content):
                    found[kind] += 1
                    blocks_contig += 1
                    continue
                key = block_key(content)
                if (rel(p), key) in unmatched:
                    allowed_used.add((rel(p), key))
                    allowed_n += 1
                    continue
                first = next((l for l in block_lines(content) if not src.block_in_one_element(l)), None)
                why = ("first line not in any source element: %r" % first[:80]) if first is not None \
                    else "every line exists, but not as one contiguous run in one source element"
                notfound.append((rel(p), ln, kind, "%s; %s" % (key, why)))
                continue
            if sq in corpus:
                found[kind] += 1
                continue
            if REPOPATH.match(content.strip()):
                (paths_ok if src.repo_path_exists(content) else paths_bad).append((rel(p), ln, content))
                continue
            if (rel(p), content) in unmatched:
                allowed_used.add((rel(p), content))
                allowed_n += 1
                continue
            notfound.append((rel(p), ln, kind, content[:100]))
    n_all = sum(total.values())
    n_found = sum(found.values())
    print("== code")
    print("code items: %d (%d blocks, %d spans)" % (n_all, total["block"], total["span"]))
    print("found in a pinned source: %d of %d" % (n_found, n_all))
    print("  blocks found line for line inside one source code element: %d of %d" % (blocks_contig, total["block"]))
    print("repo-path spans: %d, exist in the pinned repos: %d, missing: %d" % (len(paths_ok) + len(paths_bad), len(paths_ok), len(paths_bad)))
    print("commands (kubectl/bin//oc/openssl/helm, anywhere in a block, with continuations): %d" % cmd_total)
    print("  equal to a complete command in a source code element: %d" % (cmd_total - cmd_abbrev - cmd_bad))
    print("  inline mentions listed in tools/abbreviated_commands.txt: %d" % cmd_abbrev)
    print("  not verbatim: %d" % cmd_bad)
    stale = sorted(set(abbreviated) - abbreviated_used)
    for s in stale:
        print("  UNUSED ABBREVIATION ENTRY %s" % s)
    cmd_bad += len(stale)
    for f, ln, c in cmd_bad_items:
        print("  COMMAND NOT VERBATIM %s:%d %s" % (f, ln, c))
    print("items not verbatim but listed with a reason in tools/allowed_unmatched.txt: %d" % allowed_n)
    print("items not found and not listed: %d" % len(notfound))
    print("unterminated code fences: %d" % len(unterminated))
    stale_u = sorted(set(unmatched) - allowed_used)
    for f, ln, c in paths_bad:
        print("  MISSING PATH %s:%d %s" % (f, ln, c))
    for f, ln, kind, c in notfound:
        print("  NOT FOUND %s:%d [%s] %s" % (f, ln, kind, c))
    for f, ln in unterminated:
        print("  UNTERMINATED FENCE %s:%d" % (f, ln))
    for f, k in stale_u:
        print("  UNUSED ALLOWLIST ENTRY %s %s" % (f, k))
    return not paths_bad and cmd_bad == 0 and not notfound and not unterminated and not stale_u


def cited_urls(text):
    for m in URL_RE.finditer(text):
        yield m.group(0).rstrip(".,;:")


def check_url(src, url):
    """Return (kind, ok, detail)."""
    base, _, frag = url.partition("#")
    if base.rstrip("/") == STRIMZI_GUIDE:
        if not frag:
            return ("strimzi-anchor", True, "")
        return ("strimzi-anchor", frag in src.pages[STRIMZI_GUIDE][1], frag)
    for page in KAFKA_PAGES:
        if base.rstrip("/") + "/" == page:
            if not frag:
                return ("kafka-anchor", True, "")
            return ("kafka-anchor", frag in src.pages[page][1], frag)
    # index pages, fetched but not cited for content (sources.md, ATTRIBUTION.md)
    index_pages = {"https://strimzi.io/documentation/": "strimzi-documentation.html",
                   "https://kafka.apache.org/43/operations/": "k-operations.html"}
    if base in index_pages and not frag:
        return ("index-page", os.path.exists(os.path.join(src.cache, "web", index_pages[base])), "")
    if base.startswith("https://kafka.apache.org/") or base.startswith("https://strimzi.io/"):
        return ("other-doc-page", False, "not a pinned page")
    m = re.match(re.escape(STRIMZI_REPO) + r"(?:blob|tree)/1\.3\.1/(.+)$", base)
    if m:
        return ("strimzi-repo", os.path.exists(os.path.join(src.strimzi, m.group(1))), m.group(1))
    if base.startswith(STRIMZI_REPO):
        return ("strimzi-repo", False, "not pinned at tag 1.3.1")
    m = re.match(re.escape(DRAIN_REPO) + r"(?:blob|tree)/" + DRAIN_SHA + r"/(.+)$", base)
    if m:
        return ("drain-cleaner-repo", os.path.exists(os.path.join(src.drain, m.group(1))), m.group(1))
    if base.startswith(DRAIN_REPO):
        return ("drain-cleaner-repo", False, "not pinned at " + DRAIN_SHA[:7])
    return ("other", True, "")


def check_citations(src, verbose):
    occ = Counter()
    distinct = {}
    bad = []
    # sources.md lists fetched pages that are not cited, and the audit files
    # are records, so only the runbooks and the derived files are checked.
    files = runbook_files() + [os.path.join(ROOT, f) for f in ("FINDINGS.md", "coverage.md", "RESOLUTION.md", "ATTRIBUTION.md")
                               if os.path.exists(os.path.join(ROOT, f))]
    for p in files:
        text = open(p, encoding="utf-8").read()
        for url in cited_urls(text):
            kind, ok, detail = check_url(src, url)
            occ[kind] += 1
            distinct[url] = (kind, ok)
            if not ok:
                bad.append((rel(p), url, detail))
    for p in sorted(glob.glob(os.path.join(ROOT, "eval", "*.jsonl"))):
        for i, line in enumerate(open(p, encoding="utf-8")):
            if not line.strip():
                continue
            url = json.loads(line).get("source_url", "")
            kind, ok, detail = check_url(src, url)
            occ[kind] += 1
            distinct[url] = (kind, ok)
            if not ok:
                bad.append(("%s:%d" % (rel(p), i), url, detail))
    # backtick repo paths in Sources rows and prose
    path_total = 0
    path_bad = []
    for p in runbook_files():
        text = open(p, encoding="utf-8").read()
        for kind, ln, content in code_items(text):
            c = content.strip()
            if kind == "span" and REPOPATH.match(c) and ("/" in c or c.endswith(".adoc")):
                path_total += 1
                if not src.repo_path_exists(c):
                    path_bad.append((rel(p), ln, c))
    # every pinned-source URL cited in a runbook body is also in its Sources row
    row_missing = []
    for p in runbook_files():
        lines = open(p, encoding="utf-8").read().split("\n")
        idx = next((i for i, l in enumerate(lines) if l.startswith("| Sources |")), None)
        if idx is None:
            row_missing.append((rel(p), "(no Sources row)"))
            continue
        for url in cited_urls("\n".join(lines[idx + 1:])):
            kind, _, _ = check_url(src, url)
            if kind != "other" and url not in lines[idx]:
                row_missing.append((rel(p), url))
    by_kind = Counter(k for k, _ in distinct.values())
    print("== citations")
    print("URL occurrences: %d; distinct URLs: %d" % (sum(occ.values()), len(distinct)))
    for k in sorted(by_kind):
        nbad = sum(1 for u, (kk, ok) in distinct.items() if kk == k and not ok)
        print("  %-20s distinct %3d, failing %d" % (k, by_kind[k], nbad))
    print("backtick repo paths: %d, missing in the pinned repos: %d" % (path_total, len(path_bad)))
    print("body citations missing from the runbook's Sources row: %d" % len(row_missing))
    for f, url in row_missing:
        print("  NOT IN SOURCES ROW %s %s" % (f, url))
    for f, url, d in bad:
        print("  BAD %s %s (%s)" % (f, url, d))
    for f, ln, c in path_bad:
        print("  MISSING PATH %s:%d %s" % (f, ln, c))
    return not bad and not path_bad and not row_missing


def check_steps(verbose):
    per_sec = Counter()
    missing = []
    n_src_lines = 0
    marked = 0
    for p in runbook_files():
        text = open(p, encoding="utf-8").read()
        n_src_lines += len(SRCLINE.findall(text))
        for sec, ln, body in step_units(text):
            per_sec[sec] += 1
            has_src = bool(SRCLINE.search(body))
            has_no = bool(NOSRC.search(body))
            if has_no and not has_src:
                marked += 1
            if not has_src and not has_no:
                missing.append((rel(p), ln, body.split("\n")[0][:90]))
    total = sum(per_sec.values())
    print("== steps")
    print("top-level list steps in %s: %d (%s)" % (
        "/".join(STEP_SECTIONS), total, ", ".join("%s %d" % (s, per_sec[s]) for s in STEP_SECTIONS)))
    print("steps with a Source line: %d of %d" % (total - marked - len(missing), total))
    print("steps with only a NO OFFICIAL SOURCE marker: %d of %d" % (marked, total))
    print("steps with neither: %d of %d" % (len(missing), total))
    print("Source lines in all runbooks: %d" % n_src_lines)
    for f, ln, first in missing:
        print("  NO SOURCE %s:%d %s" % (f, ln, first))
    return not missing


ROW = re.compile(r"^\|(.+)\|\s*$")
LINK = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")
REQUIRED = ("diagnosis", "remediation", "verification")


def coverage_rows():
    text = open(os.path.join(ROOT, "coverage.md"), encoding="utf-8").read()
    rows = []
    for line in text.split("\n"):
        m = ROW.match(line)
        if not m:
            continue
        cells = [c.strip() for c in m.group(1).split("|")]
        if len(cells) != 6 or cells[0] in ("Area", "---") or set(cells[0]) <= {"-"}:
            continue
        rows.append(cells)
    return text, rows


def runbook_nosource_sections(path):
    text = open(path, encoding="utf-8").read()
    out = []
    for name, body in sections(text).items():
        req = next((r for r in REQUIRED if name.startswith(r)), None)
        if req and NOSRC.search(body):
            out.append(req)
    return sorted(set(out))


def check_coverage(verbose, write=False):
    text, rows = coverage_rows()
    counts = Counter(r[2] for r in rows)
    ok = True
    should_partial = []
    eval_mismatch = []
    for area, topic, status, rb, ev, gaps in rows:
        m = LINK.search(rb)
        if m:
            path = os.path.join(ROOT, m.group(2))
            if not os.path.exists(path):
                print("  MISSING RUNBOOK %s" % m.group(2))
                ok = False
            elif status == "covered":
                secs = runbook_nosource_sections(path)
                if secs:
                    should_partial.append((topic, m.group(2), secs))
        m = LINK.search(ev)
        if m:
            n = sum(1 for l in open(os.path.join(ROOT, m.group(2)), encoding="utf-8") if l.strip())
            if str(n) != m.group(1):
                eval_mismatch.append((m.group(2), m.group(1), n))
    tot = re.search(r"\| covered \| (\d+) \|\n\| partial \| (\d+) \|\n\| no official source \| (\d+) \|\n\| \*\*total\*\* \| \*\*(\d+)\*\* \|", text)
    print("== coverage")
    print("topic rows: %d (covered %d, partial %d, no official source %d)" % (
        len(rows), counts["covered"], counts["partial"], counts["no official source"]))
    if tot:
        stated = tuple(int(x) for x in tot.groups())
        actual = (counts["covered"], counts["partial"], counts["no official source"], len(rows))
        print("Totals table: covered %d, partial %d, none %d, total %d -> %s" % (
            stated + ("matches" if stated == actual else "DOES NOT MATCH",)))
        if stated != actual:
            ok = False
            if write:
                new = ("| covered | %d |\n| partial | %d |\n| no official source | %d |\n| **total** | **%d** |" % actual)
                text = text[:tot.start()] + new + text[tot.end():]
                open(os.path.join(ROOT, "coverage.md"), "w", encoding="utf-8").write(text)
                print("  Totals table rewritten")
    else:
        print("Totals table: not found")
        ok = False
    print("'covered' rows whose runbook says NO OFFICIAL SOURCE in a required section: %d" % len(should_partial))
    for topic, rb, secs in should_partial:
        print("  SHOULD BE PARTIAL %s (%s: %s)" % (topic, rb, ", ".join(secs)))
    print("eval count links that do not match the file: %d" % len(eval_mismatch))
    for f, stated, n in eval_mismatch:
        print("  EVAL COUNT %s says %s, file has %d" % (f, stated, n))
    return ok and not should_partial and not eval_mismatch


def check_eval(verbose):
    n = 0
    bad = []
    per_file = {}
    questions = defaultdict(list)
    for p in sorted(glob.glob(os.path.join(ROOT, "eval", "*.jsonl"))):
        k = 0
        for i, line in enumerate(open(p, encoding="utf-8")):
            if not line.strip():
                continue
            k += 1
            n += 1
            where = "%s:%d" % (rel(p), i)
            try:
                d = json.loads(line)
            except ValueError as e:
                bad.append((where, "invalid JSON: %s" % e))
                continue
            for key in ("question", "reference_answer", "source_url", "runbook"):
                if not d.get(key):
                    bad.append((where, "missing " + key))
            rb = os.path.join(ROOT, d.get("runbook", ""))
            if not os.path.isfile(rb):
                bad.append((where, "runbook not found: %s" % d.get("runbook")))
                continue
            if d.get("source_url", "") not in open(rb, encoding="utf-8").read():
                bad.append((where, "source_url not cited in its runbook"))
            questions[re.sub(r"\W+", " ", d.get("question", "").lower()).strip()].append(where)
        per_file[rel(p)] = k
    dups = {q: w for q, w in questions.items() if len(w) > 1}
    print("== eval")
    print("eval items: %d in %d files (%d to %d per file)" % (
        n, len(per_file), min(per_file.values()), max(per_file.values())))
    print("items failing a field/runbook/source_url check: %d of %d" % (len(bad), n))
    print("identical questions: %d" % len(dups))
    for w, msg in bad:
        print("  BAD %s %s" % (w, msg))
    for q, w in dups.items():
        print("  DUPLICATE %s: %s" % (", ".join(w), q[:80]))
    return not bad and not dups


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sources", required=False, help="cache dir from fetch_sources.sh")
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--write-totals", action="store_true", help="rewrite the coverage.md Totals table")
    ap.add_argument("checks", nargs="*", default=[])
    a = ap.parse_args()
    checks = a.checks or ["code", "citations", "steps", "coverage", "eval"]
    src = None
    if any(c in ("code", "citations") for c in checks):
        if not a.sources:
            ap.error("--sources is required for the code and citations checks")
        src = Sources(a.sources)
    ok = True
    for c in checks:
        if c == "code":
            ok &= check_code(src, a.verbose)
        elif c == "citations":
            ok &= check_citations(src, a.verbose)
        elif c == "steps":
            ok &= check_steps(a.verbose)
        elif c == "coverage":
            ok &= check_coverage(a.verbose, a.write_totals)
        elif c == "eval":
            ok &= check_eval(a.verbose)
        else:
            ap.error("unknown check: " + c)
        print()
    print("RESULT: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
