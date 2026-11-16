#!/usr/bin/env python3
"""Judge-free check for invented code-like identifiers in answers written from a pack.

Extracts identifiers an operator would type (flags, dotted config keys, shell tools,
metric names, CamelCase kinds/fields/exceptions, annotations, HTTP endpoints) and checks
each against (a) the full text of the answer's pack and (b) the whole chunk index.
Deterministic, CPU-only, stdlib only.

Matching rules (all case-insensitive, on text with brackets "[...]" removed from tokens):
  R1  the token appears as a whole identifier (not glued to [A-Za-z0-9_-] on either side;
      a trailing .sh on shell tools is optional on both sides; CamelCase tokens may carry a
      plural "s"/"es").
  R2  (path-like dotted tokens only: they contain an uppercase letter or start with
      spec./status./metadata./template.) every dotted segment appears somewhere as a whole
      identifier. Lower-case config keys (max.poll.interval.ms) need R1 only.
  R3  (endpoints) "METHOD /a/{x}/b": the method, then within 60 chars on the same line the
      path, where {x}, <x> and :x segments match any one segment.
A token is in_pack if R1/R2/R3 hold on the pack text, in_corpus if they hold on the index.
Limits: bare paths without an HTTP method, short flags (-n) and plain words are not
extracted; YAML nested keys are only checked through their camelCase/dotted spellings.
"""
import argparse
import hashlib
import json
import re
import sys
import time

VERSION = "invention_count/1"

# ---------------------------------------------------------------- frozen constants
URL_RE = re.compile(r"https?://\S+")
PLACEHOLDER_RE = re.compile(r"<[^<>\n]{0,60}>")
FENCE_RE = re.compile(r"```.*?```", re.S)
INLINE_CODE_RE = re.compile(r"`[^`\n]+`")

ENDPOINT_RE = re.compile(
    r"\b(GET|POST|PUT|DELETE|PATCH)\s+(/[A-Za-z0-9_{}<>:.\-]+(?:/[A-Za-z0-9_{}<>:.\-]+)*)")
ANNOTATION_RE = re.compile(
    r"(?<![\w./-])[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)*\.(?:io|dev|com|org|net)/[A-Za-z0-9._/-]*[A-Za-z0-9_-]")
FLAG_RE = re.compile(r"(?<![\w-])--[a-z][a-z0-9]*(?:[-.][a-z0-9]+)*")
SHELL_RE = re.compile(r"(?<![\w./-])(?:kafka-[a-z0-9]+(?:-[a-z0-9]+)*|[a-z][a-z0-9]*(?:-[a-z0-9]+)+\.sh|[a-z][a-z0-9]*\.sh)(?![\w-])")
DOTTED_RE = re.compile(
    r"(?<![\w./\-])[A-Za-z][A-Za-z0-9_-]+(?:\[[^\]\s]*\])?(?:\.[a-z0-9][A-Za-z0-9_-]+(?:\[[^\]\s]*\])?)+(?![\w-])")
CAMEL_RE = re.compile(r"(?<![\w.-])(?:[A-Z][a-z0-9]+){2,}(?![\w-])|(?<![\w.-])[a-z]+(?:[A-Z][a-z0-9]+)+(?![\w-])")
SNAKE_RE = re.compile(r"(?<![\w.-])[a-z][a-z0-9]*(?:_[a-z0-9]+)+(?![\w-])")
SNAKE_MIN_UNDERSCORES_PLAIN = 2   # outside backticks/fences; inside code one underscore is enough

# lower-case; compared after normalisation (brackets removed, trailing .sh removed)
ALLOWLIST = frozenset("""
kubectl yaml json apiversion kind metadata spec name namespace bootstrap-server
--bootstrap-server --namespace --output --filename --help --version --all-namespaces
--dry-run --selector --context --kubeconfig --follow --previous --tail --watch
localhost gitops zookeeper openshift github pagerduty
""".split())
ALLOW_PATTERNS = (
    re.compile(r"^(my|your|example|foo|bar|test)[-_.]"),        # placeholder-style names
    re.compile(r"^[A-Z0-9]+(_[A-Z0-9]+)*$"),                    # ALL_CAPS_WITH_UNDERSCORES
    re.compile(r"^v?\d[\d.\-]*$"),                              # version numbers
)
FILE_EXTENSIONS = frozenset("""
yaml yml json md txt sh properties conf cfg log xml html htm py js go java jar csv ini toml
tar gz zip tgz crt pem jks p12 sql
""".split())
DOMAIN_SUFFIXES = frozenset("local svc com org io net dev example internal cluster".split())
PATHLIKE_PREFIXES = ("spec.", "status.", "metadata.", "template.")
ENDPOINT_METHOD_WINDOW = 60


def constants_hash():
    blob = json.dumps({
        "regex": [r.pattern for r in (URL_RE, PLACEHOLDER_RE, FENCE_RE, INLINE_CODE_RE, ENDPOINT_RE,
                                      ANNOTATION_RE, FLAG_RE, SHELL_RE, DOTTED_RE, CAMEL_RE, SNAKE_RE)],
        "snake_min": SNAKE_MIN_UNDERSCORES_PLAIN,
        "allow": sorted(ALLOWLIST),
        "allow_patterns": [p.pattern for p in ALLOW_PATTERNS],
        "ext": sorted(FILE_EXTENSIONS), "dom": sorted(DOMAIN_SUFFIXES),
        "pathlike": PATHLIKE_PREFIXES, "window": ENDPOINT_METHOD_WINDOW,
    }, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:12]


# ---------------------------------------------------------------- extraction
def _mask(text, m):
    return text[:m.start()] + " " * (m.end() - m.start()) + text[m.end():]


def normalise(tok, kind):
    t = re.sub(r"\[[^\]]*\]", "", tok)
    if kind == "shell" and t.endswith(".sh"):
        t = t[:-3]
    return t


def _allowed(raw, norm):
    low = norm.lower()
    if low in ALLOWLIST or raw.lower() in ALLOWLIST:
        return True
    return any(p.match(norm) for p in ALLOW_PATTERNS)


def extract(text):
    """Return a list of (kind, token) in order of first appearance, distinct."""
    code_spans = [(m.start(), m.end()) for r in (FENCE_RE, INLINE_CODE_RE) for m in r.finditer(text)]

    def in_code(pos):
        return any(a <= pos < b for a, b in code_spans)

    work = text
    for m in URL_RE.finditer(text):
        work = _mask(work, m)
    for m in PLACEHOLDER_RE.finditer(text):
        work = _mask(work, m)
    found = []

    def run(regex, kind, handler=None):
        nonlocal work
        for m in list(regex.finditer(work)):
            item = handler(m) if handler else m.group(0)
            if item is not None:
                found.append((m.start(), kind, item))
            work = _mask(work, m)

    def endpoint(m):
        path = re.sub(r"\.+$", "", m.group(2))
        return f"{m.group(1)} {path}"

    def dotted(m):
        segs = re.sub(r"\[[^\]]*\]", "", m.group(0)).split(".")
        if segs[-1].lower() in FILE_EXTENSIONS or segs[-1].lower() in DOMAIN_SUFFIXES:
            return None
        if any(re.fullmatch(r"[\d-]+", s) for s in segs):
            return None
        return m.group(0)

    def snake(m):
        n = m.group(0).count("_")
        if n >= SNAKE_MIN_UNDERSCORES_PLAIN or (n >= 1 and in_code(m.start())):
            return m.group(0)
        return None

    run(ENDPOINT_RE, "endpoint", endpoint)
    def annotation(m):
        # group/version strings such as kafka.strimzi.io/v1beta2 are apiVersion values
        if re.fullmatch(r"v\d+((alpha|beta)\d+)?", m.group(0).rsplit("/", 1)[-1]):
            return None
        return m.group(0)

    run(ANNOTATION_RE, "annotation", annotation)
    run(FLAG_RE, "flag")
    run(SHELL_RE, "shell")
    run(DOTTED_RE, "dotted", dotted)
    run(CAMEL_RE, "camel")
    run(SNAKE_RE, "snake", snake)

    out, seen = [], set()
    for _, kind, tok in sorted(found):
        norm = normalise(tok, kind)
        if _allowed(tok, norm):
            continue
        key = (kind, norm.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append((kind, norm))
    return out


# ---------------------------------------------------------------- matching
_IDCHAR = set("abcdefghijklmnopqrstuvwxyz0123456789_-")


def _contains(hay, needle, trailing=()):
    """Whole-identifier substring search on lower-cased text."""
    start = 0
    n = len(needle)
    while True:
        i = hay.find(needle, start)
        if i < 0:
            return False
        before_ok = i == 0 or hay[i - 1] not in _IDCHAR
        j = i + n
        for suf in ("",) + tuple(trailing):
            k = j + len(suf)
            if hay[j:k] == suf and (k >= len(hay) or hay[k] not in _IDCHAR):
                if before_ok:
                    return True
        start = i + 1


def _endpoint_regex(tok):
    method, path = tok.split(" ", 1)
    parts = []
    for seg in path.strip("/").split("/"):
        if re.fullmatch(r"\{[^}]*\}|<[^>]*>|:[\w-]+", seg):
            parts.append(r"[^/\s]+")
        else:
            parts.append(re.escape(seg.lower()))
    body = "/" + "/".join(parts)
    return re.compile(r"\b" + method.lower() + r"\b[^\n]{0," + str(ENDPOINT_METHOD_WINDOW) + r"}?" + body
                      + r"(?![\w-])")


def matches(hay, kind, tok):
    """hay is lower-cased text."""
    low = tok.lower()
    if kind == "endpoint":
        return bool(_endpoint_regex(tok).search(hay))
    if kind == "shell":
        return _contains(hay, low, trailing=(".sh",))
    trailing = ("s", "es") if kind == "camel" else ()
    if _contains(hay, low, trailing):
        return True
    if kind == "dotted" and (low.startswith(PATHLIKE_PREFIXES) or tok != low):
        return all(_contains(hay, seg) for seg in low.split("."))
    return False


# ---------------------------------------------------------------- data loading
def load_index(path):
    chunks = {}
    with open(path) as f:
        for line in f:
            if line.strip():
                d = json.loads(line)
                chunks[d["id"]] = ((d.get("title") or "") + "\n" + (d.get("text") or "")).lower()
    return chunks


def load_packs(path, k):
    """Return {question: [chunk ids]}."""
    with open(path) as f:
        raw = f.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = [json.loads(l) for l in raw.splitlines() if l.strip()]
    out = {}
    for d in data:
        key = (d.get("question") or d.get("id") or "").strip()
        ids = d.get(k)
        if ids is None:
            raise SystemExit(f"pack row for {key[:40]!r} has no field {k!r}")
        out[key] = ids
    return out


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def check_answer(text, pack_text, corpus_text):
    rows = []
    for kind, tok in extract(text):
        in_pack = matches(pack_text, kind, tok)
        in_corpus = in_pack or matches(corpus_text, kind, tok)
        rows.append({"kind": kind, "token": tok, "in_pack": in_pack, "in_corpus": in_corpus})
    return rows


def run(answers, packs, chunks, corpus_text, labels=None):
    results = []
    missing_ids = 0
    for i, a in enumerate(answers):
        q = a["question"].strip()
        if q not in packs:
            raise SystemExit(f"no pack for question: {q[:60]!r}")
        parts = []
        for cid in packs[q]:
            if cid in chunks:
                parts.append(chunks[cid])
            else:
                missing_ids += 1
        rows = check_answer(a.get("text") or "", "\n".join(parts), corpus_text)
        results.append({
            "label": labels[i] if labels else q[:60],
            "question": q,
            "n_tokens": len(rows),
            "absent_pack": [r["token"] for r in rows if not r["in_pack"]],
            "absent_corpus": [r["token"] for r in rows if not r["in_corpus"]],
            "tokens": rows,
        })
    return results, missing_ids


def report(results, out=sys.stdout):
    n = len(results)
    ap = [r for r in results if r["absent_pack"]]
    ac = [r for r in results if r["absent_corpus"]]
    dp = {t.lower() for r in results for t in r["absent_pack"]}
    dc = {t.lower() for r in results for t in r["absent_corpus"]}
    w = lambda s: print(s, file=out)
    w(f"answers: {n}")
    w(f"answers with >=1 token absent from pack: {len(ap)}")
    w(f"answers with >=1 token absent from corpus: {len(ac)}")
    w(f"distinct tokens absent from pack: {len(dp)}")
    w(f"distinct tokens absent from corpus: {len(dc)}")
    w(f"mean absent-from-pack per answer: {sum(len(r['absent_pack']) for r in results) / max(n, 1):.3f}")
    w(f"mean absent-from-corpus per answer: {sum(len(r['absent_corpus']) for r in results) / max(n, 1):.3f}")
    w("per answer (label: absent from pack | of those, absent from corpus):")
    for r in results:
        if r["absent_pack"]:
            w(f"  {r['label']}: {r['absent_pack']} | {r['absent_corpus']}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--answers", required=True)
    ap.add_argument("--packs", required=True)
    ap.add_argument("--index", required=True, help="chunk index JSONL the packs were built on "
                    "(work/v2/chunks.jsonl for work/v3/rag_packs_base_exp.json)")
    ap.add_argument("--k", default="k6")
    ap.add_argument("--questions", help="JSONL in eval order (question numbers = 1-based positions)")
    ap.add_argument("--only-questions", help="comma-separated 1-based positions; needs --questions")
    ap.add_argument("--json", help="write per-answer detail here")
    args = ap.parse_args(argv)

    t0 = time.time()
    print(f"{VERSION} constants_hash={constants_hash()}")
    chunks = load_index(args.index)
    corpus_text = "\n".join(chunks.values())
    packs = load_packs(args.packs, args.k)
    answers = load_jsonl(args.answers)
    labels = None
    if args.questions:
        order = [d["question"].strip() for d in load_jsonl(args.questions)]
        pos = {q: i + 1 for i, q in enumerate(order)}
        keep = None
        if args.only_questions:
            keep = {int(x) for x in args.only_questions.split(",")}
        sel, labels = [], []
        for a in answers:
            p = pos.get(a["question"].strip())
            if p is None:
                raise SystemExit(f"answer question not in --questions: {a['question'][:60]!r}")
            if keep is None or p in keep:
                sel.append(a)
                labels.append(f"Q{p}")
        answers = sel
    elif args.only_questions:
        raise SystemExit("--only-questions needs --questions")
    results, missing = run(answers, packs, chunks, corpus_text, labels)
    if missing:
        print(f"warning: {missing} pack chunk ids not in index", file=sys.stderr)
    report(results)
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"version": VERSION, "constants_hash": constants_hash(), "k": args.k,
                       "answers": results}, f, indent=1)
    print(f"seconds: {time.time() - t0:.1f}", file=sys.stderr)


if __name__ == "__main__":
    main()
