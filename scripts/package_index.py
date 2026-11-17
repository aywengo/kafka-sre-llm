#!/usr/bin/env python3
"""Assemble the release directory for the frozen documentation index (CPU only, no network).

Reads the frozen stage-1 index (path and sha256 from runs/v3/INDEX_FROZEN.json), refuses to run
if its hash differs, checks that no chunk comes from a held-out or an excluded runbook, and writes

    <out-dir>/chunks.jsonl        the index under a stable name
    <out-dir>/SHA256SUMS          sha256 of every other file in the directory
    <out-dir>/frozen.json         copy of kafka_sre_rag/frozen.json
    <out-dir>/NOTICE              copy of the NOTICE at the repository root
    <out-dir>/notices/<name>/...  upstream NOTICE files found in raw/<name>/
    <out-dir>/README.md           dataset card generated from the template below

Usage: .venv/bin/python scripts/package_index.py [--out-dir out/kafka-sre-docs-index-v3]
       [--headline TEXT] [--size TEXT] [--measured TEXT]   (the re-measured figure, when known)
"""
import argparse
import fnmatch
import hashlib
import json
import shutil
import sys
from collections import Counter, OrderedDict
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
LOCAL_PREFIX = "data/runbooks/runbooks/"
PLACEHOLDER = "to be filled from SHIP_CHECKS.md"


class PackageError(RuntimeError):
    pass


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def check_hash(path, expected):
    """Raise PackageError unless the file's sha256 is the frozen one."""
    got = sha256_file(path)
    if got != expected:
        raise PackageError(f"{path}: sha256 {got} differs from the frozen {expected}; refusing to package")
    return got


def load_chunks(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def local_excludes(cfg):
    """Exclude patterns of the local runbooks source in sources.yaml."""
    out = []
    for s in cfg["sources"]:
        if s.get("local", "").rstrip("/").endswith("data/runbooks/runbooks"):
            out += s.get("exclude", [])
    return out


def forbidden_chunks(chunks, holdout, excludes):
    """Ids of chunks that come from a held-out runbook (relative names, one per line in holdout.txt)
    or match an exclude pattern of the runbooks source (the new runbooks awaiting review)."""
    hold = {h.strip() for h in holdout if h.strip()}
    bad = []
    for c in chunks:
        p = c.get("path", "").replace("\\", "/")
        if LOCAL_PREFIX not in p:
            continue
        rel = p.split(LOCAL_PREFIX, 1)[1]
        if rel in hold or any(fnmatch.fnmatch("/" + rel, pat) for pat in excludes):
            bad.append(c.get("id"))
    return bad


def check_forbidden(chunks, holdout, excludes):
    bad = forbidden_chunks(chunks, holdout, excludes)
    if bad:
        raise PackageError(f"{len(bad)} chunk(s) come from a held-out or excluded runbook, e.g. {bad[:3]}")


def source_table(chunks, sources, commits):
    """Markdown table: source, licence, chunks, pinned ref. `sources` is the list from sources.yaml.
    Order follows sources.yaml; a source with no chunk is left out; a chunk of an unknown source is an error."""
    counts = Counter(c["source"] for c in chunks)
    lic = {}
    for c in chunks:
        lic.setdefault(c["source"], c.get("license"))
    known = {s["name"] for s in sources}
    unknown = set(counts) - known
    if unknown:
        raise PackageError(f"chunks from sources not in sources.yaml: {sorted(unknown)}")
    rows = ["| Source | Licence | Chunks | Pinned ref |", "|---|---|---|---|"]
    for s in sources:
        n = s["name"]
        if not counts[n]:
            continue
        if "repo" in s:
            ref, commit = str(s["ref"]), commits.get(n, "")
            pin = f"`{commit}`" if ref == commit else f"`{ref}` (`{commit}`)"
        else:
            pin = "this repository (`data/runbooks/runbooks/`)"
        rows.append(f"| {n} | {lic[n] or s.get('license')} | {counts[n]} | {pin} |")
    rows.append(f"| **total** | | **{sum(counts.values())}** | |")
    return "\n".join(rows)


README_TEMPLATE = """---
license: other
license_name: mixed-apache-2.0-and-cc-by-4.0
license_link: NOTICE
licenses_included:
  - apache-2.0
  - cc-by-4.0
language:
  - en
pretty_name: Kafka SRE documentation index v3
size_categories:
  - 1K<n<10K
tags:
  - kafka
  - strimzi
  - kubernetes
  - argo-cd
  - retrieval
---

# Kafka SRE documentation index (v3)

`chunks.jsonl` is the retrieval index of the kafka-sre-llm runtime: {chunks} chunks of documentation
for Apache Kafka, Strimzi, Kubernetes, Argo CD, two Schema Registry implementations (Apicurio Registry
and Karapace), cAdvisor and one Prometheus alert runbook, plus {local_chunks} chunks from this
project's own runbooks. One JSON object per line with `id`, `source`, `license`, `path`, `title`, `text`.
This is the index that the runtime was frozen on; its sha256 is `{sha}`.

## Licence

This is a mixed-licence collection, not a single-licence file. The chunks of the Kubernetes website
documentation ({k8s_chunks} chunks) are under CC BY 4.0 and need the attribution given in `NOTICE`
(title, "The Kubernetes Authors", source, licence link, changes made). All other chunks are under the
Apache License 2.0 (column `license` in every chunk, table below). `NOTICE` has the copyright lines,
the pins and the statement of changes; upstream NOTICE files are in `notices/`. The text was converted
to plain text, cleaned and split into chunks; it is not the upstream documents and is not endorsed by
the upstream projects.

## Sources

{table}

## How it was built

From the repository root, with the pins in `sources.yaml` (no source follows a moving branch):

```
.venv/bin/python scripts/fetch_sources.py
.venv/bin/python scripts/chunk_docs.py --out work/v3/chunks_s1.jsonl
shasum -a 256 work/v3/chunks_s1.jsonl     # {sha}
```

The runbooks excluded in `sources.yaml` (the eight held-out runbooks and two new runbooks awaiting
review) are not in the index; the packaging script checks this. Rebuilding from the same checkouts
gives the same file (identical sha256 on two runs, checked 2026-11-17).

## How to use it

The runtime `python -m kafka_sre_rag` reads the index from the path in `kafka_sre_rag/frozen.json`
(`{index_path}`, relative to the repository root). Copy the file there:

```
mkdir -p work/v3 && cp chunks.jsonl work/v3/chunks_s1.jsonl
.venv/bin/python -m kafka_sre_rag ask --dry-run "Why is my KafkaRebalance stuck in PendingProposal?"
```

`--dry-run` prints the prompt with the retrieved excerpts and does not load a model. The default mode is
`default`; use `--mode strict` when the commands in the answer will be pasted into a cluster. Retrieval
settings (BM25, reranker `{reranker}` at a pinned revision, pool, k) are in `frozen.json`.

## How to check it

```
.venv/bin/python -m kafka_sre_rag verify
shasum -a 256 -c SHA256SUMS      # inside this directory
```

`verify` recomputes the index sha256 and the prompt hashes against `frozen.json`.

## Measured result, size and limits

- Base model with retrieval scores +1.19 (mean judge-score difference) over the plain base model on 40
  real questions, two judge passes. This was measured on an earlier index of 6,292 chunks.
- The figure for this index ({chunks} chunks): {measured}. Headline: {headline}. Size: {size}.
- Limits: 40 questions, standard error about 0.2, so differences under 0.2 are noise; the question set
  was used many times for decisions. The result is for the unmodified base model with retrieval. The
  fine-tuned adapters add nothing measurable. The index holds documentation at the pins above and goes
  out of date; answers can still be wrong or invent flags, so check commands before running them.
"""


def render_readme(chunks, table, cfg, measured=None, headline=None, size=None):
    counts = Counter(c["source"] for c in chunks)
    idx = cfg["index"]
    return README_TEMPLATE.format(
        chunks=len(chunks), sha=idx["sha256"], table=table,
        local_chunks=counts.get("runbooks", 0), k8s_chunks=counts.get("kubernetes", 0),
        index_path=idx["index_path"], reranker=cfg["retrieval"]["reranker"]["model"],
        measured=measured or PLACEHOLDER, headline=headline or PLACEHOLDER, size=size or PLACEHOLDER)


def write_sums(out_dir):
    files = sorted(p for p in Path(out_dir).rglob("*") if p.is_file() and p.name != "SHA256SUMS")
    lines = [f"{sha256_file(p)}  {p.relative_to(out_dir).as_posix()}" for p in files]
    (Path(out_dir) / "SHA256SUMS").write_text("\n".join(lines) + "\n")
    return lines


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out-dir", default="out/kafka-sre-docs-index-v3")
    ap.add_argument("--root", default=str(ROOT), help="repository root (default: this repository)")
    ap.add_argument("--measured", help="the re-measured result for this index (text)")
    ap.add_argument("--headline", help="the headline number for this index (text)")
    ap.add_argument("--size", help="its size: questions and passes (text)")
    a = ap.parse_args(argv)
    root = Path(a.root)
    out = Path(a.out_dir)
    if not out.is_absolute():
        out = root / out

    frozen = json.loads((root / "runs/v3/INDEX_FROZEN.json").read_text())
    cfg = json.loads((root / "kafka_sre_rag/frozen.json").read_text())
    if cfg["index"]["sha256"] != frozen["sha256"]:
        raise PackageError("frozen.json and INDEX_FROZEN.json disagree on the index hash")
    src = root / frozen["index_path"]
    check_hash(src, frozen["sha256"])
    chunks = load_chunks(src)
    if len(chunks) != frozen["chunks"]:
        raise PackageError(f"{len(chunks)} chunks, frozen {frozen['chunks']}")

    sources_cfg = yaml.safe_load((root / "sources.yaml").read_text())
    holdout = (root / "data/runbooks/holdout.txt").read_text().splitlines()
    check_forbidden(chunks, holdout, local_excludes(sources_cfg))
    if dict(Counter(c["source"] for c in chunks)) != frozen["chunks_per_source"]:
        raise PackageError("chunks per source differ from INDEX_FROZEN.json")

    table = source_table(chunks, sources_cfg["sources"], frozen["source_commits"])
    notice = root / "NOTICE"
    if not notice.exists():
        raise PackageError("NOTICE is missing at the repository root")

    out.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, out / "chunks.jsonl")
    shutil.copyfile(root / "kafka_sre_rag/frozen.json", out / "frozen.json")
    shutil.copyfile(notice, out / "NOTICE")
    for s in sources_cfg["sources"]:
        d = root / "raw" / s["name"]
        for p in sorted(d.glob("NOTICE")) if d.is_dir() else []:   # NOTICE only, not NOTICE-binary
            (out / "notices" / s["name"]).mkdir(parents=True, exist_ok=True)
            shutil.copyfile(p, out / "notices" / s["name"] / p.name)
    (out / "README.md").write_text(render_readme(chunks, table, cfg, a.measured, a.headline, a.size))
    lines = write_sums(out)
    total = sum(p.stat().st_size for p in out.rglob("*") if p.is_file())
    print(f"wrote {out}: {len(lines)} files + SHA256SUMS, {total / 1e6:.1f} MB")
    for line in lines:
        print("  " + line.split("  ", 1)[1])
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PackageError as e:
        print(f"package_index: {e}", file=sys.stderr)
        sys.exit(1)
