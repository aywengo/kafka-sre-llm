#!/usr/bin/env python3
"""Assemble the upload folder for the fact-entries dataset (CPU only, no network, nothing is uploaded).

Reads the entries index and its record, refuses to run unless they agree, checks that every entry
comes from an Apache-2.0 source of sources.yaml, and writes

    <out-dir>/entries_index.jsonl   the entries index under the name the installer expects
    <out-dir>/kb_entries.json       the record of the index
    <out-dir>/README.md             the dataset card (model_cards/kafka-sre-fact-entries-v4.md)
    <out-dir>/NOTICE                the notice (model_cards/NOTICE.kafka-sre-fact-entries-v4)
    <out-dir>/LICENSE               copy of the Apache-2.0 LICENSE at the repository root
    <out-dir>/notices/<name>/NOTICE upstream NOTICE files found in raw/<name>/ for the sources present
    <out-dir>/MANIFEST.json         sha256 and size of every other file

The card and the notice are drafts with open markers; the build prints them. --final refuses while any is left.

Usage: .venv/bin/python scripts/package_entries.py [--out-dir out/kafka-sre-fact-entries-v4] [--final]
       [--data-root DIR]   where the git-ignored work/m7/kb/entries_index.jsonl and raw/ live, if not --root
"""
import argparse
import hashlib
import json
import re
import shutil
import sys
from collections import Counter
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
HF_NAMESPACE = "aywengo"
HF_DATASET = "kafka-sre-fact-entries-v4"
CARD = f"model_cards/{HF_DATASET}.md"
NOTICE = f"model_cards/NOTICE.{HF_DATASET}"
OPEN_MARKERS = ("PLACEHOLDER", "DRAFT", "OPEN FOR THE OWNER", "[Open point")
# Nothing of the owner's machines may be in a hand-written file of the package: home paths, keys, server addresses.
LEAKS = (re.compile(r"/Users/|/home/|~/\.config"),
         re.compile(r"\b(?:sk|hf|ghp)_[A-Za-z0-9]{16,}|\bsk-[A-Za-z0-9]{16,}"),
         re.compile(r"OMLX_(?:URL|API_KEY)|JUDGE_API_KEY|https?://(?:localhost|127\.|192\.168\.|10\.)"))
IPV4 = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")


class PackageError(RuntimeError):
    pass


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def load_entries(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def check_record(index_path, record, entries):
    """The record, the file and the entries must agree: hash, count, prompt version, writer, unique ids."""
    got = sha256_file(index_path)
    if record.get("sha256") != got:
        raise PackageError(f"{index_path}: sha256 {got} differs from the record's {record.get('sha256')}; refusing to package")
    if record.get("entries") != len(entries):
        raise PackageError(f"{len(entries)} entries, the record says {record.get('entries')}")
    pv, wr = record["writer"]["prompt_version"], record["writer"]["model"]
    odd = [e.get("id") for e in entries if e.get("prompt_version") != pv or e.get("writer") != wr]
    if odd:
        raise PackageError(f"{len(odd)} entries differ from the record's writer {wr} / {pv}, e.g. {odd[:3]}")
    ids = Counter(e.get("id") for e in entries)
    dup = [i for i, n in ids.items() if n > 1]
    if dup:
        raise PackageError(f"duplicate entry ids, e.g. {dup[:3]}")
    return got


def check_sources(entries, sources):
    """Every entry source must be a documentation source of sources.yaml under Apache-2.0 (the card's licence)."""
    lic = {s["name"]: s.get("license") for s in sources}
    used = Counter()
    for e in entries:
        for s in e.get("sources", []):
            n = s.get("source")
            if n not in lic:
                raise PackageError(f"entry {e.get('id')}: source {n!r} is not in sources.yaml")
            if lic[n] != "Apache-2.0":
                raise PackageError(f"entry {e.get('id')}: source {n!r} is {lic[n]}, not Apache-2.0; not packaged")
        used[e["sources"][0]["source"]] += 1
    return used


def scan_text(name, text):
    """Raise on a home path, a key or a server address in a hand-written file (card, notice)."""
    for rx in LEAKS:
        m = rx.search(text)
        if m:
            raise PackageError(f"{name}: contains {m.group(0)!r}; refusing to package")
    ips = sorted(set(IPV4.findall(text)))
    if ips:
        raise PackageError(f"{name}: contains an address {ips[:3]}; refusing to package")


def open_markers(text):
    return [m for m in OPEN_MARKERS if m in text]


def check_card(card_text, notice_text, sha, record_sha, n):
    """The hand-written card and notice must state the hashes and the count of what is packaged."""
    for name, t in (("card", card_text), ("notice", notice_text)):
        scan_text(name, t)
    for what, val in (("index sha256", sha), ("record sha256", record_sha)):
        if val not in card_text:
            raise PackageError(f"the card does not state the {what} {val}")
    for name, t in (("card", card_text), ("notice", notice_text)):
        if not re.search(r"(?<![\w,.])" + re.escape(f"{n:,}") + r"(?![\w,])", t):
            raise PackageError(f"the {name} does not state the count {n:,}")


def write_manifest(out_dir, meta):
    out_dir = Path(out_dir)
    files = sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name != "MANIFEST.json")
    listing = [{"path": p.relative_to(out_dir).as_posix(), "sha256": sha256_file(p), "size": p.stat().st_size}
               for p in files]
    (out_dir / "MANIFEST.json").write_text(json.dumps({**meta, "files": listing}, indent=1) + "\n")
    return listing


def build(root, out, index_path, record_path, data_root=None, final=False):
    """Write the upload folder. Returns (file listing, open markers). Nothing is uploaded."""
    root, out = Path(root), Path(out)
    data_root = Path(data_root) if data_root else root
    record = json.loads(Path(record_path).read_text(encoding="utf-8"))
    entries = load_entries(index_path)
    sha = check_record(index_path, record, entries)
    sources_cfg = yaml.safe_load((root / "sources.yaml").read_text())
    used = check_sources(entries, sources_cfg["sources"])
    card_p, notice_p, license_p = root / CARD, root / NOTICE, root / "LICENSE"
    for p in (card_p, notice_p, license_p):
        if not p.exists():
            raise PackageError(f"{p} is missing")
    card_text, notice_text = card_p.read_text(encoding="utf-8"), notice_p.read_text(encoding="utf-8")
    rec_sha = sha256_file(record_path)
    check_card(card_text, notice_text, sha, rec_sha, len(entries))
    markers = sorted(set(open_markers(card_text) + open_markers(notice_text)))
    if final and markers:
        raise PackageError(f"--final: the card or the notice still has open markers {markers}")

    out.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(index_path, out / "entries_index.jsonl")
    shutil.copyfile(record_path, out / "kb_entries.json")
    shutil.copyfile(card_p, out / "README.md")
    shutil.copyfile(notice_p, out / "NOTICE")
    shutil.copyfile(license_p, out / "LICENSE")
    for name in sorted(used):
        d = data_root / "raw" / name
        for p in sorted(d.glob("NOTICE")) if d.is_dir() else []:   # NOTICE only, not NOTICE-binary
            (out / "notices" / name).mkdir(parents=True, exist_ok=True)
            shutil.copyfile(p, out / "notices" / name / p.name)
    meta = {"repo_id": f"{HF_NAMESPACE}/{HF_DATASET}", "repo_type": "dataset", "entries": len(entries),
            "entries_sha256": sha, "record_sha256": rec_sha, "entries_per_source": dict(used),
            "open_markers": markers}
    return write_manifest(out, meta), markers


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out-dir", default=f"out/{HF_DATASET}")
    ap.add_argument("--root", default=str(ROOT), help="repository root (default: this repository)")
    ap.add_argument("--data-root", default=None, help="where the git-ignored index and raw/ live (default: --root)")
    ap.add_argument("--entries-index", default="work/m7/kb/entries_index.jsonl", help="relative to --data-root")
    ap.add_argument("--record", default="kafka_sre_rag/kb_entries.json", help="relative to --root")
    ap.add_argument("--final", action="store_true", help="refuse while the card or notice has open markers")
    a = ap.parse_args(argv)
    root = Path(a.root)
    data = Path(a.data_root) if a.data_root else root
    out = Path(a.out_dir)
    if not out.is_absolute():
        out = root / out
    idx, rec = Path(a.entries_index), Path(a.record)
    idx = idx if idx.is_absolute() else data / idx
    rec = rec if rec.is_absolute() else root / rec
    listing, markers = build(root, out, idx, rec, data, a.final)
    total = sum(f["size"] for f in listing)
    print(f"wrote {out}: {len(listing)} files + MANIFEST.json, {total / 1e6:.2f} MB")
    for f in listing:
        print(f"  {f['sha256']}  {f['size']:>10}  {f['path']}")
    if markers:
        print(f"open markers still in the card or notice (draft, not for upload): {', '.join(markers)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PackageError as e:
        print(f"package_entries: {e}", file=sys.stderr)
        sys.exit(1)
