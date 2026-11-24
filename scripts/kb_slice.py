#!/usr/bin/env python3
"""M7 KB phase 2, I1: the slice of the frozen index the fact entries are written from (runs/m7/reports/kb2_design.md section 1).

Rule: every chunk of the frozen index (runs/v3/INDEX_FROZEN.json: file and sha256) whose source is strimzi or apache-kafka,
except paths under raw/strimzi/documentation/contributing/; index order. The slice file holds one JSON object per line,
{"id": ...}, in that order; its sha256 goes into the record. CPU only, no model.

  .venv/bin/python scripts/kb_slice.py --out work/m7/kb/slice.jsonl [--expect 1487]
  .venv/bin/python scripts/kb_slice.py --check work/m7/kb/slice.jsonl     # the file reproduces from the rule

Prints one line: the count, the number of files and the sha256. Exit 1 when --expect or --check fails; 2 when an input is
missing or the index's hash differs from the record. An existing --out is never overwritten (an equal file is accepted).
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from kafka_sre_rag.kb_entry import in_slice  # noqa: E402

FROZEN_RECORD = "runs/v3/INDEX_FROZEN.json"
EXPECT = 1487


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def load_index(record=FROZEN_RECORD, chunks=None):
    """The frozen index's chunks (list of dicts, index order) after its sha256 is checked against the record."""
    with open(record, encoding="utf-8") as f:
        rec = json.load(f)
    path = chunks or rec["index_path"]
    got = sha256_file(path)
    if got != rec["sha256"]:
        raise SystemExit(f"REFUSED: {path} has sha256 {got[:12]}..., the record {record} says {rec['sha256'][:12]}...")
    with open(path, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()], rec


def slice_ids(chunks):
    return [c["id"] for c in chunks if in_slice(c)]


def slice_bytes(ids):
    return "".join(json.dumps({"id": i}) + "\n" for i in ids).encode("utf-8")


def load_slice(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(l)["id"] for l in f if l.strip()]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--record", default=FROZEN_RECORD)
    ap.add_argument("--chunks", default=None, help="the index file (default: the record's index_path)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--out")
    g.add_argument("--check")
    ap.add_argument("--expect", type=int, default=None, help=f"the expected count (the design's: {EXPECT})")
    a = ap.parse_args(argv)
    for p in (a.record, a.chunks):
        if p and not Path(p).is_file():
            print(f"REFUSED: input {p} is missing", file=sys.stderr)
            return 2
    try:
        chunks, _ = load_index(a.record, a.chunks)
    except SystemExit as e:
        print(e, file=sys.stderr)
        return 2
    ids = slice_ids(chunks)
    data = slice_bytes(ids)
    sha = hashlib.sha256(data).hexdigest()
    files = len({c["path"] for c in chunks if in_slice(c)})
    if a.check:
        if not Path(a.check).is_file():
            print(f"REFUSED: {a.check} is missing", file=sys.stderr)
            return 2
        if Path(a.check).read_bytes() != data:
            print(f"FAILED: {a.check} does not reproduce from the rule (file {sha256_file(a.check)[:12]}, rule {sha[:12]})", file=sys.stderr)
            return 1
    else:
        out = Path(a.out)
        if out.exists():
            if out.read_bytes() != data:
                print(f"REFUSED: {out} exists and differs from the rule's slice; it is not overwritten", file=sys.stderr)
                return 2
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            tmp = out.with_name(out.name + ".tmp")
            tmp.write_bytes(data)
            tmp.replace(out)
    print(f"slice: {len(ids)} chunks, {files} files, sha256 {sha}")
    if a.expect is not None and len(ids) != a.expect:
        print(f"FAILED: {len(ids)} chunks, expected {a.expect}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
