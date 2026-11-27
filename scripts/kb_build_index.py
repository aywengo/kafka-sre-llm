#!/usr/bin/env python3
"""M7 KB phase 2, I4: write the tracked record kafka_sre_rag/kb_entries.json for the chain's entries index.

Input: the chain's accepted entries (work/m7/kb/entries_index.jsonl: sorted by id, each row with its rendered text), the slice file
(work/m7/kb/slice.jsonl) and the chain's MANIFEST.json (the writer and checker models and the two prompt versions). Output: the record
(file, sha256, entry count, slice sha256, writer and checker models and prompt versions, created). The runtime refuses an index whose
hash differs from the record. CPU only, no model, no server.

  python scripts/kb_build_index.py --manifest W/MANIFEST.json [--index ...] [--slice ...] [--record ...] [--created ISO] [--force]

Refused (exit 2, nothing written): a row that is out of order, repeated, not as the manifest says (prompt version, writer), without
its fields, or whose rendered text is not what kb_entry.render_text gives now; a record that exists and differs (without --force).
The recorded file name is relative to the repository root. The record is not committed until the real index exists.
"""
import argparse
import datetime
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import kb_entries, kb_entry  # noqa: E402

REQUIRED = ("id", "kind", "title", "statement", "scope", "sources", "quotes", "prompt_version", "writer", "rendered")


class Refused(Exception):
    pass


def check_rows(rows, manifest):
    """Problems of the index rows against the manifest; raises Refused with the first few."""
    bad = []
    want_v = manifest.get("entry_prompt_version")
    want_w = (manifest.get("options") or {}).get("writer")
    if not want_v or not want_w:
        raise Refused("the manifest has no entry_prompt_version or options.writer")
    if not rows:
        raise Refused("the entries index is empty")
    ids = [r.get("id") for r in rows]
    if ids != sorted(ids):
        bad.append("rows are not sorted by id")
    if len(set(ids)) != len(ids):
        bad.append("an id is repeated")
    for r in rows:
        miss = [k for k in REQUIRED if k not in r]
        if miss:
            bad.append(f"{r.get('id')}: missing {', '.join(miss)}")
            continue
        if r["prompt_version"] != want_v:
            bad.append(f"{r['id']}: prompt_version {r['prompt_version']!r}, the manifest says {want_v!r}")
        if r["writer"] != want_w:
            bad.append(f"{r['id']}: writer {r['writer']!r}, the manifest says {want_w!r}")
    if not bad:
        try:
            kb_entries.check_rendered(rows)
        except kb_entries.KbEntriesError as e:
            bad.append(str(e))
    if bad:
        raise Refused("; ".join(bad[:5]) + (f" (and {len(bad) - 5} more)" if len(bad) > 5 else ""))


def build(index, slice_file, manifest_file, *, file_name=None, created=None):
    index, slice_file = Path(index), Path(slice_file)
    if not index.is_file() or not slice_file.is_file() or not Path(manifest_file).is_file():
        raise Refused("the index, the slice file and the manifest must all exist")
    manifest = json.loads(Path(manifest_file).read_text(encoding="utf-8"))
    opts = manifest.get("options") or {}
    if not manifest.get("check_prompt_version") or not opts.get("checker"):
        raise Refused("the manifest has no check_prompt_version or options.checker")
    rows = kb_entries.rows_of(index)
    check_rows(rows, manifest)
    if file_name is None:
        try:
            file_name = str(index.resolve().relative_to(ROOT))
        except ValueError:
            raise Refused("the index is outside the repository: pass --file-name (the path to record, relative to the repository root)")
    if Path(file_name).is_absolute():
        raise Refused("the recorded file name must be relative to the repository root")
    return {"schema": kb_entries.SCHEMA, "file": file_name, "sha256": kb_entries.sha256_file(index), "entries": len(rows),
            "slice_sha256": kb_entries.sha256_file(slice_file),
            "writer": {"model": opts["writer"], "prompt_version": manifest["entry_prompt_version"]},
            "checker": {"model": opts["checker"], "prompt_version": manifest["check_prompt_version"]},
            "created": created or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--index", default="work/m7/kb/entries_index.jsonl")
    ap.add_argument("--slice", default="work/m7/kb/slice.jsonl")
    ap.add_argument("--manifest", required=True, help="the chain's MANIFEST.json (writer, checker, prompt versions)")
    ap.add_argument("--record", default=str(kb_entries.RECORD))
    ap.add_argument("--file-name", default=None, help="the path to record for the index (default: relative to the repository root)")
    ap.add_argument("--created", default=None, help="ISO time to record (default: now, UTC)")
    ap.add_argument("--force", action="store_true", help="replace a record that exists and differs")
    a = ap.parse_args(argv)
    try:
        rec = build(a.index, a.slice, a.manifest, file_name=a.file_name, created=a.created)
    except (Refused, ValueError, OSError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    out = Path(a.record)
    text = json.dumps(rec, indent=1, sort_keys=True) + "\n"
    if out.exists():
        old = json.loads(out.read_text(encoding="utf-8"))
        if {k: v for k, v in old.items() if k != "created"} == {k: v for k, v in rec.items() if k != "created"}:
            print(f"record unchanged: {out.name} ({rec['entries']} entries, sha256 {rec['sha256']})")
            return 0
        if not a.force:
            print(f"REFUSED: {out.name} exists and differs (the index or the manifest changed); pass --force to replace it", file=sys.stderr)
            return 2
    tmp = Path(str(out) + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(out)
    print(f"record written: {out.name}: {rec['entries']} entries, sha256 {rec['sha256']}, slice sha256 {rec['slice_sha256']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
