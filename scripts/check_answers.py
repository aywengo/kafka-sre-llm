#!/usr/bin/env python3
"""Run the answer checker (kafka_sre_rag.checker) over a file of answers.

  .venv/bin/python scripts/check_answers.py --answers work/v3/ans_rag_real_s1.jsonl \\
      --packs work/v3/packs_real_s1.jsonl --index work/v3/chunks_s1.jsonl --out runs/m4/check_real.json

The answers file is JSONL (or a JSON array): one row per answer with `text` and an `id` or a `question`.
With --packs and --index the excerpts of each answer are rebuilt (the chunk ids of the pack's --k list, read from
the index) and an item found in them is `in_excerpts`. Without them every item is judged by the catalogues alone.
Packs are matched to answers by `id`, else by `question`; a pack file is a JSON array or JSONL, as written by
scripts/build_packs_v3.py and scripts/rag_answers.py.

Prints one line per answer and writes a JSON report (counts, findings, the note for the user; items with --items).
CPU only: reads files, loads no model, calls no server. Stdlib only.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from kafka_sre_rag import checker  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues  # noqa: E402

VERSION = "check_answers/1"


def read_rows(path):
    text = Path(path).read_text()
    if text.lstrip().startswith("["):
        return json.loads(text)
    return [json.loads(l) for l in text.splitlines() if l.strip()]


def pack_lookup(packs_path, k):
    """{id or question: [chunk ids]} from a packs file."""
    out = {}
    for p in read_rows(packs_path):
        ids = p.get(k)
        if ids is None:
            continue
        if p.get("id"):
            out[p["id"]] = ids
        if p.get("question"):
            out[p["question"]] = ids
    return out


def index_texts(index_path, wanted):
    """{chunk id: text} for the ids in `wanted`, streamed from the chunk index (JSONL)."""
    out = {}
    with open(index_path) as f:
        for line in f:
            if not line.strip():
                continue
            c = json.loads(line)
            if c["id"] in wanted:
                out[c["id"]] = c["text"]
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--answers", required=True)
    ap.add_argument("--packs")
    ap.add_argument("--index")
    ap.add_argument("--k", default="k6", help="which pack list holds the excerpts (default k6)")
    ap.add_argument("--catalogues", help="directory of catalogues (default work/m4/catalogues)")
    ap.add_argument("--out", help="write the JSON report here (a new path; an existing file is not overwritten)")
    ap.add_argument("--items", action="store_true", help="include every extracted item in the report")
    ap.add_argument("--note", action="store_true", help="print the note for the user under each answer")
    ap.add_argument("--max-unverified", type=int, default=5)
    args = ap.parse_args(argv)
    if bool(args.packs) != bool(args.index):
        ap.error("--packs and --index go together")
    if args.out and Path(args.out).exists():
        ap.error("%s exists; write to a new path" % args.out)

    answers = read_rows(args.answers)
    packs = pack_lookup(args.packs, args.k) if args.packs else {}
    chunks = {}
    if packs:
        wanted = {i for ids in packs.values() for i in ids}
        chunks = index_texts(args.index, wanted)
    ck = checker.Checker(Catalogues.load(args.catalogues) if args.catalogues else None)

    report, totals = [], {"invented": 0, "wrong_use": 0, "unverified": 0, "items": 0}
    for n, row in enumerate(answers, 1):
        key = row.get("id") or row.get("question") or str(n)
        ids = packs.get(row.get("id")) or packs.get(row.get("question")) if packs else None
        excerpts = [chunks[i] for i in ids if i in chunks] if ids else None
        ck._unchecked_now = {}
        res = ck.check(row["text"], excerpts)
        res.unchecked_kinds = dict(ck._unchecked_now)
        c = res.counts()
        for k in totals:
            totals[k] += c[k]
        label = (row.get("question") or key)[:48].replace("\n", " ")
        print("%3d  %-48s  items %3d  invented %d  wrong_use %d  unverified %d%s" % (
            n, label, c["items"], c["invented"], c["wrong_use"], c["unverified"],
            "" if excerpts is not None else "  (no excerpts)"))
        note = checker.render_note(res, args.max_unverified)
        if args.note and note:
            print("     " + note.replace("\n", "\n     "))
        d = res.to_dict()
        if not args.items:
            d.pop("items")
        report.append({"id": row.get("id"), "question": row.get("question"), "excerpts": excerpts is not None,
                       "note": note, **d})
    print("total: %d answers, %d items, invented %d, wrong_use %d, unverified %d" % (
        len(answers), totals["items"], totals["invented"], totals["wrong_use"], totals["unverified"]))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps({"script": VERSION, "checker": checker.VERSION, "answers_file": args.answers,
                                              "totals": totals, "answers": report}, indent=1) + "\n")
        print("wrote", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
