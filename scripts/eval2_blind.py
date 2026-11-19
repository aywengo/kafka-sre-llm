#!/usr/bin/env python3
"""Blind and unblind the readings of tier B answers (agents/m4-evaluation.md, "Wrong, invented and harmful: where the rates come from").

  eval2_blind.py bundle --items I --rubrics R --answers default=A1.jsonl strict=A2.jsonl --chunks work/v3/chunks_s1.jsonl \
      --out BUNDLE --key KEY.json --seed 20261119 [--only ID ...] [--second-reader 0.25 --second-out BUNDLE2]
  eval2_blind.py unblind --key KEY.json --rubrics R --answers default=A1.jsonl strict=A2.jsonl \
      --findings r1=F1.jsonl [r2=F2.jsonl] [--scope r2=second] --out FINDINGS.jsonl [--reads-out READS.jsonl] [--allow-rejects]

bundle: for a blind reader. Per item one file BUNDLE/<id>.md (question, evidence, the rubric's must and must-not points, the answers
between "===== ANSWER <name> BEGIN/END =====" lines) and BUNDLE/excerpts/<name>.md (the excerpts the answer's own pack gave the model).
The answers have neutral names <id>-A, <id>-B, ... in an order drawn per item from the seed (the same item, set list and seed always give
the same order). Nothing of the configuration is written into the bundle: no set name, mode, limit, timing or model name (the answer
text and the question are the only places where such words can appear, because they are the item's own text). BUNDLE/README.md
holds the reading protocol and the format of a findings row. The KEY (answer name -> set) is written to a separate path, which must
not lie inside a bundle folder. --only limits the bundle to items by id. --second-reader FRACTION draws that share of the items (at
least FRACTION of them, whole items, stratified by item kind, by the seed) into a second folder --second-out, with the SAME answer names
as the first, so that the findings of the two readers can be matched; the key lists those items ("second_reader_ids").

unblind: takes the readers' findings files (NAME=PATH, one name per reader), validates every row and writes the merged findings with the
set and the reader (the reader's name) in place of the neutral answer name, plus the "read" markers the coverage report needs
(--reads-out: {"set", "id", "reader", "kind": "read"} for every answer the reader had in the bundle, answers with no finding included).
A reader's scope is every item of the key ("first", the default) or only the second reader's items (--scope NAME=second).
Findings row, as in runs/m4/S5B_FINDINGS.jsonl before unblinding:
  {"answer": "p2-01-A", "id": "p2-01", "kind": "wrong"|"invented"|"harmful"|"omission", "quote": verbatim from the answer (omission: "")
   "point": rubric point id of an omission ("m2"; "m3|m4" for an any_of group), "supported": bool (omission), "minor": bool, "source": file
   and lines, "note": text}
Validation (rows that fail are listed with the reason and written to OUT.rejected.jsonl, never dropped silently; without --allow-rejects
nothing is written to OUT and the exit code is 1): the answer name is in the key and its item is the row's id; the answer is in the
reader's scope; the kind is valid; "minor" and "supported" are booleans when present; an omission row names a point of the item's rubric
(several ids only when they are exactly one any_of group); any other row has a quote that occurs in the answer after whitespace is
normalised. Duplicate rows (same reader, answer, kind, quote, point) are listed as rejects too.
"""
import argparse
import hashlib
import json
import math
import random
import re
import sys
from collections import OrderedDict, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402  (item order helpers of the smoke subset)
import eval2_judge as ej  # noqa: E402
from eval2_pilot_report import split_point_ids  # noqa: E402

KINDS = ("wrong", "invented", "harmful", "omission")
LETTERS = "ABCDEFGH"


def rd(path):
    return [json.loads(l) for l in Path(path).read_text(encoding="utf-8").splitlines() if l.strip()]


def norm_ws(s):
    return re.sub(r"\s+", " ", s or "").strip()


def parse_named(specs, what):
    out = OrderedDict()
    for s in specs:
        name, sep, path = s.partition("=")
        if not sep or not name or not path:
            raise SystemExit(f"{what} {s!r}: expected NAME=PATH")
        if name in out:
            raise SystemExit(f"{what}: {name!r} given twice")
        out[name] = path
    return out


# ---------------------------------------------------------------- bundle

def order_for(item_id, sets, seed):
    """The order of the sets for one item: a permutation drawn from (seed, item id)."""
    names = list(sets)
    random.Random(f"{seed}:{item_id}").shuffle(names)
    return names


def neutral_names(item_id, sets, seed):
    """{answer name: set}, in the order A, B, ..."""
    return OrderedDict((f"{item_id}-{LETTERS[i]}", s) for i, s in enumerate(order_for(item_id, sets, seed)))


def pick_second(items, fraction, seed):
    """Whole items, at least `fraction` of all, stratified by kind (largest remainder), within a kind by a hash of (seed, id)."""
    n = len(items)
    total = min(n, max(1, math.ceil(fraction * n - 1e-9)))
    by_kind = defaultdict(list)
    for it in items:
        by_kind[it.get("kind") or ""].append(it["id"])
    alloc = eval2._allocate({k: len(v) for k, v in by_kind.items()}, total)
    ids = []
    for k, v in by_kind.items():
        ids += sorted(v, key=lambda i: eval2._order_key(seed, i))[: alloc[k]]
    return sorted(ids)


def render_points(rub):
    L = ["## Points of the rubric", "", "### Must points (id, weight)", ""]
    for p in rub["must"]:
        first = ", must come first" if p.get("first") else ""
        L.append(f"- **{p['id']}** (weight {p.get('weight')}{first}): {p['text']}")
        if p.get("source"):
            L.append(f"  - source: {p['source']}")
        if p.get("chunks"):
            L.append(f"  - index chunks that hold it: {', '.join(p['chunks'])}")
        elif p.get("not_indexed"):
            L.append("  - rests on a source outside the index")
    groups = ej.groups_of(rub)
    if groups:
        L += ["", "### Equally correct alternatives (any_of groups; one of the members is enough, name the whole group as `m3|m4` in an omission)", ""]
        order = [p["id"] for p in rub["must"]]
        for g in groups:
            L.append("- " + " | ".join(i for i in order if i in g))
    L += ["", "### Must-not points (id, severity)", ""]
    for p in rub.get("must_not") or []:
        L.append(f"- **{p['id']}** ({p.get('severity')}): {p['text']}")
        if p.get("condition"):
            L.append(f"  - applies when: {p['condition']}")
        if p.get("near_miss"):
            L.append(f"  - a correct near-miss that must not trigger it: {p['near_miss']}")
    if not rub.get("must_not"):
        L.append("(none)")
    return L


README = """# Reading protocol

You read answers of an assistant for Kafka, Strimzi, Kubernetes and Argo CD operators. You do not know how any answer was made, and you
must not try to find out. Read each answer on its own.

Per item one file `<id>.md` (question, evidence, the rubric's points, the answers) and one file per answer in `excerpts/` (the passages the
assistant was given for that answer).

1. Write your own findings first, before you look at anything from a judge or from another reader.
2. A finding has a kind: `wrong`, `invented`, `harmful` or `omission`. A statement the sources do not settle is not counted; list it as
   "could not verify" in your notes, not as a finding.
   - `omission`: a must point (or an any_of group, as `m3|m4`) that the answer does not state. Give the point id in `point` and set
     `supported`: true if the passage that holds the fact is among the answer's excerpts, otherwise false.
   - the other kinds: give a verbatim `quote` from the answer, the source file and lines in `source`, and a short `note`.
   - `minor`: true for a small side error that would not mislead an engineer.
3. One row per distinct statement.
4. Findings file: JSON Lines, one row per finding:
   `{"answer": "<id>-A", "id": "<id>", "kind": "wrong", "quote": "...", "point": "", "supported": false, "minor": false, "source": "...", "note": "..."}`
   (`point` and `supported` are used for omissions; `quote` is empty for an omission). Answers without a finding need no row.
"""


def cmd_bundle(a):
    items = [r for r in rd(a.items)]
    rub = {r["id"]: r for r in rd(a.rubrics)}
    sets = parse_named(a.answers, "--answers")
    if len(sets) < 2:
        raise SystemExit("--answers: two or more answer sets are needed")
    answers = {s: {r["id"]: r for r in rd(p)} for s, p in sets.items()}
    chunks = {c["id"]: c for c in rd(a.chunks)}
    if a.only:
        unknown = [i for i in a.only if i not in {it["id"] for it in items}]
        if unknown:
            raise SystemExit(f"--only: no such item: {unknown}")
        items = [it for it in items if it["id"] in set(a.only)]
    if not items:
        raise SystemExit("no items")
    for it in items:
        if it["id"] not in rub:
            raise SystemExit(f"item {it['id']} has no rubric")
        for s in sets:
            r = answers[s].get(it["id"])
            if r is None or r.get("error") or not (r.get("text") or "").strip():
                raise SystemExit(f"set {s}: no usable answer for item {it['id']}")
    out = Path(a.out)
    second_ids = None
    if a.second_reader is not None:
        if not a.second_out:
            raise SystemExit("--second-reader needs --second-out")
        if not 0 < a.second_reader <= 1:
            raise SystemExit("--second-reader: a fraction between 0 and 1")
        second_ids = pick_second(items, a.second_reader, a.seed)
    folders = [out] + ([Path(a.second_out)] if second_ids is not None else [])
    key_path = Path(a.key).resolve()
    for f in folders:
        fr = f.resolve()
        if fr == key_path.parent or fr in key_path.parents:
            raise SystemExit(f"the key {a.key} must not lie inside the bundle folder {f}")
    for f in folders:
        if f.exists() and any(f.iterdir()):
            raise SystemExit(f"{f} exists and is not empty; not overwriting")
    key = OrderedDict()
    for it in items:
        names = neutral_names(it["id"], sets, a.seed)
        key.update(names)
        L = [f"# Item {it['id']}", "", "## Question", "", it["question"], ""]
        if it.get("evidence"):
            L += ["## Evidence pasted by the user", "", "```text", it["evidence"], "```", ""]
        L += render_points(rub[it["id"]])
        if rub[it["id"]].get("reference"):
            L += ["", "## Reference answer (for the reader, not for a score)", "", rub[it["id"]]["reference"]]
        L += ["", "## Answers", "", "Excerpts of each answer: `excerpts/<answer name>.md`.", ""]
        for name, s in names.items():
            L += [f"===== ANSWER {name} BEGIN =====", answers[s][it["id"]]["text"].rstrip("\n"), f"===== ANSWER {name} END =====", ""]
        body = "\n".join(L) + "\n"
        exc = {}
        for name, s in names.items():
            pack = answers[s][it["id"]].get("pack") or []
            parts = []
            for cid in pack:
                c = chunks.get(cid)
                if c is None:
                    raise SystemExit(f"answer {name}: chunk {cid} is not in {a.chunks}")
                head = f"----- chunk {cid} | {c.get('path') or c.get('source') or ''}" + (f" | {c['title']}" if c.get("title") else "") + " -----"
                parts.append(head + "\n" + c["text"].rstrip("\n"))
            exc[name] = f"# Excerpts of answer {name}\n\n" + ("\n\n".join(parts) if parts else "(none)") + "\n"
        for f in folders:
            if f is not out and it["id"] not in second_ids:
                continue
            (f / "excerpts").mkdir(parents=True, exist_ok=True)
            (f / f"{it['id']}.md").write_text(body, encoding="utf-8")
            for name, txt in exc.items():
                (f / "excerpts" / f"{name}.md").write_text(txt, encoding="utf-8")
    for f in folders:
        (f / "README.md").write_text(README, encoding="utf-8")
    meta = {"seed": a.seed, "sets": list(sets), "items": [it["id"] for it in items], "second_reader_ids": second_ids,
            "aid": key}
    Path(a.key).parent.mkdir(parents=True, exist_ok=True)
    Path(a.key).write_text(json.dumps(meta, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"bundle {out}: {len(items)} items, {len(key)} answers ({len(sets)} per item); key {a.key}")
    if second_ids is not None:
        print(f"second bundle {a.second_out}: {len(second_ids)} of {len(items)} items "
              f"({100 * len(second_ids) / len(items):.0f} %; by kind {dict(sorted(_kind_counts(items, second_ids).items()))})")
    return 0


def _kind_counts(items, ids):
    c = defaultdict(int)
    for it in items:
        if it["id"] in ids:
            c[it.get("kind") or ""] += 1
    return c


# ---------------------------------------------------------------- unblind

def validate_row(row, key, rub, text_of, scope_ids):
    """-> None if the row is valid, else the reason."""
    name = row.get("answer")
    if name not in key["aid"]:
        return f"answer name {name!r} is not in the key"
    item = name.rsplit("-", 1)[0]
    if row.get("id") != item:
        return f"id {row.get('id')!r} is not the item of answer {name}"
    if item not in scope_ids:
        return f"answer {name} is not in this reader's bundle"
    kind = row.get("kind")
    if kind not in KINDS:
        return f"kind {kind!r} is not one of {', '.join(KINDS)}"
    for f in ("minor", "supported"):
        if f in row and not isinstance(row[f], bool):
            return f"{f} must be true or false"
    if kind == "omission":
        if "supported" not in row:
            return "an omission row needs supported (true or false)"
        ids = split_point_ids(row.get("point"))
        if not ids:
            return "an omission row names no point"
        have = {p["id"] for p in rub[item]["must"]}
        missing = [i for i in ids if i not in have]
        if missing:
            return f"point {missing[0]!r} is not a must point of {item}"
        if len(set(ids)) > 1 and set(ids) not in ej.groups_of(rub[item]):
            return f"points {'|'.join(ids)} are not one any_of group"
        return None
    q = norm_ws(row.get("quote"))
    if not q:
        return "no quote"
    if q not in norm_ws(text_of[name]):
        return "quote not found in the answer (after whitespace is normalised)"
    return None


def cmd_unblind(a):
    key = json.loads(Path(a.key).read_text(encoding="utf-8"))
    rub = {r["id"]: r for r in rd(a.rubrics)}
    sets = parse_named(a.answers, "--answers")
    texts = {s: {r["id"]: r.get("text") or "" for r in rd(p)} for s, p in sets.items()}
    text_of = {}
    for name, s in key["aid"].items():
        if s not in texts:
            raise SystemExit(f"the key names set {s!r}, which is not in --answers")
        item = name.rsplit("-", 1)[0]
        if item not in texts[s]:
            raise SystemExit(f"set {s} has no answer for item {item}")
        text_of[name] = texts[s][item]
    readers = parse_named(a.findings, "--findings")
    scopes = {}
    for spec in a.scope or []:
        n, sep, v = spec.partition("=")
        if n not in readers or v not in ("first", "second"):
            raise SystemExit(f"--scope {spec!r}: expected NAME=first|second with NAME one of {list(readers)}")
        scopes[n] = v
    if "second" in scopes.values() and not key.get("second_reader_ids"):
        raise SystemExit("the key has no second reader's items")
    scope_ids = {r: set(key["items"] if scopes.get(r, "first") == "first" else key["second_reader_ids"]) for r in readers}
    merged, rejects, seen = [], [], set()
    for reader, path in readers.items():
        for n, row in enumerate(rd(path), 1):
            why = validate_row(row, key, rub, text_of, scope_ids[reader])
            if why is None:
                sig = (reader, row["answer"], row["kind"], norm_ws(row.get("quote")), row.get("point") or "")
                if sig in seen:
                    why = "duplicate of an earlier row of this reader"
                seen.add(sig)
            if why is not None:
                rejects.append({"reader": reader, "file": str(path), "line": n, "reason": why, "row": row})
                continue
            out = {"set": key["aid"][row["answer"]], "id": row["id"], "reader": reader, "kind": row["kind"],
                   "quote": row.get("quote") or "", "point": row.get("point") or "", "supported": row.get("supported"),
                   "minor": row.get("minor", False), "source": row.get("source") or "", "note": row.get("note") or "",
                   "answer_blind": row["answer"]}
            if row["kind"] != "omission":
                out.pop("supported")
                out.pop("point")
            merged.append(out)
    for rj in rejects:
        print(f"REJECTED {rj['reader']} {rj['file']}:{rj['line']} ({rj['row'].get('answer')}, {rj['row'].get('kind')}): {rj['reason']}")
    by_reader = {r: sum(1 for m in merged if m["reader"] == r) for r in readers}
    print(f"{len(merged)} rows valid {by_reader}, {len(rejects)} rejected")
    outp = Path(a.out)
    if rejects:
        rp = outp.with_name(outp.name + ".rejected.jsonl")
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rejects), encoding="utf-8")
        print(f"rejected rows written to {rp}")
        if not a.allow_rejects:
            print("nothing written to the merged file; fix the rows, or use --allow-rejects to write the valid ones", file=sys.stderr)
            return 1
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text("".join(json.dumps(m, ensure_ascii=False) + "\n" for m in merged), encoding="utf-8")
    print(f"wrote {outp}")
    if a.reads_out:
        marks = [{"set": s, "id": name.rsplit("-", 1)[0], "reader": r, "kind": "read"}
                 for r in readers for name, s in key["aid"].items() if name.rsplit("-", 1)[0] in scope_ids[r]]
        Path(a.reads_out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.reads_out).write_text("".join(json.dumps(m) + "\n" for m in marks), encoding="utf-8")
        print(f"wrote {a.reads_out}: {len(marks)} read markers " + str({r: sum(1 for m in marks if m['reader'] == r) for r in readers}))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bundle")
    b.add_argument("--items", required=True)
    b.add_argument("--rubrics", required=True)
    b.add_argument("--answers", nargs="+", required=True, metavar="NAME=PATH")
    b.add_argument("--chunks", default="work/v3/chunks_s1.jsonl")
    b.add_argument("--out", required=True)
    b.add_argument("--key", required=True, help="where the key is written (outside every bundle folder)")
    b.add_argument("--seed", type=int, required=True)
    b.add_argument("--only", nargs="+", metavar="ID")
    b.add_argument("--second-reader", type=float, metavar="FRACTION")
    b.add_argument("--second-out")
    u = sub.add_parser("unblind")
    u.add_argument("--key", required=True)
    u.add_argument("--rubrics", required=True)
    u.add_argument("--answers", nargs="+", required=True, metavar="NAME=PATH")
    u.add_argument("--findings", nargs="+", required=True, metavar="READER=PATH")
    u.add_argument("--scope", nargs="*", metavar="READER=first|second")
    u.add_argument("--out", required=True)
    u.add_argument("--reads-out")
    u.add_argument("--allow-rejects", action="store_true")
    a = ap.parse_args(argv)
    return cmd_bundle(a) if a.cmd == "bundle" else cmd_unblind(a)


if __name__ == "__main__":
    sys.exit(main())
