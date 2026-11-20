#!/usr/bin/env python3
"""Build the WP5 development needs file: which chunk of the frozen index a retrieval has to deliver.

Rows (JSON lines, every row has "set": "dev"):
  header  first line: says that these items are a development set for WP5 (see HEADER_NOTE)
  scenario one per pilot scenario: all chunks the rubric marks as covering (covered_chunks)
  point   one per must point of a pilot scenario (eval/m4/pilot2): the rubric's covered_chunks that lie in a file
          the point cites; status mapped | not_indexed | no_source | unmapped
  fact    one per open tier-A item (eval/m4/open/tier_a.jsonl) whose fact sits in one locatable chunk of the index:
          the chunk titled with the key (or holding it as an inline "## key" heading), the chunk with the error row,
          the REST chunk titled with the endpoint, or the only chunk that contains the CLI option (a1-wc_e-002 only)

Reads only. CPU only, no model. Usage:
    .venv/bin/python scripts/retrieval_needs.py [--out eval/m4/retrieval_needs_dev.jsonl] [--ids-file FILE]
"""
import argparse
import collections
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX = "work/v3/chunks_s1.jsonl"
FROZEN = "runs/v3/INDEX_FROZEN.json"
PILOT_ITEMS = "eval/m4/pilot2/items.jsonl"
PILOT_RUBRICS = "eval/m4/pilot2/rubrics.jsonl"
TIER_A = "eval/m4/open/tier_a.jsonl"
OUT = "eval/m4/retrieval_needs_dev.jsonl"
C1 = ("a1-err_desc_a-001 a1-ns_default_a-007 a1-ns_default_b-001 a1-ns_default_b-004 a1-ns_type_b-002 "
      "a1-ns_values_a-008 a1-ns_values_b-003 a1-vt_update_b-009 a1-vt_default_a-008 a1-wc_e-002").split()

# Items whose fact is a CLI option: only this one is taken (the plan, section 2). The same rule applied to every
# kafka_cli item would add a1-real_flag-001, a1-wc_b-006 and a1-wc_c-001 (see the report).
OPTION_FACTS = {"a1-wc_e-002": "--max-messages"}

HEADER_NOTE = ("Development set for WP5 (retrieval and index coverage). The 24 pilot scenarios and the open tier-A "
               "items listed here were used to choose retrieval variants, so any change they prompt must list them "
               "and be judged on items outside this file (fresh batch B1, hidden part). Not a gate, not evidence of "
               "improvement. Built by scripts/retrieval_needs.py.")


def rd(path):
    p = Path(path)
    p = p if p.is_absolute() else ROOT / p
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def sha256(path):
    p = Path(path)
    p = p if p.is_absolute() else ROOT / p
    return hashlib.sha256(p.read_bytes()).hexdigest()


def check_index_hash(index=INDEX):
    want = json.loads((ROOT / FROZEN).read_text())["sha256"]
    got = sha256(index)
    if got != want:
        raise SystemExit(f"index {index} sha256 {got} differs from {FROZEN} ({want})")
    return got


def classify_points(rubrics, byid):
    """Rows for the must points. The covering chunks of a point are the rubric's covered_chunks that lie in a file the
    point's source cites."""
    rows = []
    for r in rubrics:
        cc = r["covered_chunks"]
        for m in r["must"]:
            src = m["source"]
            files = re.findall(r"((?:raw|raw_catalogue|data)/[^\s:;,]+)", src)
            cov = [c for c in cc if c in byid and byid[c]["path"] in files]
            if cov:
                status = "mapped"
            elif (any(f.startswith("raw/argo-cd/docs/user-guide/commands") for f in files)
                  or (files and all(f.startswith("raw_catalogue") for f in files))
                  or "TransactionsCommand" in src):
                status = "not_indexed"
            elif not files or "not covered" in src or "inferred" in src or "no occurrence" in src:
                status = "no_source"
            else:
                status = "unmapped"
            rows.append({"row_type": "point", "set": "dev", "item": r["id"], "point": m["id"],
                         "weight": m["weight"], "status": status, "chunks": cov, "files": files})
    return rows


def fact_chunks(it, chunks):
    """(chunk ids, status) of the single fact an open tier-A item needs; ids None when the item has no such fact."""
    if it["id"] in OPTION_FACTS:
        # the fact is a CLI option in a source file outside the index; the plan takes the one chunk that holds it
        opt = OPTION_FACTS[it["id"]]
        hits = [c["id"] for c in chunks if re.search(re.escape(opt) + r"(?![\w-])", c["text"])]
        return (hits, "located") if len(hits) == 1 else (None, "not_unique")
    s = it["sources"][0]
    if "absent" in s or not s.get("entry"):
        return None, "no_positive_fact"
    f = s.get("file") or ""
    cat = s.get("catalogue")
    if cat == "kafka_config" and f.startswith("raw/"):
        if "/" not in s["entry"]:      # an entry not of the form scope/key (the a3 set): no single locatable fact
            return None, "unresolved"
        key = s["entry"].split("/", 1)[1]
        hits = [c["id"] for c in chunks if c["path"] == f and (
            c["title"] == key or ("## " + key + "\n") in c["text"] or c["text"].rstrip().endswith("## " + key))]
    elif cat == "protocol_errors" and f.startswith("raw/"):
        hits = [c["id"] for c in chunks if c["path"] == f
                and re.search(r"(^|\W)" + re.escape(s["entry"]) + r"(\W|$)", c["text"])]
    elif cat == "connect_rest" and f.startswith("raw/"):
        hits = [c["id"] for c in chunks if c["path"] == f and c["title"] == s["entry"]]
    else:
        return None, "not_indexed" if not f.startswith("raw/") or "/commands/" in f else "other_kind"
    return (hits, "located") if hits else (None, "unresolved")


def build(index=INDEX, ids_file=None, tier_a_files=None):
    """tier_a_files: open tier A files (default: eval/m4/open/tier_a.jsonl). Items tagged `not_in_index` (the a3 set
    marks items whose facts are not in the index) get a fact row with status not_in_index and no chunks, reported
    apart by the funnel; every other item is handled as before."""
    check_index_hash(index)
    chunks = rd(index)
    byid = {c["id"]: c for c in chunks}
    rubrics = rd(PILOT_RUBRICS)
    rows = classify_points(rubrics, byid)
    rows += [{"row_type": "scenario", "set": "dev", "item": r["id"], "status": "mapped",
              "chunks": [c for c in r["covered_chunks"] if c in byid]} for r in rubrics]
    kinds = collections.Counter()
    facts = []
    tagged = []
    for it in [it for f in (tier_a_files or [TIER_A]) for it in rd(f)]:
        if it.get("not_in_index") is True:
            kinds["not_in_index_tagged"] += 1
            tagged.append(it["id"])
            facts.append({"row_type": "fact", "set": "dev", "item": it["id"], "item_kind": it["kind"],
                          "catalogue": None, "entry": None, "status": "not_in_index", "chunks": [],
                          "tags": ["not_in_index"]})
            continue
        ids, st = fact_chunks(it, chunks)
        kinds[st] += 1
        if ids is None:
            continue
        facts.append({"row_type": "fact", "set": "dev", "item": it["id"], "item_kind": it["kind"],
                      "catalogue": it["sources"][0]["catalogue"], "entry": it["sources"][0]["entry"],
                      "status": "mapped", "chunks": ids, "tags": ["C1"] if it["id"] in C1 else []})
    rows += facts
    info = {"scenarios": sum(1 for r in rows if r["row_type"] == "scenario"),
            "covering_chunk_refs": sum(len(r["chunks"]) for r in rows if r["row_type"] == "scenario"),
            "points": collections.Counter(r["status"] for r in rows if r["row_type"] == "point"),
            "facts": sum(1 for r in facts if r["status"] == "mapped"), "fact_scan": dict(kinds),
            "multi_chunk_facts": [r["item"] for r in facts if len(r["chunks"]) > 1],
            "c1_present": sorted(set(C1) & {r["item"] for r in facts})}
    if tagged:
        info["not_in_index_tagged"] = len(tagged)
    if ids_file and Path(ids_file).exists():
        want = set(Path(ids_file).read_text().split())
        got = {r["item"] for r in facts if r["status"] == "mapped"}
        info["vs_ids_file"] = {"in_file": len(want), "missing_from_build": sorted(want - got),
                               "extra_in_build": sorted(got - want)}
    return rows, info


def write(rows, out):
    p = Path(out)
    p = p if p.is_absolute() else ROOT / p
    head = {"row_type": "header", "set": "dev", "note": HEADER_NOTE, "index_sha256": check_index_hash(),
            "rows": len(rows)}
    with open(p, "w") as f:
        for r in [head] + rows:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=False) + "\n")


B1_ITEMS = "eval/m4/b1/items.jsonl"
B1_RUBRICS = "eval/m4/b1/rubrics.jsonl"
B1_OUT = "work/m5/b1_needs.jsonl"
B1_HEADER_NOTE = ("Needs of batch B1 (eval/m4/b1): one row per must point with the chunk ids its rubric names, its "
                  "weight, the rubric's any_of group (list of point ids, or null) and not_indexed. Built by "
                  "scripts/retrieval_needs.py --b1.")


def build_b1(index=INDEX, items=B1_ITEMS, rubrics=B1_RUBRICS):
    """Point rows of the B1 rubrics. status is mapped (has chunk ids) or not_indexed; the chunk ids are kept as
    written in the rubric (those absent from the index would show as not_in_index in the funnel)."""
    check_index_hash(index)
    kind = {i["id"]: i["kind"] for i in rd(items)}
    rows = []
    for r in rd(rubrics):
        for m in r["must"]:
            group = next((g for g in r["any_of"] if m["id"] in g), None)
            rows.append({"row_type": "point", "set": "b1", "item": r["id"], "item_kind": kind[r["id"]],
                         "point": m["id"], "weight": m["weight"],
                         "status": "not_indexed" if m.get("not_indexed") else "mapped",
                         "chunks": list(m["chunks"]), "any_of": group, "not_indexed": bool(m.get("not_indexed"))})
    return rows


def write_b1(rows, out=B1_OUT):
    p = Path(out)
    p = p if p.is_absolute() else ROOT / p
    if p.exists():
        raise SystemExit(f"{p} exists; not overwritten")
    p.parent.mkdir(parents=True, exist_ok=True)
    head = {"row_type": "header", "set": "b1", "note": B1_HEADER_NOTE, "index_sha256": check_index_hash(),
            "rows": len(rows)}
    with open(p, "w") as f:
        for r in [head] + rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def load_needs(path):
    """Rows of a needs file without the header."""
    return [r for r in rd(path) if r.get("row_type") != "header"]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help=f"default {OUT}, or {B1_OUT} with --b1")
    ap.add_argument("--b1", action="store_true", help="build the B1 needs file instead of the development one")
    ap.add_argument("--ids-file", default="runs/m4/reports/wp5_dev_tierA_ids.txt")
    ap.add_argument("--tier-a", nargs="+", default=[TIER_A], metavar="FILE",
                    help=f"open tier A file(s), for example {TIER_A} eval/m4/open/tier_a3.jsonl (default: {TIER_A})")
    a = ap.parse_args(argv)
    if a.b1:
        rows = build_b1()
        write_b1(rows, a.out or B1_OUT)
        print(f"wrote {len(rows)} rows (+ header) to {a.out or B1_OUT}")
        return
    a.out = a.out or OUT
    rows, info = build(INDEX, a.ids_file, a.tier_a)
    write(rows, a.out)
    print(json.dumps(info, indent=1, default=dict))
    print(f"wrote {len(rows)} rows (+ header) to {a.out}")


if __name__ == "__main__":
    sys.exit(main())
