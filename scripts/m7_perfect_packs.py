#!/usr/bin/env python3
"""M7 K0: the perfect packs of D5 (agents/m7-plan.md section 3). Pure CPU; no model, no server.

    .venv/bin/python scripts/m7_perfect_packs.py [--out-dir work/m7]

Pack of a scenario:
  1. head: for every must point with at least one covering set, in rubric order, the set that adds the fewest characters to what is
     already chosen (a set wholly chosen adds none; ties go to the earlier set). Chunks are kept whole, also over the frozen cap.
  2. padding: the chunks of the scenario's frozen pack (the D5 funnel's), in frozen order, skipping those in the head, while the pack
     stays within max(6, head chunks) chunks and 14,000 characters, counted as profiles.select_ranked counts them (a chunk adds
     min(length, room); stop when less than 500 characters of room remain; the head's characters are its whole lengths).
  3. points with no covering set or not indexed get nothing.
Writes packs_perfect.jsonl (+ .manifest.json), packs_perfect_unpadded.jsonl (+ manifest), packs_perfect_table.json.
"""
import argparse
import hashlib
import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KIND = "m7-perfect-packs"
CAP_CHUNKS, CAP_CHARS, MIN_ROOM = 6, 14000, 500
INPUTS = {"rubrics": "eval/m6/d5/rubrics.jsonl", "chunk_check": "eval/m6/d5/CHUNK_CHECK.json", "items": "eval/m6/d5/items.jsonl",
          "funnel_packs": "work/m6/d5_funnel/packs.jsonl", "index_frozen": "runs/v3/INDEX_FROZEN.json",
          "m5_b4_served": "scripts/m5_b4_served.py", "profiles": "kafka_sre_rag/profiles.py"}


class PackError(RuntimeError):
    pass


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def read_jsonl(p):
    with open(p, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def point_sets(pt):
    return [list(s) for s in (pt.get("chunk_sets") or []) if s]


def strict_served(pt, pack):
    """The strict count of scripts/m5_b4_served.py point_flags: one whole set in the pack."""
    P = set(pack)
    return any(set(s) <= P for s in point_sets(pt))


def build_head(points, lens):
    """points: must points in rubric order; lens: {chunk id: characters}. -> ordered head chunk ids."""
    head = []
    for pt in points:
        sets = point_sets(pt)
        if not sets:
            continue
        have = set(head)
        best, best_add = None, None
        for s in sets:
            add = sum(lens[c] for c in dict.fromkeys(s) if c not in have)
            if best is None or add < best_add:      # strict "<": ties go to the earlier set
                best, best_add = s, add
        for c in best:
            if c not in head:
                head.append(c)
    return head


def pad(head, frozen, lens, k=CAP_CHUNKS, max_chars=CAP_CHARS):
    """Head whole, then frozen-pack chunks in frozen order within max(k, len(head)) chunks and max_chars (select_ranked accounting)."""
    cap = max(k, len(head))
    out, used = list(head), sum(lens[c] for c in head)
    for c in frozen:
        if c in out:
            continue
        if len(out) >= cap:
            break
        room = max_chars - used
        if room < MIN_ROOM:
            break
        out.append(c)
        used += min(lens[c], room)
    return out


def pack_chars(pack, lens):
    return sum(lens[c] for c in pack)


def build_all(rubrics, funnel, lens):
    """-> (padded {id: pack}, unpadded {id: pack}, table rows). Raises PackError on an unknown chunk id."""
    padded, unpadded, table = {}, {}, []
    for r in rubrics:
        sid = r["id"]
        for pt in r["must"]:
            for s in point_sets(pt):
                for c in s:
                    if c not in lens:
                        raise PackError(f"{sid}/{pt['id']}: chunk {c} is not in the frozen index")
        fz, r4 = funnel[sid]["frozen"], funnel[sid]["r4g2"]
        head = build_head(r["must"], lens)
        full = pad(head, fz, lens)
        padded[sid], unpadded[sid] = full, list(head)
        table.append({"id": sid, "head": head, "head_chunks": len(head), "head_chars": pack_chars(head, lens),
                      "padding": full[len(head):], "padding_chunks": len(full) - len(head), "total_chunks": len(full),
                      "total_chars": pack_chars(full, lens), "over_frozen_cap": len(full) > CAP_CHUNKS or pack_chars(full, lens) > CAP_CHARS,
                      "head_in_frozen": sum(c in fz for c in head), "head_in_r4g2": sum(c in r4 for c in head),
                      "points_with_set": sum(bool(point_sets(p)) for p in r["must"]),
                      "points_served": sum(strict_served(p, full) for p in r["must"] if point_sets(p)),
                      "points_served_unpadded": sum(strict_served(p, head) for p in r["must"] if point_sets(p))})
    return padded, unpadded, table


def strict_counts(rubrics, packs):
    """-> (served, points with a set) over all scenarios for {id: pack}."""
    n = s = 0
    for r in rubrics:
        for pt in r["must"]:
            if point_sets(pt):
                n += 1
                s += strict_served(pt, packs[r["id"]])
    return s, n


def summary(table, rubrics, funnel, lens, perfect):
    hc, hch = [t["head_chunks"] for t in table], [t["head_chars"] for t in table]
    tc, tch = [t["total_chunks"] for t in table], [t["total_chars"] for t in table]
    fz = {r["id"]: funnel[r["id"]]["frozen"] for r in rubrics}
    r4 = {r["id"]: funnel[r["id"]]["r4g2"] for r in rubrics}
    out = {"scenarios": len(table),
           "head_chunks": {"median": statistics.median(hc), "min": min(hc), "max": max(hc), "total": sum(hc)},
           "head_chars": {"median": statistics.median(hch), "min": min(hch), "max": max(hch)},
           "total_chunks": {"median": statistics.median(tc), "min": min(tc), "max": max(tc)},
           "total_chars": {"median": statistics.median(tch), "min": min(tch), "max": max(tch)},
           "scenarios_over_frozen_cap": sum(t["over_frozen_cap"] for t in table),
           "scenarios_over_6_chunks": sum(t["total_chunks"] > CAP_CHUNKS for t in table),
           "scenarios_head_over_14000_chars": sum(t["head_chars"] > CAP_CHARS for t in table),
           "scenarios_total_over_14000_chars_whole_lengths": sum(t["total_chars"] > CAP_CHARS for t in table),
           "head_chunks_in_frozen_pack": sum(t["head_in_frozen"] for t in table),
           "head_chunks_in_r4g2_pack": sum(t["head_in_r4g2"] for t in table)}
    for name, packs in (("frozen", fz), ("r4g2", r4), ("perfect", perfect)):
        s, n = strict_counts(rubrics, packs)
        out[f"strict_{name}"] = {"served": s, "of": n}
    return out


def write_pack_file(path, packs, ids, manifest_extra):
    path = Path(path)
    with open(path, "w", encoding="utf-8") as f:
        for i in ids:
            f.write(json.dumps({"id": i, "pack": packs[i], "error": None}, ensure_ascii=False) + "\n")
    man = {"kind": KIND, "count": len(ids), "pack_file_sha256": sha256_file(path), **manifest_extra}
    Path(str(path) + ".manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return man["pack_file_sha256"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out-dir", default="work/m7")
    a = ap.parse_args(argv)
    P = lambda p: ROOT / p
    out_dir = P(a.out_dir) if not Path(a.out_dir).is_absolute() else Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    frozen = json.loads(P(INPUTS["index_frozen"]).read_text())
    index_path = P(frozen["index_path"])
    if sha256_file(index_path) != frozen["sha256"]:
        raise PackError("the index file does not match runs/v3/INDEX_FROZEN.json")
    lens = {}
    with open(index_path, encoding="utf-8") as f:
        for l in f:
            if l.strip():
                c = json.loads(l)
                lens[c["id"]] = len(c["text"])
    rubrics = read_jsonl(P(INPUTS["rubrics"]))
    funnel = {r["id"]: r for r in read_jsonl(P(INPUTS["funnel_packs"]))}
    missing = [r["id"] for r in rubrics if r["id"] not in funnel]
    if missing:
        raise PackError(f"no funnel pack for {missing}")
    padded, unpadded, table = build_all(rubrics, funnel, lens)
    ids = [r["id"] for r in rubrics]
    summ = summary(table, rubrics, funnel, lens, padded)
    summ["strict_unpadded"] = dict(zip(("served", "of"), strict_counts(rubrics, unpadded)))
    summ["chunk_ids_all_in_index"] = True
    inputs = {k: sha256_file(P(v)) for k, v in INPUTS.items()}
    extra = {"index_sha256": frozen["sha256"], "inputs_sha256": inputs, "inputs": INPUTS,
             "rule": "covering sets first (fewest added characters, ties to the earlier set), then frozen-pack padding "
                     "within max(6, head chunks) chunks and 14,000 characters"}
    sha = write_pack_file(out_dir / "packs_perfect.jsonl", padded, ids, {**extra, "variant": "padded"})
    write_pack_file(out_dir / "packs_perfect_unpadded.jsonl", unpadded, ids, {**extra, "variant": "unpadded"})
    (out_dir / "packs_perfect_table.json").write_text(json.dumps({"summary": summ, "scenarios": table}, indent=1, sort_keys=True) + "\n",
                                                      encoding="utf-8")
    print(f"packs_perfect.jsonl sha256 {sha}")
    for t in table:
        print(f"{t['id']}: head {t['head_chunks']} chunks {t['head_chars']} chars; padding {t['padding_chunks']}; "
              f"total {t['total_chunks']} chunks {t['total_chars']} chars; served {t['points_served']}/{t['points_with_set']}")
    print(json.dumps(summ, indent=1, sort_keys=True))
    s, n = summ["strict_perfect"]["served"], summ["strict_perfect"]["of"]
    ok = (s == n == 227 and summ["strict_frozen"]["served"] == 69 and summ["strict_r4g2"]["served"] == 112
          and summ["strict_unpadded"]["served"] == n)
    print("CHECK perfect serves every point with a set:", "PASS" if s == n else "FAIL", f"({s} of {n})")
    print("CHECK frozen 69 and r4g2 112 of 227 reproduced:",
          "PASS" if (summ["strict_frozen"]["served"], summ["strict_r4g2"]["served"], n) == (69, 112, 227) else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
