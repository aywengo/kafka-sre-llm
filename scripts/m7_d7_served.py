#!/usr/bin/env python3
"""M7 KB phase 2, I5: D7 packs, rating bundles and the served count (kb2_design.md section 6, stage 1). CPU only, no model, no server.

  packs     for each D7 scenario: arm A (the frozen pack on the stored search terms) and arm K (the same frozen pack, then up to 4
            fact entries, kafka_sre_rag/kb_entries.py), written in the replay format of work/m7/six_arm/packs_frozen_d5.jsonl
            (id, pack, error) with a .manifest.json, so scripts/eval2.py --replay-packs can use them (K lists chunk ids and `kbe:` 
            entry ids; kb_entries.resolve_pack turns them into items). Also the depth controls: the frozen ranking's chunks to 10
            (top10) and r4g2's pack, for the covering-set count only. Resumable; a line per scenario; refuses an existing output.
  bundles   the blind rating bundles: per scenario the union of both packs (which is the K pack), 1 to 3 decoy entries (written from
            the scenario's covering chunks, not retrieved) and about 10 % controls (recorded covering sets outside both packs),
            shuffled, with neutral labels and no arm, role or type marker; every must point of every scenario is to be rated
            (entry-only points need ratings outside the denominator). The key (labels to roles, ids and texts) is a separate file.
  count     the served count from the rating file only: a point is served by an arm when one named set of one or two items lies
            wholly in that arm's pack; a named set that contains a decoy never counts; sets with more than two items, an unknown
            label or a quote that is not verbatim are dropped and counted. The denominator is read from the ledger (its file and
            sha256; the file's sha256 must equal it). Refuses any unrated point. Prints the bar part and the parts of the row.

Search terms (written by task I7, work/m7/d7/search_terms.jsonl): one row per scenario in items order, with the fields id,
  search_terms, terms, terms_malformed, raw_reply, prompt_sha256, prompt_chars, model, settings, items_sha256, tokens, truncated,
  seconds, step ("S0") and, in stub rows only, stub. The `search_terms` string is replayed exactly as stored (it is what
  retrieval.select receives); `terms` is never used. Here: ids equal the items' ids, prompt_sha256 is the sha256 of the canonical
  messages of prompts.expand_messages(full_question(item)) (eval2.messages_fingerprint's form), items_sha256 (when present) is the
  items file's, and a stub row is refused unless --stand-in (a dry run).
Pack ids: a chunk is listed by its id, an entry by "kbe:" + its id (kb_entries.PACK_PREFIX); evalhooks.chunks_by_ids (so
  scripts/eval2.py answer --replay-packs) resolves them through kafka_sre_rag/kb_entries.py against the record's hash.
Rating file (one row per scenario and point; a second rater's file has the same form):
  {"id": scenario id, "point": must id, "sets": [{"items": ["i03"], "quotes": {"i03": "verbatim text"}}, ...], "rater": name}
  sets [] means that no item states the point; a missing row or a missing "sets" is an unrated point.
Exit codes: 0 done; 1 a check of a run failed; 2 refused or an input is missing.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import config, prompts  # noqa: E402
from kafka_sre_rag.kb_entries import PACK_PREFIX  # noqa: E402

BAR_PP = 15.0
TOP10 = (10, 24000)                       # the depth control: the frozen selection rule with 10 items and the entries pack's cap
DECOYS = (1, 3)
CONTROL_SHARE = 0.10
SECOND_SHARE = 0.20
ARMS = ("A", "K")
BUNDLE_INSTRUCTIONS = (
    "For each point below, read the items and name every item, and every pair of items, that together state the whole fact of the "
    "point (nothing more is needed and nothing may be missing). Give a verbatim quote from each item you name. If no item or pair "
    "states the point's whole fact, name none. An item that states only part of a fact, or the same words about another resource, "
    "setting or version, does not count. Items may be of different kinds; judge only what an item says.")


class Refused(Exception):
    pass


# ---------------------------------------------------------------- files
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def sha256_text(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def read_jsonl(path):
    p = Path(path)
    if not p.is_file():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def append_jsonl(path, row):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def write_new(path, text):
    p = Path(path)
    if p.exists():
        raise Refused(f"{p.name} exists; outputs go to new paths")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def norm_ws(s):
    return re.sub(r"\s+", " ", s or "").strip()


def full_question(item):
    """The question as the answer harness passes it to the expansion and the retrieval (scripts/eval2.py full_question)."""
    q = item["question"]
    if item.get("evidence"):
        q += "\n\nEvidence:\n" + item["evidence"]
    return q


def messages_fingerprint(messages):
    """(sha256 hex, characters) of a message list in eval2.PROMPT_HASH_FORM."""
    canon = [{"role": m["role"], "content": m["content"]} for m in messages]
    blob = json.dumps(canon, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest(), sum(len(m["content"]) for m in canon)


def sort_key(seed, *parts):
    return sha256_text(":".join([str(seed)] + [str(p) for p in parts]))


def load_chunks(index_path=None):
    p = Path(index_path) if index_path else config.index_file()
    return {c["id"]: c for c in read_jsonl(p)}


def load_terms(path, items, expect_sha=None, items_sha=None, allow_stub=False):
    """The stored search terms, checked against the items and the runtime's expansion call."""
    p = Path(path)
    if not p.is_file():
        raise Refused(f"no search terms file {p.name} (task I7 writes it)")
    sha = sha256_file(p)
    if expect_sha and sha != expect_sha:
        raise Refused(f"{p.name} has sha256 {sha}, expected {expect_sha}")
    rows = read_jsonl(p)
    ids = [r.get("id") for r in rows]
    if len(set(ids)) != len(ids):
        raise Refused("the search terms file has a repeated scenario id")
    want = [it["id"] for it in items]
    if sorted(ids) != sorted(want):
        raise Refused(f"the search terms' ids are not the items' ids ({len(ids)} rows, {len(want)} items)")
    by = {r["id"]: r for r in rows}
    for it in items:
        r = by[it["id"]]
        if not isinstance(r.get("search_terms"), str):
            raise Refused(f"{it['id']}: no search_terms string")
        if r.get("stub") and not allow_stub:
            raise Refused(f"{it['id']}: a stub row (a dry run's search terms are not used for real packs)")
        if items_sha and r.get("items_sha256") not in (None, items_sha):
            raise Refused(f"{it['id']}: the row was made for other items (items_sha256 differs)")
        got = messages_fingerprint(prompts.expand_messages(full_question(it)))[0]
        if r.get("prompt_sha256") != got:
            raise Refused(f"{it['id']}: prompt_sha256 is not that of prompts.expand_messages(full_question(item))")
    return by, sha


# ---------------------------------------------------------------- packs
class StandInReranker:
    """Dry run: deterministic hash scores, no model."""

    def predict(self, pairs, batch_size=8):
        return [int(hashlib.sha1((q + "\0" + t).encode()).hexdigest()[:8], 16) / 2 ** 32 for q, t in pairs]


def pack_manifest(name, ids, path, extra):
    return {"kind": "m7-d7-packs", "count": len(ids), "pack_file_sha256": sha256_file(path), "setting": name,
            "source": "scripts/m7_d7_served.py packs",
            "created_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), **extra}


def cmd_packs(a):
    from kafka_sre_rag import kb_entries, profiles, retrieval
    items = read_jsonl(a.items)
    if not items:
        raise Refused(f"no items in {a.items}")
    terms, terms_sha = load_terms(a.terms, items, a.terms_sha256, sha256_file(a.items), allow_stub=a.stand_in)
    out_dir = Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    names = ("frozen", "entries", "top10", "r4g2")
    finals = {n: out_dir / f"packs_{n}_d7.jsonl" for n in names}
    for p in finals.values():
        if p.exists() or Path(str(p) + ".manifest.json").exists():
            raise Refused(f"{p.name} (or its manifest) exists; outputs go to new paths")
    stand = StandInReranker() if a.stand_in else None
    if stand is not None:
        retrieval.RT.reranker = lambda: stand
        retrieval._reranker_at = lambda max_length: stand
    try:
        kb = kb_entries.KbEntries.load(a.kb_record, index_path=a.kb_index, reranker=stand)
    except kb_entries.KbEntriesError as e:
        raise Refused(str(e))
    ip = a.index_path
    RT, BP = retrieval.RT, retrieval.BP
    chunk_index = RT.index(str(ip or config.index_file()))[0]
    grid = profiles.grid_settings("r4g2")
    work = out_dir / "packs_d7.rows.jsonl"
    done = {r["id"] for r in read_jsonl(work)}
    todo = [it for it in items if it["id"] not in done]
    print(f"[packs] {len(items)} scenarios, {len(done)} already done, {len(todo)} to do", flush=True)
    for n, it in enumerate(todo, 1):
        t0 = time.monotonic()
        q, exp = full_question(it), terms[it["id"]]["search_terms"]
        frozen = retrieval.select(q, exp, index_path=ip)
        t1 = time.monotonic()
        merged, info = kb.merge(frozen, q, exp)
        t2 = time.monotonic()
        f_ids, k_ids = [c["id"] for c in frozen], [c["id"] for c in merged]
        if k_ids[:len(f_ids)] != f_ids or len(set(k_ids)) != len(k_ids):
            print(f"[packs] {n}/{len(todo)} id={it['id']} FAILED the entries pack does not start with the frozen pack", flush=True)
            return 1
        ranked = RT.rank(q, exp, index_path=str(ip or config.index_file()))
        top10 = [chunk_index[i]["id"] for i in BP.select(ranked, chunk_index, *TOP10)]
        r4 = [c["id"] for c in retrieval.select(q, exp, profile="r4", settings=grid, index_path=ip)]
        append_jsonl(work, {"id": it["id"], "frozen": f_ids, "entries": k_ids, "top10": top10, "r4g2": r4,
                            "added": info["added"], "skipped": info["skipped"], "pack_chars": info["pack_chars"],
                            "seconds": {"frozen": round(t1 - t0, 3), "entries": round(t2 - t1, 3), "total": round(time.monotonic() - t0, 3)}})
        print(f"[packs] {n}/{len(todo)} id={it['id']} frozen={len(f_ids)} entries_added={len(info['added'])} top10={len(top10)} "
              f"r4g2={len(r4)} chars={info['pack_chars']} seconds={time.monotonic() - t0:.1f}", flush=True)
    rows = {r["id"]: r for r in read_jsonl(work)}
    ids = [it["id"] for it in items]
    extra = {"items_sha256": sha256_file(a.items), "terms_sha256": terms_sha, "stand_in": bool(a.stand_in),
             "kb_record_sha256": sha256_file(a.kb_record or kb_entries.RECORD),
             "kb_index_sha256": kb.record["sha256"]}
    for n_, p in finals.items():
        with open(p, "w", encoding="utf-8") as f:
            for i in ids:
                f.write(json.dumps({"id": i, "pack": rows[i][n_], "error": None}, ensure_ascii=False) + "\n")
        Path(str(p) + ".manifest.json").write_text(json.dumps(pack_manifest(n_, ids, p, extra), indent=1, sort_keys=True) + "\n",
                                                    encoding="utf-8")
        print(f"[packs] wrote {p.name} sha256 {sha256_file(p)}", flush=True)
    n_add = sorted(len(r["added"]) for r in rows.values())
    marker = out_dir / "packs_d7.done"
    marker.write_text(json.dumps({"scenarios": len(ids), "files": {p.name: sha256_file(p) for p in finals.values()}, "stand_in": bool(a.stand_in)},
                                 indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"[packs] done {len(ids)} scenarios; entries added per scenario: min {n_add[0]}, median {n_add[len(n_add) // 2]}, max {n_add[-1]}", flush=True)
    return 0


# ---------------------------------------------------------------- the rubric's points
def points_of(rubrics):
    """{(scenario, point): {"text","weight","sets"}} in file order."""
    out = {}
    for r in rubrics:
        for m in r["must"]:
            out[(r["id"], m["id"])] = {"text": m["text"], "weight": m.get("weight", 1),
                                       "sets": [list(s) for s in (m.get("chunk_sets") or []) if s]}
    return out


# ---------------------------------------------------------------- bundles
def item_text(it):
    return prompts.pack_text([it])


def cmd_bundles(a):
    from kafka_sre_rag import kb_entries
    items = read_jsonl(a.items)
    rubrics = read_jsonl(a.rubrics)
    pts = points_of(rubrics)
    pk_f = {r["id"]: r["pack"] for r in read_jsonl(a.packs_frozen)}
    pk_k = {r["id"]: r["pack"] for r in read_jsonl(a.packs_entries)}
    for it in items:
        if it["id"] not in pk_f or it["id"] not in pk_k:
            raise Refused(f"{it['id']}: no pack in the packs files")
        if pk_k[it["id"]][:len(pk_f[it["id"]])] != pk_f[it["id"]]:
            raise Refused(f"{it['id']}: the entries pack does not start with the frozen pack")
    out_dir = Path(a.out_dir)
    key_path = Path(a.key_out)
    if key_path.exists() or any(out_dir.glob("bundle_*.md")) or (out_dir / "rating_template.jsonl").exists():
        raise Refused("bundles, template or key exist; outputs go to new paths")
    try:
        kb = kb_entries.KbEntries.load(a.kb_record, index_path=a.kb_index, reranker=StandInReranker())
    except kb_entries.KbEntriesError as e:
        raise Refused(str(e))
    chunks = load_chunks(a.index_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    key = {"seed": a.seed, "packs_frozen_sha256": sha256_file(a.packs_frozen), "packs_entries_sha256": sha256_file(a.packs_entries),
           "rubrics_sha256": sha256_file(a.rubrics), "items_sha256": sha256_file(a.items), "scenarios": {}}
    template, no_decoy, texts = [], [], {}
    for it in items:
        sid = it["id"]
        in_k = set(pk_k[sid])
        spts = {p: v for (s, p), v in pts.items() if s == sid}
        cover = sorted({c for v in spts.values() for s in v["sets"] for c in s})
        raw = []                                              # (role, type, id, item dict)
        for uid in pk_k[sid]:
            if uid.startswith(PACK_PREFIX):
                raw.append(("pack", "entry", uid, kb.item(uid[len(PACK_PREFIX):])))
            else:
                if uid not in chunks:
                    raise Refused(f"{sid}: chunk {uid} is not in the chunk index")
                raw.append(("pack", "chunk", uid, chunks[uid]))
        # decoys: entries written from the covering chunks (a source of the entry is one) that the K pack does not hold
        cand = sorted((PACK_PREFIX + e["id"] for e in kb.rows if PACK_PREFIX + e["id"] not in in_k
                       and any(s["id"] in cover for s in e["sources"])), key=lambda uid: sort_key(a.seed, "decoy", sid, uid))
        n_dec = DECOYS[0] + int(sort_key(a.seed, "ndecoy", sid)[:8], 16) % (DECOYS[1] - DECOYS[0] + 1)
        for uid in cand[:n_dec]:
            raw.append(("decoy", "entry", uid, kb.item(uid[len(PACK_PREFIX):])))
        if not cand:
            no_decoy.append(sid)
        n_ctrl = math.ceil(CONTROL_SHARE * len(raw) / (1 - CONTROL_SHARE))
        sets = sorted(({tuple(s) for v in spts.values() for s in v["sets"] if not (set(s) & in_k)}),
                      key=lambda s: (len(s), sort_key(a.seed, "control", sid, ",".join(s))))
        controls, used = [], 0
        for s in sets:
            if used + len(s) <= n_ctrl and not any(set(s) & set(c) for c in controls):
                controls.append(s)
                used += len(s)
        for s in controls:
            for cid in s:
                if cid not in chunks:
                    raise Refused(f"{sid}: control chunk {cid} is not in the chunk index")
                raw.append(("control", "chunk", cid, chunks[cid]))
        raw.sort(key=lambda r: sort_key(a.seed, "order", sid, r[2]))
        labels, entries = {}, []
        for n, (role, typ, uid, obj) in enumerate(raw, 1):
            lab = f"i{n:02d}"
            labels[uid] = lab
            entries.append({"label": lab, "role": role, "type": typ, "id": uid, "text": item_text(obj)})
        ctrl = []
        for s in controls:
            for pid, v in spts.items():
                if any(tuple(x) == s for x in v["sets"]):
                    ctrl.append({"point": pid, "labels": [labels[c] for c in s]})
        key["scenarios"][sid] = {"items": entries, "controls": ctrl, "points": list(spts)}
        body = [f"# Rating bundle {sid}", "", BUNDLE_INSTRUCTIONS, "", "## Points", ""]
        body += [f"- **{p}**: {v['text']}" for p, v in spts.items()]
        body += ["", "## Items", ""]
        for e in entries:
            body += [f"### {e['label']}", "", "```", e["text"], "```", ""]
        body += ["## Reply", "", "One row per point as JSON lines: "
                 '{"id": "<scenario>", "point": "<point>", "sets": [{"items": ["iNN"], "quotes": {"iNN": "verbatim text"}}], "rater": "<name>"}; '
                 '"sets": [] when no item states the point.', ""]
        (out_dir / f"bundle_{sid}.md").write_text("\n".join(body), encoding="utf-8")
        template += [{"id": sid, "point": p, "sets": None, "rater": None} for p in spts]
        print(f"[bundles] {sid}: {len(entries)} items ({sum(e['role'] == 'pack' for e in entries)} pack, "
              f"{sum(e['role'] == 'decoy' for e in entries)} decoys, {sum(e['role'] == 'control' for e in entries)} control chunks), "
              f"{len(spts)} points", flush=True)
    order = sorted(((s, p) for s, v in key["scenarios"].items() for p in v["points"]), key=lambda sp: sort_key(a.seed, "second", *sp))
    key["second_rater_points"] = [list(x) for x in order[:math.ceil(SECOND_SHARE * len(order))]]
    (out_dir / "rating_template.jsonl").write_text("".join(json.dumps(r) + "\n" for r in template), encoding="utf-8")
    write_new(key_path, json.dumps(key, indent=1, sort_keys=True) + "\n")
    print(f"[bundles] done {len(items)} bundles, {len(template)} points to rate, {len(key['second_rater_points'])} for the second rater; "
          f"scenarios without a decoy: {len(no_decoy)}{(' (' + ', '.join(no_decoy[:5]) + ')') if no_decoy else ''}; key {key_path.name}", flush=True)
    return 0


# ---------------------------------------------------------------- the ledger and the denominator
def ledger_denominator(ledger_text):
    """(path, sha256) of the denominator file as the ledger states it: the line that names it and holds a 64-hex string."""
    found = set()
    for line in ledger_text.splitlines():
        m = re.search(r"(\S*denominator\.txt)", line)
        if not m:
            continue
        for h in re.findall(r"\b[0-9a-f]{64}\b", line):
            found.add((m.group(1).strip("`|,"), h))
    if not found:
        raise Refused("the ledger names no denominator file with a sha256")
    if len({h for _, h in found}) != 1:
        raise Refused("the ledger gives more than one sha256 for the denominator")
    return sorted(found)[0]


def read_denominator(ledger, path=None):
    lp = Path(ledger)
    if not lp.is_file():
        raise Refused(f"no ledger at {lp.name}")
    name, want = ledger_denominator(lp.read_text(encoding="utf-8"))
    dp = Path(path) if path else ROOT / name
    if not dp.is_file():
        raise Refused(f"the denominator file {dp.name} is missing")
    got = sha256_file(dp)
    if got != want:
        raise Refused(f"the denominator file {dp.name} has sha256 {got}, the ledger says {want}")
    ids = [l.strip() for l in dp.read_text(encoding="utf-8").splitlines() if l.strip()]
    if len(set(ids)) != len(ids):
        raise Refused("the denominator list has a repeated point")
    pairs = []
    for x in ids:
        if x.count("/") != 1:
            raise Refused(f"denominator line {x!r} is not scenario/point")
        pairs.append(tuple(x.split("/")))
    return pairs, want


# ---------------------------------------------------------------- the rating
def load_rating(path, pts, scenarios, label):
    """{(scenario, point): sets} from a rating file. Refuses a point that is rated twice, unknown, or unrated."""
    rows = read_jsonl(path)
    out = {}
    for r in rows:
        k = (r.get("id"), r.get("point"))
        if k not in pts:
            raise Refused(f"{label}: {k} is not a point of the rubrics")
        if k in out:
            raise Refused(f"{label}: {k[0]}/{k[1]} is rated twice")
        if isinstance(r.get("sets"), list):
            out[k] = r["sets"]
    return out


def classify_sets(raw_sets, items_by_label):
    """Valid named sets (tuples of labels) and the counts of what was dropped. A set with a decoy is dropped first and never counts."""
    ok, drop = [], {"shape": 0, "decoy": 0, "quote": 0}
    for s in raw_sets:
        labs = s.get("items") if isinstance(s, dict) else None
        if (not isinstance(labs, list) or not 1 <= len(labs) <= 2 or len(set(labs)) != len(labs)
                or not all(isinstance(l, str) and l in items_by_label for l in labs)):
            drop["shape"] += 1
            continue
        if any(items_by_label[l]["role"] == "decoy" for l in labs):
            drop["decoy"] += 1
            continue
        q = s.get("quotes") if isinstance(s.get("quotes"), dict) else {}
        if not all(isinstance(q.get(l), str) and norm_ws(q[l]) and norm_ws(q[l]) in norm_ws(items_by_label[l]["text"]) for l in labs):
            drop["quote"] += 1
            continue
        t = tuple(sorted(labs))
        if t not in ok:
            ok.append(t)
    return ok, drop


def served(sets, items_by_label, pack):
    return any(all(items_by_label[l]["id"] in pack for l in s) for s in sets)


def clustered_se_pp(rows):
    """Standard error in percentage points of sum(difference)/sum(points), the scenarios being the clusters (ratio estimator)."""
    rows = [(m, d) for m, d in rows if m > 0]
    n, M = len(rows), sum(m for m, _ in rows)
    if n < 2 or M == 0:
        return None
    R = sum(d for _, d in rows) / M
    ss = sum((d - R * m) ** 2 for m, d in rows)
    return 100.0 * math.sqrt(n / (n - 1) * ss) / M


def part(keys, flags):
    """keys: list of (scenario, point); flags: {key: {"A": bool, "K": bool}}. The counts of one part."""
    n = len(keys)
    A = sum(flags[k]["A"] for k in keys)
    K = sum(flags[k]["K"] for k in keys)
    per = {}
    for k in keys:
        m, d = per.get(k[0], (0, 0))
        per[k[0]] = (m + 1, d + int(flags[k]["K"]) - int(flags[k]["A"]))
    se = clustered_se_pp(list(per.values()))
    return {"points": n, "A": A, "K": K, "A_pct": round(100.0 * A / n, 1) if n else None, "K_pct": round(100.0 * K / n, 1) if n else None,
            "diff_pp": round(100.0 * (K - A) / n, 1) if n else None, "se_pp": None if se is None else round(se, 1),
            "gained": sum(flags[k]["K"] and not flags[k]["A"] for k in keys), "lost": sum(flags[k]["A"] and not flags[k]["K"] for k in keys)}


def cover_count(keys, pts, pack_of):
    """Covering-set count of a pack family: points with a recorded set wholly in the pack (descriptive)."""
    return sum(any(set(s) <= set(pack_of[k[0]]) for s in pts[k]["sets"]) for k in keys)


def compute(a):
    items = read_jsonl(a.items)
    pts = points_of(read_jsonl(a.rubrics))
    denom, denom_sha = read_denominator(a.ledger, a.denominator)
    covered = [k for k, v in pts.items() if v["sets"]]
    for k in denom:
        if k not in pts or not pts[k]["sets"]:
            raise Refused(f"denominator point {k[0]}/{k[1]} has no recorded covering set in the rubrics")
    key = json.loads(Path(a.key).read_text(encoding="utf-8"))
    for fld, path in (("packs_frozen_sha256", a.packs_frozen), ("packs_entries_sha256", a.packs_entries), ("rubrics_sha256", a.rubrics)):
        if key.get(fld) != sha256_file(path):
            raise Refused(f"the key was made for other {fld.split('_sha')[0]} than {Path(path).name}")
    pk = {n: {r["id"]: r["pack"] for r in read_jsonl(p)} for n, p in (("A", a.packs_frozen), ("K", a.packs_entries))}
    sids = [it["id"] for it in items]
    for s in sids:
        if s not in key["scenarios"] or s not in pk["A"] or s not in pk["K"]:
            raise Refused(f"{s}: not in the key or the packs")
        if pk["K"][s][:len(pk["A"][s])] != pk["A"][s]:
            raise Refused(f"{s}: the entries pack does not start with the frozen pack")
    allpts = [k for k in pts if k[0] in sids]
    rating = load_rating(a.rating, pts, sids, "rating")
    unrated = [k for k in allpts if k not in rating]
    if unrated:
        raise Refused(f"{len(unrated)} of {len(allpts)} points are unrated (first: {unrated[0][0]}/{unrated[0][1]}); "
                      "every point of every scenario must be rated")
    by_label = {s: {e["label"]: e for e in key["scenarios"][s]["items"]} for s in sids}
    for s in sids:
        for e in by_label[s].values():
            if e["role"] == "control" and e["id"] in pk["K"][s]:
                raise Refused(f"{s}: control {e['id']} is in the entries pack; the key does not belong to these packs")
    flags, valid, drops, entry_sets = {}, {}, {"shape": 0, "decoy": 0, "quote": 0}, {}
    for k in allpts:
        sets, d = classify_sets(rating[k], by_label[k[0]])
        for x in d:
            drops[x] += d[x]
        valid[k] = sets
        flags[k] = {arm: served(sets, by_label[k[0]], set(pk[arm][k[0]])) for arm in ARMS}
        entry_sets[k] = [s for s in sets if served([s], by_label[k[0]], set(pk["K"][k[0]]))
                         and any(by_label[k[0]][l]["type"] == "entry" for l in s)]
    tags = {r["id"]: r["code_tag"] for r in json.loads(Path(a.slice_tags).read_text(encoding="utf-8"))["scenarios"]}
    res = {"denominator": {"points": len(denom), "sha256": denom_sha}, "dropped_sets": drops}
    res["bar"] = part(denom, flags)
    res["bar"]["met"] = res["bar"]["diff_pp"] is not None and res["bar"]["diff_pp"] >= BAR_PP
    res["whole_d7"] = part([k for k in covered if k[0] in sids], flags)
    res["out_of_slice"] = part([k for k in covered if k[0] in sids and tags.get(k[0]) == "outside"], flags)
    res["weight_2"] = part([k for k in denom if pts[k]["weight"] == 2], flags)
    res["weight_1"] = part([k for k in denom if pts[k]["weight"] != 2], flags)
    nocover = [k for k in allpts if not pts[k]["sets"]]
    res["entry_only"] = {"points_without_covering_set": len(nocover), "served_by_A": sum(flags[k]["A"] for k in nocover),
                         "served_by_K": sum(flags[k]["K"] for k in nocover),
                         "served_only_through_an_entry": sum(flags[k]["K"] and not flags[k]["A"] for k in nocover)}
    res["served_only_through_entries"] = sum(1 for k in denom if flags[k]["K"] and not flags[k]["A"] and entry_sets[k])
    cov = {"denominator": {}, "whole_d7": {}}
    for nm, fam in (("frozen", a.packs_frozen), ("entries_pack_chunks", a.packs_entries), ("top10", a.packs_top10), ("r4g2", a.packs_r4g2)):
        if not fam:
            continue
        pm = {r["id"]: [x for x in r["pack"] if not x.startswith(PACK_PREFIX)] for r in read_jsonl(fam)}
        cov["denominator"][nm] = cover_count(denom, pts, pm)
        cov["whole_d7"][nm] = cover_count([k for k in covered if k[0] in sids], pts, pm)
    res["covering_set_count"] = cov
    n_with = sum(any(x.startswith(PACK_PREFIX) for x in pk["K"][s]) for s in sids)
    n_rated = sum(any(entry_sets[k] for k in allpts if k[0] == s) for s in sids)
    res["entries_in_packs"] = {"scenarios": len(sids), "with_an_entry_in_the_pack": n_with, "with_an_entry_in_a_rated_set": n_rated}
    ctrl = [(s, c) for s in sids for c in key["scenarios"][s]["controls"]]
    named = sum(any(set(x) == set(c["labels"]) for x in valid[(s, c["point"])]) for s, c in ctrl)
    res["controls"] = {"controls": len(ctrl), "named_by_the_rater": named}
    if a.rating2:
        sample = [tuple(x) for x in key["second_rater_points"]]
        r2 = load_rating(a.rating2, pts, sids, "second rating")
        miss = [k for k in sample if k not in r2]
        if miss:
            raise Refused(f"the second rater left {len(miss)} of {len(sample)} sample points unrated (first: {miss[0][0]}/{miss[0][1]})")
        ag = {"points": len(sample), "agree_A": 0, "agree_K": 0, "same_sets": 0}
        for k in sample:
            s2, _ = classify_sets(r2[k], by_label[k[0]])
            f2 = {arm: served(s2, by_label[k[0]], set(pk[arm][k[0]])) for arm in ARMS}
            ag["agree_A"] += f2["A"] == flags[k]["A"]
            ag["agree_K"] += f2["K"] == flags[k]["K"]
            ag["same_sets"] += sorted(s2) == sorted(valid[k])
        res["second_rater"] = ag
    else:
        res["second_rater"] = None
    return res


def fmt_part(name, p):
    if not p["points"]:
        return f"{name}: no points"
    se = "" if p["se_pp"] is None else f", SE {p['se_pp']} pp"
    return (f"{name}: {p['points']} points; frozen (A) {p['A']} ({p['A_pct']} %), entries (K) {p['K']} ({p['K_pct']} %); "
            f"K minus A {p['diff_pp']:+.1f} pp{se}; gained {p['gained']}, lost {p['lost']}")


def format_text(r):
    L = [f"BAR (stage 1, Z4 part 2): served share of K minus A on the fixed denominator, at least +{BAR_PP:.1f} pp",
         fmt_part(f"denominator (ledger list, sha256 {r['denominator']['sha256'][:12]})", r["bar"]),
         "RESULT: " + ("BAR MET" if r["bar"]["met"] else "BAR NOT MET") + f" ({r['bar']['diff_pp']:+.1f} pp from the rating file only)",
         "", "Printed beside the bar, deciding nothing:",
         fmt_part("whole D7, all covered points", r["whole_d7"]),
         fmt_part("the out-of-slice scenarios, covered points", r["out_of_slice"]),
         fmt_part("denominator, weight 2", r["weight_2"]), fmt_part("denominator, weight 1", r["weight_1"]),
         f"denominator points served by K and not A only through a set with an entry: {r['served_only_through_entries']}",
         "entry-only points (no covering set; never in the bar): {points_without_covering_set} points; rated served by A {served_by_A}, "
         "by K {served_by_K}, only through an entry {served_only_through_an_entry}".format(**r["entry_only"])]
    cov = r["covering_set_count"]
    for sect, name in (("denominator", "denominator"), ("whole_d7", "whole D7")):
        if cov[sect]:
            L.append(f"covering-set count of recorded sets, {name} (descriptive, not rated): " + ", ".join(f"{k} {v}" for k, v in cov[sect].items()))
    L.append("the depth control and r4g2 are counted by recorded covering sets and are compared with the frozen pack's covering-set count, not "
             "with the rated count")
    e = r["entries_in_packs"]
    L.append(f"entries in the packs: {e['with_an_entry_in_the_pack']} of {e['scenarios']} scenarios have an entry in the K pack; "
             f"{e['with_an_entry_in_a_rated_set']} have an entry in a rated named set")
    d = r["dropped_sets"]
    L.append(f"named sets dropped: with a decoy {d['decoy']} (a set with a decoy never counts), not one or two known items {d['shape']}, "
             f"quote not verbatim {d['quote']}")
    c = r["controls"]
    L.append(f"controls: {c['named_by_the_rater']} of {c['controls']} recorded covering sets named by the rater for their point")
    s2 = r["second_rater"]
    L.append("second rater: not supplied" if not s2 else
             f"second rater, {s2['points']} points: same A-served flag {s2['agree_A']}, same K-served flag {s2['agree_K']}, same named sets {s2['same_sets']}")
    L.append("blinding: the rater did not know which items were in which pack; an entry looks different from a chunk (its rendered form), "
             "so the blinding is to pack membership, not to item type")
    return "\n".join(L)


def cmd_count(a):
    res = compute(a)
    text = format_text(res)
    if a.json_out:
        write_new(a.json_out, json.dumps(res, indent=1, sort_keys=True) + "\n")
    if a.text_out:
        write_new(a.text_out, text + "\n")
    print(text)
    return 0


# ---------------------------------------------------------------- command line
def build_parser():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    d7 = "eval/m7/d7/"
    p = sub.add_parser("packs")
    p.add_argument("--items", default=d7 + "items.jsonl")
    p.add_argument("--terms", default="work/m7/d7/search_terms.jsonl")
    p.add_argument("--terms-sha256", default=None, help="the sha256 recorded in the ledger (checked when given)")
    p.add_argument("--kb-record", default=None, help="the entries record (default kafka_sre_rag/kb_entries.json)")
    p.add_argument("--kb-index", default=None, help="the entries index file (default: the record's)")
    p.add_argument("--index-path", default=None, help="a chunk index other than the frozen one (tests)")
    p.add_argument("--out-dir", default="work/m7/d7")
    p.add_argument("--stand-in", action="store_true", help="dry run: a hash stand-in for the reranker (not a measurement)")
    p = sub.add_parser("bundles")
    p.add_argument("--items", default=d7 + "items.jsonl")
    p.add_argument("--rubrics", default=d7 + "rubrics.jsonl")
    p.add_argument("--packs-frozen", required=True)
    p.add_argument("--packs-entries", required=True)
    p.add_argument("--kb-record", default=None)
    p.add_argument("--kb-index", default=None)
    p.add_argument("--index-path", default=None)
    p.add_argument("--seed", default="20261126")
    p.add_argument("--out-dir", required=True)
    p.add_argument("--key-out", required=True)
    p = sub.add_parser("count")
    p.add_argument("--items", default=d7 + "items.jsonl")
    p.add_argument("--rubrics", default=d7 + "rubrics.jsonl")
    p.add_argument("--slice-tags", default=d7 + "slice_tags.json")
    p.add_argument("--ledger", default="runs/m7/D7_LEDGER.md")
    p.add_argument("--denominator", default=None, help="the denominator file (default: the path the ledger names)")
    p.add_argument("--packs-frozen", required=True)
    p.add_argument("--packs-entries", required=True)
    p.add_argument("--packs-top10", default=None)
    p.add_argument("--packs-r4g2", default=None)
    p.add_argument("--key", required=True)
    p.add_argument("--rating", required=True)
    p.add_argument("--rating2", default=None)
    p.add_argument("--json-out", default=None)
    p.add_argument("--text-out", default=None)
    return ap


def main(argv=None):
    a = build_parser().parse_args(argv)
    try:
        return {"packs": cmd_packs, "bundles": cmd_bundles, "count": cmd_count}[a.cmd](a)
    except Refused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
