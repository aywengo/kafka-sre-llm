#!/usr/bin/env python3
"""Retrieval packs for a large question file. Resumable version of build_packs.py: one rerank per
question, every result appended as it arrives, reruns skip the ids already in the output.

  build_packs_v3.py expand --questions Q.jsonl --out expansions.jsonl [--model M --url U --concurrency 4]
                           [--dry-run] [--limit N]
  build_packs_v3.py pack   --expansions expansions.jsonl --out packs.jsonl [--index work/v3/chunks.jsonl] [--limit N]
                           [--questions bank.jsonl]
  build_packs_v3.py stats  --packs packs.jsonl [--index work/v3/chunks.jsonl] [--k k6|k3] [--out report.md]
                           [--questions bank.jsonl]

expand (model server): a teacher model writes the one-line query expansion (retrieve.EXPAND_SYSTEM, question
  as the user message, max_tokens 300, temperature 0, thinking off). The address comes from --url or
  ~/.config/kafka-sre-llm/omlx_url, the key from OMLX_API_KEY as in generate_qa.py. --dry-run writes a fake
  expansion (the question's own terms) without any network, to test the flow.
  Row: {id, question, expansion}. id = the question's "id" field, else the first 16 hex digits of the sha1
  of the question text.
pack (CPU only): calls retrieve.rank() once per question and applies the selection loop of
  retrieve.retrieve() to that one ranking.
  Row: {id, question, expansion,
        k6: chunk ids of retrieve(question, 6, 14000, ...),  k3: chunk ids of retrieve(question, 3, 6000, ...),
        reserve: the 6 chunk ids ranked next after the k6 selection,
        scores: {chunk id: {bm25_rank (1 = best), ce (cross-encoder score)}} for the k6 and reserve chunks}
  With --questions (bank records: id, question, optional seed_chunk_id, treatment, category; joined with the
  expansions by id) each row also gets
        seed_in_pack: whether the seed chunk is among the k6 chunks, computed before any removal (null for
                      treatment "ooc" and for records without a seed),
        treatment_requested: the bank's treatment, treatment: what was done ("normal", "withheld", "ooc"),
        withheld: [seed chunk id] when the seed chunk was removed, else [],
        backfill_ranks: for withheld rows, the 1-based ranks (in the full ranking) of the chunks that entered
                      the k6 pack in place of the removed chunks,
        withheld_dupes: for withheld rows, how many near-duplicates of the seed chunk were removed as well
                      (token-set Jaccard of title + text >= 0.7, only among the ranked candidates); their ids
                      follow the seed chunk id in "withheld".
  A "withheld" record whose seed chunk is in the k6 selection gets that chunk, and every other ranked chunk
  that is a near-duplicate of it, removed from the ranking and the SAME selection rule (plan_pack) re-applied, so the pack is backfilled from the following ranks; k3 and
  reserve come from the same reduced ranking. If the seed chunk is not in k6 the pack is the normal one and
  the row says treatment "normal" (the question then stays a normal row). Without --questions the rows are
  exactly as before.
stats: markdown report on the packs (sizes, chunks per pack, spread over files and sources); with --questions
  also the seed-in-pack rate overall and per category, the withheld requests honoured, the backfill ranks.

The prompt gets the FULL text of each selected chunk (rag_answers.pack_text), so the character cap only
decides how many chunks are selected. Run from the repository root.
"""
import argparse, asyncio, hashlib, json, os, statistics, sys, time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import retrieve as RT

MODEL = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"
NO_THINKING = {"chat_template_kwargs": {"thinking_mode": "chat"}}  # as in gen_v2.sh for this model
INDEX = "work/v3/chunks.jsonl"
POOL = 40  # default `pool` of retrieve.rank(); pack stops if rank() returns something else
SIZES = {"k6": (6, 14000), "k3": (3, 6000)}  # name -> (k, max_chars)
RESERVE = 6
CHARS_PER_TOKEN = 4.19


def rows(path):
    return [json.loads(l) for l in open(path) if l.strip()]


def qid(r):
    return r["id"] if r.get("id") is not None else hashlib.sha1(r["question"].encode()).hexdigest()[:16]


def done_ids(out):
    """Ids already in a JSONL output. A last line cut off by a kill is removed, so appending stays valid."""
    if not out.exists():
        return set()
    data = out.read_bytes()
    good = data.rfind(b"\n") + 1
    if good != len(data):
        print(f"removing a cut-off last line from {out}", flush=True)
        os.truncate(out, good)
    return {json.loads(l)["id"] for l in data[:good].decode().splitlines() if l.strip()}


def todo_rows(src, out, limit):
    """(ids already done, input rows still to do), each row with its id; a repeated id is taken once."""
    out.parent.mkdir(parents=True, exist_ok=True)
    done = done_ids(out)
    seen, todo = set(done), []
    for r in rows(src):
        r = {**r, "id": qid(r)}
        if r["id"] not in seen:
            seen.add(r["id"])
            todo.append(r)
    return done, todo[:limit] if limit else todo


# ---------------------------------------------------------------- expand

def server_url(url):
    url = (url or (Path.home() / ".config/kafka-sre-llm/omlx_url").read_text()).strip().rstrip("/")
    return url if url.endswith("/v1") else url + "/v1"


def fake_expansion(question):
    return ", ".join(dict.fromkeys(RT.tokens(question)))


async def expand(a):
    out = Path(a.out)
    done, todo = todo_rows(a.questions, out, a.limit)
    if done and any(bool(r.get("dry_run")) != a.dry_run for r in rows(out)):
        sys.exit(f"{out} holds {'real' if a.dry_run else 'dry-run'} expansions; use another --out")
    print(f"[expand] {len(done)} done, {len(todo)} to do, model {'dry run' if a.dry_run else a.model}", flush=True)
    llm = None
    if not a.dry_run and todo:
        from generate_qa import LLM, strip_think  # LLM.chat retries three times
        llm = LLM(server_url(a.url), a.model, os.environ.get("OMLX_API_KEY", "local"), 0.0, 300, a.extra_body, Counter())
    sem, st, t0 = asyncio.Semaphore(a.concurrency), Counter(), time.monotonic()
    with out.open("a") as f:
        async def one(r):
            async with sem:
                try:
                    text = fake_expansion(r["question"]) if a.dry_run else \
                        strip_think(await llm.chat(RT.EXPAND_SYSTEM, r["question"]))
                except Exception as e:  # keep going; a rerun retries the ones not written
                    text = ""  # the message of an httpx error holds the server address: print the type only
                    print(f"  failed {r['id']}: {type(e).__name__} "
                          f"{getattr(getattr(e, 'response', None), 'status_code', '')}", flush=True)
                if not text:
                    st["failed"] += 1
                    return
                row = {"id": r["id"], "question": r["question"], "expansion": text}
                if a.dry_run:
                    row["dry_run"] = True
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                f.flush()
                st["done"] += 1
                if st["done"] % 25 == 0:
                    print(f"[expand] {st['done']}/{len(todo)} failed={st['failed']} "
                          f"{(time.monotonic() - t0) / st['done']:.2f} s/question", flush=True)
        await asyncio.gather(*(one(r) for r in todo))
    print(f"[expand] finished: {st['done']} written, {st['failed']} failed or empty", flush=True)
    if st["failed"]:
        sys.exit(1)  # so run_retry.sh runs it again


# ---------------------------------------------------------------- pack

def capture_ce():
    """rank() throws the cross-encoder scores away. Wrap predict() of the cached reranker so that the
    scores of the latest call can be read; rank() itself stays the only place the order is computed."""
    model, last = RT.reranker(), {}
    predict = model.predict

    def wrapped(*args, **kw):
        last["ce"] = predict(*args, **kw)
        return last["ce"]
    model.predict = wrapped
    return last


def select_ranked(ranked, lens, k, max_chars):
    """Pure selection rule of retrieve.retrieve(): the first k of `ranked` (chunk indices, best first) that
    fit, stopping at the first one with less than 500 characters of room. lens[i] = len(chunk i's text)."""
    out, used = [], 0
    for i in ranked[:k]:
        room = max_chars - used
        if room < 500:
            break
        out.append(i)
        used += min(lens[i], room)  # = len(text[:room])
    return out


def select(ranked, chunks, k, max_chars):
    """The chunk indices retrieve() returns for this ranking."""
    return select_ranked(ranked, {i: len(chunks[i]["text"]) for i in ranked[:k]}, k, max_chars)


DUPE_JACCARD = 0.7


def jaccard(a, b):
    """Token-set Jaccard similarity of two texts (retrieve.tokens); 0.0 when both are empty."""
    a, b = set(RT.tokens(a)), set(RT.tokens(b))
    return len(a & b) / len(a | b) if a | b else 0.0


def chunk_text(c):
    return (c.get("title") or "") + " " + c["text"]


def plan_pack(ranked, lens, treatment=None, seed=None, is_dupe=None):
    """Pure: selection for one question from its ranking (chunk indices, best first) and chunk lengths.
    treatment: None (no bank record), "normal", "withheld" or "ooc"; seed: chunk index of the seed chunk, or None
    when the record has none (an index that lacks the seed chunk: pass -1).
    -> {"sel": {"k6": [...], "k3": [...]}, "reserve": [...], "seed_in_pack": bool|None, "treatment": ...,
        "withheld": bool, "backfill_ranks": [...], "dupes": [...]}; indices, not chunk ids.
    seed_in_pack is computed on the original k6 selection. A "withheld" request whose seed chunk is in it is
    honoured: the chunk is taken out of the ranking and the same rule re-applied (k6, k3 and reserve all
    come from the reduced ranking); is_dupe(i) -> bool, called only then and only for the other ranked chunks,
    marks near-duplicates of the seed chunk, which are removed with it ("dupes", in rank order); otherwise the pack is the normal one and treatment is "normal"."""
    sel = {name: select_ranked(ranked, lens, k, cap) for name, (k, cap) in SIZES.items()}
    in_pack = None if treatment in (None, "ooc") or seed is None else seed in sel["k6"]
    withheld = treatment == "withheld" and in_pack
    backfill, dupes = [], []
    if withheld:
        rank_of = {i: n + 1 for n, i in enumerate(ranked)}
        old = set(sel["k6"])
        dupes = [i for i in ranked if i != seed and is_dupe is not None and is_dupe(i)]
        gone = {seed, *dupes}
        ranked = [i for i in ranked if i not in gone]
        sel = {name: select_ranked(ranked, lens, k, cap) for name, (k, cap) in SIZES.items()}
        backfill = [rank_of[i] for i in sel["k6"] if i not in old]
    reserve = ranked[len(sel["k6"]):len(sel["k6"]) + RESERVE]
    return {"sel": sel, "reserve": reserve, "seed_in_pack": in_pack, "withheld": bool(withheld),
            "treatment": "withheld" if withheld else "ooc" if treatment == "ooc" else "normal" if treatment else None,
            "backfill_ranks": backfill, "dupes": dupes}


def pack_one(r, chunks, index_path, last, bank=None, idx=None):
    """bank: the bank record of this question (or None); idx: {chunk id: index} (needed with a bank record)."""
    q, exp = r["question"], r.get("expansion") or ""
    # the BM25 pool, built with the expression rank() uses: position in it = BM25 rank, and the
    # cross-encoder scores come back in this order
    cand = [i for i, _ in RT.scores(q + " " + exp if exp else q, index_path).most_common(POOL)]
    last.pop("ce", None)
    ranked = RT.rank(q, exp, index_path=index_path) if cand else []
    ce = last.get("ce", [])
    if sorted(ranked) != sorted(cand) or len(ce) != len(cand):
        sys.exit(f"retrieve.rank() no longer matches this script (pool or rerank call changed): id {r['id']}")
    pos = {i: j for j, i in enumerate(cand)}
    treatment = seed = None
    if bank is not None:
        treatment = bank.get("treatment") or "normal"
        if bank.get("seed_chunk_id") is not None:
            seed = idx.get(bank["seed_chunk_id"], -1)
    seed_text = chunk_text(chunks[seed]) if seed is not None and seed >= 0 else ""
    plan = plan_pack(ranked, {i: len(chunks[i]["text"]) for i in ranked}, treatment, seed,
                     lambda i: jaccard(seed_text, chunk_text(chunks[i])) >= DUPE_JACCARD)
    sel, reserve = plan["sel"], plan["reserve"]
    row = {"id": r["id"], "question": q, "expansion": exp,
           **{name: [chunks[i]["id"] for i in ix] for name, ix in sel.items()},
           "reserve": [chunks[i]["id"] for i in reserve],
           "scores": {chunks[i]["id"]: {"bm25_rank": pos[i] + 1, "ce": round(float(ce[pos[i]]), 6)}
                      for i in sel["k6"] + reserve}}
    if bank is not None:
        row.update(treatment_requested=treatment, treatment=plan["treatment"], seed_in_pack=plan["seed_in_pack"],
                   withheld=[bank["seed_chunk_id"]] + [chunks[i]["id"] for i in plan["dupes"]]
                   if plan["withheld"] else [])
        if plan["withheld"]:
            row["backfill_ranks"] = plan["backfill_ranks"]
            row["withheld_dupes"] = len(plan["dupes"])
    return row


def pack(a):
    out = Path(a.out)
    done, todo = todo_rows(a.expansions, out, a.limit)
    print(f"[pack] {len(done)} done, {len(todo)} to do, index {a.index}", flush=True)
    if not todo:
        return
    chunks = RT.index(a.index)[0]
    idx = {c["id"]: i for i, c in enumerate(chunks)}
    bank = {qid(r): r for r in rows(a.questions)} if a.questions else None
    if bank is not None:
        no_bank = sum(r["id"] not in bank for r in todo)
        no_seed = sum(1 for r in todo if r["id"] in bank and bank[r["id"]].get("seed_chunk_id") is not None
                      and bank[r["id"]]["seed_chunk_id"] not in idx)
        print(f"[pack] bank {a.questions}: {len(bank)} records; {no_bank} expansions without a bank record (packed "
              f"as plain rows without the new fields), {no_seed} seed chunks not in this index", flush=True)
    last = capture_ce()
    print(f"[pack] index ({len(chunks)} chunks) and reranker loaded", flush=True)
    t0 = time.monotonic()
    with out.open("a") as f:
        for n, r in enumerate(todo, 1):
            f.write(json.dumps(pack_one(r, chunks, a.index, last, bank.get(r["id"]) if bank else None, idx),
                               ensure_ascii=False) + "\n")
            f.flush()
            if n % 25 == 0 or n == len(todo):
                per = (time.monotonic() - t0) / n
                print(f"[pack] {n}/{len(todo)} {per:.2f} s/question, about {(len(todo) - n) * per / 60:.0f} min left", flush=True)


# ---------------------------------------------------------------- stats

def prompt_text(cs):
    """Same layout as rag_answers.pack_text (not imported: that module loads mlx_lm)."""
    return "\n\n".join(f"{c['source']} — {c['title'] or c['path'].rsplit('/', 1)[-1]}\n{c['text']}" for c in cs)


def bank_stats(packs, a):
    """Seed-in-pack rate and withheld outcomes; empty when the packs were built without --questions."""
    have = [p for p in packs if "seed_in_pack" in p]
    if not have:
        return []
    cat = {qid(r): r.get("category") for r in rows(a.questions)} if a.questions else {}
    scored = [p for p in have if p["seed_in_pack"] is not None]
    md = ["## Seed chunk in the k6 pack", "",
          f"Rows with a seed chunk: {len(scored)} of {len(have)} (ooc rows have none). Seed chunk in the k6 pack, "
          f"computed before any removal: **{sum(p['seed_in_pack'] for p in scored)} of {len(scored)} "
          f"({sum(p['seed_in_pack'] for p in scored) / max(1, len(scored)):.1%})**.", ""]
    if cat:
        by = {}
        for p in scored:
            by.setdefault(cat.get(p["id"]) or "(not in the bank file)", []).append(p["seed_in_pack"])
        md += ["| category | rows | seed in pack | rate |", "|---|---:|---:|---:|",
               *[f"| {c} | {len(v)} | {sum(v)} | {sum(v) / len(v):.1%} |" for c, v in sorted(by.items())], ""]
    else:
        md += ["(per category needs --questions)", ""]
    asked = [p for p in have if p.get("treatment_requested") == "withheld"]
    done = [p for p in asked if p["treatment"] == "withheld"]
    md += ["## Withheld", "",
           f"Requested: {len(asked)}. Honoured (seed chunk was in k6 and was removed): **{len(done)}**. Not honoured "
           f"(seed chunk not in k6, row stays normal): {len(asked) - len(done)}.", ""]
    if done:
        md += [f"Honoured rows with at least one near-duplicate of the seed chunk removed as well "
               f"(Jaccard >= {DUPE_JACCARD}): **{sum(bool(p.get('withheld_dupes')) for p in done)}** of {len(done)}.", ""]
        first = Counter(min(p["backfill_ranks"]) if p.get("backfill_ranks") else None for p in done)
        md += ["Rank (1 = best in the full ranking) from which the backfill came, lowest rank of the chunks that "
               "entered the pack:", "", "| backfill from rank | rows |", "|---|---:|",
               *[f"| {k if k is not None else 'none (pack got shorter)'} | {v} |"
                 for k, v in sorted(first.items(), key=lambda kv: (kv[0] is None, kv[0]))], ""]
    return md


def stats(a):
    chunks = {c["id"]: c for c in rows(a.index)}
    packs = rows(a.packs)
    if not packs:
        sys.exit(f"no packs in {a.packs}")
    missing = {cid for p in packs for cid in p[a.k]} - chunks.keys()
    if missing:
        sys.exit(f"{len(missing)} chunk ids of {a.packs} are not in {a.index}: wrong index?")
    cap = SIZES[a.k][1]
    full, prompt, capped, files, sources = [], [], [], [], []
    n_chunks, by_source = Counter(), Counter()
    for p in packs:
        cs = [chunks[cid] for cid in p[a.k]]
        full.append(sum(len(c["text"]) for c in cs))
        prompt.append(len(prompt_text(cs)))
        used = 0
        for c in cs:  # what retrieve() counted against the cap
            used += len(c["text"][:max(0, cap - used)])
        capped.append(used)
        files.append(len({c["path"] for c in cs}))
        sources.append(len({c["source"] for c in cs}))
        n_chunks[len(cs)] += 1
        by_source.update(c["source"] for c in cs)
    n, total = len(packs), sum(by_source.values())

    def share(xs):
        return f"{sum(xs)} of {n} ({sum(xs) / n:.1%})"

    def size(name, xs, div=1):
        return f"| {name} | {statistics.median(xs) / div:,.0f} | {max(xs) / div:,.0f} |"

    md = [f"# Pack statistics: {a.packs} ({a.k})", "",
          f"Packs: {n}. Index: {a.index}. Selection: up to {SIZES[a.k][0]} chunks under a cap of {cap:,} characters.", "",
          "| Pack size in characters | median | max |", "|---|---:|---:|",
          size("full text of the selected chunks", full),
          size("the same as it goes into the prompt (with the source/title lines)", prompt),
          size(f"as selected under the cap of {cap:,}", capped), "",
          f"- Packs whose full chunk text exceeds {a.over:,} characters: {share([x > a.over for x in full])}; "
          f"counting the source/title lines: {share([x > a.over for x in prompt])}.",
          f"- Packs drawing on 2 or more files (path): {share([x >= 2 for x in files])}.",
          f"- Packs drawing on 2 or more sources: {share([x >= 2 for x in sources])}.", "",
          f"Estimated tokens per pack (prompt text / {CHARS_PER_TOKEN} characters per token; an estimate, not a tokenizer count):", "",
          "| | median | max |", "|---|---:|---:|", size("estimated tokens", prompt, CHARS_PER_TOKEN), "",
          "| Chunks per pack | packs | share |", "|---:|---:|---:|",
          *[f"| {k} | {v} | {v / n:.1%} |" for k, v in sorted(n_chunks.items())], "",
          "| Source | chunks | share |", "|---|---:|---:|",
          *[f"| {s} | {v} | {v / total:.1%} |" for s, v in by_source.most_common()], ""]
    md += bank_stats(packs, a)
    text = "\n".join(md)
    if a.out:
        Path(a.out).write_text(text)
    print(text)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)
    p = sub.add_parser("expand")
    p.add_argument("--questions", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--model", default=MODEL)
    p.add_argument("--url", help="OpenAI-compatible server (default: ~/.config/kafka-sre-llm/omlx_url)")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--extra-body", type=json.loads, default=NO_THINKING,
                   help="JSON merged into every request; the default turns thinking off for the default model")
    p.add_argument("--dry-run", action="store_true", help="fake expansions, no network")
    p.add_argument("--limit", type=int, help="at most N new questions")
    p = sub.add_parser("pack")
    p.add_argument("--expansions", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--index", default=INDEX)
    p.add_argument("--limit", type=int, help="at most N new questions")
    p.add_argument("--questions", help="bank records (id, seed_chunk_id, treatment): seed_in_pack and withheld packs")
    p = sub.add_parser("stats")
    p.add_argument("--packs", required=True)
    p.add_argument("--questions", help="bank records: adds the seed-in-pack rate per category")
    p.add_argument("--index", default=INDEX)
    p.add_argument("--k", choices=list(SIZES), default="k6")
    p.add_argument("--over", type=int, default=14500, help="report the share of packs above this many characters")
    p.add_argument("--out", help="also write the report to this file")
    a = ap.parse_args()
    if a.mode == "expand":
        asyncio.run(expand(a))
    else:
        {"pack": pack, "stats": stats}[a.mode](a)


if __name__ == "__main__":
    main()
