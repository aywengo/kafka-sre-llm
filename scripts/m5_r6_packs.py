#!/usr/bin/env python3
"""M5 R6: the packs of retrieval setting r4g2 for B1 and pilot2, and the measures of the informative run (MILESTONE.md, row R6; Q11).

The run decides nothing. It asks one question: do more excerpts served turn into more points covered? The picked grounded variant
(or default) answers B1 (30) and pilot2 (24) once on the packs of r4g2 and once more on the frozen packs (the development check's answers),
and the two are compared, paired by scenario.

    .venv/bin/python scripts/m5_r6_packs.py packs [--cache-dir DIR] [--out-b1 F] [--out-p2 F] ...   (CPU; no model is loaded)
    .venv/bin/python scripts/m5_r6_packs.py check --packs F --stored F --ids-from F                  (every pack starts with the stored frozen pack)
    .venv/bin/python scripts/m5_r6_packs.py sha --packs F                                           (the pack file against its recorded sha256)
    .venv/bin/python scripts/m5_r6_packs.py measures --work DIR --dev-dir DIR --variant g1 ...       (the report body)

`packs` computes every pack with the funnel's simulation of the setting (scripts/retrieval_funnel.py, read-only use) from the stored search
terms of the stored default runs, with the reranker scores read from the funnel's score caches (a missing score is an error: nothing is
loaded or computed). It then checks the result three ways, all printed: (1) every pack starts with the stored frozen pack; (2) the runtime
path (kafka_sre_rag.retrieval.select, profile r4, the setting's grid values) given the same stored scores returns the same ids in the same
order; (3) for every mapped must point the funnel's stored stage (work/m5/r4/funnel_r4.json) says "in_pack" exactly when a covering chunk is
in the pack. The pack file has one row per scenario {"id", "pack", "error": null} (the format of eval2.py answer --replay-packs) and a
sidecar <file>.manifest.json with its sha256.
"""
import argparse
import hashlib
import itertools
import json
import math
import os
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
os.environ.setdefault("HF_HUB_OFFLINE", "1")

SETTING = "r4g2"
KIND = "r6-packs"
SETS = {"b1": ("eval/m4/b1/items.jsonl", "work/m4/b1/ans_default.jsonl", "work/m5/r6/packs_b1.jsonl"),
        "p2": ("eval/m4/pilot2/items.jsonl", "work/m4/pilot2/ans_default.jsonl", "work/m5/r6/packs_p2.jsonl")}
NEEDS = {"b1": "work/m5/b1_needs.jsonl", "p2": "eval/m4/retrieval_needs_dev.jsonl"}
FUNNEL_JSON = "work/m5/r4/funnel_r4.json"
GAP = "The retrieved documentation does not cover"


class R6Error(RuntimeError):
    pass


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def rd(p):
    with open(p, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def path_of(p):
    p = Path(p)
    return p if p.is_absolute() else ROOT / p


def sidecar(p):
    return Path(str(p) + ".manifest.json")


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


# ---------------------------------------------------------------- reading mode: nothing is loaded, nothing is computed
class NoModel:
    def predict(self, pairs, batch_size=8):
        raise R6Error("a reranker score is missing from the cache and this command does not load the reranker; "
                      "point --cache-dir at a cache that holds it, or compute it under scripts/run_guarded.py first")


def make_scorer(F, ix, cache_dir):
    sc = F.open_scorer(ix, cache_dir, model=NoModel())
    sc.r4 = F.R4Scorers(ix, cache_dir, model_of=lambda L: NoModel(), say=lambda *a: None)
    return sc


def make_cache_model(F, ix, frozen_mem, r4_mem):
    """The runtime's two reranker calls answered from the caches: call 1 (the frozen pool) with the stored per-chunk scores, call 2
    (the r4 scored set) with the stored whole-call scores, found by the query and the chunk ids behind the passages."""

    class CacheModel(F.StandIn):
        def __init__(self):
            super().__init__(ix, frozen_mem)
            self.n = 0

        def predict(self, pairs, batch_size=8):
            n, self.n = self.n, self.n + 1
            if n == 0:
                return super().predict(pairs, batch_size)
            q = pairs[0][0]
            qh = hashlib.sha1(q.encode()).hexdigest()[:20]
            cands = []
            for _, t in pairs:
                c = sorted(self.by_text.get(t, ()))
                if not c:
                    raise R6Error("a passage of the runtime call matches no chunk")
                cands.append(c)
            for combo in itertools.islice(itertools.product(*cands), 4096):
                if len(set(combo)) != len(combo):
                    continue
                key = (qh, hashlib.sha1("\n".join(combo).encode()).hexdigest()[:20])
                if key in r4_mem:
                    self.calls.append((q, tuple(cands)))
                    return r4_mem[key]
            raise R6Error("the runtime's second reranker call is not in the cache (another query or another scored set)")

    return CacheModel()


# ---------------------------------------------------------------- packs
def check_prefix(packs, stored):
    """-> number of ids whose pack starts with the stored frozen pack, and the ids that do not."""
    bad = [i for i, p in packs.items() if list(p[:len(stored[i])]) != list(stored[i])]
    return len(packs) - len(bad), bad


def pack_stats(ix, packs):
    n = [len(p) for p in packs.values()]
    ch = [sum(ix.len[c] for c in p) for p in packs.values()]
    return {"n": len(n), "chunks_median": median(n), "chunks_max": max(n), "chars_median": median(ch), "chars_max": max(ch)}


def need_rows(path, ids):
    return [r for r in rd(path_of(path)) if r.get("row_type") == "point" and r.get("item") in ids]


def cmd_packs(a):
    import retrieval_funnel as F
    from unittest import mock
    ix = F.Index(a.index)
    if not ix.frozen_ok:
        raise R6Error(f"index sha256 {ix.sha} is not the frozen one in {F.FROZEN}")
    if a.setting not in F.PRESETS or not F.PRESETS[a.setting].r4:
        raise R6Error(f"{a.setting} is not an r4 setting")
    v = F.PRESETS[a.setting]
    scorer = make_scorer(F, ix, a.cache_dir)
    funnel = json.load(open(path_of(a.funnel_json), encoding="utf-8"))["per_need_stage"].get(a.setting) if a.funnel_json else None
    runtime = None
    if not a.no_runtime_check:
        from kafka_sre_rag import profiles, retrieval
        grid = profiles.grid_settings(a.setting)
        model = make_cache_model(F, ix, scorer.mem, scorer.r4.of(v).mem)
        patches = [mock.patch.object(F.RT, "reranker", lambda: model), mock.patch.object(retrieval, "_reranker_at", lambda L: model)]
        runtime = (retrieval, grid, model, patches)
    total = {"prefix": 0, "frozen_exact": 0, "runtime": 0, "n": 0, "stage_agree": 0, "stage_total": 0, "stage_missing": 0}
    outs = {"b1": a.out_b1, "p2": a.out_p2}
    for key in ("b1", "p2"):
        items_p, stored_p, _ = SETS[key]
        items = {r["id"]: r for r in rd(path_of(getattr(a, f"{key}_items") or items_p))}
        stored_rows = rd(path_of(getattr(a, f"{key}_stored") or stored_p))
        answers = {r["id"]: r for r in stored_rows}
        ids = list(items)
        states = F.make_states(ix, ids, answers, items)
        missing = [i for i in ids if i not in states]
        if missing:
            raise R6Error(f"{key}: no stored answer for {len(missing)} items (first: {missing[0]})")
        packs, stored, runtime_ok = {}, {}, 0
        for n, i in enumerate(ids, 1):
            st = states[i]
            stored[i] = list(answers[i]["pack"])
            if list(F.pack_of(st, F.FROZEN_V, scorer)) == stored[i]:
                total["frozen_exact"] += 1
            st.__dict__.pop("_rk_cache", None)
            packs[i] = list(F.pack_of(st, v, scorer))
            if runtime is not None:
                retrieval, grid, model, patches = runtime
                model.calls.clear()
                model.n = 0
                model.hint = st.head(F.FROZEN_EXACT)
                for p in patches:
                    p.start()
                try:
                    got = [c["id"] for c in retrieval.select(st.q, st.exp, profile="r4", index_path=ix.path, settings=grid)]
                finally:
                    for p in patches:
                        p.stop()
                runtime_ok += got == packs[i]
            print(f"[packs {key} {n}/{len(ids)}] {i} chunks {len(packs[i])} (frozen {len(stored[i])})", flush=True)
        ok, bad = check_prefix(packs, stored)
        total["prefix"] += ok
        total["n"] += len(ids)
        total["runtime"] += runtime_ok
        s_sim = pack_stats(ix, packs)
        s_fro = pack_stats(ix, stored)
        print(f"{key}: packs of {a.setting} that start with the stored frozen pack: {ok} of {len(ids)}" + (f" (not: {bad[:3]})" if bad else ""))
        print(f"{key}: pack size {a.setting}: chunks median {s_sim['chunks_median']:g}, max {s_sim['chunks_max']}; characters median {s_sim['chars_median']:g}, max {s_sim['chars_max']} "
              f"(chunk text lengths summed, as the funnel's pack_chars); frozen: chunks median {s_fro['chunks_median']:g}, max {s_fro['chunks_max']}; "
              f"characters median {s_fro['chars_median']:g}, max {s_fro['chars_max']}")
        # the funnel's stored stage of every mapped must point against the pack
        for r in need_rows(NEEDS[key] if not getattr(a, f"{key}_needs") else getattr(a, f"{key}_needs"), set(ids)):
            if r.get("status") != "mapped" or not r.get("chunks"):
                continue
            if funnel is None or f"point|{r['item']}|{r['point']}" not in funnel:
                total["stage_missing"] += 1
                continue
            total["stage_total"] += 1
            total["stage_agree"] += (funnel[f"point|{r['item']}|{r['point']}"] == "in_pack") == any(c in set(packs[r["item"]]) for c in r["chunks"])
        out = path_of(outs[key])
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.exists() and not a.force:
            raise R6Error(f"{out} exists; not overwritten (move it away first)")
        out.write_text("".join(json.dumps({"id": i, "pack": packs[i], "error": None}) + "\n" for i in ids), encoding="utf-8")
        sidecar(out).write_text(json.dumps({"kind": KIND, "run": f"r6-packs-{key}-{a.setting}", "setting": a.setting, "count": len(ids),
                                            "pack_file_sha256": sha256_file(out), "index_sha256": ix.sha,
                                            "stored_default_answers": getattr(a, f"{key}_stored") or stored_p,
                                            "stored_default_answers_sha256": sha256_file(path_of(getattr(a, f"{key}_stored") or stored_p)),
                                            "items_sha256": sha256_file(path_of(getattr(a, f"{key}_items") or items_p)),
                                            "score_cache_dir": str(a.cache_dir)}, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        print(f"{key}: wrote {outs[key]} ({len(ids)} packs) and {outs[key]}.manifest.json")
    n = total["n"]
    print(f"CHECK every pack starts with the stored frozen pack: {total['prefix']} of {n}")
    print(f"CHECK the funnel's simulation of the frozen setting reproduces the stored pack (ids and order): {total['frozen_exact']} of {n}")
    if runtime is not None:
        print(f"CHECK the runtime path (retrieval.select, profile r4, grid {a.setting}, the same stored scores) returns the same ids in the same order as the simulation: {total['runtime']} of {n}")
    else:
        print("CHECK runtime path: not run (--no-runtime-check)")
    print(f"CHECK the funnel's stored stage per mapped must point ({a.funnel_json}) says in_pack exactly when a covering chunk is in the pack: "
          f"{total['stage_agree']} of {total['stage_total']} points ({total['stage_missing']} points not in the funnel file)")
    bad = total["prefix"] != n or (runtime is not None and total["runtime"] != n) or total["stage_agree"] != total["stage_total"]
    return 1 if bad else 0


def cmd_check(a):
    packs = {r["id"]: r["pack"] for r in rd(a.packs)}
    stored = {r["id"]: r["pack"] for r in rd(a.stored)}
    ids = [r["id"] for r in rd(a.ids_from)]
    if sorted(ids) != sorted(packs) or any(i not in stored for i in ids):
        print(f"check: {a.packs} does not hold exactly the scenarios of {a.ids_from} (or the stored answers lack some)")
        return 1
    ok, bad = check_prefix(packs, stored)
    dup = [i for i, p in packs.items() if len(p) != len(set(p))]
    print(f"check {a.packs}: {ok} of {len(packs)} packs start with the stored frozen pack; {len(dup)} with a repeated chunk")
    return 0 if ok == len(packs) and not dup else 1


def cmd_sha(a):
    p = Path(a.packs)
    sc = sidecar(p)
    if not p.is_file() or not sc.is_file():
        print(f"sha: {p} or its manifest is missing")
        return 1
    m = json.load(open(sc, encoding="utf-8"))
    now = sha256_file(p)
    same = m.get("kind") == KIND and m.get("pack_file_sha256") == now
    print(f"sha: {p}: {now[:16]} against the recorded {str(m.get('pack_file_sha256'))[:16]}: {'equal' if same else 'DIFFERENT'}")
    return 0 if same else 1


# ---------------------------------------------------------------- measures
def verdicts(results_path, sets):
    """{(set, id): {point id: mean over the valid passes of 1 if the judge found it present}}, and the pass counts."""
    acc, npass = {}, {}
    for r in rd(results_path):
        if not r.get("valid_reply") or r.get("reference_given") or r["set"] not in sets:
            continue
        k = (r["set"], r["id"])
        npass[k] = npass.get(k, 0) + 1
        for pid, e in r["must"].items():
            acc.setdefault(k, {}).setdefault(pid, []).append(1.0 if e["verdict"] == "present" else 0.0)
    return {k: {p: sum(v) / len(v) for p, v in d.items()} for k, d in acc.items()}, npass


def se(xs):
    return statistics.stdev(xs) / math.sqrt(len(xs)) if len(xs) > 1 else float("nan")


def cmd_measures(a):
    import m5_g_rule as G
    import retrieval_funnel as F
    W, D = Path(a.work), Path(a.dev_dir)
    v = a.variant
    stored = {"b1": a.stored_b1, "p2": a.stored_p2}
    rub = {"b1": a.rubrics_b1, "p2": a.rubrics_p2}
    packs_f = {"b1": a.packs_b1, "p2": a.packs_p2}
    ix = F.Index(a.index)
    names = {"frozen": "frozen packs", "r4g2": "r4g2 packs"}
    files = {s: {"frozen": D / f"ans_{s}_{v}.jsonl", "r4g2": W / f"ans_{s}_r4g2.jsonl"} for s in ("b1", "p2")}
    P, SM = {k: [] for k in names}, {k: [] for k in names}      # per scenario
    pairs, rows_all, nid = [], {k: [] for k in names}, {k: [0, 0] for k in names}
    stored_all = []
    present = {}
    per_set = {}
    unjudged_n = {}
    for s in ("b1", "p2"):
        jr = G.judged(str(W / f"judge_{s}.jsonl"), rub[s], set(names))
        vd, npass = verdicts(str(W / f"judge_{s}.jsonl"), set(names))
        left_out = {}                       # (set, id) -> unjudged rows of that answer; only with --max-unjudged N > 0
        if a.max_unjudged > 0:
            try:                            # more than N unjudged rows in the set, or an answer with no valid pass: an error
                bad = G.check_unjudged(W / f"judge_{s}.jsonl", a.max_unjudged, set(names))
            except G.RuleError as e:
                raise R6Error(str(e))
            for r in bad:
                left_out[(r["set"], r["id"])] = left_out.get((r["set"], r["id"]), 0) + 1
        for k, n in npass.items():
            if n != 2 - left_out.get(k, 0):
                raise R6Error(f"{k}: {n} valid judge passes, {2 - left_out.get(k, 0)} expected")
        unjudged_n[s] = sum(left_out.values())
        stored_rows = G.rd(stored[s])
        stored_all += stored_rows
        ids = [r["id"] for r in G.rd(rub[s])]
        for k in names:
            rows = G.rd(files[s][k])
            if [r["id"] for r in rows] != ids and sorted(r["id"] for r in rows) != sorted(ids):
                raise R6Error(f"{files[s][k]} does not hold the scenarios of {rub[s]}")
            rows_all[k] += rows
            n_, k_ = G.parse_idcheck(W / f"idcheck_{s}_{k}.txt")
            nid[k][0] += n_
            nid[k][1] += k_
        diffs = []
        for i in ids:
            for k in names:
                if (k, i) not in jr:
                    raise R6Error(f"no valid judge result for {k} {s} {i}")
            pf, pr = jr[("frozen", i)][0], jr[("r4g2", i)][0]
            P["frozen"].append(pf)
            P["r4g2"].append(pr)
            diffs.append(pr - pf)
            if s == "b1":
                SM["frozen"].append(jr[("frozen", i)][1])
                SM["r4g2"].append(jr[("r4g2", i)][1])
        pairs.append((s, diffs))
        per_set[s] = {"n": len(ids), "P_frozen": sum(P["frozen"][-len(ids):]) / len(ids), "P_r4g2": sum(P["r4g2"][-len(ids):]) / len(ids)}
        for k in names:
            for i in ids:
                present[(s, k, i)] = vd.get((k, i), {})
    alld = [d for _, ds in pairs for d in ds]
    stats = {k: G.answer_stats(rows_all[k], stored_all, False, a.stub) for k in names}
    # served against covered
    pk = {k: {} for k in names}
    for s in ("b1", "p2"):
        for r in G.rd(packs_f[s]):
            pk["r4g2"][(s, r["id"])] = set(r["pack"])
        for r in G.rd(stored[s]):
            pk["frozen"][(s, r["id"])] = set(r["pack"])
    served = {"all": [0, 0, 0], "w2": [0, 0, 0]}            # points, covered frozen, covered r4g2
    groups = {g: {"n": 0, "frozen": 0.0, "r4g2": 0.0} for g in ("both", "new", "neither")}
    by_set = {"b1": [0, 0, 0], "p2": [0, 0, 0]}
    for s in ("b1", "p2"):
        ids = {r["id"] for r in G.rd(rub[s])}
        for r in need_rows(a.needs_b1 if s == "b1" else a.needs_dev, ids):
            if r.get("status") != "mapped" or not r.get("chunks"):
                continue
            cf = any(c in pk["frozen"][(s, r["item"])] for c in r["chunks"])
            cr = any(c in pk["r4g2"][(s, r["item"])] for c in r["chunks"])
            for key in ("all",) + (("w2",) if r.get("weight") == 2 else ()):
                served[key][0] += 1
                served[key][1] += cf
                served[key][2] += cr
            by_set[s][0] += 1
            by_set[s][1] += cf
            by_set[s][2] += cr
            g = "both" if cf and cr else ("new" if cr and not cf else "neither")
            groups[g]["n"] += 1
            for k in names:
                groups[g][k] += present[(s, k, r["item"])].get(r["point"], 0.0)
    pf_, pr_ = sum(P["frozen"]) / len(P["frozen"]), sum(P["r4g2"]) / len(P["r4g2"])
    md = sum(alld) / len(alld)
    better, worse = sum(1 for d in alld if d > 1e-9), sum(1 for d in alld if d < -1e-9)
    f = lambda x, d=1: "n/a" if x is None else f"{x:.{d}f}"  # noqa: E731
    sp = {k: pack_stats(ix, {i: sorted(p) for (s, i), p in pk[k].items()}) for k in names}
    L = []
    L.append(f"### Mode `{a.mode}`: r4g2 packs against frozen packs, same mode, same {len(alld)} scenarios (B1 {per_set['b1']['n']}, pilot2 {per_set['p2']['n']}), paired")
    L.append("")
    L.append("| measure | frozen packs | r4g2 packs | difference |")
    L.append("|---|---|---|---|")
    L.append(f"| judged coverage P, mean of two passes, {len(alld)} answers | {pf_:.3f} | {pr_:.3f} | {md:+.3f} (standard error {se(alld):.3f} over scenarios; r4g2 higher on {better}, lower on {worse}, equal on {len(alld) - better - worse}) |")
    for s, ds in pairs:
        L.append(f"| of which {'B1' if s == 'b1' else 'pilot2'} ({len(ds)}) | {per_set[s]['P_frozen']:.3f} | {per_set[s]['P_r4g2']:.3f} | {sum(ds) / len(ds):+.3f} (standard error {se(ds):.3f}) |")
    sf, sr = 10 * sum(SM["frozen"]) / len(SM["frozen"]), 10 * sum(SM["r4g2"]) / len(SM["r4g2"])
    L.append(f"| supported essential misses per 10 answers, B1 ({len(SM['frozen'])}) | {sf:.2f} | {sr:.2f} | {sr - sf:+.2f} |")
    L.append(f"| median completion tokens, {stats['frozen']['n']} answers | {f(stats['frozen']['median_tokens'], 0)} | {f(stats['r4g2']['median_tokens'], 0)} | |")
    L.append(f"| median answer seconds (the model's own time) | {f(stats['frozen']['median_answer_s'])} | {f(stats['r4g2']['median_answer_s'])} | |")
    L.append(f"| median total seconds (answer + the stored default run's search and retrieval seconds; the second reranker call of r4g2 is not in either) | {f(stats['frozen']['median_total_s'])} | {f(stats['r4g2']['median_total_s'])} | |")
    L.append(f"| answers cut at the limit | {stats['frozen']['cut']} of {stats['frozen']['n']} | {stats['r4g2']['cut']} of {stats['r4g2']['n']} | |")
    pi = {k: 10 * nid[k][1] / nid[k][0] if nid[k][0] else None for k in names}
    L.append(f"| nonexistent identifiers per 10 answers (identifier check; {nid['frozen'][1]} in {nid['frozen'][0]} / {nid['r4g2'][1]} in {nid['r4g2'][0]}) | {f(pi['frozen'], 2)} | {f(pi['r4g2'], 2)} | |")
    L.append(f"| gap sentences per answer | {stats['frozen']['gap_sentences'] / stats['frozen']['n']:.2f} | {stats['r4g2']['gap_sentences'] / stats['r4g2']['n']:.2f} | |")
    L.append(f"| pack size, chunks (median / max) | {sp['frozen']['chunks_median']:g} / {sp['frozen']['chunks_max']} | {sp['r4g2']['chunks_median']:g} / {sp['r4g2']['chunks_max']} | |")
    L.append(f"| pack size, characters (median / max) | {sp['frozen']['chars_median']:g} / {sp['frozen']['chars_max']} | {sp['r4g2']['chars_median']:g} / {sp['r4g2']['chars_max']} | |")
    L.append("")
    L.append("### More served, more covered (loose count: a point is served when any chunk its rubric names is in the pack)")
    L.append("")
    L.append("| indexed must points | points | served, frozen packs | served, r4g2 packs |")
    L.append("|---|---|---|---|")
    for lab, key in (("all", "all"), ("weight 2", "w2")):
        n, x, y = served[key]
        L.append(f"| {lab} | {n} | {x} ({100 * x / n:.1f} %) | {y} ({100 * y / n:.1f} %) |")
    for s, nm in (("b1", "B1"), ("p2", "pilot2")):
        n, x, y = by_set[s]
        L.append(f"| of which {nm} | {n} | {x} | {y} |")
    L.append("")
    L.append("Points by where they are served, and how many the judge finds present in the answer of each pack set (mean of two passes, in points):")
    L.append("")
    L.append("| points | number | judge finds present, answers on frozen packs | judge finds present, answers on r4g2 packs |")
    L.append("|---|---|---|---|")
    for g, lab in (("both", "served under both pack sets"), ("new", "newly served by r4g2 (in the r4g2 pack, not in the frozen pack)"), ("neither", "served under neither")):
        x = groups[g]
        L.append(f"| {lab} | {x['n']} | {x['frozen']:.1f} | {x['r4g2']:.1f} |")
    L.append("")
    L.append(f"The loose count is by chunk id from the needs files (`{a.needs_b1}`, `{a.needs_dev}`), which are built from the rubrics; pilot2's rubrics name files, not chunks, so its chunk ids come from the needs file. "
             "Points are counted, not any-of units. An answer can state a point that its pack does not hold (from the question or from memory), so the present counts are not limited to served points.")
    (W / "measures.json").write_text(json.dumps({"mode": a.mode, "scenarios": len(alld), "P": {"frozen": pf_, "r4g2": pr_}, "diff": md, "se": se(alld),
                                                 "served": served, "groups": groups, "supported_misses_per10": {"frozen": sf, "r4g2": sr},
                                                 **({"unjudged": {"max_unjudged": a.max_unjudged, "rows": unjudged_n}} if a.max_unjudged > 0 else {})}, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (W / "measures.md").write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L))
    return 0


# ---------------------------------------------------------------- the chain's own options (--max-unjudged, --accept-code-change)
CODE_FILES = ("m5_r6_run.sh", "m5_r6_packs.py")       # the chain's own code: the only pins whose change --accept-code-change can accept
ANSWER_STAGES = ("answers",)                          # the stage that makes the answers; the judge, checks and report stages come after it


def cmd_start_check(a):
    """The development chain's start check (m5_g_rule.cmd_start_check, called, not copied) with this chain's two code files and answering stage.
    Its refusal text names the development chain's two files in the general sentence; the lines that follow name the pins that differ."""
    import m5_g_rule as G
    saved = G.CODE_FILES, G.ANSWER_STAGES
    G.CODE_FILES, G.ANSWER_STAGES = CODE_FILES, ANSWER_STAGES
    try:
        return G.cmd_start_check(a)
    finally:
        G.CODE_FILES, G.ANSWER_STAGES = saved


def cmd_unjudged_note(a):
    """The rule script's block of the judge rows left out, then the sentence about the paired comparison. Nothing for N = 0."""
    import m5_g_rule as G
    if a.max_unjudged <= 0:
        return 0
    G.cmd_unjudged_note(a)
    print("\nFor a scenario with an unjudged row the paired comparison (frozen packs against r4g2 packs) uses one pass on that side and two on the other; "
          "the other measures of that answer are unchanged.")
    return 0


def cmd_code_change_note(a):
    import m5_g_rule as G
    m = json.load(open(a.manifest, encoding="utf-8"))
    ch = m.get(G.AUDIT_KEY) or []
    if not ch:
        return 0
    print("### Accepted change of the chain's own code (`--accept-code-change`)\n")
    print("Only `scripts/m5_r6_run.sh` and `scripts/m5_r6_packs.py` can be accepted, and only when the answering stage was done: all answers were produced "
          "under the old pins.\n")
    for c in ch:
        print(f"- {c['time']}: reason: {c['reason']}")
        for f, hh in sorted(c["files"].items()):
            print(f"  - `{f}`: old sha256 `{hh['old']}`, new sha256 `{hh['new']}`")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("packs")
    p.add_argument("--setting", default=SETTING)
    p.add_argument("--index", default="work/v3/chunks_s1.jsonl")
    p.add_argument("--cache-dir", default="work/m5/r4/cache")
    p.add_argument("--funnel-json", default=FUNNEL_JSON)
    p.add_argument("--no-runtime-check", action="store_true")
    p.add_argument("--force", action="store_true", help="overwrite an existing pack file")
    for k, (i, s, o) in SETS.items():
        p.add_argument(f"--{k}-items")
        p.add_argument(f"--{k}-stored")
        p.add_argument(f"--{k}-needs")
        p.add_argument(f"--out-{k}", default=o)
    p = sub.add_parser("check")
    for k in ("packs", "stored", "ids-from"):
        p.add_argument("--" + k, required=True)
    p = sub.add_parser("sha")
    p.add_argument("--packs", required=True)
    p = sub.add_parser("measures")
    p.add_argument("--work", required=True)
    p.add_argument("--dev-dir", required=True)
    p.add_argument("--variant", required=True)
    p.add_argument("--mode", required=True)
    p.add_argument("--index", default="work/v3/chunks_s1.jsonl")
    p.add_argument("--needs-b1", default=NEEDS["b1"])
    p.add_argument("--needs-dev", default=NEEDS["p2"])
    p.add_argument("--stub", action="store_true")
    p.add_argument("--max-unjudged", type=int, default=0, help="judge rows per judged set that may stay invalid and be left out (default 0)")
    for k in ("stored-b1", "stored-p2", "rubrics-b1", "rubrics-p2", "packs-b1", "packs-p2"):
        p.add_argument("--" + k, required=True)
    p = sub.add_parser("start-check")
    import m5_g_rule as G
    G.add_state_args(p)
    p = sub.add_parser("unjudged-note")
    p.add_argument("--work", required=True)
    p.add_argument("--max-unjudged", type=int, default=0)
    p.add_argument("--rounds", type=int, default=2)
    p = sub.add_parser("code-change-note")
    p.add_argument("--manifest", required=True)
    a = ap.parse_args(argv)
    try:
        return {"packs": cmd_packs, "check": cmd_check, "sha": cmd_sha, "measures": cmd_measures, "start-check": cmd_start_check,
                "unjudged-note": cmd_unjudged_note, "code-change-note": cmd_code_change_note}[a.cmd](a)
    except (R6Error, OSError, KeyError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
