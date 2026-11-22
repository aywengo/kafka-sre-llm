#!/usr/bin/env python3
"""M6 M0: memory, scores and time of Qwen3-Reranker-0.6B under options that could make it fit 1.5 GB added resident memory.

Measurement only; it decides nothing and changes no runtime file. CPU, offline, the pinned revision of runs/m5/RERANKER_R5.json.

  prepare   build the 20 timing questions' pairs (setting r5a: query Qb, passage (title + newline + text)[:1500], the same 24
            candidates as M5's timing pass) and the frozen call's 40 pairs per question; no reranker model is loaded.
            Writes work/m6/m0/pairs.json (a new file).
  run       ONE option in THIS fresh process (run it under scripts/run_guarded.py): index and BM25 loaded; the current
            reranker (bge) alone, its frozen call on the 20 questions; then the Qwen model loaded as the option says and the
            20 calls scored. Prints a line per question; writes work/m6/m0/opt_<name>.json (a new file).
  report    compare every opt_*.json with the baseline's scores; prints the table's numbers as JSON.

Memory is read two ways, both from the operating system:
  rss   the resident set size at that moment ("ps -o rss= -p <pid>", kilobytes of 1,024 bytes, shown in MB of 1,000,000 bytes)
  peak  the process's peak resident set size so far (getrusage ru_maxrss: the highest rss at any moment, bytes on macOS)
Added memory against the runtime with the bge reranker alone: "after load" = rss after the Qwen model is loaded and warmed up
(two short pairs) minus rss after the bge reranker is loaded and warmed up; "at peak" = peak at the end minus the peak after the
bge reranker's 20 frozen calls (what M5's time-r5 compared).
"""
import argparse
import gc
import json
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

os.environ.setdefault("HF_HUB_OFFLINE", "1")
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import retrieval_funnel as F  # noqa: E402
import r5_qwen as Q  # noqa: E402

OUT_DIR = "work/m6/m0"
PAIRS = f"{OUT_DIR}/pairs.json"

# name: (dtype, batch size, extra loader/cleanup flags)
OPTIONS = {
    "a_fp32_b8": ("float32", 8, {}),
    "a2_fp32_b8_repeat": ("float32", 8, {}),
    "b_fp32_b2": ("float32", 2, {}),
    "c_fp32_b1": ("float32", 1, {}),
    "d1_bf16_b8": ("bfloat16", 8, {}),
    "d2_bf16_b1": ("bfloat16", 1, {}),
    "d3_fp16_b8": ("float16", 8, {}),
    "d4_fp16_b1": ("float16", 1, {}),
    "d5_bf16_b2": ("bfloat16", 2, {}),
    "e1_bf16_b1_lowcpu": ("bfloat16", 1, {"low_cpu_mem_usage": True}),
    "e2_bf16_b1_cleanup": ("bfloat16", 1, {"cleanup": True}),
    "e3_fp32_b1_cleanup": ("float32", 1, {"cleanup": True}),
    "e4_bf16_b1_eager": ("bfloat16", 1, {"attn": "eager"}),
    "e5_bf16_b1_sdpa": ("bfloat16", 1, {"attn": "sdpa"}),
}


def say(*a):
    print(*a, flush=True)


def rss_mb():
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True).stdout.strip()
    return int(out) * 1024 / 1e6


def peak_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6      # bytes on macOS


def heavy_processes():
    """Other heavy python processes at this moment (unittest runs, other model jobs), by pid and the start of the command."""
    out = subprocess.run(["ps", "-axo", "pid=,rss=,command="], capture_output=True, text=True).stdout.splitlines()
    res = []
    for l in out:
        p = l.split(None, 2)
        if len(p) < 3 or int(p[0]) == os.getpid():
            continue
        if ("unittest" in p[2] or "run_guarded" in p[2] or "mlx" in p[2]) and "grep" not in p[2]:
            res.append({"pid": int(p[0]), "rss_mb": int(p[1]) * 1024 / 1e6, "cmd": p[2][:60]})
    return res


# ------------------------------------------------------------------ prepare
def cmd_prepare(a):
    out = ROOT / PAIRS
    if out.exists():
        raise SystemExit(f"{PAIRS} exists; not overwritten")
    ns = argparse.Namespace(needs=F.R4_NEEDS, answers=F.R4_ANSWERS, items=F.R4_ITEM_FILES, index=F.INDEX)
    ix = Q.new_index(ns)
    ref = json.loads((ROOT / "work/m5/r4/time_r4.json").read_text())
    ids = ref["items"]
    needs, states = Q.load_states(ix, ns, ids)
    sc = Q.frozen_scorer(ix, "work/m5/r4/cache")

    class Zero:
        def scores(self, v, query, ids_, item=None):
            return [0.0] * len(ids_), "zero", 0.0

    sc.r4 = Zero()
    r5 = Q.R5Scorers(ix, "work/m5/r5/none", {i: st.q for i, st in states.items()})
    items = []
    for n, i in enumerate(ids):
        st = states[i]
        F.ranking(st, F.PRESETS[Q.BASE_PRESET], sc)
        info = st.r4info
        q = r5.query_of(Q.variant("r5a"), info["query"], i)
        R = info["R"]
        pairs = [(q, (ix.byid[c]["title"] + "\n" + ix.byid[c]["text"])[:Q.PASSAGE_CHARS]) for c in R]
        pool = st.head(F.FROZEN_EXACT)
        frozen = [(st.q, (ix.byid[c]["title"] + "\n" + ix.byid[c]["text"])[:1500]) for c in pool]
        items.append({"item": i, "ids": R, "pairs": pairs, "frozen_pairs": frozen})
        say(f"[prepare {n + 1}/{len(ids)}] {i} {len(pairs)} pairs, {len(frozen)} frozen pairs")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"instruction": Q.DEFAULT_INSTRUCTION, "max_length": Q.MAX_LENGTH, "items": items}))
    say(f"wrote {PAIRS}")


# ------------------------------------------------------------------ run
def load_qwen(opt):
    import torch
    from sentence_transformers import CrossEncoder
    dtype, _, flags = OPTIONS[opt]
    snap = Q.snapshot_dir()
    if snap.name != Q.REVISION:
        raise SystemExit("the cache does not hold the pinned revision")
    mk = {"torch_dtype": getattr(torch, dtype)}
    if flags.get("low_cpu_mem_usage"):
        mk["low_cpu_mem_usage"] = True
    if flags.get("attn"):
        mk["attn_implementation"] = flags["attn"]
    m = CrossEncoder(Q.MODEL_ID, revision=Q.REVISION, device="cpu", max_length=Q.MAX_LENGTH, model_kwargs=mk)
    if next(m.parameters()).dtype != getattr(torch, dtype):
        raise SystemExit(f"the model did not load in {dtype}")
    if (m.prompts or {}).get("query") != Q.DEFAULT_INSTRUCTION:
        raise SystemExit("the shipped default instruction differs from the card's text")
    if flags.get("cleanup"):
        gc.collect()
        try:      # give freed malloc pages back to the system (macOS)
            import ctypes
            ctypes.CDLL("libSystem.B.dylib").malloc_zone_pressure_relief(0, 0)
        except Exception as e:  # noqa: BLE001
            say(f"  cleanup: malloc_zone_pressure_relief unavailable ({e})")
    return m


def cmd_run(a):
    opt = a.option
    dtype, batch, flags = OPTIONS[opt]
    out = ROOT / OUT_DIR / f"opt_{opt}.json"
    if out.exists():
        raise SystemExit(f"{out} exists; not overwritten")
    data = json.loads((ROOT / PAIRS).read_text())
    instr = data["instruction"]
    items = data["items"]
    rec = {"option": opt, "dtype": dtype, "batch_size": batch, "flags": flags, "other_heavy_before": heavy_processes()}
    say(f"[{opt}] pid {os.getpid()}; other heavy processes at start: {len(rec['other_heavy_before'])}")
    # 1. start: index and BM25 loaded, no reranker
    ns = argparse.Namespace(index=F.INDEX)
    Q.new_index(ns)
    rec["rss_start_mb"], rec["peak_start_mb"] = rss_mb(), peak_mb()
    say(f"start (index, BM25): rss {rec['rss_start_mb']:.0f} MB, peak {rec['peak_start_mb']:.0f} MB")
    # 2. the current reranker alone, as the runtime has it, its frozen call on each question
    import retrieve as RT
    bge = RT.reranker()
    bge.predict([("warm up", "warm up passage")] * 2, batch_size=2)
    rec["rss_bge_loaded_mb"] = rss_mb()
    bge_s = []
    for n, it in enumerate(items):
        t = time.time()
        bge.predict([tuple(p) for p in it["frozen_pairs"]], batch_size=8)
        bge_s.append(time.time() - t)
        say(f"[bge {n + 1}/{len(items)}] {it['item']} frozen call {len(it['frozen_pairs'])} pairs {bge_s[-1]:.1f}s")
    rec["rss_bge_after_calls_mb"], rec["peak_bge_mb"] = rss_mb(), peak_mb()
    rec["bge_frozen_seconds"] = bge_s
    say(f"bge alone: rss loaded {rec['rss_bge_loaded_mb']:.0f} MB, rss after calls {rec['rss_bge_after_calls_mb']:.0f} MB, "
        f"peak {rec['peak_bge_mb']:.0f} MB")
    # 3. the Qwen model
    t = time.time()
    m = load_qwen(opt)
    rec["load_seconds"] = time.time() - t
    rec["rss_qwen_loaded_mb"], rec["peak_after_load_mb"] = rss_mb(), peak_mb()
    m.predict([("warm up", "warm up passage")] * 2, batch_size=2, prompt=instr)
    rec["rss_qwen_warm_mb"], rec["peak_after_warm_mb"] = rss_mb(), peak_mb()
    say(f"Qwen loaded in {rec['load_seconds']:.1f}s: rss {rec['rss_qwen_loaded_mb']:.0f} MB, after warm-up "
        f"{rec['rss_qwen_warm_mb']:.0f} MB, peak {rec['peak_after_warm_mb']:.0f} MB")
    # 4. scoring: one predict() call per question over its 24 candidates
    tok = m.tokenizer
    res = []
    for n, it in enumerate(items):
        pairs = [tuple(p) for p in it["pairs"]]
        lens = []
        for q, d in pairs:
            ids = tok.apply_chat_template(Q.messages(instr, q, d), tokenize=True)
            lens.append(len(ids["input_ids"]) if hasattr(ids, "keys") else len(ids))
        t = time.time()
        sc = [float(x) for x in m.predict(pairs, batch_size=batch, prompt=instr)]
        secs = time.time() - t
        res.append({"item": it["item"], "scores": sc, "seconds": secs, "truncated": Q.count_over(lens, Q.MAX_LENGTH),
                    "longest": max(lens), "rss_after_mb": rss_mb()})
        say(f"[score {n + 1}/{len(items)}] {it['item']} {len(pairs)} pairs {secs:.1f}s truncated {res[-1]['truncated']} "
            f"longest {max(lens)} rss {res[-1]['rss_after_mb']:.0f} MB peak {peak_mb():.0f} MB")
    rec["rss_end_mb"], rec["peak_end_mb"] = rss_mb(), peak_mb()
    rec["results"] = res
    rec["added_after_load_mb"] = rec["rss_qwen_warm_mb"] - rec["rss_bge_loaded_mb"]
    rec["added_after_load_vs_calls_mb"] = rec["rss_qwen_warm_mb"] - rec["rss_bge_after_calls_mb"]
    rec["added_at_peak_mb"] = rec["peak_end_mb"] - rec["peak_bge_mb"]
    rec["other_heavy_after"] = heavy_processes()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rec, indent=1))
    say(f"[{opt}] added after load {rec['added_after_load_mb']:.0f} MB, at peak {rec['added_at_peak_mb']:.0f} MB, "
        f"seconds median {statistics.median(r['seconds'] for r in res):.2f}; other heavy processes at end: "
        f"{len(rec['other_heavy_after'])}; wrote {out}")


# ------------------------------------------------------------------ report
def kendall(x, y):
    n, s = len(x), 0.0
    for i in range(n):
        for j in range(i + 1, n):
            p = (x[i] - x[j]) * (y[i] - y[j])
            s += (p > 0) - (p < 0)
    return s / (n * (n - 1) / 2)


def order(sc, k=None):
    o = sorted(range(len(sc)), key=lambda j: (-sc[j], j))
    return o if k is None else o[:k]


def cmd_report(a):
    d = ROOT / OUT_DIR
    base = json.loads((d / "opt_a_fp32_b8.json").read_text())
    B = {r["item"]: r["scores"] for r in base["results"]}
    rows = {}
    for f in sorted(d.glob("opt_*.json")):
        r = json.loads(f.read_text())
        mx, taus, set_ch, ord_ch = 0.0, [], 0, 0
        for x in r["results"]:
            b, s = B[x["item"]], x["scores"]
            mx = max(mx, max(abs(u - v) for u, v in zip(b, s)))
            taus.append(kendall(b, s))
            set_ch += set(order(b, 4)) != set(order(s, 4))
            ord_ch += order(b, 4) != order(s, 4)
        sec = [x["seconds"] for x in r["results"]]
        rows[r["option"]] = {
            "added_after_load_mb": round(r["added_after_load_mb"]), "added_at_peak_mb": round(r["added_at_peak_mb"]),
            "rss_start": round(r["rss_start_mb"]), "rss_bge_loaded": round(r["rss_bge_loaded_mb"]),
            "rss_qwen_warm": round(r["rss_qwen_warm_mb"]), "peak_bge": round(r["peak_bge_mb"]), "peak_end": round(r["peak_end_mb"]),
            "max_abs_diff": mx, "tau_median": statistics.median(taus), "tau_min": min(taus),
            "top4_set_changed": set_ch, "top4_order_changed": ord_ch,
            "truncated": sum(x["truncated"] for x in r["results"]),
            "sec_median": F.pctl(sec, 50), "sec_p90": F.pctl(sec, 90),
            "other_heavy": len(r["other_heavy_before"]) + len(r["other_heavy_after"])}
    print(json.dumps(rows, indent=1))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("prepare")
    p = sub.add_parser("run")
    p.add_argument("--option", required=True, choices=list(OPTIONS))
    sub.add_parser("report")
    a = ap.parse_args()
    {"prepare": cmd_prepare, "run": cmd_run, "report": cmd_report}[a.cmd](a)


if __name__ == "__main__":
    main()
