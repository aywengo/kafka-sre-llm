#!/usr/bin/env python3
"""Write progress heartbeats for long runs without using an LLM.

Detects whether Q&A generation or LoRA training is running, measures progress
from the output files, and appends a heartbeat to <run-dir>/HEARTBEAT.md and
overwrites <run-dir>/status.json.

  nohup python scripts/heartbeat.py --every 1800 --cap kubernetes=1500 \
      > runs/v1/logs/heartbeat.log 2>&1 &

Run once (no --every) to print a single heartbeat.
"""
import argparse
import json
import re
import shutil
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import yaml

LINE_FMT = """### {ts} — Phase {phase} — {state}
progress:  {progress}
metrics:   {metrics}
eta:       {eta}
resources: {resources}
issues:    {issues}
next:      {next}
needs_human: {needs_human}
"""


def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""


def running(pattern):
    return bool(sh(["pgrep", "-f", pattern]).strip())


def resources():
    out, warn = {}, []
    m = re.search(r"free percentage:\s*(\d+)%", sh(["memory_pressure", "-Q"]))
    if m:
        out["mem_free_pct"] = int(m.group(1))
    m = re.search(r"used = ([\d.]+)([MG])", sh(["sysctl", "-n", "vm.swapusage"]))
    if m:
        gb = float(m.group(1)) / (1024 if m.group(2) == "M" else 1)
        out["swap_gb"] = round(gb, 2)
        if gb > 20:
            warn.append(f"swap {gb:.1f} GB > 20 GB")
    free = shutil.disk_usage(".").free / 2**30
    out["disk_free_gb"] = round(free)
    if free < 100:
        warn.append(f"disk {free:.0f} GB < 100 GB")
    return out, warn


def fmt_dur(minutes):
    if minutes is None:
        return "unknown"
    if minutes < 90:
        return f"~{minutes:.0f} min"
    if minutes < 48 * 60:
        return f"~{minutes / 60:.1f} h"
    return f"~{minutes / 1440:.1f} days"


# ---------- generation ---------------------------------------------------------

def generation_state(args):
    totals = Counter()
    ids = {}
    for line in Path(args.chunks).open():
        c = json.loads(line)
        ids[c["id"]] = c["source"]
        totals[c["source"]] += 1
    done, items = Counter(), 0
    raw = Path(args.raw_qa)
    if raw.exists():
        for line in raw.open():
            if not line.strip():
                continue
            r = json.loads(line)
            src = ids.get(r["chunk_id"])
            if src:
                done[src] += 1
                items += len(r.get("items", []))
    targets = {s: min(n, args.caps.get(s, n)) for s, n in totals.items()
               if s not in args.skip}
    remaining = sum(max(0, targets[s] - done[s]) for s in targets)
    total_done = sum(min(done[s], targets[s]) for s in targets)

    last = None
    log = Path(args.gen_log)
    if log.exists():
        for line in log.read_text(errors="ignore").splitlines()[-200:]:
            if line.strip().startswith("done="):
                last = line.strip()
    return {
        "per_source": {s: f"{min(done[s], targets[s])}/{targets[s]}" for s in sorted(targets)},
        "done": total_done, "target": sum(targets.values()), "remaining": remaining,
        "items": items, "last_log": last,
    }


# ---------- training -----------------------------------------------------------

TRAIN_RE = re.compile(r"Iter (\d+): Train loss ([\d.]+).*?It/sec ([\d.]+).*?Tokens/sec ([\d.]+)"
                      r"(?:.*?Peak mem ([\d.]+) GB)?")
VAL_RE = re.compile(r"Iter (\d+): Val loss ([\d.]+)")


def training_state(args):
    text = Path(args.train_log).read_text(errors="ignore") if Path(args.train_log).exists() else ""
    trains, vals = TRAIN_RE.findall(text), VAL_RE.findall(text)
    total = None
    if Path(args.train_config).exists():
        total = (yaml.safe_load(Path(args.train_config).read_text()) or {}).get("iters")
    st = {"iters_total": total, "vals": [(int(i), float(l)) for i, l in vals]}
    if trains:
        it, loss, its, tps, mem = trains[-1]
        st.update(iter=int(it), train_loss=float(loss), it_per_sec=float(its),
                  tokens_per_sec=float(tps), peak_mem_gb=float(mem) if mem else None)
    if "nan" in text.lower().split("iter")[-1]:
        st["nan"] = True
    return st


# ---------- main ---------------------------------------------------------------

def beat(args):
    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    status_path = run_dir / "status.json"
    prev = json.loads(status_path.read_text()) if status_path.exists() else {}
    now = time.time()
    res, warn = resources()
    issues, needs = list(warn), list(warn)

    gen_on, train_on = running("generate_qa.py"), running("mlx_lm.lora")
    status = {"ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
              "epoch": now, "resources": res}

    if train_on or (not gen_on and Path(args.train_log).exists()):
        t = training_state(args)
        status.update(phase="4 Train", kind="train", train=t)
        it, total = t.get("iter"), t.get("iters_total")
        best = min(t["vals"], key=lambda v: v[1]) if t["vals"] else None
        last3 = [v[1] for v in t["vals"][-4:]]
        if len(last3) == 4 and all(b > a for a, b in zip(last3, last3[1:])):
            issues.append("val loss rose on 3 consecutive evals")
            needs.append("val loss rising — stop and keep best checkpoint?")
        if t.get("nan"):
            issues.append("NaN in training log")
            needs.append("NaN loss")
        eta = (total - it) / t["it_per_sec"] / 60 if it and total and t.get("it_per_sec") else None
        progress = f"iter {it or 0:,} / {total or '?'}" + (f" ({100 * it / total:.0f}%)" if it and total else "")
        val = t["vals"][-1][1] if t["vals"] else "?"
        metrics = f"train loss {t.get('train_loss', '?')}, val loss {val}"
        if best:
            metrics += f" (best {best[1]} @ iter {best[0]})"
        metrics += f", {t.get('it_per_sec', '?')} it/s, {t.get('tokens_per_sec', '?')} tok/s"
        if t.get("peak_mem_gb"):
            metrics += f", peak mem {t['peak_mem_gb']} GB"
        state = "RUNNING" if train_on else ("DONE" if it and total and it >= total else "STOPPED")
        nxt = {"RUNNING": "keep training",
               "DONE": "agent: pick best checkpoint, then Phase 5",
               "STOPPED": "agent: training stopped early — check train.log"}[state]
        if state == "STOPPED":
            needs.append("training stopped before the last iteration")
    else:
        g = generation_state(args)
        status.update(phase="1 Data", kind="generate", gen=g)
        rate = None
        if prev.get("kind") == "generate" and prev.get("epoch"):
            dmin = (now - prev["epoch"]) / 60
            ddone = g["done"] - prev["gen"]["done"]
            if dmin > 1 and ddone >= 0:
                rate = ddone / dmin
        status["gen"]["chunks_per_min"] = round(rate, 2) if rate is not None else None
        eta = g["remaining"] / rate if rate else None
        pct = 100 * g["done"] / max(1, g["target"])
        progress = f"{g['done']:,} / {g['target']:,} chunks ({pct:.1f}%); {g['items']:,} items"
        per = ", ".join(f"{s} {v}" for s, v in g["per_source"].items())
        metrics = (f"{rate:.2f} chunks/min" if rate is not None else "rate: next beat") + f" | {per}"
        if g["last_log"]:
            m = re.search(r"rejected=(\d+).*?verify_parse_fail=(\d+).*?per chunk: (.*?) \|", g["last_log"])
            if m:
                metrics += f" | rejected {m.group(1)}, parse fails {m.group(2)}, {m.group(3)}"
        if gen_on:
            state, nxt = "RUNNING", "keep generating"
            if rate == 0:
                issues.append("no progress since last heartbeat")
                needs.append("generation running but stalled")
        elif g["remaining"] == 0:
            state, nxt = "DONE", "agent: build_dataset.py, then Phase 2"
        else:
            state, nxt = "STOPPED", "agent: generation not running — resume or investigate"
            needs.append("generation stopped before finishing")

    status.update(state=state, needs_human=bool(needs), questions=needs, issues=issues)
    resources_txt = ", ".join(f"{k} {v}" for k, v in res.items())
    entry = LINE_FMT.format(
        ts=status["ts"], phase=status["phase"], state=state, progress=progress,
        metrics=metrics, eta=fmt_dur(eta), resources=resources_txt,
        issues="; ".join(issues) or "none", next=nxt,
        needs_human=("yes — " + "; ".join(needs)) if needs else "no")
    with (run_dir / "HEARTBEAT.md").open("a") as f:
        f.write("\n" + entry)
    status_path.write_text(json.dumps(status, indent=2))
    print(entry, flush=True)
    return state


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", default="runs/v1")
    ap.add_argument("--chunks", default="work/chunks.jsonl")
    ap.add_argument("--raw-qa", default="work/raw_qa.jsonl")
    ap.add_argument("--gen-log", default="runs/v1/logs/generate.log")
    ap.add_argument("--train-log", default="runs/v1/logs/train.log")
    ap.add_argument("--train-config", default="runs/v1/lora.yaml")
    ap.add_argument("--cap", action="append", default=[], metavar="SOURCE=N",
                    help="count at most N chunks of a source toward the target")
    ap.add_argument("--skip", nargs="*", default=["flux"], help="sources not counted")
    ap.add_argument("--every", type=int, help="seconds between beats; omit to run once")
    args = ap.parse_args()
    args.caps = {k: int(v) for k, v in (c.split("=", 1) for c in args.cap)}
    args.skip = set(args.skip)
    while True:
        beat(args)
        if not args.every:
            break
        time.sleep(args.every)


if __name__ == "__main__":
    main()
