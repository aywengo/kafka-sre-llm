#!/usr/bin/env python3
"""M6 DF: the python half of scripts/m6_d5_funnel.sh, the D5 funnel as a chain (strict count).

On the 50 scenarios of D5 (eval/m6/d5/) it measures how many must points with a covering set are served by the pack of three retrieval
settings, on the same expansions: `frozen`; `r4g2` (the runtime path, kafka_sre_rag.retrieval.select with profile r4 and the grid
setting r4g2, as scripts/m5_r6_packs.py writes r4g2's packs); and `r5a` (r4g2's 24 scored candidates re-scored by the Qwen3 reranker at
full precision, batch 8, setting r5a of scripts/r5_qwen.py), the last one FOR INFORMATION ONLY: outside the memory limit of 1.5 GB.

The counts are those of scripts/m5_b4_served.py (compute, point_flags; there is no second counter). The rule (MILESTONE.md, rules of the
steps): r4g2 goes on to B4 if, on the strict count over the points with a covering set, it gains at least +15.0 percentage points over
frozen, loses no frozen-served point, and its second reranker call takes at most 6.0 s median and 9.0 s at the 90th percentile on 20
D5 questions fixed by a hash rule on their ids. r5a never enters the verdict.

Sub-commands (the chain calls them): synth, gate-qwen, ids, expand, packs, timing, served, manifest, start-check, mark, header.
Pure CPU except `expand` (server calls, 4-bit model) which the chain runs under scripts/run_guarded.py. The server address and key come
from OMLX_URL and OMLX_API_KEY (environment only); neither is printed.
"""
import argparse
import contextlib
import hashlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
os.environ.setdefault("HF_HUB_OFFLINE", "1")
import m5_b4_served as S  # noqa: E402  (the counter, the expansion step, the file helpers)

SETTING = "r4g2"
INFO = "r5a"
GAIN_MIN_PP = 15.0
MEDIAN_MAX_S = 6.0
P90_MAX_S = 9.0
TIMING_N = 20
MEMORY_NOTE = "for information only: outside the memory limit of 1.5 GB"
STAGES = ("expand", "timing", "packs", "served")
PINNED = ("kafka_sre_rag/candidate.json", "kafka_sre_rag/frozen.json", "kafka_sre_rag/profiles.py", "kafka_sre_rag/retrieval.py",
          "kafka_sre_rag/prompts.py", "kafka_sre_rag/config.py", "kafka_sre_rag/generators.py", "scripts/retrieve.py",
          "scripts/retrieval_funnel.py", "scripts/r5_qwen.py", "scripts/run_guarded.py", "scripts/m5_b4_served.py",
          "scripts/m6_d5_funnel.py", "scripts/m6_d5_funnel.sh", "runs/v3/INDEX_FROZEN.json", "runs/m5/RERANKER_R5.json")
RULE_TEXT = (
    "Rule (MILESTONE.md, rules of the steps; fixed before the run). On the strict count over the indexed points of D5 (points with at "
    "least one covering set; a point is served when one whole set of one or two chunks is in the pack), r4g2 goes on to B4 if all hold: "
    "(a) its share of served points minus the frozen retrieval's is at least +15.0 percentage points; (b) it loses no frozen-served "
    "point; (c) its second reranker call takes at most 6.0 s median and at most 9.0 s at the 90th percentile over 20 D5 questions fixed "
    "by a hash rule on their ids (the first 20 by sha256 of the id), uncached, the server idle; (d) it stays within the memory limit of "
    "1.5 GB (r4g2 uses the frozen reranker's instance at its window of 512 tokens, so it adds none). The verdict line is "
    "`VERDICT: r4g2 GOES ON TO B4` or `VERDICT: STOP, NO B4` with the failing conditions. r5a (the Qwen3 reranker at full precision) is "
    f"printed beside it, {MEMORY_NOTE}, and never enters the verdict. The loose count (any chunk of the point's `chunks` in the pack), the "
    "weight-2 split, the split per kind of task and the points served only through a set of two are printed and decide nothing."
)


# ---------------------------------------------------------------- the rule, by code
def pctl(vals, q):
    """q-th percentile (0-100) by linear interpolation (as scripts/retrieval_funnel.py pctl); None for an empty list."""
    v = sorted(vals)
    if not v:
        return None
    x = (len(v) - 1) * q / 100
    lo = int(x)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (x - lo)


def timing_summary(seconds):
    return {"n": len(seconds), "median": pctl(seconds, 50), "p90": pctl(seconds, 90)}


def rule_verdict(cell, timing):
    """cell: res['strict']['all'] of S.compute (frozen against the setting); timing: {'median', 'p90'} of the setting's second call (or
    None). Integer arithmetic on the points; no rounding at the edges. -> {'go': bool, 'conditions': {...}, 'failing': [...]}."""
    N, fr, st, lost = cell["indexed_points"], cell["frozen_served"], cell["r4_served"], cell["lost"]
    gain = N > 0 and 1000 * (st - fr) >= int(GAIN_MIN_PP * 10) * N
    med = timing is not None and timing.get("median") is not None and timing["median"] <= MEDIAN_MAX_S
    p90 = timing is not None and timing.get("p90") is not None and timing["p90"] <= P90_MAX_S
    cond = {"gain": gain, "no_loss": lost == 0, "median": med, "p90": p90}
    why = {"gain": f"gain {_pp(cell)} is under +{GAIN_MIN_PP:.1f} percentage points" if N else "no indexed points",
           "no_loss": f"{lost} frozen-served point(s) lost", "median": "second call median over 6.0 s or not measured",
           "p90": "second call 90th percentile over 9.0 s or not measured"}
    failing = [why[k] for k, v in cond.items() if not v]
    return {"go": not failing, "conditions": cond, "failing": failing}


def verdict_line(v, setting=SETTING):
    return f"VERDICT: {setting} GOES ON TO B4" if v["go"] else "VERDICT: STOP, NO B4 (failing: " + "; ".join(v["failing"]) + ")"


def _pp(c):
    return "n/a" if c.get("diff_pp") is None else f"{c['diff_pp']:+.1f} pp"


def _f(x, d=1, sign=False):
    return "n/a" if x is None else (f"{x:+.{d}f}" if sign else f"{x:.{d}f}")


# ---------------------------------------------------------------- the three settings' tables
def three_way(rubrics, packs):
    """packs: {id: {'frozen': [...], 'r4g2': [...], 'r5a': [...]}}. -> {'r4g2': res, 'r5a': res, 'r5a_vs_r4g2': res}, each a
    S.compute result (the shared counter) of the second against the first."""
    pair = lambda a, b: S.compute(rubrics, {i: {"frozen": p[a], "r4": p[b]} for i, p in packs.items()})
    return {SETTING: pair("frozen", SETTING), INFO: pair("frozen", INFO), "r5a_vs_r4g2": pair(SETTING, INFO)}


def by_kind(rubrics, packs, kinds):
    out = {}
    for k in sorted(set(kinds.values())):
        sub = [r for r in rubrics if kinds[r["id"]] == k]
        out[k] = three_way(sub, {r["id"]: packs[r["id"]] for r in sub})
    return out


def timing_ids(ids, n=TIMING_N):
    """The timing questions: the first n ids by sha256 of the id (a fixed rule that looks at nothing else)."""
    return sorted(ids, key=lambda i: (hashlib.sha256(i.encode()).hexdigest(), i))[:n]


def fmt_row(label, c_fr, c_st):
    return (f"{label:46} | frozen {c_st['frozen_served']:3} ({_f(c_st['frozen_pct'])} %) | served {c_st['r4_served']:3} ({_f(c_st['r4_pct'])} %) | "
            f"gain {_f(c_st['diff_pp'], 1, True)} pp (SE {_f(c_st['se_pp'])}) | gained {c_st['gained']} lost {c_st['lost']}")


def make_text(res, kinds_res, timing, verdict):
    """The printed report: the verdict line first. res: three_way(); timing: {'r4g2': summary, 'r5a': summary}."""
    a, b, c = res[SETTING], res[INFO], res["r5a_vs_r4g2"]
    out = [verdict_line(verdict), "",
           f"{a['items']} scenarios; points outside the counts (no indexed covering set): {a['outside_the_count']['not_indexed']} not_indexed, "
           f"{a['outside_the_count']['no_covering_set']} no_covering_set. r4g2 pack starts with the frozen pack in {a['r4_starts_with_frozen']} of "
           f"{a['items']} scenarios, r5a in {b['r4_starts_with_frozen']} of {b['items']}.", ""]
    cs = a["strict"]["all"]
    out += [f"Strict count (decides), {cs['indexed_points']} indexed points:",
            f"  (a) r4g2 minus frozen = {_pp(cs)} ({cs['r4_served']} - {cs['frozen_served']} of {cs['indexed_points']}; item-clustered standard error "
            f"{_f(cs['se_pp'])}; needs at least +{GAIN_MIN_PP:.1f}): {'holds' if verdict['conditions']['gain'] else 'FAILS'}",
            f"  (b) frozen-served points r4g2 does not serve: {cs['lost']} (needs 0): {'holds' if verdict['conditions']['no_loss'] else 'FAILS'}"]
    t = timing.get(SETTING) or {}
    out += [f"  (c) r4g2 second call over {t.get('n', 0)} D5 questions: median {_f(t.get('median'), 2)} s (needs at most {MEDIAN_MAX_S}): "
            f"{'holds' if verdict['conditions']['median'] else 'FAILS'}; 90th percentile {_f(t.get('p90'), 2)} s (needs at most {P90_MAX_S}): "
            f"{'holds' if verdict['conditions']['p90'] else 'FAILS'}",
            "  (d) memory limit of 1.5 GB: r4g2 reuses the frozen reranker's instance (no second model): holds", ""]
    t5 = timing.get(INFO) or {}
    ci = b["strict"]["all"]
    cr = c["strict"]["all"]
    out += [f"r5a ({MEMORY_NOTE}; decides nothing):",
            f"  strict: r5a minus frozen = {_pp(ci)} ({ci['r4_served']} - {ci['frozen_served']}; SE {_f(ci['se_pp'])}); lost {ci['lost']}",
            f"  strict: r5a minus r4g2 = {_pp(cr)} ({cr['r4_served']} - {cr['frozen_served']}; paired over scenarios, item-clustered SE {_f(cr['se_pp'])}); "
            f"gained {cr['gained']}, lost {cr['lost']}",
            f"  second call over {t5.get('n', 0)} D5 questions: median {_f(t5.get('median'), 2)} s, 90th percentile {_f(t5.get('p90'), 2)} s", ""]
    out.append("Counts, all indexed points and weight-2 points (setting minus frozen; decides nothing beyond the strict line above):")
    for count in S.COUNTS:
        for w, lab in (("all", "all"), ("weight2", "weight 2")):
            for name, r in ((SETTING, a), (f"{INFO} ({MEMORY_NOTE})", b)):
                x = r[count][w]
                line = fmt_row(f"{count} {lab} {name}", None, x)
                if count == "strict":
                    line += f" | only through a set of two: frozen {x['only_through_a_larger_set_frozen']}, setting {x['only_through_a_larger_set_r4']}"
                out.append(line)
    out += ["", "Per kind of task (strict, all indexed points; decides nothing):"]
    for k, kr in kinds_res.items():
        for name, r in ((SETTING, kr[SETTING]), (f"{INFO} ({MEMORY_NOTE})", kr[INFO])):
            out.append(fmt_row(f"{k} {name}", None, r["strict"]["all"]))
    return "\n".join(out)


def item_rows(res, items):
    """Per-item rows (JSON): the strict and loose flags of every indexed point under the three settings."""
    ra, rb = res[SETTING]["rows"], res[INFO]["rows"]
    rows = []
    for x, y in zip(ra, rb):
        assert x["id"] == y["id"]
        pts = []
        for p, q in zip(x["points"], y["points"]):
            assert p["id"] == q["id"]
            pts.append({"id": p["id"], "weight": p["weight"],
                        **{c: {"frozen": p[c]["frozen"], SETTING: p[c]["r4"], INFO: q[c]["r4"]} for c in S.COUNTS}})
        rows.append({"id": x["id"], "kind": items[x["id"]].get("kind"), "indexed": x["indexed"], "points": pts})
    return rows


# ---------------------------------------------------------------- the packs
class StandIn:
    """Dry run: deterministic hash scores, no model. `salt` makes the second reranker differ from the first."""

    def __init__(self, salt=""):
        self.salt = salt
        self.last_truncated = 0

    def predict(self, pairs, batch_size=8):
        return [int(hashlib.sha1((self.salt + q + "\0" + t).encode()).hexdigest()[:8], 16) / 2 ** 32 for q, t in pairs]


class Timed:
    """Wraps a reranker; records the seconds of each predict() call."""

    def __init__(self, inner):
        self.inner, self.calls = inner, []

    def predict(self, pairs, batch_size=8):
        t0 = time.monotonic()
        s = self.inner.predict(pairs, batch_size=batch_size)
        self.calls.append(time.monotonic() - t0)
        return s


class Rerankers:
    """The frozen reranker (the runtime's own instance) and the second one for r5a. Real: bge at 512 and the pinned Qwen at full
    precision (scripts/r5_qwen.py QwenModel, default instruction, max length 1,024, batch 8). Dry: stand-ins."""

    def __init__(self, stand_in):
        from kafka_sre_rag import retrieval
        self.retrieval = retrieval
        self.stand_in = stand_in
        self.orig_at = retrieval._reranker_at
        self.qwen = None
        if stand_in:
            self.bge = StandIn()
            retrieval.RT.reranker = lambda: self.bge
            self.orig_at = lambda L: self.bge
            self.second = StandIn("qwen")
        else:
            import r5_qwen
            self.qwen = r5_qwen.QwenModel()
            self.second = r5_qwen.Bound(self.qwen, r5_qwen.DEFAULT_INSTRUCTION)

    def truncated(self):
        return 0 if self.qwen is None else self.qwen.last_truncated

    def warm_up(self):
        warm = [("warm up", "warm up passage")] * 2
        self.orig_at(512).predict(warm, batch_size=2)
        if self.qwen is not None:
            self.qwen.model.predict(warm, batch_size=2)
        else:
            self.second.predict(warm, batch_size=2)

    @contextlib.contextmanager
    def at(self, setting):
        """Install the second reranker of a setting in the runtime (retrieval._reranker_at) with a timer on it; yield the timer."""
        inner = self.orig_at if setting == SETTING else (lambda L: self.second)
        timer = {}

        def at(L):
            timer.setdefault("t", Timed(inner(L)))
            return timer["t"]
        keep = self.retrieval._reranker_at
        self.retrieval._reranker_at = at
        try:
            yield timer
        finally:
            self.retrieval._reranker_at = keep


def second_call_seconds(timer):
    calls = timer["t"].calls if "t" in timer else []
    return round(sum(calls), 4) if len(calls) == 1 else None


def pack_ids(chunks):
    return [c["id"] for c in chunks]


def cmd_packs(a):
    from kafka_sre_rag import profiles, retrieval
    grid = profiles.grid_settings(SETTING)
    rr = Rerankers(a.stand_in)
    items = S.read_jsonl(a.items)
    exps = {r["id"]: r["expansion"] for r in S.read_jsonl(a.expansions)}
    done = {r["id"] for r in S.read_jsonl(a.out)}
    todo = [it for it in items if it["id"] not in done]
    print(f"[packs] {len(items)} items, {len(done)} already done, {len(todo)} to do; settings frozen, {SETTING}, {INFO} ({MEMORY_NOTE})", flush=True)
    for i, it in enumerate(todo, 1):
        if it["id"] not in exps:
            print(f"[packs] {i}/{len(todo)} id={it['id']} FAILED no stored expansion", flush=True)
            return 1
        q, exp = S.full_question(it), exps[it["id"]]
        frozen = pack_ids(retrieval.select(q, exp))
        out = {"id": it["id"], "frozen": frozen, "seconds": {}}
        for s in (SETTING, INFO):
            with rr.at(s) as timer:
                out[s] = pack_ids(retrieval.select(q, exp, profile="r4", settings=grid))
            out["seconds"][s] = second_call_seconds(timer)
        out["qwen_truncated_pairs_last_call"] = rr.truncated()
        for s in (SETTING, INFO):
            if out[s][:len(frozen)] != frozen or len(set(out[s])) != len(out[s]):
                print(f"[packs] {i}/{len(todo)} id={it['id']} FAILED the {s} pack does not start with the frozen pack (or has duplicates)", flush=True)
                return 1
        S.append_jsonl(a.out, out)
        print(f"[packs] {i}/{len(todo)} id={it['id']} frozen={len(frozen)} {SETTING}={len(out[SETTING])} {INFO}={len(out[INFO])} "
              f"second_call_s={out['seconds'][SETTING]}/{out['seconds'][INFO]} starts_with_frozen=yes", flush=True)
    print(f"[packs] done {len(items)} rows", flush=True)
    return 0


def cmd_timing(a):
    from kafka_sre_rag import profiles, retrieval
    grid = profiles.grid_settings(SETTING)
    rr = Rerankers(a.stand_in)
    items = {it["id"]: it for it in S.read_jsonl(a.items)}
    ids = timing_ids(list(items), a.n)
    exps = {r["id"]: r["expansion"] for r in S.read_jsonl(a.expansions)}
    done = {r["id"] for r in S.read_jsonl(a.out)}
    print(f"[timing] {len(ids)} questions by the hash rule, {len(done)} already done; second call of {SETTING} (bge, window 512) and {INFO} "
          f"(Qwen, {MEMORY_NOTE}), uncached", flush=True)
    rr.warm_up()
    for n, i in enumerate((x for x in ids if x not in done), 1):
        if i not in exps:
            print(f"[timing] {i} FAILED no stored expansion", flush=True)
            return 1
        q = S.full_question(items[i])
        row = {"id": i}
        for s in (SETTING, INFO):
            with rr.at(s) as timer:
                retrieval.select(q, exps[i], profile="r4", settings=grid)
            row[s] = second_call_seconds(timer)
        if row[SETTING] is None or row[INFO] is None:
            print(f"[timing] {i} FAILED the second call did not happen exactly once", flush=True)
            return 1
        S.append_jsonl(a.out, row)
        print(f"[timing] {n}/{len(ids) - len(done)} id={i} {SETTING}={row[SETTING]:.2f}s {INFO}={row[INFO]:.2f}s", flush=True)
    print(f"[timing] done {len(ids)} rows", flush=True)
    return 0


def cmd_ids(a):
    for i in timing_ids([it["id"] for it in S.read_jsonl(a.items)], a.n):
        print(i)
    return 0


# ---------------------------------------------------------------- the counts, the rule, the report and the record
def timing_of(path, ids):
    rows = {r["id"]: r for r in S.read_jsonl(path)}
    if set(rows) != set(ids):
        raise ValueError("the timing rows are not the questions the hash rule names")
    return {s: timing_summary([rows[i][s] for i in ids]) for s in (SETTING, INFO)}


def cmd_served(a):
    from kafka_sre_rag import profiles
    rubrics = S.read_jsonl(a.rubrics)
    items = {it["id"]: it for it in S.read_jsonl(a.items)}
    packs = {r["id"]: {"frozen": r["frozen"], SETTING: r[SETTING], INFO: r[INFO]} for r in S.read_jsonl(a.packs)}
    try:
        res = three_way(rubrics, packs)
        timing = timing_of(a.timing, timing_ids(list(items), a.n))
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    kinds_res = by_kind(rubrics, packs, {i: items[i].get("kind", "") for i in items})
    verdict = rule_verdict(res[SETTING]["strict"]["all"], timing[SETTING])
    text = make_text(res, kinds_res, timing, verdict)
    Path(a.text_out).write_text(text + "\n", encoding="utf-8")
    payload = {"verdict": verdict, "verdict_line": verdict_line(verdict), "rule": RULE_TEXT, "timing": timing,
               "timing_ids": timing_ids(list(items), a.n), "memory_note_r5a": MEMORY_NOTE,
               "strict_r4g2_vs_frozen": res[SETTING]["strict"], "strict_r5a_vs_frozen": res[INFO]["strict"],
               "strict_r5a_vs_r4g2": res["r5a_vs_r4g2"]["strict"], "loose_r4g2_vs_frozen": res[SETTING]["loose"],
               "loose_r5a_vs_frozen": res[INFO]["loose"], "outside_the_count": res[SETTING]["outside_the_count"]}
    Path(a.json_out).write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")
    Path(a.rows_out).write_text(json.dumps(item_rows(res, items), indent=1) + "\n", encoding="utf-8")
    rec = {"setting": SETTING, "profile": "r4", "verdict": "GO" if verdict["go"] else "STOP", "verdict_line": verdict_line(verdict),
           "failing": verdict["failing"], "settings_sha256": profiles.settings_sha256("r4", grid=SETTING),
           "settings": profiles.settings("r4", grid=SETTING), "dry_run": bool(a.dry_run), "date": time.strftime("%Y-%m-%d"),
           "report": str(a.report), "batch": "eval/m6/d5 (50 scenarios)" if not a.dry_run else "synthetic batch of a dry run",
           "decided_by": "scripts/m6_d5_funnel.py rule_verdict (the rule of MILESTONE.md), strict count"}
    Path(a.chosen_out).write_text(json.dumps(rec, indent=1) + "\n", encoding="utf-8")
    print(text)
    return 0


def header_text(a, rubrics):
    n_pts = sum(len(r["must"]) for r in rubrics)
    idx = sum(1 for r in rubrics for p in r["must"] if S.point_sets(p))
    return "\n".join([
        "# M6 DF: the D5 funnel, strict count",
        "",
        f"Written by `scripts/m6_d5_funnel.sh` on {time.strftime('%Y-%m-%d')}"
        f"{' (DRY RUN: synthetic batch, stub model, stand-in rerankers; the numbers are not measurements)' if a.dry_run else ''}. "
        "This header, with the rule, is written before the expansions start.",
        "",
        f"- Batch: {len(rubrics)} scenarios, {n_pts} must points, {idx} indexed points (at least one covering set).",
        f"- Items file sha256 {S.sha256_file(a.items)[:16]}..., rubrics sha256 {S.sha256_file(a.rubrics)[:16]}... (equal to the chunk check).",
        f"- Settings: frozen; {SETTING} (the runtime path, profile r4 with the grid setting {SETTING}); {INFO} ({MEMORY_NOTE}).",
        f"- Expansions: the 4-bit base model `{a.model}`, `prompts.EXPAND_SYSTEM`, the question plus its Evidence part, temperature 0, 300 tokens, "
        "thinking off, one request at a time; the same call as on the development sets and in the B4 chain.",
        "",
        "## The rule (fixed before the run)",
        "",
        RULE_TEXT,
        "",
    ])


def cmd_header(a):
    rubrics = S.read_jsonl(a.rubrics)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    head = header_text(a, rubrics)
    if a.phase == "header":
        out.write_text(head + "\n## Status\n\nrunning: the expansions have not finished.\n", encoding="utf-8")
        print(f"report header written: {out}")
        return 0
    res = json.load(open(a.served_json))
    exps = S.read_jsonl(a.expansions)
    packs = S.read_jsonl(a.packs)
    import statistics
    secs = [r["seconds"] for r in exps]
    body = ["## Result", "", res["verdict_line"], "",
            f"- Expansions: {len(exps)} of {len(rubrics)}; seconds per call median {statistics.median(secs):.1f}, max {max(secs):.1f}.",
            f"- Packs: {len(packs)} scenarios; median pack size frozen {statistics.median(len(r['frozen']) for r in packs):.0f}, {SETTING} "
            f"{statistics.median(len(r[SETTING]) for r in packs):.0f}, {INFO} {statistics.median(len(r[INFO]) for r in packs):.0f} chunks.",
            f"- Timing questions (hash rule, {len(res['timing_ids'])}): {', '.join(res['timing_ids'])}.",
            "", "```", Path(a.served_text).read_text(encoding="utf-8").rstrip(), "```", "",
            f"Per-item rows: `{a.rows}`. The record of the setting that goes on: `{a.chosen}`.", ""]
    title, rest = head.split("\n", 1)
    out.write_text(title + "\n\n" + res["verdict_line"] + "\n" + rest + "\n" + "\n".join(body), encoding="utf-8")   # the verdict line first
    print(f"report written: {out}")
    return 0


# ---------------------------------------------------------------- manifest and the start check
def pinned_state(a):
    files = {p: S.sha256_file(ROOT / p) for p in PINNED}
    for label, p in (("items", a.items), ("rubrics", a.rubrics), ("chunk_check", a.chunk_check)):
        files[f"input:{label}"] = S.sha256_file(p)
    files["index:work/v3/chunks_s1.jsonl"] = S.sha256_file(ROOT / "work/v3/chunks_s1.jsonl")
    return {"files": files, "sha256": S.sha256_text(json.dumps(files, sort_keys=True))}


def options_state(a):
    return {"dry_run": bool(a.dry_run), "model": a.model, "setting": SETTING, "info_setting": INFO}


def cmd_manifest(a):
    path = Path(a.work) / "DF_MANIFEST.json"
    if path.exists():
        print(f"manifest exists, not rewritten: {path}")
        return 0
    from kafka_sre_rag import config, prompts, profiles
    cfg = config.load()
    listing = None
    if a.models_json and Path(a.models_json).exists():
        try:
            for m in json.load(open(a.models_json)).get("data") or []:
                if isinstance(m, dict) and m.get("id") == a.model:
                    listing = m
        except ValueError:
            pass
    pin = json.load(open(ROOT / "runs/m5/RERANKER_R5.json"))
    m = {"harness": "m6_d5_funnel", "created": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "options": options_state(a),
         "pinned": pinned_state(a), "rule": RULE_TEXT, "rule_sha256": S.sha256_text(RULE_TEXT),
         "settings": {SETTING: {"settings_sha256": profiles.settings_sha256("r4", grid=SETTING),
                                "parameters": profiles.settings("r4", grid=SETTING)},
                      INFO: {"label": MEMORY_NOTE, "model": pin["model"], "revision": pin["revision"],
                             "settings_sha256": pin["settings"][INFO]["sha256"], "dtype": "float32", "batch_size": 8}},
         "timing": {"rule": f"the first {TIMING_N} ids by sha256 of the id", "uncached": True, "server_idle": True},
         "expansion": {
             "same_as": "the development sets' expansions and the B4 chain (scripts/eval2.py answer -> evalhooks.run_item; scripts/m5_b4_served.py expand)",
             "messages": "prompts.expand_messages(question + '\\n\\nEvidence:\\n' + evidence)",
             "prompt_constant": "prompts.EXPAND_SYSTEM", "prompt_sha256": S.sha256_text(prompts.EXPAND_SYSTEM),
             "max_tokens": cfg["expansion"]["max_tokens"], "temperature": 0, "extra_body": json.loads(S.EXTRA_BODY),
             "model_requested": a.model, "model_listed": listing, "backend": "stub" if a.dry_run else "openai",
             "one_request_at_a_time": True},
         "frozen_expansion_block": cfg["expansion"]}
    Path(a.work).mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(m, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"manifest written: {path}")
    return 0


def start_problems(a):
    work = Path(a.work)
    mpath = work / "DF_MANIFEST.json"
    markers = sorted(work.glob("*.passed"))
    outputs = [work / n for n in ("expansions.jsonl", "timing.jsonl", "packs.jsonl", "served.json")]
    if not mpath.exists():
        bad = [m.name for m in markers] + [o.name for o in outputs if o.exists()]
        return ([f"stage markers or outputs without a manifest: {', '.join(bad)}"] if bad else []), []
    m = json.load(open(mpath))
    problems = []
    now = pinned_state(a)
    for k in sorted(set(m["pinned"]["files"]) | set(now["files"])):
        if m["pinned"]["files"].get(k) != now["files"].get(k):
            problems.append(f"pinned file changed: {k}: {str(m['pinned']['files'].get(k))[:8]} -> {str(now['files'].get(k))[:8]}")
    for k, v in options_state(a).items():
        if m["options"].get(k) != v:
            problems.append(f"option differs: {k}: {m['options'].get(k)!r} -> {v!r}")
    for mk in markers:
        d = json.load(open(mk))
        if d.get("pin") != m["pinned"]["sha256"]:
            problems.append(f"marker {mk.name} was written under another pin")
        for rel, h in (d.get("outputs") or {}).items():
            p = work / rel
            if not p.exists():
                problems.append(f"marker {mk.name}: output {rel} is missing")
            elif S.sha256_file(p) != h:
                problems.append(f"marker {mk.name}: output {rel} changed since the stage passed")
    return problems, ([f"mv {work} {work}.stale   # then start again (the chain never moves or deletes anything itself)"] if problems else [])


def cmd_start_check(a):
    problems, mv = start_problems(a)
    if problems:
        print("start check REFUSED:", file=sys.stderr)
        for p in problems + mv:
            print(f"  {p}", file=sys.stderr)
        return 2
    n = len(list(Path(a.work).glob("*.passed")))
    print("start check: " + ("fresh work folder" if not (Path(a.work) / "DF_MANIFEST.json").exists()
                             else f"manifest and markers agree with the state now; {n} stage(s) done"))
    return 0


def cmd_mark(a):
    work = Path(a.work)
    m = json.load(open(work / "DF_MANIFEST.json"))
    outs = {o: S.sha256_file(work / o) for o in a.outputs}
    (work / f"{a.stage}.passed").write_text(json.dumps({"stage": a.stage, "passed": True, "pin": m["pinned"]["sha256"], "outputs": outs}) + "\n")
    print(f"marker {a.stage}.passed written")
    return 0


# ---------------------------------------------------------------- the gate on the Qwen reranker, the expansions, the synthetic batch
def cmd_gate_qwen(a):
    """The pinned Qwen revision is in the hub cache (offline). A real run also checks the weights' sha256 against the pin file."""
    import r5_qwen
    try:
        snap = r5_qwen.snapshot_dir()
    except Exception as e:  # not in the cache
        print(f"gate: the pinned Qwen reranker is not in the hub cache ({type(e).__name__})", file=sys.stderr)
        return 1
    if snap.name != r5_qwen.REVISION:
        print("gate: the hub cache holds another revision than the pinned one", file=sys.stderr)
        return 1
    if a.dry_run:
        print(f"gate: pinned Qwen revision {r5_qwen.REVISION[:12]} is in the hub cache (weights' hash not checked in a dry run)")
        return 0
    if not r5_qwen.check_pin(say=lambda *x: None):
        print("gate: the pin file, the revision or the weights' sha256 differ from the pin", file=sys.stderr)
        return 1
    print(f"gate: pinned Qwen revision {r5_qwen.REVISION[:12]} is in the hub cache and its weights equal the pin")
    return 0


def cmd_expand(a):
    return S.cmd_expand(a)


def cmd_synth(a):
    """The synthetic batch of a dry run (the B4 chain's, plus a kind per item); the chosen-setting record that one writes is removed."""
    d = Path(a.dir)
    S.cmd_synth(argparse.Namespace(dir=str(d), items=a.items, flaw=a.flaw, setting=SETTING))
    (d / "RETRIEVAL_CHOSEN.json").unlink()
    items = S.read_jsonl(d / "items.jsonl")
    for k, it in enumerate(items):
        it["kind"] = ("diagnostics", "coding", "architecture")[k % 3]
    ip = d / "items.jsonl"
    ip.write_text("".join(json.dumps(x) + "\n" for x in items), encoding="utf-8")
    cc = json.load(open(d / "CHUNK_CHECK.json"))
    cc["sha256"]["items.jsonl"] = S.sha256_file(ip)
    if a.flaw == "rubrics-hash":
        cc["sha256"]["rubrics.jsonl"] = "0" * 64
    (d / "CHUNK_CHECK.json").write_text(json.dumps(cc, indent=1) + "\n")
    return 0


# ---------------------------------------------------------------- command line
def build_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(s):
        s.add_argument("--items", required=True)
        s.add_argument("--rubrics", required=True)
        s.add_argument("--chunk-check", dest="chunk_check", required=True)
        s.add_argument("--work", required=True)
        s.add_argument("--model", default="Qwen3.8-27B-4bit")
        s.add_argument("--dry-run", dest="dry_run", action="store_true")

    s = sub.add_parser("synth")
    s.add_argument("--dir", required=True)
    s.add_argument("--items", type=int, default=8)
    s.add_argument("--flaw", choices=("none", "rubrics-hash"), default="none")
    s = sub.add_parser("gate-qwen")
    s.add_argument("--dry-run", dest="dry_run", action="store_true")
    s = sub.add_parser("ids")
    s.add_argument("--items", required=True)
    s.add_argument("--n", type=int, default=TIMING_N)
    s = sub.add_parser("expand")
    s.add_argument("--items", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--model", default="Qwen3.8-27B-4bit")
    s.add_argument("--extra-body", default=S.EXTRA_BODY)
    s.add_argument("--stub", action="store_true")
    s.add_argument("--stop-after-items", type=int, default=0)
    s = sub.add_parser("packs")
    s.add_argument("--items", required=True)
    s.add_argument("--expansions", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--stand-in", dest="stand_in", action="store_true")
    s = sub.add_parser("timing")
    s.add_argument("--items", required=True)
    s.add_argument("--expansions", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--n", type=int, default=TIMING_N)
    s.add_argument("--stand-in", dest="stand_in", action="store_true")
    s = sub.add_parser("served")
    s.add_argument("--items", required=True)
    s.add_argument("--rubrics", required=True)
    s.add_argument("--packs", required=True)
    s.add_argument("--timing", required=True)
    s.add_argument("--n", type=int, default=TIMING_N)
    s.add_argument("--json-out", required=True)
    s.add_argument("--text-out", required=True)
    s.add_argument("--rows-out", required=True)
    s.add_argument("--chosen-out", required=True)
    s.add_argument("--report", required=True)
    s.add_argument("--dry-run", dest="dry_run", action="store_true")
    s = sub.add_parser("manifest")
    common(s)
    s.add_argument("--models-json")
    s = sub.add_parser("start-check")
    common(s)
    s = sub.add_parser("mark")
    s.add_argument("--work", required=True)
    s.add_argument("--stage", required=True, choices=STAGES)
    s.add_argument("outputs", nargs="+")
    s = sub.add_parser("header")
    s.add_argument("--phase", choices=("header", "final"), required=True)
    s.add_argument("--items", required=True)
    s.add_argument("--rubrics", required=True)
    s.add_argument("--model", default="Qwen3.8-27B-4bit")
    s.add_argument("--dry-run", dest="dry_run", action="store_true")
    s.add_argument("--out", required=True)
    s.add_argument("--expansions")
    s.add_argument("--packs")
    s.add_argument("--served-json", dest="served_json")
    s.add_argument("--served-text", dest="served_text")
    s.add_argument("--rows")
    s.add_argument("--chosen")
    return p


COMMANDS = {"synth": cmd_synth, "gate-qwen": cmd_gate_qwen, "ids": cmd_ids, "expand": cmd_expand, "packs": cmd_packs, "timing": cmd_timing,
            "served": cmd_served, "manifest": cmd_manifest, "start-check": cmd_start_check, "mark": cmd_mark, "header": cmd_header}


def main(argv=None):
    a = build_parser().parse_args(argv)
    return COMMANDS[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
