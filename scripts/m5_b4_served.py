#!/usr/bin/env python3
"""M5 B4 check: the python half of scripts/m5_b4_check.sh (rule X1 of MILESTONE.md, applied once to B4).

Pure CPU. Sub-commands (the chain calls them; each prints one line per item or a short result):

  served       rubrics + packs -> both counts of "served" (strict and loose) per profile, the rule's verdict for the count named by
               --decide, per-item rows as JSON. No default for --decide.
  expand       the 30 search-term expansions on the server (the measured call: prompts.expand_messages(question + Evidence),
               cfg["expansion"]["max_tokens"], temperature 0, thinking off), one line per item, resumable by the output file.
  packs        frozen pack and the pack of the setting under check (profile r4 with that grid setting) per item from the stored
               expansions (kafka_sre_rag.retrieval.select; the reranker), one line per item, resumable; every such pack must start
               with the frozen pack.
  gate-chosen  runs/m6/RETRIEVAL_CHOSEN.json (written by the D5 funnel chain, scripts/m6_d5_funnel.sh): the verdict is GO, the setting is
               the one named by --setting, and its parameters and settings hash equal the grid setting in candidate.json.
  manifest     write this run's manifest once (pinned files, options, expansion settings, model).
  start-check  compare a stored manifest and the stage markers with the state now; exit 2 on any difference.
  mark         write a stage marker with the hash of its output files.
  report       the report runs/m5/B4_CHECK.md: header (before the expansions) or the finished report.
  synth        the synthetic batch of a dry run (items, rubrics, chunk check, chosen-setting record) in a folder.

Definitions (the Owner decided that the strict count decides; the chain accepts --decide strict only, the loose count is still printed):
  A point is "indexed" when it has at least one covering set (chunk_sets); points with empty chunk_sets (not_indexed or
  no_covering_set) are outside both counts.
  strict: a point is served when at least one whole set of it is in the pack.
  loose (the count used on B1 and B2): a point is served when at least one chunk of its `chunks` is in the pack.
Rule X1 (on the indexed points): served by r4 minus served by frozen at least +15.0 percentage points, and at most 5 % of the
frozen-served points not served by r4.

The server address comes from OMLX_URL and the key from OMLX_API_KEY (environment only); neither is printed.
"""
import argparse
import hashlib
import json
import math
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GAIN_MIN_PP = 15.0
LOSS_MAX_PCT = 5.0
COUNTS = ("strict", "loose")
PROFILES = ("frozen", "r4")
RULE_TEXT = (
    "Rule X1 (MILESTONE.md), on the indexed points of B4 (points with at least one covering set): (a) the share of points served by r4 "
    "minus the share served by the frozen retrieval is at least +15.0 percentage points; (b) at most 5 % of the points the frozen "
    "retrieval serves are not served by r4. PASS only if both hold. Two counts of served: strict (at least one whole covering set of the "
    "point is in the pack) and loose (at least one chunk of the point's `chunks` is in the pack, as on B1 and B2). The Owner decided the "
    "strict count; it decides, and the loose count is reported and decides nothing. In this text \"r4\" is the profile that carries the "
    "setting under check (the one the D5 funnel let go on, named in the report)."
)
PINNED = ("kafka_sre_rag/candidate.json", "kafka_sre_rag/frozen.json", "kafka_sre_rag/profiles.py", "kafka_sre_rag/retrieval.py",
          "kafka_sre_rag/prompts.py", "kafka_sre_rag/config.py", "kafka_sre_rag/generators.py", "scripts/retrieve.py",
          "scripts/build_packs_v3.py", "scripts/run_guarded.py", "scripts/m5_b4_served.py", "scripts/m5_b4_check.sh",
          "runs/v3/INDEX_FROZEN.json")
EXTRA_BODY = '{"chat_template_kwargs": {"enable_thinking": false}}'
STAGES = ("expand", "packs", "served")


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
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def append_jsonl(path, row):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def full_question(item):
    """The question as the answer harness passes it to the expansion and the retrieval (scripts/eval2.py full_question)."""
    q = item["question"]
    if item.get("evidence"):
        q += "\n\nEvidence:\n" + item["evidence"]
    return q


# ---------------------------------------------------------------- the counts
def point_sets(pt):
    return [list(s) for s in (pt.get("chunk_sets") or []) if s]


def point_chunks(pt):
    ch = pt.get("chunks")
    if ch:
        return list(ch)
    return sorted({c for s in point_sets(pt) for c in s})


def point_flags(pt, pack):
    """(strict, loose, only_through_a_larger_set) of one indexed point against a pack (ordered chunk ids)."""
    sets, P = point_sets(pt), set(pack)
    strict = any(set(s) <= P for s in sets)
    loose = any(c in P for c in point_chunks(pt))
    only_big = strict and not any(len(s) == 1 and s[0] in P for s in sets)
    return strict, loose, only_big


def clustered_se_pp(rows):
    """Standard error, in percentage points, of (sum of differences / sum of points) with items as the clusters (ratio
    estimator). rows: (points_in_item, difference_in_item). None for fewer than two items with points."""
    rows = [(m, d) for m, d in rows if m > 0]
    n, M = len(rows), sum(m for m, _ in rows)
    if n < 2 or M == 0:
        return None
    R = sum(d for _, d in rows) / M
    ss = sum((d - R * m) ** 2 for m, d in rows)
    return 100.0 * math.sqrt(n / (n - 1) * ss) / M


def _empty_cell():
    return {"indexed": 0, "frozen": 0, "r4": 0, "gained": 0, "lost": 0, "only_big_frozen": 0, "only_big_r4": 0, "per_item": []}


def compute(rubrics, packs):
    """rubrics: list of rubric rows; packs: {id: {"frozen": [...], "r4": [...]}}. Returns the counts and the per-item rows."""
    ids = [r["id"] for r in rubrics]
    if set(ids) != set(packs) or len(set(ids)) != len(ids):
        raise ValueError("the rubrics and the packs do not name the same items (or an id is repeated)")
    cells = {c: {"all": _empty_cell(), "weight2": _empty_cell()} for c in COUNTS}
    outside = {"not_indexed": 0, "no_covering_set": 0, "other": 0}
    items_out, prefix_ok = [], 0
    for r in rubrics:
        pk = packs[r["id"]]
        prefix_ok += pk["r4"][:len(pk["frozen"])] == pk["frozen"]
        row = {"id": r["id"], "indexed": 0, "points": []}
        per = {(c, w): {"m": 0, "d": 0} for c in COUNTS for w in ("all", "weight2")}
        for pt in r["must"]:
            if not point_sets(pt):
                outside["not_indexed" if pt.get("not_indexed") else "no_covering_set" if pt.get("no_covering_set") else "other"] += 1
                continue
            row["indexed"] += 1
            fl = {p: point_flags(pt, pk[p]) for p in PROFILES}
            prow = {"id": pt["id"], "weight": pt.get("weight", 1)}
            for ci, c in enumerate(COUNTS):
                f, g = fl["frozen"][ci], fl["r4"][ci]
                prow[c] = {"frozen": f, "r4": g}
                for w in ("all", "weight2") if pt.get("weight", 1) == 2 else ("all",):
                    cell = cells[c][w]
                    cell["indexed"] += 1
                    cell["frozen"] += f
                    cell["r4"] += g
                    cell["gained"] += g and not f
                    cell["lost"] += f and not g
                    if c == "strict":
                        cell["only_big_frozen"] += fl["frozen"][2]
                        cell["only_big_r4"] += fl["r4"][2]
                    per[(c, w)]["m"] += 1
                    per[(c, w)]["d"] += int(g) - int(f)
            row["points"].append(prow)
        for (c, w), v in per.items():
            cells[c][w]["per_item"].append((v["m"], v["d"]))
        items_out.append(row)
    res = {"items": len(rubrics), "r4_starts_with_frozen": prefix_ok, "outside_the_count": outside, "rows": items_out}
    for c in COUNTS:
        res[c] = {}
        for w, cell in cells[c].items():
            N = cell["indexed"]
            d = cell["r4"] - cell["frozen"]
            res[c][w] = {
                "indexed_points": N, "frozen_served": cell["frozen"], "r4_served": cell["r4"],
                "frozen_pct": 100.0 * cell["frozen"] / N if N else None, "r4_pct": 100.0 * cell["r4"] / N if N else None,
                "gained": cell["gained"], "lost": cell["lost"],
                "diff_points": d, "diff_pp": 100.0 * d / N if N else None, "se_pp": clustered_se_pp(cell["per_item"]),
                "only_through_a_larger_set_frozen": cell["only_big_frozen"] if c == "strict" else None,
                "only_through_a_larger_set_r4": cell["only_big_r4"] if c == "strict" else None,
            }
    return res


def verdict(cell):
    """The two conditions of X1 on one cell of compute(), in integer arithmetic (no rounding at the edges)."""
    N, fr, r4, lost = cell["indexed_points"], cell["frozen_served"], cell["r4_served"], cell["lost"]
    if N == 0:
        return {"gain_holds": False, "loss_holds": False, "pass": False, "note": "no indexed points"}
    gain = 100 * (r4 - fr) >= 15 * N
    loss = 100 * lost <= 5 * fr
    return {"gain_holds": gain, "loss_holds": loss, "pass": gain and loss, "note": ""}


def _f(x, d=1, sign=False):
    return "n/a" if x is None else (f"{x:+.{d}f}" if sign else f"{x:.{d}f}")


def rule_lines(res, count, decides):
    c = res[count]["all"]
    v = verdict(c)
    fr = c["frozen_served"]
    loss_pct = 100.0 * c["lost"] / fr if fr else 0.0
    tag = "decides" if decides else "reported, decides nothing"
    out = [f"{count} count ({tag}), on {c['indexed_points']} indexed points of {res['items']} items:",
           f"  (a) r4 minus frozen = {_f(c['diff_pp'], 1, True)} percentage points ({c['r4_served']} - {fr} of {c['indexed_points']}; "
           f"item-clustered standard error {_f(c['se_pp'])}) (needs at least +{GAIN_MIN_PP:.1f}): {'holds' if v['gain_holds'] else 'FAILS'}",
           f"  (b) of the {fr} points frozen serves, r4 does not serve {c['lost']} = {loss_pct:.1f} % (needs at most {LOSS_MAX_PCT:.0f} %): "
           f"{'holds' if v['loss_holds'] else 'FAILS'}",
           f"  X1 on the {count} count: {'PASS' if v['pass'] else 'FAIL'}" + ("" if decides else " (reported, decides nothing)")]
    if v["note"]:
        out.append("  note: " + v["note"])
    return out


def format_text(res, decide):
    out = [f"B4 served counts: {res['items']} items; r4 pack starts with the frozen pack in {res['r4_starts_with_frozen']} of {res['items']} items; "
           f"points outside both counts: {res['outside_the_count']['not_indexed']} not_indexed, "
           f"{res['outside_the_count']['no_covering_set']} no_covering_set", ""]
    for c in COUNTS:
        for w, label in (("all", "all indexed points"), ("weight2", "weight-2 points")):
            x = res[c][w]
            line = (f"{c:6} | {label:18} | {x['indexed_points']:3} points | frozen {x['frozen_served']:3} ({_f(x['frozen_pct'])} %) | "
                    f"r4 {x['r4_served']:3} ({_f(x['r4_pct'])} %) | gained {x['gained']} lost {x['lost']} | diff {_f(x['diff_pp'], 1, True)} pp "
                    f"(SE {_f(x['se_pp'])})")
            if c == "strict":
                line += f" | served only through a set of two: frozen {x['only_through_a_larger_set_frozen']}, r4 {x['only_through_a_larger_set_r4']}"
            out.append(line)
    out.append("")
    for c in (decide,) + tuple(k for k in COUNTS if k != decide):
        out += rule_lines(res, c, c == decide)
        out.append("")
    out.append(f"X1 verdict (--decide {decide}): {'PASS' if verdict(res[decide]['all'])['pass'] else 'FAIL'}")
    return "\n".join(out)


def cmd_served(a):
    rubrics = read_jsonl(a.rubrics)
    packs = {}
    for r in read_jsonl(a.packs):
        packs[r["id"]] = {"frozen": r["frozen"], "r4": r["r4"]}
    try:
        res = compute(rubrics, packs)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    text = format_text(res, a.decide)
    if a.json_out:
        Path(a.json_out).write_text(json.dumps({"decide": a.decide, "rule": RULE_TEXT, **res,
                                                "verdict": {c: verdict(res[c]["all"]) for c in COUNTS}}, indent=1) + "\n", encoding="utf-8")
    if a.text_out:
        Path(a.text_out).write_text(text + "\n", encoding="utf-8")
    print(text)
    if res["r4_starts_with_frozen"] != res["items"]:
        print("ERROR: not every r4 pack starts with the frozen pack", file=sys.stderr)
        return 1
    return 0


# ---------------------------------------------------------------- the server step: expansions
class StubGen:
    """Dry run: no model. A deterministic expansion made of the words of the question."""
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def chat(self, messages, max_tokens, on_token=None):
        words = []
        for w in messages[1]["content"].replace("\n", " ").split():
            w = w.strip(".,?:;()\"'`")
            if len(w) > 3 and w.lower() not in [x.lower() for x in words]:
                words.append(w)
        return ", ".join(words[:15]) or "kafka, strimzi, broker", 0, False


def _sanitise(msg, base):
    return (msg.replace(base, "<server>") if base else msg)[:300]


def cmd_expand(a):
    sys.path.insert(0, str(ROOT))
    from kafka_sre_rag import config, prompts
    cfg = config.load()
    items = read_jsonl(a.items)
    done = {r["id"] for r in read_jsonl(a.out)}
    todo = [it for it in items if it["id"] not in done]
    base = os.environ.get("OMLX_URL", "")
    if a.stub:
        gen = StubGen()
    else:
        from kafka_sre_rag.generators import OpenAIChatGenerator
        if not base or not os.environ.get("OMLX_API_KEY"):
            print("ERROR: OMLX_URL and OMLX_API_KEY must be set in the environment", file=sys.stderr)
            return 1
        gen = OpenAIChatGenerator(base, a.model, extra_body=json.loads(a.extra_body))
    print(f"[expand] {len(items)} items, {len(done)} already done, {len(todo)} to do", flush=True)
    new = 0
    for i, it in enumerate(todo, 1):
        if a.stop_after_items and new >= a.stop_after_items:
            print(f"[expand] stopping after {new} new items (dry-run option)", flush=True)
            return 1
        t0 = time.monotonic()
        try:
            exp, toks, trunc = gen.chat(prompts.expand_messages(full_question(it)), cfg["expansion"]["max_tokens"])
        except Exception as e:  # recorded in the log, the job stops; a rerun resumes
            print(f"[expand] {i}/{len(todo)} id={it['id']} FAILED {type(e).__name__}: {_sanitise(' '.join(str(e).split()), base)}", flush=True)
            return 1
        dt = time.monotonic() - t0
        append_jsonl(a.out, {"id": it["id"], "expansion": exp, "tokens": toks, "truncated": bool(trunc), "seconds": round(dt, 3)})
        new += 1
        print(f"[expand] {i}/{len(todo)} id={it['id']} seconds={dt:.1f} chars={len(exp)} truncated={'yes' if trunc else 'no'}", flush=True)
    print(f"[expand] done {len(items)} rows", flush=True)
    return 0


# ---------------------------------------------------------------- the packs
class StandInReranker:
    """Dry run: deterministic hash scores, no model. Stands in for both reranker instances."""

    def predict(self, pairs, batch_size=8):
        return [int(hashlib.sha1((q + "\0" + t).encode()).hexdigest()[:8], 16) / 2 ** 32 for q, t in pairs]


def cmd_packs(a):
    sys.path.insert(0, str(ROOT))
    from kafka_sre_rag import profiles, retrieval
    grid = profiles.grid_settings(json.load(open(a.chosen_record, encoding="utf-8"))["setting"])
    if a.stand_in:
        stand = StandInReranker()
        retrieval.RT.reranker = lambda: stand
        retrieval._reranker_at = lambda max_length: stand
    items = read_jsonl(a.items)
    exps = {r["id"]: r["expansion"] for r in read_jsonl(a.expansions)}
    done = {r["id"] for r in read_jsonl(a.out)}
    todo = [it for it in items if it["id"] not in done]
    print(f"[packs] {len(items)} items, {len(done)} already done, {len(todo)} to do", flush=True)
    for i, it in enumerate(todo, 1):
        if it["id"] not in exps:
            print(f"[packs] {i}/{len(todo)} id={it['id']} FAILED no stored expansion", flush=True)
            return 1
        q, exp = full_question(it), exps[it["id"]]
        t0 = time.monotonic()
        frozen = [c["id"] for c in retrieval.select(q, exp)]
        t1 = time.monotonic()
        r4 = [c["id"] for c in retrieval.select(q, exp, profile="r4", settings=grid)]
        t2 = time.monotonic()
        ok = r4[:len(frozen)] == frozen and len(set(r4)) == len(r4)
        if not ok:
            print(f"[packs] {i}/{len(todo)} id={it['id']} FAILED the r4 pack does not start with the frozen pack (or has duplicates)", flush=True)
            return 1
        append_jsonl(a.out, {"id": it["id"], "frozen": frozen, "r4": r4,
                             "seconds": {"frozen": round(t1 - t0, 3), "r4_including_frozen": round(t2 - t1, 3)}})
        print(f"[packs] {i}/{len(todo)} id={it['id']} frozen={len(frozen)} r4={len(r4)} frozen_s={t1 - t0:.1f} r4_s={t2 - t1:.1f} starts_with_frozen=yes",
              flush=True)
    print(f"[packs] done {len(items)} rows", flush=True)
    return 0


# ---------------------------------------------------------------- the gate on the chosen setting
def chosen_problems(record_path, setting, cand=None):
    """Problems with runs/m6/RETRIEVAL_CHOSEN.json (the D5 funnel's record of the setting that goes on); [] when its verdict is GO,
    it names `setting`, and its parameters and settings hash equal the grid setting in candidate.json."""
    sys.path.insert(0, str(ROOT))
    from kafka_sre_rag import profiles
    try:
        rec = json.load(open(record_path, encoding="utf-8"))
    except (OSError, ValueError) as e:
        return [f"{record_path} is missing or unreadable ({type(e).__name__}): run the D5 funnel first"]
    problems = []
    if str(rec.get("verdict", "")).upper() != "GO":
        problems.append(f"the D5 funnel's verdict is not GO (it is {rec.get('verdict')!r}): no B4 check")
    if rec.get("setting") != setting:
        problems.append(f"the record names the setting {rec.get('setting')!r}, not {setting!r}")
    try:
        grid = profiles.grid_settings(setting, cand)
    except ValueError as e:
        return problems + [str(e)]
    want = profiles.settings_sha256("r4", cand, grid=setting)
    if rec.get("settings_sha256") != want:
        problems.append(f"the record's settings_sha256 {str(rec.get('settings_sha256'))[:12]} differs from the grid setting {setting}'s {want[:12]}")
    if "settings" in rec and rec["settings"] != profiles.settings("r4", cand, grid=setting):
        problems.append(f"the record's parameters differ from the grid setting {setting} in candidate.json")
    return problems


def cmd_gate_chosen(a):
    cand = json.load(open(a.candidate, encoding="utf-8")) if a.candidate else None
    pr = chosen_problems(a.record, a.setting, cand)
    for p in pr:
        print(f"gate: {p}", file=sys.stderr)
    if not pr:
        print(f"gate: RETRIEVAL_CHOSEN record: the D5 funnel let {a.setting} go on to B4 and its parameters equal candidate.json")
    return 1 if pr else 0


# ---------------------------------------------------------------- manifest and the start check
def pinned_state(a):
    files = {p: sha256_file(ROOT / p) for p in PINNED}
    for label, p in (("items", a.items), ("rubrics", a.rubrics), ("chunk_check", a.chunk_check), ("chosen_record", a.chosen_record)):
        files[f"input:{label}"] = sha256_file(p)
    files["index:work/v3/chunks_s1.jsonl"] = sha256_file(ROOT / "work/v3/chunks_s1.jsonl")
    return {"files": files, "sha256": sha256_text(json.dumps(files, sort_keys=True))}


def options_state(a):
    return {"decide": a.decide, "dry_run": bool(a.dry_run), "model": a.model, "setting": a.setting}


def cmd_manifest(a):
    path = Path(a.work) / "B4_MANIFEST.json"
    if path.exists():
        print(f"manifest exists, not rewritten: {path}")
        return 0
    sys.path.insert(0, str(ROOT))
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
    m = {"harness": "m5_b4_check", "created": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "options": options_state(a),
         "pinned": pinned_state(a), "rule": RULE_TEXT, "rule_sha256": sha256_text(RULE_TEXT),
         "profile": {"record": str(a.chosen_record), "settings_sha256": profiles.settings_sha256("r4", grid=a.setting),
                     "setting": a.setting, "funnel_verdict": json.load(open(a.chosen_record)).get("verdict_line")},
         "expansion": {
             "same_as": "the development sets' expansions (scripts/eval2.py answer -> evalhooks.run_item; scripts/m4_b2_batch.sh)",
             "messages": "prompts.expand_messages(question + '\\n\\nEvidence:\\n' + evidence)",
             "prompt_constant": "prompts.EXPAND_SYSTEM", "prompt_sha256": sha256_text(prompts.EXPAND_SYSTEM),
             "max_tokens": cfg["expansion"]["max_tokens"], "temperature": 0, "extra_body": json.loads(EXTRA_BODY),
             "model_requested": a.model, "model_listed": listing, "backend": "stub" if a.dry_run else "openai",
             "one_request_at_a_time": True},
         "frozen_expansion_block": cfg["expansion"]}
    Path(a.work).mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(m, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"manifest written: {path}")
    return 0


def start_problems(a):
    work = Path(a.work)
    mpath = work / "B4_MANIFEST.json"
    markers = sorted(work.glob("*.passed"))
    outputs = [work / n for n in ("expansions.jsonl", "packs.jsonl", "served.json")]
    if not mpath.exists():
        bad = [m.name for m in markers] + [o.name for o in outputs if o.exists()]
        return ([f"stage markers or outputs without a manifest: {', '.join(bad)}"] if bad else []), []
    m = json.load(open(mpath))
    problems, mv = [], []
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
            elif sha256_file(p) != h:
                problems.append(f"marker {mk.name}: output {rel} changed since the stage passed")
    if problems:
        mv = [f"mv {work} {work}.stale   # then start again (the chain never moves or deletes anything itself)"]
    return problems, mv


def cmd_start_check(a):
    problems, mv = start_problems(a)
    if problems:
        print("start check REFUSED:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        for l in mv:
            print(f"  {l}", file=sys.stderr)
        return 2
    n = len(list(Path(a.work).glob("*.passed")))
    print(f"start check: {'fresh work folder' if not (Path(a.work) / 'B4_MANIFEST.json').exists() else f'manifest and markers agree with the state now; {n} stage(s) done'}")
    return 0


def cmd_mark(a):
    work = Path(a.work)
    m = json.load(open(work / "B4_MANIFEST.json"))
    outs = {o: sha256_file(work / o) for o in a.outputs}
    (work / f"{a.stage}.passed").write_text(json.dumps({"stage": a.stage, "passed": True, "pin": m["pinned"]["sha256"], "outputs": outs}) + "\n")
    print(f"marker {a.stage}.passed written")
    return 0


# ---------------------------------------------------------------- the report
def header_text(a, rubrics):
    n_pts = sum(len(r["must"]) for r in rubrics)
    idx = sum(1 for r in rubrics for p in r["must"] if point_sets(p))
    return "\n".join([
        "# M5 B4: the one-time check of the setting the D5 funnel let go on (rule X1)",
        "",
        f"Written by `scripts/m5_b4_check.sh` on {time.strftime('%Y-%m-%d')}{' (DRY RUN: synthetic batch, stub model, stand-in reranker; the numbers are not measurements)' if a.dry_run else ''}. "
        "This header is written before the expansions start.",
        "",
        f"- Decision count (`--decide`): **{a.decide}** (the Owner decided the strict count); both counts are always printed.",
        f"- Setting under check: **{a.setting}** (profile r4 with that grid setting), as the D5 funnel's record `{Path(a.chosen_record).name}` names it.",
        f"- Batch: {len(rubrics)} scenarios, {n_pts} must points, {idx} indexed points (at least one covering set).",
        f"- Items file sha256 {sha256_file(a.items)[:16]}..., rubrics sha256 {sha256_file(a.rubrics)[:16]}... (equal to the chunk check).",
        f"- Expansions: the 4-bit base model `{a.model}`, `prompts.EXPAND_SYSTEM`, the question plus its Evidence part, temperature 0, 300 tokens, "
        "thinking off, one request at a time; as on the development sets.",
        "",
        "## The rule (fixed before the run)",
        "",
        RULE_TEXT,
        "",
    ])


def cmd_report(a):
    rubrics = read_jsonl(a.rubrics)
    head = header_text(a, rubrics)
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    if a.phase == "header":
        out.write_text(head + "\n## Status\n\nrunning: the expansions have not finished.\n", encoding="utf-8")
        print(f"report header written: {out}")
        return 0
    res = json.load(open(a.served_json))
    exps = read_jsonl(a.expansions)
    secs = [r["seconds"] for r in exps]
    sys.path.insert(0, str(ROOT))
    from kafka_sre_rag import profiles
    bad = sum(1 for r in exps if profiles.clean_terms(r["expansion"])[1])
    packs = read_jsonl(a.packs)
    body = [
        "## Result", "",
        f"- Expansions: {len(exps)} of {len(rubrics)}; seconds per call median {statistics.median(secs):.1f}, max {max(secs):.1f}; "
        f"{bad} flagged malformed by `profiles.clean_terms` (information only).",
        f"- Packs: {len(packs)} items; r4 pack starts with the frozen pack in {res['r4_starts_with_frozen']} of {res['items']}; "
        f"median pack size frozen {statistics.median(len(r['frozen']) for r in packs):.0f}, r4 {statistics.median(len(r['r4']) for r in packs):.0f} chunks.",
        f"- Outside both counts (empty chunk_sets): {res['outside_the_count']['not_indexed']} not_indexed points and "
        f"{res['outside_the_count']['no_covering_set']} no_covering_set points.",
        "", "```", Path(a.served_text).read_text(encoding="utf-8").rstrip(), "```", "",
        f"Per-item rows: `{a.served_json}`.", "",
        f"**X1 verdict (--decide {a.decide}): {'PASS' if res['verdict'][a.decide]['pass'] else 'FAIL'}.** "
        "This is a funnel check on B4 only; the same direction in the hidden funnel totals (the second half of X1) is not part of it.", ""]
    out.write_text(head + "\n" + "\n".join(body), encoding="utf-8")
    print(f"report written: {out}")
    return 0


# ---------------------------------------------------------------- the synthetic batch of a dry run
def cmd_synth(a):
    """Items, rubrics, chunk check and the chosen-setting record in a folder, from the real index (no B4 question is used)."""
    sys.path.insert(0, str(ROOT))
    from kafka_sre_rag import profiles, retrieval
    d = Path(a.dir)
    d.mkdir(parents=True, exist_ok=True)
    chunks = retrieval.RT.index(str(ROOT / "work/v3/chunks_s1.jsonl"))[0]
    pick = [chunks[i] for i in range(50, len(chunks), max(1, len(chunks) // (a.items + 6))) if len(chunks[i]["text"]) > 300][: a.items + 6]
    items, rubrics = [], []
    for k in range(a.items):
        c = pick[k]
        words = " ".join(c["text"].split()[:12])
        it = {"id": f"syn-{k + 1:02d}", "question": f"Synthetic question {k + 1}: what does the documentation say about {c['title'] or c['path']}? {words}",
              "tier": "B", "kind": "open", "family": f"syn-{k + 1:02d}", "domain": "synthetic"}
        if k % 2 == 0:
            it["evidence"] = "kubectl get pods; synthetic evidence line"
        items.append(it)
        c2, c3, c4 = pick[(k + 1) % len(pick)], pick[(k + 2) % len(pick)], pick[(k + 3) % len(pick)]
        must = [{"id": "m1", "text": "synthetic point one", "weight": 2, "first": True, "chunks": [c["id"]], "chunk_sets": [[c["id"]]]},
                {"id": "m2", "text": "synthetic point two", "weight": 1, "chunks": [c2["id"], c3["id"]], "chunk_sets": [[c2["id"], c3["id"]]]},
                {"id": "m3", "text": "synthetic point three", "weight": 1, "chunks": [c4["id"], c["id"]], "chunk_sets": [[c4["id"]], [c["id"], c4["id"]]]},
                {"id": "m4", "text": "synthetic not indexed", "weight": 1, "not_indexed": True, "chunks": [], "chunk_sets": []}]
        if k % 3 == 0:
            must.append({"id": "m5", "text": "synthetic no covering set", "weight": 1, "chunks": [c2["id"]], "chunk_sets": [],
                         "no_covering_set": True})
        rubrics.append({"id": it["id"], "must": must, "must_not": [], "any_of": [], "covered_chunks": sorted({x for p in must for x in p["chunks"]})})
    flaw = a.flaw
    ip, rp = d / "items.jsonl", d / "rubrics.jsonl"
    ip.write_text("".join(json.dumps(x) + "\n" for x in items), encoding="utf-8")
    rp.write_text("".join(json.dumps(x) + "\n" for x in rubrics), encoding="utf-8")
    cc = {"note": "synthetic chunk check of a dry run", "sha256": {"rubrics.jsonl": sha256_file(rp), "items.jsonl": sha256_file(ip)}}
    if flaw == "rubrics-hash":
        cc["sha256"]["rubrics.jsonl"] = "0" * 64
    (d / "CHUNK_CHECK.json").write_text(json.dumps(cc, indent=1) + "\n")
    setting = a.setting
    rec = {"setting": setting if flaw != "other-setting" else "r4g4", "profile": "r4",
           "verdict": "GO" if flaw != "stop" else "STOP",
           "verdict_line": "VERDICT: r4g2 GOES ON TO B4 (synthetic record of a dry run)" if flaw != "stop" else "VERDICT: STOP, NO B4 (synthetic record of a dry run)",
           "settings_sha256": profiles.settings_sha256("r4", grid=setting), "settings": profiles.settings("r4", grid=setting)}
    (d / "RETRIEVAL_CHOSEN.json").write_text(json.dumps(rec, indent=1) + "\n")
    print(f"synthetic batch in {d}: {len(items)} items, flaw={flaw}")
    return 0


def cmd_gate_files(a):
    """The hashes of the two batch files against the chunk check; exit 1 and a message on a difference."""
    cc = json.load(open(a.chunk_check))["sha256"]
    bad = []
    for name, path in (("rubrics.jsonl", a.rubrics), ("items.jsonl", a.items)):
        got = sha256_file(path)
        if cc.get(name) != got:
            bad.append(f"{name}: sha256 {got[:12]} differs from the chunk check {str(cc.get(name))[:12]}")
    for b in bad:
        print(f"gate: {b}", file=sys.stderr)
    if not bad:
        print("gate: rubrics.jsonl and items.jsonl equal the hashes of the chunk check")
    return 1 if bad else 0


# ---------------------------------------------------------------- command line
def build_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(s):
        s.add_argument("--items", required=True)
        s.add_argument("--rubrics", required=True)
        s.add_argument("--chunk-check", dest="chunk_check", required=True)
        s.add_argument("--chosen-record", dest="chosen_record", required=True)
        s.add_argument("--setting", required=True)
        s.add_argument("--work", required=True)
        s.add_argument("--decide", required=True, choices=COUNTS)
        s.add_argument("--model", default="Qwen3.8-27B-4bit")
        s.add_argument("--dry-run", dest="dry_run", action="store_true")

    s = sub.add_parser("served")
    s.add_argument("--rubrics", required=True)
    s.add_argument("--packs", required=True)
    s.add_argument("--decide", required=True, choices=COUNTS)
    s.add_argument("--json-out")
    s.add_argument("--text-out")
    s = sub.add_parser("expand")
    s.add_argument("--items", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--model", default="Qwen3.8-27B-4bit")
    s.add_argument("--extra-body", default=EXTRA_BODY)
    s.add_argument("--stub", action="store_true")
    s.add_argument("--stop-after-items", type=int, default=0)
    s = sub.add_parser("packs")
    s.add_argument("--items", required=True)
    s.add_argument("--expansions", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--chosen-record", dest="chosen_record", required=True)
    s.add_argument("--stand-in", dest="stand_in", action="store_true")
    s = sub.add_parser("gate-chosen")
    s.add_argument("--record", required=True)
    s.add_argument("--setting", required=True)
    s.add_argument("--candidate", help="a candidate.json to check against (tests only; default kafka_sre_rag/candidate.json)")
    s = sub.add_parser("gate-files")
    s.add_argument("--items", required=True)
    s.add_argument("--rubrics", required=True)
    s.add_argument("--chunk-check", dest="chunk_check", required=True)
    s = sub.add_parser("manifest")
    common(s)
    s.add_argument("--models-json")
    s = sub.add_parser("start-check")
    common(s)
    s = sub.add_parser("mark")
    s.add_argument("--work", required=True)
    s.add_argument("--stage", required=True, choices=STAGES)
    s.add_argument("outputs", nargs="+")
    s = sub.add_parser("report")
    s.add_argument("--phase", choices=("header", "final"), required=True)
    s.add_argument("--items", required=True)
    s.add_argument("--rubrics", required=True)
    s.add_argument("--decide", required=True, choices=COUNTS)
    s.add_argument("--model", default="Qwen3.8-27B-4bit")
    s.add_argument("--setting", required=True)
    s.add_argument("--chosen-record", dest="chosen_record", required=True)
    s.add_argument("--dry-run", dest="dry_run", action="store_true")
    s.add_argument("--out", required=True)
    s.add_argument("--expansions")
    s.add_argument("--packs")
    s.add_argument("--served-json", dest="served_json")
    s.add_argument("--served-text", dest="served_text")
    s = sub.add_parser("synth")
    s.add_argument("--dir", required=True)
    s.add_argument("--items", type=int, default=8)
    s.add_argument("--flaw", choices=("none", "rubrics-hash", "stop", "other-setting"), default="none")
    s.add_argument("--setting", default="r4g2")
    return p


COMMANDS = {"served": cmd_served, "expand": cmd_expand, "packs": cmd_packs, "gate-chosen": cmd_gate_chosen, "gate-files": cmd_gate_files,
            "manifest": cmd_manifest, "start-check": cmd_start_check, "mark": cmd_mark, "report": cmd_report, "synth": cmd_synth}


def main(argv=None):
    a = build_parser().parse_args(argv)
    return COMMANDS[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
