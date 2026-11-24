#!/usr/bin/env python3
"""M7 KA2: the helpers of scripts/m7_ka2.sh, the Z3 measurement of the lookup cards (agents/m7-plan.md section 5 (a) and section 7 row Z3,
MILESTONE.md W12, W13, W16, W20). Pure Python, CPU only; no model server, no model.

Per model (qwen: Qwen3.8-27B-4bit; gemma31: gemma-4-31b-it-4bit) the arm L is exactly the stored arm A of the six-arm test (work/m7/six_arm/,
`eval2.py answer --answer-profile thorough --no-thinking --no-verify --replay-packs ... --catalogue ...`) plus `--lookup-cards`, on D5 (50) and
the 116 mechanical items (96 + 20). The no-cards arm is the STORED arm A (W20): its prompts are rebuilt on the CPU and compared before any call.

  rule-text                     the rule (Z3), as written into the report header and printed before any call
  code-pins                     the code files the chain pins
  manifest | start-check | mark the run's state (pinned files, options), its stage markers
  server-gate | stub-status | stub-load | stub-unload   the server gate of scripts/m7_kb2_rule.py (W2 narrowed by W16), called, not copied
  gate-stored                   the stored arm A: files, row counts, manifests (model, profile, replay, items), judge rows, score rows
  dry-stored                    a subset copy of the stored folder for a short dry run (first N items of each part)
  rebuild-check                 the rebuilt prompts (stub, CPU) against the stored rows: prompt_sha256 and pack per id; exit 2 on a difference
  lookup-relation               an L set against its rebuilt L prompts, the stored A rows and A's manifest (exactly A's call plus the cards)
  call-counts | stage-calls     the expected model calls per stage, and the calls a stage's files hold
  estimate                      server hours per stage from the six-arm test's measured rates
  measures                      the measures (measures.json, measures.md)
  rule --measures F             the rule by code; the last line names each model's result

Exit codes: 0 done; 1 a check failed; 2 refused or an input is missing (a missing measure is never a pass).
"""
import argparse
import hashlib
import json
import math
import os
import shutil
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))
import m5_g_rule as G  # noqa: E402  (judged(), the judge's coverage per answer: called, not copied)
import m7_kb2_rule as KB  # noqa: E402  (the server gate with W16 and the stub server)
import m7_six_arm_rule as SA  # noqa: E402  (Z90, LOOKUP_KINDS, ANSWER_LIMIT: imported, never copied)

SCHEMA = 1
MODELS = ("qwen", "gemma31")
MODEL_IDS = {"qwen": "Qwen3.8-27B-4bit", "gemma31": "gemma-4-31b-it-4bit"}
PARTS = ("d5", "ma", "m3")
PART_N = {"d5": 50, "ma": 96, "m3": 20}
Z90 = SA.Z90
LOOKUP_KINDS = SA.LOOKUP_KINDS
IDENT_UPPER_MAX = 0.0      # nonexistent identifiers per 10 answers, L - A: the upper end of the 90 % interval below this
MECH_LOWER_MIN = -3.0      # mechanical all-pass, L - A in percentage points on all 116: the lower end of the 90 % interval above this
TIME_MEDIAN_MAX = 1.0      # seconds per answer, L - A paired over all 166 answers: the median at most this
EB = {"chat_template_kwargs": {"enable_thinking": False}}
JUDGE_SETTING, JUDGE_INSTRUCTION = "deepseek-nothink-12k", "v2"   # the six-arm judge's setting and instruction; A's and L's rows must carry the same
CODE_FILES = ("scripts/m7_ka2.sh", "scripts/m7_ka2_rule.py", "scripts/m7_kb2_rule.py")
# manifest keys of an answer set that may differ between L and A (everything else is the same call)
MANIFEST_FREE = {"config", "m4_config", "run", "started", "ended", "runtime_git_commit", "runtime_git_commits", "runtime_tree_dirty", "reloads",
                 "unmeasured_reasons", "lookup_cards", "model_build", "dry_subset"}

RULE_TEXT = f"""\
**The rule of Z3 (agents/m7-plan.md section 7; MILESTONE.md W20), fixed before any call of this chain.** Per model (Qwen3.8-27B-4bit and
gemma-4-31b-it-4bit), arm L is the six-arm test's arm A call (the thorough path with both switches off, the default prompt, packs replayed,
temperature 0, answer limit {SA.ANSWER_LIMIT} tokens) plus `--lookup-cards` (reference cards of the question's names only, W20, after the excerpts),
on D5 (50 scenarios) and the 116 mechanical items (96 + 20). The no-cards arm is the STORED arm A of `work/m7/six_arm/` (its prompts are rebuilt on
the CPU and compared by sha256 before any call; one difference refuses the run). All measures are L against A, paired by scenario or item.

A model's result is **PASS** when all three hold, else **FAIL**:
1. nonexistent identifiers per 10 answers (the identifier check of the six-arm test, `scripts/eval2_idcheck.py`, on D5): L - A, paired per answer,
   the upper end of the 90 % interval (z = {Z90}) below {IDENT_UPPER_MAX:g};
2. mechanical all-pass on all 116 items, L - A in percentage points, paired per item: the lower end of the 90 % interval above {MECH_LOWER_MIN:g}
   (also printed without the lookup kinds {", ".join(LOOKUP_KINDS)}; that line decides nothing);
3. time: the median of the paired difference of seconds per answer (`seconds.total`, L - A, over all 166 answers) at most +{TIME_MEDIAN_MAX:g} s.

Reported, not decided: judged coverage (DeepSeek, coverage role, instruction v2, two passes; the stored judgements of A, L judged the same way)
with its SE and 90 % interval; the paired differences of `prompt_tokens` and `ttft_s` (median and 90th percentile); the lookup's seconds;
cut and empty answers per arm; cards per answer. A measure that is missing or not a number is an error, never a pass. The outcome line names
each model's result; it decides nothing about publishing (W10, W13). D5 is worn development data; the hidden part is not involved."""


class KaError(RuntimeError):
    pass


def rd(p):
    p = Path(p)
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def load_json(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def sha256_file(p):
    return KB.sha256_file(p)


def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def median(xs):
    xs = [x for x in xs if num(x)]
    return statistics.median(xs) if xs else None


def p90(xs):
    xs = sorted(x for x in xs if num(x))
    return xs[min(len(xs) - 1, int(0.9 * len(xs)))] if xs else None


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def se(xs):
    return statistics.stdev(xs) / math.sqrt(len(xs)) if len(xs) > 1 else None


def paired(d, z=Z90):
    m, s = mean(d), se(d)
    return {"mean": m, "se": s, "n": len(d), "ci90": None if s is None else [m - z * s, m + z * s]}


def sfx(model):
    return "" if model == "qwen" else f"_{model}"


def stored_file(stored, part, model):
    return Path(stored) / f"ans_{part}_A{sfx(model)}.jsonl"


def l_file(work, part, model):
    return Path(work) / f"ans_{part}_L{sfx(model)}.jsonl"


def by_id(rows):
    return {r["id"]: r for r in rows}


# ---------------------------------------------------------------- state
def state(a):
    missing = [p for p in a.pin if not Path(p).is_file()]
    if missing:
        raise ValueError(f"pinned file missing: {missing[0]}")
    return {"schema": SCHEMA, "kind": "dry" if a.dry else "real", "rule_sha256": KB.sha256_text(RULE_TEXT),
            "options": KB.kv(a.opt), "info": KB.kv(a.info), "pins": {p: sha256_file(p) for p in sorted(a.pin)}}


def cmd_manifest(a):
    m = Path(a.work) / "MANIFEST.json"
    if m.exists():
        print(f"manifest exists: {m}")
        return 0
    m.write_text(json.dumps(state(a), indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written: {m}")
    return 0


def cmd_start_check(a):
    """As scripts/m7_kb2_rule.py's start check, on this chain's state (the rule's hash instead of the KB prompts')."""
    saved = KB.state
    KB.state = state
    try:
        return KB.cmd_start_check(a)
    finally:
        KB.state = saved


def cmd_mark(a):
    return KB.cmd_mark(a)


def cmd_code_pins(a):
    pins = [p for p in SA.CODE_PINS if p not in ("scripts/m7_four_way.sh",)] + list(CODE_FILES)
    seen = []
    for p in pins:
        if p not in seen:
            seen.append(p)
    print("\n".join(seen))
    return 0


# ---------------------------------------------------------------- the stored arm A
def cmd_gate_stored(a):
    """The stored arm A of each model: whole, error-free, the call the rule names, and replayed from the given pack files."""
    S = Path(a.stored)
    replay = {"d5": a.replay_d5, "ma": a.replay_ma, "m3": a.replay_m3}
    ids = dict(kv.split("=", 1) for kv in a.model_id)
    bad = []
    items = {}
    for part in PARTS:
        f = S / f"items_{part if part != 'd5' else 'd5_h'}.jsonl"
        if not f.is_file():
            bad.append(f"{f} is missing")
            continue
        items[part] = rd(f)
        if not a.dry and len(items[part]) != PART_N[part]:
            bad.append(f"{f} holds {len(items[part])} items; {PART_N[part]} expected")
    for part in PARTS:
        if part not in items:
            continue
        rsha = sha256_file(replay[part]) if Path(replay[part]).is_file() else None
        isha = sha256_file(S / f"items_{part if part != 'd5' else 'd5_h'}.jsonl")
        want_ids = sorted(i["id"] for i in items[part])
        for model in MODELS:
            f = stored_file(S, part, model)
            mf = Path(str(f) + ".manifest.json")
            if not f.is_file() or not mf.is_file():
                bad.append(f"{f} or its manifest is missing")
                continue
            rows, m = rd(f), load_json(mf)
            if sorted(r["id"] for r in rows) != want_ids:
                bad.append(f"{f.name}: the ids are not those of the items file")
            errs = [r["id"] for r in rows if r.get("error") or not (r.get("text") or "").strip() or not r.get("prompt_sha256") or not r.get("pack")]
            if errs:
                bad.append(f"{f.name}: {len(errs)} rows with an error, no text, no prompt hash or no pack (first {errs[0]})")
            prof = m.get("answer_profile") or {}
            want = {"config": f"m7-six-{part}-A{sfx(model)}", "model_echoed": ids[model], "mode": "default", "extra_body": EB,
                    "replay.answers_sha256": rsha, "items_sha256": isha, "answer_profile.name": "thorough", "answer_profile.thinking": False,
                    "answer_profile.verify": False, "lookup_cards": None, "ended": "set"}
            got = {"config": m.get("config"), "model_echoed": m.get("model_echoed"), "mode": m.get("mode"), "extra_body": m.get("extra_body"),
                   "replay.answers_sha256": (m.get("replay") or {}).get("answers_sha256"), "items_sha256": m.get("items_sha256"),
                   "answer_profile.name": prof.get("name"), "answer_profile.thinking": prof.get("thinking"), "answer_profile.verify": prof.get("verify"),
                   "lookup_cards": m.get("lookup_cards"), "ended": "set" if m.get("ended") else None}
            for k in want:
                if got[k] != want[k]:
                    bad.append(f"{mf.name}: {k} is {str(got[k])[:40]!r}, expected {str(want[k])[:40]!r}")
            if part != "d5":
                sc = S / f"score_{part}_A{sfx(model)}.jsonl"
                if not sc.is_file() or sorted(r["id"] for r in rd(sc)) != want_ids:
                    bad.append(f"{sc.name}: missing or not one score row per item")
    jf = S / "judge_d5.jsonl"
    if not jf.is_file():
        bad.append(f"{jf} is missing")
    elif "d5" in items:
        jr = rd(jf)
        for model in MODELS:
            name = f"A{sfx(model)}"
            got = {(r["id"], r.get("pass")) for r in jr if r.get("set") == name and r.get("valid_reply") and not r.get("reference_given")}
            want = {(i["id"], p) for i in items["d5"] for p in (1, 2)}
            if not want <= got:
                bad.append(f"judge_d5.jsonl: set {name} lacks a valid row for {len(want - got)} of {len(want)} scenario passes")
            si = judge_settings(jr, name)
            if si != {(JUDGE_SETTING, JUDGE_INSTRUCTION)}:
                bad.append(f"judge_d5.jsonl: set {name} was judged with setting/instruction {sorted(si)}, not {(JUDGE_SETTING, JUDGE_INSTRUCTION)}")
    for b in bad[:15]:
        print(f"stored arm A: {b}", file=sys.stderr)
    if bad:
        print(f"REFUSED: the stored arm A in {S} is not what this chain measures against ({len(bad)} problem(s))", file=sys.stderr)
        return 2
    print(f"stored arm A ({S}): {', '.join(f'{p} {len(items[p])}' for p in PARTS)} items; both models whole, the call of arm A, replayed from the given pack files; "
          f"judge rows of A and A_gemma31 complete; score rows complete")
    return 0


def cmd_dry_stored(a):
    """A subset of the stored folder for a short dry run (first N items of each part); manifests carry the subset's item hash and `dry_subset`."""
    src, out = Path(a.src), Path(a.out)
    if (out / "items_d5_h.jsonl").is_file():
        print(f"dry stored folder exists: {out}")
        return 0
    out.mkdir(parents=True, exist_ok=True)
    keep = {}
    for part in PARTS:
        name = f"items_{part if part != 'd5' else 'd5_h'}.jsonl"
        rows = rd(src / name)[: a.n]
        keep[part] = {r["id"] for r in rows}
        (out / name).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
        for model in MODELS:
            f = stored_file(src, part, model)
            sub = [r for r in rd(f) if r["id"] in keep[part]]
            stored_file(out, part, model).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in sub), encoding="utf-8")
            m = load_json(str(f) + ".manifest.json")
            m.update(items_sha256=sha256_file(out / name), items_count=len(rows), dry_subset=a.n)
            Path(str(stored_file(out, part, model)) + ".manifest.json").write_text(json.dumps(m, indent=1) + "\n", encoding="utf-8")
            if part != "d5":
                sc = [r for r in rd(src / f"score_{part}_A{sfx(model)}.jsonl") if r["id"] in keep[part]]
                (out / f"score_{part}_A{sfx(model)}.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in sc), encoding="utf-8")
    shutil.copyfile(src / "packs_frozen_d5.jsonl", out / "packs_frozen_d5.jsonl")
    names = {f"A{sfx(m)}" for m in MODELS}
    jr = [r for r in rd(src / "judge_d5.jsonl") if r.get("set") in names and r["id"] in keep["d5"]]
    (out / "judge_d5.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in jr), encoding="utf-8")
    print(f"dry stored folder {out}: {', '.join(f'{p} {len(keep[p])}' for p in PARTS)} items of {src}")
    return 0


def cmd_rebuild_check(a):
    """The rebuilt prompts (a stub run of the same call on the CPU) against stored rows: equal prompt_sha256 and pack for every id. Exit 2 on a difference."""
    rb = by_id(rd(a.rebuilt))
    bad = []
    for f in a.stored:
        st = by_id(rd(f))
        if sorted(st) != sorted(rb):
            bad.append(f"{Path(f).name}: the ids differ from the rebuilt set")
            continue
        for i in sorted(st):
            if st[i].get("prompt_sha256") != rb[i].get("prompt_sha256") or st[i].get("pack") != rb[i].get("pack"):
                bad.append(f"{Path(f).name} {i}: stored prompt {str(st[i].get('prompt_sha256'))[:12]}, rebuilt {str(rb[i].get('prompt_sha256'))[:12]}"
                           f"{'' if st[i].get('pack') == rb[i].get('pack') else ' (the packs differ)'}")
        print(f"rebuild check {Path(f).name}: {len(st) - sum(1 for b in bad if b.startswith(Path(f).name + ' '))} of {len(st)} prompts rebuilt identically")
    if bad:
        for b in bad[:10]:
            print(f"rebuild check: {b}", file=sys.stderr)
        print(f"REFUSED: {len(bad)} stored prompt(s) of arm A are not rebuilt identically; the stored arm A cannot be the no-cards arm (W20); "
              "nothing was called", file=sys.stderr)
        return 2
    return 0


def cmd_lookup_relation(a):
    """An L set is exactly A's call plus the cards: per id the prompt equals the rebuilt L prompt (cards from the question only), the pack equals A's,
    the prompt equals A's exactly when no card was found, the card count equals the rebuild's, the request body equals the reference's except the
    messages, no reasoning; the manifest equals the reference manifest except the run's identity, the unmeasured reasons and `lookup_cards`."""
    L, RB, A = by_id(rd(a.l)), by_id(rd(a.rebuilt)), by_id(rd(a.a))
    ref = by_id(rd(a.body_ref))
    bad = []
    if not (sorted(L) == sorted(RB) == sorted(A) == sorted(ref)):
        print(f"lookup relation {Path(a.l).name}: the ids differ from the rebuilt or the stored set", file=sys.stderr)
        return 1
    with_cards = 0
    for i in sorted(L):
        r, b, s, rf = L[i], RB[i], A[i], ref[i]
        why = []
        lk = r.get("lookup") or {}
        if not isinstance(r.get("lookup"), dict) or not num(lk.get("cards")):
            why.append("no lookup record")
        elif lk.get("cards") != (b.get("lookup") or {}).get("cards"):
            why.append(f"{lk.get('cards')} cards, the rebuild {(b.get('lookup') or {}).get('cards')}")
        if r.get("prompt_sha256") != b.get("prompt_sha256"):
            why.append("the prompt differs from the rebuilt L prompt")
        if r.get("pack") != s.get("pack"):
            why.append("the pack differs from A's")
        nc = lk.get("cards") or 0
        with_cards += nc > 0
        if (r.get("prompt_sha256") == s.get("prompt_sha256")) != (nc == 0):
            why.append("the prompt is %s A's with %d cards" % ("equal to" if nc else "different from", nc))
        pa, pb = ((rf.get("draft_info") or {}).get("request_body_parts") or {}), ((r.get("draft_info") or {}).get("request_body_parts") or {})
        if not pa or not pb:
            why.append("no request body parts recorded")
        else:
            if pa.get("rest_sha256") != pb.get("rest_sha256"):
                why.append("the rest of the request body (model, temperature, stream flags, extra body) differs")
            if (pa.get("max_tokens"), pa.get("enable_thinking")) != (pb.get("max_tokens"), pb.get("enable_thinking")):
                why.append("the token limit or the thinking switch differs")
            if pb.get("messages_sha256") != r.get("prompt_sha256"):
                why.append("the messages sent are not the recorded prompt")
        di, td = r.get("draft_info") or {}, r.get("tokens_detail") or {}
        if any(num(v) and v > 0 for v in (di.get("reasoning_tokens"), di.get("reasoning_chars"), td.get("draft_reasoning"), td.get("draft_reasoning_chars"))):
            why.append("reasoning tokens or characters above 0")
        if why:
            bad.append(f"{i}: {'; '.join(why)}")
    m, mr = load_json(a.l + ".manifest.json"), load_json(a.ref_manifest)
    for k in sorted(set(m) | set(mr)):
        if k in MANIFEST_FREE:
            continue
        if m.get(k) != mr.get(k):
            bad.append(f"manifest: {k} is {str(m.get(k))[:50]!r}, the reference {str(mr.get(k))[:50]!r}")
    lc = m.get("lookup_cards") or {}
    if not lc.get("enabled") or lc.get("terms") != "question only (replayed packs)":
        bad.append("manifest: lookup_cards is not enabled with the question's names only (W20)")
    extra = [x for x in (m.get("unmeasured_reasons") or []) if x not in (mr.get("unmeasured_reasons") or [])]
    if len(extra) != 1 or not extra[0].startswith("lookup cards"):
        bad.append(f"manifest: the unmeasured reasons add {extra!r}, expected the lookup cards' only")
    for b in bad[:8]:
        print(f"lookup relation {Path(a.l).name} {b}")
    print(f"lookup relation {Path(a.l).name}: {len(L) - sum(1 for b in bad if not b.startswith('manifest'))} of {len(L)} ids as promised "
          f"({with_cards} with cards: a different prompt; {len(L) - with_cards} without: A's prompt); manifest {'as A' if not any(b.startswith('manifest') for b in bad) else 'DIFFERS'}")
    return 1 if bad else 0


# ---------------------------------------------------------------- calls and the estimate
def expected_calls(n):
    out = {f"ans_l_{m}": n["d5"] + n["ma"] + n["m3"] for m in MODELS}
    out["judge"] = len(MODELS) * n["d5"] * 2
    return out


def cmd_call_counts(a):
    c = expected_calls({"d5": a.nd, "ma": a.nma, "m3": a.nm3})
    print(" ".join(f"{k}={v}" for k, v in c.items()))
    return 0


def cmd_stage_calls(a):
    """The model calls a stage's files hold: one draft per answer row without an error (answers), one judge row per answer and pass (judge)."""
    n = 0
    for f in a.files:
        rows = rd(f)
        if a.kind == "answers":
            n += sum(1 for r in rows if not r.get("error") and r.get("prompt_sha256"))
        else:
            n += len(rows)
    ok = n == a.expect
    print(f"calls {a.stage}: {n} (expected {a.expect}){'' if ok else ' => MISMATCH'}")
    return 0 if ok else 1


def guard_hours(logdir, name):
    f = Path(logdir) / f"{name}.guard.json"
    if not f.is_file():
        return None
    import datetime
    g = load_json(f)
    try:
        t0, t1 = (datetime.datetime.fromisoformat(g[k]) for k in ("started", "updated"))
    except (KeyError, ValueError, TypeError):
        return None
    return (t1 - t0).total_seconds() / 3600


def cmd_estimate(a):
    """Server hours per stage from the six-arm test's measured rates: each arm A stage's wall time (its guard record) or, without one, the sum of its
    rows' seconds; the judge's wall time per row (its guard record) or the rows' seconds over the concurrency. A range: the measured rate to +25 %
    (the cards lengthen the prompt by up to 4,000 characters; inferred, not measured), plus about 5 minutes per model load."""
    S = Path(a.stored)
    lines, tot = [], [0.0, 0.0]
    for model in MODELS:
        h = 0.0
        src = []
        for part in PARTS:
            n = len(rd(Path(a.work_items) / f"items_{part if part != 'd5' else 'd5_h'}.jsonl")) if a.work_items else PART_N[part]
            rows = rd(stored_file(S, part, model))
            gh = guard_hours(a.logs, f"m7_four_way.m7-six-{part}-A{sfx(model)}.log") if a.logs else None
            per = (gh / PART_N[part]) if gh is not None else sum((r.get("seconds") or {}).get("total") or 0 for r in rows) / 3600 / max(1, len(rows))
            src.append("guard" if gh is not None else "rows")
            h += per * n
        lo, hi = h + 5 / 60, h * 1.25 + 5 / 60
        tot[0] += lo
        tot[1] += hi
        lines.append(f"ans_l_{model}: {lo:.1f} to {hi:.1f} h (arm A measured {h:.2f} h for these items, from {'/'.join(sorted(set(src)))})")
    jr = rd(S / "judge_d5.jsonl")
    gh = guard_hours(a.logs, "m7_four_way.judge_d5.log") if a.logs else None
    nj = a.judge_rows
    if gh is not None:
        allrows = a.six_judge_rows or len(jr)
        per = gh / allrows
        how = f"guard: {gh:.2f} h for {allrows} rows"
    else:
        per = sum(r.get("seconds") or 0 for r in jr) / 3600 / 3 / max(1, len(jr))
        how = "rows' seconds over concurrency 3"
    lo, hi = per * nj + 5 / 60, per * nj * 1.25 + 5 / 60
    tot[0] += lo
    tot[1] += hi
    lines.append(f"judge: {lo:.1f} to {hi:.1f} h for {nj} rows ({how})")
    lines.append(f"total: {tot[0]:.1f} to {tot[1]:.1f} h of model-server time (inferred from the six-arm test's measured rates; CPU stages a few minutes)")
    print("\n".join(lines))
    return 0


# ---------------------------------------------------------------- the measures
def ident_counts(path):
    d = load_json(path)
    return {r["id"]: int(r["nonexistent"]) for r in d["answers"]}


def judge_settings(rows, name):
    """{(setting, instruction)} of the judge rows of one set."""
    return {(r.get("setting"), r.get("instruction")) for r in rows if r.get("set") == name and not r.get("reference_given")}


def judge_settings_check(path_a, path_l, name_a, name_l):
    """A's and L's judge rows carry one setting and one instruction, the same on both sides (the six-arm judge's): else an error, nothing is compared."""
    sa, sl = judge_settings(rd(path_a), name_a), judge_settings(rd(path_l), name_l)
    if len(sa) != 1 or sa != sl or sa != {(JUDGE_SETTING, JUDGE_INSTRUCTION)}:
        raise KaError(f"the judge rows of {name_a} ({sorted(sa)}) and {name_l} ({sorted(sl)}) do not carry the same single setting and instruction "
                      f"{(JUDGE_SETTING, JUDGE_INSTRUCTION)}; the coverage is not compared")
    return sa.pop()


def judged_p(path, rubrics, name, ids):
    j = G.judged(str(path), rubrics, {name})
    out = {}
    for i in ids:
        if (name, i) not in j:
            raise KaError(f"no valid judge result for {name} {i} in {path}")
        out[i] = j[(name, i)][0]
    return out


def mech_pass(score_paths, items_paths):
    kinds, ids = {}, []
    for f in items_paths:
        for r in rd(f):
            kinds[r["id"]] = r.get("kind")
            ids.append(r["id"])
    rows = {}
    for f in score_paths:
        rows.update(by_id(rd(f)))
    miss = [i for i in ids if i not in rows]
    if miss:
        raise KaError(f"no score row for {len(miss)} mechanical items (first {miss[0]})")
    return {i: int(bool(rows[i]["all_pass"])) for i in ids}, kinds, {i: bool(rows[i].get("format_ok", True)) for i in ids}


def pdiff(lv, av, scale=1.0):
    ids = sorted(set(lv) & set(av))
    d = [scale * (lv[i] - av[i]) for i in ids]
    return d


def dist(d):
    return {"median": median(d), "p90": p90(d), "mean": mean(d), "n": len(d)}


def model_measures(a, model, stub):
    S, W = Path(a.stored), Path(a.work)
    x = sfx(model)
    rowsA = {p: by_id(rd(stored_file(S, p, model))) for p in PARTS}
    rowsL = {p: by_id(rd(l_file(W, p, model))) for p in PARTS}
    for p in PARTS:
        if sorted(rowsA[p]) != sorted(rowsL[p]):
            raise KaError(f"{model} {p}: the L set does not hold the ids of the stored A set")
    d5ids = sorted(rowsA["d5"])
    out = {"model": model, "model_id": MODEL_IDS[model], "n": {p: len(rowsA[p]) for p in PARTS}}
    # 1. nonexistent identifiers per 10 answers (D5)
    ia, il = ident_counts(W / f"idcheck_d5_A{x}.json"), ident_counts(W / f"idcheck_d5_L{x}.json")
    d = pdiff(il, ia, 10.0)
    out["idents"] = {"A_per10": 10 * sum(ia.values()) / len(ia) if ia else None, "L_per10": 10 * sum(il.values()) / len(il) if il else None,
                     "A_total": sum(ia.values()), "L_total": sum(il.values()), "A_checked": len(ia), "L_checked": len(il),
                     "diff_per10": paired(d), "unpaired_ids": sorted(set(ia) ^ set(il))}
    if out["idents"]["unpaired_ids"] or len(ia) != len(d5ids):
        raise KaError(f"{model}: the identifier check skipped answers (error or no text) on one side, or checked not all {len(d5ids)}: "
                      f"{out['idents']['unpaired_ids'][:5] or 'A ' + str(len(ia)) + ', L ' + str(len(il))}; an answer is never dropped from the pairing")
    st = S / f"idcheck_d5_A{x}.txt"
    if st.is_file():
        try:
            out["idents"]["stored_A_total"] = G.parse_idcheck(st)[1]
        except G.RuleError:
            out["idents"]["stored_A_total"] = None
    # 2. judged coverage (reported)
    out["judge_setting"] = list(judge_settings_check(S / "judge_d5.jsonl", W / "judge_d5.jsonl", f"A{x}", f"L{x}"))
    pa = judged_p(S / "judge_d5.jsonl", a.rubrics, f"A{x}", d5ids)
    pl = judged_p(W / "judge_d5.jsonl", a.rubrics, f"L{x}", d5ids)
    out["coverage"] = {"A": mean(list(pa.values())), "L": mean(list(pl.values())), "diff": paired(pdiff(pl, pa))}
    # 3. mechanical all-pass on the 116 items, with and without the lookup kinds
    ma, kinds, fa = mech_pass([S / f"score_{p}_A{x}.jsonl" for p in ("ma", "m3")], [S / "items_ma.jsonl", S / "items_m3.jsonl"])
    ml, _, fl = mech_pass([W / f"score_{p}_L{x}.jsonl" for p in ("ma", "m3")], [S / "items_ma.jsonl", S / "items_m3.jsonl"])
    nol = [i for i in ma if kinds.get(i) not in LOOKUP_KINDS]
    out["mech"] = {"n": len(ma), "A_pass": sum(ma.values()), "L_pass": sum(ml.values()), "diff_pts": paired(pdiff(ml, ma, 100.0)),
                   "n_without_lookup": len(nol), "A_pass_without_lookup": sum(ma[i] for i in nol), "L_pass_without_lookup": sum(ml[i] for i in nol),
                   "diff_pts_without_lookup": paired([100.0 * (ml[i] - ma[i]) for i in nol]),
                   "format_failures": {"A": sum(1 for v in fa.values() if not v), "L": sum(1 for v in fl.values() if not v)}}
    # 4. time, prompt tokens, time to the first token, the lookup's seconds
    allA = {f"{p}:{i}": r for p in PARTS for i, r in rowsA[p].items()}
    allL = {f"{p}:{i}": r for p in PARTS for i, r in rowsL[p].items()}

    def field(rows, get):
        return {k: get(r) for k, r in rows.items()}
    tot = lambda r: (r.get("seconds") or {}).get("total")  # noqa: E731
    sA, sL = field(allA, tot), field(allL, tot)
    if any(not num(v) for v in list(sA.values()) + list(sL.values())):
        raise KaError(f"{model}: an answer has no seconds.total")
    dsec = pdiff(sL, sA)
    d5sec = [sL[f"d5:{i}"] - sA[f"d5:{i}"] for i in d5ids]
    out["time"] = {"seconds_diff": dist(dsec), "seconds_diff_d5": dist(d5sec), "A_median_s": median(list(sA.values())), "L_median_s": median(list(sL.values()))}
    for name, get in (("prompt_tokens", lambda r: (r.get("draft_info") or {}).get("prompt_tokens")),
                      ("ttft_s", lambda r: (r.get("draft_info") or {}).get("ttft_s")),
                      ("cached_prompt_tokens", lambda r: (r.get("draft_info") or {}).get("cached_prompt_tokens"))):
        va, vl = field(allA, get), field(allL, get)
        both = [k for k in va if num(va[k]) and num(vl.get(k))]
        missing = len(va) - len(both)
        if missing and not stub and name != "cached_prompt_tokens":
            raise KaError(f"{model}: {missing} answers lack {name} in L or A (a missing measure is an error)")
        out["time"][name] = {"diff": dist([vl[k] - va[k] for k in both]), "A_median": median([va[k] for k in both]), "L_median": median([vl[k] for k in both]),
                             "missing": missing}
    # the control (reported, not decided): the answers that got no card have A's prompt; how often L's text equals A's there, and the paired seconds
    # there (whether the server's speed changed since the stored run)
    nc = [k for k, r in allL.items() if not ((r.get("lookup") or {}).get("cards") or 0)]
    out["control"] = {"n": len(nc), "same_prompt": sum(1 for k in nc if allL[k].get("prompt_sha256") and allL[k].get("prompt_sha256") == allA[k].get("prompt_sha256")),
                      "same_text": sum(1 for k in nc if (allL[k].get("text") or "") == (allA[k].get("text") or "")),
                      "seconds_diff": dist([sL[k] - sA[k] for k in nc]),
                      "by_part": {p: sum(1 for k in nc if k.startswith(p + ":")) for p in PARTS}}
    lk = [(r.get("lookup") or {}) for r in allL.values()]
    if any(not num(x.get("seconds")) for x in lk):
        raise KaError(f"{model}: an L answer has no lookup seconds")
    out["time"]["lookup_s"] = {"median": median([x["seconds"] for x in lk]), "p90": p90([x["seconds"] for x in lk]), "max": max(x["seconds"] for x in lk)}
    # 5. cut and empty answers, cards per answer
    out["rows"] = {}
    for arm, rows in (("A", rowsA), ("L", rowsL)):
        out["rows"][arm] = {p: {"cut": sum(1 for r in rows[p].values() if r.get("truncated")),
                                "empty": sum(1 for r in rows[p].values() if not (r.get("text") or "").strip())} for p in PARTS}
    cards = {p: [(r.get("lookup") or {}).get("cards") or 0 for r in rowsL[p].values()] for p in PARTS}
    allc = [c for p in PARTS for c in cards[p]]
    out["cards"] = {p: {"median": median(cards[p]), "mean": mean(cards[p]), "max": max(cards[p]), "with_cards": sum(1 for c in cards[p] if c)} for p in PARTS}
    out["cards"]["all"] = {"median": median(allc), "mean": mean(allc), "max": max(allc), "with_cards": sum(1 for c in allc if c)}
    out["chars"] = {"median": median([(r.get("lookup") or {}).get("chars") or 0 for p in PARTS for r in rowsL[p].values()])}
    return out


def cmd_measures(a):
    m = {"schema": SCHEMA, "stub": bool(a.stub), "models": {}}
    try:
        for model in MODELS:
            m["models"][model] = model_measures(a, model, a.stub)
    except (KaError, OSError, KeyError, ValueError, TypeError, G.RuleError) as e:
        print(f"the measures could not be computed: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    W = Path(a.work)
    (W / "measures.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (W / "measures.md").write_text(measures_md(m), encoding="utf-8")
    print(measures_md(m))
    return 0


def f(x, k=2):
    return "n/a" if x is None else f"{x:+.{k}f}" if isinstance(x, float) else str(x)


def iv(p, k=2):
    c = p.get("ci90")
    s_ = "n/a" if p["se"] is None else format(p["se"], f".{k}f")
    c_ = "n/a" if c is None else format(c[0], f"+.{k}f") + " to " + format(c[1], f"+.{k}f")
    return f"{f(p['mean'], k)} (SE {s_}, 90 % interval {c_}, n {p['n']})"


def measures_md(m):
    L = [f"### Measures, L against the stored A (paired){' (from stub data: not a measurement)' if m['stub'] else ''}", ""]
    for model, e in m["models"].items():
        t = e["time"]
        L += [f"**{model}** (`{e['model_id']}`; D5 {e['n']['d5']}, mechanical {e['n']['ma']} + {e['n']['m3']})", "",
              "| Measure | A | L | L - A |", "|---|---|---|---|",
              f"| nonexistent identifiers per 10 answers (D5) | {e['idents']['A_per10']:.2f} ({e['idents']['A_total']} in {e['idents']['A_checked']}) | "
              f"{e['idents']['L_per10']:.2f} ({e['idents']['L_total']} in {e['idents']['L_checked']}) | {iv(e['idents']['diff_per10'])} |",
              f"| judged coverage (D5, two passes) | {e['coverage']['A']:.3f} | {e['coverage']['L']:.3f} | {iv(e['coverage']['diff'], 3)} |",
              f"| mechanical all-pass, all {e['mech']['n']} | {e['mech']['A_pass']} | {e['mech']['L_pass']} | {iv(e['mech']['diff_pts'], 1)} points |",
              f"| mechanical all-pass without the lookup kinds ({e['mech']['n_without_lookup']}; decides nothing) | {e['mech']['A_pass_without_lookup']} | "
              f"{e['mech']['L_pass_without_lookup']} | {iv(e['mech']['diff_pts_without_lookup'], 1)} points |",
              f"| seconds per answer, all 166 (median; paired difference median / 90th pct.) | {f(t['A_median_s'])} | {f(t['L_median_s'])} | "
              f"{f(t['seconds_diff']['median'])} / {f(t['seconds_diff']['p90'])} |",
              f"| seconds per answer, D5 (paired difference median / 90th pct.; printed beside the bar, not decided) | | | {f(t['seconds_diff_d5']['median'])} / {f(t['seconds_diff_d5']['p90'])} |"]
        for k in ("prompt_tokens", "ttft_s", "cached_prompt_tokens"):
            v = t[k]
            L.append(f"| {k} (median; paired difference median / 90th pct.; {v['missing']} not reported) | {f(v['A_median'])} | {f(v['L_median'])} | "
                     f"{f(v['diff']['median'])} / {f(v['diff']['p90'])} |")
        L.append(f"| lookup seconds (median / 90th pct. / max) | | {t['lookup_s']['median']:.5f} / {t['lookup_s']['p90']:.5f} / {t['lookup_s']['max']:.5f} | |")
        for arm in ("A", "L"):
            r = e["rows"][arm]
            L.append(f"| cut / empty answers, arm {arm} (D5, ma, m3) | " + (" | ".join(f"{r[p]['cut']} / {r[p]['empty']}" for p in PARTS)) + " |")
        c = e["cards"]
        L.append(f"| cards per answer (median / mean / max; answers with a card) | D5 {c['d5']['median']} / {c['d5']['mean']:.2f} / {c['d5']['max']}; {c['d5']['with_cards']} | "
                 f"mechanical {c['ma']['with_cards'] + c['m3']['with_cards']} of {e['n']['ma'] + e['n']['m3']} with a card | all {c['all']['median']} / {c['all']['mean']:.2f} / {c['all']['max']}; {c['all']['with_cards']} of 166 |")
        k_ = e["control"]
        L.append(f"| control (reported, not decided): the {k_['n']} answers without a card (A's prompt; D5 {k_['by_part']['d5']}, ma {k_['by_part']['ma']}, m3 {k_['by_part']['m3']}): "
                 f"L's text equal to A's | | {k_['same_text']} of {k_['n']} | seconds, paired median {f(k_['seconds_diff']['median'])} / 90th pct. {f(k_['seconds_diff']['p90'])} |")
        if e["idents"].get("stored_A_total") is not None:
            L.append(f"\nThe stored identifier check of A counted {e['idents']['stored_A_total']} nonexistent identifiers; this chain's check of the same answers {e['idents']['A_total']} "
                     f"({'equal' if e['idents']['stored_A_total'] == e['idents']['A_total'] else 'DIFFERENT: the checker changed since the six-arm test; both arms here use the same checker'}).")
        L.append("")
    return "\n".join(L) + "\n"


def evaluate(e):
    """-> (result, {condition: (holds, text)}). A measure that is missing is an error (KaError), never a pass."""
    i, mch, t = e["idents"]["diff_per10"]["ci90"], e["mech"]["diff_pts"]["ci90"], e["time"]["seconds_diff"]["median"]
    if i is None or mch is None or not num(t):
        raise KaError(f"{e['model']}: a decided measure is missing (identifier interval {i}, mechanical interval {mch}, time median {t})")
    eps = 1e-9
    c = {"identifiers": (i[1] < IDENT_UPPER_MAX - eps, f"nonexistent identifiers per 10, L - A: upper end {i[1]:+.2f} (needs below {IDENT_UPPER_MAX:g})"),
         "mechanical": (mch[0] > MECH_LOWER_MIN + eps, f"mechanical all-pass on all {e['mech']['n']} items, L - A: lower end {mch[0]:+.1f} points (needs above {MECH_LOWER_MIN:g})"),
         "time": (t <= TIME_MEDIAN_MAX + eps, f"seconds per answer, L - A: median {t:+.2f} s (needs at most +{TIME_MEDIAN_MAX:g})")}
    return ("PASS" if all(v[0] for v in c.values()) else "FAIL"), c


def cmd_rule(a):
    m = load_json(a.measures)
    res = {}
    try:
        for model in MODELS:
            e = m["models"][model]
            r, c = evaluate(e)
            res[model] = r
            print(f"{model}: {r}")
            for k, (ok, txt) in c.items():
                print(f"  {'holds' if ok else 'FAILS'}: {txt}")
            print(f"  reported: judged coverage L - A {iv(e['coverage']['diff'], 3)}; without the lookup kinds {iv(e['mech']['diff_pts_without_lookup'], 1)} points; "
                  f"cut L {sum(e['rows']['L'][p]['cut'] for p in PARTS)}, A {sum(e['rows']['A'][p]['cut'] for p in PARTS)}; "
                  f"time on D5 alone: paired median {f(e['time']['seconds_diff_d5']['median'])} s (printed, not decided); "
                  f"control without a card: L's text equal to A's {e['control']['same_text']} of {e['control']['n']}, paired seconds median {f(e['control']['seconds_diff']['median'])}")
    except (KaError, KeyError, TypeError) as e:
        print(f"the rule could not be applied: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    print(f"M7 KA2 LOOKUP CARDS (Z3) DONE{' (STUB DATA)' if m.get('stub') else ''}: " + ", ".join(f"{k} {v}" for k, v in res.items())
          + "; decides nothing about publishing (W10, W13)")
    return 0


def cmd_rule_text(a):
    print(RULE_TEXT)
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("server-gate", "stub-status", "stub-load", "stub-unload"):
        return KB.main(argv)
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("manifest", "start-check"):
        p = sub.add_parser(name)
        p.add_argument("--work", required=True)
        p.add_argument("--pin", action="append", default=[])
        p.add_argument("--opt", action="append", default=[])
        p.add_argument("--info", action="append", default=[])
        p.add_argument("--dry", action="store_true")
    p = sub.add_parser("mark")
    p.add_argument("--work", required=True)
    p.add_argument("--stage", required=True)
    p.add_argument("--out", nargs="*", default=[])
    sub.add_parser("code-pins")
    sub.add_parser("rule-text")
    p = sub.add_parser("gate-stored")
    for x in ("--stored", "--replay-d5", "--replay-ma", "--replay-m3"):
        p.add_argument(x, required=True)
    p.add_argument("--model-id", action="append", default=[f"{k}={v}" for k, v in MODEL_IDS.items()])
    p.add_argument("--dry", action="store_true")
    p = sub.add_parser("dry-stored")
    p.add_argument("--src", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--n", type=int, required=True)
    p = sub.add_parser("rebuild-check")
    p.add_argument("--rebuilt", required=True)
    p.add_argument("--stored", nargs="+", required=True)
    p = sub.add_parser("lookup-relation")
    for x in ("--l", "--rebuilt", "--a", "--body-ref", "--ref-manifest"):
        p.add_argument(x, required=True)
    p = sub.add_parser("call-counts")
    for x in ("--nd", "--nma", "--nm3"):
        p.add_argument(x, type=int, required=True)
    p = sub.add_parser("stage-calls")
    p.add_argument("--stage", required=True)
    p.add_argument("--kind", choices=("answers", "judge"), required=True)
    p.add_argument("--expect", type=int, required=True)
    p.add_argument("--files", nargs="+", required=True)
    p = sub.add_parser("estimate")
    p.add_argument("--stored", required=True)
    p.add_argument("--logs", default=None)
    p.add_argument("--work-items", default=None)
    p.add_argument("--judge-rows", type=int, required=True)
    p.add_argument("--six-judge-rows", type=int, default=900)
    p = sub.add_parser("measures")
    for x in ("--work", "--stored", "--rubrics"):
        p.add_argument(x, required=True)
    p.add_argument("--stub", action="store_true")
    p = sub.add_parser("rule")
    p.add_argument("--measures", required=True)
    a = ap.parse_args(argv)
    return {"manifest": cmd_manifest, "start-check": cmd_start_check, "mark": cmd_mark, "code-pins": cmd_code_pins, "rule-text": cmd_rule_text,
            "gate-stored": cmd_gate_stored, "dry-stored": cmd_dry_stored, "rebuild-check": cmd_rebuild_check, "lookup-relation": cmd_lookup_relation,
            "call-counts": cmd_call_counts, "stage-calls": cmd_stage_calls, "estimate": cmd_estimate, "measures": cmd_measures, "rule": cmd_rule}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
