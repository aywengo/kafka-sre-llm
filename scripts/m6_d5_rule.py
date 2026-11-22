#!/usr/bin/env python3
"""M6 DA-chain helper: the D5 answers chain (scripts/m6_d5_answers.sh). Pure Python, CPU only; no model server and no model is used here.

The question: does a grounded prompt (grounded-g2 or grounded-g3) on the packs of the retrieval setting that the D5 funnel let go on (r4g2) hold
its coverage against today's default on the frozen packs, so that the combination goes on to the hidden look? The rule is fixed in RULE_TEXT and
applied by code (`rule`); a measure that is missing, not a number or not computed is an error, never a pass.

  rule-text                                    the rule, as written into the report header before any answer exists
  chosen-setting --record F                    print the setting named by the funnel's record (exit 2 when unreadable)
  gate-chosen --record F --dry|--real          the record: verdict GO, parameters and settings hash equal candidate.json, not a dry-run record in a real run
  gate-funnel --funnel-work D --items F --rubrics F --record F --used F [--dry]
                                               the funnel's stage markers and their outputs unchanged, its model, the used marker, the pack rows
  gate-b4 --used F --b4-work D --record F [--dry]
                                               the B4 check ran for the same setting, decide strict, and passed
  d5-packs --funnel-work D --setting S --out-frozen F --out-chosen F
                                               the D5 packs of the funnel in the replay format (one row per scenario, with a sidecar manifest)
  mech-packs --items F --stored F --out F --setting S [--stand-in]
                                               the chosen setting's packs for the mechanical sample, by the runtime path; each starts with the stored pack
  harness-items / select / check-answers / packs-equal / judge-check / check-echo   (scripts/m5_g_rule.py, called as it is, see the chain)
  manifest | start-check ...                   the chain's manifest and the start check (the state of the pinned files, options and markers)
  mark                                         the stage marker (scripts/m5_g_rule.py mark)
  prompts-check --items F --answers F --mode M
                                               every answer row's prompt hash is the hash of the mode's own prompt on its pack, and differs from the default prompt on it
  prompts-differ --a F --b F --expect N        every id common to the two answer files has a recorded prompt hash and a different one
  gate-first-call                              the stored default runs (B1, B2) are there; their median seconds.retrieval are printed (a cross-check, decides nothing)
  gate-b4-override                             with --without-b4: refuse when a B4 marker exists and its verdict is not a pass
  retrieval-timing --items F --funnel-work D --setting S --out F [--stand-in]
                                               the whole retrieval of each side, timed uncached on the funnel's 20 timing questions from its stored expansions (CPU)
  code-pins                                    the code files the chain pins (answer, mech-packs, judge, scorer and identifier-check paths; checked by an import trace in the tests)
  measures --work D ...                        the measures from the files of the chain; writes measures.json and measures.md
  rule --measures F                            the rule by code: PASS or FAIL per condition and variant, CHOICE: ..., and the choice with (b) on the frozen pack (reported)
  candidate --measures F ...                   write runs/m6/CANDIDATE_CHOSEN.json (only for a choice that names a mode; records both choices)
  unjudged-note | code-change-note             report blocks

Exit codes: 0 done; 1 a check failed; 2 refused or an input is missing (a missing input is never a pass).
"""
import argparse
import datetime
import json
import math
import os
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))
os.environ.setdefault("HF_HUB_OFFLINE", "1")
import m5_g_rule as G  # noqa: E402  (the rule helpers of the development check: called, not copied)
import m5_b4_served as S  # noqa: E402  (the counter of served points, the file helpers)

SCHEMA = 1
VARIANTS = ("g2", "g3")                          # G2 before G3: the tie goes to G2
MODES = {"g2": "grounded-g2", "g3": "grounded-g3"}
PROMPT_KEYS = {"g2": "GROUNDED_G2", "g3": "GROUNDED_G3"}
COVERAGE_MARGIN = 0.05
MISS_MARGIN = 1.0
CUT_MAX_PCT = 10                                  # at most 10 % of the D5 answers cut at the token limit (integer arithmetic)
SECONDS_MAX = 60.0
MECH_MIN = -3.0
TIE = 0.02
STORED_RETRIEVAL_FILES = ("work/m4/b1/ans_default.jsonl", "work/m4/b2/ans_default.jsonl")   # stored default runs on the frozen retrieval (a cross-check of the timing stage)
TIMING_N = 20
CODE_PINS = tuple(f"kafka_sre_rag/{n}" for n in ("__init__.py", "__main__.py", "candidate.json", "catalogue.py", "catalogue_a3.py", "checker.py", "config.py",
                                                   "destructive_patterns.json", "evalhooks.py", "frozen.json", "generators.py", "pipeline.py", "profiles.py",
                                                   "prompts.py", "retrieval.py", "revise.py", "service.py")) + tuple(
    f"scripts/{n}" for n in ("build_packs_v3.py", "retrieve.py", "eval2.py", "eval2_checks_a3.py", "eval2_checks_a3_client.py", "eval2_compare.py", "eval2_idcheck.py",
                             "eval2_judge.py", "eval2_score.py", "run_guarded.py", "m5_g_rule.py", "m5_b4_served.py", "m6_d5_funnel.py", "m6_d5_rule.py", "m6_d5_answers.sh"))
CODE_FILES = ("m6_d5_answers.sh", "m6_d5_rule.py")   # the chain's own code: the only pins whose change --accept-code-change can accept
ANSWER_STAGES = ("reference", "answers")
FUNNEL_STAGES = ("expand", "timing", "packs", "served")
DONE_LINES = ("CHOICE: grounded-g2", "CHOICE: grounded-g3", "CHOICE: neither")

RULE_TEXT = f"""\
## The rule of the D5 answers check (fixed before the first answer; MILESTONE.md, rules of the steps, M6; agents/m6-plan.md)

The 50 scenarios of D5 (eval/m6/d5) and the 116 mechanical items of the development sample (eval/m4/g_dev_ids.json: M1 56, M2 40, M3 20) are answered
with the 4-bit model, thinking off, 2,048-token limit, one request at a time, by the same call except the mode and the packs:
reference = the default mode on the FROZEN packs (answered again in this run, never taken from a stored run);
G2 = grounded-g2 and G3 = grounded-g3, each on the packs of the retrieval setting the D5 funnel let go on (the stored frozen pack first, then the chunks the setting adds).
The three D5 answer sets are judged by the development check's judge (coverage role, instruction v2, two passes). A variant is eligible only if all of these hold:
1. (a) Judged coverage P on D5 (50 answers, mean of two judge passes): P(V) - P(reference) >= -{COVERAGE_MARGIN:.2f}.
2. (b) Supported essential-miss units per 10 answers on D5: V <= reference + {MISS_MARGIN:.1f}.
3. (c) Answers cut at the token limit: at most {CUT_MAX_PCT} % of the D5 answers.
4. (d) Median seconds per D5 answer with search at most {SECONDS_MAX:.0f}: this run's answer seconds (replayed packs, so the answer call only), plus the scenario's
   search-terms seconds from the funnel's expansions, plus the median whole-retrieval seconds of that side's setting, measured in this chain before the reference: on the
   funnel's {TIMING_N} timing questions (the first {TIMING_N} ids by sha256 of the id), from the funnel's stored expansions, no server call, each whole call uncached and timed (the
   frozen retrieval for the reference; retrieval.select with profile r4 and the grid setting for a variant: BM25 term queries, both reranker calls and the lookup of the setting),
   the machine otherwise idle. Cross-check, deciding nothing: the median seconds.retrieval of the stored default runs on the frozen retrieval (B1 and B2) is printed beside it.
5. (e) Nonexistent identifiers per 10 D5 answers (scripts/eval2_idcheck.py): V <= reference.
6. (f) Projected change of the mechanical score >= {MECH_MIN:.1f} points: (dM1 + 4.875 dM2 + 4.4 dM3) / 339 * 100, d = all-pass count of V minus the reference's on the
   same items (the weights of scripts/m5_g_rule.py: population over sample size per part).
Condition (b) stands as written, on each answer's own pack. Reported beside it, deciding nothing unless the Owner amends the rule before the result is known: the same
measure on the FROZEN pack for the reference and for each variant (a miss is supported when a chunk of its point is in the frozen pack, which is the prefix of every chosen
pack). Why: the variants' packs hold up to 10 chunks and the reference's 6, so with the own pack the two sides stand on different bases; the frozen-pack figure puts them on
one. The choice that rule would give is printed after the CHOICE line as "CHOICE with (b) on the frozen pack (...): ..."; the last line stays the CHOICE line of the rule as written.
Choice: the eligible variant with the higher coverage; if both are eligible and their coverage differs by less than {TIE:.2f}, G2. If none is eligible: neither
(the combination stops and the Owner sees it).
Reported, deciding nothing: the paired coverage difference of each variant against the reference with its standard error; wrong declines among the scenarios whose
excerpts cover the fact (strict count: a whole covering set of a point is in the pack the answer saw) on D5, and on the mechanical sample weighted as in
scripts/m5_g_rule.py (f); served points per setting (strict count); answer length; format failures on the mechanical items; gap sentences per answer.
A measure that is missing, not a number or not computed is an error, never a pass.
The last line of the log and of the report is CHOICE: grounded-g2, CHOICE: grounded-g3 or CHOICE: neither.
"""


class DaError(RuntimeError):
    pass


rd, wr, sha256_file, sha256_text = G.rd, G.wr, G.sha256_file, G.sha256_text


def num(x):
    return G.num(x)


def now():
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


def kvfile(path):
    """The B4 marker: lines 'key value'."""
    out = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        k, _, v = line.partition(" ")
        if k:
            out[k] = v
    return out


# ---------------------------------------------------------------- gates (CPU)
def gate_chosen(record, dry):
    """-> (setting or None, problems)."""
    try:
        rec = json.load(open(record, encoding="utf-8"))
    except (OSError, ValueError) as e:
        return None, [f"{record} is missing or unreadable ({type(e).__name__}): run scripts/m6_d5_funnel.sh first"]
    setting = rec.get("setting")
    if not isinstance(setting, str) or not setting:
        return None, [f"{record} names no setting"]
    problems = S.chosen_problems(record, setting)
    if rec.get("dry_run") and not dry:
        problems.append("the record is the record of a dry run (synthetic batch)")
    return setting, problems


def gate_funnel(fw, items, rubrics, setting, record, used, dry):
    fw = Path(fw)
    problems = []
    mp = fw / "DF_MANIFEST.json"
    if not mp.is_file():
        return [f"{mp} is missing: the D5 funnel has not run"]
    man = json.load(open(mp, encoding="utf-8"))
    opts = man.get("options") or {}
    if opts.get("setting") != setting:
        problems.append(f"the funnel's manifest names the setting {opts.get('setting')!r}, not {setting!r}")
    if bool(opts.get("dry_run")) != bool(dry):
        problems.append(f"the funnel's manifest is of a {'dry' if opts.get('dry_run') else 'real'} run, this one is {'dry' if dry else 'real'}")
    if not Path(used).is_file():
        problems.append(f"{used} is missing: the funnel's done marker")
    for st in FUNNEL_STAGES:
        mk = fw / f"{st}.passed"
        if not mk.is_file():
            problems.append(f"funnel stage marker {mk.name} is missing")
            continue
        d = json.load(open(mk, encoding="utf-8"))
        if not d.get("passed") or d.get("pin") != (man.get("pinned") or {}).get("sha256"):
            problems.append(f"funnel stage marker {mk.name} is not a pass under the funnel's manifest")
        for rel, h in (d.get("outputs") or {}).items():
            p = fw / rel
            if not p.is_file():
                problems.append(f"funnel stage {st}: output {rel} is missing")
            elif sha256_file(p) != h:
                problems.append(f"funnel stage {st}: output {rel} differs from the hash the funnel recorded")
    if problems:
        return problems
    ids = [r["id"] for r in rd(items)]
    rub_ids = [r["id"] for r in rd(rubrics)]
    if ids != rub_ids:
        problems.append("items and rubrics of D5 do not hold the same ids in the same order")
    packs = {r["id"]: r for r in rd(fw / "packs.jsonl")}
    exps = {r["id"]: r for r in rd(fw / "expansions.jsonl")}
    if set(packs) != set(ids):
        problems.append(f"the funnel's packs hold {len(packs)} scenarios, the batch {len(ids)}, or other ids")
    if set(exps) != set(ids):
        problems.append("the funnel's expansions do not hold exactly the scenarios of the batch")
    for i in ids:
        r = packs.get(i)
        if r is None:
            continue
        fr, ch = r.get("frozen"), r.get(setting)
        if not isinstance(fr, list) or not isinstance(ch, list) or not fr:
            problems.append(f"{i}: the funnel's row has no frozen pack or no {setting} pack")
        elif ch[:len(fr)] != fr or len(set(ch)) != len(ch):
            problems.append(f"{i}: the {setting} pack does not start with the frozen pack (or repeats a chunk)")
        if i in exps and not num(exps[i].get("seconds")):
            problems.append(f"{i}: the funnel's expansion has no seconds")
    try:
        t = json.load(open(fw / "served.json", encoding="utf-8"))["timing"][setting]["median"]
        if not num(t):
            problems.append("the funnel's measured median of the second reranker call is not a number")
    except (OSError, KeyError, ValueError, TypeError):
        problems.append("the funnel's served.json has no timing of the chosen setting")
    return problems[:12]


def stored_retrieval_medians(files=None, root=ROOT):
    """The median seconds.retrieval of the stored default runs (timing fields only); a missing file is a problem. -> (problems, medians)."""
    meds = []
    for f in files or STORED_RETRIEVAL_FILES:
        p = Path(f) if Path(f).is_absolute() else Path(root) / f
        if not p.is_file():
            return [f"{f} is missing: the cross-check of the retrieval timing cannot be made"], meds
        meds.append(statistics.median(r["seconds"]["retrieval"] for r in rd(p) if not r.get("error")))
    return [], meds


def gate_b4_override(used):
    """--without-b4 skips the B4 gate but not a B4 result that exists: a marker with a verdict that is not PASS refuses."""
    if not Path(used).is_file():
        return []
    v = kvfile(used).get("verdict")
    return [] if v == "PASS" else [f"{used} exists and its verdict is {v!r}, not PASS: --without-b4 does not override a B4 check that did not pass"]


def gate_b4(used, b4_work, record, setting, dry):
    problems = []
    if not Path(used).is_file():
        return [f"{used} is missing: the B4 check has not been run (scripts/m5_b4_check.sh --decide strict --setting {setting})"]
    m = kvfile(used)
    if m.get("verdict") != "PASS":
        problems.append(f"the B4 check's verdict is {m.get('verdict')!r}, not PASS")
    if m.get("decide") != "strict":
        problems.append(f"the B4 check decided on the {m.get('decide')!r} count, not strict")
    mp = Path(b4_work) / "B4_MANIFEST.json"
    if not mp.is_file():
        problems.append(f"{mp} is missing: the B4 check's manifest names the setting it checked")
        return problems
    man = json.load(open(mp, encoding="utf-8"))
    o = man.get("options") or {}
    if o.get("setting") != setting:
        problems.append(f"the B4 check was run for the setting {o.get('setting')!r}, not {setting!r}")
    if o.get("decide") != "strict":
        problems.append("the B4 manifest's decide option is not strict")
    if bool(o.get("dry_run")) != bool(dry):
        problems.append(f"the B4 manifest is of a {'dry' if o.get('dry_run') else 'real'} run, this one is {'dry' if dry else 'real'}")
    want = json.load(open(record, encoding="utf-8")).get("settings_sha256")
    if (man.get("profile") or {}).get("settings_sha256") != want:
        problems.append("the B4 check ran with other parameters of the setting than the funnel's record")
    return problems


# ---------------------------------------------------------------- packs
def write_pack_file(path, rows, extra):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".tmp")
    tmp.write_text("".join(json.dumps({"id": i, "pack": p, "error": None}) + "\n" for i, p in rows), encoding="utf-8")
    tmp.replace(path)
    Path(str(path) + ".manifest.json").write_text(json.dumps({"kind": "m6-d5-packs", "count": len(rows), "pack_file_sha256": sha256_file(path), **extra},
                                                             indent=1, sort_keys=True) + "\n", encoding="utf-8")


def cmd_d5_packs(a):
    fw = Path(a.funnel_work)
    items = [r["id"] for r in rd(a.items)]
    packs = {r["id"]: r for r in rd(fw / "packs.jsonl")}
    missing = [i for i in items if i not in packs]
    if missing:
        raise DaError(f"the funnel's packs lack {len(missing)} scenarios (first: {missing[0]})")
    ph = sha256_file(fw / "packs.jsonl")
    for out, key in ((a.out_frozen, "frozen"), (a.out_chosen, a.setting)):
        write_pack_file(out, [(i, list(packs[i][key])) for i in items], {"source": "the D5 funnel's packs.jsonl", "source_sha256": ph, "setting": key})
        print(f"d5 packs {key}: wrote {out} ({len(items)} scenarios)")
    bad = [i for i in items if list(packs[i][a.setting])[:len(packs[i]["frozen"])] != list(packs[i]["frozen"])]
    print(f"d5 packs: {len(items) - len(bad)} of {len(items)} {a.setting} packs start with the frozen pack")
    return 1 if bad else 0


def cmd_mech_packs(a):
    from kafka_sre_rag import profiles, retrieval
    grid = profiles.grid_settings(a.setting)
    items = rd(a.items)
    stored = {r["id"]: r for r in rd(a.stored)}
    out = Path(a.out)
    done = {r["id"]: r for r in rd(out)} if out.is_file() else {}
    todo = [it for it in items if it["id"] not in done]
    print(f"[mech-packs] {len(items)} items, {len(done)} already done, {len(todo)} to do; setting {a.setting}"
          f"{' (STAND-IN reranker: dry run, the stored pack is put first)' if a.stand_in else ''}", flush=True)
    rr = None
    if a.stand_in:
        import m6_d5_funnel as F6
        rr = F6.Rerankers(True)
    for n, it in enumerate(todo, 1):
        st = stored.get(it["id"])
        if st is None or not st.get("pack") or not st.get("search_terms"):
            print(f"[mech-packs] {n}/{len(todo)} id={it['id']} FAILED no stored pack or search terms", flush=True)
            return 1
        q, exp, base = S.full_question(it), st["search_terms"], list(st["pack"])
        if rr is None:
            chunks = retrieval.select(q, exp, profile="r4", settings=grid)
        else:
            with rr.at(a.setting):
                chunks = retrieval.select(q, exp, profile="r4", settings=grid)
        pack = [c["id"] for c in chunks]
        if rr is not None:       # dry run only: the stand-in scores are not the stored ones, so the stored pack is put first (no number of a dry run is a measurement)
            pack = base + [c for c in pack if c not in base]
        if pack[:len(base)] != base or len(set(pack)) != len(pack):
            print(f"[mech-packs] {n}/{len(todo)} id={it['id']} FAILED the {a.setting} pack does not start with the stored frozen pack (or repeats a chunk)", flush=True)
            return 1
        with open(out, "a", encoding="utf-8") as f:
            f.write(json.dumps({"id": it["id"], "pack": pack, "error": None}) + "\n")
            f.flush()
            os.fsync(f.fileno())
        print(f"[mech-packs] {n}/{len(todo)} id={it['id']} stored={len(base)} {a.setting}={len(pack)} starts_with_stored=yes", flush=True)
    rows = {r["id"]: r for r in rd(out)}
    if set(rows) != {it["id"] for it in items}:
        print("[mech-packs] FAILED the pack file does not hold exactly the items", flush=True)
        return 1
    Path(str(out) + ".manifest.json").write_text(json.dumps(
        {"kind": "m6-d5-packs", "count": len(rows), "pack_file_sha256": sha256_file(out), "setting": a.setting, "stored_answers": str(a.stored),
         "stored_answers_sha256": sha256_file(a.stored), "items_sha256": sha256_file(a.items), "dry_run_stand_in": bool(a.stand_in)}, indent=1, sort_keys=True) + "\n",
        encoding="utf-8")
    print(f"[mech-packs] done {len(rows)} rows", flush=True)
    return 0


# ---------------------------------------------------------------- manifest, markers, start check
def current_state(a):
    missing = [p for p in a.pin if not Path(p).is_file()]
    if missing:
        raise DaError(f"pinned file missing: {missing[0]}" + (f" (and {len(missing) - 1} more)" if len(missing) > 1 else ""))
    return {"schema": SCHEMA, "kind": "dry" if a.dry else "real", "rule_text_sha256": sha256_text(RULE_TEXT), "prompts_sha256": G.prompt_hashes(a.frozen),
            "options": G.kv(a.opt), "judge_args": G.kv(a.judge_arg), "info": G.kv(a.info), "pins": {p: sha256_file(p) for p in sorted(a.pin)}}


def is_code_pin(path):
    p = Path(path).parts
    return len(p) >= 2 and p[-2] == "scripts" and p[-1] in CODE_FILES


def add_state_args(p):
    p.add_argument("--manifest", required=True)
    p.add_argument("--work", required=True)
    p.add_argument("--pin", action="append", default=[])
    p.add_argument("--frozen", default="kafka_sre_rag/frozen.json")
    p.add_argument("--dry", action="store_true")
    p.add_argument("--opt", action="append", default=[], help="NAME=VALUE of the options; compared")
    p.add_argument("--judge-arg", action="append", default=[])
    p.add_argument("--info", action="append", default=[], help="NAME=VALUE recorded for the reader (the git head, --max-unjudged); never compared")
    p.add_argument("--accept-code-change", default=None, metavar="REASON")


def cmd_manifest(a):
    new = current_state(a)
    if Path(a.manifest).exists():
        print(f"manifest exists: {a.manifest}")
        return 0
    Path(a.manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(a.manifest).write_text(json.dumps(new, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written: {a.manifest} ({len(new['pins'])} pinned files, rule text {new['rule_text_sha256'][:12]})")
    return 0


def cmd_start_check(a):
    """Refuse (exit 2) when the stored manifest differs from the state now, when a marker has no manifest, was written for another manifest, or
    an output it recorded is missing or changed. Moves and deletes nothing. --accept-code-change: only the chain's own two code files, only when the
    reference and answers stages are done."""
    mk = G.markers(a.work)
    mp = Path(a.manifest)
    if not mp.exists():
        if mk:
            print(f"REFUSED: stage markers exist ({', '.join(m.name for m in mk[:3])}) but there is no manifest {a.manifest}", file=sys.stderr)
            return 2
        print("start check: no manifest and no markers: a fresh start")
        return 0
    try:
        old = json.load(open(mp, encoding="utf-8"))
        new = current_state(a)
    except (DaError, ValueError, OSError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    old_eff = {**old, "pins": G.effective_pins(old)}
    diffs = G.diff_state(old_eff, new)
    accepted = None
    if diffs and a.accept_code_change is not None:
        changed = [f for f in new["pins"] if is_code_pin(f) and old_eff["pins"].get(f) != new["pins"][f]]
        rest = G.diff_state({**old_eff, "pins": {k: (new["pins"][k] if k in changed else v) for k, v in old_eff["pins"].items()}}, new)
        names = {m.stem[6:] for m in mk}
        if not a.accept_code_change.strip():
            why = "the reason of --accept-code-change is empty"
        elif not changed:
            why = "no pinned hash of scripts/m6_d5_answers.sh or scripts/m6_d5_rule.py differs (only those two can be accepted)"
        elif rest:
            why = "something besides scripts/m6_d5_answers.sh and scripts/m6_d5_rule.py differs: " + "; ".join(rest[:6])
        elif (Path(a.work) / "judge_d5.jsonl").is_file() and (Path(a.work) / "judge_d5.jsonl").stat().st_size > 0:
            why = "the judge stage has results already (judge_d5.jsonl): a change of the chain's own code is accepted only before that"
        elif not all(st in names for st in ANSWER_STAGES):
            why = "not every answering stage is done (needs " + ", ".join(ANSWER_STAGES) + "; done: " + (", ".join(sorted(names)) or "none") + ")"
        else:
            why = None
            accepted = changed
        if why:
            print(f"REFUSED: --accept-code-change: {why} (nothing was moved, deleted or written):", file=sys.stderr)
            for d in diffs[:12]:
                print("  " + d, file=sys.stderr)
            return 2
        diffs = []
    if diffs:
        print("REFUSED: the stored manifest differs from the state now (nothing was moved or deleted):", file=sys.stderr)
        for d in diffs[:12]:
            print("  " + d, file=sys.stderr)
        return 2
    mh = G.manifest_hash(old)
    bad = []
    for m in mk:
        try:
            rec = json.load(open(m, encoding="utf-8"))
        except ValueError:
            bad.append(f"{m.name}: unreadable")
            continue
        if rec.get("manifest_sha256") != mh:
            bad.append(f"{m.name}: written for another manifest (stale)")
            continue
        for f, h in (rec.get("outputs") or {}).items():
            if not Path(f).is_file():
                bad.append(f"{m.name}: output {f} is missing")
            elif sha256_file(f) != h:
                bad.append(f"{m.name}: output {f} changed since the stage finished")
    if bad:
        print("REFUSED: stage markers do not match the manifest or their outputs (nothing was moved or deleted):", file=sys.stderr)
        for b in bad[:12]:
            print("  " + b, file=sys.stderr)
        return 2
    if accepted:
        rec = {"time": now(), "reason": a.accept_code_change.strip(), "files": {f: {"old": old_eff["pins"][f], "new": new["pins"][f]} for f in accepted}}
        out = {**old, G.AUDIT_KEY: (old.get(G.AUDIT_KEY) or []) + [rec]}
        tmp = Path(str(mp) + ".tmp")
        tmp.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(mp)
        for f in accepted:
            print(f"ACCEPTED change of the chain's own code: {f}: {rec['files'][f]['old']} -> {rec['files'][f]['new']}; reason: {rec['reason']}")
    print(f"start check: manifest equal to the state now; {len(mk)} stage(s) done: {', '.join(m.stem[6:] for m in mk) or 'none'}")
    return 0


def cmd_code_change_note(a):
    ch = json.load(open(a.manifest, encoding="utf-8")).get(G.AUDIT_KEY) or []
    if not ch:
        return 0
    print("### Accepted change of the chain's own code (`--accept-code-change`)\n")
    print("Only `scripts/m6_d5_answers.sh` and `scripts/m6_d5_rule.py` can be accepted, and only when the reference and answers stages were done.\n")
    for c in ch:
        print(f"- {c['time']}: reason: {c['reason']}")
        for f, hh in sorted(c["files"].items()):
            print(f"  - `{f}`: old sha256 `{hh['old']}`, new sha256 `{hh['new']}`")
    return 0


# ---------------------------------------------------------------- the prompt check
def cmd_prompts_check(a):
    """The failure this catches: a mode that silently fell back to the default prompt. The variant's prompt hash recorded in every row must equal the hash
    of the mode's own prompt on the row's pack, and must differ from the default prompt on that same pack."""
    import eval2
    from kafka_sre_rag import evalhooks, prompts
    items = {r["id"]: r for r in rd(a.items)}
    rows = rd(a.answers)
    bad = []
    for r in rows:
        chunks = evalhooks.chunks_by_ids(r["pack"])
        q = eval2.full_question(items[r["id"]])
        want = eval2.messages_fingerprint(prompts.grounded_answer_messages(q, chunks, a.mode))[0]
        dflt = eval2.messages_fingerprint(prompts.answer_messages(q, chunks, strict=False))[0]
        got = r.get("prompt_sha256")
        if got is None:
            bad.append(f"{r['id']}: no prompt hash recorded")
        elif got != want:
            bad.append(f"{r['id']}: the recorded prompt is not the prompt of {a.mode} on its pack")
        elif got == dflt:
            bad.append(f"{r['id']}: the prompt equals the default prompt on its pack")
    print(f"prompts-check {a.answers}: {len(rows) - len(bad)} of {len(rows)} rows carry the prompt of {a.mode} on their pack, different from the default prompt on it")
    for b in bad[:8]:
        print("  " + b)
    return 0 if not bad and rows else 1


def cmd_prompts_differ(a):
    """eval2.py compare-prompts, by code: every id common to the two files has a recorded prompt hash and a different one."""
    import eval2
    A, B = rd(a.a), rd(a.b)
    r = eval2.compare_prompt_hashes(A, B)
    n = len(r["differ"])
    ok = r["common"] == a.expect and n == r["common"] and not r["equal"] and not r["not_recorded"]
    print(f"prompts-differ {a.b} against {a.a}: {r['common']} ids in both (expected {a.expect}): {len(r['equal'])} equal, {n} differ, {len(r['not_recorded'])} not recorded")
    return 0 if ok else 1


# ---------------------------------------------------------------- the measures
def need(m, *path):
    try:
        return G.need(m, *path)
    except G.RuleError as e:
        raise DaError(str(e))


def mean(xs):
    return sum(xs) / len(xs)


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def se(xs):
    return statistics.stdev(xs) / math.sqrt(len(xs)) if len(xs) > 1 else None


def supp_miss_on_pack(results_path, rubrics_path, set_name, pack_of):
    """Supported essential-miss units per answer (mean over the valid passes), a miss counting as supported when a chunk of its point is in pack_of[id]
    instead of in the pack the answer saw. -> {id: mean units}."""
    import eval2_judge as ej
    rub = {r["id"]: r for r in rd(rubrics_path)}
    acc = {}
    for r in rd(results_path):
        if not r.get("valid_reply") or r.get("reference_given") or r["set"] != set_name:
            continue
        c = ej.coverage_row({**r, "pack": list(pack_of[r["id"]])}, rub[r["id"]])
        acc.setdefault(r["id"], []).append(sum(1 for u in c["units"] if u["status"] == "missed" and u["supported"]))
    return {i: sum(x) / len(x) for i, x in acc.items()}


def d5_variant(a, v, rubrics, exp_s, retr_s, frozen):
    W = Path(a.work)
    rows = rd(W / f"ans_d5_{v}.jsonl")
    rid = [r["id"] for r in rubrics]
    if sorted(r["id"] for r in rows) != sorted(rid):
        raise DaError(f"ans_d5_{v}.jsonl does not hold exactly the D5 scenarios")
    sets_v = {v}
    G.check_unjudged(W / "judge_d5.jsonl", a.max_unjudged, sets_v)
    jr = G.judged(str(W / "judge_d5.jsonl"), a.d5_rubrics, sets_v)
    P, SM = {}, []
    for i in rid:
        j = jr.get((v, i))
        if j is None:
            raise DaError(f"no valid judge result for {v} {i}")
        P[i] = j[0]
        SM.append(j[1])
    toks = [r.get("tokens") if (r.get("tokens") or not a.stub) else max(1, len(r.get("text") or "") // 4) for r in rows]
    if any(not num(t) for t in toks):
        raise DaError("an answer row has no completion token count")
    total = [r["seconds"]["answer"] + exp_s[r["id"]] + retr_s for r in rows]
    smf = supp_miss_on_pack(W / "judge_d5.jsonl", a.d5_rubrics, v, frozen)
    if sorted(smf) != sorted(rid):
        raise DaError(f"no valid judge result on the frozen-pack measure for every {v} answer")
    nid, kid = G.parse_idcheck(W / f"idcheck_d5_{v}.txt")
    gap = sum((r.get("text") or "").count(G.GAP) for r in rows)
    import eval2_score
    by = {r["id"]: r for r in rows}
    cov_n = wd = 0
    for rb in rubrics:      # strict: a whole covering set of at least one point is in the pack THIS answer saw
        pk = by[rb["id"]]["pack"]
        if any(S.point_sets(pt) and S.point_flags(pt, pk)[0] for pt in rb["must"]):
            cov_n += 1
            wd += bool(eval2_score.detect_decline(by[rb["id"]].get("text") or "")[0])
    return {"P": mean(list(P.values())), "P_by_id": P, "answers_judged": len(P), "supp_miss_per10": 10 * sum(SM) / len(SM), "supp_miss_answers": len(SM),
            "supp_miss_per10_frozen": 10 * sum(smf.values()) / len(smf), "retrieval_s": retr_s,
            "cut": sum(1 for r in rows if r.get("truncated")), "answers": len(rows), "median_tokens": median(toks),
            "median_chars": median([len(r.get("text") or "") for r in rows]), "median_answer_s": median([r["seconds"]["answer"] for r in rows]),
            "median_total_s": median(total), "idents_per10": 10 * kid / nid if nid else None, "idents": kid, "idents_answers": nid,
            "gap_sentences_per_answer": gap / len(rows), "d5_wrong_declines": wd, "d5_strict_covered_scenarios": cov_n}


def timing_ids(ids):
    import m6_d5_funnel as F6
    return F6.timing_ids(list(ids), TIMING_N)


def cmd_retrieval_timing(a):
    """The whole retrieval of each side, uncached, on the funnel's timing questions, from the funnel's stored expansions (CPU; no server call)."""
    import time
    from kafka_sre_rag import profiles, retrieval
    grid = profiles.grid_settings(a.setting)
    items = {it["id"]: it for it in rd(a.items)}
    ids = timing_ids(items)
    exps = {r["id"]: r["expansion"] for r in rd(Path(a.funnel_work) / "expansions.jsonl")}
    out = Path(a.out)
    done = {r["id"] for r in rd(out)} if out.is_file() else set()
    rr = None
    if a.stand_in:
        import m6_d5_funnel as F6
        rr = F6.Rerankers(True)
    print(f"[rettime] {len(ids)} questions by the hash rule, {len(done)} already done; whole retrieval of the frozen setting and of {a.setting}, uncached"
          f"{' (STAND-IN reranker: dry run)' if a.stand_in else ''}", flush=True)
    # the loads are not timed: the index and the reranker are warmed on a question that is not one of the timed ones
    if rr is None:
        retrieval.RT.index(str(ROOT / "work/v3/chunks_s1.jsonl"))
        retrieval.RT.reranker().predict([("warm up", "warm up passage")] * 2, batch_size=2)
        retrieval._reranker_at(512).predict([("warm up", "warm up passage")] * 2, batch_size=2)
    else:
        rr.warm_up()
    for n, i in enumerate((x for x in ids if x not in done), 1):
        if i not in exps:
            print(f"[rettime] {i} FAILED no stored expansion", flush=True)
            return 1
        q = S.full_question(items[i])
        t0 = time.monotonic()
        retrieval.select(q, exps[i])
        t1 = time.monotonic()
        if rr is None:
            retrieval.select(q, exps[i], profile="r4", settings=grid)
        else:
            with rr.at(a.setting):
                retrieval.select(q, exps[i], profile="r4", settings=grid)
        t2 = time.monotonic()
        with open(out, "a", encoding="utf-8") as f:
            f.write(json.dumps({"id": i, "frozen_s": round(t1 - t0, 4), "chosen_s": round(t2 - t1, 4)}) + "\n")
            f.flush()
            os.fsync(f.fileno())
        print(f"[rettime] {n}/{len(ids) - len(done)} id={i} frozen={t1 - t0:.2f}s {a.setting}={t2 - t1:.2f}s", flush=True)
    print(f"[rettime] done {len(ids)} rows", flush=True)
    return 0


def cmd_measures(a):
    W = Path(a.work)
    ids = json.load(open(a.ids, encoding="utf-8"))
    rubrics = rd(a.d5_rubrics)
    fw = Path(a.funnel_work)
    exp_s = {r["id"]: r["seconds"] for r in rd(fw / "expansions.jsonl")}
    items = rd(a.d5_items)
    tids = timing_ids([it["id"] for it in items])
    trows = {r["id"]: r for r in rd(W / "retrieval_timing.jsonl")}
    if sorted(trows) != sorted(tids) or any(not num(trows[i].get("frozen_s")) or not num(trows[i].get("chosen_s")) for i in tids):
        raise DaError("retrieval_timing.jsonl does not hold a number for each side of exactly the timing questions")
    tm = {"frozen": median([trows[i]["frozen_s"] for i in tids]), a.setting: median([trows[i]["chosen_s"] for i in tids])}
    sproblems, smeds = stored_retrieval_medians()
    frozen = {r["id"]: r["pack"] for r in rd(a.packs_frozen)}
    chosen = {r["id"]: r["pack"] for r in rd(a.packs_chosen)}
    parts_a = {"M1": ids["M1"], "M2": ids["M2"]}
    parts_3 = {"M3": ids["M3"]}
    m = {"schema": SCHEMA, "setting": a.setting, "sizes": {"D5": len(rubrics), "mech": {"M1": len(ids["M1"]), "M2": len(ids["M2"]), "M3": len(ids["M3"])}},
         "pop": dict(G.POP), "retrieval_median_s": tm, "retrieval_timing_n": len(tids),
         "stored_retrieval_medians_b1_b2": None if sproblems else smeds,
         "expansion_median_s": median(list(exp_s.values())), "variants": {}}
    ref_scores = {"ma": G.score_stats(W / "score_ma_default.jsonl", parts_a), "m3": G.score_stats(W / "score_m3_default.jsonl", parts_3)}
    fw_ = G.f_weights(ids, "sample")
    for v in ("default",) + VARIANTS:
        e = d5_variant(a, v, rubrics, exp_s, tm["frozen" if v == "default" else a.setting], frozen)
        if v != "default":
            sa, s3 = G.score_stats(W / f"score_ma_{v}.jsonl", parts_a), G.score_stats(W / f"score_m3_{v}.jsonl", parts_3)
            fe = G.f_estimate(sa["parts"], fw_)
            d = {k: sa["all_pass"][k] - ref_scores["ma"]["all_pass"][k] for k in parts_a}
            d["M3"] = s3["all_pass"]["M3"] - ref_scores["m3"]["all_pass"]["M3"]
            e["mech"] = {"d_all_pass": d, "all_pass": {"ma": sa["all_pass"], "m3": s3["all_pass"]},
                         "ref_all_pass": {"ma": ref_scores["ma"]["all_pass"], "m3": ref_scores["m3"]["all_pass"]},
                         "format_failures": sa["format_failures"] + s3["format_failures"],
                         "ref_format_failures": ref_scores["ma"]["format_failures"] + ref_scores["m3"]["format_failures"],
                         "wrong_declines": fe["wd"], "covered": fe["covered"], "declines": fe["declines"], "wd_weighted": fe["wd_weighted"],
                         "covered_weighted": fe["covered_weighted"], "declines_weighted": fe["declines_weighted"], "f_weights": fe["weights"],
                         "outcomes_tier_a": sa["outcomes"], "outcomes_a3": s3["outcomes"]}
            diff = [e["P_by_id"][i] - m["variants"]["default"]["P_by_id"][i] for i in e["P_by_id"]]
            e["paired_vs_default"] = {"mean": mean(diff), "se": se(diff), "n": len(diff)}
        m["variants"][v] = e
    c = S.compute(rubrics, {i: {"frozen": frozen[i], "r4": chosen[i]} for i in frozen})
    cs, cl = c["strict"]["all"], c["loose"]["all"]
    m["served"] = {"indexed_points": cs["indexed_points"], "strict": {"frozen": cs["frozen_served"], a.setting: cs["r4_served"], "gained": cs["gained"], "lost": cs["lost"]},
                   "loose": {"frozen": cl["frozen_served"], a.setting: cl["r4_served"]}, "outside_the_count": c["outside_the_count"],
                   "chosen_starts_with_frozen": c["r4_starts_with_frozen"], "pack_size_median": {"frozen": median([len(p) for p in frozen.values()]),
                                                                                                a.setting: median([len(p) for p in chosen.values()])}}
    ma_st = {r["id"]: r["pack"] for r in rd(W / "ref_ma.jsonl")}
    m3_st = {r["id"]: r["pack"] for r in rd(W / "ref_m3.jsonl")}
    mp = {**{i: r["pack"] for i, r in ((r["id"], r) for r in rd(W / f"packs_chosen_ma.jsonl"))}, **{r["id"]: r["pack"] for r in rd(W / "packs_chosen_m3.jsonl")}}
    st = {**ma_st, **m3_st}
    m["mech_packs"] = {"items": len(mp), "chosen_size_median": median([len(p) for p in mp.values()]), "stored_size_median": median([len(st[i]) for i in mp]),
                       "gained_a_chunk": sum(1 for i in mp if len(mp[i]) > len(st[i]))}
    if a.max_unjudged > 0:
        m["unjudged"] = {"max_unjudged": a.max_unjudged, "rows": len(G.unjudged_state(W / "judge_d5.jsonl")[1])}
    (W / "measures.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (W / "measures.md").write_text(measures_md(m), encoding="utf-8")
    print(measures_md(m))
    return 0


def measures_md(m):
    f = lambda x, d=2: "n/a" if x is None else f"{x:.{d}f}"  # noqa: E731
    st = m["setting"]
    L = ["### Measures", "", f"D5: {m['sizes']['D5']} scenarios; mechanical sample {m['sizes']['mech']}; reference = the default mode on the frozen packs, answered in this run; "
         f"variants on the {st} packs.", "",
         "How the seconds with search are put together (condition d): per D5 answer, the answer seconds of this run (the packs are replayed, so this is the answer call only) + the "
         f"scenario's search-terms seconds of the funnel's expansions (median over D5 {f(m['expansion_median_s'], 1)} s) + the median whole-retrieval seconds of that side's setting, "
         f"measured in this chain before the reference on the funnel's {m['retrieval_timing_n']} timing questions, uncached, from the stored expansions: frozen {f(m['retrieval_median_s']['frozen'], 2)} s, "
         f"{st} {f(m['retrieval_median_s'][st], 2)} s (the reference gets the frozen figure, a variant the {st} figure); the median over the sums is taken. Cross-check, deciding nothing: the median "
         f"seconds.retrieval of the stored default runs on the frozen retrieval (B1, B2): {'not available' if not m['stored_retrieval_medians_b1_b2'] else ', '.join(f(x, 2) + ' s' for x in m['stored_retrieval_medians_b1_b2'])}.", "",
         "| answers | P (a) | supp. essential-miss per 10 (b), own pack | (b) on the frozen pack (reported) | cut (c) | median s answer / with search (d) | nonexistent ids per 10 (e) | median tokens | median characters | gap sentences per answer |",
         "|---|---|---|---|---|---|---|---|---|---|"]
    for v, e in m["variants"].items():
        L.append(f"| {v} | {f(e['P'], 3)} (n {e['answers_judged']}) | {f(e['supp_miss_per10'])} | {f(e['supp_miss_per10_frozen'])} | {e['cut']} of {e['answers']} | {f(e['median_answer_s'], 1)} / {f(e['median_total_s'], 1)} | "
                 f"{f(e['idents_per10'])} ({e['idents']} in {e['idents_answers']}) | {f(e['median_tokens'], 0)} | {f(e['median_chars'], 0)} | {f(e['gap_sentences_per_answer'])} |")
    L += ["", "| variant | all-pass change per part (f) | format failures (variant / reference) | wrong declines / covered, weighted | wrong declines / covered, unweighted | paired coverage difference vs reference (SE) |",
          "|---|---|---|---|---|---|"]
    for v, e in m["variants"].items():
        if "mech" in e:
            x, p = e["mech"], e["paired_vs_default"]
            L.append(f"| {v} | {x['d_all_pass']} (variant {x['all_pass']}, reference {x['ref_all_pass']}) | {x['format_failures']} / {x['ref_format_failures']} | "
                     f"{x['wd_weighted']:.1f} / {x['covered_weighted']:.1f} | {x['wrong_declines']} / {x['covered']} | {p['mean']:+.3f} (SE {f(p['se'], 3)}, n {p['n']}) |")
    L += ["", "Wrong declines on D5 (reported, deciding nothing): a scenario with at least one point whose whole covering set is in the pack the answer saw, answered with a gap sentence:"]
    for v, e in m["variants"].items():
        L.append(f"- {v}: {e['d5_wrong_declines']} of {e['d5_strict_covered_scenarios']} such scenarios")
    s = m["served"]
    L += ["", f"Served points (strict count, {s['indexed_points']} indexed points of D5; reported, deciding nothing): frozen {s['strict']['frozen']}, {st} {s['strict'][st]} "
          f"(gained {s['strict']['gained']}, lost {s['strict']['lost']}); loose count frozen {s['loose']['frozen']}, {st} {s['loose'][st]}; pack size median "
          f"{s['pack_size_median']['frozen']:g} against {s['pack_size_median'][st]:g} chunks."]
    mp = m["mech_packs"]
    L += ["", f"Packs of the {mp['items']} mechanical items: median size {mp['chosen_size_median']:g} chunks on the {st} packs against {mp['stored_size_median']:g} on the stored frozen packs; "
          f"{mp['gained_a_chunk']} of {mp['items']} gained at least one chunk.", "",
          "Each variant against the reference measures the combination (the prompt and the packs together; there is no default-on-the-chosen-packs arm), while G2 against G3 is on the same "
          "packs. The served gain on D5 is in-sample for the funnel's gate, B4 being the out-of-sample check."]
    for v, e in m["variants"].items():
        if "mech" in e:
            L.append(f"\nDecline outcomes of {v}: tier_a part {e['mech']['outcomes_tier_a']}; tier_a3 part {e['mech']['outcomes_a3']}")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- the rule
def projection(m, v):
    dall = m["variants"][v].get("mech", {}).get("d_all_pass") if isinstance(m["variants"][v].get("mech"), dict) else None
    sizes = m["sizes"].get("mech")
    if not isinstance(dall, dict) or not isinstance(sizes, dict) or set(dall) != set(sizes) or set(m["pop"]) != set(sizes):
        raise DaError(f"input missing: variants.{v}.mech.d_all_pass for the parts {sorted(sizes or {})}")
    proj = 0.0
    for part, dv in dall.items():
        if not num(dv) or not num(sizes[part]) or sizes[part] <= 0 or not num(m["pop"][part]):
            raise DaError(f"input is not a number: variants.{v}.mech.d_all_pass.{part}")
        proj += dv * m["pop"][part] / sizes[part]
    return proj / sum(m["pop"].values()) * 100, dall, sizes


def evaluate(m, frozen_b=False):
    """-> (lines, eligible {variant: bool}, choice). Raises DaError when any input is missing."""
    for k in ("variants", "sizes", "pop"):
        if k not in m:
            raise DaError("input missing: " + k)
    n = need(m, "sizes", "D5")
    bkey = "supp_miss_per10_frozen" if frozen_b else "supp_miss_per10"
    Pd, Sd, Id = (need(m, "variants", "default", k) for k in ("P", bkey, "idents_per10"))
    eps = 1e-9
    L, elig, P_of = [], {}, {}
    for v in VARIANTS:
        pre = ("variants", v)
        P, S_, cut, tot, ident = (need(m, *pre, k) for k in ("P", bkey, "cut", "median_total_s", "idents_per10"))
        proj, dall, sizes = projection(m, v)
        P_of[v] = P
        c = {
            "a": (P - Pd >= -COVERAGE_MARGIN - eps, f"(a) judged coverage P on D5: {v} {P:.3f}, reference {Pd:.3f}, difference {P - Pd:+.3f} (needs >= -{COVERAGE_MARGIN:.2f}; {int(n)} answers, two passes)"),
            "b": (S_ <= Sd + MISS_MARGIN + eps, f"(b) supported essential-miss units per 10 answers{' (on the FROZEN pack)' if frozen_b else ''}: {v} {S_:.2f}, reference {Sd:.2f} (needs <= reference + {MISS_MARGIN:.1f})"),
            "c": (cut * 100 <= CUT_MAX_PCT * n, f"(c) answers cut at the token limit: {int(cut)} of {int(n)} = {100 * cut / n:.1f} % (needs <= {CUT_MAX_PCT} %)"),
            "d": (tot <= SECONDS_MAX + eps, f"(d) median seconds per D5 answer with search: {tot:.1f} (needs <= {SECONDS_MAX:.0f}; the report says how the number is put together)"),
            "e": (ident <= Id + eps, f"(e) nonexistent identifiers per 10 answers: {v} {ident:.2f}, reference {Id:.2f} (needs <= reference)"),
            "f": (proj >= MECH_MIN - eps, f"(f) projected change of the mechanical score {proj:+.2f} points (needs >= {MECH_MIN:.1f}; all-pass change per part {dall}, sizes {sizes})"),
        }
        L.append(f"Variant {v.upper()}:")
        for k in "abcdef":
            L.append(f"  {'PASS' if c[k][0] else 'FAIL'}  {c[k][1]}")
        elig[v] = all(x[0] for x in c.values())
        L.append(f"  => {v.upper()} is {'ELIGIBLE' if elig[v] else 'NOT eligible'}")
        L.append("")
    ok = [v for v in VARIANTS if elig[v]]
    if not ok:
        choice = "neither"
    elif len(ok) == 1:
        choice = MODES[ok[0]]
    else:
        g2, g3 = P_of["g2"], P_of["g3"]
        choice = MODES["g2"] if abs(g2 - g3) < TIE - eps else MODES["g2" if g2 >= g3 else "g3"]
        L.append(f"Both eligible: coverage G2 {g2:.3f}, G3 {g3:.3f}, difference {g3 - g2:+.3f} ({'less than' if abs(g2 - g3) < TIE - eps else 'at least'} {TIE:.2f}).")
        L.append("")
    return L, elig, choice


ALT_LABEL = "CHOICE with (b) on the frozen pack (decides only if the Owner amends the rule before the result is known): "


def cmd_rule(a):
    try:
        m = json.load(open(a.measures, encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"ERROR: the measures file {a.measures} cannot be read ({type(e).__name__}); no verdict", file=sys.stderr)
        return 2
    try:
        L, elig, choice = evaluate(m)
        L2, elig2, choice2 = evaluate(m, frozen_b=True)
    except DaError as e:
        print(f"ERROR: {e}; a missing input is not a pass; no verdict", file=sys.stderr)
        return 2
    print("\n".join(L))
    if choice == "neither":
        print("(neither variant is eligible: the combination stops and the Owner sees it)")
    print(f"CHOICE: {choice}")
    print("")
    print("Reported beside the rule (decides nothing unless the Owner amends it before the result is known): (b) on the FROZEN pack, all other conditions as above:")
    for l in L2:
        if "(b)" in l or "ELIGIBLE" in l or "eligible" in l or l.startswith("Variant"):
            print("  " + l)
    print(ALT_LABEL + choice2)
    return 0


def cmd_candidate(a):
    m = json.load(open(a.measures, encoding="utf-8"))
    _, elig, choice = evaluate(m)
    _, elig2, choice2 = evaluate(m, frozen_b=True)
    if choice == "neither":
        print("candidate: no mode chosen; no CANDIDATE_CHOSEN.json is written")
        return 0
    v = next(k for k, mode in MODES.items() if mode == choice)
    rec = json.load(open(a.record, encoding="utf-8"))
    frozen = json.load(open(a.frozen, encoding="utf-8"))["prompts_sha256"]
    out = {"mode": choice, "prompt_sha256_key": PROMPT_KEYS[v], "prompt_sha256": frozen[PROMPT_KEYS[v]], "retrieval_setting": rec["setting"],
           "settings_sha256": rec["settings_sha256"], "verdict_line": f"CHOICE: {choice}", "choice_b_on_frozen_pack": choice2, "eligible_b_on_frozen_pack": elig2,
           "choice_b_on_frozen_pack_note": "reported only; decides only if the Owner amends the rule before the result is known", "manifest_sha256": G.manifest_hash(json.load(open(a.manifest, encoding="utf-8"))),
           "dry_run": bool(a.dry), "date": datetime.date.today().isoformat(), "report": str(a.report), "eligible": elig,
           "decided_by": "scripts/m6_d5_rule.py evaluate (the rule written in the report header before any answer)"}
    Path(a.out).write_text(json.dumps(out, indent=1) + "\n", encoding="utf-8")
    print(f"candidate: wrote {a.out} ({choice})")
    return 0


def cmd_unjudged_note(a):
    if a.max_unjudged <= 0:
        return 0
    path = Path(a.work) / "judge_d5.jsonl"
    rows, bad, nopass = G.unjudged_state(path)
    print("### Judge rows that could not be judged (`--max-unjudged`)\n")
    print(f"Option `--max-unjudged {a.max_unjudged}` (default 0: the chain stops when a judge row stays invalid). {len(bad)} unjudged row(s) of {len(rows)}; an answer's judged measures "
          "are then the mean over its valid passes; an answer with no valid pass stops the chain. The rule's thresholds are unchanged.\n")
    for r in bad:
        print(f"- set of answers {r['set']}, id {r['id']}, pass {r['pass']}")
    return 0


# ---------------------------------------------------------------- main
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("rule-text")
    sub.add_parser("gate-first-call")
    p = sub.add_parser("gate-b4-override")
    p.add_argument("--used", required=True)
    sub.add_parser("code-pins")
    p = sub.add_parser("retrieval-timing")
    for k in ("items", "funnel-work", "setting", "out"):
        p.add_argument("--" + k, required=True)
    p.add_argument("--stand-in", action="store_true")
    p = sub.add_parser("chosen-setting")
    p.add_argument("--record", required=True)
    p = sub.add_parser("gate-chosen")
    p.add_argument("--record", required=True)
    p.add_argument("--dry", action="store_true")
    p = sub.add_parser("gate-funnel")
    for k in ("funnel-work", "items", "rubrics", "record", "used", "setting"):
        p.add_argument("--" + k, required=True)
    p.add_argument("--dry", action="store_true")
    p = sub.add_parser("gate-b4")
    for k in ("used", "b4-work", "record", "setting"):
        p.add_argument("--" + k, required=True)
    p.add_argument("--dry", action="store_true")
    p = sub.add_parser("d5-packs")
    for k in ("funnel-work", "items", "setting", "out-frozen", "out-chosen"):
        p.add_argument("--" + k, required=True)
    p = sub.add_parser("mech-packs")
    for k in ("items", "stored", "out", "setting"):
        p.add_argument("--" + k, required=True)
    p.add_argument("--stand-in", action="store_true")
    for name in ("manifest", "start-check"):
        add_state_args(sub.add_parser(name))
    p = sub.add_parser("prompts-check")
    for k in ("items", "answers", "mode"):
        p.add_argument("--" + k, required=True)
    p = sub.add_parser("prompts-differ")
    p.add_argument("--a", required=True)
    p.add_argument("--b", required=True)
    p.add_argument("--expect", type=int, required=True)
    p = sub.add_parser("measures")
    for k in ("work", "ids", "d5-rubrics", "d5-items", "funnel-work", "setting", "packs-frozen", "packs-chosen"):
        p.add_argument("--" + k, required=True)
    p.add_argument("--stub", action="store_true")
    p.add_argument("--max-unjudged", type=int, default=0)
    p = sub.add_parser("rule")
    p.add_argument("--measures", required=True)
    p = sub.add_parser("candidate")
    for k in ("measures", "record", "manifest", "out", "report"):
        p.add_argument("--" + k, required=True)
    p.add_argument("--frozen", default="kafka_sre_rag/frozen.json")
    p.add_argument("--dry", action="store_true")
    p = sub.add_parser("unjudged-note")
    p.add_argument("--work", required=True)
    p.add_argument("--max-unjudged", type=int, default=0)
    p = sub.add_parser("code-change-note")
    p.add_argument("--manifest", required=True)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "rule-text":
            print(RULE_TEXT, end="")
            return 0
        if a.cmd == "chosen-setting":
            setting, _ = gate_chosen(a.record, True)
            if not setting:
                print(f"ERROR: {a.record} names no setting", file=sys.stderr)
                return 2
            print(setting)
            return 0
        if a.cmd == "gate-first-call":
            problems, meds = stored_retrieval_medians()
            for p_ in problems:
                print(f"gate: {p_}", file=sys.stderr)
            if not problems:
                print(f"gate: stored default runs on the frozen retrieval, median seconds.retrieval: B1 {meds[0]:.2f} s, B2 {meds[1]:.2f} s (a cross-check of the timing stage; decides nothing)")
            return 1 if problems else 0
        if a.cmd == "gate-b4-override":
            problems = gate_b4_override(a.used)
            for p_ in problems:
                print(f"gate: {p_}", file=sys.stderr)
            return 1 if problems else 0
        if a.cmd == "code-pins":
            print("\n".join(CODE_PINS))
            return 0
        if a.cmd in ("gate-chosen", "gate-funnel", "gate-b4"):
            if a.cmd == "gate-chosen":
                setting, problems = gate_chosen(a.record, a.dry)
                ok = f"gate: the funnel's record: verdict GO for {setting}, parameters equal candidate.json"
            elif a.cmd == "gate-funnel":
                problems = gate_funnel(a.funnel_work, a.items, a.rubrics, a.setting, a.record, a.used, a.dry)
                ok = "gate: the funnel's stage markers, outputs, packs and timing are intact and equal what the funnel recorded"
            else:
                problems = gate_b4(a.used, a.b4_work, a.record, a.setting, a.dry)
                ok = f"gate: the B4 check ran for {a.setting}, decided on the strict count, and passed"
            for p_ in problems:
                print(f"gate: {p_}", file=sys.stderr)
            if not problems:
                print(ok)
            return 1 if problems else 0
        fn = {"d5-packs": cmd_d5_packs, "mech-packs": cmd_mech_packs, "manifest": cmd_manifest, "start-check": cmd_start_check, "prompts-check": cmd_prompts_check,
              "prompts-differ": cmd_prompts_differ, "measures": cmd_measures, "retrieval-timing": cmd_retrieval_timing, "rule": cmd_rule, "candidate": cmd_candidate, "unjudged-note": cmd_unjudged_note,
              "code-change-note": cmd_code_change_note}[a.cmd]
        return fn(a)
    except (DaError, G.RuleError, OSError, KeyError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
