#!/usr/bin/env python3
"""M5 GD-chain helper: the development check of the grounded modes (runs/m5/reports/g0_grounded_design.md, section 3).

Pure Python, CPU only; no model, no server. Used by scripts/m5_g_dev.sh and tested by tests/test_m5_g_dev.py.

  rule-text                                   print the rule, as it is written into the report header before any answer exists
  draw-ids   --tier-a F --tier-a3 F (--out F | --check F)   draw M1, M2, M3 by the rule of section 3; --check: reproduce a stored ids file
  verify-m1  --scores F                       the 56 ids of M1 against the strict scores with decline outcomes (a scorer run of the S answers)
  select     --ids F --mechanical sample|all --tier-a F --tier-a3 F --ref-a F --ref-a3 F --out-dir D   item and reference files of the mechanical sets
  harness-items --in F --out F --domain NAME  the harness wants strings for tier, kind, family and domain (as the batch scripts do)
  manifest   ...                              write the chain's manifest once (pinned files by sha256, options, rule text hash)
  start-check ...                             compare the stored manifest and the stage markers with the state computed now; exit 2 on a difference
  mark       --work D --stage S [--out F ...] write the stage marker (manifest hash, sha256 of every output file)
  check-answers --file F --expect N [--mode M]   complete, no error, no empty text, a pack on every row, manifest ended
  determinism --stored F --new F --n N        the first N replayed default answers equal the stored ones byte for byte
  packs-equal --a F --b F                     the pack lists of the ids of --b equal those in --a
  f-estimate --scores F --ids F               condition (f) on a stored score file: weighted estimate on the sample beside the full-set share
  f-note                                      print the fixed sentence about the decline detector's bias (written under the (f) result)
  check-echo --stored F... --new F [--dry]    the model the server echoed in the new run's manifest equals the stored runs' model_echoed
  judge-compare --stored F --new F --rubrics F --set NAME   stored judge results against this run's (count of answers whose mean differs)
  measures   --work D ...                     the measures (a) to (h) from the files of the chain; writes measures.json and measures.md
  rule       --measures F                     the rule by code: PASS or FAIL per condition and variant, and the choice (G1, G2 or neither)

Exit codes: 0 done; 1 a check failed; 2 refused or an input is missing (a missing input is never a pass).
"""
import argparse
import hashlib
import json
import math
import random
import re
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

SCHEMA = 1
POP = {"M1": 56, "M2": 195, "M3": 88}          # the populations the mechanical samples stand for (339 open mechanical items)
POP_ALL = sum(POP.values())
VARIANTS = ("g1", "g2")
M2_SEED, M3_SEED, M2_N, M3_N = 20261122, 20261123, 40, 20
M2_KINDS = {"name_setting": 21, "fix_manifest": 6, "connect_rest": 4, "write_resource": 4, "version_trap": 2, "error_name": 1,
            "is_real": 1, "write_command": 1}
M3_KINDS = {"client_code": 6, "valid_but_wrong": 6, "not_covered": 3, "computed_sizing": 3, "safe_order": 2}
GAP = "The retrieved documentation does not cover"

M1_IDS = """a1-argo_cmd_a-001 a1-argo_cmd_a-002 a1-argo_cmd_a-004 a1-argo_cmd_a-006 a1-argo_cmd_a-007 a1-argo_cmd_b-001 a1-argo_cmd_b-002
a1-argo_cmd_b-005 a1-argo_cmd_b-006 a1-argo_cmd_b-007 a1-cn_open-004 a1-err_desc_a-001 a1-err_name_retr-002 a1-err_name_retr-005
a1-err_name_retr-006 a1-ns_default_a-007 a1-ns_default_b-001 a1-ns_default_b-003 a1-ns_default_b-004 a1-ns_key_desc_b-001
a1-ns_key_desc_c-002 a1-ns_type_b-002 a1-ns_values_a-008 a1-ns_values_b-003 a1-real_error-003 a1-real_flag-001 a1-real_flag-002
a1-real_flag-005 a1-real_key_plain-003 a1-real_key_plain-005 a1-real_key_plain-007 a1-real_rest-002 a1-rest_which_a-002
a1-sd_default_c-001 a1-sd_default_c-003 a1-sd_default_c-008 a1-vt_default_a-003 a1-vt_default_a-008 a1-vt_update_a-007
a1-vt_update_b-003 a1-vt_update_b-009 a1-vtrap_added_a-010 a1-vtrap_default_b-001 a1-wc_a-004 a1-wc_a-005 a1-wc_a-007 a1-wc_b-004
a1-wc_b-006 a1-wc_c-001 a1-wc_c-005 a1-wc_d-006 a1-wc_e-002 a1-wc_e-004 a1-wc_e-007 a1-wr_pdb_a-003 a1-ws_scopes_set-005""".split()

DRAW_RULE = (
    "M1: the 56 tier_a items whose strict (S) outcome is correct_decline, wrong_decline or abstained (fixed list, design section 3). "
    "M2: sorted(random.Random(20261122).sample(sorted(rest), 40)) with rest = the tier_a ids that are not in M1 (195). "
    "M3: sorted(random.Random(20261123).sample(sorted(ids), 20)) with ids = the tier_a3 ids (88).")

RULE_TEXT = """\
## The rule of the development check (fixed before the first answer; runs/m5/reports/g0_grounded_design.md, section 3)

Each variant (G1 = grounded-g1, G2 = grounded-g2) answers the stored default packs (replayed) with the 4-bit model, thinking off, 2,048-token limit.
Reference = the stored default answers on the same packs. A variant is eligible only if all of these hold:
1. (a) Judged coverage P on J (B1 30 + pilot2 24 = 54 answers, mean of two judge passes): P(V) - P(default) >= -0.05.
2. (b) Supported essential-miss units per 10 answers, on B1 (30 answers): V <= default + 1.0.
3. (c) Answers cut at the 2,048-token limit, on J: at most 5 of 54.
4. (d) Median completion tokens on J: at most 1,400.
5. (e) Projected mechanical change >= -3.0 points, where the projection is (dM1 + 4.875 dM2 + 4.4 dM3) / 339 * 100 (d = all-pass count of V minus
   the reference on the same items; with --mechanical all: the change over the 339 items / 339 * 100), and format failures of V on the
   mechanical items no more than the reference's + 2.
6. (f) Wrong declines at most 10 % of the items whose covering chunk is in the pack, on the open mechanical half, estimated from the sample
   with the weights of (e): (wd_M1 + 4.875 wd_M2) / (cov_M1 + 4.875 cov_M2) (with --mechanical all: all 251 tier_a items, weight 1). The share is
   weighted because the sample over-represents declines by design (M1 is the 56 items that strict declined or abstained on), so an unweighted
   share over the sample is not the share on the open half. This weighted share decides. Printed beside it, deciding nothing: the unweighted
   share over the sample, and the share of all declines (correct + wrong), weighted and unweighted.
7. (g) Nonexistent identifiers per 10 answers on J (scripts/eval2_idcheck.py): V <= default.
Reported, deciding nothing: (h) gap sentences per answer on J, answers that open with meta text, median seconds, M1 declines.
Choice: G1 if eligible; otherwise G2 if eligible; otherwise neither (stop, back to design, B2 not spent).
A measure that is missing, not a number or not computed is an error, never a pass.
"""


class RuleError(RuntimeError):
    pass


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def sha256_text(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def rd(p):
    return [json.loads(l) for l in open(p, encoding="utf-8") if l.strip()]


def wr(p, rows):
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------- ids
def draw(tier_a, tier_a3):
    a = rd(tier_a)
    a3 = rd(tier_a3)
    a_ids = {r["id"] for r in a}
    miss = [i for i in M1_IDS if i not in a_ids]
    if miss or len(set(M1_IDS)) != len(M1_IDS) or len(M1_IDS) != POP["M1"]:
        raise RuleError(f"M1: {len(M1_IDS)} ids, {len(miss)} not in {tier_a} (first: {miss[:1]})")
    rest = sorted(a_ids - set(M1_IDS))
    if len(rest) != POP["M2"]:
        raise RuleError(f"M2 population: {len(rest)} tier_a ids outside M1, the design says {POP['M2']}")
    ids3 = sorted(r["id"] for r in a3)
    if len(ids3) != POP["M3"]:
        raise RuleError(f"M3 population: {len(ids3)} tier_a3 ids, the design says {POP['M3']}")
    m2 = sorted(random.Random(M2_SEED).sample(rest, M2_N))
    m3 = sorted(random.Random(M3_SEED).sample(ids3, M3_N))
    kind = {r["id"]: r.get("kind") for r in a + a3}
    cnt = lambda ids: dict(sorted(((k, sum(1 for i in ids if kind[i] == k)) for k in {kind[i] for i in ids})))  # noqa: E731
    return {"rule": DRAW_RULE, "M1": sorted(M1_IDS), "M2": m2, "M3": m3,
            "counts": {"M1": len(M1_IDS), "M2": len(m2), "M3": len(m3), "total": len(M1_IDS) + len(m2) + len(m3)},
            "kinds": {"M1": cnt(M1_IDS), "M2": cnt(m2), "M3": cnt(m3)}}


def kind_differences(d):
    out = []
    for name, want in (("M2", M2_KINDS), ("M3", M3_KINDS)):
        got = d["kinds"][name]
        if got != want:
            out.append(f"{name} kinds differ from the design: drawn {got}, design {dict(sorted(want.items()))}")
    return out


def cmd_draw_ids(a):
    d = draw(a.tier_a, a.tier_a3)
    diffs = kind_differences(d)
    print(f"drew M1 {d['counts']['M1']}, M2 {d['counts']['M2']}, M3 {d['counts']['M3']} = {d['counts']['total']} ids")
    for name in ("M1", "M2", "M3"):
        print(f"  {name} kinds: {d['kinds'][name]}")
    for x in diffs:
        print("DIFFERENCE: " + x)
    if a.check:
        want = json.load(open(a.check, encoding="utf-8"))
        same = all(want.get(k) == d[k] for k in ("M1", "M2", "M3"))
        print(f"{a.check}: {'reproduces from its rule' if same else 'DOES NOT reproduce from its rule'}")
        return 0 if same else 1
    if a.out:
        if Path(a.out).exists():
            print(f"refused: {a.out} exists; not overwritten", file=sys.stderr)
            return 2
        Path(a.out).write_text(json.dumps(d, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"wrote {a.out}")
    return 0


def cmd_verify_m1(a):
    rows = rd(a.scores)
    got = sorted(r["id"] for r in rows if r.get("outcome") in ("correct_decline", "wrong_decline", "abstained"))
    want = sorted(M1_IDS)
    print(f"strict items with outcome correct_decline, wrong_decline or abstained: {len(got)}; the list of M1: {len(want)}")
    extra, short = sorted(set(got) - set(want)), sorted(set(want) - set(got))
    print(f"in the scores, not in M1: {extra}; in M1, not in the scores: {short}")
    return 0 if not extra and not short else 1


def cmd_select(a):
    d = json.load(open(a.ids, encoding="utf-8"))
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, items_p, ref_p in (("ma", a.tier_a, a.ref_a), ("m3", a.tier_a3, a.ref_a3)):
        items = rd(items_p)
        if a.mechanical == "all":
            want = [r["id"] for r in items]
        else:
            want = set(d["M1"] + d["M2"]) if name == "ma" else set(d["M3"])
        keep = [r for r in items if r["id"] in want]
        if len(keep) != len(want):
            raise RuleError(f"{name}: {len(keep)} items found for {len(want)} ids")
        ids_kept = {r["id"] for r in keep}
        ref = [r for r in rd(ref_p) if r["id"] in ids_kept]
        if {r["id"] for r in ref} != ids_kept:
            raise RuleError(f"{name}: the reference answers {ref_p} lack some of the {len(keep)} ids")
        wr(out / f"items_{name}.jsonl", keep)
        wr(out / f"ref_{name}.jsonl", ref)
        print(f"{name}: {len(keep)} items, {len(ref)} reference answers")
    return 0


def cmd_harness_items(a):
    rows = rd(getattr(a, "in"))
    with open(a.out, "w", encoding="utf-8") as f:
        for r in rows:
            if not r.get("id") or not r.get("question"):
                raise RuleError("item without id or question")
            row = {"tier": "B", "kind": "open", "family": r["id"], "domain": a.domain, **r}
            if isinstance(row["domain"], list):
                row["domains"], row["domain"] = row["domain"], ",".join(map(str, row["domain"])) or a.domain
            for k in ("tier", "kind", "family", "domain"):
                if not isinstance(row[k], str) or not row[k]:
                    raise RuleError(f"item {r['id']}: {k} is not a non-empty string")
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"{a.out}: {len(rows)} harness items")
    return 0


# ---------------------------------------------------------------- the manifest, the markers and the start check
def prompt_hashes(frozen):
    return json.load(open(frozen, encoding="utf-8"))["prompts_sha256"]


def kv(items):
    out = {}
    for x in items or []:
        k, sep, v = x.partition("=")
        if not sep or not k:
            raise RuleError(f"not NAME=VALUE: {x!r}")
        out[k] = v
    return out


def current_state(a):
    """The state computed now: pinned files by sha256, the prompt hashes of frozen.json, the options, the rule text."""
    missing = [p for p in a.pin if not Path(p).is_file()]
    if missing:
        raise RuleError(f"pinned file missing: {missing[0]}" + (f" (and {len(missing) - 1} more)" if len(missing) > 1 else ""))
    return {"schema": SCHEMA, "kind": "dry" if a.dry else "real", "mechanical": a.mechanical, "model": a.model or None,
            "rule_text_sha256": sha256_text(RULE_TEXT), "prompts_sha256": prompt_hashes(a.frozen),
            "judge_args": kv(a.judge_arg), "info": kv(a.info),
            "pins": {p: sha256_file(p) for p in sorted(a.pin)}}


def manifest_hash(m):
    return sha256_text(json.dumps(m, sort_keys=True))


def diff_state(old, new):
    out = []
    for k in sorted(set(old) | set(new)):
        if k == "info" or old.get(k) == new.get(k):     # "info" (the git head) is recorded for the reader and never decides
            continue
        if isinstance(old.get(k), dict) and isinstance(new.get(k), dict):
            for kk in sorted(set(old[k]) | set(new[k])):
                if old[k].get(kk) != new[k].get(kk):
                    out.append(f"{k}.{kk}: stored {str(old[k].get(kk))[:16]}, now {str(new[k].get(kk))[:16]}")
        else:
            out.append(f"{k}: stored {old.get(k)!r}, now {new.get(k)!r}")
    return out


def add_state_args(p):
    p.add_argument("--manifest", required=True)
    p.add_argument("--work", required=True)
    p.add_argument("--pin", action="append", default=[], help="a pinned input file (sha256 recorded)")
    p.add_argument("--frozen", default="kafka_sre_rag/frozen.json")
    p.add_argument("--mechanical", choices=["sample", "all"], default="sample")
    p.add_argument("--model", default=None, help="the 4-bit model id (real runs)")
    p.add_argument("--dry", action="store_true")
    p.add_argument("--judge-arg", action="append", default=[], help="NAME=VALUE of the judge's arguments (setting, extra body, source characters); compared")
    p.add_argument("--info", action="append", default=[], help="NAME=VALUE recorded for the reader (the git head); never compared")


def cmd_manifest(a):
    new = current_state(a)
    if Path(a.manifest).exists():
        print(f"manifest exists: {a.manifest}")
        return 0
    Path(a.manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(a.manifest).write_text(json.dumps(new, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written: {a.manifest} ({len(new['pins'])} pinned files, rule text {new['rule_text_sha256'][:12]})")
    return 0


def markers(work):
    return sorted(Path(work).glob("stage_*.done"))


def cmd_start_check(a):
    """Refuse (exit 2) when the stored manifest differs from the state now, when a marker has no manifest, when a marker was written
    for another manifest, or when an output the marker recorded is missing or changed. Moves and deletes nothing."""
    mk = markers(a.work)
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
    except (RuleError, ValueError, OSError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    diffs = diff_state(old, new)
    if diffs:
        print("REFUSED: the stored manifest differs from the state now (nothing was moved or deleted):", file=sys.stderr)
        for d in diffs[:12]:
            print("  " + d, file=sys.stderr)
        return 2
    mh = manifest_hash(old)
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
    print(f"start check: manifest equal to the state now; {len(mk)} stage(s) done: {', '.join(m.stem[6:] for m in mk) or 'none'}")
    return 0


def cmd_mark(a):
    mp = Path(a.work) / "MANIFEST.json"
    mh = manifest_hash(json.load(open(mp, encoding="utf-8")))
    rec = {"stage": a.stage, "manifest_sha256": mh, "outputs": {f: sha256_file(f) for f in a.out}}
    (Path(a.work) / f"stage_{a.stage}.done").write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"stage {a.stage} done ({len(a.out)} output files hashed)")
    return 0


# ---------------------------------------------------------------- answer checks
def cmd_check_answers(a):
    p = Path(a.file)
    if not p.is_file():
        print(f"check {a.file}: missing")
        return 1
    rows = rd(p)
    err = [r["id"] for r in rows if r.get("error")]
    empty = [r["id"] for r in rows if not r.get("error") and not (r.get("text") or "").strip()]
    nopack = [r["id"] for r in rows if not r.get("error") and not r.get("pack")]
    mp = Path(str(p) + ".manifest.json")
    man = json.load(open(mp)) if mp.exists() else {}
    ended = bool(man.get("ended"))
    modeok = a.mode is None or man.get("mode") == a.mode
    print(f"check {a.file}: {len(rows)} of {a.expect} rows, {len(err)} errors, {len(empty)} empty, {len(nopack)} without a pack, "
          f"manifest ended: {'yes' if ended else 'no'}, mode {man.get('mode')}{'' if modeok else ' (WRONG)'}")
    return 0 if len(rows) == a.expect and not err and not empty and not nopack and ended and modeok else 1


def cmd_determinism(a):
    stored = {r["id"]: r for r in rd(a.stored)}
    new = rd(a.new)[: a.n]
    if len(new) < a.n:
        print(f"determinism: {len(new)} replayed answers, {a.n} expected")
        return 1
    bad = 0
    for r in new:
        s = stored.get(r["id"])
        same = s is not None and s.get("text") == r.get("text") and bool(s.get("truncated")) == bool(r.get("truncated"))
        print(f"determinism {r['id']}: {'identical' if same else 'DIFFERENT'} ({len(r.get('text') or '')} characters)")
        bad += not same
    if bad:
        print(f"determinism check FAILED: {bad} of {a.n} replayed default answers differ from the stored ones. The stored default answers "
              "cannot be used as the reference: default must be answered again on the same sets, then the chain starts again.")
        return 1
    print(f"determinism check passed: {a.n} of {a.n} replayed default answers equal the stored ones, byte for byte")
    return 0


def cmd_packs_equal(a):
    A = {r["id"]: r.get("pack") for r in rd(a.a)}
    B = rd(a.b)
    bad = [r["id"] for r in B if A.get(r["id"]) != r.get("pack")]
    print(f"packs of {a.b} equal to those of {a.a}: {len(B) - len(bad)} of {len(B)}")
    return 0 if not bad else 1


# ---------------------------------------------------------------- the measures
def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def parse_idcheck(path):
    txt = open(path, encoding="utf-8").read()
    n = re.search(r"^answers (\d+)\s+checked (\d+)", txt, re.M)
    k = re.search(r"^nonexistent identifiers: (\d+)", txt, re.M)
    if not n or not k:
        raise RuleError(f"{path}: not an idcheck totals file")
    return int(n.group(2)), int(k.group(1))


def judged(results_path, rubrics, set_names):
    """{(set, id): (mean P over the passes, mean supported essential-miss units over the passes)}."""
    import eval2_judge as ej
    rub = {r["id"]: r for r in rd(rubrics)}
    tmp = {}
    for r in rd(results_path):
        if not r.get("valid_reply") or r.get("reference_given") or r["set"] not in set_names:
            continue
        c = ej.coverage_row(r, rub[r["id"]])
        sm = sum(1 for u in c["units"] if u["status"] == "missed" and u.get("supported"))
        tmp.setdefault((r["set"], r["id"]), []).append((c["P"], sm))
    return {k: (sum(x[0] for x in v) / len(v), sum(x[1] for x in v) / len(v), len(v)) for k, v in tmp.items()}


META = re.compile(r"^\s*(based on|according to|from|using|looking at|the (retrieved )?(excerpts?|documentation|sources?|context)|"
                  r"in the (retrieved )?(excerpts?|documentation|sources?|context)|here is|here's|let me|i (will|'ll|can))\b", re.I)


def answer_stats(rows, default_rows, is_default=False, stub=False):
    """Length, cut, time and text measures of one answer set (rows) on J. default_rows gives the search and retrieval seconds a replay skipped."""
    cut = sum(1 for r in rows if r.get("truncated"))
    toks = [r.get("tokens") if (r.get("tokens") or not stub) else max(1, len(r.get("text") or "") // 4) for r in rows]   # stub: no model, no token count
    if any(not num(t) for t in toks):
        raise RuleError("an answer row has no completion token count")
    d = {r["id"]: r for r in default_rows}
    own = lambda r: r["seconds"]["answer"]  # noqa: E731
    tot = lambda r: r["seconds"]["total"] + (0.0 if is_default else (d[r["id"]]["seconds"].get("search_terms") or 0) + (d[r["id"]]["seconds"].get("retrieval") or 0))  # noqa: E731
    gaps = sum((r.get("text") or "").count(GAP) for r in rows)
    meta = sum(1 for r in rows if META.search((r.get("text") or "").lstrip()[:200]))
    return {"n": len(rows), "cut": cut, "median_tokens": median(toks), "median_answer_s": median([own(r) for r in rows]),
            "median_total_s": median([tot(r) for r in rows]), "gap_sentences": gaps, "meta_openers": meta}


def score_stats(path, ids_by_part):
    rows = {r["id"]: r for r in rd(path)}
    out = {"n": len(rows), "format_failures": sum(1 for r in rows.values() if not r.get("format_ok")),
           "all_pass": {}, "outcomes": {}}
    for part, ids in ids_by_part.items():
        miss = [i for i in ids if i not in rows]
        if miss:
            raise RuleError(f"{path}: no score row for {len(miss)} ids of {part} (first: {miss[0]})")
        out["all_pass"][part] = sum(1 for i in ids if rows[i]["all_pass"])
    cnt = {}
    for r in rows.values():
        o = r.get("outcome")
        if o is None:
            raise RuleError(f"{path}: a score row has no decline outcome (run the scorer without --no-outcomes)")
        cnt[o] = cnt.get(o, 0) + 1
    out["outcomes"] = cnt
    out["parts"] = {}
    for part, ids in ids_by_part.items():
        rs = [rows[i] for i in ids]
        out["parts"][part] = {"n": len(rs), "wrong_declines": sum(1 for r in rs if r["outcome"] == "wrong_decline"),
                              "covered": sum(1 for r in rs if (r.get("coverage") or {}).get("covering_in_pack")),
                              "declines": sum(1 for r in rs if r["outcome"] in ("wrong_decline", "correct_decline"))}
    out["covered"] = sum(1 for r in rows.values() if (r.get("coverage") or {}).get("covering_in_pack"))
    out["wrong_declines"] = cnt.get("wrong_decline", 0)
    out["declines"] = cnt.get("wrong_decline", 0) + cnt.get("correct_decline", 0)
    return out


def f_weights(ids, mechanical):
    """The weights of the projection of (e), used for (f): sample part -> population / sample size; --mechanical all: every item counts once."""
    if mechanical == "all":
        return {"all": 1.0}
    return {k: POP[k] / len(ids[k]) for k in ("M1", "M2")}


def f_estimate(parts, weights):
    """parts: {part: {"wrong_declines", "covered", "declines"}} (score_stats()["parts"]). Weighted sums decide (f); the unweighted ones are printed."""
    if set(parts) != set(weights):
        raise RuleError(f"(f): parts {sorted(parts)} and weights {sorted(weights)} differ")
    out = {"weights": dict(weights)}
    for k, name in (("wrong_declines", "wd"), ("covered", "covered"), ("declines", "declines")):
        out[name + "_weighted"] = sum(weights[p] * parts[p][k] for p in parts)
        out[name] = sum(parts[p][k] for p in parts)
    return out


def pct(a, b):
    return "n/a" if not b else f"{100 * a / b:.1f} %"


def cmd_f_estimate(a):
    ids = json.load(open(a.ids, encoding="utf-8"))
    rows = rd(a.scores)
    parts_s = {"M1": ids["M1"], "M2": ids["M2"]}
    est = f_estimate(score_stats(a.scores, parts_s)["parts"], f_weights(ids, "sample"))
    full = score_stats(a.scores, {"all": [r["id"] for r in rows]})["parts"]["all"]
    print(f"{a.scores}: sample M1 {len(ids['M1'])} + M2 {len(ids['M2'])}, weights {est['weights']}")
    print(f"  weighted estimate on the sample: wrong declines {est['wd_weighted']:.1f} of {est['covered_weighted']:.1f} covered = "
          f"{pct(est['wd_weighted'], est['covered_weighted'])}")
    print(f"  unweighted over the sample: {est['wd']} of {est['covered']} = {pct(est['wd'], est['covered'])}")
    print(f"  full set ({full['n']} items): {full['wrong_declines']} of {full['covered']} = {pct(full['wrong_declines'], full['covered'])}")
    return 0


F_NOTE = ("Known bias of the decline detector: an answer without an ANSWER line that fails a check and contains a gap sentence is counted as a "
          "decline. That counts against the grounded variants, which write gap sentences. A reader checks the answers counted as wrong "
          "declines before a failure of (f) is acted on.")


def cmd_check_echo(a):
    def echoed(p):
        return json.load(open(str(p), encoding="utf-8")).get("model_echoed")
    stored = {p: echoed(p) for p in a.stored}
    new = echoed(a.new)
    ok = new is not None and all(v == new for v in stored.values())
    print(f"model echoed by the server: this run {new!r}; stored runs {sorted(set(map(repr, stored.values())))}: {'equal' if ok else 'DIFFERENT'}")
    if not ok and a.dry:
        print("dry run: the stub backend echoes no model; the comparison is not binding here (the real run stops)")
        return 0
    return 0 if ok else 1


def cmd_judge_compare(a):
    """Deciding nothing: how many answers' two-pass means (P, supported essential-miss units) differ between the stored judge results and this run's."""
    rub = a.rubrics
    old = judged(a.stored, rub, {a.set})
    new = judged(a.new, rub, {a.set})
    ids = sorted({i for _, i in old} | {i for _, i in new})
    diff = [i for i in ids if (a.set, i) not in old or (a.set, i) not in new
            or abs(old[(a.set, i)][0] - new[(a.set, i)][0]) > 1e-9 or abs(old[(a.set, i)][1] - new[(a.set, i)][1]) > 1e-9]
    both = [i for i in ids if (a.set, i) in old and (a.set, i) in new]
    pm = lambda d: sum(d[(a.set, i)][0] for i in both) / len(both) if both else float("nan")  # noqa: E731
    print(f"stored judge results against this run's, set {a.set}: {len(diff)} of {len(ids)} answers differ in the two-pass mean "
          f"(P mean stored {pm(old):.3f}, this run {pm(new):.3f}); printed, decides nothing")
    return 0


def cmd_measures(a):
    W = Path(a.work)
    ids = json.load(open(a.ids, encoding="utf-8"))
    parts_a = {"M1": ids["M1"], "M2": ids["M2"]} if a.mechanical == "sample" else None
    items_ma = rd(W / "items_ma.jsonl")
    items_m3 = rd(W / "items_m3.jsonl")
    if parts_a is None:     # --mechanical all: one part, all the items of both files
        parts_a = {"all": [r["id"] for r in items_ma]}
        parts_3 = {"all": [r["id"] for r in items_m3]}
    else:
        parts_3 = {"M3": ids["M3"]}
    stored = {"b1": a.stored_b1, "p2": a.stored_p2}
    rub = {"b1": a.rubrics_b1, "p2": a.rubrics_p2}
    sets_n = {"b1": len(rd(rub["b1"])), "p2": len(rd(rub["p2"]))}
    m = {"schema": SCHEMA, "mechanical": a.mechanical, "sizes": {"J": sets_n["b1"] + sets_n["p2"], "J_b1": sets_n["b1"],
                                                               "J_p2": sets_n["p2"]}, "variants": {}}
    m["sizes"]["mech"] = {k: len(parts_a.get(k, [])) + len(parts_3.get(k, [])) if k == "all" else len(v)
                          for k, v in {**parts_a, **parts_3}.items()}
    m["pop"] = POP if a.mechanical == "sample" else {"all": POP_ALL}
    fw = f_weights(ids, a.mechanical)
    ref_scores = {"ma": score_stats(W / "score_ma_ref.jsonl", parts_a), "m3": score_stats(W / "score_m3_ref.jsonl", parts_3)}
    names = {"default": "default", "g1": "g1", "g2": "g2"}
    for v in ("default",) + VARIANTS:
        P, SM, nidc, kidc = [], [], 0, 0
        rows_all, d_rows_all = [], []
        stats = {}
        for s in ("b1", "p2"):
            path = stored[s] if v == "default" else str(W / f"ans_{s}_{v}.jsonl")
            rows = rd(path)
            ds = rd(stored[s])
            rows_all += rows
            d_rows_all += ds
            jr = judged(str(W / f"judge_{s}.jsonl"), rub[s], set(names.values()))
            for r in rows:
                j = jr.get((v, r["id"]))
                if j is None:
                    raise RuleError(f"no valid judge result for {v} {s} {r['id']}")
                P.append(j[0])
                if s == "b1":
                    SM.append(j[1])
            n_, k_ = parse_idcheck(W / f"idcheck_{s}_{v}.txt")
            nidc += n_
            kidc += k_
        stats = answer_stats(rows_all, d_rows_all, v == "default", a.stub)
        e = {"P": sum(P) / len(P), "answers_judged": len(P), "supp_miss_per10": 10 * sum(SM) / len(SM), "supp_miss_answers": len(SM),
             "cut": stats["cut"], "answers": stats["n"], "median_tokens": stats["median_tokens"],
             "median_answer_s": stats["median_answer_s"], "median_total_s": stats["median_total_s"],
             "idents_per10": 10 * kidc / nidc if nidc else None, "idents": kidc, "idents_answers": nidc,
             "gap_sentences_per_answer": stats["gap_sentences"] / stats["n"], "meta_openers": stats["meta_openers"]}
        if v != "default":
            sa = score_stats(W / f"score_ma_{v}.jsonl", parts_a)
            s3 = score_stats(W / f"score_m3_{v}.jsonl", parts_3)
            fe = f_estimate(sa["parts"], fw)
            d = {k: sa["all_pass"][k] - ref_scores["ma"]["all_pass"][k] for k in parts_a}
            for k in parts_3:       # in the one-part case ("all") the two files add up
                d[k] = d.get(k, 0) + s3["all_pass"][k] - ref_scores["m3"]["all_pass"][k]
            e["mech"] = {"d_all_pass": d, "all_pass": {"ma": sa["all_pass"], "m3": s3["all_pass"]},
                         "ref_all_pass": {"ma": ref_scores["ma"]["all_pass"], "m3": ref_scores["m3"]["all_pass"]},
                         "format_failures": sa["format_failures"] + s3["format_failures"],
                         "ref_format_failures": ref_scores["ma"]["format_failures"] + ref_scores["m3"]["format_failures"],
                         "wrong_declines": fe["wd"], "covered": fe["covered"], "declines": fe["declines"],
                         "wd_weighted": fe["wd_weighted"], "covered_weighted": fe["covered_weighted"], "declines_weighted": fe["declines_weighted"],
                         "f_weights": fe["weights"],
                         "outcomes_tier_a": sa["outcomes"], "outcomes_a3": s3["outcomes"],
                         "m1_declines": None}
            if a.mechanical == "sample":
                rows = {r["id"]: r for r in rd(W / f"score_ma_{v}.jsonl")}
                e["mech"]["m1_declines"] = sum(1 for i in ids["M1"] if rows[i].get("outcome") in ("correct_decline", "wrong_decline"))
        m["variants"][v] = e
    (W / "measures.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (W / "measures.md").write_text(measures_md(m), encoding="utf-8")
    print(measures_md(m))
    return 0


def measures_md(m):
    L = ["### Measures (a) to (h)", "",
         f"J = {m['sizes']['J']} answers (B1 {m['sizes']['J_b1']}, pilot2 {m['sizes']['J_p2']}); mechanical sets: {m['sizes']['mech']} ({m['mechanical']}).", "",
         "| variant | P (a) | supp. essential-miss per 10, B1 (b) | cut (c) | median tokens (d) | median s answer / with search | nonexistent ids per 10 (g) | gap sentences per answer | meta openers |",
         "|---|---|---|---|---|---|---|---|---|"]
    f = lambda x, d=2: "n/a" if x is None else f"{x:.{d}f}"  # noqa: E731
    for v, e in m["variants"].items():
        L.append(f"| {v} | {f(e['P'], 3)} (n {e['answers_judged']}) | {f(e['supp_miss_per10'], 2)} (n {e['supp_miss_answers']}) | {e['cut']} of {e['answers']} | "
                 f"{f(e['median_tokens'], 0)} | {f(e['median_answer_s'], 1)} / {f(e['median_total_s'], 1)} | {f(e['idents_per10'], 2)} ({e['idents']} in {e['idents_answers']}) | "
                 f"{f(e['gap_sentences_per_answer'], 2)} | {e['meta_openers']} |")
    L += ["", "| variant | all-pass change per part (e) | format failures (variant / reference) | wrong declines / covered, weighted (f) | wrong declines / covered, unweighted | declines (weighted / unweighted) | M1 declines |", "|---|---|---|---|---|---|---|"]
    for v, e in m["variants"].items():
        if "mech" in e:
            x = e["mech"]
            L.append(f"| {v} | {x['d_all_pass']} (variant {x['all_pass']}, reference {x['ref_all_pass']}) | {x['format_failures']} / {x['ref_format_failures']} | "
                     f"{x['wd_weighted']:.1f} / {x['covered_weighted']:.1f} | {x['wrong_declines']} / {x['covered']} | "
                     f"{x['declines_weighted']:.1f} / {x['declines']} | {x['m1_declines']} |")
    for v, e in m["variants"].items():
        if "mech" in e:
            L.append(f"\nDecline outcomes of {v}: tier_a part {e['mech']['outcomes_tier_a']}; tier_a3 part {e['mech']['outcomes_a3']}")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- the rule
def need(d, *path):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            raise RuleError("input missing: " + ".".join(path))
        cur = cur[p]
    if not num(cur):
        raise RuleError("input is not a number: " + ".".join(path))
    return cur


def evaluate(m):
    """-> (lines, eligible {variant: bool}, choice). Raises RuleError when any input is missing."""
    for k in ("variants", "sizes", "pop"):
        if k not in m:
            raise RuleError("input missing: " + k)
    d = m["variants"].get("default")
    if not isinstance(d, dict):
        raise RuleError("input missing: variants.default")
    Pd = need(m, "variants", "default", "P")
    Sd = need(m, "variants", "default", "supp_miss_per10")
    Id = need(m, "variants", "default", "idents_per10")
    nJ = need(m, "sizes", "J")
    sizes = m["sizes"].get("mech")
    if not isinstance(sizes, dict) or not sizes:
        raise RuleError("input missing: sizes.mech")
    L, elig = [], {}
    for v in VARIANTS:
        pre = ("variants", v)
        P = need(m, *pre, "P")
        S = need(m, *pre, "supp_miss_per10")
        cut = need(m, *pre, "cut")
        med = need(m, *pre, "median_tokens")
        ident = need(m, *pre, "idents_per10")
        dall = m["variants"][v].get("mech", {}).get("d_all_pass") if isinstance(m["variants"][v].get("mech"), dict) else None
        if not isinstance(dall, dict) or set(dall) != set(sizes) or set(m["pop"]) != set(sizes):
            raise RuleError(f"input missing: variants.{v}.mech.d_all_pass for the parts {sorted(sizes)}")
        proj = 0.0
        for part, dv in dall.items():
            if not num(dv) or not num(sizes[part]) or sizes[part] <= 0 or not num(m["pop"][part]):
                raise RuleError(f"input is not a number: variants.{v}.mech.d_all_pass.{part}")
            proj += dv * m["pop"][part] / sizes[part]
        proj = proj / sum(m["pop"].values()) * 100
        ff = need(m, *pre, "mech", "format_failures")
        ffr = need(m, *pre, "mech", "ref_format_failures")
        wd = need(m, *pre, "mech", "wrong_declines")
        cov = need(m, *pre, "mech", "covered")
        dec = need(m, *pre, "mech", "declines")
        wdw = need(m, *pre, "mech", "wd_weighted")
        covw = need(m, *pre, "mech", "covered_weighted")
        decw = need(m, *pre, "mech", "declines_weighted")
        if covw <= 0:
            raise RuleError(f"input is not usable: variants.{v}.mech.covered_weighted is {covw}")
        eps = 1e-9
        c = {
            "a": (P - Pd >= -0.05 - eps, f"(a) coverage P: {v} {P:.3f}, default {Pd:.3f}, difference {P - Pd:+.3f} (needs >= -0.05; {int(nJ)} answers, two passes)"),
            "b": (S <= Sd + 1.0 + eps, f"(b) supported essential-miss units per 10 on B1: {v} {S:.2f}, default {Sd:.2f} (needs <= default + 1.0; {int(m['sizes']['J_b1'])} answers)"),
            "c": (cut <= 5, f"(c) answers cut at 2,048 tokens: {int(cut)} of {int(nJ)} (needs <= 5)"),
            "d": (med <= 1400, f"(d) median completion tokens: {med:.0f} (needs <= 1,400; {int(nJ)} answers)"),
            "e": (proj >= -3.0 - eps and ff <= ffr + 2,
                  f"(e) projected mechanical change {proj:+.2f} points (needs >= -3.0; parts {dall}, sizes {sizes}); format failures {int(ff)} against the reference's {int(ffr)} (needs <= reference + 2)"),
            "f": (wdw <= 0.10 * covw + eps,
                  f"(f) wrong declines, weighted like (e): {wdw:.1f} of {covw:.1f} items whose covering chunk is in the pack = {pct(wdw, covw)} "
                  f"(needs <= 10 %; this decides); beside it, deciding nothing: unweighted over the sample {int(wd)} of {int(cov)} = {pct(wd, cov)}; "
                  f"share of all declines {wdw:.1f} of {decw:.1f} = {pct(wdw, decw)} weighted, {int(wd)} of {int(dec)} = {pct(wd, dec)} unweighted"),
            "g": (ident <= Id + eps, f"(g) nonexistent identifiers per 10 answers: {v} {ident:.2f}, default {Id:.2f} (needs <= default; {int(nJ)} answers)"),
        }
        L.append(f"Variant {v.upper()}:")
        for k in "abcdefg":
            L.append(f"  {'PASS' if c[k][0] else 'FAIL'}  {c[k][1]}")
        elig[v] = all(x[0] for x in c.values())
        L.append(f"  => {v.upper()} is {'ELIGIBLE' if elig[v] else 'NOT eligible'}")
        L.append("")
    choice = "G1" if elig["g1"] else ("G2" if elig["g2"] else "neither")
    return L, elig, choice


def cmd_rule(a):
    try:
        m = json.load(open(a.measures, encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"ERROR: the measures file {a.measures} cannot be read ({type(e).__name__}); no verdict", file=sys.stderr)
        return 2
    try:
        L, elig, choice = evaluate(m)
    except RuleError as e:
        print(f"ERROR: {e}; a missing input is not a pass; no verdict", file=sys.stderr)
        return 2
    print("\n".join(L))
    print(f"CHOICE: {choice}" + ("  (stop: back to design, B2 is not spent)" if choice == "neither" else ""))
    return 0


# ---------------------------------------------------------------- main
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("rule-text")
    p = sub.add_parser("draw-ids")
    p.add_argument("--tier-a", required=True)
    p.add_argument("--tier-a3", required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--out")
    g.add_argument("--check")
    p = sub.add_parser("verify-m1")
    p.add_argument("--scores", required=True)
    p = sub.add_parser("select")
    p.add_argument("--ids", required=True)
    p.add_argument("--mechanical", choices=["sample", "all"], default="sample")
    for k in ("tier-a", "tier-a3", "ref-a", "ref-a3", "out-dir"):
        p.add_argument("--" + k, required=True)
    p = sub.add_parser("harness-items")
    p.add_argument("--in", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--domain", required=True)
    p = sub.add_parser("manifest")
    add_state_args(p)
    p = sub.add_parser("start-check")
    add_state_args(p)
    p = sub.add_parser("mark")
    p.add_argument("--work", required=True)
    p.add_argument("--stage", required=True)
    p.add_argument("--out", nargs="*", default=[])
    p = sub.add_parser("check-answers")
    p.add_argument("--file", required=True)
    p.add_argument("--expect", type=int, required=True)
    p.add_argument("--mode")
    p = sub.add_parser("determinism")
    p.add_argument("--stored", required=True)
    p.add_argument("--new", required=True)
    p.add_argument("--n", type=int, default=3)
    p = sub.add_parser("packs-equal")
    p.add_argument("--a", required=True)
    p.add_argument("--b", required=True)
    p = sub.add_parser("measures")
    p.add_argument("--work", required=True)
    p.add_argument("--ids", required=True)
    p.add_argument("--mechanical", choices=["sample", "all"], default="sample")
    p.add_argument("--stub", action="store_true", help="dry run: a stub answer has no token count; length/4 stands in (not a measurement)")
    for k in ("stored-b1", "stored-p2", "rubrics-b1", "rubrics-p2"):
        p.add_argument("--" + k, required=True)
    p = sub.add_parser("rule")
    p.add_argument("--measures", required=True)
    p = sub.add_parser("f-estimate")
    p.add_argument("--scores", required=True)
    p.add_argument("--ids", required=True)
    sub.add_parser("f-note")
    p = sub.add_parser("check-echo")
    p.add_argument("--stored", nargs="+", required=True)
    p.add_argument("--new", required=True)
    p.add_argument("--dry", action="store_true")
    p = sub.add_parser("judge-compare")
    for k in ("stored", "new", "rubrics", "set"):
        p.add_argument("--" + k, required=True)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "rule-text":
            print(RULE_TEXT, end="")
            return 0
        if a.cmd == "f-note":
            print(F_NOTE)
            return 0
        fn = {"f-estimate": cmd_f_estimate, "check-echo": cmd_check_echo, "judge-compare": cmd_judge_compare, "draw-ids": cmd_draw_ids, "verify-m1": cmd_verify_m1, "select": cmd_select, "harness-items": cmd_harness_items,
              "manifest": cmd_manifest, "start-check": cmd_start_check, "mark": cmd_mark, "check-answers": cmd_check_answers,
              "determinism": cmd_determinism, "packs-equal": cmd_packs_equal, "measures": cmd_measures, "rule": cmd_rule}[a.cmd]
        return fn(a)
    except (RuleError, OSError, KeyError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
