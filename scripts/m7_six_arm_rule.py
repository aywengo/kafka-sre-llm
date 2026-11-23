#!/usr/bin/env python3
"""M7 K4 helper: the six-arm test chain on D5 (scripts/m7_four_way.sh; agents/m7-plan.md sections 3 and 8). Pure Python, CPU only; no model server, no model.

The test is a screen on worn development data (D5). It decides only whether work continues. The rule is fixed in RULE_TEXT, written into the report header before
any answer exists, and applied by code (`rule`). A measure that is missing, not a number or not computed is an error, never a pass. The rows that need a reader (wrong
statements, harmful statements) are printed as "needs the reading"; the code never prints a final choice for a row that waits for it.

  rule-text                                   the rule, as written into the report header
  gate-inputs --items F --rubrics F --chunk-check F --funnel-work D --perfect F --check-variant V --think-cap N [--dry]
                                              the D5 files against the chunk check, the funnel's packs, the perfect packs and their manifest, the two run parameters
  gate-params ...                             only the two run parameters (the shell calls it first; refusal exit 2)
  frozen-packs --funnel-work D --items F --out F   the D5 frozen packs in the replay format (with a sidecar manifest)
  dry-inputs --dir D                          stand-in inputs for a dry run (synthetic batch, stub expansions, stand-in packs, perfect packs with a manifest)
  code-pins                                   the code files the chain pins
  manifest | start-check ...                  the manifest and the start check (scripts/m6_d5_rule.py's, called with this rule's text)
  derive-draft --src F --out F --arm B        arm B (or F') from the rows of D (or F): the draft before the check
  derive-check --answers F --checks F --out F arm C from A's rows and the records of `kafka_sre_rag.verify_pass run`
  check-verify --file F --answers F --expect N   the check records are complete and error-free
  prompts-relation --a F --b F --items F --kind default|same ...   the prompt hashes of two answer sets stand in the relation the arms promise
  check-rows --file F --expect N --real       every row of a profile run is whole: streamed draft, recorded request-body hash (real runs)
  measures ...                                the measures of the chain; writes measures.json and measures.md
  rule --measures F                           the rule by code
  reading-plan --key F --out F                the session plan of the blind reading (nothing is read here)
  call-counts ...                             expected and found model calls per stage
  leak-check, no-draft-ids, bodies-relation, check-errors, pilot-check, models-check, server-gate
                                              the checks the shell script makes, as commands (each tested with a planted mismatch)

Exit codes: 0 done; 1 a check failed; 2 refused or an input is missing (a missing input is never a pass).
"""
import argparse
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))
import m5_g_rule as G  # noqa: E402  (the development check's helpers: called, not copied)
import m5_b4_served as S  # noqa: E402
import m6_d5_rule as R6  # noqa: E402
from kafka_sre_rag.verify_pass import NO_DRAFT  # noqa: E402  (the text of a draft cut while thinking: imported, never copied)

SCHEMA = 1
VARIANTS = ("V1", "V1C", "none")
BUNDLE_SEED = 20261123           # drawn before any answer exists (it is in the rule text); the names of the answers of a scenario come from it
Z90 = 1.645                      # a 90 % interval, one comparison
Z90_2 = 1.960                    # a 90 % interval corrected for two comparisons (0.025 each side)
KB_BUILD, KB_BUILD_LOWER, KB_A_ONLY = 0.10, 0.04, 0.04
D_WRONG_MAX, D_COV_MIN, D_HARM_PLUS, D_CUT_PCT, D_MED_S, D_P90_S = -2.0, -0.04, 2, 10, 180.0, 270.0
LOOKUP_KINDS = ("name_setting", "error_name", "is_real", "version_trap", "connect_rest")   # tier A kinds where a card built from the question alone can state the expected answer (K7)
ANSWER_LIMIT = 2048              # the answer limit of every arm (tokens); a thinking arm's answer PART is stopped here too (generators answer_cap)
CHECK_ERROR_LIMIT = 2            # of 50 answers: more checking calls with status error than this after the redo passes mark the arm "check failures above limit"
REDO_PASSES = 2                  # a check status error is redone on resume (C, D, F) this many times; the shell script uses the same number
STUB_NOTE = "DRY RUN: stub model, stub judge, stand-in reranker; no number below is a measurement"

# the sets that are judged, by variant: set name -> file stem of its D5 answers
SET_STEMS = {"V1": {"A": "A", "B": "B", "C": "C", "D": "D", "E": "E", "F": "F", "Fp": "Fp"},
             "V1C": {"A": "A", "B": "B", "C": "C", "D": "D", "E": "E", "F": "F", "Fp": "Fp"},
             "none": {"A": "A", "B": "D", "E": "E", "Fp": "F"}}
READ_SETS = {"V1": ("A", "D", "E", "F"), "V1C": ("A", "D", "E", "F"), "none": ("A", "B", "E", "Fp")}   # the sets of the blind bundles
CODE_PINS = tuple(p for p in R6.CODE_PINS if p != "scripts/m6_d5_answers.sh") + (
    "kafka_sre_rag/verify_pass.py", "kafka_sre_rag/kb_lookup.py", "scripts/eval2_blind.py", "scripts/m7_perfect_packs.py",
    "scripts/m7_four_way.sh", "scripts/m7_six_arm_rule.py")
CODE_FILES = ("m7_four_way.sh", "m7_six_arm_rule.py")
# The two questions the Owner decided on 2026-11-23 (each a single place; the text of the decision is in RULE_TEXT):
TIME_FAIL_TAKES_CUT = False      # "Errors improve but time fails" stays NARROW: a cut failure that comes with a time failure is "neither"
NONE_THINKING_MAY_GO_TO_B4 = True   # under --check-variant none the thorough row is applied to B (thinking only) and a pass MAY go on to B4

RULE_TEXT = f"""\
## The rule of the six-arm test on D5 (fixed before the first answer; agents/m7-plan.md section 3; MILESTONE.md decisions V1 to V9)

D5 (eval/m6/d5, 50 scenarios, 269 must points) is worn development data after M6. The test is a screen: it decides only whether work continues, and it allows no
out-of-sample claim and no tuning on D5 items. Every arm uses the default prompt, the frozen search terms and replayed packs (no search call in an answer), the 4-bit model,
one request at a time, and the same transport (the draft is streamed in every arm, thinking switched on or off per call).

**The answer limit is the same in every arm ({ANSWER_LIMIT} tokens).** An arm without thinking (A, E) is limited by the server's token limit of {ANSWER_LIMIT}. An arm with thinking (B, D, F, F') has two
limits: the draft's total limit is {ANSWER_LIMIT} plus the thinking cap (a draft whose reasoning uses all of it is cut while thinking: no draft), and the ANSWER part, counted after the reasoning ended, is
stopped at {ANSWER_LIMIT} tokens by the reader of the stream and marked cut, exactly as an arm without thinking is cut. The server reports token counts only at the end of a stream, so the answer part is counted
in content chunks (one per token on this server; a chunk of two tokens would make the cap looser, never stricter). The report prints, per arm, the median answer-part tokens and the number of answers
over {ANSWER_LIMIT} (taken from the server's own counts where it reports reasoning tokens, else the chunk count); that number must be 0 for every arm, and the rule stops with an error if it is not.

| Arm | Pack | Call |
|---|---|---|
| A | frozen | the thorough path with both switches off (`--answer-profile thorough --no-thinking --no-verify`) |
| B | frozen | thinking on, no check (the draft of D, read from D's rows, not a separate run) |
| C | frozen | the checking pass on A's drafts (`kafka_sre_rag.verify_pass run`) |
| D | frozen | thinking, then the checking pass |
| E | perfect (padded; `work/m7/packs_perfect.jsonl`) | as A |
| F | perfect | as D; its draft is arm F' |

The checking pass's variant (`--check-variant`: V1 without reference cards, V1C with the draft's cards, none: no checking pass, then C, D and F are dropped, D is thinking only
and is B, F is thinking only and is F') and the thinking cap (`--think-cap`: tokens of thinking added to the {ANSWER_LIMIT}-token answer limit) come from the pilot (`--pilot-report`, pinned by its sha256); both and their
sources are in the manifest and in this report. A and D are also answered on the 116 mechanical items of the development sample (scripts/m5_g_rule.py, M1 56, M2 40, M3 20);
the packs of those items are the stored frozen packs.

**What each outcome means (fixed beforehand; decides only whether work continues):**

| Result | Recommendation |
|---|---|
| E − A at least +0.10, lower end above +0.04 | Build the knowledge base: (a), then a pilot of (b) |
| E − A between +0.04 and +0.10 | (a) only |
| E − A below +0.04 | No knowledge base for coverage. (a) only if it lowers nonexistent identifiers |
| D − A wrong statements at most −2.0 per 10, coverage at least −0.04, harmful at most A + 2, cut at most 10 %, median at most 180 s and measured 90th percentile at most 270 s | Thorough profile goes on to B4 |
| Errors improve but time fails | One g2-based thorough arm on worn data, then B4 |
| Both of the above | Both, (a) first, because it feeds the check |
| Neither | Stop both; record it |

B against C shows whether thinking or checking carries an effect. That comparison is descriptive and corrected for two comparisons.

**What is measured:**
- judged coverage (the coverage role of the development check's judge, DeepSeek, setting deepseek-nothink-12k, instruction v2, two passes), over all 269 points and over the 227
  with a covering set and the 42 without separately; the paired difference per scenario with its standard error;
- cut answers (at the token limit, cut while thinking counted apart) and empty answers (no text, or no draft after a cut while thinking);
- time per part (search terms, whole retrieval, draft with its thinking, check), the median and the 90th percentile of the time with search;
- nonexistent identifiers per 10 answers (scripts/eval2_idcheck.py), per arm;
- the 116 mechanical items for A and D (scripts/eval2_score.py), with and without the lookup (decision V7);
- a blind reading of A, D, E and F (all four answers of a scenario in one bundle, names drawn per scenario from the seed {BUNDLE_SEED}; wrong, invented and harmful
  statements by the readers; a quarter read by a second reader). B, C and F' are judged only. The reading is not part of this chain.

**What 50 scenarios can show (sample-size limits):**
- Paired coverage has a standard error of about 0.025 to 0.04, so differences under about 0.06 are not resolved.
- Wrong statements have a standard error of about 1.8 per 10, so a true -2 passes "upper end below 0" only about 30 % of the time.
- Five harmful statements in 50 answers has a 90 % interval of 2.0 to 10.5.
- A 90th percentile from 50 answers is the fifth largest, so the measured value must be at most 270 s.

**How the code reads the table (the plan's words are not changed; these are the readings the code needs):**
- Coverage means the judged coverage P of an answer (the development check's score), mean over the two passes, mean over the scenarios; E − A is the mean of the paired
  differences, its standard error is the standard deviation of the paired differences over the square root of their number (50), and the lower end is the mean minus {Z90} standard errors (a 90 % interval).
  E − A at least +0.10 with a lower end above +0.04 is the first row; E − A at least +0.04 but not the first row is the second ("between +0.04 and +0.10", also when the lower end
  of a difference above +0.10 is not above +0.04); E − A below +0.04 is the third. In the third row "(a) only if it lowers nonexistent identifiers" is NOT TESTED by this test in any variant:
  (a) means reference cards in the draft prompt (plan section 5) and no arm has them (V1C puts cards only into the checking pass, so C against A and D against B would measure the check and the
  cards together). The identifiers per 10 answers of every arm, and the differences C − A and D − B, are printed as description only and decide nothing. A "build" in the first row does not settle (b):
  decision V8 leaves the writers and the licences to the Owner.
- D − A is the difference of the arm D (in the variant none: B, thinking only) from A. Coverage at least -0.04 is the paired mean difference of P. Cut is the share of D5 answers
  truncated at the token limit (cut while thinking included), at most {D_CUT_PCT} % (integer arithmetic). Median and 90th percentile are of the seconds per D5 answer with search:
  the scenario's search-terms seconds from the D5 funnel's stored expansions, plus the median seconds of the whole frozen retrieval measured in this chain, plus the answer's own
  draft (thinking included) and check seconds; the 90th percentile is the value at position floor(0.9 n) of the n sorted times (the fifth largest of 50). The deadline of the profile
  counts from the call here, not from the question (the replay has no search), so it is slightly looser than in use. The seconds also record the time to the first token and the cached prompt
  tokens of each draft (when the server reports them), so that an effect of the server's prompt cache can be seen; D and F run before A and E for the same reason.
- The Owner's decisions of 2026-11-23 on the two readings: (1) "Errors improve but time fails" stays NARROW: it is the case where coverage and cut pass and only the median or the 90th percentile fails;
  a cut failure that comes with a time failure is "neither" (constant TIME_FAIL_TAKES_CUT in the rule script). (2) Under `--check-variant none` the thorough row is applied to B (thinking only) and a pass
  MAY go on to B4 (constant NONE_THINKING_MAY_GO_TO_B4).
- An answer with no draft (cut while thinking) is an empty answer: it is counted as cut and as empty, its judged coverage P is set to 0 and its points count as absent (by code, whatever the judge said of
  the notice text), and it is left out of the blind bundles: a scenario where any of A, D, E, F has no draft is not read, and the key lists it. The readings therefore compare only scenarios in which all four
  arms have a draft; the no-draft counts are reported apart and are never counted as an improvement of the wrong statements.
- Checking-pass failures: the statuses error, not_json, cut and not_checked are counted per arm. A checking call with status error is redone on resume ({REDO_PASSES} redo passes, for C, D and F); an arm where
  more than {CHECK_ERROR_LIMIT} of the 50 answers still have status error after that is marked "check failures above limit" in the rule output and is not read as a clean measurement.
- Wrong statements and harmful statements are counted by the readers. The code prints those conditions as "needs the reading" and, from the conditions that are measured, which
  outcomes are still possible. The combined rows ("Both of the above", "Neither") are read as the combination of the knowledge-base rows (E − A) and the thorough-profile rows (D − A).
- B against C: B − A and C − A with 90 % intervals corrected for two comparisons ({Z90_2} standard errors: 0.025 each side), and B − C with the same interval; descriptive, decides nothing.
- With and without the lookup (V7): the mechanical all-pass counts of A and D over all 116 items, and over the items that remain when the tier A kinds {", ".join(LOOKUP_KINDS)} are left
  out (K7 found that a card built from the question can state the expected answer for some of those items). This is the code's reading of V7; the report says it.
A measure that is missing, not a number or not computed is an error, never a pass.
The last line of the log and of the report is M7 SIX-ARM TEST DONE: knowledge-base row: <build|a_only|none ...>; thorough-profile row: WAITS FOR THE READING.
"""


class SixError(RuntimeError):
    pass


def rd(p):
    return [json.loads(l) for l in Path(p).read_text(encoding="utf-8").splitlines() if l.strip()]


def sha256_file(p):
    return G.sha256_file(p)


def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def mean(xs):
    return sum(xs) / len(xs)


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def p90(xs):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs) - 1, int(0.9 * len(xs)))] if xs else None


def se(xs):
    return statistics.stdev(xs) / math.sqrt(len(xs)) if len(xs) > 1 else None


def rule_text():
    return RULE_TEXT


def is_no_draft(r):
    """A draft cut by the token limit while thinking: the row holds the notice, not an answer."""
    return bool(r.get("cut_while_thinking")) or (r.get("text") or "").strip().startswith(NO_DRAFT[:60])


def answer_tokens(r):
    """Tokens of the ANSWER part of a draft: the server's completion tokens less its reasoning tokens when it reports both, else the chunk count of the
    answer part the stream reader kept, else the completion tokens (an arm without thinking). None for a no-draft row."""
    if is_no_draft(r):
        return None
    di = r.get("draft_info") or {}
    comp, rea, ch = (r.get("tokens_detail") or {}).get("draft"), di.get("reasoning_tokens"), di.get("answer_chunks")
    if di.get("answer_cap_hit"):
        return ch if num(ch) else comp
    if num(comp) and num(rea):
        return comp - rea
    if num(ch):
        return ch
    return comp if num(comp) else None


def row_counts(rows):
    """Cut (at the token limit, a draft cut while thinking included), cut while thinking, empty (no text, or no draft) and the ids with no draft."""
    return {"cut": sum(1 for r in rows if r.get("truncated")), "cut_while_thinking": sum(1 for r in rows if r.get("cut_while_thinking")),
            "empty": sum(1 for r in rows if not (r.get("text") or "").strip() or is_no_draft(r)), "no_draft_ids": sorted(r["id"] for r in rows if is_no_draft(r))}


def interval(mean_, se_, z):
    return None if se_ is None else [mean_ - z * se_, mean_ + z * se_]


# ---------------------------------------------------------------- gates
def check_params(variant, cap):
    if variant not in VARIANTS:
        raise SixError(f"--check-variant must be one of {', '.join(VARIANTS)} (got {variant!r})")
    try:
        c = int(cap)
    except (TypeError, ValueError):
        raise SixError(f"--think-cap must be a whole number of tokens (got {cap!r})")
    if str(c) != str(cap).strip() or not 1 <= c <= 8192:
        raise SixError(f"--think-cap must be a whole number between 1 and 8192 (got {cap!r})")
    return variant, c


def gate_perfect(packs, item_ids, root=ROOT):
    """The perfect packs: the file, its sidecar manifest and the inputs the manifest names (by sha256, as the manifest recorded them), one pack per D5 scenario."""
    p = Path(packs)
    mp = Path(str(p) + ".manifest.json")
    if not p.is_file() or not mp.is_file():
        raise SixError(f"the perfect packs {p} or their manifest {mp} are missing")
    man = json.load(open(mp, encoding="utf-8"))
    problems = []
    if man.get("kind") != "m7-perfect-packs" or man.get("variant") != "padded":
        problems.append(f"the manifest is not the padded perfect packs' (kind {man.get('kind')!r}, variant {man.get('variant')!r})")
    if man.get("pack_file_sha256") != sha256_file(p):
        problems.append("the pack file's sha256 is not the manifest's")
    ins, hs = man.get("inputs") or {}, man.get("inputs_sha256") or {}
    if not ins or set(ins) != set(hs):
        problems.append("the manifest names no inputs, or inputs and their hashes differ")
    for name, rel in sorted(ins.items()):
        f = Path(rel) if Path(rel).is_absolute() else Path(root) / rel
        if not f.is_file():
            problems.append(f"input {name} ({rel}) is missing")
        elif sha256_file(f) != hs.get(name):
            problems.append(f"input {name} ({rel}) changed since the packs were made")
    rows = rd(p)
    if sorted(r["id"] for r in rows) != sorted(item_ids):
        problems.append("the packs do not hold exactly the D5 scenarios")
    if man.get("count") != len(rows):
        problems.append("the manifest's count is not the number of rows")
    if any(not isinstance(r.get("pack"), list) or not r["pack"] or r.get("error") for r in rows):
        problems.append("a row has no pack, or an error")
    fi = ins.get("index_frozen")
    if fi:
        f = Path(fi) if Path(fi).is_absolute() else Path(root) / fi
        try:
            if f.is_file() and man.get("index_sha256") != json.load(open(f, encoding="utf-8"))["sha256"]:
                problems.append("the packs were made on another index than the frozen one named in their inputs")
        except (OSError, ValueError, KeyError):
            problems.append("the frozen index record is unreadable")
    if problems:
        raise SixError("the perfect packs do not match their manifest: " + "; ".join(problems))
    return man


def point_counts(rubrics):
    allp = [pt for r in rubrics for pt in r["must"]]
    n_set = sum(1 for pt in allp if S.point_sets(pt))
    return len(allp), n_set, len(allp) - n_set


def cmd_gate_params(a):
    try:
        v, c = check_params(a.check_variant, a.think_cap)
    except SixError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print(f"run parameters: check variant {v}, thinking cap {c}")
    return 0


def cmd_gate_inputs(a):
    try:
        check_params(a.check_variant, a.think_cap)
        items, rubs = rd(a.items), rd(a.rubrics)
        ids = [r["id"] for r in items]
        if sorted(ids) != sorted(r["id"] for r in rubs) or len(set(ids)) != len(ids):
            raise SixError("items and rubrics do not hold the same scenarios")
        n_all, n_set, n_no = point_counts(rubs)
        if not a.dry and (len(ids), n_all, n_set, n_no) != (50, 269, 227, 42):
            raise SixError(f"D5 has {len(ids)} scenarios and {n_all} points ({n_set} with a covering set, {n_no} without); 50, 269, 227 and 42 expected")
        if S.cmd_gate_files(argparse.Namespace(items=a.items, rubrics=a.rubrics, chunk_check=a.chunk_check)) != 0:
            raise SixError("the D5 files differ from the hashes in the chunk check")
        fp = {r["id"]: r for r in rd(Path(a.funnel_work) / "packs.jsonl")}
        miss = [i for i in ids if not isinstance(fp.get(i, {}).get("frozen"), list) or not fp[i]["frozen"]]
        if miss:
            raise SixError(f"the funnel's packs lack a frozen pack for {len(miss)} scenarios (first: {miss[0]})")
        man = gate_perfect(a.perfect, ids)
        if not a.dry:   # the perfect packs were made from this funnel file
            want = (man.get("inputs_sha256") or {}).get("funnel_packs")
            if want != sha256_file(Path(a.funnel_work) / "packs.jsonl"):
                raise SixError("the perfect packs were made from another funnel pack file than the one in the funnel's work folder")
    except (SixError, OSError, ValueError, KeyError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print(f"gate: D5 {len(ids)} scenarios, {n_all} points ({n_set} with a covering set, {n_no} without); funnel packs complete; perfect packs equal their manifest")
    return 0


def cmd_frozen_packs(a):
    items = [r["id"] for r in rd(a.items)]
    packs = {r["id"]: r for r in rd(Path(a.funnel_work) / "packs.jsonl")}
    R6.write_pack_file(a.out, [(i, list(packs[i]["frozen"])) for i in items], {"source": "the D5 funnel's packs.jsonl", "source_sha256": sha256_file(Path(a.funnel_work) / "packs.jsonl"), "setting": "frozen"})
    print(f"frozen packs: wrote {a.out} ({len(items)} scenarios)")
    return 0


# ---------------------------------------------------------------- a dry run's stand-in inputs
def cmd_dry_inputs(a):
    d = Path(a.dir)
    d.mkdir(parents=True, exist_ok=True)
    import m6_d5_funnel as F6
    F6.cmd_synth(argparse.Namespace(dir=str(d), items=a.items, flaw="none"))
    py = [sys.executable, str(HERE / "m6_d5_funnel.py")]
    subprocess.run(py + ["expand", "--items", str(d / "items.jsonl"), "--out", str(d / "expansions.jsonl"), "--stub"], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(py + ["packs", "--items", str(d / "items.jsonl"), "--expansions", str(d / "expansions.jsonl"), "--out", str(d / "packs.jsonl"), "--stand-in"], check=True, stdout=subprocess.DEVNULL)
    (d / "DF_MANIFEST.json").write_text(json.dumps({"kind": "dry", "options": {"model": "Qwen3.8-27B-4bit"}}) + "\n", encoding="utf-8")
    items = {r["id"]: r for r in rd(d / "items.jsonl")}
    fz = {r["id"]: r["frozen"] for r in rd(d / "packs.jsonl")}
    out = []
    for r in rd(d / "rubrics.jsonl"):
        head = []
        for pt in r["must"]:
            sets = S.point_sets(pt)
            for c in (sets[0] if sets else []):
                if c not in head:
                    head.append(c)
        pack = head + [c for c in fz[r["id"]] if c not in head]
        out.append((r["id"], pack[:max(6, len(head))]))
    pf = d / "packs_perfect.jsonl"
    pf.write_text("".join(json.dumps({"id": i, "pack": p, "error": None}) + "\n" for i, p in out), encoding="utf-8")
    index_frozen = str(ROOT / "runs/v3/INDEX_FROZEN.json")
    ins = {"items": str(d / "items.jsonl"), "rubrics": str(d / "rubrics.jsonl"), "chunk_check": str(d / "CHUNK_CHECK.json"), "funnel_packs": str(d / "packs.jsonl"),
           "index_frozen": index_frozen}
    man = {"kind": "m7-perfect-packs", "variant": "padded", "count": len(out), "pack_file_sha256": sha256_file(pf), "inputs": ins,
           "inputs_sha256": {k: sha256_file(v) for k, v in ins.items()}, "index_sha256": json.load(open(index_frozen))["sha256"], "note": "stand-in of a dry run"}
    Path(str(pf) + ".manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"dry inputs in {d}: {len(items)} synthetic scenarios, stand-in packs, perfect packs with a manifest")
    return 0


# ---------------------------------------------------------------- manifest and start check (scripts/m6_d5_rule.py's, with this rule's text and files)
def _with_this_rule(fn, a):
    saved = (R6.rule_text, R6.CODE_FILES, R6.ANSWER_STAGES)
    R6.rule_text, R6.CODE_FILES, R6.ANSWER_STAGES = (lambda informative=False: RULE_TEXT), CODE_FILES, ("ans_a", "ans_c", "ans_e", "ans_d", "ans_f")
    try:
        a.accept_code_change = None
        return fn(a)
    except (R6.DaError, G.RuleError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    finally:
        R6.rule_text, R6.CODE_FILES, R6.ANSWER_STAGES = saved


def cmd_manifest(a):
    return _with_this_rule(R6.cmd_manifest, a)


def cmd_start_check(a):
    return _with_this_rule(R6.cmd_start_check, a)


# ---------------------------------------------------------------- derived answer sets
def _write_derived(out, rows, src_manifest_path, info):
    G.wr(out, rows)
    man = json.load(open(src_manifest_path, encoding="utf-8")) if Path(src_manifest_path).is_file() else {}
    man = {**man, "derived": info, "ended": man.get("ended") or "derived"}
    Path(str(out) + ".manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def cmd_derive_draft(a):
    """B (or F') from the rows of D (or F): the draft as written before the check. A draft cut while thinking is no draft: the row keeps the notice, as D's does."""
    rows = []
    for r in rd(a.src):
        if r.get("error"):
            raise SixError(f"{a.src}: row {r['id']} has an error")
        sec = dict(r.get("seconds") or {})
        chk = sec.get("check") or 0.0
        draft = r.get("first_pass_text") or ""
        n = dict(r)
        n["text"] = draft if draft.strip() else r["text"]
        sec["check"] = 0.0
        sec["answer"] = (sec.get("answer") or 0.0) - chk
        sec["total"] = (sec.get("total") or 0.0) - chk
        n["seconds"] = sec
        n["thorough"], n["thorough_note"] = None, "the draft of the thorough run before the checking pass (derived, not a separate run)"
        n["derived_from"] = {"arm": a.arm, "file": Path(a.src).name, "sha256": sha256_file(a.src)}
        rows.append(n)
    _write_derived(a.out, rows, str(a.src) + ".manifest.json", {"arm": a.arm, "from": Path(a.src).name, "from_sha256": sha256_file(a.src)})
    print(f"derive-draft: {len(rows)} rows of arm {a.arm} from {a.src}")
    return 0


def cmd_derive_check(a):
    """C from A's rows and the records of verify_pass run: A's row with the pass's final text; the check's seconds are added to A's."""
    from kafka_sre_rag import verify_pass
    chk = {r["id"]: r for r in rd(a.checks)}
    rows = []
    for r in rd(a.answers):
        c = chk.get(r["id"])
        if c is None or c.get("error"):
            raise SixError(f"no complete check record for {r['id']}")
        cs = (c.get("seconds") or {}).get("total") or 0.0
        rec = {k: v for k, v in c.items() if k not in ("id", "config", "error", "stored_seconds", "deadline", "seconds")}
        sec = dict(r.get("seconds") or {})
        sec["check"] = cs
        sec["answer"] = (sec.get("answer") or 0.0) + cs
        sec["total"] = (sec.get("total") or 0.0) + cs
        n = dict(r)
        n["first_pass_text"] = r["text"]
        n["text"] = c["final_text"]
        n["seconds"] = sec
        n["thorough"], n["thorough_note"] = verify_pass.compact(rec), verify_pass.summary_line(rec)
        n["tokens_detail"] = {**(r.get("tokens_detail") or {}), "check": (rec.get("r2") or {}).get("tokens")}
        n["derived_from"] = {"arm": "A", "file": Path(a.answers).name, "sha256": sha256_file(a.answers), "checks": Path(a.checks).name, "checks_sha256": sha256_file(a.checks)}
        rows.append(n)
    _write_derived(a.out, rows, str(a.answers) + ".manifest.json", {"arm": "C", "from": Path(a.answers).name, "from_sha256": sha256_file(a.answers), "checks_sha256": sha256_file(a.checks)})
    print(f"derive-check: {len(rows)} rows of arm C")
    return 0


def cmd_check_verify(a):
    ans = [r["id"] for r in rd(a.answers)]
    rows = rd(a.file) if Path(a.file).is_file() else []
    err = [r["id"] for r in rows if r.get("error")]
    missing = sorted(set(ans) - {r["id"] for r in rows})
    print(f"check {a.file}: {len(rows)} of {a.expect} records, {len(err)} errors, {len(missing)} answers without a record")
    return 0 if len(rows) == a.expect and not err and not missing else 1


def cmd_check_rows(a):
    """A whole profile run: every row has the draft streamed, and (real runs) the draft's request-body hash and the token detail."""
    rows = rd(a.file)
    bad = []
    for r in rows:
        if r.get("draft_streamed") is not True or not isinstance(r.get("tokens_detail"), dict) or not r.get("prompt_sha256"):
            bad.append(r["id"])
        elif a.real and not (r.get("draft_info") or {}).get("request_body_sha256"):
            bad.append(r["id"])
    print(f"check rows {a.file}: {len(rows)} of {a.expect}, {len(bad)} without a streamed draft, prompt hash, token detail{' or request-body hash' if a.real else ''}")
    return 0 if len(rows) == a.expect and not bad else 1


def cmd_prompts_relation(a):
    """The arms differ only in their switch (same pack: same prompt hash); E and F differ from A only by the pack (a different pack: a different hash)."""
    A = {r["id"]: r for r in rd(a.a)}
    B = {r["id"]: r for r in rd(a.b)}
    if sorted(A) != sorted(B):
        print("prompt relation: the two sets do not hold the same ids")
        return 1
    bad = []
    for i in A:
        ha, hb = A[i].get("prompt_sha256"), B[i].get("prompt_sha256")
        same_pack = A[i].get("pack") == B[i].get("pack")
        if not ha or not hb or (ha == hb) != same_pack:
            bad.append(i)
    n_same = sum(1 for i in A if A[i].get("pack") == B[i].get("pack"))
    print(f"prompt relation {Path(a.a).name} / {Path(a.b).name}: {len(A) - len(bad)} of {len(A)} ids as promised "
          f"({n_same} on the same pack: equal prompt hash; {len(A) - n_same} on another pack: different prompt hash)")
    if a.expect_same_pack and n_same != len(A):
        print("prompt relation: the two sets were to stand on the same packs")
        return 1
    return 0 if not bad else 1


def cmd_prompts_default(a):
    """Every row's prompt hash is the hash of the default prompt on the pack the row carries (the arms keep the measured default prompt)."""
    import eval2
    from kafka_sre_rag import evalhooks, prompts
    items = {it["id"]: it for it in rd(a.items)}
    bad = []
    rows = rd(a.answers)
    for r in rows:
        want = eval2.messages_fingerprint(prompts.answer_messages(eval2.full_question(items[r["id"]]), evalhooks.chunks_by_ids(r["pack"])))[0]
        if r.get("prompt_sha256") != want:
            bad.append(r["id"])
    print(f"prompt check {a.answers}: {len(rows) - len(bad)} of {len(rows)} rows carry the hash of the default prompt on their own pack")
    return 0 if not bad else 1


# ---------------------------------------------------------------- the measures
def judged_points(results, rubrics, set_name):
    """Present must points per scenario, mean over the valid passes: {id: (present_with_set, with_set, present_without_set, without_set)}."""
    rub = {r["id"]: r for r in rubrics}
    acc = {}
    for r in results:
        if not r.get("valid_reply") or r.get("reference_given") or r["set"] != set_name:
            continue
        pts = {p["id"]: p for p in rub[r["id"]]["must"]}
        cnt = [0, 0, 0, 0]
        for pid, e in r["must"].items():
            v = e["verdict"]
            if v == "absent" and e.get("quote_ok") is False and e.get("judge_verdict") in ("present", "contradicted"):
                v = "unverified"
            k = 0 if S.point_sets(pts[pid]) else 2
            cnt[k + 1] += 1
            cnt[k] += v == "present"
        acc.setdefault(r["id"], []).append(cnt)
    return {i: tuple(sum(c[k] for c in v) / len(v) for k in range(4)) for i, v in acc.items()}


def arm_time(rows, exp_s, retr_s):
    tot, d, t, c = [], [], [], []
    for r in rows:
        sec = r.get("seconds") or {}
        draft, chk = sec.get("draft"), sec.get("check") or 0.0
        if not num(draft):
            raise SixError(f"row {r['id']} has no draft seconds")
        d.append(draft)
        if num(sec.get("thinking")):
            t.append(sec["thinking"])
        c.append(chk)
        tot.append(exp_s[r["id"]] + retr_s + draft + chk)
    return {"median_s": median(tot), "p90_s": p90(tot), "search_terms_median_s": median([exp_s[r["id"]] for r in rows]), "retrieval_s": retr_s,
            "draft_median_s": median(d), "thinking_median_s": median(t), "check_median_s": median(c), "checked": sum(1 for x in c if x > 0)}


def check_stats(rows):
    st, acc, prop = {}, 0, 0
    for r in rows:
        th = r.get("thorough")
        if not th:
            continue
        s = (th.get("r2") or {}).get("status", "?")
        st[s] = st.get(s, 0) + 1
        props = th.get("proposals") or []
        prop += len(props)
        n = sum(1 for p in props if p.get("accepted"))
        acc += n
    return {"status": st, "proposed": prop, "accepted": acc,
            "answers_with_accepted": sum(1 for r in rows if r.get("thorough") and any(p.get("accepted") for p in r["thorough"].get("proposals") or []))}


def set_stats(name, rows, a, rub, results, exp_s, retr_s, stub):
    n = len(rows)
    if sorted(r["id"] for r in rows) != sorted(r["id"] for r in rub):
        raise SixError(f"set {name} does not hold exactly the D5 scenarios")
    G.check_unjudged(Path(a.work) / "judge_d5.jsonl", a.max_unjudged, {name})
    jr = G.judged(str(Path(a.work) / "judge_d5.jsonl"), a.d5_rubrics, {name})
    P = {}
    for r in rub:
        j = jr.get((name, r["id"]))
        if j is None:
            raise SixError(f"no valid judge result for {name} {r['id']}")
        P[r["id"]] = j[0]
    pts = judged_points(results, rub, name)
    if sorted(pts) != sorted(P):
        raise SixError(f"no valid pass for every scenario of {name} on the point counts")
    nodraft = sorted(r["id"] for r in rows if is_no_draft(r))
    rubs = {r["id"]: r for r in rub}
    for i in nodraft:   # an answer with no draft is an empty answer: P 0 and no point present, by code (the rule text says so)
        P[i] = 0.0
        w = sum(1 for pt in rubs[i]["must"] if S.point_sets(pt))
        pts[i] = (0.0, w, 0.0, len(rubs[i]["must"]) - w)
    tot = [sum(v[k] for v in pts.values()) for k in range(4)]
    nid, kid = G.parse_idcheck(Path(a.work) / f"idcheck_d5_{name}.txt")
    toks = [(r.get("tokens_detail") or {}).get("draft") if not stub else max(1, len(r.get("text") or "") // 4) for r in rows]
    atoks = [answer_tokens(r) for r in rows]
    atoks_n = [x for x in atoks if num(x)]
    dinfo = [r.get("draft_info") or {} for r in rows]
    ttft = [d.get("ttft_s") for d in dinfo if num(d.get("ttft_s"))]
    cached = [d.get("cached_prompt_tokens") for d in dinfo if num(d.get("cached_prompt_tokens"))]
    if any(not num(x) for x in toks):
        raise SixError(f"set {name}: a row has no draft token count")
    reas = [(r.get("draft_info") or {}).get("reasoning_tokens") for r in rows]
    out = {"n": n, "P": mean(list(P.values())), "P_by_id": P,
           "points": {"all": {"present": tot[0] + tot[2], "of": tot[1] + tot[3]}, "with_set": {"present": tot[0], "of": tot[1]}, "without_set": {"present": tot[2], "of": tot[3]}},
           **row_counts(rows),
           "answer_tokens_median": median(atoks_n), "answer_over_limit": sum(1 for x in atoks_n if x > ANSWER_LIMIT), "answer_tokens_known": len(atoks_n),
           "answer_cap_hits": sum(1 for d in dinfo if d.get("answer_cap_hit")),
           "ttft_median_s": median(ttft), "ttft_reported": len(ttft), "cached_reported": len(cached), "cached_median": median(cached), "cached_any": sum(1 for x in cached if x > 0),
           "idents": kid, "idents_answers": nid, "idents_per10": 10 * kid / nid if nid else None,
           "median_draft_tokens": median(toks), "median_reasoning_tokens": median([x for x in reas if num(x)]), "reasoning_tokens_reported": sum(1 for x in reas if num(x)),
           "median_chars": median([len(r.get("text") or "") for r in rows]), "time": arm_time(rows, exp_s, retr_s), "check": check_stats(rows)}
    return out


def pair(m, x, y):
    px, py = m["arms"][x]["P_by_id"], m["arms"][y]["P_by_id"]
    d = [px[i] - py[i] for i in px]
    m_, s_ = mean(d), se(d)
    return {"mean": m_, "se": s_, "n": len(d), "ci196": interval(m_, s_, Z90_2)}


def mech_arm(work, arm, ids, items_kinds):
    """All-pass counts of one arm on the mechanical items: all 116, and without the tier A kinds a lookup can answer."""
    out = {"all_pass": {}, "format_failures": 0, "n": 0, "n_without_lookup": 0, "pass_without_lookup": 0, "pass_all": 0}
    for grp, parts in (("ma", {"M1": ids["M1"], "M2": ids["M2"]}), ("m3", {"M3": ids["M3"]})):
        st = G.score_stats(Path(work) / f"score_{grp}_{arm}.jsonl", parts)
        out["all_pass"].update(st["all_pass"])
        out["format_failures"] += st["format_failures"]
        rows = {r["id"]: r for r in rd(Path(work) / f"score_{grp}_{arm}.jsonl")}
        want = [i for p in parts.values() for i in p]
        for i in want:
            out["n"] += 1
            out["pass_all"] += bool(rows[i]["all_pass"])
            if items_kinds.get(i) not in LOOKUP_KINDS:
                out["n_without_lookup"] += 1
                out["pass_without_lookup"] += bool(rows[i]["all_pass"])
    return out


def cmd_measures(a):
    W = Path(a.work)
    variant, cap = check_params(a.check_variant, a.think_cap)
    stems = SET_STEMS[variant]
    rub = rd(a.d5_rubrics)
    items = rd(a.d5_items)
    exp_s = {r["id"]: r["seconds"] for r in rd(Path(a.funnel_work) / "expansions.jsonl")}
    if any(i["id"] not in exp_s or not num(exp_s[i["id"]]) for i in items):
        raise SixError("the funnel's expansions lack the seconds of some D5 scenario")
    tids = R6.timing_ids([i["id"] for i in items])
    trows = {r["id"]: r for r in rd(W / "retrieval_timing.jsonl")}
    if sorted(trows) != sorted(tids) or any(not num(trows[i].get("frozen_s")) for i in tids):
        raise SixError("retrieval_timing.jsonl does not hold a number for the frozen side of exactly the timing questions")
    retr_s = median([trows[i]["frozen_s"] for i in tids])
    ids = json.load(open(a.ids, encoding="utf-8"))
    results = rd(W / "judge_d5.jsonl")
    n_all, n_set, n_no = point_counts(rub)
    m = {"schema": SCHEMA, "variant": variant, "think_cap": cap, "stub": bool(a.stub), "sizes": {"D5": len(rub), "points": n_all, "points_with_set": n_set, "points_without_set": n_no,
                                                                                                "mech": {k: len(ids[k]) for k in ("M1", "M2", "M3")}},
         "retrieval_frozen_median_s": retr_s, "retrieval_timing_n": len(tids), "arms": {}}
    for name, stem in stems.items():
        rows = rd(W / f"ans_d5_{stem}.jsonl")
        m["arms"][name] = set_stats(name, rows, a, rub, results, exp_s, retr_s, a.stub)
        m["arms"][name]["file"] = f"ans_d5_{stem}.jsonl"
    thorough = "D" if variant != "none" else "B"
    m["thorough_arm"] = thorough
    pairs = [("E", "A"), (thorough, "A"), ("B", "A"), ("Fp", "E")]
    if variant != "none":
        pairs += [("C", "A"), ("D", "B"), ("D", "C"), ("B", "C"), ("F", "E"), ("F", "Fp")]
    m["pairs"] = {f"{x}-{y}": pair(m, x, y) for x, y in pairs if x in m["arms"] and y in m["arms"] and x != y}
    kinds = {}
    for f in ("items_ma.jsonl", "items_m3.jsonl"):
        kinds.update({r["id"]: r.get("kind") for r in rd(W / f)})
    m["mech"] = {"A": mech_arm(W, "A", ids, kinds), "D": mech_arm(W, "D", ids, kinds)}   # D is the thinking-only run in the variant none
    (W / "measures.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (W / "measures.md").write_text(measures_md(m), encoding="utf-8")
    print(measures_md(m))
    return 0


def f2(x, k=3):
    return "n/a" if x is None else f"{x:.{k}f}"


def measures_md(m):
    L = []
    if m.get("stub"):
        L += [f"**{STUB_NOTE}**", ""]
    s = m["sizes"]
    L += [f"Check variant {m['variant']}, thinking cap {m['think_cap']}. D5 {s['D5']} scenarios, {s['points']} points ({s['points_with_set']} with a covering set, {s['points_without_set']} without); "
          f"frozen retrieval median {f2(m['retrieval_frozen_median_s'], 2)} s ({m['retrieval_timing_n']} timing questions).", ""]
    L += [f"| set | P ({s['D5']} answers, 2 passes) | points present all | with set | without set | cut | cut while thinking | empty | idents per 10 | median s | p90 s | draft s | thinking s | check s |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for k, v in m["arms"].items():
        p, t = v["points"], v["time"]
        sh = lambda c: f"{c['present']:.1f}/{c['of']} ({100 * c['present'] / c['of']:.1f} %)" if c["of"] else "n/a"  # noqa: E731
        L.append(f"| {k} | {f2(v['P'])} | {sh(p['all'])} | {sh(p['with_set'])} | {sh(p['without_set'])} | {v['cut']} | {v['cut_while_thinking']} | {v['empty']} | {f2(v['idents_per10'], 2)} | "
                 f"{f2(t['median_s'], 1)} | {f2(t['p90_s'], 1)} | {f2(t['draft_median_s'], 1)} | {f2(t['thinking_median_s'], 1)} | {f2(t['check_median_s'], 1)} |")
    L += ["", "Paired differences of P (mean, standard error, n):", ""]
    for k, v in m["pairs"].items():
        L.append(f"- {k}: {v['mean']:+.3f} (SE {f2(v['se'])}, n {v['n']})")
    L += ["", f"B against C (descriptive, two comparisons: intervals of {Z90_2} standard errors, 0.025 each side; nothing is decided by them):", ""]
    for k in ("B-A", "C-A", "B-C"):
        v = m["pairs"].get(k)
        if v is None:
            continue
        ci = v.get("ci196")
        L.append(f"- {k}: {v['mean']:+.3f}, interval {('[%+.3f, %+.3f]' % tuple(ci)) if ci else 'n/a'}")
    L += ["", "Answer length (answer part, tokens; every arm must have 0 answers over the limit) and the time to the first token of the draft:", ""]
    for k, v in m["arms"].items():
        L.append(f"- {k}: median {f2(v.get('answer_tokens_median'), 0)} tokens ({v.get('answer_tokens_known')} answers measured), over {ANSWER_LIMIT}: {v.get('answer_over_limit')}, "
                 f"stopped at the answer cap: {v.get('answer_cap_hits')}; first token median {f2(v.get('ttft_median_s'), 2)} s ({v.get('ttft_reported')} reported); "
                 f"cached prompt tokens: {v.get('cached_reported')} reported, median {f2(v.get('cached_median'), 0)}, {v.get('cached_any')} with any")
    nd = {k: v.get("no_draft_ids") for k, v in m["arms"].items() if v.get("no_draft_ids")}
    L += ["", "Answers with no draft (cut while thinking; counted as cut and empty, P 0): " + (", ".join(f"{k}: {len(x)}" for k, x in nd.items()) if nd else "none")]
    L += ["", "Checking pass (arms with a check): status counts, proposed and accepted changes, answers with an accepted change:", ""]
    for k, v in m["arms"].items():
        if v["check"]["status"]:
            c = v["check"]
            st = c["status"]
            L.append(f"- {k}: {st}; error {st.get('error', 0)}, not_json {st.get('not_json', 0)}, cut {st.get('cut', 0)}, not_checked {st.get('not_checked', 0)}; "
                     f"{c['proposed']} proposed, {c['accepted']} accepted, {c['answers_with_accepted']} of {v['n']} answers")
    L += ["", "Mechanical items (all-pass counts; format failures; with and without the lookup, decision V7):", ""]
    for k, v in m["mech"].items():
        L.append(f"- {k}: {v['pass_all']} of {v['n']} all-pass ({v['all_pass']}); format failures {v['format_failures']}; "
                 f"without the items of the kinds {', '.join(LOOKUP_KINDS)}: {v['pass_without_lookup']} of {v['n_without_lookup']}")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- the rule
def need(m, *path):
    d = m
    for p in path:
        if not isinstance(d, dict) or p not in d:
            raise SixError("input missing: " + ".".join(map(str, path)))
        d = d[p]
    if not num(d):
        raise SixError("input is not a number: " + ".".join(map(str, path)))
    return d


def evaluate(m):
    """-> (lines, result) with result = {"kb": build|a_only|none_lookup_not_tested, "thorough_possible": [...], "conditions": {...}, "check_failures_above_limit": [...]}.
    The thorough row's wrong and harmful conditions wait for the reading: no final choice for that row."""
    variant, n = m.get("variant"), need(m, "sizes", "D5")
    if variant not in VARIANTS:
        raise SixError("input missing: variant")
    L, eps = [], 1e-9
    over = [(k, int(need(m, "arms", k, "answer_over_limit"))) for k in sorted(m.get("arms") or {})]   # the length check: a measure that is missing is an error
    over = [(k, n_) for k, n_ in over if n_]
    if over:
        raise SixError("the answer limit of %d tokens is broken: %s (an arm's answers are longer than the others' may be; the arms are not comparable)" % (
            ANSWER_LIMIT, ", ".join(f"arm {k}: {n_} answers over" for k, n_ in over)))
    d, sd = need(m, "pairs", "E-A", "mean"), need(m, "pairs", "E-A", "se")
    lo = d - Z90 * sd
    L.append("Knowledge-base rows (E, the perfect pack, against A, the frozen pack, same call):")
    L.append(f"  E - A judged coverage {d:+.3f}, standard error {sd:.3f}, lower end of a 90 % interval {lo:+.3f} ({int(n)} scenarios)")
    if d >= KB_BUILD - eps and lo > KB_BUILD_LOWER + eps:
        kb = "build"
        L.append("  => ROW 1: build the knowledge base: (a), then a pilot of (b). A \"build\" does not settle (b): decision V8 leaves the writers and the licences to the Owner")
    elif d >= KB_A_ONLY - eps:
        kb = "a_only"
        L.append("  => ROW 2: (a) only (E - A at least +0.04 and not row 1)")
    else:
        kb = "none_lookup_not_tested"
        L.append("  => ROW 3: no knowledge base for coverage. \"(a) only if it lowers nonexistent identifiers\" is not tested by this test: no arm has reference cards in the draft prompt "
                 f"(check variant {variant})")
        idn = {k: m.get("arms", {}).get(k, {}).get("idents_per10") for k in ("A", "B", "C", "D")}
        if all(num(v) for v in idn.values()):
            L.append(f"     description only, decides nothing: nonexistent identifiers per 10 answers C {idn['C']:.2f} against A {idn['A']:.2f} (C - A {idn['C'] - idn['A']:+.2f}), "
                     f"D {idn['D']:.2f} against B {idn['B']:.2f} (D - B {idn['D'] - idn['B']:+.2f}); these differences are the check and the cards together")
    L.append("")
    t = m.get("thorough_arm")
    cov, se_ = need(m, "pairs", f"{t}-A", "mean"), need(m, "pairs", f"{t}-A", "se")
    cut, tm = need(m, "arms", t, "cut"), need(m, "arms", t, "time", "median_s")
    p9 = need(m, "arms", t, "time", "p90_s")
    cond = {"coverage": cov >= D_COV_MIN - eps, "cut": cut * 100 <= D_CUT_PCT * n, "median": tm <= D_MED_S + eps, "p90": p9 <= D_P90_S + eps}
    L.append(f"Thorough-profile rows (arm {t} against A; {'thinking, then the checking pass' if t == 'D' else 'thinking only, no checking pass in this variant'}):")
    L.append(f"  {'PASS' if cond['coverage'] else 'FAIL'}  coverage difference {cov:+.3f}, standard error {se_:.3f} (needs >= {D_COV_MIN:+.2f})")
    L.append(f"  {'PASS' if cond['cut'] else 'FAIL'}  cut at the token limit: {int(cut)} of {int(n)} = {100 * cut / n:.1f} % (needs <= {D_CUT_PCT} %)")
    L.append(f"  {'PASS' if cond['median'] else 'FAIL'}  median seconds per answer with search {tm:.1f} (needs <= {D_MED_S:.0f})")
    L.append(f"  {'PASS' if cond['p90'] else 'FAIL'}  90th percentile of the seconds with search {p9:.1f} (needs <= {D_P90_S:.0f}; the fifth largest of 50)")
    L.append(f"  NEEDS THE READING  wrong statements per 10 answers, {t} minus A (needs <= {D_WRONG_MAX:+.1f}); the readers count them")
    L.append(f"  NEEDS THE READING  harmful statements, {t} against A (needs <= A + {D_HARM_PLUS}); the readers count them")
    time_ok = cond["median"] and cond["p90"]
    base = cond["coverage"] and cond["cut"]
    no_gain = "neither (if the readings do not show the errors improving)"
    time_fail_row = cond["coverage"] and not time_ok and (cond["cut"] or TIME_FAIL_TAKES_CUT)   # the Owner's decision of 2026-11-23: narrow (TIME_FAIL_TAKES_CUT False)
    where = "thinking, then the checking pass" if t == "D" else "thinking alone"
    if base and time_ok:
        possible = [f"goes_on_to_B4 ({where}; if the readings give wrong statements at most -2.0 per 10 and harmful at most A + 2)" + (
            "; under --check-variant none thinking alone MAY go on to B4 (the Owner's decision of 2026-11-23)" if t == "B" and NONE_THINKING_MAY_GO_TO_B4 else ""), no_gain]
        if t == "B" and not NONE_THINKING_MAY_GO_TO_B4:
            possible[0] = "waits for the Owner (whether thinking alone may go on to B4 is not decided)"
    elif time_fail_row:
        possible = ["time_fails (if the readings give the errors improving: one g2-based thorough arm on worn data, then B4)", no_gain]
    else:
        possible = ["neither (a measured condition other than time fails, or a cut failure that comes with a time failure; the reading cannot change that)"]
    L.append("  => the row waits for the reading; the outcomes still possible: " + "; ".join(possible))
    L.append("")
    L.append("Combined rows (\"Both of the above\", \"Neither\"): wait for the reading of the thorough-profile row.")
    L.append("")
    L.append(f"Checking-pass failures (a status error is redone on resume, {REDO_PASSES} passes; more than {CHECK_ERROR_LIMIT} of {int(n)} still error marks the arm):")
    above = []
    for k in sorted(m.get("arms") or {}):
        st = ((m["arms"][k].get("check") or {}).get("status")) or {}
        if not st:
            continue
        e = st.get("error", 0)
        flag = e > CHECK_ERROR_LIMIT
        if flag:
            above.append(k)
        L.append(f"  arm {k}: error {e}, not_json {st.get('not_json', 0)}, cut {st.get('cut', 0)}, not_checked {st.get('not_checked', 0)}" + (
            f"  => CHECK FAILURES ABOVE LIMIT (more than {CHECK_ERROR_LIMIT} of {int(n)}): not a clean measurement of the checking pass" if flag else ""))
    if not any((m["arms"][k].get("check") or {}).get("status") for k in m.get("arms") or {}):
        L.append("  (no arm with a checking pass in this variant)")
    return L, {"kb": kb, "thorough_possible": possible, "conditions": cond, "check_failures_above_limit": above}


def cmd_rule(a):
    try:
        m = json.load(open(a.measures, encoding="utf-8"))
        L, res = evaluate(m)
    except (OSError, ValueError, SixError) as e:
        print(f"ERROR: {e}; a missing input is not a pass; no verdict", file=sys.stderr)
        return 2
    print("\n".join(L))
    print(f"M7 SIX-ARM TEST DONE: knowledge-base row: {res['kb']}; thorough-profile row: WAITS FOR THE READING"
          + (f"; check failures above limit: {', '.join(res['check_failures_above_limit'])}" if res["check_failures_above_limit"] else ""))
    return 0


# ---------------------------------------------------------------- reading prep, call counts
def cmd_reading_plan(a):
    key = json.load(open(a.key, encoding="utf-8"))
    ids = sorted(key.get("items") or [])
    second = sorted(key.get("second_reader_ids") or [])
    chunk = lambda xs, k: [xs[i:i + k] for i in range(0, len(xs), k)]  # noqa: E731
    first_n = max(1, math.ceil(len(ids) / 4)) if ids else 0
    first = [ids[i::first_n] for i in range(first_n)] if first_n else []
    sec_n = max(1, math.ceil(len(second) / 4)) if second else 0
    sec = [second[i::sec_n] for i in range(sec_n)] if sec_n else []
    plan = {"seed": a.seed, "first_reader_sessions": first, "second_reader_sessions": sec}
    Path(a.out).write_text(json.dumps(plan, indent=1) + "\n", encoding="utf-8")
    print(f"reading plan: {len(ids)} scenarios in {len(first)} first-reader sessions, {len(second)} in {len(sec)} second-reader sessions (nothing is read here)")
    return 0


def expected_calls(nd, variant, nma, nm3):
    """Model calls per stage (answer rows are the unit: a draft; a check is one more call where a draft exists)."""
    mech = nma + nm3
    chk = variant != "none"
    st = {"ans_a": nd + mech, "ans_c": nd if chk else 0, "ans_e": nd, "ans_d": (nd + mech) * (2 if chk else 1), "ans_f": nd * (2 if chk else 1), "judge": len(SET_STEMS[variant]) * nd * 2}
    st["total_answer_calls"] = st["ans_a"] + st["ans_c"] + st["ans_e"] + st["ans_d"] + st["ans_f"]
    return st


def cmd_call_counts(a):
    exp = expected_calls(a.nd, a.check_variant, a.nma, a.nm3)
    print(" ".join(f"{k}={v}" for k, v in exp.items()))
    return 0


def cmd_stage_calls(a):
    """The model calls a stage made, from its files, against the expected counts. A draft is a row; a check is a record whose status is not no_draft or not_checked
    (those made no call). Strict (dry runs, where the stub never cuts): the counts must equal the expected ones; real: the difference is printed and explained."""
    drafts = checks = skipped = 0
    for f in a.files:
        rows = rd(f)
        if a.kind == "checks":
            checks += sum(1 for r in rows if (r.get("r2") or {}).get("status") not in ("not_checked", "no_draft"))
            skipped += sum(1 for r in rows if (r.get("r2") or {}).get("status") in ("not_checked", "no_draft"))
        else:
            drafts += len(rows)
            if a.expect_checks:
                st = [((r.get("thorough") or {}).get("r2") or {}).get("status") for r in rows]
                checks += sum(1 for x in st if x not in (None, "not_checked", "no_draft"))
                skipped += sum(1 for x in st if x in (None, "not_checked", "no_draft"))
    print(f"calls {a.stage}: drafts {drafts} (expected {a.expect_drafts}), checks {checks} (expected {a.expect_checks}), checks not made {skipped}")
    ok = drafts == a.expect_drafts and checks == a.expect_checks
    if not ok and not a.strict and drafts == a.expect_drafts and checks + skipped == a.expect_checks:
        print(f"calls {a.stage}: the difference is rows with no draft or not checked (real run; they made no check call)")
        return 0
    return 0 if ok else 1


# ---------------------------------------------------------------- checks the shell script makes, as commands (so each can be tested with a planted mismatch)
LEAK_TERMS = ("answer_profile", "--answer-profile", "enable_thinking", "think-cap", "check-variant", "no-verify", "thorough profile", "EXPERIMENTAL", "cut off by the token limit while thinking")


def cmd_leak_check(a):
    """No file of a reading bundle names a configuration (a switch, the profile, the notice of a draft cut while thinking)."""
    bad = []
    for f in sorted(Path(a.dir).rglob("*.md")):
        t = f.read_text(encoding="utf-8")
        hit = [x for x in LEAK_TERMS if x in t]
        if hit:
            bad.append((str(f), hit))
    for f, hit in bad:
        print(f"a bundle file names a configuration: {f}: {', '.join(hit)}")
    print(f"leak check {a.dir}: {len(bad)} files name a configuration")
    return 1 if bad else 0


def cmd_no_draft_ids(a):
    """The scenarios where any of the named answer sets has no draft (cut while thinking): printed one per line; with --key they are recorded in the key
    (they are left out of the bundles; the key says which arm had no draft)."""
    sets = dict(x.split("=", 1) for x in a.answers)
    per = {name: sorted(r["id"] for r in rd(f) if is_no_draft(r)) for name, f in sets.items()}
    ids = sorted({i for v in per.values() for i in v})
    if a.key:
        key = json.load(open(a.key, encoding="utf-8"))
        key["no_draft"] = per
        key["left_out_of_the_bundles"] = ids
        Path(a.key).write_text(json.dumps(key, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print("\n".join(ids))
    print(f"no-draft: {len(ids)} scenarios left out of the bundles ({', '.join(f'{k}: {len(v)}' for k, v in per.items())})", file=sys.stderr)
    return 0


def cmd_bodies_relation(a):
    """The request bodies of two arms' drafts (the parts recorded in each row: generators.body_parts). Allowed to differ: the thinking switch and the token limit (the
    thinking arm's limit is the other's plus the cap), and the messages where the packs differ. Everything else (model, temperature, stream flags, other extra-body
    keys) is equal. With --thinking-differs the second set is the thinking arm of the first."""
    A = {r["id"]: r for r in rd(a.a)}
    B = {r["id"]: r for r in rd(a.b)}
    if sorted(A) != sorted(B):
        print("bodies relation: the two sets do not hold the same ids")
        return 1
    bad = []
    for i in A:
        pa, pb = ((A[i].get("draft_info") or {}).get("request_body_parts"), (B[i].get("draft_info") or {}).get("request_body_parts"))
        if not pa or not pb:
            bad.append((i, "no request body parts recorded"))
            continue
        why = []
        if pa["rest_sha256"] != pb["rest_sha256"]:
            why.append("the rest of the body (model, temperature, stream flags, other extra-body keys) differs")
        same_pack = A[i].get("pack") == B[i].get("pack")
        if (pa["messages_sha256"] == pb["messages_sha256"]) != same_pack:
            why.append("the messages are %s on %s packs" % ("equal" if not same_pack else "different", "different" if not same_pack else "the same"))
        if a.thinking_differs:
            if (pa["enable_thinking"], pb["enable_thinking"]) != (False, True):
                why.append("the thinking switch is not off then on")
            if pb["max_tokens"] != pa["max_tokens"] + a.think_cap:
                why.append("the second token limit is not the first plus the cap %d" % a.think_cap)
        else:
            if pa["enable_thinking"] != pb["enable_thinking"] or pa["max_tokens"] != pb["max_tokens"]:
                why.append("the thinking switch or the token limit differs")
        if why:
            bad.append((i, "; ".join(why)))
    for i, w in bad[:5]:
        print(f"bodies relation {i}: {w}")
    print(f"bodies relation {Path(a.a).name} / {Path(a.b).name}: {len(A) - len(bad)} of {len(A)} ids as promised "
          f"(allowed to differ: {'the thinking switch off/on and the token limit plus the cap, ' if a.thinking_differs else ''}the messages where the packs differ)")
    return 1 if bad else 0


def _check_status(r, kind):
    return ((r.get("r2") if kind == "verify" else (r.get("thorough") or {}).get("r2")) or {}).get("status")


def cmd_check_errors(a):
    """Rows whose checking call ended with status error (a stored answer with a failed check): counted; with --drop moved to FILE.redo_dropped.jsonl and removed from FILE,
    so that the resume of the run redoes them. Prints check_errors=N."""
    rows = rd(a.file) if Path(a.file).is_file() else []
    bad = [r for r in rows if _check_status(r, a.kind) == "error"]
    if a.drop and bad:
        keep = [r for r in rows if _check_status(r, a.kind) != "error"]
        with open(str(a.file) + ".redo_dropped.jsonl", "a", encoding="utf-8") as f:
            f.write("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in bad))
        Path(a.file).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in keep), encoding="utf-8")
    print(f"check_errors={len(bad)}")
    return 0


def cmd_pilot_check(a):
    """The pilot's report the run parameters come from: a real run needs the file, not a dry-run record. A dry run needs none (the chain writes a stub)."""
    p = Path(a.file)
    if a.dry:
        print(f"pilot report: dry run, stub {p}")
        return 0
    if not p.is_file() or not p.read_text(encoding="utf-8").strip():
        print(f"REFUSED: the pilot's report {p} is missing or empty; the check variant and the thinking cap come from it (--pilot-report FILE)", file=sys.stderr)
        return 2
    head = "\n".join(p.read_text(encoding="utf-8").splitlines()[:5])
    if "DRY RUN" in head or "from stub data" in head or "stub model" in head:
        print(f"REFUSED: {p} is the record of a dry run, not a measurement", file=sys.stderr)
        return 2
    print(f"pilot report: {p} sha256 {sha256_file(p)}")
    return 0


def cmd_models_check(a):
    """Every model this chain needs is listed by the server (/v1/models): the 4-bit model and the judge."""
    d = json.load(open(a.models, encoding="utf-8"))
    names = {m.get("id") for m in (d.get("data") or []) if isinstance(m, dict)}
    miss = [n for n in a.need if n not in names]
    if miss:
        print(f"REFUSED: the server does not list the model {', '.join(miss)}", file=sys.stderr)
        return 2
    print(f"models: all {len(a.need)} needed models are listed")
    return 0


def cmd_server_gate(a):
    """The server before the chain uses it (the Owner's decision of 2026-11-23: the unload go of M6 was not extended to M7). Refuses (exit 2) when a request is
    active or waiting, or when a model that is not ours is loaded; the message names it and nothing is unloaded. The models this chain needs (the 4-bit model
    and the judge) are ours; a judge model left loaded is printed as UNLOAD <name> (the chain unloads it before the first request)."""
    try:
        st = json.load(open(a.status, encoding="utf-8"))
        busy = int(st.get("active_requests", 1)) + int(st.get("waiting_requests", 0))
        loaded = list(st.get("loaded_models") or [])
    except (OSError, ValueError, TypeError):
        print("REFUSED: the server status is unreadable", file=sys.stderr)
        return 2
    foreign = [m for m in loaded if m not in a.ours]
    if foreign:
        print(f"REFUSED: the model {', '.join(foreign)} is loaded by another client; this chain never unloads a model it did not load (wait for its owner); nothing was started", file=sys.stderr)
        return 2
    if busy:
        print(f"REFUSED: the server has {busy} active or waiting requests (another client); wait until it is idle; nothing was started", file=sys.stderr)
        return 2
    for m in loaded:
        if m != a.keep:
            print(f"UNLOAD {m}")
    return 0


def build_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("rule-text")
    sub.add_parser("code-pins")
    q = sub.add_parser("gate-params")
    q.add_argument("--check-variant", default=None)
    q.add_argument("--think-cap", default=None)
    q = sub.add_parser("gate-inputs")
    for k in ("items", "rubrics", "chunk-check", "funnel-work", "perfect"):
        q.add_argument("--" + k, required=True)
    q.add_argument("--check-variant", default=None)
    q.add_argument("--think-cap", default=None)
    q.add_argument("--dry", action="store_true")
    q = sub.add_parser("frozen-packs")
    for k in ("funnel-work", "items", "out"):
        q.add_argument("--" + k, required=True)
    q = sub.add_parser("dry-inputs")
    q.add_argument("--dir", required=True)
    q.add_argument("--items", type=int, default=8)
    for name in ("manifest", "start-check"):
        R6.add_state_args(sub.add_parser(name))
    q = sub.add_parser("derive-draft")
    for k in ("src", "out", "arm"):
        q.add_argument("--" + k, required=True)
    q = sub.add_parser("derive-check")
    for k in ("answers", "checks", "out"):
        q.add_argument("--" + k, required=True)
    q = sub.add_parser("check-verify")
    q.add_argument("--file", required=True)
    q.add_argument("--answers", required=True)
    q.add_argument("--expect", type=int, required=True)
    q = sub.add_parser("check-rows")
    q.add_argument("--file", required=True)
    q.add_argument("--expect", type=int, required=True)
    q.add_argument("--real", action="store_true")
    q = sub.add_parser("prompts-relation")
    q.add_argument("--a", required=True)
    q.add_argument("--b", required=True)
    q.add_argument("--expect-same-pack", action="store_true")
    q = sub.add_parser("leak-check")
    q.add_argument("--dir", required=True)
    q = sub.add_parser("no-draft-ids")
    q.add_argument("--answers", nargs="+", required=True, metavar="NAME=FILE")
    q.add_argument("--key", default=None)
    q = sub.add_parser("bodies-relation")
    q.add_argument("--a", required=True)
    q.add_argument("--b", required=True)
    q.add_argument("--thinking-differs", action="store_true")
    q.add_argument("--think-cap", type=int, default=0)
    q = sub.add_parser("check-errors")
    q.add_argument("--file", required=True)
    q.add_argument("--kind", choices=("verify", "answers"), required=True)
    q.add_argument("--drop", action="store_true")
    q = sub.add_parser("pilot-check")
    q.add_argument("--file", required=True)
    q.add_argument("--dry", action="store_true")
    q = sub.add_parser("models-check")
    q.add_argument("--models", required=True)
    q.add_argument("--need", nargs="+", required=True)
    q = sub.add_parser("server-gate")
    q.add_argument("--status", required=True)
    q.add_argument("--keep", required=True, help="the 4-bit model")
    q.add_argument("--ours", nargs="+", required=True)
    q = sub.add_parser("prompts-default")
    q.add_argument("--items", required=True)
    q.add_argument("--answers", required=True)
    q = sub.add_parser("measures")
    for k in ("work", "ids", "d5-rubrics", "d5-items", "funnel-work"):
        q.add_argument("--" + k, required=True)
    q.add_argument("--check-variant", required=True)
    q.add_argument("--think-cap", required=True)
    q.add_argument("--max-unjudged", type=int, default=0)
    q.add_argument("--stub", action="store_true")
    q = sub.add_parser("rule")
    q.add_argument("--measures", required=True)
    q = sub.add_parser("reading-plan")
    q.add_argument("--key", required=True)
    q.add_argument("--out", required=True)
    q.add_argument("--seed", type=int, default=BUNDLE_SEED)
    q = sub.add_parser("stage-calls")
    q.add_argument("--stage", required=True)
    q.add_argument("--files", nargs="+", required=True)
    q.add_argument("--kind", choices=("answers", "checks"), default="answers")
    q.add_argument("--expect-drafts", type=int, default=0)
    q.add_argument("--expect-checks", type=int, default=0)
    q.add_argument("--strict", action="store_true")
    q = sub.add_parser("call-counts")
    q.add_argument("--nd", type=int, required=True)
    q.add_argument("--nma", type=int, required=True)
    q.add_argument("--nm3", type=int, required=True)
    q.add_argument("--check-variant", required=True)
    return p


def main(argv=None):
    a = build_parser().parse_args(argv)
    try:
        if a.cmd == "rule-text":
            print(rule_text())
            return 0
        if a.cmd == "code-pins":
            print("\n".join(CODE_PINS))
            return 0
        fn = {"gate-params": cmd_gate_params, "gate-inputs": cmd_gate_inputs, "frozen-packs": cmd_frozen_packs, "dry-inputs": cmd_dry_inputs, "manifest": cmd_manifest,
              "start-check": cmd_start_check, "derive-draft": cmd_derive_draft, "derive-check": cmd_derive_check, "check-verify": cmd_check_verify,
              "check-rows": cmd_check_rows, "prompts-relation": cmd_prompts_relation, "prompts-default": cmd_prompts_default, "measures": cmd_measures,
              "rule": cmd_rule, "reading-plan": cmd_reading_plan, "call-counts": cmd_call_counts,
              "stage-calls": cmd_stage_calls, "leak-check": cmd_leak_check, "no-draft-ids": cmd_no_draft_ids, "bodies-relation": cmd_bodies_relation,
              "check-errors": cmd_check_errors, "pilot-check": cmd_pilot_check, "models-check": cmd_models_check, "server-gate": cmd_server_gate}[a.cmd]
        return fn(a)
    except (SixError, G.RuleError, R6.DaError, OSError, KeyError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
