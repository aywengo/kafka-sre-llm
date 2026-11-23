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
  check-rows --file F --expect N --real [--no-thinking]       every row of a profile run is whole: streamed draft, recorded request-body hash (real runs)
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
from kafka_sre_rag.verify_pass import NO_DRAFT, DEADLINE_S  # noqa: E402  (the text of a draft cut while thinking and the check's start-by deadline: imported, never copied)

SCHEMA = 1
RPACK_KEY = "r4g2"               # the funnel's pack setting arm R (grounded-g3) replays (W7; as scripts/m6_d5_answers.sh read it)
VARIANTS = ("V1", "V1C", "none")
BUNDLE_SEED = 20261123           # drawn before any answer exists (it is in the rule text); the names of the answers of a scenario come from it
Z90 = 1.645                      # a 90 % interval, one comparison
Z90_2 = 1.960                    # a 90 % interval corrected for two comparisons (0.025 each side)
KB_BUILD, KB_BUILD_LOWER, KB_A_ONLY = 0.10, 0.04, 0.04
D_WRONG_MAX, D_COV_MIN, D_CUT_PCT = -2.0, -0.04, 10
D_HARM_NEW_MAX = 0               # W6: confirmed new harmful consequences of the thorough arm against A (replaces the plan's "harmful at most A + 2")
PLAN_MED_S, PLAN_P90_S = 180.0, 270.0   # W5: the plan's time bars; the time is REPORTED against them, never decided
CMP_COV_MIN, CMP_COV_LOWER, CMP_MECH_LOWER = -0.04, -0.10, -3.0   # W10: the "clean winner" line (described, decides nothing)
LOOKUP_KINDS = ("name_setting", "error_name", "is_real", "version_trap", "connect_rest")   # tier A kinds where a card built from the question alone can state the expected answer (K7)
ANSWER_LIMIT = 2048              # the answer limit of every arm (tokens); a thinking arm's answer PART is stopped here too (generators answer_cap)
CHECK_ERROR_LIMIT = 2            # of 50 answers: more checking calls with status error than this after the redo passes mark the arm "check failures above limit"
REDO_PASSES = 2                  # a check status error is redone on resume (C, D, F) this many times; the shell script uses the same number
STUB_NOTE = "DRY RUN: stub model, stub judge, stand-in reranker; no number below is a measurement"

# the sets that are judged, by variant: set name -> file stem of its D5 answers
SET_STEMS = {"V1": {"A": "A", "B": "B", "C": "C", "D": "D", "E": "E", "F": "F", "Fp": "Fp"},
             "V1C": {"A": "A", "B": "B", "C": "C", "D": "D", "E": "E", "F": "F", "Fp": "Fp"},
             "none": {"A": "A", "B": "D", "E": "E", "Fp": "F"}}
READ_SETS = {"V1": ("A", "D", "E", "F"), "V1C": ("A", "D", "E", "F"), "none": ("A", "B", "E", "Fp")}   # Qwen's sets of the blind bundles
READ_GEMMA_SETS = {"V1": ("A", "E"), "V1C": ("A", "E"), "none": ("A", "E")}   # a Gemma model's sets of the bundles; its thorough arm (D, or B under none) is added when it ran
# The base models of the test (W8, W9). Every model gets A and E; the thorough arms run for a model only when --thinking-models names it.
MODEL_KEYS = ("qwen", "gemma26", "gemma31")
ANSWER_ORDER = ("gemma31", "gemma26", "qwen")        # answer stages are grouped by model: each model alone on the server, unloaded after its stages
GEMMA31_PLACEHOLDER = "PLACEHOLDER-gemma31-4bit-id-from-task-G0"   # the 4-bit build of the 31B is made later (task G0): the real run must pass its id
DEFAULT_MODEL_IDS = {"qwen": "Qwen3.8-27B-4bit", "gemma26": "gemma-4-26B-A4B-it-QAT-MLX-4bit", "gemma31": GEMMA31_PLACEHOLDER}
THINK_SECONDS = {"gemma26": (52.0, 141.0), "gemma31": (127.0, 177.0)}   # the measured seconds per thinking answer (median, 90th percentile), runs/m7/reports/gemma_think.md, 2026-11-23: used by the estimate
DEFAULT_THINKING = ()            # K4b (Owner's question Q1): no model thinks unless --thinking-models names it; the real run passes it explicitly (it is in the manifest and the header)
READ_ANSWERS_PER_SESSION = 16    # the reading sessions hold about this many answers when --scenarios-per-session is not given (4 scenarios of 4 answers before the models were added)
# W6: the consequence of a harmful finding, a closed list plus "other: ..." (the reader's instructions in the bundles carry the same list: scripts/eval2_blind.py)
from eval2_blind import HARM_CONSEQUENCES as CONSEQUENCES  # noqa: E402  (one list: the reader's instructions and the rule use the same tuple)
# W6 (K4b, the reviewer's strict reading): the fixed groups of the consequences. A new harm is confirmed by the second reader when he marks the same answer harmful with the same consequence, a consequence of
# the same group, or any consequence the reference answer lacks. One table; "other: ..." has its own group "other".
HARM_GROUPS = {
    "loss of data or messages": ("data loss", "topic deleted", "messages lost or duplicated", "consumer group offsets lost"),
    "outage": ("outage of the cluster", "outage of a broker or node", "outage of an application or service"),
    "security": ("credentials exposed", "security controls weakened"),
    "change": ("bad change rolled out to production", "configuration lost or overwritten"),
    "resources": ("resource exhaustion",),
}
GROUP_OF = {c: g for g, cs in HARM_GROUPS.items() for c in cs}
OTHER_GROUP = "other"
HARM_GROUP_TABLE = "\n".join(f"| {g} | {', '.join(cs)} |" for g, cs in HARM_GROUPS.items())
CODE_PINS = tuple(p for p in R6.CODE_PINS if p != "scripts/m6_d5_answers.sh") + (
    "kafka_sre_rag/verify_pass.py", "kafka_sre_rag/kb_lookup.py", "scripts/eval2_blind.py", "scripts/m7_perfect_packs.py",
    "scripts/m7_four_way.sh", "scripts/m7_six_arm_rule.py")
CODE_FILES = ("m7_four_way.sh", "m7_six_arm_rule.py")
# The two questions the Owner decided on 2026-11-23 (each a single place; the text of the decision is in RULE_TEXT):
TIME_FAIL_TAKES_CUT = False      # "Errors improve but time fails" stays NARROW: a cut failure that comes with a time failure is "neither"
ELIGIBLE_B4 = "ELIGIBLE FOR B4 (its B); whether B4 runs, and for which model, is the Owner's decision (W10; the base model stays Qwen3.8-27B unless the Owner decides otherwise)"
ELIGIBLE_B4_WORD = ELIGIBLE_B4
NONE_THINKING_MAY_GO_TO_B4 = True   # under --check-variant none the thorough row is applied to B (thinking only) and a pass MAY go on to B4

RULE_TEXT = f"""\
## The rule of the six-arm test on D5 (fixed before the first answer; agents/m7-plan.md section 3; MILESTONE.md decisions V1 to V9)

D5 (eval/m6/d5, 50 scenarios, 269 must points) is worn development data after M6. The test is a screen: it decides only whether work continues, and it allows no
out-of-sample claim and no tuning on D5 items. Every arm uses the default prompt (arm R excepted, see the table), the frozen search terms and replayed packs (no search call in an answer),
one request at a time, and the same transport (the draft is streamed in every arm, thinking switched on or off per call). The decided rows are Qwen's; each Gemma model's thorough row (its B against its own A) is decided only as "eligible for B4 or not" (the Owner decides about B4: W10); everything else about the Gemma models and arm R is described, not decided (below).

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
| R | the D5 funnel's r4g2 packs (W7) | grounded-g3 (`--mode grounded-g3`) on the r4g2 packs, otherwise as A. Judged only, outside every decided row, not read |

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

**Amended by the Owner's decisions of 2026-11-23 (W5 time, W6 harm; the plan's table above is kept word for word, these two cells are read as follows):**
- W11 (the Owner, 2026-11-23): Qwen's thinking arms B, D, F and F' are dropped from the planned test (the pilot showed every thinking draft cut); the code paths of the thinking arms and of the checking pass stay (tested) but are not part of the planned run. The plan gave the thorough row to D (thinking, then the checking pass); the checking-pass-alone candidate, arm C, would have been judged on the same row (C - A coverage at least {D_COV_MIN:+.2f}, cut at most {D_CUT_PCT} %, wrong statements at most {D_WRONG_MAX:+.1f} per 10, 0 confirmed new harmful consequences; time reported, W5).
- W11 and the pilot (2026-11-23): neither checking variant cleared its bars in the pilot, so arm C is dropped too: the planned run has `--check-variant none`, and Qwen's thorough-profile row is "not tested (W11 and the pilot)".
- The Gemma thinking measurement (2026-11-23, `runs/m7/reports/gemma_think.md`): with thinking on, on the pilot's 20 stored-pack scenarios at a cap of 8,000, Gemma 4 26B-A4B gave 18 of 20 usable drafts (2 cut while thinking; thinking tokens median 2,096, maximum 10,046; 52 s median)
  and Gemma 4 31B 20 of 20 (thinking median 1,082, maximum 3,672; 127 s median, maximum 278 s); the answers read normal. So the conductor decided (2026-11-23): **the planned run is `--thinking-models gemma26,gemma31 --check-variant none`: Qwen A, E, R; each Gemma model A, E and its
  thinking-only arm B** (no checking pass, no perfect-pack thinking arm, no Gemma arm C, D or F). The code paths of C, D, F and Qwen's thinking arms stay tested but are out of the planned run.
- The thorough-profile row applies PER MODEL that has a thinking arm: for each Gemma model, its B against its own A (thinking only, since the checking pass is dropped by the pilot), on the plan's row as amended above: coverage at least {D_COV_MIN:+.2f}, cut at most {D_CUT_PCT} %, wrong statements at most {D_WRONG_MAX:+.1f} per 10,
  {D_HARM_NEW_MAX} confirmed new harmful consequences (W6), time reported (W5). Any answer over the {ANSWER_LIMIT}-token limit in that model's B or in its A makes the row NOT AVAILABLE. The row's outcome is only "{ELIGIBLE_B4}", "neither", "waits for the reading" or "not available". **Nothing in this rule sends a Gemma model to B4:** an eligible B waits for the Owner's decision (W10; the base model stays Qwen3.8-27B unless the Owner decides otherwise, MILESTONE.md). The report prints one outcome line per model, with the wrong statements per 1,000 words, and the last line lists each model's outcome in the same words. Qwen's row is "not tested (W11 and the pilot)". A model's B against its own A is measured here (coverage, cut, time); its wrong statements and harm come from the blind reading (`thorough-rows` applies them). A model whose answer stage failed after its retries is "not available: answer stage failed" in all its rows and comparisons, and the run goes on with the other models and the judge; a failure of Qwen's own answer stage stops the run.
- `--thinking-mech yes|no` (default yes): a thinking Gemma model's B also answers the 116 mechanical items, as the plan's D did; `no` leaves them out (only when the estimate puts the run over 24 server hours).
- W5, time is reported, not decided. The median and the 90th percentile of the thorough-profile row (the plan's bars were {PLAN_MED_S:.0f} s and {PLAN_P90_S:.0f} s) are printed as "reported" with their values and never as PASS or FAIL. The operational deadline stays
  (the profile's {DEADLINE_S:.0f} s start-by of the checking call) and the number of answers it cut short (check status not_checked) is reported. The row "Errors improve but time fails" can therefore no longer arise: no time value decides a row. The
  thorough-profile row is decided by coverage, cut, wrong statements and harm only.
- W6, harm is matched by consequence. A harmful finding in a candidate's answer is NEW when the reference answer to the same scenario has no harmful finding with the same consequence (the reader records a `consequence` for every harmful finding, from a closed list of {len(CONSEQUENCES)}
  consequences or "other: ..."; any other label is refused with a message). The thorough row's harm condition is: confirmed new harmful consequences of the thorough arm against A = 0; each new one goes to the second reader. This replaces
  "harmful at most A + 2" and is the conductor's reading of W6 together with the Owner's handoff ("no confirmed newly introduced severe harm"). **Strict reading (K4b, the reviewer's recommendation):** a new harm is CONFIRMED when the second reader marks the same answer harmful
  with the same consequence, with a consequence of the same group of the table below, or with ANY consequence the reference answer lacks; so a difference of labels between the readers never lets a harm pass. **Minor harmful findings are counted in the match** (the first reader's and the second reader's;
  the wrong-statement count still leaves minor findings out). "other: ..." labels are matched by their group "other". A harm that only the second reader finds is printed as "new, unconfirmed (second reader only)"; it does not enter the bar. The raw counts (harmful
  findings of each arm, new, confirmed, not confirmed, not yet read by the second reader, second reader only) are printed too.

| Group | Consequences |
|---|---|
{HARM_GROUP_TABLE}
| other | "other: ..." |

**What is measured:**
- judged coverage (the coverage role of the development check's judge, DeepSeek, setting deepseek-nothink-12k, instruction v2, two passes), over all 269 points and over the 227
  with a covering set and the 42 without separately; the paired difference per scenario with its standard error;
- cut answers (at the token limit, cut while thinking counted apart) and empty answers (no text, or no draft after a cut while thinking);
- time per part (search terms, whole retrieval, draft with its thinking, check), the median and the 90th percentile of the time with search;
- nonexistent identifiers per 10 answers (scripts/eval2_idcheck.py), per arm;
- the 116 mechanical items for A and D, and for a thinking Gemma model's B under `none` unless `--thinking-mech no` (scripts/eval2_score.py), with and without the lookup (decision V7);
- a blind reading of Qwen's A, D, E and F (A and E when Qwen does not think), each Gemma model's A and E, and its D (B under `none`) when it ran (all answers of a scenario in one bundle, names drawn per scenario from the seed {BUNDLE_SEED}; wrong, invented and
  harmful statements by the readers, a consequence for every harmful one; a quarter read by a second reader; the model's name is in no bundle). B, C, F' and R are judged only. The reading is not part of this chain.

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
  truncated at the token limit (cut while thinking included), at most {D_CUT_PCT} % (integer arithmetic). Median and 90th percentile (REPORTED, not decided: W5) are of the seconds per D5 answer with search:
  the scenario's search-terms seconds from the D5 funnel's stored expansions, plus the median seconds of the whole frozen retrieval measured in this chain, plus the answer's own
  draft (thinking included) and check seconds; the 90th percentile is the value at position floor(0.9 n) of the n sorted times (the fifth largest of 50). The deadline of the profile
  ({DEADLINE_S:.0f} s start-by of the check) counts from the call here, not from the question (the replay has no search), so it is slightly looser than in use. The seconds also record the time to the first token and the cached prompt
  tokens of each draft (when the server reports them), so that an effect of the server's prompt cache can be seen; D and F run before A and E for the same reason.
- The Owner's decisions of 2026-11-23 on the two readings: (1) (W3) "Errors improve but time fails" stays NARROW: it is the case where coverage and cut pass and only the median or the 90th percentile fails;
  a cut failure that comes with a time failure is "neither" (constant TIME_FAIL_TAKES_CUT in the rule script, kept as recorded). Since W5 no time value decides a row, so this case no longer arises and the constant has no effect on the outcome. (2) (W4) Under `--check-variant none` the thorough row is applied to B (thinking only); for a Gemma model a pass makes B eligible for B4, and the Owner decides whether B4 runs (W10; constant NONE_THINKING_MAY_GO_TO_B4 is Qwen's reading of W4 and is not used in the planned run).
- An answer with no draft (cut while thinking) is an empty answer: it is counted as cut and as empty, its judged coverage P is set to 0 and its points count as absent (by code, whatever the judge said of
  the notice text), and it is left out of the blind bundles. K4b: the exclusion is per answer, not per scenario: a bundle may hold fewer answers under random letters, the key lists the answers left out, and a scenario is read in a set only when that set's answer has a draft. A comparison of two sets
  uses the scenarios read in both; the report states, for each read set, how many of its scenarios were read. An arm whose answers all have no draft is a printed outcome ("no usable draft"), not a failure. The no-draft counts are reported apart and are never counted as an improvement of the wrong statements.
- Checking-pass failures: the statuses error, not_json, cut and not_checked are counted per arm. A checking call with status error is redone on resume ({REDO_PASSES} redo passes, for C, D and F); an arm where
  more than {CHECK_ERROR_LIMIT} of the 50 answers still have status error after that is marked "check failures above limit" in the rule output and is not read as a clean measurement.
- Wrong statements are counted by the readers (the first reader's non-minor findings of the kinds wrong, invented and harmful, as in the sealed reading of M4) and harmful statements by consequence (W6, above). The code prints those conditions as "needs the reading" and, from the conditions that are measured, which
  outcomes are still possible. The combined rows ("Both of the above", "Neither") are read as the combination of the knowledge-base rows (E − A) and the thorough-profile rows (D − A).
- B against C: B − A and C − A with 90 % intervals corrected for two comparisons ({Z90_2} standard errors: 0.025 each side), and B − C with the same interval; descriptive, decides nothing.
- With and without the lookup (V7): the mechanical all-pass counts of A and D over all 116 items, and over the items that remain when the tier A kinds {", ".join(LOOKUP_KINDS)} are left
  out (K7 found that a card built from the question can state the expected answer for some of those items). This is the code's reading of V7; the report says it.

**Point-level and essential-unit measures (described, not decided).** They decide nothing, they are not a condition of any row above, and they cannot change a row. They are printed
in the measures before any answer is read, and the rule code never reads them. The judge's per-point verdicts are those of the two passes; a point is COVERED in an arm when it is present in
every valid pass (both of them), MISSED otherwise (a point present in only one pass is counted as missed and also listed as "disputed", so that it is not hidden); a scenario with only one valid pass uses that one.
An answer with no draft is delivered as empty: all its points are missed (and not disputed); no-draft, cut and timed-out (not checked) answers stay in every denominator of every measure below.
- Point classes, from the covering sets and the packs the answers saw (strict count: a whole set is in the pack): S-both = a whole set in the frozen pack and in the perfect pack (the 69 of D5);
  S-perfect-only = in the perfect pack only (the 158); no-set = no covering set, or not indexed (the 42: the "no covering set" and the "not indexed" points listed separately); S-other = any other
  point with a set (a set in neither pack, or in the frozen pack only; expected 0 and printed). For E against A, and for F' against B (the same comparison under thinking), per class: covered in both arms,
  missed in both, gained (missed in the reference arm, covered in the candidate), lost (covered in the reference, missed in the candidate), the points of weight 2 apart, and the regressions on S-both
  listed by scenario and point id. The denominators are the numbers of points of the class (weight 2: the points of weight 2 of the class).
- E - A (and F' - B) coverage within S-perfect-only and within S-both: the mean score of the points (a point's score is the share of valid passes in which it is present), as a ratio over
  the points of the class, with the scenarios as clusters: sum of the differences over the sum of the points; the standard error is the cluster (ratio) estimator
  sqrt(n/(n-1) * sum_i (d_i - r*m_i)^2) / sum_i m_i over the scenarios i that have points of the class; the interval is the ratio plus and minus {Z90} standard errors (a 90 % interval). No random draw is used.
- Essential units by where the evidence is: the points of weight 2, each in one of four classes by the packs of the two arms compared: a whole covering set in both packs, in the candidate's pack only,
  in the reference's pack only, in neither (a point with no covering set is in "neither", and counted apart there). For each class: missed of eligible (the denominator is the number of weight-2 points of the class,
  which counts a point once where a whole set of it is in the pack; a point with evidence for one of several points is not a unit of the class). Pairs: E against A, F' against B, the thorough arm against A
  (both frozen: the classes collapse to both and neither), and F against D when both arms exist.
- Gap sentences (scripts/eval2_score.py gap_sentences, the scorer's own detector): the count per arm and the answers with at least one, as a flag for the reading only; never counted as a refusal or as a miss.

**Models and the comparison with Qwen (W8, W9, W10; described, not decided).** The test answers on up to three base models: `qwen` (`Qwen3.8-27B-4bit`, the arms above), `gemma26` (Gemma 4 26B-A4B, M4's 4-bit build) and `gemma31` (Gemma 4 31B, 4-bit, built later in task G0; the real run must
pass its id and refuses the placeholder). `--models` takes a list of them (all three by default; Qwen must be in it, the rule is Qwen's). Every model gets arms A and E, with the same call, decoding, 2,048-token answer limit and packs as Qwen's A and E, and its A runs on the 116 mechanical items. The
2,048-token limit is the same number of tokens in every model, but a token is a different length of text under each model's tokenizer (fair by tokens, not by text); the report prints the words per answer and the wrong statements per 1,000 words next to the rate per 10 answers.
The thinking arms (B, D, F, F', and for a model other than Qwen also C) run for a model only when `--thinking-models` names it: a comma list, or `none` (the default; the real run passes it explicitly and it is recorded in the manifest and in this header). Without Qwen in the list
Qwen answers A, E and R, and, when the variant has a checking pass, C (the checking pass on A's drafts; it needs no thinking); Qwen's thorough-profile row is then NOT TESTED, and C - A is printed as description only. The planned run names the two Gemma models (the conductor's decision of 2026-11-23, after the Gemma thinking measurement,
`runs/m7/reports/gemma_think.md`); under `--check-variant none` such a model answers A, E and its thinking-only arm B. The thinking probe runs for each other model before any answer, and its result is reported; a model whose switch is not honoured, or whose thinking arrives in a format the stream reader does not know, gets no thinking arm (the chain refuses it before any answer). Each model is loaded alone and unloaded after its stages, in the order gemma31, gemma26, qwen (inside Qwen the order is unchanged); the server gate
never unloads a model another client loaded (W2). Read blind: Qwen's A, D, E, F (A and E without thinking), each Gemma model's A and E, and its thorough arm (D; B under `none`) when it ran, all of a scenario's answers in one bundle under random letters (8 answers in the planned run). For each Gemma model against Qwen, on the same arm (A with A, E with E, D with D or B with B when both ran; and a Gemma B against Qwen's A, label "B~A"; and, between two Gemma models, B against B), the report prints:
- wrong statements per 10 answers, paired by scenario, with a 90 % interval ({Z90} standard errors) (needs the reading);
- judged coverage, paired by scenario, with its standard error and a 90 % interval;
- new harmful consequences both ways (the Gemma answer's against Qwen's answer to the same scenario, and Qwen's against Gemma's), confirmed by the second reader as above (needs the reading);
- mechanical all-pass on the 116 items and without the tier A kinds {", ".join(LOOKUP_KINDS)}, as a paired difference in percentage points per item with a 90 % interval (arm E has no mechanical items: arm A's are used for it);
- cut answers, answers whose check missed the deadline, and the time (median and 90th percentile, reported).
The line "clean winner against Qwen on arm X: yes/no" is yes only when ALL of these hold: the upper end of the wrong-statement difference is below 0; the coverage difference is at least {CMP_COV_MIN:+.2f} with its lower end above {CMP_COV_LOWER:+.2f};
there are 0 confirmed new harmful consequences (the Gemma answers' against Qwen's); the lower end of the mechanical difference is above {CMP_MECH_LOWER:+.0f} points (read on both counts, with and without the lookup kinds: the conductor's decision of 2026-11-23, stricter than the one line of the brief). A condition that waits for the reading
or for the second reader is printed as waiting, not as a pass. **The comparison decides nothing: the Owner decides publishing after the tests (W10).**
Point-level measures of E against A are printed for every model. Arm R (grounded-g3 on the r4g2 packs, W7) is judged only: its coverage against A is printed as description and it is in no decided row and not read.
A measure that is missing, not a number or not computed is an error, never a pass.
The last line of the log and of the report is M7 SIX-ARM TEST DONE: knowledge-base row: <build|a_only|none ...>; thorough-profile row: <Qwen's word, and with a thinking Gemma model the outcome of each model's row>.
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


# ---------------------------------------------------------------- the models and their sets (W8, W9)
def parse_key_list(s, what, allow_none=False):
    keys = [x.strip() for x in str(s if s is not None else "").split(",") if x.strip()]
    if allow_none and (not keys or keys == ["none"]):
        return []
    if not keys:
        raise SixError(f"{what} names no model (a comma list from {', '.join(MODEL_KEYS)})")
    bad = [k for k in keys if k not in MODEL_KEYS]
    if bad:
        raise SixError(f"{what}: unknown model {bad[0]!r} (one of {', '.join(MODEL_KEYS)})")
    if len(set(keys)) != len(keys):
        raise SixError(f"{what} names a model twice")
    return keys


def is_placeholder(model_id):
    return not str(model_id or "").strip() or "placeholder" in str(model_id).lower()


def parse_yes_no(v, what):
    v = str(v if v is not None else "yes").strip().lower()
    if v not in ("yes", "no"):
        raise SixError(f"{what} must be yes or no (got {v!r})")
    return v == "yes"


def plan_models(models, thinking=None, ids=None, dry=False, think_mech="yes"):
    """The models of the run in the order their answers are made (gemma31, gemma26, qwen): [{"key", "id", "thinking"}]. Refuses (SixError): an unknown or repeated model, a
    --models list without qwen (the rule and every comparison are Qwen's), a --thinking-models list that is not inside --models (`none` or an empty list: no model thinks; a list
    without qwen: Qwen answers without thinking, K4b), and, in a real run, a placeholder or empty model id (the 31B's 4-bit build is made later, task G0) or two models with one id."""
    ms = parse_key_list(models, "--models")
    th = parse_key_list(thinking if thinking is not None else ",".join(DEFAULT_THINKING), "--thinking-models", allow_none=True)
    if "qwen" not in ms:
        raise SixError("--models must include qwen: the rule and the comparisons are Qwen's")
    if any(k not in ms for k in th):
        raise SixError("--thinking-models names a model that --models does not")
    mid = {**DEFAULT_MODEL_IDS, **{k: v for k, v in (ids or {}).items() if v}}
    tm = parse_yes_no(think_mech, "--thinking-mech")
    plan = [{"key": k, "id": mid[k], "thinking": k in th, "think_mech": k == "qwen" or tm} for k in ANSWER_ORDER if k in ms]   # think_mech: a thinking Gemma model also answers the 116 mechanical items
    if not dry:
        for m in plan:
            if is_placeholder(m["id"]):
                raise SixError(f"the model id of {m['key']} is a placeholder ({m['id']!r}): the real run must pass its id (--{m['key']}-model ID)")
        if len({m["id"] for m in plan}) != len(plan):
            raise SixError("two models have the same id")
    return plan


def set_name(arm, model):
    return arm if model == "qwen" else f"{arm}_{model}"


def model_set_stems(variant, model, thinking):
    """{set name: file stem of its D5 answers} of one model. A model that thinks: the variant's sets (A B C D E F F'; under `none` A B E F'). Qwen without thinking: A, E and, with a checking pass,
    C (the checking pass on A's drafts needs no thinking), plus R, the comparison arm. Another model without thinking: A and E."""
    if thinking:
        base = dict(SET_STEMS[variant])
    else:
        base = {"A": "A", "E": "E"}
        if model == "qwen" and variant != "none":
            base["C"] = "C"
    if thinking and model != "qwen" and variant == "none":
        base.pop("Fp", None)         # K4b, 2026-11-23: a thinking Gemma model under none has A, E and its thinking-only arm B (no perfect-pack thinking arm)
    out = {set_name(k, model): (v if model == "qwen" else f"{v}_{model}") for k, v in base.items()}
    if model == "qwen":
        out["R"] = "R"
    return out


def all_set_stems(variant, plan):
    """Every judged set of the run, Qwen first: {set name: stem}."""
    out = {}
    for m in sorted(plan, key=lambda m: (m["key"] != "qwen", m["key"])):
        out.update(model_set_stems(variant, m["key"], m["thinking"]))
    return out


def thinking_of(plan, key):
    return any(m["key"] == key and m["thinking"] for m in plan)


def thorough_of(variant, plan):
    """The thorough arm of Qwen's decided row: D (B under `none`) when Qwen thinks, else None (the row is then not run: no thinking arm in this run)."""
    if thinking_of(plan, "qwen"):
        return "D" if variant != "none" else "B"
    return "C" if variant != "none" else None      # W11: without thinking the thorough candidate is the checking pass alone (C)


def thorough_label(variant):
    """The thorough arm of a thinking Gemma model: D (thinking, then the checking pass), or B (thinking only) under `none`."""
    return "D" if variant != "none" else "B"


def plan_from_args(a, dry=False):
    ids = {"qwen": getattr(a, "qwen_model", None), "gemma26": getattr(a, "gemma26_model", None), "gemma31": getattr(a, "gemma31_model", None)}
    return plan_models(getattr(a, "models", None) or ",".join(MODEL_KEYS), getattr(a, "thinking_models", None), ids, dry, getattr(a, "thinking_mech", None) or "yes")


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


def cmd_gate_models(a):
    """The models of the run (refusal exit 2): the list, the thinking list, the ids; the real run refuses a placeholder id."""
    try:
        plan = plan_from_args(a, dry=a.dry)
    except SixError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print("models in the order of their answers: " + "; ".join(f"{m['key']} ({m['id'] if not a.dry else 'stub'}; arms A, E{', thorough arms' if m['thinking'] else ''})" for m in plan))
    return 0


def cmd_model_ids(a):
    """KEY=ID per selected model (the ids given, else the defaults); no space in an id."""
    for m in plan_from_args(a, dry=True):
        print(f"{m['key']}={m['id']}")
    return 0


def cmd_model_order(a):
    """One line for the shell: KEY:yes|no (thinking) in the order of the answers."""
    print(" ".join(f"{m['key']}:{'yes' if m['thinking'] else 'no'}" for m in plan_from_args(a, dry=True)))
    return 0


def cmd_sets(a):
    """The judged sets of the run, one 'NAME STEM decided|described' per line (Qwen first; Qwen's sets but R are decided, the rest is described)."""
    variant, _ = check_params(a.check_variant, 1)
    for k, v in all_set_stems(variant, plan_from_args(a, dry=True)).items():
        print(f"{k} {v} {'described' if is_described(k) else 'decided'}")
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
        for key in ("frozen", RPACK_KEY):   # arm R replays the funnel's r4g2 packs (W7)
            miss = [i for i in ids if not isinstance(fp.get(i, {}).get(key), list) or not fp[i][key]]
            if miss:
                raise SixError(f"the funnel's packs lack a {key} pack for {len(miss)} scenarios (first: {miss[0]})")
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
    key = a.key or "frozen"
    R6.write_pack_file(a.out, [(i, list(packs[i][key])) for i in items], {"source": "the D5 funnel's packs.jsonl", "source_sha256": sha256_file(Path(a.funnel_work) / "packs.jsonl"), "setting": key})
    print(f"{key} packs: wrote {a.out} ({len(items)} scenarios)")
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
    thought = []
    for r in rows:
        if a.no_thinking:   # a no-thinking answer (A, E, R) must show no reasoning: any reasoning token or character above 0 refuses the file
            td, di = r.get("tokens_detail") or {}, r.get("draft_info") or {}
            if any(num(v) and v > 0 for v in (td.get("draft_reasoning"), td.get("draft_reasoning_chars"), di.get("reasoning_tokens"), di.get("reasoning_chars"))):
                thought.append(r["id"])
        if r.get("draft_streamed") is not True or not isinstance(r.get("tokens_detail"), dict) or not r.get("prompt_sha256"):
            bad.append(r["id"])
        elif a.real and not (r.get("draft_info") or {}).get("request_body_sha256"):
            bad.append(r["id"])
    print(f"check rows {a.file}: {len(rows)} of {a.expect}, {len(bad)} without a streamed draft, prompt hash, token detail{' or request-body hash' if a.real else ''}")
    if thought:
        print(f"check rows {a.file}: {len(thought)} answers of a no-thinking arm show reasoning tokens or characters above 0 (the switch was not honoured): {', '.join(sorted(thought)[:5])}")
    return 0 if len(rows) == a.expect and not bad and not thought else 1


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
def effective_verdict(e):
    """The verdict of one judged point as the coverage counts it: an unquoted 'present' or 'contradicted' is 'unverified'."""
    v = e["verdict"]
    if v == "absent" and e.get("quote_ok") is False and e.get("judge_verdict") in ("present", "contradicted"):
        v = "unverified"
    return v


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
            v = effective_verdict(e)
            k = 0 if S.point_sets(pts[pid]) else 2
            cnt[k + 1] += 1
            cnt[k] += v == "present"
        acc.setdefault(r["id"], []).append(cnt)
    return {i: tuple(sum(c[k] for c in v) / len(v) for k in range(4)) for i, v in acc.items()}


# ---------------------------------------------------------------- point-level and essential-unit measures (described, not decided; the rule never reads them)
def point_scores(results, rubrics, set_name, rows):
    """{(scenario id, point id): (score, covered, disputed)} for one arm: score = share of the valid passes in which the point is present, covered = present in every valid pass,
    disputed = present in some but not all. A row with no draft is delivered as empty: score 0, not covered, not disputed. No row is dropped."""
    nodraft = {r["id"] for r in rows if is_no_draft(r)}
    acc = {}
    for r in results:
        if not r.get("valid_reply") or r.get("reference_given") or r["set"] != set_name:
            continue
        for pid, e in r["must"].items():
            acc.setdefault((r["id"], pid), []).append(effective_verdict(e) == "present")
    out = {}
    for rb in rubrics:
        for pt in rb["must"]:
            k = (rb["id"], pt["id"])
            if rb["id"] in nodraft:
                out[k] = (0.0, False, False)
                continue
            v = acc.get(k)
            if not v:
                raise SixError(f"no valid pass for the point {k[0]}/{k[1]} of set {set_name}")
            out[k] = (sum(v) / len(v), all(v), any(v) and not all(v))
    return out


def point_class(pt, frozen_pack, perfect_pack):
    """S-both, S-perfect-only, S-other (a set in the frozen pack only, or in neither), no-set."""
    sets = S.point_sets(pt)
    if not sets:
        return "no-set"
    fz, pf = S.point_flags(pt, frozen_pack)[0], S.point_flags(pt, perfect_pack)[0]
    return "S-both" if fz and pf else "S-perfect-only" if pf else "S-other"


def no_set_kind(pt):
    return "not indexed" if pt.get("not_indexed") else "no covering set" if pt.get("no_covering_set") else "no set (kind not recorded)"


def ratio_se(rows):
    """Cluster (ratio) standard error of sum(d)/sum(m) with scenarios as clusters. rows: (points m_i, difference d_i); scenarios without points are left out. -> (ratio, se, n clusters)."""
    rows = [(m, d) for m, d in rows if m > 0]
    n = len(rows)
    if n < 1:
        return None, None, 0
    M = sum(m for m, _ in rows)
    r = sum(d for _, d in rows) / M
    if n < 2:
        return r, None, n
    return r, math.sqrt(n / (n - 1) * sum((d - r * m) ** 2 for m, d in rows)) / M, n


def transition_table(cand, ref, sc_c, sc_r, rubrics, packs):
    """Transitions of the must points from the reference arm to the candidate arm, per class; packs = {id: (frozen pack, perfect pack)}."""
    classes = ("S-both", "S-perfect-only", "S-other", "no-set")
    T = {c: {"points": 0, "w2": 0, "both": 0, "neither": 0, "gained": 0, "lost": 0, "w2_both": 0, "w2_neither": 0, "w2_gained": 0, "w2_lost": 0,
             "disputed_ref": 0, "disputed_cand": 0} for c in classes}
    kinds, regress, per = {}, [], {c: {} for c in classes}
    for rb in rubrics:
        fz, pf = packs[rb["id"]]
        for pt in rb["must"]:
            k = (rb["id"], pt["id"])
            c = point_class(pt, fz, pf)
            t = T[c]
            w2 = pt.get("weight") == 2
            sr, sc = sc_r[k], sc_c[k]
            t["points"] += 1
            t["w2"] += w2
            t["disputed_ref"] += sr[2]
            t["disputed_cand"] += sc[2]
            cell = "both" if sr[1] and sc[1] else "gained" if sc[1] else "lost" if sr[1] else "neither"
            t[cell] += 1
            if w2:
                t["w2_" + cell] += 1
            if c == "no-set":
                kinds[no_set_kind(pt)] = kinds.get(no_set_kind(pt), 0) + 1
            if cell == "lost" and c == "S-both":
                regress.append(f"{rb['id']}/{pt['id']}" + (" (weight 2)" if w2 else ""))
            pc = per[c].setdefault(rb["id"], [0, 0.0])
            pc[0] += 1
            pc[1] += sc[0] - sr[0]
    clustered = {}
    for c in ("S-perfect-only", "S-both"):
        r_, se_, n_ = ratio_se([(m, d) for m, d in per[c].values()])
        clustered[c] = {"ratio": r_, "se": se_, "clusters": n_, "points": sum(m for m, _ in per[c].values()),
                        "interval90": None if se_ is None else [r_ - Z90 * se_, r_ + Z90 * se_]}
    return {"candidate": cand, "reference": ref, "classes": T, "no_set_kinds": kinds, "regressions_on_S_both": sorted(regress), "paired": clustered}


def evidence_table(cand, ref, sc_c, sc_r, rubrics, pk_c, pk_r):
    """Weight-2 points by where a whole covering set is: both packs, the candidate's only, the reference's only, neither. missed = not covered; eligible = the class's weight-2 points."""
    names = ("both packs", "candidate's pack only", "reference's pack only", "neither pack")
    T = {n: {"eligible": 0, "missed_cand": 0, "missed_ref": 0, "of_which_no_set": 0} for n in names}
    for rb in rubrics:
        for pt in rb["must"]:
            if pt.get("weight") != 2:
                continue
            ec, er = S.point_flags(pt, pk_c[rb["id"]])[0], S.point_flags(pt, pk_r[rb["id"]])[0]
            n = names[0] if ec and er else names[1] if ec else names[2] if er else names[3]
            k = (rb["id"], pt["id"])
            t = T[n]
            t["eligible"] += 1
            t["missed_cand"] += not sc_c[k][1]
            t["missed_ref"] += not sc_r[k][1]
            t["of_which_no_set"] += not S.point_sets(pt)
    return {"candidate": cand, "reference": ref, "classes": T}


def gap_stats(rows):
    import eval2_score
    per = [len(eval2_score.gap_sentences(r.get("text") or "")) for r in rows]
    return {"gap_sentences": sum(per), "answers_with_gap": sum(1 for x in per if x), "answers": len(rows)}


def point_level(arm_rows, results, rubrics, thorough, names=None):
    """All the point-level measures. Descriptive only. arm_rows: {arm name: rows}; the packs are those the answers saw (each arm's own rows). names: {arm name: the set name
    in the judge's results} where it differs (a Gemma model's sets carry a suffix); the arm names stay A, B, D, E, F, F' in every table."""
    names = names or {}
    if "A" not in arm_rows or "E" not in arm_rows:
        raise SixError("the point-level measures need the arms A and E")
    sc = {k: point_scores(results, rubrics, names.get(k, k), arm_rows[k]) for k in arm_rows}
    pk = {k: {r["id"]: list(r["pack"]) for r in rows} for k, rows in arm_rows.items()}
    packs = {rb["id"]: (pk["A"][rb["id"]], pk["E"][rb["id"]]) for rb in rubrics}
    trans = [transition_table(c, r, sc[c], sc[r], rubrics, packs) for c, r in (("E", "A"), ("Fp", "B")) if c in sc and r in sc]
    ev_pairs = [(c, r) for c, r in (("E", "A"), ("Fp", "B"), (thorough, "A"), ("F", "D")) if c in sc and r in sc and c != r]
    ev = [evidence_table(c, r, sc[c], sc[r], rubrics, pk[c], pk[r]) for c, r in ev_pairs]
    cls = {}
    for rb in rubrics:
        for pt in rb["must"]:
            c = point_class(pt, *packs[rb["id"]])
            cls[c] = cls.get(c, 0) + 1
    return {"classes": cls, "points": sum(cls.values()), "transitions": trans, "evidence": ev, "gaps": {k: gap_stats(rows) for k, rows in arm_rows.items()}}


def point_level_md(pl):
    L = ["", "**Point-level and essential-unit measures (described, not decided; nothing here enters the rule).** A point is covered when it is present in every valid pass of the judge, "
         "missed otherwise (a point present in one pass only is counted as missed and shown as disputed). No-draft, cut and timed-out answers are scored as delivered (no-draft as empty); none is dropped.", ""]
    c = pl["classes"]
    L.append(f"Point classes ({pl['points']} must points; strict count, a whole covering set in the pack the answers saw): " + ", ".join(f"{k} {c.get(k, 0)}" for k in ("S-both", "S-perfect-only", "S-other", "no-set")) + ".")
    for t in pl["transitions"]:
        cand, ref = t["candidate"], t["reference"]
        nk = ", ".join(f"{k} {v}" for k, v in sorted(t["no_set_kinds"].items())) or "none"
        L += ["", f"Transitions of points, {cand} against {ref} (both = covered in both arms, neither = missed in both; weight-2 points in brackets; no-set kinds: {nk}):", "",
              f"| class | points (w2) | both | neither | gained | lost | disputed {ref} / {cand} |", "|---|---|---|---|---|---|---|"]
        for k, v in t["classes"].items():
            L.append(f"| {k} | {v['points']} ({v['w2']}) | {v['both']} ({v['w2_both']}) | {v['neither']} ({v['w2_neither']}) | {v['gained']} ({v['w2_gained']}) | {v['lost']} ({v['w2_lost']}) | {v['disputed_ref']} / {v['disputed_cand']} |")
        L.append("")
        L.append(f"Regressions on S-both ({cand} loses a point that {ref} covered): " + (", ".join(t["regressions_on_S_both"]) if t["regressions_on_S_both"] else "none"))
        for k, v in t["paired"].items():
            iv = v["interval90"]
            rr = "n/a" if v["ratio"] is None else "%+.3f" % v["ratio"]
            L.append(f"- {cand} - {ref} score within {k}: {rr} per point over {v['points']} points in {v['clusters']} scenarios "
                     f"(scenario-clustered SE {f2(v['se'])}, 90 % interval {('[%+.3f, %+.3f]' % tuple(iv)) if iv else 'n/a'})")
    for e in pl["evidence"]:
        cand, ref = e["candidate"], e["reference"]
        L += ["", f"Essential (weight-2) units by where the evidence is, {cand} against {ref} (missed of eligible; eligible = weight-2 points of the class; a point with no covering set is in neither pack):", "",
              f"| evidence (a whole covering set) | eligible | missed by {cand} | missed by {ref} | of which no covering set |", "|---|---|---|---|---|"]
        for k, v in e["classes"].items():
            L.append(f"| {k} | {v['eligible']} | {v['missed_cand']} of {v['eligible']} | {v['missed_ref']} of {v['eligible']} | {v['of_which_no_set']} |")
    L += ["", "Gap sentences (the scorer's detector; a flag for the reading only, never a refusal or a miss): " + "; ".join(
        f"{k}: {v['gap_sentences']} in {v['answers_with_gap']} of {v['answers']} answers" for k, v in pl["gaps"].items())]
    return L


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
           "median_chars": median([len(r.get("text") or "") for r in rows]),
           "words_by_id": {r["id"]: len((r.get("text") or "").split()) for r in rows}, "time": arm_time(rows, exp_s, retr_s), "check": check_stats(rows)}
    return out


def pair(m, x, y):
    px, py = m["arms"][x]["P_by_id"], m["arms"][y]["P_by_id"]
    d = [px[i] - py[i] for i in px]
    m_, s_ = mean(d), se(d)
    return {"mean": m_, "se": s_, "n": len(d), "ci196": interval(m_, s_, Z90_2), "ci90": interval(m_, s_, Z90)}


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
            out.setdefault("pass_by_id", {})[i] = int(bool(rows[i]["all_pass"]))
            if items_kinds.get(i) in LOOKUP_KINDS:
                out.setdefault("lookup_ids", []).append(i)
            if items_kinds.get(i) not in LOOKUP_KINDS:
                out["n_without_lookup"] += 1
                out["pass_without_lookup"] += bool(rows[i]["all_pass"])
    return out


def failed_models(work):
    """The models whose answer stage failed after its retries: {key: {"thinking": bool, "why": text}}, from the markers `model_failed_<key>.json` the chain wrote (subcommand model-failed). Qwen never gets one: its failure stops the run."""
    out = {}
    for k in MODEL_KEYS:
        f = Path(work) / f"model_failed_{k}.json"
        if f.is_file() and k != "qwen":
            d = json.loads(f.read_text(encoding="utf-8"))
            out[k] = {"thinking": bool(d.get("thinking")), "why": str(d.get("why") or "")}
    return out


def cmd_model_failed(a):
    """Record that a model's answer stage failed after its retries (called by the chain; refuses Qwen, whose failure stops the run)."""
    if a.model not in MODEL_KEYS or a.model == "qwen":
        print(f"model-failed: {a.model!r} cannot be marked (Qwen's failure stops the run)", file=sys.stderr)
        return 2
    f = Path(a.work) / f"model_failed_{a.model}.json"
    if not f.exists():
        f.write_text(json.dumps({"model": a.model, "thinking": a.thinking == "yes", "why": a.why}, indent=1) + "\n", encoding="utf-8")
    print(f"model {a.model}: answer stage failed after its retries; not available ({a.why}); marker {f.name}")
    return 0


def cmd_measures(a):
    W = Path(a.work)
    variant, cap = check_params(a.check_variant, a.think_cap)
    plan = plan_from_args(a, dry=True)
    stems = all_set_stems(variant, plan)
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
         "retrieval_frozen_median_s": retr_s, "retrieval_timing_n": len(tids), "arms": {},
         "models": [{"key": mm["key"], "thinking": mm["thinking"], "think_mech": mm["think_mech"]} for mm in plan]}
    qsets = model_set_stems(variant, "qwen", thinking_of(plan, "qwen"))
    decided = {k for k in qsets if k != "R"}               # Qwen's sets (without R): the rows the rule decides. Everything else (R, the other models' sets) is described: K4b, a described set that cannot be computed is "not available" and stops nothing
    m["not_available"] = {}
    m["failed_models"] = failed_models(W)      # K4b: a model whose answer stage failed after its retries ("not available: answer stage failed"); it is not in the plan above (the chain passes the models that answered)
    arm_rows = {}
    for name, stem in stems.items():
        try:
            rows = rd(W / f"ans_d5_{stem}.jsonl")
            m["arms"][name] = set_stats(name, rows, a, rub, results, exp_s, retr_s, a.stub)
            m["arms"][name]["file"] = f"ans_d5_{stem}.jsonl"
            arm_rows[name] = rows
        except (SixError, OSError, KeyError, ValueError, TypeError, G.RuleError) as e:
            if name in decided:
                raise
            m["arms"].pop(name, None)
            m["not_available"][name] = f"{type(e).__name__}: {e}"
    thorough = thorough_of(variant, plan)
    m["thorough_arm"] = thorough
    pairs = [("E", "A"), ("B", "A"), ("Fp", "E"), ("R", "A"), ("C", "A")]   # R - A: description only (W7)
    if thorough:
        pairs.append((thorough, "A"))
    if variant != "none":
        pairs += [("D", "B"), ("D", "C"), ("B", "C"), ("F", "E"), ("F", "Fp")]
    m["point_level"] = point_level({k: v for k, v in arm_rows.items() if k in qsets and k != "R"}, results, rub, thorough)   # described, not decided: the rule never reads it
    m["point_level_models"] = {}
    for mm in plan:   # the point-level measures of E against A apply to every model
        if mm["key"] == "qwen":
            continue
        gs = model_set_stems(variant, mm["key"], mm["thinking"])
        names = {k[: -len(mm["key"]) - 1]: k for k in gs}
        if any(sn not in arm_rows for sn in names.values()):
            m["not_available"][f"point_level_{mm['key']}"] = "a set of the model is not available"
            continue
        try:
            m["point_level_models"][mm["key"]] = point_level({pk: arm_rows[sn] for pk, sn in names.items()}, results, rub, thorough, names)
        except (SixError, OSError, KeyError, ValueError, TypeError, G.RuleError) as e:
            m["not_available"][f"point_level_{mm['key']}"] = f"{type(e).__name__}: {e}"
    m["pairs"] = {f"{x}-{y}": pair(m, x, y) for x, y in pairs if x in m["arms"] and y in m["arms"] and x != y}
    tl = thorough_label(variant)
    m["thorough_models"] = {mm["key"]: {"set": set_name(tl, mm["key"]), "ref": set_name("A", mm["key"])} for mm in plan if mm["key"] != "qwen" and mm["thinking"]}
    for k, e in m["thorough_models"].items():     # each thinking Gemma model's thorough arm against its own A (K4b, 2026-11-23)
        if e["set"] in m["arms"] and e["ref"] in m["arms"]:
            try:
                m["pairs"][f"{e['set']}-{e['ref']}"] = pair(m, e["set"], e["ref"])
            except (KeyError, TypeError, ValueError) as ex:
                m["not_available"][f"thorough_{k}"] = f"{type(ex).__name__}: {ex}"
    gb = [e["set"] for e in m["thorough_models"].values() if e["set"] in m["arms"]]
    m["gemma_b_pairs"] = {}
    for i in range(len(gb)):          # B against B between two Gemma models (described)
        for j in range(i + 1, len(gb)):
            try:
                m["gemma_b_pairs"][f"{gb[j]}-{gb[i]}"] = pair(m, gb[j], gb[i])
            except (KeyError, TypeError, ValueError) as ex:
                m["not_available"][f"pair_{gb[j]}_{gb[i]}"] = f"{type(ex).__name__}: {ex}"
    for mm in plan:    # a Gemma model against Qwen on the same arm: the paired coverage differences (W10; described, decides nothing)
        if mm["key"] == "qwen":
            continue
        for k in model_set_stems(variant, mm["key"], mm["thinking"]):
            plain = k[: -len(mm["key"]) - 1]
            if plain in m["arms"] and k in m["arms"]:
                try:
                    m["pairs"][f"{k}-{plain}"] = pair(m, k, plain)
                except (KeyError, TypeError, ValueError) as e:
                    m["not_available"][f"pair_{k}"] = f"{type(e).__name__}: {e}"
    kinds = {}
    for f in ("items_ma.jsonl", "items_m3.jsonl"):
        kinds.update({r["id"]: r.get("kind") for r in rd(W / f)})
    m["mech"] = {"A": mech_arm(W, "A", ids, kinds)}
    if thorough in ("B", "D"):
        m["mech"]["D"] = mech_arm(W, "D", ids, kinds)   # D is the thinking-only run in the variant none
    for mm in plan:    # each model's A on the 116 mechanical items, and its D when it ran (described: a failure is "not available")
        if mm["key"] != "qwen":
            for v in ("A", "D") if mm["thinking"] and mm["think_mech"] else ("A",):
                try:
                    m["mech"][f"{v}_{mm['key']}"] = mech_arm(W, f"{v}_{mm['key']}", ids, kinds)
                except (SixError, OSError, KeyError, ValueError, TypeError, G.RuleError) as e:
                    m["not_available"][f"mech_{v}_{mm['key']}"] = f"{type(e).__name__}: {e}"
    m["compare"] = compare_block(m, plan)
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
    L += [f"(median s and p90 s are reported, not decided: W5)", "",
          f"| set | P ({s['D5']} answers, 2 passes) | points present all | with set | without set | cut | cut while thinking | empty | idents per 10 | median s | p90 s | draft s | thinking s | check s | cached prompt tokens, median (answers reported) | words per answer, median |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for k, v in m["arms"].items():
        p, t = v["points"], v["time"]
        sh = lambda c: f"{c['present']:.1f}/{c['of']} ({100 * c['present'] / c['of']:.1f} %)" if c["of"] else "n/a"  # noqa: E731
        L.append(f"| {k} | {f2(v['P'])} | {sh(p['all'])} | {sh(p['with_set'])} | {sh(p['without_set'])} | {v['cut']} | {v['cut_while_thinking']} | {v['empty']} | {f2(v['idents_per10'], 2)} | "
                 f"{f2(t['median_s'], 1)} | {f2(t['p90_s'], 1)} | {f2(t['draft_median_s'], 1)} | {f2(t['thinking_median_s'], 1)} | {f2(t['check_median_s'], 1)} | "
                 f"{f2(v.get('cached_median'), 0)} ({v.get('cached_reported')}) | {f2(median(list((v.get('words_by_id') or {}).values())), 0)} |")
    L += ["", "Paired differences of P (mean, standard error, n):", ""]
    for k, v in m["pairs"].items():
        L.append(f"- {k}: {v['mean']:+.3f} (SE {f2(v['se'])}, n {v['n']})")
    L += ["", f"B against C (descriptive, two comparisons: intervals of {Z90_2} standard errors, 0.025 each side; nothing is decided by them; empty without the arms B or C):", ""]
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
    L += point_level_md(m["point_level"]) if m.get("point_level") else []
    for g, pl in (m.get("point_level_models") or {}).items():
        L += ["", f"**Model {g}: the same point-level measures (E against A{', F-prime against B' if any(x['candidate'] == 'Fp' for x in pl['transitions']) else ''}).**"] + point_level_md(pl)
    nd = {k: v.get("no_draft_ids") for k, v in m["arms"].items() if v.get("no_draft_ids")}
    na = m.get("not_available") or {}
    if na:
        L += ["", "Described sets and comparisons that are NOT AVAILABLE (they stop nothing; Qwen's decided rows do not depend on them):", ""] + [f"- {k}: not available: {v}" for k, v in sorted(na.items())]
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


# ---------------------------------------------------------------- a Gemma model against Qwen (W10: described, decides nothing) and the harm by consequence (W6)
def _paired_points(x, y, ids):
    d = [100.0 * (x[i] - y[i]) for i in ids]
    m_, s_ = mean(d), se(d)
    return {"mean_pts": m_, "se_pts": s_, "n": len(d), "interval90": interval(m_, s_, Z90)}


def mech_compare(mg, mq):
    """The paired difference per mechanical item (Gemma minus Qwen), in percentage points of all-pass, over all items and without the lookup kinds."""
    pg, pq = mg.get("pass_by_id") or {}, mq.get("pass_by_id") or {}
    if not pg or sorted(pg) != sorted(pq):
        raise SixError("the mechanical items of the two arms differ or are missing")
    lookup = set(mq.get("lookup_ids") or [])
    ids = sorted(pg)
    return {"all": _paired_points(pg, pq, ids), "without_lookup": _paired_points(pg, pq, [i for i in ids if i not in lookup]),
            "all_pass": {"gemma": [mg["pass_all"], mg["n"]], "qwen": [mq["pass_all"], mq["n"]]}}


def compare_block(m, plan):
    """{model: {arm label: the measured parts of the comparison with Qwen on that arm}} for A, E and the thorough arm where both ran."""
    out = {}
    thorough = m.get("thorough_arm")
    nchk = lambda arm: ((arm.get("check") or {}).get("status") or {}).get("not_checked", 0)  # noqa: E731
    for mm in plan:
        g = mm["key"]
        if g == "qwen":
            continue
        rows = {}
        entries = []     # (label, Gemma set, Qwen set, Gemma's mechanical arm, Qwen's mechanical arm): the mechanical items are answered by A and D only, so arm E is compared on A's
        for label in dict.fromkeys(x for x in ("A", "E", thorough) if x):
            mk = "D" if (label == thorough and thorough in ("B", "D")) else "A"
            entries.append((label, set_name(label, g), label, mk, mk))
        gt = (m.get("thorough_models") or {}).get(g)
        if gt and m.get("variant") == "none":        # the Gemma model's thinking-only B against Qwen's A (K4b, 2026-11-23); B against Qwen's B is the "B" entry above when Qwen has B
            entries.append(("B~A", gt["set"], "A", "D", "A"))
        for label, gx, qx, mg, mq in entries:
            if gx not in m["arms"] or qx not in m["arms"]:
                continue
            ga, qa = m["arms"][gx], m["arms"][qx]
            gm, qm = m["mech"].get(f"{mg}_{g}"), m["mech"].get(mq)
            try:
                mech = mech_compare(gm, qm) if gm and qm else None
            except SixError as e:
                mech = None
                m.setdefault("not_available", {})[f"mech_compare_{gx}_{qx}"] = str(e)
            try:
                rows[label] = {"gemma_set": gx, "qwen_set": qx, "coverage": pair(m, gx, qx), "mech_arm": mg if mg == mq else f"{mg} ({g}) against {mq} (Qwen)", "mech": mech,
                               "cut": {"gemma": ga["cut"], "qwen": qa["cut"]}, "empty": {"gemma": ga["empty"], "qwen": qa["empty"]},
                               "not_checked": {"gemma": nchk(ga), "qwen": nchk(qa)},
                               "time": {"gemma": {k: ga["time"][k] for k in ("median_s", "p90_s")}, "qwen": {k: qa["time"][k] for k in ("median_s", "p90_s")}},
                               "cached": {"gemma": [ga.get("cached_median"), ga.get("cached_reported")], "qwen": [qa.get("cached_median"), qa.get("cached_reported")]}}
            except (KeyError, TypeError, ValueError) as e:     # described: the comparison of this arm is "not available" and stops nothing
                m.setdefault("not_available", {})[f"compare_{gx}_{qx}"] = f"{type(e).__name__}: {e}"
        out[g] = rows
    return out


def norm_cons(c):
    return " ".join(str(c or "").lower().split())


def cons_group(c):
    """The group of a normalised consequence from the list; "other" for "other: ..."."""
    return OTHER_GROUP if c.startswith("other:") else GROUP_OF[c]


def check_label(c, where):
    """A consequence label from the closed list, or "other: <words>"; anything else is refused with a message (K4b: an unknown label is not a consequence of its own)."""
    c = norm_cons(c)
    if not c:
        raise SixError(f"a harmful finding of {where} has no consequence (the field is required)")
    if c in GROUP_OF or (c.startswith("other:") and c[len("other:"):].strip()):
        return c
    raise SixError(f"a harmful finding of {where} has the consequence {c!r}, which is not on the list ({', '.join(CONSEQUENCES)}) and not 'other: <words>'")


def harm_findings(rows, setname, reader):
    """{scenario id: {consequence: [rows]}} of the harmful findings of one reader on one set (minor ones included: K4b; the rule text says so). A harmful finding without a
    consequence, or with a label outside the list (other than 'other: ...'), is an error with a message."""
    out = {}
    for r in rows:
        if r.get("set") != setname or r.get("reader") != reader or r.get("kind") != "harmful":
            continue
        c = check_label(r.get("consequence"), f"{reader} on {setname}/{r.get('id')}")
        out.setdefault(r["id"], {}).setdefault(c, []).append(r)
    return out


def confirms(c, c2, ref_cons):
    """The second reader's consequence c2 confirms the first reader's new consequence c: the same, from the same group, or one the reference answer lacks (K4b strict reading)."""
    return c2 == c or cons_group(c2) == cons_group(c) or c2 not in ref_cons


def harm_match(rows, reads, cand, ref, first, second=None):
    """W6, K4b strict reading. A harmful finding (minor ones counted) in the candidate's answer is NEW when the reference answer to the same scenario has no harmful finding with the same
    consequence (the first reader's findings on both). A new one is CONFIRMED when the second reader marks the same answer harmful with the same consequence, a consequence of the same group
    (HARM_GROUPS) or any consequence the reference lacks; NOT CONFIRMED when the second reader read that answer and marked nothing of that kind; PENDING when the second reader has not read it.
    A harm that only the second reader finds (a consequence group the first reader's findings on that answer lack, and the reference lacks the consequence) is printed as "second reader only"
    (new, unconfirmed by the first reader) and does not enter the bar. reads: the 'read' markers of eval2_blind unblind --reads-out."""
    cf, rf = harm_findings(rows, cand, first), harm_findings(rows, ref, first)
    sf = harm_findings(rows, cand, second) if second else {}
    second_read = {r["id"] for r in (reads or []) if r.get("set") == cand and r.get("reader") == second and r.get("kind") == "read"} if second else set()
    new, confirmed, not_confirmed, pending, second_only = [], [], [], [], []
    for sid in sorted(cf):
        for c in sorted(cf[sid]):
            if c in rf.get(sid, {}):
                continue
            it = {"id": sid, "consequence": c}
            new.append(it)
            ok = any(confirms(c, c2, rf.get(sid, {})) for c2 in sf.get(sid, {}))
            (confirmed if ok else not_confirmed if sid in second_read else pending).append(it)
    for sid in sorted(sf):
        for c2 in sorted(sf[sid]):
            if c2 in rf.get(sid, {}):
                continue
            if any(cons_group(c2) == cons_group(c) for c in cf.get(sid, {})):
                continue
            second_only.append({"id": sid, "consequence": c2})
    n_rows = lambda f: sum(len(v) for d in f.values() for v in d.values())  # noqa: E731
    return {"candidate": cand, "reference": ref, "candidate_findings": n_rows(cf), "reference_findings": n_rows(rf),
            "candidate_consequences": sum(len(d) for d in cf.values()), "reference_consequences": sum(len(d) for d in rf.values()),
            "new": new, "confirmed": confirmed, "not_confirmed": not_confirmed, "pending": pending, "second_only": second_only}


def harm_match_lines(h, label=None):
    L = [f"Harm by consequence, {label or h['candidate']} against {h['reference']} (W6; first reader's harmful findings, minor ones counted; raw counts):",
         f"  harmful findings: {h['candidate']} {h['candidate_findings']} ({h['candidate_consequences']} scenario-consequence pairs), {h['reference']} {h['reference_findings']} ({h['reference_consequences']} pairs)",
         f"  new (the reference's answer has no harmful finding with that consequence): {len(h['new'])}; confirmed by the second reader {len(h['confirmed'])}; "
         f"read by the second reader and not confirmed {len(h['not_confirmed'])}; waiting for the second reader {len(h['pending'])}"]
    for k in ("confirmed", "not_confirmed", "pending"):
        for it in h[k]:
            L.append(f"    {k.replace('_', ' ')}: {it['id']}: {it['consequence']}")
    so = h.get("second_only") or []
    L.append(f"  new, unconfirmed (second reader only; printed, not in the bar): {len(so)}")
    for it in so:
        L.append(f"    second reader only: {it['id']}: {it['consequence']}")
    if h["confirmed"]:
        L.append(f"  criterion (confirmed new harmful consequences = {D_HARM_NEW_MAX}): FAIL ({len(h['confirmed'])} confirmed)")
    elif h["pending"]:
        L.append(f"  criterion (confirmed new harmful consequences = {D_HARM_NEW_MAX}): WAITS FOR THE SECOND READER ({len(h['pending'])} new ones are not yet read by the second reader: scenarios "
                 + ", ".join(sorted({it['id'] for it in h['pending']})) + ")")
    else:
        L.append(f"  criterion (confirmed new harmful consequences = {D_HARM_NEW_MAX}): PASS")
    return L


def wrong_counts(rows, setname, reader):
    """Non-minor findings of the kinds wrong, invented and harmful of one reader on one set, per scenario."""
    out = {}
    for r in rows:
        if r.get("set") == setname and r.get("reader") == reader and r.get("kind") in ("wrong", "invented", "harmful") and not r.get("minor"):
            out[r["id"]] = out.get(r["id"], 0) + 1
    return out


def wrong_paired(rows, reads, x, y, reader, words=None):
    """Wrong statements per 10 answers, set x minus set y, paired by scenario over the scenarios the reader read in both sets: mean, standard error, 90 % interval. words: {set: {id: words}}
    (the answers' lengths); with it the rate per 1,000 words of each set over the same scenarios is added (a shorter answer has fewer chances for a wrong statement)."""
    rx = {r["id"] for r in reads if r.get("set") == x and r.get("reader") == reader and r.get("kind") == "read"}
    ry = {r["id"] for r in reads if r.get("set") == y and r.get("reader") == reader and r.get("kind") == "read"}
    ids = sorted(rx & ry)
    if not ids:
        raise SixError(f"no usable draft to compare: the reader {reader} read no scenario in both {x} and {y} (an arm whose answers all had no draft, or no scenario in common)")
    cx, cy = wrong_counts(rows, x, reader), wrong_counts(rows, y, reader)
    d = [10.0 * (cx.get(i, 0) - cy.get(i, 0)) for i in ids]
    m_, s_ = mean(d), se(d)
    out = {"mean_per10": m_, "se": s_, "n": len(d), "interval90": interval(m_, s_, Z90), "read_x": len(rx), "read_y": len(ry)}
    if words and x in words and y in words:
        per = lambda c, w: (1000.0 * sum(c.get(i, 0) for i in ids) / sum(w[i] for i in ids)) if sum(w[i] for i in ids) else None  # noqa: E731
        out["per1000_words"] = {"x": per(cx, words[x]), "y": per(cy, words[y]), "words_x": sum(words[x][i] for i in ids), "words_y": sum(words[y][i] for i in ids)}
    return out


def reading_for_compare(m, rows, reads, first, second):
    """The reading's parts of the comparisons: {model: {arm label: {"wrong": paired wrong statements, "harm_gemma": new harm of the Gemma answers against Qwen's, "harm_qwen": the other way}}}.
    An arm with no scenario read in both sets (no usable draft) is {"unavailable": message}."""
    words = {k: v.get("words_by_id") for k, v in (m.get("arms") or {}).items() if v.get("words_by_id")}
    out = {}
    for g, arms in (m.get("compare") or {}).items():
        out[g] = {}
        for label, e in arms.items():
            gx, qx = e["gemma_set"], e["qwen_set"]
            try:
                out[g][label] = {"wrong": wrong_paired(rows, reads, gx, qx, first, words), "harm_gemma": harm_match(rows, reads, gx, qx, first, second),
                                 "harm_qwen": harm_match(rows, reads, qx, gx, first, second)}
            except SixError as ex:
                if "no usable draft" not in str(ex):
                    raise
                out[g][label] = {"unavailable": str(ex)}
    return out


def clean_winner(e, rd_=None):
    """-> (verdict, {condition: True|False|None}); None waits for the reading or the second reader. yes only when every condition holds."""
    eps = 1e-9
    cov = e["coverage"]
    ci = cov.get("ci90")
    c = {"wrong_upper_below_0": None, "coverage": None if ci is None else bool(cov["mean"] >= CMP_COV_MIN - eps and ci[0] > CMP_COV_LOWER + eps), "harm_confirmed_0": None, "mechanical": None}
    if e.get("mech"):
        lows = [e["mech"][k]["interval90"] for k in ("all", "without_lookup")]
        c["mechanical"] = None if any(x is None for x in lows) else all(x[0] > CMP_MECH_LOWER + eps for x in lows)
    if rd_ and rd_.get("unavailable"):
        pass          # no usable draft to compare: the reading's conditions stay waiting
    elif rd_:
        w = rd_["wrong"]["interval90"]
        c["wrong_upper_below_0"] = None if w is None else bool(w[1] < -eps)
        h = rd_["harm_gemma"]
        c["harm_confirmed_0"] = False if h["confirmed"] else (None if h["pending"] else True)
    if any(v is False for v in c.values()):
        return "no", c
    return ("yes" if all(v is True for v in c.values()) else "waits"), c


def _fmt_iv(iv, k=3):
    return "n/a" if not iv else f"[{iv[0]:+.{k}f}, {iv[1]:+.{k}f}]"


def compare_lines(m, reading=None):
    """The model comparison as lines (W10). reading: reading_for_compare's result, or None (the lines then say what waits for it). Decides nothing."""
    L = ["Models against Qwen, on the same arm (W10: described, decides nothing; the Owner decides publishing after the tests):"]
    cmp_ = m.get("compare") or {}
    for g, f in sorted((m.get("failed_models") or {}).items()):
        L.append(f"  {g} against Qwen: not available: answer stage failed ({f.get('why')}); all rows and comparisons of {g} are not available")
    if not cmp_ and not m.get("failed_models"):
        L.append("  (no other model in this run)")
    for g, arms in cmp_.items():
        for label, e in arms.items():
            cov = e["coverage"]
            rdg = (reading or {}).get(g, {}).get(label)
            L.append(f"  {g} against Qwen, arm {label} ({e['gemma_set']} against {e['qwen_set']}):")
            if rdg and rdg.get("unavailable"):
                L.append(f"    wrong statements and harm: not available: {rdg['unavailable']}")
                rdg = None
            elif rdg:
                w = rdg["wrong"]
                L.append(f"    wrong statements per 10 answers (first reader, non-minor wrong, invented, harmful), paired over {w['n']} scenarios read in both sets ({e['gemma_set']}: {w.get('read_x', w['n'])} read, {e['qwen_set']}: {w.get('read_y', w['n'])} read): "
                         f"{w['mean_per10']:+.2f}, SE {f2(w['se'], 2)}, 90 % interval {_fmt_iv(w['interval90'], 2)}")
                if w.get("per1000_words"):
                    p_ = w["per1000_words"]
                    L.append(f"    wrong statements per 1,000 words over those scenarios: {e['gemma_set']} {f2(p_['x'], 2)} ({p_['words_x']} words), {e['qwen_set']} {f2(p_['y'], 2)} ({p_['words_y']} words)")
            else:
                L.append("    wrong statements per 10 answers, paired by scenario with a 90 % interval: NEEDS THE READING")
            L.append(f"    judged coverage, paired over {cov['n']} scenarios: {cov['mean']:+.3f}, SE {f2(cov['se'])}, 90 % interval {_fmt_iv(cov['ci90'])}")
            if rdg:
                for nm, h in (("Gemma's answers new against Qwen's", rdg["harm_gemma"]), ("Qwen's answers new against Gemma's", rdg["harm_qwen"])):
                    L.append(f"    new harmful consequences, {nm}: {len(h['new'])} (confirmed {len(h['confirmed'])}, not confirmed {len(h['not_confirmed'])}, waiting for the second reader {len(h['pending'])}, "
                             f"new and unconfirmed, second reader only {len(h.get('second_only') or [])}; harmful findings {h['candidate_findings']} against {h['reference_findings']})")
            else:
                L.append("    new harmful consequences, both ways (confirmed by the second reader): NEEDS THE READING")
            if e["mech"]:
                a_, w_ = e["mech"]["all"], e["mech"]["without_lookup"]
                gp, qp = e["mech"]["all_pass"]["gemma"], e["mech"]["all_pass"]["qwen"]
                L.append(f"    mechanical all-pass ({"the answers on the 116 items: " + e['mech_arm'] if " against " in e['mech_arm'] else "arm " + e['mech_arm'] + "'s answers on the 116 items"}): {g} {gp[0]} of {gp[1]}, Qwen {qp[0]} of {qp[1]}; paired difference {a_['mean_pts']:+.1f} points, 90 % interval {_fmt_iv(a_['interval90'], 1)}; "
                         f"without the lookup kinds ({w_['n']} items) {w_['mean_pts']:+.1f} points, {_fmt_iv(w_['interval90'], 1)}")
            else:
                L.append(f"    mechanical all-pass: not available (the model's answers on arm {e['mech_arm']} for the mechanical items did not run)")
            L.append(f"    cut answers {e['cut']['gemma']} against Qwen's {e['cut']['qwen']}; empty {e['empty']['gemma']} against {e['empty']['qwen']}; checks that missed the {DEADLINE_S:.0f} s start-by "
                     f"{e['not_checked']['gemma']} against {e['not_checked']['qwen']}; time per answer with search, median/90th percentile (reported) {e['time']['gemma']['median_s']:.1f}/{e['time']['gemma']['p90_s']:.1f} s against Qwen's "
                     f"{e['time']['qwen']['median_s']:.1f}/{e['time']['qwen']['p90_s']:.1f} s; cached prompt tokens per draft, median (answers reported) {f2((e.get('cached') or {}).get('gemma', [None, None])[0], 0)} ({(e.get('cached') or {}).get('gemma', [None, None])[1]}) "
                     f"against Qwen's {f2((e.get('cached') or {}).get('qwen', [None, None])[0], 0)} ({(e.get('cached') or {}).get('qwen', [None, None])[1]}): the server's prompt cache makes a later arm faster, so the times are not comparable across arms and models")
            v, c = clean_winner(e, rdg)
            wait = [k for k, x in c.items() if x is None]
            names = {"wrong_upper_below_0": "the upper end of the wrong statements below 0", "coverage": f"coverage at least {CMP_COV_MIN:+.2f} with its lower end above {CMP_COV_LOWER:+.2f}",
                     "harm_confirmed_0": "0 confirmed new harmful consequences", "mechanical": f"the mechanical lower end above {CMP_MECH_LOWER:+.0f} points"}
            tail = "" if v == "yes" else (f" (fails: {', '.join(names[k] for k, x in c.items() if x is False)})" if v == "no" and any(x is False for x in c.values()) else
                                          f" (waits for the reading: {', '.join(names[k] for k in wait)})")
            L.append(f"    clean winner against Qwen on arm {label}: {'yes' if v == 'yes' else 'no'}{tail}")
    for k, v in sorted((m.get("gemma_b_pairs") or {}).items()):
        x, y = k.split("-", 1)
        ax, ay = m["arms"][x], m["arms"][y]
        L.append(f"  B against B between two Gemma models ({x} against {y}; described): judged coverage, paired over {v['n']} scenarios: {v['mean']:+.3f}, SE {f2(v['se'])}, 90 % interval {_fmt_iv(v.get('ci90'))}; "
                 f"cut answers {ax['cut']} against {ay['cut']}; time per answer with search, median/90th percentile (reported) {ax['time']['median_s']:.1f}/{ax['time']['p90_s']:.1f} s against {ay['time']['median_s']:.1f}/{ay['time']['p90_s']:.1f} s; "
                 f"wrong statements and harm of the two sets: `harm-match` and the reading's findings by set name")
    for k, v in sorted((m.get("not_available") or {}).items()):
        L.append(f"  not available: {k}: {v}")
    return L


def arm_r_lines(m):
    """Arm R (grounded-g3 on the r4g2 packs, W7): description only; outside every decided row; not read."""
    v = (m.get("pairs") or {}).get("R-A")
    if v is None:
        na = (m.get("not_available") or {}).get("R") or (m.get("not_available") or {}).get("pair_R")
        return [f"Arm R (grounded-g3 on the r4g2 packs, W7): not available: {na}" if na else "Arm R (grounded-g3 on the r4g2 packs, W7): not in this run"]
    return [f"Arm R (grounded-g3 on the r4g2 packs, W7; judged only, outside every decided row, not read): R - A judged coverage {v['mean']:+.3f}, SE {f2(v['se'])}, 90 % interval {_fmt_iv(v.get('ci90'))} over {v['n']} scenarios"]


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


def is_described(name):
    """A set the rule only describes: arm R and every other model's sets (K4b: a failure there never stops Qwen's decided rows)."""
    return name == "R" or any(name.endswith("_" + k) for k in MODEL_KEYS)


def evaluate(m):
    """-> (lines, result) with result = {"kb": build|a_only|none_lookup_not_tested, "thorough_possible": [...], "conditions": {...}, "check_failures_above_limit": [...]}.
    The thorough row's wrong and harmful conditions wait for the reading: no final choice for that row."""
    variant, n = m.get("variant"), need(m, "sizes", "D5")
    if variant not in VARIANTS:
        raise SixError("input missing: variant")
    L, eps = [], 1e-9
    over = [(k, int(need(m, "arms", k, "answer_over_limit"))) for k in sorted(m.get("arms") or {}) if not is_described(k)]   # the length check of the decided arms: a measure that is missing is an error
    over = [(k, n_) for k, n_ in over if n_]
    if over:
        raise SixError("the answer limit of %d tokens is broken: %s (an arm's answers are longer than the others' may be; the arms are not comparable)" % (
            ANSWER_LIMIT, ", ".join(f"arm {k}: {n_} answers over" for k, n_ in over)))
    over_d = [(k, v.get("answer_over_limit")) for k, v in sorted((m.get("arms") or {}).items()) if is_described(k) and v.get("answer_over_limit")]
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
    if t is None:
        return _evaluate_no_thinking(m, L, kb, n, variant, over_d)
    cov, se_ = need(m, "pairs", f"{t}-A", "mean"), need(m, "pairs", f"{t}-A", "se")
    cut, tm = need(m, "arms", t, "cut"), need(m, "arms", t, "time", "median_s")
    p9 = need(m, "arms", t, "time", "p90_s")
    cond = {"coverage": cov >= D_COV_MIN - eps, "cut": cut * 100 <= D_CUT_PCT * n}
    nchk = ((m["arms"][t].get("check") or {}).get("status") or {}).get("not_checked", 0)
    L.append(f"Thorough-profile rows (arm {t} against A; {'thinking, then the checking pass' if t == 'D' else 'the checking pass alone on A drafts (W11)' if t == 'C' else 'thinking only, no checking pass in this variant'}):")
    L.append(f"  {'PASS' if cond['coverage'] else 'FAIL'}  coverage difference {cov:+.3f}, standard error {se_:.3f} (needs >= {D_COV_MIN:+.2f})")
    L.append(f"  {'PASS' if cond['cut'] else 'FAIL'}  cut at the token limit: {int(cut)} of {int(n)} = {100 * cut / n:.1f} % (needs <= {D_CUT_PCT} %)")
    L.append(f"  REPORTED  median seconds per answer with search {tm:.1f} (not decided: W5; the plan's bar was {PLAN_MED_S:.0f} s)")
    L.append(f"  REPORTED  90th percentile of the seconds with search {p9:.1f} (not decided: W5; the plan's bar was {PLAN_P90_S:.0f} s; the fifth largest of 50)")
    L.append(f"  REPORTED  answers whose checking call did not start by the operational deadline ({DEADLINE_S:.0f} s; status not_checked): {int(nchk)} of {int(n)}")
    L.append(f"  NEEDS THE READING  wrong statements per 10 answers, {t} minus A (needs <= {D_WRONG_MAX:+.1f}); the readers count them")
    L.append(f"  NEEDS THE READING  harmful consequences, {t} against A: confirmed new harmful consequences must be {D_HARM_NEW_MAX} (W6: a harmful finding is new when A's answer to the scenario has none with the same consequence; "
             "a new one is confirmed when the second reader also marks it harmful with the same consequence; `harm-match` prints the raw counts)")
    base = cond["coverage"] and cond["cut"]
    no_gain = "neither (if the readings do not show the errors improving)"
    where = "thinking, then the checking pass" if t == "D" else "the checking pass alone (W11)" if t == "C" else "thinking alone"
    if base:
        possible = [f"goes_on_to_B4 ({where}; if the readings give wrong statements at most {D_WRONG_MAX:+.1f} per 10 and 0 confirmed new harmful consequences)" + (
            "; under --check-variant none thinking alone MAY go on to B4 (the Owner's decision of 2026-11-23)" if t == "B" and NONE_THINKING_MAY_GO_TO_B4 else ""), no_gain]
        if t == "B" and not NONE_THINKING_MAY_GO_TO_B4:
            possible[0] = "waits for the Owner (whether thinking alone may go on to B4 is not decided)"
    else:
        possible = ["neither (a measured condition fails; the reading cannot change that). The row \"Errors improve but time fails\" cannot arise: time is reported, not decided (W5)"]
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
    L += _arm_notes(m, over_d)
    return L, {"kb": kb, "thorough_possible": possible, "conditions": cond, "check_failures_above_limit": above,
               "time_reported": {"median_s": tm, "p90_s": p9, "not_checked": int(nchk)}}


def _arm_notes(m, over_d):
    """Printed outcomes, not failures: an arm whose answers all had no draft ("no usable draft"), and a described set whose answers are over the limit."""
    L = []
    nd_ = [k for k, v in sorted((m.get("arms") or {}).items()) if v.get("n") and len(v.get("no_draft_ids") or []) == v["n"]]
    part = [(k, len(v["no_draft_ids"]), v["n"]) for k, v in sorted((m.get("arms") or {}).items()) if v.get("no_draft_ids") and k not in nd_]
    if nd_ or part:
        L.append("")
        L.append("Answers with no draft (cut while thinking), by arm (a printed outcome, not a failure; such an answer is empty, P 0, and is left out of its bundle, not of the others):")
        for k in nd_:
            L.append(f"  arm {k}: NO USABLE DRAFT (all {m['arms'][k]['n']} answers were cut while thinking)")
        for k, c, n_ in part:
            L.append(f"  arm {k}: {c} of {n_} answers have no draft")
    if over_d:
        L.append("")
        L += [f"  described set {k}: {n_} answers over the {ANSWER_LIMIT}-token limit: its comparisons are not clean (it stops nothing)" for k, n_ in over_d]
    return L


def _evaluate_no_thinking(m, L, kb, n, variant, over_d):
    """No thinking arm in the run (--thinking-models none, or without qwen): the knowledge-base rows are applied, the thorough-profile row is NOT RUN. C (the checking pass on A's drafts) is
    described as C - A when the variant has a checking pass."""
    L.append("Thorough-profile rows, Qwen: not tested (W11 and the pilot). Qwen has no thinking arm and no checking pass in this run (--check-variant none; Qwen's thinking arms are dropped), so the plan's table row for the thorough profile has nothing to apply to for Qwen. "
             "A thinking Gemma model gets its own row, printed below.")
    c = (m.get("pairs") or {}).get("C-A")
    if c:
        L.append(f"  description only, decides nothing: the checking pass alone (C, A's drafts checked), C - A judged coverage {c['mean']:+.3f}, standard error {f2(c['se'])} ({c['n']} scenarios); "
                 f"cut {m['arms']['C']['cut']} of {int(n)}")
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
    if not above and not any((m["arms"][k].get("check") or {}).get("status") for k in m.get("arms") or {}):
        L.append("  (no arm with a checking pass in this run)")
    L += _arm_notes(m, over_d)
    return L, {"kb": kb, "thorough_possible": ["not run (no thinking arm in this run)"], "conditions": {}, "check_failures_above_limit": above, "time_reported": None}


def thorough_row(cov, cut, n, wrong=None, harm=None):
    """The plan's thorough-profile row as amended (W5, W6), for one thorough arm against its own A: coverage at least D_COV_MIN, cut at most D_CUT_PCT % (integer arithmetic), wrong statements per 10 at
    most D_WRONG_MAX (from the reading; None waits), 0 confirmed new harmful consequences (harm_match's dict, or None: waits; pending ones wait). Time decides nothing (W5).
    -> ({condition: True|False|None}, "pass"|"fail"|"waits")."""
    eps = 1e-9
    c = {"coverage": cov >= D_COV_MIN - eps, "cut": cut * 100 <= D_CUT_PCT * n,
         "wrong": None if wrong is None else wrong <= D_WRONG_MAX + eps,
         "harm": None if harm is None else (False if harm["confirmed"] else None if harm["pending"] else True)}
    return c, ("fail" if any(v is False for v in c.values()) else "waits" if any(v is None for v in c.values()) else "pass")


def gemma_thorough(m, reading=None):
    """The thorough-profile row PER thinking Gemma model: its thorough arm (B, thinking only, under `none`; D otherwise) against its own A. W4: thinking alone may be eligible for B4; a passing arm is eligible (the Owner decides, W10) and B4 would then take THAT
    model's B. reading: None, or (rows, reads, first_reader, second_reader) of the blind reading's unblinded findings (the wrong statements and the harm are then applied). -> (lines, {model: {"outcome": pass|fail|waits|na, ...}})."""
    L, res = [], {}
    n = need(m, "sizes", "D5")
    tl = thorough_label(m.get("variant"))
    for g, f in sorted((m.get("failed_models") or {}).items()):
        if f.get("thinking"):
            L.append(f"Thorough-profile row, {g} (its B against its own A): NOT AVAILABLE: answer stage failed ({f.get('why')}); this is not a pass")
            L.append(f"Thorough outcome, {g}: NOT AVAILABLE: answer stage failed; {g}'s B is not eligible for B4 on this evidence")
            L.append("")
            res[g] = {"outcome": "na", "conditions": {}, "failed": True}
    for g, e in sorted((m.get("thorough_models") or {}).items()):
        x, y = e["set"], e["ref"]
        L.append(f"Thorough-profile row, {g} ({x} against {y}; {'thinking only, no checking pass' if tl == 'B' else 'thinking, then the checking pass'}; the plan's row as amended, applied per model):")
        arm_x, pr = (m.get("arms") or {}).get(x), (m.get("pairs") or {}).get(f"{x}-{y}")
        if not arm_x or not pr or not all(num(v) for v in (pr.get("mean"), arm_x.get("cut"), (arm_x.get("time") or {}).get("median_s"), (arm_x.get("time") or {}).get("p90_s"))):
            why = (m.get("not_available") or {}).get(f"thorough_{g}") or (m.get("not_available") or {}).get(x) or "a measure of the row is missing"
            L.append(f"  NOT AVAILABLE  {why}; this is not a pass")
            L.append(f"Thorough outcome, {g}: NOT AVAILABLE ({why}); {g}'s B is not eligible for B4 on this evidence")
            res[g] = {"outcome": "na", "conditions": {}}
            continue
        ov = [(k, ((m.get("arms") or {}).get(k) or {}).get("answer_over_limit")) for k in (x, y)]
        if any(not num(v) or v > 0 for _, v in ov):
            why = "answer over the limit (" + ", ".join(f"{k}: {'missing' if not num(v) else int(v)}" for k, v in ov if not num(v) or v > 0) + f" answers; the limit is {ANSWER_LIMIT} tokens in both arms)"
            L.append(f"  NOT AVAILABLE  {why}; this is not a pass")
            L.append(f"Thorough outcome, {g}: NOT AVAILABLE ({why}); {g}'s B is not eligible for B4 on this evidence")
            L.append("")
            res[g] = {"outcome": "na", "conditions": {}}
            continue
        cov, cut = pr["mean"], arm_x["cut"]
        wrong = harm = None
        w1000 = None
        why_na = None
        if reading:
            rows, reads, first, second = reading
            try:
                wp = wrong_paired(rows, reads, x, y, first, {k: v.get("words_by_id") for k, v in (m.get("arms") or {}).items() if v.get("words_by_id")})
                wrong, w1000 = wp["mean_per10"], wp.get("per1000_words")
                harm = harm_match(rows, reads, x, y, first, second)
            except SixError as ex:
                why_na = str(ex)
        c, out = thorough_row(cov, cut, n, wrong, harm)
        tag = lambda k: "WAITS" if c[k] is None else "PASS" if c[k] else "FAIL"  # noqa: E731
        L.append(f"  {tag('coverage')}  coverage difference {cov:+.3f}, standard error {f2(pr.get('se'))} (needs >= {D_COV_MIN:+.2f})")
        L.append(f"  {tag('cut')}  cut at the token limit: {int(cut)} of {int(n)} = {100 * cut / n:.1f} % (needs <= {D_CUT_PCT} %)")
        L.append(f"  REPORTED  time per answer with search, median {arm_x['time']['median_s']:.1f} s, 90th percentile {arm_x['time']['p90_s']:.1f} s (not decided: W5)")
        if wrong is None:
            L.append(f"  NEEDS THE READING  wrong statements per 10 answers, {x} minus {y} (needs <= {D_WRONG_MAX:+.1f})" + (f": not available: {why_na}" if why_na else ""))
        else:
            L.append(f"  {tag('wrong')}  wrong statements per 10 answers, {x} minus {y}: {wrong:+.2f} (needs <= {D_WRONG_MAX:+.1f})")
            if w1000:
                L.append(f"  REPORTED  wrong statements per 1,000 words over those scenarios: {x} {f2(w1000['x'], 2)} ({w1000['words_x']} words), {y} {f2(w1000['y'], 2)} ({w1000['words_y']} words)")
        if harm is None:
            L.append(f"  NEEDS THE READING  confirmed new harmful consequences, {x} against {y}, by W6 (must be {D_HARM_NEW_MAX})" + (f": not available: {why_na}" if why_na else ""))
        else:
            L.append(f"  {'WAITS (second reader)' if c['harm'] is None else tag('harm')}  confirmed new harmful consequences {len(harm['confirmed'])} (waiting for the second reader {len(harm['pending'])}; must be {D_HARM_NEW_MAX}; W6)")
        failing = [k for k, v in c.items() if v is False]
        b4 = f"{g}'s B is {ELIGIBLE_B4}"
        if out == "fail":
            L.append(f"Thorough outcome, {g}: neither ({', '.join(failing)} fails); {g}'s B is not eligible for B4")
        elif out == "waits":
            L.append(f"Thorough outcome, {g}: waits for the reading; if the readings give wrong statements at most {D_WRONG_MAX:+.1f} per 10 and {D_HARM_NEW_MAX} confirmed new harmful consequences, {b4}; otherwise neither")
        else:
            L.append(f"Thorough outcome, {g}: PASS; {b4}")
        L.append("")
        res[g] = {"outcome": out, "conditions": c, "failing": failing}
    return L, res


def thorough_final(qwen_word, gres):
    """The thorough-profile part of the last line. Without a thinking Gemma model: Qwen's word alone (as before). With one: Qwen's word and one outcome per model."""
    if not gres:
        return qwen_word
    word = {"pass": ELIGIBLE_B4_WORD, "waits": "WAITS FOR THE READING", "na": "NOT AVAILABLE"}
    parts = [f"qwen {qwen_word}"] + [f"{g} " + ("NOT AVAILABLE: answer stage failed" if r.get("failed") else f"FAILS ({', '.join(r['failing'])})" if r["outcome"] == "fail" else word[r["outcome"]]) for g, r in sorted(gres.items())]
    return "; ".join(parts)


def cmd_rule(a):
    try:
        m = json.load(open(a.measures, encoding="utf-8"))
        L, res = evaluate(m)
        GL, gres = gemma_thorough(m)
        L += [""] + GL + arm_r_lines(m) + [""] + compare_lines(m)
    except (OSError, ValueError, SixError) as e:
        print(f"ERROR: {e}; a missing input is not a pass; no verdict", file=sys.stderr)
        return 2
    print("\n".join(L))
    print(f"M7 SIX-ARM TEST DONE: knowledge-base row: {res['kb']}; thorough-profile row: {thorough_final('NOT TESTED (W11 and the pilot)' if res['time_reported'] is None else 'WAITS FOR THE READING', gres)}"
          + (f"; check failures above limit: {', '.join(res['check_failures_above_limit'])}" if res["check_failures_above_limit"] else ""))
    return 0


# ---------------------------------------------------------------- reading prep, call counts
def session_size(n_answers, per_session=0):
    """Scenarios per reading session: the given number, or as many as hold about READ_ANSWERS_PER_SESSION answers (4 scenarios of 4 answers; 2 of 8 answers)."""
    return int(per_session) if per_session else max(1, round(READ_ANSWERS_PER_SESSION / max(1, n_answers)))


def cmd_reading_plan(a):
    key = json.load(open(a.key, encoding="utf-8"))
    ids = sorted(key.get("items") or [])
    second = sorted(key.get("second_reader_ids") or [])
    n_ans = (round(len(key["aid"]) / len(ids)) if key.get("aid") and ids else 0) or len(key.get("sets") or []) or 4
    per = session_size(n_ans, a.scenarios_per_session)
    first_n = max(1, math.ceil(len(ids) / per)) if ids else 0
    first = [ids[i::first_n] for i in range(first_n)] if first_n else []
    sec_n = max(1, math.ceil(len(second) / per)) if second else 0
    sec = [second[i::sec_n] for i in range(sec_n)] if sec_n else []
    plan = {"seed": a.seed, "scenarios_per_session": per, "answers_per_scenario": n_ans, "first_reader_sessions": first, "second_reader_sessions": sec}
    Path(a.out).write_text(json.dumps(plan, indent=1) + "\n", encoding="utf-8")
    print(f"reading plan: {len(ids)} scenarios of {n_ans} answers each in {len(first)} first-reader sessions (up to {per} scenarios each), {len(second)} scenarios in {len(sec)} second-reader sessions (nothing is read here)")
    return 0


def expected_calls(nd, variant, nma, nm3, models=MODEL_KEYS, thinking=DEFAULT_THINKING, thinking_mech="yes"):
    """Model calls per stage (answer rows are the unit: a draft; a check is one more call where a draft exists). Qwen's stages keep their names; another model's carry its key
    (ans_a_gemma26 ...). Every model has A (D5 and the mechanical items) and E; a model that thinks also D (D5 and the mechanical items), F and, with a check, C. R (Qwen's comparison arm) is one draft per scenario."""
    mech = nma + nm3
    chk = variant != "none"
    plan = plan_models(",".join(models), ",".join(thinking) or "none", None, True, thinking_mech)
    st = {}
    for m in plan:
        s = "" if m["key"] == "qwen" else "_" + m["key"]
        st[f"ans_a{s}"], st[f"ans_e{s}"] = nd + mech, nd
        if m["thinking"]:
            st[f"ans_d{s}"] = (nd + (mech if m["think_mech"] else 0)) * (2 if chk else 1)      # --thinking-mech no: a thinking Gemma model skips the mechanical items
            if m["key"] == "qwen" or chk:                                                       # a thinking Gemma model under none has B only: no perfect-pack thinking arm
                st[f"ans_f{s}"] = nd * (2 if chk else 1)
        if (m["thinking"] and (chk or m["key"] == "qwen")) or (m["key"] == "qwen" and chk):    # C: the checking pass on A's drafts; Qwen has it with or without thinking (K4b)
            st[f"ans_c{s}"] = nd if chk else 0
        if m["key"] == "qwen":
            st["ans_r"] = nd
    st["judge"] = len(all_set_stems(variant, plan)) * nd * 2
    st["total_answer_calls"] = sum(v for k, v in st.items() if k.startswith("ans_"))
    return st


def cmd_call_counts(a):
    exp = expected_calls(a.nd, a.check_variant, a.nma, a.nm3, parse_key_list(a.models, "--models"), parse_key_list(a.thinking_models, "--thinking-models", allow_none=True), a.thinking_mech)
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
THINK_MARKS = ("<think>", "</think>", "<|channel>thought", "<|channel|>analysis", "<start_of_thought>")   # the thinking marks: also a leak in a bundle
LEAK_TERMS = ("answer_profile", "--answer-profile", "enable_thinking", "think-cap", "check-variant", "no-verify", "thorough profile", "EXPERIMENTAL", "cut off by the token limit while thinking") + THINK_MARKS
LEAK_MODEL_TERMS = ("qwen", "gemma")   # the base models' names (any case); the chain adds the ids it was given with --terms


def cmd_leak_check(a):
    """No file of a reading bundle names a configuration (a switch, the profile, the notice of a draft cut while thinking, a base model's name or id)."""
    bad = []
    for f in sorted(Path(a.dir).rglob("*.md")):
        t = f.read_text(encoding="utf-8")
        low = t.lower()
        hit = [x for x in LEAK_TERMS if x in t] + [x for x in LEAK_MODEL_TERMS + tuple(a.terms or ()) if x.lower() in low]
        if hit:
            bad.append((str(f), hit))
    for f, hit in bad:
        print(f"a bundle file names a configuration: {f}: {', '.join(hit)}")
    print(f"leak check {a.dir}: {len(bad)} files name a configuration")
    return 1 if bad else 0


def is_usable(r):
    return bool(r) and not is_no_draft(r) and not r.get("error") and bool((r.get("text") or "").strip())


def cmd_no_draft_ids(a):
    """The answers with no draft (cut while thinking), per set. Printed: the scenarios where any named set has none, one per line (as before). With --key the key records them
    ("no_draft": per set, "left_out_of_the_bundles": those scenarios). K4b: with --sets-by-item FILE the usable sets of every scenario are written as {id: [sets]}: the exclusion is per
    answer, not per scenario, so a bundle holds the answers that exist (under random letters); a scenario with fewer than two usable answers is listed apart (it cannot be compared)."""
    sets = dict(x.split("=", 1) for x in a.answers)
    rows = {name: {r["id"]: r for r in rd(f)} for name, f in sets.items()}
    per = {name: sorted(i for i, r in rs.items() if is_no_draft(r)) for name, rs in rows.items()}
    ids = sorted({i for v in per.values() for i in v})
    all_ids = sorted({i for rs in rows.values() for i in rs})
    sbi = {i: [n for n in sets if is_usable(rows[n].get(i))] for i in all_ids}
    few = sorted(i for i, v in sbi.items() if len(v) < 2)
    if a.key:
        key = json.load(open(a.key, encoding="utf-8"))
        key["no_draft"] = per
        key["left_out_of_the_bundles"] = ids
        key["answers_left_out"] = {i: [n for n in sets if n not in sbi[i]] for i in all_ids if len(sbi[i]) < len(sets)}
        Path(a.key).write_text(json.dumps(key, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    if a.sets_by_item:
        Path(a.sets_by_item).write_text(json.dumps(sbi, indent=1) + "\n", encoding="utf-8")
    print("\n".join(ids))
    print(f"no-draft: {len(ids)} scenarios have an answer with no draft ({', '.join(f'{k}: {len(v)}' for k, v in per.items())}); only those answers are left out of the bundles"
          + (f"; {len(few)} scenarios with fewer than two usable answers are not read" if few else ""), file=sys.stderr)
    for n in sets:
        k = sum(1 for i in all_ids if n in sbi[i])
        print(f"read set {n}: {k} of {len(all_ids)} scenarios have a usable draft" + ("  => NO USABLE DRAFT" if k == 0 else ""), file=sys.stderr)
    return 0


def cmd_reading_summary(a):
    """For each read set, how many of its scenarios are in the bundles (from the key). A set with none is printed as "no usable draft": an outcome, not a failure."""
    key = json.load(open(a.key, encoding="utf-8"))
    items = list(key.get("items") or [])
    total = len(items) + len(key.get("left_out_items") or [])
    sec = set(key.get("second_reader_ids") or [])
    cnt = {n: 0 for n in key.get("sets") or []}
    cnt2 = dict(cnt)
    for name, n in (key.get("aid") or {}).items():
        cnt[n] = cnt.get(n, 0) + 1
        if name.rsplit("-", 1)[0] in sec:
            cnt2[n] = cnt2.get(n, 0) + 1
    print(f"reading: {len(items)} of {total} scenarios are in the bundles (a scenario needs two usable answers)")
    for n in key.get("sets") or []:
        print(f"  set {n}: {cnt[n]} of {total} scenarios read by the first reader, {cnt2[n]} by the second reader" + ("  => NO USABLE DRAFT: nothing of this set is read" if cnt[n] == 0 else ""))
    return 0


def cmd_judge_sets(a):
    """The judge's rows per set (K4b). A decided set (Qwen's) must have a valid row for every scenario and pass; a described set (a Gemma model's, R) may have missing or invalid rows:
    it is listed and left out of the comparisons. Exit 1 when a decided set is incomplete. Prints one line per set."""
    rows = rd(a.file)
    bad_dec = 0
    for name in a.decided + a.described:
        got = {(r["id"], r.get("pass")) for r in rows if r.get("set") == name and r.get("valid_reply")}
        ids = {r["id"] for r in rows if r.get("set") == name}
        want = a.n * 2
        ok = len(got) >= want and len(ids) >= a.n
        dec = name in a.decided
        print(f"judge set {name}: {len(got)} valid rows of {want} ({'decided' if dec else 'described'})" + ("" if ok else ("  => INCOMPLETE" if dec else "  => INCOMPLETE: left out of the comparisons (stops nothing)")))
        bad_dec += (not ok) and dec
    return 1 if bad_dec else 0


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
        if a.across_models:      # K4b: two models answer the same call: the body is equal except for the model's name
            ha, hb_ = pa.get("rest_nomodel_sha256"), pb.get("rest_nomodel_sha256")
            if not ha or not hb_:
                why.append("no model-free body hash recorded (the rows were made before it existed)")
            elif ha != hb_:
                why.append("the rest of the body without the model (temperature, stream flags, other extra-body keys) differs")
        elif pa["rest_sha256"] != pb["rest_sha256"]:
            why.append("the rest of the body (model, temperature, stream flags, other extra-body keys) differs")
        same_pack = A[i].get("pack") == B[i].get("pack")
        if a.messages_differ:      # arm R against A: another prompt (grounded-g3), whatever the pack; everything else as A
            if pa["messages_sha256"] == pb["messages_sha256"]:
                why.append("the messages are equal: the grounded prompt was not sent")
        elif (pa["messages_sha256"] == pb["messages_sha256"]) != same_pack:
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
          f"(allowed to differ: {'the model name only' if a.across_models else ''}{'the thinking switch off/on and the token limit plus the cap, ' if a.thinking_differs else ''}{'the messages (another prompt)' if a.messages_differ else '' if a.across_models else 'the messages where the packs differ'})")
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


def probe_classify(resp):
    """What the reply of a probe request shows -> (kind, how). kind: "error" (an error reply, or none shaped like a chat completion), "empty" (no content), "thinking" (reasoning text in a
    `reasoning_content` or `reasoning` field, or a `<think>...</think>` pair in the content: the two forms the stream reader of the profile handles), "unknown format" (thinking in some
    other form: another mark in the content, or reasoning tokens in the usage only: the stream reader cannot cut such an answer or strip its thought), "none". K4b: a reply counts only with
    a non-empty content."""
    if not isinstance(resp, dict) or resp.get("error") or not isinstance(resp.get("choices"), list) or not resp["choices"]:
        return "error", "an error reply or no choices"
    msg = (resp["choices"][0] or {}).get("message") or {}
    content = str(msg.get("content") or "")
    if not content.strip():
        return "empty", "the content is empty"
    for k in ("reasoning_content", "reasoning"):
        if str(msg.get(k) or "").strip():
            return "thinking", f"{k} field"
    if "<think>" in content and "</think>" in content:
        return "thinking", "<think> ... </think> in the content"
    for mk in THINK_MARKS:
        if mk in content:
            return "unknown format", f"mark {mk} in the content"
    det = (resp.get("usage") or {}).get("completion_tokens_details") or {}
    if num(det.get("reasoning_tokens")) and det["reasoning_tokens"] > 0:
        return "unknown format", "reasoning tokens in the usage, but no reasoning field or <think> pair"
    return "none", "none"


def probe_observed(resp):
    """Did the reply of a probe request show thinking in a form the stream reader handles: (True, how) only for kind "thinking"."""
    kind, how = probe_classify(resp)
    return kind == "thinking", how if kind == "thinking" else f"{kind}: {how}"


def cmd_thinking_probe(a):
    """The probe of the thinking switch of a base model (M4's bake-off probed the same way): with the switch on the reply shows thinking (a reasoning field or a <think> pair) with a non-empty
    content, with it off it shows none. An error reply, an empty content or another thinking format fails the probe (exit 1; the message says which)."""
    try:
        resp = json.load(open(a.file, encoding="utf-8"))
        kind, how = probe_classify(resp)
    except (OSError, ValueError, AttributeError, TypeError, IndexError) as e:
        print(f"REFUSED: the probe reply {a.file} is unreadable ({type(e).__name__})", file=sys.stderr)
        return 2
    want = "thinking" if a.expect == "on" else "none"
    print(f"thinking probe {a.file}: switch {a.expect}: {kind} ({how})" + ("" if kind == want else f"; expected {want}"))
    if kind == "unknown format" and a.expect == "on":
        print("this model's thinking is in a format the chain cannot read: it gets no thinking arm (remove it from --thinking-models)", file=sys.stderr)
    return 0 if kind == want else 1


def cmd_reading_sets(a):
    """The sets of the blind bundles, NAME=PATH per line: Qwen's (A, D, E, F; under none A, B, E, F'), each other model's A and E, and its thorough arm when it ran (D; under none B)."""
    variant, _ = check_params(a.check_variant, 1)
    work = Path(a.work)
    out = []
    for mm in plan_from_args(a, dry=True)[::-1]:       # qwen first
        k = mm["key"]
        names = (list(READ_SETS[variant]) if mm["thinking"] else (["A", "C", "E"] if variant != "none" else ["A", "E"])) if k == "qwen" else list(READ_GEMMA_SETS[variant]) + ([("D" if variant != "none" else "B")] if mm["thinking"] else [])
        stems = model_set_stems(variant, k, mm["thinking"])
        out += [f"{set_name(n, k)}={work / ('ans_d5_' + stems[set_name(n, k)] + '.jsonl')}" for n in names]
    print("\n".join(out))
    return 0


# per-answer seconds (low, high) at Qwen's rates of the development check and the plan's budget; a model's scale against Qwen is inferred from M4's load test (answer median on 3 questions:
# Gemma 4 26B-A4B 8.0 s, Gemma 4 31B 35 s, the reference 35 s), loads 0.3 to 0.5 h each (inferred, not measured on this build)
MODEL_SCALE = {"qwen": (1.0, 1.0), "gemma26": (0.25, 0.5), "gemma31": (1.0, 1.3)}


def estimate(nd, cap, variant, plan, mech=116):
    """-> {stage: (low h, high h)}, total (low, high): the inferred server hours per stage (the draft 25 to 75 s per D5 answer, 10 to 13 s per mechanical answer, thinking cap/64 to cap/32 s, a check 20 to 33 s,
    a judge call 10 to 12 s, a load 0.3 to 0.5 h), scaled per model. An inference from other runs' rates, not a measurement."""
    chk = 1 if variant != "none" else 0
    plo, phi, mlo, mhi, tlo, thi, clo, chi = 25, 75, 10, 13, cap / 64, cap / 32, 20 * chk, 33 * chk
    st = {}
    for mm in plan:
        k, (sl, sh) = mm["key"], MODEL_SCALE[mm["key"]]
        sfx = "" if k == "qwen" else "_" + k
        st[f"ans_a{sfx}"] = ((nd * plo + mech * mlo) * sl, (nd * phi + mech * mhi) * sh)
        st[f"ans_e{sfx}"] = (nd * plo * sl, nd * phi * sh)
        gb = mm["thinking"] and k != "qwen" and not chk and k in THINK_SECONDS
        if gb:      # a thinking Gemma model under none: B only, at the measured seconds per thinking answer (median to 90th percentile), on the mechanical items too unless --thinking-mech no
            md, p9 = THINK_SECONDS[k]
            st[f"ans_d{sfx}"] = (nd * md + (mech * md if mm["think_mech"] else 0), nd * p9 + (mech * p9 if mm["think_mech"] else 0))
        elif mm["thinking"]:
            mc = mech if mm["think_mech"] else 0
            st[f"ans_d{sfx}"] = ((nd * (plo + tlo + clo) + mc * (mlo + tlo + clo)) * sl, (nd * (phi + thi + chi) + mc * (mhi + thi + chi)) * sh)
            st[f"ans_f{sfx}"] = (nd * (plo + tlo + clo) * sl, nd * (phi + thi + chi) * sh)
        if chk and (mm["thinking"] or k == "qwen"):
            st[f"ans_c{sfx}"] = (nd * clo * sl, nd * chi * sh)
        if k == "qwen":
            st["ans_r"] = (nd * plo, nd * phi)
    nsets = len(all_set_stems(variant, plan))
    st["judge"] = (nsets * nd * 2 * 10, nsets * nd * 2 * 12)
    st["loads"] = ((len(plan) + 2) * 1080, (len(plan) + 2) * 1800)
    h = {k: (v[0] / 3600, v[1] / 3600) for k, v in st.items()}
    return h, (sum(v[0] for v in h.values()), sum(v[1] for v in h.values()))


def cmd_estimate(a):
    plan = plan_from_args(a, dry=True)
    h, (lo, hi) = estimate(a.nd, a.think_cap, a.check_variant, plan)
    if a.detail:
        for k, (x, y) in h.items():
            print(f"{k}: {x:.2f} to {y:.2f} h")
    print(f"{lo:.1f} to {hi:.1f}")
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


def _epoch(v):
    """A load time as seconds since the epoch: a number, or an ISO 8601 string; None when it cannot be read."""
    if num(v):
        return float(v)
    try:
        import datetime
        return datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError):
        return None


def cmd_server_gate(a):
    """The server before the chain uses it (the Owner's decision of 2026-11-23: the unload go of M6 was not extended to M7). Refuses (exit 2) when a request is
    active or waiting, or when a model that is not ours is loaded; the message names it and nothing is unloaded. K4b (W2): a model is ours only with a marker this chain wrote when it
    loaded it (--marker ID=PATH, the file's time is the time of the load; --ours ID names a model that needs none, for the tests). A loaded model without a marker is another client's: refused.
    Resume edge case: a stale marker meets a model another client loaded later. When the status reports load times (`model_load_times`: {model: epoch seconds or ISO time}) a model loaded
    after its marker was written is foreign: refused. When it reports none, the gate says the marker's age cannot be checked and trusts the marker. A model of ours that is not --keep is printed as UNLOAD <name>."""
    try:
        st = json.load(open(a.status, encoding="utf-8"))
        busy = int(st.get("active_requests", 1)) + int(st.get("waiting_requests", 0))
        loaded = list(st.get("loaded_models") or [])
    except (OSError, ValueError, TypeError):
        print("REFUSED: the server status is unreadable", file=sys.stderr)
        return 2
    times = st.get("model_load_times") or {}
    ours = set(a.ours or [])
    unchecked = []
    for spec in a.marker or []:
        mid, _, path = spec.partition("=")
        if not mid or not path:
            print(f"REFUSED: --marker {spec!r}: expected ID=PATH", file=sys.stderr)
            return 2
        if not os.path.exists(path):
            continue
        if mid in loaded:
            lt = _epoch(times.get(mid)) if isinstance(times, dict) and mid in times else None
            if lt is None:
                unchecked.append(mid)
            elif lt > os.path.getmtime(path) + 1:
                print(f"REFUSED: the model {mid} was loaded at {lt:.0f}, after this chain's marker {path} was written: another client loaded it; this chain never unloads a model it did not load; nothing was started", file=sys.stderr)
                return 2
        ours.add(mid)
    foreign = [m for m in loaded if m not in ours]
    if foreign:
        print(f"REFUSED: the model {', '.join(foreign)} is loaded by another client (this chain has no marker for it); this chain never unloads a model it did not load (wait for its owner); nothing was started", file=sys.stderr)
        return 2
    if busy:
        print(f"REFUSED: the server has {busy} active or waiting requests (another client); wait until it is idle; nothing was started", file=sys.stderr)
        return 2
    for m in unchecked:
        print(f"note: the age of the marker of {m} cannot be checked against the server's load time (the server reports none); the marker is trusted", file=sys.stderr)
    for m in loaded:
        if m != a.keep:
            print(f"UNLOAD {m}")
    return 0


def add_model_args(q):
    q.add_argument("--models", default=",".join(MODEL_KEYS), help="comma list from qwen,gemma26,gemma31 (all three by default; qwen is required)")
    q.add_argument("--thinking-models", default="none", help="the models whose thinking arms (B, D, F, F' and, for a Gemma model, C) run: a comma list, or none (the default; the real run passes it explicitly)")
    q.add_argument("--thinking-mech", default="yes", help="yes|no: whether a thinking Gemma model's thinking arm also answers the 116 mechanical items (default yes; Qwen's thinking arms always do)")
    q.add_argument("--qwen-model", default=None)
    q.add_argument("--gemma26-model", default=None)
    q.add_argument("--gemma31-model", default=None)


def _read_findings(a):
    rows = rd(a.findings)
    reads = rd(a.reads) if getattr(a, "reads", None) else []
    return rows, reads


def cmd_harm_match(a):
    """W6 on two sets of the reading: the raw counts and the criterion (confirmed new harmful consequences = 0). --findings is the merged file of eval2_blind unblind, --reads its --reads-out."""
    rows, reads = _read_findings(a)
    h = harm_match(rows, reads, a.candidate, a.reference, a.first_reader, a.second_reader)
    print("\n".join(harm_match_lines(h)))
    return 0


def cmd_compare_models(a):
    """The model comparison (W10) with the measured parts; with --findings the reading's parts too. Decides nothing."""
    m = json.load(open(a.measures, encoding="utf-8"))
    reading = None
    if a.findings:
        if not (a.first_reader and a.reads):
            raise SixError("--findings needs --first-reader and --reads (and --second-reader for the harm)")
        rows, reads = _read_findings(a)
        reading = reading_for_compare(m, rows, reads, a.first_reader, a.second_reader)
    print("\n".join(compare_lines(m, reading)))
    return 0


def cmd_thorough_rows(a):
    """The thorough-profile row of each thinking Gemma model with the blind reading's findings (the wrong statements and the harm by consequence). Prints one outcome line per model and a last line."""
    m = json.load(open(a.measures, encoding="utf-8"))
    if not (a.first_reader and a.reads):
        raise SixError("--findings needs --first-reader and --reads (and --second-reader for the harm)")
    rows, reads = _read_findings(a)
    L, gres = gemma_thorough(m, (rows, reads, a.first_reader, a.second_reader))
    print("\n".join(L))
    print("M7 THOROUGH ROWS: " + thorough_final("not tested (W11 and the pilot)" if m.get("thorough_arm") is None else "see the rule's output", gres))
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
    q.add_argument("--key", default=None, help="the funnel's pack setting (default frozen; r4g2 for arm R)")
    q = sub.add_parser("gate-models")
    add_model_args(q)
    q.add_argument("--dry", action="store_true")
    q = sub.add_parser("model-order")
    add_model_args(q)
    q = sub.add_parser("model-ids")
    add_model_args(q)
    q = sub.add_parser("sets")
    add_model_args(q)
    q.add_argument("--check-variant", required=True)
    q = sub.add_parser("reading-sets")
    add_model_args(q)
    q.add_argument("--check-variant", required=True)
    q.add_argument("--work", required=True)
    q = sub.add_parser("estimate")
    add_model_args(q)
    q.add_argument("--check-variant", required=True)
    q.add_argument("--nd", type=int, required=True)
    q.add_argument("--think-cap", type=int, required=True)
    q.add_argument("--detail", action="store_true")
    q = sub.add_parser("thinking-probe")
    q.add_argument("--file", required=True)
    q.add_argument("--expect", choices=("on", "off"), required=True)
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
    q = sub.add_parser("model-failed")
    q.add_argument("--work", required=True)
    q.add_argument("--model", required=True)
    q.add_argument("--thinking", choices=("yes", "no"), default="no")
    q.add_argument("--why", required=True)
    q = sub.add_parser("check-rows")
    q.add_argument("--file", required=True)
    q.add_argument("--expect", type=int, required=True)
    q.add_argument("--real", action="store_true")
    q.add_argument("--no-thinking", action="store_true")
    q = sub.add_parser("prompts-relation")
    q.add_argument("--a", required=True)
    q.add_argument("--b", required=True)
    q.add_argument("--expect-same-pack", action="store_true")
    q = sub.add_parser("leak-check")
    q.add_argument("--dir", required=True)
    q.add_argument("--terms", nargs="*", default=[], help="more names no bundle may contain (the model ids of the run)")
    q = sub.add_parser("no-draft-ids")
    q.add_argument("--answers", nargs="+", required=True, metavar="NAME=FILE")
    q.add_argument("--key", default=None)
    q.add_argument("--sets-by-item", default=None, help="write {scenario id: [sets with a usable draft]} (eval2_blind.py bundle --sets-by-item)")
    q = sub.add_parser("reading-summary")
    q.add_argument("--key", required=True)
    q = sub.add_parser("judge-sets")
    q.add_argument("--file", required=True)
    q.add_argument("--n", type=int, required=True, help="scenarios per set")
    q.add_argument("--decided", nargs="*", default=[])
    q.add_argument("--described", nargs="*", default=[])
    q = sub.add_parser("bodies-relation")
    q.add_argument("--a", required=True)
    q.add_argument("--b", required=True)
    q.add_argument("--thinking-differs", action="store_true")
    q.add_argument("--messages-differ", action="store_true", help="the messages differ in every row, whatever the pack (arm R against A); the rest of the body is equal")
    q.add_argument("--across-models", action="store_true", help="the two sets are two models' answers to the same call: everything but the model's name is equal (K4b)")
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
    q.add_argument("--ours", nargs="*", default=[], help="models that count as ours without a marker (tests)")
    q.add_argument("--marker", nargs="*", default=[], metavar="ID=PATH", help="a model is ours when this file exists (written when the chain loaded it); its time is compared with the server's load time")
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
    add_model_args(q)
    q = sub.add_parser("rule")
    q.add_argument("--measures", required=True)
    q = sub.add_parser("harm-match")
    q.add_argument("--findings", required=True)
    q.add_argument("--reads", default=None)
    q.add_argument("--candidate", required=True, help="the set name of the candidate's answers (as in the findings file)")
    q.add_argument("--reference", required=True)
    q.add_argument("--first-reader", required=True)
    q.add_argument("--second-reader", default=None)
    q = sub.add_parser("compare-models")
    q.add_argument("--measures", required=True)
    q.add_argument("--findings", default=None)
    q.add_argument("--reads", default=None)
    q.add_argument("--first-reader", default=None)
    q.add_argument("--second-reader", default=None)
    q = sub.add_parser("thorough-rows")
    q.add_argument("--measures", required=True)
    q.add_argument("--findings", required=True)
    q.add_argument("--reads", required=True)
    q.add_argument("--first-reader", required=True)
    q.add_argument("--second-reader", default=None)
    q = sub.add_parser("reading-plan")
    q.add_argument("--key", required=True)
    q.add_argument("--out", required=True)
    q.add_argument("--seed", type=int, default=BUNDLE_SEED)
    q.add_argument("--scenarios-per-session", type=int, default=0, help="scenarios per reading session (default: about %d answers per session)" % READ_ANSWERS_PER_SESSION)
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
    q.add_argument("--models", default=",".join(MODEL_KEYS))
    q.add_argument("--thinking-models", default="none")
    q.add_argument("--thinking-mech", default="yes")
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
        fn = {"model-ids": cmd_model_ids, "thinking-probe": cmd_thinking_probe, "reading-sets": cmd_reading_sets, "estimate": cmd_estimate, "gate-models": cmd_gate_models, "model-order": cmd_model_order, "sets": cmd_sets, "gate-params": cmd_gate_params, "gate-inputs": cmd_gate_inputs, "frozen-packs": cmd_frozen_packs, "dry-inputs": cmd_dry_inputs, "manifest": cmd_manifest,
              "start-check": cmd_start_check, "derive-draft": cmd_derive_draft, "derive-check": cmd_derive_check, "check-verify": cmd_check_verify,
              "check-rows": cmd_check_rows, "model-failed": cmd_model_failed, "prompts-relation": cmd_prompts_relation, "prompts-default": cmd_prompts_default, "measures": cmd_measures,
              "rule": cmd_rule, "thorough-rows": cmd_thorough_rows, "harm-match": cmd_harm_match, "compare-models": cmd_compare_models, "reading-plan": cmd_reading_plan, "call-counts": cmd_call_counts,
              "stage-calls": cmd_stage_calls, "leak-check": cmd_leak_check, "no-draft-ids": cmd_no_draft_ids, "bodies-relation": cmd_bodies_relation,
              "check-errors": cmd_check_errors, "reading-summary": cmd_reading_summary, "judge-sets": cmd_judge_sets, "pilot-check": cmd_pilot_check, "models-check": cmd_models_check, "server-gate": cmd_server_gate}[a.cmd]
        return fn(a)
    except (SixError, G.RuleError, R6.DaError, OSError, KeyError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
