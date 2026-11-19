# Milestone M4: provable, trustworthy, fast

**Tracking document.** The conductor keeps it true at every step; workers do not edit it.
Working name "M4"; the release number is decided at the end. Started 2026-11-18, after the
release of v3.0.0 and v3.0.1. `GOAL.md` says where the project is going; this file says
where the current milestone stands.

Last updated: 2026-11-19, 14:05.

## What this milestone is for

v3 showed that retrieval is what helps, and left three things open: the evaluation is too
small and too worn to show further gains, answers still contain invented or wrong items,
and answers are too slow. M4 closes those three, in that order, and then asks whether a
different base model does better.

## Done means (exit criteria)

| # | Criterion | Bar | Status |
|---|---|---|---|
| E1 | A new evaluation exists: fresh items for the five task types of `GOAL.md`, a hidden part, mechanical checks, a measured noise level | 300 or more mechanical items, 120 or more judged items, the smallest visible difference stated per tier | mechanical tier built: 496 items, split 251 open and 245 hidden (encrypted); judged tier: the second judging pilot is a no-go too (3 of 5), stopped until the Owner has seen it |
| E2 | The starting point is measured on it: plain base, base + retrieval at full precision, and the 4-bit model with retrieval, in default and strict mode, all on the same server path | numbers with their sizes; the cost of 4-bit shown inside a margin fixed before the run (5 points on the mechanical tier) | not started |
| E3 | Every answer is checked before the user sees it | at most 2 invented or wrong items per 10 answers in default mode on fresh questions, **measured by a reading**, with the checker's own counts beside it | checker built and wired into `ask --check` and the service; on the 30 labelled errors it catches 9, reports 3 as unverified and misses 18 (15 are wrong statements it cannot see); not yet measured on fresh answers |
| E4 | Answers are fast on the 4-bit server path | median under 60 s in default mode on the Mac Studio; first words within 10 s | service and streaming built; retrieval 9.5 s to 4.4 s when kept loaded; not yet measured with a model |
| E5 | Base model decision | bake-off of the shortlist on the new evaluation; keep or switch recorded | shortlist done |

Laptop measurements (the real bar of `GOAL.md`) need a 32 to 36 GB Apple Silicon laptop;
see "Waiting for the Owner".

## What the evaluation can and cannot show (from the design review)

- The judged tier at 120 items can show differences of about 0.33 on the old 1 to 5 scale,
  not 0.1 to 0.3. Most of the noise comes from the items, not from the judge.
- Fine decisions (the cost of 4-bit, the answer checker) therefore rest on the mechanical
  tier (about 5 points at 300 items) and on readings.
- One full measurement of the starting point (E2) costs about 64 to 84 hours of exclusive
  machine time. A fixed "smoke" subset of about 2.3 hours is used for every change, the
  open part per candidate, the hidden part per decision only.

## Work packages

First slice (needs no downloads; supports the 4-bit decision):

| WP | What | Role (model) | Status | Record |
|---|---|---|---|---|
| WP1a | Catalogues of real identifiers from the pinned sources | implementer (sonnet) | **done** 2026-11-18, commits `888333f` and `39e6911` (completed from 11 MB of pinned source files in `catalogue_sources.yaml`: Strimzi and Argo CD resource definitions, 38 of 43 Kafka tools, the Kubernetes API schema): 1,177 Kafka config keys, 1,577 Strimzi field paths, 35 annotations, 28 tools, Argo CD commands | `runs/m4/reports/wp1a_catalogues.md` |
| WP1b | Evaluation design and its review | conductor; complex-reviewer (opus) | **reviewed** ("build with these changes", 15 changes, all accepted); design being revised | `agents/m4-evaluation.md`, `runs/m4/reports/evaluation_design_review.md` |
| S1 | Harness: answer, manifest, pack replay, hidden part, run sizes, report | implementer | **done** (`d0403e3`); run only with a stub model so far | `runs/m4/reports/s1_harness.md` |
| S2, S3 | Mechanical items (config, Connect REST, Argo CD, error names) and the scorer with identifier precision; catalogue provenance | implementer | **done** (`39e6911`): 496 items with resources, manifest fixes, commands and version traps; a blind guess scores 11 %; split in `e6b8d8c` | `runs/m4/reports/s2_s3_tier_a1.md`, `tier_a1_rebalance.md` |
| S4 | Rubric pilot: blind rubrics for 30 questions whose answers have a reader's findings; check of the labels that rest on memory | complex-reviewer, researcher | **done**: 30 rubrics (`eval/m4/pilot/rubrics.jsonl`); 30 distinct labels, 25 confirmed, 5 partly, 0 refuted | `runs/m4/reports/s4a_label_check.md`, `s4b_blind_rubrics.md` |
| S5 | Judge pilot: does judging point by point repeat better than a mark from 1 to 5 | job-operator; needs the model server | **done, NO-GO: 1 of 4 criteria passed** (`runs/m4/S5_JUDGE_PILOT.md`). Judge chosen in S5a: DeepSeek without thinking, with source passages (22 s a call against 691 s). The analysis (`runs/m4/reports/s5_pilot_analysis.md`): the judge repeats well and is cheap, but the score does not rank answers as the reader did, and about 1 in 6 of its error flags outside the reader's labels is real. Decided 2026-11-19: build with changes. **Second pilot queued behind S6** (`scripts/m4_s5b_judge_pilot2.sh`): 24 fresh scenarios (`eval/m4/pilot2/`), rubrics corrected by a second reviewer before any answer existed (105 changes), judge step version 2, 48 answers and 263 judge calls, then a blind reading; go or no-go on criteria fixed beforehand. **Second pilot done 2026-11-19, NO-GO: 3 of 5 criteria passed** (`runs/m4/S5B_JUDGE_PILOT2.md`). Passed: agreement between passes (kappa 0.74), the judge's share of the variance (0.19), quotes not found (4.7 %). Failed: the judge's flags are mostly real (0.78 and 0.81 confirmed) but catch only 0.43 and 0.49 of the 111 wrong, harmful or invented statements two blind readers found (bar 0.7); and its "the excerpts had it, the answer dropped it" flags were confirmed in 6 of 38 cases (bar 60 %). The stage is stopped until the Owner has seen it | `runs/m4/reports/s5_judge_step.md`, `s5b_unblind_report.md`, `s5b_blind_reading_01_12.md`, `s5b_blind_reading_13_24.md` |
| S6 | Noise (the reference run twice) and 4-bit against full precision on the same server, with pack replay | job-operator; needs the model server | **done** 2026-11-19 10:18 (`runs/m4/S6_NOISE_4BIT.md`), 251 open items, first look and not the decision: no noise (two identical runs, 251 of 251 answers byte-identical); all checks pass on 77.3 % with the 4-bit model and 76.5 % at full precision (difference +0.8 points, 90 % interval -2.8 to +4.5; on identical excerpts 0.0, -2.6 to +2.7; inside the 5-point margin both ways); strict mode 8.0 points below default (-11.9 to -4.2); median 19 to 25 s per answer at 4-bit against 44 s at full precision | `runs/m4/reports/s6_prep.md` |

After the first slice:

| WP | What | Depends on | Status |
|---|---|---|---|
| WP1c | Remaining mechanical kinds (resources, commands, client code, version traps, valid but wrong, safe order, sizing, not covered) | downloads (see 3 below), S5, S6 | not started |
| WP1d | Judged tier: scenarios and rubrics, the Owner's questions first | S5 | not started |
| WP1f | Starting-point measurement (E2) | WP1c, WP1d | not started |
| WP2a | Labelled test set for the checker: 30 distinct items from the three readings | - | **done**: `runs/m4/reports/wp2a_labels_checked.jsonl` (two local-model runs, verified by counts and against sources) |
| WP2b | Answer checker in the runtime: flag, then revise. Separate code from the scorer; shares only the catalogues | WP1a, WP2a | library **done** (`a61f818`): zero false "invented" findings on 244 stored answers; wired into `ask --check` and the service; extraction gaps from the pilot reading closed 2026-11-19 (`d0a165e`, `runs/m4/reports/wp2b_extraction_gaps.md`): it now reports 14 of the 16 names in the 48 pilot answers that the catalogues can decide (8 before), with no new "invented" finding on the 244 stored answers; the pilot answers are a development set, so this is not a measurement; "revise" not started |
| WP2c | Measure E3 by a reading | WP2b, WP1f | not started |
| WP3a | Long-running local service that keeps the index and reranker loaded; streaming | - | **done** (`d0b3faa`): loopback only, OpenAI-compatible; about 3.8 GB held when idle |
| WP3b | Fewer excerpts for simple lookups; measure E4 | WP3a, WP1f | not started |
| WP4a | Shortlist of base models | - | **done** 2026-11-18 (`runs/m4/reports/base_shortlist.md`) |
| WP4b | Bake-off on the new evaluation; decision | WP1f | the four candidates are downloaded and load-tested 2026-11-19 (`runs/m4/reports/bakeoff_load_test.md`): all four load and answer on the server; median answer on 3 questions 8.0 s (Gemma 4 26B-A4B), 9.5 s (GLM-4.7-Flash), 35 s (Gemma 4 31B), 54 s (Granite 4.2 30B), against 35 s for the reference; quality not measured |
| D1 | Find why the server path and the in-process path give different output | - | not started |
| D2 | The Owner reads ten answers that a model has already read | - | waiting for the Owner |

Workers on local models: bounded reading and extraction go to `.claude/local-worker.sh`
when the model server is free and no job needs the GPU; design, web research and judging
stay on Claude models (`AGENTS.md`).

## Base-model shortlist (WP4a)

Reference: Qwen3.8-27B (Apache-2.0, 16.1 GB at 4-bit). To test, in this order:

| # | Model | Licence | 4-bit size | Why |
|---|---|---|---|---|
| 1 | Gemma 4 26B-A4B-it | Apache-2.0 | 15.6 GB | Fits; 3.8B active parameters, so likely faster; lowest hallucination figures among those that fit |
| 2 | Gemma 4 31B-it | Apache-2.0 | 18.4 GB | Best independent evidence on answering from documents; over the size budget, so quality first |
| 3 | GLM-4.7-Flash | MIT | 16.9 GB | Led one study of faithfulness on retrieved text; same family as our judge, so a second judge is needed |
| 4 | Granite 4.2 30B | Apache-2.0 | 16.5 GB | Documented tool calling and thinking switch |
| 5 | Qwen3.5-9B | Apache-2.0 | 6 GB | For the 16 to 24 GB laptop |

No independent figure exists for Qwen3.8-27B on answering from documents, and none for
any candidate on tool calling. Two user reports say Qwen3.8-27B loops on tool calls with
thinking off.

## Where we start (measured in v3)

| What | Figure | Size |
|---|---|---|
| Base + retrieval against plain base | +1.48 | 40 worn questions, two passes, standard error 0.21 |
| Invented or wrong items, default mode | 17 per 10 answers (real), 3 per 10 (holdout) | one model reader, one pass |
| Invented or wrong items, strict mode | 1 per 10 (real), 0 per 10 (holdout) | same |
| Made-up identifiers the catalogues catch today | 5 of 13, 1 weakly, 6 not covered, 1 missed | the items of the v3 reading |
| Retrieval missed the page | at least 5 of 42 holdout questions | one reading |
| Time per answer, 4-bit through oMLX | median 70.8 s default, 47.2 s strict | 10 questions per mode, Mac Studio |
| Time per answer, 4-bit through Ollama | median 38.6 s default, 26.7 s strict | 10 questions per mode, different build |
| Retrieval time, fresh process against running service | median 9.49 s against 4.37 s | 10 questions, real index and reranker, no model |
| Quality of the 4-bit builds | not measured | - |

## Decisions

| Date | Decision | By |
|---|---|---|
| 2026-11-18 | Order of work: evaluation, then the answer checker, then speed; base-model shortlist in parallel, bake-off after the evaluation exists | Owner |
| 2026-11-18 | Workers that need no GPU may run on the local model server | Owner |
| 2026-11-18 | The 15 changes of the design review are accepted into the design; those that are the Owner's are marked as waiting | conductor |
| 2026-11-18 | **Yes to all recommendations:** the 4-bit model on oMLX is the reference configuration; half of each tier hidden, encrypted, decisions on the hidden half, at most six looks; the downloads and installs for catalogues and checks; loading the 4-bit and full-precision 27B builds for the comparison; `questions_real` becomes a legacy set and the 4-bit bar a margin fixed before the run; public questions may be raw material after a licence check; the four bake-off candidates may be downloaded; other clients' models on the server may be unloaded when it is idle | Owner |
| 2026-11-19 | **After the failed judging pilot:** build the judged tier with the analysis's changes and a second small pilot on fresh items; the first pilot stays a no-go and the second gets new criteria fixed beforehand; the judge's error flags count toward the bars of `GOAL.md` only after a reader has confirmed them; DeepSeek stays the judge, with the earlier judge re-judging a 24-item subset on every decision run; the Owner reviews which points are essential on his own questions | Owner |

## Waiting for the Owner

Decided on 2026-11-18 (see "Decisions"). Still his to do, none blocking:

The failed gate of 2026-11-18 (the judging pilot) was decided on 2026-11-19; see "Decisions".

**Open, blocking the judged tier (WP1d) and everything behind it:** the second judging
pilot failed on 2026-11-19 (3 of 5). The analysis (`runs/m4/reports/s5b_pilot2_analysis.md`)
found that the recall bar was out of reach with what the judge is shown (a perfect judge
would reach about 0.68 against the bar of 0.7), that judge and readers do agree on which
essential points an answer misses (kappa 0.75 and 0.73, found after the fact), and that
the rule deciding "the excerpts had it" was wrong in both directions. Six decisions (J5
and J6 come from the mechanical tier and block nothing yet):

- **J1. The judged tier's role.** Recommended: the judge reports coverage of the essential
  points and forbidden statements only; wrong, invented and harmful rates come from an
  identifier check against the catalogues plus blind readings with the sources; no third
  pilot of the judge as an error finder; the coverage role is confirmed on the first fresh
  batch against criteria fixed beforehand. This changes `agents/m4-evaluation.md`.
- **J2. Readings on the hidden half.** Recommended: a sealed reader session may read
  hidden-half answers and return totals only, each one logged in the ledger. Without it
  the reading-based bars rest on the open half.
- **J3. A second reader on about a quarter of the answers.** Recommended: yes.
- **J4. The answer length limit.** 11 of 24 default answers and 5 of 24 strict answers in
  the pilot were cut at the frozen 2,048-token limit. Recommended: measure a higher limit
  as its own item; it changes the measured configuration and the answer time.
- **J5. A decline outcome in the mechanical scorer.** The 8-point loss of strict mode on
  the open half is mostly not a quality loss (`runs/m4/reports/s6_strict_loss_analysis.md`,
  one reader, all 26 lost items read): in 19 of them the excerpts lacked the fact and
  strict declined, 7 are real strict faults, and 6 items go the other way. Recommended:
  report pass, correct decline, wrong decline and wrong beside the unchanged all-pass
  rate, before the first hidden run. The scorer would then read the frozen index, which
  the design does not allow today.
- **J6. A work package for retrieval and index coverage.** Both analyses point at it: in
  the pilot about 48 of 56 essential omissions were retrieval or index gaps, and on the
  mechanical tier 16 of strict's 26 losses are facts that were not retrieved (10) or not in
  the index (6: the Argo CD command pages are excluded in `sources.yaml`, and Kafka tool
  options exist only in source code). Recommended: yes, after J5; it needs a go to rebuild
  the frozen index, a licence check and a measurement on the hidden half.

1. **Fresh real questions with correct answers:** 30 to 50 from real incidents.
2. **Review of the architecture rubrics** when they exist (without it that part is 15
   items, not 25).
3. **A 32 to 36 GB Apple Silicon laptop** for the fit and speed bars, when the time comes.
4. **D2:** read ten answers that a model has already read.

## Log

- 2026-11-18: milestone opened. Evaluation brief written; catalogue worker, base-model
  researcher and design reviewer started; a local-model worker extracted the 47 labelled
  items (counts verified).
- 2026-11-18, evening: catalogues done and pushed (`888333f`). Design review back:
  build with 15 changes; the main ones are that the scorer must not be the checker, that
  the judging method is piloted before the judged tier is written, and that the hidden
  part is half, split by family and encrypted. Shortlist back. First slice started:
  harness (S1), first mechanical items and scorer (S2, S3), design revision.
- 2026-11-18, night: first slice built and pushed: harness, 313 mechanical items with
  their scorer, the judge step and its chain, 30 blind rubrics, the label check (25 of 30
  confirmed, 5 partly), the answer checker (7 of the 9 catchable labelled errors caught,
  no false "invented" finding on 244 answers), the local service with streaming
  (retrieval 9.5 s to 4.4 s). Nothing has run against a real model yet: the model server
  is occupied by another client's models.
- 2026-11-18, late night: the Owner answered yes to all recommendations. Catalogues
  completed from pinned source files; mechanical tier grown to 496 items and split;
  checker wired in; bake-off candidates downloaded (disk at 98 %, 78 GB free); licence
  check on public questions done (ideas only, by hand, nothing verbatim, private
  provenance record). The first judging run was stopped: point-by-point judging costs
  about 7,000 reasoning tokens a call. Running: S5a (judge settings). Queued behind it: S6.
- Next, in order: read `runs/m4/S5A_JUDGE_SETTINGS.md` and choose the judge setting; S6
  runs by itself; then the full judging pilot with the chosen setting; then the judged
  tier and the starting-point measurement on the hidden half (first entry in the ledger).
- 2026-11-18, 23:16: S5a done. DeepSeek without thinking judges in 22 s a call and caught
  21 of 27 labelled errors in the subset; the earlier judge took 691 s and its thinking
  cannot be switched off. 23:42: full pilot done in 25 minutes: NO-GO, 1 of 4. 23:47: S6
  started after the conductor unloaded another client's model from an idle server.
- 2026-11-19: the Owner agreed to all five recommendations on the judged tier. Started:
  fair quote matching and the answer's own excerpts in the judge step; the design changes;
  24 fresh scenarios with rubrics under the new rubric rules. The second pilot runs after S6.
- 2026-11-19, 01:45: the judged tier's design is revised (`c78171a`); judge step version 2
  and the second pilot's chain are in (`d63f0a7`, `bd31c03`); 24 fresh scenarios written
  blind and corrected by a second reviewer (`af0121a`, `44db03f`). On the server: S6 in its
  first of five runs (about 25 s an item); the second pilot waits behind it.
- Next, in order: read `runs/m4/S6_NOISE_4BIT.md` (noise, first look at the cost of 4-bit,
  ceiling check of the mechanical items); when the second pilot has answered and judged,
  have one reviewer read the 48 answers blind and fill the candidate-error file, then
  run the pilot report for go or no-go; on a go, write the judged tier at scale.
- 2026-11-19, 10:18: S6 done. The mechanical tier has no run-to-run noise on the 4-bit
  server path, is not at its ceiling (77 %), and on the open half the 4-bit model is inside
  the margin against full precision. Strict mode loses 8 points on mechanical tasks. The
  second judging pilot started by itself at 10:18.
- 2026-11-19, 11:53: the second pilot answered and judged (48 answers, 263 judge calls);
  criteria 1 to 3 passed. Two reviewers then read the 48 answers blind, 24 each, one pass.
  12:15: unblinded and the report rerun: NO-GO, criteria 4 and 5 failed. What the readers
  found, apart from the judge (24 hard scenarios, one reader per item): default mode 67
  wrong or invented statements and 1 harmful in 24 answers, strict mode 40 and 3; essential
  omissions 23 in default and 33 in strict, of which the answer's own excerpts supported 1
  and 7, so most omissions are retrieval gaps. The answer checker catches 5 of the 14
  invented names that no judge flag caught.
- 2026-11-19, 12:35: analysis of the failed pilot back (`s5b_pilot2_analysis.md`). Beyond
  the judge: retrieval put 19 of the 119 chunks the rubric authors marked as covering into
  the packs, and 12 of 24 items got none; about 48 of the 56 essential omissions are
  retrieval or index gaps; median time per answer 82 s in default and 67 s in strict mode
  (24 each, 4-bit, server path). Started: a second blind reading of six items to measure
  agreement between readers; the load test of the four bake-off candidates.
- 2026-11-19, 13:40: three things came back. The bake-off candidates all load and answer
  on the server (table row WP4b). A second reader re-read six pilot items blind
  (`runs/m4/reports/s5b_second_reading.md`): 26 wrong statements found by both readers, 1
  only by the first, 7 only by the second, none of them false; omissions 16, 2 and 0; the
  readers differ on 3 of 16 "supported" values. The checker's extraction gaps are closed
  (row WP2b); one borderline finding stays (`leader.imbalance.per.broker.percentage`, absent
  from the pinned Kafka configuration but still listed in the Strimzi documentation).
- 2026-11-19, 14:05: analysis of strict mode's 8-point loss on the open mechanical half
  back (decision J5). The strict prompt is not changed: at most 7 items could be gained,
  below what the tier can detect.
- Next: the Owner's decisions J1 to J6. Not blocked by them: the load test, the checker's
  extraction gaps the pilot showed (the pilot items behind them are listed in the analysis
  and go into the change's record), D1, the mechanical tier's hidden-half runs.
