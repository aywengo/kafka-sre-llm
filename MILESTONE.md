# Milestone M4: provable, trustworthy, fast

**Tracking document.** The conductor keeps it true at every step; workers do not edit it.
Working name "M4"; the release number is decided at the end. Started 2026-11-18, after the
release of v3.0.0 and v3.0.1. `GOAL.md` says where the project is going; this file says
where the current milestone stands.

Last updated: 2026-11-21, night. **M4 is closed** (the Owner's decision of 2026-11-21); the release is v3.1.0.

## Closing state (2026-11-21)

M4 ran from 2026-11-18 to 2026-11-21. The Owner closed it on 2026-11-21 with two exit
criteria recorded as not met. What was measured is stated with its size; what is inferred
is marked.

| # | Criterion | Result |
|---|---|---|
| E1 | A new evaluation with a hidden half | **Done.** 644 mechanical items (339 open, 305 hidden) and 121 judged scenarios (60 open, 61 hidden, of which 25 are the Owner's). Every judged batch passed an independent chunk count of 95 % or more before any answer existed. Two of six hidden looks are used |
| E2 | The starting point | **Measured** on the hidden half (305 items, 61 scenarios). All checks pass: closed book 52.8 %, 4-bit with retrieval 81.0 %, strict 75.7 %, full precision 82.6 %. Judged coverage 0.38 closed book, 0.55 with retrieval. The 4-bit margin was missed by 0.10 (−1.64 points, interval −5.10 to +1.90) and the Owner accepted the stated loss. Default stays the reference mode |
| E3 | At most 2 wrong or invented items per 10 answers | **Not met.** A sealed blind reading of 61 hidden default answers found 17.2 non-minor wrong, invented or harmful statements per 10 answers (90 % interval 14.4 to 20.0) and 13 harmful ones (bar 0); two readers 2 % apart. The checker marks nonexistent identifiers in place; the model patch step is built, off, and not tested on hidden data |
| E4 | Fast answers | **Met for short questions only.** 120 open questions through the streamed service: 26.1 s median on short mechanical questions, 90.3 s on judged scenarios (39 of 60 cut at 2,048 tokens); first words 19.2 s and 22.9 s, none within 10 s. Another client's model was loaded during that run (effect not known); the hidden runs gave 24.9 s and 81.8 s |
| E5 | Base model | **Keep Qwen3.8-27B at 4-bit.** An open screen (339 items, 60 scenarios per candidate) dropped Gemma 4 26B-A4B (judged coverage −0.10), GLM-4.7-Flash (mechanical −15.6 points, coverage −0.35, answers not reproduced) and Granite 4.2 30B (speed) by rules fixed beforehand; Gemma 4 31B was left out for memory; no hidden look spent |

Other results that shape what comes next:

- **Retrieval is the lever and its ranking is the limit.** On the two open batches the
  frozen retrieval put the needed chunk in front of the model for 37 of 149 and 56 of 166
  indexed must points; most of the rest are found by the search and ranked below the
  six-chunk pack (63 and 68), the others never found (44 and 41). Two candidates (R2, R3)
  failed rules fixed beforehand; the frozen retrieval stays.
- **The checker cannot close E3.** Most counted errors are wrong statements about
  behaviour, not names it can look up (inferred from the B1 reading; the hidden reading
  returns totals only).
- **Answer length drives time.** 36 of 61 hidden default answers were cut at 2,048 tokens;
  at 29 tokens per second that alone is about 70 s.

Not done in M4: laptop measurements (the Owner runs `agents/laptop-tests.md` himself);
the Owner's review of the architecture rubrics of the open batches; a grounded default
prompt variant (WP2d).

Known defects carried over: the E2 chain reuses a stored manifest and stage markers
without checking them at the start; a resume refuses after the server reloads a model
(the load stamp is compared); the E5 chain goes on when a step refuses; the checker does
not find names in prose and comments; the reading totals' fix for the readers'
disagreement is in (`f1094ab`).

Hidden content: the conductor saw the Owner's notes on 11 of his hidden items on
2026-11-20; they are tagged and every hidden judged result is reported with and without
them. The sealed sessions' transcripts (27 sessions, 54 files) and the plain-text tool
outputs of hidden content were deleted on 2026-11-20 and 2026-11-21, as the Owner
decided. This session's own transcript still holds the Owner's notes on those 11 items.

The release, v3.1.0 (the Owner's decision): the retrieval runtime with the answer checker
(inline marks) and the streaming service, the open half of the evaluation with its
harness, the laptop test plan; the model patch step and the retrieval candidates stay in
the code, off by default and marked experimental; the published index and adapter are
unchanged; no hidden look for the release, because the default answer path has not
changed since the E2 run (`kafka_sre_rag/` and `tests/golden/` are identical to commit
`48f98a9`).

The next milestone (the Owner's decision): retrieval ranking; fewer wrong statements (a
default mode that keeps closer to the excerpts, shorter answers); and the repairs listed
above.

## What this milestone is for

v3 showed that retrieval is what helps, and left three things open: the evaluation is too
small and too worn to show further gains, answers still contain invented or wrong items,
and answers are too slow. M4 closes those three, in that order, and then asks whether a
different base model does better.

## Done means (exit criteria)

| # | Criterion | Bar | Status |
|---|---|---|---|
| E1 | A new evaluation exists: fresh items for the five task types of `GOAL.md`, a hidden part, mechanical checks, a measured noise level | 300 or more mechanical items, 120 or more judged items, the smallest visible difference stated per tier | mechanical tier built: 496 items, split 251 open and 245 hidden (encrypted); judged tier: after two failed pilots the judge's coverage role (J1) was checked on batch B1 (30 fresh scenarios): five criteria and the second half of the sixth pass, the rubric chunk check missed by about one point (94.0 % against 95 %); **the Owner accepted the coverage role on 2026-11-19 (K1)**; **the judged tier is written (2026-11-20): 120 scenarios, 60 open (B1, B2) and 60 hidden (the Owner's 24, B3's 36), every batch with an independent chunk count of 95 % or more before any answer**; the new mechanical kinds are built and accepted by an independent review (148 items, 88 open and 60 hidden), so the mechanical tier has 644 items (339 open, 305 hidden); integration into the harness (T10) is finishing |
| E2 | The starting point is measured on it: plain base, base + retrieval at full precision, and the 4-bit model with retrieval, in default and strict mode, all on the same server path | numbers with their sizes; the cost of 4-bit shown inside a margin fixed before the run (5 points on the mechanical tier) | chain built, reviewed, **measured 2026-11-21 on the hidden half** (`runs/m4/E2_STARTING_POINT.md`): mechanical 305: 4-bit default 81.0 %, strict 75.7 %, closed book 52.8 %, full precision 82.6 %; judged coverage 0.55 with retrieval against 0.38 closed book (61 scenarios); **the 4-bit margin was missed by 0.10** (−1.64 points, interval −5.10 to +1.90 against a lower end above −5) **and the Owner accepted the stated loss on 2026-11-21: 4-bit stays the reference**; default stays the reference mode |
| E3 | Every answer is checked before the user sees it (amended 2026-11-19, K4 b: prose streams as it is written, each code block is held until its check, which takes milliseconds, and corrections to prose follow the answer) | at most 2 invented or wrong items per 10 answers in default mode on fresh questions, **measured by a reading**, with the checker's own counts beside it; a statement marked "not confirmed" still counts; minor items are reported beside the bar, not counted (K4 a, c); measured on the hidden judged half after E2 (K4 g) | **measured 2026-11-20 on the hidden judged half: NOT MET**: 17.2 non-minor wrong, invented or harmful statements per 10 default answers (61 answers, 90 % interval 14.4 to 20.0; bar 2.0), 13 harmful (bar 0); two sealed readers 2 % apart; inline marks on, the model patch step off (built, not tested on hidden data). Strict: 14.6 per 10, 11 harmful |
| E4 | Answers are fast on the 4-bit server path | median under 60 s in default mode on the Mac Studio; first words within 10 s | **measured 2026-11-21 on 120 open questions through the streamed service:** short mechanical questions 26.1 s median (met), judged scenarios 90.3 s (not met; 39 of 60 answers cut at 2,048 tokens); first words 19.2 s and 22.9 s median, none of 120 within 10 s (not met); the hidden runs agree (24.9 s and 81.8 s per answer) |
| E5 | Base model decision | bake-off of the shortlist on the new evaluation; keep or switch recorded | **open screen done 2026-11-21: all three candidates dropped by rules fixed beforehand** (Gemma 4 26B-A4B on judged coverage, −0.10; GLM-4.7-Flash on breakage, mechanical and coverage; Granite 4.2 30B on speed); Gemma 4 31B left out (memory); no hidden look spent; **the Owner decided on 2026-11-21: keep Qwen3.8-27B at 4-bit** |

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
| WP1c | Remaining mechanical kinds (resources, commands, client code, version traps, valid but wrong, safe order, sizing, not covered) | downloads (see 3 below), S5, S6 | resources, manifest fixes, commands and version traps were built in slice A2; plan for the other five 2026-11-19 (`runs/m4/reports/wp1c_plan.md`): 120 items by templates and programmatic checks, no model, in a separate generator and separate files, so the 496 items and their split stay byte-identical; 11 to 15 worker-days. **Done 2026-11-19:** T0 to T6 and the T8 code (tables, Java catalogue from the 4.3.2 jars, scorer checks `eval2_score/3`, Java and Python client checks, generators: 24 client code, 32 valid but wrong, 16 safe order, 24 sizing; assembly and grouped split, dry run only). T7 and the real T8 run done (`584fe6e`). **T9 review (2026-11-19, `runs/m4/reports/wp1c_t9_review.md`): do not use until fixed.** Structure sound (rebuild identical, 120 references pass, 453 perturbed fail, kubeconform, javac and absence records agree), but the checks reject correct answers in client code (exact overloads; try-with-resources; 10 of 18 Java perturbations are correct code) and not covered (correct declines fail), accept too-broad fixes in three valid-but-wrong oracles, one safe-order template follows a doc page the pinned tool contradicts, one hidden gap is in the index. **Fix round started** (version 2 of the a3 set: changed items get new ids, split and exposure drawn again, a3 files replaced; no answer to an a3 item exists and no hidden look was used; every template on both sides, pairs kept together, so no brief change), then the rebuild, a T9 re-check, T10 |
| WP1d | Judged tier: scenarios and rubrics, the Owner's questions first | first fresh batch B1 | **Writing at scale (K2) since 2026-11-19, evening**: B2 (30 open) written, reviewed and committed (`0b522b3`), chunk check 98.0 %; the Owner's 24 (hidden, sealed) final, chunk check 96.9 %; 54 of the about 90 further scenarios exist. **B1 done 2026-11-19** (`runs/m4/B1_COVERAGE_FINAL.md`, `B1_COVERAGE_BATCH.md`): 30 scenarios, 90 answers, 180 judgments, 60 answers read blind (16 by a second reader). Judge against reader on 158 essential units: kappa 0.86 and 0.78, precision 0.98 and 0.90, recall 0.86 and 0.86 (pass 1, pass 2); supported misses confirmed 14 of 14; unverified 0.6 %; pass-to-pass kappa 0.85; readers' totals 41 and 53 on the shared answers (23 % apart); identifier check 0 false findings. The rubric chunk check before the run was 94.0 % (bar 95 %). Waits for the Owner: the gate, and writing the tier at scale |
| WP5 | Retrieval and index coverage: candidate index and settings next to the frozen ones, one hidden-half run decides (J6) | J5 | plan, funnel tool and runtime profiles done; R2 chosen by its rule; **closed 2026-11-20 with the frozen retrieval: the one additive candidate R3 failed its rule on B2 (+7.4 points against +15, none lost; frozen serves 37 of 149 indexed must points).** Earlier: **B1 check (T7) failed 2026-11-19** (`runs/m4/reports/wp5_t7_b1_funnel.md`): indexed must points served 56 to 77 of 166 (+12.7 points, bar +15), 12 of the 56 frozen-served lost (21 %, bar 5 %); T8 and T9 do not run; waits for the Owner (K3) |
| WP1f | Starting-point measurement (E2) | WP1c, WP1d | not started |
| WP2a | Labelled test set for the checker: 30 distinct items from the three readings | - | **done**: `runs/m4/reports/wp2a_labels_checked.jsonl` (two local-model runs, verified by counts and against sources) |
| WP2b | Answer checker in the runtime: flag, then revise. Separate code from the scorer; shares only the catalogues | WP1a, WP2a | library **done** (`a61f818`): zero false "invented" findings on 244 stored answers; wired into `ask --check` and the service; extraction gaps from the pilot reading closed 2026-11-19 (`d0a165e`, `runs/m4/reports/wp2b_extraction_gaps.md`): it now reports 14 of the 16 names in the 48 pilot answers that the catalogues can decide (8 before), with no new "invented" finding on the 244 stored answers; the pilot answers are a development set, so this is not a measurement; "revise" not started |
| WP2c | Measure E3 by a reading | WP2b, WP1f | not started |
| WP3a | Long-running local service that keeps the index and reranker loaded; streaming | - | **done** (`d0b3faa`): loopback only, OpenAI-compatible; about 3.8 GB held when idle |
| WP3b | Fewer excerpts for simple lookups; measure E4 | WP3a, WP1f | not started |
| WP4a | Shortlist of base models | - | **done** 2026-11-18 (`runs/m4/reports/base_shortlist.md`) |
| WP4b | Bake-off on the new evaluation; decision | WP1f | the four candidates are downloaded and load-tested 2026-11-19 (`runs/m4/reports/bakeoff_load_test.md`): all four load and answer on the server; median answer on 3 questions 8.0 s (Gemma 4 26B-A4B), 9.5 s (GLM-4.7-Flash), 35 s (Gemma 4 31B), 54 s (Granite 4.2 30B), against 35 s for the reference; quality not measured |
| D1 | Find why the server path and the in-process path give different output | - | **answered 2026-11-19** (`runs/m4/reports/d1_code_reading.md`, `d1_paths.md`): on 20 open items with the same excerpts and the same full-precision weights, 13 of 20 answers byte-identical and the pass result equal on 20 of 20; the prompt tokens are equal, so the template is not the cause; the 7 differences start at a near-tie word inside matching text (numerics of the two execution paths; whether the server's multi-token prediction or its kernels, not separated). The in-process path stays a continuity check on outcomes, not on bytes |
| D2 | The Owner reads ten answers that a model has already read | - | waiting for the Owner |
| WP2d | Grounded default prompt variant (K4 d): a default mode that keeps to the excerpts more closely, with its own pilot; the measured prompts stay frozen | WP1f | not started |

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
| 2026-11-21 | **M4 is closed**, with E3 and part of E4 recorded plainly as not met. **The release is v3.1.0**: the retrieval runtime with the additions that work (the answer checker with inline marks, the streaming service, the open half of the evaluation with its harness, the laptop test plan); the model patch step and the retrieval candidates stay off by default and experimental; no hidden look for the release. **The next milestone targets** retrieval ranking, fewer wrong statements (a default mode closer to the excerpts, shorter answers) and the repairs found on the way; speed work and the laptop run are not targets of it | Owner |
| 2026-11-21 | **Base model (E5): keep Qwen3.8-27B at 4-bit.** The open screen dropped all three candidates by rules fixed beforehand (Gemma 4 26B-A4B on judged coverage, −0.10 against −0.08; GLM-4.7-Flash on breakage, mechanical items and coverage; Granite 4.2 30B on speed); Gemma 4 31B was left out for memory; no hidden look was spent (2 of 6 used). Gemma 4 26B-A4B is noted as a candidate for a faster configuration in a later milestone: 24.6 s per judged answer against 86.0 s, no answer cut at the limit, fewer nonexistent identifiers by the identifier check, at a cost of 0.10 judged coverage on the open half | Owner |
| 2026-11-21 | **The 4-bit model stays the reference; the margin was missed and the loss is accepted (M1).** On 305 hidden mechanical items 4-bit passed 247 and full precision 252: an estimated loss of 1.64 points, 90 % interval −5.10 to +1.90, so the lower end missed the gate (above −5) by 0.10. Median time per mechanical answer 24.9 s at 4-bit against 48.2 s at full precision. `GOAL.md` allows a stated and accepted loss | Owner |
| 2026-11-21 | **The bake-off (E5):** Gemma 4 31B is left out: loaded alone it holds 18.0 GB, above the 16.7 GB allowance, and its full laptop footprint is projected over budget (only its model load was measured) (N1); the same decoding for every model: greedy, thinking off, the same prompts, the 2,048-token limit (N2); speed is a hard limit on the median over the judged answers, 1.15 times the reference's median on the same set (N3); the reference's stored answers are read again, blind, mixed with the finalists' (N4); the correction for several finalists stays, and the error rule is tightened: a candidate needs an observed difference in non-minor errors per 10 of at most 0 against the reference, a 90 % upper end below +3.0, and the separate cap on harmful findings (N5); Qwen3.5-9B is deferred to a later milestone, for the 16 to 24 GB laptop (N6); **go for the open screen** of Gemma 4 26B-A4B, GLM-4.7-Flash and Granite 4.2 30B on 339 open mechanical items and 60 open judged scenarios each, no hidden look (N7) | Owner |
| 2026-11-21 | **Licences of the bake-off candidates** (`runs/m4/reports/e5_t5_licences.md`): (1) Gemma 4: the Apache-2.0 licence page is enough; the older Gemma terms scope themselves to the models in their appendix and send Gemma 4 users to the separate licence, so the prohibited-use policy is not read as a licence condition for the official Gemma 4 weights. (2) GLM-4.7-Flash: the MIT field is enough for local evaluation and for a runtime that links users to the upstream download; before bundling, mirroring or publishing a quantised build of the weights, get the exact copyright and permission notice from Z.ai. (3) The LM Studio 4-bit build of Gemma 26B (from a QAT checkpoint) is acceptable for the local bake-off; before it becomes the release default or is redistributed, have LM Studio confirm the source revision, or measure a build with a documented source. (4) Quantised builds the project publishes ship the full licence text and a pinned source link, mark modified files prominently, keep applicable upstream notices and NOTICE attributions; a GLM build carries the confirmed MIT notice | Owner |
| 2026-11-20 | **Agreed with all recommendations L1 to L11 for the E2 and E3 runs:** the cost of 4-bit is tested one-sided (pass if a loss of more than 5 points is ruled out) (L1); the model patch step R2 stays off in the E3 measurement, inline marks only, and R2 is recorded as built but not tested on hidden data (L2); the sealed readers read default and strict answers (L3); the second judge's cross-check on 24 scenarios stays (L4); no full-precision run on the judged tier (L5); speed is reported per tier, a lower answer limit is its own item (L6); the 7 hidden valid-but-wrong items that share a published variant are redrawn (L7); the E3 reading may run while E2's mechanical runs go (L8); a restart after a crash, or a re-run of the scoring, is the same look (L9); the shape check of the hidden files is not a look (L10); E2 uses the Owner's questions and B3 as the hidden judged files (L11) | Owner |
| 2026-11-20 | The Owner reviewed his own hidden questions: changes to nine, one kept as it is, oq-010 restored. The conductor saw his notes on these eleven items (see the log); the Owner's decision: the eleven stay in the hidden half, tagged (`eval/m4/HIDDEN_TAGS.json`), and every hidden judged result is reported with and without them | Owner |
| 2026-11-20 | The new mechanical items stay at 88 open and 60 hidden (the mechanical tier is then 339 open and 305 hidden, 47 % hidden); the moved items are not dropped and no further hidden items are drawn | Owner |
| 2026-11-20 | The plain-text leftovers of hidden items are deleted: three tool-result files now, the sealed sessions' transcripts after M4's hidden runs. The two final encrypted files of the Owner's 24 questions are committed (`3e62193`). The Owner has a 32 to 36 GB Apple Silicon laptop and runs the laptop tests himself from a committed plan and scripts, and sends back the outputs | Owner |
| 2026-11-19 | **Agreed with the recommendations K1 to K6** (see "Waiting for the Owner", kept for the record): the coverage role is confirmed and every later batch needs the rubric chunk check at 95 % before any answer exists (K1); the judged tier is written at scale, the Owner's questions first, in batches of 30 (K2); WP5 stays open for one additive candidate, R3, tested once on B2 by the rule below, fixed before B2 exists (K3); E3: (a) to (g) as recommended, among them the amended wording, a separate grounded default prompt variant with its own pilot, the model patch step R2 kept on only by its rule, and no catalogue change (K4); the not-covered generator may read the frozen index (K5); the Owner's 34 questions are admitted under the working rule for public posts, all hidden, rubrics by sealed sessions, the plain copy out of the repository (K6) | Owner |
| 2026-11-19 | **After the second failed judging pilot, agreed with all six recommendations J1 to J6:** the judge reports coverage of essential points and forbidden statements only, wrong, invented and harmful rates come from an identifier check plus blind readings, no third pilot of the judge as an error finder, the coverage role is confirmed on a first fresh batch with criteria fixed beforehand (J1); a sealed reader may read hidden-half answers and return totals only, logged in the ledger (J2); a second reader on about a quarter of the answers (J3); a higher answer limit is measured as its own item (J4); the mechanical scorer reports declines beside the unchanged all-pass rate and may read the frozen index (J5); a work package for retrieval and index coverage, built as a candidate next to the frozen index and judged on the hidden half (J6) | Owner |

## Waiting for the Owner

Decided on 2026-11-18 (see "Decisions"). Still his to do, none blocking:

The failed gate of 2026-11-18 (the judging pilot) was decided on 2026-11-19; see "Decisions".

**Decided 2026-11-19, night: K1 to K6 as recommended (kept below for the record).** The B1 gate (`runs/m4/B1_COVERAGE_FINAL.md`). The
report tool prints GO on its six checks, but criterion 2 has two halves and the tool checks
only the second. The first half, fixed before the run, was missed: the rubric chunk ids
held the point's fact in 156 of 166 points (94.0 %, bar 95 %; 10 partial, 0 wrong; eight
completed afterwards, not re-measured). Readers later found two more chunk problems (b1-27
m6, b1-26 m3|m4), which stay as written because answers exist. By the letter the gate is
not passed. Two decisions:

- **K1. The coverage role.** Recommended: accept it as confirmed. The missed half is
  rubric preparation, not the judge. What it protects, the "supported" flags, was confirmed
  in 14 of 14 cases, and every criterion on the judge passed with a margin (lowest: recall
  0.86 against 0.75). For every later batch the chunk check runs after the completions and
  must reach 95 % before any answer exists.
- **K2. Writing the judged tier at scale.** About 90 more scenarios to reach 120, the
  Owner's questions first, in batches of 30 with the same writing, second review and chunk
  check. Bulk generation, so the Owner's go. Recommended: yes.
- **K3. WP5 after the failed B1 check (T7, 2026-11-19).** R2 does not go further; the
  frozen retrieval stays. Recommended: keep WP5 open for one more candidate that only adds
  to the frozen pack (the frozen six chunks first, then exact-heading lookup and one more
  runbook slot up to 10 chunks / 24,000 characters; on the development items +24 points,
  −0 lost, about +6 s inferred), so it cannot lose a frozen-served point by construction.
  Its rule is fixed before any look, and its out-of-sample check is the next fresh batch
  (B2, if K2 is a go), not B1. The candidate index (T4) and its two source decisions wait
  until then.
- **K4. E3, after the revise design** (`runs/m4/reports/wp2b_revise_design.md`, one
  reviewer, development data B1). Of the 56 non-minor errors in B1's default answers, 13
  have nothing at run time that could show them (the excerpts seem to support them, or
  they are reasoning about the user's case), so even a perfect checker would leave default
  at about 4.3 per 10 (2.7 to 5.0) against the bar of 2; strict about 2.3. The realistic
  revise step removes about 5 to 7 of the 56. Decisions, with the recommendation:
  (a) a statement marked "not confirmed" still counts as an error: yes;
  (b) "checked before the user sees it" against streaming: stream prose, hold each code
  block until its check (milliseconds), corrections to prose follow the answer, and amend
  the E3 wording openly to say so;
  (c) minor items do not count toward the bar, reported beside it: yes;
  (d) the bar cannot be reached by checking alone in default mode: keep the bar, and open
  a separate item for a grounded default prompt variant (its own pilot), with E3 possibly
  closing M4 as not met;
  (e) build the model patch step (R2) and keep it on only by the rule in section 7 of the
  design; the code fixes (R0) and inline marks (R1) now;
  (f) no catalogue change now;
  (g) E3 is measured on the hidden judged half after E2 (needs K2).
- **K5. WP1c: may the item generator read the frozen index** (read-only, hash checked),
  to prove for each "not covered" item that its terms have no chunk in the index? The
  scorer already may (J5). Recommended: yes. Nothing waits for it before the not-covered
  generator (T7). Not a decision but for the Owner to know: the jar downloads for the
  client-code checks are taken as covered by the go of 2026-11-18 ("downloads and installs
  for catalogues and checks"), after a check of versions, hashes and licences.
- **The rule for R3 (K3), fixed on 2026-11-19 before B2 exists.** R3 is the frozen pack
  (the frozen six chunks, in frozen order, untouched) followed by the exact-heading lookup
  (2 slots) and one more runbook slot (at most one runbook chunk added beyond those the
  frozen pack holds), up to 10 chunks and 24,000 characters by the frozen cap's
  accounting. Built 2026-11-19, settings hash `d0225486…a6b55b0`
  (`runs/m5/PROFILE_R3.json`); development items +14 of 106 indexed points, −0; B1
  (descriptive) 56 to 69 of 166 (+7.8 points), −0; about +5 to +6.5 s prompt time
  (inferred). It is run once with the funnel on B2, after B2's rubrics have
  passed the chunk check (95 %) and with the search terms of B2's default run for both
  variants. An indexed must point is served when one of its chunk ids is in the pack.
  Pass only if both hold: (a) R3 serves at least 15 percentage points more indexed must
  points than frozen; (b) of the points frozen serves, R3 loses at most 5 % (0 by
  construction; a loss means a bug). On a pass, the hidden-half run of J6 decides; on a
  fail, WP5 closes with the frozen retrieval. B1 is development data for R3 and decides
  nothing.
- **The rule for E3 and the revise step (K4), fixed on 2026-11-19 before any fresh answer
  exists** (from `runs/m4/reports/wp2b_revise_design.md` section 7, with K4). Run once on
  the hidden judged half after E2, as one row of `runs/m4/HIDDEN_LEDGER.md`: E2's stored
  4-bit default answers and packs go through the revise step (no regeneration), and a
  sealed reader reads the final answers and returns totals only. (1) Configuration: the
  4-bit reference model on the server path, the retrieval E2 used, default mode, the
  answer limit of E2, first-pass prompts unchanged; the manifest records the checker
  version, the catalogue hash, the revise prompt hash and settings. (2) The E3 figure:
  the first reader's non-minor wrong + invented + harmful statements in the final
  answers per 10 answers, 90 % interval by a bootstrap over items, harmful counted
  separately; a statement marked "not confirmed" counts; minor statements are reported
  beside it and not counted; a second reader on 25 % of the answers, and if the two
  readers' totals differ by more than 30 % the reading is not used. (3) E3 is met if the
  rate is at most 2.0 per 10 and harmful is 0, on at least 60 answers; otherwise it is
  reported as not met, with the gap. (4) The model patch step (R2) stays on by default
  with `--check` only if all hold: errors removed minus errors introduced at least 1.0
  per 10 (read line by line against E2's reading of the same answer); no new harmful
  statement; no new invented identifier; judge essential-miss units no worse by more
  than 1.0 per 10; added time at most 10 s median and 25 s at the 90th percentile.
  (5) Reported beside it: the share of answers revised, the patches the re-check
  rejected, the checker's counts before and after, the time. B1, the second pilot, the
  244 stored answers, the 30 labelled errors and the open mechanical half are
  development data for the revise step. A first look on B2 (open) is allowed and
  decides nothing.
- **K6. The Owner's 34 questions** (`eval/m4/inbox/owner_questions.yaml`, 2026-11-19).
  The file calls them reconstructions of public incidents, so by the brief they are
  source 3 (public questions), not source 1. Recommended: (a) admit them under the working
  rule for public posts: ideas only, rubrics written from the pinned documentation and
  never from the posted fix, the source link kept as the private provenance record, and a
  point that only the incident shows marked "rests on a source outside the index";
  (b) keep all 34 hidden as marked, so they serve E2, E3 and E5; the next development
  batch B2 (for K3's check) is then written by the usual process and is open; (c) a sealed
  writer and a sealed reviewer write and check their rubrics and return results per id
  only; each item is checked against Kafka 4.3, Strimzi 1.3.1, Kubernetes 1.37 and Argo
  CD 3.6.1 (22 have no exact version, and an incident from the ZooKeeper era may not
  apply); (d) once the sealed copy is verified, the plain copy leaves the repository (the
  Owner keeps the original elsewhere; deleting it is the Owner's call). Not blocking: the
  base and judge models may have seen these public incidents in training, which raises
  absolute scores on them but affects both arms of a comparison alike.

**Decided on 2026-11-19, afternoon: the Owner agreed with the recommendations J1 to J6
below (see "Decisions").** The second judging pilot failed on 2026-11-19 (3 of 5). The analysis (`runs/m4/reports/s5b_pilot2_analysis.md`)
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
- 2026-11-19, 14:15: the item generator's list of Kafka settings that Strimzi forbids
  under `spec.kafka.config` was checked against the pinned API reference by a local-model
  worker: it is broader than the source everywhere, so no generated manifest can ask for a
  forbidden broker setting. One gap for connectors: `connector.plugin.version` is forbidden
  in a `KafkaConnector` but not skipped; no open item uses it, the hidden half was not
  looked at. To fix when the remaining mechanical kinds are generated (WP1c), not before:
  a change to the pool would reshuffle the existing items.
- 2026-11-19, about 13:00: the Owner agreed with J1 to J6. Started, all on CPU: the decline
  outcome in the mechanical scorer (J5); the judge step's coverage role with support by
  chunk ids, unverified verdicts and the report for the first fresh batch (J1); the
  evaluation's identifier check for free-text answers (J1); the amendment of
  `agents/m4-evaluation.md`; the analysis of where retrieval loses the needed passage and
  the plan for the retrieval work package (J6).
- 2026-11-19, 13:55: built and pushed for J1 and J5: the judge step's coverage role
  (`ca1ae42`, `ab7920a`), the scorer's decline outcomes (`21ac50e`: on the 251 open items
  default has 194 pass, 3 correct declines, 52 wrong; strict has 174 pass, 41 correct
  declines, 10 wrong declines, 21 wrong; the split is fitted to the open half), the
  evaluation's identifier check (`970e8c5`: 8.3 nonexistent identifiers per 10 pilot
  answers in default mode and 1.7 in strict, all 43 findings verified), the amended design
  (`agents/m4-evaluation.md`), the run chain and the blinding tool (`6e75ae9`).
- 2026-11-19, 14:17: batch B1 written, reviewed and pushed before any answer exists
  (`91e2b34`, `c02b429`): 30 scenarios, six per task type, 184 must points, 79 essential
  units, 74 must-not points; three second reviewers accepted all thirty with 189 changes.
  Criterion 2, first half: an independent count of all 166 points with chunk ids found the
  chunks hold the fact in 156 (94.0 %; bar 95 %), partial in 10, wrong in none; eight were
  completed afterwards, not re-measured. The run was started on the model server (90
  answers, 180 judge calls, estimated 2.6 to 2.9 h).
- 2026-11-19, 17:20: B1 done. The run took 2.3 h (14:17 to 16:35); all 90 answers and
  180 judgments valid, packs identical across the three sets. Blind readings started at
  16:25, while the judge ran: three readers on 60 answers, a second reader on 16 (8 items,
  27 % of the read answers); 441 rows, all valid; about 18 to 28 minutes and 80,000 to
  320,000 tokens per reader session. Criteria: see WP1d and "Waiting for the Owner" (K1,
  K2). Descriptive, open items written to be hard, not a decision measurement: the first
  reader found 56 wrong, invented or harmful statements that are not minor in the 30
  default answers (24 of 30 answers have at least one; 4 harmful) and 34 in the 30 strict
  answers (19 of 30; 2 harmful), against the bar of at most 2 per 10 answers; the judge's
  coverage is 0.57 default and 0.47 strict. The raised limit (J4): cut-off answers 19 of
  30 at 2,048 tokens, 1 of 30 at 4,096; +6.7 s per answer (SE 3.4); coverage +0.01 (SE
  0.02); the identifier check finds 20 nonexistent identifiers at 4,096 against 10 at
  2,048 (not verified by reading). Rubric remarks of the readers, not changed: b1-17 m5
  names only `ssl.truststore.location` (both answers used the working
  `ssl.truststore.certificates`); two pinned sources disagree on the defaults of
  `connect.protocol` and `transaction.timeout.ms`. The full funnel (WP5) stalled after 24
  of 275 items (no output for 10 minutes while the reranker scored); stopped, a fix is in
  work.
- 2026-11-19, 17:15: the full funnel ran after a fix (`6b90a8d`: the guard had killed a
  working job whose log was block-buffered): all 275 stored packs replayed exactly; on the
  development set frozen serves 32 of 106 points and 88 of 140 facts, R1 67 and 114, R2 66
  and 113 (the plan's figures; frozen facts 88 against the plan's 87). T5 (runtime
  profiles R1 and R2 next to the frozen one) started. **T6 rule, fixed now, before T5's
  runtime check** (the simulated figures above were known when it was written): a profile
  is eligible only if its runtime packs equal the funnel's simulation on the 275 items, or
  the differences are explained and change no count by more than 1; R1 is chosen only if
  its points plus facts served exceed R2's by at least 5 and it serves at least as many
  weight-2 points; otherwise R2 (no reranker, about 5 s faster). The chosen profile's
  settings hash is recorded here and in `runs/m5/PROFILE_CHOSEN.json` before anyone builds
  or reads the B1 funnel (T7), whose rule is in the plan (indexed must points served +15
  percentage points or more, at most 5 % of the frozen-served points lost).
- 2026-11-19, 17:30: T5 done (`runs/m4/reports/wp5_t5_profiles.md`): `--profile
  frozen|r1|r2` on `ask`, `chat`, `serve` and `scripts/eval2.py answer`, frozen the
  default; settings in `kafka_sre_rag/candidate.json`; answers made with r1 or r2 carry the
  profile, its settings hash and `measured: false`. The runtime reproduces all 275 stored
  frozen packs, and its r1 and r2 packs equal the funnel's simulation on 275 of 275 items
  each; a drift test guards the copied ordering rules. T6 by the rule of 17:15: both
  eligible; R1 181 against R2 179 points plus facts (difference 2, below 5), weight-2 49
  each: **R2 chosen**, settings sha256 `34dae8871e23…0e3502`
  (`runs/m5/PROFILE_CHOSEN.json`, recorded 17:25 before the B1 funnel was built).
- 2026-11-19, 17:37: T7, the B1 funnel, rule applied once: **FAIL on both parts.** The
  frozen variant reproduced the 30 stored B1 packs exactly. Indexed must points served:
  frozen 56 of 166 (33.7 %), R2 77 (46.4 %), +12.7 percentage points against the bar of
  +15; 12 of the 56 frozen-served points lost under R2 (21.4 %, bar 5 %). By kind (frozen,
  R2, lost): diagnostics 22, 16, 7; debugging 16, 24, 0; configuration 6, 19, 1; coding 7,
  11, 3; architecture 5, 7, 1. The 18 points outside the index are served by neither. On
  the development items R2 had gained 34 points and lost 3; out of sample the gain is a
  third of that and the losses are four times the bar, the early sign of fitting the plan
  named. B1 has now been looked at for retrieval, so it can no longer serve as the
  out-of-sample check of any other profile.
- 2026-11-19, 17:59: the revise step of the checker designed
  (`runs/m4/reports/wp2b_revise_design.md`): the bar of 2 per 10 is out of reach in
  default mode by checking alone (K4). The checker made 6 false flags among its 22 on the
  B1 answers (none on its development sets); its code fixes (R0, items b1-03, b1-04,
  b1-13, b1-23, b1-24) started, CPU only.
- 2026-11-19, 18:07: R0 done (`runs/m4/reports/wp2b_t1_checker_fixes.md`): on the B1
  answers the 6 false flags are gone, two missed invented names are now found (b1-24
  `--partition`, b1-23 a nonexistent Streams key), the 16 true flags are kept; on the 244
  stored and 48 pilot answers no new finding, one false flag of the same kind removed;
  catalogue hash unchanged. These B1 answers are development data for the checker now. The
  pilot invented totals in `wp2b_extraction_gaps.md` (13 and 6) were from an intermediate
  run; the committed code gives 11 and 5 (corrected there).
- 2026-11-19, 18:17: WP1c planned (`runs/m4/reports/wp1c_plan.md`) and started on CPU
  (T0 download check, T1a tables, T3 scorer checks). Conductor decisions on the plan: no
  model in the generators; the Strimzi forbidden keys are fixed in the new pools only; the
  hidden half grows through a new file; the mix floors apply to each build on its own.
- 2026-11-19, 18:25: the Owner's questions file (`eval/m4/inbox/owner_questions.yaml`,
  git-ignored) checked by counts only, no question text read: 34 items (case numbers 2 to
  51, 17 gaps the header calls review exclusions), all marked hidden; diagnostics 14,
  debugging 10, architecture 4, configuration 4, coding 2; 22 with no exact version; no
  IPv4, e-mail or secret-like strings; 50 links, all public post-mortems and issue
  trackers. The header calls them public-source reconstructions, not the Owner's own
  incidents. Sealing them into `eval/hidden/tier_b_owner.jsonl.enc` (ids `oq-NNN`, source
  and note kept apart from the question) started; nothing is written from them before K2
  and K6.
- 2026-11-19, 18:55: the 34 questions are sealed (`eval/hidden/tier_b_owner.jsonl.enc`,
  ids `oq-002` to `oq-051`, round trip checked; intake script `23c4ec3`; the sealed file
  is not committed, which is part of K6). WP1c: T1a done (`34d9c2e`; V4 and V8 marked
  unanchored, so no items are built on them yet). T3 done, scorer `eval2_score/3`, the
  496 references still 496 of 496 and the perturbed 0 of 496; its follow-up is running
  (the check reads T1a's table through one function; `strimzi.io/restart` accepts only the
  documented forms; every oracle rule carries an anchored flag). T1b (jars, sparse Kafka
  clone, Java API catalogue) and T6 (safe-order and sizing generators) started.
- 2026-11-19, evening: WP1c T1b, T2, T3, T5 and T6 done and committed (`3572c40`,
  `f6d62a9`, `7d52eb3`, `eaa4546`, `628b473`). Kafka 4.3.2 client jars and a sparse clone
  at the tag's commit, hash-checked; Java API catalogue (812 classes). V8 is settled by the
  Kafka source (an unknown topic config name is rejected), so it is not a valid-but-wrong
  rule; V4 stays without a pinned source. Python symbols from confluent-kafka 2.16.1 (the
  newest release at least 14 days old; 2.16.2 came out on 2026-11-18). Generators: 32
  valid-but-wrong, 16 safe-order, 24 sizing items, every rule with file and line; five
  candidates without a pinned passage dropped, not filled in. The 496 items still score
  496 of 496 (references) and 0 of 496 (perturbed). T4 (client code, 18 Java and 6
  Python) started.
- 2026-11-19, late evening: WP1c T4 done (18 Java, 6 Python; fixed seed per template, a
  Streams task replaced because it sat near the training data, each Python item paired
  with the Java item it restates), T2b (a call through Consumer, Producer or Admin now
  counts), and the T8 code with a dry run (95 items without the not-covered kind: 48 open,
  47 hidden, two runs identical; on the union of 591 the near-miss and scope floors are
  missed at 13.0 % and 13.5 %, reported as the plan foresaw). Commits `8604d83`,
  `635f7d1`, `704508f`, `be585d2`. D1 started as a code reading on a local model.
- 2026-11-19, night: D1 answered (see its row): 13 of 20 answers byte-identical between
  the in-process and server paths, pass result equal on 20 of 20, prompt tokens equal;
  the rest is numerics. No model loaded or unloaded on the server.
- 2026-11-19, night: the Owner agreed with K1 to K6. The R3 rule is fixed above, before B2
  exists. The plain copy of the Owner's questions moved out of the repository after its
  hash was checked against the sealed copy. Started: WP1c T7 (not covered) and the tool
  for sealed sessions (`scripts/sealed_io.py`).
- 2026-11-19, 20:00 to 21:35: WP1c T7 and T8 done: the a3 set built and split (`584fe6e`):
  120 items, 60 open and 60 hidden; the mechanical tier now has 616 items; on the union
  the near-miss, scope and value/type floors are missed (12.5, 13.0, 9.7 %), reported as
  decided; independent review T9 running. R3 built (`d3d12ea`, hash `d0225486…`). The rule
  for E3 and the revise step written (`fd634f5`); the revise module (R1, R2, streaming)
  being built. The Owner's questions: three sealed writers kept or restated 24 of 34 and
  dropped 10 (oq-008, 010, 014, 028, 029, 030, 038, 043, 045, 051: the mechanism does not
  exist on the pinned versions or cannot be checked in them); three sealed reviewers
  confirmed all 10 drops and changed 23 of the 24 rubrics; chunk check 110 of 131 points
  (84 %), so a sealed completion step runs before an independent count (bar 95 %). B2: 30
  open scenarios written by three writers (two overlapping items replaced), assembled in
  `eval/m4/b2/` (not committed until reviewed), three second reviewers running.
- For the Owner, not blocking: on his own questions, the restatements whose cause differs
  from his original (oq-018, oq-032, oq-036, oq-041, oq-042, oq-048), and the weights of
  the three architecture items (oq-002, oq-012, oq-026; in oq-012 two of his proposed
  points were left out, one over the six-unit limit, one with no pinned source). He may
  read his own hidden items; the conductor does not.
- 2026-11-19, 21:40 to 23:10: a usage limit stopped five workers at about 21:45; all were
  resumed after 21:50 from their saved state. WP1c T9 (independent review): the a3 set
  is not to be used until fixed (see the WP1c row); three fix workers, two done, then a
  rebuild as version 2 and a second review. The revise module is in (`8d72de7`). B2:
  three second reviewers accepted all thirty with 190 changes; the independent chunk
  count found 139 of 150 points (92.7 %, under the bar), a completion step followed, and
  a fresh recount gives 146 of 149 (98.0 %; partial b2-04 m5, b2-07 m1, b2-11 m1); B2
  committed before any answer exists (`0b522b3`). The Owner's 24: a sealed completion
  and a sealed independent count, twice: 119 of 129 (92.2 %), then 123 of 127 (96.9 %;
  partial oq-036 m1, oq-049 m3 and m4, oq-050 m2); final files
  `eval/hidden/tier_b_owner_{items,rubrics}_final3.jsonl.enc`, counts in
  `runs/m4/OWNER_CHUNK_CHECK.json`. The chunk helper drops chunk text between `<--` and
  `-->` (`scripts/eval2_judge.py:955`, found on b2-26); not fixed yet.
- For the Owner: (1) plain text of hidden rubric and chunk text was saved by the harness
  in three tool-result files of this session (outside the repository), and every sealed
  session's transcript holds what it decrypted; nobody reads them; deleting them is his
  call. (2) Whether the sealed Owner files are committed like `tier_a.jsonl.enc`
  (recommended: the two final3 files only). (3) Review list on B2: architecture items
  b2-09, b2-10, b2-19, b2-20, b2-29, b2-30; b2-02 (weight of the prevention point), b2-05
  (alternative path), b2-13 (sync termination as a must point); on his own questions also
  oq-047 (one point removed to keep six units).
- 2026-11-20, 00:00 to 00:30: a3 version 2 failed its second review on one point of
  mine: redrawing the split put 17 questions that were open in version 1 on the hidden
  side. Rebuilt as version 3 (`be03953`): anything ever published open is forced open,
  the hidden side is filled with fresh draws; 148 items, 88 open and 60 hidden; third
  check running. The B2 chain is committed (`cfec4a9`) and its run started at 23:58
  (estimate 2.1 to 2.5 h). `CONTRIBUTING.md` added on the Owner's request (`5abc7d0`).
  The Owner's decisions of 2026-11-20 are in "Decisions"; the laptop test plan and
  scripts are being written (`agents/laptop-tests.md`, `scripts/laptop_bench.py`).
  Conductor's decision: the next judged batch (about 36 scenarios) is hidden and written
  by sealed sessions, because E3 needs at least 60 hidden answers and the hidden judged
  half holds 24.
- 2026-11-20, 01:40: **B2 done** (`runs/m4/B2_BATCH.md`; 23:58 to 01:38, 1 h 40 min; 60
  answers and 120 judgments valid). **The R3 check failed by its rule:** of the 149
  indexed must points the frozen retrieval serves 37 (24.8 %) and R3 serves 48 (+11, −0),
  +7.4 points against the +15 needed; (b) holds (0 lost). By the rule fixed beforehand,
  WP5 closes with the frozen retrieval; B2 is used up for retrieval. Descriptive, open
  items written to be hard: judge coverage 0.57 default and 0.49 strict; essential-miss
  units 9.2 and 11.8 per 10 answers; cut off at 2,048 tokens 21 of 30 default answers
  and 11 of 30 strict; median 84.7 s per default answer with search and retrieval (the
  60 s bar of E4 is not met on these long scenarios; B1 was similar); pass-to-pass kappa
  0.88; unverified 1.3 %. Identifier check: 11 nonexistent identifiers in 5 of 30 default
  answers (3.7 per 10) and 6 in 4 of 30 strict (2.0 per 10), not verified by reading.
  First look at the revise step (decides nothing): 4 of 30 default answers flagged, 3
  patches accepted and 1 rejected, median 16 s per patch call, +0 s median and +15.7 s
  at the 90th percentile over all answers. No blind reading of B2 is planned for now:
  B1's reading already puts default far above the bar, and the hidden reading decides.
  a3 version 3, revision 2 (`46a1291`): the third check accepts all five kinds; 12
  hidden items share a published fault or scenario under other names and are tagged.
  Laptop plan and script committed (`7955616`), tested on a stub only. Started: the
  laptop script against a real model on this machine, the revise step's development run
  on B1, and the hidden judged batch B3 (36 scenarios, three sealed writers: coding 12,
  configuration 9, debugging 9, diagnostics 6; no architecture items, because the 15
  that count without the Owner's review exist).
- Where the frozen retrieval loses the indexed must points (funnel files, counts as the
  tool gives them): B2, 149 points: 37 served; 63 are among the 40 BM25 candidates but
  ranked below the six-chunk pack; 44 are never among the 40 candidates; 5 are cut by
  the character limit. B1, 166 points: 56 served, 68 below the pack, 41 never among the
  candidates, 1 cut. So ranking loses more than search does, on both batches. A next
  retrieval work package would start there; it would be the Owner's decision, would
  need a fresh batch for its out-of-sample check, and is not planned before E2.
- 2026-11-20, 01:45 to 02:40: **hidden judged batch B3 final**: 36 scenarios by three
  sealed writers, three sealed second reviews with completion, one item replaced
  (b3-32 was the same family as the open b1-04). Independent sealed count: 147 of 165
  points (89.1 %) at first, although each reviewer had counted 100 %; after a completion
  round 156 of 161 (96.9 %; partial b3-01 m2 and m5, b3-12 m3, b3-24 m1, b3-32 m2b; one
  rewording judged weakened was restored). Files
  `eval/hidden/tier_b_b3_{items,rubrics}_final.jsonl.enc`, counts in
  `runs/m4/B3_CHUNK_CHECK.json`. **The judged tier now has 120 scenarios: 60 open (B1,
  B2) and 60 hidden (the Owner's 24 and B3's 36)**; 15 are architecture items. The
  hidden half is exactly the 60 the E3 rule needs.
  Revise step: the development run on B1 and a reading of every patch (B1, B2;
  `runs/m4/reports/wp2b_t4_patch_reading.md`): the model patch step removed no counted
  error on 90 answers and introduced one or two (net 0.0 to −0.3 per 10 against the
  +1.0 the rule needs); the inline marks hold. Changes to the patch step are briefed and
  wait for the harness integration's dry run to finish. Laptop: the script ran against a
  real model on this machine (all phases; scrubbed bundle clean; n=2: 79.5 s median, every
  answer cut at 2,048 tokens, first words 14.4 s, 20.3 GB peak); fixes committed
  (`ddf33b1`); the plan is ready for the Owner.
- 2026-11-20, 02:40 to 03:55: the plan for E2 with the E3 and E4 runs
  (`runs/m4/reports/e2_plan.md`): two hidden looks (E2, E3), about 20 to 29 h of server
  time, five gaps in the harness. Built and committed since: WP1c T10, several items
  files (`a3f2fc7`); the hidden path of the judge and the chunk helper fix (`4f9e8a6`);
  the sealed reading tools (`2da54a7`); the hidden scorer (`3fcb583`); the E2 decision
  rules by code (`008b818`; on the open S6 data, deciding nothing: 4-bit minus full
  precision +0.80 points, 90 % interval −2.83 to +4.53); first-words timing (`b8e9d45`);
  closed-book mode and the M4 reference label (`2b0c767`); the revise step after the
  patch reading (`c32559d`). Running: the E2 chain with its gate and dry runs (B6), the
  revise step's second development run on B1, a full test run on the committed tree.
  An old stash entry ("local fixes": `scripts/chunk_docs.py`, `scripts/generate_qa.py`)
  exists and was left alone.
- **Waiting for the Owner before E2 (the plan's decisions, each with its
  recommendation):** L1 the 4-bit test one-sided (recommended) or two-sided; L2 the model
  patch step in the E3 measurement (recommended: no, inline marks only); L3 strict answers
  in the sealed reading (yes); L4 the second judge's cross-check on 24 items (keep);
  L5 full precision on the judged tier (no); L6 what the 60 s bar is measured on and the
  answer limit (report the tiers apart; a lower limit as its own item); L7 redraw the 7
  tagged hidden valid-but-wrong items or tag only; L8 the E3 reading while E2's
  mechanical runs still go (yes). And the go for the run itself, after the chain's dry
  run and a review.
- 2026-11-20, 04:00 to 09:15: the E2 chain was built (`f78eac7`), reviewed
  (`runs/m4/reports/e2_chain_review.md`: go after six fixes; two progress lines would
  have put per-item results of hidden items into plain logs) and fixed (`bb50bef`,
  `92b2a74`, `fec92a6`, `e6ab705`), with a rehearsal mode (`b3db92b`). **The rehearsal
  passed** (06:51 to 09:10, 2 h 18 min; `runs/m4/reports/e2_rehearsal_run.md`): every
  hidden stage against the real server on 26 open items encrypted with a throwaway key,
  no defect; the 10 gate answers byte-identical to S6's; median seconds per mechanical
  answer 29.0 (4-bit default), 13.2 (strict, replayed packs), 10.4 (closed book), 50.3
  (full precision), 18.7 (full precision, replayed), on 20 items; 76 s per judged answer
  on 6; 25 to 27 s per judge call (48 calls), 684 and 974 s for the second judge (2
  calls). Not exercised: resume after a failure, the real hidden files, real readings.
  The revise step: the second development run and reading (B1, fitted items): net +0.3
  counted errors per 10 in default, 0.0 in strict, one accepted patch harmful (it deleted
  the flag that limited a reset to one partition); so R2 stays off (L2).
- **Exposure, 2026-11-20.** The conductor created a notes file for the Owner's review of
  his hidden questions; when the Owner saved it, the harness showed the conductor its
  content. The conductor has therefore seen the Owner's notes (causes, points, parts of
  reference answers) on eleven hidden judged items: oq-002, oq-010, oq-012, oq-018,
  oq-026, oq-032, oq-036, oq-041, oq-042, oq-047, oq-048. No model saw anything and no
  look was used. Nothing from those notes is used in any decision or brief. The Owner
  decided to keep and tag them (see "Decisions"). From now on the conductor creates no
  file meant for hidden content.
- The Owner's questions after his review: sealed sessions applied his notes (all eleven
  applied as written, none contradicted by a pinned source), reviewed them, and the
  independent sealed count gives 120 of 125 points (96.0 %; partial oq-036 m1, oq-041 m1,
  oq-049 m3 and m4, oq-050 m2). Final files `tier_b_owner_{items,rubrics}_final5` (25
  items; oq-010 is a different family from oq-002). **The hidden judged half has 61
  scenarios.**
- 2026-11-20, 09:15 to 11:15: done before the go for E2: the hidden reports print totals
  with and without tagged items (`f12324b`); the 7 hidden valid-but-wrong items redrawn
  and checked a fourth time, with three fixes (`00aefe4`, `6e51062`: hidden tags now 7,
  5 safe order and 2 works controls, no hidden fault tagged; open file unchanged); the
  chain pins the content of the files a run depends on, not the head commit, takes the
  Owner's decisions as defaults and resumes across a documentation commit (`e8bb40a`);
  the two ledger rows are written. Full suite on the clean tree: 1,626 tests, OK. **The
  shape check on the real hidden files passes** (key names and counts only; 245 + 60
  mechanical items, 25 + 36 judged scenarios; all through the stub answer step, judge,
  bundles and scorer): it first stopped on two unread text keys of the judged items,
  `side` and `adaptation`, which a sealed session labelled metadata (`49e0713`).
- 2026-11-20, 11:20 to 16:55: **the Owner gave the go; the E2 run is under way.** A first
  attempt (11:20) passed its open stages and stopped at the hidden gate at 13:43, before
  any hidden item was run: the hidden-text scan matched two plain files of the open
  stage. Shown to be a false alarm in counts only: the matching answers file was
  finished 32 minutes before anything hidden was first decrypted, so the model had
  written a line that also stands in a hidden item. The scan now drops markers that are
  public text and freezes the open stages' outputs at the gate (`48f98a9`); the first
  attempt's folder, manifest and logs were renamed aside. The fresh run started at about
  15:00; identity check 60 of 60 byte-identical to S6; **the hidden gate opened at 16:55
  (14 conditions; 1,813 text markers kept, 439 dropped as public) and the first hidden
  stage runs. Looks used: 2 of 6 (E2 and E3).** Known flaw, not fixed during the run: the
  chain reuses a stored manifest and stage markers without checking them at the start.
- 2026-11-20, evening: **E3 is measured on the hidden judged half: NOT MET.** The judged
  answers (61 default, 61 strict) were finished at 19:19; eight sealed reader sessions
  read the 122 answers blind to the mode, 32 of them twice, and returned row counts and
  hashes only. Totals (`work/m4/e2/reading_totals.txt`, totals only): default mode, 105
  non-minor wrong, invented or harmful statements in 61 answers = **17.2 per 10 (90 %
  interval 14.4 to 20.0) against the bar of 2.0; 13 harmful (bar 0)**; 65 minor ones
  beside it. The two readers found 84 and 86 on the shared answers (2 % apart; the
  reading is used). Strict mode: 14.6 per 10 (12.1 to 17.1), 11 harmful; strict minus
  default −2.6 per 10 (−5.6 to +0.3). Essential omissions: 26.2 and 26.7 per 10. Without
  the 11 tagged items (50 answers, for information): default 15.2 per 10, 8 harmful. The
  model patch step was off (L2). Three rows were not counted (quote not found). Plain-text
  copies of bundle rows that the harness saved for over-long reader outputs were deleted
  (the Owner's standing decision); later sessions printed rows field by field. The
  mechanical stages of E2 continue.
- 2026-11-21, 12:31: **E2 is done** (`runs/m4/E2_STARTING_POINT.md`, totals only; hidden
  gate 2026-11-20 16:55 to the end, 19 h 36 min; every answer stage without an error;
  the session restarted once during the run, which the chain did not notice). Hidden
  mechanical tier, 305 items, all checks pass: 4-bit default 247 (81.0 %), 4-bit strict
  231 (75.7 %), closed book 161 (52.8 %), full precision 252 (82.6 %), full precision on
  the 4-bit run's excerpts 253 (83.0 %). **Rule 1(a), the cost of 4-bit: 4-bit minus full
  precision −1.64 points, 90 % interval −5.10 to +1.90 (235 clusters); the one-sided
  reading needs the lower end above −5, so by the letter it is NOT passed, by 0.10.**
  With the same excerpts −1.97 (−4.92 to +0.97). **Rule 1(b): default stays the reference
  mode** (strict minus default on the mechanical items −5.25, −8.54 to −2.01, not met;
  judged coverage −0.018, met; reading met). Starting figures, deciding nothing:
  retrieval against closed book +28.2 points on the mechanical items and +0.17 judged
  coverage (0.55 against 0.38; +0.11 to +0.23) on 61 scenarios; the new mechanical kinds
  (60 items) 76.7 % against 82.0 % for the older kinds (245); the second judge agrees on 70
  of 72 essential units (24 answers). Identifier check, nonexistent identifiers per 10
  answers: mechanical 0.49 default, 0.13 strict, 2.07 closed book; judged 2.62 default,
  0.33 strict, 2.95 closed book. Seconds per answer, median: mechanical 24.9 (4-bit
  default), 48.2 (full precision); judged 81.8, with 36 of 61 default answers cut at
  2,048 tokens. The scoring stage was run a second time to add the reading to rule 1(b)
  (the same look, L9).
- 2026-11-21, afternoon: the plan for the bake-off (`runs/m4/reports/e5_plan.md`): an open
  screen of three candidates without a look, then at most two finalists in one hidden
  look, each compared with the reference's stored E2 answers; a switch only on measured
  superiority and no loss elsewhere; about 19 to 30 h of the model server and about 12
  sealed reader sessions (inferred from 3 questions per model). Checked: every
  candidate's chat template keeps the system prompt in its own turn; the thinking
  switch works for GLM and Granite and changes nothing for Gemma
  (`runs/m4/reports/e5_t4_templates.md`). The Owner decided the licence points (see
  "Decisions"). Running: the first-words timing on open items (E4); the decision rule by
  code. A flaw found in the reading totals: the pooled disagreement of the two readers
  netted signed differences (printed 0.00; 0.62 per set); it does not change rule 1(b).
- 2026-11-21, afternoon: the Owner decided M1 and N1 to N7 (see "Decisions"; the error
  rule of the switch is tightened). Started: the screen's chain (`scripts/m4_e5_bakeoff.sh`,
  stage SCREEN) and the amended rule by code. The screen starts when the chain passes
  its dry run and the first-words timing has freed the model server. Nothing waits for
  the Owner.
- 2026-11-21, 22:45: **the open screen of the bake-off is done: no candidate goes on, so
  no hidden look is spent** (`runs/m4/reports/e5_screen.md`; open half, 339 mechanical
  items and 60 judged scenarios per candidate, the reference's stored answers re-scored
  and re-judged with the same code; 360 judge calls, none unscored). Reference: 255 of
  339, judged coverage 0.563, median 86.0 s per judged answer. **Gemma 4 26B-A4B:** 247 of
  339 (−2.36 points, inside the −4 limit), coverage 0.463 (−0.100, limit −0.08: dropped
  on S3), 24.6 s per judged answer, 15.26 GB loaded alone, 20 of 20 check answers
  reproduced, no answer cut at the limit (median 1,150 tokens), 0.8 nonexistent
  identifiers per 10 judged answers against the reference's 3.5 (identifier check, not
  read). **GLM-4.7-Flash:** 202 of 339 (−15.63), coverage 0.214 (−0.349), 6 of 20 check
  answers not reproduced, 16.48 GB: dropped on S1, S2 and S3. **Granite 4.2 30B:** median
  100.4 s per judged answer against the limit of 98.9 s, 51 of 60 answers cut at the
  limit: dropped on S4 after its judged run; its mechanical run was not made. Events:
  the server reloaded its settings at 17:20 and unloaded GLM under the running job; the
  resume refused because the reloaded model carries a new load stamp, and the chain went
  on to the next step instead of stopping (both to be fixed before any further hidden
  run); GLM's mechanical run was made again from the start.
- 2026-11-21, 23:00: the Owner decided E5: keep Qwen3.8-27B at 4-bit. All five exit
  criteria are now measured or decided.
- 2026-11-21, 23:45: the Owner closed M4 with E3 and part of E4 recorded as not met,
  chose v3.1.0 as the release, and set the next milestone's targets (see "Closing
  state"). The sealed sessions' transcripts were deleted (27 sessions). In progress: the
  release preparation, an identity check of the default path on open items, and the plan
  for the next milestone.
- 2026-11-21, 14:25: **E4 measured on open items through the streamed service**
  (`runs/m4/reports/e4_first_words_run.md`; 4-bit reference, default mode, 120 questions,
  no error). Judged scenarios (60, B1 and B2): complete answer median 90.3 s (90th
  percentile 96.8), first words median 22.9 s, 39 of 60 answers cut at 2,048 tokens: both
  bars missed. Mechanical items (60): complete answer median 26.1 s (35.5): the 60 s bar
  is met; first words median 19.2 s: missed; none of the 120 questions had its first
  words within 10 s. Caveat: another client's model was loaded on the server during the
  run; if it generated at the same time, these times are too high (not known). The
  bake-off's rule by code and the screen's chain are built and committed (`f1094ab`,
  `7dc1de8`; full suite 1,694 tests, OK); the open screen was started after this run.
- Earlier: **the go for the E2 run was asked** (two hidden looks, E2 and E3; about
  19 to 22 h of the model server by the rehearsal's rates, plus about 2 h for the
  first-words timing on open items; about eight sealed reader sessions).
- Waiting for the Owner, not blocking: redraw the 7 tagged valid-but-wrong hidden items
  or keep the tag only; the review lists above. The Owner sees the failed R3 check here;
  nothing further runs in WP5.
- Next: B2 answered on the server (default and strict, judge, identifier check, the R3
  rule, a first look at the revise step), then its blind readings; the a3 rebuild as
  version 2 with a second review, then T10; the revise step's development run on B1. For the Owner, not blocking: the essential points of
  the six architecture scenarios (b1-10, b1-18, b1-19, b1-20, b1-29, b1-30). Not blocked:
  D1, the mechanical tier's hidden-half runs.
