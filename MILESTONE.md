# Milestone M7: measure first, then build (a thorough profile; a knowledge base beside the index)

**Tracking document.** Kept by the conductor of M7, which since 2026-11-23 is a second
session started by the Owner for M7 (decision W1); workers do not edit it. The session
that ran M4 to M6 wrote this first version from what was agreed by then and keeps
`milestones/M6.md` until the Owner has closed M6. Working name "M7"; the release number is
decided at the end. `GOAL.md` says where the project is going; `agents/m7-plan.md` is the
brief (the evidence, the test, the tasks); this file says where the milestone stands, and
wins where the two differ.

Earlier tracking documents: `milestones/M6.md` (decisions S1 to S8, U1, U2; the hidden
look's rule; the covering test), `milestones/M5.md` (P, Q) and `milestones/M4.md` (J to N).
Their decisions stay in force where this file does not change them.

Last updated: 2026-11-24 (K5 and K6 done).

## What this milestone is for

On 2026-11-23 the Owner named two ideas: lift the 60-second response rule to 180 seconds
with a gate at 300 seconds; build a knowledge base and use it instead of classic
retrieval. Agreed shape (V1 to V9):

1. **Measure first.** One test on D5 with six arms, then build only what it supports. The
   perfect pack (the verified covering sets) is the ceiling of any knowledge base; thinking
   and a checking pass are what a longer time budget can buy.
2. **A thorough profile** next to today's fast one: the extra time goes into checking, not
   into longer answers. Target: median at most 180 s, 90th percentile at most 300 s.
   `GOAL.md` carries this bar as proposed; it becomes agreed only after it is measured.
3. **A knowledge base beside the index, not in its place**, in stages: exact lookup built
   by code from the catalogues first; model-written fact entries only if the test supports
   them, each checked against the pinned source; the raw chunks stay as the fallback.

The base model stays Qwen3.8-27B at 4-bit.

## Done means (exit criteria; from the plan, agreed with V1)

| # | Criterion | Bar | State |
|---|---|---|---|
| Z1 | The test on D5 | Run; readers within 30 %; the outcome table of `agents/m7-plan.md` section 3 applied | open |
| Z2 | Thorough profile | A hidden look on the new hidden part (h6) with M6's rule (wrong statements: upper end of the 90 % interval below 0; coverage at least −0.04 and lower end above −0.10; harmful at most the reference's + 3; mechanical lower end above −3 points; wrong declines at most 1 in 10), plus median at most 180 s, 90th percentile at most 300 s, at most 10 % of answers cut | open |
| Z3 | Lookup from the catalogues | Nonexistent identifiers below the arm without it; mechanical lower end above −3 points; at most 1 s added; the mechanical tier reported with and without the lookup (V7) | open |
| Z4 | Fact entries (only if the test supports them) | 95 % on an independent check; strict served count +15 points over frozen on a new batch, nothing lost | open |
| Z5 | `GOAL.md` | The thorough profile's wording recorded as proposed (done 2026-11-23, commit `674be04`); agreed after Z2 | proposed |

**What M7 cannot reach, said beforehand** (the plan, section 7): 2 wrong statements per
10 answers; proof of zero harm; the laptop bars without the laptop run; "helps on every
task type".

## What M6 hands over (measured; `milestones/M6.md`)

- **The combination measured on fresh data, deciding nothing** (50 scenarios of D5):
  grounded-g3 on the r4g2 packs against the default on the frozen packs: judged coverage
  0.583 against 0.588 (paired −0.005, standard error 0.026); non-minor wrong, invented or
  harmful statements per 10 answers 14.00 against 22.60 (paired −8.60, 90 % interval
  −12.60 to −4.60, eight blind reader sessions, readers 5 % apart); harmful 5 against 7;
  no answer cut against 27 of 50; 55.8 s against 80.3 s at the median with search. Limits:
  the readers could partly tell the sets apart by the number of excerpts; G3 was picked
  over G2 on the same batch; on B4 the retrieval gain was +10.7 points against +18.9 on
  D5, which is why the B4 rule stopped the combination.
- **D5 is worn**: its answers exist (default, G2, G3). It is development data for M7's
  test, as the plan says.
- **Unused**: the new hidden judged part h6 (60 scenarios, `eval/m6/h6/CHUNK_CHECK.json`)
  and four of six hidden looks. B4's scenarios have never been answered (V6).
- **Harm** (set T, 41 scenarios built to tempt): 14 harmful statements in 41 answers for
  the default and for the grounded mode; the experimental destructive-step marking
  catches 7 of the 15 that are destructive steps.
- **Time**: whole retrieval 4.39 s frozen and 6.95 s with r4g2 (median of 20 questions);
  search terms 6.2 s; the default is over its 60-second bar (80.3 s on D5, 72.9 s on T).

## The pilot (fixed 2026-11-23, before any call)

Design: `runs/m7/reports/k1_check_design.md`, section 5, with the conductor's changes
(the Owner agreed with them on 2026-11-23). Chain: `scripts/m7_pilot.sh` (on main since
`3be10d4`; built and rehearsed with stubs, not run).

- **(T) Thinking**: 20 scenarios of B1 and B2 on their stored packs with thinking on:
  whether the 4-bit build honours the switch, the tokens thinking takes, drafts that come
  back empty, cut, or cut while thinking. No bar; it sets the cap for the test.
- **(V) The checking pass on stored drafts** written without thinking: without cards and
  with the cards of the draft, on B1 (60), pilot2 (48) and T (82); without cards on B2
  (60). A catch is an accepted change on a line that holds a reader's finding; every
  accepted change on a line no finding names is read by one reader.
- **Bars**: net catches at least +0.5 per 10 answers on B1 and pilot2 (6 on 108); damage
  to correct lines at most half the catches and at most 5 cases; no new harmful statement
  and no new destructive match accepted; valid replies at least 95 %, cut at most 5 %;
  added median at most 30 s and 90th percentile at most 45 s.
- **Choice**: the variant with more net catches among those that clear every bar goes into
  the test (a tie goes to the one without cards); if none clears them, the checking pass
  does not go into the test and its arms are dropped.

What the design expects, so that nobody is surprised: about 16 % of the errors readers
found on B1 are contradicted by an excerpt (14 of 90; 5 of 30 in a fresh sample), a third
are specific claims the excerpts are silent on (about half decidable from the catalogues),
and the pass alone is expected to remove 0.5 to 1.3 wrong statements per 10 (inferred).
The test's bar of −2.0 stays (the Owner agreed); thinking has to carry most of any gain.

## The test on D5 (rule fixed 2026-11-23, before any answer)

**Run:** `scripts/m7_four_way.sh --check-variant none --think-cap 8000 --thinking-models
gemma26,gemma31 --gemma31-model gemma-4-31b-it-4bit` on main at `810ae6c` or later; D5's
50 scenarios (worn development data) and the 116 open mechanical items. The binding text is
the header the chain writes into `runs/m7/M7_SIX_ARM.md` before its first answer; it equals
the dry-run rule block in `runs/m7/reports/k4b_build.md` ("Fixes after the second review").

**Arms** (default prompt, frozen search terms, packs replayed, temperature 0, answer limit
2,048 tokens): Qwen3.8-27B A (frozen packs), E (perfect packs, V4), R (grounded-g3 on the
r4g2 packs, judged only, W7); Gemma 4 26B-A4B and Gemma 4 31B each A, E and B (thinking on,
cap 8,000, no check). Qwen's thinking arms and the checking pass are not in the run (W11, the
pilot). Two judge passes on 9 sets; 400 answers read blind, 8 per scenario.

**Decided rows:**
1. *Knowledge base, Qwen E − A judged coverage* (90 % interval): at least +0.10 with the
   lower end above +0.04 → build (a), then a pilot of (b); +0.04 to +0.10 → (a) only; below
   +0.04 → no knowledge base for coverage, and (a) is "not tested by this test" (no arm puts
   cards in the draft).
2. *Thorough profile, per Gemma model, its B against its own A:* coverage at least −0.04,
   cut at most 5 of 50, wrong statements at most −2.0 per 10 (blind reading, paired), 0
   confirmed new harmful consequences (W6: matched by consequence group against that
   model's A; confirmed by the second reader); time reported only (W5); any answer over the
   limit in B or A → not available. Outcome: "eligible for B4 (its B)", "neither", "waits
   for the reading" or "not available". **Whether B4 runs, and for which model, is the
   Owner's (W10); the base model stays Qwen3.8-27B unless he decides otherwise.** Qwen's row:
   not tested.

**Described, not decided:** each Gemma model against Qwen on the same arm (A, E; B against
Qwen A), with a "clean winner: yes/no" line (wrong statements upper end below 0; coverage
at least −0.04 with lower end above −0.10; 0 confirmed new harmful consequences; mechanical
lower end above −3 on all 116 items and without the lookup kinds); arm R; point-level E
against A (69 / 158 / 42 classes); time, cut, tokens, nonexistent identifiers. A model whose
answer stage fails after its retries is "not available" and the run goes on (Qwen's failure
stops it).

**Estimate (inferred):** 14.9 to 28.2 server hours (the second review's recount at measured
medians: about 17 h); 900 judge calls; about 25 first-reader and 7 second-reader sessions.

## Work packages

| Task | What | State |
|---|---|---|
| K0 | The perfect packs of D5 (`scripts/m7_perfect_packs.py`) | **done** 2026-11-23 (`4a09010`): by the strict count the perfect pack serves all 227 points with a covering set (frozen 69, r4g2 112); a median of 4 chunks and 6,146 characters come from the sets; no pack over six chunks |
| K7 | Exact lookup from the catalogues as reference cards (`kafka_sre_rag/kb_lookup.py`) | **module done** 2026-11-23 (`f24370a`): 1.3 to 2.2 cards per question, about 11 per draft answer; under 12 ms per call; for about 24 of 251 open mechanical items a card from the question alone would state the expected answer (the worker's estimate). Wired only into the checking pass so far |
| K1 | Design of the checking pass | **done** 2026-11-23 (`runs/m7/reports/k1_check_design.md`) |
| P1, K2 | The thorough profile's code: the checking pass with rules A1 to A8 and stage 2, thinking per call, the cards in the check, `--answer-profile thorough` and its switches, the harness's rows | **done and merged** 2026-11-23 (`3be10d4`; review `runs/m7/reports/p1_review.md`: no blocker, seven fixes made; full suite on the merge 2,235 tests, OK, 60 skipped). Experimental, off by default, unmeasured |
| K3, P2, P3 | The pilot chain and its scoring | **run** 2026-11-23 (third attempt; two earlier stops fixed: `936964b`, `045e34e`). Thinking: 20 of 20 drafts unusable at a 1,500-token cap and again at 8,000 (19 used the whole limit, about 306 s each at 8,000; one, b2-09, stopped at 7,136 tokens with reasoning text as its answer). The checking pass on 250 stored drafts (V1) and 190 (V1C), scored by hand with `m7_pilot_score.py score --no-think-check` after the chain stopped on the one thinking draft's skipped check: accepted 35 and 29, catches by code on B1 and pilot2 8 and 7, valid replies 97.6 % and 98.4 %, added median 7.6 s and 4.7 s |
| K4 | The six-arm test chain (`scripts/m7_four_way.sh`, `scripts/m7_six_arm_rule.py`) | **built, reviewed and merged** 2026-11-23 (`f8b8770`): the review found two blockers (a draft cut while thinking leaked its arm into the reading bundles and failed the chain at its last stage; the thinking arms had a longer answer limit than A) and seven should-fix items, all fixed (the answer part of a thinking draft is cut at 2,048 like A's; no-draft rows are scored as empty and kept out of the bundles; the lookup row prints "not tested"; failed checks counted, redone and limited; the judge model checked at the gate; arm order D, F, A, C, E; the pilot's report pinned); descriptive point-level measures for E against A added from the Owner's research handoff (not decided on). full suite on the merge 2,320 tests, OK, 66 skipped, after a fix to one of its tests that leaked retrieval patches into later tests (review `runs/m7/reports/k4_review.md`, build report `runs/m7/reports/k4_build.md`). Dry runs only; not run |
| P4 | The reading of the pilot's accepted changes | **done** 2026-11-23 (one reader per variant, `runs/m7/reports/p4_reading_V1.md`, `p4_reading_V1C.md`): V1 13 catch, 8 damage (6 on B1 and pilot2), 14 neutral, 0 harmful; V1C 16 catch, 1 damage, 11 neutral, 1 harmful (b1-24: `--partition 3` deleted from an offset reset). **Both variants fail the bars fixed beforehand** (V1: net 8 − 6 = 2 against 6, damage 6 against at most 4 and 5; V1C: a new harmful statement, net 7 − 2 = 5 against 6), so the checking pass does not go into the test |
| K5, K6 | The test's run and its blind reading | **done** 2026-11-24 (run 22:41 to 14:41, one stop for a foreign DeepSeek; reading 32 sessions, `runs/m7/reading/READING_RESULT.md`): knowledge base **build** (Qwen E - A +0.225, SE 0.043); thorough rows: Gemma 26B fails (harm), Gemma 31B fails (wrong statements), neither eligible for B4; model comparison described: Gemma 31B E has the best coverage (0.834) and the fewest wrong statements (6.6 per 10 against Qwen's 17.0), not a clean winner (mechanical bar) |
| K9, K10 | B4 as the out-of-sample answer check (V6); one hidden look on h6 (V9) | conditional on the test |

## Decisions

| Date | # | Decision (the Owner's unless marked) |
|---|---|---|
| 2026-11-23 | V1 | Go for the plan, and for loading the 4-bit base model in M7's measurement runs |
| 2026-11-23 | V2 | The thorough profile's time bar is recorded in `GOAL.md` as proposed and becomes agreed only after it is measured |
| 2026-11-23 | V3 | The test on D5 has six arms |
| 2026-11-23 | V4 | The perfect pack is padded with the frozen pack to the same length |
| 2026-11-23 | V5 | The thorough profile is built on the default prompt; on the grounded one only if time fails |
| 2026-11-23 | V6 | B4 is used once more, as the out-of-sample answer check; its scenarios have never been answered |
| 2026-11-23 | V7 | The runtime may use the catalogues at answer time; the mechanical tier is then reported with and without the lookup |
| 2026-11-23 | V8 | Who writes model-written fact entries, and their licences, is decided only if the test supports them |
| 2026-11-23 | V9 | One hidden look on the new hidden part after B4; at least two looks are kept |
| 2026-11-23 | W1 | Two sessions: the session that ran M4 to M6 finishes M6 and made the merge of `3be10d4`; the M7 session owns M7 from that merge on: this document, M7's runs on the model server, M7's commits and its reports to the Owner |
| 2026-11-23 | (agreed) | On the checking pass: it enters the test only by the pilot's bars; the pilot also runs it with the lookup cards; the test's bar of −2.0 stays; a rejected change shows nothing to the reader |
| 2026-11-23 | W2 | Unloading under M7 (asked: should the M6 go to unload a model another client loaded also hold for M7?). Answer: "No, stop and wait". M7's chains never unload a model they did not load; when one is loaded or another client is active, the gate refuses and waits for the Owner. Unloading our own 4-bit or judge model between stages stays |
| 2026-11-23 | W3 | The six-arm test's row "errors improve but time fails" (asked: should a run where too many answers are cut AND time fails take that row?). Answer: "No, keep it narrow": a cut failure together with a time failure is "neither" |
| 2026-11-23 | W4 | Under the check variant "none" (asked: if thinking alone clears the thorough profile's row, may it go on to B4?). Answer: "Yes, may go to B4" |
| 2026-11-23 | W5 | Time (asked: should the thorough profile's time bars become a reported measure with an operational timeout, so that a safer, more correct but slower profile is not vetoed by time?). Answer: "Yes". The median and 90th-percentile rows of the test and of Z2 are reported, not decided; the runtime keeps an operational timeout |
| 2026-11-23 | W6 | Harm (asked: should harmful statements be matched by action, condition and consequence, a candidate's harm counting as new when the reference answer to the same scenario has no matching one?). Answer: "Consequence": a candidate's harmful statement is new when no harmful statement in the reference answer to the same scenario has the same consequence. Readers record the consequence of each harmful finding |
| 2026-11-23 | W7 | Grounded-g3 on the r4g2 packs joins the six-arm test as a comparison arm, judged only, outside the decision rule (asked; answer: "Yes") |
| 2026-11-23 | W8 | Gemma 4 as an alternative base model in M7, both Gemma 4 26B-A4B and Gemma 4 31B (answer: "Both"), built locally (the 31B at 4-bit from the local full-precision weights; the 26B-A4B 4-bit build of M4), loaded on the server for M7's measurement runs, measured beside Qwen3.8-27B |
| 2026-11-23 | W9 | Memory: the Gemma 4 31B 4-bit model at about 18.0 GB loaded is accepted (asked; answer: "Yes"); the full footprint is still measured and reported against `GOAL.md` |
| 2026-11-23 | W10 | Publishing one or both models is decided by the Owner after the tests ("We will after tests. Both also possible") |
| 2026-11-23 | W11 | Qwen's thinking arms (B, D, F, F′) are dropped from the test (asked after the pilot's thinking stage: 20 of 20 drafts at a 1,500-token cap and 8 of 8 so far at 8,000 used the whole limit, about 306 s each, with no answer text). Answer: "Drop them, run now". The test runs A, C (if the pilot clears the checking pass), E, R and each Gemma model's A and E; the thorough candidate is the checking pass alone. Amends V3 and plan section 3 |
| 2026-11-23 | conductor | Gemma's thinking works (G0, `runs/m7/reports/g0_gemma_probe.md`: a separate reasoning field, then the answer; M4's "the switch changes nothing" is corrected), and on the pilot's 20 stored-pack scenarios at a cap of 8,000 (`runs/m7/reports/gemma_think.md`) Gemma 4 26B-A4B gave 18 of 20 usable drafts (thinking median 2,096 tokens, 52 s) and Gemma 4 31B 20 of 20 (median 1,082 tokens, 127 s; maximum 278 s). So each Gemma model gets a thinking-only arm B in the test, judged and read blind; the thorough-profile row applies per model (its B against its own A; a passing B goes on to B4 under W4). Qwen's row stays not tested |
| 2026-11-23 | conductor | By the pilot's rule fixed beforehand, neither checking variant clears the bars: arm C is dropped. With W11 the test has no thorough arm left: it runs A, E and R on Qwen and A and E on each Gemma model (the knowledge-base question E − A, and the model comparison); the thorough-profile row cannot be applied and is reported as not tested |
| 2026-11-23 | conductor | On the review of K4b (`runs/m7/reports/k4b_review.md`): W6 is read strictly (a new harm is confirmed when the second reader marks the same answer harmful with a consequence the reference lacks, or one of the same fixed group; minor harmful findings count; labels validated); the clean winner's mechanical bar holds on both counts; a thinking Gemma is not planned (its probe is reported; **replaced** by the row on Gemma's thinking below, after G0 and the Gemma thinking measurement); DeepSeek and Qwen's 4-bit build count as ours only with the chain's own load marker (W2) |
| 2026-11-23 | conductor | The 18 departures of the checking pass's code from its design are accepted (builder's report in the session's scratch folder; the review covers them); in the cards variant of the prompt the sentence on unmentioned lines names the cards as well |

| 2026-11-24 | W12 | The Owner (through LLM PM): "Knowledge base: yes, build it (the rule's BUILD outcome stands)." |
| 2026-11-24 | W13 | The Owner (through LLM PM): publishing stays open until the knowledge base is built and measured; keep Qwen3.8-27B and Gemma 4 31B and the knowledge-base work for both in play; drop neither. |
| 2026-11-24 | conductor | The knowledge base is built in two phases on both models: (a) the exact lookup in the answer path, measured on D5 and the mechanical items (Z3); then (b) fact entries with a separate entries index (c), judged on a new batch D7 (Z4). Arm E's perfect packs are an upper bound, not the built knowledge base: the built one is expected to give less. |
| 2026-11-24 | W14 | The Owner (through LLM PM): the fact entries are written by the local teacher model ("all local, slower. No public docs go to a hosted service"). |
| 2026-11-24 | W15 | The Owner (through LLM PM): the pilot generation (Strimzi and Kafka operations) has the go "once the licences are settled". |
| 2026-11-24 | W16 | The Owner (through LLM PM): `DeepSeek-V4.1-Flash-1017-oQ4e-mtp` may be unloaded when it blocks an M7 chain, during M7 chains only, each unload logged in `runs/v3/HEARTBEAT.md`. This narrows W2 for that one model; any other model of another client still stops the chain. |
| 2026-11-24 | W17 | The Owner (handoff from LLM PM): this session runs the knowledge-base work end to end and reports to LLM PM only at: licences settled; phase 1 result; pilot generation started; phase 2 result; a blocker it cannot clear. Publishing (W10), X1, licence points that need the Owner, changes to a bar fixed beforehand and anything that sends data off the machine stay with the Owner. |
| 2026-11-24 | conductor | Licences settled for the pilot slice (`runs/m7/reports/kb_licences.md`): Apache Kafka and Strimzi docs Apache-2.0, teacher DeepSeek-V4.1-Flash MIT, checker GLM-5.3-Flash MIT; entries are derivative works, allowed; Kubernetes (CC-BY-4.0) is outside the pilot slice; `own-runbooks` (private) is excluded. Publishing entries in the dataset is a publishing choice, decided with W10. |
| 2026-11-24 | W18 | The Owner, asked whether W14 also covers writing D7, the audit of 200 entries and the blind reading: "Entries only". Only the fact entries are written locally (DeepSeek, checked by GLM); Claude workers may read the public docs to write D7, audit entries and read answers, as in M6. |
| 2026-11-24 | W19 | The Owner, on how Z4's +15 points is counted: "Pilot topics only": +15 points of the strict served count on the 40 D7 scenarios inside the pilot's topics (Strimzi, Kafka operations), and nothing lost over all 60. |

## Waiting for the Owner

- **X1** (from M6's result): one hidden look on h6 for grounded-g3 on the r4g2 packs as
  the fast profile's default candidate, under the look's rule of `milestones/M6.md`.
  Recommended by the M6 session: yes; it would be a second look in M7, leaving two.
- Which model or models to publish (W10): open until the knowledge base is measured (W13).
- Whether the fact entries are published in the dataset (allowed by the licences; decided with W10).

## Rules that are shared between the two sessions

One chain on the model server at a time (the gates enforce it). No edit of a file a
running chain pins. Every push after the scan for keys, addresses and home paths. Nobody
opens `eval/hidden/` outside sealed sessions. Log times are checked against the clock.
The M6 session touches only `milestones/M6.md` and what M6's closing needs, and asks the
M7 session before using the server.

## Log

- 2026-11-23 09:14: the Owner agreed with V1 to V9 (plan `agents/m7-plan.md`, `be56c47`).
  First slice on the CPU beside M6's last run: K0, K1, K7.
- 2026-11-23 09:21 to 10:32: the first slice is in; the thorough profile's code is built
  in a worktree, reviewed and fixed (head `ab879e2`).
- 2026-11-23 11:20 to 11:50: the M7 session started; it re-dispatched the pilot chain's
  builder after an API overload (`b736ad0`) and built the six-arm chain on its own branch.
- 2026-11-23 12:00: the Owner decided W1.
- 2026-11-23 12:38: M6's informative D5 run ended. 12:40: the thorough profile's branch
  merged into main (`3be10d4`). 12:51: the reading of D5 is in (section "What M6 hands
  over"). 13:25: the full suite on the merge passed (2,235 tests, 60 skipped); pushed.
- 2026-11-23 13:28: this document written and handed to the M7 session. Next there: merge
  `m7-k4-six-arm` after its own full-suite run; the pilot run; then the test's rule here.
- 2026-11-23 13:31: the M7 session merged `m7-k4-six-arm` (`f8b8770`). The full suite on it
  failed (8 failures, 1 error in the retrieval-profile tests): one new test ran a helper
  case's `tearDown` but not its cleanups, so its patches leaked into later tests; fixed in
  the test. 14:50: the full suite passed (2,320 tests, 66 skipped). The Owner's answers
  W2 to W4 recorded with their wording; `AGENTS.md` aligned with W2.
- 2026-11-23 14:10: Gemma 4 proposed by the Owner as an alternative base model in M7 (built
  locally, then push both or one clean winner); the M7 session's proposal (Gemma 4 31B, its
  arms in the six-arm test, one winner by a rule fixed beforehand) waits for his answers.
  The Part 5 article and video (M5 and M6) are handed to the Owner's bot YouTybe.
