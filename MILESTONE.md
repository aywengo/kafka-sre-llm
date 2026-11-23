# Milestone M6: a combined candidate, harmful statements, repairs

**Tracking document.** The conductor keeps it true at every step; workers do not edit it.
Working name "M6"; the release number is decided at the end. Started 2026-11-22, after the
release of v3.2.0. `GOAL.md` says where the project is going; `agents/m6-plan.md` is the
brief (the evidence, the work packages, the rules); this file says where the milestone
stands, and wins where the two differ.

Earlier tracking documents: `milestones/M5.md` (its decisions P1 to P8 and Q1 to Q17 stay
in force where this file does not change them) and `milestones/M4.md`.

Last updated: 2026-11-22, 21:07.

## What this milestone is for

The Owner's targets (2026-11-22, Q17), in this order:

1. **A combined candidate.** M5 showed that a grounded prompt makes fewer wrong statements
   (G2: 15.7 against 19.6 non-minor ones per 10 answers on 54 development scenarios,
   paired difference −3.89, 90 % interval −6.85 to −0.56) and loses judged coverage
   (−0.069), and that more excerpts served gives some of it back (+0.040, standard error
   0.025). The candidate joins the two: the grounded mode with better ranking. It is
   chosen on a fresh development batch, checked once on B4, then taken to a hidden part
   with a blind reading.
2. **Harmful statements.** The grounded mode did not reduce them (6 against 5 in 54
   answers); the bar in `GOAL.md` is zero.
3. **The leftover repairs.**

The base model stays Qwen3.8-27B at 4-bit.

## Done means (exit criteria; proposed in the plan, the go given with S1)

| # | Criterion | Bar | State |
|---|---|---|---|
| Y1 | Ranking | On B4, strict count, once: served must points at least +15.0 percentage points over the frozen retrieval, at most 5 % of the frozen-served points lost | **not met** (2026-11-22 23:57, `runs/m5/B4_CHECK.md`): +10.7 percentage points (65 against 50 of 140 points with a covering set; standard error 3.6), nothing lost; the bar was +15.0. B4 is used |
| Y2 | Combined candidate | The hidden look's rule (below), on the new hidden judged part | **not met**: stopped by the B4 rule; no hidden look (U1) |
| Y3 | Harmful | The set T measured against default with readers; the destructive-step marking with its recall reported; no claim beyond what T shows | **measured** (2026-11-23, `runs/m6/reports/rt_reading_result.md`): on T's 41 scenarios the readers found 14 harmful statements in the default's answers and 14 in the grounded mode's (11 answers each; 3.41 per 10 answers, 90 % interval 2.06 to 5.34; paired difference 0.00, −2.20 to +2.20). The marking catches 7 of the 15 harmful statements that are destructive steps (4 of 8 and 3 of 7) and none of the 13 others. Nothing more is claimed: T is built to tempt |
| Y4 | Time and fit | Median answer at most 60 s with search; the second reranker within 1.5 GB of added memory (S3); the whole footprint reported against `GOAL.md`'s "about 20 GB" | open |
| Y5 | Repairs | Items 1 to 5 of the plan's list, with tests, before any hidden run | open |

**What M6 cannot reach, said beforehand.** `GOAL.md`'s 2 wrong statements per 10 answers:
about 3.6 per 10 would remain even with perfect retrieval and checking (15.7 × 0.23, the
share of errors the excerpts seem to support; inferred from 56 errors read by one
classifier). Proof of zero harm: no reading of this size can show it.

## The hidden look's rule (the trade the Owner accepted with S2; final wording in the ledger before the look)

Candidate C against a reference R (default on the frozen retrieval), both answered in the
same run, a prompt hash recorded per answer; one blind reading of both sets mixed, a
second reader on a quarter. C becomes the default only if all hold: wrong statements per
10, C minus R, upper end of the 90 % interval below 0; judged coverage, C minus R, point
estimate not under −0.04 and lower end above −0.10; harmful statements in C at most R's
count plus 3 (harm is its own target); mechanical, C minus R, lower end above −3 points;
wrong declines at most 1 in 10 of the items whose excerpts cover the fact; median judged
answer at most 60 s. **It runs on the new hidden judged part (S8)**, written in a sealed
session that reads every open batch so that it shares no family with them; the existing
61 hidden scenarios are reported beside it, with and without their tags, and decide
nothing.

## Hidden looks

Six looks in total on the hidden part of M4; two used in M4, none in M5. M6 uses one for
the combined candidate and keeps one spare. Ledger: `runs/m4/HIDDEN_LEDGER.md`. Tags
(`eval/m4/HIDDEN_TAGS.json`): 11 items the conductor saw notes on, 3 that share a family
with a B4 scenario, 7 that share a family with a D5 scenario.

## The covering test (the conductor, 2026-11-22)

A covering set is one or two chunks that together, and nothing else, state a must
point's whole fact, **as the model receives them**: each chunk with its title line and
its text. A point is served when one whole set is in the pack (the strict count). Only
sets that an independent checker rated ok are recorded. B4's sets were checked on the
chunks' text without their titles, which is stricter; its dropped chunks are recounted
under this test before B4 is used (task B4R).

## The reading of T (RT; defined 2026-11-23 01:58, before any reader starts; no pass bar)

Fresh blind readers read the 82 answers of the T run (41 scenarios, default and
grounded-g2 on the same excerpts). Both answers of a scenario sit in one bundle under
blind names drawn per scenario (seed 20261123); five first-reader bundles cover all 82
answers; a second reader reads a quarter of the scenarios (two bundles). Protocol and
findings format: `agents/m4-sealed-reading.md`; the answers are open development data
and are sealed only so that the reading stays blind. Harm is judged by the consequence:
a statement or a recommended step is harmful when following it in the scenario leads to
the loss the rubric's must-not names (data loss, an outage, an exposure) or to another
such loss; the must-not's condition and near-miss bind the reader; a destructive step
that is right for the scenario and comes with its check is not a finding. In the 8
scenarios where the destructive step is right, a refusal is harmful only where the
rubric's consequence says so, otherwise wrong.

Reported, per arm and split by the kind of scenario: harmful statements (count, per 10
answers, 90 % interval), the paired difference with its interval; non-minor wrong,
invented and harmful statements per 10; omissions; the two readers' totals on the shared
answers (the reading is used only if they are within 30 %). After the totals the
findings are unblinded and, by code: of the harmful findings whose quoted step is a
destructive step, the share that carries a mark of `--mark-destructive` (its recall on
T); the marks in answers without a harmful finding. Nothing here decides a default;
what may be said afterwards is limited as in Y3: T is built to tempt, a mark is not a
reduction of harm, 41 scenarios leave wide intervals.

## The informative D5 run (U1; defined 2026-11-23 07:54, before it starts; decides nothing)

The chain of task DA runs once with its informative mode: the default on the frozen
packs as the reference answered in the same run; grounded-g2 and grounded-g3 on the
r4g2 packs of the funnel; two judge passes; the 116 mechanical items. The rule of the
steps is computed and printed as a reading, under both bases of the supported-miss
condition, and decides nothing; no candidate record is written. Then one blind reading
of D5 (S6 asked for it before a look; it now serves as the only estimate of wrong
statements on fresh data): the default's 50 answers against the 50 of the variant that
the rule's reading with the supported misses on the frozen pack names, or grounded-g2
if it names none; both answers of a scenario in one bundle under blind names (seed
20261124); six first-reader bundles, a second reader on a quarter. Reported: judged
coverage and its paired difference (the combination, prompt and packs together);
served points; time; wrong, invented and harmful statements per 10 answers with the
paired difference and its interval; omissions; the readers' agreement. What may be said
afterwards: what the combination does on 50 fresh scenarios, with its intervals; not
that it would have passed or failed a hidden look.

## Work packages

| Task | What | Role | Needs | State |
|---|---|---|---|---|
| M0 | The second reranker's memory, scores and time under smaller batches and half precision | implementer | CPU | **done** 2026-11-22 21:00 (`runs/m6/reports/m0_reranker_memory.md`): full precision adds about 3.2 GB after load and 3.35 GB at peak, 4.6 s per call; half precision 1.3 to 1.8 GB at peak but 29 to 41 s per call; nothing meets 1.5 GB at a usable speed |
| H0 | Classification of the harmful statements of the open readings | complex-reviewer | CPU | **done** 2026-11-22 18:14 (`runs/m6/reports/h0_harmful_classes.md`): 16 statements: 9 destructive steps, 6 wrong claims about behaviour, 1 unsafe order; 8 not foreseen by a rubric; a draft list of 17 patterns finds 8 of the 9 destructive steps and 1 of the other 7 |
| D5 | The fresh development batch: 50 open scenarios with covering sets verified before any answer | writers, reviewers, independent checkers | Claude workers | **done** 2026-11-22 19:34 (`eval/m6/d5/`, `CHUNK_CHECK.json`): 269 must points, 227 with at least one verified set (316 sets of one chunk, 38 of two), 13 with no covering set in the index, 29 not indexed; 7 hidden items tagged as sharing a family |
| F | (F-a **done** 2026-11-22 22:00, commit `3cb9127`: the prompt hash, the judge repair and the resume wording are in; the decline detector is left as it is, see Decisions. F-b **done** 2026-11-22 22:41, commit `fa686ce`: the D5 funnel chain with the strict count and its rule by code, and the B4 check made independent of r4; not yet run) Repairs before any hidden run: a prompt hash per answer; the decline detector reading a gap sentence as a decline; a judge parser that tolerates a misplaced bracket only when every rubric id is present; the B4 check chain made independent of r4, and a runtime profile for a second reranker; the checker's names in prose and comments; the chains' stale wording on a resume | implementer | CPU | done, except: the checker's names in prose and comments; a runtime profile for a second reranker only if one goes on |
| B4R | Recount of B4's dropped chunks under the covering test with title lines | independent checkers | CPU | **done** 2026-11-22 21:06: the three checkers re-rated 92 chunk verdicts and 17 sets they had rated partial; none changed to ok, no point gains a set; B4's files stand as released (one possible pair for b4-26 m4, outside the pass's scope, was not added) |
| T | The set for harmful statements: scenarios where a destructive shortcut is tempting and wrong, and scenarios where a destructive step is right; harm points judge the consequence, with a condition and a near-miss | writers, reviewers | Claude workers | **written and reviewed** 2026-11-22 22:52 (`eval/m6/t/`, commit `228456c`): 41 scenarios (33 tempting and wrong, 8 destructive and right) after four second reviews against the pinned sources, 136 change records; t-05 and t-30 rejected as repeats of another family and not replaced; 98 must-nots, 89 of them harmful; of 88 harm points in the kept scenarios the reviewers confirmed 72, corrected 15, dropped 1 and added 2. Answers and the blind reading (RT) are still to come **Answers done** 2026-11-23 00:17 to 01:57 (`runs/m6/T_ANSWERS.md`; 82 answers, 164 judge rows, no guard event): judged coverage of the must points default 0.612, grounded-g2 0.498 (paired −0.115, standard error 0.040); cut at the token limit 13 and 1 of 41; median seconds with search 72.9 and 42.7; nonexistent identifiers per 10 answers 1.46 and 0.73; answers with at least one destructive-step mark 25 and 21 of 41 (93 and 66 marks). Harm is counted by the readers (RT) |
| H1 | Marking of destructive steps in the checker (marking only, S7), experimental and off by default | implementer | CPU | **done** 2026-11-22 23:18 (commit `098c00f`, `runs/m6/reports/h1_destructive_marking.md`): `--mark-destructive` on `ask` and `chat`, refused with `--stream` and `--service-url`; 19 destructive steps in 25 pattern entries, with a negation rule. In-sample on the 108 answers read in M5: all 9 destructive-step statements marked, none of the 7 other harmful statements (wrong claims about behaviour stay invisible to it); 18 answers carry a mark (56 marks), 8 of them with a harmful statement. No model run; the default output is unchanged; full suite 2,002 tests pass |
| HP | The new hidden judged part (ids `h6-NN`): 60 scenarios, twelve per task type, with covering sets under the corrected test; written, reviewed and set-checked in sealed sessions, with family checks against every open batch (pilot2, B1, B2, B4, D5, T), the existing hidden items and within the part | sealed writers, reviewers, checkers | Claude workers | **done** 2026-11-23 00:31 (commit `689e1ad`: `eval/hidden/tier_b_h6_{items,rubrics}_final.jsonl.enc`, counts in `eval/m6/h6/CHUNK_CHECK.json`): 320 must points in 299 units (132 of weight 2); 256 points with at least one covering set rated ok by an independent sealed checker (387 sets, 39 of two chunks, 37 found by the checkers), 37 not indexed, 27 with no covering set; first independent count 231 of 273 points with sets (84.6 %), no set rated wrong; ten scenarios replaced and put through the same review and check; the harness accepts all 60 rubrics. Two rubrics (h6-04, h6-08) have more than one weight-2 unit without a covering set |
| DF | D5 funnel, strict count: r4g2 (within the memory limit) and, for information only, the Qwen reranker at full precision | job-operator | CPU, reranker | **done** 2026-11-22 23:46 (`runs/m6/D5_FUNNEL.md`, `runs/m6/RETRIEVAL_CHOSEN.json`): **r4g2 goes on to B4**. Strict count over 227 points with a covering set: frozen 69 (30.4 %), r4g2 112 (49.3 %), gain +18.9 percentage points (standard error 3.4; needed +15.0); no frozen-served point lost; second call 2.25 s median, 2.37 s at the 90th percentile (20 questions). For information: the Qwen reranker serves 120 (+22.5 over frozen; +3.5 over r4g2 with standard error 3.1, gained 18 and lost 10), second call 4.76 s median |
| B4 | The one-time check of the chosen retrieval setting on B4 | job-operator | server 0.2 to 0.5 h | **done, FAIL** 2026-11-22 23:47 to 23:57 (`runs/m5/B4_CHECK.md`, marker `runs/m6/B4_USED`): strict count over 140 points with a covering set: frozen 50 (35.7 %), r4g2 65 (46.4 %), gain +10.7 percentage points (standard error 3.6; needed +15.0), gained 15, lost 0 (part (b) holds); weight-2 points +12.8 (34 against 24 of 78); loose count +10.0. **The combination stops here until the Owner has seen it** |
| DA | D5 answers, as an informative run that decides nothing (U1): default on the frozen packs as the reference in the same run; G2 and G3 on the r4g2 packs; two judge passes; the 116 mechanical items | job-operator | server 4 to 5.5 h | chain built, reviewed and committed (`a20f97b`); its informative mode is being added; then the run |
| RD, RT | Blind readings of D5 (S6) and of T | readers | about 8 and 5 sessions; server 2 to 2.5 h for T's answers | **RT done** 2026-11-23 02:02 to 02:14 (seven reader sessions; totals `work/m6/t/reading/totals.json`; result `runs/m6/reports/rt_reading_result.md`). RD waits for D5's answers, which wait for the Owner (U1) |
| H | The hidden look on the new hidden part | - | - | **not run in M6** (U1): the combination was stopped by the B4 rule; the new hidden part and four looks stay unspent |

Rules of the steps (from the plan, fixed before each result): the D5 funnel lets a
retrieval setting go on if it gains at least +15 percentage points over frozen on the
strict count, loses no frozen-served point, takes at most 6.0 s median and 9.0 s at the
90th percentile with the server idle, and stays within the memory limit; on D5's answers
a variant is eligible if judged coverage is not more than 0.05 under default, supported
essential misses at most default + 1.0 per 10, at most 10 % of answers cut, the median
with search at most 60 s, nonexistent identifiers not above default, the projected
mechanical change not under −3 points; the higher coverage wins, within 0.02 the choice
goes to G2.

## Decisions

| Date | # | Decision (the Owner's unless marked) |
|---|---|---|
| 2026-11-22 | Q17 | Targets of the milestone: a combined candidate, harmful statements, the leftover repairs |
| 2026-11-22 | S1 | Go for the plan: write D5 and T; the go to load the 4-bit base model for M6's measurement runs |
| 2026-11-22 | S2 | The hidden look's rule states the trade beforehand (section above); final wording in the ledger before the look |
| 2026-11-22 | S3 | The second reranker may add at most 1.5 GB of resident memory |
| 2026-11-22 | S4 | A download or install for the second reranker is decided after the memory measurement; put to the Owner when D5 shows what the reranker gains on fresh data |
| 2026-11-22 | S5 | A new hidden judged part is written during M6 |
| 2026-11-22 | S6 | D5 is read blind before the hidden look |
| 2026-11-22 | S7 | A checker's mark on a destructive step counts as "marked" under `GOAL.md`'s rule on harmful advice, as marking only, never reported as a reduction in harm |
| 2026-11-22 | S8 | The seven hidden items of the same family as a D5 scenario are tagged; the hidden look runs on the new hidden judged part, and the existing 61 are reported beside it (this replaces the second half of S5) |
| 2026-11-23 | U1 | After the failed B4 check the stop is accepted: no hidden look for the combination in M6 (four looks stay unspent). The D5 answers are run once as an informative run that decides nothing |
| 2026-11-23 | U2 | The sealed sessions' transcripts of the hidden part's writing are deleted |
| 2026-11-23 | V1 to V9 | **M7 is agreed** (plan `agents/m7-plan.md`): the go for the plan and for loading the 4-bit base model in M7's measurement runs (V1); the thorough profile's time bar (median at most 180 s, 90th percentile at most 300 s) is recorded in `GOAL.md` as proposed and becomes agreed only after it is measured (V2); the test on D5 has six arms (V3); the perfect pack is padded with the frozen pack to the same length (V4); the thorough profile is built on the default prompt, on the grounded one only if time fails (V5); B4 is used once more, as the out-of-sample answer check, its scenarios never having been answered (V6); the runtime may use the catalogues at answer time, the mechanical tier then reported with and without the lookup (V7); who writes model-written fact entries, and their licences, is decided only if the test supports them (V8); one hidden look on the new hidden part after B4, at least two looks kept (V9) |
| 2026-11-22 | conductor | The covering test reads a chunk as the model receives it, with its title line |
| 2026-11-22 | conductor | After M0: S3's limit stands; the Qwen reranker at full precision is measured on D5's funnel for information only; only a setting within the limit goes on to B4 and the hidden look |
| 2026-11-22 | conductor | The decline detector stays as it is: counting a gap sentence followed by a concrete answer as an answer would change six stored M4 outcomes (five strict answers and one a3 answer would turn from declines into wrong answers) and turn most grounded correct declines into wrong answers. Stored and new runs stay comparable; reports keep the sentence on the detector's bias, and a reader checks the answers counted as wrong declines before a failure of that condition is acted on |
| 2026-11-22 | conductor | T's harm points judge the consequence (data loss, an outage), with a condition and a near-miss, not the command alone |

## Waiting for the Owner

Nothing blocks the work now (U1 and U2 answered on 2026-11-23; T1 fell away with U1).

Still open from earlier milestones, none blocking:

- The laptop run (`agents/laptop-tests.md`); it also decides whether the footprint fits
  `GOAL.md`'s "about 20 GB".
- The Owner's review of the architecture rubrics of the open batches (b1-10, 18, 19, 20,
  29, 30; b2-09, 10, 19, 20, 29, 30; b4-19, 20, 29, 30) and the single B2 points (b2-02,
  b2-05, b2-13).
- D2: the Owner reads ten answers.

Later in M6: S4 (a quantised build of the second reranker), after the D5 funnel; whether
candidate C becomes the default, after the look.

## Log

- 2026-11-22 17:45: M5 closed (`milestones/M5.md`). 17:54: the plan drafted
  (`agents/m6-plan.md`). 18:00: the Owner agreed with S1 to S6; 18:16 with S7.
- 2026-11-22 18:05 to 19:34: D5 written by five writers, assembled, reviewed by five second
  reviewers (387 change records; two scenarios rejected as repeats of B2 families and
  replaced), counted set by set by five independent checkers (first count 187 of 227
  points, 82.4 %; under the corrected test 193, 85.0 %), and repaired by recording only
  the sets the checkers rated ok and the passing sets they found. The sealed family check
  found 7 hidden items of the same family and 9 near.
- 2026-11-22 21:00: M0 done; M5's memory figure for the second reranker (6.7 GB) was not
  reproduced (3.2 to 3.4 GB).
- 2026-11-22 21:03: the Owner gave the go for `v3.2.0` (tagged on `0021039`, release page
  published) and agreed with S8. This document replaces M5's.
- 2026-11-22 21:04: M6's setup committed (`66859be`): this document, the plan, D5, the tag
  for seven hidden items. Started: the harness repairs (F-a), the D5 funnel chain and the
  B4 check made independent of r4 (F-b), and the recount of B4 (B4R).
- 2026-11-22 21:06: B4R done: nothing changes under the corrected test.
- 2026-11-22 22:00: F-a done and committed (`3cb9127`): every answer row records the
  sha256 of the messages sent; a judge reply with one misplaced bracket is repaired when
  it holds exactly the rubric's ids (all 646 stored valid judge rows of M5 give the same
  verdicts; the reply that blocked M5's checks is repaired); full suite 1,982 tests with
  one failure, fixed, the module rerun. The decline detector is not changed (Decisions).
  22:03: four writers for T and the destructive-step marking (H1) started.
- 2026-11-22 22:41: F-b committed (`fa686ce`): the D5 funnel chain and the B4 check made
  independent of r4. The funnel's real run waits for H1's commit (a clean tree, a quiet
  CPU for the timing pass).
- 2026-11-22 22:52: set T reviewed and committed (`228456c`): 41 scenarios, 33 and 8.
  The conductor's rules for T: the two rejected scenarios are not replaced; in a
  scenario where the destructive step is right, a refusal counts as harmful only where
  its consequence is harm (t-42), otherwise as wrong (t-43).
- 2026-11-22 22:56: the new hidden judged part (HP) started: six sealed writers, ids
  h6-01 to h6-60, two scenarios per task type each, domains: Kafka brokers and KRaft;
  Strimzi; Connect, MirrorMaker 2 and the Bridge; the clients; Kafka Streams; Argo CD
  and Kubernetes. The conductor's choices: 60 scenarios (the size the look's rule was
  planned on); covering sets as in D5, under the corrected test; no family of an open
  batch or of the existing 61 hidden scenarios.
- 2026-11-22 23:18: H1 committed (`098c00f`): the destructive-step marking, experimental
  and off by default; in-sample numbers only, so the set T is its first fair test.
  23:20: the D5 funnel's real run handed to a job-operator; the D5 answers chain (DA)
  is being built and rehearsed with stubs.
- 2026-11-22 23:18 to 23:46: the D5 funnel ran (the first model-server run of M6; the
  chain unloaded an idle DeepSeek and loaded the 4-bit base model): **r4g2 goes on**,
  +18.9 percentage points on the strict count (112 against 69 of 227), nothing lost.
  The Qwen reranker's advantage over r4g2 on fresh data is 3.5 points with a standard
  error of 3.1, so nothing supports asking the Owner for a quantised build (S4): the
  conductor's recommendation is to leave it. 23:48: the one-time B4 check started.
- 2026-11-22, the hidden part so far: all six writers done (60 drafts); reviews of A, C,
  E and F in, B and D running; set checks of C (33 of 40 points fully covered) and A (26
  of 33) in; the family check found no draft of the same family as an existing hidden
  item, two of the same family as a B4 scenario (h6-40, h6-59, also one family with each
  other) and three architecture drafts of one family (h6-10, h6-19, h6-39; h6-10 stays).
  With the reviewers' drops (h6-03, h6-04, h6-21, h6-41, h6-47) nine scenarios are
  being rewritten under the same ids by two sealed replacement writers who read all
  drafts first. One reply named an open scenario next to a dropped hidden id (h6-47).
- 2026-11-22 23:57: **the B4 check failed**: r4g2 serves 65 of 140 points against the
  frozen retrieval's 50, +10.7 percentage points (standard error 3.6), bar +15.0; nothing
  lost. Y1 is not met and B4 is used. By the plan the combination stops and the Owner
  sees it (U1). Not stopped: the hidden part, the set T, the fixes to the D5 answers
  chain (it is needed for the informative run as well).
- 2026-11-22 23:50: the review of the D5 answers chain found no code blocker; fixes
  under way (more pins and a start check before every stage; commits allowed during the
  run; the B4 gate cannot be skipped after a failed check; the 60-second condition uses
  a measured whole retrieval; the supported-miss condition is also computed on the
  frozen pack and put to the Owner as T1).
- 2026-11-22 23:59, the hidden part: reviews of all six ranges in (50 of 60 drafts
  kept; h6-20 dropped as well, ten replacements being written); set checks: C 33 of 40
  points fully covered, A 26 of 33, E 32 of 35, F 38 of 44, D 31 of 41; B running.
- 2026-11-23 00:05: the D5 answers chain committed (`a20f97b`), reviewed and fixed, not
  run: its gate refuses after the failed B4 check, as the plan wants.
- 2026-11-23 00:17: **the T answers run started** (chain `scripts/m6_t_answers.sh`,
  commits `9d92e80`, `8091992`; a focused review found no blocker): default with the
  frozen retrieval on the 41 scenarios, then grounded-g2 on the same packs, the
  destructive-step marks as post-processing, two judge passes; 1.6 to 3 hours. The
  blind reading (RT) follows from its answer files.
- 2026-11-23 00:17, the hidden part: all six first-round set checks are in: 187 of 227
  points with sets fully covered (82.4 %; B 27 of 34), no set rated wrong. Replacements:
  the five architecture ones (R2) reviewed (no family repeated) and set-checked (26 of
  27 points); the other five (R1) written and in review. Then the assembly.
- 2026-11-23 00:31: **the new hidden judged part is final** (`689e1ad`): 60 scenarios
  (50 of the first 60 drafts and ten replacements: h6-03, h6-04, h6-19, h6-20, h6-21,
  h6-39, h6-40, h6-41, h6-47, h6-59). The independent family check of the replacements
  found none of the same family as anything else (one near, h6-21). 25 sealed sessions
  in all: 8 writers, 8 reviews, 8 set checks, 2 family checks, the assembly (it wrote
  only ids and counts in plain). Slips recorded: two replies named an open scenario
  next to a hidden id (both ids were dropped drafts); two sessions listed the hidden
  folder once (counts only). The architecture rubrics (12) had no review beyond the
  sealed second review, so the look's totals are also given without them.
- 2026-11-23 01:57: **the T answers run is done** (1 h 40 min): 82 answers, 164 judge
  rows. Coverage of the must points: default 0.612, grounded-g2 0.498 (−0.115, standard
  error 0.040, 41 scenarios); the default is cut at the limit in 13 of 41 answers and
  takes 72.9 s at the median with search, g2 1 of 41 and 42.7 s. The decline detector
  calls 40 of 41 g2 answers a decline (its known bias towards the gap sentence), so its
  count of refusals in the 8 destructive-right scenarios says nothing; the readers
  decide. 01:58: the reading of T defined (section above); bundles being prepared.
- 2026-11-23 02:02 to 02:14: **the reading of T** (five first readers on all 82 answers,
  two second readers on 22; the readers 5 % apart, 58 and 55 findings: the reading is
  used). Non-minor wrong, invented or harmful statements per 10 answers: default 15.12
  (90 % interval 11.95 to 18.54), grounded-g2 13.90 (10.98 to 16.83); paired −1.22
  (−4.15 to +1.71). **Harmful: 14 and 14**, in 11 answers each; 12 and 12 in the 33
  tempting scenarios, 2 and 2 in the 8 where the destructive step is right. Omissions 95
  and 113. Unblinded and classified by one worker (not checked by a second): of the 28
  harmful statements 15 are destructive steps, 11 wrong claims about behaviour, 1 an
  unsafe order, 1 other; 26 fall under a rubric must-not. **The marking's recall on T:
  7 of the 15 destructive steps** (default 4 of 8, grounded 3 of 7), none of the 13
  others; 17 and 14 of the 30 answers without a harmful statement carry marks (66 and 44
  marks). In the 8 scenarios where the destructive step is right the readers found one
  refusal per mode, and the right step with its check was missing in 6 of 8 answers per
  mode.
- 2026-11-23 02:21, the conductor: the marking's patterns are **not** tuned on T's misses
  (backticks inside two patterns, two prose forms, a code pattern): T is the only fair
  test there is, and a fix made from its misses could not be tested again in M6. The
  misses are listed in the report for a later milestone. The marking stays experimental
  and off by default; its in-sample recall of 9 of 9 (H1) did not hold on fresh data.
- 2026-11-23 07:50: **the Owner chose U1 (a) and agreed with U2.** The combination's
  stop is accepted; Y2 is not met; no hidden look in M6. The D5 answers run once as an
  informative run (defined in the section above before it starts).
- 2026-11-23 07:54: U2 carried out: the transcripts of the 27 sealed sessions of the
  hidden part's writing (8 writers, 8 reviews, 8 set checks, 2 family checks, the
  assembly) are deleted: 81 files (transcript, its metadata and its task-output link per
  session), 3 tool outputs those sessions had saved, and their helper scripts in the
  session's scratch folder; nothing matching the 27 ids is left. Not deleted, because
  outside U2's wording: the transcripts of the two earlier sealed family checks of this
  milestone and of M5 (B4 and D5 against the existing hidden items).
- 2026-11-23 09:14: **the Owner agreed with V1 to V9: M7 is planned and agreed**
  (`agents/m7-plan.md`, commit `be56c47`), from his two ideas of the morning (a longer
  time budget; a knowledge base in place of classic retrieval) and the conductor's
  answer (the time for checking, not for length; the knowledge base beside the index;
  one six-arm test on D5 first). `GOAL.md` carries the thorough profile's bar as
  proposed. M7's first slice runs on the CPU beside M6's last run, in new files only:
  the perfect packs of D5 (K0), the design of the checking pass (K1), the catalogue
  lookup module (K7). M6 closes when the informative D5 run and its reading are in;
  this document then moves to `milestones/M6.md`.
- 2026-11-23 09:21, M7's first slice is in. K0 (`4a09010`): the perfect packs serve all
  227 points with a covering set (frozen 69, r4g2 112); median 4 chunks from the sets.
  K7 (`f24370a`): the lookup module gives 1.3 to 2.2 cards per question and about 11
  per draft answer, under 12 ms per call. K1 (`runs/m7/reports/k1_check_design.md`):
  the checking pass is designed as "change only what an excerpt contradicts or a
  checker finding names, with a word-for-word quote, each change accepted by itself
  under mechanical rules". Its reach is small and the design says so: about 16 % of
  the errors readers found on B1 are contradicted by an excerpt (14 of 90 in M4's
  classification; 5 of 30 in a fresh sample), a third are specific claims the excerpts
  are silent on (about half of those decidable from the catalogues), and of T's 28
  harmful statements the excerpts state the danger for 3 to 5. Expected effect of the
  pass alone: 0.5 to 1.3 fewer wrong statements per 10 (inferred), against the test's
  bar of 2.0, so thinking has to carry most of any gain.
  The conductor's decisions on the design: (1) the pass enters the six-arm test only if
  a pilot on worn data (stored drafts of B1, pilot2, B2 and T; about 3 server hours and
  4 reader sessions) clears the bars the design fixes (at least +0.5 net catches per 10
  answers, damage to correct lines at most half the catches, no new harmful statement,
  time); (2) the pilot also runs the pass with the lookup cards of the draft in its
  input, because the cards turn silent specific claims into contradictions; (3) the
  test's bar of −2.0 stays as agreed; (4) a rejected change shows nothing to the reader.

