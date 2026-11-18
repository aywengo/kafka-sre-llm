# Milestone M4: provable, trustworthy, fast

**Tracking document.** The conductor keeps it true at every step; workers do not edit it.
Working name "M4"; the release number is decided at the end. Started 2026-11-18, after the
release of v3.0.0 and v3.0.1. `GOAL.md` says where the project is going; this file says
where the current milestone stands.

Last updated: 2026-11-18, evening.

## What this milestone is for

v3 showed that retrieval is what helps, and left three things open: the evaluation is too
small and too worn to show further gains, answers still contain invented or wrong items,
and answers are too slow. M4 closes those three, in that order, and then asks whether a
different base model does better.

## Done means (exit criteria)

| # | Criterion | Bar | Status |
|---|---|---|---|
| E1 | A new evaluation exists: fresh items for the five task types of `GOAL.md`, a hidden part, mechanical checks, a measured noise level | 300 or more mechanical items, 120 or more judged items, the smallest visible difference stated per tier | first slice being built |
| E2 | The starting point is measured on it: plain base, base + retrieval at full precision, and the 4-bit model with retrieval, in default and strict mode, all on the same server path | numbers with their sizes; the cost of 4-bit shown inside a margin fixed before the run (5 points on the mechanical tier) | not started |
| E3 | Every answer is checked before the user sees it | at most 2 invented or wrong items per 10 answers in default mode on fresh questions, **measured by a reading**, with the checker's own counts beside it | not started |
| E4 | Answers are fast on the 4-bit server path | median under 60 s in default mode on the Mac Studio; first words within 10 s | not started |
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
| WP1a | Catalogues of real identifiers from the pinned sources | implementer (sonnet) | **done** 2026-11-18, commit `888333f`: 1,177 Kafka config keys, 1,577 Strimzi field paths, 35 annotations, 28 tools, Argo CD commands | `runs/m4/reports/wp1a_catalogues.md` |
| WP1b | Evaluation design and its review | conductor; complex-reviewer (opus) | **reviewed** ("build with these changes", 15 changes, all accepted); design being revised | `agents/m4-evaluation.md`, `runs/m4/reports/evaluation_design_review.md` |
| S1 | Harness: answer, manifest, pack replay, hidden part, run sizes, report | implementer | running | `runs/m4/reports/s1_harness.md` |
| S2, S3 | About 200 mechanical items (config, Connect REST, Argo CD, error names) and the scorer with identifier precision; catalogue provenance | implementer | running | `runs/m4/reports/s2_s3_tier_a1.md` |
| S4 | Rubric pilot: blind rubrics for 30 questions whose answers have a reader's findings; check of the labels that rest on memory | complex-reviewer, researcher | next | |
| S5 | Judge pilot: does judging point by point repeat better than a mark from 1 to 5 | job-operator; needs the model server | after S4 | |
| S6 | Noise (the reference run twice) and 4-bit against full precision on the same server, with pack replay | job-operator; needs the model server | waits for the Owner (see 4 below) | |

After the first slice:

| WP | What | Depends on | Status |
|---|---|---|---|
| WP1c | Remaining mechanical kinds (resources, commands, client code, version traps, valid but wrong, safe order, sizing, not covered) | downloads (see 3 below), S5, S6 | not started |
| WP1d | Judged tier: scenarios and rubrics, the Owner's questions first | S5 | not started |
| WP1f | Starting-point measurement (E2) | WP1c, WP1d | not started |
| WP2a | Labelled test set for the checker: 47 items from the three readings | - | **draft done** by a local-model worker; counts match the reports; quotes to be spot-checked |
| WP2b | Answer checker in the runtime: flag, then revise. Separate code from the scorer; shares only the catalogues | WP1a, WP2a | not started |
| WP2c | Measure E3 by a reading | WP2b, WP1f | not started |
| WP3a | Long-running local service that keeps the index and reranker loaded; streaming | - | not started |
| WP3b | Fewer excerpts for simple lookups; measure E4 | WP3a, WP1f | not started |
| WP4a | Shortlist of base models | - | **done** 2026-11-18 (`runs/m4/reports/base_shortlist.md`) |
| WP4b | Bake-off on the new evaluation; decision | WP1f, downloads (see 7 below) | not started |
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
| Quality of the 4-bit builds | not measured | - |

## Decisions

| Date | Decision | By |
|---|---|---|
| 2026-11-18 | Order of work: evaluation, then the answer checker, then speed; base-model shortlist in parallel, bake-off after the evaluation exists | Owner |
| 2026-11-18 | Workers that need no GPU may run on the local model server | Owner |
| 2026-11-18 | The 15 changes of the design review are accepted into the design; those that are the Owner's are marked as waiting | conductor |

## Waiting for the Owner

Recommended answer first in each. None blocks the work now running.

1. **Reference configuration for M4:** yes, the 4-bit base model served by oMLX, default
   and strict mode.
2. **The hidden part:** yes to half of every tier hidden, decisions taken on the hidden
   half, stored encrypted with the key outside the repository, and at most six looks at it
   in M4, each written down with its decision rule beforehand.
3. **Downloads and installs for the catalogues and checks:** yes. Strimzi 1.3.1 resource
   definitions, Kafka 4.3.2 tool source or binary (command flags), the Kubernetes API
   schema, Argo CD 3.6.1 resource definitions, earlier Kafka config pages (for changed
   defaults), the kafka-clients jar and a Python Kafka client with `jsonschema` (to check
   written code and resources). All Apache-2.0, pinned, kept out of the frozen index.
4. **Loading the full-precision and the 4-bit 27B builds on the model server for S6:**
   yes; about 15 to 20 hours of exclusive server time.
5. **Two wordings:** `AGENTS.md` calls the old 40 questions "the gate set"; they become a
   legacy set. `GOAL.md` says the cost of 4-bit must be "within noise"; it becomes "inside
   a margin fixed before the run".
6. **Questions:** 30 to 50 fresh real questions with correct answers from you; whether
   public questions (Stack Overflow, share-alike; issue trackers) may be raw material after
   a licence check; your review of the architecture rubrics (without it that part shrinks
   from 25 to 15 items).
7. **Bake-off downloads:** the four candidates above the reference, about 16 GB each.
8. **A laptop to measure on:** a 32 to 36 GB Apple Silicon laptop.
9. **D2:** read ten answers yourself.

## Log

- 2026-11-18: milestone opened. Evaluation brief written; catalogue worker, base-model
  researcher and design reviewer started; a local-model worker extracted the 47 labelled
  items (counts verified).
- 2026-11-18, evening: catalogues done and pushed (`888333f`). Design review back:
  build with 15 changes; the main ones are that the scorer must not be the checker, that
  the judging method is piloted before the judged tier is written, and that the hidden
  part is half, split by family and encrypted. Shortlist back. First slice started:
  harness (S1), first mechanical items and scorer (S2, S3), design revision.
