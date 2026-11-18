# Milestone M4: provable, trustworthy, fast

**Tracking document.** The conductor keeps it true at every step; workers do not edit it.
Working name "M4"; the release number is decided at the end. Started 2026-11-18, after the
release of v3.0.0 and v3.0.1. `GOAL.md` says where the project is going; this file says
where the current milestone stands.

Last updated: 2026-11-18.

## What this milestone is for

v3 showed that retrieval is what helps, and left three things open: the evaluation is too
small and too worn to show further gains, answers still contain invented or wrong items,
and answers are too slow. M4 closes those three, in that order, and then asks whether a
different base model does better.

## Done means (exit criteria)

| # | Criterion | Bar | Status |
|---|---|---|---|
| E1 | A new evaluation exists: fresh questions for the five task types of `GOAL.md`, a part nobody reads, mechanical checks, a measured noise level | 300 or more mechanical items, 120 or more judged items, noise reported | not started |
| E2 | The starting point is measured on it: plain base, base + retrieval at full precision, and the 4-bit model with retrieval through a local server, in default and strict mode | numbers with their sizes; the cost of 4-bit stated | not started |
| E3 | Every answer is checked before the user sees it | at most 2 invented or wrong items per 10 answers in default mode on fresh questions (the bar of `GOAL.md`) | not started |
| E4 | Answers are fast on the 4-bit server path | median under 60 s in default mode on the Mac Studio; first words within 10 s | not started |
| E5 | Base model decision | shortlist, bake-off on the new evaluation, keep or switch recorded | shortlist in progress |

Laptop measurements (the real bar of `GOAL.md`) need a 32 to 36 GB Apple Silicon laptop;
see "Waiting for the Owner".

## Work packages

| WP | What | Role (model) | Depends on | Status | Record |
|---|---|---|---|---|---|
| WP1a | Catalogues of real identifiers from the pinned sources (Kafka config keys, Strimzi resource schemas and annotations, Argo CD and Kubernetes schemas, command flags) | implementer (sonnet) | - | started 2026-11-18 | `runs/m4/reports/` |
| WP1b | Evaluation design: three tiers, hidden part, sources of questions, scoring | conductor; reviewed by complex-reviewer (opus) | - | brief written, in review | `agents/m4-evaluation.md` |
| WP1c | Build the mechanical tier and its scorer | implementer | WP1a, WP1b | not started | |
| WP1d | Build the judged tier (questions, rubrics, references) | implementer, reviewer, Owner's questions | WP1b | not started | |
| WP1e | Harness: one command to answer, score, judge and report for any configuration | implementer | WP1b | not started | |
| WP1f | Starting-point measurement (E2) | job-operator | WP1c to WP1e | not started | |
| WP2a | Labelled test set for the checker, from the items the three readings found | local worker, then reviewer | - | queued | |
| WP2b | Answer checker in the runtime: flag, then revise | implementer | WP1a, WP2a | not started | |
| WP2c | Measure E3 | job-operator, reviewer | WP2b, WP1f | not started | |
| WP3a | Long-running local service that keeps the index and reranker loaded; streaming | implementer | - | not started | |
| WP3b | Fewer excerpts for simple lookups; measure E4 | implementer, job-operator | WP3a, WP1f | not started | |
| WP4a | Shortlist of base models (licence, size at 4-bit, thinking switch, tool calling) | researcher (sonnet) | - | started 2026-11-18 | `runs/m4/reports/base_shortlist.md` |
| WP4b | Bake-off on the new evaluation; decision | job-operator, reviewer, Owner | WP1f, WP4a | not started | |
| D1 | Find why the server path and the in-process path give different output | implementer, job-operator | - | not started | |
| D2 | The Owner reads ten answers that a model has already read, to calibrate the readings | Owner | - | waiting for the Owner | |

Workers on local models: bounded reading and scoped code changes go to
`.claude/local-worker.sh` when the model server is free and no job needs the GPU; design,
web research and judging stay on Claude models (`AGENTS.md`).

## Where we start (measured in v3)

| What | Figure | Size |
|---|---|---|
| Base + retrieval against plain base | +1.48 | 40 worn questions, two passes, standard error 0.21 |
| Invented or wrong items, default mode | 17 per 10 answers (real), 3 per 10 (holdout) | one model reader, one pass |
| Invented or wrong items, strict mode | 1 per 10 (real), 0 per 10 (holdout) | same |
| Retrieval missed the page | at least 5 of 42 holdout questions | one reading |
| Time per answer, 4-bit through oMLX | median 70.8 s default, 47.2 s strict | 10 questions per mode, Mac Studio |
| Time per answer, 4-bit through Ollama | median 38.6 s default, 26.7 s strict | 10 questions per mode, different build |
| Quality of the 4-bit builds | not measured | - |

## Decisions

| Date | Decision | By |
|---|---|---|
| 2026-11-18 | Order of work: evaluation, then the answer checker, then speed; base-model shortlist in parallel, bake-off after the evaluation exists | Owner |
| 2026-11-18 | Workers that need no GPU may run on the local model server | Owner |

## Waiting for the Owner

1. **Reference configuration for M4:** the 4-bit base model served by oMLX (the setup
   `GOAL.md` recommends), measured in default and strict mode. Recommended: yes. The
   in-process full-precision path stays as the v3 reference.
2. **Fresh real questions with correct answers** from real incidents: 30 to 50 would make
   the judged tier trustworthy. Any number helps.
3. **Sources for further questions:** whether public questions from Stack Overflow
   (CC BY-SA 4.0, share-alike) and from the projects' issue trackers may be used as raw
   material, after a licence check by a researcher.
4. **Small public downloads for the catalogues:** the Kubernetes API schema at the pinned
   version, the Argo CD resource definitions, and whatever else the catalogue worker
   reports missing. Each is a few megabytes, pinned and recorded like the other sources.
5. **A laptop to measure on:** a 32 to 36 GB Apple Silicon laptop for the fit and speed
   bars, when the time comes.
6. **D2:** read ten answers yourself.

## Log

- 2026-11-18: milestone opened. Evaluation brief written and sent for review; catalogue
  worker and base-model researcher started; a local-model worker is taking inventory.
