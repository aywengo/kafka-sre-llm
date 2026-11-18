# M4, work package 1: a new evaluation

**Status: design by the conductor, 2026-11-18, under review before anything is built.**
Tracking: `MILESTONE.md`. Goal and bars: `GOAL.md`.

## Why

The 40 real questions have been used for every decision since v1, including tuning of the
index. Their standard error is about 0.2, so they cannot show an improvement smaller than
about 0.4, and part of the last headline figure is fitted to them. They cover operations
and configuration only. Every later step of M4 (answer checker, speed, another base model,
4-bit against full precision) needs differences of 0.1 to 0.3 to be visible, on all five
task types of `GOAL.md`, without a judge model's noise where that can be avoided.

## Design in one paragraph

Three tiers. **Tier A** is large and scored by a program, with no judge: tasks whose
answers can be checked against schemas and catalogues built from the pinned sources.
**Tier B** is smaller and scored by a judge against a written list of required points,
then partly read. **Tier C** is a live cluster with injected faults and belongs to the
tools milestone; it is only specified here. Each tier has a hidden part that nobody reads
during development. The old 40 questions are kept as a legacy set for continuity and are
no longer a gate.

## Tier A: mechanical (target 300 or more items)

An item is a question plus a machine-checkable expectation. No reference prose.

| Kind | Example | Check |
|---|---|---|
| Write a resource | "Write a KafkaTopic with 12 partitions, 3 replicas, 7 days retention, compaction off." | The answer contains one YAML document that validates against the pinned schema; listed fields have the listed values |
| Write a resource with a trap | "…for a KRaft cluster using node pools" | As above, plus fields that must be absent |
| Name the setting | "Which broker setting limits the time a follower may lag before leaving the ISR, and its default?" | The answer names the catalogue key; the default matches the catalogue |
| Is this real | "Does `kafka-topics.sh` have an `--unparsed` flag?" (half of these are real, half are not) | Yes or no matches the catalogue |
| Fix the manifest | A manifest with one schema error or one wrong field | The corrected document validates and the named field is changed |
| Write the command | "Describe consumer group `orders` with members and lag." | Tool and flags exist in the catalogue; required flags present; forbidden ones absent |
| Write the client code | "A Java consumer with manual commit after processing." / the same in Python | It compiles (Java) or passes a syntax and import check (Python); listed API calls present; listed config keys exist |

Rules:

- Items are generated from the catalogues and the pinned documentation by templates and by
  a model that is neither the answering model nor the judge, then filtered by the same
  checks run on a reference solution that must pass. An item without a passing reference
  solution is dropped.
- Every item records its source (catalogue entry or document), the template, and the
  versions it is true for.
- Scoring is pass or fail per check, reported per kind and per domain, with a confidence
  interval from the item count. Partial credit is the share of checks passed.
- "Is this real" items measure invention directly and must be balanced, so that answering
  "yes" to everything scores 50 %.
- The scorer is the same code as the answer checker of work package 2. One implementation,
  two uses.

## Tier B: judged (target 120 or more items)

For diagnostics, debugging from pasted evidence, and streaming architecture, where a
program cannot decide. An item is a scenario, optional pasted evidence (log lines,
`describe` output, a stack trace), and a rubric:

- **must** points (3 to 6): facts or steps a correct answer contains;
- **must not** points (0 to 3): statements that would be wrong or harmful here;
- a short reference answer, for the reader, not for the score.

The judge decides each point separately (present or absent), in two passes with the answer
order irrelevant because answers are scored alone, not head to head. The score is the share
of must points present minus a penalty per must-not point present. Per-point judging is
used because it is more repeatable than one mark from 1 to 5; that claim is checked, not
assumed (see "Noise").

Sources of scenarios, in order of trust:

1. The Owner's real questions with correct answers, as they arrive.
2. The 42 runbook-holdout questions (used three times so far, never for tuning), rewritten
   as rubrics.
3. Scenarios written from the held-out runbooks and from pinned documentation by a model
   that is neither answerer nor judge, each rubric checked against the documents by a
   reviewer on a Claude model. The 4,050-question bank of v3 stage 1 may supply topics,
   not rubrics.
4. Public questions (Stack Overflow, issue trackers), only if the Owner allows it and a
   researcher has cleared the licence.

Task-type quota for the 120: diagnostics 40, debugging from evidence 40, architecture 25,
configuration reasoning 15. Coding is covered by tier A.

## Tier C: live (specified only)

A local cluster with Strimzi and one injected fault per case (a full disk, a wrong
listener, a missing secret, a crash-looping pod, a stuck rebalance, an out-of-sync
application). The assistant has read-only tools. Scored on cause found and fix correct.
Built in the tools milestone; nothing is installed for it in M4.

## The hidden part

- 30 % of every tier, chosen by a fixed random seed at build time, goes to `eval/hidden/`.
- Nobody (conductor, workers, Owner) opens the hidden questions, the answers to them or
  per-item results. The harness runs them and prints totals per kind only.
- The hidden part is used for exit criteria and for decisions (base model, release). The
  open part is for development.
- This is a rule, not a lock. It is written into `AGENTS.md` when the set exists.

## Reference configuration

Unless the Owner decides otherwise: the 4-bit base model served by the local model server,
frozen index, default and strict mode. The full-precision in-process path is measured once
on the same sets, to state what 4-bit costs. Every configuration is named in the result
file by model build, server, mode, index hash and runtime commit.

## Noise

Before any comparison is believed:

- Run the reference configuration twice on tier A. Decoding is greedy, so differences
  between the two runs measure the server's own non-repeatability.
- Judge tier B twice. Report per-point agreement between passes and the standard error of
  the item score.
- Have a reviewer read 20 judged items and mark each point; report agreement with the
  judge. The Owner reads 10 of those (item D2 of `MILESTONE.md`).
- State the smallest difference each tier can show at this size.

## Harness

One entry point, `scripts/eval2.py`, with steps that resume from their output files and
print a line per item: `answer` (through the runtime, any backend), `score` (tier A),
`judge` (tier B), `report` (totals with intervals, hidden part as totals only). Long steps
run under `scripts/run_guarded.py`. It also records seconds per stage and tokens, so every
evaluation run is a speed measurement too.

## What is built, in order

1. Catalogues (work package 1a, already started).
2. The scorer for each tier A kind, tested on hand-written passing and failing answers.
3. Tier A items with passing reference solutions; the hidden split.
4. Tier B rubrics from the holdout questions first (they exist), then new scenarios.
5. The harness.
6. Noise measurements, then the starting-point measurement (exit criterion E2).

## Acceptance of this work package

- 300 or more tier A items and 120 or more tier B items, each with its source recorded.
- Every tier A item's reference solution passes its own checks; a deliberately wrong
  solution fails them.
- The noise section is filled with measured values.
- No item of the new sets appears in any prompt used to generate another item's
  reference, and none is among the old 40.
- The old 40 and the 42 holdout questions still run through the new harness, for
  continuity with v3.

## Open questions for the review

1. Is per-point judging really more repeatable than a 1 to 5 mark here, and is the penalty
   for must-not points right?
2. Is a model-written tier A, filtered by reference solutions, biased toward what models
   find easy to write?
3. Can the hidden part be trusted with only a rule to protect it?
4. What is missing for "streaming architecture", which has no mechanical check and is the
   hardest to judge?
5. Which parts of tier A depend on catalogues that cannot be built from what is already in
   `raw/`?
