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
| Y1 | Ranking | On B4, strict count, once: served must points at least +15.0 percentage points over the frozen retrieval, at most 5 % of the frozen-served points lost | open |
| Y2 | Combined candidate | The hidden look's rule (below), on the new hidden judged part | open |
| Y3 | Harmful | The set T measured against default with readers; the destructive-step marking with its recall reported; no claim beyond what T shows | open |
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

## Work packages

| Task | What | Role | Needs | State |
|---|---|---|---|---|
| M0 | The second reranker's memory, scores and time under smaller batches and half precision | implementer | CPU | **done** 2026-11-22 21:00 (`runs/m6/reports/m0_reranker_memory.md`): full precision adds about 3.2 GB after load and 3.35 GB at peak, 4.6 s per call; half precision 1.3 to 1.8 GB at peak but 29 to 41 s per call; nothing meets 1.5 GB at a usable speed |
| H0 | Classification of the harmful statements of the open readings | complex-reviewer | CPU | **done** 2026-11-22 18:14 (`runs/m6/reports/h0_harmful_classes.md`): 16 statements: 9 destructive steps, 6 wrong claims about behaviour, 1 unsafe order; 8 not foreseen by a rubric; a draft list of 17 patterns finds 8 of the 9 destructive steps and 1 of the other 7 |
| D5 | The fresh development batch: 50 open scenarios with covering sets verified before any answer | writers, reviewers, independent checkers | Claude workers | **done** 2026-11-22 19:34 (`eval/m6/d5/`, `CHUNK_CHECK.json`): 269 must points, 227 with at least one verified set (316 sets of one chunk, 38 of two), 13 with no covering set in the index, 29 not indexed; 7 hidden items tagged as sharing a family |
| F | (F-a and F-b running since 2026-11-22 21:05) Repairs before any hidden run: a prompt hash per answer; the decline detector reading a gap sentence as a decline; a judge parser that tolerates a misplaced bracket only when every rubric id is present; the B4 check chain made independent of r4, and a runtime profile for a second reranker; the checker's names in prose and comments; the chains' stale wording on a resume | implementer | CPU | next |
| B4R | Recount of B4's dropped chunks under the covering test with title lines | independent checkers | CPU | **done** 2026-11-22 21:06: the three checkers re-rated 92 chunk verdicts and 17 sets they had rated partial; none changed to ok, no point gains a set; B4's files stand as released (one possible pair for b4-26 m4, outside the pass's scope, was not added) |
| T | The set for harmful statements: 35 scenarios where a destructive shortcut is tempting and wrong, 8 where a destructive step is right; harm points judge the consequence, with a condition and a near-miss | writers, reviewers | Claude workers | next |
| H1 | Marking of destructive steps in the checker (marking only, S7) | implementer | CPU | after F |
| HP | The new hidden judged part, written, reviewed and set-checked in sealed sessions, with a family check against every open batch | sealed writers, reviewers | Claude workers | next |
| DF | D5 funnel, strict count: r4g2 (within the memory limit) and, for information only, the Qwen reranker at full precision | job-operator | CPU, reranker | after F |
| B4 | The one-time check of the chosen retrieval setting on B4 | job-operator | server 0.2 to 0.5 h | after DF and B4R |
| DA | D5 answers: default on the frozen packs as the reference in the same run; G2 and G3 on the chosen packs; two judge passes; the 116 mechanical items | job-operator | server 4 to 5.5 h | after DF |
| RD, RT | Blind readings of D5 (S6) and of T | readers | about 8 and 5 sessions; server 2 to 2.5 h for T's answers | after DA |
| H | The hidden look on the new hidden part; ledger row first | job-operator, sealed readers | server 5 to 7 h | after everything above |

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
| 2026-11-22 | conductor | The covering test reads a chunk as the model receives it, with its title line |
| 2026-11-22 | conductor | After M0: S3's limit stands; the Qwen reranker at full precision is measured on D5's funnel for information only; only a setting within the limit goes on to B4 and the hidden look |
| 2026-11-22 | conductor | T's harm points judge the consequence (data loss, an outage), with a condition and a near-miss, not the command alone |

## Waiting for the Owner

Nothing blocks the work now. Still open from earlier milestones, none blocking:

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
