# Milestone M5: better ranking, fewer wrong statements, repairs

**Tracking document.** The conductor keeps it true at every step; workers do not edit it.
Working name "M5"; the release number is decided at the end. Started 2026-11-22, after the
release of v3.1.0. `GOAL.md` says where the project is going; `agents/m5-plan.md` is the
brief (the evidence, the work packages, the rules); this file says where the milestone
stands.

M4's tracking document is `milestones/M4.md`. Its decisions (J1 to J6, K1 to K6, L1 to
L11, M1, N1 to N7) stay in force, and code comments that cite "MILESTONE.md" for them mean
that file.

Last updated: 2026-11-22, 00:35.

## What this milestone is for

The Owner's targets (2026-11-21), in this order:

1. **Retrieval ranking.** On the open batches the needed excerpt reached the model for 56
   of 166 (B1) and 37 of 149 (B2) indexed must points; 68 and 63 were found by the search
   and ranked below the six-chunk pack; 41 and 44 were never among the 40 candidates.
2. **Fewer wrong statements.** A sealed reading of 61 hidden default answers found 17.2
   non-minor wrong statements per 10 answers and 13 harmful ones. Most are statements
   about behaviour, not names the checker can look up (inferred from the B1 reading).
3. **Repairs** of the defects found in M4.

Speed work as such and the laptop run are not targets. The base model stays Qwen3.8-27B at
4-bit.

## Done means (exit criteria, accepted by the Owner on 2026-11-22, P8)

| # | Criterion | Bar | State |
|---|---|---|---|
| X1 | Ranking | On B4 (30 new open scenarios, used once): indexed must points served at least +15 percentage points over the frozen retrieval, and at most 5 % of the frozen-served points lost; the same direction in the hidden funnel totals | open |
| X2 | Wrong statements | On the hidden half, candidate minus reference in one blind mixed reading: upper end of the 90 % interval below 0 and beyond the readers' disagreement; aim 12 or fewer per 10; harmful at most 13 | open |
| X3 | No loss | Coverage not down by more than 0.08; mechanical not down by more than 3 points; wrong declines at most 1 in 10 | open |
| X4 | Length and time | At most 10 % of judged answers cut at the limit; the median time reported | open |
| X5 | Repairs | Defects 1 to 4 of the plan fixed, with tests, before any hidden run | F1 running |

**What M5 cannot reach, said beforehand.**

- `GOAL.md`'s bar of at most 2 wrong statements per 10 answers. About 23 % of the errors
  of the B1 reading (13 of 56, in 30 answers, one classifier) are statements the excerpts
  seem to support or reasoning about the user's case. Applied to 17.2 that leaves about 4
  per 10 even with perfect retrieval and checking (inferred).
- A visible gain in hidden judged coverage from ranking alone. The expected gain is about
  +0.05 (inferred from open data: a must point is judged present about 0.8 of the time
  when its chunk is in the pack and about 0.4 when it is not), and the hidden half can
  show 0.07 to 0.10. The evidence for ranking is the count of needed excerpts served.

## Work packages

| Task | What | Role | Needs | State |
|---|---|---|---|---|
| F1 | Chain repairs: the E2 chain checks a stored manifest and markers at the start; the E5 chain stops when a step refuses; a resume survives a reload of the same model; the inline-marks step takes `--allow-commit-change` | implementer | CPU | running since 2026-11-22 00:28 |
| F2 | The scorer's decline outcomes and not-covered items made aware of the index in use; a guard on the default index path of `scripts/retrieve.py` | implementer | CPU | after F1 (shared files) |
| R0 | Design of profile r4: the exact settings to try, the rule that picks one on the development pool, what the simulation prints, the expansion leak | complex-reviewer | CPU | next |
| R1 | Profile r4 and its funnel preset, next to the frozen profile | implementer | CPU, reranker | after R0 |
| R2 | r4 settings chosen on the development pool (pilot 24, B1, B2, open mechanical 339) by the rule of R0; hash recorded | complex-reviewer | CPU | after R1 |
| R3 | B4: 30 new open scenarios with covering chunks; independent chunk count at least 95 % before any answer; family check against the hidden items in a sealed session | writers, reviewers | Claude workers | next |
| R4 | Expansions for B4, then the X1 rule applied once | job-operator | server 0.2 h | after R2 and R3 |
| G0 | Design of the grounded mode: the prompt text (variants G1 and G2), the rule that separates them, the reading plan on B2 | complex-reviewer | CPU | next |
| G1 | The grounded mode as a new prompt constant with its own hashes; the default and the golden hashes unchanged | implementer | CPU | after G0 |
| G2 | B2 answered in the grounded mode; a blind reading mixed with the stored default answers; judge; open mechanical 339 | job-operator, readers | server about 3 h | after G1 |
| G3 | Experiment: can the reranker's score tell supported from unsupported sentences, on the 56 errors of the B1 reading | implementer | CPU, reranker | after R0 (not on the critical path) |
| H1 | Hidden look 3: r4 with the grounded mode against E2's stored default answers; ledger row and rule first | job-operator, sealed readers | server about 4 h | after R4 and G2 |

Not in M5 (P5, P7): a second reranker or dense retrieval (a download); index changes (the
Argo CD command pages, new chunking or titles); training.

## The rule for hidden look 3 (proposed in the plan; fixed in the ledger before the run)

Candidate C = r4 with the grounded mode. Reference R = E2's stored 4-bit default answers.
One fresh blind reading of both sets mixed (122 answers), a second reader on 25 %. C
becomes the default only if all hold: wrong statements per 10, C minus R, upper 90 % end
below 0 and beyond the readers' disagreement; harmful statements in C at most R's;
coverage, C minus R, lower end above −0.08; mechanical, C minus R, lower end above −3
points; wrong declines at most 1 in 10; median judged answer at most 81.8 s. Reported and
deciding nothing: funnel totals on the hidden 61, the share of cut answers, the identifier
check, totals without the 11 tagged items. Switching the default changes the golden packs
and `frozen.json` and is the Owner's decision after the look.

## Hidden looks

Six looks on the hidden part in total. M4 used two (E2, E3). The Owner carried the
remaining four into M5 (P1); M5 uses at most two. Ledger: `runs/m4/HIDDEN_LEDGER.md`. A
new hidden judged part is due in the milestone after this one, or once two more looks are
spent on the same 61 scenarios.

## Decisions

| Date | # | Decision (the Owner's) |
|---|---|---|
| 2026-11-21 | — | Targets of the milestone: retrieval ranking, fewer wrong statements, repairs |
| 2026-11-22 | P1 | The four unused hidden looks are carried into M5; at most two are used |
| 2026-11-22 | P2 | Go to load the 4-bit build of the base model for M5 measurement runs |
| 2026-11-22 | P3 | Write B4: 30 new open scenarios, the out-of-sample check for r4 |
| 2026-11-22 | P4 | Read the B2 answers for errors, blind, grounded against the stored default |
| 2026-11-22 | P5 | r4 is built as a candidate profile with changed reranker settings; the frozen retrieval stays the default; a second reranker waits |
| 2026-11-22 | P6 | The grounded mode is added as a new prompt constant; the measured prompts stay as they are |
| 2026-11-22 | P7 | Index changes are deferred |
| 2026-11-22 | P8 | Exit criteria X1 to X5 as above |

## Waiting for the Owner

Nothing blocks the work now. Still open from M4, none blocking:

- The laptop run (`agents/laptop-tests.md`); the results are processed when they arrive.
- The Owner's review of the architecture rubrics of the open batches (b1-10, 18, 19, 20,
  29, 30; b2-09, 10, 19, 20, 29, 30) and the single B2 points (b2-02, b2-05, b2-13).
- D2: the Owner reads ten answers.
- The conductor session's transcript of M4 holds the Owner's notes on 11 hidden items; the
  Owner may delete it once that session is closed.

Later in M5: whether candidate C becomes the default (after look 3).

## Log

- 2026-11-22 00:10: the plan was drafted by a reviewer (`runs/m5/reports/m5_plan.md`,
  committed as `agents/m5-plan.md`). New measurements on open data, no run: cutting stored
  B1 answers at half length keeps 60 % of the points found present (112 of 187) and 52 %
  of the errors (29 of 56); a must point is judged present 0.80 (B1) and 0.78 (B2) of the
  time when its chunk is in the pack, 0.39 and 0.40 when it is not.
- 2026-11-22 00:28: F1 (chain repairs) started, CPU only.
- 2026-11-22 00:30: the Owner agreed with P1 to P8. This document replaces M4's, which
  moved to `milestones/M4.md`.
