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

Last updated: 2026-11-23, 13:28 (the first version, at the handover).

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

## The test on D5

Six arms as in `agents/m7-plan.md` section 3 (V3; the perfect pack padded with the frozen
pack, V4; the thorough arms on the default prompt, V5). **Its rule is not fixed in this
file yet**: the M7 session writes it here from the header its chain prints, before the
run. Open with the Owner at the handover: whether grounded-g3 on the r4g2 packs joins the
test as a further arm (asked in the M7 session), and X1 below.

## Work packages

| Task | What | State |
|---|---|---|
| K0 | The perfect packs of D5 (`scripts/m7_perfect_packs.py`) | **done** 2026-11-23 (`4a09010`): by the strict count the perfect pack serves all 227 points with a covering set (frozen 69, r4g2 112); a median of 4 chunks and 6,146 characters come from the sets; no pack over six chunks |
| K7 | Exact lookup from the catalogues as reference cards (`kafka_sre_rag/kb_lookup.py`) | **module done** 2026-11-23 (`f24370a`): 1.3 to 2.2 cards per question, about 11 per draft answer; under 12 ms per call; for about 24 of 251 open mechanical items a card from the question alone would state the expected answer (the worker's estimate). Wired only into the checking pass so far |
| K1 | Design of the checking pass | **done** 2026-11-23 (`runs/m7/reports/k1_check_design.md`) |
| P1, K2 | The thorough profile's code: the checking pass with rules A1 to A8 and stage 2, thinking per call, the cards in the check, `--answer-profile thorough` and its switches, the harness's rows | **done and merged** 2026-11-23 (`3be10d4`; review `runs/m7/reports/p1_review.md`: no blocker, seven fixes made; full suite on the merge 2,235 tests, OK, 60 skipped). Experimental, off by default, unmeasured |
| K3, P2, P3 | The pilot chain and its scoring | **built** (`b736ad0`, in the merge), not run |
| K4 | The six-arm test chain | built and reviewed on branch `m7-k4-six-arm` in the M7 session; **not on main yet**: that session merges it after its own full-suite run |
| P4 | The reading of the pilot's accepted changes | after the pilot run |
| K5, K6 | The test's run and its blind reading | after the pilot and the rule |
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
| 2026-11-23 | conductor | The 18 departures of the checking pass's code from its design are accepted (builder's report in the session's scratch folder; the review covers them); in the cards variant of the prompt the sentence on unmentioned lines names the cards as well |

Decisions the Owner gave in the M7 session on 2026-11-23 (reported by that session, to be
recorded here by it with their wording): M7's chains never unload a model another client
loaded; "errors improve but time fails" stays narrow; under the check variant "none",
thinking alone may go on to B4.

## Waiting for the Owner

- **X1** (from M6's result): one hidden look on h6 for grounded-g3 on the r4g2 packs as
  the fast profile's default candidate, under the look's rule of `milestones/M6.md`.
  Recommended by the M6 session: yes; it would be a second look in M7, leaving two.
- The M7 session's own open questions (the time bars as a reported measure with an
  operational timeout; how harm is matched; grounded-g3 on r4g2 as a judged-only arm).

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
