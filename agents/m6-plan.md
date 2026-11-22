# M6 plan: a combined candidate, harmful statements, repairs

**Status: the brief of milestone M6.** Drafted by a reviewer on 2026-11-22 from M5's evidence (read-only, no model run, the hidden part
not opened). The Owner agreed with its decisions (section 8, recorded as S1 to S6 in `MILESTONE.md`) on 2026-11-22, and later the same day
changed one of them: the hidden look runs on a new hidden judged part, not on the existing 61 scenarios (S8). Where this text and
`MILESTONE.md` differ, `MILESTONE.md` wins. Tracking: `MILESTONE.md`. M5's tracking document: `milestones/M5.md`. Run files named below
(`runs/`, `work/`) are local and git-ignored.

## 1. Recommendation

- **Start the combined candidate at C0.** C0 is grounded-g2 on top of r4g2's candidate set, with the second call scored by Qwen3-Reranker-0.6B in setting r5a, after a memory fix that is measured first.
- **Write a fresh development batch, D5.** It has 50 scenarios, and each point gets a verified covering set before any answer exists. The candidate is chosen on D5, B4 is used once, then one hidden look on the existing 61 scenarios.
- **The hidden rule needs changing before the look.** On the evidence, M5's rule for look 3 would pass a candidate that is as good as the development data suggests only about one time in five (inferred). The Owner should fix the trade first.
- **Harmful statements need a purpose-built tempting set.** No reading of 61 answers can show a change smaller than about two thirds.
- **The repairs come before any hidden run.**

## 2. What M5's evidence says

**Coverage.** These are measured figures on 54 development scenarios with two judge passes, and the scenarios are worn.
- Default scores 0.545; G2 on the frozen packs scores 0.476 (−0.069, standard error 0.029).
- R6 (G2 on the frozen packs against G2 on r4g2's packs) measured +0.040 (standard error 0.025).
- r5a serves 235 of 421 points (loose count) against 210 for r4g2.
- Inferred: scaling R6's slope (+0.040 for 58 more points served) gives r5a about +0.011 more.
- **So C0 against default is expected at about −0.02 to −0.03 on development data (inferred), before any shrinkage on fresh data.**

**Wrong statements.** One reading, 54 pairs, nine sessions.
- G2 minus default is −3.89 per 10 (90 % interval −6.85 to −0.56).
- With a correction for two candidates (`ci_by_k` "2" in `work/m5/reading_g2/totals.json`) the interval is −7.41 to 0.00.
- Nothing has been read on r4g2 or r5 packs. The identifier count on r4g2 packs went from 12 to 9 in 54 answers, which is too few to read anything into.

**Time.** G2 takes 42.4 s with search on the frozen packs and 46.9 s on r4g2's packs, without the second call (R6). The Qwen call took 5.9 to 6.2 s median, on a busy machine. That puts C0 at about 53 s (inferred), on the reference machine and not on a laptop.

**Hidden half's resolution.**
- Coverage: E2's strict minus default was −0.018, interval −0.064 to +0.030, so the standard error is about 0.029.
- Reading: strict minus default was −2.62 (−5.57 to +0.33), so the standard error is about 1.8 per 10 (`runs/m4/HIDDEN_LEDGER.md`, rows 1 and 2).

**Which M5 stops carry over to the combination, and which do not.**

| Stop | Also stops C? |
|---|---|
| Added memory 6,706 MB against 3,500 (`work/m5/r5/time_r5.json`) | **Yes.** It is tied to `GOAL.md`'s "about 20 GB". The runtime with bge already peaks at 4,041 MB, next to a 15 to 17 GB model |
| Qwen call above 6.0 s median | Yes in principle, but it was measured on a busy machine, so it is provisional |
| r4: +25 / +18 pp on S/C; R5: +20 over r4g2 | **No.** These were bars on worn data (the S half used three times); the D5 bars replace them |
| G0: coverage within 0.05 of default on the 54 development scenarios | No, replaced on D5. The hidden −0.08 bar is the one that matters |
| No frozen-served point lost | Yes, by construction |

Not known:
- **The memory figure** was never broken down. It is a difference of `ru_maxrss` high-water marks (`scripts/r5_qwen.py:158-160`) for fp32 weights at batch 8 (lines 50, 181-182). Load-time copies and memory the allocator keeps are included. sentence-transformers already keeps only the last logit (`logits_to_keep=1`), so full-vocabulary logits are not the cause.
- **Strict against loose gains.** On B4 the strict count credits fewer points, and how that changes a gain in percentage points is unmeasured.

## 3. The combined candidate

**Start: C0** = `grounded-g2`, the frozen six chunks first, then r4g2's 24 scored candidates re-scored by Qwen r5a (terms query, the card's default instruction, 1,024-token window, 1,500 characters per passage).
- G2 rather than G3: G2 is the variant that was read and that ships in v3.2.0. G3's coverage gain (+0.017) is within noise, it was fitted to 12 named open scenarios, and it did worse on supported essential misses (2.50) and on the mechanical gain (+2.85).
- Both are defensible. Coverage and supported misses on D5 separate them.

**Memory fix.** Measure before choosing. Each option is measured for resident and peak memory, the order of the 24 candidates against fp32 (Kendall's tau, and how many items change their top four), and seconds per call on an idle server.

| Option | Effect on memory (inferred) | Scores | Time | Needs |
|---|---|---|---|---|
| fp32, batch 1 to 2 | smaller activations; weights stay 2.4 GB | the same except for padding effects | unknown, possibly faster (less padding) | CPU |
| bf16 or fp16 on CPU | weights 1.2 GB | small drift; the funnel must be re-scored | may be slower on CPU | CPU |
| MLX or GGUF loader, quantised | the smallest | the most drift | unknown | a download or install: the Owner's go (`agents/train-v3.md:155-156`) |
| Qwen for both calls, bge dropped | about −2.3 GB | changes the frozen six | about −4.5 s | a different candidate; out of scope unless the Owner chooses it |

**Settings allowed.** At most two retrieval settings go into the D5 funnel: r5a with the chosen fix, and r4g2 as the fallback. At most two prompts (G2, G3) are answered on the chosen packs. Nothing is tuned after D5.

**D5.** 50 open scenarios, 10 per kind.
- Writers record covering sets (one or two chunks that together state the whole fact). Points with several parts are split.
- An independent checker counts chunk by chunk. At least 95 % must be correct before any answer exists; repairs are re-checked; only sets the checker accepts are recorded.
- A family check against B4 and, in a sealed session, against the hidden items.
- What M5 taught: the reviewers had counted 100 % where the independent count found 98 of 154. The failures were facts spread over several chunks and named parts that no recorded chunk held.

**Rules, fixed beforehand.**

1. **D5 funnel** (CPU, strict count):
   - r5a goes on if it gains at least +15 pp over frozen, loses no frozen-served point, takes at most 6.0 s median and 9.0 s at the 90th percentile with the server idle, and adds memory within the limit of decision 3.
   - Otherwise r4g2 goes on if it passes the same bars. Otherwise stop.
2. **B4** (once): the strict count, at least +15.0 pp over frozen and at most 5 % of the frozen-served points lost. On failure the combination stops and the Owner sees it.
3. **D5 answers** (default on frozen packs as a reference answered in the same run; G2 and G3 on the chosen packs; two judge passes; the 116 mechanical items). A variant is eligible if:
   - coverage is not more than 0.05 under default;
   - supported misses are at most default + 1.0 per 10;
   - at most 10 % of answers are cut;
   - the median time with search is at most 60 s;
   - nonexistent identifiers are not above default;
   - the projected mechanical change is not under −3.
   
   The higher coverage wins; within 0.02 the choice goes to G2.
4. **A blind reading of D5** (recommended): a screen that gives the only estimate of wrong statements on fresh data before a look is spent.
5. **Hidden look 3.** C and R (default, frozen) are answered in the same run, with a prompt hash recorded per answer. 61 judged scenarios and 305 mechanical items; one blind reading of 122 answers with a second reader on 25 %. Totals with and without the 11 exposed and 3 family-tagged items.

**The trade the hidden rule must state.** These are inferred normal approximations.

| Condition (M5's rule) | Pass chance if C behaves as on development data |
|---|---|
| Coverage lower end > −0.08 (true value −0.02 to −0.03) | about 0.5 to 0.65 |
| Wrong statements, upper end < 0 (true value about −3.4 on hidden) | about 0.6; about 0.4 if "beyond the readers' disagreement" means upper end below −1 |
| Harmful C ≤ R (about 13 each) | about 0.54, a coin flip |
| All together | **about 0.12 to 0.2** |

Recommended rule:
- the upper end of wrong statements below 0;
- coverage point estimate at least −0.04 and lower end above −0.10;
- harmful C at most R + 3, with harm handled as its own target;
- mechanical lower end above −3;
- wrong declines at most 1 in 10;
- median time at most 60 s.

In plain words, the Owner accepts losing up to 0.10 of coverage in exchange for a fall in wrong statements that excludes zero. The wording "beyond the readers' disagreement" should be fixed either way.

**Looks.** Use one (look 3) and keep one spare. `MILESTONE.md:569-570` and `agents/m5-plan.md:155` make a new hidden judged part due in this milestone. Q17's "the hidden half" contradicts that, so it should be amended openly.

## 4. Harmful statements

**What is known.**
- The B1 reading in M4 had 6 harmful rows (one reader; `work/m4/b1/findings_unblinded.jsonl`):
  - 3 were destructive advice that the answer itself had already marked ("destructive", "last resort");
  - 2 were wrong claims about behaviour that lose data (b1-08) or delete a resource (b1-26);
  - 3 of the 4 harmful rows in default answers were in the class "the excerpts seem to support it".
- M5's reading found 5 against 6, but those findings are still encrypted and nobody has classified them.
- No destructive-step check or prompt rule exists: no match for "destructive" in `kafka_sre_rag/*.py` or `scripts/*.py`.

**Approaches without training.**

| Approach | Can be shown on open data before a run | Will not show |
|---|---|---|
| A list of destructive commands and patterns in the checker, which marks and never removes | how many classified harmful rows contain a listed pattern (inferred 2 to 3 of 6); marks per stored answer | any fall in harmful statements; it serves `GOAL.md` rule 5 ("marked") |
| A prompt rule: read-only check first, the safer alternative, the way back, no destroy-and-recreate of resources that hold data | nothing without answers | reasoning harm (b1-08, b1-26) |
| Marking instead of removing | the share of harmful rows that were unmarked: 3 of 6 were already marked | |

**Size of the evidence.**
- 5 or 6 in 54 answers: 90 % Poisson intervals of about 2 to 11 and 2.6 to 12.
- 13 in 61: about 7.7 to 20.6.
- So a reading of 61 answers shows only a fall to about 4 or below.
- A zero observed in 61 answers bounds the rate at 0.49 per 10 answers (95 %).

**Target.** A purpose-built open set T of 35 scenarios where a destructive shortcut is tempting and wrong, plus 8 where a destructive step is right. Each has harmful must-not points and is read by readers.

**Must not be claimed.** That harm is reduced in general, from a tempting set. That marks reduce harm. That the bar of zero is met.

## 5. Leftover repairs, by damage

1. **No prompt hash per answer** (`runs/m5/reports/gdev_determinism_diag.md:8`). It blocks a byte-for-byte check in look 3.
2. **The decline detector counts a gap sentence as a decline** (`runs/m5/G_DEV.md:97`). This biases a hidden condition against the grounded mode.
3. **No tolerant judge parser.** A row that stays malformed stops a chain. The parser should repair brackets only when every rubric id is present, and be tested so that all stored valid rows come out unchanged.
4. **`m5_b4_check.sh` is tied to r4** (`PROFILE_R4.json`, lines 17-18, 44), and there is no runtime profile r5:
   - `profiles.py:14` has r1 to r4 only;
   - `retrieval.py:33-42` keys the reranker by window size, not by model.
5. **The checker misses names in prose and comments.** H1 depends on that too.
6. **Stale "has N valid rows" wording** (`m5_g_dev.sh:378`, `m5_r6_run.sh:277`).
7. **F2**: the scorer's declines are tied to the frozen index (`eval2_score.py:36-38`), and `retrieve.py:2,16` still points at `work/v2`. Needed only for an index change.
8. **M5's tests have not been run from a fresh clone.**

## 6. Exit criteria (proposed)

| # | Criterion | Bar |
|---|---|---|
| Y1 | Ranking | B4 strict: +15.0 pp, at most 5 % lost |
| Y2 | Combined candidate | Hidden rule of section 3 (decision 2) |
| Y3 | Harmful | T measured against default with readers; marking check with recall reported; no claim beyond what T shows |
| Y4 | Time and fit | Median at most 60 s; the reranker's added resident memory within decision 3; the whole footprint reported against about 20 GB |
| Y5 | Repairs | Items 1 to 5 with tests before any hidden run |

**Out of reach even if everything works.**
- Wrong statements: 15.7 per 10 × 0.23 (the "excerpts seem to support it" share) ≈ 3.6 per 10, which stays above 2 even with perfect retrieval and checking (inferred from 56 errors read by one classifier).
- Harm at zero cannot be shown.
- Supported essential misses: G2 has 2.00 per 10 on B1 against "at most 1 in 10" (the units differ slightly).

## 7. Tasks and order

| Task | Role | Files (touch / not) | Output; acceptance | Needs | Time |
|---|---|---|---|---|---|
| **M0** memory and dtype measurement | implementer, then job-operator | new script / not `r5_qwen.py` pins | resident and peak memory, order agreement, seconds per option; logs complete | CPU, server idle | 0.5 d |
| **F-1 to F-4** repairs | implementer | `eval2.py`, scorer, judge parser, chains, tests | tests pass; stored valid judge rows unchanged | CPU | 2 d |
| **H0** classify the 17 open harmful rows | complex-reviewer | report | classes and pattern recall | CPU; M5 findings unblinded (the conductor's call) | 0.5 d |
| **D5, T** write and check | writers, independent checkers | `eval/m6/` | at least 95 % chunk check; sealed family check | Claude workers; the Owner's go | 1.5 d + 1 d |
| **R5P** runtime r5 profile; B4 chain made general | implementer | `profiles.py`, `retrieval.py`, `candidate.json`, `m5_b4_check.sh` | runtime equals simulation on all items | CPU | 1.5 d |
| **H1** destructive marking | implementer | checker, tests | recall on H0; marks per answer | CPU | 1 d |
| **DF / B4 / DA** D5 funnel, B4, D5 answers | job-operator | `work/m6/` | each rule's verdict | CPU; server 0.5 + 4 to 5.5 h | 1 d |
| **RD, RT** readings of D5 and T | readers | bundles | totals; readers within 30 % | about 8 + 5 sessions; server 2 to 2.5 h for T | 1 d |
| **H** look 3 | job-operator, sealed readers | ledger row first | totals | server 5 to 7 h; about 9 sealed sessions | 1 d |

- **First slice** (needs only the go for the plan): M0, F-1 to F-4, H0.
- **In parallel:** writing D5 and T; R5P after M0.
- **Totals:** 12 to 16 server hours, 9 to 12 worker-days, about 22 reader sessions.

**Risks and how each would show early.**
- No memory fix fits: M0 shows resident memory above the limit, or the order changes on more than 10 % of the top-four sets.
- The gain shrinks on fresh data: the D5 gain falls below +15 pp.
- r5 packs bring more errors: identifiers per 10 rise in the D5 answers.
- Readers can tell the mode apart: the gap sentence and length show it.
- The hidden look fails by noise: section 3's table.

## 8. Decisions for the Owner

1. **Go for the plan, including writing D5 and T, and loading the 4-bit model for M6** (P2's go ended with M5)? I recommend yes. Without it nothing in M6 can be measured.
2. **Which hidden rule?** I recommend the stated trade of section 3: about 0.4 to 0.5 chance of passing (inferred), at the cost of accepting a coverage loss of up to 0.10. Keeping M5's rule means about 0.15 chance and a likely wasted look.
3. **The memory limit for the second reranker?**
   - I recommend at most 1.5 GB of added resident memory, which rules out fp32.
   - 3.5 GB is more likely to pass but probably breaks "about 20 GB". That is not measured, because the laptop run is still pending.
4. **Is a download or install allowed if CPU half precision fails (MLX or GGUF)?** I recommend deciding after M0. The licence questions are yours: Qwen3-Reranker has no LICENSE file and the card calls the series "proprietary".
5. **The new hidden judged part: now, or after look 3?** I recommend writing it in M6 in parallel and using the existing 61 for look 3. Amend `MILESTONE.md:569-570` openly. It costs about 2 to 3 worker-days.
6. **A D5 reading before the look?** I recommend yes: about 8 sessions, and it may save a look.

**Not verified:**
- the breakdown of the 6.7 GB;
- Qwen's time with the server idle;
- the strict-against-loose shrinkage;
- every pass chance above;
- the classes of the M5 harmful rows.

None of this leans on the 40 legacy questions. The coverage and time figures rest again on the 54 worn development scenarios.
