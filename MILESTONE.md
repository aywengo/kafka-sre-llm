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
| X1 | Ranking (not met: r4 and the second reranker R5 were each stopped by their rules on 2026-11-22; B4 not spent) | On B4 (30 new open scenarios, used once): indexed must points served at least +15 percentage points over the frozen retrieval, and at most 5 % of the frozen-served points lost; the same direction in the hidden funnel totals | open |
| X2 | Wrong statements | On the hidden half, candidate minus reference in one blind mixed reading: upper end of the 90 % interval below 0 and beyond the readers' disagreement; aim 12 or fewer per 10; harmful at most 13 | open |
| X3 | No loss | Coverage not down by more than 0.08; mechanical not down by more than 3 points; wrong declines at most 1 in 10 | open |
| X4 | Length and time | At most 10 % of judged answers cut at the limit; the median time reported | open |
| X5 | Repairs | Defects 1 to 4 of the plan fixed, with tests, before any hidden run | F1 done (not yet committed); F2 open |

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
| F1 | Chain repairs: the E2 chain checks a stored manifest and markers at the start; the E5 chain stops when a step refuses; a resume survives a reload of the same model; the inline-marks step takes `--allow-commit-change` | implementer | CPU | **done** 2026-11-22 01:45 (`runs/m5/reports/f1_chain_repairs.md`); commit `6905aa5` |
| F2 | The scorer's decline outcomes and not-covered items made aware of the index in use; a guard on the default index path of `scripts/retrieve.py` | implementer | CPU | after F1 (shared files); not needed for r4, which keeps the index (the scorer's declines depend on the index and on the answer's own pack) |
| F3 | Release defects of v3.1.0 found by a fresh-clone test (`runs/m5/reports/v310_fresh_clone_check.md`): `scripts/build_catalogues.py` reads the untracked `runs/v3/INDEX_FROZEN.json`, so the README's checker setup fails (exit 2) in a fresh clone; the catalogue hash depends on absolute paths when the source folders are symlinks; in the clone 9 unit tests fail and 15 error (a3 catalogue files missing, `.venv/bin/python` assumed); README: closed-book judged coverage 0.387 not 0.38, the 10 s bar is not `GOAL.md`'s | the article fork's implementer | CPU | not done in this session: the article fork reports that the Owner decided there (2026-11-22) to release a v3.1.1 before the article, and prepares it on a branch `release/v3.1.1` cut from the tag, in its own worktree; nothing on `main`, nothing pushed or tagged without the Owner. This session makes no parallel fix; the fork names the commits to cherry-pick into `main` afterwards |
| R0 | Design of profile r4: the exact settings to try, the rule that picks one on the development pool, what the simulation prints, the expansion leak | complex-reviewer | CPU | **done** 2026-11-22 00:46 (`runs/m5/reports/r0_r4_design.md`) |
| R1 | Profile r4 and its funnel preset, next to the frozen profile | implementer | CPU, reranker | **done** 2026-11-22 01:50 (`runs/m5/reports/r1_r4_build.md`): frozen replay 423 of 423, runtime equals simulation 423 of 423 for all eight settings, live check 8 of 8; no r4 result on S or C looked at; commit `4e89031` |
| R1b | The timing screen, the funnel over the eight settings, the rule (`choose-r4`) | job-operator | CPU, reranker; server idle | **done** 2026-11-22 04:49: **the rule says stop** (`runs/m5/reports/r1b_r4_choice.md`) |
| R2 | The reviewer confirms the rule's verdict and records the chosen setting's hash; the verdict "stop" goes to the Owner | complex-reviewer | CPU | running since 2026-11-22 04:50 (confirms the verdict; where the points are still lost) |
| R3 | B4: 30 new open scenarios with covering chunks; independent chunk count at least 95 % before any answer; family check against the hidden items in a sealed session | writers, reviewers | Claude workers | second review applied (229 records); **the independent chunk count failed** (98 of 154 points, section "B4"); completed and recounted: 140 points with independently verified covering sets (02:08); the sealed family check and the chain of the one-time check are running |
| R4 | Expansions for B4, then the X1 rule applied once | job-operator | server 0.2 h | after R2 and R3 |
| G0 | Design of the grounded mode: the prompt text (variants G1 and G2), the rule that separates them, the reading plan on B2 | complex-reviewer | CPU | **done** 2026-11-22 00:50 (`runs/m5/reports/g0_grounded_design.md`) |
| G1 | The grounded mode as a new prompt constant with its own hashes; the default and the golden hashes unchanged | implementer | CPU | **done** 2026-11-22 01:38, commit `06dc7d5` (the `scripts/eval2.py` part and a refusal in `serve` follow F1) |
| G2 | B2 answered in the grounded mode; a blind reading mixed with the stored default answers; judge; open mechanical 339 | job-operator, readers | server about 3 h | after the development check (GD), which is running since 2026-11-22 09:01 (4.5 to 6.5 h) |
| G3 | Experiment: can the reranker's score tell supported from unsupported sentences, on the 56 errors of the B1 reading | implementer | CPU, reranker | after R0 (not on the critical path) |
| R5 | The second reranker (Q8): licence and usage read directly; design of the offline test and its rule; download; build; run | researcher, complex-reviewer, job-operator, implementer | CPU, one download of about 1.2 GB | licence read, design and download done (2026-11-22 08:21); **done** 2026-11-22 11:44: **the rule says fail** (section "R5: the rule's verdict") |
| R6 | Informative run, deciding nothing (Q11): the picked grounded variant answers B1 and the pilot on r4g2's packs; coverage (two judge passes), length, cut share, identifier check, against the same variant on the frozen packs | implementer, job-operator | server about 1 h | the chain is being built; the run follows the development check |
| H1 | Hidden look 3: r4 with the grounded mode against E2's stored default answers; ledger row and rule first | job-operator, sealed readers | server about 4 h | after R4 and G2 |

Not in M5 (P7): dense retrieval; index changes (the
Argo CD command pages, new chunking or titles); training.

## The rule for hidden look 3 (proposed in the plan; fixed in the ledger before the run)

Candidate C = the grounded mode on the frozen retrieval (the Owner's decision Q7 of
2026-11-22; the plan had r4 with the grounded mode, and r4 was stopped by its rule). A
second reranker (R5) joins the candidate only if it passes its own rule and B4 before the
look's ledger row is written. Reference R = E2's stored 4-bit default answers.
One fresh blind reading of both sets mixed (122 answers), a second reader on 25 %. C
becomes the default only if all hold: wrong statements per 10, C minus R, upper 90 % end
below 0 and beyond the readers' disagreement; harmful statements in C at most R's;
coverage, C minus R, lower end above −0.08; mechanical, C minus R, lower end above −3
points; wrong declines at most 1 in 10; median judged answer at most 81.8 s. Reported and
deciding nothing: funnel totals on the hidden 61, the share of cut answers, the identifier
check, totals without the 11 tagged items. Switching the default changes the golden packs
and `frozen.json` and is the Owner's decision after the look.

## Profile r4 (design R0, 2026-11-22)

r4 adds to the frozen pack and cannot lose a point: the six frozen chunks come first,
unchanged, then up to four more (10 chunks, 24,000 characters). The candidates are the 40
of the search plus the top 3 BM25 hits of each cleaned search term; the same reranker
scores a capped set of them in a second call, with a query that carries the terms and, in
some settings, a 1,024-token window. Eight settings are tried.

Measured in the design (BM25 only, no model): if the best four candidates were added, the
served share would rise by 61.3 percentage points on the pilot (+65 of 106 points), 55.4
on B1 (+92 of 166) and 54.4 on B2 (+81 of 149). The room is there; the limit is the ordering. R3, which filled the slots in the
frozen order, gained 7.8 and 7.4 percentage points. In the reranker's 512-token window a median B2 question
leaves about 970 characters per chunk, not the 1,500 assumed (inferred from token counts).

**The rule, fixed before any result (the conductor's decision, 2026-11-22).** The
development scenarios are split in two halves by a hash of their ids (42 items each; 202
and 219 indexed points; frozen serves 29.7 % on both). Settings that add more than 6.0 s
median or 9.0 s at the 90th percentile of reranker time are dropped first. The setting
with the largest gain on half S is chosen, a near-tie going to the smaller pack. It goes
to B4 only if it gains at least +25 percentage points on S (51 of 202 points) and +18 on
C (40 of 219), loses nothing, and stays
within the size limits. Otherwise r4 stops, B4 is not spent, and the Owner sees the
result. The reviewer's own expectation is a gain of +10 to +20 percentage points on development data, so
the stop may well fire (inferred).

Also decided by the conductor: the leak of answer-format instructions into the search
expansion is not cured at the source now (that would change the frozen six or add a model
call of 5.6 to 8.7 s); r4 cleans the terms itself. A second instance of the same reranker
is allowed (about 2.3 GB of memory, no download). The 81.8 s cap of look 3 stays, although
r4 adds about 10 s per answer (inferred); it holds only if the grounded mode shortens
answers.

## Grounded mode (design G0, 2026-11-22)

Two variants that differ in one rule: what the answer does when the excerpts lack a fact.
Both put the direct answer first, take names and claims about what a setting or command
does from the excerpts or the question, name a gap in one fixed sentence, never say that
something does not exist only because the excerpts are silent, drop background,
unasked alternatives and closing summaries, and ask for under about 600 words. **G1**
does not fill a gap from memory, except when the question asks for a single name, value
or verdict. **G2** allows general knowledge on labelled lines. The text goes after the
question. The exact text and its hashes are in the design.

What the design measured on open data (no model run):

- Of the 56 errors of the B1 reading (30 default answers, one classifier), a prompt can
  plausibly act on 36 (names 9, facts stated in prose 6, claims the excerpts are silent
  on 21), not on the 7 that contradict the answer's own excerpts or the 13 the excerpts
  seem to support.
- B1 default answers: 1,030 words and 2,048 tokens median, 19 of 30 cut; the length is in
  the body (numbered causes, whole programs), 8 % in closing sections, 1 % repeated
  excerpts.
- Strict on the open mechanical half (251 items): 174 passes against 194; 41 correct
  declines, 10 wrong declines, 5 abstentions. The 56 items it declined or abstained on
  hold 25 of default's passes. A mode that declines whenever the pack lacks the fact
  loses about 10 points on this scorer (inferred), hence G1's exception.

**The development check, rule fixed before the run.** Both variants answer B1 (30) and
the pilot (24) on the stored packs, and a fixed mechanical sample of 116 open items (the
56 that strict declined, 40 drawn from the other 195, 20 from the 88 a3 items). A variant
is eligible only if: judged coverage not more than 0.05 under default; supported essential
misses not more than default + 1.0 per 10; at most 5 of 54 answers cut; median at most
1,400 tokens; projected mechanical change not under −3.0 points; wrong declines at most
10 % of the items whose covering chunk is in the pack; nonexistent identifiers not above
default. G1 if eligible, else G2 if eligible, else stop without spending B2. Server time
2.6 to 4 h (inferred).

Clarified before the run (the conductor, 2026-11-22, after the chain's review): the
wrong-decline share is estimated with the sample's weights, because the sample
over-represents declines by design (it holds all 56 items strict declined on). Unweighted,
strict itself would read 10 of 46 covered items (21.7 %) on the sample, against 10 of 161
(6.2 %) on all 251 items; weighted, 6.8 %. The weighted share decides; the unweighted one
is printed beside it. Also changed after the review: B1's default answers are judged
again in the same run as the variants; the run's manifest pins the code; an unload's
result is checked; the server's echoed model is compared with the reference runs'.

**The B2 check, rule fixed before the run.** The picked variant answers B2 on the stored
packs; a blind reading of the 60 answers mixed, second reader on a quarter. Go on to the
hidden look only if: the readers' totals differ by at most 30 %; wrong statements per 10,
grounded minus default, at most −2.0 and larger than the readers' disagreement; harmful
not above default; coverage not more than 0.05 under default; supported essential misses
not more than default + 1.0 per 10; at most 3 of 30 answers cut; the mechanical conditions
of the development check held. Thirty pairs can show a change of about 5.8 per 10, so
this is a screen and look 3 decides. After r4 exists, the picked variant is run once on
r4's packs without readers (about 1 h of server time).

Disclosure: the designer loaded the stored B2 answer files while measuring lengths and
printed their token counts and cut flags only (default 21 of 30 cut; strict 11 of 30).
No B2 text was read; the prompt text was written from B1 and the mechanical half.

## r4: the rule's verdict (2026-11-22 04:49)

`VERDICT: STOP AND REPORT, NO B4: gain on S 48 of 202 points (+23.8 pp) is under +25 pp;
gain on C 37 of 219 points (+16.9 pp) is under +18 pp`

Measured on the development scenarios (84 items, loose count as on B1 and B2), local
reranker, no model. Time screen (20 questions): only r4g5 dropped (8.0 s median, 9.1 s at
the 90th percentile); the others add 2.9 to 4.9 s median. Best setting r4g2 (terms-only
query, 512-token window, 24 scored candidates): +48 of 202 on S (+23.8 points, standard
error 5.0) and +37 of 219 on C (+16.9, standard error 3.5); 3.0 s added. r4g6 (the same
with the 1,024-token window): +38 on S (+18.8) and +39 on C (+17.8). No setting lost a
point. Ratio of the gain on C to the gain on S for r4g2: 0.77; projected gain on a fresh
batch +10.1 to +16.9 points, against the +15 that X1 needs. The earlier candidate R3
gained 7.4 to 7.8 points.

The verdict was recomputed independently by a reviewer (`runs/m5/reports/r2_r4_reading.md`)
and holds under every reading of the design: the best gain on S is 48 points against the
51 needed (r4g2), the best on C is 39 against 40 (r4g6). An earlier wording of the bar in
this document ("+25 points on S and +18 on C") was loose: the bars are percentage points,
51 of 202 and 40 of 219 points, as the design states; it is corrected above.

Where the 421 indexed development points stand under r4g2: 210 served (frozen: 125); 100
scored by the reranker but ranked below the four added places, and 4 cut by the character
limit; 92 among the candidates but outside the 24 scored; 15 never a candidate. A perfect
ordering of the same 24 scored candidates would serve 103 more points than r4g2; a scored
set of 40 another 22; only a better search the last 15. So the limit is the reranker's
ordering, not the search. What separates the settings: the terms-only query (+44 points
over the question as query, standard error 2.8 percentage points); the 512 against the
1,024-token window does not (+8, noise). The pilot carries the gain (+34.9 percentage
points; it shaped the terms-only query); on B1 and B2 alone r4g2 gains +15.2 (standard
error 3.0), which is the B4 bar before any shrinkage. By kind on B1 and B2: configuration
+22.7, debugging +22.2, coding +11.3, diagnostics +10.3, architecture +7.1.

A survey of rerankers (`runs/m5/reports/reranker_research.md`; publishers' own numbers,
no download): Qwen3-Reranker-0.6B (Apache-2.0 on its page, about 1.2 GB), an Ettin
cross-encoder (Apache-2.0, 596 MB), mxbai-rerank-base-v2 (Apache-2.0, 988 MB); the Jina
v3 rerankers are non-commercial. No benchmark covers long questions over technical
documentation.

The stage stops here until the Owner has seen it (Q7). B4 is not spent and no r4 setting
is recorded as chosen. A reviewer is checking the verdict and where the points are still
lost; a researcher is listing other rerankers with their licences (nothing is downloaded).

## R5: the second reranker (design of 2026-11-22 08:21, before any score)

`Qwen/Qwen3-Reranker-0.6B`, revision `e61197ed…`, licence field `apache-2.0` on its model
card and in the hub's metadata (no licence file in the repository; the card's text calls
the series "proprietary"; the Owner knows both). Downloaded on 2026-11-22 08:17 (1.21 GB,
hashes equal the hub's, no token sent); an offline smoke test on the CPU scored the
card's two example pairs 7.75 and −12.25 (card: 7.625 and −11.375).

The test (`runs/m5/reports/r5_design.md`): the new model replaces only r4g2's second
reranker call and scores the same 24 candidates per question; the frozen six and the pack
rule stay. Four settings: the terms-only query or the first 800 characters of the
question, each with the card's default instruction or one task instruction fixed in the
design. No runtime profile is built before a pass.

**The rule, fixed before any score.** Counted like r4 (served indexed points, loose
count; S 202 points, C 219), each setting paired with r4g2 on the same items. The setting
with the largest gain on S is chosen (near-tie: the smaller pack, then the faster). It
passes only if all hold: at least +20 points over r4g2 on S (served at least 128 of 202)
and at least +15 on C (at least 117 of 219), each at least twice its paired standard
error; over the frozen retrieval at least +25 percentage points on S (51 points) and, for
this test, also +25 on C (55 points, so served at least 120 of 219; r4's bar on C was
+18); no frozen-served point lost; the new call at most 6.0 s median and 9.0 s at the
90th percentile on the 20 timing questions; added memory at most 3.5 GB; added characters
within r4's limits. Otherwise R5 stops with no profile and no B4. The C bar is raised
because B4 decides on the strict count and C has now been read point by point; a pass on
these worn items means "it orders better on items we have studied", and only B4 is clean.

Timing while the server is busy (fixed 2026-11-22 09:18, before any number): the model
server is occupied for hours by the grounded mode's development check, so the timing pass
runs beside it. A timing taken then is final only for settings that pass it, since a busy
machine can only make it slower; if a setting fails the time screen in that pass, the
rule's verdict is provisional until the timing is repeated with the server idle.

Correction of an earlier figure: the reading of the r4 result called 8 points "two
standard errors"; its standard errors were in percentage points, and 8 points is about
one. The bar above is the corrected one.

## R5: the rule's verdict (2026-11-22 11:44)

`VERDICT: FAIL, R5 ENDS WITH NO PROFILE AND NO B4: no setting is eligible`

Conditions: added memory 6,706 MB against at most 3,500 for all four settings (4,041 MB
with the current reranker alone, 10,747 MB with Qwen3 loaded); the new call's median
6.0, 6.1, 5.9 and 6.2 s against at most 6.0 (the current reranker in the same pass:
4.7 s), measured on a busy machine, so the three timing failures are provisional and the
memory failure is not; no frozen-served point lost; added characters within the limits.
The two question-head settings truncated one pair each in the timing pass.

The gains, which the rule did not evaluate because no setting was eligible (development
scenarios, loose count; frozen serves 60 of 202 on S and 65 of 219 on C; r4g2 108 and
102):

| Setting | S served | against r4g2 (SE) | C served | against r4g2 (SE) | 140 facts served |
|---|---|---|---|---|---|
| r5a (terms query, default instruction) | 111 | +3 (5.8) | 124 | +22 (7.5) | 119 |
| r5b (terms query, task instruction) | 111 | +3 (7.8) | 117 | +15 (6.2) | 115 |
| r5c (question head, default instruction) | 107 | −1 (8.7) | 112 | +10 (9.4) | 124 |
| r5d (question head, task instruction) | 94 | −14 (8.8) | 117 | +15 (9.4) | 126 |

So the gain bars would have failed too: the best setting on S is +3 points over r4g2
against the +20 needed. On C, r5a serves 22 points more than r4g2 (about three standard
errors) and clears the frozen bar (124 against 120). S is the half r4g2 was chosen on,
which flatters r4g2 there (inferred). Over both halves r5a serves 235 of 421 indexed
points, r4g2 210, frozen 125. Nothing was measured on answers.

X1 is not met by R5. B4 stays unspent. The Owner accepted the stop (Q11).

## B4 (the out-of-sample batch)

Thirty scenarios written on 2026-11-22 by three writers blind to the assistant's results,
assembled, and reviewed by three second reviewers (229 change records; 78 covering chunks
added). After the review: 170 must points, 154 with chunks (285 chunk references), 16 not
in the index. Files in `eval/m4/b4/`.

**The independent chunk count (K1) failed, 2026-11-22 01:52.** The conductor set a
stricter test for B4 than `agents/m4-evaluation.md` words it: every recorded chunk must
hold the point's fact by itself, because the funnel counts a point as served when any one
of its chunks is served. On that test three independent checkers found 98 of 154 points
ok (63.6 %; by range 34 of 48, 24 of 50, 40 of 56), 56 partial, 0 wrong; of 285 recorded
chunks 202 ok, 83 partial, 0 wrong. Of the 56 partial points, 27 have at least one chunk
that holds the whole fact alone and 29 have none (24 of those 29 are weight-2 points):
the fact is spread over several chunks or one named part of it is in no recorded chunk.
The reviewers' own counts had been 100 %. The checkers found 18 covering chunks the
reviewers had missed. The count under the rule as M4 worded it (the recorded chunks
together) was not made.

What this shows about the earlier batches (inferred): B1 and B2 passed K1 with "the
recorded chunks together" while the funnel credited any one chunk, so their served counts
include points of which only a part reached the model. The development numbers of r4
carry the same looseness.

Repair (01:55 to 02:08): three completion workers gave every point covering sets (one or
two chunks that together state the whole fact; a point is served when one whole set is
served); the first checkers recounted every new or combined set: 135 of 146 points with
sets ok (92.5 %), 264 of 281 sets ok, 17 partial. Only the sets the independent checkers
rated ok were written into the rubrics, so nothing unverified is recorded. Final state
(`eval/m4/b4/CHUNK_CHECK.json`): 170 must points; 140 with at least one covering set (14
of them only through a set of two chunks; 216 sets of one, 48 of two); 14 with no
covering set in the index; 16 not indexed. Not measured: how many covering sets the
searches missed. For a later judged use of B4: b4-03, b4-10 and b4-18 have more than one
weight-2 point that the index does not cover.
The Owner decided (Q5): the strict count decides X1 on B4. Committed on 2026-11-22 after
the Owner's decision on Q6 (the batch stays as written).

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
| 2026-11-22 | Q1 | "Wrong declines at most 1 in 10" is counted against the items whose excerpts cover the fact, as `GOAL.md` words it; the share of all declines is reported beside it |
| 2026-11-22 | Q2 | The grounded variant G1 may answer a single name, value or verdict from memory after the gap sentence |
| 2026-11-22 | Q3 | The development check of the grounded mode uses the mechanical sample of 116 open items |
| 2026-11-22 | Q4 | The grounded mode asks for under about 600 words |
| 2026-11-22 | Q5 | On B4 a point counts as served only when a whole covering set is in the pack; the looser count of B1 and B2 is reported beside it |
| 2026-11-22 | Q6 | The three hidden judged items of the same family as a B4 scenario (b3-07, b3-17, b3-26) are tagged and every hidden judged result is reported with and without them; B4 stays as written |
| 2026-11-22 | Q7 | The r4 stop is accepted: X1 is not met by r4, B4 stays unspent, and the grounded mode goes on with the frozen retrieval; the hidden look tests the grounded mode alone |
| 2026-11-22 | Q8 | Ranking continues in M5 with one reranker download, `Qwen/Qwen3-Reranker-0.6B`, for an offline re-score of the same scored candidates, with a rule fixed beforehand; if it does not clearly beat r4g2 it stops there |
| 2026-11-22 | Q11 | The stop of the second reranker is accepted: the ranking target ends for M5 with X1 not met; r4 and R5 stay in the code as experimental; after the development check the picked grounded variant answers B1 and the pilot once on r4g2's packs, without readers and deciding nothing |
| 2026-11-22 | Q9 | The conductor may unload any model from the model server ("you can unload all models from oMLX"), when no request is active or waiting |

## Waiting for the Owner

Nothing is open for the Owner now.

Still open from M4, none blocking:

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
- 2026-11-22 00:35: started the design of r4 (R0), the design of the grounded mode (G0)
  and three writers for B4 (R3).
- 2026-11-22 00:46: R0 done; the rule for choosing an r4 setting and the stop bar are
  fixed (section "Profile r4"). 00:48: the build of r4 (R1) started.
- 2026-11-22 00:50: G0 done; the prompt text of both variants and the rules of the
  development check and of the B2 check are fixed (section "Grounded mode"). Q1 to Q4
  wait for the Owner. 00:53: the build (G1) started, without `scripts/eval2.py`, which
  F1 still holds.
- 2026-11-22 01:17 to 01:45: B4 assembled (30 drafts), reviewed (three reviewers) and the
  229 change records applied. 01:38: G1 done and committed (`06dc7d5`).
- 2026-11-22 01:45: F1 done. The conductor's decision on its open point: a resume goes on
  after a reload of the same model id and records the reload; the chain of look 3 must
  also re-answer a few stored answers after a reload and compare them byte for byte.
- 2026-11-22 01:50: R1 done. 01:52: the independent chunk count of B4 failed on the
  stricter per-chunk test (section "B4"). 01:55: completion started.
- 2026-11-22 02:06: the chain tests passed (127 tests); F1 (`6905aa5`) and R1 (`4e89031`)
  committed and pushed. 02:08: B4's covering sets recounted and written (140 points).
- 2026-11-22 02:09: started R1b (timing screen, funnel over the eight settings, the
  rule), the grounded-mode harness and development chain (G1b), the chain of the one-time
  B4 check, and a sealed check of B4's families against the hidden judged items.
- 2026-11-22 02:12: the sealed family check is back (61 of 61 hidden judged items read;
  ids and counts only): 3 same family, 6 near, 10 B4 scenarios involved. Q6 for the Owner.
- 2026-11-22 02:21 to 04:48: the funnel over the eight r4 settings (224 questions, 2 h 27
  min, slowed by test suites running beside it). 03:46: the two chains committed
  (`a56289c`, `b799e4d`) after a clean full suite (1,832 tests).
- 2026-11-22 04:49: the rule's verdict on r4: stop, no B4 (section "r4: the rule's
  verdict"). 04:50: R2 (reading of the result) and a survey of rerankers started.
- 2026-11-22 04:56: R2 back: the verdict holds; the limit is the reranker's ordering. Q8
  for the Owner.
- 2026-11-22 08:10: the Owner agreed with the recommendations on Q1 to Q6; Q7 and Q8 are
  open. B4 committed. Started: the tag for the three hidden items, and a review of the
  development chain before its run on the model server. The server holds a model this
  session did not load (`DeepSeek-V4.1-Flash`, no active request); the run cannot start
  until the Owner unloads it or allows it (Q9).
- 2026-11-22 08:15: the Owner agreed with Q8: try `Qwen/Qwen3-Reranker-0.6B`. Started: a
  direct read of its licence, files and usage (no weights fetched), and the design of the
  offline test with its rule (R5). The download follows the direct read of the licence.
- 2026-11-22 08:15: the Owner accepted the r4 stop (Q7): the grounded mode goes on with
  the frozen retrieval. Q9 (the model on the server that this session did not load) is
  the one open question and blocks the development check.
- 2026-11-22 08:16: the Owner answered Q9: any model may be unloaded from the server.
  `AGENTS.md` updated. The development check starts when the chain's review is back.
- 2026-11-22 08:17: the direct read of the Qwen3 reranker's card is back (licence field
  apache-2.0, no licence file); the download ran and passed its checks. 08:21: the design
  of the offline test is back and its rule is fixed (section "R5"). The build started.
- 2026-11-22 08:23: the review of the development chain is back: one blocker (the
  wrong-decline share on a sample that over-represents declines) and five smaller
  points; the fixes are being made (section "Grounded mode"). The run follows them.
- 2026-11-22 08:30: the fixes committed (`8533cf6`). 08:31: the development check was
  started on the model server. Its gate passed, DeepSeek was unloaded and the 4-bit base
  model loaded. **08:36: it stopped at its determinism check**: of three B1 default
  answers replayed on the stored packs (model echo equal, packs equal 3 of 3), b1-01
  differed from the stored answer from character 129 on (7,787 against 8,376
  characters); b1-02 and b1-03 were identical. No variant answer was written. The M4
  identity check (60 of 60) had covered short answers only. Started: a diagnostic (the
  same replay twice more, to tell a drift since the stored run from a server that does
  not repeat a long answer), and the change the design foresaw: the chain answers its own
  default reference in the same run. Consequence to settle for look 3: E2's stored hidden
  default answers may not be reproducible either, so the look may have to answer its
  reference again.
- 2026-11-22 08:47: the diagnostic: three fresh replays of b1-01 are identical to each
  other and differ from the answer stored on 2026-11-19; b1-02 and b1-03 are identical
  in all four runs. So the server repeats a long answer today, and the stored answer
  drifted; the cause is not in the metadata (the conductor's guess, not tested: the
  prompt cache's state, since a replay is not preceded by the search-terms request).
- 2026-11-22 08:49: the chain answers its own default reference in the same run
  (`0946515`). 09:01: the development check restarted (the stopped run moved aside with
  the suffix `.stopped_20261122`); first nine reference answers 69 s each, 5 of 9 cut at
  the limit; estimate 4.5 to 6.5 h.
- 2026-11-22 09:17: a fork of this session, opened for an article, acted on M5 between
  about 08:53 and 09:17, taking three workers for lost, and then reported it: it
  committed and pushed the R5 build (`c5a3f42`) and the tag work (`e221971`) unchanged
  from this session's workers, without the full suite; it stopped two full-suite test
  runs of those workers with SIGTERM; it started and stopped an operator for the R5 runs
  before anything ran. It does no M5 work from now on. To reconcile: the full suite on
  the committed state (a run is in progress); the R5 runs are started by this session.
  Its fact check of the release found, for the repairs list: `scripts/build_catalogues.py`
  reads `runs/v3/INDEX_FROZEN.json`, which is not tracked, so the README's checker setup
  probably fails in a fresh clone (being tested there); the README gives closed-book
  judged coverage as 0.38 where the two-pass mean is 0.387; the README attributes the
  10 s bar for first words to `GOAL.md`, which sets only the 60 s bar.
- 2026-11-22 09:20: the fork's fresh-clone test confirms the defect: in a clone of tag
  v3.1.0 `scripts/build_catalogues.py` exits 2 (the untracked index record), so
  `ask --check` and `serve --check` cannot be set up from the README. Task F3 added; a
  v3.1.1 is asked of the Owner.
- 2026-11-22 09:27: the fork reports that the Owner decided in the fork to release a
  v3.1.1 and that the fork prepares it on its own branch and worktree. F3 is therefore
  not started here. Its test runs may load the CPU during the R5 timing pass; the
  timing's reading for a busy machine (section "R5") covers that.
- 2026-11-22 09:41: the full suite on the committed state (the R5 builder's run): 1,886
  tests, 1 failure, in the tag work pushed by the fork (a test pinned the old rule for a
  tagged id absent from the data). Fixed and pushed 09:46 (`e036bad`; the four affected
  modules pass, 173 tests; the full suite not yet rerun).
- 2026-11-22 10:48: the development check's reference stage done (54 judged and 116
  mechanical default answers); 10:52 to 11:08: G1 answered B1's 30 scenarios in 16.5 min.
- 2026-11-22 11:44: the R5 chain ended: timing (busy machine), funnel (224 questions),
  rule: fail (section "R5: the rule's verdict"). Q11 for the Owner.
- 2026-11-22 11:50: the Owner agreed with Q11. The chain of the informative run on
  r4g2's packs (R6) is being built.
