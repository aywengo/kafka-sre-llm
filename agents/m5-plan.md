# M5 plan: retrieval ranking, fewer wrong statements, repairs

**Status: the brief of milestone M5.** Drafted by a reviewer on 2026-11-22 from the M4 evidence (read-only, no model run, the hidden part
not opened). The Owner agreed with the recommendations of section 8 (recorded as P1 to P8 in `MILESTONE.md`) on 2026-11-22. Tracking:
`MILESTONE.md`. The evaluation's rules: `agents/m4-evaluation.md`. Run files named below (`runs/`, `work/`) are local and git-ignored.

## 1. Recommendation

- **Retrieval.** Build one new retrieval profile, r4, kept separate from the frozen one. It uses the per-term search, which nearly stopped needed chunks from falling outside the 40-candidate pool. It then orders the candidates with the same reranker, now fed the search terms and more of each chunk. The frozen six chunks always come first, and up to 10 chunks go to the model.
- **Wrong statements.** Build a new grounded default mode with its own hashes. It keeps claims to the excerpts and asks for shorter answers. A lower token limit is not the way to shorten them.
- **Repairs.** Fix the three chain defects before any hidden run.
- **Hidden looks.** Use one look on the combination of r4 and the grounded mode, and keep one spare.
- **Bar that stays out of reach.** The GOAL bar of at most 2 wrong statements per 10 answers cannot be reached in this milestone (section 4).

## 2. What the evidence says

### Where indexed must points are lost

The funnel results are in `work/m5/b1_funnel.json` and `work/m5/b2_funnel.json`.

| Batch | Indexed must points | Served (in the pack) | Ranked below the pack | Never in the 40 candidates | Cut by the character limit | Not indexed |
|---|---|---|---|---|---|---|
| B1 | 166 | 56 | 68 | 41 | 1 | 18 |
| B2 | 149 | 37 | 63 | 44 | 5 | 25 |

What the earlier candidates recovered:
- **R2** (per-term search, no reranker), on B1: gained 33 points and lost 12 of the 56 points frozen serves, a net +12.7 percentage points. Seven of the 12 losses were diagnostics points. R2 cut "never in the candidates" from 41 to 5, but those points mostly landed below the pack (83).
- **R3** (frozen pack plus extra chunks): +13/−0 on B1 (+7.8 pp) and +11/−0 on B2 (+7.4 pp). It adds about 5 to 6.5 s of prompt time (inferred, `runs/m4/reports/wp5_r3_build.md`).
- So the search step is mostly fixable; the ordering step is not fixed yet. The reranker sees only the question, and only the first 1,500 characters of each chunk in a 512-token window (`scripts/retrieve.py:72,82`).

**New, measured here** (`runs/m5/reports/scripts/conv.py`; open data, two judge passes, association and not cause):
- A must point whose chunk is in the pack is judged present 0.80 of the time on B1 default (90 of 112 verdicts) and 0.78 on B2 (58 of 74).
- An indexed point outside the pack is judged present 0.39 of the time (85 of 220) and 0.40 (89 of 224).
- Inference: each extra 15 pp of indexed points served adds about +0.05 judged coverage. That is below the hidden half's smallest visible difference of 0.07 to 0.10.

### Kinds of wrong statements

From the B1 reading (one classifier; `runs/m4/reports/wp2b_revise_design.md` §3): 56 non-minor errors in 30 default answers.

| Class | Count | Share |
|---|---|---|
| Names the catalogues can decide | 9 | 16 % |
| Contradicted by the answer's own excerpts | 7 | 13 % |
| Catalogue facts stated in prose | 6 | 11 % |
| Excerpts silent | 21 | 38 % |
| Excerpts seem to support it, or reasoning about the user's case | 13 | 23 % |

- 3 of the 4 harmful statements are in the last class.
- On the hidden half, the identifier check finds 2.62 nonexistent identifiers per 10 default answers against 0.33 in strict. Yet strict lowers the reading by only 2.6 per 10 (−5.6 to +0.3; 61 answers). The errors are mostly about behaviour, not names.

### Length and cut answers

None of this had been measured before. Measured on B1 and B2 without a new run (`runs/m5/reports/scripts/len_err.py` and `trunc_sim.py`):
- **Cut answers.** In the 19 default answers cut at the limit: 21.1 errors per 10. In the 11 uncut: 14.5 per 10. Strict: 20.0 against 8.7. Question difficulty is mixed into this.
- **Where errors sit.** They are spread evenly through the text: the quartiles of their position are 0.25, 0.49 and 0.79. That is about 2.5 errors per 10,000 characters.
- **Cutting the stored answers.** Cutting B1 default answers at half their characters keeps 60 % of the points the judge found present (112 of 187) and 52 % of the errors (29 of 56). On B2, 61 % of present points are kept.
- So a lower limit removes coverage and errors at about the same rate. Shorter answers have to come from the prompt.
- This agrees with the raised limit measured in M4 (J4: 4,096 tokens): coverage +0.01 (SE 0.02), while nonexistent identifiers doubled from 10 to 20 (not read).

### Omissions

- Hidden reading: 26.2 omissions per 10 answers (160 rows), of which 43 had their passage in the pack. Judge: 10.5 essential-miss units per 10, of which 1.1 were supported (61 scenarios, two passes).
- So 73 to 90 % of omissions are retrieval or index gaps. On B1 and B2, 10 to 14 % of must points are not in the index at all.
- Not known: how the hidden omissions split between ranking and search. Hidden per-item data stays closed.

### Strict mode's −5.25 points on the mechanical tier

On the 305 hidden items, strict against default:
- correct declines rose from 7 to 34 (the fact was not in the pack);
- wrong declines rose from 2 to 13;
- wrong answers fell from 48 to 21.

By kind, Argo CD commands went from 6 to 1 and settings by name from 80 to 70. The Argo CD command pages are excluded from the index (`sources.yaml:94-95`). The loss is a retrieval loss.

## 3. Work packages

**WP-R: ranking (profile r4).**
- **Idea.** Per-term BM25 lists merged with the main list, then the existing reranker with the search terms in its query and longer passages. The frozen pack comes first, then up to 10 chunks and 24,000 characters.
- **Alternatives set aside:**
  - R2 as it is: it lost 21 % of frozen-served points.
  - A deeper pool with today's reranker: net −2 and −10 points on the development items.
  - A second reranker or dense retrieval: needs a download (`agents/train-v3.md:155`); kept for later if r4 fails.
  - Ranking with the 27B: about +20 to 30 s (inferred).
  - Splitting chunks or new titles (an index change): mixed result on the development items (−6 at 10 chunks), and it would invalidate the not-covered items. Deferred.
  - Training: nothing argues for it while untried ranking settings remain.
- **What it touches.** Retrieval settings (reranker `max_length`, `chunk_chars`) and `candidate.json`. The frozen files, `tests/golden/packs_k6_k3_40.json` and the index stay unchanged until the Owner switches the default.
- **Data.** Development: pilot 24, B1, B2, open mechanical 339. Never the hidden half or the 40 legacy questions.
- **Fresh batch.** B4: 30 open scenarios with covering chunks, chunk check ≥95 % before any answer, search terms from an expansion-only run.
- **Open check, model-free, once.** On B4: served indexed must points ≥ +15 pp over frozen, and at most 5 % of frozen-served points lost.
- **Expected effect.** About +0.05 coverage and about +3 mechanical points in default (inferred), both below what the hidden half can show (0.07 to 0.10; 5.7 points). Strict, about +5 points, is near the limit. The real evidence is the funnel totals.

**WP-G: grounded mode.**
- **Idea.**
  - A new prompt constant: behaviour claims only where an excerpt supports them, and one sentence naming any gap.
  - No "does not exist" claims, which the existing RULES text already forbids (`prompts.py:39-40`).
  - A direct answer first, no generic sections, about 700 words.
  - The 2,048-token limit stays as a backstop.
- **Alternatives set aside:**
  - A lower limit: the cutting measurement above.
  - Strict as the default: −5.25 points mechanical.
  - RULES as it is: −0.15 on the worn 40 legacy questions, inside noise. It shortened answers from 4,714 to 3,761 characters and cut answers with an identifier missing from the pack from 28 to 5 of 40 (`runs/v3/CEILING_REAL.md`; this leans on the legacy set again).
  - A support check by the same model: the patch step disputed 7 of 7 true findings and wrote a harmful command.
- **Support check by the reranker.** Only as a CPU experiment on B1's 56 error sentences. Marks still count as errors (K4a), and it cannot see the "excerpts seem to support it" class.
- **Two equally defensible variants.** G1 limits names and claims to the excerpts. G2 grounds the claims but allows memory in a labelled line. The mechanical decline counts and the B2 reading separate them.
- **What it touches.** `prompts.py` (a new constant), `frozen.json` (new hashes), and new golden files. The default stays as it is.
- **Open check.** B2 has not been read for errors. Read it blind with the stored default answers mixed in, on the same packs. Open mechanical 339.

**WP-F: repairs, by the damage they can do to a hidden run.**
1. The E2 chain reuses a stored manifest and stage markers without checking them. A session restart already went unnoticed.
2. The E5 chain goes on when a step refuses.
3. A resume refuses after the server reloads a model.
4. Found here: the scorer's decline outcomes and the not-covered items are tied to the frozen index (`scripts/eval2_score.py:37-38,909-937`). They must be made index-aware before any index candidate.
5. The default index path in `scripts/retrieve.py:2,16` still points at `work/v2/chunks.jsonl`.
6. The expansion leak (53 of 251 expansions answer the question) is part of WP-R, because it changes the measured expansion.
7. The checker does not find names in prose and comments. This does not change any count.

**Hidden decision rule (look 3, fixed before the run).**
- **Configuration and reference.** C = r4 + grounded. The reference R = E2's stored 4-bit default answers.
- **Reading.** One fresh blind reading of both sets mixed, 122 answers, second reader on 25 %.
- **C becomes the default only if all hold:**
  - wrong statements per 10, C − R: upper 90 % end below 0 and beyond the readers' disagreement;
  - harmful statements in C at most R's;
  - coverage, C − R: lower end above −0.08;
  - mechanical, C − R: lower end above −3 points;
  - wrong declines at most 1 in 10;
  - median judged answer at most 81.8 s.
- **Reported, deciding nothing:** funnel totals on the hidden 61, cut share, identifier check, totals without the 11 tagged items.

## 4. Exit criteria (proposed; the bars are the Owner's)

| # | Criterion | Proposed bar | Against GOAL |
|---|---|---|---|
| X1 | Ranking | B4 funnel +15 pp, ≤5 % lost; same direction in hidden funnel totals | — |
| X2 | Wrong statements | Hidden difference: upper end <0, aim ≤12 per 10; harmful ≤13 | GOAL ≤2 cannot be reached |
| X3 | No loss | Coverage > −0.08; mechanical > −3; wrong declines ≤10 % | Gaps bar ≤1 in 10 |
| X4 | Length and time | Cut ≤10 % of judged answers; median reported | 60 s needs about 1,000 tokens; reached only if the length target lands |
| X5 | Repairs | Defects 1 to 4 fixed, with tests, before any hidden run | — |

**Why the 2-per-10 bar cannot be reached.** The "excerpts seem to support it or reasoning" class is 23 % of B1 errors. 0.23 × 17.2 ≈ 4.0 per 10 even with perfect checking and retrieval. 3 of B1's 4 harmful statements are in that class.

**Why 60 s needs about 1,000 tokens.** 12.5 s fixed + 1,100 tokens ÷ 32 per second + about 10 s for search + 6 s for the bigger pack ≈ 63 s (inferred).

## 5. Order of work, hidden looks, cost

**First slice (needs only the go for this plan):**
- WP-F 1 to 5;
- the r4 simulation on the development pool;
- the grounded prompt draft with dry runs;
- the reranker support-score experiment on B1.

**After that:**
- B4 and the B2 reading need the Owner's go.
- WP-R and WP-G run in parallel on CPU. On the server they run one at a time.

**Hidden looks.** 4 are left. Use 1, keep 1 spare (for example, to measure r4 and the grounded mode apart if the combination fails), and leave 2. A new hidden judged part should be built in M6, or once 2 looks have been used on the same 61 scenarios.

**Cost.** About 13 to 16 h of model-server time, about 8 sealed reader sessions, 8 to 12 worker-days.

## 6. Tasks

| Task | Role | Files (touch / not) | Output | Acceptance | Needs | Time |
|---|---|---|---|---|---|---|
| F1 | implementer | `scripts/m4_e2_starting_point.sh`, E5 chain, tests / no frozen files | Manifest and marker check at start; stop on a refused step; resume across a model reload | Tests show a stale marker refused, a refused step stopping, a reload resumed | CPU | 1–1.5 d |
| F2 | implementer | `eval2_score.py`, `retrieve.py` callers, tests | Index-aware declines; guard on the default path | 305-item shape check unchanged | CPU | 0.5 d |
| R1 | implementer | `profiles.py`, `candidate.json`, `retrieval_funnel.py` / frozen files | Profile r4 and funnel preset | Frozen packs 275/275; r4 runtime equals the simulation | CPU + reranker | 1.5 d |
| R2 | complex-reviewer | report | r4 settings chosen on the development pool; hash recorded | Rule written before reading | CPU | 0.5 d |
| R3 | writers + reviewers | `eval/m4/b4/` | B4, 30 scenarios | Chunk count ≥95 %; family check against hidden items in a sealed session | Claude; Owner's go | 1 d |
| R4 | job-operator | `work/m5/b4/` | Expansions, then the funnel rule applied once | Rule output | Server 0.2 h | 0.5 d |
| G1 | implementer | `prompts.py` (new constant), `frozen.json`, golden | Grounded mode | Golden tests unchanged | CPU | 0.5 d |
| G2 | job-operator + readers | `work/m5/b2g/` | B2 grounded answers; blind mixed reading; judge | Rule fixed beforehand | Server 3 h | 1 d |
| G3 | implementer | scratch | Reranker support-score test on B1 | AUC reported | CPU | 0.5 d |
| H1 | job-operator, sealed readers | ledger row first | Look 3 | Rule of §3 | Server 4 h | 1 d |

## 7. Risks and how each would show early

- **Fitting the development pool.** B4's gain is much smaller than the development gain (it was a third for R2).
- **Longer reranker input is too slow.** The R1 timing goes above 10 s per question.
- **The grounded mode declines too much.** Mechanical correct declines rise toward strict's 34.
- **Shorter answers drop essential steps.** Supported essential misses go above 1.1 per 10 on B2.
- **Readers disagree.** Their totals differ by more than 30 %.

## 8. Decisions for the Owner

1. **Carry the 4 remaining looks into M5?** `AGENTS.md` says "at most six in M4". Recommended: yes, using at most 2. No means building a new hidden part first, about 2 days.
2. **Renew the go for loading the 4-bit model for M5 measurement runs?** `AGENTS.md:70` covers M4 only. Recommended: yes; without it nothing in M5 can be measured.
3. **Write B4 (bulk generation)?** Recommended: yes. Without it r4 has no out-of-sample check.
4. **Read B2 for the grounded mode?** Recommended: yes. It is the only open set not yet read for errors.
5. **Reranker settings as a candidate profile, the second reranker deferred?** Recommended: yes.
6. **Index changes?** The Argo CD command pages and the tool options from `raw_catalogue` would contradict `catalogue_sources.yaml:3-4`. Recommended: defer. The licence questions are the Owner's; no legal conclusion is given here.
7. **Switching the default later?** That changes the golden packs and `frozen.json`, and the published v2 adapter was trained on today's prompt format. Decide after look 3.

## 9. Not verified

- That bge-reranker-v2-m3 handles longer inputs, and its CPU time with them.
- The present-when-in-pack link is an association.
- The cutting measurement uses the judge's first quote only; 4 and 11 quotes were not found in their answers.
- The six error classes come from one reviewer.
- The generator's tokenizer was not measured, so the time figures are inferred.
