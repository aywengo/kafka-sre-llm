# Mission: kafka-sre v3, stage 1 — a small open-book adapter

This amends `agents/train-v3.md` after Phase 0. Where the two disagree, this file wins. Rules
in `train-v3.md` and `train-v2.md` that are not changed here still apply (Roman's go for
uploads, pushes, deletes, installs and downloads; GPU exclusivity for training and local
answer generation; GPU jobs under `scripts/run_guarded.py`; resumable jobs;
`runs/v3/HEARTBEAT.md` and `status.json` kept true). Keys and server addresses stay in
`~/.config/kafka-sre-llm/` or the environment and are never written into the repository.

Approved by Roman on 2026-11-16: steps 1 and 2 below, the corpus changes, the gap behaviour,
and amending the brief. Steps 3 and 4 need his go after the ceiling check. Items marked
**(pending Roman)** were proposed after two independent reviews of this plan
(`runs/v3/handover/openbook_stage1_design.md` and a review of 2026-11-16) and are not yet
confirmed.

## What Phase 0 found (details: `runs/v3/PHASE0_REPORT.md`)

On the 40 real questions, GLM-5.3-Flash-oQ8e-mtp, two passes, change against the plain base model:

- base model + retrieval (k=6): **+1.19**; with k=3: +1.05 (the 0.14 difference is noise);
- v1, no retrieval: +0.08; v2, no retrieval: −0.18; a "be thorough" prompt or thinking on do
  not rescue v2;
- v2 + retrieval (trained without packs): +1.18, a tie with base + retrieval head-to-head
  (+0.01), at 295 tokens against 1,217;
- invented flags, fields or commands on 10 answers read in full: base + retrieval 7 items in
  5 answers, v2 + retrieval 2, base + retrieval with the strict sentence 0 (at 516 tokens);
- on the three questions the corpus cannot answer, retrieval lowers the score (−1.00);
- recall on the packs in use (the base model writes its own search terms): 25 of 37
  answerable; with DeepSeek's terms: 28 of 37.

So closed-book training is dropped. Long retrieval answers lose by inventing, short ones lose
by omitting.

**Noise.** On the existing head-to-head (v2 + retrieval against base + retrieval, 40
questions, two passes) the per-question differences have a standard deviation of 1.33, so
the standard error of a 40-question mean is about 0.21. A difference of 0.2 is one standard
error: a contestant with no real edge reaches +0.2 about one time in six.

## Goal of stage 1

Test cheaply whether an adapter trained **with the pack in the prompt** writes answers that
are as complete as base + retrieval and as clean as the strict variant, and says so when the
excerpts do not cover something. RAG on the base model stays the baseline that ships either
way.

## Gate (fixed before any adapter answer exists)

Primary gate, on `questions_real`, GLM-5.3-Flash-oQ8e-mtp, two flipped passes, all 40 scored:
the adapter with retrieval is at least **+0.2 head-to-head against base + retrieval**, with
more wins than losses. Both contestants answer from the same packs; the base model writes
the search terms for both (adapter off for that step).

Reported with it, and required to agree in sign: the same head-to-head on the runbook
holdout (42) and on Roman's new questions; the pooled result over all three sets; the
result on `questions_real` without questions 29, 37, 38 and 39 (corpus additions were aimed
at their topics); a DeepSeek judging of the gate head-to-head as the second judge; the
count of code-like tokens absent from the pack and the corpus, against base + retrieval.

The brief's gates 1 to 3 (against plain base, category floor, holdout against plain base) are
reported as context in stage 1 and apply in full at stage 2.

**Narrow miss (pending Roman).** A result of +0.1 to +0.2 with fewer invented items than
base + retrieval and the holdout at 0 or better allows stage 2 only on Roman's go. Below
+0.1: no stage 2, ship RAG on the base model.

## Changes to the brief

| Brief | Stage 1 |
|---|---|
| Decision point 3: small packs (k=3 / 6,000) on all rows | **Full packs (k=6) on about 1,500 rows.** k6 and k3 score within noise; k6 has the better recall and gives fewer, richer rows. The adapter learns a behaviour, not facts |
| Phase 1: 4,500 questions | **About 3,800** plus a reserve of 600 that is not packed, with 8% of seed files reserved for an evaluation slice. Planned yield to training rows is about 0.5 and is re-measured at the pilot |
| Phase 1: questions only from seeds | Plus about 250 **out-of-corpus questions** from a fixed list of adjacent topics (written without opening `questions_real`), as the source of not-covered rows |
| Phase 2: `max_chars=14000` | The cap only decides how many chunks are selected; the prompt holds their **full text** (measured packs, base-model terms: median about 12,800 chars, max about 19,800; DeepSeek terms run larger: about 14,200 / 20,500; about 3,000 to 3,600 tokens median, 5,600 max). Keep that format: it is what scored +1.19 |
| Phase 3: unanswerable questions return SKIP | SKIP only for malformed questions. Uncovered and partly covered packs become training rows, see "Gap behaviour" |
| Phase 3: never mention the material | Still true, except for the one fixed gap sentence |
| Phase 3 warning: troubleshooting mean under 1,200 chars | Warning at a troubleshooting median under about 2,500 chars (strict was 2,145 and lost on completeness; base + retrieval was 4,822). Answers are capped at about 1,800 tokens, because evaluation runs at `max_tokens` 2048 |
| Phase 4: GLM re-verifies troubleshooting items, plus a 400-item audit | GLM (`oQ4e`) checks **every** row DeepSeek passed; 20 kept rows are read claim by claim instead of the audit |
| Phase 5: carry over v2 config-ref, incidents and runbook rows | **No v2 rows.** They are short and closed-book, and v2 scored −0.18 |
| Phase 7: as v2, "not fewer steps" | `max_seq_length` 8192; batch 1; `grad_accumulation_steps` 2; `iters` 1500 (mlx-lm counts micro-batches), cosine over **750 optimizer updates**, warm-up 50; `save_every` 50; adapter `adapters/kafka-sre-v3-s1`. Fewer updates than v2 (2,009), each carrying about 1.5 times the answer tokens |
| Phase 7 mid-run check | Checkpoints are evaluated after training (20 evaluation-slice questions at checkpoints 500 and 1000); training is not stopped mid-run |
| Phase 9 contestants | Drop v3 closed-book and the v1 head-to-head; add head-to-heads against the strict prompt and on the runbook holdout |

Cut from stage 1 after review: the **expansion rows** (question -> search terms in the same
adapter). The evidence for them is 28 against 25 of 37 on loose regexes, and they would mix a
retrieval change into the answer comparison. They can be added at stage 2.

Note on batching: in the installed mlx-lm the sort by length is a no-op for chat datasets
(`CacheDataset.itemlen` returns 1), so batches are consecutive rows of `train.jsonl` in a
seeded permutation. The resume command must rebuild that order.

## Gap behaviour (Roman, 2026-11-16)

When the excerpts do not cover the question or part of it, the answer says what is not
covered in one fixed sentence form, gives what the excerpts do say that helps, and adds
**no** flags, fields, commands, metric names or settings from memory. It may add general
direction in plain words (what kind of thing to check), labelled as general guidance, with
no identifiers. An answer that only says "not covered" is a failure: in Phase 0 a 34-token
"not covered" answer scored 1 where the full answer scored 5.

Training mix: about 68% fully covered, 25% partly, 7% not covered. Paired "withheld" rows
(the seed chunk removed from the pack before generation and backfilled from the next ranks;
the verifier also sees the withheld chunk and rejects any fact found only there) count
inside the 25% and 7%, up to about 150 rows. Coverage labels come from the generator and
are checked by both verifiers.

## Corpus (Roman, 2026-11-16)

- **Not used:** docs.confluent.io. Its terms of use forbid copying and scraping.
- **Removed from the index:** the `confluent-schema-registry` source (README and API spec,
  Confluent Community License), so that everything indexed is openly licensed.
- **Added, all Apache-2.0:** Apicurio Registry docs (compatibility modes and rules), Karapace
  docs (Confluent-style REST API), the cAdvisor Prometheus metrics page, the
  prometheus-operator `CPUThrottlingHigh` runbook.
- **Two runbooks of our own**, drafted from Apache-2.0 code and docs and reviewed by Roman:
  the schema-registry wire format and "Unknown magic byte", and reading the CPU throttling
  metrics.
- The two unpinned sources (`kubernetes`, `apache-kafka-config`) are pinned to the commits
  already checked out.
- After the index is rebuilt, base + retrieval is re-run once on `questions_real` and judged
  against the old answers. That index is then frozen for stage 1
  (`runs/v3/INDEX_FROZEN.json`: file hash, reranker revision, retrieval parameters); every
  later task asserts the hash.

## Steps

1. **Question bank, packs, pilot** (about 8 h). `scripts/gen_questions.py` (DeepSeek, leakage
   filter against every `eval/questions*.jsonl`), `scripts/build_packs_v3.py` (DeepSeek
   search terms, one CPU rerank per question, resumable), `generate_qa.py --mode pack` with
   two verifiers. Pilot of 40 bank questions (20 troubleshooting, 6 withheld, 3
   out-of-corpus). Gates:
   - skips at most 30%;
   - at least 80% of fully covered troubleshooting answers name 2 or more causes, each with
     a check and a fix;
   - of 10 items read claim by claim, at most 1 has an unsupported claim; no fact found only
     in a withheld chunk;
   - `scripts/invention_count.py` (frozen regex and allowlist): code-like tokens absent from
     the pack in at most 2 of 40 kept rows;
   - false gaps (a "not covered" claim where the pack holds the fact) in at most 5% of rows
     labelled fully covered.
   Also reported: length of kept rows against generated rows and reject rate by length
   quartile (verifiers must not bias the set short), verdicts that failed to parse, rejects
   per verifier and their disagreement, seconds per row with GLM running alongside, yield.
   A failed gate stops the stage until Roman has seen it. **Roman:** 3 items (fully covered,
   partly covered, how-to) and the numbers. The generation prompt is frozen after the pilot.
2. **Ceiling check** (about 4 to 7 h). The teacher (DeepSeek, frozen pilot prompt) answers
   the runbook holdout from its packs, and the answers are judged head-to-head against base
   + retrieval, two flipped passes. **(Pending Roman:)** the same on `questions_real`, and a
   "prompted student" probe (the base model with the generation rules as instructions).
   Rule, fixed in advance: **stop** if the teacher is under +0.3 on `questions_real`, or its
   wins do not exceed its losses, or the pooled 82 questions are under +0.3; **go** if it is
   at least +0.4 on `questions_real` and at least 0 on the holdout; in between Roman decides.
   If the prompted student reaches +0.2, the recommendation is to ship the prompt and train
   nothing. None of these answers is ever training data. Reported next to the scores: the
   count of tokens absent from the pack, teacher against base + retrieval. **Roman:** go or
   stop for steps 3 and 4.
3. **Bulk generation with two verifiers** (about 28 h; GLM alone needs about 19 h for 1,750
   rows), then the dataset build. Stop and report if the verifier reject rate passes 40%, if
   the yield at the first 300 items is under 0.40 (then pack the reserve), or if the
   projected time passes 35 h. The user message of every row is built by the same function
   that built the Phase 0 prompts, checked byte for byte. Shape gate on fully and partly
   covered rows; not-covered rows reported separately. **Roman:** blind spot check of 30
   items, at least 5 of them gap rows.
4. **Training** (35 to 55 h, settled by the probe). Probe A on the 40 longest rows for peak
   memory; probe B on random rows for speed. In the probe, trained tokens per iteration must
   be close to answer tokens (the prompt mask works with packs), and the learning rate must
   follow the update count. A stratified validation set of 60 rows (fully, partly, not
   covered). A tested resume command. **Roman:** go, with the probe numbers.
5. **Gate**, as defined above. Judge-free signals first (mean tokens, tokens absent from the
   pack, gap-sentence rate on covered packs, 10 answers read in full).

## Evaluation sets

`questions_real` has been used for the recall gate, the baseline, two prompt variants and
the corpus additions. No prompt changes after adapter answers are seen. Fresh evidence: the
runbook holdout (42 questions, never used for tuning; short factual questions, mostly
Strimzi, whose facts are in the index), the bank's evaluation slice (judge-free measures
only), and new real questions that Roman drafts in `eval/questions_new.jsonl` (same fields as
`questions_real`; **pending Roman:** 30 instead of 10 to 15, since 15 questions have a
standard error of about 0.35). No eval question may appear in any training-data prompt; the
ceiling check is the only place where eval questions are put to the teacher, and its answers
are never training data. The leakage filter is re-run whenever an eval file is added.

## Who runs it

From 2026-11-16 the work is run from Claude as conductor, one worker per task, each on the
role and model defined in `.claude/agents/`. Standing go (Roman, 2026-11-16) for loading and
unloading DeepSeek-V4.1-Flash, GLM-5.3-Flash-oQ4e and GLM-5.3-Flash-oQ8e-mtp on oMLX when no
other client is active; every swap is noted in `runs/v3/HEARTBEAT.md`. Commits are pushed to
`origin/main` at checkpoints (Roman, 2026-11-16), after every commit to be pushed has been
checked for keys, IP addresses, home paths and other sensitive data.
