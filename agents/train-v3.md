# Mission: kafka-sre v3 — retrieval-grounded generation

v2 failed its gate and scored below v1 on the 40 real questions (GLM: v1 +0.18, v2 −0.15;
kafka-ops −1.0). Its data was accurate but short: mean training answer 868 chars (v1 1,135),
and the tuned model now answers real questions in ~185 tokens where the base model writes
~1,400. Main suspected cause: every answer was grounded in ONE excerpt, so answers could only
be as broad as one excerpt. Read `runs/v2/HEARTBEAT.md` (Phase 6 and diagnostic entries) first.

Length is not the whole story, and v3 must not treat it as one:

- kafka-ops is below base in every tuned variant whatever the answer length: v1 −0.8 (training
  answers 1,135 chars), v2 −1.0, diagnostic variants −1.2 to −1.3.
- The final adapter at scale 10 wrote longer answers (283 tokens) and still scored −0.30.
- Tuned answers invent things (Connect REST endpoints, a `KafkaConsumer` CRD, `--export`
  output). That is a grounding problem at inference time, not a length problem.

So v3 has two aims: complete answers, and no more invention than the base model.

v3 keeps v2's grounding rules but changes what an answer is grounded in: **the 4–6 most
relevant chunks from all pinned sources**, retrieved for a realistic question, instead of the
one chunk the question was written from. Answers can then be complete (several causes, checks
and fixes from several pages) and still contain nothing the docs don't say.

## Decision points before this mission starts (Roman)

1. **The v2 diagnostic: ANSWERED (2026-11-15).** On `questions_real` (oQ8e, 40 of 40 scored):
   checkpoint 600 −0.62, checkpoint 1050 −0.48, final at scale 20 −0.18, final at scale 10
   −0.30 (`work/v2/diag/results_*_oq8.jsonl`). Earlier checkpoints are worse, so v3 does not
   train fewer steps; scale 20 is not worse than scale 10, so **v3 keeps LoRA scale 20**.
   Every variant is below base, and kafka-ops is below base in all of them (−0.8 to −1.3).
2. **RAG baseline first.** Phase 0 measures base model + retrieval with no fine-tuning and is
   run as its own step; the mission stops after it and waits for Roman. Rules for the
   Phase 0 report (the agent applies them and states which case holds; Roman confirms):
   - Recall under 28 / 40: decide nothing else. The RAG baseline and the v3 packs both
     depend on retrieval; propose the retrieval fix and wait.
   - RAG base ≥ v1 + 0.3 (paired, oQ8e): closed-book (A) is dropped. The choice is
     open-book (B) or shipping RAG on the base model as is.
   - RAG base within 0.3 of v1: go to decision point 3.
3. **Closed-book or open-book training.** As first drafted, v3 answers are written from a pack
   but the model is trained on question → answer with no pack in the prompt. A rank-16 LoRA
   over one epoch learns the style, not the facts, so long closed-book answers risk more
   invention. Options:
   - (A) closed-book only, as first drafted;
   - (B) open-book: training rows carry the pack in the user message, formatted exactly as in
     Phase 0 step 3, and the adapter ships with retrieval;
   - (C) both, with separate adapters.
   Rules for the Phase 0 report:
   - RAG base ≥ base + 0.3: (B).
   - RAG base under base + 0.3: (A) is the default, but flag it for Roman. (A) is the same
     kind of training that gave +0.18 in v1 and −0.15 in v2, with kafka-ops below base both
     times, so he decides whether a third attempt is worth ~5½ days.
   - RAG base at k=3 / 6,000 is ≥ 0.3 below RAG base at k=6 / 14,000: small packs handicap
     open-book. Then (B) uses full packs on the ~1,500-row subset instead of small packs on
     all rows (and Phases 2–3 keep the full packs).
   - (C) only if Roman wants the closed-book vs open-book comparison for the article; it
     costs both trainings (~1 day + roughly 3 days).

   **Open-book is only feasible with small packs** (or the subset above). With full packs (k=6, 14,000 chars,
   ~3,500 tokens) a row is ~4,000–4,500 tokens against ~500 in v2, which took ~22 hours;
   4,500 such rows would take roughly 4–7 days and more peak memory. So for (B) and (C) the
   pack is **k=3, capped at ~6,000 chars (~1,500 tokens per row)**, decided now, not at the
   probe. Consequences:
   - The answer must be written from the same pack the training row shows, or the row
     teaches the model to state things its context does not contain. So with (B) or (C),
     Phases 2–3 build and use the k=3 / 6,000 packs for generation as well.
   - v3 + retrieval is evaluated with the same pack shape, and Phase 0 also records RAG base
     at k=3 / 6,000 so gate 4 compares like with like.
   - If the Phase 7 probe still projects over 2 days with small packs: train on a subset of
     ~1,500 pack rows.
4. **Standing go for judge swaps: GIVEN (Roman, 2026-11-15).** Phase 0 and Phase 9 each need
   the oQ8e ↔ DeepSeek swap; the agent may do those named swaps without asking. It must
   still check, before every swap, that no other oMLX client is active, and wait for Roman
   if one is.
   **On-demand loads of the mission's own models are also covered (Roman, 2026-11-15).**
   oMLX loads models on demand, as in v2. The agent may, without asking:
   - load DeepSeek-V4.1-Flash for generation and verification (Phases 1, 3, 4) and as the
     second judge;
   - load `GLM-5.3-Flash-oQ4e` next to DeepSeek for the Phase 4 check and audit;
   - load `GLM-5.3-Flash-oQ8e-mtp` as the primary judge (Phases 0 and 9);
   - unload any of these three when its job is done or when GPU exclusivity is needed for
     training or answer generation.
   Conditions for every load or unload: no other oMLX client is active (otherwise wait for
   Roman); the models that will be loaded together fit in memory (oQ4e + DeepSeek fit;
   oQ8e goes alone); and the change is noted in `HEARTBEAT.md` with what was loaded and
   unloaded.
   Still needs Roman's go: any model not named here (for example an embedding model for
   retrieval), any download or install, and unloading a model the mission did not load.

## Goal

`adapters/kafka-sre-v3` on the same base (`runs/v1/base27b`) that passes the gate (Phase 9)
AND beats both v1 and the RAG baseline on `questions_real` (margins in Phase 9). Done = data built and checked
(Phases 1–6), adapter trained, eval with both judges and the RAG baseline, packaging local
only, `runs/v3/REPORT.md`. Nothing uploaded.

## Rules

All rules from `agents/train-v2.md` apply unchanged (cloud operator model; Roman's go for
uploads, push, deletes, installs, oMLX model changes; secrets from env only; commit rules;
GPU exclusivity for training and answer generation; resumable jobs with retry wrappers;
small context; heartbeat with the cron form, plus `scripts/heartbeat.py` during long jobs —
point it at the right log and config every time a job changes). Four additions from v2:

- **GPU jobs run under `scripts/run_guarded.py`** (training, answer generation, anything that
  loads a model locally), in place of `scripts/run_retry.sh`. It restarts a job that died of
  a GPU or memory problem after 20 minutes, at most 6 times, writes `GUARD:` lines to the
  job's log and, with `--heartbeat runs/v3/HEARTBEAT.md`, an entry per restart. Any other
  failure is not restarted. This replaces v2's "if it hangs again, do not restart".
  - Jobs that resume from their own output (generation, `compare.py` with caches) need
    nothing more.
  - Training must be started with `--resume-cmd`, a command that builds the resume config
    from the last checkpoint in the exact batch order (as `runs/v2/lora_resume.yaml` was
    built) and runs it. Without it the guard refuses to restart training, because a rerun
    would start at iteration 0 and overwrite the adapter. Write and dry-run that resume
    command before training starts.
  - Use `--stall-min` (for training: at least 3 times the gap between log lines) so a
    silent hang is caught too.
  - Exit 75 = gave up after 6 restarts, or training without a resume command: needs Roman.
  - **To stop a guarded job, send SIGTERM to the guard's PID** (it stops the job's whole
    process group and does not restart). Never kill the job or its children directly, and
    never with `kill -9`: a SIGKILL looks like an out-of-memory kill and the guard restarts it.
  - The guard runs the job in its own process group, stops the whole group on a stall, and
    refuses to restart (exit 75) if any process of the old group survived, so two GPU jobs
    never run side by side. If that happens, report the surviving PIDs and wait for Roman.
- **Stop or repoint `scripts/heartbeat.py` when its job ends.** Never leave it running against
  a finished job: in v2 it kept appending "Phase 4 Train — DONE" every 30 minutes and left
  `status.json` at phase "4 Train", `needs_human: false`, after the gate had failed and while
  a diagnostic was running. When a phase ends, a gate fails or you are waiting for Roman,
  stop it (or restart it on the new job's log) and make sure `status.json` shows the real
  phase, state and `needs_human`, matching the latest `HEARTBEAT.md` entry.
- **Every check-in reads the current job's log itself.** If a job stopped, report the last
  progress line and wait. Never rely on the heartbeat file alone (v2 lost 10 h this way).
- **When waiting for Roman, say so once and end the turn.** No repeated status messages.

## Judges (Roman's decision)

- **Primary judge for every v3 score: `GLM-5.3-Flash-oQ8e-mtp`**, the baselines included. Scores from
  the earlier `oQ4e` judge are not comparable and are not used in v3 tables.
- Second judge: DeepSeek-V4.1-Flash (thinking off), as in v2.
- Memory: oQ8e (~319 GB) does not fit next to DeepSeek (~302 GB). Load one judge at a time
  (Roman's go for each load/unload while other clients may use oMLX, unless he gave the
  standing go in decision point 4).
- Data checks during generation (Phase 4 audit and re-verify) keep `GLM-5.3-Flash-oQ4e`
  (~169 GB), which fits next to DeepSeek. That is a data filter, not a score.
- Judge noise: identical base answers scored 3.00 to 3.12 across runs of the same judge, so
  differences under ~0.2 on `questions_real` are treated as noise in the report.
- Every gate score is the mean of **two judge passes**, compared **paired per question**
  (mean delta plus wins / ties / losses). All 40 real questions must be scored; re-run any
  unscored item instead of dropping it (v1 had 39 scored, one diagnostic run 37).

## Phase 0: Retrieval and the RAG baseline (≈ ½ day + ~2 h for the v2 variants)

1. `scripts/retrieve.py`: BM25 over `work/v2/chunks.jsonl` (all pinned sources + 24 training
   runbooks; the 8 held-out runbooks are NOT indexed), with the chunk title indexed twice
   (titles carry config keys and CRD names). Function: `retrieve(question, k=6,
   max_chars=14000) -> [chunks]`. No new model downloads without Roman's go; if BM25 recall
   is poor in step 2, propose an embedding model and wait.
2. Recall check, in two parts so retrieval is not tuned on the gate set:
   - Tune (title weight, k, max_chars) on `eval/questions.jsonl` only. Read the chunk text,
     not just the titles, and mark whether the facts needed for a correct answer are there.
   - Then freeze the parameters and run once on the 40 real questions. Report hits / 40.
     Gate: ≥ 28 / 40. If it fails, do not adjust retrieval on these questions: propose a
     change (for example an embedding model) and wait.
3. RAG baseline: base model answers `questions_real` with the retrieved chunks in the user
   message ("Use these documentation excerpts where relevant"), max_tokens 2048, then GLM
   judges RAG-base vs plain base (judge: GLM-5.3-Flash-oQ8e-mtp). Record `rag_base` scores; they
   are a second bar v3 must beat. Also record RAG base with the small pack (k=3, 6,000
   chars), needed if decision point 3 goes open-book.
4. Same judge, for the final table: re-judge v1 (`eval/answers_2048.jsonl`, tuned) against base
   on `questions_real` with oQ8e, reusing the existing answers. v2 final (scale 20) is already
   judged with oQ8e (`work/v2/diag/results_final20_oq8.jsonl`: −0.18); reuse it, and add the
   second judge pass the gate rules need.
5. Inference-only variants of v2 (final adapter, scale 20) on `questions_real`, ~2 extra
   hours, same judge. They answer "would a prompt have saved v2" for the article:
   - v2 with a "be thorough" instruction added to the prompt;
   - v2 with thinking on (the base in use is a no-think build, `scripts/make_nothink_base.py`;
     if thinking cannot be enabled on it without a new base, say so and skip);
   - v2 + retrieval (same packs as step 3).
6. Report to Roman and stop: recall, the RAG base / v1 / v2 table with the v2 variants, and a
   recommendation on decision points 2 and 3. Phases 1 onward start only on his go.

## Phase 1: Question bank (≈ ½ day, DeepSeek, thinking off)

Questions come from operator situations, not from what one excerpt happens to say.

- **Seeds:** (a) every runbook section and every chunk under Kafka `/operations/`, Strimzi
  `managing`, `operators`, `cruise-control`, `upgrading`, `security`, `deploying`; (b) a
  sample of the remaining chunks so every source is covered.
- **Prompt:** "Write 2 questions an on-call engineer would actually ask that this material
  helps answer: symptoms, alerts, error messages, 'how do I safely…', 'what happens if…'.
  The question must make sense without the material." Mix: ~50% troubleshooting, 25% how-to /
  procedure, 15% config review, 10% concept or comparison.
- **Style examples:** you may show 5 questions from `eval/questions.jsonl` as style examples.
  **Never** use `questions_real`, the runbook holdout or the feedback set in prompts.
- **Category quota:** tag each question with the `questions_real` categories and keep the bank
  close to that set's mix, with kafka-ops and kubernetes (the failing categories) each at
  least their `questions_real` share. Report questions per category.
- **Leakage filter:** drop any question with normalised BM25 similarity ≥ 0.6 to any eval
  question (all eval sets), or a shared 8-word span with one. Normalised = BM25 score of the
  candidate against the eval question, divided by that eval question's score against itself.
  Report the drops.
- Dedupe (Jaccard 0.7 on questions). Target **4,500 questions**.

## Phase 2: Retrieval packs

For each question: `retrieve(question, k=6, max_chars=14000)`, stored as chunk ids in
`work/v3/packs.jsonl`. Report the median pack size and the share of packs drawing on 2 or
more different files (target ≥ 60%: breadth is the point). For open-book training (decision
point 3, B or C) the packs are k=3, max_chars=6000 instead; report the same numbers.

**Carry-over check (runs here, before any generation):** compute the length distribution of
the v2 rows that Phase 5 carries over (config-ref capped at 800, GLM-passed incidents,
runbook items). They are short by construction; if they alone would put more than 15% of the
final dataset under 600 chars, cut the carry-over (config-ref first) and tell Roman the new
caps.

## Phase 3: Answer generation (≈ 1 day, DeepSeek, thinking off, concurrency 4)

`generate_qa.py --mode pack` (new): the excerpt block holds all pack chunks, each with its
`Source — title` header. Rules = v2's GEN_SYSTEM rules, applied to the whole pack, plus:

- "Answer like a senior SRE answering a colleague: complete, not short. For troubleshooting:
  the likely causes the material names, most likely first; for each, how to check it (exact
  command, metric or field) and the fix; then what to do if none of them fit. Include every
  relevant cause, check and caveat the material supports, and nothing it doesn't."
- "If the material does not answer the question, return SKIP." (Count skips per source.)
- "If part of the question is not covered by the material, say so in one sentence."
- "Ignore chunks that are not about the question. Do not combine settings or steps from
  different products or versions unless the material itself connects them." (BM25 packs
  contain off-topic chunks.)
- Never mention the material, excerpts or sources in the answer (build filter as v2).
- Pilot first: 40 questions. Gates are on structure, not length, so the generator is not
  pushed to pad:
  - skip rate ≤ 30%;
  - ≥ 80% of troubleshooting answers name at least 2 causes, each with a check and a fix;
  - of the 10 answers you read, checking every claim against the pack, at most 1 has an
    unsupported claim.
  Report mean and median answer length (overall and troubleshooting) next to v1's and v2's;
  a troubleshooting mean under 1,200 chars is a warning to raise with Roman, not a target
  to hit. Show Roman 3 items and the numbers, then wait.

## Phase 4: Verification

- DeepSeek verifier on the same pack, claim by claim: "list each factual claim; mark it
  supported or not by the material; grounded=false if any claim is unsupported, a setting is
  at the wrong scope, a premise is accepted wrongly, or a cause/step/flag is not in the
  material." Verifier sees exactly the generator's text, headers included (v2 bug). The
  verifier also returns the number of causes that have both a check and a fix (used for the
  structure gates).
- DeepSeek is both generator and verifier, so the two share blind spots, and the v2 verifier
  already passed mild padding. Therefore **GLM (`GLM-5.3-Flash-oQ4e`) re-verifies every
  troubleshooting item** with the same claim-by-claim prompt; an item is kept only if both
  verifiers pass it. Report the disagreement rate.
- **Run the GLM check alongside generation, not after it.** It is ~2,250 items; at the ~2
  minutes per call GLM needed in v2, 3 at a time, that is ~25 hours on its own. oQ4e
  (~169 GB) fits next to DeepSeek (~302 GB), so start it as soon as the pilot is accepted
  and let it follow the generator's output file (resumable, retry wrapper). Watch free
  memory and the generator's rate in the first hour; if generation slows by more than a
  third, tell Roman and offer running the check afterwards (+1 day).
- GLM audit (`GLM-5.3-Flash-oQ4e`) on the other question types: 400-item sample, stratified by
  source and question type. Gates: ≤ 10% flagged overall, ≤ 15% per stratum. If a stratum
  fails, GLM re-verifies that stratum fully (as v2).

## Phase 5: Dataset mix and the length gate (before any training)

- Inputs: v3 pack answers (main part), v2 config-ref items capped at 800, v2 GLM-passed
  incidents (1,205), v2 runbook items. No other v2 items.
- The carry-over caps are those settled by the check at the end of Phase 2.
- **Shape gate (new, blocking):** report the answer-length distribution per source and per
  question type next to v1's and v2's. Gate: ≤ 15% of rows under 600 chars, and ≥ 80% of
  troubleshooting rows with at least 2 causes that each have a check and a fix. Mean length
  is reported, not gated. If the gate fails, stop and show Roman.
- **Prompt format:** per decision point 3. For open-book (B or C), each pack-answer row
  carries its pack in the user message exactly as Phase 0 step 3 formats it; carry-over rows
  stay closed-book. Report the token-length distribution of the rows.
- Build with v2's filters (leaks, Strimzi apiVersion, near-dup, placeholders, protocol cap).
  Report rows per source, per file and per question type, and drop reasons.

## Phase 6: Spot check — blind, by Roman

`runs/v3/spotcheck_blank.md`: 30 items with the pack's chunk titles (not the text), the
question and the answer; key in a separate file. Roman marks OK / weak / wrong himself.
Gate: ≤ 3 wrong. The agent must not pre-fill verdicts. Record in REPORT how it was done.

## Phase 7: Train

As v2 (`runs/v2/lora.yaml` copy: same hyperparameters, one epoch, `save_every 50`,
`adapter_path adapters/kafka-sre-v3`), except:

- LoRA scale 20, as in v2 (decision point 1). Not fewer steps.
- Open-book (decision point 3): rows use the small packs (k=3, 6,000 chars). Set
  `max_seq_length` so no row is truncated (report the longest row), then run a 20-iteration
  probe and report it/s, peak memory and the projected total time. If the projection is over
  2 days, stop and ask Roman; the agreed fallback is a subset of ~1,500 pack rows.
- Mid-run check, **only if the Phase 5 numbers are borderline** (the shape gate passed by
  less than 5 points on either measure, or the troubleshooting mean is under 1,200 chars).
  v2's answer length barely moved with training (149 tokens at checkpoint 600, 185 at the
  end), so the check mostly re-tests the data, which the shape gate already covers. And it
  is not free: stopping and resuming loses the optimizer state and adds a warm-up and
  ~30–40 minutes. When it does run: stop at the iteration-300 checkpoint, generate answers
  to 10 questions from `eval/questions.jsonl` with that checkpoint (GPU exclusive, so
  training must be stopped), report mean tokens and read them for invented items. If the
  mean is under 400 tokens, escalate instead of resuming. Otherwise resume from that
  checkpoint as `runs/v2/lora_resume.yaml` did (schedule value at that step).

## Phase 8: Eval sets

Same as v2: `questions_real` (gate), `questions`, runbook holdout, feedback, a new synthetic
set from the v3 test split. Base answers reused where the question is unchanged.

## Phase 9: Evaluate and gate

Answers at max_tokens 2048 (GPU exclusive), then GLM-5.3-Flash-oQ8e-mtp (primary), then DeepSeek
(one judge loaded at a time). Contestants on `questions_real`: base, RAG base (Phase 0), v1,
v3 closed-book, and **v3 + retrieval** (required, not optional: for an open-book adapter it
is the configuration that ships). "v3" in the gate means the configuration proposed for
shipping. Thresholds below were approved by Roman on 2026-11-15. Gate (GLM oQ8e, two passes,
paired, all 40 scored):
1. `questions_real`: v3 ≥ base + 0.3 with more wins than losses, and v3 ≥ v1 + 0.2
   head-to-head (see below);
2. no `questions_real` category with n ≥ 5 below base − 0.5 (at n = 5 to 10 a smaller
   threshold trips on judge noise); report every category below base − 0.2;
3. runbook holdout: v3 ≥ base + 0.3;
4. `questions_real`: v3 ≥ RAG base + 0.2 head-to-head. If not, the gate fails and the
   recommendation is to ship RAG on the base model; say so plainly in the report.

**Head-to-head judgings (gates 1 and 4).** Each contestant's score against base comes from
its own judging, and the difference of two noisy deltas is noisier than either. So the v1
and RAG comparisons are judged directly, two passes each, all 40 scored:
- v3 vs v1 (v1 answers from `eval/answers_2048.jsonl`);
- v3 vs RAG base (same pack shape as v3 + retrieval if that is the shipping configuration).
`eval/compare.py` judges an adapter against base; reuse its judge with the other
contestant's cached answers in the base slot (small change if it cannot take two answer
caches). The four-way table against base stays in the report for context only.
Then 10 tuned answers read by you (5 kafka-ops), listing every invented flag, field, endpoint
or CRD next to the count for the base model's answers to the same questions, and a blind
review of 10 real questions for Roman (not used in v1's or v2's review). In the blind review
file, strip markdown headings and cap both answers' displayed length the same way, so answer
style does not give the model away (it did in v2's review). Naming (`v3` vs `v3-preview`) is
Roman's decision.

## Phase 10: Package (local only) and report

As v2 Phase 7. `runs/v3/REPORT.md`: sources and pins; question bank and pack stats; skip and
reject rates; audit and spot check; length distribution vs v1 and v2; training; the four-way
eval table; 3 good and 3 bad answers; recommendation (ship v3, ship RAG, or both).

## Time estimate

Phase 0 ½ day · Phase 1–2 ½ day · Phase 3–4 ~1½ days · Phase 5–6 ½ day + Roman's review ·
training ~1 day closed-book · eval ~1 day (two judge passes, the extra contestant, two
head-to-head judgings). Phase 3–4 assumes the GLM check runs alongside generation; if it has
to run afterwards, add a day. **About 5½ days of machine time for closed-book.** Open-book is
planned only with the small packs (k=3, 6,000 chars); its training time comes from the
Phase 7 probe, expect roughly 3 times closed-book. Roman at these points: decision point 4
up front, the Phase 0 result (stop and go, with decision points 2 and 3), the Phase 3 pilot,
the spot check, the blind review, and any failed gate (recall, carry-over, shape,
training-time probe, mid-run check).
