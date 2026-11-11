# Mission: train kafka-sre v2

v1 is published as an experimental preview and failed its eval gate (see `MODEL_CARD.md`,
`runs/v1/REPORT.md`, `runs/v1/LEFTOVERS.md`). It is better than base on Strimzi, Kafka
config and Argo CD, and clearly worse on Kafka operations and streaming: answers are short
(~250 tokens), miss the usual cause in troubleshooting, and add caveats, modes, endpoints
and deprecations that the source never said. v2 changes the **data**, not the method.
Read those three files first.

## Goal

A LoRA adapter `adapters/kafka-sre-v2` on the same base (`runs/v1/base27b`) that passes the
v2 gate (Phase 6). Done means:
1. `data/` rebuilt from the v2 sources with the v2 generation rules, audited (Phase 2 gates).
2. Adapter trained, best checkpoint chosen by validation loss.
3. Eval run with both judges; gate result recorded honestly, pass or fail.
4. Fused model, MLX 4-bit and GGUF built locally under `out/`. **Nothing uploaded.**
5. `runs/v2/REPORT.md` written.

## Rules (same spirit as v1)

1. **You run on a cloud model (Claude Sonnet). Never use any oMLX model for your own
   reasoning.** oMLX models are only for `generate_qa.py`, the audit and `compare.py`.
2. **Needs Roman's explicit go:** HF uploads, `git push`, repo visibility, deleting `raw/`,
   `work/`, `data/`, `adapters/` or `out/`, installing software, changing oMLX settings of any
   model other than the ones named here, and loading or unloading oMLX models while other
   clients may use them (ask first; Roman pauses them).
3. **Secrets:** the oMLX key is read from `~/.config/kafka-sre-llm/judge_key` into an env
   variable. Never put a key or token on a command line, in a log, a heartbeat or a commit.
4. **Commits:** small, plain messages, code and config only. No mention of any AI tool, no
   `Co-authored-by` trailers. Never commit generated data, weights, `raw/`, `work/`, `private/`.
5. **GPU exclusivity:** while training or generating eval answers with mlx-lm, nothing else may
   run on the GPU: oMLX must have no model loaded and no other clients (ask Roman to pause
   them), no Ollama, no llama.cpp with Metal. A GPU hang from another process killed a v1 run.
6. **Everything long runs under nohup with a retry wrapper and is resumable** (generation and
   answer generation resume from their output files; judging from `--judge-cache`).
7. **Context:** `tail`, `grep`, `wc -l`, `jq`, `status.json`. Never read whole logs or datasets.
8. **Heartbeat:** create it once with the cron form (the `2h` form fails):
   `await rlm_heartbeat.create(instruction="kafka-sre v2 check-in per agents/train-v2.md: read runs/v2/status.json and the current log tail; act only on DONE, STOPPED, needs_human or a due gate; otherwise end with one line.", interval="0 */2 * * *", label="kafka-sre-v2", delivery_mode="follow_up")`.
   Also run the script heartbeat during generation and training:
   `nohup python scripts/heartbeat.py --run-dir runs/v2 --gen-log runs/v2/logs/generate.log --train-log runs/v2/logs/train.log --train-config runs/v2/lora.yaml --raw-qa work/v2/raw_qa.jsonl --chunks work/v2/chunks.jsonl --cap kubernetes=1500 --every 1800 > runs/v2/logs/heartbeat.log 2>&1 &`
   Append your own entries (gates, escalations, phase changes) to `runs/v2/HEARTBEAT.md` in
   the v1 format. End every turn that starts a long job with the time of your next check.
   Delete the heartbeat when the mission is done.
9. If something here does not match the repo (paths, flags, file formats), stop and say what
   differs. Do not guess.

## Phase 0: Preflight

- `git pull --ff-only` on `main`. Commit the uncommitted `eval/compare.py` changes from the
  leftovers run (judge cache, env key, tolerant parsing) as their own commit.
- oMLX: `DeepSeek-V4.1-Flash-1017-oQ4e-mtp` (generator, verifier, second judge, thinking off
  with `{"chat_template_kwargs": {"thinking_mode": "chat"}}`) and `GLM-5.3-Flash-oQ4e`
  (auditor, primary judge; thinking cannot be turned off, so always send `max_tokens` 32768
  and read the JSON from `content`). Both fit together (about 302 + 169 GB). Ask Roman before
  loading DeepSeek and to pause other oMLX clients for Phase 1-2.
- Free disk ≥ 300 GB; record mlx and mlx-lm versions (must match v1: mlx 0.32.4, mlx-lm 0.32.2;
  if they changed, stop and ask).
- Create `runs/v2/` and `runs/v2/logs/`; write v2 work files under `work/v2/` so v1's
  `work/` stays intact.

## Phase 1: Sources and chunks

1. **Held-out runbooks.** From the 32 runbooks in `data/runbooks/runbooks/`, hold out 8 for
   eval: 2 of the 7 under `kafka/` and 6 of the 25 under `strimzi/`, picked by the lowest
   `sha1(filename)` within each folder. Write them to `data/runbooks/holdout.txt`, commit it,
   and never generate training data from them.
2. **sources.yaml.** Add a source:
   ```yaml
   - name: runbooks
     local: ./data/runbooks/runbooks
     globs: ["**/*.md"]
     exclude: [<one "/kafka/<file>.md" or "/strimzi/<file>.md" pattern per held-out runbook>]
     max_chars: 12000
     license: Apache-2.0
   ```
   Leave the `own-runbooks` (private) entry as it is. Kafka is now pinned to 4.3.2 with the
   `content/en/43/generated` config reference (PR #1). Keep the v1 Kubernetes exclude list.
3. **chunk_docs.py:** support an optional per-source `max_chars` (default stays 4000), so a
   runbook stays one chunk with its whole procedure. Small commit.
4. `python scripts/fetch_sources.py` (re-fetches Kafka at 4.3.2 and the 43 config docs; record
   every `raw/*/.source-commit`), then
   `python scripts/chunk_docs.py --out work/v2/chunks.jsonl`. Record chunks per source. Check:
   24 runbook files (32 minus 8) and no runbook chunk over 12,000 characters.

## Phase 2: Generation rules, pilot, full run

**2a. Prompt changes in `scripts/generate_qa.py`** (one commit; keep the v1 rules):
- GEN_SYSTEM, add:
  - "Caveats, side effects, alternatives (other modes, flags, endpoints), examples, defaults
    and deprecations are facts too. Include one only if the excerpt states it. If the excerpt
    gives no caveat, write none."
  - "Troubleshooting answers are a checklist: the likely causes the excerpt names, most
    likely first; for each, how to check it (command or metric from the excerpt) and the fix.
    Never add a cause the excerpt does not name. If it names only one, give that one well."
  - "If the excerpt says something is not covered by official documentation, is a compiler's
    note, or cannot be completed from the docs, keep that warning in the answer in plain words."
  - "This dataset targets Kafka 4.3 and Strimzi 1.3.1. Do not mention other versions unless
    the excerpt does."
- STRICT_RULES (verifier), add to the reject list: "adds a caveat, alternative, example,
  endpoint, default or deprecation that is not in the excerpt; lists a troubleshooting cause
  that is not in the excerpt; drops a not-covered-by-the-docs warning that the excerpt has".
  Also: "First restate the premise of the question in a few words and check it against the
  excerpt (key vs value, component, scope, version)."

**2b. Pilot** (DeepSeek, thinking off, `--verify batch`, concurrency 3):
`--limit 40` over all sources, plus `--sources runbooks --limit 10 --max-per-chunk 8`.
Gate: reject rate 5-40%; read 10 items yourself (at least 3 troubleshooting, 2 runbook),
check every caveat against the excerpt; paste 3 into the heartbeat. Compare with 10 v1 items
from the same sources: answers should be longer for troubleshooting and have fewer
unsupported caveats. Ask Roman to look at the pilot before the full run.

**2c. Full run** (same settings), output `work/v2/raw_qa.jsonl`:
- pass 1: `--sources apache-kafka apache-kafka-config strimzi argo-cd`;
- pass 2: `--sources kubernetes --limit 1500`;
- pass 3: `--sources runbooks --max-per-chunk 8`, then pass 4: the same with `--seed 1
  --temperature 0.9 --out work/v2/raw_qa_runbooks2.jsonl` (a second set of runbook
  questions; the build step removes near-duplicates).
Expect about one day. Retry wrapper, nohup, `runs/v2/logs/generate.log`.

**2d. Independent audit (GLM).** Write `scripts/audit_items.py`: sample 400 kept items
(stratified by source, all runbook items included in the count proportionally), send each
chunk's sampled items to GLM with the same STRICT_RULES in one call (concurrency 3,
`max_tokens` 32768, per-item cache so it resumes). Report the flag rate per source and 10
flagged examples with GLM's reason. Gate: GLM flags ≤ 10% of kept items overall and ≤ 15% in
every source. If a source fails, show Roman the examples and propose a fix (prompt change and
regenerate that source, or `--reverify`); do not decide alone.

**2e. Roman's spot check:** 30 random items (8 Kafka, 6 config, 8 Strimzi, 4 runbooks,
2 Argo CD, 2 Kubernetes) in the v1 spot-check format (`runs/v1/spotcheck_v2_blank.md`) as
`runs/v2/spotcheck_blank.md`. Gate: at most 3 wrong (v1: 5). Wait for Roman.

## Phase 3: Build

`python scripts/build_dataset.py --raw work/v2/raw_qa.jsonl work/v2/raw_qa_runbooks2.jsonl
--out data` with the v1 filters (find the exact `--skip-sources`, `--drop-pattern` and
`--cap-path` arguments in `runs/v1/HEARTBEAT.md`; keep the Strimzi `v1` API filter). Move the
v1 `data/` to `data_v1/` first (rename, don't delete). Record items per source, drop reasons
and the iters hint. Check that no held-out runbook path appears in `data/*.jsonl`.

## Phase 4: Train

- Copy `runs/v1/lora.yaml` to `runs/v2/lora.yaml`: same base, same hyperparameters (rank 16,
  scale 20, lr 1e-5, batch 4, one epoch, cosine with 100 warmup), `iters` and
  `lr_schedule.arguments[1]` from the build hint, `save_every: 50`,
  `adapter_path: adapters/kafka-sre-v2`. The data is the experiment; do not change
  hyperparameters without Roman.
- Unload all oMLX models and confirm GPU exclusivity (rule 5). Then
  `nohup mlx_lm.lora -c runs/v2/lora.yaml > runs/v2/logs/train.log 2>&1 &`.
- About a day. If the GPU locks up, resume from the last checkpoint in the exact batch order,
  as v1 did (`runs/v1/lora_resume.yaml` and the v1 HEARTBEAT entries around iter 771).
- Val loss rising on 3 consecutive evals: stop, keep the best checkpoint, escalate.

## Phase 5: Eval sets and compare.py

- `eval/questions_runbooks_holdout.jsonl`: the eval items of the 8 held-out runbooks from
  `data/runbooks/eval/`, converted to `question`, `reference` (from `reference_answer`, plus
  `source_url`), `category` (`runbook-kafka` or `runbook-strimzi`). Write a small converter.
- `eval/questions_feedback.jsonl`: as it is (check the citations against the Kafka 4.3 docs).
- `eval/questions_synthetic_v2.jsonl`: 100 items from the v2 test split, built like v1's.
- `compare.py`: add `--judge-concurrency N` (default 1) for parallel judge calls; keep the
  per-item judge cache.
- **Reuse base answers.** The base model, prompt, system prompt and greedy decoding are
  unchanged, so copy the `base` rows of `eval/answers_2048.jsonl` into the v2 answer cache.
  Base answers are only generated for questions that are new.

## Phase 6: Evaluate and gate

- Answers: `max_tokens` 2048, GPU exclusive (rule 5), then load the judges.
- Judge 1 (primary): GLM-5.3-Flash-oQ4e, `--judge-concurrency 3`, `max_tokens` 32768.
  Judge 2: DeepSeek, thinking off. Both on all sets.
- **Gate (GLM scores):**
  1. `questions_real`: tuned ≥ base + 0.3;
  2. no category in `questions_real` with n ≥ 5 below base − 0.2;
  3. `questions_runbooks_holdout`: tuned ≥ base + 0.3.
  Report DeepSeek's numbers next to them, plus truncation and mean tokens per set. kafka-ops
  is the category to watch (v1: −1.6 with DeepSeek).
- Read 10 tuned answers yourself (5 kafka-ops) and list every invented key, flag, field,
  mode or caveat.
- Build a blind review of 10 real questions for Roman, as in v1 (`runs/v1/review_blind.md`,
  `review_key.json`), from questions not used in the v1 review if possible. The model name
  (`v2` vs `v2-preview`) is Roman's decision after his review, even if the gate passes.

## Phase 7: Package (local only)

As v1: fuse, `scripts/mlx_to_hf_layout.py`, bf16 HF layout, MLX 4-bit, GGUF Q4_K_M, Q5_K_M,
Q8_0; perplexity on the v1 text; greedy-identity check MLX vs HF layout. Draft the v2 model
card from `MODEL_CARD.md` (new sources table with the 4.3.2 pin and the runbooks, data
counts, both judges, gate result, known failure modes). Write the exact upload commands for
Roman, in the style of `runs/v1/prepared/`. Do not run them.

## Escalate (stop, needs_human: yes, wait)

A gate fails twice; verify reject rate over 50% or malformed JSON over 20% of chunks;
NaN or exploding loss, OOM, swap over 20 GB, free disk under 100 GB; any GPU crash after a
restart; anything in rule 2.

## Final report (`runs/v2/REPORT.md`)

Source commits; chunks and items per source with drop reasons; audit and spot-check results;
training config, best checkpoint, losses, wall-clock per phase; the eval tables for both
judges with the gate result; 3 good and 3 bad tuned answers; v1 vs v2 on the same questions;
known weaknesses; recommendations for v3. One page plus tables.
