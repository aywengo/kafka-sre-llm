# Mission: kafka-sre v1 leftovers (fair baseline, judge calibration, Ollama)

v1 is published as an experimental preview (`aywengo/kafka-sre-27b-v1-preview`, `-MLX-4bit`,
`-GGUF`). Before v2 starts, finish the v1 work that v2 needs as a baseline. Read
`runs/v1/REPORT.md` and `MODEL_CARD.md` first; they describe the v1 eval and its problems.

## Goal

1. **Fair eval baseline.** In the v1 eval, 122 of 152 base answers were cut off at 1024
   tokens, so the comparison is flawed. Re-run it with `max_tokens` 2048 for both models.
2. **Judge calibration.** v1's judge (DeepSeek) also wrote the training data. Measure whether
   a second local model is a better judge for v2, using the blind human review as ground truth.
3. **Ollama test.** The model card says the Ollama Modelfile is untested. Test it.
4. **Prepared updates.** Updated model card and per-repo READMEs, ready to publish, but
   **not published** until Roman says go.
5. A short report: `runs/v1/LEFTOVERS.md`.

You are done when the report exists and Roman has the list of what is waiting for his go.

## Rules

1. **Do not use the oMLX DeepSeek endpoint (or any oMLX model) for your own reasoning.** Those
   models are only for `eval/compare.py`.
2. **Needs Roman's explicit go:** any Hugging Face upload or README change, `git push`,
   visibility changes, deleting files, installing software, unloading or loading models in oMLX
   when another client may be using them. Ask, then wait.
3. Never ask for, print or store the HF token. Roman runs `hf auth login` himself.
4. Commits: local commits are fine. Commit messages and code comments must not mention any AI
   tool, and must not have `Co-authored-by` trailers.
5. Keep your context small: `tail -n 50`, `grep`, `wc -l`, `jq`. Never read whole logs or
   whole results files.
6. Check in rarely: every 2 hours or when Roman messages you. If a long job is running and
   healthy, end the turn with one line ("checked, running: base 60/125").
7. After each phase, append one entry to `runs/v1/HEARTBEAT.md` in the existing format
   (Phase `L-A`, `L-B`, …), with the result and the next step.
8. If something here does not match what you find on disk (paths, flags, file formats), stop
   and tell Roman what differs. Do not guess.

## Setup (check, don't change)

- Base model and adapter: use **exactly** the ones the v1 eval used (see `runs/v1/REPORT.md`):
  the patched non-thinking base in `runs/v1/base27b` and the final v1 adapter.
- Judge 1: DeepSeek on oMLX, `http://192.168.3.69:8000/v1`, model
  `DeepSeek-V4.1-Flash-1017-oQ4e-mtp`, `--judge-extra-body '{"chat_template_kwargs": {"thinking_mode": "chat"}}'`.
- Judge 2: the model Roman calls "GTM-5.3-Flash" (probably GLM-5.3-Flash). Run
  `curl -s http://192.168.3.69:8000/v1/models | jq -r '.data[].id'` and use the exact id. If
  it is not listed, or more than one id could match, ask Roman. Find out how to turn its
  thinking off (check its chat template); if you cannot, run it with thinking on and record that.
- Question sets: `eval/questions.jsonl` (12), `eval/questions_real.jsonl` (40),
  `eval/questions_synthetic.jsonl` (100). Do not change them.
- The v1 answer cache is probably `eval/results_v2.answers-only.jsonl`. Confirm that each line
  has `label`, `question`, `text`, `tokens` and `truncated`, and that it has 152 `base` and
  152 `tuned` rows. **Do not modify it.**

## Phase L-A: fair baseline (max_tokens 2048)

1. Make a new cache `eval/answers_2048.jsonl` from the v1 cache, keeping **only rows with
   `truncated: false`**. Answers are greedy (mlx-lm's default sampler is temperature 0; confirm
   in `compare.py`/mlx-lm that no sampler is set), so an answer that finished under 1024 tokens
   is the same at 2048. Only the cut-off answers (about 122 base, 3 tuned) will be regenerated.
2. Check that nothing else uses the GPU heavily (`ps`, oMLX idle), then run in the background:
   ```
   nohup python eval/compare.py --model runs/v1/base27b --adapter <v1 adapter> \
       --questions eval/questions.jsonl eval/questions_real.jsonl eval/questions_synthetic.jsonl \
       --max-tokens 2048 --cache eval/answers_2048.jsonl \
       --out eval/results_2048_deepseek.jsonl \
       --judge-url http://192.168.3.69:8000/v1 --judge-model DeepSeek-V4.1-Flash-1017-oQ4e-mtp \
       --judge-extra-body '{"chat_template_kwargs": {"thinking_mode": "chat"}}' \
       > runs/v1/logs/eval_2048.log 2>&1 &
   ```
   Expect a few hours for generation (the 27B base in bf16, ~125 answers of up to 2048 tokens),
   then about an hour of judging. Progress: `grep -c '^\[base\]' runs/v1/logs/eval_2048.log`.
   If the judge step fails after generation, the cache is kept: rerun the same command.
3. Record per set and per category: base, tuned, delta, n; the truncation counts at 2048; mean
   tokens. Compare with the v1 numbers in `MODEL_CARD.md`. State plainly whether the publish
   gate (tuned ≥ base + 0.3 on `questions_real`) passes now. **Even if it passes, the name
   stays "preview"**; that decision is Roman's.

## Phase L-B: judge calibration

The blind review in `runs/v1/` (`review_key.json`, `review_roman.md`, `review_fable.md`)
used the **1024-token** answers, so calibrate on those.

1. Judge 2 on the v1 answers, judging only, no generation (the v1 cache is complete). Copy
   the v1 cache to `eval/answers_1024_copy.jsonl` and pass that as `--cache`, so the original
   cannot change. `--max-tokens 1024`, `--out eval/results_1024_judge2.jsonl`, all three sets,
   judge 2's URL, id and extra body. If oMLX cannot hold both judges at once, ask Roman
   before unloading anything.
2. For the 10 reviewed questions, take each verdict (tuned better / base better / tie) from
   Roman, from the second LLM reviewer, from DeepSeek (v1 results) and from judge 2 (a score
   difference of 0 is a tie). Use `review_key.json` to map A/B back to base/tuned. Table:
   per question, the four verdicts; then agreement counts of each judge with Roman and with the
   second reviewer.
3. Across all 152 questions: correlation and exact-agreement rate between the DeepSeek and
   judge 2 scores; mean base and tuned score for each judge; how often judge 2 replied with
   invalid JSON.
4. Then judge the 2048 answers with judge 2 too (`--cache` a copy of `eval/answers_2048.jsonl`,
   `--max-tokens 2048`, `--out eval/results_2048_judge2.jsonl`). This is the v2 baseline if
   judge 2 is chosen.
5. Recommend one judge for v2 with the numbers behind it. Ten questions is a small sample; say
   so. Note judge 2's speed (seconds per item) and licence.

## Phase L-C: Ollama

1. `which ollama && ollama --version`. If it is not installed, ask Roman before installing it.
2. Write `out/kafka-sre-27b-v1-preview/gguf/Modelfile` from the "Ollama" block in the GGUF
   README, plus `SYSTEM` with the recommended system prompt from `MODEL_CARD.md`. Then
   `ollama create kafka-sre-v1-test -f Modelfile` (from that folder, Q4_K_M).
3. Run 3 questions from `eval/questions_real.jsonl` (one Strimzi, one kafka-ops, one Argo CD)
   with `ollama run` and with the HTTP API. Check: it loads at all (this architecture is a
   hybrid with linear-attention layers; Ollama may not support it); output is coherent; no
   `<think>` or `<|im_end|>` text leaks; it stops by itself; tokens/s and memory.
4. If it fails, record the exact error and the Ollama version, and check whether a newer
   Ollama release notes support for the Qwen3.5/3.8 architecture (no installing without Roman).
   The card will then say "not supported by Ollama X.Y; use llama.cpp".
5. Afterwards `ollama rm kafka-sre-v1-test` (that only removes Ollama's copy, not the GGUF).

## Phase L-D: prepared updates (do not publish)

1. Check the built but unpublished `Q5_K_M` and `Q8_0` GGUFs in
   `out/kafka-sre-27b-v1-preview/gguf/`: perplexity on the same 2,665-token text used before
   (Q8_0 was 3.784; Q5_K_M should be close). Record the numbers.
2. Update `MODEL_CARD.md` and the per-repo READMEs in `out/kafka-sre-27b-v1-preview/*/`:
   - Evaluation: the 2048 results become the main tables. Keep one short paragraph with the
     1024 results and why they were replaced. Keep the blind review table and add judge 2's
     verdicts on the same 10.
   - Gate banner: the new numbers, honestly, whatever they are.
   - Ollama: tested result and the working Modelfile, or "not supported".
   - Files: Q5_K_M and Q8_0 listed as available only once they are actually uploaded; until then
     prepare both versions of that line.
3. Show Roman `git diff --stat` and the changed banner and tables. Commit locally.
4. Ask Roman for a go on: (a) README updates on the three HF repos, (b) uploading Q5_K_M and
   Q8_0 with `scripts/publish.sh`, (c) `git push`. When uploading, do **not** set
   `HF_XET_HIGH_PERFORMANCE` (it got rate-limited with 429 last time).

## Phase L-E: feedback and report

1. Read-only: `gh issue list -R aywengo/kafka-sre-llm` and the Discussions tabs of the three HF
   repos. Add each concrete wrong answer to `eval/questions_feedback.jsonl`
   (`question`, `category`, `reference`, `source`). Start with the known case: the Cruise
   Control rebalance answer used the right mode but invented other modes in its caveats.
   Write references only from the pinned docs, not from memory; mark anything you are unsure of.
2. Write `runs/v1/LEFTOVERS.md`: the four tables (2048 vs 1024 per set; truncation; judge
   agreement on the 10; judge 1 vs judge 2 on 152), the gate result, the judge recommendation,
   the Ollama result, Q5/Q8 perplexity, the feedback items, and the list of actions waiting for
   Roman's go. One page.
3. Final heartbeat entry: `state: DONE`, `needs_human: yes — <the pending go list>`.
