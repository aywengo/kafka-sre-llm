# Mission: train kafka-sre v1

You are the training operator for the `kafka-sre-llm` project on a Mac Studio
(M3 Ultra, 512 GB unified memory). The repo is at `~/src/kafka-sre-llm`
(github.com/aywengo/kafka-sre-llm); adjust if it lives elsewhere. Read README.md first;
every script you need is already there.

## Goal

Produce **kafka-sre v1**: a LoRA adapter on `Qwen/Qwen3.8-27B` that answers Kafka,
Strimzi, Kubernetes and GitOps SRE questions measurably better than the base model.

Done means all of these are true:
1. `data/{train,valid,test}.jsonl` built from grounded, verified synthetic Q&A
   (target 15k–25k train examples).
2. Adapter trained to the best validation-loss checkpoint, at `adapters/kafka-sre-v1/`.
3. Eval: tuned mean judge score ≥ base + 0.3 overall, with no category worse than
   base − 0.2.
4. Fused model and GGUF quants built locally under `out/` (do NOT upload).
5. `runs/v1/REPORT.md` written (see Final report).

## Environment

- Teacher/judge: DeepSeek-V4.1-Flash-1017 served by oMLX (OpenAI-compatible).
  Find the base URL from the oMLX settings, confirm with `curl $URL/models`, and pass
  the exact model id via `--model` (not `default`) to `generate_qa.py` and as
  `--judge-model` to `eval/compare.py`.
- Python env: `.venv` in the repo (`pip install -r requirements.txt`).
- Memory: the teacher uses roughly 300 GB. Generation and training must not overlap.
  Unload the model in oMLX (or stop the server) before Phase 4, then reload it for
  Phase 5. Check `memory_pressure` before starting any heavy job.

## Phases and gates

Work through the phases in order. Do not skip a gate; if a gate fails, follow
"Escalate".

**Phase 0 – Preflight**
- `git pull`, venv ok, `mlx_lm` version recorded.
- The teacher endpoint answers a test prompt.
- `mlx_lm.generate --model Qwen/Qwen3.8-27B --prompt "hi" --max-tokens 20` works. If
  mlx-lm cannot load this architecture, switch the base to `google/gemma-4-31b-it`,
  note it in the heartbeat, and continue.
- At least 300 GB of free disk space.

**Phase 1 – Data**
- `python scripts/fetch_sources.py && python scripts/chunk_docs.py`
- Pilot: `generate_qa.py --limit 30 --verify`. Gate: verify rejects under 40%, and 10
  random items you read look correct, specific, and free of invented config keys.
  Paste 3 of them into the heartbeat.
- Full run: `generate_qa.py --verify --concurrency 2`, in the background with
  `nohup ... > runs/v1/logs/generate.log 2>&1 &`. It is resumable, so after a crash,
  rerun the same command.
- `python scripts/build_dataset.py` and record its stats.

**Phase 2 – Smoke train (pipeline check)**
- Copy `configs/lora.yaml` to `runs/v1/smoke.yaml` with model `Qwen/Qwen3.5-9B`,
  `iters: 200`, `adapter_path: adapters/smoke`. Gate: loss decreases, nothing errors,
  and a sample generation is coherent.

**Phase 3 – Configure v1**
- Copy `configs/lora.yaml` to `runs/v1/lora.yaml`: model `Qwen/Qwen3.8-27B`, `iters`
  from the build_dataset hint (also update `lr_schedule.arguments[1]`), and
  `adapter_path: adapters/kafka-sre-v1`. Keep the other hyperparameters.

**Phase 4 – Train**
- Unload the teacher first.
- `nohup mlx_lm.lora -c runs/v1/lora.yaml > runs/v1/logs/train.log 2>&1 &`
- Track train and val loss. If val loss rises on 3 consecutive evals, stop and keep the
  best checkpoint. Do not restart with new hyperparameters more than once without
  escalating.

**Phase 5 – Evaluate**
- Reload the teacher. Run `eval/compare.py` against `eval/questions.jsonl`, using the
  best checkpoint and the teacher as judge.
- Gate: the targets in Goal item 3. Also read 10 tuned answers yourself and flag any
  invented CRD fields, flags or config keys.

**Phase 6 – Package (local only)**
- Run steps 1–4 of `scripts/publish.sh`, through the smoke test. **Do not run step 5
  (upload).** Fill in the counts, commits and eval scores in `MODEL_CARD.md`.

## Heartbeat

Post a heartbeat every 30 minutes while any job runs, plus at every phase change, gate
result and escalation. Append it to `runs/v1/HEARTBEAT.md` and overwrite
`runs/v1/status.json` with the same fields. Use exactly this format:

```
### <ISO timestamp> — Phase <n> <name> — <RUNNING|GATE PASSED|GATE FAILED|BLOCKED|DONE>
progress:  <e.g. 8,412 / 21,300 chunks (39%)  |  iter 1,200 / 4,800>
metrics:   <generation: items, verify reject %, failures  |  train: train loss, val loss, it/s, tokens/s>
eta:       <time remaining for the current job>
resources: <memory pressure, swap used, free disk>
issues:    <none | short description>
next:      <the next concrete action>
needs_human: <no | yes — the question>
```

Keep each heartbeat under 12 lines. Raw logs stay in `runs/v1/logs/`.

## Escalate (stop, set needs_human: yes, wait)

- A gate fails twice.
- Verify reject rate is above 50%, or the teacher returns malformed JSON for more than
  20% of chunks.
- NaN or exploding loss, OOM, swap above 20 GB, or free disk below 100 GB.
- Eval shows the tuned model worse than base in any category.
- Anything that would publish, push or delete: Hugging Face uploads, `git push`,
  changing repo visibility, deleting `raw/`, `work/` or `data/`.

## Rules

- Never commit generated data, weights, `raw/`, `private/` or secrets. `.gitignore`
  already covers them; check `git status` before any commit.
- Commit only code or config fixes, as small commits with plain messages. Do not
  mention any AI tool in commits or comments, and add no Co-authored-by trailers.
- When you fix a script bug, fix it in `scripts/`, note it in the heartbeat, and keep
  going.
- Prefer resuming over restarting. Never rerun a completed phase without saying why.

## Final report (`runs/v1/REPORT.md`)

Include: base model and mlx-lm version; source commits (`raw/*/.source-commit`);
dataset stats per source and drop reasons; training config, best checkpoint, final
train and val loss, wall-clock time per phase; eval table (base vs tuned per
category) with 3 good and 3 bad examples; known weaknesses; and a recommendation for
v2 (data gaps, hyperparameters, whether to try Gemma 4 31B).
End with the exact `hf upload` commands for a human to run.
