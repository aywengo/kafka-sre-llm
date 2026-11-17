# Working rules for agents in this repository

kafka-sre-llm builds a Kafka / Strimzi / Kubernetes / Argo CD SRE assistant on an
open-weight 27B model, on one Apple Silicon machine with mlx-lm. v1 is published as a
preview. v3 found that retrieval is what helps (base model + retrieval scores +1.19 over the
plain base model on the 40 real questions; the fine-tuned adapters alone add nothing), so
the product is a retrieval runtime. The adapter stage (an adapter trained with the retrieved
excerpts in the prompt) was stopped on 2026-11-17 after a ceiling check: the teacher did not
beat base + retrieval by the margin fixed in advance. v3 ships as the retrieval runtime on
the base model.

## Start of every session

1. Read `runs/v3/STATE.md` (where things stand, what waits for the Owner).
2. Run `scripts/s1_status.sh` (ten lines: running jobs, markers, last log line, loaded model).
3. If a job is running, do not edit a file it imports or executes, and do not start
   anything on the GPU or the model server next to it, unless the task says so.

The mission brief is `agents/train-v3.md`, amended by `agents/train-v3-stage1.md` (the
amendment wins). The run log is `runs/v3/HEARTBEAT.md`.

## Who does what

One conductor session plans and decides; workers do the tasks. The conductor does not do
worker tasks itself: it runs the most expensive model with the largest context. Every task
goes to the role below, on the model that role's file in `.claude/agents/` names. Pick the
cheapest role that fits.

| Work | Role (model) |
|---|---|
| Code changes, chain scripts, tests | `implementer` (sonnet) |
| Starting, stopping and checking jobs | `job-operator` (sonnet) |
| Reading logs and result files | `quick-reader` (haiku) |
| Reading evidence, reviewing plans before expensive runs | `complex-reviewer` (opus) |
| Outside sources and licences | `researcher` (sonnet) |
| Plan, task briefs, decisions, reading the short result | the conductor |

`.claude/local-worker.sh <agent> <ro|rw> <prompt-file>` runs a worker on a local model when
the model server is free; it exits 3 when the server is busy, and a Claude worker is used
instead. Use it for bounded reading and scoped code changes, not for design or for judging
the output of the judge and teacher models.

A task brief names: the files the worker may touch and the ones it must not (anything a
running job uses), the behaviour wanted, an acceptance check it can run, what it needs (CPU,
GPU, model server), and where the report goes.

## Reports

Each worker writes its full report to a file (`runs/v3/reports/<task>.md`, or the session's
scratch directory for throwaway detail) and returns at most about 150 words: pass or fail of
each acceptance item, the files it changed, anything that needs a decision. It says what it
could not verify. A failed check is reported as failed, with the output.

If the harness refuses the write of a report or of a document the task asks for, the worker
does not work around it with a shell command: it says so in its summary and returns the full
text in its reply, and the conductor saves the file.

## Decisions that are the Owner's

The conductor stops only for these: the gates and go decisions in the brief, changes to the
brief, licences, installs and downloads, deletes, anything irreversible, and going ahead
with bulk generation or training. A failed gate stops the stage until the Owner has seen
it. The Owner is whoever `CODEOWNERS` names.
Loading and unloading the three models named below is covered by the Owner's standing go, when no
other client is active.

## Repository map

| Path | What it is |
|---|---|
| `agents/` | Mission briefs, one per version |
| `kafka_sre_rag/` | Retrieval runtime: `prompts.py` (the only place prompt text lives), `frozen.json`, `ask` / `chat` / `verify` |
| `scripts/` | Pipeline: sources, chunking, retrieval, question bank, packs, generation and verification, guard, chain scripts (`s1_*.sh`) |
| `eval/` | Question sets and `compare.py` (answers and judging) |
| `tests/` | Offline unit tests, golden hashes of the measured prompts |
| `sources.yaml` | Documentation sources with pins and licences |
| `data/runbooks/` | Runbooks written from the docs; 8 are held out for evaluation |
| `raw/`, `work/`, `runs/`, `adapters/`, `out/` | Fetched, generated and run files: git-ignored, never committed |

Commands: `.venv/bin/python -m unittest discover -s tests` (offline, seconds);
`.venv/bin/python -m kafka_sre_rag verify`; `.venv/bin/python -m kafka_sre_rag ask --dry-run "..."`.

## Things that are frozen

- **The index:** `runs/v3/INDEX_FROZEN.json` records its file, hash and retrieval settings.
  Do not rebuild it or change `scripts/retrieve.py` settings without a go.
- **The measured prompt format:** `tests/golden/` holds hashes of the 40 prompts that scored
  +1.19. A change must keep those tests passing. An adapter is trained on that format.
- **Generation prompts** carry `PACK_PROMPT_VERSION`; outputs of different versions are never
  mixed, and a version change means a new pilot.
- **Evaluation sets:** no eval question in any training-data prompt. `questions_real` is the
  gate set and has been used many times: no prompt or retrieval tuning on it, and say so
  whenever a conclusion leans on it again. Differences under about 0.2 on 40 questions are
  noise (the standard error is about 0.2).

## Jobs, the GPU and the model server

- Anything that loads a model locally runs under `scripts/run_guarded.py`; so do long
  server jobs. Stop a guarded job with SIGTERM to the guard's PID, never the job itself,
  never `kill -9`.
- Every step prints a line per item. The guard kills a job whose log does not grow, and a
  silent step looks like a hang.
- Jobs are resumable by their output files and end with a done marker; a chain script
  carries its own sanity checks and next steps, so it needs no watching.
- One model set on the server at a time: `DeepSeek-V4.1-Flash` (teacher, first verifier), or
  `GLM-5.3-Flash-oQ4e`, or `GLM-5.3-Flash-oQ8e-mtp` (judge). They do not fit in memory
  together. Local generation and training never overlap with server jobs.
- Before a load or unload, check that the server has no active or waiting request; if
  another client is active, wait for the Owner. Unload only what we loaded. Note every swap in
  `HEARTBEAT.md`.
- Keep `runs/v3/status.json` and `STATE.md` true at every milestone and whenever a job
  stops or waits for the Owner.

## Waiting

One background terminal waiter per milestone, which exits on done, failed or stalled and
ends with `scripts/s1_status.sh`. No scheduled checks (they are lost when the session
restarts) and no periodic polling. A notification that needs no action gets one line or
none. Each wake-up of a long session re-reads its whole context, so fewer wake-ups is what
saves tokens; for a wait of many hours, start the next session from `STATE.md`.

## Commits

- No keys, IP addresses, host names or home paths in the repository, in logs or in commit
  messages. The server address and key live in `~/.config/kafka-sre-llm/` or the
  environment (`OMLX_URL`, `OMLX_API_KEY`, `JUDGE_API_KEY`); scripts read them from there.
- Check every commit to be pushed, not only the net diff, then push to `origin/main`. The
  push follows only a clean check. No force-push and no history rewrite without the Owner.
- Workers do not commit unless the task says so. Drafts that wait for the Owner's review are
  not pushed.

## Reporting to the Owner

Lead with the result and what the Owner has to decide. Give every number with its size (how many
questions, how many passes) and say what was measured and what is inferred. Give a time
estimate as a range, and correct it when the rate is known. Say plainly when something
failed or was not checked.
