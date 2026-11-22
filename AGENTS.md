# Working rules for agents in this repository

kafka-sre-llm builds a Kafka / Strimzi / Kubernetes / Argo CD SRE assistant on an
open-weight 27B model, on one Apple Silicon machine with mlx-lm. `GOAL.md` says where the
project is going. v1 is published as a preview. v3 found that retrieval is what helps (base
model + retrieval scores +1.48 over the plain base model on the 40 real questions with the
shipped index, +1.19 with the earlier one; the fine-tuned adapters alone add nothing), so
the product is a retrieval runtime. The adapter stage (an adapter trained with the retrieved
excerpts in the prompt) was stopped on 2026-11-17 after a ceiling check: the teacher did not
beat base + retrieval by the margin fixed in advance. v3 was released on 2026-11-18: the
retrieval runtime on the base model, the documentation index as a dataset, and the v2
adapter as a retrieval-only model. `v3.0.0` is the measured release; `v3.0.1` adds the
fixes from the end-to-end test (`agents/e2e-v3.0.0.md`). M4 (an evaluation with a hidden
half, the answer checker, the streaming service, a base-model bake-off) closed on
2026-11-21 with two criteria not met, and `v3.1.0` was released on 2026-11-22
(`milestones/M4.md`). `v3.1.1` (2026-11-22, branch `release/v3.1.1`) fixes the checker's
setup from a fresh clone; its commits are also on `main`. The current milestone is M5:
retrieval ranking, fewer wrong statements, repairs.

## Start of every session

1. Read `MILESTONE.md` (the current milestone: work packages, status, what waits for the
   Owner). `runs/v3/STATE.md` is the closing state of the v3 release.
2. Run `scripts/s1_status.sh` (ten lines: running jobs, markers, last log line, loaded model).
3. If a job is running, do not edit a file it imports or executes, and do not start
   anything on the GPU or the model server next to it, unless the task says so.

The mission brief is `agents/train-v3.md`, amended by `agents/train-v3-stage1.md` (the
amendment wins). The brief of M5 is `agents/m5-plan.md`; the evaluation's rules are
`agents/m4-evaluation.md`. The run log is `runs/v3/HEARTBEAT.md`.

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

Each worker writes its full report to a file (`runs/m5/reports/<task>.md` in M5, or the session's
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
For M5 the Owner's go of 2026-11-22 also covers loading the 4-bit build of the base model
for measurement runs, and unloading any model from the server, also one that another
client left loaded, when the server has no active or waiting request. The rest of M4's
wider go (the full-precision build, the bake-off candidates) ended with M4.

## Repository map

| Path | What it is |
|---|---|
| `GOAL.md` | Where the project is going and how success is judged; changes only by the Owner's decision |
| `MILESTONE.md` | Progress of the current milestone; kept by the conductor, not edited by workers |
| `milestones/` | Tracking documents of closed milestones; code comments that cite "MILESTONE.md" for decisions J, K, L, M and N mean `milestones/M4.md` |
| `agents/` | Mission briefs, one per version or work package |
| `kafka_sre_rag/` | Retrieval runtime: `prompts.py` (the only place prompt text lives), `frozen.json`, `ask` / `chat` / `verify` |
| `scripts/` | Pipeline: sources, chunking, retrieval, question bank, packs, generation and verification, guard, chain scripts (`s1_*.sh`) |
| `model_cards/` | Cards and notices of the published Hugging Face repositories; `scripts/package_adapter.py` and `scripts/package_index.py` build the upload folders |
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
- **Evaluation sets:** no eval question in any training-data prompt. `questions_real` (the
  old 40) and the 42 runbook-holdout questions are legacy sets: kept for continuity with v3,
  no longer a gate, no prompt or retrieval tuning on them, and say so whenever a conclusion
  leans on them. Differences under about 0.2 on 40 questions are noise (the standard error
  is about 0.2). The open judged batches B1 and B2, the pilot sets and the open mechanical
  half are development data in M5. B4 is the out-of-sample check for a retrieval candidate:
  its rule is fixed beforehand and it is used once.
- **The hidden part of the M4 evaluation** (`agents/m4-evaluation.md`) is the gate. Nobody
  opens its items, the answers to them or per-item results; the harness prints totals only.
  Every run on it is written in `runs/m4/HIDDEN_LEDGER.md` with its decision rule before
  the run. Six looks in total: two were used in M4, and M5 uses at most two of the rest. A change prompted by particular open items lists those items
  and is judged on the hidden part.
- **The catalogues** (`work/m4/catalogues/`, built by `scripts/build_catalogues.py`) are
  hashed like the index. The evaluation's scorer and the runtime's answer checker share
  them and nothing else.

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
  another client is active, wait for the Owner. Unload only what we loaded, unless the
  Owner's go for the milestone says otherwise (it does for M5, see above). Note every swap in
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
