# Working rules for agents in this repository

This repository is run by one conductor session that plans and decides, and workers that do
the tasks. The mission brief is `agents/train-v3.md`, amended by `agents/train-v3-stage1.md`
(the amendment wins). The current state is in `runs/v3/STATE.md`; the run log is
`runs/v3/HEARTBEAT.md`; `scripts/s1_status.sh` prints a ten-line status.

## Who does what

The conductor does not do worker tasks itself. Every task goes to the role below, on the
model that role's file in `.claude/agents/` names. Pick the cheapest role that fits.

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
instead.

## Reports

Each worker writes its full report to a file (under `runs/v3/`, or the session's scratch
directory for throwaway detail) and returns at most about 150 words to the conductor: pass
or fail of each acceptance item, the files it changed, and anything that needs a decision.
The conductor reads the file only where it needs the detail. This keeps the conductor's
context from growing with every result.

## Decisions that are Roman's

The conductor stops only for these: the gates and go decisions in the brief, licences,
installs and downloads, deletes, anything irreversible, and going ahead with training.
Loading and unloading the three named models on the model server is covered by his standing
go, when no other client is active.

## Rules every agent follows

- No keys, IP addresses, host names or home paths in the repository, in logs or in commit
  messages. Server address and key live in `~/.config/kafka-sre-llm/` or the environment.
  Check every commit for them before it is pushed; the push follows only a clean check.
- Jobs that load a model locally run under `scripts/run_guarded.py`. Every step prints a
  line per item, or the stall guard takes it for a hang. One model set on the server at a
  time; local generation and training never overlap with judging or server-side generation.
- No eval question in any training-data prompt.
- Long waits: one background terminal waiter per milestone that exits on done, failed or
  stalled and prints a short summary. No scheduled checks, no periodic polling. Next steps
  go into the chain script so a finished step does not need a wake-up.
