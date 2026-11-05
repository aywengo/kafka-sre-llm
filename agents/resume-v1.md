# Resume: kafka-sre v1 operator (low-footprint mode)

You are taking over the v1 run described in `agents/train-v1.md`. That file still
defines the goal, phases, gates, escalation rules and the final report. This file
changes HOW you operate so that you do not compete with the run for the GPU.

## Why

The previous operator ran on the same DeepSeek model in oMLX that generates the
training data. Each of its turns sent a ~180k-token prompt, which stalled generation,
and Phase 4 unloads that model, which would have left the operator without a model.

## Rules for this session

1. **You must not use the oMLX DeepSeek endpoint for your own reasoning.** Run on a
   separate model (cloud, or a small local model on its own server). DeepSeek is
   reserved for `generate_qa.py` and the Phase 5 judge.
2. **Heartbeats come from the script, not from you.** Start it once and leave it:
   ```
   nohup python scripts/heartbeat.py --every 1800 --cap kubernetes=1500 \
       > runs/v1/logs/heartbeat.log 2>&1 &
   ```
   It appends to `runs/v1/HEARTBEAT.md` and overwrites `runs/v1/status.json`, and it
   follows both generation and training. Check that it is running whenever you wake up.
3. **Wake up rarely.** Read `runs/v1/status.json` at most every 2 hours, or when the
   human messages you. Act only if `state` is DONE or STOPPED, `needs_human` is true,
   or a gate is due. Otherwise end your turn with one line ("checked, running").
4. **Keep your context small.** Do not read whole logs. Use `tail -n 50`, `grep` and
   `status.json`. Write your own notes for gates and escalations to `runs/v1/HEARTBEAT.md`
   in the existing format, below the script's entries.

## Where we are

Read the last 5 entries of `runs/v1/HEARTBEAT.md` and `git log --oneline -10` to
confirm. As of the last handover:
- Sources: apache-kafka 750, apache-kafka-config 861, strimzi 874, argo-cd 1,247,
  kubernetes 2,705 chunks. Flux is out of scope (`build_dataset.py --skip-sources flux`).
- Thinking-on baseline pilot (20 chunks): 0.88 chunks/min, 3,218 output tokens per
  chunk, 77% of them reasoning. The pilot with verify thinking off
  (`thinking_mode: "chat"`) was running.

## Next steps

1. Finish the verify pilot and compare reasoning tokens per chunk with the baseline.
2. Pilot generation with thinking off too (`--extra-body` with the same switch) on 20
   new chunks. Read 5 samples side by side with thinking-on samples. Keep thinking on
   for generation only if those samples are clearly better.
3. Concurrency sweep (2, 4, 8; 10 minutes each) with no other oMLX clients. Keep the
   fastest by chunks/min.
4. Full run, first pass:
   `--sources apache-kafka apache-kafka-config strimzi argo-cd`, then a second pass
   `--sources kubernetes --limit 1500`. Both in the background with nohup, appending
   to `runs/v1/logs/generate.log`.
5. Post one heartbeat entry yourself with the chosen settings and the ETA, then follow
   rule 3 until generation is DONE, and continue with Phase 1's dataset build and
   Phase 2 in `agents/train-v1.md`.
