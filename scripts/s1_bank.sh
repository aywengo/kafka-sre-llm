#!/bin/sh
# Stage 1, model server (DeepSeek, thinking off): the question bank, then search terms for every bank question.
# Resumable: rerun to continue. Must not run next to local generation, training or judging.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
I=work/v3/chunks_s1.jsonl; W=work/v3
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 30 --other-retries 3"
$G --log runs/v3/logs/s1_bank.log -- .venv/bin/python scripts/gen_questions.py --chunks $I --raw $W/questions_raw.jsonl --out $W/questions.jsonl --eval-slice-out $W/questions_evalslice.jsonl --reserve-out $W/questions_reserve.jsonl --stats runs/v3/bank_stats.md --concurrency 4
$G --log runs/v3/logs/s1_expand.log -- .venv/bin/python scripts/build_packs_v3.py expand --questions $W/questions.jsonl --out $W/expansions.jsonl --concurrency 4
echo S1_BANK_DONE >> runs/v3/logs/s1_bank.done
