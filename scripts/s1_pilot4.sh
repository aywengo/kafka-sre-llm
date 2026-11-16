#!/bin/sh
# Stage 1, small check of the not-covered behaviour under prompt version pack/4 (the only change from pack/3 is
# the fixed line "What it does cover:" after the gap sentence in not-covered answers): 12 unused out-of-corpus
# questions and the not-covered questions of the third pilot. DeepSeek writes and verifies; mechanical filter.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
I=work/v3/chunks_s1.jsonl; O=work/v3/pilot4
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp
NOTHINK='{"chat_template_kwargs": {"thinking_mode": "chat"}}'
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 30 --other-retries 2"
$G --log runs/v3/logs/s1_pilot4_expand.log -- .venv/bin/python scripts/build_packs_v3.py expand \
  --questions $O/ooc_questions.jsonl --out $O/ooc_expansions.jsonl --concurrency 2
HF_HUB_OFFLINE=1 .venv/bin/python scripts/build_packs_v3.py pack --questions $O/ooc_questions.jsonl \
  --expansions $O/ooc_expansions.jsonl --index $I --out $O/ooc_packs.jsonl
gen() { # QUESTIONS PACKS OUT STATS LOG
  $G --log $5 -- .venv/bin/python scripts/generate_qa.py --mode pack \
    --url $OMLX_URL/v1 --model $DS --verify-model $DS --extra-body "$NOTHINK" --verify-extra-body "$NOTHINK" \
    --questions $1 --packs $2 --index $I --out $3 --verify each --concurrency 2 --stats $4
}
gen $O/ooc_questions.jsonl $O/ooc_packs.jsonl $O/ooc_pack.jsonl $O/ooc_gen_stats.md runs/v3/logs/s1_pilot4_ooc.log
gen $O/none_redo_questions.jsonl work/v3/pilot3/pilot_packs.jsonl $O/none_redo_pack.jsonl $O/none_redo_gen_stats.md runs/v3/logs/s1_pilot4_redo.log
echo S1_PILOT4_DONE >> runs/v3/logs/s1_pilot4.done
