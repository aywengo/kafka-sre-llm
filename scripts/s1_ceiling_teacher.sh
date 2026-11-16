#!/bin/sh
# Stage 1 ceiling check, teacher side (agents/train-v3-stage1.md, step 2): DeepSeek writes pack-mode answers
# with the pilot prompt for an eval set, from the same packs base + retrieval answered from. These answers are
# judged head-to-head against base + retrieval; they are never training data.
#   s1_ceiling_teacher.sh holdout|real
set -e
S=${1:?holdout or real}
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
I=work/v3/chunks_s1.jsonl; C=work/v3/ceiling
NOTHINK='{"chat_template_kwargs": {"thinking_mode": "chat"}}'
scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 30 --other-retries 2 --log runs/v3/logs/s1_ceiling_teacher_$S.log -- \
  .venv/bin/python scripts/generate_qa.py --mode pack --url $OMLX_URL/v1 --model DeepSeek-V4.1-Flash-1017-oQ4e-mtp \
  --extra-body "$NOTHINK" --questions $C/${S}_questions.jsonl --packs work/v3/packs_${S}_s1.jsonl --index $I \
  --out $C/teacher_${S}.jsonl --concurrency 2 --stats $C/teacher_${S}_stats.md
echo S1_CEILING_TEACHER_DONE >> runs/v3/logs/s1_ceiling_teacher_$S.done
