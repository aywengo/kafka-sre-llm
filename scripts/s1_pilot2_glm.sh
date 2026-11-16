#!/bin/sh
# Second verifier (GLM oQ4e) on the re-pilot answers. Run only when DeepSeek is unloaded: the two do not fit
# in the model server's memory together.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
I=work/v3/chunks_s1.jsonl; O=work/v3/pilot2; GLM=GLM-5.3-Flash-oQ4e
scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 45 --other-retries 2 --log runs/v3/logs/s1_pilot2_glm.log -- \
  .venv/bin/python scripts/generate_qa.py --mode pack-verify \
  --url $OMLX_URL/v1 --model $GLM --verify-model $GLM --extra-body '{}' --verify-extra-body '{}' \
  --verify-max-tokens 32768 --in $O/pilot_pack.jsonl --index $I --out $O/pilot_pack_glm.jsonl \
  --concurrency 3 --stats $O/pilot_glm_stats.md
echo S1_PILOT2_GLM_DONE >> runs/v3/logs/s1_pilot2_glm.done
