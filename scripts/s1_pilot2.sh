#!/bin/sh
# Stage 1 re-pilot: the same 40 questions and packs as scripts/s1_pilot.sh, with the revised prompts
# (PACK_PROMPT_VERSION pack/2). Answers and the first verifier (DeepSeek), then the mechanical check.
# The second verifier (GLM oQ4e) cannot be loaded next to DeepSeek and runs separately: s1_pilot2_glm.sh.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
I=work/v3/chunks_s1.jsonl; P=work/v3/pilot; O=work/v3/pilot2; mkdir -p $O
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp
NOTHINK='{"chat_template_kwargs": {"thinking_mode": "chat"}}'
scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 30 --other-retries 2 --log runs/v3/logs/s1_pilot2_gen.log -- \
  .venv/bin/python scripts/generate_qa.py --mode pack \
  --url $OMLX_URL/v1 --model $DS --verify-model $DS --extra-body "$NOTHINK" --verify-extra-body "$NOTHINK" \
  --questions $P/pilot_questions.jsonl --packs $P/pilot_packs.jsonl --index $I \
  --out $O/pilot_pack.jsonl --verify each --concurrency 2 --stats $O/pilot_gen_stats.md
.venv/bin/python - <<'PY'
import json
O = "work/v3/pilot2/"
rows = [json.loads(l) for l in open(O + "pilot_pack.jsonl")]
with open(O + "pilot_all_answers.jsonl", "w") as a, open(O + "pilot_all_packs.jsonl", "w") as p:
    for r in rows:
        if not r.get("skip") and r.get("answer"):
            a.write(json.dumps({"question": r["question"], "text": r["answer"]}, ensure_ascii=False) + "\n")
            p.write(json.dumps({"question": r["question"], "k6": r["chunk_ids"]}, ensure_ascii=False) + "\n")
PY
.venv/bin/python scripts/invention_count.py --answers $O/pilot_all_answers.jsonl --packs $O/pilot_all_packs.jsonl \
  --index $I --json $O/pilot_all_invention.json > $O/pilot_all_invention.txt
echo S1_PILOT2_DONE >> runs/v3/logs/s1_pilot2.done
