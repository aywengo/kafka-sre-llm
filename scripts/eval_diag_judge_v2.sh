#!/bin/sh
# Judge pass of the questions_real diagnostic with GLM-5.3-Flash-oQ8e-mtp. Needs JUDGE_API_KEY in env.
OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
while pgrep -f "compare.py.*work/v2/diag" >/dev/null; do sleep 20; done
D=work/v2/diag
for v in ckpt600 ckpt1050 scale10; do
  scripts/run_retry.sh runs/v2/logs/diag_judge.log .venv/bin/python eval/compare.py --model runs/v1/base27b --adapter $D/$v \
    --questions eval/questions_real.jsonl --max-tokens 2048 --cache $D/answers_$v.jsonl --out $D/results_${v}_oq8.jsonl \
    --judge-url $OMLX_URL/v1 --judge-model GLM-5.3-Flash-oQ8e-mtp --judge-extra-body '{"max_tokens": 32768}' \
    --judge-concurrency 3 --judge-cache $D/judge_${v}_oq8.cache.jsonl || exit 1
done
echo DIAG_DONE >> runs/v2/logs/diag_judge.log
