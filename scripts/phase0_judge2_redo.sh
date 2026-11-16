#!/bin/sh
OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
L=runs/v3/logs/p0_judge2.log
W=work/v3/judge
scripts/run_guarded.py --log $L --heartbeat runs/v3/HEARTBEAT.md --stall-min 60 -- .venv/bin/python eval/compare.py --model runs/v1/base27b --adapter none --questions eval/questions_real.jsonl --max-tokens 2048 --cache $W/h2h.answers.jsonl --out $W/results_h2h_p1.jsonl --judge-url $OMLX_URL/v1 --judge-model GLM-5.3-Flash-oQ8e-mtp --judge-extra-body '{"max_tokens": 32768}' --judge-concurrency 3 --judge-cache $W/judge_h2h_p1.cache.jsonl
