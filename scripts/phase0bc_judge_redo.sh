#!/bin/sh
# rerun of the one unscored judge item (ds-exp pass 1, Q23) at temperature 0
OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
scripts/run_guarded.py --log runs/v3/logs/p0bc_judge.log --heartbeat runs/v3/HEARTBEAT.md --stall-min 60 -- .venv/bin/python eval/compare.py --model runs/v1/base27b --adapter none --questions eval/questions_real.jsonl --max-tokens 2048 --cache work/v3/judge/h2h_dsexp.answers.jsonl --out work/v3/judge/results_h2h_dsexp_p1.jsonl --judge-url $OMLX_URL/v1 --judge-model GLM-5.3-Flash-oQ8e-mtp --judge-extra-body '{"max_tokens": 32768}' --judge-concurrency 3 --judge-cache work/v3/judge/judge_h2h_dsexp_p1.cache.jsonl --judge-temperature 0
