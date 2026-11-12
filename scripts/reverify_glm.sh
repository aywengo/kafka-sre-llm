#!/bin/sh
# GLM --reverify of (1) pass-1 apache-kafka-config items from non-"Type:" chunks, (2) all incidents
# items (with the +-1 neighbour context they were generated with). Needs JUDGE_API_KEY in env.
export OMLX_API_KEY="$JUDGE_API_KEY"
L=runs/v2/logs/reverify_glm.log
rv() { # RAW OUT [extra args]
  raw=$1; out=$2; shift 2
  scripts/run_retry.sh $L .venv/bin/python scripts/generate_qa.py \
    --url http://127.0.0.1:8000/v1 --model GLM-5.3-Flash-oQ4e --chunks work/v2/chunks.jsonl \
    --concurrency 3 --max-tokens 32768 --verify batch --verify-max-tokens 32768 --extra-body '{}' \
    --reverify "$raw" --out "$out" --max-reject-rate 0.4 "$@" || exit 1
}
rv work/v2/raw_qa_config_other.jsonl work/v2/raw_qa_config_other_glm.jsonl
echo CONFIG_OTHER_DONE >> $L
rv work/v2/raw_qa_incidents.jsonl work/v2/raw_qa_incidents_glm.jsonl --context-neighbours 1
echo REVERIFY_ALL_DONE >> $L
