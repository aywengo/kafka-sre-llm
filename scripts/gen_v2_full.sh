#!/bin/sh
# passes 1-4 of the v2 full run, sequential (needs OMLX_API_KEY in env)
L=runs/v2/logs/generate.log
R=work/v2/raw_qa.jsonl
scripts/gen_v2.sh $L --out $R --sources apache-kafka apache-kafka-config strimzi argo-cd || exit 1
scripts/gen_v2.sh $L --out $R --sources kubernetes --limit 1500 || exit 1
scripts/gen_v2.sh $L --out $R --sources runbooks --max-per-chunk 8 || exit 1
scripts/gen_v2.sh $L --out work/v2/raw_qa_runbooks2.jsonl --sources runbooks --max-per-chunk 8 --seed 1 --temperature 0.9 || exit 1
echo ALL_PASSES_DONE >> $L
