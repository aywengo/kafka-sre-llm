#!/bin/sh
# passes 1-4 of the v2 full run, sequential and resumable (needs OMLX_API_KEY in env).
# usage: gen_v2_full.sh [PID_TO_WAIT_FOR]
L=runs/v2/logs/generate.log
R=work/v2/raw_qa.jsonl
while [ -n "$1" ] && kill -0 "$1" 2>/dev/null; do sleep 30; done
scripts/gen_v2.sh $L --out $R --sources apache-kafka apache-kafka-config strimzi argo-cd || exit 1
# pass 2 budget = 1500 minus kubernetes chunks already written
have=$(grep -c '"source": "kubernetes"' $R)
left=$((1500 - have))
if [ "$left" -gt 0 ]; then
  scripts/gen_v2.sh $L --out $R --sources kubernetes --limit $left || exit 1
fi
scripts/gen_v2.sh $L --out $R --sources runbooks --max-per-chunk 8 || exit 1
scripts/gen_v2.sh $L --out work/v2/raw_qa_runbooks2.jsonl --sources runbooks --max-per-chunk 8 --seed 1 --temperature 0.9 || exit 1
echo ALL_PASSES_DONE >> $L
