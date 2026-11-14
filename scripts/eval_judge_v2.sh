#!/bin/sh
# GLM (primary) then DeepSeek judge passes over the cached v2 answers. Needs JUDGE_API_KEY in env.
Q="eval/questions.jsonl eval/questions_real.jsonl eval/questions_runbooks_holdout.jsonl eval/questions_feedback.jsonl eval/questions_synthetic_v2.jsonl"
cp eval/answers_v2run.jsonl eval/answers_v2run_glm_copy.jsonl
cp eval/answers_v2run.jsonl eval/answers_v2run_ds_copy.jsonl
run() { # LOG CACHE OUT JUDGE_MODEL EXTRA ANSWERS
  scripts/run_retry.sh $1 .venv/bin/python eval/compare.py --model runs/v1/base27b --adapter adapters/kafka-sre-v2-resume \
    --questions $Q --max-tokens 2048 --cache $6 --out $3 --judge-url http://127.0.0.1:8000/v1 \
    --judge-model $4 --judge-extra-body "$5" --judge-concurrency 3 --judge-cache $2 || exit 1
}
run runs/v2/logs/judge_glm.log eval/judge_v2_glm.cache.jsonl eval/results_v2_glm.jsonl GLM-5.3-Flash-oQ4e '{"max_tokens": 32768}' eval/answers_v2run_glm_copy.jsonl
echo GLM_DONE >> runs/v2/logs/judge_glm.log
run runs/v2/logs/judge_ds.log eval/judge_v2_ds.cache.jsonl eval/results_v2_ds.jsonl DeepSeek-V4.1-Flash-1017-oQ4e-mtp '{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}' eval/answers_v2run_ds_copy.jsonl
echo DS_DONE >> runs/v2/logs/judge_ds.log
