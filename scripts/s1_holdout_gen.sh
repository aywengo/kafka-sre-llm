#!/bin/sh
# Stage 1, GPU: base + retrieval answers on the frozen stage-1 index (runs/v3/INDEX_FROZEN.json).
#   1. smoke: one real question with an unchanged pack must reproduce its cached Phase 0 answer
#      (checks the prompt refactor end to end and that greedy decoding repeats);
#   2. the base model writes search terms for the 42 runbook-holdout questions; CPU builds their packs;
#   3. answers on the holdout; 4. answers on the real questions whose pack changed (the rest are reused).
# Resumable: rerun to continue. Generation never runs next to judging or server-side generation.
set -e
B=runs/v1/base27b; I=work/v3/chunks_s1.jsonl; W=work/v3; H=eval/questions_runbooks_holdout.jsonl
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 45"
R=".venv/bin/python scripts/rag_answers.py"
$G --log runs/v3/logs/s1_smoke.log -- $R answer --model $B --questions $W/s1_smoke_q.jsonl --packs $W/packs_real_s1.jsonl --k k6 --index $I --out $W/s1_smoke_ans.jsonl
.venv/bin/python - <<'PY' || { echo SMOKE_FAILED >> runs/v3/logs/s1_holdout_gen.failed; exit 1; }
import json, sys
new = json.loads(open("work/v3/s1_smoke_ans.jsonl").readline())
old = {json.loads(l)["question"]: json.loads(l) for l in open("work/v3/ans_rag_k6.jsonl")}[new["question"]]
same = new["text"] == old["text"]
print("smoke: answer identical to Phase 0:", same, "tokens", new["tokens"], old["tokens"])
sys.exit(0 if same else 1)
PY
$G --log runs/v3/logs/s1_holdout_expand.log -- $R expand --model $B --questions $H --out $W/holdout_exp_base.jsonl
HF_HUB_OFFLINE=1 .venv/bin/python scripts/build_packs_v3.py pack --expansions $W/holdout_exp_base.jsonl --index $I --out $W/packs_holdout_s1.jsonl
$G --log runs/v3/logs/s1_holdout_answers.log -- $R answer --model $B --questions $H --packs $W/packs_holdout_s1.jsonl --k k6 --index $I --out $W/ans_rag_holdout_s1.jsonl
$G --log runs/v3/logs/s1_real_answers.log -- $R answer --model $B --questions eval/questions_real.jsonl --packs $W/packs_real_s1.jsonl --k k6 --index $I --out $W/ans_rag_real_s1.jsonl
echo S1_HOLDOUT_GEN_DONE >> runs/v3/logs/s1_holdout_gen.done
