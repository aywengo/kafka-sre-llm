#!/bin/sh
# v3 Phase 0 generation: base writes its own expansions, CPU builds packs, then RAG base (k6, k3) and the
# v2 inference variants answer questions_real. GPU jobs under run_guarded.py. Resumable: rerun to continue.
set -e
B=runs/v1/base27b; V2=adapters/kafka-sre-v2-resume; Q=eval/questions_real.jsonl; W=work/v3
SNAP=$HOME/.cache/huggingface/hub/models--mlx-community--Qwen3.8-27B-bf16/snapshots/6f265714824f3c38d4452baa1628aef3d9b9aae9
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 45"
R=".venv/bin/python scripts/rag_answers.py"
$G --log runs/v3/logs/p0_expand.log -- $R expand --model $B --questions $Q --out $W/real_exp_base.jsonl
[ -s $W/rag_packs_base_exp.json ] || .venv/bin/python scripts/build_packs.py $W/real_exp_base.jsonl $W/rag_packs_base_exp.json
$G --log runs/v3/logs/p0_rag_k6.log -- $R answer --model $B --questions $Q --packs $W/rag_packs_base_exp.json --k k6 --out $W/ans_rag_k6.jsonl
$G --log runs/v3/logs/p0_rag_k3.log -- $R answer --model $B --questions $Q --packs $W/rag_packs_base_exp.json --k k3 --out $W/ans_rag_k3.jsonl
$G --log runs/v3/logs/p0_v2_thorough.log -- $R answer --model $B --adapter $V2 --questions $Q --thorough --out $W/ans_v2_thorough.jsonl
$G --log runs/v3/logs/p0_v2_rag_k6.log -- $R answer --model $B --adapter $V2 --questions $Q --packs $W/rag_packs_base_exp.json --k k6 --out $W/ans_v2_rag_k6.jsonl
$G --log runs/v3/logs/p0_v2_rag_k3.log -- $R answer --model $B --adapter $V2 --questions $Q --packs $W/rag_packs_base_exp.json --k k3 --out $W/ans_v2_rag_k3.jsonl
$G --log runs/v3/logs/p0_v2_thinking.log -- $R answer --model $SNAP --adapter $V2 --questions $Q --thinking --max-tokens 4096 --out $W/ans_v2_thinking.jsonl
echo P0_GEN_DONE >> runs/v3/logs/p0_gen.done
