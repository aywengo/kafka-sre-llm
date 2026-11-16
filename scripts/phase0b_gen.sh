#!/bin/sh
# Phase 0b: two inference-only RAG variants on questions_real (k6 / 14,000, base model, max_tokens 2048). Resumable.
B=runs/v1/base27b; Q=eval/questions_real.jsonl; W=work/v3
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 60"
R=".venv/bin/python scripts/rag_answers.py"
$G --log runs/v3/logs/p0b_strict.log -- $R answer --model $B --questions $Q --packs $W/rag_packs_base_exp.json --k k6 --strict --out $W/ans_rag_strict.jsonl || exit 1
$G --log runs/v3/logs/p0b_dsexp.log -- $R answer --model $B --questions $Q --packs $W/real_packs_frozen.json --k k6 --out $W/ans_rag_dsexp.jsonl || exit 1
echo P0B_GEN_DONE >> runs/v3/logs/p0b_gen.done
