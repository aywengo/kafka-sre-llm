#!/bin/sh
# Stage 1, overnight chain on the model server. It waits for the question bank's generation step, the teacher's
# holdout answers and the not-covered check, then uses the server for the two results the go decision needs,
# and afterwards resumes the bank's search terms:
#   1. GLM oQ4e as second verifier on the pilot rows (it cannot be loaded next to DeepSeek);
#   2. ceiling check on the runbook holdout: teacher answers against base + retrieval, GLM oQ8e, two passes;
#   3. scripts/s1_bank.sh again (resumes; DeepSeek writes the search terms).
# Model swaps happen only when the server has no active or waiting request, and each is noted in HEARTBEAT.md.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
export JUDGE_API_KEY=$OMLX_API_KEY
I=work/v3/chunks_s1.jsonl; W=work/v3; C=$W/ceiling; J=$W/judge; H=eval/questions_runbooks_holdout.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM4=GLM-5.3-Flash-oQ4e; GLM8=GLM-5.3-Flash-oQ8e-mtp
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
hb() { printf '\n### %s — %s\nneeds_human: no\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" >> runs/v3/HEARTBEAT.md; }
busy() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || { hb "s1_night: server still busy after 60 min, stopping"; exit 1; }; done; }
unload() { idle; curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/admin/api/models/$1/unload" >/dev/null; hb "s1_night: unloaded $1 (no active or waiting requests)"; }

# 0. wait for the jobs that are still using DeepSeek
until [ -s runs/v3/bank_stats.md ] && [ -e runs/v3/logs/s1_ceiling_teacher_holdout.done ] && [ -e runs/v3/logs/s1_pilot4.done ]; do sleep 60; done
# the bank chain goes straight on to its search-terms step; stop that step through its guard (it resumes in step 3)
n=0; while pgrep -f "scripts/s1_bank.sh" >/dev/null; do
  pkill -TERM -f "run_guarded.py.*s1_expand.log" || true
  sleep 10; n=$((n+1)); [ $n -lt 90 ] || { hb "s1_night: s1_bank.sh did not exit, stopping"; exit 1; }
done
hb "s1_night: question bank generated ($(wc -l < $W/questions.jsonl | tr -d ' ') questions kept); search-terms step paused for the second verifier and the ceiling check"
unload $DS

# 1. second verifier on every pilot 3 row and on the not-covered check
hb "s1_night: $GLM4 loads on first request (second verifier on the pilot rows)"
for f in pilot3/pilot_pack pilot3/redo_pack pilot4/ooc_pack pilot4/none_redo_pack; do
  $G --stall-min 45 --log runs/v3/logs/s1_night_glm.log -- .venv/bin/python scripts/generate_qa.py --mode pack-verify \
    --url $OMLX_URL/v1 --model $GLM4 --verify-model $GLM4 --extra-body '{}' --verify-extra-body '{}' \
    --verify-max-tokens 32768 --in $W/$f.jsonl --index $I --out $W/${f}_glm.jsonl --concurrency 3 --stats $W/${f}_glm_stats.md
done
echo S1_NIGHT_GLM_DONE >> runs/v3/logs/s1_night_glm.done
unload $GLM4

# 2. ceiling check on the holdout: teacher (prompt pack/4) against base + retrieval, same packs, two passes
.venv/bin/python - <<'PY'
import json
rows = [json.loads(l) for l in open("work/v3/ceiling/teacher_holdout.jsonl")]
with open("work/v3/ceiling/teacher_holdout_answers.jsonl", "w") as f:
    for r in rows:
        t = r.get("answer") or ""
        f.write(json.dumps({"label": "tuned", "question": r["question"], "text": t, "tokens": len(t) // 4,
                            "truncated": r.get("gen_finish") == "length"}, ensure_ascii=False) + "\n")
print("teacher answers:", len(rows), "empty:", sum(not (r.get("answer") or "") for r in rows))
PY
.venv/bin/python scripts/mk_h2h.py $W/ans_rag_holdout_s1.jsonl $C/teacher_holdout_answers.jsonl $J/h2h_teacher_holdout.answers.jsonl $H
hb "s1_night: $GLM8 loads on first request (judge, ceiling check on the holdout)"
judge() { # PASS [extra flag]
  $G --stall-min 60 --log runs/v3/logs/s1_night_judge.log -- .venv/bin/python eval/compare.py --model runs/v1/base27b --adapter none \
    --questions $H --max-tokens 2048 --cache $J/h2h_teacher_holdout.answers.jsonl --out $J/results_h2h_teacher_holdout_$1.jsonl \
    --judge-url $OMLX_URL/v1 --judge-model $GLM8 --judge-extra-body '{"max_tokens": 32768}' --judge-concurrency 3 \
    --judge-cache $J/judge_h2h_teacher_holdout_$1.cache.jsonl $2
}
for p in p1 p2; do
  flip=""; [ $p = p2 ] && flip="--judge-flip"
  judge $p $flip
  # an item the judge left unscored is removed from the cache and judged once more (as in Phase 0)
  if .venv/bin/python - $J/judge_h2h_teacher_holdout_$p.cache.jsonl <<'PY'
import json, sys
p = sys.argv[1]; rows = [json.loads(l) for l in open(p)]
bad = [r for r in rows if not r.get("j")]
if bad:
    open(p, "w").write("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows if r.get("j")))
print("unscored:", len(bad)); sys.exit(0 if bad else 1)
PY
  then judge $p "$flip --judge-temperature 0"; fi
done
echo S1_NIGHT_JUDGE_DONE >> runs/v3/logs/s1_night_judge.done
unload $GLM8

# 3. back to the bank: search terms for every question (DeepSeek loads on first request)
hb "s1_night: second verifier and holdout judging done; resuming scripts/s1_bank.sh for the search terms ($DS loads on first request)"
scripts/s1_bank.sh
echo S1_NIGHT_DONE >> runs/v3/logs/s1_night.done
