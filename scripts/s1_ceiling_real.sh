#!/bin/sh
# Stage 1 ceiling check on the 40 real questions (agents/train-v3-stage1.md, step 2; Roman's go 2026-11-17).
# Two contestants against base + retrieval, same packs (work/v3/packs_real_s1.jsonl, k6), GLM oQ8e, two passes:
#   teacher: DeepSeek writes pack-mode answers with the pilot prompt (pack/4);
#   student: the base model with the teacher's rules as instructions (rag_answers.py --rules).
# None of these answers is training data. Unattended: it waits for the holdout judging, pauses the bank's
# search-terms step for the GPU and judging steps, writes runs/v3/CEILING_REAL.md, and resumes the search terms
# only if the teacher is at +0.3 or better (below that the stage stops and the search terms are not needed).
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
export JUDGE_API_KEY=$OMLX_API_KEY
I=work/v3/chunks_s1.jsonl; W=work/v3; C=$W/ceiling; J=$W/judge; Q=eval/questions_real.jsonl; L=runs/v3/logs
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
fail() { hb "s1_ceiling_real: STOPPED: $1" yes; echo "$1" >> $L/s1_ceiling_real.failed; exit 1; }
busy() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/admin/api/models/$1/unload" >/dev/null; hb "s1_ceiling_real: unloaded $1 (no active or waiting requests)"; }

# 0. the holdout judging of the overnight chain must be finished first (it holds the judge model)
until [ -e $L/s1_night_judge.done ]; do sleep 30; done
hb "s1_ceiling_real: started (teacher and prompted student on the 40 real questions against base + retrieval)"

# 1. teacher answers (DeepSeek; runs next to the bank's search terms, same model)
[ -e $L/s1_ceiling_teacher_real.done ] || scripts/s1_ceiling_teacher.sh real
.venv/bin/python - <<'PY' || fail "teacher answers failed the sanity check"
import json, sys
rows = [json.loads(l) for l in open("work/v3/ceiling/teacher_real.jsonl")]
ok = [r for r in rows if (r.get("answer") or "").strip() and r.get("prompt_version") == "pack/4"]
with open("work/v3/ceiling/teacher_real_answers.jsonl", "w") as f:
    for r in rows:
        t = r.get("answer") or ""
        f.write(json.dumps({"label": "tuned", "question": r["question"], "text": t, "tokens": len(t) // 4,
                            "truncated": r.get("gen_finish") == "length"}, ensure_ascii=False) + "\n")
print("teacher: rows", len(rows), "answered with pack/4", len(ok), "coverage",
      {c: sum(r.get("coverage") == c for r in rows) for c in ("full", "partial", "none")})
sys.exit(0 if len(rows) == 40 and len(ok) >= 37 else 1)
PY

# 2. pause the bank's search terms (resumable) and free the GPU for the local model
n=0; while pgrep -f "scripts/s1_bank.sh" >/dev/null; do
  pkill -TERM -f "run_guarded.py.*s1_expand.log" || true
  sleep 10; n=$((n+1)); [ $n -lt 90 ] || fail "s1_bank.sh did not exit"
done
unload $DS
unload $GLM8

# 3. prompted student: the base model with the teacher's rules, local GPU
$G --stall-min 45 --log $L/s1_ceiling_student.log -- .venv/bin/python scripts/rag_answers.py answer --model runs/v1/base27b \
  --questions $Q --packs $W/packs_real_s1.jsonl --k k6 --index $I --rules --out $C/ans_student_rules_real.jsonl
[ "$(wc -l < $C/ans_student_rules_real.jsonl | tr -d ' ')" = "40" ] || fail "prompted student wrote fewer than 40 answers"

# 4. judging, GLM oQ8e alone, two passes per contestant, one retry for an unscored item
hb "s1_ceiling_real: $GLM8 loads on first request (judge)"
judge() { # NAME PASS [extra flags]
  $G --stall-min 60 --log $L/s1_ceiling_judge.log -- .venv/bin/python eval/compare.py --model runs/v1/base27b --adapter none \
    --questions $Q --max-tokens 2048 --cache $J/h2h_$1.answers.jsonl --out $J/results_h2h_$1_$2.jsonl \
    --judge-url $OMLX_URL/v1 --judge-model $GLM8 --judge-extra-body '{"max_tokens": 32768}' --judge-concurrency 3 \
    --judge-cache $J/judge_h2h_$1_$2.cache.jsonl $3
}
.venv/bin/python scripts/mk_h2h.py $W/ans_rag_real_s1.jsonl $C/teacher_real_answers.jsonl $J/h2h_teacher_real.answers.jsonl $Q
.venv/bin/python scripts/mk_h2h.py $W/ans_rag_real_s1.jsonl $C/ans_student_rules_real.jsonl $J/h2h_student_real.answers.jsonl $Q
for name in teacher_real student_real; do
  for p in p1 p2; do
    flip=""; [ $p = p2 ] && flip="--judge-flip"
    judge $name $p $flip
    if .venv/bin/python - $J/judge_h2h_${name}_$p.cache.jsonl <<'PY'
import json, sys
p = sys.argv[1]; rows = [json.loads(l) for l in open(p)]
bad = [r for r in rows if not r.get("j")]
if bad:
    open(p, "w").write("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows if r.get("j")))
print("unscored:", len(bad)); sys.exit(0 if bad else 1)
PY
    then judge $name $p "$flip --judge-temperature 0"; fi
  done
done
unload $GLM8

# 5. the mechanical check and the summary
for a in "ragbase $W/ans_rag_real_s1.jsonl" "teacher $C/teacher_real_answers.jsonl" "student $C/ans_student_rules_real.jsonl"; do
  set -- $a
  .venv/bin/python scripts/invention_count.py --answers $2 --packs $W/packs_real_s1.jsonl --index $I > $C/invention_real_$1.txt
done
.venv/bin/python scripts/s1_ceiling_summary.py > runs/v3/CEILING_REAL.md
V=$(sed -n 's/^verdict: //p' runs/v3/CEILING_REAL.md | head -1)
hb "s1_ceiling_real: done, verdict by the rule fixed in advance: $V (runs/v3/CEILING_REAL.md)" yes
echo S1_CEILING_REAL_DONE >> $L/s1_ceiling_real.done

# 6. the bank's search terms are only worth resuming if the stage is not stopped
case "$V" in stop*) hb "s1_ceiling_real: search terms not resumed (the rule says stop)" yes ;; *) scripts/s1_bank.sh ;; esac
