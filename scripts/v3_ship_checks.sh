#!/bin/sh
# Shipping checks of v3 (the Owner ships retrieval on the base model). Unattended; start it only when nothing else uses the server.
#   1. judging, GLM oQ8e alone: base + retrieval on the frozen stage-1 index against the plain base model, 40 real questions, two passes;
#   2. the end-to-end smoke test of kafka_sre_rag on the local GPU (first 3 real questions, mlx, base model, k6, default mode);
#   3. runs/v3/SHIP_CHECKS.md (scripts/v3_ship_summary.py).
# It waits for scripts/s1_ceiling_real.sh to finish. Markers: runs/v3/logs/v3_ship_checks.done or .failed.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
export JUDGE_API_KEY=$OMLX_API_KEY
I=work/v3/chunks_s1.jsonl; W=work/v3; J=$W/judge; SH=$W/ship; Q=eval/questions_real.jsonl; L=runs/v3/logs
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
N=ragbase_s1_vs_base
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "v3 shipping checks: judging base + retrieval (frozen stage-1 index) against plain base, runtime smoke test",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
fail() { hb "v3_ship_checks: STOPPED: $1" yes; echo "$1" >> $L/v3_ship_checks.failed; status STOPPED yes "v3_ship_checks stopped: $1; marker runs/v3/logs/v3_ship_checks.failed"; exit 1; }
busy() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/admin/api/models/$1/unload" >/dev/null; hb "v3_ship_checks: unloaded $1 (no active or waiting requests)"; }

rm -f $L/v3_ship_checks.failed $L/v3_ship_checks.done
mkdir -p $J $SH runs/v3/reports

# 0. the ceiling chain owns the server until it has finished
until ! pgrep -f "scripts/s1_ceiling_real.sh" >/dev/null; do sleep 60; done
CEIL=""
if [ -e $L/s1_ceiling_real.failed ]; then
  CEIL=" (s1_ceiling_real.sh left a failed marker; these checks do not depend on it)"
  hb "v3_ship_checks: runs/v3/logs/s1_ceiling_real.failed exists; noted, continuing$CEIL"
fi
hb "v3_ship_checks: started (judging base + retrieval on the frozen index against plain base, then the runtime smoke test)$CEIL"
status RUNNING no "scripts/v3_ship_checks.sh; result runs/v3/SHIP_CHECKS.md; markers runs/v3/logs/v3_ship_checks.done or .failed"

# 1. inputs: the plain-base rows of the diagnostic file, and the variant answers
.venv/bin/python - <<'PY' || fail "plain-base answers or variant answers are not complete (need 40 questions each)"
import json, sys
rd = lambda p: [json.loads(l) for l in open(p) if l.strip()]
qs = [r["question"] for r in rd("eval/questions_real.jsonl")]
base = {r["question"]: r for r in rd("work/v2/diag/answers_final20.jsonl") if r.get("label") == "base"}
var = {r["question"]: r for r in rd("work/v3/ans_rag_real_s1.jsonl")}
miss_b = [i + 1 for i, q in enumerate(qs) if q not in base or not (base[q].get("text") or "").strip()]
miss_v = [i + 1 for i, q in enumerate(qs) if q not in var or not (var[q].get("text") or "").strip()]
print("questions", len(qs), "plain-base rows", len(base), "variant rows", len(var), "missing base", miss_b, "missing variant", miss_v)
if len(qs) != 40 or miss_b or miss_v:
    sys.exit(1)
with open("work/v3/judge/base_plain_real.answers.jsonl", "w") as f:
    for q in qs:
        f.write(json.dumps(base[q], ensure_ascii=False) + "\n")
PY
.venv/bin/python scripts/mk_h2h.py $J/base_plain_real.answers.jsonl $W/ans_rag_real_s1.jsonl $J/h2h_$N.answers.jsonl $Q
[ "$(wc -l < $J/h2h_$N.answers.jsonl | tr -d ' ')" = "80" ] || fail "head-to-head answers cache does not have 80 rows"

# 2. judging, GLM oQ8e alone, two passes, one retry for an unscored item
unload $DS
unload $GLM4
hb "v3_ship_checks: $GLM8 loads on first request (judge); DeepSeek and GLM oQ4e unloaded if they were loaded"
judge() { # PASS [extra flags]
  $G --stall-min 60 --log $L/v3_ship_judge.log -- .venv/bin/python eval/compare.py --model runs/v1/base27b --adapter none \
    --questions $Q --max-tokens 2048 --cache $J/h2h_$N.answers.jsonl --out $J/results_h2h_${N}_$1.jsonl \
    --judge-url $OMLX_URL/v1 --judge-model $GLM8 --judge-extra-body '{"max_tokens": 32768}' --judge-concurrency 3 \
    --judge-cache $J/judge_h2h_${N}_$1.cache.jsonl $2
}
for p in p1 p2; do
  flip=""; [ $p = p2 ] && flip="--judge-flip"
  judge $p "$flip" || fail "judging pass $p failed (see runs/v3/logs/v3_ship_judge.log)"
  if .venv/bin/python - $J/judge_h2h_${N}_$p.cache.jsonl <<'PY'
import json, sys
p = sys.argv[1]; rows = [json.loads(l) for l in open(p)]
bad = [r for r in rows if not r.get("j")]
if bad:
    open(p, "w").write("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows if r.get("j")))
print("unscored:", len(bad)); sys.exit(0 if bad else 1)
PY
  then judge $p "$flip --judge-temperature 0" || fail "judging retry $p failed (see runs/v3/logs/v3_ship_judge.log)"; fi
done
unload $GLM8
hb "v3_ship_checks: judging finished, $GLM8 unloaded; the runtime smoke test loads the base model on the local GPU"

# 3. smoke test of the runtime, local GPU (nothing runs on the server now)
$G --stall-min 45 --log $L/v3_ship_smoke.log -- .venv/bin/python scripts/v3_ship_smoke.py --model runs/v1/base27b \
  --questions $Q --n 3 --out-dir $SH || fail "runtime smoke test failed (see runs/v3/logs/v3_ship_smoke.log)"
for n in 1 2 3; do [ -s $SH/smoke_$n.json ] || fail "smoke test did not write smoke_$n.json"; done

# 4. the summary
.venv/bin/python scripts/v3_ship_summary.py > runs/v3/SHIP_CHECKS.md.tmp || fail "summary failed"
mv runs/v3/SHIP_CHECKS.md.tmp runs/v3/SHIP_CHECKS.md
R=$(grep -m1 '^result:' runs/v3/SHIP_CHECKS.md)
hb "v3_ship_checks: done, $R (runs/v3/SHIP_CHECKS.md)" yes
status DONE yes "$R; runs/v3/SHIP_CHECKS.md"
echo V3_SHIP_CHECKS_DONE >> $L/v3_ship_checks.done
