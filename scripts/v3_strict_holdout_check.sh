#!/bin/sh
# Strict mode against default mode on the runbook holdout (42 questions outside questions_real). Unattended; start it only when
# you accept that it takes the local GPU and then the server after scripts/v3_v2rag_check.sh has finished.
#   1. inputs check;
#   2. local GPU: strict answers (--strict, base model, no adapter) from the same packs and settings as the default holdout answers;
#   3. sanity: 42 non-empty strict answers, mean tokens of both sets;
#   4. judging, GLM oQ8e alone, head-to-head strict against default (default = baseline side), two passes, one retry for an unscored item;
#   5. CPU: mechanical invention counts for both answer sets;
#   6. runs/v3/STRICT_HOLDOUT_CHECK.md (scripts/v3_strict_holdout_summary.py). The result waits for the Owner's decision on the default mode.
# Waits for scripts/v3_v2rag_check.sh. Markers: runs/v3/logs/v3_strict_holdout_check.done or .failed.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
export JUDGE_API_KEY=$OMLX_API_KEY
I=work/v3/chunks_s1.jsonl; W=work/v3; J=$W/judge; S=$W/strict; H=eval/questions_runbooks_holdout.jsonl; L=runs/v3/logs
B=runs/v1/base27b; N=holdout_strict_vs_default
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "v3 strict-mode check: strict against default answers on the runbook holdout (frozen stage-1 index)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
fail() { hb "v3_strict_holdout_check: STOPPED: $1" yes; echo "$1" >> $L/v3_strict_holdout_check.failed; status STOPPED yes "v3_strict_holdout_check stopped: $1; marker runs/v3/logs/v3_strict_holdout_check.failed"; exit 1; }
busy() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/admin/api/models/$1/unload" >/dev/null; hb "v3_strict_holdout_check: unloaded $1 (no active or waiting requests)"; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }

rm -f $L/v3_strict_holdout_check.failed $L/v3_strict_holdout_check.done
mkdir -p $J $S runs/v3/reports

# 0. the v2 retrieval-only chain owns the server and the GPU until it has finished
until ! pgrep -f "scripts/v3_v2rag_check.sh" >/dev/null; do sleep 60; done
PRED=""
if [ -e $L/v3_v2rag_check.failed ]; then
  PRED=" (v3_v2rag_check.sh left a failed marker; this check does not depend on it)"
  hb "v3_strict_holdout_check: runs/v3/logs/v3_v2rag_check.failed exists; noted, continuing$PRED"
fi
hb "v3_strict_holdout_check: started (strict answers on the 42 holdout questions, judged head to head against the default answers)$PRED"
status RUNNING no "scripts/v3_strict_holdout_check.sh; result runs/v3/STRICT_HOLDOUT_CHECK.md; markers runs/v3/logs/v3_strict_holdout_check.done or .failed"

# 1. inputs
for f in $I $H $W/packs_holdout_s1.jsonl $W/ans_rag_holdout_s1.jsonl; do
  [ -s $f ] || fail "input $f is missing or empty"
done
for f in $H $W/packs_holdout_s1.jsonl $W/ans_rag_holdout_s1.jsonl; do
  [ "$(rows $f)" = "42" ] || fail "$f has $(rows $f) rows, not 42"
done
echo "inputs: holdout questions 42, packs 42, default answers 42, index present"

# 2. strict answers on the local GPU; no server model may stay loaded (same order as the other chains)
unload $DS
unload $GLM4
unload $GLM8
hb "v3_strict_holdout_check: server models unloaded if they were loaded; the base model loads on the local GPU (strict answers)"
if [ "$(rows $S/ans_rag_holdout_strict.jsonl)" != "42" ]; then
  $G --stall-min 45 --log $L/v3_strict_holdout_answers.log -- .venv/bin/python scripts/rag_answers.py answer --model $B \
    --questions $H --packs $W/packs_holdout_s1.jsonl --k k6 --index $I --strict --out $S/ans_rag_holdout_strict.jsonl \
    || fail "strict answers failed (see runs/v3/logs/v3_strict_holdout_answers.log)"
fi
[ "$(rows $S/ans_rag_holdout_strict.jsonl)" = "42" ] || fail "ans_rag_holdout_strict.jsonl has $(rows $S/ans_rag_holdout_strict.jsonl) answers, not 42"

# 3. sanity
.venv/bin/python - <<'PY' || fail "strict answers failed the sanity check (empty answers or more truncated than the default set)"
import json, statistics as st, sys
rd = lambda p: [json.loads(l) for l in open(p) if l.strip()]
d = rd("work/v3/ans_rag_holdout_s1.jsonl"); s = rd("work/v3/strict/ans_rag_holdout_strict.jsonl")
empty = [i + 1 for i, r in enumerate(s) if not (r.get("text") or "").strip()]
td, ts = sum(bool(r.get("truncated")) for r in d), sum(bool(r.get("truncated")) for r in s)
print(f"default: {len(d)} answers, mean tokens {st.mean(r['tokens'] for r in d):.1f}, truncated {td}")
print(f"strict:  {len(s)} answers, mean tokens {st.mean(r['tokens'] for r in s):.1f}, truncated {ts}, empty {empty}")
sys.exit(1 if len(s) != 42 or empty or ts > td else 0)
PY

# 4. judging, GLM oQ8e alone, strict (tuned slot) against default (base slot), two passes, one retry for an unscored item
.venv/bin/python scripts/mk_h2h.py $W/ans_rag_holdout_s1.jsonl $S/ans_rag_holdout_strict.jsonl $J/h2h_$N.answers.jsonl $H
[ "$(rows $J/h2h_$N.answers.jsonl)" = "84" ] || fail "head-to-head answers cache does not have 84 rows"
hb "v3_strict_holdout_check: strict answers finished; $GLM8 loads on first request (judge)"
judge() { # PASS [extra flags]
  $G --stall-min 60 --log $L/v3_strict_holdout_judge.log -- .venv/bin/python eval/compare.py --model $B --adapter none \
    --questions $H --max-tokens 2048 --cache $J/h2h_$N.answers.jsonl --out $J/results_h2h_${N}_$1.jsonl \
    --judge-url $OMLX_URL/v1 --judge-model $GLM8 --judge-extra-body '{"max_tokens": 32768}' --judge-concurrency 3 \
    --judge-cache $J/judge_h2h_${N}_$1.cache.jsonl $2
}
for p in p1 p2; do
  flip=""; [ $p = p2 ] && flip="--judge-flip"
  judge $p "$flip" || fail "judging pass $p failed (see runs/v3/logs/v3_strict_holdout_judge.log)"
  if .venv/bin/python - $J/judge_h2h_${N}_$p.cache.jsonl <<'PY'
import json, sys
p = sys.argv[1]; rows = [json.loads(l) for l in open(p)]
bad = [r for r in rows if not r.get("j")]
if bad:
    open(p, "w").write("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows if r.get("j")))
print("unscored:", len(bad)); sys.exit(0 if bad else 1)
PY
  then judge $p "$flip --judge-temperature 0" || fail "judging retry $p failed (see runs/v3/logs/v3_strict_holdout_judge.log)"; fi
done
unload $GLM8
hb "v3_strict_holdout_check: judging finished, $GLM8 unloaded"

# 5. the mechanical check, CPU (each answer set against the packs it was written from, on the frozen index)
for a in "default $W/ans_rag_holdout_s1.jsonl" "strict $S/ans_rag_holdout_strict.jsonl"; do
  set -- $a
  .venv/bin/python scripts/invention_count.py --answers $2 --packs $W/packs_holdout_s1.jsonl --index $I > $S/invention_holdout_$1.txt \
    || fail "invention_count.py failed for $1"
  echo "invention count $1: $(grep -m1 'absent from pack' $S/invention_holdout_$1.txt)"
done

# 6. the summary
.venv/bin/python scripts/v3_strict_holdout_summary.py > runs/v3/STRICT_HOLDOUT_CHECK.md.tmp || fail "summary failed"
mv runs/v3/STRICT_HOLDOUT_CHECK.md.tmp runs/v3/STRICT_HOLDOUT_CHECK.md
RES=$(grep -m1 '^result:' runs/v3/STRICT_HOLDOUT_CHECK.md)
hb "v3_strict_holdout_check: done, $RES (runs/v3/STRICT_HOLDOUT_CHECK.md); the Owner decides the default mode after a reviewer has read the shortest strict answers" yes
status DONE yes "$RES; runs/v3/STRICT_HOLDOUT_CHECK.md; waits for the Owner's decision on the default mode"
echo V3_STRICT_HOLDOUT_CHECK_DONE >> $L/v3_strict_holdout_check.done
