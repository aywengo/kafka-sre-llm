#!/bin/sh
# Can the v2 adapter be released as a retrieval-only model? (the Owner's go, 2026-11-17). Unattended; start it only when you
# accept that it takes the server and the local GPU after the two chains that are running or queued have finished.
# Phase 0 had the base model write the search terms; a downloadable merged model would write its own, which is unmeasured.
#   1. local GPU: v2 writes the search terms for the 40 real questions;
#   2. CPU: packs from them on the frozen stage-1 index;
#   3. local GPU: v2 answers from its own packs, and (to isolate the search terms) from the base model's packs;
#   4. judging, GLM oQ8e alone, head-to-head with base + retrieval, two passes each, one retry for an unscored item;
#   5. CPU: mechanical invention counts, objective recall of the two pack sets;
#   6. runs/v3/V2RAG_CHECK.md (scripts/v3_v2rag_summary.py).
# Waits for scripts/s1_ceiling_real.sh and scripts/v3_ship_checks.sh. Markers: runs/v3/logs/v3_v2rag_check.done or .failed.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
export JUDGE_API_KEY=$OMLX_API_KEY
I=work/v3/chunks_s1.jsonl; W=work/v3; J=$W/judge; R=$W/v2rag; Q=eval/questions_real.jsonl; L=runs/v3/logs
B=runs/v1/base27b; AD=adapters/kafka-sre-v2-resume
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "v3 v2 retrieval-only check: v2 with its own search terms against base + retrieval (frozen stage-1 index)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
fail() { hb "v3_v2rag_check: STOPPED: $1" yes; echo "$1" >> $L/v3_v2rag_check.failed; status STOPPED yes "v3_v2rag_check stopped: $1; marker runs/v3/logs/v3_v2rag_check.failed"; exit 1; }
busy() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/admin/api/models/$1/unload" >/dev/null; hb "v3_v2rag_check: unloaded $1 (no active or waiting requests)"; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }

rm -f $L/v3_v2rag_check.failed $L/v3_v2rag_check.done
mkdir -p $J $R runs/v3/reports

# 0. the two other chains own the server and the GPU until they have finished
until ! pgrep -f "scripts/s1_ceiling_real.sh" >/dev/null && ! pgrep -f "scripts/v3_ship_checks.sh" >/dev/null; do sleep 60; done
[ -d $AD ] || fail "adapter directory $AD does not exist"
for f in $I $Q $W/packs_real_s1.jsonl $W/ans_rag_real_s1.jsonl $W/phase0_facts.json $W/phase0_answerable.json; do
  [ -s $f ] || fail "input $f is missing or empty"
done
hb "v3_v2rag_check: started (v2 writes its own search terms and answers; compared with base + retrieval on the frozen index)"
status RUNNING no "scripts/v3_v2rag_check.sh; result runs/v3/V2RAG_CHECK.md; markers runs/v3/logs/v3_v2rag_check.done or .failed"
unload $DS
unload $GLM4
unload $GLM8
hb "v3_v2rag_check: server models unloaded if they were loaded; v2 loads on the local GPU (search terms)"

# 1. v2 writes the search terms (rag_answers.py resumes from its output file)
[ "$(rows $R/exp_v2.jsonl)" = "40" ] || {
  $G --stall-min 45 --log $L/v3_v2rag_expand.log -- .venv/bin/python scripts/rag_answers.py expand --model $B --adapter $AD \
    --questions $Q --out $R/exp_v2.jsonl || fail "v2 search terms failed (see runs/v3/logs/v3_v2rag_expand.log)"; }
[ "$(rows $R/exp_v2.jsonl)" = "40" ] || fail "v2 wrote $(rows $R/exp_v2.jsonl) search-term rows, not 40"

# 2. packs on the frozen index, CPU
[ "$(rows $R/packs_v2exp.jsonl)" = "40" ] || \
  HF_HUB_OFFLINE=1 .venv/bin/python scripts/build_packs_v3.py pack --expansions $R/exp_v2.jsonl --index $I --out $R/packs_v2exp.jsonl \
    || fail "packs from v2's search terms failed"
[ "$(rows $R/packs_v2exp.jsonl)" = "40" ] || fail "packs_v2exp.jsonl has $(rows $R/packs_v2exp.jsonl) rows, not 40"

# 3. v2 answers: from its own packs, and from the base model's packs
answer() { # PACKS OUT LOG
  [ "$(rows $2)" = "40" ] && return 0
  $G --stall-min 45 --log $L/$3.log -- .venv/bin/python scripts/rag_answers.py answer --model $B --adapter $AD \
    --questions $Q --packs $1 --k k6 --index $I --out $2 || fail "v2 answers $2 failed (see runs/v3/logs/$3.log)"
  [ "$(rows $2)" = "40" ] || fail "$2 has $(rows $2) answers, not 40"
}
answer $R/packs_v2exp.jsonl $R/ans_v2_ownexp.jsonl v3_v2rag_ans_own
answer $W/packs_real_s1.jsonl $R/ans_v2_baseexp.jsonl v3_v2rag_ans_base

# 4. judging, GLM oQ8e alone, two passes per contestant, one retry for an unscored item
hb "v3_v2rag_check: local generation finished; $GLM8 loads on first request (judge)"
.venv/bin/python scripts/mk_h2h.py $W/ans_rag_real_s1.jsonl $R/ans_v2_ownexp.jsonl $J/h2h_v2ownexp_s1.answers.jsonl $Q
.venv/bin/python scripts/mk_h2h.py $W/ans_rag_real_s1.jsonl $R/ans_v2_baseexp.jsonl $J/h2h_v2baseexp_s1.answers.jsonl $Q
for name in v2ownexp_s1 v2baseexp_s1; do
  [ "$(rows $J/h2h_$name.answers.jsonl)" = "80" ] || fail "head-to-head answers cache for $name does not have 80 rows"
done
judge() { # NAME PASS [extra flags]
  $G --stall-min 60 --log $L/v3_v2rag_judge.log -- .venv/bin/python eval/compare.py --model $B --adapter none \
    --questions $Q --max-tokens 2048 --cache $J/h2h_$1.answers.jsonl --out $J/results_h2h_$1_$2.jsonl \
    --judge-url $OMLX_URL/v1 --judge-model $GLM8 --judge-extra-body '{"max_tokens": 32768}' --judge-concurrency 3 \
    --judge-cache $J/judge_h2h_$1_$2.cache.jsonl $3
}
for name in v2ownexp_s1 v2baseexp_s1; do
  for p in p1 p2; do
    flip=""; [ $p = p2 ] && flip="--judge-flip"
    judge $name $p "$flip" || fail "judging $name $p failed (see runs/v3/logs/v3_v2rag_judge.log)"
    if .venv/bin/python - $J/judge_h2h_${name}_$p.cache.jsonl <<'PY'
import json, sys
p = sys.argv[1]; rows = [json.loads(l) for l in open(p)]
bad = [r for r in rows if not r.get("j")]
if bad:
    open(p, "w").write("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows if r.get("j")))
print("unscored:", len(bad)); sys.exit(0 if bad else 1)
PY
    then judge $name $p "$flip --judge-temperature 0" || fail "judging retry $name $p failed (see runs/v3/logs/v3_v2rag_judge.log)"; fi
  done
done
unload $GLM8
hb "v3_v2rag_check: judging finished, $GLM8 unloaded"

# 5. the mechanical check, CPU (each answer file against the packs it was written from, on the frozen index)
for a in "ragbase $W/ans_rag_real_s1.jsonl $W/packs_real_s1.jsonl" "v2ownexp $R/ans_v2_ownexp.jsonl $R/packs_v2exp.jsonl" \
         "v2baseexp $R/ans_v2_baseexp.jsonl $W/packs_real_s1.jsonl"; do
  set -- $a
  .venv/bin/python scripts/invention_count.py --answers $2 --packs $3 --index $I > $R/invention_real_$1.txt \
    || fail "invention_count.py failed for $1"
done

# 6. the summary (recall of the two pack sets is computed there)
.venv/bin/python scripts/v3_v2rag_summary.py > runs/v3/V2RAG_CHECK.md.tmp || fail "summary failed"
mv runs/v3/V2RAG_CHECK.md.tmp runs/v3/V2RAG_CHECK.md
RES=$(grep -m1 '^result:' runs/v3/V2RAG_CHECK.md)
hb "v3_v2rag_check: done, $RES (runs/v3/V2RAG_CHECK.md); a reviewer still has to read 10 answers for invented identifiers" yes
status DONE yes "$RES; runs/v3/V2RAG_CHECK.md"
echo V3_V2RAG_CHECK_DONE >> $L/v3_v2rag_check.done
