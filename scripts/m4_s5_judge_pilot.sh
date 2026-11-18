#!/bin/sh
# M4 rubric pilot, judge step: the 30 blind rubrics against 80 answers in two passes plus 30 generic answers in pass 1 only
# (agents/m4-evaluation.md, "Rubric pilot"). Unattended; it takes the model server (the judge of the chosen settings alone), no local GPU.
# The judge settings are a required argument (there is no default, so that the full pilot is not started under a slow setting by mistake):
#   glm-think-12k  glm-think-0k  glm-nothink-12k  deepseek-nothink-12k   (see scripts/m4_s5a_judge_settings.sh and runs/m4/S5A_JUDGE_SETTINGS.md)
#   1. inputs check;
#   2. waits for an idle server, unloads the other models of ours (DeepSeek, GLM oQ4e, GLM oQ8e, whichever is not the judge); the judge
#      loads on its first request; the Qwen model of another client is never touched;
#   3. dry run with the stub judge: answers matched per set (6 sets of the pilot expected 10, 10, 10, 10, 20, 20; generic 30);
#   4. judging under scripts/run_guarded.py (stall limit 60 min), both passes on the 80 answers and pass 1 on the generic probe,
#      one more round for replies that were not valid JSON;
#   5. unloads the judge;
#   6. CPU: scripts/eval2_pilot_report.py -> runs/m4/S5_JUDGE_PILOT.md (the four go or no-go criteria; the Owner decides).
# Outputs: work/m4/pilot/judge_cache.jsonl (entries carry the settings name) and judge_results.jsonl. The cache resumes a stopped run.
# Markers: runs/m4/logs/m4_s5_judge_pilot.done or .failed. Run from the repository root:
#   scripts/m4_s5_judge_pilot.sh <settings-name> > runs/m4/logs/m4_s5_judge_pilot.out 2>&1 &
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
# settings: model, extra body of the request, characters of source passages
cfg() { case "$1" in
  glm-think-12k) MODEL=$GLM8; EB='{"max_tokens": 32768}'; SRC=12000;;
  glm-think-0k) MODEL=$GLM8; EB='{"max_tokens": 32768}'; SRC=0;;
  glm-nothink-12k) MODEL=$GLM8; EB='{"chat_template_kwargs": {"enable_thinking": false}, "max_tokens": 4096}'; SRC=12000;;
  deepseek-nothink-12k) MODEL=$DS; EB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'; SRC=12000;;
  *) return 1;; esac; }
SETTING=${1:-}
if [ -z "$SETTING" ] || ! cfg "$SETTING"; then
  echo "usage: scripts/m4_s5_judge_pilot.sh <settings-name>   (glm-think-12k | glm-think-0k | glm-nothink-12k | deepseek-nothink-12k)" >&2
  echo "no settings name given or unknown: nothing started." >&2
  exit 2
fi
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
export JUDGE_API_KEY=$OMLX_API_KEY
W=work/v3; P=work/m4/pilot; L=runs/m4/logs; N=m4_s5_judge_pilot
RUB=eval/m4/pilot/rubrics.jsonl; GEN=eval/m4/pilot/generic_answers.jsonl
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
QMAP="real=eval/questions_real.jsonl holdout=eval/questions_runbooks_holdout.jsonl"
ANS6="real_default=$W/ans_rag_real_s1.jsonl real_strict=$W/ans_rag_strict.jsonl real_v2_own_terms=$W/v2rag/ans_v2_ownexp.jsonl real_v2_base_excerpts=$W/v2rag/ans_v2_baseexp.jsonl holdout_default=$W/ans_rag_holdout_s1.jsonl holdout_strict=$W/strict/ans_rag_holdout_strict.jsonl"
ANS="$ANS6 generic=$GEN"
hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
fail() { hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed; echo "STOPPED: $1"; exit 1; }
busy() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/admin/api/models/$1/unload" >/dev/null; hb "$N: unloaded $1 (no active or waiting requests)"; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }
# the judge program, same arguments for the dry run (--stub) and the real run
judge() { .venv/bin/python scripts/eval2_judge.py run --rubrics $RUB --qmap $QMAP --chunks $W/chunks_s1.jsonl --source-chars $SRC \
  --answers $ANS --judge-model $MODEL --judge-extra-body "$EB" --settings-name $SETTING --concurrency 3 "$@"; }

rm -f $L/$N.failed $L/$N.done
mkdir -p $P $L runs/m4/reports

# 1. inputs
for f in $RUB $GEN eval/questions_real.jsonl eval/questions_runbooks_holdout.jsonl $W/chunks_s1.jsonl runs/m4/reports/wp2a_labels_checked.jsonl \
         $W/ans_rag_real_s1.jsonl $W/ans_rag_strict.jsonl $W/v2rag/ans_v2_ownexp.jsonl $W/v2rag/ans_v2_baseexp.jsonl \
         $W/ans_rag_holdout_s1.jsonl $W/strict/ans_rag_holdout_strict.jsonl; do
  [ -s $f ] || fail "input $f is missing or empty"
done
[ "$(rows $RUB)" = "30" ] || fail "$RUB has $(rows $RUB) rows, not 30"
[ "$(rows $GEN)" = "30" ] || fail "$GEN has $(rows $GEN) rows, not 30"
hb "$N: started (30 rubrics, six answer sets in two passes and a generic probe in pass 1; judge $MODEL, settings $SETTING)"

# 3. dry run first: no server needed, shows what will be judged
judge --stub --passes 1 2 > $L/$N.dry.out 2>&1 || fail "dry run failed (see $L/$N.dry.out)"
grep -E '^(matched|items|judge time)' $L/$N.dry.out
EXP="matched real_default: 10 answers matched real_strict: 10 answers matched real_v2_own_terms: 10 answers matched real_v2_base_excerpts: 10 answers matched holdout_default: 20 answers matched holdout_strict: 20 answers matched generic: 30 answers"
[ "$(grep '^matched' $L/$N.dry.out | tr '\n' ' ' | sed 's/ $//')" = "$EXP" ] || fail "answers matched to rubrics differ from the expected 10, 10, 10, 10, 20, 20 and 30 (see $L/$N.dry.out)"

# 2. the judge alone on the server
for m in $DS $GLM4 $GLM8; do [ "$m" = "$MODEL" ] || unload $m; done
hb "$N: other server models of ours unloaded if they were loaded; $MODEL loads on first request (judge)"

# 4. judging: the six sets in two passes, the generic probe in pass 1 only; a second round for replies that were not valid JSON
for round in 1 2; do
  extra=""; [ $round = 2 ] && extra="--retry-invalid"
  $G --stall-min 60 --log $L/$N.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUB --qmap $QMAP \
    --chunks $W/chunks_s1.jsonl --source-chars $SRC --answers $ANS6 --judge-model $MODEL \
    --judge-extra-body "$EB" --settings-name $SETTING --concurrency 3 --passes 1 2 --cache $P/judge_cache.jsonl --out $P/judge_results_sets.jsonl \
    --judge-url "$OMLX_URL/v1" $extra || fail "judging round $round (six sets) failed (see $L/$N.log)"
  $G --stall-min 60 --log $L/$N.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUB --qmap $QMAP \
    --chunks $W/chunks_s1.jsonl --source-chars $SRC --answers generic=$GEN --judge-model $MODEL \
    --judge-extra-body "$EB" --settings-name $SETTING --concurrency 3 --passes 1 --cache $P/judge_cache.jsonl --out $P/judge_results_generic.jsonl \
    --judge-url "$OMLX_URL/v1" $extra || fail "judging round $round (generic probe) failed (see $L/$N.log)"
  bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for f in sys.argv[1:] for l in open(f) if l.strip() and not json.loads(l)["valid_reply"]))' $P/judge_results_sets.jsonl $P/judge_results_generic.jsonl)
  echo "round $round: $bad unscored"
  [ "$bad" = "0" ] && break
done
[ "$(rows $P/judge_results_sets.jsonl)" = "160" ] || fail "$P/judge_results_sets.jsonl has $(rows $P/judge_results_sets.jsonl) rows, not 160 (80 answers x 2 passes)"
[ "$(rows $P/judge_results_generic.jsonl)" = "30" ] || fail "$P/judge_results_generic.jsonl has $(rows $P/judge_results_generic.jsonl) rows, not 30 (pass 1 only)"
cat $P/judge_results_sets.jsonl $P/judge_results_generic.jsonl > $P/judge_results.jsonl.tmp && mv $P/judge_results.jsonl.tmp $P/judge_results.jsonl
unload $MODEL
hb "$N: judging finished under settings $SETTING, $MODEL unloaded ($bad unscored judgments)"

# 6. the report, CPU
.venv/bin/python scripts/eval2_pilot_report.py --results $P/judge_results.jsonl > runs/m4/S5_JUDGE_PILOT.md.tmp || fail "pilot report failed"
mv runs/m4/S5_JUDGE_PILOT.md.tmp runs/m4/S5_JUDGE_PILOT.md
RES=$(grep -m1 'Go or no-go' runs/m4/S5_JUDGE_PILOT.md | sed 's/\*//g; s/ (criteria.*//')
hb "$N: done, $RES (runs/m4/S5_JUDGE_PILOT.md); the Owner decides whether the rubric method goes ahead" yes
echo "$RES"
echo M4_S5_JUDGE_PILOT_DONE >> $L/$N.done
