#!/bin/sh
# M4 S5a: comparison of judge settings before the full rubric pilot. 16 answers (the subset with most of the reader's labelled
# errors and omissions), pass 1 only, judged under each setting one after another, 3 in parallel
# (agents/m4-evaluation.md, "Rubric pilot"; runs/m4/reports/s5_judge_step.md). Unattended; it takes the model server, no local GPU.
#   settings, in this order:
#     glm-think-12k         GLM oQ8e, thinking on, 12000 characters of sources (the pilot's setting; cached entries are reused)
#     glm-think-0k          same, no source passages
#     glm-nothink-12k       same model, thinking off through chat_template_kwargs; the first reply is checked: if thinking is
#                           still on the setting is recorded as "switch had no effect" and the other 15 calls are not made
#     deepseek-nothink-12k  DeepSeek, thinking_mode chat; the GLM model is unloaded before its first request
#   1. inputs check;  2. dry run with the stub judge (16 answers matched under every setting);
#   3. waits for an idle server, unloads our other models (never the Qwen model of another client);
#   4. one guarded judging run per setting (stall limit 45 min);  5. unloads the model that is loaded last;
#   6. CPU: scripts/eval2_judge_settings_report.py -> runs/m4/S5A_JUDGE_SETTINGS.md (numbers only; the conductor decides).
# Before every unload the server must have no active or waiting request. Outputs are new files under work/m4/pilot/judge_settings/;
# its cache starts as a copy of work/m4/pilot/judge_cache.jsonl (the four or five entries of the stopped pilot count as glm-think-12k).
# Markers: runs/m4/logs/m4_s5a_judge_settings.done or .failed.
#   scripts/m4_s5a_judge_settings.sh > runs/m4/logs/m4_s5a_judge_settings.out 2>&1 &
#   scripts/m4_s5a_judge_settings.sh --dry-run     # stub judge, no server, no markers: matches, reuse, report from stub data
set -e
DRY=no; [ "${1:-}" = "--dry-run" ] && DRY=yes
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
  export JUDGE_API_KEY=$OMLX_API_KEY
fi
W=work/v3; P=work/m4/pilot; S=$P/judge_settings; L=runs/m4/logs; N=m4_s5a_judge_settings
RUB=eval/m4/pilot/rubrics.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
QMAP="real=eval/questions_real.jsonl holdout=eval/questions_runbooks_holdout.jsonl"
ANS="real_default=$W/ans_rag_real_s1.jsonl real_strict=$W/ans_rag_strict.jsonl real_v2_own_terms=$W/v2rag/ans_v2_ownexp.jsonl real_v2_base_excerpts=$W/v2rag/ans_v2_baseexp.jsonl holdout_default=$W/ans_rag_holdout_s1.jsonl holdout_strict=$W/strict/ans_rag_holdout_strict.jsonl"
ONLY="real_default:real-1 real_default:real-3 real_default:real-5 real_default:real-31 real_default:real-39 real_strict:real-3 real_strict:real-36 real_v2_own_terms:real-31 real_v2_own_terms:real-36 real_v2_own_terms:real-39 real_v2_base_excerpts:real-2 real_v2_base_excerpts:real-39 holdout_default:holdout-13 holdout_default:holdout-15 holdout_default:holdout-34 holdout_strict:holdout-3"
FIRST="real_default:real-1"
SETTINGS="glm-think-12k glm-think-0k glm-nothink-12k deepseek-nothink-12k"
cfg() { case "$1" in
  glm-think-12k) MODEL=$GLM8; EB='{"max_tokens": 32768}'; SRC=12000;;
  glm-think-0k) MODEL=$GLM8; EB='{"max_tokens": 32768}'; SRC=0;;
  glm-nothink-12k) MODEL=$GLM8; EB='{"chat_template_kwargs": {"enable_thinking": false}, "max_tokens": 4096}'; SRC=12000;;
  deepseek-nothink-12k) MODEL=$DS; EB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'; SRC=12000;;
  *) echo "unknown settings name $1" >&2; exit 2;; esac; }
hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M4 S5a judge settings comparison (16 answers, pass 1)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
fail() { hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed; status STOPPED yes "$N stopped: $1; marker $L/$N.failed"; echo "STOPPED: $1"; exit 1; }
busy() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/api/status" \
  | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$OMLX_URL/admin/api/models/$1/unload" >/dev/null; hb "$N: unloaded $1 (no active or waiting requests)"; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }
# the judge program for one setting; $1 = setting name, the rest = further arguments
judge() { name=$1; shift; cfg $name
  .venv/bin/python scripts/eval2_judge.py run --rubrics $RUB --qmap $QMAP --chunks $W/chunks_s1.jsonl --source-chars $SRC \
    --answers $ANS --judge-model $MODEL --judge-extra-body "$EB" --settings-name $name --passes 1 --concurrency 3 "$@"; }
# one guarded run of one setting, on the whole subset or on the answers given in $2
guarded() { name=$1; only=$2; cfg $name
  $G --stall-min 45 --log $L/$N.$name.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUB --qmap $QMAP \
    --chunks $W/chunks_s1.jsonl --source-chars $SRC --answers $ANS --judge-model $MODEL --judge-extra-body "$EB" \
    --settings-name $name --passes 1 --concurrency 3 --only $only --cache $SC --out $S/$name.jsonl --judge-url "$OMLX_URL/v1"; }
report() { # OUTFILE, results dir, extra arguments
  out=$1; dir=$2; shift 2
  args=""; for n in $SETTINGS; do args="$args $n=$dir/$n.jsonl"; done
  .venv/bin/python scripts/eval2_judge_settings_report.py report --results $args "$@" > $out.tmp && mv $out.tmp $out; }

# 1. inputs
for f in $RUB eval/questions_real.jsonl eval/questions_runbooks_holdout.jsonl $W/chunks_s1.jsonl runs/m4/reports/wp2a_labels_checked.jsonl \
         $W/ans_rag_real_s1.jsonl $W/ans_rag_strict.jsonl $W/v2rag/ans_v2_ownexp.jsonl $W/v2rag/ans_v2_baseexp.jsonl \
         $W/ans_rag_holdout_s1.jsonl $W/strict/ans_rag_holdout_strict.jsonl; do
  [ -s $f ] || { echo "input $f is missing or empty"; [ $DRY = yes ] && exit 1; fail "input $f is missing or empty"; }
done
mkdir -p $S $L runs/m4/reports

# 2. dry run: stub judge, no server; the cache is read, never written
if [ $DRY = yes ]; then D=$P/judge_settings_dry; mkdir -p $D; else D=$S/dry; mkdir -p $D; rm -f $L/$N.failed $L/$N.done
  hb "$N: started (16 answers, pass 1, settings: $SETTINGS)"; status RUNNING no "$N running: 16 answers under four judge settings; markers $L/$N.done / .failed"; fi
for name in $SETTINGS; do
  judge $name --stub --only $ONLY --cache $P/judge_cache.jsonl --out $D/$name.jsonl > $D/$name.dry.out 2>&1 \
    || { cat $D/$name.dry.out; [ $DRY = yes ] && exit 1; fail "dry run of $name failed (see $D/$name.dry.out)"; }
  echo "$name: $(grep '^settings' $D/$name.dry.out | sed 's/^settings [^:]*: //'); matched $(grep '^matched' $D/$name.dry.out | sed 's/matched [a-z0-9_]*: \([0-9]*\) answers/\1/' | paste -sd+ - | bc) answers"
  [ "$(grep '^matched' $D/$name.dry.out | sed 's/matched [a-z0-9_]*: \([0-9]*\) answers/\1/' | paste -sd+ - | bc)" = "16" ] \
    || { [ $DRY = yes ] && exit 1; fail "$name: not 16 answers matched (see $D/$name.dry.out)"; }
done
if [ $DRY = yes ]; then
  report $D/report_from_stub_data.md $D
  echo "dry run done: report from stub data in $D/report_from_stub_data.md (the real report goes to runs/m4/S5A_JUDGE_SETTINGS.md)"
  exit 0
fi

# 3. the server: idle, then the GLM judge alone (our other models unloaded; Qwen of another client stays)
SC=$S/judge_cache.jsonl
[ -f $SC ] || { [ -f $P/judge_cache.jsonl ] && cp $P/judge_cache.jsonl $SC || : > $SC; }
unload $DS
unload $GLM4
hb "$N: DeepSeek and GLM oQ4e unloaded if they were loaded; $GLM8 loads on first request"

# 4. one setting after another
NOEFFECT=""
for name in $SETTINGS; do
  cfg $name
  if [ "$MODEL" = "$DS" ]; then
    unload $GLM8   # the two do not fit together
    hb "$N: $GLM8 unloaded; $DS loads on first request"
  fi
  case $name in
  *nothink*)
    guarded $name "$FIRST" || fail "$name: probe call failed (see $L/$N.$name.log)"
    rc=0; .venv/bin/python scripts/eval2_judge_settings_report.py check-switch --cache $SC --setting $name || rc=$?
    if [ $rc = 1 ]; then
      NOEFFECT="$NOEFFECT $name"; hb "$N: $name: switch had no effect (first reply still shows thinking); the other 15 calls are not made"
      rm -f $S/$name.jsonl; continue
    fi
    [ $rc = 0 ] || hb "$N: $name: could not tell from the first reply whether thinking is off; going on"
    ;;
  esac
  guarded $name "$ONLY" || fail "$name: judging failed (see $L/$N.$name.log)"
  [ "$(rows $S/$name.jsonl)" = "16" ] || fail "$S/$name.jsonl has $(rows $S/$name.jsonl) rows, not 16"
  hb "$N: $name done (16 judgments)"
done
unload $MODEL
hb "$N: judging finished, $MODEL unloaded"

# 6. the report, CPU
report runs/m4/S5A_JUDGE_SETTINGS.md $S --noeffect $NOEFFECT || fail "report failed"
hb "$N: done (runs/m4/S5A_JUDGE_SETTINGS.md); the conductor decides the setting for the full pilot" yes
status DONE yes "$N done: runs/m4/S5A_JUDGE_SETTINGS.md; the conductor decides the judge setting"
echo M4_S5A_JUDGE_SETTINGS_DONE >> $L/$N.done
