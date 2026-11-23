#!/bin/sh
# M7 pilot chain (tasks K3, P2 and P3 of agents/m7-plan.md; the pilot of runs/m7/reports/k1_check_design.md section 5): the thinking pilot, the checking pass on stored drafts, and the scoring by
# code. Built and rehearsed (dry run, stub model) now; the real run is a separate task. Nothing here starts a model by itself: the model calls are server calls (one request at a time) under
# scripts/run_guarded.py. Everything is on worn development data (B1, B2, pilot2, T); D5, the hidden part of the M4 evaluation and the legacy sets are never read.
#
# Order (fix 1, 2026-11-23): stage V (stored drafts) first, then stage T (thinking, --think-cap N, default 1500), then the check on the thinking drafts, then stage S. A stage T with no usable
# thinking draft is a result (written in the report), not a failure: only the check on the thinking drafts is skipped. The numbers below are the steps, not the order.
# Steps (each resumable by its output files; a stage marker W/stage_NAME.done records the manifest hash and the sha256 of every output; a line per item):
#   1. Gate (CPU, refusal exit 2, nothing written before it passes): every stored answer file present, complete (row counts), every row with its pack of chunk ids and a text; the same ids
#      in both arms of a set and in the items file; the readers' findings files as the design counts them (B1 90 non-minor rows of reader r1 with the 14 catchable rows checked by set, id and
#      quote; pilot2 111; T 28 harmful statements); the sha256 of every input into the manifest; kafka_sre_rag verify; no state of an earlier run (a work folder without a manifest, a report that
#      exists before its header); the start check of a stored manifest and markers (a changed input or code file stops the chain); in real runs the server checks of scripts/m6_t_answers.sh
#      (the 4-bit model listed, no other chain or guarded job, no active or waiting request, no model of another client loaded).
#   2. Report header runs/m7/M7_PILOT.md: what the pilot is, the five bars and the rule for the variant, written once before any model call.
#   3. Select: the 20 scenarios of stage T (the first 10 ids of B1 and of B2 by sha256 of the id), their items and their stored packs (CPU).
#   4. Stage T, thinking: eval2.py answer --answer-profile thorough --no-verify (thinking on, no check) on the stored packs of the default answers (--replay-packs: no search call), with the
#      answer limit plus 1,500 tokens as the total limit; the measures (reasoning present, thinking tokens, empty, cut, cut while thinking, seconds per part).
#   5. The checking pass V1 (no cards) on the thinking drafts (drafts cut while thinking are left out): mechanical measures only.
#   6. Stage V: python -m kafka_sre_rag.verify_pass run, V1 on B1, pilot2, T and B2 (default and strict, T: default and g2) and V1C (cards) on B1, pilot2 and T: one output file per set, arm
#      and variant, resumable by id.
#   7. The 4-bit model is unloaded when this chain loaded it (it was not loaded when the chain first started).
#   8. Stage S (CPU): scripts/m7_pilot_score.py score: catches, accepted changes on lines no finding names (the files for the reader, one per variant), new destructive matches, valid
#      JSON, cut replies, added seconds, and the five bars with PASS, FAIL or "needs the reading". The code never prints a final choice.
#   9. The report body and the done marker; the last line of the log and of the report is "M7 PILOT DONE: <n> thinking drafts, <n> checks".
# The start check runs before every stage, so that a change between stages stops the chain; the pinned code is everything the answer and check paths import (scripts/m7_pilot_score.py code-pins),
# the inputs are pinned by sha256. The answer calls pass --allow-commit-change: an unrelated commit during the run does not stop it (the pins carry the protection). A stopped run is resumed by
# running the script again: the failed marker is removed and the stage markers decide what is done. Only a state the start check cannot accept (changed pins or inputs, another manifest) is
# refused, and then the refusal prints the mv lines.
#
#   mkdir -p runs/m7/logs && scripts/m7_pilot.sh > runs/m7/logs/m7_pilot.out 2>&1 &          (real)
#   scripts/m7_pilot.sh --dry-run [--work DIR]                                                (stub model, BM25-only retrieval is never used (packs are replayed), no server; nothing is
#                                                                                              written outside DIR; the numbers are not measurements)
# Options: --dry-run, --work DIR, --report FILE, --think-cap N (tokens added to the answer limit for stage T; recorded in the manifest and the report), and for dry runs --spec FILE, --plant-cut-think SUBSTR and --plant-check-fail SUBSTR (the stub fails on questions containing SUBSTR) (a set table instead of the real one; see scripts/m7_pilot_score.py).
# Markers: runs/m7/logs/m7_pilot.done or .failed (a dry run: in DIR/logs). The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/.
#
# Estimated duration of the real run (inferred, not measured: from the budget of agents/m7-plan.md section 3 and the check design, see "planned:" below): stage T about 60 to 125 s a draft
# (11 s prompt, thinking at most 47 s, answer at most 64 s), the check 14 to 33 s a call (14 s prompt of about 7k tokens, at most 19 s for 600 tokens), loads 0.3 to 0.5 h.
set -e
DRY=no; WSET=no; W=work/m7/pilot; REPORT=runs/m7/M7_PILOT.md; RSET=no; SPEC=""; CAP=1500; PLANT_CUT=""; PLANT_CHK=""
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --spec) SPEC=${2:?}; shift;;
  --think-cap) CAP=${2:?}; shift;;
  --plant-cut-think) PLANT_CUT=${2:?}; shift;; --plant-check-fail) PLANT_CHK=${2:?}; shift;;
  *) echo "usage: scripts/m7_pilot.sh [--dry-run] [--work DIR] [--report FILE] [--spec FILE (dry runs)] [--think-cap N] (see the header)" >&2; exit 2;; esac
  shift; done
case "$CAP" in ''|*[!0-9]*) echo "REFUSED: --think-cap takes a whole number of tokens; nothing started" >&2; exit 2;; esac
[ "$CAP" -gt 0 ] || { echo "REFUSED: --think-cap must be above 0; nothing started" >&2; exit 2; }
[ -z "$PLANT_CUT$PLANT_CHK" ] || [ $DRY = yes ] || { echo "REFUSED: --plant-* options are for dry runs only; nothing started" >&2; exit 2; }
# dry runs only: the stub model fails on request (comma-separated substrings of a question), read by scripts/eval2.py's stub
[ -z "$PLANT_CUT" ] || export M7_STUB_CUT_WHILE_THINKING="$PLANT_CUT"
[ -z "$PLANT_CHK" ] || export M7_STUB_CHECK_FAIL="$PLANT_CHK"
R=scripts/m7_pilot_score.py; GR=scripts/m5_g_rule.py; N=m7_pilot; L=runs/m7/logs
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
if [ -n "$SPEC" ]; then SPECARG="--spec $SPEC"; else SPECARG=""; fi
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m7_pilot.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/M7_PILOT_from_stub_data.md
  case "$W" in work/m7/pilot|work/m7/pilot/|*/work/m7/pilot|*/work/m7/pilot/) refuse "a dry run never uses the real work folder";; esac
  case "$REPORT" in runs/m7/M7_PILOT.md|*/runs/m7/M7_PILOT.md) refuse "a dry run never uses the real report";; esac
  G="scripts/run_guarded.py --other-retries 0"
else
  [ -z "$SPEC" ] || refuse "--spec is for dry runs only (the real run reads the stored files of the design)"
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
fi
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M7 pilot: thinking pilot, the checking pass on stored drafts, scoring by code",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
STOPPED_SUFFIX=stopped_$(date +%Y%m%d)
moveaside() { # the lines that move the state of a stopped run out of the way (printed, never executed here)
  echo "to start again, first move the stopped run's files aside (the chain never moves or deletes them):" >&2
  for f in $W $REPORT $L/$N.failed; do [ -e $f ] && echo "  mv $f $f.$STOPPED_SUFFIX" >&2; done
  echo "  (and runs/m7/logs/$N.out, if it holds the stopped run's output, before the next start redirects into it)" >&2; return 0; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; mkdir -p $L; echo "$1" >> $L/$N.failed; exit 1; fi
  hb "$N: STOPPED: $1" yes; mkdir -p $L; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; the 4-bit model may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- 1. the gate (CPU; nothing is written before the checks pass)
for f in $CHUNKS $FROZEN $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json kafka_sre_rag/prompts.py kafka_sre_rag/verify_pass.py kafka_sre_rag/kb_lookup.py kafka_sre_rag/checker.py \
         kafka_sre_rag/revise.py kafka_sre_rag/evalhooks.py scripts/eval2.py scripts/eval2_compare.py scripts/run_guarded.py scripts/m5_g_rule.py $R scripts/m7_pilot.sh; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
.venv/bin/python $R gate-files $SPECARG || refuse "the stored answer and findings files are not what the design names (see above)"
# no state of an earlier run that the start check cannot accept: a work folder that has no manifest, a report that exists before its header was written
if [ ! -e $W/MANIFEST.json ] && [ -d $W ] && [ -n "$(ls -A $W 2>/dev/null)" ]; then moveaside; refuse "the work folder $W is not empty and has no MANIFEST.json: the state of an earlier run"; fi
if [ ! -e $W/stage_header.done ] && [ -e $REPORT ]; then moveaside; refuse "$REPORT exists before the header of this run was written (it is not overwritten)"; fi
VOUT=$(mktemp "${TMPDIR:-/tmp}/m7_pilot_verify.XXXXXX")     # nothing is written into the repository before the gate has passed
.venv/bin/python -m kafka_sre_rag verify > $VOUT 2>&1 || { cat $VOUT >&2; rm -f $VOUT; refuse "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"; }
grep -E '^prompt ' $VOUT | head -8; rm -f $VOUT

# the server (real runs): the helpers of scripts/m6_t_answers.sh
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
fi
api() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
loaded_list() { api /api/status | python3 -c 'import sys,json; print(" ".join(json.load(sys.stdin).get("loaded_models") or []))' 2>/dev/null || echo "status-unreadable"; }
unload_now() { # any model; only when no request is active or waiting (the Owner's decision of 2026-11-22); the server's answer is checked and logged
  idle
  uresp=$(curl -s -m 300 -w '\n%{http_code}' -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" 2>/dev/null) || uresp=""
  ucode=$(printf '%s' "$uresp" | tail -n 1); ubody=$(printf '%s' "$uresp" | sed '$d' | tr '\n' ' ' | cut -c1-160 | sed 's/ *$//')
  case $ucode in [0-9][0-9][0-9]) ;; *) ucode=000;; esac
  case $ucode in
    2??) hb "$N: unloaded $1 (no active or waiting requests; the server answered $ucode: ${ubody:-no body})";;
    *) hb "$N: unload of $1 FAILED (the server answered $ucode: ${ubody:-no body})" yes; fail "the unload of $1 was not accepted (the server answered $ucode)";;
  esac; }
unload() { [ "$(loaded $1)" = "yes" ] || return 0; unload_now $1; }
clear_server() { # before the first request: no active or waiting request; every loaded model but the 4-bit model is unloaded (each unload checked and logged)
  idle
  for m in $(loaded_list); do
    [ "$m" = "$Q4" ] && continue
    [ "$m" = "status-unreadable" ] && fail "the server status is unreadable"
    unload_now $m
  done
  left=$(for m in $(loaded_list); do [ "$m" = "$Q4" ] || echo $m; done)
  [ -z "$left" ] || fail "models are still loaded after the unloads: $left"; }
others() { pgrep -fl 'scripts/(m4|m5|m6|m7|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }
wait_others() { # before anything loads a model: no other chain and no guarded job (real runs)
  [ $DRY = yes ] && return 0
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, m7_*.sh, s1_*.sh) and no guarded job runs"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done; }
if [ $DRY = yes ]; then
  Q4=Qwen3.8-27B-4bit
else
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m7_pilot_models.XXXXXX")
  api /v1/models > $MJSON 2>/dev/null || true
  Q4=${M7_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $MJSON)} || { rm -f $MJSON; refuse "the 4-bit model is not listed by the server (or ambiguous)"; }
  rm -f $MJSON
fi
if [ $DRY = yes ]; then DRYFLAG="--dry"; else DRYFLAG=""; fi

# the gate has passed: from here on the work folder and the log folder are written
mkdir -p $W $L
# the manifest and the start check: the stored inputs, the catalogues, the index and the code are pinned by sha256
PINS="--pin $CHUNKS --pin $CATS/CATALOGUES.json --pin runs/v3/INDEX_FROZEN.json"
for f in $(.venv/bin/python $R input-files $SPECARG); do PINS="$PINS --pin $f"; done
for f in $(.venv/bin/python $R code-pins) $FROZEN; do     # the code the answer and check paths import (the tests trace the imports)
  [ -f $f ] || refuse "pinned code file $f is missing"
  PINS="$PINS --pin $f"; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)   # recorded for the reader, never compared
gstate() { # SUBCOMMAND: the manifest and the start check take the same state
  .venv/bin/python $R "$1" --manifest $W/MANIFEST.json --work $W $PINS --frozen $FROZEN $DRYFLAG --opt "model=$Q4" --opt "pilot=m7-think-and-check" --opt "think_cap=$CAP" --info "git_head=$GIT_HEAD"; }
gstate start-check || { moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; }
gstate manifest
# a rerun after a stop: the failed marker is removed (its text is shown); the start check above and the stage markers decide what is done, so finished stages are kept
if [ -f $L/$N.failed ]; then echo "rerun after a stop; the earlier failure was: $(tail -n 1 $L/$N.failed)"; rm -f $L/$N.failed; fi
mkdir -p "$(dirname $REPORT)"
recheck() { # NAME: the start check again before a stage, so that a change of a pinned file, an option or an output between stages stops the chain
  gstate start-check > $W/recheck.out 2>&1 || { cat $W/recheck.out >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started (see above)"; }; }
# a finished run: nothing is redone and nothing changes
if [ -f $L/$N.done ]; then echo "already done: $(tail -n 1 $L/$N.done)"; exit 0; fi

# ---------------------------------------------------------------- the plan and the estimate
RUNS=$W/runs.tsv
.venv/bin/python $R runs $SPECARG --work $W > $RUNS.tmp && mv $RUNS.tmp $RUNS
NCHK=$(awk -F'\t' '$5 ~ /^[0-9]+$/ {s+=$5} END {print s+0}' $RUNS)
NTHINK=$(.venv/bin/python $R select-think $SPECARG --out-dir $W/think_plan | grep -o ' for the thinking stage' >/dev/null; grep -c . $W/think_plan/items.jsonl | tr -d ' '); rm -rf $W/think_plan
# the measured rate of the real run of 2026-11-23: a thinking call at about 31 tokens/s, so stage T's worst case is NTHINK x (cap + 2,048 answer limit) / 31 s
est() { awk -v t=$NTHINK -v c=$NCHK -v cap=$CAP 'BEGIN{ n=c+t; lo=t*60 + n*14 + 1100; hi=t*(cap+2048)/31 + n*33 + 1800; if (hi < lo) hi = lo; printf "%.1f to %.1f", lo/3600, hi/3600 }'; }
tworst() { awk -v t=$NTHINK -v cap=$CAP 'BEGIN{ printf "%.0f", t*(cap+2048)/31 }'; }
echo "planned: stage V first ($NCHK checking calls on stored drafts), then $NTHINK thinking drafts (cap $CAP tokens), then up to $NTHINK checking calls on the thinking drafts: $(est) h of model-server time"
echo "estimate basis: stage T worst case $NTHINK x ($CAP + 2,048) / 31 tokens per second = $(tworst) s (31 tokens/s measured in the real run of 2026-11-23, a thinking call using the whole total limit); a check 14 to 33 s, loads 0.3 to 0.5 h (budget of agents/m7-plan.md section 3); not measured on these prompts"

stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $GR mark --work $W --stage $st --out "$@" > /dev/null; }
SERVER_NEEDED=no
for st in think thinkcheck verify; do stage_done $st || SERVER_NEEDED=yes; done

# ---------------------------------------------------------------- 2. report header (before any model call)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $W/think/ans_think.jsonl; do [ ! -e $f ] || refuse "$f exists but the report header was not written yet; the header must come first"; done
  {
    echo "# M7 pilot: thinking, and the checking pass on stored drafts$([ $DRY = yes ] && echo ' (DRY RUN: stub model; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/$N.sh\` on $(date +%Y-%m-%d), 4-bit model \`$Q4\`. Helpers: \`$R\`. Stored inputs (sha256, first 16 characters):"
    echo
    for f in $(.venv/bin/python $R input-files $SPECARG); do echo "- \`$f\` $(.venv/bin/python -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest()[:16])' $f)"; done
    echo
    .venv/bin/python $R header-text
    echo "Thinking cap of stage T: $CAP tokens added to the answer limit of 2,048 (\`--think-cap\`; default 1500); stage order: V (stored drafts), T (thinking), S (scoring)."
    echo
    echo "This header was written before any model call of the run (stage marker \`$W/stage_header.done\`)."
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header written ($REPORT); no model call yet"
fi

# ---------------------------------------------------------------- 3. select (CPU): the 20 scenarios of stage T and their stored packs
if ! stage_done select; then
  .venv/bin/python $R select-think $SPECARG --out-dir $W/think || fail "the selection of the thinking scenarios failed"
  mark select $W/think/items.jsonl $W/think/replay_packs.jsonl $W/think/ids.json
fi
NT=$(grep -c . $W/think/items.jsonl | tr -d ' ')

# ---------------------------------------------------------------- the server, once
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, m7_*.sh, s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  wait_others
  hb "$N: started; 4-bit model $Q4: $NT thinking drafts on stored packs, then the checking pass (V1, V1C) on stored drafts"
  status RUNNING no "$N running: thinking drafts, checking passes, scoring; markers $L/$N.done / .failed"
  clear_server
  # whether the 4-bit model was loaded before this chain first started decides whether this chain unloads it at the end (kept across a restart)
  [ -f $W/q4_loaded_at_start ] || loaded $Q4 > $W/q4_loaded_at_start
  hb "$N: the server is idle and no model but $Q4 is loaded; the 4-bit model was loaded at the start: $(cat $W/q4_loaded_at_start)"
fi

if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; VGA="--dry-run"; EBV=""; else GA="--backend openai --model $Q4"; VGA="--backend openai --model $Q4"; EBV=$EB; fi   # the extra body is passed quoted on its own (a JSON value with spaces must not be word-split)

# ---------------------------------------------------------------- the checking runs (one output file per set, arm and variant; resumable by id)
verify_run() { # NAME ANSWERS ITEMS OUT EXPECT CARDS
  name=$1; ans=$2; items=$3; out=$4; expect=$5; cards=$6
  if .venv/bin/python $R check-verify --file $out --expect $expect > $W/check.out 2>&1; then echo "skip check $name: $(cat $W/check.out)"; return 0; fi
  [ $cards = yes ] && CF="--cards" || CF=""
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    hb "$N: checking pass $name started ($expect answers)"
    $G --stall-min 20 --log $L/$N.$name.log -- .venv/bin/python -m kafka_sre_rag.verify_pass run --answers $ans --items $items --out $out $CF --catalogues $CATS $VGA ${EBV:+--extra-body} ${EBV:+"$EBV"} </dev/null \
      || fail "checking pass $name: failed (see $L/$N.$name.log)"
    .venv/bin/python $R check-verify --file $out --expect $expect && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "checking pass $name: incomplete, with failed rows, or too many rows without a usable reply from the server after $tries passes ($out)"
  done
}
TAB=$(printf '\t')

# ---------------------------------------------------------------- 6. stage V: the checking pass on the stored drafts
if ! stage_done verify; then
  recheck verify
  mkdir -p $W/verify
  OUTS=""
  while IFS=$TAB read -r name ans items out expect cards; do
    [ "$name" = "THINK_think_V1" ] && continue
    verify_run $name $ans $items $out $expect $cards
    OUTS="$OUTS $out"
  done < $RUNS
  mark verify $OUTS
  hb "$N: the checking passes on the stored drafts are done"
fi
# ---------------------------------------------------------------- 4. stage T: thinking on the stored packs (no check)
if ! stage_done think; then
  recheck think
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    hb "$N: thinking drafts started ($NT scenarios, stored packs, thinking on, no check)"
    $G --stall-min 40 --log $L/$N.think.log -- .venv/bin/python scripts/eval2.py answer --items $W/think/items.jsonl --out $W/think/ans_think.jsonl --config m7-pilot-think $GA \
      --extra-body "$EB" --mode default --replay-packs $W/think/replay_packs.jsonl --catalogue $CATS --host-class "Mac Studio" --allow-commit-change \
      --answer-profile thorough --no-verify --think-cap $CAP </dev/null || fail "the thinking drafts failed (see $L/$N.think.log)"
    .venv/bin/python $R check-think --file $W/think/ans_think.jsonl --expect $NT && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "the thinking drafts are incomplete or failed for another reason than an empty draft after $tries passes ($W/think/ans_think.jsonl)"
  done
  .venv/bin/python $R think-measures --answers $W/think/ans_think.jsonl --out-json $W/think/measures.json --out-md $W/think/measures.md --cap $CAP $([ $DRY = yes ] && echo --stub) || fail "the thinking measures failed"
  mark think $W/think/ans_think.jsonl $W/think/measures.json $W/think/measures.md
  hb "$N: thinking drafts done ($NT scenarios)"
fi

# ---------------------------------------------------------------- 5. the checking pass (V1) on the thinking drafts
if ! stage_done thinkcheck; then
  recheck thinkcheck
  .venv/bin/python $R think-drafts --answers $W/think/ans_think.jsonl --out $W/think/ans_think_drafts.jsonl || fail "the selection of the thinking drafts failed"
  ND=$(grep -c . $W/think/ans_think_drafts.jsonl | tr -d ' ')
  if [ "$ND" = "0" ]; then
    # fix 1: no usable thinking draft is a result, not a failure: only the checking pass on the thinking drafts is skipped; stage V and S have run or run as before
    echo "no usable thinking draft at cap $CAP: every row was an error or cut while thinking (see $W/think/measures.md); the checking pass on thinking drafts is skipped"
    echo "no usable thinking draft at cap $CAP" > $W/verify_think_none
    hb "$N: no usable thinking draft at cap $CAP; the check on thinking drafts is skipped; the chain goes on"
    mark thinkcheck $W/think/ans_think_drafts.jsonl $W/verify_think_none
  else
    mkdir -p $W/verify
    verify_run THINK_think_V1 $W/think/ans_think_drafts.jsonl $W/think/items.jsonl $W/verify/think_think_V1.jsonl $ND no
    mark thinkcheck $W/think/ans_think_drafts.jsonl $W/verify/think_think_V1.jsonl
  fi
fi

# the 4-bit model is unloaded when this chain loaded it
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  if [ "$(cat $W/q4_loaded_at_start 2>/dev/null)" = "no" ]; then unload $Q4; hb "$N: model calls finished, $Q4 unloaded (this chain loaded it)"
  else hb "$N: model calls finished, $Q4 left loaded (it was loaded before this chain started)"; fi
fi

NOTHINK=""; [ ! -e $W/verify_think_none ] || NOTHINK="--no-think-check"
# ---------------------------------------------------------------- 8. stage S: scoring by code (CPU)
if ! stage_done score; then
  recheck score
  .venv/bin/python $R score $SPECARG --work $W --out-prefix $W/score $NOTHINK > $W/score.out || { tail -3 $W/score.out; fail "the scoring failed (a missing input is an error, see above)"; }
  cat $W/score.out
  mark score $W/score.json $W/score.md $W/score_reader_V1.jsonl $W/score_reader_V1C.jsonl
fi

# ---------------------------------------------------------------- 9. the report and the done marker
recheck report
DONE_LINE=$(.venv/bin/python $R done-line $SPECARG --work $W $NOTHINK)
{
  cat $HEAD
  echo
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  cat $W/think/measures.md
  echo
  cat $W/score.md
  echo "### Files for the reader and for the next step"
  echo
  echo "Reader files, one per variant (every accepted change with the old line, the new line, the quote and where it was found; catches are listed to be confirmed, changes on lines no finding names and all changes on B2, T and the thinking drafts to be read): \`$W/score_reader_V1.jsonl\`, \`$W/score_reader_V1C.jsonl\`. Check records \`$W/verify/*.jsonl\`; thinking answers \`$W/think/ans_think.jsonl\`; scores \`$W/score.json\`. After the reading, \`python scripts/m7_pilot_score.py score --work $W $NOTHINK --damage V1=N,V1C=M\` prints the bars with the damage count (it writes score.json, score.md and the reader files again; the chain's own copies are kept as they are while the stage marker stands, so write them to another prefix with --out-prefix)."
  echo
  echo "$DONE_LINE"
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
echo "$DONE_LINE" > $L/$N.done
if [ $DRY = yes ]; then echo "dry run done: report from stub data in $REPORT"; echo "$DONE_LINE"; exit 0; fi
hb "$N: done; report $REPORT; $DONE_LINE" yes
status DONE yes "$N done: $REPORT; the reading of the reader files is the next task"
echo "$DONE_LINE"
