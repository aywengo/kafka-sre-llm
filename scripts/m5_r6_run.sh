#!/bin/sh
# M5 R6-chain: an informative run that decides nothing (MILESTONE.md, row R6; the Owner's decision Q11 of 2026-11-22).
# Question: do more excerpts served turn into more points covered? The grounded variant that the development check picked (or default) answers
# B1 (30) and pilot2 (24) once on the packs of retrieval setting r4g2 (scripts/m5_r6_packs.py; work/m5/r6/packs_b1.jsonl, packs_p2.jsonl), without
# readers, and the answers are judged next to the SAME mode's answers on the frozen packs (the development check's answers, work/m5/gdev/ans_<set>_<variant>.jsonl).
# Reference configuration as in the development check: the 4-bit base model on the server, thinking off, the 2,048-token limit; the judge, its
# setting, instruction v2 and two passes are the development chain's (scripts/m5_g_dev.sh); the rule helpers are scripts/m5_g_rule.py (called, not copied).
# No rule, no threshold, no choice: the report states what was measured.
#
#   scripts/m5_r6_run.sh --mode grounded-g1|grounded-g2|default > runs/m5/logs/m5_r6_<variant>.out 2>&1 &
#   scripts/m5_r6_run.sh --mode MODE --dry-run [--work DIR]   # stub model, stub judge, no server, no marker outside DIR, no heartbeat, no file written outside
#                                                              # DIR (default: a temporary directory); the numbers are not measurements
# --mode has no default: the conductor passes the picked variant. Options: --work DIR, --report FILE, --packs-b1 F, --packs-p2 F, --dev-dir DIR, --dev-done FILE.
# Two options that are off unless given (the Owner's decision Q12 of 2026-11-22; the same meaning as in scripts/m5_g_dev.sh, the rule script's code is called):
#   --max-unjudged N (default 0: a judge row that stays invalid after the judge rounds stops the chain): up to N such rows per judged set (B1, pilot2) are left
#   out; an answer's judged measures are then the mean over its valid passes (the paired comparison of that scenario uses one pass on that side), an answer with
#   no valid pass still stops the chain; the report names every unjudged row. --accept-code-change "REASON": the start check accepts changed hashes of
#   scripts/m5_r6_run.sh and scripts/m5_r6_packs.py and of nothing else, only when the answers stage is done; old and new hashes, the reason and the time go
#   into the manifest and the report.
# Steps (each resumable, one line per item; a stage marker work/.../stage_NAME.done records the manifest hash and every output hash):
#   0. Gate (CPU, refusal exit 2, nothing moved or deleted): the inputs; the pack files against the sha256 recorded when they were made; every pack starts with
#      the stored frozen pack; the development check's done marker (and no failed marker); its comparison answers for this mode complete, on the frozen packs and
#      made on the reference configuration; the stored manifest and stage markers against the state now (start check); kafka_sre_rag verify; then the server
#      (real runs): the 4-bit base model listed, no other chain or guarded job, no active or waiting request, no model of another client loaded.
#   1. Report header (first line: "Informative run; decides nothing"), written once before any answer.
#   2. Answers of the mode on the r4g2 packs, B1 then pilot2 (guarded, stall limit 40 min, packs equal to the pack files, echoed model equal to the comparison runs').
#   3. Judge (DeepSeek, coverage role, instruction v2, deepseek-nothink-12k, two passes) on both answer sets of B1 and pilot2, no cache taken over; guarded.
#   4. Identifier check (CPU). 5. Measures and the report runs/m5/R6_R4_PACKS.md (scripts/m5_r6_packs.py measures).
#   A state left by a stopped run (work folder, manifest, report, failed marker) is refused by the start check; the refusal prints the mv lines.
set -e
DRY=no; MODE=""; W=""; WSET=no; REPORT=runs/m5/R6_R4_PACKS.md; RSET=no
PB1=work/m5/r6/packs_b1.jsonl; PP2=work/m5/r6/packs_p2.jsonl; DEV=work/m5/gdev; DEVDONE=runs/m5/logs/m5_g_dev.done; MAXU=0; ACCEPT=
USAGE="usage: scripts/m5_r6_run.sh --mode grounded-g1|grounded-g2|default [--dry-run] [--work DIR] [--report FILE] [--packs-b1 F] [--packs-p2 F] [--dev-dir DIR] [--dev-done FILE] [--max-unjudged N] [--accept-code-change REASON]"
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --mode) MODE=${2:?$USAGE}; shift;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --packs-b1) PB1=${2:?}; shift;; --packs-p2) PP2=${2:?}; shift;;
  --dev-dir) DEV=${2:?}; shift;; --dev-done) DEVDONE=${2:?}; shift;;
  --max-unjudged) MAXU=${2:?}; shift;; --accept-code-change) ACCEPT=${2:?}; shift;;
  *) echo "$USAGE" >&2; exit 2;; esac
  shift; done
case $MAXU in ''|*[!0-9]*) echo "REFUSED: --max-unjudged is a whole number (0 or more); nothing started" >&2; exit 2;; esac
[ -n "$MODE" ] || { echo "REFUSED: --mode is required (grounded-g1, grounded-g2 or default); nothing started" >&2; echo "$USAGE" >&2; exit 2; }
case $MODE in grounded-g1) VAR=g1;; grounded-g2) VAR=g2;; default) VAR=default;;
  *) echo "REFUSED: --mode is grounded-g1, grounded-g2 or default, not '$MODE'; nothing started" >&2; exit 2;; esac
L=runs/m5/logs; N=m5_r6_$VAR; R=scripts/m5_g_rule.py; PK=scripts/m5_r6_packs.py
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json
B1_ITEMS=eval/m4/b1/items.jsonl; B1_RUBS=eval/m4/b1/rubrics.jsonl; P2_ITEMS=eval/m4/pilot2/items.jsonl; P2_RUBS=eval/m4/pilot2/rubrics.jsonl
B1_ANS=work/m4/b1/ans_default.jsonl; P2_ANS=work/m4/pilot2/ans_default.jsonl
NEEDS_B1=work/m5/b1_needs.jsonl; NEEDS_DEV=eval/m4/retrieval_needs_dev.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
SETTING=deepseek-nothink-12k; SRC=12000
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m5_r6_run.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/R6_R4_PACKS_from_stub_data.md
  DEV=$W/dev_stub     # a dry run does not read the development check's folder: the comparison answers are the stub's, on the stored frozen packs
  G="scripts/run_guarded.py --other-retries 0"
else
  [ $WSET = yes ] || W=work/m5/r6/run_$VAR
fi
mkdir -p $W $L
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M5 R6: informative run on r4g2 packs",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
moveaside() { # the lines that move the state of a stopped run out of the way (printed, never executed here)
  echo "to start again, first move the stopped run's files aside (the chain never moves or deletes them):" >&2
  for f in $W $REPORT $L/$N.failed; do [ -e $f ] && echo "  mv $f $f.stopped" >&2; done
  echo "  (and runs/m5/logs/$N.out, if it holds the stopped run's output, before the next start redirects into it)" >&2; return 0; }
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- 0. gate (CPU; nothing is written before the checks pass, except $W itself)
for f in $B1_ITEMS $B1_RUBS $P2_ITEMS $P2_RUBS $B1_ANS $B1_ANS.manifest.json $P2_ANS $P2_ANS.manifest.json $PB1 $PB1.manifest.json $PP2 $PP2.manifest.json \
         $NEEDS_B1 $NEEDS_DEV $CHUNKS $FROZEN $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py \
         scripts/eval2_score.py scripts/eval2_compare.py scripts/run_guarded.py kafka_sre_rag/prompts.py scripts/m5_r6_run.sh $PK $R; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
.venv/bin/python $PK sha --packs $PB1 || refuse "the pack file $PB1 is not the one recorded when it was made (sha256 differs or its manifest is missing)"
.venv/bin/python $PK sha --packs $PP2 || refuse "the pack file $PP2 is not the one recorded when it was made (sha256 differs or its manifest is missing)"
.venv/bin/python $PK check --packs $PB1 --stored $B1_ANS --ids-from $B1_ITEMS || refuse "the packs of $PB1 do not all start with the stored frozen pack"
.venv/bin/python $PK check --packs $PP2 --stored $P2_ANS --ids-from $P2_ITEMS || refuse "the packs of $PP2 do not all start with the stored frozen pack"
if [ $DRY = no ]; then
  [ -s $DEVDONE ] && grep -q M5_G_DEV_DONE $DEVDONE || refuse "the development check's done marker $DEVDONE does not exist (the development check has not finished)"
  [ ! -e ${DEVDONE%.done}.failed ] || refuse "the development check's failed marker ${DEVDONE%.done}.failed exists"
  for s in b1 p2; do
    if [ $s = b1 ]; then n=30; else n=24; fi
    f=$DEV/ans_${s}_$VAR.jsonl
    [ -s $f ] && [ -s $f.manifest.json ] || refuse "the comparison answers $f (the development check's answers of this mode on the frozen packs) are missing"
    .venv/bin/python $R check-answers --file $f --expect $n --mode $MODE || refuse "the comparison answers $f are incomplete, have errors or another mode"
    if [ $s = b1 ]; then st=$B1_ANS; else st=$P2_ANS; fi
    .venv/bin/python $R packs-equal --a $st --b $f || refuse "the comparison answers $f are not on the stored frozen packs"
  done
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
fi
api() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
loaded_list() { api /api/status | python3 -c 'import sys,json; print(" ".join(json.load(sys.stdin).get("loaded_models") or []))' 2>/dev/null || echo "status-unreadable"; }
unload_now() { # any model; only when no request is active or waiting; the server's answer is checked and logged
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
others() { pgrep -fl 'scripts/(m4|m5|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }

if [ $DRY = yes ]; then
  Q4=Qwen3.8-27B-4bit
else
  api /v1/models > $W/models.json 2>/dev/null || true
  Q4=${M5_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || refuse "the 4-bit model is not listed by the server (or ambiguous)"
fi
DRYFLAG=""; DRYFLAG_ECHO=""; [ $DRY = yes ] && { DRYFLAG="--dry"; DRYFLAG_ECHO="--dry"; }
PINS="--pin $B1_ITEMS --pin $B1_RUBS --pin $P2_ITEMS --pin $P2_RUBS --pin $B1_ANS --pin $B1_ANS.manifest.json --pin $P2_ANS --pin $P2_ANS.manifest.json \
  --pin $PB1 --pin $PB1.manifest.json --pin $PP2 --pin $PP2.manifest.json --pin $NEEDS_B1 --pin $NEEDS_DEV --pin $CATS/CATALOGUES.json --pin runs/v3/INDEX_FROZEN.json"
if [ $DRY = no ]; then   # the development check's answers are the comparison: they must not change under this run
  for s in b1 p2; do PINS="$PINS --pin $DEV/ans_${s}_$VAR.jsonl --pin $DEV/ans_${s}_$VAR.jsonl.manifest.json"; done
  PINS="$PINS --pin $DEVDONE"
fi
for f in $PK $R scripts/m5_r6_run.sh scripts/eval2.py scripts/eval2_score.py scripts/eval2_judge.py scripts/eval2_idcheck.py kafka_sre_rag/prompts.py $FROZEN; do
  PINS="$PINS --pin $f"; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)   # recorded for the reader, never compared
gstate() { # SUBCOMMAND [EXTRA...] (script GS, default the rule script): the manifest and the start check take the same state; the judge's arguments contain spaces, so they are passed quoted
  sub=$1; shift
  .venv/bin/python ${GS:-$R} "$sub" $STATE --info "git_head=$GIT_HEAD" --judge-arg "model=$DS" --judge-arg "setting=$SETTING" --judge-arg "extra_body=$JEB" \
    --judge-arg "source_chars=$SRC" --judge-arg "instruction=v2" --judge-arg "passes=1 2" --judge-arg "role=coverage" --judge-arg "mode=$MODE" "$@"; }
STATE="--manifest $W/MANIFEST.json --work $W $PINS --frozen $FROZEN --mechanical sample --model $Q4 $DRYFLAG"
if [ -n "$ACCEPT" ]; then GS=$PK; gstate start-check --accept-code-change "$ACCEPT" || { GS=; moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; }; GS=
else gstate start-check || { moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; }; fi
.venv/bin/python -m kafka_sre_rag verify > $W/verify.out 2>&1 || { cat $W/verify.out >&2; refuse "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"; }
grep -E '^prompt ' $W/verify.out | head -8
gstate manifest
mkdir -p "$(dirname $REPORT)"

est() { awk 'BEGIN{ lo=54*35+216*10+1200; hi=54*85+216*10+1800; printf "%.1f to %.1f", lo/3600, hi/3600 }'; }
echo "planned: $MODE answers on the r4g2 packs of B1 (30) and pilot2 (24), then 216 judge calls (both pack sets, 108 answers, two passes): $(est) h of model-server time (inferred from the development check's rates: 35 to 85 s per answer, about 10 s per judge call, loads 0.3 to 0.5 h)"

stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $R mark --work $W --stage $st --out "$@" > /dev/null; }
ANS_FILES="$W/ans_b1_r4g2.jsonl $W/ans_p2_r4g2.jsonl"
SERVER_NEEDED=no
for st in answers judge; do stage_done $st || SERVER_NEEDED=yes; done

# ---------------------------------------------------------------- 1. report header (before any answer exists)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $ANS_FILES; do [ ! -e $f ] || refuse "$f exists but the report header was not written yet"; done
  [ $DRY = yes ] || [ ! -e $REPORT ] || { moveaside; refuse "$REPORT exists; it is not overwritten (move it away first)"; }
  {
    echo "Informative run; decides nothing$([ $DRY = yes ] && echo ' (DRY RUN: stub model, stub judge; no number below is a measurement)')"
    echo
    echo "# M5 R6: do more excerpts served turn into more points covered? ($MODE on the packs of r4g2, without readers)"
    echo
    echo "Written by \`scripts/m5_r6_run.sh --mode $MODE\` on $(date +%Y-%m-%d). The Owner's decision Q11 of 2026-11-22: the retrieval ranking target ends for M5; this one run is made to see whether the extra excerpts of r4g2 show up as covered points. It sets no rule and picks nothing; a number below is not compared with a bar."
    echo
    echo "Setup: mode \`$MODE\`, 4-bit model \`$Q4\`, thinking off, 2,048-token limit, B1 (30) and pilot2 (24) answered on the r4g2 packs (\`$PB1\`, \`$PP2\`: the stored frozen six, then the chunks added by the setting, up to 10 chunks and 24,000 characters). Comparison: the same mode's answers on the frozen packs (\`$DEV/ans_b1_$VAR.jsonl\`, \`ans_p2_$VAR.jsonl\`, the development check's). Judge \`$DS\`, coverage role, instruction v2, setting \`$SETTING\`, two passes; both answer sets are judged in this run, no cache taken over. Manifest hash $(.venv/bin/python -c 'import hashlib,json,sys; print(hashlib.sha256(json.dumps(json.load(open(sys.argv[1])), sort_keys=True).encode()).hexdigest()[:16])' $W/MANIFEST.json)."
    echo
    echo "Open development data only (B1 and pilot2 were used to build and choose the setting, so it is not evidence of improvement on fresh questions). Neither set is a gate; the hidden part is not involved."
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header written ($REPORT); no answer exists yet"
fi

# ---------------------------------------------------------------- the server, once
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  rm -f $L/$N.failed $L/$N.done
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done
  hb "$N: started; 4-bit model $Q4, $MODE on the r4g2 packs of B1 (30) and pilot2 (24); judge $DS (coverage role, instruction v2, $SETTING)"
  status RUNNING no "$N running: answers, judging, measures; markers $L/$N.done / .failed"
  clear_server
  hb "$N: the server is idle and no model but $Q4 is loaded; the 4-bit model loads on the first request"
fi

# ---------------------------------------------------------------- prepare: harness items (CPU)
if ! stage_done prepare; then
  .venv/bin/python $R harness-items --in $B1_ITEMS --out $W/items_b1_h.jsonl --domain b1 || fail "harness items of B1"
  .venv/bin/python $R harness-items --in $P2_ITEMS --out $W/items_p2_h.jsonl --domain pilot2 || fail "harness items of pilot2"
  mark prepare $W/items_b1_h.jsonl $W/items_p2_h.jsonl
fi
NB1=$(grep -c . $W/items_b1_h.jsonl | tr -d ' '); NP2=$(grep -c . $W/items_p2_h.jsonl | tr -d ' ')
[ "$NB1" = 30 ] && [ "$NP2" = 24 ] || fail "B1 has $NB1 items and pilot2 $NP2; 30 and 24 expected"

# ---------------------------------------------------------------- answers (one request at a time; the 4-bit model)
if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $Q4"; fi
echo_check() { # the model the server echoed equals the comparison runs' model_echoed (a difference stops the chain before the judge)
  .venv/bin/python $R check-echo --stored $cmpref.manifest.json --new $out.manifest.json $DRYFLAG_ECHO \
    || fail "answers $config: the server echoed another model than the comparison runs (the answer sets are not comparable)"; }
answer_run() { # OUT ITEMS REPLAY EXPECT MODE CONFIG CMPREF: all items, one request at a time
  out=$1; items=$2; replay=$3; expect=$4; mode=$5; config=$6; cmpref=$7
  if .venv/bin/python $R check-answers --file $out --expect $expect --mode $mode > $W/check.out 2>&1; then
    echo "skip answers: $(cat $W/check.out)"; echo_check; return 0; fi
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    hb "$N: answers $config started ($expect items, mode $mode)"
    $G --stall-min 40 --log $L/$N.$config.log -- .venv/bin/python scripts/eval2.py answer --items $items --out $out --config $config $GA \
      --extra-body "$EB" --mode $mode --replay-packs $replay --catalogue $CATS --host-class "Mac Studio" \
      || fail "answers $config: failed (see $L/$N.$config.log)"
    .venv/bin/python $R check-answers --file $out --expect $expect --mode $mode && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $config: incomplete, with errors, empty or without a pack after $tries passes ($out)"
  done
  .venv/bin/python $R packs-equal --a $replay --b $out || fail "answers $config: the packs differ from the pack file (replay failed)"
  echo_check
}
if [ $DRY = yes ] && ! stage_done cmp; then
  # dry run only: stand-ins for the development check's answers (this mode on the stored frozen packs, stub model)
  for s in b1 p2; do
    if [ $s = b1 ]; then n=30; st=$B1_ANS; else n=24; st=$P2_ANS; fi
    cmpref=$st; out=$DEV/ans_${s}_$VAR.jsonl; mkdir -p $DEV
    .venv/bin/python scripts/eval2.py answer --items $W/items_${s}_h.jsonl --out $out --config m5-r6-dry-cmp-$s $GA --extra-body "$EB" --mode $MODE \
      --replay-packs $st --catalogue $CATS --host-class "Mac Studio" > $W/cmp_$s.out 2>&1 || fail "dry run: the stand-in comparison answers of $s failed (see $W/cmp_$s.out)"
    .venv/bin/python $R check-answers --file $out --expect $n --mode $MODE || fail "dry run: the stand-in comparison answers of $s are incomplete"
  done
  mark cmp $DEV/ans_b1_$VAR.jsonl $DEV/ans_p2_$VAR.jsonl
fi
if ! stage_done answers; then
  answer_run $W/ans_b1_r4g2.jsonl $W/items_b1_h.jsonl $PB1 $NB1 $MODE m5-r6-b1-$VAR $DEV/ans_b1_$VAR.jsonl
  answer_run $W/ans_p2_r4g2.jsonl $W/items_p2_h.jsonl $PP2 $NP2 $MODE m5-r6-p2-$VAR $DEV/ans_p2_$VAR.jsonl
  mark answers $ANS_FILES
  hb "$N: all answers done (B1 $NB1, pilot2 $NP2, mode $MODE, r4g2 packs)"
fi

# ---------------------------------------------------------------- judge: DeepSeek, coverage role, two passes, both pack sets
if ! stage_done judge; then
  if [ $DRY = no ]; then unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; fi
  judge_done() { if [ $MAXU != 0 ]; then [ -f "$1" ] && .venv/bin/python $R judge-check --file "$1" --expect "$2" --max-unjudged $MAXU > /dev/null 2>&1; return; fi
    [ -f "$1" ] && .venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; sys.exit(0 if len(r)==int(sys.argv[2]) and all(x["valid_reply"] for x in r) else 1)' "$1" "$2" 2>/dev/null; }
  jrun() { # SET ITEMS RUBRICS CACHE OUT EXTRA...
    set_=$1; items=$2; rubs=$3; cache=$4; jout=$5; shift 5
    SETS="frozen=$DEV/ans_${set_}_$VAR.jsonl r4g2=$W/ans_${set_}_r4g2.jsonl"
    if [ $DRY = yes ]; then
      .venv/bin/python scripts/eval2_judge.py run --rubrics $rubs --items $items --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
        --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $SETTING --concurrency 3 --stub --passes 1 2 --cache $cache --out $jout "$@"
    else
      $G --stall-min 60 --log $L/$N.judge_$set_.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $rubs --items $items \
        --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
        --settings-name $SETTING --concurrency 3 --judge-url "$OMLX_URL/v1" --passes 1 2 --cache $cache --out $jout "$@"
    fi; }
  for set_ in b1 p2; do
    if [ $set_ = b1 ]; then items=$B1_ITEMS; rubs=$B1_RUBS; n=$NB1; else items=$P2_ITEMS; rubs=$P2_RUBS; n=$NP2; fi
    jout=$W/judge_$set_.jsonl; cache=$W/judge_cache_$set_.jsonl; calls=$((n * 2 * 2))
    if judge_done $jout $calls; then echo "skip judging $set_: $jout has $calls valid rows"; continue; fi
    hb "$N: judging $set_ started ($calls judge rows; the cache holds earlier replies)"
    for round in 1 2; do
      extra=""; [ $round = 2 ] && extra="--retry-invalid"
      if [ $DRY = yes ]; then jrun $set_ $items $rubs $cache $jout $extra > $W/judge_$set_.dry.out 2>&1 || { cat $W/judge_$set_.dry.out; fail "judge dry run ($set_) failed"; }
        grep -E '^(matched|instruction|items)' $W/judge_$set_.dry.out
      else jrun $set_ $items $rubs $cache $jout $extra || fail "judging $set_ round $round failed (see $L/$N.judge_$set_.log)"; fi
      bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $jout)
      echo "judging $set_ round $round: $bad unscored"
      [ "$bad" = "0" ] && break
    done
    if [ $MAXU != 0 ]; then
      .venv/bin/python $R judge-check --file $jout --expect $calls --max-unjudged $MAXU || fail "judging $set_: $jout does not hold $calls rows with at most $MAXU unjudged and a valid pass for every answer"
    else judge_done $jout $calls || fail "judging $set_: $jout does not hold $calls valid rows"; fi
  done
  if [ $DRY = no ]; then unload $DS; hb "$N: judging finished, $DS unloaded"; fi
  mark judge $W/judge_b1.jsonl $W/judge_p2.jsonl
fi

# ---------------------------------------------------------------- identifier check (CPU)
if ! stage_done checks; then
  OUTS=""
  for s in b1 p2; do
    for v in frozen r4g2; do
      if [ $v = frozen ]; then f=$DEV/ans_${s}_$VAR.jsonl; else f=$W/ans_${s}_r4g2.jsonl; fi
      .venv/bin/python scripts/eval2_idcheck.py --answers $f --catalogues $CATS --totals-only > $W/idcheck_${s}_$v.txt || fail "identifier check on $s $v failed"
      echo "idcheck $s $v: $(sed -n 4p $W/idcheck_${s}_$v.txt)"
      OUTS="$OUTS $W/idcheck_${s}_$v.txt"
    done
  done
  mark checks $OUTS
fi

# ---------------------------------------------------------------- measures and the report
.venv/bin/python $PK measures --work $W --dev-dir $DEV --variant $VAR --mode $MODE --stored-b1 $B1_ANS --stored-p2 $P2_ANS --rubrics-b1 $B1_RUBS --rubrics-p2 $P2_RUBS \
  --packs-b1 $PB1 --packs-p2 $PP2 --needs-b1 $NEEDS_B1 --needs-dev $NEEDS_DEV --max-unjudged $MAXU $([ $DRY = yes ] && echo --stub) > $W/measures.out \
  || fail "the measures could not be computed (a missing input is an error, see $W/measures.out)"
{
  cat $HEAD
  echo
  for note in "unjudged-note --work $W --max-unjudged $MAXU" "code-change-note --manifest $W/MANIFEST.json"; do
    txt=$(.venv/bin/python $PK $note) || fail "the report block '${note%% *}' could not be written"
    [ -z "$txt" ] || { echo "$txt"; echo; }
  done
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  cat $W/measures.md
  echo
  echo "### The packs (made by \`scripts/m5_r6_packs.py packs\`, CPU, reranker scores read from the funnel's caches)"
  echo
  echo '```'
  if [ -s work/m5/r6/packs_build.log ]; then grep -v '^\[packs' work/m5/r6/packs_build.log; else echo "(the pack build's log work/m5/r6/packs_build.log is not there)"; fi
  .venv/bin/python $PK sha --packs $PB1 || true
  .venv/bin/python $PK sha --packs $PP2 || true
  echo '```'
  echo
  echo "### Files"
  echo
  echo "Answers \`$W/ans_*_r4g2.jsonl\`; the comparison answers \`$DEV/ans_*_$VAR.jsonl\`; judge results \`$W/judge_b1.jsonl\`, \`judge_p2.jsonl\`; identifier checks \`$W/idcheck_*.txt\`; measures \`$W/measures.json\`. Informative run; decides nothing."
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
if [ $DRY = yes ]; then
  echo M5_R6_DRY_RUN_DONE >> $L/$N.done
  sed -n '1,3p' $REPORT | cut -c1-240
  echo "dry run done: report from stub data in $REPORT"
  exit 0
fi
hb "$N: done; report $REPORT (informative; decides nothing)" yes
status DONE yes "$N done: $REPORT; informative run, decides nothing; the Owner reads it"
echo M5_R6_DONE >> $L/$N.done
