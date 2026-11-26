#!/bin/sh
# M7 I6: the D7 answers chain, Z4 stage 2, "nothing lost" on answers (runs/m7/reports/kb2_design.md sections 6, 9 row I6 and 10; MILESTONE.md W19, W33, W37).
# Built and rehearsed (dry run) here; the real run is a separate task. Nothing here starts a model by itself: the answers are server calls (one request at a time)
# under scripts/run_guarded.py, with the 4-bit builds, the default prompt, temperature 0, a 2,048-token answer limit, thinking off, packs replayed (no search call).
#
# Arms (both are the six-arm test's arm A call, scripts/m7_four_way.sh: eval2.py answer --answer-profile thorough --no-thinking --no-verify --replay-packs ...;
#   they differ only in the pack file):
#   A  the frozen packs, replayed from work/m7/d7/packs_A.jsonl (+ .manifest.json)
#   K  the frozen + entries packs, replayed from work/m7/d7/packs_K.jsonl (+ .manifest.json)
#   Both files are written by task I5 in the replay format of work/m7/six_arm/packs_frozen_d5.jsonl (--packs-a and --packs-k name other paths).
# Models: --models takes a list from gemma31,qwen (both by default, the rule as fixed); the answers are made gemma31 first, then qwen, each loaded alone and unloaded
#   after its stages. The chosen list is written into the report header and the manifest before any answer exists.
# Two runs in one work folder: `--arms A` runs alone BEFORE the entries pack exists (the frozen packs are all it needs); a later `--arms K` run resumes in the same
#   folder (the manifest does not hold the arms). The first run writes the report header with the rule text (scripts/m7_d7_answers_rule.py rule-text) and prints it for
#   the conductor to copy into runs/m7/D7_LEDGER.md; it records the header's time and hash (HEADER_RECORD.json). Arm K is refused unless the header was written before the K
#   pack file and its manifest were created (the creation time is the earlier of the birth time and the modification time; unknown is a refusal; a K pack that existed when
#   the header was written is refused for good). The pack files' hashes are recorded at their first acceptance (PACKS_RECORD.json): a pack manifest whose hash differs from
#   the file's, or a file that differs from the recorded one, is refused.
# No mechanical items. Two judge passes (DeepSeek, coverage role, instruction v2, deepseek-nothink-12k) against eval/m7/d7/rubrics.jsonl, one stage per arm (judge_A, judge_K).
# Blind reading bundles (eval2_blind.py, the six-arm format; the seed is in the header) are made once, when both arms have been answered and judged: one bundle per scenario holds
#   the answers of every arm and model of the run, under blind letters. An `--arms A` run makes none (and a single model's A alone is only one answer per scenario).
# Stages (each with a marker W/stage_NAME.done that records the manifest hash and the sha256 of every output; a line per item in the logs):
#   gate (CPU, refusal exit 2) -> header -> ans_<arm>_<model> -> judge_<arm> -> checks_<arm> (identifier check) -> relations, measures, reading, rule, report (when both arms are done)
# The server (W2, narrowed by W16; as scripts/m7_ka2.sh): a model is this chain's only with a marker it wrote when it loaded it and a server-reported load time not later than
#   the marker. A model of ours that a stage does not use is unloaded; DeepSeek-V4.1-Flash-1017-oQ4e-mtp loaded by another client is unloaded under W16 when it blocks an
#   answer stage (logged in runs/v3/HEARTBEAT.md); when the judge stage needs it, that copy is used as it is and left loaded. Any other model of another client, or a request
#   active or waiting while no W16 unload is due, refuses (exit 2) and waits for the Owner.
# A stopped run resumes: the same command again removes the failed marker after the start check; finished stages are kept.
#
#   scripts/m7_d7_answers.sh --arms A > runs/m7/logs/m7_d7_answers.out 2>&1 &                 (before the entries pack exists)
#   scripts/m7_d7_answers.sh --arms K > runs/m7/logs/m7_d7_answers.out 2>&1 &                 (later, in the same work folder)
#   scripts/m7_d7_answers.sh --dry-run --work DIR [--models gemma31,qwen] [--arms A,K] [--dry-items N] [plants]    # stub answers, stub judge, stub server; nothing is written outside DIR
# Options: --models LIST, --arms LIST (default A,K), --packs-a F, --packs-k F, --work DIR, --report FILE, --gemma31-model ID, --qwen-model ID, --scenarios-per-session N; dry runs only:
#   --dry-items N (the first N scenarios), --fail-after STAGE, --plant-foreign ID, --plant-busy N, --plant-load-before STAGE=ID, --plant-k-first (the K stand-in exists before the header).
#   In a dry run the stand-in packs are made in DIR/in (K only once the header exists, as I5 would, unless --plant-k-first).
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/; they are never written or printed.
set -e
DRY=no; W=work/m7/d7_answers; WSET=no; REPORT=runs/m7/M7_D7_ANSWERS.md; RSET=no; MODELS=gemma31,qwen; ARMS=A,K; PA=work/m7/d7/packs_A.jsonl; PK=work/m7/d7/packs_K.jsonl
PASET=no; PKSET=no; G31=gemma-4-31b-it-4bit; QID=Qwen3.8-27B-4bit; SPS=0; FAILAFTER=; PFOREIGN=; PBUSY=0; PLOAD=; DRYITEMS=0; PKFIRST=no
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --models) MODELS=${2:?}; shift;; --arms) ARMS=${2:?}; shift;; --packs-a) PA=${2:?}; PASET=yes; shift;; --packs-k) PK=${2:?}; PKSET=yes; shift;;
  --gemma31-model) G31=${2:?}; shift;; --qwen-model) QID=${2:?}; shift;; --scenarios-per-session) SPS=${2:?}; shift;;
  --dry-items) DRYITEMS=${2:?}; shift;; --fail-after) FAILAFTER=${2:?}; shift;; --plant-foreign) PFOREIGN=${2:?}; shift;; --plant-busy) PBUSY=${2:?}; shift;;
  --plant-load-before) PLOAD=${2:?}; shift;; --plant-k-first) PKFIRST=yes;;
  *) echo "usage: scripts/m7_d7_answers.sh [--arms A|K|A,K] [--models gemma31,qwen] [--packs-a F] [--packs-k F] [--work DIR] [--dry-run ...] (see the header)" >&2; exit 2;; esac
  shift; done
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
case "$W$REPORT$PA$PK" in *[[:space:]]*) refuse "--work, --report, --packs-a and --packs-k take paths without white space";; esac
R=scripts/m7_d7_answers_rule.py; SAR=scripts/m7_six_arm_rule.py; GR=scripts/m5_g_rule.py; N=m7_d7_answers; L=runs/m7/logs
MODELS=$(.venv/bin/python $R parse-models --models "$MODELS") || refuse "--models is not usable (see above)"
ARMS=$(.venv/bin/python $R parse-arms --arms "$ARMS") || refuse "--arms is not usable (see above)"
case ",$ARMS," in *,K,*) WANTK=yes;; *) WANTK=no;; esac
case ",$ARMS," in *,A,*) WANTA=yes;; *) WANTA=no;; esac
case $PBUSY in ''|*[!0-9]*) refuse "--plant-busy is a whole number";; esac
case $DRYITEMS in ''|*[!0-9]*) refuse "--dry-items is a whole number";; esac
case $SPS in ''|*[!0-9]*) refuse "--scenarios-per-session is a whole number (0: automatic)";; esac
if [ $DRY = no ]; then
  [ -z "$FAILAFTER$PFOREIGN$PLOAD" ] && [ "$PBUSY" = 0 ] && [ "$DRYITEMS" = 0 ] && [ $PKFIRST = no ] || refuse "--dry-items, --fail-after, --plant-* and --plant-k-first are for dry runs only"
else
  [ $WSET = yes ] || refuse "a dry run needs --work DIR (a folder of its own)"
  case "$W" in work/m7/d7_answers|work/m7/d7_answers/*|*/work/m7/d7_answers|*/work/m7/d7_answers/*|work/m7/d7|work/m7/d7/*|*/work/m7/d7|*/work/m7/d7/*) refuse "a dry run never uses the real work folder";; esac
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/M7_D7_ANSWERS_from_stub_data.md
  case "$REPORT" in *runs/m7/M7_D7_ANSWERS.md) refuse "a dry run never writes the real report";; esac
  [ $PASET = no ] && [ $PKSET = no ] || refuse "a dry run makes its own stand-in packs (DIR/in); --packs-a and --packs-k are for real runs"
  case "$FAILAFTER" in ""|header|ans_*|judge_*|checks_*|relations|reading) ;; *) refuse "--fail-after names no stage";; esac
  case "$PLOAD" in ""|*=*) ;; *) refuse "--plant-load-before takes STAGE=ID (a server stage)";; esac
fi
if [ $DRY = yes ]; then HBFILE=$W/HEARTBEAT.md; SLEEP=0; else HBFILE=runs/v3/HEARTBEAT.md; SLEEP=30; fi
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json; REC=runs/v3/INDEX_FROZEN.json
ITEMS=eval/m7/d7/items.jsonl; RUBS=eval/m7/d7/rubrics.jsonl; CC=eval/m7/d7/CHUNK_CHECK.json; TAGS=eval/m7/d7/slice_tags.json
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'; JSETTING=deepseek-nothink-12k; SRC=12000; SEED=20261126
mid_of() { case $1 in gemma31) echo "$G31";; qwen) echo "$QID";; esac; }
hb() { mkdir -p "$(dirname $HBFILE)"; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> $HBFILE; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M7 I6: the D7 answers, Z4 stage 2 (scripts/m7_d7_answers.sh)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
moveaside() { echo "to start again, first move the stopped run's files aside (the chain never moves or deletes them):" >&2
  for f in $W $REPORT $L/$N.failed; do [ -e $f ] && echo "  mv $f $f.stopped_$(date +%Y%m%d)" >&2; done; return 0; }
fail() { mkdir -p $L; echo "$1" >> $L/$N.failed
  if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; status STOPPED yes "$N stopped: $1; marker $L/$N.failed; a model this chain loaded may still be loaded"
  echo "STOPPED: $1"; exit 1; }
refuse_late() { mkdir -p $L; echo "REFUSED: $1" >> $L/$N.failed; hb "$N: REFUSED: $1" yes
  status STOPPED yes "$N refused: $1; marker $L/$N.failed"; echo "REFUSED: $1" >&2; exit 2; }
planted() { [ "$FAILAFTER" != "$1" ] || fail "planted failure after the stage $1 (a dry run's rehearsal of a restart)"; }

# ---------------------------------------------------------------- the gate (CPU; nothing is written before it passes, except a dry run's stand-ins and stub status)
for f in $ITEMS $RUBS $CC $TAGS $CHUNKS $REC $FROZEN $CATS/CATALOGUES.json kafka_sre_rag/prompts.py kafka_sre_rag/evalhooks.py scripts/eval2.py scripts/eval2_judge.py \
         scripts/eval2_idcheck.py scripts/eval2_blind.py scripts/run_guarded.py $GR $SAR scripts/m7_ka2_rule.py scripts/m7_kb2_rule.py scripts/m7_d7_answers.sh $R; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
VOUT=$(mktemp "${TMPDIR:-/tmp}/m7_d7_verify.XXXXXX")
.venv/bin/python -m kafka_sre_rag verify > $VOUT 2>&1 || { cat $VOUT >&2; rm -f $VOUT; refuse "kafka_sre_rag verify failed (frozen index, prompts, reranker revision or catalogues)"; }
rm -f $VOUT
DRYFLAG=; [ $DRY = no ] || DRYFLAG=--dry
if [ $DRY = yes ]; then    # the stand-ins of a dry run (the only thing written before the gate passes): items, rubrics, A's packs; K's once the header exists (or planted first)
  mkdir -p $W/in
  if [ "$DRYITEMS" != 0 ] && [ ! -s $W/in/items.jsonl ]; then head -n $DRYITEMS $ITEMS > $W/in/items.jsonl; head -n $DRYITEMS $RUBS > $W/in/rubrics.jsonl; fi
  if [ -s $W/in/items.jsonl ]; then ITEMS=$W/in/items.jsonl; RUBS=$W/in/rubrics.jsonl; fi
  PA=$W/in/packs_A.jsonl; PK=$W/in/packs_K.jsonl
  [ -s $PA ] || .venv/bin/python $R dry-packs --items $ITEMS --rubrics $RUBS --chunks $CHUNKS --out-dir $W/in --arm A > /dev/null || refuse "the stand-in A packs could not be made"
  if [ ! -s $PK ] && { [ -f $W/HEADER_RECORD.json ] || [ $PKFIRST = yes ]; }; then
    .venv/bin/python $R dry-packs --items $ITEMS --rubrics $RUBS --chunks $CHUNKS --out-dir $W/in --arm K > /dev/null || refuse "the stand-in K packs could not be made"; fi
fi
.venv/bin/python $R gate-inputs --items $ITEMS --rubrics $RUBS --chunk-check $CC --slice-tags $TAGS $DRYFLAG || refuse "the D7 files are not what the chain expects (see above)"
.venv/bin/python $R check-packs --arm A --packs $PA --items $ITEMS --chunks $CHUNKS --work $W || refuse "the A pack file is not usable (see above; arm A needs only the frozen packs)"
if [ $WANTK = yes ]; then
  .venv/bin/python $R check-packs --arm K --packs $PK --a-packs $PA --items $ITEMS --chunks $CHUNKS --work $W || refuse "the K pack file is not usable (see above; run --arms A first, and --arms K when the entries pack exists)"
fi

SJSON=; STUB=$W/stub_server_status.json
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
fi
api() { if [ $DRY = yes ]; then .venv/bin/python $R stub-status --file $STUB; else curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; fi; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep $SLEEP; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload_now() { # MODEL WHY: only when no request is active or waiting; the server's answer is checked; waits until the model is gone; logged
  idle
  if [ $DRY = yes ]; then ucode=$(.venv/bin/python $R stub-unload --file $STUB --model $1); ubody="stub"
  else
    uresp=$(curl -s -m 300 -w '\n%{http_code}' -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" 2>/dev/null) || uresp=""
    ucode=$(printf '%s' "$uresp" | tail -n 1); ubody=$(printf '%s' "$uresp" | sed '$d' | tr '\n' ' ' | cut -c1-160 | sed 's/ *$//')
  fi
  case $ucode in [0-9][0-9][0-9]) ;; *) ucode=000;; esac
  case $ucode in
    2??) uw=0; while [ "$(loaded $1)" = "yes" ]; do sleep $([ $DRY = yes ] && echo 0 || echo 5); uw=$((uw+5)); [ $uw -lt 600 ] || fail "the unload of $1 was accepted but the model is still loaded after 600 s"; done
         hb "$N: unloaded $1: $2 (no active or waiting requests; the server answered $ucode: ${ubody:-no body}; gone after ${uw} s)";;
    *) hb "$N: unload of $1 FAILED (the server answered $ucode: ${ubody:-no body})" yes; fail "the unload of $1 was not accepted (the server answered $ucode)";;
  esac; }
markers() { echo "--marker $G31=$W/loaded_answer_gemma31 --marker $QID=$W/loaded_answer_qwen --marker $DS=$W/loaded_judge"; }
drop_marker() { [ "$1" != "$G31" ] || rm -f $W/loaded_answer_gemma31; [ "$1" != "$QID" ] || rm -f $W/loaded_answer_qwen; [ "$1" != "$DS" ] || rm -f $W/loaded_judge; return 0; }
server_gate() { # NEED clear|check: exit 2 on a refusal (the model named); with clear, our other models and a W16 model are unloaded
  SJSON=$(mktemp "${TMPDIR:-/tmp}/m7_d7_status.XXXXXX"); api /api/status > $SJSON 2>/dev/null || true
  gout=$(.venv/bin/python $R server-gate --status $SJSON --need $1 $(markers) 2>&1) || { echo "$gout" >&2; rm -f $SJSON; return 2; }
  rm -f $SJSON; echo "$gout" | grep '^note:' || true
  [ "$2" = clear ] || return 0
  for m in $(echo "$gout" | sed -n 's/^UNLOAD //p'); do unload_now $m "this chain's own model, not used by the next stage"; drop_marker $m; done
  for m in $(echo "$gout" | sed -n 's/^W16 //p'); do
    hb "$N: W16: $m is loaded by another client and blocks this M7 chain; unloading it under W16 (the Owner's decision of 2026-11-24) once no request is active or waiting"
    unload_now $m "W16's unload of a foreign model (loaded by another client; MILESTONE.md W16)"; done
  for m in $(echo "$gout" | sed -n 's/^ADOPT //p'); do
    if [ ! -e $W/adopted_judge ]; then touch $W/adopted_judge
      hb "$N: $m is loaded by another client and is the model the judge needs: the judge uses that copy as it is, without this chain's marker, and leaves it loaded (W16 as the conductor read it for scripts/m7_kb2.sh)"; fi; done
  return 0; }
others() { pgrep -fl 'scripts/(m4|m5|m6|m7|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }
wait_others() { [ $DRY = yes ] && return 0
  [ -z "$(others)" ] && return 0
  hb "$N: waiting until no other chain and no guarded job runs"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done; }
plant_load() { # STAGE: a dry run's planted load by another client just before the stage's server gate
  [ $DRY = yes ] && [ -n "$PLOAD" ] && [ "${PLOAD%%=*}" = "$1" ] || return 0
  .venv/bin/python $R stub-load --file $STUB --model "${PLOAD#*=}"; }
use_model() { # ROLE ID STAGE: the gate for this stage's model, then our marker (the model loads on the first request)
  wait_others; plant_load $3
  server_gate $2 clear || refuse_late "the server is not free for this chain (another client's model or request; see above)"
  server_gate $2 check || refuse_late "the server is not free for this chain after the unloads (see above)"
  if [ "$1" = judge ] && [ -e $W/adopted_judge ] && [ ! -e $W/loaded_judge ]; then return 0; fi     # another client's copy, used as it is (no marker)
  if [ ! -e $W/loaded_$1 ]; then touch $W/loaded_$1; hb "$N: loading $2 (the $1 stage; marker $W/loaded_$1; it loads on the first request)"; fi
  [ $DRY = no ] || .venv/bin/python $R stub-load --file $STUB --model $2; }
refresh_marker() { [ ! -e $W/loaded_$1 ] || touch $W/loaded_$1; }     # after a job ended: a later load by another client is then seen as foreign
done_model() { # ROLE ID: unload our model after its stage; another client's adopted copy stays loaded
  if [ "$1" = judge ] && [ -e $W/adopted_judge ] && [ ! -e $W/loaded_judge ]; then
    hb "$N: the judge is done; $2 (another client's copy, used without a marker) is left loaded"; rm -f $W/adopted_judge; return 0; fi
  if [ -e $W/loaded_$1 ] && [ "$(loaded $2)" = yes ]; then unload_now $2 "the $1 stage is done (this chain's model)"; fi
  rm -f $W/loaded_$1; }

stage_done() { [ -f $W/stage_$1.done ]; }
NEED1=; for m in $(echo $MODELS | tr ',' ' '); do for arm in $(echo $ARMS | tr ',' ' '); do [ -n "$NEED1" ] || [ -f $W/stage_ans_${arm}_$m.done ] || NEED1=$(mid_of $m); done; done
for arm in $(echo $ARMS | tr ',' ' '); do [ -n "$NEED1" ] || [ -f $W/stage_judge_$arm.done ] || NEED1=$DS; done
if [ $DRY = yes ]; then mkdir -p $W; .venv/bin/python $R stub-status --file $STUB --init --loaded "$PFOREIGN" --busy $PBUSY; fi
if [ -n "$NEED1" ]; then server_gate $NEED1 check || refuse "the server is not free for this chain (another client's model or request; see above)"; fi
if [ $DRY = no ]; then
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m7_d7_models.XXXXXX"); api /v1/models > $MJSON 2>/dev/null || true
  NEEDIDS=$DS; for m in $(echo $MODELS | tr ',' ' '); do NEEDIDS="$NEEDIDS $(mid_of $m)"; done
  .venv/bin/python $SAR models-check --models $MJSON --need $NEEDIDS || { rm -f $MJSON; refuse "a model this chain needs is not listed by the server ($NEEDIDS)"; }
  rm -f $MJSON
fi

# ---------------------------------------------------------------- the gate has passed: the work and log folders are written from here on
mkdir -p $W $L
if [ $DRY = yes ]; then
  G="scripts/run_guarded.py --other-retries 0 --max-restarts 0 --wait-min 0"; STALL=2; GA="--backend stub --no-rerank"; REALFLAG=
else
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 0"; STALL=40; REALFLAG=--real
fi
unset M7_STUB_CUT_WHILE_THINKING M7_STUB_CHECK_FAIL M7_STUB_ANSWER_FAIL
PINS=; for f in $ITEMS $RUBS $CC $TAGS $PA $PA.manifest.json $CATS/CATALOGUES.json $REC $CHUNKS $(.venv/bin/python $R code-pins); do PINS="$PINS --pin $f"; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
gstate() { sub=$1; shift     # the options do not hold the arms (an A run and a later K run share one manifest); the K pack file is checked against PACKS_RECORD.json, not pinned here
  .venv/bin/python $R $sub --work $W $PINS $DRYFLAG --opt "models=$MODELS" --opt "gemma31=$G31" --opt "qwen=$QID" --opt "judge=$DS" --opt "judge_setting=$JSETTING" \
    --opt "bundle_seed=$SEED" --opt "scenarios_per_session=$SPS" --opt "dry_items=$DRYITEMS" --opt "packs_a=$PA" --info "git_head=$GIT_HEAD" "$@"; }
SCOUT=$(mktemp "${TMPDIR:-/tmp}/m7_d7_startcheck.XXXXXX")
if ! gstate start-check > $SCOUT 2>&1; then
  cat $SCOUT >&2
  if grep -q '^  options\.' $SCOUT && ! grep '^  ' $SCOUT | grep -vq '^  options\.'; then rm -f $SCOUT; refuse "an option differs from the stored manifest (see above: the models and ids are fixed at the first run; nothing was moved or deleted)"; fi
  rm -f $SCOUT; moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; fi
cat $SCOUT; rm -f $SCOUT
if [ -f $L/$N.failed ]; then echo "rerun after a stop; the earlier failure was: $(tail -n 1 $L/$N.failed)"; rm -f $L/$N.failed; fi
gstate manifest > /dev/null
.venv/bin/python $R check-packs --arm A --packs $PA --items $ITEMS --chunks $CHUNKS --work $W --record > /dev/null || refuse_late "the A pack file could not be recorded (see above)"
recheck() { rc=$(gstate start-check 2>&1) || { echo "$rc" >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started"; }; }
mark() { st=$1; shift; .venv/bin/python $R mark --work $W --stage $st --out "$@" > /dev/null; echo "stage $st done"; planted $st; }

ND=$(grep -c . $ITEMS | tr -d ' ')
[ $DRY = yes ] || [ "$ND" = 60 ] || fail "D7 has $ND scenarios; 60 expected"
CNT=$(.venv/bin/python $R call-counts --nd $ND --models $MODELS --arms $ARMS)
cnt() { echo "$CNT" | tr ' ' '\n' | sed -n "s/^$1=//p"; }
echo "the rule (printed before any call and before any entries pack exists; the header of $REPORT holds the same text; copy it into runs/m7/D7_LEDGER.md):"
.venv/bin/python $R rule-text
echo "rule sha256: $(.venv/bin/python -c 'import sys; sys.path.insert(0, "scripts"); import m7_d7_answers_rule as r; print(r.rule_sha256())')"
echo
echo "planned: D7 $ND scenarios; models $MODELS (in this order); arms of this run $ARMS; calls: $CNT (judge rows per arm: models x $ND x 2); model-server time (inferred in kb2_design.md section 7, not measured): about 7 to 7.5 h for both models and both arms with the judging"
[ $DRY = no ] || echo "DRY RUN: stub answers, stub judge, stub server status; no number below is a measurement"
status RUNNING no "$N running ($ARMS); markers $L/$N.done / .failed"

# ---------------------------------------------------------------- header (before any answer exists, and before the entries pack is created)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $W/ans_d7_*.jsonl; do [ ! -e $f ] || refuse_late "$f exists but the report header was not written yet; the rule text comes first"; done
  [ $DRY = yes ] || [ ! -e $REPORT ] || { moveaside; refuse_late "$REPORT exists; it is not overwritten before this run's header is written (move it away first)"; }
  {
    echo "# M7 I6: D7's answers, Z4 stage 2 (nothing lost)$([ $DRY = yes ] && echo ' (DRY RUN: stub answers, stub judge, stub server; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/m7_d7_answers.sh\` on $(date +%Y-%m-%d) (git $GIT_HEAD). **Models of this run (in the order of their answers): $(echo $MODELS | sed 's/,/, /g')**; ids: gemma31 \`$G31\`, qwen \`$QID\`; judge \`$DS\` ($JSETTING, instruction v2, coverage role, two passes). Arms A (frozen packs) and K (frozen + entries packs) are replayed from files; the pack files are recorded by hash at their first acceptance. Blind letters of the reading bundles: seed $SEED. Rule: \`$R\`."
    echo
    .venv/bin/python $R rule-text
    echo "**The order that makes the rule binding.** This header was written before the K pack file was created (stage marker \`$W/stage_header.done\`, record \`$W/HEADER_RECORD.json\` with the time and the hash of this text); the chain refuses to answer arm K otherwise. The chain checks the order from the files' creation times (the earlier of the birth time and the modification time); a file copied after the header looks new, so this is evidence, not proof."
    echo
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  .venv/bin/python $R header-record --work $W --header $HEAD --models $MODELS $([ -s $PK ] && echo "--k-packs $PK") || fail "the header record could not be written"
  mark header $HEAD $W/HEADER_RECORD.json
  .venv/bin/python $R rule-text > $W/D7_LEDGER_rule_text.md
  hb "$N: report header with the rule text written ($REPORT; models $MODELS); no K pack existed unless the record says so; rule text for the ledger in $W/D7_LEDGER_rule_text.md"
  echo "the rule text is in the header and in $W/D7_LEDGER_rule_text.md: copy it into runs/m7/D7_LEDGER.md now; the entries pack (K) may be created only after this line (written $(date +%H:%M:%S))"
fi
k_order() { .venv/bin/python $R check-k-order --work $W --header $HEAD --k-packs $PK; }
if [ $WANTK = yes ]; then k_order || refuse_late "arm K is refused (see above): the rule text's header must be written before the K pack file is created"; fi
if [ $WANTK = yes ]; then .venv/bin/python $R check-packs --arm K --packs $PK --a-packs $PA --items $ITEMS --chunks $CHUNKS --work $W --record || refuse_late "the K pack file could not be recorded (see above)"; fi

# ---------------------------------------------------------------- the answers (server; each model alone, one request at a time)
.venv/bin/python scripts/m5_g_rule.py harness-items --in $ITEMS --out $W/items_d7_h.jsonl --domain d7 > /dev/null || fail "harness items of D7"
IH=$W/items_d7_h.jsonl
packs_of() { case $1 in A) echo $PA;; K) echo $PK;; esac; }
answer_run() { # ARM KEY MID: one set, all items, at most 3 passes (each after the server gate); then the set's checks
  arm=$1; k=$2; mid=$3; out=$W/ans_d7_${arm}_$k.jsonl; replay=$(packs_of $arm); expect=$ND
  [ $DRY = yes ] || GA="--backend openai --model $mid"
  bad=0
  while :; do
    if .venv/bin/python $GR check-answers --file $out --expect $expect --mode default > $W/check.out 2>&1 \
       && .venv/bin/python $SAR check-rows --file $out --expect $expect $REALFLAG --no-thinking >> $W/check.out 2>&1; then
      echo "answers $arm $k: complete: $(tr '\n' ' ' < $W/check.out)"; break; fi
    [ $bad -lt 3 ] || fail "answers $arm $k: incomplete, with errors, empty or without a whole profile row after $bad passes ($out; see $W/check.out)"
    if [ $bad -gt 0 ]; then server_gate $mid check || refuse_late "the server is not free for $mid before pass $((bad+1)) (see above)"; fi
    bad=$((bad+1)); idle
    hb "$N: answers $arm of $k started ($expect items, pass $bad; default prompt, replayed packs $replay, thinking off, 2,048 tokens)"
    set +e; $G --stall-min $STALL --log $L/$N.$arm-$k.log -- .venv/bin/python scripts/eval2.py answer --items $IH --out $out --config m7-d7-$arm-$k $GA \
      --extra-body "$EB" --mode default --replay-packs $replay --catalogue $CATS --host-class "Mac Studio" --allow-commit-change \
      --answer-profile thorough --no-thinking --no-verify; rc=$?; set -e
    refresh_marker answer_$k
    [ $rc = 0 ] || echo "answers $arm $k: pass $bad ended with exit $rc (see $L/$N.$arm-$k.log)"
  done
  .venv/bin/python $GR packs-equal --a $replay --b $out || fail "answers $arm $k: the packs differ from the pack file (replay failed)"
  if [ $arm = K ] && [ -f $W/ans_d7_A_$k.jsonl.manifest.json ]; then   # the server echoed the model that answered A
    .venv/bin/python $GR check-echo --stored $W/ans_d7_A_$k.jsonl.manifest.json --new $out.manifest.json $DRYFLAG || fail "answers $arm $k: the server echoed another model than for arm A"; fi
  .venv/bin/python $R bodies-check --answers $out --items $IH --packs $replay --model-id $([ $DRY = yes ] && echo stub || echo $mid) $([ $DRY = yes ] && echo --non-streamed) \
    || fail "answers $arm $k: the request bodies are not the six-arm arm A call on the replayed pack (see above)"
}
model_stage() { # KEY
  k=$1; mid=$(mid_of $k); todo=
  for arm in $(echo $ARMS | tr ',' ' '); do stage_done ans_${arm}_$k || todo="$todo $arm"; done
  [ -n "$todo" ] || return 0
  for arm in $todo; do recheck ans_${arm}_$k; if [ $arm = K ]; then k_order || refuse_late "arm K is refused (see above)"; fi; done
  use_model answer_$k $mid ans_${todo# }_$k
  for arm in $todo; do
    answer_run $arm $k $mid
    .venv/bin/python $R stage-calls --stage ans_${arm}_$k --kind answers --expect $(cnt ans_${arm}_$k) --files $W/ans_d7_${arm}_$k.jsonl || fail "the calls of the stage ans_${arm}_$k do not match the expected count"
    mark ans_${arm}_$k $W/ans_d7_${arm}_$k.jsonl $W/ans_d7_${arm}_$k.jsonl.manifest.json
    hb "$N: arm $arm of $k done (D7 $ND answers)"
  done
  done_model answer_$k $mid
}
for k in $(echo $MODELS | tr ',' ' '); do model_stage $k; done

# ---------------------------------------------------------------- judge (server: DeepSeek, one stage per arm, two passes on the answers of every model of the run)
judge_stage() { # ARM
  arm=$1; stage_done judge_$arm && return 0
  recheck judge_$arm
  JOUT=$W/judge_d7_$arm.jsonl; JCALLS=$(cnt judge_$arm); SETS=
  for k in $(echo $MODELS | tr ',' ' '); do SETS="$SETS ${arm}_$k=$W/ans_d7_${arm}_$k.jsonl"; done
  use_model judge $DS judge_$arm
  jrun() { # EXTRA...
    if [ $DRY = yes ]; then
      .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
        --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $JSETTING --concurrency 3 \
        --stub --passes 1 2 --cache $W/judge_cache_d7_$arm.jsonl --out $JOUT "$@" > $L/$N.judge_$arm.log 2>&1
    else
      $G --stall-min 60 --log $L/$N.judge_$arm.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS --chunks $CHUNKS --source-chars $SRC \
        --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
        --settings-name $JSETTING --concurrency 3 --judge-url "$OMLX_URL/v1" --passes 1 2 --cache $W/judge_cache_d7_$arm.jsonl --out $JOUT "$@"
    fi; }
  hb "$N: judging arm $arm started ($JCALLS judge rows over the sets $(echo $SETS | sed 's/=[^ ]*//g'); the cache holds earlier replies)"
  for round in 1 2; do
    extra=; [ $round = 2 ] && extra=--retry-invalid
    set +e; jrun $extra; rc=$?; set -e
    refresh_marker judge
    [ $rc = 0 ] || fail "judging arm $arm round $round failed (see $L/$N.judge_$arm.log)"
    jbad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $JOUT)
    echo "judging arm $arm round $round: $jbad unscored"
    [ "$jbad" = 0 ] && break
    server_gate $DS check || refuse_late "the server is not free for the judge before round 2 (see above)"
  done
  DECIDED=; for k in $(echo $MODELS | tr ',' ' '); do DECIDED="$DECIDED ${arm}_$k"; done
  .venv/bin/python $SAR judge-sets --file $JOUT --n $ND --decided $DECIDED > $W/judge_sets_$arm.txt || { cat $W/judge_sets_$arm.txt; fail "judging arm $arm: a set lacks a valid row for some scenario and pass ($JOUT)"; }
  cat $W/judge_sets_$arm.txt
  .venv/bin/python $R stage-calls --stage judge_$arm --kind judge --expect $JCALLS --files $JOUT || fail "the judge rows of arm $arm do not match the expected count"
  done_model judge $DS
  mark judge_$arm $JOUT $W/judge_sets_$arm.txt
  hb "$N: judging arm $arm done ($JCALLS rows)"
}
for arm in $(echo $ARMS | tr ',' ' '); do judge_stage $arm; done

# ---------------------------------------------------------------- checks (CPU): the identifier check of every answer set (reported, not decided)
checks_stage() { # ARM
  arm=$1; stage_done checks_$arm && return 0
  recheck checks_$arm
  OUTS=
  for k in $(echo $MODELS | tr ',' ' '); do
    f=$W/ans_d7_${arm}_$k.jsonl
    rm -f $W/idcheck_d7_${arm}_$k.json     # this stage's own output of an earlier, unfinished attempt (the checker never overwrites a file)
    .venv/bin/python scripts/eval2_idcheck.py --answers $f --catalogues $CATS --totals-only --json $W/idcheck_d7_${arm}_$k.json > $W/idcheck_d7_${arm}_$k.txt || fail "the identifier check on $f failed"
    echo "idcheck d7 $arm $k: $(sed -n 4p $W/idcheck_d7_${arm}_$k.txt)"
    OUTS="$OUTS $W/idcheck_d7_${arm}_$k.json $W/idcheck_d7_${arm}_$k.txt"
  done
  mark checks_$arm $OUTS
}
for arm in $(echo $ARMS | tr ',' ' '); do checks_stage $arm; done

# ---------------------------------------------------------------- when both arms are answered and judged: relations, measures, reading, the rule, the report (CPU)
FULL=yes
for k in $(echo $MODELS | tr ',' ' '); do for arm in A K; do stage_done ans_${arm}_$k || FULL=no; done; done
for arm in A K; do stage_done judge_$arm && stage_done checks_$arm || FULL=no; done
STUBF=; [ $DRY = no ] || STUBF=--stub
if [ $FULL = yes ]; then
  if ! stage_done relations; then
    recheck relations
    PCOUT=$W/prompts_check.txt; : > $PCOUT; PCFAIL=0
    pc() { "$@" >> $PCOUT 2>&1 || PCFAIL=1; }
    for k in $(echo $MODELS | tr ',' ' '); do      # A and K of a model differ only by the pack (the prompt and the message part of the body)
      pc .venv/bin/python $SAR prompts-relation --a $W/ans_d7_A_$k.jsonl --b $W/ans_d7_K_$k.jsonl
      pc .venv/bin/python $SAR bodies-relation --a $W/ans_d7_A_$k.jsonl --b $W/ans_d7_K_$k.jsonl
      pc .venv/bin/python $SAR prompts-default --items $ITEMS --answers $W/ans_d7_A_$k.jsonl
      pc .venv/bin/python $SAR prompts-default --items $ITEMS --answers $W/ans_d7_K_$k.jsonl
    done
    if [ "$MODELS" = gemma31,qwen ]; then      # the two models answer the same call: the same prompt on the same pack, the body equal but for the model's name
      for arm in A K; do
        pc .venv/bin/python $SAR prompts-relation --a $W/ans_d7_${arm}_gemma31.jsonl --b $W/ans_d7_${arm}_qwen.jsonl --expect-same-pack
        pc .venv/bin/python $SAR bodies-relation --a $W/ans_d7_${arm}_gemma31.jsonl --b $W/ans_d7_${arm}_qwen.jsonl --across-models
      done
    fi
    [ $PCFAIL = 0 ] || { cat $PCOUT; fail "the prompt check failed: the arms differ in more than the pack (see above)"; }
    echo "prompt check: $(wc -l < $PCOUT | tr -d ' ') lines, no failure"
    mark relations $PCOUT
  fi
  .venv/bin/python $R measures --work $W --rubrics $RUBS --slice-tags $TAGS --models $MODELS $STUBF > $W/measures.out 2> $W/measures.err || { cat $W/measures.err >&2; fail "the measures could not be computed (a missing measure is an error; see above)"; }
  RCOPY=$W/reading_for_readers; [ $DRY = yes ] || RCOPY=$(dirname $W)/$(basename $W)_reading     # a copy of the bundles for the readers, outside the work folder: no key, no answer file
  if ! stage_done reading; then
    recheck reading
    RD=$W/reading; mkdir -p $RD
    RSETS=$(.venv/bin/python $R reading-sets --work $W --models $MODELS | tr '\n' ' ')
    rm -rf $RD/first $RD/second $RD/KEY.json $RD/sessions.json $RCOPY
    .venv/bin/python $SAR no-draft-ids --answers $RSETS --sets-by-item $RD/sets_by_item.json > /dev/null 2> $RD/no_draft.err || fail "the scan for answers with no draft failed"
    cat $RD/no_draft.err
    .venv/bin/python scripts/eval2_blind.py bundle --items $ITEMS --rubrics $RUBS --answers $RSETS --chunks $CHUNKS --out $RD/first --key $RD/KEY.json --seed $SEED \
      --second-reader 0.25 --second-out $RD/second --sets-by-item $RD/sets_by_item.json --harm-consequences > $RD/bundle.out || fail "the blind bundles could not be made (fewer than two usable answers in every scenario?)"
    tail -3 $RD/bundle.out
    .venv/bin/python $SAR no-draft-ids --answers $RSETS --key $RD/KEY.json > /dev/null 2>&1 || fail "the no-draft answers could not be recorded in the key"
    .venv/bin/python $SAR reading-summary --key $RD/KEY.json | tee $RD/reading_summary.txt
    .venv/bin/python $SAR reading-plan --key $RD/KEY.json --out $RD/sessions.json --scenarios-per-session $SPS || fail "the reading plan could not be made"
    .venv/bin/python $SAR leak-check --dir $RD --terms $G31 $QID || fail "a bundle names a configuration (an arm, a pack kind or a base model)"
    mkdir -p $RCOPY && cp -R $RD/first $RD/second $RD/sessions.json $RCOPY/
    .venv/bin/python $SAR leak-check --dir $RCOPY --terms $G31 $QID || fail "the readers' copy names a configuration"
    echo "bundles for the readers (no key): $RCOPY"
    mark reading $RD/KEY.json $RD/sessions.json
  fi
  RULE_RC=0; .venv/bin/python $R rule --measures $W/measures.json > $W/rule_out.txt 2> $W/rule_out.err || RULE_RC=$?
  [ $RULE_RC = 0 ] || { cat $W/rule_out.err >&2; fail "the rule could not be applied: $(head -1 $W/rule_out.err)"; }
else
  echo "arm A answered and judged here; the arms K, the relations, the measures, the reading bundles and the rule follow when arm K has been answered and judged (--arms K, later, in this work folder)"
  .venv/bin/python $R measures --work $W --rubrics $RUBS --slice-tags $TAGS --models $MODELS $STUBF > $W/measures.out 2> $W/measures.err || { cat $W/measures.err >&2; fail "the measures could not be computed (see above)"; }
  RULE_RC=0; .venv/bin/python $R rule --measures $W/measures.json > $W/rule_out.txt 2> $W/rule_out.err || RULE_RC=$?
  [ $RULE_RC = 0 ] || { cat $W/rule_out.err >&2; fail "the rule could not be applied: $(head -1 $W/rule_out.err)"; }
fi
cat $W/rule_out.txt
FINAL=$(sed -n 's/^\(M7 D7 ANSWERS (Z4 part 3.*\)$/\1/p' $W/rule_out.txt | tail -n 1)
[ -n "$FINAL" ] || fail "the rule printed no usable last line"
{
  cat $HEAD
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')$([ $FULL = yes ] || echo ' (INTERIM: arm K is not answered here yet)')"
  echo
  cat $W/measures.md
  echo "### The rule applied by code (\`$R rule\`)"
  echo
  echo '```'; cat $W/rule_out.txt; echo '```'
  echo
  echo "### Judge rows per set"
  echo
  echo '```'; for arm in A K; do [ ! -f $W/judge_sets_$arm.txt ] || cat $W/judge_sets_$arm.txt; done; echo '```'
  echo
  if [ $FULL = yes ]; then
    echo "### The reading: scenarios read per set"
    echo
    echo '```'; cat $W/reading/reading_summary.txt; echo '```'
    echo
    echo "### The prompt check"
    echo
    echo '```'; cat $W/prompts_check.txt; echo '```'
    echo
  fi
  echo "### What this report does not show"
  echo
  echo "Wrong, invented and harmful statements, and the consequence of every harmful one: they come from the blind reading of the bundles (Claude readers, W18; the readers' copy without the key: \`${RCOPY:-not made yet}\`, the key in \`$W/reading/KEY.json\`, which no reader opens). Until the reading is unblinded with \`scripts/eval2_blind.py unblind --key ... --require-consequence\` and the harm is matched by consequence (\`scripts/m7_six_arm_rule.py harm-match\` with the sets named \`K_<model>\` and \`A_<model>\`; not run here), the harm condition of the rule is open and the result is not called met. The judge is also the entry writer (the rule text names the risk). D7 is opened only for this measurement (kb2_design.md section 8); the hidden part is not involved. Time is the server's on this machine, one request at a time."
  echo
  echo "### Files"
  echo
  echo "Answers \`$W/ans_d7_{A,K}_{$MODELS}.jsonl\`; pack records \`$W/PACKS_RECORD.json\`, header record \`$W/HEADER_RECORD.json\`; judge \`$W/judge_d7_{A,K}.jsonl\`; identifier checks \`$W/idcheck_d7_*\`; measures \`$W/measures.json\`."
  echo
  echo "$FINAL"
} > $REPORT.tmp
if [ -f $REPORT ] && cmp -s $REPORT.tmp $REPORT; then rm -f $REPORT.tmp; else mv $REPORT.tmp $REPORT; fi
DONELINE="done: $([ $FULL = yes ] && echo "both arms, final stages run" || echo "interim, arms $ARMS") of models $MODELS; report $REPORT; $FINAL"
grep -qxF "$DONELINE" $L/$N.done 2>/dev/null || { echo "$DONELINE" >> $L/$N.done; hb "$N: $DONELINE" $([ $DRY = yes ] && echo no || echo yes); }
status DONE yes "$N done: $REPORT; $FINAL"
echo "$([ $DRY = yes ] && echo 'dry run ')done: report $REPORT"
echo "$FINAL"
