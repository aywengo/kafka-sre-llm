#!/bin/sh
# M7 KA2: the Z3 measurement of the lookup cards on Qwen3.8-27B-4bit and gemma-4-31b-it-4bit (agents/m7-plan.md section 5 (a) and section 7 row Z3;
# MILESTONE.md W12, W13, W16, W20). The rule (scripts/m7_ka2_rule.py rule-text) is printed before any call and written into the report header before any
# answer of arm L exists; it decides per model PASS or FAIL and nothing about publishing (W10, W13).
#
# Arms, per model: L = exactly the six-arm test's arm A call (scripts/m7_four_way.sh: eval2.py answer --answer-profile thorough --no-thinking --no-verify
#   --replay-packs ... --catalogue work/m4/catalogues, the default prompt, temperature 0, 2,048-token limit) plus --lookup-cards (the question's names only
#   on replayed packs, W20), on D5 (50) and the mechanical items (96 + 20, as arm A). The no-cards arm is the STORED arm A of work/m7/six_arm/ (W20): the
#   chain rebuilds its prompts on the CPU (a stub run of the same call) and compares prompt_sha256 and pack per id before any call; one difference refuses.
#
# Stages (each with a marker W/stage_NAME.done that records the manifest hash and the sha256 of every output; a line per item in the logs):
#   gate     (CPU; refusal exit 2, nothing written before it passes): inputs, kafka_sre_rag verify, the stored arm A (gate-stored), real runs: the server
#            lists the three models, and the server gate below.
#   header   the report header with the rule, written before any L answer exists.
#   rebuild  (CPU) the stub rebuild of arm A's prompts (refused when one differs from the stored qwen or gemma31 rows) and of L's expected prompts.
#   ans_l_qwen, ans_l_gemma31   the answers (server, one request at a time, each model alone, unloaded after its stage when this chain loaded it); after each
#            set: whole rows, packs equal to the replay file, the server's model echo equal to A's, L against its rebuilt prompts, A's rows and A's manifest.
#   judge    DeepSeek on the two L sets of D5, two passes (coverage role, instruction v2, deepseek-nothink-12k), as in the six-arm test; A's stored judgements reused.
#   checks   (CPU) the identifier check (D5, A and L, per answer), the mechanical scorer on L (A's stored scores reused).
#   then the measures, the rule and the report runs/m7/M7_KA2.md; done marker runs/m7/logs/m7_ka2.done (or .failed).
#
# The server (W2, narrowed by W16; as scripts/m7_kb2.sh after its review): a model is this chain's only with a marker it wrote when it loaded it
#   (W/loaded_answer_qwen, W/loaded_answer_gemma31, W/loaded_judge) and a server-reported load time not later than the marker. Before a model stage a
#   model of ours that the stage does not use is unloaded; DeepSeek-V4.1-Flash-1017-oQ4e-mtp loaded by another client is unloaded under W16 when it blocks
#   (an answer stage), logged in runs/v3/HEARTBEAT.md as W16's unload; when the judge stage needs it, that copy is used as it is, without our marker, and
#   left loaded (logged). Any other model of another client, or a request active or waiting while no W16 unload is due, refuses (exit 2) and waits for
#   the Owner. The guarded jobs run with --other-retries 0; a pass that failed is retried only after the server gate again (at most 3 passes per set).
# A stopped run resumes: the same command again removes the failed marker after the start check; finished stages are kept, a stage that stopped midway
#   resumes from its output files. A changed pin, option or output is refused and the mv lines are printed (nothing is moved or deleted here).
#
#   scripts/m7_ka2.sh > runs/m7/logs/m7_ka2.out 2>&1 &                                   (the real run)
#   scripts/m7_ka2.sh --dry-run --work DIR [--dry-items N] [plants]                    # stub answers, stub judge, stub server status; nothing is written outside DIR
# Options: --work DIR, --report FILE; dry runs only: --stored DIR (another stored folder, e.g. a planted copy), --dry-items N (a subset copy of the first N
#   items of each part, made in DIR/stored), --fail-after STAGE, --plant-foreign ID (a model of another client loaded at the start), --plant-busy N (the first N
#   status reads show an active request), --plant-load-before STAGE=ID (another client loads ID just before the stage's server gate).
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/; they are never written or printed.
set -e
DRY=no; W=work/m7/ka2; WSET=no; REPORT=runs/m7/M7_KA2.md; RSET=no; STORED=work/m7/six_arm; SSET=no; FAILAFTER=; PFOREIGN=; PBUSY=0; PLOAD=; DRYITEMS=0
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; QID=Qwen3.8-27B-4bit; G31=gemma-4-31b-it-4bit
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --stored) STORED=${2:?}; SSET=yes; shift;;
  --dry-items) DRYITEMS=${2:?}; shift;; --fail-after) FAILAFTER=${2:?}; shift;; --plant-foreign) PFOREIGN=${2:?}; shift;; --plant-busy) PBUSY=${2:?}; shift;;
  --plant-load-before) PLOAD=${2:?}; shift;;
  *) echo "usage: scripts/m7_ka2.sh [--dry-run --work DIR [--dry-items N] [--stored DIR] [plants]] (see the header)" >&2; exit 2;; esac
  shift; done
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
case "$W$REPORT$STORED" in *[[:space:]]*) refuse "--work, --report and --stored take paths without white space";; esac
R=scripts/m7_ka2_rule.py; SAR=scripts/m7_six_arm_rule.py; GR=scripts/m5_g_rule.py; N=m7_ka2; L=runs/m7/logs
case $PBUSY in ''|*[!0-9]*) refuse "--plant-busy is a whole number";; esac
case $DRYITEMS in ''|*[!0-9]*) refuse "--dry-items is a whole number";; esac
if [ $DRY = no ]; then
  [ -z "$FAILAFTER$PFOREIGN$PLOAD" ] && [ "$PBUSY" = 0 ] && [ "$DRYITEMS" = 0 ] && [ $SSET = no ] || refuse "--stored, --dry-items, --fail-after and the --plant-* options are for dry runs only"
else
  [ $WSET = yes ] || refuse "a dry run needs --work DIR (a folder of its own)"
  case "$W" in work/m7/ka2|work/m7/ka2/*|*/work/m7/ka2|*/work/m7/ka2/*) refuse "a dry run never uses the real work folder";; esac
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/M7_KA2_from_stub_data.md
  case "$REPORT" in *runs/m7/M7_KA2.md) refuse "a dry run never writes the real report";; esac
  case "$FAILAFTER" in ""|header|rebuild|ans_l_qwen|ans_l_gemma31|judge|checks) ;; *) refuse "--fail-after names no stage";; esac
  case "$PLOAD" in ""|header=*|rebuild=*|ans_l_qwen=*|ans_l_gemma31=*|judge=*) ;; *) refuse "--plant-load-before takes STAGE=ID (a server stage)";; esac
fi
if [ $DRY = yes ]; then HBFILE=$W/HEARTBEAT.md; SLEEP=0; else HBFILE=runs/v3/HEARTBEAT.md; SLEEP=30; fi
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json; REC=runs/v3/INDEX_FROZEN.json
D5ITEMS=eval/m6/d5/items.jsonl; RUBS=eval/m6/d5/rubrics.jsonl; TA_ANS=work/m4/s6/ans_A1.jsonl; TA3_ANS=work/m4/e2/ans_O_Q4D_a3.jsonl
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'; JSETTING=deepseek-nothink-12k; SRC=12000
hb() { mkdir -p "$(dirname $HBFILE)"; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> $HBFILE; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M7 KA2: the lookup cards on Qwen and Gemma 31B (Z3; scripts/m7_ka2.sh)",
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

# ---------------------------------------------------------------- the gate (CPU; nothing is written before it passes, except a dry run's stub status)
for f in $CHUNKS $REC $FROZEN $CATS/CATALOGUES.json $D5ITEMS $RUBS $TA_ANS $TA3_ANS kafka_sre_rag/prompts.py kafka_sre_rag/kb_lookup.py kafka_sre_rag/evalhooks.py \
         scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_score.py scripts/run_guarded.py $GR $SAR scripts/m7_kb2_rule.py scripts/m7_ka2.sh $R; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
VOUT=$(mktemp "${TMPDIR:-/tmp}/m7_ka2_verify.XXXXXX")
.venv/bin/python -m kafka_sre_rag verify > $VOUT 2>&1 || { cat $VOUT >&2; rm -f $VOUT; refuse "kafka_sre_rag verify failed (frozen index, prompts, reranker revision or catalogues)"; }
rm -f $VOUT
DRYFLAG=; [ $DRY = no ] || DRYFLAG=--dry
if [ $DRY = yes ] && [ "$DRYITEMS" != 0 ]; then    # a subset copy of the stored folder, the only thing (with the stub status) written before the gate passes
  case "$STORED" in "$W"/*) ;; *) mkdir -p $W; .venv/bin/python $R dry-stored --src $STORED --out $W/stored --n $DRYITEMS > /dev/null || refuse "the dry run's stored subset could not be made"; STORED=$W/stored;; esac
fi
.venv/bin/python $R gate-stored --stored $STORED --replay-d5 $STORED/packs_frozen_d5.jsonl --replay-ma $TA_ANS --replay-m3 $TA3_ANS $DRYFLAG \
  || refuse "the stored arm A is not usable (see above)"

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
markers() { echo "--marker $QID=$W/loaded_answer_qwen --marker $G31=$W/loaded_answer_gemma31 --marker $DS=$W/loaded_judge"; }
drop_marker() { [ "$1" != "$QID" ] || rm -f $W/loaded_answer_qwen; [ "$1" != "$G31" ] || rm -f $W/loaded_answer_gemma31; [ "$1" != "$DS" ] || rm -f $W/loaded_judge; return 0; }
server_gate() { # NEED clear|check: exit 2 on a refusal (the model named); with clear, our other models and a W16 model are unloaded
  SJSON=$(mktemp "${TMPDIR:-/tmp}/m7_ka2_status.XXXXXX"); api /api/status > $SJSON 2>/dev/null || true
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

if [ $DRY = yes ]; then mkdir -p $W; .venv/bin/python $R stub-status --file $STUB --init --loaded "$PFOREIGN" --busy $PBUSY; fi
NEED1=; for st in ans_l_qwen:$QID ans_l_gemma31:$G31 judge:$DS; do [ -n "$NEED1" ] || [ -f $W/stage_${st%%:*}.done ] || NEED1=${st#*:}; done
if [ -n "$NEED1" ]; then server_gate $NEED1 check || refuse "the server is not free for this chain (another client's model or request; see above)"; fi
if [ $DRY = no ]; then
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m7_ka2_models.XXXXXX"); api /v1/models > $MJSON 2>/dev/null || true
  .venv/bin/python -c 'import json,sys; ids={m.get("id") for m in json.load(open(sys.argv[1])).get("data",[])}; miss=[m for m in sys.argv[2:] if m not in ids]; print("missing: "+" ".join(miss)) if miss else None; sys.exit(1 if miss else 0)' $MJSON $QID $G31 $DS \
    || { rm -f $MJSON; refuse "a model this chain needs is not listed by the server ($QID, $G31, $DS)"; }
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
PINS=; for f in $STORED/items_d5_h.jsonl $STORED/items_ma.jsonl $STORED/items_m3.jsonl $STORED/packs_frozen_d5.jsonl $STORED/judge_d5.jsonl $TA_ANS $TA3_ANS $D5ITEMS $RUBS \
                $CATS/CATALOGUES.json $REC $CHUNKS $(.venv/bin/python $R code-pins); do PINS="$PINS --pin $f"; done
for p in d5 ma m3; do for x in "" _gemma31; do PINS="$PINS --pin $STORED/ans_${p}_A$x.jsonl --pin $STORED/ans_${p}_A$x.jsonl.manifest.json"
  [ $p = d5 ] || PINS="$PINS --pin $STORED/score_${p}_A$x.jsonl"; done; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
gstate() { sub=$1; shift
  .venv/bin/python $R $sub --work $W $PINS $DRYFLAG --opt "stored=$STORED" --opt "qwen=$QID" --opt "gemma31=$G31" --opt "judge=$DS" --opt "judge_setting=$JSETTING" \
    --opt "dry_items=$DRYITEMS" --info "git_head=$GIT_HEAD" "$@"; }
SCOUT=$(mktemp "${TMPDIR:-/tmp}/m7_ka2_startcheck.XXXXXX")
if ! gstate start-check > $SCOUT 2>&1; then cat $SCOUT >&2; rm -f $SCOUT; moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; fi
cat $SCOUT; rm -f $SCOUT
if [ -f $L/$N.failed ]; then echo "rerun after a stop; the earlier failure was: $(tail -n 1 $L/$N.failed)"; rm -f $L/$N.failed; fi
gstate manifest > /dev/null
recheck() { rc=$(gstate start-check 2>&1) || { echo "$rc" >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started"; }; }
stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $R mark --work $W --stage $st --out "$@" > /dev/null; echo "stage $st done"; planted $st; }

ND=$(grep -c . $STORED/items_d5_h.jsonl | tr -d ' '); NMA=$(grep -c . $STORED/items_ma.jsonl | tr -d ' '); NM3=$(grep -c . $STORED/items_m3.jsonl | tr -d ' ')
[ $DRY = yes ] || { [ "$ND" = 50 ] && [ "$NMA" = 96 ] && [ "$NM3" = 20 ]; } || fail "the stored items are $ND + $NMA + $NM3; 50 + 96 + 20 expected"
CNT=$(.venv/bin/python $R call-counts --nd $ND --nma $NMA --nm3 $NM3)
cnt() { echo "$CNT" | tr ' ' '\n' | sed -n "s/^$1=//p"; }
EST=$(.venv/bin/python $R estimate --stored $STORED --logs runs/m7/logs --work-items $STORED --judge-rows $(cnt judge))
echo "the rule (printed before any call; the header of $REPORT holds the same text):"
.venv/bin/python $R rule-text
echo
echo "planned: D5 $ND, mechanical $NMA + $NM3; calls per stage: $CNT; model-server time per stage:"
echo "$EST" | sed 's/^/  /'
[ $DRY = no ] || echo "DRY RUN: stub answers, stub judge, stub server status; no number below is a measurement"
status RUNNING no "$N running; markers $L/$N.done / .failed"

# ---------------------------------------------------------------- header (before any L answer exists)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $W/ans_*_L*.jsonl; do [ ! -e $f ] || refuse_late "$f exists but the report header was not written yet; the rule text comes first"; done
  [ $DRY = yes ] || [ ! -e $REPORT ] || { moveaside; refuse_late "$REPORT exists; it is not overwritten before this run's header is written (move it away first)"; }
  {
    echo "# M7 KA2: the lookup cards on Qwen and Gemma 31B (Z3)$([ $DRY = yes ] && echo ' (DRY RUN: stub answers, stub judge, stub server; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/m7_ka2.sh\` on $(date +%Y-%m-%d) (git $GIT_HEAD). Models: qwen \`$QID\`, gemma31 \`$G31\`; judge \`$DS\` ($JSETTING, instruction v2, coverage role, two passes). Stored arm A: \`$STORED\`. Rule: \`$R\`."
    echo
    .venv/bin/python $R rule-text
    echo
    echo "**Planned calls and server time (inferred from the six-arm test's measured rates).** $CNT."
    echo
    echo '```'; echo "$EST"; echo '```'
    echo
    echo "This header was written before any answer of arm L existed (stage marker \`$W/stage_header.done\`)."
    echo
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header with the rule written ($REPORT); no L answer exists yet"
fi

# ---------------------------------------------------------------- rebuild (CPU): arm A's prompts and L's expected prompts, before any call
RB=$W/rebuild
items_of() { case $1 in d5) echo $STORED/items_d5_h.jsonl;; *) echo $STORED/items_$1.jsonl;; esac; }
replay_of() { case $1 in d5) echo $STORED/packs_frozen_d5.jsonl;; ma) echo $TA_ANS;; m3) echo $TA3_ANS;; esac; }
stub_answer() { # PART OUT EXTRA...: the call of arm A on the stub backend (CPU): the prompts only
  p=$1; o=$2; shift 2
  .venv/bin/python scripts/eval2.py answer --items $(items_of $p) --out $o --config m7-ka2-rebuild-$p --backend stub --no-rerank --extra-body "$EB" --mode default \
    --replay-packs $(replay_of $p) --catalogue $CATS --host-class "Mac Studio" --allow-commit-change --answer-profile thorough --no-thinking --no-verify "$@" > $o.log 2>&1 \
    || { tail -3 $o.log; fail "the stub rebuild $o failed (see $o.log)"; }; }
if ! stage_done rebuild; then
  recheck rebuild
  mkdir -p $RB; OUTS=
  for p in d5 ma m3; do
    stub_answer $p $RB/rb_$p.jsonl
    .venv/bin/python $R rebuild-check --rebuilt $RB/rb_$p.jsonl --stored $STORED/ans_${p}_A.jsonl $STORED/ans_${p}_A_gemma31.jsonl \
      || refuse_late "the stored arm A's prompts of $p are not rebuilt identically (W20 needs the stored arm A as the no-cards arm; see above)"
    stub_answer $p $RB/rbL_$p.jsonl --lookup-cards
    OUTS="$OUTS $RB/rb_$p.jsonl $RB/rb_$p.jsonl.manifest.json $RB/rbL_$p.jsonl $RB/rbL_$p.jsonl.manifest.json"
  done
  mark rebuild $OUTS
  hb "$N: the stored arm A's prompts rebuilt on the CPU and equal (qwen and gemma31, D5 $ND, mechanical $NMA + $NM3); L's prompts rebuilt"
fi

# ---------------------------------------------------------------- answers (server; each model alone, one request at a time)
answer_run() { # PART KEY MID SFX: one set of L, all items, at most 3 passes (each after the server gate); then the set's checks
  p=$1; k=$2; mid=$3; x=$4; out=$W/ans_${p}_L$x.jsonl; ref=$STORED/ans_${p}_A$x.jsonl; expect=$(grep -c . $(items_of $p) | tr -d ' ')
  [ $DRY = yes ] || GA="--backend openai --model $mid"
  bad=0
  while :; do
    if .venv/bin/python $GR check-answers --file $out --expect $expect --mode default > $W/check.out 2>&1 \
       && .venv/bin/python $SAR check-rows --file $out --expect $expect $REALFLAG --no-thinking >> $W/check.out 2>&1; then
      echo "answers $p $k: complete: $(tr '\n' ' ' < $W/check.out)"; break; fi
    [ $bad -lt 3 ] || fail "answers $p $k: incomplete, with errors, empty or without a whole profile row after $bad passes ($out; see $W/check.out)"
    if [ $bad -gt 0 ]; then server_gate $mid check || refuse_late "the server is not free for $mid before pass $((bad+1)) (see above)"; fi
    bad=$((bad+1)); idle
    hb "$N: answers $p of $k started ($expect items, pass $bad; arm A's call plus --lookup-cards)"
    set +e; $G --stall-min $STALL --log $L/$N.$p-L$x.log -- .venv/bin/python scripts/eval2.py answer --items $(items_of $p) --out $out --config m7-ka2-$p-L$x $GA \
      --extra-body "$EB" --mode default --replay-packs $(replay_of $p) --catalogue $CATS --host-class "Mac Studio" --allow-commit-change \
      --answer-profile thorough --no-thinking --no-verify --lookup-cards; rc=$?; set -e
    refresh_marker answer_$k
    [ $rc = 0 ] || echo "answers $p $k: pass $bad ended with exit $rc (see $L/$N.$p-L$x.log)"
  done
  .venv/bin/python $GR packs-equal --a $(replay_of $p) --b $out || fail "answers $p $k: the packs differ from the replay file"
  .venv/bin/python $GR check-echo --stored $ref.manifest.json --new $out.manifest.json $DRYFLAG || fail "answers $p $k: the server echoed another model than arm A's ($ref)"
  if [ $DRY = yes ]; then BREF=$RB/rb_$p.jsonl; else BREF=$ref; fi       # the request body's reference: A's rows (a dry run: the stub rebuild, the same backend)
  .venv/bin/python $R lookup-relation --l $out --rebuilt $RB/rbL_$p.jsonl --a $ref --body-ref $BREF --ref-manifest $BREF.manifest.json \
    || fail "answers $p $k: the set is not arm A's call plus the cards (see above)"
}
model_stage() { # KEY MID SFX
  k=$1; mid=$2; x=$3
  stage_done ans_l_$k && return 0
  recheck ans_l_$k
  use_model answer_$k $mid ans_l_$k
  for p in d5 ma m3; do answer_run $p $k $mid "$x"; done
  .venv/bin/python $R stage-calls --stage ans_l_$k --kind answers --expect $(cnt ans_l_$k) --files $W/ans_d5_L$x.jsonl $W/ans_ma_L$x.jsonl $W/ans_m3_L$x.jsonl \
    || fail "the calls of the stage ans_l_$k do not match the expected count"
  done_model answer_$k $mid
  mark ans_l_$k $W/ans_d5_L$x.jsonl $W/ans_ma_L$x.jsonl $W/ans_m3_L$x.jsonl $W/ans_d5_L$x.jsonl.manifest.json $W/ans_ma_L$x.jsonl.manifest.json $W/ans_m3_L$x.jsonl.manifest.json
  hb "$N: arm L of $k done (D5 $ND, mechanical $NMA + $NM3); $mid unloaded if this chain loaded it"
}
model_stage qwen $QID ""
model_stage gemma31 $G31 _gemma31

# ---------------------------------------------------------------- judge (server: DeepSeek on the two L sets of D5, two passes)
JOUT=$W/judge_d5.jsonl; JCALLS=$(cnt judge)
if ! stage_done judge; then
  recheck judge
  use_model judge $DS judge
  jrun() { # EXTRA...
    if [ $DRY = yes ]; then
      .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $D5ITEMS --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
        --answers L=$W/ans_d5_L.jsonl L_gemma31=$W/ans_d5_L_gemma31.jsonl --judge-model $DS --judge-extra-body "$JEB" --settings-name $JSETTING --concurrency 3 \
        --stub --passes 1 2 --cache $W/judge_cache_d5.jsonl --out $JOUT "$@" > $L/$N.judge_d5.log 2>&1
    else
      $G --stall-min 60 --log $L/$N.judge_d5.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $D5ITEMS --chunks $CHUNKS --source-chars $SRC \
        --instruction v2 --coverage --answers L=$W/ans_d5_L.jsonl L_gemma31=$W/ans_d5_L_gemma31.jsonl --judge-model $DS --judge-extra-body "$JEB" \
        --settings-name $JSETTING --concurrency 3 --judge-url "$OMLX_URL/v1" --passes 1 2 --cache $W/judge_cache_d5.jsonl --out $JOUT "$@"
    fi; }
  hb "$N: judging d5 started ($JCALLS judge rows over the sets L and L_gemma31; the cache holds earlier replies)"
  for round in 1 2; do
    extra=; [ $round = 2 ] && extra=--retry-invalid
    set +e; jrun $extra; rc=$?; set -e
    refresh_marker judge
    [ $rc = 0 ] || fail "judging d5 round $round failed (see $L/$N.judge_d5.log)"
    jbad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $JOUT)
    echo "judging d5 round $round: $jbad unscored"
    [ "$jbad" = 0 ] && break
    server_gate $DS check || refuse_late "the server is not free for the judge before round 2 (see above)"
  done
  .venv/bin/python $SAR judge-sets --file $JOUT --n $ND --decided L L_gemma31 > $W/judge_sets.txt || { cat $W/judge_sets.txt; fail "judging d5: an L set lacks a valid row for some scenario and pass ($JOUT)"; }
  cat $W/judge_sets.txt
  .venv/bin/python $R stage-calls --stage judge --kind judge --expect $JCALLS --files $JOUT || fail "the judge rows do not match the expected count"
  done_model judge $DS
  mark judge $JOUT $W/judge_sets.txt
  hb "$N: judging done ($JCALLS rows)"
fi

# ---------------------------------------------------------------- checks (CPU): the identifier check on D5 (A and L), the scorer on L's mechanical sets
if ! stage_done checks; then
  recheck checks
  OUTS=
  for x in "" _gemma31; do
    for arm in A L; do
      if [ $arm = A ]; then f=$STORED/ans_d5_A$x.jsonl; else f=$W/ans_d5_L$x.jsonl; fi
      rm -f $W/idcheck_d5_$arm$x.json     # this stage's own output of an earlier, unfinished attempt (the checker never overwrites a file)
      .venv/bin/python scripts/eval2_idcheck.py --answers $f --catalogues $CATS --totals-only --json $W/idcheck_d5_$arm$x.json > $W/idcheck_d5_$arm$x.txt || fail "the identifier check on $f failed"
      echo "idcheck d5 $arm$x: $(sed -n 4p $W/idcheck_d5_$arm$x.txt)"
      OUTS="$OUTS $W/idcheck_d5_$arm$x.json $W/idcheck_d5_$arm$x.txt"
    done
    for grp in ma m3; do
      .venv/bin/python scripts/eval2_score.py --items $STORED/items_$grp.jsonl --answers $W/ans_${grp}_L$x.jsonl --catalogues $CATS --scores $W/score_${grp}_L$x.jsonl \
        --summary $W/score_${grp}_L$x.summary.json > $W/score_${grp}_L$x.out 2> $W/score_${grp}_L$x.err || fail "the scorer on $grp L$x failed (see $W/score_${grp}_L$x.err)"
      echo "score $grp L$x: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print("all-pass", d["all_pass"], "of", d["items_scored"], "format failures", d["format_failures"])' $W/score_${grp}_L$x.summary.json)"
      OUTS="$OUTS $W/score_${grp}_L$x.jsonl"
    done
  done
  mark checks $OUTS
fi

# ---------------------------------------------------------------- the measures, the rule and the report (CPU)
STUBF=; [ $DRY = no ] || STUBF=--stub
.venv/bin/python $R measures --work $W --stored $STORED --rubrics $RUBS $STUBF > $W/measures.out 2> $W/measures.err || { cat $W/measures.err >&2; fail "the measures could not be computed (a missing measure is an error; see above)"; }
.venv/bin/python $R rule --measures $W/measures.json > $W/rule_out.txt 2> $W/rule_out.err || { cat $W/rule_out.err >&2; fail "the rule could not be applied: $(head -1 $W/rule_out.err)"; }
cat $W/rule_out.txt
FINAL=$(sed -n 's/^\(M7 KA2 LOOKUP CARDS (Z3) DONE.*\)$/\1/p' $W/rule_out.txt | tail -n 1)
[ -n "$FINAL" ] || fail "the rule printed no usable last line"
{
  cat $HEAD
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  cat $W/measures.md
  echo "### The rule applied by code (\`$R rule\`)"
  echo
  echo '```'; cat $W/rule_out.txt; echo '```'
  echo
  echo "### Judge rows per set"
  echo
  echo '```'; cat $W/judge_sets.txt; echo '```'
  echo
  echo "### What this report does not show"
  echo
  echo "Wrong, invented and harmful statements beyond the identifier check (no blind reading is part of Z3); the cards' effect with the search terms of a live retrieval (W20: the question's names only, on replayed packs; if anything it understates the cards); the thorough profile's checking pass with cards (not in this arm). D5 is worn development data; the hidden part is not involved. The time is the server's on this machine, one request at a time; the stored arm A was answered on 2026-11-24 in the six-arm run, so a change of the server's state between the two runs is in the paired time difference."
  echo
  echo "### Files"
  echo
  echo "Answers \`$W/ans_{d5,ma,m3}_L{,_gemma31}.jsonl\`; the stored arm A \`$STORED/ans_*_A{,_gemma31}.jsonl\`; the CPU rebuild \`$RB/\`; judge \`$JOUT\` (A's in \`$STORED/judge_d5.jsonl\`); identifier checks \`$W/idcheck_d5_*\`; scores \`$W/score_*_L*.jsonl\` (A's in \`$STORED\`); measures \`$W/measures.json\`."
  echo
  echo "$FINAL"
} > $REPORT.tmp
if [ -f $REPORT ] && cmp -s $REPORT.tmp $REPORT; then rm -f $REPORT.tmp; else mv $REPORT.tmp $REPORT; fi
if [ ! -f $L/$N.done ]; then
  echo "done; report $REPORT; $FINAL" > $L/$N.done
  hb "$N: done; report $REPORT; $FINAL" $([ $DRY = yes ] && echo no || echo yes)
fi
status DONE yes "$N done: $REPORT; $FINAL"
echo "$([ $DRY = yes ] && echo 'dry run ')done: report $REPORT"
echo "$FINAL"
