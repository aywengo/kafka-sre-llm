#!/bin/sh
# M7 release test chain (MILESTONE.md "The release test", W29, W33 to W35, W37, W39): the release configuration R (and the control C, and later an R variant) answer D7 and the hidden
# release look h6 through the live runtime, exactly the `ask ... --json` command of eval/m7/release_test/configs.json. Built and rehearsed (dry run, stub model, stub judge, stub
# server); the real run is a separate task. Nothing here starts a model by itself: the answers are server calls, one request at a time, under scripts/run_guarded.py.
#
# One work folder per set (WORK/d7, WORK/h6) with its own manifest, header, stage markers and files. Arms come from configs.json by name (--arms R,C by default; C can be dropped, and
# an arm such as an R variant can be added in a later invocation, before that set's reading bundles are made). Arms are answered grouped by model, the candidate's model first (all R
# arms before C), the sets inside each arm, so a model loads once per invocation.
#
# Phases (--phase; resumable by output files and stage markers; a line per item; h6: a counter only, no id, no text):
#   before-reading (default)
#     1. gate (CPU; refusal exit 2, nothing but the work folders written before it passes): the arms, the sets' files, the rule text; for h6 the ledger row (the token release-look-h6, the
#        configs sha256 and the rule text's sha256)
#     2. per set: the manifest (strict: configs entries of answered arms by their own hash, the set, seeds, rule text and script, the set's items and rubrics, the options; logged, not
#        compared: configs.json as a whole, the helper scripts, the chunk file) and the header (arms, hashes, seeds, rule text, for h6 the adjudication mode and who chose it) before any answer
#     3. per arm and set: kafka_sre_rag verify and the runtime files' hashes of configs.json, the server gate (W2: another client's model is never unloaded; W16: only the DeepSeek build,
#        logged; only what the gate prints as UNLOAD is unloaded; the marker is refreshed after every job), the answers, the packs (recovered on the CPU from the question and the returned
#        expansion; a mismatch is recorded as a failed pack and stops the chain, the answers are kept)
#     4. judge: two passes (DeepSeek, coverage role, instruction v2, one request at a time), a second round with --retry-invalid for invalid replies
#     5. reading bundles (--bundle-arms; default every arm of the folder): d7 anonymised and shuffled across the arms per scenario, first reader and a second family's export folder (D7 files
#        only); h6 sealed bundles, first reader only, no export. With one arm eval2_blind.py makes no d7 bundle. No reading is done here.
#   after-reading (--findings-d7 READER=PATH ..., --h6-dir DIR with the sealed findings; the Owner's verdicts when they exist)
#     6. unblind and the adjudication pack's input (every harm flag of either reader; the build command is printed, with --full-sets for h6 mode full and B4's re-read)
#     7. measures per set (h6: encrypted) and the report (H = harm the SRE found, M, coverage, omissions, wrong per 10 answers and per 1,000 words, seconds, cut share, paired R - C on complete
#        pairs, bars B1 to B4 as met / not met / not measured, W37; h6 totals only, also without the tagged item).
#
#   scripts/m7_release_test.sh --set d7,h6 --h6-mode full --h6-chosen-by "the Owner" > runs/m7/logs/m7_release_test.out 2>&1 &        (real; the conductor has written the ledger row)
#   scripts/m7_release_test.sh --dry-run --work DIR [--set d7] [--arms R] [--limit N]            (stub model, stub judge, stub server; nothing is written outside DIR)
# Options: --dry-run, --work DIR, --report FILE, --arms R,C, --candidate R, --control C, --set d7,h6, --phase before-reading|after-reading, --configs FILE, --ledger FILE, --chunks FILE,
#   --d7-items F --d7-rubrics F --h6-items F --h6-rubrics F, --h6-mode full|sample (default full) --h6-chosen-by TEXT, --second-fraction X (default 1), --h6-sessions N (default 6),
#   --judge-model ID, --bundle-arms R,C, --retry-failed-packs, --full-sets d7,h6 (B4's re-read: the pack input asks for every answer of those sets), --findings-d7 READER=PATH ..., --scope-d7 ...,
#   --h6-dir DIR, --first-reader NAME, --verdicts-d7 F --pack-key-d7 F --verdicts-h6 F.enc --pack-key-h6 F.enc, and for dry runs only: --limit N (the first N D7 items), --stub-loaded IDS.
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/ (never passed on a command line). Markers: runs/m7/logs/m7_release_test.done or .failed.
set -e
PY=${PY:-.venv/bin/python}
DRY=no; W=work/m7/release_test; REPORT=runs/m7/M7_RELEASE_TEST.md; ARMS=R,C; CAND=R; CTRL=C; SETS=d7; PHASE=before-reading
CONFIGS=eval/m7/release_test/configs.json; LEDGER=runs/m4/HIDDEN_LEDGER.md; CHUNKS=work/v3/chunks_s1.jsonl
D7I=eval/m7/d7/items.jsonl; D7R=eval/m7/d7/rubrics.jsonl; H6I=eval/hidden/tier_b_h6_items_final.jsonl.enc; H6R=eval/hidden/tier_b_h6_rubrics_final.jsonl.enc
SECOND=1; H6S=6; DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; FINDD7=; H6DIR=; FREADER=claude; VD7=; PKD7=; VH6=; PKH6=; SCOPED7=; LIMIT=0; STUBLOADED=; WSET=no; RSET=no
H6MODE=full; H6BY="the conductor (the default; the Owner has not chosen)"; BARMS=; RETRYPACKS=no; FULLSETS=
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;; --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --arms) ARMS=${2:?}; shift;; --candidate) CAND=${2:?}; shift;; --control) CTRL=${2:?}; shift;;
  --set) SETS=${2:?}; shift;; --phase) PHASE=${2:?}; shift;; --configs) CONFIGS=${2:?}; shift;; --ledger) LEDGER=${2:?}; shift;; --chunks) CHUNKS=${2:?}; shift;;
  --d7-items) D7I=${2:?}; shift;; --d7-rubrics) D7R=${2:?}; shift;; --h6-items) H6I=${2:?}; shift;; --h6-rubrics) H6R=${2:?}; shift;;
  --second-fraction) SECOND=${2:?}; shift;; --h6-sessions) H6S=${2:?}; shift;; --judge-model) DS=${2:?}; shift;; --first-reader) FREADER=${2:?}; shift;;
  --h6-mode) H6MODE=${2:?}; shift;; --h6-chosen-by) H6BY=${2:?}; shift;; --bundle-arms) BARMS=${2:?}; shift;; --retry-failed-packs) RETRYPACKS=yes;; --full-sets) FULLSETS=${2:?}; shift;;
  --findings-d7) shift; while [ $# -gt 0 ] && case $1 in --*) false;; *) true;; esac; do FINDD7="$FINDD7 $1"; shift; done; continue;;
  --scope-d7) shift; SCOPED7="$*"; break;;
  --h6-dir) H6DIR=${2:?}; shift;; --verdicts-d7) VD7=${2:?}; shift;; --pack-key-d7) PKD7=${2:?}; shift;; --verdicts-h6) VH6=${2:?}; shift;; --pack-key-h6) PKH6=${2:?}; shift;;
  --limit) LIMIT=${2:?}; shift;; --stub-loaded) STUBLOADED=${2:?}; shift;;
  *) echo "usage: scripts/m7_release_test.sh [--dry-run] [--work DIR] [--arms R,C] [--set d7,h6] [--phase before-reading|after-reading] (see the header)" >&2; exit 2;; esac
  shift; done
N=m7_release_test; L=runs/m7/logs; H=scripts/m7_release_test.py; RULE=scripts/m7_release_rule.py; KB2=scripts/m7_kb2_rule.py
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
case $PHASE in before-reading|after-reading) ;; *) refuse "--phase is before-reading or after-reading";; esac
case $H6MODE in full|sample) ;; *) refuse "--h6-mode is full or sample";; esac
case $LIMIT in ''|*[!0-9]*) refuse "--limit is a whole number";; esac
[ $DRY = yes ] || [ "$LIMIT" = 0 ] || refuse "--limit is for dry runs only"
[ $DRY = yes ] || [ -z "$STUBLOADED" ] || refuse "--stub-loaded is for dry runs only"
echo "$SETS" | tr ',' '\n' | while read s; do case $s in d7|h6) ;; *) echo "REFUSED: --set names d7 and h6 only" >&2; exit 2;; esac; done || exit 2
case ",$SETS," in *,h6,*) HAS_H6=yes;; *) HAS_H6=no;; esac
[ -f "$CONFIGS" ] || refuse "configs file $CONFIGS is missing"
CSHA=$($PY -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$CONFIGS")
ARMOUT=$($PY $H arms --configs "$CONFIGS" --arms "$ARMS") || refuse "the arms are not usable (see above)"
echo "$ARMOUT" | grep -v '^MODELS'
MODELS=$(echo "$ARMOUT" | sed -n 's/^MODELS //p')
case ",$ARMS," in *",$CAND,"*) ;; *) refuse "the candidate $CAND is not an arm";; esac
ORDARMS=$($PY $H arm-order --configs "$CONFIGS" --arms "$ARMS" --candidate $CAND) || refuse "the arms could not be ordered"
RSHA=$($PY $RULE rule-text --sha256 --candidate $CAND --control $CTRL --h6-mode $H6MODE --h6-chosen-by "$H6BY") || refuse "the rule text could not be made"
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m7_release_test.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/M7_RELEASE_TEST_from_stub_data.md
  case "$REPORT $W" in *runs/m7/M7_RELEASE_TEST.md*|*work/m7/release_test*) refuse "a dry run never uses the real report or work folder";; esac
  G="$PY scripts/run_guarded.py --other-retries 0"; G0="$G"; STUBF="--stub"; STUBFILE=$W/stub_server.json
else
  G="$PY scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"; G0="$PY scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 0"; STUBF=""; STUBFILE=
fi
ANS_STALL=70      # minutes: above two ask timeouts (1,800 s each)
# ---- the sets' inputs (CPU; refuses before anything is written, except the work folders)
mkdir -p $W $L
set_files() { # SET: ITEMS RUBS EXT SW, and for d7 the harness-shaped items (a dry run's limit takes the first N)
  SW=$W/$1; mkdir -p $SW
  case $1 in
    d7) [ -f "$D7I" ] || refuse "D7 items $D7I are missing"; ITEMS=$SW/items_d7_h.jsonl; RUBS=$D7R; EXT=
        if [ ! -s $ITEMS ]; then
          if [ "$LIMIT" = 0 ]; then cp "$D7I" $SW/d7_items_src.jsonl; else head -n $LIMIT "$D7I" > $SW/d7_items_src.jsonl; fi
          $PY scripts/m5_g_rule.py harness-items --in $SW/d7_items_src.jsonl --out $ITEMS --domain d7 > /dev/null || refuse "the D7 items could not be put in the harness shape"
        fi
        if [ "$LIMIT" != 0 ]; then head -n $LIMIT "$D7R" > $SW/rubrics_d7_limited.jsonl; RUBS=$SW/rubrics_d7_limited.jsonl; fi;;
    h6) ITEMS=$H6I; RUBS=$H6R; EXT=.enc;;
  esac; }
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s
  $PY $H check-inputs --set $s --items "$ITEMS" --rubrics "$RUBS" $STUBF || refuse "the inputs of $s are not what the chain expects (see above)"
done
[ -f "$CHUNKS" ] || refuse "the chunk file $CHUNKS is missing"
# the h6 ledger: only when an h6 answer stage is still to run
if [ $HAS_H6 = yes ] && [ $PHASE = before-reading ]; then
  H6TODO=no; for arm in $ORDARMS; do [ -f $W/h6/stage_ans_$arm.done ] || H6TODO=yes; done
  if [ $H6TODO = yes ]; then
    [ -f "$LEDGER" ] || refuse "the hidden ledger $LEDGER is missing: no h6 answer before the conductor's row exists"
    $PY $H ledger-check --configs "$CONFIGS" --ledger "$LEDGER" --configs-sha256 "$CSHA" --rule-sha256 "$RSHA" || refuse "the hidden ledger has no row with the token release-look-h6, the configs sha256 $CSHA and the rule text's sha256 $RSHA (the conductor writes it before the look)"
  fi
fi
# ---- the server (real runs)
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
fi
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
fail() { echo "STOPPED: $1"; mkdir -p $L; echo "$1" >> $L/$N.failed; hb "$N: STOPPED: $1" yes; exit 1; }
refuse_late() { echo "REFUSED: $1" >&2; mkdir -p $L; echo "REFUSED: $1" >> $L/$N.failed; hb "$N: REFUSED: $1" yes; exit 2; }
# any other exit with a failure status (a set -e exit, a signal) leaves a .failed marker too
trap 'rc=$?; if [ $rc != 0 ] && [ ! -e "$L/$N.failed" ]; then mkdir -p "$L"; echo "exited with status $rc" >> "$L/$N.failed"; fi' EXIT
stage_done() { [ -f $SW/stage_$1.done ]; }
mark() { st=$1; shift; $PY $KB2 mark --work $SW --stage $st --out "$@" > /dev/null; }
# ---- per set: the manifest (strict: the set, seeds, rule text and script, items and rubrics, the options, each answered configuration's entry; logged: the rest) and the header
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s
  PINS="--pin $([ $s = d7 ] && echo $D7I || echo $H6I) --pin $([ $s = d7 ] && echo $D7R || echo $H6R)"
  LPINS="--log-pin scripts/m7_release_test.py --log-pin scripts/m7_release_test.sh --log-pin scripts/m7_adjudication_pack.py --log-pin $CHUNKS --log-pin kafka_sre_rag/frozen.json"
  $PY $H manifest --work $SW --set $s --configs "$CONFIGS" --arms "$ARMS" --candidate $CAND --control $CTRL --h6-mode $H6MODE --h6-chosen-by "$H6BY" $PINS $LPINS \
    --opt "second_fraction=$SECOND" --opt "h6_sessions=$H6S" --opt "judge_model=$DS" --opt "judge_concurrency=1" --opt "limit=$LIMIT" --opt "dry=$DRY" || refuse "the manifest check of $s failed (see above; nothing was moved or deleted)"
  MH=$($PY -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest()[:16])' $SW/MANIFEST.json)
  if [ $PHASE = before-reading ] && ! stage_done header; then
    for f in $SW/ans_*; do [ ! -e "$f" ] || refuse "$f exists but the header of $s was not written yet; the rule text must come first"; done
    {
      echo "## Set $s$([ $DRY = yes ] && echo ' (DRY RUN: stub model, stub judge, stub server; no number is a measurement)')"
      echo
      echo "Written by \`scripts/m7_release_test.sh\` on $(date +%Y-%m-%d). Arms: $ARMS (configurations by name from \`$CONFIGS\`, whose sha256 is \`$CSHA\`; each configuration's entry hash is in the manifest, which is strict for answered configurations only). Manifest hash $MH. Seeds: $($PY -c 'import sys; sys.path.insert(0,"scripts"); import m7_release_test as t; print(", ".join(f"{k} {v}" for k, v in t.SEEDS.items()))'). Judge: $DS, concurrency 1 (W21), two passes. Rule text sha256 \`$RSHA\`."
      if [ $s = h6 ]; then echo; echo "**h6 adjudication mode: \`$H6MODE\`, chosen by $H6BY** (fixed here, before any h6 answer; it changes the SRE's time: \`full\` is every answer, \`sample\` every flag plus 20 unflagged answers per configuration)."; fi
      echo
      $PY $RULE rule-text --candidate $CAND --control $CTRL --h6-mode $H6MODE --h6-chosen-by "$H6BY"
      echo
      echo "This header, with the rule, was written before any answer of this set existed (stage marker \`$SW/stage_header.done\`)."
    } > $SW/report_header.md
    echo "$ARMS" | tr ',' '\n' > $SW/header_arms.txt
    mark header $SW/report_header.md
    hb "$N: header of $s with the rule text written; no answer exists yet"
  elif [ $PHASE = before-reading ]; then
    for arm in $(echo $ARMS | tr ',' ' '); do
      grep -qx "$arm" $SW/header_arms.txt 2>/dev/null || { echo "$(date +%Y-%m-%d): arm $arm added to this set's folder before its answers (configuration entry hash in the manifest); the rule text is unchanged" >> $SW/header_log.md; echo "$arm" >> $SW/header_arms.txt; }
    done
  fi
done
mkdir -p "$(dirname $REPORT)"
# ---- the server helpers (real: the server; dry: a stub status file)
if [ $DRY = yes ]; then
  $PY $KB2 stub-status --file $STUBFILE --init --loaded "$STUBLOADED" > /dev/null
  SV="--stub-file $STUBFILE"
else SV=""; fi
mid_marker() { echo "$W/loaded_$(echo "$1" | tr -c 'A-Za-z0-9.\n-' '_')"; }
marker_args() { o="--marker $DS=$(mid_marker $DS)"; for m in $MODELS; do o="$o --marker $m=$(mid_marker $m)"; done; echo $o; }
apply_gate() { # the gate's decisions: only what it prints as UNLOAD (ours, by marker and by the server's load time) or W16 (the DeepSeek build of another client) is unloaded
  for m in $(echo "$gout" | sed -n 's/^UNLOAD //p'); do
    $PY $H server unload --model $m $SV || fail "the unload of $m failed"; rm -f $(mid_marker $m); hb "$N: unloaded $m (ours, not needed by the next stage)"; done
  for m in $(echo "$gout" | sed -n 's/^W16 //p'); do
    $PY $H server unload --model $m $SV || fail "the W16 unload of $m failed"; hb "$N: unloaded $m under W16 (the Owner's decision of 2026-11-24: the DeepSeek build, another client's, blocks this chain)"; done
  for m in $(echo "$gout" | sed -n 's/^ADOPT //p'); do hb "$N: $m was loaded by another client and is used as it is (W16); it stays loaded"; done; }
gate_for() { # MODEL: the server before a model's stages: W2 (a foreign model or an active request refuses)
  gout=$($PY $H server gate --work $W --need "$1" $(marker_args) $SV 2>&1) || { echo "$gout" >&2; refuse_late "the server is not free for $1 (another client's model, or a request active or waiting)"; }
  apply_gate
  case "$gout" in *"ADOPT $1"*) ;; *) touch $(mid_marker $1); hb "$N: $1 loads on its first request (alone)";; esac; }
after_job() { # MODEL: the model loaded during the job, after the marker was written: the marker is refreshed (the gate refuses a load later than its marker); a dry run's stub server loads it now
  [ -e $(mid_marker $1) ] || return 0
  [ $DRY = no ] || $PY $H server load --model $1 $SV
  touch $(mid_marker $1); }
release_all() { gout=$($PY $H server gate --work $W --need none-needed $(marker_args) $SV 2>&1) || { echo "$gout" >&2; refuse_late "the server could not be released (see above)"; }; apply_gate; }
mid_of() { $PY -c 'import json,sys; c=json.load(open(sys.argv[1])); print(c[sys.argv[2]]["model"]["oMLX_model_id"])' "$CONFIGS" "$1"; }

if [ $PHASE = before-reading ]; then
rm -f $L/$N.failed $L/$N.done
hb "$N: started; arms $ARMS, sets $SETS, configs sha256 $CSHA"
[ $DRY = no ] || export M7_STUB_CHUNKS="$CHUNKS"
VERIFIED=
pre_arm() { # ARM: verify and the runtime files' hashes, once per arm and invocation
  case " $VERIFIED " in *" $1 "*) return 0;; esac
  $PY $H runtime-check --configs "$CONFIGS" --arm $1 || refuse_late "the runtime files differ from configs.json for $1"
  if [ $DRY = no ]; then $PY -m kafka_sre_rag verify > $W/verify_$1.out 2>&1 || { cat $W/verify_$1.out >&2; fail "kafka_sre_rag verify failed before the stage of $1"; }
    echo "verify before $1: $(grep -c match $W/verify_$1.out) lines match"; else echo "dry run: kafka_sre_rag verify not run (it needs the real index); run it separately"; fi
  VERIFIED="$VERIFIED $1"; }
for arm in $ORDARMS; do
  for s in $(echo $SETS | tr ',' ' '); do
    set_files $s; LDG=; [ $s != h6 ] || LDG="--ledger $LEDGER --rule-sha256 $RSHA"
    OUT=$SW/ans_${s}_$arm.jsonl$EXT
    if ! stage_done ans_$arm; then
      pre_arm $arm
      gate_for "$(mid_of $arm)"
      hb "$N: answers $arm on $s started"
      $G --stall-min $ANS_STALL --log $L/$N.ans_${s}_$arm.log -- $PY $H answer --configs "$CONFIGS" --arm $arm --set $s --items "$ITEMS" --out "$OUT" --python "$PY" $LDG $STUBF \
        || fail "the answers of $arm on $s failed (see $L/$N.ans_${s}_$arm.log)"
      after_job "$(mid_of $arm)"
      mark ans_$arm $OUT
      hb "$N: answers $arm on $s done"
    fi
    if ! stage_done packs_$arm; then
      RES=retrieval; [ $DRY = no ] || RES=keys
      RF=; [ $RETRYPACKS = no ] || RF=--retry-failed
      $G0 --stall-min 30 --log $L/$N.packs_${s}_$arm.log -- $PY $H packs --set $s --arm $arm --items "$ITEMS" --answers "$OUT" --chunks "$CHUNKS" --chunks-override --resolver $RES $STUBF $RF \
        || fail "the packs of $arm on $s could not be recovered exactly (see $L/$N.packs_${s}_$arm.log); the answers are kept"
      mark packs_$arm $OUT
    fi
  done
done
# ---- judge: the gate unloads the answer models (ours) and loads the judge (W2, W16); two rounds, the second with --retry-invalid
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s; HID=; [ $s != h6 ] || HID=--hidden
  MARMS=$($PY $H manifest-arms --work $SW)
  PENDING=; for arm in $MARMS; do { stage_done ans_$arm && stage_done packs_$arm; } || PENDING="$PENDING $arm"; done
  if [ -n "$PENDING" ]; then echo "set $s: arms still to answer in this folder:$PENDING; its judge and bundles wait"; continue; fi
  JSTAGE=judge_$(echo $MARMS | tr ' ' '_')
  if ! stage_done $JSTAGE; then
    gate_for "$DS"
    SETARGS=; for arm in $MARMS; do SETARGS="$SETARGS $arm=$SW/ans_${s}_$arm.jsonl$EXT"; done
    JOUT=$SW/judge_$s.jsonl$EXT; JCACHE=$SW/judge_cache_$s.jsonl$EXT
    JMODE=; [ $DRY = no ] || JMODE="--stub"          # the judge reads OMLX_URL and JUDGE_API_KEY from the environment: no address on a command line or in a log
    NIT=$($PY $H check-inputs --set $s --items "$ITEMS" --rubrics "$RUBS" $STUBF | sed -n 's/^[a-z0-9]*: \([0-9]*\) items.*/\1/p')
    NARM=$(echo $MARMS | wc -w | tr -d ' ')
    hb "$N: judging $s started (two passes, $DS, concurrency 1)"
    for round in 1 2; do
      EXTRA=; [ $round = 1 ] || EXTRA="--retry-invalid"
      $G --stall-min 60 --log $L/$N.judge_${s}_round$round.log -- $PY $H judge-run $HID --python "$PY" -- --rubrics "$RUBS" --items "$ITEMS" --chunks "$CHUNKS" --source-chars 12000 --instruction v2 --coverage \
        --answers $SETARGS --judge-model $DS --judge-extra-body "$JEB" --settings-name deepseek-nothink-12k --concurrency 1 $JMODE --passes 1 2 --cache $JCACHE --out $JOUT $EXTRA \
        || fail "judging $s round $round failed (see $L/$N.judge_${s}_round$round.log)"
      after_job "$DS"
      if $PY $H judge-check --out $JOUT --expect $((NIT * NARM * 2)); then break; fi
      echo "judging $s round $round: unscored rows remain"
      [ $round = 1 ] || fail "the judge rows of $s are not whole after the second round"
    done
    mark $JSTAGE $JOUT
    hb "$N: judging $s done"
  fi
done
release_all
# ---- the reading bundles (no reading is done here)
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s
  MARMS=$($PY $H manifest-arms --work $SW)
  JSTAGE=judge_$(echo $MARMS | tr ' ' '_'); stage_done $JSTAGE || continue
  BA=${BARMS:-$(echo $MARMS | tr ' ' ',')}
  if ! stage_done bundle; then
    SETARGS=; for arm in $(echo $BA | tr ',' ' '); do SETARGS="$SETARGS $arm=$SW/ans_${s}_$arm.jsonl$EXT"; done
    if [ $s = d7 ]; then
      $PY $H bundle --configs "$CONFIGS" --arms "$BA" --set d7 --items "$ITEMS" --rubrics "$RUBS" --answers $SETARGS --chunks "$CHUNKS" --out $SW/reading --second-fraction $SECOND || fail "the d7 bundles could not be made"
      if [ -f $SW/reading/KEY.json ]; then mark bundle $SW/reading/KEY.json; else mark bundle $SW/reading/NO_BUNDLE_single_arm.txt; fi
    else
      $PY $H bundle --configs "$CONFIGS" --arms "$BA" --set h6 --items "$ITEMS" --rubrics "$RUBS" --answers $SETARGS --chunks "$CHUNKS" --out $SW/reading --sessions $H6S || fail "the h6 sealed bundles could not be made"
      mark bundle $SW/reading/blind_key.json.enc
    fi
    echo "$BA" > $SW/bundle_arms.txt
  fi
done
echo "before-reading done: the readers read the bundles; then run this script with --phase after-reading"
if [ $DRY = yes ]; then echo M7_RELEASE_TEST_DRY_RUN_BEFORE_READING_DONE >> $L/$N.done; else echo M7_RELEASE_TEST_BEFORE_READING_DONE >> $L/$N.done; fi
hb "$N: before-reading done (answers, judge, bundles); the readers follow" yes
exit 0
fi

# ================================================================ after-reading
MEAS=
FS=$FULLSETS; [ $HAS_H6 = no ] || [ $H6MODE != full ] || FS="${FS:+$FS,}h6"
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s
  stage_done bundle || refuse "the before-reading phase is not done for $s (no bundle stage marker in $SW)"
  BA=$(cat $SW/bundle_arms.txt)
  SETARGS=; for arm in $(echo $BA | tr ',' ' '); do SETARGS="$SETARGS $arm=$SW/ans_${s}_$arm.jsonl$EXT"; done
  FIND=; VERD=; PK=
  if [ $s = d7 ]; then
    if [ -n "$FINDD7" ]; then
      $PY $H unblind --set d7 --key $SW/reading/KEY.json --rubrics "$RUBS" --answers $SETARGS --findings $FINDD7 --out $SW/findings_d7.jsonl --reads-out $SW/reads_d7.jsonl \
        $([ -z "$SCOPED7" ] || echo "--scope $SCOPED7") || fail "the d7 findings could not be unblinded (see above)"
      FIND="--findings $SW/findings_d7.jsonl"
      $PY $H pack-input --configs "$CONFIGS" --arms "$BA" --set d7 --items "$ITEMS" --answers $SETARGS --findings $SW/findings_d7.jsonl --out-dir $SW/pack_input --full-sets "$FS" || fail "the d7 pack input could not be made"
    fi
    [ -z "$VD7" ] || VERD="--verdicts $VD7"; [ -z "$PKD7" ] || PK="--pack-key $PKD7"; OUTM=$SW/measures_d7.json
  else
    if [ -n "$H6DIR" ]; then
      $PY $H unblind --set h6 --dir "$H6DIR" --out $SW/findings_h6.jsonl.enc --reader $FREADER || fail "the h6 findings could not be unblinded"
      FIND="--findings $SW/findings_h6.jsonl.enc"
      $PY $H pack-input --configs "$CONFIGS" --arms "$BA" --set h6 --items "$ITEMS" --answers $SETARGS --findings $SW/findings_h6.jsonl.enc --out-dir $SW/pack_input --full-sets "$FS" || fail "the h6 pack input could not be made"
    fi
    [ -z "$VH6" ] || VERD="--verdicts $VH6"; [ -z "$PKH6" ] || PK="--pack-key $PKH6"; OUTM=$SW/measures_h6.json.enc
  fi
  $PY $H measures --set $s --arms "$BA" --items "$ITEMS" --rubrics "$RUBS" --answers $SETARGS --judge $SW/judge_$s.jsonl$EXT $FIND --first-reader $FREADER $VERD $PK --out $OUTM || fail "the measures of $s could not be made"
  MEAS="$MEAS $s=$OUTM"
done
ALLARMS=$(echo $SETS | tr ',' ' ' | { read f rest; cat $W/$f/bundle_arms.txt; })
$PY $RULE report --measures $MEAS --arms "$ALLARMS" --candidate $CAND --control $CTRL --out $W/report_body.md --json-out $W/result.json > $W/rule_out.txt || fail "the report could not be made"
FINAL=$(tail -n 1 $W/rule_out.txt)
{
  echo "# M7 release test: the release configuration against the current release$([ $DRY = yes ] && echo ' (DRY RUN: stub data)')"
  echo
  for s in $(echo $SETS | tr ',' ' '); do cat $W/$s/report_header.md; [ ! -f $W/$s/header_log.md ] || { echo; cat $W/$s/header_log.md; }; echo; done
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  cat $W/report_body.md
  echo
  echo "### What this report does not show"
  echo
  echo "The readers' findings, the SRE's verdicts and the hidden items are not in this report: h6 shows totals only. Bars read \`not measured\` until the SRE's verdicts are complete. The adjudicator is the project's owner, not independent of the rubrics (W34)."
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
echo "$FINAL"
if [ $DRY = yes ]; then echo M7_RELEASE_TEST_DRY_RUN_DONE >> $L/$N.done; echo "dry run done: report from stub data in $REPORT"; else echo M7_RELEASE_TEST_DONE >> $L/$N.done; hb "$N: done; report $REPORT; $FINAL" yes; fi
