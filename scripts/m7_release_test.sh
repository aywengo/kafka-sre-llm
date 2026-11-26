#!/bin/sh
# M7 release test chain (MILESTONE.md "The release test", W29, W33 to W35, W37): the release configuration R (and the control C) answer D7 and the hidden release look h6
# through the live runtime, exactly the `ask ... --json` command of eval/m7/release_test/configs.json. Built and rehearsed (dry run, stub model, stub judge) now; the real run is a separate task.
# Nothing here starts a model by itself: the answers are server calls, one request at a time, under scripts/run_guarded.py.
#
# Phases (--phase; each resumable by its output files, stage markers W/stage_NAME.done, a line per item; h6: a counter only, no id, no text):
#   before-reading (default)
#     1. gate (CPU; refusal exit 2, nothing but the work folder written before it passes): the arms from configs.json (--arms R,C by default; C can be dropped without a code change),
#        the configs sha256 against the manifest (an amended config means a new manifest), the set files, for h6 the ledger row (token release-look-h6 and the configs sha256), the server
#     2. manifest and report header: the arm list, the configs sha256, the sets, the seeds and the rule text (measures, bars, the W37 wording) before the first answer exists
#     3. per set (d7, then h6) and arm: kafka_sre_rag verify, the server gate (W2: another client's model is never unloaded; W16: only the DeepSeek build, logged), the answers, the packs
#        (recovered on the CPU from the question and the returned expansion, equal to the returned sources or the row fails)
#     4. judge: two passes (DeepSeek, coverage role, instruction v2) against the D7 rubrics or the sealed h6 rubrics; the answer models are unloaded first
#     5. reading bundles: d7 anonymised and shuffled across the arms per scenario (seed in the manifest), first reader and a second family's export folder (D7 files only); h6 sealed bundles,
#        first reader only, no export. No reading is done here.
#   after-reading (the findings exist: --findings-d7 READER=PATH ..., --h6-dir DIR with the sealed findings; optionally the Owner's verdicts)
#     6. unblind, the adjudication pack's input (every harm flag of either reader; 20 random unflagged answers per set and configuration are drawn by the pack build, seeds in the header)
#     7. measures per set (h6: encrypted) and the report: H, M, coverage, omissions, wrong per 10 answers and per 1,000 words, seconds, cut share, paired R - C (bootstrap, 10,000, seed 0),
#        bars B1 to B4 as met / not met / not measured (W37), h6 totals also without the tagged item. Report runs/m7/M7_RELEASE_TEST.md (a dry run: in its work folder).
#
#   scripts/m7_release_test.sh --set d7,h6 > runs/m7/logs/m7_release_test.out 2>&1 &        (real; the conductor has written the ledger row)
#   scripts/m7_release_test.sh --dry-run --work DIR [--set d7] [--arms R] [--limit N]         (stub model, stub judge, stub server; nothing is written outside DIR)
# Options: --dry-run, --work DIR, --report FILE, --arms R,C, --candidate R, --control C, --set d7,h6, --phase before-reading|after-reading, --configs FILE, --ledger FILE, --chunks FILE,
#   --d7-items F --d7-rubrics F --h6-items F --h6-rubrics F, --second-fraction X (default 1), --h6-sessions N (default 6), --judge-model ID, --findings-d7 READER=PATH ... (after-reading),
#   --h6-dir DIR, --first-reader NAME, --verdicts-d7 F --pack-key-d7 F --verdicts-h6 F --pack-key-h6 F, and for dry runs only: --limit N (the first N items), --stub-loaded IDS (models the stub server holds).
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/. Markers: runs/m7/logs/m7_release_test.done or .failed (a dry run: in its work folder).
set -e
PY=${PY:-.venv/bin/python}
DRY=no; W=work/m7/release_test; REPORT=runs/m7/M7_RELEASE_TEST.md; ARMS=R,C; CAND=R; CTRL=C; SETS=d7; PHASE=before-reading
CONFIGS=eval/m7/release_test/configs.json; LEDGER=runs/m4/HIDDEN_LEDGER.md; CHUNKS=work/v3/chunks_s1.jsonl
D7I=eval/m7/d7/items.jsonl; D7R=eval/m7/d7/rubrics.jsonl; H6I=eval/hidden/tier_b_h6_items_final.jsonl.enc; H6R=eval/hidden/tier_b_h6_rubrics_final.jsonl.enc
SECOND=1; H6S=6; DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; FINDD7=; H6DIR=; FREADER=claude; VD7=; PKD7=; VH6=; PKH6=; SCOPED7=; LIMIT=0; STUBLOADED=; WSET=no; RSET=no
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;; --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --arms) ARMS=${2:?}; shift;; --candidate) CAND=${2:?}; shift;; --control) CTRL=${2:?}; shift;;
  --set) SETS=${2:?}; shift;; --phase) PHASE=${2:?}; shift;; --configs) CONFIGS=${2:?}; shift;; --ledger) LEDGER=${2:?}; shift;; --chunks) CHUNKS=${2:?}; shift;;
  --d7-items) D7I=${2:?}; shift;; --d7-rubrics) D7R=${2:?}; shift;; --h6-items) H6I=${2:?}; shift;; --h6-rubrics) H6R=${2:?}; shift;;
  --second-fraction) SECOND=${2:?}; shift;; --h6-sessions) H6S=${2:?}; shift;; --judge-model) DS=${2:?}; shift;; --first-reader) FREADER=${2:?}; shift;;
  --findings-d7) shift; while [ $# -gt 0 ] && case $1 in --*) false;; *) true;; esac; do FINDD7="$FINDD7 $1"; shift; done; continue;;
  --scope-d7) shift; SCOPED7="$*"; break;;
  --h6-dir) H6DIR=${2:?}; shift;; --verdicts-d7) VD7=${2:?}; shift;; --pack-key-d7) PKD7=${2:?}; shift;; --verdicts-h6) VH6=${2:?}; shift;; --pack-key-h6) PKH6=${2:?}; shift;;
  --limit) LIMIT=${2:?}; shift;; --stub-loaded) STUBLOADED=${2:?}; shift;;
  *) echo "usage: scripts/m7_release_test.sh [--dry-run] [--work DIR] [--arms R,C] [--set d7,h6] [--phase before-reading|after-reading] (see the header)" >&2; exit 2;; esac
  shift; done
N=m7_release_test; L=runs/m7/logs; H=scripts/m7_release_test.py; RULE=scripts/m7_release_rule.py; KB2=scripts/m7_kb2_rule.py
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
case $PHASE in before-reading|after-reading) ;; *) refuse "--phase is before-reading or after-reading";; esac
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
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m7_release_test.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/M7_RELEASE_TEST_from_stub_data.md
  case "$REPORT $W" in *runs/m7/M7_RELEASE_TEST.md*|*work/m7/release_test*) refuse "a dry run never uses the real report or work folder";; esac
  G="$PY scripts/run_guarded.py --other-retries 0"; STUBF="--stub"; STUBFILE=$W/stub_server.json
else
  G="$PY scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"; STUBF=""; STUBFILE=
fi
# ---- the sets' inputs (CPU; refuses before anything is written, except the work folder of a dry run)
mkdir -p $W $L
case ",$SETS," in *,d7,*)     # D7's items in the harness shape (as the six-arm chain does for D5: scripts/m5_g_rule.py harness-items); a dry run's limit takes the first N
  [ -f "$D7I" ] || refuse "D7 items $D7I are missing"
  if [ ! -s $W/items_d7_h.jsonl ]; then
    if [ "$LIMIT" = 0 ]; then cp "$D7I" $W/d7_items_src.jsonl; else head -n $LIMIT "$D7I" > $W/d7_items_src.jsonl; fi
    $PY scripts/m5_g_rule.py harness-items --in $W/d7_items_src.jsonl --out $W/items_d7_h.jsonl --domain d7 > /dev/null || refuse "the D7 items could not be put in the harness shape"
  fi
  if [ "$LIMIT" != 0 ]; then head -n $LIMIT "$D7R" > $W/rubrics_d7_limited.jsonl; D7R=$W/rubrics_d7_limited.jsonl; fi;; esac
set_files() { case $1 in d7) ITEMS=$W/items_d7_h.jsonl; RUBS=$D7R; EXT=;; h6) ITEMS=$H6I; RUBS=$H6R; EXT=.enc;; esac; }
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s
  $PY $H check-inputs --set $s --items "$ITEMS" --rubrics "$RUBS" $STUBF || refuse "the inputs of $s are not what the chain expects (see above)"
done
if [ $HAS_H6 = yes ] && [ $PHASE = before-reading ]; then
  [ -f "$LEDGER" ] || refuse "the hidden ledger $LEDGER is missing: no h6 answer before the conductor's row exists"
  $PY $H ledger-check --configs "$CONFIGS" --ledger "$LEDGER" --configs-sha256 "$CSHA" || refuse "the hidden ledger has no row with the token release-look-h6 and the configs sha256 $CSHA (the conductor writes it before the look)"
fi
[ -f "$CHUNKS" ] || refuse "the chunk file $CHUNKS is missing"
# ---- the server (real runs)
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
fi
mkdir -p $W $L
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
fail() { echo "STOPPED: $1"; mkdir -p $L; echo "$1" >> $L/$N.failed; hb "$N: STOPPED: $1" yes; exit 1; }
refuse_late() { echo "REFUSED: $1" >&2; mkdir -p $L; echo "REFUSED: $1" >> $L/$N.failed; hb "$N: REFUSED: $1" yes; exit 2; }
stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; $PY $KB2 mark --work $W --stage $st --out "$@" > /dev/null; }
# ---- manifest (before any answer) and the header
PINS="--pin $CONFIGS --pin $D7I --pin $D7R --pin scripts/m7_release_test.py --pin scripts/m7_release_rule.py --pin scripts/m7_release_test.sh --pin kafka_sre_rag/frozen.json --pin $CHUNKS"
[ $HAS_H6 = no ] || PINS="$PINS --pin $H6I --pin $H6R"
if [ $PHASE = before-reading ]; then
  $PY $H manifest --work $W --configs "$CONFIGS" --arms "$ARMS" --sets "$SETS" --candidate $CAND --control $CTRL $PINS \
    --opt "second_fraction=$SECOND" --opt "h6_sessions=$H6S" --opt "judge_model=$DS" --opt "limit=$LIMIT" --opt "dry=$DRY" || refuse "the manifest check failed (see above; nothing was moved or deleted)"
else
  [ -f $W/MANIFEST.json ] || refuse "the after-reading phase needs the manifest of the first phase in $W"
  $PY $H manifest --work $W --configs "$CONFIGS" --arms "$ARMS" --sets "$SETS" --candidate $CAND --control $CTRL $PINS \
    --opt "second_fraction=$SECOND" --opt "h6_sessions=$H6S" --opt "judge_model=$DS" --opt "limit=$LIMIT" --opt "dry=$DRY" || refuse "the manifest differs from the state now (see above)"
fi
MH=$($PY -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest()[:16])' $W/MANIFEST.json)
HEAD=$W/report_header.md
if [ $PHASE = before-reading ] && ! stage_done header; then
  for f in $W/ans_*; do [ ! -e "$f" ] || refuse "$f exists but the report header was not written yet; the rule text must come first"; done
  [ $DRY = yes ] || [ ! -e $REPORT ] || refuse "$REPORT exists; it is not overwritten (move it away first)"
  {
    echo "# M7 release test: the release configuration against the current release on D7$([ $HAS_H6 = yes ] && echo ' and the hidden release look (h6)')$([ $DRY = yes ] && echo ' (DRY RUN: stub model, stub judge, stub server; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/m7_release_test.sh\` on $(date +%Y-%m-%d). Arms: $ARMS. Configs: \`$CONFIGS\`, sha256 \`$CSHA\`. Sets: $SETS. Manifest hash $MH. Seeds: $($PY -c 'import sys; sys.path.insert(0,"scripts"); import m7_release_test as t; print(", ".join(f"{k} {v}" for k, v in t.SEEDS.items()))'). Rule: \`$RULE\`."
    echo
    $PY $RULE rule-text --arms "$ARMS" --sets "$SETS" --configs-sha256 "$CSHA" --candidate $CAND --control $CTRL
    echo
    echo "This header, with the rule, was written before any answer of the run existed (stage marker \`$W/stage_header.done\`)."
  } > $HEAD
  mkdir -p "$(dirname $REPORT)"; cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header with the rule text written ($REPORT); no answer exists yet"
fi
# ---- the server helpers (real: the server; dry: a stub status file)
if [ $DRY = yes ]; then
  $PY $KB2 stub-status --file $STUBFILE --init --loaded "$STUBLOADED" > /dev/null
  SV="--stub-file $STUBFILE"
else SV=""; fi
mid_marker() { echo "$W/loaded_$(echo "$1" | tr -c 'A-Za-z0-9.\n-' '_')"; }
marker_args() { o="--marker $DS=$(mid_marker $DS)"; for m in $MODELS; do o="$o --marker $m=$(mid_marker $m)"; done; echo $o; }
gate_for() { # MODEL: the server before a model's stages: W2 (a foreign model or an active request refuses), W16 (only the DeepSeek build is unloaded, logged)
  gout=$($PY $H server gate --work $W --need "$1" $(marker_args) $SV 2>&1) || { echo "$gout" >&2; refuse_late "the server is not free for $1 (another client's model, or a request active or waiting)"; }
  for m in $(echo "$gout" | sed -n 's/^UNLOAD //p'); do
    $PY $H server unload --model $m $SV || fail "the unload of $m failed"; rm -f $(mid_marker $m); hb "$N: unloaded $m (ours, not needed by the next stage)"; done
  for m in $(echo "$gout" | sed -n 's/^W16 //p'); do
    $PY $H server unload --model $m $SV || fail "the W16 unload of $m failed"; hb "$N: unloaded $m under W16 (the Owner's decision of 2026-11-24: the DeepSeek build, another client's, blocks this chain)"; done
  for m in $(echo "$gout" | sed -n 's/^ADOPT //p'); do hb "$N: $m was loaded by another client and is used as it is (W16); it stays loaded"; done
  case "$gout" in *"ADOPT $1"*) ;; *) touch $(mid_marker $1); $PY $H server load --model $1 $SV; hb "$N: $1 loads on its first request (alone)";; esac; }
unload_ours() { for m in $MODELS $DS; do
  if [ -e $(mid_marker $m) ]; then $PY $H server unload --model $m $SV || fail "the unload of $m failed"; rm -f $(mid_marker $m); hb "$N: unloaded $m"; fi; done; }
mid_of() { $PY -c 'import json,sys; c=json.load(open(sys.argv[1])); print(c[sys.argv[2]]["model"]["oMLX_model_id"])' "$CONFIGS" "$1"; }

if [ $PHASE = before-reading ]; then
rm -f $L/$N.failed $L/$N.done
hb "$N: started; arms $ARMS, sets $SETS, configs sha256 $CSHA"
CHECK_ENV=; [ $DRY = no ] || export M7_STUB_CHUNKS="$CHUNKS"
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s; HID=no; [ $s != h6 ] || HID=yes
  LDG=; [ $HID = no ] || LDG="--ledger $LEDGER"

  for arm in $(echo $ARMS | tr ',' ' '); do
    OUT=$W/ans_${s}_$arm.jsonl$EXT
    if ! stage_done ans_${s}_$arm; then
      MID=$(mid_of $arm)
      # the frozen configuration is checked before each configuration's stage (offline; the index, the prompts, the reranker revision, the catalogues)
      if [ $DRY = no ]; then $PY -m kafka_sre_rag verify > $W/verify_$arm.out 2>&1 || { cat $W/verify_$arm.out >&2; fail "kafka_sre_rag verify failed before the stage of $arm"; }
        echo "verify before $arm: $(grep -c match $W/verify_$arm.out) lines match"; else echo "dry run: kafka_sre_rag verify not run (it needs the real index); run it separately"; fi
      gate_for "$MID"
      hb "$N: answers $arm on $s started"
      $G --stall-min 40 --log $L/$N.ans_${s}_$arm.log -- $PY $H answer --configs "$CONFIGS" --arm $arm --set $s --items "$ITEMS" --out "$OUT" --python "$PY" $LDG $STUBF \
        || fail "the answers of $arm on $s failed (see $L/$N.ans_${s}_$arm.log)"
      mark ans_${s}_$arm $OUT
      hb "$N: answers $arm on $s done"
    fi
    if ! stage_done packs_${s}_$arm; then
      RES=retrieval; [ $DRY = no ] || RES=keys
      $G --stall-min 30 --log $L/$N.packs_${s}_$arm.log -- $PY $H packs --set $s --arm $arm --items "$ITEMS" --answers "$OUT" --chunks "$CHUNKS" --chunks-override --resolver $RES $STUBF \
        || fail "the packs of $arm on $s could not be recovered exactly (see $L/$N.packs_${s}_$arm.log)"
      mark packs_${s}_$arm $OUT
    fi
  done
done
# ---- judge: the answer models are unloaded, the judge loads (W2, W16)
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s; HID=; [ $s != h6 ] || HID=--hidden
  if ! stage_done judge_$s; then
    if [ $DRY = no ]; then
      for m in $MODELS; do [ ! -e $(mid_marker $m) ] || { $PY $H server unload --model $m $SV || fail "unload of $m failed"; rm -f $(mid_marker $m); hb "$N: unloaded $m (answers done)"; }; done
    fi
    gate_for "$DS"
    SETARGS=; for arm in $(echo $ARMS | tr ',' ' '); do SETARGS="$SETARGS $arm=$W/ans_${s}_$arm.jsonl$EXT"; done
    JOUT=$W/judge_$s.jsonl$EXT; JCACHE=$W/judge_cache_$s.jsonl$EXT
    JMODE="--judge-url $OMLX_URL/v1"; [ $DRY = no ] || JMODE="--stub"
    hb "$N: judging $s started (two passes, $DS)"
    $G --stall-min 60 --log $L/$N.judge_$s.log -- $PY $H judge-run $HID --python "$PY" -- --rubrics "$RUBS" --items "$ITEMS" --chunks "$CHUNKS" --source-chars 12000 --instruction v2 --coverage \
      --answers $SETARGS --judge-model $DS --judge-extra-body "$JEB" --settings-name deepseek-nothink-12k --concurrency 3 $JMODE --passes 1 2 --cache $JCACHE --out $JOUT \
      || fail "judging $s failed (see $L/$N.judge_$s.log)"
    NIT=$($PY $H check-inputs --set $s --items "$ITEMS" --rubrics "$RUBS" $STUBF | sed -n 's/^[a-z0-9]*: \([0-9]*\) items.*/\1/p')
    NARM=$(echo $ARMS | tr ',' '\n' | wc -l | tr -d ' ')
    $PY $H judge-check --out $JOUT --expect $((NIT * NARM * 2)) || fail "the judge rows of $s are not whole"
    mark judge_$s $JOUT
    hb "$N: judging $s done"
  fi
done
unload_ours
# ---- the reading bundles (no reading is done here)
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s; HID=; [ $s != h6 ] || HID=yes
  if ! stage_done bundle_$s; then
    SETARGS=; for arm in $(echo $ARMS | tr ',' ' '); do SETARGS="$SETARGS $arm=$W/ans_${s}_$arm.jsonl$EXT"; done
    if [ $s = d7 ]; then
      $PY $H bundle --configs "$CONFIGS" --arms "$ARMS" --set d7 --items "$ITEMS" --rubrics "$RUBS" --answers $SETARGS --chunks "$CHUNKS" --out $W/reading_d7 --second-fraction $SECOND || fail "the d7 bundles could not be made"
      mark bundle_d7 $W/reading_d7/KEY.json
    else
      $PY $H bundle --configs "$CONFIGS" --arms "$ARMS" --set h6 --items "$ITEMS" --rubrics "$RUBS" --answers $SETARGS --chunks "$CHUNKS" --out $W/reading_h6 --sessions $H6S || fail "the h6 sealed bundles could not be made"
      mark bundle_h6 $W/reading_h6/blind_key.json.enc
    fi
  fi
done
echo "before-reading done: the readers read the bundles; then run this script with --phase after-reading"
if [ $DRY = yes ]; then echo M7_RELEASE_TEST_DRY_RUN_BEFORE_READING_DONE >> $L/$N.done; else echo M7_RELEASE_TEST_BEFORE_READING_DONE >> $L/$N.done; fi
hb "$N: before-reading done (answers, judge, bundles); the readers follow" yes
exit 0
fi

# ================================================================ after-reading
RJ=; MEAS=
for s in $(echo $SETS | tr ',' ' '); do
  set_files $s
  SETARGS=; for arm in $(echo $ARMS | tr ',' ' '); do SETARGS="$SETARGS $arm=$W/ans_${s}_$arm.jsonl$EXT"; done
  FIND=; VERD=; PK=
  if [ $s = d7 ]; then
    if [ -n "$FINDD7" ]; then
      $PY $H unblind --set d7 --key $W/reading_d7/KEY.json --rubrics "$RUBS" --answers $SETARGS --findings $FINDD7 --out $W/findings_d7.jsonl --reads-out $W/reads_d7.jsonl \
        $([ -z "$SCOPED7" ] || echo "--scope $SCOPED7") || fail "the d7 findings could not be unblinded (see above)"
      FIND="--findings $W/findings_d7.jsonl"
      $PY $H pack-input --configs "$CONFIGS" --arms "$ARMS" --set d7 --items "$ITEMS" --answers $SETARGS --findings $W/findings_d7.jsonl --out-dir $W/pack_input_d7 || fail "the d7 pack input could not be made"
    fi
    VERD="--verdicts $VD7"; PK="--pack-key $PKD7"; OUTM=$W/measures_d7.json
  else
    if [ -n "$H6DIR" ]; then
      $PY $H unblind --set h6 --dir "$H6DIR" --out $W/findings_h6.jsonl.enc --reader $FREADER || fail "the h6 findings could not be unblinded"
      FIND="--findings $W/findings_h6.jsonl.enc"
      $PY $H pack-input --configs "$CONFIGS" --arms "$ARMS" --set h6 --items "$ITEMS" --answers $SETARGS --findings $W/findings_h6.jsonl.enc --out-dir $W/pack_input_h6 || fail "the h6 pack input could not be made"
    fi
    VERD="--verdicts $VH6"; PK="--pack-key $PKH6"; OUTM=$W/measures_h6.json.enc
  fi
  JF=$W/judge_$s.jsonl$EXT
  $PY $H measures --set $s --arms "$ARMS" --items "$ITEMS" --rubrics "$RUBS" --answers $SETARGS --judge $JF $FIND --first-reader $FREADER $VERD $PK --out $OUTM || fail "the measures of $s could not be made"
  MEAS="$MEAS $s=$OUTM"
done
$PY $RULE report --measures $MEAS --arms "$ARMS" --candidate $CAND --control $CTRL --out $W/report_body.md --json-out $W/result.json > $W/rule_out.txt || fail "the report could not be made"
FINAL=$(tail -n 1 $W/rule_out.txt)
{
  cat $W/report_header.md
  echo
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
