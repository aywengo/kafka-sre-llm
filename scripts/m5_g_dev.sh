#!/bin/sh
# M5 GD-chain: the development check of the grounded modes (runs/m5/reports/g0_grounded_design.md, section 3; MILESTONE.md task G2/GD-run).
# Both variants, grounded-g1 and grounded-g2, answer STORED default packs (eval2.py answer --replay-packs) on the reference configuration
# (the 4-bit base model on the server, thinking off, the frozen index, the 2,048-token limit), then the rule of section 3 is applied by code
# (scripts/m5_g_rule.py): PASS or FAIL per condition and variant, and the choice G1, G2 or neither. The rule's text is written into the report
# header before any answer exists. Reference for every comparison: the stored default answers on the same packs.
# Sets (the ids of the mechanical sample are in eval/m4/g_dev_ids.json, drawn by scripts/m5_g_rule.py draw-ids):
#   J  judged: B1 (30, eval/m4/b1) and pilot2 (24, eval/m4/pilot2); packs of work/m4/b1 and work/m4/pilot2 default answers
#   M1 + M2  tier_a: 56 + 40 items (packs of work/m4/s6/ans_A1.jsonl);  M3  tier_a3: 20 items (packs of work/m4/e2/ans_O_Q4D_a3.jsonl)
#   --mechanical all: all 251 + 88 = 339 open mechanical items instead of the 116 (the Owner has not yet decided; sample is the default).
# Steps (each resumable, each prints a line per item; a stage marker work/.../stage_NAME.done records the manifest hash and every output hash):
#   0. Start check (CPU, refusal exit 2, nothing moved or deleted): the stored manifest and the stage markers against the state now (pinned files
#      by sha256: the item and rubric files, the stored default answers whose packs are replayed with their manifests, the judge cache, the catalogue
#      record, the index record; the prompt hashes of frozen.json; the rule text; the options). A marker without a manifest, for another manifest, or
#      with a missing or changed output is refused.
#   1. Gate: inputs, ids file reproduces from its rule, stored default answers complete, kafka_sre_rag verify; the server (real runs): the 4-bit base
#      model listed, no other chain or guarded job, no active or waiting request, no model of another client loaded (our judge builds are unloaded).
#   2. Report header with the rule text (written once, before any answer).
#   3. Determinism check: 3 B1 default replays against the stored default answers, byte for byte. On failure the chain stops and says that default
#      must be answered again.
#   4. Answers of g1 and g2 on b1, pilot2, tier_a part, tier_a3 part (guarded, stall limit 40 min), pack equality with the stored packs.
#   5. Judge (DeepSeek, coverage role, instruction v2, setting deepseek-nothink-12k, two passes) on B1 and pilot2, default included; guarded.
#   6. Identifier check (CPU). 7. The scorer with decline outcomes (CPU). 8. Measures (a) to (h), the rule, the report runs/m5/G_DEV.md.
#   scripts/m5_g_dev.sh > runs/m5/logs/m5_g_dev.out 2>&1 &
#   scripts/m5_g_dev.sh --dry-run [--work DIR] [--mechanical all]   # stub model, stub judge, no server, no markers outside DIR, no heartbeat, no
#                                                                   # file written outside DIR (default: a temporary directory); the numbers are not measurements
# Options: --mechanical sample|all, --ids FILE, --work DIR, --report FILE. Markers: runs/m5/logs/m5_g_dev.done or .failed (a dry run: in its own DIR).
set -e
DRY=no; MECH=sample; IDS=eval/m4/g_dev_ids.json; W=work/m5/gdev; REPORT=runs/m5/G_DEV.md; WSET=no; RSET=no
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --mechanical) MECH=${2:?}; shift;; --ids) IDS=${2:?}; shift;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  *) echo "usage: scripts/m5_g_dev.sh [--dry-run] [--mechanical sample|all] [--ids FILE] [--work DIR] [--report FILE]" >&2; exit 2;; esac
  shift; done
case $MECH in sample|all) ;; *) echo "REFUSED: --mechanical is sample or all; nothing started" >&2; exit 2;; esac
L=runs/m5/logs; N=m5_g_dev; R=scripts/m5_g_rule.py
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json
B1_ITEMS=eval/m4/b1/items.jsonl; B1_RUBS=eval/m4/b1/rubrics.jsonl; P2_ITEMS=eval/m4/pilot2/items.jsonl; P2_RUBS=eval/m4/pilot2/rubrics.jsonl
B1_ANS=work/m4/b1/ans_default.jsonl; P2_ANS=work/m4/pilot2/ans_default.jsonl
TA_ITEMS=eval/m4/open/tier_a.jsonl; TA3_ITEMS=eval/m4/open/tier_a3.jsonl
TA_ANS=work/m4/s6/ans_A1.jsonl; TA3_ANS=work/m4/e2/ans_O_Q4D_a3.jsonl; B1_CACHE=work/m4/b1/judge_cache.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
SETTING=deepseek-nothink-12k; SRC=12000
# The estimate's basis (design section 3, from B1's rates): 35 to 70 s per judged answer, 10 to 13 s per mechanical answer, about 10 s per judge call.
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m5_g_dev.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/G_DEV_from_stub_data.md
  G="scripts/run_guarded.py --other-retries 0"
fi
mkdir -p $W $L
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M5 GD: development check of the grounded modes",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- 0 and 1. inputs, the model, the start check (CPU; nothing is written before the check passes, except $W itself)
for f in $B1_ITEMS $B1_RUBS $P2_ITEMS $P2_RUBS $TA_ITEMS $TA3_ITEMS $B1_ANS $B1_ANS.manifest.json $P2_ANS $P2_ANS.manifest.json $TA_ANS $TA_ANS.manifest.json \
         $TA3_ANS $TA3_ANS.manifest.json $CHUNKS $FROZEN $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json scripts/eval2.py scripts/eval2_judge.py \
         scripts/eval2_idcheck.py scripts/eval2_score.py scripts/eval2_compare.py scripts/run_guarded.py $R; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
[ -s $IDS ] || refuse "the ids file $IDS is missing (python scripts/m5_g_rule.py draw-ids --tier-a $TA_ITEMS --tier-a3 $TA3_ITEMS --out $IDS)"
if [ $DRY = no ]; then
  [ -s $B1_CACHE ] || refuse "input $B1_CACHE is missing or empty"
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
foreign() { # loaded models that are not ours (the 4-bit model, DeepSeek and the GLM builds are ours)
  api /api/status | python3 -c 'import sys,json; ours=set(sys.argv[1:]); print(" ".join(m for m in (json.load(sys.stdin).get("loaded_models") or []) if m not in ours))' $DS $GLM8 $GLM4 $Q4 2>/dev/null || echo "status-unreadable"; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0   # only models this chain knows as its own (never another client's)
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" >/dev/null; hb "$N: unloaded $1 (no active or waiting requests)"; }
server_free() { n=0; until [ "$(busy)" = "0" ] && [ -z "$(foreign)" ]; do
    sleep 30; n=$((n+1))
    [ $n -lt 120 ] || fail "server not free after 60 min (active or waiting requests, or a model of another client loaded: $(foreign))"
  done; }
others() { pgrep -fl 'scripts/(m4|m5|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }

if [ $DRY = yes ]; then
  Q4=Qwen3.8-27B-4bit
else
  api /v1/models > $W/models.json 2>/dev/null || true
  Q4=${M5_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || refuse "the 4-bit model is not listed by the server (or ambiguous)"
fi
DRYFLAG=""; [ $DRY = yes ] && DRYFLAG="--dry"
PINS="--pin $B1_ITEMS --pin $B1_RUBS --pin $P2_ITEMS --pin $P2_RUBS --pin $TA_ITEMS --pin $TA3_ITEMS --pin $IDS --pin $B1_ANS --pin $B1_ANS.manifest.json \
  --pin $P2_ANS --pin $P2_ANS.manifest.json --pin $TA_ANS --pin $TA_ANS.manifest.json --pin $TA3_ANS --pin $TA3_ANS.manifest.json \
  --pin $CATS/CATALOGUES.json --pin runs/v3/INDEX_FROZEN.json"
[ $DRY = yes ] || PINS="$PINS --pin $B1_CACHE"
STATE="--manifest $W/MANIFEST.json --work $W $PINS --frozen $FROZEN --mechanical $MECH --model $Q4 $DRYFLAG"
.venv/bin/python $R start-check $STATE || refuse "the start check failed (see above; nothing was moved or deleted)"
.venv/bin/python $R draw-ids --tier-a $TA_ITEMS --tier-a3 $TA3_ITEMS --check $IDS || refuse "$IDS does not reproduce from its rule"
for s in "$B1_ANS 30" "$P2_ANS 24" "$TA_ANS 251" "$TA3_ANS 88"; do
  set -- $s; .venv/bin/python $R check-answers --file $1 --expect $2 --mode default || refuse "the stored default answers $1 are incomplete, have errors or another mode"
done
.venv/bin/python - $B1_ANS.manifest.json $P2_ANS.manifest.json $TA_ANS.manifest.json $TA3_ANS.manifest.json "$Q4" "$DRY" <<'PY' || refuse "a stored default run was not made on the reference configuration (4-bit model, thinking off, default limit)"
import json, sys
q4, dry = sys.argv[5], sys.argv[6] == "yes"
for p in sys.argv[1:5]:
    m = json.load(open(p))
    ok = m.get("model_requested") == q4 and (m.get("extra_body") or {}).get("chat_template_kwargs", {}).get("enable_thinking") is False \
        and m.get("max_tokens") is None and m.get("mode") == "default"
    print(f"gate: {p}: model {m.get('model_requested')}, mode {m.get('mode')}, max_tokens {m.get('max_tokens')}: {'ok' if ok else 'NOT the reference configuration'}")
    if not ok and not dry:
        sys.exit(1)
PY
.venv/bin/python -m kafka_sre_rag verify > $W/verify.out 2>&1 || { cat $W/verify.out >&2; refuse "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"; }
grep -E '^prompt ' $W/verify.out | head -8
.venv/bin/python $R manifest $STATE
mkdir -p "$(dirname $REPORT)"

# ---------------------------------------------------------------- the plan and the estimate
if [ $MECH = all ]; then NMECH=339; else NMECH=116; fi
est() { awk -v m=$NMECH 'BEGIN{ lo=54*2*35+m*2*10+3*35+264*10+1200; hi=54*2*70+m*2*13+3*70+264*10+1800; printf "%.1f to %.1f", lo/3600, hi/3600 }'; }
echo "planned: 54 judged answers and $NMECH mechanical items, each in 2 variants (grounded-g1, grounded-g2), the determinism check (3 answers), about 264 judge calls: $(est) h of model-server time"
echo "estimate basis: the design's rates (35 to 70 s per judged answer, 10 to 13 s per mechanical answer, about 10 s per judge call, B1: 180 calls in 30.6 min, loads 0.3 to 0.5 h); not measured on the grounded modes"

stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $R mark --work $W --stage $st --out "$@" > /dev/null; }
ANS_FILES=""
for v in g1 g2; do for s in b1 p2 ma m3; do ANS_FILES="$ANS_FILES $W/ans_${s}_$v.jsonl"; done; done
SERVER_NEEDED=no
for st in determinism answers judge; do stage_done $st || SERVER_NEEDED=yes; done

# ---------------------------------------------------------------- 2. report header (before any answer exists)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $ANS_FILES $W/det_default.jsonl; do [ ! -e $f ] || refuse "$f exists but the report header was not written yet; the rule text must come first"; done
  [ $DRY = yes ] || [ ! -e $REPORT ] || refuse "$REPORT exists; it is not overwritten before the header of this run is written (move it away first)"
  {
    echo "# M5 development check of the grounded modes (G1, G2)$([ $DRY = yes ] && echo ' (DRY RUN: stub model, stub judge; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/$N.sh\` on $(date +%Y-%m-%d), mechanical set: $MECH ($NMECH items), 4-bit model \`$Q4\`. Manifest hash $(.venv/bin/python -c 'import hashlib,json,sys; print(hashlib.sha256(json.dumps(json.load(open(sys.argv[1])), sort_keys=True).encode()).hexdigest()[:16])' $W/MANIFEST.json). Ids: \`$IDS\`. Rule: \`scripts/m5_g_rule.py\`."
    echo
    .venv/bin/python $R rule-text
    echo
    echo "This header, with the rule, was written before any answer of the run existed (stage marker \`$W/stage_header.done\`)."
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header with the rule text written ($REPORT); no answer exists yet"
fi

# ---------------------------------------------------------------- the server, once
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  rm -f $L/$N.failed $L/$N.done
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done
  hb "$N: started; 4-bit model $Q4, grounded-g1 and grounded-g2 on J (54) and the mechanical set ($NMECH); judge $DS (coverage role, instruction v2, $SETTING); mechanical: $MECH"
  status RUNNING no "$N running: determinism check, answers, judging, scoring; markers $L/$N.done / .failed"
  server_free
  unload $DS; unload $GLM4; unload $GLM8
  hb "$N: DeepSeek and the GLM builds unloaded if they were loaded; the 4-bit model loads on the first request"
fi

# ---------------------------------------------------------------- prepare: items and reference files (CPU)
if ! stage_done prepare; then
  .venv/bin/python $R harness-items --in $B1_ITEMS --out $W/items_b1_h.jsonl --domain b1 || fail "harness items of B1"
  .venv/bin/python $R harness-items --in $P2_ITEMS --out $W/items_p2_h.jsonl --domain pilot2 || fail "harness items of pilot2"
  .venv/bin/python $R select --ids $IDS --mechanical $MECH --tier-a $TA_ITEMS --tier-a3 $TA3_ITEMS --ref-a $TA_ANS --ref-a3 $TA3_ANS --out-dir $W || fail "mechanical item files"
  .venv/bin/python - $B1_ITEMS $B1_RUBS $P2_ITEMS $P2_RUBS <<'PY' || fail "items and rubrics of B1 or pilot2 do not match"
import json, sys
for i, r in ((1, 2), (3, 4)):
    a = [json.loads(l)["id"] for l in open(sys.argv[i]) if l.strip()]
    b = [json.loads(l)["id"] for l in open(sys.argv[r]) if l.strip()]
    if a != b or len(set(a)) != len(a):
        sys.exit(f"{sys.argv[i]} and {sys.argv[r]}: ids differ or repeat")
    print(f"gate: {sys.argv[i]} and {sys.argv[r]}: {len(a)} items, same ids")
PY
  mark prepare $W/items_b1_h.jsonl $W/items_p2_h.jsonl $W/items_ma.jsonl $W/items_m3.jsonl $W/ref_ma.jsonl $W/ref_m3.jsonl
fi
NB1=$(grep -c . $W/items_b1_h.jsonl | tr -d ' '); NP2=$(grep -c . $W/items_p2_h.jsonl | tr -d ' ')
NMA=$(grep -c . $W/items_ma.jsonl | tr -d ' '); NM3=$(grep -c . $W/items_m3.jsonl | tr -d ' ')
[ "$NB1" = 30 ] && [ "$NP2" = 24 ] || fail "B1 has $NB1 items and pilot2 $NP2; 30 and 24 expected"
echo "sets: b1 $NB1, pilot2 $NP2, tier_a part $NMA, tier_a3 part $NM3"

# ---------------------------------------------------------------- answers (one request at a time; the 4-bit model)
if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $Q4"; fi
answer_run() { # OUT ITEMS REPLAY EXPECT MODE CONFIG [--limit N]: all items, one request at a time
  out=$1; items=$2; replay=$3; expect=$4; mode=$5; config=$6; shift 6
  if .venv/bin/python $R check-answers --file $out --expect $expect --mode $mode > $W/check.out 2>&1; then echo "skip answers: $(cat $W/check.out)"; return 0; fi
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    hb "$N: answers $config started ($expect items, mode $mode)"
    $G --stall-min 40 --log $L/$N.$config.log -- .venv/bin/python scripts/eval2.py answer --items $items --out $out --config $config $GA \
      --extra-body "$EB" --mode $mode --replay-packs $replay --catalogue $CATS --host-class "Mac Studio" "$@" \
      || fail "answers $config: failed (see $L/$N.$config.log)"
    .venv/bin/python $R check-answers --file $out --expect $expect --mode $mode && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $config: incomplete, with errors, empty or without a pack after $tries passes ($out)"
  done
  .venv/bin/python $R packs-equal --a $replay --b $out || fail "answers $config: the packs differ from the stored packs (replay failed)"
}
if ! stage_done determinism; then
  DET=$W/det_default.jsonl
  answer_run $DET $W/items_b1_h.jsonl $B1_ANS 3 default m5-gdev-det --limit 3
  if .venv/bin/python $R determinism --stored $B1_ANS --new $DET --n 3; then :
  elif [ $DRY = yes ]; then echo "dry run: the stub answers cannot equal the stored ones; the comparison is not binding here (the real run stops)"
  else fail "determinism check failed: default must be answered again on the same sets (the stored default answers are not reproducible on this server)"; fi
  mark determinism $DET
  hb "$N: determinism check done"
fi
if ! stage_done answers; then
  for v in g1 g2; do
    mode=grounded-$v
    answer_run $W/ans_b1_$v.jsonl $W/items_b1_h.jsonl $B1_ANS $NB1 $mode m5-gdev-b1-$v
    answer_run $W/ans_p2_$v.jsonl $W/items_p2_h.jsonl $P2_ANS $NP2 $mode m5-gdev-p2-$v
    answer_run $W/ans_ma_$v.jsonl $W/items_ma.jsonl $TA_ANS $NMA $mode m5-gdev-ma-$v
    answer_run $W/ans_m3_$v.jsonl $W/items_m3.jsonl $TA3_ANS $NM3 $mode m5-gdev-m3-$v
  done
  mark answers $ANS_FILES
  hb "$N: all answers done (g1, g2: B1 $NB1, pilot2 $NP2, tier_a part $NMA, tier_a3 part $NM3)"
fi

# ---------------------------------------------------------------- judge: DeepSeek, coverage role, two passes, default included
if ! stage_done judge; then
  if [ $DRY = no ]; then unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; fi
  judge_done() { [ -f "$1" ] && .venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; sys.exit(0 if len(r)==int(sys.argv[2]) and all(x["valid_reply"] for x in r) else 1)' "$1" "$2" 2>/dev/null; }
  jrun() { # SET ITEMS RUBRICS DEFAULT_ANS CACHE OUT EXTRA...
    set_=$1; items=$2; rubs=$3; dans=$4; cache=$5; jout=$6; shift 6
    SETS="default=$dans g1=$W/ans_${set_}_g1.jsonl g2=$W/ans_${set_}_g2.jsonl"
    if [ $DRY = yes ]; then
      .venv/bin/python scripts/eval2_judge.py run --rubrics $rubs --items $items --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
        --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $SETTING --concurrency 3 --stub --passes 1 2 --cache $cache --out $jout "$@"
    else
      $G --stall-min 60 --log $L/$N.judge_$set_.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $rubs --items $items \
        --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
        --settings-name $SETTING --concurrency 3 --judge-url "$OMLX_URL/v1" --passes 1 2 --cache $cache --out $jout "$@"
    fi; }
  [ $DRY = yes ] || [ -f $W/judge_cache_b1.jsonl ] || cp $B1_CACHE $W/judge_cache_b1.jsonl   # B1's default answers are in the cache of the earlier run
  for set_ in b1 p2; do
    if [ $set_ = b1 ]; then items=$B1_ITEMS; rubs=$B1_RUBS; dans=$B1_ANS; n=$NB1; else items=$P2_ITEMS; rubs=$P2_RUBS; dans=$P2_ANS; n=$NP2; fi
    jout=$W/judge_$set_.jsonl; cache=$W/judge_cache_$set_.jsonl; calls=$((n * 3 * 2))
    if judge_done $jout $calls; then echo "skip judging $set_: $jout has $calls valid rows"; continue; fi
    hb "$N: judging $set_ started ($calls judge rows; the cache holds earlier replies)"
    for round in 1 2; do
      extra=""; [ $round = 2 ] && extra="--retry-invalid"
      if [ $DRY = yes ]; then jrun $set_ $items $rubs $dans $cache $jout $extra > $W/judge_$set_.dry.out 2>&1 || { cat $W/judge_$set_.dry.out; fail "judge dry run ($set_) failed"; }
        grep -E '^(matched|instruction|items)' $W/judge_$set_.dry.out
      else jrun $set_ $items $rubs $dans $cache $jout $extra || fail "judging $set_ round $round failed (see $L/$N.judge_$set_.log)"; fi
      bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $jout)
      echo "judging $set_ round $round: $bad unscored"
      [ "$bad" = "0" ] && break
    done
    judge_done $jout $calls || fail "judging $set_: $jout does not hold $calls valid rows"
  done
  if [ $DRY = no ]; then unload $DS; hb "$N: judging finished, $DS unloaded"; fi
  mark judge $W/judge_b1.jsonl $W/judge_p2.jsonl
fi

# ---------------------------------------------------------------- identifier check and the scorer (CPU)
if ! stage_done checks; then
  OUTS=""
  for s in b1 p2; do
    if [ $s = b1 ]; then dans=$B1_ANS; else dans=$P2_ANS; fi
    for v in default g1 g2; do
      if [ $v = default ]; then f=$dans; else f=$W/ans_${s}_$v.jsonl; fi
      .venv/bin/python scripts/eval2_idcheck.py --answers $f --catalogues $CATS --totals-only > $W/idcheck_${s}_$v.txt || fail "identifier check on $s $v failed"
      echo "idcheck $s $v: $(sed -n 4p $W/idcheck_${s}_$v.txt)"
      OUTS="$OUTS $W/idcheck_${s}_$v.txt"
    done
  done
  for grp in ma m3; do
    for v in ref g1 g2; do
      if [ $v = ref ]; then f=$W/ref_$grp.jsonl; else f=$W/ans_${grp}_$v.jsonl; fi
      .venv/bin/python scripts/eval2_score.py --items $W/items_$grp.jsonl --answers $f --catalogues $CATS --scores $W/score_${grp}_$v.jsonl \
        --summary $W/score_${grp}_$v.summary.json > $W/score_${grp}_$v.out 2> $W/score_${grp}_$v.err || fail "scorer on $grp $v failed (see $W/score_${grp}_$v.err)"
      echo "score $grp $v: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print("all-pass", d["all_pass"], "of", d["items_scored"], "format failures", d["format_failures"], "missing answers", d["missing_answers"])' $W/score_${grp}_$v.summary.json)"
      OUTS="$OUTS $W/score_${grp}_$v.jsonl"
    done
  done
  mark checks $OUTS
fi

# ---------------------------------------------------------------- measures, the rule, the report
.venv/bin/python $R measures --work $W --ids $IDS --mechanical $MECH --stored-b1 $B1_ANS --stored-p2 $P2_ANS --rubrics-b1 $B1_RUBS --rubrics-p2 $P2_RUBS $([ $DRY = yes ] && echo --stub) \
  > $W/measures.out || fail "the measures could not be computed (a missing input is an error, see above)"
RULE_RC=0; .venv/bin/python $R rule --measures $W/measures.json > $W/rule_out.txt 2> $W/rule_out.err || RULE_RC=$?
[ $RULE_RC = 0 ] || { cat $W/rule_out.err >&2; fail "the rule could not be applied: $(head -1 $W/rule_out.err)"; }
cat $W/rule_out.txt
CHOICE=$(sed -n 's/^CHOICE: \([A-Za-z0-9]*\).*/\1/p' $W/rule_out.txt)
{
  cat $HEAD
  echo
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  echo "**Choice by the rule: ${CHOICE:-unknown}.**"
  echo
  cat $W/measures.md
  echo
  echo "### The rule applied by code (\`scripts/m5_g_rule.py rule\`)"
  echo
  echo '```'; cat $W/rule_out.txt; echo '```'
  echo
  echo "### Determinism check"
  echo
  echo '```'; .venv/bin/python $R determinism --stored $B1_ANS --new $W/det_default.jsonl --n 3 || true; echo '```'
  echo
  echo "### Files"
  echo
  echo "Answers \`$W/ans_*_g1.jsonl\`, \`ans_*_g2.jsonl\`; judge results \`$W/judge_b1.jsonl\`, \`judge_p2.jsonl\`; scores \`$W/score_*.jsonl\`; identifier checks \`$W/idcheck_*.txt\`; measures \`$W/measures.json\`. Open development data only: the result decides whether B2 is spent, and says nothing about the hidden part."
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
if [ $DRY = yes ]; then
  echo M5_G_DEV_DRY_RUN_DONE >> $L/$N.done
  sed -n '1,4p' $REPORT | cut -c1-240
  echo "dry run done: report from stub data in $REPORT"
  exit 0
fi
hb "$N: done; report $REPORT; choice by the rule: ${CHOICE:-unknown}" yes
status DONE yes "$N done: $REPORT; choice by the rule: ${CHOICE:-unknown}; the Owner decides what follows"
echo M5_G_DEV_DONE >> $L/$N.done
