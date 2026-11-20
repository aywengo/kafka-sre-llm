#!/bin/sh
# M4 E2: the starting-point measurement (the 4-bit build and the plain base, on the hidden half; ledger row 1) and the checked answers
# of E3 (row 2, the R1 marks and the sealed reading's bundles). Plan: runs/m4/reports/e2_plan.md (sections 1, 2, 6, 7).
# Takes the model server (no local GPU). Hidden rows are read and written only through scripts/eval2.py's encryption (.enc, key from
# EVAL_HIDDEN_KEY or ~/.config/kafka-sre-llm/eval_hidden.key); logs, progress lines, heartbeat lines, markers and the report carry ids,
# counts, timings and totals, never an item, an answer or a quote. Every model or reranker job runs under scripts/run_guarded.py.
#
# Stages, each resumable by its output files, a line per item, a done marker at the end:
#   PREPARE  the manifest runs/m4/E2_MANIFEST.json (index, reranker, catalogues, golden prompts, scorer, identifier check, judge settings,
#            revise version and prompt hash, checker version, chunks file, the hidden files by sha256 of the .enc files, SPLIT*.json, server
#            version, model names as the server lists them). Written once, never overwritten; the hidden gate compares with it.
#   G        byte-identity gate: the 60 open smoke items in Q4D against the stored A1 answers of S6 (work/m4/s6/ans_A1.jsonl).
#   O        open rehearsal: the 88 open a3 items in Q4D; P0 (closed book) on the smoke set (60 mechanical, the 24 open judged scenarios of
#            eval/m4/pilot2); scored, identifier check, judged (DeepSeek, 2 passes).
#   (gate)   the hidden gate, before the first hidden item of every launch: tests and verify pass, clean tree, commit equals the manifest's,
#            every manifest hash matches, ledger rows 1 and 2 present with a decision rule (the chain reads the ledger, never writes it),
#            the hidden files hold 245, 60, 24 and 36 rows, G and O passed, no plain-text file with a hidden id under the work folder.
#   H1       Q4D and Q4S (packs replayed from Q4D) on the hidden judged 60; R1 on the Q4D answers (R2 only with --r2); the blind bundles of
#            the sealed reading (default and strict; default only is refused, see --reading); the marker "readings_ready".
#   H2       Q4D, Q4S and P0 on the hidden mechanical 305 (245 + 60 a3); P0 on the hidden judged 60.
#   H3       the full-precision build: F and FR (packs from Q4D) on the 305; F on the judged 60 (packs from Q4D) only with --f-judged.
#   H4       the judge (DeepSeek, instruction v2, coverage role, 2 passes) on the hidden judged sets.
#   H5       the second judge (GLM, thinking) on a seeded 24-answer subset of Q4D, pass 1, unless --no-cross-judge; agreement as totals.
#   SCORE    hidden scoring, identifier-check totals, coverage report, eval2_compare.py e2, the report runs/m4/E2_STARTING_POINT.md
#            (totals only). The verdict of rule 1(a) is printed only with --margin-reading one-sided|two-sided; without it both readings
#            are shown labelled "not a verdict".  (CPU; needs no server.)
#   E4       first words and the 60 s bar through the service on open items (scripts/eval2_first_words.py). Its own step, not part of the
#            hidden look, and not in the default list.
# Before every model run the chain checks that the server has no active or waiting request and that no model other than the one the run
# needs is loaded; our models that are not needed are unloaded (a swap goes to runs/v3/HEARTBEAT.md); a model that is not ours is waited
# for up to 60 min, then the chain stops.
#
# Running time, hours (basis: e2_plan.md section 6; S6 rates 28 s Q4D and about 12 s Q4S per mechanical answer, 50 s F, 27 s FR; B1/B2
# 85 s Q4D and 53 to 63 s Q4S per judged answer, judge 10 to 11 s per call at 3 in parallel, GLM 485 to 691 s per call; P0 unmeasured):
#   G 0.4-0.5 | O 1.5-2 | H1 2.3-2.5 | H2 4.5-7 | H3 6.5-9.5 (+2 to 3.3 with --f-judged) | H4 1.0-1.1 | H5 1.1-2.7 | E4 about 1.8
#   loads, checks, CPU scoring 0.5 | total about 20 to 29 h (E4 included in the plan's figure); corrected when a rate is known.
#
#   scripts/m4_e2_starting_point.sh [options] > runs/m4/logs/m4_e2_starting_point.out 2>&1 &
# Options (defaults):
#   --stages LIST            comma list of PREPARE is implicit, G,O,H1,H2,H3,H4,H5,SCORE,E4 (default G,O,H1,H2,H3,H4,H5,SCORE)
#   --hidden-judged-items F24 F36 --hidden-judged-rubrics R24 R36    the judged hidden files (24 and 36 rows; no default: required)
#   --hidden-a FILE (eval/hidden/tier_a.jsonl.enc)  --hidden-a3 FILE (eval/hidden/tier_a3.jsonl.enc)
#   --reading both          blind bundles of default and strict (the only value: "default" is refused, see the gap in the report of B6:
#                           eval2_sealed_reading.py bundle needs two or more answer sets)
#   --r2                     R2 patch with re-check in H1 (off); adds a reading bundle of the patched answers
#   --f-judged               F on the judged tier (off)        --no-cross-judge   skip H5 (the cross-check is on)
#   --margin-reading one-sided|two-sided   no default: without it no verdict of rule 1(a) is printed
#   --reading-json FILE      totals of the sealed reading for rule 1(b) (default: none, the rule waits for the reading)
#   --ledger FILE (runs/m4/HIDDEN_LEDGER.md)  --manifest FILE (runs/m4/E2_MANIFEST.json)  --report FILE (runs/m4/E2_STARTING_POINT.md)
#   --bundle-seed S (20261119)   --dry-run [--work DIR] [--synth-marker STR] [--synth-drop-a N] [--ledger FILE] [--allow-dirty]
# --dry-run: stub generator, stub judge, a fake server status, a temporary key and synthetic hidden files of the right row counts in the
# work folder (a temporary directory unless --work); nothing real is started, the real heartbeat, status and ledger are not touched.
# Markers: runs/m4/logs/m4_e2_starting_point.done or .failed, .readings_ready (H1).
set -e
DRY=no; STAGES="G,O,H1,H2,H3,H4,H5,SCORE"; R2=no; READING=both; FJ=no; CROSS=yes; MARGIN=""; RJSON=""; BSEED=20261119
HID_A=eval/hidden/tier_a.jsonl.enc; HID_A3=eval/hidden/tier_a3.jsonl.enc; HID_J1=""; HID_J2=""; HID_R1=""; HID_R2=""
LEDGER=runs/m4/HIDDEN_LEDGER.md; MANIFEST=runs/m4/E2_MANIFEST.json; REPORT=runs/m4/E2_STARTING_POINT.md
W=work/m4/e2; WSET=no; MARK=SYNTHMARK; DROPA=0; DIRTY_OK=no; LSET=no; MSET=no; RSET=no; FAKE_LOADED='[]'
usage() { echo "usage: scripts/m4_e2_starting_point.sh [--dry-run] [--stages LIST] [--hidden-judged-items F F --hidden-judged-rubrics R R] [--reading default|both] [--r2] [--f-judged] [--no-cross-judge] [--margin-reading one-sided|two-sided] [--reading-json FILE] [--ledger F] [--manifest F] [--report F] (see the header)" >&2; exit 2; }
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;; --stages) STAGES=${2:?}; shift;; --r2) R2=yes;; --f-judged) FJ=yes;; --no-cross-judge) CROSS=no;;
  --reading) READING=${2:?}; shift;; --margin-reading) MARGIN=${2:?}; shift;; --reading-json) RJSON=${2:?}; shift;;
  --hidden-judged-items) HID_J1=${2:?}; HID_J2=${3:?}; shift 2;; --hidden-judged-rubrics) HID_R1=${2:?}; HID_R2=${3:?}; shift 2;;
  --hidden-a) HID_A=${2:?}; shift;; --hidden-a3) HID_A3=${2:?}; shift;;
  --ledger) LEDGER=${2:?}; LSET=yes; shift;; --manifest) MANIFEST=${2:?}; MSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --bundle-seed) BSEED=${2:?}; shift;; --work) W=${2:?}; WSET=yes; shift;; --synth-marker) MARK=${2:?}; shift;; --synth-drop-a) DROPA=${2:?}; shift;;
  --allow-dirty) DIRTY_OK=yes;; --fake-loaded) FAKE_LOADED=${2:?}; shift;;
  *) usage;; esac; shift; done
case $READING in both) ;; default) echo "REFUSED: --reading default is not possible with the present tools: scripts/eval2_sealed_reading.py bundle refuses fewer than two answer sets (cmd_bundle: len(sets) < 2). Relax that check, or read both sets; nothing started" >&2; exit 2;; *) usage;; esac
case $MARGIN in ""|one-sided|two-sided) ;; *) usage;; esac
[ $DIRTY_OK = no ] || [ $DRY = yes ] || { echo "--allow-dirty is for dry runs only" >&2; exit 2; }
for s in $(echo $STAGES | tr ',' ' '); do case $s in G|O|H1|H2|H3|H4|H5|SCORE|E4) ;; *) usage;; esac; done
want() { case ",$STAGES," in *",$1,"*) return 0;; esac; return 1; }
anyh() { want H1 || want H2 || want H3 || want H4 || want H5 || want SCORE; }

L=runs/m4/logs; N=m4_e2_starting_point; HB=runs/v3/HEARTBEAT.md
OPEN_A=eval/m4/open/tier_a.jsonl; OPEN_A3=eval/m4/open/tier_a3.jsonl; STORED_A1=work/m4/s6/ans_A1.jsonl
PIL_ITEMS=eval/m4/pilot2/items.jsonl; PIL_RUBS=eval/m4/pilot2/rubrics.jsonl
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'; JSET=deepseek-nothink-12k
CEB='{"max_tokens": 32768}'; CSET=glm-think-12k; SRC=12000
PYH=".venv/bin/python scripts/m4_e2_manifest.py"
SLEEP=30; WAITN=120

if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m4_e2_dry.XXXXXX")
  mkdir -p $W; L=$W/logs; HB=$W/HEARTBEAT.md; SLEEP=0; WAITN=1
  [ $RSET = yes ] || REPORT=$W/report_from_stub_data.md
  [ $MSET = yes ] || MANIFEST=$W/E2_MANIFEST.json
  FAKE=$W/fake_status.json
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 0"
  export EVAL_HIDDEN_KEY=$W/dry.key
else
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 2"
fi
H=$W/hidden; mkdir -p $W $H $L runs/m4/reports

hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> $HB; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M4 E2: starting point on the hidden half (4-bit, plain base, full precision)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- the server (real, or a fake status file in a dry run)
api() { if [ $DRY = yes ]; then case $1 in /v1/models) cat $W/models.json;; *) cat $FAKE;; esac
        else curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; fi; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
loaded_list() { api /api/status | python3 -c 'import sys,json; print(" ".join(json.load(sys.stdin).get("loaded_models") or []))' 2>/dev/null || echo "status-unreadable"; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep $SLEEP; n=$((n+1)); [ $n -lt $WAITN ] || fail "server still busy after 60 min"; done; }
unload_now() { # one of our models; only when no request is active or waiting
  idle
  if [ $DRY = yes ]; then $PYH fake-status --file $FAKE unload "$1"
  else curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" > /dev/null; fi
  hb "$N: unloaded $1 (no active or waiting requests)"; }
unload() { case " $(loaded_list) " in *" $1 "*) unload_now $1;; esac; }
simulate_load() { [ $DRY = no ] || $PYH fake-status --file $FAKE load "$1"; }   # a dry run: the model "loads" on its first request
snapshot() { # FILE: loaded models and version fields of the server (no address, no key)
  if [ $DRY = yes ]; then cp $FAKE "$1"; return 0; fi
  api /api/status | python3 -c '
import sys, json
d = json.load(sys.stdin)
out = {"loaded_models": d.get("loaded_models"), "active_requests": d.get("active_requests"), "waiting_requests": d.get("waiting_requests")}
out.update({k: v for k, v in d.items() if "version" in k.lower() and isinstance(v, (str, int, float))})
print(json.dumps(out))' > "$1.tmp" 2>/dev/null && mv "$1.tmp" "$1" || echo '{"unreadable": true}' > "$1"; }
prepare_server() { # NAME_OF_RUN MODEL: before EVERY run: idle, nothing loaded but the model the run needs (our other models are unloaded)
  idle
  for m in $(loaded_list); do
    [ "$m" = "$2" ] && continue
    case " $OURS " in *" $m "*) unload_now $m;; esac
  done
  n=0
  until [ "$(busy)" = "0" ] && [ -z "$(for m in $(loaded_list); do [ "$m" = "$2" ] || echo $m; done)" ]; do
    sleep $SLEEP; n=$((n+1))
    [ $n -lt $WAITN ] || fail "server not free for $1 after 60 min (a request is active, or a model that is not ours is loaded beside $2)"
  done
  [ -f $W/server_$1.json ] || snapshot $W/server_$1.json; }

# ---------------------------------------------------------------- inputs and the server address
for f in $OPEN_A $OPEN_A3 eval/m4/SPLIT.json eval/m4/SPLIT_A3.json eval/m4/smoke_ids.json $CHUNKS $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json \
         scripts/eval2.py scripts/eval2_score.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_compare.py scripts/eval2_pilot_report.py \
         scripts/eval2_sealed_reading.py scripts/eval2_first_words.py scripts/run_guarded.py scripts/m4_e2_manifest.py; do
  [ -s $f ] || { [ $DRY = yes ] && { echo "input $f is missing or empty"; exit 1; }; echo "REFUSED: input $f is missing or empty; nothing started" >&2; exit 2; }
done
if [ $DRY = no ]; then
  rm -f $L/$N.failed $L/$N.done
  if want G || want O || want H1 || want H2 || want H3 || want H4 || want H5 || want E4; then
    export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
    export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
    [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || { echo "REFUSED: no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/); nothing started" >&2; exit 2; }
    export JUDGE_API_KEY=$OMLX_API_KEY
    BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  fi
  if want G || want O || want H1 || want H2 || want H3 || want H4 || want H5 || want SCORE; then
    for v in "$HID_J1" "$HID_J2" "$HID_R1" "$HID_R2"; do
      [ -n "$v" ] || { echo "REFUSED: --hidden-judged-items and --hidden-judged-rubrics (two files each, 24 and 36 rows) are required; nothing started" >&2; exit 2; }
    done
  fi
else
  # the temporary key, the synthetic hidden files, the fake server, a stub ledger
  [ -f $EVAL_HIDDEN_KEY ] || .venv/bin/python scripts/eval2.py keygen --path $EVAL_HIDDEN_KEY > /dev/null
  SD=$W/synthetic_hidden
  [ -f $SD/tier_a.jsonl.enc ] || $PYH synth --dir $SD --marker $MARK --drop-a $DROPA > /dev/null
  HID_A=$SD/tier_a.jsonl.enc; HID_A3=$SD/tier_a3.jsonl.enc; HID_J1=$SD/judged_1_items.jsonl.enc; HID_J2=$SD/judged_2_items.jsonl.enc
  HID_R1=$SD/judged_1_rubrics.jsonl.enc; HID_R2=$SD/judged_2_rubrics.jsonl.enc
  Q4=Qwen3.8-27B-4bit; BF16=mlx-community--Qwen3.8-27B-bf16
  echo "{\"data\": [{\"id\": \"$Q4\"}, {\"id\": \"$BF16\"}, {\"id\": \"$DS\"}, {\"id\": \"$GLM8\"}]}" > $W/models.json
  [ -f $FAKE ] || $PYH fake-status --file $FAKE init "$FAKE_LOADED"
  if [ $LSET = no ]; then
    LEDGER=$W/HIDDEN_LEDGER.md
    printf '| # | Date | Run | Configuration | Decision rule written beforehand | Result totals | Who |\n|---|---|---|---|---|---|---|\n' > $LEDGER
    printf '| 1 | 2026-01-01 | E2 starting point (dry run) | stub | (a) stub rule written before the run: the lower end of the interval above -5 points; (b) stub | | conductor |\n' >> $LEDGER
    printf '| 2 | 2026-01-01 | E3 checked answers (dry run) | stub | stub rule written before the run: at most 2.0 per 10 and harmful 0 on at least 60 answers | | conductor |\n' >> $LEDGER
  fi
  echo "dry run in $W (stub generator, stub judge, fake server status, temporary key, synthetic hidden files)"
fi
OURS="$DS $GLM8 $GLM4"
HIDDEN_ARGS="tier_a=$HID_A tier_a3=$HID_A3 judged_1=$HID_J1 judged_2=$HID_J2 rubrics_1=$HID_R1 rubrics_2=$HID_R2"
MI="$HID_A $HID_A3"                                   # the hidden mechanical items, 245 + 60 (a3)
JI=$H/judged_items.jsonl.enc; JR=$H/judged_rubrics.jsonl.enc   # the hidden judged items and rubrics, merged, harness-normalised
.venv/bin/python -m kafka_sre_rag verify > /dev/null || fail "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"

# the server's model names (a stage that needs no server does not need them)
if want G || want O || want H1 || want H2 || want H3 || want H4 || want H5 || want E4; then
  if [ $DRY = no ]; then
    api /v1/models > $W/models.json 2>/dev/null || true
    Q4=${M4_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || fail "4-bit model not listed by the server (or ambiguous)"
    BF16=${M4_BF16_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind bf16 < $W/models.json)} || fail "full-precision (bf16) model not listed by the server (or ambiguous)"
  fi
  OURS="$DS $GLM8 $GLM4 $Q4 $BF16"
fi
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }

# ---------------------------------------------------------------- PREPARE: the manifest (written once)
if want G || want O || want H1 || want H2 || want H3 || want H4 || want H5; then
  if [ ! -s $MANIFEST ]; then
    snapshot $W/server_now.json
    PA=""; [ $R2 = yes ] && PA="$PA --r2"; [ $FJ = yes ] && PA="$PA --f-judged"; [ $CROSS = yes ] || PA="$PA --no-cross-judge"
    $PYH prepare --out $MANIFEST --server-json $W/server_now.json --models-json $W/models.json --reading $READING $PA --hidden $HIDDEN_ARGS \
      || fail "PREPARE: the manifest could not be written"
    hb "$N: manifest written ($MANIFEST)"
  else
    echo "manifest exists: $MANIFEST (the hidden gate compares with it)"
  fi
fi

# ---------------------------------------------------------------- answering
answer_run() { # NAME MODEL MODE OUT EXPECTED "ITEMS..." [more harness arguments]
  name=$1; model=$2; mode=$3; out=$4; want_n=$5; items=$6; shift 6
  CK="--packs"; [ $mode = closed ] && CK=""
  if $PYH check-answers --file $out --expect $want_n $CK > $W/check_$name.out 2>&1; then echo "skip $name: $(cat $W/check_$name.out)"; return 0; fi
  if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; [ $mode = closed ] && GA="--backend stub"
  else GA="--backend openai --model $model"; fi
  tries=0
  while :; do
    prepare_server $name $model
    $G --stall-min 40 --log $L/$N.$name.log -- .venv/bin/python scripts/eval2.py answer --items $items --out $out \
      --config e2-$name $GA --extra-body "$EB" --mode $mode --catalogue $CATS --host-class "Mac Studio" "$@" \
      || fail "$name: answering failed (see $L/$N.$name.log)"
    simulate_load $model
    $PYH check-answers --file $out --expect $want_n $CK && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "$name: answers incomplete, with errors or empty after $tries passes"
  done
  hb "$N: $($PYH runline --name $name --file $out)"
  status RUNNING no "$N running: run $name finished; markers $L/$N.done / .failed"; }
score_open() { # NAME ANSWERS ITEMS
  s=${2%.jsonl}
  .venv/bin/python scripts/eval2_score.py --items $3 --answers $2 --scores $s.scores.jsonl --summary $s.summary.json --catalogues $CATS > /dev/null \
    || fail "$1: scorer failed"
  echo "scored $1: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["items_scored"], "items,", d["all_pass"], "all-pass,", d["missing_answers"], "missing")' $s.summary.json)"; }
score_hidden() { # NAME ANSWERS ITEMS...   scores .enc, summary and totals text in plain files (totals only)
  nm=$1; an=$2; shift 2
  [ -s $H/$nm.scores.jsonl.enc ] && [ -s $W/$nm.summary.json ] && [ $H/$nm.scores.jsonl.enc -nt $an ] && { echo "skip scoring $nm: scored already"; return 0; }
  .venv/bin/python scripts/eval2_score.py --items "$@" --answers $an --scores $H/$nm.scores.jsonl.enc --summary $W/$nm.summary.json --catalogues $CATS \
    > $W/$nm.score.out 2> $W/$nm.score.err || fail "$nm: the hidden scorer failed (see $W/$nm.score.err)"
  echo "scored $nm: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["items_scored"], "items,", d["all_pass"], "all-pass,", d["missing_answers"], "missing")' $W/$nm.summary.json)"; }
judge_run() { # TAG MODEL SETTING EXTRA_BODY SRCCHARS PASSES CONCURRENCY RUBRICS ITEMS "NAME=PATH ..." CACHE OUT EXPECTED  (hidden when the files are .enc)
  tag=$1; jm=$2; jset=$3; jeb=$4; jsrc=$5; jpasses=$6; jconc=$7; jrub=$8; jit=$9; shift 9; jans=$1; jcache=$2; jout=$3; jexp=$4
  if $PYH judge-done --file $jout --expect $jexp; then echo "skip judging $tag: $jout has $jexp valid rows"; return 0; fi
  for round in 1 2; do
    extra=""; [ $round = 2 ] && extra="--retry-invalid"
    prepare_server judge_$tag $jm
    if [ $DRY = yes ]; then JA="--stub-calls --settings-name stub-$jset"; else JA="--settings-name $jset"; fi   # the judge takes the address from OMLX_URL
    $G --stall-min 60 --log $L/$N.judge_$tag.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $jrub --items $jit \
      --chunks $CHUNKS --source-chars $jsrc --instruction v2 --coverage --answers $jans --judge-model $jm --judge-extra-body "$jeb" \
      --concurrency $jconc --passes $jpasses --cache $jcache --out $jout $JA $extra || fail "judging $tag, round $round, failed (see $L/$N.judge_$tag.log)"
    simulate_load $jm
    bad=$(.venv/bin/python scripts/m4_e2_manifest.py judge-done --file $jout --expect $jexp && echo 0 || echo 1)
    echo "judging $tag, round $round: $([ $bad = 0 ] && echo 'all rows valid' || echo 'rows missing or invalid')"
    [ $bad = 0 ] && break
  done
  [ $bad = 0 ] || fail "judging $tag: $jout does not hold $jexp valid rows after two rounds"
  hb "$N: judge $tag finished ($jexp rows, all valid)"; }
mark_passed() { # STAGE
  .venv/bin/python - "$1" $MANIFEST $W <<'PY'
import json, sys
json.dump({"stage": sys.argv[1], "passed": True, "commit": json.load(open(sys.argv[2]))["commit"]}, open(f"{sys.argv[3]}/{sys.argv[1]}.passed", "w"))
PY
}

# ---------------------------------------------------------------- G: byte-identity gate
if want G; then
  if [ -f $W/G.passed ]; then echo "skip G: passed earlier"; else
    hb "$N: G started: 60 open smoke items in Q4D against the stored A1 answers"
    status RUNNING no "$N running: gate G; markers $L/$N.done / .failed"
    REF=$STORED_A1
    if [ $DRY = yes ]; then   # the stored run is stood in for by a first stub run; M4_E2_DRY_DRIFT=yes changes one of its texts (the gate must then fail)
      REF=$W/ref_A1.jsonl
      [ -s $REF ] || { .venv/bin/python scripts/eval2.py answer --items $OPEN_A --out $REF --config e2-ref --backend stub --no-rerank --extra-body "$EB" \
          --mode default --catalogue $CATS --subset smoke > /dev/null || fail "G: the stub reference run failed"
        [ "${M4_E2_DRY_DRIFT:-no}" = yes ] && .venv/bin/python - $REF <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
rows[0]["text"] += " drift"
open(sys.argv[1], "w").write("".join(json.dumps(r) + "\n" for r in rows))
PY
        true; }
    fi
    answer_run G $Q4 default $W/ans_G.jsonl 60 $OPEN_A --subset smoke
    $PYH gate-g --answers $W/ans_G.jsonl --stored $REF --expect 60 > $W/gate_g.txt || { cat $W/gate_g.txt; fail "gate G failed: the answers are not byte-identical to the stored run ($W/gate_g.txt); the chain stops"; }
    cat $W/gate_g.txt
    mark_passed G
    hb "$N: gate G passed ($(cat $W/gate_g.txt))"
  fi
fi

# ---------------------------------------------------------------- O: open rehearsal
if want O; then
  if [ -f $W/O.passed ]; then echo "skip O: passed earlier"; else
    hb "$N: O started: a3 open 88 in Q4D, P0 on the smoke set (60 mechanical, 24 judged), score, identifier check, judge"
    status RUNNING no "$N running: rehearsal O; markers $L/$N.done / .failed"
    .venv/bin/python scripts/m4_e2_manifest.py merge --out $W/pilot2_items_h.jsonl --in $PIL_ITEMS --normalize-items pilot2 --allow-plain-out > /dev/null || fail "O: pilot2 items could not be prepared"
    answer_run O_Q4D_a3 $Q4 default $W/ans_O_Q4D_a3.jsonl 88 $OPEN_A3
    answer_run O_P0_smoke $Q4 closed $W/ans_O_P0_smoke.jsonl 60 $OPEN_A --subset smoke
    answer_run O_P0_judged $Q4 closed $W/ans_O_P0_judged.jsonl 24 $W/pilot2_items_h.jsonl
    score_open O_Q4D_a3 $W/ans_O_Q4D_a3.jsonl $OPEN_A3
    $PYH items-for --items $OPEN_A --answers $W/ans_O_P0_smoke.jsonl --out $W/smoke_items.jsonl > /dev/null || fail "O: smoke items file"
    score_open O_P0_smoke $W/ans_O_P0_smoke.jsonl $W/smoke_items.jsonl
    for r in O_Q4D_a3 O_P0_smoke; do
      .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_$r.jsonl --catalogues $CATS --totals-only > $W/idcheck_$r.txt || fail "identifier check on $r failed"
      echo "idcheck $r: $(sed -n 4p $W/idcheck_$r.txt)"
    done
    [ $DRY = yes ] || unload $Q4
    judge_run O_P0_judged $DS $JSET "$JEB" $SRC "1 2" 3 $PIL_RUBS $PIL_ITEMS "P0=$W/ans_O_P0_judged.jsonl" $W/O_judge_cache.jsonl $W/O_judge_results.jsonl 48
    [ $DRY = yes ] || unload $DS
    mark_passed O
    hb "$N: rehearsal O passed (all runs complete, scored, identifier check run, 48 judge rows valid)"
  fi
fi

# ---------------------------------------------------------------- the hidden gate (every launch that touches a hidden item)
if want H1 || want H2 || want H3 || want H4 || want H5; then
  snapshot $W/server_now.json; api /v1/models > $W/models_now.json 2>/dev/null || true
  GA_G="--tests run --tests-ok-file $W/tests.ok"; [ $DRY = yes ] && GA_G="--tests skip --allow-dirty"
  hb "$N: the hidden gate runs"
  $PYH gate --manifest $MANIFEST --ledger $LEDGER --work-dir $W --markers $W --exclude $L --server-json $W/server_now.json \
    --models-json $W/models_now.json --hidden $HIDDEN_ARGS $GA_G > $W/hidden_gate.out 2>&1 || { cat $W/hidden_gate.out; fail "the hidden gate REFUSED (see $W/hidden_gate.out); no hidden item was run"; }
  cat $W/hidden_gate.out
  .venv/bin/python - $MANIFEST $W <<'PY'
import json, sys
json.dump({"opened": True, "commit": json.load(open(sys.argv[1]))["commit"]}, open(f"{sys.argv[2]}/GATE.opened", "w"))
PY
  hb "$N: hidden gate open (all conditions hold)"
elif want SCORE; then
  [ -f $W/GATE.opened ] || fail "SCORE: the hidden gate has not opened for this work folder (no $W/GATE.opened): run a hidden stage first"
fi
done_hidden_files() { [ -s $JI ] && [ -s $JR ]; }
if want H1 || want H2 || want H3 || want H4 || want H5 || want SCORE; then
  done_hidden_files || { $PYH merge --out $JI --in $HID_J1 $HID_J2 --normalize-items e2 > /dev/null && $PYH merge --out $JR --in $HID_R1 $HID_R2 > /dev/null; } \
    || fail "the hidden judged files could not be merged"
fi

# ---------------------------------------------------------------- H1: Q4D and Q4S on the judged 60, R1, the blind bundles
if want H1; then
  hb "$N: H1 started: Q4D and Q4S on the hidden judged 60, R1, bundles for the sealed reading"
  status RUNNING no "$N running: H1; markers $L/$N.done / .failed"
  answer_run H1_Q4D $Q4 default $H/ans_Q4D_judged.jsonl.enc 60 $JI
  answer_run H1_Q4S $Q4 strict $H/ans_Q4S_judged.jsonl.enc 60 $JI --replay-packs $H/ans_Q4D_judged.jsonl.enc
  $PYH packs-equal --a $H/ans_Q4D_judged.jsonl.enc --b $H/ans_Q4S_judged.jsonl.enc || fail "H1: the packs of Q4S differ from Q4D (replay did not hold)"
  REV=$H/revise_Q4D_judged.jsonl.enc
  if $PYH check-answers --file $REV --expect 60 --no-text > /dev/null 2>&1; then echo "skip revise: finished"; else
    RA=""; if [ $R2 = yes ]; then if [ $DRY = yes ]; then RA="--r2 --backend stub"; else RA="--r2 --backend openai --model $Q4"; fi; prepare_server revise $Q4; fi
    $G --stall-min 40 --log $L/$N.revise.log -- .venv/bin/python scripts/eval2.py answer --items $JI --revise-from $H/ans_Q4D_judged.jsonl.enc \
      --out $REV --config e2-revise $RA --extra-body "$EB" --catalogue $CATS || fail "revise failed (see $L/$N.revise.log)"
    $PYH check-answers --file $REV --expect 60 --no-text || fail "revise: records incomplete or with errors"
  fi
  hb "$N: revise R1$([ $R2 = yes ] && echo ' and R2') done (60 records)"
  if [ -s $H/bundles/blind_key.json.enc ]; then echo "skip bundles: built"; else
    BA="default=$H/ans_Q4D_judged.jsonl.enc strict=$H/ans_Q4S_judged.jsonl.enc"; FS=6; SS=2
    .venv/bin/python scripts/eval2_sealed_reading.py bundle --items $JI --rubrics $JR --answers $BA --chunks $CHUNKS --out-dir $H/bundles \
      --seed $BSEED --first-sessions $FS --second-sessions $SS > $W/bundles.out || fail "the blind bundles could not be built"
    cat $W/bundles.out
  fi
  if [ $R2 = yes ] && [ ! -s $H/bundles_r2/blind_key.json.enc ]; then
    if $PYH patched --answers $H/ans_Q4D_judged.jsonl.enc --revise $REV --out $H/ans_Q4D_patched.jsonl.enc; then
      .venv/bin/python scripts/eval2_sealed_reading.py bundle --items $JI --rubrics $JR --answers default=$H/ans_Q4D_patched.jsonl.enc --chunks $CHUNKS \
        --out-dir $H/bundles_r2 --seed $BSEED --first-sessions 1 --second-sessions 1 > $W/bundles_r2.out || fail "the bundles of the patched answers could not be built"
    fi
  fi
  printf 'readings can start: bundles in %s (reading %s, R2 %s)\n' "$H/bundles" "$READING" "$R2" > $L/$N.readings_ready
  hb "$N: H1 done; the blind bundles are in $H/bundles: the readings can start (brief agents/m4-sealed-reading.md)" yes
fi

# ---------------------------------------------------------------- H2: Q4D, Q4S, P0 on the mechanical 305; P0 on the judged 60
if want H2; then
  hb "$N: H2 started: Q4D, Q4S, P0 on the hidden mechanical 305, P0 on the hidden judged 60"
  status RUNNING no "$N running: H2; markers $L/$N.done / .failed"
  answer_run H2_Q4D $Q4 default $H/ans_Q4D_mech.jsonl.enc 305 "$MI"
  answer_run H2_Q4S $Q4 strict $H/ans_Q4S_mech.jsonl.enc 305 "$MI" --replay-packs $H/ans_Q4D_mech.jsonl.enc
  $PYH packs-equal --a $H/ans_Q4D_mech.jsonl.enc --b $H/ans_Q4S_mech.jsonl.enc || fail "H2: the packs of Q4S differ from Q4D (replay did not hold)"
  answer_run H2_P0 $Q4 closed $H/ans_P0_mech.jsonl.enc 305 "$MI"
  answer_run H2_P0J $Q4 closed $H/ans_P0_judged.jsonl.enc 60 $JI
fi

# ---------------------------------------------------------------- H3: the full-precision build
if want H3; then
  hb "$N: H3 started: F and FR on the hidden mechanical 305$([ $FJ = yes ] && echo ', F on the judged 60')"
  status RUNNING no "$N running: H3; markers $L/$N.done / .failed"
  [ $DRY = yes ] || { unload $Q4; hb "$N: $Q4 unloaded; the full-precision model loads on the first request"; }
  answer_run H3_F $BF16 default $H/ans_F_mech.jsonl.enc 305 "$MI"
  answer_run H3_FR $BF16 default $H/ans_FR_mech.jsonl.enc 305 "$MI" --replay-packs $H/ans_Q4D_mech.jsonl.enc
  $PYH packs-equal --a $H/ans_Q4D_mech.jsonl.enc --b $H/ans_FR_mech.jsonl.enc || fail "H3: the packs of FR differ from Q4D (replay did not hold)"
  [ $FJ = no ] || answer_run H3_FJ $BF16 default $H/ans_F_judged.jsonl.enc 60 $JI --replay-packs $H/ans_Q4D_judged.jsonl.enc
  [ $DRY = yes ] || { unload $BF16; hb "$N: answering finished, $BF16 unloaded"; }
fi

# ---------------------------------------------------------------- H4: the judge; H5: the second judge on a subset
JSETS="Q4D=$H/ans_Q4D_judged.jsonl.enc Q4S=$H/ans_Q4S_judged.jsonl.enc P0=$H/ans_P0_judged.jsonl.enc"; NJS=3
[ $FJ = no ] || { JSETS="$JSETS F=$H/ans_F_judged.jsonl.enc"; NJS=4; }
if want H4; then
  hb "$N: H4 started: the judge $DS on $NJS answer sets of the hidden judged 60 (2 passes, $((NJS * 120)) calls)"
  status RUNNING no "$N running: H4 (judge); markers $L/$N.done / .failed"
  [ $DRY = yes ] || unload $Q4
  judge_run H4 $DS $JSET "$JEB" $SRC "1 2" 3 $JR $JI "$JSETS" $H/judge_cache.jsonl.enc $H/judge_results.jsonl.enc $((NJS * 120))
  [ $DRY = yes ] || { unload $DS; hb "$N: judging finished, $DS unloaded"; }
fi
if want H5 && [ $CROSS = yes ]; then
  hb "$N: H5 started: the second judge $GLM8 on a seeded 24-answer subset of Q4D (pass 1)"
  status RUNNING no "$N running: H5 (cross-check); markers $L/$N.done / .failed"
  $PYH subset --in $H/ans_Q4D_judged.jsonl.enc --out $H/ans_Q4D_cross.jsonl.enc --n 24 --seed 1 > /dev/null || fail "H5: the subset could not be drawn"
  judge_run H5 $GLM8 $CSET "$CEB" $SRC "1" 3 $JR $JI "Q4D=$H/ans_Q4D_cross.jsonl.enc" $H/cross_cache.jsonl.enc $H/cross_results.jsonl.enc 24
  [ $DRY = yes ] || { unload $GLM8; hb "$N: cross-check finished, $GLM8 unloaded"; }
  $PYH agreement --a $H/judge_results.jsonl.enc --b $H/cross_results.jsonl.enc --rubrics $JR --set Q4D --passno 1 > $W/cross_agreement.txt || fail "H5: the agreement could not be computed"
  cat $W/cross_agreement.txt; hb "$N: $(cat $W/cross_agreement.txt)"
fi

# ---------------------------------------------------------------- E4: first words through the service on open items (not a hidden look)
if want E4; then
  hb "$N: E4 started: first words through the service on open items"
  status RUNNING no "$N running: E4; markers $L/$N.done / .failed"
  prepare_server e4 $Q4
  if [ $DRY = yes ]; then
    $G --stall-min 40 --log $L/$N.e4.log -- .venv/bin/python scripts/eval2_first_words.py run --out $W/e4 --stub --n 2 || fail "E4 failed (see $L/$N.e4.log)"
  else
    $G --stall-min 40 --log $L/$N.e4.log -- .venv/bin/python scripts/eval2_first_words.py run --out $W/e4 --backend openai --model $Q4 \
      --extra-body "$EB" --catalogues $CATS || fail "E4 failed (see $L/$N.e4.log)"
  fi
  simulate_load $Q4
  [ $DRY = yes ] || { unload $Q4; hb "$N: E4 finished, $Q4 unloaded"; }
  [ -s $W/e4/summary.md ] && sed -n '1,12p' $W/e4/summary.md
  [ $DRY = yes ] && { echo M4_E2_DRY_RUN_DONE >> $L/$N.done; echo "dry run done (E4)"; exit 0; }
fi
# ---------------------------------------------------------------- SCORE: scoring, identifier check, coverage report, compare, report (CPU)
if want SCORE; then
  hb "$N: scoring and the report (CPU)"
  CFGS="Q4D Q4S P0 F FR"; JCFGS="Q4D Q4S P0"; [ $FJ = no ] || JCFGS="$JCFGS F"
  for c in $CFGS; do [ -s $H/ans_${c}_mech.jsonl.enc ] || fail "SCORE: $H/ans_${c}_mech.jsonl.enc is missing (stage H2 or H3 has not finished)"; done
  for c in $JCFGS; do [ -s $H/ans_${c}_judged.jsonl.enc ] || fail "SCORE: $H/ans_${c}_judged.jsonl.enc is missing"; done
  [ -s $H/judge_results.jsonl.enc ] || fail "SCORE: the judge results are missing (stage H4)"
  for c in $CFGS; do
    score_hidden ${c}_mech $H/ans_${c}_mech.jsonl.enc $MI
    .venv/bin/python scripts/eval2_idcheck.py --answers $H/ans_${c}_mech.jsonl.enc --catalogues $CATS --totals-only > $W/idcheck_${c}_mech.txt 2> $W/idcheck_${c}_mech.err || fail "identifier check on $c (mechanical) failed"
  done
  for c in $JCFGS; do
    .venv/bin/python scripts/eval2_idcheck.py --answers $H/ans_${c}_judged.jsonl.enc --catalogues $CATS --totals-only > $W/idcheck_${c}_judged.txt 2> $W/idcheck_${c}_judged.err || fail "identifier check on $c (judged) failed"
  done
  [ -s $H/findings_none.jsonl.enc ] || $PYH empty-enc --out $H/findings_none.jsonl.enc > /dev/null
  .venv/bin/python scripts/eval2_pilot_report.py --coverage --results $H/judge_results.jsonl.enc --rubrics $JR --findings $H/findings_none.jsonl.enc \
    --items $JI --by-kind --title "E2: coverage role, hidden judged sets" > $W/coverage_report.md 2> $W/coverage_report.err || fail "the coverage report failed (see $W/coverage_report.err)"
  CA=""; for c in $CFGS; do CA="$CA --config $c=$H/${c}_mech.scores.jsonl.enc"; done
  RD=""; [ -z "$RJSON" ] || RD="--reading-json $RJSON"
  .venv/bin/python scripts/eval2_compare.py e2 --items $HID_A --items $HID_A3 $CA --judged-results $H/judge_results.jsonl.enc --rubrics $JR \
    --judged-items $JI $RD --resamples 10000 --seed 1 --json $H/e2_compare.json.enc > $W/compare_e2.txt 2> $W/compare_e2.err || fail "eval2_compare.py e2 failed (see $W/compare_e2.err)"
  $PYH verdict --compare-out $W/compare_e2.txt ${MARGIN:+--reading $MARGIN} > $W/verdict.txt || fail "the verdict line could not be built"
  {
    echo "# E2: the starting point on the hidden half (4-bit, plain base, full precision)$([ $DRY = yes ] && echo ' [DRY RUN: stub generator, stub judge, synthetic hidden files; every number below is stub data]')"
    echo
    echo "Totals only: no item, answer, quote or per-item result is in this file. Written by \`scripts/$N.sh\`. Manifest: \`$MANIFEST\` (sha256 $(shasum -a 256 $MANIFEST | cut -c1-16)...); ledger rows 1 and 2 of \`$LEDGER\` hold the decision rules written before the run. Options: R2 $R2, reading $READING, F on the judged tier $FJ, cross-check $CROSS, margin reading: ${MARGIN:-not chosen}."
    echo
    echo "## Rule 1(a): the cost of 4-bit, and the starting figures (eval2_compare.py e2)"
    echo
    echo '```'; cat $W/compare_e2.txt; echo '```'
    echo
    echo "## Verdict of rule 1(a)"
    echo
    echo '```'; cat $W/verdict.txt; echo '```'
    echo
    echo "## Runs (answers per configuration; seconds are per answer and include search and retrieval where the run had them)"
    echo
    echo '```'
    for c in $CFGS; do $PYH runline --name ${c}_mech --file $H/ans_${c}_mech.jsonl.enc; done
    for c in $JCFGS; do $PYH runline --name ${c}_judged --file $H/ans_${c}_judged.jsonl.enc; done
    echo '```'
    echo
    echo "## Hidden scorer, totals per configuration (mechanical 305)"
    for c in $CFGS; do echo; echo "### $c"; echo '```'; cat $W/${c}_mech.score.out; echo '```'; done
    echo
    echo "## Identifier check, totals"
    for c in $CFGS; do echo; echo "### $c, mechanical 305"; echo '```'; cat $W/idcheck_${c}_mech.txt; echo '```'; done
    for c in $JCFGS; do echo; echo "### $c, judged 60"; echo '```'; cat $W/idcheck_${c}_judged.txt; echo '```'; done
    echo
    echo "## Coverage report of the judged sets (totals)"
    echo
    sed 's/^# /### /' $W/coverage_report.md
    echo
    echo "## Second judge, cross-check on 24 answers"
    echo
    if [ $CROSS = yes ] && [ -s $W/cross_agreement.txt ]; then echo '```'; cat $W/cross_agreement.txt; echo '```'; else echo "not run (--no-cross-judge, or stage H5 has not run)."; fi
    echo
    echo "## Sealed reading and rule 1(b), E3"
    echo
    echo "The readings are made by sealed sessions from the bundles in \`$H/bundles\` (R1 marks do not change a count; the bundles hold the first-pass answers, so the reader cannot tell the configurations apart). The totals come from \`scripts/eval2_sealed_reading.py totals --dir $H/bundles\`; they enter rule 1(b) through \`--reading-json\`. $([ -n "$RJSON" ] && echo "Reading totals given: $RJSON." || echo "Not given: rule 1(b) waits for the reading.")"
    echo
    echo "## Revise step (R1$([ $R2 = yes ] && echo ' and R2'))"
    echo
    echo "Records: \`$H/revise_Q4D_judged.jsonl.enc\` (60 records, encrypted). Counts are read by the reading script, not here."
  } > $REPORT.tmp && mv $REPORT.tmp $REPORT
  # nothing under the work folder may hold a hidden id in plain text (the logs, which carry ids by design, are outside or excluded)
  $PYH scan --dir $W --exclude $L --hidden $HIDDEN_ARGS > $W/scan_after.txt || fail "after the run: a plain-text file under $W holds a hidden id ($W/scan_after.txt)"
  cat $W/scan_after.txt
  if [ $DRY = yes ]; then
    echo M4_E2_DRY_RUN_DONE >> $L/$N.done
    sed -n '1,12p' $REPORT | cut -c1-240
    echo "dry run done: report from stub data in $REPORT"
    exit 0
  fi
  hb "$N: done; report $REPORT (totals only; the verdict needs --margin-reading, the readings' totals need --reading-json)" yes
  status DONE yes "$N done: $REPORT; the sealed readings and the Owner's margin reading are next"
  echo M4_E2_STARTING_POINT_DONE >> $L/$N.done
  exit 0
fi

echo "stages $STAGES finished; the report (SCORE) is written when that stage is included"
[ $DRY = yes ] && echo M4_E2_DRY_RUN_PARTIAL_DONE >> $L/$N.done
exit 0
