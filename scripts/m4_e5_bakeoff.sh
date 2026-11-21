#!/bin/sh
# M4 E5: the base-model bake-off (WP4b). This part builds the stage SCREEN: the open screen of three candidates, no hidden look.
# Plan: runs/m4/reports/e5_plan.md (section 1: configurations and the rule S1 to S4; section 3: the order; section 4: task T3).
# Templates and the stored reference answers: runs/m4/reports/e5_t4_templates.md. Candidates' ids and sizes: runs/m4/reports/bakeoff_load_test.md.
# Decisions of the Owner, 2026-11-21: Gemma 4 31B is left out; the same deterministic decoding for every model (greedy, thinking off through the
# same extra body, the golden prompts, the frozen index and retrieval, the 2,048-token limit, one request at a time); speed is a hard limit on the
# median over the judged answers, 1.15 times the reference's median on the same scenarios.
#
# OPEN ITEMS ONLY. This script never reads, lists or names anything under the hidden folder and refuses any argument that is a hidden-side path
# (a name ending in .enc, or a path with a hidden part). The later stages of the hidden look (ledger row 3) are added to this same script: the stage list,
# the work folder work/m4/e5/ and the markers runs/m4/logs/m4_e5_bakeoff.* are meant for them too.
#
# Stage SCREEN (resumable by its output files; a line per item; a done marker at the end; the server is the only GPU user, nothing runs locally
# on the GPU):
#   PREPARE   the manifest runs/m4/E5_SCREEN_MANIFEST.json, written once: the stored reference answers pinned by sha256 (work/m4/s6/ans_A1.jsonl, the 251 open tier A
#             items; work/m4/e2/ans_O_Q4D_a3.jsonl, the 88 open a3 items; work/m4/b1/ans_default.jsonl and work/m4/b2/ans_default.jsonl, the 60 open judged
#             scenarios), the open inputs, the frozen files, the scripts the answers, the scorer, the identifier check and the judge come from (the judge
#             script's hash is set beside the one in E2's manifest), the settings, the model ids as the server lists them. Every launch checks the pins.
#   REFERENCE (CPU) the reference is never regenerated: its mechanical answers are re-scored with the current scorer (251 + 88 items) and its identifier
#             check run; the two judged files are joined (60 scenarios, with the joined items and rubrics) and are judged later with the candidates'.
#   CANDIDATES in this order: gemma (Gemma 4 26B-A4B), glm (GLM-4.7-Flash), granite (Granite 4.2 30B); for each one model, loaded alone:
#             (1) the model is loaded by a small request, the server's memory is read from its status call before the first answer;
#             (2) determinism: the first 20 open smoke items answered twice, in two separate runs (byte-identical or not);
#             (3) default mode, end to end, on the 339 open mechanical items and on the 60 open judged scenarios; a candidate listed in --judged-first
#                 (default: granite) does the judged scenarios first: a running median of its answer times is printed after every answer, with the
#                 limit (1.15 x the reference's median), so that an operator can stop the chain (SIGTERM to the guard's PID) within the first ten;
#             (4) per answer seconds and tokens, answers cut at the limit, reasoning text or think tags, errors and empty answers after up to three
#                 passes (an error or an empty answer is not a reason to stop: it is counted, and rule S1 drops the candidate), the server's peak memory
#                 during the judged run (sampled in the background from the status call; if the call gives no figure, the report says so);
#             then the candidate is unloaded.
#   JUDGE     after ALL answering: DeepSeek (instruction v2, coverage role, settings deepseek-nothink-12k, two passes) on the 60 judged answers of the
#             reference and of every candidate, one run, one cache.
#   SCORE     (CPU) the scorer on every candidate's 339 answers, the identifier check, then `scripts/eval2_compare.py e5-screen` (rule S1 to S4; it prints
#             dropped or kept and, if three are kept, the two with the highest mechanical estimate that go on), the report runs/m4/reports/e5_screen.md.
# Before every model run the chain checks that the server has no active or waiting request and no model other than the one needed is loaded (our models
# that are not needed are unloaded; a model that is not ours is waited for up to 60 min, then the chain stops); every swap goes to runs/v3/HEARTBEAT.md.
# Everything that loads a model or the reranker runs under scripts/run_guarded.py. Before it starts, a real run waits until no other chain or guarded job runs.
#
# Running time, hours (inferred, to be corrected when the rates are known): basis the load test (runs/m4/reports/bakeoff_load_test.md: answer seconds 8-10
# Gemma, 8-10 GLM, 51-58 Granite, 30-39 the reference; retrieval 8.5-11 s for every model) scaled by the reference's hidden medians (24.9 s mechanical,
# 81.8 s judged) as in e5_plan.md section 3, plus the 40 smoke answers and the loads:
#   gemma 1.7-2.4 | glm 2.2-2.9 | granite 4.8-6.2 | judge (4 answer sets, 480 calls at 10.8 s of wall time) about 1.6 | CPU scoring and loads 0.3-0.5
#   total for all three about 10.5 to 13.5 h; one candidate alone: its figure plus the judge (0.8 for two sets) plus 0.3. Granite may be stopped after ten
#   judged answers when its running median is above the limit.
#
#   scripts/m4_e5_bakeoff.sh [options] > runs/m4/logs/m4_e5_bakeoff.out 2>&1 &
# Options (defaults):
#   --stages LIST            SCREEN (the only stage built so far)
#   --candidates LIST        comma list of gemma,glm,granite (default all three, in this order); one candidate can be run alone or dropped
#   --gemma-model ID --glm-model ID --granite-model ID   the model ids (defaults: the load test's gemma-4-26B-A4B-it-QAT-MLX-4bit, GLM-4.7-Flash-4bit,
#                            granite-4.2-30b-q4-mlx), resolved against the server's list (`eval2_compare.py pick-model --id`)
#   --judged-first LIST      candidates that do the judged scenarios before the rest (default granite)
#   --judge-model ID         (DeepSeek-V4.1-Flash-1017-oQ4e-mtp)   --report FILE (runs/m4/reports/e5_screen.md)   --manifest FILE (runs/m4/E5_SCREEN_MANIFEST.json)
#   --dry-run [--work DIR]   stub generator, stub judge, a fake server status; small samples (12 mechanical and 6 judged answers per candidate, the reference
#                            in full); nothing real is started; the real heartbeat, status and markers are not touched. Dry-run test hooks (environment):
#                            M4_E5_DRY_NONDET=KEY changes one answer of that candidate's repeat run, M4_E5_DRY_EMPTY=KEY blanks two of its mechanical answers.
# Markers: runs/m4/logs/m4_e5_bakeoff.done or .failed.   Work folder: work/m4/e5/ (git-ignored).
set -e
# open files only: refuse any argument that is a hidden-side path
for a in "$@"; do case $a in *.enc|*/hidden/*|hidden/*|*/hidden) echo "REFUSED: $a is a hidden-side path; the screen uses open files only; nothing started" >&2; exit 2;; esac; done
DRY=no; STAGES="SCREEN"; CANDS="gemma,glm,granite"; JFIRST="granite"
MODEL_gemma=gemma-4-26B-A4B-it-QAT-MLX-4bit; MODEL_glm=GLM-4.7-Flash-4bit; MODEL_granite=granite-4.2-30b-q4-mlx
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e; Q4=Qwen3.8-27B-4bit
REPORT=runs/m4/reports/e5_screen.md; MANIFEST=runs/m4/E5_SCREEN_MANIFEST.json; W=work/m4/e5; WSET=no; RSET=no; MSET=no
usage() { echo "usage: scripts/m4_e5_bakeoff.sh [--dry-run] [--work DIR] [--stages SCREEN] [--candidates gemma,glm,granite] [--gemma-model ID] [--glm-model ID] [--granite-model ID] [--judged-first LIST] [--judge-model ID] [--report FILE] [--manifest FILE] (see the header)" >&2; exit 2; }
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;; --stages) STAGES=${2:?}; shift;; --candidates) CANDS=${2:?}; shift;; --judged-first) JFIRST=${2:?}; shift;;
  --gemma-model) MODEL_gemma=${2:?}; shift;; --glm-model) MODEL_glm=${2:?}; shift;; --granite-model) MODEL_granite=${2:?}; shift;;
  --judge-model) DS=${2:?}; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --manifest) MANIFEST=${2:?}; MSET=yes; shift;; --work) W=${2:?}; WSET=yes; shift;;
  *) usage;; esac; shift; done
for s in $(echo $STAGES | tr ',' ' '); do case $s in SCREEN) ;; *) usage;; esac; done
for k in $(echo $CANDS | tr ',' ' '); do case $k in gemma|glm|granite) ;; *) echo "REFUSED: unknown candidate $k (gemma, glm, granite); nothing started" >&2; exit 2;; esac; done
want() { case ",$STAGES," in *",$1,"*) return 0;; esac; return 1; }
ALLK="gemma glm granite"   # the screen's order

L=runs/m4/logs; N=m4_e5_bakeoff; HB=runs/v3/HEARTBEAT.md; PHASE="M4 E5: base-model bake-off, open screen"
OPEN_A=eval/m4/open/tier_a.jsonl; OPEN_A3=eval/m4/open/tier_a3.jsonl
B1_ITEMS=eval/m4/b1/items.jsonl; B2_ITEMS=eval/m4/b2/items.jsonl; B1_RUBS=eval/m4/b1/rubrics.jsonl; B2_RUBS=eval/m4/b2/rubrics.jsonl
REF_A1=work/m4/s6/ans_A1.jsonl; REF_A3=work/m4/e2/ans_O_Q4D_a3.jsonl; REF_B1=work/m4/b1/ans_default.jsonl; REF_B2=work/m4/b2/ans_default.jsonl
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'; JSET=deepseek-nothink-12k; SRC=12000
PYE=".venv/bin/python scripts/m4_e5_screen_helper.py"
SLEEP=30; WAITN=120; MONSLEEP=60
NM=339; NJC=60; MECH_LIM=""; JUD_LIM=""   # the real numbers: 251 + 88 mechanical, 60 judged scenarios; a dry run takes small samples
if [ $DRY = yes ]; then
  NM=12; NJC=6; MECH_LIM="--limit 12"; JUD_LIM="--limit 6"
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m4_e5_dry.XXXXXX")
  mkdir -p $W; L=$W/logs; HB=$W/HEARTBEAT.md; SLEEP=0; WAITN=1; MONSLEEP=1
  [ $RSET = yes ] || REPORT=$W/report_from_stub_data.md
  [ $MSET = yes ] || MANIFEST=$W/E5_SCREEN_MANIFEST.json
  FAKE=$W/fake_status.json
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 0"
else
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 2"
fi
mkdir -p $W $W/ref $L runs/m4/reports
for k in $ALLK; do mkdir -p $W/$k; done
case "$W/" in work/m4/e5/|*/work/m4/e5/) [ $DRY = no ] || { echo "DRY RUN FAILED: a dry run never uses the real work folder"; exit 1; };; esac

hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> $HB; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" "$PHASE" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": sys.argv[4],
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
MONPID=""
cleanup() { [ -z "$MONPID" ] || kill $MONPID 2>/dev/null || true; }
trap cleanup EXIT
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
tick() { date +%s; }
timed() { # KEY STEP START   one line "key step start end" in the work folder
  echo "$1 $2 $3 $(tick)" >> $W/times.tsv; }

# ---------------------------------------------------------------- inputs and the server address
for f in $OPEN_A $OPEN_A3 $B1_ITEMS $B2_ITEMS $B1_RUBS $B2_RUBS $REF_A1 $REF_A3 $REF_B1 $REF_B2 eval/m4/smoke_ids.json $CHUNKS $CATS/CATALOGUES.json \
         runs/v3/INDEX_FROZEN.json scripts/eval2.py scripts/eval2_score.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_compare.py \
         scripts/run_guarded.py scripts/m4_e5_screen_helper.py; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
if [ $DRY = no ]; then
  rm -f $L/$N.failed $L/$N.done
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
fi

# ---------------------------------------------------------------- the server (real, or a fake status file in a dry run)
api() { if [ $DRY = yes ]; then case $1 in /v1/models) cat $W/models.json;; *) cat $FAKE;; esac
        else curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; fi; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
loaded_list() { api /api/status | python3 -c 'import sys,json; print(" ".join(json.load(sys.stdin).get("loaded_models") or []))' 2>/dev/null || echo "status-unreadable"; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep $SLEEP; n=$((n+1)); [ $n -lt $WAITN ] || fail "server still busy after 60 min"; done; }
unload_now() { # one of our models; only when no request is active or waiting; the server's answer is checked and logged
  idle
  if [ $DRY = yes ]; then $PYE fake-status --file $FAKE unload "$1"; ucode=200; ubody="dry run: fake status edited"
  else
    uresp=$(curl -s -m 300 -w '\n%{http_code}' -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" 2>/dev/null) || uresp=""
    ucode=$(printf '%s' "$uresp" | tail -n 1); ubody=$(printf '%s' "$uresp" | sed '$d' | tr '\n' ' ' | cut -c1-160 | sed 's/ *$//')
    case $ucode in [0-9][0-9][0-9]) ;; *) ucode=000;; esac
  fi
  case $ucode in
    2??) hb "$N: unloaded $1 (no active or waiting requests; the server answered $ucode: ${ubody:-no body})";;
    *) hb "$N: unload of $1 FAILED (the server answered $ucode: ${ubody:-no body})" yes; fail "the unload of $1 was not accepted (the server answered $ucode)";;
  esac; }
unload() { case " $(loaded_list) " in *" $1 "*) unload_now $1;; esac; }
simulate_load() { [ $DRY = no ] || $PYE fake-status --file $FAKE load "$1"; }   # a dry run: the model "loads" on its first request
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
  done; }
others() { pgrep -fl 'scripts/(m4|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }

# ---------------------------------------------------------------- the model ids (as the server lists them)
if [ $DRY = yes ]; then
  echo "{\"data\": [{\"id\": \"$MODEL_gemma\"}, {\"id\": \"$MODEL_glm\"}, {\"id\": \"$MODEL_granite\"}, {\"id\": \"$DS\"}, {\"id\": \"$GLM8\"}, {\"id\": \"$Q4\"}]}" > $W/models.json
  [ -f $FAKE ] || echo '{"loaded_models": [], "active_requests": 0, "waiting_requests": 0, "version": "dry-run"}' > $FAKE
else
  api /v1/models > $W/models.json 2>/dev/null || true
fi
SEL=$(echo $CANDS | tr ',' ' ')
CANDARGS=""
for k in $ALLK; do
  eval "cfg=\$MODEL_$k"
  res=$(.venv/bin/python scripts/eval2_compare.py pick-model --id "$cfg" < $W/models.json 2>/dev/null) || res=""
  eval "SID_$k=\"$res\""
  case " $SEL " in *" $k "*) [ -n "$res" ] || fail "candidate $k: the model id $cfg is not listed by the server (or ambiguous)";; esac
  CANDARGS="$CANDARGS --candidate $k=$cfg=$res"
done
OURS="$DS $GLM8 $GLM4 $Q4 $SID_gemma $SID_glm $SID_granite"
.venv/bin/python -m kafka_sre_rag verify > /dev/null || fail "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"

if [ $DRY = no ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, scripts/s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done
fi

# ---------------------------------------------------------------- PREPARE: the manifest (written once), the pins (every launch)
SETTINGS=$(.venv/bin/python - "$EB" "$JEB" "$JSET" "$SRC" <<'PY'
import json, sys
print(json.dumps({"extra_body": json.loads(sys.argv[1]), "decoding": "greedy (temperature 0, generators.py)", "max_tokens": "null: the config default of 2,048 (frozen.json)",
                  "mode": "default, end to end (the generator writes its own search terms)", "k": "k6", "rerank": True, "concurrency": 1,
                  "judge": {"extra_body": json.loads(sys.argv[2]), "settings_name": sys.argv[3], "instruction": "v2", "coverage": True, "passes": [1, 2], "source_chars": int(sys.argv[4])},
                  "smoke_determinism": "first 20 open smoke items (eval/m4/smoke_ids.json), two separate runs",
                  "speed_limit": "median judged answer at most 1.15 x the reference's median (S4)"}))
PY
)
if [ ! -s $MANIFEST ]; then
  snapshot $W/server_now.json
  $PYE manifest --out $MANIFEST --settings "$SETTINGS" $CANDARGS --selected "$CANDS" --models-json $W/models.json --server-json $W/server_now.json \
    $([ $DRY = yes ] && echo --dry-run) || fail "PREPARE: the manifest could not be written"
  hb "$N: manifest written ($MANIFEST)"
else
  echo "manifest exists: $MANIFEST"
fi
$PYE pin-check --manifest $MANIFEST || fail "a pinned file changed since prepare (see above); nothing was run"

# ---------------------------------------------------------------- REFERENCE (CPU): the stored answers, re-scored; the judged files joined
if want SCREEN; then
  hb "$N: SCREEN started (candidates: $CANDS)"
  status RUNNING no "$N running: SCREEN; markers $L/$N.done / .failed"
  $PYE merge-rows --out $W/ref/ans_B_judged.jsonl --in $REF_B1 $REF_B2 || fail "the reference's judged answers could not be joined"
  $PYE merge-items --out $W/items_judged.jsonl --in $B1_ITEMS $B2_ITEMS --domain b12 || fail "the judged items could not be joined"
  $PYE merge-rows --out $W/rubrics_judged.jsonl --in $B1_RUBS $B2_RUBS || fail "the judged rubrics could not be joined"
  REFMED=$($PYE median --file $W/ref/ans_B_judged.jsonl) || fail "no median of the reference's judged answers"
  echo "$REFMED" > $W/ref/median.txt
  echo "reference: the median judged answer is $REFMED s, so the speed limit S4 is $(.venv/bin/python -c "print(round(1.15*$REFMED,1))") s"
  for part in A1:$OPEN_A:$REF_A1 a3:$OPEN_A3:$REF_A3; do
    nm=${part%%:*}; rest=${part#*:}; items=${rest%%:*}; ans=${rest#*:}
    [ -s $W/ref/$nm.summary.json ] && [ $W/ref/$nm.summary.json -nt $ans ] && { echo "skip scoring reference $nm: scored already"; continue; }
    .venv/bin/python scripts/eval2_score.py --items $items --answers $ans --scores $W/ref/$nm.scores.jsonl --summary $W/ref/$nm.summary.json --catalogues $CATS > $W/ref/$nm.score.out 2>&1 \
      || fail "reference $nm: the scorer failed"
    .venv/bin/python scripts/eval2_idcheck.py --answers $ans --catalogues $CATS --totals-only > $W/ref/idcheck_$nm.txt || fail "reference $nm: the identifier check failed"
    echo "scored reference $nm: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["items_scored"], "items,", d["all_pass"], "all-pass,", d["missing_answers"], "missing")' $W/ref/$nm.summary.json)"
  done
  .venv/bin/python scripts/eval2_idcheck.py --answers $W/ref/ans_B_judged.jsonl --catalogues $CATS --totals-only > $W/ref/idcheck_judged.txt || fail "reference judged: the identifier check failed"
fi

# ---------------------------------------------------------------- answering
cand_run() { # KEY NAME MODEL EXPECTED "ITEMS" [more harness arguments]: one answers file W/KEY/ans_NAME.jsonl, up to three passes
  k=$1; name=$2; model=$3; want_n=$4; items=$5; shift 5
  out=$W/$k/ans_$name.jsonl
  st=0; $PYE answers-state --file $out --expect $want_n > $W/$k/state_$name.out 2>&1 || st=$?
  if [ $st = 0 ] || [ -f $out.gaveup ]; then echo "skip $k $name: $(cat $W/$k/state_$name.out)$([ -f $out.gaveup ] && echo ' (given up after three passes; counted by rule S1)')"; return 0; fi
  if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $model"; fi
  tries=0; t0=$(tick)
  while :; do
    prepare_server ${k}_$name $model
    rc=0
    $G --stall-min 40 --log $L/$N.${k}_$name.log -- .venv/bin/python scripts/eval2.py answer --items $items --out $out \
      --config e5-$k-$name $GA --extra-body "$EB" --mode default --catalogue $CATS --host-class "Mac Studio" --allow-commit-change \
      --max-consecutive-errors 25 "$@" || rc=$?
    simulate_load $model
    [ $rc != 130 ] || fail "$k $name: stopped by a signal (an operator stop); the run resumes from its output file"
    if [ $rc != 0 ]; then
      echo "$k $name: the run ended with exit code $rc (see $L/$N.${k}_$name.log); its missing answers are counted as errors by rule S1"
      touch $out.gaveup; break
    fi
    st=0; $PYE answers-state --file $out --expect $want_n > $W/$k/state_$name.out 2>&1 || st=$?
    cat $W/$k/state_$name.out
    [ $st != 0 ] || break
    tries=$((tries+1))
    if [ $tries -ge 3 ]; then echo "$k $name: errors or missing answers remain after three passes; they are counted by rule S1"; touch $out.gaveup; break; fi
  done
  timed $k $name $t0
  [ ! -s $out ] || hb "$N: $($PYE runline --name $k/$name --file $out)"
  status RUNNING no "$N running: $k $name finished; markers $L/$N.done / .failed"; }
plant() { # KEY FILE KIND ENVNAME: a dry run's planted defect (idempotent)
  [ $DRY = yes ] || return 0
  eval "v=\${$4:-}"
  case ",$v," in *",$1,"*) [ ! -s $2 ] || $PYE dry-inject --file $2 --kind $3;; esac; }
warm_load() { # KEY MODEL: the model loads on a small request; the server's memory is read from its status call before the first answer
  k=$1; model=$2
  if [ -s $W/$k/loaded.json ]; then echo "skip warm load $k: memory read earlier"; return 0; fi
  prepare_server load_$k $model
  if [ $DRY = yes ]; then simulate_load $model
  else
    code=$(curl -s -m 900 -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $OMLX_API_KEY" -H 'Content-Type: application/json' \
      -d "{\"model\": \"$model\", \"messages\": [{\"role\": \"user\", \"content\": \"Reply with the word ready.\"}], \"max_tokens\": 8, \"chat_template_kwargs\": {\"enable_thinking\": false}}" \
      "$BASE/v1/chat/completions" 2>/dev/null) || code=000
    case $code in 2??) ;; *) fail "$k: the first small request to load $model was not accepted (the server answered $code)";; esac
  fi
  api /api/status > $W/$k/status_loaded.json 2>/dev/null || echo '{}' > $W/$k/status_loaded.json
  mem=$($PYE mem-parse < $W/$k/status_loaded.json)
  ll=$(loaded_list)
  .venv/bin/python - $W/$k/loaded.json "$model" "$mem" "$ll" <<'PY'
import json, sys
loaded = sys.argv[4].split()
json.dump({"model": sys.argv[2], "loaded": loaded, "alone": loaded == [sys.argv[2]],
           "memory_gb": None if sys.argv[3] == "none" else float(sys.argv[3])}, open(sys.argv[1], "w"))
PY
  hb "$N: $model loaded for $k (memory with the model loaded alone: $([ "$mem" = none ] && echo 'the status call gave no figure' || echo "$mem GB"); loaded now: $ll)"; }
mon_start() { # KEY ANSWERS_FILE: in the background, the server's memory and the running median of the judged answers
  rm -f $W/$1/.stop
  ( while [ ! -e $W/$1/.stop ]; do
      printf '%s %s\n' "$(date +%s)" "$(api /api/status | $PYE mem-parse)" >> $W/$1/mem_samples.txt || true
      $PYE running-median --file $2 --label "$1 judged" --ref-median "$REFMED" --state $W/$1/.rm_state || true
      sleep $MONSLEEP
    done ) &
  MONPID=$!; }
mon_stop() { touch $W/$1/.stop; [ -z "$MONPID" ] || { kill $MONPID 2>/dev/null || true; wait $MONPID 2>/dev/null || true; }; MONPID=""; }
run_judged() { k=$1; model=$2
  out=$W/$k/ans_judged.jsonl
  mon_start $k $out
  cand_run $k judged $model $NJC $W/items_judged.jsonl $JUD_LIM
  mon_stop $k; }
run_mech() { k=$1; model=$2
  cand_run $k smokeA $model 20 $OPEN_A --subset smoke --limit 20
  cand_run $k smokeB $model 20 $OPEN_A --subset smoke --limit 20
  plant $k $W/$k/ans_smokeB.jsonl nondet M4_E5_DRY_NONDET
  cand_run $k mech $model $NM "$OPEN_A $OPEN_A3" $MECH_LIM
  plant $k $W/$k/ans_mech.jsonl empty M4_E5_DRY_EMPTY; }
if want SCREEN; then
  for k in $ALLK; do
    case " $SEL " in *" $k "*) ;; *) continue;; esac
    if [ -f $W/$k/cand.done ]; then echo "skip candidate $k: finished earlier"; continue; fi
    eval "model=\$SID_$k"
    hb "$N: candidate $k ($model) started"
    t0=$(tick)
    prepare_server load_$k $model
    warm_load $k $model
    case " $(echo $JFIRST | tr ',' ' ') " in
      *" $k "*) run_judged $k $model; run_mech $k $model;;
      *) run_mech $k $model; run_judged $k $model;;
    esac
    [ $DRY = yes ] || { unload $model; hb "$N: candidate $k finished, $model unloaded"; }
    [ $DRY = no ] || $PYE fake-status --file $FAKE unload $model
    timed $k total $t0
    touch $W/$k/cand.done
    hb "$N: candidate $k finished: $($PYE runline --name $k/mech --file $W/$k/ans_mech.jsonl)"
  done
fi

# ---------------------------------------------------------------- JUDGE: the reference's and every candidate's 60 judged answers, one run (after ALL answering)
DONE_K=""
for k in $ALLK; do [ -f $W/$k/cand.done ] && DONE_K="$DONE_K $k"; done
if want SCREEN; then
  [ -n "$DONE_K" ] || fail "no candidate is finished: nothing to judge"
  SETS="R=$W/ref/ans_B_judged.jsonl"
  for k in $DONE_K; do [ -s $W/$k/ans_judged.jsonl ] && SETS="$SETS $k=$W/$k/ans_judged.jsonl"; done
  EXP=$($PYE judge-expect --passes 2 --answers $SETS)
  JOUT=$W/judge_results.jsonl
  t0=$(tick)
  if $PYE judge-done --file $JOUT --expect $EXP; then echo "skip judging: $JOUT has $EXP valid rows"; else
    hb "$N: judge $DS on $(echo $SETS | wc -w | tr -d ' ') answer sets (reference and candidates), two passes, $EXP calls"
    status RUNNING no "$N running: judge; markers $L/$N.done / .failed"
    for round in 1 2; do
      extra=""; [ $round = 2 ] && extra="--retry-invalid"
      prepare_server judge $DS
      if [ $DRY = yes ]; then JA="--stub-calls --settings-name stub-$JSET"; else JA="--settings-name $JSET"; fi
      $G --stall-min 60 --log $L/$N.judge.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $W/rubrics_judged.jsonl --items $W/items_judged.jsonl \
        --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
        --concurrency 3 --passes 1 2 --cache $W/judge_cache.jsonl --out $JOUT $JA $extra || fail "judging, round $round, failed (see $L/$N.judge.log)"
      simulate_load $DS
      if $PYE judge-done --file $JOUT --expect $EXP; then echo "judging, round $round: all $EXP rows valid"; break; else echo "judging, round $round: rows missing or invalid"; fi
    done
    $PYE judge-done --file $JOUT --expect $EXP || fail "judging: $JOUT does not hold $EXP valid rows after two rounds"
    [ $DRY = yes ] || { unload $DS; hb "$N: judging finished, $DS unloaded"; }
    [ $DRY = no ] || $PYE fake-status --file $FAKE unload $DS
  fi
  timed judge judge $t0
fi

# ---------------------------------------------------------------- SCORE (CPU): the scorer, the identifier check, rule S1 to S4, the report
if want SCREEN; then
  hb "$N: scoring, the rule S1 to S4 and the report (CPU)"
  SC="--scores R=$W/ref/A1.scores.jsonl --scores R=$W/ref/a3.scores.jsonl"; CA=""; SMA=""; JA2="--judged-answers R=$W/ref/ans_B_judged.jsonl"; CK=""; NAMES=""
  for k in $DONE_K; do
    eval "model=\$SID_$k"
    NAMES="$NAMES $k=$model"; CK="$CK --candidate $k"
    if [ -s $W/$k/ans_mech.jsonl ]; then
      .venv/bin/python scripts/eval2_score.py --items $OPEN_A $OPEN_A3 --answers $W/$k/ans_mech.jsonl --scores $W/$k/mech.scores.jsonl --summary $W/$k/mech.summary.json \
        --catalogues $CATS > $W/$k/mech.score.out 2>&1 || fail "$k: the scorer failed"
      .venv/bin/python scripts/eval2_idcheck.py --answers $W/$k/ans_mech.jsonl --catalogues $CATS --totals-only > $W/$k/idcheck_mech.txt || fail "$k: the identifier check failed (mechanical)"
      echo "scored $k: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["items_scored"], "items,", d["all_pass"], "all-pass,", d["missing_answers"], "missing")' $W/$k/mech.summary.json)"
      SC="$SC --scores $k=$W/$k/mech.scores.jsonl"; CA="$CA --answers $k=$W/$k/ans_mech.jsonl"
    else
      CA="$CA --s1 $k.answers=$NM --s1 $k.errors=$NM --s1 $k.empty=0 --s1 $k.reasoning=0 --s1 $k.truncated=0"
    fi
    [ -s $W/$k/ans_judged.jsonl ] && {
      .venv/bin/python scripts/eval2_idcheck.py --answers $W/$k/ans_judged.jsonl --catalogues $CATS --totals-only > $W/$k/idcheck_judged.txt || fail "$k: the identifier check failed (judged)"
      JA2="$JA2 --judged-answers $k=$W/$k/ans_judged.jsonl"; }
    [ -s $W/$k/ans_smokeA.jsonl ] && [ -s $W/$k/ans_smokeB.jsonl ] && SMA="$SMA --smoke-a $k=$W/$k/ans_smokeA.jsonl --smoke-b $k=$W/$k/ans_smokeB.jsonl"
  done
  # the call as the plan defines the inputs; checked against `eval2_compare.py e5-screen --help` of 2026-11-21
  .venv/bin/python scripts/eval2_compare.py e5-screen --reference R $CK $SC $CA $SMA --judged-results $W/judge_results.jsonl --rubrics $W/rubrics_judged.jsonl $JA2 \
    > $W/screen_compare.txt 2> $W/screen_compare.err || fail "eval2_compare.py e5-screen failed (see $W/screen_compare.err)"
  cat $W/screen_compare.txt
  $PYE report --work $W --out $REPORT --candidates "$(echo $DONE_K | tr ' ' ',')" --names $NAMES --compare-out $W/screen_compare.txt --manifest $MANIFEST \
    --mem-every $MONSLEEP $([ $DRY = yes ] && echo --dry-run) || fail "the report could not be written"
  if [ $DRY = yes ]; then
    echo M4_E5_DRY_RUN_DONE >> $L/$N.done
    echo "dry run done: report from stub data in $REPORT"
    exit 0
  fi
  hb "$N: SCREEN done; report $REPORT" yes
  status DONE yes "$N done: $REPORT; the Owner or the conductor reads the screen's verdict"
  echo M4_E5_SCREEN_DONE >> $L/$N.done
  exit 0
fi
echo "stages $STAGES finished"
exit 0
