#!/bin/sh
# M6 DF: the D5 funnel as a chain, strict count (MILESTONE.md, row DF; agents/m6-plan.md section 3, rule 1).
# Written for the later real run; the real run is a separate task. Nothing here starts a model by itself: the expansions are server calls
# (the 4-bit base model, one request at a time) under scripts/run_guarded.py; the timing pass and the packs load two rerankers on the CPU
# (the frozen bge reranker and, for r5a, the pinned Qwen3 reranker at full precision) under the guard.
#
# What it measures, on the 50 scenarios of D5 (eval/m6/d5/): how many must points with a covering set are served, by the strict count (a
# whole covering set in the pack) and beside it the loose count (any chunk of `chunks`), for three settings on the same expansions:
#   frozen;  r4g2 (the runtime path: retrieval.select, profile r4, the grid setting r4g2);  r5a (r4g2's 24 scored candidates re-scored by
#   the Qwen3 reranker at full precision, batch 8) -- r5a is labelled everywhere "for information only: outside the memory limit of 1.5 GB".
# The rule (MILESTONE.md), by code: r4g2 goes on to B4 if, on the strict count over the points with a covering set, it gains at least +15.0
# percentage points over frozen, loses no frozen-served point, and its second reranker call takes at most 6.0 s median and 9.0 s at the 90th
# percentile (20 D5 questions fixed by a hash rule on their ids, uncached, the server idle). r5a never enters the verdict.
#
# Steps (each resumable by its output file; a line per item; a stage marker W/<stage>.passed with the hash of its output):
#   1. Gate (CPU, refusal exit 2, nothing written in a real run before it passes): inputs present; the funnel was not run before
#      (runs/m6/D5_FUNNEL_USED and runs/m6/RETRIEVAL_CHOSEN.json do not exist: D5's funnel decides which setting goes on, so it is run once);
#      sha256 of rubrics.jsonl and items.jsonl equal those in eval/m6/d5/CHUNK_CHECK.json; the frozen index hash (INDEX_FROZEN.json,
#      frozen.json, the file) and `kafka_sre_rag verify`; the pinned Qwen revision (and its weights' hash) in the hub cache; the start check
#      of a stored manifest and markers; then the server: the 4-bit base model is the only model there (our own other builds are unloaded when
#      no request is active or waiting, a model of another client is waited for up to 60 min, then REFUSED) and the server is idle.
#   2. The manifest, and the report header with the rule's text, are written to runs/m6/D5_FUNNEL.md BEFORE the expansions start.
#   3. Expansions of the 50 questions on the server, guarded: the same call as on the development sets and in the B4 chain (prompts.expand_messages,
#      300 tokens, temperature 0, thinking off, the 4-bit build); the call, prompt hash and settings go in the manifest. The 4-bit model is
#      unloaded afterwards when this chain loaded it.
#   4. The timing pass: the second reranker call of r4g2 (bge, window 512) and of r5a (Qwen) on 20 D5 questions (the first 20 ids by sha256 of the
#      id), uncached, the server idle (checked again here).
#   5. The packs of the three settings (every r4g2 and r5a pack must start with the frozen pack, else the chain stops).
#   6. The counts (scripts/m5_b4_served.py compute is the counter), the rule's verdict, per-item rows (JSON), the report, runs/m6/RETRIEVAL_CHOSEN.json
#      (setting, parameters, settings hash and verdict, for scripts/m5_b4_check.sh), then the marker runs/m6/D5_FUNNEL_USED.
# Heartbeat lines for the job starts and ends go to runs/v3/HEARTBEAT.md. Markers: runs/m6/logs/m6_d5_funnel.done or .failed.
#
# Estimated duration of the real run (inferred from M0's and M5's measured calls, not measured on D5): about 0.6 to 1.2 h. 50 expansions at
# about 7 s median (4 to 8 min) plus the model load; the timing pass 20 questions x (bge about 3 s + Qwen about 5 s + the frozen call about 4 s each)
# 5 to 8 min plus loading the two models 1 to 2 min; the packs 50 x (3 frozen calls + 2 second calls) 20 to 40 min; the counts and the report 1 min.
#
#   scripts/m6_d5_funnel.sh > runs/m6/logs/m6_d5_funnel.out 2>&1 &
#   scripts/m6_d5_funnel.sh --dry-run [--work DIR]   # synthetic batch in a temporary folder, stub model, stand-in rerankers, fake server status;
#                                                    # never touches D5, the real marker, record, report or heartbeat
# Options: --dry-run, --work DIR, --report FILE, --used-marker FILE, --chosen-out FILE, --items F --rubrics F --chunk-check F.
# Dry-run only: --synth-flaw none|rubrics-hash, --stop-after expand|timing|packs, --synth-stop-after-items N, --fake-loaded JSON.
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/ (never printed, never written).
set -e
DRY=no; WSET=no; W=work/m6/d5_funnel; REPORT=runs/m6/D5_FUNNEL.md; RSET=no; USED=runs/m6/D5_FUNNEL_USED; USET=no
CHOSENOUT=runs/m6/RETRIEVAL_CHOSEN.json; COSET=no
ITEMS=eval/m6/d5/items.jsonl; RUBS=eval/m6/d5/rubrics.jsonl; CC=eval/m6/d5/CHUNK_CHECK.json; INSET=no
FLAW=none; STOP=""; STOPITEMS=0; FAKE_LOADED='[]'
usage() { echo "usage: scripts/m6_d5_funnel.sh [--dry-run] [--work DIR] [--report FILE] [--used-marker FILE] [--chosen-out FILE] [--items F --rubrics F --chunk-check F] (see the header)" >&2; exit 2; }
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --used-marker) USED=${2:?}; USET=yes; shift;;
  --chosen-out) CHOSENOUT=${2:?}; COSET=yes; shift;;
  --items) ITEMS=${2:?}; INSET=yes; shift;; --rubrics) RUBS=${2:?}; INSET=yes; shift;; --chunk-check) CC=${2:?}; INSET=yes; shift;;
  --synth-flaw) FLAW=${2:?}; shift;; --stop-after) STOP=${2:?}; shift;; --synth-stop-after-items) STOPITEMS=${2:?}; shift;; --fake-loaded) FAKE_LOADED=${2:?}; shift;;
  *) usage;; esac; shift; done
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
if [ $DRY = no ]; then
  { [ "$FLAW" = none ] && [ -z "$STOP" ] && [ "$STOPITEMS" = 0 ] && [ "$FAKE_LOADED" = '[]' ]; } || refuse "--synth-flaw, --stop-after, --synth-stop-after-items and --fake-loaded are for dry runs only"
fi
case $FLAW in none|rubrics-hash) ;; *) usage;; esac
case "$STOP" in ""|expand|timing|packs) ;; *) usage;; esac

PYS=".venv/bin/python scripts/m6_d5_funnel.py"
L=runs/m6/logs; N=m6_d5_funnel; HB=runs/v3/HEARTBEAT.md; SLEEP=30; WAITN=120
PHASE="M6 DF: the D5 funnel, strict count"
CHUNKS=work/v3/chunks_s1.jsonl; Q4=""
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EXP_ARGS=""; PACK_ARGS=""; GATEQ=""
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m6_df_dry.XXXXXX")
  mkdir -p $W
  L=$W/logs; HB=$W/HEARTBEAT.md; SLEEP=0; WAITN=1
  [ $RSET = yes ] || REPORT=$W/D5_FUNNEL.md
  [ $USET = yes ] || USED=$W/D5_FUNNEL_USED
  [ $COSET = yes ] || CHOSENOUT=$W/RETRIEVAL_CHOSEN.json
  case "$USED $REPORT $CHOSENOUT" in *runs/m6/D5_FUNNEL_USED*|*runs/m6/D5_FUNNEL.md*|*runs/m6/RETRIEVAL_CHOSEN.json*) refuse "a dry run never uses the real marker, report or record";; esac
  [ $INSET = no ] || case "$ITEMS $RUBS $CC" in *eval/m6/d5*) refuse "a dry run never uses the real D5 files";; esac
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 0"
  EXP_ARGS="--stub"; PACK_ARGS="--stand-in"; GATEQ="--dry-run"
  if [ $INSET = no ]; then   # the synthetic batch of the dry run (made once, in the work folder)
    IN=$W/in
    [ -s $IN/items.jsonl ] || $PYS synth --dir $IN --flaw $FLAW > /dev/null || refuse "the synthetic batch could not be made"
    ITEMS=$IN/items.jsonl; RUBS=$IN/rubrics.jsonl; CC=$IN/CHUNK_CHECK.json
  fi
else
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 2"
fi

# ---------------------------------------------------------------- 1. the gate (CPU; refusal = exit 2)
for f in $ITEMS $RUBS $CC kafka_sre_rag/candidate.json kafka_sre_rag/frozen.json runs/v3/INDEX_FROZEN.json runs/m5/RERANKER_R5.json $CHUNKS \
         scripts/run_guarded.py scripts/m5_b4_served.py scripts/m6_d5_funnel.py scripts/r5_qwen.py scripts/eval2_compare.py; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
[ ! -e $USED ] || refuse "$USED exists: the D5 funnel was run already (it decides which setting goes on, so it is run once); a second run is refused"
[ ! -e $CHOSENOUT ] || [ -e $W/DF_MANIFEST.json ] || refuse "$CHOSENOUT exists but $W/DF_MANIFEST.json does not; the record is not overwritten"
[ ! -e $REPORT ] || [ -e $W/DF_MANIFEST.json ] || refuse "$REPORT exists but $W/DF_MANIFEST.json does not; the report is not overwritten"
.venv/bin/python scripts/m5_b4_served.py gate-files --items $ITEMS --rubrics $RUBS --chunk-check $CC || refuse "the batch files differ from the hashes in the chunk check (the batch changed after its check)"
.venv/bin/python - <<'PY' || refuse "the frozen index hash does not agree between INDEX_FROZEN.json, frozen.json and the index file"
import hashlib, json
a = json.load(open("runs/v3/INDEX_FROZEN.json"))["sha256"]
b = json.load(open("kafka_sre_rag/frozen.json"))["index"]["sha256"]
h = hashlib.sha256()
with open("work/v3/chunks_s1.jsonl", "rb") as f:
    for blk in iter(lambda: f.read(1 << 20), b""):
        h.update(blk)
ok = a == b == h.hexdigest()
print(f"gate: frozen index sha256 {h.hexdigest()[:8]}...{h.hexdigest()[-7:]} {'equals' if ok else 'DIFFERS FROM'} INDEX_FROZEN.json and frozen.json")
raise SystemExit(0 if ok else 1)
PY
.venv/bin/python -m kafka_sre_rag verify > /dev/null || refuse "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"
echo "gate: kafka_sre_rag verify passed"
$PYS gate-qwen $GATEQ || refuse "the pinned Qwen reranker is not usable (not in the hub cache, another revision, or its weights differ from the pin)"
SC_MODEL=$([ -s $W/DF_MANIFEST.json ] && .venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["options"]["model"])' $W/DF_MANIFEST.json || echo "Qwen3.8-27B-4bit")
SC_DRY=""; [ $DRY = no ] || SC_DRY="--dry-run"
start_check() { # MODEL
  $PYS start-check --items $ITEMS --rubrics $RUBS --chunk-check $CC --work $W --model "$1" $SC_DRY; }
sc=0; start_check "$SC_MODEL" || sc=$?
[ $sc = 0 ] || refuse "the start check failed (exit code $sc, see above)"

# ---------------------------------------------------------------- state, helpers (nothing is written before this point in a real run)
mkdir -p $W $L
hb() { printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> $HB; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" "$PHASE" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"), "phase": sys.argv[4],
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; the 4-bit model may still be loaded"; echo "STOPPED: $1"; exit 1; }
stage_done() { [ -f $W/$1.passed ]; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }
NEED=$(grep -c . $ITEMS | tr -d ' ')
echo "planned: $NEED expansions on the server (4-bit model), the timing pass on 20 questions, then $NEED packs of three settings (reranker, CPU); about 0.6 to 1.2 h (inferred, not measured on D5)"
[ $DRY = yes ] || rm -f $L/$N.failed $L/$N.done

# ---------------------------------------------------------------- the server (real, or a fake status file in a dry run)
FAKE=$W/fake_status.json
fake_edit() { python3 - "$FAKE" "$1" "${2-}" <<'PY'
import json, sys
p, op, m = sys.argv[1:4]
d = json.load(open(p))
l = [x for x in d["loaded_models"] if x != m]
d["loaded_models"] = l + [m] if op == "load" else l
json.dump(d, open(p, "w"))
PY
}
api() { if [ $DRY = yes ]; then case $1 in /v1/models) cat $W/models.json;; *) cat $FAKE;; esac
        else curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; fi; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
loaded_list() { api /api/status | python3 -c 'import sys,json; print(" ".join(json.load(sys.stdin).get("loaded_models") or []))' 2>/dev/null || echo "status-unreadable"; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep $SLEEP; n=$((n+1)); [ $n -lt $WAITN ] || refuse "the server is not idle (an active or waiting request) after 60 min"; done; }
unload_now() { # one of our models; only when no request is active or waiting
  idle
  if [ $DRY = yes ]; then fake_edit unload "$1"; ucode=200
  else
    ucode=$(curl -s -m 300 -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" 2>/dev/null) || ucode=000
  fi
  case $ucode in 2??) hb "$N: unloaded $1 (no active or waiting requests; the server answered $ucode)";;
    *) hb "$N: unload of $1 FAILED (the server answered $ucode)" yes; fail "the unload of $1 was not accepted (the server answered $ucode)";; esac; }
others() { pgrep -fl 'scripts/(m4|m5|m6|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }

if ! stage_done expand; then
  if [ $DRY = no ]; then
    export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
    export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
    [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
    BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
    hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, s1_*.sh) and no guarded job runs, then the server check"
    status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
    n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || refuse "another chain or guarded job still runs after 12 h: $(others | head -3)"; done
    api /v1/models > $W/models.json 2>/dev/null || true
    Q4=${M4_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || refuse "the 4-bit model is not listed by the server (or ambiguous)"
  else
    Q4=Qwen3.8-27B-4bit
    echo "{\"data\": [{\"id\": \"$Q4\"}]}" > $W/models.json
    [ -f $FAKE ] || echo "{\"loaded_models\": $FAKE_LOADED, \"active_requests\": 0, \"waiting_requests\": 0}" > $FAKE
  fi
  # the server gate: idle, and the 4-bit model the only model loaded (our own other builds are unloaded, another client's are waited for)
  idle
  for m in $(loaded_list); do
    [ "$m" = "$Q4" ] && continue
    case " $DS $GLM8 $GLM4 " in *" $m "*) unload_now $m;; esac
  done
  n=0
  until [ "$(busy)" = "0" ] && [ -z "$(for m in $(loaded_list); do [ "$m" = "$Q4" ] || echo $m; done)" ]; do
    sleep $SLEEP; n=$((n+1))
    [ $n -lt $WAITN ] || refuse "the server is not free after 60 min (a request is active, or a model that is not ours is loaded beside $Q4)"
  done
  case " $(loaded_list) " in *" $Q4 "*) ;; *) echo "$Q4 not loaded at the start: this chain loads it on the first request" > $W/q4_loaded_by_chain;; esac
  echo "gate: server idle; $Q4 is the only model that may be loaded"
else
  Q4=$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["options"]["model"])' $W/DF_MANIFEST.json)
  if [ $DRY = no ]; then   # a resume after the expansions: the server is still needed idle for the timing pass; the address and key are read here
    export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
    export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
    [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
    BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  fi
fi
sc=0; start_check "$Q4" || sc=$?
[ $sc = 0 ] || refuse "the start check failed for the model $Q4 (exit code $sc, see above)"

# ---------------------------------------------------------------- 2. the manifest (written once) and the report header (before the expansions)
NEWMAN=no; [ -s $W/DF_MANIFEST.json ] || NEWMAN=yes
$PYS manifest --items $ITEMS --rubrics $RUBS --chunk-check $CC --work $W --model "$Q4" $SC_DRY --models-json $W/models.json || fail "the manifest could not be written"
if [ $NEWMAN = yes ]; then
  mkdir -p "$(dirname $REPORT)"
  $PYS header --phase header --items $ITEMS --rubrics $RUBS --model "$Q4" $SC_DRY --out $REPORT || fail "the report header could not be written"
fi

# ---------------------------------------------------------------- 3. expansions
EXPS=$W/expansions.jsonl
if stage_done expand; then
  echo "skip expansions: passed earlier ($(rows $EXPS) rows)"
else
  hb "$N: expansions started ($NEED items, $Q4)"
  status RUNNING no "$N running: expansions; markers $L/$N.done / .failed"
  [ $DRY = no ] || fake_edit load "$Q4"
  $G --stall-min 40 --log $L/$N.expand.log -- .venv/bin/python scripts/m6_d5_funnel.py expand --items $ITEMS --out $EXPS --model "$Q4" $EXP_ARGS \
      $([ "$STOPITEMS" = 0 ] || echo "--stop-after-items $STOPITEMS") || fail "expansions failed (see $L/$N.expand.log)"
  [ "$(rows $EXPS)" = "$NEED" ] || fail "$EXPS has $(rows $EXPS) rows, not $NEED"
  $PYS mark --work $W --stage expand expansions.jsonl
  hb "$N: expansions done ($NEED rows)"
  if [ -f $W/q4_loaded_by_chain ]; then
    unload_now $Q4
    rm -f $W/q4_loaded_by_chain
  fi
fi
[ "$STOP" != expand ] || { echo "dry run stopped after the expansions (--stop-after expand)"; exit 0; }

# ---------------------------------------------------------------- 4. the timing pass (the server idle)
TIM=$W/timing.jsonl
if stage_done timing; then
  echo "skip timing: passed earlier ($(rows $TIM) rows)"
else
  idle
  hb "$N: timing pass started (second reranker call of r4g2 and r5a on the 20 questions of the hash rule, server idle)"
  status RUNNING no "$N running: timing; markers $L/$N.done / .failed"
  $G --stall-min 15 --log $L/$N.timing.log -- .venv/bin/python scripts/m6_d5_funnel.py timing --items $ITEMS --expansions $EXPS --out $TIM $PACK_ARGS \
      || fail "the timing pass failed (see $L/$N.timing.log)"
  NT=$($PYS ids --items $ITEMS | grep -c .)
  [ "$(rows $TIM)" = "$NT" ] || fail "$TIM has $(rows $TIM) rows, not $NT"
  $PYS mark --work $W --stage timing timing.jsonl
  hb "$N: timing pass done ($NT rows)"
fi
[ "$STOP" != timing ] || { echo "dry run stopped after the timing pass (--stop-after timing)"; exit 0; }

# ---------------------------------------------------------------- 5. packs (rerankers under the guard)
PACKS=$W/packs.jsonl
if stage_done packs; then
  echo "skip packs: passed earlier ($(rows $PACKS) rows)"
else
  hb "$N: packs started (frozen, r4g2 and r5a, rerankers on the CPU)"
  status RUNNING no "$N running: packs; markers $L/$N.done / .failed"
  $G --stall-min 15 --log $L/$N.packs.log -- .venv/bin/python scripts/m6_d5_funnel.py packs --items $ITEMS --expansions $EXPS --out $PACKS $PACK_ARGS \
      || fail "packs failed (see $L/$N.packs.log; an r4g2 or r5a pack that does not start with the frozen pack stops the chain)"
  [ "$(rows $PACKS)" = "$NEED" ] || fail "$PACKS has $(rows $PACKS) rows, not $NEED"
  $PYS mark --work $W --stage packs packs.jsonl
  hb "$N: packs done ($NEED rows)"
fi
[ "$STOP" != packs ] || { echo "dry run stopped after the packs (--stop-after packs)"; exit 0; }

# ---------------------------------------------------------------- 6. the counts, the verdict, the report, the record and the marker
if stage_done served; then
  echo "skip counts: passed earlier"
else
  $PYS served --items $ITEMS --rubrics $RUBS --packs $PACKS --timing $TIM --json-out $W/served.json --text-out $W/served.txt --rows-out $W/rows.json \
      --chosen-out $CHOSENOUT --report $REPORT $SC_DRY > $W/served.out || { cat $W/served.out; fail "the counts failed (see above)"; }
  $PYS mark --work $W --stage served served.json served.txt rows.json
fi
cat $W/served.txt
$PYS header --phase final --items $ITEMS --rubrics $RUBS --model "$Q4" $SC_DRY --out $REPORT --expansions $EXPS --packs $PACKS \
  --served-json $W/served.json --served-text $W/served.txt --rows $W/rows.json --chosen $CHOSENOUT || fail "the report could not be written"
VERDICT=$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["verdict"]["go"] and "GO" or "STOP")' $W/served.json)
mkdir -p "$(dirname $USED)"
( set -C; printf 'used %s\nverdict %s\nreport %s\nrecord %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$VERDICT" "$REPORT" "$CHOSENOUT" > $USED ) \
  || fail "$USED could not be written (it exists already?)"
hb "$N: done; the funnel's verdict for r4g2: $VERDICT; report $REPORT; record $CHOSENOUT; marker $USED" yes
status DONE yes "$N done: verdict for r4g2 $VERDICT; report $REPORT"
if [ $DRY = yes ]; then echo M6_D5_FUNNEL_DRY_RUN_DONE >> $L/$N.done; echo "dry run done: verdict $VERDICT (stub data, not a measurement); report $REPORT; marker $USED"
else echo M6_D5_FUNNEL_DONE >> $L/$N.done; echo "done: verdict $VERDICT; report $REPORT; record $CHOSENOUT; marker $USED"; fi
