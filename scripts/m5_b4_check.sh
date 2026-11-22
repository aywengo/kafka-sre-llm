#!/bin/sh
# M5 R4-chain: the one-time check of the chosen r4 setting on B4 (rule X1 of MILESTONE.md). B4 (eval/m4/b4) is used ONCE.
# Written for the later real run; the real run is a separate task. Nothing here starts a model by itself: the expansions are server calls
# (the 4-bit base model, one request at a time) under scripts/run_guarded.py, the packs load the reranker under the guard.
#
# What it measures. Each must point of B4 carries chunk_sets (sets of one or two chunk ids, each verified to state the point's whole fact);
# points with empty chunk_sets (not_indexed or no_covering_set) are outside the count. Two counts of "served" for a pack:
#   strict  at least one whole set of the point is in the pack;
#   loose   at least one chunk of the point's `chunks` is in the pack (the count used on B1 and B2).
# The rule X1, on the indexed points: served by r4 minus served by frozen at least +15.0 percentage points, and at most 5 % of the
# frozen-served points not served by r4. The Owner has not decided which count decides, so --decide strict|loose has NO default and the
# chain refuses (exit 2) without it. Both counts are always printed; the one not named is labelled "reported, decides nothing".
#
# Steps (each resumable by its output file; a line per item; a stage marker W/<stage>.passed with the hash of its output):
#   1. Gate (CPU, refusal exit 2, nothing written outside the work folder in a dry run, nothing at all in a real run before it passes):
#      --decide given; inputs present; runs/m5/B4_USED does not exist; sha256 of rubrics.jsonl and items.jsonl equal those in
#      eval/m4/b4/CHUNK_CHECK.json; runs/m5/PROFILE_R4.json says r4 is CHOSEN (not "candidate, not chosen" or a placeholder) and its settings
#      hash equals candidate.json's r4 hash; the frozen index hash (INDEX_FROZEN.json, frozen.json, the file) and `kafka_sre_rag verify`;
#      the start check of a stored manifest and markers (pinned files, options, outputs of finished stages); then the server: the 4-bit
#      base model is the only model there (our own other builds are unloaded when no request is active or waiting, a model of another
#      client is waited for up to 60 min, then REFUSED) and the server is idle.
#   2. The report header (the rule's text and the --decide value) is written to runs/m5/B4_CHECK.md BEFORE the expansions start.
#   3. Expansions of the 30 questions on the server, guarded: the same call as on the development sets (scripts/eval2.py answer -> evalhooks.run_item;
#      scripts/m4_b2_batch.sh): prompts.expand_messages(question + Evidence), 300 tokens, temperature 0, thinking off, the 4-bit build.
#      The call, prompt hash and settings are recorded in the run's manifest. The 4-bit model is unloaded afterwards when this chain loaded it.
#   4. Packs, guarded (reranker): per item the frozen pack and the r4 pack with the runtime's own code (kafka_sre_rag.retrieval.select);
#      every r4 pack must start with the frozen pack (else the chain stops).
#   5. scripts/m5_b4_served.py: both counts, per-item rows (JSON), the rule's verdict for --decide.
#   6. The report; then the marker runs/m5/B4_USED with the date and the verdict, so that a second run is refused.
# Heartbeat lines for the job starts and ends go to runs/v3/HEARTBEAT.md. Markers: runs/m5/logs/m5_b4_check.done or .failed.
#
# Estimated duration of the real run (inferred, not measured on B4): about 0.2 to 0.5 h. 30 expansions at 7.0 s median and 10.4 s p90 (stored B2
# runs on the 4-bit model: 3.5 to 5.5 min) plus the model load; 30 packs, each the frozen reranker call (about 4 s) and the r4 call (about 3 s at the
# 512 window, 6 to 8 s at 1024; design R0, unmeasured), 3 to 8 min; checks and the report 1 min.
#
#   scripts/m5_b4_check.sh --decide strict|loose > runs/m5/logs/m5_b4_check.out 2>&1 &
#   scripts/m5_b4_check.sh --decide strict|loose --dry-run [--work DIR]   # synthetic batch in a temporary folder, stub model, stand-in reranker,
#                                                                         # fake server status; never touches B4, the real marker, report or heartbeat
# Options: --decide, --dry-run, --work DIR, --report FILE, --used-marker FILE, --items F --rubrics F --chunk-check F --profile-record F.
# Dry-run only: --synth-flaw none|rubrics-hash|placeholder, --stop-after expand|packs, --synth-stop-after-items N, --fake-loaded JSON.
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/ (never printed, never written).
set -e
DRY=no; DECIDE=""; WSET=no; W=work/m5/b4; REPORT=runs/m5/B4_CHECK.md; RSET=no; USED=runs/m5/B4_USED; USET=no
ITEMS=eval/m4/b4/items.jsonl; RUBS=eval/m4/b4/rubrics.jsonl; CC=eval/m4/b4/CHUNK_CHECK.json; PREC=runs/m5/PROFILE_R4.json; INSET=no
FLAW=none; STOP=""; STOPITEMS=0; FAKE_LOADED='[]'
usage() { echo "usage: scripts/m5_b4_check.sh --decide strict|loose [--dry-run] [--work DIR] [--report FILE] [--used-marker FILE] [--items F --rubrics F --chunk-check F --profile-record F] (see the header)" >&2; exit 2; }
while [ $# -gt 0 ]; do case $1 in
  --decide) DECIDE=${2-}; shift;; --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --used-marker) USED=${2:?}; USET=yes; shift;;
  --items) ITEMS=${2:?}; INSET=yes; shift;; --rubrics) RUBS=${2:?}; INSET=yes; shift;; --chunk-check) CC=${2:?}; INSET=yes; shift;;
  --profile-record) PREC=${2:?}; INSET=yes; shift;;
  --synth-flaw) FLAW=${2:?}; shift;; --stop-after) STOP=${2:?}; shift;; --synth-stop-after-items) STOPITEMS=${2:?}; shift;; --fake-loaded) FAKE_LOADED=${2:?}; shift;;
  *) usage;; esac; shift; done
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
[ -n "$DECIDE" ] || refuse "--decide strict|loose is required and has no default (the Owner has not decided which count decides X1)"
case $DECIDE in strict|loose) ;; *) refuse "--decide must be strict or loose, got '$DECIDE'";; esac
if [ $DRY = no ]; then
  { [ "$FLAW" = none ] && [ -z "$STOP" ] && [ "$STOPITEMS" = 0 ] && [ "$FAKE_LOADED" = '[]' ]; } || refuse "--synth-flaw, --stop-after, --synth-stop-after-items and --fake-loaded are for dry runs only"
fi
case $FLAW in none|rubrics-hash|placeholder) ;; *) usage;; esac
case "$STOP" in ""|expand|packs) ;; *) usage;; esac

PYS=".venv/bin/python scripts/m5_b4_served.py"
L=runs/m5/logs; N=m5_b4_check; HB=runs/v3/HEARTBEAT.md; SLEEP=30; WAITN=120
PHASE="M5 R4: one-time check of r4 on B4 (rule X1)"
CHUNKS=work/v3/chunks_s1.jsonl; Q4=""
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EXP_ARGS=""; PACK_ARGS=""; CAND_ARGS=""
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m5_b4_dry.XXXXXX")
  mkdir -p $W
  L=$W/logs; HB=$W/HEARTBEAT.md; SLEEP=0; WAITN=1
  [ $RSET = yes ] || REPORT=$W/B4_CHECK.md
  [ $USET = yes ] || USED=$W/B4_USED
  case "$USED $REPORT" in *runs/m5/B4_USED*|*runs/m5/B4_CHECK.md*) refuse "a dry run never uses the real marker or the real report";; esac
  [ $INSET = no ] || case "$ITEMS $RUBS $CC $PREC" in *eval/m4/b4*|*runs/m5/PROFILE_R4.json*) refuse "a dry run never uses the real B4 files or the real profile record";; esac
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 0"
  EXP_ARGS="--stub"; PACK_ARGS="--stand-in"
  if [ $INSET = no ]; then   # the synthetic batch of the dry run (made once, in the work folder)
    IN=$W/in
    [ -s $IN/items.jsonl ] || $PYS synth --dir $IN --flaw $FLAW > /dev/null || refuse "the synthetic batch could not be made"
    ITEMS=$IN/items.jsonl; RUBS=$IN/rubrics.jsonl; CC=$IN/CHUNK_CHECK.json; PREC=$IN/PROFILE_R4.json
  fi
  [ -s $W/in/candidate.json ] && CAND_ARGS="--candidate $W/in/candidate.json"   # the synthetic batch carries its own chosen-r4 candidate file (the real one is still a placeholder)
else
  G="scripts/run_guarded.py --heartbeat $HB --other-retries 2"
fi

# ---------------------------------------------------------------- 1. the gate (CPU; refusal = exit 2)
for f in $ITEMS $RUBS $CC $PREC kafka_sre_rag/candidate.json kafka_sre_rag/frozen.json runs/v3/INDEX_FROZEN.json $CHUNKS \
         scripts/run_guarded.py scripts/m5_b4_served.py scripts/eval2_compare.py; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
[ ! -e $USED ] || refuse "$USED exists: B4 was used already (it is used once); a second run is refused"
[ ! -e $REPORT ] || [ -e $W/B4_MANIFEST.json ] || refuse "$REPORT exists but $W/B4_MANIFEST.json does not; the report is not overwritten"
$PYS gate-files --items $ITEMS --rubrics $RUBS --chunk-check $CC || refuse "the batch files differ from the hashes in the chunk check (the batch changed after its check)"
$PYS gate-profile --record $PREC $CAND_ARGS || refuse "the record of the chosen r4 setting is not usable (still a placeholder or not chosen, or not equal to candidate.json)"
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
# the start check of a stored manifest and the stage markers (the model is compared again below, once the server has named it)
SC_MODEL=$([ -s $W/B4_MANIFEST.json ] && .venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["options"]["model"])' $W/B4_MANIFEST.json || echo "Qwen3.8-27B-4bit")
SC_DRY=""; [ $DRY = no ] || SC_DRY="--dry-run"
start_check() { # MODEL
  $PYS start-check --items $ITEMS --rubrics $RUBS --chunk-check $CC --profile-record $PREC --work $W --decide $DECIDE --model "$1" $SC_DRY; }
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
echo "planned: $NEED expansions on the server (4-bit model), then $NEED frozen and r4 packs (reranker, CPU); about 0.2 to 0.5 h (inferred from B2's stored seconds; the packs' time is unmeasured on B4); --decide $DECIDE"
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
others() { pgrep -fl 'scripts/(m4|m5|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }

if ! stage_done expand; then
  if [ $DRY = no ]; then
    export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
    export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
    [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
    BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
    hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, s1_*.sh) and no guarded job runs, then the server check"
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
  Q4=$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["options"]["model"])' $W/B4_MANIFEST.json)
fi
sc=0; start_check "$Q4" || sc=$?
[ $sc = 0 ] || refuse "the start check failed for the model $Q4 (exit code $sc, see above)"

# ---------------------------------------------------------------- the manifest (written once) and the report header (before the expansions)
NEWMAN=no; [ -s $W/B4_MANIFEST.json ] || NEWMAN=yes
$PYS manifest --items $ITEMS --rubrics $RUBS --chunk-check $CC --profile-record $PREC --work $W --decide $DECIDE --model "$Q4" $SC_DRY --models-json $W/models.json \
  || fail "the manifest could not be written"
if [ $NEWMAN = yes ]; then
  mkdir -p "$(dirname $REPORT)"
  $PYS report --phase header --items $ITEMS --rubrics $RUBS --decide $DECIDE --model "$Q4" $SC_DRY --out $REPORT || fail "the report header could not be written"
fi

# ---------------------------------------------------------------- 3. expansions
EXPS=$W/expansions.jsonl
if stage_done expand; then
  echo "skip expansions: passed earlier ($(rows $EXPS) rows)"
else
  hb "$N: expansions started ($NEED items, $Q4, --decide $DECIDE)"
  status RUNNING no "$N running: expansions; markers $L/$N.done / .failed"
  [ $DRY = no ] || fake_edit load "$Q4"
  $G --stall-min 40 --log $L/$N.expand.log -- .venv/bin/python scripts/m5_b4_served.py expand --items $ITEMS --out $EXPS --model "$Q4" $EXP_ARGS \
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

# ---------------------------------------------------------------- 4. packs (reranker under the guard)
PACKS=$W/packs.jsonl
if stage_done packs; then
  echo "skip packs: passed earlier ($(rows $PACKS) rows)"
else
  hb "$N: packs started (frozen and r4, reranker on the CPU)"
  status RUNNING no "$N running: packs; markers $L/$N.done / .failed"
  $G --stall-min 15 --log $L/$N.packs.log -- .venv/bin/python scripts/m5_b4_served.py packs --items $ITEMS --expansions $EXPS --out $PACKS $PACK_ARGS \
      || fail "packs failed (see $L/$N.packs.log; an r4 pack that does not start with the frozen pack stops the chain)"
  [ "$(rows $PACKS)" = "$NEED" ] || fail "$PACKS has $(rows $PACKS) rows, not $NEED"
  $PYS mark --work $W --stage packs packs.jsonl
  hb "$N: packs done ($NEED rows)"
fi
[ "$STOP" != packs ] || { echo "dry run stopped after the packs (--stop-after packs)"; exit 0; }

# ---------------------------------------------------------------- 5. the counts and the verdict
if stage_done served; then
  echo "skip counts: passed earlier"
else
  $PYS served --rubrics $RUBS --packs $PACKS --decide $DECIDE --json-out $W/served.json --text-out $W/served.txt > $W/served.out || { cat $W/served.out; fail "the counts failed (see above)"; }
  $PYS mark --work $W --stage served served.json served.txt
fi
cat $W/served.txt

# ---------------------------------------------------------------- 6. the report and the marker B4_USED
$PYS report --phase final --items $ITEMS --rubrics $RUBS --decide $DECIDE --model "$Q4" $SC_DRY --out $REPORT --expansions $EXPS --packs $PACKS \
  --served-json $W/served.json --served-text $W/served.txt || fail "the report could not be written"
VERDICT=$(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print("PASS" if d["verdict"][d["decide"]]["pass"] else "FAIL")' $W/served.json)
mkdir -p "$(dirname $USED)"
( set -C; printf 'used %s\ndecide %s\nverdict %s\nreport %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$DECIDE" "$VERDICT" "$REPORT" > $USED ) \
  || fail "$USED could not be written (it exists already?)"
hb "$N: done; X1 on the $DECIDE count: $VERDICT; report $REPORT; B4 is used (marker $USED)" yes
status DONE yes "$N done: X1 $VERDICT on the $DECIDE count; report $REPORT; B4 used"
if [ $DRY = yes ]; then echo M5_B4_CHECK_DRY_RUN_DONE >> $L/$N.done; echo "dry run done: X1 $VERDICT (stub data, not a measurement); report $REPORT; marker $USED"
else echo M5_B4_CHECK_DONE >> $L/$N.done; echo "done: X1 $VERDICT on the $DECIDE count; report $REPORT; marker $USED"; fi
