#!/bin/sh
# M4 S6: noise (the reference configuration twice) and the cost of 4-bit against full precision, on the same server path.
# Open mechanical tier only (eval/m4/open/tier_a.jsonl, 251 items), through scripts/eval2.py with the server backend, one request at
# a time, thinking off, frozen index, catalogues hashed into every manifest. Unattended; it takes the model server, no local GPU.
#   waits until no m4_s5a_judge_settings.sh and no m4_s5_judge_pilot.sh process exists, then
#   0. server: idle, our judge models (GLM builds, DeepSeek) unloaded; a model that is not ours or another client -> wait up to
#      60 min, then fail (speed figures need an otherwise idle server); the server's loaded models and version are recorded
#      before every run (work/m4/s6/server_<run>.json)
#   1. gate: the first 5 items of the smoke subset (eval/m4/smoke_ids.json) on the 4-bit model, default mode: five non-empty answers,
#      no error, manifest written, the scorer runs. A failed gate stops everything.
#   2. A1: 4-bit, default, all open items      3. A2: the same again (noise)      4. S: 4-bit, strict
#   5. unload the 4-bit model; F: full precision (bf16 build), default, end to end
#   6. FR: full precision, default, pack replay from A1 (identical excerpts)         7. unload the model
#   8. score all five (scripts/eval2_score.py), compare and write runs/m4/S6_NOISE_4BIT.md (scripts/eval2_compare.py s6)
# The first look on the open half only: the decision on the cost of 4-bit is taken on the hidden half (a hidden run, ledger first).
# Model names are taken from GET /v1/models (4-bit: Qwen3.8-27B-4bit; full precision: the Qwen3.8-27B bf16 build); the chain fails
# if one is absent or ambiguous. Override with M4_Q4_MODEL / M4_BF16_MODEL.
# Every answering step resumes from its answers file and runs under scripts/run_guarded.py with --stall-min 40: the harness prints a
# line per item, the client times out after 600 s per request and an item makes two requests (search terms, answer), so a healthy
# run is never silent for more than about 20 min; a single full-precision answer takes up to about 3.5 min (v3 path check: 134 to
# 212 s), and the first request of a build also loads it (a few minutes). 40 min is above all of these and still catches a hang.
# Running time (v3 figures: 4-bit default 47 to 71 s per answer, strict 37 to 65 s; full precision 77 to 212 s; 251 items; the
# mechanical items may be shorter, answers are mostly one fact, a command or a manifest; replay skips the search-terms call):
#   gate 0.1 h; A1 3.3 to 5.0 h; A2 3.3 to 5.0 h; S 2.6 to 4.5 h; F 5.4 to 14.8 h; FR 5 to 14 h; loads and checks 0.5 h
#   total about 20 to 44 h with the v3 figures; with shorter mechanical answers more likely 15 to 30 h. Corrected when the first
#   run's rate is known (progress lines in runs/m4/logs/m4_s6_noise_4bit.<run>.log).
# Outputs are new files under work/m4/s6/ (answers, scores, summaries, server state), the report runs/m4/S6_NOISE_4BIT.md.
# Markers: runs/m4/logs/m4_s6_noise_4bit.done or .failed.
#   scripts/m4_s6_noise_4bit.sh > runs/m4/logs/m4_s6_noise_4bit.out 2>&1 &
#   scripts/m4_s6_noise_4bit.sh --dry-run     # stub generator, 8 items, no server, no markers, no heartbeat; prints the report
set -e
DRY=no; [ "${1:-}" = "--dry-run" ] && DRY=yes
OPEN=eval/m4/open/tier_a.jsonl; SPLIT=eval/m4/SPLIT.json; CATS=work/m4/catalogues
W=work/m4/s6; L=runs/m4/logs; N=m4_s6_noise_4bit; REPORT=runs/m4/S6_NOISE_4BIT.md
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
STALL=40
JUDGE_RE='^(/bin/)?(ba)?sh +(\./)?scripts/m4_s5(a_judge_settings|_judge_pilot)\.sh'
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
else
  W=$(mktemp -d "${TMPDIR:-/tmp}/m4_s6_dry.XXXXXX"); L=$W/logs; mkdir -p $L; REPORT=$W/report_from_stub_data.md
  G="scripts/run_guarded.py --other-retries 0"
fi
ITEMS=$OPEN

hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M4 S6: noise and 4-bit against full precision, open mechanical tier",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }
api() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
foreign() { # loaded models that are not ours (the models this chain uses or unloads)
  api /api/status | python3 -c 'import sys,json; ours=set(sys.argv[1:]); print(" ".join(m for m in (json.load(sys.stdin).get("loaded_models") or []) if m not in ours))' $DS $GLM8 $GLM4 $Q4 $BF16 2>/dev/null || echo "status-unreadable"; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" >/dev/null; hb "$N: unloaded $1 (no active or waiting requests)"; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }
server_state() { # RUN: loaded models and version fields of the server before the run (no address, no key)
  [ $DRY = yes ] && return 0; [ -f $W/server_$1.json ] && return 0
  api /api/status | python3 -c '
import sys, json
d = json.load(sys.stdin)
out = {"loaded_models": d.get("loaded_models"), "active_requests": d.get("active_requests"), "waiting_requests": d.get("waiting_requests")}
out.update({k: v for k, v in d.items() if "version" in k.lower() and isinstance(v, (str, int, float))})
print(json.dumps(out))' > $W/server_$1.json.tmp 2>/dev/null && mv $W/server_$1.json.tmp $W/server_$1.json || echo '{"unreadable": true}' > $W/server_$1.json; }
check_answers() { # FILE EXPECTED_ROWS: complete, no error, no empty text
  .venv/bin/python - "$1" "$2" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
n = int(sys.argv[2])
err = [r["id"] for r in rows if r.get("error")]
empty = [r["id"] for r in rows if not r.get("error") and not (r.get("text") or "").strip()]
print(f"check {sys.argv[1]}: {len(rows)} of {n} rows, {len(err)} errors, {len(empty)} empty")
sys.exit(0 if len(rows) == n and not err and not empty else 1)
PY
}
runline() { # NAME FILE: one heartbeat line with the run's totals
  .venv/bin/python - "$1" "$2" <<'PY'
import json, statistics, sys
rows = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
t = [r["seconds"]["total"] for r in rows if not r.get("error")]
tk = [r["tokens"] for r in rows if r.get("tokens") is not None]
print(f"run {sys.argv[1]}: {len(rows)} answers, {sum(1 for r in rows if r.get('error'))} errors, median {statistics.median(t):.1f} s per answer, "
      f"max {max(t):.1f} s, median {statistics.median(tk) if tk else 0:.0f} tokens")
PY
}
answer_run() { # NAME MODEL MODE OUT EXPECTED [more harness arguments]
  name=$1; model=$2; mode=$3; out=$4; want=$5; shift 5
  if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $model"; fi
  tries=0
  while :; do
    [ $DRY = yes ] || { idle; server_state $name; }
    $G --stall-min $STALL --log $L/$N.$name.log -- .venv/bin/python scripts/eval2.py answer --items $ITEMS --out $out \
      --config s6-$name $GA --extra-body "$EB" --mode $mode --catalogue $CATS --host-class "Mac Studio" "$@" \
      || fail "$name: answering failed (see $L/$N.$name.log)"
    check_answers $out $want && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "$name: answers incomplete, with errors or empty after $tries passes ($out)"
  done
  hb "$N: $(runline $name $out)"
  status RUNNING no "$N running: run $name finished; markers $L/$N.done / .failed"
}
score_run() { # NAME ANSWERS ITEMS: scores next to the answers file (the harness report finds them there)
  s=${2%.jsonl}
  .venv/bin/python scripts/eval2_score.py --items $3 --answers $2 --scores $s.scores.jsonl --summary $s.summary.json \
    --catalogues $CATS > /dev/null || fail "$1: scorer failed"
  echo "scored $1: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["items_scored"], "items,", d["all_pass"], "all-pass,", d["missing_answers"], "missing")' $s.summary.json)"
}

# ---- inputs (before any waiting)
for f in $OPEN $SPLIT eval/m4/smoke_ids.json $CATS/CATALOGUES.json scripts/eval2.py scripts/eval2_score.py scripts/eval2_compare.py scripts/run_guarded.py; do
  [ -s $f ] || { [ $DRY = yes ] && { echo "input $f is missing or empty"; exit 1; }; fail "input $f is missing or empty"; }
done
NOPEN=$(rows $OPEN)
HIDDEN_N=$(.venv/bin/python -c 'import json; print(json.load(open("eval/m4/SPLIT.json"))["counts"]["hidden"])')
.venv/bin/python -m kafka_sre_rag verify > /dev/null || fail "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"
mkdir -p $W $L runs/m4/reports

if [ $DRY = yes ]; then
  # ---- dry run: stub generator, 8 items, no server; every step of the chain except the server calls
  head -n 8 $OPEN > $W/items.jsonl; ITEMS=$W/items.jsonl; NOPEN=8
  echo "dry run in $W (stub generator, BM25 order, 8 items)"
  Q4=Qwen3.8-27B-4bit; BF16=mlx-community--Qwen3.8-27B-bf16
  # model picking from a stub listing
  echo "{\"data\": [{\"id\": \"$Q4\"}, {\"id\": \"$BF16\"}, {\"id\": \"$GLM4\"}]}" > $W/models.json
  [ "$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)" = "$Q4" ] || fail "pick-model q4"
  [ "$(.venv/bin/python scripts/eval2_compare.py pick-model --kind bf16 < $W/models.json)" = "$BF16" ] || fail "pick-model bf16"
  GATE_ARGS="--limit 5"
else
  rm -f $L/$N.failed $L/$N.done
  hb "$N: waiting until no m4_s5a_judge_settings.sh and no m4_s5_judge_pilot.sh process exists, then the server check"
  until ! pgrep -f "$JUDGE_RE" >/dev/null; do sleep 60; done
  # ---- 0. the server: model names, idle, our judge models unloaded, nothing foreign
  api /v1/models > $W/models.json 2>/dev/null || true
  Q4=${M4_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || fail "4-bit model not listed by the server (or ambiguous)"
  BF16=${M4_BF16_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind bf16 < $W/models.json)} || fail "full-precision (bf16) model not listed by the server (or ambiguous)"
  hb "$N: started; 4-bit model $Q4, full-precision model $BF16; $NOPEN open items; gate, then A1, A2, S, F, FR; report $REPORT"
  status RUNNING no "$N running: gate, then runs A1 A2 S F FR on $NOPEN open items; markers $L/$N.done / .failed"
  n=0
  until [ "$(busy)" = "0" ] && [ -z "$(foreign)" ]; do
    sleep 30; n=$((n+1))
    [ $n -lt 120 ] || fail "server not free after 60 min (active or waiting requests, or a model of another client loaded: $(foreign)); speed figures need an otherwise idle server"
  done
  unload $DS; unload $GLM4; unload $GLM8
  hb "$N: DeepSeek and the GLM builds unloaded if they were loaded; the 4-bit model loads on the first request"
  GATE_ARGS="--subset smoke --limit 5"
fi

# ---- 1. gate: first 5 items of the smoke subset, 4-bit, default mode
answer_run gate $Q4 default $W/ans_gate.jsonl 5 $GATE_ARGS
[ -s $W/ans_gate.jsonl.manifest.json ] || fail "gate: no manifest written"
.venv/bin/python - $ITEMS $W/ans_gate.jsonl $W/gate_items.jsonl <<'PY' || fail "gate: could not build the gate items file"
import json, sys
ids = {json.loads(l)["id"] for l in open(sys.argv[2]) if l.strip()}
with open(sys.argv[3], "w") as f:
    f.writelines(l for l in open(sys.argv[1]) if l.strip() and json.loads(l)["id"] in ids)
PY
score_run gate $W/ans_gate.jsonl $W/gate_items.jsonl
[ "$(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["items_scored"], d["missing_answers"])' $W/ans_gate.summary.json)" = "5 0" ] || fail "gate: the scorer did not score five answers"
hb "$N: gate passed (5 non-empty answers, no error, manifest written, scorer ran)"

# ---- 2 to 4. 4-bit: A1, A2 (the same again), S (strict)
answer_run A1 $Q4 default $W/ans_A1.jsonl $NOPEN
answer_run A2 $Q4 default $W/ans_A2.jsonl $NOPEN
answer_run S  $Q4 strict  $W/ans_S.jsonl  $NOPEN

# ---- 5, 6. full precision: end to end (F), then on the excerpts of A1 (FR)
[ $DRY = yes ] || { unload $Q4; hb "$N: 4-bit model unloaded; the full-precision model loads on the first request"; }
answer_run F  $BF16 default $W/ans_F.jsonl  $NOPEN
answer_run FR $BF16 default $W/ans_FR.jsonl $NOPEN --replay-packs $W/ans_A1.jsonl
# the replay must have used the stored packs of A1
.venv/bin/python - $W/ans_A1.jsonl $W/ans_FR.jsonl <<'PY' || fail "FR: packs differ from A1 (replay did not hold)"
import json, sys
rd = lambda p: {r["id"]: r.get("pack") for r in map(json.loads, filter(str.strip, open(p)))}
a, b = rd(sys.argv[1]), rd(sys.argv[2])
bad = [i for i in b if a.get(i) != b[i]]
print(f"replay check: {len(b) - len(bad)} of {len(b)} packs identical to A1")
sys.exit(1 if bad else 0)
PY

# ---- 7. unload
[ $DRY = yes ] || { unload $BF16; hb "$N: answering finished, $BF16 unloaded"; }

# ---- 8. score, compare, report (CPU)
for r in A1 A2 S F FR; do score_run $r $W/ans_$r.jsonl $ITEMS; done
SRV=""; for r in A1 A2 S F FR; do [ -f $W/server_$r.json ] && SRV="$SRV --server $r=$W/server_$r.json"; done
NOTE="Model names as the server listed them: 4-bit $Q4, full precision $BF16."
[ $DRY = yes ] && NOTE="Dry run: stub generator on 8 items, BM25 order, no server; every number here is stub data."
.venv/bin/python scripts/eval2_compare.py s6 --run A1=$W/ans_A1.jsonl --run A2=$W/ans_A2.jsonl --run S=$W/ans_S.jsonl \
  --run F=$W/ans_F.jsonl --run FR=$W/ans_FR.jsonl --items $ITEMS --out $REPORT --margin 5 --hidden-n $HIDDEN_N \
  --resamples 10000 --seed 1 $SRV --note "$NOTE" $( [ $DRY = yes ] && echo --dry ) || fail "report failed"
if [ $DRY = yes ]; then
  cat $REPORT
  echo "dry run done: report from stub data in $REPORT"
  exit 0
fi
RES=$(sed -n '3p' $REPORT | cut -c1-400)
hb "$N: done; $RES (runs/m4/S6_NOISE_4BIT.md; first look on the open half, not the decision)" yes
status DONE yes "$N done: runs/m4/S6_NOISE_4BIT.md; the decision on 4-bit waits for a hidden run"
echo M4_S6_NOISE_4BIT_DONE >> $L/$N.done
