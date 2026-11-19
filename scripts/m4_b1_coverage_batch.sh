#!/bin/sh
# M4 B1: the first fresh batch of the judged tier (agents/m4-evaluation.md, "First fresh batch (B1): confirms the coverage role").
# About 30 fresh open scenarios, each answered on the reference configuration (the 4-bit base model on the server, thinking off, frozen
# index, catalogues given, one request at a time) in three ways with the SAME excerpts:
#   default      the frozen settings (answer limit 2,048 tokens); the retrieval of this run is the pack of the other two;
#   strict       strict mode, packs replayed from default (scripts/eval2.py answer --replay-packs);
#   default_4k   default mode, packs replayed from default, answer limit raised to 4,096 tokens (--max-tokens 4096; the frozen limit
#                stays 2,048, this set is descriptive only, decision J4).
# Judged by DeepSeek without thinking (settings name deepseek-nothink-12k), instruction v2, in two passes, in the coverage role
# (eval2_judge.py run --coverage); identifier check (CPU) on the three answer sets; report runs/m4/B1_COVERAGE_BATCH.md.
# Unattended; it takes the model server, no local GPU.
# Inputs (written by someone else; the chain refuses to start, exit 2, without them or with a problem in them):
#   eval/m4/b1/items.jsonl     rows {"id", "tier", "kind", "family", "domain", "question", "evidence" (string or null), ...}
#   eval/m4/b1/rubrics.jsonl   rows (id = the item's id, same order): must (every point with "chunks" or "not_indexed"), must_not, any_of, ...
#   Further refusals: a recorded chunk id that is not in the frozen index, a weight-2 point without "chunks" or not_indexed true, item and
#   rubric ids that differ, weights_status that says "proposed" on any rubric (--proposed-weights lets only a dry run go on). A mismatch
#   between the recorded chunk ids and what eval2_judge.py fill-chunks --check places from the cited lines is a warning with a count.
# Steps:
#   0. CPU: inputs checked, plan printed (answers, judge calls, time estimate); then waits until no other chain (scripts/m4_*.sh,
#      scripts/s1_*.sh) and no guarded job runs;
#   1. server: no active or waiting request and no model of another client loaded (waits, up to 60 min; never unloads another client's
#      model); our judge models (DeepSeek, GLM builds) unloaded; the 4-bit model's name from GET /v1/models (override: M4_Q4_MODEL);
#   2. ALL answering first, assistant model only: default, strict, default_4k (resumable, guarded, stall limit 40 min). Per answer the
#      harness records completion tokens, whether the limit was hit ("truncated") and the seconds of retrieval and of the answer;
#   3. unloads the 4-bit model; then ALL judging with the judge only (guarded, stall limit 60 min): 3 sets x 2 passes; one more round
#      for replies that were not valid JSON; unloads the judge;
#   4. CPU: identifier check on the three sets; the report (coverage report with the reader criteria WAITING, identifier totals, the
#      default_4k comparison, the commands to finish it).
# Before every unload the server must have no active or waiting request. Resumable: finished answer files and finished judge results are
# skipped, a started answer file continues, the judge continues from its cache. Outputs are new files under work/m4/b1/.
# Markers: runs/m4/logs/m4_b1_coverage_batch.done or .failed.
#   scripts/m4_b1_coverage_batch.sh > runs/m4/logs/m4_b1_coverage_batch.out 2>&1 &
#   (the chain itself runs no model; every answer step and the judge step runs under scripts/run_guarded.py inside it)
#   scripts/m4_b1_coverage_batch.sh --dry-run [--work DIR]    # stub generator and stub judge, no server, no markers, no heartbeat, nothing
#                                                              # written outside DIR (default: a temporary directory); run it again with the
#                                                              # same --work to see the resume
# Options: --items FILE --rubrics FILE (defaults above), --proposed-weights, --work DIR, --report FILE.
# Running time (inferred from the pilot's rates, not measured on B1): see the line "estimate" the chain prints.
set -e
DRY=no; PROPOSED=no
ITEMS_IN=${B1_ITEMS:-eval/m4/b1/items.jsonl}; RUBS_IN=${B1_RUBRICS:-eval/m4/b1/rubrics.jsonl}
W=work/m4/b1; REPORT=runs/m4/B1_COVERAGE_BATCH.md; WSET=no; RSET=no
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;; --proposed-weights) PROPOSED=yes;;
  --items) ITEMS_IN=${2:?}; shift;; --rubrics) RUBS_IN=${2:?}; shift;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  *) echo "usage: scripts/m4_b1_coverage_batch.sh [--dry-run] [--items FILE] [--rubrics FILE] [--proposed-weights] [--work DIR] [--report FILE]" >&2; exit 2;; esac
  shift; done
L=runs/m4/logs; N=m4_b1_coverage_batch
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
SETTING=deepseek-nothink-12k; SRC=12000
SETS_ORDER="default strict default_4k"
# the pilot's rates (82 s default, 67 s strict per answer, 22 s per judge call); the raised limit is guessed at 1.0 to 1.5 x default
S_DEFAULT=82; S_STRICT=67; S_4K_LO=82; S_4K_HI=123; S_JUDGE=22; JPAR=3

if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m4_b1_dry.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/report_from_stub_data.md
  G="scripts/run_guarded.py --other-retries 0"
fi
mkdir -p $W $L
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M4 B1: first fresh batch of the judged tier (coverage role)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- 0. inputs (CPU; before anything is written outside $W)
for f in $ITEMS_IN $RUBS_IN $CHUNKS scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_pilot_report.py scripts/run_guarded.py; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
[ -s $CATS/CATALOGUES.json ] || refuse "input $CATS/CATALOGUES.json is missing"
NEED=$(grep -c . $ITEMS_IN | tr -d ' ')
[ "$(grep -c . $RUBS_IN | tr -d ' ')" = "$NEED" ] && [ "$NEED" -gt 0 ] || refuse "$ITEMS_IN and $RUBS_IN must have the same number of rows (at least one)"
.venv/bin/python - $ITEMS_IN $RUBS_IN <<'PY' || refuse "items and rubrics do not match (same ids in the same order)"
import json, sys
it = [json.loads(l)["id"] for l in open(sys.argv[1]) if l.strip()]
rb = [json.loads(l)["id"] for l in open(sys.argv[2]) if l.strip()]
if it != rb or len(set(it)) != len(it):
    sys.exit("item ids and rubric ids differ, or an id is repeated")
PY
# Gate on the rubrics (the chunk ids are recorded by hand by the author, checked by a second reviewer; fill-chunks only places the cited
# lines, so its mismatches are a warning, not a failure): every recorded chunk id exists in the frozen index; every weight-2 must point has
# non-empty "chunks" or not_indexed true. Done here, not by fill-chunks --check, which cannot tell "missing from the index" from "placed elsewhere".
gate() { # RUBRICS_FILE -> 0 or 1 (reasons on stderr)
  .venv/bin/python - "$1" $CHUNKS <<'PY'
import json, sys
ids = {json.loads(l)["id"] for l in open(sys.argv[2]) if l.strip()}
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
bad = []
for r in rows:
    for p in r["must"]:
        for c in p.get("chunks") or []:
            if c not in ids:
                bad.append("%s %s: chunk id %s is not in the frozen index" % (r["id"], p["id"], c))
        if p.get("weight") == 2 and not (p.get("chunks") or p.get("not_indexed") is True):
            bad.append("%s %s: weight-2 point with neither 'chunks' nor not_indexed true" % (r["id"], p["id"]))
for b in bad[:15]:
    print(b, file=sys.stderr)
if len(bad) > 15:
    print("... and %d more" % (len(bad) - 15), file=sys.stderr)
sys.exit(1 if bad else 0)
PY
}
RUBS=$RUBS_IN
if [ $DRY = yes ] && ! gate $RUBS_IN 2> $W/gate.out; then
  # a dry run on rubrics that fail the gate (the pilot's have no chunk ids) fills them into a copy; a real start refuses them
  RUBS=$W/rubrics_filled.jsonl
  .venv/bin/python scripts/eval2_judge.py fill-chunks --rubrics $RUBS_IN --chunks $CHUNKS --out $RUBS > $W/fill.out 2>&1 || { cat $W/fill.out; echo "DRY RUN FAILED: fill-chunks"; exit 1; }
  echo "dry run: the rubrics fail the gate (a real start would refuse them: $(head -1 $W/gate.out)); chunk ids filled into $RUBS"
fi
gate $RUBS 2> $W/gate.out || { head -15 $W/gate.out >&2; refuse "the rubrics fail the gate: a weight-2 must point has neither 'chunks' nor not_indexed, or a recorded chunk id is not in the frozen index"; }
echo "gate: every recorded chunk id is in the frozen index, every weight-2 point has chunks or not_indexed"
# the helper's line-level placement against the recorded ids: a warning only
.venv/bin/python scripts/eval2_judge.py fill-chunks --rubrics $RUBS --chunks $CHUNKS --check > $W/chunks_check.out 2>&1 || true
echo "warning: fill-chunks --check mismatches (recorded chunk differs from the one placed from the cited lines; the second reviewer decides): $(grep -c '^problem:' $W/chunks_check.out || true)"
bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and "propos" in str(json.loads(l).get("weights_status","")).lower()))' $RUBS)
if [ "$bad" != 0 ]; then
  if [ $DRY = yes ] && [ $PROPOSED = yes ]; then echo "dry run: weights_status says proposed in $bad rubrics (allowed by --proposed-weights in a dry run only)"
  else refuse "weights_status says proposed in $bad rubrics$([ $PROPOSED = yes ] && echo '; --proposed-weights is for dry runs only')"; fi
fi
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
  rm -f $L/$N.failed $L/$N.done
fi
mkdir -p runs/m4/reports; mkdir -p "$(dirname $REPORT)"

api() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
foreign() { # loaded models that are not ours
  api /api/status | python3 -c 'import sys,json; ours=set(sys.argv[1:]); print(" ".join(m for m in (json.load(sys.stdin).get("loaded_models") or []) if m not in ours))' $DS $GLM8 $GLM4 $Q4 2>/dev/null || echo "status-unreadable"; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0   # only models this chain knows as its own (never another client's)
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" >/dev/null; hb "$N: unloaded $1 (no active or waiting requests)"; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }
others() { pgrep -fl 'scripts/(m4|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }

# harness items: the harness wants strings for tier, kind, family and domain
HITEMS=$W/items_harness.jsonl
.venv/bin/python - $ITEMS_IN $HITEMS <<'PY' || fail "could not build the harness items file"
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
with open(sys.argv[2], "w") as f:
    for r in rows:
        if not r.get("id") or not r.get("question"):
            sys.exit("item without id or question")
        row = {"tier": "B", "kind": "open", "family": r["id"], "domain": "b1", **r}
        if isinstance(row["domain"], list):   # the harness wants a string: joined, the list kept as "domains"
            row["domains"], row["domain"] = row["domain"], ",".join(map(str, row["domain"])) or "b1"
        for k in ("tier", "kind", "family", "domain"):
            if not isinstance(row[k], str) or not row[k]:
                sys.exit(f"item {r['id']}: {k} is not a non-empty string")
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
PY
.venv/bin/python -m kafka_sre_rag verify > /dev/null || fail "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"

# the plan and the estimate
NSETS=3; NANS=$((NEED * NSETS)); CALLS=$((NANS * 2))
est() { # items -> answers, judge calls and the time
  n=$1; a_lo=$((n * (S_DEFAULT + S_STRICT + S_4K_LO))); a_hi=$((n * (S_DEFAULT + S_STRICT + S_4K_HI))); j=$((n * NSETS * 2 * S_JUDGE / JPAR))
  load=1080   # loads, checks and unloads: 18 min
  echo "$((n * NSETS)) answers ($n items x default, strict, default_4k), $((n * NSETS * 2)) judge calls: answering $((a_lo / 60)) to $((a_hi / 60)) min, judging $((j / 60)) min, loads and checks 18 min; total $(awk -v lo=$((a_lo + j + load)) -v hi=$((a_hi + j + load)) 'BEGIN{printf "%.1f to %.1f", lo/3600, hi/3600}') h"; }
echo "planned: $(est $NEED)"
[ "$NEED" = 30 ] || echo "planned for 30 items: $(est 30)"
echo "estimate (inferred from the pilot's rates: ${S_DEFAULT} s default and ${S_STRICT} s strict per answer, ${S_JUDGE} s per judge call, $JPAR in parallel; the raised limit guessed at ${S_4K_LO} to ${S_4K_HI} s; not measured on B1)"

# ---------------------------------------------------------------- state: what is finished already
check_answers() { # FILE EXPECTED_ROWS: complete, no error, no empty text, every row with a pack, manifest ended
  .venv/bin/python - "$1" "$2" <<'PY'
import json, os, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
n = int(sys.argv[2])
err = [r["id"] for r in rows if r.get("error")]
empty = [r["id"] for r in rows if not r.get("error") and not (r.get("text") or "").strip()]
nopack = [r["id"] for r in rows if not r.get("error") and not r.get("pack")]
mp = sys.argv[1] + ".manifest.json"
ended = os.path.exists(mp) and bool(json.load(open(mp)).get("ended"))
print(f"check {sys.argv[1]}: {len(rows)} of {n} rows, {len(err)} errors, {len(empty)} empty, {len(nopack)} without a pack, manifest ended: {'yes' if ended else 'no'}")
sys.exit(0 if len(rows) == n and not err and not empty and not nopack and ended else 1)
PY
}
all_answers_done=yes
for s in $SETS_ORDER; do check_answers $W/ans_$s.jsonl $NEED >/dev/null 2>&1 || all_answers_done=no; done
judge_done() { # FILE EXPECTED_ROWS
  [ -f "$1" ] && .venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; sys.exit(0 if len(r)==int(sys.argv[2]) and all(x["valid_reply"] for x in r) else 1)' "$1" "$2" 2>/dev/null; }
all_done=no; { [ $all_answers_done = yes ] && judge_done $W/judge_results.jsonl $CALLS; } && all_done=yes

# ---------------------------------------------------------------- 1. waiting, server (nothing to wait for when all is finished)
if [ $DRY = yes ]; then
  echo "dry run in $W (stub generator, stub judge, no server)"
  Q4=Qwen3.8-27B-4bit
elif [ $all_done = yes ]; then
  echo "all answers and all judge results are finished: no server needed"
  Q4=none
else
  hb "$N: waiting until no other chain (scripts/m4_*.sh, scripts/s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done
  api /v1/models > $W/models.json 2>/dev/null || true
  Q4=${M4_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || fail "4-bit model not listed by the server (or ambiguous)"
  hb "$N: started; 4-bit model $Q4; $NEED items in default, strict and default_4k, then judge $DS (coverage role, instruction v2, settings $SETTING; $CALLS judge calls planned)$([ $PROPOSED = yes ] && echo '; WEIGHTS ONLY PROPOSED (--proposed-weights)')"
  status RUNNING no "$N running: answers ($NANS), then judging ($CALLS calls); markers $L/$N.done / .failed"
  n=0
  until [ "$(busy)" = "0" ] && [ -z "$(foreign)" ]; do
    sleep 30; n=$((n+1))
    [ $n -lt 120 ] || fail "server not free after 60 min (active or waiting requests, or a model of another client loaded: $(foreign))"
  done
  unload $DS; unload $GLM4; unload $GLM8
  hb "$N: DeepSeek and the GLM builds unloaded if they were loaded; the 4-bit model loads on the first request"
fi

# ---------------------------------------------------------------- 2. answers: everything the assistant model has to do comes first
answer_run() { # NAME MODE [extra answer arguments]: all items, one request at a time
  name=$1; mode=$2; shift 2; out=$W/ans_$name.jsonl
  if check_answers $out $NEED > $W/check_$name.out 2>&1; then
    echo "skip answers $name: $(cat $W/check_$name.out)"; return 0; fi
  if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $Q4"; fi
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    $G --stall-min 40 --log $L/$N.answers_$name.log -- .venv/bin/python scripts/eval2.py answer --items $HITEMS --out $out \
      --config b1-$name $GA --extra-body "$EB" --mode $mode --catalogue $CATS --host-class "Mac Studio" "$@" \
      || fail "answers $name: failed (see $L/$N.answers_$name.log)"
    check_answers $out $NEED && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $name: incomplete, with errors, empty or without a pack after $tries passes ($out)"
  done
  hb "$N: answers $name done ($NEED rows)"
  status RUNNING no "$N running: answers $name done; markers $L/$N.done / .failed"
}
if [ $all_done = no ]; then
  answer_run default default
  answer_run strict strict --replay-packs $W/ans_default.jsonl
  answer_run default_4k default --replay-packs $W/ans_default.jsonl --max-tokens 4096
else
  echo "skip answers: all three sets are finished"
fi
.venv/bin/python - $W $NEED <<'PY' || fail "the three answer sets do not have identical packs (replay failed)"
import json, sys
w = sys.argv[1]
ps = {}
for s in ("default", "strict", "default_4k"):
    ps[s] = {}
    for line in open(f"{w}/ans_{s}.jsonl"):
        if line.strip():
            r = json.loads(line)
            ps[s][r["id"]] = r["pack"]
ok = ps["default"] == ps["strict"] == ps["default_4k"]
print(f"packs identical in default, strict and default_4k: {'yes' if ok else 'NO'} ({len(ps['default'])} items)")
sys.exit(0 if ok else 1)
PY
SETS="default=$W/ans_default.jsonl strict=$W/ans_strict.jsonl default_4k=$W/ans_default_4k.jsonl"
if [ $DRY = no ] && [ $all_done = no ]; then unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; fi

# ---------------------------------------------------------------- 3. judging: the judge only; all answers, two passes, coverage role
UNSCORED=0
jrun() { # extra arguments
  if [ $DRY = yes ]; then
    .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS_IN --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
      --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $SETTING --concurrency $JPAR --stub "$@"
  else
    $G --stall-min 60 --log $L/$N.judge.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS_IN \
      --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
      --settings-name $SETTING --concurrency $JPAR --judge-url "$OMLX_URL/v1" "$@"
  fi; }
out=$W/judge_results.jsonl
if judge_done $out $CALLS; then
  echo "skip judging: $out has $CALLS valid rows"
else
  for round in 1 2; do
    extra=""; [ $round = 2 ] && extra="--retry-invalid"
    if [ $DRY = yes ]; then
      jrun --passes 1 2 --cache $W/judge_cache.jsonl --out $out $extra > $W/judge.dry.out 2>&1 || { cat $W/judge.dry.out; fail "judge dry run failed"; }
      grep -E '^(matched|instruction|items)' $W/judge.dry.out
    else
      jrun --passes 1 2 --cache $W/judge_cache.jsonl --out $out $extra || fail "judging round $round failed (see $L/$N.judge.log)"
    fi
    bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $out)
    echo "judging round $round: $bad unscored"
    [ "$bad" = "0" ] && break
  done
  UNSCORED=$bad
  [ "$(rows $out)" = "$CALLS" ] || fail "$out has $(rows $out) rows, not $CALLS"
  if [ $DRY = no ]; then unload $DS; hb "$N: judging finished, $DS unloaded ($UNSCORED unscored judgments)"; fi
fi

# ---------------------------------------------------------------- 4. identifier check and the report (CPU)
for s in $SETS_ORDER; do
  .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_$s.jsonl --catalogues $CATS --totals-only > $W/idcheck_$s.txt || fail "identifier check on $s failed"
  echo "idcheck $s: $(sed -n 4p $W/idcheck_$s.txt)"
done
: > $W/findings_none.jsonl
.venv/bin/python scripts/eval2_pilot_report.py --coverage --results $out --rubrics $RUBS --findings $W/findings_none.jsonl \
  --title "B1: coverage role, first fresh batch" > $W/coverage_report.md || fail "coverage report failed"
.venv/bin/python - $W $RUBS $NEED > $W/comparison.md <<'PY' || fail "default_4k comparison failed"
import json, math, statistics as st, sys
sys.path.insert(0, "scripts")
import eval2_judge as ej
W, RUBS, N = sys.argv[1], sys.argv[2], int(sys.argv[3])
rd = lambda p: [json.loads(l) for l in open(p) if l.strip()]
rub = {r["id"]: r for r in rd(RUBS)}
ans = {s: {r["id"]: r for r in rd(f"{W}/ans_{s}.jsonl")} for s in ("default", "strict", "default_4k")}
res = rd(f"{W}/judge_results.jsonl")
cov = {}                                    # (set, id) -> {"P": mean over passes, "miss": mean over passes}
tmp = {}
for r in res:
    if r.get("valid_reply") and not r.get("reference_given"):
        c = ej.coverage_row(r, rub[r["id"]])
        tmp.setdefault((r["set"], r["id"]), []).append((c["P"], sum(1 for u in c["units"] if u["status"] == "missed")))
for k, v in tmp.items():
    cov[k] = {"P": sum(x[0] for x in v) / len(v), "miss": sum(x[1] for x in v) / len(v)}
ids = list(rub)
f = lambda x, d=2: "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.{d}f}"
def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else None
def own(s, i):  return ans[s][i]["seconds"]["answer"]
def total(s, i):  # a replayed set did no search or retrieval: the default run's search and retrieval seconds are added
    d = ans["default"][i]["seconds"]
    base = 0.0 if s == "default" else d["search_terms"] + d["retrieval"]
    return ans[s][i]["seconds"]["total"] + base
def cut(s, i): return 1.0 if ans[s][i].get("truncated") else 0.0
measures = [("share of answers cut off at the limit (%)", lambda s, i: 100 * cut(s, i)),
            ("seconds for the answer (generation)", own),
            ("seconds per answer including the default run's search and retrieval", total),
            ("completion tokens", lambda s, i: ans[s][i]["tokens"]),
            ("coverage P (mean of the two passes)", lambda s, i: cov[(s, i)]["P"] if (s, i) in cov else None),
            ("essential-miss units per 10 answers", lambda s, i: 10 * cov[(s, i)]["miss"] if (s, i) in cov else None)]
print("### Per answer set (all items; the two judge passes averaged per answer)")
print()
print("| answer set | answers | cut off at the limit | median completion tokens | median seconds, answer | median seconds, with search and retrieval | coverage P | essential-miss units per 10 |")
print("|---|---|---|---|---|---|---|---|")
for s in ("default", "strict", "default_4k"):
    cs = [cut(s, i) for i in ids]
    P = [cov[(s, i)]["P"] for i in ids if (s, i) in cov]
    M = [10 * cov[(s, i)]["miss"] for i in ids if (s, i) in cov]
    print(f"| {s} | {len(ids)} | {f(100 * sum(cs) / len(cs), 1)} % ({int(sum(cs))}) | {f(med([ans[s][i]['tokens'] for i in ids]), 0)} | "
          f"{f(med([own(s, i) for i in ids]), 1)} | {f(med([total(s, i) for i in ids]), 1)} | {f(sum(P) / len(P) if P else None)} | {f(sum(M) / len(M) if M else None, 1)} |")
print()
print("### default_4k against default (paired by item; difference = default_4k minus default; SE = standard deviation of the paired differences over the square root of the items)")
print()
print("| measure | default | default_4k | items | mean paired difference (SE) |")
print("|---|---|---|---|---|")
for name, fn in measures:
    a, b, d = [], [], []
    for i in ids:
        x, y = fn("default", i), fn("default_4k", i)
        if x is not None and y is not None:
            a.append(x); b.append(y); d.append(y - x)
    se = st.stdev(d) / math.sqrt(len(d)) if len(d) > 1 else None
    print(f"| {name} | {f(sum(a) / len(a) if a else None)} | {f(sum(b) / len(b) if b else None)} | {len(d)} | {f(sum(d) / len(d) if d else None)} ({f(se)}) |")
print()
print("Medians of the seconds and of the tokens are in the table above; the paired rows give means. The raised limit is descriptive on this batch (decision J4); "
      "the frozen limit stays 2,048. Seconds of a replayed set do not include search and retrieval, they were done once, in the default run; "
      "the row 'including' adds the default run's seconds for them. The default and default_4k answers are two separate generations of the same pack, so part of a "
      "difference is the model's own variation, not the limit.")
PY
{
  echo "# B1: first fresh batch of the judged tier (coverage role)"
  echo
  echo "Written by \`scripts/$N.sh\`$([ $DRY = yes ] && echo ' (DRY RUN: stub generator and stub judge, the numbers below are not measurements)'). $NEED items ($ITEMS_IN, rubrics $RUBS_IN), reference configuration, answer sets default, strict, default_4k with identical packs, judged in two passes by $DS (coverage role, instruction v2, settings $SETTING). Answers: \`$W/ans_*.jsonl\`, judge results \`$out\`."
  echo
  echo "**Status: the criteria that need the readers' findings are WAITING (C1, C2, C5), and C6 waits for the identifier check's false 'invented' count on the stored answers.** In the coverage report below, the sub-lines of C1 that say FAIL with 0 units and the line 'Go or no-go: NO-GO' count empty reader data: they are not results. C3 and C4 are decided now."
  echo
  sed 's/^# /## /; s/^## /### /' $W/coverage_report.md | sed '1s/.*/## Coverage report (eval2_pilot_report.py --coverage)/'
  echo
  echo "## Identifier check (scripts/eval2_idcheck.py, totals; CPU)"
  echo
  for s in $SETS_ORDER; do echo '```'; sed "s/^== idcheck [^ ]*/== idcheck $s/" $W/idcheck_$s.txt; echo '```'; echo; done
  echo "## The raised limit (J4, descriptive)"
  echo
  cat $W/comparison.md
  echo
  cat <<EOF
## To finish the report (the readers; CPU)

1. Bundles for the blind reader and the second reader (the key stays outside the bundle folders):
   \`.venv/bin/python scripts/eval2_blind.py bundle --items $ITEMS_IN --rubrics $RUBS --answers default=$W/ans_default.jsonl strict=$W/ans_strict.jsonl --out $W/blind_r1 --key $W/blind_key.json --second-reader 0.25 --seed 20261119 --second-out $W/blind_r2\`
2. Each reader writes a findings file (one JSON row per finding, format in the docstring of \`scripts/eval2_blind.py\`) without seeing the judge or the other reader.
3. Unblind and merge (validates the rows and lists the ones that fail; writes the findings with set and reader and the read markers):
   \`.venv/bin/python scripts/eval2_blind.py unblind --key $W/blind_key.json --rubrics $RUBS --answers default=$W/ans_default.jsonl strict=$W/ans_strict.jsonl --findings r1=<findings of the first reader> r2=<findings of the second reader> --out $W/findings_unblinded.jsonl --reads-out $W/reads.jsonl\`
4. The identifier check's false "invented" findings on the 244 stored answers and the 48 pilot answers (N, from the identifier check's own report), then the final report:
   \`.venv/bin/python scripts/eval2_pilot_report.py --coverage --results $out --rubrics $RUBS --findings $W/findings_unblinded.jsonl --reads $W/reads.jsonl --primary-reader r1 --idcheck-false-invented N --title "B1: coverage role, first fresh batch" > runs/m4/B1_COVERAGE_FINAL.md\`
EOF
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
if [ $DRY = yes ]; then
  echo M4_B1_COVERAGE_BATCH_DRY_RUN_DONE >> $L/$N.done      # inside the dry run's own directory
  sed -n '1,8p' $REPORT | cut -c1-240
  grep -E '^\| (default|strict|default_4k) \|' $REPORT | cut -c1-200 | head -12
  echo "dry run done: report from stub data in $REPORT"
  exit 0
fi
hb "$N: done; report $REPORT ($UNSCORED unscored judgments); the readers' findings are next" yes
status DONE yes "$N done: $REPORT; the blind readings wait for the readers"
echo M4_B1_COVERAGE_BATCH_DONE >> $L/$N.done
