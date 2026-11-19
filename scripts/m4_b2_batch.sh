#!/bin/sh
# M4 B2: the second fresh batch (open, development data for the retrieval check R3 and a first look at the revise step).
# B2 is B1's chain with its own markers, folders and report (scripts/m4_b1_coverage_batch.sh stays as it is). Differences from B1:
# two answer sets only (default, strict; no 4,096-token set), a gate on the independent chunk count, the R3 funnel check, and a first
# look at the revise step. Written for the Owner's go of 2026-11-18/19 (M4 standing go: the 4-bit build and the judge DeepSeek).
# 30 fresh open scenarios (eval/m4/b2), each answered on the reference configuration (the 4-bit base model on the server, thinking off,
# frozen index, catalogues given, one request at a time):
#   default      the frozen settings (answer limit 2,048 tokens); its retrieval is the pack of the strict set;
#   strict       strict mode, packs replayed from default (scripts/eval2.py answer --replay-packs).
# Steps (each resumable by its output file, each prints a line per item):
#   1. Gate (CPU, refusal exit 2, nothing written): inputs; same ids in items and rubrics; every rubric weights_status "reviewed"; rubric
#      rules (chunk ids in the frozen index, weight-2 points with chunks or not_indexed, never both, any_of groups of at least 2 existing
#      points, 3 to 6 units per item, covered_chunks = union of the points' chunks); fill-chunks --check clean (exit 0, no problem line);
#      runs/m4/B2_CHUNK_CHECK.json present with share_ok >= 0.95 (written by the conductor from the independent chunk count).
#   2. Answers, default then strict: assistant model only (guarded, stall limit 40 min).
#   3. Judge, two passes, instruction version 2, coverage role, both sets (guarded, stall limit 60 min); one more round for invalid replies.
#   4. Identifier check on both sets (CPU).
#   5. The R3 funnel (CPU and the reranker, no model server; guarded): scripts/retrieval_funnel.py, variants frozen and r3, on B2's
#      indexed must points with the search terms of the default run. The R3 settings hash is checked against runs/m5/PROFILE_R3.json
#      first. The rule of MILESTONE.md ("The rule for R3 (K3)") is applied by code: (a) r3 minus frozen at least +15 percentage points
#      of the indexed must points; (b) of the points frozen serves, r3 loses at most 5 %. PASS only if both hold.
#   6. (runs right after step 2, so the 4-bit model loads once; then it is unloaded and the judge loads) A first look at the revise step, "decides nothing": scripts/eval2.py answer --revise-from the stored default answers and packs with
#      --r2 (the 4-bit model, still loaded; guarded); counts only.
#   7. The report runs/m4/B2_BATCH.md with every number and its size. Blind bundles for the readers are built afterwards, not here.
# Model handling as B1: waits until no other chain or guarded job runs; the server must have no active or waiting request and no model of
# another client loaded; our judge builds (DeepSeek, GLM) are unloaded before the first answer; the 4-bit model is unloaded before the judge
# loads and after the revise step; the judge after judging. Heartbeat line for every swap and step. Real runs touch the model server.
#   scripts/m4_b2_batch.sh > runs/m4/logs/m4_b2_batch.out 2>&1 &
#   scripts/m4_b2_batch.sh --dry-run [--work DIR] [--chunk-check FILE]   # stub generator, stub judge, no server, no markers, no heartbeat,
#                                                                       # nothing written outside DIR (default: a temporary directory)
# Options: --items FILE --rubrics FILE (defaults eval/m4/b2/...), --chunk-check FILE, --work DIR, --report FILE, --funnel-out FILE.
# Markers: runs/m4/logs/m4_b2_batch.done or .failed.
set -e
DRY=no
ITEMS_IN=${B2_ITEMS:-eval/m4/b2/items.jsonl}; RUBS_IN=${B2_RUBRICS:-eval/m4/b2/rubrics.jsonl}
CHUNKCHECK=runs/m4/B2_CHUNK_CHECK.json
W=work/m4/b2; REPORT=runs/m4/B2_BATCH.md; FUNNEL_OUT=work/m5/b2_funnel.json; WSET=no; RSET=no; FSET=no
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --items) ITEMS_IN=${2:?}; shift;; --rubrics) RUBS_IN=${2:?}; shift;; --chunk-check) CHUNKCHECK=${2:?}; shift;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --funnel-out) FUNNEL_OUT=${2:?}; FSET=yes; shift;;
  *) echo "usage: scripts/m4_b2_batch.sh [--dry-run] [--items FILE] [--rubrics FILE] [--chunk-check FILE] [--work DIR] [--report FILE] [--funnel-out FILE]" >&2; exit 2;; esac
  shift; done
L=runs/m4/logs; N=m4_b2_batch
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; PROFILE_R3=runs/m5/PROFILE_R3.json
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
SETTING=deepseek-nothink-12k; SRC=12000
SETS_ORDER="default strict"
# B1's measured rates (30 items, 2026-11-19): default 87.2 s per answer including search and retrieval (75.0 s generation), strict 56.3 s;
# the judge 180 calls in 30.6 min (3 in parallel). The funnel and the revise step have no B2 or B1 measurement of their own (see est()).
S_DEFAULT=87; S_STRICT=56; S_JUDGE_PER_CALL=10

if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m4_b2_dry.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/report_from_stub_data.md
  [ $FSET = yes ] || FUNNEL_OUT=$W/b2_funnel.json
  G="scripts/run_guarded.py --other-retries 0"
fi
mkdir -p $W $L
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M4 B2: second fresh batch (R3 funnel check, first look at the revise step)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- 1. the gate (CPU; before anything is written outside $W)
for f in $ITEMS_IN $RUBS_IN $CHUNKS $PROFILE_R3 scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_pilot_report.py \
         scripts/run_guarded.py scripts/retrieval_funnel.py scripts/retrieval_needs.py; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
[ -s $CATS/CATALOGUES.json ] || refuse "input $CATS/CATALOGUES.json is missing"
[ -s $CHUNKCHECK ] || refuse "$CHUNKCHECK is missing: the independent chunk count has not been written (the answers wait for it)"
.venv/bin/python - $CHUNKCHECK <<'PY' || refuse "$CHUNKCHECK: share_ok is missing, not a number, or below 0.95 (the answers wait for the independent chunk count)"
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    v = d["share_ok"]
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise ValueError("share_ok is not a number")
except Exception as e:
    print(f"chunk check unreadable: {type(e).__name__}: {e}", file=sys.stderr)
    sys.exit(1)
print(f"gate: independent chunk count share_ok = {v} (needs at least 0.95)")
sys.exit(0 if v >= 0.95 else 1)
PY
NEED=$(grep -c . $ITEMS_IN | tr -d ' ')
[ "$(grep -c . $RUBS_IN | tr -d ' ')" = "$NEED" ] && [ "$NEED" -gt 0 ] || refuse "$ITEMS_IN and $RUBS_IN must have the same number of rows (at least one)"
.venv/bin/python - $ITEMS_IN $RUBS_IN <<'PY' || refuse "items and rubrics do not match (same ids in the same order)"
import json, sys
it = [json.loads(l)["id"] for l in open(sys.argv[1]) if l.strip()]
rb = [json.loads(l)["id"] for l in open(sys.argv[2]) if l.strip()]
if it != rb or len(set(it)) != len(it):
    sys.exit("item ids and rubric ids differ, or an id is repeated")
PY
.venv/bin/python - $RUBS_IN $CHUNKS <<'PY' 2> $W/gate.out || { head -15 $W/gate.out >&2; refuse "the rubrics fail the gate (reviewed, rubric rules)"; }
import json, sys
ids = {json.loads(l)["id"] for l in open(sys.argv[2]) if l.strip()}
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
bad = []
for r in rows:
    i = r["id"]
    if r.get("weights_status") != "reviewed":
        bad.append(f"{i}: weights_status is {r.get('weights_status')!r}, not 'reviewed'")
    pts = {p["id"]: p for p in r["must"]}
    if len(pts) != len(r["must"]):
        bad.append(f"{i}: a must point id is repeated")
    union = set()
    for p in r["must"]:
        ch = p.get("chunks") or []
        ni = p.get("not_indexed") is True
        union.update(ch)
        for c in ch:
            if c not in ids:
                bad.append(f"{i} {p['id']}: chunk id {c} is not in the frozen index")
        if ch and ni:
            bad.append(f"{i} {p['id']}: both chunks and not_indexed")
        if not ch and not ni:
            bad.append(f"{i} {p['id']}: neither chunks nor not_indexed true")
        if p.get("weight") not in (1, 2):
            bad.append(f"{i} {p['id']}: weight is not 1 or 2")
    groups = r.get("any_of") or []
    grouped = set()
    for g in groups:
        if len(g) < 2 or any(m not in pts for m in g):
            bad.append(f"{i}: any_of group {g} has fewer than 2 members or a member that does not exist")
        grouped.update(g)
    units = len(r["must"]) - sum(len(g) for g in groups) + len(groups)
    if not 3 <= units <= 6:
        bad.append(f"{i}: {units} units (3 to 6 allowed)")
    if set(r.get("covered_chunks") or []) != union:
        bad.append(f"{i}: covered_chunks differs from the union of the points' chunks")
    for m in r.get("must_not") or []:
        if not m.get("id") or not m.get("text"):
            bad.append(f"{i}: a must_not point without id or text")
for b in bad[:15]:
    print(b, file=sys.stderr)
if len(bad) > 15:
    print("... and %d more" % (len(bad) - 15), file=sys.stderr)
print(f"gate: {len(rows)} rubrics, all weights_status reviewed, rubric rules hold", file=sys.stderr if bad else sys.stdout)
sys.exit(1 if bad else 0)
PY
echo "gate: all rubrics reviewed, rubric rules hold (chunk ids in the frozen index, weight-2 points with chunks or not_indexed, any_of groups, 3 to 6 units, covered_chunks)"
.venv/bin/python scripts/eval2_judge.py fill-chunks --rubrics $RUBS_IN --chunks $CHUNKS --check > $W/chunks_check.out 2>&1 \
  || { head -15 $W/chunks_check.out >&2; refuse "fill-chunks --check reports a problem"; }
nprob=$(grep -c '^problem:' $W/chunks_check.out || true)
[ "$nprob" = 0 ] || refuse "fill-chunks --check: $nprob problem lines"
echo "gate: fill-chunks --check clean (0 problems)"
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
  rm -f $L/$N.failed $L/$N.done
fi
mkdir -p runs/m4/reports; mkdir -p "$(dirname $REPORT)" "$(dirname $FUNNEL_OUT)"
RUBS=$RUBS_IN

api() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
foreign() { # loaded models that are not ours
  api /api/status | python3 -c 'import sys,json; ours=set(sys.argv[1:]); print(" ".join(m for m in (json.load(sys.stdin).get("loaded_models") or []) if m not in ours))' $DS $GLM8 $GLM4 $Q4 2>/dev/null || echo "status-unreadable"; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0   # only models this chain knows as its own (never another client's)
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" >/dev/null; hb "$N: unloaded $1 (no active or waiting requests)"; }
server_free() { n=0; until [ "$(busy)" = "0" ] && [ -z "$(foreign)" ]; do
    sleep 30; n=$((n+1))
    [ $n -lt 120 ] || fail "server not free after 60 min (active or waiting requests, or a model of another client loaded: $(foreign))"
  done; }
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
        row = {"tier": "B", "kind": "open", "family": r["id"], "domain": "b2", **r}
        if isinstance(row["domain"], list):   # the harness wants a string: joined, the list kept as "domains"
            row["domains"], row["domain"] = row["domain"], ",".join(map(str, row["domain"])) or "b2"
        for k in ("tier", "kind", "family", "domain"):
            if not isinstance(row[k], str) or not row[k]:
                sys.exit(f"item {r['id']}: {k} is not a non-empty string")
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
PY
.venv/bin/python -m kafka_sre_rag verify > /dev/null || fail "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"

# the plan and the estimate (B1's measured rates; the funnel and the revise step are guesses and say so)
NSETS=2; NANS=$((NEED * NSETS)); CALLS=$((NANS * 2))
est() { # items -> the plan and the time
  n=$1; a=$((n * (S_DEFAULT + S_STRICT))); j=$((n * NSETS * 2 * S_JUDGE_PER_CALL)); fun_lo=300; fun_hi=900; rev_lo=$((n * 12)); rev_hi=$((n * 40)); load=1500
  echo "$((n * NSETS)) answers ($n items x default, strict), $((n * NSETS * 2)) judge calls: answering $((a / 60)) min, judging $((j / 60)) min, funnel 5 to 15 min (guess), revise $((rev_lo / 60)) to $((rev_hi / 60)) min (guess), loads and checks 25 min; total $(awk -v lo=$((a + j + fun_lo + rev_lo + load)) -v hi=$((a + j + fun_hi + rev_hi + load)) 'BEGIN{printf "%.1f to %.1f", lo/3600, hi/3600}') h"; }
echo "planned: $(est $NEED)"
[ "$NEED" = 30 ] || echo "planned for 30 items: $(est 30)"
echo "estimate basis: B1's measured rates (${S_DEFAULT} s per default answer including search and retrieval, ${S_STRICT} s per strict answer, judge 180 calls in 30.6 min = ${S_JUDGE_PER_CALL} s per call at 3 in parallel); funnel and revise are not measured"

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
judge_done() { # FILE EXPECTED_ROWS
  [ -f "$1" ] && .venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; sys.exit(0 if len(r)==int(sys.argv[2]) and all(x["valid_reply"] for x in r) else 1)' "$1" "$2" 2>/dev/null; }
check_revise() { # FILE EXPECTED_ROWS: one row per answer, no error, manifest ended
  [ -f "$1" ] && .venv/bin/python -c '
import json, os, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
mp = sys.argv[1] + ".manifest.json"
ok = len(rows) == int(sys.argv[2]) and not any(r.get("error") for r in rows) and os.path.exists(mp) and bool(json.load(open(mp)).get("ended"))
sys.exit(0 if ok else 1)' "$1" "$2" 2>/dev/null; }
funnel_done() { [ -f "$1" ] && .venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if "frozen" in d["totals"] and "r3" in d["totals"] else 1)' "$1" 2>/dev/null; }
ANS_DONE=yes
for s in $SETS_ORDER; do check_answers $W/ans_$s.jsonl $NEED >/dev/null 2>&1 || ANS_DONE=no; done
JUDGE_DONE=no; judge_done $W/judge_results.jsonl $CALLS && JUDGE_DONE=yes
REV=$W/revise_default.jsonl
REV_DONE=no; check_revise $REV $NEED && REV_DONE=yes
SERVER_NEEDED=yes; [ $ANS_DONE = yes ] && [ $JUDGE_DONE = yes ] && [ $REV_DONE = yes ] && SERVER_NEEDED=no

# ---------------------------------------------------------------- waiting and the server (nothing to wait for when all is finished)
if [ $DRY = yes ]; then
  echo "dry run in $W (stub generator, stub judge, stub reviser, no server)"
  Q4=Qwen3.8-27B-4bit
elif [ $SERVER_NEEDED = no ]; then
  echo "answers, judge results and the revise records are finished: no server needed"
  Q4=none
else
  hb "$N: waiting until no other chain (scripts/m4_*.sh, scripts/s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done
  api /v1/models > $W/models.json 2>/dev/null || true
  Q4=${M4_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || fail "4-bit model not listed by the server (or ambiguous)"
  hb "$N: started; 4-bit model $Q4; $NEED items in default and strict, then judge $DS (coverage role, instruction v2, settings $SETTING; $CALLS judge calls planned), the R3 funnel (CPU), the revise look (4-bit model again)"
  status RUNNING no "$N running: answers ($NANS), judging ($CALLS calls), funnel, revise; markers $L/$N.done / .failed"
  server_free
  unload $DS; unload $GLM4; unload $GLM8
  hb "$N: DeepSeek and the GLM builds unloaded if they were loaded; the 4-bit model loads on the first request"
fi

# ---------------------------------------------------------------- 2. answers
answer_run() { # NAME MODE [extra answer arguments]: all items, one request at a time
  name=$1; mode=$2; shift 2; out=$W/ans_$name.jsonl
  if check_answers $out $NEED > $W/check_$name.out 2>&1; then
    echo "skip answers $name: $(cat $W/check_$name.out)"; return 0; fi
  if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $Q4"; fi
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    $G --stall-min 40 --log $L/$N.answers_$name.log -- .venv/bin/python scripts/eval2.py answer --items $HITEMS --out $out \
      --config b2-$name $GA --extra-body "$EB" --mode $mode --catalogue $CATS --host-class "Mac Studio" "$@" \
      || fail "answers $name: failed (see $L/$N.answers_$name.log)"
    check_answers $out $NEED && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $name: incomplete, with errors, empty or without a pack after $tries passes ($out)"
  done
  hb "$N: answers $name done ($NEED rows)"
  status RUNNING no "$N running: answers $name done; markers $L/$N.done / .failed"
}
if [ $ANS_DONE = no ]; then
  answer_run default default
  answer_run strict strict --replay-packs $W/ans_default.jsonl
else
  echo "skip answers: both sets are finished"
fi
.venv/bin/python - $W <<'PY' || fail "the two answer sets do not have identical packs (replay failed)"
import json, sys
w = sys.argv[1]
ps = {}
for s in ("default", "strict"):
    ps[s] = {}
    for line in open(f"{w}/ans_{s}.jsonl"):
        if line.strip():
            r = json.loads(line)
            ps[s][r["id"]] = r["pack"]
ok = ps["default"] == ps["strict"]
print(f"packs identical in default and strict: {'yes' if ok else 'NO'} ({len(ps['default'])} items)")
sys.exit(0 if ok else 1)
PY
SETS="default=$W/ans_default.jsonl strict=$W/ans_strict.jsonl"

# ---------------------------------------------------------------- 2b. a first look at the revise step (decides nothing); the 4-bit model is still loaded from the answers
if check_revise $REV $NEED; then
  echo "skip revise: $REV is finished"
else
  if [ $DRY = yes ]; then RA="--backend stub"
  else
    RA="--backend openai --model $Q4"
  fi
  $G --stall-min 40 --log $L/$N.revise.log -- .venv/bin/python scripts/eval2.py answer --items $HITEMS --revise-from $W/ans_default.jsonl \
    --out $REV --config b2-revise --r2 $RA --extra-body "$EB" --catalogue $CATS || fail "revise look failed (see $L/$N.revise.log)"
  check_revise $REV $NEED || fail "revise look: $REV is incomplete or has errors"
fi
.venv/bin/python - $REV $NEED > $W/revise_counts.txt <<'PY' || fail "could not count the revise records"
import json, statistics as st, sys, collections
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
n = len(rows)
changed = sum(1 for r in rows if r["final_text"] != r["first_pass_text"])
flagged = sum(1 for r in rows if r["flagged"])
status = collections.Counter(r["r2"]["status"] for r in rows)
called = [r for r in rows if r["r2"]["status"] in ("accepted", "rejected", "no_patch", "error")]
t = sorted(r["timings"]["r2_s"] for r in called)
rc = sorted(r["timings"]["recheck_s"] for r in rows if r["timings"].get("recheck_s"))
tot = sorted(r["timings"]["total_s"] for r in rows)
p90 = lambda xs: xs[min(len(xs) - 1, int(0.9 * len(xs)))] if xs else None
f = lambda x: "n/a" if x is None else f"{x:.1f}"
print(f"revise look, labelled 'decides nothing' (first look on B2, MILESTONE.md K4); {n} default answers, R1 marks and the R2 patch with re-check")
print(f"answers with at least one flagged finding: {flagged} of {n}")
print(f"answers changed (final text differs from the first-pass text): {changed} of {n}")
print(f"R2 patches accepted: {status['accepted']}; rejected: {status['rejected']}; no patch named: {status['no_patch']}; model call failed: {status['error']}; not needed: {status['not_needed']}")
print(f"seconds per R2 model call ({len(called)} calls): median {f(st.median(t) if t else None)}, 90th percentile {f(p90(t))}, max {f(t[-1] if t else None)}")
print(f"seconds for the re-check after an accepted or tested patch ({len(rc)} rows): median {f(st.median(rc) if rc else None)}")
print(f"added seconds per answer over all {n} answers (check, R2 call, re-check): median {f(st.median(tot))}, 90th percentile {f(p90(tot))}")
PY
cat $W/revise_counts.txt

# ---------------------------------------------------------------- 3. judging: the judge only; two passes, coverage role
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then unload $Q4; hb "$N: answering and revise look finished, $Q4 unloaded; the judge $DS loads on the first request"; fi
UNSCORED=0
jrun() { # extra arguments
  if [ $DRY = yes ]; then
    .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS_IN --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
      --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $SETTING --concurrency 3 --stub "$@"
  else
    $G --stall-min 60 --log $L/$N.judge.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS_IN \
      --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
      --settings-name $SETTING --concurrency 3 --judge-url "$OMLX_URL/v1" "$@"
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

# ---------------------------------------------------------------- 4. identifier check (CPU)
for s in $SETS_ORDER; do
  .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_$s.jsonl --catalogues $CATS --totals-only > $W/idcheck_$s.txt || fail "identifier check on $s failed"
  echo "idcheck $s: $(sed -n 4p $W/idcheck_$s.txt)"
done

# ---------------------------------------------------------------- 5. the R3 funnel (CPU and the reranker; no model server)
.venv/bin/python - $PROFILE_R3 <<'PY' || fail "the R3 settings hash differs from $PROFILE_R3 (kafka_sre_rag/candidate.json or frozen.json changed)"
import json, sys
sys.path.insert(0, ".")
from kafka_sre_rag import profiles
want = json.load(open(sys.argv[1]))["settings_sha256"]
got = profiles.settings_sha256("r3")
print(f"R3 settings hash {got[:8]}...{got[-7:]} {'equals' if got == want else 'DIFFERS FROM'} {sys.argv[1]} ({want[:8]}...{want[-7:]})")
sys.exit(0 if got == want else 1)
PY
NEEDS=$W/b2_needs.jsonl
if [ ! -s $NEEDS ]; then
  .venv/bin/python - $ITEMS_IN $RUBS $NEEDS <<'PY' || fail "could not build the needs file of B2"
import json, sys
sys.path.insert(0, "scripts")
import retrieval_needs as rn
rows = rn.build_b1(rn.INDEX, sys.argv[1], sys.argv[2])   # point rows of the rubrics; the function is not specific to B1
head = {"row_type": "header", "set": "b2", "note": "Needs of batch B2 (eval/m4/b2): one row per must point; built by scripts/m4_b2_batch.sh with retrieval_needs.build_b1.",
        "index_sha256": rn.check_index_hash(), "rows": len(rows)}
with open(sys.argv[3], "w") as f:
    for r in [head] + rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
n_idx = sum(1 for r in rows if r["status"] != "not_indexed")
print(f"needs of B2: {len(rows)} must points, {n_idx} indexed, {len(rows) - n_idx} not_indexed")
PY
fi
if funnel_done $FUNNEL_OUT; then
  echo "skip funnel: $FUNNEL_OUT is finished"
else
  [ ! -e $FUNNEL_OUT ] || fail "$FUNNEL_OUT exists but holds no frozen and r3 totals; not overwritten"
  if [ $DRY = yes ]; then FCACHE=$W/funnel_cache; else FCACHE=work/m5/funnel_cache; fi
  hb "$N: funnel (frozen, r3) on B2 started (CPU and reranker)"
  $G --stall-min 30 --log $L/$N.funnel_replay.log -- .venv/bin/python scripts/retrieval_funnel.py replay --needs $NEEDS --answers $W/ans_default.jsonl \
    --items $ITEMS_IN --cache-dir $FCACHE || fail "funnel replay failed (see $L/$N.funnel_replay.log)"
  grep '^replay of' $L/$N.funnel_replay.log || true
  if [ $DRY = no ] && ! grep -q '^replay of [0-9]* stored packs with the frozen variant: exact [0-9]*$' $L/$N.funnel_replay.log; then
    fail "the frozen variant of the funnel does not reproduce every stored pack exactly (see $L/$N.funnel_replay.log); the comparison would not be valid"
  fi
  $G --stall-min 30 --log $L/$N.funnel.log -- .venv/bin/python scripts/retrieval_funnel.py run --needs $NEEDS --answers $W/ans_default.jsonl \
    --items $ITEMS_IN --presets r3 --cache-dir $FCACHE --json $FUNNEL_OUT || fail "funnel failed (see $L/$N.funnel.log)"
  hb "$N: funnel done ($FUNNEL_OUT)"
fi
.venv/bin/python - $FUNNEL_OUT > $W/funnel_verdict.txt <<'PY' || fail "could not apply the rule of R3 to the funnel output"
import json, sys
d = json.load(open(sys.argv[1]))["totals"]
fr, r3 = d["frozen"]["points"], d["r3"]["points"]
of = fr["of"]
pp = 100.0 * (r3["served"] - fr["served"]) / of if of else float("nan")
loss = 100.0 * r3["lost"] / fr["served"] if fr["served"] else 0.0
a, b = pp >= 15.0, loss <= 5.0
print(f"R3 rule (MILESTONE.md, K3), on {of} indexed must points of B2: frozen serves {fr['served']}, r3 serves {r3['served']} (gained {r3['gained']}, lost {r3['lost']})")
print(f"(a) r3 minus frozen = {pp:+.1f} percentage points (needs at least +15): {'holds' if a else 'FAILS'}")
print(f"(b) of the {fr['served']} points frozen serves, r3 loses {r3['lost']} = {loss:.1f} % (needs at most 5 %): {'holds' if b else 'FAILS'}")
w2f, w2r = d["frozen"]["weight2"], d["r3"]["weight2"]
print(f"beside it, weight-2 points: frozen {w2f['served']} of {w2f['of']}, r3 {w2r['served']} of {w2r['of']}")
print(f"R3 funnel check: {'PASS' if a and b else 'FAIL'}")
PY
cat $W/funnel_verdict.txt

# ---------------------------------------------------------------- 7. the report
: > $W/findings_none.jsonl
.venv/bin/python scripts/eval2_pilot_report.py --coverage --results $out --rubrics $RUBS --findings $W/findings_none.jsonl \
  --title "B2: coverage role, second fresh batch" > $W/coverage_report.md || fail "coverage report failed"
.venv/bin/python - $W $RUBS $NEED > $W/per_set.md <<'PY' || fail "per-set table failed"
import json, math, statistics as st, sys
sys.path.insert(0, "scripts")
import eval2_judge as ej
W, RUBS, N = sys.argv[1], sys.argv[2], int(sys.argv[3])
rd = lambda p: [json.loads(l) for l in open(p) if l.strip()]
rub = {r["id"]: r for r in rd(RUBS)}
ans = {s: {r["id"]: r for r in rd(f"{W}/ans_{s}.jsonl")} for s in ("default", "strict")}
tmp = {}
for r in rd(f"{W}/judge_results.jsonl"):
    if r.get("valid_reply") and not r.get("reference_given"):
        c = ej.coverage_row(r, rub[r["id"]])
        tmp.setdefault((r["set"], r["id"]), []).append((c["P"], sum(1 for u in c["units"] if u["status"] == "missed")))
cov = {k: {"P": sum(x[0] for x in v) / len(v), "miss": sum(x[1] for x in v) / len(v)} for k, v in tmp.items()}
ids = list(rub)
f = lambda x, d=2: "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x:.{d}f}"
def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else None
def total(s, i):  # a replayed set did no search or retrieval: the default run's seconds for them are added
    d = ans["default"][i]["seconds"]
    own = ans[s][i]["seconds"]["total"]
    return None if own is None else own + (0.0 if s == "default" else (d["search_terms"] or 0) + (d["retrieval"] or 0))
print("### Per answer set (all items; the two judge passes averaged per answer)")
print()
print("| answer set | answers | cut off at the limit | median completion tokens | median seconds, answer | median seconds, with search and retrieval | coverage P | essential-miss units per 10 |")
print("|---|---|---|---|---|---|---|---|")
for s in ("default", "strict"):
    cut = sum(1 for i in ids if ans[s][i].get("truncated"))
    P = [cov[(s, i)]["P"] for i in ids if (s, i) in cov]
    M = [10 * cov[(s, i)]["miss"] for i in ids if (s, i) in cov]
    print(f"| {s} | {len(ids)} | {f(100 * cut / len(ids), 1)} % ({cut}) | {f(med([ans[s][i]['tokens'] for i in ids]), 0)} | "
          f"{f(med([ans[s][i]['seconds']['answer'] for i in ids]), 1)} | {f(med([total(s, i) for i in ids]), 1)} | {f(sum(P) / len(P) if P else None)} | {f(sum(M) / len(M) if M else None, 1)} |")
PY
{
  echo "# B2: second fresh batch (open; development data for R3 and a first look at the revise step)"
  echo
  echo "Written by \`scripts/$N.sh\`$([ $DRY = yes ] && echo ' (DRY RUN: stub generator, stub judge, stub reviser; the numbers below are not measurements)'). $NEED items ($ITEMS_IN, rubrics $RUBS_IN), reference configuration, answer sets default and strict with identical packs, judged in two passes by $DS (coverage role, instruction v2, settings $SETTING). Answers: \`$W/ans_*.jsonl\`, judge results \`$out\`. B2 is open: it is development data and decides nothing except the R3 funnel check below, by the rule fixed in MILESTONE.md (K3) before B2 existed."
  echo
  echo "## Gate"
  echo
  echo "- independent chunk count: \`$CHUNKCHECK\`, share_ok $(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["share_ok"])' $CHUNKCHECK) (at least 0.95 needed)"
  echo "- rubrics: $NEED, all weights_status reviewed, rubric rules hold; fill-chunks --check: 0 problems"
  echo
  echo "## R3 funnel check (K3; CPU and reranker; search terms of the default run for both variants)"
  echo
  echo '```'; cat $W/funnel_verdict.txt; echo '```'
  echo
  echo "Funnel output: \`$FUNNEL_OUT\`. B2 is open development data; on a PASS the hidden-half run of J6 decides, on a FAIL WP5 closes with the frozen retrieval."
  echo
  echo "## First look at the revise step (decides nothing)"
  echo
  echo '```'; cat $W/revise_counts.txt; echo '```'
  echo
  echo "Records: \`$REV\` (first-pass text, findings, patches, final text, both check results, timings). The rule that decides the revise step is the one of MILESTONE.md (K4), run once on the hidden half; counts on B2 are a look only. Errors removed and introduced are not counted here: that needs a reader."
  echo
  cat $W/per_set.md
  echo
  echo "**The criteria that need the readers' findings are WAITING.** In the coverage report below, the sub-lines of C1 that say FAIL with 0 units and the line 'Go or no-go: NO-GO' count empty reader data: they are not results. Blind bundles for the readers are built by the conductor afterwards."
  echo
  sed 's/^# /## /; s/^## /### /' $W/coverage_report.md | sed '1s/.*/## Coverage report (eval2_pilot_report.py --coverage)/'
  echo
  echo "## Identifier check (scripts/eval2_idcheck.py, totals; CPU)"
  echo
  for s in $SETS_ORDER; do echo '```'; sed "s/^== idcheck [^ ]*/== idcheck $s/" $W/idcheck_$s.txt; echo '```'; echo; done
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
if [ $DRY = yes ]; then
  echo M4_B2_BATCH_DRY_RUN_DONE >> $L/$N.done      # inside the dry run's own directory
  sed -n '1,6p' $REPORT | cut -c1-240
  grep -E '^\| (default|strict) \|' $REPORT | cut -c1-200 | head -4
  echo "dry run done: report from stub data in $REPORT"
  exit 0
fi
hb "$N: done; report $REPORT ($UNSCORED unscored judgments); the funnel verdict and the readers' bundles are next" yes
status DONE yes "$N done: $REPORT; the blind bundles for the readers wait for the conductor"
echo M4_B2_BATCH_DONE >> $L/$N.done
