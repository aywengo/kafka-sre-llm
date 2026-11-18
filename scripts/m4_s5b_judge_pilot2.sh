#!/bin/sh
# M4 S5b: second pilot of the judge step, version 2 (runs/m4/reports/s5_pilot_analysis.md, "Second pilot";
# runs/m4/reports/s5b_judge_v2.md). 24 fresh tier B items, answered in default and strict mode on the reference configuration
# (the 4-bit base model on the server, thinking off, frozen index, catalogues given, one request at a time): 48 answers; judged in two
# passes by DeepSeek without thinking (settings name deepseek-nothink-12k), instruction v2, three calls in parallel. Unattended; it
# takes the model server, no local GPU.
# Inputs (written by someone else, the chain refuses to start without them):
#   eval/m4/pilot2/items.jsonl     24 rows {"id", "question", optional "evidence"}  (tier, kind, family, domain are filled in if missing)
#   eval/m4/pilot2/rubrics.jsonl   24 rows as eval/m4/pilot/rubrics.jsonl (id = the item's id); optional per row "reference" answer
# Steps:
#   0. waits until none of our chains runs (m4_s6_noise_4bit.sh, m4_s5a_judge_settings.sh, m4_s5_judge_pilot.sh);
#   1. server: idle, our judge models (DeepSeek, GLM builds) unloaded, no model of another client loaded; the 4-bit model's name comes
#      from GET /v1/models (override: M4_Q4_MODEL);
#   2. answers: default, then strict (scripts/eval2.py answer, resumable, guarded, stall limit 40 min); 24 non-empty rows each, with a pack;
#   3. unloads the 4-bit model; judging (scripts/eval2_judge.py run --instruction v2, guarded, stall limit 60 min), pass 1 and 2 on the
#      48 answers, one more round for replies that were not valid JSON; with --reference-arm also pass 1 on the 48 answers with the
#      rubric's reference answer added (sets default_ref and strict_ref; needs a "reference" in every rubric); unloads the judge;
#   4. CPU: the review file of candidate errors (runs/m4/S5B_REVIEW_FLAGS.jsonl) and the report runs/m4/S5B_JUDGE_PILOT2.md
#      (criteria 1 to 3 are decided; 4, 5 and the ordering wait for the reviewer's review file and findings file; the Owner decides).
# Before every unload the server must have no active or waiting request. Outputs are new files under work/m4/pilot2/.
# Markers: runs/m4/logs/m4_s5b_judge_pilot2.done or .failed.
#   scripts/m4_s5b_judge_pilot2.sh [--reference-arm] > runs/m4/logs/m4_s5b_judge_pilot2.out 2>&1 &
#   scripts/m4_s5b_judge_pilot2.sh --dry-run [--reference-arm]   # stub generator and stub judge on 4 fixture items, no server, no markers,
#                                                                 # no heartbeat, nothing written outside a temporary directory
# Running time: answering 48 x 47 to 71 s = 0.6 to 1.0 h; judging 96 to 144 calls, 0.2 to 0.4 h; loads and checks 0.3 h. About 1.1 to 1.7 h.
set -e
DRY=no; REFARM=no
for arg in "$@"; do case $arg in
  --dry-run) DRY=yes;; --reference-arm) REFARM=yes;;
  *) echo "usage: scripts/m4_s5b_judge_pilot2.sh [--dry-run] [--reference-arm]" >&2; exit 2;; esac; done
IN=eval/m4/pilot2; ITEMS_IN=$IN/items.jsonl; RUBS=$IN/rubrics.jsonl
W=work/m4/pilot2; L=runs/m4/logs; N=m4_s5b_judge_pilot2; REPORT=runs/m4/S5B_JUDGE_PILOT2.md; REVIEW=runs/m4/S5B_REVIEW_FLAGS.jsonl
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
SETTING=deepseek-nothink-12k; SRC=12000; NEED=24
CHAINS_RE='^(/bin/)?(ba)?sh +(\./)?scripts/m4_(s6_noise_4bit|s5a_judge_settings|s5_judge_pilot)\.sh'

# 0. inputs: refuse to start without the items and the rubrics (nothing is written: no marker, no heartbeat, no status)
if [ $DRY = no ]; then
  for f in $ITEMS_IN $RUBS $CHUNKS $CATS/CATALOGUES.json scripts/eval2.py scripts/eval2_judge.py scripts/eval2_pilot_report.py scripts/run_guarded.py; do
    [ -s $f ] || { echo "REFUSED: input $f is missing or empty; nothing started" >&2; exit 2; }
  done
  for f in $ITEMS_IN $RUBS; do
    [ "$(wc -l < $f | tr -d ' ')" = "$NEED" ] || { echo "REFUSED: $f has $(wc -l < $f | tr -d ' ') rows, not $NEED; nothing started" >&2; exit 2; }
  done
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
else
  W=$(mktemp -d "${TMPDIR:-/tmp}/m4_s5b_dry.XXXXXX"); L=$W/logs; mkdir -p $L; REPORT=$W/report_from_stub_data.md; REVIEW=$W/review_flags.jsonl
  G="scripts/run_guarded.py --other-retries 0"; NEED=4
fi

hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M4 S5b: judge pilot 2 (24 items, default and strict, instruction v2)",
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
foreign() { # loaded models that are not ours
  api /api/status | python3 -c 'import sys,json; ours=set(sys.argv[1:]); print(" ".join(m for m in (json.load(sys.stdin).get("loaded_models") or []) if m not in ours))' $DS $GLM8 $GLM4 $Q4 2>/dev/null || echo "status-unreadable"; }
unload() { idle; [ "$(loaded $1)" = "yes" ] || return 0
  curl -s -m 300 -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" >/dev/null; hb "$N: unloaded $1 (no active or waiting requests)"; }
rows() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }
check_answers() { # FILE EXPECTED_ROWS: complete, no error, no empty text, every row with a pack
  .venv/bin/python - "$1" "$2" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
n = int(sys.argv[2])
err = [r["id"] for r in rows if r.get("error")]
empty = [r["id"] for r in rows if not r.get("error") and not (r.get("text") or "").strip()]
nopack = [r["id"] for r in rows if not r.get("error") and not r.get("pack")]
print(f"check {sys.argv[1]}: {len(rows)} of {n} rows, {len(err)} errors, {len(empty)} empty, {len(nopack)} without a pack")
sys.exit(0 if len(rows) == n and not err and not empty and not nopack else 1)
PY
}
answer_run() { # NAME MODE: all items, one request at a time
  name=$1; mode=$2; out=$W/ans_$name.jsonl
  if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $Q4"; fi
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    $G --stall-min 40 --log $L/$N.answers_$name.log -- .venv/bin/python scripts/eval2.py answer --items $HITEMS --out $out \
      --config s5b-$name $GA --extra-body "$EB" --mode $mode --catalogue $CATS --host-class "Mac Studio" \
      || fail "answers $name: failed (see $L/$N.answers_$name.log)"
    check_answers $out $NEED && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $name: incomplete, with errors, empty or without a pack after $tries passes ($out)"
  done
  hb "$N: answers $name done ($NEED rows)"
  status RUNNING no "$N running: answers $name done; markers $L/$N.done / .failed"
}
# the judge program; $1 = answer sets (NAME=PATH ...), the rest = further arguments
judge() { sets=$1; shift
  .venv/bin/python scripts/eval2_judge.py run --rubrics $JRUBS --items $ITEMS --chunks $CHUNKS --source-chars $SRC --instruction v2 \
    --answers $sets --judge-model $DS --judge-extra-body "$JEB" --settings-name $SETTING --concurrency 3 "$@"; }
jguard() { sets=$1; shift # guarded real run: $1 = answer sets, the rest further arguments (round extras)
  $G --stall-min 60 --log $L/$N.judge.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $JRUBS --items $ITEMS \
    --chunks $CHUNKS --source-chars $SRC --instruction v2 --answers $sets --judge-model $DS --judge-extra-body "$JEB" \
    --settings-name $SETTING --concurrency 3 --judge-url "$OMLX_URL/v1" "$@"; }

mkdir -p $W $L runs/m4/reports
[ $DRY = yes ] || rm -f $L/$N.failed $L/$N.done

# the items for the harness (tier, kind, family, domain filled in) and, in the dry run, 4 fixture items from the first pilot
if [ $DRY = yes ]; then
  ITEMS=$W/items.jsonl; JRUBS=$W/rubrics.jsonl
  .venv/bin/python - $ITEMS $JRUBS <<'PY' || { echo "DRY RUN FAILED: fixture"; exit 1; }
import json, sys
qs = [json.loads(l)["question"] for l in open("eval/questions_real.jsonl") if l.strip()]
rubs = [json.loads(l) for l in open("eval/m4/pilot/rubrics.jsonl") if l.strip()]
keep = [r for r in rubs if r["id"] in ("real-1", "real-3", "real-5", "real-31")]
with open(sys.argv[1], "w") as f, open(sys.argv[2], "w") as g:
    for r in keep:
        q = qs[int(r["id"].split("-")[1]) - 1]
        f.write(json.dumps({"id": r["id"], "question": q}) + "\n")
        g.write(json.dumps({**r, "question": q, "reference": "Fixture reference answer for " + r["id"]}) + "\n")
PY
else
  ITEMS=$ITEMS_IN; JRUBS=$RUBS
fi
HITEMS=$W/items_harness.jsonl
.venv/bin/python - $ITEMS $HITEMS <<'PY' || fail "could not build the harness items file"
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
with open(sys.argv[2], "w") as f:
    for r in rows:
        if not r.get("id") or not r.get("question"):
            sys.exit("item without id or question")
        f.write(json.dumps({"tier": "B", "kind": "open", "family": r["id"], "domain": "pilot2", **r}, ensure_ascii=False) + "\n")
PY
.venv/bin/python - $ITEMS $JRUBS $REFARM <<'PY' || fail "items and rubrics do not match (same ids, same order) or a rubric has no reference for --reference-arm"
import json, sys
it = [json.loads(l)["id"] for l in open(sys.argv[1]) if l.strip()]
rb = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
if it != [r["id"] for r in rb]:
    sys.exit("item ids and rubric ids differ")
if sys.argv[3] == "yes" and not all((r.get("reference") or "").strip() for r in rb):
    sys.exit("a rubric has no reference")
PY
.venv/bin/python -m kafka_sre_rag verify > /dev/null || fail "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"

if [ $DRY = yes ]; then
  echo "dry run in $W (stub generator, stub judge, $NEED fixture items; real inputs $ITEMS_IN and $RUBS present: $([ -s $ITEMS_IN ] && [ -s $RUBS ] && echo yes || echo no))"
  Q4=Qwen3.8-27B-4bit
else
  hb "$N: waiting until none of m4_s6_noise_4bit.sh, m4_s5a_judge_settings.sh, m4_s5_judge_pilot.sh runs, then the server check"
  status RUNNING no "$N waiting for our other chains to end; markers $L/$N.done / .failed"
  until ! pgrep -f "$CHAINS_RE" >/dev/null; do sleep 60; done
  api /v1/models > $W/models.json 2>/dev/null || true
  Q4=${M4_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || fail "4-bit model not listed by the server (or ambiguous)"
  hb "$N: started; 4-bit model $Q4; $NEED items in default and strict mode, then judge $DS (instruction v2, settings $SETTING)$([ $REFARM = yes ] && echo ', with the reference arm')"
  status RUNNING no "$N running: answers (48), then judging; markers $L/$N.done / .failed"
  n=0
  until [ "$(busy)" = "0" ] && [ -z "$(foreign)" ]; do
    sleep 30; n=$((n+1))
    [ $n -lt 120 ] || fail "server not free after 60 min (active or waiting requests, or a model of another client loaded: $(foreign))"
  done
  unload $DS; unload $GLM4; unload $GLM8
  hb "$N: DeepSeek and the GLM builds unloaded if they were loaded; the 4-bit model loads on the first request"
fi

# 2. answers
answer_run default default
answer_run strict strict
SETS="default=$W/ans_default.jsonl strict=$W/ans_strict.jsonl"
SETS_REF="default_ref=$W/ans_default.jsonl strict_ref=$W/ans_strict.jsonl"
[ $DRY = yes ] || { unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; }

# 3. judging
if [ $DRY = yes ]; then
  judge "$SETS" --stub --passes 1 2 --out $W/judge_results_main.jsonl > $W/judge.dry.out 2>&1 || { cat $W/judge.dry.out; fail "judge dry run failed"; }
  grep -E '^(matched|instruction|items)' $W/judge.dry.out
  [ "$(grep -c '^matched' $W/judge.dry.out)" = "2" ] && [ "$(rows $W/judge_results_main.jsonl)" = "$((NEED * 4))" ] || fail "judge dry run: answers not matched or rows missing"
  cp $W/judge_results_main.jsonl $W/judge_results.jsonl
  if [ $REFARM = yes ]; then
    judge "$SETS_REF" --stub --passes 1 --with-reference --out $W/judge_results_ref.jsonl > $W/judge_ref.dry.out 2>&1 || { cat $W/judge_ref.dry.out; fail "reference arm dry run failed"; }
    cat $W/judge_results_ref.jsonl >> $W/judge_results.jsonl
  fi
else
  for round in 1 2; do
    extra=""; [ $round = 2 ] && extra="--retry-invalid"
    jguard "$SETS" --passes 1 2 --cache $W/judge_cache.jsonl --out $W/judge_results_main.jsonl $extra || fail "judging round $round failed (see $L/$N.judge.log)"
    bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $W/judge_results_main.jsonl)
    echo "round $round: $bad unscored"
    [ "$bad" = "0" ] && break
  done
  [ "$(rows $W/judge_results_main.jsonl)" = "$((NEED * 4))" ] || fail "$W/judge_results_main.jsonl has $(rows $W/judge_results_main.jsonl) rows, not $((NEED * 4)) ($((NEED * 2)) answers x 2 passes)"
  cp $W/judge_results_main.jsonl $W/judge_results.jsonl.tmp
  if [ $REFARM = yes ]; then
    jguard "$SETS_REF" --passes 1 --with-reference --cache $W/judge_cache.jsonl --out $W/judge_results_ref.jsonl --retry-invalid || fail "reference arm failed (see $L/$N.judge.log)"
    [ "$(rows $W/judge_results_ref.jsonl)" = "$((NEED * 2))" ] || fail "$W/judge_results_ref.jsonl has $(rows $W/judge_results_ref.jsonl) rows, not $((NEED * 2))"
    cat $W/judge_results_ref.jsonl >> $W/judge_results.jsonl.tmp
  fi
  mv $W/judge_results.jsonl.tmp $W/judge_results.jsonl
  unload $DS
  hb "$N: judging finished, $DS unloaded ($bad unscored judgments)"
fi

# 4. the review file and the report (CPU)
.venv/bin/python scripts/eval2_judge.py flags --results $W/judge_results_main.jsonl --out $REVIEW || fail "review file failed"
.venv/bin/python scripts/eval2_pilot_report.py --v2 --results $W/judge_results.jsonl --title "Judge pilot 2" > $REPORT.tmp || fail "pilot report failed"
{ cat $REPORT.tmp; cat <<EOF

## Files and what waits

- Judge results: \`$W/judge_results.jsonl\` (answers: \`$W/ans_default.jsonl\`, \`$W/ans_strict.jsonl\`; judge cache \`$W/judge_cache.jsonl\`).
- Review file of candidate errors, one row per candidate, for the reviewer to fill in \`confirmed\` (yes, harmful, no, unclear) and \`note\`: \`$REVIEW\`.
- The reviewer's findings per answer (JSONL; format in the docstring of \`scripts/eval2_pilot_report.py\`), written blind to the configuration.
- When both are in: \`.venv/bin/python scripts/eval2_pilot_report.py --v2 --results $W/judge_results.jsonl --review <filled review file> --findings <findings file>\`
  decides criteria 4 and 5 and gives the ordering; \`.venv/bin/python scripts/eval2_judge.py confirmed --review <filled review file> --results $W/judge_results_main.jsonl\`
  gives confirmed wrong and harmful items per 10 answers per mode.
EOF
} > $REPORT.tmp2 && mv $REPORT.tmp2 $REPORT && rm -f $REPORT.tmp
if [ $DRY = yes ]; then
  sed -n '1,12p;/^## Result/,$p' $REPORT | cut -c1-220
  echo "dry run done: report from stub data in $REPORT, review file $REVIEW ($(rows $REVIEW) rows)"
  exit 0
fi
RES=$(grep -m1 'Go or no-go' $REPORT | sed 's/\*//g' | cut -c1-300)
hb "$N: done; $RES (runs/m4/S5B_JUDGE_PILOT2.md; criteria 4, 5 and the ordering wait for the reviewer)" yes
status DONE yes "$N done: runs/m4/S5B_JUDGE_PILOT2.md; the review file $REVIEW waits for the reviewer"
echo M4_S5B_JUDGE_PILOT2_DONE >> $L/$N.done
