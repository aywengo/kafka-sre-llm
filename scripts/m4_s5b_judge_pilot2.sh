#!/bin/sh
# M4 S5b: second pilot of the judge step, version 2 (runs/m4/reports/s5_pilot_analysis.md, "Second pilot";
# runs/m4/reports/s5b_judge_v2.md). 24 fresh tier B items, answered in default and strict mode on the reference configuration
# (the 4-bit base model on the server, thinking off, frozen index, catalogues given, one request at a time): 48 answers; judged in two
# passes by DeepSeek without thinking (settings name deepseek-nothink-12k), instruction v2, three calls in parallel. Unattended; it
# takes the model server, no local GPU.
# Inputs (written by someone else, the chain refuses to start without them):
#   eval/m4/pilot2/items.jsonl     24 rows {"id", "tier", "kind", "family", "domain", "question", "evidence" (string or null), ...}
#   eval/m4/pilot2/rubrics.jsonl   24 rows (id = the item's id): must, must_not (with condition and near_miss), any_of, covered_chunks,
#                                  reference, correct_alternative, generic, weights_status
#   The chain refuses to start (exit 2) while a rubric's weights_status says the weights are only proposed, unless started with
#   --proposed-weights (the conductor changes that field when the second reviewer's corrections are in).
# Steps:
#   0. CPU: inputs and weights_status checked, probe answer sets built from the rubrics (eval2_judge.py gen-probes), planned judge
#      calls and time printed; then waits until none of our chains runs (m4_s6_noise_4bit.sh, m4_s5a_judge_settings.sh, m4_s5_judge_pilot.sh);
#   1. server: idle, our judge models (DeepSeek, GLM builds) unloaded, no model of another client loaded; the 4-bit model's name comes
#      from GET /v1/models (override: M4_Q4_MODEL);
#   2. ALL answering first, with the assistant model only: default, then strict (scripts/eval2.py answer, resumable, guarded, stall limit
#      40 min); 24 non-empty rows each, with a pack. The item's evidence goes to the assistant under a line "Evidence:" (the harness does it);
#   3. unloads the 4-bit model, so that the judge is never loaded together with it; then ALL judging with the judge only (scripts/eval2_judge.py
#      run --instruction v2, guarded, stall limit 60 min): the 48 answers in passes 1 and 2; the four probe sets in pass 1 (generic,
#      correct_alternative, reference, near-miss: 118 answers); with --reference-arm also pass 1 on the 48 answers with the rubric's
#      reference answer added (sets default_ref and strict_ref); one more round for replies that were not valid JSON; unloads the judge;
#   4. CPU: the review file of candidate errors (runs/m4/S5B_REVIEW_FLAGS.jsonl) and the report runs/m4/S5B_JUDGE_PILOT2.md
#      (criteria 1 to 3 are decided; 4, 5 and the ordering wait for the reviewer's review file and findings file; the Owner decides;
#      sections "Rubric probes" and "Sources of the must points" are not criteria).
# Before every unload the server must have no active or waiting request. Outputs are new files under work/m4/pilot2/.
# Markers: runs/m4/logs/m4_s5b_judge_pilot2.done or .failed.
#   scripts/m4_s5b_judge_pilot2.sh [--proposed-weights] [--reference-arm] > runs/m4/logs/m4_s5b_judge_pilot2.out 2>&1 &
#   scripts/m4_s5b_judge_pilot2.sh --dry-run [--reference-arm]   # the REAL items and rubrics, stub generator and stub judge, no server,
#                                                                 # no markers, no heartbeat, nothing written outside a temporary directory
# Running time: answering 48 x 47 to 71 s = 0.6 to 1.0 h; judging 214 calls (262 with the reference arm) at 22 s a call, 3 in parallel:
# 0.4 to 0.5 h; loads and checks 0.3 h. About 1.3 to 1.8 h.
set -e
DRY=no; REFARM=no; PROPOSED=no
for arg in "$@"; do case $arg in
  --dry-run) DRY=yes;; --reference-arm) REFARM=yes;; --proposed-weights) PROPOSED=yes;;
  *) echo "usage: scripts/m4_s5b_judge_pilot2.sh [--dry-run] [--reference-arm] [--proposed-weights]" >&2; exit 2;; esac; done
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
  if [ $PROPOSED = no ]; then
    .venv/bin/python - $RUBS <<'PY' || { echo "REFUSED: the rubrics' weights are only proposed (weights_status); start with --proposed-weights to run anyway; nothing started" >&2; exit 2; }
import json, sys
bad = [r["id"] for r in (json.loads(l) for l in open(sys.argv[1]) if l.strip()) if "propos" in str(r.get("weights_status", "")).lower()]
if bad:
    print(f"weights_status says proposed in {len(bad)} rubrics (first: {bad[0]})", file=sys.stderr)
    sys.exit(1)
PY
  fi
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
else
  W=$(mktemp -d "${TMPDIR:-/tmp}/m4_s5b_dry.XXXXXX"); L=$W/logs; mkdir -p $L; REPORT=$W/report_from_stub_data.md; REVIEW=$W/review_flags.jsonl
  G="scripts/run_guarded.py --other-retries 0"
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
# one judge run: $1 = rubrics file, $2 = answer sets (NAME=PATH ...), the rest = further arguments. Dry run: stub judge, no server.
jrun() { rubf=$1; sets=$2; shift 2
  if [ $DRY = yes ]; then
    .venv/bin/python scripts/eval2_judge.py run --rubrics $rubf --items $ITEMS --chunks $CHUNKS --source-chars $SRC --instruction v2 \
      --answers $sets --judge-model $DS --judge-extra-body "$JEB" --settings-name $SETTING --concurrency 3 --stub "$@"
  else
    $G --stall-min 60 --log $L/$N.judge.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $rubf --items $ITEMS \
      --chunks $CHUNKS --source-chars $SRC --instruction v2 --answers $sets --judge-model $DS --judge-extra-body "$JEB" \
      --settings-name $SETTING --concurrency 3 --judge-url "$OMLX_URL/v1" "$@"
  fi; }
UNSCORED=0
phase() { # NAME RUBRICS SETS EXPECTED_ROWS [run arguments]: up to two rounds (the second redoes replies that were not valid JSON)
  pn=$1; pr=$2; ps=$3; pexp=$4; shift 4; out=$W/judge_results_$pn.jsonl
  for round in 1 2; do
    extra=""; [ $round = 2 ] && extra="--retry-invalid"
    if [ $DRY = yes ]; then
      jrun $pr "$ps" "$@" --cache $W/judge_cache.jsonl --out $out $extra > $W/judge.$pn.dry.out 2>&1 || { cat $W/judge.$pn.dry.out; fail "judge dry run $pn failed"; }
      grep -E '^(matched|instruction|items)' $W/judge.$pn.dry.out
    else
      jrun $pr "$ps" "$@" --cache $W/judge_cache.jsonl --out $out $extra || fail "judging $pn round $round failed (see $L/$N.judge.log)"
    fi
    bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $out)
    echo "$pn round $round: $bad unscored"
    [ "$bad" = "0" ] && break
  done
  UNSCORED=$((UNSCORED + bad))
  [ "$(rows $out)" = "$pexp" ] || fail "$out has $(rows $out) rows, not $pexp"; }

mkdir -p $W $L runs/m4/reports
[ $DRY = yes ] || rm -f $L/$N.failed $L/$N.done

# the real items and rubrics, in the dry run too (the dry run replaces only the generator and the judge by stubs)
ITEMS=$ITEMS_IN; JRUBS=$RUBS
if [ $DRY = yes ]; then
  for f in $ITEMS $JRUBS $CHUNKS; do [ -s $f ] || { echo "DRY RUN FAILED: input $f is missing or empty"; exit 1; }; done
fi
HITEMS=$W/items_harness.jsonl
.venv/bin/python - $ITEMS $HITEMS <<'PY' || fail "could not build the harness items file"
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
with open(sys.argv[2], "w") as f:
    for r in rows:
        if not r.get("id") or not r.get("question"):
            sys.exit("item without id or question")
        # the harness wants strings for tier, kind, family and domain; the pilot's domain is a list: joined, the list kept as "domains"
        row = {"tier": "B", "kind": "open", "family": r["id"], "domain": "pilot2", **r}
        if isinstance(row["domain"], list):
            row["domains"], row["domain"] = row["domain"], ",".join(map(str, row["domain"])) or "pilot2"
        for k in ("tier", "kind", "family", "domain"):
            if not isinstance(row[k], str) or not row[k]:
                sys.exit(f"item {r['id']}: {k} is not a non-empty string")
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
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

# probe answer sets from the rubrics (generic, correct_alternative, reference, near-miss), and the plan
PD=$W/probes; PRUBS=$PD/probe_rubrics.jsonl
.venv/bin/python scripts/eval2_judge.py gen-probes --rubrics $JRUBS --items $ITEMS --out-dir $PD || fail "probe sets could not be built from the rubrics"
PSETS="probe_generic=$PD/probe_generic.jsonl probe_alt=$PD/probe_alt.jsonl probe_ref=$PD/probe_ref.jsonl probe_nearmiss=$PD/probe_nearmiss.jsonl"
NPROBE=$(cat $PD/probe_generic.jsonl $PD/probe_alt.jsonl $PD/probe_ref.jsonl $PD/probe_nearmiss.jsonl | wc -l | tr -d ' ')
CALLS_MAIN=$((NEED * 2 * 2)); CALLS_REF=0; [ $REFARM = yes ] && CALLS_REF=$((NEED * 2))
CALLS=$((CALLS_MAIN + NPROBE + CALLS_REF))
echo "planned: $((NEED * 2)) answers ($NEED items x default, strict); judge calls $CALLS = $CALLS_MAIN (48 answers x 2 passes) + $NPROBE (probe sets, 1 pass) + $CALLS_REF (reference arm, 1 pass); at 22 s a call, 3 in parallel: about $((CALLS * 22 / 3 / 60)) minutes"

if [ $DRY = yes ]; then
  echo "dry run in $W (real items and rubrics, stub generator, stub judge)"
  Q4=Qwen3.8-27B-4bit
else
  hb "$N: waiting until none of m4_s6_noise_4bit.sh, m4_s5a_judge_settings.sh, m4_s5_judge_pilot.sh runs, then the server check"
  status RUNNING no "$N waiting for our other chains to end; markers $L/$N.done / .failed"
  until ! pgrep -f "$CHAINS_RE" >/dev/null; do sleep 60; done
  api /v1/models > $W/models.json 2>/dev/null || true
  Q4=${M4_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $W/models.json)} || fail "4-bit model not listed by the server (or ambiguous)"
  hb "$N: started; 4-bit model $Q4; $NEED items in default and strict mode, then judge $DS (instruction v2, settings $SETTING; $CALLS judge calls planned)$([ $REFARM = yes ] && echo ', with the reference arm')$([ $PROPOSED = yes ] && echo '; WEIGHTS ONLY PROPOSED (--proposed-weights)')"
  status RUNNING no "$N running: answers (48), then judging ($CALLS calls); markers $L/$N.done / .failed"
  n=0
  until [ "$(busy)" = "0" ] && [ -z "$(foreign)" ]; do
    sleep 30; n=$((n+1))
    [ $n -lt 120 ] || fail "server not free after 60 min (active or waiting requests, or a model of another client loaded: $(foreign))"
  done
  unload $DS; unload $GLM4; unload $GLM8
  hb "$N: DeepSeek and the GLM builds unloaded if they were loaded; the 4-bit model loads on the first request"
fi

# 2. answers: everything the assistant model has to do comes first
answer_run default default
answer_run strict strict
SETS="default=$W/ans_default.jsonl strict=$W/ans_strict.jsonl"
SETS_REF="default_ref=$W/ans_default.jsonl strict_ref=$W/ans_strict.jsonl"
[ $DRY = yes ] || { unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; }

# 3. judging: the judge only; the 48 answers in two passes, the probe sets and the optional reference arm in one pass
phase main $JRUBS "$SETS" $((NEED * 4)) --passes 1 2
cp $W/judge_results_main.jsonl $W/judge_results.jsonl.tmp
phase probes $PRUBS "$PSETS" $NPROBE --passes 1
if [ $REFARM = yes ]; then
  phase ref $JRUBS "$SETS_REF" $((NEED * 2)) --passes 1 --with-reference
  cat $W/judge_results_ref.jsonl >> $W/judge_results.jsonl.tmp
fi
mv $W/judge_results.jsonl.tmp $W/judge_results.jsonl
[ $DRY = yes ] || { unload $DS; hb "$N: judging finished, $DS unloaded ($UNSCORED unscored judgments)"; }

# 4. the review file and the report (CPU)
.venv/bin/python scripts/eval2_judge.py flags --results $W/judge_results_main.jsonl --out $REVIEW || fail "review file failed"
.venv/bin/python scripts/eval2_pilot_report.py --v2 --results $W/judge_results.jsonl --rubrics $JRUBS --chunks $CHUNKS \
  --probes $W/judge_results_probes.jsonl --title "Judge pilot 2" > $REPORT.tmp || fail "pilot report failed"
{ cat $REPORT.tmp; cat <<EOF

## Files and what waits

- Judge results: \`$W/judge_results.jsonl\`, probe sets \`$W/judge_results_probes.jsonl\` (answers: \`$W/ans_default.jsonl\`, \`$W/ans_strict.jsonl\`; judge cache \`$W/judge_cache.jsonl\`).
- Review file of candidate errors, one row per candidate, for the reviewer to fill in \`confirmed\` (yes, harmful, no, unclear) and \`note\`: \`$REVIEW\`.
- The reviewer's findings per answer (JSONL; format in the docstring of \`scripts/eval2_pilot_report.py\`), written blind to the configuration.
- When both are in: \`.venv/bin/python scripts/eval2_pilot_report.py --v2 --results $W/judge_results.jsonl --rubrics $JRUBS --chunks $CHUNKS --probes $W/judge_results_probes.jsonl --review <filled review file> --findings <findings file>\`
  decides criteria 4 and 5 and gives the ordering; \`.venv/bin/python scripts/eval2_judge.py confirmed --review <filled review file> --results $W/judge_results_main.jsonl\`
  gives confirmed wrong and harmful items per 10 answers per mode.
EOF
} > $REPORT.tmp2 && mv $REPORT.tmp2 $REPORT && rm -f $REPORT.tmp
if [ $DRY = yes ]; then
  sed -n '1,12p;/^## Result/,$p' $REPORT | grep -v '^  - not met' | cut -c1-260
  echo "dry run done: report from stub data in $REPORT, review file $REVIEW ($(rows $REVIEW) rows)"
  exit 0
fi
RES=$(grep -m1 'Go or no-go' $REPORT | sed 's/\*//g' | cut -c1-300)
hb "$N: done; $RES (runs/m4/S5B_JUDGE_PILOT2.md; criteria 4, 5 and the ordering wait for the reviewer)" yes
status DONE yes "$N done: runs/m4/S5B_JUDGE_PILOT2.md; the review file $REVIEW waits for the reviewer"
echo M4_S5B_JUDGE_PILOT2_DONE >> $L/$N.done
