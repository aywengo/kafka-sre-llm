#!/bin/sh
# M6 DA-chain: the D5 answers chain (MILESTONE.md, task DA; agents/m6-plan.md). Built and rehearsed (dry run) now; the real run is a separate task.
# The question: does a grounded prompt (grounded-g2 or grounded-g3) on the packs of the better-ranked retrieval setting (r4g2, which the D5 funnel let go on and
# the B4 check confirmed) hold its coverage against today's default on the frozen packs? The chain answers the 50 D5 scenarios and the 116 mechanical items of
# the development sample, judges the D5 answers, and applies a rule fixed beforehand by code (scripts/m6_d5_rule.py): the choice is CHOICE: grounded-g2,
# CHOICE: grounded-g3 or CHOICE: neither (the combination stops and the Owner sees it). The rule's text is written into the report header before any answer exists.
# Reference configuration as in the development check (scripts/m5_g_dev.sh): the 4-bit base model on the server, thinking off, the frozen index, the 2,048-token
# limit; the judge, its setting, instruction v2 and two passes are the development chain's. Nothing here starts a model by itself: the answers are server calls
# (one request at a time) under scripts/run_guarded.py; the packs of the mechanical sample load the frozen reranker on the CPU under the guard.
#
# Steps (each resumable by its output files; a stage marker W/stage_NAME.done records the manifest hash and the sha256 of every output; a line per item):
#   1. Gate (CPU, refusal exit 2, nothing written before it passes except the work folder): the inputs; runs/m6/RETRIEVAL_CHOSEN.json has a GO for a setting (and is not
#      a dry-run record in a real run); the D5 funnel's stage markers and the hashes of its outputs (packs, expansions, timing) are what the funnel recorded, its
#      model is the one on the server; the B4 check ran for the same setting, decided on the strict count and passed (runs/m6/B4_USED and the B4 manifest;
#      --without-b4 "REASON" skips that gate, for the conductor only, records the reason in the manifest and the report, and still refuses when runs/m6/B4_USED exists with a verdict that is not PASS); the D5 item and rubric hashes equal
#      eval/m6/d5/CHUNK_CHECK.json; kafka_sre_rag verify; the start check of a stored manifest and markers; then, in real runs, the server of m5_g_dev.sh (the 4-bit
#      model listed, no other chain or guarded job, no active or waiting request, no model of another client loaded; our judge builds are unloaded).
#   2. Report header runs/m6/D5_ANSWERS.md with the rule's text (written once, before any answer exists).
#   3. prepare + packs: harness items, the D5 packs in the replay format (frozen and the chosen setting, from the funnel's pack file); the chosen setting's packs for
#      the 116 mechanical items by the runtime path (retrieval.select, profile r4, the grid setting) from the stored search terms, the frozen reranker on the CPU under
#      the guard; every pack starts with the stored frozen pack.
#   3b. Retrieval timing (CPU, guarded, the server idle, no server call): the whole retrieval of the frozen setting and of the chosen setting, uncached, on the funnel's 20
#      timing questions from its stored expansions; condition (d) adds the median of that side's setting to each answer's seconds and the funnel's search-terms seconds.
#   The start check runs again before the timing, the reference, the variants, the judge and the measures, so that a change between stages stops the chain; the pinned code is
#      everything the answer, mech-packs, judge, scorer and identifier-check paths import (scripts/m6_d5_rule.py code-pins). The answer calls pass --allow-commit-change: an unrelated
#      commit during the run does not stop it (the pins carry the protection); every commit is listed in the answers' manifests.
#   4. Reference: default on the FROZEN packs, D5 (50) and the 116 mechanical items, answered in this run.
#   5. Variants: grounded-g2 and grounded-g3 on the chosen setting's packs, the same call except mode and packs. After EACH variant's answer sets the prompt check: every answer row carries the
#      hash of the mode's own prompt on its pack, different from the default prompt on it, and eval2.py compare-prompts shows a different prompt for every id; the packs
#      of every answer equal the pack files (checked after each run).
#   6. Judge (DeepSeek, coverage role, instruction v2, setting deepseek-nothink-12k, two passes) on the three D5 answer sets: 300 judge calls; --max-unjudged N as in
#      m5_g_dev.sh (default 0). 7. Identifier check (D5) and the scorer with decline outcomes (mechanical), CPU. 8. The rule by code. 9. Report, CANDIDATE_CHOSEN.json
#      (only when a mode is chosen), the done marker.
# A state left by a stopped run (work folder, manifest, report, failed marker) is refused by the start check; the refusal prints the mv lines.
#
#   scripts/m6_d5_answers.sh > runs/m6/logs/m6_d5_answers.out 2>&1 &        (real: after scripts/m6_d5_funnel.sh and scripts/m5_b4_check.sh --decide strict --setting r4g2)
#   scripts/m6_d5_answers.sh --dry-run --work DIR --funnel-work FDIR --chosen-record F --b4-used F --b4-work D     # stub model, stub judge, stand-in reranker, no server;
#       # nothing is written outside DIR (the dry run of the funnel and of the B4 check into temporary folders gives FDIR and the B4 files); the numbers are not measurements
# Options: --dry-run, --work DIR, --report FILE, --candidate-out FILE, --funnel-work DIR (default work/m6/d5_funnel), --funnel-used FILE, --chosen-record FILE, --b4-used FILE,
#   --b4-work DIR (default work/m5/b4), --without-b4 "REASON", --ids FILE, --items F --rubrics F --chunk-check F (dry runs: default is FDIR/in/), --max-unjudged N (default 0: a
#   judge row that stays invalid after the judge rounds stops the chain; up to N such rows are left out and named; an answer with no valid pass still stops the chain),
#   --accept-code-change "REASON" (the start check accepts changed hashes of scripts/m6_d5_answers.sh and scripts/m6_d5_rule.py and of nothing else, only when the
#   reference and answers stages are done; old and new hashes, the reason and the time go into the manifest and the report).
# Markers: runs/m6/logs/m6_d5_answers.done or .failed (a dry run: in its own DIR). The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/.
#
# Estimated duration of the real run (inferred from the development check's rates, not measured on D5 or on the grounded modes on the chosen packs): 3.6 to 5.4 h of
# model-server time (see "planned:" below), plus about 20 to 40 min of CPU for the 116 mechanical packs (reranker) before the first answer.
set -e
DRY=no; WSET=no; W=work/m6/d5_answers; REPORT=runs/m6/D5_ANSWERS.md; RSET=no; CHOSENOUT=runs/m6/CANDIDATE_CHOSEN.json; COSET=no
FW=work/m6/d5_funnel; REC=runs/m6/RETRIEVAL_CHOSEN.json; USED=runs/m6/D5_FUNNEL_USED; B4USED=runs/m6/B4_USED; B4W=work/m5/b4; NOB4=; MAXU=0; ACCEPT=
IDS=eval/m4/g_dev_ids.json; ITEMS=eval/m6/d5/items.jsonl; RUBS=eval/m6/d5/rubrics.jsonl; CC=eval/m6/d5/CHUNK_CHECK.json; INSET=no; FWSET=no; RECSET=no; USEDSET=no; B4SET=no
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;; --candidate-out) CHOSENOUT=${2:?}; COSET=yes; shift;;
  --funnel-work) FW=${2:?}; FWSET=yes; shift;; --funnel-used) USED=${2:?}; USEDSET=yes; shift;; --chosen-record) REC=${2:?}; RECSET=yes; shift;;
  --b4-used) B4USED=${2:?}; B4SET=yes; shift;; --b4-work) B4W=${2:?}; B4SET=yes; shift;; --without-b4) NOB4=${2:?}; shift;;
  --ids) IDS=${2:?}; shift;; --items) ITEMS=${2:?}; INSET=yes; shift;; --rubrics) RUBS=${2:?}; INSET=yes; shift;; --chunk-check) CC=${2:?}; INSET=yes; shift;;
  --max-unjudged) MAXU=${2:?}; shift;; --accept-code-change) ACCEPT=${2:?}; shift;;
  *) echo "usage: scripts/m6_d5_answers.sh [--dry-run] [--work DIR] [--report FILE] [--candidate-out FILE] [--funnel-work DIR] [--funnel-used FILE] [--chosen-record FILE] [--b4-used FILE] [--b4-work DIR] [--without-b4 REASON] [--ids FILE] [--max-unjudged N] [--accept-code-change REASON] (see the header)" >&2; exit 2;; esac
  shift; done
case $MAXU in ''|*[!0-9]*) echo "REFUSED: --max-unjudged is a whole number (0 or more); nothing started" >&2; exit 2;; esac
R=scripts/m6_d5_rule.py; GR=scripts/m5_g_rule.py; N=m6_d5_answers; L=runs/m6/logs
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json
TA_ITEMS=eval/m4/open/tier_a.jsonl; TA3_ITEMS=eval/m4/open/tier_a3.jsonl; TA_ANS=work/m4/s6/ans_A1.jsonl; TA3_ANS=work/m4/e2/ans_O_Q4D_a3.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
JSETTING=deepseek-nothink-12k; SRC=12000
VARS="g2 g3"
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
if [ $DRY = yes ]; then
  [ $FWSET = yes ] || refuse "a dry run needs --funnel-work (the output of a dry run of scripts/m6_d5_funnel.sh)"
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m6_d5_answers.XXXXXX")
  mkdir -p $W
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/D5_ANSWERS_from_stub_data.md; [ $COSET = yes ] || CHOSENOUT=$W/CANDIDATE_CHOSEN.json
  [ $RECSET = yes ] || REC=$FW/RETRIEVAL_CHOSEN.json; [ $USEDSET = yes ] || USED=$FW/D5_FUNNEL_USED
  case "$REPORT $CHOSENOUT $REC $USED $FW $([ $B4SET = yes ] && echo "$B4USED $B4W")" in *runs/m6/D5_ANSWERS.md*|*runs/m6/CANDIDATE_CHOSEN.json*|*runs/m6/RETRIEVAL_CHOSEN.json*|*runs/m6/D5_FUNNEL_USED*|*runs/m6/B4_USED*|*work/m6/d5_funnel*|*work/m5/b4*)
    refuse "a dry run never uses the real report, record, markers or funnel and B4 work folders";; esac
  if [ $INSET = no ]; then ITEMS=$FW/in/items.jsonl; RUBS=$FW/in/rubrics.jsonl; CC=$FW/in/CHUNK_CHECK.json
  else case "$ITEMS $RUBS $CC" in *eval/m6/d5*) refuse "a dry run never uses the real D5 files";; esac; fi
  [ -n "$NOB4" ] || [ $B4SET = yes ] || refuse "a dry run needs --b4-used and --b4-work (a dry run of scripts/m5_b4_check.sh), or --without-b4 REASON"
  G="scripts/run_guarded.py --other-retries 0"; STANDIN="--stand-in"
else
  [ $INSET = no ] || refuse "--items, --rubrics and --chunk-check are for dry runs only (the real run reads eval/m6/d5)"
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"; STANDIN=""
fi
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M6 DA: the D5 answers chain (grounded prompt on the better-ranked packs)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
STOPPED_SUFFIX=stopped_$(date +%Y%m%d)
moveaside() { # the lines that move the state of a stopped run out of the way (printed, never executed here)
  echo "to start again, first move the stopped run's files aside (the chain never moves or deletes them):" >&2
  for f in $W $REPORT $CHOSENOUT $L/$N.failed; do [ -e $f ] && echo "  mv $f $f.$STOPPED_SUFFIX" >&2; done
  echo "  (and runs/m6/logs/$N.out, if it holds the stopped run's output, before the next start redirects into it)" >&2; return 0; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- 1. the gate (CPU; nothing is written before the checks pass, except $W itself)
for f in $ITEMS $RUBS $CC $REC $FW/DF_MANIFEST.json $FW/packs.jsonl $FW/expansions.jsonl $FW/served.json $IDS $TA_ITEMS $TA3_ITEMS $TA_ANS $TA_ANS.manifest.json $TA3_ANS $TA3_ANS.manifest.json \
         $CHUNKS $FROZEN $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json kafka_sre_rag/candidate.json kafka_sre_rag/prompts.py kafka_sre_rag/retrieval.py kafka_sre_rag/profiles.py \
         kafka_sre_rag/evalhooks.py scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_score.py scripts/eval2_compare.py scripts/run_guarded.py \
         scripts/m5_g_rule.py scripts/m5_b4_served.py scripts/m6_d5_funnel.py scripts/m6_d5_answers.sh $R; do
  [ -s $f ] || refuse "input $f is missing or empty (a missing funnel file means the D5 funnel has not run: scripts/m6_d5_funnel.sh)"
done
if [ $DRY = yes ]; then DRYFLAG="--dry"; else DRYFLAG=""; fi
SETTING=$(.venv/bin/python $R chosen-setting --record $REC) || refuse "$REC is unreadable or names no setting (run scripts/m6_d5_funnel.sh first)"
.venv/bin/python $R gate-chosen --record $REC $DRYFLAG || refuse "the record $REC is not usable: no go for $SETTING (verdict STOP, parameters differ from candidate.json, or a dry-run record in a real run)"
.venv/bin/python scripts/m5_b4_served.py gate-files --items $ITEMS --rubrics $RUBS --chunk-check $CC || refuse "the D5 files differ from the hashes in the chunk check"
.venv/bin/python $R gate-funnel --funnel-work $FW --items $ITEMS --rubrics $RUBS --record $REC --used $USED --setting $SETTING $DRYFLAG \
  || refuse "the D5 funnel's files are not what the funnel recorded (a pack file with another hash, a missing stage marker or marker file, or a pack that does not start with the frozen pack)"
.venv/bin/python $R gate-first-call || refuse "the stored default runs (B1, B2) for the timing cross-check are missing"
if [ -n "$NOB4" ] && { [ $DRY = no ] || [ $B4SET = yes ]; }; then      # --without-b4 does not override a B4 check that ran and did not pass
  .venv/bin/python $R gate-b4-override --used $B4USED || refuse "$B4USED exists and its verdict is not PASS: --without-b4 does not override a B4 check that did not pass"
fi
if [ -n "$NOB4" ]; then
  echo "gate: the B4 gate is SKIPPED by --without-b4; reason recorded in the manifest and the report: $NOB4"
else
  .venv/bin/python $R gate-b4 --used $B4USED --b4-work $B4W --record $REC --setting $SETTING $DRYFLAG \
    || refuse "the B4 check has not been run for $SETTING and passed (scripts/m5_b4_check.sh --decide strict --setting $SETTING; $B4USED must say verdict PASS)"
fi
VOUT=$(mktemp "${TMPDIR:-/tmp}/m6_da_verify.XXXXXX")     # nothing is written into the repository before the gate has passed
.venv/bin/python -m kafka_sre_rag verify > $VOUT 2>&1 || { cat $VOUT >&2; refuse "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"; }
grep -E '^prompt ' $VOUT | head -8
.venv/bin/python $GR draw-ids --tier-a $TA_ITEMS --tier-a3 $TA3_ITEMS --check $IDS || refuse "$IDS does not reproduce from its rule"
for s in "$TA_ANS 251" "$TA3_ANS 88"; do
  set -- $s; .venv/bin/python $GR check-answers --file $1 --expect $2 --mode default || refuse "the stored default answers $1 are incomplete, have errors or another mode"
done
FUNNEL_MODEL=$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["options"]["model"])' $FW/DF_MANIFEST.json)

# the server (real runs): the helpers of scripts/m5_g_dev.sh
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  export JUDGE_API_KEY=$OMLX_API_KEY
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
fi
api() { curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep 30; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
loaded_list() { api /api/status | python3 -c 'import sys,json; print(" ".join(json.load(sys.stdin).get("loaded_models") or []))' 2>/dev/null || echo "status-unreadable"; }
unload_now() { # any model; only when no request is active or waiting (the Owner's decision of 2026-11-22); the server's answer is checked and logged
  idle
  uresp=$(curl -s -m 300 -w '\n%{http_code}' -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" 2>/dev/null) || uresp=""
  ucode=$(printf '%s' "$uresp" | tail -n 1); ubody=$(printf '%s' "$uresp" | sed '$d' | tr '\n' ' ' | cut -c1-160 | sed 's/ *$//')
  case $ucode in [0-9][0-9][0-9]) ;; *) ucode=000;; esac
  case $ucode in
    2??) hb "$N: unloaded $1 (no active or waiting requests; the server answered $ucode: ${ubody:-no body})";;
    *) hb "$N: unload of $1 FAILED (the server answered $ucode: ${ubody:-no body})" yes; fail "the unload of $1 was not accepted (the server answered $ucode)";;
  esac; }
unload() { [ "$(loaded $1)" = "yes" ] || return 0; unload_now $1; }
clear_server() { # before the first request: no active or waiting request; every loaded model but the 4-bit model is unloaded (each unload checked and logged)
  idle
  for m in $(loaded_list); do
    [ "$m" = "$Q4" ] && continue
    [ "$m" = "status-unreadable" ] && fail "the server status is unreadable"
    unload_now $m
  done
  left=$(for m in $(loaded_list); do [ "$m" = "$Q4" ] || echo $m; done)
  [ -z "$left" ] || fail "models are still loaded after the unloads: $left"; }
others() { pgrep -fl 'scripts/(m4|m5|m6|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }
wait_others() { # before anything loads a reranker or a model: no other chain and no guarded job (real runs)
  [ $DRY = yes ] && return 0
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, s1_*.sh) and no guarded job runs"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done; }
if [ $DRY = yes ]; then
  Q4=Qwen3.8-27B-4bit
else
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m6_da_models.XXXXXX")
  api /v1/models > $MJSON 2>/dev/null || true
  Q4=${M6_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $MJSON)} || refuse "the 4-bit model is not listed by the server (or ambiguous)"
  [ "$Q4" = "$FUNNEL_MODEL" ] || refuse "the 4-bit model on the server ($Q4) is not the one the D5 funnel's expansions were made with ($FUNNEL_MODEL)"
fi
[ $DRY = no ] || [ "$Q4" = "$FUNNEL_MODEL" ] || echo "dry run: the funnel's dry manifest names $FUNNEL_MODEL"

# the gate has passed: from here on the work folder and the log folder are written
mkdir -p $W $L
# the manifest and the start check
PINS="--pin $ITEMS --pin $RUBS --pin $CC --pin $REC --pin $FW/DF_MANIFEST.json --pin $FW/packs.jsonl --pin $FW/expansions.jsonl --pin $FW/served.json --pin $IDS \
  --pin $TA_ITEMS --pin $TA3_ITEMS --pin $TA_ANS --pin $TA_ANS.manifest.json --pin $TA3_ANS --pin $TA3_ANS.manifest.json --pin $CATS/CATALOGUES.json --pin runs/v3/INDEX_FROZEN.json --pin work/m4/b1/ans_default.jsonl --pin work/m4/b2/ans_default.jsonl"
[ -n "$NOB4" ] || PINS="$PINS --pin $B4USED --pin $B4W/B4_MANIFEST.json"
for f in $(.venv/bin/python $R code-pins) $FROZEN; do     # the code the answer, mech-packs, judge, scorer and identifier-check paths import (the tests trace the imports)
  [ -f $f ] || refuse "pinned code file $f is missing"
  PINS="$PINS --pin $f"; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)   # recorded for the reader, never compared
gstate() { # SUBCOMMAND [EXTRA...]: the manifest and the start check take the same state
  sub=$1; shift
  .venv/bin/python $R "$sub" --manifest $W/MANIFEST.json --work $W $PINS --frozen $FROZEN $DRYFLAG --opt "setting=$SETTING" --opt "model=$Q4" --opt "without_b4=$NOB4" --opt "mechanical=sample" \
    --info "git_head=$GIT_HEAD" --info "max_unjudged=$MAXU" --judge-arg "model=$DS" --judge-arg "setting=$JSETTING" --judge-arg "extra_body=$JEB" --judge-arg "source_chars=$SRC" \
    --judge-arg "instruction=v2" --judge-arg "passes=1 2" --judge-arg "role=coverage" "$@"; }
if [ -n "$ACCEPT" ]; then gstate start-check --accept-code-change "$ACCEPT" || { moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; }
else gstate start-check || { moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; }; fi
gstate manifest
mkdir -p "$(dirname $REPORT)"
recheck() { # NAME: the start check again before a stage, so that a change of a pinned file, an option or an output between stages stops the chain
  gstate start-check > $W/recheck.out 2>&1 || { cat $W/recheck.out >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started (see above)"; }; }

# ---------------------------------------------------------------- the plan and the estimate
ND=$(grep -c . $ITEMS | tr -d ' ')
est() { awk -v d=$ND 'BEGIN{ lo=d*35+116*10 + 2*(d*35+116*10) + 3*d*2*10 + 1200; hi=d*85+116*13 + 2*(d*60+116*13) + 3*d*2*10 + 1800; printf "%.1f to %.1f", lo/3600, hi/3600 }'; }
echo "planned: $ND D5 scenarios and 116 mechanical items answered in 3 modes (default on the frozen packs; grounded-g2 and grounded-g3 on the $SETTING packs), $((ND * 6)) judge calls: $(est) h of model-server time; plus about 20 to 40 min of CPU for the mechanical packs"
echo "estimate basis: the development check's rates (35 to 85 s per judged answer, 10 to 13 s per mechanical answer, about 10 s per judge call, loads 0.3 to 0.5 h); not measured on D5 or on the grounded modes on the chosen packs"

stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $GR mark --work $W --stage $st --out "$@" > /dev/null; }
ANS_FILES=""; REF_FILES=""
for s in d5 ma m3; do REF_FILES="$REF_FILES $W/ans_${s}_default.jsonl"; for v in $VARS; do ANS_FILES="$ANS_FILES $W/ans_${s}_$v.jsonl"; done; done
SERVER_NEEDED=no
for st in rettime reference answers judge; do stage_done $st || SERVER_NEEDED=yes; done

# ---------------------------------------------------------------- 2. report header (before any answer exists)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $ANS_FILES $REF_FILES; do [ ! -e $f ] || refuse "$f exists but the report header was not written yet; the rule text must come first"; done
  [ $DRY = yes ] || [ ! -e $REPORT ] || { moveaside; refuse "$REPORT exists; it is not overwritten before the header of this run is written (move it away first)"; }
  {
    echo "# M6 D5 answers: grounded prompt on the $SETTING packs against the default on the frozen packs$([ $DRY = yes ] && echo ' (DRY RUN: stub model, stub judge, stand-in reranker; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/$N.sh\` on $(date +%Y-%m-%d), 4-bit model \`$Q4\`, retrieval setting \`$SETTING\` (record \`$REC\`: $(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["verdict_line"])' $REC)). Manifest hash $(.venv/bin/python -c 'import hashlib,json,sys; print(hashlib.sha256(json.dumps({k: v for k, v in json.load(open(sys.argv[1])).items() if k != "accepted_code_changes"}, sort_keys=True).encode()).hexdigest()[:16])' $W/MANIFEST.json). Ids of the mechanical sample: \`$IDS\`. Rule: \`$R\`."
    echo
    if [ -n "$NOB4" ]; then
      echo "**The B4 gate was skipped (\`--without-b4\`), for the conductor only. Reason recorded: $NOB4**"
    else
      echo "B4 gate: \`$B4USED\` says $(grep '^verdict' $B4USED | tr -d '\n') on the $(grep '^decide' $B4USED | cut -d' ' -f2) count, for $SETTING."
    fi
    echo
    .venv/bin/python $R rule-text
    echo
    echo "This header, with the rule, was written before any answer of the run existed (stage marker \`$W/stage_header.done\`)."
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header with the rule text written ($REPORT); no answer exists yet"
fi

# ---------------------------------------------------------------- prepare: items, the D5 packs (CPU)
if ! stage_done prepare; then
  .venv/bin/python $GR harness-items --in $ITEMS --out $W/items_d5_h.jsonl --domain d5 || fail "harness items of D5"
  .venv/bin/python $GR select --ids $IDS --mechanical sample --tier-a $TA_ITEMS --tier-a3 $TA3_ITEMS --ref-a $TA_ANS --ref-a3 $TA3_ANS --out-dir $W || fail "mechanical item files"
  .venv/bin/python $R d5-packs --funnel-work $FW --items $ITEMS --setting $SETTING --out-frozen $W/packs_frozen_d5.jsonl --out-chosen $W/packs_chosen_d5.jsonl || fail "the D5 pack files"
  mark prepare $W/items_d5_h.jsonl $W/items_ma.jsonl $W/items_m3.jsonl $W/ref_ma.jsonl $W/ref_m3.jsonl $W/packs_frozen_d5.jsonl $W/packs_chosen_d5.jsonl
fi
NMA=$(grep -c . $W/items_ma.jsonl | tr -d ' '); NM3=$(grep -c . $W/items_m3.jsonl | tr -d ' ')
[ "$NMA" = 96 ] && [ "$NM3" = 20 ] || fail "the mechanical sample has $NMA + $NM3 items; 96 + 20 expected"
echo "sets: D5 $ND, tier_a part $NMA, tier_a3 part $NM3"
[ $DRY = yes ] || [ "$ND" = 50 ] || fail "D5 has $ND scenarios; 50 expected"

# ---------------------------------------------------------------- 3. packs of the chosen setting for the mechanical sample (CPU, the frozen reranker, guarded)
if ! stage_done mechpacks; then
  wait_others
  hb "$N: packs of $SETTING for the mechanical sample started (frozen reranker on the CPU)"
  $G --stall-min 30 --log $L/$N.mechpacks_ma.log -- .venv/bin/python $R mech-packs --items $W/items_ma.jsonl --stored $W/ref_ma.jsonl --out $W/packs_chosen_ma.jsonl --setting $SETTING $STANDIN \
    || fail "the mechanical packs (tier_a part) failed (see $L/$N.mechpacks_ma.log; a pack that does not start with the stored frozen pack stops the chain)"
  $G --stall-min 30 --log $L/$N.mechpacks_m3.log -- .venv/bin/python $R mech-packs --items $W/items_m3.jsonl --stored $W/ref_m3.jsonl --out $W/packs_chosen_m3.jsonl --setting $SETTING $STANDIN \
    || fail "the mechanical packs (tier_a3 part) failed (see $L/$N.mechpacks_m3.log)"
  mark mechpacks $W/packs_chosen_ma.jsonl $W/packs_chosen_m3.jsonl
  hb "$N: packs of $SETTING for the mechanical sample done"
fi

# ---------------------------------------------------------------- the server, once
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  rm -f $L/$N.failed $L/$N.done
  wait_others
  hb "$N: started; 4-bit model $Q4, default on the frozen packs and grounded-g2, grounded-g3 on the $SETTING packs: D5 ($ND) and the mechanical sample (116); judge $DS (coverage role, instruction v2, $JSETTING)"
  status RUNNING no "$N running: answers, judging, scoring; markers $L/$N.done / .failed"
  clear_server
  hb "$N: the server is idle and no model but $Q4 is loaded; the 4-bit model loads on the first request"
fi

# ---------------------------------------------------------------- the timing of the whole retrieval of each side (CPU, guarded, the server idle, before the reference)
if ! stage_done rettime; then
  recheck rettime
  [ $DRY = yes ] || idle
  wait_others
  hb "$N: retrieval timing started (the whole retrieval of the frozen setting and of $SETTING on the funnel's 20 timing questions, uncached, no server call)"
  $G --stall-min 15 --log $L/$N.rettime.log -- .venv/bin/python $R retrieval-timing --items $ITEMS --funnel-work $FW --setting $SETTING --out $W/retrieval_timing.jsonl $STANDIN \
    || fail "the retrieval timing failed (see $L/$N.rettime.log)"
  mark rettime $W/retrieval_timing.jsonl
  hb "$N: retrieval timing done"
fi

# ---------------------------------------------------------------- answers (one request at a time; the 4-bit model)
if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $Q4"; fi
ECHOREF=$W/ans_d5_default.jsonl.manifest.json
echo_check() { # the model the server echoed equals the reference run's (D5 default), and the stored default run's where this run replays one
  .venv/bin/python $GR check-echo --stored $ECHOREF $echo_extra --new $out.manifest.json $( [ $DRY = yes ] && echo --dry ) \
    || fail "answers $config: the server echoed another model than the reference answers of this run (the answer sets are not comparable)"; }
answer_run() { # OUT ITEMS REPLAY EXPECT MODE CONFIG [STORED_MANIFEST]: all items, one request at a time
  out=$1; items=$2; replay=$3; expect=$4; mode=$5; config=$6; echo_extra=$7
  if .venv/bin/python $GR check-answers --file $out --expect $expect --mode $mode > $W/check.out 2>&1; then
    echo "skip answers: $(cat $W/check.out)"; echo_check; return 0; fi
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    hb "$N: answers $config started ($expect items, mode $mode)"
    $G --stall-min 40 --log $L/$N.$config.log -- .venv/bin/python scripts/eval2.py answer --items $items --out $out --config $config $GA \
      --extra-body "$EB" --mode $mode --replay-packs $replay --catalogue $CATS --host-class "Mac Studio" --allow-commit-change \
      || fail "answers $config: failed (see $L/$N.$config.log)"
    .venv/bin/python $GR check-answers --file $out --expect $expect --mode $mode && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $config: incomplete, with errors, empty or without a pack after $tries passes ($out)"
  done
  .venv/bin/python $GR packs-equal --a $replay --b $out || fail "answers $config: the packs differ from the pack file (replay failed)"
  echo_check
}
if ! stage_done reference; then
  recheck reference
  # the reference: default on the FROZEN packs, answered in this run (the D5 answers first: they set the echoed model)
  answer_run $W/ans_d5_default.jsonl $W/items_d5_h.jsonl $W/packs_frozen_d5.jsonl $ND default m6-da-d5-default
  answer_run $W/ans_ma_default.jsonl $W/items_ma.jsonl $TA_ANS $NMA default m6-da-ma-default "--stored $TA_ANS.manifest.json"
  answer_run $W/ans_m3_default.jsonl $W/items_m3.jsonl $TA3_ANS $NM3 default m6-da-m3-default "--stored $TA3_ANS.manifest.json"
  mark reference $REF_FILES
  hb "$N: reference answers done (default on the frozen packs: D5 $ND, tier_a part $NMA, tier_a3 part $NM3)"
fi
if ! stage_done answers; then
  recheck answers
  for v in $VARS; do
    mode=grounded-$v
    answer_run $W/ans_d5_$v.jsonl $W/items_d5_h.jsonl $W/packs_chosen_d5.jsonl $ND $mode m6-da-d5-$v
    answer_run $W/ans_ma_$v.jsonl $W/items_ma.jsonl $W/packs_chosen_ma.jsonl $NMA $mode m6-da-ma-$v
    answer_run $W/ans_m3_$v.jsonl $W/items_m3.jsonl $W/packs_chosen_m3.jsonl $NM3 $mode m6-da-m3-$v
    # the prompt check of this variant right after its answer sets (CPU): a mode that silently fell back to the default prompt is the failure this catches
    if ! stage_done prompts_$v; then
      : > $W/prompts_check_$v.txt
      for s in "d5 $ND $W/items_d5_h.jsonl" "ma $NMA $W/items_ma.jsonl" "m3 $NM3 $W/items_m3.jsonl"; do
        set -- $s; part=$1; n=$2; its=$3
        { .venv/bin/python scripts/eval2.py compare-prompts $W/ans_${part}_default.jsonl $W/ans_${part}_$v.jsonl
          .venv/bin/python $R prompts-differ --a $W/ans_${part}_default.jsonl --b $W/ans_${part}_$v.jsonl --expect $n
          .venv/bin/python $R prompts-check --items $its --answers $W/ans_${part}_$v.jsonl --mode grounded-$v; } >> $W/prompts_check_$v.txt 2>&1 \
          || { cat $W/prompts_check_$v.txt; fail "the prompt check failed for $v on $part: a mode answered with another prompt than its own (see above)"; }
      done
      cat $W/prompts_check_$v.txt
      mark prompts_$v $W/prompts_check_$v.txt
    fi
  done
  mark answers $ANS_FILES
  hb "$N: all answers done (grounded-g2, grounded-g3 on the $SETTING packs: D5 $ND, tier_a part $NMA, tier_a3 part $NM3); the prompt check passed for both"
fi

# ---------------------------------------------------------------- 6. judge: DeepSeek, coverage role, two passes, the three D5 answer sets
if ! stage_done judge; then
  recheck judge
  if [ $DRY = no ]; then unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; fi
  judge_done() { if [ $MAXU != 0 ]; then [ -f "$1" ] && .venv/bin/python $GR judge-check --file "$1" --expect "$2" --max-unjudged $MAXU > /dev/null 2>&1; return; fi
    [ -f "$1" ] && .venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; sys.exit(0 if len(r)==int(sys.argv[2]) and all(x["valid_reply"] for x in r) else 1)' "$1" "$2" 2>/dev/null; }
  jrun() { # EXTRA...
    SETS="default=$W/ans_d5_default.jsonl g2=$W/ans_d5_g2.jsonl g3=$W/ans_d5_g3.jsonl"
    if [ $DRY = yes ]; then
      .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
        --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $JSETTING --concurrency 3 --stub --passes 1 2 --cache $cache --out $jout "$@"
    else
      $G --stall-min 60 --log $L/$N.judge_d5.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS \
        --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
        --settings-name $JSETTING --concurrency 3 --judge-url "$OMLX_URL/v1" --passes 1 2 --cache $cache --out $jout "$@"
    fi; }
  jout=$W/judge_d5.jsonl; cache=$W/judge_cache_d5.jsonl; calls=$((ND * 3 * 2))
  if judge_done $jout $calls; then echo "skip judging d5: $jout has $(.venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; v=sum(1 for x in r if x["valid_reply"]); print(f"{v} valid rows, {len(r) - v} unjudged of {len(r)} (expected {sys.argv[2]})")' $jout $calls)"
  else
    hb "$N: judging d5 started ($calls judge rows; the cache holds earlier replies)"
    for round in 1 2; do
      extra=""; [ $round = 2 ] && extra="--retry-invalid"
      if [ $DRY = yes ]; then jrun $extra > $W/judge_d5.dry.out 2>&1 || { cat $W/judge_d5.dry.out; fail "judge dry run (d5) failed"; }
        grep -E '^(matched|instruction|items)' $W/judge_d5.dry.out
      else jrun $extra || fail "judging d5 round $round failed (see $L/$N.judge_d5.log)"; fi
      bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $jout)
      rep=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and json.loads(l).get("repaired")))' $jout)
      echo "judging d5 round $round: $bad unscored, $rep repaired (valid, counted apart)"
      [ "$bad" = "0" ] && break
    done
    if [ $MAXU != 0 ]; then
      .venv/bin/python $GR judge-check --file $jout --expect $calls --max-unjudged $MAXU || fail "judging d5: $jout does not hold $calls rows with at most $MAXU unjudged and a valid pass for every answer"
    else judge_done $jout $calls || fail "judging d5: $jout does not hold $calls valid rows"; fi
  fi
  if [ $DRY = no ]; then unload $DS; hb "$N: judging finished, $DS unloaded"; fi
  mark judge $W/judge_d5.jsonl
fi

# ---------------------------------------------------------------- 7. identifier check (D5) and the scorer (mechanical) (CPU)
if ! stage_done checks; then
  OUTS=""
  for v in default $VARS; do
    .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_d5_$v.jsonl --catalogues $CATS --totals-only > $W/idcheck_d5_$v.txt || fail "identifier check on d5 $v failed"
    echo "idcheck d5 $v: $(sed -n 4p $W/idcheck_d5_$v.txt)"
    OUTS="$OUTS $W/idcheck_d5_$v.txt"
  done
  for grp in ma m3; do
    for v in default $VARS; do
      .venv/bin/python scripts/eval2_score.py --items $W/items_$grp.jsonl --answers $W/ans_${grp}_$v.jsonl --catalogues $CATS --scores $W/score_${grp}_$v.jsonl \
        --summary $W/score_${grp}_$v.summary.json > $W/score_${grp}_$v.out 2> $W/score_${grp}_$v.err || fail "scorer on $grp $v failed (see $W/score_${grp}_$v.err)"
      echo "score $grp $v: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print("all-pass", d["all_pass"], "of", d["items_scored"], "format failures", d["format_failures"], "missing answers", d["missing_answers"])' $W/score_${grp}_$v.summary.json)"
      OUTS="$OUTS $W/score_${grp}_$v.jsonl"
    done
  done
  mark checks $OUTS
fi

# ---------------------------------------------------------------- 8. the measures, the rule by code, 9. the report
recheck measures
.venv/bin/python $R measures --work $W --ids $IDS --d5-rubrics $RUBS --d5-items $ITEMS --funnel-work $FW --setting $SETTING --packs-frozen $W/packs_frozen_d5.jsonl \
  --packs-chosen $W/packs_chosen_d5.jsonl --max-unjudged $MAXU $([ $DRY = yes ] && echo --stub) > $W/measures.out || { tail -3 $W/measures.out; fail "the measures could not be computed (a missing input is an error, see above)"; }
RULE_RC=0; .venv/bin/python $R rule --measures $W/measures.json > $W/rule_out.txt 2> $W/rule_out.err || RULE_RC=$?
[ $RULE_RC = 0 ] || { cat $W/rule_out.err >&2; fail "the rule could not be applied: $(head -1 $W/rule_out.err)"; }
cat $W/rule_out.txt
CHOICE=$(sed -n 's/^CHOICE: \(.*\)$/\1/p' $W/rule_out.txt | tail -n 1)
case $CHOICE in grounded-g2|grounded-g3|neither) ;; *) fail "the rule printed no usable CHOICE line";; esac
.venv/bin/python $R candidate --measures $W/measures.json --record $REC --manifest $W/MANIFEST.json --out $CHOSENOUT --report $REPORT $DRYFLAG || fail "CANDIDATE_CHOSEN.json could not be written"
{
  cat $HEAD
  echo
  for note in "unjudged-note --work $W --max-unjudged $MAXU" "code-change-note --manifest $W/MANIFEST.json"; do
    txt=$(.venv/bin/python $R $note) || fail "the report block '${note%% *}' could not be written"
    [ -z "$txt" ] || { echo "$txt"; echo; }
  done
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  echo "**Choice by the rule: $CHOICE.**"
  echo
  cat $W/measures.md
  echo
  echo "### The rule applied by code (\`$R rule\`)"
  echo
  echo '```'; cat $W/rule_out.txt; echo '```'
  echo
  echo "### The prompt check"
  echo
  echo '```'; cat $W/prompts_check_g2.txt $W/prompts_check_g3.txt; echo '```'
  echo
  echo "### Files"
  echo
  echo "Answers \`$W/ans_*_{default,g2,g3}.jsonl\`; packs \`$W/packs_*.jsonl\`; judge results \`$W/judge_d5.jsonl\`; identifier checks \`$W/idcheck_d5_*.txt\`; scores \`$W/score_*.jsonl\`; measures \`$W/measures.json\`. Development data only (D5 is the fresh development batch of M6); the hidden part is not involved."
  echo
  echo "CHOICE: $CHOICE"
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
if [ $DRY = yes ]; then
  grep -q M6_D5_ANSWERS_DRY_RUN_DONE $L/$N.done 2>/dev/null || echo M6_D5_ANSWERS_DRY_RUN_DONE >> $L/$N.done   # a resume of a finished run changes nothing
  echo "dry run done: report from stub data in $REPORT"
  echo "CHOICE: $CHOICE"
  exit 0
fi
hb "$N: done; report $REPORT; choice by the rule: $CHOICE" yes
status DONE yes "$N done: $REPORT; choice by the rule: $CHOICE; the Owner decides what follows"
grep -q M6_D5_ANSWERS_DONE $L/$N.done 2>/dev/null || echo M6_D5_ANSWERS_DONE >> $L/$N.done
echo "CHOICE: $CHOICE"
