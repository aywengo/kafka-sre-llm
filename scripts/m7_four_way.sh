#!/bin/sh
# M7 K4: the six-arm test chain on D5 (agents/m7-plan.md sections 3 and 8; MILESTONE.md decisions V1 to V9). Built and rehearsed (dry run) now; the real run is a separate task (K5).
# The question: what do thinking, a checking pass and a perfect pack each add to today's answer on D5? It is a screen on worn development data: the rule (scripts/m7_six_arm_rule.py)
# decides only whether work continues, and its text, with the outcome table of the plan, is written into the report header before any answer exists. Nothing here starts a model by
# itself: the answers are server calls (one request at a time) under scripts/run_guarded.py, with the 4-bit base model, the default prompt, the frozen search terms, packs replayed.
#
# Arms (all on the default prompt; the packs are files, so no search call is made in an answer):
#   A  frozen pack, the thorough path with both switches off (eval2.py answer --answer-profile thorough --no-thinking --no-verify): the same transport as every arm
#   B  frozen pack, thinking on, no check: read from D's rows (the draft before the check), not a separate run
#   C  frozen pack, the checking pass on A's drafts (python -m kafka_sre_rag.verify_pass run)
#   D  frozen pack, thinking, then the checking pass (eval2.py answer --answer-profile thorough --think-cap N)
#   E  perfect pack (work/m7/packs_perfect.jsonl, padded), as A
#   F  perfect pack, as D; its draft is arm F' (read from F's rows)
# Required arguments (the real run and the dry run refuse without them): --check-variant V1|V1C|none and --think-cap N. They come from the pilot (K3 and the check pilot); the real run
#   also needs --check-variant-source and --think-cap-source (where the value comes from), recorded in the manifest and the report. V1: the checking pass without reference cards, V1C:
#   with the draft's cards, none: no checking pass at all; arms C, D and F are then dropped, D is thinking only (= B), F is thinking only (= F'), and the chain judges A, B, E, F'.
#
# Steps (each resumable by its output files; a stage marker W/stage_NAME.done records the manifest hash and the sha256 of every output; a line per item):
#   1. Gate (CPU, refusal exit 2, nothing written before it passes except the work folder): the two run parameters; D5 items and rubrics equal eval/m6/d5/CHUNK_CHECK.json; the funnel's frozen
#      packs; the perfect packs against their manifest (file hash, input hashes, one pack per scenario); the stored mechanical references; kafka_sre_rag verify; the start check of a stored
#      manifest and markers; then, in real runs, the server of m6_d5_answers.sh (the 4-bit model listed and the one the funnel's expansions were made with, no other chain or guarded job,
#      no active or waiting request, no model of another client loaded; our judge builds are unloaded).
#   2. Report header runs/m7/M7_SIX_ARM.md with the rule's text, the run parameters and their sources (written once, before any answer exists).
#   3. prepare: harness items, the 116 mechanical items (96 + 20) with their stored frozen packs, the D5 frozen packs in the replay format. 3b. retrieval timing (CPU, guarded, no server call):
#      the whole frozen retrieval on the funnel's 20 timing questions from its stored expansions (the time with search adds it to each answer's seconds, with the stored search-terms seconds).
#   4. Answers, one request at a time, in this order: A (D5, 96 + 20 mechanical), C (the checking pass on A's D5 drafts), E, D (D5 and the mechanical items), F. After each set: the packs of every
#      answer equal the pack file; every profile row is whole (streamed draft, prompt hash, token detail; in real runs the request-body hash); the calls the set made equal the expected count.
#   5. Derived sets: B and F' from D's and F's rows, C's answers from A's rows and the check records. Then the prompt check: every draft prompt hash is the default prompt on the row's own pack,
#      equal to A's on the same pack (the arms differ only in their switch), E and F differ from A only by the pack.
#   6. Judge (DeepSeek, coverage role, instruction v2, setting deepseek-nothink-12k, two passes) on the judged sets (A B C D E F F': 700 calls; the variant none: A B E F': 400); the
#      4-bit model is unloaded and the judge loaded as in m6_d5_answers.sh; every swap is noted in HEARTBEAT.md.
#   7. CPU measures: identifier check per set; the mechanical scorer for A and D; cut, empty, time per part (median and 90th percentile).
#   8. Reading prep: blind bundles of A, D, E, F (all four answers of a scenario in one bundle, names drawn per scenario from the seed in the rule text), first-reader and second-reader (a quarter)
#      bundles and the session plan; no reading is done here.
#   9. The rule by code (scripts/m7_six_arm_rule.py rule): coverage rows applied; the wrong-statement and harmful rows are printed as "needs the reading"; no final choice for rows that wait for it.
#      Report, done marker runs/m7/logs/m7_six_arm.done (or .failed).
# A state left by a stopped run (work folder, manifest, report, failed marker) is refused by the start check; the refusal prints the mv lines.
#
#   scripts/m7_four_way.sh --check-variant V1C --think-cap 1500 --check-variant-source "..." --think-cap-source "..." > runs/m7/logs/m7_six_arm.out 2>&1 &     (real, K5)
#   scripts/m7_four_way.sh --dry-run --work DIR --check-variant V1|V1C|none --think-cap N       # stub model, stub judge, stand-in reranker and inputs, no server; nothing is written outside DIR
# Options: --dry-run, --work DIR, --report FILE, --check-variant V, --think-cap N, --check-variant-source TXT, --think-cap-source TXT, --perfect-packs F, --ids FILE, --max-unjudged N
#   (default 0), --fail-after STAGE (dry runs only: plant a failure after that stage, to rehearse a restart), --dry-items N (dry runs: stand-in scenarios, default 8).
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/. Markers: runs/m7/logs/m7_six_arm.done or .failed (a dry run: in its own DIR).
#
# Estimated duration of the real run: printed as "planned:" below (inferred from the development check's and the plan's rates, not measured with thinking on this build; the plan says
# 10 to 14 server hours).
set -e
DRY=no; WSET=no; W=work/m7/six_arm; REPORT=runs/m7/M7_SIX_ARM.md; RSET=no; VARIANT=; CAP=; VSRC=; CSRC=; PERFECT=work/m7/packs_perfect.jsonl; MAXU=0; FAILAFTER=; DRYITEMS=8; PSET=no
IDS=eval/m4/g_dev_ids.json; ITEMS=eval/m6/d5/items.jsonl; RUBS=eval/m6/d5/rubrics.jsonl; CC=eval/m6/d5/CHUNK_CHECK.json; FW=work/m6/d5_funnel
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --check-variant) VARIANT=${2:?}; shift;; --think-cap) CAP=${2:?}; shift;; --check-variant-source) VSRC=${2:?}; shift;; --think-cap-source) CSRC=${2:?}; shift;;
  --perfect-packs) PERFECT=${2:?}; PSET=yes; shift;; --ids) IDS=${2:?}; shift;; --max-unjudged) MAXU=${2:?}; shift;; --fail-after) FAILAFTER=${2:?}; shift;; --dry-items) DRYITEMS=${2:?}; shift;;
  *) echo "usage: scripts/m7_four_way.sh [--dry-run] [--work DIR] [--report FILE] --check-variant V1|V1C|none --think-cap N [--check-variant-source TXT --think-cap-source TXT] [--perfect-packs F] [--ids FILE] [--max-unjudged N] (see the header)" >&2; exit 2;; esac
  shift; done
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
R=scripts/m7_six_arm_rule.py; GR=scripts/m5_g_rule.py; R6=scripts/m6_d5_rule.py; N=m7_four_way; L=runs/m7/logs
[ -n "$VARIANT" ] || refuse "--check-variant V1|V1C|none is required (from the pilot)"
[ -n "$CAP" ] || refuse "--think-cap N is required (from the pilot)"
.venv/bin/python $R gate-params --check-variant "$VARIANT" --think-cap "$CAP" || refuse "the run parameters are not usable (see above)"
case $MAXU in ''|*[!0-9]*) refuse "--max-unjudged is a whole number (0 or more)";; esac
case $DRYITEMS in ''|*[!0-9]*) refuse "--dry-items is a whole number";; esac
if [ $DRY = no ]; then
  case "$VSRC" in *[![:space:]]*) ;; *) refuse "--check-variant-source is required in a real run (where the variant comes from, e.g. the pilot's report)";; esac
  case "$CSRC" in *[![:space:]]*) ;; *) refuse "--think-cap-source is required in a real run (where the cap comes from, e.g. the pilot's report)";; esac
  [ -z "$FAILAFTER" ] || refuse "--fail-after is for dry runs only"
else
  [ -n "$VSRC" ] || VSRC="dry run: none"; [ -n "$CSRC" ] || CSRC="dry run: none"
fi
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json
TA_ITEMS=eval/m4/open/tier_a.jsonl; TA3_ITEMS=eval/m4/open/tier_a3.jsonl; TA_ANS=work/m4/s6/ans_A1.jsonl; TA3_ANS=work/m4/e2/ans_O_Q4D_a3.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
JSETTING=deepseek-nothink-12k; SRC=12000
case $VARIANT in
  none) SETNAMES="A B E Fp"; STEM_B=D; STEM_FP=F; CHECKING=no;;
  *) SETNAMES="A B C D E F Fp"; STEM_B=B; STEM_FP=Fp; CHECKING=yes;;
esac
NSETS=$(echo $SETNAMES | wc -w | tr -d ' ')
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m7_four_way.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/M7_SIX_ARM_from_stub_data.md
  case "$REPORT $W $([ $PSET = yes ] && echo $PERFECT)" in *runs/m7/M7_SIX_ARM.md*|*work/m7/six_arm*|*work/m7/packs_perfect*) refuse "a dry run never uses the real report, work folder or perfect packs";; esac
  case "$FAILAFTER" in ""|header|prepare|rettime|ans_a|ans_c|ans_e|ans_d|ans_f|derive|judge|checks|reading) ;; *) refuse "--fail-after names no stage";; esac
  mkdir -p $W     # only after the refusals above: a refused dry run writes nothing
  IN=$W/in
  [ -s $IN/items.jsonl ] || .venv/bin/python $R dry-inputs --dir $IN --items $DRYITEMS > /dev/null || refuse "the stand-in inputs could not be made"
  ITEMS=$IN/items.jsonl; RUBS=$IN/rubrics.jsonl; CC=$IN/CHUNK_CHECK.json; FW=$IN; PERFECT=$IN/packs_perfect.jsonl
  G="scripts/run_guarded.py --other-retries 0"; STANDIN="--stand-in"; DRYFLAG="--dry"
else
  [ $WSET = no ] || [ "$W" = work/m7/six_arm ] || echo "note: a real run into $W"
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"; STANDIN=""; DRYFLAG=""
fi
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M7 K5: the six-arm test on D5 (thinking, checking pass, perfect pack)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
STOPPED_SUFFIX=stopped_$(date +%Y%m%d)
moveaside() { # the lines that move the state of a stopped run out of the way (printed, never executed here)
  echo "to start again, first move the stopped run's files aside (the chain never moves or deletes them):" >&2
  for f in $W $REPORT $L/$N.failed; do [ -e $f ] && echo "  mv $f $f.$STOPPED_SUFFIX" >&2; done
  echo "  (and runs/m7/logs/m7_six_arm.out, if it holds the stopped run's output, before the next start redirects into it)" >&2; return 0; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N: STOPPED: $1" yes; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }
planted() { [ "$FAILAFTER" != "$1" ] || fail "planted failure after the stage $1 (a dry run's rehearsal of a restart)"; }

# ---------------------------------------------------------------- 1. the gate (CPU; nothing is written before the checks pass, except $W itself)
for f in $ITEMS $RUBS $CC $FW/DF_MANIFEST.json $FW/packs.jsonl $FW/expansions.jsonl $PERFECT $PERFECT.manifest.json $IDS $TA_ITEMS $TA3_ITEMS $TA_ANS $TA_ANS.manifest.json $TA3_ANS $TA3_ANS.manifest.json \
         $CHUNKS $FROZEN $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json kafka_sre_rag/prompts.py kafka_sre_rag/retrieval.py kafka_sre_rag/profiles.py kafka_sre_rag/verify_pass.py kafka_sre_rag/kb_lookup.py \
         kafka_sre_rag/evalhooks.py scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_score.py scripts/eval2_compare.py scripts/eval2_blind.py scripts/run_guarded.py \
         scripts/m5_g_rule.py scripts/m5_b4_served.py scripts/m6_d5_funnel.py scripts/m6_d5_rule.py scripts/m7_four_way.sh $R; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
.venv/bin/python $R gate-inputs --items $ITEMS --rubrics $RUBS --chunk-check $CC --funnel-work $FW --perfect $PERFECT --check-variant "$VARIANT" --think-cap "$CAP" $DRYFLAG \
  || refuse "the inputs are not what the chain expects (see above: the D5 files, the funnel's frozen packs, the perfect packs and their manifest)"
VOUT=$(mktemp "${TMPDIR:-/tmp}/m7_six_verify.XXXXXX")     # nothing is written into the repository before the gate has passed
.venv/bin/python -m kafka_sre_rag verify > $VOUT 2>&1 || { cat $VOUT >&2; refuse "kafka_sre_rag verify failed (frozen index, prompts, reranker revision or catalogues)"; }
grep -E '^(prompt |catalogues)' $VOUT | head -9
.venv/bin/python $GR draw-ids --tier-a $TA_ITEMS --tier-a3 $TA3_ITEMS --check $IDS || refuse "$IDS does not reproduce from its rule"
for s in "$TA_ANS 251" "$TA3_ANS 88"; do
  set -- $s; .venv/bin/python $GR check-answers --file $1 --expect $2 --mode default || refuse "the stored default answers $1 are incomplete, have errors or another mode"
done
FUNNEL_MODEL=$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["options"]["model"])' $FW/DF_MANIFEST.json)

# the server (real runs): the helpers of scripts/m6_d5_answers.sh
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
others() { pgrep -fl 'scripts/(m4|m5|m6|m7|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }
wait_others() { # before anything loads a reranker or a model: no other chain and no guarded job (real runs)
  [ $DRY = yes ] && return 0
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, m7_*.sh, s1_*.sh) and no guarded job runs"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done; }
if [ $DRY = yes ]; then
  Q4=Qwen3.8-27B-4bit
else
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m7_six_models.XXXXXX")
  api /v1/models > $MJSON 2>/dev/null || true
  Q4=${M7_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $MJSON)} || refuse "the 4-bit model is not listed by the server (or ambiguous)"
  [ "$Q4" = "$FUNNEL_MODEL" ] || refuse "the 4-bit model on the server ($Q4) is not the one the D5 funnel's expansions were made with ($FUNNEL_MODEL)"
fi

# the gate has passed: from here on the work folder and the log folder are written
mkdir -p $W $L
# the manifest and the start check
PINS="--pin $ITEMS --pin $RUBS --pin $CC --pin $FW/DF_MANIFEST.json --pin $FW/packs.jsonl --pin $FW/expansions.jsonl --pin $PERFECT --pin $PERFECT.manifest.json --pin $IDS \
  --pin $TA_ITEMS --pin $TA3_ITEMS --pin $TA_ANS --pin $TA_ANS.manifest.json --pin $TA3_ANS --pin $TA3_ANS.manifest.json --pin $CATS/CATALOGUES.json --pin runs/v3/INDEX_FROZEN.json"
for f in $(.venv/bin/python $R code-pins) $FROZEN; do     # the code the answer, check, judge, scorer and identifier-check paths import
  [ -f $f ] || refuse "pinned code file $f is missing"
  PINS="$PINS --pin $f"; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)   # recorded for the reader, never compared
gstate() { # SUBCOMMAND [EXTRA...]: the manifest and the start check take the same state
  sub=$1; shift
  .venv/bin/python $R "$sub" --manifest $W/MANIFEST.json --work $W $PINS --frozen $FROZEN $DRYFLAG --opt "model=$Q4" --opt "check_variant=$VARIANT" --opt "think_cap=$CAP" \
    --opt "check_variant_source=$VSRC" --opt "think_cap_source=$CSRC" --opt "bundle_seed=20261123" \
    --info "git_head=$GIT_HEAD" --info "max_unjudged=$MAXU" --judge-arg "model=$DS" --judge-arg "setting=$JSETTING" --judge-arg "extra_body=$JEB" --judge-arg "source_chars=$SRC" \
    --judge-arg "instruction=v2" --judge-arg "passes=1 2" --judge-arg "role=coverage" "$@"; }
gstate start-check || { moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; }
gstate manifest
mkdir -p "$(dirname $REPORT)"
recheck() { # NAME: the start check again before a stage, so that a change of a pinned file, an option or an output between stages stops the chain
  rcout=$(gstate start-check 2>&1) || { echo "$rcout" >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started (see above)"; }; }

# ---------------------------------------------------------------- the plan and the estimate
ND=$(grep -c . $ITEMS | tr -d ' ')
est() { awk -v d=$ND -v cap=$CAP -v chk=$([ $CHECKING = yes ] && echo 1 || echo 0) -v ns=$NSETS 'BEGIN{
    tlo=cap/64; thi=cap/32; clo=20*chk; chi=33*chk; plo=25; phi=75; mlo=10; mhi=13
    lo=2*d*plo + d*chk*clo + 2*d*(plo+tlo+clo) + 116*mlo + 116*(mlo+tlo+clo) + ns*d*2*10 + 3*1200
    hi=2*d*phi + d*chk*chi + 2*d*(phi+thi+chi) + 116*mhi + 116*(mhi+thi+chi) + ns*d*2*12 + 3*1800
    printf "%.1f to %.1f", lo/3600, hi/3600 }'; }
NMA=96; NM3=20
CNT=$(.venv/bin/python $R call-counts --nd $ND --nma $NMA --nm3 $NM3 --check-variant $VARIANT)
cnt() { echo "$CNT" | tr ' ' '\n' | sed -n "s/^$1=//p"; }
echo "planned: $ND D5 scenarios; arms $(echo $SETNAMES | tr ' ' ',') (check variant $VARIANT, thinking cap $CAP); answer calls: $CNT; $(est) h of model-server time (inferred from the development check's rates, the plan's budget per answer and the cap; not measured with thinking on this build; the plan says 10 to 14 h); CPU: the retrieval timing, a few minutes"

stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $GR mark --work $W --stage $st --out "$@" > /dev/null; planted $st; }
SERVER_NEEDED=no
for st in rettime ans_a ans_c ans_e ans_d ans_f judge; do stage_done $st || SERVER_NEEDED=yes; done

# ---------------------------------------------------------------- 2. report header (before any answer exists)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $W/ans_*.jsonl; do [ ! -e $f ] || refuse "$f exists but the report header was not written yet; the rule text must come first"; done
  [ $DRY = yes ] || [ ! -e $REPORT ] || { moveaside; refuse "$REPORT exists; it is not overwritten before the header of this run is written (move it away first)"; }
  {
    echo "# M7 six-arm test on D5: thinking, a checking pass and a perfect pack against today's answer$([ $DRY = yes ] && echo ' (DRY RUN: stub model, stub judge, stand-in reranker and inputs; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/m7_four_way.sh\` on $(date +%Y-%m-%d), 4-bit model \`$Q4\`. Manifest hash $(.venv/bin/python -c 'import hashlib,json,sys; print(hashlib.sha256(json.dumps({k: v for k, v in json.load(open(sys.argv[1])).items() if k != "accepted_code_changes"}, sort_keys=True).encode()).hexdigest()[:16])' $W/MANIFEST.json). Ids of the mechanical sample: \`$IDS\`. Rule: \`$R\`."
    echo
    echo "**Run parameters (from the pilot).** Checking-pass variant: \`$VARIANT\` (source: $VSRC). Thinking cap: $CAP tokens added to the 2,048-token answer limit (source: $CSRC). Judged sets: $(echo $SETNAMES | sed 's/Fp/F-prime/;s/ /, /g')."
    echo
    .venv/bin/python $R rule-text
    echo
    echo "This header, with the rule, was written before any answer of the run existed (stage marker \`$W/stage_header.done\`)."
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header with the rule text written ($REPORT); no answer exists yet"
fi

# ---------------------------------------------------------------- prepare: items, the D5 frozen packs (CPU)
if ! stage_done prepare; then
  .venv/bin/python $GR harness-items --in $ITEMS --out $W/items_d5_h.jsonl --domain d5 || fail "harness items of D5"
  .venv/bin/python $GR select --ids $IDS --mechanical sample --tier-a $TA_ITEMS --tier-a3 $TA3_ITEMS --ref-a $TA_ANS --ref-a3 $TA3_ANS --out-dir $W || fail "mechanical item files"
  .venv/bin/python $R frozen-packs --funnel-work $FW --items $ITEMS --out $W/packs_frozen_d5.jsonl || fail "the D5 frozen pack file"
  mark prepare $W/items_d5_h.jsonl $W/items_ma.jsonl $W/items_m3.jsonl $W/ref_ma.jsonl $W/ref_m3.jsonl $W/packs_frozen_d5.jsonl
fi
NMA=$(grep -c . $W/items_ma.jsonl | tr -d ' '); NM3=$(grep -c . $W/items_m3.jsonl | tr -d ' ')
[ "$NMA" = 96 ] && [ "$NM3" = 20 ] || fail "the mechanical sample has $NMA + $NM3 items; 96 + 20 expected"
echo "sets: D5 $ND, tier_a part $NMA, tier_a3 part $NM3"
[ $DRY = yes ] || [ "$ND" = 50 ] || fail "D5 has $ND scenarios; 50 expected"

# ---------------------------------------------------------------- the server, once
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, m7_*.sh, s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  rm -f $L/$N.failed $L/$N.done
  wait_others
  hb "$N: started; 4-bit model $Q4, arms $(echo $SETNAMES | tr ' ' ','), check variant $VARIANT, thinking cap $CAP: D5 ($ND) and the mechanical sample (116); judge $DS (coverage role, instruction v2, $JSETTING)"
  status RUNNING no "$N running: answers, judging, scoring; markers $L/$N.done / .failed"
  clear_server
  hb "$N: the server is idle and no model but $Q4 is loaded; the 4-bit model loads on the first request"
fi

# ---------------------------------------------------------------- the whole frozen retrieval timed (CPU, guarded, the server idle, no server call)
if ! stage_done rettime; then
  recheck rettime
  [ $DRY = yes ] || idle
  wait_others
  hb "$N: retrieval timing started (the whole frozen retrieval on the funnel's 20 timing questions from its stored expansions, uncached, no server call)"
  $G --stall-min 15 --log $L/$N.rettime.log -- .venv/bin/python $R6 retrieval-timing --items $ITEMS --funnel-work $FW --setting r4g2 --out $W/retrieval_timing.jsonl $STANDIN \
    || fail "the retrieval timing failed (see $L/$N.rettime.log)"
  mark rettime $W/retrieval_timing.jsonl
  hb "$N: retrieval timing done"
fi

# ---------------------------------------------------------------- answers (one request at a time; the 4-bit model)
if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; VA="--dry-run"; else GA="--backend openai --model $Q4"; VA="--backend openai --model $Q4"; fi
REALFLAG=""; [ $DRY = yes ] || REALFLAG="--real"
ECHOREF=$W/ans_d5_A.jsonl.manifest.json
echo_check() { # the model the server echoed equals the first answers' of this run (and the stored mechanical references' where this run replays one)
  .venv/bin/python $GR check-echo --stored $ECHOREF $echo_extra --new $out.manifest.json $( [ $DRY = yes ] && echo --dry ) \
    || fail "answers $config: the server echoed another model than the first answers of this run (the answer sets are not comparable)"; }
answer_run() { # OUT ITEMS REPLAY EXPECT CONFIG "PROFILE ARGS" [STORED_MANIFEST]: all items, one request at a time
  out=$1; items=$2; replay=$3; expect=$4; config=$5; pargs=$6; echo_extra=$7
  if .venv/bin/python $GR check-answers --file $out --expect $expect --mode default > $W/check.out 2>&1 && .venv/bin/python $R check-rows --file $out --expect $expect $REALFLAG >> $W/check.out 2>&1; then
    echo "skip answers: $(cat $W/check.out | tr '\n' ' ')"; echo_check; return 0; fi
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    hb "$N: answers $config started ($expect items; $pargs)"
    $G --stall-min 40 --log $L/$N.$config.log -- .venv/bin/python scripts/eval2.py answer --items $items --out $out --config $config $GA \
      --extra-body "$EB" --mode default --replay-packs $replay --catalogue $CATS --host-class "Mac Studio" --allow-commit-change --answer-profile thorough $pargs \
      || fail "answers $config: failed (see $L/$N.$config.log)"
    { .venv/bin/python $GR check-answers --file $out --expect $expect --mode default && .venv/bin/python $R check-rows --file $out --expect $expect $REALFLAG; } && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $config: incomplete, with errors, empty, without a pack or without a whole profile row after $tries passes ($out)"
  done
  .venv/bin/python $GR packs-equal --a $replay --b $out || fail "answers $config: the packs differ from the pack file (replay failed)"
  echo_check
}
PA="--no-thinking --no-verify"                                     # arms A and E: today's call on the profile's transport
case $VARIANT in
  V1) PD="--think-cap $CAP --no-cards";; V1C) PD="--think-cap $CAP";; none) PD="--think-cap $CAP --no-verify";;
esac                                                               # arms D and F
if [ $CHECKING = yes ]; then CHK_EXPECT_D5=$ND; CHK_EXPECT_M=$((NMA + NM3)); else CHK_EXPECT_D5=0; CHK_EXPECT_M=0; fi
calls() { # STAGE EXPECT_DRAFTS EXPECT_CHECKS FILES...   the model calls the set made against the expected count (strict in a dry run)
  st=$1; ed=$2; ec=$3; shift 3
  .venv/bin/python $R stage-calls --stage $st --files "$@" --expect-drafts $ed --expect-checks $ec $([ $DRY = yes ] && echo --strict) || fail "the calls of the stage $st do not match the expected count"; }

if ! stage_done ans_a; then
  recheck ans_a
  answer_run $W/ans_d5_A.jsonl $W/items_d5_h.jsonl $W/packs_frozen_d5.jsonl $ND m7-six-d5-A "$PA"
  answer_run $W/ans_ma_A.jsonl $W/items_ma.jsonl $TA_ANS $NMA m7-six-ma-A "$PA" "--stored $TA_ANS.manifest.json"
  answer_run $W/ans_m3_A.jsonl $W/items_m3.jsonl $TA3_ANS $NM3 m7-six-m3-A "$PA" "--stored $TA3_ANS.manifest.json"
  calls ans_a $((ND + NMA + NM3)) 0 $W/ans_d5_A.jsonl $W/ans_ma_A.jsonl $W/ans_m3_A.jsonl
  mark ans_a $W/ans_d5_A.jsonl $W/ans_ma_A.jsonl $W/ans_m3_A.jsonl
  hb "$N: arm A answers done (D5 $ND, tier_a part $NMA, tier_a3 part $NM3)"
fi

# ---------------------------------------------------------------- C: the checking pass on A's D5 drafts (the same 4-bit model; thinking off per call)
if [ $CHECKING = yes ] && ! stage_done ans_c; then
  recheck ans_c
  tries=0
  until .venv/bin/python $R check-verify --file $W/check_d5_C.jsonl --answers $W/ans_d5_A.jsonl --expect $ND; do
    tries=$((tries+1)); [ $tries -le 3 ] || fail "the checking pass on A's drafts is incomplete or has errors after 3 passes ($W/check_d5_C.jsonl)"
    [ $DRY = yes ] || idle
    hb "$N: the checking pass on A's drafts started ($ND answers; variant $VARIANT)"
    $G --stall-min 40 --log $L/$N.check_C.log -- .venv/bin/python -m kafka_sre_rag.verify_pass run --answers $W/ans_d5_A.jsonl --items $ITEMS --out $W/check_d5_C.jsonl \
      --catalogues $CATS $VA --extra-body "$EB" $([ $VARIANT = V1C ] && echo --cards) || fail "the checking pass on A's drafts failed (see $L/$N.check_C.log)"
  done
  calls ans_c 0 $ND $W/check_d5_C.jsonl --kind checks
  mark ans_c $W/check_d5_C.jsonl
  hb "$N: the checking pass on A's drafts done (arm C)"
fi

if ! stage_done ans_e; then
  recheck ans_e
  answer_run $W/ans_d5_E.jsonl $W/items_d5_h.jsonl $PERFECT $ND m7-six-d5-E "$PA"
  calls ans_e $ND 0 $W/ans_d5_E.jsonl
  mark ans_e $W/ans_d5_E.jsonl
  hb "$N: arm E answers done (perfect packs, D5 $ND)"
fi

if ! stage_done ans_d; then
  recheck ans_d
  answer_run $W/ans_d5_D.jsonl $W/items_d5_h.jsonl $W/packs_frozen_d5.jsonl $ND m7-six-d5-D "$PD"
  answer_run $W/ans_ma_D.jsonl $W/items_ma.jsonl $TA_ANS $NMA m7-six-ma-D "$PD" "--stored $TA_ANS.manifest.json"
  answer_run $W/ans_m3_D.jsonl $W/items_m3.jsonl $TA3_ANS $NM3 m7-six-m3-D "$PD" "--stored $TA3_ANS.manifest.json"
  calls ans_d $((ND + NMA + NM3)) $((CHK_EXPECT_D5 + CHK_EXPECT_M)) $W/ans_d5_D.jsonl $W/ans_ma_D.jsonl $W/ans_m3_D.jsonl
  mark ans_d $W/ans_d5_D.jsonl $W/ans_ma_D.jsonl $W/ans_m3_D.jsonl
  hb "$N: arm D answers done (thinking$([ $CHECKING = yes ] && echo ', then the checking pass'); D5 $ND, tier_a part $NMA, tier_a3 part $NM3)"
fi

if ! stage_done ans_f; then
  recheck ans_f
  answer_run $W/ans_d5_F.jsonl $W/items_d5_h.jsonl $PERFECT $ND m7-six-d5-F "$PD"
  calls ans_f $ND $CHK_EXPECT_D5 $W/ans_d5_F.jsonl
  mark ans_f $W/ans_d5_F.jsonl
  hb "$N: arm F answers done (perfect packs, thinking$([ $CHECKING = yes ] && echo ', then the checking pass'); D5 $ND)"
fi

# ---------------------------------------------------------------- derived sets (B and F' from the thorough rows, C's answers from A's rows) and the prompt check (CPU)
if ! stage_done derive; then
  DOUT=""
  if [ $CHECKING = yes ]; then
    .venv/bin/python $R derive-draft --src $W/ans_d5_D.jsonl --out $W/ans_d5_B.jsonl --arm B || fail "arm B could not be derived from D's rows"
    .venv/bin/python $R derive-draft --src $W/ans_d5_F.jsonl --out $W/ans_d5_Fp.jsonl --arm Fp || fail "arm F-prime could not be derived from F's rows"
    .venv/bin/python $R derive-check --answers $W/ans_d5_A.jsonl --checks $W/check_d5_C.jsonl --out $W/ans_d5_C.jsonl || fail "arm C could not be derived from A's rows and the check records"
    DOUT="$W/ans_d5_B.jsonl $W/ans_d5_Fp.jsonl $W/ans_d5_C.jsonl"
  fi
  : > $W/prompts_check.txt
  PCFAIL=0
  pc() { "$@" >> $W/prompts_check.txt 2>&1 || PCFAIL=1; }
  for s in A D E F; do pc .venv/bin/python $R prompts-default --items $ITEMS --answers $W/ans_d5_$s.jsonl; done
  for part in ma m3; do
    for s in A D; do pc .venv/bin/python $R prompts-default --items $W/items_$part.jsonl --answers $W/ans_${part}_$s.jsonl; done
    pc .venv/bin/python $R prompts-relation --a $W/ans_${part}_A.jsonl --b $W/ans_${part}_D.jsonl --expect-same-pack
  done
  pc .venv/bin/python $R prompts-relation --a $W/ans_d5_A.jsonl --b $W/ans_d5_D.jsonl --expect-same-pack
  pc .venv/bin/python $R prompts-relation --a $W/ans_d5_E.jsonl --b $W/ans_d5_F.jsonl --expect-same-pack
  pc .venv/bin/python $R prompts-relation --a $W/ans_d5_A.jsonl --b $W/ans_d5_E.jsonl
  pc .venv/bin/python $R prompts-relation --a $W/ans_d5_D.jsonl --b $W/ans_d5_F.jsonl
  [ $PCFAIL = 0 ] || { cat $W/prompts_check.txt; fail "the prompt check failed: an arm answered with another prompt than the default on its pack, or the arms differ in more than their switch (see above)"; }
  cat $W/prompts_check.txt
  .venv/bin/python $GR packs-equal --a $W/packs_frozen_d5.jsonl --b $W/ans_d5_A.jsonl > /dev/null && .venv/bin/python $GR packs-equal --a $PERFECT --b $W/ans_d5_E.jsonl > /dev/null \
    || fail "the packs of A or E differ from their pack files"
  for s in $SETNAMES; do
    case $s in A) stem=A;; B) stem=$STEM_B;; C) stem=C;; D) stem=D;; E) stem=E;; F) stem=F;; Fp) stem=$STEM_FP;; esac
    .venv/bin/python $GR check-answers --file $W/ans_d5_$stem.jsonl --expect $ND --mode default > /dev/null || fail "the answer set $s ($W/ans_d5_$stem.jsonl) is not whole"
  done
  mark derive $W/prompts_check.txt $DOUT
fi

# ---------------------------------------------------------------- 6. judge: DeepSeek, coverage role, two passes, the judged D5 answer sets
SETS=""
for s in $SETNAMES; do
  case $s in A) stem=A;; B) stem=$STEM_B;; C) stem=C;; D) stem=D;; E) stem=E;; F) stem=F;; Fp) stem=$STEM_FP;; esac
  SETS="$SETS $s=$W/ans_d5_$stem.jsonl"
done
if ! stage_done judge; then
  recheck judge
  if [ $DRY = no ]; then unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; fi
  judge_done() { if [ $MAXU != 0 ]; then [ -f "$1" ] && .venv/bin/python $GR judge-check --file "$1" --expect "$2" --max-unjudged $MAXU > /dev/null 2>&1; return; fi
    [ -f "$1" ] && .venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; sys.exit(0 if len(r)==int(sys.argv[2]) and all(x["valid_reply"] for x in r) else 1)' "$1" "$2" 2>/dev/null; }
  jrun() { # EXTRA...
    if [ $DRY = yes ]; then
      .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
        --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $JSETTING --concurrency 3 --stub --passes 1 2 --cache $cache --out $jout "$@"
    else
      $G --stall-min 60 --log $L/$N.judge_d5.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS \
        --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
        --settings-name $JSETTING --concurrency 3 --judge-url "$OMLX_URL/v1" --passes 1 2 --cache $cache --out $jout "$@"
    fi; }
  jout=$W/judge_d5.jsonl; cache=$W/judge_cache_d5.jsonl; calls=$((ND * NSETS * 2))
  if judge_done $jout $calls; then echo "skip judging d5: $jout has $(.venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; v=sum(1 for x in r if x["valid_reply"]); print(f"{v} valid rows, {len(r) - v} unjudged of {len(r)} (expected {sys.argv[2]})")' $jout $calls)"
  else
    hb "$N: judging d5 started ($calls judge rows over $NSETS sets; the cache holds earlier replies)"
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
  JROWS=$(grep -c . $jout | tr -d ' ')
  [ "$JROWS" = "$calls" ] || fail "judge calls: $JROWS rows found, $calls expected"
  echo "calls judge: $JROWS judge rows (expected $calls = $ND scenarios x $NSETS sets x 2 passes)"
  if [ $DRY = no ]; then unload $DS; hb "$N: judging finished, $DS unloaded"; fi
  mark judge $W/judge_d5.jsonl
fi

# ---------------------------------------------------------------- 7. identifier check (D5, every set) and the scorer (mechanical, A and D) (CPU)
if ! stage_done checks; then
  OUTS=""
  for s in $SETNAMES; do
    case $s in A) stem=A;; B) stem=$STEM_B;; C) stem=C;; D) stem=D;; E) stem=E;; F) stem=F;; Fp) stem=$STEM_FP;; esac
    .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_d5_$stem.jsonl --catalogues $CATS --totals-only > $W/idcheck_d5_$s.txt || fail "identifier check on d5 $s failed"
    echo "idcheck d5 $s: $(sed -n 4p $W/idcheck_d5_$s.txt)"
    OUTS="$OUTS $W/idcheck_d5_$s.txt"
  done
  for grp in ma m3; do
    for v in A D; do
      .venv/bin/python scripts/eval2_score.py --items $W/items_$grp.jsonl --answers $W/ans_${grp}_$v.jsonl --catalogues $CATS --scores $W/score_${grp}_$v.jsonl \
        --summary $W/score_${grp}_$v.summary.json > $W/score_${grp}_$v.out 2> $W/score_${grp}_$v.err || fail "scorer on $grp $v failed (see $W/score_${grp}_$v.err)"
      echo "score $grp $v: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print("all-pass", d["all_pass"], "of", d["items_scored"], "format failures", d["format_failures"], "missing answers", d["missing_answers"])' $W/score_${grp}_$v.summary.json)"
      OUTS="$OUTS $W/score_${grp}_$v.jsonl"
    done
  done
  mark checks $OUTS
fi

# ---------------------------------------------------------------- 8. the measures, the reading prep, 9. the rule and the report
recheck measures
.venv/bin/python $R measures --work $W --ids $IDS --d5-rubrics $RUBS --d5-items $ITEMS --funnel-work $FW --check-variant $VARIANT --think-cap $CAP --max-unjudged $MAXU \
  $([ $DRY = yes ] && echo --stub) > $W/measures.out || { tail -3 $W/measures.out; fail "the measures could not be computed (a missing input is an error, see above)"; }
if ! stage_done reading; then
  RD=$W/reading; mkdir -p $RD
  RSETS=""
  case $VARIANT in none) RSETS="A=$W/ans_d5_A.jsonl B=$W/ans_d5_D.jsonl E=$W/ans_d5_E.jsonl Fp=$W/ans_d5_F.jsonl";; *) RSETS="A=$W/ans_d5_A.jsonl D=$W/ans_d5_D.jsonl E=$W/ans_d5_E.jsonl F=$W/ans_d5_F.jsonl";; esac
  rm -rf $RD/first $RD/second $RD/KEY.json $RD/sessions.json
  .venv/bin/python scripts/eval2_blind.py bundle --items $ITEMS --rubrics $RUBS --answers $RSETS --chunks $CHUNKS --out $RD/first --key $RD/KEY.json --seed 20261123 \
    --second-reader 0.25 --second-out $RD/second > $RD/bundle.out || fail "the blind bundles could not be made"
  tail -3 $RD/bundle.out
  .venv/bin/python $R reading-plan --key $RD/KEY.json --out $RD/sessions.json || fail "the reading plan could not be made"
  # the key is written outside every bundle folder; nothing of the configuration is in a bundle (eval2_blind.py); no reading is done here
  .venv/bin/python - $RD <<'PY' || fail "a bundle names a configuration (a switch or the profile)"
import pathlib, sys
bad = ("answer_profile", "--answer-profile", "enable_thinking", "think-cap", "check-variant", "no-verify")
for f in pathlib.Path(sys.argv[1]).glob("*/*.md"):
    t = f.read_text(encoding="utf-8")
    if any(b in t for b in bad):
        print("a bundle file names a configuration:", f); sys.exit(1)
PY
  mark reading $RD/KEY.json $RD/sessions.json
fi
RULE_RC=0; .venv/bin/python $R rule --measures $W/measures.json > $W/rule_out.txt 2> $W/rule_out.err || RULE_RC=$?
[ $RULE_RC = 0 ] || { cat $W/rule_out.err >&2; fail "the rule could not be applied: $(head -1 $W/rule_out.err)"; }
cat $W/rule_out.txt
FINAL=$(sed -n 's/^\(M7 SIX-ARM TEST DONE: .*\)$/\1/p' $W/rule_out.txt | tail -n 1)
[ -n "$FINAL" ] || fail "the rule printed no usable last line"
{
  cat $HEAD
  echo
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  echo "**Knowledge-base rows by the rule; the thorough-profile row waits for the blind reading (wrong and harmful statements).**"
  echo
  cat $W/measures.md
  echo
  echo "### The rule applied by code (\`$R rule\`)"
  echo
  echo '```'; cat $W/rule_out.txt; echo '```'
  echo
  echo "### The prompt check and the call counts"
  echo
  echo '```'; cat $W/prompts_check.txt; echo '```'
  echo
  echo "### What this report does not show"
  echo
  echo "Wrong, invented and harmful statements (blind readings of A, D, E and F wait: bundles in \`$W/reading/first\`, the second reader's quarter in \`$W/reading/second\`, the plan in \`$W/reading/sessions.json\`, the key in \`$W/reading/KEY.json\`, which no reader opens); the deadline of the profile counts from the call, not from the question (the replay has no search); the 4-bit build's honouring of the thinking switch is seen in the rows' reasoning tokens, not assumed. Development data only (D5 is worn after M6); the hidden part is not involved."
  echo
  echo "### Files"
  echo
  echo "Answers \`$W/ans_*.jsonl\` (B, C and F' derived: their \`derived_from\` field names the source); check records \`$W/check_d5_C.jsonl\`; packs \`$W/packs_frozen_d5.jsonl\` and \`$PERFECT\`; judge results \`$W/judge_d5.jsonl\`; identifier checks \`$W/idcheck_d5_*.txt\`; scores \`$W/score_*.jsonl\`; measures \`$W/measures.json\`."
  echo
  echo "$FINAL"
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
if [ $DRY = yes ]; then
  grep -q M7_SIX_ARM_DRY_RUN_DONE $L/$N.done 2>/dev/null || echo M7_SIX_ARM_DRY_RUN_DONE >> $L/$N.done   # a resume of a finished run changes nothing
  echo "dry run done: report from stub data in $REPORT"
  echo "$FINAL"
  exit 0
fi
hb "$N: done; report $REPORT; $FINAL" yes
status DONE yes "$N done: $REPORT; $FINAL; the readings follow; the Owner decides what follows"
grep -q M7_SIX_ARM_DONE $L/$N.done 2>/dev/null || echo M7_SIX_ARM_DONE >> $L/$N.done
echo "$FINAL"
