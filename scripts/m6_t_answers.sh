#!/bin/sh
# M6 T-chain: the answers chain for the set T (exit criterion Y3 of MILESTONE.md; agents/m6-plan.md). Built and rehearsed (dry run, stub model, stub judge) now; the real run is a
# separate task. The set T (eval/m6/t: 41 open scenarios, 33 where a destructive shortcut is tempting and wrong, 8 where a destructive step is right; item field t_kind) is
# answered by the default runtime; the answer files go to blind readers for harmful statements (a later task, which takes this chain's files); the experimental destructive-step
# marking (kafka_sre_rag/checker.py, runs/m6/reports/h1_destructive_marking.md) is run over the answers as post-processing and reported with the marks it makes. There is no pass bar:
# the run measures. Nothing here starts a model by itself: the answers are server calls (one request at a time) under scripts/run_guarded.py; the reference's retrieval loads the
# frozen reranker on the CPU under the guard.
#
# Steps (each resumable by its output files; a stage marker W/stage_NAME.done records the manifest hash and the sha256 of every output; a line per item):
#   1. Gate (CPU, refusal exit 2, nothing written before it passes): T's files present, 41 items and 41 rubrics with the same ids and the t_kind split 33 + 8, their sha256 into the
#      manifest; the judge's path accepts all 41 rubrics offline (scripts/m6_t_report.py judge-offline-check); kafka_sre_rag verify; no state of an earlier run (a work folder without
#      a manifest, a report that exists before its header); the start check of a stored manifest and markers; in real runs the server checks of scripts/m6_d5_answers.sh
#      (the 4-bit model listed, no other chain or guarded job, no active or waiting request, no model of another client loaded; our judge builds unloaded).
#   2. Report header runs/m6/T_ANSWERS.md: what is measured and what is not, and that nothing is a pass bar (written once, before any answer exists).
#   3. Reference: the default mode on the reference configuration (the 4-bit base model, thinking off, the 2,048-token limit, the frozen retrieval with its own search-terms call),
#      all 41 scenarios; the rows keep their packs and their real search-terms, retrieval and answer seconds.
#   4. Variant: grounded-g2 on the SAME packs (--replay-packs from the reference file). Right after the set: the packs equal the reference's, and the prompt check (eval2.py
#      compare-prompts: a different prompt for every id; the recorded prompt hash is the mode's own prompt on the pack).
#   5. Marks (CPU): the destructive-step marking over both answer files, post-processing only (the text the judge and the readers see is the unmarked answer). Per answer: marks,
#      matched pattern ids, marks on a step the answer itself labels as destructive.
#   6. Judge (DeepSeek, coverage role, instruction v2, setting deepseek-nothink-12k, two passes) on both answer sets: 164 judge calls; --max-unjudged N as in scripts/m6_d5_answers.sh.
#   7. Identifier check (CPU) on each arm, whole and split by t_kind. The scorer's decline detector is applied in the measures (T has no mechanical checks to score).
#   8. Report body (scripts/m6_t_report.py measures): per arm and split by t_kind. 9. The done marker; the last line of the log and of the report is
#      "T ANSWERS DONE: <answers> answers, <judge rows> judge rows".
# The start check runs before the reference, the variant, the marks, the judge and the measures, so that a change between stages stops the chain; the pinned code is everything the
# answer, judge, identifier-check and marks paths import (scripts/m6_t_report.py code-pins). The answer calls pass --allow-commit-change: an unrelated commit during the run does not
# stop it (the pins carry the protection). There is no --accept-code-change: a change of a pinned file, this script included, stops the chain; the stopped run is moved aside.
# A stopped run is resumed by running the script again: the failed marker is removed and the stage markers decide what is done. Only a state the start check cannot accept
# (changed pins or inputs, another manifest) is refused, and then the refusal prints the mv lines.
#
#   scripts/m6_t_answers.sh > runs/m6/logs/m6_t_answers.out 2>&1 &          (real)
#   scripts/m6_t_answers.sh --dry-run --work DIR                              (stub model, stub judge, BM25-only retrieval, no server; nothing is written outside DIR; the numbers are not measurements)
# Options: --dry-run, --work DIR, --report FILE, --max-unjudged N (default 0: a judge row that stays invalid after the judge rounds stops the chain; up to N such rows are left out
#   and named; an answer with no valid pass still stops the chain), and for dry runs --items F --rubrics F (never the real T files; a refusal rehearsal).
# Markers: runs/m6/logs/m6_t_answers.done or .failed (a dry run: in DIR/logs). The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/.
#
# Estimated duration of the real run (inferred from the development check's and the D5 chain's rates in scripts/m6_d5_answers.sh, not measured on T): see "planned:" below, about 1.6 to
# 3.0 h of model-server time; plus a few minutes of CPU for the checks.
set -e
DRY=no; WSET=no; W=work/m6/t; REPORT=runs/m6/T_ANSWERS.md; RSET=no; MAXU=0
ITEMS=eval/m6/t/items.jsonl; RUBS=eval/m6/t/rubrics.jsonl; INSET=no
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --items) ITEMS=${2:?}; INSET=yes; shift;; --rubrics) RUBS=${2:?}; INSET=yes; shift;;
  --max-unjudged) MAXU=${2:?}; shift;;
  *) echo "usage: scripts/m6_t_answers.sh [--dry-run] [--work DIR] [--report FILE] [--max-unjudged N] [--items F --rubrics F (dry runs)] (see the header)" >&2; exit 2;; esac
  shift; done
case $MAXU in ''|*[!0-9]*) echo "REFUSED: --max-unjudged is a whole number (0 or more); nothing started" >&2; exit 2;; esac
R=scripts/m6_t_report.py; DR=scripts/m6_d5_rule.py; GR=scripts/m5_g_rule.py; N=m6_t_answers; L=runs/m6/logs
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
JSETTING=deepseek-nothink-12k; SRC=12000
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m6_t_answers.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/T_ANSWERS_from_stub_data.md
  case "$W" in work/m6/t|work/m6/t/|*/work/m6/t|*/work/m6/t/) refuse "a dry run never uses the real work folder";; esac
  case "$REPORT" in runs/m6/T_ANSWERS.md|*/runs/m6/T_ANSWERS.md) refuse "a dry run never uses the real report";; esac
  if [ $INSET = yes ]; then case "$ITEMS $RUBS" in *eval/m6/t/*) refuse "a dry run's --items and --rubrics are not the real T files (omit them to use the real files)";; esac; fi
  G="scripts/run_guarded.py --other-retries 0"
else
  [ $INSET = no ] || refuse "--items and --rubrics are for dry runs only (the real run reads eval/m6/t)"
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 2"
fi
hb() { [ $DRY = yes ] && return 0; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> runs/v3/HEARTBEAT.md; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M6 T: the answers chain for the set T (harmful statements)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
STOPPED_SUFFIX=stopped_$(date +%Y%m%d)
moveaside() { # the lines that move the state of a stopped run out of the way (printed, never executed here)
  echo "to start again, first move the stopped run's files aside (the chain never moves or deletes them):" >&2
  for f in $W $REPORT $L/$N.failed; do [ -e $f ] && echo "  mv $f $f.$STOPPED_SUFFIX" >&2; done
  echo "  (and runs/m6/logs/$N.out, if it holds the stopped run's output, before the next start redirects into it)" >&2; return 0; }
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; mkdir -p $L; echo "$1" >> $L/$N.failed; exit 1; fi
  hb "$N: STOPPED: $1" yes; mkdir -p $L; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }

# ---------------------------------------------------------------- 1. the gate (CPU; nothing is written before the checks pass)
for f in $ITEMS $RUBS $CHUNKS $FROZEN $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json kafka_sre_rag/prompts.py kafka_sre_rag/retrieval.py kafka_sre_rag/checker.py \
         kafka_sre_rag/destructive_patterns.json kafka_sre_rag/evalhooks.py scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_score.py scripts/eval2_compare.py \
         scripts/run_guarded.py scripts/m5_g_rule.py scripts/m6_d5_rule.py $R scripts/m6_t_answers.sh; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
.venv/bin/python $R gate-files --items $ITEMS --rubrics $RUBS --expect-n 41 || refuse "the files of the set T are not what is expected: 41 items and 41 rubrics with the same ids, t_kind 33 + 8"
.venv/bin/python $R judge-offline-check --items $ITEMS --rubrics $RUBS --chunks $CHUNKS || refuse "the judge's path does not accept the T rubrics (see above)"
# no state of an earlier run that the start check cannot accept: a work folder that has no manifest, a report that exists before its header was written
if [ ! -e $W/MANIFEST.json ] && [ -d $W ] && [ -n "$(ls -A $W 2>/dev/null)" ]; then moveaside; refuse "the work folder $W is not empty and has no MANIFEST.json: the state of an earlier run"; fi
if [ ! -e $W/stage_header.done ] && [ -e $REPORT ]; then moveaside; refuse "$REPORT exists before the header of this run was written (it is not overwritten)"; fi
VOUT=$(mktemp "${TMPDIR:-/tmp}/m6_t_verify.XXXXXX")     # nothing is written into the repository before the gate has passed
.venv/bin/python -m kafka_sre_rag verify > $VOUT 2>&1 || { cat $VOUT >&2; rm -f $VOUT; refuse "kafka_sre_rag verify failed (frozen index, prompts or reranker revision)"; }
grep -E '^prompt ' $VOUT | head -8; rm -f $VOUT

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
others() { pgrep -fl 'scripts/(m4|m5|m6|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }
wait_others() { # before anything loads a reranker or a model: no other chain and no guarded job (real runs)
  [ $DRY = yes ] && return 0
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, s1_*.sh) and no guarded job runs"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done; }
if [ $DRY = yes ]; then
  Q4=Qwen3.8-27B-4bit
else
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m6_t_models.XXXXXX")
  api /v1/models > $MJSON 2>/dev/null || true
  Q4=${M6_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $MJSON)} || { rm -f $MJSON; refuse "the 4-bit model is not listed by the server (or ambiguous)"; }
  rm -f $MJSON
fi
if [ $DRY = yes ]; then DRYFLAG="--dry"; else DRYFLAG=""; fi

# the gate has passed: from here on the work folder and the log folder are written
mkdir -p $W $L
# the manifest and the start check
PINS="--pin $ITEMS --pin $RUBS --pin $CHUNKS --pin $CATS/CATALOGUES.json --pin runs/v3/INDEX_FROZEN.json"
for f in $(.venv/bin/python $R code-pins) $FROZEN; do     # the code the answer, judge, identifier-check and marks paths import (the tests trace the imports)
  [ -f $f ] || refuse "pinned code file $f is missing"
  PINS="$PINS --pin $f"; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)   # recorded for the reader, never compared
gstate() { # SUBCOMMAND: the manifest and the start check take the same state
  .venv/bin/python $R "$1" --manifest $W/MANIFEST.json --work $W $PINS --frozen $FROZEN $DRYFLAG --opt "model=$Q4" --opt "set=T" \
    --info "git_head=$GIT_HEAD" --info "max_unjudged=$MAXU" --judge-arg "model=$DS" --judge-arg "setting=$JSETTING" --judge-arg "extra_body=$JEB" --judge-arg "source_chars=$SRC" \
    --judge-arg "instruction=v2" --judge-arg "passes=1 2" --judge-arg "role=coverage"; }
gstate start-check || { moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; }
gstate manifest
# a rerun after a stop: the failed marker is removed (its text is shown); the start check above and the stage markers decide what is done, so finished stages are kept
if [ -f $L/$N.failed ]; then echo "rerun after a stop; the earlier failure was: $(tail -n 1 $L/$N.failed)"; rm -f $L/$N.failed; fi
mkdir -p "$(dirname $REPORT)"
recheck() { # NAME: the start check again before a stage, so that a change of a pinned file, an option or an output between stages stops the chain
  gstate start-check > $W/recheck.out 2>&1 || { cat $W/recheck.out >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started (see above)"; }; }
# a finished run: nothing is redone and nothing changes
if [ -f $L/$N.done ]; then echo "already done: $(tail -n 1 $L/$N.done)"; exit 0; fi

# ---------------------------------------------------------------- the plan and the estimate
ND=$(grep -c . $ITEMS | tr -d ' ')
est() { awk -v d=$ND 'BEGIN{ lo=d*40 + d*35 + d*4*10 + 1200; hi=d*97 + d*85 + d*4*10 + 1800; printf "%.1f to %.1f", lo/3600, hi/3600 }'; }
echo "planned: $ND T scenarios answered in 2 modes (default on the reference configuration with its own search and retrieval; grounded-g2 on the same packs), $((ND * 4)) judge calls: $(est) h of model-server time"
echo "estimate basis: the development check's and the D5 chain's rates (35 to 85 s per answer, 5 to 12 s of search terms and retrieval for the reference, about 10 s per judge call, loads 0.3 to 0.5 h); not measured on T"

stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $GR mark --work $W --stage $st --out "$@" > /dev/null; }
SERVER_NEEDED=no
for st in reference answers judge; do stage_done $st || SERVER_NEEDED=yes; done

# ---------------------------------------------------------------- 2. report header (before any answer exists)
HEAD=$W/report_header.md
if ! stage_done header; then
  for f in $W/ans_t_default.jsonl $W/ans_t_g2.jsonl; do [ ! -e $f ] || refuse "$f exists but the report header was not written yet; the header must come first"; done
  {
    echo "# M6 T answers: the set T answered by the default runtime, read for destructive-step marks$([ $DRY = yes ] && echo ' (DRY RUN: stub model, stub judge, BM25-only retrieval; no number below is a measurement)')"
    echo
    echo "Written by \`scripts/$N.sh\` on $(date +%Y-%m-%d), 4-bit model \`$Q4\`. Items \`$ITEMS\` (sha256 $(.venv/bin/python -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest()[:16])' $ITEMS)), rubrics \`$RUBS\` (sha256 $(.venv/bin/python -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest()[:16])' $RUBS)). Helpers: \`$R\`."
    echo
    .venv/bin/python $R header-text
    echo "This header was written before any answer of the run existed (stage marker \`$W/stage_header.done\`)."
  } > $HEAD
  cp $HEAD $REPORT.tmp && mv $REPORT.tmp $REPORT
  mark header $HEAD
  hb "$N: report header written ($REPORT); no answer exists yet"
fi

# ---------------------------------------------------------------- prepare: harness items (CPU)
if ! stage_done prepare; then
  .venv/bin/python $GR harness-items --in $ITEMS --out $W/items_t_h.jsonl --domain t || fail "harness items of T"
  mark prepare $W/items_t_h.jsonl
fi

# ---------------------------------------------------------------- the server, once
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  wait_others
  hb "$N: started; 4-bit model $Q4, default (own search and retrieval) and grounded-g2 (same packs) on T ($ND); judge $DS (coverage role, instruction v2, $JSETTING)"
  status RUNNING no "$N running: answers, marks, judging; markers $L/$N.done / .failed"
  clear_server
  hb "$N: the server is idle and no model but $Q4 is loaded; the 4-bit model loads on the first request"
fi

# ---------------------------------------------------------------- answers (one request at a time; the 4-bit model)
if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; else GA="--backend openai --model $Q4"; fi
answer_run() { # OUT REPLAY(or none) MODE CONFIG: all items, one request at a time
  out=$1; replay=$2; mode=$3; config=$4
  if .venv/bin/python $GR check-answers --file $out --expect $ND --mode $mode > $W/check.out 2>&1; then echo "skip answers: $(cat $W/check.out)"; return 0; fi
  [ "$replay" = none ] && RP="" || RP="--replay-packs $replay"
  tries=0
  while :; do
    [ $DRY = yes ] || idle
    hb "$N: answers $config started ($ND items, mode $mode)"
    $G --stall-min 40 --log $L/$N.$config.log -- .venv/bin/python scripts/eval2.py answer --items $W/items_t_h.jsonl --out $out --config $config $GA \
      --extra-body "$EB" --mode $mode $RP --catalogue $CATS --host-class "Mac Studio" --allow-commit-change \
      || fail "answers $config: failed (see $L/$N.$config.log)"
    .venv/bin/python $GR check-answers --file $out --expect $ND --mode $mode && break
    tries=$((tries+1)); [ $tries -lt 3 ] || fail "answers $config: incomplete, with errors, empty or without a pack after $tries passes ($out)"
  done
}
if ! stage_done reference; then
  recheck reference
  answer_run $W/ans_t_default.jsonl none default m6-t-default
  # the rows keep their packs and their real search-terms and retrieval seconds
  .venv/bin/python - $W/ans_t_default.jsonl $DRY <<'PY' || fail "the reference rows lack a pack or the search-terms and retrieval seconds"
import json, sys, statistics
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
bad = [r["id"] for r in rows if not r.get("pack") or not all(isinstance(r["seconds"].get(k), (int, float)) for k in ("search_terms", "retrieval", "answer"))]
st, rt = statistics.median(r["seconds"]["search_terms"] for r in rows), statistics.median(r["seconds"]["retrieval"] for r in rows)
print(f"reference rows: {len(rows) - len(bad)} of {len(rows)} with a pack and the three seconds; median search-terms {st:.2f} s, retrieval {rt:.2f} s")
sys.exit(1 if bad or (sys.argv[2] == "no" and not (st > 0 and rt > 0)) else 0)
PY
  mark reference $W/ans_t_default.jsonl
  hb "$N: reference answers done (default, own search and retrieval: $ND)"
fi
if ! stage_done answers; then
  recheck answers
  answer_run $W/ans_t_g2.jsonl $W/ans_t_default.jsonl grounded-g2 m6-t-g2
  .venv/bin/python $GR packs-equal --a $W/ans_t_default.jsonl --b $W/ans_t_g2.jsonl || fail "answers g2: the packs differ from the reference's (replay failed)"
  .venv/bin/python $GR check-echo --stored $W/ans_t_default.jsonl.manifest.json --new $W/ans_t_g2.jsonl.manifest.json $([ $DRY = yes ] && echo --dry) \
    || fail "answers g2: the server echoed another model than the reference answers (the answer sets are not comparable)"
  # the prompt check right after the set (CPU): a mode that silently fell back to the default prompt is the failure this catches
  { .venv/bin/python scripts/eval2.py compare-prompts $W/ans_t_default.jsonl $W/ans_t_g2.jsonl
    .venv/bin/python $DR prompts-differ --a $W/ans_t_default.jsonl --b $W/ans_t_g2.jsonl --expect $ND
    .venv/bin/python $DR prompts-check --items $W/items_t_h.jsonl --answers $W/ans_t_g2.jsonl --mode grounded-g2; } > $W/prompts_check_g2.txt 2>&1 \
    || { cat $W/prompts_check_g2.txt; fail "the prompt check failed for g2: a mode answered with another prompt than its own (see above)"; }
  cat $W/prompts_check_g2.txt
  mark answers $W/ans_t_g2.jsonl $W/prompts_check_g2.txt
  hb "$N: variant answers done (grounded-g2 on the reference's packs: $ND); the prompt check passed"
fi

# ---------------------------------------------------------------- 5. marks (CPU; post-processing of the unmarked answers)
if ! stage_done marks; then
  recheck marks
  for arm in default g2; do
    .venv/bin/python $R marks --answers $W/ans_t_$arm.jsonl --out $W/marks_$arm.jsonl --report $W/marks_$arm.txt || fail "the destructive-step marking over the $arm answers failed"
  done
  mark marks $W/marks_default.jsonl $W/marks_g2.jsonl $W/marks_default.txt $W/marks_g2.txt
fi

# ---------------------------------------------------------------- 6. judge: DeepSeek, coverage role, two passes, both answer sets
if ! stage_done judge; then
  recheck judge
  if [ $DRY = no ]; then unload $Q4; hb "$N: answering finished, $Q4 unloaded; the judge $DS loads on the first request"; fi
  judge_done() { if [ $MAXU != 0 ]; then [ -f "$1" ] && .venv/bin/python $GR judge-check --file "$1" --expect "$2" --max-unjudged $MAXU > /dev/null 2>&1; return; fi
    [ -f "$1" ] && .venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; sys.exit(0 if len(r)==int(sys.argv[2]) and all(x["valid_reply"] for x in r) else 1)' "$1" "$2" 2>/dev/null; }
  jrun() { # EXTRA...
    SETS="default=$W/ans_t_default.jsonl g2=$W/ans_t_g2.jsonl"
    if [ $DRY = yes ]; then
      .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage \
        --answers $SETS --judge-model $DS --judge-extra-body "$JEB" --settings-name $JSETTING --concurrency 3 --stub --passes 1 2 --cache $cache --out $jout "$@"
    else
      $G --stall-min 60 --log $L/$N.judge_t.log -- .venv/bin/python scripts/eval2_judge.py run --rubrics $RUBS --items $ITEMS \
        --chunks $CHUNKS --source-chars $SRC --instruction v2 --coverage --answers $SETS --judge-model $DS --judge-extra-body "$JEB" \
        --settings-name $JSETTING --concurrency 3 --judge-url "$OMLX_URL/v1" --passes 1 2 --cache $cache --out $jout "$@"
    fi; }
  jout=$W/judge_t.jsonl; cache=$W/judge_cache_t.jsonl; calls=$((ND * 2 * 2))
  if judge_done $jout $calls; then echo "skip judging t: $jout has $(.venv/bin/python -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]; v=sum(1 for x in r if x["valid_reply"]); print(f"{v} valid rows, {len(r) - v} unjudged of {len(r)} (expected {sys.argv[2]})")' $jout $calls)"
  else
    hb "$N: judging t started ($calls judge rows; the cache holds earlier replies)"
    for round in 1 2; do
      extra=""; [ $round = 2 ] && extra="--retry-invalid"
      if [ $DRY = yes ]; then jrun $extra > $W/judge_t.dry.out 2>&1 || { cat $W/judge_t.dry.out; fail "judge dry run (t) failed"; }
        grep -E '^(matched|instruction|items)' $W/judge_t.dry.out
      else jrun $extra || fail "judging t round $round failed (see $L/$N.judge_t.log)"; fi
      bad=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and not json.loads(l)["valid_reply"]))' $jout)
      rep=$(.venv/bin/python -c 'import json,sys; print(sum(1 for l in open(sys.argv[1]) if l.strip() and json.loads(l).get("repaired")))' $jout)
      echo "judging t round $round: $bad unscored, $rep repaired (valid, counted apart)"
      [ "$bad" = "0" ] && break
    done
    if [ $MAXU != 0 ]; then
      .venv/bin/python $GR judge-check --file $jout --expect $calls --max-unjudged $MAXU || fail "judging t: $jout does not hold $calls rows with at most $MAXU unjudged and a valid pass for every answer"
    else judge_done $jout $calls || fail "judging t: $jout does not hold $calls valid rows"; fi
  fi
  if [ $DRY = no ]; then unload $DS; hb "$N: judging finished, $DS unloaded"; fi
  mark judge $W/judge_t.jsonl
fi

# ---------------------------------------------------------------- 7. identifier check (CPU), whole and split by t_kind
if ! stage_done checks; then
  recheck checks
  OUTS=""
  for arm in default g2; do
    .venv/bin/python $R split --items $ITEMS --answers $W/ans_t_$arm.jsonl --out-prefix $W/ans_t_${arm}_split || fail "the split of the $arm answers by t_kind failed"
    .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_t_$arm.jsonl --catalogues $CATS --totals-only > $W/idcheck_t_$arm.txt || fail "identifier check on t $arm failed"
    echo "idcheck t $arm: $(sed -n 4p $W/idcheck_t_$arm.txt)"
    OUTS="$OUTS $W/idcheck_t_$arm.txt $W/ans_t_${arm}_split_tempting_wrong.jsonl $W/ans_t_${arm}_split_destructive_right.jsonl"
    for k in tempting_wrong destructive_right; do
      .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_t_${arm}_split_$k.jsonl --catalogues $CATS --totals-only > $W/idcheck_t_${arm}_$k.txt || fail "identifier check on t $arm $k failed"
      OUTS="$OUTS $W/idcheck_t_${arm}_$k.txt"
    done
  done
  mark checks $OUTS
fi

# ---------------------------------------------------------------- 8. the measures, 9. the report and the done marker
recheck measures
.venv/bin/python $R measures --work $W --items $ITEMS --rubrics $RUBS --max-unjudged $MAXU $([ $DRY = yes ] && echo --stub) > $W/measures.out || { tail -3 $W/measures.out; fail "the measures could not be computed (a missing input is an error, see above)"; }
DONE_LINE=$(.venv/bin/python $R done-line --work $W)
{
  cat $HEAD
  echo
  if [ $MAXU != 0 ]; then
    .venv/bin/python scripts/m6_d5_rule.py unjudged-note --work $W --max-unjudged $MAXU 2>/dev/null | sed 's/judge_d5/judge_t/g' || true
  fi
  echo "## Result$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
  echo
  cat $W/measures.md
  echo
  echo "### The prompt check"
  echo
  echo '```'; cat $W/prompts_check_g2.txt; echo '```'
  echo
  echo "### Files for the blind reading (the unmarked answers)"
  echo
  echo "Answers \`$W/ans_t_default.jsonl\` and \`$W/ans_t_g2.jsonl\` (the same 41 scenario ids; the packs of the two files are equal); marks per answer \`$W/marks_{default,g2}.jsonl\`, the checker's report per pattern \`$W/marks_{default,g2}.txt\`; judge results \`$W/judge_t.jsonl\`; identifier checks \`$W/idcheck_t_*.txt\`; measures \`$W/measures.json\`."
  echo
  echo "$DONE_LINE"
} > $REPORT.tmp && mv $REPORT.tmp $REPORT
echo "$DONE_LINE" > $L/$N.done
if [ $DRY = yes ]; then echo "dry run done: report from stub data in $REPORT"; echo "$DONE_LINE"; exit 0; fi
hb "$N: done; report $REPORT; $DONE_LINE" yes
status DONE yes "$N done: $REPORT; the blind reading of the answer files is the next task"
echo "$DONE_LINE"
