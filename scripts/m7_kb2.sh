#!/bin/sh
# M7 KB phase 2, I3: the chain of the fact-entries pilot (runs/m7/reports/kb2_design.md sections 3, 4, 7 and 9; MILESTONE.md W12 to W19).
# Two modes, each resumable by its stage markers:
#   --sanity  the 50 sanity windows (the lowest sha256("kb2-sanity:" + chunk id)): writer (DeepSeek), checker (GLM oQ8e), the sanity rule's mechanical parts,
#             the blind reading sample (40 accepted, 20 rejected). The reading itself is a Claude reading session (W18), not done here.
#   --full    all 1,487 windows of the slice (W15's go; after the sanity run passed its rule and its reading: --sanity-reading FILE, refused when more than 4 of the
#             40 accepted entries are rated not ok, or when the sanity run was made for other prompts or models); the
#             sanity windows are reused when the prompt versions and models are the same; then the accepted entries (work/m7/kb/entries_index.jsonl: checker
#             "ok", exact duplicates dropped, sorted by id, rendered) and the audit's sample (200 + cross up to 40 + 50 rejected; second rater a quarter).
#             The entries index's search, the record kafka_sre_rag/kb_entries.json and the runtime option are task I4, not this chain.
#
# Steps (each with a stage marker W/MODE/stage_NAME.done that records the manifest hash and the sha256 of every output; a line per window in the logs):
#   gate (CPU; refusal exit 2, nothing written before it passes): the inputs, kafka_sre_rag verify, the start check of a stored manifest; --full: the sanity
#        result and the reading count; real runs: the server lists both models, no other chain or guarded job, and the server gate below.
#   slice   scripts/kb_slice.py (1,487 ids, its sha256 in the report); a full run checks that the file still reproduces.
#   header  the report header with the sanity rule and the audit bar (written once, before any entry exists).
#   write   the writer under scripts/run_guarded.py (--stall-min 20), after the server gate for DeepSeek; DeepSeek is unloaded after the stage (ours only).
#   check   the checker likewise, for GLM-5.3-Flash-oQ8e-mtp (GLM-5.3-Flash-oQ4e only with --checker-model and --fallback-note, the conductor's note).
#   rule (sanity) | index, audit (full)   CPU.
#   report  runs/m7/KB2_PILOT.md: the header, then each mode's part. Done marker runs/m7/logs/m7_kb2.MODE.done (or .failed).
#
# The server (W2, narrowed by W16): a model is this chain's only with a marker it wrote when it loaded it (W/loaded_writer, W/loaded_checker). Before a model
#   stage, a model of ours that the stage does not use is unloaded; DeepSeek-V4.1-Flash-1017-oQ4e-mtp loaded by another client is unloaded under W16 (after no
#   request is active or waiting: the unload waits until the server is idle, then until the model is gone) with a HEARTBEAT.md line naming it as W16's unload
#   of a foreign model; any other model of another client, or a request active or waiting while no W16 unload is due, refuses (exit 2) and waits for the Owner.
#   W16 as the conductor read it (review of f45dd13): when the foreign model is the DeepSeek build the writer needs, it does not block: the writer uses that
#   copy as it is, without our marker (logged), and leaves it loaded after the writer; the checker stage then unloads it under W16, because there it blocks.
#   A marker counts only when the server reports the model's load time and it is not later than the marker (a leftover marker never makes another client's
#   model ours); when the server reports no load time the gate refuses. A reply from another model than the one asked for stops a stage with a refusal
#   (exit 2) that is never retried, and the stages run with --other-retries 0, so no model loads in a stage without a server check first.
#   Every load and unload is noted in runs/v3/HEARTBEAT.md.
# A stopped run resumes: running the same command again removes the failed marker after the start check; finished stages are kept, a stage that stopped
#   midway resumes from its output files. A changed pin, option or output is refused and the mv lines are printed (nothing is moved or deleted here).
#
#   scripts/m7_kb2.sh --sanity > runs/m7/logs/m7_kb2.sanity.out 2>&1 &                          (J1)
#   scripts/m7_kb2.sh --full --sanity-reading FILE > runs/m7/logs/m7_kb2.full.out 2>&1 &     (J2, W15's go)
#   scripts/m7_kb2.sh --sanity|--full --dry-run --work DIR [plants]     # stub server, stub writer and checker; nothing is written outside DIR
# Options: --work DIR, --report FILE, --sanity-reading FILE (one JSON object per line: id, rating ok|partial|wrong, reason; the ids of
#   W/sanity/reading/sanity_reading_key.json), --checker-model ID, --fallback-note TXT; dry runs only: --fail-after STAGE, --plant-foreign ID
#   (a model of another client loaded at the start), --plant-busy N (the first N status reads show an active request), --plant-stop N (the stub server answers
#   HTTP 500 after N calls: a stop inside a stage), --plant-wrong-model N (another model answers after N calls: a refusal), --plant-write-bad IDS, --plant-write-retry IDS, --plant-write-gate IDS, --plant-check-bad IDS (comma lists of
#   target chunk ids; see the writer's and checker's headers).
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/; they are never written or printed.
set -e
MODE=; DRY=no; W=work/m7/kb; WSET=no; REPORT=runs/m7/KB2_PILOT.md; RSET=no; READING=; FAILAFTER=; PFOREIGN=; PBUSY=0; PSTOP=
PWBAD=; PWRETRY=; PWGATE=; PCBAD=; PWRONG=
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e; CHK=$GLM8; FBNOTE=
WCONC=3; CCONC=2; WMAX=3000; CMAX=1500
while [ $# -gt 0 ]; do case $1 in
  --sanity) MODE=sanity;; --full) MODE=full;; --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --sanity-reading) READING=${2:?}; shift;; --checker-model) CHK=${2:?}; shift;; --fallback-note) FBNOTE=${2:?}; shift;;
  --fail-after) FAILAFTER=${2:?}; shift;; --plant-foreign) PFOREIGN=${2:?}; shift;; --plant-busy) PBUSY=${2:?}; shift;; --plant-stop) PSTOP=${2:?}; shift;;
  --plant-write-bad) PWBAD=${2:?}; shift;; --plant-write-retry) PWRETRY=${2:?}; shift;; --plant-write-gate) PWGATE=${2:?}; shift;; --plant-check-bad) PCBAD=${2:?}; shift;; --plant-wrong-model) PWRONG=${2:?}; shift;;
  *) echo "usage: scripts/m7_kb2.sh --sanity|--full [--sanity-reading FILE] [--dry-run --work DIR] (see the header)" >&2; exit 2;; esac
  shift; done
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
case "$W$REPORT$READING" in *[[:space:]]*) refuse "--work, --report and --sanity-reading take paths without white space";; esac   # unquoted paths below (review of f45dd13)
R=scripts/m7_kb2_rule.py; N=m7_kb2; L=runs/m7/logs
[ -n "$MODE" ] || refuse "--sanity or --full is required"
case $CHK in "$GLM8") ;; "$GLM4") case "$FBNOTE" in *[![:space:]]*) ;; *) refuse "the checker $GLM4 needs --fallback-note (the conductor's note before the run)";; esac;; *) refuse "the checker is $GLM8 (or $GLM4 with --fallback-note)";; esac
case $PBUSY in ''|*[!0-9]*) refuse "--plant-busy is a whole number";; esac
if [ $DRY = no ]; then
  [ -z "$FAILAFTER$PFOREIGN$PSTOP$PWBAD$PWRETRY$PWGATE$PCBAD$PWRONG" ] && [ "$PBUSY" = 0 ] || refuse "--fail-after and the --plant-* options are for dry runs only"
fi
if [ $MODE = full ]; then
  [ -n "$READING" ] || refuse "--full needs --sanity-reading FILE (the sanity reading's result)"
  [ -s "$READING" ] || refuse "the sanity reading's result $READING is missing or empty"
fi
if [ $DRY = yes ]; then
  [ $WSET = yes ] || refuse "a dry run needs --work DIR (a folder of its own; the sanity and the full dry run share it)"
  case "$W" in work/m7/kb|work/m7/kb/*|*/work/m7/kb|*/work/m7/kb/*) refuse "a dry run never uses the real work folder";; esac
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/KB2_PILOT_from_stub_data.md
  case "$REPORT" in *runs/m7/KB2_PILOT.md) refuse "a dry run never writes the real report";; esac
  case "$FAILAFTER" in ""|slice|header|write|check|rule|index|audit|report) ;; *) refuse "--fail-after names no stage";; esac
  HBFILE=$W/HEARTBEAT.md; SLEEP=0
else
  HBFILE=runs/v3/HEARTBEAT.md; SLEEP=30
fi
RW=$W/$MODE; SLICE=$W/slice.jsonl; CHUNKS=work/v3/chunks_s1.jsonl; REC=runs/v3/INDEX_FROZEN.json
if [ $MODE = full ]; then WDONE=$L/$N.write.done; CDONE=$L/$N.check.done; SEL=all; else WDONE=$L/$N.sanity.write.done; CDONE=$L/$N.sanity.check.done; SEL=sanity; fi
hb() { mkdir -p "$(dirname $HBFILE)"; printf '\n### %s — %s\nneeds_human: %s\n' "$(date +%Y-%m-%dT%H:%M:%S%z)" "$1" "${2:-no}" >> $HBFILE; }
status() { # STATE NEEDS_HUMAN NOTE
  [ $DRY = yes ] && return 0
  .venv/bin/python - "$1" "$2" "$3" <<'PY'
import json, sys, datetime
json.dump({"ts": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
           "phase": "M7 KB phase 2: the fact-entries pilot (scripts/m7_kb2.sh)",
           "kind": "job", "state": sys.argv[1], "needs_human": sys.argv[2] == "yes", "note": sys.argv[3]},
          open("runs/v3/status.json", "w"), indent=2)
PY
}
moveaside() { echo "to start again, first move the stopped run's files aside (the chain never moves or deletes them):" >&2
  for f in $RW $L/$N.$MODE.failed; do [ -e $f ] && echo "  mv $f $f.stopped_$(date +%Y%m%d)" >&2; done; return 0; }
fail() { mkdir -p $L; echo "$1" >> $L/$N.$MODE.failed
  if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; exit 1; fi
  hb "$N ($MODE): STOPPED: $1" yes; status STOPPED yes "$N ($MODE) stopped: $1; marker $L/$N.$MODE.failed; a model this chain loaded may still be loaded"
  echo "STOPPED: $1"; exit 1; }
refuse_late() { mkdir -p $L; echo "REFUSED: $1" >> $L/$N.$MODE.failed; hb "$N ($MODE): REFUSED: $1" yes
  status STOPPED yes "$N ($MODE) refused: $1; marker $L/$N.$MODE.failed"; echo "REFUSED: $1" >&2; exit 2; }
planted() { [ "$FAILAFTER" != "$1" ] || fail "planted failure after the stage $1 (a dry run's rehearsal of a restart)"; }

# ---------------------------------------------------------------- the gate (CPU; nothing is written before it passes)
for f in $CHUNKS $REC kafka_sre_rag/prompts.py kafka_sre_rag/kb_entry.py kafka_sre_rag/generators.py scripts/kb_slice.py scripts/kb_write_entries.py \
         scripts/kb_check_entries.py scripts/run_guarded.py scripts/m7_kb2.sh $R; do
  [ -s $f ] || refuse "input $f is missing or empty"
done
VOUT=$(mktemp "${TMPDIR:-/tmp}/m7_kb2_verify.XXXXXX")
.venv/bin/python -m kafka_sre_rag verify > $VOUT 2>&1 || { cat $VOUT >&2; refuse "kafka_sre_rag verify failed (frozen index, prompts, reranker revision or catalogues)"; }
rm -f $VOUT
if [ $MODE = full ]; then
  FGOUT=$(.venv/bin/python $R full-gate --sanity-result $W/sanity/sanity_result.json --sanity-dir $W/sanity --reading $READING --writer $DS --checker $CHK) \
    || refuse "the full run's preconditions do not hold (see above)"
  echo "$FGOUT"
  [ -f $W/sanity/stage_report.done ] || refuse "the sanity run has not finished (no $W/sanity/stage_report.done)"
fi

# the server (real runs): status, loads and unloads; a dry run reads and changes a stub status file instead
SJSON=; STUB=$W/stub_server_status.json
if [ $DRY = no ]; then
  export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}
  export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)}
  [ -n "$OMLX_URL" ] && [ -n "$OMLX_API_KEY" ] || refuse "no server address or key (OMLX_URL, OMLX_API_KEY or ~/.config/kafka-sre-llm/)"
  BASE=${OMLX_URL%/}; BASE=${BASE%/v1}
  BACK="--backend openai"
else
  BACK="--backend stub"
fi
api() { if [ $DRY = yes ]; then .venv/bin/python $R stub-status --file $STUB; else curl -s -m 20 -H "Authorization: Bearer $OMLX_API_KEY" "$BASE$1"; fi; }
busy() { api /api/status | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("active_requests",1))+int(d.get("waiting_requests",0)))' 2>/dev/null || echo 1; }
idle() { n=0; until [ "$(busy)" = "0" ]; do sleep $SLEEP; n=$((n+1)); [ $n -lt 120 ] || fail "server still busy after 60 min"; done; }
loaded() { api /api/status | python3 -c 'import sys,json; print("yes" if sys.argv[1] in (json.load(sys.stdin).get("loaded_models") or []) else "no")' "$1" 2>/dev/null || echo no; }
unload_now() { # MODEL WHY: only when no request is active or waiting; the server's answer is checked; waits until the model is gone; logged
  idle
  if [ $DRY = yes ]; then ucode=$(.venv/bin/python $R stub-unload --file $STUB --model $1); ubody="stub"
  else
    uresp=$(curl -s -m 300 -w '\n%{http_code}' -X POST -H "Authorization: Bearer $OMLX_API_KEY" "$BASE/admin/api/models/$1/unload" 2>/dev/null) || uresp=""
    ucode=$(printf '%s' "$uresp" | tail -n 1); ubody=$(printf '%s' "$uresp" | sed '$d' | tr '\n' ' ' | cut -c1-160 | sed 's/ *$//')
  fi
  case $ucode in [0-9][0-9][0-9]) ;; *) ucode=000;; esac
  case $ucode in
    2??) uw=0; while [ "$(loaded $1)" = "yes" ]; do sleep 5; uw=$((uw+5)); [ $uw -lt 600 ] || fail "the unload of $1 was accepted but the model is still loaded after 600 s"; done
         hb "$N ($MODE): unloaded $1: $2 (no active or waiting requests; the server answered $ucode: ${ubody:-no body}; gone after ${uw} s)";;
    *) hb "$N ($MODE): unload of $1 FAILED (the server answered $ucode: ${ubody:-no body})" yes; fail "the unload of $1 was not accepted (the server answered $ucode)";;
  esac; }
markers() { echo "--marker $DS=$W/loaded_writer --marker $CHK=$W/loaded_checker"; }
drop_marker() { [ "$1" != "$DS" ] || rm -f $W/loaded_writer; [ "$1" != "$CHK" ] || rm -f $W/loaded_checker; return 0; }
server_gate() { # NEED clear|check: exit 2 on a refusal (the model named); with clear, our other models and a W16 model are unloaded
  SJSON=$(mktemp "${TMPDIR:-/tmp}/m7_kb2_status.XXXXXX"); api /api/status > $SJSON 2>/dev/null || true
  gout=$(.venv/bin/python $R server-gate --status $SJSON --need $1 $(markers) 2>&1) || { echo "$gout" >&2; rm -f $SJSON; return 2; }
  rm -f $SJSON; echo "$gout" | grep '^note:' || true
  [ "$2" = clear ] || return 0
  for m in $(echo "$gout" | sed -n 's/^UNLOAD //p'); do unload_now $m "this chain's own model, not used by the next stage"; drop_marker $m; done
  for m in $(echo "$gout" | sed -n 's/^W16 //p'); do
    hb "$N ($MODE): W16: $m is loaded by another client and blocks this M7 chain; unloading it under W16 (the Owner's decision of 2026-11-24) once no request is active or waiting"
    unload_now $m "W16's unload of a foreign model (loaded by another client; MILESTONE.md W16)"; done
  for m in $(echo "$gout" | sed -n 's/^ADOPT //p'); do
    if [ ! -e $W/adopted_writer ]; then touch $W/adopted_writer
      hb "$N ($MODE): $m is loaded by another client and is the model the writer needs: the writer uses that copy as it is, without this chain's marker, and leaves it loaded (W16 as the conductor read it; the checker stage unloads it under W16)"; fi; done
  return 0; }
others() { pgrep -fl 'scripts/(m4|m5|m6|m7|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }
wait_others() { [ $DRY = yes ] && return 0
  [ -z "$(others)" ] && return 0
  hb "$N ($MODE): waiting until no other chain and no guarded job runs"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done; }
use_model() { # ROLE ID: the gate for this stage's model, then our marker (the model loads on the first request)
  wait_others
  server_gate $2 clear || refuse_late "the server is not free for this chain (another client's model or request; see above)"
  server_gate $2 check || refuse_late "the server is not free for this chain after the unloads (see above)"
  if [ "$1" = writer ] && [ -e $W/adopted_writer ] && [ ! -e $W/loaded_writer ]; then return 0; fi     # another client's copy, used as it is (no marker)
  if [ ! -e $W/loaded_$1 ]; then touch $W/loaded_$1; hb "$N ($MODE): loading $2 (the $1; marker $W/loaded_$1; it loads on the first request)"; fi
  [ $DRY = no ] || .venv/bin/python $R stub-load --file $STUB --model $2; }
refresh_marker() { # ROLE: after a stage's job ended (done or stopped, not refused): the marker's time moves past the load of the stage's first request,
  # so that on a resume a later load (another client's) is seen as foreign by the gate
  [ ! -e $W/loaded_$1 ] || touch $W/loaded_$1; }
done_model() { # ROLE ID: unload our model after its stage; another client's adopted copy stays loaded
  if [ "$1" = writer ] && [ -e $W/adopted_writer ] && [ ! -e $W/loaded_writer ]; then
    hb "$N ($MODE): the writer is done; $2 (another client's copy, used without a marker) is left loaded"; rm -f $W/adopted_writer; return 0; fi
  if [ -e $W/loaded_$1 ] && [ "$(loaded $2)" = yes ]; then unload_now $2 "the $1 stage is done (this chain's model)"; fi
  rm -f $W/loaded_$1; }
run_stage() { # ROLE NAME CMD...: the guarded job; exit 2 of the job (another model answered) is a refusal, never retried
  role=$1; what=$2; shift 2
  set +e; "$@"; rc=$?; set -e
  [ $rc = 2 ] || refresh_marker $role
  case $rc in 0) ;; 2) refuse_late "the $what was refused (see its log: the server answered with another model, or an input was refused); nothing is retried";;
    *) fail "the $what stopped (see its log; run again to resume)";; esac; }

if [ $DRY = yes ]; then     # the stub server's status lives in the dry run's own folder (the only thing written before the gate passes)
  mkdir -p $W; .venv/bin/python $R stub-status --file $STUB --init --loaded "$PFOREIGN" --busy $PBUSY
fi
if [ ! -f $RW/stage_write.done ] || [ ! -f $RW/stage_check.done ]; then
  server_gate $DS check || refuse "the server is not free for this chain (another client's model or request; see above)"
fi
if [ $DRY = no ]; then
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m7_kb2_models.XXXXXX"); api /v1/models > $MJSON 2>/dev/null || true
  .venv/bin/python -c 'import json,sys; ids={m.get("id") for m in json.load(open(sys.argv[1])).get("data",[])}; miss=[m for m in sys.argv[2:] if m not in ids]; print("missing: "+" ".join(miss)) if miss else None; sys.exit(1 if miss else 0)' $MJSON $DS $CHK \
    || { rm -f $MJSON; refuse "a model this chain needs is not listed by the server ($DS, $CHK)"; }
  rm -f $MJSON
fi

# the gate has passed: the work and log folders are written from here on
mkdir -p $W $RW $L
if [ $DRY = yes ]; then
  export KB_STUB_WRITE_BAD="$PWBAD" KB_STUB_WRITE_RETRY="$PWRETRY" KB_STUB_WRITE_GATE="$PWGATE" KB_STUB_CHECK_BAD="$PCBAD" KB_STUB_STOP_AFTER="$PSTOP" KB_STUB_WRONG_MODEL_AFTER="$PWRONG"
  G="scripts/run_guarded.py --other-retries 0"
else
  G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --other-retries 0"     # no retry without the chain's server gate (review of f45dd13)
fi
PINS=; for f in $CHUNKS $REC kafka_sre_rag/prompts.py kafka_sre_rag/kb_entry.py kafka_sre_rag/generators.py scripts/kb_slice.py scripts/kb_write_entries.py scripts/kb_check_entries.py \
                scripts/run_guarded.py scripts/m7_kb2.sh $R; do PINS="$PINS --pin $f"; done
RSHA=; [ -z "$READING" ] || RSHA=$(shasum -a 256 $READING | cut -d' ' -f1)
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
gstate() { sub=$1; shift
  .venv/bin/python $R $sub --work $RW $PINS $([ $DRY = yes ] && echo --dry) --opt "mode=$MODE" --opt "writer=$DS" --opt "checker=$CHK" --opt "fallback_note=$FBNOTE" \
    --opt "writer_concurrency=$WCONC" --opt "checker_concurrency=$CCONC" --opt "writer_max_tokens=$WMAX" --opt "checker_max_tokens=$CMAX" \
    --opt "sanity_reading_sha256=$RSHA" --info "git_head=$GIT_HEAD" "$@"; }
SCOUT=$(mktemp "${TMPDIR:-/tmp}/m7_kb2_startcheck.XXXXXX")
if ! gstate start-check > $SCOUT 2>&1; then cat $SCOUT >&2; rm -f $SCOUT; moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"; fi
cat $SCOUT; rm -f $SCOUT
if [ -f $L/$N.$MODE.failed ]; then echo "rerun after a stop; the earlier failure was: $(tail -n 1 $L/$N.$MODE.failed)"; rm -f $L/$N.$MODE.failed; fi
gstate manifest
recheck() { rc=$(gstate start-check 2>&1) || { echo "$rc" >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started"; }; }
stage_done() { [ -f $RW/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $R mark --work $RW --stage $st --out "$@" > /dev/null; echo "stage $st done"; planted $st; }

if [ $MODE = sanity ]; then
  echo "planned: 50 sanity windows; writer $DS (concurrency $WCONC), checker $CHK (concurrency $CCONC); about 0.6 to 1.2 h of model-server time incl. two swaps (inferred from the design's rates, section 7; not measured)"
else
  echo "planned: 1,487 windows (the sanity windows reused); writer about 6 to 12 h, checker about 4 to 10 h of model-server time (inferred from the design's rates, section 7; the sanity run's projection is in the report)"
fi
[ $DRY = no ] || echo "DRY RUN: stub server, stub writer and checker; no number below is a measurement"
status RUNNING no "$N ($MODE) running"

# ---------------------------------------------------------------- slice (CPU)
if ! stage_done slice; then
  if [ $MODE = full ]; then .venv/bin/python scripts/kb_slice.py --check $SLICE --expect 1487 || fail "the slice file does not reproduce"
  else .venv/bin/python scripts/kb_slice.py --out $SLICE --expect 1487 || fail "the slice could not be made (see above)"; fi
  mark slice $SLICE
fi
SLICE_SHA=$(shasum -a 256 $SLICE | cut -d' ' -f1)

# ---------------------------------------------------------------- header (before any entry exists)
HEAD=$W/report_header.md; PART=$RW/report_part.md
if ! stage_done header; then
  if [ $MODE = sanity ]; then
    [ ! -s $RW/windows.jsonl ] || refuse_late "$RW/windows.jsonl exists but the header was not written yet; the rule text comes first"
    [ $DRY = yes ] || [ ! -e $REPORT ] || { moveaside; refuse_late "$REPORT exists; it is not overwritten before this run's header is written (move it away first)"; }
    {
      echo "# M7 KB phase 2: the fact-entries pilot$([ $DRY = yes ] && echo ' (DRY RUN: stub server, stub writer and checker; no number below is a measurement)')"
      echo
      echo "Written by \`scripts/m7_kb2.sh\` on $(date +%Y-%m-%d). Spec: \`runs/m7/reports/kb2_design.md\`. Writer \`$DS\` (prompt \`$(.venv/bin/python -c 'from kafka_sre_rag import prompts as P; print(P.KB_ENTRY_PROMPT_VERSION)')\`), checker \`$CHK\` (prompt \`$(.venv/bin/python -c 'from kafka_sre_rag import prompts as P; print(P.KB_CHECK_PROMPT_VERSION)')\`). Slice \`$SLICE\`: 1,487 chunks, sha256 \`$SLICE_SHA\`."
      echo
      .venv/bin/python $R rule-text
      echo
      echo "This header was written before any entry existed (stage marker \`$RW/stage_header.done\`)."
      echo
    } > $HEAD
  else
    [ -s $HEAD ] || fail "the report header of the sanity run is missing ($HEAD)"
  fi
  if [ $MODE = sanity ]; then mark header $HEAD; else mark header $SLICE; fi
fi
assemble() { # the report from the header and the parts; rewritten only when its text changes
  { cat $HEAD; for p in $W/sanity/report_part.md $W/full/report_part.md; do [ ! -s $p ] || cat $p; done; } > $REPORT.tmp
  if [ -f $REPORT ] && cmp -s $REPORT.tmp $REPORT; then rm -f $REPORT.tmp; else mv $REPORT.tmp $REPORT; fi; }

# ---------------------------------------------------------------- write (server: the writer)
REUSE=; [ $MODE = sanity ] || REUSE="--reuse $W/sanity"
if ! stage_done write; then
  recheck write
  use_model writer $DS
  hb "$N ($MODE): the writer starts ($SEL windows; log $L/$N.$MODE.write.log)"
  run_stage writer "writer ($L/$N.$MODE.write.log)" $G --log $L/$N.$MODE.write.log --stall-min 20 -- .venv/bin/python scripts/kb_write_entries.py --slice $SLICE \
     --record $REC --select $SEL --out-dir $RW --model $DS $BACK --concurrency $WCONC --max-tokens $WMAX --done $WDONE $REUSE
  tail -n 1 $L/$N.$MODE.write.log
  done_model writer $DS
  mark write $RW/windows.jsonl $RW/entries_raw.jsonl $RW/entries_gate_rejects.jsonl $WDONE
  hb "$N ($MODE): the writer is done: $(tail -n 1 $L/$N.$MODE.write.log | cut -c1-200)"
fi

# ---------------------------------------------------------------- check (server: the checker)
CREUSE=; [ $MODE = sanity ] || CREUSE="--reuse $W/sanity"
if ! stage_done check; then
  recheck check
  use_model checker $CHK
  hb "$N ($MODE): the checker starts (log $L/$N.$MODE.check.log)"
  FBA=; [ -z "$FBNOTE" ] || FBA="--fallback-note"
  run_stage checker "checker ($L/$N.$MODE.check.log)" $G --log $L/$N.$MODE.check.log --stall-min 20 -- .venv/bin/python scripts/kb_check_entries.py --slice $SLICE \
     --record $REC --write-dir $RW --write-done $WDONE --out-dir $RW --model $CHK ${FBA:+$FBA "$FBNOTE"} $BACK --concurrency $CCONC --max-tokens $CMAX --done $CDONE $CREUSE
  tail -n 1 $L/$N.$MODE.check.log
  done_model checker $CHK
  mark check $RW/check_windows.jsonl $RW/checks.jsonl $CDONE
  hb "$N ($MODE): the checker is done: $(tail -n 1 $L/$N.$MODE.check.log | cut -c1-200)"
fi

# ---------------------------------------------------------------- CPU stages
if [ $MODE = sanity ]; then
  if ! stage_done rule; then
    set +e; .venv/bin/python $R sanity-rule --write-dir $RW --check-dir $RW --concurrency $WCONC --writer $DS --checker $CHK --out $RW/sanity_result.json > $RW/sanity_rule.txt; rc=$?; set -e
    [ $rc -le 1 ] || fail "the sanity rule could not be computed"     # 1: the rule failed (recorded; the chain stops after the report)
    cat $RW/sanity_rule.txt
    .venv/bin/python $R reading-sample --write-dir $RW --check-dir $RW --slice $SLICE --record $REC --out-dir $RW/reading > $RW/reading_sample.txt || fail "the reading sample could not be drawn"
    cat $RW/reading_sample.txt
    mark rule $RW/sanity_result.json $RW/sanity_rule.txt $RW/reading_sample.txt $RW/reading/sanity_reading.md $RW/reading/sanity_reading_key.json
  fi
else
  IDX=$W/entries_index.jsonl
  if ! stage_done index; then
    .venv/bin/python $R index --write-dir $RW --check-dir $RW --slice $SLICE --out $IDX > $RW/index.txt || fail "the accepted entries could not be collected"
    cat $RW/index.txt; mark index $IDX $RW/index.txt
  fi
  if ! stage_done audit; then
    .venv/bin/python $R audit-sample --index $IDX --write-dir $RW --check-dir $RW --slice $SLICE --record $REC --out-dir $RW/audit > $RW/audit.txt || fail "the audit sample could not be drawn"
    cat $RW/audit.txt; mark audit $RW/audit.txt $RW/audit/audit_main.md $RW/audit/audit_main_key.json
  fi
fi

# ---------------------------------------------------------------- report
if ! stage_done report; then
  {
    echo "## $([ $MODE = sanity ] && echo 'Sanity run (50 windows)' || echo 'Full run (1,487 windows)')$([ $DRY = yes ] && echo ' (from stub data: not a measurement)')"
    echo
    echo "Writer: \`$(tail -n 1 $L/$N.$MODE.write.log)\`"
    echo
    echo "Checker: \`$(tail -n 1 $L/$N.$MODE.check.log)\`"
    echo
    if [ $MODE = sanity ]; then
      echo "The sanity rule's mechanical parts (the reading's part is pending; the full run takes its count):"
      echo; echo '```'; cat $RW/sanity_rule.txt; echo '```'; echo
      echo "Reading sample: \`$RW/reading/sanity_reading.md\` (blind; the checker's verdicts in \`sanity_reading_key.json\`). $(cat $RW/reading_sample.txt)"
    else
      echo "Sanity reading: \`$READING\` (sha256 \`$RSHA\`). $FGOUT."
      echo; echo '```'; cat $RW/index.txt; cat $RW/audit.txt; echo '```'; echo
      echo "Accepted entries: \`$W/entries_index.jsonl\`. The audit (at least 190 of 200 ok) is a reading session; its bundles are in \`$RW/audit/\`."
    fi
    echo
  } > $PART
  assemble
  mark report $PART
fi
assemble
if [ $MODE = sanity ] && ! .venv/bin/python -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1]))["mechanical_pass"] else 1)' $RW/sanity_result.json; then
  fail "the sanity run failed its rule (see $REPORT): rework the prompt (new version, new sanity run); a second failed sanity run stops the pilot until the conductor has seen it"
fi
if [ ! -f $L/$N.$MODE.done ]; then
  echo "$MODE done; report $REPORT" > $L/$N.$MODE.done
  hb "$N ($MODE): done; report $REPORT$([ $MODE = sanity ] && echo '; the sanity reading waits (Claude readers, W18)')"
fi
if [ $MODE = sanity ]; then status WAITING no "$N sanity done; the reading of $RW/reading/sanity_reading.md waits; then the full run (W15)"
else status WAITING no "$N full done; the audit of $RW/audit/ waits"; fi
echo "$([ $DRY = yes ] && echo 'dry run ')done: $MODE; report $REPORT"
