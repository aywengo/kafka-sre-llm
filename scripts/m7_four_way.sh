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
#   R  comparison arm (W7): grounded-g3 on the D5 funnel's r4g2 packs, A's call otherwise (--mode grounded-g3); judged only, outside every decided row, not read
# Models (W8, W9, W10; K4b): --models takes a list from qwen,gemma26,gemma31 (all three by default; qwen is required, the rule is Qwen's). Every model gets arms A and E (the same call, decoding, 2,048-token limit and packs
#   as Qwen's A and E) and its A on the 116 mechanical items; the thorough arms (B, C, D, F, F') run for a model only when --thinking-models names it (default qwen; Qwen's checking arms follow --check-variant). The ids are options
#   (--qwen-model, --gemma26-model, --gemma31-model; defaults Qwen3.8-27B-4bit, gemma-4-26B-A4B-it-QAT-MLX-4bit, and a placeholder for the 31B, whose 4-bit build is made later, task G0: a real run refuses a placeholder).
#   The answer stages are grouped by model, each model loaded alone and unloaded after its stages, in the order gemma31, gemma26, qwen (inside Qwen: D, F, A, C, E, R). The server gate never unloads a model another client
#   loaded (W2): a model of this chain is one it loaded (a marker in the work folder), plus Qwen's 4-bit build and the judge. Another model's switch is probed after its load (thinking on shows thinking, off shows none).
#   K4b (after the review; W11): --thinking-models defaults to none and a real run must pass it explicitly (recorded in the manifest and the header): with none, Qwen answers A, E, R and C (the checking pass alone is the thorough
#   candidate, W11) and the thinking arms' code paths stay. THE PLANNED RUN (the conductor, 2026-11-23, after the Gemma thinking measurement in runs/m7/reports/gemma_think.md) is
#   --thinking-models gemma26,gemma31 --check-variant none: Qwen A, E, R; each Gemma model A, E and its thinking-only arm B (stage ans_d, files ans_*_D_<model>; no perfect-pack thinking arm); the thorough-profile row is applied per Gemma model
#   (its B against its own A) and is "not tested (W11 and the pilot)" for Qwen; --thinking-mech yes|no (default yes): whether a Gemma B also answers the 116 mechanical items. Every model other than Qwen is probed alone at the gate, before any answer (switch off and on; stage `probes`); a model counts as ours (W2)
#   only with a marker this chain wrote when it loaded it, Qwen's 4-bit build and the judge included; the prompt check and the model-free request-body check run right after each model's stages; an answer with no draft is left out of its
#   bundle, not the scenario; a described set (R, another model's) that cannot be computed is "not available" and stops nothing.
#   Time is reported, not decided (W5); harm is matched by consequence (W6); the comparison of each model with Qwen is described, not decided (W10): see the rule text in the report header.
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
#   4. Answers, one request at a time, in this order: D (D5 and the mechanical items), F, A (D5, 96 + 20 mechanical), C (the checking pass on A's D5 drafts), E. D and F run first so that the server's prompt
#      cache (A's draft prompt equals D's) cannot make D look faster than it would be in use; each draft records its time to the first token and its cached prompt tokens. After each set: the packs of every
#      answer equal the pack file; every profile row is whole (streamed draft, prompt hash, token detail; in real runs the request-body hash); the calls the set made equal the expected count.
#   5. Derived sets: B and F' from D's and F's rows, C's answers from A's rows and the check records. Then the prompt check: every draft prompt hash is the default prompt on the row's own pack,
#      equal to A's on the same pack (the arms differ only in their switch), E and F differ from A only by the pack.
#   6. Judge (DeepSeek, coverage role, instruction v2, setting deepseek-nothink-12k, two passes) on the judged sets (A B C D E F F': 700 calls; the variant none: A B E F': 400); the
#      4-bit model is unloaded and the judge loaded as in m6_d5_answers.sh; every swap is noted in HEARTBEAT.md.
#   7. CPU measures: identifier check per set; the mechanical scorer for A and D; cut, empty, time per part (median and 90th percentile).
#   8. Reading prep: blind bundles of A, D, E, F (all four answers of a scenario in one bundle, names drawn per scenario from the seed in the rule text), first-reader and second-reader (a quarter)
#      bundles and the session plan; no reading is done here.
#   9. The rule by code (scripts/m7_six_arm_rule.py rule): coverage rows applied; the wrong-statement and harmful rows are printed as "needs the reading"; no final choice for rows that wait for it.
#      Report, done marker runs/m7/logs/m7_four_way.done (or .failed).
# A stopped run resumes: running the script again removes the failed marker after the start check, and the stage markers decide what is done. Only a state the start check cannot accept (a changed
#   pinned file or input, another manifest, a changed output) is refused; then the refusal prints the mv lines. A changed option (for example another --think-cap) is refused without mv lines: give the stored value.
# The server (real runs): another client's model is never unloaded (the Owner did not extend M6's go to M7): when a model that is not this chain's (the 4-bit model, the judge) is loaded, or a request is
#   active or waiting, the script refuses (exit 2, naming the model) and waits for the Owner; only this chain's own 4-bit and judge models are unloaded between stages.
#
#   scripts/m7_four_way.sh --check-variant V1C --think-cap 1500 --check-variant-source "..." --think-cap-source "..." --gemma31-model ID > runs/m7/logs/m7_six_arm.out 2>&1 &     (real, K5)
#   scripts/m7_four_way.sh --dry-run --work DIR --check-variant V1|V1C|none --think-cap N       # stub model, stub judge, stand-in reranker and inputs, no server; nothing is written outside DIR
# Options: --dry-run, --work DIR, --report FILE, --check-variant V, --think-cap N, --check-variant-source TXT, --think-cap-source TXT, --pilot-report FILE (default runs/m7/M7_PILOT.md; pinned by its sha256;
#   a real run refuses without it or with a dry-run record; a dry run uses a stub), --perfect-packs F, --ids FILE, --max-unjudged N (default 0), --models LIST, --thinking-models LIST, --thinking-mech yes|no, --qwen-model ID, --gemma26-model ID, --gemma31-model ID, --scenarios-per-session N (reading sessions; default about 16 answers
#   each), and for dry runs only: --fail-after STAGE (plant a failure
#   after that stage, to rehearse a restart), --dry-items N (stand-in scenarios, default 8), --plant-cut SUBSTRINGS (comma-separated substrings of a question: the thinking drafts of those scenarios are
#   cut while thinking), --plant-check-fail SUBSTRINGS (the checking calls of those scenarios fail, every time).
# The server address and key come from OMLX_URL and OMLX_API_KEY or ~/.config/kafka-sre-llm/. Markers: runs/m7/logs/m7_four_way.done or .failed (a dry run: in its own DIR).
#
# Estimated duration of the real run: printed as "planned:" below (inferred from the development check's and the plan's rates, not measured with thinking on this build; the plan says
# 10 to 14 server hours).
set -e
DRY=no; WSET=no; W=work/m7/six_arm; REPORT=runs/m7/M7_SIX_ARM.md; RSET=no; VARIANT=; CAP=; VSRC=; CSRC=; PERFECT=work/m7/packs_perfect.jsonl; MAXU=0; FAILAFTER=; DRYITEMS=8; PSET=no
PILOT=runs/m7/M7_PILOT.md; PLANTCUT=; PLANTCHK=; MODELS=qwen,gemma26,gemma31; THINK=none; THSET=no; THMECH=yes; QID=; G26ID=; G31ID=; SPS=0
IDS=eval/m4/g_dev_ids.json; ITEMS=eval/m6/d5/items.jsonl; RUBS=eval/m6/d5/rubrics.jsonl; CC=eval/m6/d5/CHUNK_CHECK.json; FW=work/m6/d5_funnel
while [ $# -gt 0 ]; do case $1 in
  --dry-run) DRY=yes;;
  --work) W=${2:?}; WSET=yes; shift;; --report) REPORT=${2:?}; RSET=yes; shift;;
  --check-variant) VARIANT=${2:?}; shift;; --think-cap) CAP=${2:?}; shift;; --check-variant-source) VSRC=${2:?}; shift;; --think-cap-source) CSRC=${2:?}; shift;;
  --perfect-packs) PERFECT=${2:?}; PSET=yes; shift;; --ids) IDS=${2:?}; shift;; --max-unjudged) MAXU=${2:?}; shift;; --fail-after) FAILAFTER=${2:?}; shift;; --dry-items) DRYITEMS=${2:?}; shift;;
  --pilot-report) PILOT=${2:?}; shift;; --plant-cut) PLANTCUT=${2:?}; shift;; --plant-check-fail) PLANTCHK=${2:?}; shift;;
  --models) MODELS=${2:?}; shift;; --thinking-models) THINK=${2:?}; THSET=yes; shift;; --thinking-mech) THMECH=${2:?}; shift;; --qwen-model) QID=${2:?}; shift;; --gemma26-model) G26ID=${2:?}; shift;; --gemma31-model) G31ID=${2:?}; shift;;
  --scenarios-per-session) SPS=${2:?}; shift;;
  *) echo "usage: scripts/m7_four_way.sh [--dry-run] [--work DIR] [--report FILE] --check-variant V1|V1C|none --think-cap N [--check-variant-source TXT --think-cap-source TXT] [--perfect-packs F] [--ids FILE] [--pilot-report F] [--max-unjudged N] [--models LIST] [--thinking-models LIST] [--thinking-mech yes|no] [--gemma31-model ID] (see the header)" >&2; exit 2;; esac
  shift; done
refuse() { echo "REFUSED: $1; nothing started" >&2; exit 2; }
R=scripts/m7_six_arm_rule.py; GR=scripts/m5_g_rule.py; R6=scripts/m6_d5_rule.py; N=m7_four_way; L=runs/m7/logs
[ -n "$VARIANT" ] || refuse "--check-variant V1|V1C|none is required (from the pilot)"
[ -n "$CAP" ] || refuse "--think-cap N is required (from the pilot)"
.venv/bin/python $R gate-params --check-variant "$VARIANT" --think-cap "$CAP" || refuse "the run parameters are not usable (see above)"
MARGS="--models $MODELS --thinking-models $THINK --thinking-mech $THMECH"
if [ -n "$QID" ]; then MARGS="$MARGS --qwen-model $QID"; fi
if [ -n "$G26ID" ]; then MARGS="$MARGS --gemma26-model $G26ID"; fi
if [ -n "$G31ID" ]; then MARGS="$MARGS --gemma31-model $G31ID"; fi
GMFLAG=; if [ $DRY = yes ]; then GMFLAG=--dry; fi
.venv/bin/python $R gate-models $MARGS $GMFLAG || refuse "the models are not usable (see above: the list, the thinking list, a placeholder id)"
case $MAXU in ''|*[!0-9]*) refuse "--max-unjudged is a whole number (0 or more)";; esac
case $SPS in ''|*[!0-9]*) refuse "--scenarios-per-session is a whole number (0: automatic)";; esac
case $DRYITEMS in ''|*[!0-9]*) refuse "--dry-items is a whole number";; esac
if [ $DRY = no ]; then
  case "$VSRC" in *[![:space:]]*) ;; *) refuse "--check-variant-source is required in a real run (where the variant comes from, e.g. the pilot's report)";; esac
  case "$CSRC" in *[![:space:]]*) ;; *) refuse "--think-cap-source is required in a real run (where the cap comes from, e.g. the pilot's report)";; esac
  [ -z "$FAILAFTER" ] || refuse "--fail-after is for dry runs only"
  [ -z "$PLANTCUT$PLANTCHK" ] || refuse "--plant-cut and --plant-check-fail are for dry runs only"
  [ $THSET = yes ] || refuse "--thinking-models LIST|none is required in a real run (the default is none: whether Qwen's thinking arms run at all is the Owner's decision; the value is recorded in the manifest and the header)"
  .venv/bin/python $R pilot-check --file "$PILOT" || refuse "the pilot's report is not usable (see above)"
else
  [ -n "$VSRC" ] || VSRC="dry run: none"; [ -n "$CSRC" ] || CSRC="dry run: none"
fi
CATS=work/m4/catalogues; CHUNKS=work/v3/chunks_s1.jsonl; FROZEN=kafka_sre_rag/frozen.json
TA_ITEMS=eval/m4/open/tier_a.jsonl; TA3_ITEMS=eval/m4/open/tier_a3.jsonl; TA_ANS=work/m4/s6/ans_A1.jsonl; TA3_ANS=work/m4/e2/ans_O_Q4D_a3.jsonl
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM8=GLM-5.3-Flash-oQ8e-mtp; GLM4=GLM-5.3-Flash-oQ4e
EB='{"chat_template_kwargs": {"enable_thinking": false}}'
JEB='{"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}'
JSETTING=deepseek-nothink-12k; SRC=12000
case $VARIANT in none) CHECKING=no;; *) CHECKING=yes;; esac
SETLINES=$(.venv/bin/python $R sets --check-variant $VARIANT $MARGS) || refuse "the judged sets could not be listed"
SETNAMES=$(echo "$SETLINES" | awk '{print $1}' | tr '\n' ' ')
DECSETS=$(echo "$SETLINES" | awk '$3=="decided"{print $1}' | tr '\n' ' '); DESCSETS=$(echo "$SETLINES" | awk '$3=="described"{print $1}' | tr '\n' ' ')     # Qwen's sets decide the rows; R and the other models' sets are described (K4b)
NSETS=$(echo $SETNAMES | wc -w | tr -d ' ')
stem_of() { echo "$SETLINES" | awk -v n=$1 '$1==n{print $2}'; }     # the file stem of a set's D5 answers
MODELORDER=$(.venv/bin/python $R model-order $MARGS) || refuse "the model order could not be made"     # e.g. gemma31:no gemma26:no qwen:yes
MODELKEYS=$(echo $MODELORDER | tr ' ' '\n' | sed 's/:.*//' | tr '\n' ' ')
hasf() { [ $1 = qwen ] || [ $CHECKING = yes ]; }       # does a thinking model have the perfect-pack thinking arm F? (a thinking Gemma model under none has B only)
hasm() { [ $1 = qwen ] || [ $THMECH = yes ]; }         # does a thinking model's D (B under none) answer the 116 mechanical items?
thinks() { case " $MODELORDER " in *" $1:yes "*) return 0;; esac; return 1; }
if [ $DRY = yes ]; then
  [ $WSET = yes ] || W=$(mktemp -d "${TMPDIR:-/tmp}/m7_four_way.XXXXXX")
  L=$W/logs; [ $RSET = yes ] || REPORT=$W/M7_SIX_ARM_from_stub_data.md
  case "$REPORT $W $([ $PSET = yes ] && echo $PERFECT)" in *runs/m7/M7_SIX_ARM.md*|*work/m7/six_arm*|*work/m7/packs_perfect*) refuse "a dry run never uses the real report, work folder or perfect packs";; esac
  case "$FAILAFTER" in ""|header|prepare|rettime|probes|ans_[acdefr]|ans_[acdef]_gemma26|ans_[acdef]_gemma31|derive|judge|checks|reading) ;; *) refuse "--fail-after names no stage";; esac
  mkdir -p $W     # only after the refusals above: a refused dry run writes nothing
  IN=$W/in
  [ -s $IN/items.jsonl ] || .venv/bin/python $R dry-inputs --dir $IN --items $DRYITEMS > /dev/null || refuse "the stand-in inputs could not be made"
  ITEMS=$IN/items.jsonl; RUBS=$IN/rubrics.jsonl; CC=$IN/CHUNK_CHECK.json; FW=$IN; PERFECT=$IN/packs_perfect.jsonl
  [ -s $IN/M7_PILOT_stub.md ] || echo "stand-in for the pilot's report in a dry run (no pilot exists here)" > $IN/M7_PILOT_stub.md
  PILOT=$IN/M7_PILOT_stub.md
  export M7_STUB_CUT_WHILE_THINKING="$PLANTCUT" M7_STUB_CHECK_FAIL="$PLANTCHK"
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
fail() { if [ $DRY = yes ]; then echo "DRY RUN FAILED: $1"; mkdir -p $L; echo "$1" >> $L/$N.failed; exit 1; fi
  hb "$N: STOPPED: $1" yes; mkdir -p $L; echo "$1" >> $L/$N.failed
  status STOPPED yes "$N stopped: $1; marker $L/$N.failed; models loaded by this chain may still be loaded"; echo "STOPPED: $1"; exit 1; }
refuse_late() { # the server gate after the chain started (another client appeared): exit 2, as at the gate; nothing is unloaded
  hb "$N: REFUSED: $1" yes; mkdir -p $L; echo "REFUSED: $1" >> $L/$N.failed
  status STOPPED yes "$N refused: $1; marker $L/$N.failed"; echo "REFUSED: $1" >&2; exit 2; }
planted() { [ "$FAILAFTER" != "$1" ] || fail "planted failure after the stage $1 (a dry run's rehearsal of a restart)"; }

# ---------------------------------------------------------------- 1. the gate (CPU; nothing is written before the checks pass, except $W itself)
for f in $ITEMS $RUBS $CC $FW/DF_MANIFEST.json $FW/packs.jsonl $FW/expansions.jsonl $PERFECT $PERFECT.manifest.json $IDS $TA_ITEMS $TA3_ITEMS $TA_ANS $TA_ANS.manifest.json $TA3_ANS $TA3_ANS.manifest.json \
         $CHUNKS $FROZEN $CATS/CATALOGUES.json runs/v3/INDEX_FROZEN.json kafka_sre_rag/prompts.py kafka_sre_rag/retrieval.py kafka_sre_rag/profiles.py kafka_sre_rag/verify_pass.py kafka_sre_rag/kb_lookup.py \
         kafka_sre_rag/evalhooks.py scripts/eval2.py scripts/eval2_judge.py scripts/eval2_idcheck.py scripts/eval2_score.py scripts/eval2_compare.py scripts/eval2_blind.py scripts/run_guarded.py \
         scripts/m5_g_rule.py scripts/m5_b4_served.py scripts/m6_d5_funnel.py scripts/m6_d5_rule.py scripts/m7_four_way.sh $R $PILOT; do
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
mid_of() { case $1 in qwen) echo "$Q4";; gemma26) echo "$MID_gemma26";; gemma31) echo "$MID_gemma31";; esac; }
marker_args() { # W2 (K4b): a model is ours only with a marker this chain wrote when it loaded it (before its first request): Qwen's 4-bit build, a Gemma model and the judge alike
  o="$DS=$W/loaded_judge"; for k in $MODELKEYS; do o="$o $(mid_of $k)=$W/loaded_$k"; done; echo $o; }
drop_marker() { # MODEL ID: the model was unloaded: its marker goes
  for k in $MODELKEYS; do [ "$(mid_of $k)" != "$1" ] || rm -f $W/loaded_$k; done; [ "$1" != "$DS" ] || rm -f $W/loaded_judge; return 0; }
server_gate() { # check|clear: before the first request (real runs): a request active or waiting, or a model without our marker loaded, refuses (exit 2, the model named); nothing of another client is unloaded.
  # Ours (W2, K4b): only a model this chain loaded, shown by its marker (Qwen's 4-bit build and the judge too: another client's copy of them is foreign). A model of ours that is not the one kept is unloaded.
  api /api/status > $SJSON 2>/dev/null || true
  gout=$(.venv/bin/python $R server-gate --status $SJSON --keep "$Q4" --marker $(marker_args) 2>&1) || { echo "$gout" >&2; return 2; }
  echo "$gout" | grep '^note:' || true
  [ "$1" = clear ] || return 0
  for m in $(echo "$gout" | sed -n 's/^UNLOAD //p'); do unload_now $m; drop_marker $m; done; return 0; }
others() { pgrep -fl 'scripts/(m4|m5|m6|m7|s1)_[a-z0-9_]*\.sh|run_guarded\.py' | grep -v "$N" || true; }
wait_others() { # before anything loads a reranker or a model: no other chain and no guarded job (real runs)
  [ $DRY = yes ] && return 0
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, m7_*.sh, s1_*.sh) and no guarded job runs"
  n=0; until [ -z "$(others)" ]; do sleep 60; n=$((n+1)); [ $n -lt 720 ] || fail "another chain or guarded job still runs after 12 h: $(others | head -3)"; done; }
allstages() { # the stages that need the server (or CPU, rettime): in the order of the chain
  s="probes rettime"
  for mo in $MODELORDER; do k=${mo%%:*}; x=; [ $k = qwen ] || x=_$k
    s="$s ans_a$x ans_e$x"
    if [ ${mo#*:} = yes ]; then s="$s ans_d$x"; if hasf $k; then s="$s ans_f$x"; fi; fi
    if [ $CHECKING = yes ] && { [ ${mo#*:} = yes ] || [ $k = qwen ]; }; then s="$s ans_c$x"; fi
    [ $k != qwen ] || s="$s ans_r"
  done; echo "$s judge"; }
MIDOUT=$(.venv/bin/python $R model-ids $MARGS) || refuse "the model ids could not be made"
MID_gemma26=$(echo "$MIDOUT" | sed -n 's/^gemma26=//p'); MID_gemma31=$(echo "$MIDOUT" | sed -n 's/^gemma31=//p')
if [ $DRY = yes ]; then
  Q4=${QID:-Qwen3.8-27B-4bit}
else
  MJSON=$(mktemp "${TMPDIR:-/tmp}/m7_six_models.XXXXXX")
  api /v1/models > $MJSON 2>/dev/null || true
  Q4=${QID:-${M7_Q4_MODEL:-$(.venv/bin/python scripts/eval2_compare.py pick-model --kind q4 < $MJSON)}} || refuse "the 4-bit model is not listed by the server (or ambiguous)"
  [ "$Q4" = "$FUNNEL_MODEL" ] || refuse "the 4-bit model on the server ($Q4) is not the one the D5 funnel's expansions were made with ($FUNNEL_MODEL)"
  NEEDIDS="$Q4 $DS"; for k in $MODELKEYS; do [ $k = qwen ] || NEEDIDS="$NEEDIDS $(mid_of $k)"; done
  .venv/bin/python $R models-check --models $MJSON --need $NEEDIDS || refuse "a model this chain needs is not listed by the server (the base models $(echo $NEEDIDS | sed "s/ $DS//") and the judge $DS)"
  SJSON=$(mktemp "${TMPDIR:-/tmp}/m7_six_status.XXXXXX")
  SN=no; for st in $(allstages); do [ -f $W/stage_$st.done ] || SN=yes; done
  if [ $SN = yes ]; then server_gate check || refuse "the server is not free for this chain (another client's model or request; see above)"; fi
fi

# the gate has passed: from here on the work folder and the log folder are written
mkdir -p $W $L
# the manifest and the start check
PINS="--pin $ITEMS --pin $RUBS --pin $CC --pin $FW/DF_MANIFEST.json --pin $FW/packs.jsonl --pin $FW/expansions.jsonl --pin $PERFECT --pin $PERFECT.manifest.json --pin $IDS \
  --pin $TA_ITEMS --pin $TA3_ITEMS --pin $TA_ANS --pin $TA_ANS.manifest.json --pin $TA3_ANS --pin $TA3_ANS.manifest.json --pin $CATS/CATALOGUES.json --pin runs/v3/INDEX_FROZEN.json --pin $PILOT"
for f in $(.venv/bin/python $R code-pins) $FROZEN; do     # the code the answer, check, judge, scorer and identifier-check paths import
  [ -f $f ] || refuse "pinned code file $f is missing"
  PINS="$PINS --pin $f"; done
GIT_HEAD=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)   # recorded for the reader, never compared
gstate() { # SUBCOMMAND [EXTRA...]: the manifest and the start check take the same state
  sub=$1; shift
  .venv/bin/python $R "$sub" --manifest $W/MANIFEST.json --work $W $PINS --frozen $FROZEN $DRYFLAG --opt "model=$Q4" --opt "check_variant=$VARIANT" --opt "think_cap=$CAP" \
    --opt "models=$MODELS" --opt "thinking_models=$THINK" --opt "thinking_mech=$THMECH" --opt "gemma26_id=$MID_gemma26" --opt "gemma31_id=$MID_gemma31" --opt "scenarios_per_session=$SPS" \
    --opt "check_variant_source=$VSRC" --opt "think_cap_source=$CSRC" --opt "bundle_seed=20261123" --opt "pilot_report=$PILOT" \
    --info "git_head=$GIT_HEAD" --info "max_unjudged=$MAXU" --judge-arg "model=$DS" --judge-arg "setting=$JSETTING" --judge-arg "extra_body=$JEB" --judge-arg "source_chars=$SRC" \
    --judge-arg "instruction=v2" --judge-arg "passes=1 2" --judge-arg "role=coverage" "$@"; }
SCOUT=$(mktemp "${TMPDIR:-/tmp}/m7_six_startcheck.XXXXXX")
if ! gstate start-check > $SCOUT 2>&1; then
  cat $SCOUT >&2
  # only an option that differs (a mistyped --think-cap, another variant): nothing to move, give the stored value; any other difference: the lines that move the stopped run aside
  if grep -q '^  options\.' $SCOUT && ! grep '^  ' $SCOUT | grep -vq '^  options\.'; then refuse "an option differs from the stored manifest (see above: give the stored value; nothing was moved or deleted)"; fi
  moveaside; refuse "the start check failed (see above; nothing was moved or deleted)"
fi
cat $SCOUT
# a rerun after a stop: the failed marker is removed after the start check (its text is shown); the stage markers decide what is done, so finished stages are kept
if [ -f $L/$N.failed ]; then echo "rerun after a stop; the earlier failure was: $(tail -n 1 $L/$N.failed)"; rm -f $L/$N.failed; fi
gstate manifest
mkdir -p "$(dirname $REPORT)"
recheck() { # NAME: the start check again before a stage, so that a change of a pinned file, an option or an output between stages stops the chain
  rcout=$(gstate start-check 2>&1) || { echo "$rcout" >&2; fail "the start check failed before the stage $1: something pinned changed since the chain started (see above)"; }; }

# ---------------------------------------------------------------- the plan and the estimate
ND=$(grep -c . $ITEMS | tr -d ' ')
est() { .venv/bin/python $R estimate --nd $ND --think-cap $CAP --check-variant $VARIANT $MARGS; }
NMA=96; NM3=20
CNT=$(.venv/bin/python $R call-counts --nd $ND --nma $NMA --nm3 $NM3 --check-variant $VARIANT --models $MODELS --thinking-models $THINK --thinking-mech $THMECH)
cnt() { echo "$CNT" | tr ' ' '\n' | sed -n "s/^$1=//p"; }
echo "planned: $ND D5 scenarios; models in the order of their answers: $MODELORDER (key:thinking); sets $(echo $SETNAMES | tr ' ' ',') (check variant $VARIANT, thinking cap $CAP); answer calls: $CNT; $(est) h of model-server time (inferred from the development check's rates, the plan's budget per answer, the cap and M4's load test for the other models' speed; none measured with thinking on these builds); CPU: the retrieval timing, a few minutes"

stage_done() { [ -f $W/stage_$1.done ]; }
mark() { st=$1; shift; .venv/bin/python $GR mark --work $W --stage $st --out "$@" > /dev/null; planted $st; }
SERVER_NEEDED=no
for st in $(allstages); do stage_done $st || SERVER_NEEDED=yes; done

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
    echo "**Models.** In the order of their answers (key:thinking arms): $MODELORDER; ids: qwen \`$Q4\`, gemma26 \`$MID_gemma26\`, gemma31 \`$MID_gemma31\` (the ids of the models that are not in the run are not used). A model's thorough arms run only when it is named in --thinking-models ($THINK)."
    echo
    echo "**Run parameters (from the pilot).** Checking-pass variant: \`$VARIANT\` (source: $VSRC). Thinking cap: $CAP tokens added to the 2,048-token answer limit (source: $CSRC). Pilot report: \`$([ $DRY = yes ] && echo 'a stand-in (dry run)' || echo $PILOT)\` (pinned by its sha256 in the manifest). Judged sets: $(echo $SETNAMES | sed 's/Fp/F-prime/g;s/ /, /g')."
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
  .venv/bin/python $R frozen-packs --funnel-work $FW --items $ITEMS --key r4g2 --out $W/packs_r4g2_d5.jsonl || fail "the D5 r4g2 pack file (arm R)"
  mark prepare $W/items_d5_h.jsonl $W/items_ma.jsonl $W/items_m3.jsonl $W/ref_ma.jsonl $W/ref_m3.jsonl $W/packs_frozen_d5.jsonl $W/packs_r4g2_d5.jsonl
fi
NMA=$(grep -c . $W/items_ma.jsonl | tr -d ' '); NM3=$(grep -c . $W/items_m3.jsonl | tr -d ' ')
[ "$NMA" = 96 ] && [ "$NM3" = 20 ] || fail "the mechanical sample has $NMA + $NM3 items; 96 + 20 expected"
echo "sets: D5 $ND, tier_a part $NMA, tier_a3 part $NM3"
[ $DRY = yes ] || [ "$ND" = 50 ] || fail "D5 has $ND scenarios; 50 expected"

# ---------------------------------------------------------------- the server, once
if [ $DRY = no ] && [ $SERVER_NEEDED = yes ]; then
  hb "$N: waiting until no other chain (scripts/m4_*.sh, m5_*.sh, m6_*.sh, m7_*.sh, s1_*.sh) and no guarded job runs, then the server check"
  status RUNNING no "$N waiting for other chains to end; markers $L/$N.done / .failed"
  rm -f $L/$N.done
  wait_others
  hb "$N: started; 4-bit model $Q4, arms $(echo $SETNAMES | tr ' ' ','), check variant $VARIANT, thinking cap $CAP: D5 ($ND) and the mechanical sample (116); judge $DS (coverage role, instruction v2, $JSETTING)"
  status RUNNING no "$N running: answers, judging, scoring; markers $L/$N.done / .failed"
  server_gate clear || refuse_late "the server is not free for this chain (another client's model or request appeared while it waited)"
  hb "$N: the server is idle and no model of another client is loaded; the 4-bit model loads on the first request"
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

# ---------------------------------------------------------------- answers (one request at a time; every model alone on the server, in the order gemma31, gemma26, qwen)
REALFLAG=""; [ $DRY = yes ] || REALFLAG="--real"
REDO_PASSES=$(.venv/bin/python -c 'import sys; sys.path.insert(0, "scripts"); import m7_six_arm_rule as r; print(r.REDO_PASSES, r.CHECK_ERROR_LIMIT)')
CHECK_LIMIT=${REDO_PASSES#* }; REDO_PASSES=${REDO_PASSES% *}
echo_check() { # the model the server echoed equals the first answers' of this run for this model (and the stored mechanical references' where this run replays one: Qwen only)
  .venv/bin/python $GR check-echo --stored $ECHOREF $echo_extra --new $out.manifest.json $( [ $DRY = yes ] && echo --dry ) \
    || fail "answers $config: the server echoed another model than the first answers of this run for this model (the answer sets are not comparable)"; }
answer_run() { # OUT ITEMS REPLAY EXPECT CONFIG "PROFILE ARGS" [STORED_MANIFEST] [MODE]: all items, one request at a time. A row whose checking call ended with status error is redone (REDO_PASSES passes; the count is kept in OUT.redo)
  out=$1; items=$2; replay=$3; expect=$4; config=$5; pargs=$6; echo_extra=$7; amode=${8:-default}
  CK=yes; case "$pargs" in *--no-verify*) CK=no;; esac
  redo=$(cat $out.redo 2>/dev/null || echo 0); bad=0
  while :; do
    if .venv/bin/python $GR check-answers --file $out --expect $expect --mode $amode > $W/check.out 2>&1 && .venv/bin/python $R check-rows --file $out --expect $expect $REALFLAG >> $W/check.out 2>&1; then
      echo "answers $config: complete: $(cat $W/check.out | tr '\n' ' ')"
      [ $CK = yes ] || break
      nerr=$(.venv/bin/python $R check-errors --file $out --kind answers | sed -n 's/^check_errors=//p')
      [ "$nerr" != 0 ] || break
      if [ $redo -ge $REDO_PASSES ]; then echo "answers $config: $nerr checks still have status error after $redo redo passes; the rule marks the arm if there are more than $CHECK_LIMIT"; break; fi
      redo=$((redo+1)); echo $redo > $out.redo
      .venv/bin/python $R check-errors --file $out --kind answers --drop > /dev/null
      echo "answers $config: redo pass $redo of the $nerr rows whose checking call failed (the rows are kept in $out.redo_dropped.jsonl)"
    else
      [ $bad -lt 3 ] || fail "answers $config: incomplete, with errors, empty, without a pack or without a whole profile row after $bad passes ($out)"
      bad=$((bad+1))
    fi
    [ $DRY = yes ] || idle
    hb "$N: answers $config started ($expect items; $pargs; mode $amode)"
    $G --stall-min 40 --log $L/$N.$config.log -- .venv/bin/python scripts/eval2.py answer --items $items --out $out --config $config $GA \
      --extra-body "$EB" --mode $amode --replay-packs $replay --catalogue $CATS --host-class "Mac Studio" --allow-commit-change --answer-profile thorough $pargs \
      || fail "answers $config: failed (see $L/$N.$config.log)"
  done
  .venv/bin/python $GR packs-equal --a $replay --b $out || fail "answers $config: the packs differ from the pack file (replay failed)"
  echo_check
}
PA="--no-thinking --no-verify"                                     # arms A, E and R: today's call on the profile's transport
case $VARIANT in
  V1) PD="--think-cap $CAP --no-cards";; V1C) PD="--think-cap $CAP";; none) PD="--think-cap $CAP --no-verify";;
esac                                                               # arms D and F
if [ $CHECKING = yes ]; then CHK_EXPECT_D5=$ND; CHK_EXPECT_M=$((NMA + NM3)); else CHK_EXPECT_D5=0; CHK_EXPECT_M=0; fi
calls() { # STAGE EXPECT_DRAFTS EXPECT_CHECKS FILES...   the model calls the set made against the expected count (strict in a dry run)
  st=$1; ed=$2; ec=$3; shift 3
  .venv/bin/python $R stage-calls --stage $st --files "$@" --expect-drafts $ed --expect-checks $ec $([ $DRY = yes ] && [ -z "$PLANTCUT" ] && echo --strict) || fail "the calls of the stage $st do not match the expected count"; }
pc() { "$@" >> $PCOUT 2>&1 || PCFAIL=1; }
pcheck_model() { # KEY THINKING (PCOUT, PCFAIL): the checks of one model's sets: every draft prompt hash is the default prompt on the row's own pack; the arms differ only in their switch; E (and F) differ from A (and D) only by the pack
  k=$1; mt=$2; x=; [ $k = qwen ] || x=_$k
  if [ $mt = yes ]; then if hasf $k; then sl="A D E F"; else sl="A D E"; fi; else sl="A E"; fi
  for s in $sl; do pc .venv/bin/python $R prompts-default --items $ITEMS --answers $W/ans_d5_$s$x.jsonl; done
  pc .venv/bin/python $R prompts-relation --a $W/ans_d5_A$x.jsonl --b $W/ans_d5_E$x.jsonl
  pc .venv/bin/python $GR packs-equal --a $W/packs_frozen_d5.jsonl --b $W/ans_d5_A$x.jsonl
  pc .venv/bin/python $GR packs-equal --a $PERFECT --b $W/ans_d5_E$x.jsonl
  if [ $mt = yes ]; then
    for part in ma m3; do
      if hasm $k; then
        for s in A D; do pc .venv/bin/python $R prompts-default --items $W/items_$part.jsonl --answers $W/ans_${part}_$s$x.jsonl; done
        pc .venv/bin/python $R prompts-relation --a $W/ans_${part}_A$x.jsonl --b $W/ans_${part}_D$x.jsonl --expect-same-pack
      else pc .venv/bin/python $R prompts-default --items $W/items_$part.jsonl --answers $W/ans_${part}_A$x.jsonl; fi
    done
    pc .venv/bin/python $R prompts-relation --a $W/ans_d5_A$x.jsonl --b $W/ans_d5_D$x.jsonl --expect-same-pack
    # the request bodies of the drafts: A and D (and E and F) differ in the thinking switch and the token limit (plus the cap) only; A and E (D and F) in the messages only, where the packs differ
    for part in d5 ma m3; do
      if [ $part = d5 ] || hasm $k; then pc .venv/bin/python $R bodies-relation --a $W/ans_${part}_A$x.jsonl --b $W/ans_${part}_D$x.jsonl --thinking-differs --think-cap $CAP; fi
    done
    if hasf $k; then
      pc .venv/bin/python $R prompts-relation --a $W/ans_d5_E$x.jsonl --b $W/ans_d5_F$x.jsonl --expect-same-pack
      pc .venv/bin/python $R prompts-relation --a $W/ans_d5_D$x.jsonl --b $W/ans_d5_F$x.jsonl
      pc .venv/bin/python $R bodies-relation --a $W/ans_d5_E$x.jsonl --b $W/ans_d5_F$x.jsonl --thinking-differs --think-cap $CAP
      pc .venv/bin/python $R bodies-relation --a $W/ans_d5_D$x.jsonl --b $W/ans_d5_F$x.jsonl
    fi
  else
    for part in ma m3; do pc .venv/bin/python $R prompts-default --items $W/items_$part.jsonl --answers $W/ans_${part}_A$x.jsonl; done
  fi
  pc .venv/bin/python $R bodies-relation --a $W/ans_d5_A$x.jsonl --b $W/ans_d5_E$x.jsonl
  return 0; }
cross_pc() { # OTHER_SFX THIS_SFX: two models' A and E are the same call: the same prompt on the same pack, and the request body equal but for the model's name (K4b: the model-free body hash)
  for s in A E; do
    pc .venv/bin/python $R prompts-relation --a $W/ans_d5_$s$1.jsonl --b $W/ans_d5_$s$2.jsonl --expect-same-pack
    pc .venv/bin/python $R bodies-relation --a $W/ans_d5_$s$1.jsonl --b $W/ans_d5_$s$2.jsonl --across-models
  done
  for part in ma m3; do pc .venv/bin/python $R bodies-relation --a $W/ans_${part}_A$1.jsonl --b $W/ans_${part}_A$2.jsonl --across-models; done
  return 0; }
use_model() { # KEY: the model of the answers that follow (SFX is the suffix of its files and stages; empty for qwen)
  mk=$1; MID=$(mid_of $mk); SFX=; [ $mk = qwen ] || SFX=_$mk
  if [ $DRY = yes ]; then GA="--backend stub --no-rerank"; VA="--dry-run"; else GA="--backend openai --model $MID"; VA="--backend openai --model $MID"; fi
  ECH_MA=; ECH_M3=; if [ $mk = qwen ]; then ECH_MA="--stored $TA_ANS.manifest.json"; ECH_M3="--stored $TA3_ANS.manifest.json"; fi   # the stored mechanical references are Qwen's answers
  return 0; }
prepare_model() { # KEY (real runs): only models of this chain may be loaded when this model's first request comes (the others of ours are unloaded); a model of another client stops the chain (exit 2, nothing unloaded)
  [ $DRY = yes ] && return 0
  mk=$1; mid=$(mid_of $mk)
  idle
  api /api/status > $SJSON 2>/dev/null || true
  gout=$(.venv/bin/python $R server-gate --status $SJSON --keep "$mid" --marker $(marker_args) 2>&1) || { echo "$gout" >&2; refuse_late "the server is not free for the model $mid (a model without this chain's marker is loaded, or a request is active or waiting)"; }
  echo "$gout" | grep '^note:' || true
  for m in $(echo "$gout" | sed -n 's/^UNLOAD //p'); do unload_now $m; drop_marker $m; done
  touch $W/loaded_$mk     # the marker: this chain loads the model on its first request (W2)
  hb "$N: the server is idle and holds no model of another client; $mid loads on its first request (alone, after the other models of this chain were unloaded)"
  return 0; }
probe_model() { # KEY THINKING (real runs, models other than qwen; at the gate, before any answer, with the model loaded alone): two short requests, switch off and switch on (K4b)
  # off must show no thinking (A and E are answered with it off) and a non-empty content; on must show thinking in a form the stream reader handles (a reasoning field or a <think> pair).
  # A model that thinks in this run and fails the "on" probe gets no thinking arm: the chain stops here, before any answer (remove it from --thinking-models); for the other models the result is only reported.
  [ $DRY = yes ] && return 0
  [ $1 != qwen ] || return 0
  [ ! -s $W/probe_$1.done ] || { echo "skip the thinking probe of $1: done"; return 0; }
  for sw in false true; do
    idle
    curl -s -m 900 -X POST -H "Authorization: Bearer $OMLX_API_KEY" -H 'Content-Type: application/json' \
      -d "{\"model\": \"$MID\", \"messages\": [{\"role\": \"user\", \"content\": \"What is 17 times 23? Reply with the number only.\"}], \"max_tokens\": 1500, \"temperature\": 0, \"chat_template_kwargs\": {\"enable_thinking\": $sw}}" \
      "$BASE/v1/chat/completions" > $W/probe_$1_$sw.json 2>/dev/null || fail "the thinking probe of $1 (switch $sw): no reply from the server"
    want=off; [ $sw = false ] || want=on
    pline=$(.venv/bin/python $R thinking-probe --file $W/probe_$1_$sw.json --expect $want 2>&1) && pok=yes || pok=no
    echo "$1 ($MID): $pline" | tee -a $W/probes.txt
    if [ $pok = no ]; then
      if [ $sw = false ]; then fail "the thinking switch of $1 is not honoured with the switch off (see $W/probe_$1_$sw.json): its answers would not be comparable"; fi
      if [ $2 = yes ]; then fail "the thinking probe of $1 failed with the switch on (see $W/probe_$1_$sw.json): it has no thinking arm: remove $1 from --thinking-models (nothing has been answered yet; the start check refuses an option change, so move the run aside, see the mv lines of a refused start)"; fi
      echo "$1: the probe with the switch on did not show thinking in a readable form; no thinking arm is planned for it (reported only)" | tee -a $W/probes.txt
    fi
  done
  echo done > $W/probe_$1.done
  hb "$N: the thinking switch of $1 ($MID) was probed before any answer (off, on)"
  return 0; }
finish_model() { # KEY (real runs): the model is unloaded after its stages (it was loaded by this chain)
  [ $DRY = yes ] && return 0
  unload $MID; rm -f $W/loaded_$1
  hb "$N: the answers of $1 are done; $MID unloaded"
  return 0; }

model_group() { # KEY THINKING(yes|no): the stages of one model, D F A C E (R for qwen), in this order (D and F before A and E: the server's prompt cache cannot make D look faster than it would be in use)
  use_model $1; mt=$2
  gs="ans_a$SFX ans_e$SFX"; if [ $mt = yes ]; then gs="$gs ans_d$SFX"; if hasf $mk; then gs="$gs ans_f$SFX"; fi; fi; { [ $CHECKING = no ] || { [ $mt = no ] && [ $mk != qwen ]; }; } || gs="$gs ans_c$SFX"; [ $mk != qwen ] || gs="$gs ans_r"
  gneed=no; for st in $gs; do stage_done $st || gneed=yes; done
  [ $gneed = yes ] || return 0
  prepare_model $mk
  if [ $mt = yes ]; then ECHOREF=$W/ans_d5_D$SFX.jsonl.manifest.json; else ECHOREF=$W/ans_d5_A$SFX.jsonl.manifest.json; fi     # the first answers of this run for this model

  if [ $mt = yes ] && ! stage_done ans_d$SFX; then
    recheck ans_d$SFX
    answer_run $W/ans_d5_D$SFX.jsonl $W/items_d5_h.jsonl $W/packs_frozen_d5.jsonl $ND m7-six-d5-D$SFX "$PD"
    if hasm $mk; then
      answer_run $W/ans_ma_D$SFX.jsonl $W/items_ma.jsonl $TA_ANS $NMA m7-six-ma-D$SFX "$PD" "$ECH_MA"
      answer_run $W/ans_m3_D$SFX.jsonl $W/items_m3.jsonl $TA3_ANS $NM3 m7-six-m3-D$SFX "$PD" "$ECH_M3"
      calls ans_d$SFX $((ND + NMA + NM3)) $((CHK_EXPECT_D5 + CHK_EXPECT_M)) $W/ans_d5_D$SFX.jsonl $W/ans_ma_D$SFX.jsonl $W/ans_m3_D$SFX.jsonl
      mark ans_d$SFX $W/ans_d5_D$SFX.jsonl $W/ans_ma_D$SFX.jsonl $W/ans_m3_D$SFX.jsonl
    else     # --thinking-mech no: this model's thinking arm leaves the mechanical items out
      calls ans_d$SFX $ND $CHK_EXPECT_D5 $W/ans_d5_D$SFX.jsonl
      mark ans_d$SFX $W/ans_d5_D$SFX.jsonl
    fi
    hb "$N: arm D answers of $mk done (thinking$([ $CHECKING = yes ] && echo ', then the checking pass'); D5 $ND, tier_a part $NMA, tier_a3 part $NM3)"
  fi

  if [ $mt = yes ] && hasf $mk && ! stage_done ans_f$SFX; then
    recheck ans_f$SFX
    answer_run $W/ans_d5_F$SFX.jsonl $W/items_d5_h.jsonl $PERFECT $ND m7-six-d5-F$SFX "$PD"
    calls ans_f$SFX $ND $CHK_EXPECT_D5 $W/ans_d5_F$SFX.jsonl
    mark ans_f$SFX $W/ans_d5_F$SFX.jsonl
    hb "$N: arm F answers of $mk done (perfect packs, thinking$([ $CHECKING = yes ] && echo ', then the checking pass'); D5 $ND)"
  fi

  if ! stage_done ans_a$SFX; then
    recheck ans_a$SFX
    answer_run $W/ans_d5_A$SFX.jsonl $W/items_d5_h.jsonl $W/packs_frozen_d5.jsonl $ND m7-six-d5-A$SFX "$PA" ""
    answer_run $W/ans_ma_A$SFX.jsonl $W/items_ma.jsonl $TA_ANS $NMA m7-six-ma-A$SFX "$PA" "$ECH_MA"
    answer_run $W/ans_m3_A$SFX.jsonl $W/items_m3.jsonl $TA3_ANS $NM3 m7-six-m3-A$SFX "$PA" "$ECH_M3"
    calls ans_a$SFX $((ND + NMA + NM3)) 0 $W/ans_d5_A$SFX.jsonl $W/ans_ma_A$SFX.jsonl $W/ans_m3_A$SFX.jsonl
    mark ans_a$SFX $W/ans_d5_A$SFX.jsonl $W/ans_ma_A$SFX.jsonl $W/ans_m3_A$SFX.jsonl
    hb "$N: arm A answers of $mk done (D5 $ND, tier_a part $NMA, tier_a3 part $NM3)"
  fi

  # C: the checking pass on A's D5 drafts (this model, thinking off per call). A record whose checking call ended with status error is redone (REDO_PASSES passes)
  if { [ $mt = yes ] || [ $mk = qwen ]; } && [ $CHECKING = yes ] && ! stage_done ans_c$SFX; then
    recheck ans_c$SFX
    bad=0; redo=$(cat $W/check_d5_C$SFX.jsonl.redo 2>/dev/null || echo 0)
    while :; do
      if .venv/bin/python $R check-verify --file $W/check_d5_C$SFX.jsonl --answers $W/ans_d5_A$SFX.jsonl --expect $ND; then
        nerr=$(.venv/bin/python $R check-errors --file $W/check_d5_C$SFX.jsonl --kind verify | sed -n 's/^check_errors=//p')
        [ "$nerr" != 0 ] && [ $redo -lt $REDO_PASSES ] || { [ "$nerr" = 0 ] || echo "the checking pass on A's drafts ($mk): $nerr checks still have status error after $redo redo passes; the rule marks the arm if there are more than $CHECK_LIMIT"; break; }
        redo=$((redo+1)); echo $redo > $W/check_d5_C$SFX.jsonl.redo
        .venv/bin/python $R check-errors --file $W/check_d5_C$SFX.jsonl --kind verify --drop > /dev/null
        echo "the checking pass on A's drafts ($mk): redo pass $redo of the $nerr records whose checking call failed (kept in $W/check_d5_C$SFX.jsonl.redo_dropped.jsonl)"
      else
        [ $bad -lt 3 ] || fail "the checking pass on A's drafts ($mk) is incomplete or has errors after 3 passes ($W/check_d5_C$SFX.jsonl)"
        bad=$((bad+1))
      fi
      [ $DRY = yes ] || idle
      hb "$N: the checking pass on A's drafts of $mk started ($ND answers; variant $VARIANT)"
      $G --stall-min 40 --log $L/$N.check_C$SFX.log -- .venv/bin/python -m kafka_sre_rag.verify_pass run --answers $W/ans_d5_A$SFX.jsonl --items $ITEMS --out $W/check_d5_C$SFX.jsonl \
        --catalogues $CATS $VA --extra-body "$EB" $([ $VARIANT = V1C ] && echo --cards) || fail "the checking pass on A's drafts ($mk) failed (see $L/$N.check_C$SFX.log)"
    done
    calls ans_c$SFX 0 $ND $W/check_d5_C$SFX.jsonl --kind checks
    mark ans_c$SFX $W/check_d5_C$SFX.jsonl
    hb "$N: the checking pass on A's drafts of $mk done (arm C)"
  fi

  if ! stage_done ans_e$SFX; then
    recheck ans_e$SFX
    answer_run $W/ans_d5_E$SFX.jsonl $W/items_d5_h.jsonl $PERFECT $ND m7-six-d5-E$SFX "$PA"
    calls ans_e$SFX $ND 0 $W/ans_d5_E$SFX.jsonl
    mark ans_e$SFX $W/ans_d5_E$SFX.jsonl
    hb "$N: arm E answers of $mk done (perfect packs, D5 $ND)"
  fi

  # R (W7): grounded-g3 on the funnel's r4g2 packs, A's call otherwise; judged only, outside every decided row
  if [ $mk = qwen ] && ! stage_done ans_r; then
    recheck ans_r
    answer_run $W/ans_d5_R.jsonl $W/items_d5_h.jsonl $W/packs_r4g2_d5.jsonl $ND m7-six-d5-R "$PA" "" grounded-g3
    calls ans_r $ND 0 $W/ans_d5_R.jsonl
    mark ans_r $W/ans_d5_R.jsonl
    hb "$N: arm R answers done (grounded-g3 on the r4g2 packs, D5 $ND)"
  fi
  # K4b: the prompt check of this model's sets right after its stages, against the model answered before it (CPU), so that a mismatch stops the chain before the next model loads
  PCOUT=$W/prompts_check_$mk.txt; : > $PCOUT; PCFAIL=0
  pcheck_model $mk $mt
  [ -z "$PREVK" ] || cross_pc "$PREVSFX" "$SFX"
  [ $PCFAIL = 0 ] || { cat $PCOUT; fail "the prompt check of the model $mk failed right after its answers: its prompts or request bodies are not what the arms promise, or differ from the model before it (see above)"; }
  echo "prompt check of $mk (right after its answers): $(wc -l < $PCOUT | tr -d ' ') lines, no failure"
  finish_model $mk
  return 0
}
# the thinking probes at the gate: every model other than Qwen, alone, before any answer (K4b). Qwen's switch is the pilot's measured fact (it is not probed here: a thinking probe could use its whole limit)
if ! stage_done probes; then
  recheck probes
  if [ $DRY = yes ]; then echo "dry run: no model is loaded and no thinking switch is probed" > $W/probes.txt
  else
    : >> $W/probes.txt
    for mo in $MODELORDER; do pk=${mo%%:*}; [ $pk != qwen ] || continue
      use_model $pk; prepare_model $pk; probe_model $pk ${mo#*:}; finish_model $pk
    done
    echo "qwen: not probed here (its thinking behaviour is the pilot's: $PILOT)" >> $W/probes.txt
  fi
  mark probes $W/probes.txt
  hb "$N: thinking probes done (every model other than Qwen, before any answer): $(tr '\n' ' ' < $W/probes.txt | cut -c1-300)"
fi
PREVK=; PREVSFX=
for mo in $MODELORDER; do model_group ${mo%%:*} ${mo#*:}; PREVK=${mo%%:*}; PREVSFX=; [ $PREVK = qwen ] || PREVSFX=_$PREVK; done

# ---------------------------------------------------------------- derived sets (B and F' from the thorough rows, C's answers from A's rows) and the prompt check (CPU)
if ! stage_done derive; then
  DOUT=""
  : > $W/prompts_check.txt
  PCFAIL=0; PCOUT=$W/prompts_check.txt
  for mo in $MODELORDER; do
    k=${mo%%:*}; mt=${mo#*:}; x=; [ $k = qwen ] || x=_$k
    if [ $mt = yes ] && [ $CHECKING = yes ]; then
      .venv/bin/python $R derive-draft --src $W/ans_d5_D$x.jsonl --out $W/ans_d5_B$x.jsonl --arm B || fail "arm B of $k could not be derived from D's rows"
      .venv/bin/python $R derive-draft --src $W/ans_d5_F$x.jsonl --out $W/ans_d5_Fp$x.jsonl --arm Fp || fail "arm F-prime of $k could not be derived from F's rows"
      DOUT="$DOUT $W/ans_d5_B$x.jsonl $W/ans_d5_Fp$x.jsonl"
    fi
    if [ $CHECKING = yes ] && { [ $mt = yes ] || [ $k = qwen ]; }; then
      .venv/bin/python $R derive-check --answers $W/ans_d5_A$x.jsonl --checks $W/check_d5_C$x.jsonl --out $W/ans_d5_C$x.jsonl || fail "arm C of $k could not be derived from A's rows and the check records"
      DOUT="$DOUT $W/ans_d5_C$x.jsonl"
    fi
    pcheck_model $k $mt
    if [ $k != qwen ]; then cross_pc "" "$x"; fi     # the same prompt on the same pack and the same body (but for the model's name) as Qwen's A and E (the other models get today's call)
  done
  # arm R (W7): the prompt of grounded-g3 on its pack, different from the default prompt; the packs are the funnel's r4g2; the body is A's except the messages
  pc .venv/bin/python $R6 prompts-check --items $ITEMS --answers $W/ans_d5_R.jsonl --mode grounded-g3
  pc .venv/bin/python $GR packs-equal --a $W/packs_r4g2_d5.jsonl --b $W/ans_d5_R.jsonl
  pc .venv/bin/python $R bodies-relation --a $W/ans_d5_A.jsonl --b $W/ans_d5_R.jsonl --messages-differ
  [ $PCFAIL = 0 ] || { cat $W/prompts_check.txt; fail "the prompt check failed: an arm answered with another prompt than its own on its pack, or the arms differ in more than their switch (see above)"; }
  cat $W/prompts_check.txt
  for s in $SETNAMES; do
    stem=$(stem_of $s)
    .venv/bin/python $GR check-answers --file $W/ans_d5_$stem.jsonl --expect $ND --mode $([ $s = R ] && echo grounded-g3 || echo default) > /dev/null || fail "the answer set $s ($W/ans_d5_$stem.jsonl) is not whole"
  done
  mark derive $W/prompts_check.txt $DOUT
fi

# ---------------------------------------------------------------- 6. judge: DeepSeek, coverage role, two passes, the judged D5 answer sets
SETS=""
for s in $SETNAMES; do SETS="$SETS $s=$W/ans_d5_$(stem_of $s).jsonl"; done
if ! stage_done judge; then
  recheck judge
  if [ $DRY = no ]; then
    for k in $MODELKEYS; do if [ -e $W/loaded_$k ]; then unload $(mid_of $k); rm -f $W/loaded_$k; fi; done     # only the models this chain loaded: a marker (W2, K4b)
    idle
    api /api/status > $SJSON 2>/dev/null || true
    gout=$(.venv/bin/python $R server-gate --status $SJSON --keep "$DS" --marker $(marker_args) 2>&1) || { echo "$gout" >&2; refuse_late "the server is not free for the judge $DS (a model without this chain's marker is loaded, or a request is active or waiting)"; }
    echo "$gout" | grep '^note:' || true
    for m in $(echo "$gout" | sed -n 's/^UNLOAD //p'); do unload_now $m; drop_marker $m; done
    touch $W/loaded_judge     # the marker: this chain loads the judge on its first request
    hb "$N: answering finished, the base models of this chain unloaded; the judge $DS loads on the first request"
  fi
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
    else
      # K4b: Qwen's decided sets must be complete; a described set (R, another model's) may have missing or invalid rows: it is listed and left out of the comparisons
      .venv/bin/python $R judge-sets --file $jout --n $ND --decided $DECSETS --described $DESCSETS > $W/judge_sets.txt || { cat $W/judge_sets.txt; fail "judging d5: a decided set (Qwen's) does not hold a valid row for every scenario and pass ($jout; see $W/judge_sets.txt)"; }
      cat $W/judge_sets.txt
    fi
  fi
  JROWS=$(grep -c . $jout | tr -d ' ')
  [ "$JROWS" = "$calls" ] || fail "judge calls: $JROWS rows found, $calls expected"
  echo "calls judge: $JROWS judge rows (expected $calls = $ND scenarios x $NSETS sets x 2 passes)"
  if [ $DRY = no ]; then unload $DS; rm -f $W/loaded_judge; hb "$N: judging finished, $DS unloaded"; fi
  mark judge $W/judge_d5.jsonl
fi

# ---------------------------------------------------------------- 7. identifier check (D5, every set) and the scorer (mechanical, A and D) (CPU)
if ! stage_done checks; then
  OUTS=""
  for s in $SETNAMES; do
    stem=$(stem_of $s)
    .venv/bin/python scripts/eval2_idcheck.py --answers $W/ans_d5_$stem.jsonl --catalogues $CATS --totals-only > $W/idcheck_d5_$s.txt || fail "identifier check on d5 $s failed"
    echo "idcheck d5 $s: $(sed -n 4p $W/idcheck_d5_$s.txt)"
    OUTS="$OUTS $W/idcheck_d5_$s.txt"
  done
  for mo in $MODELORDER; do
    k=${mo%%:*}; x=; [ $k = qwen ] || x=_$k
    if [ ${mo#*:} = yes ] && hasm $k; then vs="A D"; else vs="A"; fi
    for grp in ma m3; do
      for v in $vs; do
        .venv/bin/python scripts/eval2_score.py --items $W/items_$grp.jsonl --answers $W/ans_${grp}_$v$x.jsonl --catalogues $CATS --scores $W/score_${grp}_$v$x.jsonl \
          --summary $W/score_${grp}_$v$x.summary.json > $W/score_${grp}_$v$x.out 2> $W/score_${grp}_$v$x.err || fail "scorer on $grp $v ($k) failed (see $W/score_${grp}_$v$x.err)"
        echo "score $grp $v $k: $(.venv/bin/python -c 'import json,sys; d=json.load(open(sys.argv[1])); print("all-pass", d["all_pass"], "of", d["items_scored"], "format failures", d["format_failures"], "missing answers", d["missing_answers"])' $W/score_${grp}_$v$x.summary.json)"
        OUTS="$OUTS $W/score_${grp}_$v$x.jsonl"
      done
    done
  done
  mark checks $OUTS
fi

# ---------------------------------------------------------------- 8. the measures, the reading prep, 9. the rule and the report
recheck measures
.venv/bin/python $R measures --work $W --ids $IDS --d5-rubrics $RUBS --d5-items $ITEMS --funnel-work $FW --check-variant $VARIANT --think-cap $CAP --max-unjudged $MAXU $MARGS \
  $([ $DRY = yes ] && echo --stub) > $W/measures.out || { tail -3 $W/measures.out; fail "the measures could not be computed (a missing input is an error, see above)"; }
RCOPY=$W/reading_for_readers; [ $DRY = yes ] || RCOPY=$(dirname $W)/$(basename $W)_reading     # a copy of the bundles for the readers, outside the work folder: no key, no answer file
if ! stage_done reading; then
  RD=$W/reading; mkdir -p $RD
  # Qwen's A, D, E, F (under none A, B, E, F-prime), each other model's A and E and its D when it ran; all the answers of a scenario in one bundle under blind names
  RSETS=$(.venv/bin/python $R reading-sets --check-variant $VARIANT --work $W $MARGS | tr '\n' ' ') || fail "the sets of the reading could not be listed"
  rm -rf $RD/first $RD/second $RD/KEY.json $RD/sessions.json $RCOPY
  # K4b: an answer with no draft (cut while thinking) is left out of its scenario's bundle, not the whole scenario: the bundle holds the answers that exist under random letters; the key lists them
  .venv/bin/python $R no-draft-ids --answers $RSETS --sets-by-item $RD/sets_by_item.json > /dev/null 2> $RD/no_draft.err || fail "the scan for answers with no draft failed"
  cat $RD/no_draft.err
  .venv/bin/python scripts/eval2_blind.py bundle --items $ITEMS --rubrics $RUBS --answers $RSETS --chunks $CHUNKS --out $RD/first --key $RD/KEY.json --seed 20261123 \
    --second-reader 0.25 --second-out $RD/second --sets-by-item $RD/sets_by_item.json --harm-consequences > $RD/bundle.out || fail "the blind bundles could not be made (fewer than two usable answers in every scenario?)"
  tail -3 $RD/bundle.out
  .venv/bin/python $R no-draft-ids --answers $RSETS --key $RD/KEY.json > /dev/null 2>&1 || fail "the no-draft answers could not be recorded in the key"
  .venv/bin/python $R reading-summary --key $RD/KEY.json | tee $RD/reading_summary.txt
  .venv/bin/python $R reading-plan --key $RD/KEY.json --out $RD/sessions.json --scenarios-per-session $SPS || fail "the reading plan could not be made"
  # the key is written outside every bundle folder; nothing of the configuration is in a bundle (eval2_blind.py); no reading is done here
  LEAKT=$Q4; for k in $MODELKEYS; do LEAKT="$LEAKT $(mid_of $k)"; done
  .venv/bin/python $R leak-check --dir $RD --terms $LEAKT || fail "a bundle names a configuration (a switch, the profile, the notice of a cut draft or a base model)"
  mkdir -p $RCOPY && cp -R $RD/first $RD/second $RD/sessions.json $RCOPY/
  .venv/bin/python $R leak-check --dir $RCOPY --terms $LEAKT || fail "the readers' copy names a configuration"
  echo "bundles for the readers (no key): $RCOPY"
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
  echo "**Knowledge-base rows by the rule; the thorough-profile row waits for the blind reading (wrong statements and harm by consequence); time is reported, not decided; the model comparison is described, not decided.**"
  echo
  cat $W/measures.md
  echo
  echo "### The rule applied by code (\`$R rule\`)"
  echo
  echo '```'; cat $W/rule_out.txt; echo '```'
  echo
  echo "### Thinking-switch probes (before any answer; the result is reported for every model other than Qwen)"
  echo
  echo '```'; cat $W/probes.txt; echo '```'
  echo
  echo "### Judge rows per set (a described set may be incomplete and is then left out of the comparisons)"
  echo
  echo '```'; cat $W/judge_sets.txt 2>/dev/null || echo "(all sets valid, or a dry run's stub judge)"; echo '```'
  echo
  echo "### The reading: scenarios read per set"
  echo
  echo '```'; cat $W/reading/reading_summary.txt 2>/dev/null || echo "(not prepared)"; echo '```'
  echo
  echo "### The prompt check and the call counts"
  echo
  echo '```'; cat $W/prompts_check.txt; echo '```'
  echo
  echo "### What this report does not show"
  echo
  echo "Wrong, invented and harmful statements, and the consequence of every harmful one (blind readings of Qwen's A, D, E and F, each other model's A and E and its D, or B under none, when it ran wait; \`$R thorough-rows --measures $W/measures.json --findings F --reads R --first-reader NAME --second-reader NAME\` applies each thinking Gemma model's thorough row with the findings; with the findings, \`$R harm-match\` matches the harm by consequence and \`$R compare-models\` prints the whole comparison with the reading's parts; a new harmful consequence goes to the second reader beyond the quarter: \`scripts/eval2_blind.py extra-second --key $W/reading/KEY.json --items ITEMS --rubrics RUBRICS --answers NAME=PATH... --out DIR --ids ID...\` writes a further bundle with the same blind names and widens the key's second-reader scope (\`reading-sets\` lists the NAME=PATH pairs), and \`scripts/eval2_blind.py merge --findings F1 F2 --reads R1 R2 --out F --reads-out R\` joins the two unblinded files for \`harm-match\`: the blind names stay the same: the readers' copy of the bundles, without the key, in \`$RCOPY\`; the work folder's bundles in \`$W/reading/first\`, the second reader's quarter in \`$W/reading/second\`, the plan in \`$W/reading/sessions.json\`, the key, which lists the scenarios left out because an answer had no draft, in \`$W/reading/KEY.json\`, which no reader opens). A \"build\" of the knowledge base does not settle (b): decision V8 leaves the writers and the licences to the Owner; the deadline of the profile counts from the call, not from the question (the replay has no search); the 4-bit build's honouring of the thinking switch is seen in the rows' reasoning tokens, not assumed. Development data only (D5 is worn after M6); the hidden part is not involved."
  echo
  echo "### Files"
  echo
  echo "Answers \`$W/ans_*.jsonl\` (B, C and F' derived: their \`derived_from\` field names the source; another model's files carry its key: \`_gemma26\`, \`_gemma31\`; arm R in \`ans_d5_R.jsonl\`); check records \`$W/check_d5_C*.jsonl\`; packs \`$W/packs_frozen_d5.jsonl\`, \`$W/packs_r4g2_d5.jsonl\` and \`$PERFECT\`; judge results \`$W/judge_d5.jsonl\`; identifier checks \`$W/idcheck_d5_*.txt\`; scores \`$W/score_*.jsonl\`; measures \`$W/measures.json\`."
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
