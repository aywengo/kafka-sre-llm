#!/bin/sh
# Run one headless Cursor agent worker on a Grok model (M7 decision W23, same limits as Codex, W22).
#
#   .claude/cursor-worker.sh <ro|rw> <prompt-file> <workspace-dir> [model]   # answer on stdout
#
# The worker sees only <workspace-dir>: a folder the conductor fills with the files the task needs
# (public doc chunks, D7 drafts, a brief). It never runs in the repository itself, so it cannot reach
# hidden sets, B4, the ledger or keys. ro = Cursor's read-only "ask" mode; rw = may edit files and run
# commands inside the workspace.
# Default model: grok-4.7-high (Owner's choice: Grok models in Cursor). Override with the 4th argument
# or CURSOR_WORKER_MODEL; only grok models are accepted.
set -eu
MODE=$1; PROMPT=$2; WS=$3
MODEL=${4:-${CURSOR_WORKER_MODEL:-grok-4.7-high}}
REPO=$(cd "$(dirname "$0")/.." && pwd -P)

case "$MODEL" in
  grok-*|cursor-grok-*) ;;
  *) echo "only Grok models are allowed: $MODEL" >&2; exit 2 ;;
esac
[ -d "$WS" ] || { echo "no workspace dir: $WS" >&2; exit 2; }
WSR=$(cd "$WS" && pwd -P)
case "$WSR/" in
  "$REPO"/*|"$HOME"/|/) echo "workspace must be a separate folder outside the repository: $WSR" >&2; exit 2 ;;
esac
# Nothing from the hidden sets or B4 goes off the machine (W22).
if grep -rIlE 'eval/hidden|HIDDEN_LEDGER|eval/m6/h6|eval/m6/b4|tier_b_' "$WSR" "$PROMPT" 2>/dev/null | head -1 | grep -q .; then
  echo "refused: the workspace or prompt names a hidden set or B4" >&2; exit 2
fi
if find "$WSR" \( -name '*.enc' -o -path '*hidden*' -o -path '*/b4/*' -o -path '*/h6/*' \) | head -1 | grep -q .; then
  echo "refused: the workspace holds hidden or B4 files" >&2; exit 2
fi

case "$MODE" in
  ro) FLAGS="--mode ask" ;;
  rw) FLAGS="--force --sandbox enabled" ;;
  *) echo "mode must be ro or rw" >&2; exit 2 ;;
esac

echo "cursor-worker: mode=$MODE model=$MODEL workspace=$WSR" >&2
# shellcheck disable=SC2086
exec "$HOME/.local/bin/cursor-agent" -p --output-format text --model "$MODEL" --trust \
  --workspace "$WSR" $FLAGS "$(cat "$PROMPT")"
