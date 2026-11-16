#!/bin/sh
# Run one headless Claude Code worker on a local oMLX model instead of a Claude model.
#
#   .claude/local-worker.sh <agent> <ro|rw> <prompt-file>      # answer on stdout
#
# <agent> is a file in .claude/agents (quick-reader, implementer, complex-reviewer, ...).
# ro = may only read (Read, Grep, Glob and a few read-only shell commands); rw = may also edit files
# and run the project's python and git diff/status.
#
# Exit 3 = the local server is not free (a pipeline job is running, another client is active, or a
# model outside the allowed two is loaded). The caller then uses a Claude subagent instead.
# Allowed models (owner's choice): GLM-5.3-Flash-oQ8e-mtp, DeepSeek-V4.1-Flash-1017-oQ4e-mtp.
set -eu
AGENT=$1; MODE=$2; PROMPT=$3
# Server address and key live outside the repository; neither is ever written here.
CFG="$HOME/.config/kafka-sre-llm"
URL=${OMLX_URL:-$(cat "$CFG/omlx_url")}
KEY=$(cat "$CFG/judge_key")
PREFER=${LOCAL_WORKER_MODEL:-GLM-5.3-Flash-oQ8e-mtp}

# Pipeline jobs own the GPU and the server while they run.
if pgrep -f "run_guarded.py|eval/compare.py|rag_answers.py|generate_qa.py|mlx_lm|phase0.*\.sh" >/dev/null; then
  echo "BUSY: a pipeline job is running" >&2; exit 3
fi

MODEL=$(curl -s -m 10 -H "Authorization: Bearer $KEY" "$URL/api/status" | PREFER="$PREFER" python3 -c '
import json, os, sys
allowed = ["GLM-5.3-Flash-oQ8e-mtp", "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"]
try:
    s = json.load(sys.stdin)
except Exception:
    sys.exit("BUSY: no status from the model server")
if s.get("active_requests") or s.get("waiting_requests") or s.get("models_loading"):
    sys.exit("BUSY: another client is using the model server")
loaded = s.get("loaded_models") or []
ours = [m for m in loaded if m in allowed]
if ours:
    print(ours[0])                      # use what is loaded, never swap for a worker
elif loaded:
    sys.exit("BUSY: other models are loaded: " + ", ".join(loaded))
else:
    print(os.environ["PREFER"] if os.environ["PREFER"] in allowed else allowed[0])
') || exit 3

case "$MODE" in
  ro) TOOLS="Read Grep Glob Bash(ls:*) Bash(tail:*) Bash(head:*) Bash(wc:*) Bash(grep:*) Bash(cat:*) Bash(ps:*) Bash(date)" ;;
  rw) TOOLS="Read Grep Glob Edit Write Bash(ls:*) Bash(tail:*) Bash(head:*) Bash(wc:*) Bash(grep:*) Bash(cat:*) Bash(.venv/bin/python:*) Bash(git diff:*) Bash(git status:*)" ;;
  *) echo "mode must be ro or rw" >&2; exit 2 ;;
esac

echo "local-worker: agent=$AGENT mode=$MODE model=$MODEL" >&2
# Every model alias points at the local model, so no request names a model the server would have to load.
# shellcheck disable=SC2086
exec env \
  ANTHROPIC_BASE_URL="$URL" ANTHROPIC_AUTH_TOKEN="$KEY" \
  ANTHROPIC_MODEL="$MODEL" ANTHROPIC_SMALL_FAST_MODEL="$MODEL" \
  ANTHROPIC_DEFAULT_HAIKU_MODEL="$MODEL" ANTHROPIC_DEFAULT_SONNET_MODEL="$MODEL" ANTHROPIC_DEFAULT_OPUS_MODEL="$MODEL" \
  CLAUDE_CODE_SUBAGENT_MODEL="$MODEL" CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
  claude -p --agent "$AGENT" --model "$MODEL" --setting-sources project --strict-mcp-config \
    --permission-mode default --allowedTools $TOOLS --output-format text < "$PROMPT"
