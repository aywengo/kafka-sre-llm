#!/bin/sh
# usage: gen_v2.sh LOG extra generate_qa.py args... (needs OMLX_API_KEY in env)
log=$1; shift
exec scripts/run_retry.sh "$log" .venv/bin/python scripts/generate_qa.py \
  --url http://127.0.0.1:8000/v1 --model DeepSeek-V4.1-Flash-1017-oQ4e-mtp \
  --chunks work/v2/chunks.jsonl --concurrency 3 --verify batch \
  --extra-body '{"chat_template_kwargs": {"thinking_mode": "chat"}}' "$@"
