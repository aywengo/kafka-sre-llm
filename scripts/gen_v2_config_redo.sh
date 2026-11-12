#!/bin/sh
# config-ref redo over apache-kafka-config chunks that contain "Type:" (setting entries);
# resumes by chunk id. Starts when pass 2 (kubernetes) is running. (needs OMLX_API_KEY in env)
until pgrep -f "sources kubernetes" > /dev/null; do sleep 30; done
scripts/gen_v2.sh runs/v2/logs/config_redo.log --out work/v2/raw_qa_config_redo.jsonl \
  --sources apache-kafka-config --profile config-ref --text-contains "Type:"
