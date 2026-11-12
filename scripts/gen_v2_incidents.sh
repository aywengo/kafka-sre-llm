#!/bin/sh
# pass 5 (incidents): starts after pass 2 (kubernetes) has finished. (needs OMLX_API_KEY in env)
while pgrep -f "sources kubernetes" > /dev/null; do sleep 60; done
scripts/gen_v2.sh runs/v2/logs/incidents.log --out work/v2/raw_qa_incidents.jsonl \
  --sources apache-kafka apache-kafka-config strimzi \
  --paths /operations/ /kafka_config.html /consumer_config.html /producer_config.html /topic_config.html \
    documentation/modules/managing/ documentation/modules/cruise-control/ \
    documentation/modules/operators/ documentation/modules/metrics/ documentation/modules/upgrading/ \
  --context-neighbours 1 --types troubleshooting --max-per-chunk 3 --concurrency 4
