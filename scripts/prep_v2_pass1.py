#!/usr/bin/env python3
"""work/v2/raw_qa.jsonl without apache-kafka-config records: config items come from the
config redo and the GLM-checked config-other file instead."""
import json
with open("work/v2/raw_qa.jsonl") as f, open("work/v2/raw_qa_pass1_noconfig.jsonl", "w") as o:
    for l in f:
        if json.loads(l)["source"] != "apache-kafka-config":
            o.write(l)
