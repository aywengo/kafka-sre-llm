#!/usr/bin/env python3
"""Convert the eval items of the held-out runbooks to the compare.py question format."""
import json
from pathlib import Path

holdout = Path("data/runbooks/holdout.txt").read_text().split()
with open("eval/questions_runbooks_holdout.jsonl", "w") as out:
    for h in holdout:
        folder = h.split("/")[0]
        for l in Path("data/runbooks/eval").glob("*.jsonl"):
            for line in l.open():
                if not line.strip():
                    continue
                r = json.loads(line)
                if r["runbook"] != f"runbooks/{h}":
                    continue
                out.write(json.dumps({
                    "category": f"runbook-{folder}",
                    "question": r["question"],
                    "reference": f"{r['reference_answer']}\nSource: {r['source_url']}",
                }, ensure_ascii=False) + "\n")
