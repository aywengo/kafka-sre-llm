#!/usr/bin/env python3
"""Build a compare.py answers cache for a head-to-head: base slot = reference answers, tuned slot = variant.
  mk_h2h.py base_answers.jsonl variant_answers.jsonl out.jsonl questions.jsonl"""
import json, sys
rd = lambda p: [json.loads(l) for l in open(p) if l.strip()]
qs = [r["question"] for r in rd(sys.argv[4])]
base = {r["question"]: r for r in rd(sys.argv[1])}
var = {r["question"]: r for r in rd(sys.argv[2])}
with open(sys.argv[3], "w") as f:
    for q in qs:
        f.write(json.dumps({**base[q], "label": "base"}, ensure_ascii=False) + "\n")
        f.write(json.dumps({**var[q], "label": "tuned"}, ensure_ascii=False) + "\n")
