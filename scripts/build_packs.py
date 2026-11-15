#!/usr/bin/env python3
"""CPU: packs for the questions in an expansions.jsonl (written by rag_answers.py expand).
  build_packs.py expansions.jsonl packs.json"""
import json, sys
sys.path.insert(0, "scripts")
import retrieve as RT
out = []
for l in open(sys.argv[1]):
    r = json.loads(l)
    out.append({"question": r["question"], "expansion": r["expansion"],
                "k6": [c["id"] for c in RT.retrieve(r["question"], 6, 14000, expansion=r["expansion"])],
                "k3": [c["id"] for c in RT.retrieve(r["question"], 3, 6000, expansion=r["expansion"])]})
json.dump(out, open(sys.argv[2], "w"))
print("packs", len(out))
