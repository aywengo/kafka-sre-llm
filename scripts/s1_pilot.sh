#!/bin/sh
# Stage 1 pilot (agents/train-v3-stage1.md, step 1): 40 bank questions through the pack-mode generator
# (DeepSeek, thinking off) and both verifiers (DeepSeek, then GLM oQ4e), then the mechanical check.
# The questions come from a provisional bank: a snapshot of the raw file while the full bank is still
# being generated, plus the out-of-corpus stream. Model server only; resumable; rerun to continue.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
I=work/v3/chunks_s1.jsonl; P=work/v3/pilot; mkdir -p $P
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp; GLM=GLM-5.3-Flash-oQ4e
NOTHINK='{"chat_template_kwargs": {"thinking_mode": "chat"}}'
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 30 --other-retries 2"

# 1. provisional bank: first 100 seeds of the plan (already generated in the snapshot) + out-of-corpus questions
[ -s $P/raw.jsonl ] || cp work/v3/questions_raw.jsonl $P/raw.jsonl
$G --log runs/v3/logs/s1_pilot_bank.log -- .venv/bin/python scripts/gen_questions.py --chunks $I --raw $P/raw.jsonl \
  --limit 100 --withheld 10 --out $P/questions_prov.jsonl --eval-slice-out $P/evalslice_prov.jsonl \
  --reserve-out $P/reserve_prov.jsonl --stats $P/bank_prov_stats.md --concurrency 2

# 2. pick 40 train questions: every withheld-marked one (10), 3 out-of-corpus, troubleshooting up to 20, the rest spread over categories
[ -s $P/pilot_questions.jsonl ] || .venv/bin/python - <<'PY'
import json, random
rows = [json.loads(l) for l in open("work/v3/pilot/questions_prov.jsonl")]
rnd = random.Random(42)
train = [r for r in rows if r.get("split") == "train"]
wh = [r for r in train if r["treatment"] == "withheld"]
ooc = [r for r in train if r["treatment"] == "ooc"]; rnd.shuffle(ooc)
norm = [r for r in train if r["treatment"] == "normal"]; rnd.shuffle(norm)
pick = wh[:10] + ooc[:3]
need = 20 - sum(r["type"] == "troubleshooting" for r in pick)
pick += [r for r in norm if r["type"] == "troubleshooting"][:max(0, need)]
rest = [r for r in norm if r["type"] != "troubleshooting"]
by = {}
for r in rest:
    by.setdefault(r["category"], []).append(r)
while len(pick) < 40 and any(by.values()):
    for c in sorted(by):
        if by[c] and len(pick) < 40:
            pick.append(by[c].pop())
with open("work/v3/pilot/pilot_questions.jsonl", "w") as f:
    for r in pick:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
from collections import Counter
print("pilot:", len(pick), dict(Counter(r["type"] for r in pick)), dict(Counter(r["treatment"] for r in pick)),
      dict(Counter(r["category"] for r in pick)))
PY

# 3. search terms (DeepSeek) and packs (CPU reranker) for the 40
$G --log runs/v3/logs/s1_pilot_expand.log -- .venv/bin/python scripts/build_packs_v3.py expand \
  --questions $P/pilot_questions.jsonl --out $P/pilot_expansions.jsonl --concurrency 2
HF_HUB_OFFLINE=1 .venv/bin/python scripts/build_packs_v3.py pack --questions $P/pilot_questions.jsonl \
  --expansions $P/pilot_expansions.jsonl --index $I --out $P/pilot_packs.jsonl

# 4. answers and the first verifier (DeepSeek)
$G --log runs/v3/logs/s1_pilot_gen.log -- .venv/bin/python scripts/generate_qa.py --mode pack \
  --url $OMLX_URL/v1 --model $DS --verify-model $DS --extra-body "$NOTHINK" --verify-extra-body "$NOTHINK" \
  --questions $P/pilot_questions.jsonl --packs $P/pilot_packs.jsonl --index $I \
  --out $P/pilot_pack.jsonl --verify each --concurrency 2 --stats $P/pilot_gen_stats.md

# 5. the second verifier (GLM oQ4e, fits in memory next to DeepSeek) on every item
$G --log runs/v3/logs/s1_pilot_glm.log -- .venv/bin/python scripts/generate_qa.py --mode pack-verify \
  --url $OMLX_URL/v1 --model $GLM --verify-model $GLM --extra-body '{}' --verify-extra-body '{}' \
  --verify-max-tokens 32768 --in $P/pilot_pack.jsonl --index $I --out $P/pilot_pack_glm.jsonl \
  --concurrency 3 --stats $P/pilot_glm_stats.md

# 6. mechanical check: identifiers absent from the pack, on all answered rows and on rows both verifiers kept
.venv/bin/python - <<'PY'
import json
P = "work/v3/pilot/"
rows = [json.loads(l) for l in open(P + "pilot_pack.jsonl")]
glm = {json.loads(l)["id"]: json.loads(l).get("verify") for l in open(P + "pilot_pack_glm.jsonl")}
def ok(v): return bool(v) and v.get("grounded") is True
for name, sel in (("all", lambda r: not r.get("skip") and r.get("answer")),
                  ("kept", lambda r: not r.get("skip") and r.get("answer") and ok(r.get("verify")) and ok(glm.get(r["id"])))):
    with open(P + f"pilot_{name}_answers.jsonl", "w") as a, open(P + f"pilot_{name}_packs.jsonl", "w") as p:
        n = 0
        for r in rows:
            if sel(r):
                a.write(json.dumps({"question": r["question"], "text": r["answer"]}, ensure_ascii=False) + "\n")
                p.write(json.dumps({"question": r["question"], "k6": r["chunk_ids"]}, ensure_ascii=False) + "\n"); n += 1
    print(name, n)
PY
for n in all kept; do
  .venv/bin/python scripts/invention_count.py --answers $P/pilot_${n}_answers.jsonl --packs $P/pilot_${n}_packs.jsonl \
    --index $I --json $P/pilot_${n}_invention.json > $P/pilot_${n}_invention.txt
done
echo S1_PILOT_DONE >> runs/v3/logs/s1_pilot.done
