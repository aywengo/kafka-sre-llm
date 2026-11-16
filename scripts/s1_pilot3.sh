#!/bin/sh
# Stage 1, third pilot (prompt version pack/3): 40 FRESH questions from the provisional bank (none used in the
# first two pilots: 10 marked withheld, 3 out-of-corpus, troubleshooting up to 20, the rest spread over
# categories) plus the 6 questions on which pack/2 still failed. Search terms, packs (CPU), answers and the first
# verifier (DeepSeek), then the mechanical check. The second verifier runs separately when DeepSeek is unloaded.
set -e
export OMLX_URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url")}
export OMLX_API_KEY=${OMLX_API_KEY:-$(cat "$HOME/.config/kafka-sre-llm/judge_key")}
I=work/v3/chunks_s1.jsonl; P=work/v3/pilot; O=work/v3/pilot3; mkdir -p $O
DS=DeepSeek-V4.1-Flash-1017-oQ4e-mtp
NOTHINK='{"chat_template_kwargs": {"thinking_mode": "chat"}}'
G="scripts/run_guarded.py --heartbeat runs/v3/HEARTBEAT.md --stall-min 30 --other-retries 2"

[ -s $O/pilot_questions.jsonl ] || .venv/bin/python - <<'PY'
import json, random
from collections import Counter
P, O = "work/v3/pilot/", "work/v3/pilot3/"
rows = [json.loads(l) for l in open(P + "questions_prov.jsonl")]
old = [json.loads(l) for l in open(P + "pilot_questions.jsonl")]
used = {r["id"] for r in old}
rnd = random.Random(43)
train = [r for r in rows if r.get("split") == "train" and r["id"] not in used]
ooc = [r for r in train if r["treatment"] == "ooc"]; rnd.shuffle(ooc)
norm = [r for r in train if r["origin"] == "seed"]; rnd.shuffle(norm)
elig = [r for r in norm if r["type"] in ("troubleshooting", "how-to", "config-review")]
wh = [dict(r, treatment="withheld") for r in elig[:10]]
whids = {r["id"] for r in wh}
norm = [dict(r, treatment="normal") for r in norm if r["id"] not in whids]
pick = wh + ooc[:3]
need = 20 - sum(r["type"] == "troubleshooting" for r in pick)
ts = [r for r in norm if r["type"] == "troubleshooting"][:max(0, need)]
pick += ts
tsids = {r["id"] for r in ts}
by = {}
for r in norm:
    if r["type"] != "troubleshooting" and r["id"] not in tsids:
        by.setdefault(r["category"], []).append(r)
while len(pick) < 40 and any(by.values()):
    for c in sorted(by):
        if by[c] and len(pick) < 40:
            pick.append(by[c].pop())
again = ("286c21fd", "69c82360", "484abe48", "52424f4f", "5e3a51ff", "ea548af9")
redo = [r for r in old if r["id"].startswith(again)]
with open(O + "pilot_questions.jsonl", "w") as f:
    for r in pick:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
with open(O + "redo_questions.jsonl", "w") as f:
    for r in redo:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("pilot3 fresh:", len(pick), dict(Counter(r["type"] for r in pick)), dict(Counter(r["treatment"] for r in pick)),
      dict(Counter(r["category"] for r in pick)), "| redo:", len(redo))
PY

# search terms and packs for the fresh 40; the 6 repeated questions keep their packs from the first pilot
$G --log runs/v3/logs/s1_pilot3_expand.log -- .venv/bin/python scripts/build_packs_v3.py expand \
  --questions $O/pilot_questions.jsonl --out $O/pilot_expansions.jsonl --concurrency 2
HF_HUB_OFFLINE=1 .venv/bin/python scripts/build_packs_v3.py pack --questions $O/pilot_questions.jsonl \
  --expansions $O/pilot_expansions.jsonl --index $I --out $O/pilot_packs.jsonl

gen() { # QUESTIONS PACKS OUT STATS LOG
  $G --log $5 -- .venv/bin/python scripts/generate_qa.py --mode pack \
    --url $OMLX_URL/v1 --model $DS --verify-model $DS --extra-body "$NOTHINK" --verify-extra-body "$NOTHINK" \
    --questions $1 --packs $2 --index $I --out $3 --verify each --concurrency 2 --stats $4
}
gen $O/pilot_questions.jsonl $O/pilot_packs.jsonl $O/pilot_pack.jsonl $O/pilot_gen_stats.md runs/v3/logs/s1_pilot3_gen.log
gen $O/redo_questions.jsonl $P/pilot_packs.jsonl $O/redo_pack.jsonl $O/redo_gen_stats.md runs/v3/logs/s1_pilot3_redo.log

.venv/bin/python - <<'PY'
import json
O = "work/v3/pilot3/"
rows = [json.loads(l) for l in open(O + "pilot_pack.jsonl")] + [json.loads(l) for l in open(O + "redo_pack.jsonl")]
with open(O + "pilot_all_answers.jsonl", "w") as a, open(O + "pilot_all_packs.jsonl", "w") as p:
    for r in rows:
        if not r.get("skip") and r.get("answer"):
            a.write(json.dumps({"question": r["question"], "text": r["answer"]}, ensure_ascii=False) + "\n")
            p.write(json.dumps({"question": r["question"], "k6": r["chunk_ids"]}, ensure_ascii=False) + "\n")
PY
.venv/bin/python scripts/invention_count.py --answers $O/pilot_all_answers.jsonl --packs $O/pilot_all_packs.jsonl \
  --index $I --json $O/pilot_all_invention.json > $O/pilot_all_invention.txt
echo S1_PILOT3_DONE >> runs/v3/logs/s1_pilot3.done
