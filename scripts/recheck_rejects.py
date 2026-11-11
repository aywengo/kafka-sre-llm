"""Re-run the verifier on rejected items: recheck_rejects.py REJECTS N (needs OMLX_API_KEY)."""
import asyncio, json, os, sys, types, collections
sys.path.insert(0, "scripts")
import generate_qa as g

rejects = [json.loads(l) for l in open(sys.argv[1])][: int(sys.argv[2])]
chunks = {c["id"]: c for c in map(json.loads, open("work/v2/chunks.jsonl"))}
body = {"chat_template_kwargs": {"thinking_mode": "chat"}}
args = types.SimpleNamespace(verify_max_tokens=2048, verify_extra_body=body)
usage = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "reasoning_tokens": 0}
llm = g.LLM("http://127.0.0.1:8000/v1", "DeepSeek-V4.1-Flash-1017-oQ4e-mtp",
            os.environ["OMLX_API_KEY"], 0.7, 4096, body, usage)

async def main():
    by = collections.defaultdict(list)
    for r in rejects:
        by[r["chunk_id"]].append(r)
    kept = 0
    for cid, items in by.items():
        stats = {"rejected": 0, "verify_parse_fail": 0}
        rej = []
        ok = await g.verify_batch(chunks[cid], items, llm, args, stats, rej)
        kept += len(ok)
        for it in items:
            print("KEPT " if it in ok else "REJ  ", it["source"], "|", it["question"][:90],
                  "| old:", (it["reason"] or "")[:90],
                  "" if it in ok else "| new: " + next((x["reason"] or "" for x in rej if x["question"] == it["question"]), "")[:90])
    print(f"now kept {kept}/{len(rejects)}")

asyncio.run(main())
