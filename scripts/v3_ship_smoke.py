#!/usr/bin/env python3
"""End-to-end smoke test of the runtime (kafka_sre_rag.pipeline.ask, mlx backend, frozen index, k6, default mode).
The CLI's JSON has no chunk ids, so this driver captures them from retrieval.select. One JSON file per question.
  v3_ship_smoke.py --model runs/v1/base27b --questions eval/questions_real.jsonl --n 3 --out-dir work/v3/ship
Optional: --adapter DIR, and --stored-expansions / --stored-packs / --stored-answers (jsonl, matched by question
text) to compare search terms, k6 chunk ids and answer text with stored files; the record gets a "compare" dict.
Resumable: an existing smoke_<n>.json is kept. Runs the frozen-configuration check first."""
import argparse, json, sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kafka_sre_rag import config, pipeline, retrieval  # noqa: E402


def load_by_question(path):
    if not path:
        return {}
    return {r["question"]: r for r in (json.loads(l) for l in open(path) if l.strip())}


def compare(q, rec, exp, packs, answers):
    """yes/no per stored file given; None when no stored file or no row for the question."""
    out = {}
    if exp is not None:
        r = exp.get(q)
        out["expansion_equal"] = None if r is None else r["expansion"] == rec["expansion"]
    if packs is not None:
        r = packs.get(q)
        out["chunk_ids_equal"] = None if r is None else list(r["k6"]) == rec["chunk_ids"]
    if answers is not None:
        r = answers.get(q)
        out["answer_equal"] = None if r is None else r["text"] == rec["text"]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--questions", required=True)
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--adapter")
    ap.add_argument("--stored-expansions")
    ap.add_argument("--stored-packs")
    ap.add_argument("--stored-answers")
    a = ap.parse_args()
    cfg = config.load()
    config.verify(cfg=cfg)
    print("frozen configuration check: ok", flush=True)
    qs = [json.loads(l)["question"] for l in open(a.questions) if l.strip()][:a.n]
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    todo = [(i, q) for i, q in enumerate(qs, 1) if not (out / f"smoke_{i}.json").exists()]
    if not todo:
        print("all smoke results exist", flush=True)
        return 0
    from kafka_sre_rag.generators import MLXGenerator
    t0 = time.monotonic()
    gen = MLXGenerator(a.model, a.adapter)
    print(f"model loaded in {time.monotonic() - t0:.1f} s", flush=True)
    stored = [load_by_question(f) if f else None for f in (a.stored_expansions, a.stored_packs, a.stored_answers)]
    captured = {}
    real_select = retrieval.select

    def select(*args, **kw):
        chunks = real_select(*args, **kw)
        captured["ids"] = [c["id"] for c in chunks]
        return chunks
    retrieval.select = select
    for i, q in todo:
        captured.clear()
        ans = pipeline.ask(q, gen, cfg, k="k6", mode="default")
        rec = {"question": q, "expansion": ans.expansion, "chunk_ids": captured.get("ids"), "text": ans.text,
               "timings": ans.timings, "measured": ans.measured, "unmeasured_reasons": ans.unmeasured_reasons,
               "tokens": ans.tokens, "truncated": ans.truncated}
        if any(x is not None for x in stored):
            rec["compare"] = compare(q, rec, *stored)
        tmp = out / f"smoke_{i}.json.tmp"
        tmp.write_text(json.dumps(rec, ensure_ascii=False))
        tmp.rename(out / f"smoke_{i}.json")
        t = ans.timings
        print(f"smoke {i}/{len(qs)}: measured={ans.measured} tokens={ans.tokens} expand {t['expand_s']:.1f}s "
              f"retrieve {t['retrieve_s']:.1f}s answer {t['answer_s']:.1f}s"
              + (f" compare {rec['compare']}" if "compare" in rec else ""), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
