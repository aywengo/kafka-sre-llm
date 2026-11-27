#!/usr/bin/env python3
"""CPU preflight of the release test's pack recovery (no model, no server): `python -m kafka_sre_rag ask --dry-run --json` on the first N D7 questions
(the stub generator, the real frozen retrieval and reranker), then the chain's pack resolver (scripts/m7_release_test.py retrieval_resolver) on each question and the
expansion that call returned. Every pack must be recovered exactly: the same chunks in the same order with the same scores. Exit 0 only when N of N match.

  .venv/bin/python scripts/m7_release_preflight.py [--n 10] [--items eval/m7/d7/items.jsonl]

Needs the frozen index (work/v3/chunks_s1.jsonl) and the reranker in the local cache; the tests skip without them. Prints counts and ids of the open D7 questions only.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
import eval2 as E  # noqa: E402
import m7_release_test as T  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--items", default=str(ROOT / "eval/m7/d7/items.jsonl"))
    a = ap.parse_args(argv)
    if E.is_hidden(a.items):
        print("REFUSED: the preflight runs on open items only", file=sys.stderr)
        return 2
    items = [json.loads(l) for l in Path(a.items).read_text(encoding="utf-8").splitlines() if l.strip()][:a.n]
    resolve = T.retrieval_resolver(None)
    ok = 0
    for i, it in enumerate(items, 1):
        q = E.full_question(it)
        p = subprocess.run([sys.executable, "-m", "kafka_sre_rag", "ask", "--dry-run", "--json", "--", q], cwd=ROOT, capture_output=True, text=True, timeout=900)
        try:
            d = json.loads(p.stdout.strip().splitlines()[-1])
            ids = resolve(q, d.get("expansion") or "", d["sources"])
            good = len(ids) == len(d["sources"])
        except (T.Refused, ValueError, KeyError, IndexError) as e:
            good = False
            print(f"[preflight] {i}/{len(items)} {it['id']} FAILED: {type(e).__name__}: {e}", flush=True)
        else:
            print(f"[preflight] {i}/{len(items)} {it['id']} {'exact' if good else 'FAILED'} ({len(ids)} chunks)", flush=True)
        ok += good
    print(f"preflight: {ok} of {len(items)} packs recovered exactly")
    return 0 if ok == len(items) and items else 1


if __name__ == "__main__":
    sys.exit(main())
