"""python -m kafka_sre_rag ask|chat|verify"""
import argparse
import json
import os
import sys

from . import config
from .generators import EmptyAnswerError

DRY_EXPANSION = "consumer group, kafka-consumer-groups.sh, --describe, --group, LAG, max.poll.interval.ms"


class StubGenerator:
    """--dry-run: fixed expansion, echoes the request. Prints the exact messages it receives."""
    backend = "stub"
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def __init__(self, out=sys.stdout):
        self.out, self.n = out, 0

    def chat(self, messages, max_tokens, on_token=None):
        self.n += 1
        label = "EXPANSION REQUEST" if self.n % 2 == 1 else "ANSWER REQUEST"
        print(f"===== {label} (max_tokens {max_tokens}) =====", file=self.out)
        for m in messages:
            print(f"--- {m['role']} ---\n{m['content']}", file=self.out)
        text = DRY_EXPANSION if self.n % 2 == 1 else "[dry run: no model was called]"
        return text, 0, False


def build_parser():
    ap = argparse.ArgumentParser(prog="python -m kafka_sre_rag", description="Retrieval-augmented Kafka SRE answers.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--k", choices=["k6", "k3"], default="k6")
        p.add_argument("--mode", choices=["default", "strict"], default="default")
        p.add_argument("--backend", choices=["mlx", "openai"], default="mlx")
        p.add_argument("--model", help="mlx: model directory; openai: model name sent to the server")
        p.add_argument("--adapter", help="mlx only: LoRA adapter directory")
        p.add_argument("--base-url", help="openai: server URL (default: environment variable OMLX_URL)")
        p.add_argument("--extra-body", type=json.loads, default=None, metavar="JSON",
                       help="openai: JSON merged into the request body, e.g. "
                            "'{\"chat_template_kwargs\": {\"enable_thinking\": false}}' to switch thinking off")
        p.add_argument("--max-tokens", type=int, default=None, metavar="N",
                       help="token limit for both model calls (default: the frozen limits, 300 and 2048). "
                            "A reasoning model on the server needs a larger value; the run is then unmeasured")
        p.add_argument("--sources", action="store_true", help="print the retrieved sources under the answer")
        p.add_argument("--json", action="store_true", help="print one JSON object")
        p.add_argument("--dry-run", action="store_true",
                       help="no model: stub generator, prints the exact messages that would be sent")
        p.add_argument("--no-rerank-in-dry-run", action="store_true",
                       help="with --dry-run, skip the cross-encoder (BM25 order; fast, unmeasured)")

    a = sub.add_parser("ask", help="answer one question")
    a.add_argument("question")
    common(a)
    c = sub.add_parser("chat", help="interactive loop; each turn is retrieved and answered independently "
                                    "(no conversation history is sent; multi-turn is unmeasured)")
    common(c)
    sub.add_parser("verify", help="check the index sha256 and the prompt hashes against frozen.json")
    return ap


def make_generator(a):
    if a.dry_run:
        return StubGenerator()
    if a.backend == "openai":
        from .generators import OpenAIChatGenerator
        if not a.model:
            sys.exit("--model is required for the openai backend")
        return OpenAIChatGenerator(a.base_url or os.environ.get("OMLX_URL"), a.model,
                                   extra_body=a.extra_body)
    from .generators import MLXGenerator
    model = a.model or os.environ.get("BASE_PATH")
    if not model:
        sys.exit("--model (or BASE_PATH) is required for the mlx backend")
    return MLXGenerator(model, a.adapter)


def run_one(question, gen, cfg, a):
    from .pipeline import ask
    stream = None if (a.json or a.dry_run) else (lambda t: print(t, end="", flush=True))
    ans = ask(question, gen, cfg, k=a.k, mode=a.mode, on_token=stream,
              rerank=not (a.dry_run and a.no_rerank_in_dry_run), max_tokens=a.max_tokens)
    if a.json:
        print(json.dumps({"question": question, "text": ans.text, "sources": ans.sources, "expansion": ans.expansion,
                          "timings": ans.timings, "measured": ans.measured, "tokens": ans.tokens,
                          "truncated": ans.truncated, "unmeasured_reasons": ans.unmeasured_reasons},
                         ensure_ascii=False))
        return
    banner = config.unmeasured_banner(ans.unmeasured_reasons)
    if banner:
        print(banner, file=sys.stderr)
    print(ans.text if not stream else "")
    if a.sources:
        print("\nSources:")
        for s in ans.sources:
            ce = "n/a" if s["ce_score"] is None else f"{s['ce_score']:.3f}"
            print(f"  [{s['bm25_rank']}] {s['source']} - {s['title'] or s['path'].rsplit('/', 1)[-1]} ({s['path']}) ce={ce}")
    if ans.truncated:
        print("(answer cut at max_tokens)", file=sys.stderr)


def main(argv=None):
    a = build_parser().parse_args(argv)
    if a.cmd == "verify":
        try:
            rep = config.verify()
        except config.ConfigError as e:
            print(e, file=sys.stderr)
            return 1
        i = rep["index"]
        print(f"index    {'match' if i['match'] else 'MISMATCH'}  {i['sha256']}")
        for n, p in rep["prompts"].items():
            print(f"prompt   {'match' if p['match'] else 'MISMATCH'}  {n}  {p['sha256']}")
        r = rep["reranker"]
        print(f"reranker revision {r['revision']}  local cache: "
              f"{'not found' if r['cached'] is None else ('match' if r['match'] else 'MISMATCH')}")
        return 0
    cfg = config.load()
    if not a.dry_run:  # a real run must be on the frozen index and prompts
        try:
            config.verify(cfg=cfg)
        except config.ConfigError as e:
            print(e, file=sys.stderr)
            return 1
    gen = make_generator(a)
    if a.cmd == "ask":
        try:
            run_one(a.question, gen, cfg, a)
        except EmptyAnswerError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
        return 0
    try:  # chat
        while True:
            q = input("question> ").strip()
            if q in ("", "exit", "quit"):
                if q:
                    break
                continue
            try:
                run_one(q, gen, cfg, a)
            except EmptyAnswerError as e:
                print(f"error: {e}", file=sys.stderr)
    except (EOFError, KeyboardInterrupt):
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
