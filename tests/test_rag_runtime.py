import contextlib
import copy
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import config, pipeline, prompts  # noqa: E402

CHUNKS = [
    {"id": "a", "source": "apache-kafka", "title": "Consumer configs", "path": "raw/a/b.md", "text": "Set max.poll.interval.ms.",
     "bm25_rank": 1, "ce_score": 0.9},
    {"id": "b", "source": "strimzi", "title": "", "path": "raw/s/lag.md", "text": "Use kafka-consumer-groups.sh.",
     "bm25_rank": 3, "ce_score": 0.4},
]


class Stub:
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def __init__(self, identity=None):
        self.calls = []
        if identity:
            self.identity = identity

    def chat(self, messages, max_tokens, on_token=None):
        self.calls.append((messages, max_tokens))
        return ("terms, a, b" if len(self.calls) == 1 else "the answer"), 5, False


class TestConfig(unittest.TestCase):
    def test_verify_real(self):
        rep = config.verify()
        self.assertTrue(rep["index"]["match"])
        self.assertTrue(all(p["match"] for p in rep["prompts"].values()))

    def test_tampered_index(self):
        src = config.index_file()
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "chunks.jsonl"
            with open(src, "rb") as f, open(p, "wb") as g:
                g.write(f.read())
                g.write(b" ")
            with self.assertRaisesRegex(config.ConfigError, "index sha256 mismatch"):
                config.verify(index_path=p)

    def test_prompt_monkeypatched(self):
        with mock.patch.object(prompts, "STRICT", prompts.STRICT + " x"):
            with self.assertRaisesRegex(config.ConfigError, "prompt STRICT changed"):
                config.verify()

    def test_frozen_has_no_path_user_or_ip(self):
        text = (ROOT / "kafka_sre_rag/frozen.json").read_text()
        self.assertNotRegex(text, r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
        self.assertNotIn("/Users/", text)
        self.assertNotIn(os.path.expanduser("~"), text)
        self.assertNotIn("roman", text.lower())
        idx = json.loads(text)["index"]["index_path"]
        self.assertFalse(os.path.isabs(idx))

    def test_banner(self):
        self.assertEqual(config.unmeasured_banner([]), "")
        self.assertTrue(config.unmeasured_banner(["k k3"]).startswith("UNMEASURED CONFIGURATION"))


class TestAsk(unittest.TestCase):
    def run_ask(self, gen=None, **kw):
        gen = gen or Stub()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS) as sel:
            ans = pipeline.ask("q?", gen, config.load(), **kw)
        return ans, gen, sel

    def test_default_messages(self):
        ans, gen, sel = self.run_ask()
        self.assertEqual(gen.calls[0], (prompts.expand_messages("q?"), 300))
        self.assertEqual(sel.call_args.args, ("q?", "terms, a, b"))
        self.assertEqual(gen.calls[1], (prompts.answer_messages("q?", CHUNKS), 2048))
        self.assertEqual(ans.sources[0], {"source": "apache-kafka", "title": "Consumer configs", "path": "raw/a/b.md",
                                          "bm25_rank": 1, "ce_score": 0.9})

    def test_strict_messages(self):
        ans, gen, _ = self.run_ask(mode="strict")
        self.assertEqual(gen.calls[1][0], prompts.answer_messages("q?", CHUNKS, strict=True))
        self.assertIn(prompts.STRICT, gen.calls[1][0][1]["content"])
        self.assertFalse(ans.measured)

    def test_measured_flag(self):
        ref = config.load()["reference_generator"]
        refgen = {"backend": "mlx", "model": "/x/snapshots/" + ref["snapshot"], "adapter": None}
        self.assertTrue(self.run_ask(Stub(refgen))[0].measured)
        self.assertFalse(self.run_ask(Stub(refgen), k="k3")[0].measured)
        self.assertFalse(self.run_ask(Stub(refgen), mode="strict")[0].measured)
        self.assertFalse(self.run_ask(Stub(refgen), rerank=False)[0].measured)
        self.assertFalse(self.run_ask(Stub({**refgen, "adapter": "ad"}))[0].measured)
        self.assertFalse(self.run_ask()[0].measured)  # stub is not the reference generator


def mock_client(handler):
    from kafka_sre_rag.generators import OpenAIChatGenerator
    return OpenAIChatGenerator("http://example.invalid", "m1", api_key="k", transport=httpx.MockTransport(handler))


class TestGenerators(unittest.TestCase):
    def test_request_and_think(self):
        seen = {}

        def handler(req):
            seen["body"] = json.loads(req.content)
            seen["url"] = str(req.url)
            return httpx.Response(200, json={"choices": [{"message": {"content": "<think>x\ny</think>\nhello"},
                                                          "finish_reason": "stop"}], "usage": {"completion_tokens": 7}})
        text, toks, trunc = mock_client(handler).chat([{"role": "user", "content": "hi"}], 50)
        self.assertEqual((text, toks, trunc), ("hello", 7, False))
        self.assertEqual(seen["body"]["temperature"], 0)
        self.assertEqual(seen["body"]["model"], "m1")
        self.assertEqual(seen["body"]["max_tokens"], 50)
        self.assertTrue(seen["url"].endswith("/v1/chat/completions"))

    def test_truncated(self):
        h = lambda req: httpx.Response(200, json={"choices": [{"message": {"content": "cut"}, "finish_reason": "length"}]})
        self.assertTrue(mock_client(h).chat([], 5)[2])

    def test_base_url_from_env(self):
        from kafka_sre_rag.generators import OpenAIChatGenerator
        with mock.patch.dict(os.environ, {"OMLX_URL": "http://example.invalid/v1"}):
            self.assertEqual(OpenAIChatGenerator(None, "m").url, "http://example.invalid/v1/chat/completions")

    def test_import_does_not_load_mlx(self):
        code = "import sys, kafka_sre_rag.generators; sys.exit(1 if any(m == 'mlx' or m.startswith('mlx.') or m == 'mlx_lm' for m in sys.modules) else 0)"
        r = subprocess.run([sys.executable, "-c", code], cwd=ROOT)
        self.assertEqual(r.returncode, 0)


class TestCLI(unittest.TestCase):
    def test_dry_run(self):
        r = subprocess.run([sys.executable, "-m", "kafka_sre_rag", "ask", "--dry-run", "--no-rerank-in-dry-run", "q"],
                           cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("EXPANSION REQUEST", r.stdout)
        self.assertIn("ANSWER REQUEST", r.stdout)
        self.assertIn(prompts.SYSTEM_PROMPT, r.stdout)
        self.assertIn("Question: q", r.stdout)

    def test_verify(self):
        r = subprocess.run([sys.executable, "-m", "kafka_sre_rag", "verify"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotIn("MISMATCH", r.stdout)


@unittest.skipUnless(os.environ.get("RAG_SLOW_TESTS") == "1", "set RAG_SLOW_TESTS=1 (runs the cross-encoder on CPU)")
class TestSlowRetrieval(unittest.TestCase):
    def test_k6_ids_match_stored(self):
        from kafka_sre_rag import retrieval
        packs = ROOT / "work/v3/packs_real_s1.jsonl"
        if not packs.exists():
            self.skipTest("work/v3/packs_real_s1.jsonl missing")
        rows = [json.loads(l) for l in open(packs) if l.strip()][:3]
        for r in rows:
            got = retrieval.select(r["question"], r["expansion"], k="k6")
            self.assertEqual([c["id"] for c in got], r["k6"])
            self.assertTrue(all("text" in c and "bm25_rank" in c and "ce_score" in c for c in got))


if __name__ == "__main__":
    unittest.main()
