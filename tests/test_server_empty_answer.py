import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import __main__ as cli, pipeline  # noqa: E402
from kafka_sre_rag.generators import EmptyAnswerError, OpenAIChatGenerator  # noqa: E402


def gen_for(message, finish="stop", seen=None):
    def handler(req):
        if seen is not None:
            seen.append(json.loads(req.content))
        return httpx.Response(200, json={"choices": [{"message": message, "finish_reason": finish}],
                                         "usage": {"completion_tokens": 7}})
    return OpenAIChatGenerator("http://example.invalid", "m", transport=httpx.MockTransport(handler))


MSG = [{"role": "user", "content": "x"}]


class EmptyAnswer(unittest.TestCase):
    def test_empty_cut_off(self):
        with self.assertRaises(EmptyAnswerError) as cm:
            gen_for({"content": ""}, "length").chat(MSG, 50)
        m = str(cm.exception)
        self.assertIn("cut off by the token limit (finish_reason length)", m)
        self.assertIn("no reasoning content", m)
        self.assertIn("--max-tokens", m)
        self.assertIn("--extra-body", m)

    def test_empty_with_reasoning(self):
        with self.assertRaises(EmptyAnswerError) as cm:
            gen_for({"content": None, "reasoning_content": "hmm"}, "stop").chat(MSG, 50)
        self.assertIn("not cut off", str(cm.exception))
        self.assertIn("carried reasoning content", str(cm.exception))

    def test_only_think_block_is_empty(self):
        with self.assertRaises(EmptyAnswerError):
            gen_for({"content": "<think>x</think>  "}).chat(MSG, 50)

    def test_normal(self):
        self.assertEqual(gen_for({"content": " hi "}).chat(MSG, 50), ("hi", 7, False))

    def test_streaming_empty(self):
        body = 'data: {"choices":[{"delta":{"reasoning_content":"r"},"finish_reason":"length"}]}\n\ndata: [DONE]\n\n'
        g = OpenAIChatGenerator("http://example.invalid", "m",
                                transport=httpx.MockTransport(lambda r: httpx.Response(200, text=body)))
        with self.assertRaises(EmptyAnswerError) as cm:
            g.chat(MSG, 50, on_token=lambda t: None)
        self.assertIn("carried reasoning content", str(cm.exception))

    def test_ask_exits_nonzero(self):
        err = io.StringIO()
        with mock.patch.object(cli, "make_generator", return_value=gen_for({"content": ""}, "length")), \
                mock.patch("kafka_sre_rag.config.verify"), contextlib.redirect_stderr(err):
            rc = cli.main(["ask", "q", "--backend", "openai", "--model", "m"])
        self.assertEqual(rc, 2)
        self.assertIn("returned no answer text", err.getvalue())


class MaxTokens(unittest.TestCase):
    def test_flag_reaches_request(self):
        seen = []
        a = cli.build_parser().parse_args(["ask", "q", "--backend", "openai", "--model", "m", "--max-tokens", "16000"])
        self.assertEqual(a.max_tokens, 16000)
        g = gen_for({"content": "an answer"}, seen=seen)
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=[]):
            ans = pipeline.ask("q", g, max_tokens=a.max_tokens)
        self.assertEqual([b["max_tokens"] for b in seen], [16000, 16000])
        self.assertIn("max_tokens override 16000", ans.unmeasured_reasons)

    def test_default_unchanged(self):
        seen = []
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=[]):
            pipeline.ask("q", gen_for({"content": "a"}, seen=seen))
        self.assertEqual([b["max_tokens"] for b in seen], [300, 2048])
        self.assertIsNone(cli.build_parser().parse_args(["ask", "q"]).max_tokens)


if __name__ == "__main__":
    unittest.main()
