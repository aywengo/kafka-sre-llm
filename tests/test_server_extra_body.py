import json
import sys
import unittest
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import __main__ as cli  # noqa: E402

THINK_OFF = '{"chat_template_kwargs": {"thinking": false}}'


class ExtraBodyFlag(unittest.TestCase):
    def test_flag_reaches_request_body(self):
        seen = []

        def handler(req):
            seen.append(json.loads(req.content))
            return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                                             "usage": {"completion_tokens": 1}})
        a = cli.build_parser().parse_args(["ask", "q", "--backend", "openai", "--model", "m1",
                                           "--base-url", "http://example.invalid", "--extra-body", THINK_OFF])
        gen = cli.make_generator(a)
        gen.client = httpx.Client(transport=httpx.MockTransport(handler))
        self.assertEqual(gen.chat([{"role": "user", "content": "x"}], 5)[0], "ok")
        self.assertEqual(seen[0]["chat_template_kwargs"], {"thinking": False})
        self.assertEqual(seen[0]["temperature"], 0)

    def test_default_adds_nothing(self):
        a = cli.build_parser().parse_args(["ask", "q", "--backend", "openai", "--model", "m1",
                                           "--base-url", "http://example.invalid"])
        self.assertIsNone(a.extra_body)
        self.assertEqual(cli.make_generator(a).extra_body, {})


if __name__ == "__main__":
    unittest.main()
