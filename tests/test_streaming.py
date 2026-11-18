import contextlib
import io
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import __main__ as cli, config  # noqa: E402
from kafka_sre_rag.generators import (EmptyAnswerError, OpenAIChatGenerator, ServerError, ThinkFilter,  # noqa: E402
                                      strip_think)

MSG = [{"role": "user", "content": "x"}]
CHUNKS = [{"id": "a", "source": "apache-kafka", "title": "T", "path": "raw/a.md", "text": "Use lag.",
           "bm25_rank": 1, "ce_score": 0.5}]


def sse(pieces, model="m", finish="stop", extra_delta=None):
    lines = []
    for p in pieces:
        d = {"model": model, "choices": [{"delta": {"content": p}, "finish_reason": None}]}
        lines.append("data: " + json.dumps(d))
    for d in extra_delta or []:
        lines.append("data: " + json.dumps({"model": model, "choices": [{"delta": d, "finish_reason": None}]}))
    lines.append("data: " + json.dumps({"model": model, "choices": [{"delta": {}, "finish_reason": finish}],
                                       "usage": {"completion_tokens": 7}}))
    lines.append("data: [DONE]")
    return "\n\n".join(lines) + "\n\n"


def gen_for(pieces, **kw):
    full = "".join(pieces)

    def h(req):
        if req.method == "GET":
            return httpx.Response(404)
        body = json.loads(req.content)
        if body.get("stream"):
            return httpx.Response(200, text=sse(pieces, **kw))
        return httpx.Response(200, json={"model": "m", "choices": [{"message": {"content": full},
                                                                    "finish_reason": "stop"}],
                                         "usage": {"completion_tokens": 7}})
    return OpenAIChatGenerator("http://example.invalid/v1", "m", api_key="k", transport=httpx.MockTransport(h))


class TestThinkFilter(unittest.TestCase):
    def run_filter(self, text, step):
        f, out = ThinkFilter(), ""
        for i in range(0, len(text), step):
            out += f.feed(text[i:i + step])
        return out + f.finish()

    def test_equals_strip_think_for_every_chunking(self):
        cases = ["  hi <think>a\nb</think>\n\n there  ", "<think>x</think>ans", "a<think>never closed",
                 "plain text", "<think>a</think><think>b</think>  z ", "   ", "a <thi nk> b", "<think>",
                 "line one\n\nline two\n"]
        for c in cases:
            for step in (1, 2, 3, 7, 1000):
                self.assertEqual(self.run_filter(c, step), strip_think(c), (c, step))

    def test_think_text_never_emitted_before_finish(self):
        f = ThinkFilter()
        seen = f.feed("<thi") + f.feed("nk>secret reasoning") + f.feed(" more</thi") + f.feed("nk>answer")
        self.assertNotIn("secret", seen)
        self.assertEqual(seen + f.finish(), "answer")


class TestServerStreaming(unittest.TestCase):
    def test_streamed_equals_non_streamed(self):
        pieces = ["Check ", "the lag ", "with kafka-consumer-groups.sh.\n", "Then restart."]
        g = gen_for(pieces)
        got = []
        text_s, toks, trunc = g.chat(MSG, 50, on_token=got.append)
        text_n, _, _ = g.chat(MSG, 50)
        self.assertEqual(text_s, text_n)
        self.assertEqual("".join(got), "".join(pieces))
        self.assertEqual(toks, 7)

    def test_think_block_not_streamed_to_the_user(self):
        pieces = ["<thi", "nk>I should look at ", "the lag</th", "ink>\n\n", "Answer ", "text. "]
        g = gen_for(pieces)
        flt, shown = ThinkFilter(), []
        text, _, _ = g.chat(MSG, 50, on_token=lambda p: shown.append(flt.feed(p)))
        shown.append(flt.finish())
        self.assertEqual("".join(shown), "Answer text.")
        self.assertEqual(text, "Answer text.")
        self.assertNotIn("lag", "".join(shown))

    def test_reasoning_content_deltas_not_shown(self):
        g = gen_for(["Answer."], extra_delta=[{"reasoning_content": "private thoughts"}])
        got = []
        g.chat(MSG, 50, on_token=got.append)
        self.assertEqual("".join(got), "Answer.")

    def test_empty_stream_is_the_empty_answer_error(self):
        g = gen_for([], finish="length")
        with self.assertRaises(EmptyAnswerError) as cm:
            g.chat(MSG, 50, on_token=lambda p: None)
        self.assertIn("token limit", str(cm.exception))

    def test_stream_model_mismatch_and_http_error(self):
        with self.assertRaises(ServerError):
            gen_for(["a"], model="other").chat(MSG, 5, on_token=lambda p: None)

        def h(req):
            return httpx.Response(503, json={"error": {"message": "busy"}})
        g = OpenAIChatGenerator("http://example.invalid/v1", "m", transport=httpx.MockTransport(h))
        g._model_checked = True
        with self.assertRaisesRegex(ServerError, "HTTP 503: busy"):
            g.chat(MSG, 5, on_token=lambda p: None)


class TestAskStreamFlag(unittest.TestCase):
    def ask(self, pieces, *flags):
        gen = gen_for(pieces)
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(cli, "make_generator", return_value=gen), \
                mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS), \
                mock.patch.object(config, "verify", return_value={}), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = cli.main(["ask", *flags, "--backend", "openai", "--model", "m", "q?"])
        return rc, out.getvalue(), err.getvalue()

    def test_stream_output_equals_plain_output_and_sources_follow(self):
        pieces = ["<think>hidden</think>\n", "One. ", "Two.\n"]
        rc1, plain, _ = self.ask(pieces, "--sources")
        rc2, streamed, _ = self.ask(pieces, "--stream", "--sources")
        self.assertEqual((rc1, rc2), (0, 0))
        self.assertIn("One. Two.", streamed)
        self.assertNotIn("hidden", streamed)
        self.assertTrue(streamed.index("One. Two.") < streamed.index("Sources:"))
        self.assertIn("[1] apache-kafka", streamed)

    def test_stream_empty_answer_exit_2(self):
        rc, out, err = self.ask([], "--stream")
        self.assertEqual(rc, 2)
        self.assertIn("error: the model returned no answer text", err)


if __name__ == "__main__":
    unittest.main()
