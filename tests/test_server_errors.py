import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import __main__ as cli, config  # noqa: E402
from kafka_sre_rag.generators import OpenAIChatGenerator, ServerError  # noqa: E402

MSG = [{"role": "user", "content": "x"}]
KEY = "sk-test-secret-key"


def chat_ok(model="m"):
    return httpx.Response(200, json={"model": model, "choices": [{"message": {"content": "hi"},
                                                                  "finish_reason": "stop"}],
                                     "usage": {"completion_tokens": 3}})


def gen(handler, model="m"):
    return OpenAIChatGenerator("http://example.invalid/v1", model, api_key=KEY,
                               transport=httpx.MockTransport(handler))


class ModelList(unittest.TestCase):
    def handler(self, names=None, list_status=200, answer_model="m", log=None):
        def h(req):
            if log is not None:
                log.append((req.method, req.url.path))
            if req.method == "GET":
                if list_status != 200:
                    return httpx.Response(list_status, text="not found")
                return httpx.Response(200, json={"data": [{"id": n} for n in names]})
            return chat_ok(answer_model)
        return h

    def test_name_present(self):
        log = []
        g = gen(self.handler(["a", "m"], log=log))
        self.assertEqual(g.chat(MSG, 5), ("hi", 3, False))
        g.chat(MSG, 5)
        self.assertEqual(log, [("GET", "/v1/models"), ("POST", "/v1/chat/completions"),
                               ("POST", "/v1/chat/completions")])  # listed once, before the first chat call

    def test_name_absent(self):
        log = []
        names = [f"n{i}" for i in range(25)]
        with self.assertRaises(ServerError) as cm:
            gen(self.handler(names, log=log)).chat(MSG, 5)
        msg = str(cm.exception)
        self.assertIn("'m'", msg)
        self.assertIn("n19", msg)
        self.assertNotIn("n20", msg)
        self.assertEqual(log, [("GET", "/v1/models")])  # no chat call was made

    def test_list_endpoint_404_continues(self):
        self.assertEqual(gen(self.handler(list_status=404)).chat(MSG, 5), ("hi", 3, False))

    def test_response_model_differs(self):
        with self.assertRaises(ServerError) as cm:
            gen(self.handler(["m"], answer_model="other")).chat(MSG, 5)
        self.assertIn("'other'", str(cm.exception))
        self.assertIn("'m'", str(cm.exception))

    def test_response_model_differs_streaming(self):
        body = 'data: {"model":"other","choices":[{"delta":{"content":"a"}}]}\n\ndata: [DONE]\n\n'

        def h(req):
            return httpx.Response(404) if req.method == "GET" else httpx.Response(200, text=body)
        with self.assertRaises(ServerError):
            gen(h).chat(MSG, 5, on_token=lambda t: None)


class Failures(unittest.TestCase):
    def test_connection_failure(self):
        def h(req):
            raise httpx.ConnectError("refused", request=req)
        with self.assertRaises(ServerError) as cm:
            gen(h).chat(MSG, 5)
        self.assertIn("cannot reach http://example.invalid/v1", str(cm.exception))
        self.assertNotIn(KEY, str(cm.exception))

    def test_http_error_status_with_server_text(self):
        def h(req):
            if req.method == "GET":
                return httpx.Response(404)
            return httpx.Response(500, json={"error": {"message": "unable to load model " + "x" * 400}})
        with self.assertRaises(ServerError) as cm:
            gen(h).chat(MSG, 5)
        msg = str(cm.exception)
        self.assertIn("HTTP 500", msg)
        self.assertIn("unable to load model", msg)
        self.assertLessEqual(len(msg.split(": ", 1)[1]), 300 + len("HTTP 500: "))
        self.assertNotIn(KEY, msg)

    def test_http_error_status_streaming(self):
        def h(req):
            return httpx.Response(404) if req.method == "GET" else httpx.Response(503, text="overloaded")
        with self.assertRaises(ServerError) as cm:
            gen(h).chat(MSG, 5, on_token=lambda t: None)
        self.assertIn("HTTP 503: overloaded", str(cm.exception))

    def test_ask_exits_nonzero_without_traceback(self):
        def h(req):
            raise httpx.ConnectError("refused", request=req)
        err = io.StringIO()
        with mock.patch.object(cli, "make_generator", return_value=gen(h)), \
                mock.patch("kafka_sre_rag.config.verify"), contextlib.redirect_stderr(err):
            rc = cli.main(["ask", "q", "--backend", "openai", "--model", "m"])
        self.assertEqual(rc, 1)
        self.assertIn("error: cannot reach", err.getvalue())
        self.assertNotIn("Traceback", err.getvalue())

    def test_ask_wrong_model_exits_nonzero(self):
        g = gen(lambda req: httpx.Response(200, json={"data": [{"id": "other"}]}))
        err = io.StringIO()
        with mock.patch.object(cli, "make_generator", return_value=g), \
                mock.patch("kafka_sre_rag.config.verify"), contextlib.redirect_stderr(err):
            rc = cli.main(["ask", "q", "--backend", "openai", "--model", "m"])
        self.assertEqual(rc, 1)
        self.assertIn("not on the server", err.getvalue())
        self.assertNotIn("Traceback", err.getvalue())


class RerankerCache(unittest.TestCase):
    REV = "953dc6f6" + "0" * 32
    CFG = {"retrieval": {"reranker": {"model": "org/rr", "revision": REV}}}

    def cached(self, tmp):
        with mock.patch.dict("os.environ", {"HF_HUB_CACHE": tmp}):
            return config.reranker_revision_cached(self.CFG)

    def test_snapshot_dir_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "models--org--rr/snapshots" / self.REV).mkdir(parents=True)
            self.assertEqual(self.cached(tmp), self.REV)

    def test_refs_main_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            refs = Path(tmp) / "models--org--rr/refs"
            refs.mkdir(parents=True)
            (refs / "main").write_text("abc123\n")
            self.assertEqual(self.cached(tmp), "abc123")

    def test_absent_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(self.cached(tmp))


class DryRunNotice(unittest.TestCase):
    def test_stub_flushes_before_notice(self):
        out = io.StringIO()
        out.flush = mock.Mock()
        cli.StubGenerator(out).chat(MSG, 5)
        out.flush.assert_called()


if __name__ == "__main__":
    unittest.main()
