import contextlib
import io
import json
import os
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import __main__ as cli, config, pipeline, service  # noqa: E402
from kafka_sre_rag.generators import OpenAIChatGenerator  # noqa: E402

INDEX = Path(os.environ.get("RAG_TEST_INDEX") or config.index_file())
NEEDS_INDEX = unittest.skipUnless(INDEX.exists(), "frozen index absent (work/ is git-ignored)")

CHUNKS = [{"id": "a", "source": "apache-kafka", "title": "Consumer configs", "path": "raw/a/b.md",
           "text": "Set max.poll.interval.ms.", "bm25_rank": 1, "ce_score": 0.9},
          {"id": "b", "source": "strimzi", "title": "", "path": "raw/s/lag.md", "text": "Use kafka-consumer-groups.sh.",
           "bm25_rank": 3, "ce_score": 0.4}]


class StubGen:
    """First call: search terms. Second call: the answer, streamed in pieces when on_token is given."""
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"
    PIECES = ["<think>private</think>", "Check ", "the lag. ", "Done.\n"]

    def __init__(self, delay=0.0):
        self.calls, self.delay, self.log, self.n = [], delay, [], 0
        self.mu = threading.Lock()

    def chat(self, messages, max_tokens, on_token=None):
        with self.mu:
            self.n += 1
            first = self.n % 2 == 1
        self.calls.append((messages, max_tokens))
        self.log.append(("start", time.monotonic()))
        time.sleep(self.delay)
        try:
            if first:
                return "terms, a, b", 3, False
            if on_token:
                for p in self.PIECES:
                    on_token(p)
            return "Check the lag. Done.", 9, False
        finally:
            self.log.append(("end", time.monotonic()))


class Running:
    def __init__(self, gen, **kw):
        self.gen = gen
        self.svc = service.Service(gen, config.load(), verified=True, **kw)
        self.srv = service.make_server(self.svc, "127.0.0.1", 0)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        self.t = threading.Thread(target=self.srv.serve_forever, daemon=True)

    def __enter__(self):
        self.t.start()
        return self

    def __exit__(self, *a):
        self.srv.shutdown()
        self.srv.server_close()


def post(url, body, **kw):
    return httpx.post(url + "/v1/chat/completions", json=body, timeout=30, trust_env=False, **kw)


def body(q="How do I check lag?", **kw):
    return {"model": "kafka-sre", "messages": [{"role": "user", "content": q}], **kw}


def sse_events(text):
    return [e[5:].strip() for e in text.split("\n\n") if e.startswith("data:")]


@mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS)
class TestService(unittest.TestCase):
    def test_non_streaming(self, sel):
        with Running(StubGen()) as s:
            r = post(s.url, body())
            self.assertEqual(r.status_code, 200)
            d = r.json()
            self.assertEqual(d["choices"][0]["message"]["content"], "Check the lag. Done.")
            self.assertEqual(d["kafka_sre"]["chunk_ids"], ["a", "b"])
            self.assertEqual(d["kafka_sre"]["sources"][0]["ce_score"], 0.9)
            self.assertIn("not sent", d["kafka_sre"]["history"])
            self.assertEqual(r.headers["x-kafka-sre-history"], "not-sent")
            self.assertEqual(d["kafka_sre"]["expansion"], "terms, a, b")

    def test_streaming_equals_non_streaming_and_hides_think(self, sel):
        with Running(StubGen()) as s:
            plain = post(s.url, body()).json()["choices"][0]["message"]["content"]
            r = post(s.url, body(stream=True))
            self.assertEqual(r.headers["content-type"], "text/event-stream")
            ev = sse_events(r.text)
            self.assertEqual(ev[-1], "[DONE]")
            objs = [json.loads(e) for e in ev[:-1]]
            text = "".join(o["choices"][0]["delta"].get("content") or "" for o in objs)
            self.assertEqual(text, plain)
            self.assertNotIn("private", r.text)
            self.assertEqual(objs[-1]["choices"][0]["finish_reason"], "stop")
            self.assertEqual(objs[-1]["kafka_sre"]["chunk_ids"], ["a", "b"])

    def test_models_and_health(self, sel):
        with Running(StubGen()) as s:
            m = httpx.get(s.url + "/v1/models", trust_env=False).json()
            self.assertEqual([x["id"] for x in m["data"]], ["kafka-sre", "kafka-sre-strict"])
            h = httpx.get(s.url + "/healthz", trust_env=False).json()
            cfg = config.load()
            self.assertEqual(h["index_sha256"], cfg["index"]["sha256"])
            self.assertEqual(h["reranker_revision"], cfg["retrieval"]["reranker"]["revision"])
            self.assertEqual((h["backend"], h["model"], h["verify_passed"]), ("stub", "stub", True))

    def test_mode_by_model_suffix_and_field(self, sel):
        with Running(StubGen()) as s:
            d = post(s.url, body(model="kafka-sre-strict")).json()
            self.assertEqual(d["kafka_sre"]["mode"], "strict")
            self.assertIn("mode strict", d["kafka_sre"]["unmeasured_reasons"])
            self.assertEqual(post(s.url, body(mode="strict")).json()["kafka_sre"]["mode"], "strict")
            self.assertEqual(post(s.url, body()).json()["kafka_sre"]["mode"], "default")
            self.assertEqual(post(s.url, body(mode="loose")).status_code, 400)

    def test_only_last_user_message_is_used(self, sel):
        g = StubGen()
        with Running(g) as s:
            msgs = [{"role": "system", "content": "be nice"}, {"role": "user", "content": "first question"},
                    {"role": "assistant", "content": "earlier answer"}, {"role": "user", "content": "the real one"}]
            post(s.url, {"model": "kafka-sre", "messages": msgs})
        sent = json.dumps(g.calls)
        self.assertIn("the real one", sent)
        self.assertNotIn("first question", sent)
        self.assertNotIn("earlier answer", sent)
        self.assertNotIn("be nice", sent)

    def test_bad_requests(self, sel):
        with Running(StubGen()) as s:
            self.assertEqual(post(s.url, {"messages": []}).status_code, 400)
            self.assertEqual(post(s.url, {"messages": [{"role": "assistant", "content": "x"}]}).status_code, 400)
            r = httpx.post(s.url + "/v1/chat/completions", content=b"{nope", trust_env=False)
            self.assertEqual(r.status_code, 400)
            self.assertEqual(httpx.get(s.url + "/nothing", trust_env=False).status_code, 404)

    def test_wrong_host_header_refused(self, sel):
        with Running(StubGen()) as s:
            r = httpx.get(s.url + "/healthz", headers={"Host": "evil.example"}, trust_env=False)
            self.assertEqual(r.status_code, 403)

    def test_concurrent_requests_are_served_one_after_the_other(self, sel):
        g = StubGen(delay=0.15)
        with Running(g) as s:
            res = []
            ts = [threading.Thread(target=lambda: res.append(post(s.url, body()).status_code)) for _ in range(2)]
            [t.start() for t in ts]
            [t.join() for t in ts]
        self.assertEqual(res, [200, 200])
        kinds = [k for k, _ in g.log]
        self.assertEqual(kinds, ["start", "end"] * 4)  # no model call began before the previous one ended
        # both requests' two calls each: the second request's first call starts after the first request's last ends
        self.assertEqual(g.n, 4)

    def test_model_error_is_http_502_with_the_message(self, sel):
        class Bad(StubGen):
            def chat(self, *a, **k):
                from kafka_sre_rag.generators import ServerError
                raise ServerError("cannot reach http://x (ConnectError)")
        with Running(Bad()) as s:
            r = post(s.url, body())
            self.assertEqual(r.status_code, 502)
            self.assertIn("cannot reach", r.json()["error"]["message"])
            r = post(s.url, body(stream=True))
            self.assertEqual(r.status_code, 502)


class TestBind(unittest.TestCase):
    def test_non_loopback_refused_without_flag(self):
        svc = service.Service(StubGen(), config.load())
        for host in ("0.0.0.0", "192.0.2.10", "example.org", ""):
            with self.assertRaises(ValueError):
                service.make_server(svc, host, 0)
        self.assertTrue(service.is_loopback_bind("127.0.0.2") and service.is_loopback_bind("::1"))
        for host in ("127.0.0.1", "localhost"):
            service.make_server(svc, host, 0).server_close()

    def test_cli_refuses_and_warns(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = cli.main(["serve", "--dry-run", "--host", "0.0.0.0", "--port", "0"])
        self.assertEqual(rc, 2)
        self.assertIn("loopback", err.getvalue())
        err = io.StringIO()
        with mock.patch("kafka_sre_rag.service.Service.warm_up", return_value=0.0), \
                mock.patch("kafka_sre_rag.service.ThreadingHTTPServer.serve_forever", side_effect=KeyboardInterrupt), \
                contextlib.redirect_stderr(err):
            rc = cli.main(["serve", "--dry-run", "--host", "0.0.0.0", "--port", "0", "--allow-non-local"])
        self.assertEqual(rc, 0)
        self.assertIn("WARNING", err.getvalue())


class StubModelServer:
    """An OpenAI-compatible server on an ephemeral loopback port; records the request bodies."""

    def __init__(self, think=False):
        outer = self
        self.bodies = []

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_POST(self):
                b = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                outer.bodies.append(b)
                first = len(outer.bodies) % 2 == 1
                parts = ["terms, a"] if first else (["<think>x</think>"] if think else []) + ["Answer ", "text."]
                if b.get("stream"):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    for p in parts:
                        d = {"model": "m", "choices": [{"delta": {"content": p}, "finish_reason": None}]}
                        self.wfile.write(f"data: {json.dumps(d)}\n\n".encode())
                    self.wfile.write(b'data: {"model":"m","choices":[{"delta":{},"finish_reason":"stop"}]}\n\n')
                    self.wfile.write(b"data: [DONE]\n\n")
                else:
                    data = json.dumps({"model": "m", "choices": [{"message": {"content": "".join(parts)},
                                                                  "finish_reason": "stop"}]}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}/v1"

    def __enter__(self):
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *a):
        self.srv.shutdown()
        self.srv.server_close()


@mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS)
class TestThroughModelServerAndAskClient(unittest.TestCase):
    def test_service_with_stub_model_server_and_ask_service_url(self, sel):
        with StubModelServer(think=True) as ms:
            gen = OpenAIChatGenerator(ms.url, "m", api_key="k")
            with Running(gen) as s:
                direct = post(s.url, body()).json()["choices"][0]["message"]["content"]
                self.assertEqual(direct, "Answer text.")
                out = io.StringIO()
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                    rc = cli.main(["ask", "--service-url", s.url, "--sources", "q?"])
                self.assertEqual(rc, 0)
                self.assertIn("Answer text.", out.getvalue())
                self.assertIn("Sources:", out.getvalue())
                self.assertIn("[1] apache-kafka - Consumer configs (raw/a/b.md) ce=0.900", out.getvalue())
                out2 = io.StringIO()
                with contextlib.redirect_stdout(out2), contextlib.redirect_stderr(io.StringIO()):
                    rc = cli.main(["ask", "--service-url", s.url, "--stream", "--sources", "q?"])
                self.assertEqual(rc, 0)
                self.assertTrue(out2.getvalue().startswith("Answer text.\n"))
                self.assertNotIn("<think>", out2.getvalue())
                self.assertIn("Sources:", out2.getvalue())
                out3 = io.StringIO()
                with contextlib.redirect_stdout(out3):
                    cli.main(["ask", "--service-url", s.url, "--json", "q?"])
                j = json.loads(out3.getvalue())
                self.assertEqual(j["text"], "Answer text.")
                self.assertEqual(j["expansion"], "terms, a")

    def test_ask_service_url_errors(self, sel):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = cli.main(["ask", "--service-url", "http://127.0.0.1:1", "q?"])
        self.assertEqual(rc, 1)
        self.assertIn("error: cannot reach the service", err.getvalue())
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(cli.main(["ask", "--service-url", "http://127.0.0.1:1", "--model", "m", "q?"]), 2)


class TestSamePrompt(unittest.TestCase):
    """The prompts that reach the generator through the service are those of a direct ask."""

    def direct(self, mode, rerank):
        g = StubGen()
        pipeline.ask("How do I check consumer lag?", g, config.load(), mode=mode, rerank=rerank)
        return g.calls

    def through_service(self, mode, rerank):
        g = StubGen()
        with Running(g, rerank=rerank) as s:
            self.assertEqual(post(s.url, body("How do I check consumer lag?", mode=mode)).status_code, 200)
        return g.calls

    def check(self, mode, rerank):
        a, b = self.direct(mode, rerank), self.through_service(mode, rerank)
        self.assertEqual(json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True))
        self.assertEqual(len(a), 2)

    def test_patched_retrieval(self):
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS):
            for mode in ("default", "strict"):
                self.check(mode, True)

    @NEEDS_INDEX
    def test_real_index_bm25_order(self):
        for mode in ("default", "strict"):
            self.check(mode, False)


if __name__ == "__main__":
    unittest.main()
