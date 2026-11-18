"""A server on this machine is never reached through a proxy; a failing remote server names the proxy."""
import http.server
import json
import os
import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kafka_sre_rag.generators import OpenAIChatGenerator, ServerError  # noqa: E402

PROXY_ENV = {"HTTP_PROXY": "http://127.0.0.1:9", "HTTPS_PROXY": "http://127.0.0.1:9",
             "ALL_PROXY": "http://127.0.0.1:9", "http_proxy": "http://127.0.0.1:9",
             "https_proxy": "http://127.0.0.1:9", "all_proxy": "http://127.0.0.1:9"}
MSG = [{"role": "user", "content": "q"}]


def has_proxy_mounts(g):
    return bool(g.client._mounts)


class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, obj):
        b = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        self._send({"data": [{"id": "m"}]})

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self._send({"model": "m", "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"completion_tokens": 1}})

    def log_message(self, *a):
        pass


class LoopbackIgnoresProxy(unittest.TestCase):
    def build(self, url):
        with mock.patch.dict(os.environ, PROXY_ENV):
            return OpenAIChatGenerator(url, "m")

    def test_loopback_hosts_ignore_proxy_variables(self):
        for url in ("http://localhost:11434/v1", "http://127.0.0.1:11434/v1", "http://127.5.6.7:1/v1",
                    "http://[::1]:11434/v1"):
            with self.subTest(url=url):
                g = self.build(url)
                self.assertTrue(g.local)
                self.assertFalse(g.client._trust_env)
                self.assertFalse(has_proxy_mounts(g))

    def test_non_loopback_keeps_environment(self):
        for url in ("http://llm.example.invalid/v1", "http://localhost.example.invalid/v1"):
            with self.subTest(url=url):
                g = self.build(url)
                self.assertFalse(g.local)
                self.assertTrue(g.client._trust_env)
                self.assertTrue(has_proxy_mounts(g))

    def test_no_proxy_does_not_matter_for_loopback(self):
        env = dict(PROXY_ENV, NO_PROXY="", no_proxy="")
        with mock.patch.dict(os.environ, env):
            g = OpenAIChatGenerator("http://127.0.0.1:1/v1", "m")
        self.assertFalse(has_proxy_mounts(g))

    def test_request_arrives_with_proxy_to_closed_port(self):
        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            with mock.patch.dict(os.environ, dict(PROXY_ENV, NO_PROXY="", no_proxy="")):
                g = OpenAIChatGenerator(f"http://127.0.0.1:{srv.server_address[1]}/v1", "m")
                out, toks, trunc = g.chat(MSG, 5)  # model list and chat call
        finally:
            srv.shutdown()
            srv.server_close()
        self.assertEqual(out, "ok")


class ErrorText(unittest.TestCase):
    def fail(self, url, env):
        def h(req):
            raise httpx.ConnectError("refused", request=req)
        clean = {k: v for k, v in os.environ.items() if k.upper() not in PROXY_ENV and k.upper() != "NO_PROXY"}
        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.update(clean)
            os.environ.update(env)
            g = OpenAIChatGenerator(url, "m", transport=httpx.MockTransport(h))
            with self.assertRaises(ServerError) as cm:
                g.chat(MSG, 5)
        return str(cm.exception)

    def test_remote_with_proxy_names_proxy_without_address(self):
        msg = self.fail("http://llm.example.invalid/v1", {"HTTPS_PROXY": "http://secret-proxy.invalid:3128"})
        self.assertIn("a proxy is configured in the environment; add the server's host to NO_PROXY", msg)
        self.assertNotIn("secret-proxy", msg)
        self.assertNotIn("3128", msg)
        self.assertEqual(len(msg.splitlines()), 1)

    def test_remote_without_proxy_has_no_hint(self):
        msg = self.fail("http://llm.example.invalid/v1", {})
        self.assertEqual(msg, "cannot reach http://llm.example.invalid/v1 (ConnectError)")

    def test_loopback_with_proxy_has_no_hint(self):
        msg = self.fail("http://127.0.0.1:9/v1", {"HTTP_PROXY": "http://secret-proxy.invalid:3128"})
        self.assertNotIn("proxy", msg)


if __name__ == "__main__":
    unittest.main()
