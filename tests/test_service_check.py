"""serve --check and the per-request `check` field. Offline: stub generator, retrieval patched, fixture catalogues."""
import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from kafka_sre_rag import config, service  # noqa: E402
from test_ask_check import BAD, CHUNKS, GOOD, make_catalogue_dir  # noqa: E402
from test_service import Running, body, post, sse_events  # noqa: E402


class AnsGen:
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"

    def __init__(self, text):
        self.text, self.n, self.calls = text, 0, []

    def chat(self, messages, max_tokens, on_token=None):
        self.n += 1
        self.calls.append(json.dumps(messages, sort_keys=True))
        if self.n % 2 == 1:
            return "terms", 1, False
        if on_token:
            for i in range(0, len(self.text), 7):
                on_token(self.text[i:i + 7])
        return self.text, 5, False


@mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS)
class TestServiceCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cat = make_catalogue_dir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cat, ignore_errors=True)

    def test_off_by_default_and_per_request(self, sel):
        with Running(AnsGen(BAD), catalogues=str(self.cat)) as s:
            d = post(s.url, body()).json()
            self.assertEqual(d["choices"][0]["message"]["content"], BAD)
            self.assertNotIn("check", d["kafka_sre"])
            d = post(s.url, body(check=True)).json()
            c = d["choices"][0]["message"]["content"]
            self.assertTrue(c.startswith(BAD + " [not confirmed: `log.retention.hours`"))  # R1 mark after the sentence
            self.assertTrue(c.endswith("\n\n" + d["kafka_sre"]["check"]["note"]))
            self.assertIn("Could not confirm", c)
            self.assertIn("log.retention.hours", [f["text"] for f in d["kafka_sre"]["check"]["findings"]])
            self.assertEqual(post(s.url, body(check="yes")).status_code, 400)

    def test_serve_check_default_and_opt_out(self, sel):
        with Running(AnsGen(GOOD), check=True, catalogues=str(self.cat)) as s:
            d = post(s.url, body()).json()
            self.assertIn("confirmed against the catalogues", d["choices"][0]["message"]["content"])
            self.assertEqual(d["kafka_sre"]["check"]["catalogue_hash"],
                             service.AnswerChecker(self.cat).catalogue_hash)
            d = post(s.url, body(check=False)).json()
            self.assertEqual(d["choices"][0]["message"]["content"], GOOD)

    def test_prompt_unchanged_and_note_last(self, sel):
        g0, g1 = AnsGen(BAD), AnsGen(BAD)
        with Running(g0, catalogues=str(self.cat)) as s:
            post(s.url, body())
        with Running(g1, catalogues=str(self.cat)) as s:
            d = post(s.url, body(check=True)).json()
        self.assertEqual(g0.calls, g1.calls)
        c = d["choices"][0]["message"]["content"]
        self.assertTrue(c.startswith(BAD))
        self.assertTrue(c.endswith("\n\n" + d["kafka_sre"]["check"]["note"]))

    def test_streaming_note_is_last_content_before_done(self, sel):
        with Running(AnsGen(BAD), catalogues=str(self.cat)) as s:
            plain = post(s.url, body(check=True)).json()["choices"][0]["message"]["content"]
            r = post(s.url, body(check=True, stream=True))
            ev = sse_events(r.text)
            self.assertEqual(ev[-1], "[DONE]")
            objs = [json.loads(e) for e in ev[:-1]]
            text = "".join(o["choices"][0]["delta"].get("content") or "" for o in objs)
            # the streamed prose is as written (no inline mark: it was shown before the check); the note follows
            self.assertEqual(text, BAD + "\n\n" + objs[-1]["kafka_sre"]["check"]["note"])
            self.assertTrue(plain.startswith(BAD + " [not confirmed:"))
            self.assertTrue(text.startswith(BAD + "\n\nCould not confirm"))
            self.assertEqual(objs[-1]["choices"][0]["finish_reason"], "stop")
            self.assertIn("check", objs[-1]["kafka_sre"])

    def test_missing_catalogues(self, sel):
        with Running(AnsGen(BAD), catalogues="/nonexistent/x") as s:
            r = post(s.url, body(check=True))
            self.assertEqual(r.status_code, 400)
            self.assertIn("build_catalogues.py", r.json()["error"]["message"])
            r = post(s.url, body(check=True, stream=True))
            self.assertEqual(r.status_code, 400)
            self.assertEqual(post(s.url, body()).status_code, 200)  # unchecked requests still work

    def test_serve_check_fails_at_start(self, sel):
        with self.assertRaises(service.CatalogueError):
            service.Service(AnsGen(BAD), config.load(), check=True, catalogues="/nonexistent/x")


if __name__ == "__main__":
    unittest.main()
