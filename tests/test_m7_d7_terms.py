"""Offline tests of scripts/m7_d7_terms.py (M7 step S0): stub server only, no network, no model."""
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import m7_d7_terms as d7  # noqa: E402
from kafka_sre_rag import config, profiles, prompts  # noqa: E402

ITEMS = [json.loads(l) for l in (ROOT / "eval/m7/d7/items.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]


def run(*extra, out):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        rc = d7.main(["--stub", "--model", "qwen-test", "--out", str(out), *extra])
    return rc, buf.getvalue()


class TermsStep(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "st.jsonl"
        d7.STUB_LOG.clear()

    def tearDown(self):
        self.tmp.cleanup()

    def test_stub_run_writes_60_rows_with_prompt_hash(self):
        rc, text = run(out=self.out)
        self.assertEqual(rc, 0, text)
        rows = d7.read_rows(self.out)
        self.assertEqual(len(rows), 60)
        self.assertEqual([r["id"] for r in rows], [i["id"] for i in ITEMS])
        for r, it in zip(rows, ITEMS):
            sha, chars = eval2.messages_fingerprint(prompts.expand_messages(eval2.full_question(it)))
            self.assertEqual((r["prompt_sha256"], r["prompt_chars"]), (sha, chars))
            self.assertTrue(r["stub"])
            self.assertEqual(r["model"], "qwen-test")
        self.assertEqual(text.count("[d7-terms] "), 62)   # a header, a line per scenario, the done line
        marker = json.loads(Path(str(self.out) + ".done").read_text())
        self.assertEqual(marker["sha256"], d7.sha256_file(self.out))
        self.assertIn(marker["sha256"], text)

    def test_second_run_writes_nothing_new(self):
        run(out=self.out)
        before = self.out.read_bytes()
        n_calls = len(d7.STUB_LOG)
        rc, text = run(out=self.out)
        self.assertEqual(rc, 0)
        self.assertEqual(self.out.read_bytes(), before)
        self.assertEqual(len(d7.STUB_LOG), n_calls)   # no request at all
        self.assertIn("60 already done, 0 to do", text)

    def test_calls_equal_the_runtime_expansion_call(self):
        run(out=self.out)
        cfg = config.load()
        self.assertEqual(len(d7.STUB_LOG), 60)
        for body, it in zip(d7.STUB_LOG, ITEMS):
            self.assertEqual(body["messages"], prompts.expand_messages(eval2.full_question(it)))
            self.assertEqual(body["max_tokens"], cfg["expansion"]["max_tokens"])
            self.assertEqual(body["temperature"], cfg["expansion"]["temperature"])
            self.assertEqual(body["chat_template_kwargs"], {"enable_thinking": False})
            self.assertEqual(body["model"], "qwen-test")

    def test_terms_equal_the_runtime_parser(self):
        run(out=self.out)
        for r in d7.read_rows(self.out):
            terms, bad = profiles.clean_terms(r["search_terms"])
            self.assertEqual((r["terms"], r["terms_malformed"]), (terms, bad))
            self.assertGreaterEqual(len(r["terms"]), 3)

    def test_raw_reply_keeps_the_think_block_search_terms_does_not(self):
        run(out=self.out)
        first = d7.read_rows(self.out)[0]
        self.assertTrue(first["raw_reply"].startswith("<think>planning</think>"))
        self.assertNotIn("<think>", first["search_terms"])
        self.assertEqual(first["search_terms"], first["raw_reply"].split("</think>", 1)[1].strip())

    def test_failure_stops_and_resume_keeps_earlier_rows(self):
        real = d7.make_generator
        calls = {"n": 0}

        def flaky(a, extra):
            g = real(a, extra)
            orig = g.chat

            def chat(*args, **kw):
                calls["n"] += 1
                if calls["n"] == 25:
                    raise RuntimeError("http://secret.example:1 down")
                return orig(*args, **kw)
            g.chat = chat
            return g
        with mock.patch.object(d7, "make_generator", flaky):
            rc, text = run(out=self.out)
        self.assertEqual(rc, 1)
        self.assertNotIn("secret.example", text)       # only the class name is printed
        self.assertEqual(len(d7.read_rows(self.out)), 24)
        self.assertFalse(Path(str(self.out) + ".done").exists())
        prefix = self.out.read_bytes()
        rc, _ = run(out=self.out)
        self.assertEqual(rc, 0)
        self.assertTrue(self.out.read_bytes().startswith(prefix))
        self.assertEqual(len(d7.read_rows(self.out)), 60)

    def test_refuses_other_model_and_the_real_path_for_a_stub(self):
        run(out=self.out)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            rc = d7.main(["--stub", "--model", "other", "--out", str(self.out)])
        self.assertEqual(rc, 2)
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            self.assertEqual(d7.main(["--stub", "--model", "m"]), 2)   # default path is the real one

    def test_thinking_must_be_off(self):
        rc, _ = run("--extra-body", '{"chat_template_kwargs": {"enable_thinking": true}}', out=self.out)
        self.assertEqual(rc, 2)
        self.assertFalse(self.out.exists())

    def test_no_url_no_run(self):
        with mock.patch.dict("os.environ", {}, clear=False) as env:
            env.pop("OMLX_URL", None)
            with self.assertRaises(SystemExit):
                d7.main(["--model", "m", "--out", str(self.out)])


if __name__ == "__main__":
    unittest.main()
