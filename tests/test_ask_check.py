"""ask --check: the note, the JSON field, the missing-catalogues error, and that the prompt and the answer text
do not change. Offline: stub generator, retrieval patched, fixture catalogues."""
import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import __main__ as cli, config, service  # noqa: E402
from kafka_sre_rag.catalogue import manifest_for  # noqa: E402

HERE = Path(__file__).parent
CHUNKS = [{"id": "a", "source": "apache-kafka", "title": "Consumer", "path": "raw/a.md",
           "text": "Set max.poll.interval.ms.", "bm25_rank": 1, "ce_score": 0.9}]
BAD = "Set `log.retention.hours` on the broker."
GOOD = "Raise `max.poll.interval.ms` on the consumer."


def make_catalogue_dir():
    d = Path(tempfile.mkdtemp())
    for sub in ("catalogues", "catalogues_ext"):
        for f in (HERE / "fixtures" / sub).glob("*.json"):
            shutil.copy(f, d / f.name)
    (d / "CATALOGUES.json").write_text(json.dumps(manifest_for(d)))
    return d


class RecGen:
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"

    def __init__(self, answer):
        self.answer, self.calls = answer, []

    def chat(self, messages, max_tokens, on_token=None):
        self.calls.append(json.dumps(messages, sort_keys=True))
        if len(self.calls) % 2 == 1:
            return "terms", 1, False
        if on_token:
            on_token(self.answer)
        return self.answer, 5, False


def run(argv, answer):
    a = cli.build_parser().parse_args(["ask", "How do I check lag?", *argv])
    gen, out = RecGen(answer), io.StringIO()
    ck = service.AnswerChecker(a.catalogues) if a.check else None
    with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS), contextlib.redirect_stdout(out):
        cli.run_one(a.question, gen, config.load(), a, ck)
    return gen, out.getvalue()


class TestAskCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cat = make_catalogue_dir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cat, ignore_errors=True)

    def test_prompt_and_answer_identical_with_and_without_check(self):
        g0, o0 = run(["--sources"], BAD)
        g1, o1 = run(["--check", "--catalogues", str(self.cat), "--sources"], BAD)
        self.assertEqual(g0.calls, g1.calls)           # both prompts, byte for byte
        self.assertTrue(o1.startswith(o0.split("\nSources:")[0]))  # the answer text is printed as before
        self.assertNotIn("Could not confirm", o0)

    def test_json_first_pass_text_identical_and_findings_structured(self):
        _, o0 = run(["--json"], BAD)
        _, o1 = run(["--json", "--check", "--catalogues", str(self.cat)], BAD)
        d0, d1 = json.loads(o0), json.loads(o1)
        self.assertNotIn("check", d0)
        self.assertEqual(d0["text"], d1["first_pass_text"])      # the model's text is kept, unchanged
        self.assertTrue(d1["text"].startswith(d0["text"] + " [not confirmed: `log.retention.hours`"))  # R1 mark
        c = d1["check"]
        self.assertEqual(c["catalogue_hash"], manifest_for(self.cat)["catalogue_hash"])
        self.assertIn("log.retention.hours", [f["text"] for f in c["findings"] if f["level"] == "invented"])
        self.assertIn("Could not confirm", c["note"])

    def test_note_between_answer_and_sources(self):
        _, o = run(["--check", "--catalogues", str(self.cat), "--sources"], BAD)
        self.assertLess(o.index(BAD), o.index("Could not confirm"))
        self.assertLess(o.index("Could not confirm"), o.index("Sources:"))

    def test_nothing_to_report_says_so_with_count(self):
        _, o = run(["--check", "--catalogues", str(self.cat)], GOOD)
        self.assertIn("confirmed against the catalogues (1 checked", o)
        self.assertNotIn("Could not confirm", o)

    def test_stream_note_after_streamed_answer(self):
        _, o = run(["--check", "--catalogues", str(self.cat), "--stream"], BAD)
        self.assertLess(o.index(BAD), o.index("Could not confirm"))

    def test_missing_catalogues_one_line_error(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = cli.main(["ask", "--dry-run", "--check", "--catalogues", "/nonexistent/x", "q"])
        self.assertEqual(rc, 2)
        self.assertEqual(len(err.getvalue().strip().splitlines()), 1)
        self.assertIn("python scripts/build_catalogues.py", err.getvalue())

    def test_missing_manifest_is_missing(self):
        d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, True)
        with self.assertRaises(service.CatalogueError):
            service.AnswerChecker(d)

    def test_verify_line(self):
        self.assertIn("not built", config.catalogues_line("/nonexistent/x"))
        self.assertIn(manifest_for(self.cat)["catalogue_hash"], config.catalogues_line(self.cat))

    def test_default_off(self):
        a = cli.build_parser().parse_args(["ask", "q"])
        self.assertFalse(a.check)
        self.assertIsNone(a.catalogues)


if __name__ == "__main__":
    unittest.main()
