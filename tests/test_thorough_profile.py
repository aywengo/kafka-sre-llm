"""M7 P1: the thorough answer profile (EXPERIMENTAL, unmeasured) in ask, in the evaluation harness and in the
`verify_pass run` command; the refusals; the default path unchanged. Stub models, fixture catalogues, retrieval
patched. Offline: no model server, no GPU."""
import contextlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
from kafka_sre_rag import __main__ as cli, config, pipeline, prompts, service, verify_pass as vp  # noqa: E402
from test_ask_check import make_catalogue_dir  # noqa: E402
from test_eval2 import Base, item  # noqa: E402
from test_verify_pass import CHUNKS, DRAFT, POLL_FIX, POLL_QUOTE, Q, rep  # noqa: E402

FULL = [dict(c, bm25_rank=i, ce_score=0.5) for i, c in enumerate(CHUNKS, 1)]


class Gen:
    """Stub: answers the three kinds of call by their system prompt and records the keyword arguments."""
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"

    def __init__(self, check_reply=None, draft=DRAFT):
        self.calls, self.check_reply, self.draft = [], check_reply or rep(), draft

    def chat(self, messages, max_tokens, on_token=None, **kw):
        self.calls.append({"system": messages[0]["content"], "max_tokens": max_tokens, "kw": dict(kw)})
        if messages[0]["content"] == prompts.EXPAND_SYSTEM:
            return "terms", 1, False
        if messages[0]["content"] == prompts.VERIFY_SYSTEM:
            return self.check_reply, 4, False
        if "info" in kw:
            kw["info"].update(reasoning_tokens=100, think_s=2.0)
        return self.draft, 50, False


class Cat(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = make_catalogue_dir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)


def run_cli(argv, gen, catalogues):
    """run_one as main() sets it up, with the stub model and retrieval patched."""
    a = cli.build_parser().parse_args(argv)
    assert cli.answer_profile_error(a) is None
    a.thorough_profile = cli.thorough_profile(a)
    a.thorough_checker = service.AnswerChecker(catalogues) if a.thorough_profile else None
    ck = service.AnswerChecker(catalogues) if a.check else None
    out, err = io.StringIO(), io.StringIO()
    with mock.patch("kafka_sre_rag.retrieval.select", return_value=FULL), contextlib.redirect_stdout(out), \
            contextlib.redirect_stderr(err):
        cli.run_one(a.question, gen, config.load(), a, ck)
    return out.getvalue(), err.getvalue()


class TestRefusals(unittest.TestCase):
    def refuse(self, *argv):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = cli.main(list(argv))
        self.assertEqual(rc, 2, err.getvalue())
        return err.getvalue()

    def test_refused_where_it_must_be(self):
        self.assertIn("serve", self.refuse("serve", "--answer-profile", "thorough"))
        self.assertIn("--stream", self.refuse("ask", "q", "--answer-profile", "thorough", "--stream"))
        self.assertIn("--service-url", self.refuse("ask", "q", "--answer-profile", "thorough",
                                                   "--service-url", "http://127.0.0.1:9"))
        self.assertIn("--revise", self.refuse("ask", "q", "--answer-profile", "thorough", "--check", "--revise"))
        self.assertIn("needs --answer-profile", self.refuse("ask", "q", "--no-cards"))
        self.assertIn("needs --answer-profile", self.refuse("chat", "--no-thinking"))
        self.assertIn("leaves nothing", self.refuse("ask", "q", "--answer-profile", "thorough", "--no-thinking",
                                                    "--no-verify"))

    def test_help_says_experimental_and_unmeasured(self):
        h = cli.build_parser()._subparsers._group_actions[0].choices["ask"].format_help()
        self.assertIn("EXPERIMENTAL and UNMEASURED", " ".join(h.split()))

    def test_profile_off_by_default(self):
        a = cli.build_parser().parse_args(["ask", "q"])
        self.assertIsNone(a.answer_profile)
        self.assertIsNone(cli.thorough_profile(a))
        self.assertIsNone(cli.answer_profile_error(a))


class TestAsk(Cat):
    def test_default_path_sends_no_thinking_argument(self):
        g = Gen()
        out, _ = run_cli(["ask", Q, "--json"], g, self.dir)
        self.assertEqual([c["kw"] for c in g.calls], [{}, {}])   # today's calls: chat(messages, max_tokens[, on_token])
        self.assertNotIn("answer_profile", json.loads(out))

    def test_thorough_calls_and_output(self):
        g = Gen(rep((2, POLL_QUOTE, POLL_FIX)))
        out, err = run_cli(["ask", Q, "--json", "--answer-profile", "thorough"], g, self.dir)
        self.assertEqual([(c["system"][:20], c["kw"].get("thinking"), c["max_tokens"]) for c in g.calls],
                         [(prompts.EXPAND_SYSTEM[:20], False, 300), (prompts.SYSTEM_PROMPT[:20], True, 2048 + 1500),
                          (prompts.VERIFY_SYSTEM[:20], False, vp.MAX_TOKENS)])
        d = json.loads(out)
        self.assertEqual(d["text"].split("\n")[1], POLL_FIX)
        ap = d["answer_profile"]
        self.assertEqual((ap["name"], ap["measured"], ap["thinking"], ap["verify"], ap["cards"]),
                         ("thorough", False, True, True, True))
        self.assertEqual(ap["draft_text"], DRAFT)
        self.assertEqual(ap["check"]["r2"]["status"], "accepted")
        self.assertEqual(ap["draft_info"]["reasoning_tokens"], 100)
        self.assertEqual(ap["seconds"]["thinking"], 2.0)
        self.assertFalse(d["measured"])
        self.assertTrue(any(r.startswith("answer profile thorough (EXPERIMENTAL, unmeasured")
                            for r in d["unmeasured_reasons"]))
        for k in ("draft_s", "thinking_s", "check_s"):
            self.assertIn(k, d["timings"])

    def test_text_output_notice_and_banner(self):
        out, err = run_cli(["ask", Q, "--answer-profile", "thorough", "--no-cards"], Gen(), self.dir)
        self.assertIn("UNMEASURED CONFIGURATION:", err)
        self.assertIn("answer profile thorough (EXPERIMENTAL, unmeasured: thinking on, checking pass on, cards off)", err)
        self.assertIn("(thorough profile (EXPERIMENTAL, unmeasured): checked against the excerpts; 0 changes proposed, "
                      "0 accepted)", out)
        self.assertTrue(out.startswith(DRAFT))

    def test_not_checked_after_the_deadline(self):
        g = Gen(rep((2, POLL_QUOTE, POLL_FIX)))
        with mock.patch.object(vp.Profile, "__init__", _profile_init(deadline_s=-1.0)):
            out, _ = run_cli(["ask", Q, "--answer-profile", "thorough"], g, self.dir)
        self.assertEqual(len(g.calls), 2)   # no checking call
        self.assertIn("not checked", out)
        self.assertTrue(out.startswith(DRAFT))

    def test_thinking_only_and_check_only(self):
        g = Gen()
        run_cli(["ask", Q, "--json", "--answer-profile", "thorough", "--no-verify"], g, self.dir)
        self.assertEqual([c["kw"].get("thinking") for c in g.calls], [False, True])
        g = Gen()
        run_cli(["ask", Q, "--json", "--answer-profile", "thorough", "--no-thinking"], g, self.dir)
        self.assertEqual([(c["kw"].get("thinking"), c["max_tokens"]) for c in g.calls],
                         [(False, 300), (False, 2048), (False, vp.MAX_TOKENS)])

    def test_check_marks_apply_to_the_final_text(self):
        out, _ = run_cli(["ask", Q, "--json", "--answer-profile", "thorough", "--check"], Gen(), self.dir)
        d = json.loads(out)
        self.assertIn("not confirmed: `log.retention.hours`", d["text"])   # R1 marks of today's --check stay
        self.assertEqual(d["answer_profile"]["draft_text"], DRAFT)


def _profile_init(**over):
    real = vp.Profile.__init__

    def init(self, *a, **kw):
        kw.update(over)
        real(self, *a, **kw)
    return init


class TestHarness(Base):
    def setUp(self):
        super().setUp()
        self.cat = make_catalogue_dir()
        self.addCleanup(shutil.rmtree, self.cat, True)

    def test_rows_with_the_profile(self):
        f, out = self.items_file([item(1)]), self.tmp / "a.jsonl"
        rc, text = self.run_answer(f, out, "--answer-profile", "thorough", "--catalogue", str(self.cat),
                                   gen=Gen(draft="Raise `max.poll.interval.ms` on the consumer."))
        self.assertEqual(rc, 0, text)
        r = eval2.read_jsonl(out)[0]
        self.assertIsNone(r["error"])
        for k in ("first_pass_text", "tokens_detail", "thorough", "thorough_note", "draft_info"):
            self.assertIn(k, r)
        self.assertEqual(set(r["seconds"]), {"search_terms", "retrieval", "answer", "total", "draft", "thinking", "check"})
        self.assertEqual(r["tokens_detail"]["draft_reasoning"], 100)
        self.assertEqual(r["thorough"]["r2"]["status"], "no_patch")
        self.assertNotIn("first_pass_text", r["thorough"])
        m = json.loads(eval2.manifest_path(out).read_text())
        self.assertEqual(m["answer_profile"]["name"], "thorough")
        self.assertTrue(any(x.startswith("answer profile thorough") for x in m["unmeasured_reasons"]))

    def test_rows_without_the_profile_are_as_before(self):
        f, out = self.items_file([item(1)]), self.tmp / "a.jsonl"
        rc, _ = self.run_answer(f, out)
        self.assertEqual(rc, 0)
        r = eval2.read_jsonl(out)[0]
        self.assertEqual(set(r), {"id", "text", "tokens", "truncated", "seconds", "search_terms", "pack", "error", "run",
                                  "prompt_sha256", "prompt_chars", "search_terms_prompt_sha256", "search_terms_prompt_chars"})
        m = json.loads(eval2.manifest_path(out).read_text())
        self.assertNotIn("answer_profile", m)

    def test_refusals(self):
        f, out = self.items_file([item(1)]), self.tmp / "a.jsonl"
        with self.assertRaises(eval2.EvalError):
            self.run_answer(f, out, "--answer-profile", "thorough")                       # no --catalogue
        with self.assertRaises(eval2.EvalError):
            self.run_answer(f, out, "--no-cards")
        with self.assertRaises(eval2.EvalError):
            self.run_answer(f, out, "--answer-profile", "thorough", "--catalogue", str(self.cat), "--mode", "closed")


class TestRunCommand(Cat):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        m = mock.patch("kafka_sre_rag.evalhooks.chunks_by_ids",
                       side_effect=lambda ids, index_path=None: [{c["id"]: c for c in CHUNKS}[i] for i in ids])
        m.start()
        self.addCleanup(m.stop)
        self.items = self.tmp / "items.jsonl"
        self.items.write_text("".join(json.dumps(it) + "\n" for it in [
            {"id": "x1", "tier": "B", "kind": "k", "family": "f", "domain": "kafka", "question": Q},
            {"id": "x2", "tier": "B", "kind": "k", "family": "g", "domain": "kafka", "question": Q}]))
        self.answers = self.tmp / "ans.jsonl"
        self.answers.write_text("".join(json.dumps(r) + "\n" for r in [
            {"id": "x1", "text": DRAFT, "pack": ["c1", "c2"], "truncated": False, "error": None},
            {"id": "x2", "text": DRAFT, "pack": ["c2"], "truncated": True, "error": None}]))

    def run_cmd(self, *extra):
        buf = io.StringIO()
        a = vp.build_parser().parse_args(["run", "--answers", str(self.answers), "--items", str(self.items),
                                          "--out", str(self.tmp / "out.jsonl"), "--dry-run",
                                          "--catalogues", str(self.dir), *extra])
        rc = vp.run_command(a, out=buf)
        return rc, buf.getvalue()

    def test_dry_run_line_per_item_and_resume(self):
        rc, text = self.run_cmd("--cards")
        self.assertEqual(rc, 0)
        lines = [l for l in text.splitlines() if l.startswith("[verify] ") and "id=" in l]
        self.assertEqual(len(lines), 2)
        rows = [json.loads(l) for l in (self.tmp / "out.jsonl").read_text().splitlines()]
        self.assertEqual([r["id"] for r in rows], ["x1", "x2"])
        self.assertEqual(rows[0]["r2"]["status"], "no_patch")
        self.assertTrue(rows[0]["config"]["cards"])
        self.assertTrue(rows[1]["draft_truncated"])
        rc, text = self.run_cmd("--cards")
        self.assertIn("2 already done, 0 to do", text)
        self.assertEqual(len((self.tmp / "out.jsonl").read_text().splitlines()), 2)
        with self.assertRaises(SystemExit):     # another configuration is not mixed into the same file
            self.run_cmd()

    def test_limit(self):
        rc, text = self.run_cmd("--limit", "1")
        self.assertIn("1 answers, 0 already done, 1 to do", text)

    def test_hidden_files_are_refused(self):
        a = vp.build_parser().parse_args(["run", "--answers", str(self.tmp / "a.jsonl.enc"), "--items", str(self.items),
                                          "--out", str(self.tmp / "o.jsonl"), "--dry-run"])
        with self.assertRaises(SystemExit):
            vp.run_command(a, out=io.StringIO())


if __name__ == "__main__":
    unittest.main()
