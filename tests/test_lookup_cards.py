"""M7 KA1: the lookup cards in the answer prompt (EXPERIMENTAL, off by default). Offline: synthetic catalogues, stub
generator, no model, no server."""
import hashlib
import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2  # noqa: E402
from kafka_sre_rag import __main__ as cli, config, evalhooks, kb_lookup, pipeline, prompts  # noqa: E402
from test_eval2 import CHUNKS, Base, Gen, item  # noqa: E402
from test_kb_lookup import synthetic  # noqa: E402

HEADER_SHA256 = "bce6f54f796a04e95c8e77ee5f6eaa4fc35337ae2010dd1d6b53db55423b2014"   # the heading is part of every prompt made with the option: a change is a new prompt
Q = "What does min.insync.replicas do for a topic?"
Q_NONE = "How do I restart the pods?"


REAL_KB = kb_lookup.KbLookup


def _kb():
    return REAL_KB(synthetic(), verify=False)


class GoldenOff(unittest.TestCase):
    """With the option off every prompt is byte for byte as it was."""

    def test_none_and_empty_cards_equal_no_argument(self):
        ch = [CHUNKS["c1"], CHUNKS["c2"]]
        for strict in (False, True):
            base = prompts.answer_messages(Q, ch, strict=strict)
            self.assertEqual(base, prompts.answer_messages(Q, ch, strict=strict, cards_text=None))
            self.assertEqual(base, prompts.answer_messages(Q, ch, strict=strict, cards_text=""))
        for v in prompts.GROUNDED:
            base = prompts.grounded_answer_messages(Q, ch, v)
            self.assertEqual(base, prompts.grounded_answer_messages(Q, ch, v, cards_text=""))
        self.assertEqual(prompts.user_message(Q), Q)           # closed book untouched
        self.assertEqual(prompts.user_message(Q, None, cards_text="x"), Q)

    def test_phase0_hashes_unchanged(self):
        self.assertEqual(config.prompt_hashes(), {n: config.load()["prompts_sha256"][n] for n in config.PROMPT_NAMES})

    def test_ask_off_is_identical_to_before(self):
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=[CHUNKS["c1"], CHUNKS["c2"]]):
            g1, g2 = Gen(), Gen()
            a1 = pipeline.ask(Q, g1, config.load())
            a2 = pipeline.ask(Q, g2, config.load(), lookup_cards=False)
        self.assertEqual(g1.calls, g2.calls)
        self.assertEqual(a1.messages, a2.messages)
        self.assertIsNone(a2.lookup)
        self.assertNotIn("lookup_s", a2.timings)
        self.assertNotIn("lookup", " ".join(a2.unmeasured_reasons))

    def test_evalhooks_off_has_no_lookup_key(self):
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=[CHUNKS["c1"]]):
            r = evalhooks.run_item(Q, Gen(), config.load())
        self.assertNotIn("lookup", r)


class BlockOn(unittest.TestCase):
    def setUp(self):
        self.kb = _kb()
        self.ch = [CHUNKS["c1"], CHUNKS["c2"]]

    def test_header_hash(self):
        self.assertEqual(hashlib.sha256(prompts.LOOKUP_CARDS_HEADER.encode("utf-8")).hexdigest(), HEADER_SHA256)

    def test_block_after_excerpts_before_question(self):
        block, info = kb_lookup.answer_block(Q, "", kb=self.kb)
        self.assertTrue(block.startswith(prompts.LOOKUP_CARDS_HEADER))
        self.assertIn("`min.insync.replicas`", block)
        self.assertIn("pin ", block)
        for msgs in (prompts.answer_messages(Q, self.ch, cards_text=block),
                     prompts.answer_messages(Q, self.ch, strict=True, cards_text=block),
                     prompts.grounded_answer_messages(Q, self.ch, "grounded-g1", cards_text=block)):
            u = msgs[1]["content"]
            self.assertEqual(u.count(prompts.LOOKUP_CARDS_HEADER), 1)
            self.assertLess(u.index(prompts.pack_text(self.ch)) + len(prompts.pack_text(self.ch)), u.index(block))
            self.assertLess(u.index(block) + len(block), u.index("Question: " + Q))

    def test_prompt_without_the_block_is_the_old_prompt(self):
        block, _ = kb_lookup.answer_block(Q, "", kb=self.kb)
        on = prompts.answer_messages(Q, self.ch, cards_text=block)[1]["content"]
        off = prompts.answer_messages(Q, self.ch)[1]["content"]
        self.assertEqual(on.replace("\n\n" + block, "", 1), off)

    def test_empty_card_list_adds_nothing(self):
        block, info = kb_lookup.answer_block(Q_NONE, "", kb=self.kb)
        self.assertEqual(block, "")
        self.assertEqual((info["cards_found"], info["cards"], info["chars"]), (0, 0, 0))
        off = prompts.answer_messages(Q_NONE, self.ch)
        self.assertEqual(off, prompts.answer_messages(Q_NONE, self.ch, cards_text=block or None))
        self.assertNotIn("REFERENCE CARDS", off[1]["content"])

    def test_budget_respected(self):
        q = Q + " compression.type log.retention.ms"
        full, fi = kb_lookup.answer_block(q, "", kb=self.kb)
        self.assertLessEqual(len(full), kb_lookup.DEFAULT_MAX_CHARS)
        small, si = kb_lookup.answer_block(q, "", kb=self.kb, max_chars=len(prompts.LOOKUP_CARDS_HEADER) + 400)
        self.assertLessEqual(len(small), len(prompts.LOOKUP_CARDS_HEADER) + 400)
        self.assertLess(si["cards"], si["cards_found"])
        self.assertEqual(si["chars"], len(small))
        none, ni = kb_lookup.answer_block(q, "", kb=self.kb, max_chars=len(prompts.LOOKUP_CARDS_HEADER) + 5)
        self.assertEqual((none, ni["cards"]), ("", 0))     # nothing fits: no heading either

    def test_terms_are_read_like_the_retrieval_does(self):
        block, info = kb_lookup.answer_block(Q_NONE, "foo, compression.type, kafka-topics.sh", kb=self.kb)
        self.assertIn("`compression.type`", block)
        self.assertGreaterEqual(info["cards"], 1)

    def test_info_fields(self):
        _, info = kb_lookup.answer_block(Q, "", kb=self.kb)
        self.assertEqual(set(info), {"cards_found", "cards", "chars", "seconds", "catalogue_sha256"})
        self.assertGreaterEqual(info["seconds"], 0)

    def test_cards_hold_only_catalogue_facts_with_pin(self):
        block, _ = kb_lookup.answer_block(Q, "", kb=self.kb)
        for line in block.splitlines()[1:]:
            if line.startswith("- "):
                self.assertIn("pin ", line)


class AskPipeline(unittest.TestCase):
    def test_on_records_metadata_and_marks_unmeasured(self):
        kb = _kb()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=[CHUNKS["c1"], CHUNKS["c2"]]):
            g = Gen()
            ans = pipeline.ask(Q, g, config.load(), lookup_cards=True, lookup_kb=kb)
        u = ans.messages[1]["content"]
        self.assertIn(prompts.LOOKUP_CARDS_HEADER, u)
        self.assertEqual(g.calls[1][0], ans.messages)               # the model got exactly these messages
        self.assertGreaterEqual(ans.lookup["cards"], 1)
        self.assertEqual(ans.lookup["chars"], len(u.split("\n\n" + prompts.pack_text([CHUNKS["c1"], CHUNKS["c2"]]) + "\n\n")[1].split("\n\nQuestion:")[0]))
        self.assertEqual(ans.timings["lookup_s"], ans.lookup["seconds"])
        self.assertFalse(ans.measured)
        self.assertTrue(any(r == pipeline.LOOKUP_REASON for r in ans.unmeasured_reasons))


class Cli(unittest.TestCase):
    def run_ask(self, *extra):
        out = io.StringIO()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=[CHUNKS["c1"], CHUNKS["c2"]]), \
                mock.patch("kafka_sre_rag.service.AnswerChecker") as AC, \
                mock.patch("kafka_sre_rag.kb_lookup.KbLookup", side_effect=lambda cats, d, verify=True: _kb()), \
                mock.patch.object(cli, "make_generator", return_value=cli.StubGenerator(out=out)), \
                mock.patch.object(sys, "stdout", out), mock.patch.object(sys, "stderr", io.StringIO()):
            AC.return_value.cats, AC.return_value.directory = object(), Path("x")
            rc = cli.main(["ask", "--dry-run", "--no-rerank-in-dry-run", *extra, Q])
        return rc, out.getvalue()

    def test_dry_run_shows_block_only_with_the_option(self):
        rc, on = self.run_ask("--lookup-cards")
        self.assertEqual(rc, 0)
        self.assertIn(prompts.LOOKUP_CARDS_HEADER, on)
        self.assertIn("`min.insync.replicas`", on)
        rc, off = self.run_ask()
        self.assertEqual(rc, 0)
        self.assertNotIn("REFERENCE CARDS", off)

    def test_service_url_refused(self):
        err = io.StringIO()
        with mock.patch.object(sys, "stderr", err):
            rc = cli.main(["ask", "--service-url", "http://127.0.0.1:1", "--lookup-cards", "q"])
        self.assertEqual(rc, 2)
        self.assertIn("--lookup-cards", err.getvalue())

    def test_option_exists_on_ask_and_chat_not_serve(self):
        p = cli.build_parser()
        self.assertTrue(p.parse_args(["ask", "--lookup-cards", "q"]).lookup_cards)
        self.assertTrue(p.parse_args(["chat", "--lookup-cards"]).lookup_cards)
        self.assertFalse(p.parse_args(["ask", "q"]).lookup_cards)
        with mock.patch.object(sys, "stderr", io.StringIO()), self.assertRaises(SystemExit):
            p.parse_args(["serve", "--lookup-cards"])


class Eval2Answer(Base):
    def _run(self, items, out, *extra):
        kb = _kb()
        with mock.patch("kafka_sre_rag.service.AnswerChecker") as AC, \
                mock.patch("kafka_sre_rag.kb_lookup.KbLookup", return_value=kb):
            AC.return_value.cats, AC.return_value.directory = object(), Path("x")
            return self.run_answer(items, out, *extra)

    def test_option_passes_through_and_is_recorded(self):
        cat = self.tmp / "cat"
        cat.mkdir()
        (cat / "a.json").write_text("{}")
        f = self.items_file([item(1, question=Q), item(2, question=Q_NONE)])
        out = self.tmp / "a.jsonl"
        gen = Gen()
        kb = _kb()
        with mock.patch("kafka_sre_rag.service.AnswerChecker") as AC, \
                mock.patch("kafka_sre_rag.kb_lookup.KbLookup", return_value=kb):
            AC.return_value.cats, AC.return_value.directory = object(), Path("x")
            rc, _ = self.run_answer(f, out, "--catalogue", str(cat), "--lookup-cards", gen=gen)
        self.assertEqual(rc, 0)
        rows = {r["id"]: r for r in eval2.read_jsonl(out)}
        r1, r2 = rows["i001"], rows["i002"]
        self.assertGreaterEqual(r1["lookup"]["cards"], 1)
        self.assertEqual(set(r1["lookup"]), {"cards_found", "cards", "chars", "seconds", "catalogue_sha256"})
        self.assertEqual(r2["lookup"]["cards"], 0)
        answer_msgs = [m for m, _ in gen.calls if m[0]["content"] == prompts.SYSTEM_PROMPT]
        self.assertIn(prompts.LOOKUP_CARDS_HEADER, answer_msgs[0][1]["content"])
        self.assertNotIn("REFERENCE CARDS", answer_msgs[1][1]["content"])
        self.assertEqual(r1["prompt_chars"], sum(len(m["content"]) for m in answer_msgs[0]))
        m = json.loads(eval2.manifest_path(out).read_text())
        self.assertTrue(m["lookup_cards"]["enabled"])
        self.assertEqual(m["lookup_cards"]["catalogue_sha256"], eval2.catalogue_hash(cat))
        self.assertEqual(m["lookup_cards"]["header_sha256"],
                         hashlib.sha256(prompts.LOOKUP_CARDS_HEADER.encode()).hexdigest())
        self.assertTrue(any("lookup cards" in r for r in m["unmeasured_reasons"]))

    def test_off_adds_nothing_to_rows_or_manifest(self):
        f = self.items_file([item(1, question=Q)])
        out = self.tmp / "b.jsonl"
        rc, _ = self.run_answer(f, out)
        self.assertEqual(rc, 0)
        self.assertNotIn("lookup", eval2.read_jsonl(out)[0])
        m = json.loads(eval2.manifest_path(out).read_text())
        self.assertNotIn("lookup_cards", m)
        self.assertFalse(any("lookup" in r for r in m["unmeasured_reasons"]))

    def test_off_prompt_equals_on_prompt_minus_block(self):
        f = self.items_file([item(1, question=Q)])
        cat = self.tmp / "cat"
        cat.mkdir()
        (cat / "a.json").write_text("{}")
        g_off, g_on = Gen(), Gen()
        self.run_answer(f, self.tmp / "off.jsonl", gen=g_off)
        kb = _kb()
        with mock.patch("kafka_sre_rag.service.AnswerChecker") as AC, \
                mock.patch("kafka_sre_rag.kb_lookup.KbLookup", return_value=kb):
            AC.return_value.cats, AC.return_value.directory = object(), Path("x")
            self.run_answer(f, self.tmp / "on.jsonl", "--catalogue", str(cat), "--lookup-cards", gen=g_on)
        off = g_off.calls[1][0][1]["content"]
        on = g_on.calls[1][0][1]["content"]
        block, _ = kb_lookup.answer_block(Q, "terms one, terms two", kb=kb)
        self.assertEqual(on.replace("\n\n" + block, "", 1), off)

    def test_refusals(self):
        f = self.items_file([item(1, question=Q)])
        for extra, word in ((("--lookup-cards",), "--catalogue"),
                            (("--lookup-cards", "--catalogue", str(self.tmp), "--mode", "closed"), "closed")):
            with self.assertRaises(eval2.EvalError) as cm:
                self.run_answer(f, self.tmp / "x.jsonl", *extra)
            self.assertIn(word, str(cm.exception))


if __name__ == "__main__":
    unittest.main()
