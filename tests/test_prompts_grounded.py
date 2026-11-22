import contextlib
import hashlib
import io
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import __main__ as M, config, pipeline, evalhooks  # noqa: E402
from kafka_sre_rag import prompts as P  # noqa: E402

PACKS, INDEX = ROOT / "work/v3/rag_packs_base_exp.json", ROOT / "work/v2/chunks.jsonl"
GOLDEN = ROOT / "tests/golden/prompts_grounded.json"
HEAD = "Use these documentation excerpts where relevant."
CHUNKS = [
    {"source": "apache-kafka", "title": "Consumer configs", "path": "raw/a/b.md", "text": "Set max.poll.interval.ms."},
    {"source": "strimzi", "title": "", "path": "raw/strimzi/docs/rolling.md", "text": "Use maxUnavailable."},
]
Q = "Why does the group rebalance?"


def h(s):
    return {"sha256": hashlib.sha256(s.encode("utf-8")).hexdigest(), "len": len(s)}


class Constants(unittest.TestCase):
    def test_golden_file(self):
        gold = json.loads(GOLDEN.read_text())
        self.assertEqual(h(P.GROUNDED_G1), gold["GROUNDED_G1"])
        self.assertEqual(h(P.GROUNDED_G2), gold["GROUNDED_G2"])

    def test_frozen_json(self):
        cfg = config.load()["prompts_sha256"]
        self.assertEqual(cfg["GROUNDED_G1"], h(P.GROUNDED_G1)["sha256"])
        self.assertEqual(cfg["GROUNDED_G2"], h(P.GROUNDED_G2)["sha256"])
        self.assertIn("GROUNDED_G1", config.PROMPT_NAMES)
        self.assertIn("GROUNDED_G2", config.PROMPT_NAMES)

    def test_gap_sentence_literal(self):
        for t in (P.GROUNDED_G1, P.GROUNDED_G2):
            self.assertIn('"The retrieved documentation does not cover <what is missing>."', t)

    def test_registry(self):
        self.assertEqual(P.GROUNDED, {"grounded-g1": P.GROUNDED_G1, "grounded-g2": P.GROUNDED_G2})


class Messages(unittest.TestCase):
    def test_placement(self):
        for v, t in P.GROUNDED.items():
            self.assertEqual(P.grounded_user_message(Q, CHUNKS, v),
                             HEAD + "\n\n" + P.pack_text(CHUNKS) + "\n\nQuestion: " + Q + "\n\n" + t)
            m = P.grounded_answer_messages(Q, CHUNKS, v)
            self.assertEqual(m[0], {"role": "system", "content": P.SYSTEM_PROMPT})
            self.assertTrue(m[1]["content"].endswith(t))

    def test_closed_book_raises(self):
        for v in P.GROUNDED:
            with self.assertRaises(ValueError):
                P.grounded_user_message(Q, None, v)
            with self.assertRaises(ValueError):
                P.grounded_answer_messages(Q, None, v)

    def test_unknown_variant(self):
        with self.assertRaises(ValueError):
            P.grounded_user_message(Q, CHUNKS, "grounded-g3")

    def test_default_and_strict_unchanged(self):
        self.assertEqual(P.user_message(Q, CHUNKS), HEAD + "\n\n" + P.pack_text(CHUNKS) + "\n\nQuestion: " + Q)
        self.assertEqual(P.user_message(Q, CHUNKS, strict=True),
                         HEAD + " " + P.STRICT + "\n\n" + P.pack_text(CHUNKS) + "\n\nQuestion: " + Q)
        self.assertEqual(P.user_message(Q, None), Q)

    def test_modes_registered(self):
        self.assertEqual(pipeline.GROUNDED_MODES, ("grounded-g1", "grounded-g2"))
        self.assertEqual(evalhooks.GROUNDED_MODES, ("grounded-g1", "grounded-g2"))
        with self.assertRaises(ValueError):
            pipeline.ask(Q, None, mode="grounded-g3")

    def test_evalhooks_closed_book_grounded_refused(self):
        # a stored pack is required for a grounded mode without retrieval; no model is ever called here
        class Gen:
            identity = {}

            def chat(self, *a, **k):
                raise AssertionError("no model call expected")
        with self.assertRaises(ValueError):
            evalhooks.run_item(Q, Gen(), mode="grounded-g3")


@unittest.skipUnless(PACKS.exists() and INDEX.exists(), "work/v3/rag_packs_base_exp.json or work/v2/chunks.jsonl absent (work/ is git-ignored)")
class LegacyPacks(unittest.TestCase):
    def test_hashes(self):
        gold = json.loads(GOLDEN.read_text())
        by_id = {}
        for line in open(INDEX):
            c = json.loads(line)
            by_id[c["id"]] = c
        rows = json.load(open(PACKS))
        self.assertEqual(len(rows), 40)
        self.assertEqual(len(gold["questions"]), 40)
        for i, r in enumerate(rows, 1):
            k6 = [by_id[x] for x in r["k6"]]
            g = gold["questions"][str(i)]
            self.assertEqual(h(P.grounded_user_message(r["question"], k6, "grounded-g1")), g["user_k6_grounded_g1"], i)
            self.assertEqual(h(P.grounded_user_message(r["question"], k6, "grounded-g2")), g["user_k6_grounded_g2"], i)


class Cli(unittest.TestCase):
    def run_main(self, argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = M.main(argv)
            except SystemExit as e:
                code = e.code
        return code, out.getvalue(), err.getvalue()

    def test_service_url_refused(self):
        for v in P.GROUNDED:
            code, out, err = self.run_main(["ask", "--service-url", "http://localhost:9", "--mode", v, "q"])
            self.assertEqual(code, 2)
            self.assertIn("--service-url", err)
            self.assertEqual(out, "")

    def test_mode_choices_and_help(self):
        p = M.build_parser()
        a = p.parse_args(["ask", "--mode", "grounded-g2", "q"])
        self.assertEqual(a.mode, "grounded-g2")
        with self.assertRaises(SystemExit):
            p.parse_args(["ask", "--mode", "grounded-g3", "q"])
        code, out, err = self.run_main(["ask", "--help"])
        self.assertIn("EXPERIMENTAL", out)


if __name__ == "__main__":
    unittest.main()
