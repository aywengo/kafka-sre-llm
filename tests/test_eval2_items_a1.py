"""Construction rules of the tier A1 items (scripts/eval2_items_a1.py), checked on the generated file.

Skipped, with a reason, when eval/m4/build/tier_a1.jsonl or the catalogues have not been built.
Also: nothing under kafka_sre_rag/ may import the scorer.
"""
import json
import sys
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_items_a1 as gen  # noqa: E402
from kafka_sre_rag.catalogue import catalogue_hash, default_dir  # noqa: E402

ITEMS = ROOT / "eval" / "m4" / "build" / "tier_a1.jsonl"
HAVE = ITEMS.is_file() and (default_dir() / "CATALOGUES.json").is_file()


class NoImportTests(unittest.TestCase):
    def test_runtime_does_not_import_the_scorer(self):
        for f in (ROOT / "kafka_sre_rag").glob("*.py"):
            self.assertNotIn("eval2_score", f.read_text(), f.name)

    def test_scorer_imports_nothing_from_the_runtime_but_the_catalogue(self):
        text = (ROOT / "scripts" / "eval2_score.py").read_text()
        imports = [l for l in text.splitlines() if l.startswith(("import ", "from "))]
        for l in imports:
            if "kafka_sre_rag" in l:
                self.assertEqual(l.split()[1], "kafka_sre_rag.catalogue", l)
        self.assertFalse(any("evalhooks" in l or "eval2 " in l or "checker" in l for l in imports))


class HelperTests(unittest.TestCase):
    def test_overlap_and_band(self):
        ov = gen.overlap("default value of max.poll.records", "The maximum number of records returned in poll")
        self.assertEqual(set(ov), {"q_in_desc", "desc_in_q"})
        self.assertIsNone(gen.overlap("x y z", None))
        self.assertEqual(gen.band(None), "none")
        self.assertEqual(gen.band({"q_in_desc": 0.6, "desc_in_q": 0.1}), "high")
        self.assertEqual(gen.band({"q_in_desc": 0.3, "desc_in_q": 0.1}), "mid")
        self.assertEqual(gen.band({"q_in_desc": 0.1, "desc_in_q": 0.1}), "low")

    def test_near_duplicate_rule(self):
        corpus = [("a.jsonl", gen.tokens("Which broker setting controls the log retention time in hours for old segments"))]
        hit, d = gen.near_duplicate("Which broker setting controls the log retention time in hours for old segments?", corpus)
        self.assertTrue(hit)
        hit, d = gen.near_duplicate("What is the default of group.initial.rebalance.delay.ms for the broker?", corpus)
        self.assertFalse(hit)

    def test_excerpt(self):
        self.assertIsNone(gen.excerpt("Short."))
        self.assertTrue(gen.excerpt("This is a first sentence of some length. And this is a second one that follows it.", 30, 80))


@unittest.skipUnless(HAVE, "items not generated: run scripts/build_catalogues.py then scripts/eval2_items_a1.py")
class GeneratedFileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.items = [json.loads(l) for l in ITEMS.read_text().splitlines() if l.strip()]

    def test_fields(self):
        need = {"id", "tier", "kind", "family", "domain", "question", "checks", "reference", "sources", "versions",
                "template", "anchored", "failure_class", "exposed", "overlap", "catalogue_hash", "seed"}
        for it in self.items:
            self.assertTrue(need <= set(it), it["id"])
            self.assertEqual(it["tier"], "A")
            self.assertTrue(it["sources"], it["id"])
            for s in it["sources"]:
                self.assertTrue(s.get("file") and s.get("catalogue"), (it["id"], s))

    def test_ids_and_questions_unique(self):
        self.assertEqual(len({i["id"] for i in self.items}), len(self.items))
        self.assertEqual(len({i["question"] for i in self.items}), len(self.items))

    def test_at_most_ten_per_template_and_is_real_share(self):
        self.assertLessEqual(max(Counter(i["template"] for i in self.items).values()), gen.MAX_PER_TEMPLATE)
        real = sum(i["kind"] == "is_real" for i in self.items)
        self.assertLessEqual(real, 0.10 * len(self.items))

    def test_family_is_template_plus_group(self):
        for it in self.items:
            self.assertTrue(it["family"].startswith(it["template"] + ":"), it["id"])

    def test_catalogue_hash_is_the_current_one(self):
        self.assertEqual({i["catalogue_hash"] for i in self.items}, {catalogue_hash()})

    def test_exposed_count(self):
        self.assertEqual(sum(i["exposed"] for i in self.items), gen.EXPOSED_N)

    def test_every_item_ends_with_an_answer_line_or_asks_for_a_command(self):
        for it in self.items:
            fmt = [c for c in it["checks"] if c.get("format")]
            self.assertEqual(len(fmt), 1, it["id"])
            if fmt[0]["type"] == "answer_line":
                self.assertIn("ANSWER:", it["question"], it["id"])

    def test_no_item_names_its_answer_key_in_a_description_question(self):
        for it in self.items:
            if it["template"].startswith("ns_key_desc"):
                key = it["checks"][1]["accepted"][0]
                self.assertNotIn(key, it["question"], it["id"])

    def test_not_covered_items_are_not_anchored(self):
        for it in self.items:
            if it["failure_class"] == "not_covered":
                self.assertFalse(it["anchored"])
                self.assertEqual(it["checks"][1]["expected"], "not in my sources")

    def test_near_duplicates_of_the_corpus_are_absent(self):
        corpus, _ = gen.corpus_texts()
        for it in self.items:
            core = it["question"].split("\n\n")[0] if it["kind"] != "error_name" or "A)" not in it["question"] else it["question"]
            hit, d = gen.near_duplicate(core, corpus)
            self.assertFalse(hit, (it["id"], d))


if __name__ == "__main__":
    unittest.main()
