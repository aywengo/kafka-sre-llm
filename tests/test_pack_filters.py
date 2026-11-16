"""scripts/pack_filters.py"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import pack_filters as F  # noqa: E402

GAP = "The retrieved documentation does not cover the retention of X."


class GapTests(unittest.TestCase):
    def test_count(self):
        self.assertEqual(F.gap_sentence_count("plain"), 0)
        self.assertEqual(F.gap_sentence_count("a\n" + GAP), 1)
        self.assertEqual(F.gap_sentence_count(GAP + "\n" + GAP), 2)

    def test_partial_one_passes(self):
        self.assertIsNone(F.gap_problem("partial", "Facts.\n\n" + GAP + " In general, look at logs."))

    def test_partial_missing_and_repeated(self):
        self.assertEqual(F.gap_problem("partial", "Facts."), "gap_sentence_missing")
        self.assertEqual(F.gap_problem("partial", GAP + "\n" + GAP), "gap_sentence_repeated")

    def test_none_first_passes(self):
        self.assertIsNone(F.gap_problem("none", "  \n" + GAP + "\nOther facts."))

    def test_none_second_fails(self):
        self.assertEqual(F.gap_problem("none", "Intro.\n" + GAP), "gap_sentence_not_first")

    def test_none_missing_and_two(self):
        self.assertEqual(F.gap_problem("none", "Intro."), "gap_sentence_missing")
        self.assertEqual(F.gap_problem("none", GAP + "\n" + GAP), "gap_sentence_repeated")

    def test_full(self):
        self.assertIsNone(F.gap_problem("full", "All covered."))
        self.assertEqual(F.gap_problem("full", "Text.\n" + GAP), "gap_sentence_in_full")


class LeakTests(unittest.TestCase):
    def test_matches(self):
        for t in ("The documentation says so.", "Caveats from the material:", "This is undocumented.",
                  "see the excerpts", "the Strimzi documentation lists it", "nothing here"):
            self.assertIsNotNone(F.leak_problem(t), t)
        self.assertEqual(F.leak_problem("the documentation says"), "the documentation says")

    def test_documented_as_plain_word_is_fine(self):
        # "the documented behaviour" is how an engineer speaks; only statements that something is NOT
        # documented are gap statements outside the gap sentence
        for t in ("This is the documented behaviour of the setting.", "Your execution matches the documented shape:"):
            self.assertIsNone(F.leak_problem(t), t)
        for t in ("The asymmetry is not documented for this join.", "Nothing is documented about it.",
                  "That flag isn't documented."):
            self.assertIsNotNone(F.leak_problem(t), t)

    def test_bridge_line_only_in_not_covered(self):
        t = GAP + "\nWhat it does cover:\n- `num.recovery.threads.per.data.dir` sets the threads."
        self.assertIsNone(F.leak_problem(t, "none"))
        self.assertEqual(F.mechanical_problems({"coverage": "none", "answer": t}), [])
        self.assertIsNotNone(F.leak_problem(t))
        self.assertIsNotNone(F.leak_problem(t, "partial"))
        # only the first occurrence is allowed
        self.assertIsNotNone(F.leak_problem(t + "\nWhat it does cover:", "none"))

    def test_widened_patterns(self):
        for t in ("Strimzi documents this as", "Example from the `KafkaUser` quotas documentation",
                  "The documentation's example", "isn't included in what I have in front of me",
                  "What it does give that bears on this situation:",
                  "(the excerpt on end-entity rotation is cut off)", "an excerpt"):
            self.assertIsNotNone(F.leak_problem(t), t)
            self.assertIsNotNone(F.leak_problem(t, "none"), t)
        self.assertIsNotNone(F.leak_problem("What it does cover is the Strimzi side", "partial"))
        self.assertIsNotNone(F.leak_problem(GAP + " See the documentation for more."))

    def test_source_alone_is_fine(self):
        self.assertIsNone(F.leak_problem("Point the connector at the source cluster."))
        self.assertIsNone(F.leak_problem("Check the source."))

    def test_gap_sentence_is_not_a_leak(self):
        self.assertIsNone(F.leak_problem(GAP + " In general, check broker logs."))
        self.assertIsNone(F.leak_problem("The retrieved documentation does not cover X."))

    def test_leak_next_to_gap_sentence(self):
        self.assertEqual(F.leak_problem(GAP + " The docs are silent."), "The docs")


class MechanicalTests(unittest.TestCase):
    def test_skip_and_error(self):
        self.assertEqual(F.mechanical_problems({"skip": True, "answer": ""}), ["skip"])
        self.assertEqual(F.mechanical_problems({"gen_error": "parse", "answer": ""}), ["gen_error"])

    def test_pass_and_fail(self):
        self.assertEqual(F.mechanical_problems({"coverage": "full", "answer": "Fine."}), [])
        self.assertEqual(F.mechanical_problems({"coverage": "partial", "answer": "Fine."}),
                         ["gap_sentence_missing"])
        self.assertEqual(F.mechanical_problems({"coverage": "full", "answer": "As the material says."}),
                         ["leak:the material"])
        self.assertEqual(F.mechanical_problems({"coverage": "full", "answer": GAP + " the docs"}),
                         ["gap_sentence_in_full", "leak:the docs"])


if __name__ == "__main__":
    unittest.main()
