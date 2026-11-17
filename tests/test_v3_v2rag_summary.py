import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import v3_v2rag_summary as S  # noqa: E402


class HoldsRule(unittest.TestCase):
    def test_minus_point_two_holds(self):
        self.assertTrue(S.holds_rule(-0.2, 3, 5)[0])

    def test_minus_point_21_does_not_hold(self):
        ok, why = S.holds_rule(-0.21, 3, 5)
        self.assertFalse(ok)
        self.assertIn("-0.21", why)

    def test_positive_mean_and_equal_count_holds(self):
        self.assertTrue(S.holds_rule(0.01, 4, 4)[0])

    def test_higher_count_does_not_hold(self):
        ok, why = S.holds_rule(0.5, 5, 4)
        self.assertFalse(ok)
        self.assertIn("5 against 4", why)

    def test_both_reasons_listed(self):
        ok, why = S.holds_rule(-0.5, 6, 4)
        self.assertFalse(ok)
        self.assertIn("below", why)
        self.assertIn("6 against 4", why)

    def test_missing_count_or_incomplete_judging_does_not_hold(self):
        self.assertFalse(S.holds_rule(0.0, None, 4)[0])
        self.assertFalse(S.holds_rule(0.0, 1, 4, n=38)[0])
        self.assertFalse(S.holds_rule(float("nan"), 1, 4, n=0)[0])


class Recall(unittest.TestCase):
    chunks = {"a": {"title": "Consumer Config", "text": "Set max.poll.interval.ms and session.timeout.ms"},
              "b": {"title": "", "text": "Use --dry-run first"},
              "c": {"title": None, "text": "unrelated"}}
    facts = {"1": [r"max\.poll\.interval\.ms", r"session\.timeout\.ms"],
             "2": [r"--dry-run", r"--reset-offsets"],
             "3": [r"nothing here"]}
    questions = ["q1", "q2", "q3"]

    def test_hit_partial_miss(self):
        packs = {"q1": {"k6": ["a", "c"]}, "q2": {"k6": ["b"]}, "q3": {"k6": ["c"]}}
        s = S.recall_scores(packs, self.facts, [1, 2, 3], self.chunks, self.questions)
        self.assertEqual(s, {1: 1.0, 2: 0.5, 3: 0.0})
        self.assertEqual(S.recall_counts(s), {"hits": 1, "partial": 1, "miss": 1, "n": 3})

    def test_title_is_matched_and_text_is_lowercased(self):
        packs = {"q1": {"k6": ["a"]}}
        self.assertEqual(S.recall_scores(packs, {"1": ["consumer config"]}, [1], self.chunks, self.questions), {1: 1.0})

    def test_only_answerable_questions_count(self):
        packs = {"q1": {"k6": ["a"]}, "q2": {"k6": ["b"]}}
        s = S.recall_scores(packs, self.facts, [1], self.chunks, self.questions)
        self.assertEqual(list(s), [1])

    def test_identical_packs(self):
        a = {"q1": {"k6": ["a", "b"]}, "q2": {"k6": ["a"]}}
        b = {"q1": {"k6": ["a", "b"]}, "q2": {"k6": ["b"]}}
        self.assertEqual(S.count_identical(a, b), (1, 2))
        self.assertEqual(S.count_identical({"q1": {"k6": ["a", "b"]}}, {"q1": {"k6": ["b", "a"]}}), (0, 1))


class Small(unittest.TestCase):
    def test_parse_invention(self):
        t = "answers with >=1 token absent from pack: 7\nanswers with >=1 token absent from corpus: 2\n"
        self.assertEqual(S.parse_invention(t), (7, 2))
        self.assertEqual(S.parse_invention(""), (None, None))

    def test_big_differences(self):
        num = {"a": 1, "b": 2, "c": 3}
        self.assertEqual(S.big_differences({"a": 2.0, "b": -1.5, "c": -2.5}, num), [(1, 2.0), (3, -2.5)])

    def test_median_chars(self):
        self.assertEqual(S.median_chars([{"text": "ab"}, {"text": "abcd"}, {"text": "abcdef"}]), 4)
        self.assertIsNone(S.median_chars([]))


if __name__ == "__main__":
    unittest.main()
