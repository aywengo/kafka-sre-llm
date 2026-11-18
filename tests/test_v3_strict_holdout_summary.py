import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import v3_strict_holdout_summary as S  # noqa: E402
from v3_ship_summary import aggregate, both_passes, pass_deltas  # noqa: E402


def _odd(q):
    return int(hashlib.md5(q.encode()).hexdigest(), 16) % 2


def row(q, base, var, flip):
    """Judge-cache row for a given (default score, strict score) as pass 1 or (flip=1) pass 2 would decode it."""
    swap = _odd(q) ^ flip
    return {"question": q, "j": {"a": var, "b": base} if swap else {"a": base, "b": var}}


class Fixture(unittest.TestCase):
    qs = ["q1", "q2", "q3", "q4"]
    # (default, strict) per pass: q1 strict better in both, q2 a tie, q3 mixed, q4 unscored in pass 2
    p1 = [row("q1", 3, 5, 0), row("q2", 4, 4, 0), row("q3", 4, 3, 0), row("q4", 2, 3, 0)]
    p2 = [row("q1", 3, 4, 1), row("q2", 4, 4, 1), row("q3", 5, 3, 1), {"question": "q4", "j": None}]

    def test_two_passes(self):
        d1, d2 = pass_deltas(self.p1, 0), pass_deltas(self.p2, 1)
        self.assertEqual(d1, {"q1": 2, "q2": 0, "q3": -1, "q4": 1})
        self.assertEqual(d2, {"q1": 1, "q2": 0, "q3": -2})   # unscored item left out
        b = both_passes(d1, d2)
        self.assertEqual(b, {"q1": 1.5, "q2": 0.0, "q3": -1.5})   # q4 not in both
        a = aggregate(b.values())
        self.assertEqual((a["n"], a["wins"], a["ties"], a["losses"]), (3, 1, 1, 1))
        self.assertAlmostEqual(a["mean"], 0.0)

    def test_result_line_measured_only(self):
        a = aggregate([1.5, 0.0, -1.5, 0.5])
        line = S.result_line(a, 4)
        self.assertTrue(line.startswith("result: strict minus default"))
        self.assertIn("mean +0.12", line)
        self.assertIn("standard error", line)
        self.assertIn("2 / 1 / 1", line)
        for word in ("wins the", "better", "default mode should", "HOLDS"):
            self.assertNotIn(word, line)

    def test_result_line_incomplete(self):
        self.assertTrue(S.result_line(aggregate([1.0, 0.0]), 4).startswith("result: INCOMPLETE, 2 of 4"))
        self.assertTrue(S.result_line(aggregate([]), 4).startswith("result: INCOMPLETE, 0 of 4"))


class Helpers(unittest.TestCase):
    def test_gap_count_counts_answers(self):
        rows = [{"text": "Use X. The provided documentation excerpts do not contain the exact flag."}, {"text": "Use Y."},
                {"text": "The excerpts do not cover A.\nThe excerpts do not cover B."}, {"text": None}]
        self.assertEqual(S.gap_count(rows), 2)

    def test_coverage_phrasings_seen_in_real_answers(self):
        for t in ("The provided documentation excerpts do not contain any information about a metric.",
                  "*Note:* The documentation does not provide a specific CLI command to query this MBean.",
                  "Based on the provided documentation excerpts, the specific command or procedure to stop a running rebalance is not included.",
                  "**Note:** The provided excerpts do not cover how to check the status.",
                  "The provided documentation excerpts do not explicitly describe the behavior.",
                  "a complex operation not covered by the provided excerpts.",
                  "the documentation provided does not explicitly state that includeTasks is supported.",
                  "or potentially specific internal metrics of certain Kafka clients or tools not covered in these excerpts."):
            self.assertTrue(S.has_coverage_statement(t), t)

    def test_no_coverage_statement(self):
        for t in ("Kafka does not support decreasing the number of partitions.",
                  "Run kubectl get pods; the pod does not restart.",
                  "The excerpts show how to run it.", "", None):
            self.assertFalse(S.has_coverage_statement(t), t)

    def test_gap_questions_numbers(self):
        num = {"a": 3, "b": 1, "c": 2}
        rows = [{"question": "a", "text": "The excerpts do not cover X."}, {"question": "b", "text": "ok"},
                {"question": "c", "text": "The documentation does not specify Y."}, {"question": "zz", "text": "The excerpts do not cover X."}]
        self.assertEqual(S.gap_questions(rows, num), [2, 3])

    def test_shortest_ten_by_number_and_tokens(self):
        num = {f"q{i}": i for i in range(1, 13)}
        rows = [{"question": f"q{i}", "tokens": 100 + (i % 3) * 10 + i} for i in range(1, 13)]
        rows.append({"question": "outside", "tokens": 1})
        s = S.shortest(rows, num)
        self.assertEqual(len(s), 10)
        self.assertEqual(s[0], (3, 103))   # q3: 103
        self.assertEqual(s, sorted(s, key=lambda t: (t[1], t[0])))
        self.assertNotIn(0, [n for n, _ in s])

    def test_shortest_tie_breaks_by_number(self):
        num = {"a": 2, "b": 1}
        self.assertEqual(S.shortest([{"question": "a", "tokens": 5}, {"question": "b", "tokens": 5}], num), [(1, 5), (2, 5)])

    def test_mean_tokens(self):
        self.assertEqual(S.mean_tokens([{"tokens": 100}, {"tokens": 300}]), 200)
        self.assertIsNone(S.mean_tokens([]))


if __name__ == "__main__":
    unittest.main()
