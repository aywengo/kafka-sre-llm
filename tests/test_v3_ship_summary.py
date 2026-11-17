import hashlib
import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import v3_ship_summary as S  # noqa: E402


def odd(q):
    return int(hashlib.md5(q.encode()).hexdigest(), 16) % 2


def find(parity):
    return next(f"question {i}" for i in range(1000) if odd(f"question {i}") == parity)


class Decode(unittest.TestCase):
    def test_even_hash_shows_base_first_in_pass_1(self):
        q = find(0)  # variant is B, base is A
        self.assertEqual(S.decode_cache_row({"question": q, "j": {"a": 2, "b": 5}}, 0), (2, 5))

    def test_odd_hash_shows_variant_first_in_pass_1(self):
        q = find(1)  # variant is A, base is B
        self.assertEqual(S.decode_cache_row({"question": q, "j": {"a": 5, "b": 2}}, 0), (2, 5))

    def test_flip_swaps_the_slots(self):
        for parity in (0, 1):
            q = find(parity)
            row = {"question": q, "j": {"a": 5, "b": 2}}
            self.assertEqual(S.decode_cache_row(row, 0), tuple(reversed(S.decode_cache_row(row, 1))))
        q = find(0)  # flipped: variant is A
        self.assertEqual(S.decode_cache_row({"question": q, "j": {"a": 4, "b": 1}}, 1), (1, 4))

    def test_unscored(self):
        self.assertIsNone(S.decode_cache_row({"question": "x", "j": None}, 0))
        self.assertIsNone(S.decode_cache_row({"question": "x"}, 1))

    def test_pass_deltas_and_both(self):
        q0, q1 = find(0), find(1)
        rows = [{"question": q0, "j": {"a": 2, "b": 5}}, {"question": q1, "j": {"a": 5, "b": 2}},
                {"question": "u", "j": None}]
        p1 = S.pass_deltas(rows, 0)
        self.assertEqual(p1, {q0: 3, q1: 3})
        p2 = S.pass_deltas([{"question": q0, "j": {"a": 4, "b": 4}}], 1)
        self.assertEqual(S.both_passes(p1, p2), {q0: 1.5})


class Aggregate(unittest.TestCase):
    def test_numbers(self):
        a = S.aggregate([2, 0, -1, 1])
        self.assertEqual((a["n"], a["wins"], a["ties"], a["losses"]), (4, 2, 1, 1))
        self.assertAlmostEqual(a["mean"], 0.5)
        self.assertAlmostEqual(a["se"], math.sqrt(1.25) / 2)

    def test_empty(self):
        a = S.aggregate([])
        self.assertEqual(a["n"], 0)
        self.assertTrue(math.isnan(a["mean"]))

    def test_first_diff(self):
        self.assertIsNone(S.first_diff("abc", "abc"))
        self.assertEqual(S.first_diff("abcd", "abxd"), 2)
        self.assertEqual(S.first_diff("ab", "abcd"), 2)


if __name__ == "__main__":
    unittest.main()
