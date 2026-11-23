import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import m7_perfect_packs as M  # noqa: E402

LENS = {"a": 100, "b": 200, "c": 300, "d": 5000, "e": 5000, "f": 5000, "g": 900, "h": 100, "i": 100, "j": 100, "k": 100}


def pt(*sets):
    return {"id": "m", "chunk_sets": [list(s) for s in sets]}


class Head(unittest.TestCase):
    def test_fewest_characters_and_ties(self):
        self.assertEqual(M.build_head([pt(["b"], ["a"])], LENS), ["a"])
        self.assertEqual(M.build_head([pt(["a"], ["h"])], LENS), ["a"])          # tie: earlier set

    def test_whole_set_already_chosen_adds_none(self):
        head = M.build_head([pt(["a", "b"]), pt(["c"], ["b"])], LENS)
        self.assertEqual(head, ["a", "b"])                                       # b adds 0, c adds 300

    def test_order_and_unindexed(self):
        head = M.build_head([pt(["c"]), {"id": "x", "chunk_sets": []}, {"id": "y"}, pt(["a"])], LENS)
        self.assertEqual(head, ["c", "a"])

    def test_two_chunk_set_partly_chosen(self):
        head = M.build_head([pt(["a"]), pt(["a", "b"], ["c"])], LENS)
        self.assertEqual(head, ["a", "b"])                                       # adds 200 against 300


class Padding(unittest.TestCase):
    def test_frozen_order_skip_head_cap(self):
        out = M.pad(["a"], ["h", "a", "i", "j", "k", "b", "c"], LENS)
        self.assertEqual(out, ["a", "h", "i", "j", "k", "b"])                    # 6 chunks

    def test_big_head_kept_whole_no_padding(self):
        head = ["a", "b", "c", "h", "i", "j", "k"]
        self.assertEqual(M.pad(head, ["g"], LENS), head)                         # 7 > 6, cap = 7

    def test_character_budget(self):
        self.assertEqual(M.pad(["d", "e"], ["f", "g"], LENS), ["d", "e", "f"])   # room 4000 after d,e; f partly counted -> 0 left? used 14000
        self.assertEqual(M.pad(["d", "e", "f"], ["g"], LENS), ["d", "e", "f"])   # 15000 used > 14000: no room

    def test_room_under_500_stops(self):
        lens = dict(LENS, d=13600)
        self.assertEqual(M.pad(["d"], ["a", "b"], lens), ["d"])                  # room 400


class Counts(unittest.TestCase):
    def test_strict(self):
        p = pt(["a", "b"], ["c"])
        self.assertTrue(M.strict_served(p, ["c"]))
        self.assertFalse(M.strict_served(p, ["a"]))
        self.assertFalse(M.strict_served({"id": "x"}, ["a"]))

    def test_build_all_and_unknown_chunk(self):
        rub = [{"id": "s1", "must": [pt(["a", "b"]), pt(["c"]), {"id": "z"}]}]
        fun = {"s1": {"frozen": ["h", "a"], "r4g2": ["c"]}}
        pad_, unp, tab = M.build_all(rub, fun, LENS)
        self.assertEqual(unp["s1"], ["a", "b", "c"])
        self.assertEqual(pad_["s1"], ["a", "b", "c", "h"])
        self.assertEqual(M.strict_counts(rub, pad_), (2, 2))
        self.assertEqual((tab[0]["head_in_frozen"], tab[0]["head_in_r4g2"]), (1, 1))
        with self.assertRaises(M.PackError):
            M.build_all([{"id": "s", "must": [pt(["nope"])]}], {"s": {"frozen": [], "r4g2": []}}, LENS)

    def test_deterministic_files(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            h = [M.write_pack_file(Path(d) / "p.jsonl", {"s": ["a", "b"]}, ["s"], {"x": 1}) for _ in range(2)]
            self.assertEqual(h[0], h[1])
            self.assertTrue((Path(d) / "p.jsonl.manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
