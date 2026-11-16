"""scripts/build_packs_v3.py: selection rule, withheld logic and bank statistics, without the reranker."""
import io
import json
import sys
import tempfile
import unittest
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import build_packs_v3 as bp  # noqa: E402
import retrieve as RT  # noqa: E402

GOLDEN = ROOT / "tests/golden/packs_k6_k3_40.json"


def lens_of(n, size=2500):
    return {i: size for i in range(n)}


class SelectionRuleTests(unittest.TestCase):
    def test_matches_retrieve_loop(self):
        """select_ranked gives the chunks retrieve.retrieve() returns, for lengths around the 500-char stop."""
        rng_sizes = [[3000] * 12, [9000, 4800, 600, 400, 3000, 3000], [14000, 100, 100], [13600, 450, 2000, 3000],
                     [13499, 2000], [1] * 10, [7000, 7000, 7000, 7000]]
        with tempfile.TemporaryDirectory() as d:
            for n_case, sizes in enumerate(rng_sizes):
                path = Path(d) / f"chunks{n_case}.jsonl"  # retrieve.index() caches by path
                path.write_text("\n".join(json.dumps({"id": f"c{i}", "source": "s", "path": "p", "title": "",
                                                      "text": "x" * n}) for i, n in enumerate(sizes)) + "\n")
                ranked = list(reversed(range(len(sizes))))
                for k, cap in bp.SIZES.values():
                    with mock.patch.object(RT, "rank", lambda *a, **kw: ranked):
                        want = [c["id"] for c in RT.retrieve("q", k, cap, "e", index_path=str(path))]
                    got = bp.select_ranked(ranked, dict(enumerate(sizes)), k, cap)
                    self.assertEqual([f"c{i}" for i in got], want, (sizes, k, cap))
                    chunks = [{"text": "x" * n} for n in sizes]
                    self.assertEqual(bp.select(ranked, chunks, k, cap), got)

    @unittest.skipUnless(GOLDEN.exists(), "golden file missing")
    def test_reproduces_acceptance_run(self):
        """The 40 questions of the morning's acceptance run (real reranker, index work/v3/chunks.jsonl at the
        time): the top of each ranking is k6 + reserve, and the pure function gives back k6, k3 and reserve."""
        g = json.loads(GOLDEN.read_text())
        self.assertEqual(len(g["rows"]), 40)
        ids = {cid: n for n, cid in enumerate(sorted(g["lens"]))}
        lens = {ids[cid]: n for cid, n in g["lens"].items()}
        back = {n: cid for cid, n in ids.items()}
        for r in g["rows"]:
            ranked = [ids[c] for c in r["k6"] + r["reserve"]]
            plan = bp.plan_pack(ranked, lens)
            self.assertEqual([back[i] for i in plan["sel"]["k6"]], r["k6"])
            self.assertEqual([back[i] for i in plan["sel"]["k3"]], r["k3"])
            self.assertEqual([back[i] for i in plan["reserve"]], r["reserve"])
            self.assertIsNone(plan["seed_in_pack"])
            self.assertFalse(plan["withheld"])


class WithheldTests(unittest.TestCase):
    def setUp(self):
        self.ranked = list(range(20))
        self.lens = lens_of(20)

    def test_seed_in_pack_is_removed_and_backfilled(self):
        base = bp.plan_pack(self.ranked, self.lens)
        self.assertEqual(base["sel"]["k6"], [0, 1, 2, 3, 4, 5])  # 2500 each: the sixth gets 1500 of room
        self.assertEqual(base["sel"]["k3"], [0, 1, 2])
        p = bp.plan_pack(self.ranked, self.lens, "withheld", 2)
        self.assertTrue(p["withheld"])
        self.assertTrue(p["seed_in_pack"])
        self.assertEqual(p["treatment"], "withheld")
        self.assertEqual(p["sel"]["k6"], [0, 1, 3, 4, 5, 6])  # same rule, next rank steps in
        self.assertEqual(p["sel"]["k3"], [0, 1, 3])
        self.assertEqual(p["backfill_ranks"], [7])  # rank 7 (1-based) of the full ranking stepped in
        self.assertEqual(p["reserve"][:3], [7, 8, 9])  # reserve from the reduced ranking
        self.assertEqual(len(p["reserve"]), bp.RESERVE)
        for name in ("k6", "k3"):
            self.assertNotIn(2, p["sel"][name])
        self.assertNotIn(2, p["reserve"])

    def test_removal_applies_the_room_rule(self):
        # 14000 cap: with the 9000 chunk gone, the next chunks fit differently; the same stop at room < 500
        lens = {0: 9000, 1: 4600, 2: 3000, 3: 900, 4: 3000, 5: 3000, 6: 3000}
        ranked = list(range(7))
        base = bp.plan_pack(ranked, lens)
        self.assertEqual(base["sel"]["k6"], [0, 1])  # 13600 used, 400 left: stop
        p = bp.plan_pack(ranked, lens, "withheld", 1)
        # 9000 + 3000 + 900 = 12900, chunk 4 still gets 1100 of room (>= 500), then no room left
        self.assertEqual(p["sel"]["k6"], [0, 2, 3, 4])
        self.assertEqual(p["sel"]["k6"], bp.select_ranked([0, 2, 3, 4, 5, 6], lens, 6, 14000))
        self.assertEqual(p["backfill_ranks"], [3, 4, 5])  # ranks (1-based) of chunks 2, 3, 4 in the full ranking

    def test_seed_not_in_pack_stays_normal(self):
        p = bp.plan_pack(self.ranked, self.lens, "withheld", 12)
        self.assertFalse(p["seed_in_pack"])
        self.assertFalse(p["withheld"])
        self.assertEqual(p["treatment"], "normal")
        self.assertEqual(p["sel"], bp.plan_pack(self.ranked, self.lens)["sel"])
        self.assertEqual(p["backfill_ranks"], [])
        # seed chunk that is not in the index at all
        q = bp.plan_pack(self.ranked, self.lens, "withheld", -1)
        self.assertEqual((q["seed_in_pack"], q["treatment"]), (False, "normal"))

    def test_normal_and_ooc(self):
        n = bp.plan_pack(self.ranked, self.lens, "normal", 0)
        self.assertTrue(n["seed_in_pack"])
        self.assertFalse(n["withheld"])  # a normal row keeps its seed chunk
        self.assertIn(0, n["sel"]["k6"])
        o = bp.plan_pack(self.ranked, self.lens, "ooc", None)
        self.assertIsNone(o["seed_in_pack"])
        self.assertEqual(o["treatment"], "ooc")
        self.assertEqual(o["sel"], bp.plan_pack(self.ranked, self.lens)["sel"])
        o2 = bp.plan_pack(self.ranked, self.lens, "ooc", 0)  # an ooc record never loses a chunk
        self.assertFalse(o2["withheld"])
        self.assertIsNone(o2["seed_in_pack"])

    def test_seed_in_k6_but_not_in_k3(self):
        p = bp.plan_pack(self.ranked, self.lens, "withheld", 4)
        self.assertEqual(p["sel"]["k3"], [0, 1, 2])  # k3 unchanged because the seed was not in it
        self.assertEqual(p["sel"]["k6"], [0, 1, 2, 3, 5, 6])


class PackOneTests(unittest.TestCase):
    """pack_one() with retrieve.scores / retrieve.rank stubbed: the row fields, no reranker."""

    def run_one(self, bank, ranked):
        chunks = [{"id": f"c{i}", "text": "x" * 3000} for i in range(20)]
        idx = {c["id"]: i for i, c in enumerate(chunks)}
        pool = list(range(20))
        last = {}

        def rank(q, exp, index_path=None):
            last["ce"] = [float(i) for i in range(len(pool))]
            return ranked

        with mock.patch.object(RT, "scores", lambda q, p: Counter({i: 20 - i for i in pool})), \
                mock.patch.object(RT, "rank", rank):
            return bp.pack_one({"id": "q1", "question": "q", "expansion": "e"}, chunks, "unused", last, bank, idx)

    def test_without_bank_rows_are_as_before(self):
        r = self.run_one(None, list(range(20)))
        self.assertEqual(set(r), {"id", "question", "expansion", "k6", "k3", "reserve", "scores"})

    def test_withheld_row(self):
        r = self.run_one({"treatment": "withheld", "seed_chunk_id": "c2"}, list(range(20)))
        self.assertEqual(r["k6"], ["c0", "c1", "c3", "c4", "c5"])
        self.assertEqual((r["treatment"], r["treatment_requested"], r["withheld"], r["seed_in_pack"]),
                         ("withheld", "withheld", ["c2"], True))
        self.assertEqual(r["backfill_ranks"], [6])
        self.assertNotIn("c2", r["scores"])
        self.assertEqual(r["id"], "q1")

    def test_normal_not_in_pack_and_ooc_rows(self):
        r = self.run_one({"treatment": "withheld", "seed_chunk_id": "c15"}, list(range(20)))
        self.assertEqual((r["treatment"], r["withheld"], r["seed_in_pack"], r["treatment_requested"]),
                         ("normal", [], False, "withheld"))
        self.assertEqual(r["k6"], ["c0", "c1", "c2", "c3", "c4"])
        r = self.run_one({"treatment": "ooc"}, list(range(20)))
        self.assertEqual((r["treatment"], r["withheld"], r["seed_in_pack"]), ("ooc", [], None))
        r = self.run_one({"treatment": "normal", "seed_chunk_id": "gone"}, list(range(20)))
        self.assertEqual((r["treatment"], r["seed_in_pack"]), ("normal", False))


class StatsTests(unittest.TestCase):
    def test_bank_stats(self):
        packs = [{"id": "a", "seed_in_pack": True, "treatment": "withheld", "treatment_requested": "withheld",
                  "backfill_ranks": [7]},
                 {"id": "b", "seed_in_pack": False, "treatment": "normal", "treatment_requested": "withheld"},
                 {"id": "c", "seed_in_pack": True, "treatment": "normal", "treatment_requested": "normal"},
                 {"id": "d", "seed_in_pack": None, "treatment": "ooc", "treatment_requested": "ooc"}]
        with tempfile.TemporaryDirectory() as d:
            q = Path(d) / "q.jsonl"
            q.write_text("\n".join(json.dumps({"id": i, "question": i, "category": c})
                                   for i, c in (("a", "kafka-ops"), ("b", "kafka-ops"), ("c", "strimzi"),
                                                ("d", "kafka-ops"))) + "\n")
            md = "\n".join(bp.bank_stats(packs, SimpleNamespace(questions=str(q))))
        self.assertIn("2 of 3 (66.7%)", md)
        self.assertIn("| kafka-ops | 2 | 1 | 50.0% |", md)
        self.assertIn("| strimzi | 1 | 1 | 100.0% |", md)
        self.assertIn("Requested: 2. Honoured (seed chunk was in k6 and was removed): **1**", md)
        self.assertIn("| 7 | 1 |", md)
        self.assertEqual(bp.bank_stats([{"id": "x"}], SimpleNamespace(questions=None)), [])


if __name__ == "__main__":
    unittest.main()
