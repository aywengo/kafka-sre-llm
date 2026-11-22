"""Profile r3 (WP5, K3): the frozen pack exactly, then the exact-heading lookup (2 slots) and at most one runbook,
10 chunks / 24,000 characters. Offline: a deterministic stand-in replaces the reranker, so the tests check the
construction (additive, caps, determinism, funnel drift), not the live model. The live check is the guarded
acceptance run in runs/m4/reports/wp5_r3_build.md."""
import hashlib
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import retrieval_funnel as F  # noqa: E402
import retrieve as RT  # noqa: E402
from kafka_sre_rag import config, profiles, retrieval  # noqa: E402
from test_profiles_drift import QUESTIONS, StubReranker, make_rows  # noqa: E402

PROF = profiles.load_candidate()["profiles"]["r3"]
DEV_ITEMS = [ROOT / "eval/m4/pilot2/items.jsonl", ROOT / "eval/m4/open/tier_a.jsonl"]
DEV_ANSWERS = [ROOT / "work/m4/pilot2/ans_default.jsonl", ROOT / "work/m4/s6/ans_A1.jsonl"]


def rd(p):
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


class TestR3Settings(unittest.TestCase):
    def test_settings(self):
        self.assertIn("r3", profiles.CANDIDATES)
        self.assertEqual((PROF["k"], PROF["max_chars"], PROF["lookup"], PROF["added_runbook_cap"]), (10, 24000, 2, 1))
        k6 = config.load()["retrieval"]["k6"]
        self.assertEqual((PROF["base_k"], PROF["base_chars"]), (k6["k"], k6["max_chars"]))  # the frozen pack size
        self.assertEqual(profiles.load_candidate()["index"]["sha256"], config.load()["index"]["sha256"])  # frozen index
        self.assertEqual(profiles.settings_sha256("r3"), profiles.settings_sha256("r3"))

    def test_r1_r2_hashes_unchanged(self):
        self.assertEqual(profiles.settings_sha256("r1"), "d1959b5a77b151f1a1547c5163eeb8d134e81d33a53c3ecea54624dff7771b47")
        self.assertEqual(profiles.settings_sha256("r2"), "34dae8871e2312aab32817547bcc2883c7e5c2dfdd80ca9485e689490c0e3502")

    def test_funnel_preset(self):
        v = F.parse_variant("r3")[1]
        self.assertEqual((v.additive, v.lookup, v.runbook_cap, v.k, v.chars), (True, 2, 1, 10, 24000))


class TestAdditivePackRules(unittest.TestCase):
    """profiles.additive_pack on hand-made lists."""
    class NoLookup:
        def hits(self, *a, **k):
            return []

    def pack(self, frozen_list, lens=None, source_of=None, lookup=None, prof=None):
        lens = lens or {c: 1000 for c in frozen_list}
        src = source_of or {c: "strimzi" for c in frozen_list}
        return profiles.additive_pack(prof or PROF, "q", "", frozen_list=frozen_list, bm=list(frozen_list), score={},
                                      source_of=src, lens=lens, lookup=lookup or self.NoLookup())

    def test_frozen_pack_is_a_prefix_and_fills_to_ten(self):
        fl = [f"c{i}" for i in range(40)]
        pack, _ = self.pack(fl)
        self.assertEqual(pack, fl[:10])

    def test_lookup_after_frozen_pack(self):
        fl = [f"c{i}" for i in range(40)]

        class LK:
            def hits(self, *a, **k):
                return ["c30", "c2"]   # c2 is already in the frozen pack: no duplicate, still a used slot
        pack, _ = self.pack(fl, lookup=LK())
        self.assertEqual(pack[:6], fl[:6])
        self.assertEqual(pack[6:8], ["c30", "c6"])   # c30 first, then the frozen order goes on
        self.assertEqual(len(pack), len(set(pack)))

    def test_character_cap_drops_added_never_frozen(self):
        fl = [f"c{i}" for i in range(20)]
        pack, _ = self.pack(fl, lens={c: 4000 for c in fl})   # frozen: 4 x 4,000 = 16,000 > 14,000 -> 4 chunks (3 full + 2,000)
        frozen = profiles.select_ranked(fl, {c: 4000 for c in fl}, 6, 14000)
        self.assertEqual(pack[:len(frozen)], frozen)
        used = 0
        for c in pack[len(frozen):]:
            used += 4000
        self.assertLessEqual(len(pack), 10)
        self.assertEqual(pack, fl[:len(pack)])
        self.assertLessEqual(sum(min(4000, 24000 - 4000 * i) for i in range(len(pack))), 24000)

    def test_runbook_cap_is_one_added_on_top_of_frozen_runbooks(self):
        fl = [f"c{i}" for i in range(20)]
        src = {c: "strimzi" for c in fl}
        src.update({"c1": "runbooks", "c2": "runbooks", "c8": "runbooks", "c9": "runbooks"})
        pack, capped = self.pack(fl, source_of=src)
        self.assertIn("c1", pack)
        self.assertIn("c2", pack)               # two frozen runbook chunks stay
        self.assertEqual(pack[:6], fl[:6])
        self.assertIn("c8", pack)               # one more runbook chunk is added on top of the frozen ones
        self.assertNotIn("c9", pack)
        self.assertEqual(capped, ["c9"])
        src = {c: "strimzi" for c in fl}
        src.update({"c8": "runbooks", "c9": "runbooks"})
        pack, capped = self.pack(fl, source_of=src)
        self.assertEqual(sum(1 for c in pack if src[c] == "runbooks"), 1)
        self.assertIn("c8", pack)
        self.assertEqual(capped, ["c9"])

    def test_short_frozen_pool(self):
        fl = ["a", "b"]
        self.assertEqual(self.pack(fl)[0], ["a", "b"])


class TestR3OnIndex(unittest.TestCase):
    def setUp(self):
        if not (ROOT / "runs/v3/INDEX_FROZEN.json").is_file():
            self.skipTest("runs/v3/INDEX_FROZEN.json is not in the repository (it is kept with the run files; no command makes it)")
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        p = self.tmp / "idx.jsonl"
        p.write_text("\n".join(json.dumps(r) for r in make_rows()))
        self.path = str(p)
        self.model = StubReranker()
        m = mock.patch.object(RT, "reranker", lambda: self.model)
        m.start()
        self.addCleanup(m.stop)

    def test_additive_equals_frozen_prefix_synthetic(self):
        n = 0
        for q, exp in QUESTIONS:
            fro = [c["id"] for c in retrieval.select(q, exp, profile="frozen", index_path=self.path)]
            r3 = [c["id"] for c in retrieval.select(q, exp, profile="r3", index_path=self.path)]
            self.assertEqual(r3[:len(fro)], fro, q)
            self.assertLessEqual(len(r3), 10)
            self.assertEqual(len(r3), len(set(r3)))
            n += bool(r3)
        self.assertGreaterEqual(n, 5)

    def test_runtime_equals_funnel(self):
        ix = F.Index(self.path)
        scorer = F.CEScorer(ix.byid, self.tmp / "ce.jsonl", model=self.model)
        scorer.heartbeat = 0
        for q, exp in QUESTIONS:
            st = F.ItemState(ix, q, exp)
            want = F.pack_of(st, F.parse_variant("r3")[1], scorer)
            got = [c["id"] for c in retrieval.select(q, exp, profile="r3", index_path=self.path)]
            self.assertEqual(got, want, q)

    def test_deterministic(self):
        for q, exp in QUESTIONS:
            a = [c["id"] for c in retrieval.select(q, exp, profile="r3", index_path=self.path)]
            b = [c["id"] for c in retrieval.select(q, exp, profile="r3", index_path=self.path)]
            self.assertEqual(a, b)

    def test_without_reranker_runs(self):
        got = retrieval.select(QUESTIONS[0][0], QUESTIONS[0][1], profile="r3", index_path=self.path, rerank=False)
        self.assertTrue(got)
        self.assertTrue(all(c["ce_score"] is None for c in got))


@unittest.skipUnless(all(p.exists() for p in DEV_ITEMS + DEV_ANSWERS), "development items and stored answers (git-ignored work/) not present")
class TestR3DevelopmentQueries(unittest.TestCase):
    """The additive property on the real frozen index with the development items' own search terms. The reranker is a
    deterministic stand-in (no model): this checks the construction on real BM25 pools, not the live ordering."""
    def test_additive_on_dev_queries(self):
        items = {i["id"]: i for it in DEV_ITEMS for i in rd(it)}
        answers = {}
        for p in DEV_ANSWERS:
            for a in rd(p):
                answers.setdefault(a["id"], a)
        ids = sorted(i for i in items if i in answers)
        self.assertGreaterEqual(len(ids), 50)
        model = StubReranker()
        with mock.patch.object(RT, "reranker", lambda: model):
            for i in ids[:60]:
                q = F.full_question(items[i])
                exp = answers[i].get("search_terms") or ""
                fro = [c["id"] for c in retrieval.select(q, exp, profile="frozen")]
                r3 = [c["id"] for c in retrieval.select(q, exp, profile="r3")]
                self.assertEqual(r3[:len(fro)], fro, i)
                self.assertLessEqual(len(r3), 10)
                self.assertEqual(len(r3), len(set(r3)))
                self.assertLessEqual(sum(1 for c in retrieval.select(q, exp, profile="r3") if c["source"] == "runbooks"),
                                     1 + sum(1 for c in retrieval.select(q, exp, profile="frozen") if c["source"] == "runbooks"))


if __name__ == "__main__":
    unittest.main()
