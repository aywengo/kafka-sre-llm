"""scripts/retrieval_funnel.py on a small synthetic index with a stub reranker (no model, no real index)."""
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import build_packs_v3 as BP  # noqa: E402
import retrieval_funnel as RF  # noqa: E402
import retrieve as RT  # noqa: E402


class StubModel:
    """Deterministic stand-in for the cross-encoder: number of distinct query words found in the passage."""

    def __init__(self):
        self.calls = []

    def predict(self, pairs, batch_size=8):
        self.calls.append(len(pairs))
        return [float(len(set(re.findall(r"\w+", q.lower())) & set(re.findall(r"\w+", p.lower())))) for q, p in pairs]


class NoModel:
    def predict(self, *a, **k):
        raise AssertionError("the reranker must not be used")


def make_index(d, n=12):
    chunks = []
    for i in range(n):
        chunks.append({"id": f"c{i:02d}", "source": "runbooks" if i in (1, 2) else "kafka", "license": "x",
                       "path": f"raw/doc{i}.md", "title": f"key.{i}.alpha" if i % 3 == 0 else f"Topic {i}",
                       "text": ("alpha beta " * (i + 1)) + f" word{i} " + ("filler " * (400 * (i % 2 + 1)))})
    p = Path(d) / "chunks.jsonl"
    p.write_text("".join(json.dumps(c) + "\n" for c in chunks))
    return str(p)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = make_index(self.tmp.name)
        self.ix = RF.Index(self.path)
        self.model = StubModel()
        self.scorer = RF.CEScorer(self.ix.byid, Path(self.tmp.name) / "ce.jsonl", model=self.model)

    def state(self, q="alpha beta word5", exp="word7, key.3.alpha"):
        return RF.ItemState(self.ix, q, exp)


class RuleTests(Base):
    def test_select_ranked_equals_build_packs(self):
        lens = {i: n for i, n in enumerate([9000, 4800, 600, 400, 3000, 3000, 14000, 100])}
        for k, mc in ((6, 14000), (3, 6000), (8, 20000)):
            order = list(lens)
            self.assertEqual(RF.select_ranked(order, lens, k, mc), BP.select_ranked(order, lens, k, mc))

    def test_frozen_order_equals_retrieve_rank(self):
        st = self.state()
        with mock.patch.object(RT, "reranker", return_value=StubModel()):
            want = [self.ix.ids[i] for i in RT.rank(st.q, st.exp, index_path=self.path)]
        got = RF.frozen_order(st.head(40), self.scorer.scores(st.q, st.head(40)))
        self.assertEqual(got, want)

    def test_compare_pack(self):
        self.assertEqual(RF.compare_pack(["a", "b"], ["a", "b"]), "exact")
        self.assertEqual(RF.compare_pack(["b", "a"], ["a", "b"]), "order_only")
        self.assertEqual(RF.compare_pack(["a", "c"], ["a", "b"]), "content")

    def test_rrf_and_runbook_cap(self):
        self.assertEqual(RF.rrf([["a", "b", "c"], ["c", "b", "a"]], [1, 1])[:1], ["a"])
        src = {"a": "runbooks", "b": "kafka", "c": "runbooks"}
        self.assertEqual(RF.cap_runbooks(["a", "b", "c"], src, 1), ["a", "b"])
        self.assertEqual(RF.cap_runbooks(["a", "b", "c"], src, 0), ["b"])


class ScorerTests(Base):
    def test_cache_hits_and_resume(self):
        ids = self.state().head(40)
        a = self.scorer.scores("alpha word5", ids)
        n = sum(self.model.calls)
        self.assertEqual(self.scorer.scores("alpha word5", ids), a)
        self.assertEqual(sum(self.model.calls), n)
        again = RF.CEScorer(self.ix.byid, self.scorer.path, model=NoModel())    # resumed from the file
        self.assertEqual(again.scores("alpha word5", ids), a)

    def test_head_is_one_call_in_pool_order(self):
        ids = self.ix.ids
        self.scorer.scores("q", ids[:5], exact=3)
        self.assertEqual(self.model.calls, [3, 2])
        self.scorer.scores("q", ids[:6], exact=3)       # only the new tail chunk is scored
        self.assertEqual(self.model.calls, [3, 2, 1])


class StageTests(Base):
    def stage(self, v, cid, st=None):
        return RF.chunk_stage(st or self.state(), v, self.scorer, cid)

    def test_all_stages(self):
        st = self.state()
        pool2 = RF.Variant(pool=2, order="bm25", k=1, chars=14000)
        top = st.head(12)
        self.assertEqual(self.stage(pool2, "nope", st), "not_in_index")
        self.assertEqual(self.stage(pool2, top[0], st), "in_pack")
        self.assertEqual(self.stage(pool2, top[1], st), "below_cut")
        self.assertEqual(self.stage(pool2, top[5], st), "outside_pool")
        small = RF.Variant(order="bm25", k=3, chars=len(self.ix.byid[top[0]]["text"]) + 100)
        self.assertEqual(self.stage(small, top[1], st), "cut_by_chars")
        self.assertEqual(self.stage(small, top[2], st), "cut_by_chars")
        self.assertEqual(self.stage(small, top[3], st), "below_cut")
        rb = next(c for c in top if self.ix.source[c] == "runbooks")
        self.assertEqual(self.stage(RF.Variant(order="bm25", runbook_cap=0, k=12, chars=10 ** 7), rb, st), "capped_runbook")

    def test_frozen_pack_is_selection_of_frozen_order(self):
        st = self.state()
        rk = RF.frozen_order(st.head(40), self.scorer.scores(st.q, st.head(40)))
        self.assertEqual(RF.pack_of(st, RF.FROZEN_V, self.scorer), RF.select_ranked(rk, self.ix.len, 6, 14000))

    def test_terms_fusion_needs_no_reranker(self):
        sc = RF.CEScorer(self.ix.byid, Path(self.tmp.name) / "none.jsonl", model=NoModel())
        st = self.state(exp="word7, word9, key.3.alpha")
        rk, cands = RF.ranking(st, RF.PRESETS["r2"], sc)
        self.assertIn(self.ix.ids[7], rk)
        self.assertEqual(set(rk), cands)
        self.assertFalse(RF.PRESETS["r2"].uses_reranker)
        self.assertTrue(RF.PRESETS["r1"].uses_reranker)

    def test_lookup_reserves_front_slots(self):
        st = self.state(q="what is key.9.alpha", exp="")
        rk, _ = RF.ranking(st, RF.Variant(order="bm25", lookup=1), self.scorer)
        self.assertEqual(rk[0], "c09")
        self.assertEqual(RF.Lookup(self.ix.chunks).hits("see key.6.alpha and key.3.alpha", "", 2, lambda c: 0), ["c06", "c03"])


class FunnelTests(Base):
    def test_evaluate_and_totals(self):
        needs = [{"row_type": "point", "item": "i1", "point": "m1", "weight": 2, "status": "mapped", "chunks": ["c05"]},
                 {"row_type": "point", "item": "i1", "point": "m2", "weight": 1, "status": "not_indexed", "chunks": []},
                 {"row_type": "scenario", "item": "i1", "chunks": ["c05", "c06"]},
                 {"row_type": "fact", "item": "i1", "chunks": ["c03"], "tags": ["C1"]}]
        states = {"i1": self.state()}
        variants = [("frozen", RF.FROZEN_V), ("bm25", RF.PRESETS["bm25"]), ("all", RF.Variant(order="bm25", k=12, chars=10 ** 7))]
        lines = []
        stages, skipped = RF.evaluate(self.ix, needs, states, variants, self.scorer, lines.append)
        self.assertEqual(skipped, [])
        self.assertEqual(len(lines), 1)                       # one line per item
        self.assertEqual(stages["all"][("point", "i1", "m1")], "in_pack")
        self.assertEqual(stages["all"][("point", "i1", "m2")], "not_indexed")
        tot = RF.totals(needs, stages, "frozen")
        self.assertEqual(tot["all"]["points"]["of"], 1)
        self.assertEqual(tot["all"]["points"]["served"], 1)
        s0 = tot["frozen"]["points"]["served"]
        self.assertEqual(tot["all"]["points"]["gained"], 1 - s0)
        self.assertEqual(tot["frozen"]["points"]["gained"], 0)
        self.assertEqual(tot["all"]["facts_C1"]["served"], 1)
        self.assertIn("| all |", RF.table(tot))

    def test_replay_counts(self):
        st = self.state()
        st.stored = RF.pack_of(st, RF.FROZEN_V, self.scorer)
        st2 = self.state(q="alpha", exp="")
        st2.stored = ["c00"]
        res, detail = RF.replay({"a": st, "b": st2}, self.scorer, lambda s: None)
        self.assertEqual(res["exact"], 1)
        self.assertEqual(detail["a"], "exact")
        self.assertIn(detail["b"], ("order_only", "content"))

    def test_slow_scoring_prints_progress(self):
        import time

        class Slow(StubModel):
            def predict(self, pairs, batch_size=8):
                time.sleep(0.5)
                return super().predict(pairs, batch_size)

        lines = []
        sc = RF.CEScorer(self.ix.byid, Path(self.tmp.name) / "slow.jsonl", model=Slow())
        sc.say, sc.heartbeat = lines.append, 0.1
        got = sc.scores("alpha word5", self.ix.ids[:6])
        self.assertGreaterEqual(len(lines), 2)
        self.assertIn("6 pairs", lines[0])
        self.assertEqual(got, RF.CEScorer(self.ix.byid, Path(self.tmp.name) / "ref.jsonl", model=StubModel())
                         .scores("alpha word5", self.ix.ids[:6]))       # the heartbeat does not change scores

    def test_say_flushes(self):
        with mock.patch("builtins.print") as pr:
            RF.say_flush("x")
        pr.assert_called_once_with("x", flush=True)

    def test_parse_variant(self):
        lab, v = RF.parse_variant("r2:k=10,chars=24000")
        self.assertEqual((v.k, v.chars, v.lookup, v.fuse), (10, 24000, 2, ("bm25", "terms")))
        _, v = RF.parse_variant("custom:order=fusion,fuse=bm25+terms,fuse_weights=2+1")
        self.assertEqual((v.fuse, v.fuse_weights), (("bm25", "terms"), (2.0, 1.0)))
        with self.assertRaises(SystemExit):
            RF.parse_variant("nope")
        self.assertEqual(RF.PRESETS["frozen"], RF.Variant())

    def test_frozen_index_hash_is_checked(self):
        self.assertFalse(self.ix.frozen_ok)      # the synthetic index is not the frozen one


if __name__ == "__main__":
    unittest.main()
