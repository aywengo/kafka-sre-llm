"""Profile r4 (M5): settings and grid, clean_terms on fixtures, the scored set and the pack rule, the choice rule
(choose-r4) on synthetic funnel tables, the split rule, and runtime equals funnel simulation on a synthetic index with
a deterministic stand-in for the reranker. No model, no network. The live check is the guarded run in
runs/m5/reports/r1_r4_build.md."""
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
from kafka_sre_rag import profiles, retrieval  # noqa: E402
from test_profiles_drift import QUESTIONS, StubReranker, make_rows  # noqa: E402

CAND = profiles.load_candidate()
GRID = CAND["r4_grid"]
DESIGN_S = ("p2-01 p2-05 p2-07 p2-08 p2-09 p2-11 p2-14 p2-16 p2-19 p2-20 p2-21 p2-22 "
            "b1-03 b1-04 b1-06 b1-08 b1-10 b1-11 b1-12 b1-17 b1-18 b1-22 b1-23 b1-25 b1-26 b1-27 b1-29 "
            "b2-01 b2-03 b2-06 b2-07 b2-09 b2-10 b2-12 b2-14 b2-15 b2-17 b2-21 b2-24 b2-26 b2-28 b2-30").split()


class TestSettings(unittest.TestCase):
    def test_r4_entry_is_the_g4_placeholder(self):
        self.assertIn("r4", profiles.CANDIDATES)
        r4 = dict(CAND["profiles"]["r4"])
        g4 = dict(GRID["r4g4"])
        self.assertTrue(r4.pop("description").startswith("candidate, not chosen"))
        g4.pop("description")
        self.assertEqual(r4, g4)

    def test_all_parameters_present(self):
        need = {"order", "base_k", "base_chars", "terms_source", "max_terms", "per_term_n", "bm25_depth", "rerank_set",
                "ce_max_length", "ce_chunk_chars", "ce_batch_size", "query", "q_head_chars", "terms_chars", "lookup",
                "lookup_source", "lookup_score_depth", "lookup_max_chunks", "added_runbook_cap", "k", "max_chars"}
        for name, g in GRID.items():
            self.assertTrue(need <= set(g), name)
        self.assertEqual(sorted(GRID), [f"r4g{i}" for i in range(1, 9)])

    def test_r1_r2_r3_hashes_unchanged(self):
        self.assertEqual(profiles.settings_sha256("r1"), "d1959b5a77b151f1a1547c5163eeb8d134e81d33a53c3ecea54624dff7771b47")
        self.assertEqual(profiles.settings_sha256("r2"), "34dae8871e2312aab32817547bcc2883c7e5c2dfdd80ca9485e689490c0e3502")
        self.assertEqual(profiles.settings_sha256("r3"), "d02254869d50e8e7d560449ae9fc319b9b466f28f3a8dca6c4924a681b6a55b0")

    def test_grid_hashes_differ(self):
        hs = {profiles.settings_sha256("r4", grid=n) for n in GRID}
        self.assertEqual(len(hs), 8)
        with self.assertRaises(ValueError):
            profiles.settings_sha256("r3", grid="r4g1")

    def test_funnel_presets_equal_candidate_json(self):
        for name, g in GRID.items():
            v = F.PRESETS[name]
            self.assertEqual((v.r4, v.query, v.ce_max_length, v.ce_chunk_chars, v.rerank_set, v.terms_source),
                             (True, g["query"], g["ce_max_length"], g["ce_chunk_chars"], g["rerank_set"], g["terms_source"]))
            self.assertEqual((v.k, v.chars, v.lookup, v.runbook_cap, v.per_term_n, v.max_terms, v.bm25_depth, v.ce_batch_size,
                              v.q_head_chars, v.terms_chars),
                             (g["k"], g["max_chars"], g["lookup"], g["added_runbook_cap"], g["per_term_n"], g["max_terms"],
                              g["bm25_depth"], g["ce_batch_size"], g["q_head_chars"], g["terms_chars"]))
            self.assertEqual((g["base_k"], g["base_chars"], g["lookup_source"], g["lookup_score_depth"], g["lookup_max_chunks"]),
                             (6, 14000, "question", F.LOOKUP_SCORE_DEPTH, 12))
            self.assertEqual(g["description"].split(":")[0], f"r4 grid setting G{name[-1]}")


FENCE = "```yaml\nkey: value\nfoo, bar\n```"
CASES = {
    "fence_only": (FENCE, [], True),
    "answer_line": ("ANSWER: kubectl get pods\nmax.poll.interval.ms, consumer lag, offset", ["max.poll.interval.ms", "consumer lag", "offset"], False),
    "yaml": ("apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\n- item\nreplicas, brokers, kafka.config", ["replicas", "brokers", "kafka.config"], False),
    "loop": ("lag, lag, Lag, lag, lag, lag", ["lag"], True),
    "ordinary": ("`session.timeout.ms`, \"heartbeat.interval.ms\", consumer group, rebalance", ["session.timeout.ms", "heartbeat.interval.ms", "consumer group", "rebalance"], False),
}


class TestCleanTerms(unittest.TestCase):
    def test_fixtures(self):
        for name, (exp, want, bad) in CASES.items():
            self.assertEqual(profiles.clean_terms(exp), (want, bad), name)
            self.assertEqual(F.clean_terms(exp), (want, bad), name)

    def test_unterminated_fence_and_limits(self):
        self.assertEqual(profiles.clean_terms("a, b, c\n```python\nx, y, z"), (["a", "b", "c"], False))
        long = ", ".join([f"t{i}" for i in range(60)] + ["w " * 9, "x" * 81])
        t, bad = profiles.clean_terms(long)
        self.assertEqual((len(t), t[0], bad), (40, "t0", False))
        self.assertEqual(profiles.clean_terms(""), ([], True))
        self.assertEqual(profiles.clean_terms("one, two")[1], True)

    def test_terms_sources(self):
        q = "Why does `max.poll.interval.ms` and auto.offset.reset matter?"
        P = dict(GRID["r4g1"])
        P["terms_source"] = "E"
        self.assertEqual(profiles.r4_terms(P, q, "consumer, lag, group"), (["consumer", "lag", "group"], False))
        self.assertEqual(profiles.r4_terms(P, q, "ANSWER: x"), (["max.poll.interval.ms", "auto.offset.reset"], True))
        P["terms_source"] = "EI"
        self.assertEqual(profiles.r4_terms(P, q, "Consumer, MAX.POLL.INTERVAL.MS, lag")[0],
                         ["max.poll.interval.ms", "auto.offset.reset", "Consumer", "lag"])
        P["terms_source"] = "I"
        self.assertEqual(profiles.r4_terms(P, q, "consumer, lag, group")[0], ["max.poll.interval.ms", "auto.offset.reset"])
        for src in "E", "EI", "I":
            P["terms_source"] = src
            v = F.dataclasses.replace(F.PRESETS["r4g1"], terms_source=src)
            self.assertEqual(F.r4_terms(v, q, "consumer, lag, group, ANSWER: z"), profiles.r4_terms(P, q, "consumer, lag, group, ANSWER: z"))

    def test_queries(self):
        P = dict(GRID["r4g4"])
        q = "word " * 300
        terms = [f"term{i}" for i in range(200)]
        qa = profiles.r4_query(P, q, terms)
        head, _, tail = qa.partition("\nSearch terms: ")
        self.assertLessEqual(len(head), 800)
        self.assertTrue(head.endswith("word"))                 # cut back to a whitespace, not inside a word
        self.assertLessEqual(len(tail), 600)
        self.assertTrue(tail.split(", ")[-1] in terms)         # ends at a term boundary
        self.assertEqual(profiles.r4_query(dict(P, query="Qb"), q, ["a", "b"]), "a, b")
        self.assertEqual(profiles.r4_query(dict(P, query="Qb"), "short q", []), "short q")
        self.assertEqual(profiles.r4_query(dict(P, query="Qf"), q, terms), q)
        self.assertEqual(profiles.r4_query(P, "short q", []), "short q")
        for name in ("r4g1", "r4g2", "r4g4"):
            v = F.PRESETS[name]
            self.assertEqual(F.r4_query(v, q, terms), profiles.r4_query(GRID[name], q, terms))


class TestSetAndPack(unittest.TestCase):
    P = GRID["r4g1"]

    def test_rerank_set(self):
        bm = [f"b{i}" for i in range(300)]
        fl = bm[:40]
        r = profiles.r4_rerank_set(self.P, bm=bm, term_list=["t1", "t2"], frozen_list=fl, lookup_hits=["lk", "b9"],
                                   frozen_pack=fl[:6])
        self.assertEqual(r[:2], ["lk", "b9"])
        self.assertEqual(len(r), 24)
        self.assertEqual(len(set(r)), 24)
        self.assertFalse(set(r) & set(fl[:6]))
        r40 = profiles.r4_rerank_set(dict(self.P, rerank_set=40), bm=bm, term_list=[], frozen_list=fl, lookup_hits=[], frozen_pack=fl[:6])
        self.assertEqual(len(r40), 40)

    def pack(self, order, lens=None, src=None, fl=None):
        fl = fl or [f"f{i}" for i in range(10)]
        lens = lens or {c: 1000 for c in fl + order}
        src = src or {c: "strimzi" for c in fl + order}
        return profiles.r4_pack(self.P, frozen_list=fl, order_r=order, source_of=src, lens=lens)

    def test_frozen_first_then_four(self):
        order = [f"r{i}" for i in range(20)]
        pack, capped, rest, stop = self.pack(order)
        self.assertEqual(pack, [f"f{i}" for i in range(6)] + order[:4])
        self.assertEqual((stop, rest[0], capped), ("k", "r4", []))

    def test_runbook_cap_one_added(self):
        order = ["r0", "r1", "r2", "r3", "r4", "r5", "r6"]
        src = {c: "strimzi" for c in order + [f"f{i}" for i in range(10)]}
        src.update({"f1": "runbooks", "r1": "runbooks", "r2": "runbooks"})
        pack, capped, _, _ = self.pack(order, src=src)
        self.assertIn("f1", pack)
        self.assertIn("r1", pack)
        self.assertNotIn("r2", pack)
        self.assertEqual(capped, ["r2"])
        self.assertEqual(len(pack), 10)

    def test_character_stop_whole_chunk(self):
        order = [f"r{i}" for i in range(10)]
        fl = [f"f{i}" for i in range(10)]
        lens = {c: 1000 for c in fl}
        lens.update({c: 6000 for c in order})
        pack, _, rest, stop = self.pack(order, lens=lens, fl=fl)
        # frozen 6 x 1000 = 6000; r0..r2 -> 24,000 accounted (the third is cut by the accounting only); stop: no room
        self.assertEqual(pack, fl[:6] + order[:3])
        self.assertEqual(stop, "chars")
        self.assertEqual(rest[0], "r3")


class TestChooseRule(unittest.TestCase):
    def stat(self, points, served, base, lost=0, added=(8000,) * 5):
        return {"points": points, "served": served, "base": base, "lost": lost, "added_chars": list(added), "gain_pp": 0,
                "se_pp": 0, "items": 42}

    def table(self, **kw):
        out = {}
        for lab, (s, c, lost, added) in kw.items():
            out[lab] = {"S": self.stat(202, 60 + s, 60, lost, added), "C": self.stat(219, 65 + c, 65), "facts": {"lost": 0}}
        return out

    def run_rule(self, table, time=None, equal=None):
        labs = list(table)
        time = time or {l: [3.0, 4.0, 5.0] for l in labs}
        equal = equal or {l: True for l in labs}
        return F.choose_r4(table, time, equal)

    def test_clear_winner(self):
        r = self.run_rule(self.table(r4g1=(40, 30, 0, (5000,) * 5), r4g2=(70, 50, 0, (9000,) * 5)))
        self.assertEqual((r["verdict"], r["chosen"]), ("CONTINUE", "r4g2"))
        self.assertTrue(F.choose_report(r, self.table(r4g1=(40, 30, 0, (5000,) * 5), r4g2=(70, 50, 0, (9000,) * 5))).startswith("VERDICT: CONTINUE"))

    def test_near_tie_decided_by_smaller_pack(self):
        t = self.table(r4g1=(70, 50, 0, (9000,) * 5), r4g2=(67, 50, 0, (6000,) * 5), r4g3=(65, 50, 0, (3000,) * 5))
        r = self.run_rule(t)
        self.assertEqual(r["chosen"], "r4g2")          # g3 is 5 points below the best: outside the 4-point band
        t = self.table(r4g1=(70, 50, 0, (9000,) * 5), r4g2=(66, 50, 0, (6000,) * 5), r4g3=(70, 50, 0, (6000,) * 5))
        self.assertEqual(self.run_rule(t, time={"r4g1": [3, 3, 3], "r4g2": [4, 4, 4], "r4g3": [2, 2, 2]})["chosen"], "r4g3")

    def test_gain_under_bar_on_s(self):
        r = self.run_rule(self.table(r4g1=(49, 50, 0, (8000,) * 5)))      # 49 of 202 = 24.3 pp
        self.assertEqual(r["verdict"], "STOP")
        self.assertIn("gain on S", r["reasons"][0])
        self.assertEqual(self.run_rule(self.table(r4g1=(51, 50, 0, (8000,) * 5)))["verdict"], "CONTINUE")   # 25.2 pp
        self.assertTrue(F.choose_report(r, self.table(r4g1=(49, 50, 0, (8000,) * 5))).startswith("VERDICT: STOP AND REPORT, NO B4"))

    def test_gain_under_bar_on_c(self):
        r = self.run_rule(self.table(r4g1=(60, 39, 0, (8000,) * 5)))      # 39 of 219 = 17.8 pp
        self.assertEqual(r["verdict"], "STOP")
        self.assertIn("gain on C", " ".join(r["reasons"]))
        self.assertEqual(self.run_rule(self.table(r4g1=(60, 40, 0, (8000,) * 5)))["verdict"], "CONTINUE")

    def test_loss(self):
        r = self.run_rule(self.table(r4g1=(70, 50, 1, (8000,) * 5)))
        self.assertEqual(r["verdict"], "STOP")       # the only setting loses, so it is not eligible
        self.assertIn("e1", r["settings"]["r4g1"]["why_not"][0])
        t = self.table(r4g1=(70, 50, 1, (8000,) * 5), r4g2=(60, 50, 0, (8000,) * 5))
        self.assertEqual(self.run_rule(t)["chosen"], "r4g2")     # a loser never wins
        t = self.table(r4g1=(70, 50, 0, (8000,) * 5))
        t["r4g1"]["facts"] = {"lost": 2}
        self.assertEqual(self.run_rule(t)["verdict"], "STOP")

    def test_no_eligible_setting(self):
        t = self.table(r4g1=(70, 50, 0, (20000,) * 5), r4g2=(70, 50, 0, (8000,) * 5), r4g3=(70, 50, 0, (8000,) * 5))
        r = self.run_rule(t, time={"r4g1": [3, 3, 3], "r4g2": [7, 7, 7], "r4g3": [3, 3, 3]}, equal={"r4g1": True, "r4g2": True, "r4g3": False})
        self.assertEqual((r["verdict"], r["chosen"], r["reasons"]), ("STOP", None, ["no setting is eligible"]))
        self.assertIn("e4", " ".join(r["settings"]["r4g1"]["why_not"]))
        self.assertIn("e3", " ".join(r["settings"]["r4g2"]["why_not"]))
        self.assertIn("e2", " ".join(r["settings"]["r4g3"]["why_not"]))
        self.assertTrue(F.choose_report(r, t).startswith("VERDICT: STOP AND REPORT, NO B4: no setting is eligible"))

    def test_p90_limits(self):
        t = self.table(r4g1=(70, 50, 0, (8000,) * 3 + (17000,) * 2))        # p90 above 16,000
        self.assertEqual(self.run_rule(t)["verdict"], "STOP")
        self.assertEqual(self.run_rule(self.table(r4g1=(70, 50, 0, (8000,) * 5)), time={"r4g1": [1, 1, 1, 9.5, 9.5]})["verdict"], "STOP")


class TestSplit(unittest.TestCase):
    def test_rule_alternates_by_hash(self):
        ids = [("x-%02d" % i, "k") for i in range(5)]
        S, C = F.build_split({"x": ids})
        order = sorted((i for i, _ in ids), key=lambda i: hashlib.sha256(("m5-r4-split:" + i).encode()).hexdigest())
        self.assertEqual(S, sorted([order[0], order[2], order[4]]))
        self.assertEqual(C, sorted([order[1], order[3]]))

    def test_split_file_and_design_list(self):
        p = ROOT / "runs/m5/r4_split.json"
        if not p.exists():
            self.skipTest("runs/m5/r4_split.json (git-ignored) not present")
        d = json.loads(p.read_text())
        self.assertEqual(sorted(d["S"]), sorted(DESIGN_S))
        self.assertEqual((len(d["S"]), len(d["C"])), (42, 42))
        self.assertFalse(set(d["S"]) & set(d["C"]))

    def test_split_from_items_reproduces_design(self):
        files = {"pilot": ROOT / "eval/m4/pilot2/items.jsonl", "b1": ROOT / "eval/m4/b1/items.jsonl", "b2": ROOT / "eval/m4/b2/items.jsonl"}
        if not all(f.exists() for f in files.values()):
            self.skipTest("item files not present")
        by = {b: [(json.loads(l)["id"], json.loads(l)["kind"]) for l in f.read_text().splitlines() if l.strip()] for b, f in files.items()}
        S, C = F.build_split(by)
        self.assertEqual(S, sorted(DESIGN_S))
        self.assertEqual(len(C), 42)


class TestRuntimeEqualsSimulation(unittest.TestCase):
    def setUp(self):
        if not (ROOT / "runs/v3/INDEX_FROZEN.json").is_file():
            self.skipTest("runs/v3/INDEX_FROZEN.json is not in the repository (it is kept with the run files; no command makes it)")
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        p = self.tmp / "idx.jsonl"
        p.write_text("\n".join(json.dumps(r) for r in make_rows()))
        self.path = str(p)
        self.model = StubReranker()
        for m in (mock.patch.object(RT, "reranker", lambda: self.model),
                  mock.patch.object(retrieval, "_reranker_at", lambda L: self.model)):
            m.start()
            self.addCleanup(m.stop)

    def scorers(self):
        ix = F.Index(self.path)
        sc = F.CEScorer(ix.byid, self.tmp / "ce.jsonl", model=self.model)
        sc.heartbeat = 0
        sc.r4 = F.R4Scorers(ix, self.tmp / "r4", model_of=lambda L: self.model, say=lambda *a: None)
        return ix, sc

    def test_all_grid_settings(self):
        ix, sc = self.scorers()
        added = 0
        for q, exp in QUESTIONS:
            st = F.ItemState(ix, q, exp)
            fro = [c["id"] for c in retrieval.select(q, exp, profile="frozen", index_path=self.path)]
            for name, g in GRID.items():
                got = [c["id"] for c in retrieval.select(q, exp, profile="r4", index_path=self.path, settings=g)]
                st.__dict__.pop("_rk_cache", None)
                want = F.pack_of(st, F.PRESETS[name], sc)
                self.assertEqual(got, want, f"{name} {q!r}")
                self.assertEqual(got[:len(fro)], fro)
                self.assertLessEqual(len(got), 10)
                self.assertEqual(len(got), len(set(got)))
                added += len(got) - len(fro)
        self.assertGreater(added, 20)

    def test_one_call_and_stand_in_order(self):
        ix, sc = self.scorers()
        calls = []
        orig = self.model.predict
        self.model.predict = lambda pairs, batch_size=8: (calls.append(len(pairs)), orig(pairs, batch_size))[1]
        q, exp = QUESTIONS[0]
        retrieval.select(q, exp, profile="r4", index_path=self.path, settings=GRID["r4g1"])
        self.assertEqual(len(calls), 2)                  # the frozen call (40 pairs) and ONE r4 call
        self.assertEqual(calls[0], 40)
        self.assertLessEqual(calls[1], 24)

    def test_default_profile_uses_candidate_entry(self):
        got = [c["id"] for c in retrieval.select(QUESTIONS[0][0], QUESTIONS[0][1], profile="r4", index_path=self.path)]
        want = [c["id"] for c in retrieval.select(QUESTIONS[0][0], QUESTIONS[0][1], profile="r4", index_path=self.path, settings=GRID["r4g4"])]
        self.assertEqual(got, want)

    def test_without_reranker(self):
        got = retrieval.select(QUESTIONS[0][0], QUESTIONS[0][1], profile="r4", index_path=self.path, rerank=False)
        self.assertTrue(got)
        self.assertEqual(len(got), len(set(c["id"] for c in got)))

    def test_whole_call_cache(self):
        ix, sc = self.scorers()
        v = F.PRESETS["r4g1"]
        ids = ["c001", "c002", "c003"]
        a = sc.r4.scores(v, "q", ids, "x")
        b = sc.r4.scores(v, "q", ids, "x")
        self.assertEqual((a[1], b[1], a[0] == b[0]), ("computed", "cached", True))
        c = sc.r4.scores(v, "q", ids[::-1], "x")             # a different order is a different call
        self.assertEqual(c[1], "computed")
        again = F.R4Scorers(ix, self.tmp / "r4", model_of=lambda L: None, say=lambda *a: None)
        self.assertEqual(again.scores(v, "q", ids, "x")[1], "cached")
        self.assertTrue((self.tmp / "r4" / f"ce_{ix.sha[:12]}_512_1500_frozenfmt_r4.jsonl").exists())
        v2 = F.PRESETS["r4g4"]
        sc.r4.scores(v2, "q", ids, "x")
        self.assertTrue((self.tmp / "r4" / f"ce_{ix.sha[:12]}_1024_3000_frozenfmt_r4.jsonl").exists())

    def test_stages_and_stand_in(self):
        ix, sc = self.scorers()
        q, exp = QUESTIONS[1]
        st = F.ItemState(ix, q, exp)
        st.id = "t"
        v = F.PRESETS["r4g3"]
        pack = F.pack_of(st, v, sc)
        info = st.r4info
        for c in ix.ids:
            stage = F.chunk_stage(st, v, sc, c)
            if c in pack:
                self.assertEqual(stage, "in_pack")
            elif c in info["rest"]:
                self.assertIn(stage, ("below_cut", "cut_by_chars"))
            elif c in st.capped:
                self.assertEqual(stage, "capped_runbook")
            else:
                self.assertIn(stage, ("outside_rerank_set", "outside_pool"))
        model = F.StandIn(ix, {})
        c0 = ix.chunks[3]
        s = model.predict([("q", (c0["title"] + "\n" + c0["text"])[:1500])])
        self.assertTrue(F.StandIn.same_call(model.calls[0], ("q", (c0["id"],))))
        self.assertEqual(s, model.predict([("q", (c0["title"] + "\n" + c0["text"])[:1500])]))
        self.assertEqual(F.check_pack_rules(["a", "b", "c"], ["a", "b"]), {"prefix": True, "unique": True, "max10": True, "loses_0": True})
        self.assertFalse(F.check_pack_rules(["b", "a"], ["a", "b"])["prefix"])



class TestTablesOnSyntheticIndex(unittest.TestCase):
    """evaluate -> r4_tables -> r4_table -> choose_report -> cmd_choose_r4 on the synthetic index, made-up needs."""

    def setUp(self):
        if not (ROOT / "runs/v3/INDEX_FROZEN.json").is_file():
            self.skipTest("runs/v3/INDEX_FROZEN.json is not in the repository (it is kept with the run files; no command makes it)")
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        p = self.tmp / "idx.jsonl"
        p.write_text("\n".join(json.dumps(r) for r in make_rows()))
        self.model = StubReranker()

    def test_tables_and_choose_command(self):
        import argparse
        ix = F.Index(str(self.tmp / "idx.jsonl"))
        sc = F.CEScorer(ix.byid, self.tmp / "ce.jsonl", model=self.model)
        sc.heartbeat = 0
        sc.r4 = F.R4Scorers(ix, self.tmp / "r4", model_of=lambda L: self.model, say=lambda *a: None)
        states, needs = {}, []
        for n, (q, exp) in enumerate(QUESTIONS[:5]):
            i = f"s{n}"
            states[i] = F.ItemState(ix, q, exp)
            states[i].id = i
            for k in range(3):
                needs.append({"row_type": "point", "item": i, "point": f"m{k}", "weight": 1 + k % 2, "status": "mapped",
                              "chunks": [f"c{(n * 7 + k * 11) % 70:03d}", f"c{(n * 5 + k) % 70:03d}"]})
            needs.append({"row_type": "fact", "item": i, "point": None, "chunks": [f"c{n:03d}"], "tags": []})
        variants = [F.parse_variant(x) for x in ("frozen", "r4g1", "r4g4")]
        info = {}
        stages, _ = F.evaluate(ix, needs, states, variants, sc, lambda *a: None, info)
        split = ({"s0", "s1", "s2"}, {"s3", "s4"})
        r4 = F.r4_tables(needs, stages, info, ["r4g1", "r4g4"], split, {"r4g1": [1.0, 2.0], "r4g4": [3.0, 4.0]})
        self.assertEqual(r4["points"]["r4g1"]["S"]["points"], 9)
        self.assertEqual(r4["points"]["r4g1"]["S+C"]["points"], 15)
        self.assertEqual(r4["points"]["r4g1"]["S"]["lost"], 0)          # r4 adds to the frozen pack: nothing is lost
        self.assertEqual(r4["facts"]["r4g1"]["facts"]["lost"], 0)
        self.assertTrue(r4["points"]["r4g1"]["S"]["pack_chars"])
        md = F.r4_table(r4["points"], {"r4g1": [1.0, 2.0]})
        self.assertEqual(md.splitlines()[0].count("|"), len(F.R4_COLS) + 1)
        for line in md.splitlines():
            self.assertEqual(line.count("|"), len(F.R4_COLS) + 1)
        stage_sum = sum(r4["points"]["r4g4"]["S+C"]["stages"].values())
        self.assertEqual(stage_sum, 15)
        # the choose command on files
        funnel = {"r4": json.loads(json.dumps(r4, default=list))}
        tm = {"settings": {l: {"seconds": [1.0, 2.0, 3.0]} for l in ("r4g1", "r4g4")}}
        eq = {"settings": {l: {"runtime_equals_sim": True} for l in ("r4g1", "r4g4")}}
        paths = {}
        for k, obj in (("f", funnel), ("t", tm), ("e", eq)):
            paths[k] = self.tmp / f"{k}.json"
            paths[k].write_text(json.dumps(obj))
        lines = []
        res = F.cmd_choose_r4(argparse.Namespace(funnel=str(paths["f"]), time=str(paths["t"]), equal=str(paths["e"]), out=None),
                              lines.append)
        self.assertTrue(lines[0].startswith("VERDICT:"))
        self.assertIn(res["verdict"], ("CONTINUE", "STOP"))
        self.assertTrue(any("S and C together" in l for l in lines))


if __name__ == "__main__":
    unittest.main()
