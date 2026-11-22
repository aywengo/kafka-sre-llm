"""M5 R5 (scripts/r5_qwen.py): the four settings, the pin, the scorer and its cache on a synthetic index with fake models,
the paired difference, and the rule (choose-r5) on synthetic tables. No model, no network. The live checks (loader,
template, cache; the stand-in; R.json) are the guarded runs in runs/m5/reports/r5_build.md."""
import argparse
import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import r5_qwen as R  # noqa: E402
import retrieval_funnel as F  # noqa: E402
from test_retrieval_funnel import make_index  # noqa: E402


class FakeQwen:
    """predict(pairs, batch_size, instruction) -> overlap scores; records every call."""

    def __init__(self):
        self.calls, self.last_truncated = [], 0
        self.model = self      # the loader's own predict (the warm-up) is this one

    def predict(self, pairs, batch_size=8, instruction=R.DEFAULT_INSTRUCTION):
        self.calls.append((instruction, [q for q, _ in pairs][:1], len(pairs)))
        return [float(len(set(re.findall(r"\w+", q.lower())) & set(re.findall(r"\w+", p.lower())))) for q, p in pairs]


class TestSettings(unittest.TestCase):
    def test_four_settings(self):
        self.assertEqual(R.SETTINGS, {"r5a": ("Qb", "default"), "r5b": ("Qb", "task"), "r5c": ("Qh", "default"),
                                      "r5d": ("Qh", "task")})
        self.assertEqual(R.DEFAULT_INSTRUCTION, "Given a web search query, retrieve relevant passages that answer the query")
        self.assertEqual(R.TASK_INSTRUCTION,
                         "Given an operations question about Kafka, Strimzi, Kubernetes or Argo CD, or search terms taken from "
                         "one, retrieve documentation passages that state a fact needed to answer it")
        self.assertEqual((R.MAX_LENGTH, R.PASSAGE_CHARS, R.SCORED_SET, R.QH_CHARS), (1024, 1500, 24, 800))

    def test_variant_is_r4g2_with_a_name(self):
        base = F.PRESETS["r4g2"]
        for n in R.SETTINGS:
            v = R.variant(n)
            self.assertEqual((v.r4, v.query, v.ce_chunk_chars, v.rerank_set, v.terms_source, v.k, v.chars, v.runbook_cap),
                             (base.r4, base.query, base.ce_chunk_chars, base.rerank_set, base.terms_source, base.k, base.chars,
                              base.runbook_cap))
            self.assertEqual(v.setting, n)
        self.assertEqual(len({R.variant(n) for n in R.SETTINGS}), 4)
        self.assertEqual((base.query, base.rerank_set, base.ce_chunk_chars), ("Qb", 24, 1500))

    def test_hashes_differ_and_are_stable(self):
        hs = {R.settings_hash(n) for n in R.SETTINGS}
        self.assertEqual(len(hs), 4)
        self.assertEqual(R.settings_hash("r5a"), R.settings_hash("r5a"))

    def test_cache_names(self):
        a, b = R.cache_name("default"), R.cache_name("task")
        self.assertNotEqual(a, b)
        self.assertRegex(a, r"^qwen3r_e61197ed4502_1024_1500_[0-9a-f]{8}_fp32\.jsonl$")

    def test_pin_file_equals_the_code(self):
        if not (ROOT / R.PIN_FILE).is_file():
            self.skipTest(R.PIN_FILE + " is not in the repository (git-ignored run file; no command makes it)")
        pin = R.load_pin()
        self.assertEqual((pin["model"], pin["revision"]), (R.MODEL_ID, R.REVISION))
        self.assertEqual(pin["license"], "apache-2.0")
        self.assertEqual(pin["license_note"], "no licence file in the repository")
        self.assertEqual(pin["hub_cache"], "the default hub cache")
        self.assertRegex(pin["model_safetensors_sha256"], r"^[0-9a-f]{64}$")
        self.assertTrue(pin["model_safetensors_sha256"].startswith("27cd75a4"))
        self.assertEqual(pin, json.loads(json.dumps(R.build_pin(pin["model_safetensors_sha256"]))))
        self.assertEqual(set(pin["settings"]), set(R.SETTINGS))
        for n, s in pin["settings"].items():
            self.assertEqual(s["sha256"], R.settings_hash(n))

    def test_new_files_hold_no_home_path_host_address_or_key(self):
        if not (ROOT / R.PIN_FILE).is_file():
            self.skipTest(R.PIN_FILE + " is not in the repository (git-ignored run file; no command makes it)")
        pats = [re.compile(p) for p in ("/Us" + "ers/", "/ho" + "me/", r"\b\d{1,3}(\.\d{1,3}){3}\b", r"hf_[A-Za-z0-9]{10,}", r"sk-[A-Za-z0-9]{10,}",
                                        r"\.lo" + r"cal\b")]
        for f in ("scripts/r5_qwen.py", "tests/test_r5.py", "runs/m5/RERANKER_R5.json"):
            text = (ROOT / f).read_text()
            for p in pats:
                self.assertIsNone(p.search(text), f"{f}: {p.pattern}")


class TestQueriesAndTruncation(unittest.TestCase):
    def test_qh_is_the_head_rule(self):
        q = ("word " * 300).strip() + "\n\nEvidence:\n" + "x " * 400
        h = R.qwen_query("r5c", "terms here", q)
        self.assertEqual(h, F._head(q, 800))
        self.assertLessEqual(len(h), 800)
        self.assertEqual(R.qwen_query("r5d", "terms here", "short question"), "short question")
        self.assertEqual(R.qwen_query("r5a", "terms here", q), "terms here")
        self.assertEqual(R.qwen_query("r5b", "terms here", q), "terms here")

    def test_messages_carry_the_instruction_as_system(self):
        m = R.messages("I", "Q", "D")
        self.assertEqual([x["role"] for x in m], ["system", "query", "document"])
        self.assertEqual([x["content"] for x in m], ["I", "Q", "D"])

    def test_count_over(self):
        self.assertEqual(R.count_over([10, 1024, 1025, 3000], 1024), 2)
        self.assertEqual(R.count_over([], 1024), 0)


class Base(unittest.TestCase):
    def setUp(self):
        if not (ROOT / "runs/v3/INDEX_FROZEN.json").is_file():
            self.skipTest("runs/v3/INDEX_FROZEN.json is not in the repository (it is kept with the run files; no command makes it)")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = make_index(self.tmp.name, 40)
        self.ix = F.Index(self.path)
        self.model = FakeQwen()
        self.bge_model = FakeQwen()
        q = "alpha beta word5 " + "extra words " * 100
        self.states = {"i1": F.ItemState(self.ix, q, "word7, key.3.alpha, word9, key.6.alpha"),
                       "i2": F.ItemState(self.ix, "alpha word11 filler", "word13, key.9.alpha, word2")}
        for i, st in self.states.items():
            st.id = i
        self.questions = {i: st.q for i, st in self.states.items()}

        class Stub:
            def predict(s, pairs, batch_size=8):
                return self.bge_model.predict(pairs, batch_size)

        self.frozen = F.CEScorer(self.ix.byid, Path(self.tmp.name) / "ce.jsonl", model=Stub())
        self.bge = F.R4Scorers(self.ix, Path(self.tmp.name) / "bge", model_of=lambda L: Stub(), say=lambda *x: None)
        self.r5 = R.R5Scorers(self.ix, Path(self.tmp.name) / "qwen", self.questions, qwen=self.model, bge=self.bge,
                              say=lambda *x: None)
        self.frozen.r4 = self.r5


class TestScorer(Base):
    def test_one_call_per_question_and_a_rerun_costs_nothing(self):
        st = self.states["i1"]
        v = R.variant("r5a")
        F.ranking(st, v, self.frozen)
        self.assertEqual(len(self.model.calls), 1)
        self.assertEqual(self.model.calls[0][2], st.r4info["pairs"])
        self.assertEqual(self.model.calls[0][0], R.DEFAULT_INSTRUCTION)
        files = list((Path(self.tmp.name) / "qwen").glob("qwen3r_*"))
        self.assertEqual([f.name for f in files], [R.cache_name("default")])
        rec = json.loads(files[0].read_text().splitlines()[0])
        self.assertEqual((rec["n"], rec["trunc"]), (st.r4info["pairs"], 0))
        # a fresh scorer object over the same files: no model call
        again = R.R5Scorers(self.ix, Path(self.tmp.name) / "qwen", self.questions, qwen=FakeQwen(), bge=self.bge, say=lambda *x: None)
        self.frozen.r4 = again
        st.__dict__.pop("_rk_cache", None)
        F.ranking(st, v, self.frozen)
        self.assertEqual(again.qwen.calls, [])
        self.assertEqual(again.computed, 0)
        self.assertEqual(again.truncated_total(), (st.r4info["pairs"], 0))

    def test_instructions_and_queries_reach_the_model(self):
        st = self.states["i1"]
        seen = {}
        for n in R.SETTINGS:
            self.model.calls.clear()
            st.__dict__.pop("_rk_cache", None)
            F.ranking(st, R.variant(n), self.frozen)
            (instr, q, n_pairs), = self.model.calls
            seen[n] = (instr, q[0])
        self.assertEqual(seen["r5a"][0], R.DEFAULT_INSTRUCTION)
        self.assertEqual(seen["r5b"][0], R.TASK_INSTRUCTION)
        self.assertEqual(seen["r5c"][0], R.DEFAULT_INSTRUCTION)
        self.assertEqual(seen["r5d"][0], R.TASK_INSTRUCTION)
        self.assertEqual(seen["r5a"][1], seen["r5b"][1])
        self.assertEqual(seen["r5c"][1], seen["r5d"][1])
        self.assertEqual(seen["r5c"][1], F._head(st.q, 800))
        self.assertNotEqual(seen["r5a"][1], seen["r5c"][1])
        self.assertEqual(len(list((Path(self.tmp.name) / "qwen").glob("qwen3r_*"))), 2)   # one file per instruction

    def test_same_scores_give_r4g2_exactly_and_the_frozen_six_first(self):
        """The wiring: with the same scores r5a (Qb, same passage, same set) gives r4g2's pack and stage for every chunk."""
        for i, st in self.states.items():
            st.__dict__.pop("_rk_cache", None)
            old = F.pack_of(st, F.PRESETS["r4g2"], self.frozen)
            old_info = dict(st.r4info)
            st.__dict__.pop("_rk_cache", None)
            new = F.pack_of(st, R.variant("r5a"), self.frozen)
            self.assertEqual(new, old)
            self.assertEqual((st.r4info["R"], st.r4info["query"]), (old_info["R"], old_info["query"]))
            self.assertEqual(new[:len(old_info["frozen_pack"])], old_info["frozen_pack"])
            self.assertLessEqual(len(new), 10)
            self.assertEqual(len(new), len(set(new)))
            st.__dict__.pop("_rk_cache", None)
            for c in list(st.r4info["R"]) + list(self.ix.byid)[:10]:
                self.assertEqual(F.chunk_stage(st, R.variant("r5a"), self.frozen, c),
                                 F.chunk_stage(st, F.PRESETS["r4g2"], self.frozen, c))

    def test_the_scored_set_is_r4g2s_in_its_order(self):
        st = self.states["i1"]
        F.ranking(st, F.PRESETS["r4g2"], self.frozen)
        Rset = list(st.r4info["R"])
        self.assertEqual(len(Rset), 24)
        self.model.calls.clear()
        st.__dict__.pop("_rk_cache", None)
        F.ranking(st, R.variant("r5c"), self.frozen)
        self.assertEqual(st.r4info["R"], Rset)
        self.assertEqual(self.model.calls[0][2], 24)

    def test_truncated_count_is_stored_and_summed(self):
        class Cut(FakeQwen):
            def predict(s, pairs, batch_size=8, instruction=R.DEFAULT_INSTRUCTION):
                s.last_truncated = 2
                return super().predict(pairs, batch_size, instruction)

        sc = R.R5Scorers(self.ix, Path(self.tmp.name) / "q2", self.questions, qwen=Cut(), bge=self.bge, say=lambda *x: None)
        self.frozen.r4 = sc
        F.ranking(self.states["i1"], R.variant("r5a"), self.frozen)
        F.ranking(self.states["i2"], R.variant("r5a"), self.frozen)
        pairs, trunc = sc.truncated_total()
        self.assertEqual(trunc, 4)
        self.assertGreater(pairs, 0)

    def test_replay_stand_in_serves_cached_scores_and_a_miss_is_an_error(self):
        old_packs = {}
        for i, st in self.states.items():
            st.__dict__.pop("_rk_cache", None)
            old_packs[i] = F.pack_of(st, F.PRESETS["r4g2"], self.frozen)
        f = next(Path(self.tmp.name, "bge").glob("ce_*_frozenfmt_r4.jsonl"))
        rp = R.Replay(self.ix, f, self.questions)
        self.frozen.r4 = rp
        for i, st in self.states.items():
            st.__dict__.pop("_rk_cache", None)
            self.assertEqual(F.pack_of(st, R.variant("r5a"), self.frozen), old_packs[i])
        st = self.states["i1"]
        st.__dict__.pop("_rk_cache", None)
        with self.assertRaises(SystemExit):
            F.ranking(st, R.variant("r5c"), self.frozen)      # a Qh query has no bge score

    def test_a_missing_old_score_cannot_load_a_model(self):
        with self.assertRaises(SystemExit):
            R._NoModel().predict([])


class TestCommands(Base):
    """cmd_run, cmd_time_r5 and cmd_choose_r5 end to end on a synthetic index, synthetic needs and fake models (nothing real)."""

    def files(self, d):
        d = Path(d)
        ids = ["s1", "s2", "c1", "c2"]
        ans = [{"id": i, "search_terms": self.states["i1"].exp if n % 2 == 0 else self.states["i2"].exp, "pack": []}
               for n, i in enumerate(ids)]
        items = [{"id": i, "kind": "scenario", "question": "alpha beta word5 " + "extra words " * 100 if n % 2 == 0 else "alpha word11 filler"}
                 for n, i in enumerate(ids)]
        cids = list(self.ix.byid)
        needs = []
        for n, i in enumerate(ids):
            for p in range(3):
                needs.append({"row_type": "point", "item": i, "point": p, "status": "mapped", "weight": 1 + (p == 0),
                              "chunks": [cids[(7 * n + 5 * p) % 40], cids[(3 * n + 11 * p + 1) % 40]]})
            needs.append({"row_type": "fact", "item": i, "point": None, "chunks": [cids[(n + 2) % 40]], "tags": [], "status": "mapped",
                          "weight": 1})
        w = lambda name, rows: (d / name).write_text("".join(json.dumps(r) + "\n" for r in rows))  # noqa: E731
        w("needs.jsonl", needs)
        w("answers.jsonl", ans)
        w("items.jsonl", items)
        (d / "split.json").write_text(json.dumps({"S": ["s1", "s2"], "C": ["c1", "c2"]}))
        return d

    def patch(self):
        from unittest import mock
        ps = [mock.patch.object(R, "new_index", lambda a: self.ix),
              mock.patch.object(R, "check_pin", lambda *a, **k: True),
              mock.patch.object(R, "frozen_scorer", lambda ix, cd: self.frozen_for_cmd),
              mock.patch.object(R, "bge_scorers", lambda ix, cd, say=None: self.bge),
              mock.patch.object(R, "QwenModel", lambda: self.model)]
        for p in ps:
            p.start()
            self.addCleanup(p.stop)

    def args(self, d, **kw):
        base = dict(needs=[str(d / "needs.jsonl")], answers=[str(d / "answers.jsonl")], items=[str(d / "items.jsonl")],
                    index=self.path, cache_dir=str(d / "old"), split=str(d / "split.json"), pin=R.PIN_FILE,
                    qwen_cache_dir=str(d / "qwen"))
        base.update(kw)
        return argparse.Namespace(**base)

    def test_run_writes_the_funnel_json_with_the_paired_blocks_and_choose_reads_it(self):
        d = self.files(self.tmp.name)
        self.frozen_for_cmd = self.frozen
        self.patch()
        out = Path(self.tmp.name) / "funnel.json"
        lines = []
        R.cmd_run(self.args(d, time=None, json=str(out)), say=lines.append)
        f = json.loads(out.read_text())
        self.assertEqual(set(f["r4"]["points"]), {"r4g2", "r5a", "r5b", "r5c", "r5d"})
        self.assertEqual(set(f["r5"]["paired_vs_r4g2"]), set(R.SETTINGS))
        for lab in R.SETTINGS:
            for h in ("S", "C", "facts"):
                self.assertIn("se_points", f["r5"]["paired_vs_r4g2"][lab][h])
            self.assertEqual(f["r5"]["proxy_all_recorded_chunks_in_pack"][lab]["S"]["points"], 6)
        self.assertEqual(set(f["r5"]["proxy_all_recorded_chunks_in_pack"]), {"frozen", "r4g2"} | set(R.SETTINGS))
        self.assertEqual(f["r5"]["truncated_pairs"], 0)
        self.assertEqual(f["r5"]["pin"], {n: R.settings_hash(n) for n in R.SETTINGS})
        self.assertEqual(len([l for l in lines if l.startswith("[packs ")]), 4)       # a line per question
        with self.assertRaises(SystemExit):                                          # an output file is never overwritten
            R.cmd_run(self.args(d, time=None, json=str(out)), say=lambda *x: None)
        # time-r5 on the same items, with a fake bge and the fake Qwen
        from unittest import mock
        ref = Path(self.tmp.name) / "ref_time.json"
        ref.write_text(json.dumps({"items": ["s1", "s2", "c1", "c2"]}))
        import retrieve as RT
        with mock.patch.object(RT, "reranker", lambda: self.bge_model_as_ce()):
            tl = []
            tout = Path(self.tmp.name) / "time.json"
            self.frozen_for_cmd = self.frozen
            R.cmd_time_r5(self.args(d, reference=str(ref), settings=None, out=str(tout)), say=tl.append)
        self.assertEqual(len([l for l in tl if l.startswith("[time ") and "computed, truncated" in l]), 4 * 4)
        t = json.loads(tout.read_text())
        self.assertEqual(set(t["settings"]), set(R.SETTINGS))
        self.assertEqual(len(t["settings"]["r5a"]["seconds"]), 4)
        self.assertIn("added_mb", t["memory"])
        self.assertEqual(t["truncated_pairs"], 0)
        # choose-r5 over the two files: a missing key is an error, the real file gives a verdict line first
        lines = []
        res = R.cmd_choose_r5(argparse.Namespace(funnel=str(out), time=str(tout), out=None), say=lines.append)
        self.assertTrue(lines[0].startswith("VERDICT: "))
        self.assertIn(res["verdict"], ("PASS", "FAIL"))
        broken = json.loads(out.read_text())
        del broken["r5"]["paired_vs_r4g2"]["r5b"]
        bp = Path(self.tmp.name) / "broken.json"
        bp.write_text(json.dumps(broken))
        with self.assertRaises(SystemExit):
            R.cmd_choose_r5(argparse.Namespace(funnel=str(bp), time=str(tout), out=None), say=lambda *x: None)

    def bge_model_as_ce(self):
        m = self.bge_model

        class CE:
            def predict(s, pairs, batch_size=8):
                return m.predict(pairs, batch_size)
        return CE()


class TestPaired(unittest.TestCase):
    def rows(self, per_item):
        needs, stages = [], {"A": {}, "B": {}}
        for i, (a, b, n) in per_item.items():
            for p in range(n):
                needs.append({"row_type": "point", "item": i, "point": p, "status": "mapped", "weight": 1, "chunks": ["c"]})
                stages["A"][("point", i, p)] = "in_pack" if p < a else "below_cut"
                stages["B"][("point", i, p)] = "in_pack" if p < b else "below_cut"
        return needs, stages

    def test_difference_and_se_of_the_total(self):
        needs, stages = self.rows({"x": (3, 1, 4), "y": (2, 2, 3), "z": (1, 0, 2), "w": (0, 0, 0)})
        p = R.paired_diff(needs, stages, "A", "B", {"x", "y", "z", "w"}, "points")
        self.assertEqual((p["points"], p["served"], p["ref_served"], p["diff"], p["items"]), (9, 6, 3, 3, 4))
        ds = [2, 0, 1, 0]
        mu = 0.75
        var = sum((d - mu) ** 2 for d in ds) / 3
        self.assertAlmostEqual(p["se_points"], (4 * var) ** 0.5)
        self.assertEqual(p["items_differing"], 2)

    def test_items_without_points_count_in_n(self):
        needs, stages = self.rows({"x": (1, 0, 1), "y": (0, 0, 1)})
        a = R.paired_diff(needs, stages, "A", "B", {"x", "y"}, "points")
        b = R.paired_diff(needs, stages, "A", "B", {"x", "y", "e1", "e2"}, "points")
        self.assertEqual((a["items"], b["items"]), (2, 4))

    def test_proxy_all_chunks_in_the_pack(self):
        needs = [{"row_type": "point", "item": "x", "point": 0, "status": "mapped", "weight": 1, "chunks": ["a", "b"]},
                 {"row_type": "point", "item": "x", "point": 1, "status": "mapped", "weight": 1, "chunks": ["a"]},
                 {"row_type": "point", "item": "x", "point": 2, "status": "mapped", "weight": 1, "chunks": []}]
        got = R.proxy_served(needs, {"L": {"x": ["a", "z"]}}, "L", {"x"}, "points")
        self.assertEqual(got, {"points": 2, "all_chunks_in_pack": 1})


def stat(points, served, base, lost=0, added=(8000,) * 5, items=42):
    return {"points": points, "served": served, "base": base, "lost": lost, "added_chars": list(added), "items": items}


class TestChooseRule(unittest.TestCase):
    """Synthetic tables: r4g2 serves 108 of 202 on S and 102 of 219 on C; the frozen retrieval 60 and 65."""

    def setting(self, s_gain=25, c_gain=20, s_se=5.0, c_se=4.0, lost=0, added=(8000,) * 5, facts_lost=0, secs=(3.0, 4.0, 5.0),
                s_vs_frozen=None, c_vs_frozen=None):
        S = stat(202, 60 + (s_vs_frozen if s_vs_frozen is not None else 48 + s_gain), 60, lost, added)
        C = stat(219, 65 + (c_vs_frozen if c_vs_frozen is not None else 37 + c_gain), 65)
        return ({"S": S, "C": C, "facts": {"lost": facts_lost}},
                {"S": {"diff": s_gain, "se_points": s_se, "points": 202, "ref_served": 108, "items": 42},
                 "C": {"diff": c_gain, "se_points": c_se, "points": 219, "ref_served": 102, "items": 42}},
                list(secs))

    def go(self, mem=1500.0, trunc=0, **per):
        table, paired, time_s = {}, {}, {}
        for n in R.SETTINGS:
            kw = per.get(n, {})
            table[n], paired[n], time_s[n] = self.setting(**kw)
        return R.choose_r5(table, paired, time_s, {"added_mb": mem}, trunc), table, paired

    def test_a_pass(self):
        res, table, paired = self.go()
        self.assertEqual((res["verdict"], res["reasons"]), ("PASS", []))
        text = R.choose_report(res, table, paired)
        self.assertTrue(text.startswith("VERDICT: PASS, chosen r5a"))
        self.assertIn("PASS gain over r4g2 on S: +25 of 202 points", text)
        self.assertNotIn("FAIL", text.split("\n")[0])

    def test_under_plus_20_on_s(self):
        res, table, paired = self.go(**{n: dict(s_gain=19) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertTrue(any("gain over r4g2 on S: +19" in r for r in res["reasons"]))
        self.assertTrue(R.choose_report(res, table, paired).startswith("VERDICT: FAIL, R5 ENDS WITH NO PROFILE AND NO B4"))
        self.assertEqual(self.go(**{n: dict(s_gain=20) for n in R.SETTINGS})[0]["verdict"], "PASS")

    def test_under_plus_15_on_c(self):
        res, *_ = self.go(**{n: dict(c_gain=14, c_vs_frozen=55) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertTrue(any("gain over r4g2 on C: +14" in r for r in res["reasons"]))
        self.assertEqual(self.go(**{n: dict(c_gain=15, c_vs_frozen=55) for n in R.SETTINGS})[0]["verdict"], "PASS")

    def test_gain_under_twice_its_standard_error(self):
        res, *_ = self.go(**{n: dict(s_gain=24, s_se=12.5) for n in R.SETTINGS})      # 2 x 12.5 = 25 > 24
        self.assertEqual(res["verdict"], "FAIL")
        self.assertTrue(any("paired standard errors" in r and " on S " in r for r in res["reasons"]))
        res, *_ = self.go(**{n: dict(c_gain=16, c_se=8.5) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertTrue(any("paired standard errors" in r and " on C " in r for r in res["reasons"]))
        self.assertEqual(self.go(**{n: dict(s_gain=24, s_se=12.0) for n in R.SETTINGS})[0]["verdict"], "PASS")   # exactly 2 SE

    def test_under_the_frozen_bars(self):
        # S: frozen 60, bar +25 pp of 202 = 50.5 -> 51 served-over-frozen (111 served); C: 65 + 55 = 120 served
        res, *_ = self.go(**{n: dict(s_vs_frozen=50) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertTrue(any("over the frozen retrieval on S: +50" in r for r in res["reasons"]))
        self.assertEqual(self.go(**{n: dict(s_vs_frozen=51) for n in R.SETTINGS})[0]["verdict"], "PASS")
        res, *_ = self.go(**{n: dict(c_vs_frozen=54) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertTrue(any("over the frozen retrieval on C: +54" in r for r in res["reasons"]))
        self.assertEqual(self.go(**{n: dict(c_vs_frozen=55) for n in R.SETTINGS})[0]["verdict"], "PASS")

    def test_a_lost_point(self):
        res, *_ = self.go(**{n: dict(lost=1) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertIn("no setting is eligible", res["reasons"][0])
        self.assertIn("E1", res["reasons"][0])
        res, *_ = self.go(**{n: dict(facts_lost=2) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        res, *_ = self.go(r5a=dict(lost=1, s_gain=40, s_vs_frozen=88), r5b=dict(s_gain=26), r5c=dict(s_gain=21), r5d=dict(s_gain=21))
        self.assertEqual((res["verdict"], res["chosen"]), ("PASS", "r5b"))      # a loser never wins

    def test_too_slow(self):
        res, table, paired = self.go(**{n: dict(secs=(7.0, 7.5, 8.0)) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertIn("E2", res["reasons"][0])
        self.assertTrue(R.choose_report(res, table, paired).startswith("VERDICT: FAIL"))
        res, *_ = self.go(**{n: dict(secs=[3.0] * 15 + [9.5] * 5) for n in R.SETTINGS})      # median fine, p90 over 9.0
        self.assertEqual(res["verdict"], "FAIL")
        self.assertEqual(self.go(**{n: dict(secs=[6.0] * 20) for n in R.SETTINGS})[0]["verdict"], "PASS")
        res, *_ = self.go(r5a=dict(secs=(7.0,) * 3, s_gain=40, s_vs_frozen=88))
        self.assertEqual(res["chosen"], "r5b")      # only the slow one is out

    def test_too_much_memory(self):
        res, *_ = self.go(mem=3501.0)
        self.assertEqual(res["verdict"], "FAIL")
        self.assertIn("E3", res["reasons"][0])
        self.assertEqual(self.go(mem=3500.0)[0]["verdict"], "PASS")

    def test_added_characters(self):
        res, *_ = self.go(**{n: dict(added=(12000,) * 5) for n in R.SETTINGS})
        self.assertEqual(res["verdict"], "FAIL")
        self.assertIn("E4", res["reasons"][0])

    def test_no_eligible_setting(self):
        res, table, paired = self.go(r5a=dict(lost=1), r5b=dict(secs=(9.0,) * 3), r5c=dict(added=(20000,) * 5),
                                      r5d=dict(facts_lost=1))
        self.assertEqual((res["verdict"], res["chosen"]), ("FAIL", None))
        text = R.choose_report(res, table, paired)
        self.assertTrue(text.startswith("VERDICT: FAIL, R5 ENDS WITH NO PROFILE AND NO B4: no setting is eligible"))
        for tag in ("E1", "E2", "E4"):
            self.assertIn(tag, text.split("\n")[0])
        self.assertIn("no eligible setting", text)

    def test_near_tie_decided_by_the_smaller_pack_then_seconds(self):
        res, *_ = self.go(r5a=dict(s_gain=30, s_vs_frozen=78, added=(9000,) * 5), r5b=dict(s_gain=27, s_vs_frozen=75, added=(6000,) * 5),
                           r5c=dict(s_gain=25, s_vs_frozen=73, added=(3000,) * 5))
        self.assertEqual(res["chosen"], "r5b")      # r5c is 5 points below the best: outside the band of 4
        res, *_ = self.go(r5a=dict(s_gain=30, s_vs_frozen=78, added=(6000,) * 5, secs=(4.0,) * 3),
                           r5b=dict(s_gain=28, s_vs_frozen=76, added=(6000,) * 5, secs=(2.0,) * 3),
                           r5c=dict(s_gain=20, s_vs_frozen=68), r5d=dict(s_gain=20, s_vs_frozen=68))
        self.assertEqual(res["chosen"], "r5b")      # same pack: the faster
        res, *_ = self.go(r5a=dict(s_gain=30, s_vs_frozen=78, added=(9000,) * 5), r5b=dict(s_gain=27, s_vs_frozen=75, added=(6000,) * 5))
        self.assertEqual(res["chosen"], "r5b")
        # no re-choice by C: the chosen setting fails on C although another would pass
        res, *_ = self.go(r5a=dict(s_gain=30, s_vs_frozen=78, c_gain=5, c_vs_frozen=22), r5b=dict(s_gain=20, c_gain=30))
        self.assertEqual((res["chosen"], res["verdict"]), ("r5a", "FAIL"))

    def test_the_largest_gain_on_s_wins_outside_the_band(self):
        res, *_ = self.go(r5a=dict(s_gain=40, s_vs_frozen=88, added=(10000,) * 5), r5b=dict(s_gain=30, s_vs_frozen=78, added=(1000,) * 5))
        self.assertEqual(res["chosen"], "r5a")

    def test_truncated_pairs_make_the_run_invalid(self):
        res, *_ = self.go(trunc=3)
        self.assertEqual(res["verdict"], "FAIL")
        self.assertTrue(any("truncated pairs 3" in r for r in res["reasons"]))

    def test_a_missing_input_is_an_error(self):
        table, paired, time_s = {}, {}, {}
        for n in R.SETTINGS:
            table[n], paired[n], time_s[n] = self.setting()
        mem = {"added_mb": 1000.0}
        for bad in (lambda: R.choose_r5(table, paired, {k: v for k, v in time_s.items() if k != "r5c"}, mem, 0),
                    lambda: R.choose_r5(table, {k: v for k, v in paired.items() if k != "r5b"}, time_s, mem, 0),
                    lambda: R.choose_r5({k: v for k, v in table.items() if k != "r5d"}, paired, time_s, mem, 0),
                    lambda: R.choose_r5(table, paired, time_s, {}, 0),
                    lambda: R.choose_r5(table, paired, time_s, mem, None)):
            with self.assertRaises(ValueError):
                bad()

    def test_the_cli_refuses_a_missing_file(self):
        with self.assertRaises(SystemExit):
            R.cmd_choose_r5(argparse.Namespace(funnel="/nonexistent/f.json", time="/nonexistent/t.json", out=None))

    def test_the_report_gives_sizes(self):
        res, table, paired = self.go()
        text = R.choose_report(res, table, paired)
        self.assertIn("of 202 points", text)
        self.assertIn("of 219 points", text)
        self.assertIn("42 items", text)
        self.assertIn("pp", text)


if __name__ == "__main__":
    unittest.main()
