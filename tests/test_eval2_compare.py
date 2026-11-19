import contextlib
import io
import json
import math
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import eval2_compare as cmp  # noqa: E402

NEEDS_OPENSSL = unittest.skipUnless(shutil.which("openssl"), "openssl not on PATH")
KINDS = ("alpha", "beta", "gamma")


def make_items(n=60, per_family=2):
    return [{"id": f"i{j:03d}", "tier": "A", "kind": KINDS[j % 3], "family": f"f{j // per_family:03d}",
             "domain": "d", "question": f"secret question text {j}"} for j in range(n)]


def score_rows(items, passed):
    return [{"id": it["id"], "all_pass": it["id"] in passed, "checks_passed": 1 if it["id"] in passed else 0,
             "checks_total": 1, "format_ok": True,
             "identifiers": {"known": [{"token": "x"}], "unknown": [{"token": "y"}] if it["id"] not in passed else [],
                             "not_covered": []}} for it in items]


def answer_rows(items, text=lambda i: f"answer {i}", run="r"):
    return [{"id": it["id"], "text": text(it["id"]), "tokens": 10 + n, "truncated": False, "error": None, "pack": [f"c{n % 3}"],
             "seconds": {"search_terms": 1.0, "retrieval": 2.0, "answer": 10.0 + n, "total": 13.0 + n}, "run": run}
            for n, it in enumerate(items)]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.key = self.tmp / "key"
        eval2.keygen(self.key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(self.key)})
        p.start()
        self.addCleanup(p.stop)

    def write_scores(self, name, items, passed):
        p = self.tmp / name
        eval2.write_jsonl(p, score_rows(items, passed))
        return p


class TestStats(unittest.TestCase):
    def test_wilson(self):
        lo, hi = cmp.wilson(0, 10)
        self.assertEqual(lo, 0.0)
        self.assertAlmostEqual(hi, 0.2775, places=3)
        lo, hi = cmp.wilson(50, 100)
        self.assertAlmostEqual(lo, 0.4038, places=3)
        self.assertAlmostEqual(hi, 0.5962, places=3)
        self.assertEqual(cmp.wilson(0, 0), (None, None))

    def test_mde_formula(self):
        self.assertAlmostEqual(cmp.mde(0.10, 300), 2.80 * math.sqrt(0.10 / 300))
        self.assertAlmostEqual(cmp.mde(0.15, 150) * 100, 8.85, places=1)  # the design's table: 8.9
        self.assertAlmostEqual(cmp.mde(0.25, 90) * 100, 14.76, places=1)  # 14.8
        self.assertIsNone(cmp.mde(0.1, 0))

    def test_percentile(self):
        self.assertEqual(cmp.percentile([1, 2, 3, 4, 5], 50), 3)
        self.assertAlmostEqual(cmp.percentile([0, 10], 25), 2.5)
        self.assertIsNone(cmp.percentile([], 50))

    def test_paired_counts_and_difference(self):
        ids = [f"i{j}" for j in range(20)]
        a = {i: j < 10 for j, i in enumerate(ids)}                 # passes 0..9
        b = {i: 4 <= j < 16 for j, i in enumerate(ids)}            # passes 4..15
        fam = {i: i for i in ids}
        r = cmp.paired_stats(ids, a, b, fam, 500, 1, 5)
        self.assertEqual((r["a_pass"], r["b_pass"]), (10, 12))
        self.assertEqual((r["a_to_b_loss"], r["a_to_b_gain"]), (4, 6))  # lost 0..3, gained 10..15
        self.assertEqual(r["discordant"], 10)
        self.assertAlmostEqual(r["p_d"], 0.5)
        self.assertAlmostEqual(r["agreement"], 0.5)
        self.assertAlmostEqual(r["diff"], 0.1)
        self.assertAlmostEqual(r["mde"], 2.80 * math.sqrt(0.5 / 20))
        self.assertLessEqual(r["ci90"][0], r["diff"] + 1e-9)
        self.assertGreaterEqual(r["ci90"][1], r["diff"] - 1e-9)
        self.assertLessEqual(r["ci95"][0], r["ci90"][0])
        self.assertEqual(r["resamples"], 500)

    def test_identical_runs(self):
        ids = [f"i{j}" for j in range(30)]
        a = {i: j % 3 != 0 for j, i in enumerate(ids)}
        r = cmp.paired_stats(ids, a, dict(a), {i: i[:-1] for i in ids}, 300, 1, 5)
        self.assertEqual((r["p_d"], r["diff"], r["ci90"], r["mde"]), (0.0, 0.0, (0.0, 0.0), 0.0))
        self.assertIsNone(r["n_eff"])
        self.assertEqual(r["verdict"], "pass")

    def test_bootstrap_seeded_and_clustered(self):
        ids = [f"i{j}" for j in range(40)]
        fam = {i: f"f{j // 4}" for j, i in enumerate(ids)}       # 10 families of 4
        a = {i: True for i in ids}
        b = {i: not (j < 8) for j, i in enumerate(ids)}          # the 8 losses sit in two whole families
        r1 = cmp.paired_stats(ids, a, b, fam, 400, 7, 5)
        r2 = cmp.paired_stats(ids, a, b, fam, 400, 7, 5)
        r3 = cmp.paired_stats(ids, a, b, fam, 400, 8, 5)
        self.assertEqual(r1, r2)
        cl = [(-4, 4), (-4, 4)] + [(0, 4)] * 8
        self.assertEqual(cmp.cluster_bootstrap(cl, 200, 7), cmp.cluster_bootstrap(cl, 200, 7))
        self.assertNotEqual(cmp.cluster_bootstrap(cl, 200, 7)[0], cmp.cluster_bootstrap(cl, 200, 8)[0])
        self.assertEqual(r3["resamples"], 400)
        self.assertLess(r1["n_eff"], 40)                          # clustered differences count for fewer items
        spread = {i: i for i in ids}
        b2 = {i: j % 5 != 0 for j, i in enumerate(ids)}          # same size of loss, spread over all families
        self.assertGreater(cmp.paired_stats(ids, a, b2, fam, 400, 7)["n_eff"], r1["n_eff"])
        self.assertEqual(r1["families"], 10)
        self.assertEqual(spread["i3"], "i3")

    def test_noninferiority_rule(self):
        ids = [f"i{j}" for j in range(200)]
        fam = {i: i for i in ids}
        a = {i: True for i in ids}
        lose_many = {i: j >= 30 for j, i in enumerate(ids)}       # 15 points lost
        self.assertEqual(cmp.paired_stats(ids, a, lose_many, fam, 500, 1, 5)["verdict"], "not shown")
        same = dict(a)
        self.assertTrue(cmp.paired_stats(ids, a, same, fam, 500, 1, 5)["noninferior"])
        # a small loss whose interval still crosses the margin is not shown either
        lose_few = {i: j >= 8 for j, i in enumerate(ids)}         # 4 points lost, interval reaches below -5
        r = cmp.paired_stats(ids, a, lose_few, fam, 2000, 1, 5)
        self.assertEqual(r["noninferior"], r["ci90"][0] > -0.05)
        self.assertNotIn("verdict", cmp.paired_stats(ids, a, same, fam, 50, 1))


class TestPair(Base):
    def setUp(self):
        super().setUp()
        self.items = make_items()
        self.items_path = self.tmp / "items.jsonl"
        eval2.write_jsonl(self.items_path, self.items)

    def test_pair_open_lists_differing_ids_by_kind(self):
        ids = [i["id"] for i in self.items]
        a = self.write_scores("a.scores.jsonl", self.items, set(ids[:40]))
        b = self.write_scores("b.scores.jsonl", self.items, set(ids[:38]) | {ids[45]})
        r = cmp.compare_pair(a, b, self.items_path, 200, 1, 5, sizes=(120,))
        self.assertFalse(r["hidden"])
        self.assertEqual({d["id"] for d in r["differ"]}, {ids[38], ids[39], ids[45]})
        self.assertEqual(r["differ_counts"], {"gain": 1, "loss": 2})
        self.assertEqual(set(r["by_kind"]), set(KINDS))
        self.assertTrue(all(s["descriptive"] for s in r["by_kind"].values()))
        self.assertAlmostEqual(r["mde_at_sizes"]["120"], cmp.mde(3 / 60, 120))
        out = io.StringIO()
        cmp.render_pair(r, "A", "B", out)
        t = out.getvalue()
        for needle in ("descriptive", "p_d", "Wilson 95", "90 %", "effective item count", "MDE = 2.80", "i038", "same p_d at n 120"):
            self.assertIn(needle, t)
        self.assertIn("non-inferiority of B to A, margin 5 points", t)
        self.assertIn(ids[45], t)

    def test_self_comparison_of_one_file(self):
        a = self.write_scores("a.scores.jsonl", self.items, {"i001", "i002"})
        r = cmp.compare_pair(a, a, self.items_path, 100, 1, 5)
        self.assertEqual(r["overall"]["discordant"], 0)
        self.assertEqual(r["differ"], [])

    def test_only_in_one_file_is_counted_not_paired(self):
        a = self.write_scores("a.scores.jsonl", self.items, {"i001"})
        b = self.write_scores("b.scores.jsonl", self.items[:50], {"i001"})
        r = cmp.compare_pair(a, b, self.items_path, 50, 1)
        self.assertEqual((r["overall"]["n"], r["only_in_a"], r["only_in_b"]), (50, 10, 0))

    def test_answer_identity(self):
        a, b = self.tmp / "a.jsonl", self.tmp / "b.jsonl"
        eval2.write_jsonl(a, answer_rows(self.items))
        eval2.write_jsonl(b, answer_rows(self.items, text=lambda i: "other" if i in ("i001", "i002") else f"answer {i}"))
        s = self.write_scores("s.scores.jsonl", self.items, set())
        r = cmp.compare_pair(s, s, self.items_path, 50, 1, answers_a=a, answers_b=b)
        self.assertEqual((r["answers"]["common"], r["answers"]["identical_text"]), (60, 58))
        self.assertEqual(r["answers"]["identical_pack"], 60)
        out = io.StringIO()
        cmp.render_pair(r, "A1", "A2", out)
        self.assertIn("byte-identical: 58 of 60", out.getvalue())

    @NEEDS_OPENSSL
    def test_hidden_prints_totals_only(self):
        items_h = self.tmp / "items.jsonl.enc"
        eval2.write_jsonl(items_h, self.items)
        ids = [i["id"] for i in self.items]
        a = self.tmp / "a.scores.jsonl.enc"
        b = self.tmp / "b.scores.jsonl.enc"
        eval2.write_jsonl(a, score_rows(self.items, set(ids[:40])))
        eval2.write_jsonl(b, score_rows(self.items, set(ids[:35])))
        ans_a, ans_b = self.tmp / "a.jsonl.enc", self.tmp / "b.jsonl.enc"
        eval2.write_jsonl(ans_a, answer_rows(self.items))
        eval2.write_jsonl(ans_b, answer_rows(self.items, text=lambda i: "x"))
        out, js = io.StringIO(), self.tmp / "r.json.enc"
        with contextlib.redirect_stdout(out):
            rc = cmp.main(["pair", "--a", str(a), "--b", str(b), "--items", str(items_h), "--margin", "5", "--resamples", "100",
                           "--answers-a", str(ans_a), "--answers-b", str(ans_b), "--json", str(js)])
        self.assertEqual(rc, 0)
        t = out.getvalue()
        self.assertIn("hidden", t)
        self.assertIn("no ids shown", t)
        self.assertNotIn("secret question", t)
        for i in ids:
            self.assertNotIn(i, t)
        self.assertNotIn(b"differ", js.read_bytes()[:20])       # encrypted: no clear json
        dec = json.loads(eval2.read_text(js))
        self.assertNotIn("differ", dec)
        self.assertNotIn("identical_ids", dec["answers"])
        self.assertEqual(dec["differ_counts"], {"gain": 0, "loss": 5})

    def test_cli_pair_open_and_json(self):
        a = self.write_scores("a.scores.jsonl", self.items, {"i000"})
        b = self.write_scores("b.scores.jsonl", self.items, {"i000", "i005"})
        out, js = io.StringIO(), self.tmp / "r.json"
        with contextlib.redirect_stdout(out):
            rc = cmp.main(["pair", "--a", str(a), "--b", str(b), "--items", str(self.items_path), "--resamples", "50",
                           "--json", str(js), "--a-name", "ref", "--b-name", "cand"])
        self.assertEqual(rc, 0)
        self.assertIn("cand against ref", out.getvalue())
        self.assertEqual(json.loads(js.read_text())["overall"]["n"], 60)

    def test_clear_json_for_hidden_input_is_refused(self):
        a = self.write_scores("a.scores.jsonl.enc", self.items, set())
        with contextlib.redirect_stderr(io.StringIO()):
            rc = cmp.main(["pair", "--a", str(a), "--b", str(a), "--json", str(self.tmp / "r.json")])
        self.assertEqual(rc, 1)

    def test_no_score_rows_is_an_error(self):
        p = self.tmp / "empty.jsonl"
        p.write_text("")
        with self.assertRaises(cmp.CompareError):
            cmp.load_scores(p)


class TestPickModel(unittest.TestCase):
    IDS = ["Qwen3.8-27B-4bit", "mlx-community--Qwen3.8-27B-bf16", "GLM-5.3-Flash-oQ4e", "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"]

    def test_picks(self):
        self.assertEqual(cmp.pick_model(self.IDS, "q4"), "Qwen3.8-27B-4bit")
        self.assertEqual(cmp.pick_model(self.IDS, "bf16"), "mlx-community--Qwen3.8-27B-bf16")

    def test_absent_or_ambiguous_fails(self):
        with self.assertRaises(cmp.CompareError):
            cmp.pick_model(["GLM-5.3-Flash-oQ4e"], "bf16")
        with self.assertRaises(cmp.CompareError):
            cmp.pick_model(["Qwen3.8-27B-bf16", "other--Qwen3.8-27B-bf16"], "bf16")

    def test_cli_reads_models_listing(self):
        out = io.StringIO()
        with mock.patch.object(sys, "stdin", io.StringIO(json.dumps({"data": [{"id": i} for i in self.IDS]}))), \
                contextlib.redirect_stdout(out):
            self.assertEqual(cmp.main(["pick-model", "--kind", "bf16"]), 0)
        self.assertEqual(out.getvalue().strip(), "mlx-community--Qwen3.8-27B-bf16")


class TestS6Report(Base):
    def make_run(self, name, items, passed, text=lambda i: f"answer {i}", mode="default", replay=None):
        p = self.tmp / f"{name}.jsonl"
        eval2.write_jsonl(p, answer_rows(items, text, run=f"cfg-{name}"))
        m = {"run": f"cfg-{name}", "config": name, "backend": "openai", "model_requested": "m", "model_echoed": "m", "mode": mode,
             "extra_body": {"chat_template_kwargs": {"enable_thinking": False}}, "index_sha256": "ab" * 32,
             "catalogue_sha256": "cd" * 32, "runtime_git_commit": "0" * 40, "concurrency": 1, "items_count": len(items),
             "replay": replay, "started": "t0", "ended": "t1"}
        eval2.write_manifest(p, m)
        eval2.write_jsonl(eval2.score_path(p), score_rows(items, passed))
        return p

    def test_report_has_every_section(self):
        items = make_items(60)
        ip = self.tmp / "items.jsonl"
        eval2.write_jsonl(ip, items)
        ids = [i["id"] for i in items]
        runs = {"A1": self.make_run("A1", items, set(ids[:45])),
                "A2": self.make_run("A2", items, set(ids[:44]) | {ids[50]}, text=lambda i: "diff" if i == "i001" else f"answer {i}"),
                "S": self.make_run("S", items, set(ids[:40]), mode="strict"),
                "F": self.make_run("F", items, set(ids[:46])),
                "FR": self.make_run("FR", items, set(ids[:46]), replay={"run": "cfg-A1"})}
        srv = self.tmp / "srv.json"
        srv.write_text(json.dumps({"loaded_models": ["m"]}))
        out = self.tmp / "report.md"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = cmp.main(sum((["--run", f"{n}={p}"] for n, p in runs.items()), ["s6"]) +
                          ["--items", str(ip), "--out", str(out), "--resamples", "100", "--hidden-n", "245",
                           "--server", f"A1={srv}", "--note", "an extra note"])
        self.assertEqual(rc, 0)
        t = out.read_text()
        self.assertTrue(t.split("\n")[2].startswith("result: noise A1 vs A2: p_d"))
        for needle in ("## Runs and manifests", "## All-pass rate per run", "## A2 against A1", "## A1 against F", "## A1 against FR",
                       "## S against A1", "## Smallest visible difference", "MDE at hidden n 245", "## Ceiling and floor",
                       "## Pass rate per kind", "## Identifier precision", "## Timing", "over 60 s", "## Not measured",
                       "not the decision", "byte-identical: 59 of 60", "an extra note", "loaded_models", "cfg-A1"):
            self.assertIn(needle, t, needle)
        self.assertIn("every run passes", t)
        self.assertIn("no run passes", t)

    def test_ceiling_floor(self):
        s1 = {"a": True, "b": True, "c": False, "d": False}
        s2 = {"a": True, "b": False, "c": False, "d": False}
        self.assertEqual(cmp.ceiling_floor([s1, s2]), {"n": 4, "all_runs_pass": 1, "no_run_passes": 2})

    def test_timing_and_identifier_totals(self):
        rows = answer_rows(make_items(10))
        rows[0]["seconds"]["total"] = 75.0
        rows[1]["error"] = "boom"
        t = cmp.timing(rows)
        self.assertEqual((t["answers"], t["errors"], t["over_60s"]), (10, 1, 1))
        self.assertEqual(t["total"]["max"], 75.0)
        ids = cmp.identifier_totals(score_rows(make_items(4), {"i000"}))
        self.assertEqual((ids["known"], ids["unknown"], ids["answers_with_unknown"]), (4, 3, 3))
        self.assertAlmostEqual(ids["precision"], 4 / 7)


@NEEDS_OPENSSL
class TestOutcomeTable(Base):
    def test_outcome_table_paired_and_hidden_has_no_ids(self):
        items = make_items(6)
        oc = ["pass", "pass", "wrong", "wrong", "correct_decline", "abstained"]
        ob = ["pass", "correct_decline", "wrong", "wrong_decline", "correct_decline", "abstained"]
        def rows(outs):
            return [dict(r, outcome=o) for r, o in zip(score_rows(items, set()), outs)]
        for hidden in (False, True):
            suffix = ".jsonl.enc" if hidden else ".jsonl"
            a, b, it = self.tmp / ("a" + suffix), self.tmp / ("b" + suffix), self.tmp / ("items" + suffix)
            eval2.write_jsonl(a, rows(oc))
            eval2.write_jsonl(b, rows(ob))
            eval2.write_jsonl(it, items)
            res = cmp.compare_pair(a, b, it, resamples=50)
            t = res["outcomes"]
            self.assertEqual(t["n"], 6)
            self.assertEqual(t["matrix"]["pass"]["correct_decline"], 1)
            self.assertEqual(t["matrix"]["wrong"]["wrong_decline"], 1)
            self.assertEqual(t["a"]["wrong"], 2)
            buf = io.StringIO()
            cmp.render_pair(res, "A", "B", buf)
            text = buf.getvalue()
            self.assertIn("decline outcome, paired", text)
            if hidden:
                for i in items:
                    self.assertNotIn(i["id"], text)
                    self.assertNotIn(i["question"], text)
                self.assertNotIn("i00", json.dumps(res["outcomes"]))
        # without outcome rows nothing is added
        eval2.write_jsonl(self.tmp / "c.jsonl", score_rows(items, set()))
        self.assertNotIn("outcomes", cmp.compare_pair(self.tmp / "c.jsonl", self.tmp / "c.jsonl", self.tmp / "items.jsonl", resamples=50))


if __name__ == "__main__":
    unittest.main()
