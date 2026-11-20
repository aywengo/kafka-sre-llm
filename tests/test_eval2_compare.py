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


# ---------------------------------------------------------------- the e2 subcommand (rules of runs/m4/reports/e2_plan.md)

def e2_items(n_fam=40, per=5):
    """n_fam families of per items; the last quarter of the families are slice a3 (ids a3-...), with 2 tagged; kinds by family."""
    out = []
    for f in range(n_fam):
        a3 = f >= n_fam * 3 // 4
        for j in range(per):
            it = {"id": f"{'a3-' if a3 else 'a1-'}f{f:02d}-{j}", "tier": "A", "kind": f"k{f % 3}" if not (a3 and f == n_fam - 1) else "rare",
                  "family": f"fam{f:02d}", "question": f"hidden question text {f}-{j}"}
            if a3 and f == n_fam - 1 and j < 2:
                it["shares_variant_with_published"] = True
            out.append(it)
    return out


def jrows(setname, items, ps):
    """Judge result rows for one set: one must point m1 of weight 1, present for the items whose number is in ps."""
    return [{"set": setname, "id": it["id"], "pass": 1, "valid_reply": True,
             "must": {"m1": {"verdict": "present" if ps[it["id"]] else "absent"}}, "must_not": {}, "essential_points": []}
            for it in items]


class TestE2(Base):
    def setUp(self):
        super().setUp()
        self.items = e2_items()
        self.ids = [i["id"] for i in self.items]
        self.fam_of = {i["id"]: i["family"] for i in self.items}

    def run_e2(self, scores, reading=None, hidden=False, judged=None, resamples=400, items=None):
        sfx = ".jsonl.enc" if hidden else ".jsonl"
        items = items or self.items
        ip = self.tmp / ("items" + sfx)
        eval2.write_jsonl(ip, items)
        cfg = {}
        for n, passed in scores.items():
            q = self.tmp / (n + ".scores" + sfx)
            eval2.write_jsonl(q, score_rows(items, passed))
            cfg[n] = [q]
        jd = None
        if judged:
            jr, jrub, jit = self.tmp / ("jres" + sfx), self.tmp / ("jrub" + sfx), self.tmp / ("jit" + sfx)
            rows = [r for st, ps in judged["sets"].items() for r in jrows(st, judged["items"], ps)]
            eval2.write_jsonl(jr, rows)
            eval2.write_jsonl(jrub, [{"id": it["id"], "must": [{"id": "m1", "text": "t", "weight": 1}], "must_not": []} for it in judged["items"]])
            eval2.write_jsonl(jit, judged["items"])
            jd = {"results": [jr], "rubrics": jrub, "items": jit}
        return cmp.compare_e2(cfg, [ip], jd, resamples, 1, reading)

    def spread_loss(self, k):
        """Passing set with k losses spread one per family (families have 5 items)."""
        lost = set()
        for f in range(k):
            lost.add(f"{'a3-' if f >= 30 else 'a1-'}f{f:02d}-0")
        return set(self.ids) - lost

    def clustered_loss(self, k):
        """The same number of losses, packed into whole families."""
        order = sorted(self.ids)
        return set(self.ids) - set(order[:k])

    def test_pass_in_both_readings_when_equal(self):
        r = self.run_e2({"F": set(self.ids), "Q4D": set(self.ids)})
        q = r["a_cost_of_4bit"]["Q4D_minus_F"]
        self.assertEqual((q["diff"], q["one_sided"], q["two_sided"]), (0.0, "PASS", "PASS"))

    def test_small_loss_passes_one_sided_and_fails_two_sided(self):
        # true difference -2 points (4 of 200), losses spread; the interval is wide enough to cross +5 and stay above -5 or not:
        # the known truth is a small loss, inside the margin; the two readings differ on the upper end
        ids = self.ids
        f = set(ids)
        d = set(ids) - set(ids[::50])                       # 4 losses of 200 = -2 points
        r = self.run_e2({"F": f, "Q4D": d}, resamples=2000)
        q = r["a_cost_of_4bit"]["Q4D_minus_F"]
        self.assertAlmostEqual(q["diff"], -0.02)
        self.assertEqual(q["one_sided"], "PASS")
        self.assertEqual(q["two_sided"], "PASS" if q["ci90"][1] < 0.05 else "FAIL")
        self.assertLess(q["ci90"][1], 0.05)

    def test_gain_passes_one_sided_fails_two_sided(self):
        # Q4D better by 20 points: lower end far above -5 (one-sided PASS), upper end above +5 (two-sided FAIL; noted, not a fail of rule 1(a) one-sided)
        base = set(self.ids[::5] + self.ids[1::5])           # F passes 40 %
        better = base | set(self.ids[2::5])                  # Q4D passes 60 %
        r = self.run_e2({"F": base, "Q4D": better})
        q = r["a_cost_of_4bit"]["Q4D_minus_F"]
        self.assertAlmostEqual(q["diff"], 0.2)
        self.assertEqual((q["one_sided"], q["two_sided"], q["upper_above_margin"]), ("PASS", "FAIL", True))
        t = io.StringIO()
        cmp.render_e2(r, t)
        self.assertIn("one-sided reading", t.getvalue())
        self.assertIn("two-sided reading", t.getvalue())
        self.assertIn("upper end is above +5", t.getvalue())

    def test_large_loss_fails_both(self):
        r = self.run_e2({"F": set(self.ids), "Q4D": self.spread_loss(30)})       # 30 of 200 = -15 points
        q = r["a_cost_of_4bit"]["Q4D_minus_F"]
        self.assertAlmostEqual(q["diff"], -0.15)
        self.assertEqual((q["one_sided"], q["two_sided"]), ("FAIL", "FAIL"))

    def test_clustering_by_family_widens_the_interval(self):
        full = set(self.ids)
        sp = self.run_e2({"F": full, "Q4D": self.spread_loss(20)}, resamples=1500)["a_cost_of_4bit"]["Q4D_minus_F"]
        cl = self.run_e2({"F": full, "Q4D": self.clustered_loss(20)}, resamples=1500)["a_cost_of_4bit"]["Q4D_minus_F"]
        self.assertEqual((sp["diff"], sp["discordant"]), (cl["diff"], cl["discordant"]))
        self.assertGreater(cl["ci90"][1] - cl["ci90"][0], 2 * (sp["ci90"][1] - sp["ci90"][0]))   # whole families lost: far wider
        self.assertLess(cl["n_eff"], sp["n_eff"])
        # the same input with every item its own family is narrower than the clustered one
        solo = [dict(i, family=i["id"]) for i in self.items]
        so = self.run_e2({"F": full, "Q4D": self.clustered_loss(20)}, resamples=1500, items=solo)["a_cost_of_4bit"]["Q4D_minus_F"]
        self.assertLess(so["ci90"][1] - so["ci90"][0], cl["ci90"][1] - cl["ci90"][0])

    def test_mde_from_observed_discordance(self):
        r = self.run_e2({"F": set(self.ids), "Q4D": self.spread_loss(20)})["a_cost_of_4bit"]["Q4D_minus_F"]
        self.assertAlmostEqual(r["mde"], cmp.mde(20 / 200, 200))

    def test_fr_beside_and_missing_configs(self):
        r = self.run_e2({"F": set(self.ids), "Q4D": set(self.ids)})
        self.assertIsNone(r["a_cost_of_4bit"]["Q4D_minus_FR"])
        r = self.run_e2({"F": set(self.ids), "Q4D": set(self.ids), "FR": self.spread_loss(10)})
        self.assertAlmostEqual(r["a_cost_of_4bit"]["Q4D_minus_FR"]["diff"], 10 / 200)

    def test_mode_waits_for_the_reading(self):
        scores = {"Q4D": set(self.ids), "Q4S": set(self.ids)}
        r = self.run_e2(scores)
        self.assertEqual(r["b_mode"]["verdict"], "waiting for the reading")
        self.assertEqual(r["b_mode"]["criteria"]["mechanical"], "met")
        out = io.StringIO()
        cmp.render_e2(r, out)
        self.assertIn("waiting for the reading", out.getvalue())
        self.assertNotIn("STRICT meets", out.getvalue())
        # an incomplete reading is still waiting and names what is missing
        r = self.run_e2(scores, {"default_rate": 3.0, "strict_rate": 1.0})
        self.assertIn("missing", r["b_mode"]["reading_text"])
        self.assertEqual(r["b_mode"]["verdict"], "waiting for the reading")
        # readers differing by more than 30 %: no verdict
        full = {"default_rate": 3.0, "strict_rate": 1.0, "default_harmful": 0, "strict_harmful": 0, "reader_disagreement": 0.5, "readers_usable": False}
        self.assertEqual(self.run_e2(scores, full)["b_mode"]["verdict"], "waiting for the reading")

    def judged_case(self, strict_lower):
        its = [{"id": f"j{n:02d}", "tier": "B", "kind": "diag" if n < 20 else "arch", "family": f"j{n:02d}"} for n in range(24)]
        d = {it["id"]: True for it in its}
        s = {it["id"]: (n >= strict_lower) for n, it in enumerate(its)}
        p0 = {it["id"]: n < 8 for n, it in enumerate(its)}
        return {"items": its, "sets": {"Q4D": d, "Q4S": s, "P0": p0}}

    def test_mode_all_three_met_and_each_failure(self):
        same = {"Q4D": set(self.ids), "Q4S": set(self.ids)}
        ok = {"default_rate": 3.0, "strict_rate": 1.5, "default_harmful": 1, "strict_harmful": 1, "reader_disagreement": 1.0, "readers_usable": True}
        r = self.run_e2(same, ok, judged=self.judged_case(0))
        self.assertEqual(r["b_mode"]["criteria"], {"mechanical": "met", "judged_P": "met", "reading": "met"})
        self.assertEqual(r["b_mode"]["verdict"], "STRICT meets all three criteria")
        # reading: improvement not larger than the readers' disagreement
        r = self.run_e2(same, dict(ok, reader_disagreement=2.0), judged=self.judged_case(0))
        self.assertEqual(r["b_mode"]["verdict"], "DEFAULT stays the reference mode (not met: reading)")
        # reading: harmful higher
        r = self.run_e2(same, dict(ok, strict_harmful=2), judged=self.judged_case(0))
        self.assertEqual(r["b_mode"]["criteria"]["reading"], "not met")
        # judged coverage: 12 of 24 scenarios lose P 1.0: mean -0.5, lower end below -0.10
        r = self.run_e2(same, ok, judged=self.judged_case(12))
        self.assertAlmostEqual(r["b_mode"]["judged_P"]["mean"], -0.5)
        self.assertEqual(r["b_mode"]["criteria"]["judged_P"], "not met")
        self.assertIn("judged_P", r["b_mode"]["verdict"])
        # mechanical: strict loses 15 points
        r = self.run_e2({"Q4D": set(self.ids), "Q4S": self.spread_loss(30)}, ok, judged=self.judged_case(0))
        self.assertEqual(r["b_mode"]["criteria"]["mechanical"], "not met")
        self.assertIn("mechanical", r["b_mode"]["verdict"])

    def test_figures_sizes_kinds_and_a3_with_and_without_tagged(self):
        r = self.run_e2({"Q4D": set(self.ids)})
        f = r["c_figures"]["mechanical"]["Q4D"]
        self.assertEqual((f["mechanical"]["n"], f["a3"]["n"], f["tier_A_without_a3"]["n"]), (200, 50, 150))
        self.assertEqual((f["a3_tagged"], f["a3_without_tagged"]["n"]), (2, 48))
        self.assertEqual(f["mechanical"]["rate"], 1.0)
        self.assertEqual(set(f["kinds"]), {"k0", "k1", "k2", "rare"})   # "rare" has exactly 5 items: reported
        # a kind with fewer than 5 items is left out
        small = [dict(i, kind="tiny") if n < 4 else i for n, i in enumerate(self.items)]
        f = self.run_e2({"Q4D": set(self.ids)}, items=small)["c_figures"]["mechanical"]["Q4D"]
        self.assertNotIn("tiny", f["kinds"])

    def test_closed_book_against_retrieval(self):
        r = self.run_e2({"P0": set(self.ids[:100]), "Q4D": set(self.ids[:150])})
        cb = r["c_closed_book_vs_retrieval"]
        self.assertAlmostEqual(cb["mechanical"]["diff"], 0.25)
        self.assertTrue(cb["descriptive"])
        r = self.run_e2({"Q4D": set(self.ids)}, judged=self.judged_case(0))
        self.assertAlmostEqual(r["c_closed_book_vs_retrieval"]["judged_P"]["mean"], 16 / 24)

    def test_deterministic_for_a_fixed_seed(self):
        sc = {"F": set(self.ids), "Q4D": self.spread_loss(7), "Q4S": self.spread_loss(9)}
        r1, r2 = self.run_e2(sc), self.run_e2(sc)
        self.assertEqual(json.dumps(r1, sort_keys=True), json.dumps(r2, sort_keys=True))
        a, b = io.StringIO(), io.StringIO()
        cmp.render_e2(r1, a)
        cmp.render_e2(r2, b)
        self.assertEqual(a.getvalue(), b.getvalue())

    @NEEDS_OPENSSL
    def test_hidden_input_prints_no_id_and_writes_enc_only(self):
        sc = {"F": set(self.ids), "Q4D": self.spread_loss(7), "Q4S": self.spread_loss(9), "P0": set(self.ids[:100])}
        jc = self.judged_case(3)
        ok = {"default_rate": 3.0, "strict_rate": 1.5, "default_harmful": 1, "strict_harmful": 1, "reader_disagreement": 1.0}
        r = self.run_e2(sc, ok, hidden=True, judged=jc)
        self.assertTrue(r["hidden"])
        out = io.StringIO()
        cmp.render_e2(r, out)
        text = out.getvalue() + json.dumps(r)
        self.assertIn("hidden: totals only", out.getvalue())
        for it in self.items + jc["items"]:
            self.assertNotIn(it["id"], text)
            self.assertNotIn(it.get("question", "\0"), text)
        # through the command line: the json must be .enc, and nothing but .enc files appears
        buf, err = io.StringIO(), io.StringIO()
        args = ["e2", "--items", str(self.tmp / "items.jsonl.enc"), "--resamples", "100"]
        for n in sc:
            args += ["--config", f"{n}={self.tmp / (n + '.scores.jsonl.enc')}"]
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
            self.assertEqual(cmp.main(args + ["--json", str(self.tmp / "o.json")]), 1)
        self.assertIn("a hidden comparison writes a .enc json", err.getvalue())
        self.assertFalse((self.tmp / "o.json").exists())
        with contextlib.redirect_stdout(buf):
            self.assertEqual(cmp.main(args + ["--json", str(self.tmp / "o.json.enc")]), 0)
        for it in self.items:
            self.assertNotIn(it["id"], buf.getvalue())
        self.assertNotIn(b"a1-f00", (self.tmp / "o.json.enc").read_bytes())
        self.assertNotIn("a1-f00", eval2.decrypt_bytes((self.tmp / "o.json.enc").read_bytes()).decode())   # not even inside

    @NEEDS_OPENSSL
    def test_open_and_hidden_are_never_mixed(self):
        ip = self.tmp / "items.jsonl.enc"
        eval2.write_jsonl(ip, self.items)
        q = self.tmp / "Q4D.scores.jsonl"
        eval2.write_jsonl(q, score_rows(self.items, set(self.ids)))
        with self.assertRaises(cmp.CompareError):
            cmp.compare_e2({"Q4D": [q]}, [ip], None, 50, 1)

    def test_cli_open_output_and_merged_files(self):
        ip = self.tmp / "items.jsonl"
        eval2.write_jsonl(ip, self.items)
        a1 = [i for i in self.items if i["id"].startswith("a1-")]
        a3 = [i for i in self.items if i["id"].startswith("a3-")]
        args = ["e2", "--items", str(ip), "--resamples", "100"]
        for n in ("F", "Q4D"):
            for part, nm in ((a1, "a1"), (a3, "a3")):
                q = self.tmp / f"{n}_{nm}.scores.jsonl"
                eval2.write_jsonl(q, score_rows(part, {i["id"] for i in part}))
                args += ["--config", f"{n}={q}"]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(cmp.main(args), 0)
        t = buf.getvalue()
        for needle in ("(a) the cost of 4-bit", "one-sided reading", "two-sided reading", "(b) the mode", "(c) starting figures", "a3_without_tagged"):
            self.assertIn(needle, t)
        self.assertRegex(t, r"Q4D\s+mechanical\s+200\s+200")


if __name__ == "__main__":
    unittest.main()
