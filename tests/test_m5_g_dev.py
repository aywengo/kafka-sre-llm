"""M5 G1b and GD-chain: the grounded modes in eval2.py and the serve refusal, the rule script (scripts/m5_g_rule.py) on synthetic inputs, the start
check of the chain's manifest and markers, the ids of the mechanical sample, and one dry run of scripts/m5_g_dev.sh (stub model, stub judge).
Offline, CPU only; no server, no model."""
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import m5_g_rule as R  # noqa: E402

TIER_A, TIER_A3 = ROOT / "eval/m4/open/tier_a.jsonl", ROOT / "eval/m4/open/tier_a3.jsonl"
IDS_FILE = ROOT / "eval/m4/g_dev_ids.json"
B1_DEFAULT = ROOT / "work/m4/b1/ans_default.jsonl"
HAVE_WORK = all(p.exists() for p in (B1_DEFAULT, ROOT / "work/m4/b1/judge_results.jsonl", ROOT / "work/m4/pilot2/ans_default.jsonl", ROOT / "work/m4/s6/ans_A1.jsonl",
                                     ROOT / "work/m4/e2/ans_O_Q4D_a3.jsonl", ROOT / "work/v3/chunks_s1.jsonl", TIER_A, TIER_A3))


def run_rule(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = R.main(argv)
        except SystemExit as e:
            code = e.code
    return code, out.getvalue(), err.getvalue()


# ---------------------------------------------------------------- Part 1: eval2.py and serve
class GroundedModes(unittest.TestCase):
    def test_eval2_mode_choices_and_profiles(self):
        import eval2
        from kafka_sre_rag import profiles
        p = eval2.build_parser()
        for mode in ("default", "strict", "closed", "grounded-g1", "grounded-g2"):
            a = p.parse_args(["answer", "--items", "x", "--config", "c", "--mode", mode])
            self.assertEqual(a.mode, mode)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            p.parse_args(["answer", "--items", "x", "--config", "c", "--mode", "grounded-g3"])
        for prof in profiles.PROFILES:       # the choices come from the profiles module, not from a second list
            self.assertEqual(p.parse_args(["answer", "--items", "x", "--config", "c", "--profile", prof]).profile, prof)
        self.assertIn("r4", profiles.PROFILES)

    @unittest.skipUnless(B1_DEFAULT.exists() and (ROOT / "work/v3/chunks_s1.jsonl").exists(), "work/ files absent")
    def test_stub_replay_writes_the_mode_in_the_manifest(self):
        import eval2
        for mode in ("grounded-g1", "grounded-g2"):
            with tempfile.TemporaryDirectory() as t:
                items = Path(t) / "items.jsonl"
                R.main(["harness-items", "--in", str(ROOT / "eval/m4/b1/items.jsonl"), "--out", str(items), "--domain", "b1"])
                out = Path(t) / "ans.jsonl"
                code = eval2.main(["answer", "--items", str(items), "--out", str(out), "--config", "t", "--backend", "stub", "--mode", mode,
                                   "--replay-packs", str(B1_DEFAULT), "--limit", "2", "--no-rerank"])
                self.assertEqual(code, 0)
                rows = [json.loads(l) for l in open(out) if l.strip()]
                self.assertEqual(len(rows), 2)
                self.assertTrue(all(r["error"] is None and r["pack"] for r in rows))
                man = json.load(open(str(out) + ".manifest.json"))
                self.assertEqual(man["mode"], mode)
                self.assertTrue(any(mode in x for x in man["unmeasured_reasons"]))


class ServeRefusal(unittest.TestCase):
    def test_serve_refuses_a_grounded_mode(self):
        from kafka_sre_rag import __main__ as M
        for mode in ("grounded-g1", "grounded-g2"):
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = M.main(["serve", "--mode", mode, "--dry-run", "--port", "0"])
            self.assertEqual(code, 2)
            self.assertIn("serve", err.getvalue())
            self.assertIn(mode, err.getvalue())
            self.assertEqual(out.getvalue(), "")       # nothing was loaded or started


# ---------------------------------------------------------------- the rule on synthetic inputs
def variant(P=0.60, S=4.0, cut=2, med=1200, ident=3.0, d=None, ff=3, ffr=3, wd=4, cov=80, dec=20, wdw=None, covw=None, decw=None):
    """wd, cov, dec: unweighted over the sample (printed); wdw, covw, decw: weighted like (e) (they decide (f)); default: the same numbers."""
    return {"P": P, "answers_judged": 54, "supp_miss_per10": S, "supp_miss_answers": 30, "cut": cut, "answers": 54, "median_tokens": med,
            "idents_per10": ident,
            "mech": {"d_all_pass": d if d is not None else {"M1": 0, "M2": 0, "M3": 0}, "format_failures": ff, "ref_format_failures": ffr,
                     "wrong_declines": wd, "covered": cov, "declines": dec,
                     "wd_weighted": wd if wdw is None else wdw, "covered_weighted": cov if covw is None else covw,
                     "declines_weighted": dec if decw is None else decw}}


def measures(g1=None, g2=None):
    return {"schema": 1, "mechanical": "sample", "sizes": {"J": 54, "J_b1": 30, "J_p2": 24, "mech": {"M1": 56, "M2": 40, "M3": 20}},
            "pop": {"M1": 56, "M2": 195, "M3": 88},
            "variants": {"default": {"P": 0.62, "supp_miss_per10": 4.0, "idents_per10": 3.0, "cut": 19, "median_tokens": 2048},
                         "g1": g1 if g1 is not None else variant(), "g2": g2 if g2 is not None else variant()}}


class Rule(unittest.TestCase):
    def verdict(self, m):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "m.json"
            p.write_text(json.dumps(m))
            return run_rule(["rule", "--measures", str(p)])

    def test_g1_passes_all(self):
        code, out, err = self.verdict(measures())
        self.assertEqual(code, 0, err)
        self.assertIn("CHOICE: G1", out)
        self.assertEqual(out.count("FAIL"), 0)
        self.assertEqual(out.count("PASS"), 14)         # seven conditions, two variants

    def test_g1_fails_one_condition_and_g2_passes(self):
        code, out, err = self.verdict(measures(g1=variant(med=1500)))
        self.assertEqual(code, 0, err)
        self.assertIn("CHOICE: G2", out)
        self.assertIn("FAIL  (d) median completion tokens: 1500", out)
        self.assertEqual(out.count("FAIL"), 1)

    def test_both_fail(self):
        code, out, err = self.verdict(measures(g1=variant(cut=6), g2=variant(P=0.50)))
        self.assertEqual(code, 0, err)
        self.assertIn("CHOICE: neither", out)
        self.assertIn("FAIL  (c)", out)
        self.assertIn("FAIL  (a)", out)

    def test_each_threshold_edge(self):
        cases = {"a": dict(P=0.569), "b": dict(S=5.5), "c": dict(cut=6), "d": dict(med=1401), "g": dict(ident=3.5),
                 "e1": dict(d={"M1": 0, "M2": -3, "M3": 0}), "e2": dict(ff=6, ffr=3), "f": dict(wd=9, cov=80)}
        for name, kw in cases.items():
            code, out, err = self.verdict(measures(g1=variant(**kw)))
            self.assertEqual(code, 0, err)
            self.assertIn("FAIL", out, name)
            self.assertIn("CHOICE: G2", out, name)
        for kw in (dict(P=0.57 + 0.0001 + 0.0), dict(S=5.0), dict(cut=5), dict(med=1400), dict(ident=3.0), dict(ff=5, ffr=3), dict(wd=8, cov=80),
                   dict(d={"M1": -3, "M2": 0, "M3": 0})):
            code, out, err = self.verdict(measures(g1=variant(**kw)))
            self.assertIn("CHOICE: G1", out, kw)

    def test_projection_scales_the_samples(self):
        # M1 -1, M2 -1, M3 -1: (-1 - 4.875 - 4.4) / 339 * 100 = -3.03 points: fails (e)
        code, out, err = self.verdict(measures(g1=variant(d={"M1": -1, "M2": -1, "M3": -1})))
        self.assertIn("projected mechanical change -3.03 points", out)
        self.assertIn("CHOICE: G2", out)

    def test_wrong_declines_are_counted_against_the_covered_items_and_the_share_of_declines_is_printed(self):
        code, out, err = self.verdict(measures(g1=variant(wd=8, cov=100, dec=10)))
        self.assertIn("8.0 of 100.0 items whose covering chunk is in the pack = 8.0 %", out)
        self.assertIn("8.0 of 10.0 = 80.0 % weighted, 8 of 10 = 80.0 % unweighted", out)
        self.assertIn("CHOICE: G1", out)          # the first decides

    def test_f_the_weighted_share_decides_when_the_unweighted_share_fails(self):
        # the sample over-represents declines: 10 of 46 = 21.7 % unweighted, but 10 of 146.8 = 6.8 % weighted
        code, out, err = self.verdict(measures(g1=variant(wd=10, cov=46, dec=30, wdw=10.0, covw=146.8, decw=40.0)))
        self.assertEqual(code, 0, err)
        self.assertIn("CHOICE: G1", out)
        self.assertIn("6.8 %", out)
        self.assertIn("unweighted over the sample 10 of 46 = 21.7 %", out)
        self.assertEqual(out.count("FAIL"), 0)

    def test_f_the_weighted_share_decides_when_the_unweighted_share_passes(self):
        code, out, err = self.verdict(measures(g1=variant(wd=4, cov=46, dec=30, wdw=20.0, covw=146.8, decw=60.0)))
        self.assertEqual(code, 0, err)
        self.assertIn("CHOICE: G2", out)
        self.assertIn("FAIL  (f)", out)
        self.assertIn("unweighted over the sample 4 of 46 = 8.7 %", out)      # printed, decides nothing

    def test_f_the_share_of_declines_decides_nothing(self):
        code, out, err = self.verdict(measures(g1=variant(wdw=5.0, covw=100.0, decw=5.0, wd=5, cov=100, dec=5)))
        self.assertIn("CHOICE: G1", out)
        self.assertIn("= 100.0 % weighted", out)

    def test_f_weighted_inputs_are_required(self):
        for k in ("wd_weighted", "covered_weighted", "declines_weighted"):
            m = measures()
            del m["variants"]["g1"]["mech"][k]
            code, out, err = self.verdict(m)
            self.assertEqual(code, 2, k)
            self.assertIn("a missing input is not a pass", err)
        m = measures(g1=variant(covw=0.0))
        self.assertEqual(self.verdict(m)[0], 2)

    def test_f_estimate_uses_the_weights_of_the_projection(self):
        ids = {"M1": [f"a{i}" for i in range(56)], "M2": [f"b{i}" for i in range(40)]}
        w = R.f_weights(ids, "sample")
        self.assertEqual(w, {"M1": 1.0, "M2": 4.875})
        self.assertEqual(R.f_weights(ids, "all"), {"all": 1.0})
        est = R.f_estimate({"M1": {"wrong_declines": 9, "covered": 30, "declines": 40}, "M2": {"wrong_declines": 1, "covered": 16, "declines": 2}}, w)
        self.assertAlmostEqual(est["wd_weighted"], 9 + 4.875)
        self.assertAlmostEqual(est["covered_weighted"], 30 + 4.875 * 16)
        self.assertAlmostEqual(est["declines_weighted"], 40 + 4.875 * 2)
        self.assertEqual((est["wd"], est["covered"], est["declines"]), (10, 46, 42))
        with self.assertRaises(R.RuleError):
            R.f_estimate({"M1": {"wrong_declines": 0, "covered": 1, "declines": 0}}, w)

    def test_the_rule_text_says_why_the_share_is_weighted_and_the_bias_sentence_exists(self):
        code, o, e = run_rule(["rule-text"])
        self.assertIn("weighted because the sample over-represents declines", o)
        self.assertIn("(wd_M1 + 4.875 wd_M2) / (cov_M1 + 4.875 cov_M2)", o)
        code, o, e = run_rule(["f-note"])
        self.assertEqual(code, 0)
        self.assertIn("without an ANSWER line", o)
        self.assertIn("counts against the grounded variants", o)
        self.assertIn("checks the answers counted as wrong declines", o)

    def test_all_mode(self):
        m = measures()
        m["mechanical"], m["sizes"]["mech"], m["pop"] = "all", {"all": 339}, {"all": 339}
        for v in ("g1", "g2"):
            m["variants"][v]["mech"]["d_all_pass"] = {"all": -9}
        code, out, err = self.verdict(m)
        self.assertEqual(code, 0, err)
        self.assertIn("CHOICE: G1", out)                   # -9 / 339 * 100 = -2.65 points
        m["variants"]["g1"]["mech"]["d_all_pass"] = {"all": -11}    # -3.24 points
        code, out, err = self.verdict(m)
        self.assertIn("projected mechanical change -3.24 points", out)
        self.assertIn("CHOICE: G2", out)

    def test_a_missing_input_is_an_error_not_a_pass(self):
        base = measures()
        for path in (("variants", "g1", "P"), ("variants", "g1", "mech", "wrong_declines"), ("variants", "default", "idents_per10"),
                     ("variants", "g2", "cut"), ("variants", "g2", "mech", "d_all_pass"), ("sizes", "J")):
            m = json.loads(json.dumps(base))
            cur = m
            for k in path[:-1]:
                cur = cur[k]
            del cur[path[-1]]
            code, out, err = self.verdict(m)
            self.assertEqual(code, 2, path)
            self.assertNotIn("CHOICE", out)
            self.assertIn("a missing input is not a pass", err)
        for bad in (None, "x", float("nan")):
            m = json.loads(json.dumps(base))
            m["variants"]["g1"]["P"] = bad
            code, out, err = self.verdict(m)
            self.assertEqual(code, 2, bad)
        code, out, err = run_rule(["rule", "--measures", "/nonexistent/measures.json"])
        self.assertEqual(code, 2)
        self.assertNotIn("CHOICE", out)

    def test_a_missing_variant_is_an_error_even_when_the_other_passes(self):
        m = measures()
        del m["variants"]["g2"]
        code, out, err = self.verdict(m)
        self.assertEqual(code, 2)


# ---------------------------------------------------------------- the ids
class Ids(unittest.TestCase):
    def synthetic(self, t):
        a = [{"id": i, "kind": "name_setting"} for i in R.M1_IDS] + [{"id": f"x-{n:03d}", "kind": "name_setting"} for n in range(195)]
        a3 = [{"id": f"y-{n:03d}", "kind": "client_code"} for n in range(88)]
        R.wr(Path(t) / "a.jsonl", a)
        R.wr(Path(t) / "a3.jsonl", a3)
        return Path(t) / "a.jsonl", Path(t) / "a3.jsonl"

    def test_draw_is_deterministic_and_has_the_sizes(self):
        with tempfile.TemporaryDirectory() as t:
            a, a3 = self.synthetic(t)
            d1, d2 = R.draw(a, a3), R.draw(a, a3)
            self.assertEqual(d1, d2)
            self.assertEqual((len(d1["M1"]), len(d1["M2"]), len(d1["M3"])), (56, 40, 20))
            self.assertFalse(set(d1["M1"]) & set(d1["M2"]))
            self.assertEqual(d1["M2"], sorted(d1["M2"]))
            out = Path(t) / "ids.json"
            self.assertEqual(run_rule(["draw-ids", "--tier-a", str(a), "--tier-a3", str(a3), "--out", str(out)])[0], 0)
            self.assertEqual(run_rule(["draw-ids", "--tier-a", str(a), "--tier-a3", str(a3), "--out", str(out)])[0], 2)   # not overwritten
            self.assertEqual(run_rule(["draw-ids", "--tier-a", str(a), "--tier-a3", str(a3), "--check", str(out)])[0], 0)
            d = json.load(open(out))
            d["M2"][0] = "x-999"
            out.write_text(json.dumps(d))
            self.assertEqual(run_rule(["draw-ids", "--tier-a", str(a), "--tier-a3", str(a3), "--check", str(out)])[0], 1)

    def test_m1_ids_are_unique_and_56(self):
        self.assertEqual(len(R.M1_IDS), 56)
        self.assertEqual(len(set(R.M1_IDS)), 56)

    @unittest.skipUnless(TIER_A.exists() and TIER_A3.exists(), "open item files absent")
    def test_real_ids_file_reproduces_from_its_rule_with_the_design_counts(self):
        d = R.draw(TIER_A, TIER_A3)
        self.assertEqual((len(d["M1"]), len(d["M2"]), len(d["M3"])), (56, 40, 20))
        self.assertEqual(d["counts"]["total"], 116)
        self.assertEqual(R.kind_differences(d), [], "the drawn kinds differ from the design's list (report it; do not adjust the rule)")
        self.assertEqual(d["kinds"]["M2"], R.M2_KINDS)
        self.assertEqual(d["kinds"]["M3"], R.M3_KINDS)
        if IDS_FILE.exists():
            stored = json.load(open(IDS_FILE))
            for k in ("M1", "M2", "M3"):
                self.assertEqual(stored[k], d[k], k)
            self.assertIn("random.Random(20261122)", stored["rule"])


# ---------------------------------------------------------------- the start check
class StartCheck(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.t = Path(self._t.name)
        self.addCleanup(self._t.cleanup)
        self.w = self.t / "w"
        self.w.mkdir()
        self.f1, self.f2 = self.t / "items.jsonl", self.t / "answers.jsonl"
        self.f1.write_text('{"id": "a"}\n')
        self.f2.write_text('{"id": "a", "text": "x"}\n')
        self.frozen = self.t / "frozen.json"
        self.frozen.write_text(json.dumps({"prompts_sha256": {"INSTRUCTION": "aa", "GROUNDED_G1": "bb"}}))

    def args(self, cmd, extra=()):
        return [cmd, "--manifest", str(self.w / "MANIFEST.json"), "--work", str(self.w), "--pin", str(self.f1), "--pin", str(self.f2),
                "--frozen", str(self.frozen), "--mechanical", "sample", "--model", "m", "--dry", *extra]

    def test_fresh_start_then_resume(self):
        self.assertEqual(run_rule(self.args("start-check"))[0], 0)
        self.assertEqual(run_rule(self.args("manifest"))[0], 0)
        self.assertEqual(run_rule(self.args("start-check"))[0], 0)
        out = self.w / "out.txt"
        out.write_text("x")
        self.assertEqual(run_rule(["mark", "--work", str(self.w), "--stage", "answers", "--out", str(out)])[0], 0)
        code, o, e = run_rule(self.args("start-check"))
        self.assertEqual(code, 0)
        self.assertIn("1 stage(s) done: answers", o)

    def test_a_changed_pinned_file_is_refused_with_exit_code_2(self):
        run_rule(self.args("manifest"))
        self.f2.write_text('{"id": "a", "text": "changed"}\n')
        code, o, e = run_rule(self.args("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("pins." + str(self.f2), e)
        self.assertTrue((self.w / "MANIFEST.json").exists())        # nothing moved or deleted

    def test_a_changed_prompt_hash_the_options_and_the_model_are_refused(self):
        run_rule(self.args("manifest"))
        self.frozen.write_text(json.dumps({"prompts_sha256": {"INSTRUCTION": "aa", "GROUNDED_G1": "cc"}}))
        self.assertEqual(run_rule(self.args("start-check"))[0], 2)
        self.frozen.write_text(json.dumps({"prompts_sha256": {"INSTRUCTION": "aa", "GROUNDED_G1": "bb"}}))
        self.assertEqual(run_rule(self.args("start-check"))[0], 0)
        a = self.args("start-check")
        a[a.index("sample")] = "all"
        self.assertEqual(run_rule(a)[0], 2)
        a = self.args("start-check")
        a[a.index("m")] = "other-model"
        self.assertEqual(run_rule(a)[0], 2)

    def test_the_judge_arguments_are_compared_and_the_info_is_not(self):
        run_rule(self.args("manifest", ["--judge-arg", "setting=s1", "--judge-arg", 'extra_body={"a": 1}', "--info", "git_head=abc"]))
        m = json.load(open(self.w / "MANIFEST.json"))
        self.assertEqual(m["judge_args"], {"setting": "s1", "extra_body": '{"a": 1}'})
        self.assertEqual(m["info"], {"git_head": "abc"})
        same = ["--judge-arg", "setting=s1", "--judge-arg", 'extra_body={"a": 1}']
        self.assertEqual(run_rule(self.args("start-check", same + ["--info", "git_head=other"]))[0], 0)       # another head: information only
        code, o, e = run_rule(self.args("start-check", ["--judge-arg", "setting=s2", "--judge-arg", 'extra_body={"a": 1}']))
        self.assertEqual(code, 2)
        self.assertIn("judge_args.setting", e)
        self.assertEqual(run_rule(self.args("start-check", same[:2]))[0], 2)                                  # a missing argument differs too
        self.assertEqual(run_rule(self.args("manifest", ["--judge-arg", "novalue"]))[0], 2)

    def test_a_missing_pinned_file_is_refused(self):
        run_rule(self.args("manifest"))
        self.f1.unlink()
        self.assertEqual(run_rule(self.args("start-check"))[0], 2)

    def test_a_marker_without_a_manifest_is_refused(self):
        (self.w / "stage_answers.done").write_text("{}")
        code, o, e = run_rule(self.args("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("no manifest", e)

    def test_a_stale_marker_is_refused(self):
        run_rule(self.args("manifest"))
        (self.w / "stage_answers.done").write_text(json.dumps({"stage": "answers", "manifest_sha256": "0" * 64, "outputs": {}}))
        code, o, e = run_rule(self.args("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("stale", e)

    def test_a_marker_whose_output_changed_or_vanished_is_refused(self):
        run_rule(self.args("manifest"))
        out = self.w / "out.txt"
        out.write_text("x")
        run_rule(["mark", "--work", str(self.w), "--stage", "answers", "--out", str(out)])
        out.write_text("y")
        self.assertEqual(run_rule(self.args("start-check"))[0], 2)
        out.unlink()
        code, o, e = run_rule(self.args("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("missing", e)

    def test_dry_and_real_manifests_do_not_mix(self):
        run_rule(self.args("manifest"))
        a = [x for x in self.args("start-check") if x != "--dry"]
        self.assertEqual(run_rule(a)[0], 2)


class SmallChecks(unittest.TestCase):
    def test_determinism_is_a_measurement_and_stops_nothing(self):
        with tempfile.TemporaryDirectory() as t:
            st, new = Path(t) / "s.jsonl", Path(t) / "n.jsonl"
            rows = [{"id": f"i{n}", "text": f"text {n}", "truncated": False} for n in range(4)]
            R.wr(st, rows)
            R.wr(new, rows[:3])
            code, o, e = run_rule(["determinism", "--stored", str(st), "--new", str(new), "--n", "3"])
            self.assertEqual(code, 0)
            self.assertIn("3 of 3 fresh default answers equal the stored ones", o)
            bad = [dict(r) for r in rows[:3]]
            bad[1]["text"] = "text 1 plus more"
            R.wr(new, bad)
            code, o, e = run_rule(["determinism", "--stored", str(st), "--new", str(new), "--n", "3"])
            self.assertEqual(code, 0)          # a difference is printed, the chain goes on
            self.assertIn("i1: DIFFERENT (stored 6 characters, fresh 16; first difference at character 6)", o)
            self.assertIn("2 of 3 fresh default answers equal", o)
            self.assertEqual(run_rule(["determinism", "--stored", str(st), "--new", str(new), "--n", "5"])[0], 1)   # too few rows is an error

    def test_repeat_within_the_run_prints_the_fixed_sentence_on_a_difference_and_stops_nothing(self):
        with tempfile.TemporaryDirectory() as t:
            a, b = Path(t) / "a.jsonl", Path(t) / "b.jsonl"
            rows = [{"id": f"i{n}", "text": f"text {n}" * 5, "truncated": False} for n in range(3)]
            R.wr(a, rows)
            R.wr(b, rows)
            code, o, e = run_rule(["repeat", "--first", str(a), "--again", str(b), "--n", "3"])
            self.assertEqual(code, 0)
            self.assertIn("3 of 3 answers equal this run's first answers", o)
            self.assertNotIn(R.REPEAT_SENTENCE, o)
            alt = [dict(r) for r in rows]
            alt[2]["text"] = alt[2]["text"][:7] + "X" + alt[2]["text"][8:]
            R.wr(b, alt)
            code, o, e = run_rule(["repeat", "--first", str(a), "--again", str(b), "--n", "3"])
            self.assertEqual(code, 0)
            self.assertIn("i2: DIFFERENT", o)
            self.assertIn("first difference at character 7", o)
            self.assertIn("The server does not reproduce long answers between two requests, so single answers are not exactly repeatable and the "
                          "rule's measures are averages over 54 and 116 answers.", o)

    def test_the_rule_follows_the_fresh_default_not_the_stored_one(self):
        # the rule reads variants.default only (the chain fills it from this run's own default answers). The same variant passes against a
        # fresh default of 0.62 and fails against a fresh default of 0.70, whatever a stored default would have been.
        m = measures(g1=variant(P=0.60))
        code, out, err = Rule.verdict(self, m)
        self.assertIn("CHOICE: G1", out)
        m["variants"]["default"]["P"] = 0.70
        code, out, err = Rule.verdict(self, m)
        self.assertIn("FAIL  (a) coverage P: g1 0.600, default 0.700", out)
        self.assertNotIn("CHOICE: G1", out)
        self.assertIn("this run's own default answers", R.RULE_TEXT)
        self.assertIn("decide nothing", " ".join(R.RULE_TEXT.split()))

    def test_check_echo(self):
        with tempfile.TemporaryDirectory() as t:
            st, st2, new = (Path(t) / n for n in ("s.json", "s2.json", "n.json"))
            st.write_text(json.dumps({"model_echoed": "m-4bit"}))
            st2.write_text(json.dumps({"model_echoed": "m-4bit"}))
            new.write_text(json.dumps({"model_echoed": "m-4bit"}))
            self.assertEqual(run_rule(["check-echo", "--stored", str(st), str(st2), "--new", str(new)])[0], 0)
            new.write_text(json.dumps({"model_echoed": "m-8bit"}))
            code, o, e = run_rule(["check-echo", "--stored", str(st), "--new", str(new)])
            self.assertEqual(code, 1)
            self.assertIn("DIFFERENT", o)
            new.write_text(json.dumps({"model_echoed": None}))
            self.assertEqual(run_rule(["check-echo", "--stored", str(st), "--new", str(new)])[0], 1)      # not echoed: not verified
            self.assertEqual(run_rule(["check-echo", "--stored", str(st), "--new", str(new), "--dry"])[0], 0)
            new.write_text(json.dumps({"model_echoed": "m-4bit"}))
            st2.write_text(json.dumps({"model_echoed": "other"}))
            self.assertEqual(run_rule(["check-echo", "--stored", str(st), str(st2), "--new", str(new)])[0], 1)

    def test_packs_equal(self):
        with tempfile.TemporaryDirectory() as t:
            a, b = Path(t) / "a.jsonl", Path(t) / "b.jsonl"
            R.wr(a, [{"id": "1", "pack": ["c1", "c2"]}, {"id": "2", "pack": ["c3"]}])
            R.wr(b, [{"id": "1", "pack": ["c1", "c2"]}])
            self.assertEqual(run_rule(["packs-equal", "--a", str(a), "--b", str(b)])[0], 0)
            R.wr(b, [{"id": "1", "pack": ["c2", "c1"]}])
            self.assertEqual(run_rule(["packs-equal", "--a", str(a), "--b", str(b)])[0], 1)

    def test_rule_text_is_stable_and_names_every_condition(self):
        code, o, e = run_rule(["rule-text"])
        self.assertEqual(code, 0)
        for c in ("(a)", "(b)", "(c)", "(d)", "(e)", "(f)", "(g)", "-0.05", "1,400", "4.875", "neither"):
            self.assertIn(c, o)


# ---------------------------------------------------------------- the chain, dry (stub model, stub judge)
@unittest.skipUnless(HAVE_WORK and IDS_FILE.exists(), "work/ inputs or eval/m4/g_dev_ids.json absent")
class ChainDryRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._t = tempfile.TemporaryDirectory()
        cls.w = Path(cls._t.name) / "w"
        cls.first = cls.chain()

    @classmethod
    def tearDownClass(cls):
        cls._t.cleanup()

    @classmethod
    def chain(cls, *extra):
        env = {k: v for k, v in os.environ.items() if k not in ("OMLX_URL", "OMLX_API_KEY", "JUDGE_API_KEY")}
        return subprocess.run(["sh", str(ROOT / "scripts/m5_g_dev.sh"), "--dry-run", "--work", str(cls.w), *extra], cwd=ROOT, env=env,
                              capture_output=True, text=True, timeout=1800)

    def test_1_dry_run_exits_0_and_writes_the_rule_into_the_header_first(self):
        self.assertEqual(self.first.returncode, 0, self.first.stdout[-2000:] + self.first.stderr[-2000:])
        rep = (self.w / "G_DEV_from_stub_data.md").read_text()
        self.assertIn("The rule of the development check", rep)
        self.assertLess(rep.index("The rule of the development check"), rep.index("## Result"))
        self.assertIn("CHOICE", (self.w / "rule_out.txt").read_text())
        self.assertIn("Note on (f): Known bias of the decline detector", rep)
        self.assertIn("B1: the stored judge results against this run's judgement of the fresh default answers", rep)
        self.assertIn("answers differ in the two-pass mean", rep)
        man = json.load(open(self.w / "MANIFEST.json"))
        for f in ("scripts/m5_g_rule.py", "scripts/m5_g_dev.sh", "scripts/eval2.py", "scripts/eval2_score.py", "scripts/eval2_judge.py",
                  "scripts/eval2_idcheck.py", "kafka_sre_rag/prompts.py", "kafka_sre_rag/frozen.json", "work/m4/b1/judge_results.jsonl"):
            self.assertIn(f, man["pins"])
        self.assertEqual(man["judge_args"]["setting"], "deepseek-nothink-12k")
        self.assertIn("git_head", man["info"])
        self.assertNotIn("judge_cache.jsonl", " ".join(man["pins"]))
        self.assertIn("stored against fresh default; decides nothing", rep)
        self.assertIn("Determinism measurement", rep)
        self.assertIn("determinism (i) stored against fresh", rep)
        self.assertIn("determinism (ii) within this run", rep)
        self.assertIn("this run's own default answers", rep)
        self.assertIn("this run's own default answers", man["reference"] if "reference" in man else json.load(open(self.w / "measures.json"))["reference"])
        for f in ("stage_header.done", "stage_reference.done", "stage_determinism.done", "stage_answers.done", "stage_judge.done", "stage_checks.done", "MANIFEST.json"):
            self.assertTrue((self.w / f).exists(), f)
        self.assertEqual(sum(1 for _ in open(self.w / "ans_b1_g1.jsonl")), 30)
        for name, n in (("b1", 30), ("p2", 24), ("ma", 116 - 20), ("m3", 20)):
            self.assertEqual(sum(1 for _ in open(self.w / f"ans_{name}_default.jsonl")), n, name)
        dm = json.load(open(str(self.w / "ans_b1_default.jsonl") + ".manifest.json"))
        self.assertEqual(dm["mode"], "default")
        self.assertEqual(sum(1 for _ in open(self.w / "det_default.jsonl")), 3)
        self.assertEqual(json.load(open(str(self.w / "det_default.jsonl") + ".manifest.json"))["mode"], "default")
        man = json.load(open(str(self.w / "ans_ma_g2.jsonl") + ".manifest.json"))
        self.assertEqual(man["mode"], "grounded-g2")

    def test_2_the_header_was_written_before_any_answer(self):
        h = (self.w / "stage_header.done").stat().st_mtime
        for f in ("ans_b1_default.jsonl", "ans_b1_g1.jsonl", "det_default.jsonl"):
            self.assertLess(h, (self.w / f).stat().st_mtime, f)
        self.assertLess((self.w / "stage_reference.done").stat().st_mtime, (self.w / "ans_b1_g1.jsonl").stat().st_mtime)

    def test_2b_the_measures_take_the_default_from_the_fresh_answers(self):
        # the stub's fresh default answers differ from the stored ones; the measures follow the fresh file
        m = json.load(open(self.w / "measures.json"))
        fresh = R.answer_stats(R.rd(self.w / "ans_b1_default.jsonl") + R.rd(self.w / "ans_p2_default.jsonl"), [], True, True)
        stored = R.answer_stats(R.rd(ROOT / "work/m4/b1/ans_default.jsonl") + R.rd(ROOT / "work/m4/pilot2/ans_default.jsonl"), [], True, True)
        self.assertNotEqual((fresh["cut"], fresh["median_tokens"]), (stored["cut"], stored["median_tokens"]))
        self.assertEqual((m["variants"]["default"]["cut"], m["variants"]["default"]["median_tokens"]), (fresh["cut"], fresh["median_tokens"]))
        rep = (self.w / "G_DEV_from_stub_data.md").read_text()
        row = [l for l in rep.splitlines() if l.startswith("| B1 | 30 |")]
        self.assertEqual(len(row), 1, rep[-3000:])

    def test_2c_a_stopped_runs_state_is_refused_with_the_mv_lines(self):
        # the work folder of this dry run with another rule text in its manifest stands for the stopped run's manifest
        mp = self.w / "MANIFEST.json"
        good = mp.read_text()
        d = json.loads(good)
        d["rule_text_sha256"] = "0" * 64
        mp.write_text(json.dumps(d))
        try:
            r = self.chain()
            self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
            self.assertIn("rule_text_sha256", r.stderr)
            self.assertIn(f"mv {self.w} {self.w}.stopped_20261122", r.stderr)
            self.assertEqual(mp.read_text(), json.dumps(d))        # nothing moved or deleted
        finally:
            mp.write_text(good)

    def test_3_resumes(self):
        r = self.chain()
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertIn("stage(s) done", r.stdout)

    def test_4_a_stale_marker_is_refused_with_exit_code_2(self):
        m = self.w / "stage_answers.done"
        good = m.read_text()
        rec = json.loads(good)
        rec["manifest_sha256"] = "0" * 64
        m.write_text(json.dumps(rec))
        try:
            r = self.chain()
            self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
            self.assertIn("stale", r.stderr)
        finally:
            m.write_text(good)

    def test_5_a_changed_pinned_file_is_refused_with_exit_code_2(self):
        mp = self.w / "MANIFEST.json"
        good = mp.read_text()
        d = json.loads(good)
        k = next(iter(d["pins"]))
        d["pins"][k] = "f" * 64            # as if the pinned file had changed since the manifest was written
        mp.write_text(json.dumps(d))
        try:
            r = self.chain()
            self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
            self.assertIn("REFUSED", r.stderr)
            self.assertIn("pins.", r.stderr)
        finally:
            mp.write_text(good)

    def test_6_a_changed_output_is_refused(self):
        f = self.w / "ans_b1_g1.jsonl"
        good = f.read_text()
        f.write_text(good + "\n")
        try:
            r = self.chain()
            self.assertEqual(r.returncode, 2)
            self.assertIn("changed since the stage finished", r.stderr)
        finally:
            f.write_text(good)


if __name__ == "__main__":
    unittest.main()
