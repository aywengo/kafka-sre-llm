"""M5 G1b and GD-chain: the grounded modes in eval2.py and the serve refusal, the rule script (scripts/m5_g_rule.py) on synthetic inputs, the start
check of the chain's manifest and markers, the ids of the mechanical sample, and one dry run of scripts/m5_g_dev.sh (stub model, stub judge).
Offline, CPU only; no server, no model."""
import contextlib
import io
import json
import os
import shutil
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
        for mode in ("default", "strict", "closed", "grounded-g1", "grounded-g2", "grounded-g3"):
            a = p.parse_args(["answer", "--items", "x", "--config", "c", "--mode", mode])
            self.assertEqual(a.mode, mode)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            p.parse_args(["answer", "--items", "x", "--config", "c", "--mode", "grounded-g4"])
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


# ---------------------------------------------------------------- a judge row that cannot be judged: --max-unjudged
def judge_rows(invalid=(), sets=("default", "g1", "g2"), ids=("q1", "q2"), cut_all=()):
    """Synthetic judge rows, two passes per answer; invalid = (set, id, pass) triples; cut_all = (set, id) pairs whose two passes are invalid."""
    rows = []
    for st in sets:
        for i in ids:
            for ps in (1, 2):
                bad = (st, i, ps) in invalid or (st, i) in cut_all
                rows.append({"set": st, "id": i, "pass": ps, "key": f"{st}-{i}-{ps}", "reference_given": False, "valid_reply": not bad,
                             "judge_errors": ["invalid_json"] if bad else [], "Pval": 0.2 * ps + (0.1 if st == "g1" else 0.0)})
    return rows


class Unjudged(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.t = Path(self._t.name)
        self.addCleanup(self._t.cleanup)

    def file(self, rows, name="judge_b1.jsonl"):
        R.wr(self.t / name, rows)
        return self.t / name

    def test_one_unjudged_row_goes_on_with_n1_and_the_coverage_is_the_valid_pass(self):
        import unittest.mock
        import eval2_judge as ej
        f = self.file(judge_rows(invalid=[("g2", "q1", 1)]))
        self.assertEqual(len(R.check_unjudged(f, 1)), 1)
        code, o, e = run_rule(["judge-check", "--file", str(f), "--expect", "12", "--max-unjudged", "1"])
        self.assertEqual(code, 0, o + e)
        self.assertIn("1 unjudged", o)
        self.assertIn("1 answers with one valid pass", o)
        rub = self.t / "rub.jsonl"
        R.wr(rub, [{"id": "q1"}, {"id": "q2"}])
        with unittest.mock.patch.object(ej, "coverage_row", lambda r, rb: {"P": r["Pval"], "units": []}):
            got = R.judged(str(f), str(rub), {"default", "g1", "g2"})
        self.assertEqual(got[("g2", "q1")], (0.4, 0.0, 1))              # pass 2 alone: 0.2 * 2
        self.assertAlmostEqual(got[("g2", "q2")][0], 0.3)               # two passes: mean of 0.2 and 0.4
        self.assertEqual(got[("g2", "q2")][2], 2)

    def test_the_same_with_n0_stops_as_today(self):
        f = self.file(judge_rows(invalid=[("g2", "q1", 1)]))
        with self.assertRaises(R.RuleError):
            R.check_unjudged(f, 0)
        code, o, e = run_rule(["judge-check", "--file", str(f), "--expect", "12"])
        self.assertEqual(code, 1)
        code, o, e = run_rule(["judge-check", "--file", str(f), "--expect", "12", "--max-unjudged", "0"])
        self.assertEqual(code, 1)

    def test_two_unjudged_rows_with_n1_stop(self):
        f = self.file(judge_rows(invalid=[("g2", "q1", 1), ("g1", "q2", 2)]))
        with self.assertRaises(R.RuleError):
            R.check_unjudged(f, 1)
        self.assertEqual(run_rule(["judge-check", "--file", str(f), "--expect", "12", "--max-unjudged", "1"])[0], 1)
        self.assertEqual(run_rule(["judge-check", "--file", str(f), "--expect", "12", "--max-unjudged", "2"])[0], 0)

    def test_an_answer_without_any_valid_pass_stops(self):
        f = self.file(judge_rows(cut_all=[("g1", "q2")]))
        with self.assertRaises(R.RuleError) as cm:
            R.check_unjudged(f, 5)                # both rows of the answer are invalid: stops whatever N is
        self.assertIn("without any valid judge pass", str(cm.exception))
        self.assertEqual(run_rule(["judge-check", "--file", str(f), "--expect", "12", "--max-unjudged", "5"])[0], 1)

    def test_a_short_file_does_not_pass_and_a_reference_row_is_not_counted(self):
        rows = judge_rows()
        f = self.file(rows[:-1])
        self.assertEqual(run_rule(["judge-check", "--file", str(f), "--expect", "12", "--max-unjudged", "1"])[0], 1)
        ref = dict(rows[0], reference_given=True, valid_reply=False)
        self.assertEqual(R.check_unjudged(self.file(rows + [ref]), 0), [])

    def test_the_report_block_names_the_option_the_row_the_parse_error_and_the_sentence(self):
        w = self.t / "w"
        R.wr(w / "judge_b1.jsonl", judge_rows(invalid=[("g2", "q1", 1)]))
        R.wr(w / "judge_p2.jsonl", judge_rows())
        R.wr(w / "judge_cache_b1.jsonl", [{"key": "g2-q1-1", "reply": '{"must": [1, 2,'}])
        code, o, e = run_rule(["unjudged-note", "--work", str(w), "--max-unjudged", "1"])
        self.assertEqual(code, 0, e)
        self.assertIn("--max-unjudged 1", o)
        self.assertIn("set of answers g2, id q1, pass 1", o)
        self.assertIn("invalid_json: JSONDecodeError", o)
        self.assertIn("attempts: 4", o)
        self.assertIn("this answer's judged measures rest on one pass", o)
        self.assertIn("pilot2: 0 unjudged", o)
        code, o, e = run_rule(["unjudged-note", "--work", str(w), "--max-unjudged", "0"])
        self.assertEqual((code, o), (0, ""))                  # off by default: nothing is printed

    def test_the_rule_text_is_not_changed(self):
        self.assertEqual(R.sha256_text(R.RULE_TEXT), "f1dc99a68170d0ca6c81c17a75d6732901351d3746ebdf31c2eb135162767683")


# ---------------------------------------------------------------- --accept-code-change
class AcceptCodeChange(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.t = Path(self._t.name)
        self.addCleanup(self._t.cleanup)
        (self.t / "scripts").mkdir()
        self.w = self.t / "w"
        self.w.mkdir()
        self.rule, self.sh = self.t / "scripts/m5_g_rule.py", self.t / "scripts/m5_g_dev.sh"
        self.other, self.ev = self.t / "items.jsonl", self.t / "scripts/eval2.py"
        for f, txt in ((self.rule, "rule v1"), (self.sh, "sh v1"), (self.other, "items"), (self.ev, "eval2 v1")):
            f.write_text(txt)
        self.frozen = self.t / "frozen.json"
        self.frozen.write_text(json.dumps({"prompts_sha256": {"A": "aa"}}))
        R.main(self.args("manifest"))
        self.old_manifest = (self.w / "MANIFEST.json").read_text()

    def args(self, cmd, extra=()):
        pins = [x for f in (self.rule, self.sh, self.other, self.ev) for x in ("--pin", str(f))]
        return [cmd, "--manifest", str(self.w / "MANIFEST.json"), "--work", str(self.w), *pins, "--frozen", str(self.frozen),
                "--mechanical", "sample", "--model", "m", "--dry", *extra]

    def stage(self, name):
        run_rule(["mark", "--work", str(self.w), "--stage", name, "--out", str(self.other)])

    def done(self, *names):
        for n in names:
            self.stage(n)

    def test_accepted_with_the_option_when_every_answering_stage_is_done(self):
        self.done("header", "prepare", "reference", "determinism", "answers")
        self.rule.write_text("rule v2")
        self.sh.write_text("sh v2")
        code, o, e = run_rule(self.args("start-check", ["--accept-code-change", "judge row policy"]))
        self.assertEqual(code, 0, o + e)
        self.assertIn("ACCEPTED change of the chain's own code", o)
        m = json.load(open(self.w / "MANIFEST.json"))
        ch = m["accepted_code_changes"]
        self.assertEqual(len(ch), 1)
        self.assertEqual(ch[0]["reason"], "judge row policy")
        self.assertIn("T", ch[0]["time"])
        self.assertEqual(set(ch[0]["files"]), {str(self.rule), str(self.sh)})
        self.assertEqual(ch[0]["files"][str(self.rule)], {"old": R.sha256_text("rule v1"), "new": R.sha256_text("rule v2")})
        self.assertEqual(m["pins"][str(self.rule)], R.sha256_text("rule v1"))          # the original pins stay as they were
        # the markers are still valid, a start check without the option now passes, and a further change is refused again
        self.assertEqual(run_rule(self.args("start-check"))[0], 0)
        self.sh.write_text("sh v3")
        self.assertEqual(run_rule(self.args("start-check"))[0], 2)
        code, o, e = run_rule(self.args("start-check", ["--accept-code-change", "second"]))
        self.assertEqual(code, 0, o + e)
        ch = json.load(open(self.w / "MANIFEST.json"))["accepted_code_changes"]
        self.assertEqual(ch[1]["files"][str(self.sh)]["old"], R.sha256_text("sh v2"))        # the old hash is the one in force before
        code, o, e = run_rule(["code-change-note", "--manifest", str(self.w / "MANIFEST.json")])
        self.assertIn(R.sha256_text("rule v1"), o)
        self.assertIn(R.sha256_text("rule v2"), o)
        self.assertIn("judge row policy", o)

    def test_refused_without_the_option(self):
        self.done("reference", "determinism", "answers")
        self.rule.write_text("rule v2")
        code, o, e = run_rule(self.args("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("the stored manifest differs from the state now", e)
        self.assertEqual((self.w / "MANIFEST.json").read_text(), self.old_manifest)

    def test_refused_when_an_answering_stage_is_not_done(self):
        for missing in ("reference", "determinism", "answers"):
            for f in self.w.glob("stage_*.done"):
                f.unlink()
            self.done(*[s for s in ("reference", "determinism", "answers") if s != missing], "header")
            self.rule.write_text("rule v2")
            code, o, e = run_rule(self.args("start-check", ["--accept-code-change", "why"]))
            self.assertEqual(code, 2, missing)
            self.assertIn("not every answering stage is done", e)
            self.assertEqual((self.w / "MANIFEST.json").read_text(), self.old_manifest)
            self.rule.write_text("rule v1")

    def test_refused_when_another_pinned_file_changed(self):
        self.done("reference", "determinism", "answers")
        self.rule.write_text("rule v2")
        for f in (self.ev, self.other):
            f.write_text(f.read_text() + " changed")
            code, o, e = run_rule(self.args("start-check", ["--accept-code-change", "why"]))
            self.assertEqual(code, 2, f)
            self.assertIn("besides scripts/m5_g_rule.py and scripts/m5_g_dev.sh", e)
            f.write_text(f.read_text().replace(" changed", ""))
        self.frozen.write_text(json.dumps({"prompts_sha256": {"A": "bb"}}))
        self.assertEqual(run_rule(self.args("start-check", ["--accept-code-change", "why"]))[0], 2)
        self.assertEqual((self.w / "MANIFEST.json").read_text(), self.old_manifest)

    def test_refused_with_an_empty_reason_and_with_a_stale_marker_and_nothing_to_accept_is_a_no_op(self):
        self.done("reference", "determinism", "answers")
        self.assertEqual(run_rule(self.args("start-check", ["--accept-code-change", "why"]))[0], 0)        # nothing differs: nothing to accept, nothing written
        self.assertEqual((self.w / "MANIFEST.json").read_text(), self.old_manifest)
        self.rule.write_text("rule v2")
        self.assertEqual(run_rule(self.args("start-check", ["--accept-code-change", "  "]))[0], 2)
        (self.w / "stage_answers.done").write_text(json.dumps({"stage": "answers", "manifest_sha256": "0" * 64, "outputs": {}}))
        code, o, e = run_rule(self.args("start-check", ["--accept-code-change", "why"]))
        self.assertEqual(code, 2)
        self.assertIn("stale", e)
        self.assertEqual((self.w / "MANIFEST.json").read_text(), self.old_manifest)       # nothing written

    def test_no_note_without_an_accepted_change(self):
        code, o, e = run_rule(["code-change-note", "--manifest", str(self.w / "MANIFEST.json")])
        self.assertEqual((code, o), (0, ""))

    def test_the_manifest_hash_ignores_the_audit_key(self):
        m = json.loads(self.old_manifest)
        self.assertEqual(R.manifest_hash(m), R.manifest_hash({**m, "accepted_code_changes": [{"x": 1}]}))


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

    def test_5b_the_new_options_are_checked_before_anything_starts(self):
        for bad in ("x", "-1", "1.5"):
            r = self.chain("--max-unjudged", bad)
            self.assertEqual(r.returncode, 2, bad)
            self.assertIn("--max-unjudged is a whole number", r.stderr)

    def test_5c_the_options_are_accepted_by_a_dry_resume_and_the_report_stays_as_it_is(self):
        # all judge rows of the stub are valid: nothing is left out, the unjudged block says so, and the accepted-change block is absent
        # (the code did not change here, so the option has nothing to accept and the dry start check goes on with the unchanged manifest)
        before = (self.w / "G_DEV_from_stub_data.md").read_text()
        r = self.chain("--max-unjudged", "1")
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        rep = (self.w / "G_DEV_from_stub_data.md").read_text()
        self.assertIn("Judge rows that could not be judged", rep)
        self.assertIn("No row is unjudged.", rep)
        self.assertNotIn("Accepted change of the chain's own code", rep)
        self.assertNotIn("Judge rows that could not be judged", before)

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


# ---------------------------------------------------------------- the check of other variants against a stored reference (G3; the Owner's decision Q13)
def variant_measures(variants, **kw):
    m = measures()
    m["variants"] = {"default": m["variants"]["default"], **{v: variant(**kw) for v in variants}}
    m["variants_checked"] = list(variants)
    return m


class ReferenceRule(unittest.TestCase):
    def verdict(self, m):
        return Rule.verdict(self, m)

    def test_the_first_checks_rule_text_is_byte_for_byte_the_old_one(self):
        # sha256 of the rule text recorded in the first development check's manifest (work/m5/gdev/MANIFEST.json): the default invocation does not change it
        self.assertEqual(R.sha256_text(R.RULE_TEXT), "f1dc99a68170d0ca6c81c17a75d6732901351d3746ebdf31c2eb135162767683")
        self.assertEqual(R.rule_text(None, None), R.RULE_TEXT)
        self.assertEqual(R.rule_text(("g1", "g2"), None), R.RULE_TEXT)
        self.assertEqual(run_rule(["rule-text"])[1], R.RULE_TEXT)

    def test_the_variants_rule_text_keeps_the_seven_conditions_and_names_the_reference(self):
        ref = {"dir": "work/x", "manifest_sha256": "ab" * 32}
        t = R.rule_text(("g3",), ref)
        body = R.RULE_TEXT[R.RULE_TEXT.index("1. (a)"):R.RULE_TEXT.index("Choice:")]
        self.assertIn(body, t)          # the seven conditions, the numbers and the weights, character for character
        self.assertIn("A measure that is missing, not a number or not computed is an error, never a pass.", t)
        self.assertIn("`work/x`", t)
        self.assertIn("manifest hash " + "ab" * 8, t)
        self.assertIn("repeat probe", t)
        self.assertIn("byte for byte", t)
        self.assertIn("run without --reference-from", t)
        self.assertIn("Choice: G3 if eligible", t)
        self.assertNotIn("G1", t)
        self.assertIn("wrong declines on the 20 tier_a3 items", t)
        self.assertIn("diagnostics", t)
        self.assertNotEqual(t, R.RULE_TEXT)

    def test_a_single_variant_reads_choice_g3_or_neither(self):
        code, out, err = self.verdict(variant_measures(["g3"]))
        self.assertEqual(code, 0, err)
        self.assertIn("CHOICE: G3", out)
        self.assertEqual(out.count("PASS"), 7)
        self.assertNotIn("Variant G1", out)
        code, out, err = self.verdict(variant_measures(["g3"], P=0.50))
        self.assertIn("FAIL  (a) coverage P: g3 0.500", out)
        self.assertIn("CHOICE: neither", out)
        self.assertNotIn("CHOICE: G3", out)

    def test_every_threshold_applies_to_the_listed_variant(self):
        for name, kw in {"a": dict(P=0.569), "b": dict(S=5.5), "c": dict(cut=6), "d": dict(med=1401), "g": dict(ident=3.5),
                         "e1": dict(d={"M1": 0, "M2": -3, "M3": 0}), "e2": dict(ff=6, ffr=3), "f": dict(wd=9, cov=80)}.items():
            code, out, err = self.verdict(variant_measures(["g3"], **kw))
            self.assertEqual(code, 0, err)
            self.assertIn("CHOICE: neither", out, name)
        code, out, err = self.verdict(variant_measures(["g3"], P=0.5701, S=5.0, cut=5, med=1400, ident=3.0, ff=5, ffr=3, wd=8, cov=80))
        self.assertIn("CHOICE: G3", out)

    def test_a_missing_listed_variant_is_an_error(self):
        m = variant_measures(["g3"])
        del m["variants"]["g3"]
        code, out, err = self.verdict(m)
        self.assertEqual(code, 2)
        self.assertNotIn("CHOICE", out)

    def test_parse_variants(self):
        self.assertEqual(R.parse_variants(None), ("g1", "g2"))
        self.assertEqual(R.parse_variants("g3"), ("g3",))
        self.assertEqual(R.parse_variants("g1,g3"), ("g1", "g3"))
        self.assertEqual(R.parse_variants("g1 g2"), ("g1", "g2"))
        for bad in ("g4", "g3,g3", "default"):
            with self.assertRaises(R.RuleError):
                R.parse_variants(bad)

    def test_the_probe_stops_on_any_difference_and_says_what_to_do(self):
        with tempfile.TemporaryDirectory() as t:
            ref, same, diff = Path(t) / "r.jsonl", Path(t) / "s.jsonl", Path(t) / "d.jsonl"
            rows = [{"id": f"b1-0{i}", "text": f"answer {i}", "truncated": False} for i in range(1, 4)]
            for f, rs in ((ref, rows), (same, rows), (diff, rows[:2] + [{**rows[2], "text": "answer 3 changed"}])):
                f.write_text("".join(json.dumps(r) + "\n" for r in rs))
            code, out, err = run_rule(["probe", "--reference", str(ref), "--again", str(same), "--n", "3"])
            self.assertEqual(code, 0, out)
            self.assertIn("3 of 3 answers equal", out)
            code, out, err = run_rule(["probe", "--reference", str(ref), "--again", str(diff), "--n", "3"])
            self.assertEqual(code, 1)
            self.assertIn("b1-03: DIFFERENT", out)
            self.assertIn("the reference must be answered again (run without --reference-from)", out)
            code, out, err = run_rule(["probe", "--reference", str(ref), "--again", str(same), "--n", "4"])      # too few rows is not a pass
            self.assertEqual(code, 1)

    def test_ref_check_needs_an_intact_finished_run(self):
        with tempfile.TemporaryDirectory() as t:
            d = Path(t) / "ref"
            d.mkdir()
            (d / "MANIFEST.json").write_text(json.dumps({"kind": "dry", "mechanical": "sample", "model": "m", "rule_text_sha256": "x"}))
            f = d / "out.txt"
            f.write_text("one")
            for st in R.REF_STAGES:
                self.assertEqual(run_rule(["mark", "--work", str(d), "--stage", st, "--out", str(f)])[0], 0)
            args = ["ref-check", "--reference-from", str(d), "--model", "m", "--dry"]
            self.assertEqual(run_rule(args)[0], 0)
            self.assertEqual(run_rule(args[:-1])[0], 2)                         # a dry reference is not a real one
            self.assertEqual(run_rule(args[:-2] + ["--model", "other", "--dry"])[0], 2)
            f.write_text("two")                                                 # an output changed since the stage finished
            code, out, err = run_rule(args)
            self.assertEqual(code, 2)
            self.assertIn("changed since the stage finished", err)
            f.write_text("one")
            (d / "stage_judge.done").unlink()                                   # a stage missing
            code, out, err = run_rule(args)
            self.assertEqual(code, 2)
            self.assertIn("stage_judge.done is missing", err)

    def test_start_check_records_the_variants_and_the_reference_and_refuses_a_change(self):
        with tempfile.TemporaryDirectory() as t:
            t = Path(t)
            ref = t / "ref"
            ref.mkdir()
            (ref / "MANIFEST.json").write_text(json.dumps({"kind": "dry", "x": 1}))
            pin = t / "pin.txt"
            pin.write_text("a")
            frozen = t / "frozen.json"
            frozen.write_text(json.dumps({"prompts_sha256": {"A": "1"}}))
            base = ["--manifest", str(t / "w/MANIFEST.json"), "--work", str(t / "w"), "--pin", str(pin), "--frozen", str(frozen), "--dry"]
            self.assertEqual(run_rule(["manifest", *base, "--variants", "g3", "--reference-from", str(ref)])[0], 0)
            man = json.load(open(t / "w/MANIFEST.json"))
            self.assertEqual(man["variants"], ["g3"])
            self.assertEqual(man["reference"], R.reference_info(ref))
            self.assertEqual(man["rule_text_sha256"], R.sha256_text(R.rule_text(("g3",), R.reference_info(ref))))
            self.assertEqual(run_rule(["start-check", *base, "--variants", "g3", "--reference-from", str(ref)])[0], 0)
            self.assertEqual(run_rule(["start-check", *base])[0], 2)                                            # other variants, no reference
            self.assertEqual(run_rule(["start-check", *base, "--variants", "g2", "--reference-from", str(ref)])[0], 2)
            (ref / "MANIFEST.json").write_text(json.dumps({"kind": "dry", "x": 2}))                             # the reference's manifest hash changed
            code, out, err = run_rule(["start-check", *base, "--variants", "g3", "--reference-from", str(ref)])
            self.assertEqual(code, 2)
            self.assertIn("reference", err)
            # the first check's manifest (no --variants, no --reference-from) has neither key
            plain = ["--manifest", str(t / "w2/MANIFEST.json"), "--work", str(t / "w2"), "--pin", str(pin), "--frozen", str(frozen), "--dry"]
            self.assertEqual(run_rule(["manifest", *plain])[0], 0)
            man2 = json.load(open(t / "w2/MANIFEST.json"))
            self.assertNotIn("variants", man2)
            self.assertNotIn("reference", man2)
            self.assertEqual(man2["rule_text_sha256"], R.sha256_text(R.RULE_TEXT))

    def test_the_accepted_code_change_needs_the_probe_and_the_answers_in_a_reference_run(self):
        with tempfile.TemporaryDirectory() as t:
            t = Path(t)
            ref = t / "ref"
            ref.mkdir()
            (ref / "MANIFEST.json").write_text(json.dumps({"kind": "dry"}))
            (t / "scripts").mkdir()
            code_f = t / "scripts" / "m5_g_rule.py"
            code_f.write_text("one")
            frozen = t / "frozen.json"
            frozen.write_text(json.dumps({"prompts_sha256": {}}))
            base = ["--manifest", str(t / "w/MANIFEST.json"), "--work", str(t / "w"), "--pin", str(code_f), "--frozen", str(frozen), "--dry",
                    "--variants", "g3", "--reference-from", str(ref)]
            self.assertEqual(run_rule(["manifest", *base])[0], 0)
            code_f.write_text("two")
            code, out, err = run_rule(["start-check", *base, "--accept-code-change", "why"])
            self.assertEqual(code, 2)
            self.assertIn("needs probe, answers", err)
            for st in ("probe", "answers"):
                self.assertEqual(run_rule(["mark", "--work", str(t / "w"), "--stage", st])[0], 0)
            code, out, err = run_rule(["start-check", *base, "--accept-code-change", "why"])
            self.assertEqual(code, 0, err)
            self.assertIn("ACCEPTED change", out)


@unittest.skipUnless(HAVE_WORK and IDS_FILE.exists(), "work/ inputs or eval/m4/g_dev_ids.json absent")
class ChainDryRunReference(unittest.TestCase):
    """The first check's chain is run dry once (the finished reference); then the chain checks g3 against it."""
    @classmethod
    def setUpClass(cls):
        cls._t = tempfile.TemporaryDirectory()
        cls.t = Path(cls._t.name)
        cls.ref = cls.t / "ref"
        r = cls.run_chain("--work", str(cls.ref))
        assert r.returncode == 0, r.stdout[-1500:] + r.stderr[-1500:]
        cls.ref_hashes = cls.tree(cls.ref)
        cls.ref_copy = cls.t / "ref_copy"           # the check reads the copy, so that a test may change it
        shutil.copytree(cls.ref, cls.ref_copy)
        cls.w = cls.t / "g3"
        cls.first = cls.chain(cls.w, cls.ref_copy)

    @classmethod
    def tearDownClass(cls):
        cls._t.cleanup()

    @staticmethod
    def tree(d):
        return {str(p.relative_to(d)): R.sha256_file(p) for p in sorted(Path(d).rglob("*")) if p.is_file()}

    @staticmethod
    def run_chain(*args):
        env = {k: v for k, v in os.environ.items() if k not in ("OMLX_URL", "OMLX_API_KEY", "JUDGE_API_KEY")}
        return subprocess.run(["sh", str(ROOT / "scripts/m5_g_dev.sh"), "--dry-run", *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=1800)

    @classmethod
    def chain(cls, w, ref, *extra):
        return cls.run_chain("--work", str(w), "--variants", "g3", "--reference-from", str(ref), "--max-unjudged", "1", *extra)

    def test_1_exits_0_checks_only_g3_and_leaves_the_reference_alone(self):
        self.assertEqual(self.first.returncode, 0, self.first.stdout[-2000:] + self.first.stderr[-2000:])
        w = self.w
        man = json.load(open(w / "MANIFEST.json"))
        self.assertEqual(man["variants"], ["g3"])
        self.assertEqual(man["reference"], R.reference_info(self.ref_copy))
        for f in ("MANIFEST.json", "judge_b1.jsonl", "judge_p2.jsonl", "ans_b1_default.jsonl", "score_ma_default.jsonl", "idcheck_b1_default.txt", "measures.json"):
            self.assertIn(f"{self.ref_copy}/{f}", man["pins"])
        self.assertEqual(man["pins"][f"{self.ref_copy}/judge_b1.jsonl"], R.sha256_file(self.ref_copy / "judge_b1.jsonl"))
        for f in ("stage_header.done", "stage_prepare.done", "stage_probe.done", "stage_answers.done", "stage_judge.done", "stage_checks.done", "probe_default.jsonl"):
            self.assertTrue((w / f).exists(), f)
        for f in ("stage_reference.done", "stage_determinism.done", "ans_b1_default.jsonl", "ans_b1_g1.jsonl", "ans_b1_g2.jsonl"):
            self.assertFalse((w / f).exists(), f)
        for name, n in (("b1", 30), ("p2", 24), ("ma", 96), ("m3", 20)):
            self.assertEqual(sum(1 for _ in open(w / f"ans_{name}_g3.jsonl")), n, name)
        self.assertEqual(json.load(open(str(w / "ans_ma_g3.jsonl") + ".manifest.json"))["mode"], "grounded-g3")
        self.assertEqual(sum(1 for _ in open(w / "probe_default.jsonl")), 3)
        for s_, n in (("b1", 60), ("p2", 48)):         # two passes of the one variant: nothing of default, g1 or g2 is judged again
            rows = R.rd(w / f"judge_{s_}.jsonl")
            self.assertEqual(len(rows), n)
            self.assertEqual({r["set"] for r in rows}, {"g3"})
        self.assertEqual(self.tree(self.ref), self.ref_hashes)          # the finished run was not touched
        out = (w / "rule_out.txt").read_text()
        self.assertEqual(out.count("Variant "), 1)
        self.assertRegex(out, r"CHOICE: (G3|neither)")
        self.assertNotIn("G1", out.replace("first development check", ""))

    def test_2_the_header_and_the_rule_come_first_and_say_which_reference(self):
        rep = (self.w / "G_DEV_from_stub_data.md").read_text()
        self.assertLess(rep.index("The rule of the development check"), rep.index("## Result"))
        self.assertIn(f"`{self.ref_copy}`", rep[:rep.index("## Result")])
        self.assertIn("manifest hash " + R.reference_info(self.ref_copy)["manifest_sha256"][:16], rep[:rep.index("## Result")])
        h = (self.w / "stage_header.done").stat().st_mtime
        for f in ("probe_default.jsonl", "ans_b1_g3.jsonl"):
            self.assertLess(h, (self.w / f).stat().st_mtime, f)
        self.assertLess((self.w / "stage_probe.done").stat().st_mtime, (self.w / "ans_b1_g3.jsonl").stat().st_mtime)

    def test_3_the_report_has_the_reported_parts_and_the_first_checks_rows(self):
        rep = (self.w / "G_DEV_from_stub_data.md").read_text()
        self.assertIn("**Choice by the rule: ", rep)
        self.assertIn("### Reported, deciding nothing", rep)
        self.assertIn("wrong declines on the tier_a3 part", rep)
        self.assertIn("judged coverage on the diagnostics scenarios", rep)
        self.assertIn("over 14 scenarios", rep)
        self.assertIn("| g1 (from the first development check) |", rep)
        self.assertIn("| g2 (from the first development check) |", rep)
        self.assertIn("| default (the reference run's) |", rep)
        self.assertIn("The repeat probe", rep)
        self.assertIn("3 of 3 answers equal the reference run's answers, byte for byte", rep)
        m = json.load(open(self.w / "measures.json"))
        self.assertEqual(m["variants_checked"], ["g3"])
        self.assertEqual(set(m["variants"]), {"default", "g3"})
        self.assertEqual(m["variants"]["default"], json.load(open(self.ref / "measures.json"))["variants"]["default"])      # the reference's numbers, as they are
        self.assertEqual(set(m["reported"]["a3"]), {"default", "g3"})
        self.assertEqual(m["reported"]["diagnostics"]["g3"]["scenarios"], 14)
        self.assertEqual(set(m["first_check"]), {"g1", "g2"})

    def test_4_resumes(self):
        r = self.chain(self.w, self.ref_copy)
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertIn("stage(s) done", r.stdout)
        self.assertEqual(self.tree(self.ref), self.ref_hashes)

    def test_5_a_changed_reference_file_is_refused_with_exit_code_2(self):
        f = self.ref_copy / "judge_b1.jsonl"
        good = f.read_bytes()
        f.write_bytes(good + b"\n")
        try:
            r = self.chain(self.w, self.ref_copy)
            self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
            self.assertIn("REFUSED", r.stderr)
            self.assertIn("pins.", r.stderr)
        finally:
            f.write_bytes(good)

    def test_6_the_old_default_invocation_is_unchanged(self):
        man = json.load(open(self.ref / "MANIFEST.json"))
        self.assertNotIn("variants", man)
        self.assertNotIn("reference", man)
        self.assertEqual(man["rule_text_sha256"], R.sha256_text(R.RULE_TEXT))
        for f in ("stage_reference.done", "stage_determinism.done", "ans_b1_g1.jsonl", "ans_b1_g2.jsonl", "ans_b1_default.jsonl", "det_default.jsonl"):
            self.assertTrue((self.ref / f).exists(), f)
        self.assertEqual({r["set"] for r in R.rd(self.ref / "judge_b1.jsonl")}, {"default", "g1", "g2"})
        self.assertEqual(len(R.rd(self.ref / "judge_b1.jsonl")), 180)
        self.assertRegex((self.ref / "rule_out.txt").read_text(), r"CHOICE: (G1|G2|neither)")

    def test_7_the_probe_stops_the_chain_on_a_difference(self):
        diff_ref = self.t / "ref_diff"
        shutil.copytree(self.ref, diff_ref)
        f = diff_ref / "ans_b1_default.jsonl"
        rows = R.rd(f)
        rows[0]["text"] = rows[0]["text"] + " (changed)"
        R.wr(f, rows)           # the stage markers name the original files, so the reference still counts as intact; the pin and the probe see the change
        w = self.t / "g3_diff"
        r = self.chain(w, diff_ref)
        self.assertEqual(r.returncode, 1, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertIn("DRY RUN FAILED", r.stdout)
        self.assertIn("the reference must be answered again (run without --reference-from)", r.stdout)
        self.assertIn("b1-01: DIFFERENT", r.stdout)
        self.assertFalse((w / "stage_probe.done").exists())
        self.assertFalse((w / "ans_b1_g3.jsonl").exists())        # nothing of the variants was answered
        self.assertTrue((w / "stage_header.done").exists())

    def test_8_a_reference_that_is_not_a_finished_run_is_refused(self):
        w = self.t / "g3_bad"
        bad = self.t / "ref_unfinished"
        shutil.copytree(self.ref, bad)
        (bad / "stage_judge.done").unlink()
        r = self.chain(w, bad)
        self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
        self.assertIn("stage_judge.done is missing", r.stderr)
        self.assertFalse((w / "MANIFEST.json").exists())

    def test_9_options_are_checked_before_anything_starts(self):
        r = self.run_chain("--variants", "g4")
        self.assertEqual(r.returncode, 2)
        self.assertIn("--variants is a list of g1, g2, g3", r.stderr)
        r = self.run_chain("--work", str(self.t / "x"), "--variants", "g3", "--reference-from", str(self.t / "nowhere"))
        self.assertEqual(r.returncode, 2)
        self.assertIn("is not a folder", r.stderr)
        r = self.run_chain("--work", str(self.ref), "--variants", "g3", "--reference-from", str(self.ref))
        self.assertEqual(r.returncode, 2)
        self.assertIn("--work is the reference run's folder", r.stderr)
        # a real run needs its own work folder, report and marker name: nothing of the finished run is overwritten
        env = {k: v for k, v in os.environ.items() if k not in ("OMLX_URL", "OMLX_API_KEY", "JUDGE_API_KEY")}
        r = subprocess.run(["sh", str(ROOT / "scripts/m5_g_dev.sh"), "--variants", "g3", "--reference-from", str(self.ref)], cwd=ROOT, env=env,
                           capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 2)
        self.assertIn("--work, --report and --name are required", r.stderr)
        r = subprocess.run(["sh", str(ROOT / "scripts/m5_g_dev.sh"), "--variants", "g3", "--reference-from", str(self.ref), "--work", str(self.t / "n"),
                            "--report", "runs/m5/G_DEV.md", "--name", "m5_g_dev3"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 2)
        self.assertIn("must differ from the finished run's", r.stderr)


if __name__ == "__main__":
    unittest.main()
