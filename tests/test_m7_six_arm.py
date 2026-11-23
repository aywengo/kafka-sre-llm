"""M7 K4: the six-arm test chain (scripts/m7_four_way.sh, scripts/m7_six_arm_rule.py): the rule on synthetic totals for each outcome row, the gates (the two run parameters,
a changed input after the manifest exists, perfect packs whose manifest does not match), the derived arms, the prompt relations, the stage call counts, and the two small
changes to existing code (--think-cap and arm A's switches in eval2.py; the request-body hash in the generators' call info). Offline, CPU only; no server, no model.
The whole dry run of the chain for each variant (about 1.5 minutes each) is opt-in:
    M7_SIX_E2E=1 .venv/bin/python -m unittest tests.test_m7_six_arm.EndToEndDryRun"""
import contextlib
import copy
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))

import m7_six_arm_rule as R  # noqa: E402
from kafka_sre_rag import generators as GEN  # noqa: E402

SCRIPT = ROOT / "scripts/m7_four_way.sh"


def run_main(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = R.main(argv)
        except SystemExit as e:
            code = e.code
    return code, out.getvalue(), err.getvalue()


def arm(P=0.5, cut=0, med=100.0, p90=150.0, ident=1.0):
    return {"P": P, "cut": cut, "idents_per10": ident, "answer_over_limit": 0, "time": {"median_s": med, "p90_s": p90}}


def measures(variant="V1C", ea=(0.0, 0.03), da=(0.0, 0.03), cut=0, med=100.0, p90=150.0, ident=None):
    """Synthetic totals. ea, da: (mean, se) of the paired differences."""
    ident = ident or {"A": 2.0, "B": 2.0, "C": 2.0, "D": 2.0}
    t = "D" if variant != "none" else "B"
    m = {"variant": variant, "think_cap": 1500, "thorough_arm": t, "sizes": {"D5": 50},
         "arms": {k: arm(ident=v) for k, v in ident.items()},
         "pairs": {"E-A": {"mean": ea[0], "se": ea[1], "n": 50}, f"{t}-A": {"mean": da[0], "se": da[1], "n": 50}}}
    m["arms"][t] = arm(cut=cut, med=med, p90=p90, ident=ident.get(t, 2.0))
    for k in ("A", "B", "E"):
        m["arms"].setdefault(k, arm(ident=2.0))
    return m


class RuleTests(unittest.TestCase):
    def ev(self, **kw):
        return R.evaluate(measures(**kw))

    def test_knowledge_base_row_1_needs_a_difference_of_0_10_and_a_lower_end_above_0_04(self):
        _, res = self.ev(ea=(0.12, 0.03))                # lower end 0.12 - 1.645 * 0.03 = 0.0707
        self.assertEqual(res["kb"], "build")
        _, res = self.ev(ea=(0.10, 0.03))                # the edge: 0.10 and a lower end of 0.0507
        self.assertEqual(res["kb"], "build")
        _, res = self.ev(ea=(0.12, 0.05))                # difference above 0.10 but the lower end 0.0378: not row 1, row 2
        self.assertEqual(res["kb"], "a_only")

    def test_knowledge_base_row_2_is_between_0_04_and_0_10(self):
        for d in (0.04, 0.07, 0.0999):
            self.assertEqual(self.ev(ea=(d, 0.03))[1]["kb"], "a_only", d)

    def test_knowledge_base_row_3_never_claims_a_lookup_decision(self):
        # review should-fix 1: (a) means cards in the draft prompt and no arm has them: "not tested by this test" in every variant, the identifier differences as description only
        for v in ("V1", "V1C", "none"):
            for ident in ({"A": 2.0, "B": 2.0, "C": 1.0, "D": 1.5}, {"A": 2.0, "B": 2.1, "C": 1.9, "D": 2.0}, {"A": 2.0, "B": 2.0, "C": 2.0, "D": 2.0}):
                L, res = self.ev(variant=v, ea=(0.039, 0.03), ident=ident)
                self.assertEqual(res["kb"], "none_lookup_not_tested", (v, ident))
                text = "\n".join(L)
                self.assertIn("is not tested by this test", text)
                self.assertNotIn("(a) only\n", text)
                self.assertNotIn("the lookup lowers them", text)
        L, _ = self.ev(variant="V1C", ea=(0.0, 0.03), ident={"A": 2.0, "B": 2.0, "C": 1.0, "D": 1.5})
        self.assertIn("description only, decides nothing", "\n".join(L))
        self.assertIn("C - A -1.00", "\n".join(L))

    def test_a_build_does_not_settle_b(self):
        L, _ = self.ev(ea=(0.12, 0.03))
        self.assertIn("does not settle (b)", "\n".join(L))
        self.assertIn("V8", "\n".join(L))

    def test_the_owners_decisions_of_2026_11_23(self):
        # (1) narrow: a cut failure that comes with a time failure is "neither"
        L, res = self.ev(cut=6, med=200.0)
        self.assertEqual(len(res["thorough_possible"]), 1)
        self.assertTrue(res["thorough_possible"][0].startswith("neither"))
        # a time failure alone is the second row
        _, res = self.ev(med=200.0)
        self.assertTrue(any(p.startswith("time_fails") for p in res["thorough_possible"]))
        # the single place for the other reading: TIME_FAIL_TAKES_CUT
        old = R.TIME_FAIL_TAKES_CUT
        try:
            R.TIME_FAIL_TAKES_CUT = True
            _, res = self.ev(cut=6, med=200.0)
            self.assertTrue(any(p.startswith("time_fails") for p in res["thorough_possible"]))
            _, res = self.ev(cut=6)                                         # cut alone still fails: neither
            self.assertTrue(res["thorough_possible"][0].startswith("neither"))
        finally:
            R.TIME_FAIL_TAKES_CUT = old
        # (2) under none, thinking alone MAY go on to B4, and the printed outcome says so
        _, res = self.ev(variant="none")
        self.assertTrue(res["thorough_possible"][0].startswith("goes_on_to_B4"))
        self.assertIn("thinking alone MAY go on to B4", res["thorough_possible"][0])
        old = R.NONE_THINKING_MAY_GO_TO_B4
        try:
            R.NONE_THINKING_MAY_GO_TO_B4 = False
            _, res = self.ev(variant="none")
            self.assertTrue(res["thorough_possible"][0].startswith("waits for the Owner"))
        finally:
            R.NONE_THINKING_MAY_GO_TO_B4 = old
        self.assertIn("TIME_FAIL_TAKES_CUT", R.RULE_TEXT)
        self.assertIn("NONE_THINKING_MAY_GO_TO_B4", R.RULE_TEXT)
        self.assertIn("stays NARROW", R.RULE_TEXT)
        self.assertNotIn("unload-others", (ROOT / "scripts/m7_four_way.sh").read_text(encoding="utf-8"))

    def test_checking_failures_above_the_limit_mark_the_arm(self):
        m = measures()
        for k, e in (("C", 2), ("D", 3), ("F", 0)):
            m["arms"].setdefault(k, arm())
            m["arms"][k]["check"] = {"status": {"error": e, "not_json": 1, "cut": 0, "no_patch": 49 - e}}
        L, res = R.evaluate(m)
        self.assertEqual(res["check_failures_above_limit"], ["D"])            # more than 2 of 50: 3; exactly 2 is at the limit
        text = "\n".join(L)
        self.assertIn("arm D: error 3, not_json 1, cut 0", text)
        self.assertIn("CHECK FAILURES ABOVE LIMIT", text)
        p = Path(tempfile.mkdtemp()) / "m.json"
        p.write_text(json.dumps(m))
        code, out, _ = run_main(["rule", "--measures", str(p)])
        self.assertEqual(code, 0)
        self.assertTrue(out.strip().splitlines()[-1].endswith("check failures above limit: D"))
        shutil.rmtree(p.parent, True)

    def test_an_answer_over_the_limit_stops_the_rule(self):
        m = measures()
        m["arms"]["D"]["answer_over_limit"] = 1
        with self.assertRaises(R.SixError) as cm:
            R.evaluate(m)
        self.assertIn("arm D: 1 answers over", str(cm.exception))
        m = measures()
        del m["arms"]["E"]["answer_over_limit"]
        with self.assertRaises(R.SixError):                                  # a missing measure is not a pass
            R.evaluate(m)

    def test_the_thorough_row_waits_for_the_reading_and_prints_no_final_choice(self):
        L, res = self.ev()
        text = "\n".join(L)
        self.assertEqual(text.count("NEEDS THE READING"), 2)
        self.assertIn("wrong statements", text)
        self.assertIn("harmful statements", text)
        self.assertNotIn("CHOICE", text)
        self.assertIn("waits for the reading", text)
        self.assertTrue(any(p.startswith("goes_on_to_B4") for p in res["thorough_possible"]))
        self.assertFalse(any(p.startswith("time_fails") for p in res["thorough_possible"]))

    def test_each_measured_condition_of_the_thorough_row(self):
        cases = {"coverage": dict(da=(-0.0401, 0.03)), "cut": dict(cut=6), "median": dict(med=180.1), "p90": dict(p90=270.1)}
        for key, kw in cases.items():
            with self.subTest(failing=key):
                L, res = self.ev(**kw)
                fails = [l for l in L if l.strip().startswith("FAIL")]
                self.assertEqual(len(fails), 1, fails)
                self.assertEqual(res["conditions"][key], False)
                if key in ("median", "p90"):      # time fails: the second thorough row is still possible
                    self.assertTrue(any(p.startswith("time_fails") for p in res["thorough_possible"]))
                    self.assertFalse(any(p.startswith("goes_on") for p in res["thorough_possible"]))
                else:                             # coverage or cut fails: only "neither" is possible
                    self.assertEqual(len(res["thorough_possible"]), 1)
                    self.assertTrue(res["thorough_possible"][0].startswith("neither"))
        # the edges pass: -0.04 exactly, cut 5 of 50 (10 %), 180 s, 270 s
        _, res = self.ev(da=(-0.04, 0.03), cut=5, med=180.0, p90=270.0)
        self.assertEqual(res["conditions"], {"coverage": True, "cut": True, "median": True, "p90": True})

    def test_the_variant_none_reads_the_thorough_row_on_b(self):
        L, res = self.ev(variant="none")
        self.assertIn("arm B against A", "\n".join(L))
        self.assertEqual(res["conditions"]["cut"], True)

    def test_a_missing_or_non_numeric_measure_is_an_error_never_a_pass(self):
        for path in (("pairs", "E-A", "mean"), ("pairs", "D-A", "se"), ("arms", "D", "cut"), ("arms", "D", "time", "p90_s"), ("sizes", "D5")):
            m = measures()
            d = m
            for p in path[:-1]:
                d = d[p]
            del d[path[-1]]
            with self.subTest(missing=path), self.assertRaises(R.SixError):
                R.evaluate(m)
        m = measures()
        m["arms"]["D"]["time"]["median_s"] = float("nan")
        with self.assertRaises(R.SixError):
            R.evaluate(m)

    def test_the_rule_command_prints_the_last_line_and_exits_2_on_missing_input(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.json"
            p.write_text(json.dumps(measures(ea=(0.12, 0.03))))
            code, out, _ = run_main(["rule", "--measures", str(p)])
            self.assertEqual(code, 0)
            self.assertEqual(out.strip().splitlines()[-1], "M7 SIX-ARM TEST DONE: knowledge-base row: build; thorough-profile row: WAITS FOR THE READING")
            bad = measures()
            del bad["pairs"]["E-A"]
            p.write_text(json.dumps(bad))
            code, out, err = run_main(["rule", "--measures", str(p)])
            self.assertEqual(code, 2)
            self.assertNotIn("DONE", out)
            self.assertIn("no verdict", err)

    def test_the_rule_text_carries_the_plans_table_verbatim(self):
        plan = (ROOT / "agents/m7-plan.md").read_text(encoding="utf-8")
        start = plan.index("| Result | Recommendation |")
        end = plan.index("\n\n", start)
        table = plan[start:end]
        self.assertIn(table, R.RULE_TEXT)
        for line in ("Paired coverage has a standard error of about 0.025 to 0.04, so differences under about 0.06 are not resolved.",
                     "A 90th percentile from 50 answers is the fifth largest, so the measured value must be at most 270 s."):
            self.assertIn(line, plan)
            self.assertIn(line, R.RULE_TEXT)


class Helpers(unittest.TestCase):
    def test_p90_is_the_fifth_largest_of_fifty(self):
        xs = list(range(1, 51))
        self.assertEqual(R.p90(xs), 46)                   # the fifth largest of 1..50
        self.assertEqual(R.p90(list(reversed(xs))), 46)
        self.assertEqual(R.p90([7]), 7)
        self.assertIsNone(R.p90([]))

    def test_run_parameters(self):
        self.assertEqual(R.check_params("V1C", "1500"), ("V1C", 1500))
        for v, c in (("V2", "1500"), ("", "1500"), ("V1", "0"), ("V1", "-3"), ("V1", "1.5"), ("V1", "abc"), ("V1", "99999"), ("V1", None)):
            with self.subTest(v=v, c=c), self.assertRaises(R.SixError):
                R.check_params(v, c)

    def test_expected_calls(self):
        e = R.expected_calls(50, "V1C", 96, 20)
        self.assertEqual((e["ans_a"], e["ans_c"], e["ans_e"], e["ans_d"], e["ans_f"], e["judge"]), (166, 50, 50, 332, 100, 700))
        e = R.expected_calls(50, "none", 96, 20)
        self.assertEqual((e["ans_c"], e["ans_d"], e["ans_f"], e["judge"]), (0, 166, 50, 400))

    def test_the_judged_sets_by_variant(self):
        self.assertEqual(list(R.SET_STEMS["V1"]), ["A", "B", "C", "D", "E", "F", "Fp"])
        self.assertEqual(R.SET_STEMS["none"], {"A": "A", "B": "D", "E": "E", "Fp": "F"})


class Derived(unittest.TestCase):
    def setUp(self):
        self.td = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.td, True)

    def rows(self, name, rows, manifest=None):
        p = self.td / name
        p.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        Path(str(p) + ".manifest.json").write_text(json.dumps(manifest or {"mode": "default", "ended": "t"}), encoding="utf-8")
        return p

    def test_derive_draft_takes_the_draft_and_removes_the_check_seconds(self):
        d = self.rows("D.jsonl", [
            {"id": "x1", "text": "final", "first_pass_text": "draft", "seconds": {"draft": 40.0, "thinking": 10.0, "check": 15.0, "answer": 55.0, "total": 56.0}, "truncated": False, "pack": ["c"], "thorough": {"r2": {}}},
            {"id": "x2", "text": "No answer: the model was cut off by the token limit while thinking, ...", "first_pass_text": "", "seconds": {"draft": 50.0, "check": 0.0, "answer": 50.0, "total": 51.0},
             "truncated": True, "cut_while_thinking": True, "pack": ["c"], "thorough": {"r2": {"status": "no_draft"}}}])
        out = self.td / "B.jsonl"
        self.assertEqual(run_main(["derive-draft", "--src", str(d), "--out", str(out), "--arm", "B"])[0], 0)
        b = {r["id"]: r for r in R.rd(out)}
        self.assertEqual(b["x1"]["text"], "draft")
        self.assertEqual((b["x1"]["seconds"]["check"], b["x1"]["seconds"]["answer"], b["x1"]["seconds"]["total"]), (0.0, 40.0, 41.0))
        self.assertIsNone(b["x1"]["thorough"])
        self.assertTrue(b["x2"]["text"].startswith("No answer"))      # a draft cut while thinking stays the notice
        self.assertEqual(b["x1"]["derived_from"]["sha256"], R.sha256_file(d))
        self.assertEqual(json.load(open(str(out) + ".manifest.json"))["derived"]["arm"], "B")

    def test_derive_check_adds_the_check_seconds_to_as_row(self):
        a = self.rows("A.jsonl", [{"id": "x1", "text": "draft", "seconds": {"draft": 30.0, "check": 0.0, "answer": 30.0, "total": 31.0}, "tokens_detail": {"draft": 99, "check": None}, "pack": ["c"]}])
        rec = {"id": "x1", "config": {}, "error": None, "stored_seconds": 31.0, "seconds": {"total": 12.5}, "final_text": "fixed", "first_pass_text": "draft", "check_first": {}, "check_after": {},
               "proposals": [{"accepted": True, "line": 1}], "r2": {"status": "accepted", "tokens": 41}}
        c = self.rows("check.jsonl", [rec])
        out = self.td / "C.jsonl"
        self.assertEqual(run_main(["derive-check", "--answers", str(a), "--checks", str(c), "--out", str(out)])[0], 0)
        r = R.rd(out)[0]
        self.assertEqual((r["text"], r["first_pass_text"]), ("fixed", "draft"))
        self.assertEqual((r["seconds"]["check"], r["seconds"]["total"], r["seconds"]["answer"]), (12.5, 43.5, 42.5))
        self.assertEqual(r["tokens_detail"]["check"], 41)
        self.assertEqual(r["thorough"]["r2"]["status"], "accepted")
        # a missing or failed record stops it
        c2 = self.rows("check2.jsonl", [{**rec, "error": "boom"}])
        code, _, err = run_main(["derive-check", "--answers", str(a), "--checks", str(c2), "--out", str(self.td / "C2.jsonl")])
        self.assertEqual(code, 2)
        self.assertIn("no complete check record", err)

    def test_prompt_relation_same_pack_same_hash_other_pack_other_hash(self):
        a = self.rows("a.jsonl", [{"id": "1", "pack": ["x"], "prompt_sha256": "h1"}, {"id": "2", "pack": ["x"], "prompt_sha256": "h2"}])
        same = self.rows("same.jsonl", [{"id": "1", "pack": ["x"], "prompt_sha256": "h1"}, {"id": "2", "pack": ["x"], "prompt_sha256": "h2"}])
        other = self.rows("other.jsonl", [{"id": "1", "pack": ["y"], "prompt_sha256": "h9"}, {"id": "2", "pack": ["x"], "prompt_sha256": "h2"}])
        drift = self.rows("drift.jsonl", [{"id": "1", "pack": ["x"], "prompt_sha256": "h1"}, {"id": "2", "pack": ["x"], "prompt_sha256": "hZ"}])
        self.assertEqual(run_main(["prompts-relation", "--a", str(a), "--b", str(same), "--expect-same-pack"])[0], 0)
        self.assertEqual(run_main(["prompts-relation", "--a", str(a), "--b", str(other)])[0], 0)
        self.assertEqual(run_main(["prompts-relation", "--a", str(a), "--b", str(other), "--expect-same-pack"])[0], 1)
        self.assertEqual(run_main(["prompts-relation", "--a", str(a), "--b", str(drift)])[0], 1)        # same pack, another prompt: the arms differ in more than their switch
        same_hash_other_pack = self.rows("shop.jsonl", [{"id": "1", "pack": ["y"], "prompt_sha256": "h1"}, {"id": "2", "pack": ["x"], "prompt_sha256": "h2"}])
        self.assertEqual(run_main(["prompts-relation", "--a", str(a), "--b", str(same_hash_other_pack)])[0], 1)

    def test_stage_calls_counts_drafts_and_checks(self):
        ok = {"thorough": {"r2": {"status": "no_patch"}}}
        nd = {"thorough": {"r2": {"status": "no_draft"}}}
        f = self.rows("d.jsonl", [{"id": "1", **ok}, {"id": "2", **ok}, {"id": "3", **nd}])
        base = ["stage-calls", "--stage", "ans_d", "--files", str(f), "--expect-drafts", "3"]
        self.assertEqual(run_main(base + ["--expect-checks", "2", "--strict"])[0], 0)
        self.assertEqual(run_main(base + ["--expect-checks", "3", "--strict"])[0], 1)       # a dry run needs the exact count
        code, out, _ = run_main(base + ["--expect-checks", "3"])                             # a real run explains the difference
        self.assertEqual(code, 0)
        self.assertIn("no draft or not checked", out)
        self.assertEqual(run_main(["stage-calls", "--stage", "ans_d", "--files", str(f), "--expect-drafts", "4", "--expect-checks", "3"])[0], 1)
        a = self.rows("a.jsonl", [{"id": "1", "thorough": None}])
        self.assertEqual(run_main(["stage-calls", "--stage", "ans_a", "--files", str(a), "--expect-drafts", "1", "--expect-checks", "0", "--strict"])[0], 0)
        ck = self.rows("c.jsonl", [{"id": "1", "r2": {"status": "accepted"}}, {"id": "2", "r2": {"status": "not_checked"}}])
        self.assertEqual(run_main(["stage-calls", "--stage", "ans_c", "--files", str(ck), "--kind", "checks", "--expect-checks", "2"])[0], 0)

    def test_check_rows_wants_a_streamed_draft_and_in_real_runs_the_request_body_hash(self):
        good = {"id": "1", "draft_streamed": True, "tokens_detail": {"draft": 1}, "prompt_sha256": "h", "draft_info": {"request_body_sha256": "b"}}
        f = self.rows("g.jsonl", [good])
        self.assertEqual(run_main(["check-rows", "--file", str(f), "--expect", "1", "--real"])[0], 0)
        f = self.rows("h.jsonl", [{**good, "draft_info": {}}])
        self.assertEqual(run_main(["check-rows", "--file", str(f), "--expect", "1"])[0], 0)           # a dry run (stub) has no request body
        self.assertEqual(run_main(["check-rows", "--file", str(f), "--expect", "1", "--real"])[0], 1)
        f = self.rows("i.jsonl", [{**good, "draft_streamed": False}])
        self.assertEqual(run_main(["check-rows", "--file", str(f), "--expect", "1"])[0], 1)


class Gates(unittest.TestCase):
    """The gate's refusals. A synthetic D5 stand-in: items, rubrics, a chunk check with their hashes, a funnel pack file, perfect packs with a manifest."""

    def setUp(self):
        self.td = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.td, True)
        d = self.td
        items = [{"id": f"s-{i}", "question": f"q{i}", "tier": "B", "kind": "open"} for i in (1, 2)]
        rubs = [{"id": f"s-{i}", "must": [{"id": "m1", "chunk_sets": [["c1"]]}, {"id": "m2", "chunk_sets": []}], "must_not": []} for i in (1, 2)]
        (d / "items.jsonl").write_text("".join(json.dumps(x) + "\n" for x in items), encoding="utf-8")
        (d / "rubrics.jsonl").write_text("".join(json.dumps(x) + "\n" for x in rubs), encoding="utf-8")
        (d / "CHUNK_CHECK.json").write_text(json.dumps({"sha256": {"items.jsonl": R.sha256_file(d / "items.jsonl"), "rubrics.jsonl": R.sha256_file(d / "rubrics.jsonl")}}), encoding="utf-8")
        (d / "fw").mkdir()
        (d / "fw/packs.jsonl").write_text("".join(json.dumps({"id": f"s-{i}", "frozen": ["c1", "c2"], "r4g2": ["c1", "c2", "c3"]}) + "\n" for i in (1, 2)), encoding="utf-8")
        pf = d / "perfect.jsonl"
        pf.write_text("".join(json.dumps({"id": f"s-{i}", "pack": ["c1", "c2"], "error": None}) + "\n" for i in (1, 2)), encoding="utf-8")
        idx = ROOT / "runs/v3/INDEX_FROZEN.json"
        self.ins = {"items": str(d / "items.jsonl"), "rubrics": str(d / "rubrics.jsonl"), "chunk_check": str(d / "CHUNK_CHECK.json"), "funnel_packs": str(d / "fw/packs.jsonl")}
        if idx.is_file():
            self.ins["index_frozen"] = str(idx)
        self.man = {"kind": "m7-perfect-packs", "variant": "padded", "count": 2, "pack_file_sha256": R.sha256_file(pf), "inputs": self.ins,
                    "inputs_sha256": {k: R.sha256_file(v) for k, v in self.ins.items()}}
        if idx.is_file():
            self.man["index_sha256"] = json.load(open(idx))["sha256"]
        self.write_man()
        self.pf = pf

    def write_man(self):
        (self.td / "perfect.jsonl.manifest.json").write_text(json.dumps(self.man), encoding="utf-8")

    def gate(self, *extra, variant="V1C", cap="1500"):
        d = self.td
        return run_main(["gate-inputs", "--items", str(d / "items.jsonl"), "--rubrics", str(d / "rubrics.jsonl"), "--chunk-check", str(d / "CHUNK_CHECK.json"), "--funnel-work", str(d / "fw"),
                         "--perfect", str(self.pf), "--check-variant", variant, "--think-cap", cap, "--dry", *extra])

    def test_a_consistent_stand_in_passes(self):
        code, out, err = self.gate()
        self.assertEqual(code, 0, err)
        self.assertIn("perfect packs equal their manifest", out)

    def test_missing_or_bad_run_parameters_are_refused(self):
        for v, c in (("V1C", None), (None, "1500"), ("V9", "1500"), ("V1", "0")):
            code, _, err = run_main(["gate-params", *(["--check-variant", v] if v else []), *(["--think-cap", c] if c else [])])
            self.assertEqual(code, 2, (v, c))
            self.assertIn("REFUSED", err)
        self.assertEqual(run_main(["gate-params", "--check-variant", "none", "--think-cap", "1"])[0], 0)

    def test_perfect_packs_that_do_not_match_their_manifest_are_refused(self):
        self.pf.write_text(self.pf.read_text(encoding="utf-8").replace("c2", "c9"), encoding="utf-8")
        code, _, err = self.gate()
        self.assertEqual(code, 2)
        self.assertIn("pack file's sha256", err)

    def test_an_input_of_the_perfect_packs_that_changed_is_refused(self):
        rows = [json.loads(l) for l in open(self.td / "fw/packs.jsonl")]
        rows[0]["frozen"] = ["c1", "c7"]
        (self.td / "fw/packs.jsonl").write_text("".join(json.dumps(x) + "\n" for x in rows), encoding="utf-8")
        code, _, err = self.gate()
        self.assertEqual(code, 2)
        self.assertIn("input funnel_packs", err)
        self.assertIn("changed since the packs were made", err)

    def test_a_missing_manifest_wrong_kind_or_wrong_scenarios_are_refused(self):
        saved = copy.deepcopy(self.man)
        self.man["variant"] = "unpadded"
        self.write_man()
        self.assertEqual(self.gate()[0], 2)
        self.man = saved
        self.man["inputs_sha256"] = {}
        self.write_man()
        self.assertEqual(self.gate()[0], 2)
        self.man = saved
        self.write_man()
        self.pf.write_text(self.pf.read_text(encoding="utf-8").splitlines()[0] + "\n", encoding="utf-8")
        self.man["pack_file_sha256"] = R.sha256_file(self.pf)
        self.man["count"] = 1
        self.write_man()
        code, _, err = self.gate()
        self.assertEqual(code, 2)
        self.assertIn("exactly the D5 scenarios", err)
        os.remove(str(self.pf) + ".manifest.json")
        self.assertEqual(self.gate()[0], 2)

    def test_d5_files_that_differ_from_the_chunk_check_are_refused(self):
        with open(self.td / "items.jsonl", "a", encoding="utf-8") as f:
            f.write("\n")
        self.assertEqual(self.gate()[0], 2)

    def test_a_real_gate_wants_the_real_d5_size(self):
        code, _, err = self.gate()   # --dry is passed by gate(); without it the synthetic batch is not 50 scenarios
        self.assertEqual(code, 0)
        d = self.td
        code, _, err = run_main(["gate-inputs", "--items", str(d / "items.jsonl"), "--rubrics", str(d / "rubrics.jsonl"), "--chunk-check", str(d / "CHUNK_CHECK.json"),
                                 "--funnel-work", str(d / "fw"), "--perfect", str(self.pf), "--check-variant", "V1", "--think-cap", "100"])
        self.assertEqual(code, 2)
        self.assertIn("50, 269, 227 and 42 expected", err)


class ManifestAndStartCheck(unittest.TestCase):
    def test_a_changed_pinned_input_after_the_manifest_exists_is_refused(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            pin = td / "input.jsonl"
            pin.write_text("one\n", encoding="utf-8")
            argv = lambda sub: [sub, "--manifest", str(td / "MANIFEST.json"), "--work", str(td), "--pin", str(pin), "--frozen", str(ROOT / "kafka_sre_rag/frozen.json"),  # noqa: E731
                                "--opt", "check_variant=V1C", "--opt", "think_cap=1500"]
            self.assertEqual(run_main(argv("start-check"))[0], 0)                  # nothing yet: a fresh start
            self.assertEqual(run_main(argv("manifest"))[0], 0)
            self.assertEqual(run_main(argv("start-check"))[0], 0)
            (td / "stage_x.done").write_text(json.dumps({"stage": "x", "manifest_sha256": "stale", "outputs": {}}), encoding="utf-8")
            code, _, err = run_main(argv("start-check"))
            self.assertEqual(code, 2)                                              # a marker written for another manifest
            self.assertIn("stale", err)
            (td / "stage_x.done").unlink()
            pin.write_text("two\n", encoding="utf-8")
            code, _, err = run_main(argv("start-check"))
            self.assertEqual(code, 2)
            self.assertIn("input.jsonl", err)
            pin.write_text("one\n", encoding="utf-8")
            self.assertEqual(run_main(argv("start-check"))[0], 0)
            changed = argv("start-check")[:-1] + ["think_cap=1501"]               # another thinking cap is another run
            code, _, err = run_main(changed)
            self.assertEqual(code, 2)
            self.assertIn("think_cap", err)

    def test_a_stage_marker_whose_output_changed_is_refused(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            pin = td / "p.txt"
            pin.write_text("x", encoding="utf-8")
            out = td / "out.txt"
            out.write_text("result", encoding="utf-8")
            base = ["--manifest", str(td / "MANIFEST.json"), "--work", str(td), "--pin", str(pin), "--frozen", str(ROOT / "kafka_sre_rag/frozen.json")]
            run_main(["manifest", *base])
            mh = R.G.manifest_hash(json.load(open(td / "MANIFEST.json")))
            (td / "stage_a.done").write_text(json.dumps({"stage": "a", "manifest_sha256": mh, "outputs": {str(out): R.sha256_file(out)}}), encoding="utf-8")
            self.assertEqual(run_main(["start-check", *base])[0], 0)
            out.write_text("changed", encoding="utf-8")
            code, _, err = run_main(["start-check", *base])
            self.assertEqual(code, 2)
            self.assertIn("changed since the stage finished", err)

    def test_the_pins_exist_and_cover_the_new_code(self):
        pins = R.CODE_PINS
        for p in pins:
            self.assertTrue((ROOT / p).is_file(), p)
        for p in ("kafka_sre_rag/verify_pass.py", "kafka_sre_rag/kb_lookup.py", "kafka_sre_rag/generators.py", "scripts/eval2.py", "scripts/m7_four_way.sh", "scripts/m7_six_arm_rule.py", "scripts/eval2_blind.py"):
            self.assertIn(p, pins)
        self.assertNotIn("scripts/m6_d5_answers.sh", pins)


class ScriptTests(unittest.TestCase):
    def sh(self, *argv, cwd=ROOT):
        return subprocess.run(["sh", str(SCRIPT), *argv], cwd=str(cwd), capture_output=True, text=True, timeout=120)

    def test_the_script_has_valid_shell_syntax(self):
        self.assertEqual(subprocess.run(["sh", "-n", str(SCRIPT)], capture_output=True).returncode, 0)

    def test_a_real_run_without_the_run_parameters_is_refused_before_anything_is_written(self):
        before = {p for p in ("runs/m7", "work/m7/six_arm") if (ROOT / p).exists()}
        for argv in ((), ("--check-variant", "V1C"), ("--think-cap", "1500"), ("--check-variant", "V9", "--think-cap", "1500"), ("--check-variant", "V1", "--think-cap", "x")):
            r = self.sh(*argv)
            self.assertEqual(r.returncode, 2, (argv, r.stdout, r.stderr))
            self.assertIn("REFUSED", r.stderr)
            self.assertIn("nothing started", r.stderr)
        self.assertEqual({p for p in ("runs/m7", "work/m7/six_arm") if (ROOT / p).exists()}, before)

    def test_a_real_run_needs_the_sources_of_both_parameters(self):
        r = self.sh("--check-variant", "V1C", "--think-cap", "1500")
        self.assertEqual(r.returncode, 2)
        self.assertIn("--check-variant-source is required", r.stderr)
        r = self.sh("--check-variant", "V1C", "--think-cap", "1500", "--check-variant-source", "pilot report")
        self.assertEqual(r.returncode, 2)
        self.assertIn("--think-cap-source is required", r.stderr)

    def test_a_dry_run_never_uses_the_real_files(self):
        existed = (ROOT / "work/m7/six_arm").exists()
        r = self.sh("--dry-run", "--check-variant", "V1", "--think-cap", "100", "--work", "work/m7/six_arm")
        self.assertEqual(r.returncode, 2)
        self.assertIn("never uses the real", r.stderr)
        self.assertEqual((ROOT / "work/m7/six_arm").exists(), existed)             # a refused dry run writes nothing, not even the folder
        r = self.sh("--dry-run", "--check-variant", "V1", "--think-cap", "100", "--report", "runs/m7/M7_SIX_ARM.md", "--work", tempfile.mkdtemp())
        self.assertEqual(r.returncode, 2)

    def test_a_real_run_needs_the_pilots_report_and_refuses_a_dry_run_record_and_the_plants(self):
        base = ("--check-variant", "V1", "--think-cap", "100", "--check-variant-source", "a", "--think-cap-source", "b")
        r = self.sh(*base, "--pilot-report", "/nonexistent/M7_PILOT.md")
        self.assertEqual(r.returncode, 2)
        self.assertIn("pilot's report", r.stderr)
        td = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, td, True)
        (td / "p.md").write_text("# M7 pilot (DRY RUN: stub model; no number below is a measurement)\n", encoding="utf-8")
        r = self.sh(*base, "--pilot-report", str(td / "p.md"))
        self.assertEqual(r.returncode, 2)
        self.assertIn("dry run", r.stderr)
        r = self.sh(*base, "--plant-cut", "x")
        self.assertEqual(r.returncode, 2)
        self.assertIn("dry runs only", r.stderr)
        text = SCRIPT.read_text(encoding="utf-8")
        for needle in ("pilot-check", "models-check", "server-gate", "--pin $PILOT", "pilot_report=$PILOT", "leak-check", "bodies-relation", "check-errors"):
            self.assertIn(needle, text)

    def test_the_arms_run_in_the_order_d_f_a_c_e(self):
        text = SCRIPT.read_text(encoding="utf-8")
        pos = [text.index(f"if ! stage_done {st}; then") if st != "ans_c" else text.index("&& ! stage_done ans_c") for st in ("ans_d", "ans_f", "ans_a", "ans_c", "ans_e")]
        self.assertEqual(pos, sorted(pos))
        self.assertIn("ECHOREF=$W/ans_d5_D.jsonl.manifest.json", text)

    def test_fail_after_is_for_dry_runs_only(self):
        r = self.sh("--check-variant", "V1", "--think-cap", "100", "--check-variant-source", "a", "--think-cap-source", "b", "--fail-after", "ans_a")
        self.assertEqual(r.returncode, 2)
        self.assertIn("dry runs only", r.stderr)


class SmallChangesToExistingCode(unittest.TestCase):
    """--think-cap and arm A's switches in eval2.py; the request-body hash in the generators' call info."""

    def test_body_hash_equals_the_hash_of_the_body_that_was_sent_and_changes_nothing_sent(self):
        sent = []

        def respond(req):
            sent.append(req.content)
            if json.loads(req.content).get("stream"):
                return httpx.Response(200, text='data: {"model": "m1", "choices": [{"delta": {"content": "A"}, "finish_reason": "stop"}]}\n\ndata: [DONE]\n\n')
            return httpx.Response(200, json={"model": "m1", "choices": [{"message": {"content": "A"}, "finish_reason": "stop"}], "usage": {"completion_tokens": 1}})

        def gen():
            g = GEN.OpenAIChatGenerator("http://example.invalid/v1", "m1", api_key="k", extra_body={"chat_template_kwargs": {"enable_thinking": False}})
            g.client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(404) if req.url.path.endswith("/models") else respond(req)))
            return g
        msgs = [{"role": "user", "content": "q"}]
        for on_token in (None, lambda s: None):
            sent.clear()
            info, noinfo = {}, None
            gen().chat(msgs, 50, on_token, thinking=True, info=info)
            gen().chat(msgs, 50, on_token, thinking=True)
            self.assertEqual(sent[0], sent[1])                                          # the info changes nothing that is sent
            self.assertEqual(info["request_body_sha256"], GEN.body_sha256(json.loads(sent[0])))
            self.assertEqual(len(info["request_body_sha256"]), 64)
        a, b = {}, {}
        gen().chat(msgs, 50, None, thinking=True, info=a)
        gen().chat(msgs, 50, None, thinking=False, info=b)
        self.assertNotEqual(a["request_body_sha256"], b["request_body_sha256"])       # the switch is in the body, so in the hash

    def test_eval2_think_cap_and_both_switches_off(self):
        import argparse
        import eval2
        ns = argparse.Namespace(answer_profile="thorough", no_thinking=True, no_verify=True, no_cards=False, think_cap=None)
        p = eval2.thorough_profile(ns)
        self.assertEqual((p.thinking, p.verify), (False, False))                         # arm A: allowed in the harness
        ns = argparse.Namespace(answer_profile="thorough", no_thinking=False, no_verify=False, no_cards=True, think_cap=700)
        p = eval2.thorough_profile(ns)
        self.assertEqual((p.thinking, p.verify, p.cards, p.thinking_tokens), (True, True, False, 700))
        self.assertEqual(p.manifest()["thinking_tokens"], 700)
        ns.think_cap = None
        self.assertEqual(eval2.thorough_profile(ns).thinking_tokens, 1500)               # the profile's own default stays

    def test_eval2_answer_with_both_switches_off_and_with_a_cap(self):
        from test_eval2 import Base, item
        from test_thorough_profile import Gen, OFF
        from test_ask_check import make_catalogue_dir
        import eval2

        class T(Base):
            def runTest(self):
                pass
        t = T()
        t.setUp()
        try:
            cat = make_catalogue_dir()
            f, out = t.items_file([item(1)]), t.tmp / "a.jsonl"
            g = Gen(draft="Raise `max.poll.interval.ms` on the consumer.")
            rc, text = t.run_answer(f, out, "--answer-profile", "thorough", "--no-thinking", "--no-verify", "--catalogue", str(cat), *OFF, gen=g)
            self.assertEqual(rc, 0, text)
            r = eval2.read_jsonl(out)[0]
            self.assertIsNone(r["error"])
            self.assertEqual((r["draft_streamed"], r["cut_while_thinking"], r["thorough"]), (True, False, None))
            self.assertEqual(r["text"], r["first_pass_text"])
            self.assertEqual(len(g.calls), 2)                                           # the search terms, then the draft: no check call
            self.assertIs(g.calls[-1]["kw"]["thinking"], False)
            m = json.loads(eval2.manifest_path(out).read_text())
            self.assertEqual((m["answer_profile"]["thinking"], m["answer_profile"]["verify"], m["answer_profile"]["thinking_tokens"]), (False, False, 0))
            # a cap: the draft's token limit is the answer limit plus the cap, recorded in the manifest and the row
            out2 = t.tmp / "b.jsonl"
            g2 = Gen(draft="Raise `max.poll.interval.ms` on the consumer.")
            rc, text = t.run_answer(f, out2, "--answer-profile", "thorough", "--no-verify", "--think-cap", "321", "--catalogue", str(cat), *OFF, gen=g2)
            self.assertEqual(rc, 0, text)
            r2 = eval2.read_jsonl(out2)[0]
            base = r["tokens_detail"]["draft_max_tokens"]
            self.assertEqual(r2["tokens_detail"]["draft_max_tokens"], base + 321)
            self.assertEqual(json.loads(eval2.manifest_path(out2).read_text())["answer_profile"]["thinking_tokens"], 321)
            # --think-cap without the profile is refused
            with self.assertRaises(eval2.EvalError):
                t.run_answer(f, t.tmp / "c.jsonl", "--think-cap", "300")
            shutil.rmtree(cat, True)
        finally:
            t.tearDown()


class ChecksOfTheShellScript(unittest.TestCase):
    """Every check the shell script makes, run with a planted mismatch (review: a removed check must fail a default test)."""

    def setUp(self):
        self.td = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.td, True)

    def rows(self, name, rows, manifest=None):
        p = self.td / name
        p.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        if manifest is not None:
            Path(str(p) + ".manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return p

    def g(self, *argv):
        r = subprocess.run([sys.executable, str(ROOT / "scripts/m5_g_rule.py"), *argv], cwd=str(ROOT), capture_output=True, text=True, timeout=60)
        return r.returncode

    # --- the commands of m5_g_rule.py the chain calls
    def test_check_answers_packs_equal_and_check_echo_fail_on_a_planted_mismatch(self):
        good = self.rows("a.jsonl", [{"id": "1", "text": "t", "pack": ["x"], "error": None}], {"mode": "default", "ended": "t", "model_echoed": "m"})
        self.assertEqual(self.g("check-answers", "--file", str(good), "--expect", "1", "--mode", "default"), 0)
        self.assertEqual(self.g("check-answers", "--file", str(good), "--expect", "2", "--mode", "default"), 1)         # a row missing
        self.assertEqual(self.g("check-answers", "--file", str(good), "--expect", "1", "--mode", "closed"), 1)          # another mode
        empty = self.rows("e.jsonl", [{"id": "1", "text": " ", "pack": ["x"], "error": None}], {"mode": "default", "ended": "t"})
        self.assertEqual(self.g("check-answers", "--file", str(empty), "--expect", "1", "--mode", "default"), 1)        # an empty answer
        packs = self.rows("p.jsonl", [{"id": "1", "pack": ["x"]}])
        self.assertEqual(self.g("packs-equal", "--a", str(packs), "--b", str(good)), 0)
        other = self.rows("o.jsonl", [{"id": "1", "pack": ["y"]}])
        self.assertEqual(self.g("packs-equal", "--a", str(other), "--b", str(good)), 1)                                 # the replay used another pack
        m2 = self.td / "m2.json"
        m2.write_text(json.dumps({"model_echoed": "other"}))
        self.assertEqual(self.g("check-echo", "--stored", str(good) + ".manifest.json", "--new", str(good) + ".manifest.json"), 0)
        self.assertEqual(self.g("check-echo", "--stored", str(good) + ".manifest.json", "--new", str(m2)), 1)           # another model echoed

    # --- the leak check of the bundles
    def test_leak_check_names_a_planted_configuration(self):
        d = self.td / "first"
        (d / "excerpts").mkdir(parents=True)
        (d / "s-1.md").write_text("# Item s-1\n\nAn answer about consumer lag.\n", encoding="utf-8")
        self.assertEqual(run_main(["leak-check", "--dir", str(d)])[0], 0)
        for planted in ("Ask again, or without --answer-profile.", "thorough profile, EXPERIMENTAL", R.NO_DRAFT, "--think-cap 600", "enable_thinking"):
            with self.subTest(planted=planted):
                (d / "excerpts" / "x.md").write_text("text " + planted, encoding="utf-8")
                code, out, _ = run_main(["leak-check", "--dir", str(d)])
                self.assertEqual(code, 1)
                self.assertIn("names a configuration", out)

    def test_a_no_draft_row_is_left_out_of_the_bundles_and_recorded_in_the_key(self):
        a = self.rows("A.jsonl", [{"id": "s-1", "text": "fine"}, {"id": "s-2", "text": "fine"}])
        d = self.rows("D.jsonl", [{"id": "s-1", "text": "fine"}, {"id": "s-2", "text": R.NO_DRAFT, "cut_while_thinking": True, "truncated": True}])
        key = self.td / "KEY.json"
        key.write_text(json.dumps({"items": ["s-1"], "aid": {}}))
        code, out, err = run_main(["no-draft-ids", "--answers", f"A={a}", f"D={d}", "--key", str(key)])
        self.assertEqual(code, 0)
        self.assertEqual(out.split(), ["s-2"])
        k = json.load(open(key))
        self.assertEqual(k["no_draft"], {"A": [], "D": ["s-2"]})
        self.assertEqual(k["left_out_of_the_bundles"], ["s-2"])

    # --- the request bodies of two arms' drafts
    def parts(self, think, max_tokens, messages="m", rest="r"):
        return {"draft_info": {"request_body_parts": {"messages_sha256": messages, "max_tokens": max_tokens, "enable_thinking": think, "rest_sha256": rest}}}

    def test_bodies_relation_fails_on_each_planted_difference(self):
        a = self.rows("A.jsonl", [{"id": "1", "pack": ["x"], **self.parts(False, 2048)}])
        ok = self.rows("D.jsonl", [{"id": "1", "pack": ["x"], **self.parts(True, 2648)}])
        base = ["bodies-relation", "--a", str(a), "--thinking-differs", "--think-cap", "600", "--b"]
        self.assertEqual(run_main(base + [str(ok)])[0], 0)
        planted = {"another rest of the body": self.parts(True, 2648, rest="r2"), "another message on the same pack": self.parts(True, 2648, messages="m2"),
                   "thinking not switched on": self.parts(False, 2648), "another token limit": self.parts(True, 2600)}
        for name, parts in planted.items():
            with self.subTest(planted=name):
                f = self.rows("x.jsonl", [{"id": "1", "pack": ["x"], **parts}])
                code, out, _ = run_main(base + [str(f)])
                self.assertEqual(code, 1, out)
        f = self.rows("none.jsonl", [{"id": "1", "pack": ["x"], "draft_info": {}}])
        self.assertEqual(run_main(base + [str(f)])[0], 1)                                  # no recorded parts is not a pass
        # A against E (no thinking difference; other pack, other messages): the messages may differ only where the packs differ
        e = self.rows("E.jsonl", [{"id": "1", "pack": ["y"], **self.parts(False, 2048, messages="m9")}])
        self.assertEqual(run_main(["bodies-relation", "--a", str(a), "--b", str(e)])[0], 0)
        e2 = self.rows("E2.jsonl", [{"id": "1", "pack": ["y"], **self.parts(False, 2048)}])  # another pack but the same messages
        self.assertEqual(run_main(["bodies-relation", "--a", str(a), "--b", str(e2)])[0], 1)
        e3 = self.rows("E3.jsonl", [{"id": "1", "pack": ["y"], **self.parts(True, 2048, messages="m9")}])
        self.assertEqual(run_main(["bodies-relation", "--a", str(a), "--b", str(e3)])[0], 1)

    def test_body_parts_differ_only_where_the_arms_differ(self):
        mk = lambda think, mt, msgs=("q",): GEN.OpenAIChatGenerator("http://example.invalid/v1", "m1", api_key="k", extra_body={"chat_template_kwargs": {"enable_thinking": False, "z": 1}}).request_body(  # noqa: E731
            [{"role": "user", "content": x} for x in msgs], mt, think)
        a, d = GEN.body_parts(mk(False, 2048)), GEN.body_parts(mk(True, 2648))
        self.assertEqual(a["rest_sha256"], d["rest_sha256"])
        self.assertEqual(a["messages_sha256"], d["messages_sha256"])
        self.assertEqual((a["enable_thinking"], d["enable_thinking"], a["max_tokens"], d["max_tokens"]), (False, True, 2048, 2648))
        self.assertNotEqual(a["messages_sha256"], GEN.body_parts(mk(False, 2048, ("other",)))["messages_sha256"])
        other_extra = GEN.OpenAIChatGenerator("http://example.invalid/v1", "m1", api_key="k", extra_body={"chat_template_kwargs": {"enable_thinking": False, "z": 2}}).request_body(
            [{"role": "user", "content": "q"}], 2048, False)
        self.assertNotEqual(a["rest_sha256"], GEN.body_parts(other_extra)["rest_sha256"])

    # --- the pilot's report, the models and the server
    def test_pilot_check(self):
        self.assertEqual(run_main(["pilot-check", "--file", str(self.td / "none.md")])[0], 2)           # missing
        (self.td / "e.md").write_text("", encoding="utf-8")
        self.assertEqual(run_main(["pilot-check", "--file", str(self.td / "e.md")])[0], 2)              # empty
        (self.td / "d.md").write_text("# M7 pilot: thinking (DRY RUN: stub model; no number below is a measurement)\n", encoding="utf-8")
        code, _, err = run_main(["pilot-check", "--file", str(self.td / "d.md")])
        self.assertEqual(code, 2)
        self.assertIn("dry run", err)
        (self.td / "r.md").write_text("# M7 pilot: thinking, and the checking pass on stored drafts\n\nResult.\n", encoding="utf-8")
        self.assertEqual(run_main(["pilot-check", "--file", str(self.td / "r.md")])[0], 0)
        self.assertEqual(run_main(["pilot-check", "--file", str(self.td / "none.md"), "--dry"])[0], 0)

    def test_models_check_names_a_model_the_server_does_not_list(self):
        f = self.td / "models.json"
        f.write_text(json.dumps({"data": [{"id": "Qwen-4bit"}, {"id": "Judge-1"}]}))
        self.assertEqual(run_main(["models-check", "--models", str(f), "--need", "Qwen-4bit", "Judge-1"])[0], 0)
        code, _, err = run_main(["models-check", "--models", str(f), "--need", "Qwen-4bit", "Judge-2"])
        self.assertEqual(code, 2)
        self.assertIn("Judge-2", err)

    def test_server_gate_never_unloads_another_clients_model(self):
        st = self.td / "status.json"
        base = ["server-gate", "--status", str(st), "--keep", "Q4", "--ours", "Q4", "Judge"]
        st.write_text(json.dumps({"active_requests": 0, "waiting_requests": 0, "loaded_models": []}))
        self.assertEqual(run_main(base)[0], 0)
        st.write_text(json.dumps({"active_requests": 0, "waiting_requests": 0, "loaded_models": ["Q4"]}))
        self.assertEqual(run_main(base), (0, "", ""))
        st.write_text(json.dumps({"active_requests": 0, "waiting_requests": 0, "loaded_models": ["Judge"]}))
        code, out, _ = run_main(base)
        self.assertEqual((code, out.strip()), (0, "UNLOAD Judge"))                            # our own judge model is unloaded
        st.write_text(json.dumps({"active_requests": 0, "waiting_requests": 0, "loaded_models": ["Q4", "GLM-other"]}))
        code, out, err = run_main(base)
        self.assertEqual(code, 2)
        self.assertIn("GLM-other", err)                                                       # the message names the model
        self.assertNotIn("UNLOAD", out)                                                       # and nothing is unloaded
        st.write_text(json.dumps({"active_requests": 1, "waiting_requests": 0, "loaded_models": ["Q4"]}))
        self.assertEqual(run_main(base)[0], 2)
        st.write_text(json.dumps({"active_requests": 0, "waiting_requests": 2, "loaded_models": []}))
        self.assertEqual(run_main(base)[0], 2)
        st.write_text("not json")
        self.assertEqual(run_main(base)[0], 2)
        self.assertNotIn("--unload-others", subprocess.run(["sh", str(SCRIPT), "--help-nothing"], capture_output=True, text=True).stderr)

    # --- redoing a failed checking call
    def test_check_errors_counts_and_drops_rows_whose_check_failed(self):
        rows = [{"id": "1", "thorough": {"r2": {"status": "error"}}}, {"id": "2", "thorough": {"r2": {"status": "no_patch"}}}, {"id": "3", "thorough": None}]
        f = self.rows("D.jsonl", rows)
        code, out, _ = run_main(["check-errors", "--file", str(f), "--kind", "answers"])
        self.assertEqual(out.strip(), "check_errors=1")
        run_main(["check-errors", "--file", str(f), "--kind", "answers", "--drop"])
        self.assertEqual([r["id"] for r in R.rd(f)], ["2", "3"])
        self.assertEqual([r["id"] for r in R.rd(str(f) + ".redo_dropped.jsonl")], ["1"])      # kept, never deleted
        v = self.rows("C.jsonl", [{"id": "1", "r2": {"status": "error"}}, {"id": "2", "r2": {"status": "accepted"}}])
        self.assertEqual(run_main(["check-errors", "--file", str(v), "--kind", "verify"])[1].strip(), "check_errors=1")

    def test_check_verify_wants_every_record_without_an_error(self):
        a = self.rows("A.jsonl", [{"id": "1"}, {"id": "2"}])
        c = self.rows("c.jsonl", [{"id": "1", "error": None}, {"id": "2", "error": None}])
        self.assertEqual(run_main(["check-verify", "--file", str(c), "--answers", str(a), "--expect", "2"])[0], 0)
        self.assertEqual(run_main(["check-verify", "--file", str(c), "--answers", str(a), "--expect", "3"])[0], 1)
        self.assertEqual(run_main(["check-verify", "--file", str(self.td / "none.jsonl"), "--answers", str(a), "--expect", "2"])[0], 1)
        bad = self.rows("b.jsonl", [{"id": "1", "error": None}, {"id": "2", "error": "boom"}])
        self.assertEqual(run_main(["check-verify", "--file", str(bad), "--answers", str(a), "--expect", "2"])[0], 1)
        missing = self.rows("m.jsonl", [{"id": "1", "error": None}, {"id": "9", "error": None}])
        self.assertEqual(run_main(["check-verify", "--file", str(missing), "--answers", str(a), "--expect", "2"])[0], 1)

    def test_reading_plan_has_13_first_and_4_second_sessions_for_50_scenarios(self):
        ids = [f"s-{i:02d}" for i in range(50)]
        key = self.td / "KEY.json"
        key.write_text(json.dumps({"items": ids, "second_reader_ids": ids[:13]}))
        out = self.td / "sessions.json"
        self.assertEqual(run_main(["reading-plan", "--key", str(key), "--out", str(out)])[0], 0)
        plan = json.load(open(out))
        self.assertEqual((len(plan["first_reader_sessions"]), len(plan["second_reader_sessions"])), (13, 4))
        self.assertEqual(sorted(i for s_ in plan["first_reader_sessions"] for i in s_), ids)
        self.assertEqual(max(len(s_) for s_ in plan["first_reader_sessions"]), 4)

    def test_prompts_default_fails_on_a_planted_prompt(self):
        from unittest import mock
        import eval2
        from kafka_sre_rag import evalhooks, prompts
        from test_verify_pass import CHUNKS
        item = {"id": "s-1", "question": "Why is lag growing?"}
        items = self.rows("items.jsonl", [item])
        with mock.patch.object(evalhooks, "chunks_by_ids", lambda ids: [c for i in ids for c in CHUNKS if c["id"] == i]):
            pack = [CHUNKS[0]["id"]]
            h = eval2.messages_fingerprint(prompts.answer_messages(eval2.full_question(item), CHUNKS[:1]))[0]
            good = self.rows("good.jsonl", [{"id": "s-1", "pack": pack, "prompt_sha256": h}])
            bad = self.rows("bad.jsonl", [{"id": "s-1", "pack": pack, "prompt_sha256": "0" * 64}])
            self.assertEqual(run_main(["prompts-default", "--items", str(items), "--answers", str(good)])[0], 0)
            self.assertEqual(run_main(["prompts-default", "--items", str(items), "--answers", str(bad)])[0], 1)


class MeasuresOfTheChain(unittest.TestCase):
    def test_a_cut_while_thinking_row_counts_as_cut_and_empty_and_has_no_answer_tokens(self):
        rows = [{"id": "1", "text": "fine", "truncated": False}, {"id": "2", "text": R.NO_DRAFT, "truncated": True, "cut_while_thinking": True},
                {"id": "3", "text": "long answer cut at the limit", "truncated": True}, {"id": "4", "text": "", "truncated": False}]
        c = R.row_counts(rows)
        self.assertEqual((c["cut"], c["cut_while_thinking"], c["empty"], c["no_draft_ids"]), (2, 1, 2, ["2"]))
        self.assertTrue(R.is_no_draft(rows[1]))
        self.assertIsNone(R.answer_tokens(rows[1]))
        # the empty count follows the text of verify_pass.NO_DRAFT, imported, not a copy
        self.assertTrue(R.is_no_draft({"id": "5", "text": R.NO_DRAFT}))
        self.assertFalse(R.is_no_draft({"id": "6", "text": "No answer was found in the excerpts."}))

    def test_answer_tokens(self):
        self.assertEqual(R.answer_tokens({"tokens_detail": {"draft": 1500}, "draft_info": {"reasoning_tokens": 400}}), 1100)             # the server's own counts
        self.assertEqual(R.answer_tokens({"tokens_detail": {"draft": 1500}, "draft_info": {"answer_chunks": 1000}}), 1000)               # the chunk count
        self.assertEqual(R.answer_tokens({"tokens_detail": {"draft": 2048}, "draft_info": {}}), 2048)                                    # an arm without thinking
        self.assertEqual(R.answer_tokens({"tokens_detail": {"draft": 3000}, "draft_info": {"answer_cap_hit": True, "answer_chunks": 2048}}), 2048)
        self.assertIsNone(R.answer_tokens({"tokens_detail": {}, "draft_info": {}}))

    def test_arm_time_adds_search_retrieval_draft_and_check_and_needs_the_draft_seconds(self):
        rows = [{"id": "a", "seconds": {"draft": 40.0, "thinking": 10.0, "check": 5.0}}, {"id": "b", "seconds": {"draft": 20.0, "check": 0.0}}]
        t = R.arm_time(rows, {"a": 6.0, "b": 8.0}, 2.0)
        self.assertEqual(t["median_s"], statistics_median([6.0 + 2.0 + 40.0 + 5.0, 8.0 + 2.0 + 20.0]))
        self.assertEqual((t["draft_median_s"], t["thinking_median_s"], t["check_median_s"], t["checked"]), (30.0, 10.0, 2.5, 1))
        with self.assertRaises(R.SixError):
            R.arm_time([{"id": "a", "seconds": {}}], {"a": 1.0}, 1.0)

    def test_judged_points_split_and_the_unverified_verdict(self):
        rub = [{"id": "s-1", "must": [{"id": "p1", "chunk_sets": [["c1"]]}, {"id": "p2", "chunk_sets": []}, {"id": "p3", "chunk_sets": [["c2"]]}]}]
        res = [{"id": "s-1", "set": "A", "valid_reply": True, "reference_given": False,
                "must": {"p1": {"verdict": "present"}, "p2": {"verdict": "absent"}, "p3": {"verdict": "absent", "quote_ok": False, "judge_verdict": "present"}}},
               {"id": "s-1", "set": "A", "valid_reply": True, "reference_given": False,
                "must": {"p1": {"verdict": "present"}, "p2": {"verdict": "present"}, "p3": {"verdict": "absent"}}},
               {"id": "s-1", "set": "A", "valid_reply": False, "reference_given": False, "must": {"p1": {"verdict": "present"}, "p2": {"verdict": "present"}, "p3": {"verdict": "present"}}},
               {"id": "s-1", "set": "B", "valid_reply": True, "reference_given": False, "must": {"p1": {"verdict": "present"}, "p2": {"verdict": "present"}, "p3": {"verdict": "present"}}}]
        # pass 1: with a covering set 1 of 2 present (p3 is "unverified", not present), without 0 of 1; pass 2: with 1 of 2, without 1 of 1; invalid and other-set rows left out
        self.assertEqual(R.judged_points(res, rub, "A"), {"s-1": (1.0, 2.0, 0.5, 1.0)})
        self.assertEqual(R.judged_points(res, rub, "B"), {"s-1": (2.0, 2.0, 1.0, 1.0)})

    @unittest.skipUnless((ROOT / "eval/m6/d5/rubrics.jsonl").is_file(), "D5 rubrics not in this tree")
    def test_the_d5_points_split_269_227_42(self):
        self.assertEqual(R.point_counts(R.rd(ROOT / "eval/m6/d5/rubrics.jsonl")), (269, 227, 42))

    def test_mech_arm_leaves_out_the_lookup_kinds(self):
        td = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, td, True)
        ids = {"M1": ["a1", "a2"], "M2": ["b1"], "M3": ["c1"]}
        kinds = {"a1": "name_setting", "a2": "other", "b1": "is_real", "c1": "x"}
        mk = lambda name, rows: (td / name).write_text("".join(json.dumps({"format_ok": True, "outcome": "answered", **r}) + "\n" for r in rows), encoding="utf-8")  # noqa: E731
        mk("score_ma_D.jsonl", [{"id": "a1", "all_pass": True}, {"id": "a2", "all_pass": False}, {"id": "b1", "all_pass": True}])
        mk("score_m3_D.jsonl", [{"id": "c1", "all_pass": True}])
        out = R.mech_arm(td, "D", ids, kinds)
        self.assertEqual((out["n"], out["pass_all"], out["n_without_lookup"], out["pass_without_lookup"]), (4, 3, 2, 1))
        self.assertEqual(set(R.LOOKUP_KINDS), {"name_setting", "error_name", "is_real", "version_trap", "connect_rest"})

    def test_the_1_96_intervals_of_b_against_c(self):
        mk = lambda P: {"P_by_id": dict(P)}  # noqa: E731
        m = {"arms": {"A": mk({"1": 0.2, "2": 0.4, "3": 0.6}), "B": mk({"1": 0.3, "2": 0.5, "3": 0.9}), "C": mk({"1": 0.2, "2": 0.5, "3": 0.6})}}
        p = R.pair(m, "B", "A")
        d = [0.1, 0.1, 0.3]
        mean_ = sum(d) / 3
        se_ = R.se(d)
        self.assertAlmostEqual(p["ci196"][0], mean_ - 1.96 * se_)
        self.assertAlmostEqual(p["ci196"][1], mean_ + 1.96 * se_)
        self.assertEqual(R.Z90_2, 1.96)
        self.assertEqual(R.pair(m, "B", "C")["n"], 3)
        full = lambda **kw: {"P": 0.5, "P_by_id": {}, "points": {k: {"present": 1.0, "of": 2} for k in ("all", "with_set", "without_set")}, "cut": 0, "cut_while_thinking": 0, "empty": 0,  # noqa: E731
                             "idents_per10": 1.0, "time": {"median_s": 1.0, "p90_s": 1.0, "draft_median_s": 1.0, "thinking_median_s": None, "check_median_s": 0.0}, "check": {"status": {}}, **kw}
        mm = {"stub": True, "variant": "V1C", "think_cap": 600, "sizes": {"D5": 8, "points": 35, "points_with_set": 24, "points_without_set": 11},
              "retrieval_frozen_median_s": 1.0, "retrieval_timing_n": 20, "arms": {"A": full(), "B": full()},
              "pairs": {"B-A": {"mean": 0.1, "se": 0.05, "n": 8, "ci196": [0.002, 0.198]}, "C-A": {"mean": 0.0, "se": 0.04, "n": 8, "ci196": [-0.078, 0.078]}, "B-C": {"mean": 0.1, "se": 0.03, "n": 8, "ci196": [0.04, 0.16]}},
              "mech": {}}
        md = R.measures_md(mm)
        self.assertIn("P (8 answers, 2 passes)", md)                 # the size comes from the data, not from a constant
        self.assertIn("B-A: +0.100, interval [+0.002, +0.198]", md)
        self.assertIn("B-C: +0.100, interval [+0.040, +0.160]", md)
        self.assertIn("0.025 each side", md)


def statistics_median(xs):
    import statistics
    return statistics.median(xs)


class AnswerCap(unittest.TestCase):
    """K2: the ANSWER part of a thinking draft is stopped at the answer limit, exactly as a draft without thinking is cut; the thinking part is limited as before."""

    @staticmethod
    def stream(events):
        body = "".join("data: %s\n\n" % json.dumps(dict(e, model="m1")) for e in events) + "data: [DONE]\n\n"
        return httpx.Response(200, text=body)

    def gen(self, events, sent=None):
        def respond(req):
            if req.url.path.endswith("/models"):
                return httpx.Response(404)
            if sent is not None:
                sent.append(req.content)
            return self.stream(events)
        g = GEN.OpenAIChatGenerator("http://example.invalid/v1", "m1", api_key="k", extra_body={"chat_template_kwargs": {"enable_thinking": False}})
        g.client = httpx.Client(transport=httpx.MockTransport(respond))
        return g

    MSGS = [{"role": "user", "content": "q"}]

    def test_a_long_answer_after_short_thinking_is_cut_at_2048_answer_tokens(self):
        events = [{"choices": [{"delta": {"reasoning_content": "t"}}]} for _ in range(20)]
        events += [{"choices": [{"delta": {"content": "w%d " % i}}]} for i in range(3000)]
        events += [{"choices": [{"delta": {}, "finish_reason": "stop"}]}]
        shown, info = [], {}
        text, toks, trunc = self.gen(events).chat(self.MSGS, 2048 + 1500, shown.append, thinking=True, info=info, answer_cap=2048)
        self.assertEqual(len(shown), 2048)
        self.assertEqual(text.split()[-1], "w2047")
        self.assertTrue(trunc)                                              # marked cut, as a draft cut by max_tokens is
        self.assertEqual((info["finish_reason"], info["answer_chunks"], info["answer_cap_hit"], info["cut_while_thinking"]), ("length", 2048, True, False))
        self.assertEqual(toks, 2048 + 20)                                   # no usage arrived: the chunks read stand in for it

    def test_the_same_stream_without_the_cap_is_read_to_its_end(self):
        events = [{"choices": [{"delta": {"reasoning_content": "t"}}]}] + [{"choices": [{"delta": {"content": "w "}}]} for _ in range(100)] + [{"choices": [{"delta": {}, "finish_reason": "stop"}]}]
        info = {}
        text, _, trunc = self.gen(events).chat(self.MSGS, 3000, lambda s: None, thinking=True, info=info)
        self.assertEqual((len(text.split()), trunc, info["answer_chunks"], info["answer_cap_hit"]), (100, False, 100, False))

    def test_an_answer_under_the_cap_is_not_cut_and_the_cap_does_not_count_the_thinking(self):
        events = [{"choices": [{"delta": {"reasoning_content": "t"}}]} for _ in range(30)] + [{"choices": [{"delta": {"content": "w "}}]} for _ in range(5)]
        events += [{"choices": [{"delta": {}, "finish_reason": "stop"}]}]
        info = {}
        text, _, trunc = self.gen(events).chat(self.MSGS, 3000, lambda s: None, thinking=True, info=info, answer_cap=10)
        self.assertEqual((len(text.split()), trunc, info["answer_cap_hit"]), (5, False, False))

    def test_inline_think_block_counts_after_the_closing_tag_only(self):
        events = [{"choices": [{"delta": {"content": "<think>"}}]}] + [{"choices": [{"delta": {"content": "x "}}]} for _ in range(50)]
        events += [{"choices": [{"delta": {"content": "</think>"}}]}] + [{"choices": [{"delta": {"content": "a "}}]} for _ in range(40)]
        info = {}
        text, _, trunc = self.gen(events).chat(self.MSGS, 3000, lambda s: None, thinking=True, info=info, answer_cap=10)
        self.assertTrue(trunc)
        self.assertEqual(info["answer_chunks"], 10)
        self.assertEqual(text.split(), ["a"] * 9)                           # the chunk with the tag is the first of the ten and is no text

    def test_thinking_that_never_ends_is_cut_while_thinking_as_before(self):
        events = [{"choices": [{"delta": {"reasoning_content": "t"}}]} for _ in range(40)] + [{"choices": [{"delta": {}, "finish_reason": "length"}]}]
        info = {}
        with self.assertRaises(GEN.EmptyAnswerError):
            self.gen(events).chat(self.MSGS, 30, lambda s: None, thinking=True, info=info, answer_cap=10)
        self.assertTrue(info["cut_while_thinking"])
        self.assertFalse(info["answer_cap_hit"])

    def test_the_cap_changes_nothing_that_is_sent_and_a_call_without_thinking_is_unchanged_byte_for_byte(self):
        events = [{"choices": [{"delta": {"content": "w "}}]} for _ in range(30)] + [{"choices": [{"delta": {}, "finish_reason": "stop"}]}]
        s1, s2, s3 = [], [], []
        i1, i3 = {}, {}
        r1 = self.gen(events, s1).chat(self.MSGS, 2048, lambda s: None, thinking=False, info=i1)
        r2 = self.gen(events, s2).chat(self.MSGS, 2048, lambda s: None, thinking=False, info={}, answer_cap=2048)
        self.assertEqual(r1, r2)
        self.assertEqual(s1, s2)
        r3 = self.gen(events, s3).chat(self.MSGS, 2048, lambda s: None, thinking=True, info=i3, answer_cap=2048)
        self.assertEqual(r3[0], r1[0])
        self.assertEqual(GEN.body_parts(json.loads(s3[0]))["rest_sha256"], GEN.body_parts(json.loads(s1[0]))["rest_sha256"])
        self.assertEqual(i1["request_body_parts"]["max_tokens"], 2048)

    def test_the_profile_passes_the_cap_to_a_thinking_draft_only(self):
        from kafka_sre_rag import verify_pass as vp
        from test_thorough_profile import Gen
        from test_verify_pass import CHUNKS, Q
        g = Gen()
        vp.draft_and_check(Q, [{"role": "user", "content": "q"}], CHUNKS, g, 2048, vp.Profile(thinking=True, verify=False, thinking_tokens=600), None)
        self.assertEqual(g.calls[-1]["kw"]["answer_cap"], 2048)
        self.assertEqual(g.calls[-1]["max_tokens"], 2048 + 600)
        g = Gen()
        vp.draft_and_check(Q, [{"role": "user", "content": "q"}], CHUNKS, g, 2048, vp.Profile(thinking=False, verify=False), None)
        self.assertNotIn("answer_cap", g.calls[-1]["kw"])                   # arm A: the call is as before
        self.assertEqual(g.calls[-1]["max_tokens"], 2048)

    def test_time_to_first_token_and_cached_prompt_tokens_are_recorded(self):
        events = [{"choices": [{"delta": {"content": "A"}}]}, {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                  {"choices": [], "usage": {"completion_tokens": 1, "prompt_tokens": 900, "prompt_tokens_details": {"cached_tokens": 850}}}]
        info = {}
        self.gen(events).chat(self.MSGS, 50, lambda s: None, thinking=False, info=info)
        self.assertEqual(info["cached_prompt_tokens"], 850)
        self.assertGreaterEqual(info["ttft_s"], 0)
        info2 = {}
        self.gen([{"choices": [{"delta": {"content": "A"}, "finish_reason": "stop"}]}]).chat(self.MSGS, 50, lambda s: None, thinking=False, info=info2)
        self.assertIsNone(info2["cached_prompt_tokens"])                    # the server did not report it

    def test_mlx_generator_cap_counts_tokens_after_the_close_tag(self):
        class Tok:
            def apply_chat_template(self, messages, add_generation_prompt, tokenize, enable_thinking):
                return "P"

        class Piece:
            def __init__(self, text, n, finish=None):
                self.text, self.generation_tokens, self.finish_reason, self.prompt_tokens = text, n, finish, 5

        pieces = [Piece("<think>", 1)] + [Piece("x", 1 + i) for i in range(1, 20)] + [Piece("</think>", 21)] + [Piece("a ", 21 + i) for i in range(1, 100)]
        g = object.__new__(GEN.MLXGenerator)
        g.tok, g.model, g._stream = Tok(), None, lambda *a, **k: iter(pieces)
        text, toks, trunc = g.chat([{"role": "user", "content": "q"}], 3000, thinking=True, answer_cap=10)
        self.assertTrue(trunc)
        self.assertEqual(len(text.split()), 10)
        self.assertEqual(toks, 31)


@unittest.skipUnless(os.environ.get("M7_SIX_E2E") == "1", "opt-in: about 1.5 minutes per variant (M7_SIX_E2E=1)")
class EndToEndDryRun(unittest.TestCase):
    def run_dry(self, variant, *extra):
        td = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, td, True)
        r = subprocess.run(["sh", str(SCRIPT), "--dry-run", "--work", td + "/w", "--check-variant", variant, "--think-cap", "600", *extra], cwd=str(ROOT), capture_output=True, text=True, timeout=1500)
        return td, r

    def test_every_variant_runs_to_the_last_line(self):
        for variant in ("V1", "V1C", "none"):
            with self.subTest(variant=variant):
                td, r = self.run_dry(variant)
                self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])
                self.assertRegex(r.stdout.strip().splitlines()[-1], r"^M7 SIX-ARM TEST DONE: knowledge-base row: \S+; thorough-profile row: WAITS FOR THE READING")
                self.assertIn("thorough-profile row: WAITS FOR THE READING", r.stdout)
                self.assertNotIn("DRY RUN FAILED", r.stdout)

    def test_a_planted_failure_and_a_restart_keep_the_finished_stages(self):
        td, r = self.run_dry("V1", "--fail-after", "ans_a")          # D, F and A are done (the order is D, F, A, C, E)
        self.assertEqual(r.returncode, 1)
        w = Path(td) / "w"
        self.assertTrue((w / "logs/m7_four_way.failed").is_file())
        before = {p: R.sha256_file(w / p) for p in ("ans_d5_A.jsonl", "ans_d5_D.jsonl", "ans_d5_F.jsonl", "stage_ans_d.done", "stage_ans_f.done", "stage_ans_a.done")}
        r2 = subprocess.run(["sh", str(SCRIPT), "--dry-run", "--work", str(w), "--check-variant", "V1", "--think-cap", "600"], cwd=str(ROOT), capture_output=True, text=True, timeout=1500)
        self.assertEqual(r2.returncode, 0, r2.stdout[-2000:])
        self.assertEqual(before, {p: R.sha256_file(w / p) for p in before})
        self.assertFalse((w / "logs/m7_four_way.failed").exists())          # the restart cleared the failed marker

    def test_a_planted_failure_leaves_the_failed_marker_until_a_restart(self):
        td, r = self.run_dry("none", "--fail-after", "ans_f")
        self.assertEqual(r.returncode, 1)
        self.assertTrue((Path(td) / "w/logs/m7_four_way.failed").is_file())

    def test_a_draft_cut_while_thinking_completes_the_chain_without_a_leak(self):
        td, r = self.run_dry("V1C", "--plant-cut", "Synthetic question 2:")
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2000:])
        w = Path(td) / "w"
        key = json.load(open(w / "reading/KEY.json"))
        self.assertEqual(key["left_out_of_the_bundles"], ["syn-02"])
        self.assertNotIn("syn-02", key["items"])
        self.assertEqual(run_main(["leak-check", "--dir", str(w / "reading_for_readers")])[0], 0)
        self.assertFalse((w / "reading_for_readers/KEY.json").exists())
        m = json.load(open(w / "measures.json"))
        self.assertEqual((m["arms"]["D"]["cut"], m["arms"]["D"]["cut_while_thinking"], m["arms"]["D"]["empty"]), (1, 1, 1))
        self.assertEqual(m["arms"]["D"]["no_draft_ids"], ["syn-02"])
        self.assertEqual(m["arms"]["A"]["empty"], 0)

    def test_a_check_that_keeps_failing_is_redone_and_then_marked(self):
        td, r = self.run_dry("V1C", "--plant-check-fail", "Synthetic question 1:,Synthetic question 3:,Synthetic question 4:")
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2000:])
        self.assertIn("redo pass 2", r.stdout)
        self.assertIn("CHECK FAILURES ABOVE LIMIT", r.stdout)
        self.assertIn("check failures above limit: C, D, F", r.stdout)
        self.assertTrue((Path(td) / "w/check_d5_C.jsonl.redo_dropped.jsonl").is_file())

    def test_a_second_invocation_changes_nothing(self):
        td, r = self.run_dry("V1")
        self.assertEqual(r.returncode, 0)
        w = Path(td) / "w"
        snap = lambda: {str(p.relative_to(w)): R.sha256_file(p) for p in sorted(w.rglob("*")) if p.is_file() and "logs" not in p.parts}  # noqa: E731
        before = snap()
        r2 = subprocess.run(["sh", str(SCRIPT), "--dry-run", "--work", str(w), "--check-variant", "V1", "--think-cap", "600"], cwd=str(ROOT), capture_output=True, text=True, timeout=1500)
        self.assertEqual(r2.returncode, 0, r2.stdout[-2000:])
        self.assertEqual(before, snap())


if __name__ == "__main__":
    unittest.main()
