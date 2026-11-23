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
    return {"P": P, "cut": cut, "idents_per10": ident, "time": {"median_s": med, "p90_s": p90}}


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

    def test_knowledge_base_row_3_and_the_lookup_clause(self):
        # below +0.04: (a) only if the lookup lowers the nonexistent identifiers (cards in the test: V1C, on both pairs)
        _, res = self.ev(ea=(0.039, 0.03), ident={"A": 2.0, "B": 2.0, "C": 1.0, "D": 1.5})
        self.assertEqual(res["kb"], "none_lookup_helps")
        _, res = self.ev(ea=(0.0, 0.03), ident={"A": 2.0, "B": 2.0, "C": 1.0, "D": 2.0})
        self.assertEqual(res["kb"], "none_lookup_no_help")
        _, res = self.ev(ea=(0.0, 0.03), ident={"A": 2.0, "B": 2.0, "C": 2.0, "D": 2.0})
        self.assertEqual(res["kb"], "none_lookup_no_help")
        for v in ("V1", "none"):
            _, res = self.ev(variant=v, ea=(0.0, 0.03))
            self.assertEqual(res["kb"], "none_lookup_not_tested", v)

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
                self.assertEqual(r.stdout.strip().splitlines()[-1], "M7 SIX-ARM TEST DONE: knowledge-base row: " + r.stdout.strip().splitlines()[-1].split("row: ")[1])
                self.assertIn("thorough-profile row: WAITS FOR THE READING", r.stdout)
                self.assertNotIn("DRY RUN FAILED", r.stdout)

    def test_a_planted_failure_and_a_restart_keep_the_finished_stages(self):
        td, r = self.run_dry("V1", "--fail-after", "ans_d")
        self.assertEqual(r.returncode, 1)
        w = Path(td) / "w"
        before = {p: R.sha256_file(w / p) for p in ("ans_d5_A.jsonl", "ans_d5_D.jsonl", "check_d5_C.jsonl", "stage_ans_d.done")}
        r2 = subprocess.run(["sh", str(SCRIPT), "--dry-run", "--work", str(w), "--check-variant", "V1", "--think-cap", "600"], cwd=str(ROOT), capture_output=True, text=True, timeout=1500)
        self.assertEqual(r2.returncode, 0, r2.stdout[-2000:])
        self.assertEqual(before, {p: R.sha256_file(w / p) for p in before})


if __name__ == "__main__":
    unittest.main()
