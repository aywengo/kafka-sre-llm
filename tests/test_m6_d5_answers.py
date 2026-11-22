"""M6 DA-chain: the rule of the D5 answers check (scripts/m6_d5_rule.py) on synthetic measures, its gates, the manifest and the start check on
synthetic folders, the prompt check, and the refusals of scripts/m6_d5_answers.sh. Offline, CPU only; no server, no model.
The whole dry run of the chain (funnel dry run, B4 dry run, then the chain with stub model and stub judge, about 3 minutes) is opt-in:
    M6_DA_E2E=1 .venv/bin/python -m unittest tests.test_m6_d5_answers.EndToEndDryRun"""
import contextlib
import copy
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

import m6_d5_rule as R  # noqa: E402

SCRIPT = ROOT / "scripts/m6_d5_answers.sh"
INDEX = ROOT / "work/v3/chunks_s1.jsonl"


def variant(P=0.70, S=2.0, cut=2, tot=40.0, ident=1.0, d=None, Sf=None):
    return {"P": P, "supp_miss_per10": S, "supp_miss_per10_frozen": S if Sf is None else Sf, "cut": cut, "median_total_s": tot, "idents_per10": ident, "answers": 50,
            "mech": {"d_all_pass": d or {"M1": 0, "M2": 0, "M3": 0}}}


def measures(g2=None, g3=None, ref=None):
    return {"sizes": {"D5": 50, "mech": {"M1": 56, "M2": 40, "M3": 20}}, "pop": {"M1": 56, "M2": 195, "M3": 88},
            "variants": {"default": ref or variant(P=0.70, S=2.0, cut=0, tot=30.0, ident=1.0), "g2": g2 or variant(), "g3": g3 or variant(P=0.60)}}


def run_main(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = R.main(argv)
        except SystemExit as e:
            code = e.code
    return code, out.getvalue(), err.getvalue()


# one failing condition each, by the key used in the rule's lines; the value is the keyword arguments of variant()
FAILING = {"a": {"P": 0.70 - 0.0501}, "b": {"S": 2.0 + 1.01}, "c": {"cut": 6}, "d": {"tot": 60.1}, "e": {"ident": 1.01}, "f": {"d": {"M1": 0, "M2": 0, "M3": -3}}}


class RuleTests(unittest.TestCase):
    def test_everything_passes_and_g2_is_chosen_when_both_are_close(self):
        L, elig, choice = R.evaluate(measures(g2=variant(P=0.70), g3=variant(P=0.71)))
        self.assertEqual((elig, choice), ({"g2": True, "g3": True}, "grounded-g2"))
        self.assertEqual(sum(1 for l in L if l.strip().startswith("PASS")), 12)

    def test_each_condition_failing_alone(self):
        for key, kw in FAILING.items():
            with self.subTest(condition=key):
                m = measures(g2=variant(**kw), g3=variant(**kw))
                L, elig, choice = R.evaluate(m)
                self.assertEqual(elig, {"g2": False, "g3": False})
                self.assertEqual(choice, "neither")
                fails = [l for l in L if l.strip().startswith("FAIL")]
                self.assertEqual(len(fails), 2, fails)
                self.assertTrue(all(f"({key})" in f for f in fails), fails)
                # and the other variant carries the choice when only one fails
                _, elig2, choice2 = R.evaluate(measures(g2=variant(**kw), g3=variant()))
                self.assertEqual((elig2, choice2), ({"g2": False, "g3": True}, "grounded-g3"))
                _, elig3, choice3 = R.evaluate(measures(g2=variant(), g3=variant(**kw)))
                self.assertEqual((elig3, choice3), ({"g2": True, "g3": False}, "grounded-g2"))

    def test_thresholds_at_the_edges_pass(self):
        edge = variant(P=0.70 - 0.05, S=3.0, cut=5, tot=60.0, ident=1.0, d={"M1": 0, "M2": 0, "M3": 0})
        _, elig, _ = R.evaluate(measures(g2=edge, g3=edge))
        self.assertEqual(elig, {"g2": True, "g3": True})
        # -3.0 points exactly: M3 weighs 4.4 per item; 339 * 3.0 / 100 = 10.17 / 4.4 is not a whole number, so use the M2 weight of 4.875 over two parts
        proj, _, _ = R.projection(measures(g2=variant(d={"M1": -1, "M2": -1, "M3": -1})), "g2")
        self.assertAlmostEqual(proj, (-1 - 4.875 - 4.4) / 339 * 100, places=9)
        self.assertLess(proj, -3.0)
        proj2, _, _ = R.projection(measures(g2=variant(d={"M1": -2, "M2": -1, "M3": -1})), "g2")
        self.assertAlmostEqual(proj2, (-2 - 4.875 - 4.4) / 339 * 100, places=9)

    def test_the_projection_weights_are_the_development_checks(self):
        m = measures(g2=variant(d={"M1": 3, "M2": 2, "M3": -4}))
        proj, d, sizes = R.projection(m, "g2")
        self.assertAlmostEqual(proj, (3 * 56 / 56 + 2 * 195 / 40 + -4 * 88 / 20) / 339 * 100, places=9)

    def test_tie_goes_to_g2_below_0_02_and_the_higher_coverage_wins_from_0_02(self):
        for gap, want in ((0.019, "grounded-g2"), (0.021, "grounded-g3"), (0.02, "grounded-g3"), (0.0, "grounded-g2")):
            with self.subTest(g3_minus_g2=gap):
                _, _, choice = R.evaluate(measures(g2=variant(P=0.70), g3=variant(P=0.70 + gap)))
                self.assertEqual(choice, want)
        _, _, choice = R.evaluate(measures(g2=variant(P=0.721), g3=variant(P=0.70)))
        self.assertEqual(choice, "grounded-g2")
        _, _, choice = R.evaluate(measures(g2=variant(P=0.70), g3=variant(P=0.80)))
        self.assertEqual(choice, "grounded-g3")

    def test_one_eligible_and_none(self):
        _, elig, choice = R.evaluate(measures(g2=variant(P=0.10), g3=variant(P=0.65)))
        self.assertEqual((elig, choice), ({"g2": False, "g3": True}, "grounded-g3"))
        _, elig, choice = R.evaluate(measures(g2=variant(P=0.10), g3=variant(P=0.10)))
        self.assertEqual((elig, choice), ({"g2": False, "g3": False}, "neither"))

    def test_a_missing_or_non_numeric_measure_is_an_error_never_a_pass(self):
        for path in (("variants", "g2", "P"), ("variants", "g3", "cut"), ("variants", "default", "idents_per10"), ("sizes", "D5")):
            m = measures()
            d = m
            for p in path[:-1]:
                d = d[p]
            del d[path[-1]]
            with self.subTest(missing=path):
                with self.assertRaises(R.DaError):
                    R.evaluate(m)
        m = measures()
        m["variants"]["g2"]["median_total_s"] = float("nan")
        with self.assertRaises(R.DaError):
            R.evaluate(m)
        m = measures()
        m["variants"]["g3"]["mech"]["d_all_pass"] = {"M1": 0}
        with self.assertRaises(R.DaError):
            R.evaluate(m)

    def test_the_rule_command_prints_the_last_line_and_exits_2_on_missing_input(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.json"
            p.write_text(json.dumps(measures(g2=variant(P=0.10), g3=variant(P=0.65))))
            code, out, _ = run_main(["rule", "--measures", str(p)])
            self.assertEqual(code, 0)
            self.assertEqual([l for l in out.strip().splitlines() if l.startswith("CHOICE: ")][-1], "CHOICE: grounded-g3")
            p.write_text(json.dumps(measures(g2=variant(P=0.10), g3=variant(P=0.10))))
            code, out, _ = run_main(["rule", "--measures", str(p)])
            self.assertEqual([l for l in out.strip().splitlines() if l.startswith("CHOICE: ")][-1], "CHOICE: neither")
            bad = measures()
            del bad["variants"]["g2"]["P"]
            p.write_text(json.dumps(bad))
            code, out, err = run_main(["rule", "--measures", str(p)])
            self.assertEqual(code, 2)
            self.assertNotIn("CHOICE", out)
            self.assertIn("no verdict", err)

    def test_the_rule_text_names_every_threshold(self):
        t = R.RULE_TEXT
        for s in ("-0.05", "+ 1.0", "10 %", "60", "-3.0", "0.02", "4.875", "CHOICE: grounded-g2", "CHOICE: neither", "whole-retrieval", "FROZEN pack", "10 chunks"):
            self.assertIn(s, t)


# ---------------------------------------------------------------- the gates on synthetic folders
def synthetic_funnel(td, setting="r4g2", go=True):
    fw = Path(td) / "funnel"
    fw.mkdir()
    ids = ["x-1", "x-2"]
    items = fw / "items.jsonl"
    rubrics = fw / "rubrics.jsonl"
    items.write_text("".join(json.dumps({"id": i, "question": "q"}) + "\n" for i in ids))
    rubrics.write_text("".join(json.dumps({"id": i, "must": []}) + "\n" for i in ids))
    (fw / "packs.jsonl").write_text("".join(json.dumps({"id": i, "frozen": ["a", "b"], setting: ["a", "b", "c"], "seconds": {}}) + "\n" for i in ids))
    (fw / "expansions.jsonl").write_text("".join(json.dumps({"id": i, "expansion": "e", "seconds": 5.0}) + "\n" for i in ids))
    (fw / "timing.jsonl").write_text(json.dumps({"id": "x-1", setting: 2.0}) + "\n")
    (fw / "served.json").write_text(json.dumps({"timing": {setting: {"n": 1, "median": 2.0, "p90": 2.0}}}))
    pin = "p" * 64
    (fw / "DF_MANIFEST.json").write_text(json.dumps({"options": {"dry_run": True, "model": "m", "setting": setting}, "pinned": {"sha256": pin}}))
    for st, out in (("expand", "expansions.jsonl"), ("timing", "timing.jsonl"), ("packs", "packs.jsonl"), ("served", "served.json")):
        (fw / f"{st}.passed").write_text(json.dumps({"stage": st, "passed": True, "pin": pin, "outputs": {out: R.sha256_file(fw / out)}}))
    used = fw / "D5_FUNNEL_USED"
    used.write_text("used\nverdict GO\n")
    return fw, items, rubrics, used


class GateTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.addCleanup(self.td.cleanup)

    def record(self, **over):
        from kafka_sre_rag import profiles
        rec = {"setting": "r4g2", "profile": "r4", "verdict": "GO", "settings_sha256": profiles.settings_sha256("r4", grid="r4g2"),
               "settings": profiles.settings("r4", grid="r4g2"), "dry_run": True}
        rec.update(over)
        p = Path(self.td.name) / "rec.json"
        p.write_text(json.dumps(rec))
        return p

    def test_chosen_record(self):
        s, pr = R.gate_chosen(self.record(), dry=True)
        self.assertEqual((s, pr), ("r4g2", []))
        s, pr = R.gate_chosen(self.record(verdict="STOP"), dry=True)
        self.assertTrue(any("not GO" in p for p in pr), pr)
        s, pr = R.gate_chosen(self.record(settings_sha256="0" * 64), dry=True)
        self.assertTrue(any("settings_sha256" in p for p in pr), pr)
        s, pr = R.gate_chosen(self.record(), dry=False)
        self.assertTrue(any("dry run" in p for p in pr), pr)
        s, pr = R.gate_chosen(self.record(dry_run=False), dry=False)
        self.assertEqual(pr, [])
        s, pr = R.gate_chosen(Path(self.td.name) / "nope.json", dry=True)
        self.assertIsNone(s)
        self.assertTrue(pr)

    def test_funnel_gate_passes_and_catches_a_changed_pack_file(self):
        fw, items, rubrics, used = synthetic_funnel(self.td.name)
        rec = self.record()
        self.assertEqual(R.gate_funnel(fw, items, rubrics, "r4g2", rec, used, dry=True), [])
        with open(fw / "packs.jsonl", "a") as f:
            f.write(" \n")
        pr = R.gate_funnel(fw, items, rubrics, "r4g2", rec, used, dry=True)
        self.assertTrue(any("differs from the hash the funnel recorded" in p for p in pr), pr)

    def test_funnel_gate_other_refusals(self):
        fw, items, rubrics, used = synthetic_funnel(self.td.name)
        rec = self.record()
        self.assertTrue(R.gate_funnel(fw, items, rubrics, "r4g2", rec, used, dry=False))     # a dry funnel in a real run
        self.assertTrue(R.gate_funnel(fw, items, rubrics, "r4g4", rec, used, dry=True))      # another setting
        self.assertTrue(R.gate_funnel(fw, items, rubrics, "r4g2", rec, fw / "missing", dry=True))
        (fw / "timing.passed").unlink()
        self.assertTrue(any("timing.passed is missing" in p for p in R.gate_funnel(fw, items, rubrics, "r4g2", rec, used, dry=True)))

    def test_funnel_gate_catches_a_pack_that_does_not_start_with_the_frozen_pack(self):
        fw, items, rubrics, used = synthetic_funnel(self.td.name)
        rows = [json.loads(l) for l in open(fw / "packs.jsonl")]
        rows[0]["r4g2"] = ["c", "a", "b"]
        (fw / "packs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        pin = json.load(open(fw / "DF_MANIFEST.json"))["pinned"]["sha256"]
        (fw / "packs.passed").write_text(json.dumps({"stage": "packs", "passed": True, "pin": pin, "outputs": {"packs.jsonl": R.sha256_file(fw / "packs.jsonl")}}))
        pr = R.gate_funnel(fw, items, rubrics, "r4g2", self.record(), used, dry=True)
        self.assertTrue(any("does not start with the frozen pack" in p for p in pr), pr)

    def b4_files(self, verdict="PASS", decide="strict", setting="r4g2", dry=True, sha=None):
        d = Path(self.td.name) / "b4"
        d.mkdir(exist_ok=True)
        rec = self.record()
        (d / "B4_USED").write_text(f"used 2026-11-22\ndecide {decide}\nverdict {verdict}\nreport x\n")
        (d / "B4_MANIFEST.json").write_text(json.dumps({"options": {"decide": decide, "setting": setting, "dry_run": dry},
                                                        "profile": {"settings_sha256": sha or json.load(open(rec))["settings_sha256"]}}))
        return d, rec

    def test_b4_gate(self):
        d, rec = self.b4_files()
        self.assertEqual(R.gate_b4(d / "B4_USED", d, rec, "r4g2", dry=True), [])
        for kw, word in (({"verdict": "FAIL"}, "not PASS"), ({"setting": "r4g4"}, "r4g4"), ({"decide": "loose"}, "loose"), ({"dry": False}, "dry"), ({"sha": "0" * 64}, "other parameters")):
            d, rec = self.b4_files(**kw)
            pr = R.gate_b4(d / "B4_USED", d, rec, "r4g2", dry=True)
            self.assertTrue(any(word in p for p in pr), (kw, pr))
        pr = R.gate_b4(Path(self.td.name) / "none" / "B4_USED", Path(self.td.name) / "none", rec, "r4g2", dry=True)
        self.assertTrue(any("has not been run" in p for p in pr), pr)


class StateTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.root = Path(self.td.name)
        (self.root / "scripts").mkdir()
        self.code = self.root / "scripts/m6_d5_answers.sh"
        self.other = self.root / "input.txt"
        self.code.write_text("code v1\n")
        self.other.write_text("input v1\n")
        self.w = self.root / "w"
        self.w.mkdir()

    def argv(self, cmd, extra=()):
        return [cmd, "--manifest", str(self.w / "MANIFEST.json"), "--work", str(self.w), "--pin", str(self.code), "--pin", str(self.other),
                "--frozen", str(ROOT / "kafka_sre_rag/frozen.json"), "--dry", "--opt", "setting=r4g2", "--opt", "without_b4=", *extra]

    def mark(self, stage, *outs):
        mh = R.G.manifest_hash(json.load(open(self.w / "MANIFEST.json")))
        (self.w / f"stage_{stage}.done").write_text(json.dumps({"stage": stage, "manifest_sha256": mh, "outputs": {str(o): R.sha256_file(o) for o in outs}}))

    def test_fresh_manifest_then_a_changed_pin_option_and_marker_output_are_refused(self):
        self.assertEqual(run_main(self.argv("start-check"))[0], 0)
        self.assertEqual(run_main(self.argv("manifest"))[0], 0)
        self.assertEqual(run_main(self.argv("start-check"))[0], 0)
        out = self.w / "out.txt"
        out.write_text("x")
        self.mark("header", out)
        self.assertEqual(run_main(self.argv("start-check"))[0], 0)
        out.write_text("changed")
        code, _, err = run_main(self.argv("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("changed since the stage finished", err)
        out.write_text("x")
        self.other.write_text("input v2\n")
        code, _, err = run_main(self.argv("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("input.txt", err)
        self.other.write_text("input v1\n")
        code, _, err = run_main(self.argv("start-check", ["--opt", "setting=other"]))
        self.assertEqual(code, 2)
        self.assertIn("options.setting", err)

    def test_a_marker_without_a_manifest_is_refused(self):
        (self.w / "stage_header.done").write_text("{}")
        code, _, err = run_main(self.argv("start-check"))
        self.assertEqual(code, 2)
        self.assertIn("no manifest", err)

    def test_accept_code_change_only_for_the_chains_two_files_and_only_after_the_answering_stages(self):
        run_main(self.argv("manifest"))
        self.code.write_text("code v2\n")
        self.assertEqual(run_main(self.argv("start-check", ["--accept-code-change", "fix a typo"]))[0], 2)     # stages not done
        o = self.w / "o.txt"
        o.write_text("o")
        self.mark("reference", o)
        self.mark("answers", o)
        self.assertEqual(run_main(self.argv("start-check", ["--accept-code-change", ""]))[0], 2)               # empty reason
        self.other.write_text("input v2\n")
        self.assertEqual(run_main(self.argv("start-check", ["--accept-code-change", "fix"]))[0], 2)            # another file changed too
        self.other.write_text("input v1\n")
        code, out, _ = run_main(self.argv("start-check", ["--accept-code-change", "fix a typo"]))
        self.assertEqual(code, 0)
        self.assertIn("ACCEPTED", out)
        m = json.load(open(self.w / "MANIFEST.json"))
        self.assertEqual(m[R.G.AUDIT_KEY][0]["reason"], "fix a typo")
        self.assertEqual(run_main(self.argv("start-check"))[0], 0)       # the new hash is in force; the markers still match (the audit key is outside the manifest hash)


class PromptCheckTests(unittest.TestCase):
    def test_prompts_differ_counts(self):
        with tempfile.TemporaryDirectory() as td:
            a, b = Path(td) / "a.jsonl", Path(td) / "b.jsonl"
            a.write_text("".join(json.dumps({"id": f"i{k}", "prompt_sha256": f"h{k}"}) + "\n" for k in range(3)))
            b.write_text("".join(json.dumps({"id": f"i{k}", "prompt_sha256": f"g{k}"}) + "\n" for k in range(3)))
            self.assertEqual(run_main(["prompts-differ", "--a", str(a), "--b", str(b), "--expect", "3"])[0], 0)
            self.assertEqual(run_main(["prompts-differ", "--a", str(a), "--b", str(b), "--expect", "4"])[0], 1)
            b.write_text(b.read_text().replace('"g1"', '"h1"'))       # one id with the same prompt: a mode that fell back
            code, out, _ = run_main(["prompts-differ", "--a", str(a), "--b", str(b), "--expect", "3"])
            self.assertEqual(code, 1)
            self.assertIn("1 equal", out)
            b.write_text(b.read_text().replace('"prompt_sha256": "g2"', '"x": 1'))       # one id without a recorded hash
            self.assertEqual(run_main(["prompts-differ", "--a", str(a), "--b", str(b), "--expect", "3"])[0], 1)

    @unittest.skipUnless(INDEX.exists(), "the frozen index file is generated (git-ignored work/v3)")
    def test_prompts_check_catches_a_fall_back_to_the_default_prompt_on_the_same_pack(self):
        import eval2
        from kafka_sre_rag import evalhooks, prompts
        ids = [json.loads(l)["id"] for l in open(INDEX)][:3]
        item = {"id": "t-1", "question": "How do I list consumer groups?", "evidence": "kafka-consumer-groups.sh --list"}
        q = eval2.full_question(item)
        chunks = evalhooks.chunks_by_ids(ids)
        good = eval2.messages_fingerprint(prompts.grounded_answer_messages(q, chunks, "grounded-g2"))[0]
        fallen = eval2.messages_fingerprint(prompts.answer_messages(q, chunks, strict=False))[0]
        with tempfile.TemporaryDirectory() as td:
            items, ans = Path(td) / "items.jsonl", Path(td) / "ans.jsonl"
            items.write_text(json.dumps(item) + "\n")
            ans.write_text(json.dumps({"id": "t-1", "pack": ids, "prompt_sha256": good}) + "\n")
            self.assertEqual(run_main(["prompts-check", "--items", str(items), "--answers", str(ans), "--mode", "grounded-g2"])[0], 0)
            self.assertEqual(run_main(["prompts-check", "--items", str(items), "--answers", str(ans), "--mode", "grounded-g3"])[0], 1)     # the other mode's prompt
            ans.write_text(json.dumps({"id": "t-1", "pack": ids, "prompt_sha256": fallen}) + "\n")
            code, out, _ = run_main(["prompts-check", "--items", str(items), "--answers", str(ans), "--mode", "grounded-g2"])
            self.assertEqual(code, 1)
            self.assertIn("not the prompt of grounded-g2", out)
            ans.write_text(json.dumps({"id": "t-1", "pack": ids}) + "\n")
            self.assertEqual(run_main(["prompts-check", "--items", str(items), "--answers", str(ans), "--mode", "grounded-g2"])[0], 1)


class StoredMediansAndOverrideTests(unittest.TestCase):
    def write(self, td, name, secs):
        p = Path(td) / name
        p.write_text("".join(json.dumps({"id": f"i{k}", "seconds": {"retrieval": v}}) + "\n" for k, v in enumerate(secs)))
        return str(p)

    def test_stored_medians_and_a_missing_file(self):
        with tempfile.TemporaryDirectory() as td:
            a, b = self.write(td, "a.jsonl", [4.2, 4.27, 4.3]), self.write(td, "b.jsonl", [3.9, 4.01, 9.0])
            problems, meds = R.stored_retrieval_medians([a, b])
            self.assertEqual((problems, [round(m, 2) for m in meds]), ([], [4.27, 4.01]))
            problems, _ = R.stored_retrieval_medians([a, str(Path(td) / "missing.jsonl")])
            self.assertIn("missing", problems[0])

    @unittest.skipUnless(all((ROOT / f).exists() for f in R.STORED_RETRIEVAL_FILES), "stored default runs are generated files")
    def test_the_stored_runs_give_the_known_medians(self):
        problems, meds = R.stored_retrieval_medians()
        self.assertEqual(problems, [])
        self.assertEqual([round(m, 2) for m in meds], [4.27, 4.01])

    def test_without_b4_does_not_override_a_failed_check(self):
        with tempfile.TemporaryDirectory() as td:
            u = Path(td) / "B4_USED"
            self.assertEqual(R.gate_b4_override(u), [])                       # no marker: the override stands
            u.write_text("used x\ndecide strict\nverdict FAIL\n")
            self.assertTrue(R.gate_b4_override(u))
            u.write_text("used x\ndecide strict\nverdict PASS\n")
            self.assertEqual(R.gate_b4_override(u), [])
            u.write_text("garbage\n")
            self.assertTrue(R.gate_b4_override(u))
            self.assertEqual(run_main(["gate-b4-override", "--used", str(u)])[0], 1)


class FrozenPackMeasureTests(unittest.TestCase):
    def test_the_two_choices_can_differ_and_the_rule_prints_both(self):
        # own pack: G2 passes (b); on the frozen pack G2 fails (b) and G3 carries the choice
        m = measures(g2=variant(P=0.72, S=2.0, Sf=4.0), g3=variant(P=0.70, S=2.0, Sf=2.5))
        _, elig, choice = R.evaluate(m)
        _, elig2, choice2 = R.evaluate(m, frozen_b=True)
        self.assertEqual((elig, choice), ({"g2": True, "g3": True}, "grounded-g2"))
        self.assertEqual((elig2, choice2), ({"g2": False, "g3": True}, "grounded-g3"))
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.json"
            p.write_text(json.dumps(m))
            code, out, _ = run_main(["rule", "--measures", str(p)])
            lines = out.strip().splitlines()
            self.assertEqual(code, 0)
            self.assertIn("CHOICE: grounded-g2", lines)
            self.assertEqual(lines[-1], R.ALT_LABEL + "grounded-g3")
            self.assertEqual(sum(1 for l in lines if l.startswith("CHOICE: ")), 1)
            man, rec, out_p = Path(td) / "man.json", Path(td) / "rec.json", Path(td) / "cand.json"
            man.write_text(json.dumps({"x": 1}))
            rec.write_text(json.dumps({"setting": "r4g2", "settings_sha256": "s"}))
            code, _, _ = run_main(["candidate", "--measures", str(p), "--record", str(rec), "--manifest", str(man), "--out", str(out_p), "--report", "r.md"])
            self.assertEqual(code, 0)
            c = json.load(open(out_p))
            self.assertEqual((c["mode"], c["choice_b_on_frozen_pack"]), ("grounded-g2", "grounded-g3"))

    def test_a_missing_frozen_pack_measure_is_an_error(self):
        m = measures()
        del m["variants"]["g2"]["supp_miss_per10_frozen"]
        R.evaluate(m)                                                    # the rule as written does not need it
        with self.assertRaises(R.DaError):
            R.evaluate(m, frozen_b=True)
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "m.json"
            p.write_text(json.dumps(m))
            self.assertEqual(run_main(["rule", "--measures", str(p)])[0], 2)

    def test_supp_miss_on_pack_reads_the_frozen_pack_instead_of_the_own_pack(self):
        rub = {"id": "x", "must": [{"id": "m1", "text": "t", "weight": 2, "chunks": ["c9"]}, {"id": "m2", "text": "t", "weight": 1, "chunks": ["c1"]}], "must_not": []}
        row = {"set": "g2", "id": "x", "pass": 1, "valid_reply": True, "pack": ["c1", "c9"],
               "must": {"m1": {"verdict": "absent"}, "m2": {"verdict": "absent"}}, "must_not": {}}
        with tempfile.TemporaryDirectory() as td:
            rp, jp = Path(td) / "rub.jsonl", Path(td) / "judge.jsonl"
            rp.write_text(json.dumps(rub) + "\n")
            jp.write_text(json.dumps(row) + "\n")
            own = R.supp_miss_on_pack(jp, rp, "g2", {"x": ["c1", "c9"]})
            frozen = R.supp_miss_on_pack(jp, rp, "g2", {"x": ["c1"]})
            self.assertGreater(own["x"], frozen["x"])
            self.assertEqual(R.supp_miss_on_pack(jp, rp, "g2", {"x": []})["x"], 0)


IMPORT_TRACE = """
import sys, os, importlib
sys.path[:0] = ['scripts', '.']
for m in ('eval2', 'eval2_judge', 'eval2_score', 'eval2_idcheck', 'eval2_compare', 'run_guarded', 'm6_d5_rule', 'm5_g_rule', 'm5_b4_served', 'm6_d5_funnel',
          'kafka_sre_rag.__main__', 'kafka_sre_rag.retrieval', 'kafka_sre_rag.evalhooks', 'kafka_sre_rag.pipeline', 'kafka_sre_rag.generators', 'kafka_sre_rag.profiles',
          'kafka_sre_rag.service', 'kafka_sre_rag.revise', 'kafka_sre_rag.checker', 'kafka_sre_rag.catalogue'):
    importlib.import_module(m)
root = os.getcwd()
print('\\n'.join(sorted({os.path.relpath(v.__file__, root) for v in sys.modules.values()
                       if getattr(v, '__file__', None) and os.path.abspath(v.__file__).startswith(root) and '.venv' not in v.__file__ and '__pycache__' not in v.__file__})))
"""


class CodePinTests(unittest.TestCase):
    def test_the_pins_cover_every_repository_module_the_chain_imports(self):
        r = subprocess.run([sys.executable, "-c", IMPORT_TRACE], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr[-500:])
        imported = set(r.stdout.split())
        self.assertTrue({"kafka_sre_rag/generators.py", "scripts/retrieve.py", "kafka_sre_rag/checker.py", "scripts/build_packs_v3.py"} <= imported, sorted(imported))
        self.assertEqual(sorted(imported - set(R.CODE_PINS)), [], "imported by the chain but not pinned")
        for f in R.CODE_PINS:
            self.assertTrue((ROOT / f).is_file(), f)

    def test_the_script_takes_its_pins_from_code_pins_and_passes_the_commit_flag(self):
        txt = SCRIPT.read_text()
        self.assertIn("$R code-pins", txt)
        self.assertIn("--allow-commit-change", txt)
        for stage in ("rettime", "reference", "answers", "judge", "measures"):
            self.assertIn(f"recheck {stage}", txt)


class PackFileTests(unittest.TestCase):
    def test_d5_packs_writes_the_replay_format_and_a_sidecar(self):
        with tempfile.TemporaryDirectory() as td:
            fw, items, rubrics, used = synthetic_funnel(td)
            of, oc = Path(td) / "pf.jsonl", Path(td) / "pc.jsonl"
            code, out, _ = run_main(["d5-packs", "--funnel-work", str(fw), "--items", str(items), "--setting", "r4g2", "--out-frozen", str(of), "--out-chosen", str(oc)])
            self.assertEqual(code, 0, out)
            self.assertEqual([json.loads(l) for l in open(of)], [{"id": "x-1", "pack": ["a", "b"], "error": None}, {"id": "x-2", "pack": ["a", "b"], "error": None}])
            self.assertEqual([json.loads(l)["pack"] for l in open(oc)], [["a", "b", "c"]] * 2)
            side = json.load(open(str(oc) + ".manifest.json"))
            self.assertEqual(side["pack_file_sha256"], R.sha256_file(oc))
            self.assertEqual(side["source_sha256"], R.sha256_file(fw / "packs.jsonl"))


# ---------------------------------------------------------------- the shell script
def sh(args, cwd=ROOT, env=None):
    return subprocess.run(["sh", str(SCRIPT), *args], cwd=cwd, capture_output=True, text=True, env=env, timeout=300)


class ScriptTests(unittest.TestCase):
    def test_syntax(self):
        r = subprocess.run(["sh", "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_no_address_host_or_home_path_in_the_new_files(self):
        import re
        for p in (SCRIPT, ROOT / "scripts/m6_d5_rule.py", Path(__file__)):
            txt = p.read_text()
            self.assertIsNone(re.search(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", txt), p)
            self.assertNotIn("/" + "Users/", txt, p)
            self.assertNotIn("/private" + "/tmp", txt, p)

    def dummy_funnel(self, td, verdict="GO"):
        fw = Path(td) / "funnel"
        (fw / "in").mkdir(parents=True)
        for rel in ("DF_MANIFEST.json", "packs.jsonl", "expansions.jsonl", "served.json", "in/items.jsonl", "in/rubrics.jsonl", "in/CHUNK_CHECK.json"):
            (fw / rel).write_text("{}\n")
        return fw

    def test_refusals_leave_nothing_behind(self):
        with tempfile.TemporaryDirectory() as td:
            fw = self.dummy_funnel(td)
            w = Path(td) / "w"
            rep = Path(td) / "report.md"
            common = ["--dry-run", "--work", str(w), "--report", str(rep), "--funnel-work", str(fw), "--without-b4", "test"]
            r = sh(common + ["--chosen-record", str(Path(td) / "nope.json")])
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
            self.assertIn("REFUSED", r.stderr)
            rec = Path(td) / "stop.json"
            from kafka_sre_rag import profiles
            rec.write_text(json.dumps({"setting": "r4g2", "verdict": "STOP", "settings_sha256": profiles.settings_sha256("r4", grid="r4g2"),
                                       "settings": profiles.settings("r4", grid="r4g2")}))
            r = sh(common + ["--chosen-record", str(rec)])
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
            self.assertIn("no go for r4g2", r.stderr)
            self.assertFalse(rep.exists())
            self.assertEqual(sorted(p.name for p in w.iterdir()), [])        # the dry run's work folder holds nothing: no marker, no manifest

    def test_a_dry_run_refuses_the_real_files_and_needs_the_funnel_folder(self):
        r = sh(["--dry-run"])
        self.assertEqual(r.returncode, 2)
        self.assertIn("--funnel-work", r.stderr)
        with tempfile.TemporaryDirectory() as td:
            r = sh(["--dry-run", "--funnel-work", td, "--without-b4", "x", "--chosen-record", "runs/m6/RETRIEVAL_CHOSEN.json"])
            self.assertEqual(r.returncode, 2)
            self.assertIn("never uses the real", r.stderr)
            r = sh(["--dry-run", "--funnel-work", td, "--chosen-record", str(Path(td) / "r.json")])
            self.assertEqual(r.returncode, 2)
            self.assertIn("--b4-used", r.stderr)
            r = sh(["--dry-run", "--funnel-work", td, "--without-b4", "x", "--items", "eval/m6/d5/items.jsonl"])
            self.assertEqual(r.returncode, 2)
            self.assertIn("real D5 files", r.stderr)

    def test_a_real_run_without_the_funnel_record_writes_nothing(self):
        if (ROOT / "runs/m6/RETRIEVAL_CHOSEN.json").exists():
            self.skipTest("the funnel has run here")
        before = sorted(p.name for p in (ROOT / "runs/m6").iterdir()) if (ROOT / "runs/m6").exists() else None
        r = sh([])
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("REFUSED", r.stderr)
        after = sorted(p.name for p in (ROOT / "runs/m6").iterdir()) if (ROOT / "runs/m6").exists() else None
        self.assertEqual(before, after)

    def test_real_run_options_are_refused(self):
        r = sh(["--max-unjudged", "x"])
        self.assertEqual(r.returncode, 2)
        r = sh(["--items", "a.jsonl"])
        self.assertEqual(r.returncode, 2)
        self.assertIn("dry runs only", r.stderr)


@unittest.skipUnless(os.environ.get("M6_DA_E2E") == "1", "opt-in (about 3 minutes): set M6_DA_E2E=1")
class EndToEndDryRun(unittest.TestCase):
    def test_funnel_dry_run_then_b4_dry_run_then_the_chain_to_a_choice_line_and_a_resume_that_changes_nothing(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            f = subprocess.run(["sh", str(ROOT / "scripts/m6_d5_funnel.sh"), "--dry-run", "--work", str(td / "f")], cwd=ROOT, capture_output=True, text=True, timeout=600)
            self.assertEqual(f.returncode, 0, f.stdout[-800:] + f.stderr[-800:])
            b = subprocess.run(["sh", str(ROOT / "scripts/m5_b4_check.sh"), "--decide", "strict", "--setting", "r4g2", "--dry-run", "--work", str(td / "b"),
                                "--used-marker", str(td / "b/B4_USED")], cwd=ROOT, capture_output=True, text=True, timeout=600)
            self.assertEqual(b.returncode, 0, b.stdout[-800:] + b.stderr[-800:])
            rec = json.load(open(td / "f/RETRIEVAL_CHOSEN.json"))      # the dry run's synthetic data decides STOP and FAIL: edited copies stand in for GO and PASS
            rec["verdict"] = "GO"
            (td / "go.json").write_text(json.dumps(rec))
            (td / "b/B4_USED").write_text((td / "b/B4_USED").read_text().replace("verdict FAIL", "verdict PASS"))
            args = ["--dry-run", "--work", str(td / "w"), "--funnel-work", str(td / "f"), "--chosen-record", str(td / "go.json"), "--b4-used", str(td / "b/B4_USED"), "--b4-work", str(td / "b")]
            r = sh(args)
            self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
            self.assertIn(r.stdout.strip().splitlines()[-1], ("CHOICE: grounded-g2", "CHOICE: grounded-g3", "CHOICE: neither"))
            snap = lambda: {str(p.relative_to(td / "w")): R.sha256_file(p) for p in sorted((td / "w").rglob("*")) if p.is_file()}      # noqa: E731
            before = snap()
            r2 = sh(args)
            self.assertEqual(r2.returncode, 0, r2.stdout[-800:] + r2.stderr[-800:])
            self.assertEqual(before, snap())


if __name__ == "__main__":
    unittest.main()
