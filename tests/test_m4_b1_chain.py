"""Offline tests of scripts/m4_b1_coverage_batch.sh (stub generator and stub judge; never the model server)."""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SH = ROOT / "scripts/m4_b1_coverage_batch.sh"
PILOT_ITEMS, PILOT_RUBS = ROOT / "eval/m4/pilot2/items.jsonl", ROOT / "eval/m4/pilot2/rubrics.jsonl"
NEED = [ROOT / "work/v3/chunks_s1.jsonl", ROOT / "work/m4/catalogues/CATALOGUES.json", PILOT_ITEMS, PILOT_RUBS, ROOT / ".venv/bin/python"]


def sh(*args, timeout=900, env=None):
    return subprocess.run(["sh", str(SH), *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout,
                          env={**os.environ, **(env or {})})


class Static(unittest.TestCase):
    def test_syntax_and_executable(self):
        self.assertTrue(os.access(SH, os.X_OK))
        self.assertEqual(subprocess.run(["sh", "-n", str(SH)]).returncode, 0)
        self.assertEqual(subprocess.run(["bash", "-n", str(SH)]).returncode, 0)

    def test_order_of_work_and_the_three_answer_sets(self):
        t = SH.read_text()
        i = [t.index("answer_run default default\n"), t.index("answer_run strict strict --replay-packs"), t.index("answer_run default_4k default --replay-packs"),
             t.index('unload $Q4; hb'), t.rindex("eval2_judge.py run"), t.index('unload $DS; hb'), t.rindex("eval2_idcheck.py"),
             t.rindex("eval2_pilot_report.py --coverage")]
        self.assertEqual(i, sorted(i))
        self.assertIn("--max-tokens 4096", t)
        self.assertIn("--instruction v2 --coverage", t)
        self.assertIn("--passes 1 2", t)
        self.assertIn("DeepSeek-V4.1-Flash-1017-oQ4e-mtp", t)

    def test_no_address_key_or_home_path_in_the_script(self):
        t = SH.read_text()
        for bad in ("/Users/", "http://", "https://", "192.168.", "sk-"):
            self.assertNotIn(bad, t)

    def test_never_unloads_a_model_of_another_client(self):
        t = SH.read_text()
        self.assertIn("unload $DS; unload $GLM4; unload $GLM8", t)
        self.assertIn("-z \"$(foreign)\"", t)                # waits while a model of another client is loaded


class Refusals(unittest.TestCase):
    def check_refused(self, *args, text=None):
        hb, st = ROOT / "runs/v3/HEARTBEAT.md", ROOT / "runs/v3/status.json"
        before = (hb.read_bytes() if hb.exists() else None, st.read_bytes() if st.exists() else None)
        r = sh(*args, timeout=120)
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("REFUSED", r.stderr)
        if text:
            self.assertIn(text, r.stderr)
        self.assertEqual(before, (hb.read_bytes() if hb.exists() else None, st.read_bytes() if st.exists() else None))   # nothing written
        return r

    def test_unknown_option(self):
        self.assertEqual(sh("--nonsense").returncode, 2)

    def test_missing_inputs(self):
        self.check_refused("--items", "/nonexistent/items.jsonl", "--rubrics", "/nonexistent/rubrics.jsonl", text="missing or empty")

    def test_default_inputs_are_the_b1_files(self):
        t = SH.read_text()
        self.assertIn("eval/m4/b1/items.jsonl", t)
        self.assertIn("eval/m4/b1/rubrics.jsonl", t)
        if not (ROOT / "eval/m4/b1/items.jsonl").exists():
            self.check_refused(text="missing or empty")

    @unittest.skipUnless(all(p.exists() for p in NEED), "pilot files or generated inputs not on this machine")
    def test_a_real_start_refuses_rubrics_without_chunk_ids(self):
        # the pilot's rubrics carry no chunk ids: only the dry run fills them into a copy
        self.check_refused("--items", str(PILOT_ITEMS), "--rubrics", str(PILOT_RUBS), text="fail the gate")

    @unittest.skipUnless(all(p.exists() for p in NEED), "pilot files or generated inputs not on this machine")
    def test_ids_of_items_and_rubrics_must_match(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        rows = [json.loads(l) for l in PILOT_RUBS.read_text().splitlines() if l.strip()]
        rows[0]["id"] = "zz-99"
        (tmp / "r.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        self.check_refused("--items", str(PILOT_ITEMS), "--rubrics", str(tmp / "r.jsonl"), text="do not match")

    @unittest.skipUnless(all(p.exists() for p in NEED), "pilot files or generated inputs not on this machine")
    def test_a_weight_2_point_needs_chunks_or_not_indexed_and_proposed_weights_are_refused(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        filled = tmp / "filled.jsonl"
        subprocess.run([str(ROOT / ".venv/bin/python"), "scripts/eval2_judge.py", "fill-chunks", "--rubrics", str(PILOT_RUBS), "--chunks",
                        str(ROOT / "work/v3/chunks_s1.jsonl"), "--out", str(filled)], cwd=ROOT, check=True, capture_output=True)
        rows = [json.loads(l) for l in filled.read_text().splitlines() if l.strip()]
        w2 = next(p for p in rows[0]["must"] if p["weight"] == 2)
        w2.pop("chunks", None), w2.pop("not_indexed", None)
        (tmp / "bad.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        self.check_refused("--items", str(PILOT_ITEMS), "--rubrics", str(tmp / "bad.jsonl"), text="fail the gate")
        rows = [json.loads(l) for l in filled.read_text().splitlines() if l.strip()]
        rows[0]["weights_status"] = "proposed by the author"
        (tmp / "prop.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        self.check_refused("--items", str(PILOT_ITEMS), "--rubrics", str(tmp / "prop.jsonl"), text="weights_status says proposed")
        self.check_refused("--items", str(PILOT_ITEMS), "--rubrics", str(tmp / "prop.jsonl"), "--proposed-weights", text="dry runs only")
        rows = [json.loads(l) for l in filled.read_text().splitlines() if l.strip()]
        rows[0]["must"][0]["chunks"] = ["no-such-chunk"]
        (tmp / "ghost.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        r = self.check_refused("--items", str(PILOT_ITEMS), "--rubrics", str(tmp / "ghost.jsonl"), text="fail the gate")
        self.assertIn("not in the frozen index", r.stderr)


@unittest.skipUnless(all(p.exists() for p in NEED), "pilot files or generated inputs not on this machine")
class DryRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        cls.work = cls.tmp / "w"
        cls.args = ("--dry-run", "--items", str(PILOT_ITEMS), "--rubrics", str(PILOT_RUBS), "--work", str(cls.work))
        cls.hb = ROOT / "runs/v3/HEARTBEAT.md"
        cls.hb_before = cls.hb.read_bytes() if cls.hb.exists() else None
        cls.marker = ROOT / "runs/m4/logs/m4_b1_coverage_batch.done"   # a real run may have left one; the dry run must not touch it
        cls.marker_before = cls.marker.stat().st_mtime_ns if cls.marker.exists() else None
        cls.first = sh(*cls.args)
        cls.second = sh(*cls.args)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, True)

    def test_first_run_completes_with_stubs(self):
        r = self.first
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        out = r.stdout
        for line in ("planned: 72 answers (24 items x default, strict, default_4k), 144 judge calls", "planned for 30 items: 90 answers", "180 judge calls",
                     "estimate (inferred from the pilot's rates", "packs identical in default, strict and default_4k: yes (24 items)",
                     "matched default: 24 answers", "matched strict: 24 answers", "matched default_4k: 24 answers",
                     "items 72, passes [1, 2], judge calls 144", "judging round 1: 0 unscored", "idcheck default:", "idcheck strict:", "idcheck default_4k:",
                     "dry run done"):
            self.assertIn(line, out)

    def test_answers_sets_and_what_the_rows_record(self):
        for s in ("default", "strict", "default_4k"):
            rows = [json.loads(l) for l in (self.work / f"ans_{s}.jsonl").read_text().splitlines() if l.strip()]
            self.assertEqual(len(rows), 24)
            for r in rows:
                self.assertTrue({"tokens", "truncated", "seconds", "pack"} <= set(r))
                self.assertTrue({"retrieval", "answer"} <= set(r["seconds"]))
        d = {r["id"]: r for r in map(json.loads, (self.work / "ans_default.jsonl").read_text().splitlines())}
        for s in ("strict", "default_4k"):
            m = json.loads((self.work / f"ans_{s}.jsonl.manifest.json").read_text())
            self.assertEqual(m["mode"], "strict" if s == "strict" else "default")
            self.assertEqual(m["max_tokens"], 4096 if s == "default_4k" else None)
            self.assertIsNotNone(m["replay"])
            for r in map(json.loads, (self.work / f"ans_{s}.jsonl").read_text().splitlines()):
                self.assertEqual(r["pack"], d[r["id"]]["pack"])
        self.assertIsNone(json.loads((self.work / "ans_default.jsonl.manifest.json").read_text())["max_tokens"])

    def test_judge_results_two_passes_all_sets_in_the_coverage_role(self):
        rows = [json.loads(l) for l in (self.work / "judge_results.jsonl").read_text().splitlines() if l.strip()]
        self.assertEqual(len(rows), 24 * 3 * 2)
        self.assertEqual({(r["set"], r["pass"]) for r in rows}, {(s, p) for s in ("default", "strict", "default_4k") for p in (1, 2)})
        self.assertTrue(all("pack" in r for r in rows))                 # --coverage: rows carry the answer's pack

    def test_report_written_with_all_sections_and_the_finishing_commands(self):
        t = (self.work / "report_from_stub_data.md").read_text()
        for s in ("DRY RUN", "## Coverage report", "WAITING", "## Identifier check", "idcheck default_4k", "## The raised limit", "### default_4k against default",
                  "cut off at the limit", "essential-miss units per 10", "mean paired difference (SE)", "## To finish the report",
                  "eval2_blind.py bundle", "--second-reader 0.25 --seed 20261119", "eval2_blind.py unblind", "eval2_pilot_report.py --coverage",
                  "--idcheck-false-invented N"):
            self.assertIn(s, t)
        self.assertEqual(t.count("```\n== idcheck"), 3)

    def test_done_marker_and_nothing_written_to_the_repository(self):
        self.assertTrue(any((self.work / "logs").glob("*.done")))
        hb = ROOT / "runs/v3/HEARTBEAT.md"
        self.assertEqual(self.hb_before, hb.read_bytes() if hb.exists() else None)
        self.assertEqual(self.marker_before, self.marker.stat().st_mtime_ns if self.marker.exists() else None)

    def test_second_run_resumes_without_redoing_finished_steps(self):
        r = self.second
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        self.assertIn("skip answers: all three sets are finished", r.stdout)
        self.assertIn("skip judging:", r.stdout)
        self.assertNotIn("judging round", r.stdout)
        self.assertNotIn("matched default:", r.stdout)

    def test_partial_state_continues_only_the_missing_step(self):
        # remove the judge results: answers are skipped (finished), judging is redone
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        w = tmp / "w"
        shutil.copytree(self.work, w)
        (w / "judge_results.jsonl").unlink()
        r = sh("--dry-run", "--items", str(PILOT_ITEMS), "--rubrics", str(PILOT_RUBS), "--work", str(w))
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        for n in ("default", "strict", "default_4k"):
            self.assertIn(f"skip answers {n}:", r.stdout)
        self.assertIn("judging round 1: 0 unscored", r.stdout)
        self.assertTrue((w / "judge_results.jsonl").exists())

    def test_one_missing_answer_set_is_answered_again_and_the_finished_ones_are_kept(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        w = tmp / "w"
        shutil.copytree(self.work, w)
        for f in ("ans_default_4k.jsonl", "ans_default_4k.jsonl.manifest.json", "judge_results.jsonl"):
            (w / f).unlink()
        r = sh("--dry-run", "--items", str(PILOT_ITEMS), "--rubrics", str(PILOT_RUBS), "--work", str(w))
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertIn("skip answers default:", r.stdout)
        self.assertIn("skip answers strict:", r.stdout)
        self.assertNotIn("skip answers default_4k", r.stdout)
        self.assertEqual(len((w / "ans_default_4k.jsonl").read_text().splitlines()), 24)


if __name__ == "__main__":
    unittest.main()
