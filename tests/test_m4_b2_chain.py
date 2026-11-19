"""Offline tests of scripts/m4_b2_batch.sh (stub generator, stub judge, stub reviser; never the model server)."""
import json
import os
import subprocess
import tempfile
import shutil
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SH = ROOT / "scripts/m4_b2_batch.sh"
NEED = [ROOT / "eval/m4/b2/items.jsonl", ROOT / "eval/m4/b2/rubrics.jsonl", ROOT / "work/v3/chunks_s1.jsonl",
        ROOT / "work/m4/catalogues/CATALOGUES.json", ROOT / "runs/m5/PROFILE_R3.json", ROOT / ".venv/bin/python"]
READY = all(p.exists() for p in NEED)


def sh(*args, timeout=1500):
    return subprocess.run(["sh", str(SH), *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout)


class Static(unittest.TestCase):
    def test_syntax_and_executable(self):
        self.assertTrue(os.access(SH, os.X_OK))
        self.assertEqual(subprocess.run(["sh", "-n", str(SH)]).returncode, 0)
        self.assertEqual(subprocess.run(["bash", "-n", str(SH)]).returncode, 0)

    def test_order_of_the_seven_steps_and_no_4k_set(self):
        t = SH.read_text()
        i = [t.index("share_ok"), t.index("answer_run default default\n"), t.index("answer_run strict strict --replay-packs"),
             t.rindex("--revise-from"), t.index("unload $Q4; hb"), t.rindex("eval2_judge.py run"),
             t.index("eval2_idcheck.py --answers"), t.index("retrieval_funnel.py replay"), t.index("--presets r3"), t.index("7. the report\n")]
        self.assertEqual(i, sorted(i))
        for need in ("--instruction v2 --coverage", "--passes 1 2", "work/m5/b2_funnel.json", "m4_b2_batch.done", "revise_default.jsonl",
                     "runs/m4/B2_BATCH.md", "runs/m5/PROFILE_R3.json", "pp >= 15.0", "loss <= 5.0", "--r2"):
            self.assertIn(need, t)
        self.assertNotIn("default_4k", t)
        self.assertNotIn("--max-tokens 4096", t)

    def test_no_address_key_or_home_path_in_the_script(self):
        t = SH.read_text()
        for bad in ("/Users/", "http://", "https://", "192.168.", "sk-"):
            self.assertNotIn(bad, t)


@unittest.skipUnless(READY, "B2 files or generated inputs not on this machine")
class Gate(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def refused(self, chunk_check_text=None, text="chunk count"):
        cc = self.tmp / "cc.json"
        if chunk_check_text is not None:
            cc.write_text(chunk_check_text)
        w = self.tmp / "w"
        r = sh("--dry-run", "--work", str(w), "--chunk-check", str(cc))
        self.assertEqual(r.returncode, 2, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertIn("REFUSED", r.stderr)
        self.assertIn(text, r.stderr)
        self.assertFalse((w / "ans_default.jsonl").exists())      # refused before any answer
        return r

    def test_refuses_without_the_chunk_check_file(self):
        self.refused(None, "is missing")

    def test_refuses_below_95_percent(self):
        self.refused('{"share_ok": 0.94}', "below 0.95")

    def test_refuses_when_share_ok_is_missing_or_not_a_number(self):
        self.refused('{"other": 1}', "below 0.95")
        self.refused('{"share_ok": "0.99"}', "below 0.95")

    def test_unknown_option(self):
        self.assertEqual(sh("--nonsense").returncode, 2)

    def test_default_inputs_are_the_b2_files(self):
        t = SH.read_text()
        self.assertIn("eval/m4/b2/items.jsonl", t)
        self.assertIn("eval/m4/b2/rubrics.jsonl", t)
        self.assertIn("runs/m4/B2_CHUNK_CHECK.json", t)


@unittest.skipUnless(READY, "B2 files or generated inputs not on this machine")
class DryRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        cls.cc = cls.tmp / "cc.json"
        cls.cc.write_text('{"share_ok": 0.95}')
        cls.work = cls.tmp / "w"
        cls.hb = ROOT / "runs/v3/HEARTBEAT.md"
        cls.hb_before = cls.hb.read_bytes() if cls.hb.exists() else None
        cls.marker = ROOT / "runs/m4/logs/m4_b2_batch.done"
        cls.marker_before = cls.marker.stat().st_mtime_ns if cls.marker.exists() else None
        cls.args = ("--dry-run", "--work", str(cls.work), "--chunk-check", str(cls.cc))
        cls.first = sh(*cls.args)
        cls.second = sh(*cls.args)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, True)

    def test_all_seven_steps_run_and_exit_0(self):
        r = self.first
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        for line in ("gate: independent chunk count share_ok = 0.95", "gate: fill-chunks --check clean", "planned: 60 answers (30 items x default, strict), 120 judge calls",
                     "packs identical in default and strict: yes (30 items)", "items 60, passes [1, 2], judge calls 120", "idcheck default:", "idcheck strict:",
                     "equals runs/m5/PROFILE_R3.json", "needs of B2: ", "replay of 30 stored packs", "R3 rule (MILESTONE.md, K3)",
                     "(a) r3 minus frozen", "(b) of the", "R3 funnel check:", "labelled 'decides nothing'", "R2 patches accepted", "dry run done"):
            self.assertIn(line, r.stdout)

    def test_outputs_in_the_work_folder_only(self):
        for f in ("ans_default.jsonl", "ans_strict.jsonl", "judge_results.jsonl", "idcheck_default.txt", "b2_funnel.json", "revise_default.jsonl",
                  "report_from_stub_data.md"):
            self.assertTrue((self.work / f).exists(), f)
        self.assertEqual(len((self.work / "revise_default.jsonl").read_text().splitlines()), 30)
        rows = [json.loads(l) for l in (self.work / "judge_results.jsonl").read_text().splitlines() if l.strip()]
        self.assertEqual({(r["set"], r["pass"]) for r in rows}, {(s, p) for s in ("default", "strict") for p in (1, 2)})
        t = (self.work / "report_from_stub_data.md").read_text()
        for s in ("DRY RUN", "## Gate", "## R3 funnel check", "## First look at the revise step", "## Identifier check"):
            self.assertIn(s, t)
        self.assertTrue(any((self.work / "logs").glob("*.done")))
        hb = ROOT / "runs/v3/HEARTBEAT.md"
        self.assertEqual(self.hb_before, hb.read_bytes() if hb.exists() else None)
        self.assertEqual(self.marker_before, self.marker.stat().st_mtime_ns if self.marker.exists() else None)

    def test_second_run_resumes(self):
        r = self.second
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        for s in ("skip answers:", "skip judging:", "skip funnel:", "skip revise:"):
            self.assertIn(s, r.stdout)


if __name__ == "__main__":
    unittest.main()
