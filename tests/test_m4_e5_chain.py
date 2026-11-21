"""Offline tests of scripts/m4_e5_bakeoff.sh and scripts/m4_e5_screen_helper.py (E5: the open screen of the base-model bake-off).

Stub generator, stub judge and a fake server status, all in temporary folders: no model, no model server, no network. The screen uses open items
only; nothing under the hidden folder is read, and the tests check that the script refuses any hidden-side path and names none.

Every dry run scores the reference's stored open answers on the CPU, so a dry run takes minutes; the cases share two of them."""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SH = ROOT / "scripts/m4_e5_bakeoff.sh"
HELP = ROOT / "scripts/m4_e5_screen_helper.py"
PY = ROOT / ".venv/bin/python"
NEED = [ROOT / "eval/m4/open/tier_a.jsonl", ROOT / "eval/m4/open/tier_a3.jsonl", ROOT / "eval/m4/b1/items.jsonl", ROOT / "eval/m4/b2/items.jsonl",
        ROOT / "eval/m4/b1/rubrics.jsonl", ROOT / "eval/m4/b2/rubrics.jsonl", ROOT / "work/v3/chunks_s1.jsonl", ROOT / "work/m4/s6/ans_A1.jsonl",
        ROOT / "work/m4/e2/ans_O_Q4D_a3.jsonl", ROOT / "work/m4/b1/ans_default.jsonl", ROOT / "work/m4/b2/ans_default.jsonl",
        ROOT / "work/m4/catalogues/CATALOGUES.json", ROOT / "runs/v3/INDEX_FROZEN.json", ROOT / "eval/m4/smoke_ids.json", PY]
READY = all(p.exists() for p in NEED)
CAND_IDS = {"gemma": "gemma-4-26B-A4B-it-QAT-MLX-4bit", "glm": "GLM-4.7-Flash-4bit", "granite": "granite-4.2-30b-q4-mlx"}
REF_FILES = {"A1_answers": "work/m4/s6/ans_A1.jsonl", "a3_answers": "work/m4/e2/ans_O_Q4D_a3.jsonl",
             "B1_answers": "work/m4/b1/ans_default.jsonl", "B2_answers": "work/m4/b2/ans_default.jsonl"}


def sh(*args, env=None, timeout=1800):
    e = dict(os.environ)
    e.update(env or {})
    return subprocess.run(["sh", str(SH), *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=e)


def helper(*args, stdin=None, timeout=300):
    return subprocess.run([str(PY), str(HELP), *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout, input=stdin)


def sha(p):
    import hashlib
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


class Static(unittest.TestCase):
    def test_syntax_and_executable(self):
        self.assertTrue(os.access(SH, os.X_OK))
        self.assertTrue(os.access(HELP, os.X_OK))
        self.assertEqual(subprocess.run(["sh", "-n", str(SH)]).returncode, 0)
        self.assertEqual(subprocess.run(["bash", "-n", str(SH)]).returncode, 0)

    def test_stage_order_and_guarded_jobs(self):
        t = SH.read_text()
        i = [t.index("# ---------------------------------------------------------------- PREPARE"), t.index("# ---------------------------------------------------------------- REFERENCE"),
             t.index("# ---------------------------------------------------------------- answering"), t.index("# ---------------------------------------------------------------- JUDGE"),
             t.index("# ---------------------------------------------------------------- SCORE")]
        self.assertEqual(i, sorted(i))
        # every job that loads a model or the reranker runs under the guard
        for need in ("$G --stall-min 40 --log $L/$N.${k}_$name.log -- .venv/bin/python scripts/eval2.py answer", "$G --stall-min 60 --log $L/$N.judge.log"):
            self.assertIn(need, t)
        # all answering finishes before the judge loads: the judge step comes after the candidate loop, and judges every set in one run
        self.assertLess(t.index("for k in $ALLK; do\n    case \" $SEL \""), t.index("prepare_server judge $DS"))
        self.assertEqual(t.count("scripts/eval2_judge.py run"), 1)

    def test_candidate_order_and_defaults(self):
        t = SH.read_text()
        self.assertIn('ALLK="gemma glm granite"', t)
        for k, v in CAND_IDS.items():
            self.assertIn(f"MODEL_{k}={v}", t)
        self.assertIn('CANDS="gemma,glm,granite"', t)
        self.assertIn('JFIRST="granite"', t)
        self.assertNotIn("gemma-4-31b", t)

    def test_no_address_key_host_or_home_path(self):
        for f in (SH, HELP):
            t = f.read_text()
            for bad in ("/Users/", "192.168.", "10.0.", "sk-", "Bearer sk", "omlx_url_value"):
                self.assertNotIn(bad, t, f"{f.name} holds {bad}")
            self.assertEqual(re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", t), [], f.name)

    def test_names_nothing_under_the_hidden_folder(self):
        for f in (SH, HELP):
            t = f.read_text()
            self.assertNotIn("eval/hidden", t, f.name)
            self.assertNotIn("tier_b_", t, f.name)
            self.assertNotIn("EVAL_HIDDEN_KEY", t, f.name)
            self.assertNotIn("eval_hidden.key", t, f.name)

    def test_changes_no_existing_file(self):
        # new files only: this task's files are untracked until the conductor commits them
        names = subprocess.run(["git", "ls-files", "--error-unmatch", "scripts/m4_e5_bakeoff.sh"], cwd=ROOT, capture_output=True, text=True)
        self.assertIn(names.returncode, (0, 1))


class Refusals(unittest.TestCase):
    def test_refuses_any_enc_argument(self):
        for args in (["--dry-run", "--work", "x.enc"], ["--report", "a/b.jsonl.enc"], ["--manifest", "eval/m4/hidden/x"], ["--dry-run", "hidden/x"], ["x.enc"]):
            r = sh(*args)
            self.assertEqual(r.returncode, 2, (args, r.stdout, r.stderr))
            self.assertIn("REFUSED", r.stderr)
            self.assertIn("hidden-side path", r.stderr)

    def test_unknown_option_stage_or_candidate(self):
        self.assertEqual(sh("--bogus").returncode, 2)
        self.assertEqual(sh("--stages", "H1").returncode, 2)
        r = sh("--candidates", "gemma,gemma31")
        self.assertEqual(r.returncode, 2)
        self.assertIn("unknown candidate", r.stderr)

    def test_the_helper_refuses_hidden_paths(self):
        for args in (["merge-rows", "--out", "x.jsonl", "--in", "a.jsonl.enc"], ["runline", "--name", "x", "--file", "a.enc"],
                     ["median", "--file", "eval/m4/hidden/f.jsonl"], ["pin-check", "--manifest", "m.enc"]):
            r = helper(*args)
            self.assertEqual(r.returncode, 2, (args, r.stdout, r.stderr))
            self.assertIn("hidden-side path", r.stderr)


class HelperUnits(unittest.TestCase):
    def test_mem_parse(self):
        for stdin, want in (('{"model_memory_used_formatted": "15.26 GB"}', "15.26"), ('{"model_memory_used": 16480000000}', "16.48"),
                            ('{"x": {"model_memory_used_formatted": "16100 MB"}}', "16.10"), ("{}", "none"), ("not json", "none"), ("", "none")):
            r = helper("mem-parse", stdin=stdin)
            self.assertEqual((r.returncode, r.stdout.strip()), (0, want), stdin)

    def test_answers_state_codes_and_running_median(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "a.jsonl"
            rows = [{"id": f"i{n}", "text": "x", "error": None, "seconds": {"total": float(n)}} for n in range(1, 6)]
            f.write_text("".join(json.dumps(r) + "\n" for r in rows))
            self.assertEqual(helper("answers-state", "--file", str(f), "--expect", "5").returncode, 1, "no manifest: not ended")
            Path(str(f) + ".manifest.json").write_text(json.dumps({"ended": "2026-01-01"}))
            self.assertEqual(helper("answers-state", "--file", str(f), "--expect", "5").returncode, 0)
            self.assertEqual(helper("answers-state", "--file", str(f), "--expect", "6").returncode, 1)
            rows[2]["error"] = "boom"
            f.write_text("".join(json.dumps(r) + "\n" for r in rows))
            self.assertEqual(helper("answers-state", "--file", str(f), "--expect", "5").returncode, 2)
            st = Path(d) / "st"
            r = helper("running-median", "--file", str(f), "--ref-median", "2.0", "--state", str(st), "--label", "x")
            self.assertIn("4 answers so far, running median 3.0 s", r.stdout)
            self.assertIn("ABOVE the limit", r.stdout)   # 3.0 against 1.15 x 2.0
            self.assertEqual(helper("running-median", "--file", str(f), "--ref-median", "2.0", "--state", str(st)).stdout, "", "prints only when the count changed")
            self.assertEqual(helper("judge-expect", "--passes", "2", "--answers", f"R={f}").stdout.strip(), "8")

    def test_pin_check_fails_on_a_changed_pin(self):
        if not READY:
            self.skipTest("inputs missing")
        with tempfile.TemporaryDirectory() as d:
            m = Path(d) / "m.json"
            r = helper("manifest", "--out", str(m), "--settings", "{}")
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(helper("manifest", "--out", str(m), "--settings", "{}").returncode, 2, "written once")
            self.assertEqual(helper("pin-check", "--manifest", str(m)).returncode, 0)
            doc = json.loads(m.read_text())
            doc["pinned"]["files"]["reference/A1_answers"] = "0" * 64
            m.write_text(json.dumps(doc))
            r = helper("pin-check", "--manifest", str(m))
            self.assertEqual(r.returncode, 1)
            self.assertIn("reference/A1_answers", r.stdout)


@unittest.skipUnless(READY, "inputs of the screen are missing")
class DryRuns(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="e5_test_")
        cls.w1 = Path(cls.tmp) / "w1"
        # one full dry run of all three candidates with a planted nondeterminism (gemma) and planted empty answers (glm)
        cls.r1 = sh("--dry-run", "--work", str(cls.w1), env={"M4_E5_DRY_NONDET": "gemma", "M4_E5_DRY_EMPTY": "glm"})

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_dry_run_exits_0_and_writes_the_report(self):
        self.assertEqual(self.r1.returncode, 0, self.r1.stdout[-3000:] + self.r1.stderr[-2000:])
        self.assertIn("dry run done", self.r1.stdout)
        rep = (self.w1 / "report_from_stub_data.md").read_text()
        self.assertIn("DRY RUN", rep)
        for k, v in CAND_IDS.items():
            self.assertIn(v, rep)
            self.assertTrue((self.w1 / k / "cand.done").exists())
        for need in ("Verdict per candidate", "Determinism check", "Server memory with the candidate loaded alone", "median", "Time on the server"):
            self.assertIn(need, rep)
        self.assertIn("M4_E5_DRY_RUN_DONE", (self.w1 / "logs/m4_e5_bakeoff.done").read_text())

    def test_a_failed_determinism_check_is_an_s1_failure(self):
        cmp_txt = (self.w1 / "screen_compare.txt").read_text()
        blk = re.search(r"^-- gemma: (\w+)\n  S1 (\w+):(.*)", cmp_txt, re.M)
        self.assertEqual(blk.group(2), "DROP", cmp_txt)
        self.assertIn("1 of 20 smoke answers differ between the two runs", cmp_txt)
        self.assertEqual(blk.group(1), "DROP")
        # the other candidates were deterministic
        self.assertNotIn("smoke answers differ", re.search(r"^-- granite:.*?(?=^go on)", cmp_txt, re.M | re.S).group(0))

    def test_a_candidate_with_empty_answers_is_an_s1_failure(self):
        cmp_txt = (self.w1 / "screen_compare.txt").read_text()
        blk = re.search(r"^-- glm: (\w+)\n  S1 (\w+):(.*)", cmp_txt, re.M)
        self.assertEqual(blk.group(2), "DROP", cmp_txt)
        self.assertIn("empty 2", blk.group(3))
        self.assertEqual(blk.group(1), "DROP")
        self.assertIn("S1 DROP", (self.w1 / "report_from_stub_data.md").read_text())

    def test_the_manifest_pins_the_reference_files(self):
        m = json.loads((self.w1 / "E5_SCREEN_MANIFEST.json").read_text())
        self.assertEqual(sorted(m["reference"]["files"]), sorted(REF_FILES))
        for k, rel in REF_FILES.items():
            e = m["reference"]["files"][k]
            self.assertEqual(e["file"], rel)
            self.assertEqual(e["sha256"], sha(ROOT / rel))
            self.assertEqual(m["pinned"]["files"][f"reference/{k}"], sha(ROOT / rel))
        self.assertEqual({k: m["reference"]["files"][k]["rows"] for k in REF_FILES}, {"A1_answers": 251, "a3_answers": 88, "B1_answers": 30, "B2_answers": 30})
        self.assertEqual(sorted(m["candidates"]), sorted(CAND_IDS))
        self.assertEqual(m["settings"]["extra_body"], {"chat_template_kwargs": {"enable_thinking": False}})
        self.assertEqual(m["judge"]["script_sha256"], sha(ROOT / "scripts/eval2_judge.py"))
        self.assertIn("equals_e2_manifest", m["judge"])
        self.assertTrue(m["dry_run"])

    def test_the_reference_is_never_regenerated_and_is_rescored(self):
        # the joined reference judged answers are byte-for-byte the two stored files
        joined = (self.w1 / "ref/ans_B_judged.jsonl").read_text()
        ids = [json.loads(l)["id"] for l in joined.splitlines()]
        self.assertEqual(len(ids), 60)
        self.assertEqual(ids, [json.loads(l)["id"] for l in (ROOT / REF_FILES["B1_answers"]).read_text().splitlines()]
                         + [json.loads(l)["id"] for l in (ROOT / REF_FILES["B2_answers"]).read_text().splitlines()])
        for nm, n in (("A1", 251), ("a3", 88)):
            d = json.loads((self.w1 / f"ref/{nm}.summary.json").read_text())
            self.assertEqual(d["items_scored"], n)
        # no answer file of the reference lives in the work folder under a candidate's name, and none was generated
        self.assertFalse((self.w1 / "R").exists())

    def test_one_judge_run_covers_the_reference_and_every_candidate(self):
        rows = [json.loads(l) for l in (self.w1 / "judge_results.jsonl").read_text().splitlines() if l.strip()]
        self.assertEqual({r["set"] for r in rows}, {"R", "gemma", "glm", "granite"})
        self.assertEqual(sum(1 for r in rows if r["set"] == "R"), 120)
        self.assertEqual(sum(1 for r in rows if r["set"] == "gemma"), 12)

    def test_memory_is_sampled_and_read_before_the_first_answer(self):
        for k in CAND_IDS:
            j = json.loads((self.w1 / k / "loaded.json").read_text())
            self.assertTrue(j["alone"])
            self.assertEqual(j["memory_gb"], 15.3)   # the fake status's stub figure
            self.assertTrue((self.w1 / k / "mem_samples.txt").read_text().strip())
        self.assertIn("peak 15.30 GB", (self.w1 / "report_from_stub_data.md").read_text())

    def test_granite_does_the_judged_scenarios_first(self):
        times = [l.split() for l in (self.w1 / "times.tsv").read_text().splitlines()]
        gr = [t[1] for t in times if t[0] == "granite" and t[1] != "total"]
        self.assertEqual(gr[0], "judged")
        ge = [t[1] for t in times if t[0] == "gemma" and t[1] != "total"]
        self.assertEqual(ge[-1], "judged")
        self.assertIn("[watch] granite judged", self.r1.stdout)

    def test_no_hidden_path_is_read_or_written(self):
        blob = self.r1.stdout + self.r1.stderr + (self.w1 / "report_from_stub_data.md").read_text() + (self.w1 / "HEARTBEAT.md").read_text()
        self.assertNotIn(".enc", blob)
        self.assertNotIn("eval/hidden", blob)
        self.assertEqual([p for p in self.w1.rglob("*") if p.suffix == ".enc"], [])

    def test_a_second_run_resumes_and_regenerates_nothing(self):
        before = {p.name: sha(p) for k in CAND_IDS for p in (self.w1 / k).glob("ans_*.jsonl")}
        mt = {p: p.stat().st_mtime_ns for p in self.w1.rglob("ans_*.jsonl")}
        r = sh("--dry-run", "--work", str(self.w1), env={"M4_E5_DRY_NONDET": "gemma", "M4_E5_DRY_EMPTY": "glm"})
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])
        for k in CAND_IDS:
            self.assertIn(f"skip candidate {k}: finished earlier", r.stdout)
        self.assertIn("manifest exists", r.stdout)
        self.assertIn("skip judging", r.stdout)
        self.assertEqual(before, {p.name: sha(p) for k in CAND_IDS for p in (self.w1 / k).glob("ans_*.jsonl")})
        self.assertEqual(mt, {p: p.stat().st_mtime_ns for p in self.w1.rglob("ans_*.jsonl")})

    def test_one_candidate_alone(self):
        w2 = Path(self.tmp) / "w2"
        shutil.copytree(self.w1 / "ref", w2 / "ref", copy_function=shutil.copy2)   # the scored reference, to keep the test short
        r = sh("--dry-run", "--work", str(w2), "--candidates", "glm")
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])
        self.assertTrue((w2 / "glm/cand.done").exists())
        self.assertFalse((w2 / "gemma/cand.done").exists())
        self.assertFalse((w2 / "granite/cand.done").exists())
        rep = (w2 / "report_from_stub_data.md").read_text()
        self.assertIn(CAND_IDS["glm"], rep)
        self.assertNotIn("## Candidate gemma", rep)
        self.assertNotIn("## Candidate granite", rep)
        cmp_txt = (w2 / "screen_compare.txt").read_text()
        self.assertIn("-- glm:", cmp_txt)
        self.assertNotIn("-- gemma:", cmp_txt)
        # a clean candidate has no S1 failure
        self.assertRegex(cmp_txt, r"S1 ok:")


if __name__ == "__main__":
    unittest.main()
