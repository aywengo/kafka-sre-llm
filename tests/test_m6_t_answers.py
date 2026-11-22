"""M6 T-chain: the helpers of scripts/m6_t_report.py (gate of the set T, the judge's path on all 41 real T rubrics, manifest and start check, marks, the measures on synthetic
rows) and the refusals and the dry run of scripts/m6_t_answers.sh. Offline, CPU only; no server, no model (the dry run uses the stub backend and the stub judge)."""
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import m6_t_report as T  # noqa: E402

SCRIPT = ROOT / "scripts/m6_t_answers.sh"
ITEMS, RUBS, CHUNKS = ROOT / "eval/m6/t/items.jsonl", ROOT / "eval/m6/t/rubrics.jsonl", ROOT / "work/v3/chunks_s1.jsonl"


def synthetic_items(n_wrong=3, n_right=2):
    return [{"id": f"s-{i}", "question": "q", "t_kind": "tempting_wrong" if i < n_wrong else "destructive_right"} for i in range(n_wrong + n_right)]


def answer_row(i, text="An answer.", secs=None, truncated=False, tokens=100):
    return {"id": i, "text": text, "tokens": tokens, "truncated": truncated, "pack": ["a"], "error": None,
            "seconds": secs or {"search_terms": 1.0, "retrieval": 2.0, "answer": 10.0, "total": 13.0}}


class GateTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.d = Path(self.td.name)

    def write(self, items, rubs):
        (self.d / "i.jsonl").write_text("".join(json.dumps(x) + "\n" for x in items))
        (self.d / "r.jsonl").write_text("".join(json.dumps(x) + "\n" for x in rubs))
        return str(self.d / "i.jsonl"), str(self.d / "r.jsonl")

    def test_a_good_pair_passes_and_each_defect_is_named(self):
        items = synthetic_items()
        rubs = [{"id": it["id"], "must": [{"id": "m1"}]} for it in items]
        self.assertEqual(T.gate_files(*self.write(items, rubs), 5, {"tempting_wrong": 3, "destructive_right": 2}), [])
        self.assertTrue(any("rubrics" in p for p in T.gate_files(*self.write(items, rubs[:-1]), 5)))                      # a missing rubric
        self.assertTrue(any("differ in ids" in p for p in T.gate_files(*self.write(items, rubs[:-1] + [{"id": "zz", "must": [{"id": "m1"}]}]), 5)))
        bad = [dict(items[0], t_kind="other")] + items[1:]
        self.assertTrue(any("t_kind" in p for p in T.gate_files(*self.write(bad, rubs), 5)))
        self.assertTrue(any("no must" in p for p in T.gate_files(*self.write(items, rubs[:-1] + [{"id": "s-4", "must": []}]), 5)))
        self.assertTrue(any("missing" in p for p in T.gate_files(str(self.d / "none.jsonl"), str(self.d / "r.jsonl"), 5)))
        self.assertTrue(any("counts" in p for p in T.gate_files(*self.write(items, rubs), 5, {"tempting_wrong": 33, "destructive_right": 8})))

    def test_the_real_files_pass(self):
        self.assertEqual(T.gate_files(str(ITEMS), str(RUBS), 41, T.KINDS), [])


@unittest.skipUnless(CHUNKS.is_file(), "the chunk file work/v3/chunks_s1.jsonl is not here")
class JudgePathOnRealRubrics(unittest.TestCase):
    def test_the_prompt_builder_the_result_row_and_coverage_row_accept_all_41_t_rubrics(self):
        r = T.judge_offline_check(str(ITEMS), str(RUBS), str(CHUNKS))
        self.assertEqual(r["problems"], [])
        self.assertEqual((r["rubrics"], r["prompts"], r["rows"]), (41, 82, 82))
        self.assertEqual(r["rule"], {"chunk": 82})
        self.assertGreater(r["not_indexed_points"], 0)                       # the points of T that no chunk states are in the rubrics
        for k in ("severity", "consequence", "condition", "near_miss"):      # the extra must-not fields travel with the rubric
            self.assertIn(k, r["must_not_fields"])

    def test_a_rubric_the_builder_cannot_read_is_reported_not_raised(self):
        with tempfile.TemporaryDirectory() as td:
            rubs = [json.loads(l) for l in open(RUBS)][:2]
            for p in rubs[0]["must"]:
                p.pop("text")                                                # a must point without text
            Path(td, "r.jsonl").write_text("".join(json.dumps(x) + "\n" for x in rubs))
            Path(td, "i.jsonl").write_text("".join(l for l in open(ITEMS).readlines()[:2]))
            try:
                r = T.judge_offline_check(str(Path(td, "i.jsonl")), str(Path(td, "r.jsonl")), str(CHUNKS))
            except KeyError:
                return                                                        # a KeyError is the loud failure; the chain turns it into a refusal (exit 2 in main)
            self.assertTrue(r["problems"])


class MarkTests(unittest.TestCase):
    def test_marks_patterns_labels_and_negation(self):
        plain = T.marks_for_answer("Run `kubectl delete pvc data-0 -n kafka` now.")
        self.assertGreaterEqual(plain["marks"], 1)
        self.assertEqual(plain["labelled"], 0)
        self.assertEqual(len(plain["patterns"]), plain["marks"])
        lab = T.marks_for_answer("This is destructive and irreversible: run `kubectl delete pvc data-0 -n kafka`.")
        self.assertEqual(lab["labelled"], lab["marks"])
        self.assertGreaterEqual(lab["marks"], 1)
        neg = T.marks_for_answer("Do not run `kubectl delete pvc data-0`.")
        self.assertEqual((neg["marks"], neg["patterns"]), (0, []))
        self.assertGreaterEqual(neg["skipped"], 1)
        fence = T.marks_for_answer("Steps:\n\n```bash\n# caution, this loses data\nkubectl delete pvc data-0 -n kafka\n```\n")
        self.assertEqual((fence["marks"], fence["labelled"]), (1, 1))
        self.assertEqual(T.marks_for_answer("Check the logs.")["marks"], 0)

    def test_the_marks_command_agrees_with_the_checkers_report_and_changes_no_text(self):
        with tempfile.TemporaryDirectory() as td:
            rows = [answer_row("a", "Run `kubectl delete pvc data-0`."), answer_row("b", "Look at the logs."), answer_row("c", "Do not run `kubectl delete pvc x`.")]
            src = Path(td, "ans.jsonl")
            src.write_text("".join(json.dumps(r) + "\n" for r in rows))
            before = src.read_bytes()
            code = T.main(["marks", "--answers", str(src), "--out", str(Path(td, "m.jsonl")), "--report", str(Path(td, "m.txt"))])
            self.assertEqual(code, 0)
            self.assertEqual(src.read_bytes(), before)
            per = {r["id"]: r for r in map(json.loads, open(Path(td, "m.jsonl")))}
            self.assertGreaterEqual(per["a"]["marks"], 1)
            self.assertEqual((per["b"]["marks"], per["c"]["marks"]), (0, 0))
            self.assertIn("answers 3, with a mark 1", Path(td, "m.txt").read_text())


class MeasureTests(unittest.TestCase):
    def test_the_split_by_t_kind_the_paired_difference_and_the_marks_counts(self):
        items = synthetic_items(3, 2)                                         # s-0..s-2 tempting_wrong, s-3 s-4 destructive_right
        self.assertEqual(T.split_ids(items, "all"), ["s-0", "s-1", "s-2", "s-3", "s-4"])
        self.assertEqual(T.split_ids(items, "tempting_wrong"), ["s-0", "s-1", "s-2"])
        self.assertEqual(T.split_ids(items, "destructive_right"), ["s-3", "s-4"])
        ref = [answer_row(i["id"], secs={"search_terms": 1.0, "retrieval": 2.0, "answer": 10.0 + n, "total": 13.0 + n}) for n, i in enumerate(items)]
        var = [answer_row(i["id"], secs={"search_terms": 0.0, "retrieval": 0.0, "answer": 20.0 + n, "total": 20.0 + n}, truncated=(n == 4)) for n, i in enumerate(items)]
        P_ref = [0.5, 0.6, 0.7, 0.2, 0.4]
        P_var = [0.6, 0.6, 0.9, 0.4, 0.4]
        judged = {**{("default", i["id"]): (p, 0, 2) for i, p in zip(items, P_ref)}, **{("g2", i["id"]): (p, 0, 2) for i, p in zip(items, P_var)}}
        # paired difference of g2 minus default: all five, then the two kinds
        pa = T.paired(judged, T.split_ids(items, "all"))
        self.assertAlmostEqual(pa["mean"], (0.1 + 0 + 0.2 + 0.2 + 0) / 5)
        self.assertEqual(pa["n"], 5)
        d = [0.1, 0.0, 0.2, 0.2, 0.0]
        m_ = sum(d) / 5
        self.assertAlmostEqual(pa["se"], (sum((x - m_) ** 2 for x in d) / 4) ** 0.5 / 5 ** 0.5)
        pt = T.paired(judged, T.split_ids(items, "tempting_wrong"))
        self.assertAlmostEqual(pt["mean"], 0.1)
        self.assertEqual(pt["n"], 3)
        pd = T.paired(judged, T.split_ids(items, "destructive_right"))
        self.assertAlmostEqual(pd["mean"], 0.1)
        self.assertEqual(T.paired(judged, [])["mean"], None)
        # arm stats: marks, cut, seconds (the variant gets the reference's search and retrieval seconds), declines, identifiers
        marks_ref = {"s-0": {"marks": 2, "patterns": ["D9", "D9p"], "labelled": 1, "skipped": 0, "doubtful": 0},
                     "s-1": {"marks": 0, "patterns": [], "labelled": 0, "skipped": 1, "doubtful": 0},
                     "s-2": {"marks": 1, "patterns": ["D9"], "labelled": 0, "skipped": 0, "doubtful": 0},
                     "s-3": {"marks": 1, "patterns": ["D2"], "labelled": 1, "skipped": 0, "doubtful": 0},
                     "s-4": {"marks": 0, "patterns": [], "labelled": 0, "skipped": 0, "doubtful": 0}}
        ref[4]["text"] = "The retrieved documentation does not cover this."
        e_all = T.arm_split_stats(ref, ref, T.split_ids(items, "all"), "default", judged, marks_ref, (5, 1), False)
        self.assertEqual((e_all["answers"], e_all["marked_answers"], e_all["marks"], e_all["labelled_marks"], e_all["skipped_matches"]), (5, 3, 4, 2, 1))
        self.assertAlmostEqual(e_all["marks_per_answer"], 0.8)
        self.assertEqual(e_all["marks_by_pattern"], {"D2": 1, "D9": 2, "D9p": 1})
        self.assertAlmostEqual(e_all["idents_per10"], 2.0)
        self.assertEqual(e_all["declines"], 1)
        self.assertEqual(e_all["median_total_s"], 10.0 + 2 + 3.0)         # median of 13..17
        e_r = T.arm_split_stats(ref, ref, T.split_ids(items, "destructive_right"), "default", judged, marks_ref, None, False)
        self.assertEqual((e_r["answers"], e_r["marks"], e_r["declines"]), (2, 1, 1))
        e_t = T.arm_split_stats(ref, ref, T.split_ids(items, "tempting_wrong"), "default", judged, marks_ref, None, False)
        self.assertEqual((e_t["answers"], e_t["marked_answers"], e_t["marks"], e_t["declines"]), (3, 2, 3, 0))
        marks_var = {i["id"]: {"marks": 0, "patterns": [], "labelled": 0, "skipped": 0, "doubtful": 0} for i in items}
        v = T.arm_split_stats(var, ref, T.split_ids(items, "all"), "g2", judged, marks_var, None, False)
        self.assertEqual(v["cut"], 1)
        self.assertEqual(v["median_answer_s"], 22.0)
        self.assertEqual(v["median_total_s"], 25.0)                         # answer seconds 20..24 plus the reference's 1 + 2, median 22 + 3

    def test_a_missing_token_count_is_an_error(self):
        items = synthetic_items(1, 0)
        rows = [answer_row("s-0", tokens=None)]
        judged = {("default", "s-0"): (0.5, 0, 2)}
        with self.assertRaises(T.TError):
            T.arm_split_stats(rows, rows, ["s-0"], "default", judged, {"s-0": {"marks": 0, "patterns": [], "labelled": 0, "skipped": 0, "doubtful": 0}}, None, False)

    def test_the_markdown_has_every_split_and_arm_and_the_closing_block(self):
        items = synthetic_items(3, 2)
        rows = [answer_row(i["id"]) for i in items]
        judged = {(a, i["id"]): (0.5, 0, 2) for a in T.ARMS for i in items}
        mk = {i["id"]: {"marks": 0, "patterns": [], "labelled": 0, "skipped": 0, "doubtful": 0} for i in items}
        m = {"sizes": {s: len(T.split_ids(items, s)) for s in T.SPLITS}, "arms": {}, "paired": {s: T.paired(judged, T.split_ids(items, s)) for s in T.SPLITS}}
        for arm in T.ARMS:
            m["arms"][arm] = {s: T.arm_split_stats(rows, rows, T.split_ids(items, s), arm, judged, mk, (0, 3), False) for s in T.SPLITS}
        md = T.measures_md(m)
        for s in T.SPLITS:
            for arm in T.ARMS:
                self.assertIn(f"| {s} | {arm} |", md)
        self.assertIn("What this run does not show", md)
        self.assertIn("decision S7", md)
        self.assertIn("built to tempt", md)
        self.assertIn("counted by the blind readers", md)


class StateTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.addCleanup(self.td.cleanup)
        self.root = Path(self.td.name)
        self.pin = self.root / "input.txt"
        self.pin.write_text("v1\n")
        self.w = self.root / "w"
        self.w.mkdir()

    def argv(self, cmd):
        return [cmd, "--manifest", str(self.w / "MANIFEST.json"), "--work", str(self.w), "--pin", str(self.pin), "--frozen", str(ROOT / "kafka_sre_rag/frozen.json"), "--dry",
                "--opt", "set=T"]

    def test_manifest_then_a_changed_pin_and_a_changed_marker_output_are_refused_and_an_orphan_marker_too(self):
        (self.w / "stage_x.done").write_text("{}")
        self.assertEqual(T.main(self.argv("start-check")), 2)               # a marker without a manifest
        (self.w / "stage_x.done").unlink()
        self.assertEqual(T.main(self.argv("start-check")), 0)               # a fresh start
        self.assertEqual(T.main(self.argv("manifest")), 0)
        self.assertEqual(T.main(self.argv("start-check")), 0)
        out = self.w / "out.txt"
        out.write_text("o1\n")
        mh = T.G.manifest_hash(json.load(open(self.w / "MANIFEST.json")))
        (self.w / "stage_x.done").write_text(json.dumps({"stage": "x", "manifest_sha256": mh, "outputs": {str(out): T.sha256_file(out)}}))
        self.assertEqual(T.main(self.argv("start-check")), 0)
        out.write_text("o2\n")
        self.assertEqual(T.main(self.argv("start-check")), 2)               # an output changed
        out.write_text("o1\n")
        self.pin.write_text("v2\n")
        self.assertEqual(T.main(self.argv("start-check")), 2)               # a pinned file changed


IMPORT_TRACE = """
import sys, os, importlib
sys.path[:0] = ['scripts', '.']
for m in ('eval2', 'eval2_judge', 'eval2_score', 'eval2_idcheck', 'eval2_compare', 'run_guarded', 'm6_t_report', 'm6_d5_rule', 'm5_g_rule', 'm5_b4_served', 'm6_d5_funnel',
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
        self.assertEqual(sorted(imported - set(T.CODE_PINS)), [], "imported by the chain but not pinned")
        for f in T.CODE_PINS:
            self.assertTrue((ROOT / f).is_file(), f)
        self.assertIn("scripts/m6_t_answers.sh", T.CODE_PINS)

    def test_the_script_takes_its_pins_from_code_pins_and_passes_the_commit_flag(self):
        txt = SCRIPT.read_text()
        self.assertIn("$R code-pins", txt)
        self.assertIn("--allow-commit-change", txt)
        self.assertIn("--replay-packs", txt)
        for stage in ("reference", "answers", "marks", "judge", "checks", "measures"):
            self.assertIn(f"recheck {stage}", txt)


def sh(args, td, cwd=ROOT):
    import os
    env = {**os.environ, "TMPDIR": str(td)}
    return subprocess.run(["sh", str(SCRIPT), *args], cwd=cwd, capture_output=True, text=True, env=env, timeout=600)


class ScriptTests(unittest.TestCase):
    def test_syntax(self):
        r = subprocess.run(["sh", "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_no_address_host_key_or_home_path_in_the_new_files(self):
        for p in (SCRIPT, ROOT / "scripts/m6_t_report.py", Path(__file__)):
            txt = p.read_text()
            self.assertIsNone(re.search(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", txt), p)
            self.assertNotIn("/" + "Users/", txt, p)
            self.assertNotIn("/private" + "/tmp", txt, p)
            self.assertIsNone(re.search(r"\bsk-[A-Za-z0-9]{8,}", txt), p)

    def test_real_run_options_and_a_dry_run_on_the_real_folders_are_refused_and_write_nothing(self):
        with tempfile.TemporaryDirectory() as td:
            r = sh(["--items", "a.jsonl"], td)
            self.assertEqual(r.returncode, 2)
            self.assertIn("dry runs only", r.stderr)
            r = sh(["--max-unjudged", "x"], td)
            self.assertEqual(r.returncode, 2)
            r = sh(["--dry-run", "--work", "work/m6/t"], td)
            self.assertEqual(r.returncode, 2)
            self.assertIn("real work folder", r.stderr)
            r = sh(["--dry-run", "--work", str(Path(td) / "w"), "--report", "runs/m6/T_ANSWERS.md"], td)
            self.assertEqual(r.returncode, 2)
            self.assertIn("real report", r.stderr)
            r = sh(["--dry-run", "--work", str(Path(td) / "w"), "--items", "eval/m6/t/items.jsonl", "--rubrics", "eval/m6/t/rubrics.jsonl"], td)
            self.assertEqual(r.returncode, 2)
            self.assertFalse((Path(td) / "w").exists())

    def test_a_rerun_after_a_stop_in_the_judge_stage_keeps_the_answers_and_finishes(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            w = td / "w"
            r = sh(["--dry-run", "--work", str(w)], td)
            self.assertEqual(r.returncode, 0, r.stdout[-500:] + r.stderr[-500:])
            files = ("ans_t_default.jsonl", "ans_t_g2.jsonl", "ans_t_default.jsonl.manifest.json", "ans_t_g2.jsonl.manifest.json", "marks_g2.jsonl")
            before = {f: (w / f).read_bytes() for f in files}
            for f in ("stage_judge.done", "stage_checks.done", "judge_t.jsonl", "judge_cache_t.jsonl", "measures.json", "logs/m6_t_answers.done", "T_ANSWERS_from_stub_data.md"):
                (w / f).unlink(missing_ok=True)
            (w / "logs" / "m6_t_answers.failed").write_text("judging t round 1 failed\n")
            r2 = sh(["--dry-run", "--work", str(w)], td)
            self.assertEqual(r2.returncode, 0, r2.stdout[-800:] + r2.stderr[-800:])
            self.assertIn("rerun after a stop", r2.stdout)
            self.assertFalse((w / "logs" / "m6_t_answers.failed").exists())
            self.assertEqual(r2.stdout.strip().splitlines()[-1], "T ANSWERS DONE: 82 answers, 164 judge rows")
            self.assertEqual(before, {f: (w / f).read_bytes() for f in files})
            self.assertNotIn("answers m6-t", r2.stdout)

    def test_gate_refusals_a_missing_rubric_a_changed_item_file_and_a_state_left_by_an_earlier_run(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            items, rubs, short = td / "items.jsonl", td / "rubrics.jsonl", td / "rubrics_short.jsonl"
            items.write_text(ITEMS.read_text())
            rubs.write_text(RUBS.read_text())
            short.write_text("".join(RUBS.read_text().splitlines(True)[1:]))
            r = sh(["--dry-run", "--work", str(td / "wa"), "--items", str(items), "--rubrics", str(short)], td)
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
            self.assertIn("REFUSED", r.stderr)
            self.assertFalse((td / "wa").exists())                               # nothing written before the gate passed
            # a state left by an earlier run: a work folder with files and no manifest, and a failed marker
            (td / "wc").mkdir()
            (td / "wc" / "ans_t_default.jsonl").write_text("x\n")
            r = sh(["--dry-run", "--work", str(td / "wc")], td)
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
            self.assertIn("state of an earlier run", r.stderr)
            (td / "wd" / "logs").mkdir(parents=True)
            (td / "wd" / "logs" / "m6_t_answers.failed").write_text("boom\n")
            r = sh(["--dry-run", "--work", str(td / "wd")], td)
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
            self.assertIn("no MANIFEST.json", r.stderr)
            # a changed item file after the manifest exists (the dry run to the end, then one byte changed): the start check refuses
            w = td / "wb"
            r = sh(["--dry-run", "--work", str(w), "--items", str(items), "--rubrics", str(rubs)], td)
            self.assertEqual(r.returncode, 0, r.stdout[-800:] + r.stderr[-800:])
            self.assertEqual(r.stdout.strip().splitlines()[-1], "T ANSWERS DONE: 82 answers, 164 judge rows")
            snap = lambda: {str(p.relative_to(w)): T.sha256_file(p) for p in sorted(w.rglob("*")) if p.is_file()}      # noqa: E731
            before = snap()
            r2 = sh(["--dry-run", "--work", str(w), "--items", str(items), "--rubrics", str(rubs)], td)      # a resume of a finished run changes nothing
            self.assertEqual(r2.returncode, 0, r2.stdout[-500:] + r2.stderr[-500:])
            self.assertIn("already done", r2.stdout)
            self.assertEqual(before, snap())
            items.write_text(items.read_text().replace("Anything I am missing?", "Anything I am missing??", 1))
            r3 = sh(["--dry-run", "--work", str(w), "--items", str(items), "--rubrics", str(rubs)], td)
            self.assertEqual(r3.returncode, 2, r3.stdout[-500:] + r3.stderr[-500:])
            self.assertIn("differs from the state now", r3.stderr)
            self.assertEqual(before, snap())
            # what the dry run wrote: both answer files with 41 rows, the report with the header first and the done line last
            self.assertEqual(sum(1 for _ in open(w / "ans_t_default.jsonl")), 41)
            self.assertEqual(sum(1 for _ in open(w / "ans_t_g2.jsonl")), 41)
            rep = (w / "T_ANSWERS_from_stub_data.md").read_text()
            self.assertLess(rep.index("What this run measures"), rep.index("### Measures"))
            self.assertEqual(rep.strip().splitlines()[-1], "T ANSWERS DONE: 82 answers, 164 judge rows")
            self.assertEqual((w / "logs" / "m6_t_answers.done").read_text().strip(), "T ANSWERS DONE: 82 answers, 164 judge rows")


if __name__ == "__main__":
    unittest.main()
