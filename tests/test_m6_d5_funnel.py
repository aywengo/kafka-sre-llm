"""Offline tests of scripts/m6_d5_funnel.py (the rule on synthetic tables, the counts through the shared counter of m5_b4_served.py) and of
scripts/m6_d5_funnel.sh (dry runs on a synthetic batch with a stub model, stand-in rerankers and a fake server status; never the model
server, never D5)."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SH = ROOT / "scripts/m6_d5_funnel.sh"
PY = ROOT / "scripts/m6_d5_funnel.py"
sys.path.insert(0, str(ROOT / "scripts"))
import m5_b4_served as S  # noqa: E402
import m6_d5_funnel as F  # noqa: E402

NEED = (ROOT / "work/v3/chunks_s1.jsonl", ROOT / ".venv/bin/python", ROOT / "runs/v3/INDEX_FROZEN.json", ROOT / "scripts/eval2_compare.py",
        ROOT / "runs/m5/RERANKER_R5.json")
READY = all(p.exists() for p in NEED)
READY_REASON = "files not in a clone (work/ and runs/ are git-ignored; the chain script starts .venv/bin/python); missing: " + \
    (", ".join(str(p.relative_to(ROOT)) for p in NEED if not p.exists()) or "none")


def pt(pid, sets, weight=1):
    return {"id": pid, "weight": weight, "chunks": sorted({c for s in sets for c in s}), "chunk_sets": [list(s) for s in sets]}


def table(n_points, frozen_served, served, extra=None, lose=0):
    """One scenario per point. The frozen pack serves the first `frozen_served` points; the setting's pack serves the first `served`
    (it starts with the frozen pack unless `lose` points of the frozen-served ones are dropped from it). r5a = extra (a number of points served)."""
    rubrics = [{"id": f"i{i}", "must": [pt("m1", [[f"p{i}"]])]} for i in range(n_points)]
    packs = {}
    for i in range(n_points):
        fz = [f"p{i}"] if i < frozen_served else ["zz"]
        st = list(fz) + ([f"p{i}"] if frozen_served <= i < served else [])
        if i < lose:
            st = ["zz"]
        r5 = list(fz) + ([f"p{i}"] if frozen_served <= i < (served if extra is None else extra) else [])
        packs[f"i{i}"] = {"frozen": fz, F.SETTING: st, F.INFO: r5}
    return rubrics, packs


FAST = {"median": 3.0, "p90": 5.0}


def verdict_of(rubrics, packs, timing=FAST):
    res = F.three_way(rubrics, packs)
    return F.rule_verdict(res[F.SETTING]["strict"]["all"], timing), res


class Rule(unittest.TestCase):
    def test_a_pass(self):
        v, res = verdict_of(*table(20, 2, 6))          # +4 of 20 = +20.0
        self.assertTrue(v["go"], v)
        self.assertEqual(F.verdict_line(v), "VERDICT: r4g2 GOES ON TO B4")
        c = res[F.SETTING]["strict"]["all"]
        self.assertEqual((c["indexed_points"], c["frozen_served"], c["r4_served"]), (20, 2, 6))

    def test_plus_15_point_0_exactly_passes_and_under_fails(self):
        self.assertTrue(verdict_of(*table(20, 2, 5))[0]["go"])               # +3 of 20 = +15.0
        v, _ = verdict_of(*table(20, 2, 4))                                  # +2 of 20 = +10.0
        self.assertFalse(v["go"])
        self.assertFalse(v["conditions"]["gain"])
        self.assertTrue(F.verdict_line(v).startswith("VERDICT: STOP, NO B4 (failing: "))

    def test_a_lost_frozen_served_point_stops(self):
        v, res = verdict_of(*table(20, 4, 10, lose=1))
        self.assertEqual(res[F.SETTING]["strict"]["all"]["lost"], 1)
        self.assertTrue(v["conditions"]["gain"])
        self.assertFalse(v["conditions"]["no_loss"])
        self.assertFalse(v["go"])

    def test_too_slow_stops_and_the_edges_pass(self):
        r, p = table(20, 2, 8)
        self.assertTrue(verdict_of(r, p, {"median": 6.0, "p90": 9.0})[0]["go"])
        v, _ = verdict_of(r, p, {"median": 6.01, "p90": 5.0})
        self.assertFalse(v["go"] or v["conditions"]["median"])
        v, _ = verdict_of(r, p, {"median": 3.0, "p90": 9.01})
        self.assertFalse(v["go"] or v["conditions"]["p90"])
        self.assertFalse(verdict_of(r, p, None)[0]["go"])                      # not measured is not a pass

    def test_r5a_better_than_r4g2_does_not_change_the_verdict(self):
        base, _ = verdict_of(*table(20, 2, 4))                                 # a STOP on the gain
        better, res = verdict_of(*table(20, 2, 4, extra=15))                   # r5a serves far more
        self.assertEqual((base["go"], base["conditions"]), (better["go"], better["conditions"]))
        self.assertFalse(better["go"])
        self.assertGreater(res[F.INFO]["strict"]["all"]["diff_pp"], res[F.SETTING]["strict"]["all"]["diff_pp"])
        self.assertGreater(res["r5a_vs_r4g2"]["strict"]["all"]["diff_points"], 0)
        # and the other way round: a PASS stays a PASS when r5a is worse
        ok, _ = verdict_of(*table(20, 2, 8, extra=2))
        self.assertTrue(ok["go"])

    def test_no_indexed_points_is_a_stop(self):
        rubrics = [{"id": "i0", "must": [{"id": "m1", "chunks": [], "chunk_sets": [], "not_indexed": True}]}]
        packs = {"i0": {"frozen": ["a"], F.SETTING: ["a"], F.INFO: ["a"]}}
        v, _ = verdict_of(rubrics, packs)
        self.assertFalse(v["go"])

    def test_the_counts_come_from_the_shared_counter(self):
        self.assertIs(F.S, S)
        rubrics = [{"id": "i0", "must": [pt("m1", [["a", "b"]], weight=2)]}]
        packs = {"i0": {"frozen": ["z"], F.SETTING: ["z", "a"], F.INFO: ["z", "a", "b"]}}
        res = F.three_way(rubrics, packs)
        self.assertEqual(res[F.SETTING]["strict"]["all"]["r4_served"], 0)      # a set of two with one chunk: loose only
        self.assertEqual(res[F.SETTING]["loose"]["all"]["r4_served"], 1)
        self.assertEqual(res[F.INFO]["strict"]["all"]["r4_served"], 1)
        self.assertEqual(res[F.INFO]["strict"]["all"]["only_through_a_larger_set_r4"], 1)
        self.assertEqual(res[F.INFO]["strict"]["weight2"]["r4_served"], 1)

    def test_per_kind_split_and_rows(self):
        rubrics, packs = table(6, 1, 4)
        kinds = {f"i{i}": ("coding" if i % 2 else "debugging") for i in range(6)}
        k = F.by_kind(rubrics, packs, kinds)
        self.assertEqual(sorted(k), ["coding", "debugging"])
        self.assertEqual(sum(k[x][F.SETTING]["strict"]["all"]["indexed_points"] for x in k), 6)
        res = F.three_way(rubrics, packs)
        rows = F.item_rows(res, {f"i{i}": {"kind": kinds[f"i{i}"]} for i in range(6)})
        self.assertEqual(len(rows), 6)
        self.assertEqual(set(rows[0]["points"][0]["strict"]), {"frozen", F.SETTING, F.INFO})

    def test_the_text_starts_with_the_verdict_and_labels_r5a(self):
        rubrics, packs = table(20, 2, 6)
        res = F.three_way(rubrics, packs)
        v = F.rule_verdict(res[F.SETTING]["strict"]["all"], FAST)
        text = F.make_text(res, F.by_kind(rubrics, packs, {r["id"]: "coding" for r in rubrics}), {F.SETTING: {**FAST, "n": 20}, F.INFO: {"median": 5.0, "p90": 6.0, "n": 20}}, v)
        self.assertTrue(text.startswith("VERDICT: r4g2 GOES ON TO B4"))
        self.assertIn("for information only: outside the memory limit of 1.5 GB", text)
        self.assertIn("paired over scenarios", text)

    def test_timing_ids_follow_the_hash_rule_and_the_percentile(self):
        ids = [f"d5-{i:02d}" for i in range(1, 51)]
        a = F.timing_ids(ids)
        self.assertEqual(len(a), 20)
        self.assertEqual(a, F.timing_ids(list(reversed(ids))))                 # does not depend on the order of the file
        self.assertEqual(F.timing_ids(ids[:5]), sorted(ids[:5], key=lambda i: __import__("hashlib").sha256(i.encode()).hexdigest()))
        self.assertEqual(F.pctl([1, 2, 3, 4], 50), 2.5)
        self.assertEqual(F.pctl([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 90), 9.1)
        self.assertIsNone(F.pctl([], 50))


class Static(unittest.TestCase):
    def test_syntax_executable_and_no_secrets_or_home_paths(self):
        self.assertEqual(subprocess.run(["sh", "-n", str(SH)]).returncode, 0)
        self.assertEqual(subprocess.run(["bash", "-n", str(SH)]).returncode, 0)
        self.assertTrue(os.access(SH, os.X_OK))
        for p in (SH, PY, Path(__file__)):
            t = p.read_text()
            for bad in ("/Us" + "ers/", "http" + "://", "https" + "://", "192." + "168.", "sk" + "-", "10." + "0."):
                self.assertNotIn(bad, t, f"{bad!r} in {p.name}")

    def test_order_of_the_steps(self):
        t = SH.read_text()
        i = [t.index("gate-files"), t.index("gate-qwen"), t.index("start_check \"$SC_MODEL\""), t.index("header --phase header"),
             t.index("m6_d5_funnel.py expand"), t.index("m6_d5_funnel.py timing"), t.index("m6_d5_funnel.py packs"), t.index("$PYS served"),
             t.index("header --phase final"), t.index("set -C; printf 'used")]
        self.assertEqual(i, sorted(i))

    def test_no_second_counter(self):
        t = PY.read_text()
        self.assertIn("S.compute(", t)
        self.assertNotIn("def point_flags", t)
        self.assertNotIn("def compute", t)


def sh(*args, timeout=900, env=None):
    e = dict(os.environ)
    for k in ("OMLX_URL", "OMLX_API_KEY", "JUDGE_API_KEY"):
        e.pop(k, None)
    e.update(env or {})
    return subprocess.run(["sh", str(SH), *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=e)


@unittest.skipUnless(READY, READY_REASON)
class Chain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.real = [(ROOT / p).exists() for p in ("runs/m6/D5_FUNNEL_USED", "runs/m6/D5_FUNNEL.md", "runs/m6/RETRIEVAL_CHOSEN.json")]

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.w = self.tmp / "w"

    def tearDown(self):   # a dry run never touches the real marker, report or record
        self.assertEqual([(ROOT / p).exists() for p in ("runs/m6/D5_FUNNEL_USED", "runs/m6/D5_FUNNEL.md", "runs/m6/RETRIEVAL_CHOSEN.json")], self.real)

    def dry(self, *args, **kw):
        return sh("--dry-run", "--work", str(self.w), *args, **kw)

    def assertRefused(self, r, text):
        self.assertEqual(r.returncode, 2, r.stdout[-1200:] + r.stderr[-1200:])
        self.assertIn("REFUSED", r.stderr)
        self.assertIn(text, r.stderr)

    def test_a_changed_rubrics_hash_is_refused(self):
        r = self.dry("--synth-flaw", "rubrics-hash")
        self.assertRefused(r, "differ from the hashes in the chunk check")
        self.assertFalse((self.w / "DF_MANIFEST.json").exists())

    def test_a_batch_file_changed_after_the_synthetic_check_is_refused(self):
        self.assertEqual(self.dry("--stop-after", "expand").returncode, 0)
        items = self.w / "in/items.jsonl"
        items.write_text(items.read_text().replace("Synthetic", "Changed", 1))
        self.assertRefused(self.dry(), "differ from the hashes in the chunk check")

    def test_a_dry_run_never_takes_the_real_files(self):
        self.assertRefused(self.dry("--used-marker", "runs/m6/D5_FUNNEL_USED"), "never uses the real marker")
        self.assertRefused(self.dry("--chosen-out", "runs/m6/RETRIEVAL_CHOSEN.json"), "never uses the real marker")
        self.assertRefused(self.dry("--items", "eval/m6/d5/items.jsonl"), "never uses the real D5 files")

    def test_dry_only_options_are_refused_in_a_real_run(self):
        self.assertRefused(sh("--stop-after", "expand"), "dry runs only")

    def test_stage_markers_without_a_manifest_are_a_stale_marker(self):
        self.w.mkdir(parents=True)
        (self.w / "expand.passed").write_text("{}")
        self.assertRefused(self.dry(), "stage markers or outputs without a manifest")

    def test_a_dry_run_completes_a_second_run_is_refused_and_the_record_is_written(self):
        r = self.dry()
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        self.assertTrue(r.stdout.count("VERDICT: ") >= 1)
        for name in ("expansions.jsonl", "timing.jsonl", "packs.jsonl", "served.json", "served.txt", "rows.json", "expand.passed", "timing.passed",
                     "packs.passed", "served.passed", "D5_FUNNEL_USED", "D5_FUNNEL.md", "DF_MANIFEST.json", "RETRIEVAL_CHOSEN.json"):
            self.assertTrue((self.w / name).exists(), name)
        n = len([l for l in (self.w / "in/items.jsonl").read_text().splitlines() if l.strip()])
        self.assertEqual(len((self.w / "packs.jsonl").read_text().splitlines()), n)
        for line in (self.w / "packs.jsonl").read_text().splitlines():
            row = json.loads(line)
            for s in (F.SETTING, F.INFO):
                self.assertEqual(row[s][:len(row["frozen"])], row["frozen"])
        rep = (self.w / "D5_FUNNEL.md").read_text().splitlines()
        self.assertTrue(rep[2].startswith("VERDICT: "), rep[:4])               # the verdict line is first, under the title
        text = "\n".join(rep)
        self.assertIn("The rule (fixed before the run)", text)
        self.assertIn("for information only: outside the memory limit of 1.5 GB", text)
        self.assertIn("DRY RUN", text)
        rec = json.loads((self.w / "RETRIEVAL_CHOSEN.json").read_text())
        self.assertEqual(rec["setting"], "r4g2")
        self.assertIn(rec["verdict"], ("GO", "STOP"))
        self.assertEqual(rec["verdict_line"], json.loads((self.w / "served.json").read_text())["verdict_line"])
        m = json.loads((self.w / "DF_MANIFEST.json").read_text())
        self.assertEqual(m["expansion"]["max_tokens"], 300)
        self.assertEqual(m["expansion"]["prompt_constant"], "prompts.EXPAND_SYSTEM")
        self.assertEqual(m["settings"]["r5a"]["label"], F.MEMORY_NOTE)
        self.assertRefused(self.dry(), "the D5 funnel was run already")

    def test_the_header_is_written_before_the_expansions_and_a_stopped_run_resumes(self):
        r = self.dry("--synth-stop-after-items", "3")
        self.assertEqual(r.returncode, 1, r.stdout[-1500:])
        rep = (self.w / "D5_FUNNEL.md").read_text()
        self.assertIn("The rule (fixed before the run)", rep)
        self.assertNotIn("## Result", rep)
        self.assertNotIn("\nVERDICT: ", rep)
        first = (self.w / "expansions.jsonl").read_text()
        self.assertEqual(len(first.splitlines()), 3)
        r = self.dry()
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        self.assertTrue((self.w / "expansions.jsonl").read_text().startswith(first))
        self.assertIn("3 already done", (self.w / "logs/m6_d5_funnel.expand.log").read_text())

    def test_each_stopped_stage_resumes_and_a_changed_output_is_refused(self):
        for stage in ("expand", "timing", "packs"):
            r = self.dry("--stop-after", stage)
            self.assertEqual(r.returncode, 0, stage + r.stdout[-1500:] + r.stderr[-1500:])
            self.assertTrue((self.w / f"{stage}.passed").exists())
            self.assertFalse((self.w / "D5_FUNNEL_USED").exists())
        exp = self.w / "expansions.jsonl"
        keep = exp.read_text()
        exp.write_text(keep + "\n")
        self.assertRefused(self.dry(), "changed since the stage passed")
        exp.write_text(keep)
        r = self.dry()
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        for s in ("skip expansions", "skip timing", "skip packs"):
            self.assertIn(s, r.stdout)
        self.assertTrue((self.w / "D5_FUNNEL_USED").exists())

    def test_a_model_of_another_client_refuses_and_our_own_build_is_unloaded(self):
        r = self.dry("--fake-loaded", '["some-other-client-model"]')
        self.assertRefused(r, "not free")
        w2 = self.tmp / "w2"
        r = sh("--dry-run", "--work", str(w2), "--fake-loaded", '["DeepSeek-V4.1-Flash-1017-oQ4e-mtp"]')
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])
        self.assertIn("unloaded DeepSeek-V4.1-Flash-1017-oQ4e-mtp", (w2 / "HEARTBEAT.md").read_text())


if __name__ == "__main__":
    unittest.main()
