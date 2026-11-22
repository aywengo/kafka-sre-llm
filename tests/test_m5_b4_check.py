"""Offline tests of scripts/m5_b4_served.py (the counts of rule X1 on synthetic rubrics and packs) and of scripts/m5_b4_check.sh
(dry runs on a synthetic batch with a stub model, a stand-in reranker and a fake server status; never the model server, never B4)."""
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SH = ROOT / "scripts/m5_b4_check.sh"
PY = ROOT / "scripts/m5_b4_served.py"
sys.path.insert(0, str(ROOT / "scripts"))
import m5_b4_served as S  # noqa: E402

NEED = (ROOT / "work/v3/chunks_s1.jsonl", ROOT / ".venv/bin/python", ROOT / "runs/v3/INDEX_FROZEN.json", ROOT / "scripts/eval2_compare.py")
READY = all(p.exists() for p in NEED)
READY_REASON = "files not in a clone (work/ and runs/ are git-ignored; the chain script starts .venv/bin/python, made by python3 -m venv .venv); missing: " + \
    (", ".join(str(p.relative_to(ROOT)) for p in NEED if not p.exists()) or "none")


def pt(pid, sets, weight=1, chunks=None, **kw):
    ch = chunks if chunks is not None else sorted({c for s in sets for c in s})
    return {"id": pid, "weight": weight, "chunks": ch, "chunk_sets": [list(s) for s in sets], **kw}


def rub(iid, *points):
    return {"id": iid, "must": list(points)}


def counts(rubrics, packs):
    return S.compute(rubrics, packs)


class Flags(unittest.TestCase):
    def test_a_set_of_two_with_one_chunk_served_is_loose_only(self):
        p = pt("m1", [["a", "b"]])
        self.assertEqual(S.point_flags(p, ["a", "x"])[:2], (False, True))
        self.assertEqual(S.point_flags(p, ["a", "b"])[:2], (True, True))

    def test_served_only_through_a_set_of_two(self):
        p = pt("m1", [["a"], ["b", "c"]])
        self.assertEqual(S.point_flags(p, ["b", "c"]), (True, True, True))
        self.assertEqual(S.point_flags(p, ["a", "b", "c"]), (True, True, False))
        self.assertEqual(S.point_flags(p, ["a"]), (True, True, False))

    def test_loose_uses_the_chunks_field_the_union(self):
        p = pt("m1", [["a"], ["b", "c"]], chunks=["a", "b", "c"])
        self.assertTrue(S.point_flags(p, ["c"])[1])
        self.assertFalse(S.point_flags(p, ["z"])[1])


class Counts(unittest.TestCase):
    def test_a_point_without_sets_is_outside_both_counts(self):
        r = [rub("i1", pt("m1", [["a"]]), pt("m2", [], chunks=["a"], no_covering_set=True), pt("m3", [], chunks=[], not_indexed=True))]
        res = counts(r, {"i1": {"frozen": ["a"], "r4": ["a"]}})
        for c in S.COUNTS:
            self.assertEqual(res[c]["all"]["indexed_points"], 1)
        self.assertEqual(res["outside_the_count"], {"not_indexed": 1, "no_covering_set": 1, "other": 0})

    def test_strict_and_loose_differ_on_a_set_of_two(self):
        r = [rub("i1", pt("m1", [["a", "b"]]))]
        res = counts(r, {"i1": {"frozen": ["x"], "r4": ["x", "a"]}})
        self.assertEqual((res["strict"]["all"]["r4_served"], res["loose"]["all"]["r4_served"]), (0, 1))
        self.assertEqual((res["strict"]["all"]["gained"], res["loose"]["all"]["gained"]), (0, 1))

    def test_a_lost_point_is_counted_and_the_pack_prefix_flag_is_kept(self):
        r = [rub("i1", pt("m1", [["a"]]), pt("m2", [["b"]]))]
        res = counts(r, {"i1": {"frozen": ["a", "b"], "r4": ["b", "c"]}})
        self.assertEqual((res["strict"]["all"]["lost"], res["strict"]["all"]["gained"]), (1, 0))
        self.assertEqual(res["r4_starts_with_frozen"], 0)

    def test_weight2_cell_and_only_through_two(self):
        r = [rub("i1", pt("m1", [["a", "b"]], weight=2), pt("m2", [["c"]], weight=1))]
        res = counts(r, {"i1": {"frozen": ["c"], "r4": ["c", "a", "b"]}})
        w2 = res["strict"]["weight2"]
        self.assertEqual((w2["indexed_points"], w2["frozen_served"], w2["r4_served"], w2["gained"]), (1, 0, 1, 1))
        self.assertEqual(res["strict"]["all"]["only_through_a_larger_set_r4"], 1)
        self.assertEqual(res["strict"]["all"]["only_through_a_larger_set_frozen"], 0)
        self.assertIsNone(res["loose"]["all"]["only_through_a_larger_set_r4"])

    def test_ids_must_match(self):
        with self.assertRaises(ValueError):
            counts([rub("i1", pt("m1", [["a"]]))], {"i2": {"frozen": [], "r4": []}})

    def test_item_clustered_standard_error(self):
        self.assertIsNone(S.clustered_se_pp([(5, 1)]))
        self.assertAlmostEqual(S.clustered_se_pp([(5, 1), (5, 1), (5, 1)]), 0.0)   # every item has the same ratio
        self.assertGreater(S.clustered_se_pp([(5, 0), (5, 4), (5, 1)]), 0.0)


def edge_case(n_points, frozen_served, r4_served):
    """One item per point (n_points of them); chunk 'p<i>'. The frozen pack serves the first frozen_served points, the r4 pack (which
    starts with the frozen pack) serves the first r4_served."""
    rubrics = [rub(f"i{i}", pt("m1", [[f"p{i}"]])) for i in range(n_points)]
    packs = {}
    for i in range(n_points):
        fz = [f"p{i}"] if i < frozen_served else ["zz"]
        packs[f"i{i}"] = {"frozen": fz, "r4": fz + ([f"p{i}"] if i < r4_served and i >= frozen_served else [])}
    return counts(rubrics, packs)


class RuleEdges(unittest.TestCase):
    def test_plus_15_point_0_exactly_passes_and_just_below_fails(self):
        v = S.verdict(edge_case(20, 2, 5)["strict"]["all"])      # +3 of 20 = +15.0
        self.assertTrue(v["gain_holds"] and v["pass"])
        v = S.verdict(edge_case(20, 2, 4)["strict"]["all"])      # +2 of 20 = +10.0
        self.assertFalse(v["gain_holds"] or v["pass"])

    def test_five_percent_loss_exactly_passes_and_just_above_fails(self):
        c = S.verdict({"indexed_points": 100, "frozen_served": 20, "r4_served": 40, "lost": 1})
        self.assertTrue(c["loss_holds"])                          # 1 of 20 = 5.0 %
        c = S.verdict({"indexed_points": 100, "frozen_served": 19, "r4_served": 40, "lost": 1})
        self.assertFalse(c["loss_holds"])                         # 1 of 19 = 5.26 %
        c = S.verdict({"indexed_points": 100, "frozen_served": 0, "r4_served": 40, "lost": 0})
        self.assertTrue(c["loss_holds"])                          # nothing frozen serves, nothing lost

    def test_both_conditions_are_needed_and_no_points_is_a_fail(self):
        c = S.verdict({"indexed_points": 100, "frozen_served": 10, "r4_served": 40, "lost": 5})
        self.assertTrue(c["gain_holds"] and not c["loss_holds"] and not c["pass"])
        self.assertFalse(S.verdict({"indexed_points": 0, "frozen_served": 0, "r4_served": 0, "lost": 0})["pass"])

    def test_the_decide_count_decides_and_the_other_is_labelled(self):
        # strict fails (set of two not whole), loose passes
        rubrics = [rub(f"i{i}", pt("m1", [["a%d" % i, "b%d" % i]])) for i in range(4)]
        packs = {f"i{i}": {"frozen": ["z"], "r4": ["z", "a%d" % i]} for i in range(4)}
        res = counts(rubrics, packs)
        t_strict, t_loose = S.format_text(res, "strict"), S.format_text(res, "loose")
        self.assertIn("X1 verdict (--decide strict): FAIL", t_strict)
        self.assertIn("X1 verdict (--decide loose): PASS", t_loose)
        self.assertIn("loose count (reported, decides nothing)", t_strict)
        self.assertIn("strict count (reported, decides nothing)", t_loose)
        self.assertIn("served only through a set of two", t_strict)


class CommandLine(unittest.TestCase):
    def test_served_has_no_default_for_decide(self):
        r = subprocess.run([sys.executable, str(PY), "served", "--rubrics", "x", "--packs", "y"], capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)
        self.assertIn("--decide", r.stderr)

    def test_full_question_equals_the_harness_function(self):
        import eval2
        for it in ({"question": "q"}, {"question": "q", "evidence": "e"}, {"question": "q", "evidence": ""}):
            self.assertEqual(S.full_question(it), eval2.full_question(it))

    def test_served_command_writes_rows_and_exits_nonzero_when_a_prefix_breaks(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "r.jsonl").write_text(json.dumps(rub("i1", pt("m1", [["a"]]))) + "\n")
            (d / "p.jsonl").write_text(json.dumps({"id": "i1", "frozen": ["a"], "r4": ["a", "b"]}) + "\n")
            r = subprocess.run([sys.executable, str(PY), "served", "--rubrics", str(d / "r.jsonl"), "--packs", str(d / "p.jsonl"),
                                "--decide", "loose", "--json-out", str(d / "o.json")], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            out = json.loads((d / "o.json").read_text())
            self.assertEqual(out["rows"][0]["id"], "i1")
            self.assertEqual(out["decide"], "loose")
            (d / "p.jsonl").write_text(json.dumps({"id": "i1", "frozen": ["a"], "r4": ["b", "a"]}) + "\n")
            r = subprocess.run([sys.executable, str(PY), "served", "--rubrics", str(d / "r.jsonl"), "--packs", str(d / "p.jsonl"),
                                "--decide", "loose"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 1)


@unittest.skipUnless(READY, READY_REASON)
class ProfileGate(unittest.TestCase):
    def setUp(self):
        from kafka_sre_rag import profiles
        self.profiles = profiles
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.cand = copy.deepcopy(profiles.load_candidate())
        grid = profiles.grid_settings("r4g2", self.cand)
        self.cand["profiles"]["r4"] = {**grid, "description": "chosen: grid setting G2"}
        self.rec = {"profile": "r4", "status": "chosen", "chosen": "r4g2", "settings_sha256": profiles.settings_sha256("r4", self.cand)}

    def problems(self, rec=None, cand=None):
        p = self.tmp / "rec.json"
        p.write_text(json.dumps(rec or self.rec))
        return S.profile_problems(p, cand or self.cand)

    def test_a_chosen_record_that_equals_the_candidate_file_has_no_problem(self):
        self.assertEqual(self.problems(), [])

    def test_a_placeholder_or_not_chosen_record_is_refused(self):
        self.assertTrue(self.problems({**self.rec, "status": "candidate, not chosen; unmeasured; r4 entry is the G4 placeholder"}))
        c = copy.deepcopy(self.cand)
        c["profiles"]["r4"]["description"] = "candidate, not chosen: placeholder, the same values as grid setting G4"
        self.assertTrue(any("placeholder" in p for p in self.problems(cand=c)))

    def test_missing_chosen_a_wrong_hash_and_a_grid_mismatch_are_refused(self):
        self.assertTrue(any("chosen" in p for p in self.problems({k: v for k, v in self.rec.items() if k != "chosen"})))
        self.assertTrue(any("settings_sha256" in p for p in self.problems({**self.rec, "settings_sha256": "0" * 64})))
        self.assertTrue(any("differs from the chosen grid" in p for p in self.problems({**self.rec, "chosen": "r4g5"})))


class Static(unittest.TestCase):
    def test_syntax_executable_and_no_secrets_or_home_paths(self):
        self.assertEqual(subprocess.run(["sh", "-n", str(SH)]).returncode, 0)
        self.assertEqual(subprocess.run(["bash", "-n", str(SH)]).returncode, 0)
        self.assertTrue(os.access(SH, os.X_OK))
        for p in (SH, PY, Path(__file__)):
            t = p.read_text()
            for bad in ("/Us" + "ers/", "http" + "://", "https" + "://", "192." + "168.", "sk" + "-", "10." + "0."):
                self.assertNotIn(bad, t, f"{bad!r} in {p.name}")

    def test_order_of_the_steps_and_no_default_decide(self):
        t = SH.read_text()
        i = [t.index("gate-files"), t.index("gate-profile"), t.index("start_check \"$SC_MODEL\""), t.index("report --phase header"),
             t.index("m5_b4_served.py expand"), t.index("m5_b4_served.py packs"), t.index("$PYS served"), t.index("report --phase final"),
             t.index("set -C; printf 'used")]
        self.assertEqual(i, sorted(i))
        self.assertIn("is required and has no default", t)
        self.assertNotIn("DECIDE=strict", t)
        self.assertNotIn("DECIDE=loose", t)


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
        cls.real_marker_before = (ROOT / "runs/m5/B4_USED").exists()
        cls.real_report_before = (ROOT / "runs/m5/B4_CHECK.md").exists()

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.w = self.tmp / "w"

    def dry(self, *args, **kw):
        return sh("--dry-run", "--work", str(self.w), "--decide", kw.pop("decide", "strict"), *args, **kw)

    def assertRefused(self, r, text):
        self.assertEqual(r.returncode, 2, r.stdout[-1200:] + r.stderr[-1200:])
        self.assertIn("REFUSED", r.stderr)
        self.assertIn(text, r.stderr)

    def tearDown(self):   # a dry run never touches the real marker or report
        self.assertEqual((ROOT / "runs/m5/B4_USED").exists(), self.real_marker_before)
        self.assertEqual((ROOT / "runs/m5/B4_CHECK.md").exists(), self.real_report_before)

    # --- refusals, exit code 2
    def test_no_decide_is_refused_in_a_real_and_in_a_dry_run(self):
        for extra in ([], ["--dry-run", "--work", str(self.w)]):
            r = sh(*extra)
            self.assertRefused(r, "--decide")
        self.assertFalse(self.w.exists())
        self.assertRefused(sh("--decide", "both"), "strict or loose")

    def test_a_rubrics_file_that_differs_from_the_chunk_check_is_refused(self):
        r = self.dry("--synth-flaw", "rubrics-hash")
        self.assertRefused(r, "differ from the hashes in the chunk check")
        self.assertFalse((self.w / "expansions.jsonl").exists())
        self.assertFalse((self.w / "B4_MANIFEST.json").exists())

    def test_a_placeholder_record_is_refused_in_a_dry_run_and_in_a_real_run_before_any_server_call(self):
        self.assertRefused(self.dry("--synth-flaw", "placeholder"), "not usable")
        # real mode (no --dry-run) on synthetic files that carry a placeholder record: refused at the gate, nothing is written, no server is reached
        syn = self.tmp / "syn"
        subprocess.run([sys.executable, str(PY), "synth", "--dir", str(syn), "--flaw", "placeholder"], check=True, capture_output=True, cwd=ROOT)
        w2 = self.tmp / "realw"
        r = sh("--decide", "loose", "--items", str(syn / "items.jsonl"), "--rubrics", str(syn / "rubrics.jsonl"), "--chunk-check", str(syn / "CHUNK_CHECK.json"),
               "--profile-record", str(syn / "PROFILE_R4.json"), "--work", str(w2), "--report", str(self.tmp / "rep.md"), "--used-marker", str(self.tmp / "used"))
        self.assertRefused(r, "not usable")
        self.assertFalse(w2.exists())
        self.assertFalse((self.tmp / "rep.md").exists() or (self.tmp / "used").exists())

    def test_an_existing_b4_used_marker_is_refused(self):
        self.w.mkdir(parents=True)
        (self.w / "B4_USED").write_text("used earlier\n")
        self.assertRefused(self.dry(), "B4 was used already")

    def test_a_dry_run_never_takes_the_real_marker_or_real_files(self):
        self.assertRefused(self.dry("--used-marker", "runs/m5/B4_USED"), "never uses the real marker")
        self.assertRefused(self.dry("--items", "eval/m4/b4/items.jsonl"), "never uses the real B4 files")

    def test_options_for_dry_runs_only_are_refused_in_a_real_run(self):
        self.assertRefused(sh("--decide", "strict", "--stop-after", "expand"), "dry runs only")

    def test_stage_markers_without_a_manifest_are_a_stale_marker(self):
        self.w.mkdir(parents=True)
        (self.w / "expand.passed").write_text("{}")
        self.assertRefused(self.dry(), "stage markers or outputs without a manifest")

    # --- the dry run end to end, resume, stale marker, changed pinned file
    def test_a_dry_run_completes_writes_the_report_and_the_marker_in_its_own_folder_and_a_second_run_is_refused(self):
        r = self.dry()
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        for name in ("expansions.jsonl", "packs.jsonl", "served.json", "served.txt", "expand.passed", "packs.passed", "served.passed", "B4_USED", "B4_CHECK.md", "B4_MANIFEST.json"):
            self.assertTrue((self.w / name).exists(), name)
        n = len([l for l in (self.w / "in/items.jsonl").read_text().splitlines() if l.strip()])
        self.assertEqual(len((self.w / "expansions.jsonl").read_text().splitlines()), n)
        self.assertEqual(len((self.w / "packs.jsonl").read_text().splitlines()), n)
        rep = (self.w / "B4_CHECK.md").read_text()
        self.assertIn("Decision count (`--decide`): **strict**", rep)
        self.assertIn("Rule X1", rep)
        self.assertIn("DRY RUN", rep)
        self.assertIn("strict count (decides)", rep)
        self.assertIn("loose count (reported, decides nothing)", rep)
        self.assertIn(f"r4 pack starts with the frozen pack in {n} of {n}", rep)
        marker = (self.w / "B4_USED").read_text()
        self.assertIn("decide strict", marker)
        self.assertRegex(marker, r"verdict (PASS|FAIL)")
        hb = (self.w / "HEARTBEAT.md").read_text()
        for s in ("expansions started", "expansions done", "packs started", "packs done", "B4 is used"):
            self.assertIn(s, hb)
        m = json.loads((self.w / "B4_MANIFEST.json").read_text())
        self.assertEqual(m["expansion"]["max_tokens"], 300)
        self.assertEqual(m["expansion"]["temperature"], 0)
        self.assertEqual(m["expansion"]["prompt_constant"], "prompts.EXPAND_SYSTEM")
        self.assertEqual(m["options"]["decide"], "strict")
        for line in (self.w / "packs.jsonl").read_text().splitlines():
            row = json.loads(line)
            self.assertEqual(row["r4"][:len(row["frozen"])], row["frozen"])
        self.assertRefused(self.dry(), "B4 was used already")

    def test_both_decide_values_give_a_verdict_and_label_the_other_count(self):
        r = self.dry(decide="loose")
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])
        rep = (self.w / "B4_CHECK.md").read_text()
        self.assertIn("loose count (decides)", rep)
        self.assertIn("strict count (reported, decides nothing)", rep)

    def test_the_header_is_written_before_the_expansions_and_a_stopped_run_resumes(self):
        r = self.dry("--synth-stop-after-items", "3")
        self.assertEqual(r.returncode, 1, r.stdout[-1500:])
        rep = (self.w / "B4_CHECK.md").read_text()
        self.assertIn("Decision count (`--decide`): **strict**", rep)          # the header exists although the expansions did not finish
        self.assertIn("The rule (fixed before the run)", rep)
        self.assertNotIn("X1 verdict", rep)
        first = (self.w / "expansions.jsonl").read_text()
        self.assertEqual(len(first.splitlines()), 3)
        r = self.dry()
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        self.assertTrue((self.w / "expansions.jsonl").read_text().startswith(first))   # the first rows are kept, not redone
        self.assertIn("3 already done", (self.w / "logs/m5_b4_check.expand.log").read_text())
        self.assertTrue((self.w / "B4_USED").exists())

    def test_a_finished_stage_is_skipped_on_resume_and_a_changed_output_or_pinned_file_is_refused(self):
        r = self.dry("--stop-after", "expand")
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertTrue((self.w / "expand.passed").exists())
        self.assertFalse((self.w / "packs.jsonl").exists())
        self.assertFalse((self.w / "B4_USED").exists())
        exp = self.w / "expansions.jsonl"
        keep = exp.read_text()
        exp.write_text(keep.replace("kafka", "KAFKA", 1) if "kafka" in keep else keep + "\n")
        r = self.dry()
        self.assertRefused(r, "changed since the stage passed")
        exp.write_text(keep)
        rec = self.w / "in/PROFILE_R4.json"
        old = rec.read_text()
        d = json.loads(old)
        d["note"] = "edited after the manifest"
        rec.write_text(json.dumps(d))
        r = self.dry()
        self.assertRefused(r, "pinned file changed: input:profile_record")
        rec.write_text(old)
        r = self.dry()
        self.assertEqual(r.returncode, 0, r.stdout[-2500:] + r.stderr[-2500:])
        self.assertIn("skip expansions: passed earlier", r.stdout)
        self.assertTrue((self.w / "B4_USED").exists())

    def test_a_changed_option_is_refused_on_resume(self):
        self.assertEqual(self.dry("--stop-after", "expand").returncode, 0)
        self.assertRefused(self.dry(decide="loose"), "option differs: decide")

    # --- the server gate
    def test_a_model_of_another_client_refuses_and_our_own_build_is_unloaded(self):
        r = self.dry("--fake-loaded", '["some-other-client-model"]')
        self.assertRefused(r, "not free")
        self.assertFalse((self.w / "B4_MANIFEST.json").exists())
        w2 = self.tmp / "w2"
        r = sh("--dry-run", "--work", str(w2), "--decide", "strict", "--fake-loaded", '["DeepSeek-V4.1-Flash-1017-oQ4e-mtp"]')
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])
        self.assertIn("unloaded DeepSeek-V4.1-Flash-1017-oQ4e-mtp", (w2 / "HEARTBEAT.md").read_text())
        self.assertIn("unloaded Qwen3.8-27B-4bit", (w2 / "HEARTBEAT.md").read_text())   # loaded by this chain, so unloaded after the expansions


if __name__ == "__main__":
    unittest.main()
