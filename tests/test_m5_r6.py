"""M5 R6: the pack writer of scripts/m5_r6_packs.py on a synthetic index with synthetic score caches, the pack-file checks, and the chain
scripts/m5_r6_run.sh (one dry run with the stub model and the stub judge, its resume, and the refusals with exit code 2).
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
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import m5_r6_packs as PK  # noqa: E402
import retrieval_funnel as F  # noqa: E402
from test_profiles_drift import QUESTIONS, StubReranker, make_rows  # noqa: E402

PACKS_B1, PACKS_P2 = ROOT / "work/m5/r6/packs_b1.jsonl", ROOT / "work/m5/r6/packs_p2.jsonl"
HAVE_WORK = all(p.exists() for p in (ROOT / "work/m4/b1/ans_default.jsonl", ROOT / "work/m4/pilot2/ans_default.jsonl", ROOT / "work/v3/chunks_s1.jsonl",
                                     ROOT / "work/m5/b1_needs.jsonl", PACKS_B1, PACKS_P2, Path(str(PACKS_B1) + ".manifest.json"),
                                     Path(str(PACKS_P2) + ".manifest.json"), ROOT / "runs/v3/INDEX_FROZEN.json"))


def run_pk(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = PK.main(argv)
        except SystemExit as e:
            code = e.code
    return code, out.getvalue(), err.getvalue()


class PackWriter(unittest.TestCase):
    """A synthetic index (70 chunks), a stub reranker whose scores are written to the caches the way the funnel does it, then the pack
    writer in its reading mode (no reranker: a score that is not in a cache is an error)."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.idx = self.tmp / "idx.jsonl"
        self.idx.write_text("\n".join(json.dumps(r) for r in make_rows()))
        self.cache = self.tmp / "cache"
        frozen = self.tmp / "frozen.json"
        import hashlib
        frozen.write_text(json.dumps({"sha256": hashlib.sha256(self.idx.read_bytes()).hexdigest()}))
        p = mock.patch.object(F, "FROZEN", str(frozen))
        p.start()
        self.addCleanup(p.stop)
        # the sets: b1 = the first three questions, p2 = the next two; stored default answers = the frozen pack of each
        ix = F.Index(str(self.idx))
        model = StubReranker()
        sc = F.CEScorer(ix.byid, self.cache / f"ce_{ix.sha[:12]}_1500.jsonl", model=model)
        sc.heartbeat = 0
        sc.r4 = F.R4Scorers(ix, self.cache, model_of=lambda L: model, say=lambda *a: None)
        self.sets = {"b1": [], "p2": []}
        for n, (q, exp) in enumerate(QUESTIONS[:5]):
            key, i = ("b1", n) if n < 3 else ("p2", n - 3)
            st = F.ItemState(ix, q, exp)
            st.id = f"{key}-{i + 1:02d}"
            self.sets[key].append((st.id, q, exp, list(F.pack_of(st, F.FROZEN_V, sc))))
            st.__dict__.pop("_rk_cache", None)
            F.pack_of(st, F.PRESETS["r4g2"], sc)               # fills the r4 cache
        for key in ("b1", "p2"):
            (self.tmp / f"{key}_items.jsonl").write_text("".join(json.dumps({"id": i, "question": q}) + "\n" for i, q, _, _ in self.sets[key]))
            (self.tmp / f"{key}_stored.jsonl").write_text("".join(json.dumps({"id": i, "search_terms": e, "pack": p}) + "\n" for i, _, e, p in self.sets[key]))
            rows = [{"row_type": "point", "item": i, "point": "m1", "weight": 2, "status": "mapped", "chunks": p[:1] + ["c999"]} for i, _, _, p in self.sets[key]]
            (self.tmp / f"{key}_needs.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))

    def argv(self, *extra, cache=None):
        t = self.tmp
        a = ["packs", "--index", str(self.idx), "--cache-dir", str(cache or self.cache), "--funnel-json", ""]
        for k in ("b1", "p2"):
            a += [f"--{k}-items", str(t / f"{k}_items.jsonl"), f"--{k}-stored", str(t / f"{k}_stored.jsonl"), f"--{k}-needs", str(t / f"{k}_needs.jsonl"),
                  f"--out-{k}", str(t / f"packs_{k}.jsonl")]
        return a + list(extra)

    def test_writes_packs_that_start_with_the_stored_frozen_pack_and_the_runtime_agrees(self):
        code, out, err = run_pk(self.argv())
        self.assertEqual(code, 0, out[-1500:] + err)
        self.assertIn("CHECK every pack starts with the stored frozen pack: 5 of 5", out)
        self.assertIn("reproduces the stored pack (ids and order): 5 of 5", out)
        self.assertIn("same ids in the same order as the simulation: 5 of 5", out)
        self.assertIn("pack size r4g2: chunks median", out)
        added = 0
        for key in ("b1", "p2"):
            rows = {r["id"]: r for r in PK.rd(self.tmp / f"packs_{key}.jsonl")}
            self.assertEqual(list(rows), [i for i, _, _, _ in self.sets[key]])
            for i, _, _, stored in self.sets[key]:
                pack = rows[i]["pack"]
                self.assertEqual(pack[:len(stored)], stored)
                self.assertEqual(len(pack), len(set(pack)))
                self.assertLessEqual(len(pack), 10)
                self.assertIsNone(rows[i]["error"])
                added += len(pack) - len(stored)
            m = json.load(open(str(self.tmp / f"packs_{key}.jsonl") + ".manifest.json"))
            self.assertEqual((m["kind"], m["setting"], m["count"]), ("r6-packs", "r4g2", len(rows)))
            self.assertEqual(m["pack_file_sha256"], PK.sha256_file(self.tmp / f"packs_{key}.jsonl"))
        self.assertGreater(added, 0)                          # the setting adds chunks on this index
        self.assertNotIn(str(Path.home()), out)

    def test_the_packs_equal_the_simulation_computed_again(self):
        run_pk(self.argv())
        ix = F.Index(str(self.idx))
        sc = PK.make_scorer(F, ix, self.cache)
        for key in ("b1", "p2"):
            rows = {r["id"]: r["pack"] for r in PK.rd(self.tmp / f"packs_{key}.jsonl")}
            for i, q, exp, _ in self.sets[key]:
                st = F.ItemState(ix, q, exp)
                st.id = i
                self.assertEqual(rows[i], F.pack_of(st, F.PRESETS["r4g2"], sc))

    def test_a_missing_score_is_an_error_and_nothing_is_written(self):
        empty = self.tmp / "empty"
        empty.mkdir()
        code, out, err = run_pk(self.argv(cache=empty))
        self.assertEqual(code, 2)
        self.assertIn("does not load the reranker", err)
        self.assertFalse((self.tmp / "packs_b1.jsonl").exists())

    def test_an_existing_pack_file_is_not_overwritten(self):
        self.assertEqual(run_pk(self.argv())[0], 0)
        good = (self.tmp / "packs_b1.jsonl").read_text()
        code, out, err = run_pk(self.argv())
        self.assertEqual(code, 2)
        self.assertIn("not overwritten", err)
        self.assertEqual((self.tmp / "packs_b1.jsonl").read_text(), good)
        self.assertEqual(run_pk(self.argv("--force"))[0], 0)

    def test_sha_and_check_commands(self):
        run_pk(self.argv())
        f = self.tmp / "packs_b1.jsonl"
        self.assertEqual(run_pk(["sha", "--packs", str(f)])[0], 0)
        self.assertEqual(run_pk(["check", "--packs", str(f), "--stored", str(self.tmp / "b1_stored.jsonl"), "--ids-from", str(self.tmp / "b1_items.jsonl")])[0], 0)
        rows = PK.rd(f)
        rows[0]["pack"] = rows[0]["pack"][1:]                 # a pack that no longer starts with the stored frozen pack
        f.write_text("".join(json.dumps(r) + "\n" for r in rows))
        code, out, _ = run_pk(["sha", "--packs", str(f)])
        self.assertEqual(code, 1)
        self.assertIn("DIFFERENT", out)
        code, out, _ = run_pk(["check", "--packs", str(f), "--stored", str(self.tmp / "b1_stored.jsonl"), "--ids-from", str(self.tmp / "b1_items.jsonl")])
        self.assertEqual(code, 1)
        self.assertIn("2 of 3 packs start", out)


@unittest.skipUnless(HAVE_WORK, "work/ inputs or the pack files of work/m5/r6 absent")
class ChainDryRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._t = tempfile.TemporaryDirectory()
        cls.t = Path(cls._t.name)
        cls.w = cls.t / "w"
        cls.first = cls.chain("--mode", "grounded-g1", "--work", str(cls.w))

    @classmethod
    def tearDownClass(cls):
        cls._t.cleanup()

    @classmethod
    def chain(cls, *args, dry=True, env_extra=None):
        env = {k: v for k, v in os.environ.items() if k not in ("OMLX_URL", "OMLX_API_KEY", "JUDGE_API_KEY")}
        env.update(env_extra or {})
        return subprocess.run(["sh", str(ROOT / "scripts/m5_r6_run.sh"), *(["--dry-run"] if dry else []), *args], cwd=ROOT, env=env,
                              capture_output=True, text=True, timeout=1800)

    def test_1_dry_run_exits_0_and_the_report_says_it_decides_nothing(self):
        self.assertEqual(self.first.returncode, 0, self.first.stdout[-2000:] + self.first.stderr[-2000:])
        rep = (self.w / "R6_R4_PACKS_from_stub_data.md").read_text()
        self.assertEqual(rep.splitlines()[0].split(" (")[0], "Informative run; decides nothing")
        self.assertIn("r4g2 packs against frozen packs, same mode, same 54 scenarios", rep)
        for s in ("judged coverage P", "supported essential misses per 10", "median completion tokens", "median answer seconds", "answers cut at the limit",
                  "nonexistent identifiers per 10", "gap sentences per answer", "pack size, chunks", "newly served by r4g2"):
            self.assertIn(s, rep)
        man = json.load(open(self.w / "MANIFEST.json"))
        for f in ("scripts/m5_r6_packs.py", "scripts/m5_r6_run.sh", "scripts/m5_g_rule.py", "work/m5/r6/packs_b1.jsonl", "work/m5/r6/packs_p2.jsonl",
                  "work/m4/b1/ans_default.jsonl", "kafka_sre_rag/prompts.py"):
            self.assertIn(f, man["pins"])
        self.assertEqual(man["judge_args"]["mode"], "grounded-g1")
        for f in ("stage_header.done", "stage_answers.done", "stage_judge.done", "stage_checks.done", "MANIFEST.json"):
            self.assertTrue((self.w / f).exists(), f)
        self.assertEqual(sum(1 for _ in open(self.w / "ans_b1_r4g2.jsonl")), 30)
        self.assertEqual(sum(1 for _ in open(self.w / "ans_p2_r4g2.jsonl")), 24)
        # the answers sit on the pack files, and the header was written before any answer
        for key, pf in (("b1", PACKS_B1), ("p2", PACKS_P2)):
            want = {r["id"]: r["pack"] for r in PK.rd(pf)}
            self.assertEqual({r["id"]: r["pack"] for r in PK.rd(self.w / f"ans_{key}_r4g2.jsonl")}, want)
        self.assertLess((self.w / "stage_header.done").stat().st_mtime, (self.w / "ans_b1_r4g2.jsonl").stat().st_mtime)
        self.assertEqual(json.load(open(str(self.w / "ans_b1_r4g2.jsonl") + ".manifest.json"))["mode"], "grounded-g1")
        self.assertEqual(sum(1 for _ in open(self.w / "judge_b1.jsonl")), 30 * 2 * 2)
        self.assertEqual(sum(1 for _ in open(self.w / "judge_p2.jsonl")), 24 * 2 * 2)

    def test_2_resumes(self):
        r = self.chain("--mode", "grounded-g1", "--work", str(self.w))
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertIn("stage(s) done", r.stdout)

    def test_3_a_stale_marker_is_refused_with_exit_code_2(self):
        m = self.w / "stage_answers.done"
        good = m.read_text()
        rec = json.loads(good)
        rec["manifest_sha256"] = "0" * 64
        m.write_text(json.dumps(rec))
        try:
            r = self.chain("--mode", "grounded-g1", "--work", str(self.w))
            self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
            self.assertIn("stale", r.stderr)
        finally:
            m.write_text(good)

    def test_4_a_changed_output_is_refused(self):
        f = self.w / "ans_b1_r4g2.jsonl"
        good = f.read_text()
        f.write_text(good + "\n")
        try:
            r = self.chain("--mode", "grounded-g1", "--work", str(self.w))
            self.assertEqual(r.returncode, 2)
            self.assertIn("changed since the stage finished", r.stderr)
        finally:
            f.write_text(good)

    def test_5_a_missing_mode_is_refused_with_exit_code_2(self):
        r = self.chain(dry=False)
        self.assertEqual(r.returncode, 2, r.stdout[-500:] + r.stderr[-500:])
        self.assertIn("--mode is required", r.stderr)
        r = self.chain("--dry-run", dry=False)
        self.assertEqual(r.returncode, 2)
        r = self.chain("--mode", "grounded-g3", dry=False)
        self.assertEqual(r.returncode, 2)
        self.assertIn("not 'grounded-g3'", r.stderr)

    def test_6_a_missing_development_done_marker_is_refused_with_exit_code_2(self):
        w = self.t / "w_nomarker"
        r = self.chain("--mode", "grounded-g1", "--work", str(w), "--dev-done", str(self.t / "none.done"), "--report", str(self.t / "r.md"), dry=False,
                       env_extra={"OMLX_URL": "http://localhost:9", "OMLX_API_KEY": "x"})
        self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
        self.assertIn("done marker", r.stderr)
        self.assertFalse((self.t / "r.md").exists())
        self.assertFalse(list(w.glob("*")))                    # nothing was written

    def test_7_a_changed_pack_file_is_refused_with_exit_code_2(self):
        d = self.t / "packs"
        d.mkdir(exist_ok=True)
        for p in (PACKS_B1, PACKS_P2):
            shutil.copy(p, d / p.name)
            shutil.copy(str(p) + ".manifest.json", d / (p.name + ".manifest.json"))
        with open(d / PACKS_B1.name, "a") as f:
            f.write("\n")                                      # the same packs, another byte sequence
        r = self.chain("--mode", "grounded-g1", "--work", str(self.t / "w_changed"), "--packs-b1", str(d / PACKS_B1.name), "--packs-p2", str(d / PACKS_P2.name))
        self.assertEqual(r.returncode, 2, r.stdout[-1000:] + r.stderr[-1000:])
        self.assertIn("not the one recorded", r.stderr)

    # ------------------------------------------------------------ --max-unjudged and --accept-code-change (copies of the dry run's work folder)
    def copy_work(self, name):
        d = self.t / name
        shutil.copytree(self.w, d)
        return d

    def measures(self, d, n, rows_off=()):
        """Leave out judge rows of the copy (valid_reply false; (file, set, id, pass) tuples), then run `measures` as the chain does."""
        for f in ("b1", "p2"):
            path = d / f"judge_{f}.jsonl"
            rows = PK.rd(path)
            for r in rows:
                if (f, r["set"], r["id"], r["pass"]) in rows_off:
                    r["valid_reply"] = False
                    r["judge_errors"] = ["invalid_json"]
            path.write_text("".join(json.dumps(r) + "\n" for r in rows))
        a = ["measures", "--work", str(d), "--dev-dir", str(d / "dev_stub"), "--variant", "g1", "--mode", "grounded-g1",
             "--stored-b1", "work/m4/b1/ans_default.jsonl", "--stored-p2", "work/m4/pilot2/ans_default.jsonl",
             "--rubrics-b1", "eval/m4/b1/rubrics.jsonl", "--rubrics-p2", "eval/m4/pilot2/rubrics.jsonl", "--packs-b1", str(PACKS_B1), "--packs-p2", str(PACKS_P2),
             "--needs-b1", "work/m5/b1_needs.jsonl", "--needs-dev", "eval/m4/retrieval_needs_dev.jsonl", "--stub", "--max-unjudged", str(n)]
        os.chdir(ROOT)
        return run_pk(a)

    def first_ids(self):
        rows = PK.rd(self.w / "judge_b1.jsonl")
        ids = []
        for r in rows:
            if r["id"] not in ids:
                ids.append(r["id"])
        return ids

    def test_8_one_unjudged_row_with_n1_goes_on_and_the_report_names_it(self):
        d = self.copy_work("w_u1")
        i = self.first_ids()[0]
        code, out, err = self.measures(d, 1, {("b1", "r4g2", i, 1)})
        self.assertEqual(code, 0, out[-800:] + err[-800:])
        self.assertEqual(json.load(open(d / "measures.json"))["unjudged"], {"max_unjudged": 1, "rows": {"b1": 1, "p2": 0}})
        code, out, err = run_pk(["unjudged-note", "--work", str(d), "--max-unjudged", "1"])
        self.assertEqual(code, 0, err)
        self.assertIn(f"set of answers r4g2, id {i}, pass 1", out)
        self.assertIn("B1: 1 unjudged row(s)", out)
        self.assertIn("paired comparison", out)
        self.assertIn("one pass on that side", out)
        self.assertIn("attempts: 4", out)

    def test_9_the_same_with_n0_stops_as_today(self):
        d = self.copy_work("w_u0")
        i = self.first_ids()[0]
        code, out, err = self.measures(d, 0, {("b1", "r4g2", i, 1)})
        self.assertEqual(code, 2, out[-500:] + err[-500:])
        self.assertIn("valid judge passes", err)
        self.assertEqual(run_pk(["unjudged-note", "--work", str(d), "--max-unjudged", "0"])[1], "")      # nothing in the report for N = 0

    def test_a_two_unjudged_rows_in_a_set_with_n1_stop(self):
        d = self.copy_work("w_u2")
        a, b = self.first_ids()[:2]
        code, out, err = self.measures(d, 1, {("b1", "r4g2", a, 1), ("b1", "frozen", b, 2)})
        self.assertEqual(code, 2, out[-500:] + err[-500:])
        self.assertIn("at most 1 allowed", err)

    def test_b_an_answer_with_no_valid_pass_stops(self):
        d = self.copy_work("w_u3")
        i = self.first_ids()[0]
        code, out, err = self.measures(d, 5, {("b1", "r4g2", i, 1), ("b1", "r4g2", i, 2)})
        self.assertEqual(code, 2, out[-500:] + err[-500:])
        self.assertIn("without any valid judge pass", err)

    def changed_copy(self, name, pin="scripts/m5_r6_run.sh"):
        """A copy of the dry run's folder whose manifest pins another hash of `pin` (markers re-signed for that manifest, as if the chain's code
        had changed after the answers were made)."""
        import m5_g_rule as G
        d = self.copy_work(name)
        m = json.load(open(d / "MANIFEST.json"))
        m["pins"][pin] = "0" * 64
        (d / "MANIFEST.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
        for mk in d.glob("stage_*.done"):
            rec = json.load(open(mk))
            rec["manifest_sha256"] = G.manifest_hash(m)
            mk.write_text(json.dumps(rec))
        return d

    def test_c_a_code_change_is_refused_without_the_option_and_accepted_with_it(self):
        d = self.changed_copy("w_cc")
        r = self.chain("--mode", "grounded-g1", "--work", str(d))
        self.assertEqual(r.returncode, 2, r.stdout[-800:] + r.stderr[-800:])
        self.assertIn("pins.scripts/m5_r6_run.sh", r.stderr)
        self.assertEqual(json.load(open(d / "MANIFEST.json")).get("accepted_code_changes"), None)
        r = self.chain("--mode", "grounded-g1", "--work", str(d), "--accept-code-change", "judge row cannot be judged")
        self.assertEqual(r.returncode, 0, r.stdout[-1500:] + r.stderr[-1500:])
        self.assertIn("ACCEPTED change of the chain's own code: scripts/m5_r6_run.sh", r.stdout)
        ch = json.load(open(d / "MANIFEST.json"))["accepted_code_changes"]
        self.assertEqual(ch[0]["reason"], "judge row cannot be judged")
        self.assertEqual(ch[0]["files"]["scripts/m5_r6_run.sh"]["old"], "0" * 64)
        self.assertEqual(len(ch[0]["files"]["scripts/m5_r6_run.sh"]["new"]), 64)
        self.assertTrue(ch[0]["time"])
        rep = (d / "R6_R4_PACKS_from_stub_data.md").read_text()
        self.assertIn("Accepted change of the chain's own code", rep)
        self.assertIn("judge row cannot be judged", rep)
        self.assertIn("`" + "0" * 64 + "`", rep)

    def test_d_a_code_change_is_refused_when_the_answers_stage_is_not_done(self):
        d = self.changed_copy("w_cc2")
        (d / "stage_answers.done").unlink()
        r = self.chain("--mode", "grounded-g1", "--work", str(d), "--accept-code-change", "x")
        self.assertEqual(r.returncode, 2, r.stdout[-800:] + r.stderr[-800:])
        self.assertIn("not every answering stage is done", r.stderr)
        self.assertEqual(json.load(open(d / "MANIFEST.json")).get("accepted_code_changes"), None)

    def test_e_only_the_chains_own_two_files_can_be_accepted(self):
        d = self.changed_copy("w_cc3", "scripts/m5_g_rule.py")
        r = self.chain("--mode", "grounded-g1", "--work", str(d), "--accept-code-change", "x")
        self.assertEqual(r.returncode, 2, r.stdout[-800:] + r.stderr[-800:])
        self.assertIn("--accept-code-change", r.stderr)
        d = self.changed_copy("w_cc4", "scripts/m5_r6_packs.py")
        r = self.chain("--mode", "grounded-g1", "--work", str(d), "--accept-code-change", "x")
        self.assertEqual(r.returncode, 0, r.stdout[-800:] + r.stderr[-800:])

    def test_f_the_options_in_the_dry_chain(self):
        d = self.copy_work("w_opt")
        r = self.chain("--mode", "grounded-g1", "--work", str(d), "--max-unjudged", "1")
        self.assertEqual(r.returncode, 0, r.stdout[-800:] + r.stderr[-800:])
        self.assertIn("No row is unjudged.", (d / "R6_R4_PACKS_from_stub_data.md").read_text())
        r = self.chain("--mode", "grounded-g1", "--work", str(d), "--max-unjudged", "x", dry=False)
        self.assertEqual(r.returncode, 2)
        self.assertIn("whole number", r.stderr)


if __name__ == "__main__":
    unittest.main()
