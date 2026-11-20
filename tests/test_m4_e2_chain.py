"""Offline tests of scripts/m4_e2_starting_point.sh and scripts/m4_e2_manifest.py (E2: the starting point on the hidden half).

Stub generator, stub judge, a fake server status, a temporary key and SYNTHETIC hidden files in temporary folders; the model server,
the real hidden files and the real key are never used. The dry runs plant a marker string in every synthetic hidden row and the tests
look for it in every file the run leaves behind."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SH = ROOT / "scripts/m4_e2_starting_point.sh"
HELP = ROOT / "scripts/m4_e2_manifest.py"
PY = ROOT / ".venv/bin/python"
NEED = [ROOT / "eval/m4/open/tier_a.jsonl", ROOT / "eval/m4/open/tier_a3.jsonl", ROOT / "eval/m4/b1/items.jsonl", ROOT / "eval/m4/b2/items.jsonl",
        ROOT / "eval/m4/b1/rubrics.jsonl", ROOT / "eval/m4/b2/rubrics.jsonl", ROOT / "eval/m4/pilot2/items.jsonl",
        ROOT / "eval/m4/pilot2/rubrics.jsonl", ROOT / "work/v3/chunks_s1.jsonl", ROOT / "work/m4/catalogues/CATALOGUES.json",
        ROOT / "runs/v3/INDEX_FROZEN.json", ROOT / "eval/m4/SPLIT.json", ROOT / "eval/m4/SPLIT_A3.json", ROOT / "eval/m4/smoke_ids.json", PY]
READY = all(p.exists() for p in NEED)
MARK = "ZQXMARKER"
sys.path.insert(0, str(ROOT / "scripts"))


def sh(*args, env=None, timeout=1500):
    e = dict(os.environ)
    e.update(env or {})
    return subprocess.run(["sh", str(SH), *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=e)


def helper(*args, env=None, timeout=300):
    e = dict(os.environ)
    e.update(env or {})
    return subprocess.run([str(PY), str(HELP), *args], cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=e)


class Static(unittest.TestCase):
    def test_syntax_and_executable(self):
        self.assertTrue(os.access(SH, os.X_OK))
        self.assertTrue(os.access(HELP, os.X_OK))
        self.assertEqual(subprocess.run(["sh", "-n", str(SH)]).returncode, 0)
        self.assertEqual(subprocess.run(["bash", "-n", str(SH)]).returncode, 0)

    def test_stage_order_and_guarded_jobs(self):
        t = SH.read_text()
        i = [t.index("# ---------------------------------------------------------------- PREPARE"), t.index("# ---------------------------------------------------------------- G: byte"),
             t.index("# ---------------------------------------------------------------- O: open rehearsal"), t.index("# ---------------------------------------------------------------- the hidden gate"),
             t.index("# ---------------------------------------------------------------- H1:"), t.index("# ---------------------------------------------------------------- H2:"),
             t.index("# ---------------------------------------------------------------- H3:"), t.index("# ---------------------------------------------------------------- H4"),
             t.index("if want H5"), t.index("# ---------------------------------------------------------------- E4:"), t.index("# ---------------------------------------------------------------- SCORE:")]
        self.assertEqual(i, sorted(i))
        # every job that loads a model or the reranker runs under the guard
        for need in ("$G --stall-min 40 --log $L/$N.$name.log -- .venv/bin/python scripts/eval2.py answer", "$G --stall-min 60 --log $L/$N.judge_$tag.log",
                     "$G --stall-min 40 --log $L/$N.revise.log", "$G --stall-min 40 --log $L/$N.e4.log"):
            self.assertIn(need, t)

    def test_defaults(self):
        t = SH.read_text()
        for need in ('R2=no', 'FJ=no', 'CROSS=yes', 'MARGIN=""', 'READING=both', 'STAGES="G,O,H1,H2,H3,H4,H5,SCORE"'):
            self.assertIn(need, t)

    def test_the_chain_never_writes_the_ledger(self):
        t = SH.read_text()
        lo, hi = t.index("# the temporary key, the synthetic hidden files"), t.index('OURS="$DS $GLM8 $GLM4"\n')
        pos = 0
        for line in t.splitlines(keepends=True):
            if "$LEDGER" in line and (">" in line.replace("->", "").replace("2>", "")) and "$PYH" not in line and "--ledger" not in line:
                # the only writes are the synthetic ledger of a dry run, inside the dry-run branch
                self.assertTrue(lo < pos < hi and "printf" in line, line)
            pos += len(line)
        self.assertNotIn("HIDDEN_LEDGER.md >", t)
        self.assertIn("the chain reads the ledger, never writes it", t)

    def test_no_address_key_host_or_home_path(self):
        for f in (SH, HELP):
            t = f.read_text()
            for bad in ("/Users/", "192.168.", "10.0.", "sk-", "Bearer sk", "omlx_url_value"):
                self.assertNotIn(bad, t, f"{f.name}: {bad}")
        self.assertIn("OMLX_URL", SH.read_text())

    def test_no_default_for_the_margin_reading_and_the_verdict_is_withheld(self):
        r = sh("--dry-run", "--margin-reading", "sideways")
        self.assertEqual(r.returncode, 2)
        r = sh("--reading", "default", "--dry-run")
        self.assertEqual(r.returncode, 2)
        self.assertIn("two answer sets", r.stderr)


class HelperUnits(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="e2_helper_"))
        cls.key = cls.tmp / "k.key"
        cls._old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = str(cls.key)
        import eval2
        cls.eval2 = eval2
        eval2.keygen(cls.key)
        import m4_e2_manifest as mm
        cls.mm = mm

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)
        if cls._old is None:
            os.environ.pop("EVAL_HIDDEN_KEY", None)
        else:
            os.environ["EVAL_HIDDEN_KEY"] = cls._old

    def rows(self, name, n, prefix):
        p = self.tmp / name
        self.eval2.write_jsonl(p, [{"id": f"{prefix}-{i}", "question": f"{MARK} q {i}"} for i in range(n)])
        return str(p)

    def test_ledger_rows(self):
        f = self.tmp / "l.md"
        f.write_text("| # | Date |\n|---|---|\n")
        self.assertEqual(len(self.mm.ledger_problems(f)), 2)
        rule = "(a) PASS if the lower end of the 90 % interval is above -5 points on the 305 items"
        f.write_text(f"| 1 | 2026-11-21 | E2 | cfg | {rule} | | conductor |\n")
        self.assertEqual(self.mm.ledger_problems(f), ["ledger row 2 is missing"])
        f.write_text(f"| 1 | 2026-11-21 | E2 | cfg | {rule} | | conductor |\n| 2 | <date> | E3 | cfg | short | | conductor |\n")
        self.assertEqual(self.mm.ledger_problems(f), ["ledger row 2 has no date", "ledger row 2 has no decision rule"])
        f.write_text(f"| 1 | 2026-11-21 | E2 | cfg | {rule} | | conductor |\n| 2 | 2026-11-21 | E3 | cfg | {rule} | | conductor |\n")
        self.assertEqual(self.mm.ledger_problems(f), [])
        self.assertEqual(self.mm.ledger_problems(self.tmp / "absent.md"), ["the ledger file is missing"])

    def test_counts(self):
        h = {"tier_a": self.rows("a.enc", 245, "a"), "tier_a3": self.rows("a3.enc", 60, "c"), "judged_1": self.rows("j1.enc", 24, "j"),
             "judged_2": self.rows("j2.enc", 36, "k"), "rubrics_1": self.rows("r1.enc", 24, "j"), "rubrics_2": self.rows("r2.enc", 36, "k")}
        self.assertEqual(self.mm.count_problems(h), [])
        bad = dict(h, tier_a=self.rows("a_short.enc", 244, "a"))
        self.assertEqual(self.mm.count_problems(bad), ["tier_a: 244 rows, expected 245"])
        bad = dict(h, rubrics_2=self.rows("r2_other.enc", 36, "zz"))
        self.assertEqual(self.mm.count_problems(bad), ["rubrics_2: ids differ from judged_2 (or other order)"])
        bad = dict(h, judged_2=self.rows("j2_dup.enc", 36, "j"))   # the ids of judged_2 repeat those of judged_1
        self.assertTrue(any("repeat" in p for p in self.mm.count_problems(bad)))
        bad = dict(h)
        del bad["judged_1"]
        self.assertIn("judged_1: no file given", self.mm.count_problems(bad))

    def test_scan_finds_a_plain_file_with_an_id_and_skips_enc_and_excluded_folders(self):
        d = self.tmp / "scan"
        (d / "logs").mkdir(parents=True)
        (d / "ok.txt").write_text("hj-0010 is another id; tier_a-1 nothing\n")
        (d / "logs/run.log").write_text("id=hj-001\n")
        (d / "x.enc").write_bytes(b"hj-001")
        self.assertEqual(self.mm.scan_plain(d, {"hj-001"}, exclude=[d / "logs"]), [])
        (d / "leak.json").write_text('{"id": "hj-001"}')
        self.assertEqual(self.mm.scan_plain(d, {"hj-001"}, exclude=[d / "logs"]), ["leak.json"])

    def test_tree_dirty_ignores_untracked_hidden_files_only(self):
        r = self.tmp / "repo"
        (r / "eval/hidden").mkdir(parents=True)
        for c in (["init", "-q"], ["config", "user.email", "t@t"], ["config", "user.name", "t"]):
            subprocess.run(["git", *c], cwd=r, check=True)
        (r / "a.txt").write_text("a")
        subprocess.run(["git", "add", "a.txt"], cwd=r, check=True)
        subprocess.run(["git", "commit", "-qm", "x"], cwd=r, check=True)
        self.assertEqual(self.mm.tree_dirty(r), [])
        (r / "eval/hidden/x.enc").write_bytes(b"x")
        self.assertEqual(self.mm.tree_dirty(r), [])
        (r / "new.sh").write_text("x")
        self.assertEqual(len(self.mm.tree_dirty(r)), 1)
        (r / "new.sh").unlink()
        (r / "a.txt").write_text("changed")
        self.assertEqual(len(self.mm.tree_dirty(r)), 1)

    def test_gate_g_needs_identical_texts_and_packs(self):
        st, got = self.tmp / "st.jsonl", self.tmp / "got.jsonl"
        rows = [{"id": f"i{n}", "text": f"t{n}", "pack": [f"c{n}"]} for n in range(3)]
        st.write_text("".join(json.dumps(r) + "\n" for r in rows))
        got.write_text("".join(json.dumps(r) + "\n" for r in rows))
        self.assertEqual(helper("gate-g", "--answers", str(got), "--stored", str(st), "--expect", "3").returncode, 0)
        rows[1]["text"] += " "
        got.write_text("".join(json.dumps(r) + "\n" for r in rows))
        r = helper("gate-g", "--answers", str(got), "--stored", str(st), "--expect", "3")
        self.assertEqual(r.returncode, 1)
        self.assertIn("2 of 3 texts byte-identical", r.stdout)
        self.assertEqual(helper("gate-g", "--answers", str(got), "--stored", str(st), "--expect", "4").returncode, 1)

    def test_subset_is_seeded_and_merge_normalises_items(self):
        src = self.tmp / "ans.jsonl.enc"
        self.eval2.write_jsonl(src, [{"id": f"x{i}", "set": "Q4D"} for i in range(40)])
        a, b = self.tmp / "s1.enc", self.tmp / "s2.enc"
        for out in (a, b):
            self.assertEqual(helper("subset", "--in", str(src), "--out", str(out), "--n", "24", "--seed", "1").returncode, 0)
        self.assertEqual(self.eval2.read_jsonl(a), self.eval2.read_jsonl(b))
        self.assertEqual(len(self.eval2.read_jsonl(a)), 24)
        i1 = self.tmp / "i1.enc"
        self.eval2.write_jsonl(i1, [{"id": "p1", "question": "q", "domain": ["kafka", "k8s"]}])
        out = self.tmp / "m.enc"
        self.assertEqual(helper("merge", "--out", str(out), "--in", str(i1), "--normalize-items", "e2").returncode, 0)
        row = self.eval2.read_jsonl(out)[0]
        self.assertEqual((row["tier"], row["kind"], row["family"], row["domain"], row["domains"]), ("B", "open", "p1", "kafka,k8s", ["kafka", "k8s"]))
        r = helper("merge", "--out", str(self.tmp / "plain.jsonl"), "--in", str(i1))
        self.assertEqual(r.returncode, 1)   # a plain output for an encrypted input is refused

    def test_verdict_is_withheld_without_the_chosen_reading(self):
        f = self.tmp / "cmp.txt"
        f.write_text("(a) x\n  one-sided reading (PASS if the lower end is above -5): PASS  (lower end -2.82)\n"
                     "  two-sided reading (PASS if the whole interval is inside +-5): FAIL  (interval [-7.0, +4.5])\n")
        r = helper("verdict", "--compare-out", str(f))
        self.assertIn("NOT CHOSEN", r.stdout)
        self.assertNotIn("VERDICT", r.stdout)
        self.assertIn("[not a verdict]", r.stdout)
        self.assertIn("VERDICT of rule 1(a), one-sided reading: PASS", helper("verdict", "--compare-out", str(f), "--reading", "one-sided").stdout)
        self.assertIn("two-sided reading: FAIL", helper("verdict", "--compare-out", str(f), "--reading", "two-sided").stdout)


@unittest.skipUnless(READY, "needs the open files, the index, the catalogues and the venv")
class HiddenGate(unittest.TestCase):
    """The helper's gate on synthetic hidden files, a temporary key and a manifest written by `prepare`."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="e2_gate_"))
        cls.env = {"EVAL_HIDDEN_KEY": str(cls.tmp / "k.key")}
        subprocess.run([str(PY), str(ROOT / "scripts/eval2.py"), "keygen", "--path", cls.env["EVAL_HIDDEN_KEY"]], cwd=ROOT, check=True, capture_output=True)
        cls.sd = cls.tmp / "syn"
        r = helper("synth", "--dir", str(cls.sd), "--marker", MARK, env=cls.env)
        assert r.returncode == 0, r.stderr
        (cls.tmp / "models.json").write_text(json.dumps({"data": [{"id": "Qwen3.8-27B-4bit"}, {"id": "mlx-community--Qwen3.8-27B-bf16"},
                                                                    {"id": "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"}, {"id": "GLM-5.3-Flash-oQ8e-mtp"}]}))
        (cls.tmp / "server.json").write_text(json.dumps({"loaded_models": [], "version": "0.0-test"}))
        rule = "(a) PASS if the lower end of the 90 % interval is above -5 points on the 305 items, paired bootstrap by family"
        cls.ledger = cls.tmp / "ledger.md"
        cls.ledger.write_text(f"| 1 | 2026-11-21 | E2 | cfg | {rule} | | conductor |\n| 2 | 2026-11-21 | E3 | cfg | {rule} | | conductor |\n")
        cls.hidden = cls.hargs(cls.sd)
        cls.manifest = cls.tmp / "m.json"
        r = helper("prepare", "--out", str(cls.manifest), "--server-json", str(cls.tmp / "server.json"), "--models-json", str(cls.tmp / "models.json"),
                   "--hidden", *cls.hidden, env=cls.env)
        assert r.returncode == 0, r.stdout + r.stderr
        cls.work = cls.tmp / "work"
        cls.work.mkdir()
        cls.commit = json.loads(cls.manifest.read_text())["commit"]
        for s in ("G", "O"):
            (cls.work / f"{s}.passed").write_text(json.dumps({"stage": s, "passed": True, "commit": cls.commit}))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    @staticmethod
    def hargs(sd):
        return [f"tier_a={sd}/tier_a.jsonl.enc", f"tier_a3={sd}/tier_a3.jsonl.enc", f"judged_1={sd}/judged_1_items.jsonl.enc",
                f"judged_2={sd}/judged_2_items.jsonl.enc", f"rubrics_1={sd}/judged_1_rubrics.jsonl.enc", f"rubrics_2={sd}/judged_2_rubrics.jsonl.enc"]

    def gate(self, manifest=None, ledger=None, hidden=None, markers=None, extra=(), work=None):
        r = helper("gate", "--manifest", str(manifest or self.manifest), "--ledger", str(ledger or self.ledger), "--work-dir", str(work or self.work),
                   "--markers", str(markers or self.work), "--server-json", str(self.tmp / "server.json"), "--models-json", str(self.tmp / "models.json"),
                   "--tests", "skip", "--allow-dirty", "--hidden", *(hidden or self.hidden), *extra, env=self.env, timeout=600)
        return r.returncode, r.stdout + r.stderr

    def failed(self, out):
        return [l for l in out.splitlines() if l.startswith("gate FAIL")]

    def test_all_conditions_hold(self):
        rc, out = self.gate()
        self.assertEqual(rc, 0, out)
        self.assertIn("hidden gate: OPEN", out)
        self.assertNotIn(MARK, out)

    def test_refuses_without_ledger_rows(self):
        led = self.tmp / "l_none.md"
        led.write_text("| # | Date |\n|---|---|\n")
        rc, out = self.gate(ledger=led)
        self.assertEqual(rc, 1)
        self.assertEqual(len(self.failed(out)), 1, out)
        self.assertIn("ledger rows 1 and 2", out)
        led.write_text(self.ledger.read_text().split("\n")[0] + "\n")   # row 2 only missing
        rc, out = self.gate(ledger=led)
        self.assertEqual(rc, 1)
        self.assertIn("row 2 is missing", out)

    def test_refuses_a_changed_hash(self):
        m = json.loads(self.manifest.read_text())
        m["index"]["sha256"] = "0" * 64
        bad = self.tmp / "m_bad_index.json"
        bad.write_text(json.dumps(m))
        rc, out = self.gate(manifest=bad)
        self.assertEqual(rc, 1)
        self.assertIn("index.sha256", out)
        m = json.loads(self.manifest.read_text())
        first = next(iter(m["hidden_files"]))
        m["hidden_files"][first]["sha256"] = "f" * 64
        bad2 = self.tmp / "m_bad_hidden.json"
        bad2.write_text(json.dumps(m))
        rc, out = self.gate(manifest=bad2)
        self.assertEqual(rc, 1)
        self.assertIn("hidden_files." + first, out)
        m = json.loads(self.manifest.read_text())
        m["revise"]["prompt_sha256"] = "1" * 64
        bad3 = self.tmp / "m_bad_revise.json"
        bad3.write_text(json.dumps(m))
        self.assertIn("revise.prompt_sha256", self.gate(manifest=bad3)[1])

    def test_refuses_a_wrong_row_count(self):
        sd2 = self.tmp / "syn_short"
        self.assertEqual(helper("synth", "--dir", str(sd2), "--marker", MARK, "--drop-a", "1", env=self.env).returncode, 0)
        h2 = self.hargs(sd2)
        man = self.tmp / "m_short.json"
        r = helper("prepare", "--out", str(man), "--server-json", str(self.tmp / "server.json"), "--models-json", str(self.tmp / "models.json"),
                   "--hidden", *h2, env=self.env)
        self.assertEqual(r.returncode, 0, r.stderr)
        rc, out = self.gate(manifest=man, hidden=h2)
        self.assertEqual(rc, 1)
        self.assertEqual(len(self.failed(out)), 1, out)   # only the count: the manifest was made from these files
        self.assertIn("tier_a: 244 rows, expected 245", out)
        self.assertNotIn(MARK, out)

    def test_refuses_when_g_or_o_did_not_pass(self):
        mk = self.tmp / "marks"
        mk.mkdir()
        (mk / "O.passed").write_text(json.dumps({"stage": "O", "passed": True, "commit": self.commit}))
        rc, out = self.gate(markers=mk)
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: stage G passed", out)
        self.assertNotIn("gate FAIL: stage O passed", out)
        (mk / "G.passed").write_text(json.dumps({"stage": "G", "passed": False, "commit": self.commit}))
        self.assertIn("gate FAIL: stage G passed", self.gate(markers=mk)[1])
        (mk / "G.passed").write_text(json.dumps({"stage": "G", "passed": True, "commit": "deadbeef"}))   # a marker of another commit
        self.assertIn("gate FAIL: stage G passed", self.gate(markers=mk)[1])

    def test_refuses_a_plain_text_file_with_a_hidden_id(self):
        w = self.tmp / "work_leak"
        shutil.copytree(self.work, w)
        (w / "oops.json").write_text('{"id": "hj-001"}')
        rc, out = self.gate(work=w, markers=self.work)
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: no plain-text file with a hidden id", out)
        self.assertNotIn("hj-001", out)   # the file is named, the id is not

    def test_refuses_failing_tests_and_a_dirty_tree(self):
        r = helper("gate", "--manifest", str(self.manifest), "--ledger", str(self.ledger), "--work-dir", str(self.work), "--markers", str(self.work),
                   "--server-json", str(self.tmp / "server.json"), "--models-json", str(self.tmp / "models.json"), "--tests", "run", "--test-cmd", "false",
                   "--allow-dirty", "--hidden", *self.hidden, env=self.env, timeout=600)
        self.assertEqual(r.returncode, 1)
        self.assertIn("gate FAIL: tests", r.stdout)
        # without --allow-dirty the tree of this checkout decides: when it is dirty the gate says so, when clean it does not
        r = helper("gate", "--manifest", str(self.manifest), "--ledger", str(self.ledger), "--work-dir", str(self.work), "--markers", str(self.work),
                   "--server-json", str(self.tmp / "server.json"), "--models-json", str(self.tmp / "models.json"), "--tests", "skip",
                   "--hidden", *self.hidden, env=self.env, timeout=600)
        dirty = self.mm_dirty()
        self.assertEqual("gate FAIL: clean tree" in r.stdout, bool(dirty))

    def mm_dirty(self):
        import m4_e2_manifest as mm
        return mm.tree_dirty(ROOT)

    def test_manifest_is_not_overwritten(self):
        before = self.manifest.read_bytes()
        r = helper("prepare", "--out", str(self.manifest), "--server-json", str(self.tmp / "server.json"), "--models-json", str(self.tmp / "models.json"),
                   "--hidden", *self.hidden, env=self.env)
        self.assertEqual(r.returncode, 0)
        self.assertIn("not overwritten", r.stdout)
        self.assertEqual(before, self.manifest.read_bytes())

    def test_manifest_fields(self):
        m = json.loads(self.manifest.read_text())
        for k in ("commit", "index", "catalogues", "golden_prompts", "scorer", "idcheck", "judge", "revise", "checker", "hidden_files", "split", "server"):
            self.assertIn(k, m)
        self.assertEqual(m["scorer"]["version"], "eval2_score/3")
        self.assertEqual(m["idcheck"]["version"], "eval2_idcheck/1")
        self.assertEqual(sorted(m["hidden_files"]), ["judged_1", "judged_2", "rubrics_1", "rubrics_2", "tier_a", "tier_a3"])
        self.assertEqual(sorted(m["split"]), ["SPLIT.json", "SPLIT_A3.json"])
        self.assertEqual(m["server"]["q4"], "Qwen3.8-27B-4bit")
        self.assertEqual(m["server"]["bf16"], "mlx-community--Qwen3.8-27B-bf16")
        self.assertEqual(m["judge"]["settings_name"], "deepseek-nothink-12k")
        self.assertEqual(m["revise"]["prompt_sha256"], __import__("kafka_sre_rag.revise", fromlist=["x"]).prompt_sha256())


@unittest.skipUnless(READY, "needs the open files, the index, the catalogues and the venv")
class DryRun(unittest.TestCase):
    """The whole chain with the stub backend: open stages on open files, hidden stages on synthetic hidden files (temporary key)."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="e2_dry_"))
        cls.w = cls.tmp / "run"
        cls.r1 = sh("--dry-run", "--work", str(cls.w), "--allow-dirty", "--synth-marker", MARK)
        cls.out1 = cls.r1.stdout + cls.r1.stderr

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_first_run_passes_every_stage(self):
        self.assertEqual(self.r1.returncode, 0, self.out1[-3000:])
        for need in ("gate G: 60 answers", "PASS", "hidden gate: OPEN", "bundle first_1.jsonl.enc", "cross-check agreement",
                     "dry run done: report from stub data", "no plain-text file under"):
            self.assertIn(need, self.out1)
        self.assertTrue((self.w / "report_from_stub_data.md").exists())
        self.assertTrue((self.w / "logs/m4_e2_starting_point.done").exists())
        self.assertTrue((self.w / "logs/m4_e2_starting_point.readings_ready").exists())
        self.assertNotIn("DRY RUN FAILED", self.out1)

    def test_every_hidden_output_is_encrypted(self):
        magic = None
        for p in (self.w / "hidden").rglob("*"):
            if p.is_file():
                data = p.read_bytes()
                magic = magic or data[:4]
                self.assertTrue(p.name.endswith(".enc"), p.name)
                self.assertTrue(data.startswith(magic), p.name)

    def test_no_hidden_text_anywhere(self):
        ids = set()
        import eval2
        os.environ["EVAL_HIDDEN_KEY"] = str(self.w / "dry.key")
        try:
            for f in (self.w / "synthetic_hidden").glob("*_items.jsonl.enc"):
                ids |= {r["id"] for r in eval2.read_jsonl(f)}
            for f in ("tier_a.jsonl.enc", "tier_a3.jsonl.enc"):
                ids |= {r["id"] for r in eval2.read_jsonl(self.w / "synthetic_hidden" / f)}
        finally:
            os.environ.pop("EVAL_HIDDEN_KEY", None)
        self.assertEqual(len(ids), 365)
        n_files = 0
        for p in self.w.rglob("*"):
            if not p.is_file():
                continue
            n_files += 1
            data = p.read_bytes()
            self.assertNotIn(MARK.encode(), data, f"marker text in {p.relative_to(self.w)}")
        self.assertGreater(n_files, 50)
        self.assertNotIn(MARK, self.out1)
        # no hidden id in the heartbeat or in any plain-text file outside the logs (the logs carry ids by design)
        hb = (self.w / "HEARTBEAT.md").read_text()
        for i in list(ids)[:365]:
            self.assertNotIn(i, hb)
        for p in self.w.rglob("*"):
            if p.is_file() and not p.name.endswith(".enc") and "logs" not in p.relative_to(self.w).parts and p.name != "dry.key" and not p.name.endswith(".tmp"):
                t = p.read_text(errors="ignore")
                for i in ("hj-001", "hj-060", "tier_a-001", "tier_a3-060"):
                    self.assertNotIn(i, t, f"{i} in {p.name}")
        # the logs hold ids and timings: progress lines of the hidden stages
        logs = "".join(p.read_text(errors="ignore") for p in (self.w / "logs").glob("*.log"))
        self.assertIn("tier_a-001", logs)

    def test_heartbeat_has_the_swaps_and_the_stage_lines(self):
        hb = (self.w / "HEARTBEAT.md").read_text()
        for need in ("G started", "rehearsal O passed", "hidden gate open", "H1 done", "unloaded DeepSeek-V4.1-Flash-1017-oQ4e-mtp", "run H2_Q4D", "judge H4 finished"):
            self.assertIn(need, hb)

    def test_report_totals_only_and_no_verdict_without_the_margin_reading(self):
        rep = (self.w / "report_from_stub_data.md").read_text()
        self.assertIn("MARGIN READING NOT CHOSEN", rep)
        self.assertNotIn("VERDICT of rule 1(a)", rep)
        self.assertIn("[not a verdict]", rep)
        self.assertIn("items 305", rep)
        self.assertIn("DRY RUN", rep)

    def test_resumed_run_skips_finished_stages_and_the_margin_reading_gives_the_verdict(self):
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty", "--synth-marker", MARK, "--margin-reading", "one-sided")
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out[-3000:])
        self.assertGreaterEqual(out.count("skip "), 12)
        for need in ("skip G: passed earlier", "skip O: passed earlier", "skip bundles: built", "skip judging H4", "skip judging H5", "skip scoring Q4D_mech"):
            self.assertIn(need, out)
        self.assertNotIn("[answer] run", out)
        rep = (self.w / "report_from_stub_data.md").read_text()
        self.assertIn("VERDICT of rule 1(a), one-sided reading", rep)
        self.assertNotIn("MARGIN READING NOT CHOSEN", rep)

    def test_the_hidden_stages_refuse_without_ledger_rows_or_with_a_changed_manifest(self):
        led = self.tmp / "empty_ledger.md"
        led.write_text("| # | Date |\n|---|---|\n")
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty", "--synth-marker", MARK, "--stages", "H1", "--ledger", str(led))
        self.assertEqual(r.returncode, 1)
        self.assertIn("hidden gate REFUSED", r.stdout)
        self.assertIn("gate FAIL: ledger rows 1 and 2", r.stdout)
        m = json.loads((self.w / "E2_MANIFEST.json").read_text())
        m["checker"]["version"] = "checker/0"
        bad = self.tmp / "m_changed.json"
        bad.write_text(json.dumps(m))
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty", "--synth-marker", MARK, "--stages", "H1", "--manifest", str(bad))
        self.assertEqual(r.returncode, 1)
        self.assertIn("gate FAIL: manifest hashes (differs in: checker.version)", r.stdout)


@unittest.skipUnless(READY, "needs the open files, the index, the catalogues and the venv")
class DryRunRefusals(unittest.TestCase):
    """Refusals that show early: a wrong row count, a failed G, the server not free."""

    def run_in(self, *args, env=None):
        d = Path(tempfile.mkdtemp(prefix="e2_dryr_"))
        self.addCleanup(shutil.rmtree, d, True)
        return d, sh("--dry-run", "--work", str(d / "run"), "--allow-dirty", *args, env=env)

    def test_wrong_row_count_refuses_the_hidden_stages(self):
        d, r = self.run_in("--synth-drop-a", "1", "--stages", "H1")
        self.assertEqual(r.returncode, 1)
        self.assertIn("gate FAIL: hidden row counts 245, 60, 24, 36", r.stdout)
        self.assertIn("tier_a: 244 rows, expected 245", r.stdout)
        self.assertFalse((d / "run/hidden/ans_Q4D_judged.jsonl.enc").exists())

    def test_a_failed_g_stops_the_chain_and_leaves_no_passed_marker(self):
        d, r = self.run_in("--stages", "G", env={"M4_E2_DRY_DRIFT": "yes"})
        self.assertEqual(r.returncode, 1)
        self.assertIn("59 of 60 texts byte-identical", r.stdout)
        self.assertIn("gate G failed", r.stdout)
        self.assertFalse((d / "run/G.passed").exists())
        r2 = sh("--dry-run", "--work", str(d / "run"), "--allow-dirty", "--stages", "H1")
        self.assertEqual(r2.returncode, 1)
        self.assertIn("gate FAIL: stage G passed", r2.stdout)
        self.assertFalse((d / "run/hidden/ans_Q4D_judged.jsonl.enc").exists())

    def test_a_model_of_another_client_is_not_unloaded_and_stops_the_chain(self):
        d, r = self.run_in("--stages", "G", "--fake-loaded", '["Some-Other-Client-Model"]')
        self.assertEqual(r.returncode, 1)
        self.assertIn("server not free", r.stdout)
        self.assertFalse((d / "run/ans_G.jsonl").exists())

    def test_our_unneeded_model_is_unloaded_before_the_run_and_the_swap_is_logged(self):
        d, r = self.run_in("--stages", "G", "--fake-loaded", '["DeepSeek-V4.1-Flash-1017-oQ4e-mtp"]')
        self.assertEqual(r.returncode, 0, r.stdout[-1500:])
        hb = (d / "run/HEARTBEAT.md").read_text()
        self.assertIn("unloaded DeepSeek-V4.1-Flash-1017-oQ4e-mtp", hb)
        self.assertTrue((d / "run/G.passed").exists())


if __name__ == "__main__":
    unittest.main()
