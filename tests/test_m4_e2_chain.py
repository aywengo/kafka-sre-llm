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
        ROOT / "eval/m4/pilot2/rubrics.jsonl", ROOT / "work/v3/chunks_s1.jsonl", ROOT / "work/m4/s6/ans_A1.jsonl", ROOT / "work/m4/catalogues/CATALOGUES.json",
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

    def test_unload_checks_the_servers_answer_and_logs_it(self):
        text = SH.read_text()
        fn = text[text.index("unload_now() {"):text.index("unload() {")]
        d = Path(tempfile.mkdtemp(prefix="e2_unload_"))
        self.addCleanup(shutil.rmtree, d, True)
        (d / "bin").mkdir()

        def run(reply):
            c = d / "bin/curl"
            c.write_text("#!/bin/sh\nprintf '%s' '" + reply + "'\n")
            c.chmod(0o755)
            script = ("N=chain; DRY=no; BASE=http://example.invalid; OMLX_API_KEY=k; PYH=true; idle() { :; }; "
                      "hb() { echo \"HB $1\"; }; fail() { echo \"FAIL $1\"; exit 1; }\n" + fn + "\nunload_now Some-Model\necho after\n")
            return subprocess.run(["sh", "-c", script], capture_output=True, text=True, env={"PATH": f"{d / 'bin'}:/usr/bin:/bin"})
        ok = run('{"status":"ok"}\n200')
        self.assertIn("HB chain: unloaded Some-Model (no active or waiting requests; the server answered 200: {\"status\":\"ok\"})", ok.stdout)
        self.assertIn("after", ok.stdout)
        bad = run('{"error":"busy"}\n409')
        self.assertEqual(bad.returncode, 1)
        self.assertIn("HB chain: unload of Some-Model FAILED (the server answered 409", bad.stdout)
        self.assertIn("FAIL the unload of Some-Model was not accepted (the server answered 409)", bad.stdout)
        self.assertNotIn("after", bad.stdout)
        gone = run("")
        self.assertEqual(gone.returncode, 1)
        self.assertIn("answered 000", gone.stdout)

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

    def test_merged_kind_groups_leave_no_group_under_five(self):
        mk = self.mm.merged_kind_groups
        self.assertEqual(mk({"a": 8, "b": 6}), [("a", 8), ("b", 6)])
        self.assertEqual(mk({"a": 8, "b": 2, "c": 1}), [("a + merged 2 kind(s)", 11)])   # 3 left over: they join the smallest shown group
        self.assertEqual(mk({"a": 8, "b": 3, "c": 3}), [("a", 8), ("merged 2 kind(s)", 6)])
        self.assertEqual(mk({"a": 3, "b": 1}), [("all 2 kind(s)", 4)])
        for counter in ({"a": 20, "b": 4, "c": 4, "d": 1}, {"a": 5, "b": 4, "c": 1}, {"x": 7}):
            self.assertTrue(all(c >= 5 for _, c in mk(counter)) or sum(counter.values()) < 5, counter)
            self.assertEqual(sum(c for _, c in mk(counter)), sum(counter.values()))

    def judged_rows(self, n=12):
        return [{"id": f"j-{i}", "tier": "B", "kind": "debugging", "family": f"f{i}", "domain": ["kafka"], "question": f"{MARK} question {i} words words",
                 "evidence": f"{MARK} evidence of {i} some words" if i % 2 else None} for i in range(n)]

    def test_shape_file_names_keys_and_counts_never_values(self):
        rows = self.judged_rows()
        rows[0].pop("question")
        rows[1]["evidence"] = ""
        for r in rows[:4]:
            r["logs"] = f"{MARK} a log line with words"          # a text key that nothing reads
        rows[5]["tier"] = "Z"
        lines, problems = self.mm.shape_file("judged_1", "judged_items", rows)
        out = "\n".join(lines + problems)
        self.assertNotIn(MARK, out)
        self.assertIn("judged_1: required key question: 1 lacking, 0 empty", problems)
        self.assertIn("judged_1: key logs holds text in 4 row(s) and nothing reads it", problems)
        self.assertIn("judged_1: tier is not one of ['A', 'B', 'legacy'] in 1 row(s)", problems)
        self.assertTrue(any("evidence (optional)" in l and "5 filled, 0 lacking, 7 empty" in l for l in lines), out)
        self.assertEqual(self.mm.shape_file("judged_1", "judged_items", self.judged_rows())[1], [])
        # a key that holds no text (a number, a flag, an empty string) is not a problem
        ok = self.judged_rows()
        for r in ok:
            r.update({"weight": 3, "flag": True, "blank": " "})
        self.assertEqual(self.mm.shape_file("judged_1", "judged_items", ok)[1], [])

    def test_shape_rubrics_and_mechanical_rows(self):
        rub = [{"id": f"j-{i}", "must": [{"id": "m1", "text": f"{MARK} text", "weight": 2}], "must_not": [{"id": "n1", "text": "x", "severity": "wrong"}],
                "covered_chunks": ["c1"]} for i in range(6)]
        self.assertEqual(self.mm.shape_file("rubrics_1", "judged_rubrics", rub)[1], [])
        rub[0]["must"][0].pop("weight")
        rub[1]["must"] = []
        rub[2]["context"] = f"{MARK} an unread rubric field"
        _, probs = self.mm.shape_file("rubrics_1", "judged_rubrics", rub)
        self.assertIn("rubrics_1: required key must: 0 lacking, 1 empty", probs)
        self.assertIn("rubrics_1: a must point lacks id, text or weight in 1 row(s)", probs)
        self.assertIn("rubrics_1: key context holds text in 1 row(s) and nothing reads it", probs)
        self.assertNotIn(MARK, "".join(probs))
        mech = [{"id": f"a-{i}", "tier": "A", "kind": "k", "family": "f", "domain": "kafka", "question": "q", "checks": [{"type": "has_command"}]} for i in range(3)]
        self.assertEqual(self.mm.shape_file("tier_a", "mech_items", mech)[1], [])
        mech[0]["checks"] = [{"command": "x"}]
        mech[1]["hint"] = "an unread hint with words"
        _, probs = self.mm.shape_file("tier_a", "mech_items", mech)
        self.assertIn("tier_a: checks are not a non-empty list of typed objects in 1 row(s)", probs)
        self.assertIn("tier_a: key hint holds text in 1 row(s) and nothing reads it", probs)

    def test_row_markers_and_scan_text(self):
        row = {"id": "x-1", "question": f"{MARK}-x-1 which flags does the tool need\nsecond line of the question text here", "kind": "name_setting",
               "must": [{"id": "m1", "text": f"{MARK}-m1 a must point that is distinctive enough"}], "short": "yes it is"}
        marks = self.mm.row_markers(row)
        self.assertTrue(all(len(m) <= 60 and m == " ".join(m.lower().split()) for m in marks))
        self.assertTrue(any("must point" in m for m in marks))
        self.assertFalse(any(m in ("yes it is", "name_setting") for m in marks))
        d = self.tmp / "scan_text"
        (d / "logs").mkdir(parents=True)
        (d / "ok.txt").write_text("nothing about the row here\n")
        (d / "x.enc").write_bytes(f"{MARK}-m1 a must point that is distinctive enough".encode())
        self.assertEqual(self.mm.scan_text(d, marks), [])
        (d / "logs/run.log").write_text("progress\n  " + f"{MARK}-m1  A MUST POINT\nthat is distinctive enough".replace("\n", " ") + "\n")
        self.assertEqual(self.mm.scan_text(d, marks), ["logs/run.log"])      # logs are scanned too; case and spacing do not matter
        hb = self.tmp / "hb.md"
        hb.write_text('{"t": "' + f"{MARK}-x-1 which flags does the tool need" + '"}')
        self.assertEqual(self.mm.scan_text(self.tmp / "nodir", marks, also=[hb]), ["hb.md"])
        self.assertEqual(self.mm.scan_text(d, set()), [])

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

    def test_a_changed_chunks_file_or_s6_reference_fails_the_gate_and_prepare(self):
        ch = self.tmp / "chunks_changed.jsonl"
        ch.write_bytes((ROOT / "work/v3/chunks_s1.jsonl").read_bytes() + b"\n")
        rc, out = self.gate(extra=("--chunks", str(ch)))
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: manifest hashes", out)
        self.assertIn("chunks.sha256", out)
        self.assertIn("hidden gate: REFUSED", out)
        ref = self.tmp / "ans_A1_changed.jsonl"
        ref.write_bytes((ROOT / "work/m4/s6/ans_A1.jsonl").read_bytes() + b"\n")
        rc, out = self.gate(extra=("--s6-ref", str(ref)))
        self.assertEqual(rc, 1)
        self.assertIn("s6_reference.sha256", out)
        # prepare refuses a chunks file that is not the frozen one
        r = helper("prepare", "--out", str(self.tmp / "m_chunks.json"), "--server-json", str(self.tmp / "server.json"), "--models-json", str(self.tmp / "models.json"),
                   "--chunks", str(ch), "--hidden", *self.hidden, env=self.env)
        self.assertEqual(r.returncode, 1)
        self.assertIn("chunks file hash differs", r.stderr)
        self.assertFalse((self.tmp / "m_chunks.json").exists())

    def decrypted(self, name):
        import eval2
        old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = self.env["EVAL_HIDDEN_KEY"]
        try:
            return eval2.read_jsonl(self.sd / name)
        finally:
            if old is None:
                os.environ.pop("EVAL_HIDDEN_KEY", None)
            else:
                os.environ["EVAL_HIDDEN_KEY"] = old

    def test_the_synthetic_rows_carry_a_marker_in_every_text_field(self):
        it = self.decrypted("judged_1_items.jsonl.enc")
        self.assertTrue(all(MARK in r["question"] for r in it))
        self.assertTrue(any(MARK in (r.get("evidence") or "") for r in it))
        rb = self.decrypted("judged_1_rubrics.jsonl.enc")
        for r in rb:
            for k in ("question", "reference", "generic", "correct_alternative", "note"):
                if r.get(k):
                    self.assertIn(MARK, r[k], k)
            self.assertTrue(all(MARK in p["text"] for p in r["must"]))
            self.assertTrue(all(MARK in p["text"] for p in r["must_not"]))
        for name in ("tier_a.jsonl.enc", "tier_a3.jsonl.enc"):
            self.assertTrue(all(MARK in r["question"] for r in self.decrypted(name)))
            self.assertTrue(all(MARK in r["reference"] for r in self.decrypted(name) if r.get("reference")))

    def test_a_leak_of_any_text_field_is_found_by_the_scan_and_refused_by_the_gate(self):
        import m4_e2_manifest as mm
        old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = self.env["EVAL_HIDDEN_KEY"]
        try:
            marks, nrows, _ = mm.text_markers(mm.parse_hidden(self.hidden))
        finally:
            if old is None:
                os.environ.pop("EVAL_HIDDEN_KEY", None)
            else:
                os.environ["EVAL_HIDDEN_KEY"] = old
        self.assertEqual(nrows, 245 + 60 + 24 + 36 + 24 + 36)
        # one leaked line of each kind of field: the question, a must text, a must_not text, the reference, the evidence, a mechanical reference
        rb = self.decrypted("judged_1_rubrics.jsonl.enc")[0]
        it = next(r for r in self.decrypted("judged_1_items.jsonl.enc") if r.get("evidence"))
        mech = next(r for r in self.decrypted("tier_a.jsonl.enc") if r.get("reference"))
        longest = lambda s: max(s.split("\n"), key=len)
        samples = {"question": it["question"], "evidence": longest(it["evidence"]), "must": rb["must"][0]["text"], "must_not": rb["must_not"][0]["text"],
                   "reference": longest(rb["reference"]), "generic": longest(rb["generic"]), "mech reference": longest(mech["reference"])}
        for label, text in samples.items():
            d = self.tmp / ("leak_" + label.replace(" ", "_"))
            d.mkdir(exist_ok=True)
            (d / "plain.txt").write_text("log line: " + text + "\n")
            self.assertEqual(mm.scan_text(d, marks), ["plain.txt"], label)
        # the gate: a leak in a logs folder of the work folder, and a leak in an extra file (the heartbeat), each refuse it, naming the file only
        w = self.tmp / "work_text_leak"
        shutil.copytree(self.work, w)
        (w / "logs").mkdir()
        (w / "logs/run.log").write_text("[answer] " + samples["must"] + "\n")
        rc, out = self.gate(work=w, markers=self.work)
        self.assertEqual(rc, 1)
        self.assertEqual(self.failed(out), ["gate FAIL: no plain file with hidden text (1 file(s): logs/run.log)"], out)
        self.assertNotIn(MARK, out)
        hb = self.tmp / "HB_leak.md"
        hb.write_text("### 2026 - run: " + samples["question"] + "\n")
        rc, out = self.gate(extra=("--also", str(hb)))
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: no plain file with hidden text (1 file(s): HB_leak.md)", out)
        rc, out = self.gate(extra=("--also", str(self.tmp / "no_such_heartbeat.md")))
        self.assertEqual(rc, 0, out)

    def test_shape_ok_on_the_synthetic_files_runs_the_tools_and_removes_its_temporary_folder(self):
        priv = self.tmp / "shape_tmp"      # the tools make their temporary folder here (TMPDIR), so that parallel runs of the suite cannot disturb the check
        priv.mkdir()
        r = helper("shape", "--hidden", *self.hidden, env={**self.env, "TMPDIR": str(priv)}, timeout=900)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out[-2500:])
        for need in ("shape: OK", "stub judge rows: 120 valid of 120 (expected 120)", "stub answers on the judged items: 60 of 60 usable",
                     "scorer: 305 scored, 0 missing", "throwaway reading bundle (two answer sets, 6 first and 2 second sessions): exit 0",
                     "the temporary folder is removed (yes)", "read by answer, judge, bundle: evidence (optional)", "kinds (merged so that no group is under 5)"):
            self.assertIn(need, out)
        self.assertNotIn(MARK, out)
        for rid in ("hj-001", "hj-060", "tier_a-001", "tier_a3-060"):
            self.assertNotIn(rid, out)
        self.assertEqual(list(priv.iterdir()), [])

    def reencrypt(self, name, edit):
        import eval2
        old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = self.env["EVAL_HIDDEN_KEY"]
        try:
            rows = eval2.read_jsonl(self.sd / name)
            edit(rows)
            sd2 = self.tmp / ("syn_" + name.split(".")[0])
            shutil.copytree(self.sd, sd2, dirs_exist_ok=True)
            eval2.write_jsonl(sd2 / name, rows)
            return sd2
        finally:
            if old is None:
                os.environ.pop("EVAL_HIDDEN_KEY", None)
            else:
                os.environ["EVAL_HIDDEN_KEY"] = old

    def test_shape_fails_on_a_missing_key_an_unread_text_key_or_an_empty_field_naming_keys_and_counts_only(self):
        def edit(rows):
            rows[0].pop("question")
            rows[1]["question"] = " "
            for r in rows[:3]:
                r["scenario_notes"] = f"{MARK} notes the answer step never reads"
        sd2 = self.reencrypt("judged_1_items.jsonl.enc", edit)
        r = helper("shape", "--hidden", *self.hargs(sd2), "--no-tools", env=self.env, timeout=300)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 1, out[-2000:])
        self.assertIn("shape PROBLEM: judged_1: required key question: 1 lacking, 1 empty", out)
        self.assertIn("shape PROBLEM: judged_1: key scenario_notes holds text in 3 row(s) and nothing reads it", out)
        self.assertNotIn(MARK, out)
        self.assertNotIn("hj-", out)
        # with the tools: they are not run on files that already have problems
        r = helper("shape", "--hidden", *self.hargs(sd2), env=self.env, timeout=300)
        self.assertEqual(r.returncode, 1)
        self.assertIn("shape tools: not run", r.stdout)
        # a rubric whose point lacks its weight: the stub judge is not needed to see it
        def edit2(rows):
            rows[0]["must"][0].pop("weight")
        sd3 = self.reencrypt("judged_2_rubrics.jsonl.enc", edit2)
        r = helper("shape", "--hidden", *self.hargs(sd3), "--no-tools", env=self.env, timeout=300)
        self.assertEqual(r.returncode, 1)
        self.assertIn("shape PROBLEM: rubrics_2: a must point lacks id, text or weight in 1 row(s)", r.stdout)

    def test_manifest_is_not_overwritten(self):
        before = self.manifest.read_bytes()
        r = helper("prepare", "--out", str(self.manifest), "--server-json", str(self.tmp / "server.json"), "--models-json", str(self.tmp / "models.json"),
                   "--hidden", *self.hidden, env=self.env)
        self.assertEqual(r.returncode, 0)
        self.assertIn("not overwritten", r.stdout)
        self.assertEqual(before, self.manifest.read_bytes())

    def test_manifest_fields(self):
        m = json.loads(self.manifest.read_text())
        for k in ("commit", "index", "chunks", "s6_reference", "catalogues", "golden_prompts", "scorer", "idcheck", "judge", "revise", "checker", "hidden_files", "split", "server"):
            self.assertIn(k, m)
        self.assertEqual(m["chunks"]["sha256"], m["chunks"]["recorded_sha256"])   # equals the hash in runs/v3/INDEX_FROZEN.json
        self.assertEqual(m["chunks"]["recorded_sha256"], json.loads((ROOT / "runs/v3/INDEX_FROZEN.json").read_text())["sha256"])
        self.assertEqual(len(m["s6_reference"]["sha256"]), 64)
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

    def test_the_shape_check_runs_before_the_gate_and_the_text_scan_passes_on_the_run(self):
        shape = (self.w / "shape.out").read_text()
        self.assertIn("shape: OK", shape)
        self.assertIn("stub judge rows: 120 valid of 120", shape)
        self.assertNotIn(MARK, shape)
        self.assertLess(self.out1.index("shape: OK"), self.out1.index("hidden gate: OPEN"))
        hb = (self.w / "HEARTBEAT.md").read_text()
        self.assertLess(hb.index("the shape check of the hidden files passed"), hb.index("hidden gate open"))
        after = (self.w / "scan_after.txt").read_text()
        self.assertIn("holds distinctive text of a hidden row", after)
        self.assertIn("logs and the heartbeat included", after)
        # the same scan by hand, then with a leaked line in a log of the run: refused
        sd = self.w / "synthetic_hidden"
        hargs = ["tier_a=%s/tier_a.jsonl.enc" % sd, "tier_a3=%s/tier_a3.jsonl.enc" % sd, "judged_1=%s/judged_1_items.jsonl.enc" % sd,
                 "judged_2=%s/judged_2_items.jsonl.enc" % sd, "rubrics_1=%s/judged_1_rubrics.jsonl.enc" % sd, "rubrics_2=%s/judged_2_rubrics.jsonl.enc" % sd]
        env = {"EVAL_HIDDEN_KEY": str(self.w / "dry.key")}
        r = helper("scan", "--dir", str(self.w), "--exclude", str(self.w / "logs"), "--text", "--also", str(self.w / "HEARTBEAT.md"), "--hidden", *hargs, env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        import eval2
        old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = env["EVAL_HIDDEN_KEY"]
        try:
            line = next(p["text"] for p in eval2.read_jsonl(sd / "judged_2_rubrics.jsonl.enc")[3]["must"])
        finally:
            if old is None:
                os.environ.pop("EVAL_HIDDEN_KEY", None)
            else:
                os.environ["EVAL_HIDDEN_KEY"] = old
        leak = self.tmp / "heartbeat_with_leak.md"
        leak.write_text("### run\n" + line.split("\n")[0] + "\n")
        r = helper("scan", "--dir", str(self.w), "--exclude", str(self.w / "logs"), "--text", "--also", str(leak), "--hidden", *hargs, env=env)
        self.assertEqual(r.returncode, 1)
        self.assertIn("hold hidden text: heartbeat_with_leak.md", r.stdout)
        self.assertNotIn(MARK, r.stdout)

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
        self.assertIn("the server answered 200", hb)      # the answer of the unload call is checked and logged
        self.assertTrue((d / "run/G.passed").exists())


if __name__ == "__main__":
    unittest.main()
