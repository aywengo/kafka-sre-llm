"""Offline tests of scripts/m4_e2_starting_point.sh and scripts/m4_e2_manifest.py (E2: the starting point on the hidden half).

Stub generator, stub judge, a fake server status, a temporary key and SYNTHETIC hidden files in temporary folders; the model server,
the real hidden files and the real key are never used. The dry runs plant a marker string in every synthetic hidden row and the tests
look for it in every file the run leaves behind."""
import json
import re
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


def make_repo(parent):
    """A temporary git repository holding a copy of the tracked files of this checkout as they are in the working tree (a worker's uncommitted
    edit included), with `work` and `runs` linked to the real ones (git-ignored there, and excluded here), one commit. Tests that commit,
    edit or reset do it here, never in the real repository."""
    repo = Path(parent) / "repo"
    repo.mkdir()
    names = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True).stdout.decode().split("\0")
    for n in filter(None, names):
        if n.split("/")[0] == "runs" or not (ROOT / n).is_file():
            continue
        (repo / n).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / n, repo / n)
    for d in ("work", "runs"):
        os.symlink(ROOT / d, repo / d)
    for c in (["init", "-q"], ["config", "user.email", "t@t"], ["config", "user.name", "t"], ["config", "commit.gpgsign", "false"]):
        subprocess.run(["git", *c], cwd=repo, check=True)
    (repo / ".git/info/exclude").write_text("/work\n/runs\n")
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True, capture_output=True)
    return repo


def tgit(repo, *args):
    """git in a temporary repository only."""
    assert Path(repo).resolve() != ROOT and "e2_" in str(repo), "tgit is for the temporary repositories of these tests only"
    r = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True)
    return r.stdout.strip()


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

    def test_the_owners_decisions_are_the_defaults_and_each_is_still_an_option(self):
        t = SH.read_text()
        for need in ('MARGIN=one-sided', 'R2=no', 'READING=both', 'FJ=no', 'CROSS=yes', "none) MARGIN=\"\"",
                     "HID_J1=eval/hidden/tier_b_owner_items_final5.jsonl.enc", "HID_J2=eval/hidden/tier_b_b3_items_final.jsonl.enc",
                     "HID_R1=eval/hidden/tier_b_owner_rubrics_final5.jsonl.enc", "HID_R2=eval/hidden/tier_b_b3_rubrics_final.jsonl.enc",
                     "NA=245; NA3=60; NJ1=25; NJ2=36; NJ=61; NM=305", "TAGF=eval/m4/HIDDEN_TAGS.json", "--margin-reading) MARGIN=", "--r2) R2=yes",
                     "--f-judged) FJ=yes", "--no-cross-judge) CROSS=no", "--tag-file) TAGF="):
            self.assertIn(need, t)
        h = HELP.read_text()
        self.assertIn('"judged_1": 25, "judged_2": 36', h)
        self.assertIn('"tier_a": 245, "tier_a3": 60', h)
        self.assertEqual(sh("--dry-run", "--margin-reading", "sideways").returncode, 2)

    def test_the_tag_file_goes_to_the_three_report_tools(self):
        t = SH.read_text()
        # the coverage report, the sealed reading's totals, eval2_compare.py e2, and the sentence of the report that names the totals command
        # (M5 F1 added the start check and PREPARE, which record the tag file's hash in the manifest's options: 4 + 2)
        self.assertEqual(t.count("--tag-file $TAGF"), 6)
        self.assertIn("--by-kind --tag-file $TAGF --title", t)
        self.assertIn("eval2_sealed_reading.py totals --dir $H/bundles --tag-file $TAGF --json-out", t)
        self.assertIn("--judged-items $JI $RD --tag-file $TAGF --resamples", t)

    def test_the_hours_in_the_header_use_the_rehearsals_rates(self):
        t = SH.read_text()
        self.assertIn("runs/m4/reports/e2_rehearsal_run.md", t)
        self.assertIn("total about 19 to 22 h", t)
        self.assertIn("A restart after a crash and a re-run of the scoring stage are the same look", t)

    def test_every_answer_call_allows_a_commit_change_because_the_pin_guards_the_code(self):
        t = SH.read_text()
        self.assertIn('--host-class "Mac Studio" --allow-commit-change "$@"', t)

    def test_every_hidden_stage_checks_the_pinned_files(self):
        t = SH.read_text()
        for st in ("H1", "H2", "H3", "H4", "H5", "SCORE"):
            self.assertIn(f"pin_check {st}", t)
        self.assertIn('--hidden $HIDDEN_ARGS', t)

    def test_the_margin_reading_option_and_the_manifest_option_guards(self):
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
        h = {"tier_a": self.rows("a.enc", 245, "a"), "tier_a3": self.rows("a3.enc", 60, "c"), "judged_1": self.rows("j1.enc", 25, "j"),
             "judged_2": self.rows("j2.enc", 36, "k"), "rubrics_1": self.rows("r1.enc", 25, "j"), "rubrics_2": self.rows("r2.enc", 36, "k")}
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
        (r / "scripts").mkdir()
        (r / "scripts/a.txt").write_text("a")
        (r / "MILESTONE.md").write_text("m")
        subprocess.run(["git", "add", "scripts/a.txt", "MILESTONE.md"], cwd=r, check=True)
        subprocess.run(["git", "commit", "-qm", "x"], cwd=r, check=True)
        self.assertEqual(self.mm.tree_dirty(r), [])
        (r / "eval/hidden/x.enc").write_bytes(b"x")
        self.assertEqual(self.mm.tree_dirty(r), [])
        (r / "MILESTONE.md").write_text("changed, but not a pinned path")
        (r / "new_root_file.sh").write_text("x")
        self.assertEqual(self.mm.tree_dirty(r), [])
        (r / "scripts/new.sh").write_text("x")
        self.assertEqual(len(self.mm.tree_dirty(r)), 1)
        (r / "scripts/new.sh").unlink()
        (r / "scripts/a.txt").write_text("changed")
        self.assertEqual(len(self.mm.tree_dirty(r)), 1)
        self.assertEqual(self.mm.compute_pins(r)["count"], 1)

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
        cls.repo = make_repo(cls.tmp)   # the gate and the manifest look at this copy: a worker's edit in the real tree cannot disturb the tests
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
        r = helper("prepare", "--repo", str(cls.repo), "--out", str(cls.manifest), "--server-json", str(cls.tmp / "server.json"),
                   "--models-json", str(cls.tmp / "models.json"), "--hidden", *cls.hidden, env=cls.env)
        assert r.returncode == 0, r.stdout + r.stderr
        cls.work = cls.tmp / "work"
        cls.work.mkdir()
        cls.commit = json.loads(cls.manifest.read_text())["commit"]
        cls.pin = json.loads(cls.manifest.read_text())["pinned"]["sha256"]
        cls.base = cls.commit
        for s in ("G", "O"):
            (cls.work / f"{s}.passed").write_text(json.dumps({"stage": s, "passed": True, "pin": cls.pin, "commit": cls.commit}))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    @staticmethod
    def hargs(sd):
        return [f"tier_a={sd}/tier_a.jsonl.enc", f"tier_a3={sd}/tier_a3.jsonl.enc", f"judged_1={sd}/judged_1_items.jsonl.enc",
                f"judged_2={sd}/judged_2_items.jsonl.enc", f"rubrics_1={sd}/judged_1_rubrics.jsonl.enc", f"rubrics_2={sd}/judged_2_rubrics.jsonl.enc"]

    def gate(self, manifest=None, ledger=None, hidden=None, markers=None, extra=(), work=None):
        r = helper("gate", "--repo", str(self.repo), "--manifest", str(manifest or self.manifest), "--ledger", str(ledger or self.ledger),
                   "--work-dir", str(work or self.work), "--markers", str(markers or self.work), "--server-json", str(self.tmp / "server.json"),
                   "--models-json", str(self.tmp / "models.json"), "--tests", "skip", "--hidden", *(hidden or self.hidden), *extra, env=self.env, timeout=600)
        return r.returncode, r.stdout + r.stderr

    def reset(self):
        """Back to the base commit and a clean tree (the temporary repository only)."""
        tgit(self.repo, "reset", "-q", "--hard", self.base)
        tgit(self.repo, "clean", "-fdq")

    def setUp(self):
        self.reset()

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
        r = helper("prepare", "--repo", str(self.repo), "--out", str(man), "--server-json", str(self.tmp / "server.json"),
                   "--models-json", str(self.tmp / "models.json"), "--hidden", *h2, env=self.env)
        self.assertEqual(r.returncode, 0, r.stderr)
        rc, out = self.gate(manifest=man, hidden=h2)
        self.assertEqual(rc, 1)
        self.assertEqual(len(self.failed(out)), 1, out)   # only the count: the manifest was made from these files
        self.assertIn("tier_a: 244 rows, expected 245", out)
        self.assertNotIn(MARK, out)

    def test_refuses_when_g_or_o_did_not_pass(self):
        mk = self.tmp / "marks"
        mk.mkdir()
        (mk / "O.passed").write_text(json.dumps({"stage": "O", "passed": True, "pin": self.pin}))
        rc, out = self.gate(markers=mk)
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: stage G passed", out)
        self.assertNotIn("gate FAIL: stage O passed", out)
        (mk / "G.passed").write_text(json.dumps({"stage": "G", "passed": False, "pin": self.pin}))
        self.assertIn("gate FAIL: stage G passed", self.gate(markers=mk)[1])
        (mk / "G.passed").write_text(json.dumps({"stage": "G", "passed": True, "pin": "0" * 64}))   # a marker of other pinned files
        self.assertIn("gate FAIL: stage G passed", self.gate(markers=mk)[1])
        (mk / "G.passed").write_text(json.dumps({"stage": "G", "passed": True, "commit": self.commit}))   # a marker that knows only the commit
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
        rc, out = self.gate(extra=("--tests", "run", "--test-cmd", "false"))
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: tests", out)
        # an uncommitted change in a pinned path: the gate says so (and the pinned-files condition names the file); nowhere else it is clean
        self.assertEqual(self.gate()[0], 0)
        (self.repo / "scripts/eval2_judge.py").write_text((self.repo / "scripts/eval2_judge.py").read_text() + "\n# changed\n")
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: no uncommitted change in a pinned path (1 changed file(s): scripts/eval2_judge.py)", out)
        self.assertIn("gate FAIL: pinned files (a pinned file changed since prepare: scripts/eval2_judge.py)", out)

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

    # ---------------------------------------------------------------- pinned files, not the head commit

    def make_commit(self, **files):
        """Write the files (path -> text, appended to the existing text when the file exists) in the temporary repository and commit them."""
        for rel, text in files.items():
            f = self.repo / rel
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text((f.read_text() if f.exists() else "") + text)
            tgit(self.repo, "add", rel)
        tgit(self.repo, "commit", "-qm", "change " + ", ".join(files))

    def pin_check(self, manifest=None, extra=()):
        r = helper("pin-check", "--repo", str(self.repo), "--manifest", str(manifest or self.manifest), "--hidden", *self.hidden, *extra, env=self.env)
        return r.returncode, r.stdout + r.stderr

    def test_a_commit_touching_only_unpinned_files_does_not_refuse_the_gate_or_a_hidden_stage(self):
        head = tgit(self.repo, "rev-parse", "HEAD")
        self.make_commit(**{"MILESTONE.md": "\nnotes of the day\n", "agents/e2-note.md": "a note\n", "README.md": "\nreadme line\n",
                       "tests/test_unpinned_elsewhere.py": "# a test file outside tests/golden\n"})
        self.assertNotEqual(tgit(self.repo, "rev-parse", "HEAD"), head)
        rc, out = self.gate()
        self.assertEqual(rc, 0, out)
        self.assertIn("hidden gate: OPEN", out)
        self.assertIn("gate PASS: commit (information only) (HEAD ", out)
        self.assertIn("differs from the manifest's", out)
        self.assertIn("gate PASS: pinned files (", out)
        rc, out = self.pin_check()                  # the check before every hidden stage
        self.assertEqual(rc, 0, out)
        self.assertIn("information only", out)
        # an uncommitted change outside the pinned paths does not count either
        (self.repo / "MILESTONE.md").write_text("another edit, not committed\n")
        (self.repo / "notes_untracked.md").write_text("x\n")
        self.assertEqual(self.gate()[0], 0)
        self.assertEqual(self.pin_check()[0], 0)

    def test_a_change_to_a_pinned_file_refuses_whether_committed_or_not(self):
        # one file of every pinned path: scripts/, kafka_sre_rag/, tests/golden/, eval/m4/ (the tag file), requirements.txt
        for rel in ("scripts/eval2_score.py", "kafka_sre_rag/pipeline.py", "tests/golden/make_golden.py", "eval/m4/HIDDEN_TAGS.json", "requirements.txt",
                    "scripts/m4_e2_starting_point.sh"):
            self.reset()
            self.make_commit(**{rel: "\n# one more line\n" if not rel.endswith(".json") else "\n"})
            rc, out = self.gate()
            self.assertEqual(rc, 1, rel + out)
            self.assertIn("gate FAIL: pinned files (a pinned file changed since prepare: " + rel + ")", out, rel)
            self.assertIn("gate FAIL: manifest hashes (differs in: ", out, rel)
            self.assertIn("pinned.files." + rel, out, rel)
            rc, out = self.pin_check()
            self.assertEqual(rc, 1, rel + out)
            self.assertIn("pin REFUSED: a pinned file changed since prepare: " + rel, out, rel)
            # the same change not committed: the dirty condition names it too
            self.reset()
            f = self.repo / rel
            f.write_text(f.read_text() + ("\n# one more line\n" if not rel.endswith(".json") else "\n"))
            rc, out = self.gate()
            self.assertEqual(rc, 1, rel + out)
            self.assertIn("gate FAIL: no uncommitted change in a pinned path (1 changed file(s): " + rel + ")", out, rel)
            self.assertEqual(self.pin_check()[0], 1, rel)
            self.assertEqual(self.pin_check(extra=("--allow-dirty",))[0], 1, rel)   # --allow-dirty (dry runs) skips only the dirty condition, not the hash
        # a new tracked file under a pinned path, and a new untracked one
        self.reset()
        self.make_commit(**{"scripts/e2_new_helper.py": "print(1)\n"})
        self.assertIn("pinned files (a pinned file changed since prepare: scripts/e2_new_helper.py)", self.gate()[1])
        self.reset()
        (self.repo / "eval/m4/untracked_note.json").write_text("{}\n")
        rc, out = self.gate()
        self.assertEqual(rc, 1)
        self.assertIn("no uncommitted change in a pinned path (1 changed file(s): eval/m4/untracked_note.json)", out)

    def test_the_pin_is_the_content_of_the_files_not_the_commit(self):
        self.make_commit(**{"scripts/eval2_score.py": "\n# temporary\n"})
        self.assertEqual(self.gate()[0], 1)
        (self.repo / "scripts/eval2_score.py").write_text(tgit(self.repo, "show", self.base + ":scripts/eval2_score.py") + "\n")   # restored (the text has a final newline)
        tgit(self.repo, "add", "scripts/eval2_score.py")
        tgit(self.repo, "commit", "-qm", "restore", "--allow-empty")
        rc, out = self.gate()
        if rc != 0:   # the restored text may differ from the original by the final newline only: then restore the exact bytes
            tgit(self.repo, "checkout", self.base, "--", "scripts/eval2_score.py")
            tgit(self.repo, "commit", "-qm", "restore exactly", "--allow-empty")
            rc, out = self.gate()
        self.assertEqual(rc, 0, out)
        self.assertNotEqual(tgit(self.repo, "rev-parse", "HEAD"), self.base)

    def test_pin_check_refuses_a_changed_hidden_file_and_a_manifest_without_pins(self):
        rc, out = self.pin_check()
        self.assertEqual(rc, 0, out)
        self.assertIn("6 hidden files match", out)
        sd2 = self.tmp / "syn_other"
        self.assertEqual(helper("synth", "--dir", str(sd2), "--marker", MARK + "X", env=self.env).returncode, 0)
        r = helper("pin-check", "--repo", str(self.repo), "--manifest", str(self.manifest), "--hidden", *self.hargs(sd2), env=self.env)
        self.assertEqual(r.returncode, 1)
        self.assertIn("pin REFUSED: hidden file tier_a differs from the manifest", r.stdout)
        self.assertNotIn(MARK, r.stdout + r.stderr)
        m = json.loads(self.manifest.read_text())
        m.pop("pinned")
        old = self.tmp / "m_no_pins.json"
        old.write_text(json.dumps(m))
        rc, out = self.pin_check(manifest=old)
        self.assertEqual(rc, 1)
        self.assertIn("the manifest records no pinned files", out)

    def test_the_tests_are_rerun_only_when_a_pinned_file_changed(self):
        ok = self.tmp / "tests.ok"
        ok.unlink(missing_ok=True)
        rc, out = self.gate(extra=("--tests", "run", "--test-cmd", "true", "--tests-ok-file", str(ok)))
        self.assertEqual(rc, 0, out)
        self.assertIn("gate PASS: tests", out)
        self.make_commit(**{"MILESTONE.md": "\nnotes\n"})   # an unpinned commit: the earlier pass counts, the (failing) command is not run again
        rc, out = self.gate(extra=("--tests", "run", "--test-cmd", "false", "--tests-ok-file", str(ok)))
        self.assertEqual(rc, 0, out)
        self.assertIn("passed earlier on these pinned files", out)
        self.make_commit(**{"scripts/eval2_score.py": "\n# changed\n"})   # a pinned change: the command runs again (and fails)
        rc, out = self.gate(extra=("--tests", "run", "--test-cmd", "false", "--tests-ok-file", str(ok)))
        self.assertIn("gate FAIL: tests", out)

    def test_tree_dirty_counts_pinned_paths_only(self):
        import m4_e2_manifest as mm
        self.assertEqual(mm.tree_dirty(self.repo), [])
        (self.repo / "MILESTONE.md").write_text("x\n")
        (self.repo / "eval/hidden").mkdir(parents=True, exist_ok=True)
        (self.repo / "eval/hidden/x.enc").write_bytes(b"x")
        (self.repo / "runs_note.txt").write_text("x")
        self.assertEqual(mm.tree_dirty(self.repo), [])
        (self.repo / "scripts/new_script.sh").write_text("x")
        self.assertEqual(len(mm.tree_dirty(self.repo)), 1)

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
        self.assertEqual(nrows, 245 + 60 + 25 + 36 + 25 + 36)
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
        self.assertEqual(len(self.failed(out)), 1, out)
        self.assertTrue(self.failed(out)[0].startswith("gate FAIL: no plain file with hidden text (1 file(s): logs/run.log;"), out)   # then the counts
        self.assertNotIn(MARK, out)
        hb = self.tmp / "HB_leak.md"
        hb.write_text("### 2026 - run: " + samples["question"] + "\n")
        rc, out = self.gate(extra=("--also", str(hb)))
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: no plain file with hidden text (1 file(s): HB_leak.md;", out)
        rc, out = self.gate(extra=("--also", str(self.tmp / "no_such_heartbeat.md")))
        self.assertEqual(rc, 0, out)

    # ---------------------------------------------------------------- markers that are public are not markers; the open stages' files are frozen

    def two_lines(self):
        """Two long lines of different hidden rows (synthetic): one an open-stage answer repeats, one that stays hidden-only."""
        longest = lambda s: max(s.split("\n"), key=len)
        a = self.decrypted("tier_a.jsonl.enc")
        return longest(a[0]["question"]), longest(a[1]["question"])

    def open_work(self, name, public_line):
        """A work folder with stage markers and plain open-stage files, one of which repeats `public_line`."""
        w = self.tmp / name
        shutil.copytree(self.work, w, ignore=shutil.ignore_patterns("*.frozen.json"))
        (w / "ans_O_P0_smoke.jsonl").write_text(json.dumps({"id": "open-smoke-1", "text": "Answer: " + public_line}) + "\n")
        (w / "ans_O_P0_smoke.scores.jsonl").write_text(json.dumps({"id": "open-smoke-1", "note": public_line}) + "\n")
        return w

    def scan(self, w, *extra):
        r = helper("scan", "--dir", str(w), "--exclude", str(w / "logs"), "--text", "--hidden", *self.hidden, *extra, env=self.env, timeout=600)
        return r.returncode, r.stdout + r.stderr

    def test_an_open_stage_answer_that_repeats_a_hidden_line_does_not_fail_the_gate_and_the_coincidence_is_counted(self):
        shared, _ = self.two_lines()
        w = self.open_work("work_shared", shared)
        # the old behaviour: the line is a marker and the open-stage file holds it (shown by scanning with the markers of the hidden rows only)
        import m4_e2_manifest as mm
        old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = self.env["EVAL_HIDDEN_KEY"]
        try:
            marks, _, _ = mm.text_markers(mm.parse_hidden(self.hidden))
        finally:
            if old is None:
                os.environ.pop("EVAL_HIDDEN_KEY", None)
            else:
                os.environ["EVAL_HIDDEN_KEY"] = old
        self.assertEqual(mm.scan_text(w, marks), ["ans_O_P0_smoke.jsonl", "ans_O_P0_smoke.scores.jsonl"])
        rc, out = self.gate(work=w, markers=w)
        self.assertEqual(rc, 0, out)
        self.assertIn("hidden gate: OPEN", out)
        m = re.search(r"(\d+) marker\(s\) built, (\d+) dropped as public .* (\d+) kept; (\d+) of (\d+) hidden row\(s\) left with no marker", out)
        self.assertIsNotNone(m, out)
        self.assertGreaterEqual(int(m.group(2)), 1)
        self.assertEqual(int(m.group(1)), int(m.group(2)) + int(m.group(3)))
        self.assertIn("froze 2 plain file(s) of stages G and O", out)
        self.assertNotIn(MARK, out)
        self.assertNotIn("tier_a-001", out)   # counts only, no ids

    def test_a_hidden_only_line_still_fails_in_any_plain_file_a_log_and_a_heartbeat_when_open_stage_files_exist(self):
        shared, hidden_only = self.two_lines()
        w = self.open_work("work_hidden_only", shared)
        (w / "logs").mkdir(exist_ok=True)
        for where, content in (("other.txt", "notes: " + hidden_only), ("logs/run.log", "[answer] " + hidden_only)):
            (w / where).write_text(content + "\n")
            rc, out = self.gate(work=w, markers=w)
            self.assertEqual(rc, 1, where)
            self.assertIn("gate FAIL: no plain file with hidden text (1 file(s): " + where + ";", out)
            (w / where).unlink()
        hb = self.tmp / "HB_hidden_only.md"
        hb.write_text("### x: " + hidden_only + "\n")
        rc, out = self.gate(work=w, markers=w, extra=("--also", str(hb)))
        self.assertEqual(rc, 1)
        self.assertIn("HB_hidden_only.md", out)
        self.assertNotIn(MARK, out)
        self.assertFalse((w / "OPEN_STAGES.frozen.json").exists())   # a refused gate freezes nothing
        rc, out = self.gate(work=w, markers=w)
        self.assertEqual(rc, 0, out)

    def test_a_changed_open_stage_file_after_the_gate_is_caught_by_the_later_scan_and_the_next_gate(self):
        shared, hidden_only = self.two_lines()
        w = self.open_work("work_changed", shared)
        (w / "ans_O_P0_smoke.scores.jsonl").write_text("{}\n")   # only the answers file repeats the line
        rc, out = self.gate(work=w, markers=w)
        self.assertEqual(rc, 0, out)
        rec = json.loads((w / "OPEN_STAGES.frozen.json").read_text())
        self.assertEqual(sorted(rec["files"]), ["ans_O_P0_smoke.jsonl", "ans_O_P0_smoke.scores.jsonl"])
        self.assertTrue(all(len(v) == 64 for v in rec["files"].values()))
        rc, out = self.scan(w, "--require-freeze")
        self.assertEqual(rc, 0, out)
        self.assertIn("dropped as public", out)
        (w / "hidden_stage_output.txt").write_text("nothing special\n")
        self.assertEqual(self.scan(w, "--require-freeze")[0], 0)
        # a changed file is scanned in full: the line it repeated is no longer public, so it is a kept marker and the scan fails
        with open(w / "ans_O_P0_smoke.jsonl", "a") as f:
            f.write(json.dumps({"id": "open-smoke-2", "text": "edited after the gate"}) + "\n")
        rc, out = self.scan(w, "--require-freeze")
        self.assertEqual(rc, 1)
        self.assertIn("changed or appeared after the gate", out)
        self.assertIn("hold hidden text: ans_O_P0_smoke.jsonl", out)
        self.assertNotIn(MARK, out)
        rc, out = self.gate(work=w, markers=w)
        self.assertEqual(rc, 1)
        self.assertIn("gate FAIL: plain files of stages G and O unchanged since they were frozen (1 changed or new: ans_O_P0_smoke.jsonl)", out)
        # a changed file without any hidden line passes the scan (and is named)
        w2 = self.open_work("work_changed_clean", "an open line that no hidden row holds in any field of it")
        self.assertEqual(self.gate(work=w2, markers=w2)[0], 0)
        with open(w2 / "ans_O_P0_smoke.jsonl", "a") as f:
            f.write("more open text\n")
        rc, out = self.scan(w2, "--require-freeze")
        self.assertEqual(rc, 0, out)
        self.assertIn("1 plain file(s) of stages G and O changed or appeared after the gate", out)
        # a new open-stage file after the gate is a change too
        (w2 / "ans_G_late.jsonl").write_text("late\n")
        self.assertIn("ans_G_late.jsonl", self.scan(w2, "--require-freeze")[1])

    def test_the_scan_after_the_run_requires_the_record_of_the_gate(self):
        shared, _ = self.two_lines()
        w = self.open_work("work_norecord", shared)
        rc, out = self.scan(w, "--require-freeze")
        self.assertEqual(rc, 1)
        self.assertIn("the gate has not frozen them", out)

    def test_explain_mode_prints_counts_only_and_fails_on_a_marker_that_is_nowhere_public(self):
        shared, hidden_only = self.two_lines()
        w = self.open_work("work_explain", shared)
        longest = lambda s: max(s.split("\n"), key=len)
        a = self.decrypted("tier_a.jsonl.enc")
        corpus = self.tmp / "explain_corpus.jsonl"      # stands for the pinned open evaluation files
        corpus.write_text(json.dumps({"question": shared}) + "\n")
        (w / "smoke_items.jsonl").write_text(json.dumps({"question": longest(a[2]["question"])}) + "\n")   # an open stage's input
        (w / "ans_O_P0_smoke.jsonl").write_text(json.dumps({"text": shared + " " + longest(a[2]["question"])}) + "\n")
        rc, out = self.scan(w, "--explain", "--corpus-files", str(corpus))
        self.assertEqual(rc, 0, out)
        self.assertIn("explain: ans_O_P0_smoke.jsonl (open-stage file): 2 matching marker(s); 1 in the open evaluation files, 1 in the open stages' inputs, 0 nowhere public", out)
        self.assertIn("explain: 3 plain file(s) match; 0 with a marker that is nowhere public", out)   # the scores file and the input file match too
        for forbidden in (MARK, shared, hidden_only, "tier_a-001"):
            self.assertNotIn(forbidden, out)
        # nowhere public in a file an open stage produced: the scan fails and says so
        (w / "ans_O_P0_smoke.jsonl").write_text(json.dumps({"text": hidden_only}) + "\n")
        rc, out = self.scan(w, "--explain", "--corpus-files", str(corpus))
        self.assertEqual(rc, 1)
        self.assertIn("explain FAIL: ans_O_P0_smoke.jsonl: 1 matching marker(s) occur nowhere public although an open stage produced this file", out)
        self.assertNotIn(hidden_only, out)
        self.assertNotIn(MARK, out)
        # another file with such a marker fails too, as the file of another kind
        (w / "ans_O_P0_smoke.jsonl").write_text("{}\n")
        (w / "other.txt").write_text(hidden_only + "\n")
        rc, out = self.scan(w, "--explain", "--corpus-files", str(corpus))
        self.assertEqual(rc, 1)
        self.assertIn("explain: other.txt (other file): 1 matching marker(s); 0 in the open evaluation files, 0 in the open stages' inputs, 1 nowhere public", out)

    def test_shape_ok_on_the_synthetic_files_runs_the_tools_and_removes_its_temporary_folder(self):
        priv = self.tmp / "shape_tmp"      # the tools make their temporary folder here (TMPDIR), so that parallel runs of the suite cannot disturb the check
        priv.mkdir()
        r = helper("shape", "--hidden", *self.hidden, env={**self.env, "TMPDIR": str(priv)}, timeout=900)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out[-2500:])
        for need in ("shape: OK", "stub judge rows: 122 valid of 122 (expected 122)", "stub answers on the judged items: 61 of 61 usable",
                     "scorer: 305 scored, 0 missing", "throwaway reading bundle (two answer sets, 6 first and 2 second sessions): exit 0",
                     "the temporary folder is removed (yes)", "read by answer, judge, bundle: evidence (optional)", "kinds (merged so that no group is under 5)"):
            self.assertIn(need, out)
        self.assertNotIn(MARK, out)
        for rid in ("hj-001", "hj-061", "tier_a-001", "tier_a3-060"):
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

    def test_shape_passes_on_known_judged_metadata_keys_and_fails_on_another_unread_key(self):
        def edit(rows):
            for r in rows:
                r["side"] = f"{MARK} side text"
            for r in rows[:2]:
                r["adaptation"] = f"{MARK} adaptation text"
        sd2 = self.reencrypt("judged_1_items.jsonl.enc", edit)
        r = helper("shape", "--hidden", *self.hargs(sd2), "--no-tools", env=self.env, timeout=300)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out[-2000:])
        self.assertIn("known metadata (labelled metadata by a sealed session, 2026-11-20): adaptation 2, side ", out)
        self.assertNotIn("PROBLEM", out)
        self.assertNotIn(MARK, out)

        def edit2(rows):
            edit(rows)
            rows[0]["side_note"] = f"{MARK} other"
        sd3 = self.reencrypt("judged_1_items.jsonl.enc", edit2)
        r = helper("shape", "--hidden", *self.hargs(sd3), "--no-tools", env=self.env, timeout=300)
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 1, out[-2000:])
        self.assertIn("shape PROBLEM: judged_1: key side_note holds text in 1 row(s) and nothing reads it", out)
        self.assertNotIn("key side holds", out)
        self.assertNotIn(MARK, out)

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
        cls.r1 = sh("--dry-run", "--work", str(cls.w), "--allow-dirty", "--synth-marker", MARK, "--margin-reading", "none")
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
        self.assertEqual(len(ids), 366)
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
        for i in list(ids)[:366]:
            self.assertNotIn(i, hb)
        for p in self.w.rglob("*"):
            if p.is_file() and not p.name.endswith(".enc") and "logs" not in p.relative_to(self.w).parts and p.name != "dry.key" and not p.name.endswith(".tmp"):
                t = p.read_text(errors="ignore")
                for i in ("hj-001", "hj-061", "tier_a-001", "tier_a3-060"):
                    self.assertNotIn(i, t, f"{i} in {p.name}")
        # the logs hold ids and timings: progress lines of the hidden stages
        logs = "".join(p.read_text(errors="ignore") for p in (self.w / "logs").glob("*.log"))
        self.assertIn("tier_a-001", logs)

    def test_the_shape_check_runs_before_the_gate_and_the_text_scan_passes_on_the_run(self):
        shape = (self.w / "shape.out").read_text()
        self.assertIn("shape: OK", shape)
        self.assertIn("stub judge rows: 122 valid of 122", shape)
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
        self.assertEqual(r.returncode, 2)   # since M5 F1 the start check refuses a changed manifest before any stage; the gate's own check is tested in HiddenGate
        self.assertIn("start check REFUSED", r.stdout)
        self.assertIn("differs: checker.version", r.stdout)
        self.assertNotIn("[answer] run", r.stdout)


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
        self.assertIn("gate FAIL: hidden row counts 245, 60, 25, 36", r.stdout)
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


@unittest.skipUnless(READY, "needs the open files, the index, the catalogues and the venv")
class RehearsalPrepare(unittest.TestCase):
    """rehearsal-prepare: encrypted copies of open items, plain input files, the ledger; and its refusals."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="e2_reh_"))
        cls.env = {"EVAL_HIDDEN_KEY": str(cls.tmp / "rehearsal.key")}
        subprocess.run([str(PY), str(ROOT / "scripts/eval2.py"), "keygen", "--path", cls.env["EVAL_HIDDEN_KEY"]], cwd=ROOT, check=True, capture_output=True)
        cls.r = helper("rehearsal-prepare", "--dir", str(cls.tmp), env=cls.env)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def enc(self, name):
        import eval2
        old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = self.env["EVAL_HIDDEN_KEY"]
        try:
            return eval2.read_jsonl(self.tmp / "rehearsal_hidden" / name)
        finally:
            if old is None:
                os.environ.pop("EVAL_HIDDEN_KEY", None)
            else:
                os.environ["EVAL_HIDDEN_KEY"] = old

    def plain(self, name):
        return [json.loads(l) for l in (self.tmp / "rehearsal_plain" / name).read_text().splitlines() if l.strip()]

    def test_counts_and_files(self):
        self.assertEqual(self.r.returncode, 0, self.r.stderr)
        self.assertEqual([len(self.enc(n)) for n in ("tier_a.jsonl.enc", "tier_a3.jsonl.enc", "judged_1_items.jsonl.enc", "judged_2_items.jsonl.enc")], [10, 10, 2, 4])
        self.assertEqual([len(self.enc(n)) for n in ("judged_1_rubrics.jsonl.enc", "judged_2_rubrics.jsonl.enc")], [2, 4])
        self.assertEqual(self.enc("judged_1_items.jsonl.enc")[0]["id"], self.enc("judged_1_rubrics.jsonl.enc")[0]["id"])
        for n in (("smoke10_items", 10), ("smoke6_items", 6), ("a3_10_items", 10), ("pilot6_items", 6), ("pilot6_rubrics", 6)):
            self.assertEqual(len(self.plain(n[0] + ".jsonl")), n[1], n[0])
        for p in (self.tmp / "rehearsal_hidden").iterdir():
            self.assertTrue(p.name.endswith(".enc"))
        for r in self.enc("tier_a.jsonl.enc") + self.enc("judged_2_items.jsonl.enc"):
            self.assertNotIn(r["id"], self.r.stdout)   # counts only

    def test_samples_are_disjoint_spread_and_keep_pairs_together(self):
        a_ids = {r["id"] for r in self.enc("tier_a.jsonl.enc")}
        smoke = set(json.loads((ROOT / "eval/m4/smoke_ids.json").read_text())["ids"])
        self.assertFalse(a_ids & smoke)
        self.assertGreaterEqual(len({r["kind"] for r in self.enc("tier_a.jsonl.enc")}), 5)       # spread over the kinds
        hid3, o3 = self.enc("tier_a3.jsonl.enc"), self.plain("a3_10_items.jsonl")
        self.assertFalse({r["id"] for r in hid3} & {r["id"] for r in o3})
        allr = [json.loads(l) for l in (ROOT / "eval/m4/open/tier_a3.jsonl").read_text().splitlines() if l.strip()]
        for sample in (hid3, o3):
            ids = {r["id"] for r in sample}
            for pr in {r["pair"] for r in sample if r.get("pair")}:
                self.assertEqual({r["id"] for r in allr if r.get("pair") == pr}, {r["id"] for r in sample if r.get("pair") == pr})
            self.assertEqual(len(ids), 10)
        j = {r["id"] for n in ("judged_1_items.jsonl.enc", "judged_2_items.jsonl.enc") for r in self.enc(n)}
        self.assertFalse(j & {r["id"] for r in self.plain("pilot6_items.jsonl")})
        self.assertTrue(j <= {json.loads(l)["id"] for l in (ROOT / "eval/m4/b2/items.jsonl").read_text().splitlines() if l.strip()})

    def test_ledger_is_in_the_folder_and_complete_and_the_real_one_is_not_touched(self):
        self.assertEqual(self.r.returncode, 0)
        led = self.tmp / "REHEARSAL_LEDGER.md"
        self.assertTrue(led.exists())
        import m4_e2_manifest as mm
        self.assertEqual(mm.ledger_problems(led), [])
        self.assertIn("REHEARSAL", led.read_text())

    def test_the_key_must_be_in_the_rehearsal_folder_and_files_are_not_overwritten(self):
        other = Path(tempfile.mkdtemp(prefix="e2_reh_k_"))
        self.addCleanup(shutil.rmtree, other, True)
        r = helper("rehearsal-prepare", "--dir", str(other), env={"EVAL_HIDDEN_KEY": self.env["EVAL_HIDDEN_KEY"]})
        self.assertEqual(r.returncode, 2)
        self.assertIn("never the real key", r.stderr)
        self.assertEqual(list(other.iterdir()), [])
        before = (self.tmp / "rehearsal_hidden/tier_a.jsonl.enc").read_bytes()
        self.assertIn("not overwritten", helper("rehearsal-prepare", "--dir", str(self.tmp), env=self.env).stdout)
        self.assertEqual(before, (self.tmp / "rehearsal_hidden/tier_a.jsonl.enc").read_bytes())

    def test_spread_fills_exactly_and_skips_groups_that_do_not_fit(self):
        import m4_e2_manifest as mm
        groups = [("k1", f"g{i}", [1, 2]) for i in range(4)] + [("k2", f"s{i}", [1]) for i in range(3)]
        got = mm.spread(groups, 5, "1", size=lambda g: len(g[2]))
        self.assertEqual(sum(len(g[2]) for g in got), 5)
        self.assertEqual({g[0] for g in got}, {"k1", "k2"})
        with self.assertRaises(Exception):
            mm.spread([("k", "a", [1, 2])], 3, "1", size=lambda g: len(g[2]))

    def test_git_ignores_the_rehearsal_folder(self):
        r = subprocess.run(["git", "check-ignore", "-q", "work/m4/e2_rehearsal/x.jsonl"], cwd=ROOT)
        self.assertEqual(r.returncode, 0)


@unittest.skipUnless(READY, "needs the open files, the index, the catalogues and the venv")
class RehearsalDryRun(unittest.TestCase):
    """The chain in rehearsal mode with the stub backend: every stage, the fake findings, the totals, the JSON hand-over; refusals; resume."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="e2_rehdry_"))
        cls.w = cls.tmp / "run"
        cls.r1 = sh("--dry-run", "--rehearsal", "--work", str(cls.w), "--allow-dirty", "--margin-reading", "none")
        cls.out1 = cls.r1.stdout + cls.r1.stderr

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_every_stage_passes_with_the_rehearsal_counts_and_the_prefixes(self):
        self.assertEqual(self.r1.returncode, 0, self.out1[-3000:])
        for need in ("REHEARSAL copies of open items", "gate G: 10 answers", "hidden row counts 10, 10, 2, 4", "hidden gate: OPEN", "shape: OK",
                     "scorer: 20 scored, 0 missing", "first readers 2, second readers 1", "judging H4, round 1: all rows valid", "judging H5, round 1: all rows valid",
                     "rehearsal-findings: 3 findings file(s) written", "no plain file under", "dry run done"):
            self.assertIn(need, self.out1)
        self.assertTrue((self.w / "logs/REHEARSAL_m4_e2_starting_point.done").exists())
        self.assertTrue((self.w / "logs/REHEARSAL_m4_e2_starting_point.readings_ready").exists())
        self.assertFalse(list((self.w / "logs").glob("m4_e2_starting_point.*")))      # nothing under the name of the real run
        hb = (self.w / "HEARTBEAT.md").read_text()
        self.assertIn("REHEARSAL_m4_e2_starting_point: G started", hb)
        for line in hb.splitlines():
            if "m4_e2_starting_point:" in line:
                self.assertIn("REHEARSAL_m4_e2_starting_point:", line)
        rep = (self.w / "report_from_stub_data.md").read_text()
        self.assertTrue(rep.startswith("# REHEARSAL E2"))
        self.assertIn("a verdict on 20 items means nothing", rep)
        self.assertIn("MARGIN READING NOT CHOSEN", rep)
        self.assertTrue((self.w / "REHEARSAL.mode").exists())
        self.assertFalse((self.w / "HIDDEN_LEDGER.md").exists())
        self.assertTrue((self.w / "REHEARSAL_LEDGER.md").exists())
        self.assertTrue((self.w / "E2_REHEARSAL_MANIFEST.json").exists())
        self.assertFalse((self.w / "dry.key").exists())          # the rehearsal's own key
        self.assertTrue((self.w / "rehearsal.key").exists())

    def test_the_reading_totals_and_the_json_hand_over_ran_on_the_fake_findings(self):
        self.assertTrue((self.w / "hidden/reading_totals.json.enc").exists())
        self.assertTrue((self.w / "hidden/bundles/findings_first_1.jsonl.enc").exists())
        txt = (self.w / "reading_totals.txt").read_text()
        self.assertIn("set default: answers read", txt)
        self.assertIn("E3", txt)
        cmp_ = (self.w / "compare_e2.txt").read_text()
        self.assertIn("(b)", cmp_)

    def test_scan_finds_no_rehearsal_id_or_text_outside_the_enc_files(self):
        after = (self.w / "scan_after.txt").read_text()
        self.assertIn("holds an id of a hidden item (26 ids checked)", after)
        self.assertIn("holds distinctive text of a hidden row", after)
        import m4_e2_manifest as mm
        sd = self.w / "rehearsal_hidden"
        h = {"tier_a": f"{sd}/tier_a.jsonl.enc", "tier_a3": f"{sd}/tier_a3.jsonl.enc", "judged_1": f"{sd}/judged_1_items.jsonl.enc",
             "judged_2": f"{sd}/judged_2_items.jsonl.enc", "rubrics_1": f"{sd}/judged_1_rubrics.jsonl.enc", "rubrics_2": f"{sd}/judged_2_rubrics.jsonl.enc"}
        old = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = str(self.w / "rehearsal.key")
        try:
            ids = mm.hidden_ids(h)
            marks, _, _ = mm.text_markers(h, corpus_files=sorted(str(p) for p in (self.w / "rehearsal_plain").glob("*.jsonl")))
        finally:
            if old is None:
                os.environ.pop("EVAL_HIDDEN_KEY", None)
            else:
                os.environ["EVAL_HIDDEN_KEY"] = old
        self.assertEqual(len(ids), 26)
        self.assertGreater(len(marks), 50)
        # ids: only the logs may hold them (progress lines, one per item, are what the guard watches); text: no plain file at all, logs included
        self.assertEqual(mm.scan_plain(self.w, ids, exclude=[self.w / "logs"]), [])
        self.assertEqual(mm.scan_text(self.w, marks, also=[self.w / "HEARTBEAT.md"]), [])
        logs = "".join(p.read_text(errors="ignore") for p in (self.w / "logs").glob("*.log"))
        self.assertTrue(any(i in logs for i in ids))

    def test_refusals(self):
        r = sh("--dry-run", "--rehearsal", "--hidden-a", "eval/hidden/tier_a.jsonl.enc", "--allow-dirty")
        self.assertEqual(r.returncode, 2)
        self.assertIn("never combined with the real hidden files", r.stderr)
        for opt in ("--ledger", "--manifest"):
            r = sh("--dry-run", "--rehearsal", opt, str(self.tmp / "x"), "--allow-dirty")
            self.assertEqual(r.returncode, 2, opt)
            self.assertIn("has its own ledger and manifest", r.stderr)
        r = sh("--dry-run", "--rehearsal", "--work", "work/m4/e2", "--allow-dirty")
        self.assertEqual(r.returncode, 2)
        self.assertIn("never runs in the work folder of the real run", r.stderr)
        # a real run does not start in a folder with rehearsal markers
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty")
        self.assertEqual(r.returncode, 2)
        self.assertIn("holds the markers of a rehearsal", r.stderr)
        # and a rehearsal does not start in a folder with the markers of a real run
        d = self.tmp / "real_marked"
        d.mkdir()
        (d / "REAL.mode").write_text("real\n")
        r = sh("--dry-run", "--rehearsal", "--work", str(d), "--allow-dirty")
        self.assertEqual(r.returncode, 2)
        self.assertIn("holds the markers of a real run", r.stderr)
        self.assertFalse((d / "rehearsal.key").exists())

    def test_the_real_run_marks_its_folder_and_the_default_rehearsal_folder_is_not_the_real_one(self):
        t = SH.read_text()
        self.assertIn("W=work/m4/e2_rehearsal", t)
        self.assertIn("runs/m4/E2_REHEARSAL.md", t)
        self.assertNotIn("HIDDEN_LEDGER.md", t[t.index("if [ $REH = yes ]; then   # the rehearsal's temporary key"):t.index("if [ $DRY = no ]; then\n  rm -f")])

    def test_resume_skips_finished_stages(self):
        r = sh("--dry-run", "--rehearsal", "--work", str(self.w), "--allow-dirty")
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 0, out[-3000:])
        self.assertGreaterEqual(out.count("skip "), 12)
        for need in ("skip G: passed earlier", "skip O: passed earlier", "skip bundles: built", "skip judging H4", "skip scoring Q4D_mech"):
            self.assertIn(need, out)
        self.assertNotIn("[answer] run", out)
        self.assertIn("rehearsal-findings: 0 findings file(s) written", out)


def tree_state(*roots):
    """name -> sha256 of every file under the roots (a refusal must change nothing)."""
    import hashlib
    out = {}
    for r in roots:
        r = Path(r)
        for f in ([r] if r.is_file() else sorted(r.rglob("*"))):
            if f.is_file():
                out[str(f)] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


@unittest.skipUnless(READY, "needs the open files, the index, the catalogues and the venv")
class StartCheck(unittest.TestCase):
    """M5 F1: the check at the start of every launch (helper `start-check`) on synthetic hidden files, a temporary key and a manifest written by `prepare`."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="e2_start_"))
        cls.repo = make_repo(cls.tmp)
        cls.env = {"EVAL_HIDDEN_KEY": str(cls.tmp / "k.key")}
        subprocess.run([str(PY), str(ROOT / "scripts/eval2.py"), "keygen", "--path", cls.env["EVAL_HIDDEN_KEY"]], cwd=ROOT, check=True, capture_output=True)
        cls.sd = cls.tmp / "syn"
        assert helper("synth", "--dir", str(cls.sd), "--marker", MARK, env=cls.env).returncode == 0
        cls.models = cls.tmp / "models.json"
        cls.models.write_text(json.dumps({"data": [{"id": "Qwen3.8-27B-4bit"}, {"id": "mlx-community--Qwen3.8-27B-bf16"},
                                                   {"id": "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"}, {"id": "GLM-5.3-Flash-oQ8e-mtp"}]}))
        (cls.tmp / "server.json").write_text(json.dumps({"loaded_models": [], "version": "0.0-test"}))
        cls.tags = cls.tmp / "TAGS.json"
        cls.tags.write_text(json.dumps({"schema": 1, "tags": {"conductor_exposed": {"ids": ["synthetic-1"]}}}))
        cls.hidden = HiddenGate.hargs(cls.sd)
        cls.manifest = cls.tmp / "m.json"
        r = helper("prepare", "--repo", str(cls.repo), "--out", str(cls.manifest), "--server-json", str(cls.tmp / "server.json"), "--models-json", str(cls.models),
                   "--mode", "real", "--tag-file", str(cls.tags), "--hidden", *cls.hidden, env=cls.env)
        assert r.returncode == 0, r.stdout + r.stderr
        cls.pin = json.loads(cls.manifest.read_text())["pinned"]["sha256"]
        cls.base = tgit(cls.repo, "rev-parse", "HEAD")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self):
        tgit(self.repo, "reset", "-q", "--hard", self.base)
        tgit(self.repo, "clean", "-fdq")
        self.work = self.tmp / "work"
        shutil.rmtree(self.work, ignore_errors=True)
        self.work.mkdir()
        (self.work / "ans.txt").write_text("answers\n")
        self.write_marker("G", {"ans.txt": __import__("hashlib").sha256(b"answers\n").hexdigest()})

    def write_marker(self, stage, outputs=None, pin=None):
        d = {"stage": stage, "passed": True, "pin": pin or self.pin, "commit": "c"}
        if outputs is not None:
            d["outputs"] = outputs
        (self.work / f"{stage}.passed").write_text(json.dumps(d))

    def check(self, *, manifest=None, models=None, mode="real", tags=None, extra=()):
        r = helper("start-check", "--repo", str(self.repo), "--manifest", str(manifest or self.manifest), "--work-dir", str(self.work),
                   "--models-json", str(models or self.models), "--mode", mode, "--tag-file", str(tags or self.tags), *extra, "--hidden", *self.hidden,
                   env=self.env, timeout=300)
        return r.returncode, r.stdout + r.stderr

    def refused(self, out, *fields):
        self.assertIn("start check REFUSED", out)
        for f in fields:
            self.assertIn(f"differs: {f}", out)
        self.assertNotIn(MARK, out)

    def test_an_unchanged_restart_resumes_and_says_how_many_stages_are_done(self):
        rc, out = self.check()
        self.assertEqual(rc, 0, out)
        self.assertIn("resuming; 1 stage(s) already done (G)", out)
        self.assertEqual(len(out.strip().splitlines()), 1, "one line")

    def test_a_fresh_work_folder_starts(self):
        shutil.rmtree(self.work)
        rc, out = self.check(manifest=self.tmp / "none.json")
        self.assertEqual(rc, 0, out)
        self.assertIn("a fresh run", out)

    def test_a_changed_pinned_file_is_refused_with_its_name(self):
        f = self.repo / "scripts/eval2_score.py"
        f.write_text(f.read_text() + "\n# changed\n")
        rc, out = self.check()
        self.assertEqual(rc, 2, out)
        self.refused(out, "pinned.files.scripts/eval2_score.py", "pinned.sha256", "scorer.sha256")
        self.assertRegex(out, r"pinned\.files\.scripts/eval2_score\.py: [0-9a-f]{8} -> [0-9a-f]{8}\n")   # short hashes only

    def test_a_changed_option_is_refused_with_the_option_named(self):
        for extra, field in ((("--r2",), "options.r2"), (("--f-judged",), "options.f_judged"), (("--no-cross-judge",), "options.cross_judge")):
            rc, out = self.check(extra=extra)
            self.assertEqual(rc, 2, out)
            self.refused(out, field)

    def test_a_changed_tag_file_is_refused(self):
        other = self.tmp / "TAGS2.json"
        other.write_text(json.dumps({"schema": 1, "tags": {"conductor_exposed": {"ids": ["synthetic-1", "synthetic-2"]}}}))
        rc, out = self.check(tags=other)
        self.assertEqual(rc, 2, out)
        self.refused(out, "options.tag_file.name", "options.tag_file.sha256")
        self.assertNotIn("synthetic-", out)   # names and short hashes only, never content
        self.tags.write_text(self.tags.read_text())
        self.tags.write_text(json.dumps({"schema": 1, "tags": {"conductor_exposed": {"ids": ["synthetic-9"]}}}))
        try:
            rc, out = self.check()
            self.assertEqual(rc, 2, out)
            self.refused(out, "options.tag_file.sha256")
        finally:
            self.tags.write_text(json.dumps({"schema": 1, "tags": {"conductor_exposed": {"ids": ["synthetic-1"]}}}))

    def test_a_changed_model_id_and_a_changed_mode_are_refused(self):
        m2 = self.tmp / "models2.json"
        m2.write_text(json.dumps({"data": [{"id": "Qwen3.8-27B-4bit-b"}, {"id": "mlx-community--Qwen3.8-27B-bf16"},
                                           {"id": "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"}, {"id": "GLM-5.3-Flash-oQ8e-mtp"}]}))
        rc, out = self.check(models=m2)
        self.assertEqual(rc, 2, out)
        self.refused(out, "server.q4")
        rc, out = self.check(mode="rehearsal")
        self.assertEqual(rc, 2, out)
        self.refused(out, "options.mode")

    def test_a_changed_hidden_file_is_refused(self):
        other = [h.replace("tier_a3.jsonl.enc", "tier_a3.jsonl.enc") for h in self.hidden]
        shutil.copy(self.sd / "tier_a.jsonl.enc", self.tmp / "copy.enc")
        (self.tmp / "copy.enc").write_bytes((self.tmp / "copy.enc").read_bytes() + b"x")
        other[0] = f"tier_a={self.tmp}/copy.enc"
        r = helper("start-check", "--repo", str(self.repo), "--manifest", str(self.manifest), "--work-dir", str(self.work), "--models-json", str(self.models),
                   "--mode", "real", "--tag-file", str(self.tags), "--hidden", *other, env=self.env, timeout=300)
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.refused(r.stdout, "hidden_files.tier_a.sha256")

    def test_a_marker_without_a_manifest_is_refused(self):
        rc, out = self.check(manifest=self.tmp / "absent.json")
        self.assertEqual(rc, 2, out)
        self.assertIn("start check REFUSED", out)
        self.assertIn("differs: stage marker(s) without a manifest: G.passed", out)

    def test_a_done_marker_whose_output_changed_or_is_missing_is_refused(self):
        (self.work / "ans.txt").write_text("answers, edited\n")
        rc, out = self.check()
        self.assertEqual(rc, 2, out)
        self.assertRegex(out, r"differs: marker G\.passed: output ans\.txt changed \([0-9a-f]{8} -> [0-9a-f]{8}\)")
        (self.work / "ans.txt").unlink()
        rc, out = self.check()
        self.assertEqual(rc, 2, out)
        self.assertIn("differs: marker G.passed: output ans.txt is missing", out)

    def test_a_marker_of_another_manifest_is_refused(self):
        self.write_marker("O", {"ans.txt": __import__("hashlib").sha256(b"answers\n").hexdigest()}, pin="0" * 64)
        rc, out = self.check()
        self.assertEqual(rc, 2, out)
        self.assertIn("marker O.passed: written under another manifest (pin 00000000 against", out)

    def test_a_marker_without_recorded_outputs_resumes_with_a_note(self):
        self.write_marker("G")   # a marker of an older chain
        rc, out = self.check()
        self.assertEqual(rc, 0, out)
        self.assertIn("marker G.passed records no output hashes", out)

    def test_a_refusal_moves_and_deletes_nothing_and_prints_the_mv_lines(self):
        before = tree_state(self.work, self.manifest)
        (self.work / "ans.txt").write_text("answers, edited\n")
        before = tree_state(self.work, self.manifest)
        rc, out = self.check(extra=("--r2",))
        self.assertEqual(rc, 2, out)
        self.assertEqual(before, tree_state(self.work, self.manifest))
        self.assertTrue(self.work.exists() and self.manifest.exists())
        mv = [l.strip() for l in out.splitlines() if l.strip().startswith("mv ")]
        self.assertEqual(len(mv), 2, out)
        self.assertRegex(mv[0], rf"^mv {re.escape(str(self.work))} {re.escape(str(self.work))}\.aside-\d{{8}}-\d{{6}}$")
        self.assertRegex(mv[1], rf"^mv {re.escape(str(self.manifest))} {re.escape(str(self.manifest))}\.aside-\d{{8}}-\d{{6}}$")


@unittest.skipUnless(READY, "needs the open files, the index, the catalogues and the venv")
class StartCheckInTheChain(unittest.TestCase):
    """M5 F1 through the chain itself (stub backend, fake server): the check runs before any stage and a refusal exits with code 2."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="e2_startchain_"))
        cls.w = cls.tmp / "run"
        cls.r1 = sh("--dry-run", "--work", str(cls.w), "--allow-dirty", "--stages", "G")
        cls.out_sha = __import__("hashlib").sha256((cls.w / "ans_G.jsonl").read_bytes()).hexdigest() if (cls.w / "ans_G.jsonl").exists() else None   # before any test edits it

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_the_first_launch_is_fresh_and_the_marker_records_its_output_hashes(self):
        self.assertEqual(self.r1.returncode, 0, self.r1.stdout[-2000:])
        self.assertIn("start check: no manifest and no stage marker: a fresh run", self.r1.stdout)
        d = json.loads((self.w / "G.passed").read_text())
        self.assertEqual(list(d["outputs"]), ["ans_G.jsonl"])
        self.assertEqual(d["outputs"]["ans_G.jsonl"], self.out_sha)
        m = json.loads((self.w / "E2_MANIFEST.json").read_text())
        self.assertEqual(m["options"]["mode"], "real")
        self.assertEqual(m["options"]["tag_file"]["name"], "HIDDEN_TAGS.json")

    def test_an_unchanged_restart_resumes_and_a_changed_option_or_output_is_refused_with_code_2(self):
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty", "--stages", "G")
        self.assertEqual(r.returncode, 0, r.stdout[-2000:])
        self.assertIn("start check: the stored manifest agrees with this launch; resuming; 1 stage(s) already done (G)", r.stdout)
        self.assertIn("skip G: passed earlier", r.stdout)
        before = tree_state(self.w)
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty", "--stages", "G", "--r2")
        out = r.stdout + r.stderr
        self.assertEqual(r.returncode, 2, out)
        self.assertIn("differs: options.r2: False -> True", out)
        self.assertIn(f"mv {self.w} {self.w}.aside-", out)
        self.assertNotIn("[answer] run", out)
        self.assertNotIn("skip G", out)
        self.assertEqual(before, tree_state(self.w), "a refusal changes nothing")
        # a changed output of a done stage
        (self.w / "ans_G.jsonl").write_text((self.w / "ans_G.jsonl").read_text() + "\n")
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty", "--stages", "G")
        self.assertEqual(r.returncode, 2, r.stdout[-1500:])
        self.assertIn("differs: marker G.passed: output ans_G.jsonl changed", r.stdout)
        # a marker without a manifest
        (self.w / "E2_MANIFEST.json").rename(self.tmp / "manifest.moved")
        r = sh("--dry-run", "--work", str(self.w), "--allow-dirty", "--stages", "G")
        self.assertEqual(r.returncode, 2, r.stdout[-1500:])
        self.assertIn("stage marker(s) without a manifest: G.passed", r.stdout)
        (self.tmp / "manifest.moved").rename(self.w / "E2_MANIFEST.json")

    def test_the_inline_marks_step_takes_allow_commit_change(self):
        t = SH.read_text()
        i = t.index("--revise-from $H/ans_Q4D_judged.jsonl.enc")
        self.assertIn("--allow-commit-change", t[i:i + 400])


if __name__ == "__main__":
    unittest.main()
