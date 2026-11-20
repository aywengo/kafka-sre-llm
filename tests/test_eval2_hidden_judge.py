"""Hidden path of the judge step and of its coverage report (scripts/eval2_judge.py, scripts/eval2_pilot_report.py), and the chunk
helper's tag stripping. Offline: synthetic rows, a temporary key in a temporary folder, a stub judge; nothing under eval/hidden/."""
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import eval2_judge as ej  # noqa: E402
import eval2_pilot_report as pr  # noqa: E402

SECRETS = ("SECRETQ", "SECRETANS", "SECRETQUOTE")
KINDS = ["cfg"] * 6 + ["dbg"] * 3 + ["arch"] * 2     # 11 items: dbg and arch have fewer than 5 items each, together 5


def rubric(i):
    return {"id": f"hid-{i:02d}",
            "must": [{"id": "m1", "text": "a", "weight": 2, "first": False, "source": "x", "chunks": ["c1"]},
                     {"id": "m2", "text": "b", "weight": 2, "first": False, "source": "x", "chunks": ["c2"]},
                     {"id": "m3", "text": "c", "weight": 1, "first": False, "source": "x", "not_indexed": True}],
            "must_not": [{"id": "n1", "text": "z", "severity": "wrong"}], "any_of": []}


def item(i, n_kinds=KINDS):
    return {"id": f"hid-{i:02d}", "tier": "B", "kind": n_kinds[i], "family": f"f{i}", "domain": "kafka",
            "question": f"SECRETQ-{i} how do I fix this?"}


def answer(i, tag):
    return {"id": f"hid-{i:02d}", "text": f"SECRETANS-{i} {tag} we set a. we set b.", "pack": ["c1"]}


class Base(unittest.TestCase):
    N = len(KINDS)

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.keydir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.keydir, True)
        self.key = self.keydir / "key"
        eval2.keygen(self.key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(self.key)})
        p.start()
        self.addCleanup(p.stop)
        self.chunks = self.tmp / "chunks.jsonl"
        self.chunks.write_text("".join(json.dumps({"id": c, "text": f"chunk text {c}"}) + "\n" for c in ("c1", "c2")))

    def put(self, name, rows, hidden=True):
        p = self.tmp / (name + (".jsonl.enc" if hidden else ".jsonl"))
        ej.write_rows(p, rows)
        return p

    def inputs(self, hidden=True, sets=("A", "B")):
        d = {"rub": self.put("rub", [rubric(i) for i in range(self.N)], hidden),
             "items": self.put("items", [item(i) for i in range(self.N)], hidden)}
        for s in sets:
            d[s] = self.put(f"ans_{s}", [answer(i, s) for i in range(self.N)], hidden)
        return d

    def judge(self, args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                rc = ej.main(args)
            except SystemExit as e:
                rc = e.code
        return rc, out.getvalue(), err.getvalue()

    def run_args(self, d, sets=("A", "B"), cache=None, out=None, extra=()):
        a = ["run", "--rubrics", str(d["rub"]), "--items", str(d["items"]), "--chunks", str(self.chunks),
             "--answers", *[f"{s}={d[s]}" for s in sets], "--coverage", "--stub-calls", "--settings-name", "stub-test", "--passes", "1", "2"]
        if cache:
            a += ["--cache", str(cache)]
        if out:
            a += ["--out", str(out)]
        return a + list(extra)

    def assertNoSecret(self, text):
        for s in SECRETS:
            self.assertNotIn(s, text)


class HiddenJudge(Base):
    def test_stub_run_end_to_end_writes_only_encrypted_files_and_prints_no_text(self):
        d = self.inputs()
        cache, out = self.tmp / "cache.jsonl.enc", self.tmp / "res.jsonl.enc"
        rc, so, se = self.judge(self.run_args(d, cache=cache, out=out))
        self.assertEqual(rc, 0, se)
        self.assertNoSecret(so + se)
        self.assertIn(f"matched A: {self.N} answers", so)
        self.assertIn(f"{2 * self.N * 2} judgments, {2 * self.N * 2} called now", so)
        for line in so.splitlines():                       # progress lines: set, id, counts, timing
            if line.startswith("[judge]"):
                self.assertRegex(line, r"^\[judge\] \d+ cached  [AB] hid-\d\d p[12] valid=True")
        rows = ej.rd(out)
        self.assertEqual(len(rows), 2 * self.N * 2)
        self.assertTrue(any("SECRETQ" in r["question"] for r in rows))      # the content is in the encrypted result
        self.assertEqual(len(ej.rd(cache)), 2 * self.N * 2)
        for p in self.tmp.iterdir():                       # nothing in plain text: no marker in any file but the open chunk list
            if p == self.chunks:
                continue
            self.assertTrue(p.name.endswith(".enc"), p.name)
            self.assertNoSecret(p.read_bytes().decode("latin-1"))
        self.assertFalse([p for p in self.tmp.iterdir() if p.name.endswith(".tmp")])

    def test_resume_uses_the_encrypted_cache(self):
        d = self.inputs()
        cache = self.tmp / "cache.jsonl.enc"
        rc, so, _ = self.judge(self.run_args(d, sets=("A",), cache=cache))
        self.assertEqual(rc, 0)
        self.assertIn(f"{2 * self.N} judgments, {2 * self.N} called now", so)
        before = cache.read_bytes()
        rc, so, se = self.judge(self.run_args(d, sets=("A", "B"), cache=cache))
        self.assertEqual(rc, 0, se)
        self.assertIn(f"{2 * self.N} of {4 * self.N} judgments already in the cache", so)
        self.assertIn(f"{4 * self.N} judgments, {2 * self.N} called now", so)
        self.assertNoSecret(so + se)
        self.assertNotEqual(cache.read_bytes(), before)
        self.assertEqual(len(ej.rd(cache)), 4 * self.N)
        rc, so, _ = self.judge(self.run_args(d, sets=("A", "B"), cache=cache))      # a third run calls nothing
        self.assertIn(f"{4 * self.N} judgments, 0 called now", so)

    def test_rescore_from_the_encrypted_cache(self):
        d = self.inputs()
        cache, out = self.tmp / "cache.jsonl.enc", self.tmp / "res.jsonl.enc"
        self.judge(self.run_args(d, cache=cache))
        a = ["run", "--rubrics", str(d["rub"]), "--items", str(d["items"]), "--chunks", str(self.chunks), "--answers", f"A={d['A']}", f"B={d['B']}",
             "--coverage", "--rescore", "--settings-name", "stub-test", "--cache", str(cache), "--out", str(out)]
        rc, so, se = self.judge(a)
        self.assertEqual(rc, 0, se)
        self.assertEqual(len(ej.rd(out)), 4 * self.N)

    def test_open_and_hidden_inputs_are_never_mixed(self):
        d = self.inputs()
        plain = self.put("plain_ans", [answer(i, "A") for i in range(self.N)], hidden=False)
        for bad in ({**d, "A": plain}, {**d, "rub": self.put("prub", [rubric(i) for i in range(self.N)], hidden=False)},
                    {**d, "items": self.put("pitems", [item(i) for i in range(self.N)], hidden=False)}):
            rc, so, se = self.judge(self.run_args(bad, sets=("A",)))
            self.assertEqual(rc, 2)
            self.assertIn("never mixed", se)
            self.assertNoSecret(so + se)
        pk = self.put("ppacks", [{"question": "q", "pack": ["c1"]}], hidden=False)     # a plain packs file next to hidden inputs
        rc, _, se = self.judge(self.run_args(d, sets=("A",), extra=["--packs", f"A={pk}"]))
        self.assertEqual(rc, 2)
        self.assertIn("never mixed", se)

    def test_outputs_must_match_the_inputs(self):
        d, o = self.inputs(), self.inputs(hidden=False)
        rc, _, se = self.judge(self.run_args(d, cache=self.tmp / "cache.jsonl"))
        self.assertEqual(rc, 2)
        self.assertIn("writes only .enc", se)
        rc, _, se = self.judge(self.run_args(d, out=self.tmp / "res.jsonl"))
        self.assertEqual(rc, 2)
        rc, _, se = self.judge(self.run_args(o, out=self.tmp / "res.jsonl.enc"))
        self.assertEqual(rc, 2)
        self.assertIn("no .enc file", se)
        self.assertFalse((self.tmp / "res.jsonl").exists() or (self.tmp / "res.jsonl.enc").exists())

    def test_other_commands_refuse_enc_paths(self):
        d = self.inputs()
        for cmd in (["flags", "--results", str(d["A"]), "--out", str(self.tmp / "o.jsonl")],
                    ["fill-chunks", "--rubrics", str(d["rub"]), "--chunks", str(self.chunks), "--check"],
                    ["gen-generic", "--rubrics", str(d["rub"]), "--out", str(self.tmp / "g.jsonl")]):
            rc, so, se = self.judge(cmd)
            self.assertEqual(rc, 2, cmd[0])
            self.assertIn("not supported", se)
        self.assertFalse((self.tmp / "o.jsonl").exists() or (self.tmp / "g.jsonl").exists())

    def test_stub_calls_needs_a_stub_setting(self):
        d = self.inputs(hidden=False)
        rc, _, se = self.judge(self.run_args(d, extra=["--settings-name", "glm-think-12k"]))
        self.assertEqual(rc, 2)
        self.assertIn("starting with 'stub'", se)

    def test_dry_stub_does_not_write_the_cache(self):
        d = self.inputs()
        cache = self.tmp / "cache.jsonl.enc"
        a = self.run_args(d, cache=cache)
        a.remove("--stub-calls")
        a.append("--stub")
        rc, so, se = self.judge(a)
        self.assertEqual(rc, 0, se)
        self.assertFalse(cache.exists())

    def test_unexpected_error_is_reported_by_class_only(self):
        d = self.inputs()
        with mock.patch.object(ej, "prepare_items", side_effect=KeyError("SECRETQUOTE-in-a-key")):
            rc, so, se = self.judge(self.run_args(d))
        self.assertEqual(rc, 1)
        self.assertIn("KeyError", se)
        self.assertNoSecret(so + se)

    def test_wrong_key_is_an_error_without_values(self):
        d = self.inputs()
        other = self.keydir / "other"
        eval2.keygen(other)
        with mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(other)}):
            rc, so, se = self.judge(self.run_args(d))
        self.assertEqual(rc, 1)
        self.assertIn("integrity", se)
        self.assertNoSecret(so + se)

    def test_progress_lines_hold_no_quote_even_when_the_judge_quotes(self):
        """A judge that quotes the answer: the quote is in the encrypted cache and result, in no output line."""
        quote = "SECRETQUOTE we set a."
        it = {"set": "A", "id": "hid-00", "question": "SECRETQ-0", "answer": f"x {quote} y", "rubric": rubric(0), "pack": ["c1"]}

        def judge(system, user):
            j = {"must": [{"id": "m1", "verdict": "present", "quote": quote}, {"id": "m2", "verdict": "absent", "quote": ""},
                          {"id": "m3", "verdict": "absent", "quote": ""}],
                 "must_not": [{"id": "n1", "verdict": "absent", "quote": ""}],
                 "other_errors": [{"quote": quote, "source_quote": "chunk text c1", "why": "SECRETQUOTE why"}]}
            return j, json.dumps(j), False, {"usage": {"completion_tokens": 3}, "reasoning_chars": None, "finish_reason": "stop"}

        lines, cache = [], self.tmp / "q_cache.jsonl.enc"
        chunks = {c["id"]: c for c in ej.rd(self.chunks)}
        rows, n_new, _ = ej.run([it], [1, 2], judge, "m", chunks, 0, cache, say=lines.append, instruction="v2", coverage=True)
        self.assertEqual(n_new, 2)
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertNoSecret(line)
        self.assertIn(quote, json.dumps(ej.load_cache(cache)))
        self.assertNotIn(b"SECRETQUOTE", cache.read_bytes())


class OpenPath(Base):
    def test_open_run_unchanged_and_plain(self):
        d = self.inputs(hidden=False)
        cache, out = self.tmp / "cache.jsonl", self.tmp / "res.jsonl"
        rc, so, se = self.judge(self.run_args(d, cache=cache, out=out))
        self.assertEqual(rc, 0, se)
        self.assertEqual(len(ej.rd(out)), 4 * self.N)
        self.assertEqual(len(cache.read_text().splitlines()), 4 * self.N)        # appended line by line, as before
        self.assertIn("SECRETQ", out.read_text())

    def test_open_chain_on_stored_b2_results_is_byte_identical(self):
        w, e = ROOT / "work/m4/b2", ROOT / "eval/m4/b2"
        if not (w / "judge_results.jsonl").exists():
            self.skipTest("work/m4/b2 not present")
        empty = self.tmp / "findings_none.jsonl"
        empty.write_text("")
        out = io.StringIO()
        with redirect_stdout(out):
            rc = pr.main(["--coverage", "--results", str(w / "judge_results.jsonl"), "--rubrics", str(e / "rubrics.jsonl"),
                          "--findings", str(empty), "--title", "B2: coverage role, second fresh batch"])
        self.assertEqual(rc, 0)
        self.assertEqual(out.getvalue(), (w / "coverage_report.md").read_text())
        # rescoring the stored cache gives the stored results file, byte for byte
        rc, so, se = self.judge(["run", "--rubrics", str(e / "rubrics.jsonl"), "--items", str(e / "items.jsonl"), "--chunks", str(ROOT / "work/v3/chunks_s1.jsonl"),
                                 "--answers", f"default={w / 'ans_default.jsonl'}", f"strict={w / 'ans_strict.jsonl'}", "--coverage", "--rescore",
                                 "--judge-model", "DeepSeek-V4.1-Flash-1017-oQ4e-mtp", "--settings-name", "deepseek-nothink-12k", "--source-chars", "12000",
                                 "--instruction", "v2", "--passes", "1", "2", "--cache", str(w / "judge_cache.jsonl"), "--out", str(self.tmp / "rescored.jsonl")])
        self.assertEqual(rc, 0, se)
        self.assertEqual((self.tmp / "rescored.jsonl").read_text(), (w / "judge_results.jsonl").read_text())


class HiddenReport(Base):
    def results(self, hidden=True):
        d = self.inputs(hidden=hidden)
        ext = ".enc" if hidden else ""
        out = self.tmp / ("res.jsonl" + ext)
        rc, _, se = self.judge(self.run_args(d, out=out))
        self.assertEqual(rc, 0, se)
        return d, out

    def report(self, args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                rc = pr.main(args)
            except SystemExit as e:
                rc = e.code
        return rc, out.getvalue(), err.getvalue()

    def test_hidden_report_prints_totals_and_no_ids_or_text(self):
        d, res = self.results()
        fnd = self.put("fnd", [{"set": s, "id": f"hid-{i:02d}", "reader": r, "kind": "read"} for s in "AB" for i in range(self.N) for r in ("r1", "r2")]
                       + [{"set": s, "id": f"hid-{i:02d}", "reader": r, "kind": "wrong", "quote": "SECRETQUOTE"} for s in "AB" for i in range(4) for r in ("r1", "r2")]
                       + [{"set": "A", "id": "hid-03", "reader": "r1", "kind": "omission", "point": "zz"}])
        rc, so, se = self.report(["--coverage", "--by-kind", "--items", str(d["items"]), "--results", str(res), "--rubrics", str(d["rub"]), "--findings", str(fnd)])
        self.assertEqual(rc, 0, se)
        self.assertNoSecret(so + se)
        self.assertNotIn("hid-", so)                       # no item id anywhere
        for needle in ("| answer set | pass |", "kappa between the judge's two passes", "Paired difference between answer sets", "mean difference (SE)",
                       "unverified share", "essential-miss units per 10", "must-not present"):
            self.assertIn(needle, so)
        self.assertIn("do not map to a unit (not used): 1", so)           # the count stays, the row does not
        self.assertNotIn("| set | answer |", so)                          # the per-answer table of the two readers is left out
        # the same findings on an open report do list ids
        d2, res2 = self.results(hidden=False)
        fnd2 = self.put("fnd2", ej.rd(fnd), hidden=False)
        rc, so2, _ = self.report(["--coverage", "--results", str(res2), "--rubrics", str(d2["rub"]), "--findings", str(fnd2)])
        self.assertIn("hid-03", so2)
        self.assertIn("| set | answer |", so2)

    def test_by_kind_merges_small_kinds_and_hides_a_small_other(self):
        d, res = self.results()
        fnd = self.put("fnd", [])
        rc, so, se = self.report(["--coverage", "--by-kind", "--items", str(d["items"]), "--results", str(res), "--rubrics", str(d["rub"]), "--findings", str(fnd)])
        self.assertEqual(rc, 0, se)
        kinds = {l.split("|")[1].strip() for l in so.split("## Totals per kind")[1].splitlines() if l.startswith("| ") and not l.startswith("| kind") and "---" not in l}
        self.assertEqual(kinds, {"cfg", "other"})                  # dbg (3) and arch (2) are merged: 5 items
        # three small items only: 'other' has fewer than 5 and is not shown
        items = [item(i, ["cfg"] * 8 + ["dbg", "dbg", "arch"]) for i in range(self.N)]
        d["items"] = self.put("items2", items)
        rc, so, se = self.report(["--coverage", "--by-kind", "--items", str(d["items"]), "--results", str(res), "--rubrics", str(d["rub"]), "--findings", str(fnd)])
        kinds = {l.split("|")[1].strip() for l in so.split("## Totals per kind")[1].splitlines() if l.startswith("| ") and not l.startswith("| kind") and "---" not in l}
        self.assertEqual(kinds, {"cfg"})

    def test_by_kind_needs_items_and_no_mixing(self):
        d, res = self.results()
        fnd = self.put("fnd", [])
        rc, _, se = self.report(["--coverage", "--by-kind", "--results", str(res), "--rubrics", str(d["rub"]), "--findings", str(fnd)])
        self.assertEqual(rc, 2)
        plain = self.put("pfnd", [], hidden=False)
        rc, so, se = self.report(["--coverage", "--results", str(res), "--rubrics", str(d["rub"]), "--findings", str(plain)])
        self.assertEqual(rc, 2)
        self.assertIn("never mixed", se)
        self.assertEqual(so, "")

    def test_hidden_report_error_has_no_values(self):
        d, res = self.results()
        fnd = self.put("fnd", [{"set": "A", "id": "hid-00", "reader": "r1", "kind": "omission", "point": "SECRETQUOTE", "quote": "SECRETQ"}])
        with mock.patch.object(pr, "map_omissions", side_effect=KeyError("SECRETQUOTE")):
            rc, so, se = self.report(["--coverage", "--results", str(res), "--rubrics", str(d["rub"]), "--findings", str(fnd)])
        self.assertEqual(rc, 1)
        self.assertIn("KeyError", se)
        self.assertNoSecret(so + se)


class TagStripping(unittest.TestCase):
    def test_arrow_text_is_not_markup(self):
        t = "start <-- the old way --> end, and <b>bold</b> <a href='x'>link</a><br/>"
        self.assertEqual(ej._tokens(t), ["start", "the", "old", "way", "end", "and", "bold", "link"])
        self.assertEqual(ej._tokens("x <!-- note --> y <!DOCTYPE html> z <-- a --> w"), ["x", "y", "z", "a", "w"])
        self.assertEqual(ej._tokens("1 < 2 and 3 > 2, a<b>c"), ["1", "2", "and", "3", "2", "a", "c"])

    def test_placement_on_a_synthetic_chunk(self):
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        (root / "raw").mkdir()
        lines = ["intro line about nothing", "use the <-- old flag --> to keep the broker alive during rolling restarts", "other line"]
        (root / "raw/x.md").write_text("\n".join(lines) + "\n")
        chunks = {"cc1": {"id": "cc1", "path": "raw/x.md", "source": "raw/x.md",
                          "text": "intro line about nothing\nuse the <-- old flag --> to keep the broker alive during rolling restarts\nother line"}}
        with mock.patch.object(ej, "index_paths_of", return_value={"raw/x.md": ["cc1"]}):
            places = ej.place_ref("raw/x.md:2", chunks, str(root))
        self.assertEqual(places[0]["status"], "placed")
        self.assertEqual(places[0]["chunks"], ["cc1"])
        self.assertEqual(places[0]["overlap"]["cc1"], 1.0)
        # with the old pattern the cited text vanished from both sides between "<--" and "-->": the words "old flag" were lost
        words = ej._tokens(lines[1])
        self.assertIn("old", words)
        self.assertIn("flag", words)


if __name__ == "__main__":
    unittest.main()
