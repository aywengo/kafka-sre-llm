"""E2 fix W1: hidden-mode progress lines of the answer step, the revise step and the judge carry no per-item result
(review F1, F2), their totals are printed once at the end, and the hidden report shows a kind only when it and the
remainder hold at least 5 items. Offline: synthetic rows, a temporary key in a temporary folder, stubs; nothing under
eval/hidden/."""
import contextlib
import io
import re
import sys
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import test_eval2 as te  # noqa: E402
import test_eval2_hidden_judge as thj  # noqa: E402
import test_revise as trv  # noqa: E402

FORBIDDEN = re.compile(r"\bS=|flagged=|quotes_failed=|candidates=|tokens=|\br2=")
TOTALS = re.compile(r"\btotals over\b")


def lines_with_ids(text):
    return [l for l in text.splitlines() if re.search(r"id=|hid-\d\d|\bi00\d\b|\bi\d\b", l)]


class HiddenAnswer(te.Base):
    def test_answer_lines_have_no_tokens_and_the_totals_come_once(self):
        items = [te.item(i, tier="B") for i in range(1, 4)]
        f, out = self.items_file(items, "h.jsonl.enc"), self.tmp / "a.jsonl.enc"
        rc, text = self.run_answer(f, out)
        self.assertEqual(rc, 0, text)
        prog = [l for l in text.splitlines() if l.startswith("[answer] ") and "id=" in l]
        self.assertEqual(len(prog), 3)
        for l in prog:
            self.assertIsNone(FORBIDDEN.search(l), l)
            self.assertRegex(l, r"^\[answer\] \d/3 id=i00\d search=[\d.]+s retrieval=[\d.]+s answer=[\d.]+s total=[\d.]+s ok$")
        self.assertEqual(len(TOTALS.findall(text)), 1)
        self.assertIn("totals over 3 valid rows: tokens 33, truncated 0", text)

    def test_open_answer_lines_are_unchanged(self):
        f, out = self.items_file([te.item(1)]), self.tmp / "a.jsonl"
        rc, text = self.run_answer(f, out)
        line = [l for l in text.splitlines() if "id=i001" in l][0]
        self.assertRegex(line, r"^\[answer\] 1/1 id=i001 search=[\d.]+s retrieval=[\d.]+s answer=[\d.]+s total=[\d.]+s tokens=11 ok$")
        self.assertNotIn("totals over", text)


class HiddenRevise(trv.TestEval2Replay):
    def setUp(self):
        super().setUp()
        eval2 = self.eval2
        eval2.keygen(self.tmp / "key")
        p = mock.patch.dict("os.environ", {"EVAL_HIDDEN_KEY": str(self.tmp / "key")})
        p.start()
        self.addCleanup(p.stop)
        eval2.write_jsonl(self.tmp / "items.jsonl.enc", eval2.read_jsonl(self.tmp / "items.jsonl"))
        eval2.write_jsonl(self.tmp / "ans.jsonl.enc", eval2.read_jsonl(self.tmp / "ans.jsonl"))

    def run_hidden(self):
        a = self.eval2.build_parser().parse_args(
            ["answer", "--items", str(self.tmp / "items.jsonl.enc"), "--config", "rev", "--revise-from",
             str(self.tmp / "ans.jsonl.enc"), "--catalogue", str(self.dir), "--backend", "stub"])
        buf = io.StringIO()
        with mock.patch.object(self.eval2, "make_generator", return_value=None):
            rc = self.eval2.cmd_answer(a, out=buf)
        return rc, buf.getvalue()

    def test_hidden_revise_lines_carry_no_flagged_or_r2_and_totals_come_once(self):
        rc, out = self.run_hidden()
        self.assertEqual(rc, 0, out)
        prog = [l for l in out.splitlines() if l.startswith("[revise] ") and "id=" in l]
        self.assertEqual(len(prog), 2)
        for l in prog:
            self.assertIsNone(FORBIDDEN.search(l), l)
            self.assertRegex(l, r"^\[revise\] \d/2 id=i\d total=[\d.]+s ok$")
        self.assertEqual(len(TOTALS.findall(out)), 1)
        self.assertIn("totals over 2 valid rows: flagged 1, r2 off 2", out)

    def test_open_revise_line_is_unchanged(self):
        rc, out = self.run_replay()
        self.assertIn("[revise] 1/2 id=i1 flagged=1 r2=off total=", out)
        self.assertNotIn("totals over", out)


class HiddenJudgeLines(thj.Base):
    def test_hidden_judge_lines_have_no_per_item_result_and_totals_come_once(self):
        d = self.inputs()
        rc, so, se = self.judge(self.run_args(d, cache=self.tmp / "c.jsonl.enc", out=self.tmp / "r.jsonl.enc"))
        self.assertEqual(rc, 0, se)
        for l in (so + se).splitlines():
            self.assertIsNone(FORBIDDEN.search(l), l)
        self.assertEqual(len(TOTALS.findall(so)), 1)
        self.assertEqual(len([l for l in so.splitlines() if re.match(r"\[judge\] \d+ cached ", l)]), 2 * self.N * 2)

    def test_open_judge_lines_are_unchanged(self):
        d = self.inputs(hidden=False)
        rc, so, se = self.judge(self.run_args(d, cache=self.tmp / "c.jsonl", out=self.tmp / "r.jsonl"))
        self.assertEqual(rc, 0, se)
        line = [l for l in so.splitlines() if l.startswith("[judge] 1 cached")][0]
        self.assertRegex(line, r"valid=True quotes_failed=\d+/\d+ S=[\d.]+ candidates=\d+ [\d.]+s tokens=")
        self.assertNotIn("totals over", so)


class KindRule(unittest.TestCase):
    def test_groups(self):
        g = eval2.hidden_kind_groups
        self.assertEqual(g({"a": 6, "b": 5}), ({"a": 6, "b": 5}, []))                    # remainder empty
        self.assertEqual(g({"a": 6, "b": 3, "c": 2}), ({"a": 6}, ["b", "c"]))            # remainder 5
        self.assertEqual(g({"a": 8, "b": 2}), ({}, ["a", "b"]))                          # remainder 2: merge a in; nothing is shown
        self.assertEqual(g({"a": 9, "b": 6, "c": 2}), ({"a": 9}, ["b", "c"]))            # b merged: remainder 8
        self.assertEqual(g({"a": 9, "b": 5, "c": 4, "d": 1}), ({"a": 9, "b": 5}, ["c", "d"]))
        self.assertEqual(g({"a": 4, "b": 4}), ({}, ["a", "b"]))
        for counts in ({"a": 7, "b": 4, "c": 1}, {"a": 5, "b": 5, "c": 1, "d": 2}, {"a": 12, "b": 3}):
            shown, other = g(counts)
            rest = sum(counts.values()) - sum(shown.values())
            self.assertTrue(rest == 0 or rest >= 5, counts)
            self.assertTrue(all(n >= 5 for n in shown.values()))

    def test_hidden_report_merges_small_kinds_and_open_report_does_not(self):
        import json, shutil, tempfile
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        key = tmp / "key"
        eval2.keygen(key)
        kinds = ["cfg"] * 6 + ["dbg"] * 3 + ["arch"] * 2
        items = [{"id": f"x{i:02d}", "tier": "A", "kind": k, "family": f"f{i}", "domain": "d", "question": "q"} for i, k in enumerate(kinds)]
        scores = [{"id": it["id"], "all_pass": True, "checks_passed": 1, "checks_total": 1} for it in items]
        with mock.patch.dict("os.environ", {"EVAL_HIDDEN_KEY": str(key)}):
            texts = {}
            for ext in ("", ".enc"):
                ip, sp = tmp / f"i.jsonl{ext}", tmp / f"s.jsonl{ext}"
                eval2.write_jsonl(ip, items)
                eval2.write_jsonl(sp, scores)
                buf = io.StringIO()
                eval2.items_breakdown(str(ip), scores, bool(ext), lambda s="": print(s, file=buf))
                texts[ext] = buf.getvalue()
        self.assertIn("kind arch: scored 2", texts[""])
        self.assertIn("kind dbg: scored 3", texts[""])
        self.assertIn("kind cfg: scored 6", texts[".enc"])
        self.assertIn("kind other: scored 5", texts[".enc"])
        self.assertNotIn("kind dbg", texts[".enc"])
        self.assertNotIn("kind arch", texts[".enc"])


if __name__ == "__main__":
    unittest.main()
