"""Report of the judge-settings comparison (scripts/eval2_judge_settings_report.py): subset, statistics, switch check. Offline."""
import io
import contextlib
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_judge_settings_report as rep  # noqa: E402


def row(s, i, secs=100.0, S=0.5, must=None, must_not=None, other=None, essential=False, valid=True, p=1, toks=500, reas=None):
    r = {"set": s, "id": i, "pass": p, "valid_reply": valid, "seconds": secs, "completion_tokens": toks, "reasoning_chars": reas}
    if valid:
        r.update(S=S, must={k: {"verdict": v} for k, v in (must or {"m1": "absent"}).items()},
                 must_not={k: {"verdict": v} for k, v in (must_not or {"n1": "absent"}).items()},
                 other_errors=other or [], essential_miss=essential, quote_claims=2, quote_failed=1)
    return r


def label(qs, ans, q, quote="the wrong statement here"):
    return {"item": 1, "question_set": qs, "question": q, "answer_set": ans, "quote": quote, "verdict": "confirmed"}


class Subset(unittest.TestCase):
    def test_sixteen_answers_as_specified(self):
        self.assertEqual(len(rep.SUBSET), 16)
        self.assertEqual(len(set(rep.SUBSET)), 16)
        self.assertIn(("holdout_strict", "holdout-3"), rep.SUBSET)
        self.assertIn(("real_v2_base_excerpts", "real-2"), rep.SUBSET)


class Switch(unittest.TestCase):
    def test_reasoning_field_means_thinking_on(self):
        self.assertTrue(rep.thinking_still_on({"reasoning_chars": 120, "reply": "{}", "usage": {"completion_tokens": 5}}))

    def test_tokens_far_above_reply_size_means_thinking_on(self):
        self.assertTrue(rep.thinking_still_on({"reasoning_chars": None, "reply": "x" * 1000, "usage": {"completion_tokens": 9000}}))
        self.assertFalse(rep.thinking_still_on({"reasoning_chars": 0, "reply": "x" * 1000, "usage": {"completion_tokens": 350}}))

    def test_think_block_and_unknown(self):
        self.assertTrue(rep.thinking_still_on({"reply": "<think>x</think>{}"}))
        self.assertIsNone(rep.thinking_still_on({"reply": "{}"}))

    def test_check_switch_exit_codes_and_default_setting(self):
        tmp = Path(tempfile.mkdtemp())
        c = tmp / "c.jsonl"
        c.write_text(json.dumps({"setting": "n", "reply": "x" * 100, "usage": {"completion_tokens": 40}}) + "\n"
                     + json.dumps({"reply": "x", "usage": {"completion_tokens": 9000}}) + "\n")   # no label: glm-think-12k
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(rep.check_switch(str(c), "n"), 0)
            self.assertEqual(rep.check_switch(str(c), "glm-think-12k"), 1)
            self.assertEqual(rep.check_switch(str(c), "zz"), 2)


class Statistics(unittest.TestCase):
    def test_pearson(self):
        self.assertAlmostEqual(rep.pearson([1, 2, 3, 4], [2, 4, 6, 8]), 1.0)
        self.assertAlmostEqual(rep.pearson([1, 2, 3, 4], [4, 3, 2, 1]), -1.0)
        self.assertTrue(math.isnan(rep.pearson([1, 1, 1], [1, 2, 3])))
        self.assertTrue(math.isnan(rep.pearson([1, 2], [1, 2])))

    def test_hours(self):
        self.assertAlmostEqual(rep.hours(220, 600), 220 * 600 / 3 / 3600)

    def test_report_numbers(self):
        a, b = {}, {}
        res_a = [row(s, i, secs=100 + n, S=0.5, must={"m1": "present"}) for n, (s, i) in enumerate(rep.SUBSET)]
        res_b = [row(s, i, secs=50, S=0.5 if n % 2 else 0.0, must={"m1": "present" if n % 2 else "absent"}) for n, (s, i) in enumerate(rep.SUBSET)]
        res_b.append(row("real_default", "real-1", p=2))                       # pass 2 and answers outside the subset are ignored
        res_b.append(row("real_default", "real-2"))
        # reader: label on real_default real-1 caught by a must-not in a, missed in b; omission real_strict real-3 marked only in a
        res_a = [dict(r, must_not={"n1": {"verdict": "present"}}) if (r["set"], r["id"]) == ("real_default", "real-1") else r for r in res_a]
        res_a = [dict(r, essential_miss=True) if (r["set"], r["id"]) == ("real_strict", "real-3") else r for r in res_a]
        labels = [label("real", "default", 1), label("real", "default", 2)]    # real-2 is not in the subset
        text = rep.build_report({"a": res_a, "b": res_b}, labels, noeffect=["c"], omissions=[("real_strict", "real-3"), ("real_v2_own_terms", "real-1")])
        self.assertIn("| a | 16 of 16 | 16 | 108 | 115 |", text)
        self.assertIn("| b | 16 of 16 | 16 | 50 | 50 |", text)
        self.assertIn("16 of 32", text)
        self.assertIn("| a | 1 of 1 | 0 | 1 of 1 | 0 |", text)
        self.assertIn("| b | 0 of 1 | 0 | 0 of 1 | 0 |", text)
        self.assertIn("| c | switch had no effect", text)
        self.assertIn("Switch had no effect: c", text)
        self.assertIn("| a vs b | 16 | 32 | 71.9 % |", text)   # 23 of 32: m1 equal on 8 of 16 answers, n1 on 15 (a marks one present)
        self.assertIn("| a vs b | 16 | 32 | 71.9 % | 0.45 | n/a |", text)    # S of a is constant, so no correlation
        self.assertIn(f"{rep.hours(220, 108):.1f} h", text)
        self.assertNotIn("winner is", text)

    def test_invalid_reply_is_not_scored_and_not_judged_counts(self):
        res = [row(s, i, valid=(n != 0)) for n, (s, i) in enumerate(rep.SUBSET)]
        text = rep.build_report({"a": res}, [label("real", "default", 1)], omissions=[])
        self.assertIn("| a | 16 of 16 | 15 |", text)
        self.assertIn("| a | 0 of 1 | 1 |", text)

    def test_main_report_from_files(self):
        tmp = Path(tempfile.mkdtemp())
        (tmp / "a.jsonl").write_text("".join(json.dumps(row(s, i)) + "\n" for s, i in rep.SUBSET))
        lab = tmp / "l.jsonl"
        lab.write_text(json.dumps(label("real", "default", 1)) + "\n")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = rep.main(["report", "--results", f"a={tmp / 'a.jsonl'}", f"missing={tmp / 'zz.jsonl'}", "--labels", str(lab)])
        self.assertEqual(rc, 0)
        self.assertIn("# S5a: judge settings", buf.getvalue())
        self.assertIn("| missing | 0 of 16 |", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
