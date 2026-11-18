"""Pilot report (scripts/eval2_pilot_report.py): kappa and ICC against hand-computed values, quote overlap, old-mark
matching, and a run of the whole report on a small fabricated judge cache. Offline."""
import io
import json
import math
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_judge as ej  # noqa: E402
import eval2_pilot_report as pr  # noqa: E402


class Statistics(unittest.TestCase):
    def test_kappa_hand_computed(self):
        # 10 pairs: 4 present/present, 4 absent/absent, 1 present/absent, 1 absent/present
        pairs = [("p", "p")] * 4 + [("a", "a")] * 4 + [("p", "a"), ("a", "p")]
        k, po, pe, n, conf = pr.cohen_kappa(pairs)
        self.assertEqual(n, 10)
        self.assertAlmostEqual(po, 0.8)
        self.assertAlmostEqual(pe, 0.5)
        self.assertAlmostEqual(k, 0.6)
        self.assertEqual(conf[("p", "a")], 1)

    def test_kappa_unequal_marginals_and_degenerate(self):
        # 3 categories: (p,p)x3, (a,a)x5, (p,a)x1, (c,c)x1: po = 9/10; marginals pass1 p4 a5 c1, pass2 p3 a6 c1
        pairs = [("p", "p")] * 3 + [("a", "a")] * 5 + [("p", "a"), ("c", "c")]
        k, po, pe, *_ = pr.cohen_kappa(pairs)
        self.assertAlmostEqual(pe, 0.4 * 0.3 + 0.5 * 0.6 + 0.1 * 0.1)   # 0.43
        self.assertAlmostEqual(k, (0.9 - 0.43) / (1 - 0.43))
        self.assertTrue(math.isnan(pr.cohen_kappa([("a", "a")] * 5)[0]))

    def test_icc_2_1_hand_computed(self):
        # (1,2) (2,2) (3,4) (4,4): SSR 8.5, SSC 0.5, SSE 0.5 -> MSR 17/6, MSC 0.5, MSE 1/6;
        # ICC = (MSR - MSE) / (MSR + MSE + 2 (MSC - MSE) / 4) = (8/3) / (19/6) = 16/19
        self.assertAlmostEqual(pr.icc_2_1([(1, 2), (2, 2), (3, 4), (4, 4)]), 16 / 19)
        self.assertAlmostEqual(pr.icc_2_1([(1, 1), (2, 2), (3, 3)]), 1.0)
        self.assertTrue(math.isnan(pr.icc_2_1([(1, 1)])))

    def test_mde_formula(self):
        self.assertAlmostEqual(pr.mde(0.4, 100), 2.80 * 0.4 / 10)

    def test_quote_overlap(self):
        self.assertTrue(pr.quotes_overlap("raise  max.poll.interval.ms", "You should raise max.poll.interval.ms to cover it"))
        self.assertTrue(pr.quotes_overlap("the quick brown fox jumps over the lazy dog", "a quick brown fox jumps over the lazy cat"))
        self.assertFalse(pr.quotes_overlap("completely different text", "nothing alike here at all"))


RUB = {"id": "real-1", "question": "Why does X fail?",
       "must": [{"id": "m1", "text": "Names a", "weight": 2, "first": False},
                {"id": "m2", "text": "Names b", "weight": 1, "first": False}],
       "must_not": [{"id": "n1", "text": "Says restart", "severity": "wrong"}]}
RUB2 = dict(RUB, id="real-2", question="Why does Y fail?")


def reply(m1="absent", m2="absent", n1="absent", other=None, quote_a="use a", quote_b="use b", quote_n="restart it"):
    return {"must": [{"id": "m1", "verdict": m1, "quote": quote_a if m1 != "absent" else ""},
                     {"id": "m2", "verdict": m2, "quote": quote_b if m2 != "absent" else ""}],
            "must_not": [{"id": "n1", "verdict": n1, "quote": quote_n if n1 != "absent" else ""}],
            "other_errors": other or []}


class Fabricated(unittest.TestCase):
    """Judge replies scripted per (set, question, pass); old marks fabricated in a temp directory."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        qs = {"real-1": RUB["question"], "real-2": RUB2["question"]}
        ans = {("real_default", "real-1"): "We use a. Then restart it. Also use b.",
               ("real_default", "real-2"): "We use a and use b.",
               ("real_v2_own_terms", "real-1"): "We use b only.",
               ("real_v2_own_terms", "real-2"): "We use a, nothing else.",
               ("generic", "real-1"): "Check the logs.", ("generic", "real-2"): "Check the logs."}
        script = {  # (set, id) -> (pass1 reply, pass2 reply)
            ("real_default", "real-1"): (reply(m1="present", m2="present", n1="present"), reply(m1="present", m2="present", n1="present")),
            ("real_default", "real-2"): (reply(m1="present", m2="present"), reply(m1="present", m2="absent")),
            ("real_v2_own_terms", "real-1"): (reply(m2="present", quote_b="use b"), reply(m2="present", quote_b="use b")),
            ("real_v2_own_terms", "real-2"): (reply(m1="present"), reply(m1="present", m2="present", quote_b="invented quote")),
            ("generic", "real-1"): (reply(m2="present", quote_b="Check the logs"), reply()),
            ("generic", "real-2"): (reply(), reply()),
        }
        items = [{"set": s, "id": i, "question": qs[i], "answer": t, "rubric": RUB if i == "real-1" else RUB2}
                 for (s, i), t in ans.items()]
        self.items = items

        def judge_for(it, p):
            return script[(it["set"], it["id"])][p - 1]
        rows = []
        for it in items:
            for p in (1, 2):
                rows.append(ej.result_row(it, p, {"key": f"{it['set']}{it['id']}{p}", "j": judge_for(it, p), "seconds": 1}))
        self.rows = rows
        self.rubrics = [RUB, RUB2]
        # reader labels: default real-1 has a wrong item (the restart advice); v2 own real-2 has one that the judge misses
        self.labels = [{"item": 1, "question_set": "real", "question": 1, "answer_set": "default", "quote": "Then restart it.", "verdict": "confirmed"},
                       {"item": 2, "question_set": "real", "question": 2, "answer_set": "v2_own_terms", "quote": "nothing else", "verdict": "partly"}]
        # old marks: base side = default, tuned side = v2 own; pass 1 and pass 2 files
        old = self.tmp / "old"
        old.mkdir()
        for p, marks in ((1, [(5, 3), (4, 2)]), (2, [(5, 2), (2, 3)])):
            with open(old / f"results_h2h_fab_p{p}.jsonl", "w") as f:
                for (i, q), (mb, mt) in zip((("real-1", RUB["question"]), ("real-2", RUB2["question"])), marks):
                    f.write(json.dumps({"question": q, "base": ans[("real_default", i)], "tuned": ans[("real_v2_own_terms", i)],
                                        "score_base": mb, "score_tuned": mt}) + "\n")
        self.old = str(old)

    def test_scores_in_rows(self):
        r = {(x["set"], x["id"], x["pass"]): x for x in self.rows}
        d = r[("real_default", "real-1", 1)]
        self.assertAlmostEqual(d["P"], 1.0)
        self.assertEqual(d["W"], 1)
        self.assertAlmostEqual(d["S"], 0.75)
        bad = r[("real_v2_own_terms", "real-2", 2)]            # "invented quote" is not in the answer: counted, turned to absent
        self.assertEqual(bad["quote_failed"], 1)
        self.assertEqual(bad["must"]["m2"]["verdict"], "absent")

    def test_report_prints_four_criteria_and_extras(self):
        text, nums = pr.build_report(self.rows, self.rubrics, self.labels, self.old,
                                     omissions=[("real_v2_own_terms", "real-1"), ("real_v2_own_terms", "real-2")])
        for n in (1, 2, 3, 4):
            self.assertIn(f"\n{n}. **", text)
        for n in (1, 2, 3, 4):
            line = next(l for l in text.splitlines() if l.startswith(f"{n}. **"))
            self.assertTrue(line.endswith("**PASS**") or line.endswith("**FAIL**"), line)
        self.assertIn("Go or no-go", text)
        self.assertIn("Generic-answer probe", text)
        self.assertIn("real-1 m2", text)                        # the generic answer satisfied m2 of real-1 in pass 1
        self.assertIn("labelled wrong items", text)
        self.assertIn("Points present in every answer or in none", text)
        # criterion 4: claims = verdicts present in the 8 non-generic judgments; one failed quote
        self.assertGreater(nums["quote_fail_share"], 0.05)
        # criterion 3: real_v2_own_terms real-1 misses m1 (weight 2) in both passes; real-2 has m1 present: 2 of 4
        self.assertAlmostEqual(nums["omission_share"], 0.5)
        self.assertEqual(nums["matched_old"], 4)
        self.assertFalse(nums["go"])

    def test_old_marks_matching_and_left_out_count(self):
        answers = [(r["set"], r["id"], r["question"], r["answer"]) for r in self.rows if r["pass"] == 1]
        got, used, missing = pr.old_marks(self.old, answers)
        self.assertEqual(got[("real_default", "real-1")][:2], (5, 5))
        self.assertEqual(got[("real_v2_own_terms", "real-2")][:2], (2, 3))
        self.assertEqual(sorted(missing), [("generic", "real-1"), ("generic", "real-2")])
        self.assertEqual(used["h2h_fab"], 4)

    def test_main_reads_files(self):
        res, rub, lab = self.tmp / "res.jsonl", self.tmp / "rub.jsonl", self.tmp / "lab.jsonl"
        res.write_text("".join(json.dumps(x) + "\n" for x in self.rows))
        rub.write_text("".join(json.dumps(x) + "\n" for x in self.rubrics))
        lab.write_text("".join(json.dumps(x) + "\n" for x in self.labels))
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = pr.main(["--results", str(res), "--rubrics", str(rub), "--labels", str(lab), "--old-dir", self.old])
        self.assertEqual(rc, 0)
        self.assertIn("kappa", buf.getvalue())
        self.assertIn("ICC(2,1)", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
