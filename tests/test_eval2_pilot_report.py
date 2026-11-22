"""Pilot report (scripts/eval2_pilot_report.py): kappa and ICC against hand-computed values, quote overlap, old-mark
matching, and a run of the whole report on a small fabricated judge cache. Offline."""
import io
import json
import math
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
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

    def test_same_statement_option_changes_only_the_caught_rule(self):
        labels = self.labels + [{"item": 3, "question_set": "real", "question": 1, "answer_set": "default",
                                 "quote": "Also use b.", "verdict": "confirmed"}]    # the judge's flag on this answer is about "restart it"
        default, _ = pr.build_report(self.rows, self.rubrics, labels, self.old, omissions=[])
        same, _ = pr.build_report(self.rows, self.rubrics, labels, self.old, omissions=[], same_statement=True)
        caught = lambda t: next(l for l in t.splitlines() if l.startswith("Caught in"))
        self.assertIn("Caught in at least one pass: 2 of 3", caught(default))      # items 1 and 3 by the must-not, on the same answer
        self.assertIn("Caught in at least one pass: 1 of 3", caught(same))         # only item 1 concerns the flagged statement
        self.assertIn("the same statement", same)
        # everything outside the labelled-items paragraph is identical
        cut = lambda t: t.split("### The reader's 30 labelled")[0]
        self.assertEqual(cut(default), cut(same))

    def test_first_pilot_numbers_on_the_stored_cache(self):
        res = ROOT / "work/m4/pilot/judge_results_fair.jsonl"
        if not (res.exists() and (ROOT / "work/v3/judge").exists()):
            self.skipTest("rescored first-pilot results not on this machine (generated, git-ignored)")
        rows, rub, lab = pr.rd(res), pr.rd(ROOT / "eval/m4/pilot/rubrics.jsonl"), pr.rd(ROOT / "runs/m4/reports/wp2a_labels_checked.jsonl")
        caught = lambda t: next(l for l in t.splitlines() if l.startswith("Caught in"))
        old, _ = pr.build_report(rows, rub, lab, str(ROOT / "work/v3/judge"))
        same, _ = pr.build_report(rows, rub, lab, str(ROOT / "work/v3/judge"), same_statement=True)
        self.assertIn("25 of 30 labelled items judged in both passes; in both passes: 21", caught(old))
        # the analysis found 14 in at least one pass and 10 in both; this overlap rule gives 14 and 11
        self.assertIn("Caught in at least one pass: 14 of 30", caught(same))
        self.assertIn("in both passes: 11", caught(same))

# ---------------------------------------------------------------- version 2

R2 = {"id": "q-1", "question": "Why?",
      "must": [{"id": "m1", "text": "a", "weight": 2, "first": False, "source": "raw/a/one.md:3"},
               {"id": "m2", "text": "b", "weight": 2, "first": False, "source": "raw/b/two.md:3"},
               {"id": "m3", "text": "c", "weight": 1, "first": False}],
      "must_not": [{"id": "n1", "text": "Says restart", "severity": "wrong"}]}


def row2(set_name, rid, pass_no, verdicts, n1=None, other=None, pack=("c1",), paths=("raw/a/one.md",), answer=None):
    ans = answer or "We set a. We set b. We use c. We restart it. Odd claim one. Odd claim two."
    it = {"set": set_name, "id": rid, "question": "Why?", "answer": ans, "rubric": R2, "pack": list(pack) if pack else None,
          "excerpt_paths": list(paths), "source": "", "excerpts": "Odd claim one is false. Odd claim two is false."}
    j = {"must": [{"id": k, "verdict": v, "quote": {"m1": "We set a.", "m2": "We set b.", "m3": "We use c."}[k] if v != "absent" else ""}
                  for k, v in verdicts.items()],
         "must_not": [{"id": "n1", "verdict": "present" if n1 else "absent", "quote": n1 or ""}],
         "other_errors": [{"quote": q, "source_quote": "Odd claim one is false.", "why": "w"} for q in (other or [])]}
    return ej.result_row(it, pass_no, {"key": f"{set_name}{rid}{pass_no}", "j": j, "instruction": "v2"})


class VarianceShare(unittest.TestCase):
    def test_hand_computed_example(self):
        # S passes per item. A: (.8,.6) (.4,.4) (.2,0) (1,.8); B: (.5,.5) (.3,.1) (0,0) (.6,.4)
        # pass means A .7 .4 .1 .9, B .5 .2 0 .5, d = .2 .2 .1 .4, mean .225, sum of squares .0475, Var(d) = .0475 / 3
        # within-answer variances (x1 - x2)^2 / 2: A .02 0 .02 .02 -> .015; B 0 .02 0 .02 -> .01
        # judge part = (.015 + .01) / 2 = .0125; share = .0125 / (.0475 / 3) = 15 / 19
        A = [(.8, .6), (.4, .4), (.2, 0), (1, .8)]
        B = [(.5, .5), (.3, .1), (0, 0), (.6, .4)]
        rows = []
        for name, data in (("A", A), ("B", B)):
            for i, (p1, p2) in enumerate(data):
                for p, v in ((1, p1), (2, p2)):
                    rows.append({"set": name, "id": f"i{i}", "pass": p, "valid_reply": True, "S": v, "P": v})
        share, var_d, var_j, n = pr.judge_variance_share(rows, "A", "B")
        self.assertEqual(n, 4)
        self.assertAlmostEqual(var_d, 0.0475 / 3)
        self.assertAlmostEqual(var_j, 0.0125)
        self.assertAlmostEqual(share, 15 / 19)
        self.assertIsNone(pr.judge_variance_share(rows[:4], "A", "B"))      # fewer than 3 items

    def test_no_judge_noise_gives_zero(self):
        rows = [{"set": s, "id": f"i{i}", "pass": p, "valid_reply": True, "S": v + (0.3 if s == "A" else 0), "P": 0}
                for s in "AB" for i, v in enumerate((0.1, 0.5, 0.9)) for p in (1, 2)]
        self.assertAlmostEqual(pr.judge_variance_share(rows, "A", "B")[0], 0.0)


class ReviewerCriteria(unittest.TestCase):
    def setUp(self):
        # configuration "d": answer q-1 and q-2; "s": answer q-1. Judge flags: pass 1 on d/q-1: restart (must_not) and odd claim one;
        # pass 2 on d/q-1: restart only. d/q-2 and s/q-1: no flags.
        self.rows = []
        for rid in ("q-1", "q-2"):
            for p in (1, 2):
                self.rows.append(row2("d", rid, p, {"m1": "present", "m2": "absent", "m3": "present"},
                                      n1="We restart it." if rid == "q-1" else None,
                                      other=["Odd claim one."] if (rid == "q-1" and p == 1) else None))
        for p in (1, 2):
            self.rows.append(row2("s", "q-1", p, {"m1": "present", "m2": "present", "m3": "present"}))
        self.review = ej.review_rows(self.rows)

    def test_review_rows(self):
        self.assertEqual(len(self.review), 2)
        self.assertEqual([x["passes"] for x in self.review], [[1, 2], [1]])

    def fill(self, confirmed):
        for x, c in zip(self.review, confirmed):
            x["confirmed"] = c
        return self.review

    def test_precision_and_recall_same_statement_only(self):
        findings = [{"set": "d", "id": "q-1", "kind": "wrong", "quote": "We restart it."},          # caught in both passes
                    {"set": "d", "id": "q-1", "kind": "wrong", "quote": "Odd claim one."},          # caught in pass 1 only
                    {"set": "d", "id": "q-2", "kind": "wrong", "quote": "We use c."},               # a flag elsewhere on the answer: none on q-2
                    {"set": "s", "id": "q-1", "kind": "wrong", "quote": "We restart it."},          # flags exist on d/q-1, not on s/q-1
                    {"set": "d", "id": "q-1", "kind": "omission", "point": "m2"}]                   # not a semantic error
        res, undecided = pr.precision_recall(self.fill(["yes", "no"]), findings)
        self.assertEqual(undecided, 0)
        self.assertEqual((res[1]["flags"], res[1]["confirmed"], res[1]["findings"], res[1]["caught"]), (2, 1, 4, 2))
        self.assertAlmostEqual(res[1]["precision"], 0.5)
        self.assertAlmostEqual(res[1]["recall"], 0.5)
        self.assertEqual((res[2]["flags"], res[2]["caught"]), (1, 1))
        self.assertAlmostEqual(res[2]["precision"], 1.0)
        self.assertAlmostEqual(res[2]["recall"], 0.25)

    def test_a_finding_names_the_flag(self):
        findings = [{"set": "d", "id": "q-1", "kind": "wrong", "quote": "totally different wording", "flag": ["d:q-1:2"]}]
        res, _ = pr.precision_recall(self.fill(["yes", "yes"]), findings)
        self.assertEqual((res[1]["caught"], res[2]["caught"]), (1, 0))     # flag 2 exists in pass 1 only

    def test_omission_shares(self):
        # judge: m2 is a supported essential miss on d/q-1 and d/q-2 (pass 1 and 2 each): 4 flags. s/q-1 has none.
        findings = [{"set": "d", "id": "q-1", "kind": "omission", "point": "m2"},
                    {"set": "d", "id": "q-2", "kind": "omission", "point": "m1"},       # not flagged by the judge (m1 present)
                    {"set": "d", "id": "q-2", "kind": "omission", "point": "m3", "supported": False}]    # unsupported: left out
        rows = [dict(r, essential_points=[{"id": "m2", "supported": True, "rule": "path"}]) if r["set"] == "d" else r for r in self.rows]
        f1, t1, f2, t2 = pr.omission_shares(rows, findings)
        self.assertEqual((f1, t1), (0.5, 4))        # 2 omissions x 2 passes; flagged: q-1 in both passes
        self.assertEqual((f2, t2), (0.5, 4))        # 4 judge flags; confirmed: the two on q-1

    def test_report_waits_for_the_reviewer_and_decides_with_the_files(self):
        text, nums = pr.build_report_v2(self.rows)
        self.assertEqual(nums["criteria"]["4 flag precision and recall"], "WAITING")
        self.assertEqual(nums["criteria"]["5 supported omissions"], "WAITING")
        self.assertFalse(nums["go"])
        self.assertIn("**Go or no-go: NO-GO**", text)
        self.assertIn("wait for the reviewer's files", text)
        self.assertIn("Candidate errors", text)
        findings = [{"set": "d", "id": "q-1", "kind": "wrong", "quote": "We restart it."}]
        text2, nums2 = pr.build_report_v2(self.rows, self.fill(["yes", "no"]), findings)
        pr_by_pass = nums2["precision_recall"]
        self.assertAlmostEqual(pr_by_pass[1]["precision"], 0.5)
        self.assertAlmostEqual(pr_by_pass[1]["recall"], 1.0)
        self.assertEqual(nums2["criteria"]["4 flag precision and recall"], "PASS")
        self.assertIn("order of paired answers", text2.lower())
        self.assertEqual(nums2["criteria"]["5 supported omissions"], "N/A")     # no omission findings and no supported flags: no data
        _, nums3 = pr.build_report_v2(self.rows, self.fill(["", "no"]), findings)
        self.assertEqual(nums3["criteria"]["4 flag precision and recall"], "WAITING")     # a flag without a decision


class ReportV2Measures(unittest.TestCase):
    def test_measures_rule_and_reference_arm_apart(self):
        rows = [row2("d", "q-1", p, {"m1": "absent", "m2": "absent", "m3": "present"}) for p in (1, 2)]
        rows += [row2("d", "q-2", p, {"m1": "present", "m2": "absent", "m3": "present"}, pack=None, paths=()) for p in (1, 2)]
        ref = [dict(row2("d_ref", "q-1", 1, {"m1": "present", "m2": "present", "m3": "present"}), reference_given=True)]
        text, nums = pr.build_report_v2(rows + ref)
        self.assertIn("Rule used for the essential-miss points: no_pack 2, path 4", text)
        self.assertIn("Arm with the reference answer", text)
        self.assertNotIn("d_ref", text.split("## The five criteria")[1])        # not in the criteria
        m = ej.measures([r for r in rows if r["valid_reply"]])["d"][1]
        self.assertEqual((m["n"], m["essential_miss"], m["supported_essential_miss"]), (2, 2, 1))     # m1 (path in excerpts) on q-1; m2 on both
        self.assertAlmostEqual(m["supported_essential_miss_per10"], 5.0)

    def test_main_v2_reads_files(self):
        tmp = Path(tempfile.mkdtemp())
        res = tmp / "res.jsonl"
        res.write_text("".join(json.dumps(r) + "\n" for r in (row2("d", "q-1", p, {"m1": "present", "m2": "absent", "m3": "present"}) for p in (1, 2))))
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = pr.main(["--v2", "--results", str(res)])
        self.assertEqual(rc, 0)
        self.assertIn("## The five criteria", buf.getvalue())
        self.assertIn("Go or no-go", buf.getvalue())


PROBE_RUBRIC = {"id": "r-1",
                "must": [{"id": "m1", "text": "a", "weight": 2, "first": True, "source": "raw/src/a.md:1"},
                         {"id": "m2", "text": "b", "weight": 2, "first": False, "source": "raw/src/b.md:1"},
                         {"id": "m3", "text": "c", "weight": 2, "first": False, "source": "inferred"},
                         {"id": "m4", "text": "d", "weight": 1, "first": False, "source": "raw_catalogue/t/T.java:1"}],
                "must_not": [{"id": "n1", "text": "x", "severity": "wrong"}, {"id": "n2", "text": "y", "severity": "harmful"}],
                "any_of": [["m2", "m3"]]}


def prow(set_, rid, must, must_not, valid=True):
    if not valid:
        return {"set": set_, "id": rid, "valid_reply": False}
    return {"set": set_, "id": rid, "valid_reply": True,
            "must": {k: {"verdict": v, "quote": "q " + k} for k, v in must.items()},
            "must_not": {k: {"verdict": v, "quote": "q " + k} for k, v in must_not.items()}}


ALL_ABSENT = {"m1": "absent", "m2": "absent", "m3": "absent", "m4": "absent"}
NONE_NOT = {"n1": "absent", "n2": "absent"}


class RubricProbes(unittest.TestCase):
    def test_counts_and_unmet_points_per_probe(self):
        rows = [
            prow("probe_generic", "r-1", dict(ALL_ABSENT, m4="present"), NONE_NOT),                      # m4 present: 1 of 4 not met
            prow("probe_alt", "r-1", dict(ALL_ABSENT, m1="present", m3="present"), dict(NONE_NOT, n2="present")),   # group {m2,m3} via m3; n2 hit
            prow("probe_ref", "r-1", dict(ALL_ABSENT, m1="present", m2="present"), NONE_NOT),            # m3 (weight 2) absent: not met
            prow("probe_nearmiss", "r-1~n1", ALL_ABSENT, NONE_NOT),
            prow("probe_nearmiss", "r-1~n2", ALL_ABSENT, dict(NONE_NOT, n2="present")),
        ]
        res = pr.probe_results(rows, [PROBE_RUBRIC])
        self.assertEqual((res["probe_generic"]["met"], res["probe_generic"]["expected"]), (3, 4))
        self.assertEqual([(i, p) for i, p, _ in res["probe_generic"]["unmet"]], [("r-1", "m4")])
        # alt: units m1, the group m2+m3 (met through m3), n1, n2 -> 4 expectations, n2 unmet
        self.assertEqual((res["probe_alt"]["met"], res["probe_alt"]["expected"]), (3, 4))
        self.assertEqual([(i, p) for i, p, _ in res["probe_alt"]["unmet"]], [("r-1", "n2")])
        # ref: weight-2 points m1, m2, m3 present, plus n1, n2 absent -> m3 unmet
        self.assertEqual((res["probe_ref"]["met"], res["probe_ref"]["expected"]), (4, 5))
        self.assertEqual([(i, p) for i, p, _ in res["probe_ref"]["unmet"]], [("r-1", "m3")])
        self.assertEqual((res["probe_nearmiss"]["met"], res["probe_nearmiss"]["expected"]), (1, 2))
        self.assertEqual([(i, p) for i, p, _ in res["probe_nearmiss"]["unmet"]], [("r-1", "n2")])

    def test_missing_or_invalid_answers_are_unmet_not_skipped(self):
        res = pr.probe_results([prow("probe_nearmiss", "r-1~n1", {}, {}, valid=False)], [PROBE_RUBRIC])
        self.assertEqual(res["probe_nearmiss"]["met"], 0)
        self.assertEqual([n for _, _, n in res["probe_nearmiss"]["unmet"]], ["no valid reply", "not judged"])
        self.assertEqual(res["probe_generic"]["expected"], 4)
        self.assertEqual(res["probe_generic"]["met"], 0)

    def test_section_in_the_report_and_not_a_criterion(self):
        rows = [row2("d", "q-1", p, {"m1": "present", "m2": "absent", "m3": "present"}) for p in (1, 2)]
        probes = [prow("probe_generic", "r-1", ALL_ABSENT, NONE_NOT)]
        text, nums = pr.build_report_v2(rows, probes=probes, rubrics=[PROBE_RUBRIC], chunks={"c1": {"id": "c1", "source": "src", "path": "raw/src/a.md", "text": "t"}})
        self.assertIn("## Rubric probes", text)
        self.assertIn("**probe_generic** (generic answer; 1 answers; expected: no must point present): **4 of 4 expectations met**", text)
        self.assertIn("**probe_nearmiss**", text)
        self.assertNotIn("probe_", text.split("## The five criteria")[1].split("## Rubric probes")[0])
        self.assertEqual(nums["criteria"].keys(), {"1 kappa", "2 variance share", "3 quote failures", "4 flag precision and recall", "5 supported omissions"})
        self.assertNotIn("## Rubric probes", pr.build_report_v2(rows)[0])


class SourceSection(unittest.TestCase):
    def test_counts_of_points_without_a_source_to_match(self):
        chunks = {"c1": {"id": "c1", "source": "src", "path": "raw/src/a.md", "text": "t"},
                  "c2": {"id": "c2", "source": "src", "path": "b.md", "text": "t"}}          # relative to the source's root
        text = "\n".join(pr.source_section([PROBE_RUBRIC], chunks))
        self.assertIn("| file path with chunks in the index | 2 | 2 |", text)       # m1, m2 (b.md becomes raw/src/b.md)
        self.assertIn("| only a raw_catalogue file (tool source, not in the index) | 1 | 0 |", text)
        self.assertIn("| no file path ('searched', 'inferred', 'not covered') | 1 | 1 |", text)
        self.assertIn("Points that can never be a supported essential miss: 2 of 4 (1 of 3 with weight 2); of these, 1 have no source", text)


class CoverageReport(unittest.TestCase):
    """The coverage role's batch report on a synthetic batch of 40 answers (offline): every criterion is printed."""
    N = 40
    ANSWER = "We set a. We set b. We use c. We use d."
    QUOTE = {"m1": "We set a.", "m2": "We set b.", "m3": "We use c.", "m4": "We use d."}

    @staticmethod
    def rubric(i):
        return {"id": f"t-{i:02d}", "question": "q",
                "must": [{"id": "m1", "text": "a", "weight": 2, "first": False, "source": "x", "chunks": ["c1"]},
                         {"id": "m2", "text": "b", "weight": 2, "first": False, "source": "x", "chunks": ["c2"]},
                         {"id": "m3", "text": "c", "weight": 2, "first": False, "source": "x", "chunks": ["c3"]},
                         {"id": "m4", "text": "d", "weight": 2, "first": False, "source": "x", "chunks": ["c4"]},
                         {"id": "m5", "text": "e", "weight": 1, "first": False, "source": "x", "not_indexed": True}],
                "must_not": [{"id": "n1", "text": "z", "severity": "wrong"}], "any_of": [["m3", "m4"]]}

    @staticmethod
    def truth(i):
        """Units missed in answer i: m1 if i % 4 == 0, m2 if i % 3 == 0, the group {m3, m4} if i % 5 == 0."""
        return {"m1": i % 4 == 0, "m2": i % 3 == 0, "m34": i % 5 == 0}

    def judge_rows(self, bad_quote_pass1=1):
        rows = []
        for i in range(self.N):
            t = self.truth(i)
            for p in (1, 2):
                v = {"m1": "absent" if t["m1"] else "present", "m2": "absent" if t["m2"] else "present",
                     "m3": "absent" if t["m34"] else "present", "m4": "absent", "m5": "absent"}
                quotes = dict(self.QUOTE)
                if p == 1 and i == bad_quote_pass1:
                    quotes["m2"] = "text the answer never had"
                j = {"must": [{"id": k, "verdict": x, "quote": quotes.get(k, "We set a.") if x != "absent" else ""} for k, x in v.items()],
                     "must_not": [{"id": "n1", "verdict": "absent", "quote": ""}], "other_errors": []}
                it = {"set": "default" if i % 2 == 0 else "strict", "id": f"t-{i:02d}", "question": "q", "answer": self.ANSWER, "rubric": self.rubric(i),
                      "pack": ["c1", "c3"]}
                rows.append(ej.result_row(it, p, {"key": f"{i}{p}", "j": j, "instruction": "v2"}, coverage=True))
        return rows

    def findings(self, second_reader_extra=0):
        sets_ = lambda i: "default" if i % 2 == 0 else "strict"
        f = [{"set": sets_(i), "id": f"t-{i:02d}", "reader": "A", "kind": "read"} for i in range(self.N)]    # marker: answer read by A
        for i in range(self.N):
            t = self.truth(i)
            sid = "default" if i % 2 == 0 else "strict"
            base = {"set": sid, "id": f"t-{i:02d}", "reader": "A"}
            if t["m1"]:
                f.append({**base, "kind": "omission", "point": "m1", "supported": True})
            if t["m2"]:
                f.append({**base, "kind": "omission", "point": "m2", "supported": False})
            if t["m34"]:
                f.append({**base, "kind": "omission", "point": "m3|m4"})
            if i % 3 == 1:
                f.append({**base, "kind": "wrong", "quote": "w"})
            if i % 10 == 2:
                f.append({**base, "kind": "harmful", "quote": "h"})
            if i < 10:     # reader B reads the first ten answers as well
                f.append({"set": sid, "id": f"t-{i:02d}", "reader": "B", "kind": "wrong" if i % 3 == 1 else "invented", "quote": "x"})
                if i % 3 == 1:
                    f.append({"set": sid, "id": f"t-{i:02d}", "reader": "B", "kind": "invented", "quote": "y", "minor": True})
        return f

    def report(self, findings=None, rows=None, **kw):
        rubs = [self.rubric(i) for i in range(self.N)]
        return pr.build_report_coverage(rows or self.judge_rows(), rubs, findings if findings is not None else self.findings(), **kw)

    def test_every_criterion_is_printed_and_the_numbers_are_hand_computed(self):
        text, num = self.report(idcheck_false=0)
        for c in ("C1", "C2", "C3", "C4", "C5", "C6", "Descriptive, not criteria", "Paired difference"):
            self.assertIn(c, text)
        # 120 units per pass (3 per answer). Pass 2: judge and reader agree on all of them.
        self.assertEqual(num["c1_pass2"]["units"], 120)
        self.assertEqual((num["c1_pass2"]["judge_only"], num["c1_pass2"]["reader_only"]), (0, 0))
        self.assertAlmostEqual(num["c1_pass2"]["kappa"], 1.0)
        # Pass 1: in answer 1 the judge says m2 present but its quote is not found: unverified, not a miss (the old rule made it a judge-only miss).
        self.assertEqual((num["c1_pass1"]["judge_only"], num["c1_pass1"]["reader_only"]), (0, 0))
        self.assertEqual(num["c1_pass1"]["both"], num["c1_pass2"]["both"])
        self.assertEqual(num["c3"]["unverified"], 1)
        self.assertEqual(num["c3"]["must_verdicts"], 40 * 2 * 5)
        self.assertEqual(num["c3"]["raw_demoted"], 0)
        self.assertEqual(num["criteria"]["C1 essential-miss units, judge against reader"], "PASS")
        self.assertEqual(num["criteria"]["C3 unverified verdicts"], "PASS")
        self.assertEqual(num["criteria"]["C6 identifier check"], "PASS")

    def test_supported_misses_are_confirmed_by_a_reader_omission_with_supported_true(self):
        _, num = self.report()
        # pack = c1, c3: m1 (c1) and the group (c3 is a member's chunk) are supported; m2 (c2) is not.
        # Judge-supported miss units per pass: m1 missed 10 answers (i % 4 == 0), group missed 8 (i % 5 == 0) -> 18 per pass, 36 in all.
        self.assertEqual(num["c2"]["flags"], 36)
        # The reader confirms m1 (supported true): 10 per pass; its group rows carry no 'supported' field = true: 8 per pass.
        self.assertEqual(num["c2"]["confirmed"], 36)
        self.assertEqual(num["criteria"]["C2 supported misses confirmed by the reader"], "PASS")
        # now the reader says the excerpts lacked the m1 passage: 20 of 36 flags lose their confirmation
        f = [dict(x, supported=False) if x.get("point") == "m1" else x for x in self.findings()]
        _, num2 = self.report(f)
        self.assertEqual((num2["c2"]["confirmed"], num2["c2"]["flags"]), (16, 36))
        self.assertEqual(num2["criteria"]["C2 supported misses confirmed by the reader"], "FAIL")

    def test_two_readers_totals_and_table(self):
        text, num = self.report()
        c5 = num["c5"]
        self.assertEqual(c5["answers"], 10)
        # A on answers 0..9: wrong at i % 3 == 1 (1, 4, 7) = 3, harmful at i % 10 == 2 (2) = 1 -> 4. B: wrong 1, 4, 7 and invented on 0, 2, 3, 5, 6, 8, 9
        # (7) plus 3 minor invented -> 3 + 7 + 3 = 13
        self.assertEqual((c5["total_a"], c5["total_b"]), (4, 13))
        self.assertAlmostEqual(c5["difference"], 9 / 13)
        self.assertEqual(num["criteria"]["C5 two readers agree on totals"], "FAIL")
        self.assertIn("Readings not usable for a bar", text)
        self.assertIn("| default | t-00 | 0 | 1 |", text)
        self.assertIn("without findings marked minor", text)
        # readers within 30 %: drop B's findings to match A's total of 4
        f = [x for x in self.findings() if x.get("reader") != "B"]
        f += [{"set": "default" if i % 2 == 0 else "strict", "id": f"t-{i:02d}", "reader": "B", "kind": "wrong", "quote": "x"} for i in (1, 4, 7, 2)]
        _, num2 = self.report(f)
        self.assertEqual((num2["c5"]["total_a"], num2["c5"]["total_b"]), (4, 4))
        self.assertEqual(num2["criteria"]["C5 two readers agree on totals"], "PASS")

    def test_waiting_and_failing_criteria_and_the_go_rule(self):
        text, num = self.report()
        self.assertEqual(num["criteria"]["C6 identifier check"], "WAITING")
        self.assertFalse(num["go"])
        self.assertNotIn("Go or no-go", text)              # C6 waits: neither GO nor NO-GO
        text, num = self.report(idcheck_false=0)
        self.assertIn("Go or no-go: NO-GO", text)          # C5 fails, nothing waits
        f = [x for x in self.findings() if x.get("reader") != "B"]
        _, n1 = self.report(f, idcheck_false=0)            # no second reader: C5 waits
        self.assertEqual(n1["criteria"]["C5 two readers agree on totals"], "WAITING")
        self.assertFalse(n1["go"])
        _, n2 = self.report(f, idcheck_false=3)
        self.assertEqual(n2["criteria"]["C6 identifier check"], "FAIL")
        # all six hold: add a second reader within 30 %
        f2 = f + [{"set": "default" if i % 2 == 0 else "strict", "id": f"t-{i:02d}", "reader": "B", "kind": "wrong", "quote": "x"} for i in (1, 4, 7, 2)]
        text3, n3 = self.report(f2, idcheck_false=0)
        self.assertTrue(n3["go"], text3)
        self.assertIn("Go or no-go: GO", text3)

    def test_fewer_than_100_units_waits_and_rows_that_do_not_map_are_listed(self):
        rubs = [self.rubric(i) for i in range(self.N)]
        rows = [r for r in self.judge_rows() if int(r["id"][2:]) < 20]
        f = [x for x in self.findings() if int(x["id"][2:]) < 20]
        f.append({"set": "default", "id": "t-00", "reader": "A", "kind": "omission", "point": "m1|m2"})     # not an any_of group
        f.append({"set": "default", "id": "t-02", "reader": "A", "kind": "omission", "point": "m5"})        # weight 1: not a unit
        text, num = pr.build_report_coverage(rows, rubs, f)
        self.assertEqual(num["criteria"]["C1 essential-miss units, judge against reader"], "WAITING")
        self.assertIn("fewer than 100 units", text)
        self.assertIn("do not map to a unit (not used): 2", text)
        self.assertIn("several points that are not one any_of group", text)
        self.assertIn("not a weight-2 unit", text)

    def test_rows_made_without_the_coverage_option_fail_c3_and_say_so(self):
        # the old demotion stored as absent: C3 needs 0 such verdicts
        rows = self.judge_rows()
        r = next(x for x in rows if x["id"] == "t-01" and x["pass"] == 1)
        r["must"]["m2"] = dict(r["must"]["m2"], verdict="absent")
        text, num = self.report(rows=rows)
        self.assertEqual(num["c3"]["raw_demoted"], 1)
        self.assertEqual(num["criteria"]["C3 unverified verdicts"], "FAIL")

    def test_command_line_end_to_end_with_two_readers(self):
        import io
        tmp = Path(tempfile.mkdtemp())
        (tmp / "res.jsonl").write_text("".join(json.dumps(x) + "\n" for x in self.judge_rows()))
        (tmp / "rub.jsonl").write_text("".join(json.dumps(self.rubric(i)) + "\n" for i in range(self.N)))
        (tmp / "f.jsonl").write_text("".join(json.dumps(x) + "\n" for x in self.findings()))
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = pr.main(["--coverage", "--results", str(tmp / "res.jsonl"), "--rubrics", str(tmp / "rub.jsonl"), "--findings", str(tmp / "f.jsonl"),
                          "--idcheck-false-invented", "0"])
        self.assertEqual(rc, 0)
        out = buf.getvalue()
        for c in ("C1 essential-miss", "C2 supported", "C3 unverified", "C4 kappa", "C5 two readers", "C6 identifier"):
            self.assertIn(c, out)

    def test_paired_difference_between_sets_with_its_standard_error(self):
        rows = [dict(r, set=name) for r in self.judge_rows() if int(r["id"][2:]) < 3 for name in ("default", "strict")]
        rubs = [self.rubric(i) for i in range(3)]
        f = []
        for name, counts in (("default", (2, 1, 3)), ("strict", (0, 1, 1))):
            for i, n in enumerate(counts):
                f.append({"set": name, "id": f"t-{i:02d}", "reader": "A", "kind": "read"})
                f += [{"set": name, "id": f"t-{i:02d}", "reader": "A", "kind": "wrong", "quote": "w"}] * n
        text, _ = pr.build_report_coverage(rows, rubs, f)
        # d = (2, 0, 2): mean 1.333, SD 1.1547, SE 0.667; per 10 answers: default 20.0 (5.8), strict 6.7 (3.3)
        self.assertIn("| reader_wih | default - strict | 3 | 1.333 (0.667) |", text)
        self.assertIn("| default | 3 | 20.0 (5.8) |", text)
        self.assertIn("| strict | 3 | 6.7 (3.3) |", text)

    def test_empty_findings_everything_that_needs_readers_waits(self):
        text, num = self.report(findings=[], idcheck_false=0)
        for k in ("C1 essential-miss units, judge against reader", "C2 supported misses confirmed by the reader", "C5 two readers agree on totals"):
            self.assertEqual(num["criteria"][k], "WAITING")
        self.assertNotIn("FAIL", text)
        self.assertNotIn("NO-GO", text)
        self.assertNotIn("Go or no-go", text)
        self.assertIn("Waiting for the readers' files", text)
        self.assertIn("3 of 6 criteria passed, 0 failed, 3 waiting", text)
        self.assertEqual(num["criteria"]["C3 unverified verdicts"], "PASS")
        self.assertEqual(num["criteria"]["C4 kappa between the passes"], "PASS")
        self.assertEqual(num["criteria"]["C6 identifier check"], "PASS")
        self.assertFalse(num["go"])

    def test_partly_read_batch_computes_c1_on_the_read_answers_only(self):
        f = [x for x in self.findings() if int(x["id"][2:]) < 30 and x.get("reader") != "B"]
        text, num = self.report(f, idcheck_false=0)
        self.assertEqual(num["c1_pass2"]["units"], 90)          # 30 read answers x 3 units
        self.assertIn("computed on the 30 read answers of 40", text)
        self.assertEqual(num["criteria"]["C1 essential-miss units, judge against reader"], "WAITING")   # under 100 units
        self.assertNotIn("Go or no-go", text)

    def test_old_modes_still_print_their_reports(self):
        # the first pilot's and the second pilot's modes are untouched by the coverage mode (their own tests above); the v2 report still builds
        text, _ = pr.build_report_v2([row2("default", "p-1", 1, {"m1": "present", "m2": "absent", "m3": "present"}),
                                      row2("default", "p-1", 2, {"m1": "present", "m2": "absent", "m3": "present"})])
        self.assertIn("## The five criteria", text)


if __name__ == "__main__":
    unittest.main()


class CoverageTagFile(unittest.TestCase):
    """--tag-file: every total again without the tagged items; synthetic encrypted data for the hidden path."""
    N, ANSWER, QUOTE = CoverageReport.N, CoverageReport.ANSWER, CoverageReport.QUOTE
    rubric = staticmethod(CoverageReport.rubric)
    truth = staticmethod(CoverageReport.truth)
    judge_rows = CoverageReport.judge_rows
    # default: 2, 1, 3 wrong findings on t-00 .. t-02; strict: 0, 1, 1

    def small(self):
        rows = [dict(r, set=name) for r in self.judge_rows() if int(r["id"][2:]) < 3 for name in ("default", "strict")]
        rubs = [self.rubric(i) for i in range(3)]
        f = []
        for name, counts in (("default", (2, 1, 3)), ("strict", (0, 1, 1))):
            for i, n in enumerate(counts):
                f.append({"set": name, "id": f"t-{i:02d}", "reader": "A", "kind": "read"})
                f += [{"set": name, "id": f"t-{i:02d}", "reader": "A", "kind": "wrong", "quote": "w"}] * n
        return rows, rubs, f

    def run_main(self, tmp, tags, hidden=False, extra=()):
        import io
        tagmap = tags if isinstance(tags, dict) else {"exposed": tags}
        sfx = ".jsonl.enc" if hidden else ".jsonl"
        rows, rubs, f = self.small()
        for name, data in (("res", rows), ("rub", rubs), ("f", f)):
            if hidden:
                eval2.write_jsonl(tmp / (name + sfx), data)
            else:
                (tmp / (name + sfx)).write_text("".join(json.dumps(x) + "\n" for x in data))
        tf = tmp / "tags.json"
        tf.write_text(json.dumps({"tags": {n: {"ids": v} for n, v in tagmap.items()}}))
        argv = ["--coverage", "--results", str(tmp / ("res" + sfx)), "--rubrics", str(tmp / ("rub" + sfx)),
                "--findings", str(tmp / ("f" + sfx)), "--idcheck-false-invented", "0", *extra]
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            rc = pr.main(argv)
        return rc, out.getvalue(), err.getvalue(), str(tf)

    def test_both_sets_of_totals_on_a_constructed_case(self):
        tmp = Path(tempfile.mkdtemp())
        _, plain, _, tf = self.run_main(tmp, ["t-00"])
        self.assertNotIn("without the items tagged", plain)
        rc, out, err, _ = self.run_main(tmp, ["t-00"], extra=["--tag-file", tf])
        self.assertEqual((rc, err), (0, ""))
        self.assertTrue(out.startswith(plain))      # the totals over all items come first and are the unchanged output
        head = "# without the items tagged exposed (1 items; 2 remain)"
        self.assertIn(head, out)
        first, second = out.split(head)
        # all items: d = (2, 0, 2): mean 1.333; default 20.0 (5.8) per 10
        self.assertIn("| reader_wih | default - strict | 3 | 1.333 (0.667) |", first)
        self.assertIn("| default | 3 | 20.0 (5.8) |", first)
        # without t-00: d = (0, 2): mean 1.000, SE 1.000; default (1, 3) -> 20.0 (10.0); strict (1, 1) -> 10.0 (0.0)
        self.assertIn("| reader_wih | default - strict | 2 | 1.000 (1.000) |", second)
        self.assertIn("| default | 2 | 20.0 (10.0) |", second)
        self.assertIn("| strict | 2 | 10.0 (0.0) |", second)
        for c in ("C1", "C2", "C3", "C4", "C5", "C6", "Descriptive, not criteria"):
            self.assertIn(c, second)
        self.assertNotIn("t-00", out)

    def test_hidden_path_with_a_temporary_key(self):
        import os
        from unittest import mock
        tmp = Path(tempfile.mkdtemp())
        key = tmp / "k"
        eval2.keygen(key)
        with mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(key)}):
            rc, plain, err, tf = self.run_main(tmp, ["t-00"], hidden=True)
            self.assertEqual((rc, err), (0, ""))
            rc2, out, err2, _ = self.run_main(tmp, ["t-00"], hidden=True, extra=["--tag-file", tf])
            self.assertEqual((rc2, err2), (0, ""))
            self.assertTrue(out.startswith(plain))
            self.assertIn("# without the items tagged exposed (1 items; 2 remain)", out)
            self.assertIn("| reader_wih | default - strict | 2 | 1.000 (1.000) |", out)
            self.assertNotIn("t-0", out)
            self.assertFalse([p for p in tmp.iterdir() if p.suffix in (".jsonl",)])
            # an unknown id is not an error: the block says how many tagged items are present
            rc3, out3, err3, _ = self.run_main(tmp, ["t-00", "t-77"], hidden=True, extra=["--tag-file", str(tmp / "tags.json")])
            self.assertEqual((rc3, err3), (0, ""))
            self.assertIn("# without the items tagged exposed (1 of 2 tagged items present; 2 remain)", out3)
            self.assertNotIn("t-77", out3)

    def test_two_tags_give_one_block_each_and_the_union(self):
        tmp = Path(tempfile.mkdtemp())
        tags = {"A": ["t-00", "t-01"], "B": ["t-01", "zz-9"], "C": ["zz-8"]}
        rc, plain, err, tf = self.run_main(tmp, tags)
        rc, out, err, _ = self.run_main(tmp, tags, extra=["--tag-file", tf])
        self.assertEqual((rc, err), (0, ""))
        self.assertNotIn("without the", plain)
        self.assertTrue(out.startswith(plain))
        heads = [l for l in out.splitlines() if l.startswith("# without")]
        self.assertEqual(heads, ["# without the items tagged A (2 items; 1 remain)",
                                 "# without the items tagged B (1 of 2 tagged items present; 2 remain)",
                                 "# without the items tagged C (0 of 1 tagged items present; 3 remain)",
                                 "# without the union of all tagged items (2 of 4 tagged items present; 1 remain)"])
        self.assertNotIn("zz-", out)

    def test_one_tag_has_no_union_block(self):
        tmp = Path(tempfile.mkdtemp())
        _, out, _, tf = self.run_main(tmp, ["t-00"], extra=["--tag-file", str(tmp / "tags.json")])
        self.assertNotIn("union", out)

    def test_tag_file_without_coverage_is_refused(self):
        with self.assertRaises(SystemExit):
            with redirect_stderr(io.StringIO()):
                pr.main(["--results", "x", "--tag-file", "y"])
