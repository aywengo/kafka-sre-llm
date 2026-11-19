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


if __name__ == "__main__":
    unittest.main()
