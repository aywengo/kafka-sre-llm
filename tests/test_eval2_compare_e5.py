"""Offline tests of the E5 parts of scripts/eval2_compare.py (e5, e5-screen, pick-model --id) and of the ledger rows of
scripts/m4_e2_manifest.py. Synthetic data only; hidden cases use a temporary key and .enc files in a temporary folder."""
import contextlib
import io
import json
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2  # noqa: E402
import eval2_compare as cmp  # noqa: E402
import m4_e2_manifest as mm  # noqa: E402
import test_eval2_compare as T  # noqa: E402

NEEDS_OPENSSL = unittest.skipUnless(shutil.which("openssl"), "openssl not on PATH")
LIM = {"memory_gb": 16.7}
JIDS = [f"j{n:02d}" for n in range(12)]


def reading(cands, dis=0.6, usable=True, harm_r=1, ref="R", ref_rate=6.0):
    """cands: {name: dict(dr, hi_sup1, hi_sup2, hi90, harm)}: a reading JSON of the shape of eval2_sealed_reading.py totals."""
    sets = {ref: {"answers_read": 61, "rate_per_10": ref_rate, "harmful": harm_r}}
    for n, c in cands.items():
        sets[n] = {"answers_read": 61, "rate_per_10": ref_rate + c["dr"], "harmful": c.get("harm", harm_r),
                   "paired_vs_default": {"diff": c["dr"], "ci90": [c["dr"] - 2.0, c["hi90"]],
                                         "ci_by_k": {"1": [c["dr"] - 2.0, c["hi_sup1"]], "2": [c["dr"] - 2.5, c["hi_sup2"]]}}}
    return {"default_set": ref, "readers_usable": usable, "reader_disagreement_mean_per_set": dis, "sets": sets}


class E5Base(T.Base):
    def setUp(self):
        super().setUp()
        self.items = T.e2_items()
        self.ids = [i["id"] for i in self.items]
        self.base = set(self.ids) - set(self.ids[::2])             # the reference passes 100 of 200
        self.fails = self.ids[::2]

    def answers_file(self, name, seconds, hidden, ids=None):
        """Judged answers of one configuration: one row per scenario, seconds.total from `seconds` (a number, or a function of the index)."""
        p = self.tmp / (f"{name}.janswers" + (".jsonl.enc" if hidden else ".jsonl"))
        rows = []
        for n, j in enumerate(ids if ids is not None else JIDS):
            sec = seconds(n) if callable(seconds) else seconds
            rows.append({"id": j, "text": "t", "error": None, "seconds": {"total": sec}})
        eval2.write_jsonl(p, rows)
        return [p]

    def run_e5(self, extras, reading_=None, judged=None, hidden=False, limits=LIM, memory=None, median=None, licence=None,
               ref_pass=None, resamples=400, tags=None):
        """extras: {candidate: set of the reference's failures it passes, or a full set via ('set', S)}."""
        sfx = ".jsonl.enc" if hidden else ".jsonl"
        ip = self.tmp / ("items" + sfx)
        eval2.write_jsonl(ip, self.items)
        cfg = {}
        rp = self.base if ref_pass is None else ref_pass
        p = self.tmp / ("R.scores" + sfx)
        eval2.write_jsonl(p, T.score_rows(self.items, rp))
        cfg["R"] = [p]
        for n, ex in extras.items():
            passed = ex[1] if isinstance(ex, tuple) else rp | set(ex)
            p = self.tmp / (n + ".scores" + sfx)
            eval2.write_jsonl(p, T.score_rows(self.items, passed))
            cfg[n] = [p]
        jd = None
        if judged:
            jres, jrub = self.tmp / ("jres" + sfx), self.tmp / ("jrub" + sfx)
            jitems = [{"id": j} for j in JIDS]
            rows = [r for st, ps in judged.items() for r in T.jrows(st, jitems, ps)]
            eval2.write_jsonl(jres, rows)
            eval2.write_jsonl(jrub, [{"id": j, "must": [{"id": "m1", "text": "t", "weight": 1}], "must_not": []} for j in JIDS])
            jd = {"results": [jres], "rubrics": jrub}
        med = {"R": 80.0, **({n: 80.0 for n in extras} if median is None else median)}
        ja = {n: self.answers_file(n, v, hidden) for n, v in med.items()}
        if median == {}:
            ja = {}
        return cmp.compare_e5(cfg, [ip], jd, "R", list(extras), reading_, limits, licence if licence is not None else {n: True for n in extras},
                              memory if memory is not None else {n: 15.0 for n in extras}, ja, resamples, 1, tags)

    def same_p(self, *names):
        return {n: {j: True for j in JIDS} for n in ("R", *names)}

    def good_reading(self, *names, **kw):
        return reading({n: {"dr": -5.0, "hi_sup1": -2.0, "hi_sup2": -1.5, "hi90": -2.5} for n in names}, **kw)


class TestSuperiorityAndNonInferiority(E5Base):
    GAIN = {"C": set(T.e2_items()[0:0])}

    def strong(self):
        return set(self.fails[:40])                        # +20 points

    def test_dm_superiority_both_ways_and_the_interval_level_follows_k(self):
        up = self.run_e5({"C": self.strong()}, self.good_reading("C"), self.same_p("C"))
        self.assertEqual(up["cands"]["C"]["criteria"]["1 superiority"], "met")
        self.assertTrue(up["cands"]["C"]["dm_arm"])
        self.assertEqual(up["sup_confidence_pct"], 90.0)
        flat = self.run_e5({"C": set()}, self.good_reading("C"), self.same_p("C"))
        self.assertEqual(flat["cands"]["C"]["dm"]["diff"], 0.0)
        # Dr arm: good_reading has hi_sup1 -2 below 0 and |Dr| 5 above 0.6, so superiority is still met through the reading
        self.assertTrue(flat["cands"]["C"]["dr_arm"])
        self.assertFalse(flat["cands"]["C"]["dm_arm"])

    def test_the_interval_is_wider_for_two_candidates_and_can_flip_the_arm(self):
        extra = set(self.fails[:6])                        # +3 points: lower end of the 90 % interval just above 0, of the 95 % not
        r1 = self.run_e5({"C": extra}, None)
        r2 = self.run_e5({"C": extra, "D": set()}, None)
        self.assertEqual(r2["sup_confidence_pct"], 95.0)
        a, b = r1["cands"]["C"]["dm"], r2["cands"]["C"]["dm"]
        self.assertEqual(a["sup_ci"], a["ci90"])           # k = 1: the superiority interval is the 90 % interval
        self.assertEqual(a["ci90"], b["ci90"])
        self.assertLessEqual(b["sup_ci"][0], a["sup_ci"][0])
        self.assertGreaterEqual(b["sup_ci"][1], a["sup_ci"][1])
        self.assertTrue(r1["cands"]["C"]["dm_arm"])
        self.assertFalse(r2["cands"]["C"]["dm_arm"])

    def test_dr_arm_needs_hi_sup_below_zero_and_the_gap_above_the_disagreement(self):
        ok = reading({"C": {"dr": -3.0, "hi_sup1": -0.5, "hi_sup2": -0.2, "hi90": -1.0}}, dis=1.0)
        r = self.run_e5({"C": set()}, ok, self.same_p("C"))
        self.assertTrue(r["cands"]["C"]["dr_arm"])
        self.assertEqual(r["cands"]["C"]["criteria"]["1 superiority"], "met")
        small = reading({"C": {"dr": -3.0, "hi_sup1": -0.5, "hi_sup2": -0.2, "hi90": -1.0}}, dis=3.0)     # |Dr| not above the disagreement
        r = self.run_e5({"C": set()}, small, self.same_p("C"))
        self.assertFalse(r["cands"]["C"]["dr_arm"])
        self.assertEqual(r["cands"]["C"]["criteria"]["1 superiority"], "not met")
        cross = reading({"C": {"dr": -3.0, "hi_sup1": 0.4, "hi_sup2": 0.5, "hi90": -1.0}}, dis=1.0)       # upper end not below 0
        r = self.run_e5({"C": set()}, cross, self.same_p("C"))
        self.assertEqual(r["cands"]["C"]["criteria"]["1 superiority"], "not met")

    def test_k_two_uses_the_k_two_interval_of_the_reading(self):
        rd = reading({"C": {"dr": -3.0, "hi_sup1": -0.5, "hi_sup2": 0.3, "hi90": -1.0}, "D": {"dr": 0.0, "hi_sup1": 3, "hi_sup2": 4, "hi90": 2.0}}, dis=1.0)
        r1 = self.run_e5({"C": set()}, rd, self.same_p("C", "D"))
        r2 = self.run_e5({"C": set(), "D": set()}, rd, self.same_p("C", "D"))
        self.assertTrue(r1["cands"]["C"]["dr_arm"])
        self.assertFalse(r2["cands"]["C"]["dr_arm"])
        self.assertEqual(r2["cands"]["C"]["dr"]["hi_sup"], 0.3)

    def test_non_inferiority_of_each_of_the_three_both_ways(self):
        # Dm: a loss of 20 points fails, equal passes
        lost = (("set", self.base - set(sorted(self.base)[:40])),)[0]
        r = self.run_e5({"C": lost}, self.good_reading("C"), self.same_p("C"))
        self.assertEqual(r["cands"]["C"]["parts_2"]["Dm"], "not met")
        self.assertEqual(r["cands"]["C"]["criteria"]["2 non-inferiority"], "not met")
        r = self.run_e5({"C": set()}, self.good_reading("C"), self.same_p("C"))
        self.assertEqual(r["cands"]["C"]["parts_2"], {"Dm": "met", "Dp": "met", "Dr observed": "met", "Dr upper end": "met"})
        self.assertEqual(r["cands"]["C"]["criteria"]["2 non-inferiority"], "met")
        # Dp: the candidate loses coverage on 6 of 12 scenarios (-0.5)
        jp = {"R": {j: True for j in JIDS}, "C": {j: n >= 6 for n, j in enumerate(JIDS)}}
        r = self.run_e5({"C": set()}, self.good_reading("C"), jp)
        self.assertAlmostEqual(r["cands"]["C"]["dp"]["mean"], -0.5)
        self.assertEqual(r["cands"]["C"]["parts_2"]["Dp"], "not met")
        # Dr (the Owner's amendment of 2026-11-21): BOTH the observed Dr at most 0 AND the 90 % upper end below +3.0.
        # observed above 0 with the upper end below +3.0: does not qualify (a candidate that is good enough on mechanical grounds)
        strong = set(self.fails[:40])
        a = reading({"C": {"dr": 0.4, "hi_sup1": 2.0, "hi_sup2": 2.1, "hi90": 2.5}})
        r = self.run_e5({"C": strong}, a, self.same_p("C"))
        x = r["cands"]["C"]
        self.assertEqual((x["parts_2"]["Dr observed"], x["parts_2"]["Dr upper end"]), ("not met", "met"))
        self.assertEqual(x["criteria"]["2 non-inferiority"], "not met")
        self.assertEqual(x["qualifies"], False)
        self.assertEqual(r["overall"], "R STAYS")
        # observed at most 0 with the upper end at or above +3.0: does not qualify
        b = reading({"C": {"dr": -0.5, "hi_sup1": 3.5, "hi_sup2": 3.6, "hi90": 3.0}})
        r = self.run_e5({"C": strong}, b, self.same_p("C"))
        x = r["cands"]["C"]
        self.assertEqual((x["parts_2"]["Dr observed"], x["parts_2"]["Dr upper end"]), ("met", "not met"))
        self.assertEqual((x["criteria"]["2 non-inferiority"], x["qualifies"]), ("not met", False))
        # both met (observed exactly 0, upper end just below +3.0): qualifies
        c = reading({"C": {"dr": 0.0, "hi_sup1": 3.5, "hi_sup2": 3.6, "hi90": 2.99}})
        r = self.run_e5({"C": strong}, c, self.same_p("C"))
        x = r["cands"]["C"]
        self.assertEqual((x["parts_2"]["Dr observed"], x["parts_2"]["Dr upper end"]), ("met", "met"))
        self.assertEqual((x["criteria"]["2 non-inferiority"], x["qualifies"], x["verdict"]), ("met", True, "REPLACES"))
        out = io.StringIO()
        cmp.render_e5(r, out)
        self.assertIn("Dr observed at most 0: met; Dr 90 % upper end below +3.0: met", out.getvalue())

    def test_dp_missing_waits(self):
        r = self.run_e5({"C": set()}, self.good_reading("C"), None)
        self.assertEqual(r["cands"]["C"]["parts_2"]["Dp"], "waiting")
        self.assertIn("waiting", r["cands"]["C"]["verdict"])


class TestOtherCriteriaAndVerdicts(E5Base):
    def best(self, **kw):
        return self.run_e5({"C": set(self.fails[:40])}, self.good_reading("C"), self.same_p("C"), **kw)

    def test_all_met_replaces(self):
        r = self.best()
        x = r["cands"]["C"]
        self.assertEqual(set(x["criteria"].values()), {"met"})
        self.assertEqual(x["verdict"], "REPLACES")
        self.assertEqual(r["overall"], "C REPLACES R")

    def test_harmful_both_ways(self):
        rd = reading({"C": {"dr": -5.0, "hi_sup1": -2.0, "hi_sup2": -1.5, "hi90": -2.5, "harm": 2}}, harm_r=1)
        r = self.run_e5({"C": set(self.fails[:40])}, rd, self.same_p("C"))
        self.assertEqual(r["cands"]["C"]["criteria"]["3 harmful"], "not met")
        self.assertTrue(r["cands"]["C"]["verdict"].startswith("STAYS (not met: 3 harmful"))
        rd = reading({"C": {"dr": -5.0, "hi_sup1": -2.0, "hi_sup2": -1.5, "hi90": -2.5, "harm": 1}}, harm_r=1)     # equal is allowed
        r = self.run_e5({"C": set(self.fails[:40])}, rd, self.same_p("C"))
        self.assertEqual(r["cands"]["C"]["criteria"]["3 harmful"], "met")

    def test_memory_and_speed_both_ways_and_missing(self):
        for kw, state in (({"memory": {"C": 16.7}}, "met"), ({"memory": {"C": 16.71}}, "not met"),
                          ({"median": {"C": 92.0}}, "met"), ({"median": {"C": 92.1}}, "not met")):      # 1.15 x the reference's 80 s = 92 s
            r = self.best(**kw)
            self.assertEqual(r["cands"]["C"]["criteria"]["4 memory and speed"], state, kw)
        # the limit follows the reference's own median, taken from the files: 1.15 x 100 s = 115 s
        sp = self.best(median={"R": 100.0, "C": 114.0})
        self.assertEqual(sp["cands"]["C"]["criteria"]["4 memory and speed"], "met")
        sp = self.run_e5({"C": set(self.fails[:40])}, self.good_reading("C"), self.same_p("C"), median={"C": 114.0})     # R stays 80 s
        self.assertEqual(sp["cands"]["C"]["criteria"]["4 memory and speed"], "not met")
        self.assertAlmostEqual(sp["cands"]["C"]["speed"]["limit_s"], 92.0)
        r = self.best(memory={})
        self.assertEqual(r["cands"]["C"]["criteria"]["4 memory and speed"], "waiting")
        self.assertIn("waiting for 4 memory and speed", r["cands"]["C"]["verdict"])
        r = self.run_e5({"C": set(self.fails[:40])}, self.good_reading("C"), self.same_p("C"), limits={})
        self.assertEqual(r["cands"]["C"]["criteria"]["4 memory and speed"], "waiting")
        r = self.best(median={})                                     # no answer files: the speed cannot be judged
        self.assertEqual(r["cands"]["C"]["criteria"]["4 memory and speed"], "waiting")

    def test_speed_uses_only_the_scenarios_both_answered(self):
        # the candidate is very slow on a scenario the reference did not answer: it does not count
        ref = self.answers_file("R", 80.0, False, JIDS[:11])
        cand = self.answers_file("C", lambda n: 80.0 if n < 11 else 500.0, False)
        r0, c0, n = cmp.common_medians(ref, cand)
        self.assertEqual((r0, c0, n), (80.0, 80.0, 11))
        self.assertEqual(cmp.common_medians([], [])[2], 0)

    def test_licence_both_ways(self):
        r = self.best(licence={"C": False})
        self.assertEqual(r["cands"]["C"]["criteria"]["5 licence"], "not met")
        self.assertEqual(r["overall"], "R STAYS")
        r = self.best(licence={})
        self.assertEqual(r["cands"]["C"]["criteria"]["5 licence"], "not met")

    def test_waiting_for_the_reading_and_reading_not_used(self):
        r = self.run_e5({"C": set(self.fails[:40])}, None, self.same_p("C"))
        self.assertEqual(r["cands"]["C"]["verdict"], "waiting for the reading")
        self.assertTrue(r["overall"].startswith("waiting"))
        r = self.run_e5({"C": set(self.fails[:40])}, {"default_set": "R", "sets": {}}, self.same_p("C"))
        self.assertTrue(r["cands"]["C"]["verdict"].startswith("waiting for the reading"))
        r = self.run_e5({"C": set(self.fails[:40])}, self.good_reading("C", usable=False), self.same_p("C"))
        self.assertTrue(r["cands"]["C"]["verdict"].startswith("STAYS (reading not used"))
        self.assertEqual(r["overall"], "R STAYS")
        r = self.run_e5({"C": set(self.fails[:40])}, self.good_reading("C", usable=None), self.same_p("C"))
        self.assertIn("no second reader", r["cands"]["C"]["verdict"])
        # a waiting candidate next to a failing one: the failure is shown as a note, the verdict still waits
        r = self.run_e5({"C": set(self.fails[:40])}, None, self.same_p("C"), licence={"C": False})
        self.assertIn("5 licence", r["cands"]["C"]["note"])

    def test_tie_break_lowest_dr_then_highest_dm(self):
        rd = reading({"C": {"dr": -4.0, "hi_sup1": -1.0, "hi_sup2": -1.0, "hi90": -2.0},
                      "D": {"dr": -6.0, "hi_sup1": -2.0, "hi_sup2": -2.0, "hi90": -3.0}})
        r = self.run_e5({"C": set(self.fails[:60]), "D": set(self.fails[:40])}, rd, self.same_p("C", "D"))
        self.assertEqual((r["winner"], r["overall"]), ("D", "D REPLACES R"))
        self.assertEqual(r["cands"]["D"]["verdict"], "REPLACES")
        self.assertTrue(r["cands"]["C"]["verdict"].startswith("STAYS (qualifies, ranked behind D"))
        rd = reading({"C": {"dr": -5.0, "hi_sup1": -2.0, "hi_sup2": -2.0, "hi90": -3.0},
                      "D": {"dr": -5.0, "hi_sup1": -2.0, "hi_sup2": -2.0, "hi90": -3.0}})
        r = self.run_e5({"C": set(self.fails[:40]), "D": set(self.fails[:60])}, rd, self.same_p("C", "D"))
        self.assertEqual(r["winner"], "D")                  # equal Dr: the higher Dm
        r = self.run_e5({"C": set(self.fails[:60]), "D": set(self.fails[:40])}, rd, self.same_p("C", "D"))
        self.assertEqual(r["winner"], "C")

    def test_one_qualifier_one_failure(self):
        rd = reading({"C": {"dr": -5.0, "hi_sup1": -2.0, "hi_sup2": -2.0, "hi90": -3.0},
                      "D": {"dr": -5.0, "hi_sup1": -2.0, "hi_sup2": -2.0, "hi90": -3.0, "harm": 5}})
        r = self.run_e5({"C": set(self.fails[:40]), "D": set(self.fails[:40])}, rd, self.same_p("C", "D"))
        self.assertEqual(r["overall"], "C REPLACES R")
        self.assertEqual(r["cands"]["D"]["criteria"]["3 harmful"], "not met")

    def test_inputs_are_checked(self):
        with self.assertRaises(cmp.CompareError):
            cmp.compare_e5({"R": []}, [], None, "R", [])
        with self.assertRaises(cmp.CompareError):
            cmp.compare_e5({"R": []}, [], None, "R", ["R"])
        with self.assertRaises(cmp.CompareError):
            cmp.compare_e5({"R": []}, [], None, "R", ["X"])

    def test_render_and_json_hold_every_criterion(self):
        r = self.best()
        out = io.StringIO()
        cmp.render_e5(r, out)
        t = out.getvalue()
        for s in ("(1) superiority: met", "(2) non-inferiority: met", "(3) harmful", "(4) memory", "(5) licence accepted: met", "verdict: REPLACES",
                  "overall: C REPLACES R", "k = 1", "90.00 %"):
            self.assertIn(s, t)
        v = cmp.e5_verdicts(r)
        json.dumps(v)
        self.assertEqual(v["winner"], "C")


class TestTags(E5Base):
    def test_judged_parts_again_without_the_tagged_scenarios(self):
        tag = JIDS[:6]
        jp = {"R": {j: True for j in JIDS}, "C": {j: n >= 6 for n, j in enumerate(JIDS)}}       # all the losses are in the tagged ones
        rd = self.good_reading("C")
        rd["without_tagged"] = {"t": self.good_reading("C")}
        r = self.run_e5({"C": set()}, rd, jp, tags={"t": tag})
        self.assertAlmostEqual(r["cands"]["C"]["dp"]["mean"], -0.5)
        sub = r["without_tagged"]["t"]
        self.assertTrue(sub["judged_affected"])
        self.assertFalse(sub["mechanical_affected"])
        self.assertAlmostEqual(sub["res"]["cands"]["C"]["dp"]["mean"], 0.0)
        self.assertEqual(sub["res"]["cands"]["C"]["dp"]["n"], 6)
        out = io.StringIO()
        cmp.render_e5(r, out)
        self.assertIn("without the items tagged t (6 items", out.getvalue())
        self.assertIn("for information", out.getvalue())
        # an id that is not in the data is not an error: counted as not present
        r2 = self.run_e5({"C": set()}, rd, jp, tags={"t": ["nope"]})
        w = r2["without_tagged"]["t"]
        self.assertEqual((w["excluded"], w["tagged"], w["judged_affected"], w["mechanical_affected"]), (0, 1, False, False))
        out = io.StringIO()
        cmp.render_e5(r2, out)
        self.assertIn("without the items tagged t (0 of 1 tagged items present;", out.getvalue())
        self.assertNotIn("nope", out.getvalue())


@NEEDS_OPENSSL
class TestHidden(E5Base):
    def test_hidden_prints_no_id_and_json_is_enc_only(self):
        jp = {"R": {j: True for j in JIDS}, "C": {j: True for j in JIDS}}
        r = self.run_e5({"C": set(self.fails[:40])}, self.good_reading("C"), jp, hidden=True)
        self.assertTrue(r["hidden"])
        out = io.StringIO()
        cmp.render_e5(r, out)
        text = out.getvalue() + json.dumps(cmp.e5_verdicts(r))
        self.assertIn("hidden: totals only", out.getvalue())
        for i in self.ids[:30] + JIDS:
            self.assertNotIn(i, text)
        self.assertNotIn("hidden question text", text)
        # through the command line
        args = ["e5", "--reference", "R", "--candidate", "C", "--items", str(self.tmp / "items.jsonl.enc"), "--resamples", "100",
                "--config", f"R={self.tmp / 'R.scores.jsonl.enc'}", "--config", f"C={self.tmp / 'C.scores.jsonl.enc'}",
                "--judged-results", str(self.tmp / "jres.jsonl.enc"), "--rubrics", str(self.tmp / "jrub.jsonl.enc"),
                "--memory", "C=15", "--memory-limit", "16.7", "--licence", "C=yes",
                "--judged-answers", f"R={self.tmp / 'R.janswers.jsonl.enc'}", "--judged-answers", f"C={self.tmp / 'C.janswers.jsonl.enc'}"]
        o, e = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(o), contextlib.redirect_stderr(e):
            self.assertEqual(cmp.main(args + ["--json-out", str(self.tmp / "v.json")]), 1)
        self.assertIn("a hidden comparison writes a .enc json", e.getvalue())
        self.assertFalse((self.tmp / "v.json").exists())
        rdp = self.tmp / "reading.json.enc"
        eval2.write_atomic(rdp, eval2.encrypt_bytes(json.dumps(self.good_reading("C")).encode()))
        o = io.StringIO()
        with contextlib.redirect_stdout(o):
            self.assertEqual(cmp.main(args + ["--reading-json", str(rdp), "--json-out", str(self.tmp / "v.json.enc")]), 0)
        for i in self.ids[:30] + JIDS:
            self.assertNotIn(i, o.getvalue())
        self.assertIn("overall: C REPLACES R", o.getvalue())
        v = json.loads(eval2.decrypt_bytes((self.tmp / "v.json.enc").read_bytes()))
        self.assertEqual(v["winner"], "C")
        self.assertNotIn(self.ids[0].encode(), (self.tmp / "v.json.enc").read_bytes())

    def test_open_and_hidden_are_never_mixed(self):
        self.run_e5({"C": set()}, None, None, hidden=True)
        eval2.write_jsonl(self.tmp / "C2.scores.jsonl", T.score_rows(self.items, self.base))
        with self.assertRaises(cmp.CompareError):
            cmp.compare_e5({"R": [self.tmp / "R.scores.jsonl.enc"], "C": [self.tmp / "C2.scores.jsonl"]}, [self.tmp / "items.jsonl.enc"], None, "R", ["C"])


class TestScreen(T.Base):
    def setUp(self):
        super().setUp()
        self.items = T.make_items(60)
        self.ids = [i["id"] for i in self.items]

    def files(self, name, passed, answers=None, **kw):
        sp = self.tmp / f"{name}.scores.jsonl"
        eval2.write_jsonl(sp, T.score_rows(self.items, passed))
        out = {"scores": sp}
        if answers is not None:
            ap = self.tmp / f"{name}.ans.jsonl"
            rows = T.answer_rows(self.items, run=name)
            for f in answers:
                f(rows)
            eval2.write_jsonl(ap, rows)
            eval2.write_manifest(ap, {"items_count": len(self.items)})
            out["answers"] = ap
        return out

    def screen(self, cand_pass=None, mutate=(), ref_pass=None, smoke=True, med=None, judged=None, **kw):
        ref_pass = set(self.ids) if ref_pass is None else ref_pass
        cand_pass = set(self.ids) if cand_pass is None else cand_pass
        r = self.files("R", ref_pass)
        c = self.files("C", cand_pass, answers=list(mutate))
        smoke_a = smoke_b = {}
        if smoke:
            a, b = self.tmp / "sa.jsonl", self.tmp / "sb.jsonl"
            rows = T.answer_rows(self.items[:25], run="s")
            eval2.write_jsonl(a, rows)
            rows2 = json.loads(json.dumps(rows))
            if smoke == "differs":
                rows2[3]["text"] += "!"
            if smoke == "differs-late":
                rows2[22]["text"] += "!"            # outside the first 20: not compared
            eval2.write_jsonl(b, rows2)
            smoke_a, smoke_b = {"C": a}, {"C": b}
        medians = {"R": 80.0, "C": 80.0} if med is None else med
        return cmp.screen_e5("R", ["C"], {"R": [r["scores"]], "C": [c["scores"]]}, {"C": c["answers"]}, smoke_a, smoke_b, judged, {},
                             kw.get("s1"), medians)

    def test_a_clean_candidate_is_kept(self):
        r = self.screen()
        x = r["cands"]["C"]
        self.assertEqual(x["criteria"], {"S1": "ok", "S2": "ok", "S3": "not measured", "S4": "ok"})
        self.assertEqual(x["result"], "UNDECIDED")      # S3 needs the judged results
        self.assertEqual(r["go_on"], [])

    def judged_both(self, c_p):
        jr, jb = self.tmp / "jres.jsonl", self.tmp / "jrub.jsonl"
        jitems = [{"id": j} for j in JIDS]
        eval2.write_jsonl(jr, [r for st, ps in (("R", {j: True for j in JIDS}), ("C", c_p)) for r in T.jrows(st, jitems, ps)])
        eval2.write_jsonl(jb, [{"id": j, "must": [{"id": "m1", "text": "t", "weight": 1}], "must_not": []} for j in JIDS])
        return {"results": [jr], "rubrics": jb}

    def test_each_criterion_both_ways(self):
        ok_j = self.judged_both({j: True for j in JIDS})
        r = self.screen(judged=ok_j)
        self.assertEqual(r["cands"]["C"]["result"], "KEEP")
        self.assertEqual(r["go_on"], ["C"])
        # S1: an error, an empty answer, think tags, 6 of 60 cut (10 %: allowed), 7 of 60 cut (above), a differing smoke answer
        def err(rows): rows[0]["error"] = "boom"
        def empty(rows): rows[0]["text"] = "  "
        def think(rows): rows[0]["text"] = "<think>hmm</think> answer"
        def cut(n): return lambda rows: [rows[i].__setitem__("truncated", True) for i in range(n)]
        for m, why in ((err, "errors 1"), (empty, "empty 1"), (think, "reasoning or think tags in 1"), (cut(7), "7 of 60 answers cut")):
            x = self.screen(mutate=[m], judged=ok_j)["cands"]["C"]
            self.assertEqual((x["criteria"]["S1"], x["result"]), ("DROP", "DROP"), why)
            self.assertIn(why, "; ".join(x["values"]["S1"]["reasons"]))
        x = self.screen(mutate=[cut(6)], judged=ok_j)["cands"]["C"]
        self.assertEqual(x["criteria"]["S1"], "ok")
        x = self.screen(smoke="differs", judged=ok_j)["cands"]["C"]
        self.assertEqual(x["criteria"]["S1"], "DROP")
        self.assertIn("1 of 20 smoke answers differ", x["values"]["S1"]["reasons"][0])
        self.assertEqual(self.screen(smoke="differs-late", judged=ok_j)["cands"]["C"]["criteria"]["S1"], "ok")
        self.assertEqual(self.screen(smoke=False, judged=ok_j)["cands"]["C"]["criteria"]["S1"], "not measured")
        # a missing answer (the manifest says 60) counts as an error
        def short(rows): del rows[-1]
        self.assertEqual(self.screen(mutate=[short], judged=ok_j)["cands"]["C"]["criteria"]["S1"], "DROP")
        # S2: -4.0 points is not below -4; -4.17 is
        two = set(self.ids)
        x = self.screen(cand_pass=two - set(self.ids[:3]), ref_pass=two, judged=ok_j)["cands"]["C"]      # -5.0
        self.assertEqual(x["criteria"]["S2"], "DROP")
        x = self.screen(cand_pass=two - set(self.ids[:2]), ref_pass=two, judged=ok_j)["cands"]["C"]      # -3.33
        self.assertEqual(x["criteria"]["S2"], "ok")
        # S3: -0.0833 drops, -0.0 passes
        x = self.screen(judged=self.judged_both({j: n != 0 for n, j in enumerate(JIDS)}))["cands"]["C"]
        self.assertAlmostEqual(x["values"]["S3"]["diff_p"], -1 / 12)
        self.assertEqual(x["criteria"]["S3"], "DROP")
        # S4: 1.15 x 80 = 92 passes, 92.1 drops
        self.assertEqual(self.screen(med={"R": 80.0, "C": 92.0}, judged=ok_j)["cands"]["C"]["criteria"]["S4"], "ok")
        self.assertEqual(self.screen(med={"R": 80.0, "C": 92.1}, judged=ok_j)["cands"]["C"]["criteria"]["S4"], "DROP")
        self.assertEqual(self.screen(med={}, judged=ok_j)["cands"]["C"]["criteria"]["S4"], "not measured")

    def test_s1_on_the_command_line_and_the_source_is_named(self):
        ok_j = self.judged_both({j: True for j in JIDS})
        r = self.screen(judged=ok_j, s1={"C": {"errors": 2}})
        x = r["cands"]["C"]
        self.assertEqual(x["criteria"]["S1"], "DROP")
        self.assertIn("overridden on the command line", x["sources"]["S1"])
        out = io.StringIO()
        cmp.render_screen(r, out)
        self.assertIn("source: answer files, overridden on the command line", out.getvalue())
        # no answer file at all: numbers on the command line only
        sp = self.tmp / "R.scores.jsonl"
        eval2.write_jsonl(sp, T.score_rows(self.items, set(self.ids)))
        r = cmp.screen_e5("R", ["C"], {"R": [sp], "C": [sp]}, {}, {}, {}, ok_j, {}, {"C": {"answers": 60, "errors": 0, "empty": 0, "reasoning": 0, "truncated": 0, "nondeterministic": 0}}, {"R": 80.0, "C": 80.0})
        self.assertEqual(r["cands"]["C"]["result"], "KEEP")
        self.assertTrue(r["cands"]["C"]["sources"]["S1"].startswith("command line"))

    def test_three_candidates_the_two_with_the_best_s2_go_on(self):
        ok_j = self.judged_both({j: True for j in JIDS})
        jr, jb = self.tmp / "j3.jsonl", self.tmp / "j3rub.jsonl"
        jitems = [{"id": j} for j in JIDS]
        eval2.write_jsonl(jr, [r for st in ("R", "C1", "C2", "C3") for r in T.jrows(st, jitems, {j: True for j in JIDS})])
        eval2.write_jsonl(jb, [{"id": j, "must": [{"id": "m1", "text": "t", "weight": 1}], "must_not": []} for j in JIDS])
        scores = {}
        for n, lost in (("R", 0), ("C1", 2), ("C2", 0), ("C3", 1)):
            p = self.tmp / f"{n}.s.jsonl"
            eval2.write_jsonl(p, T.score_rows(self.items, set(self.ids) - set(self.ids[:lost])))
            scores[n] = [p]
        answers, sa, sb = {}, {}, {}
        for n in ("C1", "C2", "C3"):
            a = self.tmp / f"{n}.ans.jsonl"
            eval2.write_jsonl(a, T.answer_rows(self.items, run=n))
            answers[n] = a
            sa[n] = sb[n] = a
        r = cmp.screen_e5("R", ["C1", "C2", "C3"], scores, answers, sa, sb, {"results": [jr], "rubrics": jb}, {}, None,
                          {n: 80.0 for n in ("R", "C1", "C2", "C3")})
        self.assertEqual([x["result"] for x in r["cands"].values()], ["KEEP"] * 3)
        self.assertEqual(r["go_on"], ["C2", "C3"])

    def test_s4_from_answer_files_over_the_same_scenarios(self):
        ok_j = self.judged_both({j: True for j in JIDS})
        def jans(name, sec, ids=JIDS):
            p = self.tmp / f"{name}.ja.jsonl"
            eval2.write_jsonl(p, [{"id": j, "error": None, "seconds": {"total": sec(n)}} for n, j in enumerate(ids)])
            return p
        for c_sec, want in ((92.0, "ok"), (92.5, "DROP")):
            r = self.screen(judged=ok_j, med={})
            sp = self.tmp / "R.scores.jsonl"
            cand = self.files("C2", set(self.ids), answers=[])
            res = cmp.screen_e5("R", ["C"], {"R": [sp], "C": [cand["scores"]]}, {"C": cand["answers"]}, {"C": cand["answers"]}, {"C": cand["answers"]},
                                ok_j, {"R": jans("R", lambda n: 80.0), "C": jans("C", lambda n: c_sec)})
            x = res["cands"]["C"]
            self.assertEqual(x["criteria"]["S4"], want, c_sec)
            self.assertEqual(x["values"]["S4"]["scenarios"], 12)
            self.assertIn("answer files, over the 12 scenarios answered by both", x["sources"]["S4"])
        # a scenario only the candidate answered (and slowly) does not count; one only the reference answered neither
        res = cmp.screen_e5("R", ["C"], {"R": [sp], "C": [cand["scores"]]}, {"C": cand["answers"]}, {"C": cand["answers"]}, {"C": cand["answers"]},
                            ok_j, {"R": jans("R", lambda n: 80.0, JIDS[:11]), "C": jans("C", lambda n: 80.0 if n < 11 else 900.0)})
        self.assertEqual(res["cands"]["C"]["criteria"]["S4"], "ok")
        self.assertEqual(res["cands"]["C"]["values"]["S4"]["scenarios"], 11)

    def test_hidden_input_is_refused(self):
        sp = self.tmp / "R.scores.jsonl.enc"
        eval2.write_jsonl(sp, T.score_rows(self.items, set(self.ids)))
        with self.assertRaises(cmp.CompareError):
            cmp.screen_e5("R", ["C"], {"R": [sp], "C": [sp]})

    def test_command_line(self):
        r, c = self.files("R", set(self.ids)), self.files("C", set(self.ids), answers=[])
        ok_j = self.judged_both({j: True for j in JIDS})
        rows = T.answer_rows(self.items[:25])
        sa = self.tmp / "sa.jsonl"
        eval2.write_jsonl(sa, rows)
        o = io.StringIO()
        with contextlib.redirect_stdout(o):
            rc = cmp.main(["e5-screen", "--candidate", "C", "--scores", f"R={r['scores']}", "--scores", f"C={c['scores']}", "--answers", f"C={c['answers']}",
                           "--smoke-a", f"C={sa}", "--smoke-b", f"C={sa}", "--judged-results", str(ok_j["results"][0]), "--rubrics", str(ok_j["rubrics"]),
                           "--median-s", "R=80", "--median-s", "C=85"])
        self.assertEqual(rc, 0)
        self.assertIn("-- C: KEEP", o.getvalue())
        self.assertIn("go on to the hidden look (the two with the highest S2 estimate among those kept): C", o.getvalue())
        e = io.StringIO()
        with contextlib.redirect_stderr(e):
            self.assertEqual(cmp.main(["e5-screen", "--candidate", "C", "--scores", f"R={r['scores']}", "--s1", "C.bogus=1"]), 1)
        self.assertIn("--s1 takes", e.getvalue())


class TestPickModelId(unittest.TestCase):
    IDS = ["Qwen3.8-27B-4bit", "mlx-community--gemma-4-26b-a4b-it-4bit", "GLM-4.7-Flash-4bit", "granite-4.2-30b-4bit", "mlx-community--granite-4.2-30b-4bit-b"]

    def test_resolution(self):
        self.assertEqual(cmp.pick_model_id(self.IDS, "GLM-4.7-Flash-4bit"), "GLM-4.7-Flash-4bit")
        self.assertEqual(cmp.pick_model_id(self.IDS, "glm-4.7-flash-4BIT"), "GLM-4.7-Flash-4bit")
        self.assertEqual(cmp.pick_model_id(self.IDS, "gemma-4-26b-a4b-it-4bit"), "mlx-community--gemma-4-26b-a4b-it-4bit")
        self.assertEqual(cmp.pick_model_id(self.IDS, "granite-4.2-30b-4bit"), "granite-4.2-30b-4bit")      # exact wins over a longer id
        for bad in ("gemma", "nothing-4bit", "", "Qwen3.8-27B"):
            with self.assertRaises(cmp.CompareError):
                cmp.pick_model_id(self.IDS, bad)
        with self.assertRaises(cmp.CompareError):
            cmp.pick_model_id(["a--x", "b--x"], "x")           # two suffix matches

    def test_the_reference_behaviour_is_unchanged(self):
        self.assertEqual(cmp.pick_model(["Qwen3.8-27B-4bit", "other"], "q4"), "Qwen3.8-27B-4bit")
        self.assertEqual(cmp.pick_model(["Qwen3.8-27B-bf16"], "bf16"), "Qwen3.8-27B-bf16")

    def test_command_line(self):
        import sys as _s
        from unittest import mock
        models = json.dumps({"data": [{"id": i} for i in self.IDS]})
        for args, want, rc in ((["--id", "GLM-4.7-Flash-4bit"], "GLM-4.7-Flash-4bit", 0), (["--kind", "q4"], "Qwen3.8-27B-4bit", 0),
                               (["--id", "nope"], "", 1), ([], "", 1), (["--id", "a", "--kind", "q4"], "", 1)):
            o, e = io.StringIO(), io.StringIO()
            with mock.patch.object(_s, "stdin", io.StringIO(models)), contextlib.redirect_stdout(o), contextlib.redirect_stderr(e):
                self.assertEqual(cmp.main(["pick-model", *args]), rc, args)
            self.assertEqual(o.getvalue().strip(), want)


class TestLedgerRows(unittest.TestCase):
    RULE = "(a) PASS if the lower end of the 90 % interval is above -5 points on the 305 items"

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.f = self.tmp / "l.md"

    def row(self, n, date="2026-11-21", rule=None):
        return f"| {n} | {date} | E{n} | cfg | {rule or self.RULE} | | conductor |\n"

    def test_default_is_rows_one_and_two_as_before(self):
        self.f.write_text(self.row(1) + self.row(2))
        self.assertEqual(mm.ledger_problems(self.f), [])
        self.f.write_text(self.row(1))
        self.assertEqual(mm.ledger_problems(self.f), ["ledger row 2 is missing"])

    def test_row_three_is_required_when_asked(self):
        self.f.write_text(self.row(1) + self.row(2))
        self.assertEqual(mm.ledger_problems(self.f, ("3",)), ["ledger row 3 is missing"])
        self.f.write_text(self.row(1) + self.row(2) + self.row(3, date="<date>", rule="short"))
        self.assertEqual(mm.ledger_problems(self.f, ("3",)), ["ledger row 3 has no date", "ledger row 3 has no decision rule"])
        self.assertEqual(mm.ledger_problems(self.f), [])                       # the default still looks at 1 and 2 only
        self.f.write_text(self.row(1) + self.row(2) + self.row(3))
        self.assertEqual(mm.ledger_problems(self.f, ("3",)), [])
        self.assertEqual(mm.ledger_problems(self.f, ("1", "2", "3")), [])
        self.assertEqual(mm.ledger_problems(self.f, (3,)), [])
        self.assertEqual(mm.ledger_label(("1", "2")), "rows 1 and 2")
        self.assertEqual(mm.ledger_label(("3",)), "row 3")
        self.assertEqual(mm.ledger_label(("1", "2", "3")), "rows 1, 2 and 3")

    def test_command_line_rows(self):
        py = ROOT / ".venv/bin/python"
        if not py.exists():
            self.skipTest("no venv")
        helper = ROOT / "scripts/m4_e2_manifest.py"
        self.f.write_text(self.row(1) + self.row(2))
        run = lambda *a: subprocess.run([str(py), str(helper), "ledger", "--file", str(self.f), *a], capture_output=True, text=True)
        r = run()
        self.assertEqual((r.returncode, r.stdout.strip()), (0, "ledger: rows 1 and 2 present, each with a date and a decision rule"))
        r = run("--rows", "3")
        self.assertEqual((r.returncode, r.stdout.strip()), (1, "ledger row 3 is missing"))
        self.f.write_text(self.row(3))
        r = run("--rows", "3")
        self.assertEqual((r.returncode, r.stdout.strip()), (0, "ledger: row 3 present, each with a date and a decision rule"))


if __name__ == "__main__":
    unittest.main()
