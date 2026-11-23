"""M7 pilot chain: the scoring of scripts/m7_pilot_score.py on synthetic rows (a catch, a change on a line no finding names, a quote not found, a new destructive match, each bar's
verdict), the gate's refusals (a missing stored answer file, an answer file without packs, a changed input after the manifest exists) and the refusals of scripts/m7_pilot.sh.
Offline, CPU only; no server, no model. The full dry run of the script is done by hand (it takes about a minute and a half)."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import m7_pilot_score as P  # noqa: E402

SCRIPT = ROOT / "scripts/m7_pilot.sh"
TEXT = "Intro line.\n\nStep one: run `kubectl delete pod x`.\n\n```bash\nkubectl delete pvc data \\\n  --force\n```\n\nThe operator will recreate everything."


def prop(line, accepted=True, quote="kubectl delete pod x", stage1=None, stage2=None):
    return {"line": line, "accepted": accepted, "was": "old", "text": "new", "quote": quote, "quote_found": "pack", "stage1": stage1 or [], "stage2": stage2 or []}


def rec(props, status="accepted", before=None, after=None, secs=10.0, text=TEXT):
    return {"id": "q1", "first_pass_text": text, "proposals": props, "r2": {"status": status}, "seconds": {"total": secs},
            "destructive_before": before or {}, "destructive_after": after}


def finding(quote, counted=True, catchable=False, minor=False):
    return {"arm": "default", "id": "q1", "quote": quote, "kind": "wrong", "minor": minor, "counted": counted, "catchable": catchable}


class ScoreRowTests(unittest.TestCase):
    def test_catch(self):
        s = P.score_row(rec([prop(3)]), [finding("kubectl delete pod x")])
        self.assertEqual((s["accepted"], s["catches"], s["catching_changes"], len(s["unnamed"])), (1, 1, 1, 0))
        self.assertTrue(s["changed"])

    def test_catch_in_a_multi_line_command_unit(self):
        # the finding's quote stands on the first line of the continued command; the change is on its second line: same unit, so a catch
        s = P.score_row(rec([prop(7)]), [finding("kubectl delete pvc data")])
        self.assertEqual(s["catches"], 1)

    def test_change_on_a_line_no_finding_names(self):
        s = P.score_row(rec([prop(10)]), [finding("kubectl delete pod x")])
        self.assertEqual((s["catches"], len(s["unnamed"]), s["unnamed"][0]["minor_finding"]), (0, 1, False))

    def test_change_on_a_line_named_only_by_a_minor_finding(self):
        s = P.score_row(rec([prop(10)]), [finding("kubectl delete pod x"), finding("The operator will recreate everything", counted=False, minor=True)])
        self.assertEqual((s["catches"], len(s["unnamed"]), s["unnamed"][0]["minor_finding"]), (0, 1, True))

    def test_quote_not_found_is_counted_and_left_out(self):
        s = P.score_row(rec([prop(3)]), [finding("a sentence that is not in the draft"), finding("kubectl delete pod x")])
        self.assertEqual((s["findings"], s["findings_found"], s["findings_not_found"], s["catches"]), (2, 1, 1, 1))

    def test_rejected_change_is_no_catch_and_reasons_are_counted(self):
        s = P.score_row(rec([prop(3, accepted=False, stage1=["A3 something"], stage2=["new destructive step"])], status="no_patch"), [finding("kubectl delete pod x")])
        self.assertEqual((s["accepted"], s["rejected"], s["catches"], s["changed"]), (0, 1, 0, False))
        self.assertEqual(dict(s["reasons"]), {"A3": 1, "S2:destructive": 1})

    def test_new_destructive_match_only_when_the_count_went_up_and_a_change_was_accepted(self):
        s = P.score_row(rec([prop(3)], before={"rm": 1}, after={"rm": 2, "dd": 0}), [])
        self.assertEqual(s["new_destructive"], ["rm"])
        s = P.score_row(rec([prop(3)], before={"rm": 1}, after={"rm": 1}), [])
        self.assertEqual(s["new_destructive"], [])
        s = P.score_row(rec([prop(3, accepted=False)], before={}, after={"rm": 1}, status="rejected"), [])
        self.assertEqual(s["new_destructive"], [])

    def test_cut_and_invalid_replies(self):
        self.assertTrue(P.score_row(rec([], status="cut"), [])["cut"])
        self.assertFalse(P.score_row(rec([], status="error"), [])["valid_json"])
        self.assertTrue(P.score_row(rec([], status="no_patch"), [])["valid_json"])

    def test_quote_matching_is_normalised(self):
        self.assertEqual(P.quote_lines("a **bold   move** here\nnext", "a bold move"), {1})
        self.assertIsNone(P.quote_lines("abc def ghi", "zzz"))


def agg(answers=108, catches=0, accepted=0, nd=0, valid=None, cut=0, med=10.0, p90=20.0, ids=()):
    return {"answers": answers, "catches": catches, "accepted": accepted, "new_destructive": nd, "new_destructive_ids": list(ids),
            "valid_json": answers if valid is None else valid, "cut": cut, "not_run": 0, "seconds_median": med, "seconds_p90": p90}


class BarTests(unittest.TestCase):
    def test_bar_1_needs_6_net_on_108_and_waits_for_the_reading(self):
        self.assertEqual(P.bars(agg(), agg(catches=5), True)[1][0], "FAIL")
        self.assertEqual(P.bars(agg(), agg(catches=6), True)[1][0], "needs the reading")
        self.assertEqual(P.bars(agg(), agg(catches=8), True, damage=2)[1][0], "PASS")
        self.assertEqual(P.bars(agg(), agg(catches=8), True, damage=3)[1][0], "FAIL")

    def test_bar_2_damage_at_most_half_and_at_most_5(self):
        self.assertEqual(P.bars(agg(), agg(catches=8), True)[2][0], "needs the reading")
        self.assertEqual(P.bars(agg(), agg(catches=8), True, damage=4)[2][0], "PASS")
        self.assertEqual(P.bars(agg(), agg(catches=8), True, damage=5)[2][0], "FAIL")
        self.assertEqual(P.bars(agg(), agg(catches=12), True, damage=6)[2][0], "FAIL")      # half of 12 is 6, but more than 5 cases
        self.assertEqual(P.bars(agg(), agg(), False)[2][0], "PASS")

    def test_bar_3_destructive(self):
        self.assertEqual(P.bars(agg(nd=1, ids=["rm"]), agg(), True)[3][0], "FAIL")
        self.assertEqual(P.bars(agg(), agg(), True)[3][0], "needs the reading")
        self.assertEqual(P.bars(agg(), agg(), False)[3][0], "PASS")

    def test_bar_4_valid_json_and_cut(self):
        self.assertEqual(P.bars(agg(answers=100, valid=95), agg(), False)[4][0], "PASS")
        self.assertEqual(P.bars(agg(answers=100, valid=94), agg(), False)[4][0], "FAIL")
        self.assertEqual(P.bars(agg(answers=100, cut=5), agg(), False)[4][0], "PASS")
        self.assertEqual(P.bars(agg(answers=100, cut=6), agg(), False)[4][0], "FAIL")

    def test_bar_5_added_seconds(self):
        self.assertEqual(P.bars(agg(med=30.0, p90=45.0), agg(), False)[5][0], "PASS")
        self.assertEqual(P.bars(agg(med=30.1, p90=40.0), agg(), False)[5][0], "FAIL")
        self.assertEqual(P.bars(agg(med=20.0, p90=45.1), agg(), False)[5][0], "FAIL")

    def test_no_verdict_names_a_choice(self):
        for k, (v, txt) in P.bars(agg(), agg(catches=8), True).items():
            self.assertIn(v, ("PASS", "FAIL", "needs the reading"))


class ScoreAllTests(unittest.TestCase):
    """The scoring end to end on a tiny synthetic spec: one set with findings, one without; reader files; no final choice."""

    def test_end_to_end(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            spec = {"sets": [{"name": "S", "items": str(d / "i.jsonl"), "variants": ["V1"], "arms": [{"name": "default", "answers": str(d / "a.jsonl"), "n": 1}],
                              "findings": {"file": str(d / "f.jsonl"), "kind": "s5b", "expect": 1}},
                             {"name": "B2", "items": str(d / "i.jsonl"), "variants": ["V1"], "arms": [{"name": "default", "answers": str(d / "a.jsonl"), "n": 1}], "findings": None}]}
            P.wr(d / "f.jsonl", [{"set": "default", "id": "q1", "quote": "kubectl delete pod x", "kind": "wrong"}])
            (d / "verify").mkdir()
            for name in ("s_default_V1", "b2_default_V1"):
                P.wr(d / "verify" / f"{name}.jsonl", [rec([prop(3), prop(10)])])
            # the thinking drafts run is also planned
            (d / "think").mkdir()
            P.wr(d / "verify/think_think_V1.jsonl", [rec([])])
            res, reader = P.score_all(spec, d)
            v = res["variants"]["V1"]
            self.assertEqual(v["sets"]["S"]["total"]["catches"], 1)
            self.assertEqual(v["sets"]["S"]["total"]["unnamed_accepted"], 1)
            self.assertEqual(v["total"]["answers"], 2)
            kinds = {(r["set"], r["to_read"].split(";")[0]) for r in reader["V1"]}
            self.assertIn(("S", "catch: confirm"), kinds)
            self.assertIn(("S", "line no finding names"), kinds)
            self.assertEqual(sum(1 for r in reader["V1"] if r["set"] == "B2"), 2)           # every accepted change on B2 is read
            self.assertNotIn("choice", json.dumps(res).lower())
            self.assertIn("The code does not choose a variant", P.score_md(res))

    def test_missing_check_output_is_an_error(self):
        spec = {"sets": [{"name": "S", "items": "i", "variants": ["V1"], "arms": [{"name": "default", "answers": "a", "n": 1}], "findings": None}]}
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(P.PilotError):
                P.score_all(spec, d)


class DamageArgTests(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(P.parse_damage("V1=2,V1C=0"), {"V1": 2, "V1C": 0})
        self.assertIsNone(P.parse_damage(None))
        with self.assertRaises(P.PilotError):
            P.parse_damage("V9=1")


def small_spec(d, packs=True, missing=False, n=2):
    d = Path(d)
    rows = [{"id": f"x{i}", "text": "a", "pack": ["c1"] if packs else []} for i in range(n)]
    P.wr(d / "items.jsonl", [{"id": f"x{i}", "question": "q"} for i in range(n)])
    for arm in ("default", "strict"):
        if not (missing and arm == "strict"):
            P.wr(d / f"ans_{arm}.jsonl", rows)
    spec = {"sets": [{"name": "S", "items": str(d / "items.jsonl"), "variants": ["V1"], "think_n": 1,
                      "arms": [{"name": a, "answers": str(d / f"ans_{a}.jsonl"), "n": n} for a in ("default", "strict")], "findings": None}]}
    (d / "spec.json").write_text(json.dumps(spec))
    return d / "spec.json", spec


class GateTests(unittest.TestCase):
    def test_a_good_set_passes(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(P.gate_files(small_spec(d)[1]), [])

    def test_missing_stored_answer_file(self):
        with tempfile.TemporaryDirectory() as d:
            probs = P.gate_files(small_spec(d, missing=True)[1])
            self.assertTrue(any("missing or empty" in p and "strict" in p for p in probs), probs)

    def test_answer_file_without_packs(self):
        with tempfile.TemporaryDirectory() as d:
            probs = P.gate_files(small_spec(d, packs=False)[1])
            self.assertTrue(any("without a pack" in p for p in probs), probs)

    def test_wrong_row_count(self):
        with tempfile.TemporaryDirectory() as d:
            _, spec = small_spec(d)
            spec["sets"][0]["arms"][0]["n"] = 3
            self.assertTrue(any("3 expected" in p for p in P.gate_files(spec)))

    def test_hidden_and_d5_files_are_refused(self):
        for bad in ("eval/hidden/x.jsonl", "work/m6/d5_answers/x.jsonl", "a.jsonl.enc"):
            with self.assertRaises(P.PilotError):
                with tempfile.TemporaryDirectory() as d:
                    p, spec = small_spec(d)
                    spec["sets"][0]["items"] = bad
                    Path(p).write_text(json.dumps(spec))
                    P.load_spec(str(p))

    def test_changed_input_after_the_manifest_exists(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            _, spec = small_spec(d)
            pin = d / "pinned.txt"
            pin.write_text("one")
            args = ["--manifest", str(d / "w/MANIFEST.json"), "--work", str(d / "w"), "--pin", str(pin), "--dry", "--frozen", str(ROOT / "kafka_sre_rag/frozen.json")]
            run = lambda cmd: subprocess.run([sys.executable, str(ROOT / "scripts/m7_pilot_score.py"), cmd] + args, capture_output=True, text=True, cwd=ROOT)
            self.assertEqual(run("start-check").returncode, 0)          # fresh start
            self.assertEqual(run("manifest").returncode, 0)
            self.assertEqual(run("start-check").returncode, 0)
            pin.write_text("two")
            r = run("start-check")
            self.assertEqual(r.returncode, 2)
            self.assertIn("REFUSED", r.stderr)


class PlanTests(unittest.TestCase):
    def test_real_plan_counts_and_think_selection(self):
        runs = P.plan_runs(P.DEFAULT_SPEC, "W")
        self.assertEqual(runs[0]["name"], "THINK_think_V1")
        self.assertEqual(sum(r["expect"] for r in runs if r["variant"] == "V1" and r["set"] != "THINK"), 60 + 48 + 82 + 60)
        self.assertEqual(sum(r["expect"] for r in runs if r["variant"] == "V1C"), 60 + 48 + 82)
        self.assertEqual({r["set"] for r in runs if r["variant"] == "V1C"}, {"B1", "pilot2", "T"})

    def test_think_selection_is_the_first_ten_by_sha256(self):
        import hashlib
        with tempfile.TemporaryDirectory() as d:
            _, spec = small_spec(d, n=5)
            spec["sets"][0]["think_n"] = 2
            ids = P.think_ids(spec)
            want = sorted((f"x{i}" for i in range(5)), key=lambda i: hashlib.sha256(i.encode()).hexdigest())[:2]
            self.assertEqual([i for _, i in ids], want)

    def test_code_pins_exist_and_header_has_the_five_bars(self):
        for p in P.CODE_PINS:
            self.assertTrue((ROOT / p).is_file(), p)
        for k in "12345":
            self.assertIn(f"\n{k}. ", P.HEADER_TEXT)


class ScriptRefusalTests(unittest.TestCase):
    def test_syntax(self):
        self.assertEqual(subprocess.run(["sh", "-n", str(SCRIPT)]).returncode, 0)

    def test_gate_refusal_writes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            spec, _ = small_spec(d, packs=False)
            work = Path(d) / "w"
            r = subprocess.run([str(SCRIPT), "--dry-run", "--work", str(work), "--spec", str(spec)], capture_output=True, text=True, cwd=ROOT)
            self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
            self.assertIn("REFUSED", r.stderr)
            self.assertFalse(work.exists())

    def test_missing_answer_file_refusal(self):
        with tempfile.TemporaryDirectory() as d:
            spec, _ = small_spec(d, missing=True)
            r = subprocess.run([str(SCRIPT), "--dry-run", "--work", str(Path(d) / "w"), "--spec", str(spec)], capture_output=True, text=True, cwd=ROOT)
            self.assertEqual(r.returncode, 2)
            self.assertIn("stored answer file", r.stderr)

    def test_a_dry_run_never_uses_the_real_folders_and_a_real_run_takes_no_spec(self):
        for extra in (["--work", "work/m7/pilot"], ["--report", "runs/m7/M7_PILOT.md"]):
            r = subprocess.run([str(SCRIPT), "--dry-run"] + extra, capture_output=True, text=True, cwd=ROOT)
            self.assertEqual(r.returncode, 2, extra)
        r = subprocess.run([str(SCRIPT), "--spec", "x.json"], capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(r.returncode, 2)


if __name__ == "__main__":
    unittest.main()
