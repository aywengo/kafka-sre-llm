"""E5: the sealed reading with three answer sets (the reference and two candidates), the readers' disagreement that no longer nets
signs over the sets, and the JSON hand-over read by `eval2_compare.py e5 --reading-json`. Synthetic rows, a temporary key."""
import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2  # noqa: E402
import eval2_compare as cmp  # noqa: E402
import eval2_sealed_reading as sr  # noqa: E402
import sealed_io  # noqa: E402
import test_eval2_compare as T  # noqa: E402

SECRET = "zebra-sentinel-text"
SETS = ("R", "C1", "C2")


def call(fn, argv, stdin=""):
    o, e = io.StringIO(), io.StringIO()
    with mock.patch.object(sys, "stdin", io.StringIO(stdin)), contextlib.redirect_stdout(o), contextlib.redirect_stderr(e):
        rc = fn(argv)
    return rc, o.getvalue(), e.getvalue()


def jl(rows):
    return "".join(json.dumps(r) + "\n" for r in rows)


@unittest.skipUnless(shutil.which("openssl"), "openssl not on PATH")
class ThreeSets(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.d, True)
        key = self.d / "k"
        eval2.keygen(key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(key)})
        p.start()
        self.addCleanup(p.stop)
        self.inp, self.out = self.d / "in", self.d / "out"
        self.inp.mkdir()

    def build(self, sets=SETS, n_items=20, first=2, second=1):
        items = [{"id": f"zq-{i:03d}", "kind": "a" if i % 2 else "b", "question": f"{SECRET} q {i}", "evidence": "e"} for i in range(n_items)]
        rubrics = [{"id": it["id"], "must": [{"id": "m1", "text": SECRET, "weight": 2}, {"id": "m2", "text": SECRET, "weight": 1}],
                    "must_not": [], "any_of": []} for it in items]
        specs = []
        for s in sets:
            p = self.inp / f"{s}.jsonl.enc"
            eval2.write_jsonl(p, [{"id": it["id"], "text": f"{SECRET} alpha claim {it['id']} {s} beta claim two", "pack": ["c1"]} for it in items])
            specs.append(f"{s}={p}")
        eval2.write_jsonl(self.inp / "items.jsonl.enc", items)
        eval2.write_jsonl(self.inp / "rubrics.jsonl.enc", rubrics)
        eval2.write_jsonl(self.inp / "chunks.jsonl.enc", [{"id": "c1", "path": "x.md", "text": "chunk"}])
        return call(sr.main, ["bundle", "--items", str(self.inp / "items.jsonl.enc"), "--rubrics", str(self.inp / "rubrics.jsonl.enc"),
                              "--answers", *specs, "--chunks", str(self.inp / "chunks.jsonl.enc"), "--out-dir", str(self.out), "--seed", "7",
                              "--first-sessions", str(first), "--second-sessions", str(second), "--second-reader", "0.25"])

    def key(self):
        return json.loads(eval2.decrypt_bytes((self.out / sr.KEY_NAME).read_bytes()))

    def findings(self, bundle, make):
        """make(set_name) -> list of 'wrong' findings (count) for each answer of the bundle."""
        key = self.key()
        rows = []
        for r in sealed_io.load(self.out / f"{bundle}.jsonl.enc"):
            n = make(key["aid"][r["id"]], r["id"])
            if not n:
                rows.append({"answer": r["id"], "id": r["item"], "kind": "clean", "quote": "", "point": "", "supported": False, "minor": False,
                             "source": "", "note": ""})
            for q in range(n):
                rows.append({"answer": r["id"], "id": r["item"], "kind": "wrong", "quote": ["alpha claim", "beta claim two"][q % 2], "point": "",
                             "supported": False, "minor": False, "source": "s", "note": "n"})
        rc, _, e = call(sealed_io.main, ["seal", str(self.out / f"findings_{bundle}.jsonl.enc"), "--schema", "finding"], jl(rows))
        self.assertEqual(rc, 0, e)

    def read(self, first_make, second_make):
        for b in [b for b in self.key()["bundles"] if b.startswith("first_")]:
            self.findings(b, first_make)
        self.findings("second_1", second_make)

    def totals(self, *extra):
        return call(sr.main, ["totals", "--dir", str(self.out), "--default", "R", "--resamples", "300", *extra])

    def test_bundle_has_three_answers_per_item_with_distinct_letters(self):
        rc, out, err = self.build()
        self.assertEqual(rc, 0, err)
        key = self.key()
        self.assertEqual(key["sets"], list(SETS))
        self.assertEqual(len(key["aid"]), 60)
        by_item = {}
        for n, s in key["aid"].items():
            by_item.setdefault(n.rsplit("-", 1)[0], []).append((n.rsplit("-", 1)[1], s))
        for item, v in by_item.items():
            self.assertEqual(sorted(x[1] for x in v), sorted(SETS))
            self.assertEqual(sorted(x[0] for x in v), ["A", "B", "C"])
        for name in key["bundles"]:
            for r in sealed_io.load(self.out / f"{name}.jsonl.enc"):
                self.assertRegex(r["id"], r"^zq-\d{3}-[ABC]$")
                for w in SETS:
                    self.assertNotIn(f" {w} ", " " + json.dumps({k: v for k, v in r.items() if k != "answer"}) + " ")
        self.assertEqual(len(key["bundles"]["second_1"]), 15)        # 5 shared items x 3 answers
        self.assertNotIn("one answer set only", out)

    def test_too_many_sets_are_refused(self):
        rc, _, err = self.build(sets=tuple(f"S{i}" for i in range(9)))
        self.assertEqual(rc, 1)
        self.assertIn("at most 8 answer sets", err)

    def test_pooled_disagreement_does_not_net_signs_over_the_sets(self):
        self.build()
        shared = set(self.key()["second_ids"])
        # first reader: one non-minor wrong on every R answer of the shared items; second reader: one on every C1 answer of them.
        first = lambda s, a: 1 if s == "R" and a.rsplit("-", 1)[0] in shared else 0
        second = lambda s, a: 1 if s == "C1" else 0
        self.read(first, second)
        rc, out, err = self.totals()
        self.assertEqual(rc, 0, err)
        self.assertIn("set R 10.00, set C1 10.00, set C2 0.00", out)
        self.assertIn("pooled 6.67", out)                 # (5 + 5 + 0) per 15 shared answers, times 10
        self.assertIn("mean of the per-set figures 6.67", out)
        self.assertIn("net of signs 0.00 (not used)", out)
        self.assertNotIn(SECRET, out)

    def test_json_carries_pooled_net_and_the_mean_of_per_set_figures(self):
        self.build()
        shared = set(self.key()["second_ids"])
        first = lambda s, a: (2 if s == "R" else 0) if a.rsplit("-", 1)[0] in shared else 0
        second = lambda s, a: 0
        self.read(first, second)
        rc, out, _ = self.totals("--json-out", str(self.d / "t.json.enc"))
        self.assertEqual(rc, 0)
        js = json.loads(eval2.decrypt_bytes((self.d / "t.json.enc").read_bytes()))
        dis = js["disagreement"]
        self.assertAlmostEqual(dis["per_set_per_10"]["R"], 20.0)
        self.assertAlmostEqual(dis["mean_per_set_per_10"], 20.0 / 3)
        self.assertAlmostEqual(js["reader_disagreement_mean_per_set"], 20.0 / 3)
        self.assertAlmostEqual(dis["pooled_per_10"], 10 * 10 / 15)
        self.assertAlmostEqual(dis["pooled_net_per_10"], 10 * 10 / 15)

    def test_json_hand_over_carries_every_set_and_e5_reads_it(self):
        self.build()
        # R: one wrong on every answer of items zq-000..zq-009; C1: none; C2: one on every answer plus a second one on a few (harmful-free)
        low = lambda a: int(a.split("-")[1]) < 10
        make = lambda s, a: (1 if low(a) else 0) if s == "R" else 0 if s == "C1" else (1 if low(a) else 0) + (1 if a.endswith("-A") else 0)
        second_ids = set(self.key()["second_ids"])
        self.read(make, make)
        tj = self.d / "t.json.enc"
        rc, out, err = self.totals("--json-out", str(tj))
        self.assertEqual(rc, 0, err)
        js = json.loads(eval2.decrypt_bytes(tj.read_bytes()))
        self.assertEqual(js["answer_sets"], list(SETS))
        self.assertEqual(set(js["sets"]), set(SETS))
        self.assertIsNone(js["strict_set"])                    # three sets: no "strict"
        self.assertTrue(js["readers_usable"])
        for s in ("C1", "C2"):
            pv = js["sets"][s]["paired_vs_default"]
            self.assertEqual(set(pv["ci_by_k"]), {"1", "2", "3", "4"})
            self.assertEqual(pv["ci_by_k"]["1"], pv["ci90"])
            lo2, hi2 = pv["ci_by_k"]["2"]
            self.assertLessEqual(lo2, pv["ci90"][0])
            self.assertGreaterEqual(hi2, pv["ci90"][1])
            self.assertAlmostEqual(pv["diff"], js["sets"][s]["rate_per_10"] - js["sets"]["R"]["rate_per_10"])
        self.assertNotIn("paired_vs_default", js["sets"]["R"])
        self.assertNotIn("without_tagged", js)                  # no tag file: no such block
        # the e5 rule reads it (hidden input throughout), two candidates: k = 2
        items = T.make_items(40)
        ids = [i["id"] for i in items]
        eval2.write_jsonl(self.d / "items.enc.jsonl.enc", items)
        paths = {}
        for n, lost in (("R", 0), ("C1", 0), ("C2", 5)):
            paths[n] = self.d / f"{n}.scores.jsonl.enc"
            eval2.write_jsonl(paths[n], T.score_rows(items, set(ids) - set(ids[:lost])))
        o, e = io.StringIO(), io.StringIO()
        args = ["e5", "--reference", "R", "--candidate", "C1", "--candidate", "C2", "--items", str(self.d / "items.enc.jsonl.enc"),
                "--resamples", "200", "--reading-json", str(tj), "--memory-limit", "16.7",
                *sum((["--config", f"{n}={p}"] for n, p in paths.items()), [])]
        with contextlib.redirect_stdout(o), contextlib.redirect_stderr(e):
            rc = cmp.main(args)
        self.assertEqual(rc, 0, e.getvalue())
        text = o.getvalue()
        self.assertIn("k = 2", text)
        self.assertIn("95.00 % interval", text)
        self.assertEqual(text.count("Dr non-minor wrong+invented+harmful per 10:"), 2)
        self.assertNotIn("waiting for the reading", text)
        self.assertNotIn("zq-0", text)
        # C1 has fewer non-minor findings than R (rate down by 5 per 10), C2 more
        self.assertLess(js["sets"]["C1"]["paired_vs_default"]["diff"], 0)
        self.assertGreater(js["sets"]["C2"]["paired_vs_default"]["diff"], 0)
        self.assertEqual(second_ids, set(self.key()["second_ids"]))

    def test_readers_not_usable_is_carried_and_e5_does_not_use_the_reading(self):
        self.build()
        shared = set(self.key()["second_ids"])
        self.read(lambda s, a: 2 if a.rsplit("-", 1)[0] in shared else 0, lambda s, a: 0)
        tj = self.d / "t.json.enc"
        self.assertEqual(self.totals("--json-out", str(tj))[0], 0)
        js = json.loads(eval2.decrypt_bytes(tj.read_bytes()))
        self.assertFalse(js["readers_usable"])
        r = cmp.e5_reading_for(js, "R", "C1", 2)
        self.assertEqual(r["state"], "not used")
        self.assertIn("more than 30 %", r["text"])


class TestReadingFor(unittest.TestCase):
    def test_missing_keys_wait_and_the_interval_for_k_must_exist(self):
        self.assertEqual(cmp.e5_reading_for(None, "R", "C", 1)["state"], "waiting")
        self.assertEqual(cmp.e5_reading_for({}, "R", "C", 1)["state"], "waiting")
        rd = {"default_set": "R", "readers_usable": True, "reader_disagreement_mean_per_set": 0.5,
              "sets": {"R": {"rate_per_10": 5, "harmful": 1},
                       "C": {"rate_per_10": 3, "harmful": 0, "paired_vs_default": {"diff": -2, "ci90": [-3, -1], "ci_by_k": {"1": [-3, -1]}}}}}
        self.assertEqual(cmp.e5_reading_for(rd, "R", "C", 1)["state"], "ok")
        r = cmp.e5_reading_for(rd, "R", "C", 2)
        self.assertEqual(r["state"], "waiting")
        self.assertIn("interval for k = 2", r["text"])
        self.assertEqual(cmp.e5_reading_for(dict(rd, default_set="X"), "R", "C", 1)["state"], "waiting")


if __name__ == "__main__":
    unittest.main()
