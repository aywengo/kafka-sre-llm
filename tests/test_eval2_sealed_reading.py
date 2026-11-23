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
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import eval2_sealed_reading as sr  # noqa: E402
import sealed_io  # noqa: E402

NEEDS_OPENSSL = unittest.skipUnless(shutil.which("openssl"), "openssl not on PATH")
SECRET = "zebra-sentinel-text"


def jl(rows):
    return "".join(json.dumps(r) + "\n" for r in rows)


def call(fn, argv, stdin=""):
    o, e = io.StringIO(), io.StringIO()
    with mock.patch.object(sys, "stdin", io.StringIO(stdin)), contextlib.redirect_stdout(o), contextlib.redirect_stderr(e):
        rc = fn(argv)
    return rc, o.getvalue(), e.getvalue()


@NEEDS_OPENSSL
class SealedReading(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.d, True)
        key = self.d / "k"
        eval2.keygen(key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(key)})
        p.start()
        self.addCleanup(p.stop)
        self.inp = self.d / "in"
        self.inp.mkdir()
        self.out = self.d / "out"

    # ------------------------------------------------------------ helpers
    def build(self, n_items=20, first=2, second=1, frac=0.25):
        items = [{"id": f"zq-{i:03d}", "kind": "a" if i % 2 else "b", "question": f"{SECRET} question {i}",
                  "evidence": f"{SECRET} evidence"} for i in range(n_items)]
        rubrics = [{"id": it["id"], "must": [{"id": "m1", "text": SECRET, "weight": 2},
                                              {"id": "m2", "text": SECRET, "weight": 1},
                                              {"id": "m3", "text": SECRET, "weight": 1}],
                    "must_not": [{"id": "n1", "text": SECRET, "severity": "high"}], "any_of": [["m2", "m3"]]}
                   for it in items]
        sets = ("default", "strict")
        paths = {}
        for s in sets:
            rows = [{"id": it["id"], "text": f"{SECRET} alpha claim {it['id']} {s} beta claim two {it['id']} {s}",
                     "pack": ["c1"]} for it in items]
            paths[s] = self.inp / f"{s}.jsonl.enc"
            eval2.write_jsonl(paths[s], rows)
        eval2.write_jsonl(self.inp / "items.jsonl.enc", items)
        eval2.write_jsonl(self.inp / "rubrics.jsonl.enc", rubrics)
        eval2.write_jsonl(self.inp / "chunks.jsonl.enc", [{"id": "c1", "path": "x.md", "text": f"{SECRET} chunk"}])
        argv = ["bundle", "--items", str(self.inp / "items.jsonl.enc"), "--rubrics", str(self.inp / "rubrics.jsonl.enc"),
                "--answers", f"default={paths['default']}", f"strict={paths['strict']}",
                "--chunks", str(self.inp / "chunks.jsonl.enc"), "--out-dir", str(self.out), "--seed", "7",
                "--first-sessions", str(first), "--second-sessions", str(second), "--second-reader", str(frac)]
        return call(sr.main, argv)

    def key(self):
        return json.loads(eval2.decrypt_bytes((self.out / sr.KEY_NAME).read_bytes()))

    def bundle_rows(self, name):
        return sealed_io.load(self.out / f"{name}.jsonl.enc")

    def write_findings(self, name, make, clean_rows=True):
        """make(row, set_name) -> list of finding dicts (kind, minor, supported optional)."""
        key = self.key()
        rows = []
        for r in self.bundle_rows(name):
            fs = make(r, key["aid"][r["id"]]) or []
            if not fs and clean_rows:
                rows.append({"answer": r["id"], "id": r["item"], "kind": "clean", "quote": "", "point": "",
                             "supported": False, "minor": False, "source": "", "note": ""})
            for f in fs:
                text = r["answer"]
                quote = "alpha claim" if f.get("quote", "q1") == "q1" else "beta claim two"
                if f["kind"] == "omission":
                    row = {"answer": r["id"], "id": r["item"], "kind": "omission", "quote": "", "point": f.get("point", "m1"),
                           "supported": f.get("supported", False), "minor": False, "source": "s", "note": "n"}
                else:
                    row = {"answer": r["id"], "id": r["item"], "kind": f["kind"], "quote": f.get("quote_text", quote),
                           "point": "", "supported": False, "minor": f.get("minor", False), "source": "s", "note": "n"}
                assert text
                rows.append(row)
        if not rows:
            return None
        rc, o, e = call(sealed_io.main, ["seal", str(self.out / f"findings_{name}.jsonl.enc"), "--schema", "finding"], jl(rows))
        self.assertEqual(rc, 0, e)
        return len(rows)

    def totals(self, *extra):
        return call(sr.main, ["totals", "--dir", str(self.out), "--resamples", "200", *extra])

    def first_names(self):
        return [b for b in self.key()["bundles"] if b.startswith("first_")]

    def second_names(self):
        return [b for b in self.key()["bundles"] if b.startswith("second_")]

    # ------------------------------------------------------------ bundle
    def test_bundle_blinding_and_shape(self):
        rc, out, err = self.build()
        self.assertEqual(rc, 0, err)
        self.assertNotIn(SECRET, out)
        key = self.key()
        self.assertEqual(len(key["aid"]), 40)
        self.assertEqual(sorted(key["bundles"]), ["first_1", "first_2", "second_1"])
        self.assertEqual(sum(len(v) for k, v in key["bundles"].items() if k.startswith("first_")), 40)
        self.assertEqual(len(key["second_ids"]), 5)
        self.assertEqual(len(key["bundles"]["second_1"]), 10)
        for name in key["bundles"]:
            blob = (self.out / f"{name}.jsonl.enc").read_bytes()
            self.assertNotIn(SECRET.encode(), blob)
            rows = self.bundle_rows(name)
            self.assertEqual(len(rows), len(key["bundles"][name]))
            for r in rows:
                self.assertEqual(set(r), {"id", "item", "question", "evidence", "points", "reference", "answer", "excerpts"})
                self.assertIn("chunk c1", r["excerpts"])
                self.assertIn("m1", r["points"])
                for word in ("default", "strict"):
                    self.assertNotIn(word, json.dumps({k: v for k, v in r.items() if k != "answer"}))
                self.assertRegex(r["id"], r"^zq-\d{3}-[AB]$")
        # the answer texts name the set in this synthetic data only; the key, not a bundle, holds the mapping
        self.assertEqual(set(key["aid"].values()), {"default", "strict"})
        # both answers of an item lie in one bundle, with one answer per set
        for name, ids in key["bundles"].items():
            by_item = {}
            for n in ids:
                by_item.setdefault(n.rsplit("-", 1)[0], []).append(key["aid"][n])
            self.assertTrue(all(sorted(v) == ["default", "strict"] for v in by_item.values()))
        # second readers: same answer names as the first readers'
        first_names = {n for b in self.first_names() for n in key["bundles"][b]}
        self.assertTrue(set(key["bundles"]["second_1"]) <= first_names)
        # the letters vary over items (not always the same set first)
        letters = {key["aid"][f"{i}-A"] for i in {n.rsplit("-", 1)[0] for n in key["aid"]}}
        self.assertEqual(letters, {"default", "strict"})
        # outputs are ciphertext only
        self.assertEqual({p.name for p in self.out.iterdir()}, {f"{n}.jsonl.enc" for n in key["bundles"]} | {sr.KEY_NAME})

    def test_points_carry_consequence_and_t_kind_only_when_present(self):
        # inputs without the fields: the points text is what render_points gives, nothing more
        rb = {"must": [{"id": "m1", "text": "x", "weight": 1}], "must_not": [{"id": "n1", "text": "y", "severity": "high"}],
              "any_of": []}
        import eval2_blind as eb
        self.assertEqual(sr.extra_points({"id": "i"}, rb), [])
        self.assertEqual("\n".join(eb.render_points(rb) + sr.extra_points({"id": "i"}, rb)), "\n".join(eb.render_points(rb)))
        # T-style inputs: the consequence of each must-not and the t_kind reach the reader
        rb["must_not"][0].update({"consequence": "CONSEQ-SENTINEL", "condition": "c", "near_miss": "nm"})
        txt = "\n".join(sr.extra_points({"id": "i", "t_kind": "destructive_right"}, rb))
        self.assertIn("**n1**: CONSEQ-SENTINEL", txt)
        self.assertIn("destructive_right", txt)
        self.assertIn("t_kind", txt)

    def test_second_stratify_by_field(self):
        items = [{"id": f"zq-{i:03d}", "kind": "a", "t_kind": "r" if i < 4 else "w", "question": "q"} for i in range(20)]
        rubrics = [{"id": it["id"], "must": [{"id": "m1", "text": "x", "weight": 1}], "must_not": [], "any_of": []} for it in items]
        for s in ("p", "q"):
            eval2.write_jsonl(self.inp / f"{s}.jsonl.enc", [{"id": it["id"], "text": f"ans {s}", "pack": []} for it in items])
        eval2.write_jsonl(self.inp / "items.jsonl.enc", items)
        eval2.write_jsonl(self.inp / "rubrics.jsonl.enc", rubrics)
        eval2.write_jsonl(self.inp / "chunks.jsonl.enc", [])
        base = ["bundle", "--items", str(self.inp / "items.jsonl.enc"), "--rubrics", str(self.inp / "rubrics.jsonl.enc"),
                "--answers", f"p={self.inp / 'p.jsonl.enc'}", f"q={self.inp / 'q.jsonl.enc'}", "--chunks", str(self.inp / "chunks.jsonl.enc"),
                "--seed", "7", "--first-sessions", "2", "--second-sessions", "1", "--second-reader", "0.25"]
        rc, _, err = call(sr.main, base + ["--out-dir", str(self.d / "o1"), "--second-stratify-by", "t_kind"])
        self.assertEqual(rc, 0, err)
        key = json.loads(eval2.decrypt_bytes((self.d / "o1" / sr.KEY_NAME).read_bytes()))
        self.assertEqual(len(key["second_ids"]), 5)
        # 4 of 20 items are 'r': a quarter of the draw (1 of 5) is one of them, not decided by the single 'kind'
        self.assertEqual(sum(1 for i in key["second_ids"] if int(i[3:]) < 4), 1)

    def test_bundle_is_deterministic_and_does_not_overwrite(self):
        self.build()
        k1 = self.key()
        rc, _, err = self.build()
        self.assertEqual(rc, 1)
        self.assertIn("exists", err)
        shutil.rmtree(self.out)
        self.build()
        self.assertEqual(self.key(), k1)

    def test_plain_inputs_are_refused(self):
        self.build()
        shutil.rmtree(self.out)
        plain = self.inp / "plain.jsonl"
        plain.write_text("{}\n")
        rc, _, err = call(sr.main, ["bundle", "--items", str(plain), "--rubrics", str(plain), "--answers", f"a={plain}", f"b={plain}",
                                    "--chunks", str(plain), "--out-dir", str(self.out), "--first-sessions", "1",
                                    "--second-sessions", "0"])
        self.assertEqual(rc, 1)
        self.assertIn(".enc", err)

    # ------------------------------------------------------------ totals
    def test_end_to_end_numbers(self):
        self.build()
        key = self.key()

        def make(r, s):
            # default: a wrong (non-minor) on every item with an even number, a minor invented on item 1;
            # strict: one wrong and one harmful everywhere; an omission on every answer, supported on defaults
            i = int(r["item"].split("-")[1])
            out = []
            if s == "default":
                if i % 2 == 0:
                    out.append({"kind": "wrong"})
                if i == 1:
                    out.append({"kind": "invented", "minor": True})
                out.append({"kind": "omission", "point": "m1", "supported": True})
            else:
                out += [{"kind": "wrong"}, {"kind": "harmful", "quote": "q2"}]
                out.append({"kind": "omission", "point": "m2|m3", "supported": False})
            return out

        for b in self.first_names():
            self.write_findings(b, make)
        self.write_findings("second_1", lambda r, s: [])  # nothing written: clean
        rc, out, err = self.totals()
        self.assertEqual(rc, 0, err)
        self.assertIn("set default: answers read 20", out)
        self.assertIn("set strict: answers read 20", out)
        self.assertIn("non-minor 10 = 5.00 per 10", out)      # default: 10 even items
        self.assertIn("minor 1 = 0.50 per 10", out)
        self.assertIn("non-minor 40 = 20.00 per 10", out)     # strict: 2 per answer
        self.assertIn("harmful 20", out)
        self.assertIn("omissions 20 = 10.00 per 10; supported 20", out)
        self.assertIn("omissions 20 = 10.00 per 10; supported 0", out)
        self.assertIn("paired strict minus default", out)
        self.assertIn("+15.00", out)
        self.assertIn("E3: NOT MEASURED (readings not used", out)   # the second reader found nothing
        self.assertNotIn(SECRET, out)
        self.assertEqual(key["sets"], ["default", "strict"])

    def readers(self, first_per_answer, second_count):
        """First readers: one wrong on every answer. Second reader: one wrong on `second_count` of its answers."""
        for b in self.first_names():
            self.write_findings(b, lambda r, s: [{"kind": "wrong"}])
        left = [second_count]

        def make(r, s):
            if left[0] > 0:
                left[0] -= 1
                return [{"kind": "wrong"}]
            return []
        self.write_findings("second_1", make)
        return ()

    def test_thirty_percent_rule_both_ways(self):
        self.build()
        extra = self.readers(1, 7)    # 40 first-reader rows on 40 answers; shared 10 answers: first 10, second 7 -> 30 %
        rc, out, _ = self.totals()
        self.assertIn("readers on 10 shared answers: first 10, second 7", out)
        self.assertIn("difference 30 %", out)
        self.assertIn(": PASS", out)
        shutil.rmtree(self.out)
        self.build()
        extra = self.readers(1, 6)    # 10 vs 6 -> 40 %
        rc, out, _ = self.totals()
        self.assertIn("difference 40 %", out)
        self.assertIn("readings not used", out)
        self.assertTrue(out.splitlines()[-1].startswith("E3: NOT MEASURED (readings not used"), out.splitlines()[-1])
        self.assertNotIn("NOT MET", out)
        shutil.rmtree(self.out)
        self.build()
        extra = self.readers(1, 0)    # second reader found nothing
        rc, out, _ = self.totals()
        self.assertIn("first 10, second 0", out)
        self.assertIn("difference 100 %", out)
        self.assertIn("E3: NOT MEASURED", out)

    def e3_env(self, n_items, errors, harmful=0, second_matches=True):
        self.build(n_items=n_items, first=3, second=1, frac=0.25)
        left = {"n": errors, "h": harmful}

        def make(r, s):
            if s != "default":
                return []
            if left["h"] > 0:
                left["h"] -= 1
                return [{"kind": "harmful"}]
            if left["n"] > 0:
                left["n"] -= 1
                return [{"kind": "invented"}]
            return []
        seen = {}
        for b in self.first_names():
            rows = self.bundle_rows(b)
            self.write_findings(b, make)
            for r in rows:
                seen[r["id"]] = 1
        # the second reader repeats exactly what the first reader wrote on the shared answers
        first = {}
        for b in self.first_names():
            f = self.out / f"findings_{b}.jsonl.enc"
            if f.exists():
                for row in sealed_io.load(f):
                    first.setdefault(row["answer"], []).append(row)
        rows = [dict(row) for r in self.bundle_rows("second_1") for row in first.get(r["id"], [])]
        shared = {r["id"] for r in self.bundle_rows("second_1")}
        rows += [{"answer": n, "id": n.rsplit("-", 1)[0], "kind": "clean", "quote": "", "point": "", "supported": False,
                  "minor": False, "source": "", "note": ""} for n in sorted(shared - {r["answer"] for r in rows})]
        call(sealed_io.main, ["seal", str(self.out / "findings_second_1.jsonl.enc"), "--schema", "finding"], jl(rows))
        return ()

    def test_e3_met(self):
        extra = self.e3_env(60, errors=10)   # 10 non-minor in 60 default answers = 1.67 per 10, harmful 0
        rc, out, err = self.totals()
        self.assertEqual(rc, 0, err)
        self.assertIn("set default: answers read 60", out)
        last = out.splitlines()[-1]
        self.assertIn("rate 1.67 per 10", last)
        self.assertTrue(last.endswith(": MET"), last)

    def test_e3_not_met_on_rate_with_gap(self):
        extra = self.e3_env(60, errors=16)   # 2.67 per 10
        rc, out, _ = self.totals()
        last = out.splitlines()[-1]
        self.assertIn("NOT MET", last)
        self.assertIn("rate +0.67 per 10 above 2.0", last)

    def test_e3_not_met_on_one_harmful(self):
        extra = self.e3_env(60, errors=3, harmful=1)
        rc, out, _ = self.totals()
        last = out.splitlines()[-1]
        self.assertIn("NOT MET", last)
        self.assertIn("harmful 1 (must be 0)", last)

    def test_e3_not_met_with_fewer_than_60_answers(self):
        extra = self.e3_env(40, errors=1)    # clean rate, but 40 answers per set
        rc, out, _ = self.totals()
        last = out.splitlines()[-1]
        self.assertIn("answers 40", last)
        self.assertIn("NOT MET", last)
        self.assertIn("20 answers short of 60", last)

    def test_e3_not_measured_when_readers_disagree(self):
        # first reader writes findings on every shared answer; the second reader reads and finds nothing
        self.build(n_items=60, first=3, second=1)
        for b in self.first_names():
            self.write_findings(b, lambda r, s: [{"kind": "wrong"}])
        self.write_findings("second_1", lambda r, s: [])
        rc, out, _ = self.totals()
        last = out.splitlines()[-1]
        self.assertTrue(last.startswith("E3: NOT MEASURED (readings not used"), last)

    def test_no_second_reader_is_not_measured(self):
        self.build(n_items=60, first=3, second=0)
        for b in self.first_names():
            self.write_findings(b, lambda r, s: [])
        rc, out, err = self.totals()
        self.assertEqual(rc, 0, err)
        self.assertTrue(out.splitlines()[-1].startswith("E3: NOT MEASURED (no second reader"), out)
        self.assertNotIn("NOT MET", out)

    def test_unread_answer_is_an_error_not_a_clean_one(self):
        self.build()
        for b in self.first_names():
            n = [0]

            def make(r, s):
                n[0] += 1
                return [{"kind": "wrong"}] if n[0] == 1 else []
            self.write_findings(b, make, clean_rows=False)   # answers without findings get no clean row
        self.write_findings("second_1", lambda r, s: [])
        rc, out, err = self.totals()
        self.assertEqual(rc, 1)
        self.assertRegex(err, r"bundle first_\d: \d+ of \d+ answers have neither a finding nor a clean row")
        self.assertNotIn("zq-", err)
        self.assertEqual(out, "")

    def test_clean_row_and_finding_on_one_answer_is_an_error(self):
        self.build()
        for b in self.first_names() + self.second_names():
            self.write_findings(b, lambda r, s: [])
        name = self.bundle_rows("first_1")[0]
        rows = sealed_io.load(self.out / "findings_first_1.jsonl.enc")
        rows = [r for r in rows if r["answer"] != name["id"]]
        rows += [{"answer": name["id"], "id": name["item"], "kind": "clean", "quote": "", "point": "", "supported": False,
                  "minor": False, "source": "", "note": ""},
                 {"answer": name["id"], "id": name["item"], "kind": "wrong", "quote": "alpha claim", "point": "",
                  "supported": False, "minor": False, "source": "s", "note": "n"}]
        (self.out / "findings_first_1.jsonl.enc").unlink()
        call(sealed_io.main, ["seal", str(self.out / "findings_first_1.jsonl.enc"), "--schema", "finding"], jl(rows))
        rc, out, err = self.totals()
        self.assertEqual(rc, 1)
        self.assertIn("both a clean row and a finding", err)

    # ------------------------------------------------------------ W3: non-minor rule, agreement, json-out, one set
    def test_e3_ignores_minor_rows_and_reports_them_per_kind(self):
        # a minor harmful row never makes "harmful 0" fail and is not in the rate; it is reported beside, per kind
        self.build(n_items=60, first=3, second=1)

        def make(r, s):
            return [{"kind": "harmful", "minor": True}] if s == "default" else []
        for b in self.first_names():
            self.write_findings(b, make)
        first = {}
        for b in self.first_names():
            for row in sealed_io.load(self.out / f"findings_{b}.jsonl.enc"):
                first.setdefault(row["answer"], []).append(row)
        rows = [dict(row) for r in self.bundle_rows("second_1") for row in first.get(r["id"], [])]
        rows += [{"answer": r["id"], "id": r["item"], "kind": "clean", "quote": "", "point": "", "supported": False,
                  "minor": False, "source": "", "note": ""} for r in self.bundle_rows("second_1") if r["id"] not in first]
        call(sealed_io.main, ["seal", str(self.out / "findings_second_1.jsonl.enc"), "--schema", "finding"], jl(rows))
        rc, out, err = self.totals()
        self.assertEqual(rc, 0, err)
        self.assertIn("minor by kind: wrong 0, invented 0, harmful 60", out)
        self.assertIn("harmful (non-minor) 0", out)
        last = out.splitlines()[-1]
        self.assertIn("rate 0.00 per 10", last)
        self.assertTrue(last.endswith(": MET"), last)

    def test_agreement_counts_minor_rows_and_reports_non_minor_and_per_set(self):
        self.build()
        for b in self.first_names():
            self.write_findings(b, lambda r, s: [{"kind": "wrong"}, {"kind": "invented", "minor": True, "quote": "q2"}])
        # the second reader: one non-minor wrong on every shared answer only
        self.write_findings("second_1", lambda r, s: [{"kind": "wrong"}])
        rc, out, err = self.totals()
        self.assertEqual(rc, 0, err)
        # 10 shared answers: first 20 rows (10 minor), second 10 -> 50 % with minor rows; non-minor 10 vs 10 -> 0 %
        self.assertIn("readers on 10 shared answers: first 20, second 10", out)
        self.assertIn("difference 50 %", out)
        self.assertIn("readings not used", out)
        self.assertIn("non-minor rows only: first 10, second 10, difference 0 %", out)
        self.assertIn("set default: 5 shared answers", out)
        self.assertIn("set strict: 5 shared answers", out)
        self.assertIn("pooled 0.00", out)

    def test_json_out_shape_and_hygiene(self):
        self.build()
        for b in self.first_names():
            self.write_findings(b, lambda r, s: [{"kind": "wrong"}] if s == "default" else [{"kind": "harmful", "quote": "q2"}])
        left = {"n": 7}

        def make(r, s):
            if s == "default" and left["n"] > 0:
                left["n"] -= 1
                return [{"kind": "wrong"}]
            return [{"kind": "harmful", "quote": "q2"}] if s == "strict" else []
        self.write_findings("second_1", make)
        target = self.d / "totals.json"
        rc, out, err = self.totals("--json-out", str(target))
        self.assertEqual(rc, 0, err)
        js = json.loads(target.read_text())
        self.assertEqual(js["default_set"], "default")
        self.assertEqual(js["strict_set"], "strict")
        self.assertEqual(js["sets"]["default"]["answers_read"], 20)
        self.assertEqual(js["sets"]["default"]["rate_per_10"], 10.0)
        self.assertEqual(js["default_rate"], 10.0)
        self.assertEqual(js["strict_rate"], 10.0)
        self.assertEqual((js["default_harmful"], js["strict_harmful"]), (0, 20))
        self.assertEqual(len(js["sets"]["default"]["rate_ci90"]), 2)
        # shared: 5 default answers first 5, second 5 -> equal ...; the second reader found 5 of 5 wrong on default (7 allowed)
        self.assertIn("pooled_per_10", js["disagreement"])
        self.assertEqual(set(js["disagreement"]["per_set_per_10"]), {"default", "strict"})
        self.assertIsInstance(js["readers_usable"], bool)
        self.assertEqual(js["reader_disagreement"], js["disagreement"]["pooled_per_10"])
        self.assertIn(js["e3"]["verdict"], ("MET", "NOT MET", "NOT MEASURED"))
        # the reader of eval2_compare.py takes it
        import eval2_compare as ec
        ns = type("A", (), {"reading_json": str(target), "readers_usable": None, "reading_default_rate": None,
                            "reading_strict_rate": None, "reading_default_harmful": None, "reading_strict_harmful": None,
                            "reading_reader_disagreement": None})()
        rd = ec.reading_from_args(ns)
        state, text = ec.mode_reading_verdict(rd)
        self.assertIn(state, ("met", "not met", "waiting"))
        self.assertNotEqual(text, "waiting for the reading")
        # no id and no text in the json or in the output; refuses to overwrite
        blob = target.read_text()
        for bad in ("zq-", SECRET, "alpha claim"):
            self.assertNotIn(bad, blob)
            self.assertNotIn(bad, out)
        rc, _, err = self.totals("--json-out", str(target))
        self.assertEqual(rc, 1)
        self.assertIn("exists", err)

    def test_json_out_enc_path_is_encrypted(self):
        self.build()
        for b in self.first_names() + self.second_names():
            self.write_findings(b, lambda r, s: [])
        target = self.d / "totals.json.enc"
        rc, out, err = self.totals("--json-out", str(target))
        self.assertEqual(rc, 0, err)
        self.assertNotIn(b"default_rate", target.read_bytes())
        js = json.loads(eval2.read_text(target))
        self.assertEqual(js["e3"]["verdict"], "NOT MET")   # readers agree on 0 findings; 20 < 60 answers
        self.assertEqual(js["readers_usable"], True)

    def test_one_answer_set_bundle(self):
        items = [{"id": f"zq-{i:03d}", "kind": "a", "question": f"{SECRET} q {i}", "evidence": ""} for i in range(8)]
        rubrics = [{"id": it["id"], "must": [{"id": "m1", "text": SECRET, "weight": 1}], "must_not": [], "any_of": []}
                   for it in items]
        eval2.write_jsonl(self.inp / "i.jsonl.enc", items)
        eval2.write_jsonl(self.inp / "r.jsonl.enc", rubrics)
        eval2.write_jsonl(self.inp / "a.jsonl.enc", [{"id": it["id"], "text": f"{SECRET} alpha claim", "pack": ["c1"]} for it in items])
        eval2.write_jsonl(self.inp / "c.jsonl.enc", [{"id": "c1", "path": "x.md", "text": SECRET}])
        rc, out, err = call(sr.main, ["bundle", "--items", str(self.inp / "i.jsonl.enc"), "--rubrics", str(self.inp / "r.jsonl.enc"),
                                      "--answers", f"default={self.inp / 'a.jsonl.enc'}", "--chunks", str(self.inp / "c.jsonl.enc"),
                                      "--out-dir", str(self.out), "--first-sessions", "2", "--second-sessions", "1"])
        self.assertEqual(rc, 0, err)
        self.assertIn("readers know the set", out)
        self.assertEqual(len(self.key()["aid"]), 8)
        self.assertNotIn(SECRET, out)
        self.assertNotIn("zq-", out)
        for b in self.first_names() + self.second_names():
            self.write_findings(b, lambda r, s: [])
        rc, out, err = self.totals("--json-out", str(self.d / "t.json"))
        self.assertEqual(rc, 0, err)
        js = json.loads((self.d / "t.json").read_text())
        self.assertIsNone(js["strict_set"])
        self.assertNotIn("strict_rate", js)

    # ------------------------------------------------------------ validation and output hygiene
    def test_quote_not_found_and_bad_rows_are_not_counted(self):
        self.build()
        for b in self.first_names():
            self.write_findings(b, lambda r, s: [{"kind": "wrong", "quote_text": "no such words"}] if s == "default" else [])
        self.write_findings("second_1", lambda r, s: [])
        rc, out, _ = self.totals()
        self.assertIn("quotes not found in their answer: 20", out)
        self.assertIn("non-minor 0 = 0.00 per 10", out)

    def test_missing_findings_file_is_an_error_naming_the_bundle_only(self):
        self.build()
        rc, out, err = call(sr.main, ["totals", "--dir", str(self.out)])
        self.assertEqual(rc, 1)
        self.assertIn("first_1", err)
        self.assertNotIn("zq-", err)

    def test_no_text_value_or_item_id_in_any_output_and_no_plain_file(self):
        rc1, o1, e1 = self.build()
        self.write_findings(self.first_names()[0], lambda r, s: [{"kind": "wrong"}, {"kind": "omission", "point": "m1"}])
        self.write_findings(self.first_names()[1], lambda r, s: [{"kind": "wrong"}])
        rc2, o2, e2 = self.totals()
        rc3, o3, e3 = call(sr.main, ["totals", "--dir", str(self.d / "nope")])   # error path
        for text in (o1, e1, o2, e2, o3, e3):
            self.assertNotIn(SECRET, text)
            self.assertNotIn("zq-", text)
            self.assertNotIn("alpha claim", text)
        # no plain file anywhere under the temp folder: every file but the key file is ciphertext
        for p in self.d.rglob("*"):
            if p.is_file() and p.name != "k":
                self.assertTrue(p.name.endswith(".enc"), p.name)
                self.assertNotIn(SECRET.encode(), p.read_bytes())

    def test_totals_are_seeded(self):
        self.build()
        for b in self.first_names():
            self.write_findings(b, lambda r, s: [{"kind": "wrong"}] if r["item"] < "zq-007" else [])
        _, o1, _ = self.totals()
        _, o2, _ = self.totals()
        self.assertEqual(o1, o2)

    # ------------------------------------------------------------ tag file
    def tag_file(self, ids, name="exposed"):
        p = self.d / f"tags_{name}.json"
        p.write_text(json.dumps({"tags": {name: {"ids": ids}}}))
        return p

    def tagged_case(self):
        """default: one non-minor wrong on each of the items zq-000 .. zq-003; strict: one harmful on zq-004. Every reader writes
        the same rows, so the second reader agrees with the first."""
        self.build()

        def make(r, s):
            i = int(r["item"].split("-")[1])
            if s == "default" and i < 4:
                return [{"kind": "wrong"}]
            if s == "strict" and i == 4:
                return [{"kind": "harmful", "quote": "q2"}]
            return []
        for b in self.first_names() + self.second_names():
            self.write_findings(b, make)

    def test_tag_file_prints_both_sets_of_totals_and_the_json_carries_both(self):
        self.tagged_case()
        tags = self.tag_file(["zq-000", "zq-001", "zq-002", "zq-003"])
        target = self.d / "totals.json"
        rc, out, err = self.totals("--tag-file", str(tags), "--json-out", str(target))
        self.assertEqual(rc, 0, err)
        head, tail = out.split("\nwithout the items tagged exposed (4 items; 16 remain)\n")
        self.assertIn("set default: answers read 20", head)
        self.assertIn("non-minor 4 = 2.00 per 10", head)
        self.assertIn("answers short of 60", head)
        self.assertNotIn("for information", head)
        self.assertIn("set default: answers read 16", tail)
        self.assertIn("non-minor 0 = 0.00 per 10", tail)
        self.assertIn("harmful (non-minor) 1", tail)          # strict keeps its one
        self.assertIn("for information", tail)
        self.assertIn("bootstrap over 16 items", tail)
        self.assertNotIn("zq-", out)                          # ids of items are not printed, the count is enough
        js = json.loads(target.read_text())
        self.assertEqual(js["default_rate"], 2.0)
        self.assertEqual(js["sets"]["default"]["answers_read"], 20)
        self.assertEqual(js["e3"]["verdict"], "NOT MET")
        self.assertNotIn("for_information", js["e3"])
        w = js["without_tagged"]["exposed"]
        self.assertEqual((w["excluded_items"], w["remaining_items"]), (4, 16))
        self.assertEqual(w["default_rate"], 0.0)
        self.assertEqual(w["sets"]["default"]["answers_read"], 16)
        self.assertEqual(w["strict_rate"], w["sets"]["strict"]["rate_per_10"])
        self.assertEqual(w["sets"]["strict"]["harmful"], 1)
        self.assertEqual(w["sets"]["strict"]["rate_per_10"], 10.0 / 16)
        self.assertTrue(w["e3"]["for_information"])
        self.assertEqual(w["e3"]["verdict"], "NOT MET")     # 16 answers are short of 60 as well
        # the shared answers of the second reader only count items that remain
        self.assertEqual(js["disagreement"]["shared_answers"], 10)
        self.assertLessEqual(w["disagreement"]["shared_answers"], 10)

    def test_tag_file_second_reader_totals_without_the_items(self):
        self.tagged_case()
        key = self.key()
        shared = {n.rsplit("-", 1)[0] for n in key["bundles"]["second_1"]}
        ids = [i for i in sorted(shared)][:2]
        tags = self.tag_file(ids)
        rc, out, err = self.totals("--tag-file", str(tags))
        self.assertEqual(rc, 0, err)
        tail = out.split("\nwithout the items tagged exposed (2 items; 18 remain)\n")[1]
        self.assertIn("readers on %d shared answers" % (len(key["bundles"]["second_1"]) - 4), tail)

    def test_output_is_unchanged_without_the_option(self):
        self.tagged_case()
        _, plain, _ = self.totals()
        rc, with_tags, _ = self.totals("--tag-file", str(self.tag_file(["zq-000"])))
        self.assertEqual(rc, 0)
        self.assertTrue(with_tags.startswith(plain))
        self.assertNotIn("without the items tagged", plain)
        self.assertNotIn("for information", plain)
        target = self.d / "plain.json"
        self.totals("--json-out", str(target))
        self.assertNotIn("without_tagged", json.loads(target.read_text()))

    def test_unknown_tag_id_is_counted_as_not_present(self):
        self.tagged_case()
        rc, out, err = self.totals("--tag-file", str(self.tag_file(["zq-000", "zq-999"])))
        self.assertEqual((rc, err), (0, ""))
        self.assertIn("without the items tagged exposed (1 of 2 tagged items present; 19 remain)", out)
        self.assertNotIn("zq-999", out)
        rc, out, err = self.totals("--tag-file", str(self.tag_file(["zq-998"])))
        self.assertEqual((rc, err), (0, ""))
        self.assertIn("without the items tagged exposed (0 of 1 tagged items present; 20 remain)", out)

    def test_two_tags_give_one_block_each_and_the_union(self):
        self.tagged_case()
        p = self.d / "tags2.json"
        p.write_text(json.dumps({"tags": {"A": {"ids": ["zq-000", "zq-001"]}, "B": {"ids": ["zq-001", "zq-002", "zq-999"]},
                                          "C": {"ids": ["zq-998"]}}}))
        target = self.d / "totals2.json"
        rc, out, err = self.totals("--tag-file", str(p), "--json-out", str(target))
        self.assertEqual((rc, err), (0, ""))
        heads = [l for l in out.splitlines() if l.startswith("without ")]
        self.assertEqual(heads, ["without the items tagged A (2 items; 18 remain)",
                                 "without the items tagged B (2 of 3 tagged items present; 18 remain)",
                                 "without the items tagged C (0 of 1 tagged items present; 20 remain)",
                                 "without the union of all tagged items (3 of 5 tagged items present; 17 remain)"])
        self.assertNotIn("zq-", out)
        w = json.loads(target.read_text())["without_tagged"]
        self.assertEqual(list(w), ["A", "B", "C", "union of all tags"])
        self.assertEqual([(w[k]["excluded_items"], w[k]["tagged_items"], w[k]["remaining_items"]) for k in w],
                         [(2, 2, 18), (2, 3, 18), (0, 1, 20), (3, 5, 17)])

    def test_one_tag_has_no_union_block(self):
        self.tagged_case()
        rc, out, err = self.totals("--tag-file", str(self.tag_file(["zq-000"])))
        self.assertNotIn("union", out)

    def test_bad_tag_file_is_an_error(self):
        self.tagged_case()
        p = self.d / "bad.json"
        p.write_text("{}")
        rc, out, err = self.totals("--tag-file", str(p))
        self.assertEqual(rc, 1)
        self.assertIn("not a tag file", err)


@NEEDS_OPENSSL
class SealedIOFinding(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.d, True)
        key = self.d / "k"
        eval2.keygen(key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(key)})
        p.start()
        self.addCleanup(p.stop)
        self.out = self.d / "f.enc"

    def row(self, **kw):
        r = {"answer": "x1-A", "id": "x1", "kind": "wrong", "quote": SECRET, "point": "", "supported": False,
             "minor": False, "source": "s", "note": "n"}
        r.update(kw)
        return r

    def seal(self, rows, *extra):
        return call(sealed_io.main, ["seal", str(self.out), "--schema", "finding", *extra], jl(rows))

    def test_several_rows_per_item_and_append(self):
        rc, out, _ = self.seal([self.row(), self.row(kind="invented", quote=SECRET + "2"),
                                self.row(kind="omission", quote="", point="m1", supported=True)])
        self.assertEqual(rc, 0)
        self.assertIn("written 3 rows (3 in file)", out)
        self.assertIn("ids x1\n", out)
        rc, out, _ = self.seal([self.row(answer="x1-B", quote=SECRET)], "--append")
        self.assertEqual(rc, 0)
        self.assertIn("(4 in file)", out)
        self.assertEqual(len(sealed_io.load(self.out)), 4)
        self.assertNotIn(SECRET.encode(), self.out.read_bytes())

    def test_invalid_rows_are_refused_without_echoing_values(self):
        for bad, frag in [(self.row(kind="odd"), "'kind'"), (self.row(minor="no"), "'minor'"),
                          (self.row(quote=""), "'quote'"), (self.row(kind="omission", quote="", point=""), "'point'"),
                          (self.row(answer="y1-A"), "'answer'")]:
            rc, out, err = self.seal([bad])
            self.assertEqual(rc, 1)
            self.assertIn(frag, err)
            self.assertFalse(self.out.exists())
        r = self.row()
        del r["note"]
        rc, _, err = self.seal([r])
        self.assertIn("missing field 'note'", err)
        rc, _, err = self.seal([self.row(), self.row()])
        self.assertEqual(rc, 1)
        self.assertIn("duplicate finding", err)
        self.assertNotIn(SECRET, err)

    def test_clean_row(self):
        rc, out, _ = self.seal([self.row(kind="clean", quote="")])
        self.assertEqual(rc, 0)
        rc, _, err = self.seal([self.row(kind="clean", quote="text")], "--append")
        self.assertEqual(rc, 1)
        self.assertIn("clean row with", err)

    def test_duplicate_on_append_is_refused(self):
        self.seal([self.row()])
        rc, _, err = self.seal([self.row()], "--append")
        self.assertEqual(rc, 1)
        self.assertIn("duplicate findings", err)
        self.assertEqual(len(sealed_io.load(self.out)), 1)


if __name__ == "__main__":
    unittest.main()
