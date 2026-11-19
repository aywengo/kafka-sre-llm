"""Offline tests of scripts/eval2_blind.py (synthetic data only)."""
import io
import json
import re
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_blind as eb  # noqa: E402

SETS = ("default", "strict", "default_4k")
KINDS = ("diagnostics", "debugging", "architecture")


def wj(path, rows):
    Path(path).write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def rj(path):
    return [json.loads(l) for l in Path(path).read_text().splitlines() if l.strip()]


class Base(unittest.TestCase):
    N = 8

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.ids = [f"t-{i:02d}" for i in range(1, self.N + 1)]
        items, rubs = [], []
        for n, i in enumerate(self.ids):
            items.append({"id": i, "tier": "B", "kind": KINDS[n % 3], "family": i, "domain": "x", "question": f"Why does consumer {n} lag?",
                          "evidence": f"lag line {n}" if n % 2 else None})
            rubs.append({"id": i, "must": [{"id": "m1", "text": f"Names the lag cause {n}", "weight": 2, "first": True, "source": "raw/a.md:1", "chunks": ["c1"]},
                                           {"id": "m2", "text": "Raises the timeout", "weight": 2, "first": False, "source": "raw/a.md:2", "chunks": ["c2"]},
                                           {"id": "m3", "text": "Or restarts", "weight": 1, "first": False, "source": "raw/a.md:3", "not_indexed": True}],
                         "must_not": [{"id": "n1", "text": "Deletes the topic", "severity": "harmful", "condition": "always", "near_miss": "deletes a test topic"}],
                         "any_of": [["m2", "m3"]], "reference": f"Reference text {n}"})
        self.items, self.rubs = self.tmp / "items.jsonl", self.tmp / "rubrics.jsonl"
        wj(self.items, items)
        wj(self.rubs, rubs)
        self.chunks = self.tmp / "chunks.jsonl"
        wj(self.chunks, [{"id": "c1", "path": "raw/a.md", "title": "T", "text": "Chunk one text."}, {"id": "c2", "path": "raw/b.md", "title": "", "text": "Chunk two text."}])
        self.ans = {}
        for s in SETS:
            p = self.tmp / f"ans_{s}.jsonl"
            wj(p, [{"id": i, "text": f"Answer for {i}: raise  the\nfetch timeout, variant {s[-2:]}.", "tokens": 100, "truncated": s == "strict",
                    "seconds": {"answer": 12.5, "retrieval": 3.0}, "pack": ["c1", "c2"], "run": f"b1-{s}-abc", "error": None} for i in self.ids])
            self.ans[s] = p
        self.out, self.key, self.out2 = self.tmp / "bundle1", self.tmp / "keys/key.json", self.tmp / "bundle2"

    def bundle(self, *extra, sets=("default", "strict"), seed="7"):
        argv = ["bundle", "--items", str(self.items), "--rubrics", str(self.rubs), "--answers", *[f"{s}={self.ans[s]}" for s in sets],
                "--chunks", str(self.chunks), "--out", str(self.out), "--key", str(self.key), "--seed", seed, *extra]
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = eb.main(argv)
        return rc, buf.getvalue()


class Bundle(Base):
    def test_files_neutral_names_and_content(self):
        rc, _ = self.bundle()
        self.assertEqual(rc, 0)
        files = sorted(p.name for p in self.out.iterdir())
        self.assertEqual(files, sorted(["README.md", "excerpts"] + [f"{i}.md" for i in self.ids]))
        ex = sorted(p.name for p in (self.out / "excerpts").iterdir())
        self.assertEqual(ex, sorted(f"{i}-{x}.md" for i in self.ids for x in "AB"))
        text = (self.out / "t-02.md").read_text()
        for need in ("Why does consumer 1 lag?", "lag line 1", "Names the lag cause 1", "Deletes the topic", "t-02-A", "t-02-B", "m2 | m3", "Reference text 1",
                     "===== ANSWER t-02-A BEGIN =====", "Chunk one text."[:0]):
            self.assertIn(need, text)
        self.assertIn("Chunk one text.", (self.out / "excerpts/t-02-A.md").read_text())
        key = json.loads(self.key.read_text())
        self.assertEqual(sorted(key["aid"]), sorted(f"{i}-{x}" for i in self.ids for x in "AB"))
        self.assertEqual({key["aid"]["t-01-A"], key["aid"]["t-01-B"]}, {"default", "strict"})

    def test_nothing_reveals_the_set(self):
        self.bundle(sets=SETS, seed="3")
        bad = re.compile(r"default|strict|4096|4,096|2048|2,048|4k|qwen|deepseek|glm|mode|limit|truncat|seconds|tokens|replay|b1-|run\b", re.I)
        for p in self.out.rglob("*"):
            if p.is_file():
                m = bad.search(p.read_text())
                # the answers of this synthetic data say 'variant 4k' / 'ault' / 'ict' by set: remove them before the check
                txt = re.sub(r"variant \w+", "", p.read_text())
                self.assertIsNone(bad.search(txt), f"{p}: {m and m.group(0)}")
        self.assertFalse(any(str(self.key.parent) in str(p) for p in self.out.rglob("*")))

    def test_order_is_drawn_per_item_and_is_reproducible(self):
        self.bundle(sets=SETS, seed="11")
        k1 = json.loads(self.key.read_text())["aid"]
        shutil.rmtree(self.out)
        self.bundle(sets=SETS, seed="11")
        self.assertEqual(k1, json.loads(self.key.read_text())["aid"])
        orders = {tuple(k1[f"{i}-{x}"] for x in "ABC") for i in self.ids}
        self.assertGreater(len(orders), 1)                        # not the same order for every item
        for o in orders:
            self.assertEqual(sorted(o), sorted(SETS))
        shutil.rmtree(self.out)
        self.bundle(sets=SETS, seed="12")
        self.assertNotEqual(k1, json.loads(self.key.read_text())["aid"])

    def test_key_inside_a_bundle_is_refused(self):
        self.key = self.out / "key.json"
        with self.assertRaises(SystemExit) as c:
            self.bundle()
        self.assertIn("must not lie inside", str(c.exception))

    def test_subset_by_id_and_non_empty_folder(self):
        self.bundle("--only", "t-03", "t-05")
        self.assertEqual(sorted(p.name for p in self.out.glob("t-*.md")), ["t-03.md", "t-05.md"])
        with self.assertRaises(SystemExit):
            self.bundle()                                          # the folder is not empty now

    def test_missing_answer_is_refused(self):
        rows = rj(self.ans["strict"])[:-1]
        wj(self.ans["strict"], rows)
        with self.assertRaises(SystemExit) as c:
            self.bundle()
        self.assertIn("no usable answer", str(c.exception))

    def test_second_reader_share_strata_and_shared_key(self):
        rc, text = self.bundle("--second-reader", "0.25", "--second-out", str(self.out2))
        self.assertIn("second bundle", text)
        key = json.loads(self.key.read_text())
        sec = key["second_reader_ids"]
        self.assertGreaterEqual(len(sec), 0.25 * self.N)
        self.assertEqual(sorted(p.stem for p in self.out2.glob("t-*.md")), sec)
        self.assertEqual(sorted(p.stem for p in (self.out2 / "excerpts").iterdir()), sorted(f"{i}-{x}" for i in sec for x in "AB"))
        for i in sec:                                              # the same names and the same text as the first bundle
            self.assertEqual((self.out / f"{i}.md").read_text(), (self.out2 / f"{i}.md").read_text())
        kinds = {it["id"]: it["kind"] for it in rj(self.items)}
        self.assertEqual(len({kinds[i] for i in sec}), min(3, len(sec)))   # stratified: every kind is represented
        # reproducible
        shutil.rmtree(self.out), shutil.rmtree(self.out2)
        self.bundle("--second-reader", "0.25", "--second-out", str(self.out2))
        self.assertEqual(sec, json.loads(self.key.read_text())["second_reader_ids"])

    def test_second_reader_needs_a_folder(self):
        with self.assertRaises(SystemExit):
            self.bundle("--second-reader", "0.25")


class Unblind(Base):
    def setUp(self):
        super().setUp()
        self.bundle("--second-reader", "0.25", "--second-out", str(self.out2))
        self.k = json.loads(self.key.read_text())

    def unblind(self, findings, *extra, allow=False):
        argv = ["unblind", "--key", str(self.key), "--rubrics", str(self.rubs), "--answers", f"default={self.ans['default']}", f"strict={self.ans['strict']}",
                "--findings", *findings, "--out", str(self.tmp / "merged.jsonl"), "--reads-out", str(self.tmp / "reads.jsonl"), *extra]
        if allow:
            argv.append("--allow-rejects")
        buf, err = io.StringIO(), io.StringIO()
        with redirect_stdout(buf), redirect_stderr(err):
            rc = eb.main(argv)
        return rc, buf.getvalue() + err.getvalue()

    def good_rows(self, item=None):
        item = item or self.ids[0]
        a, b = f"{item}-A", f"{item}-B"
        return [{"answer": a, "id": item, "kind": "wrong", "quote": "raise the fetch   timeout", "minor": False, "source": "raw/x.md:1", "note": "n"},
                {"answer": b, "id": item, "kind": "omission", "quote": "", "point": "m1", "supported": False, "minor": False, "source": "s", "note": "n"},
                {"answer": b, "id": item, "kind": "omission", "quote": "", "point": "m2|m3", "supported": True, "minor": False, "source": "s", "note": "n"}]

    def test_round_trip_sets_readers_and_read_markers(self):
        f1 = self.tmp / "f1.jsonl"
        wj(f1, self.good_rows())
        rc, text = self.unblind([f"r1={f1}"])
        self.assertEqual(rc, 0, text)
        merged = rj(self.tmp / "merged.jsonl")
        self.assertEqual(len(merged), 3)
        want = {self.ids[0] + "-A": self.k["aid"][self.ids[0] + "-A"], self.ids[0] + "-B": self.k["aid"][self.ids[0] + "-B"]}
        self.assertEqual(merged[0]["set"], want[self.ids[0] + "-A"])
        self.assertEqual(merged[1]["set"], want[self.ids[0] + "-B"])
        self.assertTrue(all(m["reader"] == "r1" and m["id"] == self.ids[0] for m in merged))
        self.assertEqual((merged[1]["point"], merged[1]["supported"]), ("m1", False))
        self.assertEqual(merged[2]["point"], "m2|m3")
        reads = rj(self.tmp / "reads.jsonl")
        self.assertEqual(len(reads), self.N * 2)                    # every answer, with or without a finding
        self.assertTrue(all(r["kind"] == "read" and r["reader"] == "r1" for r in reads))
        self.assertEqual({(r["set"], r["id"]) for r in reads}, {(s, i) for i in self.ids for s in ("default", "strict")})

    def test_the_report_reads_the_output(self):
        import eval2_pilot_report as pr
        f1 = self.tmp / "f1.jsonl"
        wj(f1, self.good_rows())
        self.assertEqual(self.unblind([f"r1={f1}"])[0], 0)
        readers, rows = pr.reader_table(rj(self.tmp / "merged.jsonl"), rj(self.tmp / "reads.jsonl"))
        self.assertEqual(len(readers), self.N * 2)
        self.assertEqual(sum(len(v) for v in rows.values()), 3)
        units = [{"ids": ["m1"]}, {"ids": ["m2", "m3"]}]
        key = (self.k["aid"][self.ids[0] + "-B"], self.ids[0], "r1")
        mapped, bad = pr.map_omissions(rows[key], units)
        self.assertEqual((sorted(mapped), bad), ([0, 1], []))

    def test_two_readers_and_scope_of_the_second(self):
        sec = self.k["second_reader_ids"]
        f1, f2 = self.tmp / "f1.jsonl", self.tmp / "f2.jsonl"
        wj(f1, self.good_rows(sec[0]))
        wj(f2, self.good_rows(sec[0])[:1])
        rc, text = self.unblind([f"r1={f1}", f"r2={f2}"], "--scope", "r2=second")
        self.assertEqual(rc, 0, text)
        reads = rj(self.tmp / "reads.jsonl")
        self.assertEqual(sum(1 for r in reads if r["reader"] == "r1"), self.N * 2)
        self.assertEqual(sum(1 for r in reads if r["reader"] == "r2"), len(sec) * 2)
        self.assertEqual({m["reader"] for m in rj(self.tmp / "merged.jsonl")}, {"r1", "r2"})

    def test_rows_that_fail_are_listed_and_nothing_is_merged(self):
        sec = set(self.k["second_reader_ids"])
        outside = next(i for i in self.ids if i not in sec)
        g = self.good_rows()
        a = f"{self.ids[0]}-A"
        bad = [
            dict(g[0], answer="nope-A"),                                       # answer not in the key
            dict(g[0], id=self.ids[1]),                                        # id is not the answer's item
            dict(g[0], kind="bogus"),                                          # kind
            dict(g[0], quote="never said in this answer"),                     # quote not found
            dict(g[0], quote=""),                                              # no quote
            dict(g[0], minor="yes"),                                           # type
            dict(g[1], point="m9"),                                            # unknown point
            dict(g[1], point=""),                                              # no point
            dict(g[1], point="m1|m2"),                                         # several, not one any_of group
            {k: v for k, v in g[1].items() if k != "supported"},               # no supported
            g[0], g[0],                                                        # the second is a duplicate
        ]
        f1, f2 = self.tmp / "f1.jsonl", self.tmp / "f2.jsonl"
        wj(f1, bad)
        wj(f2, self.good_rows(outside)[:1])                                    # outside the second reader's scope
        rc, text = self.unblind([f"r1={f1}", f"r2={f2}"], "--scope", "r2=second")
        self.assertEqual(rc, 1)
        self.assertEqual(text.count("REJECTED"), 12)
        for why in ("not in the key", "is not the item", "kind 'bogus'", "quote not found", "no quote", "true or false", "is not a must point",
                    "names no point", "not one any_of group", "needs supported", "duplicate", "not in this reader's bundle"):
            self.assertIn(why, text)
        self.assertFalse((self.tmp / "merged.jsonl").exists())
        rej = rj(self.tmp / "merged.jsonl.rejected.jsonl")
        self.assertEqual(len(rej), 12)
        self.assertEqual(rej[0]["row"]["answer"], "nope-A")
        # with --allow-rejects the valid rows are written and the rejects are still listed
        rc, text = self.unblind([f"r1={f1}"], allow=True)
        self.assertEqual(rc, 0)
        self.assertIn("REJECTED", text)
        self.assertEqual(len(rj(self.tmp / "merged.jsonl")), 1)
        self.assertEqual(a.rsplit("-", 1)[0], self.ids[0])

    def test_whitespace_in_a_quote_is_normalised_but_nothing_else(self):
        f1 = self.tmp / "f1.jsonl"
        wj(f1, [dict(self.good_rows()[0], quote="Answer for t-01:   raise\tthe fetch\n timeout"), dict(self.good_rows()[0], quote="Raise the fetch timeout")])
        rc, text = self.unblind([f"r1={f1}"])
        self.assertEqual(rc, 1)
        self.assertEqual(text.count("REJECTED"), 1)               # the changed capital letter is not the answer's text


if __name__ == "__main__":
    unittest.main()
