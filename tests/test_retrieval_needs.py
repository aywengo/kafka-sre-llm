"""scripts/retrieval_needs.py: how needs are derived (synthetic chunks) and the counts of the built dev file."""
import collections
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import retrieval_needs as RN  # noqa: E402

NEEDS_FILE = ROOT / "eval/m4/retrieval_needs_dev.jsonl"


def chunk(cid, path, title="", text="x"):
    return {"id": cid, "path": path, "title": title, "text": text, "source": "s"}


class PointTests(unittest.TestCase):
    def test_statuses(self):
        byid = {"c1": chunk("c1", "raw/a/doc.md"), "c2": chunk("c2", "raw/a/other.md")}
        rub = [{"id": "s1", "covered_chunks": ["c1", "c2"], "must": [
            {"id": "m1", "weight": 2, "source": "raw/a/doc.md:10"},
            {"id": "m2", "weight": 1, "source": "raw/argo-cd/docs/user-guide/commands/argocd_app.md:3"},
            {"id": "m3", "weight": 1, "source": "not covered"},
            {"id": "m4", "weight": 1, "source": "raw/a/third.md:5"},
            {"id": "m5", "weight": 1, "source": "raw_catalogue/apache-kafka-tools/x.java:1"}]}]
        rows = RN.classify_points(rub, byid)
        self.assertEqual([r["status"] for r in rows], ["mapped", "not_indexed", "no_source", "unmapped", "not_indexed"])
        self.assertEqual(rows[0]["chunks"], ["c1"])
        self.assertTrue(all(r["set"] == "dev" for r in rows))


class FactTests(unittest.TestCase):
    def setUp(self):
        f = "raw/k/config.html"
        self.chunks = [chunk("t", f, title="a.b.c"), chunk("i", f, title="other", text="## x.y.z\nrow\n"),
                       chunk("e", "raw/k/protocol_errors.html", text="FOO_ERROR\n12\nTrue\ndesc"),
                       chunk("r", "raw/k/rest.md", title="GET /connectors"),
                       chunk("o", "raw/k/tools.md", text="use --max-messages N"), chunk("n", "raw/k/n.md", text="no")]

    def item(self, cat, entry, f, iid="z"):
        return {"id": iid, "sources": [{"catalogue": cat, "entry": entry, "file": f}]}

    def test_rules(self):
        c = self.chunks
        self.assertEqual(RN.fact_chunks(self.item("kafka_config", "broker/a.b.c", "raw/k/config.html"), c)[0], ["t"])
        self.assertEqual(RN.fact_chunks(self.item("kafka_config", "broker/x.y.z", "raw/k/config.html"), c)[0], ["i"])
        self.assertEqual(RN.fact_chunks(self.item("protocol_errors", "FOO_ERROR", "raw/k/protocol_errors.html"), c)[0], ["e"])
        self.assertEqual(RN.fact_chunks(self.item("connect_rest", "GET /connectors", "raw/k/rest.md"), c)[0], ["r"])
        self.assertEqual(RN.fact_chunks(self.item("argocd", "argocd app", "raw/argo-cd/docs/user-guide/commands/x.md"), c),
                         (None, "not_indexed"))
        self.assertEqual(RN.fact_chunks(self.item("kafka_config", "broker/q", "raw/k/config.html"), c), (None, "unresolved"))
        self.assertEqual(RN.fact_chunks({"id": "z", "sources": [{"absent": True, "catalogue": "kafka_config"}]}, c)[1], "no_positive_fact")

    def test_option_fact_only_when_unique(self):
        RN.OPTION_FACTS["zz"] = "--max-messages"
        try:
            self.assertEqual(RN.fact_chunks({"id": "zz", "sources": [{}]}, self.chunks), (["o"], "located"))
            self.assertEqual(RN.fact_chunks({"id": "zz", "sources": [{}]}, self.chunks + [chunk("o2", "p", text="--max-messages")])[1],
                             "not_unique")
        finally:
            del RN.OPTION_FACTS["zz"]


class BuiltFileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = [json.loads(l) for l in NEEDS_FILE.read_text().splitlines()]

    def test_header_and_set(self):
        self.assertEqual(self.rows[0]["row_type"], "header")
        self.assertIn("development set", self.rows[0]["note"].lower())
        self.assertTrue(all(r["set"] == "dev" for r in self.rows))

    def test_counts_equal_the_plan(self):
        pts = collections.Counter(r["status"] for r in self.rows if r["row_type"] == "point")
        self.assertEqual(dict(pts), {"mapped": 106, "not_indexed": 9, "no_source": 4, "unmapped": 1})
        facts = [r for r in self.rows if r["row_type"] == "fact"]
        self.assertEqual(len(facts), 140)
        self.assertEqual(sum(1 for r in self.rows if r["row_type"] == "scenario"), 24)
        self.assertEqual(sum(len(r["chunks"]) for r in self.rows if r["row_type"] == "scenario"), 119)
        self.assertEqual(sorted(r["item"] for r in facts if "C1" in r["tags"]), sorted(RN.C1))

    def test_mapped_rows_have_chunks(self):
        for r in self.rows[1:]:
            if r["status"] == "mapped":
                self.assertTrue(r["chunks"], r)


class B1Tests(unittest.TestCase):
    def setUp(self):
        if not (ROOT / "runs/v3/INDEX_FROZEN.json").is_file():
            self.skipTest("runs/v3/INDEX_FROZEN.json is not in the repository (it is kept with the run files; no command makes it)")
    def test_build_b1_counts(self):
        rows = RN.build_b1()
        self.assertEqual(len(rows), 184)
        self.assertEqual(sum(1 for r in rows if r["status"] == "not_indexed"), 18)
        self.assertTrue(all(r["status"] == "mapped" and r["chunks"] for r in rows if not r["not_indexed"]))
        self.assertTrue(all(not r["chunks"] for r in rows if r["not_indexed"]))
        g = [r for r in rows if r["item"] == "b1-16" and r["point"] in ("m2", "m6")]
        self.assertEqual([r["any_of"] for r in g], [["m2", "m6"], ["m2", "m6"]])

    def test_write_b1_refuses_overwrite(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "n.jsonl"
            RN.write_b1(RN.build_b1(), out)
            self.assertEqual(len(RN.load_needs(out)), 184)
            with self.assertRaises(SystemExit):
                RN.write_b1([], out)


if __name__ == "__main__":
    unittest.main()
