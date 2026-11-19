"""Sizing generator of the a3 slice (scripts/eval2_items_a3_sizing.py): every reference passes number_within, every
unit slip and every off-by-a-factor answer fails it, the output does not depend on PYTHONHASHSEED, and every formula
cites a pinned file and line that contain the quoted phrase. Skipped when the catalogues or raw/ are absent."""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_items_a3_order as order  # noqa: E402
import eval2_items_a3_sizing as gen  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, default_dir  # noqa: E402

HAVE = (default_dir() / "CATALOGUES.json").is_file() and (ROOT / "raw" / "apache-kafka").is_dir()
SNIPPET = ("import sys, json; sys.path.insert(0, %r); import eval2_items_a3_sizing as g; "
           "print(json.dumps(g.generate(), sort_keys=True))" % str(ROOT / "scripts"))


@unittest.skipUnless(HAVE, "catalogues or raw/ not present")
class SizingItems(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cats = Catalogues.load()
        cls.items, cls.stats = gen.generate_with_stats(cls.cats)

    def test_count_classes_families(self):
        self.assertEqual(len(self.items), 24, dict(self.stats))
        self.assertEqual({i["failure_class"] for i in self.items}, {"arith_only", "rule_from_docs"})
        self.assertGreaterEqual(len({i["family"] for i in self.items}), 8)
        self.assertEqual(len({i["id"] for i in self.items}), 24)
        for t in {i["template"] for i in self.items}:
            self.assertLessEqual(sum(1 for i in self.items if i["template"] == t), 10)

    def test_reference_passes(self):
        for i in self.items:
            res = sc.run_checks(i, i["reference"], self.cats)
            self.assertTrue(all(r["pass"] for r in res), (i["id"], res))

    def test_every_perturbed_answer_fails_number_within(self):
        for i in self.items:
            kinds = {p["kind"] for p in i["perturbed"]}
            self.assertEqual(kinds, {"unit", "factor"}, i["id"])
            for p in i["perturbed"]:
                res = sc.run_checks(i, p["text"], self.cats)
                r = res[p["violates"]]
                self.assertEqual(r["type"], "number_within")
                self.assertFalse(r["pass"], (i["id"], p["label"], r["detail"]))

    def test_known_unit_slips_fail(self):
        chk = {"type": "number_within", "value": 1024 ** 3, "unit": "bytes", "tolerance": 0}
        for ans in ("1 GB", "1000000000 bytes", "1048576 bytes", "1073741824 KiB", "1073741824 ms"):
            self.assertFalse(sc.CHECKS["number_within"](chk, "ANSWER: " + ans, self.cats)[0], ans)
        for ans in ("1073741824", "1073741824 bytes", "1 GiB", "1048576 KiB"):
            self.assertTrue(sc.CHECKS["number_within"](chk, "ANSWER: " + ans, self.cats)[0], ans)

    def test_tolerance_is_below_the_binary_decimal_gap(self):
        for i in self.items:
            chk = next(c for c in i["checks"] if c["type"] == "number_within")
            self.assertLessEqual(chk["tolerance"], 0.01, i["id"])

    def test_every_formula_cites_an_existing_line_with_the_phrase(self):
        for i in self.items:
            self.assertTrue(i["sources"], i["id"])
            for s in i["sources"]:
                ok, why = order.verify_anchor(s["file"], s["line"], s["fact"])
                self.assertTrue(ok, (i["id"], why))

    def test_a_formula_without_a_passage_is_counted(self):
        self.assertGreaterEqual(len([k for k in self.stats if k.startswith("derivation_failed")]), 2)
        t = dict(gen.TEMPLATES[0], anchor=(gen.TEMPLATES[0]["anchor"][0], 1, "no such phrase"))
        from collections import Counter
        st = Counter()
        self.assertIsNone(gen.build_item(t, t["params"][0], 1, self.cats, "h", "s", __import__("random").Random(1), [], st))
        self.assertEqual(sum(st.values()), 1)

    def test_no_item_is_a_near_duplicate(self):
        for i in self.items:
            self.assertLess(i["dedup"]["max_jaccard"], 0.5, i["id"])
            self.assertLess(i["dedup"]["max_containment"], 0.8, i["id"])

    def test_no_hash_seed_dependence(self):
        outs = []
        for seed in ("0", "1234"):
            env = dict(os.environ, PYTHONHASHSEED=seed)
            r = subprocess.run([sys.executable, "-c", SNIPPET], env=env, capture_output=True, text=True, timeout=300)
            self.assertEqual(r.returncode, 0, r.stderr[-500:])
            outs.append(r.stdout)
        self.assertEqual(outs[0], outs[1])
        self.assertEqual(json.loads(outs[0]), json.loads(json.dumps(self.items)))


if __name__ == "__main__":
    unittest.main()
