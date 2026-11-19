"""Safe-order generator of the a3 slice (scripts/eval2_items_a3_order.py): every reference passes its checks, every
perturbed order fails cmd_order, the output does not depend on PYTHONHASHSEED, and every ordering constraint cites a
pinned file and line that contain the quoted phrase. Skipped when the catalogues or the pinned raw/ files are absent."""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_items_a3_order as gen  # noqa: E402
import eval2_score as sc  # noqa: E402
from eval2_checks_a3 import check_cmd_order  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, default_dir  # noqa: E402

HAVE = (default_dir() / "CATALOGUES.json").is_file() and (ROOT / "raw" / "apache-kafka").is_dir()
SNIPPET = ("import sys, json; sys.path.insert(0, %r); import eval2_items_a3_order as g; "
           "print(json.dumps(g.generate(), sort_keys=True))" % str(ROOT / "scripts"))


@unittest.skipUnless(HAVE, "catalogues or raw/ not present")
class OrderItems(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cats = Catalogues.load()
        cls.items, cls.stats = gen.generate_with_stats(cls.cats)

    def test_count_and_families(self):
        self.assertEqual(len(self.items), 16, dict(self.stats))
        self.assertGreaterEqual(len({i["family"] for i in self.items}), 8)
        self.assertLessEqual(max(sum(1 for i in self.items if i["template"] == t) for t in {i["template"] for i in self.items}), 10)

    def test_ids_unique_and_shape(self):
        self.assertEqual(len({i["id"] for i in self.items}), len(self.items))
        for i in self.items:
            self.assertEqual((i["tier"], i["kind"], i["anchored"]), ("A", "safe_order", True))
            self.assertTrue(i["id"].startswith("a3-"))
            self.assertIn("cmd_order", [c["type"] for c in i["checks"]])

    def test_reference_passes_every_check(self):
        for i in self.items:
            res = sc.run_checks(i, i["reference"], self.cats)
            self.assertTrue(all(r["pass"] for r in res), (i["id"], res))

    def test_perturbed_orders_fail_cmd_order(self):
        for i in self.items:
            self.assertEqual({p["label"] for p in i["perturbed"]}, {"swap", "missing", "forbidden"})
            for p in i["perturbed"]:
                res = sc.run_checks(i, p["text"], self.cats)
                self.assertFalse(res[p["violates"]]["pass"], (i["id"], p["label"], res[p["violates"]]))
                self.assertEqual(res[p["violates"]]["type"], "cmd_order")
            self.assertEqual(i["perturbed_reference"], i["perturbed"][0]["text"])

    def test_dropping_any_required_step_fails(self):
        for i in self.items:
            chk = next(c for c in i["checks"] if c["type"] == "cmd_order")
            lines = i["reference"].split("\n")[1:-1]
            for k in range(len(lines)):
                kept = lines[:k] + lines[k + 1:]
                text = "```bash\n%s\n```" % "\n".join(kept)
                ok, why = check_cmd_order(chk, text, self.cats)
                if len(lines) == 3 or k == 1 or k == 0:  # safe, mutate (and verify, which the reassignment requires)
                    self.assertFalse(ok, (i["id"], k, why))

    def test_every_constraint_cites_an_existing_line_with_the_phrase(self):
        n = 0
        for i in self.items:
            cited = [s for s in i["sources"] if s["file"]]
            self.assertTrue(cited, i["id"])
            for s in cited:
                ok, why = gen.verify_anchor(s["file"], s["line"], s["fact"])
                self.assertTrue(ok, (i["id"], why))
                n += 1
        self.assertGreaterEqual(n, 16)

    def test_anchor_check_rejects_a_wrong_line(self):
        a = gen.specs()[0]["anchors"][0]
        self.assertFalse(gen.verify_anchor(a["file"], a["line"] + 1, a["phrase"])[0])
        self.assertFalse(gen.verify_anchor(a["file"], 10 ** 7, a["phrase"])[0])
        self.assertFalse(gen.verify_anchor("raw/no-such-file.md", 1, "x")[0])

    def test_derivation_failures_are_counted(self):
        failed = [k for k in self.stats if k.startswith("derivation_failed")]
        self.assertGreaterEqual(len(failed), 3)

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
