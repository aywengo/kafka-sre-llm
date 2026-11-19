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

    # ---------------------------------------------------------------- WP1c fix F3 (B2, S1, S2): review rewordings
    def item(self, tpl, k=0):
        return [i for i in self.items if i["template"] == tpl][k]

    def passes(self, item, text):
        res = sc.run_checks(item, text, self.cats)
        return all(r["pass"] for r in res), [(r["type"], r["detail"][:80]) for r in res if not r["pass"]]

    def test_changed_templates_have_new_ids(self):
        for t in gen.V2_CHANGED:
            for i in self.items:
                if i["template"] == t:
                    self.assertGreater(int(i["id"].rsplit("-", 1)[1]), 100, i["id"])
        self.assertTrue(all(i["generator"] == "eval2_items_a3_order/2" for i in self.items))

    def test_sg_delete_offsets_follows_the_tool_source(self):
        i = self.item("order_sg_delete_offsets")
        g = "orders-streams"
        t = "bin/kafka-streams-groups.sh --bootstrap-server kafka:9092 "
        # review fixture sgd_r1: describe the offsets, then delete them
        self.assertEqual(self.passes(i, "```bash\n%s--describe --group %s --offsets\n%s--delete-offsets --group %s --all-input-topics\n```" % (t, g, t, g))[0], True)
        # the doc page's form (--dry-run / --execute with --delete-offsets) is rejected by the tool: the item fails it
        for mut in ("--delete-offsets --group %s --all-input-topics --execute" % g, "--delete-offsets --group %s --all-input-topics --dry-run" % g):
            ok, why = self.passes(i, "```bash\n%s--delete-offsets --group %s --all-input-topics --dry-run\n%s%s\n```" % (t, g, t, mut))
            self.assertFalse(ok, mut)
        self.assertFalse(self.passes(i, "```bash\n%s--delete-offsets --group %s --all-input-topics\n```" % (t, g))[0])  # no describe first
        self.assertTrue(any("kafka-streams-group-sh.md" in s["file"] for s in i["sources"] if s["file"]))
        self.assertIn("StreamsGroupCommandOptions.java:60-61, 235-236", i["note"])
        self.assertIn("dry-run", i["note"])

    def test_tool_rule_is_enforced_in_the_generator(self):
        spec = dict(next(x for x in gen.specs() if x["template"] == "order_sg_delete_offsets"))
        spec["safe"] = spec["safe"] + [("--dry-run", None)]
        from collections import Counter
        st = Counter()
        self.assertIsNone(gen.build_item(spec, self.cats, "h", "s", 1, __import__("random").Random(1), [], st))
        self.assertTrue(any("accepted only with --reset-offsets" in k for k in st), dict(st))

    def test_sg_delete_offsets_anchors_hold_the_tool_source_lines(self):
        i = self.item("order_sg_delete_offsets")
        files = {(s["file"].rsplit("/", 1)[-1], s["line"]) for s in i["sources"] if s["file"]}
        self.assertTrue({("StreamsGroupCommandOptions.java", 60), ("StreamsGroupCommandOptions.java", 61),
                         ("StreamsGroupCommandOptions.java", 236)} <= files, files)

    def test_argo_diff_counts_as_the_safe_step_and_prune_false_is_not_pruning(self):
        i = self.item("order_argo_sync", 1)
        a = "web-frontend"
        for text in ("```bash\nargocd app diff %s\nargocd app sync %s\n```" % (a, a),                         # ar_r1
                     "```bash\nargocd app get %s\nargocd app sync %s --dry-run\nargocd app sync %s\n```" % (a, a, a),  # ar_r2
                     "```bash\nargocd app sync %s --dry-run\nargocd app sync %s --prune=false\n```" % (a, a),            # ar_r3
                     "```\n$ argocd app sync %s --dry-run\n$ argocd app sync %s\n```" % (a, a),                          # ar_r4
                     "```bash\nargocd app diff %s --exit-code=false\nargocd app sync %s\n```" % (a, a)):                # ar_r5
            ok, why = self.passes(i, text)
            self.assertTrue(ok, (text, why))
        for text in ("```bash\nargocd app sync %s\n```" % a, "```bash\nargocd app sync %s --prune\nargocd app diff %s\n```" % (a, a),
                     "```bash\nargocd app diff %s\nargocd app sync %s --prune\n```" % (a, a),
                     "```bash\nargocd app sync %s\nargocd app diff %s\n```" % (a, a)):
            self.assertFalse(self.passes(i, text)[0], text)

    def test_reassign_asks_to_generate_the_plan(self):
        i = self.item("order_reassign")
        self.assertIn("Generate the plan and save it as", i["question"])
        self.assertNotIn("is saved in", i["question"])
        bs = "bin/kafka-reassign-partitions.sh --bootstrap-server kafka:9092 "
        # review ra1_r1 (execute, verify, no generate) fails; ra1_r2 (generate, execute, verify) passes
        self.assertFalse(self.passes(i, "```bash\n%s--execute --reassignment-json-file expand-cluster-reassignment.json\n%s--verify --reassignment-json-file expand-cluster-reassignment.json\n```" % (bs, bs))[0])
        self.assertTrue(self.passes(i, "```bash\n%s--topics-to-move-json-file topics-to-move.json --broker-list \"5,6\" --generate\n# save the proposed assignment as expand-cluster-reassignment.json\n%s--reassignment-json-file expand-cluster-reassignment.json --execute\n%s--reassignment-json-file expand-cluster-reassignment.json --verify\n```" % (bs, bs, bs))[0])


if __name__ == "__main__":
    unittest.main()
