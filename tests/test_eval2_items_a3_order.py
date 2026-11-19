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


def gen_fresh(i):
    return gen.FRESH_ID_BASE < int(i["id"].rsplit("-", 1)[1]) < gen.FRESH_ID_BASE + 100


@unittest.skipUnless(HAVE, "catalogues or raw/ not present")
class OrderItems(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cats = Catalogues.load()
        cls.items, cls.stats = gen.generate_with_stats(cls.cats)

    def test_count_and_families(self):
        self.assertEqual(len(self.items), 19, dict(self.stats))   # 16 and the 3 draws of round 2 (N1)
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
            lines = [l for l in i["reference"].split("\n")[1:-1] if not l.startswith("#")]   # a comment line is no step
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
        """The k-th item of the template among the items that are not new draws (sorted by id)."""
        return [i for i in self.items if i["template"] == tpl and not gen_fresh(i)][k]

    def passes(self, item, text):
        res = sc.run_checks(item, text, self.cats)
        return all(r["pass"] for r in res), [(r["type"], r["detail"][:80]) for r in res if not r["pass"]]

    def test_changed_templates_have_new_ids(self):
        for t in gen.V2_CHANGED:
            for i in self.items:
                if i["template"] == t and not gen_fresh(i):
                    self.assertGreater(int(i["id"].rsplit("-", 1)[1]), 100, i["id"])
        for t in gen.V3_CHANGED:      # round 2 (N6, N7): the checks changed again, new numbers from 301
            for i in self.items:
                if i["template"] == t and not gen_fresh(i):
                    self.assertGreater(int(i["id"].rsplit("-", 1)[1]), 300, i["id"])
        self.assertTrue(all(i["generator"] == "eval2_items_a3_order/3" for i in self.items))
        # the items of the other templates keep their version-2 numbers
        self.assertEqual(sorted(i["id"] for i in self.items if i["template"] in ("order_cg_reset", "order_shg_reset", "order_sg_reset", "order_app_reset") and not gen_fresh(i)),
                         ["a3-order_app_reset-001", "a3-order_app_reset-002", "a3-order_cg_reset-001", "a3-order_cg_reset-002",
                          "a3-order_cg_reset-003", "a3-order_sg_reset-001", "a3-order_sg_reset-002", "a3-order_sg_reset-003",
                          "a3-order_shg_reset-001", "a3-order_shg_reset-002"])

    def test_the_new_draws_have_their_own_numbers_and_entries(self):
        fresh = [i for i in self.items if gen_fresh(i)]
        self.assertEqual(sorted(i["template"] for i in fresh), ["order_app_reset", "order_argo_sync", "order_reassign"])
        # revision 2: the fresh order_reassign item has the new id 251 (201 is not reused for changed content)
        self.assertTrue(all(i["id"].endswith("-251" if i["template"] == "order_reassign" else "-201") for i in fresh))
        self.assertEqual(len({i["family"] for i in self.items}), len(self.items))

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

    def test_sg_delete_offsets_review2_rewordings_n6(self):
        i = self.item("order_sg_delete_offsets")
        t = "bin/kafka-streams-groups.sh --bootstrap-server kafka:9092 "
        g = "orders-streams"
        desc, dele = "%s--describe --group %s --offsets" % (t, g), "%s--delete-offsets --group %s --all-input-topics" % (t, g)
        block = lambda *l: "```bash\n%s\n```" % "\n".join(l)  # noqa: E731
        # pass: sgd_r1, the default sub-action (describe without --offsets), a delete scoped by --input-topic
        self.assertTrue(self.passes(i, block(desc, dele))[0])
        self.assertTrue(self.passes(i, block("%s--describe --group %s" % (t, g), dele))[0])                       # sgd_default
        self.assertTrue(self.passes(i, block(desc, "%s--delete-offsets --group %s --input-topic orders --input-topic payments" % (t, g)))[0])  # sgd_topics
        # fail: --delete (the group), --execute, delete before describe, no topic scope, --offsets with --members
        self.assertFalse(self.passes(i, block(desc, "%s--delete --group %s" % (t, g)))[0])                         # sgd_bad_delete
        self.assertFalse(self.passes(i, block(desc, dele + " --execute"))[0])                                        # sgd_bad_exec
        self.assertFalse(self.passes(i, block(dele, desc))[0])                                                        # sgd_bad_order
        self.assertFalse(self.passes(i, block(desc, "%s--delete-offsets --group %s" % (t, g)))[0])                 # sgd_bad_notopic
        self.assertFalse(self.passes(i, block("%s--describe --offsets --members --group %s" % (t, g), dele))[0])   # sgd_bad_mutex
        self.assertFalse(self.passes(i, block("%s--describe --members --group %s" % (t, g), dele))[0])
        # no group on the describe line: the tool refuses it
        self.assertFalse(self.passes(i, block("%s--describe --offsets" % t, dele))[0])

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

    def test_reassign_checks_the_throttle_and_the_reference_saves_the_plan_n7(self):
        i = [x for x in self.items if x["family"].endswith(":throttle")][0]
        self.assertIn("50000000", i["question"])
        bs = "bin/kafka-reassign-partitions.sh --bootstrap-server kafka:9092 "
        gen_ = "%s--topics-to-move-json-file move-orders.json --broker-list \"4,5,6\" --generate" % bs
        ex = "%s--reassignment-json-file bigger-cluster.json --execute" % bs
        ver = "%s--reassignment-json-file bigger-cluster.json --verify" % bs
        block = lambda *l: "```bash\n%s\n```" % "\n".join(l)  # noqa: E731
        self.assertTrue(self.passes(i, block(gen_, "# copy the proposed JSON into bigger-cluster.json", ex + " --throttle 50000000", ver))[0])   # ra_ok1
        self.assertFalse(self.passes(i, block(gen_, ex + " --throttle 5000", ver))[0])     # ra_bad_throttle: the wrong value
        self.assertFalse(self.passes(i, block(gen_, ex, ver))[0])                           # ra_bad_nothrottle
        self.assertFalse(self.passes(i, block(gen_, ex + " --throttle 50000000"))[0])      # ra_bad_noverify
        self.assertFalse(self.passes(i, block(gen_ + " --disable-rack-aware", ex + " --throttle 50000000", ver))[0])   # ra_bad_rack
        # revision 2: --generate prints two JSON blocks, so the reference does not redirect its output into the plan file; a comment line
        # after it says to save the proposed block as the plan file that the execute and verify lines use (DOC_OPS 398)
        ref = i["reference"].split("\n")
        self.assertIn("--generate", ref[1])
        self.assertNotIn(">", ref[1])
        self.assertTrue(ref[2].startswith("# save the") and "Proposed partition reassignment configuration" in ref[2] and ref[2].count("bigger-cluster.json") == 1, ref[2])
        self.assertIn(("raw/apache-kafka/docs/operations/basic-kafka-operations.md", 398), [(x["file"], x["line"]) for x in i["sources"]])
        # the plain item asks for no throttle: none is required, and none is checked
        j = [x for x in self.items if x["family"].endswith(":plain")][0]
        self.assertNotIn("--throttle", j["reference"])
        self.assertNotIn(">", j["reference"].split("\n")[1])
        self.assertIn("expand-cluster-reassignment.json", j["reference"].split("\n")[2])
        for x in self.items:
            if x["template"] == "order_reassign":   # all three (two open, one fresh): no redirect of the generate output, new ids
                self.assertTrue(all(">" not in l for l in x["reference"].split("\n") if "--generate" in l))
                self.assertTrue(all(">" not in t["text"].split("\n")[k] for t in x["perturbed"] for k, l in enumerate(t["text"].split("\n")) if "--generate" in l))
                self.assertIn(int(x["id"].rsplit("-", 1)[1]), (401, 402, 251))
            

    def test_reassign_asks_to_generate_the_plan(self):
        i = [x for x in self.items if x["family"].endswith(":plain")][0]
        self.assertIn("Generate the candidate assignment and save the proposed assignment as", i["question"])
        self.assertNotIn("is saved in", i["question"])
        bs = "bin/kafka-reassign-partitions.sh --bootstrap-server kafka:9092 "
        # review ra1_r1 (execute, verify, no generate) fails; ra1_r2 (generate, execute, verify) passes
        self.assertFalse(self.passes(i, "```bash\n%s--execute --reassignment-json-file expand-cluster-reassignment.json\n%s--verify --reassignment-json-file expand-cluster-reassignment.json\n```" % (bs, bs))[0])
        self.assertTrue(self.passes(i, "```bash\n%s--topics-to-move-json-file topics-to-move.json --broker-list \"5,6\" --generate\n# save the proposed assignment as expand-cluster-reassignment.json\n%s--reassignment-json-file expand-cluster-reassignment.json --execute\n%s--reassignment-json-file expand-cluster-reassignment.json --verify\n```" % (bs, bs, bs))[0])


if __name__ == "__main__":
    unittest.main()
