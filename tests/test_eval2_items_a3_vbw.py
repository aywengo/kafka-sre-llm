"""Tests of scripts/eval2_items_a3_vbw.py (WP1c T5): the valid-but-wrong generator. Offline, no model."""
import copy
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
A3_DIR = ROOT / "work" / "m4" / "catalogues_a3"
HAVE = (ROOT / "work" / "m4" / "catalogues").is_dir() and (A3_DIR / "semantic_rules.json").is_file()

if HAVE:
    import eval2_checks_a3 as c3
    import eval2_items_a3_vbw as g
    import eval2_score as sc
    from kafka_sre_rag.catalogue import Catalogues


def norm(s):
    return re.sub(r"\s+", " ", s).strip()


@unittest.skipUnless(HAVE, "catalogues and catalogues_a3 not built")
class Vbw(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.before = sorted(str(p) for p in (ROOT / "eval").rglob("*"))
        cls.items = g.generate()
        cls.cats = Catalogues.load(None)

    def test_counts(self):
        self.assertEqual(len(g.generate.dropped), 0)
        self.assertEqual(len(self.items), 41)   # 32, minus the dropped V6 fault, plus the 10 draws of round 2 (N1, N2)
        self.assertEqual(len({i["id"] for i in self.items}), len(self.items))
        for t in {i["template"] for i in self.items}:
            n = [i for i in self.items if i["template"] == t]
            self.assertLessEqual(len(n), 10)
            self.assertGreaterEqual(sum(i["vbw"]["role"] == "control" for i in n), 1)
        self.assertGreaterEqual(len({i["family"] for i in self.items}), 8)

    def test_no_file_written(self):
        self.assertEqual(self.before, sorted(str(p) for p in (ROOT / "eval").rglob("*")))

    def test_each_item(self):
        for it in self.items:
            with self.subTest(it["id"]):
                wrong = it["vbw"]["role"] == "wrong"
                lang = it["vbw"]["artifact_language"]
                shown = re.search(r"```%s\n(.*?)\n```" % lang, it["question"], re.S).group(0)
                # half 1: the shown artifact is valid
                if lang == "yaml":
                    docs = list(sc.yaml.safe_load_all(re.search(r"```yaml\n(.*?)```", shown, re.S).group(1)))
                    self.assertEqual(len(docs), 1)
                    r = self.cats.validate_resource(docs[0])
                    self.assertTrue(r.covered and r.ok, [(v.code, v.path) for v in r.violations])
                else:
                    props = c3.parse_properties(re.search(r"```properties\n(.*?)```", shown, re.S).group(1))
                    self.assertTrue(props)
                # half 2: the oracle fails the shown artifact (wrong) or passes it (control)
                oracle = [c for c in it["checks"] if c["type"] in g.ORACLE_TYPES] if wrong else it["vbw"]["oracle_checks_on_control"]
                res = sc.run_checks({"id": it["id"], "checks": oracle}, shown, self.cats)
                self.assertTrue(res)
                if wrong:
                    self.assertTrue(any(not r["pass"] for r in res), res)
                else:
                    self.assertTrue(all(r["pass"] for r in res), res)
                # the control artifact passes
                ctl = "```%s\n%s\n```" % (lang, it["vbw"]["control_artifact"])
                res = sc.run_checks({"id": it["id"], "checks": oracle}, ctl, self.cats)
                self.assertTrue(all(r["pass"] for r in res), res)
                # reference passes everything, perturbed fails the verdict
                self.assertTrue(all(r["pass"] for r in sc.run_checks(it, it["reference"], self.cats)))
                p = sc.run_checks(it, it["perturbed_reference"], self.cats)
                self.assertTrue(p[0]["pass"] and not p[it["perturbed_violates"]]["pass"])
                if wrong:  # the blind answer "works" and the blind answer "does not work" with the shown artifact both fail
                    blind = sc.run_checks(it, shown + "\nANSWER: does not work", self.cats)
                    self.assertFalse(all(r["pass"] for r in blind))
                else:
                    self.assertTrue(all(r["pass"] for r in sc.run_checks(it, "ANSWER: works", self.cats)))

    def test_sources_exist(self):
        for it in self.items:
            self.assertTrue(it["sources"], it["id"])
            for s in it["sources"]:
                with self.subTest(it["id"], source=s["file"], line=s["line"]):
                    f = ROOT / s["file"]
                    self.assertTrue(f.is_file(), s["file"])
                    lines = f.read_text(errors="replace").split("\n")
                    self.assertTrue(1 <= s["line"] <= len(lines))
                    self.assertIn(norm(s["phrase"]), norm(lines[s["line"] - 1]))

    def test_only_anchored_rules(self):
        st = g.mechanism_status()
        for it in self.items:
            m = it["vbw"]["mechanism"]
            if m != "FK":
                self.assertTrue(st[m][0], it["id"])
            for r in it["vbw"]["rules"]:
                self.assertTrue(c3.rule_anchored(r), (it["id"], r))
        used = {i["vbw"]["mechanism"] for i in self.items}
        self.assertNotIn("V4", used)
        self.assertNotIn("V8", used)
        for r in c3.unanchored_rules():
            self.assertFalse(any(r in i["vbw"]["rules"] for i in self.items))

    def test_deterministic_across_hash_seeds(self):
        code = ("import sys; sys.path.insert(0, %r); import eval2_items_a3_vbw as g; print(g.digest(g.generate(dedup=False)))"
                % str(ROOT / "scripts"))
        outs = []
        for seed in ("1", "2718"):
            env = dict(os.environ, PYTHONHASHSEED=seed)
            outs.append(subprocess.run([sys.executable, "-c", code], env=env, cwd=str(ROOT), capture_output=True,
                                       text=True, check=True).stdout.strip())
        self.assertEqual(outs[0], outs[1])
        self.assertEqual(outs[0], g.digest(g.generate(dedup=False)))


@unittest.skipUnless(HAVE, "catalogues and catalogues_a3 not built")
class FixRound2(unittest.TestCase):
    """Review findings B3 (V1, V2, V7), S3, S4 and the pair field: the reviewer's wrong fixes fail, its correct
    rewordings pass."""

    @classmethod
    def setUpClass(cls):
        cls.items = g.generate(dedup=False)
        cls.cats = Catalogues.load(None)

    def of(self, tpl):
        return [i for i in self.items if i["template"] == tpl]

    def legacy(self, tpl):
        """The items of a template that are not revision-3 draws (`-v3-1NN`)."""
        return [i for i in self.of(tpl) if not re.search(r"-v3-1\d\d$", i["id"])]

    def run_oracle(self, it, doc):
        oracle = it["vbw"]["oracle_checks_on_control"]
        res = sc.run_checks({"id": it["id"], "checks": oracle}, "```yaml\n%s\n```" % doc.rstrip("\n"), self.cats)
        return all(r["pass"] for r in res)

    @staticmethod
    def shown(it):
        return re.search(r"```yaml\n(.*?)\n```", it["question"], re.S).group(1)

    def fix_of(self, it):
        return it["vbw"]["control_artifact"]

    def with_from(self, it, frm):
        d = sc.yaml.safe_load(self.fix_of(it))
        d["spec"]["ingress"][0]["from"] = frm
        return sc.yaml.safe_dump(d, sort_keys=False)

    def test_ids_version_and_pairs(self):
        self.assertEqual(g.VERSION, "eval2_items_a3_vbw/3")
        for it in self.items:
            self.assertRegex(it["id"], r"-v[23]-\d+$")
            self.assertTrue(it["pair"].startswith("vbw:"), it["id"])
            self.assertEqual(it["generator"], g.VERSION)
        for tpl in {i["template"] for i in self.items}:
            items = self.of(tpl)
            for c in [i for i in items if i["vbw"]["role"] == "control"]:
                self.assertTrue(any(w["pair"] == c["pair"] and w["vbw"]["role"] == "wrong" for w in items), c["id"])
        for tpl in ("vbw_netpol_from", "vbw_restart_annotation", "vbw_default_value", "vbw_pdb_no_eviction"):
            self.assertEqual(len({i["pair"] for i in self.of(tpl)}), 3, tpl)  # a, b and the new pair c of round 2
        for tpl in ("vbw_heartbeat_session", "vbw_min_isr_replicas"):
            self.assertEqual(len({i["pair"] for i in self.of(tpl)}), 2, tpl)

    def test_round_2_every_template_has_two_controls_in_two_groups_n2(self):
        """A control on each side needs two groups that each hold a control (the split puts a group on one side)."""
        for tpl in ("vbw_netpol_from", "vbw_restart_annotation", "vbw_default_value", "vbw_heartbeat_session",
                    "vbw_min_isr_replicas", "vbw_pdb_no_eviction", "vbw_forbidden_key"):
            ctl_pairs = {i["pair"] for i in self.of(tpl) if i["vbw"]["role"] == "control"}
            self.assertGreaterEqual(len(ctl_pairs), 2, tpl)
            for c in [i for i in self.of(tpl) if i["vbw"]["role"] == "control"]:
                self.assertTrue(any(w["pair"] == c["pair"] and w["vbw"]["role"] == "wrong" for w in self.of(tpl)), c["id"])

    def test_round_2_the_draws_are_new_entries_and_one_old_fault_is_dropped(self):
        v3 = [i for i in self.items if "-v3-" in i["id"] and not re.search(r"-v3-1\d\d$", i["id"])]
        # revision 3 replaced the version-3 drafts of V1, V2 and V7 (published variants) by `-v3-1NN` draws
        self.assertEqual(sorted(i["template"] for i in v3), sorted(
            ["vbw_default_value"] * 2 + ["vbw_heartbeat_session", "vbw_min_isr_replicas"]))
        self.assertEqual(len({i["family"] for i in self.items}), len(self.items))
        # the dropped fault keeps its number free: version-2 ids stay what they were
        self.assertNotIn("a3-vbw_min_isr_replicas-v2-003", {i["id"] for i in self.items})
        self.assertIn("a3-vbw_min_isr_replicas-v2-004", {i["id"] for i in self.items})
        # the new V6 control joins the pair of the kept fault; the new V5 control the pair of the two published faults
        self.assertEqual({i["pair"] for i in v3 if i["template"] == "vbw_min_isr_replicas"}, {"vbw:V6:b"})
        self.assertEqual({i["pair"] for i in v3 if i["template"] == "vbw_heartbeat_session"}, {"vbw:V5:b"})
        for i in v3:
            self.assertEqual(i["generator"], g.VERSION)

    def test_round_2_n8_the_plugin_version_item_carries_a_note(self):
        it = next(i for i in self.items if i["family"].endswith("KafkaConnector:connector.plugin.version"))
        self.assertIn("spec.version", it["note"])
        self.assertEqual(sum(1 for i in self.items if "note" in i), 1)

    def test_v1_four_probes_and_the_reviewers_wrong_fixes(self):
        for it in self.legacy("vbw_netpol_from"):
            probes = [c for c in it["vbw"]["oracle_checks_on_control"] if c["oracle"] == "netpol_allows"]
            self.assertEqual([c["expect"] for c in probes], [True, False, False, False])
            self.assertEqual(len({(c["source"]["namespace"], str(c["source"]["pod_labels"])) for c in probes}), 4)
            self.assertTrue(self.run_oracle(it, self.fix_of(it)), it["id"])
            ctl = sc.yaml.safe_load(self.fix_of(it))["spec"]["ingress"][0]["from"][0]
            ns_only = {"namespaceSelector": ctl["namespaceSelector"]}
            pod_only = {"podSelector": ctl["podSelector"]}
            both = {**ctl}
            # namespace-only selector admits every pod of the namespace; pod-only reaches the policy's own namespace
            self.assertFalse(self.run_oracle(it, self.with_from(it, [ns_only])), it["id"])
            self.assertFalse(self.run_oracle(it, self.with_from(it, [pod_only])), it["id"])
            # a second entry for the policy's own namespace (the reviewer's wrong fix for namespace `analytics`)
            self.assertFalse(self.run_oracle(it, self.with_from(it, [both, pod_only])), it["id"])
            self.assertFalse(self.run_oracle(it, self.with_from(it, [ns_only, pod_only])), it["id"])
            # correct rewording: the same AND with matchExpressions in the namespace selector
            ns = ctl["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
            alt = {"namespaceSelector": {"matchExpressions": [{"key": "kubernetes.io/metadata.name", "operator": "In",
                                                                "values": [ns]}]}, "podSelector": ctl["podSelector"]}
            self.assertTrue(self.run_oracle(it, self.with_from(it, [alt])), it["id"])

    def test_v2_expect_states_all_three_flags(self):
        for it in self.legacy("vbw_restart_annotation"):
            for c in it["vbw"]["oracle_checks_on_control"]:
                self.assertTrue({"restart_connector", "include_tasks", "only_failed"} <= set(c["expect"]), it["id"])

    def test_v2_reviewers_wrong_fixes(self):
        conns = [i for i in self.of("vbw_restart_annotation") if i["vbw"]["role"] == "wrong"
                 and "KafkaConnector" in i["family"]]
        tasks_item = next(i for i in self.of("vbw_restart_annotation") if i["vbw"]["role"] == "wrong"
                          and "restart_connector" in i["vbw"]["oracle_checks_on_control"][0]["expect"]
                          and i["vbw"]["oracle_checks_on_control"][0]["expect"]["include_tasks"] and "KafkaConnector" in i["family"])
        only_item = next(i for i in conns if i["vbw"]["oracle_checks_on_control"][0]["expect"]["only_failed"])

        def conn(it, value):
            d = sc.yaml.safe_load(self.fix_of(it))
            d["metadata"]["annotations"]["strimzi.io/restart"] = value
            return sc.yaml.safe_dump(d, sort_keys=False)
        # "restart the connector and all of its tasks": includeTasks,onlyFailed is too narrow
        self.assertTrue(self.run_oracle(tasks_item, conn(tasks_item, "includeTasks")))
        self.assertFalse(self.run_oracle(tasks_item, conn(tasks_item, "includeTasks,onlyFailed")))
        self.assertFalse(self.run_oracle(tasks_item, conn(tasks_item, "true")))
        # "restart only if FAILED": the combined value also restarts the tasks
        self.assertTrue(self.run_oracle(only_item, conn(only_item, "onlyFailed")))
        self.assertFalse(self.run_oracle(only_item, conn(only_item, "includeTasks,onlyFailed")))
        # MirrorMaker 2: the broad form fails, includeTasks passes
        mm2 = next(i for i in self.of("vbw_restart_annotation") if "KafkaMirrorMaker2" in i["family"]
                   and i["vbw"]["role"] == "wrong")
        d = sc.yaml.safe_load(self.fix_of(mm2))
        ann = d["metadata"]["annotations"]["strimzi.io/restart-connector"]
        self.assertTrue(ann.endswith(":includeTasks"))
        self.assertTrue(self.run_oracle(mm2, self.fix_of(mm2)))
        d["metadata"]["annotations"]["strimzi.io/restart-connector"] = ann + ",onlyFailed"
        self.assertFalse(self.run_oracle(mm2, sc.yaml.safe_dump(d, sort_keys=False)))

    # ---- revision 3 (Owner's decision L7): variants that were never published
    R3 = {"vbw_netpol_from": 2, "vbw_restart_annotation": 2, "vbw_pdb_no_eviction": 2, "vbw_min_isr_replicas": 1}

    def r3(self, tpl):
        return [i for i in self.of(tpl) if re.search(r"-v3-1\d\d$", i["id"])]

    def test_r3_draws_exist_and_replace_the_published_variant_drafts(self):
        ids = {i["id"] for i in self.items}
        for tpl, n in self.R3.items():
            self.assertEqual(len(self.r3(tpl)), n, tpl)
        for gone in ("a3-vbw_netpol_from-v3-001", "a3-vbw_netpol_from-v3-002", "a3-vbw_restart_annotation-v3-001",
                     "a3-vbw_restart_annotation-v3-002", "a3-vbw_pdb_no_eviction-v3-001", "a3-vbw_pdb_no_eviction-v3-002",
                     "a3-vbw_min_isr_replicas-v2-002", "a3-vbw_min_isr_replicas-v3-101", "a3-vbw_restart_annotation-v3-101"):
            self.assertNotIn(gone, ids)
        self.assertIn("a3-vbw_min_isr_replicas-v3-001", ids)   # the hidden control, unchanged
        self.assertEqual(len(self.items), 41)

    def test_r3_variants_differ_from_the_published_ones(self):
        import eval2_items_a3 as asm
        rec = json.loads((ROOT / asm.PUBLISHED_PATH).read_text())
        pub = {i["variant"] for v in rec["versions"] for i in v["items"] if i.get("variant")}
        for tpl in self.R3:
            for it in self.r3(tpl):
                self.assertNotIn(asm.variant_key(it), pub, it["id"])

    def test_r3_references_pass_and_perturbations_fail(self):
        for tpl in self.R3:
            for it in self.r3(tpl):
                ref = sc.run_checks(it, it["reference"], self.cats)
                self.assertTrue(all(r["pass"] for r in ref), it["id"])
                pert = sc.run_checks(it, it["perturbed_reference"], self.cats)
                self.assertFalse(all(r["pass"] for r in pert), it["id"])
                self.assertTrue(all(s["file"] and s["line"] for s in it["sources"]), it["id"])

    def test_r3_netpol_narrow_and_its_wrong_fixes(self):
        w = next(i for i in self.r3("vbw_netpol_from") if i["vbw"]["role"] == "wrong")
        c = next(i for i in self.r3("vbw_netpol_from") if i["vbw"]["role"] == "control")
        d = sc.yaml.safe_load(self.fix_of(w))
        ns = d["spec"]["ingress"][0]["from"][0]
        self.assertEqual(list(ns), ["namespaceSelector"])
        self.assertTrue(self.run_oracle(w, self.fix_of(w)))
        self.assertTrue(self.run_oracle(w, c["vbw"]["control_artifact"]))
        both = {**ns, "podSelector": {"matchLabels": {"app": "fx-rates"}}}
        self.assertFalse(self.run_oracle(w, self.with_from(w, [both])))              # the shown fault
        self.assertFalse(self.run_oracle(w, self.with_from(w, [{"podSelector": {}}])))  # own namespace only
        # correct rewording: matchExpressions instead of matchLabels
        alt = {"namespaceSelector": {"matchExpressions": [{"key": "kubernetes.io/metadata.name", "operator": "In",
                                                            "values": ["treasury"]}]}}
        self.assertTrue(self.run_oracle(w, self.with_from(w, [alt])))

    def test_r3_restart_task_and_its_wrong_fixes(self):
        w = next(i for i in self.r3("vbw_restart_annotation") if i["vbw"]["role"] == "wrong")

        def ann(value_key, value):
            d = sc.yaml.safe_load(self.fix_of(w))
            d["metadata"]["annotations"] = {value_key: value}
            return sc.yaml.safe_dump(d, sort_keys=False)
        self.assertTrue(self.run_oracle(w, self.fix_of(w)))
        self.assertTrue(self.run_oracle(w, ann("strimzi.io/restart-task", "2")))
        self.assertFalse(self.run_oracle(w, ann("strimzi.io/restart-task", "1")))      # another task
        self.assertFalse(self.run_oracle(w, ann("strimzi.io/restart", "true")))        # the shown fault
        self.assertFalse(self.run_oracle(w, ann("strimzi.io/restart", "includeTasks")))
        self.assertFalse(self.run_oracle(w, ann("strimzi.io/restart-task", "two")))

    def test_r3_pdb_percentages_round_up(self):
        w = next(i for i in self.r3("vbw_pdb_no_eviction") if i["vbw"]["role"] == "wrong")
        c = next(i for i in self.r3("vbw_pdb_no_eviction") if i["vbw"]["role"] == "control")

        def pdb(it, spec):
            d = sc.yaml.safe_load(self.fix_of(it))
            d["spec"].pop("maxUnavailable", None)
            d["spec"].update(spec)
            return sc.yaml.safe_dump(d, sort_keys=False)
        self.assertTrue(self.run_oracle(w, self.fix_of(w)))
        self.assertFalse(self.run_oracle(w, pdb(w, {"minAvailable": "90%"})))         # 5 pods: rounds up to 5
        self.assertFalse(self.run_oracle(w, pdb(w, {"maxUnavailable": 0})))
        self.assertTrue(self.run_oracle(w, pdb(w, {"maxUnavailable": "20%"})))        # 1 of 5
        self.assertFalse(self.run_oracle(w, pdb(w, {"maxUnavailable": "50%"})))       # 3 of 5: more than one
        self.assertTrue(self.run_oracle(c, self.fix_of(c)))                            # 25 % of 4 = 1
        self.assertFalse(self.run_oracle(c, pdb(c, {"maxUnavailable": "50%"})))

    def test_r3_min_isr_below_two(self):
        w = self.r3("vbw_min_isr_replicas")[0]
        self.assertEqual(w["vbw"]["role"], "wrong")
        self.assertTrue(w["id"].endswith("-v3-104"))
        self.assertTrue(self.run_oracle(w, self.fix_of(w)))
        d = sc.yaml.safe_load(self.fix_of(w))
        self.assertEqual((d["spec"]["replicas"], d["spec"]["config"]["min.insync.replicas"]), (3, 2))
        d["spec"]["config"]["min.insync.replicas"] = 1
        self.assertFalse(self.run_oracle(w, sc.yaml.safe_dump(d, sort_keys=False)))     # the shown fault
        d["spec"]["config"]["min.insync.replicas"] = 3
        self.assertFalse(self.run_oracle(w, sc.yaml.safe_dump(d, sort_keys=False)))     # equal to replicas: one lost broker stops writes
        d["spec"]["replicas"] = 4
        d["spec"]["config"]["min.insync.replicas"] = 3
        self.assertTrue(self.run_oracle(w, sc.yaml.safe_dump(d, sort_keys=False)))      # another correct pair
        self.assertTrue(any(s["file"].endswith("topic_config.html") and s["line"] == 246 for s in w["sources"]))

    def test_r3_restart_task_must_not_keep_the_restart_annotation(self):
        w = next(i for i in self.r3("vbw_restart_annotation") if i["vbw"]["role"] == "wrong")
        self.assertTrue(w["id"].endswith("-v3-103"))
        self.assertIn("yaml_fields_absent", [c["type"] for c in w["checks"]])

        def answer(anns):
            d = sc.yaml.safe_load(self.fix_of(w))
            d["metadata"]["annotations"] = anns
            return "```yaml\n%s\n```\nANSWER: does not work" % sc.yaml.safe_dump(d, sort_keys=False)

        def passes(text):
            return all(r["pass"] for r in sc.run_checks(w, text, self.cats))
        self.assertTrue(passes(answer({"strimzi.io/restart-task": "2"})))
        self.assertFalse(passes(answer({"strimzi.io/restart": "true", "strimzi.io/restart-task": "2"})))
        self.assertFalse(passes(answer({"strimzi.io/restart": "false", "strimzi.io/restart-task": "2"})))
        self.assertFalse(passes(answer({"strimzi.io/restart": "true"})))

    def test_min_isr_variant_ignores_the_replica_count(self):
        import eval2_items_a3 as asm
        mk = lambda fam, fc: {"kind": "valid_but_wrong", "template": "vbw_min_isr_replicas", "family": fam, "failure_class": fc}  # noqa: E731
        k = asm.variant_key
        self.assertEqual(k(mk("vbw_min_isr_replicas:orders:3", "valid_but_wrong")), k(mk("vbw_min_isr_replicas:ledger:5", "valid_but_wrong")))
        self.assertNotEqual(k(mk("vbw_min_isr_replicas:orders:3", "valid_but_wrong")),
                            k(mk("vbw_min_isr_replicas:below_two:telemetry:3", "valid_but_wrong")))
        self.assertNotEqual(k(mk("vbw_min_isr_replicas:orders:3", "valid_but_wrong")), k(mk("vbw_min_isr_replicas:orders:3", "valid_control")))

    def test_v7_exactly_one_allowed(self):
        for it in self.of("vbw_pdb_no_eviction"):
            c = it["vbw"]["oracle_checks_on_control"][0]
            self.assertEqual(c["allowed"], 1)
            rep = c["replicas"]
            self.assertTrue(self.run_oracle(it, self.fix_of(it)))
            for spec in ({"maxUnavailable": rep}, {"minAvailable": 1}, {"minAvailable": 0}, {"maxUnavailable": 0},
                         {"minAvailable": "100%"}):
                d = sc.yaml.safe_load(self.fix_of(it))
                d["spec"].pop("maxUnavailable", None)
                d["spec"].update(spec)
                self.assertFalse(self.run_oracle(it, sc.yaml.safe_dump(d, sort_keys=False)), (it["id"], spec))
            d = sc.yaml.safe_load(self.fix_of(it))
            d["spec"].pop("maxUnavailable")
            d["spec"]["minAvailable"] = rep - 1          # a correct rewording of "one at a time"
            self.assertTrue(self.run_oracle(it, sc.yaml.safe_dump(d, sort_keys=False)))

    def test_v3_compaction_keys_only_on_compacted_topics(self):
        for it in self.of("vbw_default_value"):
            key = it["family"].split(":")[1]
            for text in (self.shown(it), self.fix_of(it)):
                cfg = sc.yaml.safe_load(text)["spec"]["config"]
                self.assertEqual("cleanup.policy" in cfg and cfg["cleanup.policy"] == "compact",
                                 key in g.COMPACTED_KEYS, it["id"])
            # own Default line, not the min.insync.replicas lines
            dl = [s for s in it["sources"] if s["entry"].endswith("Default")]
            self.assertEqual(len(dl), 1)
            self.assertIn("<th>Default:</th>", dl[0]["phrase"])
            self.assertTrue(any(s["entry"].startswith(key + " ") for s in it["sources"]))
            if key != "min.insync.replicas":
                self.assertFalse(any("min.insync.replicas" in s["entry"] for s in it["sources"]), it["id"])

    def test_s4_wording_in_the_pool(self):
        pool = dict((k, n) for k, n, _ in g.V3_POOL)
        self.assertIn("record batches of up to 2 MiB", pool["max.message.bytes"])


@unittest.skipUnless(HAVE, "catalogues and catalogues_a3 not built")
class Gating(unittest.TestCase):
    """A rule that the table anchors later is picked up by rerunning; valid_but_wrong false keeps a rule out."""

    def make(self, edit):
        d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, True)
        for f in A3_DIR.glob("*.json"):
            shutil.copy(f, d / f.name)
        t = json.loads((d / "semantic_rules.json").read_text())
        edit(t["mechanisms"])
        (d / "semantic_rules.json").write_text(json.dumps(t, indent=1, sort_keys=True))
        return d

    def used(self, d):
        return {m for m, (u, _) in g.mechanism_status(str(d)).items() if u}

    def test_valid_but_wrong_false_is_skipped(self):
        def edit(m):
            m["V8"]["anchored"] = True
            m["V8"]["valid_but_wrong"] = False
        d = self.make(edit)
        self.assertNotIn("V8", self.used(d))
        items = g.generate(catalogues_a3_dir=str(d), dedup=False)
        self.assertFalse(any(i["vbw"]["mechanism"] == "V8" for i in items))

    def test_real_table_skips_v4_and_v8(self):
        used = self.used(A3_DIR)
        self.assertNotIn("V4", used)
        self.assertNotIn("V8", used)
        self.assertFalse(g.mechanism_status()["V8"][0])

    def test_unanchored_table_entry_is_skipped(self):
        def edit(m):
            m["V1"]["anchored"] = False
        self.assertNotIn("V1", self.used(self.make(edit)))

    def test_v8_picked_up_when_valid_but_wrong(self):
        def edit(m):
            m["V8"]["anchored"] = True
            m["V8"]["valid_but_wrong"] = True
            m["V8"]["anchors"] = copy.deepcopy(m["V1"]["anchors"])
        d = self.make(edit)
        items = g.generate(catalogues_a3_dir=str(d), dedup=False)
        v8 = [i for i in items if i["vbw"]["mechanism"] == "V8"]
        self.assertEqual(len(v8), 4)
        self.assertEqual(len(items), 41)  # FK gives up four places

    def test_v4_picked_up_when_both_anchors_flip(self):
        def edit(m):
            m["V4"]["anchored"] = True
            m["V4"]["anchors"] = copy.deepcopy(m["V1"]["anchors"])
        d = self.make(edit)
        self.assertNotIn("V4", self.used(d))  # the scorer module still marks properties.load_semantics unanchored
        old = c3.RULES["properties.load_semantics"]["anchored"]
        c3.RULES["properties.load_semantics"]["anchored"] = True
        try:
            self.assertIn("V4", self.used(d))
            items = g.generate(catalogues_a3_dir=str(d), dedup=False)
        finally:
            c3.RULES["properties.load_semantics"]["anchored"] = old
        self.assertEqual(len([i for i in items if i["vbw"]["mechanism"] == "V4"]), 4)


if __name__ == "__main__":
    unittest.main()
