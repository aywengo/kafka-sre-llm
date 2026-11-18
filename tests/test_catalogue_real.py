"""Assertions against the real catalogues in work/m4/catalogues (built from the pinned sources).

Skipped when the catalogues have not been built: run `.venv/bin/python scripts/build_catalogues.py`.
"""
import unittest

import yaml

from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues, default_dir

NEEDED = ("kafka_config", "strimzi_resources", "strimzi_annotations", "kafka_cli", "argocd", "kubernetes",
          "protocol_errors", "connect_rest", "kafka_config_history", "CATALOGUES")
HAVE = all((default_dir() / (n + ".json")).is_file() for n in NEEDED)


@unittest.skipUnless(HAVE, "catalogues not built in work/m4/catalogues (run scripts/build_catalogues.py)")
class RealCatalogueTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Catalogues.load()

    def test_max_poll_interval_is_consumer_key_default_300000(self):
        r = self.c.config_default("max.poll.interval.ms", "consumer")
        self.assertEqual(r.status, KNOWN)
        self.assertEqual(r.data["default_value"], "300000")

    def test_errors_tolerance_none_and_all_not_class(self):
        self.assertEqual(self.c.config_value("errors.tolerance", "none", "sink_connector").status, KNOWN)
        self.assertEqual(self.c.config_value("errors.tolerance", "all", "sink_connector").status, KNOWN)
        self.assertEqual(self.c.config_value("errors.tolerance", "class", "sink_connector").status, UNKNOWN)
        self.assertEqual(self.c.config_value("errors.tolerance", "class").status, UNKNOWN)

    def test_rebalance_timeout_is_not_a_consumer_key(self):
        r = self.c.config_key("rebalance.timeout.ms", "consumer")
        self.assertEqual(r.status, UNKNOWN)
        self.assertEqual(self.c.config_key("rebalance.timeout.ms", "connect").status, KNOWN)

    def test_quotas_consumer_is_not_a_kafkauser_field(self):
        self.assertEqual(self.c.field_path("KafkaUser", "spec.quotas.consumer").status, UNKNOWN)
        for f in ("consumerByteRate", "producerByteRate", "requestPercentage", "controllerMutationRate"):
            self.assertEqual(self.c.field_path("KafkaUser", "spec.quotas." + f).status, KNOWN, f)

    def test_rebalance_annotation_is_defined(self):
        r = self.c.annotation("strimzi.io/rebalance", "approve")
        self.assertEqual(r.status, KNOWN)
        self.assertEqual(r.data["value_status"], "seen")

    def test_unparsed_is_not_a_known_option_of_kafka_topics(self):
        # the option parser of kafka-topics.sh was read from the Kafka 4.3.2 source: the tool is complete
        r = self.c.cli_option("kafka-topics.sh", "--unparsed")
        self.assertEqual(r.status, UNKNOWN)
        self.assertEqual(r.evidence, "source_code")
        e = [t for t in self.c.data["kafka_cli"]["entries"] if t["tool"] == "kafka-topics.sh"][0]
        self.assertTrue(e["complete"])
        self.assertEqual(self.c.cli_option("kafka-topics.sh", "--partitions").data["takes_argument"], "required")
        self.assertEqual(self.c.cli_option("kafka-topics.sh", "--describe").data["takes_argument"], "none")

    def test_leader_election_has_no_describe(self):
        self.assertEqual(self.c.cli_option("kafka-leader-election.sh", "--describe").status, UNKNOWN)
        for o in ("--election-type", "--all-topic-partitions", "--path-to-json-file", "--bootstrap-server"):
            self.assertEqual(self.c.cli_option("kafka-leader-election.sh", o).status, KNOWN, o)

    def test_tool_listing_and_completeness_per_tool(self):
        cli = self.c.data["kafka_cli"]
        self.assertTrue(cli["bin_complete"])
        self.assertEqual(self.c.cli_tool("kafka-cluster.sh").status, KNOWN)
        self.assertEqual(self.c.cli_tool("kafka-clusters.sh").status, UNKNOWN)
        inc = {e["tool"]: e["reason"] for e in cli["entries"] if e.get("in_bin") and not e["complete"]}
        self.assertEqual(sorted(inc), ["connect-distributed.sh", "connect-standalone.sh", "kafka-run-class.sh",
                                       "kafka-server-start.sh", "kafka-server-stop.sh"])
        self.assertTrue(all(inc.values()))
        complete = [e for e in cli["entries"] if e.get("in_bin") and e["complete"]]
        self.assertEqual(len(complete), 38)
        for e in complete:
            self.assertTrue(e["options"], e["tool"])
            self.assertTrue(all(o["evidence"] == "source_code" for o in e["options"].values()), e["tool"])
        self.assertEqual(self.c.cli_option("kafka-server-start.sh", "--nope").status, NOT_COVERED)

    def test_constraints_are_recorded_where_the_code_states_them(self):
        kinds = {c["kind"] for c in self.c.cli_constraints("kafka-consumer-groups.sh")}
        self.assertIn("availableIf", kinds)
        topics = self.c.cli_constraints("kafka-topics.sh")
        self.assertTrue(any(c["kind"] == "checkRequiredArgs" and "--topic" in c["args"] for c in topics))

    def test_options_table_tool_can_say_unknown(self):
        self.assertEqual(self.c.cli_option("kafka-streams-application-reset.sh", "--dry-run").status, KNOWN)
        self.assertEqual(self.c.cli_option("kafka-streams-application-reset.sh", "--unparsed").status, UNKNOWN)

    def test_every_catalogue_and_tool_says_complete(self):
        for n in ("kafka_config", "strimzi_resources", "strimzi_annotations", "kafka_cli", "argocd",
                  "kubernetes", "protocol_errors", "connect_rest", "kafka_config_history"):
            if self.c.data.get(n):
                self.assertIsInstance(self.c.data[n]["complete"], bool, n)
        self.assertTrue(self.c.complete("kafka_config"))
        self.assertTrue(self.c.complete("protocol_errors"))
        self.assertTrue(self.c.complete("connect_rest"))
        self.assertFalse(self.c.complete("kafka_cli"))
        self.assertTrue(self.c.complete("kafka_config_history"))
        self.assertFalse(self.c.complete("strimzi_annotations"))
        for e in self.c.data["kafka_cli"]["entries"]:
            self.assertIsInstance(e["complete"], bool)

    def test_every_entry_has_provenance(self):
        ok = {"exists", "documented"}
        for n, key in (("kafka_config", "entries"), ("strimzi_resources", "entries"),
                       ("strimzi_resources", "reference_entries"), ("argocd", "resources"), ("kubernetes", "resources"),
                       ("strimzi_annotations", "entries"), ("kafka_cli", "entries"),
                       ("protocol_errors", "entries"), ("connect_rest", "entries"),
                       ("kafka_config_history", "entries")):
            for e in self.c.data[n][key]:
                self.assertTrue(e["provenance"] and set(e["provenance"]) <= ok, (n, e.get("key") or e.get("name")))
        for e in self.c.data["argocd"]["cli"] + self.c.data["kubernetes"]["cli"]:
            self.assertTrue(set(e["provenance"]) <= ok)
            for o in e["options"].values():
                self.assertTrue(o["provenance"])

    def test_kubectl_flags_are_examples_only_and_incomplete(self):
        r = self.c.cli_option("kubectl get", "--output")
        self.assertEqual((r.status, r.evidence), (KNOWN, "seen_in_example"))
        e = [t for t in self.c.data["kubernetes"]["cli"] if t["tool"] == "kubectl get"][0]
        self.assertFalse(e["complete"])
        self.assertEqual(e["options"]["--output"]["provenance"], ["documented"])
        self.assertEqual(self.c.cli_option("kubectl get", "--no-such-flag").status, NOT_COVERED)

    def test_reassign_partitions_cancel_is_known(self):
        # real, and absent from the docs on disk: the source of the tool has it
        r = self.c.cli_option("kafka-reassign-partitions.sh", "--cancel")
        self.assertEqual((r.status, r.evidence), (KNOWN, "source_code"))
        self.assertEqual(self.c.cli_option("kafka-reassign-partitions.sh", "--cancle").status, UNKNOWN)

    def test_annotation_miss_is_not_covered(self):
        self.assertEqual(self.c.annotation("strimzi.io/no-such-annotation").status, NOT_COVERED)

    def test_protocol_errors(self):
        r = self.c.error_name("NOT_ENOUGH_REPLICAS_AFTER_APPEND")
        self.assertEqual(r.status, KNOWN)
        self.assertEqual(r.data["code"], 20)
        self.assertEqual(self.c.error_name("NotEvenoughReplicasAfterAppend").status, UNKNOWN)
        self.assertEqual(len(self.c.data["protocol_errors"]["entries"]), 135)

    def test_connect_rest_put_restart_is_wrong_method(self):
        self.assertEqual(self.c.rest_operation("POST", "/connectors/x/restart").status, KNOWN)
        r = self.c.rest_operation("PUT", "/connectors/x/restart")
        self.assertEqual((r.status, r.data["allowed_methods"]), (UNKNOWN, ["POST"]))
        self.assertEqual(self.c.rest_operation("POST", "/connectors/x/tasks/0/restart").status, KNOWN)
        self.assertEqual(self.c.rest_operation("PUT", "/connectors/x/tasks/0/restart").status, UNKNOWN)

    def test_argocd_cli_complete_unknown_flag_and_command(self):
        self.assertEqual(self.c.cli_option("argocd app sync", "--prune").status, KNOWN)
        self.assertEqual(self.c.cli_option("argocd app sync", "--no-such-flag").status, UNKNOWN)
        self.assertEqual(self.c.argocd_command("argocd app frobnicate").status, UNKNOWN)

    def test_manifest_matches_the_files(self):
        import json
        man = json.loads((default_dir() / "CATALOGUES.json").read_text())
        from kafka_sre_rag.catalogue import manifest_for
        self.assertEqual(man, manifest_for(default_dir()))

    def test_every_entry_has_source_file_and_pin(self):
        for e in self.c.data["kafka_config"]["entries"]:
            self.assertTrue(e["source_file"] and e["pin"])
        for e in self.c.data["strimzi_resources"]["entries"]:
            self.assertTrue(e["source_file"] and e["pin"])
        for e in self.c.data["strimzi_annotations"]["entries"]:
            self.assertTrue(e["source_file"] and e["pin"])
        for e in self.c.data["kafka_cli"]["entries"]:
            self.assertTrue(e["pin"])


@unittest.skipUnless(HAVE, "catalogues not built in work/m4/catalogues (run scripts/build_catalogues.py)")
class RealSchemaTests(unittest.TestCase):
    """Schemas from the CRDs and the Kubernetes OpenAPI document (catalogue_sources.yaml)."""

    @classmethod
    def setUpClass(cls):
        cls.c = Catalogues.load()

    def violations(self, text):
        return [(v.code, v.path) for v in self.c.validate_resource(yaml.safe_load(text)).violations]

    KAFKAUSER = """apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: u
  labels: {strimzi.io/cluster: my-cluster}
spec:
  authentication: {type: tls}
  quotas:
    %s
"""

    def test_quotas_consumer_is_not_a_kafkauser_field_and_fails_validation(self):
        r = self.c.field_path("KafkaUser", "spec.quotas.consumer")
        self.assertEqual((r.status, r.evidence), (UNKNOWN, "crd"))
        self.assertEqual(self.violations(self.KAFKAUSER % "consumer: 5"), [("unknown_field", "spec.quotas.consumer")])
        self.assertEqual(self.violations(self.KAFKAUSER % "consumerByteRate: 5"), [])
        self.assertEqual(self.violations(self.KAFKAUSER % 'consumerByteRate: "5"'), [("wrong_type", "spec.quotas.consumerByteRate")])

    def test_strimzi_crd_entries_are_complete_exist_and_v1_only(self):
        es = self.c.data["strimzi_resources"]["entries"]
        self.assertEqual(len(es), 10)
        for e in es:
            self.assertEqual((e["source"], e["api_version"].split("/")[1], e["complete"], e["provenance"]),
                             ("crd", "v1", True, ["exists"]), e["kind"])
        ref = self.c.data["strimzi_resources"]["reference_entries"]
        self.assertEqual({e["kind"] for e in ref}, {e["kind"] for e in es})
        self.assertTrue(all(e["provenance"] == ["documented"] for e in ref))
        t = self.c.field_path("KafkaUser", "spec.authentication.type").data["info"]
        self.assertEqual(t["enum"], ["scram-sha-512", "tls", "tls-external"])
        self.assertTrue(t["required"])
        cfg = self.c.field_path("Kafka", "spec.kafka.config")
        self.assertTrue(cfg.known and cfg.data["info"]["open"])       # free-form: the key catalogue judges what is below
        self.assertEqual(self.c.field_path("Kafka", "spec.kafka.config.num.partitions").status, NOT_COVERED)

    def test_unserved_strimzi_version_is_reported(self):
        got = self.violations("apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata: {name: t}\nspec: {partitions: 3}\n")
        self.assertEqual(got, [("unknown_api_version", "apiVersion")])
        self.assertEqual(self.violations("apiVersion: kafka.strimzi.io/v1\nkind: KafkaCluster\n"), [("unknown_kind", "kind")])

    def test_operator_pod_label_value_is_cluster_operator(self):
        self.assertEqual(self.c.operator_pod_labels()["strimzi.io/kind"], "cluster-operator")
        r = self.c.operator_label("strimzi.io/kind", "cluster-operator")
        self.assertEqual((r.status, r.data["value_ok"]), (KNOWN, True))
        self.assertFalse(self.c.operator_label("strimzi.io/kind", "ClusterOperator").data["value_ok"])
        self.assertEqual(self.c.operator_env("STRIMZI_NAMESPACE").status, KNOWN)
        self.assertEqual(self.c.operator_env("STRIMZI_FULL_RECONCILIATION_INTERVAL_MS").data["value"], "120000")
        self.assertEqual(self.c.operator_env("STRIMZI_NO_SUCH").status, NOT_COVERED)

    APP = """apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: a, namespace: argocd}
spec:
  project: default
  source: {repoURL: "https://example.invalid/r.git", path: p, targetRevision: HEAD}
"""

    def test_application_without_destination_fails_validation(self):
        self.assertEqual(self.violations(self.APP), [("missing_required", "spec.destination")])
        ok = self.APP + "  destination: {server: 'https://kubernetes.default.svc', namespace: n}\n"
        self.assertEqual(self.violations(ok), [])
        self.assertEqual(self.violations(ok + "  syncPolicy: {automated: {prune: true, selfheel: true}}\n"),
                         [("unknown_field", "spec.syncPolicy.automated.selfheel")])

    def test_argocd_resources_are_complete_for_the_three_kinds(self):
        a = self.c.data["argocd"]
        self.assertEqual(a["complete_parts"]["resource_kinds_complete"], ["AppProject", "Application", "ApplicationSet"])
        for e in a["resources"]:
            self.assertEqual(e["complete"], e["source"] == "crd")
        self.assertTrue(self.c.argocd_command("argocd app sync").known)

    def test_kubernetes_schema(self):
        es = [e for e in self.c.data["kubernetes"]["resources"] if e["source"] == "openapi"]
        self.assertGreater(len(es), 200)
        by = {(e["kind"], e["api_version"]): e for e in es}
        for k in (("Pod", "v1"), ("Deployment", "apps/v1"), ("Service", "v1"), ("NetworkPolicy", "networking.k8s.io/v1"),
                  ("StatefulSet", "apps/v1"), ("ConfigMap", "v1")):
            self.assertIn(k, by)
            self.assertTrue(by[k]["complete"])
        self.assertEqual(self.c.data["kubernetes"]["complete_parts"], {"resources": True, "cli": False})
        dep = """apiVersion: apps/v1
kind: Deployment
metadata: {name: d}
spec:
  replicas: 2
  selector: {matchLabels: {a: b}}
  template:
    metadata: {labels: {a: b}}
    spec:
      containers:
        - name: c
          image: x
          resources: {limits: {cpu: 1, memory: 1Gi}}
          ports: [{containerPort: 80}]
"""
        self.assertEqual(self.violations(dep), [])
        bad = dep.replace("replicas: 2", 'replicas: "2"\n  replica: 1')
        self.assertEqual(sorted(self.violations(bad)), [("unknown_field", "spec.replica"), ("wrong_type", "spec.replicas")])
        self.assertEqual(self.violations("apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: d}\nspec: {replicas: 1}\n"),
                         [("missing_required", "spec.selector"), ("missing_required", "spec.template")])

    def test_config_history(self):
        r = self.c.config_history("controller.listener.names")
        self.assertEqual(r.status, KNOWN)
        self.assertIn("type", r.data["changes"])
        e = r.entries[0]
        self.assertEqual((e["versions"]["4.1"]["type"], e["versions"]["4.3"]["type"]), ("string", "list"))
        self.assertEqual(self.c.config_history("max.poll.interval.ms").status, UNKNOWN)
        for e in self.c.data["kafka_config_history"]["entries"]:
            self.assertTrue(e["changes"])
            self.assertEqual(sorted(e["versions"]), ["4.1", "4.2", "4.3"])


if __name__ == "__main__":
    unittest.main()
