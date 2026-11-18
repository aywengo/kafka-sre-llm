"""Assertions against the real catalogues in work/m4/catalogues (built from the pinned sources).

Skipped when the catalogues have not been built: run `.venv/bin/python scripts/build_catalogues.py`.
"""
import unittest

from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues, default_dir

NEEDED = ("kafka_config", "strimzi_resources", "strimzi_annotations", "kafka_cli", "argocd", "kubernetes",
          "protocol_errors", "connect_rest", "CATALOGUES")
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
        # GAP: the docs on disk have no options table for kafka-topics.sh, only options seen in
        # examples. The option is "not known", but the catalogue cannot call it invented: the
        # lookup is NOT_COVERED, not UNKNOWN. See the WP1a report.
        r = self.c.cli_option("kafka-topics.sh", "--unparsed")
        self.assertNotEqual(r.status, KNOWN)
        self.assertEqual(r.status, NOT_COVERED)

    def test_options_table_tool_can_say_unknown(self):
        self.assertEqual(self.c.cli_option("kafka-streams-application-reset.sh", "--dry-run").status, KNOWN)
        self.assertEqual(self.c.cli_option("kafka-streams-application-reset.sh", "--unparsed").status, UNKNOWN)

    def test_every_catalogue_and_tool_says_complete(self):
        for n in ("kafka_config", "strimzi_resources", "strimzi_annotations", "kafka_cli", "argocd",
                  "kubernetes", "protocol_errors", "connect_rest"):
            if self.c.data.get(n):
                self.assertIsInstance(self.c.data[n]["complete"], bool, n)
        self.assertTrue(self.c.complete("kafka_config"))
        self.assertTrue(self.c.complete("protocol_errors"))
        self.assertTrue(self.c.complete("connect_rest"))
        self.assertFalse(self.c.complete("kafka_cli"))
        self.assertFalse(self.c.complete("strimzi_annotations"))
        for e in self.c.data["kafka_cli"]["entries"]:
            self.assertIsInstance(e["complete"], bool)

    def test_every_entry_has_provenance(self):
        ok = {"exists", "documented"}
        for n, key in (("kafka_config", "entries"), ("strimzi_resources", "entries"),
                       ("strimzi_annotations", "entries"), ("kafka_cli", "entries"),
                       ("protocol_errors", "entries"), ("connect_rest", "entries")):
            for e in self.c.data[n][key]:
                self.assertTrue(e["provenance"] and set(e["provenance"]) <= ok, (n, e.get("key") or e.get("name")))
        for e in self.c.data["argocd"]["cli"] + self.c.data["kubernetes"]["cli"]:
            self.assertTrue(set(e["provenance"]) <= ok)
            for o in e["options"].values():
                self.assertTrue(o["provenance"])

    def test_kafka_options_in_examples_only_are_documented_not_complete(self):
        r = self.c.cli_option("kafka-topics.sh", "--describe")
        self.assertEqual((r.status, r.evidence), (KNOWN, "seen_in_example"))
        e = [t for t in self.c.data["kafka_cli"]["entries"] if t["tool"] == "kafka-topics.sh"][0]
        self.assertFalse(e["complete"])
        self.assertEqual(e["options"]["--describe"]["provenance"], ["documented"])

    def test_cancel_style_miss_is_not_covered_not_unknown(self):
        # kafka-reassign-partitions.sh --cancel is real but absent from the docs on disk
        self.assertEqual(self.c.cli_option("kafka-reassign-partitions.sh", "--cancel").status, NOT_COVERED)

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


if __name__ == "__main__":
    unittest.main()
