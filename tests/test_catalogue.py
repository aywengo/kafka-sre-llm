"""Lookups of kafka_sre_rag.catalogue on small fixture catalogues (not the real raw/ or work/)."""
import os
import unittest

from kafka_sre_rag import catalogue as cat
from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "catalogues")


class FixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Catalogues.load(FIX)

    def test_three_distinct_statuses(self):
        self.assertEqual(len({KNOWN, UNKNOWN, NOT_COVERED}), 3)

    def test_loaded(self):
        self.assertEqual(self.c.loaded(), ["kafka_config", "strimzi_resources", "strimzi_annotations", "kafka_cli"])

    def test_config_key_known_with_scope(self):
        r = self.c.config_key("max.poll.interval.ms", "consumer")
        self.assertEqual(r.status, KNOWN)
        self.assertEqual(r.data["scopes"], ["consumer"])

    def test_config_key_wrong_scope_is_unknown_and_names_other_scope(self):
        r = self.c.config_key("rebalance.timeout.ms", "consumer")
        self.assertEqual(r.status, UNKNOWN)
        self.assertEqual(r.data["also_in"], ["connect"])

    def test_config_key_without_scope_searches_all(self):
        self.assertEqual(self.c.config_key("rebalance.timeout.ms").status, KNOWN)
        self.assertEqual(self.c.config_key("no.such.key").status, UNKNOWN)

    def test_config_scopes_of_shared_key(self):
        self.assertEqual(self.c.config_key("min.insync.replicas").data["scopes"], ["broker", "topic"])

    def test_scope_not_in_catalogue_is_not_covered(self):
        self.assertEqual(self.c.config_key("x", "nosuchscope").status, NOT_COVERED)

    def test_foreign_and_prefixed_keys_are_not_covered(self):
        self.assertEqual(self.c.config_key("spring.kafka.consumer.max-poll-interval-ms").status, NOT_COVERED)
        self.assertEqual(self.c.config_key("consumer.override.max.poll.records").status, NOT_COVERED)
        self.assertEqual(self.c.config_key("listener.name.internal.ssl.keystore.type").status, NOT_COVERED)

    def test_default_strips_gloss(self):
        r = self.c.config_default("max.poll.interval.ms", "consumer")
        self.assertEqual(r.data["default_value"], "300000")
        self.assertEqual(r.data["default"], "300000 (5 minutes)")

    def test_default_of_unknown_key_is_unknown(self):
        self.assertEqual(self.c.config_default("nope", "consumer").status, UNKNOWN)

    def test_valid_values(self):
        r = self.c.config_valid_values("errors.tolerance", "sink_connector")
        self.assertEqual(r.data["list"], ["none", "all"])

    def test_config_value(self):
        self.assertEqual(self.c.config_value("errors.tolerance", "all").status, KNOWN)
        self.assertEqual(self.c.config_value("errors.tolerance", "class").status, UNKNOWN)
        # range only: nothing enumerated, so not checked
        self.assertEqual(self.c.config_value("max.poll.interval.ms", "abc", "consumer").status, NOT_COVERED)
        self.assertEqual(self.c.config_value("nope", "x").status, NOT_COVERED)

    def test_field_path(self):
        self.assertEqual(self.c.field_path("KafkaUser", "spec.quotas.consumerByteRate").status, KNOWN)
        r = self.c.field_path("KafkaUser", "spec.quotas.consumer")
        self.assertEqual(r.status, UNKNOWN)
        self.assertIn("spec.quotas.consumerByteRate", r.data["similar"])

    def test_field_path_array_forms(self):
        self.assertEqual(self.c.field_path("KafkaUser", "spec.authorization.acls[0].host").status, KNOWN)
        self.assertEqual(self.c.field_path("KafkaUser", "spec.authorization.acls[*].host").status, KNOWN)
        self.assertEqual(self.c.field_path("KafkaUser", "spec.authorization.acls.host").status, KNOWN)

    def test_field_path_without_spec_prefix(self):
        self.assertEqual(self.c.field_path("KafkaUser", "quotas.producerByteRate").status, KNOWN)

    def test_field_path_below_free_form_field_is_not_covered(self):
        r = self.c.field_path("Kafka", "spec.kafka.config.anything.at.all")
        self.assertEqual(r.status, NOT_COVERED)
        self.assertEqual(r.data["field"], "spec.kafka.config")
        self.assertEqual(self.c.field_path("KafkaUser", "spec.template.secret.metadata.labels.x").status, NOT_COVERED)

    def test_field_path_unknown_kind_and_envelope_not_covered(self):
        self.assertEqual(self.c.field_path("Deployment", "spec.replicas").status, NOT_COVERED)
        self.assertEqual(self.c.field_path("KafkaUser", "metadata.name").status, NOT_COVERED)
        self.assertEqual(self.c.field_path("KafkaUser", "apiVersion").status, NOT_COVERED)

    def test_annotation(self):
        self.assertEqual(self.c.annotation("strimzi.io/rebalance").status, KNOWN)
        self.assertEqual(self.c.annotation("strimzi.io/rebalanse").status, UNKNOWN)
        self.assertEqual(self.c.annotation("example.com/foo").status, NOT_COVERED)

    def test_annotation_values(self):
        self.assertEqual(self.c.annotation("strimzi.io/rebalance", "approve").data["value_status"], "seen")
        self.assertEqual(self.c.annotation("strimzi.io/kind", "ClusterOperator").data["value_status"], "not_seen")
        self.assertEqual(self.c.annotation("strimzi.io/pool-name", "x").data["value_status"], "no_values_documented")

    def test_annotation_removal_form(self):
        self.assertEqual(self.c.annotation("strimzi.io/rebalance-").status, KNOWN)

    def test_cli_option_with_options_table(self):
        r = self.c.cli_option("kafka-streams-application-reset.sh", "--dry-run")
        self.assertEqual((r.status, r.evidence), (KNOWN, "options_table"))
        self.assertEqual(self.c.cli_option("kafka-streams-application-reset.sh", "--dry-runn").status, UNKNOWN)

    def test_cli_option_examples_only_is_never_unknown(self):
        r = self.c.cli_option("bin/kafka-topics.sh", "--describe")
        self.assertEqual((r.status, r.evidence), (KNOWN, "seen_in_example"))
        r = self.c.cli_option("kafka-topics", "--unparsed")
        self.assertEqual(r.status, NOT_COVERED)
        self.assertIn("--describe", r.data["options_seen"])

    def test_cli_option_with_value_and_unknown_tool(self):
        self.assertEqual(self.c.cli_option("kafka-topics.sh", "--bootstrap-server=localhost:9092").status, KNOWN)
        self.assertEqual(self.c.cli_option("kafka-nonesuch.sh", "--x").status, NOT_COVERED)


class MissingCatalogueTests(unittest.TestCase):
    def test_nothing_loaded_is_always_not_covered(self):
        c = Catalogues.load(os.path.join(FIX, "does-not-exist"))
        self.assertEqual(c.loaded(), [])
        for r in (c.config_key("a.b"), c.field_path("Kafka", "spec.x"), c.annotation("strimzi.io/x"),
                  c.cli_option("kafka-topics.sh", "--x"), c.strimzi_type("Kafka")):
            self.assertEqual(r.status, NOT_COVERED)

    def test_partial_load_only_covers_what_is_there(self):
        c = Catalogues.load(FIX)  # no argocd, no kubernetes
        self.assertEqual(c.cli_option("argocd app sync", "--prune").status, NOT_COVERED)
        self.assertEqual(c.field_path("Application", "spec.source").status, NOT_COVERED)

    def test_strimzi_type(self):
        c = Catalogues.load(FIX)
        self.assertEqual(c.strimzi_type("KafkaUserQuotas").status, KNOWN)
        self.assertEqual(c.strimzi_type("KafkaConsumerConfig").status, UNKNOWN)

    def test_default_dir_is_work_m4(self):
        self.assertTrue(str(cat.default_dir()).endswith(os.path.join("work", "m4", "catalogues")))


if __name__ == "__main__":
    unittest.main()
