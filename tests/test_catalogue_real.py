"""Assertions against the real catalogues in work/m4/catalogues (built from the pinned sources).

Skipped when the catalogues have not been built: run `.venv/bin/python scripts/build_catalogues.py`.
"""
import unittest

from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues, default_dir

NEEDED = ("kafka_config", "strimzi_resources", "strimzi_annotations", "kafka_cli")
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
