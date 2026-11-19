"""Offline tests of the WP2b R0 checker fixes (false flags and extraction gaps seen on batch B1). Synthetic snippets
only; the catalogues are the fixtures plus the additions of tests.test_checker_gaps (complete script list)."""
import unittest

from kafka_sre_rag.checker import INVENTED, UNVERIFIED, WRONG_USE

from tests.test_checker import fence, levels
from tests.test_checker_gaps import run


class SubstitutionTests(unittest.TestCase):
    CMD = ("kubectl exec -n kafka $(kubectl get pod -l app=x -o name | head -1) -- "
           "/opt/kafka/bin/kafka-topics.sh --bootstrap-server b:9092 --nope t")

    def test_tool_behind_a_substitution_with_a_pipe_is_parsed(self):
        res = run(fence("bash", self.CMD))
        self.assertTrue(any(i.type == "cli_tool" and i.text == "kafka-topics.sh" for i in res.items))
        self.assertTrue(any(i.type == "cli_flag" and i.text == "--nope" for i in res.items))

    def test_inline_span_with_a_substitution(self):
        res = run("Run `%s` now." % self.CMD)
        self.assertTrue(any(i.type == "cli_tool" and i.text == "kafka-topics.sh" for i in res.items))

    def test_pipes_outside_a_substitution_still_split(self):
        res = run(fence("bash", "kubectl logs x | kafka-topics.sh --bootstrap-server b:9092 --nope t"))
        self.assertTrue(any(i.type == "cli_tool" and i.text == "kafka-topics.sh" for i in res.items))


class PropertiesFenceTests(unittest.TestCase):
    def test_key_whose_first_word_starts_no_real_key_is_judged_when_near_a_real_key(self):
        res = run(fence("properties", "global.retention.ms=5"))
        self.assertEqual(levels(res), [(INVENTED, "global.retention.ms")])

    def test_key_far_from_every_real_key_is_unverified_not_invented(self):
        res = run(fence("properties", "global.zzz.qqq=5"))
        self.assertEqual(levels(res), [(UNVERIFIED, "global.zzz.qqq")])

    def test_first_word_rule_still_applies_in_prose(self):
        self.assertEqual(run("Set `global.retention.ms` to 5.").findings, [])

    def test_file_name_shapes_are_still_not_keys(self):
        self.assertEqual(run(fence("properties", "app.config.properties=1\napp.yaml=x")).findings, [])


class ErrorNameTests(unittest.TestCase):
    def test_camel_exception_class_is_not_matched_to_a_protocol_error(self):
        res = run("It fails with `NotEnoughReplicasAfterAppendsException`.")
        self.assertEqual(res.findings, [])

    def test_misspelt_camel_name_without_exception_is_still_invented(self):
        self.assertEqual(levels(run("The producer raises NotEvenoughReplicasAfterAppend.")),
                         [(INVENTED, "NotEvenoughReplicasAfterAppend")])

    def test_acl_operation_is_not_a_protocol_error(self):
        self.assertEqual(run("The `IDEMPOTENT_WRITE` operation is not the cause of this error.").findings, [])

    def test_every_acl_operation_name_is_exempt(self):
        for op in ("CLUSTER_ACTION", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "CREATE_TOKENS", "DESCRIBE_TOKENS"):
            self.assertEqual(run("The ACL operation `%s` is not part of this error." % op).findings, [], op)

    def test_other_upper_snake_name_in_an_acl_sentence_is_still_checked(self):
        res = run("The ACL did not cause this error `FOO_BAR_BAZ`.")
        self.assertEqual(levels(res), [(INVENTED, "FOO_BAR_BAZ")])
        self.assertEqual(run("With a missing ACL the error `NOT_ENOUGH_REPLICAS_AFTER_APPEND` is a different problem.").findings, [])

    def test_upper_snake_error_sentence_without_acl_is_still_judged(self):
        self.assertEqual(levels(run("The broker returns error FENCED_STATE_VERSION.")), [(INVENTED, "FENCED_STATE_VERSION")])


class LabelMapTests(unittest.TestCase):
    Y = ("apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nspec:\n  kafka:\n    listeners:\n    - name: a\n"
         "      networkPolicyPeers:\n      - namespaceSelector:\n          %s:\n            kubernetes.io/metadata.name: payments")

    def test_keys_under_a_label_selector_map_are_not_field_paths(self):
        for m in ("matchLabels", "nodeSelector"):
            res = run(fence("yaml", self.Y % m))
            self.assertFalse([i for i in res.items if "kubernetes.io/metadata.name" in i.text], m)
            self.assertFalse([f for f in res.findings if "kubernetes.io" in f.item.text], m)

    def test_real_field_next_to_it_is_still_judged(self):
        res = run(fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nspec:\n  kafka:\n    listenerz: x"))
        self.assertTrue(res.findings)

    def test_prose_path_below_match_labels_is_not_judged(self):
        res = run("Set `Kafka.spec.kafka.listeners[].networkPolicyPeers[].namespaceSelector.matchLabels.team` here.")
        self.assertEqual([i for i in res.items if i.type == "field_path"], [])


class ProseScopeTests(unittest.TestCase):
    def test_properties_file_named_in_the_paragraph_before_gives_a_prose_key_no_scope(self):
        text = "Edit `server.properties` first.\n\nThen raise the consumer's `max.poll.interval.ms` to 600000."
        self.assertEqual(run(text).by_level(WRONG_USE), [])

    def test_properties_file_named_in_the_same_sentence_still_gives_the_scope(self):
        res = run("In `server.properties` set `max.poll.interval.ms`.")
        self.assertEqual(levels(res), [(WRONG_USE, "max.poll.interval.ms")])

    def test_fence_still_takes_the_file_named_before_it(self):
        text = "Put this in consumer.properties:\n" + fence("properties", "rebalance.timeout.ms=60000")
        self.assertEqual(levels(run(text)), [(WRONG_USE, "rebalance.timeout.ms")])


if __name__ == "__main__":
    unittest.main()
