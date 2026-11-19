"""Offline tests of the checker's extraction rules for scripts, apiVersions, enumerated values and key scope
(WP2b extraction gaps). All inputs are small synthetic snippets; the catalogues are the fixtures plus a few
entries added here (a complete script list, CRD entries that serve one apiVersion, an Argo CD kind)."""
import copy
import unittest

from kafka_sre_rag.checker import INVENTED, UNVERIFIED, WRONG_USE, IN_EXCERPTS, check_answer
from kafka_sre_rag.catalogue import Catalogues

from tests.test_checker import fence, fixture_catalogues, levels


def _crd(kind, api, paths):
    return {"kind": kind, "api_version": api, "complete": True, "source": "crd", "source_file": "fixture",
            "pin": "fixture", "paths": paths}


def build(bin_complete=True):
    data = copy.deepcopy(fixture_catalogues().data)
    cli = data["kafka_cli"]
    cli["bin_complete"] = bin_complete
    cli["bin_tools"] = ["kafka-topics.sh", "kafka-streams-application-reset.sh", "kafka-metadata-quorum.sh"]
    quorum = copy.deepcopy(cli["entries"][0])
    quorum["tool"] = "kafka-metadata-quorum.sh"
    cli["entries"].append(quorum)
    for e in cli["entries"]:
        e["in_bin"] = True
    cfg = data["kafka_config"]["entries"]
    cfg.append({"key": "delete.retention.ms", "scope": "topic", "type": "long", "default": "86400000", "valid_values": [],
                "provenance": ["documented"], "source_file": "fixture"})
    data["strimzi_resources"]["entries"] += [
        _crd("Kafka", "kafka.strimzi.io/v1", {
            "spec": {"type": ["object"]}, "spec.kafka": {"type": ["object"]},
            "spec.kafka.listeners": {"type": ["array"]}, "spec.kafka.listeners[].name": {"type": ["string"]},
            "spec.kafka.listeners[].authentication": {"type": ["object"]},
            "spec.kafka.listeners[].authentication.type": {"type": ["string"], "enum": ["tls", "scram-sha-512", "custom"]},
            "spec.kafka.listeners[].type": {"type": ["string"], "enum": ["internal", "nodeport"]},
            "spec.kafka.config": {"type": ["object"], "open": True}}),
        _crd("KafkaTopic", "kafka.strimzi.io/v1", {
            "spec": {"type": ["object"]}, "spec.config": {"type": ["object"], "open": True},
            "spec.partitions": {"type": ["integer"]}}),
    ]
    data["argocd"]["resources"] = [_crd("Application", "argoproj.io/v1alpha1", {"spec": {"type": ["object"]}})]
    return Catalogues(data)


CATS = build()


def run(text, excerpts=None, cats=CATS):
    return check_answer(text, excerpts, cats)


class ScriptNameTests(unittest.TestCase):
    def test_script_that_bin_does_not_have_is_invented_in_prose_and_in_a_command(self):
        res = run("Use `kafka-metadata.sh` to read the log.")
        self.assertEqual(levels(res), [(INVENTED, "kafka-metadata.sh")])
        self.assertIn("kafka-metadata-quorum.sh", res.findings[0].evidence)
        res = run(fence("bash", "kafka-log-dump.sh --files /data/x.log"))
        self.assertIn((INVENTED, "kafka-log-dump.sh"), levels(res))
        res = run("Run kafka-log-dump.sh on the segment.")
        self.assertEqual(levels(res), [(INVENTED, "kafka-log-dump.sh")])

    def test_real_script_is_not_reported(self):
        self.assertEqual(run("Use `kafka-topics.sh --list` and kafka-metadata-quorum.sh here.").by_level(INVENTED), [])
        self.assertEqual(run(fence("bash", "/opt/kafka/bin/kafka-topics.sh --topic t")).by_level(INVENTED), [])

    def test_name_without_the_sh_suffix_is_not_invented(self):
        res = run("The Application was called `kafka-prod-app` before.")
        self.assertEqual(res.by_level(INVENTED), [])

    def test_incomplete_script_list_gives_only_the_weaker_kind(self):
        res = run("Use `kafka-metadata.sh` to read the log.", cats=build(bin_complete=False))
        self.assertEqual(levels(res), [(UNVERIFIED, "kafka-metadata.sh")])

    def test_excerpts_shield_the_script(self):
        res = run("Use `kafka-metadata.sh` here.", excerpts=["run kafka-metadata.sh to ..."])
        self.assertEqual(res.findings, [])
        self.assertTrue(any(i.status == IN_EXCERPTS for i in res.items))


class ApiVersionTests(unittest.TestCase):
    def y(self, api, kind="Kafka"):
        return fence("yaml", "apiVersion: %s\nkind: %s\nspec:\n  partitions: 3" % (api, kind))

    def test_version_the_crd_does_not_serve_is_wrong_use_with_the_served_one(self):
        res = run(self.y("kafka.strimzi.io/v1beta2", "KafkaTopic"))
        self.assertEqual(levels(res), [(WRONG_USE, "kafka.strimzi.io/v1beta2")])
        self.assertIn("kafka.strimzi.io/v1", res.findings[0].evidence)
        self.assertEqual(res.findings[0].item.line, 2)

    def test_served_version_passes(self):
        self.assertEqual(run(self.y("kafka.strimzi.io/v1", "KafkaTopic")).findings, [])

    def test_argocd_version_is_checked_the_same_way(self):
        self.assertEqual(levels(run(self.y("argoproj.io/v1beta1", "Application"))), [(WRONG_USE, "argoproj.io/v1beta1")])
        self.assertEqual(run(self.y("argoproj.io/v1alpha1", "Application")).findings, [])

    def test_same_kind_name_in_another_group_is_not_judged(self):
        self.assertEqual(run(self.y("app.k8s.io/v1beta1", "Application")).findings, [])

    def test_excerpts_that_show_the_version_shield_it(self):
        res = run(self.y("kafka.strimzi.io/v1beta2", "KafkaTopic"), excerpts=["apiVersion: kafka.strimzi.io/v1beta2"])
        self.assertEqual(res.findings, [])

    def test_fixture_entries_that_are_not_crd_serve_no_verdict(self):
        res = check_answer(self.y("kafka.strimzi.io/v1beta2", "KafkaUser"), None, fixture_catalogues())
        self.assertEqual(res.by_level(WRONG_USE), [])


class EnumTests(unittest.TestCase):
    def listener(self, auth, head="apiVersion: kafka.strimzi.io/v1\nkind: Kafka\n"):
        return fence("yaml", head + "spec:\n  kafka:\n    listeners:\n      - name: ext\n        type: nodeport\n"
                     "        authentication:\n          type: %s" % auth)

    def test_value_outside_the_enum_is_wrong_use_and_names_the_valid_ones(self):
        res = run(self.listener("none"))
        self.assertEqual(levels(res), [(WRONG_USE, "spec.kafka.listeners[].authentication.type=none")])
        self.assertIn("scram-sha-512", res.findings[0].evidence)

    def test_valid_value_passes(self):
        self.assertEqual(run(self.listener("tls")).findings, [])

    def test_snippet_without_kind_that_starts_at_spec_kafka_is_a_kafka_resource(self):
        res = run("Update the listener:\n" + self.listener("none", head=""))
        self.assertEqual([f.level for f in res.findings], [WRONG_USE])
        self.assertEqual(run("Update:\n" + self.listener("tls", head="")).findings, [])

    def test_placeholder_and_template_values_are_not_judged(self):
        self.assertEqual(run(self.listener("<type>")).findings, [])
        self.assertEqual(run(self.listener("{{ .Values.auth }}")).findings, [])

    def test_no_enum_check_under_a_version_that_is_not_served(self):
        res = run(self.listener("none", head="apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\n"))
        self.assertEqual(levels(res), [(WRONG_USE, "kafka.strimzi.io/v1beta2")])


class ScopeInManifestTests(unittest.TestCase):
    def test_broker_only_key_under_kafkatopic_config_is_wrong_use_not_invented(self):
        y = fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nspec:\n  config:\n    log.retention.ms: 5")
        res = run(y)
        self.assertEqual(levels(res), [(WRONG_USE, "log.retention.ms")])

    def test_topic_key_under_kafkatopic_config_passes(self):
        y = fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nspec:\n  config:\n    delete.retention.ms: 5")
        self.assertEqual(run(y).findings, [])

    def test_topic_only_key_under_kafka_config_is_wrong_use(self):
        y = fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nspec:\n  kafka:\n    config:\n"
                  "      delete.retention.ms: 5\n      log.retention.ms: 5")
        self.assertEqual(levels(run(y)), [(WRONG_USE, "delete.retention.ms")])


class EnumLikeWordTests(unittest.TestCase):
    def test_upper_snake_value_in_a_sentence_with_returns_is_not_an_error_name(self):
        res = check_answer("`READ_COMMITTED` returns data below the last stable offset.", None, build())
        self.assertEqual(res.by_level(INVENTED), [])
        res = check_answer("With READ_COMMITTED the consumer raises nothing and returns committed records.", None, build())
        self.assertEqual(res.by_level(INVENTED), [])


class FlagAttributionTests(unittest.TestCase):
    def test_flag_in_a_sentence_naming_two_tools_is_not_attributed(self):
        res = run("`kafka-topics.sh` has no such option; `--force` is documented for `kafka-streams-application-reset.sh`.")
        self.assertEqual(res.by_level(INVENTED), [])

    def test_flag_after_one_tool_is_still_checked(self):
        res = run("Run `kafka-topics.sh` with `--bogus`.")
        self.assertEqual(levels(res), [(UNVERIFIED, "--bogus")])   # the fixture's option list is not complete


if __name__ == "__main__":
    unittest.main()
