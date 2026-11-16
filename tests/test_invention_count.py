import io
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import invention_count as ic  # noqa: E402


def toks(text):
    return {t for _, t in ic.extract(text)}


class ExtractTests(unittest.TestCase):
    def test_flags(self):
        self.assertIn("--under-min-isr-partitions", toks("Run kafka-topics.sh --describe --under-min-isr-partitions now"))

    def test_dotted_keys(self):
        self.assertIn("max.poll.interval.ms", toks("Raise `max.poll.interval.ms` to 600000."))

    def test_metric(self):
        t = toks("Watch container_cpu_cfs_throttled_periods_total and UnderReplicatedPartitions.")
        self.assertIn("container_cpu_cfs_throttled_periods_total", t)
        self.assertIn("UnderReplicatedPartitions", t)

    def test_camel(self):
        t = toks("Create a KafkaNodePool and set maxUnavailable; expect NotEnoughReplicasException.")
        self.assertTrue({"KafkaNodePool", "maxUnavailable", "NotEnoughReplicasException"} <= t)

    def test_dotted_path(self):
        self.assertIn("spec.kafka.template.pod", toks("Set spec.kafka.template.pod in the Kafka resource."))

    def test_annotation(self):
        self.assertIn("strimzi.io/manual-rolling-update",
                      toks("kubectl annotate pod x strimzi.io/manual-rolling-update=true"))

    def test_api_version_not_annotation(self):
        self.assertEqual(toks("apiVersion: kafka.strimzi.io/v1beta2"), set())

    def test_endpoint(self):
        self.assertIn("GET /connectors/{name}/status", toks("Use GET /connectors/{name}/status to check."))

    def test_shell_tool_normalised(self):
        self.assertIn("kafka-topics", toks("Use kafka-topics.sh here"))

    def test_code_fence(self):
        text = "Run:\n```\nkafka-configs.sh --alter --add-config min.insync.replicas=2\n```\n"
        t = toks(text)
        self.assertTrue({"kafka-configs", "--add-config", "min.insync.replicas"} <= t)

    def test_allowlist_ignored(self):
        t = toks("kubectl get pods --namespace kafka --bootstrap-server localhost:9092 apiVersion kind metadata spec")
        self.assertEqual(t, set())

    def test_placeholders_ignored(self):
        t = toks("kafka-topics.sh --topic <my-topic> BOOTSTRAP_SERVER=x MY_VAR my_topic.name my-cluster")
        self.assertEqual({x for x in t if "topic>" in x or x.isupper()}, set())
        self.assertNotIn("my_topic.name", t)
        self.assertNotIn("BOOTSTRAP_SERVER", t)

    def test_prose_not_extracted(self):
        text = "Whenever the broker restarts, Kafka rebalances. However, Strimzi and Kubernetes help. e.g. this, i.e. that."
        self.assertEqual(toks(text), set())

    def test_versions_and_files(self):
        text = "Upgrade to 3.7.1 or v3.5; edit values.yaml, server.properties and config.json on docs.example.com."
        self.assertEqual(toks(text), set())

    def test_urls_ignored(self):
        self.assertEqual(toks("See https://kafka.apache.org/documentation/#max.poll.interval.ms"), set())

    def test_dedup_order(self):
        self.assertEqual([t for _, t in ic.extract("--aaa-b then --aaa-b and --ccc-d")], ["--aaa-b", "--ccc-d"])


class MatchTests(unittest.TestCase):
    PACK = "KafkaNodePool sets spec.replicas. Use kafka-topics.sh --describe. max.poll.interval.ms matters. POST /connectors/{name}/restart"
    CORPUS_ONLY = "The strimzi.io/manual-rolling-update annotation and --bootstrap-server and UnderReplicatedPartitions exist."

    def setUp(self):
        self.pack = self.PACK.lower()
        self.corpus = (self.PACK + "\n" + self.CORPUS_ONLY + "\nspec: kafka: template: pod:").lower()

    def check(self, text):
        return {r["token"]: r for r in ic.check_answer(text, self.pack, self.corpus)}

    def test_in_pack(self):
        r = self.check("KafkaNodePool, kafka-topics.sh, max.poll.interval.ms")
        for t in r.values():
            self.assertTrue(t["in_pack"], t)

    def test_tool_with_or_without_sh(self):
        self.assertTrue(self.check("kafka-topics")["kafka-topics"]["in_pack"])

    def test_corpus_only(self):
        r = self.check("UnderReplicatedPartitions strimzi.io/manual-rolling-update")
        for t in r.values():
            self.assertFalse(t["in_pack"])
            self.assertTrue(t["in_corpus"])

    def test_absent(self):
        r = self.check("Use --made-up-flag and fake.config.key and InventedThingException")
        self.assertEqual(len(r), 3)
        for t in r.values():
            self.assertFalse(t["in_corpus"])

    def test_dotted_path_segments(self):
        r = self.check("spec.kafka.template.pod")
        self.assertTrue(r["spec.kafka.template.pod"]["in_corpus"])
        self.assertFalse(r["spec.kafka.template.pod"]["in_pack"])

    def test_lowercase_key_needs_full_string(self):
        self.assertFalse(self.check("max.interval.poll.ms")["max.interval.poll.ms"]["in_corpus"])

    def test_endpoint_method_matters(self):
        self.assertTrue(self.check("POST /connectors/{name}/restart")["POST /connectors/{name}/restart"]["in_pack"])
        self.assertFalse(self.check("PUT /connectors/{name}/restart")["PUT /connectors/{name}/restart"]["in_corpus"])
        self.assertTrue(self.check("POST /connectors/{c}/restart")["POST /connectors/{c}/restart"]["in_pack"])

    def test_boundary(self):
        self.assertFalse(ic.matches("--bootstrap-servers", "flag", "--bootstrap-server"))
        self.assertTrue(ic.matches("a --foo-bar.", "flag", "--foo-bar"))


class PipelineTests(unittest.TestCase):
    def test_run_and_determinism(self):
        chunks = {"c1": "t1\nmax.poll.interval.ms and KafkaNodePool",
                  "c2": "t2\n--under-min-isr-partitions flag",
                  "c3": "t3\nstrimzi.io/pause-reconciliation"}
        corpus = "\n".join(chunks.values())
        packs = {"q1": ["c1"]}
        answers = [{"question": "q1", "text": "Set max.poll.interval.ms, use --under-min-isr-partitions, "
                    "strimzi.io/pause-reconciliation and --nonexistent-flag."}]
        res1, _ = ic.run(answers, packs, chunks, corpus)
        res2, _ = ic.run(answers, packs, chunks, corpus)
        self.assertEqual(res1, res2)
        r = res1[0]
        self.assertEqual(r["absent_pack"], ["--under-min-isr-partitions", "strimzi.io/pause-reconciliation", "--nonexistent-flag"])
        self.assertEqual(r["absent_corpus"], ["--nonexistent-flag"])
        a, b = io.StringIO(), io.StringIO()
        ic.report(res1, a)
        ic.report(res2, b)
        self.assertEqual(a.getvalue(), b.getvalue())
        self.assertIn("answers with >=1 token absent from corpus: 1", a.getvalue())

    def test_hash_stable(self):
        self.assertEqual(ic.constants_hash(), ic.constants_hash())
        self.assertEqual(len(ic.constants_hash()), 12)


if __name__ == "__main__":
    unittest.main()
