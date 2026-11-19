"""The a3 forbidden-keys table and semantic rules: loader behaviour on a small fixture, and the real
build (work/m4/catalogues_a3/, skipped when not built) checked against the pinned text in raw/."""
import json
import os
import tempfile
import unittest
from pathlib import Path

from kafka_sre_rag import catalogue_a3 as a3

ROOT = Path(__file__).resolve().parent.parent
REAL = ROOT / "work" / "m4" / "catalogues_a3"

FIXTURE = {"schema": 1, "resources": {
    "K": {"config_path": "spec.config", "forbidden_prefixes": ["ssl.", "listeners"], "forbidden_keys": ["name"],
          "exceptions": ["ssl.protocol", "ssl.x."], "exception_globs": ["listeners.*.ok"],
          "source": "f.adoc:5",
          "anchors": [{"role": "forbidden_prefixes", "file": "f.adoc", "line": 5, "items": ["ssl.", "listeners"]},
                      {"role": "forbidden_keys", "file": "f.adoc", "line": 6, "items": ["name"]}]},
    "K:spec.other.config": {"config_path": "spec.other.config", "forbidden_prefixes": [], "forbidden_keys": ["id"],
                            "exceptions": [], "exception_globs": [], "source": "f.adoc:9", "anchors": []}}}


class LoaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        (Path(cls.tmp.name) / "strimzi_forbidden.json").write_text(json.dumps(FIXTURE))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def f(self, kind, key, path=None):
        return a3.forbidden(kind, key, path, directory=self.tmp.name)

    def test_prefix_match_names_prefix_and_line(self):
        r = self.f("K", "ssl.keystore.type")
        self.assertEqual((r["rule"], r["match"], r["line"]), ("prefix", "ssl.", 5))

    def test_key_match(self):
        r = self.f("K", "name")
        self.assertEqual((r["rule"], r["line"]), ("key", 6))

    def test_exact_exception_allows_key(self):
        self.assertIsNone(self.f("K", "ssl.protocol"))

    def test_exception_prefix_ending_in_dot(self):
        self.assertIsNone(self.f("K", "ssl.x.anything"))
        self.assertIsNotNone(self.f("K", "ssl.xy"))

    def test_exact_exception_is_not_a_prefix(self):
        self.assertIsNotNone(self.f("K", "ssl.protocol.extra"))

    def test_exception_glob(self):
        self.assertIsNone(self.f("K", "listeners.abc.ok"))
        self.assertIsNotNone(self.f("K", "listeners.abc.other"))

    def test_allowed_key(self):
        self.assertIsNone(self.f("K", "retries"))

    def test_prefix_without_dot_is_a_prefix(self):
        self.assertIsNotNone(self.f("K", "listeners.x"))

    def test_other_config_path_and_unknown_kind(self):
        self.assertIsNotNone(self.f("K", "id", "spec.other.config"))
        self.assertIsNone(self.f("K", "id"))
        self.assertIsNone(self.f("K", "name", "spec.other.config"))
        self.assertIsNone(self.f("Nope", "name"))
        self.assertIsNotNone(self.f("K", "name", "spec.config"))


@unittest.skipUnless((REAL / "MANIFEST.json").is_file(), "work/m4/catalogues_a3 not built")
class RealTests(unittest.TestCase):
    def test_manifest_verifies(self):
        self.assertTrue(a3.verify(REAL))

    def test_connector_plugin_version(self):
        r = a3.forbidden("KafkaConnector", "connector.plugin.version", directory=REAL)
        self.assertIsNotNone(r)
        self.assertEqual((r["rule"], r["line"]), ("key", 3478))
        self.assertTrue(r["file"].endswith("appendix_crds.adoc"))

    def test_mm2_connector_is_line_3792(self):
        r = a3.forbidden("KafkaMirrorMaker2", "connector.plugin.version",
                         "spec.mirrors[].sourceConnector.config", directory=REAL)
        self.assertEqual(r["line"], 3792)

    def test_prefixes_exceptions_and_allowed_keys(self):
        d = REAL
        self.assertEqual(a3.forbidden("KafkaConnect", "ssl.truststore.location", directory=d)["match"], "ssl.")
        self.assertIsNone(a3.forbidden("KafkaConnect", "ssl.cipher.suites", directory=d))
        self.assertIsNone(a3.forbidden("KafkaConnect", "offset.flush.interval.ms", directory=d))
        self.assertEqual(a3.forbidden("KafkaConnect", "group.id", directory=d)["match"], "group.id")
        self.assertEqual(a3.forbidden("Kafka", "node.id", directory=d)["rule"], "key")
        self.assertEqual(a3.forbidden("Kafka", "listener.security.protocol.map", directory=d)["match"], "listener.")
        self.assertIsNone(a3.forbidden("Kafka", "ssl.enabled.protocols", directory=d))
        self.assertIsNone(a3.forbidden("Kafka", "listener.name.l1-9900.connections.max.reauth.ms", directory=d))
        self.assertIsNone(a3.forbidden("Kafka", "controller.quorum.fetch.timeout.ms", directory=d))
        self.assertIsNotNone(a3.forbidden("Kafka", "controller.quorum.voters", directory=d))
        self.assertIsNone(a3.forbidden("Kafka", "min.insync.replicas", directory=d))
        self.assertIsNotNone(a3.forbidden("CruiseControl", "webserver.http.port", directory=d))
        self.assertIsNone(a3.forbidden("CruiseControl", "webserver.http.cors.enabled", directory=d))
        self.assertIsNotNone(a3.forbidden("KafkaBridge", "bridge.id", directory=d))
        self.assertIsNone(a3.forbidden("KafkaBridge", "ssl.protocol", "spec.consumer.config", directory=d))
        self.assertIsNotNone(a3.forbidden("KafkaBridge", "group.id", "spec.consumer.config", directory=d))
        self.assertIsNone(a3.forbidden("KafkaBridge", "group.id", "spec.producer.config", directory=d))

    def test_every_cited_line_contains_its_items(self):
        res = a3.resources(REAL)
        n = 0
        for k, e in res.items():
            for a in e["anchors"]:
                text = (ROOT / a["file"]).read_text(encoding="utf-8").split("\n")[a["line"] - 1]
                for it in a["items"]:
                    self.assertIn(it, text, "%s %s:%d" % (k, a["file"], a["line"]))
                    n += 1
            for role in ("forbidden_prefixes", "forbidden_keys", "exceptions"):
                for it in e[role]:
                    self.assertTrue(any(a["role"] == role and it in a["items"] for a in e["anchors"]),
                                    "%s %s %s has no anchor" % (k, role, it))
        self.assertGreater(n, 100)

    def test_semantic_anchors_and_unanchored_marks(self):
        sem = a3.semantic_rules(REAL)
        self.assertFalse(sem["mechanisms"]["V4"]["anchored"])
        self.assertTrue(sem["mechanisms"]["V4"]["reason"])
        v8 = sem["mechanisms"]["V8"]          # T1b: anchored to the Kafka source, but as "rejected"
        self.assertTrue(v8["anchored"])
        self.assertFalse(v8["valid_but_wrong"])
        self.assertEqual(v8["outcome"], "rejected")
        self.assertTrue(v8["reason"])
        self.assertNotIn("replicas above", json.dumps(sem["mechanisms"]["V6"]["anchors"]))
        anchors = list(sem["memory_quantity"]["anchors"])
        for k, m in sem["mechanisms"].items():
            if m["anchored"]:
                self.assertTrue(m["anchors"], k)
            anchors += m["anchors"]
        for a in anchors:
            text = (ROOT / a["file"]).read_text(encoding="utf-8").split("\n")[a["line"] - 1]
            self.assertIn(a["quote"], text, "%s:%d" % (a["file"], a["line"]))
        self.assertEqual(123 * 2 ** 20, 128974848)
        self.assertEqual(sem["memory_quantity"]["multipliers"]["binary"]["Mi"], 2 ** 20)


# ------------------------------------------------------------------ T1b: jar check and Java API catalogue
import importlib.util
import sys

_spec = importlib.util.spec_from_file_location("fetch_maven_jars", ROOT / "scripts" / "fetch_maven_jars.py")
fj = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fj)
sys.path.insert(0, str(ROOT / "scripts"))
import build_catalogues_a3 as bld  # noqa: E402


class JarCheckTests(unittest.TestCase):
    def test_wrong_hash_and_size_are_refused(self):
        with tempfile.TemporaryDirectory() as t:
            p = Path(t) / "x.jar"
            p.write_bytes(b"not a jar")
            import hashlib
            good = {"bytes": 9, "sha256": hashlib.sha256(b"not a jar").hexdigest()}
            self.assertIsNone(fj.check(p, good))
            self.assertIn("sha256", fj.check(p, dict(good, sha256="0" * 64)))
            self.assertIn("size", fj.check(p, dict(good, bytes=10)))

    def test_pinned_values_are_those_of_t0(self):
        self.assertEqual(fj.JARS["kafka-clients-4.3.2.jar"]["bytes"], 10322356)
        self.assertEqual(fj.JARS["kafka-streams-4.3.2.jar"]["bytes"], 2290469)
        self.assertEqual(fj.COMMIT, "0a6c96d3b61167f1141df2032f25f10fd6e118fe")


class JavapParserTests(unittest.TestCase):
    TEXT = """Compiled from "A.java"
public class a.b.A<K, V> extends java.lang.Object {
  public static final java.lang.String X_CONFIG = "x.y";
  public static final java.lang.String X_DOC = "long";
  public static final int N = 5;
  public a.b.A(java.util.Map<java.lang.String, java.lang.Object>);
  public <T> java.util.List<T> get(java.lang.String, java.util.Map<java.lang.String, java.lang.Integer>) throws java.io.IOException;
  public static void main(java.lang.String[]);
}
Compiled from "B.java"
class a.b.B {
  public void hidden();
}
"""

    def test_parse(self):
        c = bld.parse_javap(self.TEXT)
        self.assertEqual(list(c), ["a.b.A"])
        a = c["a.b.A"]
        self.assertEqual(a["constants"]["X_CONFIG"]["value"], "x.y")
        self.assertNotIn("X_DOC", a["constants"])
        self.assertEqual(a["constants"]["N"]["value"], "5")
        self.assertEqual(a["constructors"][0]["params"], ["java.util.Map<java.lang.String, java.lang.Object>"])
        g = [m for m in a["methods"] if m["name"] == "get"][0]
        self.assertEqual(g["params"], ["java.lang.String", "java.util.Map<java.lang.String, java.lang.Integer>"])
        self.assertEqual(g["returns"], "java.util.List<T>")


JAVA = REAL / "java_api.json"


@unittest.skipUnless(JAVA.is_file() and (ROOT / "raw_catalogue" / "maven").is_dir(),
                     "java_api.json not built (jars absent)")
class JavaApiTests(unittest.TestCase):
    def test_manifest_covers_java_api(self):
        self.assertIn("java_api.json", a3.manifest(REAL)["files"])
        self.assertTrue(a3.verify(REAL))

    def test_consumer_poll(self):
        ms = a3.java_methods("org.apache.kafka.clients.consumer.KafkaConsumer", "poll", ["java.time.Duration"], REAL)
        self.assertEqual(len(ms), 1)
        self.assertEqual(a3.java_methods("org.apache.kafka.clients.consumer.KafkaConsumer", "poll", ["long"], REAL), [])

    def test_config_constants(self):
        self.assertEqual(a3.java_constant("org.apache.kafka.clients.consumer.ConsumerConfig", "GROUP_ID_CONFIG", REAL),
                         "group.id")
        self.assertEqual(a3.java_constant("org.apache.kafka.clients.producer.ProducerConfig",
                                          "ENABLE_IDEMPOTENCE_CONFIG", REAL), "enable.idempotence")
        self.assertIn("enable.idempotence", a3.client_config_names("producer", REAL))
        self.assertIn("group.id", a3.client_config_names("consumer", REAL))
        self.assertIn("application.id", a3.client_config_names("streams", REAL))

    def test_admin_and_streams(self):
        self.assertIsNotNone(a3.java_class("org.apache.kafka.clients.admin.AdminClient", REAL))
        self.assertTrue(a3.java_methods("org.apache.kafka.clients.admin.Admin", "createTopics", directory=REAL))
        self.assertTrue(a3.java_methods("org.apache.kafka.streams.StreamsBuilder", "stream", directory=REAL))
        self.assertEqual(a3.java_methods("org.apache.kafka.clients.consumer.KafkaConsumer", "pol", directory=REAL), [])

    def test_no_internal_classes_and_sources_resolve(self):
        j = a3.java_api(REAL)
        self.assertFalse([k for k in j["classes"] if ".internals." in k or ".internal." in k])
        c = j["classes"]["org.apache.kafka.clients.consumer.KafkaConsumer"]
        text = (ROOT / c["source"]).read_text(encoding="utf-8").split("\n")[c["line"] - 1]
        self.assertIn("class KafkaConsumer", text)

    def test_v8_and_v4_anchor_lines_contain_their_quotes(self):
        sem = a3.semantic_rules(REAL)["mechanisms"]
        for k in ("V4", "V8"):
            for a in sem[k]["anchors"] + sem[k].get("context_anchors", []):
                text = (ROOT / a["file"]).read_text(encoding="utf-8").split("\n")[a["line"] - 1]
                self.assertIn(a["quote"], text)


if __name__ == "__main__":
    unittest.main()
