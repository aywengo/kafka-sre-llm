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
        self.assertFalse(sem["mechanisms"]["V8"]["anchored"])
        self.assertTrue(sem["mechanisms"]["V8"]["reason"])
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


if __name__ == "__main__":
    unittest.main()
