"""Schema validation, CLI completeness lookups, configuration history and operator facts of
kafka_sre_rag.catalogue, on small catalogues built in memory with the parsers of build_catalogues.py."""
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import build_catalogues as bc  # noqa: E402
from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues  # noqa: E402
try:
    from tests.test_build_catalogue_sources import CRD  # noqa: E402
except ImportError:      # run with the tests directory as the start directory
    from test_build_catalogue_sources import CRD  # noqa: E402


def catalogues():
    crd = bc.crd_entries([(Path("thing.yaml"), CRD)], "pin")
    defs = {
        "io.k8s.api.apps.v1.Box": {
            "type": "object", "required": ["spec"],
            "x-kubernetes-group-version-kind": [{"group": "apps", "kind": "Box", "version": "v1"}],
            "properties": {"apiVersion": {"type": "string"}, "kind": {"type": "string"},
                           "metadata": {"$ref": "#/definitions/Meta"},
                           "spec": {"type": "object", "properties": {
                               "replicas": {"type": "integer", "format": "int32"},
                               "cpu": {"$ref": "#/definitions/io.k8s.apimachinery.pkg.api.resource.Quantity"},
                               "limits": {"type": "object", "additionalProperties": {
                                   "$ref": "#/definitions/io.k8s.apimachinery.pkg.api.resource.Quantity"}}}}}},
        "Meta": {"type": "object"},
        "io.k8s.apimachinery.pkg.api.resource.Quantity": {"type": "string"},
    }
    paths = bc.flatten_schema(defs["io.k8s.api.apps.v1.Box"], defs)
    box = {"kind": "Box", "api_version": "apps/v1", "complete": True, "provenance": ["exists"], "source": "openapi",
           "paths": paths}
    return Catalogues({
        "strimzi_resources": {"complete": True, "entries": crd, "reference_entries": []},
        "kubernetes": {"complete": False, "resources": [box], "cli": []},
        "kafka_cli": {"complete": False, "bin_complete": True, "bin_tools": ["kafka-topics.sh", "kafka-cluster.sh"],
                      "entries": [
                          {"tool": "kafka-topics.sh", "in_bin": True, "complete": True, "provenance": ["exists"],
                           "options": {"--describe": {"evidence": "source_code", "files": ["T.java"], "takes_argument": "none",
                                                      "description": "List details."}},
                           "constraints": [{"kind": "checkRequiredArgs", "args": ["--bootstrap-server"], "source": "T.java:1"}]},
                          {"tool": "kafka-cluster.sh", "in_bin": True, "complete": False, "reason": "not read",
                           "provenance": ["exists"], "options": {"--id": {"evidence": "seen_in_example", "files": ["x"]}}}]},
        "kafka_config_history": {"complete": True, "entries": [
            {"scope": "broker", "key": "a.b", "changes": ["default"], "versions": {"4.1": None}}]},
    })


def doc(**spec):
    return {"apiVersion": "kafka.example.io/v1", "kind": "Thing", "metadata": {"name": "x"}, "spec": dict({"name": "n"}, **spec)}


class ValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = catalogues()

    def codes(self, d, **kw):
        return [(v.code, v.path) for v in self.c.validate_resource(d, **kw).violations]

    def test_valid_document_passes(self):
        r = self.c.validate_resource(doc(mode="a", size=3, tags=["x"], labels={"a": "b"}, config={"any": {"thing": 1}}))
        self.assertTrue(r.ok, r.violations)
        self.assertTrue(r.covered)
        self.assertEqual(r.source, "crd")

    def test_unknown_field_is_an_error_with_a_suggestion(self):
        v = self.c.validate_resource(doc(nmae="x")).violations
        self.assertEqual([(x.code, x.path) for x in v], [("unknown_field", "spec.nmae")])
        d = doc()
        d["spec"]["labell"] = {}
        self.assertEqual(self.codes(d), [("unknown_field", "spec.labell")])
        self.assertIn("labels", self.c.validate_resource(d).violations[0].similar)

    def test_unknown_field_below_a_free_form_field_is_not_an_error(self):
        self.assertTrue(self.c.validate_resource(doc(config={"a": {"b": [1, 2]}})).ok)
        self.assertTrue(self.c.validate_resource(doc(labels={"anything": "goes"})).ok)

    def test_wrong_types(self):
        self.assertEqual(self.codes(doc(name=3)), [("wrong_type", "spec.name")])
        self.assertEqual(self.codes(doc(size=[1])), [("wrong_type", "spec.size")])
        self.assertEqual(self.codes(doc(labels={"a": 1})), [("wrong_type", "spec.labels.a")])
        self.assertEqual(self.codes(doc(items="x")), [("wrong_type", "spec.items")])
        self.assertEqual(self.codes(doc(tags=[1])), [("wrong_type", "spec.tags[0]")])
        self.assertEqual(self.codes(doc(size=3)), [])          # integer or string
        self.assertEqual(self.codes(doc(size="3Gi")), [])
        d = doc()
        d["status"] = {"ok": "yes"}
        self.assertEqual(self.codes(d), [("wrong_type", "status.ok")])

    def test_enumerations(self):
        self.assertEqual(self.codes(doc(mode="c")), [("bad_enum", "spec.mode")])
        self.assertEqual(self.codes(doc(tags=["x", "z"])), [("bad_enum", "spec.tags[1]")])
        self.assertEqual(self.codes(doc(mode="b")), [])

    def test_required_and_one_of(self):
        d = doc()
        del d["spec"]["name"]
        self.assertEqual(self.codes(d), [("missing_required", "spec.name")])
        d = {"apiVersion": "kafka.example.io/v1", "kind": "Thing"}
        self.assertEqual(self.codes(d), [("missing_required", "spec")])
        self.assertEqual(self.codes(doc(items=[{"value": "1"}])), [])
        self.assertEqual(self.codes(doc(items=[{"value": "1", "valueFrom": {}}])), [("one_of", "spec.items[0]")])
        self.assertEqual(self.codes(doc(items=[{}])), [("one_of", "spec.items[0]")])

    def test_item_paths_carry_the_index(self):
        d = doc(items=[{"value": "1"}, {"value": "1", "valu": "2"}])
        self.assertEqual(self.codes(d), [("unknown_field", "spec.items[1].valu")])

    def test_unserved_version_and_unknown_kind(self):
        d = doc()
        d["apiVersion"] = "kafka.example.io/v0"
        self.assertEqual(self.codes(d), [("unknown_api_version", "apiVersion")])
        d = doc()
        d["kind"] = "Thang"
        self.assertEqual(self.codes(d), [("unknown_kind", "kind")])
        d = {"apiVersion": "other.io/v1", "kind": "Thing", "spec": {"anything": 1}}
        self.assertEqual(self.codes(d), [("unknown_api_version", "apiVersion")])   # Thing exists, in another group
        d = {"apiVersion": "other.io/v1", "kind": "Unrelated", "spec": {"anything": 1}}
        r = self.c.validate_resource(d)
        self.assertFalse(r.covered)           # no complete schema for that group: nothing is claimed
        self.assertTrue(r.ok)

    def test_metadata_and_envelope_are_not_checked(self):
        d = doc()
        d["metadata"] = {"whatever": [1]}
        self.assertTrue(self.c.validate_resource(d).ok)

    def test_not_a_mapping_and_missing_kind(self):
        self.assertEqual(self.codes([1]), [("not_a_mapping", "")])
        self.assertEqual(self.codes({"spec": {}}), [("missing_kind", "")])

    def test_null_values_are_skipped(self):
        self.assertTrue(self.c.validate_resource(doc(mode=None, labels=None)).ok)

    def test_quantity_takes_numbers_and_strings(self):
        box = {"apiVersion": "apps/v1", "kind": "Box", "spec": {"replicas": 2, "cpu": 1, "limits": {"cpu": 0.5, "memory": "1Gi"}}}
        self.assertTrue(self.c.validate_resource(box).ok, self.c.validate_resource(box).violations)
        box["spec"]["replicas"] = "2"
        self.assertEqual(self.codes(box), [("wrong_type", "spec.replicas")])
        del box["spec"]
        self.assertEqual(self.codes(box), [("missing_required", "spec")])


class LookupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = catalogues()

    def test_complete_tool_says_unknown_incomplete_says_not_covered(self):
        self.assertEqual(self.c.cli_option("kafka-topics.sh", "--describe").status, KNOWN)
        r = self.c.cli_option("kafka-topics.sh", "--unparsed")
        self.assertEqual(r.status, UNKNOWN)
        self.assertEqual(r.evidence, "source_code")
        self.assertEqual(self.c.cli_option("kafka-cluster.sh", "--nope").status, NOT_COVERED)
        self.assertEqual(self.c.cli_option("kafka-topics.sh", "--describe").data["takes_argument"], "none")

    def test_tool_listing(self):
        self.assertEqual(self.c.cli_tool("kafka-topics.sh").status, KNOWN)
        self.assertEqual(self.c.cli_tool("bin/kafka-clusters.sh").status, UNKNOWN)
        self.assertIn("kafka-cluster.sh", self.c.cli_tool("kafka-clusters.sh").data["similar"])
        self.assertEqual(self.c.cli_tool("zookeeper-shell.sh").status, NOT_COVERED)

    def test_constraints(self):
        self.assertEqual(self.c.cli_constraints("kafka-topics")[0]["kind"], "checkRequiredArgs")

    def test_config_history(self):
        self.assertEqual(self.c.config_history("a.b").status, KNOWN)
        self.assertEqual(self.c.config_history("x.y").status, UNKNOWN)

    def test_operator_facts_absent_means_not_covered(self):
        self.assertEqual(self.c.operator_label("name").status, NOT_COVERED)
        self.assertEqual(self.c.operator_pod_labels(), {})


if __name__ == "__main__":
    unittest.main()
