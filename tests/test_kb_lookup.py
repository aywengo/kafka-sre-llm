"""Offline tests of kafka_sre_rag.kb_lookup: small synthetic catalogues, and a few real keys when the catalogues are on disk."""
import json
import tempfile
import unittest
from pathlib import Path

from kafka_sre_rag import kb_lookup as kb
from kafka_sre_rag.catalogue import Catalogues, default_dir

PIN = "apache-kafka-config@" + "a" * 40 + "#aaaaaaaaaaaa"


def _cfg(key, scope, default="1", typ="int", valid="[1,...]", desc="A first sentence. A second sentence."):
    return {"key": key, "scope": scope, "default": default, "default_value": default, "type": typ,
            "valid_values": valid, "valid_values_list": None, "description": desc, "importance": "high",
            "pin": PIN, "provenance": ["exists"]}


def synthetic() -> Catalogues:
    paths = {"spec": {"required": True, "type": ["object"]},
             "spec.mode": {"enum": ["a", "b"], "required": True, "type": ["string"]},
             "spec.size": {"type": ["integer"]}}
    return Catalogues({
        "kafka_config": {"complete": True, "versions": ["4.3"], "entries": [
            _cfg("min.insync.replicas", "broker"), _cfg("min.insync.replicas", "topic"),
            _cfg("compression.type", "broker", "producer", "string", "[gzip, producer]"),
            _cfg("compression.type", "producer", "none", "string", "[none, gzip]"),
            _cfg("log.retention.ms", "broker", "604800000", "long", "[-1,...]", "x" * 600)]},
        "kafka_config_history": {"complete": True, "versions": ["4.1", "4.2", "4.3"], "entries": [
            {"key": "log.retention.ms", "scope": "broker", "changes": ["default_changed"],
             "versions": {"4.1": {"default_value": "1", "type": "long"}, "4.2": None,
                          "4.3": {"default_value": "604800000", "type": "long"}}}]},
        "kafka_cli": {"complete": False, "bin_complete": True, "bin_tools": ["kafka-topics.sh"], "versions": ["4.3.2"],
                      "entries": [{"tool": "kafka-topics.sh", "complete": True, "in_bin": True,
                                   "pin": "apache-kafka-tools@4.3.2#bbbbbbbbbbbb", "constraints": [],
                                   "options": {"--partitions": {"description": "The number of partitions.",
                                                                "evidence": "source_code", "files": [],
                                                                "takes_argument": "required"},
                                               "--topic": {"description": "The topic.", "evidence": "source_code",
                                                           "files": [], "takes_argument": "required"}}}]},
        "strimzi_resources": {"complete": True, "versions": ["strimzi 1.3.1"], "entries": [
            {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaThing", "complete": True, "source": "crd",
             "pin": "strimzi-install@1.3.1#cccccccccccc", "paths": paths}], "reference_entries": []},
        "strimzi_annotations": {"complete": False, "entries": [
            {"name": "strimzi.io/thing", "kind": "annotation", "values_seen": ["true"], "pin": "strimzi@1.3.1#dddd"}]},
        "protocol_errors": {"complete": True, "versions": ["4.3"], "entries": [
            {"name": "SOME_ERROR", "code": 7, "retriable": True, "description": "Something failed.", "pin": PIN}]},
        "connect_rest": {"complete": True, "versions": ["4.3"], "entries": [
            {"method": "PUT", "path": "/connectors/{connector}/pause", "summary": "Pause it", "operation_id": "p",
             "path_params": ["connector"], "pin": PIN},
            {"method": "PUT", "path": "/connectors/{connector}/resume", "summary": "Resume it", "operation_id": "r",
             "path_params": ["connector"], "pin": PIN}]},
    })


class Synthetic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kb = kb.KbLookup(synthetic(), verify=False)

    def cards(self, text, **kw):
        return self.kb.cards(text, **kw) if kw else self.kb.cards(text)

    def by_id(self, cs):
        return {c.ident: c for c in cs}

    def test_key_with_default_and_history(self):
        c = self.by_id(self.cards("Set `log.retention.ms` on the broker."))["log.retention.ms"]
        f = dict(c.facts)
        self.assertEqual(c.kind, "config_key")
        self.assertEqual(f["default"], "604800000")
        self.assertEqual(f["type"], "long")
        self.assertIn("history (broker)", f)
        self.assertIn("4.1 default_value=1", f["history (broker)"])
        self.assertIn("4.2 absent", f["history (broker)"])
        self.assertIn("#aaaaaaaaaaaa", c.pin)
        self.assertNotIn("a" * 40, c.pin)           # the commit is shortened
        self.assertLessEqual(len(f["description"]), kb.DESC_CHARS + 4)

    def test_key_in_two_scopes_is_one_card(self):
        cs = [c for c in self.cards("`compression.type` and compression.type again") if c.ident == "compression.type"]
        self.assertEqual(len(cs), 1)
        f = dict(cs[0].facts)
        self.assertEqual(f["scopes"], "broker, producer")
        self.assertIn("broker: producer", f["default"])
        self.assertIn("producer: none", f["default"])
        same = dict(self.by_id(self.cards("min.insync.replicas"))["min.insync.replicas"].facts)
        self.assertEqual(same["default"], "1")      # identical in both scopes: no per-scope split

    def test_option_of_a_tool(self):
        cs = self.by_id(self.cards("```bash\nkafka-topics.sh --create --topic x --partitions 3\n```"))
        c = cs["kafka-topics.sh --partitions"]
        self.assertEqual(c.kind, "cli_option")
        self.assertEqual(dict(c.facts)["description"], "The number of partitions.")
        self.assertNotIn("kafka-topics.sh --create", cs)     # not in this catalogue: no card

    def test_strimzi_path_and_kind(self):
        text = "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaThing\nspec:\n  mode: a\n```"
        cs = self.by_id(self.cards(text))
        f = dict(cs["KafkaThing spec.mode"].facts)
        self.assertEqual(f["enum"], "a, b")
        self.assertEqual(f["required"], "yes")
        self.assertEqual(cs["KafkaThing"].kind, "resource_kind")
        self.assertNotIn("KafkaThing spec", cs)

    def test_other_kinds(self):
        cs = self.by_id(self.cards("SOME_ERROR and strimzi.io/thing and PUT /connectors/x/pause"))
        self.assertEqual(dict(cs["SOME_ERROR"].facts)["code"], "7")
        self.assertIn("true", dict(cs["strimzi.io/thing"].facts)["values seen"])
        # a wrong method on a known path still gives the card, which lists the right one
        self.assertIn("PUT", dict(cs["/connectors/{connector}/pause"].facts)["operations"])

    def test_unknown_gives_no_card_and_no_statement(self):
        text = ("`made.up.setting` and `kafka-nothing.sh --x` and NOT_AN_ERROR and strimzi.io/nothing "
                "and kind: KafkaMissing")
        self.assertEqual(self.cards(text), [])
        self.assertEqual(kb.render(self.cards(text)), "")
        self.assertNotIn("not exist", kb.render(self.cards("`made.up.setting` min.insync.replicas")).lower())

    def test_case_and_sources(self):
        self.assertEqual([c.ident for c in self.cards("MIN.INSYNC.REPLICAS", sources=("profiles",))],
                         ["min.insync.replicas"])
        self.assertEqual(self.cards("MIN.INSYNC.REPLICAS", sources=("checker",)), [])

    def test_order_is_first_mention_and_deterministic(self):
        text = "First log.retention.ms, then SOME_ERROR, then min.insync.replicas, then log.retention.ms."
        a = [c.ident for c in self.cards(text)]
        self.assertEqual(a, ["log.retention.ms", "SOME_ERROR", "min.insync.replicas"])
        for _ in range(3):
            self.assertEqual([c.ident for c in self.cards(text)], a)
        self.assertEqual(kb.render(self.cards(text)), kb.render(self.cards(text)))

    def test_budget_cuts_whole_cards(self):
        cs = self.cards("log.retention.ms SOME_ERROR min.insync.replicas")
        full = kb.render(cs, 100000)
        for c in cs:
            self.assertIn(c.text(), full)
        total = len(kb.HEADER) + 1 + sum(len(c.text()) + 1 for c in cs)
        self.assertLessEqual(len(full), total)
        limit = len(kb.HEADER) + 1 + len(cs[0].text()) + 1 + len(cs[1].text()) + 1
        part = kb.render(cs, limit)
        self.assertLessEqual(len(part), limit)
        self.assertIn(cs[0].text(), part)
        self.assertIn(cs[1].text(), part)
        self.assertNotIn(cs[2].text(), part)
        for line in part.splitlines():          # no card is cut inside
            self.assertTrue(line.startswith("- ") or line.startswith("    ") or line == kb.HEADER)
        self.assertEqual(kb.render(cs, 50), "")          # nothing fits: no block at all

    def test_render_marks_itself_and_pin(self):
        out = kb.render(self.cards("min.insync.replicas"))
        self.assertTrue(out.startswith("REFERENCE CARDS"))
        self.assertIn("not documentation excerpts", out)
        self.assertIn("pin apache-kafka-config#aaaaaaaaaaaa", out)

    def test_cut_sentence(self):
        self.assertEqual(kb.cut_sentence("One. Two. Three.", 10), "One. Two.")
        self.assertEqual(kb.cut_sentence("short", 50), "short")
        self.assertTrue(kb.cut_sentence("word " * 50, 40).endswith("..."))

    def test_hash_check(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "kafka_config.json").write_text("{}")
            from kafka_sre_rag.catalogue import catalogue_hash
            (Path(d) / "CATALOGUES.json").write_text(json.dumps({"catalogue_hash": catalogue_hash(d)}))
            kb.KbLookup._verify(Path(d))
            (Path(d) / "kafka_config.json").write_text("{ }")
            with self.assertRaises(kb.CatalogueHashError):
                kb.KbLookup._verify(Path(d))


@unittest.skipUnless((default_dir() / "CATALOGUES.json").is_file(), "pinned catalogues not on disk")
class Real(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kb = kb.KbLookup.load()

    def test_real_key_scopes_default_history(self):
        cs = {c.ident: c for c in self.kb.cards("compression.type, retention.ms and log.message.timestamp.after.max.ms")}
        self.assertEqual(dict(cs["compression.type"].facts)["scopes"], "broker, producer, topic")
        self.assertIn("604800000", dict(cs["retention.ms"].facts)["default"])

    def test_real_history_present_for_a_changed_key(self):
        key = next(iter(self.kb._hist))
        c = self.kb.config_card(key)
        self.assertTrue(any(k.startswith("history") for k, _ in c.facts), key)

    def test_real_option_path_and_unknown(self):
        txt = ("```bash\nkafka-configs.sh --alter --entity-type topics --bootstrap-server b:9092\n```\n"
               "KafkaUser spec.quotas.consumerByteRate and `no.such.key.at.all`")
        ids = [c.ident for c in self.kb.cards(txt)]
        self.assertIn("kafka-configs.sh --alter", ids)
        self.assertIn("KafkaUser spec.quotas.consumerByteRate", ids)
        self.assertNotIn("no.such.key.at.all", ids)


if __name__ == "__main__":
    unittest.main()
