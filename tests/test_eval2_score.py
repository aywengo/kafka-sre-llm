"""Tier A scorer (scripts/eval2_score.py): hand-written passing and failing answers for every check type,
format failures, identifier precision, and, when the generated items exist, the reference of every item
passes and its perturbed version fails.

Fixture catalogues only, except the last class, which needs work/m4/catalogues and eval/m4/build/tier_a1.jsonl.
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_score as sc  # noqa: E402
from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues, default_dir  # noqa: E402

FIX = ROOT / "tests" / "fixtures"


def fixture_catalogues():
    d = tempfile.mkdtemp()
    for sub in ("catalogues", "catalogues_ext"):
        for f in (FIX / sub).glob("*.json"):
            shutil.copy(f, d)
    return d


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = fixture_catalogues()
        cls.c = Catalogues.load(cls.dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def run1(self, check, text):
        return sc.run_checks({"id": "x", "checks": [check]}, text, self.c)[0]["pass"]


class AnswerLineTests(Base):
    def test_parse(self):
        self.assertEqual(sc.parse_answer_line("text\nANSWER: max.poll.records"), "max.poll.records")
        self.assertEqual(sc.parse_answer_line("**ANSWER: 5**"), "5")
        self.assertEqual(sc.parse_answer_line("ANSWER: a\nmore\nANSWER: b"), "b")  # the last one counts
        self.assertEqual(sc.parse_answer_line("ANSWER: .*"), ".*")
        self.assertIsNone(sc.parse_answer_line("The answer is 5"))
        self.assertIsNone(sc.parse_answer_line("answer: 5"))

    def test_missing_line_is_a_format_failure(self):
        item = {"id": "i", "checks": [{"type": "answer_line", "format": True},
                                      {"type": "answer_equals", "accepted": ["x"], "norm": "exact"}]}
        row = sc.score_item(item, "I think it is x.", self.c)
        self.assertFalse(row["format_ok"])
        self.assertFalse(row["all_pass"])
        row = sc.score_item(item, "I think so.\nANSWER: y", self.c)
        self.assertTrue(row["format_ok"])
        self.assertFalse(row["all_pass"])
        self.assertTrue(sc.score_item(item, "ANSWER: x", self.c)["all_pass"])


class ValueChecks(Base):
    def test_answer_equals(self):
        chk = {"type": "answer_equals", "accepted": ["300000", "300000 (5 minutes)", "5 minutes"], "norm": "ci"}
        self.assertTrue(self.run1(chk, "ANSWER: 300000"))
        self.assertTrue(self.run1(chk, "ANSWER: `300,000`"))
        self.assertTrue(self.run1(chk, "ANSWER: 300000 (5 minutes)."))
        self.assertTrue(self.run1(chk, "ANSWER: 5 minutes"))
        self.assertFalse(self.run1(chk, "ANSWER: 30000"))
        self.assertFalse(self.run1(chk, "ANSWER: 300000 ms"))
        self.assertFalse(self.run1(chk, "300000"))

    def test_exact_is_case_sensitive_and_upper_snake_is_lenient(self):
        self.assertFalse(self.run1({"type": "answer_equals", "accepted": ["Foo.Bar"], "norm": "exact"}, "ANSWER: foo.bar"))
        chk = {"type": "answer_equals", "accepted": ["NOT_ENOUGH_REPLICAS"], "norm": "upper_snake"}
        self.assertTrue(self.run1(chk, "ANSWER: not enough replicas"))
        self.assertTrue(self.run1(chk, "ANSWER: `NOT_ENOUGH_REPLICAS`"))
        self.assertFalse(self.run1(chk, "ANSWER: NOT_ENOUGH_REPLICAS_AFTER_APPEND"))

    def test_answer_set(self):
        chk = {"type": "answer_set", "accepted": ["none", "all"]}
        self.assertTrue(self.run1(chk, "ANSWER: all, none"))
        self.assertTrue(self.run1(chk, "ANSWER: None; All"))
        self.assertTrue(self.run1(chk, "ANSWER: [none, all]"))
        self.assertFalse(self.run1(chk, "ANSWER: none"))
        self.assertFalse(self.run1(chk, "ANSWER: none, all, class"))

    def test_verdict_three_way(self):
        for exp, good, bad in (("exists", "exists", "does not exist"),
                               ("does not exist", "Does not exist.", "not in my sources"),
                               ("not in my sources", "not in my sources", "does not exist")):
            chk = {"type": "answer_verdict", "expected": exp}
            self.assertTrue(self.run1(chk, "ANSWER: " + good), (exp, good))
            self.assertFalse(self.run1(chk, "ANSWER: " + bad), (exp, bad))
        chk = {"type": "answer_verdict", "expected": "exists"}
        self.assertFalse(self.run1(chk, "ANSWER: probably exists"))  # not one of the three
        self.assertFalse(self.run1(chk, "It exists."))

    def test_letter(self):
        chk = {"type": "answer_letter", "expected": "C"}
        for good in ("C", "(c)", "C) it means", "c."):
            self.assertTrue(self.run1(chk, "ANSWER: " + good), good)
        self.assertFalse(self.run1(chk, "ANSWER: B"))
        self.assertFalse(self.run1(chk, "ANSWER: Connect"))


class RestChecks(Base):
    def test_method_and_path(self):
        m = {"type": "rest_method", "method": "POST"}
        p = {"type": "rest_path", "template": "/connectors/{connector}/restart"}
        self.assertTrue(self.run1(m, "ANSWER: POST /connectors/orders-sink/restart"))
        self.assertTrue(self.run1(p, "ANSWER: POST /connectors/orders-sink/restart"))
        self.assertTrue(self.run1(p, "ANSWER: `POST /connectors/{name}/restart?includeTasks=true`"))
        self.assertFalse(self.run1(m, "ANSWER: PUT /connectors/orders-sink/restart"))  # the trap
        self.assertTrue(self.run1(p, "ANSWER: PUT /connectors/orders-sink/restart"))   # path alone is right
        self.assertFalse(self.run1(p, "ANSWER: POST /connectors/orders-sink/pause"))
        self.assertFalse(self.run1(m, "ANSWER: restart it"))
        self.assertFalse(self.run1(p, "ANSWER: restart it"))


class ArgoChecks(Base):
    spec = {"type": "cmdline_spec", "command": "argocd app sync", "required_flags": ["--prune"],
            "tokens": ["my-app"], "flag_alternatives": {}}
    bad = {"type": "no_invented_cli", "tool": "argocd"}

    def test_cmdline_spec(self):
        self.assertTrue(self.run1(self.spec, "```bash\nargocd app sync my-app --prune\n```"))
        self.assertTrue(self.run1(self.spec, "Run `argocd app sync --prune my-app` now."))
        self.assertTrue(self.run1(self.spec, "$ argocd app sync my-app \\\n  --prune"))
        self.assertFalse(self.run1(self.spec, "```\nargocd app sync my-app\n```"))
        self.assertFalse(self.run1(self.spec, "```\nargocd app get my-app --prune\n```"))
        self.assertFalse(self.run1(self.spec, "```\nargocd app sync other --prune\n```"))
        # one good line among others is enough
        self.assertTrue(self.run1(self.spec, "```\nargocd app get my-app\nargocd app sync my-app --prune\n```"))

    def test_short_flag_alternative(self):
        spec = dict(self.spec, required_flags=["--prune", "--dry-run"], flag_alternatives={"--dry-run": ["-n"]})
        self.assertTrue(self.run1(spec, "```\nargocd app sync my-app --prune -n\n```"))
        self.assertFalse(self.run1(spec, "```\nargocd app sync my-app --prune\n```"))

    def test_no_invented_flag_or_command(self):
        self.assertTrue(self.run1(self.bad, "```\nargocd app sync my-app --prune --dry-run\n```"))
        self.assertTrue(self.run1(self.bad, "```\nargocd app sync my-app --server x\n```"))  # inherited flag
        self.assertFalse(self.run1(self.bad, "```\nargocd app sync my-app --auto-prune\n```"))
        self.assertFalse(self.run1(self.bad, "```\nargocd app syncc my-app\n```"))
        # any line counts, not only the one that meets the specification
        self.assertFalse(self.run1(self.bad, "```\nargocd app sync my-app --prune\nargocd app sync --nope\n```"))

    def test_has_command_is_the_format_check(self):
        item = {"id": "a", "checks": [{"type": "has_command", "format": True}, self.spec, self.bad]}
        row = sc.score_item(item, "You can sync it from the UI.", self.c)
        self.assertFalse(row["format_ok"])
        row = sc.score_item(item, "```\nargocd app sync my-app\n```", self.c)
        self.assertTrue(row["format_ok"])
        self.assertFalse(row["all_pass"])
        self.assertEqual((row["checks_passed"], row["checks_total"]), (2, 3))


class IdentifierPrecision(Base):
    def ids(self, text):
        return sc.classify_identifiers(text, self.c)

    def test_made_up_key_is_caught(self):
        r = self.ids("Set `max.poll.intervals.ms` to 600000 and `max.poll.interval.ms` too.")
        self.assertEqual([x["token"] for x in r["unknown"]], ["max.poll.intervals.ms"])
        self.assertEqual(r["unknown"][0]["similar"], ["max.poll.interval.ms"])
        self.assertEqual([x["token"] for x in r["known"]], ["max.poll.interval.ms"])

    def test_key_of_an_incomplete_catalogue_is_not_covered_never_unknown(self):
        r = self.ids("Use `strimzi.io/no-such-annotation` and spring.kafka.consumer.max-poll-interval-ms. "
                     "Then run kafka-topics.sh --describe --unparsed --topic t")
        self.assertEqual(r["unknown"], [])
        nc = {x["token"] for x in r["not_covered"]}
        self.assertIn("strimzi.io/no-such-annotation", nc)
        self.assertIn("spring.kafka.consumer.max-poll-interval-ms", nc)
        self.assertIn("kafka-topics.sh --unparsed", nc)
        self.assertIn("kafka-topics.sh --describe", {x["token"] for x in r["known"]})

    def test_complete_tool_gives_unknown(self):
        r = self.ids("kafka-streams-application-reset.sh --application-id a --no-such")
        self.assertEqual([x["token"] for x in r["unknown"]], ["kafka-streams-application-reset.sh --no-such"])

    def test_rest_pairs(self):
        r = self.ids("Run curl -X PUT http://connect:8083/connectors/orders/restart\nand then GET /connectors/orders/status")
        self.assertEqual([x["token"] for x in r["unknown"]], ["PUT /connectors/orders/restart"])
        self.assertEqual(r["unknown"][0]["similar"], ["POST"])
        self.assertEqual([x["token"] for x in r["known"]], ["GET /connectors/orders/status"])
        # a full stop after the path belongs to the sentence
        r = self.ids("Use POST /connectors/orders/restart. Then `GET /connectors/orders/status`.")
        self.assertEqual(r["unknown"], [])
        self.assertEqual(len(r["known"]), 2)

    def test_argocd_and_errors(self):
        r = self.ids("```\nargocd app sync a --prune --made-up\nargocd app frob b\n```\n"
                     "NOT_ENOUGH_REPLICAS_AFTER_APPEND and NOT_ENOUGH_REPLICA_AFTER_APPEND, KAFKA_HEAP_OPTS")
        unk = {x["token"] for x in r["unknown"]}
        self.assertEqual(unk, {"argocd app sync --made-up", "argocd app frob", "NOT_ENOUGH_REPLICA_AFTER_APPEND"})
        known = {x["token"] for x in r["known"]}
        self.assertIn("argocd app sync --prune", known)
        self.assertIn("NOT_ENOUGH_REPLICAS_AFTER_APPEND", known)
        everything = unk | known | {x["token"] for x in r["not_covered"]}
        self.assertNotIn("KAFKA_HEAP_OPTS", everything)  # an environment variable, not an error name

    def test_strimzi_yaml_invented_field(self):
        text = ("```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nspec:\n  quotas:\n    consumer: 3\n"
                "    producerByteRate: 5\n```")
        r = self.ids(text)
        self.assertEqual([x["token"] for x in r["unknown"]], ["KafkaUser spec.quotas.consumer"])
        self.assertIn("KafkaUser spec.quotas.producerByteRate", {x["token"] for x in r["known"]})

    def test_urls_files_and_versions_are_not_keys(self):
        r = self.ids("See https://kafka.apache.org/documentation and server.properties, version 4.3.2, "
                     "apiVersion: kafka.strimzi.io/v1, e.g. this.")
        self.assertEqual(r, {"known": [], "unknown": [], "not_covered": []})

    def test_summary_lists_unknown_and_counts(self):
        item = {"id": "i1", "checks": [{"type": "answer_line", "format": True}], "kind": "k", "domain": "d",
                "failure_class": "f", "overlap_band": "low", "anchored": True}
        rows, missing, extra = sc.score_files([item, dict(item, id="i2")],
                                              [{"id": "i1", "text": "ANSWER: x `no.such.key.here`"},
                                               {"id": "zz", "text": "ANSWER: x"}], self.c)
        s = sc.summarise(rows, [item, dict(item, id="i2")], missing, extra)
        self.assertEqual((s["items_scored"], s["missing_answers"], s["answers_for_unknown_ids"]), (2, 1, 1))
        self.assertEqual(s["all_pass"], 1)
        self.assertEqual(s["by_kind"]["k"]["n"], 2)
        self.assertEqual(s["identifier_precision"]["unknown"], 0)  # `no.such.key.here`: first word is no key prefix


class StatTests(unittest.TestCase):
    def test_wilson(self):
        lo, hi = sc.wilson(8, 10)
        self.assertAlmostEqual(lo, 0.4902, places=3)
        self.assertAlmostEqual(hi, 0.9433, places=3)
        self.assertEqual(sc.wilson(0, 0), (None, None))
        self.assertEqual(sc.wilson(10, 10)[1], 1.0)


ITEMS = ROOT / "eval" / "m4" / "build" / "tier_a1.jsonl"
HAVE = ITEMS.is_file() and all((default_dir() / (n + ".json")).is_file() for n in
                               ("kafka_config", "protocol_errors", "connect_rest", "argocd", "kafka_cli"))


@unittest.skipUnless(HAVE, "generated items or real catalogues absent: run scripts/build_catalogues.py and "
                           "scripts/eval2_items_a1.py")
class GeneratedItemsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Catalogues.load()
        cls.items = [json.loads(l) for l in ITEMS.read_text().splitlines() if l.strip()]

    def test_reference_of_every_item_passes_and_perturbed_fails(self):
        self.assertGreater(len(self.items), 100)
        for it in self.items:
            res = sc.run_checks(it, it["reference"], self.c)
            self.assertTrue(all(r["pass"] for r in res), (it["id"], res))
            pres = sc.run_checks(it, it["perturbed_reference"], self.c)
            self.assertFalse(pres[it["perturbed_violates"]]["pass"], (it["id"], pres))
            self.assertFalse(all(r["pass"] for r in pres), it["id"])

    def test_scorer_summary_on_references_and_perturbed(self):
        for field, expect_all in (("reference", True), ("perturbed_reference", False)):
            rows, missing, extra = sc.score_files(self.items, [{"id": i["id"], "text": i[field]} for i in self.items], self.c)
            self.assertEqual((missing, extra), (0, 0))
            self.assertEqual(all(r["all_pass"] for r in rows), expect_all)
            if not expect_all:
                self.assertEqual(sum(r["all_pass"] for r in rows), 0)

    def test_references_use_no_invented_identifier(self):
        rows, _, _ = sc.score_files(self.items, [{"id": i["id"], "text": i["reference"]} for i in self.items], self.c)
        # is_real items quote their (fake) subject only in the question, never in the reference
        self.assertEqual([r["id"] for r in rows if r["identifiers"].get("unknown")], [])


if __name__ == "__main__":
    unittest.main()
