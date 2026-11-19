"""Tier A scorer (scripts/eval2_score.py): hand-written passing and failing answers for every check type,
format failures, identifier precision, and, when the generated items exist, the reference of every item
passes and its perturbed version fails.

Fixture catalogues only, except the last class, which needs work/m4/catalogues and eval/m4/build/tier_a1.jsonl.

The check types of slice A2 (strict YAML validation of the one document of the requested kind, field equals,
field absent, config keys of a free-form map, a command line for a tool with required, forbidden and unknown
flags, a forbidden old value) are tested on small catalogues built in memory (as test_catalogue_validate.py
does) and, when they exist, on the completed catalogues.
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
sys.path.insert(0, str(ROOT / "tests"))
import test_catalogue_validate as tcv  # noqa: E402
HAVE_REAL = (default_dir() / "CATALOGUES.json").is_file()


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


def small_catalogues():
    """The catalogues of test_catalogue_validate (a kind `Thing`, kafka-topics.sh complete with --describe only)
    with a complete kafka-consumer-groups.sh that has availableIf and checkInvalidArgs rules."""
    c = tcv.catalogues()
    ent = {"tool": "kafka-consumer-groups.sh", "in_bin": True, "complete": True, "provenance": ["exists"],
           "options": {o: {"evidence": "source_code", "files": ["C.java"], "takes_argument": t} for o, t in (
               ("--bootstrap-server", "required"), ("--describe", "none"), ("--members", "none"), ("--state", "optional"),
               ("--group", "required"), ("--reset-offsets", "none"), ("--dry-run", "none"), ("--execute", "none"),
               ("--to-earliest", "none"), ("--to-latest", "none"), ("--list", "none"))},
           "constraints": [{"kind": "availableIf", "option": "--members", "others": ["--describe"], "source": "C.java:1"},
                           {"kind": "availableIf", "option": "--state", "others": ["--describe", "--list"], "source": "C.java:2"},
                           {"kind": "checkInvalidArgs", "args": ["--to-earliest", "--to-latest"], "source": "C.java:3"},
                           {"kind": "checkInvalidArgs", "args": ["--to-latest", "minus(allOpts, x)"], "source": "C.java:4"}]}
    data = dict(c.data)
    k = dict(data["kafka_cli"])
    k["entries"] = list(k["entries"]) + [ent]
    k["bin_tools"] = list(k["bin_tools"]) + ["kafka-consumer-groups.sh"]
    data["kafka_cli"] = k
    return Catalogues(data)


def thing(**spec):
    return tcv.doc(**spec)


def yml(d):
    import yaml
    return "```yaml\n%s\n```" % yaml.safe_dump(d, sort_keys=False)


class YamlChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = small_catalogues()

    def run1(self, check, text):
        return sc.run_checks({"id": "x", "checks": [check]}, text, self.c)[0]

    def test_documents_are_read_from_code_blocks_or_the_whole_answer(self):
        self.assertEqual(len(sc.answer_yaml_docs(yml(thing()))), 1)
        self.assertEqual(len(sc.answer_yaml_docs("apiVersion: v1\nkind: X\nmetadata: {name: a}\n")), 1)
        self.assertEqual(sc.answer_yaml_docs("no yaml here"), [])
        two = "```yaml\napiVersion: v1\nkind: A\n---\napiVersion: v1\nkind: B\n```"
        self.assertEqual(len(sc.answer_yaml_docs(two)), 2)

    def test_yaml_document_is_the_format_check(self):
        item = {"id": "i", "checks": [{"type": "yaml_document", "format": True}]}
        self.assertTrue(sc.score_item(item, yml(thing()), self.c)["format_ok"])
        self.assertFalse(sc.score_item(item, "Use a manifest.", self.c)["format_ok"])

    def test_exactly_one_document_of_the_kind_and_api_version(self):
        chk = {"type": "yaml_one_doc", "kind": "Thing", "api_version": "kafka.example.io/v1"}
        self.assertTrue(self.run1(chk, yml(thing()))["pass"])
        d2 = dict(thing(), apiVersion="kafka.example.io/v2")
        self.assertFalse(self.run1(chk, yml(d2))["pass"])
        self.assertFalse(self.run1(chk, yml(dict(thing(), kind="Other")))["pass"])
        two = "```yaml\n%s\n---\n%s\n```" % (__import__("yaml").safe_dump(thing()), __import__("yaml").safe_dump(thing()))
        r = self.run1(chk, two)
        self.assertFalse(r["pass"])
        self.assertIn("2 documents", r["detail"])
        self.assertFalse(self.run1(chk, "nothing")["pass"])

    def test_strict_validation_fails_an_unknown_field_and_passes_a_free_form_one(self):
        chk = {"type": "yaml_strict", "kind": "Thing"}
        self.assertTrue(self.run1(chk, yml(thing(mode="a")))["pass"])
        r = self.run1(chk, yml(thing(nmae="x")))
        self.assertFalse(r["pass"])
        self.assertIn("unknown_field spec.nmae", r["detail"])
        self.assertTrue(self.run1(chk, yml(thing(config={"anything": {"goes": 1}})))["pass"])
        self.assertFalse(self.run1(chk, yml(thing(mode="c")))["pass"])                       # outside the enumeration
        self.assertFalse(self.run1(chk, yml(dict(thing(), apiVersion="kafka.example.io/v1beta2")))["pass"])
        self.assertFalse(self.run1({"type": "yaml_strict", "kind": "Nope"}, yml(thing()))["pass"])  # no document of that kind

    def test_a_kind_no_schema_covers_is_not_a_pass(self):
        r = self.run1({"type": "yaml_strict", "kind": "Pod"}, yml({"apiVersion": "v1", "kind": "Pod", "spec": {}}))
        self.assertFalse(r["pass"])
        self.assertIn("no complete schema", r["detail"])

    def test_fields_equal_with_types_loose_values_and_sets(self):
        d = thing(size=3, tags=["y", "x"], config={"retention.ms": 5, "flag": True})
        t = yml(d)
        eq = lambda fields: self.run1({"type": "yaml_fields_equal", "kind": "Thing", "fields": fields}, t)
        self.assertTrue(eq([{"path": ["spec", "size"], "value": 3}])["pass"])
        self.assertFalse(eq([{"path": ["spec", "size"], "value": "3"}])["pass"])           # a string is not the number
        self.assertTrue(eq([{"path": ["spec", "config", "retention.ms"], "value": "5", "loose": True}])["pass"])
        self.assertTrue(eq([{"path": ["spec", "config", "flag"], "value": "true", "loose": True}])["pass"])
        self.assertFalse(eq([{"path": ["spec", "config", "flag"], "value": "true"}])["pass"])
        self.assertTrue(eq([{"path": ["spec", "tags"], "value": ["x", "y"], "as_set": True}])["pass"])
        self.assertFalse(eq([{"path": ["spec", "tags"], "value": ["x", "y"]}])["pass"])   # order counts without as_set
        self.assertTrue(eq([{"path": ["spec", "tags", 0], "value": "y"}])["pass"])
        r = eq([{"path": ["spec", "missing"], "value": 1}])
        self.assertFalse(r["pass"])
        self.assertIn("missing", r["detail"])
        self.assertFalse(eq([{"path": ["spec", "size"], "value": True}])["pass"])          # 1 is not True, True is not 3

    def test_bool_is_not_an_int(self):
        self.assertFalse(sc.same_value(True, 1))
        self.assertFalse(sc.same_value(1, True))
        self.assertTrue(sc.same_value(2, 2.0))

    def test_fields_absent(self):
        t = yml(thing(mode="a"))
        chk = {"type": "yaml_fields_absent", "kind": "Thing", "paths": [["spec", "size"], ["spec", "config", "x.y"]]}
        self.assertTrue(self.run1(chk, t)["pass"])
        r = self.run1(chk, yml(thing(size=1)))
        self.assertFalse(r["pass"])
        self.assertIn("spec.size", r["detail"])

    def test_config_keys_must_exist_for_the_scope(self):
        c = Catalogues({"kafka_config": {"complete": True, "entries": [
            {"scope": "topic", "key": "retention.ms", "type": "long"}, {"scope": "broker", "key": "log.retention.ms", "type": "long"}]}})
        chk = {"type": "yaml_config_keys", "kind": "Thing", "path": ["spec", "config"], "scope": "topic"}
        run = lambda cfg: sc.run_checks({"id": "x", "checks": [chk]}, yml(thing(config=cfg)), c)[0]
        self.assertTrue(run({"retention.ms": 1})["pass"])
        r = run({"retention.ms": 1, "log.retention.ms": 2})
        self.assertFalse(r["pass"])
        self.assertIn("log.retention.ms", r["detail"])
        self.assertFalse(run({"nonsense.key": 1})["pass"])
        self.assertFalse(sc.run_checks({"id": "x", "checks": [dict(chk, path=["spec", "nothing"])]}, yml(thing()), c)[0]["pass"])

    def test_old_value_forbidden(self):
        chk = {"type": "old_value_forbidden", "forbidden": ["5", "0"], "norm": "ci"}
        self.assertFalse(self.run1(chk, "ANSWER: 5")["pass"])
        self.assertFalse(self.run1(chk, "ANSWER: `0`.")["pass"])
        self.assertTrue(self.run1(chk, "ANSWER: -1")["pass"])
        self.assertFalse(self.run1(chk, "no answer line")["pass"])


class CommandChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = small_catalogues()

    def run1(self, check, text):
        return sc.run_checks({"id": "x", "checks": [check]}, text, self.c)[0]

    TOOL = "kafka-consumer-groups.sh"

    def test_command_lines_are_found_in_blocks_prompts_and_inline_code(self):
        t = ("Run:\n```bash\nbin/kafka-consumer-groups.sh --bootstrap-server b:9092 \\\n  --describe --group g\n```\n"
             "or `$ kafka-topics --describe`\n$ kafka-consumer-groups.sh --list | head\nA sentence that mentions kafka-topics.sh --list is not a command line.")
        lines = sc.kafka_lines(t)
        tools = [sc.kafka_tool_name(l[0]) for l in lines]
        self.assertEqual(tools.count(self.TOOL), 2)
        self.assertIn("kafka-topics.sh", tools)
        self.assertEqual(sc.kafka_lines("kafka is a log"), [])
        self.assertEqual(sc.kafka_lines("KAFKA_HEAP_OPTS=-Xmx1g bin/kafka-topics.sh --list")[0][0], "bin/kafka-topics.sh")

    def test_flags_and_values(self):
        toks = sc.tokenize("bin/kafka-consumer-groups.sh --group g --shift-by -100 --to-datetime=2026-01-01T00:00:00.000 --dry-run")
        self.assertEqual(sc.kafka_flags(toks), [("--group", "g"), ("--shift-by", "-100"),
                                                ("--to-datetime", "2026-01-01T00:00:00.000"), ("--dry-run", None)])

    def test_has_kafka_command_is_the_format_check(self):
        item = {"id": "i", "checks": [{"type": "has_kafka_command", "format": True}]}
        self.assertTrue(sc.score_item(item, "```\nbin/kafka-topics.sh --list\n```", self.c)["format_ok"])
        self.assertFalse(sc.score_item(item, "run the topics tool", self.c)["format_ok"])

    def spec(self, text, req, forb=()):
        return self.run1({"type": "kcmd_spec", "tool": self.TOOL, "required_flags": req, "forbidden_flags": list(forb)}, text)

    def test_spec_requires_flags_values_and_forbids_others(self):
        req = {"--bootstrap-server": "b:9092", "--reset-offsets": None, "--group": "g", "--dry-run": None}
        ok = "```bash\nbin/kafka-consumer-groups.sh --bootstrap-server b:9092 --reset-offsets --group g --to-earliest --dry-run\n```"
        self.assertTrue(self.spec(ok, req, ["--execute"])["pass"])
        self.assertTrue(self.spec(ok.replace("--group g", "--group=g"), req)["pass"])      # --flag=value
        r = self.spec(ok.replace("--dry-run", "--execute"), req, ["--execute"])
        self.assertFalse(r["pass"])
        self.assertIn("--dry-run missing", r["detail"])
        self.assertIn("--execute is forbidden", r["detail"])
        self.assertFalse(self.spec(ok.replace("--group g", "--group h"), req)["pass"])      # wrong value
        self.assertFalse(self.spec(ok.replace("kafka-consumer-groups", "kafka-topics"), req)["pass"])   # wrong tool
        self.assertFalse(self.spec("no command", req)["pass"])

    def test_known_flags_unknown_is_a_failure_and_names_the_flag(self):
        chk = {"type": "kcmd_known_flags", "tool": self.TOOL}
        self.assertTrue(self.run1(chk, "bin/kafka-consumer-groups.sh --describe --group g")["pass"])
        r = self.run1(chk, "bin/kafka-consumer-groups.sh --describe --group g --made-up --unparsed")
        self.assertFalse(r["pass"])
        self.assertIn("--made-up", r["detail"])
        self.assertFalse(self.run1(chk, "bin/kafka-topics.sh --list")["pass"])           # no line for the tool

    def test_a_tool_without_a_full_option_list_gives_no_verdict_on_a_flag(self):
        chk = {"type": "kcmd_known_flags", "tool": "kafka-cluster.sh"}                  # complete: False in the fixture
        self.assertTrue(self.run1(chk, "bin/kafka-cluster.sh --whatever x")["pass"])

    def test_constraints_available_if_and_cannot_go_with(self):
        chk = {"type": "kcmd_constraints", "tool": self.TOOL}
        ok = lambda flags: self.run1(chk, "bin/kafka-consumer-groups.sh " + flags)["pass"]
        self.assertTrue(ok("--describe --group g --members --state"))
        r = self.run1(chk, "bin/kafka-consumer-groups.sh --members --group g")
        self.assertFalse(r["pass"])
        self.assertIn("--members needs one of --describe", r["detail"])
        self.assertTrue(ok("--list --state Stable"))                                      # --state goes with --list too
        self.assertFalse(ok("--to-earliest --to-latest"))
        self.assertTrue(ok("--to-latest"))                                                # the expression rule is skipped
        self.assertEqual(sc.constraint_problems(self.TOOL, ["--describe"], self.c), [])

    def test_identifier_precision_a_made_up_flag_of_a_complete_tool_is_unknown(self):
        r = sc.classify_identifiers("```\nbin/kafka-consumer-groups.sh --describe --group g --made-up\n```", self.c)
        self.assertEqual([x["token"] for x in r["unknown"]], ["kafka-consumer-groups.sh --made-up"])
        self.assertEqual(r["not_covered"], [])
        self.assertIn("kafka-consumer-groups.sh --describe", {x["token"] for x in r["known"]})
        # a made-up tool written as a script is unknown too; a repository name is not a tool
        r = sc.classify_identifiers("Run kafka-clusters.sh --list. The repo is kafka-config.git.", self.c)
        self.assertEqual([x["token"] for x in r["unknown"] if x["type"] == "kafka_cli_tool"], ["kafka-clusters.sh"])


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class CompletedCatalogueTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.c = Catalogues.load()

    def test_made_up_kafka_flags_are_unknown_not_covered_flags_stay_not_covered(self):
        r = sc.classify_identifiers("kafka-topics.sh --describe --unparsed --topic t", self.c)
        self.assertEqual([x["token"] for x in r["unknown"]], ["kafka-topics.sh --unparsed"])
        self.assertEqual(r["not_covered"], [])
        r = sc.classify_identifiers("kafka-server-start.sh --nope config/server.properties", self.c)   # no option parser
        self.assertEqual(r["unknown"], [])
        self.assertEqual([x["token"] for x in r["not_covered"]], ["kafka-server-start.sh --nope"])

    def test_manifest_fields_of_kubernetes_and_argocd_kinds_are_classified(self):
        t = ("```yaml\napiVersion: apps/v1\nkind: Deployment\nspec:\n  replicas: 2\n  replica: 3\n```\n"
             "```yaml\napiVersion: argoproj.io/v1alpha1\nkind: Application\nspec:\n  project: p\n  projekt: q\n```")
        r = sc.classify_identifiers(t, self.c)
        self.assertEqual({x["token"] for x in r["unknown"]}, {"Deployment spec.replica", "Application spec.projekt"})
        self.assertIn("Deployment spec.replicas", {x["token"] for x in r["known"]})

    def test_config_keys_in_a_topic_manifest_are_looked_up_in_the_topic_scope(self):
        t = ("```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nspec:\n  config:\n"
             "    retention.ms: 1\n    log.retention.ms: 2\n```")
        r = sc.classify_identifiers(t, self.c)
        self.assertEqual([x["token"] for x in r["unknown"]], ["log.retention.ms"])
        self.assertIn("retention.ms", {x["token"] for x in r["known"]})

    def test_a_real_resource_command_and_trap_item_end_to_end(self):
        topic = {"apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaTopic", "metadata": {"name": "t"},
                 "spec": {"partitions": 3, "replicas": 2, "config": {"retention.ms": 86400000}}}
        checks = [{"type": "yaml_document", "format": True},
                  {"type": "yaml_one_doc", "kind": "KafkaTopic", "api_version": "kafka.strimzi.io/v1"},
                  {"type": "yaml_strict", "kind": "KafkaTopic"},
                  {"type": "yaml_fields_equal", "kind": "KafkaTopic", "fields": [
                      {"path": ["spec", "partitions"], "value": 3},
                      {"path": ["spec", "config", "retention.ms"], "value": "86400000", "loose": True}]},
                  {"type": "yaml_fields_absent", "kind": "KafkaTopic", "paths": [["spec", "topicName"]]},
                  {"type": "yaml_config_keys", "kind": "KafkaTopic", "path": ["spec", "config"], "scope": "topic"}]
        item = {"id": "x", "checks": checks}
        good = sc.score_item(item, yml(topic), self.c)
        self.assertTrue(good["all_pass"], good["results"])
        bad = dict(topic, spec=dict(topic["spec"], topicName="t", config={"log.retention.ms": 1}))
        res = sc.run_checks(item, yml(bad), self.c)
        self.assertEqual([r["pass"] for r in res], [True, True, True, False, False, False])
        wrong_api = dict(topic, apiVersion="kafka.strimzi.io/v1beta2")
        self.assertFalse(sc.run_checks(item, yml(wrong_api), self.c)[1]["pass"])
        self.assertFalse(sc.run_checks(item, yml(wrong_api), self.c)[2]["pass"])
        cmd = [{"type": "has_kafka_command", "format": True},
               {"type": "kcmd_spec", "tool": "kafka-leader-election.sh",
                "required_flags": {"--bootstrap-server": "b:9092", "--election-type": "preferred", "--topic": "t", "--partition": "3"},
                "forbidden_flags": ["--all-topic-partitions"]},
               {"type": "kcmd_known_flags", "tool": "kafka-leader-election.sh"},
               {"type": "kcmd_constraints", "tool": "kafka-leader-election.sh"}]
        line = "```bash\nbin/kafka-leader-election.sh --bootstrap-server b:9092 --election-type preferred --topic t --partition 3\n```"
        self.assertTrue(sc.score_item({"id": "y", "checks": cmd}, line, self.c)["all_pass"])
        bad = line.replace("--partition 3", "--partition 3 --all-topic-partitions --describe")
        res = sc.run_checks({"id": "y", "checks": cmd}, bad, self.c)
        self.assertEqual([r["pass"] for r in res], [True, False, False, True])     # forbidden flag; --describe does not exist
        # the rule `--admin.config` against `--command-config` of the same tool
        both = "bin/kafka-leader-election.sh --admin.config a --command-config b"
        self.assertFalse(sc.run_checks({"id": "z", "checks": [cmd[3]]}, both, self.c)[0]["pass"])


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


class TestDeclineOutcome(unittest.TestCase):
    """Decision J5: covering chunks, the decline detector and the four outcomes, on small synthetic items and a
    synthetic index (no evaluation item, no real index)."""

    @classmethod
    def setUpClass(cls):
        rows = [
            {"id": "k1", "source": "s", "path": "raw/ref/cfg.html", "title": "zz.alpha.ms", "text": "x\n\n## zz.alpha.ms\nThe alpha wait."},
            {"id": "k2", "source": "s", "path": "raw/ref/cfg.html", "title": "zz.beta.ms", "text": "## zz.beta.ms\nThe beta wait. See zz.alpha.ms."},
            {"id": "k3", "source": "s", "path": "raw/other.md", "title": "", "text": "Use qq-tool.sh with --frob to frob."},
            {"id": "k4", "source": "s", "path": "raw/other.md", "title": "", "text": "The qq-tool.sh lists things."},
            {"id": "k5", "source": "s", "path": "raw/ref/rest.yaml", "title": "POST /widgets", "text": "Rest API: POST /widgets\nmake one"},
        ]
        cls.idx = sc.ChunkIndex(rows)

    def cfg_item(self, key="zz.alpha.ms", others=("zz.beta.ms",)):
        srcs = [{"catalogue": "kafka_config", "entry": "broker/" + k, "file": "raw/ref/cfg.html", "fact": "f"}
                for k in (key,) + tuple(others)]
        return {"id": "t-1", "kind": "name_setting", "question": "Which key waits?", "sources": srcs,
                "checks": [{"type": "answer_line", "format": True},
                           {"type": "answer_equals", "accepted": [key], "norm": "exact"}]}

    def cmd_item(self):
        return {"id": "t-2", "kind": "write_command", "question": "Write the qq-tool.sh command to frob.",
                "sources": [{"catalogue": "kafka_cli", "entry": "qq-tool.sh --frob", "file": "raw_catalogue/x.java", "fact": "f"},
                            {"catalogue": "kafka_cli", "entry": "qq-tool.sh --zap", "file": "raw_catalogue/x.java", "fact": "f"},
                            {"catalogue": "kafka_cli", "entry": "qq-tool.sh", "file": "qq-tool.sh", "fact": "f"}],
                "checks": [{"type": "has_kafka_command", "format": True},
                           {"type": "kcmd_spec", "tool": "qq-tool.sh", "required_flags": {"--frob": None, "--zap": None}}]}

    def row(self, item, text, passed=False, detail="answer 'x', accepted ['zz.alpha.ms']"):
        return {"id": item["id"], "all_pass": passed, "results": [] if passed else [{"pass": False, "detail": detail, "type": "t"}]}

    def classify(self, item, text, pack, passed=False, detail="answer 'x', accepted ['zz.alpha.ms']"):
        cov, cache = sc.item_coverage(item, self.idx)
        return sc.classify_answer(item, self.row(item, text, passed, detail), text, pack, cache)[0], cov

    def test_covering_chunks_from_sources(self):
        cov, _ = sc.item_coverage(self.cfg_item(), self.idx)
        u = {x["label"]: x for x in cov["units"]}
        self.assertEqual(cov["covering_chunks"], ["k1", "k2"])   # own section plus the chunk that names the key
        self.assertEqual(u["kafka_config:broker/zz.alpha.ms"]["via"], "strict")
        self.assertFalse(u["kafka_config:broker/zz.beta.ms"]["decisive"])  # an option beside the answer
        self.assertFalse(cov["not_in_index"])

    def test_not_in_index_and_given_by_question(self):
        cov, _ = sc.item_coverage(self.cmd_item(), self.idx)
        by = {x["label"]: x for x in cov["units"]}
        self.assertEqual(by["kafka_cli:qq-tool.sh --frob"]["chunks"], 1)
        self.assertEqual(by["kafka_cli:qq-tool.sh --zap"]["via"], "none")
        self.assertFalse(by["kafka_cli:qq-tool.sh"]["decisive"])    # the tool is named in the question
        self.assertTrue(cov["not_in_index"])
        none = self.cfg_item(key="zz.missing.ms", others=())
        self.assertTrue(sc.item_coverage(none, self.idx)[0]["not_in_index"])

    def test_outcomes(self):
        it = self.cfg_item()
        gap = "The retrieved documentation does not cover the key.\n\nANSWER: The excerpts do not cover this."
        self.assertEqual(self.classify(it, "ANSWER: zz.alpha.ms", [], passed=True)[0], "pass")
        self.assertEqual(self.classify(it, "ANSWER: zz.beta.ms", ["k1"])[0], "wrong")
        self.assertEqual(self.classify(it, gap, ["k1"])[0], "wrong_decline")
        self.assertEqual(self.classify(it, gap, ["k2"])[0], "wrong_decline")   # names the key
        self.assertEqual(self.classify(it, gap, ["k3"])[0], "correct_decline")
        none = self.cfg_item(key="zz.missing.ms", others=())
        self.assertEqual(self.classify(none, gap, ["k1"], detail="accepted ['zz.missing.ms']")[0], "correct_decline")

    def test_is_real_abstained(self):
        it = {"id": "t-3", "kind": "is_real", "question": "Is zz.nope.ms a key?", "sources": [
            {"catalogue": "kafka_config", "absent": "zz.nope.ms", "file": "raw/ref/cfg.html", "fact": "f"}],
            "checks": [{"type": "answer_line", "format": True}, {"type": "answer_verdict", "expected": "does not exist"}]}
        self.assertEqual(self.classify(it, "ANSWER: not in my sources", [])[0], "abstained")
        self.assertEqual(self.classify(it, "ANSWER: exists", [])[0], "wrong")
        it2 = dict(it, sources=[{"catalogue": "connect_rest", "entry": "POST /widgets", "file": "raw/ref/rest.yaml", "fact": "f"}],
                   checks=[{"type": "answer_line", "format": True}, {"type": "answer_verdict", "expected": "exists"}])
        self.assertEqual(self.classify(it2, "ANSWER: not in my sources", ["k5"])[0], "wrong_decline")
        self.assertEqual(self.classify(it2, "ANSWER: not in my sources", ["k1"])[0], "correct_decline")

    def test_partial_command_gap_on_other_element(self):
        it = self.cmd_item()
        d = "closest line 'qq-tool.sh': --zap missing"
        cmd = "```\nqq-tool.sh --frob\n```"
        # the gap names --frob but the failed element is --zap: not a decline of that element
        o, _ = self.classify(it, "The documentation excerpts do not cover --frob.\n" + cmd, [], detail=d)
        self.assertEqual(o, "wrong")
        # the gap names the failed element, which is in no chunk: correct decline
        o, _ = self.classify(it, "The documentation excerpts do not cover --zap.\n" + cmd, [], detail=d)
        self.assertEqual(o, "correct_decline")
        # a gap that names no element: undecided, counted as declined
        o, _ = self.classify(it, "The excerpts do not cover everything asked.\n" + cmd, [], detail=d)
        self.assertEqual(o, "correct_decline")

    def test_failed_element_named_in_question_is_a_wrong_decline(self):
        it = {"id": "t-4", "kind": "write_resource", "question": "Give a Widget with `sizeLimit: 3`.",
              "sources": [{"catalogue": "kubernetes", "entry": "Widget spec.sizeLimit", "file": "raw_catalogue/s.json", "fact": "f"}],
              "checks": [{"type": "yaml_document", "format": True}]}
        o, _ = self.classify(it, "The documentation does not cover sizeLimit.\n```yaml\nkind: Widget\n```", [],
                             detail="spec.sizeLimit missing")
        self.assertEqual(o, "wrong_decline")

    def test_detector(self):
        for t in ("The retrieved documentation does not cover the key.", "The excerpts do not contain this value.",
                  "ANSWER: not in my sources", "No information about it in the provided excerpts.",
                  "x\n\nANSWER: null", "The value is not mentioned in the provided documentation.",
                  "I cannot determine this from the excerpts."):
            self.assertTrue(sc.detect_decline(t)[0], t)
        for t in ("ANSWER: zz.alpha.ms", "Set `retention.ms` to 1000.", "```bash\nkafka-topics.sh --list\n```",
                  "The excerpt does not list it for producers, so it is a broker key.\n\nANSWER: broker"):
            self.assertFalse(sc.detect_decline(t)[0], t)

    def test_all_pass_unchanged_and_summary_counts(self):
        it = self.cfg_item()
        answers = [{"id": "t-1", "text": "ANSWER: zz.beta.ms", "pack": ["k1"]}]
        rows, _, _ = sc.score_files([it], answers, Catalogues.load(fixture_catalogues()))
        before = json.dumps(rows, sort_keys=True)
        sc.add_outcomes(rows, [it], answers, self.idx)
        self.assertEqual(rows[0]["outcome"], "wrong")
        self.assertFalse(rows[0]["all_pass"])
        stripped = [{k: v for k, v in r.items() if k not in ("outcome", "declined", "coverage")} for r in rows]
        self.assertEqual(json.dumps(stripped, sort_keys=True), before)
        c = sc.outcome_counts(rows, [it])
        self.assertEqual((c["total"]["n"], c["total"]["wrong"], c["name_setting"]["pass"]), (1, 1, 0))
        self.assertIn("name_setting", "\n".join(sc.format_outcomes(c)))

    def test_index_hash_is_checked(self):
        d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, True)
        idxf, fz = d / "i.jsonl", d / "f.json"
        idxf.write_text(json.dumps({"id": "a", "path": "p", "title": "", "text": "t"}) + "\n")
        import hashlib
        fz.write_text(json.dumps({"sha256": hashlib.sha256(idxf.read_bytes()).hexdigest()}))
        self.assertEqual(len(sc.ChunkIndex.load(idxf, fz).chunks), 1)
        fz.write_text(json.dumps({"sha256": "0" * 64}))
        with self.assertRaises(sc.IndexMismatch):
            sc.ChunkIndex.load(idxf, fz)


if __name__ == "__main__":
    unittest.main()
