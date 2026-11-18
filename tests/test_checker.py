"""Offline tests of kafka_sre_rag.checker and scripts/check_answers.py on the small fixture catalogues."""
import contextlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path

from kafka_sre_rag import checker
from kafka_sre_rag.catalogue import Catalogues
from kafka_sre_rag.checker import INVENTED, UNVERIFIED, WRONG_USE, IN_EXCERPTS, check_answer, render_note

HERE = os.path.dirname(__file__)
ROOT = Path(HERE).parent
sys.path.insert(0, str(ROOT / "scripts"))


def fixture_catalogues():
    data = {}
    for d in ("catalogues", "catalogues_ext"):
        base = os.path.join(HERE, "fixtures", d)
        for f in os.listdir(base):
            if f.endswith(".json") and f != "CATALOGUES.json":
                data[f[:-5]] = json.load(open(os.path.join(base, f)))
    return Catalogues(data)


CATS = fixture_catalogues()


def run(text, excerpts=None):
    return check_answer(text, excerpts, CATS)


def levels(res):
    return sorted((f.level, f.item.text) for f in res.findings)


def fence(lang, body):
    return "```%s\n%s\n```" % (lang, body)


class FieldPathTests(unittest.TestCase):
    KU = fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nmetadata:\n  name: u\nspec:\n  quotas:\n"
               "    consumer:\n      - topic: t\n        rate: 1\n    producerByteRate: 5\n  authentication:\n    type: tls")

    def test_invented_field_reported_once_with_nearest_real(self):
        res = run("Set this:\n" + self.KU)
        self.assertEqual(levels(res), [(INVENTED, "spec.quotas.consumer")])
        f = res.findings[0]
        self.assertIn("spec.quotas.consumerByteRate", f.evidence)
        self.assertEqual(f.item.line, 9)

    def test_real_fields_are_known(self):
        res = run(fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nspec:\n  quotas:\n    producerByteRate: 1"))
        self.assertEqual(res.findings, [])
        self.assertIn("spec.quotas.producerByteRate", [i.text for i in res.items if i.status == "known"])

    def test_excerpts_shield_the_item(self):
        res = run("x\n" + self.KU, excerpts=["quotas:\n  consumer:\n    - topic: a"])
        self.assertEqual(res.by_level(INVENTED), [])
        self.assertTrue(any(i.status == IN_EXCERPTS and i.text == "spec.quotas.consumer" for i in res.items))

    def test_old_api_version_without_a_near_name_is_unverified_not_invented(self):
        y = fence("yaml", "apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nspec:\n  kafka:\n    storage:\n      type: ephemeral")
        res = run(y)
        self.assertEqual(levels(res), [(UNVERIFIED, "spec.kafka.storage")])

    def test_inline_path_with_kind_in_sentence(self):
        res = run("In the `KafkaUser` resource set `spec.quotas.consumer` to a rate.")
        self.assertEqual(levels(res), [(INVENTED, "spec.quotas.consumer")])

    def test_inline_path_without_kind_is_not_judged(self):
        self.assertEqual(run("Look at `spec.bogus.field` for details.").findings, [])

    def test_kind_dot_path_in_prose(self):
        res = run("Configure it in KafkaUser.spec.authentication.truststore or similar.")
        self.assertEqual([f.level for f in res.findings], [INVENTED])

    def test_unknown_kind_in_yaml_is_unchecked_and_silent(self):
        res = run(fence("yaml", "apiVersion: v1\nkind: ConfigMap\nspec:\n  foo: 1"))
        self.assertEqual(res.findings, [])
        self.assertEqual(res.unchecked_kinds, {"ConfigMap": 1})


class ConfigTests(unittest.TestCase):
    def test_keys_inside_spec_kafka_config_use_the_broker_scope(self):
        y = fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nspec:\n  kafka:\n    config:\n"
                  "      min.insync.replicas: 2\n      max.poll.interval.ms: 5")
        res = run(y)
        self.assertEqual(levels(res), [(WRONG_USE, "max.poll.interval.ms")])
        self.assertIn("consumer", res.findings[0].evidence)

    def test_scope_from_properties_file_named_before_the_block(self):
        text = "Put this in consumer.properties:\n" + fence("properties", "rebalance.timeout.ms=60000")
        self.assertEqual(levels(run(text)), [(WRONG_USE, "rebalance.timeout.ms")])

    def test_no_scope_no_scope_finding(self):
        self.assertEqual(run("Use `rebalance.timeout.ms` for the group.").findings, [])
        self.assertEqual(run("A consumer uses `rebalance.timeout.ms` here.").findings, [])

    def test_sentence_words_do_not_set_a_scope(self):
        self.assertEqual(run("The producer fails when `min.insync.replicas` is too high.").findings, [])

    def test_heading_scope_only_for_key_value_settings(self):
        text = "### Consumer configuration\n\n`rebalance.timeout.ms=1000`\n\nMention `rebalance.timeout.ms` again."
        res = run(text)
        self.assertEqual(levels(res), [(WRONG_USE, "rebalance.timeout.ms")])
        self.assertEqual(res.findings[0].item.line, 3)

    def test_unknown_key_near_a_real_one_is_invented(self):
        res = run("Set `log.retention.hours` on the broker.")
        self.assertEqual(levels(res), [(INVENTED, "log.retention.hours")])
        self.assertIn("log.retention.ms", res.findings[0].evidence)

    def test_unknown_key_far_from_any_real_key_is_unverified(self):
        res = run("Set `log.compaction.schemas` and `max.zzz.qqq`.")
        self.assertEqual(sorted(f.level for f in res.findings), [UNVERIFIED, UNVERIFIED])   # Kafka-like first word, no near key
        self.assertEqual(res.by_level(INVENTED), [])

    def test_abbreviation_of_a_real_key_is_not_a_finding(self):
        self.assertEqual(run("For example, RF=3, min.insync=2.").findings, [])

    def test_foreign_prefix_is_unverified_and_grouped(self):
        text = fence("properties", "spring.kafka.consumer.group-id=a\nspring.kafka.consumer.max-poll-interval-ms=1")
        res = run(text)
        self.assertEqual(sorted(f.level for f in res.findings), [UNVERIFIED, UNVERIFIED])
        self.assertIn("`spring.kafka.consumer.*` (2 keys)", render_note(res))

    def test_value_outside_valid_values_in_properties_and_prose_alternatives(self):
        self.assertEqual(levels(run(fence("properties", "errors.tolerance=class"))), [(WRONG_USE, "errors.tolerance=class")])
        res = run("Change `errors.tolerance` to `all` or `class` for a DLQ, or `errors.tolerance=none` (or `bogus`).")
        self.assertEqual(levels(res), [(WRONG_USE, "errors.tolerance=bogus"), (WRONG_USE, "errors.tolerance=class")])

    def test_connector_config_in_yaml_unknown_key_is_not_covered(self):
        y = fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nspec:\n  config:\n    errors.tolerance: all")
        self.assertEqual(run(y).by_level(INVENTED), [])

    def test_add_config_uses_entity_type(self):
        sh = fence("bash", "kafka-configs.sh --bootstrap-server b:9092 --alter --entity-type topics --entity-name t \\\n"
                   "  --add-config min.insync.replicas=2,errors.tolerance=none")
        res = run(sh)
        scopes = {i.text: i.context.get("scope") for i in res.items if i.type == "config_key"}
        self.assertEqual(scopes["min.insync.replicas"], "topic")

    def test_file_names_domains_versions_are_not_keys(self):
        text = "See server.properties, my-cluster.kafka.svc, 4.3.2 and kafka.example.com."
        self.assertEqual([i for i in run(text).items if i.type == "config_key"], [])


class CliTests(unittest.TestCase):
    def test_complete_tool_unknown_flag_is_invented(self):
        res = run(fence("bash", "bin/kafka-streams-application-reset.sh --application-id a --execute"))
        self.assertEqual(levels(res), [(INVENTED, "--execute")])

    def test_incomplete_tool_unknown_flag_is_unverified(self):
        res = run(fence("bash", "bin/kafka-topics.sh --bootstrap-server b:9092 --describe --unparsed"))
        self.assertEqual(levels(res), [(UNVERIFIED, "--unparsed")])
        self.assertIn("kafka-topics.sh", res.findings[0].evidence)

    def test_flag_named_for_one_tool_but_seen_for_another_in_the_excerpts(self):
        text = fence("bash", "kafka-streams-application-reset.sh --application-id a --execute")
        other = "kafka-groups.sh --execute and nothing else"
        self.assertEqual(levels(run(text, [other, "kafka-streams-application-reset.sh --application-id"])),
                         [(INVENTED, "--execute")])
        self.assertEqual(run(text, [other]).by_level(INVENTED), [])      # no excerpt names the tool: lenient

    def test_inline_flag_uses_the_tool_named_before_it(self):
        res = run("Use `kafka-topics.sh` with `--unparsed` to see it.")
        self.assertEqual(levels(res), [(UNVERIFIED, "--unparsed")])

    def test_flag_without_a_tool_is_not_judged(self):
        self.assertEqual(run("Pass `--unparsed` to it.").findings, [])

    def test_argocd_commands_and_flags(self):
        res = run(fence("bash", "argocd app sync my-app --prune\nargocd app sync my-app --bogus"))
        self.assertEqual(levels(res), [(INVENTED, "--bogus")])

    def test_kubectl_selector_keys_and_values(self):
        res = run(fence("bash", "kubectl logs -n strimzi -l strimzi.io/kind=kafka --tail=10"))
        self.assertIn((WRONG_USE, "strimzi.io/kind=kafka"), levels(res))

    def test_pipe_and_exec_commands(self):
        res = run(fence("bash", "kubectl exec -it pod-0 -- bin/kafka-topics.sh --bootstrap-server b:9092 --unparsed | sort"))
        self.assertIn((UNVERIFIED, "--unparsed"), levels(res))


class HttpTests(unittest.TestCase):
    def test_wrong_method_is_wrong_use_with_the_allowed_one(self):
        res = run(fence("", "PUT /connectors/{name}/restart"))
        self.assertEqual(levels(res), [(WRONG_USE, "PUT /connectors/{name}/restart")])
        self.assertIn("POST", res.findings[0].evidence)

    def test_right_method_and_curl_forms(self):
        self.assertEqual(run("`POST /connectors/{name}/restart`").findings, [])
        res = run(fence("bash", "curl -X PUT http://connect:8083/connectors/a/restart"))
        self.assertEqual(levels(res), [(WRONG_USE, "PUT http://connect:8083/connectors/a/restart")])

    def test_unknown_connect_path_is_invented_other_rest_is_not_judged(self):
        self.assertEqual(levels(run("`GET /connectors/{name}/nothing`")), [(INVENTED, "GET /connectors/{name}/nothing")])
        self.assertEqual(run("`GET /subjects/x/versions`").findings, [])
        self.assertEqual(run("curl http://pod:9404/metrics").findings, [])

    def test_method_in_excerpts_is_not_flagged(self):
        self.assertEqual(run("`PUT /connectors/{name}/restart`", ["PUT /connectors/{name}/restart"]).by_level(WRONG_USE), [])


class NameTests(unittest.TestCase):
    def test_misspelt_error_name_is_invented_with_the_real_one(self):
        res = run("The producer raises NotEvenoughReplicasAfterAppend.")
        self.assertEqual(levels(res), [(INVENTED, "NotEvenoughReplicasAfterAppend")])
        self.assertIn("NOT_ENOUGH_REPLICAS_AFTER_APPEND", res.findings[0].evidence)

    def test_real_error_spellings_and_ordinary_class_names_pass(self):
        for t in ("NotEnoughReplicasAfterAppend", "NOT_ENOUGH_REPLICAS_AFTER_APPEND", "UnknownTopicOrPartitionException"):
            self.assertEqual(run("It returns `%s`." % t).findings, [], t)
        res = run("Use the NetworkPolicy and the KafkaConsumer class.")
        self.assertEqual([i for i in res.items if i.type == "error_name"], [])

    def test_upper_snake_in_an_error_sentence_must_be_a_real_error(self):
        self.assertEqual(levels(run("The broker returns error FENCED_STATE_VERSION.")), [(INVENTED, "FENCED_STATE_VERSION")])
        self.assertEqual(run("Set KAFKA_HEAP_OPTS before the start.").findings, [])

    def test_kafka_prefixed_name_is_a_type_only_where_used_as_a_kind(self):
        self.assertEqual(run("Check the KafkaNodeX settings.").findings, [])
        res = run("Check the `KafkaUserz` custom resource and the `KafkaProducer` class.")
        self.assertEqual(levels(res), [(INVENTED, "KafkaUserz")])

    def test_annotation_unknown_name_is_unverified_never_invented(self):
        res = run("Add `strimzi.io/no-such-thing: \"true\"` to the resource.")
        self.assertEqual(levels(res), [(UNVERIFIED, "strimzi.io/no-such-thing")])

    def test_annotation_value_near_miss_and_boolean(self):
        res = run(fence("bash", "kubectl annotate kafkarebalance r strimzi.io/rebalance=Approve"))
        self.assertIn((WRONG_USE, "strimzi.io/rebalance=Approve"), levels(res))
        self.assertEqual(run(fence("bash", "kubectl annotate kafkarebalance r strimzi.io/rebalance=approve")).findings, [])
        self.assertEqual(run(fence("bash", "kubectl annotate kafkarebalance r strimzi.io/rebalance=other")).by_level(WRONG_USE), [])

    def test_yaml_labels_and_annotations(self):
        y = fence("yaml", "apiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nmetadata:\n  labels:\n    strimzi.io/kind: kafka\nspec:\n  authentication:\n    type: tls")
        self.assertIn((WRONG_USE, "strimzi.io/kind=kafka"), levels(run(y)))


class NoteAndStructureTests(unittest.TestCase):
    def test_note_lists_findings_and_groups_unverified(self):
        text = ("Use `kafka-topics.sh --unparsed --also-bad --third --fourth --fifth --sixth` and `log.retention.hours`.")
        res = run(text)
        note = render_note(res, max_unverified=3)
        self.assertIn("Could not confirm: `log.retention.hours`", note)
        self.assertIn("Not checked, no reference covers them: `--unparsed`, `--also-bad`, `--third` and 3 more", note)
        self.assertEqual(note.count("\n"), 1)

    def test_nothing_to_say_gives_an_empty_note(self):
        self.assertEqual(render_note(run("Nothing to see here.")), "")

    def test_result_is_structured_and_json_serialisable(self):
        res = run("Use `kafka-topics.sh --unparsed` and `log.retention.hours`.")
        d = json.loads(json.dumps(res.to_dict()))
        self.assertEqual(d["counts"]["invented"], 1)
        f = [x for x in d["findings"] if x["level"] == INVENTED][0]
        for k in ("level", "text", "type", "line", "sentence", "rule", "evidence", "suggestion", "catalogue"):
            self.assertIn(k, f)
        for i in d["items"]:
            for k in ("text", "type", "line", "sentence", "status"):
                self.assertIn(k, i)

    def test_items_keep_line_and_sentence(self):
        res = run("First line.\nAnd `log.retention.hours` is wrong. Next sentence.")
        f = res.findings[0]
        self.assertEqual(f.item.line, 2)
        self.assertEqual(f.item.sentence, "And `log.retention.hours` is wrong.")

    def test_repeated_items_are_counted_once(self):
        res = run("`log.retention.hours` and again `log.retention.hours`.")
        self.assertEqual(len(res.findings), 1)
        self.assertEqual(res.items[0].count, 2)

    def test_invented_is_never_reported_from_an_incomplete_catalogue(self):
        data = {n: CATS.data[n] for n in CATS.data}
        cfg = json.loads(json.dumps(data["kafka_config"]))
        cfg["complete"] = False
        data["kafka_config"] = cfg
        res = check_answer("Set `log.retention.hours`.", None, Catalogues(data))
        self.assertEqual(res.by_level(INVENTED), [])
        self.assertEqual([f.level for f in res.findings], [UNVERIFIED])

    def test_markdown_fences_are_not_confused_by_nested_text(self):
        res = run("~~~yaml\nspec: {}\n~~~\nand then `log.retention.hours`.")
        self.assertEqual(levels(res), [(INVENTED, "log.retention.hours")])


class IndependenceTests(unittest.TestCase):
    def test_checker_imports_only_the_catalogue_and_reads_nothing_from_eval(self):
        pkg = ROOT / "kafka_sre_rag"
        src = (pkg / "checker.py").read_text()
        imports = re.findall(r"^\s*(?:from|import)\s+([\w.]+)", src, re.M)
        local = re.findall(r"^\s*from\s+(\.[\w.]*|kafka_sre_rag[\w.]*)\s+import", src, re.M)
        self.assertTrue(local and all(m in (".catalogue", "kafka_sre_rag.catalogue") for m in local), local)
        self.assertNotIn("scripts", " ".join(imports))
        code = re.sub(r'""".*?"""', "", src, flags=re.S)
        self.assertNotIn("eval2", code)
        self.assertNotIn("eval/", code)
        self.assertNotIn("m4/", code)
        for f in pkg.glob("*.py"):
            self.assertNotRegex(f.read_text(), r"^\s*(?:from|import)\s+scripts\b", f.name)


class ScriptTests(unittest.TestCase):
    def test_script_with_packs_index_and_report(self):
        import check_answers
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "ans.jsonl").write_text("\n".join(json.dumps(r) for r in [
                {"id": "q1", "question": "Q one", "text": "Set `log.retention.hours` please."},
                {"question": "Q two", "text": "Set `log.retention.hours` and `kafka-topics.sh --unparsed`."}]) + "\n")
            (d / "packs.jsonl").write_text("\n".join(json.dumps(r) for r in [
                {"id": "q1", "question": "Q one", "k6": ["c1"]}, {"question": "Q two", "k6": ["c2"]}]) + "\n")
            (d / "chunks.jsonl").write_text("\n".join(json.dumps(r) for r in [
                {"id": "c1", "text": "the key log.retention.hours is documented here"},
                {"id": "c2", "text": "unrelated"}]) + "\n")
            cats = d / "cats"
            cats.mkdir()
            for sub in ("catalogues", "catalogues_ext"):
                for f in os.listdir(os.path.join(HERE, "fixtures", sub)):
                    if f.endswith(".json") and f != "CATALOGUES.json":
                        (cats / f).write_text(Path(HERE, "fixtures", sub, f).read_text())
            out = d / "report.json"
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = check_answers.main(["--answers", str(d / "ans.jsonl"), "--packs", str(d / "packs.jsonl"),
                                         "--index", str(d / "chunks.jsonl"), "--out", str(out), "--note",
                                         "--catalogues", str(cats)])
            self.assertEqual(rc, 0)
            lines = buf.getvalue().splitlines()
            self.assertIn("invented 0", lines[0])       # shielded by its excerpt
            self.assertIn("invented 1", [l for l in lines if l.startswith("  2")][0])
            rep = json.loads(out.read_text())
            self.assertEqual(rep["totals"]["invented"], 1)
            self.assertEqual(len(rep["answers"]), 2)
            self.assertTrue(rep["answers"][0]["excerpts"])
            self.assertIn("Could not confirm", rep["answers"][1]["note"])
            with self.assertRaises(SystemExit):
                with contextlib.redirect_stderr(io.StringIO()):
                    check_answers.main(["--answers", str(d / "ans.jsonl"), "--out", str(out)])    # exists: refused


if __name__ == "__main__":
    unittest.main()
