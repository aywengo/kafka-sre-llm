"""Identifier check of the M4 evaluation (scripts/eval2_idcheck.py): small synthetic answers, one rule at a time,
for what is decided (exists, nonexistent) and for what must stay undecided. The real catalogues are needed
(work/m4/catalogues, built by scripts/build_catalogues.py); without them the tests are skipped.
"""
import io
import json
import sys
import tempfile
import types
import unittest
import unittest.mock
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
import eval2_idcheck as ic  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, default_dir  # noqa: E402

HAVE_REAL = (default_dir() / "CATALOGUES.json").is_file()
CTX = ic.Ctx(Catalogues.load()) if HAVE_REAL else None
FENCE = "```"


def check(text):
    return ic.check_text(text, CTX)


def statuses(text, kind=None):
    """{ident: status} of the identifiers of `kind` (all kinds when None)."""
    return {it["ident"]: it["status"] for it in check(text)["items"] if kind in (None, it["kind"])}


def block(body, lang="yaml"):
    return "%s%s\n%s\n%s\n" % (FENCE, lang, body, FENCE)


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class Scripts(unittest.TestCase):
    def test_unknown_script_is_nonexistent_known_exists(self):
        s = statuses("Run `kafka-nosuch.sh --list` or `kafka-topics.sh --list`.", "cli_tool")
        self.assertEqual(s["kafka-nosuch.sh"], "nonexistent")
        self.assertEqual(s["kafka-topics.sh"], "exists")

    def test_name_without_sh_is_not_a_script(self):
        self.assertEqual(statuses("Look at the `kafka-prod-app` deployment.", "cli_tool"), {})

    def test_script_the_answer_creates_is_undecided(self):
        s = statuses("Create a wrapper script called `kafka-health.sh` and run it.", "cli_tool")
        self.assertEqual(s["kafka-health.sh"], "undecided")
        s = statuses(block("cat > kafka-health.sh <<EOF\necho hi\nEOF", "bash"), "cli_tool")
        self.assertEqual(s["kafka-health.sh"], "undecided")


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class Flags(unittest.TestCase):
    def test_unknown_flag_on_complete_tool(self):
        s = statuses(block("kafka-topics.sh --bootstrap-server b:9092 --list --nope", "bash"), "cli_flag")
        self.assertEqual(s["kafka-topics.sh --nope"], "nonexistent")
        self.assertEqual(s["kafka-topics.sh --list"], "exists")

    def test_placeholder_in_angle_brackets_does_not_cut_the_line(self):
        s = statuses(block("kafka-transactions.sh --bootstrap-server <broker>:9092 --nope", "bash"), "cli_flag")
        self.assertEqual(s["kafka-transactions.sh --nope"], "nonexistent")

    def test_redirect_and_pipe_still_split(self):
        s = statuses(block("kafka-topics.sh --list | grep --color foo", "bash"), "cli_flag")
        self.assertNotIn("kafka-topics.sh --color", s)

    def test_kubectl_exec_wrapper(self):
        s = statuses(block("kubectl exec -n k my-pod -- bin/kafka-topics.sh --bootstrap-server b:9092 --nope", "bash"),
                     "cli_flag")
        self.assertEqual(s["kafka-topics.sh --nope"], "nonexistent")

    def test_pod_name_is_not_a_tool(self):
        self.assertEqual(statuses(block("kubectl get pod kafka-cluster-kafka-0 --nope", "bash"), "cli_flag"), {})

    def test_tool_without_option_list_is_undecided(self):
        s = check(block("kafka-run-class.sh kafka.Foo --whatever", "bash"))
        flags = [it for it in s["items"] if it["kind"] == "cli_flag"]
        self.assertEqual([it["status"] for it in flags], ["undecided"])

    def test_flag_of_unlisted_tool_is_undecided(self):
        s = check(block("kafka-nosuch.sh --x", "bash"))
        self.assertEqual([it["status"] for it in s["items"] if it["kind"] == "cli_flag"], ["undecided"])

    def test_add_config_keys_are_read(self):
        s = statuses(block("kafka-configs.sh --bootstrap-server b:9092 --alter --entity-type topics "
                           "--entity-name t --add-config min.in.sync.replicas=2,retention.ms=1000", "bash"))
        self.assertEqual(s["min.in.sync.replicas"], "nonexistent")
        self.assertEqual(s["retention.ms"], "exists")


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class ArgoCD(unittest.TestCase):
    def test_flag_of_a_leaf_command(self):
        s = statuses(block("argocd app sync my-app --prune --nonesuch", "bash"), "argocd_flag")
        self.assertEqual(s["argocd app sync --prune"], "exists")
        self.assertEqual(s["argocd app sync --nonesuch"], "nonexistent")

    def test_deep_command_is_resolved(self):
        s = statuses(block("argocd proj source-integrity git policies add --nonesuch", "bash"))
        self.assertEqual(s.get("argocd proj source-integrity git policies add"), "exists")

    def test_unknown_subcommand_is_undecided_because_of_plugins(self):
        s = check(block("argocd app frobnicate my-app", "bash"))
        cmd = [it for it in s["items"] if it["kind"] == "argocd_command"]
        self.assertEqual([(it["status"], it["why"]) for it in cmd], [("undecided", "argocd_plugins_possible")])

    def test_group_command_flag_is_undecided(self):
        s = check("Use `argocd app --local`.")
        self.assertEqual([it["status"] for it in s["items"] if it["kind"] == "argocd_flag"], ["undecided"])

    def test_global_flag_before_the_command(self):
        s = check(block("argocd --server host app list", "bash"))
        self.assertEqual([it for it in s["items"] if it["status"] == "nonexistent"], [])


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class ApiVersions(unittest.TestCase):
    def test_unserved_version_is_nonexistent(self):
        s = statuses(block("apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nmetadata:\n  name: x"), "api_version")
        self.assertEqual(s["Kafka kafka.strimzi.io/v1beta2"], "nonexistent")

    def test_served_version_exists(self):
        s = statuses(block("apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: x"), "api_version")
        self.assertEqual(s["KafkaTopic kafka.strimzi.io/v1"], "exists")

    def test_argocd_version(self):
        s = statuses(block("apiVersion: argoproj.io/v1beta1\nkind: Application\nmetadata:\n  name: x"), "api_version")
        self.assertEqual(s["Application argoproj.io/v1beta1"], "nonexistent")

    def test_old_version_shown_on_purpose_is_undecided(self):
        txt = "The previous manifest, before the upgrade, looked like this:\n" + block(
            "apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nmetadata:\n  name: x")
        res = check(txt)
        it = [i for i in res["items"] if i["kind"] == "api_version"][0]
        self.assertEqual((it["status"], it["why"]), ("undecided", "old_version_context"))

    def test_other_group_and_core_kinds_are_not_judged(self):
        res = check(block("apiVersion: example.com/v1beta2\nkind: Kafka\nmetadata:\n  name: x"))
        self.assertEqual([i["status"] for i in res["items"] if i["kind"] == "api_version"], ["undecided"])
        res = check(block("apiVersion: batch/v1beta1\nkind: CronJob\nmetadata:\n  name: x"))
        self.assertEqual([i["status"] for i in res["items"] if i["kind"] == "api_version"], ["undecided"])

    def test_fields_under_an_unserved_version_are_not_judged(self):
        res = check(block("apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nmetadata:\n  name: x\n"
                          "spec:\n  kafka:\n    replicas: 3"))
        self.assertEqual([i for i in res["items"] if i["kind"] == "manifest_field" and i["status"] == "nonexistent"], [])


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class Manifests(unittest.TestCase):
    KAFKA = "apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: x\nspec:\n  kafka:\n    version: 4.3.0\n%s"

    def test_unknown_field_of_a_crd_is_nonexistent(self):
        s = statuses(block(self.KAFKA % "    replicas: 3"), "manifest_field")
        self.assertEqual(s["Kafka spec.kafka.replicas"], "nonexistent")
        self.assertEqual(s["Kafka spec.kafka.version"], "exists")

    def test_children_of_an_unknown_field_are_not_listed(self):
        s = statuses(block(self.KAFKA % "    nodes:\n      a: 1\n      b: 2"), "manifest_field")
        self.assertEqual(s["Kafka spec.kafka.nodes"], "nonexistent")
        self.assertNotIn("Kafka spec.kafka.nodes.a", s)

    def test_free_form_config_map_is_not_a_field_check(self):
        s = statuses(block(self.KAFKA % "    config:\n      offsets.topic.replication.factor: 3"), "manifest_field")
        self.assertEqual(s["Kafka spec.kafka.config"], "exists")
        self.assertNotIn("Kafka spec.kafka.config.offsets.topic.replication.factor", s)

    def test_elision_marker_makes_the_structure_undecided(self):
        res = check(block(self.KAFKA % "    replicas: 3\n    # ..."))
        self.assertEqual([i for i in res["items"] if i["status"] == "nonexistent"], [])
        self.assertIn("elided_snippet", [i["why"] for i in res["items"]])

    def test_placeholder_read_as_flow_mapping_is_skipped(self):
        res = check(block(self.KAFKA % "    image: {Image}"))
        self.assertEqual([i for i in res["items"] if i["status"] == "nonexistent"], [])

    def test_kubernetes_kind_is_not_called_nonexistent(self):
        txt = block("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: x\nspec:\n  bogusField: 1\n"
                    "  replicas: 2")
        s = check(txt)
        self.assertEqual([i for i in s["items"] if i["status"] == "nonexistent"], [])
        self.assertIn("kubernetes_schema", [i["why"] for i in s["items"]])
        self.assertEqual(statuses(txt, "manifest_field")["Deployment spec.replicas"], "exists")

    def test_unparsable_block_is_undecided(self):
        res = check(block("apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nspec: [unclosed"))
        self.assertIn("yaml_parse", [i["why"] for i in res["items"]])

    def test_argocd_application_fields(self):
        s = statuses(block("apiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  name: x\nspec:\n"
                           "  project: default\n  source:\n    valueFiles: [a.yaml]\n"), "manifest_field")
        self.assertEqual(s["Application spec.project"], "exists")
        self.assertEqual(s["Application spec.source.valueFiles"], "nonexistent")


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class EnumValues(unittest.TestCase):
    SNIP = "spec:\n  kafka:\n    listeners:\n      - name: ext\n        port: 9094\n        type: nodeport\n" \
           "        tls: true\n        authentication:\n          type: %s\n"

    def test_enum_value_in_a_snippet_without_kind(self):
        s = {k: v for k, v in statuses(block(self.SNIP % "none"), "enum_value").items() if "authentication" in k}
        self.assertEqual(list(s.values()), ["nonexistent"])
        s = {k: v for k, v in statuses(block(self.SNIP % "tls"), "enum_value").items() if "authentication" in k}
        self.assertEqual(list(s.values()), ["exists"])

    def test_kindless_snippet_of_a_core_resource_is_not_read_as_a_crd(self):
        res = check(block("spec:\n  template:\n    spec:\n      containers:\n        - name: a\n"))
        self.assertEqual(res["items"], [])

    def test_case_only_difference_is_undecided(self):
        s = check(block(self.SNIP % "TLS"))
        self.assertEqual([(i["status"], i["why"]) for i in s["items"]
                          if i["kind"] == "enum_value" and "authentication" in i["ident"]], [("undecided", "case_only")])

    def test_placeholder_value_is_skipped(self):
        s = statuses(block(self.SNIP % "<type>"), "enum_value")
        self.assertEqual([k for k in s if "authentication" in k], [])


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class ProsePaths(unittest.TestCase):
    def test_path_with_its_kind_in_the_line(self):
        s = statuses("Set `spec.kafka.nosuchfield` in the `Kafka` custom resource.", "prose_path")
        self.assertEqual(s["spec.kafka.nosuchfield"], "nonexistent")
        s = statuses("Set `spec.kafka.version` in the `Kafka` custom resource.", "prose_path")
        self.assertEqual(s["spec.kafka.version"], "exists")

    def test_kind_prefix(self):
        s = statuses("The field `KafkaRebalance.spec.hardGoals` is a list.", "prose_path")
        self.assertEqual(s["spec.hardGoals"], "nonexistent")

    def test_heading_names_the_kind(self):
        txt = "### Check the `KafkaConnector` status\n\nLook at:\n- `status.nosuch` - is it set?\n"
        self.assertEqual(statuses(txt, "prose_path")["status.nosuch"], "nonexistent")

    def test_core_kind_in_the_line_does_not_decide(self):
        s = statuses("Reference the `ConfigMap` in the `spec.logging` property of the custom resource.", "prose_path")
        self.assertEqual(s["spec.logging"], "undecided")

    def test_bare_word_kafka_does_not_decide(self):
        s = statuses("In the node pool, set `spec.roles` for the Kafka nodes.", "prose_path")
        self.assertNotEqual(s.get("spec.roles"), "nonexistent")

    def test_path_of_a_kind_the_answer_does_not_name_is_undecided(self):
        s = statuses("Check `status.connectors` first.", "prose_path")
        self.assertNotEqual(s.get("status.connectors"), "nonexistent")


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class ConfigKeys(unittest.TestCase):
    def test_close_miss_is_nonexistent_exact_exists(self):
        s = statuses("Set `min.in.sync.replicas=2` and `min.insync.replicas=2`.", "config_key")
        self.assertEqual(s["min.in.sync.replicas"], "nonexistent")
        self.assertEqual(s["min.insync.replicas"], "exists")

    def test_far_unknown_key_is_undecided(self):
        s = statuses("Set `log.flavour.of.the.month=3` for fun.", "config_key")
        self.assertEqual(s["log.flavour.of.the.month"], "undecided")

    def test_foreign_and_open_prefixes_are_undecided(self):
        s = statuses("Use `producer.override.compression.typo` and `key.converter.schemas.enable`.", "config_key")
        self.assertNotIn("nonexistent", s.values())

    def test_line_about_another_client_is_undecided(self):
        s = check("In the Spring application set `min.in.sync.replicas=2` in the client.")
        it = [i for i in s["items"] if i["ident"] == "min.in.sync.replicas"][0]
        self.assertEqual((it["status"], it["why"]), ("undecided", "other_client_line"))

    def test_properties_block(self):
        s = statuses(block("num.partitions=3\nlog.retention.hour=24\n# group.min.session.typo=1", "properties"),
                     "config_key")
        self.assertEqual(s["num.partitions"], "exists")
        self.assertEqual(s["log.retention.hour"], "nonexistent")
        self.assertNotIn("group.min.session.typo", s)

    def test_resource_paths_and_files_are_not_keys(self):
        s = statuses("See `status.conditions`, `metadata.generation` and `server.properties`.", "config_key")
        self.assertEqual(s, {})

    def test_key_that_existed_in_an_older_version_is_undecided(self):
        hist = [e for e in CTX.cats.data["kafka_config_history"]["entries"]
                if e["versions"].get("4.1") and not e["versions"].get("4.3")]
        if not hist:
            self.skipTest("no key of the history was removed after 4.1")
        s = check("Set `%s`." % hist[0]["key"])
        it = [i for i in s["items"] if i["ident"] == hist[0]["key"]][0]
        self.assertNotEqual(it["status"], "nonexistent")

    def test_kafkatopic_config_takes_topic_keys_only(self):
        txt = block("apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: t\nspec:\n  config:\n"
                    "    retention.ms: 1000\n    log.cleaner.backoff.ms: 15000\n")
        s = statuses(txt, "config_key")
        self.assertEqual(s["retention.ms"], "exists")
        self.assertEqual(s["log.cleaner.backoff.ms"], "nonexistent")

    def test_kafkatopic_scope_does_not_need_a_served_apiversion(self):
        txt = block("apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: t\nspec:\n  config:\n"
                    "    log.cleaner.backoff.ms: 15000\n")
        self.assertEqual(statuses(txt, "config_key")["log.cleaner.backoff.ms"], "nonexistent")

    def test_topic_only_key_in_broker_config(self):
        txt = block("apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: x\nspec:\n  kafka:\n"
                    "    config:\n      local.retention.ms: 1000\n      log.retention.ms: 1000\n"
                    "      min.insync.replicas: 2\n")
        s = statuses(txt, "config_key")
        self.assertEqual(s["local.retention.ms"], "nonexistent")
        self.assertEqual(s["log.retention.ms"], "exists")
        self.assertEqual(s["min.insync.replicas"], "exists")


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class ConfigValues(unittest.TestCase):
    def test_enumerated_values(self):
        s = statuses("Set `errors.tolerance=record`, `errors.tolerance=all`, `compression.type=GZIP`.", "config_value")
        self.assertEqual(s["errors.tolerance=record"], "nonexistent")
        self.assertEqual(s["errors.tolerance=all"], "exists")
        self.assertEqual(s["compression.type=GZIP"], "undecided")

    def test_list_value(self):
        s = statuses("Set `cleanup.policy=compact,delete` and `cleanup.policy=compact,purge`.", "config_value")
        self.assertEqual(s["cleanup.policy=compact,delete"], "exists")
        self.assertEqual(s["cleanup.policy=compact,purge"], "nonexistent")

    def test_value_of_a_key_without_enumeration_is_not_judged(self):
        self.assertEqual(statuses("Set `retention.ms=forever`.", "config_value"), {})


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class Rest(unittest.TestCase):
    def test_method_not_served(self):
        s = statuses(block("curl -X POST http://c:8083/connectors/my-conn/config", "bash"), "rest_operation")
        self.assertEqual(s["POST /connectors/{connector}/config"], "nonexistent")
        s = statuses(block("curl -X PUT http://c:8083/connectors/my-conn/config -d @x.json", "bash"), "rest_operation")
        self.assertEqual(s["PUT /connectors/{connector}/config"], "exists")

    def test_default_method_follows_curl(self):
        s = statuses(block("curl http://c:8083/connectors/my-conn/status", "bash"), "rest_operation")
        self.assertEqual(s["GET /connectors/{connector}/status"], "exists")
        s = statuses(block("curl -H 'Content-Type: application/json' -d @x.json http://c:8083/connectors", "bash"),
                     "rest_operation")
        self.assertEqual(s["POST /connectors"], "exists")

    def test_path_in_a_markdown_table_cell(self):
        s = statuses("| Status | `curl -s http://<connect>:8083/connectors/<name>/status` |", "rest_operation")
        self.assertEqual(list(s.values()), ["exists"])

    def test_prose_pair(self):
        s = statuses("Use PUT /connectors/{name}/restart to restart it.", "rest_operation")
        self.assertEqual(s["PUT /connectors/{connector}/restart"], "nonexistent")

    def test_upload_without_method_is_not_judged(self):
        self.assertEqual(statuses(block("curl -T f.json http://c:8083/connectors/x/config", "bash"), "rest_operation"), {})

    def test_other_services_are_not_judged(self):
        self.assertEqual(statuses(block("curl -X POST http://cc:9090/kafkacruisecontrol/rebalance", "bash")), {})


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class ErrorNames(unittest.TestCase):
    def test_close_miss_in_a_sentence_about_an_error(self):
        s = statuses("The producer fails with the error NOT_ENOUGH_REPLICAS_AFTER_APPENDS.", "protocol_error")
        self.assertEqual(s["NOT_ENOUGH_REPLICAS_AFTER_APPENDS"], "nonexistent")
        s = statuses("The producer fails with the error NOT_ENOUGH_REPLICAS_AFTER_APPEND.", "protocol_error")
        self.assertEqual(s["NOT_ENOUGH_REPLICAS_AFTER_APPEND"], "exists")

    def test_enum_value_and_far_names_are_not_judged(self):
        self.assertEqual(statuses("The error is raised when READ_COMMITTED returns nothing.", "protocol_error"), {})
        self.assertEqual(statuses("There was no error in MY_OWN_CONSTANT_NAME.", "protocol_error"), {})

    def test_sentence_without_an_error_word(self):
        self.assertEqual(statuses("Use NOT_ENOUGH_REPLICAS_AFTER_APPENDS here.", "protocol_error"), {})


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class NameMentionedToDenyIt(unittest.TestCase):
    def test_lines_that_reject_the_name_are_undecided(self):
        for line in ("There is no `kafka-nosuch.sh` in Kafka.",
                     "The key `min.in.sync.replicas` is a typo; the real one is `min.insync.replicas`.",
                     "Is there a `kafka-nosuch.sh`? No.",
                     "Use `min.insync.replicas`, not `min.in.sync.replicas`."):
            res = check(line)
            self.assertEqual([i for i in res["items"] if i["status"] == "nonexistent"], [], line)
            self.assertIn("mentioned_as_nonexistent", [i["why"] for i in res["items"]], line)

    def test_a_line_that_uses_the_name_stays_nonexistent(self):
        res = check("Run `kafka-nosuch.sh` to see the offsets.")
        self.assertEqual([i["status"] for i in res["items"]], ["nonexistent"])


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class Annotations(unittest.TestCase):
    def test_never_nonexistent(self):
        s = check("Add `strimzi.io/no-such-annotation: x`.")
        self.assertEqual([(i["status"], i["why"]) for i in s["items"] if i["kind"] == "annotation"],
                         [("undecided", "annotation_not_complete")])


@unittest.skipUnless(HAVE_REAL, "catalogues not built")
class TotalsAndCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.rows = [
            {"id": "secret-id-77", "text": "Use `kafka-nosuch.sh` and `kafka-topics.sh --list`."},
            {"id": "secret-id-78", "text": "Run `kafka-topics.sh --list`."},
            {"id": "secret-id-79", "text": "", "error": "boom"},
        ]
        self.path = self.dir / "answers.jsonl"
        self.path.write_text("".join(json.dumps(r) + "\n" for r in self.rows), encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def run_main(self, *args):
        out = io.StringIO()
        with redirect_stdout(out):
            rc = ic.main(list(args))
        return rc, out.getvalue()

    def test_totals(self):
        t = ic.totals(ic.check_rows(self.rows, CTX))
        self.assertEqual((t["answers"], t["answers_checked"], t["skipped"]), (3, 2, 1))
        self.assertEqual(t["answers_with_nonexistent"], 1)
        self.assertEqual(t["nonexistent"], 1)
        self.assertEqual(t["nonexistent_per_10_answers"], 5.0)
        self.assertEqual(t["by_kind"]["cli_tool"]["nonexistent"], 1)
        self.assertAlmostEqual(t["share_exists_of_decided"], t["exists"] / (t["exists"] + 1), places=4)

    def test_per_answer_output_names_the_identifier_and_line(self):
        rc, out = self.run_main("--answers", str(self.path))
        self.assertEqual(rc, 0)
        self.assertIn("secret-id-77", out)
        self.assertIn("NONEXISTENT [cli_tool] kafka-nosuch.sh  line 1", out)

    def test_totals_only_prints_no_id_and_no_identifier(self):
        rc, out = self.run_main("--answers", str(self.path), "--totals-only")
        self.assertEqual(rc, 0)
        for leak in ("secret-id", "kafka-nosuch", "kafka-topics", "boom", "line 1"):
            self.assertNotIn(leak, out)
        self.assertIn("nonexistent identifiers: 1", out)

    def test_json_never_overwrites(self):
        dest = self.dir / "out.json"
        self.assertEqual(self.run_main("--answers", str(self.path), "--json", str(dest))[0], 0)
        self.assertEqual(json.loads(dest.read_text())["totals"]["nonexistent"], 1)
        err = io.StringIO()
        with redirect_stdout(io.StringIO()), unittest.mock.patch("sys.stderr", err):
            rc = ic.main(["--answers", str(self.path), "--json", str(dest)])
        self.assertEqual(rc, 2)

    def test_hidden_path_is_totals_only_and_refuses_json(self):
        stub = types.ModuleType("eval2")
        stub.read_jsonl = lambda p: self.rows
        hidden = self.dir / "answers.jsonl.enc"
        hidden.write_bytes(b"x")
        with unittest.mock.patch.dict(sys.modules, {"eval2": stub}):
            rc, out = self.run_main("--answers", str(hidden))
            self.assertEqual(rc, 0)
            for leak in ("secret-id", "kafka-nosuch", "kafka-topics", "line 1", "answers.jsonl"):
                self.assertNotIn(leak, out)
            self.assertIn("hidden: totals only", out)
            err = io.StringIO()
            with unittest.mock.patch("sys.stderr", err):
                self.assertEqual(ic.main(["--answers", str(hidden), "--json", str(self.dir / "x.json")]), 2)
                self.assertEqual(ic.main(["--answers", str(hidden), "--show-undecided"]), 2)

    def test_report_block_for_the_harness(self):
        txt = ic.report_block(self.rows, hidden=True)
        self.assertIn("hidden: totals only", txt)
        for leak in ("secret-id", "kafka-nosuch", "kafka-topics"):
            self.assertNotIn(leak, txt)

    def test_rows_without_ids_get_a_position(self):
        res = ic.check_rows([{"text": "Use `kafka-nosuch.sh`."}], CTX)
        self.assertEqual(res[0][0], "#1")


class NoCheckerImport(unittest.TestCase):
    def test_module_does_not_import_the_runtime_checker(self):
        src = (ROOT / "scripts" / "eval2_idcheck.py").read_text()
        self.assertNotIn("kafka_sre_rag.checker", src)
        self.assertNotIn("from kafka_sre_rag import checker", src)


if __name__ == "__main__":
    import unittest.mock  # noqa: F401
    unittest.main()
