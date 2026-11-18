"""Parsers added for the catalogue sources (catalogue_sources.yaml) on small inline fixtures:
tool options from Java and Scala source, OpenAPI / CRD schemas, Cluster Operator install files, the
configuration history. No raw/ or raw_catalogue/ needed."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import build_catalogues as bc  # noqa: E402
import fetch_catalogue_sources as fc  # noqa: E402

JOPT = '''
public class TopicCommandOptions extends CommandDefaultOptions {
    // parser.accepts("commented-out", "never");
    public TopicCommandOptions(String[] args) {
        super(args);
        bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server " +
                "to connect to.")
            .withRequiredArg()
            .describedAs("server to connect to")
            .ofType(String.class);
        listOpt = parser.accepts("list", "List all available topics.");
        describeOpt = parser.accepts("describe", "List details for the given topics.");
        topicOpt = parser
            .accepts("topic", "The topic to describe.")
            .withRequiredArg()
            .availableIf(describeOpt, listOpt);
        parser.acceptsAll(Arrays.asList("c", "command-config"), "Config file.").withOptionalArg();
        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
        CommandLineUtils.checkInvalidArgs(parser, options, listOpt, describeOpt);
    }
}
'''

SCALA = '''
object Kafka {
  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values")
      .withRequiredArg()
      .ofType(classOf[String])
    optionParser.accepts("version", "Print version information and exit.")
  }
}
'''

ARGP = '''
ArgumentParser parser = ArgumentParsers.newArgumentParser("kafka-features").defaultHelp(true);
MutuallyExclusiveGroup bootstrapGroup = parser.addMutuallyExclusiveGroup();
bootstrapGroup.addArgument("--bootstrap-server").help("Brokers.");
parser.addArgument("--command-config").type(Arguments.fileType()).help("Config" + " file.");
Subparser upgradeParser = subparsers.addParser("upgrade").help("Upgrade.");
upgradeParser.addArgument("--dry-run").help("Validate only.").action(storeTrue());
upgradeParser.addArgument("--feature", "-f").action(append());
parser.addArgument("config").help("positional");
'''


def parse(src, name="Tool.java", consts=None):
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / name
        f.write_text(src)
        return bc.parse_option_source(f, src, consts or {})


class MaskAndSplitTests(unittest.TestCase):
    def test_comments_blanked_strings_kept_and_length_equal(self):
        s = 'a = "x // not a comment"; // gone\nb = 1; /* gone\n too */ c = \'"\';'
        m = bc.mask_comments(s)
        self.assertEqual(len(m), len(s))
        self.assertIn('"x // not a comment"', m)
        self.assertNotIn("gone", m)
        self.assertEqual(m.count("\n"), s.count("\n"))

    def test_split_args_ignores_commas_in_strings_and_calls(self):
        s = 'f("a, b", g(1, 2), "c")'
        args, end = bc.split_args(s, 1)
        self.assertEqual([s[x:y].strip() for x, y in args], ['"a, b"', "g(1, 2)", '"c"'])
        self.assertEqual(s[end], ")")

    def test_literals_and_partial_detection(self):
        self.assertEqual(bc.literals('"a" + "b\\n"'), ["a", "b\n"])
        self.assertTrue(bc.only_literals('"a" +\n "b"'))
        self.assertFalse(bc.only_literals('"a" + Foo.BAR'))
        self.assertFalse(bc.only_literals('String.format("a %s", x)'))

    def test_dash(self):
        self.assertEqual((bc.dash("x"), bc.dash("topic"), bc.dash("--a"), bc.dash("<c>")), ("-x", "--topic", "--a", "<c>"))


class JoptsimpleTests(unittest.TestCase):
    def setUp(self):
        self.opts, self.cons, self.problems, self.var2opt, self.inherits, self.argp = parse(JOPT)

    def test_options_with_argument_kind_and_description(self):
        o = self.opts
        self.assertEqual(sorted(o), ["--bootstrap-server", "--command-config", "--describe", "--list", "--topic", "-c"])
        self.assertEqual(o["--bootstrap-server"]["takes_argument"], "required")
        self.assertEqual(o["--bootstrap-server"]["description"], "REQUIRED: The Kafka server to connect to.")
        self.assertEqual(o["--bootstrap-server"]["argument_name"], "server to connect to")
        self.assertEqual(o["--list"]["takes_argument"], "none")
        self.assertEqual(o["--command-config"]["takes_argument"], "optional")
        self.assertEqual(o["--command-config"]["aliases"], ["-c"])

    def test_commented_option_is_not_read(self):
        self.assertNotIn("--commented-out", self.opts)

    def test_constraints_use_option_names(self):
        self.assertEqual(self.var2opt["describeOpt"], "--describe")
        av = [c for c in self.cons if c.get("kind") == "availableIf"][0]
        self.assertEqual((av["option"], av["others"]), ("--topic", ["describeOpt", "listOpt"]))  # mapped later by parse_tool
        ck = [c for c in self.cons if c.get("kind") == "checkInvalidArgs"][0]
        self.assertEqual(ck["args"], ["listOpt", "describeOpt"])
        self.assertEqual([c for c in self.cons if c.get("kind") == "checkRequiredArgs"][0]["args"], ["bootstrapServerOpt"])

    def test_inherits_default_options(self):
        self.assertTrue(self.inherits)
        self.assertEqual(self.problems, [])

    def test_name_from_constant(self):
        src = 'x = parser.accepts(BOOTSTRAP_OPT, "Servers.").withRequiredArg();'
        opts = parse(src, consts={"BOOTSTRAP_OPT": "bootstrap-server"})[0]
        self.assertEqual(list(opts), ["--bootstrap-server"])

    def test_unresolvable_name_is_reported_not_guessed(self):
        opts, _, problems, *_ = parse('x = parser.accepts(someVariable, "d");')
        self.assertEqual(opts, {})
        self.assertEqual(len(problems), 1)

    def test_scala_without_semicolons(self):
        opts = parse(SCALA, "Kafka.scala")[0]
        self.assertEqual(sorted(opts), ["--override", "--version"])
        self.assertEqual(opts["--override"]["takes_argument"], "required")
        self.assertEqual(opts["--version"]["takes_argument"], "none")


class Argparse4jTests(unittest.TestCase):
    def setUp(self):
        self.opts, self.cons, self.problems, _, _, self.argp = parse(ARGP)

    def test_options_and_subcommands(self):
        o = self.opts
        self.assertTrue(self.argp)
        self.assertEqual(o["--bootstrap-server"]["global"], True)
        self.assertEqual(o["--command-config"]["description"], "Config file.")
        self.assertEqual(o["--dry-run"]["commands"], ["upgrade"])
        self.assertEqual(o["--dry-run"]["takes_argument"], "none")
        self.assertEqual(o["--feature"]["takes_argument"], "required")
        self.assertIn("-f", o)
        self.assertIn("<config>", o)

    def test_tool_classes_from_script(self):
        self.assertEqual(bc.tool_classes('exec $(dirname $0)/kafka-run-class.sh org.apache.kafka.tools.TopicCommand "$@"'),
                         [(None, "org.apache.kafka.tools.TopicCommand")])
        self.assertEqual(bc.tool_classes('exec $x/kafka-run-class.sh $EXTRA_ARGS kafka.Kafka "$@"'), [(None, "kafka.Kafka")])
        cases = 'case $a in\n    agent) CLASS="org.x.Agent";;\n    client) CLASS="org.x.Client";;\nesac'
        self.assertEqual(bc.tool_classes(cases), [("agent", "org.x.Agent"), ("client", "org.x.Client")])


class ParseToolTests(unittest.TestCase):
    def test_tool_is_complete_only_when_the_parser_was_read(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "tools/src/main/java/org/x"
            src.mkdir(parents=True)
            (src / "TopicCommand.java").write_text(JOPT)
            (src / "Other.java").write_text("public class Other { void f() {} }")
            (root / "bin").mkdir()
            good, bad, none = (root / "bin" / n for n in ("kafka-topics.sh", "kafka-other.sh", "kafka-run-class.sh"))
            good.write_text('exec $(dirname $0)/kafka-run-class.sh org.x.TopicCommand "$@"')
            bad.write_text('exec $(dirname $0)/kafka-run-class.sh org.x.Other "$@"')
            none.write_text("echo hello")
            idx = bc.cls_index(root)
            self.assertEqual(sorted(idx), ["org.x.Other", "org.x.TopicCommand"])
            default = {"--help": {"takes_argument": "none", "description": "", "files": [], "evidence": "source_code"}}
            r = bc.parse_tool(good, root, idx, {}, default)
            self.assertTrue(r["complete"])
            self.assertIn("--help", r["options"])
            av = [c for c in r["constraints"] if c.get("kind") == "availableIf"][0]
            self.assertEqual(av["others"], ["--describe", "--list"])   # variables replaced by option names
            r = bc.parse_tool(bad, root, idx, {}, default)
            self.assertFalse(r["complete"])
            self.assertIn("no option definition", r["reason"])
            r = bc.parse_tool(none, root, idx, {}, default)
            self.assertFalse(r["complete"])
            self.assertIn("kafka-run-class.sh", r["reason"])


CRD = {
    "kind": "CustomResourceDefinition",
    "spec": {
        "group": "kafka.example.io", "scope": "Namespaced", "names": {"kind": "Thing", "plural": "things"},
        "versions": [
            {"name": "v1", "served": True, "storage": True, "schema": {"openAPIV3Schema": {
                "type": "object", "required": ["spec"],
                "properties": {
                    "apiVersion": {"type": "string"}, "kind": {"type": "string"}, "metadata": {"type": "object"},
                    "spec": {"type": "object", "required": ["name"], "properties": {
                        "name": {"type": "string"},
                        "mode": {"type": "string", "enum": ["b", "a"]},
                        "size": {"x-kubernetes-int-or-string": True, "anyOf": [{"type": "integer"}, {"type": "string"}]},
                        "config": {"type": "object", "x-kubernetes-preserve-unknown-fields": True},
                        "labels": {"type": "object", "additionalProperties": {"type": "string"}},
                        "items": {"type": "array", "items": {"type": "object", "properties": {
                            "value": {"type": "string"}, "valueFrom": {"type": "object"}},
                            "oneOf": [{"required": ["value"]}, {"required": ["valueFrom"]}]}},
                        "tags": {"type": "array", "items": {"type": "string", "enum": ["x", "y"]}},
                    }},
                    "status": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
                }}}},
            {"name": "v0", "served": False, "storage": False, "schema": {"openAPIV3Schema": {"type": "object"}}},
        ]}}


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.paths = bc.flatten_schema(CRD["spec"]["versions"][0]["schema"]["openAPIV3Schema"])

    def test_paths_types_enum_required_open(self):
        p = self.paths
        self.assertNotIn("apiVersion", p)
        self.assertNotIn("metadata", p)
        self.assertEqual(p["spec"]["required"], True)
        self.assertEqual(p["spec.name"], {"type": ["string"], "required": True})
        self.assertEqual(p["spec.mode"]["enum"], ["a", "b"])
        self.assertEqual(p["spec.size"]["type"], ["integer", "string"])
        self.assertEqual((p["spec.config"]["open"], p["spec.config"]["preserve_unknown"]), (True, True))
        self.assertEqual((p["spec.labels"]["open"], p["spec.labels"]["map_values"]), (True, ["string"]))
        self.assertEqual(p["spec.items"]["type"], ["array"])
        self.assertEqual(p["spec.items[]"]["one_of_required"], [["value"], ["valueFrom"]])
        self.assertEqual(p["spec.items[].value"]["type"], ["string"])
        self.assertEqual(p["spec.tags[]"]["enum"], ["x", "y"])
        self.assertEqual(p["status.ok"]["type"], ["boolean"])

    def test_only_served_versions_become_entries(self):
        es = bc.crd_entries([(Path("x.yaml"), CRD)], "pin")
        self.assertEqual([(e["kind"], e["api_version"]) for e in es], [("Thing", "kafka.example.io/v1")])
        self.assertTrue(es[0]["complete"])
        self.assertEqual(es[0]["provenance"], ["exists"])
        self.assertEqual(es[0]["source"], "crd")

    def test_ref_allof_and_recursion(self):
        defs = {
            "A": {"type": "object", "properties": {"b": {"$ref": "#/definitions/B"}, "q": {"$ref": "#/definitions/Quantity"}}},
            "B": {"type": "object", "properties": {"self": {"allOf": [{"$ref": "#/definitions/B"}]}, "n": {"type": "integer"}}},
            "io.k8s.apimachinery.pkg.api.resource.Quantity": {"type": "string"},
        }
        defs["Quantity"] = defs["io.k8s.apimachinery.pkg.api.resource.Quantity"]
        p = bc.flatten_schema({"$ref": "#/definitions/A"}, defs)
        self.assertEqual(p["b.n"]["type"], ["integer"])
        self.assertEqual((p["b.self"]["recursive"], p["b.self"]["open"]), (True, True))
        self.assertNotIn("b.self.n", p)
        self.assertEqual(p["q"]["type"], ["string"])   # only the real Quantity definition widens the type
        with self.assertRaises(bc.MissingSource):
            bc.flatten_schema({"$ref": "#/definitions/Nope"}, defs)

    def test_quantity_accepts_numbers(self):
        defs = {"io.k8s.apimachinery.pkg.api.resource.Quantity": {"type": "string"}}
        p = bc.flatten_schema({"type": "object", "properties": {"cpu": {"$ref": "#/definitions/io.k8s.apimachinery.pkg.api.resource.Quantity"}}}, defs)
        self.assertEqual(p["cpu"]["type"], ["integer", "number", "string"])


DEPLOY = """
apiVersion: apps/v1
kind: Deployment
metadata: {name: op, labels: {app: strimzi}}
spec:
  selector: {matchLabels: {name: op, strimzi.io/kind: cluster-operator}}
  template:
    metadata: {labels: {name: op, strimzi.io/kind: cluster-operator}}
    spec:
      serviceAccountName: op-sa
      containers:
        - name: op
          image: quay.io/x/operator:1
          args: [run.sh]
          env:
            - {name: A, value: "1"}
            - name: B
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
"""
CM = "kind: ConfigMap\napiVersion: v1\nmetadata: {name: op, labels: {app: strimzi}}\ndata:\n  log4j2.properties: |\n    a = b\n"


class OperatorFactsTests(unittest.TestCase):
    def test_labels_env_and_configmap(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "060-Deployment-strimzi-cluster-operator.yaml").write_text(DEPLOY)
            (d / "050-ConfigMap-strimzi-cluster-operator.yaml").write_text(CM)
            o = bc.operator_deployment(d, "pin")
        self.assertEqual(o["pod_labels"], {"name": "op", "strimzi.io/kind": "cluster-operator"})
        self.assertEqual(o["env"]["A"], {"value": "1"})
        self.assertIn("fieldRef", o["env"]["B"]["value_from"])
        self.assertEqual(o["configmap"]["data_keys"], ["log4j2.properties"])
        self.assertEqual(o["container"]["image"], "quay.io/x/operator:1")


def cfg_html(entries):
    li = "".join('<li>\n<h4><a id="%s"></a><a id="x_%s" href="#x">%s</a></h4>\n<p>d</p>\n<table><tbody>'
                 '<tr><th>Type:</th><td>%s</td></tr><tr><th>Default:</th><td>%s</td></tr>'
                 '<tr><th>Valid Values:</th><td></td></tr><tr><th>Importance:</th><td>low</td></tr>'
                 '</tbody></table>\n</li>' % (k, k, k, t, dflt) for k, t, dflt in entries)
    return '<ul class="config-list">%s</ul>' % li


class ConfigHistoryTests(unittest.TestCase):
    def test_changes_between_versions(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            pages = {"4.1": [("a", "string", "null"), ("same", "int", "1"), ("gone", "int", "1")],
                     "4.2": [("a", "list", "null"), ("same", "int", "1"), ("new", "int", "2"), ("gone", "int", "1")],
                     "4.3": [("a", "list", "\"\""), ("same", "int", "1"), ("new", "int", "3")]}
            for ver, es in pages.items():
                (d / ver).mkdir()
                (d / ver / "producer_config.html").write_text(cfg_html(es))
            versions = {v: bc.config_entries_of(d / v, "pin", v) for v in pages}
            self.assertEqual(sorted(k[2] for k in versions["4.3"]), ["a", "new", "same"])
            es = bc.diff_config_versions(versions, ["4.1", "4.2", "4.3"])
            self.assertEqual({e["key"]: e["changes"] for e in es},
                             {"a": ["default", "type"], "gone": ["removed_after_4.2"], "new": ["added_in_4.2", "default"]})
            a = [e for e in es if e["key"] == "a"][0]
            self.assertEqual((a["versions"]["4.1"]["type"], a["versions"]["4.3"]["type"]), ("string", "list"))
            self.assertIsNone([e for e in es if e["key"] == "new"][0]["versions"]["4.1"])


class FetchHelpersTests(unittest.TestCase):
    def test_licence_detection(self):
        apache = "                                 Apache License\n                           Version 2.0, January 2004\n"
        self.assertEqual(fc.licence_of(apache), "Apache-2.0")
        self.assertEqual(fc.licence_of("MIT License\nPermission is hereby granted"), "unknown")

    def test_source_list_is_pinned_and_apache(self):
        root = Path(__file__).resolve().parent.parent
        src = yaml.safe_load((root / "catalogue_sources.yaml").read_text())["sources"]
        self.assertEqual([s["name"] for s in src],
                         ["strimzi-install", "apache-kafka-tools", "kubernetes-api", "argocd-crds", "kafka-config-pages"])
        for s in src:
            self.assertRegex(s["commit"], r"^[0-9a-f]{40}$")
            self.assertEqual(s["license"], "Apache-2.0")
            self.assertTrue(s["globs"] and s["paths"])
        # the sources.yaml pins these must match (same releases as the documentation)
        docs = {s["name"]: s for s in yaml.safe_load((root / "sources.yaml").read_text())["sources"]}
        by = {s["name"]: s for s in src}
        self.assertEqual(by["strimzi-install"]["ref"], str(docs["strimzi"]["ref"]))
        self.assertEqual(by["apache-kafka-tools"]["ref"], str(docs["apache-kafka"]["ref"]))
        self.assertEqual(by["argocd-crds"]["ref"], str(docs["argo-cd"]["ref"]))
        self.assertEqual(by["kafka-config-pages"]["commit"], str(docs["apache-kafka-config"]["ref"]))

    def test_catalogue_sources_never_enter_the_index_sources(self):
        root = Path(__file__).resolve().parent.parent
        docs = {s["name"] for s in yaml.safe_load((root / "sources.yaml").read_text())["sources"]}
        for s in yaml.safe_load((root / "catalogue_sources.yaml").read_text())["sources"]:
            self.assertNotIn(s["name"], docs)


if __name__ == "__main__":
    unittest.main()
