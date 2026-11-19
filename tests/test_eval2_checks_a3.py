"""The a3 checks (scripts/eval2_checks_a3.py): each check type has a passing and a failing answer, on hand-written
synthetic items only (no evaluation item, no hidden file). The Properties parser is compared with the real
java.util.Properties on 20+ cases when javac and java are installed (skipped otherwise). The forbidden-keys check
runs on a synthetic table in the shared format of the WP1c brief.
"""
import base64
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2_checks_a3 as a3  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues  # noqa: E402
from test_eval2_score import fixture_catalogues  # noqa: E402


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = fixture_catalogues()
        cls.c = Catalogues.load(cls.dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def run1(self, chk, text):
        return sc.run_checks({"id": "x", "checks": [chk]}, text, self.c)[0]

    def ok(self, chk, text):
        r = self.run1(chk, text)
        self.assertTrue(r["pass"], r)

    def bad(self, chk, text, in_detail=None):
        r = self.run1(chk, text)
        self.assertFalse(r["pass"], r)
        if in_detail:
            self.assertIn(in_detail, r["detail"])


def fence(body, lang="yaml"):
    return "```%s\n%s\n```" % (lang, body)


class Wiring(unittest.TestCase):
    def test_merged_into_the_scorer_and_versioned(self):
        self.assertEqual(sc.VERSION, "eval2_score/3")
        for name in a3.CHECKS_A3:
            self.assertIs(sc.CHECKS[name], a3.CHECKS_A3[name])
        self.assertEqual(len(sc.CHECKS), 21 + len(a3.CHECKS_A3))   # the 21 earlier types are unchanged


# ---------------------------------------------------------------- semantic_rule: NetworkPolicy
NS = {"app": {"team": "a"}, "other": {"team": "b"}}
SRC_OTHER = {"namespace": "other", "pod_labels": {"role": "client"}}
SRC_OWN = {"namespace": "kafka", "pod_labels": {"role": "client"}}
TARGET = {"namespace": "kafka", "pod_labels": {"app": "broker"}}


def netpol(frm, extra=""):
    inner = "\n".join("    " + l for l in frm.strip("\n").split("\n")) if frm else ""
    ing = "  ingress:\n  - from:\n" + inner if frm else "  ingress:\n  - {}"
    return fence("apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: p\n  namespace: kafka\n"
                 "spec:\n  podSelector:\n    matchLabels:\n      app: broker\n" + extra + ing + "\n")


class NetpolTests(Base):
    def chk(self, source, expect, **kw):
        return dict({"type": "semantic_rule", "oracle": "netpol_allows", "source": source, "target": TARGET,
                     "namespaces": {"other": {"team": "b"}, "kafka": {}}, "expect": expect}, **kw)

    def test_pod_selector_alone_means_the_policys_own_namespace(self):
        t = netpol("- podSelector:\n    matchLabels:\n      role: client")
        self.ok(self.chk(SRC_OWN, True), t)
        self.bad(self.chk(SRC_OTHER, True), t, "no rule matches")
        self.ok(self.chk(SRC_OTHER, False), t)

    def test_namespace_selector_alone_means_all_pods_of_matching_namespaces(self):
        t = netpol("- namespaceSelector:\n    matchLabels:\n      team: b")
        self.ok(self.chk({"namespace": "other", "pod_labels": {"anything": "x"}}, True), t)
        self.ok(self.chk(SRC_OWN, False), t)

    def test_both_in_one_entry_is_and_separate_entries_are_or(self):
        both = netpol("- namespaceSelector:\n    matchLabels:\n      team: b\n  podSelector:\n    matchLabels:\n      role: client")
        sep = netpol("- namespaceSelector:\n    matchLabels:\n      team: b\n- podSelector:\n    matchLabels:\n      role: client")
        stranger = {"namespace": "other", "pod_labels": {"role": "other"}}
        self.ok(self.chk(SRC_OTHER, True), both)
        self.ok(self.chk(stranger, False), both)         # right namespace, wrong pod: AND fails
        self.ok(self.chk(stranger, True), sep)            # OR: any pod of the namespace
        self.ok(self.chk(SRC_OWN, True), sep)             # OR: the role=client pod of the own namespace
        self.ok(self.chk({"namespace": "kafka", "pod_labels": {"role": "x"}}, False), sep)

    def test_name_label_and_expressions(self):
        t = netpol("- namespaceSelector:\n    matchExpressions:\n    - key: kubernetes.io/metadata.name\n      operator: In\n      values: [other]")
        self.ok(self.chk(SRC_OTHER, True), t)
        self.ok(self.chk(SRC_OWN, False), t)

    def test_empty_from_allows_all_and_no_rule_denies_all(self):
        self.ok(self.chk(SRC_OTHER, True), netpol(""))
        none = fence("apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: p\n  namespace: kafka\n"
                     "spec:\n  podSelector: {}\n  policyTypes: [Ingress]\n")
        self.ok(self.chk(SRC_OTHER, False), none)

    def test_ports_and_ip_block(self):
        # RFC 5737 documentation range: example addresses only, never a real host
        t = netpol("- ipBlock:\n    cidr: 192.0.2.0/24\n    except: [192.0.2.5/32]")
        self.ok(self.chk({"namespace": "x", "ip": "192.0.2.9"}, True), t)
        self.ok(self.chk({"namespace": "x", "ip": "192.0.2.5"}, False), t)
        ported = fence("apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: p\n  namespace: kafka\n"
                       "spec:\n  podSelector: {}\n  ingress:\n  - from:\n    - podSelector: {}\n    ports:\n"
                       "    - port: 9092\n      endPort: 9094\n")
        self.ok(self.chk(SRC_OWN, True, port=9093), ported)
        self.ok(self.chk(SRC_OWN, False, port=9095), ported)

    def test_a_policy_that_does_not_decide_fails_with_a_reason(self):
        other = netpol("- podSelector: {}").replace("app: broker", "app: zk")
        self.bad(self.chk(SRC_OWN, True), other, "does not select")
        self.bad(self.chk(SRC_OWN, True), "no manifest", "no NetworkPolicy")


# ---------------------------------------------------------------- semantic_rule: restart annotations
class RestartTests(Base):
    def chk(self, kind, **expect):
        return {"type": "semantic_rule", "oracle": "restart_annotation", "kind": kind, "expect": expect}

    def test_connector_values(self):
        c = self.chk("KafkaConnector", restart_connector=True, include_tasks=False, only_failed=False)
        self.ok(c, 'kubectl annotate KafkaConnector my-c strimzi.io/restart="true"')
        self.bad(c, 'kubectl annotate KafkaConnector my-c strimzi.io/restart="includeTasks"')   # also restarts tasks
        self.bad(c, 'kubectl annotate KafkaConnector my-c strimzi.io/restart="yes"', "no annotation")
        # `true` combined with parameters is not a form the pinned text shows
        self.bad(self.chk("KafkaConnector", restart_connector=True),
                 'kubectl annotate KafkaConnector my-c strimzi.io/restart="true,includeTasks"', "no effect")
        c2 = self.chk("KafkaConnector", restart_connector=True, include_tasks=True, only_failed=True)
        self.ok(c2, 'kubectl annotate KafkaConnector my-c strimzi.io/restart="includeTasks,onlyFailed"')
        self.ok(c2, 'kubectl annotate KafkaConnector my-c strimzi.io/restart="onlyFailed,includeTasks"')
        self.bad(c2, 'kubectl annotate KafkaConnector my-c strimzi.io/restart="true"')

    def test_task_restart_and_manifest_form(self):
        c = self.chk("KafkaConnector", task_id=0, restart_connector=False)
        self.ok(c, 'kubectl annotate KafkaConnector my-c strimzi.io/restart-task="0"')
        self.ok(c, fence("apiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: c\n  annotations:\n    strimzi.io/restart-task: \"0\"\n"))
        self.bad(c, 'kubectl annotate KafkaConnector my-c strimzi.io/restart-task="x"')

    def test_mirrormaker2_uses_other_annotations_and_the_colon_form(self):
        c = self.chk("KafkaMirrorMaker2", restart_connector=True, include_tasks=True, only_failed=True,
                     connector="a->b.MirrorSourceConnector")
        self.ok(c, 'kubectl annotate KafkaMirrorMaker2 mm2 "strimzi.io/restart-connector=a->b.MirrorSourceConnector:includeTasks,onlyFailed"')
        self.bad(c, 'kubectl annotate KafkaMirrorMaker2 mm2 "strimzi.io/restart-connector=a->b.MirrorSourceConnector"')
        self.bad(c, 'kubectl annotate KafkaMirrorMaker2 mm2 strimzi.io/restart="true"')           # not defined for MM2
        t = self.chk("KafkaMirrorMaker2", task_id=2, connector="a->b.MirrorSourceConnector")
        self.ok(t, 'kubectl annotate KafkaMirrorMaker2 mm2 "strimzi.io/restart-connector-task=a->b.MirrorSourceConnector:2"')
        self.bad(t, 'kubectl annotate KafkaMirrorMaker2 mm2 "strimzi.io/restart-connector-task=a->b.MirrorSourceConnector"')

    def test_wrong_kind(self):
        self.bad(self.chk("KafkaConnector", restart_connector=True), 'kubectl annotate KafkaMirrorMaker2 m strimzi.io/restart="true"')


# ---------------------------------------------------------------- semantic_rule: config_rule, pdb
HB = {"type": "semantic_rule", "oracle": "config_rule", "rules": [
    {"a": "heartbeat.interval.ms", "op": "lt", "b": "session.timeout.ms"},
    {"a": "heartbeat.interval.ms", "op": "le", "b": "session.timeout.ms", "scale": "1/3"}]}
ISR = {"type": "semantic_rule", "oracle": "config_rule", "rules": [
    {"a": "min.insync.replicas", "op": "lt", "b": {"path": "spec.replicas"}}]}


def topic(replicas, isr):
    return fence("apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: t\nspec:\n  partitions: 3\n"
                 "  replicas: %s\n  config:\n    min.insync.replicas: %s\n" % (replicas, isr))


class ConfigRuleTests(Base):
    def test_heartbeat_below_session_and_at_most_a_third(self):
        self.ok(HB, fence("session.timeout.ms=45000\nheartbeat.interval.ms=15000", "properties"))
        self.bad(HB, fence("session.timeout.ms=45000\nheartbeat.interval.ms=20000", "properties"), "x 1/3")
        self.bad(HB, fence("session.timeout.ms=10000\nheartbeat.interval.ms=10000", "properties"), "is not lt")
        self.bad(HB, fence("session.timeout.ms=45000", "properties"), "not set to a number")

    def test_a_default_stands_in_for_a_key_the_answer_leaves_out(self):
        chk = dict(HB, defaults={"session.timeout.ms": 45000})
        self.ok(chk, fence("heartbeat.interval.ms=3000", "properties"))
        self.bad(chk, fence("heartbeat.interval.ms=30000", "properties"))

    def test_values_in_a_yaml_config_map(self):
        cm = "apiVersion: kafka.strimzi.io/v1\nkind: KafkaConnect\nmetadata:\n  name: c\nspec:\n  config:\n    %s\n"
        self.ok(HB, fence(cm % "session.timeout.ms: 30000\n    heartbeat.interval.ms: 9000"))
        self.bad(HB, fence(cm % "session.timeout.ms: 30000\n    heartbeat.interval.ms: 12000"))

    def test_min_isr_below_replicas(self):
        self.ok(ISR, topic(3, 2))
        self.bad(ISR, topic(3, 3), "is not lt")
        self.bad(ISR, topic(2, 3))
        self.bad(ISR, "no manifest", "not set")


class PdbTests(Base):
    def chk(self, replicas, expect):
        return {"type": "semantic_rule", "oracle": "pdb_evicts", "replicas": replicas, "expect": expect}

    def pdb(self, field):
        return fence("apiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: p\nspec:\n  %s\n  selector:\n"
                     "    matchLabels:\n      app: kafka\n" % field)

    def test_max_unavailable_zero_blocks_every_eviction(self):
        self.ok(self.chk(3, False), self.pdb("maxUnavailable: 0"))
        self.ok(self.chk(3, False), self.pdb('maxUnavailable: "0%"'))
        self.bad(self.chk(3, True), self.pdb("maxUnavailable: 0"), "0 eviction")
        self.ok(self.chk(3, True), self.pdb("maxUnavailable: 1"))

    def test_min_available_equal_to_replicas_or_100_percent_blocks(self):
        self.ok(self.chk(3, False), self.pdb("minAvailable: 3"))
        self.ok(self.chk(3, False), self.pdb('minAvailable: "100%"'))
        self.ok(self.chk(3, True), self.pdb("minAvailable: 2"))

    def test_percentages_round_up(self):
        self.ok(self.chk(7, True), self.pdb('minAvailable: "50%"'))     # 4 must stay, 3 may go
        self.ok(self.chk(1, True), self.pdb('maxUnavailable: "30%"'))   # one replica: still allowed (example 4)
        self.ok(self.chk(3, False), self.pdb('minAvailable: "67%"'))    # ceil(2.01) = 3 must stay
        self.ok(self.chk(10, True), self.pdb('minAvailable: "90%"'))    # 9 stay, 1 may go

    def test_exactly_one_allowed_disruption(self):
        c = dict(self.chk(3, True), allowed=1)
        self.ok(c, self.pdb("maxUnavailable: 1"))
        self.ok(c, self.pdb("minAvailable: 2"))
        self.bad(c, self.pdb("maxUnavailable: 3"), "expected exactly 1")   # every pod may go at once
        self.bad(c, self.pdb("maxUnavailable: 2"), "expected exactly 1")
        self.bad(c, self.pdb("maxUnavailable: 0"), "expected exactly 1")
        self.bad(c, self.pdb("minAvailable: 0"), "expected exactly 1")

    def test_no_verdict_cases_fail(self):
        self.bad(self.chk(3, True), self.pdb("minAvailable: 1\n  maxUnavailable: 1"), "only one")
        self.bad(self.chk(3, True), self.pdb("paused: true"), "neither")
        self.bad(self.chk(3, True), "none", "no PodDisruptionBudget")


# ---------------------------------------------------------------- semantic_rule: properties_parse
class PropertiesCheckTests(Base):
    def chk(self, expect, **kw):
        return dict({"type": "semantic_rule", "oracle": "properties_parse", "expect": expect}, **kw)

    def test_a_hash_after_a_value_belongs_to_the_value(self):
        c = self.chk({"retention.ms": "86400000 # one day"})
        self.ok(c, fence("retention.ms=86400000 # one day", "properties"))
        self.bad(c, fence("retention.ms=86400000", "properties"), "expected")
        self.bad(self.chk({"retention.ms": "86400000"}), fence("retention.ms=86400000 # one day", "properties"))

    def test_exact_and_a_block_that_does_not_load(self):
        self.ok(self.chk({"a": "1"}, exact=True), fence("a=1\n# c", "properties"))
        self.bad(self.chk({"a": "1"}, exact=True), fence("a=1\nb=2", "properties"), "extra keys")
        self.bad(self.chk({"a": "1"}), fence("a=\\u12", "properties"), "does not load")
        self.bad(self.chk({"a": "1"}), "no block", "no properties block")


class PropertiesParserTests(unittest.TestCase):
    P = staticmethod(a3.parse_properties)

    def test_separators_and_values(self):
        self.assertEqual(self.P("a=b"), {"a": "b"})
        self.assertEqual(self.P("a:b"), {"a": "b"})
        self.assertEqual(self.P("a b"), {"a": "b"})
        self.assertEqual(self.P("a = b "), {"a": "b "})
        self.assertEqual(self.P("a=b=c"), {"a": "b=c"})
        self.assertEqual(self.P("a b=c"), {"a": "b=c"})
        self.assertEqual(self.P("k"), {"k": ""})
        self.assertEqual(self.P("a\\ b=c"), {"a b": "c"})

    def test_comments_continuations_escapes(self):
        self.assertEqual(self.P("# c\n! d\n  # e\na=1"), {"a": "1"})
        self.assertEqual(self.P("a=x\\\n   y"), {"a": "xy"})
        self.assertEqual(self.P("a=x\\\n# not a comment"), {"a": "x# not a comment"})
        self.assertEqual(self.P("a=x\\\\\nb=y"), {"a": "x\\", "b": "y"})
        self.assertEqual(self.P("a=\\u0041\\t\\q"), {"a": "A\tq"})
        self.assertEqual(self.P("a=1\r\nb=2\rc=3"), {"a": "1", "b": "2", "c": "3"})
        with self.assertRaises(ValueError):
            self.P("a=\\u00")


# ---------------------------------------------------------------- the Properties parser against the real JDK
JAVA_SRC = r"""
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class PropsDump {
    static String b64(String s) { return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8)); }
    public static void main(String[] a) throws Exception {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(a[0]), "case_*.properties")) { for (Path p : ds) files.add(p); }
        Collections.sort(files);
        for (Path p : files) {
            System.out.println("CASE " + p.getFileName());
            try (Reader r = new InputStreamReader(new FileInputStream(p.toFile()), StandardCharsets.UTF_8)) {
                Properties pr = new Properties();
                pr.load(r);
                for (String k : new TreeSet<>(pr.stringPropertyNames())) System.out.println("KV " + b64(k) + " " + b64(pr.getProperty(k)));
                System.out.println("END");
            } catch (IllegalArgumentException e) { System.out.println("ERROR"); }
        }
    }
}
"""

# (name, text): the cases the generator's V4 items and their near neighbours rely on, and the corners of the format
JDK_CASES = [
    "a=b", "a:b", "a b", "a\tb", "a\fb", "a = b", "a : b", "a  =  b", "a=b=c", "a:b:c", "a b=c", "a=b c",
    "retention.ms=86400000 # one day", "retention.ms = 86400000   # one day  ", "a=b ! not a comment",
    "# comment\na=1", "! comment\na=1", "   # indented comment\na=1", "a=1\n# c\nb=2",
    "a=1\\\n   2", "a=1\\\n# still value", "a=1\\\\\nb=2", "a=1\\\\\\\n  2", "a\\\n  b=c", "a=b\\", "a=b \\\n c",
    "a=\\u0041", "a=\\u00e9\\u20ac", "\\u0061=1", "a=\\t\\n\\r\\f", "a=\\q\\z", "a=\\=b", "a\\=b=c", "a\\:b:c", "a\\ b c",
    "a=1\r\nb=2", "a=1\rb=2", "a=1\n\n\nb=2", "=v", ":v", "k", "k=", "k:", "  k  =  v  ", "a=1\na=2",
    "a=\\u00", "a=\\uZZZZ", "a=\\", "\\", "key with spaces = value", "a=b\\\n\\\n c",
    "bootstrap.servers=b1:9092,b2:9092", "sasl.jaas.config=org.x.Login required user=\"u\" pass=\"p\";",
    "a=é", "é=€", "a=1\f\nb=2", "a=1  \\\n  \\\n  2",
]


@unittest.skipUnless(shutil.which("javac") and shutil.which("java"), "no JDK: javac and java are needed")
class PropertiesAgainstTheJdk(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        (cls.tmp / "PropsDump.java").write_text(JAVA_SRC)
        r = subprocess.run(["javac", "-proc:none", "-d", str(cls.tmp), str(cls.tmp / "PropsDump.java")],
                           capture_output=True, text=True, timeout=120)
        if r.returncode:
            raise RuntimeError("javac failed: " + r.stderr[-500:])
        cases = cls.tmp / "cases"
        cases.mkdir()
        for i, t in enumerate(JDK_CASES):
            (cases / ("case_%03d.properties" % i)).write_bytes(t.encode("utf-8"))
        r = subprocess.run(["java", "-Dfile.encoding=UTF-8", "-cp", str(cls.tmp), "PropsDump", str(cases)],
                           capture_output=True, text=True, timeout=120)
        if r.returncode:
            raise RuntimeError("java failed: " + r.stderr[-500:])
        cls.jdk = {}
        cur = None
        for line in r.stdout.splitlines():
            if line.startswith("CASE "):
                cur = int(line.split("_")[1].split(".")[0])
                cls.jdk[cur] = {}
            elif line.startswith("KV "):
                _, k, v = line.split(" ")
                cls.jdk[cur][base64.b64decode(k).decode()] = base64.b64decode(v).decode()
            elif line == "ERROR":
                cls.jdk[cur] = "ERROR"

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_at_least_twenty_cases_and_all_agree(self):
        self.assertGreaterEqual(len(JDK_CASES), 20)
        self.assertEqual(len(self.jdk), len(JDK_CASES))
        errors = sum(v == "ERROR" for v in self.jdk.values())
        self.assertGreaterEqual(errors, 2)   # the malformed \u cases really are rejected by the JDK
        for i, text in enumerate(JDK_CASES):
            want = self.jdk[i]
            try:
                got = a3.parse_properties(text)
            except ValueError:
                got = "ERROR"
            self.assertEqual(got, want, "case %d %r" % (i, text))


# ---------------------------------------------------------------- cmd_order
RESET = {"type": "cmd_order", "steps": [
    {"role": "safe", "tool": "kafka-consumer-groups.sh", "flags_all": ["--reset-offsets"], "flags_forbidden": ["--execute"]},
    {"role": "mutate", "tool": "kafka-consumer-groups.sh", "flags_all": ["--reset-offsets", "--execute"]}]}
REASSIGN = {"type": "cmd_order", "require_verify": True, "steps": [
    {"role": "safe", "tool": "kafka-reassign-partitions.sh", "flags_all": ["--generate"]},
    {"role": "mutate", "tool": "kafka-reassign-partitions.sh", "flags_all": ["--execute"]},
    {"role": "verify", "tool": "kafka-reassign-partitions.sh", "flags_all": ["--verify"]}]}
ARGO = {"type": "cmd_order", "steps": [
    {"role": "safe", "tool": "argocd", "words": ["app", "diff"]},
    {"role": "safe", "tool": "argocd", "words": ["app", "sync"], "flags_all": ["--dry-run"]},
    {"role": "mutate", "tool": "argocd", "words": ["app", "sync"], "flags_forbidden": ["--dry-run"]}]}
G = "bin/kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --topic t --to-earliest --reset-offsets"


class CmdOrderTests(Base):
    def test_dry_run_then_execute_passes(self):
        self.ok(RESET, "```bash\n%s\n%s --execute\n```" % (G, G))
        self.ok(RESET, "First preview:\n```\n%s\n```\nThen apply it:\n```\n%s --execute\n```" % (G, G))
        self.ok(RESET, "run `%s` and then `%s --execute`" % (G, G))

    def test_the_three_perturbations_fail(self):
        self.bad(RESET, "```\n%s --execute\n```" % G, "no safe line before")            # mutating line alone
        self.bad(RESET, "```\n%s --execute\n%s\n```" % (G, G), "no safe line before")   # order swapped
        self.bad(RESET, "```\n%s\n```" % G, "no mutating line")                          # dry run only
        self.bad(RESET, "no command at all", "no mutating line")

    def test_verify_after_and_required_verify(self):
        R = "bin/kafka-reassign-partitions.sh --bootstrap-server b:9092 --reassignment-json-file f.json"
        full = "```\n%s --generate --topics-to-move-json-file t.json --broker-list 1,2\n%s --execute\n%s --verify\n```" % (R, R, R)
        self.ok(REASSIGN, full)
        self.bad(REASSIGN, full.rsplit("\n", 2)[0] + "\n```", "no verify line after")
        self.ok(dict(REASSIGN, require_verify=False), full.rsplit("\n", 2)[0] + "\n```")
        swapped = "```\n%s --generate --broker-list 1\n%s --verify\n%s --execute\n```" % (R, R, R)
        self.bad(REASSIGN, swapped, "no verify line after")

    def test_argocd_words_and_flags(self):
        self.ok(ARGO, "```\nargocd app diff my-app\nargocd app sync my-app\n```")
        self.ok(ARGO, "```\nargocd app sync my-app --dry-run\nargocd app sync my-app --prune\n```")
        self.bad(ARGO, "```\nargocd app sync my-app\n```", "no safe line")
        self.bad(ARGO, "```\nargocd app sync my-app --dry-run\n```", "no mutating line")

    def test_continuations_and_other_tools_are_handled(self):
        self.ok(RESET, "```\n%s \\\n  --dry-run\n%s \\\n  --execute\n```" % (G, G))
        self.bad(RESET, "```\nbin/kafka-topics.sh --describe --topic t\n```", "no mutating line")


# ---------------------------------------------------------------- number_within
class NumberTests(Base):
    def chk(self, value, unit, tol=0.01, **kw):
        return dict({"type": "number_within", "value": value, "unit": unit, "tolerance": tol}, **kw)

    def test_units_and_default_unit(self):
        c = self.chk(1.5, "GiB")
        self.ok(c, "ANSWER: 1.5 GiB")
        self.ok(c, "ANSWER: 1536 MiB")
        self.ok(c, "ANSWER: 1.5")                      # no unit: the asked unit
        self.ok(c, "ANSWER: ~1.5 GiB (about)")
        self.ok(c, "ANSWER: **1.5 GiB**")
        self.bad(c, "ANSWER: 1.5 MiB")
        self.bad(c, "ANSWER: 1.5 GB", "within")        # GB and GiB stay distinct (7.4 %)
        self.ok(self.chk(1.5, "GiB", 0.1), "ANSWER: 1.5 GB")   # but a wide tolerance would let it pass

    def test_decimal_and_binary_prefixes_distinct(self):
        c = self.chk(1000, "MB", 0.02)
        self.ok(c, "ANSWER: 1 GB")
        self.bad(c, "ANSWER: 1 GiB")                   # 1073.7 MB, 7.4 % off
        self.bad(self.chk(1, "KiB", 0.02), "ANSWER: 1 KB")  # 2.4 % off, over 2 %

    def test_time_units(self):
        c = self.chk(604800000, "ms", 0)
        self.ok(c, "ANSWER: 7 days")
        self.ok(c, "ANSWER: 168 h")
        self.ok(c, "ANSWER: 604,800,000")
        self.ok(c, "ANSWER: 10080 min")
        self.bad(c, "ANSWER: 604800 ms", "within")     # factor 1000 slip
        self.bad(c, "ANSWER: 7 GiB", "is a size")      # wrong dimension
        self.ok(self.chk(7, "days", 0), "ANSWER: 604800 s")

    def test_rates_and_tolerance(self):
        c = self.chk(40, "MiB/s", 0.02)
        self.ok(c, "ANSWER: 40 MiB/s")
        self.ok(c, "ANSWER: 2400 MiB per min")
        self.ok(c, "ANSWER: 40.5 MiB/s")
        self.bad(c, "ANSWER: 41 MiB/s")
        self.ok(self.chk(10, "GiB", 0, abs_tolerance=0.5), "ANSWER: 10.4 GiB")

    def test_format_failures(self):
        c = self.chk(5, "GiB")
        self.bad(c, "five gibibytes", "no ANSWER")
        self.bad(c, "ANSWER: about five", "not a number")
        self.bad(c, "ANSWER: 5 apples", "unknown unit")
        self.bad(c, "ANSWER: 5 GiB per broker and 7 GiB total", "not a number")


# ---------------------------------------------------------------- declines, no_invented_value
GAP = "ANSWER: not in my sources\nThe retrieved documentation does not cover this setting."


class DeclineTests(Base):
    def test_declines(self):
        c = {"type": "declines"}
        self.ok(c, GAP)
        self.ok(c, "The retrieved documentation does not cover the default of that key in Kafka 3.5.")
        self.ok(c, "ANSWER: unknown")
        self.bad(c, "ANSWER: 604800000")
        self.bad(c, "The default is 7 days.")

    def test_no_invented_value(self):
        c = {"type": "no_invented_value"}
        self.ok(c, GAP)
        self.bad(c, "ANSWER: 604800000\nThe retrieved documentation does not cover it.", "answer value")
        self.bad(c, "ANSWER: not in my sources\nIt is probably 7 days.", "number")
        self.bad(c, "ANSWER: not in my sources\n```properties\nlog.retention.hours=168\n```", "code block")

    def test_round_2_premise_versions_negative_numbers_hedges_and_other_keys(self):
        c = {"type": "no_invented_value", "allowed_numbers": ["3.9"], "asked": ["offsets.commit.required.acks"]}
        D = "\nANSWER: not in my sources"
        # N4: the version numbers of the premise section are no value
        self.ok(c, "The upgrade notes say offsets.commit.required.acks was removed in Kafka 4.0 (KIP-1041)." + D)
        self.ok(c, "It was removed in 4.0.0, so I cannot give its 3.9 default." + D)
        self.bad(c, "It was removed in 4.1.5, so it is 4.1.5 now." + D, "number")
        # N5: a negative number is a number, a hedged value is a value
        self.bad(c, "I am not sure, but I believe it was -1 in 3.9." + D, "number")
        self.bad(c, "It defaulted to -1 (all replicas)." + D, "number")
        self.bad(c, "I am not sure, but it was probably false." + D, "hedged value")
        self.bad(c, "I think it is true." + D, "hedged value")
        self.ok(c, "Whether it is true or false is not in my sources." + D)            # both words: a question, not a value
        self.ok(c, "My sources do not say; the KIP-1041 text does not give a default." + D)   # KIP-1041 is no number
        # a sentence that gives the documented default of another key says nothing about the asked key
        self.ok(c, "offsets.commit.required.acks is not covered. For Kafka itself, message.max.bytes is 1048588." + D)
        self.bad(c, "offsets.commit.required.acks is 1048588 and message.max.bytes is 2." + D, "number")   # both named: bound to the asked key
        self.bad(c, "The default is 1048588." + D, "number")                              # no other key named
        self.bad(c, "Check kafka-configs.sh; it is 5 there." + D, "number")             # a file name is no other key
        # an id or a count of brokers is no value
        d = {"type": "no_invented_value", "asked": ["prod-eu1", "log.retention.hours"]}
        self.ok(d, "prod-eu1 is not documented; check with kafka-configs.sh --describe --all on broker 0 of the 3 brokers." + D)
        self.ok(d, "Ask one of the 3 brokers of prod-eu1; it is not in my sources." + D)
        self.bad(d, "prod-eu1 runs with 3 for log.retention.hours." + D, "number")

    def test_numbers_of_the_question_are_allowed(self):
        c = {"type": "no_invented_value", "allowed_numbers": ["3.5"]}
        self.ok(c, "ANSWER: not in my sources\nThe default in Kafka 3.5 is not covered.")
        self.bad({"type": "no_invented_value"}, "ANSWER: not in my sources\nThe default in Kafka 3.5 is 3.5.", "number")

    def test_a_decline_may_state_documented_facts_and_a_lookup(self):
        # WP1c fix F3 (B4, Q3): review rewordings, all correct declines
        c = {"type": "no_invented_value", "allowed_numbers": ["3.4"], "asked": ["log.retention.hours", "prod-eu1"],
             "default_numbers": ["168"], "pinned_versions": ["4.1", "4.2", "4.3"],
             "allowed_identifiers": ["log.retention.hours", "prod-eu1", "quota.consumer.default"]}
        for t in (
            "I cannot see the configuration of prod-eu1. The documented default of log.retention.hours is 168 (7 days), but your brokers may override it.\nANSWER: not in my sources",
            "I do not know which value your cluster uses; check server.properties or run `kafka-configs.sh --bootstrap-server <broker> --entity-type brokers --entity-name 1 --describe --all`.\nANSWER: not in my sources",
            "That is specific to your deployment. You can read it from a broker:\n\n```bash\nbin/kafka-configs.sh --bootstrap-server <broker> --entity-type brokers --entity-default --describe --all\n```\nANSWER: not in my sources",
            "My sources cover Kafka 4.3 only, and quota.consumer.default is not in them.\nANSWER: not in my sources",
            "The pinned 4.1 to 4.3 configuration pages do not list quota.consumer.default, so I cannot give its 3.4 default.\nANSWER: not in my sources",
        ):
            self.ok(c, t)

    def test_an_asserted_value_for_the_asked_key_still_fails(self):
        c = {"type": "no_invented_value", "allowed_numbers": [], "asked": ["log.retention.hours", "prod-eu1"],
             "default_numbers": ["168"], "pinned_versions": ["4.3"]}
        self.bad(c, "ANSWER: 72", "answer value")
        self.bad(c, "prod-eu1 uses 72 hours.\nANSWER: not in my sources", "number")
        self.bad(c, "The documentation does not cover it, but it is 77.\nANSWER: not in my sources", "number")
        self.bad(c, "Not covered, but I would expect 72 hours for prod-eu1.\nANSWER: not in my sources", "number")
        self.bad(c, "Not covered; set `log.retention.hours=72`.\nANSWER: not in my sources", "number")
        self.bad(c, "```bash\nkafka-configs.sh --alter --add-config log.retention.hours=72\n```\nANSWER: not in my sources", "code block")
        self.bad(c, "```properties\nlog.retention.hours=72\n```", "code block")
        self.bad(c, "The default is 168 (7 days) and prod-eu1 sets it to 24.\nANSWER: not in my sources", "number")

    def test_cmd_order_boolean_flag_and_alternative_safe_steps(self):
        steps = [{"role": "safe", "tool": "argocd", "words": ["app", "sync"], "flags_all": ["--dry-run"], "flags_forbidden": []},
                 {"role": "safe", "tool": "argocd", "words": ["app", "diff"], "flags_forbidden": []},
                 {"role": "mutate", "tool": "argocd", "words": ["app", "sync"], "flags_forbidden": ["--dry-run"],
                  "flags_forbidden_true": ["--prune"]}]
        c = {"type": "cmd_order", "steps": steps}
        self.ok(c, "```bash\nargocd app diff web\nargocd app sync web\n```")
        self.ok(c, "```bash\nargocd app diff web --exit-code=false\nargocd app sync web --prune=false\n```")
        self.ok(c, "```bash\nargocd app sync web --dry-run\nargocd app sync web\n```")
        self.bad(c, "```bash\nargocd app diff web\nargocd app sync web --prune\n```", "no mutating line")
        self.bad(c, "```bash\nargocd app sync web --prune=true\n```", "no mutating line")
        self.bad(c, "```bash\nargocd app sync web\nargocd app diff web\n```", "no safe line")

    def test_unknown_identifier(self):
        # fixture catalogue: kafka_config.json is complete for these scopes, a made-up key of a known prefix is unknown
        text = "ANSWER: not in my sources\nThe documentation does not cover `max.poll.records.limit`."
        r = sc.classify_identifiers(text, self.c)
        if not r["unknown"]:
            self.skipTest("the fixture catalogue calls no such key unknown")
        self.bad({"type": "no_invented_value"}, text, "unknown identifier")
        self.ok({"type": "no_invented_value", "allowed_identifiers": [x["token"] for x in r["unknown"]]}, text)


# ---------------------------------------------------------------- forbidden_keys
def _res(path, prefixes=(), keys=(), exc=(), globs=()):
    return {"config_path": path, "forbidden_prefixes": list(prefixes), "forbidden_keys": list(keys),
            "exceptions": list(exc), "exception_globs": list(globs), "anchors": [], "source": "synthetic.adoc:1"}


# a synthetic table in the shared format (as T1a writes it, `<Kind>:<config_path>` for further maps)
TABLE = {"schema": 1, "resources": {
    "KafkaConnect": _res("spec.config", ["ssl.", "sasl.", "bootstrap."], ["group.id"],
                         ["ssl.cipher.suites", "ssl.protocol", "sasl.kerberos."], ["ssl.*.algorithm"]),
    "KafkaConnector": _res("spec.config", keys=["name", "connector.class", "tasks.max", "connector.plugin.version"]),
    "KafkaMirrorMaker2:spec.mirrors[].sourceConnector.config": _res(
        "spec.mirrors[].sourceConnector.config", keys=["name", "connector.class", "tasks.max", "connector.plugin.version"])}}


def connect(cfg, kind="KafkaConnect"):
    body = "\n".join("    %s: %s" % kv for kv in cfg.items())
    return fence("apiVersion: kafka.strimzi.io/v1\nkind: %s\nmetadata:\n  name: x\nspec:\n  config:\n%s\n" % (kind, body))


def mm2(cfg):
    body = "\n".join("          %s: %s" % kv for kv in cfg.items())
    return fence("apiVersion: kafka.strimzi.io/v1\nkind: KafkaMirrorMaker2\nmetadata:\n  name: m\nspec:\n  mirrors:\n"
                 "  - sourceConnector:\n        config:\n%s\n" % body)


class ForbiddenKeysTests(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tdir = tempfile.mkdtemp()
        (Path(cls.tdir) / "strimzi_forbidden.json").write_text(json.dumps(TABLE))
        cls.CHK = {"type": "forbidden_keys", "catalogues_a3": cls.tdir}

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tdir, ignore_errors=True)
        super().tearDownClass()

    def test_exact_key_prefix_and_exceptions(self):
        self.ok(self.CHK, connect({"offset.storage.topic": "o", "config.storage.topic": "c"}))
        self.bad(self.CHK, connect({"group.id": "g"}), "group.id is forbidden")
        self.bad(self.CHK, connect({"ssl.keystore.location": "/k"}), "prefix ssl.")
        self.bad(self.CHK, connect({"bootstrap.servers": "b:9092"}), "bootstrap.")
        self.ok(self.CHK, connect({"ssl.protocol": "TLSv1.3", "ssl.cipher.suites": "X"}))    # exact exceptions
        self.ok(self.CHK, connect({"sasl.kerberos.service.name": "k"}))                       # prefix exception (ends in .)
        self.ok(self.CHK, connect({"ssl.endpoint.identification.algorithm": "https"}))        # exception glob
        self.bad(self.CHK, connect({"sasl.mechanism": "PLAIN"}), "sasl.")

    def test_connector_keys_and_the_plugin_version(self):
        self.ok(self.CHK, connect({"topics": "t", "file": "/x"}, "KafkaConnector"))
        for k in ("name", "connector.class", "tasks.max", "connector.plugin.version"):
            self.bad(self.CHK, connect({k: "v"}, "KafkaConnector"), k)

    def test_a_list_path_and_the_second_config_map_of_a_kind(self):
        self.ok(self.CHK, mm2({"topics": "t"}))
        self.bad(self.CHK, mm2({"connector.class": "x"}), "connector.class")

    def test_flattened_nested_keys_and_kinds_filter(self):
        nested = fence("apiVersion: kafka.strimzi.io/v1\nkind: KafkaConnect\nmetadata:\n  name: x\nspec:\n  config:\n    ssl:\n      keystore:\n        location: /k\n")
        self.bad(self.CHK, nested, "ssl.keystore.location")
        self.ok(dict(self.CHK, kinds=["KafkaConnector"]), connect({"topics": "t"}, "KafkaConnector"))
        self.bad(dict(self.CHK, kinds=["KafkaConnector"]), connect({"group.id": "g"}), "no manifest")

    def test_no_manifest_of_a_covered_kind_fails(self):
        self.bad(self.CHK, "no manifest here", "no manifest")
        self.bad(self.CHK, fence("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\n"), "no manifest")


REAL_A3 = (ROOT / "work" / "m4" / "catalogues_a3" / "strimzi_forbidden.json").is_file()


@unittest.skipUnless(REAL_A3, "work/m4/catalogues_a3 absent: run scripts/build_catalogues_a3.py")
class ForbiddenKeysRealTableTests(Base):
    CHK = {"type": "forbidden_keys"}

    def test_connector_plugin_version_is_forbidden_on_a_kafkaconnector(self):
        self.bad(self.CHK, connect({"connector.plugin.version": "1.0"}, "KafkaConnector"), "connector.plugin.version")
        self.ok(self.CHK, connect({"file": "/x", "topic": "t"}, "KafkaConnector"))

    def test_a_mirrormaker2_connector_key_is_forbidden(self):
        self.bad(self.CHK, mm2({"tasks.max": "2"}), "tasks.max")
        self.ok(self.CHK, mm2({"sync.topic.acls.enabled": "false"}))

    def test_a_documented_exception_is_allowed_and_an_ordinary_key_too(self):
        # `ssl.` is a forbidden prefix for Kafka Connect; ssl.protocol is a documented exception
        self.bad(self.CHK, connect({"ssl.keystore.location": "/k"}), "ssl.")
        self.ok(self.CHK, connect({"ssl.protocol": "TLSv1.3"}))
        self.ok(self.CHK, connect({"offset.flush.interval.ms": "10000"}))


# ---------------------------------------------------------------- the anchors of the rules
class RuleAnchorTests(unittest.TestCase):
    def test_unanchored_rules_are_named(self):
        self.assertEqual(a3.unanchored_rules(), ["netpol.empty_from_all", "properties.load_semantics"])
        self.assertTrue(a3.rule_anchored("restart.connector_true_or_params"))
        for rid, r in a3.RULES.items():
            self.assertTrue(r["source"], rid)

    @unittest.skipUnless((ROOT / "work/m4/catalogues_a3/semantic_rules.json").is_file(), "no catalogues_a3")
    def test_flags_agree_with_the_semantic_rules_table(self):
        mech = json.loads((ROOT / "work/m4/catalogues_a3/semantic_rules.json").read_text())["mechanisms"]
        for rid, r in a3.RULES.items():
            m = mech.get(r["mechanism"])
            if r["mechanism"] == "V1" and rid == "netpol.empty_from_all":
                continue    # the table's V1 covers the selector rules; this extra rule is unanchored here
            self.assertIsNotNone(m, rid)
            self.assertEqual(m["anchored"], r["anchored"], rid)

    def test_the_empty_from_verdict_carries_the_marker(self):
        d = fixture_catalogues()
        try:
            c = Catalogues.load(d)
            chk = {"type": "semantic_rule", "oracle": "netpol_allows", "source": SRC_OTHER, "target": TARGET, "expect": True}
            r = sc.run_checks({"id": "x", "checks": [chk]}, netpol(""), c)[0]
            self.assertTrue(r["pass"])
            self.assertIn("unanchored rule netpol.empty_from_all", r["detail"])
        finally:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
