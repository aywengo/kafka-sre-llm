"""Client-code checks of the a3 slice (WP1c T2): hand-written passing and failing Java and Python answers.

Java tests need javac, java and the two pinned jars (raw_catalogue/maven/, scripts/fetch_maven_jars.py) and are
skipped without them; the Java config tests also need java_api.json. Python tests run on a small synthetic symbol
table everywhere, and on the real python_symbols.json (the pinned confluent-kafka wheel) where it is built.
Nothing in an answer is ever executed: the tests also show that a program with a side effect only compiles."""
import json
import os
import shutil
import stat
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_checks_a3_client as cc  # noqa: E402
from kafka_sre_rag import catalogue_a3 as a3  # noqa: E402

REAL = ROOT / "work" / "m4" / "catalogues_a3"
HAVE_JAVA = bool(shutil.which("javac") and shutil.which("java") and cc.jar_paths()[0])
HAVE_API = (REAL / "java_api.json").is_file()
HAVE_PYSYM = (REAL / "python_symbols.json").is_file()


def jb(code):
    return "Here is the program.\n\n```java\n%s\n```\n" % code.strip("\n")


def pb(code):
    return "Here is the program.\n\n```python\n%s\n```\n" % code.strip("\n")


CONSUMER = """
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ManualCommit {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("orders"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                records.forEach(r -> System.out.println(r.value()));
                consumer.commitSync();
            }
        }
    }
}
"""
KC = "org.apache.kafka.clients.consumer.KafkaConsumer"
POLL = {"class": KC, "method": "poll", "params": ["java.time.Duration"]}
COMMIT = {"class": KC, "method": "commitSync", "params": []}


@unittest.skipUnless(HAVE_JAVA, "javac, java or the pinned jars are missing")
class JavaCompilesTests(unittest.TestCase):
    def test_passing_program_compiles(self):
        ok, d = cc.check_java_compiles({}, jb(CONSUMER), {})
        self.assertTrue(ok, d)

    def test_wrong_signature_does_not_compile(self):
        ok, d = cc.check_java_compiles({}, jb(CONSUMER.replace("Duration.ofMillis(200)", "200")), {})
        self.assertFalse(ok)
        self.assertTrue(d.startswith("java_compile_error"), d)

    def test_import_of_a_name_that_does_not_exist(self):
        ok, d = cc.check_java_compiles({}, jb(CONSUMER.replace("clients.consumer.KafkaConsumer;", "clients.consumer.KafkaConsumr;")), {})
        self.assertFalse(ok)
        self.assertTrue(d.startswith("java_compile_error"), d)

    def test_invented_config_constant_does_not_compile(self):
        ok, d = cc.check_java_compiles({}, jb(CONSUMER.replace("ENABLE_AUTO_COMMIT_CONFIG", "ENABLE_AUTO_COMMITS_CONFIG")), {})
        self.assertFalse(ok)
        self.assertIn("ENABLE_AUTO_COMMITS_CONFIG", d)

    def test_no_block_and_no_class(self):
        self.assertTrue(cc.check_java_compiles({}, "no code at all", {})[1].startswith("java_no_block"))
        ok, d = cc.check_java_compiles({}, jb("int x = 1;"), {})
        self.assertFalse(ok)
        self.assertTrue(d.startswith("java_no_class"), d)

    def test_streams_snippet_compiles_against_the_streams_jar(self):
        code = """
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import java.util.Properties;
public class Pipe {
    public static void main(String[] a) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "pipe");
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        StreamsBuilder b = new StreamsBuilder();
        b.stream("in").to("out");
        new KafkaStreams(b.build(), p).start();
    }
}
"""
        ok, d = cc.check_java_compiles({}, jb(code), {})
        self.assertTrue(ok, d)

    def test_rocksdb_failure_is_a_template_problem_with_its_own_reason(self):
        code = """
import org.apache.kafka.streams.state.RocksDBConfigSetter;
public class A { void g(RocksDBConfigSetter s) { s.close("x", null); } }
"""
        ok, d = cc.check_java_compiles({}, jb(code), {})
        self.assertFalse(ok)
        self.assertTrue(d.startswith("java_template_rocksdb"), d)

    def test_nothing_is_run(self):
        # a program whose main would create a file: it must compile and leave no file behind
        with tempfile.TemporaryDirectory() as t:
            marker = Path(t) / "ran"
            code = ('public class Boom { static { new java.io.File("%s").mkdir(); }\n'
                    '  public static void main(String[] a) throws Exception { new java.io.File("%s").mkdir(); } }'
                    % (marker, marker))
            self.assertTrue(cc.check_java_compiles({}, jb(code), {})[0])
            self.assertTrue(cc.check_java_calls({"required": []}, jb(code), {})[0])
            self.assertFalse(marker.exists())

    def test_annotation_processing_is_off(self):
        cmd = cc.javac_command(["a.jar"], "out", ["A.java"])
        self.assertIn("-proc:none", cmd)
        self.assertEqual(cmd[cmd.index("-cp") + 1], "a.jar")


class JavaTimeoutAndEnvironmentTests(unittest.TestCase):
    """The timeout and the unavailable reasons, with a stand-in javac; run wherever the jars are present."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old = (cc.JAVAC, dict(cc._jar_state))

    def tearDown(self):
        cc.JAVAC, state = self.old
        cc._jar_state.clear()
        cc._jar_state.update(state)
        self.tmp.cleanup()

    def fake_javac(self, body):
        p = Path(self.tmp.name) / "fakejavac"
        p.write_text("#!/bin/sh\n" + body + "\n")
        p.chmod(p.stat().st_mode | stat.S_IEXEC)
        cc.JAVAC = str(p)

    def with_jars(self):
        cc._jar_state["r"] = (["a.jar", "b.jar"], None)

    def test_timeout_returns_a_failure_with_its_own_reason_and_kills_the_child(self):
        self.with_jars()
        self.fake_javac("sleep 30")
        t = time.time()
        ok, d = cc.check_java_compiles({"timeout": 0.5}, jb("public class A {}"), {})
        self.assertFalse(ok)
        self.assertTrue(d.startswith("java_timeout"), d)
        self.assertLess(time.time() - t, 10)

    def test_rocksdb_message_has_its_own_reason(self):
        self.with_jars()
        self.fake_javac('echo "A.java:2: error: cannot access Options" >&2; echo "  class file for org.rocksdb.Options not found" >&2; exit 1')
        ok, d = cc.check_java_compiles({}, jb("public class A {}"), {})
        self.assertFalse(ok)
        self.assertTrue(d.startswith("java_template_rocksdb"), d)

    def test_other_errors_are_compile_errors(self):
        self.with_jars()
        self.fake_javac('echo "A.java:2: error: cannot find symbol" >&2; exit 1')
        ok, d = cc.check_java_compiles({}, jb("public class A {}"), {})
        self.assertTrue(d.startswith("java_compile_error"), d)

    def test_missing_javac_and_missing_jars_are_unavailable_not_answer_failures(self):
        self.with_jars()
        cc.JAVAC = str(Path(self.tmp.name) / "nope")
        self.assertTrue(cc.check_java_compiles({}, jb("public class A {}"), {})[1].startswith("java_unavailable"))
        cc._jar_state["r"] = (None, "java_unavailable: no jars")
        self.assertTrue(cc.check_java_compiles({}, jb("public class A {}"), {})[1].startswith("java_unavailable"))

    def test_the_command_has_proc_none_a_temp_folder_and_no_network_flag_needed(self):
        self.with_jars()
        self.fake_javac('for a in "$@"; do echo "$a"; done > "$0.args"; exit 0')
        self.assertTrue(cc.check_java_compiles({}, jb("public class A {}"), {})[0])
        args = (Path(self.tmp.name) / "fakejavac.args").read_text().split("\n")
        self.assertIn("-proc:none", args)
        self.assertEqual(args[args.index("-cp") + 1], "a.jar" + os.pathsep + "b.jar")
        self.assertIn("-sourcepath", args)
        d = args[args.index("-d") + 1]
        self.assertTrue(d.rstrip("/").split("/")[-2].startswith("a3java_"), d)
        self.assertFalse(Path(d).exists())                      # the temporary folder is gone


@unittest.skipUnless(HAVE_JAVA, "javac, java or the pinned jars are missing")
class JavaCallsTests(unittest.TestCase):
    def test_required_calls_resolved(self):
        ok, d = cc.check_java_calls({"required": [POLL, COMMIT]}, jb(CONSUMER), {})
        self.assertTrue(ok, d)

    def test_constructor_and_receiver_type_match(self):
        ok, d = cc.check_java_calls({"required": [{"class": KC, "method": "<init>"},
                                                  {"class": "java.util.Properties", "method": "put"}]}, jb(CONSUMER), {})
        self.assertTrue(ok, d)

    def test_missing_call_fails(self):
        ok, d = cc.check_java_calls({"required": [POLL, COMMIT]}, jb(CONSUMER.replace("consumer.commitSync();", "")), {})
        self.assertFalse(ok)
        self.assertIn("commitSync", d)

    def test_wrong_signature_fails(self):
        code = CONSUMER.replace("consumer.commitSync();", "consumer.commitSync(Duration.ofSeconds(5));")
        ok, d = cc.check_java_calls({"required": [POLL, COMMIT]}, jb(code), {})
        self.assertFalse(ok)
        self.assertIn("commitSync()", d)
        ok, d = cc.check_java_calls({"required": [POLL, {"class": KC, "method": "commitSync", "params": ["java.time.Duration"]}]},
                                    jb(code), {})
        self.assertTrue(ok, d)

    def test_unresolved_call_matches_nothing(self):
        ok, d = cc.check_java_calls({"required": [POLL]}, jb(CONSUMER.replace("Duration.ofMillis(200)", "200")), {})
        self.assertFalse(ok)
        self.assertIn("could not resolve", d)

    def test_forbidden_call_present_fails(self):
        ok, d = cc.check_java_calls({"required": [POLL], "forbidden": [COMMIT]}, jb(CONSUMER), {})
        self.assertFalse(ok)
        self.assertIn("forbidden", d)
        ok, d = cc.check_java_calls({"required": [POLL], "forbidden": [{"class": KC, "method": "commitAsync"}]}, jb(CONSUMER), {})
        self.assertTrue(ok, d)

    def test_generic_parameter_types_are_erased(self):
        code = CONSUMER.replace("consumer.commitSync();", "consumer.commitSync(java.util.Map.of());")
        e = {"class": KC, "method": "commitSync",
             "params": ["java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>"]}
        ok, d = cc.check_java_calls({"required": [e]}, jb(code), {})
        self.assertTrue(ok, d)

    # ---- T2b: calls through the interface or superclass a required class implements
    @unittest.skipUnless(HAVE_API, "work/m4/catalogues_a3/java_api.json not built: run scripts/build_catalogues_a3.py")
    def test_interface_typed_consumer_passes(self):
        code = CONSUMER.replace("KafkaConsumer<String, String> consumer", "Consumer<String, String> consumer") \
                       .replace("import org.apache.kafka.clients.consumer.KafkaConsumer;",
                                "import org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.clients.consumer.Consumer;")
        ok, d = cc.check_java_calls({"required": [POLL, COMMIT]}, jb(code), {})
        self.assertTrue(ok, d)
        ok, d = cc.check_java_call_order({"order": [{"class": KC, "method": "subscribe"}, POLL, COMMIT]}, jb(code), {})
        self.assertTrue(ok, d)
        self.assertFalse(cc.check_java_calls({"required": [POLL, COMMIT]}, jb(code.replace("consumer.commitSync();", "")), {})[0])
        # a wrong signature through the interface still fails
        bad = code.replace("consumer.commitSync();", "consumer.commitSync(Duration.ofSeconds(5));")
        self.assertFalse(cc.check_java_calls({"required": [POLL, COMMIT]}, jb(bad), {})[0])

    @unittest.skipUnless(HAVE_API, "work/m4/catalogues_a3/java_api.json not built: run scripts/build_catalogues_a3.py")
    def test_interface_typed_producer_and_admin_pass(self):
        prod = """
import org.apache.kafka.clients.producer.*;
import java.util.Properties;
public class P { public static void main(String[] a) {
    Properties props = new Properties();
    Producer<String, String> p = new KafkaProducer<>(props);
    p.send(new ProducerRecord<>("t", "v"));
    p.flush();
} }"""
        ok, d = cc.check_java_calls({"required": [
            {"class": "org.apache.kafka.clients.producer.KafkaProducer", "method": "send",
             "params": ["org.apache.kafka.clients.producer.ProducerRecord"]},
            {"class": "org.apache.kafka.clients.producer.KafkaProducer", "method": "flush"}]}, jb(prod), {})
        self.assertTrue(ok, d)
        adm = """
import org.apache.kafka.clients.admin.*;
import java.util.*;
public class A { public static void main(String[] a) throws Exception {
    Properties props = new Properties();
    Admin admin = Admin.create(props);
    admin.createTopics(List.of(new NewTopic("t", 3, (short) 2)));
} }"""
        for cls in ("org.apache.kafka.clients.admin.Admin", "org.apache.kafka.clients.admin.AdminClient",
                    "org.apache.kafka.clients.admin.KafkaAdminClient"):
            ok, d = cc.check_java_calls({"required": [{"class": cls, "method": "createTopics",
                                                       "params": ["java.util.Collection"]}]}, jb(adm), {})
            self.assertTrue(ok, "%s: %s" % (cls, d))

    def test_unrelated_type_with_same_method_name_fails(self):
        code = """
import java.time.Duration;
public class U {
    static class Fake { Object poll(Duration d) { return null; } void commitSync() {} }
    public static void main(String[] a) { Fake c = new Fake(); c.poll(Duration.ofMillis(1)); c.commitSync(); }
}"""
        ok, d = cc.check_java_calls({"required": [POLL]}, jb(code), {})
        self.assertFalse(ok)
        self.assertIn("poll", d)
        self.assertFalse(cc.check_java_call_order({"order": [POLL, COMMIT]}, jb(code), {})[0])
        # the other direction: a required interface is not satisfied by a call on its subtype's own method
        self.assertFalse(cc.check_java_calls({"required": [{"class": "org.apache.kafka.clients.consumer.Consumer",
                                                            "method": "nosuch"}]}, jb(CONSUMER), {})[0])

    @unittest.skipUnless(HAVE_API, "java_api.json is not built")
    def test_supertypes_in_the_catalogue(self):
        self.assertTrue(a3.java_is_subtype(KC, "org.apache.kafka.clients.consumer.Consumer"))
        self.assertTrue(a3.java_is_subtype("org.apache.kafka.clients.admin.KafkaAdminClient", "org.apache.kafka.clients.admin.Admin"))
        self.assertFalse(a3.java_is_subtype("org.apache.kafka.clients.consumer.Consumer", KC))
        self.assertFalse(a3.java_is_subtype(KC, "org.apache.kafka.clients.producer.Producer"))


class EntrySignatureTests(unittest.TestCase):
    """WP1c fix round, B1: an entry accepts any of several overloads ("signatures"); "params" still works alone."""

    def test_entry_signatures(self):
        self.assertIsNone(cc.entry_signatures({"class": KC, "method": "poll"}))
        self.assertEqual(cc.entry_signatures({"params": []}), [[]])
        self.assertEqual(cc.entry_signatures({"signatures": [[], ["java.time.Duration"]]}), [[], ["java.time.Duration"]])
        self.assertEqual(cc.entry_signatures({"params": [], "signatures": [["java.time.Duration"]]}), [[], ["java.time.Duration"]])

    def test_match_on_any_signature_only(self):
        call = {"owner": KC, "receiver": KC, "name": "commitSync", "params": ["java.time.Duration"]}
        self.assertTrue(cc._entry_matches({"class": KC, "method": "commitSync", "signatures": [[], ["java.time.Duration"]]}, call))
        self.assertFalse(cc._entry_matches({"class": KC, "method": "commitSync", "signatures": [[], ["java.util.Map"]]}, call))
        self.assertTrue(cc._entry_matches({"class": KC, "method": "commitSync", "signatures": []}, dict(call, params=["x"])))  # no list: any
        self.assertTrue(cc._entry_matches({"class": KC, "method": "commitSync", "params": ["java.time.Duration"]}, call))

    def test_description_lists_the_alternatives(self):
        d = cc._describe({"class": KC, "method": "commitSync", "signatures": [[], ["java.time.Duration"]]})
        self.assertEqual(d, "KafkaConsumer.commitSync( | Duration)")


@unittest.skipUnless(HAVE_JAVA, "javac, java or the pinned jars are missing")
class JavaOverloadAndCloseTests(unittest.TestCase):
    SIGS = {"class": KC, "method": "commitSync", "signatures": [[], ["java.time.Duration"],
                                                                   ["java.util.Map"], ["java.util.Map", "java.time.Duration"]]}

    def test_every_listed_overload_passes(self):
        for call in ("consumer.commitSync();", "consumer.commitSync(Duration.ofSeconds(5));", "consumer.commitSync(java.util.Map.of());",
                     "consumer.commitSync(java.util.Map.of(), Duration.ofSeconds(5));"):
            ok, d = cc.check_java_calls({"required": [POLL, self.SIGS]}, jb(CONSUMER.replace("consumer.commitSync();", call)), {})
            self.assertTrue(ok, (call, d))

    def test_an_overload_not_listed_or_not_in_the_api_fails(self):
        ok, d = cc.check_java_calls({"required": [POLL, dict(self.SIGS, signatures=[[], ["java.util.Map"]])]},
                                    jb(CONSUMER.replace("consumer.commitSync();", "consumer.commitSync(Duration.ofSeconds(5));")), {})
        self.assertFalse(ok)
        ok, d = cc.check_java_calls({"required": [POLL, self.SIGS]}, jb(CONSUMER.replace("Duration.ofMillis(200)", "200")), {})
        self.assertFalse(ok)                                  # poll(long) is not in the 4.3.2 API
        self.assertIn("could not resolve", d)

    def test_try_with_resources_is_a_close_call(self):
        close = {"class": KC, "method": "close", "params": []}
        ok, d = cc.check_java_calls({"required": [POLL, close]}, jb(CONSUMER), {})
        self.assertTrue(ok, d)
        info, why = cc.list_calls(jb(CONSUMER))
        self.assertIsNone(why)
        implicit = [c for c in info["calls"] if c.get("implicit")]
        self.assertEqual([(c["name"], c["owner"], c["params"]) for c in implicit], [("close", KC, [])])
        no_close = CONSUMER.replace("try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {",
                                    "{ KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);").replace(
                                        "while (true) {", "for (int i = 0; i < 3; i++) {")
        ok, d = cc.check_java_calls({"required": [POLL, close]}, jb(no_close), {})
        self.assertFalse(ok)
        self.assertIn("close", d)
        ok, d = cc.check_java_calls({"required": [POLL, close]}, jb(no_close.replace("consumer.commitSync();", "consumer.commitSync();\n consumer.close();")), {})
        self.assertTrue(ok, d)

    def test_the_implicit_close_comes_after_the_body_and_before_the_catch_and_finally(self):
        code = """
import org.apache.kafka.clients.admin.Admin;
import java.util.Properties;
public class A {
    public static void main(String[] args) throws Exception {
        try (Admin admin = Admin.create(new Properties())) {
            admin.listTopics();
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
        } finally {
            System.out.println("done");
        }
    }
}
"""
        info, why = cc.list_calls(jb(code))
        self.assertIsNone(why)
        names = [(c["name"], bool(c.get("implicit"))) for c in info["calls"] if c["name"] in ("create", "listTopics", "close", "getMessage", "println")]
        self.assertEqual(names, [("create", False), ("listTopics", False), ("close", True), ("getMessage", False), ("println", False),
                                 ("println", False)])
        self.assertLess([n for n, _ in names].index("listTopics"), [n for n, _ in names].index("close"))
        self.assertLess([n for n, _ in names].index("close"), [n for n, _ in names].index("getMessage"))
        ok, d = cc.check_java_call_order({"order": [{"class": "org.apache.kafka.clients.admin.Admin", "method": "listTopics"},
                                                    {"class": "org.apache.kafka.clients.admin.Admin", "method": "close"}]}, jb(code), {})
        self.assertTrue(ok, d)

    @unittest.skipUnless(HAVE_API, "work/m4/catalogues_a3/java_api.json not built: run scripts/build_catalogues_a3.py")
    def test_two_resources_close_in_reverse_order_and_an_interface_typed_resource_matches(self):
        code = """
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Properties;
public class A {
    public static void main(String[] args) {
        Properties p = new Properties();
        try (Admin admin = Admin.create(p); Consumer<String, String> consumer = new KafkaConsumer<>(p)) {
            System.out.println(admin);
        }
    }
}
"""
        info, why = cc.list_calls(jb(code))
        self.assertIsNone(why)
        implicit = [c for c in info["calls"] if c.get("implicit")]
        self.assertEqual([c["receiver"].rsplit(".", 1)[-1] for c in implicit], ["Consumer", "Admin"])
        self.assertTrue(cc.check_java_calls({"required": [{"class": KC, "method": "close", "params": []}]}, jb(code), {})[0])

    def test_a_resource_given_as_a_variable_is_closed_too(self):
        code = """
import org.apache.kafka.clients.admin.Admin;
import java.util.Properties;
public class A {
    public static void main(String[] args) {
        Admin admin = Admin.create(new Properties());
        try (admin) {
            System.out.println(admin);
        }
    }
}
"""
        info, why = cc.list_calls(jb(code))
        self.assertIsNone(why)
        self.assertEqual([c["name"] for c in info["calls"] if c.get("implicit")], ["close"])


@unittest.skipUnless(HAVE_JAVA, "javac, java or the pinned jars are missing")
class JavaCallOrderTests(unittest.TestCase):
    def test_poll_before_commit_passes(self):
        ok, d = cc.check_java_call_order({"order": [{"method": "subscribe"}, POLL, COMMIT]}, jb(CONSUMER), {})
        self.assertTrue(ok, d)

    def test_commit_before_poll_fails(self):
        code = CONSUMER.replace("""                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                records.forEach(r -> System.out.println(r.value()));
                consumer.commitSync();""", """                consumer.commitSync();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                records.forEach(r -> System.out.println(r.value()));""")
        ok, d = cc.check_java_call_order({"order": [POLL, COMMIT]}, jb(code), {})
        self.assertFalse(ok)
        self.assertIn("not after", d)

    def test_missing_step_fails(self):
        ok, d = cc.check_java_call_order({"order": [POLL, COMMIT]}, jb(CONSUMER.replace("consumer.commitSync();", "")), {})
        self.assertFalse(ok)
        self.assertIn("never called", d)

    def test_argument_calls_run_before_the_call(self):
        code = CONSUMER.replace("consumer.poll(Duration.ofMillis(200))", "consumer.poll(Duration.ofMillis(200))")
        ok, d = cc.check_java_call_order({"order": [{"class": "java.time.Duration", "method": "ofMillis"}, POLL]}, jb(code), {})
        self.assertTrue(ok, d)


@unittest.skipUnless(HAVE_JAVA and HAVE_API, "javac, jars or java_api.json missing")
class JavaConfigTests(unittest.TestCase):
    SPEC = {"keys": {"enable.auto.commit": "false", "group.id": "orders"}, "forbidden": {"enable.auto.commit": "true"},
            "scope": ["consumer"]}

    def test_passing(self):
        ok, d = cc.check_java_config(self.SPEC, jb(CONSUMER), {})
        self.assertTrue(ok, d)

    def test_constants_and_string_keys_are_resolved_alike(self):
        code = CONSUMER.replace("ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG", '"enable.auto.commit"')
        self.assertTrue(cc.check_java_config(self.SPEC, jb(code), {})[0])

    def test_wrong_value_and_forbidden_value_fail(self):
        code = CONSUMER.replace('ENABLE_AUTO_COMMIT_CONFIG, "false"', 'ENABLE_AUTO_COMMIT_CONFIG, "true"')
        ok, d = cc.check_java_config(self.SPEC, jb(code), {})
        self.assertFalse(ok)
        self.assertIn("enable.auto.commit", d)

    def test_missing_key_fails(self):
        ok, d = cc.check_java_config(self.SPEC, jb(CONSUMER.replace('ConsumerConfig.GROUP_ID_CONFIG, "orders"', 'ConsumerConfig.CLIENT_ID_CONFIG, "x"')), {})
        self.assertFalse(ok)
        self.assertIn("group.id is not set", d)

    def test_invented_string_key_fails(self):
        code = CONSUMER.replace("ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG", '"enable.auto.commits"')
        ok, d = cc.check_java_config({"scope": ["consumer"]}, jb(code), {})
        self.assertFalse(ok)
        self.assertIn("unknown config key 'enable.auto.commits'", d)

    def test_invented_config_constant_fails(self):
        code = CONSUMER.replace("ENABLE_AUTO_COMMIT_CONFIG", "ENABLE_AUTO_COMMITS_CONFIG")
        ok, d = cc.check_java_config({"scope": ["consumer"]}, jb(code), {})
        self.assertFalse(ok)
        self.assertIn("invented config constant ENABLE_AUTO_COMMITS_CONFIG", d)

    def test_map_of_and_topic_names(self):
        code = """
import org.apache.kafka.clients.admin.NewTopic;
import java.util.Map;
public class T { NewTopic t = new NewTopic("a", 3, (short) 2).configs(Map.of("min.insync.replicas", "2", "retention.ms", "1000")); }
"""
        ok, d = cc.check_java_config({"keys": {"min.insync.replicas": "2"}, "scope": ["topic"]}, jb(code), {})
        self.assertTrue(ok, d)
        ok, d = cc.check_java_config({"scope": ["topic"]}, jb(code.replace("retention.ms", "retention.msec")), {})
        self.assertFalse(ok)
        self.assertIn("retention.msec", d)

    def test_streams_prefixes_are_stripped(self):
        code = """
import org.apache.kafka.streams.StreamsConfig;
import java.util.Properties;
public class S { void f() { Properties p = new Properties();
  p.put(StreamsConfig.APPLICATION_ID_CONFIG, "x");
  p.put("consumer.max.poll.records", "10");
  p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2); } }
"""
        ok, d = cc.check_java_config({"keys": {"processing.guarantee": "exactly_once_v2"}, "scope": ["streams"]}, jb(code), {})
        self.assertTrue(ok, d)


FIX = {"schema": 1, "version": "9.9.9", "modules": {
    "confluent_kafka": {"names": ["Consumer", "KafkaError", "Producer", "version"]},
    "confluent_kafka.admin": {"names": ["AdminClient", "NewTopic"]}},
    "classes": {
        "confluent_kafka.Consumer": {"members": ["close", "commit", "poll", "subscribe"], "methods": {
            "poll": {"params": ["timeout"], "pos_only": [], "var_pos": False, "var_kw": False},
            "commit": {"params": ["asynchronous", "message", "offsets"], "pos_only": [], "var_pos": False, "var_kw": False},
            "subscribe": {"params": ["topics", "on_assign"], "pos_only": [], "var_pos": False, "var_kw": False},
            "close": {"params": [], "pos_only": [], "var_pos": False, "var_kw": False}}},
        "confluent_kafka.Producer": {"members": ["flush", "produce"], "methods": {
            "produce": {"params": ["topic", "value", "key", "callback"], "pos_only": [], "var_pos": False, "var_kw": False},
            "flush": {"params": ["timeout"], "pos_only": [], "var_pos": False, "var_kw": False}}}},
    "config_properties": ["bootstrap.servers", "enable.auto.commit", "group.id", "acks", "error_cb"]}

PY_OK = """
from confluent_kafka import Consumer, KafkaError

conf = {"bootstrap.servers": "localhost:9092", "group.id": "orders", "enable.auto.commit": False}
consumer = Consumer(conf)
consumer.subscribe(["orders"])
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()
"""


class PythonFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        (Path(cls.tmp.name) / "python_symbols.json").write_text(json.dumps(FIX))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def chk(self, fn, spec, code):
        return fn(dict(spec, catalogues_a3=self.tmp.name), pb(code), {})

    # py_syntax
    def test_syntax_passes_and_fails(self):
        self.assertTrue(cc.check_py_syntax({}, pb(PY_OK), {})[0])
        ok, d = cc.check_py_syntax({}, pb("def f(:\n  pass"), {})
        self.assertFalse(ok)
        self.assertTrue(d.startswith("py_syntax_error"), d)
        self.assertTrue(cc.check_py_syntax({}, "no code", {})[1].startswith("py_no_block"))

    def test_python_is_parsed_not_run(self):
        with tempfile.TemporaryDirectory() as t:
            marker = Path(t) / "ran"
            code = "open(%r, 'w').write('x')\nraise SystemExit(3)\n" % str(marker)
            self.assertTrue(cc.check_py_syntax({}, pb(code), {})[0])
            self.assertTrue(self.chk(cc.check_py_calls, {"required": []}, code)[0])
            self.assertFalse(marker.exists())

    # py_imports
    def test_imports_pass(self):
        ok, d = self.chk(cc.check_py_imports, {"require": ["Consumer"]}, PY_OK)
        self.assertTrue(ok, d)

    def test_import_of_a_name_that_does_not_exist_fails(self):
        ok, d = self.chk(cc.check_py_imports, {}, PY_OK.replace("import Consumer, KafkaError", "import Consumr, KafkaError"))
        self.assertFalse(ok)
        self.assertIn("no name Consumr", d)

    def test_import_from_a_module_that_does_not_exist_fails(self):
        ok, d = self.chk(cc.check_py_imports, {}, "from confluent_kafka.streams import Foo\n")
        self.assertFalse(ok)
        self.assertIn("no module confluent_kafka.streams", d)

    def test_attribute_on_the_module_alias_is_checked(self):
        ok, d = self.chk(cc.check_py_imports, {}, "import confluent_kafka as ck\nc = ck.Consumr({})\n")
        self.assertFalse(ok)
        self.assertIn("Consumr", d)
        self.assertTrue(self.chk(cc.check_py_imports, {}, "import confluent_kafka as ck\nc = ck.Consumer({})\n")[0])

    def test_submodule_import_and_required_name(self):
        code = "from confluent_kafka.admin import AdminClient, NewTopic\n"
        self.assertTrue(self.chk(cc.check_py_imports, {"require": ["AdminClient"]}, code)[0])
        ok, d = self.chk(cc.check_py_imports, {"require": ["Consumer"]}, code)
        self.assertFalse(ok)
        self.assertIn("Consumer is not imported", d)

    def test_no_confluent_import_fails(self):
        ok, d = self.chk(cc.check_py_imports, {}, "from kafka import KafkaConsumer\n")
        self.assertFalse(ok)
        self.assertIn("no import of confluent_kafka", d)

    # py_calls
    def test_calls_pass_with_order(self):
        spec = {"required": [{"name": "poll", "class": "Consumer", "kwargs": ["timeout"]},
                             {"name": "commit", "class": "Consumer", "kwargs": ["asynchronous"]}],
                "order": [{"name": "subscribe"}, {"name": "poll"}, {"name": "commit"}, {"name": "close"}]}
        ok, d = self.chk(cc.check_py_calls, spec, PY_OK)
        self.assertTrue(ok, d)

    def test_missing_call_fails(self):
        ok, d = self.chk(cc.check_py_calls, {"required": [{"name": "commit", "class": "Consumer"}]},
                         PY_OK.replace("consumer.commit(message=msg, asynchronous=False)", "pass"))
        self.assertFalse(ok)
        self.assertIn("missing call Consumer.commit", d)

    def test_wrong_order_fails(self):
        code = PY_OK.replace("consumer.subscribe([\"orders\"])", "consumer.poll(1.0)\nconsumer.subscribe([\"orders\"])")
        code = code.replace("        msg = consumer.poll(timeout=1.0)\n", "        msg = {}\n")
        ok, d = self.chk(cc.check_py_calls, {"order": [{"name": "subscribe"}, {"name": "poll"}]}, code)
        self.assertFalse(ok)
        self.assertIn("not after", d)

    def test_invented_method_on_a_known_class_fails(self):
        ok, d = self.chk(cc.check_py_calls, {}, PY_OK.replace("consumer.close()", "consumer.shutdown()"))
        self.assertFalse(ok)
        self.assertIn("Consumer has no method shutdown", d)

    def test_wrong_signature_unknown_keyword_and_too_many_arguments_fail(self):
        ok, d = self.chk(cc.check_py_calls, {}, PY_OK.replace("poll(timeout=1.0)", "poll(wait=1.0)"))
        self.assertFalse(ok)
        self.assertIn("no parameter wait", d)
        ok, d = self.chk(cc.check_py_calls, {}, PY_OK.replace("poll(timeout=1.0)", "poll(1.0, 2.0)"))
        self.assertFalse(ok)
        self.assertIn("at most 1 positional", d)

    def test_forbidden_call_fails(self):
        ok, d = self.chk(cc.check_py_calls, {"forbidden": [{"name": "close"}]}, PY_OK)
        self.assertFalse(ok)
        self.assertIn("forbidden call close", d)

    def test_unknown_receiver_matches_but_is_not_checked(self):
        code = "def run(c):\n    c.poll(timeout=1)\n    c.anything()\n"
        self.assertTrue(self.chk(cc.check_py_calls, {"required": [{"name": "poll", "class": "Consumer"}]}, code)[0])

    def test_producer_class_is_distinguished(self):
        code = "from confluent_kafka import Producer\np = Producer({})\np.poll(1)\n"
        ok, d = self.chk(cc.check_py_calls, {}, code)
        self.assertFalse(ok)
        self.assertIn("Producer has no method poll", d)

    # py_config
    def test_config_passes(self):
        ok, d = self.chk(cc.check_py_config, {"keys": {"enable.auto.commit": "false", "group.id": None},
                                              "forbidden": {"enable.auto.commit": "true"}}, PY_OK)
        self.assertTrue(ok, d)

    def test_invented_config_key_fails(self):
        ok, d = self.chk(cc.check_py_config, {}, PY_OK.replace("enable.auto.commit", "enable.auto.commits"))
        self.assertFalse(ok)
        self.assertIn("unknown config key 'enable.auto.commits'", d)

    def test_wrong_value_missing_key_and_forbidden_value_fail(self):
        ok, d = self.chk(cc.check_py_config, {"keys": {"enable.auto.commit": "false"}}, PY_OK.replace("False", "True"))
        self.assertFalse(ok)
        self.assertIn("wanted false", d)
        ok, d = self.chk(cc.check_py_config, {"keys": {"acks": None}}, PY_OK)
        self.assertFalse(ok)
        self.assertIn("acks is not set", d)
        ok, d = self.chk(cc.check_py_config, {"forbidden": {"enable.auto.commit": "false"}}, PY_OK)
        self.assertFalse(ok)

    def test_config_built_with_subscript_assignment_and_inline_dict(self):
        code = ("from confluent_kafka import Producer\nconf = {'bootstrap.servers': 'x'}\nconf['acks'] = 'all'\n"
                "p = Producer(conf)\nq = Producer({'bootstrap.servers': 'y', 'acksx': '1'})\n")
        ok, d = self.chk(cc.check_py_config, {"keys": {"acks": "all"}}, code)
        self.assertFalse(ok)
        self.assertEqual(d, "unknown config key 'acksx'")
        self.assertTrue(self.chk(cc.check_py_config, {"keys": {"acks": "all"}}, code.replace("'acksx'", "'acks'"))[0])

    def test_no_config_dict_fails(self):
        ok, d = self.chk(cc.check_py_config, {}, "from confluent_kafka import Producer\np = Producer(get_conf())\n")
        self.assertFalse(ok)
        self.assertTrue(d.startswith("py_no_config"), d)

    def test_without_a_property_table_only_names_are_checked_and_it_says_so(self):
        with tempfile.TemporaryDirectory() as t:
            fx = dict(FIX, config_properties=[])
            (Path(t) / "python_symbols.json").write_text(json.dumps(fx))
            ok, d = cc.check_py_config({"catalogues_a3": t}, pb(PY_OK.replace("enable.auto.commit", "enable.auto.commits")), {})
            self.assertTrue(ok, d)
            self.assertIn("no property table", d)

    def test_symbols_absent_is_its_own_reason(self):
        with tempfile.TemporaryDirectory() as t:
            ok, d = cc.check_py_imports({"catalogues_a3": t}, pb(PY_OK), {})
            self.assertFalse(ok)
            self.assertTrue(d.startswith("py_symbols_unavailable"), d)


@unittest.skipUnless(HAVE_PYSYM, "python_symbols.json not built")
class PythonCallbackAliasTests(unittest.TestCase):
    """WP1c fix round, B1: `callback` is accepted as the alias of `on_delivery` (python_symbols.json lists both)."""
    ALIAS = {"required": [{"name": "produce", "class": "Producer", "kwargs": [["on_delivery", "callback"]]}]}
    CODE = "from confluent_kafka import Producer\np = Producer({})\np.produce('t', value='v', %s=print)\n"

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        fix = json.loads(json.dumps(FIX))
        fix["classes"]["confluent_kafka.Producer"]["methods"]["produce"]["params"].append("on_delivery")   # as in the wheel
        (Path(cls.tmp.name) / "python_symbols.json").write_text(json.dumps(fix))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def run_check(self, spec, code):
        return cc.check_py_calls(dict(spec, catalogues_a3=self.tmp.name), pb(code), {})

    def test_either_name_passes(self):
        for kw in ("on_delivery", "callback"):
            ok, d = self.run_check(self.ALIAS, self.CODE % kw)
            self.assertTrue(ok, (kw, d))

    def test_neither_name_fails_and_a_plain_name_stays_exact(self):
        ok, d = self.run_check(self.ALIAS, "from confluent_kafka import Producer\np = Producer({})\np.produce('t', value='v')\n")
        self.assertFalse(ok)
        self.assertIn("missing call Producer.produce", d)
        ok, d = self.run_check({"required": [{"name": "produce", "class": "Producer", "kwargs": ["on_delivery"]}]}, self.CODE % "callback")
        self.assertFalse(ok)

    def test_an_unknown_keyword_still_fails(self):
        ok, d = self.run_check(self.ALIAS, self.CODE % "on_delivery" + "p.produce('t', value='v', sync=True)\n")
        self.assertFalse(ok)
        self.assertIn("no parameter sync", d)

    @unittest.skipUnless(HAVE_PYSYM, "python_symbols.json is not built")
    def test_the_real_symbol_list_has_both_names(self):
        params = a3.py_method("confluent_kafka.Producer", "produce")["params"]
        self.assertIn("on_delivery", params)
        self.assertIn("callback", params)
        for kw in ("on_delivery", "callback"):
            ok, d = cc.check_py_calls(self.ALIAS, pb(self.CODE % kw), {})
            self.assertTrue(ok, (kw, d))


@unittest.skipUnless(HAVE_PYSYM, "python_symbols.json not built")
class PythonRound2Tests(unittest.TestCase):
    """WP1c fix round 2: N3 (a kwargs entry may fix the value) and a Python callback passed by position."""
    COMMIT = {"required": [{"name": "commit", "class": "Consumer", "kwargs": [{"name": "asynchronous", "value": False}]}]}
    HEAD = "from confluent_kafka import Consumer\nc = Consumer({})\nm = c.poll(1.0)\n"
    PRODUCE = {"required": [{"name": "produce", "class": "Producer", "kwargs": [["on_delivery", "callback"]]}]}
    PHEAD = "from confluent_kafka import Producer\np = Producer({})\n"

    def commit(self, call):
        return cc.check_py_calls(self.COMMIT, pb(self.HEAD + call + "\n"), {})

    def test_the_value_of_asynchronous_is_compared_n3(self):
        for call in ("c.commit(message=m, asynchronous=False)", "c.commit(m, asynchronous=False)",
                     "c.commit(asynchronous=False, message=m)"):                       # pcc_r1, pcc_r2
            ok, d = self.commit(call)
            self.assertTrue(ok, (call, d))
        for call in ("c.commit(message=m, asynchronous=True)",                           # pcc_bad3
                     "c.commit(message=m)",                                              # pcc_bad1
                     "c.commit(message=m, asynchronous=0)",                              # False is not 0
                     "c.commit(message=m, asynchronous=flag)"):                          # a value the check cannot know
            ok, d = self.commit(call)
            self.assertFalse(ok, call)
            self.assertIn("missing call Consumer.commit", d)

    def test_an_entry_without_a_value_still_takes_any_value(self):
        spec = {"required": [{"name": "commit", "class": "Consumer", "kwargs": ["asynchronous"]}]}
        for v in ("True", "False", "flag"):
            self.assertTrue(cc.check_py_calls(spec, pb(self.HEAD + "c.commit(message=m, asynchronous=%s)\n" % v), {})[0], v)

    def test_unpacked_keywords_cannot_be_disproved(self):
        self.assertTrue(self.commit("c.commit(message=m, **opts)")[0])

    def test_a_callback_by_position_counts_where_the_symbol_list_gives_its_position(self):
        params = a3.py_method("confluent_kafka.Producer", "produce")["params"]
        pos = min(params.index("callback"), params.index("on_delivery"))
        self.assertEqual(pos, 4)                                                         # topic, value, key, partition, callback
        ok, d = cc.check_py_calls(self.PRODUCE, pb(self.PHEAD + "p.produce('t', 'v', 'k', -1, print)\n"), {})
        self.assertTrue(ok, d)                                                           # ppi_pos
        ok, d = cc.check_py_calls(self.PRODUCE, pb(self.PHEAD + "p.produce('t', 'v', 'k', -1)\n"), {})
        self.assertFalse(ok)                                                             # four positional arguments: no callback
        ok, d = cc.check_py_calls(self.PRODUCE, pb(self.PHEAD + "p.produce('t', 'v', 'k')\n"), {})
        self.assertFalse(ok)
        ok, d = cc.check_py_calls(self.PRODUCE, pb(self.PHEAD + "p.produce('t', 'v', 'k', -1, callback=print)\n"), {})
        self.assertTrue(ok, d)

    def test_a_value_given_by_position_is_compared_too(self):
        params = a3.py_method("confluent_kafka.Consumer", "commit")["params"]
        i = params.index("asynchronous")
        args = ["None"] * i + ["False"]
        ok, d = self.commit("c.commit(%s)" % ", ".join(args))
        self.assertTrue(ok, d)
        args[-1] = "True"
        self.assertFalse(self.commit("c.commit(%s)" % ", ".join(args))[0])


class PythonRealSymbolsTests(unittest.TestCase):
    @unittest.skipUnless(HAVE_PYSYM, "work/m4/catalogues_a3/python_symbols.json not built: run scripts/build_catalogues_a3.py")
    def test_a_realistic_consumer_passes_every_python_check(self):
        text = pb(PY_OK)
        self.assertTrue(cc.check_py_syntax({}, text, {})[0])
        for fn, spec in ((cc.check_py_imports, {"require": ["Consumer"]}),
                         (cc.check_py_calls, {"required": [{"name": "poll", "class": "Consumer"}, {"name": "commit", "class": "Consumer"}]}),
                         (cc.check_py_config, {"keys": {"enable.auto.commit": "false"}})):
            ok, d = fn(spec, text, {})
            self.assertTrue(ok, d)

    def test_invented_names_fail_against_the_real_table(self):
        self.assertFalse(cc.check_py_imports({}, pb("from confluent_kafka import Consumr\n"), {})[0])
        self.assertFalse(cc.check_py_calls({}, pb(PY_OK.replace("consumer.close()", "consumer.shutdown()")), {})[0])
        self.assertFalse(cc.check_py_config({}, pb(PY_OK.replace("enable.auto.commit", "enable.auto.commits")), {})[0])

    @unittest.skipUnless(HAVE_PYSYM, "work/m4/catalogues_a3/python_symbols.json not built: run scripts/build_catalogues_a3.py")
    def test_admin_and_producer_calls(self):
        code = """
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})
admin.create_topics([NewTopic("t", num_partitions=3, replication_factor=2)])
p = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True, "acks": "all"})
p.produce("t", value=b"x", key=b"k")
p.flush()
"""
        text = pb(code)
        self.assertTrue(cc.check_py_imports({"require": ["Producer", "AdminClient"]}, text, {})[0])
        self.assertTrue(cc.check_py_calls({"required": [{"name": "create_topics", "class": "AdminClient"},
                                                       {"name": "produce", "class": "Producer"}],
                                          "order": [{"name": "produce"}, {"name": "flush"}]}, text, {})[0])
        self.assertTrue(cc.check_py_config({"keys": {"enable.idempotence": "true"}}, text, {})[0])
        self.assertFalse(cc.check_py_calls({}, pb(code.replace("p.flush()", "p.flush_all()")), {})[0])


class RegistrationTests(unittest.TestCase):
    def test_client_checks_are_in_the_a3_registry_and_the_scorer(self):
        import eval2_checks_a3 as a3c
        import eval2_score
        for name in cc.CHECKS_A3_CLIENT:
            self.assertIn(name, a3c.CHECKS_A3)
            self.assertIn(name, eval2_score.CHECKS)

    def test_listcalls_is_cached_by_the_hash_of_its_source(self):
        if not HAVE_JAVA:
            self.skipTest("javac missing")
        d, why = cc.listcalls_dir()
        self.assertIsNone(why)
        import hashlib
        self.assertEqual(d.name, hashlib.sha256(cc.LISTCALLS.read_bytes()).hexdigest())
        self.assertTrue((d / "ListCalls.class").is_file())
        d2, _ = cc.listcalls_dir()
        self.assertEqual(d, d2)


if __name__ == "__main__":
    unittest.main()
