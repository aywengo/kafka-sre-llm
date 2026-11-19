"""Client-code item generator of the a3 slice (WP1c T4): scripts/eval2_items_a3_client.py.

The Java half needs javac, the two pinned jars (raw_catalogue/maven/) and java_api.json, the Python half needs
python_symbols.json, the anchors need the pinned docs and the Kafka sources under raw/ and raw_catalogue/. Whatever is
missing is skipped. javac runs only through the checks of scripts/eval2_checks_a3_client.py (temporary folder, timeout);
nothing compiled is run. No model, no network."""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_checks_a3_client as cc  # noqa: E402
import eval2_items_a3_client as gen  # noqa: E402
from kafka_sre_rag import catalogue_a3 as a3  # noqa: E402

REAL = ROOT / "work" / "m4" / "catalogues_a3"
HAVE_CATS = (ROOT / "work" / "m4" / "catalogues").is_dir() and REAL.is_dir()
HAVE_JAVA = bool(shutil.which("javac") and shutil.which("java") and cc.jar_paths()[0]) and (REAL / "java_api.json").is_file()
HAVE_PY = (REAL / "python_symbols.json").is_file()
HAVE_OPEN = (ROOT / "eval" / "m4" / "open" / "tier_a.jsonl").is_file()
NEED = HAVE_CATS and HAVE_OPEN and (HAVE_JAVA or HAVE_PY)

_state = {}


def built():
    """(items, problems): the items of generate(verify=False) and verify_item's verdict per id, computed once."""
    if "items" not in _state:
        items = gen.generate(verify=False)
        from kafka_sre_rag.catalogue import Catalogues
        cats = Catalogues.load()
        problems = {}
        with gen._Memo():
            for it in items:
                problems[it["id"]] = gen.verify_item(it, cats)
        _state["items"], _state["problems"] = items, problems
    return _state["items"], _state["problems"]


def by_lang(lang):
    return [i for i in built()[0] if i["language"] == lang]


# ---- the independent review's rewordings (runs/m4/reports/wp1c_t9_review.md, B1), correct programs that the v1 checks failed
REVIEW_FIXTURES = {
    'ad_r1': """```java
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

public class CreateCompacted {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9093");
        try (Admin admin = Admin.create(props)) {
            NewTopic topic = new NewTopic("customers-compacted", 3, (short) 3)
                .configs(Map.of("cleanup.policy", "compact", "retention.ms", "86400000"));
            admin.createTopics(List.of(topic)).all().get();
        }
    }
}
```
""",
    'ad_r2': """```java
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

public class CreateCompacted {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9093");
        Admin admin = Admin.create(props);
        Map<String, String> configs = new HashMap<>();
        configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        configs.put(TopicConfig.RETENTION_MS_CONFIG, "86400000");
        NewTopic topic = new NewTopic("customers-compacted", 3, (short) 3).configs(configs);
        admin.createTopics(Collections.singleton(topic), new CreateTopicsOptions()).all().get(30, TimeUnit.SECONDS);
        admin.close();
    }
}
```
""",
    'ad_r3': """```java
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

public class CreateCompacted {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9093");
        Admin admin = Admin.create(props);
        try {
            NewTopic topic = new NewTopic("customers-compacted", 3, (short) 3);
            topic.configs(Map.of("retention.ms", "86400000", "cleanup.policy", "compact"));
            admin.createTopics(List.of(topic)).all().get();
        } finally {
            admin.close();
        }
    }
}
```
""",
    'ad_r4': """```java
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

public class CreateCompacted {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9093");
        AdminClient admin = AdminClient.create(props);
        NewTopic topic = new NewTopic("customers-compacted", 3, (short) 3);
        topic.configs(Map.of("retention.ms", "86400000", "cleanup.policy", "compact"));
        admin.createTopics(List.of(topic)).all().get();
        admin.close();
    }
}
```
""",
    'cs_r1': """```java
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class OrderReader {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("orders"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
                consumer.commitSync(Duration.ofSeconds(10));
            }
        }
    }
}
```
""",
    'pi_r1': """```java
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class InvoiceSender {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9093");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 25; i++) {
                producer.send(new ProducerRecord<>("invoices", "key-" + i, "value-" + i), (md, e) -> {
                    if (e != null) e.printStackTrace();
                });
            }
            producer.flush();
        }
    }
}
```
""",
    'ppi_r1': """```python
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)

producer = Producer({"bootstrap.servers": "broker-1:9093", "enable.idempotence": True, "acks": "all"})
for i in range(25):
    producer.produce("invoices", key=str(i), value="value-%d" % i, callback=delivery_report)
    producer.poll(0)
producer.flush()
```
""",
    'ppi_r2': """```python
from confluent_kafka import Producer

def on_delivery(err, msg):
    if err:
        print("failed", err)

p = Producer({"bootstrap.servers": "broker-1:9093", "enable.idempotence": "true", "acks": "all"})
for i in range(25):
    p.produce(topic="invoices", value=f"value-{i}", key=str(i), on_delivery=on_delivery)
    p.poll(0)
p.flush()
```
""",
    'rl_r1': """```java
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StockSync {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("group.id", "stock-sync");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("inventory"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync();
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
        });
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> r : records) {
                    System.out.println(r.value());
                }
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) System.err.println("commit failed: " + exception);
                });
            }
        } finally {
            consumer.close();
        }
    }
}
```
""",
    'rl_r2': """```java
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StockSync {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-sync");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("inventory"), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(current);
            }
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
        });
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> r : records) {
                current.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
            }
            consumer.commitAsync();
        }
    }
}
```
""",
    'rl_r3': """```java
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class StockSync {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-sync");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("inventory"), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) { consumer.commitSync(); }
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
            });
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(r -> System.out.println(r.value()));
                consumer.commitAsync();
            }
        }
    }
}
```
""",
    'su_r1': """```java
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class ExactlyOnceShout {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "shout-eos");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9093");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("comments", Consumed.with(Serdes.String(), Serdes.String()))
               .mapValues(v -> v.toUpperCase())
               .to("comments-upper", Produced.with(Serdes.String(), Serdes.String()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
```
""",
}
FIXTURE_ITEM = {"ad_r1": "client_admin_create_topic-v2-002", "ad_r2": "client_admin_create_topic-v2-002",
                "ad_r3": "client_admin_create_topic-v2-002", "ad_r4": "client_admin_create_topic-v2-002",
                "cs_r1": "client_consumer_commit_sync-v2-001", "pi_r1": "client_producer_idempotent-v2-002",
                "ppi_r1": "client_py_producer_idempotent-v2-002", "ppi_r2": "client_py_producer_idempotent-v2-002",
                "rl_r1": "client_consumer_rebalance_listener-v2-001", "rl_r2": "client_consumer_rebalance_listener-v2-001",
                "rl_r3": "client_consumer_rebalance_listener-v2-001", "su_r1": "client_streams_uppercase-v2-002"}
# wrong programs: (fixture, old, new, the check that must fail)
WRONG = [("ad_r1", "admin.createTopics(List.of(topic))", "admin.createTopics(topic)", "java_calls"),
         ("ad_r3", "admin.close();", "", "java_calls"),
         ("cs_r1", "consumer.commitSync(Duration.ofSeconds(10));", "", "java_calls"),
         ("cs_r1", "consumer.poll(Duration.ofMillis(500))", "consumer.poll(500)", "java_calls"),
         ("pi_r1", "producer.flush();", "", "java_calls"),
         ("pi_r1", 'producer.send(new ProducerRecord<>("invoices", "key-" + i, "value-" + i), (md, e) -> {\n'
                   '                    if (e != null) e.printStackTrace();\n                });',
          'producer.send("invoices", "key-" + i, "value-" + i);', "java_calls"),
         ("ppi_r1", ", callback=delivery_report", "", "py_calls"),
         ("ppi_r2", "p.flush()", "", "py_calls"),
         ("ppi_r1", "callback=delivery_report", "sync=delivery_report", "py_calls"),
         ("rl_r1", 'consumer.subscribe(List.of("inventory"), new ConsumerRebalanceListener() {', 'consumer.subscribe(List.of("inventory")); new ConsumerRebalanceListener() {',
          "java_calls"),
         ("su_r1", '.to("comments-upper", Produced.with(Serdes.String(), Serdes.String()))', '.to("comments-upper", Serdes.String(), Serdes.String())', "java_calls"),
         ("su_r1", "streams.start();", "", "java_calls")]


def _by_id():
    return {i["id"]: i for i in built()[0]}


@unittest.skipUnless(NEED, "catalogues, the open file, and the jars or the Python symbol list are missing")
class Round2Tests(unittest.TestCase):
    """WP1c fix round 2: N1 (variants for the hidden side), N3 (the value of `asynchronous`), a callback by position."""

    PCC = """```python
from confluent_kafka import Consumer

c = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "billing", "enable.auto.commit": False})
c.subscribe(["orders"])
try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        print(msg.value())
        c.commit(message=msg, %s)
finally:
    c.close()
```"""

    def test_the_variants_for_the_hidden_side_are_new_entries_with_their_own_numbers(self):
        v3 = [i for i in built()[0] if "-v3-" in i["id"] and i["template"] != "client_py_consumer_commit"]
        self.assertEqual(sorted(i["template"] for i in v3), [
            "client_admin_create_topic", "client_consumer_assign_seek", "client_consumer_commit_async",
            "client_consumer_rebalance_listener", "client_producer_idempotent", "client_py_producer_idempotent"][:6 if HAVE_JAVA else 1])
        for i in v3:
            self.assertTrue(i["id"].endswith("-v3-001"), i["id"])
        fam = {i["family"] for i in built()[0]}
        self.assertEqual(len(fam), len(built()[0]))
        # the Java task and its Python restatement share the new variant: one split group
        j = next(i for i in v3 if i["template"] == "client_producer_idempotent")
        p = next(i for i in v3 if i["template"] == "client_py_producer_idempotent")
        self.assertEqual(j["pair"], p["pair"])

    def test_the_commit_item_changed_in_round_2_has_new_ids(self):
        ids = sorted(i["id"] for i in built()[0] if i["template"] == "client_py_consumer_commit")
        self.assertEqual(ids, ["a3-client_py_consumer_commit-v3-001", "a3-client_py_consumer_commit-v3-002"])
        self.assertTrue(all(i["id"].startswith("a3-client_py_consumer_commit-v3-") for i in built()[0] if i["template"] == "client_py_consumer_commit"))

    def test_asynchronous_must_be_false_n3(self):
        it = next(i for i in built()[0] if i["template"] == "client_py_consumer_commit")
        calls = next(c for c in it["checks"] if c["type"] == "py_calls")
        self.assertIn({"name": "asynchronous", "value": False}, calls["required"][2]["kwargs"])
        self.assertTrue(all(r["pass"] for r in _score(it, it["reference"])))
        for arg, ok in (("asynchronous=False", True), ("asynchronous=True", False)):          # pcc_r1, pcc_bad3
            res = _score(it, self.PCC % arg)
            self.assertEqual(all(r["pass"] for r in res), ok, (arg, [r for r in res if not r["pass"]]))
        self.assertFalse(all(r["pass"] for r in _score(it, self.PCC % "")))                     # pcc_bad1

    def test_a_callback_by_position_passes_the_idempotent_item(self):
        it = next(i for i in built()[0] if i["template"] == "client_py_producer_idempotent")
        text = """```python
from confluent_kafka import Producer

def report(err, msg):
    if err is not None:
        print("failed:", err)

p = Producer({"bootstrap.servers": "%s", "enable.idempotence": True, "acks": "all"})
for i in range(%s):
    p.produce("%s", str(i), str(i), -1, report)
    p.poll(0)
p.flush()
```"""
        q = it["question"]
        import re as _re
        topic = _re.search(r"topic `([^`]+)`", q).group(1)
        bs = _re.search(r"on `([^`]+)`", q).group(1)
        n = _re.search(r"sends (\d+) messages", q).group(1)
        res = _score(it, text % (bs, n, topic))
        self.assertTrue(all(r["pass"] for r in res), [r for r in res if not r["pass"]])


def _score(item, text):
    import eval2_score as sc
    from kafka_sre_rag.catalogue import Catalogues
    return sc.run_checks(item, text, Catalogues.load())


@unittest.skipUnless(NEED, "catalogues, the open file, and the jars or the Python symbol list are missing")
class ReviewFixtureTests(unittest.TestCase):
    """WP1c fix round F1 (review B1): every correct rewording passes, every wrong one fails."""

    def item(self, key):
        return next(i for i in built()[0] if i["id"] == "a3-" + FIXTURE_ITEM[key])

    def test_the_correct_rewordings_pass(self):
        for key, text in REVIEW_FIXTURES.items():
            it = self.item(key)
            if it["language"] == "java" and not HAVE_JAVA or it["language"] == "python" and not HAVE_PY:
                continue
            with gen._Memo():
                res = _score(it, text)
            self.assertTrue(all(r["pass"] for r in res), (key, [r for r in res if not r["pass"]]))

    def test_the_wrong_programs_fail_their_check(self):
        for key, old, new, check in WRONG:
            it = self.item(key)
            if it["language"] == "java" and not HAVE_JAVA or it["language"] == "python" and not HAVE_PY:
                continue
            text = REVIEW_FIXTURES[key]
            self.assertIn(old, text, (key, old))
            with gen._Memo():
                res = _score(it, text.replace(old, new))
            failed = [r["type"] for r in res if not r["pass"]]
            self.assertIn(check, failed, (key, old, failed))

    def test_a_perturbed_wrong_signature_is_never_an_accepted_overload(self):
        for it in built()[0]:
            sig = next(p for p in it["perturbed"] if p["kind"] == "wrong_signature")
            if it["language"] == "java":
                tpl = next(t for t in gen.JAVA_TEMPLATES if t["template"] == it["template"])
                self.assertIn(sig["check"], ("java_calls",))
                if "faults" in tpl:
                    why, absent, cites = tpl["faults"]["wrong_signature"]
                    for cls_, m_, params in absent:
                        self.assertNotIn([cc._erase(x) for x in params], gen.catalogue_signatures(cls_, m_), why)
                    self.assertTrue(cites)
                    self.assertIn("java_compiles", sig["fails"], it["id"])   # a call the API does not have does not compile


@unittest.skipUnless(HAVE_JAVA and (REAL / "java_api.json").is_file(), "java_api.json or javac missing")
class OverloadTests(unittest.TestCase):
    def test_every_accepted_signature_is_in_the_catalogue_and_cited(self):
        for t in gen.JAVA_TEMPLATES:
            for e in t["calls"]:
                for sig in cc.entry_signatures(e) or []:
                    if e["method"].startswith("%"):
                        continue
                    self.assertIn([cc._erase(x) for x in sig], gen.catalogue_signatures(e["class"], e["method"]), (t["template"], e))
                if e.get("signatures") and e["class"] != gen.KFUTURE:   # KafkaFuture is not in the pinned source: its cite is the catalogue
                    self.assertTrue(t.get("overloads"), t["template"])
                    names = " ".join(a["requires"] for a in t["overloads"])
                    self.assertIn(e["method"], names, (t["template"], e["method"]))

    def test_the_items_carry_the_overload_and_fault_sources(self):
        for it in built()[0]:
            if it["language"] == "java" and any(c.get("signatures") for c in it["checks"] if c["type"] == "java_calls"):
                self.assertTrue(any(s["requires"].startswith("accepted overload") for s in it["sources"]), it["id"])


@unittest.skipUnless(NEED, "catalogues or the open file are missing")
class NotInIndexTests(unittest.TestCase):
    """S6, per item against the frozen index file (read only; the generator itself never reads it)."""

    def test_the_tag_matches_where_each_items_passages_are(self):
        idx = ROOT / "work" / "v3" / "chunks_s1.jsonl"
        if not idx.is_file():
            self.skipTest("the frozen index file is not on this machine")
        paths = set()
        for line in idx.read_text(errors="replace").splitlines():
            if line.strip():
                paths.add(json.loads(line)["path"])
        for it in built()[0]:
            files = {s["file"] for s in it["sources"] if s.get("file")}
            in_index = any(f in paths for f in files)
            if it["language"] == "java":
                self.assertEqual(it["not_in_index"], not in_index, (it["id"], sorted(files)))
            else:
                self.assertTrue(it["not_in_index"])
        java = [i for i in built()[0] if i["language"] == "java"]
        if java:
            self.assertEqual(sorted({i["template"] for i in java if not i["not_in_index"]}), ["client_streams_uppercase"])


class ApplyOpTests(unittest.TestCase):
    SRC = "a\n    b\n    c\nd"

    def test_replace_needs_one_match(self):
        self.assertEqual(gen.apply_op(self.SRC, ("replace", "c", "C")), "a\n    b\n    C\nd")
        with self.assertRaises(AssertionError):
            gen.apply_op("x x", ("replace", "x", "y"))

    def test_remove(self):
        self.assertEqual(gen.apply_op(self.SRC, ("remove", "b")), "a\n    c\nd")

    def test_move_keeps_the_indentation_of_the_target(self):
        self.assertEqual(gen.apply_op(self.SRC, ("move_before", "d", "b")), "a\n    d\n    b\n    c")
        self.assertEqual(gen.apply_op(self.SRC, ("move_after", "a", "c")), "    b\n    c\n    a\nd")

    def test_fill_leaves_nothing_open(self):
        self.assertEqual(gen.fill("x %%a%% y", {"a": 1}), "x 1 y")
        with self.assertRaises(AssertionError):
            gen.fill("x %%b%%", {"a": 1})

    def test_sub_replaces_longest_name_first(self):
        self.assertEqual(gen.sub("%topic-%t", {"t": "T", "topic": "orders"}), "orders-T")

    def test_anchor_check(self):
        ok, why = gen.verify_anchor("tests/test_eval2_items_a3_client.py", 1, "Client-code item generator")
        self.assertTrue(ok, why)
        self.assertFalse(gen.verify_anchor("tests/test_eval2_items_a3_client.py", 1, "no such phrase")[0])
        self.assertFalse(gen.verify_anchor("tests/test_eval2_items_a3_client.py", 99999, "x")[0])
        self.assertFalse(gen.verify_anchor("tests/none.py", 1, "x")[0])

    def test_templates_are_consistent(self):
        # no model and no network in the generator; every template names a probe or config scope it can mutate
        text = (ROOT / "scripts" / "eval2_items_a3_client.py").read_text()
        for word in ("urllib", "import requests", "mlx", "socket", "openai", "anthropic"):
            self.assertNotIn(word, text)
        for t in gen.JAVA_TEMPLATES + gen.PY_TEMPLATES:
            self.assertGreaterEqual(len(t["variants"]), 2, t["template"])
            self.assertTrue(t["anchors"], t["template"])
            self.assertNotIn("rocksdb", t["source"].lower(), t["template"])
        self.assertEqual(sum(len(t["variants"]) for t in gen.JAVA_TEMPLATES), gen.PLAN["java"])
        self.assertEqual(sum(len(t["variants"]) for t in gen.PY_TEMPLATES), gen.PLAN["python"])

    def test_memo_restores_list_calls(self):
        orig = cc.list_calls
        with gen._Memo():
            self.assertIsNot(cc.list_calls, orig)
        self.assertIs(cc.list_calls, orig)


@unittest.skipUnless(NEED, "catalogues, the open file, and the jars or the Python symbol list are missing")
class ItemTests(unittest.TestCase):
    def test_counts_against_the_plan(self):
        nj, npy = len(by_lang("java")), len(by_lang("python"))
        fj = sum(len(t.get("fresh", [])) for t in gen.JAVA_TEMPLATES)      # round 2 (N1): the variants for the hidden side
        fp = sum(len(t.get("fresh", [])) for t in gen.PY_TEMPLATES)
        self.assertEqual((fj, fp), (5, 1))
        print("\nclient items: %d Java (plan %d), %d Python (plan %d)" % (nj, gen.PLAN["java"], npy, gen.PLAN["python"]))
        if HAVE_JAVA:
            self.assertEqual(nj, gen.PLAN["java"] + fj)
        else:
            self.assertEqual(nj, 0)
        if HAVE_PY:
            self.assertEqual(npy, gen.PLAN["python"] + fp)

    def test_every_reference_passes_every_check(self):
        from kafka_sre_rag.catalogue import Catalogues
        import eval2_score as sc
        cats = Catalogues.load()
        items, problems = built()
        with gen._Memo():
            for it in items:
                res = sc.run_checks(it, it["reference"], cats)
                self.assertTrue(all(r["pass"] for r in res), (it["id"], [r for r in res if not r["pass"]]))
        self.assertTrue(all(v is None for v in problems.values()), {k: v for k, v in problems.items() if v})

    def test_every_perturbed_program_fails_its_named_check(self):
        from kafka_sre_rag.catalogue import Catalogues
        import eval2_score as sc
        cats = Catalogues.load()
        items, _ = built()
        exact_kinds = {"wrong_order", "invented_key", "bad_import", "invented_method"}
        seen = set()
        with gen._Memo():
            for it in items:
                kinds = [p["kind"] for p in it["perturbed"]]
                self.assertEqual(len(kinds), 6, it["id"])
                self.assertEqual(len(set(kinds)), 6, it["id"])
                for pt in it["perturbed"]:
                    res = sc.run_checks(it, pt["text"], cats)
                    failed = sorted(r["type"] for r in res if not r["pass"])
                    self.assertIn(pt["check"], failed, (it["id"], pt["kind"], failed))
                    self.assertEqual(failed, pt["fails"], (it["id"], pt["kind"]))
                    self.assertEqual(it["checks"][pt["violates"]]["type"], pt["check"])
                    if pt["kind"] in exact_kinds or (pt["kind"] == "wrong_signature" and pt["fails"] == [pt["check"]]):
                        self.assertEqual(failed, [pt["check"]], (it["id"], pt["kind"]))
                    self.assertNotEqual(pt["text"], it["reference"])
                    seen.add(pt["kind"])
        self.assertEqual(seen, set(gen.JAVA_KINDS + gen.PY_KINDS) if (HAVE_JAVA and HAVE_PY) else seen)

    @unittest.skipUnless(HAVE_JAVA, "javac or the jars are missing")
    def test_every_java_skeleton_compiles_against_the_jars(self):
        for it in by_lang("java"):
            ok, detail = cc.check_java_compiles({"timeout": 30}, it["reference"], None)
            self.assertTrue(ok, (it["id"], detail))
            self.assertFalse(detail.startswith("java_template_rocksdb"))
            self.assertNotIn("rocksdb", it["reference"].lower())

    def test_every_cited_source_exists(self):
        n = 0
        for it in built()[0]:
            for s in it["sources"]:
                if s["file"] is None:
                    self.assertIn(s["catalogue"], ("java_api", "python_symbols"))
                    continue
                p = ROOT / s["file"]
                if not p.is_file():
                    self.skipTest("%s is not fetched" % s["file"])
                lines = p.read_text(errors="replace").split("\n")
                self.assertTrue(1 <= s["line"] <= len(lines), (it["id"], s["file"], s["line"]))
                ok, why = gen.verify_anchor(s["file"], s["line"], s["fact"])
                self.assertTrue(ok, (it["id"], why))
                n += 1
        self.assertGreater(n, 0)

    def test_item_fields(self):
        for it in built()[0]:
            self.assertEqual(it["kind"], "client_code")
            self.assertEqual(it["tier"], "A")
            self.assertTrue(it["id"].startswith("a3-client_"))
            self.assertTrue(it["anchored"])
            self.assertEqual(it["not_in_index"], gen.not_in_index(
                next(t for t in gen.JAVA_TEMPLATES + gen.PY_TEMPLATES if t["template"] == it["template"]), it["language"]))
            self.assertRegex(it["id"], r"^a3-%s-v[23]-\d{3}$" % it["template"])
            self.assertNotIn("concrete class", it["question"])
            self.assertEqual(it["generator"], gen.VERSION)
            self.assertEqual(gen.VERSION, "eval2_items_a3_client/3")
            self.assertEqual(it["seed"], gen.DEFAULT_SEED)
            self.assertEqual(it["perturbed_reference"], it["perturbed"][0]["text"])
            self.assertEqual(sum(1 for c in it["checks"] if c.get("format")), 1)
            self.assertIn("catalogue_hash_a3", it)
            self.assertNotIn(it["reference"], it["question"])
            fence = "```java" if it["language"] == "java" else "```python"
            self.assertTrue(it["reference"].startswith(fence))
            for pt in it["perturbed"]:
                self.assertTrue(pt["text"].startswith(fence))
        ids = [i["id"] for i in built()[0]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(ids, sorted(ids))
        self.assertGreaterEqual(len({i["family"] for i in built()[0] if i["language"] == "java"}), 9 if HAVE_JAVA else 0)

    def test_every_python_item_shares_a_pair_with_exactly_one_java_item(self):
        items = built()[0]
        java = [i for i in items if i["language"] == "java"]
        for it in items:
            if it["language"] == "python":
                mates = [j for j in java if j["pair"] == it["pair"]]
                self.assertEqual(len(mates), 1, it["id"])
                self.assertEqual([p for p in items if p["language"] == "python" and p["pair"] == it["pair"]], [it])
        self.assertEqual(len({j["pair"] for j in java}), len(java))

    def test_questions_are_not_near_duplicates(self):
        for it in built()[0]:
            d = it["dedup"]
            self.assertLess(d["max_jaccard"], 0.5, it["id"])
            self.assertLess(d["max_containment"], 0.8, it["id"])
            self.assertLessEqual(d["max_containment"], 0.7, it["id"])   # the conductor's margin (T4 follow-up)

    def test_questions_name_what_the_checks_need(self):
        for it in built()[0]:
            q = it["question"]
            for c in it["checks"]:
                for k, v in (c.get("keys") or {}).items():
                    if k in ("bootstrap.servers", "group.id", "transactional.id", "application.id") and v:
                        self.assertIn(v, q, (it["id"], k))

    def test_no_file_is_written_under_eval(self):
        def snap():
            return sorted((str(p.relative_to(ROOT)), p.stat().st_size, p.stat().st_mtime_ns)
                          for p in (ROOT / "eval").rglob("*") if p.is_file() and "hidden" not in p.parts)
        before = snap()
        gen.generate(verify=False)
        self.assertEqual(before, snap())


@unittest.skipUnless(NEED, "catalogues, the open file, and the jars or the Python symbol list are missing")
class CatalogueIndependenceTests(unittest.TestCase):
    def test_an_unrelated_catalogue_change_reshuffles_nothing(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp) / "cat"
            shutil.copytree(REAL, d)
            f = d / "strimzi_forbidden.json"
            j = json.loads(f.read_text())
            j["unrelated_extra_field"] = {"note": "no generator reads this"}
            f.write_text(json.dumps(j))
            self.assertNotEqual(a3.manifest(d)["catalogue_hash"], a3.manifest(REAL)["catalogue_hash"])
            other = gen.generate(verify=False, a3_dir=d)
        base = built()[0]
        strip = lambda items: [{k: v for k, v in i.items() if k != "catalogue_hash_a3"} for i in items]  # noqa: E731
        self.assertEqual(strip(base), strip(other))
        self.assertEqual({i["catalogue_hash_a3"] for i in base}, {a3.manifest(REAL)["catalogue_hash"]})
        self.assertEqual(len({i["catalogue_hash_a3"] for i in other}), 1)
        self.assertNotEqual({i["catalogue_hash_a3"] for i in other}, {i["catalogue_hash_a3"] for i in base})

    def test_the_seed_does_not_contain_the_hash(self):
        text = (ROOT / "scripts" / "eval2_items_a3_client.py").read_text()
        self.assertIn('random.Random("%s|%s" % (seed, tpl["template"]))', text)


@unittest.skipUnless(NEED, "catalogues, the open file, and the jars or the Python symbol list are missing")
class DeterminismTests(unittest.TestCase):
    CODE = ("import sys, json, hashlib; sys.path.insert(0, %r); import eval2_items_a3_client as g; "
            "items = g.generate(verify=False); "
            "print(hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest(), len(items))" % str(ROOT / "scripts"))

    def run_with(self, seed):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        r = subprocess.run([sys.executable, "-c", self.CODE], env=env, capture_output=True, text=True, cwd=str(ROOT), timeout=300)
        self.assertEqual(r.returncode, 0, r.stderr[-500:])
        return r.stdout.strip()

    def test_identical_under_two_hash_seeds(self):
        a, b = self.run_with("1"), self.run_with("2")
        self.assertEqual(a, b)
        items = built()[0]
        self.assertEqual(a, "%s %d" % (hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest(), len(items)))


if __name__ == "__main__":
    unittest.main()
