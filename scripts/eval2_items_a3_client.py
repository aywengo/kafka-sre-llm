#!/usr/bin/env python3
"""Client-code items of the a3 slice of tier A (WP1c T4): "write a Kafka client program", Java and Python.

Each item asks for one complete program. The checks are the client checks of scripts/eval2_checks_a3_client.py:
  Java    java_compiles (format), java_calls, java_call_order, java_config   (jars: kafka-clients and kafka-streams 4.3.2)
  Python  py_syntax (format), py_imports, py_calls, py_config                (symbols: the confluent-kafka 2.16.1 wheel)

Truth, no model (D6):
  - the skeletons are written here, once, by hand; a parameter set (topic, group, brokers, counts) fills them in. The
    reference of an item is its skeleton instance, and it must pass every check of the item (the generator runs them);
  - every behavioural requirement (commit after processing, enable.auto.commit=false, idempotence with acks=all,
    initTransactions before beginTransaction, flush after send, application.id, ...) is a passage of a pinned file
    (the Kafka docs of sources.yaml, or the Javadoc of the pinned Kafka source in raw_catalogue/apache-kafka-api/):
    file, line and a phrase that the line must contain (checked at build time; a spec whose phrase is not on its line
    is a failed derivation, not repaired). Class and method names resolve in the Java catalogue (java_api.json, built
    from the two jars), configuration names in its client config tables;
  - the Python items cite the same Kafka passages for what the program must do (they are language independent: the
    consumer commits after processing, the transactional producer calls init_transactions first, ...). The pinned
    documentation has no text on the Python client; the names of imports, calls and configuration properties are checked
    against the symbol list of the confluent-kafka wheel (python_symbols.json), which is a catalogue entry without a
    line. Python items are therefore marked "not_in_index": true and reported as their own subset.
  - Java templates never touch RocksDB types: a `java_template_rocksdb` result is a failure of the build.

Perturbed programs, one of each kind per item (`perturbed`: kind, text, check, fails):
  wrong_signature   a call to an overload (or arity) that the item does not accept (Python: an unknown keyword or too
                    many positional arguments)
  missing_call      a required call left out
  wrong_order       a call moved before the call it must follow
  invented_constant (Java) a `*_CONFIG` constant that does not exist           invented_method (Python) a method that does not exist
  invented_key      a configuration key with a typo (a string, not a constant in Java)
  bad_import        an import of a class (Java) or name (Python) that does not exist
`check` is the check meant to fail; `fails` is the whole set of the item's checks that the program fails. For
wrong_signature (when an overload exists), wrong_order, invented_key and bad_import this is the one named check. Where the
fault breaks compilation (invented_constant) or removes a call that also stands in the order list (missing_call), the
failing set is declared per kind and asserted to be exactly that.

Pairs: every item has `pair` = "client:<java template>:<variant>". A Python item restates the Java instance of the same
variant of its paired template (consumer commit -> client_consumer_commit_sync, idempotent -> client_producer_idempotent,
transaction -> client_producer_transaction; the parameter sets are the same), so each Python item shares its `pair` with
exactly one Java item; Java items without a Python restatement have a pair of their own. T8 keeps a pair on one side.

Deterministic: one random stream per template (Random("seed|template"); the catalogue hash is recorded, not seeded), no set iteration.
`generate()` returns the items and writes no file. Run: .venv/bin/python scripts/eval2_items_a3_client.py
"""
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2_checks_a3_client as ck  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag import catalogue_a3  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, catalogue_hash, default_dir  # noqa: E402

VERSION = "eval2_items_a3_client/1"
DEFAULT_SEED = "m4-a3-1"
KIND = "client_code"
PLAN = {"java": 18, "python": 6}

JAVA_NOTE = ("Give one complete Java source file in a single ```java code block. It must compile against kafka-clients "
             "4.3.2 (and kafka-streams 4.3.2 where Streams is used) with no other library. Declare the client with its "
             "concrete class (KafkaConsumer, KafkaProducer, Admin).")
PY_NOTE = ("Give one complete Python script in a single ```python code block, using only the confluent-kafka package.")

KAFKA = "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka"
KC = KAFKA + "/clients/consumer/KafkaConsumer.java"
KP = KAFKA + "/clients/producer/KafkaProducer.java"
KA = KAFKA + "/clients/admin/Admin.java"
KT = KAFKA + "/common/config/TopicConfig.java"
CFG_STREAMS = "raw/apache-kafka/docs/streams/developer-guide/config-streams.md"
CORE_STREAMS = "raw/apache-kafka/docs/streams/core-concepts.md"
TUTORIAL = "raw/apache-kafka/docs/streams/tutorial.md"

P = "org.apache.kafka.clients."
CONSUMER, PRODUCER, ADMIN = P + "consumer.KafkaConsumer", P + "producer.KafkaProducer", P + "admin.Admin"
NEWTOPIC = P + "admin.NewTopic"
SB = "org.apache.kafka.streams.StreamsBuilder"
KS = "org.apache.kafka.streams.kstream.KStream"
KGS = "org.apache.kafka.streams.kstream.KGroupedStream"
KT_ = "org.apache.kafka.streams.kstream.KTable"
STREAMS = "org.apache.kafka.streams.KafkaStreams"
BAD_JAVA_IMPORT = "org.apache.kafka.clients.ClientFactory"        # not in java_api.json (asserted)
BAD_PY_NAME = "ConsumerFactory"                                      # not in python_symbols.json (asserted)


def E(cls, method, params=None):
    e = {"class": cls, "method": method}
    if params is not None:
        e["params"] = params
    return e


def O(cls, method):                      # an entry of an order list: no signature, only the order matters
    return {"class": cls, "method": method}


def anchor(file, line, phrase, requires):
    return {"file": file, "line": line, "phrase": phrase, "requires": requires}


def verify_anchor(file, line, phrase, root=ROOT):
    """(True, "") when `file` exists, `line` (1-based) is in range and that line contains `phrase`
    (whitespace-normalised, markdown backslashes dropped); else (False, reason)."""
    p = Path(root) / file
    if not p.is_file():
        return False, "file %s not present" % file
    lines = p.read_text(errors="replace").split("\n")
    if not 1 <= line <= len(lines):
        return False, "%s has %d lines, not %d" % (file, len(lines), line)
    norm = lambda s: re.sub(r"\s+", " ", s.replace("\\", "")).strip()  # noqa: E731
    if norm(phrase) not in norm(lines[line - 1]):
        return False, "%s:%d does not contain %r" % (file, line, phrase)
    return True, ""


# ============================================================== Java skeletons
# `%%name%%` is replaced by a parameter; Java has no such token.
J_CONSUMER_HEAD = """import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
"""

JAVA_SYNC = J_CONSUMER_HEAD + """
public class %%cls%% {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "%%group%%");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("%%topic%%"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key() + " " + record.value());
                }
                consumer.commitSync();
            }
        }
    }
}
"""

JAVA_ASYNC = J_CONSUMER_HEAD + """
public class %%cls%% {
    static void logFailure(Exception error) {
        if (error != null) {
            System.err.println("offset commit failed: " + error.getMessage());
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "%%group%%");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("%%topic%%"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key() + " " + record.value());
                }
                consumer.commitAsync((offsets, error) -> logFailure(error));
            }
        }
    }
}
"""

JAVA_ASSIGN = """import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class %%cls%% {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition partition = new TopicPartition("%%topic%%", %%part%%);
            consumer.assign(List.of(partition));
            consumer.%%seek%%(List.of(partition));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + " " + record.value());
            }
        }
    }
}
"""

JAVA_REBALANCE = """import java.time.Duration;
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

public class %%cls%% {
    static class CommitOnRevoke implements ConsumerRebalanceListener {
        private final KafkaConsumer<String, String> consumer;

        CommitOnRevoke(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            consumer.commitSync();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("assigned " + partitions);
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "%%group%%");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("%%topic%%"), new CommitOnRevoke(consumer));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key() + " " + record.value());
                }
                consumer.commitAsync();
            }
        }
    }
}
"""

J_PRODUCER_HEAD = """import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
"""

JAVA_IDEMPOTENT = J_PRODUCER_HEAD + """
public class %%cls%% {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < %%n%%; i++) {
                producer.send(new ProducerRecord<>("%%topic%%", "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }
}
"""

JAVA_TXN = """import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

public class %%cls%% {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "%%txid%%");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();
            try {
                producer.beginTransaction();
                for (int i = 0; i < %%n%%; i++) {
                    producer.send(new ProducerRecord<>("%%topic%%", "key-" + i, "value-" + i));
                }
                producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                producer.close();
            } catch (KafkaException e) {
                producer.abortTransaction();
            }
        }
    }
}
"""

JAVA_CALLBACK = J_PRODUCER_HEAD + """
public class %%cls%% {
    static void report(Exception error) {
        if (error != null) {
            System.err.println("delivery failed: " + error.getMessage());
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < %%n%%; i++) {
                producer.send(new ProducerRecord<>("%%topic%%", "key-" + i, "value-" + i), (metadata, exception) -> report(exception));
            }
            producer.flush();
        }
    }
}
"""

JAVA_ADMIN = """import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

public class %%cls%% {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        Admin admin = Admin.create(props);
        NewTopic topic = new NewTopic("%%topic%%", %%parts%%, (short) %%rf%%);
        topic.configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, "%%ret%%", TopicConfig.CLEANUP_POLICY_CONFIG, "%%cleanup%%"));
        CreateTopicsResult result = admin.createTopics(List.of(topic));
        result.all().get();
        admin.close();
    }
}
"""

JAVA_STREAMS = """import java.util.Locale;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class %%cls%% {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "%%appid%%");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "%%bs%%");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
%%eos%%        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream("%%in%%");
        lines.mapValues(value -> value.toUpperCase(Locale.ROOT)).to("%%out%%");
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
"""

# ============================================================== Python skeletons
PY_COMMIT = """from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "%%bs%%",
    "group.id": "%%group%%",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
}
consumer = Consumer(conf)
consumer.subscribe(["%%topic%%"])
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue
        print(msg.key(), msg.value())
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()
"""

PY_IDEMPOTENT = """from confluent_kafka import Producer

conf = {"bootstrap.servers": "%%bs%%", "enable.idempotence": True, "acks": "all"}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print("delivery failed:", err)


for i in range(%%n%%):
    producer.produce("%%topic%%", key=str(i), value="value-" + str(i), on_delivery=delivery_report)
    producer.poll(0)
producer.flush()
"""

PY_TXN = """from confluent_kafka import KafkaException, Producer

conf = {"bootstrap.servers": "%%bs%%", "transactional.id": "%%txid%%"}
producer = Producer(conf)
producer.init_transactions()
try:
    producer.begin_transaction()
    for i in range(%%n%%):
        producer.produce("%%topic%%", key=str(i), value="value-" + str(i))
    producer.commit_transaction()
except KafkaException:
    producer.abort_transaction()
"""

# ============================================================== the templates
CONSUMER_ANCHORS = [
    anchor(KC, 221, 'props.setProperty("bootstrap.servers"', "the client is configured with bootstrap.servers"),
    anchor(KC, 222, 'props.setProperty("group.id"', "a subscribing consumer sets group.id"),
    anchor(KC, 223, 'props.setProperty("enable.auto.commit", "false");', "enable.auto.commit=false for manual commits"),
    anchor(KC, 227, "consumer.subscribe(", "the consumer subscribes before it polls"),
    anchor(KC, 237, "consumer.commitSync();", "commitSync after the polled records are handled"),
]

# Each template: lang, source, domain, tasks (3 phrasings use the same {task}), variants, checks, perturbations.
# `ops` are applied to the filled source: ("replace", old, new) | ("remove", substring) | ("move_before"|"move_after", moved, anchor)
JAVA_TEMPLATES = [
    {"template": "client_consumer_commit_sync", "source": JAVA_SYNC, "domain": "kafka",
     "task": ("consumes the topic `{topic}` as consumer group `{group}` from `{bs}`. Offsets must not be committed "
              "automatically: after each poll it processes the records and only then commits the offsets synchronously."),
     "variants": [("orders", {"cls": "OrderReader", "topic": "orders", "group": "billing", "bs": "kafka:9092"}),
                  ("clicks", {"cls": "ClickConsumer", "topic": "page-clicks", "group": "analytics", "bs": "broker-1:9093"})],
     "anchors": CONSUMER_ANCHORS,
     "calls": [E(CONSUMER, "subscribe", ["java.util.Collection"]), E(CONSUMER, "poll", ["java.time.Duration"]),
               E(CONSUMER, "commitSync", [])],
     "order": [O(CONSUMER, "subscribe"), O(CONSUMER, "poll"), O(CONSUMER, "commitSync")],
     "config": {"keys": ["bootstrap.servers=%bs", "group.id=%group", "enable.auto.commit=false"], "scope": ["consumer"]},
     "probe": ("ConsumerConfig", "ENABLE_AUTO_COMMIT_CONFIG"),
     "ops": {"wrong_signature": ("replace", "consumer.commitSync();", "consumer.commitSync(Duration.ofSeconds(5));"),
             "missing_call": ("remove", "consumer.commitSync();"),
             "wrong_order": ("move_before", "consumer.commitSync();", "ConsumerRecords<String, String> records")}},
    {"template": "client_consumer_commit_async", "source": JAVA_ASYNC, "domain": "kafka",
     "task": ("consumes the topic `{topic}` as consumer group `{group}` from `{bs}` with auto commit turned off. After each "
              "poll it processes the records and then commits the offsets asynchronously, passing a callback that reports "
              "a failed commit."),
     "variants": [("shipments", {"cls": "ShipmentReader", "topic": "shipments", "group": "tracking", "bs": "kafka:9092"}),
                  ("audit", {"cls": "AuditConsumer", "topic": "audit-log", "group": "compliance", "bs": "kafka-bootstrap:9092"})],
     "anchors": CONSUMER_ANCHORS[:4] + [
         anchor(KC, 1125, "This is an asynchronous call and will not block. Any errors encountered are either passed to the callback",
                "commitAsync reports errors only through its callback")],
     "calls": [E(CONSUMER, "subscribe", ["java.util.Collection"]), E(CONSUMER, "poll", ["java.time.Duration"]),
               E(CONSUMER, "commitAsync", [P + "consumer.OffsetCommitCallback"])],
     "order": [O(CONSUMER, "subscribe"), O(CONSUMER, "poll"), O(CONSUMER, "commitAsync")],
     "config": {"keys": ["bootstrap.servers=%bs", "group.id=%group", "enable.auto.commit=false"], "scope": ["consumer"]},
     "probe": ("ConsumerConfig", "ENABLE_AUTO_COMMIT_CONFIG"),
     "ops": {"wrong_signature": ("replace", "consumer.commitAsync((offsets, error) -> logFailure(error));", "consumer.commitAsync();"),
             "missing_call": ("remove", "consumer.commitAsync("),
             "wrong_order": ("move_before", "consumer.commitAsync(", "ConsumerRecords<String, String> records")}},
    {"template": "client_consumer_assign_seek", "source": JAVA_ASSIGN, "domain": "kafka",
     "task": ("reads partition {part} of the topic `{topic}` from `{bs}` without subscribing to a group: it assigns the "
              "partition itself, keeps auto commit off, moves the position to the {where} of the partition, then polls once "
              "and prints the records."),
     "variants": [("beginning", {"cls": "ReplayReader", "topic": "ledger", "part": "0", "bs": "kafka:9092",
                                 "seek": "seekToBeginning", "where": "beginning"}),
                  ("end", {"cls": "TailReader", "topic": "metrics", "part": "2", "bs": "broker-1:9093",
                           "seek": "seekToEnd", "where": "end"})],
     "anchors": [anchor(KC, 221, 'props.setProperty("bootstrap.servers"', "the client is configured with bootstrap.servers"),
                 anchor(KC, 352, "Configure <code>enable.auto.commit=false</code>", "enable.auto.commit=false when the application keeps its position"),
                 anchor(KC, 835, "Manually assign a list of partitions to this consumer", "assign gives the partitions without a group"),
                 anchor(KC, 1222, "Seek to the first offset for each of the given partitions. This function evaluates lazily",
                        "seekToBeginning moves the position, evaluated at the next poll"),
                 anchor(KC, 1235, "Seek to the last offset for each of the given partitions. This function evaluates lazily",
                        "seekToEnd moves the position, evaluated at the next poll")],
     "calls": [E(CONSUMER, "assign", ["java.util.Collection"]), E(CONSUMER, "%seek", ["java.util.Collection"]),
               E(CONSUMER, "poll", ["java.time.Duration"])],
     "order": [O(CONSUMER, "assign"), O(CONSUMER, "%seek"), O(CONSUMER, "poll")],
     "config": {"keys": ["bootstrap.servers=%bs", "enable.auto.commit=false"], "scope": ["consumer"]},
     "probe": ("ConsumerConfig", "ENABLE_AUTO_COMMIT_CONFIG"),
     "ops": {"wrong_signature": ("replace", "consumer.%seek(List.of(partition));", "consumer.%seek(partition);"),
             "missing_call": ("remove", "consumer.%seek("),
             "wrong_order": ("move_before", "consumer.%seek(", "consumer.assign(")},
     "fails": {"wrong_signature": ["java_call_order", "java_calls", "java_compiles"]}},   # no overload takes one partition
    {"template": "client_consumer_rebalance_listener", "source": JAVA_REBALANCE, "domain": "kafka",
     "task": ("consumes the topic `{topic}` as consumer group `{group}` from `{bs}` with auto commit turned off. It "
              "registers a rebalance listener that commits the offsets synchronously when partitions are revoked, and in the "
              "poll loop it commits asynchronously after each batch."),
     "variants": [("inventory", {"cls": "InventoryReader", "topic": "inventory", "group": "stock-sync", "bs": "kafka:9092"}),
                  ("payments", {"cls": "PaymentConsumer", "topic": "payments", "group": "ledger", "bs": "broker-2:9094"})],
     "anchors": CONSUMER_ANCHORS[:4] + [
         anchor(KC, 363, "For example, when partitions are taken from a consumer the consumer will want to commit its offset",
                "commit in the rebalance listener"),
         anchor(KC, 364, "implementing {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}",
                "the commit goes into onPartitionsRevoked"),
         anchor(KC, 1125, "This is an asynchronous call and will not block. Any errors encountered are either passed to the callback",
                "commitAsync in the loop does not block")],
     "calls": [E(CONSUMER, "subscribe", ["java.util.Collection", P + "consumer.ConsumerRebalanceListener"]),
               E(CONSUMER, "poll", ["java.time.Duration"]), E(CONSUMER, "commitSync", []), E(CONSUMER, "commitAsync", [])],
     "order": [O(CONSUMER, "subscribe"), O(CONSUMER, "poll"), O(CONSUMER, "commitAsync")],
     "config": {"keys": ["bootstrap.servers=%bs", "group.id=%group", "enable.auto.commit=false"], "scope": ["consumer"]},
     "probe": ("ConsumerConfig", "ENABLE_AUTO_COMMIT_CONFIG"),
     "ops": {"wrong_signature": ("replace", 'consumer.subscribe(List.of("%topic"), new CommitOnRevoke(consumer));',
                                 'consumer.subscribe(List.of("%topic"));'),
             "missing_call": ("remove", "consumer.commitSync();"),
             "wrong_order": ("move_after", "consumer.subscribe(", "ConsumerRecords<String, String> records")},
     "fails": {"missing_call": ["java_calls"]}},                   # commitSync is not in the order list
    {"template": "client_producer_idempotent", "source": JAVA_IDEMPOTENT, "domain": "kafka",
     "task": ("sends {n} records to the topic `{topic}` on `{bs}` with an idempotent producer (idempotence enabled "
              "explicitly, acks set to all) and flushes the producer before it exits."),
     "variants": [("ticks", {"cls": "TickProducer", "topic": "ticks", "n": "100", "bs": "kafka:9092"}),
                  ("invoices", {"cls": "InvoiceSender", "topic": "invoices", "n": "25", "bs": "broker-1:9093"})],
     "anchors": [anchor(KP, 198, 'props.put("bootstrap.servers"', "the producer is configured with bootstrap.servers"),
                 anchor(KP, 165, "From Kafka 3.0, the <code>enable.idempotence</code> configuration defaults to true.",
                        "enable.idempotence is the setting of the idempotent producer"),
                 anchor(KP, 167, "default to <code>all</code>", "idempotence goes with acks=all"),
                 anchor(KP, 1178, "callbacks passed to {@link #send(ProducerRecord,Callback)} have been called",
                        "flush completes the earlier sends")],
     "calls": [E(PRODUCER, "send", [P + "producer.ProducerRecord"]), E(PRODUCER, "flush", [])],
     "order": [O(PRODUCER, "send"), O(PRODUCER, "flush")],
     "config": {"keys": ["bootstrap.servers=%bs", "enable.idempotence=true", "acks=all"], "scope": ["producer"]},
     "probe": ("ProducerConfig", "ENABLE_IDEMPOTENCE_CONFIG"),
     "ops": {"wrong_signature": ("replace", 'producer.send(new ProducerRecord<>("%topic", "key-" + i, "value-" + i));',
                                 'producer.send(new ProducerRecord<>("%topic", "key-" + i, "value-" + i), null);'),
             "missing_call": ("remove", "producer.flush();"),
             "wrong_order": ("move_before", "producer.flush();", "for (int i = 0;")}},
    {"template": "client_producer_transaction", "source": JAVA_TXN, "domain": "kafka",
     "task": ("sends {n} records to the topic `{topic}` on `{bs}` in one transaction with the transactional id `{txid}`: it "
              "initialises transactions once, begins the transaction, sends the records and commits; when a KafkaException "
              "is thrown it aborts the transaction."),
     "variants": [("readings", {"cls": "ReadingWriter", "topic": "readings", "txid": "reading-writer-1", "n": "10", "bs": "kafka:9092"}),
                  ("ledger", {"cls": "LedgerWriter", "topic": "ledger-entries", "txid": "ledger-tx", "n": "50", "bs": "broker-2:9094"})],
     "anchors": [anchor(KP, 198, 'props.put("bootstrap.servers"', "the producer is configured with bootstrap.servers"),
                 anchor(KP, 199, 'props.put("transactional.id"', "a transactional producer sets transactional.id"),
                 anchor(KP, 202, "producer.initTransactions();", "initTransactions first"),
                 anchor(KP, 205, "producer.beginTransaction();", "beginTransaction after initTransactions"),
                 anchor(KP, 208, "producer.commitTransaction();", "commitTransaction after the sends"),
                 anchor(KP, 214, "producer.abortTransaction();", "abortTransaction on a KafkaException"),
                 anchor(KP, 635, "Needs to be called before any other methods when the {@code transactional.id} is set",
                        "initTransactions before any other method")],
     "calls": [E(PRODUCER, "initTransactions", []), E(PRODUCER, "beginTransaction", []),
               E(PRODUCER, "send", [P + "producer.ProducerRecord"]), E(PRODUCER, "commitTransaction", []),
               E(PRODUCER, "abortTransaction", [])],
     "order": [O(PRODUCER, "initTransactions"), O(PRODUCER, "beginTransaction"), O(PRODUCER, "send"),
               O(PRODUCER, "commitTransaction")],
     "config": {"keys": ["bootstrap.servers=%bs", "transactional.id=%txid"], "forbidden": {"enable.idempotence": "false"},
                "scope": ["producer"]},
     "probe": ("ProducerConfig", "TRANSACTIONAL_ID_CONFIG"),
     "ops": {"wrong_signature": ("replace", 'producer.send(new ProducerRecord<>("%topic", "key-" + i, "value-" + i));',
                                 'producer.send(new ProducerRecord<>("%topic", "key-" + i, "value-" + i), null);'),
             "missing_call": ("remove", "producer.abortTransaction();"),
             "wrong_order": ("move_after", "producer.initTransactions();", "producer.beginTransaction();")},
     "fails": {"missing_call": ["java_calls"]}},                   # abortTransaction is not in the order list
    {"template": "client_producer_callback", "source": JAVA_CALLBACK, "domain": "kafka",
     "task": ("sends {n} records to the topic `{topic}` on `{bs}` asynchronously, passing a callback to every send that "
              "reports a failed delivery, and flushes the producer before it exits."),
     "variants": [("events", {"cls": "EventProducer", "topic": "events", "n": "20", "bs": "kafka:9092"}),
                  ("heartbeats", {"cls": "HeartbeatSender", "topic": "heartbeats", "n": "5", "bs": "kafka-bootstrap:9092"})],
     "anchors": [anchor(KP, 198, 'props.put("bootstrap.servers"', "the producer is configured with bootstrap.servers"),
                 anchor(KP, 883, "Fully non-blocking usage can make use of the {@link Callback} parameter",
                        "send takes a Callback for asynchronous sends"),
                 anchor(KP, 1178, "callbacks passed to {@link #send(ProducerRecord,Callback)} have been called",
                        "flush completes the earlier sends and their callbacks")],
     "calls": [E(PRODUCER, "send", [P + "producer.ProducerRecord", P + "producer.Callback"]), E(PRODUCER, "flush", [])],
     "order": [O(PRODUCER, "send"), O(PRODUCER, "flush")],
     "config": {"keys": ["bootstrap.servers=%bs"], "scope": ["producer"]},
     "probe": ("ProducerConfig", "BOOTSTRAP_SERVERS_CONFIG"),
     "ops": {"wrong_signature": ("replace", ', (metadata, exception) -> report(exception));', ");"),
             "missing_call": ("remove", "producer.flush();"),
             "wrong_order": ("move_before", "producer.flush();", "for (int i = 0;")}},
    {"template": "client_admin_create_topic", "source": JAVA_ADMIN, "domain": "kafka",
     "task": ("creates the topic `{topic}` ({parts} partitions, replication factor {rf}) on `{bs}` through the admin client, "
              "with the topic configs `retention.ms={ret}` and `cleanup.policy={cleanup}`, waits for the result and then "
              "closes the admin client."),
     "variants": [("retained", {"cls": "TopicCreator", "topic": "orders-retained", "parts": "6", "rf": "3", "bs": "kafka:9092",
                                "ret": "604800000", "cleanup": "delete"}),
                  ("compacted", {"cls": "CompactedTopicSetup", "topic": "customers-compacted", "parts": "3", "rf": "3",
                                 "bs": "broker-1:9093", "ret": "86400000", "cleanup": "compact"})],
     "anchors": [anchor(KA, 104, "The {@code bootstrap.servers} config", "the admin client is configured with bootstrap.servers"),
                 anchor(KA, 88, "CreateTopicsResult result = admin.createTopics(Collections.singleton(", "createTopics creates the topic"),
                 anchor(KA, 90, ".configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG", "topic configs go through NewTopic.configs"),
                 anchor(KT, 75, 'RETENTION_MS_CONFIG = "retention.ms"', "retention.ms is a topic config"),
                 anchor(KA, 95, "Call get() to block until the topic creation is complete or has failed", "get() waits for the result")],
     "calls": [E(ADMIN, "createTopics", ["java.util.Collection"]), E(NEWTOPIC, "configs", ["java.util.Map"]),
               E("org.apache.kafka.common.KafkaFuture", "get", []), E(ADMIN, "close", [])],
     "order": [O(NEWTOPIC, "configs"), O(ADMIN, "createTopics"), O("org.apache.kafka.common.KafkaFuture", "get"), O(ADMIN, "close")],
     "config": {"keys": ["bootstrap.servers=%bs", "retention.ms=%ret", "cleanup.policy=%cleanup"], "scope": ["admin", "topic"]},
     "probe": ("TopicConfig", "RETENTION_MS_CONFIG"),
     "ops": {"wrong_signature": ("replace", "admin.createTopics(List.of(topic));",
                                 "admin.createTopics(List.of(topic), new org.apache.kafka.clients.admin.CreateTopicsOptions());"),
             "missing_call": ("remove", "topic.configs("),
             "wrong_order": ("move_before", "admin.close();", "result.all().get();")},
     "fails": {"missing_call": ["java_call_order", "java_calls", "java_config"]}},   # the removed line also carries both topic configs
    {"template": "client_streams_uppercase", "source": JAVA_STREAMS, "domain": "streams",
     "task": ("converts text with Kafka Streams: every value read from the topic `{in}` is written to `{out}` in upper "
              "case, keys unchanged. Application id `{appid}`, brokers `{bs}`{eos_text}. It starts the application."),
     "variants": [("plain", {"cls": "ShoutApp", "in": "notes", "out": "notes-upper", "appid": "shout-app",
                             "bs": "kafka:9092", "eos": "", "eos_text": ""}),
                  ("eos", {"cls": "ExactlyOnceShout", "in": "comments", "out": "comments-upper", "appid": "shout-eos",
                           "bs": "broker-1:9093",
                           "eos": "        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);\n",
                           "eos_text": ", using the EOS v2 mode of `processing.guarantee`"})],
     "anchors": [anchor(CFG_STREAMS, 159, "(Required) The application ID.", "application.id is required"),
                 anchor(CFG_STREAMS, 175, "(Required) The Kafka bootstrap servers.", "bootstrap.servers is required"),
                 anchor(CORE_STREAMS, 154, "set the `processing.guarantee` config value", "processing.guarantee for exactly-once"),
                 anchor(CFG_STREAMS, 856, 'exactly_once_v2', "the value of the exactly-once processing mode"),
                 anchor("raw/apache-kafka/docs/streams/developer-guide/dsl-api.md", 714, "stream.mapValues(value -> value.toUpperCase())", "mapValues transforms each value, keys unchanged"),
                 anchor(TUTORIAL, 125, "final Topology topology = builder.build();", "the topology is built from the builder"),
                 anchor(TUTORIAL, 149, "final KafkaStreams streams = new KafkaStreams(topology, props);", "KafkaStreams from topology and config"),
                 anchor(TUTORIAL, 151, "By calling its `start()` function we can trigger the execution", "start() runs the application")],
     "calls": [E(SB, "stream", ["java.lang.String"]), E(KS, "mapValues"), E(KS, "to", ["java.lang.String"]), E(STREAMS, "<init>"),
               E(STREAMS, "start", [])],
     "order": [O(SB, "stream"), O(KS, "mapValues"), O(KS, "to"), O(STREAMS, "<init>"), O(STREAMS, "start")],
     "config": {"keys": ["application.id=%appid", "bootstrap.servers=%bs"], "scope": ["streams"],
                "variant_keys": {"eos": ["processing.guarantee=exactly_once_v2"]}},
     "probe": ("StreamsConfig", "APPLICATION_ID_CONFIG"),
     "ops": {"wrong_signature": ("replace", '.to("%out");', '.to("%out", org.apache.kafka.streams.kstream.Produced.with(Serdes.String(), Serdes.String()));'),
             "missing_call": ("remove", "streams.start();"),
             "wrong_order": ("move_after", "lines.mapValues(", "streams.start();")}},
]

PY_TEMPLATES = [
    {"template": "client_py_consumer_commit", "pair_with": "client_consumer_commit_sync", "source": PY_COMMIT, "domain": "kafka",
     "task": ("consumes the topic `{topic}` as consumer group `{group}` from `{bs}` with auto commit turned off; for every "
              "message it processes it and only then commits that message's offset synchronously."),
     "variants": [("orders", {"topic": "orders", "group": "billing", "bs": "kafka:9092"}),
                  ("clicks", {"topic": "page-clicks", "group": "analytics", "bs": "broker-1:9093"})],
     "anchors": CONSUMER_ANCHORS,
     "calls": [{"name": "subscribe", "class": "Consumer"}, {"name": "poll", "class": "Consumer"},
               {"name": "commit", "class": "Consumer", "kwargs": ["asynchronous"]}],
     "order": [{"name": "subscribe", "class": "Consumer"}, {"name": "poll", "class": "Consumer"},
               {"name": "commit", "class": "Consumer"}],
     "require_import": ["Consumer"],
     "config": {"keys": ["bootstrap.servers=%bs", "group.id=%group", "enable.auto.commit=false"]},
     "ops": {"wrong_signature": ("replace", "consumer.commit(message=msg, asynchronous=False)", "consumer.commit(message=msg, sync=True)"),
             "missing_call": ("remove", "consumer.commit("),
             "wrong_order": ("move_before", "consumer.commit(", "msg = consumer.poll("),
             "invented_method": ("replace", "consumer.commit(", "consumer.commit_sync("),
             "invented_key": ("replace", '"enable.auto.commit"', '"enable.auto.commits"'),
             "bad_import": ("replace", "from confluent_kafka import Consumer", "from confluent_kafka import Consumer, %s" % BAD_PY_NAME)}},
    {"template": "client_py_producer_idempotent", "pair_with": "client_producer_idempotent", "source": PY_IDEMPOTENT, "domain": "kafka",
     "task": ("sends {n} messages to the topic `{topic}` on `{bs}` with an idempotent producer (idempotence enabled "
              "explicitly, acks set to all), with a delivery callback on every message, and flushes before it exits."),
     "variants": [("ticks", {"topic": "ticks", "n": "100", "bs": "kafka:9092"}),
                  ("invoices", {"topic": "invoices", "n": "25", "bs": "broker-1:9093"})],
     "anchors": [anchor(KP, 198, 'props.put("bootstrap.servers"', "the producer is configured with bootstrap.servers"),
                 anchor(KP, 165, "From Kafka 3.0, the <code>enable.idempotence</code> configuration defaults to true.",
                        "enable.idempotence is the setting of the idempotent producer"),
                 anchor(KP, 167, "default to <code>all</code>", "idempotence goes with acks=all"),
                 anchor(KP, 1178, "callbacks passed to {@link #send(ProducerRecord,Callback)} have been called",
                        "flush completes the earlier sends and their callbacks")],
     "calls": [{"name": "produce", "class": "Producer", "kwargs": ["on_delivery"]}, {"name": "flush", "class": "Producer"}],
     "order": [{"name": "produce", "class": "Producer"}, {"name": "flush", "class": "Producer"}],
     "require_import": ["Producer"],
     "config": {"keys": ["bootstrap.servers=%bs", "enable.idempotence=true", "acks=all"]},
     "ops": {"wrong_signature": ("replace", "producer.flush()", "producer.flush(1.0, 2)"),
             "missing_call": ("remove", "producer.flush()"),
             "wrong_order": ("move_before", "producer.flush()", "for i in range("),
             "invented_method": ("replace", "producer.flush()", "producer.flush_all()"),
             "invented_key": ("replace", '"enable.idempotence"', '"enable.idempotences"'),
             "bad_import": ("replace", "from confluent_kafka import Producer", "from confluent_kafka import Producer, %s" % BAD_PY_NAME)}},
    {"template": "client_py_producer_transaction", "pair_with": "client_producer_transaction", "source": PY_TXN, "domain": "kafka",
     "task": ("sends {n} messages to the topic `{topic}` on `{bs}` in one transaction with the transactional id `{txid}`: it "
              "initialises transactions once, begins the transaction, produces the messages and commits; when a "
              "KafkaException is raised it aborts the transaction."),
     "variants": [("readings", {"topic": "readings", "txid": "reading-writer-1", "n": "10", "bs": "kafka:9092"}),
                  ("ledger", {"topic": "ledger-entries", "txid": "ledger-tx", "n": "50", "bs": "broker-2:9094"})],
     "anchors": [anchor(KP, 198, 'props.put("bootstrap.servers"', "the producer is configured with bootstrap.servers"),
                 anchor(KP, 199, 'props.put("transactional.id"', "a transactional producer sets transactional.id"),
                 anchor(KP, 202, "producer.initTransactions();", "init_transactions first"),
                 anchor(KP, 205, "producer.beginTransaction();", "begin_transaction after init_transactions"),
                 anchor(KP, 208, "producer.commitTransaction();", "commit_transaction after the sends"),
                 anchor(KP, 214, "producer.abortTransaction();", "abort_transaction on a KafkaException"),
                 anchor(KP, 635, "Needs to be called before any other methods when the {@code transactional.id} is set",
                        "init_transactions before any other method")],
     "calls": [{"name": "init_transactions", "class": "Producer"}, {"name": "begin_transaction", "class": "Producer"},
               {"name": "produce", "class": "Producer"}, {"name": "commit_transaction", "class": "Producer"},
               {"name": "abort_transaction", "class": "Producer"}],
     "order": [{"name": "init_transactions", "class": "Producer"}, {"name": "begin_transaction", "class": "Producer"},
               {"name": "produce", "class": "Producer"}, {"name": "commit_transaction", "class": "Producer"}],
     "require_import": ["Producer"],
     "config": {"keys": ["bootstrap.servers=%bs", "transactional.id=%txid"], "forbidden": {"enable.idempotence": "false"}},
     "ops": {"wrong_signature": ("replace", 'producer.produce("%topic", key=str(i), value="value-" + str(i))',
                                 'producer.produce("%topic", key=str(i), value="value-" + str(i), sync=True)'),
             "missing_call": ("replace", "producer.abort_transaction()", "pass"),
             "wrong_order": ("move_after", "producer.init_transactions()", "producer.begin_transaction()"),
             "invented_method": ("replace", "producer.begin_transaction()", "producer.start_transaction()"),
             "invented_key": ("replace", '"transactional.id"', '"transactional.ids"'),
             "bad_import": ("replace", "from confluent_kafka import KafkaException, Producer",
                            "from confluent_kafka import KafkaException, Producer, %s" % BAD_PY_NAME)}},
]

# The failing set the checks must give for each kind of perturbed program (a template may override it in "fails").
JAVA_FAILS = {"wrong_signature": ["java_calls"], "missing_call": ["java_call_order", "java_calls"],
              "wrong_order": ["java_call_order"], "invented_constant": ["java_compiles", "java_config"],
              "invented_key": ["java_config"], "bad_import": ["java_compiles"]}
JAVA_PRIMARY = {"wrong_signature": "java_calls", "missing_call": "java_calls", "wrong_order": "java_call_order",
                "invented_constant": "java_config", "invented_key": "java_config", "bad_import": "java_compiles"}
PY_FAILS = {"wrong_signature": ["py_calls"], "missing_call": ["py_calls"], "wrong_order": ["py_calls"],
            "invented_method": ["py_calls"], "invented_key": ["py_config"], "bad_import": ["py_imports"]}
PY_PRIMARY = dict(PY_FAILS, **{k: v[0] for k, v in PY_FAILS.items()})
JAVA_KINDS = ["wrong_signature", "missing_call", "wrong_order", "invented_constant", "invented_key", "bad_import"]
PY_KINDS = ["wrong_signature", "missing_call", "wrong_order", "invented_method", "invented_key", "bad_import"]

PHRASINGS = {
    "java": ["Write a Java program for Apache Kafka 4.3 that {task}",
             "I need a Java client program (kafka-clients 4.3) that {task}",
             "Using the Java client libraries of Apache Kafka 4.3, write a program that {task}"],
    "python": ["Write a Python script with the confluent-kafka client that {task}",
               "I need a Python program (confluent-kafka) that {task}",
               "Using confluent-kafka for Python, write a script that {task}"],
}


# ============================================================== filling and perturbing
def fill(text, params):
    for k, v in params.items():
        text = text.replace("%%" + k + "%%", str(v))
    assert "%%" not in text, "unfilled placeholder in %r" % text[:80]
    return text


def sub(text, params):
    """`%name` in ops, entries and config values."""
    for k in sorted(params, key=len, reverse=True):
        text = text.replace("%" + k, str(params[k]))
    return text


def _indent(line):
    return line[:len(line) - len(line.lstrip())]


def apply_op(src, op):
    kind = op[0]
    if kind == "replace":
        assert src.count(op[1]) == 1, "replace target not unique: %r" % op[1]
        return src.replace(op[1], op[2])
    lines = src.split("\n")
    ia = [i for i, l in enumerate(lines) if op[1] in l]
    assert len(ia) == 1, "%s: %r matches %d lines" % (kind, op[1], len(ia))
    moved = lines.pop(ia[0])
    if kind == "remove":
        return "\n".join(lines)
    ib = [i for i, l in enumerate(lines) if op[2] in l]
    assert len(ib) == 1, "%s: anchor %r matches %d lines" % (kind, op[2], len(ib))
    tgt = lines[ib[0]]
    moved = _indent(tgt) + moved.strip()
    lines.insert(ib[0] if kind == "move_before" else ib[0] + 1, moved)
    return "\n".join(lines)


def fence(code, lang):
    return "```%s\n%s\n```" % (lang, code.rstrip("\n"))


def parse_kv(s, params):
    k, v = sub(s, params).split("=", 1)
    return k, v


def make_checks(tpl, lang, variant_key, params):
    cfg = tpl["config"]
    kv = [parse_kv(s, params) for s in cfg["keys"] + cfg.get("variant_keys", {}).get(variant_key, [])]
    keys = dict(kv)
    config = {"type": "java_config" if lang == "java" else "py_config", "keys": keys}
    if cfg.get("forbidden"):
        config["forbidden"] = dict(cfg["forbidden"])
    if lang == "java":
        config["scope"] = list(cfg["scope"])
        entries = lambda es: [dict(e, method=sub(e["method"], params)) for e in es]  # noqa: E731
        return [{"type": "java_compiles", "format": True, "timeout": 30},
                {"type": "java_calls", "required": entries(tpl["calls"])},
                {"type": "java_call_order", "order": entries(tpl["order"])}, config]
    return [{"type": "py_syntax", "format": True},
            {"type": "py_imports", "require": list(tpl["require_import"])},
            {"type": "py_calls", "required": [dict(e) for e in tpl["calls"]], "order": [dict(e) for e in tpl["order"]]},
            config]


def probe_mutations(tpl, params):
    """Java: (constant expression, invented constant expression, typo key literal) of the template's probe key."""
    cls, const = tpl["probe"]
    fq = {"ConsumerConfig": P + "consumer.ConsumerConfig", "ProducerConfig": P + "producer.ProducerConfig",
          "AdminClientConfig": P + "admin.AdminClientConfig", "StreamsConfig": "org.apache.kafka.streams.StreamsConfig",
          "TopicConfig": "org.apache.kafka.common.config.TopicConfig"}[cls]
    c = catalogue_a3.java_constant(fq, const)
    if not c:
        raise AssertionError("%s.%s is not in the Java catalogue" % (cls, const))
    value = c
    bad_const = const[:-len("_CONFIG")] + "S_CONFIG"
    assert catalogue_a3.java_constant(fq, bad_const) is None, "%s.%s exists" % (cls, bad_const)
    typo = value + "s"
    return "%s.%s" % (cls, const), "%s.%s" % (cls, bad_const), '"%s"' % typo, value, typo


# ============================================================== one item
def build_item(tpl, lang, variant_key, params, n, rng, cats, chash, chash_a3, corpus, stats, verify):
    name = tpl["template"]
    fail = lambda why: stats.update(["derivation_failed:%s:%s: %s" % (name, variant_key, why)])  # noqa: E731
    # anchors: every cited phrase is on its cited line
    for a in tpl["anchors"]:
        ok, why = verify_anchor(a["file"], a["line"], a["phrase"])
        if not ok:
            fail("anchor: " + why)
            return None
    p = dict(params)
    src = fill(tpl["source"], p)
    checks = make_checks(tpl, lang, variant_key, p)
    # Java: the required classes, methods and config names exist in the catalogue (the checks run javac, but say it first)
    if lang == "java":
        for e in checks[1]["required"]:
            if e["method"] != "<init>" and not catalogue_a3.java_methods(e["class"], e["method"]):
                fail("%s.%s is not in the Java catalogue" % (e["class"], e["method"]))
                return None
        scopes = checks[3]["scope"]
        names = ck.java_config_names(scopes + ["common"])
        for k in checks[3]["keys"]:
            if k not in names:
                fail("config key %s is not in the Java catalogue scopes %s" % (k, scopes))
                return None
    else:
        props = set(catalogue_a3.py_config_properties())
        for k in checks[3]["keys"]:
            if props and k not in props:
                fail("config key %s is not a librdkafka property of the wheel" % k)
                return None
        for r in tpl["require_import"]:
            if r not in (catalogue_a3.py_module_names("confluent_kafka") or ()):
                fail("%s is not a name of confluent_kafka" % r)
                return None
        for e in tpl["calls"]:
            if e["name"] not in (catalogue_a3.py_class("confluent_kafka." + e["class"]) or {"members": {}})["members"]:
                fail("%s.%s is not in the wheel" % (e["class"], e["name"]))
                return None
    # the question
    tmpl = PHRASINGS[lang][rng.randrange(3)]
    core = tmpl.format(task=tpl["task"].format(**p))
    question = core + "\n\n" + (JAVA_NOTE if lang == "java" else PY_NOTE)
    import eval2_items_a1 as a1
    hit, dd = a1.near_duplicate(core, corpus)
    if hit:
        stats["dropped:near_duplicate:%s" % name] += 1
        return None
    # the reference
    ref = fence(src, lang)
    # the perturbed programs
    perts = []
    if lang == "java":
        const, bad_const, typo_lit, key_value, typo_key = probe_mutations(tpl, p)
        ops = {k: tuple(sub(x, p) for x in v) for k, v in tpl["ops"].items()}
        assert src.count(const) >= 1, "probe constant %s not in the skeleton" % const
        first = src.index(const)
        kinds, fails, primary = JAVA_KINDS, JAVA_FAILS, JAVA_PRIMARY
    else:
        ops = {k: tuple(sub(x, p) for x in v) for k, v in tpl["ops"].items()}
        kinds, fails, primary = PY_KINDS, PY_FAILS, PY_PRIMARY
    for kind in kinds:
        try:
            if lang == "java" and kind == "invented_constant":
                text = src[:first] + bad_const + src[first + len(const):]
            elif lang == "java" and kind == "invented_key":
                text = src[:first] + typo_lit + src[first + len(const):]
            elif lang == "java" and kind == "bad_import":
                m = list(re.finditer(r"^import [^\n]+;\n", src, re.M))[-1]
                text = src[:m.end()] + "import %s;\n" % BAD_JAVA_IMPORT + src[m.end():]
            else:
                text = apply_op(src, ops[kind])
        except AssertionError as e:
            fail("perturbation %s: %s" % (kind, e))
            return None
        assert text != src, "%s: perturbation %s changes nothing" % (name, kind)
        perts.append({"label": kind, "kind": kind, "text": fence(text, lang), "check": primary[kind],
                      "violates": next(i for i, c in enumerate(checks) if c["type"] == primary[kind]),
                      "fails": sorted(tpl.get("fails", {}).get(kind, fails[kind]))})
    item = {
        "id": "a3-%s-%03d" % (name, n), "tier": "A", "kind": KIND, "family": "%s:%s" % (name, variant_key),
        "domain": tpl["domain"], "language": lang, "question": question, "checks": checks, "reference": ref,
        "sources": [{"catalogue": "pinned_text", "entry": name, "fact": a["phrase"], "file": a["file"], "line": a["line"],
                     "requires": a["requires"]} for a in tpl["anchors"]] + [
            ({"catalogue": "java_api", "entry": "kafka-clients 4.3.2, kafka-streams 4.3.2",
              "fact": "every required class, method and *_CONFIG constant resolves in the jars", "file": None, "line": None}
             if lang == "java" else
             {"catalogue": "python_symbols", "entry": "confluent-kafka 2.16.1",
              "fact": "every imported name, called method, keyword and config property exists in the wheel",
              "file": None, "line": None})],
        "versions": {"kafka": "4.3"} if lang == "java" else {"kafka": "4.3", "confluent-kafka": "2.16.1"},
        "template": name, "anchored": True, "failure_class": "client_code", "exposed": False,
        "not_in_index": lang == "python", "pair": "client:%s:%s" % (tpl.get("pair_with", name), variant_key), "catalogue_hash": chash, "catalogue_hash_a3": chash_a3, "seed": None,
        "generator": VERSION, "dedup": dd, "chance": 0.0,
        "perturbed_reference": perts[0]["text"], "perturbed_violates": perts[0]["violates"], "perturbed": perts,
    }
    if verify:
        why = verify_item(item, cats)
        if why:
            fail(why)
            return None
    return item


def verify_item(item, cats):
    """The reference passes every check; each perturbed program fails exactly its declared set. None, or a reason."""
    res = sc.run_checks(item, item["reference"], cats)
    bad = [r for r in res if not r["pass"]]
    if bad:
        if any("java_template_rocksdb" in r["detail"] for r in bad):
            raise RuntimeError("%s: java_template_rocksdb: the template touches RocksDB types" % item["id"])
        return "the reference fails %s" % "; ".join("%s: %s" % (r["type"], r["detail"]) for r in bad)
    for pt in item["perturbed"]:
        res = sc.run_checks(item, pt["text"], cats)
        if any("java_template_rocksdb" in r["detail"] for r in res):
            raise RuntimeError("%s: java_template_rocksdb in a perturbed program" % item["id"])
        failed = sorted(r["type"] for r in res if not r["pass"])
        if failed != pt["fails"] or pt["check"] not in failed:
            return "perturbed %s fails %s, expected %s (%s)" % (
                pt["kind"], failed, pt["fails"], "; ".join(r["detail"] for r in res if not r["pass"])[:200])
    return None


# ------------------------------------------------------------------ near-duplicate corpus (the matcher of the A1 generator)
_CORPUS = None


def load_corpus():
    """Tokenised questions of eval/*.jsonl, work/v3/questions*.jsonl, data/{train,valid,test}.jsonl (the A1 corpus) and
    eval/m4/open/tier_a.jsonl (read only; nothing under eval/hidden/ is opened)."""
    global _CORPUS
    if _CORPUS is None:
        import eval2_items_a1 as a1
        corpus, _ = a1.corpus_texts()
        f = ROOT / "eval" / "m4" / "open" / "tier_a.jsonl"
        for line in f.read_text(errors="replace").splitlines():
            if line.strip():
                row = json.loads(line)
                if isinstance(row.get("question"), str):
                    corpus.append(("eval/m4/open/tier_a.jsonl", a1.tokens(row["question"])))
        _CORPUS = corpus
    return _CORPUS


# ------------------------------------------------------------------ generate
class _Memo:
    """Within one verification run, answer `list_calls` once per program text (the same result, three checks use it)."""

    def __enter__(self):
        self.orig = ck.list_calls
        cache = {}

        def memo(text, timeout=None):
            k = (text, timeout)
            if k not in cache:
                cache[k] = self.orig(text, timeout)
            return cache[k]
        ck.list_calls = memo
        return self

    def __exit__(self, *a):
        ck.list_calls = self.orig


def environment():
    """{"java": reason or None, "python": reason or None}: why items of a language cannot be built here."""
    out = {}
    jars, why = ck.jar_paths()
    out["java"] = why if why else (None if catalogue_a3.java_api() is not None else "java_api.json is not built")
    out["python"] = None if catalogue_a3.python_symbols() is not None else "python_symbols.json is not built"
    return out


def generate_with_stats(cats=None, seed=DEFAULT_SEED, corpus=None, verify=True, a3_dir=None):
    cats = cats or Catalogues.load()
    chash = catalogue_hash(default_dir())
    a3d = a3_dir or catalogue_a3.default_dir()        # only the recorded hash comes from here
    chash_a3 = catalogue_a3.manifest(a3d)["catalogue_hash"]
    corpus = load_corpus() if corpus is None else corpus
    env = environment()
    stats = Counter()
    items = []
    memo = _Memo() if verify else None
    if memo:
        memo.__enter__()
    try:
        for lang, templates in (("java", JAVA_TEMPLATES), ("python", PY_TEMPLATES)):
            if env[lang]:
                stats["derivation_failed:%s: %s" % (lang, env[lang])] += 1
                continue
            for tpl in templates:
                rng = random.Random("%s|%s" % (seed, tpl["template"]))   # not the catalogue hash: an unrelated catalogue change must not reshuffle
                n = 0
                for variant_key, params in tpl["variants"]:
                    n += 1
                    item = build_item(tpl, lang, variant_key, params, n, rng, cats, chash, chash_a3, corpus, stats, verify)
                    if item is None:
                        n -= 1
                        continue
                    item["seed"] = seed
                    items.append(item)
    finally:
        if memo:
            memo.__exit__()
    items.sort(key=lambda i: i["id"])
    stats["items"] = len(items)
    return items, stats


def generate(cats=None, seed=DEFAULT_SEED, verify=True, a3_dir=None):
    return generate_with_stats(cats, seed, verify=verify, a3_dir=a3_dir)[0]


def max_similarity(items):
    best = (0.0, 0.0, None, None)
    for it in items:
        d = it["dedup"]
        if max(d["max_jaccard"], d["max_containment"]) > max(best[0], best[1]):
            best = (d["max_jaccard"], d["max_containment"], d["nearest"], it["id"])
    return best


def main():
    items, stats = generate_with_stats()
    nj = sum(1 for i in items if i["language"] == "java")
    npy = sum(1 for i in items if i["language"] == "python")
    print("%s: %d Java items (plan %d), %d Python items (plan %d)" % (VERSION, nj, PLAN["java"], npy, PLAN["python"]))
    for k in sorted(stats):
        if k != "items":
            print("  %s x%d" % (k, stats[k]))
    if nj < PLAN["java"] or npy < PLAN["python"]:
        print("SHORTFALL: Java %d, Python %d (not padded)" % (PLAN["java"] - nj, PLAN["python"] - npy))
    print("highest similarity to an eval, training or open question: jaccard %.3f, containment %.3f (%s, item %s)" % max_similarity(items))
    print("families: %d; templates: %s" % (len({i["family"] for i in items}), sorted({i["template"] for i in items})))
    print("perturbed programs: %d" % sum(len(i["perturbed"]) for i in items))


if __name__ == "__main__":
    main()
