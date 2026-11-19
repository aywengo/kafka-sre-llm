#!/usr/bin/env python3
"""Assembly, split and manifest of the a3 set of tier A (WP1c T8).

Collects the items of the a3 generators (client code, valid but wrong, safe order, computed sizing, and the
not-covered generator when it exists), checks them against the 496 items of eval2_items_a1 (regenerated in memory,
never read from disk) and against every question text the repository holds, builds the mix and blind-guess tables,
marks the exposed items, splits whole groups to the open and the hidden side, and writes the manifest.

Nothing is written without --write. The default is a dry run: it prints counts, tables, maxima and the hashes the
real run would write, and writes no file. With --write the outputs go to --open-out, --hidden-out (encrypted with
eval2.encrypt_bytes) and --manifest-out; an existing output is never overwritten. The plain build (hidden half
included) only exists in memory; no plain hidden item is written anywhere.

Output is ids and counts only for the hidden half; the item text of a hidden item is never printed.

Split rule (version 2): the method of scripts/eval2.py split_families on the a3 items alone, with seed 20261119 (the
exposure seed label is m4-a3-2) and share 0.5, and an a3 item counts in the cells (tier, kind, template) and (tier,
kind), so that every template with two or more groups has items on both sides and each kind still splits evenly. Items that belong together (an item and its control, a task
and its restatement) carry a `pair` and form one group on one side; every other item is its own group. `split_extend`
keeps every id of an earlier split on its side when items are added later, and gives the split_families result for
items that are not a3 items. The manifest records the sha256 of the generator files (not a commit id) and `replaces`
(version 1).
"""
import argparse
import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import eval2  # noqa: E402
import eval2_items_a1 as a1  # noqa: E402

VERSION = "eval2_items_a3/2"
SEED = "m4-a3-2"          # exposure draw of version 2 (version 1 used m4-a3-1)
SPLIT_SEED = 20261119    # split draw of version 2 (version 1 used 20261118, the seed of the 496)
OLD_SPLIT_SEED = 20261118  # the seed of the 496: only for the proof that the 496 do not move
REPLACES = {"generator": "eval2_items_a3/1", "items_sha256": "d425cdada882b81d1124a6dc49d5d43b03ca7af6326882aa9a67e98eaf036ce3"}
GENERATOR_FILES = ("eval2_items_a3_client.py", "eval2_items_a3_vbw.py", "eval2_items_a3_order.py",
                   "eval2_items_a3_sizing.py", "eval2_items_a3_gap.py", "eval2_checks_a3.py", "eval2_checks_a3_client.py",
                   "javacheck/ListCalls.java")
SHARE = 0.5
EXPOSED_PER_KIND = 2
OPEN_PATH = "eval/m4/open/tier_a3.jsonl"
HIDDEN_PATH = "eval/hidden/tier_a3.jsonl.enc"
MANIFEST_PATH = "eval/m4/SPLIT_A3.json"
SPLIT_A1 = ROOT / "eval" / "m4" / "SPLIT.json"
OPEN_A1 = ROOT / "eval" / "m4" / "open" / "tier_a.jsonl"

# module, entry point; the order is the order of the build and of the output. client and vbw are keyword-compatible.
MODULES = ("client", "vbw", "order", "sizing", "gap")
KIND_OF = {"client": "client_code", "vbw": "valid_but_wrong", "order": "safe_order", "sizing": "computed_sizing",
           "gap": "not_covered"}
PLAN = {"client_code": 24, "valid_but_wrong": 32, "safe_order": 16, "computed_sizing": 24, "not_covered": 24}
MIN_KIND_ITEMS = 8        # items of a kind on each side, and families of a kind on each side
MIN_KIND_FAMILIES = 8
# Blind guess of an item that carries no `chance`: a control is passed by a blanket "works" or "not in my sources".
DEFAULT_GUESS = {"valid_control": 0.5, "not_covered": 0.5}
LOOKUP_FLOORS = ("near_miss_name", "scope", "confusable_neighbour", "value_type_trap")  # A1's floors on the lookup classes
EXPOSED_A3_TEXT = r'''
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "timeout": 30, "type": "java_compiles"}, {"required": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "subscribe", "signatures": [["java.util.Collection"], ["java.util.Collection", "org.apache.kafka.clients.consumer.ConsumerRebalanceListener"]]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll", "params": ["java.time.Duration"]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitSync", "signatures": [[], ["java.time.Duration"], ["java.util.Map"], ["java.util.Map", "java.time.Duration"]]}], "type": "java_calls"}, {"order": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "subscribe"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitSync"}], "type": "java_call_order"}, {"keys": {"bootstrap.servers": "kafka:9092", "enable.auto.commit": "false", "group.id": "billing"}, "scope": ["consumer"], "type": "java_config"}], "dedup": {"max_containment": 0.6, "max_jaccard": 0.214, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "client_code", "family": "client_consumer_commit_sync:orders", "generator": "eval2_items_a3_client/2", "id": "a3-client_consumer_commit_sync-v2-001", "kind": "client_code", "language": "java", "not_in_index": true, "pair": "client:client_consumer_commit_sync:orders", "perturbed": [{"check": "java_calls", "fails": ["java_call_order", "java_calls", "java_compiles"], "kind": "wrong_signature", "label": "wrong_signature", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(100);\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitSync();\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_calls", "fails": ["java_call_order", "java_calls"], "kind": "missing_call", "label": "missing_call", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_call_order", "fails": ["java_call_order"], "kind": "wrong_order", "label": "wrong_order", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                consumer.commitSync();\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n            }\n        }\n    }\n}\n```", "violates": 2}, {"check": "java_config", "fails": ["java_compiles", "java_config"], "kind": "invented_constant", "label": "invented_constant", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMITS_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitSync();\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_config", "fails": ["java_config"], "kind": "invented_key", "label": "invented_key", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(\"enable.auto.commits\", \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitSync();\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_compiles", "fails": ["java_compiles"], "kind": "bad_import", "label": "bad_import", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\nimport org.apache.kafka.clients.ClientFactory;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitSync();\n            }\n        }\n    }\n}\n```", "violates": 0}], "perturbed_reference": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(100);\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitSync();\n            }\n        }\n    }\n}\n```", "perturbed_violates": 1, "question": "I need a Java client program (kafka-clients 4.3) that consumes the topic `orders` as consumer group `billing` from `kafka:9092`. Offsets must not be committed automatically: after each poll it processes the records and only then commits the offsets synchronously.\n\nGive one complete Java source file in a single ```java code block. It must compile against kafka-clients 4.3.2 (and kafka-streams 4.3.2 where Streams is used) with no other library.", "reference": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class OrderReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"billing\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"orders\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitSync();\n            }\n        }\n    }\n}\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "props.setProperty(\"bootstrap.servers\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 221, "requires": "the client is configured with bootstrap.servers"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "props.setProperty(\"group.id\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 222, "requires": "a subscribing consumer sets group.id"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "props.setProperty(\"enable.auto.commit\", \"false\");", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 223, "requires": "enable.auto.commit=false for manual commits"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "consumer.subscribe(", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 227, "requires": "the consumer subscribes before it polls"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "consumer.commitSync();", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 237, "requires": "commitSync after the polled records are handled"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public void subscribe(Collection<String> topics) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 726, "requires": "accepted overload: subscribe(Collection) subscribes to the topics"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 700, "requires": "accepted overload: subscribe(Collection, ConsumerRebalanceListener) subscribes to the same topics and registers a listener"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public ConsumerRecords<K, V> poll(final Duration timeout) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 904, "requires": "accepted overload: poll(Duration) is the only poll in 4.3.2"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public void commitSync() {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 950, "requires": "accepted overload: commitSync() commits the offsets of the last poll, blocking"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public void commitSync(Duration timeout) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 995, "requires": "accepted overload: commitSync(Duration) is the same blocking commit with a timeout"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1049, "requires": "accepted overload: commitSync(Map) is a blocking commit of the given offsets"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1103, "requires": "accepted overload: commitSync(Map, Duration) is a blocking commit of the given offsets with a timeout"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_sync", "fact": "public ConsumerRecords<K, V> poll(final Duration timeout) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 904, "requires": "the wrong_signature program is wrong, consumer.poll(long): not in the 4.3.2 API: poll(Duration) is the only poll in 4.3.2"}, {"catalogue": "java_api", "entry": "kafka-clients 4.3.2, kafka-streams 4.3.2", "fact": "every required class, method and *_CONFIG constant resolves in the jars", "file": null, "line": null}], "template": "client_consumer_commit_sync", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "timeout": 30, "type": "java_compiles"}, {"required": [{"class": "org.apache.kafka.clients.producer.KafkaProducer", "method": "send", "params": ["org.apache.kafka.clients.producer.ProducerRecord", "org.apache.kafka.clients.producer.Callback"]}, {"class": "org.apache.kafka.clients.producer.KafkaProducer", "method": "flush", "params": []}], "type": "java_calls"}, {"order": [{"class": "org.apache.kafka.clients.producer.KafkaProducer", "method": "send"}, {"class": "org.apache.kafka.clients.producer.KafkaProducer", "method": "flush"}], "type": "java_call_order"}, {"keys": {"bootstrap.servers": "kafka:9092"}, "scope": ["producer"], "type": "java_config"}], "dedup": {"max_containment": 0.4, "max_jaccard": 0.182, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "client_code", "family": "client_producer_callback:events", "generator": "eval2_items_a3_client/2", "id": "a3-client_producer_callback-v2-001", "kind": "client_code", "language": "java", "not_in_index": true, "pair": "client:client_producer_callback:events", "perturbed": [{"check": "java_calls", "fails": ["java_calls"], "kind": "wrong_signature", "label": "wrong_signature", "text": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i));\n            }\n            producer.flush();\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_calls", "fails": ["java_call_order", "java_calls"], "kind": "missing_call", "label": "missing_call", "text": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i), (metadata, exception) -> report(exception));\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_call_order", "fails": ["java_call_order"], "kind": "wrong_order", "label": "wrong_order", "text": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            producer.flush();\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i), (metadata, exception) -> report(exception));\n            }\n        }\n    }\n}\n```", "violates": 2}, {"check": "java_config", "fails": ["java_compiles", "java_config"], "kind": "invented_constant", "label": "invented_constant", "text": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ProducerConfig.BOOTSTRAP_SERVERSS_CONFIG, \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i), (metadata, exception) -> report(exception));\n            }\n            producer.flush();\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_config", "fails": ["java_config"], "kind": "invented_key", "label": "invented_key", "text": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(\"bootstrap.serverss\", \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i), (metadata, exception) -> report(exception));\n            }\n            producer.flush();\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_compiles", "fails": ["java_compiles"], "kind": "bad_import", "label": "bad_import", "text": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\nimport org.apache.kafka.clients.ClientFactory;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i), (metadata, exception) -> report(exception));\n            }\n            producer.flush();\n        }\n    }\n}\n```", "violates": 0}], "perturbed_reference": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i));\n            }\n            producer.flush();\n        }\n    }\n}\n```", "perturbed_violates": 1, "question": "I need a Java client program (kafka-clients 4.3) that sends 20 records to the topic `events` on `kafka:9092` asynchronously, passing a callback to every send that reports a failed delivery, and flushes the producer before it exits.\n\nGive one complete Java source file in a single ```java code block. It must compile against kafka-clients 4.3.2 (and kafka-streams 4.3.2 where Streams is used) with no other library.", "reference": "```java\nimport java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\nimport org.apache.kafka.common.serialization.StringSerializer;\n\npublic class EventProducer {\n    static void report(Exception error) {\n        if (error != null) {\n            System.err.println(\"delivery failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());\n        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {\n            for (int i = 0; i < 20; i++) {\n                producer.send(new ProducerRecord<>(\"events\", \"key-\" + i, \"value-\" + i), (metadata, exception) -> report(exception));\n            }\n            producer.flush();\n        }\n    }\n}\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "client_producer_callback", "fact": "props.put(\"bootstrap.servers\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java", "line": 198, "requires": "the producer is configured with bootstrap.servers"}, {"catalogue": "pinned_text", "entry": "client_producer_callback", "fact": "Fully non-blocking usage can make use of the {@link Callback} parameter", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java", "line": 883, "requires": "send takes a Callback for asynchronous sends"}, {"catalogue": "pinned_text", "entry": "client_producer_callback", "fact": "callbacks passed to {@link #send(ProducerRecord,Callback)} have been called", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java", "line": 1178, "requires": "flush completes the earlier sends and their callbacks"}, {"catalogue": "java_api", "entry": "kafka-clients 4.3.2, kafka-streams 4.3.2", "fact": "every required class, method and *_CONFIG constant resolves in the jars", "file": null, "line": null}], "template": "client_producer_callback", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["does not work"], "norm": "ci", "type": "answer_equals"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaTopic", "type": "yaml_one_doc"}, {"kind": "KafkaTopic", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "payments"}, {"path": ["spec", "partitions"], "value": 6}], "kind": "KafkaTopic", "type": "yaml_fields_equal"}, {"oracle": "config_rule", "rule": "V6", "rules": [{"a": "min.insync.replicas", "b": {"path": "spec.replicas"}, "op": "lt"}, {"a": "min.insync.replicas", "b": {"const": 2}, "op": "ge"}], "type": "semantic_rule"}], "dedup": {"max_containment": 0.647, "max_jaccard": 0.19, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "valid_but_wrong", "family": "vbw_min_isr_replicas:payments:3", "generator": "eval2_items_a3_vbw/2", "id": "a3-vbw_min_isr_replicas-v2-001", "kind": "valid_but_wrong", "overlap": null, "overlap_band": "none", "pair": "vbw:V6:a", "perturbed_reference": "The artifact does what is wanted.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: payments\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.insync.replicas: 3\n```\nANSWER: works", "perturbed_violates": 1, "question": "Producers write to the topic `payments` of the cluster `events` with `acks=all`; at least 2 replicas must hold every acknowledged write, and writes must keep succeeding while one broker is down. Does this `KafkaTopic` do that?\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: payments\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.insync.replicas: 3\n```\n\nFinish with a final line `ANSWER: works` or `ANSWER: does not work`. If it does not work, give the corrected manifest in one fenced code block before that line and change only what is needed.", "reference": "With `min.insync.replicas` equal to `replicas` (3), one lost replica leaves the in-sync set below the minimum and `acks=all` writes fail.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: payments\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.insync.replicas: 2\n```\nANSWER: does not work", "seed": "m4-a3-vbw-1", "sources": [{"catalogue": "catalogues_a3", "entry": "V6", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 521, "phrase": "If <code>acks=all</code> and the current ISR set contains fewer than <code>min.insync.replicas</code> members, then the producer will raise an exception"}, {"catalogue": "catalogues_a3", "entry": "V6", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 521, "phrase": "A typical scenario would be to create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2"}, {"catalogue": "catalogues_a3", "entry": "V6", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 246, "phrase": "If <code>acks=all</code> and the current ISR set contains fewer than <code>min.insync.replicas</code> members, then the producer will raise an exception"}, {"catalogue": "kafka_config", "entry": "min.insync.replicas (topic)", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 245, "phrase": "min.insync.replicas"}], "template": "vbw_min_isr_replicas", "tier": "A", "vbw": {"artifact_language": "yaml", "control_artifact": "apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: payments\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.insync.replicas: 2", "mechanism": "V6", "oracle_checks": [5], "oracle_checks_on_control": [{"oracle": "config_rule", "rule": "V6", "rules": [{"a": "min.insync.replicas", "b": {"path": "spec.replicas"}, "op": "lt"}, {"a": "min.insync.replicas", "b": {"const": 2}, "op": "ge"}], "type": "semantic_rule"}], "role": "wrong", "rules": ["config.min_isr_below_replicas"], "shown_fails_oracle": true, "shown_valid": true, "shown_valid_note": "validates against the crd schema"}, "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["does not work"], "norm": "ci", "type": "answer_equals"}, {"api_version": "policy/v1", "kind": "PodDisruptionBudget", "type": "yaml_one_doc"}, {"kind": "PodDisruptionBudget", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "orders-kafka-pdb"}, {"path": ["spec", "selector"], "value": {"matchLabels": {"app": "orders-kafka"}}}], "kind": "PodDisruptionBudget", "type": "yaml_fields_equal"}, {"allowed": 1, "expect": true, "oracle": "pdb_evicts", "replicas": 3, "rule": "V7", "type": "semantic_rule"}], "dedup": {"max_containment": 0.641, "max_jaccard": 0.417, "nearest": "eval/m4/open/tier_a.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "valid_but_wrong", "family": "vbw_pdb_no_eviction:max0:orders-kafka", "generator": "eval2_items_a3_vbw/2", "id": "a3-vbw_pdb_no_eviction-v2-001", "kind": "valid_but_wrong", "overlap": null, "overlap_band": "none", "pair": "vbw:V7:a", "perturbed_reference": "The artifact does what is wanted.\n\n```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: orders-kafka-pdb\n  namespace: kafka\nspec:\n  maxUnavailable: 0\n  selector:\n    matchLabels:\n      app: orders-kafka\n```\nANSWER: works", "perturbed_violates": 1, "question": "The StatefulSet `orders-kafka` runs 3 broker pods. Node drains must be able to evict one broker pod at a time. Does this PodDisruptionBudget allow that?\n\n```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: orders-kafka-pdb\n  namespace: kafka\nspec:\n  maxUnavailable: 0\n  selector:\n    matchLabels:\n      app: orders-kafka\n```\n\nFinish with a final line `ANSWER: works` or `ANSWER: does not work`. If it does not work, give the corrected manifest in one fenced code block before that line and change only what is needed.", "reference": "The budget requires zero voluntary evictions ({\"maxUnavailable\": 0}), so a node drain cannot evict a broker pod.\n\n```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: orders-kafka-pdb\n  namespace: kafka\nspec:\n  maxUnavailable: 1\n  selector:\n    matchLabels:\n      app: orders-kafka\n```\nANSWER: does not work", "seed": "m4-a3-vbw-1", "sources": [{"catalogue": "catalogues_a3", "entry": "V7", "file": "raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md", "line": 155, "phrase": "If you set `maxUnavailable` to 0% or 0, or you set `minAvailable` to 100% or the number of replicas,"}, {"catalogue": "catalogues_a3", "entry": "V7", "file": "raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md", "line": 156, "phrase": "you are requiring zero voluntary evictions. When you set zero voluntary evictions for a workload"}, {"catalogue": "catalogues_a3", "entry": "V7", "file": "raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md", "line": 157, "phrase": "object such as ReplicaSet, then you cannot successfully drain a Node running one of those Pods."}, {"catalogue": "catalogues_a3", "entry": "V7", "file": "raw/kubernetes/content/en/docs/tasks/run-application/configure-pdb.md", "line": 123, "phrase": "You can specify only one of `maxUnavailable` and `minAvailable` in a single `PodDisruptionBudget`."}], "template": "vbw_pdb_no_eviction", "tier": "A", "vbw": {"artifact_language": "yaml", "control_artifact": "apiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: orders-kafka-pdb\n  namespace: kafka\nspec:\n  maxUnavailable: 1\n  selector:\n    matchLabels:\n      app: orders-kafka", "mechanism": "V7", "oracle_checks": [5], "oracle_checks_on_control": [{"allowed": 1, "expect": true, "oracle": "pdb_evicts", "replicas": 3, "rule": "V7", "type": "semantic_rule"}], "role": "wrong", "rules": ["pdb.zero_evictions"], "shown_fails_oracle": true, "shown_valid": true, "shown_valid_note": "validates against the openapi schema"}, "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "has_command"}, {"steps": [{"flags_all": ["--dry-run"], "flags_forbidden": [], "role": "safe", "tool": "argocd", "words": ["app", "sync"]}, {"flags_forbidden": [], "role": "safe", "tool": "argocd", "words": ["app", "diff"]}, {"flags_forbidden": ["--dry-run"], "flags_forbidden_true": ["--prune"], "role": "mutate", "tool": "argocd", "words": ["app", "sync"]}], "type": "cmd_order"}, {"tool": "argocd", "type": "no_invented_cli"}], "dedup": {"max_containment": 0.6, "max_jaccard": 0.25, "nearest": "data/train.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "safe_order", "family": "order_argo_sync:argocd-app-sync:billing", "generator": "eval2_items_a3_order/2", "id": "a3-order_argo_sync-101", "kind": "safe_order", "perturbed": [{"check": "cmd_order", "label": "swap", "text": "```bash\nargocd app sync billing\nargocd app sync billing --dry-run\n```", "violates": 1}, {"check": "cmd_order", "label": "missing", "text": "```bash\nargocd app sync billing\n```", "violates": 1}, {"check": "cmd_order", "label": "forbidden", "text": "```bash\nargocd app sync billing --dry-run\nargocd app sync billing --prune\n```", "violates": 1}], "perturbed_reference": "```bash\nargocd app sync billing\nargocd app sync billing --dry-run\n```", "perturbed_violates": 1, "question": "I need to sync the Argo CD application `billing`, after checking what the sync would do without changing the cluster. Do not prune resources. What exact commands do I run, and in what order? `argocd` is already logged in.\n\nGive the commands in the order they should be run, in one code block, one command per line; use only options that exist for the tool.", "reference": "```bash\nargocd app sync billing --dry-run\nargocd app sync billing\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "order_argo_sync", "fact": "Preview apply without affecting cluster", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_sync.md", "line": 49}, {"catalogue": "pinned_text", "entry": "order_argo_sync", "fact": "Perform a diff against the target and live state.", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_diff.md", "line": 5}, {"catalogue": "argocd", "entry": "argocd app sync", "fact": "every option on the lines exists for the tool: --dry-run, --prune", "file": null, "line": null}], "template": "order_argo_sync", "tier": "A", "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "has_kafka_command"}, {"steps": [{"flags_all": ["--reset-offsets", "--dry-run"], "flags_forbidden": [], "role": "safe", "tool": "kafka-share-groups.sh"}, {"flags_all": ["--reset-offsets", "--execute"], "flags_forbidden": ["--all-topics"], "role": "mutate", "tool": "kafka-share-groups.sh"}], "type": "cmd_order"}, {"tool": "kafka-share-groups.sh", "type": "kcmd_known_flags"}, {"tool": "kafka-share-groups.sh", "type": "kcmd_constraints"}], "dedup": {"max_containment": 0.625, "max_jaccard": 0.208, "nearest": "data/valid.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "safe_order", "family": "order_shg_reset:kafka-share-groups:my-share-group", "generator": "eval2_items_a3_order/2", "id": "a3-order_shg_reset-001", "kind": "safe_order", "perturbed": [{"check": "cmd_order", "label": "swap", "text": "```bash\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --execute\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --dry-run\n```", "violates": 1}, {"check": "cmd_order", "label": "missing", "text": "```bash\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --execute\n```", "violates": 1}, {"check": "cmd_order", "label": "forbidden", "text": "```bash\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --dry-run\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --execute --all-topics\n```", "violates": 1}], "perturbed_reference": "```bash\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --execute\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --dry-run\n```", "perturbed_violates": 1, "question": "I need to reset the offsets of the share group `my-share-group` on the topic `topic1` to the earliest offset. Only the topic `topic1` may be reset, not all topics of the group. What exact commands do I run, and in what order? Kafka 4.3, bootstrap server `kafka:9092`.\n\nGive the commands in the order they should be run, in one code block, one command per line; use only options that exist for the tool.", "reference": "```bash\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --dry-run\nbin/kafka-share-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group my-share-group --topic topic1 --to-earliest --execute\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "order_shg_reset", "fact": "--dry-run: to display which offsets to reset", "file": "raw/apache-kafka/docs/operations/basic-kafka-operations.md", "line": 302}, {"catalogue": "pinned_text", "entry": "order_shg_reset", "fact": "to execute --reset-offsets process", "file": "raw/apache-kafka/docs/operations/basic-kafka-operations.md", "line": 303}, {"catalogue": "kafka_cli", "entry": "kafka-share-groups.sh", "fact": "every option on the lines exists for the tool: --reset-offsets, --group, --topic, --to-earliest, --dry-run, --execute, --all-topics", "file": null, "line": null}], "template": "order_shg_reset", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"tolerance": 0.01, "type": "number_within", "unit": "GiB", "value": 632.8125}], "dedup": {"max_containment": 0.417, "max_jaccard": 0.143, "nearest": "eval/answers_1024_copy.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "arith_only", "family": "size_disk_rf:5-3-3-6", "generator": "eval2_items_a3_sizing/2", "id": "a3-size_disk_rf-001", "kind": "computed_sizing", "perturbed": [{"check": "number_within", "kind": "unit", "label": "GB against GiB", "text": "Worked out with the stated rule.\nANSWER: 632.8 GB", "violates": 1}, {"check": "number_within", "kind": "unit", "label": "MiB against GiB", "text": "Worked out with the stated rule.\nANSWER: 632.8 MiB", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "forgot the replication factor", "text": "Worked out with the stated rule.\nANSWER: 210.9 GiB", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "forgot to divide by the brokers", "text": "Worked out with the stated rule.\nANSWER: 3797 GiB", "violates": 1}], "perturbed_reference": "Worked out with the stated rule.\nANSWER: 632.8 GB", "perturbed_violates": 1, "question": "A topic takes 5 MiB/s of producer traffic and keeps its data for 3 days. Its replication factor is 3 and its partitions are spread evenly over 6 brokers. How much disk does one broker use for this topic's retained data, in GiB? Use: disk per broker = write rate x retention x replication factor / number of brokers; ignore indexes and any overhead.\n\nFinish with a final line `ANSWER: <number> <unit>`.", "reference": "Worked out with the stated rule.\nANSWER: 632.8 GiB", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "size_disk_rf", "fact": "more storage space as well (3x with the replication factor of 3)", "file": "raw/apache-kafka/docs/streams/developer-guide/config-streams.md", "line": 1636}], "template": "size_disk_rf", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"tolerance": 0, "type": "number_within", "unit": "ms", "value": 30000}], "dedup": {"max_containment": 0.625, "max_jaccard": 0.208, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "rule_from_docs", "family": "size_heartbeat:90000", "generator": "eval2_items_a3_sizing/2", "id": "a3-size_heartbeat-003", "kind": "computed_sizing", "perturbed": [{"check": "number_within", "kind": "unit", "label": "ms against s", "text": "Worked out with the stated rule.\nANSWER: 30000 s", "violates": 1}, {"check": "number_within", "kind": "unit", "label": "seconds written as ms", "text": "Worked out with the stated rule.\nANSWER: 30 ms", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "session timeout itself", "text": "Worked out with the stated rule.\nANSWER: 90000 ms", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "two thirds instead of one third", "text": "Worked out with the stated rule.\nANSWER: 60000 ms", "violates": 1}], "perturbed_reference": "Worked out with the stated rule.\nANSWER: 30000 s", "perturbed_violates": 1, "question": "Quick sizing question. A classic-protocol consumer group uses `session.timeout.ms` = 90000. What is the highest `heartbeat.interval.ms` the Kafka documentation says you should typically set, in milliseconds?\n\nFinish with a final line `ANSWER: <number> <unit>`.", "reference": "Worked out with the stated rule.\nANSWER: 30000 ms", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "size_heartbeat", "fact": "typically should be set no higher than 1/3 of that value", "file": "raw/apache-kafka-config/content/en/43/generated/consumer_config.html", "line": 64}], "template": "size_heartbeat", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 1.0, "checks": [{"format": true, "type": "answer_line"}, {"type": "declines"}, {"allowed_identifiers": ["offsets.commit.required.acks"], "allowed_numbers": ["3.9"], "asked": ["offsets.commit.required.acks"], "default_numbers": [], "pinned_versions": ["4.1", "4.2", "4.3", "4.3.2"], "type": "no_invented_value"}], "dedup": {"max_containment": 0.556, "max_jaccard": 0.217, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "not_covered", "family": "nc_old_version:gap:offsets.commit.required.acks", "generator": "eval2_items_a3_gap/2", "id": "a3-nc_old_version-101", "kind": "not_covered", "overlap": null, "overlap_band": "none", "pair": "nc:old_version:offsets.commit.required.acks", "perturbed": [{"check": "no_invented_value", "label": "unsupported_value", "text": "The default is -1.\nANSWER: -1", "violates": 2}, {"check": "declines", "label": "confident_no_decline", "text": "That setting has a sensible built-in value that applies unless it is overridden.\nANSWER: the built-in value", "violates": 1}], "perturbed_reference": "The default is -1.\nANSWER: -1", "perturbed_violates": 2, "question": "Kafka 3.9: what was the default of the broker setting `offsets.commit.required.acks` in that release?\n\nFinish with a final line `ANSWER: <value>`. Give the bare value. If the documentation you have does not cover it, finish with `ANSWER: not in my sources`.", "reference": "The documentation I have does not cover `offsets.commit.required.acks` for Kafka 3.9, so I cannot give the default it had then.\nANSWER: not in my sources", "seed": "m4-a3-1", "sources": [{"absent": "offsets.commit.required.acks", "catalogue": "frozen_index", "chunks_searched": 6444, "fact": "no chunk of the frozen index mentions the terms", "file": "work/v3/chunks_s1.jsonl", "hit_rule": "a chunk hits a term when the term is in its text, title or path exactly, ignoring case, or after dropping every non-alphanumeric character from both (all chunks of every source)", "hits": 0, "hits_per_term": {"offsets.commit.required.acks": {"case_insensitive": 1, "exact": 1, "separator_insensitive": 1}}, "index_sha256": "233c23b0478006a8afae881b83836a226a6af89517256be4d9aac2cc7d500e92", "line": null, "premise": {"chunk_ids": ["cec1fb84292e3113"], "file": "raw/apache-kafka/docs/getting-started/upgrade.md", "hits_in_premise_file": 3, "line": 266, "phrase": "The `offsets.commit.required.acks` configuration was removed", "rule": "the key is in the removal note of the pinned upgrade notes, which shows it existed in 3.9; no other chunk mentions it and the sentences naming it state no default"}, "terms": ["offsets.commit.required.acks"]}, {"absent": "offsets.commit.required.acks", "catalogue": "kafka_config", "entry": null, "fact": "the key is not in the Kafka 4.3 reference of scope broker", "file": null, "line": null, "scope": "broker"}], "template": "nc_old_version", "tier": "A", "versions": {"kafka": "3.9"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["5000", "5000 (5 seconds)", "5 seconds", "5000 ms", "5000 milliseconds"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.7, "max_jaccard": 0.212, "nearest": "work/v3/questions.rejects.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "plain_lookup", "family": "nc_old_version:ctl:offset.flush.timeout.ms", "generator": "eval2_items_a3_gap/2", "id": "a3-nc_old_version-108", "kind": "not_covered", "overlap": null, "overlap_band": "none", "pair": "nc:old_version:add.source.alias.to.metrics", "perturbed": [{"check": "answer_equals", "label": "decline", "text": "The documentation I have does not cover this.\nANSWER: not in my sources", "violates": 1}, {"check": "answer_equals", "label": "wrong_value", "text": "The default of `offset.flush.timeout.ms` is 10000.\nANSWER: 10000", "violates": 1}], "perturbed_reference": "The documentation I have does not cover this.\nANSWER: not in my sources", "perturbed_violates": 1, "question": "Kafka 4.3: what is the default of the Connect worker setting `offset.flush.timeout.ms` in that release?\n\nFinish with a final line `ANSWER: <value>`. Give the bare value. If the documentation you have does not cover it, finish with `ANSWER: not in my sources`.", "reference": "The default of `offset.flush.timeout.ms` is 5000.\nANSWER: 5000", "seed": "m4-a3-1", "sources": [{"catalogue": "kafka_config", "entry": "connect/offset.flush.timeout.ms", "fact": "default 5000 (5 seconds)", "file": "raw/apache-kafka-config/content/en/43/generated/connect_config.html", "line": 813}, {"catalogue": "frozen_index", "chunk_id": "2a2b4e652803e6a7", "fact": "chunk holds the key heading and its Default line", "file": "raw/apache-kafka-config/content/en/43/generated/connect_config.html", "index_sha256": "233c23b0478006a8afae881b83836a226a6af89517256be4d9aac2cc7d500e92", "line": null}], "template": "nc_old_version", "tier": "A", "versions": {"kafka": "4.3"}}
'''  # the exposed rows, embedded verbatim after the first real build (empty until then), one JSON object per line


class AssemblyError(RuntimeError):
    pass


# ------------------------------------------------------------------ generators
def _quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


def _load(name):
    import importlib
    return importlib.import_module("eval2_items_a3_" + name)


def collect(allow_missing=(), verify=False):
    """(items, notes): the items of every generator module present, in MODULES order, each sorted by id."""
    items, notes = [], []
    for name in MODULES:
        try:
            mod = _load(name)
        except ModuleNotFoundError as e:
            if e.name != "eval2_items_a3_" + name:
                raise
            if name not in allow_missing:
                raise AssemblyError("generator module eval2_items_a3_%s is absent; pass --allow-missing %s to build "
                                    "without it" % (name, name))
            notes.append("generator %s is absent (--allow-missing %s): kind %s is not in this build" % (name, name, KIND_OF[name]))
            continue
        part = _quiet(mod.generate, verify=verify) if name == "client" else _quiet(mod.generate)
        part = sorted(part, key=lambda i: i["id"])
        for it in part:
            if it.get("kind") != KIND_OF[name]:
                raise AssemblyError("%s: item %s has kind %r, expected %r" % (name, it.get("id"), it.get("kind"), KIND_OF[name]))
        items += part
    return items, notes


def serialise(items):
    return "".join(json.dumps(i, sort_keys=True, ensure_ascii=False) + "\n" for i in items)


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ predecessor
def predecessor():
    """The 496 items of eval2_items_a1, regenerated in memory, and the generator object (for its guess tables).
    Raises when their sha256 differs from items_sha256 of SPLIT.json."""
    from kafka_sre_rag.catalogue import Catalogues, catalogue_hash, default_dir
    manifest = json.loads(SPLIT_A1.read_text())
    cats = Catalogues.load()
    chash = catalogue_hash(default_dir())
    g = _quiet(a1.generate, cats, a1.DEFAULT_SEED, chash)
    got = sha(serialise(g.items))
    if got != manifest["items_sha256"]:
        raise AssemblyError("the 496 regenerated in memory have sha256 %s, SPLIT.json records %s" % (got, manifest["items_sha256"]))
    if chash != manifest["catalogue_hash"]:
        raise AssemblyError("catalogue hash %s differs from SPLIT.json (%s)" % (chash, manifest["catalogue_hash"]))
    return g, manifest


def check_ids(a3, old_ids):
    ids = [i["id"] for i in a3]
    dup = [k for k, n in Counter(ids).items() if n > 1]
    if dup:
        raise AssemblyError("duplicate a3 ids: %s" % dup[:5])
    bad = [i for i in ids if not i.startswith("a3-")]
    if bad:
        raise AssemblyError("a3 ids must start with 'a3-': %s" % bad[:5])
    clash = sorted(set(ids) & set(old_ids))
    if clash:
        raise AssemblyError("a3 ids collide with existing ids: %s" % clash[:5])
    eval2.validate_items(a3)


# ------------------------------------------------------------------ near duplicates
NOTE_STARTS = ("Give the commands in the order", "Finish with a final line", "Give one complete")  # format notes of the generators


def core_of(it):
    """The question without its closing format note (the generators compare the core, as A1 does)."""
    head, sep, last = it["question"].rpartition("\n\n")
    return head if sep and last.startswith(NOTE_STARTS) else it["question"]


def _source_of(rel):
    if rel.startswith("eval/m4/open/"):
        return "open tier_a.jsonl"
    if rel.startswith("eval/"):
        return "eval/*.jsonl"
    if rel.startswith("data/"):
        return "data/*.jsonl"
    if rel.startswith("work/"):
        return "work/v3 questions"
    return rel


def near_duplicates(a3, old_items):
    """(report, hits). report: per source group the highest (jaccard, containment, item id, nearest file) of any
    a3 question against that group; the groups are the eval files, the open tier_a.jsonl, the training data, the 496
    regenerated (open and hidden, as one group, ids not shown), and the a3 items among themselves (other items only).
    hits: the (a3 id, group, jaccard, containment) over the plan's thresholds (Jaccard 0.5, containment 0.8, at least
    8 tokens) against anything outside a3; they stop the build. pairs: a3 items of different templates over the same
    thresholds (a, b, jaccard, containment); reported as a warning, because the generators were never asked to avoid
    them (a Java task and its Python restatement are such a pair by design). The a3 items of one template are
    parameter variants and are only reported (highest value)."""
    corpus, files = a1.corpus_texts()
    seen = {tuple(sorted(tk)) for _, tk in corpus}
    for line in OPEN_A1.read_text().splitlines():
        if line.strip():
            tk = a1.tokens(json.loads(line)["question"])
            if tuple(sorted(tk)) not in seen:
                corpus.append(("eval/m4/open/tier_a.jsonl", tk))
    groups = defaultdict(list)
    for rel, tk in corpus:
        groups[_source_of(rel)].append((rel, tk))
    groups["the 496 (open and hidden)"] = [("a1", a1.tokens(i["question"])) for i in old_items]
    toks = {i["id"]: a1.tokens(core_of(i)) for i in a3}
    report, hits, pairs = {}, [], set()
    for gname in sorted(groups):
        best = (0.0, 0.0, None, None)
        for it in a3:
            hit, dd = a1.near_duplicate(core_of(it), groups[gname])
            if max(dd["max_jaccard"], dd["max_containment"]) > max(best[0], best[1]):
                best = (dd["max_jaccard"], dd["max_containment"], it["id"], dd["nearest"])
            if hit:
                hits.append((it["id"], gname, dd["max_jaccard"], dd["max_containment"]))
        report[gname] = best
    # the a3 items among themselves: items of one template are parameter variants of one wording and are reported
    # only; items of different templates must not be near-duplicates (a failure)
    for label, same in (("a3 items, other templates", False), ("a3 items, same template (reported)", True)):
        best = (0.0, 0.0, None, None)
        for it in a3:
            others = [(o["id"], toks[o["id"]]) for o in a3
                      if o["id"] != it["id"] and (o["template"] == it["template"]) == same]
            hit, dd = a1.near_duplicate(core_of(it), others)
            if max(dd["max_jaccard"], dd["max_containment"]) > max(best[0], best[1]):
                best = (dd["max_jaccard"], dd["max_containment"], it["id"], dd["nearest"])
            if hit and not same:
                pairs.add(tuple(sorted((it["id"], dd["nearest"]))) + (dd["max_jaccard"], dd["max_containment"]))
        report[label] = best
    return report, sorted(hits), sorted(pairs), len(corpus)


# ------------------------------------------------------------------ mix and guess
def item_guess(it):
    c = it.get("chance")
    if c is None:
        c = DEFAULT_GUESS.get(it["failure_class"], DEFAULT_GUESS.get(it["kind"], 0.0))
    return float(c)


def guess_table(a3, old_g=None):
    """(lines, a3_overall, union_overall); the union uses A1's own guess of the 496 (kept modal shares, chances)."""
    n = len(a3)
    gs = [item_guess(i) for i in a3]
    lines = ["score of a blind guess (chance over the listed options; a control is passed by the blanket answer: %s):" % (
        ", ".join("%s %.2f" % kv for kv in sorted(DEFAULT_GUESS.items())))]
    by = defaultdict(list)
    for it, g in zip(a3, gs):
        by[it["kind"]].append(g)
    for k in sorted(by):
        lines.append("  %-18s %3d items  guess %5.1f %%" % (k, len(by[k]), 100.0 * sum(by[k]) / len(by[k])))
    overall = sum(gs) / n if n else 0.0
    lines.append("  a3 alone: overall guess over %d items: %.1f %% (limit %.0f %%)  %s" % (
        n, 100 * overall, 100 * a1.MAX_GUESS, "met" if overall < a1.MAX_GUESS else "NOT MET"))
    union = overall
    if old_g is not None:
        km = a1.kept_modal_shares(old_g.items)
        og = [a1.item_guess(old_g, i, km) for i in old_g.items]
        union = (sum(og) + sum(gs)) / (len(og) + n)
        lines.append("  union (%d + %d = %d items): overall guess %.1f %% (limit %.0f %%, reported)  %s" % (
            len(og), n, len(og) + n, 100 * union, 100 * a1.MAX_GUESS, "met" if union < a1.MAX_GUESS else "NOT MET"))
    return lines, overall, union


def mix_lines(a3, old_items):
    """Mix of the failure classes against A1's targets on the a3 build alone and on the union (reported)."""
    out = ["mix against the targets (a3 alone: floors that apply; union: reported, not forced):"]
    problems = []
    for title, items in (("a3 alone", a3), ("union", list(old_items) + list(a3))):
        n = len(items)
        grp = Counter(a1.mix_group(i["failure_class"]) for i in items)
        plain = grp["plain_lookup"] / n
        out.append("  %s (%d items)" % (title, n))
        ok = plain <= a1.MAX_PLAIN
        out.append("    %-34s %3d  %5.1f %%  (at most %.0f %%)  %s" % ("plain lookup", grp["plain_lookup"], 100 * plain,
                                                                      100 * a1.MAX_PLAIN, "met" if ok else "NOT MET"))
        if title == "a3 alone" and not ok:
            problems.append("plain lookup share %.1f %% above %.0f %%" % (100 * plain, 100 * a1.MAX_PLAIN))
        for k, lo in a1.MIX_MIN.items():
            share = grp[k] / n
            if title == "a3 alone":
                state = "not applicable: the a3 kinds have no %s items by construction" % a1.MIX_NAMES[k]
            else:
                state = "met" if share >= lo else "NOT MET"
            out.append("    %-34s %3d  %5.1f %%  (at least %.0f %%)  %s" % (a1.MIX_NAMES[k], grp[k], 100 * share, 100 * lo, state))
        new = sum(1 for i in items if i["kind"] in a1.NEW_KINDS or i["kind"] in PLAN)
        out.append("    new kinds (A2 kinds and a3 kinds) %d of %d = %.1f %% (at least %.0f %%)  %s" % (
            new, n, 100.0 * new / n, 100 * a1.NEW_KIND_SHARE, "met" if new >= a1.NEW_KIND_SHARE * n else "NOT MET"))
        real = sum(1 for i in items if i["kind"] == "is_real")
        out.append("    is_real %d of %d = %.1f %% (at most %.0f %%)  %s" % (real, n, 100.0 * real / n, 100 * a1.REAL_SHARE,
                                                                           "met" if real <= a1.REAL_SHARE * n else "NOT MET"))
        if title == "a3 alone" and real > a1.REAL_SHARE * n:
            problems.append("is_real share above %.0f %%" % (100 * a1.REAL_SHARE))
    return out, problems


def floor_problems(a3, allow_missing):
    """Floors of the plan on the a3 build alone: items and families per kind, items per template."""
    out = []
    for kind in PLAN:
        if kind == "not_covered" and "gap" in allow_missing:
            continue
        its = [i for i in a3 if i["kind"] == kind]
        if len(its) < MIN_KIND_ITEMS * 2:
            out.append("%s: %d items, at least %d needed" % (kind, len(its), MIN_KIND_ITEMS * 2))
        fams = {i["family"] for i in its}
        if len(fams) < MIN_KIND_FAMILIES:
            out.append("%s: %d families, at least %d needed" % (kind, len(fams), MIN_KIND_FAMILIES))
    for t, n in sorted(Counter(i["template"] for i in a3).items()):
        if n > a1.MAX_PER_TEMPLATE:
            out.append("template %s: %d items, at most %d" % (t, n, a1.MAX_PER_TEMPLATE))
    return out


# ------------------------------------------------------------------ exposure
def exposure_key(iid):
    return hashlib.sha256(("%s|exposed|%s" % (SEED, iid)).encode()).hexdigest()


def group_of(it):
    """The split group of an item: items that belong together (an item and its control, a task and its restatement)
    share a `pair` and one side; every other item is its own group (its family)."""
    return "pair:%s" % it["pair"] if it.get("pair") else it["family"]


def cells_of(it):
    """The cells whose share is reached. An a3 item counts in (tier, kind, template) and in (tier, kind), so that every
    template with two or more groups has items on both sides and the kind still splits evenly; any other item (the 496)
    counts in (tier, kind) as in eval2.split_families."""
    cell = (it["tier"], it["kind"])
    return [cell + (it["template"],), cell] if it["id"].startswith("a3-") else [cell]


def count_cells(its):
    c = Counter()
    for it in its:
        c.update(cells_of(it))
    return c


def choose_exposed(items):
    """EXPOSED_PER_KIND items of each kind, by seed, from different groups; their groups go open as a whole."""
    chosen = []
    for kind in sorted({i["kind"] for i in items}):
        seen = set()
        for it in sorted((i for i in items if i["kind"] == kind), key=lambda i: exposure_key(i["id"])):
            g = group_of(it)
            if g in seen:
                continue
            seen.add(g)
            chosen.append(it["id"])
            if len(seen) == EXPOSED_PER_KIND:
                break
    return sorted(chosen)


def embedded_rows():
    return [json.loads(l) for l in EXPOSED_A3_TEXT.splitlines() if l.strip()]


def mark_exposed(items):
    """Returns (items with `exposed` set, exposed ids). Embedded rows (EXPOSED_A3_TEXT) win over a new choice: they
    must equal the built items, else the generator changed an item people have seen."""
    emb = embedded_rows()
    if emb:
        by = {i["id"]: i for i in items}
        for row in emb:
            cur = by.get(row["id"])
            if cur is None or {**cur, "exposed": True} != {**row, "exposed": True}:
                raise AssemblyError("embedded exposed item %s differs from the built item" % row["id"])
        ids = sorted(r["id"] for r in emb)
    else:
        ids = choose_exposed(items)
    out = [{**i, "exposed": i["id"] in set(ids)} for i in items]
    return out, ids


# ------------------------------------------------------------------ split
def split_extend(items, seed=SPLIT_SEED, share=SHARE, prior=None):
    """(open, hidden). The method of eval2.split_families (whole groups, greedy on the distance to the target, order by
    seed and group), with groups as families and cells_of (the template and the kind) as the cells, so that a3 shares are reached per template and per kind.
    prior = {"open": ids, "hidden": ids} of an earlier split: its groups stay on their side, the others are decided
    on the remaining targets. For items without a3 ids it returns what split_families returns."""
    fam = defaultdict(list)
    for it in items:
        fam[group_of(it)].append(it)
    forced_open = {f for f, its in fam.items() if any(it.get("exposed") is True for it in its)}
    cells_total = count_cells(items)
    target = {c: share * n for c, n in cells_total.items()}
    got = Counter()
    hidden = set()
    fixed = set()
    if prior:
        po, ph = set(prior["open"]), set(prior["hidden"])
        for f, its in fam.items():
            ids = {i["id"] for i in its}
            side_h, side_o = ids & ph, ids & po
            if side_h and side_o:
                raise AssemblyError("group %s was split over both sides before" % f)
            if side_h:
                if f in forced_open:
                    raise AssemblyError("group %s is hidden in the earlier split but now exposed" % f)
                hidden.add(f)
                got.update(count_cells(its))
                fixed.add(f)
            elif side_o:
                fixed.add(f)
    # every a3 template with two or more groups gets one hidden group first (the greedy pass alone can leave a template
    # on the open side when its kind has reached its share); then the greedy pass decides the rest
    free = sorted((f for f in fam if f not in forced_open and f not in fixed), key=lambda f: eval2._order_key(seed, f))
    tgroups = defaultdict(set)
    for f, its in fam.items():
        for it in its:
            if it["id"].startswith("a3-"):
                tgroups[it["template"]].add(f)
    seen_hidden = {it["template"] for f in hidden for it in fam[f] if it["id"].startswith("a3-")}
    for t in sorted(tgroups):
        if len(tgroups[t]) >= 2 and t not in seen_hidden:
            pick = next((f for f in free if f in tgroups[t] and f not in hidden), None)
            if pick is not None:
                hidden.add(pick)
                got.update(count_cells(fam[pick]))
                seen_hidden |= {it["template"] for it in fam[pick]}
    for f in free:
        if f in hidden:
            continue
        add = count_cells(fam[f])
        before = sum(abs(got[c] - target[c]) for c in add)
        after = sum(abs(got[c] + n - target[c]) for c, n in add.items())
        if after < before:
            hidden.add(f)
            got.update(add)
    return ([it for it in items if group_of(it) not in hidden], [it for it in items if group_of(it) in hidden])


def side_problems(op, hd, allow_missing):
    out = []
    for kind in PLAN:
        if kind == "not_covered" and "gap" in allow_missing:
            continue
        for side, its in (("open", op), ("hidden", hd)):
            k = [i for i in its if i["kind"] == kind]
            if len(k) < MIN_KIND_ITEMS:
                out.append("%s, %s side: %d items, at least %d needed" % (kind, side, len(k), MIN_KIND_ITEMS))
            f = {i["family"] for i in k}
            if len(f) < MIN_KIND_FAMILIES:
                out.append("%s, %s side: %d families, at least %d needed" % (kind, side, len(f), MIN_KIND_FAMILIES))
    return out


def template_sides(op, hd):
    """(problems, one_side): a template with two or more split groups must have items on both sides (problem
    otherwise); one_side lists (template, side, items, groups) of the templates that sit on one side only."""
    groups = defaultdict(set)
    side = defaultdict(set)
    n = Counter()
    for tag, its in (("open", op), ("hidden", hd)):
        for i in its:
            groups[i["template"]].add(group_of(i))
            side[i["template"]].add(tag)
            n[i["template"]] += 1
    problems, one = [], []
    for t in sorted(side):
        if len(side[t]) == 1:
            one.append((t, next(iter(side[t])), n[t], len(groups[t])))
            if len(groups[t]) >= 2:
                problems.append("template %s has %d groups and all its items are on the %s side" % (t, len(groups[t]), next(iter(side[t]))))
    return problems, one


def old_sides_hold(old_items, a3_open, a3_hidden, seed=OLD_SPLIT_SEED, share=SHARE):
    """In-memory proof that adding the a3 items to the 496 moves no old id: the split of both, with the seed of the 496,
    gives the open ids of tier_a.jsonl on the old side. (The a3 items have their own cells and their own seed.)
    Returns the problems."""
    old_open = {json.loads(l)["id"] for l in OPEN_A1.read_text().splitlines() if l.strip()}
    op, _ = split_extend(list(old_items) + list(a3_open) + list(a3_hidden), seed, share)
    old_ids = {i["id"] for i in old_items}
    if {i["id"] for i in op if i["id"] in old_ids} != old_open:
        return ["an old id moved: the open side of the 496 differs from eval/m4/open/tier_a.jsonl"]
    return []


# ------------------------------------------------------------------ build
def generator_files():
    """sha256 of the generator and check files that make the a3 items (the commit that holds them is made after the build;
    this assembly file is left out because the exposed rows are embedded in it after the first build)."""
    return {n: eval2.sha256_file(ROOT / "scripts" / n) for n in GENERATOR_FILES}


def build(allow_missing=(), verify=False, with_predecessor=True):
    """Everything the dry run and the real run share. Returns a dict; raises AssemblyError when a floor fails."""
    items, notes = collect(allow_missing, verify)
    r = {"notes": notes, "problems": []}
    old_g = old_items = None
    if with_predecessor:
        old_g, _ = predecessor()
        old_items = old_g.items
        check_ids(items, [i["id"] for i in old_items])
    else:
        check_ids(items, [])
    items, exposed = mark_exposed(items)
    r.update(items=items, exposed=exposed, old_g=old_g, old_items=old_items or [])
    r["problems"] += floor_problems(items, allow_missing)
    if with_predecessor:
        rep, hits, pairs, ncorpus = near_duplicates(items, old_items)
        r.update(dup_report=rep, dup_corpus=ncorpus, dup_pairs=pairs)
        r["problems"] += ["near-duplicate: %s against %s (jaccard %.3f, containment %.3f)" % h for h in hits]
    r["guess_lines"], r["guess"], r["guess_union"] = guess_table(items, old_g)
    if r["guess"] >= a1.MAX_GUESS:
        r["problems"].append("blind guess of the a3 build %.1f %% not below %.0f %%" % (100 * r["guess"], 100 * a1.MAX_GUESS))
    r["mix_lines"], mp = mix_lines(items, old_items or [])
    r["problems"] += mp
    op, hd = split_extend(items)
    r["open"], r["hidden"] = op, hd
    r["problems"] += side_problems(op, hd, allow_missing)
    tp, r["one_side"] = template_sides(op, hd)
    r["problems"] += tp
    if with_predecessor:
        r["problems"] += old_sides_hold(old_items, op, hd)
    return r


def manifest_of(r, enc_sha=None):
    items, op, hd = r["items"], r["open"], r["hidden"]
    from kafka_sre_rag import catalogue_a3
    from kafka_sre_rag.catalogue import catalogue_hash, default_dir
    cell = lambda its: dict(sorted(Counter(i["kind"] for i in its).items()))  # noqa: E731
    return {
        "generator": VERSION, "generator_files_sha256": generator_files(), "replaces": REPLACES, "seed": SPLIT_SEED, "share": SHARE, "exposure_seed": SEED,
        "catalogue_hash": catalogue_hash(default_dir()), "catalogue_hash_a3": catalogue_a3.manifest()["catalogue_hash"],
        "predecessor": {"items_sha256": json.loads(SPLIT_A1.read_text())["items_sha256"], "items": 496},
        "items_total": len(items), "items_sha256": sha(serialise(items)),
        "counts": {"open": len(op), "hidden": len(hd), "exposed": len(r["exposed"]),
                   "families_open": len({i["family"] for i in op}), "families_hidden": len({i["family"] for i in hd})},
        "per_kind": {k: {"open": cell(op).get(k, 0), "hidden": cell(hd).get(k, 0), "total": cell(items)[k]} for k in cell(items)},
        "exposed_ids": r["exposed"],
        "outputs": {"open": {"path": OPEN_PATH, "sha256": sha(serialise(op)), "items": len(op)},
                    "hidden": {"path": HIDDEN_PATH, "plaintext_sha256": sha(serialise(hd)), "items": len(hd),
                               "sha256": enc_sha,
                               "note": "encrypted with a random salt: the ciphertext differs on every run; the plaintext "
                                       "(jsonl, sort_keys, in the order of the items) is reproduced exactly"}},
        "rule": "groups on one side (an item and its control, a task and its restatement, by `pair`); groups with any item "
                "exposed:true go open; the share per (tier, kind, template) and (tier, kind) cell is reached with the remaining groups "
                "(eval2_items_a3.split_extend, the method of scripts/eval2.py split_families with the template as part of "
                "the cell)",
        "grouping": "items with `pair` by it; all other items by their own family",
        "templates_one_side": [{"template": t, "side": sd, "items": n, "groups": g} for t, sd, n, g in r.get("one_side", [])],
        "kinds_missing": sorted(k for k in PLAN if k not in cell(items)),
    }


def report(r, allow_missing):
    items, op, hd = r["items"], r["open"], r["hidden"]
    out = ["%s  seed %s  split seed %d share %.2f" % (VERSION, SEED, SPLIT_SEED, SHARE)]
    out += r["notes"]
    out.append("a3 items: %d  (families %d, templates %d, exposed %d)" % (
        len(items), len({i["family"] for i in items}), len({i["template"] for i in items}), len(r["exposed"])))
    out.append("per kind and side (open / hidden / total; families open / hidden):")
    for k in sorted({i["kind"] for i in items}):
        o, h = [i for i in op if i["kind"] == k], [i for i in hd if i["kind"] == k]
        out.append("  %-16s %3d / %3d / %3d   families %3d / %3d" % (k, len(o), len(h), len(o) + len(h),
                                                                     len({i["family"] for i in o}), len({i["family"] for i in h})))
    out.append("  %-16s %3d / %3d / %3d   (split groups %d, hidden %d)" % (
        "all", len(op), len(hd), len(items), len({group_of(i) for i in items}), len({group_of(i) for i in hd})))
    if r.get("one_side"):
        out.append("templates on one side only (template, side, items, split groups): %s" % "; ".join(
            "%s %s %d %d" % x for x in r["one_side"]))
    out.append("per template: %s" % ", ".join("%s %d" % kv for kv in sorted(Counter(i["template"] for i in items).items())))
    out.append("per failure class: %s" % ", ".join("%s %d" % kv for kv in sorted(Counter(i["failure_class"] for i in items).items())))
    out.append("exposed (open by construction; the list for T9, ids only): %s" % ", ".join(r["exposed"]))
    out += r["mix_lines"]
    out += r["guess_lines"]
    if "dup_report" in r:
        out.append("near-duplicates: %d texts in the corpus (eval, work/v3, data, open tier_a) + the 496 + the a3 items; thresholds "
                   "Jaccard >= %.2f or containment >= %.2f (both sets >= %d tokens)" % (
                       r["dup_corpus"], a1.JACCARD_T, a1.CONTAIN_T, a1.MIN_TOKENS))
        out.append("  highest values (jaccard, containment, a3 item, nearest):")
        for g, b in r["dup_report"].items():
            out.append("    %-28s %.3f  %.3f  %s  %s" % (g, b[0], b[1], b[2], b[3]))
    if r.get("dup_pairs"):
        out.append("warning, not gating: %d pairs of a3 items of different templates over the thresholds (ids, jaccard, containment):"
                   % len(r["dup_pairs"]))
        for a, b, j, c in r["dup_pairs"]:
            out.append("    %s  %s  %.3f  %.3f" % (a, b, j, c))
    m = manifest_of(r)
    out.append("hashes the real run would write:")
    out.append("  items_sha256 (plain build, in memory only)  %s" % m["items_sha256"])
    out.append("  %s  sha256 %s" % (OPEN_PATH, m["outputs"]["open"]["sha256"]))
    out.append("  %s  plaintext sha256 %s (the file itself differs on every run: random salt)" % (HIDDEN_PATH, m["outputs"]["hidden"]["plaintext_sha256"]))
    out.append("  catalogue_hash %s  catalogue_hash_a3 %s" % (m["catalogue_hash"], m["catalogue_hash_a3"]))
    out.append("problems: %s" % ("none" if not r["problems"] else ""))
    out += ["  " + p for p in r["problems"]]
    return "\n".join(out)


def repo_path(p, external_ok=False):
    """The path as stored in the manifest: relative to the repository root. A path outside the root is refused;
    with external_ok (temporary test runs) only its file name is stored, never an absolute path."""
    p = Path(p).resolve()
    try:
        return p.relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        if external_ok:
            return p.name
        raise AssemblyError("%s is outside the repository; the manifest stores repository-relative paths" % p)


def write_outputs(r, open_out, hidden_out, manifest_out, key_file=None, external_ok=False):
    """The real run: three new files; an existing output stops it before anything is written."""
    rel_open, rel_hidden = repo_path(open_out, external_ok), repo_path(hidden_out, external_ok)
    repo_path(manifest_out, external_ok)
    for p in (open_out, hidden_out, manifest_out):
        if Path(p).exists():
            raise AssemblyError("%s exists; the a3 split is made once and never overwritten" % p)
    if r["problems"]:
        raise AssemblyError("not writing: %d problem(s)" % len(r["problems"]))
    blob = eval2.encrypt_bytes(serialise(r["hidden"]).encode("utf-8"), key_file)
    m = manifest_of(r, enc_sha=eval2.sha256_bytes(blob))
    m["outputs"]["open"]["path"], m["outputs"]["hidden"]["path"] = rel_open, rel_hidden
    eval2.write_atomic(hidden_out, blob)
    eval2.write_atomic(open_out, serialise(r["open"]).encode("utf-8"))
    eval2.write_atomic(manifest_out, (json.dumps(m, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    return m


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--allow-missing", action="append", default=[], choices=list(MODULES),
                    help="build without this generator module when it does not exist (the run says so)")
    ap.add_argument("--verify", action="store_true", help="let the client generator run its per-item verification (slow)")
    ap.add_argument("--write", action="store_true", help="write the three outputs (real run; never overwrites)")
    ap.add_argument("--open-out", default=str(ROOT / OPEN_PATH))
    ap.add_argument("--hidden-out", default=str(ROOT / HIDDEN_PATH))
    ap.add_argument("--manifest-out", default=str(ROOT / MANIFEST_PATH))
    args = ap.parse_args(argv)
    try:
        if args.write and args.allow_missing:
            raise AssemblyError("--write with --allow-missing: the real split is made once, with all five modules")
        r = build(tuple(args.allow_missing), verify=args.verify or args.write)
        print(report(r, args.allow_missing))
        if args.write:
            write_outputs(r, args.open_out, args.hidden_out, args.manifest_out)
            print("wrote %s, %s (encrypted), %s" % (args.open_out, args.hidden_out, args.manifest_out))
        else:
            print("dry run: nothing written")
        return 1 if r["problems"] else 0
    except AssemblyError as e:
        print("FAIL: %s" % e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
