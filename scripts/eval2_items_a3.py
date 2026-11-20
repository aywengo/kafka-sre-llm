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

Split rule (version 3, round 2): publication decides. Every open file of an earlier version (`git show 584fe6e:...` for
version 1, `git show 36dad96:...` for version 2; the record is eval/m4/A3_PUBLISHED_OPEN.json: id and sha256 of the
normalised question of each item) is public. An item of this build whose normalised question equals one of them, or
whose token set has a Jaccard of at least LEAK_JACCARD with one of them, or whose family (the drawn entry) is one of
theirs, is published; its whole split group (an item and its control, a task and its restatement: `pair`) goes to the
open side. Every other group is hidden, so the hidden side is made only of items that were never open. Items that
belong together carry a `pair`; every other item is its own group. Where a template cannot fill its hidden share that
way, its generator draws further entries (ids with 2xx or `-v3-`; items that exist keep their text and id); the build
checks the counts per kind, that every template has items on both sides and that every valid-but-wrong template has a
"works" control on each side. The 496 are unaffected (`split_extend` is kept for the proof that no old id moves).
The manifest records the sha256 of the generator files (not a commit id), `replaces` (versions 1 and 2) and the hash of
the published record.
"""
import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import eval2  # noqa: E402
import eval2_items_a1 as a1  # noqa: E402

VERSION = "eval2_items_a3/3"
REVISION = {"number": 4, "reason": "independent check of revision 3: the single-task restart item gets a yaml_fields_absent check on strimzi.io/restart "
                                   "(new id -v3-103); the hidden min_isr fault is redrawn as min.insync.replicas below 2 (new id -v3-104) and the "
                                   "variant key of that template ignores the replica count; the published record now also holds the open file of "
                                   "this version (00aefe4) and every hidden item with a variant in it is tagged; revision 3 (hidden valid-but-wrong "
                                   "items with published variants redrawn, L7) and revision 2 stay"}
SEED = "m4-a3-3"          # exposure draw of version 3 (version 1 used m4-a3-1, version 2 m4-a3-2)
SPLIT_SEED = 20261119    # the split draw of version 2: not used for the a3 split any more (publication decides), kept for the record
OLD_SPLIT_SEED = 20261118  # the seed of the 496: only for the proof that the 496 do not move
REPLACES = [{"generator": "eval2_items_a3/1", "items_sha256": "d425cdada882b81d1124a6dc49d5d43b03ca7af6326882aa9a67e98eaf036ce3"},
            {"generator": "eval2_items_a3/2", "items_sha256": "62bbed2411bd2cd5c544841898aa6433ba1455da0d0bffb9f0d067c6b38fa9d8"}]
PUBLISHED_PATH = "eval/m4/A3_PUBLISHED_OPEN.json"
# the open files of the earlier versions: (generator version, commit, path in that commit)
PUBLISHED_SOURCES = [("eval2_items_a3/1", "584fe6e", "eval/m4/open/tier_a3.jsonl"),
                     ("eval2_items_a3/2", "36dad96", "eval/m4/open/tier_a3.jsonl"),
                     # revision 4: the open file of this version as committed with revision 3 (the open file does not change)
                     ("eval2_items_a3/3", "00aefe4", "eval/m4/open/tier_a3.jsonl")]
# An item is the same as a published one when its question has the same token set up to LEAK_JACCARD (a prefix, a
# re-ordering, one changed word in a long question), when the normalised texts are equal, or when it is the same drawn
# entry (family). Parameter variants of one template (another topic name, another number) are other items: the thresholds
# of the near-duplicate rule (Jaccard 0.5, containment 0.8) cannot hold between two items of one template by design; the
# report gives the maxima against both.
LEAK_JACCARD = 0.97
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
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "timeout": 30, "type": "java_compiles"}, {"required": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "assign", "params": ["java.util.Collection"]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "seekToBeginning", "params": ["java.util.Collection"]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll", "params": ["java.time.Duration"]}], "type": "java_calls"}, {"order": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "assign"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "seekToBeginning"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll"}], "type": "java_call_order"}, {"keys": {"bootstrap.servers": "kafka:9092", "enable.auto.commit": "false"}, "scope": ["consumer"], "type": "java_config"}], "dedup": {"max_containment": 0.385, "max_jaccard": 0.135, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "client_code", "family": "client_consumer_assign_seek:beginning", "generator": "eval2_items_a3_client/3", "id": "a3-client_consumer_assign_seek-v2-001", "kind": "client_code", "language": "java", "not_in_index": true, "pair": "client:client_consumer_assign_seek:beginning", "perturbed": [{"check": "java_calls", "fails": ["java_call_order", "java_calls", "java_compiles"], "kind": "wrong_signature", "label": "wrong_signature", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.assign(List.of(partition));\n            consumer.seekToBeginning(partition);\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_calls", "fails": ["java_call_order", "java_calls"], "kind": "missing_call", "label": "missing_call", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.assign(List.of(partition));\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_call_order", "fails": ["java_call_order"], "kind": "wrong_order", "label": "wrong_order", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.seekToBeginning(List.of(partition));\n            consumer.assign(List.of(partition));\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "violates": 2}, {"check": "java_config", "fails": ["java_compiles", "java_config"], "kind": "invented_constant", "label": "invented_constant", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMITS_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.assign(List.of(partition));\n            consumer.seekToBeginning(List.of(partition));\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_config", "fails": ["java_config"], "kind": "invented_key", "label": "invented_key", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(\"enable.auto.commits\", \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.assign(List.of(partition));\n            consumer.seekToBeginning(List.of(partition));\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_compiles", "fails": ["java_compiles"], "kind": "bad_import", "label": "bad_import", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\nimport org.apache.kafka.clients.ClientFactory;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.assign(List.of(partition));\n            consumer.seekToBeginning(List.of(partition));\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "violates": 0}], "perturbed_reference": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.assign(List.of(partition));\n            consumer.seekToBeginning(partition);\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "perturbed_violates": 1, "question": "Using the Java client libraries of Apache Kafka 4.3, write a program that reads partition 0 of the topic `ledger` from `kafka:9092` without subscribing to a group: it assigns the partition itself, keeps auto commit off, moves the position to the beginning of the partition, then polls once and prints the records.\n\nGive one complete Java source file in a single ```java code block. It must compile against kafka-clients 4.3.2 (and kafka-streams 4.3.2 where Streams is used) with no other library.", "reference": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class ReplayReader {\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            TopicPartition partition = new TopicPartition(\"ledger\", 0);\n            consumer.assign(List.of(partition));\n            consumer.seekToBeginning(List.of(partition));\n            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));\n            for (ConsumerRecord<String, String> record : records) {\n                System.out.println(record.offset() + \" \" + record.value());\n            }\n        }\n    }\n}\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "client_consumer_assign_seek", "fact": "props.setProperty(\"bootstrap.servers\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 221, "requires": "the client is configured with bootstrap.servers"}, {"catalogue": "pinned_text", "entry": "client_consumer_assign_seek", "fact": "Configure <code>enable.auto.commit=false</code>", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 352, "requires": "enable.auto.commit=false when the application keeps its position"}, {"catalogue": "pinned_text", "entry": "client_consumer_assign_seek", "fact": "Manually assign a list of partitions to this consumer", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 835, "requires": "assign gives the partitions without a group"}, {"catalogue": "pinned_text", "entry": "client_consumer_assign_seek", "fact": "Seek to the first offset for each of the given partitions. This function evaluates lazily", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1222, "requires": "seekToBeginning moves the position, evaluated at the next poll"}, {"catalogue": "pinned_text", "entry": "client_consumer_assign_seek", "fact": "Seek to the last offset for each of the given partitions. This function evaluates lazily", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1235, "requires": "seekToEnd moves the position, evaluated at the next poll"}, {"catalogue": "java_api", "entry": "kafka-clients 4.3.2, kafka-streams 4.3.2", "fact": "every required class, method and *_CONFIG constant resolves in the jars", "file": null, "line": null}], "template": "client_consumer_assign_seek", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "timeout": 30, "type": "java_compiles"}, {"required": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "subscribe", "signatures": [["java.util.Collection"], ["java.util.Collection", "org.apache.kafka.clients.consumer.ConsumerRebalanceListener"]]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll", "params": ["java.time.Duration"]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitAsync", "signatures": [["org.apache.kafka.clients.consumer.OffsetCommitCallback"], ["java.util.Map", "org.apache.kafka.clients.consumer.OffsetCommitCallback"]]}], "type": "java_calls"}, {"order": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "subscribe"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitAsync"}], "type": "java_call_order"}, {"keys": {"bootstrap.servers": "kafka-bootstrap:9092", "enable.auto.commit": "false", "group.id": "compliance"}, "scope": ["consumer"], "type": "java_config"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.147, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "client_code", "family": "client_consumer_commit_async:audit", "generator": "eval2_items_a3_client/3", "id": "a3-client_consumer_commit_async-v2-002", "kind": "client_code", "language": "java", "not_in_index": true, "pair": "client:client_consumer_commit_async:audit", "perturbed": [{"check": "java_calls", "fails": ["java_calls"], "kind": "wrong_signature", "label": "wrong_signature", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_calls", "fails": ["java_call_order", "java_calls"], "kind": "missing_call", "label": "missing_call", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_call_order", "fails": ["java_call_order"], "kind": "wrong_order", "label": "wrong_order", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                consumer.commitAsync((offsets, error) -> logFailure(error));\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n            }\n        }\n    }\n}\n```", "violates": 2}, {"check": "java_config", "fails": ["java_compiles", "java_config"], "kind": "invented_constant", "label": "invented_constant", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMITS_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync((offsets, error) -> logFailure(error));\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_config", "fails": ["java_config"], "kind": "invented_key", "label": "invented_key", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(\"enable.auto.commits\", \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync((offsets, error) -> logFailure(error));\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_compiles", "fails": ["java_compiles"], "kind": "bad_import", "label": "bad_import", "text": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\nimport org.apache.kafka.clients.ClientFactory;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync((offsets, error) -> logFailure(error));\n            }\n        }\n    }\n}\n```", "violates": 0}], "perturbed_reference": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "perturbed_violates": 1, "question": "Write a Java program for Apache Kafka 4.3 that consumes the topic `audit-log` as consumer group `compliance` from `kafka-bootstrap:9092` with auto commit turned off. After each poll it processes the records and then commits the offsets asynchronously, passing a callback that reports a failed commit.\n\nGive one complete Java source file in a single ```java code block. It must compile against kafka-clients 4.3.2 (and kafka-streams 4.3.2 where Streams is used) with no other library.", "reference": "```java\nimport java.time.Duration;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class AuditConsumer {\n    static void logFailure(Exception error) {\n        if (error != null) {\n            System.err.println(\"offset commit failed: \" + error.getMessage());\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka-bootstrap:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"compliance\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"audit-log\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync((offsets, error) -> logFailure(error));\n            }\n        }\n    }\n}\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "props.setProperty(\"bootstrap.servers\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 221, "requires": "the client is configured with bootstrap.servers"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "props.setProperty(\"group.id\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 222, "requires": "a subscribing consumer sets group.id"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "props.setProperty(\"enable.auto.commit\", \"false\");", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 223, "requires": "enable.auto.commit=false for manual commits"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "consumer.subscribe(", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 227, "requires": "the consumer subscribes before it polls"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "This is an asynchronous call and will not block. Any errors encountered are either passed to the callback", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1125, "requires": "commitAsync reports errors only through its callback"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "public void subscribe(Collection<String> topics) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 726, "requires": "accepted overload: subscribe(Collection) subscribes to the topics"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 700, "requires": "accepted overload: subscribe(Collection, ConsumerRebalanceListener) subscribes to the same topics and registers a listener"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "public ConsumerRecords<K, V> poll(final Duration timeout) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 904, "requires": "accepted overload: poll(Duration) is the only poll in 4.3.2"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "public void commitAsync(OffsetCommitCallback callback) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1138, "requires": "accepted overload: commitAsync(callback) commits the offsets of the last poll without blocking"}, {"catalogue": "pinned_text", "entry": "client_consumer_commit_async", "fact": "public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1169, "requires": "accepted overload: commitAsync(Map, callback) commits the given offsets without blocking"}, {"catalogue": "java_api", "entry": "kafka-clients 4.3.2, kafka-streams 4.3.2", "fact": "every required class, method and *_CONFIG constant resolves in the jars", "file": null, "line": null}], "template": "client_consumer_commit_async", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["does not work"], "norm": "ci", "type": "answer_equals"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaTopic", "type": "yaml_one_doc"}, {"kind": "KafkaTopic", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "page-views"}, {"path": ["spec", "partitions"], "value": 6}, {"path": ["spec", "replicas"], "value": 3}, {"path": ["spec", "config", "cleanup.policy"], "value": "compact"}], "kind": "KafkaTopic", "type": "yaml_fields_equal"}, {"oracle": "config_rule", "rule": "V3", "rules": [{"a": "min.cleanable.dirty.ratio", "b": {"const": 0.1}, "op": "eq"}], "type": "semantic_rule"}], "dedup": {"max_containment": 0.647, "max_jaccard": 0.193, "nearest": "data/train.jsonl"}, "default": "0.5", "domain": "strimzi", "exposed": true, "failure_class": "valid_but_wrong", "family": "vbw_default_value:min.cleanable.dirty.ratio:page-views", "generator": "eval2_items_a3_vbw/3", "id": "a3-vbw_default_value-v2-003", "kind": "valid_but_wrong", "overlap": null, "overlap_band": "none", "pair": "vbw:V3:b", "perturbed_reference": "The artifact does what is wanted.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    cleanup.policy: compact\n    min.cleanable.dirty.ratio: 0.5\n```\nANSWER: works", "perturbed_violates": 1, "question": "In the Strimzi cluster `events`, log compaction must start once 10 % of the log is dirty (ratio 0.1). I apply this `KafkaTopic`. Does it do that?\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    cleanup.policy: compact\n    min.cleanable.dirty.ratio: 0.5\n```\n\nFinish with a final line `ANSWER: works` or `ANSWER: does not work`. If it does not work, give the corrected manifest in one fenced code block before that line and change only what is needed.", "reference": "`min.cleanable.dirty.ratio: 0.5` is the default of this setting (0.5), so the topic behaves as if nothing was set; the requirement needs 0.1.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    cleanup.policy: compact\n    min.cleanable.dirty.ratio: 0.1\n```\nANSWER: does not work", "seed": "m4-a3-vbw-1", "sources": [{"catalogue": "kafka_config", "entry": "min.cleanable.dirty.ratio (topic)", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 223, "phrase": "min.cleanable.dirty.ratio"}, {"catalogue": "kafka_config", "entry": "min.cleanable.dirty.ratio (topic) Default", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 227, "phrase": "<tr><th>Default:</th><td>0.5</td></tr>"}], "template": "vbw_default_value", "tier": "A", "vbw": {"artifact_language": "yaml", "control_artifact": "apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    cleanup.policy: compact\n    min.cleanable.dirty.ratio: 0.1", "mechanism": "V3", "oracle_checks": [5], "oracle_checks_on_control": [{"oracle": "config_rule", "rule": "V3", "rules": [{"a": "min.cleanable.dirty.ratio", "b": {"const": 0.1}, "op": "eq"}], "type": "semantic_rule"}], "role": "wrong", "rules": [], "shown_fails_oracle": true, "shown_valid": true, "shown_valid_note": "validates against the crd schema"}, "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["does not work"], "norm": "ci", "type": "answer_equals"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaConnector", "type": "yaml_one_doc"}, {"kind": "KafkaConnector", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "metrics-source"}, {"path": ["spec", "class"], "value": "org.apache.kafka.connect.file.FileStreamSourceConnector"}, {"path": ["spec", "config"], "value": {"file": "/var/log/metrics.log", "topic": "metrics"}}], "kind": "KafkaConnector", "type": "yaml_fields_equal"}, {"expect": {"include_tasks": false, "only_failed": true, "restart_connector": true}, "kind": "KafkaConnector", "oracle": "restart_annotation", "rule": "V2", "type": "semantic_rule"}], "dedup": {"max_containment": 0.75, "max_jaccard": 0.103, "nearest": "data/train.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "valid_but_wrong", "family": "vbw_restart_annotation:KafkaConnector:metrics-source", "generator": "eval2_items_a3_vbw/3", "id": "a3-vbw_restart_annotation-v2-002", "kind": "valid_but_wrong", "overlap": null, "overlap_band": "none", "pair": "vbw:V2:b", "perturbed_reference": "The artifact does what is wanted.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: metrics-source\n  namespace: observability\n  labels:\n    strimzi.io/cluster: metrics-connect\n  annotations:\n    strimzi.io/restart: 'true'\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSourceConnector\n  tasksMax: 2\n  config:\n    file: /var/log/metrics.log\n    topic: metrics\n```\nANSWER: works", "perturbed_violates": 1, "question": "The `KafkaConnector` below runs on the Connect cluster `metrics-connect`. I want Strimzi to restart the connector instance only if it is in the FAILED state, so I apply this manifest. Does it do that?\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: metrics-source\n  namespace: observability\n  labels:\n    strimzi.io/cluster: metrics-connect\n  annotations:\n    strimzi.io/restart: 'true'\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSourceConnector\n  tasksMax: 2\n  config:\n    file: /var/log/metrics.log\n    topic: metrics\n```\n\nFinish with a final line `ANSWER: works` or `ANSWER: does not work`. If it does not work, give the corrected manifest in one fenced code block before that line and change only what is needed.", "reference": "`true` restarts the instance whatever its state; `onlyFailed` restarts only instances with a FAILED status.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: metrics-source\n  namespace: observability\n  labels:\n    strimzi.io/cluster: metrics-connect\n  annotations:\n    strimzi.io/restart: onlyFailed\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSourceConnector\n  tasksMax: 2\n  config:\n    file: /var/log/metrics.log\n    topic: metrics\n```\nANSWER: does not work", "seed": "m4-a3-vbw-1", "sources": [{"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 33, "phrase": "The `restart` annotation is set to `true`."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 35, "phrase": "You can refine the restart behavior using the `includeTasks` and `onlyFailed` parameters. Both default to `false`."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 38, "phrase": "* `includeTasks` restarts both the connector instance and its task instances."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 39, "phrase": "* `onlyFailed` restarts only instances with a `FAILED` status when set to `true`."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 46, "phrase": "strimzi.io/restart=\"includeTasks,onlyFailed\""}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/apache-kafka/docs/kafka-connect/user-guide.md", "line": 308, "phrase": "the \"includeTasks\" parameter specifies whether to restart the connector instance and task instances"}], "template": "vbw_restart_annotation", "tier": "A", "vbw": {"artifact_language": "yaml", "control_artifact": "apiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: metrics-source\n  namespace: observability\n  labels:\n    strimzi.io/cluster: metrics-connect\n  annotations:\n    strimzi.io/restart: onlyFailed\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSourceConnector\n  tasksMax: 2\n  config:\n    file: /var/log/metrics.log\n    topic: metrics", "mechanism": "V2", "oracle_checks": [5], "oracle_checks_on_control": [{"expect": {"include_tasks": false, "only_failed": true, "restart_connector": true}, "kind": "KafkaConnector", "oracle": "restart_annotation", "rule": "V2", "type": "semantic_rule"}], "role": "wrong", "rules": ["restart.connector_true_or_params", "restart.mm2_connector"], "shown_fails_oracle": true, "shown_valid": true, "shown_valid_note": "validates against the crd schema"}, "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "has_kafka_command"}, {"steps": [{"flags_all": ["--describe", "--group"], "flags_forbidden": ["--members", "--state"], "role": "safe", "tool": "kafka-streams-groups.sh"}, {"flags_all": ["--delete-offsets", "--group"], "flags_any": ["--all-input-topics", "--input-topic"], "flags_forbidden": ["--dry-run", "--execute", "--delete"], "role": "mutate", "tool": "kafka-streams-groups.sh"}], "type": "cmd_order"}, {"tool": "kafka-streams-groups.sh", "type": "kcmd_known_flags"}, {"tool": "kafka-streams-groups.sh", "type": "kcmd_constraints"}], "dedup": {"max_containment": 0.421, "max_jaccard": 0.211, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "safe_order", "family": "order_sg_delete_offsets:kafka-streams-groups:delete-offsets:orders-streams", "generator": "eval2_items_a3_order/3", "id": "a3-order_sg_delete_offsets-301", "kind": "safe_order", "note": "The doc page kafka-streams-group-sh.md (lines 35, 114, 165) says to preview with --dry-run before every mutating operation, but the pinned tool source accepts --dry-run and --execute only with --reset-offsets (StreamsGroupCommandOptions.java:60-61, 235-236). The item follows the tool: --describe --offsets first, then --delete-offsets without --dry-run or --execute.", "perturbed": [{"check": "cmd_order", "label": "swap", "text": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --delete-offsets --group orders-streams --all-input-topics\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --describe --offsets --group orders-streams\n```", "violates": 1}, {"check": "cmd_order", "label": "missing", "text": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --delete-offsets --group orders-streams --all-input-topics\n```", "violates": 1}, {"check": "cmd_order", "label": "forbidden", "text": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --describe --offsets --group orders-streams\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --delete-offsets --group orders-streams --all-input-topics --delete\n```", "violates": 1}], "perturbed_reference": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --delete-offsets --group orders-streams --all-input-topics\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --describe --offsets --group orders-streams\n```", "perturbed_violates": 1, "question": "Write the procedure to delete the committed offsets of all input topics of the Streams group `orders-streams`, so that it re-reads them on restart (the instances are stopped), after looking at the offsets the group has committed. Keep the group: it must not be deleted. Kafka 4.3 command-line tools; bootstrap server `kafka:9092`.\n\nGive the commands in the order they should be run, in one code block, one command per line; use only options that exist for the tool.", "reference": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --describe --offsets --group orders-streams\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --delete-offsets --group orders-streams --all-input-topics\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "order_sg_delete_offsets", "fact": "Mutating operations (offset resets/deletes, group deletion)", "file": "raw/apache-kafka/docs/streams/developer-guide/kafka-streams-group-sh.md", "line": 35}, {"catalogue": "pinned_text", "entry": "order_sg_delete_offsets", "fact": "Only show results without executing changes on streams group. Supported operations: reset-offsets.", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java", "line": 60}, {"catalogue": "pinned_text", "entry": "order_sg_delete_offsets", "fact": "Execute operation. Supported operations: reset-offsets.", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java", "line": 61}, {"catalogue": "pinned_text", "entry": "order_sg_delete_offsets", "fact": "\"Only Option \" + resetOffsetsOpt + \" accepts \"", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java", "line": 236}, {"catalogue": "kafka_cli", "entry": "kafka-streams-groups.sh", "fact": "every option on the lines exists for the tool: --describe, --offsets, --delete-offsets, --group, --all-input-topics, --delete", "file": null, "line": null}], "template": "order_sg_delete_offsets", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "has_kafka_command"}, {"steps": [{"flags_all": ["--reset-offsets", "--dry-run"], "flags_forbidden": [], "role": "safe", "tool": "kafka-streams-groups.sh"}, {"flags_all": ["--reset-offsets", "--execute"], "flags_forbidden": ["--delete"], "role": "mutate", "tool": "kafka-streams-groups.sh"}], "type": "cmd_order"}, {"tool": "kafka-streams-groups.sh", "type": "kcmd_known_flags"}, {"tool": "kafka-streams-groups.sh", "type": "kcmd_constraints"}], "dedup": {"max_containment": 0.75, "max_jaccard": 0.333, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "safe_order", "family": "order_sg_reset:kafka-streams-groups:shift", "generator": "eval2_items_a3_order/3", "id": "a3-order_sg_reset-003", "kind": "safe_order", "perturbed": [{"check": "cmd_order", "label": "swap", "text": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --execute\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --dry-run\n```", "violates": 1}, {"check": "cmd_order", "label": "missing", "text": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --execute\n```", "violates": 1}, {"check": "cmd_order", "label": "forbidden", "text": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --dry-run\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --execute --delete\n```", "violates": 1}], "perturbed_reference": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --execute\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --dry-run\n```", "perturbed_violates": 1, "question": "I need to reset the input topic offsets of the Streams group `billing-streams` back by 200 offsets (the application instances are stopped). Keep the group: it must not be deleted. What exact commands do I run, and in what order? Kafka 4.3, bootstrap server `kafka:9092`.\n\nGive the commands in the order they should be run, in one code block, one command per line; use only options that exist for the tool.", "reference": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --dry-run\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group billing-streams --all-input-topics --shift-by -200 --execute\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "order_sg_reset", "fact": "Always preview changes with `--dry-run` before using `--execute`", "file": "raw/apache-kafka/docs/streams/developer-guide/kafka-streams-group-sh.md", "line": 90}, {"catalogue": "pinned_text", "entry": "order_sg_reset", "fact": "Preview changes with `--dry-run`", "file": "raw/apache-kafka/docs/streams/developer-guide/kafka-streams-group-sh.md", "line": 172}, {"catalogue": "kafka_cli", "entry": "kafka-streams-groups.sh", "fact": "every option on the lines exists for the tool: --reset-offsets, --group, --all-input-topics, --dry-run, --execute, --shift-by, --delete", "file": null, "line": null}], "template": "order_sg_reset", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"tolerance": 0.01, "type": "number_within", "unit": "MiB/s", "value": 40}], "dedup": {"max_containment": 0.444, "max_jaccard": 0.16, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "arith_only", "family": "size_follower_traffic:20-3", "generator": "eval2_items_a3_sizing/3", "id": "a3-size_follower_traffic-001", "kind": "computed_sizing", "perturbed": [{"check": "number_within", "kind": "unit", "label": "MB/s against MiB/s", "text": "Worked out with the stated rule.\nANSWER: 40 MB/s", "violates": 1}, {"check": "number_within", "kind": "unit", "label": "MiB/min against MiB/s", "text": "Worked out with the stated rule.\nANSWER: 40 MiB/min", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "counted the leader copy too", "text": "Worked out with the stated rule.\nANSWER: 60 MiB/s", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "one follower only", "text": "Worked out with the stated rule.\nANSWER: 20 MiB/s", "violates": 1}], "perturbed_reference": "Worked out with the stated rule.\nANSWER: 40 MB/s", "perturbed_violates": 1, "question": "Producers write 20 MiB/s to a topic with replication factor 3. Each follower fetches the whole stream from the partition leaders. How much replication traffic do the followers pull in total, in MiB/s? Use: follower traffic = ingress x (replication factor - 1).\n\nFinish with a final line `ANSWER: <number> <unit>`.", "reference": "Worked out with the stated rule.\nANSWER: 40 MiB/s", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "size_follower_traffic", "fact": "Followers consume messages from the leader just as a normal Kafka consumer would", "file": "raw/apache-kafka/docs/design/design.md", "line": 287}], "template": "size_follower_traffic", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"tolerance": 0, "type": "number_within", "unit": "bytes", "value": 2147483648}], "dedup": {"max_containment": 0.462, "max_jaccard": 0.111, "nearest": "eval/m4/open/tier_a.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "rule_from_docs", "family": "size_k8s_quantity:2Gi-2147483648", "generator": "eval2_items_a3_sizing/3", "id": "a3-size_k8s_quantity-002", "kind": "computed_sizing", "perturbed": [{"check": "number_within", "kind": "unit", "label": "Gi read as G", "text": "Worked out with the stated rule.\nANSWER: 2000000000 bytes", "violates": 1}, {"check": "number_within", "kind": "unit", "label": "bytes against KiB", "text": "Worked out with the stated rule.\nANSWER: 2147483648 KiB", "violates": 1}, {"check": "number_within", "kind": "unit", "label": "KiB count written as bytes", "text": "Worked out with the stated rule.\nANSWER: 2097152 bytes", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "a factor of 1000", "text": "Worked out with the stated rule.\nANSWER: 2147483648000 bytes", "violates": 1}], "perturbed_reference": "Worked out with the stated rule.\nANSWER: 2000000000 bytes", "perturbed_violates": 1, "question": "For capacity planning: A container's memory limit in a Kubernetes manifest is `2Gi`. How many bytes is that? Give an integer number of bytes.\n\nFinish with a final line `ANSWER: <number> <unit>`.", "reference": "Worked out with the stated rule.\nANSWER: 2147483648 bytes", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "size_k8s_quantity", "fact": "power-of-two equivalents: Ei, Pi, Ti, Gi", "file": "raw/kubernetes/content/en/docs/concepts/configuration/manage-resources-containers.md", "line": 184}], "template": "size_k8s_quantity", "tier": "A", "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 1.0, "checks": [{"format": true, "type": "answer_line"}, {"type": "declines"}, {"allowed_identifiers": ["add.source.alias.to.metrics"], "allowed_numbers": ["2", "3.9"], "asked": ["add.source.alias.to.metrics"], "default_numbers": [], "pinned_versions": ["4.1", "4.2", "4.3", "4.3.2"], "type": "no_invented_value"}], "dedup": {"max_containment": 0.6, "max_jaccard": 0.2, "nearest": "work/v3/questions.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "not_covered", "family": "nc_old_version:gap:add.source.alias.to.metrics", "generator": "eval2_items_a3_gap/3", "id": "a3-nc_old_version-107", "kind": "not_covered", "overlap": null, "overlap_band": "none", "pair": "nc:old_version:add.source.alias.to.metrics", "perturbed": [{"check": "no_invented_value", "label": "unsupported_value", "text": "The default is true.\nANSWER: true", "violates": 2}, {"check": "declines", "label": "confident_no_decline", "text": "That setting has a sensible built-in value that applies unless it is overridden.\nANSWER: the built-in value", "violates": 1}], "perturbed_reference": "The default is true.\nANSWER: true", "perturbed_violates": 2, "question": "Kafka 3.9: what was the default of the MirrorMaker 2 source connector setting `add.source.alias.to.metrics` in that release?\n\nFinish with a final line `ANSWER: <value>`. Give the bare value. If the documentation you have does not cover it, finish with `ANSWER: not in my sources`.", "reference": "The documentation I have does not cover `add.source.alias.to.metrics` for Kafka 3.9, so I cannot give the default it had then.\nANSWER: not in my sources", "seed": "m4-a3-1", "sources": [{"absent": "add.source.alias.to.metrics", "catalogue": "frozen_index", "chunks_searched": 6444, "fact": "no chunk of the frozen index mentions the terms", "file": "work/v3/chunks_s1.jsonl", "hit_rule": "a chunk hits a term when the term is in its text, title or path exactly, ignoring case, or after dropping every non-alphanumeric character from both (all chunks of every source)", "hits": 0, "hits_per_term": {"add.source.alias.to.metrics": {"case_insensitive": 1, "exact": 1, "separator_insensitive": 1}}, "index_sha256": "233c23b0478006a8afae881b83836a226a6af89517256be4d9aac2cc7d500e92", "line": null, "premise": {"chunk_ids": ["cec1fb84292e3113"], "file": "raw/apache-kafka/docs/getting-started/upgrade.md", "hits_in_premise_file": 3, "line": 275, "phrase": "The `add.source.alias.to.metrics` configuration was removed", "rule": "the key is in the removal note of the pinned upgrade notes, which shows it existed in 3.9; no other chunk mentions it and the sentences naming it state no default"}, "terms": ["add.source.alias.to.metrics"]}, {"absent": "add.source.alias.to.metrics", "catalogue": "kafka_config", "entry": null, "fact": "the key is not in the Kafka 4.3 reference of scope connect", "file": null, "line": null, "scope": "connect"}], "template": "nc_old_version", "tier": "A", "versions": {"kafka": "3.9"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["1048588", "1048588 bytes"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.75, "max_jaccard": 0.286, "nearest": "eval/m4/open/tier_a.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "nc_other_product:ctl:message.max.bytes", "generator": "eval2_items_a3_gap/3", "id": "a3-nc_other_product-108", "kind": "not_covered", "overlap": null, "overlap_band": "none", "pair": "nc:other_product:kafka_batch_max_bytes", "perturbed": [{"check": "answer_equals", "label": "decline", "text": "The documentation I have does not cover this.\nANSWER: not in my sources", "violates": 1}, {"check": "answer_equals", "label": "wrong_value", "text": "The default of `message.max.bytes` is 1048576.\nANSWER: 1048576", "violates": 1}], "perturbed_reference": "The documentation I have does not cover this.\nANSWER: not in my sources", "perturbed_violates": 1, "question": "In Apache Kafka 4.3, what does the broker setting `message.max.bytes` default to?\n\nFinish with a final line `ANSWER: <value>`. Give the bare value. If the documentation you have does not cover it, finish with `ANSWER: not in my sources`.", "reference": "The default of `message.max.bytes` is 1048588.\nANSWER: 1048588", "seed": "m4-a3-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/message.max.bytes", "fact": "default 1048588", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 432}, {"catalogue": "frozen_index", "chunk_id": "718ce83bb6b44d5f", "fact": "chunk holds the key heading and its Default line", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "index_sha256": "233c23b0478006a8afae881b83836a226a6af89517256be4d9aac2cc7d500e92", "line": null}], "template": "nc_other_product", "tier": "A", "versions": {"kafka": "4.3"}}
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


# ------------------------------------------------------------------ the open files of the earlier versions
NORMALISATION = "sha256 of the question with leading and trailing white space removed, lower case, runs of white space as one space"
FRESH_ID = re.compile(r"-(?:v3-\d+|2\d\d)$")          # ids of the entries drawn for version 3 (N1)
CHANGED_NOT_FRESH = ("client_py_consumer_commit",)      # version-3 ids that are changed items, not new draws (N3)


def norm_text(q):
    return re.sub(r"\s+", " ", q.strip().lower())


def q_sha(q):
    return sha(norm_text(q))


# Revision 2 of version 3: the "variant" of a valid-but-wrong or safe-order item is its entry without the resource names: the family
# label (template:fault or scenario:resource ...) with the resource segment taken out, and for valid-but-wrong the failure class
# (wrong or control). Two items of one variant teach the same fault or scenario with other names. The segment index per template.
RESOURCE_SEGMENT = {"vbw_default_value": 2, "vbw_netpol_from": 2, "vbw_pdb_no_eviction": 2, "vbw_heartbeat_session": 2,
                    "vbw_restart_annotation": 2, "vbw_min_isr_replicas": 1,
                    "order_app_reset": 2, "order_argo_sync": 2, "order_sg_delete_offsets": 3, "order_shg_reset": 2,
                    "order_reassign": 3}   # order_reassign: the plan file name, present only on the throttled entry of the fresh draw


def variant_key(it):
    """None for kinds without a variant (client, sizing, not covered), else the entry without resource names."""
    if it["kind"] not in ("valid_but_wrong", "safe_order"):
        return None
    seg = it["family"].split(":")
    if it["template"] == "vbw_min_isr_replicas":
        # revision 4: the replica count is not part of the variant; the fault is. Version-2 families are "tpl:name:replicas" (the
        # fault `min.insync.replicas` equal to `replicas`), a later fault carries its label: "tpl:fault:name:replicas".
        seg = seg[:2] if len(seg) == 4 else seg[:1]
        return ":".join(seg) + "|" + it["failure_class"]
    i = RESOURCE_SEGMENT.get(it["template"])
    if i is not None and i < len(seg):
        del seg[i]
    key = ":".join(seg)
    return key + ("|" + it["failure_class"] if it["kind"] == "valid_but_wrong" else "")


def git_rows(commit, path):
    """The rows of a file of an earlier commit, or None when git or the commit is not available here."""
    try:
        out = subprocess.run(["git", "-C", str(ROOT), "show", "%s:%s" % (commit, path)], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return [json.loads(l) for l in out.stdout.splitlines() if l.strip()]


def published_record():
    """The record built from git: per earlier open file the id and the sha256 of the normalised question of each item."""
    versions = []
    for gen, commit, path in PUBLISHED_SOURCES:
        rows = git_rows(commit, path)
        if rows is None:
            raise AssemblyError("the open file of %s (%s:%s) is not available from git" % (gen, commit, path))
        versions.append({"generator": gen, "commit": commit, "path": path,
                         "items": [dict({"id": r["id"], "question_sha256": q_sha(r["question"])},
                                      **({"variant": variant_key(r)} if variant_key(r) else {})) for r in rows]})
    return {"schema": 1, "purpose": "the open items of every earlier a3 version are public; no hidden item of a later version may equal "
                                    "or nearly equal one of them (scripts/eval2_items_a3.py)",
            "normalisation": NORMALISATION, "versions": versions}


def record_text(rec):
    return json.dumps(rec, indent=1, sort_keys=True) + "\n"


class Published:
    """The earlier open items. `rows` (the texts, read from git at build time) give the full rule; without git only the
    hash list is left (the record) and an item counts as published when its normalised question is in it."""

    def __init__(self, record, rows=None):
        self.record = record
        self.versions = {v["generator"]: v for v in record["versions"]}
        self.hashes = {i["question_sha256"] for v in record["versions"] for i in v["items"]}
        self.id_hashes = {}
        for v in record["versions"]:
            for i in v["items"]:
                self.id_hashes.setdefault(i["id"], set()).add(i["question_sha256"])
        self.variants = {i["variant"] for v in record["versions"] for i in v["items"] if i.get("variant")}
        self.rows = rows
        self.families = {r["family"] for r in rows} if rows else set()
        self.tokens = [a1.tokens(core_of(r)) for r in rows] if rows else []

    def match(self, it):
        """None, or the reason an item counts as published."""
        if q_sha(it["question"]) in self.hashes:
            return "same question"
        if not self.rows:
            return None
        if it["family"] in self.families:
            return "same entry"
        t = a1.tokens(core_of(it))
        for u in self.tokens:
            if t and u and len(t & u) / len(t | u) >= LEAK_JACCARD:
                return "same words"
        return None

    def shares_variant(self, it):
        """True when the item has the variant of a published item (same fault or scenario, other resource names)."""
        k = variant_key(it)
        return bool(k) and k in self.variants

    def maxima(self, it):
        """(jaccard, containment) of the question against the published texts (None without texts)."""
        if not self.rows:
            return None
        t = a1.tokens(core_of(it))
        bj = bc = 0.0
        for u in self.tokens:
            inter = len(t & u)
            if not inter:
                continue
            bj = max(bj, inter / len(t | u))
            if min(len(t), len(u)) >= a1.MIN_TOKENS:
                bc = max(bc, inter / min(len(t), len(u)))
        return bj, bc


def load_published(path=None, use_git=True):
    """(Published): the record from its file (the record of the repository), the texts from git when available; the texts
    are checked against the record (a mismatch stops the build)."""
    path = Path(path or ROOT / PUBLISHED_PATH)
    if not path.exists():
        raise AssemblyError("%s is missing; write it with --write-published" % path.name)
    rec = json.loads(path.read_text())
    rows = None
    if use_git:
        rows = []
        for gen, commit, src in PUBLISHED_SOURCES:
            part = git_rows(commit, src)
            if part is None:
                rows = None
                break
            want = {i["id"]: i["question_sha256"] for v in rec["versions"] if v["generator"] == gen for i in v["items"]}
            got = {r["id"]: q_sha(r["question"]) for r in part}
            if got != want:
                raise AssemblyError("the open file of %s from git differs from %s" % (gen, path.name))
            rows += part
    return Published(rec, rows)


def published_problems(items, pub):
    """An id of an earlier open file may be used again only for the same question (unchanged items keep id and text)."""
    out = []
    for it in items:
        have = pub.id_hashes.get(it["id"])
        if have and q_sha(it["question"]) not in have:
            out.append("id %s is in an earlier open file with another question" % it["id"])
    return out


def split_by_publication(items, pub):
    """(open, hidden, published_groups): a group with any published item goes open as a whole; every other group is hidden.
    Both lists keep the order of `items`."""
    fam = defaultdict(list)
    for it in items:
        fam[group_of(it)].append(it)
    groups = {f for f, its in fam.items() if any(pub.match(i) for i in its)}
    return ([i for i in items if group_of(i) in groups], [i for i in items if group_of(i) not in groups], groups)


def control_sides(op, hd):
    """Every valid-but-wrong template has a "works" control on each side (round 2, N2)."""
    out = []
    for t in sorted({i["template"] for i in op + hd if i["kind"] == "valid_but_wrong"}):
        for side, its in (("open", op), ("hidden", hd)):
            if not any(i["template"] == t and i["failure_class"] == "valid_control" for i in its):
                out.append("template %s has no control on the %s side" % (t, side))
    return out


def hidden_count_problems(hd, allow_missing):
    out = []
    for kind, n in PLAN.items():
        if kind == "not_covered" and "gap" in allow_missing:
            continue
        got = sum(1 for i in hd if i["kind"] == kind)
        if got != n // 2:
            out.append("%s: %d hidden items, the plan has %d" % (kind, got, n // 2))
    return out


def pair_problems(op, hd):
    side = {}
    for tag, its in (("open", op), ("hidden", hd)):
        for i in its:
            if i.get("pair"):
                side.setdefault(i["pair"], set()).add(tag)
    return ["pair %s is on both sides" % k for k, v in sorted(side.items()) if len(v) > 1]


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


def mark_exposed(items, candidates=None):
    """Returns (items with `exposed` set, exposed ids). Embedded rows (EXPOSED_A3_TEXT) win over a new choice: they
    must equal the built items, else the generator changed an item people have seen. A new choice is made among
    `candidates` (the items of the open side by publication; all items when None)."""
    emb = embedded_rows()
    if emb:
        by = {i["id"]: i for i in items}
        for row in emb:
            cur = by.get(row["id"])
            if cur is None or {**cur, "exposed": True} != {**row, "exposed": True}:
                raise AssemblyError("embedded exposed item %s differs from the built item" % row["id"])
        ids = sorted(r["id"] for r in emb)
    else:
        ids = choose_exposed(items if candidates is None else candidates)
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


def build(allow_missing=(), verify=False, with_predecessor=True, published=None):
    """Everything the dry run and the real run share. Returns a dict; raises AssemblyError when a floor fails."""
    items, notes = collect(allow_missing, verify)
    r = {"notes": notes, "problems": []}
    pub = published if published is not None else load_published()
    r["published"] = pub
    r["problems"] += published_problems(items, pub)
    old_g = old_items = None
    if with_predecessor:
        old_g, _ = predecessor()
        old_items = old_g.items
        check_ids(items, [i["id"] for i in old_items])
    else:
        check_ids(items, [])
    op0, _, _ = split_by_publication(items, pub)
    items, exposed = mark_exposed(items, op0)
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
    op, hd, r["published_groups"] = split_by_publication(items, pub)
    r["open"], r["hidden"] = op, hd
    for i in hd:   # revision 2: a hidden item that repeats the fault or scenario of a published item with other names is tagged, not moved
        if pub.shares_variant(i):
            i["shares_variant_with_published"] = True
    r["problems"] += ["hidden item %s is published (%s)" % (i["id"], pub.match(i)) for i in hd if pub.match(i)]
    r["problems"] += ["exposed item %s is on the hidden side" % i["id"] for i in hd if i.get("exposed")]
    r["problems"] += hidden_count_problems(hd, allow_missing)
    r["problems"] += side_problems(op, hd, allow_missing)
    tp, r["one_side"] = template_sides(op, hd)
    r["problems"] += tp
    r["problems"] += control_sides(op, hd) + pair_problems(op, hd)
    if with_predecessor:
        r["problems"] += old_sides_hold(old_items, op, hd)
    return r


def manifest_of(r, enc_sha=None):
    items, op, hd = r["items"], r["open"], r["hidden"]
    from kafka_sre_rag import catalogue_a3
    from kafka_sre_rag.catalogue import catalogue_hash, default_dir
    cell = lambda its: dict(sorted(Counter(i["kind"] for i in its).items()))  # noqa: E731
    return {
        "generator": VERSION, "generator_files_sha256": generator_files(), "replaces": REPLACES, "split": "by publication",
        "published_open": {"path": PUBLISHED_PATH, "sha256": sha(record_text(r["published"].record)),
                           "leak_jaccard": LEAK_JACCARD, "versions": [v["generator"] for v in r["published"].record["versions"]]},
        "exposure_seed": SEED,
        "revision": REVISION,
        "shares_variant_with_published": {
            "rule": "hidden items whose `variant` (the entry without resource names: family label minus its resource segment, for "
                    "valid_but_wrong plus the failure class; eval2_items_a3.variant_key) equals that of an item of A3_PUBLISHED_OPEN.json; "
                    "tagged `shares_variant_with_published: true`, not moved; hidden reports give the total with and without them",
            "per_template": dict(sorted(Counter(i["template"] for i in hd if i.get("shares_variant_with_published")).items())),
            "per_kind": dict(sorted(Counter(i["kind"] for i in hd if i.get("shares_variant_with_published")).items())),
            "total": sum(1 for i in hd if i.get("shares_variant_with_published"))},
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
        "rule": "groups on one side (an item and its control, a task and its restatement, by `pair`); a group with an item "
                "that is in an open file of an earlier version (same normalised question, same drawn entry, or the same words: "
                "Jaccard of the token sets of at least leak_jaccard) goes open as a whole; every other group is hidden, so the "
                "hidden side holds only items that were never open (eval2_items_a3.split_by_publication)",
        "published_matches": dict(sorted(Counter(r["published"].match(i) or "none" for i in op).items())),
        "fresh_items": dict(sorted(Counter(i["template"] for i in items if FRESH_ID.search(i["id"]) and i["template"] not in CHANGED_NOT_FRESH).items())),
        "near_duplicate_limit": "items of one template are parameter variants of one wording: the near-duplicate thresholds "
                                "(Jaccard 0.5, containment 0.8) cannot hold between a hidden item and an open item of the same template; "
                                "hidden items differ from every published item by the rule above",
        "grouping": "items with `pair` by it; all other items by their own family",
        "templates_one_side": [{"template": t, "side": sd, "items": n, "groups": g} for t, sd, n, g in r.get("one_side", [])],
        "kinds_missing": sorted(k for k in PLAN if k not in cell(items)),
    }


def report(r, allow_missing):
    items, op, hd = r["items"], r["open"], r["hidden"]
    out = ["%s  exposure seed %s  split by publication (leak rule: same question, same entry or Jaccard >= %.2f)" % (VERSION, SEED, LEAK_JACCARD)]
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
    out += publication_lines(r)
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


def publication_lines(r):
    """Counts only: how many items of this build are published, how many are new draws, and how far the hidden items are
    from the published ones (maxima over the hidden items; no text)."""
    items, op, hd, pub = r["items"], r["open"], r["hidden"], r["published"]
    out = []
    v2 = pub.versions.get("eval2_items_a3/2")
    v2_hashes = {i["question_sha256"] for i in v2["items"]} if v2 else set()
    fresh = [i for i in items if FRESH_ID.search(i["id"]) and i["template"] not in CHANGED_NOT_FRESH]
    fresh_open = [i for i in fresh if i in op]
    new_open = [i for i in op if q_sha(i["question"]) not in v2_hashes]
    out.append("published earlier (open files of %s): %s items; this build: open %d, hidden %d" % (
        " and ".join(v["generator"] for v in pub.record["versions"]), " + ".join(str(len(v["items"])) for v in pub.record["versions"]),
        len(op), len(hd)))
    out.append("open items that were not in the version-2 open file: %d (of them new draws %d, so %d were version-2 hidden items moved "
               "open because a published item has the same question, entry or words)" % (len(new_open), len(fresh_open), len(new_open) - len(fresh_open)))
    out.append("new draws (ids -v3- and 2xx) per template, all / hidden: %s" % (", ".join(
        "%s %d/%d" % (t, n, sum(1 for i in fresh if i["template"] == t and i in hd))
        for t, n in sorted(Counter(i["template"] for i in fresh).items())) or "none"))
    if pub.rows:
        mx = [pub.maxima(i) for i in hd]
        out.append("hidden against the published open items (%d hidden): highest Jaccard %.3f, highest containment %.3f; over the leak rule "
                   "(Jaccard >= %.2f): %d; over the near-duplicate thresholds (0.5 / 0.8; they cannot hold between items of one template): %d" % (
                       len(hd), max(m[0] for m in mx), max(m[1] for m in mx), LEAK_JACCARD,
                       sum(1 for m in mx if m[0] >= LEAK_JACCARD), sum(1 for m in mx if m[0] >= a1.JACCARD_T or m[1] >= a1.CONTAIN_T)))
    else:
        out.append("the texts of the earlier open files are not available (no git): only the hash list was used")
    return out


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
    ap.add_argument("--write-published", action="store_true",
                    help="write %s from the earlier open files in git (never overwrites) and stop" % PUBLISHED_PATH)
    ap.add_argument("--open-out", default=str(ROOT / OPEN_PATH))
    ap.add_argument("--hidden-out", default=str(ROOT / HIDDEN_PATH))
    ap.add_argument("--manifest-out", default=str(ROOT / MANIFEST_PATH))
    args = ap.parse_args(argv)
    try:
        if args.write_published:
            dest = ROOT / PUBLISHED_PATH
            if dest.exists():
                raise AssemblyError("%s exists; it is the record of what was published and is never overwritten" % PUBLISHED_PATH)
            eval2.write_atomic(dest, record_text(published_record()).encode("utf-8"))
            print("wrote %s" % PUBLISHED_PATH)
            return 0
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
