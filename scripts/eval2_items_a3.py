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

Split rule: scripts/eval2.py split_families, on the a3 items alone, with the seed and share of the 496 (20261118,
0.5). Items that belong together (an item and its control: an item may carry `pair`; the valid-but-wrong items are
paired by mechanism, or for the forbidden-key template by resource kind) form one group and go to one side. A group
is one family for the split. `split_extend` keeps every id of an earlier split on its side when items are added
later (new groups are decided on the remaining targets), and equals split_families when there is no earlier split.
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

VERSION = "eval2_items_a3/1"
SEED = "m4-a3-1"
SPLIT_SEED = 20261118
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
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "timeout": 30, "type": "java_compiles"}, {"required": [{"class": "org.apache.kafka.clients.admin.Admin", "method": "createTopics", "params": ["java.util.Collection"]}, {"class": "org.apache.kafka.clients.admin.NewTopic", "method": "configs", "params": ["java.util.Map"]}, {"class": "org.apache.kafka.common.KafkaFuture", "method": "get", "params": []}, {"class": "org.apache.kafka.clients.admin.Admin", "method": "close", "params": []}], "type": "java_calls"}, {"order": [{"class": "org.apache.kafka.clients.admin.NewTopic", "method": "configs"}, {"class": "org.apache.kafka.clients.admin.Admin", "method": "createTopics"}, {"class": "org.apache.kafka.common.KafkaFuture", "method": "get"}, {"class": "org.apache.kafka.clients.admin.Admin", "method": "close"}], "type": "java_call_order"}, {"keys": {"bootstrap.servers": "broker-1:9093", "cleanup.policy": "compact", "retention.ms": "86400000"}, "scope": ["admin", "topic"], "type": "java_config"}], "dedup": {"max_containment": 0.455, "max_jaccard": 0.152, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "client_code", "family": "client_admin_create_topic:compacted", "generator": "eval2_items_a3_client/1", "id": "a3-client_admin_create_topic-002", "kind": "client_code", "language": "java", "not_in_index": false, "pair": "client:client_admin_create_topic:compacted", "perturbed": [{"check": "java_calls", "fails": ["java_calls"], "kind": "wrong_signature", "label": "wrong_signature", "text": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        topic.configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, \"86400000\", TopicConfig.CLEANUP_POLICY_CONFIG, \"compact\"));\n        CreateTopicsResult result = admin.createTopics(List.of(topic), new org.apache.kafka.clients.admin.CreateTopicsOptions());\n        result.all().get();\n        admin.close();\n    }\n}\n```", "violates": 1}, {"check": "java_calls", "fails": ["java_call_order", "java_calls", "java_config"], "kind": "missing_call", "label": "missing_call", "text": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        CreateTopicsResult result = admin.createTopics(List.of(topic));\n        result.all().get();\n        admin.close();\n    }\n}\n```", "violates": 1}, {"check": "java_call_order", "fails": ["java_call_order"], "kind": "wrong_order", "label": "wrong_order", "text": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        topic.configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, \"86400000\", TopicConfig.CLEANUP_POLICY_CONFIG, \"compact\"));\n        CreateTopicsResult result = admin.createTopics(List.of(topic));\n        admin.close();\n        result.all().get();\n    }\n}\n```", "violates": 2}, {"check": "java_config", "fails": ["java_compiles", "java_config"], "kind": "invented_constant", "label": "invented_constant", "text": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        topic.configs(Map.of(TopicConfig.RETENTION_MSS_CONFIG, \"86400000\", TopicConfig.CLEANUP_POLICY_CONFIG, \"compact\"));\n        CreateTopicsResult result = admin.createTopics(List.of(topic));\n        result.all().get();\n        admin.close();\n    }\n}\n```", "violates": 3}, {"check": "java_config", "fails": ["java_config"], "kind": "invented_key", "label": "invented_key", "text": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        topic.configs(Map.of(\"retention.mss\", \"86400000\", TopicConfig.CLEANUP_POLICY_CONFIG, \"compact\"));\n        CreateTopicsResult result = admin.createTopics(List.of(topic));\n        result.all().get();\n        admin.close();\n    }\n}\n```", "violates": 3}, {"check": "java_compiles", "fails": ["java_compiles"], "kind": "bad_import", "label": "bad_import", "text": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\nimport org.apache.kafka.clients.ClientFactory;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        topic.configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, \"86400000\", TopicConfig.CLEANUP_POLICY_CONFIG, \"compact\"));\n        CreateTopicsResult result = admin.createTopics(List.of(topic));\n        result.all().get();\n        admin.close();\n    }\n}\n```", "violates": 0}], "perturbed_reference": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        topic.configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, \"86400000\", TopicConfig.CLEANUP_POLICY_CONFIG, \"compact\"));\n        CreateTopicsResult result = admin.createTopics(List.of(topic), new org.apache.kafka.clients.admin.CreateTopicsOptions());\n        result.all().get();\n        admin.close();\n    }\n}\n```", "perturbed_violates": 1, "question": "I need a Java client program (kafka-clients 4.3) that creates the topic `customers-compacted` (3 partitions, replication factor 3) on `broker-1:9093` through the admin client, with the topic configs `retention.ms=86400000` and `cleanup.policy=compact`, waits for the result and then closes the admin client.\n\nGive one complete Java source file in a single ```java code block. It must compile against kafka-clients 4.3.2 (and kafka-streams 4.3.2 where Streams is used) with no other library. Declare the client with its concrete class (KafkaConsumer, KafkaProducer, Admin).", "reference": "```java\nimport java.util.List;\nimport java.util.Map;\nimport java.util.Properties;\nimport org.apache.kafka.clients.admin.Admin;\nimport org.apache.kafka.clients.admin.AdminClientConfig;\nimport org.apache.kafka.clients.admin.CreateTopicsResult;\nimport org.apache.kafka.clients.admin.NewTopic;\nimport org.apache.kafka.common.config.TopicConfig;\n\npublic class CompactedTopicSetup {\n    public static void main(String[] args) throws Exception {\n        Properties props = new Properties();\n        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, \"broker-1:9093\");\n        Admin admin = Admin.create(props);\n        NewTopic topic = new NewTopic(\"customers-compacted\", 3, (short) 3);\n        topic.configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, \"86400000\", TopicConfig.CLEANUP_POLICY_CONFIG, \"compact\"));\n        CreateTopicsResult result = admin.createTopics(List.of(topic));\n        result.all().get();\n        admin.close();\n    }\n}\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "client_admin_create_topic", "fact": "The {@code bootstrap.servers} config", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java", "line": 104, "requires": "the admin client is configured with bootstrap.servers"}, {"catalogue": "pinned_text", "entry": "client_admin_create_topic", "fact": "CreateTopicsResult result = admin.createTopics(Collections.singleton(", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java", "line": 88, "requires": "createTopics creates the topic"}, {"catalogue": "pinned_text", "entry": "client_admin_create_topic", "fact": ".configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java", "line": 90, "requires": "topic configs go through NewTopic.configs"}, {"catalogue": "pinned_text", "entry": "client_admin_create_topic", "fact": "RETENTION_MS_CONFIG = \"retention.ms\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java", "line": 75, "requires": "retention.ms is a topic config"}, {"catalogue": "pinned_text", "entry": "client_admin_create_topic", "fact": "Call get() to block until the topic creation is complete or has failed", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/admin/Admin.java", "line": 95, "requires": "get() waits for the result"}, {"catalogue": "java_api", "entry": "kafka-clients 4.3.2, kafka-streams 4.3.2", "fact": "every required class, method and *_CONFIG constant resolves in the jars", "file": null, "line": null}], "template": "client_admin_create_topic", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 0.0, "checks": [{"format": true, "timeout": 30, "type": "java_compiles"}, {"required": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "subscribe", "params": ["java.util.Collection", "org.apache.kafka.clients.consumer.ConsumerRebalanceListener"]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll", "params": ["java.time.Duration"]}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitSync", "params": []}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitAsync", "params": []}], "type": "java_calls"}, {"order": [{"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "subscribe"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "poll"}, {"class": "org.apache.kafka.clients.consumer.KafkaConsumer", "method": "commitAsync"}], "type": "java_call_order"}, {"keys": {"bootstrap.servers": "kafka:9092", "enable.auto.commit": "false", "group.id": "stock-sync"}, "scope": ["consumer"], "type": "java_config"}], "dedup": {"max_containment": 0.6, "max_jaccard": 0.171, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "client_code", "family": "client_consumer_rebalance_listener:inventory", "generator": "eval2_items_a3_client/1", "id": "a3-client_consumer_rebalance_listener-001", "kind": "client_code", "language": "java", "not_in_index": false, "pair": "client:client_consumer_rebalance_listener:inventory", "perturbed": [{"check": "java_calls", "fails": ["java_calls"], "kind": "wrong_signature", "label": "wrong_signature", "text": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n            consumer.commitSync();\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"inventory\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_calls", "fails": ["java_calls"], "kind": "missing_call", "label": "missing_call", "text": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"inventory\"), new CommitOnRevoke(consumer));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "violates": 1}, {"check": "java_call_order", "fails": ["java_call_order"], "kind": "wrong_order", "label": "wrong_order", "text": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n            consumer.commitSync();\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                consumer.subscribe(List.of(\"inventory\"), new CommitOnRevoke(consumer));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "violates": 2}, {"check": "java_config", "fails": ["java_compiles", "java_config"], "kind": "invented_constant", "label": "invented_constant", "text": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n            consumer.commitSync();\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMITS_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"inventory\"), new CommitOnRevoke(consumer));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_config", "fails": ["java_config"], "kind": "invented_key", "label": "invented_key", "text": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n            consumer.commitSync();\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(\"enable.auto.commits\", \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"inventory\"), new CommitOnRevoke(consumer));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "violates": 3}, {"check": "java_compiles", "fails": ["java_compiles"], "kind": "bad_import", "label": "bad_import", "text": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\nimport org.apache.kafka.clients.ClientFactory;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n            consumer.commitSync();\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"inventory\"), new CommitOnRevoke(consumer));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "violates": 0}], "perturbed_reference": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n            consumer.commitSync();\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"inventory\"));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "perturbed_violates": 1, "question": "I need a Java client program (kafka-clients 4.3) that consumes the topic `inventory` as consumer group `stock-sync` from `kafka:9092` with auto commit turned off. It registers a rebalance listener that commits the offsets synchronously when partitions are revoked, and in the poll loop it commits asynchronously after each batch.\n\nGive one complete Java source file in a single ```java code block. It must compile against kafka-clients 4.3.2 (and kafka-streams 4.3.2 where Streams is used) with no other library. Declare the client with its concrete class (KafkaConsumer, KafkaProducer, Admin).", "reference": "```java\nimport java.time.Duration;\nimport java.util.Collection;\nimport java.util.List;\nimport java.util.Properties;\nimport org.apache.kafka.clients.consumer.ConsumerConfig;\nimport org.apache.kafka.clients.consumer.ConsumerRebalanceListener;\nimport org.apache.kafka.clients.consumer.ConsumerRecord;\nimport org.apache.kafka.clients.consumer.ConsumerRecords;\nimport org.apache.kafka.clients.consumer.KafkaConsumer;\nimport org.apache.kafka.common.TopicPartition;\nimport org.apache.kafka.common.serialization.StringDeserializer;\n\npublic class InventoryReader {\n    static class CommitOnRevoke implements ConsumerRebalanceListener {\n        private final KafkaConsumer<String, String> consumer;\n\n        CommitOnRevoke(KafkaConsumer<String, String> consumer) {\n            this.consumer = consumer;\n        }\n\n        @Override\n        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {\n            consumer.commitSync();\n        }\n\n        @Override\n        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {\n            System.out.println(\"assigned \" + partitions);\n        }\n    }\n\n    public static void main(String[] args) {\n        Properties props = new Properties();\n        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, \"kafka:9092\");\n        props.put(ConsumerConfig.GROUP_ID_CONFIG, \"stock-sync\");\n        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, \"false\");\n        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());\n        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {\n            consumer.subscribe(List.of(\"inventory\"), new CommitOnRevoke(consumer));\n            while (true) {\n                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));\n                for (ConsumerRecord<String, String> record : records) {\n                    System.out.println(record.key() + \" \" + record.value());\n                }\n                consumer.commitAsync();\n            }\n        }\n    }\n}\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "client_consumer_rebalance_listener", "fact": "props.setProperty(\"bootstrap.servers\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 221, "requires": "the client is configured with bootstrap.servers"}, {"catalogue": "pinned_text", "entry": "client_consumer_rebalance_listener", "fact": "props.setProperty(\"group.id\"", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 222, "requires": "a subscribing consumer sets group.id"}, {"catalogue": "pinned_text", "entry": "client_consumer_rebalance_listener", "fact": "props.setProperty(\"enable.auto.commit\", \"false\");", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 223, "requires": "enable.auto.commit=false for manual commits"}, {"catalogue": "pinned_text", "entry": "client_consumer_rebalance_listener", "fact": "consumer.subscribe(", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 227, "requires": "the consumer subscribes before it polls"}, {"catalogue": "pinned_text", "entry": "client_consumer_rebalance_listener", "fact": "For example, when partitions are taken from a consumer the consumer will want to commit its offset", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 363, "requires": "commit in the rebalance listener"}, {"catalogue": "pinned_text", "entry": "client_consumer_rebalance_listener", "fact": "implementing {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 364, "requires": "the commit goes into onPartitionsRevoked"}, {"catalogue": "pinned_text", "entry": "client_consumer_rebalance_listener", "fact": "This is an asynchronous call and will not block. Any errors encountered are either passed to the callback", "file": "raw_catalogue/apache-kafka-api/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java", "line": 1125, "requires": "commitAsync in the loop does not block"}, {"catalogue": "java_api", "entry": "kafka-clients 4.3.2, kafka-streams 4.3.2", "fact": "every required class, method and *_CONFIG constant resolves in the jars", "file": null, "line": null}], "template": "client_consumer_rebalance_listener", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["does not work"], "norm": "ci", "type": "answer_equals"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaTopic", "type": "yaml_one_doc"}, {"kind": "KafkaTopic", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "page-views"}, {"path": ["spec", "partitions"], "value": 6}, {"path": ["spec", "replicas"], "value": 3}], "kind": "KafkaTopic", "type": "yaml_fields_equal"}, {"oracle": "config_rule", "rule": "V3", "rules": [{"a": "min.cleanable.dirty.ratio", "b": {"const": 0.1}, "op": "eq"}], "type": "semantic_rule"}], "dedup": {"max_containment": 0.647, "max_jaccard": 0.204, "nearest": "data/train.jsonl"}, "default": "0.5", "domain": "strimzi", "exposed": true, "failure_class": "valid_but_wrong", "family": "vbw_default_value:min.cleanable.dirty.ratio:page-views", "generator": "eval2_items_a3_vbw/1", "id": "a3-vbw_default_value-003", "kind": "valid_but_wrong", "overlap": null, "overlap_band": "none", "perturbed_reference": "The artifact does what is wanted.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.cleanable.dirty.ratio: 0.5\n```\nANSWER: works", "perturbed_violates": 1, "question": "In the Strimzi cluster `events`, log compaction must start once 10 % of the log is dirty (ratio 0.1). I apply this `KafkaTopic`. Does it do that?\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.cleanable.dirty.ratio: 0.5\n```\n\nFinish with a final line `ANSWER: works` or `ANSWER: does not work`. If it does not work, give the corrected manifest in one fenced code block before that line and change only what is needed.", "reference": "`min.cleanable.dirty.ratio: 0.5` is the default of this setting (0.5), so the topic behaves as if nothing was set; the requirement needs 0.1.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.cleanable.dirty.ratio: 0.1\n```\nANSWER: does not work", "seed": "m4-a3-vbw-1", "sources": [{"catalogue": "catalogues_a3", "entry": "V3", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 251, "phrase": "<tr><th>Server Default Property:</th><td>min.insync.replicas</td></tr>"}, {"catalogue": "catalogues_a3", "entry": "V3", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 245, "phrase": "<h4><a id=\"min.insync.replicas\"></a><a id=\"topicconfigs_min.insync.replicas\""}, {"catalogue": "catalogues_a3", "entry": "V3", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 520, "phrase": "<h4><a id=\"min.insync.replicas\"></a><a id=\"brokerconfigs_min.insync.replicas\""}, {"catalogue": "catalogues_a3", "entry": "V3", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 249, "phrase": "<tr><th>Default:</th><td>1</td></tr>"}, {"catalogue": "catalogues_a3", "entry": "V3", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 524, "phrase": "<tr><th>Default:</th><td>1</td></tr>"}, {"catalogue": "kafka_config", "entry": "min.cleanable.dirty.ratio (topic)", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 223, "phrase": "min.cleanable.dirty.ratio"}], "template": "vbw_default_value", "tier": "A", "vbw": {"artifact_language": "yaml", "control_artifact": "apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: page-views\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: events\nspec:\n  partitions: 6\n  replicas: 3\n  config:\n    min.cleanable.dirty.ratio: 0.1", "mechanism": "V3", "oracle_checks": [5], "oracle_checks_on_control": [{"oracle": "config_rule", "rule": "V3", "rules": [{"a": "min.cleanable.dirty.ratio", "b": {"const": 0.1}, "op": "eq"}], "type": "semantic_rule"}], "role": "wrong", "rules": [], "shown_fails_oracle": true, "shown_valid": true, "shown_valid_note": "validates against the crd schema"}, "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["does not work"], "norm": "ci", "type": "answer_equals"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaConnector", "type": "yaml_one_doc"}, {"kind": "KafkaConnector", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "orders-sink"}, {"path": ["spec", "class"], "value": "org.apache.kafka.connect.file.FileStreamSinkConnector"}, {"path": ["spec", "config"], "value": {"file": "/data/orders.txt", "topics": "orders"}}], "kind": "KafkaConnector", "type": "yaml_fields_equal"}, {"expect": {"include_tasks": true, "restart_connector": true}, "kind": "KafkaConnector", "oracle": "restart_annotation", "rule": "V2", "type": "semantic_rule"}], "dedup": {"max_containment": 0.706, "max_jaccard": 0.203, "nearest": "data/train.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "valid_but_wrong", "family": "vbw_restart_annotation:KafkaConnector:orders-sink", "generator": "eval2_items_a3_vbw/1", "id": "a3-vbw_restart_annotation-001", "kind": "valid_but_wrong", "overlap": null, "overlap_band": "none", "perturbed_reference": "The artifact does what is wanted.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: orders-sink\n  namespace: streaming\n  labels:\n    strimzi.io/cluster: orders-connect\n  annotations:\n    strimzi.io/restart: 'true'\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSinkConnector\n  tasksMax: 2\n  config:\n    file: /data/orders.txt\n    topics: orders\n```\nANSWER: works", "perturbed_violates": 1, "question": "A Kafka Connect cluster `orders-connect` runs the `KafkaConnector` below. To restart the connector instance and all of its tasks, I apply this manifest. Does it do that?\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: orders-sink\n  namespace: streaming\n  labels:\n    strimzi.io/cluster: orders-connect\n  annotations:\n    strimzi.io/restart: 'true'\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSinkConnector\n  tasksMax: 2\n  config:\n    file: /data/orders.txt\n    topics: orders\n```\n\nFinish with a final line `ANSWER: works` or `ANSWER: does not work`. If it does not work, give the corrected manifest in one fenced code block before that line and change only what is needed.", "reference": "`strimzi.io/restart: \"true\"` restarts the connector instance only; `includeTasks` restarts the tasks as well.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: orders-sink\n  namespace: streaming\n  labels:\n    strimzi.io/cluster: orders-connect\n  annotations:\n    strimzi.io/restart: includeTasks\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSinkConnector\n  tasksMax: 2\n  config:\n    file: /data/orders.txt\n    topics: orders\n```\nANSWER: does not work", "seed": "m4-a3-vbw-1", "sources": [{"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 33, "phrase": "The `restart` annotation is set to `true`."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 35, "phrase": "You can refine the restart behavior using the `includeTasks` and `onlyFailed` parameters. Both default to `false`."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 38, "phrase": "* `includeTasks` restarts both the connector instance and its task instances."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 39, "phrase": "* `onlyFailed` restarts only instances with a `FAILED` status when set to `true`."}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/strimzi/documentation/modules/configuring/proc-manual-restart-connector.adoc", "line": 46, "phrase": "strimzi.io/restart=\"includeTasks,onlyFailed\""}, {"catalogue": "catalogues_a3", "entry": "V2", "file": "raw/apache-kafka/docs/kafka-connect/user-guide.md", "line": 308, "phrase": "the \"includeTasks\" parameter specifies whether to restart the connector instance and task instances"}], "template": "vbw_restart_annotation", "tier": "A", "vbw": {"artifact_language": "yaml", "control_artifact": "apiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: orders-sink\n  namespace: streaming\n  labels:\n    strimzi.io/cluster: orders-connect\n  annotations:\n    strimzi.io/restart: includeTasks\nspec:\n  class: org.apache.kafka.connect.file.FileStreamSinkConnector\n  tasksMax: 2\n  config:\n    file: /data/orders.txt\n    topics: orders", "mechanism": "V2", "oracle_checks": [5], "oracle_checks_on_control": [{"expect": {"include_tasks": true, "restart_connector": true}, "kind": "KafkaConnector", "oracle": "restart_annotation", "rule": "V2", "type": "semantic_rule"}], "role": "wrong", "rules": ["restart.connector_true_or_params", "restart.mm2_connector"], "shown_fails_oracle": true, "shown_valid": true, "shown_valid_note": "validates against the crd schema"}, "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "has_command"}, {"steps": [{"flags_all": ["--dry-run"], "flags_forbidden": [], "role": "safe", "tool": "argocd", "words": ["app", "sync"]}, {"flags_forbidden": ["--dry-run", "--prune"], "role": "mutate", "tool": "argocd", "words": ["app", "sync"]}], "type": "cmd_order"}, {"tool": "argocd", "type": "no_invented_cli"}], "dedup": {"max_containment": 0.556, "max_jaccard": 0.2, "nearest": "data/valid.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "safe_order", "family": "order_argo_sync:argocd-app-sync:web-frontend", "generator": "eval2_items_a3_order/1", "id": "a3-order_argo_sync-002", "kind": "safe_order", "perturbed": [{"check": "cmd_order", "label": "swap", "text": "```bash\nargocd app sync web-frontend\nargocd app sync web-frontend --dry-run\n```", "violates": 1}, {"check": "cmd_order", "label": "missing", "text": "```bash\nargocd app sync web-frontend\n```", "violates": 1}, {"check": "cmd_order", "label": "forbidden", "text": "```bash\nargocd app sync web-frontend --dry-run\nargocd app sync web-frontend --prune\n```", "violates": 1}], "perturbed_reference": "```bash\nargocd app sync web-frontend\nargocd app sync web-frontend --dry-run\n```", "perturbed_violates": 1, "question": "On a CI runner where `argocd` is logged in I have to sync the Argo CD application `web-frontend`, after checking what the sync would do without changing the cluster. Do not prune resources. Give the commands in the order I should run them.\n\nGive the commands in the order they should be run, in one code block, one command per line; use only options that exist for the tool.", "reference": "```bash\nargocd app sync web-frontend --dry-run\nargocd app sync web-frontend\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "order_argo_sync", "fact": "Preview apply without affecting cluster", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_sync.md", "line": 49}, {"catalogue": "argocd", "entry": "argocd app sync", "fact": "every option on the lines exists for the tool: --dry-run, --prune", "file": null, "line": null}], "template": "order_argo_sync", "tier": "A", "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "has_kafka_command"}, {"steps": [{"flags_all": ["--reset-offsets"], "flags_forbidden": ["--execute"], "role": "safe", "tool": "kafka-consumer-groups.sh"}, {"flags_all": ["--reset-offsets", "--execute"], "flags_forbidden": ["--all-topics"], "role": "mutate", "tool": "kafka-consumer-groups.sh"}], "type": "cmd_order"}, {"tool": "kafka-consumer-groups.sh", "type": "kcmd_known_flags"}, {"tool": "kafka-consumer-groups.sh", "type": "kcmd_constraints"}], "dedup": {"max_containment": 0.636, "max_jaccard": 0.412, "nearest": "eval/m4/open/tier_a.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "safe_order", "family": "order_cg_reset:kafka-consumer-groups:earliest", "generator": "eval2_items_a3_order/1", "id": "a3-order_cg_reset-001", "kind": "safe_order", "perturbed": [{"check": "cmd_order", "label": "swap", "text": "```bash\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --execute\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --dry-run\n```", "violates": 1}, {"check": "cmd_order", "label": "missing", "text": "```bash\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --execute\n```", "violates": 1}, {"check": "cmd_order", "label": "forbidden", "text": "```bash\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --dry-run\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --execute --all-topics\n```", "violates": 1}], "perturbed_reference": "```bash\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --execute\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --dry-run\n```", "perturbed_violates": 1, "question": "On a broker host I have to reset the offsets of the consumer group `orders` on the topic `orders-events` to the earliest offset. Only the topic `orders-events` may be reset, not all topics of the group. Give the commands in the order I should run them (bootstrap server `kafka:9092`).\n\nGive the commands in the order they should be run, in one code block, one command per line; use only options that exist for the tool.", "reference": "```bash\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --dry-run\nbin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --reset-offsets --group orders --topic orders-events --to-earliest --execute\n```", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "order_cg_reset", "fact": "(default) to display which offsets to reset", "file": "raw/apache-kafka/docs/operations/basic-kafka-operations.md", "line": 230}, {"catalogue": "pinned_text", "entry": "order_cg_reset", "fact": "to execute --reset-offsets process", "file": "raw/apache-kafka/docs/operations/basic-kafka-operations.md", "line": 231}, {"catalogue": "kafka_cli", "entry": "kafka-consumer-groups.sh", "fact": "every option on the lines exists for the tool: --reset-offsets, --group, --topic, --dry-run, --execute, --to-earliest, --all-topics", "file": null, "line": null}], "template": "order_cg_reset", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"tolerance": 0.01, "type": "number_within", "unit": "MiB/s", "value": 40}], "dedup": {"max_containment": 0.444, "max_jaccard": 0.16, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "arith_only", "family": "size_follower_traffic:20-3", "generator": "eval2_items_a3_sizing/1", "id": "a3-size_follower_traffic-001", "kind": "computed_sizing", "perturbed": [{"check": "number_within", "kind": "unit", "label": "MB/s against MiB/s", "text": "Worked out with the stated rule.\nANSWER: 40 MB/s", "violates": 1}, {"check": "number_within", "kind": "unit", "label": "MiB/min against MiB/s", "text": "Worked out with the stated rule.\nANSWER: 40 MiB/min", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "counted the leader copy too", "text": "Worked out with the stated rule.\nANSWER: 60 MiB/s", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "one follower only", "text": "Worked out with the stated rule.\nANSWER: 20 MiB/s", "violates": 1}], "perturbed_reference": "Worked out with the stated rule.\nANSWER: 40 MB/s", "perturbed_violates": 1, "question": "Producers write 20 MiB/s to a topic with replication factor 3. Each follower fetches the whole stream from the partition leaders. How much replication traffic do the followers pull in total, in MiB/s? Use: follower traffic = ingress x (replication factor - 1).\n\nFinish with a final line `ANSWER: <number> <unit>`.", "reference": "Worked out with the stated rule.\nANSWER: 40 MiB/s", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "size_follower_traffic", "fact": "Followers consume messages from the leader just as a normal Kafka consumer would", "file": "raw/apache-kafka/docs/design/design.md", "line": 287}], "template": "size_follower_traffic", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "chance": 0.0, "checks": [{"format": true, "type": "answer_line"}, {"tolerance": 0.01, "type": "number_within", "unit": "GiB", "value": 11.71875}], "dedup": {"max_containment": 0.375, "max_jaccard": 0.125, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "rule_from_docs", "family": "size_page_cache:400", "generator": "eval2_items_a3_sizing/1", "id": "a3-size_page_cache-002", "kind": "computed_sizing", "perturbed": [{"check": "number_within", "kind": "unit", "label": "GB against GiB", "text": "Worked out with the stated rule.\nANSWER: 11.72 GB", "violates": 1}, {"check": "number_within", "kind": "unit", "label": "MiB against GiB", "text": "Worked out with the stated rule.\nANSWER: 11.72 MiB", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "60 seconds instead of 30", "text": "Worked out with the stated rule.\nANSWER: 23.44 GiB", "violates": 1}, {"check": "number_within", "kind": "factor", "label": "forgot the buffering time", "text": "Worked out with the stated rule.\nANSWER: 0.3906 GiB", "violates": 1}], "perturbed_reference": "Worked out with the stated rule.\nANSWER: 11.72 GB", "perturbed_violates": 1, "question": "A Kafka cluster takes 400 MiB/s of writes. Using the back-of-the-envelope rule of the Kafka documentation for the memory needed to buffer active readers and writers, how much memory should the cluster have for that buffer, in GiB?\n\nFinish with a final line `ANSWER: <number> <unit>`.", "reference": "Worked out with the stated rule.\nANSWER: 11.72 GiB", "seed": "m4-a3-1", "sources": [{"catalogue": "pinned_text", "entry": "size_page_cache", "fact": "assuming you want to be able to buffer for 30 seconds and compute your memory need as write_throughput*30", "file": "raw/apache-kafka/docs/operations/hardware-and-os.md", "line": 31}], "template": "size_page_cache", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 1.0, "checks": [{"format": true, "type": "answer_line"}, {"type": "declines"}, {"allowed_identifiers": ["log.retention.hours", "prod-eu1"], "allowed_numbers": [], "type": "no_invented_value"}], "dedup": {"max_containment": 0.667, "max_jaccard": 0.4, "nearest": "eval/answers_1024_copy.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "not_covered", "family": "nc_deployment:gap:log.retention.hours", "generator": "eval2_items_a3_gap/1", "id": "a3-nc_deployment-005", "kind": "not_covered", "overlap": null, "overlap_band": "none", "pair": "nc:deployment:log.retention.hours", "perturbed": [{"check": "no_invented_value", "label": "invented_value", "text": "It is set to 72 there.\nANSWER: 72", "violates": 2}, {"check": "declines", "label": "confident_no_decline", "text": "That setting has a sensible built-in value that applies unless it is overridden.\nANSWER: the built-in value", "violates": 1}], "perturbed_reference": "It is set to 72 there.\nANSWER: 72", "perturbed_violates": 2, "question": "Which value of the broker setting `log.retention.hours` do the brokers of our cluster `prod-eu1` run with?\n\nFinish with a final line `ANSWER: <value>`. Give the bare value. If the documentation you have does not cover it, finish with `ANSWER: not in my sources`.", "reference": "The value of `log.retention.hours` for `prod-eu1` belongs to your own deployment; the documentation I have does not cover it.\nANSWER: not in my sources", "seed": "m4-a3-1", "sources": [{"absent": "prod-eu1", "catalogue": "frozen_index", "chunks_searched": 6444, "fact": "no chunk of the frozen index mentions the terms", "file": "work/v3/chunks_s1.jsonl", "hit_rule": "a chunk hits a term when the term is in its text, title or path exactly, ignoring case, or after dropping every non-alphanumeric character from both (all chunks of every source)", "hits": 0, "hits_per_term": {"prod-eu1": {"case_insensitive": 0, "exact": 0, "separator_insensitive": 0}}, "index_sha256": "233c23b0478006a8afae881b83836a226a6af89517256be4d9aac2cc7d500e92", "line": null, "terms": ["prod-eu1"]}], "template": "nc_deployment", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "catalogue_hash_a3": "1f696466faad70ee2d2cfc8c89872512a27ff813671bd7efc2450c86a84e3f50", "chance": 1.0, "checks": [{"format": true, "type": "answer_line"}, {"type": "declines"}, {"allowed_identifiers": ["quota.consumer.default"], "allowed_numbers": ["3.4"], "type": "no_invented_value"}], "dedup": {"max_containment": 0.0, "max_jaccard": 0.25, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "not_covered", "family": "nc_old_version:gap:quota.consumer.default", "generator": "eval2_items_a3_gap/1", "id": "a3-nc_old_version-001", "kind": "not_covered", "overlap": null, "overlap_band": "none", "pair": "nc:old_version:quota.consumer.default", "perturbed": [{"check": "no_invented_value", "label": "invented_value", "text": "The default is 9223372036854775807.\nANSWER: 9223372036854775807", "violates": 2}, {"check": "declines", "label": "confident_no_decline", "text": "That setting has a sensible built-in value that applies unless it is overridden.\nANSWER: the built-in value", "violates": 1}], "perturbed_reference": "The default is 9223372036854775807.\nANSWER: 9223372036854775807", "perturbed_violates": 2, "question": "Kafka 3.4: what was the default of the broker setting `quota.consumer.default` in that release?\n\nFinish with a final line `ANSWER: <value>`. Give the bare value. If the documentation you have does not cover it, finish with `ANSWER: not in my sources`.", "reference": "The documentation I have does not cover `quota.consumer.default` for Kafka 3.4, so I cannot give the default it had then.\nANSWER: not in my sources", "seed": "m4-a3-1", "sources": [{"absent": "quota.consumer.default", "catalogue": "frozen_index", "chunks_searched": 6444, "fact": "no chunk of the frozen index mentions the terms", "file": "work/v3/chunks_s1.jsonl", "hit_rule": "a chunk hits a term when the term is in its text, title or path exactly, ignoring case, or after dropping every non-alphanumeric character from both (all chunks of every source)", "hits": 0, "hits_per_term": {"quota.consumer.default": {"case_insensitive": 0, "exact": 0, "separator_insensitive": 0}}, "index_sha256": "233c23b0478006a8afae881b83836a226a6af89517256be4d9aac2cc7d500e92", "line": null, "terms": ["quota.consumer.default"]}, {"absent": "quota.consumer.default", "catalogue": "kafka_config", "entry": null, "fact": "the key is not in the Kafka 4.3 reference of scope broker", "file": null, "line": null, "scope": "broker"}], "template": "nc_old_version", "tier": "A", "versions": {"kafka": "3.4"}}
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
    """The split group of an item: items that belong together (an item and its control) share one."""
    if it.get("pair"):
        return "pair:%s" % it["pair"]
    v = it.get("vbw")
    if v:
        if v.get("mechanism") == "FK":
            return "pair:" + ":".join(it["family"].split(":")[:2])
        return "pair:vbw:%s" % v["mechanism"]
    return it["family"]


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
    """(open, hidden). Same method as eval2.split_families, with groups as families. prior = {"open": ids,
    "hidden": ids} of an earlier split: its groups stay on their side, the others are decided on the remaining
    targets. With no prior it returns exactly what split_families returns for the grouped items."""
    fam = defaultdict(list)
    for it in items:
        fam[group_of(it)].append(it)
    forced_open = {f for f, its in fam.items() if any(it.get("exposed") is True for it in its)}
    cells_total = Counter((it["tier"], it["kind"]) for it in items)
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
                got.update(Counter((it["tier"], it["kind"]) for it in its))
                fixed.add(f)
            elif side_o:
                fixed.add(f)
    for f in sorted((f for f in fam if f not in forced_open and f not in fixed), key=lambda f: eval2._order_key(seed, f)):
        add = Counter((it["tier"], it["kind"]) for it in fam[f])
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


def old_sides_hold(old_items, a3_open, a3_hidden, seed=SPLIT_SEED, share=SHARE):
    """In-memory proof that adding the a3 items to the 496 moves no old id and no a3 id: split_families over both
    gives the open ids of tier_a.jsonl on the old side and the a3 split on the new side. Returns the problems."""
    old_open = {json.loads(l)["id"] for l in OPEN_A1.read_text().splitlines() if l.strip()}
    both = list(old_items) + [{**i, "family": group_of(i)} for i in a3_open + a3_hidden]
    op, hd = eval2.split_families(both, seed, share)
    op_ids = {i["id"] for i in op}
    out = []
    old_ids = {i["id"] for i in old_items}
    if {i for i in op_ids if i in old_ids} != old_open:
        out.append("an old id moved: the open side of the 496 differs from eval/m4/open/tier_a.jsonl")
    if {i for i in op_ids if i not in old_ids} != {i["id"] for i in a3_open}:
        out.append("the a3 part of the combined split differs from the a3-alone split")
    return out


# ------------------------------------------------------------------ build
def git_commit():
    return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip() or None


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
    if with_predecessor:
        r["problems"] += old_sides_hold(old_items, op, hd)
    return r


def manifest_of(r, enc_sha=None):
    items, op, hd = r["items"], r["open"], r["hidden"]
    from kafka_sre_rag import catalogue_a3
    from kafka_sre_rag.catalogue import catalogue_hash, default_dir
    cell = lambda its: dict(sorted(Counter(i["kind"] for i in its).items()))  # noqa: E731
    return {
        "generator": VERSION, "generator_commit": git_commit(), "seed": SPLIT_SEED, "share": SHARE, "exposure_seed": SEED,
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
        "rule": "groups on one side (an item and its control together); families with any item exposed:true go open; "
                "share per (tier, kind) cell reached with the remaining groups (eval2_items_a3.split_extend, the method "
                "of scripts/eval2.py split_families)",
        "grouping": "valid_but_wrong by mechanism (forbidden-key template by resource kind); items with `pair` by it; "
                    "all other items by their own family",
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
