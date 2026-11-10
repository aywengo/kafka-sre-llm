# Partition reassignment: expanding, decommissioning, increasing replication factor, throttling

| Field | Value |
|---|---|
| Product | Apache Kafka (KRaft) |
| Doc version | Apache Kafka 4.4 documentation, Operations > Basic Kafka Operations (docs source: apache/kafka tag 4.4.0, commit 1156b2752aaa28eafc0b4c4d387e63356793268d) |
| Sources | https://kafka.apache.org/44/operations/basic-kafka-operations/#expanding-your-cluster ; https://kafka.apache.org/44/operations/basic-kafka-operations/#automatically-migrating-data-to-new-machines ; https://kafka.apache.org/44/operations/basic-kafka-operations/#decommissioning-brokers ; https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor ; https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration ; https://kafka.apache.org/44/operations/basic-kafka-operations/#safe-usage-of-throttled-replication |
| License | Derived from the Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Version note.** These are Kafka 4.4 docs; Strimzi 1.3.1 supports Kafka up to 4.3.2. The cordon step (`cordoned.log.dirs`) appears in the 4.4 docs, and these runbooks do not establish whether Kafka 4.3.2 has it (see FINDINGS.md). For scaling on Strimzi, the Strimzi docs also describe the Cruise Control `KafkaRebalance` modes `add-brokers` and `remove-brokers`; see [../strimzi/kafkarebalance-notready-and-refresh.md](../strimzi/kafkarebalance-notready-and-refresh.md).

> **[DATA MOVEMENT]** Every `--execute` in this runbook moves replica data between brokers.

## Symptoms / trigger

- New brokers were added but get no partitions until new topics are created.
- A broker must be decommissioned.
- A partition's replication factor must be increased.
- Reassignment traffic has to be limited so it does not affect users.

— *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#expanding-your-cluster ; https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration (Kafka 4.4, retrieved 2026-11-10)

## Diagnosis steps

1. The tool `kafka-reassign-partitions.sh` has three mutually exclusive modes: `--generate` (proposes a candidate plan for given topics and target brokers), `--execute` (starts the reassignment from a JSON plan) and `--verify` (status of the partitions listed in the last `--execute`: completed, failed or in progress). It does not study data distribution by itself; the admin decides what to move.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#expanding-your-cluster (Kafka 4.4, retrieved 2026-11-10)
2. Describe a topic to see `Leader`, `Replicas` and `Isr`:
   ```
   $ bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic foo --describe
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor (Kafka 4.4, retrieved 2026-11-10)

## Remediation steps

### A. Move whole topics to new brokers

1. Write the topics to move in a JSON file (`topics-to-move.json`, with `"topics": [{ "topic": "foo1" }, { "topic": "foo2" }]` and `"version": 1`). Then generate a candidate plan:
   ```
   $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --topics-to-move-json-file topics-to-move.json --broker-list "5,6" --generate
   ```
   Nothing moves yet. **Save the "Current partition replica assignment" for rollback**, and save the proposed plan to a file such as `expand-cluster-reassignment.json`.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#automatically-migrating-data-to-new-machines (Kafka 4.4, retrieved 2026-11-10)
2. **[DATA MOVEMENT]** Execute the plan:
   ```
   $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file expand-cluster-reassignment.json --execute
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#automatically-migrating-data-to-new-machines (Kafka 4.4, retrieved 2026-11-10)

### B. Decommission a broker

3. Cordon the broker through the Admin API, for example broker 1:
   ```
   $ bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --add-config cordoned.log.dirs="*" --entity-type brokers --entity-name 1
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#decommissioning-brokers (Kafka 4.4, retrieved 2026-11-10)
4. **[DATA MOVEMENT]** Reassign all partitions from that broker to other brokers. The tool cannot generate a decommissioning plan yet, so write the plan by hand.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#decommissioning-brokers (Kafka 4.4, retrieved 2026-11-10)
5. **[DESTRUCTIVE]** When all reassignments are done, shut down the broker and unregister it:
   ```
   $ bin/kafka-cluster.sh unregister --bootstrap-server localhost:9092 --id 1
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#decommissioning-brokers (Kafka 4.4, retrieved 2026-11-10)

### C. Increase the replication factor

6. Write a custom plan that adds the extra replicas, for example `{"version":1, "partitions":[{"topic":"foo","partition":0,"replicas":[5,6,7]}]}` in `increase-replication-factor.json`. **[DATA MOVEMENT]** Then execute it:
   ```
   $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --reassignment-json-file increase-replication-factor.json --execute
   ```
   The output prints the current assignment with "Save this to use as the --reassignment-json-file option during rollback".
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor (Kafka 4.4, retrieved 2026-11-10)

### D. Throttle the data movement

7. Apply a throttle when you start the reassignment, which the source calls the simplest and safest way. Example: at most 50 MB/s between brokers and 100 MB/s between disks on one broker:
   ```
   $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --execute --reassignment-json-file bigger-cluster.json --throttle 50000000 --replica-alter-log-dirs-throttle 100000000
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration (Kafka 4.4, retrieved 2026-11-10)
8. To change the throttle during the reassignment, run execute again with `--additional` and the same JSON file:
   ```
   $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --additional --execute --reassignment-json-file bigger-cluster.json --throttle 700000000
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration (Kafka 4.4, retrieved 2026-11-10)
9. If replication is not making progress, raise the throttle. This happens when `max(BytesInPerSec) > throttle`. Watch `kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=([-.\w]+),topic=([-.\w]+),partition=([0-9]+)`, which should keep going down.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#safe-usage-of-throttled-replication (Kafka 4.4, retrieved 2026-11-10)

## Verification

1. Verify with the **same** JSON file used for `--execute`. When the reassignment has finished, `--verify` also removes the throttles:
   ```
   $ bin/kafka-reassign-partitions.sh --bootstrap-server localhost:9092 --verify --reassignment-json-file bigger-cluster.json
   ```
   Expect `Reassignment of partition [...] is completed`, and with throttles, `Clearing broker-level throttles on brokers ...` and `Clearing topic-level throttles on topic ...`.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration ; https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor (Kafka 4.4, retrieved 2026-11-10)
2. Check the throttle configuration:
   ```
   $ bin/kafka-configs.sh --describe --bootstrap-server localhost:9092 --entity-type brokers
   ```
   ```
   $ bin/kafka-configs.sh --describe --bootstrap-server localhost:9092 --entity-type topics
   ```
   The broker-level rates are `leader.replication.throttled.rate`, `follower.replication.throttled.rate` and `replica.alter.log.dirs.io.max.bytes.per.second`. The per-topic replica lists are `leader.replication.throttled.replicas` and `follower.replication.throttled.replicas`. `kafka-reassign-partitions.sh` sets all five.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration (Kafka 4.4, retrieved 2026-11-10)
3. For a replication-factor change, `kafka-topics.sh --describe` should show the new `ReplicationFactor` and `Replicas`/`Isr`.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor (Kafka 4.4, retrieved 2026-11-10)

## Rollback or caveats

- **Rollback:** both `--generate` and `--execute` print the current assignment. Save it so you can pass it as `--reassignment-json-file` to roll back.
  — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#automatically-migrating-data-to-new-machines ; https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor (Kafka 4.4, retrieved 2026-11-10)
- Remove throttles promptly after the reassignment by running `--verify`. Throttles left in place can throttle normal replication traffic.
  — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration ; https://kafka.apache.org/44/operations/basic-kafka-operations/#safe-usage-of-throttled-replication (Kafka 4.4, retrieved 2026-11-10)
- By default, the leader throttle applies to all replicas that existed before the rebalance, and the follower throttle to all move destinations.
  — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#limiting-bandwidth-usage-during-data-migration (Kafka 4.4, retrieved 2026-11-10)

## Related runbooks

- [graceful-shutdown-and-leadership.md](graceful-shutdown-and-leadership.md)
- [monitoring-key-metrics.md](monitoring-key-metrics.md)
- [../strimzi/kafkarebalance-notready-and-refresh.md](../strimzi/kafkarebalance-notready-and-refresh.md)
