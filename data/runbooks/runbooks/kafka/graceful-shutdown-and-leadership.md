# Graceful broker shutdown and restoring preferred leadership

| Field | Value |
|---|---|
| Product | Apache Kafka (KRaft) |
| Doc version | Apache Kafka 4.4 documentation, Operations > Basic Kafka Operations (docs source: apache/kafka tag 4.4.0, commit 1156b2752aaa28eafc0b4c4d387e63356793268d, `docs/operations/basic-kafka-operations.md`) |
| Sources | https://kafka.apache.org/44/operations/basic-kafka-operations/#graceful-shutdown ; https://kafka.apache.org/44/operations/basic-kafka-operations/#balancing-leadership ; https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor |
| License | Derived from: Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands and config quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Version note.** Strimzi 1.3.1 supports Kafka up to 4.3.2, while these docs are for Kafka 4.4 (see FINDINGS.md). On Strimzi, brokers are restarted through the Cluster Operator ([../strimzi/manual-rolling-update.md](../strimzi/manual-rolling-update.md)), not by stopping processes by hand.

## Symptoms / trigger

- A broker is being stopped on purpose for maintenance or a configuration change.
- After a broker restarts, it is a follower for all its partitions, so it serves no client reads or writes and leadership is imbalanced.

— *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#graceful-shutdown ; https://kafka.apache.org/44/operations/basic-kafka-operations/#balancing-leadership (Kafka 4.4, retrieved 2026-11-10)

## Diagnosis steps

1. Kafka detects any broker shutdown or failure on its own and elects new leaders for that broker's partitions, whether the stop was planned or not.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#graceful-shutdown (Kafka 4.4, retrieved 2026-11-10)
2. Controlled shutdown only succeeds if every partition on the broker has other replicas: replication factor greater than 1 and at least one of those replicas alive.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#graceful-shutdown (Kafka 4.4, retrieved 2026-11-10)
3. Preferred replica: for a replica list `1,5,9`, node 1 is the preferred leader because it comes first in the list.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#balancing-leadership (Kafka 4.4, retrieved 2026-11-10)

## Remediation steps

1. **[RESTART]** Stop brokers gracefully, not with a hard kill. A graceful stop does two things:
   - It syncs all logs to disk, so no log recovery is needed on restart.
   - It moves leadership of the broker's partitions to other replicas before shutting down, so each partition is unavailable for only a few milliseconds.

   Log syncing happens on any stop except a hard kill. The controlled leadership migration needs this setting:
   ```
   controlled.shutdown.enable=true
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#graceful-shutdown (Kafka 4.4, retrieved 2026-11-10)
2. By default, Kafka tries to move leadership back to the preferred replicas. This is controlled by:
   ```
   auto.leader.rebalance.enable=true
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#balancing-leadership (Kafka 4.4, retrieved 2026-11-10)
3. If the setting is `false`, restore leadership by hand. **[Changes partition leadership]**
   ```
   $ bin/kafka-leader-election.sh --bootstrap-server localhost:9092 --election-type preferred --all-topic-partitions
   ```
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#balancing-leadership (Kafka 4.4, retrieved 2026-11-10)

## Verification

NO OFFICIAL SOURCE FOUND for a verification step in these sections. Leadership can be inspected with `kafka-topics.sh --describe`, whose output (`Leader:`, `Replicas:`, `Isr:`) is shown in the replication-factor section; see [partition-reassignment-and-throttling.md](partition-reassignment-and-throttling.md).
— *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#increasing-replication-factor (Kafka 4.4, retrieved 2026-11-10)

## Rollback or caveats

- Shutting down the last replica of a partition makes that partition unavailable. This is why controlled shutdown needs live replicas elsewhere.
  — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#graceful-shutdown (Kafka 4.4, retrieved 2026-11-10)

## Related runbooks

- [partition-reassignment-and-throttling.md](partition-reassignment-and-throttling.md)
- [monitoring-key-metrics.md](monitoring-key-metrics.md)
- [../strimzi/manual-rolling-update.md](../strimzi/manual-rolling-update.md)
