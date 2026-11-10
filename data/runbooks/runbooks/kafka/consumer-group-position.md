# Checking consumer position and managing consumer groups

| Field | Value |
|---|---|
| Product | Apache Kafka (KRaft) |
| Doc version | Apache Kafka 4.3 documentation, Operations > Basic Kafka Operations (docs source: apache/kafka tag 4.3.2, commit 0a6c96d3b61167f1141df2032f25f10fd6e118fe) |
| Sources | https://kafka.apache.org/43/operations/basic-kafka-operations/#checking-consumer-position ; https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups |
| License | Derived from: Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Version note.** These are the Kafka 4.3 docs (source at tag 4.3.2); Strimzi 1.3.1 supports Kafka 4.2.0 to 4.3.2 (see FINDINGS.md). The rendered page prints some long options with en dashes in prose (for example "–verbose"). The real options use two hyphens, as in the code blocks.

## Symptoms / trigger

You need to see where a consumer group is and how far it is behind the end of the log, for example when lag alerts fire ([../strimzi/consumer-lag.md](../strimzi/consumer-lag.md)), or you need to list, delete or reset groups.
— *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#checking-consumer-position (Kafka 4.3, retrieved 2026-11-10)

## Diagnosis steps

1. Show the position and lag of every consumer in a group:
   ```
   $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
   ```
   Output columns: `TOPIC`, `PARTITION`, `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`, `CONSUMER-ID`, `HOST`, `CLIENT-ID`.
   — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#checking-consumer-position (Kafka 4.3, retrieved 2026-11-10)
2. List all consumer groups:
   ```
   $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
   ```
   — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)
3. List the active members, then their partition assignments:
   ```
   $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members
   ```
   ```
   $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --members --verbose
   ```
   — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)
4. Show group-level information (coordinator, assignment strategy, state such as `Stable`, number of members):
   ```
   $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group --state
   ```
   `--offsets` is the default describe option and gives the same output as plain `--describe`.
   — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)
5. Authorization: for a group that uses the consumer protocol, the admin client needs DESCRIBE access to all topics the members subscribe to. The classic protocol does not need DESCRIBE on all topics.
   — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)

## Remediation steps

1. **[DESTRUCTIVE]** Delete one or more groups. Manual deletion only works if the group has no active members.
   ```
   $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group my-group --group my-other-group
   ```
   — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)
2. **[Changes consumer positions]** Reset the offsets of one group. Rules:
   - The consumer instances must be inactive first.
   - It works on one group at a time.
   - You must pick a scope, `--all-topics` or `--topic`, unless you use `--from-file`.
   - Execution options: the default, which only displays which offsets would be reset; `--execute`, which runs the reset; `--export`, which exports the results to CSV. The Kafka 4.3 docs name no flag for the default.
   - Scenarios: `--to-datetime`, `--to-earliest`, `--to-latest`, `--shift-by`, `--from-file`, `--to-current`, `--by-duration`, `--to-offset`.

   Example from the source. It has no `--execute`, so by the rule above it only displays which offsets would be reset:
   ```
   $ bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --group my-group --topic topic1 --to-latest
   ```
   — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)

## Verification

Run `--describe --group <group>` again and check `CURRENT-OFFSET` and `LAG`.
— *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#checking-consumer-position (Kafka 4.3, retrieved 2026-11-10)

## Rollback or caveats

- Out-of-range offsets are moved to the available end. Example: if the end offset is 10 and you ask to shift by 15, offset 10 is used.
  — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)
- Groups are also deleted automatically when their last committed offset expires.
  — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)
- `--export` writes the reset information as CSV, which can later be used with `--from-file`. The source does not describe this as a rollback method.
  — *Source:* https://kafka.apache.org/43/operations/basic-kafka-operations/#managing-consumer-groups (Kafka 4.3, retrieved 2026-11-10)

## Related runbooks

- [../strimzi/consumer-lag.md](../strimzi/consumer-lag.md)
- [monitoring-key-metrics.md](monitoring-key-metrics.md)
