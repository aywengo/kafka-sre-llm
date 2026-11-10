# Debugging the KRaft metadata quorum

| Field | Value |
|---|---|
| Product | Apache Kafka (KRaft) |
| Doc version | Apache Kafka 4.4 documentation, Operations > KRaft and Operations > Monitoring (docs source: apache/kafka tag 4.4.0, commit 1156b2752aaa28eafc0b4c4d387e63356793268d, `docs/operations/kraft.md`, `docs/operations/monitoring.md`) |
| Sources | https://kafka.apache.org/44/operations/kraft/#metadata-quorum-tool ; https://kafka.apache.org/44/operations/kraft/#dump-log-tool ; https://kafka.apache.org/44/operations/kraft/#metadata-shell ; https://kafka.apache.org/44/operations/monitoring/#kraft-quorum-monitoring-metrics ; https://kafka.apache.org/44/operations/monitoring/#kraft-controller-monitoring-metrics ; https://kafka.apache.org/44/operations/monitoring/#kraft-broker-monitoring-metrics ; https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums ; https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-rules.yaml ; https://kafka.apache.org/44/operations/kraft/#add-new-controller ; https://kafka.apache.org/44/operations/monitoring/#kraft-monitoring-metrics |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed; Strimzi repo example files (strimzi-kafka-operator tag 1.3.1), Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0); Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Version note.** These are Kafka 4.4 docs; Strimzi 1.3.1 supports Kafka up to 4.3.2 (see FINDINGS.md). The Strimzi docs fetched here do not describe running these tools inside Strimzi pods. NO OFFICIAL SOURCE FOUND for the pod-level invocation on Strimzi. Strimzi 1.3.1 uses static controller quorums for all deployments (https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums, Strimzi 1.3.1, retrieved 2026-11-10), so on Strimzi expect `kraft.version` level 0 and do not use the dynamic-quorum commands in [kraft-controller-membership.md](kraft-controller-membership.md).

## Symptoms / trigger

The source gives no symptom list for these tools. Related signals from the Strimzi example rules and the Kafka metric descriptions:
- `AbnormalControllerState`: the sum of `ActiveControllerCount` is not 1 ([../strimzi/prometheus-alert-rules.md](../strimzi/prometheus-alert-rules.md)).
- Controller metric `ActiveControllerCount`: valid values per node are `0` or `1`.
- Controller metric `MetadataErrorCount`: the number of errors the controller hit while processing the metadata log.
- Broker metrics `Metadata Load Error Count` and `Metadata Apply Error Count`.

— *Source:* https://kafka.apache.org/44/operations/monitoring/#kraft-controller-monitoring-metrics ; https://kafka.apache.org/44/operations/monitoring/#kraft-broker-monitoring-metrics (Kafka 4.4, retrieved 2026-11-10); https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-rules.yaml (Strimzi repo tag 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Get a summary of the runtime state of the metadata partition:
   ```
   $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
   ```
   The output includes `ClusterId`, `LeaderId`, `LeaderEpoch`, `HighWatermark`, `MaxFollowerLag`, `MaxFollowerLagTimeMs`, `CurrentVoters` and `CurrentObservers`.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#metadata-quorum-tool (Kafka 4.4, retrieved 2026-11-10)
2. Watch replication to the controllers with `bin/kafka-metadata-quorum.sh describe --replication`. The docs mention this command in the add-controller procedure.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#add-new-controller (Kafka 4.4, retrieved 2026-11-10)
3. Decode metadata log segments and snapshots:
   ```
   $ bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000000.log
   ```
   ```
   $ bin/kafka-dump-log.sh --cluster-metadata-decoder --files metadata_log_dir/__cluster_metadata-0/00000000000000000100-0000000001.checkpoint
   ```
   — *Source:* https://kafka.apache.org/44/operations/kraft/#dump-log-tool (Kafka 4.4, retrieved 2026-11-10)
4. Inspect the metadata state interactively from a snapshot (`ls /`, `ls /topics`, `cat /topics/foo/0/data`, `exit`):
   ```
   $ bin/kafka-metadata-shell.sh --snapshot metadata_log_dir/__cluster_metadata-0/00000000000000007228-0000000001.checkpoint
   ```
   — *Source:* https://kafka.apache.org/44/operations/kraft/#metadata-shell (Kafka 4.4, retrieved 2026-11-10)
5. Check the quorum metrics (MBean `kafka.server:type=raft-metrics`): `Current State` (leader, candidate, voted, follower, unattached, observer), `Current Leader` (`-1` means unknown), `Current Epoch`, `High Watermark` (`-1` if unknown), commit latency and election latency. Also check `LastAppliedRecordLagMs`. On the active controller it is always zero; on brokers it is in the `broker-metadata-metrics` group.
   — *Source:* https://kafka.apache.org/44/operations/monitoring/#kraft-quorum-monitoring-metrics ; https://kafka.apache.org/44/operations/monitoring/#kraft-controller-monitoring-metrics ; https://kafka.apache.org/44/operations/monitoring/#kraft-broker-monitoring-metrics (Kafka 4.4, retrieved 2026-11-10)

## Remediation steps

NO OFFICIAL SOURCE FOUND. The Kafka 4.4 KRaft "Debugging" section only describes inspection tools. For changing controller membership see [kraft-controller-membership.md](kraft-controller-membership.md).

## Verification

NO OFFICIAL SOURCE FOUND.

## Rollback or caveats

- `00000000000000000000-0000000000.checkpoint` contains no cluster metadata. Give `kafka-metadata-shell.sh` a valid snapshot file.
  — *Source:* https://kafka.apache.org/44/operations/kraft/#metadata-shell (Kafka 4.4, retrieved 2026-11-10)
- Some metrics depend on the node's role as set by `process.roles`.
  — *Source:* https://kafka.apache.org/44/operations/monitoring/#kraft-monitoring-metrics (Kafka 4.4, retrieved 2026-11-10)

## Related runbooks

- [kraft-controller-membership.md](kraft-controller-membership.md)
- [monitoring-key-metrics.md](monitoring-key-metrics.md)
- [../strimzi/prometheus-alert-rules.md](../strimzi/prometheus-alert-rules.md)
