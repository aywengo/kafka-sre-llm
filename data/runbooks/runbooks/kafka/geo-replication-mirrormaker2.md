# Operating geo-replication with MirrorMaker 2 (Apache Kafka)

| Field | Value |
|---|---|
| Product | Apache Kafka MirrorMaker 2 |
| Doc version | Apache Kafka 4.4 documentation, Operations > Geo-Replication (docs source: apache/kafka tag 4.4.0, commit 1156b2752aaa28eafc0b4c4d387e63356793268d) |
| Sources | https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#geo-replication-overview ; https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#starting-geo-replication ; https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#stopping-geo-replication ; https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#applying-configuration-changes ; https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication ; https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#preventing-configuration-conflicts |
| License | Derived from: Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands and metric names quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Strimzi note.** This describes standalone MirrorMaker processes (`connect-mirror-maker.sh`). On Strimzi, MirrorMaker 2 runs as a `KafkaMirrorMaker2` resource managed by the Cluster Operator; for connector and task restarts there, see [../strimzi/mirrormaker2-restart-connector-and-task.md](../strimzi/mirrormaker2-restart-connector-and-task.md). Kafka 4.4 docs; Strimzi 1.3.1 supports Kafka up to 4.3.2.

## Symptoms / trigger

You need to check that every defined replication flow is up and working, or to start, stop or reconfigure MirrorMaker processes. MirrorMaker 2 is built on Kafka Connect. It replicates topics (data and configuration), consumer groups with offsets, and ACLs.
— *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication ; https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#geo-replication-overview (Kafka 4.4, retrieved 2026-11-10)

## Diagnosis steps

1. MirrorMaker inherits all Kafka Connect metrics, such as `source-record-poll-rate`, and adds its own under the `kafka.connect.mirror` group. They are tagged `source`, `target`, `topic` (the replicated topic on the target) and `partition`. Example: `topic1` replicated primary->secondary shows up as `target=secondary`, `topic=primary.topic1`.
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication (Kafka 4.4, retrieved 2026-11-10)
2. Replication metrics, MBean `kafka.connect.mirror:type=MirrorSourceConnector,...`: `record-count`, `record-rate`, `record-age-ms` (min/max/avg), `replication-latency-ms` (min/max/avg; the time it takes records to propagate source->target), `byte-rate`, `byte-count`.
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication (Kafka 4.4, retrieved 2026-11-10)
3. Offset-replication metrics, MBean `kafka.connect.mirror:type=MirrorCheckpointConnector,...`: `checkpoint-latency-ms` (min/max/avg; the time it takes to replicate consumer offsets).
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication (Kafka 4.4, retrieved 2026-11-10)
4. If one of two topics configured for the same target is not replicated, check for configuration conflicts. MirrorMaker processes share configuration through their target cluster, so different configurations against the same target race. Example: one process sets `A->B.topics = foo`, another sets `A->B.topics = bar`; only one of the two topics is replicated, depending on which process is the elected leader.
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#preventing-configuration-conflicts (Kafka 4.4, retrieved 2026-11-10)
5. Consumer groups created by `kafka-console-consumer.sh` are not replicated by default (default `groups.exclude = console-consumer-.*, connect-.*, __.*`).
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#starting-geo-replication (Kafka 4.4, retrieved 2026-11-10)

## Remediation steps

1. Start a MirrorMaker process:
   ```
   $ bin/connect-mirror-maker.sh connect-mirror-maker.properties
   ```
   To produce only to nearby clusters, add `--clusters` (the alias must be defined in the configuration file):
   ```
   $ bin/connect-mirror-maker.sh connect-mirror-maker.properties --clusters us-west
   ```
   To increase replication throughput, one option is to run more MirrorMaker processes in parallel. Processes that replicate the same clusters find each other and share the work.
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#starting-geo-replication (Kafka 4.4, retrieved 2026-11-10)
2. **[RESTART]** Configuration changes take effect only after the MirrorMaker processes are restarted.
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#applying-configuration-changes (Kafka 4.4, retrieved 2026-11-10)
3. Stop a process with SIGTERM:
   ```
   $ kill <MirrorMaker pid>
   ```
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#stopping-geo-replication (Kafka 4.4, retrieved 2026-11-10)
4. To avoid conflicts, keep the MirrorMaker configuration the same across all flows to one target cluster, for example with automation or one shared configuration file.
   — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#preventing-configuration-conflicts (Kafka 4.4, retrieved 2026-11-10)

## Verification

After startup, it can take a few minutes before a MirrorMaker process starts replicating. NO OFFICIAL SOURCE FOUND for an explicit verification step. The monitoring section recommends monitoring the MirrorMaker processes, using the metrics above, to make sure all defined replication flows are running correctly.
— *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#starting-geo-replication ; https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication (Kafka 4.4, retrieved 2026-11-10)

## Rollback or caveats

- These metrics do not distinguish between created-at and log-append timestamps.
  — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication (Kafka 4.4, retrieved 2026-11-10)
- If you change `groups.exclude` for testing, change it back afterwards.
  — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#starting-geo-replication (Kafka 4.4, retrieved 2026-11-10)
- The MBean patterns in the docs read `([-.w]+)`, while other Kafka MBean patterns use `([-.\w]+)`. The backslash is missing in both the published page and the docs source (see FINDINGS.md).
  — *Source:* https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/#monitoring-geo-replication (Kafka 4.4, retrieved 2026-11-10)

## Related runbooks

- [../strimzi/mirrormaker2-restart-connector-and-task.md](../strimzi/mirrormaker2-restart-connector-and-task.md)
- [monitoring-key-metrics.md](monitoring-key-metrics.md)
