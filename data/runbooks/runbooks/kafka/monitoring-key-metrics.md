# Key broker metrics and their normal values

| Field | Value |
|---|---|
| Product | Apache Kafka (KRaft) |
| Doc version | Apache Kafka 4.4 documentation, Operations > Monitoring (docs source: apache/kafka tag 4.4.0, commit 1156b2752aaa28eafc0b4c4d387e63356793268d, `docs/operations/monitoring.md`) |
| Sources | https://kafka.apache.org/44/operations/monitoring/#security-considerations-for-remote-monitoring-using-jmx (the broker metrics table has no anchor of its own and sits under this heading); https://kafka.apache.org/44/operations/monitoring/#kraft-controller-monitoring-metrics |
| License | Derived from the Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). MBean names and normal values quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Version note.** These are Kafka 4.4 docs; Strimzi 1.3.1 supports Kafka up to 4.3.2 (see FINDINGS.md). In Strimzi's example Prometheus rules, these MBeans appear under Prometheus names such as `kafka_server_replicamanager_underreplicatedpartitions`; see [../strimzi/prometheus-alert-rules.md](../strimzi/prometheus-alert-rules.md).

## Symptoms / trigger

A broker metric is outside the normal value that the Kafka docs list, or a Strimzi example alert built on it fires. The Kafka docs say they graph and alert on these metrics.
— *Source:* https://kafka.apache.org/44/operations/monitoring/#security-considerations-for-remote-monitoring-using-jmx (Kafka 4.4, retrieved 2026-11-10)

## Diagnosis steps: compare with the documented normal values

| Description | MBean | Normal value (from the source) |
|---|---|---|
| # of under replicated partitions (\|ISR\| < \|all replicas\|) | `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | 0 |
| # of under minIsr partitions (\|ISR\| < min.insync.replicas) | `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | 0 |
| # of at minIsr partitions (\|ISR\| = min.insync.replicas) | `kafka.server:type=ReplicaManager,name=AtMinIsrPartitionCount` | 0 |
| # of offline log directories | `kafka.log:type=LogManager,name=OfflineLogDirectoryCount` | 0 |
| Is controller active on broker | `kafka.controller:type=KafkaController,name=ActiveControllerCount` | only one broker in the cluster should have 1 |
| Leader election rate | `kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs` | non-zero when there are broker failures |
| Unclean leader election rate | `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec` | 0 |
| Leader replica counts | `kafka.server:type=ReplicaManager,name=LeaderCount` | mostly even across brokers |
| ISR shrink rate | `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` | If a broker goes down, ISR for some partitions shrinks; it expands again once the replicas are fully caught up. Otherwise the expected value is 0. |
| ISR expansion rate | `kafka.server:type=ReplicaManager,name=IsrExpandsPerSec` | see ISR shrink rate |
| Max lag in messages btw follower and leader replicas | `kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica` | lag should be proportional to the maximum batch size of a produce request |
| Average fraction of time the network processors are idle | `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | between 0 and 1, ideally > 0.3 |
| Average fraction of time the request handler threads are idle | `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | between 0 and 1, ideally > 0.3 |

— *Source:* https://kafka.apache.org/44/operations/monitoring/#security-considerations-for-remote-monitoring-using-jmx (Kafka 4.4, retrieved 2026-11-10)

KRaft controller metrics with no "normal value" column in the source: `ActiveControllerCount` per node is `0` or `1`; `FencedBrokerCount` is the number of fenced brokers this controller sees; `ActiveBrokerCount` is the number of active brokers; `OfflinePartitionsCount` is the number of offline non-internal partitions; `MetadataErrorCount` counts errors while processing the metadata log.
— *Source:* https://kafka.apache.org/44/operations/monitoring/#kraft-controller-monitoring-metrics (Kafka 4.4, retrieved 2026-11-10)

## Remediation steps

NO OFFICIAL SOURCE FOUND in the Kafka monitoring page. It lists metrics and normal values but no remediation. For related procedures see:
- Cross-link, NO OFFICIAL SOURCE FOUND for this route: under-replicated partitions or offline log directories on Strimzi, when brokers are unavailable because of storage: [../strimzi/broker-storage-issues.md](../strimzi/broker-storage-issues.md) (its last step is destructive)
- Cross-link, NO OFFICIAL SOURCE FOUND for this route: leadership imbalance (`LeaderCount` not even): [graceful-shutdown-and-leadership.md](graceful-shutdown-and-leadership.md)
- Cross-link, NO OFFICIAL SOURCE FOUND for this route: controller state: [kraft-quorum-debugging.md](kraft-quorum-debugging.md)

## Verification

NO OFFICIAL SOURCE FOUND for a verification step. The normal values to compare against are in the table above.

## Rollback or caveats

- Apache Kafka disables remote JMX by default. You can enable it with the `JMX_PORT` environment variable for processes started from the CLI, or with standard Java system properties. In production, enable security when you enable remote JMX.
  — *Source:* https://kafka.apache.org/44/operations/monitoring/#security-considerations-for-remote-monitoring-using-jmx (Kafka 4.4, retrieved 2026-11-10)

## Related runbooks

- [../strimzi/prometheus-alert-rules.md](../strimzi/prometheus-alert-rules.md)
- [kraft-quorum-debugging.md](kraft-quorum-debugging.md)
- [partition-reassignment-and-throttling.md](partition-reassignment-and-throttling.md)
