# Strimzi example Prometheus alert rules: what each alert means

| Field | Value |
|---|---|
| Product | Strimzi example metrics configuration, KRaft |
| Doc version | Strimzi 1.3.1: "Deploying and Managing Strimzi" §25.1.3, §25.2.3; repo files at tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-alertmanager-examples-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-str ; https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules ; https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/documentation/modules/metrics/proc_metrics-deploying-prometheus.adoc |
| License | Derived from Strimzi documentation (CC BY 4.0, https://creativecommons.org/licenses/by/4.0/) and Strimzi example files (Apache-2.0), © the Strimzi authors. Rule names, expressions and annotation text quoted verbatim. |
| Retrieved | 2026-11-10 |

> **No `runbook_url`.** None of the example rules in Strimzi 1.3.1 has a `runbook_url` annotation. The only annotations are `summary` and `description`. The "Related runbook" column below is this compilation's own cross-link; the rule files do not contain it.

## Symptoms / trigger

An alert from the Strimzi example rules fires. Alerting rules are PromQL expressions that Prometheus evaluates continuously. When an expression is true for the rule's `for` period, Prometheus sends the alert to Alertmanager, which sends the notification.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-alertmanager-examples-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps: look up the alert

All rule files are `PrometheusRule` resources with labels `role: alert-rules` and `app: strimzi`.
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules (repo tag 1.3.1, retrieved 2026-11-10)

### `prometheus-kafka-rules.yaml` (group `kafka`)
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-rules.yaml (repo tag 1.3.1, retrieved 2026-11-10)

| Alert | Expression (abridged where marked) | for | severity | summary | Related runbook |
|---|---|---|---|---|---|
| `KafkaRunningOutOfSpace` | `kubelet_volume_stats_available_bytes * 100 / kubelet_volume_stats_capacity_bytes` joined to Kafka pods, `< 15` (abridged) | 10s | warning | Kafka is running out of free disk space | [broker-storage-issues.md](broker-storage-issues.md) |
| `UnderReplicatedPartitions` | `kafka_server_replicamanager_underreplicatedpartitions > 0` | 10s | warning | Kafka under replicated partitions | [broker-storage-issues.md](broker-storage-issues.md), [../kafka/monitoring-key-metrics.md](../kafka/monitoring-key-metrics.md) |
| `AbnormalControllerState` | `sum(kafka_controller_kafkacontroller_activecontrollercount) by (strimzi_io_name, namespace) != 1` | 10s | warning | Kafka abnormal controller state | [../kafka/kraft-quorum-debugging.md](../kafka/kraft-quorum-debugging.md) |
| `OfflinePartitions` | `sum(kafka_controller_kafkacontroller_offlinepartitionscount) by (namespace, pod) > 0` | 10s | warning | Kafka offline partitions | [../kafka/monitoring-key-metrics.md](../kafka/monitoring-key-metrics.md) |
| `UnderMinIsrPartitionCount` | `kafka_server_replicamanager_underminisrpartitioncount > 0` | 10s | warning | Kafka under min ISR partitions | [../kafka/monitoring-key-metrics.md](../kafka/monitoring-key-metrics.md) |
| `OfflineLogDirectoryCount` | `kafka_log_logmanager_offlinelogdirectorycount > 0` | 10s | warning | Kafka offline log directories | [broker-storage-issues.md](broker-storage-issues.md) |
| `ScrapeProblem` | `up{namespace!~"openshift-.+",strimzi_io_component_type="kafka",strimzi_io_broker_role="true"} == 0` | 3m | major | Prometheus unable to scrape metrics from … | NO OFFICIAL SOURCE FOUND |
| `KafkaNodeContainersDown` | `absent(container_last_seen{container="kafka"} and on(namespace,pod) …)` (abridged) | 3m | major | All `kafka` containers down or in CrashLookBackOff status | [kafka-restart-events.md](kafka-restart-events.md) |
| `KafkaContainerRestartedInTheLast5Minutes` | restart count over 5m `> 2 *` container count (abridged) | 5m | warning | One or more Kafka containers restarted too often | [kafka-restart-events.md](kafka-restart-events.md) |

### `prometheus-kafka-exporter-topic-rules-group.yaml` (group `kafka-exporter-topic`)
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-exporter-topic-rules-group.yaml (repo tag 1.3.1, retrieved 2026-11-10)

| Alert | Expression | for | severity | summary | Related runbook |
|---|---|---|---|---|---|
| `UnderReplicatedPartition` | `kafka_topic_partition_under_replicated_partition > 0` | 10s | warning | Topic has under-replicated partitions | [broker-storage-issues.md](broker-storage-issues.md) |
| `TooLargeConsumerGroupLag` | `kafka_consumergroup_lag > 1000` | 10s | warning | Consumer group lag is too big | [consumer-lag.md](consumer-lag.md) |
| `NoMessageForTooLong` | `changes(kafka_topic_partition_current_offset[10m]) == 0` | 10s | warning | No message for 10 minutes | NO OFFICIAL SOURCE FOUND |

### `prometheus-kafka-certificate-rules.yaml` (group `kafka-certificates`)
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-certificate-rules.yaml (repo tag 1.3.1, retrieved 2026-11-10)

| Alert | Expression | for | severity | summary | Related runbook |
|---|---|---|---|---|---|
| `CertificateExpiration` | `strimzi_certificate_expiration_timestamp_ms/1000 - time() < 30 * 24 * 60 * 60` | 5m | warning | Certificate will expire in less than 30 days | [expired-ca-certificates.md](expired-ca-certificates.md) (after expiry) |

### `prometheus-kafka-connect-rules.yaml` (group `kafka-connect`)
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-connect-rules.yaml (repo tag 1.3.1, retrieved 2026-11-10)

| Alert | Expression | for | severity | summary | Related runbook |
|---|---|---|---|---|---|
| `ConnectContainersDown` | `absent(container_last_seen{container=~".+-connect",pod=~".+-connect-.+"})` | 3m | major | All Kafka Connect containers down or in CrashLookBackOff status | NO OFFICIAL SOURCE FOUND |
| `ConnectFailedConnector` | `sum(kafka_connect_connector_status{status="failed"}) by (namespace, pod) > 0` | 5m | major | Kafka Connect Connector Failure | [connect-restart-connector-and-task.md](connect-restart-connector-and-task.md) |
| `ConnectFailedTask` | `sum(kafka_connect_worker_connector_failed_task_count) by (namespace, pod) > 0` | 5m | major | Kafka Connect Task Failure | [connect-restart-connector-and-task.md](connect-restart-connector-and-task.md) |

### Other rule files
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules (repo tag 1.3.1, retrieved 2026-11-10)

| File | Alert | Expression | for | severity |
|---|---|---|---|---|
| `prometheus-mirrormaker2-rules.yaml` | `MirrorMaker2ContainerDown` | `absent(container_last_seen{container=~".+-mirrormaker2",pod=~".+-mirrormaker2-.+"})` | 3m | major |
| `prometheus-strimzi-cluster-operator-rules.yaml` | `ClusterOperatorContainerDown` | `count((container_last_seen{container="strimzi-cluster-operator"} > (time() - 90))) < 1 or absent(container_last_seen{container="strimzi-cluster-operator"})` | 1m | major |
| `prometheus-strimzi-entity-operator-rules.yaml` | `TopicOperatorContainerDown` | `absent(container_last_seen{container="topic-operator",pod=~".+-entity-operator-.+"})` | 3m | major |
| `prometheus-strimzi-entity-operator-rules.yaml` | `UserOperatorContainerDown` | `absent(container_last_seen{container="user-operator",pod=~".+-entity-operator-.+"})` | 3m | major |
| `prometheus-kafka-bridge-rules.yaml` | `BridgeContainersDown`, `AvgProducerLatency`, `AvgConsumerFetchLatency`, `AvgConsumerCommitLatency`, `Http4xxErrorRate`, `Http5xxErrorRate` | see file | 3m / 10s / 1m | major / warning |

The rules for Kafka custom resources (`KafkaNotReady`, `KafkaTopicNotReady`, `KafkaRebalanceNotReady` and others) are in a separate kube-state-metrics file. See [kube-state-metrics-cr-monitoring.md](kube-state-metrics-cr-monitoring.md).

There is **no Cruise Control alert** in any of the 1.3.1 example rule files.
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics (repo tag 1.3.1, retrieved 2026-11-10)

## Remediation steps: deploying the rules

1. Review the example alerting rules. Then deploy them together with the Prometheus resources:
   ```
   kubectl apply -f pod-monitors/*.yaml
   kubectl apply -f prometheus-rules/*.yaml
   kubectl apply -f prometheus.yaml
   ```
   The commands are quoted from the AsciiDoc source. The rendered HTML page drops the `*` (see FINDINGS.md).
   — *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/documentation/modules/metrics/proc_metrics-deploying-prometheus.adoc ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Adapt the default configuration of the example rules to your own needs.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-alertmanager-examples-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

NO OFFICIAL SOURCE FOUND for checking that the rules are loaded.

## Rollback or caveats

- The `for` property sets how long a condition must hold before the alert fires.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-alertmanager-examples-str (Strimzi 1.3.1, retrieved 2026-11-10)
- `KafkaRunningOutOfSpace` depends on the Kubernetes configuration and storage implementation. On some platforms the storage cannot report available space, and then this alert cannot fire.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-alertmanager-examples-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [alertmanager-and-grafana.md](alertmanager-and-grafana.md)
- [kube-state-metrics-cr-monitoring.md](kube-state-metrics-cr-monitoring.md)
- [consumer-lag.md](consumer-lag.md)
