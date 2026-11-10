# Monitoring Strimzi custom resources with kube-state-metrics

| Field | Value |
|---|---|
| Product | Strimzi example metrics configuration (kube-state-metrics), KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§25.4); repo files at tag 1.3.1 |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-custom-resource-monitoring-str ; https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/kube-state-metrics/prometheus-rules.yaml |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed; Strimzi repo example files (strimzi-kafka-operator tag 1.3.1), Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

You want alerts when Strimzi custom resources are not ready or report warnings, or one of the kube-state-metrics (KSM) alerts below fired. KSM listens to the Kubernetes API server and exports metrics about object state. You can use it to track component health, find resources that are not ready, and alert on warning conditions.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-custom-resource-monitoring-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps: KSM alerts in the example `PrometheusRule`

All of them are severity `warning` with `for: 15m`, except the two rebalance-progress alerts.
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/kube-state-metrics/prometheus-rules.yaml (repo tag 1.3.1, retrieved 2026-11-10)

| Alert | Expression | for / severity | Related runbook |
|---|---|---|---|
| `KafkaNotReady` | `strimzi_kafka_resource_info{ready!="True"}` | 15m / warning | [cr-status-and-conditions.md](cr-status-and-conditions.md) |
| `KafkaTopicNotReady` | `strimzi_kafka_topic_resource_info{ready!="True"}` | 15m / warning | [topic-not-ready.md](topic-not-ready.md) |
| `KafkaUserNotReady` | `strimzi_kafka_user_resource_info{ready!="True"}` | 15m / warning | [user-not-ready.md](user-not-ready.md) |
| `KafkaRebalanceNotReady` | `strimzi_kafka_rebalance_resource_info{ready!="True",template!="true"}` | 15m / warning | [kafkarebalance-notready-and-refresh.md](kafkarebalance-notready-and-refresh.md) |
| `KafkaRebalanceProposalPending` | `strimzi_kafka_rebalance_resource_info{ready="True",template!="true",proposal_ready="True"}` | 1h / warning | [kafkarebalance-notready-and-refresh.md](kafkarebalance-notready-and-refresh.md) |
| `KafkaRebalanceRebalancing` | `strimzi_kafka_rebalance_resource_info{ready="True",template!="true",rebalancing="True"}` | 1h / info | [kafkarebalance-tracking-and-stopping.md](kafkarebalance-tracking-and-stopping.md) |
| `KafkaConnectNotReady` | `strimzi_kafka_connect_resource_info{ready!="True"}` | 15m / warning | [connect-restart-connector-and-task.md](connect-restart-connector-and-task.md) |
| `KafkaConnectorNotReady` | `strimzi_kafka_connector_resource_info{ready!="True"}` | 15m / warning | [connect-restart-connector-and-task.md](connect-restart-connector-and-task.md) |
| `KafkaMirrorMaker2NotReady` | `strimzi_kafka_mm2_resource_info{ready!="True"}` | 15m / warning | [mirrormaker2-restart-connector-and-task.md](mirrormaker2-restart-connector-and-task.md) |
| `KafkaAccessNotReady` | `strimzi_kafka_access_resource_info{ready!="True"}` | 15m / warning | NO OFFICIAL SOURCE FOUND |
| `*Warnings` (Kafka, KafkaTopic, KafkaUser, KafkaNodePool, KafkaRebalance, KafkaConnect, KafkaConnector, KafkaMirrorMaker2, KafkaAccess) | `strimzi_<resource>_resource_info{warnings="Warning"}` | 15m / warning | [cr-status-and-conditions.md](cr-status-and-conditions.md) |
| `*Deprecated` (same resources) | `strimzi_<resource>_resource_info{deprecated="Warning"}` | 15m / warning | [cr-status-and-conditions.md](cr-status-and-conditions.md) |

The rules use a `message` annotation and have no `runbook_url`. The "Related runbook" column is this compilation's own cross-link.
— *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/kube-state-metrics/prometheus-rules.yaml (repo tag 1.3.1, retrieved 2026-11-10)

## Remediation steps: deploying KSM for Strimzi resources

Prerequisite: Prometheus and Prometheus Alertmanager are deployed.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-custom-resource-monitoring-str (Strimzi 1.3.1, retrieved 2026-11-10)

1. If you use the example `examples/metrics/kube-state-metrics/ksm.yaml`, replace the example namespace `myproject` with your own.
   Linux:
   ```
   sed -E -i '/[[:space:]]*namespace: [a-zA-Z0-9-]*$/s/namespace:[[:space:]]*[a-zA-Z0-9-]*$/namespace: myproject/' examples/metrics/kube-state-metrics/ksm.yaml
   ```
   macOS:
   ```
   sed -i '' -e '/[[:space:]]*namespace: [a-zA-Z0-9-]*$/s/namespace:[[:space:]]*[a-zA-Z0-9-]*$/namespace: myproject/' examples/metrics/kube-state-metrics/ksm.yaml
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-custom-resource-monitoring-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Deploy KSM with the example configuration ConfigMap:
   ```
   kubectl apply -f configmap.yaml
   kubectl apply -f ksm.yaml
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-custom-resource-monitoring-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. For alerts, use the provided `PrometheusRule` resource `examples/metrics/kube-state-metrics/prometheus-rules.yaml`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-custom-resource-monitoring-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

Check that Prometheus scrapes the KSM metrics. They are scraped from the `strimzi-kube-state-metrics` service through the ServiceMonitor configured for the KSM deployment.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-custom-resource-monitoring-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

NO OFFICIAL SOURCE FOUND.

## Related runbooks

- [prometheus-alert-rules.md](prometheus-alert-rules.md)
- [cr-status-and-conditions.md](cr-status-and-conditions.md)
