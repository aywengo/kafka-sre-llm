# Coverage matrix

Scope: KRaft-based Strimzi on Kubernetes, plus Apache Kafka operations. Sources: Strimzi 1.3.1 "Deploying and Managing Strimzi", the Strimzi repo at tag 1.3.1, the strimzi/drain-cleaner README, and Apache Kafka 4.4 operations docs. All retrieved 2026-11-10. See [sources.md](sources.md).

Legend:
- **covered**: diagnosis, remediation and verification all come from official text.
- **partial**: the runbook exists, but one or more sections say NO OFFICIAL SOURCE FOUND, or the source defers to documentation outside the allowed set.
- **no official source**: nothing in the allowed sources.

| Area | Topic | Status | Runbook | Eval | Gaps |
|---|---|---|---|---|---|
| Diagnosing | CR status and conditions | covered | [cr-status-and-conditions](runbooks/strimzi/cr-status-and-conditions.md) | [6](eval/cr-status-and-conditions.jsonl) | No generic remediation (depends on the condition) |
| Diagnosing | Operator logs | covered | [operator-logs](runbooks/strimzi/operator-logs.md) | [5](eval/operator-logs.jsonl) | No verification step for log-level changes |
| Diagnosing | Lock acquisition warnings | partial | [lock-acquisition-warnings](runbooks/strimzi/lock-acquisition-warnings.md) | [5](eval/lock-acquisition-warnings.jsonl) | "Restart the Cluster Operator" has no command; no verification |
| Diagnosing | Kafka restart reasons and restart events | covered | [kafka-restart-events](runbooks/strimzi/kafka-restart-events.md) | [7](eval/kafka-restart-events.jsonl) | Remediation is per reason, in other runbooks |
| Rolling updates | Manual rolling update (pod annotation) | covered | [manual-rolling-update](runbooks/strimzi/manual-rolling-update.md) | [6](eval/manual-rolling-update.jsonl) | |
| Rolling updates | Manual rolling update (StrimziPodSet annotation) | covered | [manual-rolling-update](runbooks/strimzi/manual-rolling-update.md) | (same file) | |
| Rolling updates | Drain Cleaner | covered | [drain-cleaner](runbooks/strimzi/drain-cleaner.md) | [6](eval/drain-cleaner.jsonl) | |
| Rolling updates | Node maintenance / Kubernetes upgrade | covered | [node-maintenance-kubernetes-upgrade](runbooks/strimzi/node-maintenance-kubernetes-upgrade.md) | [5](eval/node-maintenance-kubernetes-upgrade.jsonl) | Restoring the PDB afterwards is not documented |
| Recovery | Broker storage issues | partial | [broker-storage-issues](runbooks/strimzi/broker-storage-issues.md) | [5](eval/broker-storage-issues.jsonl) | No diagnosis commands; remediation is high level |
| Recovery | Deleting Kafka nodes via annotation | covered | [delete-kafka-node-annotation](runbooks/strimzi/delete-kafka-node-annotation.md) | [5](eval/delete-kafka-node-annotation.jsonl) | |
| Recovery | Expired CA certificates | covered | [expired-ca-certificates](runbooks/strimzi/expired-ca-certificates.md) | [6](eval/expired-ca-certificates.jsonl) | The "Kafka secret" to delete is not named (FINDINGS F8) |
| Recovery | Cluster recovery from PVs / deleted cluster | covered | [cluster-recovery-from-persistent-volumes](runbooks/strimzi/cluster-recovery-from-persistent-volumes.md) | [7](eval/cluster-recovery-from-persistent-volumes.jsonl) | Example IDs do not match each other (FINDINGS F9) |
| Topic/User Operators | KafkaTopic NotReady | covered | [topic-not-ready](runbooks/strimzi/topic-not-ready.md) | [6](eval/topic-not-ready.jsonl) | No fix documented for a conflict between two existing resources |
| Topic/User Operators | KafkaUser NotReady | covered | [user-not-ready](runbooks/strimzi/user-not-ready.md) | [5](eval/user-not-ready.jsonl) | |
| Cruise Control | KafkaRebalance NotReady | covered | [kafkarebalance-notready-and-refresh](runbooks/strimzi/kafkarebalance-notready-and-refresh.md) | [6](eval/kafkarebalance-notready-and-refresh.jsonl) | |
| Cruise Control | Refreshing a proposal | covered | [kafkarebalance-notready-and-refresh](runbooks/strimzi/kafkarebalance-notready-and-refresh.md) | (same file) | |
| Cruise Control | Tracking (and stopping) rebalances | covered | [kafkarebalance-tracking-and-stopping](runbooks/strimzi/kafkarebalance-tracking-and-stopping.md) | [5](eval/kafkarebalance-tracking-and-stopping.jsonl) | |
| Cruise Control | Anomaly-detection alerts | partial | [cruise-control-anomaly-alerts](runbooks/strimzi/cruise-control-anomaly-alerts.md) | [5](eval/cruise-control-anomaly-alerts.jsonl) | Metric names and notifier config are in the Cruise Control docs (not allowed); no CC alert in the example rules |
| Connect / MM2 | Restart Kafka Connect connectors and tasks | covered | [connect-restart-connector-and-task](runbooks/strimzi/connect-restart-connector-and-task.md) | [5](eval/connect-restart-connector-and-task.jsonl) | |
| Connect / MM2 | Restart MirrorMaker 2 connectors and tasks | covered | [mirrormaker2-restart-connector-and-task](runbooks/strimzi/mirrormaker2-restart-connector-and-task.md) | [5](eval/mirrormaker2-restart-connector-and-task.jsonl) | Example task ID does not match (FINDINGS F16) |
| Upgrades | Cluster Operator "Kafka version" errors | covered | [upgrade-kafka-version-error](runbooks/strimzi/upgrade-kafka-version-error.md) | [5](eval/upgrade-kafka-version-error.jsonl) | |
| Upgrades | Checking upgrade status | covered | [upgrade-status-and-kraft-requirement](runbooks/strimzi/upgrade-status-and-kraft-requirement.md) | [5](eval/upgrade-status-and-kraft-requirement.jsonl) | |
| Upgrades | KRaft requirement | covered | [upgrade-status-and-kraft-requirement](runbooks/strimzi/upgrade-status-and-kraft-requirement.md) | (same file) | ZooKeeper→KRaft migration and v1 API conversion are deferred to other doc versions |
| Upgrades | Upgrading Kafka version / metadataVersion | covered | [upgrade-kafka-cluster](runbooks/strimzi/upgrade-kafka-cluster.md) | [6](eval/upgrade-kafka-cluster.jsonl) | |
| Monitoring | Example Prometheus rules | covered | [prometheus-alert-rules](runbooks/strimzi/prometheus-alert-rules.md) | [7](eval/prometheus-alert-rules.jsonl) | Rules have no runbook_url (FINDINGS F1) |
| Monitoring | Alertmanager | covered | [alertmanager-and-grafana](runbooks/strimzi/alertmanager-and-grafana.md) | [6](eval/alertmanager-and-grafana.jsonl) | |
| Monitoring | Grafana dashboards | covered | [alertmanager-and-grafana](runbooks/strimzi/alertmanager-and-grafana.md) | (same file) | |
| Monitoring | Consumer lag | covered | [consumer-lag](runbooks/strimzi/consumer-lag.md) | [6](eval/consumer-lag.jsonl) | |
| Monitoring | kube-state-metrics | covered | [kube-state-metrics-cr-monitoring](runbooks/strimzi/kube-state-metrics-cr-monitoring.md) | [5](eval/kube-state-metrics-cr-monitoring.jsonl) | |
| Core Kafka | Basic operations: graceful shutdown, leadership | covered | [graceful-shutdown-and-leadership](runbooks/kafka/graceful-shutdown-and-leadership.md) | [5](eval/graceful-shutdown-and-leadership.jsonl) | |
| Core Kafka | Basic operations: consumer groups | covered | [consumer-group-position](runbooks/kafka/consumer-group-position.md) | [6](eval/consumer-group-position.jsonl) | |
| Core Kafka | Basic operations: reassignment, decommission, RF, throttling | covered | [partition-reassignment-and-throttling](runbooks/kafka/partition-reassignment-and-throttling.md) | [6](eval/partition-reassignment-and-throttling.jsonl) | `cordoned.log.dirs` is from the 4.4 docs (FINDINGS F2) |
| Core Kafka | KRaft management: controller membership | partial | [kraft-controller-membership](runbooks/kafka/kraft-controller-membership.md) | [6](eval/kraft-controller-membership.jsonl) | Remediation is Apache Kafka reference only: Strimzi 1.3.1 uses static controller quorums for all deployments, so it does not apply on Strimzi. Verification is not named by the add/remove sections. Sources conflict on migration (FINDINGS F20) |
| Core Kafka | KRaft management: quorum debugging | partial | [kraft-quorum-debugging](runbooks/kafka/kraft-quorum-debugging.md) | [5](eval/kraft-quorum-debugging.jsonl) | Inspection tools only; no remediation or verification |
| Core Kafka | Monitoring metrics | partial | [monitoring-key-metrics](runbooks/kafka/monitoring-key-metrics.md) | [6](eval/monitoring-key-metrics.jsonl) | Metrics and normal values only; no remediation |
| Core Kafka | Geo-replication (MirrorMaker 2) | covered | [geo-replication-mirrormaker2](runbooks/kafka/geo-replication-mirrormaker2.md) | [6](eval/geo-replication-mirrormaker2.jsonl) | No explicit verification step |
| GitOps | Argo CD with Strimzi | no official source | none | none | The allowed source list has no Argo CD or GitOps source, and the Strimzi 1.3.1 deploying guide has no Argo CD procedure. Flux is out of scope. |

## Totals

| Status | Topic rows |
|---|---|
| covered | 31 |
| partial | 5 |
| no official source | 1 |
| **total** | **37** |

- Runbook files: 32 (25 Strimzi, 7 Kafka). Some runbooks cover more than one topic row.
- Eval items: 180, between 5 and 7 per runbook.
- Steps with a citation: 289 of 289 (counted automatically; see FINDINGS.md, "Verification").
