# Coverage matrix

Attribution and licences of the sources used here: [ATTRIBUTION.md](ATTRIBUTION.md).

Scope: KRaft-based Strimzi on Kubernetes, plus Apache Kafka operations. Sources: Strimzi 1.3.1 "Deploying and Managing Strimzi", the Strimzi repo at tag 1.3.1, the strimzi/drain-cleaner README, and Apache Kafka 4.3 operations docs (source at tag 4.3.2). All retrieved 2026-11-10. See [sources.md](sources.md).

Legend:
- **covered**: diagnosis, remediation and verification all come from official text, and none of those sections says NO OFFICIAL SOURCE FOUND (checked by `tools/check_runbooks.py coverage`).
- **partial**: the runbook exists, but one or more sections say NO OFFICIAL SOURCE FOUND, or the source defers to documentation outside the allowed set.
- **no official source**: nothing in the allowed sources.

| Area | Topic | Status | Runbook | Eval | Gaps |
|---|---|---|---|---|---|
| Diagnosing | CR status and conditions | partial | [cr-status-and-conditions](runbooks/strimzi/cr-status-and-conditions.md) | [6](eval/cr-status-and-conditions.jsonl) | No generic remediation (depends on the condition); NO OFFICIAL SOURCE FOUND in part of: remediation |
| Diagnosing | Operator logs | partial | [operator-logs](runbooks/strimzi/operator-logs.md) | [5](eval/operator-logs.jsonl) | No verification step for log-level changes; NO OFFICIAL SOURCE FOUND in part of: verification |
| Diagnosing | Lock acquisition warnings | partial | [lock-acquisition-warnings](runbooks/strimzi/lock-acquisition-warnings.md) | [5](eval/lock-acquisition-warnings.jsonl) | "Restart the Cluster Operator" has no command; no verification |
| Diagnosing | Kafka restart reasons and restart events | partial | [kafka-restart-events](runbooks/strimzi/kafka-restart-events.md) | [7](eval/kafka-restart-events.jsonl) | Remediation is per reason, in other runbooks; NO OFFICIAL SOURCE FOUND in part of: remediation, verification |
| Rolling updates | Manual rolling update (pod annotation) | covered | [manual-rolling-update](runbooks/strimzi/manual-rolling-update.md) | [6](eval/manual-rolling-update.jsonl) | |
| Rolling updates | Manual rolling update (StrimziPodSet annotation) | covered | [manual-rolling-update](runbooks/strimzi/manual-rolling-update.md) | (same file) | |
| Rolling updates | Drain Cleaner | partial | [drain-cleaner](runbooks/strimzi/drain-cleaner.md) | [6](eval/drain-cleaner.jsonl) | NO OFFICIAL SOURCE FOUND in part of: remediation |
| Rolling updates | Node maintenance / Kubernetes upgrade | partial | [node-maintenance-kubernetes-upgrade](runbooks/strimzi/node-maintenance-kubernetes-upgrade.md) | [5](eval/node-maintenance-kubernetes-upgrade.jsonl) | Restoring the PDB afterwards is not documented; NO OFFICIAL SOURCE FOUND in part of: verification |
| Recovery | Broker storage issues | partial | [broker-storage-issues](runbooks/strimzi/broker-storage-issues.md) | [5](eval/broker-storage-issues.jsonl) | No diagnosis commands; remediation is high level |
| Recovery | Deleting Kafka nodes via annotation | covered | [delete-kafka-node-annotation](runbooks/strimzi/delete-kafka-node-annotation.md) | [5](eval/delete-kafka-node-annotation.jsonl) | |
| Recovery | Expired CA certificates | partial | [expired-ca-certificates](runbooks/strimzi/expired-ca-certificates.md) | [8](eval/expired-ca-certificates.jsonl) | The "Kafka secret" to delete is not named (FINDINGS F8); NO OFFICIAL SOURCE FOUND in part of: remediation |
| Recovery | Cluster recovery from PVs / deleted cluster | covered | [cluster-recovery-from-persistent-volumes](runbooks/strimzi/cluster-recovery-from-persistent-volumes.md) | [8](eval/cluster-recovery-from-persistent-volumes.jsonl) | Example IDs do not match each other (FINDINGS F9) |
| Topic/User Operators | KafkaTopic NotReady | partial | [topic-not-ready](runbooks/strimzi/topic-not-ready.md) | [6](eval/topic-not-ready.jsonl) | No fix documented for a conflict between two existing resources; NO OFFICIAL SOURCE FOUND in part of: remediation |
| Topic/User Operators | KafkaUser NotReady | covered | [user-not-ready](runbooks/strimzi/user-not-ready.md) | [5](eval/user-not-ready.jsonl) | |
| Cruise Control | KafkaRebalance NotReady | covered | [kafkarebalance-notready-and-refresh](runbooks/strimzi/kafkarebalance-notready-and-refresh.md) | [6](eval/kafkarebalance-notready-and-refresh.jsonl) | |
| Cruise Control | Refreshing a proposal | covered | [kafkarebalance-notready-and-refresh](runbooks/strimzi/kafkarebalance-notready-and-refresh.md) | (same file) | |
| Cruise Control | Tracking (and stopping) rebalances | covered | [kafkarebalance-tracking-and-stopping](runbooks/strimzi/kafkarebalance-tracking-and-stopping.md) | [5](eval/kafkarebalance-tracking-and-stopping.jsonl) | |
| Cruise Control | Anomaly-detection alerts | partial | [cruise-control-anomaly-alerts](runbooks/strimzi/cruise-control-anomaly-alerts.md) | [5](eval/cruise-control-anomaly-alerts.jsonl) | Three anomaly-detector metric names are in the Strimzi example dashboard `strimzi-cruise-control.json`; alert expressions and notifier config are in the Cruise Control docs (not allowed); no CC alert in the example rules |
| Connect / MM2 | Restart Kafka Connect connectors and tasks | covered | [connect-restart-connector-and-task](runbooks/strimzi/connect-restart-connector-and-task.md) | [5](eval/connect-restart-connector-and-task.jsonl) | |
| Connect / MM2 | Restart MirrorMaker 2 connectors and tasks | covered | [mirrormaker2-restart-connector-and-task](runbooks/strimzi/mirrormaker2-restart-connector-and-task.md) | [5](eval/mirrormaker2-restart-connector-and-task.jsonl) | Example task ID does not match (FINDINGS F16) |
| Upgrades | Cluster Operator "Kafka version" errors | covered | [upgrade-kafka-version-error](runbooks/strimzi/upgrade-kafka-version-error.md) | [5](eval/upgrade-kafka-version-error.jsonl) | |
| Upgrades | Checking upgrade status | partial | [upgrade-status-and-kraft-requirement](runbooks/strimzi/upgrade-status-and-kraft-requirement.md) | [6](eval/upgrade-status-and-kraft-requirement.jsonl) | NO OFFICIAL SOURCE FOUND in part of: remediation |
| Upgrades | KRaft requirement | partial | [upgrade-status-and-kraft-requirement](runbooks/strimzi/upgrade-status-and-kraft-requirement.md) | (same file) | ZooKeeper→KRaft migration and v1 API conversion are deferred to other doc versions; NO OFFICIAL SOURCE FOUND in part of: remediation |
| Upgrades | Upgrading Kafka version / metadataVersion | covered | [upgrade-kafka-cluster](runbooks/strimzi/upgrade-kafka-cluster.md) | [6](eval/upgrade-kafka-cluster.jsonl) | |
| Monitoring | Example Prometheus rules | partial | [prometheus-alert-rules](runbooks/strimzi/prometheus-alert-rules.md) | [7](eval/prometheus-alert-rules.jsonl) | Rules have no runbook_url (FINDINGS F1); NO OFFICIAL SOURCE FOUND in part of: diagnosis, verification |
| Monitoring | Alertmanager | covered | [alertmanager-and-grafana](runbooks/strimzi/alertmanager-and-grafana.md) | [6](eval/alertmanager-and-grafana.jsonl) | |
| Monitoring | Grafana dashboards | covered | [alertmanager-and-grafana](runbooks/strimzi/alertmanager-and-grafana.md) | (same file) | |
| Monitoring | Consumer lag | partial | [consumer-lag](runbooks/strimzi/consumer-lag.md) | [6](eval/consumer-lag.jsonl) | NO OFFICIAL SOURCE FOUND in part of: diagnosis |
| Monitoring | kube-state-metrics | partial | [kube-state-metrics-cr-monitoring](runbooks/strimzi/kube-state-metrics-cr-monitoring.md) | [5](eval/kube-state-metrics-cr-monitoring.jsonl) | NO OFFICIAL SOURCE FOUND in part of: diagnosis |
| Core Kafka | Basic operations: graceful shutdown, leadership | partial | [graceful-shutdown-and-leadership](runbooks/kafka/graceful-shutdown-and-leadership.md) | [5](eval/graceful-shutdown-and-leadership.jsonl) | NO OFFICIAL SOURCE FOUND in part of: verification |
| Core Kafka | Basic operations: consumer groups | covered | [consumer-group-position](runbooks/kafka/consumer-group-position.md) | [6](eval/consumer-group-position.jsonl) | |
| Core Kafka | Basic operations: reassignment, decommission, RF, throttling | covered | [partition-reassignment-and-throttling](runbooks/kafka/partition-reassignment-and-throttling.md) | [6](eval/partition-reassignment-and-throttling.jsonl) | `cordoned.log.dirs` is in the 4.3 docs and defined in Kafka 4.3.2 (FINDINGS F2) |
| Core Kafka | KRaft management: controller membership | partial | [kraft-controller-membership](runbooks/kafka/kraft-controller-membership.md) | [11](eval/kraft-controller-membership.jsonl) | Remediation is Apache Kafka reference only: Strimzi 1.3.1 uses static controller quorums for all deployments, so it does not apply on Strimzi. Verification is not named by the add/remove sections. Sources conflict on migration (FINDINGS F20). No Kafka 4.3 source for unregistering a removed controller |
| Core Kafka | KRaft management: quorum debugging | partial | [kraft-quorum-debugging](runbooks/kafka/kraft-quorum-debugging.md) | [5](eval/kraft-quorum-debugging.jsonl) | Inspection tools only; no remediation or verification |
| Core Kafka | Monitoring metrics | partial | [monitoring-key-metrics](runbooks/kafka/monitoring-key-metrics.md) | [6](eval/monitoring-key-metrics.jsonl) | Metrics and normal values only; no remediation |
| Core Kafka | Geo-replication (MirrorMaker 2) | partial | [geo-replication-mirrormaker2](runbooks/kafka/geo-replication-mirrormaker2.md) | [6](eval/geo-replication-mirrormaker2.jsonl) | No explicit verification step; NO OFFICIAL SOURCE FOUND in part of: verification |
| GitOps | Argo CD with Strimzi | no official source | none | none | The allowed source list has no Argo CD source, and the Strimzi 1.3.1 deploying guide has no Argo CD procedure. The guide mentions GitOps only in the OLM upgrade procedure without an upgrade path (pause reconciliation, update the `Subscription` in Git; FINDINGS F19). Flux is out of scope. |

## Totals

Regenerated with `python3 -I tools/check_runbooks.py --write-totals coverage`.

| Status | Topic rows |
|---|---|
| covered | 16 |
| partial | 20 |
| no official source | 1 |
| **total** | **37** |

- Runbook files: 32 (25 Strimzi, 7 Kafka). Some runbooks cover more than one topic row.
- Eval items: 189, between 5 and 11 per runbook (counted by `tools/check_runbooks.py eval`).
- Steps and their citations are counted by `tools/check_runbooks.py steps`; see FINDINGS.md, "Verification", for the current numbers.
