# Restarting Kafka Connect connectors and tasks (KafkaConnector resources)

| Field | Value |
|---|---|
| Product | Strimzi Kafka Connect, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§11.8.2, §11.8.3); example rules at repo tag 1.3.1 |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-task-str ; https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-connect-rules.yaml |
| License | Derived from Strimzi documentation (CC BY 4.0, https://creativecommons.org/licenses/by/4.0/) and Strimzi example files (Apache-2.0), © the Strimzi authors. Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[RESTART]** These steps restart connector and/or task instances. They do not roll Kafka Connect pods.

## Symptoms / trigger

- The example alerts `ConnectFailedConnector` (`sum(kafka_connect_connector_status{status="failed"}) by (namespace, pod) > 0`, 5m, major) or `ConnectFailedTask` (`sum(kafka_connect_worker_connector_failed_task_count) by (namespace, pod) > 0`, 5m, major) fire.
  — *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-connect-rules.yaml (repo tag 1.3.1, retrieved 2026-11-10)
- You need to restart a connector or a task that is managed through a `KafkaConnector` resource.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Prerequisite: the Cluster Operator is running.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Find the `KafkaConnector` resource that controls the connector:
   ```
   kubectl get KafkaConnector
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. To restart a task, find its ID. Task IDs are non-negative integers starting at 0.
   ```
   kubectl describe KafkaConnector <kafka_connector_name>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

### Restart a connector

1. **[RESTART]** Add the restart annotation:
   ```
   kubectl annotate KafkaConnector <kafka_connector_name> strimzi.io/restart="true"
   ```
   Two parameters refine the restart, and both default to false:
   - `includeTasks`: restart the connector instance and its task instances.
   - `onlyFailed`: restart only instances with status `FAILED`.

   For example, to restart only failed connectors and their tasks:
   ```
   strimzi.io/restart="includeTasks,onlyFailed"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Wait for the next reconciliation, every two minutes by default. The connector restarts when the reconciliation detects the annotation.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)

### Restart one task

3. **[RESTART]** Add the restart-task annotation with the task ID (here task 0):
   ```
   kubectl annotate KafkaConnector <kafka_connector_name> strimzi.io/restart-task="0"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. Wait for the next reconciliation, every two minutes by default.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

When Kafka Connect accepts the restart request, the annotation is removed from the `KafkaConnector` resource. This holds for both `strimzi.io/restart` and `strimzi.io/restart-task`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- These annotations only apply to connectors managed through `KafkaConnector` resources.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
- To roll the Kafka Connect **pods** instead, see [manual-rolling-update.md](manual-rolling-update.md) (`<cluster_name>-connect-<index_number>`).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [mirrormaker2-restart-connector-and-task.md](mirrormaker2-restart-connector-and-task.md)
- [prometheus-alert-rules.md](prometheus-alert-rules.md)
- [kube-state-metrics-cr-monitoring.md](kube-state-metrics-cr-monitoring.md)
