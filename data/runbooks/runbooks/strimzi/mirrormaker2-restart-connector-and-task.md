# Restarting MirrorMaker 2 connectors and tasks

| Field | Value |
|---|---|
| Product | Strimzi KafkaMirrorMaker2, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§11.10.1, §11.10.3, §11.10.4) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-task-str ; https://strimzi.io/docs/operators/1.3.1/deploying#checking_connector_task_operations ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-task-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[RESTART]** These steps restart MirrorMaker 2 connector and/or task instances. They do not roll the MirrorMaker 2 pods.

## Symptoms / trigger

You need to restart a MirrorMaker 2 connector, or one of its tasks, managed by a `KafkaMirrorMaker2` resource. The source does not name a specific symptom. The example MirrorMaker 2 Grafana dashboard shows the number of tasks, replication latency and offset synchronization latency.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#checking_connector_task_operations (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Prerequisite: the Cluster Operator is running.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Find the `KafkaMirrorMaker2` resource:
   ```
   kubectl get KafkaMirrorMaker2
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Find the connector name and, for a task restart, the task ID. Task IDs are non-negative integers starting at 0.
   ```
   kubectl describe KafkaMirrorMaker2 <mirrormaker_cluster_name>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

### Restart a connector

1. **[RESTART]** Annotate the `KafkaMirrorMaker2` resource with the connector name:
   ```
   kubectl annotate KafkaMirrorMaker2 <mirrormaker_cluster_name> "strimzi.io/restart-connector=<mirrormaker_connector_name>"
   ```
   Example: restart `source-cluster->target-cluster.MirrorCheckpointConnector` in `my-mirror-maker-2`:
   ```
   kubectl annotate KafkaMirrorMaker2 my-mirror-maker-2 "strimzi.io/restart-connector=source-cluster->target-cluster.MirrorCheckpointConnector"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Optional parameters, both default false: `includeTasks` (restart the connector and its tasks) and `onlyFailed` (only instances in `FAILED` status). Put a colon (`:`) between the connector name and the parameters:
   ```
   kubectl annotate KafkaMirrorMaker2 my-mirror-maker-2 "strimzi.io/restart-connector=source-cluster->target-cluster.MirrorCheckpointConnector:includeTasks,onlyFailed"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Wait for the next reconciliation, every two minutes by default.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str (Strimzi 1.3.1, retrieved 2026-11-10)

### Restart one task

4. **[RESTART]** Annotate with `<connector_name>:<task_id>`:
   ```
   kubectl annotate KafkaMirrorMaker2 <mirrormaker_cluster_name> "strimzi.io/restart-connector-task=<mirrormaker_connector_name>:<task_id>"
   ```
   The source's example (its text says task 0, but the command restarts task 2; see FINDINGS.md):
   ```
   kubectl annotate KafkaMirrorMaker2 my-mirror-maker-2 "strimzi.io/restart-connector-task=source-cluster->target-cluster.MirrorSourceConnector:2"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. Wait for the next reconciliation, every two minutes by default.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

The restart happens if the reconciliation detected the annotation. When MirrorMaker 2 accepts the request, the annotation is removed from the `KafkaMirrorMaker2` resource.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- MirrorMaker 2 annotations differ from Kafka Connect ones: MM2 uses `strimzi.io/restart-connector` and `strimzi.io/restart-connector-task` on the `KafkaMirrorMaker2` resource. Kafka Connect uses `strimzi.io/restart` and `strimzi.io/restart-task` on the `KafkaConnector` resource.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-mirrormaker2-connector-task-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-restart-connector-task-str (Strimzi 1.3.1, retrieved 2026-11-10)
- To roll MirrorMaker 2 **pods**, see [manual-rolling-update.md](manual-rolling-update.md) (`<cluster_name>-mirrormaker2-<index_number>`).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [connect-restart-connector-and-task.md](connect-restart-connector-and-task.md)
- [../kafka/geo-replication-mirrormaker2.md](../kafka/geo-replication-mirrormaker2.md)
