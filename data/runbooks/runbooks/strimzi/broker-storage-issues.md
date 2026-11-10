# Recovering from broker storage issues

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§11.2.9) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-kafka-recovering-brokers-str ; https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-resizing-persistent-volumes-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-setting-broker-limits-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-choosing-a-quota-plugin-str ; https://kafka.apache.org/44/operations/basic-kafka-operations/#setting-quotas ; https://kafka.apache.org/44/operations/basic-kafka-operations/#decommissioning-log-directories ; https://strimzi.io/docs/operators/1.3.1/deploying#con-cluster-recovery-scenarios-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str ; repo `documentation/modules/configuring/con-kafka-recovering-brokers.adoc`, `documentation/modules/configuring/con-config-storage-kraft.adoc`, `documentation/modules/managing/proc-setting-broker-limits.adoc`, `documentation/modules/managing/con-choosing-a-quota-plugin.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed; Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

The source lists these typical symptoms of broker storage issues:
- Broker pods restart repeatedly or fail to start.
- Errors about full disks or insufficient storage.
- Volume mounts fail, or there are PVC-related errors.
- Partitions stay under-replicated because brokers are unavailable.

— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-kafka-recovering-brokers-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

NO OFFICIAL SOURCE FOUND for diagnosis commands in this section. Related diagnostics: [operator-logs.md](operator-logs.md), [kafka-restart-events.md](kafka-restart-events.md) (`PodStuck`, `PodUnresponsive`, `FileSystemResizeNeeded`), [cr-status-and-conditions.md](cr-status-and-conditions.md).

## Remediation steps

The source orders the options to keep broker data and lower the risk of data loss. Try the non-destructive options first.

1. Free disk space or increase the available storage capacity.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)

   **[ROLLING RESTART]** To increase capacity of persistent-claim volumes, raise the `size` storage property in the `KafkaNodePool` (for JBOD, on each volume under `storage.volumes`). The source's example sets `size: 2000Gi`. Resizing has no risk of data loss as long as the storage infrastructure supports it. Decreasing the size is not supported in Kubernetes. When the resizing is complete, the Cluster Operator restarts all pods that use the resized volumes, automatically. Check the new capacity with:
   ```
   kubectl get pv
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-resizing-persistent-volumes-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Review topic retention and compaction settings to reduce disk usage.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)
3. Review quota settings that limit producer throughput, so disks do not fill as quickly. The source's additional resources for this are "Choosing a quota plugin" and "Setting throughput and storage limits on brokers".
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach ; https://strimzi.io/docs/operators/1.3.1/deploying#con-choosing-a-quota-plugin-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-setting-broker-limits-str (Strimzi 1.3.1, retrieved 2026-11-10)

   Apache Kafka reference (Kafka 4.4 docs; on Strimzi, use the Strimzi sections above): client quotas with `kafka-configs.sh` (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`), and moving data off a log directory by cordoning it and reassigning its partitions.
   — *Source:* https://kafka.apache.org/44/operations/basic-kafka-operations/#setting-quotas ; https://kafka.apache.org/44/operations/basic-kafka-operations/#decommissioning-log-directories (Kafka 4.4, retrieved 2026-11-10)
4. If the problem involves persistent volumes or PVCs (failed mounts, unavailable volumes), the source points to the cluster recovery from persistent volumes information: [cluster-recovery-from-persistent-volumes.md](cluster-recovery-from-persistent-volumes.md).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)

   That procedure applies only after the PVCs are lost: it recovers "a deleted Kafka cluster", after a namespace was deleted or the Kubernetes cluster was lost. Do not run it on a live cluster whose PVCs still exist.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-cluster-recovery-scenarios-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. **[DESTRUCTIVE: possible permanent data loss]** If a broker cannot be recovered and its storage must be reset, replace it by deleting its pod and its PVC. Do this only when no non-destructive option works. See [delete-kafka-node-annotation.md](delete-kafka-node-annotation.md).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

After recovery, confirm that broker pods are stable and partitions are fully replicated. Keep monitoring logs and storage usage.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- Replacing a broker (step 5) cannot be rolled back. It can cause permanent data loss.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [delete-kafka-node-annotation.md](delete-kafka-node-annotation.md)
- [cluster-recovery-from-persistent-volumes.md](cluster-recovery-from-persistent-volumes.md)
- [prometheus-alert-rules.md](prometheus-alert-rules.md) (disk-space alerts)
- [../kafka/monitoring-key-metrics.md](../kafka/monitoring-key-metrics.md) (under-replicated partitions)
