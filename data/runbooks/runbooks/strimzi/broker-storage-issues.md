# Recovering from broker storage issues

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§11.2.9) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-kafka-recovering-brokers-str ; https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach ; repo `documentation/modules/configuring/con-kafka-recovering-brokers.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded. |
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
2. Review topic retention and compaction settings to reduce disk usage.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)
3. Review quota settings that limit producer throughput, so disks do not fill as quickly.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)
4. If the problem involves persistent volumes or PVCs (failed mounts, unavailable volumes), follow the cluster recovery from persistent volumes procedure: [cluster-recovery-from-persistent-volumes.md](cluster-recovery-from-persistent-volumes.md).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)
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
