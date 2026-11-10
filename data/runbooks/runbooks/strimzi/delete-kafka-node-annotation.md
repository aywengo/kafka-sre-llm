# Deleting a Kafka node (pod and PVC) using an annotation

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§11.2.10) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str ; repo `documentation/modules/configuring/proc-manual-delete-pod-pvc-kafka.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[DESTRUCTIVE]** This procedure deletes a broker's PersistentVolumeClaim. It can cause permanent data loss, and cluster availability is not guaranteed.

## Symptoms / trigger

Use this only for storage issues, and only after non-destructive recovery has been tried. See [broker-storage-issues.md](broker-storage-issues.md).
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Confirm that the Cluster Operator managing the Kafka cluster is running.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Find the name of the pod to delete. The source gives the pattern `<cluster_name>-kafka-<index_number>`, with the index running from zero to the number of replicas minus one, for example `my-cluster-kafka-0`. With node pools, other sections of the same guide name pods `<cluster>-<pool>-<id>` (see FINDINGS.md).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. **[DESTRUCTIVE]** Annotate the Pod:
   ```
   kubectl annotate pod <cluster_name>-kafka-<index_number> strimzi.io/delete-pod-and-pvc="true"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Wait for the next reconciliation. The operator deletes the annotated pod and its PVC (if the cluster uses persistent storage) and recreates both automatically.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

The pod and its PVC are recreated automatically after deletion. As in the storage recovery section, confirm that broker pods are stable and partitions are fully replicated.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10); https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- No rollback. The deleted PVC's data may be lost permanently.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [broker-storage-issues.md](broker-storage-issues.md)
- [cluster-recovery-from-persistent-volumes.md](cluster-recovery-from-persistent-volumes.md)
