# Deleting a Kafka node (pod and PVC) using an annotation

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§11.2.10) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str ; https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums ; https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach ; repo `documentation/modules/configuring/proc-manual-delete-pod-pvc-kafka.adoc`, `documentation/modules/configuring/ref-list-of-kafka-cluster-resources.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed. Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[DESTRUCTIVE]** This procedure deletes a broker's PersistentVolumeClaim. It can cause permanent data loss, and cluster availability is not guaranteed.
>
> **Controller pods and the quorum.** Compiler's note, not from the source: the procedure speaks only of the pod "on which the Kafka broker is running" and is silent on controller or dual-role nodes. A pod in a node pool with the controller role holds a copy of the metadata log, and Strimzi 1.3.1 uses static controller quorums (https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums). Before annotating such a pod, make sure the remaining controllers still form a majority, and check that the partitions on the broker have in-sync replicas on other brokers. NO OFFICIAL SOURCE FOUND for either check in this procedure.

## Symptoms / trigger

Use this only for storage issues, and only after non-destructive recovery has been tried. See [broker-storage-issues.md](broker-storage-issues.md).
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Confirm that the Cluster Operator managing the Kafka cluster is running.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Find the name of the pod to delete. The procedure gives the pattern `<cluster_name>-kafka-<index_number>`, with the index running from zero to the number of replicas minus one, for example `my-cluster-kafka-0`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)

   With node pools (required for KRaft), the guide's list of Kafka cluster resources names the pods created by a node pool's StrimziPodSet `<kafka_cluster_name>-<pool_name>-<pod_id>`. The node's PVC is `data-<kafka_cluster_name>-<pool_name>-<pod_id>` with single-volume persistent storage. With JBOD storage there is one PVC per persistent volume, named `data-<id>-<kafka_cluster_name>-<pool_name>-<pod_id>`, where `<id>` is the volume `id`. The two sections use different patterns (FINDINGS F4).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. **[DESTRUCTIVE]** Annotate the Pod. The procedure's command:
   ```
   kubectl annotate pod <cluster_name>-kafka-<index_number> strimzi.io/delete-pod-and-pvc="true"
   ```
   With node pools, the pod name in this command takes the node-pool form `<kafka_cluster_name>-<pool_name>-<pod_id>` (from the list of Kafka cluster resources). The procedure itself shows only the form above. If the pod is in a node pool with the controller role, read the controller note at the top first.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str (Strimzi 1.3.1, retrieved 2026-11-10)
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
