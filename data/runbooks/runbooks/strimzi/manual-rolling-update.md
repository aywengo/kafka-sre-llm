# Manual rolling update of Kafka, Kafka Connect or MirrorMaker 2 pods

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-strimzipodset-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str ; repo `documentation/modules/managing/proc-manual-rolling-update-pods.adoc`, `proc-manual-rolling-update-strimzipodset.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed. Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[ROLLING RESTART]** Every remediation step in this runbook restarts pods.

## Symptoms / trigger

You need to restart the pods of an existing Kafka, Kafka Connect or MirrorMaker 2 cluster through the Cluster Operator. The source does not name a specific symptom.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps (prerequisites)

1. Confirm that the Cluster Operator is running and that the target cluster (Kafka, Kafka Connect or MirrorMaker 2) is running.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. A Kafka rolling update works with any replication factor. To keep Kafka available during the update, the cluster must be highly available and topics must be replicated: replication factor at least 3 and `min.insync.replicas` one less than the replication factor (for example `replicas: 3`, `min.insync.replicas: 2`).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

### Option A: annotate individual pods (preferred by the source to avoid a race)

1. Find the pod name. The source gives these patterns: `<cluster_name>-kafka-<index_number>` (Kafka), `<cluster_name>-connect-<index_number>` (Kafka Connect), `<cluster_name>-mirrormaker2-<index_number>` (MirrorMaker 2). The index starts at zero.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)

   With node pools (required for KRaft), pods are named `<kafka_cluster_name>-<pool_name>-<pod_id>`, and the StrimziPodSet `<kafka_cluster_name>-<pool_name>`. The Kubernetes upgrade section annotates a pod named `my-cluster-pool-a-1` in this way. The procedure above shows only the non-node-pool patterns (FINDINGS F4).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. **[ROLLING RESTART]** Annotate the pod:
   ```
   kubectl annotate pod <cluster_name>-kafka-<index_number> strimzi.io/manual-rolling-update="true"

   kubectl annotate pod <cluster_name>-connect-<index_number> strimzi.io/manual-rolling-update="true"

   kubectl annotate pod <cluster_name>-mirrormaker2-<index_number> strimzi.io/manual-rolling-update="true"
   ```
   If you annotate several pods, they are rolled one after another in the same reconciliation run.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Wait for the next reconciliation, every two minutes by default. The annotated pod is rolled if the reconciliation detected the annotation.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)

### Option B: annotate the StrimziPodSet (rolls all its pods)

1. Find the StrimziPodSet name. The source's examples: Kafka cluster `my-cluster` gives `my-cluster-kafka`, Kafka Connect `my-connect-cluster` gives `my-connect-cluster-connect`, MirrorMaker 2 `my-mm2-cluster` gives `my-mm2-cluster-mirrormaker2`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-strimzipodset-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. **[ROLLING RESTART]** Annotate it:
   ```
   kubectl annotate strimzipodset <cluster_name>-kafka strimzi.io/manual-rolling-update="true"

   kubectl annotate strimzipodset <cluster_name>-connect strimzi.io/manual-rolling-update="true"

   kubectl annotate strimzipodset <cluster_name>-mirrormaker2 strimzi.io/manual-rolling-update="true"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-strimzipodset-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Wait for the next reconciliation, every two minutes by default. All pods managed by the annotated resource are rolled.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-strimzipodset-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

- Pod annotation: the annotation is removed from the Pod automatically when its rolling update is complete.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)
- StrimziPodSet annotation: the annotation is removed from the resource automatically when all its pods have been rolled.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-strimzipodset-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The restart appears as a `ManualRollingUpdate` restart event; see [kafka-restart-events.md](kafka-restart-events.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- If you annotate a StrimziPodSet while the Cluster Operator is reconciling it, the annotation can be overwritten and no rolling update happens. To avoid this race, annotate the pods directly.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-strimzipodset-str (Strimzi 1.3.1, retrieved 2026-11-10)
- Annotating a pod that a rolling update is already replacing does not guarantee an extra restart.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The reconciliation continues even if the manual rolling update fails. This lets the operator recover from some fixable situations later in the same reconciliation, for example by recreating a missing PVC or PV that made the update fail.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-rolling-update-pods-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [drain-cleaner.md](drain-cleaner.md)
- [node-maintenance-kubernetes-upgrade.md](node-maintenance-kubernetes-upgrade.md)
- [kafka-restart-events.md](kafka-restart-events.md)
