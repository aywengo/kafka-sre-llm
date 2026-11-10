# Node maintenance and Kubernetes upgrades with minimal Kafka downtime

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§30.2) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str ; https://strimzi.io/docs/operators/1.3.1/deploying#rolling_pods_using_drain_cleaner ; https://strimzi.io/docs/operators/1.3.1/deploying#rolling_pods_manually_alternative_to_drain_cleaner ; repo `documentation/modules/upgrading/con-upgrade-cluster.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[ROLLING RESTART]** Both methods below roll Kafka pods.

## Symptoms / trigger

You are upgrading Kubernetes, or taking worker nodes out for maintenance, and the Kafka clusters have to stay available.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Check the Kubernetes upgrade path and node upgrade steps in the Kubernetes documentation. Also check which Kubernetes versions your Strimzi version supports.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Make sure topics are replicated for high availability: replication factor at least 3 and `min.insync.replicas` one less than the replication factor.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. Configure pod disruption budgets.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Roll the pods using one of two methods.

   **Method A, recommended: Drain Cleaner.** **[ROLLING RESTART]** While nodes are evicted during the upgrade, Drain Cleaner annotates the pods to be evicted with the manual rolling update annotation. The Cluster Operator then rolls each pod and moves it off the node being upgraded. See [drain-cleaner.md](drain-cleaner.md).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#rolling_pods_using_drain_cleaner (Strimzi 1.3.1, retrieved 2026-11-10)

   **Method B: manual pod annotation.**
   1. To behave like Drain Cleaner and keep topics available, set `maxUnavailable` to zero for the pod disruption budget. This stops Kubernetes from evicting pods automatically:
      ```
      apiVersion: kafka.strimzi.io/v1
      kind: Kafka
      metadata:
        name: my-cluster
        namespace: myproject
      spec:
        kafka:
          # ...
          template:
            podDisruptionBudget:
              maxUnavailable: 0
      # ...
      ```
      The PodDisruptionBudget created for a Kafka cluster covers the pods of all its node pools.
      — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#rolling_pods_manually_alternative_to_drain_cleaner (Strimzi 1.3.1, retrieved 2026-11-10)
   2. **[ROLLING RESTART]** Watch the node pool pods that must be drained and annotate each one. Example for pod `my-cluster-pool-a-1`:
      ```
      kubectl annotate pod my-cluster-pool-a-1 strimzi.io/manual-rolling-update="true"
      ```
      — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#rolling_pods_manually_alternative_to_drain_cleaner (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

**Expectation from the source, not a check:** in a highly available setup, the Cluster Operator keeps the minimum number of in-sync replicas for topics during the upgrade, so there is no downtime. NO OFFICIAL SOURCE FOUND for a verification step specific to this procedure. For per-pod verification see [manual-rolling-update.md](manual-rolling-update.md) and [drain-cleaner.md](drain-cleaner.md).
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- With `maxUnavailable: 0`, Kubernetes cannot evict Kafka pods automatically. Pods must be moved through the Cluster Operator.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#rolling_pods_manually_alternative_to_drain_cleaner (Strimzi 1.3.1, retrieved 2026-11-10)
- NO OFFICIAL SOURCE FOUND for setting `maxUnavailable` back after the maintenance. Compiler's note, not from the source: write down the `spec.kafka.template.podDisruptionBudget` value before Method B step 1, and restore it in the `Kafka` resource when the maintenance is over, because the PodDisruptionBudget covers the pods of all node pools.

## Related runbooks

- [drain-cleaner.md](drain-cleaner.md)
- [manual-rolling-update.md](manual-rolling-update.md)
