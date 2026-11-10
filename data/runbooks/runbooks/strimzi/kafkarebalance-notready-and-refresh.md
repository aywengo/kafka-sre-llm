# KafkaRebalance NotReady: troubleshooting and refreshing a proposal

| Field | Value |
|---|---|
| Product | Strimzi with Cruise Control, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (chapter 21) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-approving-optimization-proposal-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-generating-optimization-proposals-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-optimization-proposals-modes-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str ; repo `documentation/modules/cruise-control/*.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Prose reworded; commands and error text quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

A `KafkaRebalance` resource is in the `NotReady` state. Errors from creating the resource, or from talking to Cruise Control, are reported in its status together with guidance on how to fix them.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

The `KafkaRebalance` states named in the source:

| State | Meaning |
|---|---|
| `PendingProposal` | The rebalance operator is polling the Cruise Control API to see whether the proposal is ready |
| `ProposalReady` | The proposal is ready for review and approval |
| `Rebalancing` | The rebalance is in progress |
| `Ready` | The rebalance is complete |
| `NotReady` | An error occurred |
| `Stopped` | The rebalance was stopped (see [kafkarebalance-tracking-and-stopping.md](kafkarebalance-tracking-and-stopping.md)) |

— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-approving-optimization-proposal-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Read the error from the `KafkaRebalance` status:
   ```
   kubectl describe kafkarebalance <kafka_rebalance_resource_name>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Compare it with the common causes in the source:
   - Misconfigured parameters in the `KafkaRebalance` resource.
   - The `strimzi.io/cluster` label that names the Kafka cluster is missing from the `KafkaRebalance` resource.
   - Cruise Control is not deployed because the `cruiseControl` property is missing from the `Kafka` resource.
   - The Cruise Control server is not reachable.

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. If the status shows an insufficient CPU capacity error, the cluster is overloaded in CPU terms. Example:
   ```
   com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException:
   [CpuCapacityGoal] Insufficient capacity for cpu (Utilization 615.21, Allowed Capacity 420.00, Threshold: 0.70). Add at least 3 brokers with the same cpu capacity (100.00) as broker-0. Add at least 3 brokers with the same cpu capacity (100.00) as broker-0.
   ```
   CPU capacity is shown as a percentage, not as a number of cores. It does not map directly to the CPUs configured in the `Kafka` resource: it is like one virtual CPU per broker with the cycles of the CPUs set in `KafkaNodePool.spec.resources.limits.cpu`. `excludedTopics` does not change this value, because excluded topics still count toward utilization even though their replicas are not moved.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-generating-optimization-proposals-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. If the error is about `spec.mode` or `spec.brokers`, check them against the documented modes:

   | `spec.mode` | Use |
   |---|---|
   | `full` (default when `spec.mode` is not set) | Full rebalance; moves replicas across all brokers |
   | `add-brokers` | After scaling up; moves replicas from existing brokers to the new brokers listed in `spec.brokers` |
   | `remove-brokers` | Before scaling down; moves replicas off the brokers listed in `spec.brokers` |
   | `remove-disks` | Reassigns partitions between JBOD disks on the same broker; takes broker IDs with their volume IDs |

   The source names exactly these four modes.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-optimization-proposals-modes-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. Fix the configuration of the `KafkaRebalance` resource, or the problem with the Cruise Control deployment.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Refresh the proposal. A refresh requests a new optimization proposal from Cruise Control:
   ```
   kubectl annotate kafkarebalance <kafka_rebalance_resource_name> strimzi.io/rebalance="refresh"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

```
kubectl describe kafkarebalance <kafka_rebalance_resource_name>
```
Wait until the status changes to `PendingProposal` or straight to `ProposalReady`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- **Approving a proposal triggers a cluster rebalance (data movement).** Unless the proposal is new, refresh it before approving and review its contents. On approval, Cruise Control re-evaluates the cluster state and may recompute the optimization.
  ```
  kubectl annotate kafkarebalance <kafka_rebalance_resource_name> strimzi.io/rebalance="approve"
  ```
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-approving-optimization-proposal-str (Strimzi 1.3.1, retrieved 2026-11-10)
- To generate another proposal from the same `KafkaRebalance` after a `Ready` rebalance, apply the refresh annotation. The resource moves to `PendingProposal` or `ProposalReady`.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-approving-optimization-proposal-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The troubleshooting section lists "You have approved an optimization proposal" as a prerequisite, but `NotReady` can also occur when the resource is first created (see FINDINGS.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-fixing-problems-with-kafkarebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)
- On scale-down, brokers are shut down even if they host replicas when the checks are skipped.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-optimization-proposals-modes-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [kafkarebalance-tracking-and-stopping.md](kafkarebalance-tracking-and-stopping.md)
- [cruise-control-anomaly-alerts.md](cruise-control-anomaly-alerts.md)
- [cr-status-and-conditions.md](cr-status-and-conditions.md)
