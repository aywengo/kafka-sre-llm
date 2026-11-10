# Tracking and stopping a Cruise Control rebalance

| Field | Value |
|---|---|
| Product | Strimzi with Cruise Control, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§21.5, §21.6) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-tracking-cluster-rebalance-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str ; repo `documentation/modules/cruise-control/proc-tracking-cluster-rebalance.adoc`, `proc-stopping-cluster-rebalance.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Prose reworded; commands quoted verbatim, except the executor-state query corrected in FINDINGS F26. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

A rebalance is running (the `KafkaRebalance` is `Rebalancing`). You need its progress to plan maintenance, scaling or broker upgrades, or you need to stop it because it affects cluster performance.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-tracking-cluster-rebalance-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps (tracking)

While a rebalance runs, Strimzi writes progress to a ConfigMap. The ConfigMap has the same name as the `KafkaRebalance` resource and is referenced by `status.progress.rebalanceProgressConfigMap`. It is updated on each reconciliation of the `KafkaRebalance`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-tracking-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

1. Estimated minutes until the rebalance completes:
   ```
   kubectl get configmaps <my_rebalance> -o jsonpath="{['data']['estimatedTimeToCompletionInMinutes']}"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-tracking-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Percentage of data movement completed (bytes, an integer 0–100, rounded down):
   ```
   kubectl get configmaps <my_rebalance> -o jsonpath="{['data']['completedByteMovementPercentage']}"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-tracking-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Executor state: the non-verbose JSON from Cruise Control's `/kafkacruisecontrol/state?substates=executor` endpoint, including partition movement progress, concurrency limits and total data to move:
   ```
   kubectl get configmaps <my_rebalance> -o jsonpath="{['data']['executorState\.json']}"
   ```
   **Corrected command (FINDINGS F26).** The source writes `['executorState.json']` without the backslash. kubectl's JSONPath reads the unescaped dot as a field separator, even inside brackets, so the source command prints nothing for the `executorState.json` key. The same guide escapes the dot elsewhere, in `{.data.ca\.crt}`. Compiler's note, not from the source: both forms were tested with kubectl 1.37.1 on a local dry-run ConfigMap; only the escaped form printed the value.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-tracking-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps (stopping)

1. **[Changes cluster state]** Annotate the `KafkaRebalance` to stop the rebalance. Cruise Control finishes the current batch of partition reassignments, then stops.
   ```
   kubectl annotate kafkarebalance <kafka_rebalance_resource_name> strimzi.io/rebalance="stop"
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

```
kubectl describe kafkarebalance <kafka_rebalance_resource_name>
```
Wait until the status changes to `Stopped`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- Stopping does not undo anything. Reassignments that already completed stay applied, so the cluster differs from its state before the rebalance. In this stopped, intermediate state, performance might be worse than before the rebalance.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)
- If more rebalancing is needed, generate a new optimization proposal.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-stopping-cluster-rebalance-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [kafkarebalance-notready-and-refresh.md](kafkarebalance-notready-and-refresh.md)
- [cruise-control-anomaly-alerts.md](cruise-control-anomaly-alerts.md)
