# Checking Strimzi custom resource status and conditions

| Field | Value |
|---|---|
| Product | Strimzi (Kafka operators for Kubernetes), KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-accessing-resource-status-str ; repo `documentation/modules/managing/con-custom-resources-status.adoc`, `documentation/modules/managing/proc-accessing-resource-status.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, licensed CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

The source gives no alert or symptom for this procedure. It is the general first check whenever a Strimzi resource does not reach the state defined in its `spec`. Status conditions record when and why the resource's state changed, and they show events that stop or delay the operator from reaching the desired state.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

Resources that publish status: `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, `KafkaConnect`, `KafkaConnector`, `KafkaMirrorMaker2`, `KafkaBridge`, `KafkaRebalance`, `StrimziPodSet`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Retrieve the full status of the resource using its `status` subresource and a JSONPath expression:
   ```
   kubectl get kafka <kafka_resource_name> -o jsonpath='{.status}' | jq
   ```
   To narrow the output, use dot notation such as `status.listeners` or `status.observedGeneration`. `jq` makes the output easier to read.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-accessing-resource-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

2. Read `status.conditions`. They describe the resource's current state and give the time and reason of each state change. For a `Kafka` resource, the `Ready` condition means the Cluster Operator considers the cluster ready to handle traffic.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

3. Compare `status.observedGeneration` with `metadata.generation`. If they differ, the operator has not yet processed the latest change to the resource. If they are equal, the status reflects the latest change.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

4. For a `Kafka` resource, check these fields:
   - `kafkaMetadataState`: `KRaft` means KRaft manages the metadata.
   - `kafkaVersion`: the Kafka version the cluster uses.
   - `operatorLastSuccessfulVersion`: the operator version that last completed a reconciliation.
   - `kafkaNodePools`, `listeners`, `clusterId`.

   Use `operatorLastSuccessfulVersion` and `kafkaVersion` to tell whether a Strimzi or Kafka upgrade has finished.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

NO OFFICIAL SOURCE FOUND for a generic remediation. The fix depends on the condition reason. See the related runbooks for specific resources.

Do not try to fix a resource by editing its `status`. The status cannot be edited, for example through `kubectl edit`, and changing it would not affect the cluster configuration anyway.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

The status is current when `observedGeneration` equals `metadata.generation`. For a `Kafka` resource, the cluster is ready when the `Ready` condition has `status: 'True'`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- The bootstrap addresses listed in the status do not mean those endpoints, or the Kafka cluster, are `Ready`.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)
- Strimzi evaluates and updates the status periodically, so it may lag behind changes.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [operator-logs.md](operator-logs.md)
- [lock-acquisition-warnings.md](lock-acquisition-warnings.md)
- [upgrade-status-and-kraft-requirement.md](upgrade-status-and-kraft-requirement.md)
- [topic-not-ready.md](topic-not-ready.md), [user-not-ready.md](user-not-ready.md)
- [kafkarebalance-notready-and-refresh.md](kafkarebalance-notready-and-refresh.md)
