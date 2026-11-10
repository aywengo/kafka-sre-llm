# KafkaTopic not ready

| Field | Value |
|---|---|
| Product | Strimzi Topic Operator, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (chapter 12) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-operator-topic-names-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-application-topic-handling-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-topic-replication-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-deleting-managed-topics-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-removing-topic-finalizers-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-disabling-topic-deletion-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str ; repo `documentation/modules/operators/proc-configuring-kafka-topic.adoc`, `documentation/modules/operators/con-operator-topic-names.adoc`, `documentation/modules/operators/con-application-topic-handling.adoc`, `documentation/modules/operators/con-topic-replication.adoc`, `documentation/modules/operators/con-deleting-managed-topics.adoc`, `documentation/modules/operators/con-removing-topic-finalizers.adoc`, `documentation/modules/operators/con-disabling-topic-deletion.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

After `kubectl apply` of a `KafkaTopic`, the `READY` column in `kubectl get kafkatopics -o wide` stays blank, or the resource has a `NotReady` condition.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Watch the topic status:
   ```
   kubectl get kafkatopics -o wide -w -n <namespace>
   ```
   A topic was created successfully when `READY` shows `True`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. If `READY` stays blank, read the status message in the resource YAML or in the Topic Operator logs:
   ```
   kubectl get kafkatopics my-topic-2 -o yaml
   ```
   The source shows this step once with `oc get` and once with `kubectl get` (see FINDINGS.md). The Topic Operator log runs in the `topic-operator` container of the Entity Operator pod: `kubectl logs my-cluster-entity-operator-0 -c topic-operator`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Match the status to the documented causes:

   | Cause | What the source says |
   |---|---|
   | Partition count decreased | Example condition: `type: NotReady`, `reason: PartitionDecreaseException`, `message: Number of partitions cannot be decreased`. Kafka does not support decreasing partitions. |
   | Topic renamed | `spec.topicName` cannot be changed; a mismatch between `spec.topicName` and `status.topicName` is detected. |
   | `spec.replicas` changed | Listed as a change that `KafkaTopic` does not allow. §12.5 says changing replicas requires Cruise Control (see FINDINGS.md). |
   | Two `KafkaTopic` resources for one Kafka topic | The resource created first manages the topic. The newer resources get a conflict in their status and `Ready` set to `False`. |

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-operator-topic-names-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-topic-replication-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. Check that a Topic Operator is running (usually deployed with the Entity Operator). This is a prerequisite of the topic procedure. The procedure's other prerequisite, a broker listener with mTLS authentication and TLS encryption, belongs to its example and is not a documented cause of `NotReady`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. **Partition decrease:** reset the topic configuration in the `KafkaTopic` to the original partition count. The status then shows the topic as ready.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. **Conflicting resources:** for a Kafka topic whose name is invalid in Kubernetes (for example created by Kafka Streams), create the `KafkaTopic` with a different, valid `.metadata.name` and set `spec.topicName` to the Kafka topic name. NO OFFICIAL SOURCE FOUND for a step that resolves a conflict between two existing resources.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-operator-topic-names-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. **Replication factor change:** use Cruise Control, as described in the source chapter "Using Cruise Control to modify topic replication factor" (not covered by this runbook).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-topic-replication-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. Apply the corrected resource:
   ```
   kubectl apply -f <topic_config_file>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)

### KafkaTopic stuck in deletion

5. The Topic Operator uses finalizers by default (`STRIMZI_USE_FINALIZERS=true`, finalizer `strimzi.io/topic-operator`). If the operator is not running, it cannot remove the finalizer. Deleting `KafkaTopic` resources or their namespace then fails or times out, and the namespace can get stuck terminating.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-deleting-managed-topics-str (Strimzi 1.3.1, retrieved 2026-11-10)
6. **[DESTRUCTIVE: skips cleanup]** Only if the Topic Operator is not running and you want to bypass the finalization process when deleting managed topics, remove the finalizers. Cleanup is then not performed: if you remove the finalizer from a `KafkaTopic` and then delete it, the Kafka topic is not deleted. Compiler's note, not from the source: the all-topics command has no `-n`, so it acts on every `KafkaTopic` in the current namespace of your `kubectl` context; check that context first.

   Remove the finalizers on all topics:
   ```
   kubectl get kt -o=json | jq '.items[].metadata.finalizers = null' | kubectl apply -f -
   ```
   or on one topic:
   ```
   kubectl get kt <topic_name> -o=json | jq '.metadata.finalizers = null' | kubectl apply -f -
   ```
   You can then delete the topics. Topics that were already being deleted then finish deleting.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-removing-topic-finalizers-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

```
kubectl get kafkatopics my-topic-2 -o wide -w -n <namespace>
```
`READY` shows `True`, and the YAML condition is `type: Ready`, `status: 'True'` with no message.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- Changes go one way only, from the `KafkaTopic` to Kafka. Changes made to a managed topic outside the `KafkaTopic` are reverted.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-application-topic-handling-str (Strimzi 1.3.1, retrieved 2026-11-10)
- Increasing `spec.partitions` on a topic with keys changes how records are partitioned. This can cause problems, especially with semantic partitioning.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)
- If you remove the finalizer while the Topic Operator is not running and then delete the `KafkaTopic`, the Kafka topic itself is **not** deleted.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-removing-topic-finalizers-str (Strimzi 1.3.1, retrieved 2026-11-10)
- Deleting topics needs `delete.topic.enable=true` (the default). With `delete.topic.enable=false`, the Topic Operator never removes the finalizers (you can remove them by hand), and the related CRDs or namespaces cannot be deleted. To avoid finalizers, set `STRIMZI_USE_FINALIZERS` to `false`.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-disabling-topic-deletion-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-topic-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [cr-status-and-conditions.md](cr-status-and-conditions.md)
- [operator-logs.md](operator-logs.md)
- [user-not-ready.md](user-not-ready.md)
