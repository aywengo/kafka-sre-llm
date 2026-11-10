# KafkaUser not ready

| Field | Value |
|---|---|
| Product | Strimzi User Operator, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§13.1, §16.3.1) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-securing-client-labels-str ; repo `documentation/modules/operators/proc-configuring-kafka-user.adoc`, `documentation/modules/security/con-securing-client-labels.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

After `kubectl apply` of a `KafkaUser`, the `READY` column in `kubectl get kafkausers -o wide` stays blank, the resource has a `NotReady` condition, or its status stays empty.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-securing-client-labels-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Watch the user status:
   ```
   kubectl get kafkausers -o wide -w -n <namespace>
   ```
   A user was created successfully when `READY` shows `True`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. If `READY` stays blank, read the status message in the resource YAML or in the User Operator logs:
   ```
   kubectl get kafkausers my-user-2 -o yaml
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Match the status to the documented causes:

   | Cause | What the source says |
   |---|---|
   | ACLs configured but simple authorization not enabled in Kafka | Example condition: `type: NotReady`, `reason: InvalidResourceException`, `message: Simple authorization ACL rules are configured but not supported in the Kafka cluster configuration.` |
   | `strimzi.io/cluster` label does not match the Kafka cluster | The User Operator cannot identify the `KafkaUser` and does not create the user; the status stays empty. |
   | Mechanism mismatch | The authentication and authorization mechanisms in the `KafkaUser` must match the equivalent Kafka configuration. |

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-securing-client-labels-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. Check the prerequisites: a running Kafka cluster with a broker listener that uses mTLS authentication and TLS encryption, and a running User Operator (usually deployed with the Entity Operator).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. **[POSSIBLE ROLLING RESTART: cluster-wide authorization change on the `Kafka` resource]** **Simple authorization missing:** enable it in the `Kafka` resource:
   ```
     apiVersion: kafka.strimzi.io/v1
     kind: Kafka
     metadata:
       name: my-cluster
     spec:
       kafka:
         # ...
         authorization:
           type: simple
   ```
   The source does not say whether this change triggers a rolling update. Treat it as one that may roll every broker: the restart reasons table says some Kafka configuration changes are applied dynamically, while others require that the broker be restarted (`ConfigChangeRequiresRestart`). Compiler's note, not from the source: enabling authorization also changes which clients are allowed, so plan it as a cluster-wide change.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. **Wrong label:** set `metadata.labels.strimzi.io/cluster` to the name of the `Kafka` resource:
   ```
   apiVersion: kafka.strimzi.io/v1
   kind: KafkaUser
   metadata:
     name: my-user
     labels:
       strimzi.io/cluster: my-cluster
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-securing-client-labels-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Apply the corrected resource:
   ```
   kubectl apply -f <user_config_file>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

```
kubectl get kafkausers my-user-2 -o wide -w -n <namespace>
```
`READY` shows `True`, and fetching the YAML shows no messages.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-configuring-kafka-user-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

NO OFFICIAL SOURCE FOUND.

## Related runbooks

- [topic-not-ready.md](topic-not-ready.md)
- [cr-status-and-conditions.md](cr-status-and-conditions.md)
- [cluster-recovery-from-persistent-volumes.md](cluster-recovery-from-persistent-volumes.md) (recreating users after recovery)
