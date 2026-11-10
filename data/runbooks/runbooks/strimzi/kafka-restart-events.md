# Finding why the Cluster Operator restarted a Kafka pod

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-fields-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str ; repo `documentation/modules/deploying/ref-operator-restart-events-reasons.adoc`, `ref-operator-restart-events-fields.adoc`, `proc-operator-restart-events.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) ; Kubernetes v1.34.0 source (FINDINGS F26) |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Prose reworded; commands quoted verbatim, except the three event queries corrected in FINDINGS F26. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

A Kafka pod was restarted (rolled) and you need to know why. The Cluster Operator emits a Kubernetes event with a reason for each restart it starts.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. List all restart events emitted by the Cluster Operator. Query the `events.k8s.io` API group by name:
   ```
   kubectl -n kafka get events.events.k8s.io --field-selector reportingController=strimzi.io/cluster-operator
   ```
   **Corrected command (FINDINGS F26).** The source writes `kubectl -n kafka get events --field-selector reportingController=strimzi.io/cluster-operator`. Compiler's note, not from the Strimzi source (from the Kubernetes v1.34.0 source): a bare `events` resolves to the core/v1 Event API, which does not accept the `reportingController` field selector (it calls the field `reportingComponent`). Naming the `events.k8s.io` API group makes the source's field names work.
   The example output in the source shows `TYPE`, `REASON`, `OBJECT` (for example `kafka/strimzi-cluster`) and a `MESSAGE` such as `Rolling Pod strimzi-cluster-kafka-0 due to CA certificate renewed`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str (Strimzi 1.3.1, retrieved 2026-11-10); https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/staging/src/k8s.io/client-go/restmapper/discovery.go ; https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/pkg/apis/core/v1/conversion.go ; https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/pkg/apis/events/v1/conversion.go (Kubernetes v1.34.0, retrieved 2026-11-10)
2. Filter by reason:
   ```
   kubectl -n kafka get events.events.k8s.io --field-selector reportingController=strimzi.io/cluster-operator,reason=PodForceRestartOnError
   ```
   Corrected in the same way as step 1 (FINDINGS F26).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Get the full details in YAML:
   ```
   kubectl -n kafka get events.events.k8s.io --field-selector reportingController=strimzi.io/cluster-operator,reason=PodForceRestartOnError -o yaml
   ```
   In the detailed event, `action` is `StrimziInitiatedPodRestart`, `related` names the restarted Pod, `reportingController` is `strimzi.io/cluster-operator` and `type` is `Normal`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. Fields you can use with `--field-selector`. The field names depend on the API group you query, not on the Kubernetes version (FINDINGS F26):

   | Source's field | `events.events.k8s.io` | core `events` | Meaning (from the source) |
   |---|---|---|---|
   | `regardingObject.kind` | `regarding.kind` | `involvedObject.kind` | Owner of the restarted Pod; always `Kafka` for restart events |
   | `regarding.namespace` | `regarding.namespace` | `involvedObject.namespace` | Namespace of the resource |
   | `regardingObject.name`, `regardingObject.uid` | `regarding.name`, `regarding.uid` | `involvedObject.name`, `involvedObject.uid` | Name and unique ID of the resource |
   | `reason` | `reason` | `reason` | Restart reason |
   | `reportingController` | `reportingController` | `reportingComponent` | Always `strimzi.io/cluster-operator` |
   | `source` | not accepted | `source` | Older form of `reportingController` |
   | `type` | `type` | `type` | `Warning` or `Normal`; Strimzi restart events are `Normal` |

   The source lists the names in the first column and says that "In older versions of Kubernetes, the fields using the `regarding` prefix might use an `involvedObject` prefix instead. `reportingController` was previously called `reportingComponent`." Compiler's note, not from the Strimzi source: in the Kubernetes v1.34.0 source, the `events.k8s.io/v1` Event API accepts the `regarding.*` names and `reportingController` and maps them to the core field names, and the core/v1 Event API accepts only the `involvedObject.*` names, `reportingComponent` and `source`. Neither accepts `regardingObject.*`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-fields-str (Strimzi 1.3.1, retrieved 2026-11-10); https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/pkg/apis/events/v1/conversion.go ; https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/pkg/apis/core/v1/conversion.go (Kubernetes v1.34.0, retrieved 2026-11-10)
5. Look up the reason:

   | Reason | Meaning (reworded from the source) |
   |---|---|
   | `CaCertHasOldGeneration` | Pod still uses a server certificate signed by an old CA; restarted as part of the certificate update |
   | `CaCertRemoved` | Expired CA certificates were removed; pod restarted to run with the current certificates |
   | `CaCertRenewed` | CA certificates were renewed; pod restarted to use them |
   | `ClusterCaCertKeyReplaced` | Key that signs the cluster CA certificates was replaced; restart is part of CA renewal |
   | `ConfigChangeRequiresRestart` | Some Kafka configuration properties are changed dynamically, but others require that the broker be restarted |
   | `FileSystemResizeNeeded` | File system size increased; restart needed to apply it |
   | `KafkaCertificatesChanged` | One or more broker TLS certificates updated; restart needed to use them |
   | `ManualRollingUpdate` | User annotated the pod or its StrimziPodSet to trigger a restart |
   | `PodForceRestartOnError` | An error occurred that a pod restart is needed to fix |
   | `PodHasOldRevision` | A disk was added to or removed from the Kafka volumes, or the pod's StrimziPodSet changed and the pod must be recreated (the source lists this reason twice; see FINDINGS.md) |
   | `PodStuck` | Pod still pending and not (or not able to be) scheduled; operator restarted it as a final attempt |
   | `PodUnresponsive` | Operator could not connect to the pod, which can mean the broker did not start correctly; restarted to try to fix it |

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

NO OFFICIAL SOURCE FOUND for remediation in this chapter. The restart events are informational. The routes below are this compilation's own cross-links; NO OFFICIAL SOURCE FOUND for any of them.
- Own cross-link, NO OFFICIAL SOURCE FOUND for the route. CA reasons (`CaCertRenewed`, `CaCertHasOldGeneration`, `CaCertRemoved`, `ClusterCaCertKeyReplaced`): these describe restarts during normal CA renewal. For example, `CaCertRenewed` means the CA certificates have been renewed and the pod is restarted to run with the updated certificates (source: https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str, Strimzi 1.3.1, retrieved 2026-11-10). First check the validity of the CA certificates with the `openssl x509 -noout -dates` commands in [expired-ca-certificates.md](expired-ca-certificates.md), Diagnosis step 3. Go on to that runbook's **destructive** remediation only if a CA has actually expired.
- Own cross-link, NO OFFICIAL SOURCE FOUND for the route. `ManualRollingUpdate`: [manual-rolling-update.md](manual-rolling-update.md)
- Own cross-link, NO OFFICIAL SOURCE FOUND for the route. `PodStuck`, `PodUnresponsive`: start with [cr-status-and-conditions.md](cr-status-and-conditions.md) and [operator-logs.md](operator-logs.md). Use [broker-storage-issues.md](broker-storage-issues.md) only if the storage symptoms listed there are present; its last step is destructive.

## Verification

NO OFFICIAL SOURCE FOUND.

## Rollback or caveats

- The `firstTimestamp`, `lastTimestamp` and `source` event fields are deprecated and are not filled in for these events.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The filter section gives `JbodVolumesChanged` as an example `reason`, but the reasons table does not list it.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-fields-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [manual-rolling-update.md](manual-rolling-update.md)
- [expired-ca-certificates.md](expired-ca-certificates.md)
- [operator-logs.md](operator-logs.md)
