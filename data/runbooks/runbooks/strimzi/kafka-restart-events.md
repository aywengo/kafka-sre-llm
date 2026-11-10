# Finding why the Cluster Operator restarted a Kafka pod

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-fields-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str ; repo `documentation/modules/deploying/ref-operator-restart-events-reasons.adoc`, `ref-operator-restart-events-fields.adoc`, `proc-operator-restart-events.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

A Kafka pod was restarted (rolled) and you need to know why. The Cluster Operator emits a Kubernetes event with a reason for each restart it starts.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. List all restart events emitted by the Cluster Operator:
   ```
   kubectl -n kafka get events --field-selector reportingController=strimzi.io/cluster-operator
   ```
   The example output in the source shows `TYPE`, `REASON`, `OBJECT` (for example `kafka/strimzi-cluster`) and a `MESSAGE` such as `Rolling Pod strimzi-cluster-kafka-0 due to CA certificate renewed`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Filter by reason:
   ```
   kubectl -n kafka get events --field-selector reportingController=strimzi.io/cluster-operator,reason=PodForceRestartOnError
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Get the full details in YAML:
   ```
   kubectl -n kafka get events --field-selector reportingController=strimzi.io/cluster-operator,reason=PodForceRestartOnError -o yaml
   ```
   In the detailed event, `action` is `StrimziInitiatedPodRestart`, `related` names the restarted Pod, `reportingController` is `strimzi.io/cluster-operator` and `type` is `Normal`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-operator-restart-events-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. Fields you can use with `--field-selector`:

   | Field | Meaning |
   |---|---|
   | `regardingObject.kind` | Owner of the restarted Pod; always `Kafka` for restart events |
   | `regarding.namespace` | Namespace of the resource (the field name is spelled this way in the source) |
   | `regardingObject.name`, `regardingObject.uid` | Name and unique ID of the resource |
   | `reason` | Restart reason |
   | `reportingController` | Always `strimzi.io/cluster-operator` |
   | `source` | Older form of `reportingController` |
   | `type` | `Warning` or `Normal`; Strimzi restart events are `Normal` |

   On older Kubernetes versions the `regarding` fields may use the `involvedObject` prefix instead, and `reportingController` used to be called `reportingComponent`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-fields-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. Look up the reason:

   | Reason | Meaning (reworded from the source) |
   |---|---|
   | `CaCertHasOldGeneration` | Pod still uses a server certificate signed by an old CA; restarted as part of the certificate update |
   | `CaCertRemoved` | Expired CA certificates were removed; pod restarted to run with the current certificates |
   | `CaCertRenewed` | CA certificates were renewed; pod restarted to use them |
   | `ClusterCaCertKeyReplaced` | Key that signs the cluster CA certificates was replaced; restart is part of CA renewal |
   | `ConfigChangeRequiresRestart` | Kafka configuration changed and some of the changed properties need a broker restart |
   | `FileSystemResizeNeeded` | File system size increased; restart needed to apply it |
   | `KafkaCertificatesChanged` | One or more broker TLS certificates updated; restart needed to use them |
   | `ManualRollingUpdate` | User annotated the pod or its StrimziPodSet to trigger a restart |
   | `PodForceRestartOnError` | An error occurred that a pod restart is needed to fix |
   | `PodHasOldRevision` | A disk was added to or removed from the Kafka volumes, or the pod's StrimziPodSet changed and the pod must be recreated (the source lists this reason twice; see FINDINGS.md) |
   | `PodStuck` | Pod still pending and not (or not able to be) scheduled; operator restarted it as a final attempt |
   | `PodUnresponsive` | Operator could not connect to the pod, which can mean the broker did not start correctly; restarted to try to fix it |

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

NO OFFICIAL SOURCE FOUND for remediation in this chapter. The restart events are informational. Follow the runbook that matches the reason:
- CA reasons: [expired-ca-certificates.md](expired-ca-certificates.md)
- `ManualRollingUpdate`: [manual-rolling-update.md](manual-rolling-update.md)
- `PodStuck`, `PodUnresponsive`, storage-related reasons: [broker-storage-issues.md](broker-storage-issues.md)

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
