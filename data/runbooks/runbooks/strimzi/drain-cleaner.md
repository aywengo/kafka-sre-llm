# Moving Kafka pods off a draining node with Strimzi Drain Cleaner

| Field | Value |
|---|---|
| Product | Strimzi Drain Cleaner with Strimzi Cluster Operator, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (chapter 27); drain-cleaner repo README at main commit 70f1bc25bc14aacd15eb478768db6566bde783e3 (latest tag at retrieval: 1.8.0) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#default_webhook_configuration ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-deploying-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-using-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-certs-renewing-str ; https://github.com/strimzi/drain-cleaner/blob/70f1bc25bc14aacd15eb478768db6566bde783e3/README.md ; repo `documentation/modules/drain-cleaner/*.adoc` @ strimzi-kafka-operator tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Also derived from: strimzi/drain-cleaner README, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[ROLLING RESTART]** Draining a node with Drain Cleaner makes the Cluster Operator roll the affected Kafka pods.

## Symptoms / trigger

A Kubernetes worker node that hosts Kafka pods has to be drained, for example for maintenance or a Kubernetes upgrade. Drain Cleaner works with the Cluster Operator to move Kafka broker pods off the node being drained. It annotates the pods with the rolling-update annotation, and the Cluster Operator then rolls them.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-using-str (Strimzi 1.3.1, retrieved 2026-11-10)

The drain-cleaner README also mentions controller pods: Drain Cleaner annotates Kafka broker or controller pods with `strimzi.io/manual-rolling-update` and denies the eviction request.
— *Source:* https://github.com/strimzi/drain-cleaner/blob/70f1bc25bc14aacd15eb478768db6566bde783e3/README.md (drain-cleaner main@70f1bc2, retrieved 2026-11-10)

## Diagnosis steps (before draining)

1. Check the prerequisites: Drain Cleaner is deployed; the Kafka cluster is highly available; topics have a replication factor of at least 3 and `min.insync.replicas` one less than the replication factor.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-deploying-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Find out which mode Drain Cleaner runs in.
   - The default mode denies (blocks) the eviction request and has the Cluster Operator move the pod. This mode works better with cluster autoscaling tools and needs no special PodDisruptionBudget.
   - Legacy mode (`STRIMZI_DENY_EVICTION` set to `"false"`) allows the eviction and also tells the Cluster Operator to move the pod. Legacy mode needs the Kafka PodDisruptionBudget set to `maxUnavailable: 0`.

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-deploying-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. If you use anti-affinity for Kafka pods, consider adding a spare worker node. Otherwise the rolled pods may be unschedulable and the drain may fail.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-using-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. The webhook is a `ValidatingWebhookConfiguration` that only evaluates `CREATE` operations on the `pods/eviction` subresource. It calls service `strimzi-drain-cleaner` in namespace `strimzi-drain-cleaner`, on path `/drainer`, port 443, over TLS. No manual configuration is needed unless you use your own TLS certificates.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#default_webhook_configuration (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

### Deploy Drain Cleaner (if not deployed)

1. Legacy mode only: set `spec.kafka.template.podDisruptionBudget.maxUnavailable: 0` in the `Kafka` resource, then apply it:
   ```
   kubectl apply -f <kafka_configuration_file>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-deploying-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Deploy Drain Cleaner:
   - With cert-manager:
     ```
     kubectl apply -f ./install/drain-cleaner/certmanager
     ```
     The webhook TLS certificates are generated and injected automatically.
   - Without cert-manager: add TLS certificates (you must renew them before they expire), then:
     ```
     kubectl apply -f ./install/drain-cleaner/kubernetes
     ```
   - On OpenShift:
     ```
     kubectl apply -f ./install/drain-cleaner/openshift
     ```

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-deploying-str (Strimzi 1.3.1, retrieved 2026-11-10)

### Drain the node

3. **[ROLLING RESTART] [FORCED DRAIN: `--force`, `--delete-emptydir-data`; NO OFFICIAL SOURCE FOUND in these sources for what the two flags do]** Drain the Kubernetes node that hosts the Kafka broker:
   ```
   kubectl get nodes
   kubectl drain <name_of_node> --delete-emptydir-data --ignore-daemonsets --timeout=6000s --force
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-using-str (Strimzi 1.3.1, retrieved 2026-11-10)

   **Expect the drain to fail in the default mode.** `kubectl drain` cordons the node and evicts the pods that can be evicted. It then fails, because Drain Cleaner denies the eviction of the Kafka broker and controller pods. The error reads `admission webhook "strimzi-drain-cleaner.strimzi.io" denied the request: The pod will be rolled by the Strimzi Cluster Operator`. Once the Cluster Operator has restarted those pods, run `kubectl drain` again; it should then succeed and print `<worker-node> drained`. In legacy mode (`STRIMZI_DENY_EVICTION=false`) the eviction requests are allowed instead.
   — *Source:* https://github.com/strimzi/drain-cleaner/blob/70f1bc25bc14aacd15eb478768db6566bde783e3/README.md ("See it in action", lines 196-232; "Legacy mode", lines 99-110) (drain-cleaner main@70f1bc2, retrieved 2026-11-10)

## Verification

1. In the Drain Cleaner log, check the eviction events that show the pods were annotated:
   ```
   INFO ... Received eviction webhook for Pod my-cluster-kafka-0 in namespace my-project
   INFO ... Pod my-cluster-kafka-0 in namespace my-project will be annotated for restart
   INFO ... Pod my-cluster-kafka-0 in namespace my-project found and annotated for restart
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-using-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. In the Cluster Operator log, check the rolling updates:
   ```
   INFO  PodOperator:68 - Reconciliation #13(timer) Kafka(my-project/my-cluster): Rolling Pod my-cluster-kafka-0
   INFO  AbstractOperator:500 - Reconciliation #13(timer) Kafka(my-project/my-cluster): reconciled
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-using-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- Certificate watch: by default the Drain Cleaner Deployment watches its TLS secret and restarts itself when the secret changes (for example on renewal). The installation files enable this. To disable it, set `STRIMZI_CERTIFICATE_WATCH_ENABLED` to `false` in `060-Deployment.yaml`. Related variables: `STRIMZI_CERTIFICATE_WATCH_NAMESPACE` (default `strimzi-drain-cleaner`), `STRIMZI_CERTIFICATE_WATCH_POD_NAME`, `STRIMZI_CERTIFICATE_WATCH_SECRET_NAME` (default `strimzi-drain-cleaner`), `STRIMZI_CERTIFICATE_WATCH_SECRET_KEYS` (default `tls.crt, tls.key`). The source suggests the Downward API for the namespace and pod-name variables. The table lists the `STRIMZI_CERTIFICATE_WATCH_ENABLED` default as `false`, while the text says the installation files enable it (see FINDINGS.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-certs-renewing-str (Strimzi 1.3.1, retrieved 2026-11-10)
- To stop Drain Cleaner from draining Kafka pods, set `STRIMZI_DRAIN_KAFKA` to `false`. To restrict the namespaces it handles, set `STRIMZI_DRAIN_NAMESPACES` to a comma-separated list; empty (the default) means all namespaces. When it denies an eviction, it returns HTTP status `500` by default.
  — *Source:* https://github.com/strimzi/drain-cleaner/blob/70f1bc25bc14aacd15eb478768db6566bde783e3/README.md (drain-cleaner main@70f1bc2, retrieved 2026-11-10)

## Related runbooks

- [manual-rolling-update.md](manual-rolling-update.md)
- [node-maintenance-kubernetes-upgrade.md](node-maintenance-kubernetes-upgrade.md)
