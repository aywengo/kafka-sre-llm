# Findings

Attribution and licences of the sources used here: [ATTRIBUTION.md](ATTRIBUTION.md).

Compiled from the sources in [sources.md](sources.md), all retrieved 2026-11-10. Strimzi docs: 1.3.1 (repo commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50). Apache Kafka docs: 4.3 (docs source at tag 4.3.2, commit 0a6c96d3b61167f1141df2032f25f10fd6e118fe). Earlier commits on this branch used the 4.4 docs; see F2.

## F1. Strimzi repo check: `runbook_url` and runbook folders

**Result: neither exists.**

- Searched `strimzi/strimzi-kafka-operator` at tag 1.3.1 (all `*.yaml`, `*.yml`, `*.json`, `*.adoc`, `*.md`) for `runbook`, case-insensitive: **0 matches**.
- `find . -iname '*runbook*'`: **no files or directories**.
- The same two checks on `strimzi/drain-cleaner` (main @ 70f1bc2): **0 matches**.
- Alert rule files checked (`packaging/examples/metrics/`):
  - `prometheus-install/prometheus-rules/`: `prometheus-kafka-rules.yaml`, `prometheus-kafka-exporter-topic-rules-group.yaml`, `prometheus-kafka-certificate-rules.yaml`, `prometheus-kafka-connect-rules.yaml`, `prometheus-mirrormaker2-rules.yaml`, `prometheus-strimzi-cluster-operator-rules.yaml`, `prometheus-strimzi-entity-operator-rules.yaml`, `prometheus-kafka-bridge-rules.yaml`
  - `kube-state-metrics/prometheus-rules.yaml`
- Annotation keys used: `summary` and `description` in all `prometheus-install` rules; `message` in all kube-state-metrics rules. **No rule has a `runbook_url`.**
- None of the 1.3.1 rule files contains a Cruise Control alert.
- The "related runbook" columns in [prometheus-alert-rules.md](runbooks/strimzi/prometheus-alert-rules.md) and [kube-state-metrics-cr-monitoring.md](runbooks/strimzi/kube-state-metrics-cr-monitoring.md) are this compilation's own cross-links. Strimzi does not ship them.

## F2. Version mismatch between the pinned Strimzi and Kafka docs

- Strimzi 1.3.1 supports Kafka 4.2.0 to 4.3.2. The default and latest supported version is 4.3.2 (attributes `KafkaVersionLower`, `KafkaVersionHigher`, `DefaultKafkaVersion` in `documentation/shared/`, and §30.8.2).
- The brief pinned the Apache Kafka docs at **4.4**, which a Strimzi 1.3.1 cluster cannot run. **Decided by the owner on 2026-11-10: the Kafka docs are now pinned at 4.3** (published pages under https://kafka.apache.org/43/operations/ , docs source at tag 4.3.2, commit 0a6c96d3b61167f1141df2032f25f10fd6e118fe). Each Kafka runbook has a version note.
- Content that is only in the 4.4 docs was removed when the pin moved. Each case was found by diffing the cited docs files between tags 4.4.0 and 4.3.2:
  - `kafka-metadata-quorum.sh remove-controller ... --unregister`, and the whole "Unregister Controller" section (`kafka-cluster.sh unregister-controller`): neither is in the 4.3 KRaft page. kraft-controller-membership.md step 7 is now labelled "4.4 only, not valid on Strimzi 1.3.1" and says NO OFFICIAL SOURCE FOUND, and its eval item was replaced. The apache/kafka 4.3.2 tools have neither command: `ClusterTool.java` defines `cluster-id`, `unregister` and `list-endpoints`, and `MetadataQuorumCommand.java` defines `remove-controller` with no unregister option.
  - The note that `--command-config` must point to the configuration of the controller being added, and that its `controller.quorum.bootstrap.servers` must include a current quorum member. The 4.3 page says only that Admin Client settings such as authentication go in `controller.properties`. The runbook now says that and marks the rest NO OFFICIAL SOURCE FOUND.
  - The 4.3 reset-offsets list names no flag for the default execution option ("(default) to display which offsets to reset"), whereas 4.4 says `--dry-run`. consumer-group-position.md and its eval item no longer call the default `--dry-run`.
  - The other differences in the cited files are not used by any runbook: front-matter descriptions, whitespace, the 4.4 `by_duration` recommendation, share-group reset options and DLQ, streams-group tool text, the controller formatting sections, and group-coordinator and consumer metrics.
- Decommissioning starts with cordoning (`cordoned.log.dirs="*"`). **Resolved:** `cordoned.log.dirs` exists in Kafka 4.3.2 (apache/kafka tag 4.3.2, commit 0a6c96d3b61167f1141df2032f25f10fd6e118fe): `server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java:54` defines `CORDONED_LOG_DIRS_CONFIG = "cordoned.log.dirs"`, `core/src/main/scala/kafka/server/DynamicBrokerConfig.scala:590` validates it, and `docs/operations/basic-kafka-operations.md:471-476` at that tag has the same cordon step. So the cordon step is valid for Strimzi 1.3.1 clusters (Kafka up to 4.3.2).
- The repo's own `sources.yaml` pinned Kafka 4.4.0 next to Strimzi 1.3.1. It now pins apache/kafka `4.3.2` and kafka-site `content/en/43/generated`. `MODEL_CARD.md` still describes the 4.4.0 sources that the existing dataset was built from.
- `documentation/shared/attributes.adoc` at tag 1.3.1 sets `ProductVersion: 1.3.0` (line 17), while the published page says 1.3.1 throughout. Take version strings from the published page, not from that attribute.

## F3. Conflict: `reserved.broker.max.id`

- Strimzi 1.3.1 §11.3.1 (https://strimzi.io/docs/operators/1.3.1/deploying#proc-managing-node-pools-ids-str) says to add `reserved.broker.max.id` to the Kafka resource to allow node IDs above 999. The prose spells it `reserved.broker-max.id`, and the YAML example spells it `reserved.broker.max.id: 10000`.
- The Kafka 4.3 ZooKeeper→KRaft page (https://kafka.apache.org/43/getting-started/zk2kraft/#configurations) lists `reserved.broker.max.id` under "Removed the broker id generation-related configurations… used in ZooKeeper mode".
- These sources disagree. The property was not put in any runbook.

## F4. Pod and StrimziPodSet naming with node pools

- §28.1 (manual rolling update) and §11.2.10 (delete pod and PVC) name Kafka pods `<cluster_name>-kafka-<index_number>`. §28.2 names the StrimziPodSet `<cluster_name>-kafka`.
- §30.2.2 (same guide) uses the node-pool pod name `my-cluster-pool-a-1`. §18.3.3 names per-node secrets `<cluster_name>-<pool_name>-<node_id>`. §33.2 uses PVCs such as `data-0-my-cluster-broker-0`.
- The two patterns are inconsistent within the same guide. **Correction:** the guide does state the node-pool names explicitly, in its list of Kafka cluster resources (https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str, `documentation/modules/configuring/ref-list-of-kafka-cluster-resources.adoc:48`): pods are `<kafka_cluster_name>-<pool_name>-<pod_id>`, the StrimziPodSet is `<kafka_cluster_name>-<pool_name>`, and the PVC is `data-<kafka_cluster_name>-<pool_name>-<pod_id>` for single-volume persistent storage, or `data-<id>-<kafka_cluster_name>-<pool_name>-<pod_id>` for each JBOD volume `id` (line 61). An earlier version of this finding said the node-pool names were not stated; that was wrong. The procedures in §28.1 and §11.2.10 still show only the non-node-pool form. The runbooks quote the procedure commands and show the node-pool form next to them, citing that list.

## F5. Restart reasons table (§29.1, §29.2)

- `PodHasOldRevision` appears **twice**, with the two descriptions (disk added or removed; StrimziPodSet updated) cross-referencing each other.
- §29.2 uses `JbodVolumesChanged` as an example `reason`, but it is **not in the reasons table**.
- §29.2 lists the field as `regarding.namespace`, while the other fields use `regardingObject.*`.

## F6. Drain Cleaner

- §27.6: the table gives the default of `STRIMZI_CERTIFICATE_WATCH_ENABLED` as `false`, while the text says the installation files enable the watch by default. The docs do not explain the difference.
- The Strimzi docs (§27.4) talk about moving "Kafka broker pods". The drain-cleaner README says broker **or controller** pods.
- The README was read at `main` (70f1bc2), not at release tag 1.8.0.

## F7. Status editability (§1.1.3 vs §33.2)

§1.1.3 says the `status` cannot be edited (for example with `kubectl edit`). §33.2 tells you to run `kubectl edit kafka <cluster-name> --subresource status` to set `.status.clusterId`. The docs do not reconcile the two statements.

## F8. Expired CA recovery: unnamed "Kafka secret" (§18.4.6)

The procedure says "Delete the Kafka secret." and "Wait for the Cluster Operator to detect the missing Kafka secret and recreate it." (`documentation/modules/security/proc-recovering-expired-ca-certs.adoc:71-72`), without naming the secret. The guide defines a per-node secret `<cluster_name>-<pool_name>-<pod_id>`, which "Contains the private and public keys for a Kafka node. Each pod has its own secret" (§18.3.2, https://strimzi.io/docs/operators/1.3.1/deploying#con-certificates-str, `documentation/modules/security/ref-certificates-and-secrets.adoc:102-103`). The resource list gives the same pattern (§7.2.4, https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str, `documentation/modules/configuring/ref-list-of-kafka-cluster-resources.adoc:48-51`). The procedure does not say that this is the secret it means, and its singular wording may be left over from before node pools. `<kafka_cluster_name>-kafka-brokers` is a Service, not a Secret (same file, line 29). An earlier version of this finding gave the pattern as `<node_id>` and the section as §18.3.3. The source has `<pod_id>`, and the section is §18.3.2.

**Owner's decision (2026-11-10):** quote the procedure verbatim. Give the per-node secret only as a labelled compiler's note, not as a command. Mark step 4.1 "cannot be completed from official docs; ask the Strimzi maintainers". The other secrets the guide lists (`<cluster_name>-cluster-operator-certs`, `<cluster_name>-entity-topic-operator-certs` and the like) are kept out of the steps: the procedure says the Cluster Operator updates other components on reconciliation after the Kafka pods restart. The secret name stays DEFERRED until the maintainers answer.

## F9. Cluster recovery example values do not match (§33.2)

- In the `kubectl get pv` output, `pvc-7e1f67f9-…` is the PV for `data-0-my-cluster-controller-3`. The example PVC `data-0-my-cluster-broker-0` uses `volumeName: pvc-7e1f67f9-…`, and the broker-0 PV in that output is `pvc-5e9c5c7f-…`.
- The example PV is named `pvc-5ead43d1-…` (broker-2 in the output), but its `claimRef` points to `data-0-my-cluster-broker-0` and its `selfLink` ends in `pvc-7e226978-…`.
- The same procedure warns that mounting a PV into the wrong PVC can corrupt data. The runbook tells readers not to copy the example values.

## F10. KafkaTopic

- §12.4 lists "Modifying the number of replicas specified in spec.replicas" as not allowed. §12.5 says changing replicas "requires a deployment of Cruise Control". The docs do not reconcile the two statements. The Cruise Control procedure (chapter 22) was not compiled here.
- `oc get kafkatopics` (OpenShift CLI) appears in three modules, not one: `operators/proc-configuring-kafka-topic.adoc:90` (`my-topic-2`), `operators/proc-converting-non-managed-topics.adoc:58` (`my-topic-1`) and `cruise-control/proc-cruise-control-topic-replication.adoc:53` (`my-topic`). The rest of the guide uses `kubectl get …`. (An earlier version of this finding said it appears once, in §12.4.)

## F11. Cruise Control rebalancing modes

- The "Rebalancing modes" section lists **four** modes (`full`, `add-brokers`, `remove-brokers`, `remove-disks`), then says "The procedure to run a rebalance is actually the same across the three different modes."
- §21.3 lists only `full`, `add-brokers` and `remove-brokers`.
- §21.7 (troubleshooting) lists "You have approved an optimization proposal" as a prerequisite, but `NotReady` can also occur when the resource is first created (the same section says so).
- This matters for v2: a known v1 failure was a Cruise Control answer that invented extra modes. The eval item in `eval/kafkarebalance-notready-and-refresh.jsonl` uses the four documented modes.

## F12. Example Prometheus rules (§25.1.3)

- The docs list rule components for Kafka, Entity Operator, Kafka Connect, HTTP Bridge, MirrorMaker2 and Kafka Exporter. The 1.3.1 repo also has `prometheus-strimzi-cluster-operator-rules.yaml` and `prometheus-kafka-certificate-rules.yaml`, which are not mentioned.
- The summary text of `KafkaNodeContainersDown` and `ConnectContainersDown` says "CrashLookBackOff" (sic).
- `KafkaRebalanceProposalPending` (kube-state-metrics rules) fires on `proposal_ready="True"` for 1h. The name says "pending", but the expression matches a proposal that is ready and not yet approved.

## F13. Rendering bug: `pod-monitors/*.yaml` (§25.2.3)

The published HTML shows `kubectl apply -f pod-monitors/.yaml` and `prometheus-rules/.yaml`. The AsciiDoc source (`documentation/modules/metrics/proc_metrics-deploying-prometheus.adoc`, lines 67–68) has `pod-monitors/*.yaml` and `prometheus-rules/*.yaml`; the asterisks were rendered as bold markup. The runbook quotes the source form and cites the .adoc file.

## F14. Grafana dashboards

The repo has `grafana-dashboards/strimzi-kafka-oauth.json`, which the §25.1.4 table does not list.

`grafana-dashboards/strimzi-cruise-control.json` queries three Cruise Control anomaly-detector metrics: `kafka_cruisecontrol_anomalydetector_balancedness_score_value` (line 352), `kafka_cruisecontrol_anomalydetector_goal_violation_rate_count` (line 547) and `kafka_cruisecontrol_anomalydetector_disk_failure_rate_count` (line 641). An earlier version of these findings, and of the Cruise Control runbook, said no allowed source names anomaly-detector metrics; that was wrong. Alert expressions and the anomaly notifier configuration are still not in the allowed sources.

## F15. Consumer lag panel name

§25.5 calls the panel "Messages consumed per minute". The tip in §25.5.1 says "Messages consumed per second".

## F16. MirrorMaker 2 task restart example (§11.10.4)

The text says "task 0 for connector … MirrorSourceConnector … is restarted", but the command ends in `…MirrorSourceConnector:2`, which is task 2. The heading text of §11.10.4 also says "trigger a restart of a MirrorMaker 2 connector" for the task procedure.

## F17. Kafka upgrade procedure (§30.6)

- `kubectl edit kafka <kafka_configuration_file>` uses a file placeholder where `kubectl edit` takes a resource name.
- The procedure says "Otherwise, go to step 9", but the rendered procedure (`<ol class="arabic">`) has only **8** top-level steps.

## F18. Apache Kafka 4.3 docs: rendering and source issues

- In prose, long options appear with an en dash ("–execute", "–verify", "–members –verbose", "–bootstrap-controller"). Code blocks use the correct `--`. The runbooks use `--`.
- The MirrorMaker MBean patterns read `([-.w]+)`, while the other Kafka MBean patterns use `([-.\w]+)`. The backslash is missing in the published page **and** in the docs source (`docs/operations/geo-replication-(cross-cluster-data-mirroring).md` at 4.3.2; also at 4.4.0).
- The broker metrics table on the monitoring page has no heading or anchor of its own. It sits under `#security-considerations-for-remote-monitoring-using-jmx`, which is what the runbooks cite.
- Typo "Kafka’sintra-cluster" on the geo-replication overview.

## F19. Out of scope or not covered

- **Argo CD / GitOps:** the brief's scope allows Argo CD, but the allowed source list has no Argo CD source, and the Strimzi 1.3.1 deploying guide has no Argo CD procedure. Marked "no official source" in coverage.md. The guide does mention GitOps twice, in the OLM upgrade procedure without an upgrade path (`documentation/modules/upgrading/proc-upgrade-cluster-operator-olm-no-upgrade-path.adoc:33` and `:83`): pause GitOps reconciliation of the OLM resources first, then update the `Subscription` manifest in Git and resume. The default of `STRIMZI_LABELS_EXCLUSION_PATTERN` also excludes Flux labels (`kustomize.toolkit.fluxcd.io.*`, `operators/con-configuring-cluster-operator.adoc:85`). Neither is an Argo CD procedure.
- **Strimzi in-development docs** were not fetched, so no comparison was made.
- **ZooKeeper** content is included only as the migration path required before upgrading (§30.3).

## F20. Conflict: static-to-dynamic controller quorum migration

- Strimzi 1.3.1 §2.1.2 (https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums, `documentation/assemblies/deploying/assembly-kraft-mode.adoc:78-79`): "Migration between static and dynamic controller quorums is not currently supported by Apache Kafka". As a result, "Strimzi uses static controller quorums for all deployments, including new installations."
- Apache Kafka 4.3 KRaft page (https://kafka.apache.org/43/operations/kraft/#upgrade, `docs/operations/kraft.md:69` at 4.3.2; the same line at 4.4.0): "Apache Kafka 4.1 added support for upgrading a cluster from a static controller configuration to a dynamic controller configuration."
- These sources disagree on whether Kafka supports the migration. For a Strimzi 1.3.1 cluster the Strimzi statement governs what the operator does: the quorum is static, and controller node pools cannot be added, removed, scaled or renamed. [kraft-controller-membership.md](runbooks/kafka/kraft-controller-membership.md) now states this first and marks every dynamic-quorum command as Apache Kafka reference, not for Strimzi-managed clusters.
- The earlier version of that runbook said NO OFFICIAL SOURCE FOUND for how Strimzi handles controller membership, and `sources.md` said the Kafka "Upgrade" section was outside the fetched pages. Both were wrong: the Strimzi section exists (§2.1.1, §2.1.2) and the Upgrade section is on the cited KRaft page.

## F21. Drain Cleaner: the drain is expected to fail in the default mode

The drain-cleaner README ("See it in action", lines 206-232 at 70f1bc2) says `kubectl drain` evicts the pods that can be evicted "and eventually fail because Drain Cleaner denied the eviction" of the Kafka broker and controller pods. Once the Cluster Operator has rolled them, the drain is retried and succeeds. The Strimzi 1.3.1 Drain Cleaner procedure (https://strimzi.io/docs/operators/1.3.1/deploying#proc-drain-cleaner-using-str) does not say this. [drain-cleaner.md](runbooks/strimzi/drain-cleaner.md) step 3 now does.

## F22. Storage procedures the storage runbook did not use

The broker storage section (§11.2.9) says "Free disk space or increase available storage capacity" and "Review quota settings" without a procedure, but the same guide has them: resizing persistent volumes (`#proc-resizing-persistent-volumes-str`, `configuring/con-config-storage-kraft.adoc:99-160`), "Setting throughput and storage limits on brokers" and "Choosing a quota plugin" (the last two are the section's own additional resources). [broker-storage-issues.md](runbooks/strimzi/broker-storage-issues.md) now links them.

## F23. Recovery from persistent volumes is routed to from a live-cluster symptom

The broker storage section sends issues "such as failed mounts or unavailable volumes" to cluster recovery from persistent volumes (`configuring/con-kafka-recovering-brokers.adoc:27`). That procedure recovers "a deleted Kafka cluster" after namespace deletion or cluster loss (`managing/proc-cluster-recovery-volume.adoc:11`, `managing/con-cluster-recovery-scenarios.adoc:11-16`). The guide does not say how a failed mount on a running cluster maps to it. The runbooks now state the precondition and flag the ambiguity.

## F24. Strimzi documentation licence

- **Owner's decision (2026-11-10):** the License row of every runbook that uses the Strimzi docs reads "Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC." CC BY 4.0 is not used.
- Evidence checked here at tag 1.3.1: `LICENSE` is the Apache License 2.0; `README.md` says "Strimzi is licensed under the [Apache License](./LICENSE), Version 2.0" (line 98); `documentation/README.md` says the folder "contains the source files for managing the Strimzi documentation" and lists `/documentation/deploying/` for the Deploying and Managing guide. No CC BY statement was found in the repo. The footer of https://strimzi.io/documentation/ gives a copyright line and no licence. The owner also cites the Strimzi documentation contributor guide, which `documentation/README.md` links to, for the docs living in `/documentation`. That page was not fetched here.
- **Not checked:** the site terms at lfprojects.org/policies, which the page footer links to.
- The Apache Kafka docs, the Strimzi example files under `packaging/examples/` and the drain-cleaner README keep their Apache License 2.0 attribution.

## F25. Resource names and commands in the guide that do not work as written

Found by review on PR #1, then checked against the 1.3.1 sources and the Kafka 4.3 docs:

- **Entity Operator pod.** §"Using loggers" (`con-using-loggers_str`, `documentation/modules/configuring/con-using-loggers.adoc:41`) shows `kubectl logs my-cluster-entity-operator-0 -c topic-operator`. The list of Kafka cluster resources (`ref-list-of-kafka-cluster-resources-str`, `ref-list-of-kafka-cluster-resources.adoc:67-74`) says `<kafka_cluster_name>-entity-operator` is a Deployment and its pod is `<kafka_cluster_name>-entity-operator-<random_string>`, so `-0` does not exist. The runbooks use the guide's other form, `kubectl logs -n kafka deployment/my-cluster-entity-operator -c topic-operator` (`proc-configuring-entity-operator-watched-namespace-str`, `proc-configuring-entity-operator-watched-namespace.adoc:121`).
- **Kafka Exporter pod.** The deploy procedure (`proc-kafka-exporter-deploy.adoc:98`) says "Resources, including a `Service` and `Pod`, are created for the Kafka Exporter with the naming convention `<kafka_cluster_name>-kafka-exporter`". The resource list (`:84-93`) names the Deployment, Service, service account and network policy `<kafka_cluster_name>-kafka-exporter`, and the Pod `<kafka_cluster_name>-kafka-exporter-<random_string>`.
- **Cluster Operator logging ConfigMap.** `ref-operator-cluster-logging-configmap.adoc:14-20` says the ConfigMap "is created with default values during installation", then gives `kubectl create -f install/cluster-operator/050-ConfigMap-strimzi-cluster-operator.yaml` to "reapply" an edited file. Compiler's note, not from the source: `kubectl create` fails for an object that already exists. The runbook puts the documented `kubectl edit configmap strimzi-cluster-operator` first.
- **`ActiveControllerCount`.** The Kafka 4.3 broker metrics table says "only one broker in the cluster should have 1". The KRaft controller metrics list the same MBean as "The number of Active Controllers on this node. Valid values are '0' or '1'", and `kraft.md` (#controllers) says each controller "is either an active or a hot standby for the current active controller". The runbook and eval point to the controller-role nodes.
- **mTLS prerequisite of the `KafkaUser` procedure.** Like the `KafkaTopic` procedure (F10), it lists "a Kafka broker listener using mTLS authentication and TLS encryption" as a prerequisite; its example uses mTLS authentication. The runbook treats it as part of the example, not as a requirement for every `KafkaUser`.

## F26. Source commands that do not run as written (corrected in the runbooks)

Found by review on PR #1 and checked here. Each corrected command is listed in `tools/corrected_commands.txt`; every other command is still verbatim.

- **Globs after one `-f`** (`proc_metrics-deploying-prometheus.adoc`, cited in prometheus-alert-rules.md): `kubectl apply -f pod-monitors/*.yaml` and `kubectl apply -f prometheus-rules/*.yaml`. At tag 1.3.1, `pod-monitors/` holds 4 files and `prometheus-rules/` holds 8, so the shell passes extra file names as positional arguments. Tested with kubectl 1.37.1, an empty kubeconfig and an unreachable server: `error: Unexpected args: [pod-monitors/b.yaml]`. The directory form `kubectl apply -f pod-monitors/` gets past argument parsing. `kubectl apply -h` describes `-f` as "The files, directories or URLs that contain the configurations to apply".
- **`describe --replication` without a connection option** (Kafka 4.3 `kraft.md` #add-new-controller, line 195): `bin/kafka-metadata-quorum.sh describe --replication`. In apache/kafka 4.3.2 `tools/src/main/java/org/apache/kafka/tools/MetadataQuorumCommand.java:103`, `--bootstrap-server` and `--bootstrap-controller` form a mutually exclusive group that is `required(true)`. The 4.3 docs give the complete command in "Replace KRaft Controller Disk" (https://kafka.apache.org/43/operations/hardware-and-os/#replace-kraft-controller-disk, `docs/operations/hardware-and-os.md:135`): `$ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication`. That page is now pinned. The checker did not catch this because the short form appears as inline code in the docs source, so it counted as a complete source command.
- **Restart-event queries** (`proc-operator-restart-events-str`, `ref-operator-restart-events-fields-str`): `kubectl -n kafka get events --field-selector reportingController=...`. Kubernetes v1.34.0 source, commit 275918a59a3df182fc5e9f7f9f6e960384399a35:
  - `staging/src/k8s.io/client-go/restmapper/discovery.go:47-48`: "/v1 is special.  It should always come first", so a bare `events` resolves to the core/v1 Event API (https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/staging/src/k8s.io/client-go/restmapper/discovery.go).
  - `pkg/apis/core/v1/conversion.go:445-467`: core/v1 Event field selectors accept `involvedObject.{kind,namespace,name,uid,apiVersion,resourceVersion,fieldPath}`, `reason`, `reportingComponent`, `source`, `type`, `metadata.namespace` and `metadata.name`; anything else is "field label not supported" (https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/pkg/apis/core/v1/conversion.go).
  - `pkg/apis/events/v1/conversion.go:63-77`: events.k8s.io/v1 accepts `regarding.*`, `reportingController`, `reason`, `type`, `metadata.namespace` and `metadata.name`, and maps them to the core names (https://github.com/kubernetes/kubernetes/blob/275918a59a3df182fc5e9f7f9f6e960384399a35/pkg/apis/events/v1/conversion.go).

  So the source's `reportingController` queries need `events.events.k8s.io`; `regardingObject.*` is accepted by neither API; and the guide's note that the names differ "in older versions of Kubernetes" describes a difference between API groups. These field-selector rules were read from the source and not run against a cluster.
- **Unescaped dot in a JSONPath key** (`proc-tracking-cluster-rebalance.adoc:52`): `-o jsonpath="{['data']['executorState.json']}"`. Tested with kubectl 1.37.1 on a `--dry-run=client` ConfigMap holding the key `executorState.json`: the source form prints nothing, and `{['data']['executorState\.json']}` prints the value. The same guide escapes the dot in `{.data.ca\.crt}` (expired CA procedure).

## Verification

The first version of this table came from scripts kept outside the repo, so its counts could not be reproduced (two audits found 2 steps without a citation and 1 missing repo path where it reported 0). The checks are now in `tools/`:

```
tools/fetch_sources.sh <cache_dir>
python3 -I tools/check_runbooks.py --sources <cache_dir>
```

Run 2026-11-10 against clean checkouts (no modified or untracked files) of strimzi-kafka-operator 1.3.1 (555c3e0), drain-cleaner 70f1bc2, apache/kafka 4.3.2 (0a6c96d) and the pages in sources.md fetched by the script (hashes matched):

| Check | Result |
|---|---|
| Code items (fenced blocks and inline spans) found in a pinned source | **956 of 1046** (131 blocks, 915 spans). The other 90: 43 repo-path spans, all of which exist; 47 items that are knowingly not verbatim (26 placeholders, elisions or composed spans, 3 names from the Kafka 4.3.2 tools source, the abridged block below, 5 blocks with corrected commands and 12 Kubernetes field names, API group names and error text, F26), each listed with a reason in `tools/allowed_unmatched.txt`. An item that is neither found nor listed fails the check, and so does a list entry that matches no item |
| Code blocks whose lines (whitespace collapsed, callout markers and blank lines dropped) appear in order and contiguous inside one source code element | **125 of 131** (the abridged Kafka Exporter example in consumer-lag.md, labelled as abridged, and 5 blocks holding the commands corrected in F26; each list entry is keyed by a hash of the block, so any edit to it fails the check) |
| Unterminated code fences | **0** (an unclosed fence fails the check, and its contents are still checked) |
| Commands (`kubectl`, `bin/`, `oc`, `openssl`, `helm`; anywhere in a block, as complete logical commands with continuation lines and multi-line quoted arguments) that are not equal to a complete command in a source code element | **0 of 143**: 130 equal a source command exactly; 7 are inline prose mentions listed with a reason in `tools/abbreviated_commands.txt` (spans only); 6 are corrections of source commands that do not run as written, listed in `tools/corrected_commands.txt`, each naming the FINDINGS entry with the evidence (F26). An unused entry, or one without a FINDINGS reference, fails |
| Cited Strimzi anchors / Kafka anchors / GitHub paths / index pages that fail | **0 of 79 / 0 of 37 / 0 of 11 / 0 of 2** (distinct URLs in the runbooks, FINDINGS.md, coverage.md, RESOLUTION.md, ATTRIBUTION.md and eval `source_url`) |
| Backtick repo paths missing in the pinned repos | **0 of 53** |
| Pinned-source URLs cited in a runbook body but missing from its Sources row | **0** |
| Top-level list steps in Diagnosis / Remediation / Verification / Rollback | **293** (93 / 122 / 15 / 63) |
| Steps with a Source line | **282 of 293**; the other 11 carry a NO OFFICIAL SOURCE marker; **0 of 293** have neither |
| Coverage rows | **37**: 16 covered, 20 partial, 1 no official source; Totals table matches; 0 "covered" rows with a NO OFFICIAL SOURCE marker in a required section |
| Eval items | **189** in 32 files; 0 fail the field, runbook or `source_url` check; 0 identical questions |

With the earlier 4.4 pin, run against the original PR head (`0fabf2c`), the script gave 878 code items (125 blocks, 753 spans), 289 steps and 1 missing repo path, which matches both audits. On steps it is stricter than both audits: it also counts cross-link bullets under a section-level NO OFFICIAL SOURCE paragraph, so it finds 8 unmarked steps where the audits found 2.

**Not checked mechanically:** whether a code item sits in the section its step cites (both audits did this by hand), and whether each eval answer is fully supported by its runbook. The eval check only confirms that the `source_url` is cited in the runbook.
