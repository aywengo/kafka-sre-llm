# Findings

Compiled from the sources in [sources.md](sources.md), all retrieved 2026-11-10. Strimzi docs: 1.3.1 (repo commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50). Apache Kafka docs: 4.4 (docs source at tag 4.4.0, commit 1156b2752aaa28eafc0b4c4d387e63356793268d).

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
- The Apache Kafka docs pinned for this task are **4.4**. A Strimzi 1.3.1 cluster cannot run Kafka 4.4, so 4.4-only behaviour in the Kafka runbooks may not apply to it. Each Kafka runbook has a version note.
- One concrete case: decommissioning starts with cordoning (`cordoned.log.dirs="*"`) in the 4.4 docs. These runbooks do not establish whether Kafka 4.3.2 supports it, because the 4.3 docs were not fetched.
- The repo's own `sources.yaml` also pins Kafka 4.4.0 next to Strimzi 1.3.1, so the training data has the same mismatch. **Open decision for v2:** whether to pin the Kafka docs at 4.3 to match Strimzi 1.3.1.

## F3. Conflict: `reserved.broker.max.id`

- Strimzi 1.3.1 §11.3.1 (https://strimzi.io/docs/operators/1.3.1/deploying#proc-managing-node-pools-ids-str) says to add `reserved.broker.max.id` to the Kafka resource to allow node IDs above 999. The prose spells it `reserved.broker-max.id`, and the YAML example spells it `reserved.broker.max.id: 10000`.
- The Kafka 4.4 ZooKeeper→KRaft page (https://kafka.apache.org/44/getting-started/zk2kraft/#configurations) lists `reserved.broker.max.id` under "Removed the broker id generation-related configurations… used in ZooKeeper mode".
- These sources disagree. The property was not put in any runbook.

## F4. Pod and StrimziPodSet naming with node pools

- §28.1 (manual rolling update) and §11.2.10 (delete pod and PVC) name Kafka pods `<cluster_name>-kafka-<index_number>`. §28.2 names the StrimziPodSet `<cluster_name>-kafka`.
- §30.2.2 (same guide) uses the node-pool pod name `my-cluster-pool-a-1`. §18.3.3 names per-node secrets `<cluster_name>-<pool_name>-<node_id>`. §33.2 uses PVCs such as `data-0-my-cluster-broker-0`.
- The two patterns are inconsistent within the same guide. **Correction:** the guide does state the node-pool names explicitly, in its list of Kafka cluster resources (https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str, `documentation/modules/configuring/ref-list-of-kafka-cluster-resources.adoc:48`): pods are `<kafka_cluster_name>-<pool_name>-<pod_id>`, the StrimziPodSet is `<kafka_cluster_name>-<pool_name>`, and the PVC is `data-<kafka_cluster_name>-<pool_name>-<pod_id>`. An earlier version of this finding said the node-pool names were not stated; that was wrong. The procedures in §28.1 and §11.2.10 still show only the non-node-pool form. The runbooks quote the procedure commands and show the node-pool form next to them, citing that list.

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

The procedure says to "Delete the Kafka secret" without naming it. The secrets reference (§18.3.3) lists per-node secrets `<cluster_name>-<pool_name>-<node_id>` and several operator certificate secrets. Which secrets are meant is **ambiguous**. The runbook leaves it unnamed.

## F9. Cluster recovery example values do not match (§33.2)

- In the `kubectl get pv` output, `pvc-7e1f67f9-…` is the PV for `data-0-my-cluster-controller-3`. The example PVC `data-0-my-cluster-broker-0` uses `volumeName: pvc-7e1f67f9-…`, and the broker-0 PV in that output is `pvc-5e9c5c7f-…`.
- The example PV is named `pvc-5ead43d1-…` (broker-2 in the output), but its `claimRef` points to `data-0-my-cluster-broker-0` and its `selfLink` ends in `pvc-7e226978-…`.
- The same procedure warns that mounting a PV into the wrong PVC can corrupt data. The runbook tells readers not to copy the example values.

## F10. KafkaTopic

- §12.4 lists "Modifying the number of replicas specified in spec.replicas" as not allowed. §12.5 says changing replicas "requires a deployment of Cruise Control". The docs do not reconcile the two statements. The Cruise Control procedure (chapter 22) was not compiled here.
- §12.4 uses `oc get kafkatopics my-topic-2 -o yaml` (OpenShift CLI) once and `kubectl get …` elsewhere. §13.1 uses only `kubectl`.

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

## F15. Consumer lag panel name

§25.5 calls the panel "Messages consumed per minute". The tip in §25.5.1 says "Messages consumed per second".

## F16. MirrorMaker 2 task restart example (§11.10.4)

The text says "task 0 for connector … MirrorSourceConnector … is restarted", but the command ends in `…MirrorSourceConnector:2`, which is task 2. The heading text of §11.10.4 also says "trigger a restart of a MirrorMaker 2 connector" for the task procedure.

## F17. Kafka upgrade procedure (§30.6)

- `kubectl edit kafka <kafka_configuration_file>` uses a file placeholder where `kubectl edit` takes a resource name.
- The procedure says "Otherwise, go to step 9", but the rendered procedure (`<ol class="arabic">`) has only **8** top-level steps.

## F18. Apache Kafka 4.4 docs: rendering and source issues

- In prose, long options appear with an en dash ("–execute", "–verify", "–members –verbose", "–bootstrap-controller"). Code blocks use the correct `--`. The runbooks use `--`.
- The MirrorMaker MBean patterns read `([-.w]+)`, while the other Kafka MBean patterns use `([-.\w]+)`. The backslash is missing in the published page **and** in the docs source (`docs/operations/geo-replication-(cross-cluster-data-mirroring).md` at 4.4.0).
- The broker metrics table on the monitoring page has no heading or anchor of its own. It sits under `#security-considerations-for-remote-monitoring-using-jmx`, which is what the runbooks cite.
- Typo "Kafka’sintra-cluster" on the geo-replication overview.

## F20. Conflict: static-to-dynamic controller quorum migration

- Strimzi 1.3.1 §2.1.2 (https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums, `documentation/assemblies/deploying/assembly-kraft-mode.adoc:78-79`): "Migration between static and dynamic controller quorums is not currently supported by Apache Kafka". As a result, "Strimzi uses static controller quorums for all deployments, including new installations."
- Apache Kafka 4.4 KRaft page (https://kafka.apache.org/44/operations/kraft/#upgrade, `docs/operations/kraft.md:69` at 4.4.0): "Apache Kafka 4.1 added support for upgrading a cluster from a static controller configuration to a dynamic controller configuration."
- These sources disagree on whether Kafka supports the migration. For a Strimzi 1.3.1 cluster the Strimzi statement governs what the operator does: the quorum is static, and controller node pools cannot be added, removed, scaled or renamed. [kraft-controller-membership.md](runbooks/kafka/kraft-controller-membership.md) now states this first and marks every dynamic-quorum command as Apache Kafka reference, not for Strimzi-managed clusters.
- The earlier version of that runbook said NO OFFICIAL SOURCE FOUND for how Strimzi handles controller membership, and `sources.md` said the Kafka "Upgrade" section was outside the fetched pages. Both were wrong: the Strimzi section exists (§2.1.1, §2.1.2) and the Upgrade section is on the cited KRaft page.

## F19. Out of scope or not covered

- **Argo CD / GitOps:** the brief's scope allows Argo CD, but the allowed source list has no Argo CD or GitOps source, and the Strimzi 1.3.1 deploying guide has no Argo CD procedure. Marked "no official source" in coverage.md.
- **Strimzi in-development docs** were not fetched, so no comparison was made.
- **ZooKeeper** content is included only as the migration path required before upgrading (§30.3).

## Verification

Done with scripts in the session scratchpad, against the fetched copies listed in sources.md.

| Check | Result |
|---|---|
| Steps in Diagnosis / Remediation / Verification / Rollback sections (top-level list items) | **289** |
| Steps without a citation (or an explicit NO OFFICIAL SOURCE FOUND) | **0** |
| Cited `strimzi.io` / `kafka.apache.org` anchors missing from the fetched page | **0** |
| Cited Strimzi repo paths missing at tag 1.3.1 | **0** (2 wrong paths found and fixed) |
| Code-block lines not found verbatim in a fetched page, `.adoc` source, example YAML or the drain-cleaner README | **0** |
| Inline `kubectl` / `bin/` commands not found verbatim | **0** |
| Broken links between runbooks | **0** |
| Eval items whose `source_url` is not cited in their runbook | **0** (1 found and fixed) |
| Steps re-opened at random and checked by hand against the cited source | **10** of 10 confirmed (seed 20261110; all happened to be Strimzi) plus **3** extra Kafka steps, all confirmed. **13 total.** |

During drafting, several sentences that went beyond the source were removed or reworded. One example: a claim that a Kafka feature upgrade is irreversible, which no fetched source states.

**Limit of the eval check:** the 180 eval items were written from the runbooks, and each item's `source_url` was checked to appear in its runbook. Whether each answer is fully supported by the runbook text was checked by reading, not mechanically.
