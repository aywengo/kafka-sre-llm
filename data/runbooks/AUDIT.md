# Independent audit of the Strimzi/Kafka runbooks and eval set (PR #1)

Audit date: 2026-11-10. Nothing under `runbooks/`, `eval/`, `coverage.md`, `sources.md` or `FINDINGS.md` was edited. No process on the training host was touched; the audit ran in a separate clone.

## Verdict

- **1 runbook fails**: `kafka/kraft-controller-membership.md`. It tells a Strimzi operator how to switch to a dynamic controller quorum and add or remove controllers, while the Strimzi 1.3.1 guide says Strimzi uses static quorums for every deployment. The producer reported "no official source" for this.
- **13 runbooks pass with fixes**, 18 pass.
- **Commands are sound**: every `kubectl` / `bin/` command and all but one code block match a source verbatim.
- **All 403 citations resolve**, on the live pages as well as in the source repos.
- **The eval set is internally consistent** (0 disagreements in 180), but it inherits the runbooks' gaps and cannot detect them.
- **Three producer verification claims did not reproduce**, and FINDINGS.md missed two things that matter: the Strimzi static-quorum statement and the Cruise Control anomaly metric names in the Strimzi repo.

## What was audited, and from where

| Item | Value |
|---|---|
| Audited commit | `4d7a835121b15151c7df6a8ba5deab1b9d6a9198` (`refs/pull/1/head`; the remote branch is `runbooks-official-sources`, there is no remote branch named `pr1`) |
| Audit branch | `audit-runbooks-pr1`, created from that commit |
| Random seed (check C) | `48213` (Python `random.seed`, `random.sample` over cited step units) |
| strimzi/strimzi-kafka-operator | tag `1.3.1` = `555c3e0f07c203911bc3cb7d13ac085bfaa81b50` (confirmed) |
| strimzi/drain-cleaner | `main` = `70f1bc25bc14aacd15eb478768db6566bde783e3` |
| apache/kafka | tag `4.4.0` = `1156b2752aaa28eafc0b4c4d387e63356793268d` |
| apache/kafka-site | `cd727c4dd8664857751a87cd0442e51a3d3aca8f`, `content/en/44/` (the four operations files are byte-identical to the 4.4.0 tag) |
| strimzi/strimzi.github.io | `37c5b215f33cd5699b1dbb108f6f0d0fc2dcda52`, `docs/operators/1.3.1/deploying-book.html` (body of the published guide) |
| Live pages | Fetched in a browser on 2026-11-10: the Strimzi 1.3.1 deploying guide and the four Kafka 4.4 operations pages (basic operations, KRaft, monitoring, geo-replication). All returned HTTP 200. |

Comparison with the producer's `sources.md`, done only at the end: the SHA-256 prefix and byte size of all five live pages I fetched match the producer's values (`c43b40792bbb362e`, `554551ef7d388cd4`, `6ec83b8f87fd0b8c`, `9b4ce983f19d132b`, `e7bfc284a96658c7`). The other five URLs in `sources.md` (`latest/deploying`, `latest/full/deploying.html`, the operations index, `datacenters/`, `zk2kraft/`) were **not fetched live: could not verify** their hashes. The `zk2kraft` content was checked in the 4.4.0 tag instead.

## Numbers

| Check | Checked | Result |
|---|---|---|
| A. Code blocks and code spans | 878 (125 blocks, 753 spans) | 48 not found in any source: 22 are repo-path references (21 exist, **1 does not**), 1 abridged block, 25 abridged or self-composed spans. `kubectl` / `bin/` commands not verbatim: **0**. |
| A. Code items inside the section the step cites | 878 | 34 are not in the cited section: 19 found nowhere (the abridged/composed ones above), 15 found only in a different section or file |
| B. Citation URLs | 403 occurrences, 108 distinct | 0 missing anchors or paths, in the repos and on the live pages (68 Strimzi anchors, 30 Kafka anchors) |
| B. Steps with a source line | 289 top-level list items in Diagnosis / Remediation / Verification / Rollback sections (91 / 121 / 15 / 62), which reproduces the producer's count; 359 source lines in total | 2 list steps have neither a citation nor a NO OFFICIAL SOURCE marker |
| C. Random sample | 24 steps (12 Strimzi, 12 Kafka) | 23 confirmed, **1 wrong** |
| C. Marked steps not already in the sample | 21 units (all `[ROLLING RESTART]`, `[RESTART]`, `[DESTRUCTIVE]`, `[DATA MOVEMENT]`, `[POSSIBLY IRREVERSIBLE]`) | 20 confirmed, 1 drops a source caveat |
| D. Eval items | 180 | **0 disagreements** between my blind answer and the reference; 9 items flagged for other reasons; 3 items were not blind |
| Alert tables vs rule YAML | 54 alerts (26 `prometheus-install`, 28 kube-state-metrics) | 0 mismatches in name, expression, `for` or severity |
| Metrics table vs Kafka monitoring page | 13 rows | 0 mismatches |
| G. License row present and of the right type | 32 runbooks | 32 present; 4 incomplete for mixed sources; 0 of 35 other files carry attribution |

Limits of check D: three `drain-cleaner` references (items 1 to 3) were on screen before I answered, so they are not blind. The eval was written from the runbooks, so agreement shows the runbook supports the answer; it does not show the answer is right. Item 2 of `kraft-controller-membership` agrees with the runbook and is incomplete against the source.

## Per-runbook verdicts

| Runbook | Verdict | Why |
|---|---|---|
| kafka/kraft-controller-membership | **fail** | AUD-01, AUD-02 |
| kafka/kraft-quorum-debugging | pass-with-fixes | Needs the Strimzi static-quorum note (AUD-01); mixed-license header (AUD-15) |
| kafka/partition-reassignment-and-throttling | pass | Faithful; cordon step correctly flagged as 4.4-only |
| kafka/graceful-shutdown-and-leadership | pass | Faithful |
| kafka/consumer-group-position | pass | Faithful; dry-run default stated |
| kafka/geo-replication-mirrormaker2 | pass | Faithful |
| kafka/monitoring-key-metrics | pass | Table matches source row for row |
| strimzi/cruise-control-anomaly-alerts | pass-with-fixes | AUD-03 |
| strimzi/drain-cleaner | pass-with-fixes | AUD-04 |
| strimzi/cluster-recovery-from-persistent-volumes | pass-with-fixes | AUD-08 |
| strimzi/expired-ca-certificates | pass-with-fixes | AUD-09 |
| strimzi/delete-kafka-node-annotation | pass-with-fixes | AUD-07 |
| strimzi/broker-storage-issues | pass-with-fixes | AUD-11 |
| strimzi/topic-not-ready | pass-with-fixes | AUD-05 |
| strimzi/consumer-lag | pass-with-fixes | AUD-06, AUD-12 |
| strimzi/upgrade-status-and-kraft-requirement | pass-with-fixes | AUD-12 |
| strimzi/upgrade-kafka-cluster | pass-with-fixes | AUD-10 |
| strimzi/user-not-ready | pass-with-fixes | AUD-13 |
| strimzi/mirrormaker2-restart-connector-and-task | pass-with-fixes | AUD-14 |
| strimzi/prometheus-alert-rules, kube-state-metrics-cr-monitoring, alertmanager-and-grafana, connect-restart-connector-and-task | pass | Content matches the rule files and cited sections. `alertmanager-and-grafana` has an incomplete license row (AUD-15) |
| strimzi/manual-rolling-update, node-maintenance-kubernetes-upgrade, upgrade-kafka-version-error, kafka-restart-events, cr-status-and-conditions, operator-logs, lock-acquisition-warnings, kafkarebalance-notready-and-refresh, kafkarebalance-tracking-and-stopping | pass | Faithful to the cited sections |

## Findings

Quotes are exact and at most 15 words. Paths without a host are in the repos at the commits above.

| ID | Severity | File:line | What is wrong | Evidence | Suggested fix |
|---|---|---|---|---|---|
| AUD-01 | blocker | `runbooks/kafka/kraft-controller-membership.md:11`, `:35-80`; `coverage.md` row "KRaft management: controller membership" | The version note says there is NO OFFICIAL SOURCE for how Strimzi handles controller membership. The Strimzi 1.3.1 guide, §2.1.1 and §2.1.2, covers it: static quorums only, and no scaling of controller pools. The runbook then gives a `kraft.version` upgrade and add/remove-controller commands with no statement that they do not apply on Strimzi. | https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums : "Strimzi uses static controller quorums for all deployments, including new installations" | Put the Strimzi limitation at the top, mark the remediation section as Apache Kafka reference that must not be run on a Strimzi-managed cluster, cite `#controller_scaling` and `#limitations_with_static_controller_quorums`, and set the coverage row to partial. |
| AUD-02 | major | `runbooks/kafka/kraft-controller-membership.md:37-46`; `sources.md:39`; `eval/kraft-controller-membership.jsonl` item 2 | The runbook says the static-to-dynamic migration is in an "Upgrade section, which is not covered here", and `sources.md` says that section is outside the fetched pages. It is on the cited KRaft page (`#upgrade`, `#update-kraft-config`, both present live). The step after the feature upgrade, changing the quorum properties on every node, is missing, so the procedure is incomplete. The eval reference repeats the incomplete answer. | `docs/operations/kraft.md` (4.4.0), "Update KRaft Config": "remove the `controller.quorum.voters` property and add the `controller.quorum.bootstrap.servers`" | Add the config step with its citation, correct `sources.md`, and extend the eval reference. |
| AUD-03 | major | `runbooks/strimzi/cruise-control-anomaly-alerts.md:11`, `:35`; `coverage.md` row "Anomaly-detection alerts" | "NO OFFICIAL SOURCE FOUND (within the allowed sources) for anomaly-detector metric names" is not true. The Strimzi example Cruise Control dashboard, in the allowed repo, uses three anomaly-detector metrics. | `packaging/examples/metrics/grafana-dashboards/strimzi-cruise-control.json` (1.3.1): "kafka_cruisecontrol_anomalydetector_goal_violation_rate_count" | List the three metric names (`…balancedness_score_value`, `…disk_failure_rate_count`, `…goal_violation_rate_count`) with the dashboard as source. Keep the gap statement for alert expressions and notifier config. |
| AUD-04 | major | `runbooks/strimzi/drain-cleaner.md:63-68` | The drain step implies the command completes. The README, cited elsewhere in the same runbook, says that in the default mode the drain evicts what it can and then fails, because the Kafka evictions are denied. The step also uses `--force` with only a `[ROLLING RESTART]` tag. | `README.md` (drain-cleaner 70f1bc2), line 208: "eventually fail because Drain Cleaner denied the eviction" | Add the expected failure and what to do next, as the README describes, and tag `--force`. |
| AUD-05 | minor | `runbooks/strimzi/topic-not-ready.md:7` | Cited repo path `documentation/modules/topic-operator/*.adoc` matches no file at tag 1.3.1. The Topic Operator modules are under `documentation/modules/operators/`. | `ls documentation/modules` at 555c3e0: no `topic-operator` directory | Replace with the real paths, for example `modules/operators/proc-configuring-kafka-topic.adoc`. |
| AUD-06 | minor | `runbooks/strimzi/consumer-lag.md:24-40` | The only code block that is not verbatim: lines are cut, `# ...` is inserted and callouts `<1>` are rewritten as `(1)`. The header says commands are quoted verbatim. It is labelled "abridged", so nothing is hidden. | `documentation/modules/metrics/proc-kafka-exporter-deploy.adoc:39-58` | Quote the block in full, or state in the header that YAML examples may be abridged. |
| AUD-07 | minor | `runbooks/strimzi/delete-kafka-node-annotation.md:22`; `FINDINGS.md` F4 | The runbook says other sections of the guide name node-pool pods `<cluster>-<pool>-<id>`, but cites no section for it and shortens the placeholders. The guide does define the pattern, so the claim is supported; only the citation is missing. F4 is wrong to say the guide does not state the node-pool pod name. The destructive command still uses the non-node-pool pattern only. | https://strimzi.io/docs/operators/1.3.1/deploying#ref-list-of-kafka-cluster-resources-str (`documentation/modules/configuring/ref-list-of-kafka-cluster-resources.adoc:48`): "`<kafka_cluster_name>-<pool_name>-<pod_id>`" | Cite that section, use its placeholder names, show the node-pool form next to the command, and correct F4. |
| AUD-08 | minor | `runbooks/strimzi/cluster-recovery-from-persistent-volumes.md:52`, `:57`, `:80-83`, `:109` | Three small departures. The source's opening WARNING about user deletion sits last, under caveats. The condition "if the Topic Operator and User Operator are deployed" is dropped from steps 7 and 8. Step 11 (editing `.status.clusterId`) and step 5 (deleting `claimRef`) carry no risk tag. | `documentation/modules/managing/proc-cluster-recovery-volume.adoc` (WARNING): "users are deleted from the Kafka cluster immediately after recovery" | Move the warning next to the banner, restore the condition, tag steps 5 and 11. |
| AUD-09 | minor | `runbooks/strimzi/expired-ca-certificates.md:11`, `:73` | The banner says Kafka is unavailable until pods are recreated; the cited section does not say this (it is a fair inference, but unsupported). The verification bullet about `CaCertRenewed` restart events concerns operator-initiated restarts, while this procedure deletes pods by hand. | `documentation/modules/security/proc-recovering-expired-ca-certs.adoc`: no availability statement | Mark the banner sentence as the compiler's inference, and drop or reword the restart-event bullet. |
| AUD-10 | minor | `runbooks/strimzi/upgrade-kafka-cluster.md:86` | The last sentence of the source WARNING on `metadataVersion` is dropped. | `documentation/modules/upgrading/proc-upgrade-kafka-kraft.adoc`: "understand the potential implications on support and compatibility when maintaining an older version" | Add it. |
| AUD-11 | minor | `runbooks/strimzi/broker-storage-issues.md:23`, `:29-33` | "Increase the available storage capacity" and the quota step are left without procedures. The same guide has them, and the source section links two of them as additional resources. | Anchors `#proc-resizing-persistent-volumes-str`, `#proc-setting-broker-limits-str`, `#con-choosing-a-quota-plugin-str` exist on the live page | Link them from steps 1 and 3. |
| AUD-12 | minor | `runbooks/strimzi/consumer-lag.md:49`; `runbooks/strimzi/upgrade-status-and-kraft-requirement.md:74` | Two list steps have neither a source line nor a NO OFFICIAL SOURCE marker (both are cross-links). | n/a | Mark them as cross-links outside the numbered steps, or cite them. |
| AUD-13 | minor | `runbooks/strimzi/user-not-ready.md:43-56` | Enabling `authorization.type: simple` on a running cluster is given with no risk tag. The runbook does note that the source is silent on a rolling update. | `documentation/modules/operators/proc-configuring-kafka-user.adoc` gives the YAML only | Tag the step as a cluster-wide authorization change. |
| AUD-14 | minor | `runbooks/strimzi/mirrormaker2-restart-connector-and-task.md:75-76` | The caveat names `strimzi.io/restart-connector-task` and `strimzi.io/restart-task`, but the two sections that define them are not in its source line. | Annotations appear only under `#proc-manual-restart-mirrormaker2-connector-task-str` and `#proc-manual-restart-connector-task-str` | Add both anchors. |
| AUD-15 | minor | License rows: `strimzi/alertmanager-and-grafana.md`, `strimzi/cruise-control-anomaly-alerts.md`, `kafka/kraft-quorum-debugging.md`, `kafka/kraft-controller-membership.md`; all of `eval/*.jsonl`, `FINDINGS.md`, `coverage.md`, `sources.md` | The two Strimzi runbooks cite files from the Strimzi repo examples (Apache-2.0) but name only CC BY 4.0. The two Kafka runbooks also cite Strimzi material (a rule file, a guide section) but name only the Kafka license. The 180 eval items and three top-level files are derived from the same docs and carry no attribution. Five other runbooks that use example files or the drain-cleaner README already name both licenses. | `LICENSE` in both Strimzi repos is Apache-2.0 | Name every source license in these four files, and add a `NOTICE` or `eval/README` covering the JSONL files. |
| AUD-16 | minor | `eval/cr-status-and-conditions.jsonl` item 3 vs `eval/upgrade-status-and-kraft-requirement.jsonl` item 4 | Near-duplicate questions with different answers (two status fields vs three). | Both references are supported by their own runbook | Merge, or make each question name its section. |
| AUD-17 | minor | `eval/cr-status-and-conditions.jsonl` item 6; `eval/kafka-restart-events.jsonl` item 3; `eval/expired-ca-certificates.jsonl` item 6; `eval/prometheus-alert-rules.jsonl` item 1 | Items 6 and 3 include a clause that is in the source but not in the runbook. In the other two, the second sentence is supported by a different URL than `source_url` (`#cluster_ca_secrets`, and the kube-state-metrics rules file). | Runbook text and `source_url` sections compared | Trim the clause or add it to the runbook; widen `source_url` or split the item. |

## Check E: producer findings re-derived

| Finding | Result |
|---|---|
| F1 `runbook` search | **Confirmed.** 0 matches in all files of strimzi-kafka-operator 1.3.1 and drain-cleaner main, and no file or folder named `*runbook*`. Nine rule files under `packaging/examples/metrics/` (the same nine are duplicated under top-level `examples/metrics/`). Annotation keys: `summary` and `description` in 26 alerts, `message` in 28 kube-state-metrics alerts; the only label is `severity`. |
| F2 supported Kafka versions | **Confirmed** from `kafka-versions.yaml`: 4.2.0, 4.2.1, 4.2.2, 4.3.0, 4.3.1, 4.3.2 (default). 4.4 is not supported. |
| F3 `reserved.broker.max.id` | **Confirmed.** Strimzi prose spells it `reserved.broker-max.id`, the YAML has `reserved.broker.max.id: 10000`, and the Kafka 4.4.0 `zk2kraft.md` lists it among removed configurations. |
| F4 pod naming with node pools | **Partly refuted.** The inconsistency between sections is real, but the guide does state the node-pool pod name, in its list of Kafka cluster resources (AUD-07). |
| F9 PV/PVC names | **Confirmed.** PVC `data-0-my-cluster-broker-0` has `volumeName: pvc-7e1f67f9-…`, which the `kubectl get pv` output assigns to `controller-3`; the example PV is `pvc-5ead43d1-…` with a `selfLink` ending `pvc-7e226978-…`. |
| F11 three vs four modes | **Confirmed.** Four modes are defined, then the text says "three different modes" (`con-cruise-control-overview.adoc:269`). |
| F13 lost `*` | **Confirmed** on the live page: the command block reads `pod-monitors/.yaml`. The `.adoc` has `pod-monitors/*.yaml`. |
| F16 task 0 vs task 2 | **Confirmed** (`proc-manual-restart-mirrormaker2-connector-task.adoc:41` and `:45`). |
| Spot checks: F5, F6, F12, F14, F15, F17, F18 | **All confirmed.** F18 was checked on the live pages: en dashes in prose, `([-.w]+)` six times on the geo-replication page, and the broker metrics table sitting under the JMX-security heading. One correction to F10: `oc get kafkatopics` appears in three modules, not one. |
| Argo CD (the "no official source" topic) | **Confirmed**: no occurrence of "Argo" in the guide or in either repo's docs. The producer missed that the guide does mention GitOps twice, in the OLM upgrade-without-upgrade-path procedure (pause reconciliation, then update the `Subscription` in Git). Not an Argo CD procedure, but it is the only GitOps guidance in the allowed sources. |
| Five "partial" topics | Lock warnings, quorum debugging and key metrics: nothing further found. Broker storage: see AUD-11. Cruise Control anomaly alerts: see AUD-03. |

## Check F: steps that can cause data loss, an outage or an unrecoverable change

| Runbook: step | Risk | Marked clearly? |
|---|---|---|
| delete-kafka-node-annotation: remediation 1 | Deletes a PVC; permanent data loss | Yes (banner and step). The command shows only the non-node-pool pod name; the node-pool form is documented but not cited (AUD-07) |
| broker-storage-issues: step 5 | Same, by reference | Yes |
| expired-ca-certificates: steps 1 and 4 | Deletes CA secrets, the Kafka secret and all Kafka pods | Yes. Which "Kafka secret" is meant stays ambiguous, as the producer reported (F8) |
| cluster-recovery: steps 4 and 5 | Wrong PV-to-PVC mapping corrupts data | Banner only; steps untagged (AUD-08) |
| cluster-recovery: step 11 | Wrong `clusterId` written to status | **No** (AUD-08) |
| cluster-recovery: skipping step 8 | Users deleted after recovery | Yes, but at the bottom (AUD-08) |
| topic-not-ready: step 6 | Removes finalizers on every `KafkaTopic` in the namespace; cleanup skipped | Yes |
| user-not-ready: remediation 1 | Cluster-wide authorization change | **No** (AUD-13) |
| drain-cleaner: step 3 | `kubectl drain … --force` | Partly: restart tag only (AUD-04) |
| manual-rolling-update, node-maintenance, upgrade-kafka-cluster, upgrade-kafka-version-error | Rolling restarts; `metadataVersion` raise blocks downgrade | Yes |
| kafkarebalance-notready-and-refresh: approve | Starts data movement | Yes, in bold text, without a bracket tag |
| kafkarebalance-tracking-and-stopping: stop | Leaves cluster in an intermediate state | Yes |
| partition-reassignment: `--execute`, `unregister` | Data movement; broker removed from the cluster | Yes. The cordon step (config change) is untagged |
| consumer-group-position: delete, reset offsets | Group deleted; consumer positions changed | Yes |
| kraft-controller-membership: feature upgrade, `remove-controller`, `unregister-controller` | Quorum change; not valid on Strimzi | **No** for Strimzi (AUD-01); `unregister-controller` untagged |
| graceful-shutdown, geo-replication (`kill`), connector and task restarts | Restarts | Yes, except the MirrorMaker `kill` step |

## Producer claims I could not reproduce

| Claim (where) | What I found |
|---|---|
| "Steps without a citation (or an explicit NO OFFICIAL SOURCE FOUND): 0" | 2 found (AUD-12). |
| "Cited Strimzi repo paths missing at tag 1.3.1: 0" | 1 found (AUD-05). |
| "Code-block lines not found verbatim …: 0" | 1 block is altered (AUD-06). |
| "Apache Kafka 'Upgrade' section … outside the four requested Kafka operations pages" (sources.md) | False; it is on the KRaft page (AUD-02). |
| "NO OFFICIAL SOURCE FOUND" for Strimzi controller membership and for anomaly-detector metric names | Both exist in the allowed sources (AUD-01, AUD-03). |
| Strimzi docs are CC BY 4.0 | **Could not verify.** The published guide carries no license text, and the operator repo's `LICENSE` is Apache-2.0. Confirm the docs license at its source before release. |
| Hashes of the five pages listed above as not fetched | Could not verify. |
| "10 of 10 confirmed (seed 20261110)" | Not re-run; the producer's sampling script is not in the repo. My own sample found 1 wrong step in 24. |

## Found here, missed by FINDINGS.md

1. **Strimzi static-quorum limitation** (AUD-01). This also adds a second cross-source conflict of the F3 kind: the Strimzi 1.3.1 guide says migration between static and dynamic quorums is not supported by Apache Kafka, while the Kafka 4.4 KRaft page says 4.1 added that upgrade.
2. **Anomaly-detector metric names in the Strimzi Cruise Control dashboard** (AUD-03).
3. **Drain Cleaner README: the drain is expected to fail in default mode** (AUD-04).
4. **GitOps is mentioned in the guide** (OLM procedure), relevant to the Argo CD gap.
5. **Storage procedures in the same guide** that the broker-storage runbook does not link (AUD-11).
6. **`documentation/shared/attributes.adoc` at tag 1.3.1 sets `ProductVersion: 1.3.0`**, while the published page says 1.3.1 throughout. Version strings should be taken from the published page, not from that attribute.
7. **The eval has no item that tests a "does not apply on Strimzi" answer.** Given AUD-01, a model trained on these files would learn Kafka controller commands with no Strimzi guard.
