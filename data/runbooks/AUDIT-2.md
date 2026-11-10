# Second independent audit of data/runbooks (PR #1)

This is a second audit, run separately from [AUDIT.md](AUDIT.md) (merged in PR #2). The addendum at the end compares the two. The audited commit is `4d7a835`; this file sits on top of `2aeb430`.

Independent audit of the Strimzi and Kafka runbooks and the eval set. I only report; no runbook or eval file was changed.

## Commits and sources audited

- Audited: `aywengo/kafka-sre-llm` PR #1 head `4d7a835121b15151c7df6a8ba5deab1b9d6a9198` (= `refs/pull/1/head` = `runbooks-official-sources`). Audit branch `audit/runbooks-pr1` starts from it. The branch name `pr1` does not exist on the remote; I used the PR head.
- Strimzi docs: https://strimzi.io/docs/operators/1.3.1/deploying, fetched 2026-11-10 (1,708,706 bytes, SHA-256 prefix `c43b40792bbb362e`).
- strimzi/strimzi-kafka-operator tag `1.3.1` = `555c3e0f07c203911bc3cb7d13ac085bfaa81b50` (matches the producer).
- strimzi/drain-cleaner `main` = `70f1bc25bc14aacd15eb478768db6566bde783e3`. Its README is byte-identical to the one at tag `1.8.0`.
- Apache Kafka 4.4 operations pages (basic-kafka-operations, kraft, monitoring, geo-replication, zk2kraft) and apache/kafka tag `4.4.0` = `1156b2752aaa28eafc0b4c4d387e63356793268d` (matches). Extra: tag `4.3.2` = `0a6c96d3b61167f1141df2032f25f10fd6e118fe`.
- Every page I fetched has the same size and SHA-256 prefix as in sources.md. This was compared only after I fetched them myself.
- Random seed for check C: **4207** (Python `random.Random(4207)`).

I worked in a fresh clone under `/tmp`, not in the producer's working tree. I did not touch pid 46091.

## Summary

- No command, flag, annotation name, default value or number in the runbooks contradicts its cited source. All 126 `kubectl`/`bin/`/`openssl`-style commands match verbatim.
- I found no blocker. I found 11 major and 12 minor findings (table below). The majors are mostly safety framing (A-01 to A-06), one stale state-changing example (A-07), unsourced routes to destructive runbooks (A-14), a wrong "no source" claim (A-15), missed official material (A-12) and an unverified licence claim (A-23).
- Producer claims I could not reproduce are listed in their own section. The main ones: "0 steps without citation", "0 missing repo paths", and the "10 of 10 + 3" hand check (no script or record in the repo).
- Verdicts: 5 pass, 27 pass-with-fixes, 0 fail. "Pass" means no finding from A, B, the sampled C steps and D. It is not proof for the steps I did not sample.

### Verdict per runbook

| Runbook | Verdict | Why |
|---|---|---|
| kafka/consumer-group-position | pass-with-fixes | Source note on out-of-range offsets dropped (minor, A-21). |
| kafka/geo-replication-mirrormaker2 | pass-with-fixes | No verification source; "covered" label (A-18). |
| kafka/graceful-shutdown-and-leadership | pass-with-fixes | Controlled-shutdown precondition (RF>1, all partitions) weakened (A-09). |
| kafka/kraft-controller-membership | pass-with-fixes | Stale `--release-version 4.1` shown as "latest" (A-07); "or missing" unsupported (A-08). |
| kafka/kraft-quorum-debugging | pass | No finding from A, B, the sampled C steps or D. |
| kafka/monitoring-key-metrics | pass-with-fixes | Runbook faithful; eval items 0 and 4 have a bare "0." as the reference answer (A-22). |
| kafka/partition-reassignment-and-throttling | pass-with-fixes | Unregister step comes before the only completion check; no uncordon on abort (A-06). |
| strimzi/alertmanager-and-grafana | pass-with-fixes | Symptom paragraph has no Source line (minor, A-13). |
| strimzi/broker-storage-issues | pass-with-fixes | Routes failed mounts to deleted-cluster recovery (A-01); omits the PV resize procedure the guide has (A-12). |
| strimzi/cluster-recovery-from-persistent-volumes | pass-with-fixes | Faithful to source, but no "deleted cluster only" precondition (A-01) and the user-deletion warning sits at the end (A-02). |
| strimzi/connect-restart-connector-and-task | pass-with-fixes | Runbook faithful; eval item 1 is ambiguous (A-22). |
| strimzi/consumer-lag | pass-with-fixes | Abridged config block, `showAllOffsets` meaning dropped, one step with no Source line (A-13). |
| strimzi/cr-status-and-conditions | pass-with-fixes | No generic remediation; "covered" label (A-18). |
| strimzi/cruise-control-anomaly-alerts | pass-with-fixes | "No official source" for metric names is wrong: the allowed repo names them (A-15). |
| strimzi/delete-kafka-node-annotation | pass-with-fixes | Pod-name pattern does not fit node pools; no controller-node warning (A-05). |
| strimzi/drain-cleaner | pass-with-fixes | Runbook faithful; eval item 0 cites a source_url that lacks the webhook/deny detail (A-22). |
| strimzi/expired-ca-certificates | pass-with-fixes | Destructive step 1 is unconditional, no expiry check (A-03); "Kafka secret" left unnamed (A-04). |
| strimzi/kafka-restart-events | pass-with-fixes | Routes CA/PodStuck reasons to destructive runbooks with no source; "covered" but remediation and verification have no source (A-14, A-18). |
| strimzi/kafkarebalance-notready-and-refresh | pass | No finding from A, B, the sampled C steps or D. |
| strimzi/kafkarebalance-tracking-and-stopping | pass | No finding from A, B, the sampled C steps or D. |
| strimzi/kube-state-metrics-cr-monitoring | pass-with-fixes | Runbook text matches the rules; eval item 2 adds "waiting for approval", which neither runbook nor rule file says (A-22). |
| strimzi/lock-acquisition-warnings | pass | No finding from A, B, the sampled C steps or D. |
| strimzi/manual-rolling-update | pass-with-fixes | Pod-name pattern vs node pools (producer F4); pool-a-1 example cited to another section (A-17). |
| strimzi/mirrormaker2-restart-connector-and-task | pass-with-fixes | Annotation names cited to the wrong section (A-17). |
| strimzi/node-maintenance-kubernetes-upgrade | pass-with-fixes | "Verification" is a source expectation, not a check; PDB left at 0 with no way back (A-19). |
| strimzi/operator-logs | pass-with-fixes | Verification has no source (A-18). |
| strimzi/prometheus-alert-rules | pass-with-fixes | Own cross-link sends UnderReplicatedPartitions to storage runbook (A-14); verification step has no source (A-18). |
| strimzi/topic-not-ready | pass-with-fixes | Create-topic prerequisites reused as a diagnosis (A-11); finalizer removal step lacks its precondition (A-11); missing path (A-16). |
| strimzi/upgrade-kafka-cluster | pass-with-fixes | Image-validation warning dropped from step 4 (A-10). |
| strimzi/upgrade-kafka-version-error | pass | No finding from A, B, the sampled C steps or D. |
| strimzi/upgrade-status-and-kraft-requirement | pass-with-fixes | One bullet with no Source line (A-13). |
| strimzi/user-not-ready | pass-with-fixes | Authorization change on `Kafka` not marked as possible broker roll (A-20). |

## Numbers

| Check | Result |
|---|---|
| A. Code spans and blocks extracted | 878 (125 blocks, 753 spans) from 32 runbooks |
| A. Found in a pinned source after whitespace normalisation | 831 |
| A. Not found | 47: 25 file paths (24 exist, 1 missing, see B), 21 spans that are placeholders, elisions or runbook-built text (listed below), and 1 block (consumer-lag.md:24, abridged) |
| A. Commands (126 starting `kubectl`, `bin/`, `$ bin/`, `openssl`, `jq`, `sed`, `curl`, `oc`) | 126 checked, 0 non-matching |
| A. Block lines, one by one | all lines found; 124 of 125 blocks found as one contiguous piece |
| A. Spans or blocks not in the cited section but on the cited page | 9 tokens (see A-17) |
| B. Distinct URLs cited (runbooks and eval) | 105; 0 anchors missing; 7 GitHub paths, all exist |
| B. Repo paths cited in backticks | 38; 1 missing (A-16) |
| B. Top-level list items in Diagnosis/Remediation/Verification/Rollback | 289 (reproduces the producer's count) |
| B. Source-cited units in those sections | 320; 2 list items and 1 symptom paragraph have neither a Source line nor NO OFFICIAL SOURCE (A-13) |
| B. Numbers and identifiers in 346 cited units vs the cited section | 0 missing (22 flags were runbook file names or its own words) |
| C. Steps in population (cited list items) | 276 (212 Strimzi, 64 Kafka) |
| C. Sampled | 12 Strimzi + 12 Kafka at random, plus 31 marked steps ([ROLLING RESTART], [RESTART], [DESTRUCTIVE], [POSSIBLY IRREVERSIBLE], [DATA MOVEMENT], [Changes ...]); 49 unique after overlap |
| C. Confirmed without gap | 38 |
| C. Confirmed with a gap (dropped condition, misplaced warning, unsupported detail) | 11 (5 of them in the random 24) |
| C. Wrong (contradicts source) | 0 |
| D. Eval items blind-answered and compared | 180 of 180 |
| D. Substantive disagreements with the reference | 0 |
| D. Partial agreement (reference holds a fact I lacked, or I held one it lacks) | 7 |
| D. Reference not fully supported by the runbook | 3 (kube-state-metrics:2, cr-status-and-conditions:5, kafka-restart-events:2) |
| D. source_url does not fully support the reference | 1 (drain-cleaner:0); lexical check on the rest, see limits |
| D. Ambiguous question | 1 (connect-restart-connector-and-task:1) |
| D. Other flags | broker-storage-issues:3 bakes in A-01; monitoring-key-metrics:0 and :4 have the bare answer "0."; no duplicate questions; 1 near-pair (cruise-control-anomaly-alerts:2 and prometheus-alert-rules:0) is distinct |

Elisions and placeholders behind the 21 span non-matches: `--describe --group <group>`; two MBean names ending in `,...`; three output messages with `...`; three PVC fields rebuilt from the example (cluster-recovery:43); `<cluster>-<pool>-<id>`; `spec.kafka.template.podDisruptionBudget.maxUnavailable: 0`; two `strimzi_<resource>_resource_info{...}` forms; `*Warnings`; `<connector_name>:<task_id>`; two `runbook_url` mentions (the runbooks say it is absent); an abridged `absent(...)`; `prometheus-kafka-exporter-topic-rules-group.yaml` (a file name); `STRIMZI_USE_FINALIZERS=true` (the source says the variable defaults to true). None is a command change.

Limits of the checks: A uses whitespace-free matching, so a short span like `true` can match anywhere. I covered that with the contiguous-block check and the cited-section check. D: the blind answers were written by a separate 27B model from question plus runbook only; I compared them to the references myself. I read the 50 lowest-overlap pairs in full and used word overlap on the other 130. Phase 2 was meant for the workers, but they exited without writing it.

## Findings

Severity: blocker = data loss or outage if followed blindly and not marked; major = misleading, unsafe framing, or a false claim; minor = small gap. Evidence quotes are 15 words or fewer.

| ID | Severity | File:line | What is wrong | Evidence | Suggested fix |
|---|---|---|---|---|---|
| A-01 | major | strimzi/cluster-recovery-from-persistent-volumes.md:13-26; strimzi/broker-storage-issues.md:35 | The procedure is for a deleted namespace or lost cluster, but has no "do not use on a live cluster" precondition. broker-storage-issues sends any "failed mount" here. Following it on a live cluster means recreating PVCs and removing `claimRef` from PVs that are in use. Eval item broker-storage-issues:3 teaches the same route. | https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach: "such as failed mounts or unavailable volumes" | Add a precondition (cluster and PVCs are gone). In broker-storage-issues say this applies only after the PVCs are lost. |
| A-02 | major | strimzi/cluster-recovery-from-persistent-volumes.md:52-57,109 | The source warning "users are deleted" is at the end, in Rollback. Steps 7-8 drop the source condition "if the Topic Operator and User Operator are deployed", and step 8 is unconditional. | https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str: "users are deleted from the Kafka cluster immediately after recovery" | Move the warning into step 8 and the top box. Restore the "if deployed" condition on steps 7-8. |
| A-03 | major | strimzi/expired-ca-certificates.md:18-40 | Diagnosis has no step to find which CA expired. Step 1 lists both `kubectl delete secret` commands one after the other. A reader can delete a CA that has not expired. | https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str: "Delete the secret containing the expired CA certificate." | Add the `openssl x509 -noout -dates` check to Diagnosis. Say "run only for the CA that expired". |
| A-04 | major | strimzi/expired-ca-certificates.md:52 | "Delete the Kafka secret" is not named, so a destructive sequence cannot be run as written. Confirms producer F8. The runbook offers the per-node secret pattern, which the source table does not tie to this step. | https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str: "Delete the Kafka secret." | Keep the gap flag, and state which secrets the maintainers confirm. Ask Strimzi, or mark the step "cannot be completed from official docs". |
| A-05 | major | strimzi/delete-kafka-node-annotation.md:22-27 | The command uses `<cluster_name>-kafka-<index_number>`. KRaft needs node pools, whose pods are `<cluster>-<pool>-<id>`. Nothing says the annotation also works on controller pods, whose PVC loss touches the quorum. Nothing says to check that other replicas are in sync first. | https://strimzi.io/docs/operators/1.3.1/deploying#proc-manual-delete-pod-pvc-kafka-str: "Kafka broker pods are named <cluster_name>-kafka-<index_number>" | Show the node-pool pod name in the command. Add: controller pods; check ISR before deleting (mark as outside the source). |
| A-06 | major | kafka/partition-reassignment-and-throttling.md:59,89,109 | Step 5 shuts down and unregisters the broker "when all reassignments are done". The only completion check (`--verify`) is in Verification, after the step. Rollback has no uncordon after step 3. | https://kafka.apache.org/44/operations/basic-kafka-operations/#decommissioning-brokers: "Once all the reassignment is done, shutdown the broker and unregister it" | Put `--verify` before step 5. Add the uncordon command (`--delete-config cordoned.log.dirs`, shown for log dirs) or flag it as unsourced for brokers. |
| A-07 | major | kafka/kraft-controller-membership.md:37-44 | "Upgrade all feature versions to the latest" is shown with `--release-version 4.1`. In the 4.4 docs this is a stale example; it is not the latest. The command changes cluster state, and the runbook carries the mismatch over. | https://kafka.apache.org/44/operations/kraft/#upgrade-kraft-version: "upgrade all of the feature versions to the latest version" | Flag `4.1` as a docs example, or use `--feature kraft.version=1` only. On Strimzi, metadataVersion is set in the `Kafka` resource. |
| A-08 | minor | kafka/kraft-controller-membership.md:24 | "level 0 or missing" means static: the source only says level 0 needs an upgrade. "or missing" has no support. | https://kafka.apache.org/44/operations/kraft/#describe-kraft-version: "is 0, the version needs to be upgraded to at least 1" | Delete "or missing". |
| A-09 | minor | kafka/graceful-shutdown-and-leadership.md:31-40,58 | Caveat is weaker than the source: the source requires every partition on the broker to have replicas (RF > 1, one alive). The runbook only says the last replica becomes unavailable. | https://kafka.apache.org/44/operations/basic-kafka-operations/#graceful-shutdown: "controlled shutdown will only succeed if all the partitions hosted on the broker have replicas" | Quote the source condition. |
| A-10 | minor | strimzi/upgrade-kafka-cluster.md:49 | Step 4 drops the warning about custom images. | https://strimzi.io/docs/operators/1.3.1/deploying#con-versions-and-images-str: "The Cluster Operator cannot validate that an image actually contains a Kafka broker" | Add the warning to step 4. |
| A-11 | minor | strimzi/topic-not-ready.md:40,61-70 | Step 4 lists the prerequisites of the create-topic example (mTLS listener) as a NotReady diagnosis. Step 6 removes finalizers on all topics in the current namespace; the source condition "Topic Operator is not running" is only implied by step 5. | https://strimzi.io/docs/operators/1.3.1/deploying#con-removing-topic-finalizers-str: "If the Topic Operator is not running, and you want to bypass the finalization process" | Drop the listener prerequisite from the diagnosis. State the precondition and the namespace scope inside step 6. |
| A-12 | major | strimzi/broker-storage-issues.md:23-40; coverage.md "partial" | The guide has a procedure the runbook does not use: resizing persistent volumes by raising `size` (the Cluster Operator then restarts the pods). Also not used: Kafka 4.4 "decommissioning-log-directories" and "setting-quotas". The remediation "free disk space or increase capacity" stays high level. | https://strimzi.io/docs/operators/1.3.1/deploying#proc-resizing-persistent-volumes-str: "Persistent volumes can be resized by changing the size storage property" | Add the resize procedure to Remediation. Link the Kafka log-directory and quota sections. |
| A-13 | minor | strimzi/consumer-lag.md:23-42,49; strimzi/upgrade-status-and-kraft-requirement.md:74; strimzi/alertmanager-and-grafana.md:13 | (a) The config block is abridged, so it is not verbatim, and the `showAllOffsets` meaning is dropped. (b) Two list items and one symptom paragraph have no Source line. | https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str: "Setting showAllOffsets to false stops collecting metrics for disconnected consumers." | Add the callout text. Add Source lines, or mark them as cross-references. |
| A-14 | major | strimzi/kafka-restart-events.md:71-73; strimzi/prometheus-alert-rules.md:29 | No source supports these routes: CA restart reasons to expired-ca-certificates (a destructive recovery runbook), PodStuck/PodUnresponsive to broker-storage-issues, UnderReplicatedPartitions to broker-storage-issues. CaCertRenewed is a normal renewal. | https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str: "CA certificates have been renewed, and the pod is restarted" | Route CA reasons to a renewal check first; mark all own cross-links as unsourced. |
| A-15 | major | strimzi/cruise-control-anomaly-alerts.md:27-37 | "NO OFFICIAL SOURCE FOUND" for anomaly-detector metric names is wrong. The allowed repo (tag 1.3.1) names them in `packaging/examples/metrics/grafana-dashboards/strimzi-cruise-control.json`. The runbook never cites that file. Producer F14 lists dashboards but misses this. | kafka_cruisecontrol_anomalydetector_balancedness_score_value (also ..._disk_failure_rate_count, ..._goal_violation_rate_count) | Cite the dashboard file. Re-label the metric names as sourced from the repo. |
| A-16 | minor | strimzi/topic-not-ready.md:7 | Cited path `documentation/modules/topic-operator/*.adoc` does not exist at 1.3.1. Topic Operator modules are in `operators/` and `deploying/`. Producer says 0 missing paths. | `ls documentation/modules` at 1.3.1: no topic-operator directory | Cite the real files. |
| A-17 | minor | strimzi/mirrormaker2-restart-connector-and-task.md:75-76; kafka-restart-events.md:35; manual-rolling-update.md:29; user-not-ready.md:57; kraft-controller-membership.md:26-31 | Tokens appear on the cited page but not in the cited section: `strimzi.io/restart-task` (in 11.8.3, cited 11.8.2), `--field-selector`, `my-cluster-pool-a-1`, `metadata.labels.strimzi.io/cluster`, `--standalone`. Seven anchors lack their .adoc in the Sources row. | strimzi.io/restart-task is in #proc-manual-restart-connector-task-str, not #proc-manual-restart-connector-str | Cite the section that holds the text. |
| A-18 | minor | coverage.md (Status column) | Legend: "covered" needs diagnosis, remediation and verification from official text. Seven "covered" rows state NO OFFICIAL SOURCE for one of these: kafka-restart-events (remediation, verification), operator-logs, prometheus-alert-rules, graceful-shutdown (verification), cr-status (remediation), topic-not-ready (one step), geo-replication (verification). Real partial count is about 12, not 5. | coverage.md: "diagnosis, remediation and verification all come from official text" | Re-label as partial. |
| A-19 | minor | strimzi/node-maintenance-kubernetes-upgrade.md:27,58-62 | The "Verification" is the source expectation "no downtime", not a check. Method B sets the PDB to 0 and the runbook has no way back. | https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-str: "so that there is no downtime" | Mark as expectation. Add a PDB-restore note as unsourced. |
| A-20 | minor | strimzi/user-not-ready.md:43 | Changing `authorization` on the `Kafka` resource is a broker config change. The runbook says the source is silent but has no [ROLLING RESTART] caution. | (source silent; could not verify) | Add a caution that a roll is possible. |
| A-21 | minor | kafka/consumer-group-position.md:55 | Source note dropped: out-of-range offsets are adjusted to the available end. | https://kafka.apache.org/44/operations/basic-kafka-operations/#managing-consumer-groups: "out of range offsets will be adjusted to available offset end" | Add the note. |
| A-22 | minor | eval/kube-state-metrics-cr-monitoring.jsonl:2; eval/cr-status-and-conditions.jsonl:5; eval/kafka-restart-events.jsonl:2; eval/drain-cleaner.jsonl:0; eval/connect-restart-connector-and-task.jsonl:1; eval/monitoring-key-metrics.jsonl:0,4 | See D section. Reference adds facts not in the runbook (approval wait; "coordinating operations"; "others applied dynamically"). drain-cleaner:0 source_url lacks the webhook/deny detail. connect:1 is ambiguous. Two references are a bare "0." | n/a | Align reference answers with runbook text; split or reword the ambiguous question. |
| A-23 | major | all 25 strimzi runbooks, line 8; eval/*.jsonl | All Strimzi runbooks say "CC BY 4.0". I found no CC BY statement in the 1.3.1 guide, in the repo at 1.3.1, or in strimzi.github.io. The repo README says Apache-2.0. Could not verify the licence claim. Eval files carry no attribution. (The audit brief itself states CC BY 4.0.) | strimzi-kafka-operator@1.3.1 README: "Strimzi is licensed under the Apache License, Version 2.0" | Owner to confirm the docs licence in writing. Add attribution to the eval files or a data/runbooks LICENSE note. |

## E. Re-derived findings and gaps

| Item | My result |
|---|---|
| F1 `runbook` search | Confirmed. `grep -rIil runbook` finds 0 files in strimzi-kafka-operator 1.3.1 and in drain-cleaner main; no file or folder named `*runbook*`. Alert rule files: 9, exactly the producer's list. Annotation keys: `summary` and `description` (8 files, 26 rules); `message` (kube-state-metrics, 28 rules). No `runbook_url`. No Cruise Control alert. All 54 rule expressions, `for` and severity in the runbook tables match the YAML. |
| F2 supported versions | Confirmed: `kafka-versions.yaml` supports 4.2.0, 4.2.1, 4.2.2, 4.3.0, 4.3.1, 4.3.2; default 4.3.2. The producer's open question is answerable: `cordoned.log.dirs` is in the Kafka 4.3.2 source (`DynamicBrokerConfig.scala`, `ServerLogConfigs.java`), so the cordon step is valid for Strimzi 1.3.1 clusters. I did not check the docs text of 4.3. |
| F3 `reserved.broker.max.id` | Confirmed. Strimzi 1.3.1 prose says `reserved.broker-max.id`, its YAML says `reserved.broker.max.id: 10000`. Kafka 4.4 zk2kraft lists it as removed. Extra evidence: no Java or Scala file in Kafka 4.3.2 or 4.4.0 defines it (only `docs/getting-started/zk2kraft.md`). So the Strimzi step likely has no effect on 4.x. Not put in any runbook (confirmed). |
| PV/PVC names (F9) | Confirmed. PVC example `volumeName: pvc-7e1f67f9-...` is the PV of `data-0-my-cluster-controller-3` in the listing; broker-0 is `pvc-5e9c5c7f-...`. The PV example is named `pvc-5ead43d1-...` (broker-2) with `claimRef` broker-0 and `selfLink` `pvc-7e226978-...`. |
| "three modes" (F11) | Confirmed. "Proposals ... in four modes" lists full, add-brokers, remove-brokers, remove-disks; later "the same across the three different modes". |
| MM2 task 0 vs 2 (F16) | Confirmed. Text says "task 0"; the command ends `MirrorSourceConnector:2`. |
| `pod-monitors/*.yaml` (F13) | Confirmed. HTML shows `pod-monitors/<strong>.yaml ... prometheus-rules/</strong>.yaml`. The .adoc lines 67-68 have `*.yaml`. The runbook uses the source form. |
| Spot-checks | F5 (duplicate `PodHasOldRevision`, `JbodVolumesChanged` not in table, `regarding.namespace`): confirmed. F6 (`STRIMZI_CERTIFICATE_WATCH_ENABLED` default false vs enabled in install files): confirmed. F7 (status not editable vs `--subresource status`): confirmed; the guide has a second `--subresource status` edit too. F12 (`CrashLookBackOff`, ProposalPending expression): confirmed. F15 (per minute vs per second): confirmed. F17 ("go to step 9"): text confirmed; I did not count the rendered steps. |
| 5 "partial" topics | Missed material: broker-storage-issues has the PV resize procedure (A-12). cruise-control-anomaly-alerts: metric names exist in the repo dashboard (A-15). lock-acquisition-warnings: "Restarting the Cluster Operator can resolve such issues" is the only text; no command exists in the guide (producer right). kraft-quorum-debugging and monitoring-key-metrics: no extra remediation in the Strimzi guide (0 hits for `kafka-metadata-quorum`); producer right. |
| Argo CD | No Argo CD content in the guide, strimzi-kafka-operator 1.3.1 or drain-cleaner (producer right). Missed GitOps mentions: the OLM no-upgrade-path procedure says to pause GitOps reconciliation (`proc-upgrade-cluster-operator-olm-no-upgrade-path.adoc`), and `STRIMZI_LABELS_EXCLUSION_PATTERN` excludes Flux labels. The `proc-pausing-reconciliation-str` procedure has no runbook. |

## F. Safety

| Step | Risk | Marked clearly? |
|---|---|---|
| cluster-recovery steps 4-5 (create PVCs with `volumeName`, delete `claimRef`) | Wrong PV in wrong PVC corrupts data; editing `claimRef` of an in-use PV | Partly. Top box says "Data-corruption risk" and example values do not match. No step marker, and no deleted-cluster precondition (A-01). |
| cluster-recovery step 8 (skip `KafkaUser` recreate) | Users deleted from Kafka right after recovery | Late: only in Rollback, marked [DESTRUCTIVE if skipped] (A-02). |
| cluster-recovery step 11 (`kubectl edit ... --subresource status`) | A wrong `clusterId` for the data on the volumes | Not marked. The runbook notes the §1.1.3 contradiction only. |
| delete-kafka-node-annotation step 1 | Permanent PVC data loss; controller nodes touch the quorum | Marked [DESTRUCTIVE] with a top banner. Pod name wrong for node pools; no controller or ISR warning (A-05). |
| expired-ca-certificates steps 1 and 4 | Deletes CA secrets and all Kafka pods; outage until recreated; clients must trust the new CA | Marked [DESTRUCTIVE] with banner. Both CA deletions unconditional (A-03); unnamed secret (A-04). Banner claim "Kafka is not available" is the runbook's inference. |
| broker-storage-issues step 4 and step 5 | Step 4 routes to deleted-cluster recovery; step 5 deletes pod and PVC | Step 5 marked [DESTRUCTIVE]. Step 4 not (A-01). |
| topic-not-ready step 6 (remove finalizers on all topics) | Kafka topics are not deleted and cleanup is skipped | Marked [DESTRUCTIVE: skips cleanup]; precondition and scope not in the step (A-11). |
| partition-reassignment steps 2, 4, 6, 7 | Heavy data movement; throttle left on slows normal replication | Marked [DATA MOVEMENT]; throttle caveat present; rollback JSON advice present. |
| partition-reassignment step 5 (unregister) | Removes the broker; irreversible if data not moved | Marked [DESTRUCTIVE]; no completion check before it (A-06). |
| kraft-controller-membership remove-controller; feature upgrade | Smaller quorum; feature level changes | Marked. Feature upgrade example stale (A-07). |
| consumer-group-position delete / reset | Group loss; offset change | Marked [DESTRUCTIVE] and [Changes consumer positions]; dry-run default explained. |
| drain-cleaner step 3 and node-maintenance (`kubectl drain --force --delete-emptydir-data`) | Pods roll; `--force` and emptyDir data removal | [ROLLING RESTART] marked. The two flags are not explained. Spare-node advice is kept. |
| node-maintenance Method B (`maxUnavailable: 0`) | PDB stays at 0; blocks other drains | Caveat present; no way back, flagged as no source (A-19). |
| upgrade-kafka-cluster steps 3 and 7 | Rolling restart; metadataVersion may block a downgrade | Both marked. Step 4 image warning dropped (A-10). |
| user-not-ready step 1 (enable `authorization` on `Kafka`) | Possible broker roll | Not marked (A-20). |
| connect / MM2 restart annotations | Connector or task restart | Marked [RESTART]. |

## G. Licence headers

- All 32 runbooks have a License row at line 8. The 7 Kafka runbooks say Apache License 2.0, © The Apache Software Foundation. This matches the licence header of the Kafka docs source files (checked in `docs/operations/basic-kafka-operations.md` and `LICENSE`).
- All 25 Strimzi runbooks say CC BY 4.0. I could not verify this from any pinned source (A-23). Five Strimzi runbooks that quote repo YAML add "Apache-2.0" for the example files; drain-cleaner.md says Apache-2.0 for its repo. Those are consistent with the repo LICENSE.
- The 32 eval files, coverage.md, sources.md and FINDINGS.md carry no per-file attribution. The eval files are derived from the runbooks.

## Producer claims I could not reproduce

| Claim | Result |
|---|---|
| FINDINGS: "Steps without a citation (or explicit NO OFFICIAL SOURCE FOUND): 0" | Not reproduced. consumer-lag.md:49 and upgrade-status-and-kraft-requirement.md:74 are list items with no Source line (cross-references). |
| FINDINGS: "Cited Strimzi repo paths missing at tag 1.3.1: 0" | Not reproduced. `documentation/modules/topic-operator/*.adoc` (topic-not-ready.md:7) does not exist. |
| FINDINGS: "Code-block lines not found verbatim: 0" | Reproduced for lines. One block (consumer-lag.md:24) is abridged and not contiguous in the source, though the runbook says "abridged". |
| FINDINGS: "10 of 10 re-opened steps confirmed (seed 20261110) plus 3 Kafka steps" | Could not reproduce. The scripts are in a scratchpad, not in the repo, and the 13 steps are not listed. My own sample (49 steps) found 11 with gaps and 0 wrong. |
| FINDINGS: 289 steps | Reproduced (top-level list items under the four h2 sections). |
| coverage.md: 31 covered / 5 partial / 1 none, eval counts per runbook | Row counts and eval counts reproduced. The "covered" labels do not follow the legend (A-18). |
| FINDINGS F2: the cordon feature may not exist in Kafka 4.3.2 | Open question answered: it exists in 4.3.2 code. |
| FINDINGS: "Eval ... answer fully supported by the runbook text was checked by reading" | 3 references are not fully supported (A-22). |
| sources.md: drain-cleaner README read at `main`, not the tag | No difference from the 1.8.0 README. |

## Missed by the producer's FINDINGS.md

- A-01, A-02, A-03, A-05, A-06, A-07, A-14: safety framing and routing problems.
- A-12 (PV resize and Kafka log-directory/quota sections unused) and A-15 (Cruise Control metric names exist in the allowed repo).
- A-16, A-17, A-18, A-23.
- Gap list: no runbooks for `proc-pausing-reconciliation-str`, CA force-renew and key replacement (`proc-renewing-ca-certs-manually-str`, `proc-replacing-private-keys-str`), node-pool scale up and down (§11.3.3-11.3.4), Cruise Control JBOD disk moves (§23), Kafka topic create/modify and quotas.
- GitOps mentions in the guide (see Argo CD row).

## Not verified

- Whether the 1.3.1 guide text and the AsciiDoc source differ in places other than `pod-monitors/*.yaml` (I compared only the cited ones).
- Strimzi docs licence (A-23).
- Kafka 4.3 docs text (only the 4.3.2 source tree was checked).
- Steps outside the 49 sampled were checked only by the mechanical A and B checks.

## Addendum: after comparing with the producer-branch AUDIT.md

Written after my audit was committed (`7fed6eb`). I read the other audit (`2aeb430`, file `data/runbooks/AUDIT.md`) only after finishing mine, and re-checked each of its findings that I had not made.

I missed these. Each is confirmed against the pinned sources:

| Item | My check |
|---|---|
| Strimzi uses static controller quorums only (their AUD-01, blocker) | Confirmed. Guide §2.1.2: "Strimzi uses static controller quorums for all deployments". The runbook kraft-controller-membership.md gives dynamic-quorum commands and says Strimzi handling has "NO OFFICIAL SOURCE FOUND". I listed the headings `kraft_limitations` and `controller_scaling` but did not read them. This makes my A-07 and A-08 secondary: the runbook should be rated **fail** (blocker) until it says these commands do not apply on Strimzi. |
| Static-to-dynamic migration is on the cited KRaft page (their AUD-02) | Confirmed. `#upgrade` and `#update-kraft-config` exist on the 4.4 page; the runbook and sources.md say that section is not covered. Note the conflict: Strimzi says Kafka does not support the migration, Kafka 4.4 says 4.1 added it. |
| Drain Cleaner README: the drain "eventually fails" in default mode (their AUD-04) | Confirmed, README line 208. The runbook step 3 omits it. |
| Node-pool pod name is defined in the guide (their AUD-07) | Confirmed: `<kafka_cluster_name>-<pool_name>-<pod_id>` in §7.2.4. Producer F4 is wrong to say it is not stated. |
| Last sentence of the `metadataVersion` warning is dropped (their AUD-10) | Confirmed (I saw it and did not list it). |
| `documentation/shared/attributes.adoc` has `ProductVersion: 1.3.0` at tag 1.3.1 | Confirmed. |

Not re-checked: their findings on licence rows of mixed-source runbooks (AUD-15) and eval items (AUD-16, AUD-17).
