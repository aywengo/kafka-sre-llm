# Resolution of the audit findings

Resolves the findings in [AUDIT.md](AUDIT.md) (AUD-01 to AUD-17) and [AUDIT-2.md](AUDIT-2.md) (A-01 to A-23; A-08, A-09 and A-21 were withdrawn by that audit). Neither audit file was edited.

Every finding was checked again against the pinned sources before any change: Strimzi 1.3.1 guide and strimzi-kafka-operator tag 1.3.1 (555c3e0f07c203911bc3cb7d13ac085bfaa81b50), strimzi/drain-cleaner 70f1bc25bc14aacd15eb478768db6566bde783e3, Apache Kafka 4.4 operations docs and apache/kafka tag 4.4.0 (1156b2752aaa28eafc0b4c4d387e63356793268d). Kafka tag 4.3.2 (0a6c96d3b61167f1141df2032f25f10fd6e118fe) was read only to answer the `cordoned.log.dirs` question. All retrieved 2026-11-10. Page hashes matched sources.md.

Status values: **APPLIED** (fixed as described), **REJECTED** (the source refutes the finding; quote given, 15 words or fewer), **DEFERRED** (needs a human decision). Commits are on branch `runbooks-audit-fixes`.

| ID | Status | File(s) | Commit | Note or refuting quote |
|---|---|---|---|---|
| AUD-01 | APPLIED | runbooks/kafka/kraft-controller-membership.md, runbooks/kafka/kraft-quorum-debugging.md, coverage.md | d841456 | Static-quorum limitation first; every dynamic-quorum command marked "Apache Kafka reference, do not run on a Strimzi-managed cluster"; row set to partial |
| AUD-02 | APPLIED | runbooks/kafka/kraft-controller-membership.md, sources.md, FINDINGS.md (F20), eval/kraft-controller-membership.jsonl | d841456, 46f4617 | Update KRaft Config step added from `#update-kraft-config`; Upgrade section cited; migration conflict recorded as F20 |
| AUD-03 | APPLIED | runbooks/strimzi/cruise-control-anomaly-alerts.md, coverage.md, FINDINGS.md (F14) | 757b301, 171bd41 | Three metric names cited to `strimzi-cruise-control.json`; gap kept for alert expressions and notifier config |
| AUD-04 | APPLIED | runbooks/strimzi/drain-cleaner.md, FINDINGS.md (F21) | 89c9333, 171bd41 | README "See it in action": drain fails in default mode, retry after the roll; `--force` tagged |
| AUD-05 | APPLIED | runbooks/strimzi/topic-not-ready.md | 757b301 | Real paths under `documentation/modules/operators/` |
| AUD-06 | APPLIED | runbooks/strimzi/consumer-lag.md | 757b301 | License row and step say the YAML example is abridged, how, and the full source range |
| AUD-07 | APPLIED | runbooks/strimzi/delete-kafka-node-annotation.md, FINDINGS.md (F4) | 89c9333 | `#ref-list-of-kafka-cluster-resources-str` cited; node-pool form beside the command; F4 corrected |
| AUD-08 | APPLIED | runbooks/strimzi/cluster-recovery-from-persistent-volumes.md | 89c9333 | Warning at the top and in step 8; steps 4, 5, 11 tagged. The "Topic Operator and User Operator" condition is restored per operator: step 7 is gated on the Topic Operator and step 8 on the User Operator, because the Entity Operator can deploy "one or both operators" (`ref-kafka-entity-operator.adoc:17`) and the procedure steps are unconditional (review follow-up) |
| AUD-09 | APPLIED | runbooks/strimzi/expired-ca-certificates.md | 89c9333 | Availability sentence marked as compiler's inference; restart-event bullet reworded |
| AUD-10 | APPLIED | runbooks/strimzi/upgrade-kafka-cluster.md | 757b301 | Last sentence of the `metadataVersion` warning restored |
| AUD-11 | APPLIED | runbooks/strimzi/broker-storage-issues.md, FINDINGS.md (F22) | 757b301, 171bd41 | PV resize, broker limits and quota plugin linked |
| AUD-12 | APPLIED | runbooks/strimzi/consumer-lag.md, runbooks/strimzi/upgrade-status-and-kraft-requirement.md | 757b301 | Both marked as cross-links with a NO OFFICIAL SOURCE marker |
| AUD-13 | APPLIED | runbooks/strimzi/user-not-ready.md | 89c9333 | Tagged as a possible rolling restart, cluster-wide authorization change |
| AUD-14 | APPLIED | runbooks/strimzi/mirrormaker2-restart-connector-and-task.md | 757b301 | Both task-restart anchors added |
| AUD-15 | APPLIED; final wording DEFERRED | all 32 runbooks (License rows), ATTRIBUTION.md, eval/README.md, FINDINGS.md, coverage.md, sources.md | a9261b8 | Rows name every applicable licence per file. Strimzi docs licence: see "Open for a human decision" |
| AUD-16 | APPLIED | eval/cr-status-and-conditions.jsonl (item 3), eval/upgrade-status-and-kraft-requirement.jsonl (item 4) | 46f4617 | Each question now names its section and the number of fields |
| AUD-17 | APPLIED | eval/cr-status-and-conditions.jsonl, eval/kafka-restart-events.jsonl, eval/expired-ca-certificates.jsonl, eval/prometheus-alert-rules.jsonl; runbooks/strimzi/cr-status-and-conditions.md, runbooks/strimzi/kafka-restart-events.md | 46f4617 | Clauses added to the runbooks from the cited sections; expired-CA item split; alert-rules item trimmed, `source_url` set to the rules directory |
| A-01 | APPLIED | runbooks/strimzi/cluster-recovery-from-persistent-volumes.md, runbooks/strimzi/broker-storage-issues.md, eval/broker-storage-issues.jsonl, FINDINGS.md (F23) | 89c9333, 46f4617 | Precondition box; `con-kafka-recovering-brokers.adoc:27` routing and the ambiguity noted |
| A-02 | APPLIED | runbooks/strimzi/cluster-recovery-from-persistent-volumes.md | 89c9333 | Same change as AUD-08 |
| A-03 | APPLIED | runbooks/strimzi/expired-ca-certificates.md | 89c9333 | `openssl x509 -noout -dates` check added to Diagnosis (reuse marked as compiler's note); each CA deletion conditional on that CA having expired |
| A-04 | APPLIED (flag); secret name DEFERRED | runbooks/strimzi/expired-ca-certificates.md, FINDINGS.md (F8) | 89c9333, 171bd41 | Step 4.1 marked "cannot be completed from official docs"; the per-node secret pattern removed from the step; no name guessed |
| A-05 | APPLIED | runbooks/strimzi/delete-kafka-node-annotation.md | 89c9333 | Node-pool pod name shown; controller and ISR note labelled as compiler's note (the procedure is silent) |
| A-06 | APPLIED | runbooks/kafka/partition-reassignment-and-throttling.md | 89c9333 | `--verify` moved into step 5 before unregister; broker uncordon labelled NO OFFICIAL SOURCE FOUND (docs show it only for log directories); cordon step tagged |
| A-07 | APPLIED | runbooks/kafka/kraft-controller-membership.md | d841456 | `--release-version 4.1` kept verbatim and flagged as the docs example value |
| A-08 | REJECTED (withdrawn by AUDIT-2) | runbooks/kafka/kraft-controller-membership.md | none | kraft.md: "If the `kraft.version` field is level 0 or absent, you are using a static quorum" |
| A-09 | REJECTED (withdrawn by AUDIT-2) | runbooks/kafka/graceful-shutdown-and-leadership.md | none | basic-kafka-operations.md: "will only succeed if _all_ the partitions hosted on the broker have replicas" (runbook Diagnosis step 2 states it) |
| A-10 | APPLIED | runbooks/strimzi/upgrade-kafka-cluster.md | 757b301 | Custom-image validation warning added to step 4 |
| A-11 | APPLIED | runbooks/strimzi/topic-not-ready.md | 757b301 | Listener prerequisite dropped from Diagnosis; precondition, cleanup warning and namespace scope inside step 6 (scope labelled as compiler's note) |
| A-12 | APPLIED | runbooks/strimzi/broker-storage-issues.md | 757b301 | PV resize procedure in step 1; Kafka `#setting-quotas` and `#decommissioning-log-directories` linked as Apache Kafka reference |
| A-13 | APPLIED | runbooks/strimzi/consumer-lag.md, runbooks/strimzi/upgrade-status-and-kraft-requirement.md, runbooks/strimzi/alertmanager-and-grafana.md | 757b301 | (a) abridged block labelled; (b) two cross-links and the symptom paragraph marked NO OFFICIAL SOURCE |
| A-14 | APPLIED | runbooks/strimzi/kafka-restart-events.md, runbooks/strimzi/prometheus-alert-rules.md, runbooks/kafka/monitoring-key-metrics.md | 757b301 | Routes labelled as unsourced own cross-links; CA reasons go to a validity check first |
| A-15 | APPLIED | runbooks/strimzi/cruise-control-anomaly-alerts.md | 757b301 | Same change as AUD-03 |
| A-16 | APPLIED | runbooks/strimzi/topic-not-ready.md | 757b301 | Same change as AUD-05 |
| A-17 (MirrorMaker 2 annotations) | APPLIED | runbooks/strimzi/mirrormaker2-restart-connector-and-task.md | 757b301 | Same change as AUD-14 |
| A-17 (`my-cluster-pool-a-1`) | APPLIED | runbooks/strimzi/manual-rolling-update.md | 757b301 | Cited to `#con-upgrade-cluster-str` and `#ref-list-of-kafka-cluster-resources-str` |
| A-17 (`--field-selector`, kafka-restart-events.md:35) | REJECTED | runbooks/strimzi/kafka-restart-events.md | none | ref-operator-restart-events-fields.adoc: "you can specify a `field-selector` to filter on Kubernetes event fields" (cited section) |
| A-17 (`metadata.labels.strimzi.io/cluster`, user-not-ready.md:57) | REJECTED | runbooks/strimzi/user-not-ready.md | none | con-securing-client-labels.adoc (cited section): "labels: strimzi.io/cluster: my-cluster"; the runbook spells out that path |
| A-17 ("seven anchors lack their .adoc in the Sources row") | APPLIED in part | 19 runbooks (Sources rows) | 757b301 | The audit does not name the seven. Every pinned-source URL cited in a body is now in its Sources row (checked by `check_runbooks.py citations`); `.adoc` paths were not added for each anchor |
| A-18 | APPLIED | coverage.md | 5be6c8f (and d841456 for the KRaft row) | 15 rows relabelled partial by the mechanical rule; totals regenerated: 16 covered, 20 partial, 1 no official source |
| A-19 | APPLIED | runbooks/strimzi/node-maintenance-kubernetes-upgrade.md | 757b301 | Verification marked as a source expectation; PDB restore note labelled as compiler's note |
| A-20 | APPLIED | runbooks/strimzi/user-not-ready.md | 89c9333 | Same change as AUD-13; the possible roll is supported by the `ConfigChangeRequiresRestart` reason |
| A-21 | REJECTED (withdrawn by AUDIT-2) | runbooks/kafka/consumer-group-position.md | none | basic-kafka-operations.md: "out of range offsets will be adjusted to available offset end" (runbook Rollback, lines 75-76) |
| A-22 | APPLIED | eval/kube-state-metrics-cr-monitoring.jsonl, eval/cr-status-and-conditions.jsonl, eval/kafka-restart-events.jsonl, eval/drain-cleaner.jsonl, eval/connect-restart-connector-and-task.jsonl; three runbooks | 46f4617 | Facts added to the runbooks from the cited sources; drain-cleaner `source_url` widened to the README; connect question names the annotation |
| A-23 | APPLIED; final wording DEFERRED | as AUD-15 | a9261b8 | Same change as AUD-15 |

## Other items from the fix list

| Item | Status | File(s) | Commit |
|---|---|---|---|
| FINDINGS: static quorum conflict | APPLIED | FINDINGS.md (F20) | d841456 |
| FINDINGS: Cruise Control dashboard metrics, drain-cleaner README failure, storage procedures, GitOps mentions, ProductVersion 1.3.0, F4, F10 | APPLIED | FINDINGS.md (F14, F21, F22, F19, F2, F4, F10) | 171bd41 |
| Eval: 6-10 "does not apply on Strimzi" items | APPLIED: 8 items (static quorum, controller pool scaling, quorum commands, controller pool storage change, migration conflict, direct ZooKeeper upgrade, own-CA expiry, PV recovery on a live cluster) | eval/kraft-controller-membership.jsonl, eval/upgrade-status-and-kraft-requirement.jsonl, eval/expired-ca-certificates.jsonl, eval/cluster-recovery-from-persistent-volumes.jsonl | 46f4617 |
| Reproducible checks | APPLIED | tools/fetch_sources.sh, tools/check_runbooks.py | 39c8ab1 |

## Open for a human decision

1. **Kafka docs pin (4.3 vs 4.4).** Not decided. The pin stays at 4.4 and each Kafka runbook keeps its version note. Relevant fact: `cordoned.log.dirs` exists in Kafka 4.3.2 (`ServerLogConfigs.java:54`, validated in `DynamicBrokerConfig.scala:590`), and the 4.3.2 docs source has the same cordon step (`docs/operations/basic-kafka-operations.md:471-476`). So the cordon step is valid for Strimzi 1.3.1 clusters.
2. **Strimzi docs licence wording.** No CC BY or other licence statement was found in the published 1.3.1 guide, on https://strimzi.io/documentation/ or in strimzi-kafka-operator at tag 1.3.1. The page footer gives only "Copyright © Strimzi a Series of LF Projects, LLC" and points to lfprojects.org/policies/. The AsciiDoc source of the guide lives in that repo, whose `LICENSE` file is Apache License 2.0. The rows say "licence unconfirmed" until the owner decides.
3. **The unnamed "Kafka secret" in the expired-CA procedure** (`proc-recovering-expired-ca-certs.adoc:71-76`). No section of the guide names it. Step 4.1 is marked as not completable from official docs; the name has to come from the Strimzi maintainers.

## Final check numbers

From `python3 -I tools/check_runbooks.py --sources <cache>` on the branch head; see FINDINGS.md, "Verification", for the full table.

- Code items found in a pinned source: 905 of 971. The other 66: 40 repo paths (all exist) and 26 placeholders or elisions. Commands: 0 of 140 differ from a complete source command (133 exact; 7 inline prose mentions allowlisted with a reason in `tools/abbreviated_commands.txt`). Contiguous blocks: 130 of 131.
- Citations failing: 0 of 77 Strimzi anchors, 0 of 36 Kafka anchors, 0 of 11 GitHub paths, 0 of 2 index pages, 0 of 50 backtick repo paths (distinct URLs; 808 occurrences).
- Steps: 294; 284 with a Source line, 10 with only a NO OFFICIAL SOURCE marker, 0 with neither.
- Coverage: 37 rows, 16 covered, 20 partial, 1 no official source; Totals table matches.
- Eval: 189 items; 0 failing.
