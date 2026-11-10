# KRaft controller membership: static vs dynamic quorum, adding and removing controllers

| Field | Value |
|---|---|
| Product | Apache Kafka (KRaft) |
| Doc version | Apache Kafka 4.4 documentation, Operations > KRaft (docs source: apache/kafka tag 4.4.0, commit 1156b2752aaa28eafc0b4c4d387e63356793268d, `docs/operations/kraft.md`) |
| Sources | https://kafka.apache.org/44/operations/kraft/#static-versus-dynamic-kraft-quorums ; https://kafka.apache.org/44/operations/kraft/#describe-kraft-version ; https://kafka.apache.org/44/operations/kraft/#upgrade-kraft-version ; https://kafka.apache.org/44/operations/kraft/#add-new-controller ; https://kafka.apache.org/44/operations/kraft/#remove-controller ; https://kafka.apache.org/44/operations/kraft/#unregister-controller |
| License | Derived from the Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Version note.** These are Kafka 4.4 docs; Strimzi 1.3.1 supports Kafka up to 4.3.2. On Strimzi, node roles belong to node pools: the `KafkaNodePool` status reports the nodes in the pool and their roles (https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str). This runbook does not cover how Strimzi drives controller membership changes (NO OFFICIAL SOURCE FOUND in the fetched sections). Treat this as Apache Kafka reference material.

## Symptoms / trigger

You need to add a controller to, or remove one from, a KRaft controller quorum, or you need to know whether the quorum is static or dynamic.
— *Source:* https://kafka.apache.org/44/operations/kraft/#static-versus-dynamic-kraft-quorums (Kafka 4.4, retrieved 2026-11-10)

## Diagnosis steps

1. Find out which quorum type is in use:
   ```
   $ bin/kafka-features.sh --bootstrap-controller localhost:9093 describe
   ```
   If `kraft.version` is level 0 or missing, the quorum is **static**. If it is 1 or higher, it is **dynamic** (KIP-853). Dynamic controller clusters arrived with `kraft.version=1`, or release version 4.1.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#static-versus-dynamic-kraft-quorums ; https://kafka.apache.org/44/operations/kraft/#describe-kraft-version (Kafka 4.4, retrieved 2026-11-10)
2. Configuration differs by type:
   - Dynamic: `controller.quorum.voters` must not be set; `controller.quorum.bootstrap.servers` is set instead. It should list as many controllers as possible, much like a client's `bootstrap.servers`.
   - Static: every broker and controller configuration lists the IDs, hostnames and ports of all controllers in `controller.quorum.voters`.

   The type is fixed at format time. A quorum is formatted as dynamic when `controller.quorum.voters` is absent and one of `--standalone`, `--initial-controllers` or `--no-initial-controllers` is given.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#static-versus-dynamic-kraft-quorums (Kafka 4.4, retrieved 2026-11-10)

## Remediation steps

### Enable dynamic membership (if `kraft.version` is 0)

1. **[Feature version upgrade; NO OFFICIAL SOURCE FOUND in these sections on whether it can be reverted]** Upgrade all feature versions to the latest:
   ```
   $ bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --release-version 4.1
   ```
   or only the KRaft feature version:
   ```
   $ bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature kraft.version=1
   ```
   To migrate an existing static voter set to a dynamic one, the source points to its Upgrade section, which is not covered here.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#upgrade-kraft-version ; https://kafka.apache.org/44/operations/kraft/#static-versus-dynamic-kraft-quorums (Kafka 4.4, retrieved 2026-11-10)

### Add a controller (dynamic quorum)

2. Provision the new controller with `kafka-storage.sh` and start it.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#add-new-controller (Kafka 4.4, retrieved 2026-11-10)
3. Watch replication to it with `bin/kafka-metadata-quorum.sh describe --replication` until it has caught up with the active controller.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#add-new-controller (Kafka 4.4, retrieved 2026-11-10)
4. Add it to the quorum, through broker endpoints:
   ```
   $ bin/kafka-metadata-quorum.sh --command-config config/controller.properties --bootstrap-server localhost:9092 add-controller
   ```
   or through controller endpoints:
   ```
   $ bin/kafka-metadata-quorum.sh --command-config config/controller.properties --bootstrap-controller localhost:9093 add-controller
   ```
   `--command-config` must point to the configuration file of the controller being added. Its `controller.quorum.bootstrap.servers` must include at least one controller already in the quorum. Admin Client settings such as authentication also go in that file.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#add-new-controller (Kafka 4.4, retrieved 2026-11-10)

### Remove a controller (dynamic quorum)

5. **[Reduces quorum size]** Run `remove-controller` **before** shutting the controller down:
   ```
   $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 remove-controller --controller-id <id> --controller-directory-id <directory-id>
   ```
   or with `--bootstrap-controller localhost:9093`. To also unregister the controller, add `--unregister`:
   ```
   $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 remove-controller --controller-id <id> --controller-directory-id <directory-id> --unregister
   ```
   — *Source:* https://kafka.apache.org/44/operations/kraft/#remove-controller (Kafka 4.4, retrieved 2026-11-10)
6. To unregister a controller that was already removed from the voter set, use:
   ```
   $ bin/kafka-cluster.sh unregister-controller --bootstrap-server localhost:9092 --id <id>
   ```
   or with `--bootstrap-controller localhost:9093`. It must be removed from the voter set first, and should be shut down, because a running controller registers itself again.
   — *Source:* https://kafka.apache.org/44/operations/kraft/#unregister-controller (Kafka 4.4, retrieved 2026-11-10)

## Verification

`bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status` lists the `CurrentVoters` and `CurrentObservers`. The add and remove sections do not name it as their verification step.
— *Source:* https://kafka.apache.org/44/operations/kraft/#metadata-quorum-tool (Kafka 4.4, retrieved 2026-11-10)

## Rollback or caveats

- Remove a controller from the quorum before shutting it down.
  — *Source:* https://kafka.apache.org/44/operations/kraft/#remove-controller (Kafka 4.4, retrieved 2026-11-10)
- A running controller re-registers itself, so shut it down before unregistering.
  — *Source:* https://kafka.apache.org/44/operations/kraft/#unregister-controller (Kafka 4.4, retrieved 2026-11-10)

## Related runbooks

- [kraft-quorum-debugging.md](kraft-quorum-debugging.md)
- [../strimzi/upgrade-status-and-kraft-requirement.md](../strimzi/upgrade-status-and-kraft-requirement.md)
