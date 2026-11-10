# KRaft controller membership: static vs dynamic quorum, adding and removing controllers

| Field | Value |
|---|---|
| Product | Apache Kafka (KRaft); Strimzi 1.3.1 limitation |
| Doc version | Apache Kafka 4.3 documentation, Operations > KRaft (docs source: apache/kafka tag 4.3.2, commit 0a6c96d3b61167f1141df2032f25f10fd6e118fe, `docs/operations/kraft.md`). Strimzi 1.3.1 "Deploying and Managing Strimzi", §2.1 KRaft limitations (repo tag 1.3.1, commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50, `documentation/assemblies/deploying/assembly-kraft-mode.adoc`) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums ; https://strimzi.io/docs/operators/1.3.1/deploying#controller_scaling ; https://kafka.apache.org/43/operations/kraft/#static-versus-dynamic-kraft-quorums ; https://kafka.apache.org/43/operations/kraft/#upgrade ; https://kafka.apache.org/43/operations/kraft/#describe-kraft-version ; https://kafka.apache.org/43/operations/kraft/#upgrade-kraft-version ; https://kafka.apache.org/43/operations/kraft/#update-kraft-config ; https://kafka.apache.org/43/operations/kraft/#add-new-controller ; https://kafka.apache.org/43/operations/kraft/#remove-controller ; https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str ; https://kafka.apache.org/43/operations/kraft/#metadata-quorum-tool ; https://kafka.apache.org/43/operations/hardware-and-os/#replace-kraft-controller-disk |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Also derived from: Apache Kafka documentation, © The Apache Software Foundation, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Strimzi limitation: read this first.** Strimzi uses static controller quorums for all deployments, including new installations. Existing KRaft clusters with static quorums must keep using them, and Strimzi does the same for compatibility. Because of this, dynamic scaling of the controller quorum cannot be used on Strimzi for:
> - adding or removing node pools with controller roles;
> - adding the controller role to an existing node pool, or removing it from one;
> - scaling a node pool with the controller role;
> - renaming a node pool with the controller role.
>
> Changing the storage type of a node pool with a controller role is also not possible, because it involves scaling the controller quorum. Strimzi says it will assess dynamic quorums once migration is possible.
> — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums ; https://strimzi.io/docs/operators/1.3.1/deploying#controller_scaling (Strimzi 1.3.1, retrieved 2026-11-10)
>
> **Every remediation command below is Apache Kafka reference. Do not run it on a Strimzi-managed cluster.** The two sources disagree on migration: Strimzi 1.3.1 says Apache Kafka does not currently support migration between static and dynamic quorums, while the Kafka 4.3 docs say Kafka 4.1 added the upgrade from static to dynamic (see FINDINGS.md, F20).

> **Version note.** These are the Kafka 4.3 docs (source at tag 4.3.2); Strimzi 1.3.1 supports Kafka 4.2.0 to 4.3.2. On Strimzi, node roles belong to node pools: the `KafkaNodePool` status reports the nodes in the pool and their roles (https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str).

## Symptoms / trigger

You need to add a controller to, or remove one from, a KRaft controller quorum, or you need to know whether the quorum is static or dynamic. On Strimzi 1.3.1 the quorum is static (see the box above).
— *Source:* https://kafka.apache.org/43/operations/kraft/#static-versus-dynamic-kraft-quorums (Kafka 4.3, retrieved 2026-11-10); https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Find out which quorum type is in use:
   ```
   $ bin/kafka-features.sh --bootstrap-controller localhost:9093 describe
   ```
   If `kraft.version` is level 0 or missing, the quorum is **static**. If it is 1 or higher, it is **dynamic** (KIP-853). Dynamic controller clusters arrived with `kraft.version=1`, or release version 4.1.
   — *Source:* https://kafka.apache.org/43/operations/kraft/#static-versus-dynamic-kraft-quorums ; https://kafka.apache.org/43/operations/kraft/#describe-kraft-version (Kafka 4.3, retrieved 2026-11-10)
2. Configuration differs by type:
   - Dynamic: `controller.quorum.voters` must not be set; `controller.quorum.bootstrap.servers` is set instead. It should list as many controllers as possible, much like a client's `bootstrap.servers`.
   - Static: every broker and controller configuration lists the IDs, hostnames and ports of all controllers in `controller.quorum.voters`.

   The type is fixed at format time. A quorum is formatted as dynamic when `controller.quorum.voters` is absent and one of `--standalone`, `--initial-controllers` or `--no-initial-controllers` is given.
   — *Source:* https://kafka.apache.org/43/operations/kraft/#static-versus-dynamic-kraft-quorums (Kafka 4.3, retrieved 2026-11-10)

## Remediation steps

**Apache Kafka reference, do not run on a Strimzi-managed cluster.** Strimzi 1.3.1 uses static controller quorums only (https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums, Strimzi 1.3.1, retrieved 2026-11-10).

### Migrate a static quorum to a dynamic one (if `kraft.version` is 0)

Kafka 4.1 added the upgrade from a static to a dynamic controller configuration. It has two parts: upgrade the KRaft feature version, then update the configuration of the nodes.
— *Source:* https://kafka.apache.org/43/operations/kraft/#upgrade (Kafka 4.3, retrieved 2026-11-10)

1. **[Apache Kafka reference, do not run on a Strimzi-managed cluster] [Feature version upgrade; NO OFFICIAL SOURCE FOUND in these sections on whether it can be reverted]** Upgrade all feature versions to the latest. The docs show this with `--release-version 4.1`. Treat `4.1` as the docs example value, not as the latest release:
   ```
   $ bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --release-version 4.1
   ```
   or only the KRaft feature version:
   ```
   $ bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature kraft.version=1
   ```
   — *Source:* https://kafka.apache.org/43/operations/kraft/#upgrade-kraft-version (Kafka 4.3, retrieved 2026-11-10)
2. **[Apache Kafka reference, do not run on a Strimzi-managed cluster] [Configuration change on every node]** Check that the KRaft version is now at least `1`. Then, on all nodes (controllers and brokers), remove `controller.quorum.voters` and add `controller.quorum.bootstrap.servers`. KRaft version 1 deprecated `controller.quorum.voters`. The docs example:
   ```properties
   process.roles=...
   node.id=...
   controller.quorum.bootstrap.servers=controller1.example.com:9093,controller2.example.com:9093,controller3.example.com:9093
   controller.listener.names=CONTROLLER
   ```
   — *Source:* https://kafka.apache.org/43/operations/kraft/#update-kraft-config (Kafka 4.3, retrieved 2026-11-10)

### Add a controller (dynamic quorum)

3. **[Apache Kafka reference, do not run on a Strimzi-managed cluster]** Provision the new controller with `kafka-storage.sh` and start it.
   — *Source:* https://kafka.apache.org/43/operations/kraft/#add-new-controller (Kafka 4.3, retrieved 2026-11-10)
4. **[Apache Kafka reference, do not run on a Strimzi-managed cluster]** Watch replication to it until it has caught up with the active controller:
   ```
   $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
   ```
   The add-controller section writes the command without `--bootstrap-server` or `--bootstrap-controller`, and the tool needs one of them (FINDINGS F26).
   — *Source:* https://kafka.apache.org/43/operations/kraft/#add-new-controller ; https://kafka.apache.org/43/operations/hardware-and-os/#replace-kraft-controller-disk (Kafka 4.3, retrieved 2026-11-10)
5. **[Apache Kafka reference, do not run on a Strimzi-managed cluster] [Changes quorum membership]** Add it to the quorum, through broker endpoints:
   ```
   $ bin/kafka-metadata-quorum.sh --command-config config/controller.properties --bootstrap-server localhost:9092 add-controller
   ```
   or through controller endpoints:
   ```
   $ bin/kafka-metadata-quorum.sh --command-config config/controller.properties --bootstrap-controller localhost:9093 add-controller
   ```
   Settings that the Admin Client needs, such as authentication, also go in `controller.properties`. NO OFFICIAL SOURCE FOUND in the Kafka 4.3 docs for which controller's configuration file `--command-config` must point to.
   — *Source:* https://kafka.apache.org/43/operations/kraft/#add-new-controller (Kafka 4.3, retrieved 2026-11-10)

### Remove a controller (dynamic quorum)

6. **[Apache Kafka reference, do not run on a Strimzi-managed cluster] [Reduces quorum size]** Run `remove-controller` **before** shutting the controller down:
   ```
   $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 remove-controller --controller-id <id> --controller-directory-id <directory-id>
   ```
   or with `--bootstrap-controller localhost:9093` in place of `--bootstrap-server localhost:9092`.
   — *Source:* https://kafka.apache.org/43/operations/kraft/#remove-controller (Kafka 4.3, retrieved 2026-11-10)
7. **[4.4 only, not valid on Strimzi 1.3.1]** Unregistering a controller after it was removed from the voter set is described only in the Kafka 4.4 docs. NO OFFICIAL SOURCE FOUND in the Kafka 4.3 docs: the 4.3 "Remove Controller" section ends with the `remove-controller` command. Compiler's note, not from the source: the apache/kafka 4.3.2 tools have no such command either. In `ClusterTool.java` the subcommands are `cluster-id`, `unregister` and `list-endpoints`, and `MetadataQuorumCommand.java` defines `remove-controller` with no unregister option.

## Verification

`bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status` lists the `CurrentVoters` and `CurrentObservers`. The add and remove sections do not name it as their verification step.
— *Source:* https://kafka.apache.org/43/operations/kraft/#metadata-quorum-tool (Kafka 4.3, retrieved 2026-11-10)

## Rollback or caveats

- On Strimzi 1.3.1, none of the remediation steps apply: the quorum is static, and controller node pools cannot be added, removed, scaled or renamed.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#limitations_with_static_controller_quorums (Strimzi 1.3.1, retrieved 2026-11-10)
- Remove a controller from the quorum before shutting it down.
  — *Source:* https://kafka.apache.org/43/operations/kraft/#remove-controller (Kafka 4.3, retrieved 2026-11-10)

## Related runbooks

- [kraft-quorum-debugging.md](kraft-quorum-debugging.md)
- [../strimzi/upgrade-status-and-kraft-requirement.md](../strimzi/upgrade-status-and-kraft-requirement.md)
