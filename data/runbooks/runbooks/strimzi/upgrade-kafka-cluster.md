# Upgrading the Kafka version and metadataVersion of a KRaft cluster

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§30.6) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-versions-and-images-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-status-str |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Prose reworded; commands and YAML quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[ROLLING RESTART]** Changing `spec.kafka.version` rolls every Kafka node. **[POSSIBLY IRREVERSIBLE]** Raising `metadataVersion` can make a Kafka downgrade impossible.

## Symptoms / trigger

You are upgrading a Kafka cluster to a newer supported Kafka version and KRaft metadata version, usually after upgrading the Cluster Operator. The source refers to the Apache Kafka documentation for the latest on Kafka upgrade support.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps (prerequisites)

1. The Cluster Operator is running.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. The `Kafka` resource contains no configuration options that the new Kafka version does not support.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. Edit the Kafka cluster configuration. The source's command uses the placeholder `<kafka_configuration_file>`, but `kubectl edit kafka` takes the resource name (see FINDINGS.md):
   ```
   kubectl edit kafka <kafka_configuration_file>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. If `spec.kafka.metadataVersion` is set, check that the current value is supported by the target Kafka version. Example: `4.2-IV1` when upgrading from Kafka 4.2.0 to 4.3.2. If it is not set, Strimzi updates it to the current default automatically after the Kafka version change in the next step. `metadataVersion` must be a string, so that it is not read as a floating-point number.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. **[ROLLING RESTART]** Change `Kafka.spec.kafka.version` to the new version and leave `metadataVersion` at the current version's default:
   ```
   apiVersion: kafka.strimzi.io/v1
   kind: Kafka
   metadata:
     name: my-cluster
   spec:
     kafka:
       replicas: 3
       metadataVersion: 4.2-IV1 # (1)
       version: 4.3.2 # (2)
       # ...
   ```
   During the roll, some brokers run the old binaries and others the new ones. Keeping `metadataVersion` unchanged lets brokers and controllers keep talking to each other throughout the upgrade.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. If `Kafka.spec.kafka.image` is set, point it to an image with the new Kafka version. **Warning from the source:** the Cluster Operator cannot validate that an image actually contains a Kafka broker of the expected version. Make sure the image matches the Kafka version you set.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-versions-and-images-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. Save, exit the editor, and wait for the rolling updates to finish. Follow progress by watching pod state transitions, for example:
   ```
   kubectl get pods my-cluster-kafka-0 -o jsonpath='{.spec.containers[0].image}'
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
6. If needed, set the new Kafka version for Kafka Connect (`KafkaConnect.spec.version`) and MirrorMaker 2 (`KafkaMirrorMaker2.spec.version`). Rebuild any manually built custom images on the latest Strimzi base image.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
7. **[POSSIBLY IRREVERSIBLE]** If `metadataVersion` is set, update it to the new version, for example for Kafka 4.3.2:
   ```
   apiVersion: kafka.strimzi.io/v1
   kind: Kafka
   metadata:
     name: my-cluster
   spec:
     kafka:
       replicas: 3
       metadataVersion: 4.3-IV0
       version: 4.3.2
       # ...
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
8. Wait for the Cluster Operator to update the cluster.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)
9. Upgrade client applications to the new client binaries and check their compatibility. Coordinate with the teams that own them if needed.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

- Check from the `Kafka` resource status that the upgrade completed (`kafkaVersion`, `operatorLastSuccessfulVersion`, `kafkaMetadataVersion`); see [upgrade-status-and-kraft-requirement.md](upgrade-status-and-kraft-requirement.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-status-str (Strimzi 1.3.1, retrieved 2026-11-10)
- To check that clients use the latest message format, use the metric `kafka.server:type=BrokerTopicMetrics,name={Produce|Fetch}MessageConversionsPerSec`. It shows 0 when the latest format is in use.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- Be careful when changing `metadataVersion`. You cannot downgrade Kafka if the new `metadataVersion` is higher than the Kafka version you want to downgrade to. Understand the potential implications on support and compatibility if you keep an older version.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [upgrade-kafka-version-error.md](upgrade-kafka-version-error.md)
- [upgrade-status-and-kraft-requirement.md](upgrade-status-and-kraft-requirement.md)
