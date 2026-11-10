# Cluster Operator upgrade or downgrade returns an unsupported Kafka version error

| Field | Value |
|---|---|
| Product | Strimzi Cluster Operator, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§30.5.5, §30.8.1, §30.8.2, §31.2.1, §31.3.2) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-operator-unsupported-kafka-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-downgrade-cluster-operator-unsupported-kafka-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-paths-kafka-versions-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-downgrade-paths-kafka-versions-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-versions-and-images-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-status-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed. Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[ROLLING RESTART]** Changing `spec.kafka.version` rolls the Kafka nodes onto new binaries (see [upgrade-kafka-cluster.md](upgrade-kafka-cluster.md)).

## Symptoms / trigger

After you upgrade or downgrade the Cluster Operator to a version that does not support the Kafka version in use, you get an unsupported Kafka version error. This applies to every installation method.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-operator-unsupported-kafka-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-downgrade-cluster-operator-unsupported-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Look for the error in the status conditions of the `Kafka` resource:
   ```
   kubectl get kafka <kafka_cluster_name> -n <namespace> -o jsonpath='{.status.conditions}'
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-operator-unsupported-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Check which Kafka versions the target Strimzi version supports in the "Supported versions" table on the Strimzi website. The Operators column lists Strimzi releases, and the Kafka versions column lists the Kafka versions each one supports.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-paths-kafka-versions-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. In Strimzi 1.3.1, `Kafka.spec.kafka.version` defaults to the latest supported Kafka version, 4.3.2, when it is not set.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-versions-and-images-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. **Upgrade case. [ROLLING RESTART]** Upgrade Kafka as part of the Strimzi upgrade: set `spec.kafka.version` in the `Kafka` resource to a version the new Strimzi version supports. Multi-version upgrades are possible even when the old and new Strimzi versions support different Kafka versions.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-paths-kafka-versions-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-operator-unsupported-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. **Downgrade case. [ROLLING RESTART]** If possible, downgrade Kafka as part of the Strimzi downgrade: set `spec.kafka.version` to a version the target Strimzi version supports. If Kafka cannot be downgraded, that Strimzi downgrade path is not supported. Known limitation: for a target Strimzi version of 0.45.0 or earlier, the current Kafka version must be supported by the target version. Downgrading from an unsupported Kafka version is only possible in Strimzi 0.46.0 and later.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-downgrade-paths-kafka-versions-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-downgrade-cluster-operator-unsupported-kafka-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. If `Kafka.spec.kafka.image` is set, it overrides the default image for the version. Update it to an image with the new Kafka version. Otherwise the default image comes from the Cluster Operator's `STRIMZI_KAFKA_IMAGES` mapping (`<kafka_version>=<image>`, for example `4.3.2=quay.io/strimzi/kafka:1.3.1-kafka-4.3.2`).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-versions-and-images-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

Check that the error has cleared from `.status.conditions`. Then check `kafkaVersion` and `operatorLastSuccessfulVersion` in the status; see [upgrade-status-and-kraft-requirement.md](upgrade-status-and-kraft-requirement.md).
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-cluster-operator-unsupported-kafka-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- The Cluster Operator cannot check that an image really contains the expected Kafka version. Make sure the image matches the version.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-versions-and-images-str (Strimzi 1.3.1, retrieved 2026-11-10)
- Downgrading Kafka may be impossible after `metadataVersion` was raised; see [upgrade-kafka-cluster.md](upgrade-kafka-cluster.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-upgrade-kafka-kraft-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [upgrade-kafka-cluster.md](upgrade-kafka-cluster.md)
- [upgrade-status-and-kraft-requirement.md](upgrade-status-and-kraft-requirement.md)
