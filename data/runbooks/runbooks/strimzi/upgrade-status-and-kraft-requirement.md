# Upgrade order, the KRaft requirement and checking upgrade status

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§30.1, §30.3, §30.4, §30.7) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-migrate-kraft-pre-0.39-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-api-conversion-v1-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-status-str |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded. ZooKeeper content is included only where the source marks it as a migration path. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

You are planning a Strimzi upgrade, or need to confirm that an upgrade or downgrade has finished.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps (pre-upgrade checks, in order)

To upgrade brokers and clients without downtime, follow this order:
1. Make sure your Kubernetes version is supported. Strimzi 1.3.1 requires Kubernetes 1.32 or later.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Make sure the Kafka clusters are KRaft-based. Upgrades from ZooKeeper-based clusters are not supported. From a version earlier than 0.39, you must migrate from ZooKeeper to KRaft before upgrading the Cluster Operator.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Prepare for the v1 API conversion. From a version before 0.49, this includes a `KafkaUser` update that must be done before upgrading the Cluster Operator.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. Upgrade the Cluster Operator.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. Update the Kafka version and `metadataVersion` (see [upgrade-kafka-cluster.md](upgrade-kafka-cluster.md)).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str (Strimzi 1.3.1, retrieved 2026-11-10)

From Strimzi 0.39, upgrades and downgrades between KRaft-based clusters are supported.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-sequence-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

### Coming from a version earlier than 0.39 (ZooKeeper migration path)

1. You cannot upgrade directly to 1.3.1 from a version earlier than 0.39. Required path:
   1. Upgrade to a Strimzi version between 0.39 and 0.45.
   2. Migrate the cluster from ZooKeeper to KRaft. The source points to "Using Kafka in KRaft mode", which this runbook does not cover.
   3. With the cluster running on KRaft, upgrade to Strimzi 1.3.1.

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-migrate-kraft-pre-0.39-str (Strimzi 1.3.1, retrieved 2026-11-10)

### Coming from a version earlier than 1.0.0

2. Convert your custom resources and CRDs to the v1 API. The source refers to the Strimzi 1.0.0 documentation for this, which is outside the pinned 1.3.1 docs. NO OFFICIAL SOURCE FOUND in the 1.3.1 docs for the conversion steps.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-api-conversion-v1-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification (checking an upgrade or downgrade)

After an upgrade or downgrade, check these fields in the `Kafka` resource status (`operatorLastSuccessfulVersion`, `kafkaVersion`, `kafkaMetadataVersion`):

| Field | Meaning |
|---|---|
| `operatorLastSuccessfulVersion` | The Strimzi operator version that last reconciled successfully |
| `kafkaVersion` | The Kafka version the cluster uses |
| `kafkaMetadataVersion` | The metadata version the cluster uses |

Example after an upgrade:
```
status:
  # ...
  kafkaVersion: 4.3.2
  operatorLastSuccessfulVersion: 1.3.1
  kafkaMetadataVersion: 4.3
```
Get the status with `kubectl get kafka <kafka_resource_name> -o jsonpath='{.status}' | jq` (see [cr-status-and-conditions.md](cr-status-and-conditions.md)).
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-upgrade-status-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-accessing-resource-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- ZooKeeper-based clusters cannot be upgraded to 1.3.1. They must first be migrated to KRaft on a Strimzi version between 0.39 and 0.45.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-migrate-kraft-pre-0.39-str (Strimzi 1.3.1, retrieved 2026-11-10)
- For downgrade limits see [upgrade-kafka-version-error.md](upgrade-kafka-version-error.md).

## Related runbooks

- [upgrade-kafka-cluster.md](upgrade-kafka-cluster.md)
- [upgrade-kafka-version-error.md](upgrade-kafka-version-error.md)
- [node-maintenance-kubernetes-upgrade.md](node-maintenance-kubernetes-upgrade.md)
