# Accessing operator and Kafka component logs

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-cluster-logging-configmap-str ; repo `documentation/modules/configuring/con-using-loggers.adoc`, `documentation/modules/operators/ref-operator-cluster-logging-configmap.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Strimzi documentation, part of strimzi/strimzi-kafka-operator, licensed under the Apache License 2.0 (repository LICENSE and README at tag 1.3.1). © Strimzi a Series of LF Projects, LLC. Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

The source lists these reasons to read logs: to confirm that a component started, to monitor cluster activity, and to diagnose connection failures, authorization errors or replication problems. Strimzi operator and Kafka component logs are available without any logging configuration.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. View the logs of a pod:
   ```
   kubectl logs my-cluster-kafka-0
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Follow the logs in real time:
   ```
   kubectl logs -f my-cluster-kafka-0
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str (Strimzi 1.3.1, retrieved 2026-11-10)
3. View the logs of one container in a pod, for example the Topic Operator container in the Entity Operator pod:
   ```
   kubectl logs my-cluster-entity-operator-0 -c topic-operator
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps (changing Cluster Operator log levels)

The Cluster Operator's logging is controlled by the ConfigMap `strimzi-cluster-operator`. It is created with default values at install time and is defined in `install/cluster-operator/050-ConfigMap-strimzi-cluster-operator.yaml`. This ConfigMap controls the root logger level, the output format, per-component log levels, Kafka AdminClient and Netty log levels, and how often the configuration is reloaded.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-cluster-logging-configmap-str (Strimzi 1.3.1, retrieved 2026-11-10)

1. Change the logging configuration in one of two ways.
   - Edit the YAML file and apply it again:
     ```
     kubectl create -f install/cluster-operator/050-ConfigMap-strimzi-cluster-operator.yaml
     ```
   - Or edit the ConfigMap directly:
     ```
     kubectl edit configmap strimzi-cluster-operator
     ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-cluster-logging-configmap-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. The configuration is reloaded every `monitorInterval` seconds (default 30). Do not remove `monitorInterval` from the ConfigMap.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-cluster-logging-configmap-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

NO OFFICIAL SOURCE FOUND for a verification step. The source only states that the configuration reloads after the `monitorInterval` interval.

## Rollback or caveats

- If the ConfigMap is missing when the Cluster Operator is deployed, the default logging configuration applies. If it is deleted after deployment, the last loaded configuration stays in effect. To load a new configuration, create a new ConfigMap.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-cluster-logging-configmap-str (Strimzi 1.3.1, retrieved 2026-11-10)
- `kubectl logs` is meant for direct, real-time debugging of recent pod logs. For production, the source recommends a persistent, Kubernetes-supported cluster-level logging architecture that collects logs from all components.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-using-loggers_str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [lock-acquisition-warnings.md](lock-acquisition-warnings.md)
- [cr-status-and-conditions.md](cr-status-and-conditions.md)
- [kafka-restart-events.md](kafka-restart-events.md)
