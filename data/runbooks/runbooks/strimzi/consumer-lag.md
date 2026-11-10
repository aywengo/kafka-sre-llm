# Consumer lag: monitoring with Kafka Exporter and reducing lag

| Field | Value |
|---|---|
| Product | Strimzi Kafka Exporter, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§11.2.6, §25.5, §25.5.1); example rules at repo tag 1.3.1 |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-kafka-exporter-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str ; https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-exporter-topic-rules-group.yaml |
| License | Derived from Strimzi documentation (CC BY 4.0, https://creativecommons.org/licenses/by/4.0/) and Strimzi example files (Apache-2.0), © the Strimzi authors. Prose reworded; config quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

- The example alert `TooLargeConsumerGroupLag` (`kafka_consumergroup_lag > 1000`, `for: 10s`, severity `warning`) fires with the description "Consumer group {{ $labels.consumergroup}} lag is too big ({{ $value }}) on topic {{ $labels.topic }}/partition {{ $labels.partition }}".
  — *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-exporter-topic-rules-group.yaml (repo tag 1.3.1, retrieved 2026-11-10)
- Consumers are slow or stalled and fall behind production.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-kafka-exporter-str (Strimzi 1.3.1, retrieved 2026-11-10)

Definition: consumer lag for a group is the distance between the last message in the partition and the message the consumer is currently picking up, that is, between the producer offset (partition head) and the consumer offset. Example: at 100 messages per second, a lag of 1000 messages is a 10-second delay. Possible causes named in the source: consuming too much old data that has not been purged, or unplanned shutdowns.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Make sure Kafka Exporter is enabled. It exposes consumer group lag metrics that broker JMX metrics do not provide. Enable it with a `kafkaExporter` section in the `Kafka` resource. The source's example (abridged):
   ```
   apiVersion: kafka.strimzi.io/v1
   kind: Kafka
   metadata:
     name: my-cluster
   spec:
     # ...
     kafkaExporter:
       # Collection filters (recommended)
       groupRegex: ".*" # (1)
       topicRegex: ".*" # (2)
       groupExcludeRegex: "^excluded-.*" # (3)
       topicExcludeRegex: "^excluded-.*" # (4)
       # ...
       # Metrics for all consumers (optional)
       showAllOffsets: false # (5)
   ```
   Apply the change. Strimzi then creates a Service and a Pod named `<kafka_cluster_name>-kafka-exporter`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-kafka-exporter-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Make sure Prometheus scrapes the exporter. The example Prometheus deployment already does: its PodMonitor `kafka-resources-metrics` matches the `strimzi.io/kind: Kafka` label that identifies the Kafka Exporter.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Import the Kafka Exporter Grafana dashboard. Use the **Lag by consumer group** panel and the messages-consumed panel to analyse lag.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. If lag metrics are missing, check that consumer groups are in use and committing offsets. Kafka Exporter reads lag from the `__consumer_offsets` topic, which only has data when consumer groups actively commit offsets.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. For the Apache Kafka CLI view of a group's position, see [../kafka/consumer-group-position.md](../kafka/consumer-group-position.md).

## Remediation steps

Typical actions to reduce lag, from the source:
1. Scale up the consumer group by adding consumers.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Increase the retention time, so messages stay in the topic longer.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Add disk capacity to increase the message buffer.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str (Strimzi 1.3.1, retrieved 2026-11-10)

Which action helps depends on the infrastructure and use case. For example, a lagging consumer is less likely to benefit from the broker serving fetch requests from its disk cache. In some cases it may be acceptable to drop messages automatically until a consumer catches up.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

If an action helps, the **Lag by consumer group** chart goes down and the messages-consumed chart goes up. §25.5 calls this chart "Messages consumed per minute" and §25.5.1 calls it "Messages consumed per second" (see FINDINGS.md).
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-kafka-exporter-lag-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- Kafka Exporter only provides consumer group and lag metrics. For general Kafka metrics, configure metrics on the brokers.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str (Strimzi 1.3.1, retrieved 2026-11-10)
- By default, metrics are collected for all consumers whether or not they are connected. `showAllOffsets: false` stops collecting metrics for disconnected consumers.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-kafka-exporter-deploy-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [prometheus-alert-rules.md](prometheus-alert-rules.md)
- [alertmanager-and-grafana.md](alertmanager-and-grafana.md)
- [../kafka/consumer-group-position.md](../kafka/consumer-group-position.md)
