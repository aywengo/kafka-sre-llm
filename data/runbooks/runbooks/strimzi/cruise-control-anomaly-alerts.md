# Cruise Control monitoring and anomaly-detection alerts

| Field | Value |
|---|---|
| Product | Strimzi with Cruise Control, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§25.6) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-cruise-control-str ; https://strimzi.io/docs/operators/1.3.1/deploying#monitoring_balancedness_scores ; https://strimzi.io/docs/operators/1.3.1/deploying#setting_up_alerts_for_anomaly_detection ; https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics ; https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/grafana-dashboards/strimzi-cruise-control.json ; repo `documentation/modules/metrics/con-metrics-cruise-control.adoc`, `packaging/examples/metrics/grafana-dashboards/strimzi-cruise-control.json` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed; Strimzi repo example files (strimzi-kafka-operator tag 1.3.1), Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded. |
| Retrieved | 2026-11-10 |

> **Coverage: partial.** The Strimzi docs describe the options but defer the details (AnomalyDetector metrics, anomaly notifier configuration) to the Cruise Control project documentation, which is outside the allowed sources. Three anomaly-detector metric names do appear in the Strimzi example Cruise Control dashboard (Diagnosis step 4). No alert expression or notifier configuration is in the allowed sources. The Strimzi 1.3.1 example Prometheus rules contain no Cruise Control alert (see [prometheus-alert-rules.md](prometheus-alert-rules.md); source: https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics (repo tag 1.3.1, retrieved 2026-11-10)).

## Symptoms / trigger

You want alerts when Cruise Control's anomaly detector reports conditions that block the generation of optimization goals, such as broker failures.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#setting_up_alerts_for_anomaly_detection (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Cruise Control tracks broker, topic and partition utilization and also publishes metrics about itself (called sensors). You can use them to monitor running rebalances or alert on anomalies. The Cruise Control metrics reporter collects raw metrics from the brokers and produces them to topics that Cruise Control creates automatically.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-cruise-control-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Balancedness score: the Cruise Control metric `balancedness-score` can differ from the balancedness score in the `KafkaRebalance` resource. The metric is computed with `anomaly.detection.goals` (set in `spec.cruiseControl.config` of the `Kafka` resource), while the `KafkaRebalance` uses `default.goals`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#monitoring_balancedness_scores (Strimzi 1.3.1, retrieved 2026-11-10)
3. A refresh returns the latest cached proposal if the `KafkaRebalance` goals match `default.goals` or are not specified. Otherwise Cruise Control generates a new proposal on every refresh, and this can affect performance monitoring.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#monitoring_balancedness_scores (Strimzi 1.3.1, retrieved 2026-11-10)

4. The Strimzi example Grafana dashboard for Cruise Control queries three anomaly-detector metrics:

   | Metric | Dashboard panel | Dashboard query, abridged (`...` stands for the metric with its label selectors) |
   |---|---|---|
   | `kafka_cruisecontrol_anomalydetector_balancedness_score_value` | Balancedness Score | the metric itself |
   | `kafka_cruisecontrol_anomalydetector_goal_violation_rate_count` | Goal Violations | `round(max_over_time(increase(...[10m])[10m:]))` |
   | `kafka_cruisecontrol_anomalydetector_disk_failure_rate_count` | Kafka Reported Disk Failure Rate | `sum(irate(...[5m]))` |

   The dashboard selects them by `namespace` and `strimzi_io_cluster`, and the two rate metrics also by `kubernetes_pod_name=~"$strimzi_cluster_name-cruise-control-.*"`. The dashboard shows these metrics; it does not define alerts on them.
   — *Source:* https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/packaging/examples/metrics/grafana-dashboards/strimzi-cruise-control.json (repo tag 1.3.1, commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50, retrieved 2026-11-10)

## Remediation steps (setting up alerting)

The source gives two options:
1. Configure Cruise Control's anomaly notifier to route alerts through a notification channel.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#setting_up_alerts_for_anomaly_detection (Strimzi 1.3.1, retrieved 2026-11-10)
2. Or have Prometheus scrape the anomaly detector metrics and generate alerts, and let Prometheus Alertmanager route them. For exposing Cruise Control metrics, the source points to "Using Prometheus with Strimzi" (see [alertmanager-and-grafana.md](alertmanager-and-grafana.md)).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#setting_up_alerts_for_anomaly_detection ; https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-cruise-control-str (Strimzi 1.3.1, retrieved 2026-11-10)

NO OFFICIAL SOURCE FOUND (within the allowed sources) for alert expressions or thresholds on the anomaly-detector metrics, or for the anomaly notifier configuration. The metric names themselves are in Diagnosis step 4.

## Verification

NO OFFICIAL SOURCE FOUND.

## Rollback or caveats

NO OFFICIAL SOURCE FOUND.

## Related runbooks

- [kafkarebalance-notready-and-refresh.md](kafkarebalance-notready-and-refresh.md)
- [prometheus-alert-rules.md](prometheus-alert-rules.md)
- [alertmanager-and-grafana.md](alertmanager-and-grafana.md)
