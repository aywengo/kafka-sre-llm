# Cruise Control monitoring and anomaly-detection alerts

| Field | Value |
|---|---|
| Product | Strimzi with Cruise Control, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§25.6) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-cruise-control-str ; https://strimzi.io/docs/operators/1.3.1/deploying#monitoring_balancedness_scores ; https://strimzi.io/docs/operators/1.3.1/deploying#setting_up_alerts_for_anomaly_detection ; repo `documentation/modules/metrics/con-metrics-cruise-control.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded. |
| Retrieved | 2026-11-10 |

> **Coverage: partial.** The Strimzi docs describe the options but defer the details (AnomalyDetector metric names, anomaly notifier configuration) to the Cruise Control project documentation, which is outside the allowed sources. The Strimzi 1.3.1 example Prometheus rules contain no Cruise Control alert (see [prometheus-alert-rules.md](prometheus-alert-rules.md); source: https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics (repo tag 1.3.1, retrieved 2026-11-10)).

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

## Remediation steps (setting up alerting)

The source gives two options:
1. Configure Cruise Control's anomaly notifier to route alerts through a notification channel.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#setting_up_alerts_for_anomaly_detection (Strimzi 1.3.1, retrieved 2026-11-10)
2. Or have Prometheus scrape the anomaly detector metrics and generate alerts, and let Prometheus Alertmanager route them. For exposing Cruise Control metrics, the source points to "Using Prometheus with Strimzi" (see [alertmanager-and-grafana.md](alertmanager-and-grafana.md)).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#setting_up_alerts_for_anomaly_detection ; https://strimzi.io/docs/operators/1.3.1/deploying#con-metrics-cruise-control-str (Strimzi 1.3.1, retrieved 2026-11-10)

NO OFFICIAL SOURCE FOUND (within the allowed sources) for anomaly-detector metric names, alert expressions or notifier configuration.

## Verification

NO OFFICIAL SOURCE FOUND.

## Rollback or caveats

NO OFFICIAL SOURCE FOUND.

## Related runbooks

- [kafkarebalance-notready-and-refresh.md](kafkarebalance-notready-and-refresh.md)
- [prometheus-alert-rules.md](prometheus-alert-rules.md)
- [alertmanager-and-grafana.md](alertmanager-and-grafana.md)
