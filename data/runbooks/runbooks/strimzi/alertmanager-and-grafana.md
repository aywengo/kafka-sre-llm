# Deploying Alertmanager and the example Grafana dashboards

| Field | Value |
|---|---|
| Product | Strimzi example metrics configuration, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§25.1.4, §25.2.4, §25.3) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-alertmanager-str ; https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-dashboards-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str ; https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics/grafana-dashboards |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

Prometheus alerts are not being delivered, or there are no dashboards for the Strimzi components. The source does not give a symptom; this is a setup procedure.

## Diagnosis steps (prerequisites)

1. Alertmanager needs metrics enabled and exposed, and Prometheus deployed.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-alertmanager-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Grafana needs metrics enabled and exposed (Prometheus JMX Exporter or Strimzi Metrics Reporter), and Prometheus and Alertmanager deployed.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

### Alertmanager (example routes to Slack)

The example deploys an `Alertmanager` (manages the pod), a `Secret` (holds the configuration) and a `Service` (gives Prometheus and others a stable hostname). The files are `examples/metrics/prometheus-install/alert-manager.yaml` and `examples/metrics/prometheus-alertmanager-config/alert-manager-config.yaml`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-alertmanager-str (Strimzi 1.3.1, retrieved 2026-11-10)

1. In `alert-manager-config.yaml`, replace `slack_api_url` with the Slack API URL of your Slack app, and `channel` with the Slack channel for notifications.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-alertmanager-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Create the Secret from the configuration file:
   ```
   kubectl apply -f alert-manager-config.yaml
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-alertmanager-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Deploy Alertmanager:
   ```
   kubectl apply -f alert-manager.yaml
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-deploying-prometheus-alertmanager-str (Strimzi 1.3.1, retrieved 2026-11-10)

### Grafana and the example dashboards

4. Deploy Grafana from `examples/metrics/grafana-install/grafana.yaml`:
   ```
   kubectl apply -f grafana.yaml
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. Get the Grafana service details and note the port (the example shows `3000/TCP`):
   ```
   kubectl get service grafana
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)
6. Forward the Grafana UI to localhost:3000:
   ```
   kubectl port-forward svc/grafana 3000:3000
   ```
   Open http://localhost:3000 and log in. The default user name and password are both `admin`; you can change the password after the first login.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)
7. In **Configuration > Data Sources**, add Prometheus: give it a name, choose type Prometheus, and set the server URL. The Prometheus operator service `prometheus-operated` is reachable inside the cluster at `http://prometheus-operated:9090`. Save and test the connection.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)
8. Click **+**, then **Import**. Paste the JSON of a dashboard from `examples/metrics/grafana-dashboards` and click **Load**. Repeat for the other dashboards.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)

Dashboard files listed in the docs: `strimzi-operators.json`, `strimzi-kafka.json`, `strimzi-kraft.json` (KRaft controllers), `strimzi-kafka-connect.json`, `strimzi-kafka-mirror-maker-2.json`, `strimzi-kafka-bridge.json`, `strimzi-cruise-control.json`, `strimzi-kafka-exporter.json`. The 1.3.1 repo also contains `strimzi-kafka-oauth.json`, which the docs table does not list.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-dashboards-str ; https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/packaging/examples/metrics/grafana-dashboards (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

The imported dashboards appear on the Grafana **Dashboards** home page.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- The Grafana example defines no alert notification rules. Alerts come from the Prometheus rules through Alertmanager.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The example dashboards show a representative set of metrics, not every Kafka metric. All of them include JVM metrics.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-dashboards-str (Strimzi 1.3.1, retrieved 2026-11-10)
- With no traffic in the cluster yet, the Kafka Exporter dashboard shows `N/A` for numbers and "No data to show" for graphs.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-metrics-dashboards-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The Grafana pod name differs per user; use the service for port forwarding.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-metrics-grafana-dashboard-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [prometheus-alert-rules.md](prometheus-alert-rules.md)
- [consumer-lag.md](consumer-lag.md)
- [kube-state-metrics-cr-monitoring.md](kube-state-metrics-cr-monitoring.md)
