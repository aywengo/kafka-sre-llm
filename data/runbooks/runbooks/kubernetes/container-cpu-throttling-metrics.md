# Container CPU throttling: reading the CFS throttling metrics and what to change

| Field | Value |
|---|---|
| Product | Kubernetes containers monitored with cAdvisor and Prometheus; example workload: a Kafka broker pod managed by Strimzi |
| Doc version | cAdvisor v0.60.6 (commit 5bf5d43ac6f60d7ee36a13b4d5b4a78ad2d0abc7); kubernetes-mixin version-1.5.6 (commit 464d0ae592cb7d7b1c6f40741fc72fb7bb2449ac); prometheus-operator/runbooks commit a685d14cf5128bb30e2bf935c3983decd772d885; Kubernetes website commit 18f8e9f6d2b0bc9d33e82ceac8abcfffdf8c1ca8 (default branch, no tag); Strimzi 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| Sources | Short names used below: **cAdvisor** = https://github.com/google/cadvisor/tree/v0.60.6 ; **mixin** = https://github.com/kubernetes-monitoring/kubernetes-mixin/tree/version-1.5.6 ; **prom-op runbook** = https://github.com/prometheus-operator/runbooks/blob/a685d14cf5128bb30e2bf935c3983decd772d885/content/runbooks/kubernetes/CPUThrottlingHigh.md ; **K8s docs** = https://github.com/kubernetes/website/tree/18f8e9f6d2b0bc9d33e82ceac8abcfffdf8c1ca8/content/en/docs ; **Strimzi docs** = https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.1/documentation . Full paths and line ranges: [Sources](#sources). |
| License | Derived from: cAdvisor, © The cAdvisor Authors, Apache License 2.0. kubernetes-mixin, © kubernetes-mixin Authors, Apache License 2.0. prometheus-operator/runbooks, Apache License 2.0. Kubernetes documentation, © The Kubernetes Authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Strimzi documentation and example files, part of strimzi/strimzi-kafka-operator, Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0). Prose reworded; metric names, expressions, commands and YAML quoted verbatim. |
| Retrieved | 2026-11-16 |

> **Scope note.** The Linux kernel documentation was not used. What the kernel does is stated only as far as the Kubernetes docs and the cAdvisor source describe it.

## The metrics (reference)

| Metric (all of type Counter) | cAdvisor description | Field it is read from, and that field's comment |
|---|---|---|
| `container_cpu_cfs_periods_total` | "Number of elapsed enforcement period intervals" | `CFS.Periods`: "Total number of elapsed enforcement intervals." |
| `container_cpu_cfs_throttled_periods_total` | "Number of throttled period intervals" | `CFS.ThrottledPeriods`: "Total number of times tasks in the cgroup have been throttled." |
| `container_cpu_cfs_throttled_seconds_total` | "Total time duration the container has been throttled", unit seconds | `CFS.ThrottledTime` ("Total time duration for which tasks in the cgroup have been throttled. Unit: nanoseconds."), divided by `time.Second` |

— *Source:* cAdvisor `docs/storage/prometheus.md` L15-L24; `lib/metrics/prometheus.go` L210-L253; `lib/model/container.go` L333-L351

- cAdvisor exports these three series only for a container whose CPU spec has a quota: each has the condition `s.Cpu.Quota != 0`.
  — *Source:* cAdvisor `lib/metrics/prometheus.go` L213, L228, L243
- How the limit is enforced. A `cpu` limit is "a hard limit the kernel enforces"; it is "enforced by CPU throttling". "During each scheduling interval (time slice), the Linux kernel checks to see if this limit is exceeded; if so, the kernel waits before allowing that cgroup to resume execution." By default the kubelet uses CFS quota to enforce pod CPU limits. Container runtimes do not terminate pods or containers for excessive CPU usage.
  — *Source:* K8s docs `concepts/configuration/manage-resources-containers.md` L36-L46, L261-L275, L294-L295; `tasks/administer-cluster/cpu-management-policies.md` L43-L48

## Symptoms / trigger

- The alert `CPUThrottlingHigh` fires. Meaning: "Processes experience elevated CPU throttling." Severity `info`.
- The "CPU Throttling" panel of the mixin's pod dashboard is above its threshold line.
- A container (for example a Kafka broker) is slow and its CPU limit is suspected.

Impact, as the prom-op runbook states it: "The alert is purely informative and unless there is some other issue with the application, it can be skipped."
— *Source:* prom-op runbook L8-L15; mixin `alerts/resource_alerts.libsonnet` L254-L284, `dashboards/resources/pod.libsonnet` L122-L154

## Diagnosis steps

1. Compute the throttled-period ratio: throttled periods divided by elapsed periods, each as an increase over 5 minutes. This is the `CPUThrottlingHigh` expression, quoted from the mixin (a jsonnet template; `%(...)s` are configuration values):
   ```
   sum without (id, metrics_path, name, image, endpoint, job, node) (
     topk by (%(clusterLabel)s, %(namespaceLabel)s, pod, container, instance) (1,
       increase(
         container_cpu_cfs_throttled_periods_total{container!="", %(cadvisorSelector)s, %(cpuThrottlingSelector)s}
       [5m])
     )
   )
   / on (%(clusterLabel)s, %(namespaceLabel)s, pod, container, instance) group_left
   sum without (id, metrics_path, name, image, endpoint, job, node) (
     topk by (%(clusterLabel)s, %(namespaceLabel)s, pod, container, instance) (1,
       increase(
         container_cpu_cfs_periods_total{%(cadvisorSelector)s, %(cpuThrottlingSelector)s}
       [5m])
     )
   )
   > ( %(cpuThrottlingPercent)s / 100 )
   ```
   Mixin defaults: `cpuThrottlingPercent: 25`, `cpuThrottlingSelector: ''`, `cadvisorSelector: 'job="cadvisor"'`, `clusterLabel: 'cluster'`, `namespaceLabel: 'namespace'`. The alert has `'for': '15m'` and `severity: 'info'`. So by default it fires when more than 25% of the enforcement periods of a container were throttled, for 15 minutes.

   The same expression with the defaults put in by hand (text substitution of the template above, not jsonnet output; the trailing `, ` inside the braces comes from the empty `cpuThrottlingSelector`):
   ```
   sum without (id, metrics_path, name, image, endpoint, job, node) (
     topk by (cluster, namespace, pod, container, instance) (1,
       increase(
         container_cpu_cfs_throttled_periods_total{container!="", job="cadvisor", }
       [5m])
     )
   )
   / on (cluster, namespace, pod, container, instance) group_left
   sum without (id, metrics_path, name, image, endpoint, job, node) (
     topk by (cluster, namespace, pod, container, instance) (1,
       increase(
         container_cpu_cfs_periods_total{job="cadvisor", }
       [5m])
     )
   )
   > ( 25 / 100 )
   ```
   — *Source:* mixin `alerts/resource_alerts.libsonnet` L105-L106, L254-L284; `config.libsonnet` L36, L123, L125
2. For one pod, per container: the mixin's pod dashboard query (template; `$namespace`, `$pod`, `$cluster` are dashboard variables). Its panel draws the threshold at `cpuThrottlingPercent / 100`.
   ```
   sum(increase(container_cpu_cfs_throttled_periods_total{%(cadvisorSelector)s, namespace="$namespace", pod="$pod", container!="", %(clusterLabel)s="$cluster"}[%(grafanaIntervalVar)s])) by (container) /sum(increase(container_cpu_cfs_periods_total{%(cadvisorSelector)s, namespace="$namespace", pod="$pod", container!="", %(clusterLabel)s="$cluster"}[%(grafanaIntervalVar)s])) by (container)
   ```
   On a Strimzi cluster the broker container is selected with `container="kafka"` in Strimzi's own example rules (they use it with the cAdvisor metric `container_last_seen`).
   — *Source:* mixin `dashboards/resources/queries/pod.libsonnet` L35-L36, `dashboards/resources/pod.libsonnet` L122-L154; Strimzi `packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-rules.yaml` L89
3. Check whether the application is performing normally. For a Kafka broker the prom-op runbook names no check; cross-link, NO OFFICIAL SOURCE FOUND for this route: [../kafka/monitoring-key-metrics.md](../kafka/monitoring-key-metrics.md).
   — *Source:* prom-op runbook L17-L21
4. Check whether CPU requests fit the application's usage. Usage of a pod, from the Kubernetes docs (their example pod and namespace):
   ```
   kubectl top pod cpu-demo --namespace=cpu-example
   ```
   In that example the pod shows 974 milliCPU against a limit of 1 CPU: "The container's CPU use is being throttled, because the container is attempting to use more CPU resources than its limit."
   — *Source:* prom-op runbook L20; K8s docs `tasks/configure-pod-container/assign-cpu-resource.md` L108-L122
5. Find the configured request and limit. On a Strimzi-managed cluster, Kafka nodes take them from the `KafkaNodePool` custom resource (`spec.resources`). Example from the Strimzi docs:
   ```
   spec:
     replicas: 3
     roles:
       - broker
     resources:
         requests:
           memory: 64Gi
           cpu: "8"
         limits:
           memory: 64Gi
           cpu: "12"
   ```
   — *Source:* Strimzi docs `modules/con-common-configuration-properties.adoc` L163-L165, L190-L200; `modules/appendix_crds.adoc` L3984-L4003
6. Check the kernel version of the node. The prom-op runbook says that in specific cases the node has a "too old kernel which is known to have issues with assigning CPU resources to the process", and links https://github.com/kubernetes/kubernetes/issues/67577. It gives no command and no version number; NO OFFICIAL SOURCE FOUND for either.
   — *Source:* prom-op runbook L21, L37-L38

**Throttling while average CPU usage looks low.** What the sources say: the kernel checks the limit "During each scheduling interval (time slice)"; and the prom-op runbook advises to "update app so that it is aware it runs in cgroups, or explicitly set number of CPU it can use, or limit number of threads". No source read states in so many words that a container can be throttled while its average usage is below the limit; see [Not covered](#not-covered).
— *Source:* K8s docs `concepts/configuration/manage-resources-containers.md` L270-L272; prom-op runbook L42-L43

## Remediation steps

1. If the application is healthy, change nothing. "User shouldn't increase CPU limits unless the application is behaving erratically (another alert firing)." For this reason kube-prometheus inhibits the alert by default and sends it only if another alert in the same namespace is firing.
   — *Source:* prom-op runbook L25-L31
2. **[ROLLING RESTART possible]** When other alerts fire too: "Give specific container in the pod more CPU limits. Requests can stay the same." On Strimzi, raise `limits` `cpu` under `spec.resources` of the `KafkaNodePool`. Strimzi can apply the change in place when the `Kafka` resource has the annotation `strimzi.io/in-place-resizing` set to `"true"` (experimental); when an in-place update is not possible, "Strimzi uses a rolling update instead".
   - Trade-off: "The amount between the request and limit is not reserved and might not be always available." A limit above the request lets the pod burst, and bounds the burst.
   - For accurate Cruise Control rebalance proposals with CPU goals, the Strimzi docs suggest CPU requests equal to CPU limits in `KafkaNodePool.spec.resources`, or setting the CPU capacity manually when they cannot be equal.
   — *Source:* prom-op runbook L33-L35; Strimzi docs `modules/con-common-configuration-properties.adoc` L150-L154, L288-L326, `api/io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec.adoc` L227-L231; K8s docs `tasks/configure-pod-container/assign-cpu-resource.md` L232-L240
3. **[ROLLING RESTART]** Remove the CPU limit. Then "The Container has no upper bound on the CPU resources it can use. The Container could use all of the CPU resources available on the Node where it is running", unless the namespace has a default CPU limit (a LimitRange), which is then assigned automatically. On Strimzi, "Removing resource limits" is one of the cases where an in-place update is not possible and a rolling update is used.
   - cAdvisor exports the three throttling series only while the container's CPU spec has a non-zero quota (`s.Cpu.Quota != 0`). The sources read do not say which quota value cAdvisor records once the limit is removed; if it is zero, the alert expression has no data for that container.
   - Trade-off: equal requests and limits "guarantees quality of service"; a pod without a CPU limit does not meet the `Guaranteed` criteria.
   — *Source:* K8s docs `tasks/configure-pod-container/assign-cpu-resource.md` L213-L223, `concepts/workloads/pods/pod-qos.md` L46-L51; Strimzi docs `modules/con-common-configuration-properties.adoc` L159-L161, L316-L319; cAdvisor `lib/metrics/prometheus.go` L213, L228, L243
4. Raising the CPU request does not raise the ceiling. The request "typically defines a weighting" between containers on a contended system, and requested resources "are reserved and always available to the container"; the limit is the hard ceiling. Strimzi: "A CPU request should be enough to give a reliable performance at any time." If the request is more than the free resources in the cluster, the pod is not scheduled.
   — *Source:* K8s docs `concepts/configuration/manage-resources-containers.md` L270-L275; Strimzi docs `modules/con-common-configuration-properties.adoc` L150, L226, L264
5. **[NODE DRAIN, kubelet restart]** CPU manager `static` policy ("CPU Pinning and isolation - in short give to the container full CPU cores" in the prom-op runbook). Containers in `Guaranteed` pods with integer CPU `requests` get exclusive CPUs, and "CFS quota is not used to bound the CPU usage of these containers". Containers in `BestEffort` and `Burstable` pods, and in `Guaranteed` pods with fractional CPU `requests`, run in the shared pool.
   - Requirements: the pod must be `Guaranteed` (every container has CPU and memory limits equal to its requests), the CPU request must be an integer, and the kubelet needs a CPU reservation greater than zero.
   - Set with the `--cpu-manager-policy` kubelet flag or the `cpuManagerPolicy` field in KubeletConfiguration. To change it on a node: drain the node; stop kubelet; remove `/var/lib/kubelet/cpu_manager_state`; edit the kubelet configuration; start kubelet. Skipping this makes the kubelet crashloop with `could not restore state from checkpoint: configured policy "static" differs from state checkpoint policy "none", please drain this node and delete the CPU manager checkpoint file "/var/lib/kubelet/cpu_manager_state" before restarting Kubelet`.
   - Draining a node that runs Kafka brokers: [../strimzi/node-maintenance-kubernetes-upgrade.md](../strimzi/node-maintenance-kubernetes-upgrade.md), [../strimzi/drain-cleaner.md](../strimzi/drain-cleaner.md).
   — *Source:* prom-op runbook L40-L41; K8s docs `concepts/resource-management/resource-managers.md` L52-L62, L71-L96, `concepts/workloads/pods/pod-qos.md` L46-L51, `tasks/administer-cluster/cpu-management-policies.md` L62-L70, L92-L111, L122-L143
6. Application side: "ensure to update app so that it is aware it runs in cgroups, or explicitly set number of CPU it can use, or limit number of threads." NO OFFICIAL SOURCE FOUND for which Kafka or JVM settings this means on a Strimzi broker. The Strimzi docs list "The number of network threads handling messages" among the things that affect the resources Kafka needs.
   — *Source:* prom-op runbook L42-L43; Strimzi docs `modules/con-common-configuration-properties.adoc` L143-L148

## Verification

1. The ratio from diagnosis step 1 stays at or below `cpuThrottlingPercent / 100` (0.25 by default) and `CPUThrottlingHigh` stops firing.
   — *Source:* mixin `alerts/resource_alerts.libsonnet` L105, L254-L284
2. The application performs normally. NO OFFICIAL SOURCE FOUND for a specific check.

## Rollback or caveats

- The two Strimzi/Kubernetes statements on a missing request or limit differ. Strimzi docs: "If you set limits without requests or vice versa, Kubernetes uses the same value for both." Kubernetes docs: a limit without a request gets a request equal to the limit, but a request without a limit means no upper bound (or the namespace default limit).
  — *Source:* Strimzi docs `modules/con-common-configuration-properties.adoc` L159; K8s docs `tasks/configure-pod-container/assign-cpu-resource.md` L213-L230
- With the `static` policy, system services such as the container runtime and the kubelet can still run on the exclusive CPUs; the exclusivity only extends to other pods.
  — *Source:* K8s docs `concepts/resource-management/resource-managers.md` L64-L67
- Strimzi in-place resource updates are experimental. `strimzi.io/in-place-resizing-wait-for-deferred` prevents pods rolling while a resize is deferred; Strimzi then waits indefinitely for Kubernetes to complete it.
  — *Source:* Strimzi docs `modules/con-common-configuration-properties.adoc` L297, L324-L326
- The cAdvisor source at v0.60.6 also defines `container_cpu_cfs_burst_periods_total`, which `docs/storage/prometheus.md` at the same tag does not list.
  — *Source:* cAdvisor `lib/metrics/prometheus.go` L255-L256; `docs/storage/prometheus.md` L19-L31

## Related runbooks

- [../strimzi/prometheus-alert-rules.md](../strimzi/prometheus-alert-rules.md)
- [../kafka/monitoring-key-metrics.md](../kafka/monitoring-key-metrics.md)
- [../strimzi/node-maintenance-kubernetes-upgrade.md](../strimzi/node-maintenance-kubernetes-upgrade.md)
- [../strimzi/drain-cleaner.md](../strimzi/drain-cleaner.md)

## Sources

Retrieved, or verified at the stated commit, on 2026-11-16. Line ranges are those cited above.

| Source and licence | Files (lines) |
|---|---|
| **cAdvisor**: https://github.com/google/cadvisor tag `v0.60.6`, commit 5bf5d43ac6f60d7ee36a13b4d5b4a78ad2d0abc7. Apache-2.0 (repository `LICENSE`, `lib/LICENSE`, header of the Go files) | `docs/storage/prometheus.md` (L15-L31), `lib/metrics/prometheus.go` (L210-L256), `lib/model/container.go` (L333-L351) |
| **mixin**: https://github.com/kubernetes-monitoring/kubernetes-mixin tag `version-1.5.6`, commit 464d0ae592cb7d7b1c6f40741fc72fb7bb2449ac. Apache-2.0 (repository `LICENSE`, SPDX header of the files) | `alerts/resource_alerts.libsonnet` (L95-L111, L254-L284), `config.libsonnet` (L36, L123, L125), `dashboards/resources/queries/pod.libsonnet` (L35-L36), `dashboards/resources/pod.libsonnet` (L122-L154) |
| **prom-op runbook**: https://github.com/prometheus-operator/runbooks commit a685d14cf5128bb30e2bf935c3983decd772d885. Apache-2.0 (repository `LICENSE`) | `content/runbooks/kubernetes/CPUThrottlingHigh.md` (L8-L45) |
| **K8s docs**: https://github.com/kubernetes/website commit 18f8e9f6d2b0bc9d33e82ceac8abcfffdf8c1ca8 (default branch, no tag). CC BY 4.0 (repository `LICENSE`), © The Kubernetes Authors | Under `content/en/docs/`: `concepts/configuration/manage-resources-containers.md` (L36-L46, L261-L275, L294-L295), `tasks/configure-pod-container/assign-cpu-resource.md` (L108-L122, L213-L240), `tasks/administer-cluster/cpu-management-policies.md` (L43-L48, L62-L70, L92-L111, L122-L143), `concepts/resource-management/resource-managers.md` (L52-L67, L71-L96), `concepts/workloads/pods/pod-qos.md` (L46-L51) |
| **Strimzi docs**: https://github.com/strimzi/strimzi-kafka-operator tag `1.3.1`, commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50. Apache-2.0 (repository `LICENSE`) | Under `documentation/`: `modules/con-common-configuration-properties.adoc` (L133-L226, L264, L288-L326), `modules/appendix_crds.adoc` (L3984-L4003), `api/io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec.adoc` (L227-L231). `packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-rules.yaml` (L89) |

## Not covered

Wanted, but not in an allowed source that was read, so left out above:

- A query on `container_cpu_cfs_throttled_seconds_total` (for example a per-second rate). No source read uses this metric in an expression; only its definition is given.
- An explicit statement that throttling can occur while average CPU usage is below the limit, and the mechanism (bursts inside one period, many threads). The sources give only the per-interval check and the prom-op runbook's advice about threads.
- The length of the enforcement period and how to change it. The Kubernetes pages read mention it only in an unexplained example kubelet configuration dump.
- Which cgroup files cAdvisor reads these counters from, and differences between cgroup v1 and v2 (kernel documentation was excluded).
- What quota value cAdvisor records for a container without a CPU limit. Only the export condition `s.Cpu.Quota != 0` was read.
- Which kernel versions have the problem the prom-op runbook mentions, and a command to check the node's kernel version.
- Whether a Strimzi Kafka pod meets the `Guaranteed` criteria once the `kafka` container has equal requests and limits (the other containers of the pod were not checked), and any Strimzi statement about the CPU manager `static` policy.
- What Strimzi does on a resources change when `strimzi.io/in-place-resizing` is not set. The section read says only that a rolling update is used "instead" when an in-place update is not possible.
- A field path for CPU resources in the `Kafka` custom resource for Kafka nodes. The Strimzi text read names only `KafkaNodePool` `spec.resources` for Kafka nodes.
- The kube-prometheus inhibition rule itself (only the prom-op runbook's sentence about it was read).
- Thresholds other than the mixin default of 25%, and any Kafka-specific guidance on acceptable throttling.
