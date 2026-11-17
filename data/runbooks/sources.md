# Sources

Attribution and licences of the sources used here: [ATTRIBUTION.md](ATTRIBUTION.md).

Retrieval date for everything below: **2026-11-10**. All fetches were plain HTTP GETs, and every request returned HTTP 200. The SHA-256 prefix identifies the exact bytes used.

## Web pages fetched (primary)

| URL | Version shown / pinned | Size (bytes) | SHA-256 (first 16) | Used for |
|---|---|---|---|---|
| https://strimzi.io/docs/operators/1.3.1/deploying | Strimzi 1.3.1 (page title "Deploying and Managing (1.3.1)") | 1,708,706 | c43b40792bbb362e | **All Strimzi citations** |
| https://strimzi.io/docs/operators/latest/deploying | Resolved to 1.3.1 at retrieval time | 1,708,803 | 6e92bde6f6eb524a | Version pin check. Same content as the 1.3.1 page; only internal links differ (`/latest/` vs `/1.3.1/`). |
| https://strimzi.io/docs/operators/latest/full/deploying.html | Strimzi latest (1.3.1), single-page variant | 1,676,002 | 5c20cc9e374f4b8e | Not cited; fetched while locating the guide |
| https://kafka.apache.org/43/operations/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 59,059 | 7b1f3c5e2381bc6d | Index of the operations pages |
| https://kafka.apache.org/43/operations/basic-kafka-operations/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 153,280 | 23a5dfb277d50adb | Kafka basic-ops runbooks |
| https://kafka.apache.org/43/operations/kraft/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 91,527 | 6ca9f92aa9a55856 | KRaft runbooks, including the Upgrade section (`#upgrade`, `#update-kraft-config`) on the same page |
| https://kafka.apache.org/43/operations/monitoring/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 231,556 | 450615c61b0790ee | Metrics runbooks |
| https://kafka.apache.org/43/operations/geo-replication-cross-cluster-data-mirroring/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 111,651 | e8422af4c8f8b16f | Geo-replication runbook |
| https://kafka.apache.org/43/operations/hardware-and-os/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 76,761 | 049775f08c8c82db | KRaft quorum runbooks: the complete `describe --replication` command (`#replace-kraft-controller-disk`) |
| https://kafka.apache.org/43/operations/datacenters/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 60,857 | ef8eb308766ebfdb | Not cited (short page with no operational procedure) |
| https://kafka.apache.org/43/getting-started/zk2kraft/ | Apache Kafka 4.3 (page says "version (4.3) of Kafka") | 79,890 | d9279825825f328f | FINDINGS F3 only (`reserved.broker.max.id` removed in KRaft) |
| https://strimzi.io/documentation/ | Strimzi documentation index | 13,492 | 431ca56a891c78e0 | Licence check only ([ATTRIBUTION.md](ATTRIBUTION.md)): the page has a copyright line and no licence statement |

## Repositories cloned (primary)

| Repository | Ref | Commit | What was used |
|---|---|---|---|
| https://github.com/strimzi/strimzi-kafka-operator | tag `1.3.1` (shallow clone) | 555c3e0f07c203911bc3cb7d13ac085bfaa81b50 | `documentation/**/*.adoc` (anchor→file mapping, AsciiDoc source for the `pod-monitors/*.yaml` command, version attributes); `packaging/examples/metrics/**` (Prometheus rules, kube-state-metrics rules, Grafana dashboards); repo-wide search for `runbook` |
| https://github.com/strimzi/drain-cleaner | default branch `main` (shallow clone) | 70f1bc25bc14aacd15eb478768db6566bde783e3 | `README.md` (operational notes); repo-wide search for `runbook`. The latest release tag at retrieval was `1.8.0` (eec41dc5825ccc128fe930829355d48118bec4db). The README was read at `main`, not at the tag. |
| https://github.com/apache/kafka | tag `4.3.2` (shallow sparse clone of `docs/`) | 0a6c96d3b61167f1141df2032f25f10fd6e118fe | `docs/operations/*.md`, to check the published text against the docs source (FINDINGS F18); `ServerLogConfigs.java` and `DynamicBrokerConfig.scala`, for whether `cordoned.log.dirs` exists in Kafka 4.3.2 (FINDINGS F2) |

Kafka pin: 4.3.2 is the newest Kafka version Strimzi 1.3.1 supports (FINDINGS F2). Earlier commits on this branch pinned the Apache Kafka 4.4 docs (tag `4.4.0`, commit 1156b2752aaa28eafc0b4c4d387e63356793268d). The owner moved the pin to 4.3 on 2026-11-10. Every Kafka runbook was then checked again against the 4.3.2 docs source. Content that is only in the 4.4 docs was removed or marked NO OFFICIAL SOURCE FOUND (FINDINGS F2).

The local `raw/strimzi` copy (also 555c3e0, from `sources.yaml`) is a sparse clone of `documentation/` only. A fresh clone at tag 1.3.1 was made so the example metrics files could be read as well.

## Code and tools consulted to check commands (FINDINGS F26)

Not documentation, and not in the documentation source list above. Used only to check whether a command given in the documentation runs as written. The runbooks label every claim based on them as a compiler's note.

| Source | Ref | What was used |
|---|---|---|
| https://github.com/apache/kafka `tools/src/main/java/org/apache/kafka/tools/MetadataQuorumCommand.java` | tag `4.3.2` (0a6c96d3b61167f1141df2032f25f10fd6e118fe), fetched with the GitHub contents API | Line 103: `--bootstrap-server` / `--bootstrap-controller` are a required mutually exclusive group |
| https://github.com/kubernetes/kubernetes `pkg/apis/core/v1/conversion.go`, `pkg/apis/events/v1/conversion.go`, `staging/src/k8s.io/client-go/restmapper/discovery.go` | tag `v1.34.0` (275918a59a3df182fc5e9f7f9f6e960384399a35), fetched with the GitHub contents API | Event field selectors accepted by each API group; core `v1` resources take priority when a bare resource name such as `events` is resolved |
| kubectl 1.37.1 (local binary, Homebrew) | `kubectl version --client` | `kubectl apply -h` text for `-f`; offline tests with an empty kubeconfig and an unreachable server (glob after one `-f`), and `--dry-run=client` ConfigMaps (JSONPath dot escaping). No cluster was contacted |

## Referenced by the sources but not fetched

These are outside the allowed source list or outside the pinned versions. Runbooks mark the gaps with NO OFFICIAL SOURCE FOUND or point to the reference by name.

| Reference | Where it is referenced | Why not fetched |
|---|---|---|
| Strimzi 1.0.0 documentation (v1 API conversion) | Strimzi 1.3.1 §30.4 | Not the pinned version |
| "Using Kafka in KRaft mode" (ZooKeeper→KRaft migration, Strimzi 0.39–0.45) | Strimzi 1.3.1 §30.3 | Not the pinned version; ZooKeeper content is legacy-only scope |
| Cruise Control project documentation (AnomalyDetector metrics, anomaly notifier, sensors) | Strimzi 1.3.1 §25.6, §25.6.2 | Not in the allowed source list |
| Strimzi "Supported versions" table on strimzi.io | Strimzi 1.3.1 §30.8.1 | Not fetched; the runbooks only state that it should be checked |
| Kubernetes documentation (upgrade path, PDB) | Strimzi 1.3.1 §30.2 | Not in the allowed source list |
| Strimzi in-development docs | Task brief ("only for comparison") | Not fetched; no comparison was made |

The checks in `tools/check_runbooks.py` run against a cache made by `tools/fetch_sources.sh`, which clones the three repositories at the commits above and fetches the seven primary pages plus the two index pages, failing on a commit mismatch, on modified or untracked files in a checkout, or on a page hash mismatch. The pinned page hashes are in `tools/pinned_pages.txt`, and `check_runbooks.py` refuses to run against cached pages that do not match them.

## Sources that could not be reached

None. Every URL above returned HTTP 200.

## Secondary sources

None were consulted, not even for locating content. Everything was found in the primary sources above.

## Added 2026-11-16: sources of two further runbooks

Everything above this heading is unchanged and describes the runbooks retrieved on 2026-11-10. This section covers only [runbooks/kafka/schema-registry-wire-format-and-magic-byte.md](runbooks/kafka/schema-registry-wire-format-and-magic-byte.md) and [runbooks/kubernetes/container-cpu-throttling-metrics.md](runbooks/kubernetes/container-cpu-throttling-metrics.md). Retrieval or verification date: **2026-11-16**. Each runbook ends with a Sources section that gives the file paths and line ranges.

These runbooks cite repository files at pinned commits, mostly source code, not published documentation pages. They also use secondary sources for cross-checks, named as such below. `tools/fetch_sources.sh` and `tools/check_runbooks.py` do not know these repositories: the cache has none of them, so the `code` check cannot match the tokens of the two runbooks.

| Repository | Ref | Commit | What was used | Used for |
|---|---|---|---|---|
| https://github.com/confluentinc/schema-registry | tag `v8.4.1` (shallow, sparse clone made 2026-11-16) | 2e7a638f235f5ef18acff5556652e6a17a5b2f0c | Only the modules `schema-serializer`, `avro-serializer`, `json-schema-serializer`, `protobuf-serializer`, each with its own Apache-2.0 `LICENSE` (same SHA-256 prefix cb5e8e7e5f4a3988 as the root `LICENSE-Apache`) and an Apache header in every file cited. Not used: the repository root and `core/` (Confluent Community License). | Schema-registry runbook, primary |
| https://github.com/Apicurio/apicurio-registry | tag `3.3.4` (local `raw/apicurio-registry`; sparse clone for one source file) | 5662f79947dce0c09a3b9f61ddd5c4bc588fa004 | `docs/modules/ROOT/pages/getting-started/assembly-configuring-kafka-client-serdes.adoc`, `assembly-migrating-registry-v2-v3.adoc`, `assembly-confluent-schema-registry-compatibility.adoc`; `serdes/generic/serde-common/src/main/java/io/apicurio/registry/serde/BaseSerde.java` | Schema-registry runbook, secondary (cross-check, Apicurio settings) |
| https://github.com/Aiven-Open/karapace | tag `6.2.4` (local `raw/karapace`; sparse clone for one source file) | 09cba53ffcc961038f608b34f7d00fc2df1d313c | `src/karapace/core/serialization.py`, `website/docs/import-mode.md`, `README.rst` | Schema-registry runbook, secondary (cross-check) |
| https://github.com/apache/kafka | tag `4.3.2` (local `raw/apache-kafka`) | 0a6c96d3b61167f1141df2032f25f10fd6e118fe | `docs/implementation/message-format.md`, `docs/operations/monitoring.md`, `docs/operations/tiered-storage.md`, `docs/operations/basic-kafka-operations.md`, `docs/streams/quickstart.md`, `docs/streams/developer-guide/config-streams.md`, `docs/getting-started/upgrade.md`, `docs/kafka-connect/user-guide.md` | Schema-registry runbook (console consumer, error handling, record magic) |
| https://github.com/apache/kafka-site | default branch (local `raw/apache-kafka-config`) | cd727c4dd8664857751a87cd0442e51a3d3aca8f | `content/en/43/generated/consumer_config.html`, `producer_config.html` | Schema-registry runbook (`key.deserializer`, `value.deserializer`, `key.serializer`, `value.serializer`) |
| https://github.com/google/cadvisor | tag `v0.60.6` (sparse clone) | 5bf5d43ac6f60d7ee36a13b4d5b4a78ad2d0abc7 | `docs/storage/prometheus.md`, `lib/metrics/prometheus.go`, `lib/model/container.go` | CPU throttling runbook, primary (metric definitions) |
| https://github.com/kubernetes-monitoring/kubernetes-mixin | tag `version-1.5.6` (sparse clone) | 464d0ae592cb7d7b1c6f40741fc72fb7bb2449ac | `alerts/resource_alerts.libsonnet`, `config.libsonnet`, `dashboards/resources/queries/pod.libsonnet`, `dashboards/resources/pod.libsonnet` | CPU throttling runbook, primary (alert expression, threshold) |
| https://github.com/prometheus-operator/runbooks | default branch (sparse clone) | a685d14cf5128bb30e2bf935c3983decd772d885 | `content/runbooks/kubernetes/CPUThrottlingHigh.md` | CPU throttling runbook, primary (meaning, impact, diagnosis, mitigation) |
| https://github.com/kubernetes/website | default branch (local `raw/kubernetes`) | 18f8e9f6d2b0bc9d33e82ceac8abcfffdf8c1ca8 | Under `content/en/docs/`: `concepts/configuration/manage-resources-containers.md`, `concepts/resource-management/resource-managers.md`, `concepts/workloads/pods/pod-qos.md`, `tasks/configure-pod-container/assign-cpu-resource.md`, `tasks/administer-cluster/cpu-management-policies.md`. The five files have the same blob ids at upstream commit 48a1f06596294a8ea2304fcc33047d3b639f135d (default branch on 2026-11-16). | CPU throttling runbook (limit enforcement, CPU manager policies) |
| https://github.com/strimzi/strimzi-kafka-operator | tag `1.3.1` (local `raw/strimzi`; one raw file fetch, HTTP 200, 4,400 bytes, SHA-256 prefix e7120e9b8163475e) | 555c3e0f07c203911bc3cb7d13ac085bfaa81b50 | `documentation/modules/con-common-configuration-properties.adoc`, `documentation/modules/appendix_crds.adoc`, `documentation/api/io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec.adoc`; `packaging/examples/metrics/prometheus-install/prometheus-rules/prometheus-kafka-rules.yaml` (the raw fetch) | CPU throttling runbook (where CPU requests and limits are set) |

How the commits were checked: `git rev-parse HEAD` in every checkout, `git ls-remote --tags` for the tags `v8.4.1`, `v0.60.6` and `version-1.5.6`, and `git status` for the local `raw/` checkouts (no modified files; only the untracked `.source-commit` and `.source-ref` markers).

Licences: Apache License 2.0 for all rows except kubernetes/website, which is CC BY 4.0 (repository `LICENSE`, "Attribution 4.0 International").

Excluded on purpose: Confluent website text, the `core/` module and the root of confluentinc/schema-registry, and the Linux kernel documentation (GPL). Facts that would have needed them are listed in the "Not covered" section of each runbook.
