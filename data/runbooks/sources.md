# Sources

Attribution and licences of the sources used here: [ATTRIBUTION.md](ATTRIBUTION.md).

Retrieval date for everything below: **2026-11-10**. All fetches were plain HTTP GETs, and every request returned HTTP 200. The SHA-256 prefix identifies the exact bytes used.

## Web pages fetched (primary)

| URL | Version shown / pinned | Size (bytes) | SHA-256 (first 16) | Used for |
|---|---|---|---|---|
| https://strimzi.io/docs/operators/1.3.1/deploying | Strimzi 1.3.1 (page title "Deploying and Managing (1.3.1)") | 1,708,706 | c43b40792bbb362e | **All Strimzi citations** |
| https://strimzi.io/docs/operators/latest/deploying | Resolved to 1.3.1 at retrieval time | 1,708,803 | 6e92bde6f6eb524a | Version pin check. Same content as the 1.3.1 page; only internal links differ (`/latest/` vs `/1.3.1/`). |
| https://strimzi.io/docs/operators/latest/full/deploying.html | Strimzi latest (1.3.1), single-page variant | 1,676,002 | 5c20cc9e374f4b8e | Not cited; fetched while locating the guide |
| https://kafka.apache.org/44/operations/ | Apache Kafka 4.4 | 62,069 | 2b35d2d807277120 | Index of the operations pages |
| https://kafka.apache.org/44/operations/basic-kafka-operations/ | Apache Kafka 4.4 | 165,374 | 554551ef7d388cd4 | Kafka basic-ops runbooks |
| https://kafka.apache.org/44/operations/kraft/ | Apache Kafka 4.4 | 96,642 | 6ec83b8f87fd0b8c | KRaft runbooks, including the Upgrade section (`#upgrade`, `#update-kraft-config`) on the same page |
| https://kafka.apache.org/44/operations/monitoring/ | Apache Kafka 4.4 | 242,785 | 9b4ce983f19d132b | Metrics runbooks |
| https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/ | Apache Kafka 4.4 | 113,827 | e7bfc284a96658c7 | Geo-replication runbook |
| https://kafka.apache.org/44/operations/datacenters/ | Apache Kafka 4.4 | 63,092 | 4f92734413fbd920 | Not cited (short page with no operational procedure) |
| https://kafka.apache.org/44/getting-started/zk2kraft/ | Apache Kafka 4.4 | 80,239 | 256b31647bb85209 | FINDINGS F3 only (`reserved.broker.max.id` removed in KRaft) |
| https://strimzi.io/documentation/ | Strimzi documentation index | 13,492 | 431ca56a891c78e0 | Licence check only ([ATTRIBUTION.md](ATTRIBUTION.md)): the page has a copyright line and no licence statement |

## Repositories cloned (primary)

| Repository | Ref | Commit | What was used |
|---|---|---|---|
| https://github.com/strimzi/strimzi-kafka-operator | tag `1.3.1` (shallow clone) | 555c3e0f07c203911bc3cb7d13ac085bfaa81b50 | `documentation/**/*.adoc` (anchor→file mapping, AsciiDoc source for the `pod-monitors/*.yaml` command, version attributes); `packaging/examples/metrics/**` (Prometheus rules, kube-state-metrics rules, Grafana dashboards); repo-wide search for `runbook` |
| https://github.com/strimzi/drain-cleaner | default branch `main` (shallow clone) | 70f1bc25bc14aacd15eb478768db6566bde783e3 | `README.md` (operational notes); repo-wide search for `runbook`. The latest release tag at retrieval was `1.8.0` (eec41dc5825ccc128fe930829355d48118bec4db). The README was read at `main`, not at the tag. |
| https://github.com/apache/kafka (existing pinned copy in `raw/apache-kafka`) | tag `4.4.0` | 1156b2752aaa28eafc0b4c4d387e63356793268d | `docs/operations/*.md`, to check the published text against the docs source (FINDINGS F18) |
| https://github.com/apache/kafka | tag `4.3.2` | 0a6c96d3b61167f1141df2032f25f10fd6e118fe | `ServerLogConfigs.java`, `DynamicBrokerConfig.scala`, `docs/operations/basic-kafka-operations.md`: whether `cordoned.log.dirs` exists in Kafka 4.3.2 (FINDINGS F2). Not used as a runbook citation. |

The local `raw/strimzi` copy (also 555c3e0, from `sources.yaml`) is a sparse clone of `documentation/` only. A fresh clone at tag 1.3.1 was made so the example metrics files could be read as well.

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

The checks in `tools/check_runbooks.py` run against a cache made by `tools/fetch_sources.sh`, which clones the three repositories at the commits above and fetches the six primary pages plus the two index pages, failing on a commit mismatch and warning on a page hash mismatch.

## Sources that could not be reached

None. Every URL above returned HTTP 200.

## Secondary sources

None were consulted, not even for locating content. Everything was found in the primary sources above.
