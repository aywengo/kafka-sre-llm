# Attribution

This note covers the files under `data/runbooks/` that carry no per-file licence row: `eval/*.jsonl`, `FINDINGS.md`, `coverage.md` and `sources.md`. Each runbook under `runbooks/` has its own License row, which names the licences of the sources it cites.

These files are derived from the same sources as the runbooks (see [sources.md](sources.md)), all retrieved 2026-11-10:

| Source | Copyright | Licence |
|---|---|---|
| Strimzi 1.3.1 "Deploying and Managing Strimzi" (https://strimzi.io/docs/operators/1.3.1/deploying) | © Strimzi a Series of LF Projects, LLC | Apache License 2.0: the guide is part of strimzi/strimzi-kafka-operator (source under `documentation/`), whose `LICENSE` and `README.md` at tag 1.3.1 give Apache License 2.0. No CC BY statement was found. The site terms at lfprojects.org/policies, linked from the page footer, were not checked (FINDINGS F24). |
| strimzi/strimzi-kafka-operator, tag 1.3.1, example files under `packaging/examples/` | The Strimzi authors | Apache License 2.0 (repo `LICENSE`) |
| strimzi/drain-cleaner README, commit 70f1bc25bc14aacd15eb478768db6566bde783e3 | The Strimzi authors | Apache License 2.0 (repo `LICENSE`) |
| Apache Kafka 4.3 documentation (https://kafka.apache.org/43/operations/), source at apache/kafka tag 4.3.2 | © The Apache Software Foundation | Apache License 2.0 (header of the docs source files) |

- `eval/*.jsonl`: each item names its runbook and `source_url`; the item is derived from that runbook and carries that runbook's licences.
- `FINDINGS.md`, `coverage.md`, `sources.md`: quote and describe all four sources above, with short quotes only.

Added 2026-11-16. The section "Added 2026-11-16" of `sources.md` also describes the sources of two further runbooks (`runbooks/kafka/schema-registry-wire-format-and-magic-byte.md`, `runbooks/kubernetes/container-cpu-throttling-metrics.md`), retrieved or verified on 2026-11-16. It names them and their files and quotes nothing from them. Each of the two runbooks has its own License row.

| Source | Copyright | Licence |
|---|---|---|
| confluentinc/schema-registry, tag v8.4.1, modules `schema-serializer`, `avro-serializer`, `json-schema-serializer`, `protobuf-serializer` only | © Confluent Inc. (file headers) | Apache License 2.0 (the `LICENSE` of each of the four modules and the header of each file used). The repository root and `core/` are under the Confluent Community License and were not used. |
| Apicurio/apicurio-registry, tag 3.3.4 | Copyright Apicurio Registry a Series of LF Projects, LLC (repo `README.md`); Copyright 2022 Red Hat (repo `LICENSE`) | Apache License 2.0 (repo `LICENSE`) |
| Aiven-Open/karapace, tag 6.2.4 | © Aiven Ltd (file header, `README.rst`) | Apache License 2.0 (repo `LICENSE`) |
| apache/kafka-site, commit cd727c4dd8664857751a87cd0442e51a3d3aca8f, `content/en/43/generated/` | © The Apache Software Foundation | Apache License 2.0 (repo `LICENSE`) |
| google/cadvisor, tag v0.60.6 | Copyright 2014 The cAdvisor Authors (repo `LICENSE`); Copyright 2014 Google Inc. (Go file headers) | Apache License 2.0 (repo `LICENSE`, `lib/LICENSE`) |
| kubernetes-monitoring/kubernetes-mixin, tag version-1.5.6 | Copyright kubernetes-mixin Authors (file headers) | Apache License 2.0 (repo `LICENSE`, SPDX header of the files) |
| prometheus-operator/runbooks, commit a685d14cf5128bb30e2bf935c3983decd772d885 | No copyright line found in the file read or in the repo `LICENSE` | Apache License 2.0 (repo `LICENSE`) |
| kubernetes/website, commit 18f8e9f6d2b0bc9d33e82ceac8abcfffdf8c1ca8, `content/en/docs/` | © The Kubernetes Authors (`hugo.toml`) | CC BY 4.0 (repo `LICENSE`, "Attribution 4.0 International") |
