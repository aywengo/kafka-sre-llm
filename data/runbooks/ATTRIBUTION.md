# Attribution

This note covers the files under `data/runbooks/` that carry no per-file licence row: `eval/*.jsonl`, `FINDINGS.md`, `coverage.md` and `sources.md`. Each runbook under `runbooks/` has its own License row, which names the licences of the sources it cites.

These files are derived from the same sources as the runbooks (see [sources.md](sources.md)), all retrieved 2026-11-10:

| Source | Copyright | Licence |
|---|---|---|
| Strimzi 1.3.1 "Deploying and Managing Strimzi" (https://strimzi.io/docs/operators/1.3.1/deploying) | © Strimzi a Series of LF Projects, LLC | **Unconfirmed.** No CC BY or other licence statement was found in the published guide, in https://strimzi.io/documentation/ or in the strimzi-kafka-operator repo at tag 1.3.1. The page footer gives only the copyright line and points to lfprojects.org/policies/. The AsciiDoc source of the guide lives in that repo, whose `LICENSE` file is Apache License 2.0. The final wording is open for a human decision. |
| strimzi/strimzi-kafka-operator, tag 1.3.1, example files under `packaging/examples/` | The Strimzi authors | Apache License 2.0 (repo `LICENSE`) |
| strimzi/drain-cleaner README, commit 70f1bc25bc14aacd15eb478768db6566bde783e3 | The Strimzi authors | Apache License 2.0 (repo `LICENSE`) |
| Apache Kafka 4.3 documentation (https://kafka.apache.org/43/operations/), source at apache/kafka tag 4.3.2 | © The Apache Software Foundation | Apache License 2.0 (header of the docs source files) |

- `eval/*.jsonl`: each item names its runbook and `source_url`; the item is derived from that runbook and carries that runbook's licences.
- `FINDINGS.md`, `coverage.md`, `sources.md`: quote and describe all four sources above, with short quotes only.
