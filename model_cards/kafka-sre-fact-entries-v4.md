---
license: apache-2.0
language:
  - en
pretty_name: Kafka SRE fact entries v4
size_categories:
  - 1K<n<10K
tags:
  - kafka
  - strimzi
  - retrieval
  - synthetic
  - model-generated
---

# Kafka SRE fact entries (v4)

> **DRAFT, for the Owner. Not published.** Nothing in this folder has been uploaded. Publication is planned with v4.0.0,
> after the Owner's go. The box "For the Owner's review" at the end lists what is still the Owner's to decide.

`entries_index.jsonl` holds 7,028 short fact statements about Apache Kafka and Strimzi. **A language model wrote them
from the documentation chunks of the kafka-sre-llm index. They are not documentation, they may be partial, and they are
not a primary source.** Read the audit and the limits before relying on one.

This dataset sits next to the documentation index
[`aywengo/kafka-sre-docs-index-v3`](https://huggingface.co/datasets/aywengo/kafka-sre-docs-index-v3) and is meant to be used with the
kafka-sre-llm runtime (release v4.0.0, planned).

## What the entries are

One JSON object per line, 7,028 lines, all of prompt version `kbe/5`. Each entry has three kinds of text:

- **Written by a model:** `title`, `statement` and `applies_when` (a restatement of what the chunk says, and the condition it
  holds under; `applies_when` is absent when the writer gave none).
- **Verbatim quotes** of at most 300 characters (`quotes`), each tied to a chunk id of the index.
- **Taken mechanically** from the chunk: the heading trail, the lead-in line before a list, the source path and the pin
  (`sources`, `heading_trail`, `lead_in`, `scope`, `window`). `rendered` is the text the runtime puts into the prompt.

| Field of the record | Value |
|---|---|
| Writer | `DeepSeek-V4.1-Flash-1017-oQ4e-mtp` (local, prompt `kbe/5`) |
| Checker | `GLM-5.3-Flash-oQ8e-mtp` (local, prompt `kbc/4`): it rejects an entry that a chunk does not support |
| Entries | 7,028 (5,227 fact, 755 warning, 568 value, 463 procedure, 15 cross) |
| Sources | Apache Kafka 4.3.2 documentation: 3,007 entries; Strimzi 1.3.1 documentation: 4,021 entries |
| sha256 of `entries_index.jsonl` | `9722a8f3b323bccc4c89e7c38929236e57a13507965548754e2eef28ac266938` |
| sha256 of the record `kb_entries.json` | `c71d74afa91f62232b439682f56b8b0ae1a2c98c074dd4047868dfc04d587577` |

`kb_entries.json` is the record of the index: the writer and checker models, the prompt versions, the count and the
hash of the index. The runtime refuses an entries file whose hash differs from the record.

The entries come from Apache Kafka and Strimzi documentation only, both Apache-2.0. The slice they were written for
holds no Kubernetes text (CC BY 4.0) and no text from the project's own runbooks, so most chunks of the index have no entry.

## How they are used

With the kafka-sre-llm runtime, `python -m kafka_sre_rag ask ... --kb-entries`, in the **release configuration RE** of
v4.0.0 (Gemma 4 31B, the frozen retrieval, the lookup cards, and the fact entries). The runtime merges the entries that
match the question into the retrieved pack after the documentation excerpts, with a cap of **10 items and 24,000
characters** for the whole pack. The documentation excerpts remain in the prompt; the entries add to them and do not
replace them. The installer of the release is to download this dataset at a pinned revision and check both hashes above (not yet wired).

**Consult the pinned source before applying an entry.** An entry is a pointer to the documentation, not a replacement
for it: open the cited chunk and the upstream page at the pin named in `NOTICE`, and check the condition, the version
and the scope there before you run anything on a cluster.

## The audit

An independent audit rated a sample of the accepted entries. Eight blind Claude sessions rated 200 entries drawn from the
7,028 accepted ones; they saw only a bundle of each entry with its source text, and the definition of "ok"; they did not see the checker's verdict.
A rating is one of ok, partial (true to the source but drops a condition, a version, a scope or the like) or wrong (the
source contradicts it).

| Sample | n | ok | partial | wrong | ok share | 90 % interval (Clopper-Pearson) |
|---|---|---|---|---|---|---|
| Main sample | 200 | **178** | 22 | **0** | **89.0 %** | 84.7 % to 92.4 % |

- A second rater re-rated 50 of the 200 blind and **agreed on 50 of 50**. It is the same model family (Claude), so the
  agreement shows that the ratings are stable, not that they are free of a shared bias.
- The strictest session found 10 partial of its 40; the result does not depend on it.
- **The bar was changed after the result.** The bar fixed beforehand for admitting the entries was **190 of 200 (95 %)**.
  178 of 200 does not meet it. After the result was known, the Owner set the bar to **80 %** for this release only
  (decision W43, 2026-11-27); 178 of 200 passes that. This is a bar moved after the fact, and the entries were admitted
  into v4.0.0 on the moved bar.
- What the 22 partial entries have wrong: a condition or a scope dropped (a version, a mode, a sibling step, the subject of
  the sentence), an example stated as a rule, stronger wording than the source, a claim that stands only in the lead-in
  or in an unquoted sentence, or two sentences joined into a claim the source does not make. No rater found a claim that
  the source contradicts. **A dropped condition can mislead a reader.**
- 14 entries of the kind `cross` (from two chunks) were rated separately: 10 ok, 4 partial. They are not in the figure above.
- All 200 sampled entries carried the checker's verdict ok, so the checker's precision on accepted entries is 89 %.
- Not checked: whether a rater of another model family would agree, and whether the 22 partial entries would harm an answer.

## Limits

- **The entries are model-written.** Titles, statements and `applies_when` are restatements by a language model. Only
  the quoted spans are verbatim.
- **They may be partial.** 22 of 200 sampled entries (11 %) drop a condition, a version or a scope.
- **They are not a primary source.** The documentation is. Check a command or a value in the cited chunk and the
  upstream page before running it on a cluster. The entries follow the pins named in `NOTICE` and go out of date.
- **Two different figures, not to be mixed.** (1) *Retrieval credit* counts documentation chunks only: an entry never
  serves a required point (D7 rule 5), so the entries cannot raise the retrieval figure. (2) *Judged answer coverage* is
  what the judge finds in the final answer, and the entries can affect it, because the answer is written with them in
  the prompt.
  <!-- source: D7 ledger, rule 5; runs/m7/reports/b9_d7_rules34.md -->
- **Their effect was measured only inside RE, on one batch.** On the 60 scenarios of D7, RE (with entries) against R (the
  same configuration without entries) is descriptive, with no bar: judged coverage **+0.016** (90 % interval -0.015 to
  +0.047); in the slice of 40 scenarios where entries quote a covering chunk **+0.035**; outside it (20 scenarios)
  **-0.022**. The interval of the whole batch includes zero, so no effect is shown. D7 is not independent of the
  entries: 586 of the 7,028 entries quote a chunk that covers a D7 point, all in the 40 scenarios of the slice. The
  entries were written before D7 existed and D7's writers never saw them.
- Measured with Gemma 4 31B only, with the project's own judge and readers (models), on 60 scenarios.
- No effect on questions that the entries do not touch is claimed.

**Harm.** Nothing here isolates the effect of the entries on harmful statements.
<!-- sources: runs/m7/M7_RELEASE_TEST_FINAL.md lines 119 to 120 (h6, B1); runs/m7/reports/b9_d7_rules34.md line 50, "answers with a harm flag (Claude, reading-only)", first column -->

- On h6 (the hidden decision set, 60 scenarios), the Owner judged 7 of 60 answers harmful for Gemma with entries (RE) and
  7 of 60 for the Qwen control (the limit was 1 of 60). The control is a different model without entries, so this
  cannot isolate the effect of the entries.
- On D7 (open, 60 scenarios), adding the entries (RE minus R) raised the share of answers with a harm flag from the
  first reader by 6.7 points (90 % interval +1.7 to +11.7). The flags were not adjudicated.
- On D7 the adjudicated bars B1, B2 and B4 are "not adjudicated (not measured)"; B3 (coverage) is measured and not met.
  The Owner dropped the D7 adjudication (W46).
- On h6, conclusions are unchanged on 59 scenarios.

## Licences

**This dataset is released under the Apache License 2.0** (`LICENSE`). `NOTICE` has the attribution and the statement of
changes; the Apache Kafka NOTICE file is in `notices/apache-kafka/NOTICE`. Not endorsed by the Apache Software Foundation
or the Strimzi authors.

| Source (documentation the entries restate) | Licence | Entries | Pin |
|---|---|---|---|
| Apache Kafka documentation | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | 3,007 | tag 4.3.2 (commit `0a6c96d3b61167f1141df2032f25f10fd6e118fe`) |
| Strimzi documentation | [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) | 4,021 | tag 1.3.1 (commit `555c3e0f07c203911bc3cb7d13ac085bfaa81b50`) |

The models that wrote and checked the entries, and the model that uses them:

| Model | Role | Licence |
|---|---|---|
| [DeepSeek-V4.1-Flash](https://huggingface.co/deepseek-ai/DeepSeek-V4.1-Flash) | wrote the entries | MIT |
| [GLM-5.3-Flash](https://huggingface.co/zai-org/GLM-5.3-Flash) | checked the entries | MIT |
| [Gemma 4 31B](https://huggingface.co/google/gemma-4-31B-it) | answers with the entries in the release configuration; wrote and checked none | Apache-2.0 ([licence text](https://ai.google.dev/gemma/docs/gemma_4_license)) |

DeepSeek and GLM wrote and checked the entries locally, on the project's own machine, as open-weight models on a local
server; no hosted service was involved. Their MIT licences contain no clause on outputs.
<!-- source: runs/m7/reports/m8_fact_entry_scope.md section 6 ("Both run locally"); the record kb_entries.json names the local writer and checker --> The audit
raters (hosted Claude sessions) wrote ratings only; no entry text was written by them.

**Gemma prohibited-use policy.** Google publishes a prohibited-use policy for Gemma:
<https://ai.google.dev/gemma/prohibited_use_policy>. Gemma 4 31B-it is published under Apache-2.0
([licence text](https://ai.google.dev/gemma/docs/gemma_4_license)). The project did not verify whether the policy
binds a user of Gemma 4 under Apache-2.0. Gemma wrote and checked no entry; the card names it as the model that uses them.
<!-- source: runs/m7/reports/gemma4_licence.md -->

## For the Owner's review (this draft)

1. Whether the upload goes out under the account named in this card (`aywengo`), and the release go.
2. The runtime's own wording about entries (`--kb-entries` help and warning) is not touched here and is a separate question.
