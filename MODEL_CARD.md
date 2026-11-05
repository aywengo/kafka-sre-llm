---
license: apache-2.0          # must be compatible with the base model's license
base_model: Qwen/Qwen3-14B
library_name: mlx
tags: [kafka, strimzi, kubernetes, gitops, sre, devops, lora]
language: [en]
---

# kafka-sre-14b-v1

A LoRA fine-tune of **Qwen/Qwen3-14B** for operating Apache Kafka on Kubernetes:
Strimzi, cluster operations and troubleshooting, GitOps with Argo CD, and
data streaming patterns.

## Intended use
- Answering operational questions and walking through troubleshooting
- Drafting Strimzi custom resources and GitOps manifests for human review

## Limitations
- Knowledge is frozen at the documentation snapshots listed below; CRD fields, config
  keys and defaults change between releases. Verify against current docs before
  applying anything to production.
- It can produce plausible but wrong configuration. Pair it with retrieval over
  current docs or tool access for exact values.

## Training data
Synthetic question/answer pairs generated from, and checked against, these sources:

| Source | License | Snapshot |
|---|---|---|
| Apache Kafka documentation | Apache-2.0 | `<commit>` |
| Strimzi documentation | Apache-2.0 | `<commit>` |
| Kubernetes documentation (© The Kubernetes Authors) | CC BY 4.0 | `<commit>` |
| Argo CD documentation | Apache-2.0 | `<commit>` |

Kubernetes documentation is used under CC BY 4.0; derived training data was modified
(rewritten into question/answer form).

## Training
- Method: LoRA (rank 16, all layers) with mlx-lm on Apple Silicon
- Examples: `<n train>` train / `<n valid>` valid, ~`<epochs>` epochs
- Max sequence length: 4096

## Evaluation
| Category | Base | Fine-tuned |
|---|---|---|
| kafka-ops | | |
| strimzi | | |
| kubernetes | | |
| gitops | | |

Scores are 1–5 from a judge model on a held-out question set.

## Prompt format
Use the base model's chat template. Recommended system prompt:

> You are an expert Site Reliability Engineer for Apache Kafka, Strimzi, Kubernetes,
> GitOps and data streaming platforms. Give precise, practical answers with commands
> or YAML where useful, and call out risks and version-specific behaviour. If you are
> not sure about an exact config key or flag, say so.
