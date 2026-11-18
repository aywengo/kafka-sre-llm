---
license: apache-2.0
base_model: Qwen/Qwen3.8-27B
library_name: mlx
pipeline_tag: text-generation
tags: [lora, adapter, kafka, kubernetes, strimzi, argo-cd, retrieval, rag, sre, experimental]
language: [en]
---

# kafka-sre-27b-v2-retrieval-only

> **Experimental. Verify every config and command before applying it to a cluster. Not for unattended use.**

This is a LoRA adapter (about 467 MB) for **Qwen/Qwen3.8-27B**, trained on question and answer pairs about
Apache Kafka, Strimzi, Kubernetes and Argo CD. It writes short answers (median 1,208 characters against 4,714 for
the base model with retrieval, 40 answers each) when it is given retrieved documentation excerpts, and it writes its own search terms for the retrieval step.
**Use it only with retrieved documentation excerpts in the prompt**: the retrieval runtime `kafka_sre_rag` of the
kafka-sre-llm repository with the documentation index of release v3.0.0, with the adapter writing its own search
terms. Without excerpts it scored -0.18 against the plain base model (40 real questions, two judge passes) and must not be used that way.

**It is not a better model than the base model with retrieval.** The project's default recommendation remains the
unmodified base model plus retrieval (release v3.0.0). This adapter is published for people who want much shorter
answers and who accept that short answers leave out steps. See "What was measured" below.

It is the adapter only. No fused, 4-bit or GGUF build exists, because none was measured.

## What was measured

All rows are on the 40 hand-written real questions of the project (`questions_real`), judged by a local model
(GLM-5.3-Flash, `oQ8e-mtp` build), two judge passes (the second with the answer order flipped). A difference is the
contestant's score minus the reference's score, on a 1 to 5 scale. The standard error is about 0.2, so differences
under about 0.2 are noise. **This question set has been reused many times for decisions**; every number below leans on it
again. The judge is a model, not a person.

| Measurement | Size | Result |
|---|---|---|
| v2 + retrieval, **its own search terms**, against base + retrieval | 40 questions, 2 passes | mean -0.07, standard error 0.21, 15 wins / 6 ties / 19 losses |
| v2 + retrieval, **the base model's search terms**, against base + retrieval | 40 questions, 2 passes | mean -0.29, standard error 0.21, 9 wins / 7 ties / 24 losses |
| v2, **no retrieval**, against the plain base model | 40 questions, 2 passes | mean -0.18, 17 wins / 6 ties / 17 losses |
| Median answer length, v2 own terms against base + retrieval | 40 answers each | 1,208 against 4,714 characters |
| Answers with an identifier absent from their own excerpts (mechanical count) | 40 answers each | base + retrieval 28, v2 own search terms 1, v2 with the base model's search terms 3 |
| Pack recall (all fact patterns found in the retrieved text), answerable questions | 37 questions | base model's search terms: 25 hits, 6 partial, 6 miss; v2's own: 25 hits, 9 partial, 3 miss |
| A reviewer's reading of answers for invented or wrong items | 10 answers per set, one reader, one pass | base + retrieval 17 items in 7 answers; v2 own terms 3 items in 3 answers (two silently do nothing; one misleads: a Secret referenced from the KafkaUser, which the User Operator generates itself) |
| Same reading: essential, excerpt-supported step dropped | 10 answers | v2 own terms: 4 of 10 (plus 2 partial); v2 with the base model's search terms: 3 of 10 |

What this means, stated plainly:

- **No gain in judged score.** With its own search terms the adapter ties base + retrieval within noise (-0.07, standard
  error 0.21). With the base model's search terms it is lower (-0.29). Neither row beats base + retrieval.
- **What it is good for:** answers about a quarter the length (1,208 against 4,714 characters at the median), far
  fewer identifiers that are absent from the excerpts (1 of 40 answers against 28 of 40), and the same judged score within
  noise when it writes its own search terms.
- **What it costs:** the short answers drop an essential step that the excerpts support in 3 to 4 of the 10 answers read.
  The project's bar in GOAL.md is at most 1 in 10 answers. This adapter is above that bar on this reading.
- **Fewer invented items is mostly shorter text.** Per 1,000 tokens the reviewer counted 1.27 invented items for
  base + retrieval and 0.87 for v2 with its own search terms on the ten answers; the intervals overlap, so ten answers
  cannot show that v2 is more careful per word. The mechanical count sees identifiers only; it does not see wrong semantics.
- Per category, the adapter is above base + retrieval on Argo CD (+1.40 on 5 questions) and below it on Kafka operations
  (-0.35 on 10), Strimzi (-0.35 on 10) and Kubernetes (-0.60 on 5), with its own search terms. These are small groups.
- Not measured: a quantised or fused build, a live cluster, questions outside the 40.

Speed (3 questions, local GPU, base model with this adapter, frozen index): about 16 to 34 s for the search terms,
3.5 to 9 s for retrieval and 59 to 73 s for the answer. On these 3 questions the runtime with `--adapter`
reproduced the stored search terms, chunk ids and answers exactly (3 of 3 also from the packaged files).

## How to use it

Prerequisites: the repository of the retrieval runtime (`kafka_sre_rag`), the documentation index of release v3.0.0 (its
hash is in `kafka_sre_rag/frozen.json`), mlx-lm 0.32.2 and mlx 0.32.4 on Apple silicon, and the base model
`mlx-community/Qwen3.8-27B-bf16` (revision `6f265714824f3c38d4452baa1628aef3d9b9aae9`) **wrapped so that its chat template
is non-thinking**. The adapter was trained and measured on that wrapper, with the unmodified upstream template the behaviour is not what was measured.
The repository builds the wrapper with `scripts/make_nothink_base.py` (weights are symlinked, only `chat_template.jinja` is changed).

With the runtime, placeholder paths:

```bash
export BASE_PATH=/path/to/nothink-base          # the wrapped base model directory
python -m kafka_sre_rag ask --adapter /path/to/kafka-sre-27b-v2-retrieval-only \
    --sources "Why is my KafkaRebalance stuck in PendingProposal?"
```

`--adapter` applies to both model calls, the search terms and the answer, so the adapter writes its own search terms
(this is the configuration measured in the first row of the table). The runtime reports the run as not the reference
configuration, because the reference is the base model without an adapter. `--dry-run` prints the prompt with the excerpts and loads no model.

Plain mlx-lm, to load the base and the adapter (you must build the retrieval prompt yourself; without excerpts do not use it):

```python
from mlx_lm import load, generate
model, tokenizer = load("/path/to/nothink-base", adapter_path="/path/to/kafka-sre-27b-v2-retrieval-only")
```

Use greedy decoding (temperature 0), at most 300 new tokens for the search terms and 2,048 for the answer, with thinking off, as the measurements did.
The adapter folder needs `adapters.safetensors` and `adapter_config.json`; mlx-lm reads the LoRA settings from the config.

## Training

- **Base:** Qwen/Qwen3.8-27B (Apache-2.0), the `mlx-community/Qwen3.8-27B-bf16` conversion, revision
  `6f265714824f3c38d4452baa1628aef3d9b9aae9`, with the non-thinking chat template described above.
- **Method:** LoRA, rank 16, scale 20, dropout 0.05, all layers; AdamW; batch size 4; maximum sequence length 4,096; loss on the
  answer only; seed 42; learning rate 1e-5 with 100 warmup steps and cosine decay to 1e-7. mlx-lm 0.32.2, mlx 0.32.4.
- **One epoch:** 8,039 train rows, 2,009 steps. The run was resumed from its step-1,050 checkpoint for the remaining 959 steps
  (learning rate 5.479e-6, the schedule's value at step 1,050, decaying to 1e-7, with 20 warmup steps), on the batches not yet seen.
  This file is the final step. Validation loss: best 1.076 at step 1,650, 1.08 at the end.
- **Data:** 8,039 train, 278 validation and 137 test rows of synthetic question and answer pairs, each written from one
  documentation excerpt and checked against it by a verifier model. The training prompts have **no retrieved excerpts**:
  this adapter was trained closed-book and is used with excerpts at run time. That mismatch is why its closed-book score is
  low and why it was measured with retrieval.
- **Quality check of the data:** the hand spot check of the v2 training data was not completed (its gate was never evaluated). This card has no verified
  quality figure for the v2 data. The same check on the v1 data found 5 wrong items in 30.

| Source | Licence | Pin | Train rows (of 8,039) |
|---|---|---|---|
| Kubernetes documentation (`concepts`, `tasks`) | CC BY 4.0 | commit `18f8e9f6d2b0bc9d33e82ceac8abcfffdf8c1ca8` (default branch of 2026-11-11) | 1,757 |
| Strimzi documentation | Apache-2.0 | tag `1.3.1` | 1,530 |
| Argo CD documentation | Apache-2.0 | tag `v3.6.1` | 1,556 |
| Apache Kafka documentation | Apache-2.0 | tag `4.3.2` | 1,490 |
| Apache Kafka generated configuration reference (kafka-site, `content/en/43/generated`) | Apache-2.0 | commit `cd727c4dd8664857751a87cd0442e51a3d3aca8f` (probable; the fetch followed the default branch) | 1,354 |
| Runbooks written for this project from the documentation above | Apache-2.0 | the project repository | 352 |

- **Teacher:** all answers were generated by DeepSeek-V4.1-Flash (MIT licence; local build `DeepSeek-V4.1-Flash-1017-oQ4e-mtp`), which was also the first verifier.
- **Second verifier:** GLM-5.3-Flash (MIT licence; local build `GLM-5.3-Flash-oQ4e`) re-checked 1,263 of the training rows and filtered them; it did not write rows.
  GLM-5.3-Flash also judged the evaluation answers, which does not touch the training data.
- The 8 runbooks held out for evaluation are not in the training data.

## Limitations

- Needs excerpts. Without them it scored -0.18 against the plain base model and it invents; nothing in the weights stops that.
- Short answers drop essential steps (3 to 4 of 10 answers read, above the project's bar of 1 in 10), and its search terms retrieve
  different excerpts than the base model's: the packs matched on 0 of 40 questions.
- It still invents: 3 items in 3 of 10 answers read: two silently ineffective, one that would send an operator the wrong way (a Secret that the User Operator already generates).
- Measured on 40 questions that have been used many times; one reader read 10 answers. Nothing is known outside these.
- Not validated on a quantised or fused build, and not validated against a live cluster.
- The documentation index goes out of date (Kafka 4.3.2, Strimzi 1.3.1, Argo CD v3.6.1, as of its pins). English only.
- Scope is Apache Kafka and Strimzi on Kubernetes with Argo CD; managed Kafka services are out of scope.

## Licence and attribution

The adapter weights are released under the **Apache License 2.0** (`LICENSE`). The base model, Qwen/Qwen3.8-27B, is
licensed under Apache-2.0 (https://huggingface.co/Qwen/Qwen3.8-27B); the adapter is useless without it and is applied to it at load time.
Whether a file of weight deltas is a derivative work of the documentation or of the base model is not settled by the licence texts;
this card and `NOTICE` apply the attribution duties of those licences as a precaution.

**Attribution (CC BY 4.0).** Kubernetes Documentation (content/en/docs/concepts and content/en/docs/tasks), © The Kubernetes Authors,
https://github.com/kubernetes/website (commit 18f8e9f6d2b0bc9d33e82ceac8abcfffdf8c1ca8), licensed under CC BY 4.0,
https://creativecommons.org/licenses/by/4.0/. Changes were made: excerpts of the pages were used by a language model
(DeepSeek-V4.1-Flash) to write question-and-answer pairs, some of which were filtered out; those pairs were used to fine-tune this adapter.
Not endorsed by The Kubernetes Authors or The Linux Foundation. The Kubernetes documentation is licensed under CC BY 4.0 and is provided without warranties, as set out in Section 5 of that licence (Disclaimer of Warranties and Limitation of Liability), https://creativecommons.org/licenses/by/4.0/legalcode.

**Apache-2.0 documentation.** The training pairs were written from the documentation of Apache Kafka (© The Apache Software Foundation,
https://github.com/apache/kafka, tag 4.3.2, and the generated configuration reference of https://github.com/apache/kafka-site),
Strimzi (https://github.com/strimzi/strimzi-kafka-operator, tag 1.3.1) and Argo CD (© The Argo Authors, https://github.com/argoproj/argo-cd, tag v3.6.1),
all under the Apache License 2.0, https://www.apache.org/licenses/LICENSE-2.0. Changes were made: the pages were converted to text and split into excerpts,
a language model wrote question-and-answer pairs from them, some were filtered out, and the pairs were used to fine-tune this adapter.
The adapter contains no copy of the documentation. `NOTICE` has the details per source.

**Models used to build the data.** DeepSeek-V4.1-Flash (MIT licence, © DeepSeek) wrote and checked the training pairs; GLM-5.3-Flash (MIT licence, © Z.AI) was the second verifier.

**Names.** This is an unofficial, community adapter. It is not endorsed by, affiliated with or approved by the Apache Software Foundation, the Strimzi project,
the Argo project, The Kubernetes Authors, The Linux Foundation, Alibaba Cloud (Qwen), DeepSeek or Z.AI. Apache Kafka, Strimzi, Argo CD and Kubernetes are
trademarks of their projects and owners.
