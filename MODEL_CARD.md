---
license: apache-2.0
base_model: Qwen/Qwen3.8-27B
library_name: mlx
pipeline_tag: text-generation
tags: [kafka, strimzi, kubernetes, argo-cd, gitops, sre, devops, lora, experimental]
language: [en]
---

# kafka-sre-27b-v1-preview

> **Experimental preview — verify all configs and commands before applying them to a cluster. Not for unattended use.**

> **Automatic eval gate: FAILED.**
> DeepSeek judge on 40 real-world questions: tuned 3.05 vs base 3.15 (gate needed +0.3).
> Better on Strimzi (+1.2), Kafka config (+0.8), Argo CD (+0.4); worse on Kafka
> operations (-1.8), streaming and Kubernetes. Blind human review on 10 of those
> questions favoured the tuned model (8-1 and 5-4), mainly because the base model
> invents CRD fields, endpoints and defaults. The tuned model gives short answers
> (~230 tokens) that are more accurate but less complete for troubleshooting.

A LoRA fine-tune of **Qwen/Qwen3.8-27B** for operating Apache Kafka on Kubernetes: Strimzi,
cluster operations and troubleshooting, GitOps with Argo CD, and data streaming patterns.
It is the first version (v1). A second version with cleaner data is planned.

## Intended use
- Answering operational questions and walking through troubleshooting, as a first draft
- Drafting Strimzi custom resources and GitOps manifests **for human review**

Not for: unattended changes to a cluster, or anything where a wrong default or flag is costly
without a person checking it against the current docs.

## Base model and training
- **Base:** Qwen/Qwen3.8-27B. Training used the `mlx-community/Qwen3.8-27B-bf16` conversion,
  revision `6f265714824f3c38d4452baa1628aef3d9b9aae9` (6f26571).
- **Method:** LoRA, rank 16, scale 20, dropout 0.05, on all layers; AdamW; batch size 4;
  max sequence length 4096 (longest example: 1,140 tokens); loss on the answer only.
  **One epoch**, 6,858 train examples (1,714 steps; the last 2 examples are never used by
  mlx-lm's batching). Library: mlx-lm 0.32.2, mlx 0.32.4, on a Mac Studio (Apple silicon).
- **Learning rate:** 1e-5 with 100 warmup steps and cosine decay, until step 600; then 8.06e-6
  (the schedule's value at step 600) decaying to 1e-7 with 20 warmup steps for the rest.
- **Bundled chat template is non-thinking.** The upstream Qwen template runs in thinking mode
  by default. mlx-lm renders training data with that default, so a training row would have
  started with a reasoning preamble and trained the model to emit `</think>`. We wanted direct
  answers, with training, evaluation and serving all using the same prompt. The bundled
  `chat_template.jinja` always renders an empty `<think>\n\n</think>\n\n` block and has no
  reasoning-effort preamble. The upstream template is not recommended with this model.
- **Training curve** (validation loss, 198 held-out items): 1.505 at the start, 1.119 at step 200,
  1.094 at 400, 1.081 at 600, 1.066 at 1000 (resume step 400), 1.053 at the end.
- **Crash and resume.** At step 771 the GPU hit a firmware-detected lockup (macOS gpuEvent report;
  the last checkpoint was step 600). Training resumed from the step-600 checkpoint. mlx-lm
  visits batches in a fixed seeded order, so we reproduced that order exactly (checked against
  the logged trained-token counts of the first 770 steps) and resumed on the 1,114 batches not yet seen. The optimizer
  state was not restored, which is why the resume has a short warmup. A later stall of about 85
  minutes (memory exhaustion by another process) did not affect results.

## Training data
Synthetic question/answer pairs, 7,201 items (6,858 train / 198 validation / 145 test), written by
**DeepSeek-V4.1-Flash** (thinking off) from pinned documentation, one excerpt at a time.
Each item was then checked by the same model against its excerpt (grounding verifier; v2 rules
reject unresolved placeholders, API versions not in the excerpt, settings at the wrong scope and
accepted false premises). The build step drops items mentioning "the excerpt", items with
unresolved `{Attribute}` placeholders, Strimzi manifests with a `kafka.strimzi.io` version other
than `v1`, broken code fences and near-duplicates, and caps the generated protocol reference at
150 items.

| Source | License | Pinned to | Commit (date) | Items |
|---|---|---|---|---|
| Apache Kafka documentation (`docs/`) | Apache-2.0 | tag 4.4.0 | `1156b27` (2026-10-06) | 1,438 |
| Apache Kafka generated config reference (kafka-site `content/en/44/generated`) | Apache-2.0 | default branch | `e6cb1ef` (2026-10-28) | 827 |
| Strimzi documentation (AsciiDoc attributes resolved from the repo) | Apache-2.0 | tag 1.3.1 | `555c3e0` (2026-11-02) | 1,379 |
| Argo CD documentation | Apache-2.0 | tag v3.6.1 | `94037c4` (2026-11-04) | 1,649 |
| Kubernetes documentation, `concepts` and `tasks` (© The Kubernetes Authors) | CC BY 4.0 | default branch | `4f32cd5` (2026-11-04) | 1,908 |

**Attribution (CC BY 4.0).** The Kubernetes documentation (https://github.com/kubernetes/website,
© The Kubernetes Authors) is licensed under CC BY 4.0. It was changed: excerpts were rewritten
into question/answer pairs by a language model, and some were filtered out.

**Quality check of the training data.** We read 30 random items by hand (8 Kafka, 6 Kafka config,
8 Strimzi, 4 Argo CD, 4 Kubernetes): **17 OK, 8 weak, 5 wrong**. The wrong ones were a false premise
accepted (null key vs null value in a KTable join), a deprecated setting and a wrong endpoint
added from memory, a wrong claim about what happens when a connector name is reused, a wrong
cause for consumer session timeouts, and a scope error (manifest-generate-paths). A first
version of the data (30% wrong in the same check) was discarded before training. The verifier is a
weak filter: it rejected only 1-3% of items it re-checked.

## Evaluation

**The judge is the same model that generated the training data** (DeepSeek-V4.1-Flash, local 4-bit build `DeepSeek-V4.1-Flash-1017-oQ4e-mtp`), so its scores can favour the style and the mistakes of the training data; read them together with the blind human review below. Judge settings: thinking off, scores 1-5 per answer with the reference notes; answer order randomised; the judge was told to score content only and not to penalise answers cut off by the length limit (1024 new tokens). Base and tuned answers use the same non-thinking prompt.

### questions.jsonl (12 hand-written, first set)

All pairs:

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| gitops | 4.00 | 2.50 | -1.50 | 2 |
| kafka-ops | 4.33 | 3.67 | -0.67 | 3 |
| kubernetes | 3.00 | 4.50 | +1.50 | 2 |
| schema-registry | 1.00 | 2.00 | +1.00 | 1 |
| streaming | 5.00 | 4.00 | -1.00 | 1 |
| strimzi | 4.00 | 2.67 | -1.33 | 3 |
| **Overall** | **3.75** | **3.25** | **-0.50** | **12** |

Pairs where neither answer was truncated:

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| strimzi | 5.00 | 2.00 | -3.00 | 1 |
| **Overall** | **5.00** | **2.00** | **-3.00** | **1** |

### questions_real.jsonl (40 hand-written, publish gate)

All pairs:

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| argo-cd | 2.80 | 3.20 | +0.40 | 5 |
| kafka-config | 2.50 | 3.33 | +0.83 | 6 |
| kafka-ops | 4.10 | 2.30 | -1.80 | 10 |
| kubernetes | 3.60 | 3.20 | -0.40 | 5 |
| schema-registry | 2.50 | 2.50 | +0.00 | 2 |
| streaming | 3.50 | 2.00 | -1.50 | 2 |
| strimzi | 2.60 | 3.80 | +1.20 | 10 |
| **Overall** | **3.15** | **3.05** | **-0.10** | **40** |

Pairs where neither answer was truncated:

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| kafka-config | 1.00 | 3.50 | +2.50 | 2 |
| kafka-ops | 4.00 | 1.50 | -2.50 | 2 |
| schema-registry | 1.00 | 1.00 | +0.00 | 1 |
| strimzi | 4.00 | 4.00 | +0.00 | 2 |
| **Overall** | **2.71** | **2.71** | **+0.00** | **7** |

### questions_synthetic.jsonl (100 held-out synthetic test items)

All pairs:

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| apache-kafka | 2.65 | 2.06 | -0.59 | 17 |
| apache-kafka-config | 1.70 | 2.70 | +1.00 | 10 |
| argo-cd | 2.00 | 1.83 | -0.17 | 18 |
| kubernetes | 2.62 | 2.54 | -0.08 | 24 |
| strimzi | 2.13 | 2.81 | +0.68 | 31 |
| **Overall** | **2.27** | **2.43** | **+0.16** | **100** |

Pairs where neither answer was truncated:

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| apache-kafka | 4.25 | 2.75 | -1.50 | 4 |
| apache-kafka-config | 1.75 | 3.50 | +1.75 | 4 |
| argo-cd | 2.20 | 2.80 | +0.60 | 5 |
| kubernetes | 3.50 | 2.75 | -0.75 | 4 |
| strimzi | 2.00 | 2.60 | +0.60 | 5 |
| **Overall** | **2.68** | **2.86** | **+0.18** | **22** |

### Truncation and length

| Set | Base truncated | Tuned truncated | Base mean tokens | Tuned mean tokens |
|---|---|---|---|---|
| questions | 11/12 | 0/12 | 1014 | 252 |
| questions_real | 33/40 | 0/40 | 993 | 218 |
| questions_synthetic | 78/100 | 3/100 | 991 | 250 |
| **All** | **122/152** | **3/152** | | |

### Five biggest wins and losses on the real set (tuned vs base)

Judge notes are shortened; "base" and "tuned" replace the judge's A/B labels (answer order was randomised).

Wins:

| Category | Question | Base | Tuned | Judge note |
|---|---|---|---|---|
| kafka-config | What is the difference between log.retention.hours, log.retention.ms and retention.ms, and which one wins? | 1 | 5 | tuned is correct: log.retention.* are broker defaults, retention.ms is the topic-level override that wins, and log.retention.ms takes precedence over log.retention.hours at broker level. Minor nit: the deprecation caveat about log.retention.ms is questionable,… |
| argo-cd | Argo CD wants to prune KafkaTopics that the application created at runtime. How do we stop it from deleting to… | 2 | 5 | base recommends AppProject 'exclusions' as a way to exclude KafkaTopics from Argo CD management/pruning; AppProject has no such 'exclusions' field (it has clusterResourceWhitelist/blacklist, namespaceResourceWhitelist/blacklist, and sync windows), so this is a… |
| strimzi | We still run a ZooKeeper-based Strimzi cluster. What do we need to know before upgrading Strimzi to the latest… | 2 | 5 | base is partly correct but contains significant errors: it claims ZooKeeper is only deprecated and that upgrading Strimzi while staying on ZooKeeper is a viable path, whereas recent Strimzi releases removed ZooKeeper support entirely. It also invents/implies i… |
| strimzi | We added two brokers. How do we move existing partitions onto them with Cruise Control? | 2 | 5 | base describes generic Cruise Control REST workflow but ignores Strimzi KafkaRebalance CR, uses invented/incorrect REST endpoints and flags (e.g. /kafka/cruisecontrol/proposal, dryrun, execute), and misses the add-brokers mode and approval annotation. tuned co… |
| kafka-config | What are the default values of linger.ms and batch.size in current Kafka producers, and when would I change th… | 1 | 4 | base is wrong: it states linger.ms default is 0 in Kafka 4.0 and explicitly denies any change, contradicting KIP-1030 (default 5 ms in 4.0). It also fails to flag version dependence. tuned correctly gives 5 ms and 16384 bytes, explains trade-offs and caveats, … |

Losses:

| Category | Question | Base | Tuned | Judge note |
|---|---|---|---|---|
| kafka-ops | A broker took 40 minutes to come back after it crashed. Why so long and how can I make restarts faster? | 5 | 2 | tuned is vague and partly wrong: it claims reducing segment count/size is the main lever, but the reference notes emphasize num.recovery.threads.per.data.dir (default 1), unclean shutdown, partition/segment counts, disk speed, and controlled shutdown. It omits… |
| kafka-ops | We run Kafka in KRaft mode. How do I check that the controller quorum is healthy? | 4 | 1 | base is largely correct: uses kafka-metadata-quorum describe --status, mentions leader/epoch/high watermark/voters, and adds replication/logs/metrics. Minor issues: 'describe --replica' is not the documented subcommand (should be --replication), and some JMX m… |
| argo-cd | A bad Kafka config change was synced by Argo CD. What is the right way to roll it back? | 5 | 2 | base correctly recommends reverting the commit in Git and letting Argo CD sync, keeps Git as source of truth, warns against manual kubectl edits, and notes Strimzi broker restarts for non-dynamic configs. Minor issue: the emergency 'argocd app set --sync-optio… |
| streaming | How do we enable exactly-once processing in Kafka Streams and what does it require? | 5 | 2 | tuned has a dangerous factual error: claims EOS is only supported when input and output topics are internal topics, which is false (EOS works with external source/sink topics; only repartition/changelog topics are internal). Also misattributes transaction.stat… |
| kafka-ops | After deploying a new version of our consumer service, the consumer group keeps rebalancing every few minutes … | 4 | 2 | tuned misdiagnoses the issue: it attributes rebalances to unclean shutdown/session.timeout.ms rather than max.poll.interval.ms exceeded due to slow processing, which is the likely cause per reference. It also incorrectly states session.timeout.ms is a consumer… |

### Blind human review (10 of the real questions)

| Reviewer | Tuned better | Base better | Tie | Major errors base / tuned |
|---|---|---|---|---|
| Roman | 8 | 1 | 1 | 9 / 4 |
| Fable | 5 | 4 | 1 | 7 / 3 |
| DeepSeek judge (same 10) | 4 | 5 | 1 | |

## Known failure modes
Found in the evaluation and in manual review of answers. Check these especially:
- **Consumer group rebalancing:** misses `max.poll.interval.ms` as the usual cause when members
  leave the group after a deploy.
- **Unclean leader election:** confuses broker-level and topic-level scope.
- **Broker recovery after a crash:** vague; misses `num.recovery.threads.per.data.dir` and the effect of unclean shutdown.
- **Kafka Streams exactly-once:** gives wrong internals (for example, claims EOS only works with internal topics).
- **Schema Registry:** weak on the wire format and the "Unknown magic byte" error.
- **Invented details:** settings, endpoints and examples added in caveats that the source did not
  contain, and wrong placement of a setting (broker vs topic vs client vs Connect worker).
- **No system prompt:** without the recommended system prompt the model can invent tools and
  scripts (for example a non-existent `strimzi-lag-report.sh` for consumer lag). Always use the prompt below.
- **Short answers:** about 230 tokens on average; less complete than the base model for
  multi-step troubleshooting.

## Limitations
- Knowledge is frozen at the documentation snapshots in the table above. CRD fields, config
  keys and defaults change between releases; verify against current docs.
- The training data is synthetic and was checked by the model that wrote it (see Evaluation).
- It can produce plausible but wrong configuration. Pair it with retrieval over current docs or
  tool access for exact values.
- English only. Evaluated only on short questions, not on long incident threads.

## Recommended system prompt
> You are an expert Site Reliability Engineer for Apache Kafka, Strimzi, Kubernetes,
> GitOps and data streaming platforms. Give precise, practical answers with commands or
> YAML where useful, and call out risks and version-specific behaviour. If you are not
> sure about an exact config key or flag, say so.

Use the bundled chat template (non-thinking). Prompts end with an empty think block.

## Files and formats
| Repo | Content | Notes |
|---|---|---|
| `kafka-sre-27b-v1-preview` | bf16 safetensors, Hugging Face layout | loads with `transformers` (`Qwen3_5ForConditionalGeneration`); vision tower is the unchanged upstream one; the MTP head is not included (not fine-tuned) |
| `kafka-sre-27b-v1-preview-MLX-4bit` | MLX 4-bit (group size 64) | text only; for `mlx-lm` on Apple silicon |
| `kafka-sre-27b-v1-preview-GGUF` | Q4_K_M, Q5_K_M, Q8_0 | text only (no `mmproj`); for llama.cpp and Ollama |

mlx-lm stores Qwen3.5-family weights in its own layout (RMSNorm weights shifted by +1, conv1d
weight transposed). The bf16 and GGUF files were made from a copy converted back to the
Hugging Face layout (`scripts/mlx_to_hf_layout.py`), because the raw fused model gives gibberish in
llama.cpp. Checked: the converted model loads in `transformers` with no missing or extra tensors
and no shape errors, and mlx-lm gives identical greedy output (30 tokens, 3 prompts) from the
converted copy and the raw fused model. Perplexity on a 2,665-token documentation text:
3.775 for the MLX bf16 model, 3.784 for the Q8_0 GGUF in llama.cpp.

## How to run
**MLX** (Apple silicon):
```bash
pip install mlx-lm
mlx_lm.generate --model aywengo/kafka-sre-27b-v1-preview-MLX-4bit --max-tokens 600 \
  --system-prompt "<system prompt above>" \
  --prompt "Strimzi rolling update is stuck because of under-replicated partitions. What now?"
```

**llama.cpp** (GGUF files in the `-GGUF` repo: Q4_K_M, Q5_K_M, Q8_0):
```bash
llama-cli -m kafka-sre-27b-v1-preview-Q4_K_M.gguf --jinja -sys "<system prompt above>" -p "..."
```

**Ollama** (Modelfile not tested): save as `Modelfile` next to the GGUF file, then `ollama create kafka-sre -f Modelfile`:
```
FROM ./kafka-sre-27b-v1-preview-Q4_K_M.gguf
TEMPLATE """{{- if .System }}<|im_start|>system
{{ .System }}<|im_end|>
{{ end }}<|im_start|>user
{{ .Prompt }}<|im_end|>
<|im_start|>assistant
<think>

</think>

"""
PARAMETER stop "<|im_end|>"
```

## License
Apache-2.0, the same as the base model. The Kubernetes documentation part of the training data
is CC BY 4.0 (see above).

## Version
v1 is an experimental preview. v2 is planned, with generation rules that reject caveats and
examples not found in the source, a stricter premise check, and a second verifier.
