---
license: apache-2.0          # must be compatible with the base model's license
base_model: Qwen/Qwen3.8-27B
library_name: mlx
tags: [kafka, strimzi, kubernetes, gitops, sre, devops, lora]
language: [en]
---

# kafka-sre-27b-v1

> **Automatic eval gate: FAILED.**
> DeepSeek judge on 40 real-world questions: tuned 3.05 vs base 3.15 (gate needed +0.3).
> Better on Strimzi (+1.2), Kafka config (+0.8), Argo CD (+0.4); worse on Kafka
> operations (-1.8), streaming and Kubernetes. Blind human review on 10 of those
> questions favoured the tuned model (8-1 and 5-4), mainly because the base model
> invents CRD fields, endpoints and defaults. The tuned model gives short answers
> (~230 tokens) that are more accurate but less complete for troubleshooting.

A LoRA fine-tune of **Qwen/Qwen3.8-27B** for operating Apache Kafka on Kubernetes:
Strimzi, cluster operations and troubleshooting, GitOps with Argo CD, and
data streaming patterns.

Trained on the mlx-community/Qwen3.8-27B-bf16 conversion, revision 6f26571.

## Intended use
- Answering operational questions and walking through troubleshooting
- Drafting Strimzi custom resources and GitOps manifests for human review

## Usage
Fine-tuned for direct answers (non-thinking mode). The bundled chat template disables the
reasoning block; the upstream Qwen template is not recommended with this model.

## Evaluation (v1)

Judge: DeepSeek-V4.1-Flash, thinking off, scores 1-5 per answer with the reference notes; answer order randomised; the judge was told to score content only and not to penalise answers cut off by the length limit (1024 new tokens). Base and tuned answers use the same non-thinking prompt.

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

Wins:

| Category | Question | Base | Tuned | Judge note |
|---|---|---|---|---|
| kafka-config | What is the difference between log.retention.hours, log.retention.ms and retention.ms, and which one wins? | 1 | 5 | A is correct: log.retention.* are broker defaults, retention.ms is the topic-level override that wins, and log.retention.ms takes precedence over log.retention.hours at broker level. Minor nit: the deprecation caveat about log.ret… |
| argo-cd | Argo CD wants to prune KafkaTopics that the application created at runtime. How do we stop it from deleting to… | 2 | 5 | A recommends AppProject 'exclusions' as a way to exclude KafkaTopics from Argo CD management/pruning; AppProject has no such 'exclusions' field (it has clusterResourceWhitelist/blacklist, namespaceResourceWhitelist/blacklist, and … |
| strimzi | We still run a ZooKeeper-based Strimzi cluster. What do we need to know before upgrading Strimzi to the latest… | 2 | 5 | A is partly correct but contains significant errors: it claims ZooKeeper is only deprecated and that upgrading Strimzi while staying on ZooKeeper is a viable path, whereas recent Strimzi releases removed ZooKeeper support entirely… |
| strimzi | We added two brokers. How do we move existing partitions onto them with Cruise Control? | 2 | 5 | A describes generic Cruise Control REST workflow but ignores Strimzi KafkaRebalance CR, uses invented/incorrect REST endpoints and flags (e.g. /kafka/cruisecontrol/proposal, dryrun, execute), and misses the add-brokers mode and ap… |
| kafka-config | What are the default values of linger.ms and batch.size in current Kafka producers, and when would I change th… | 1 | 4 | A is wrong: it states linger.ms default is 0 in Kafka 4.0 and explicitly denies any change, contradicting KIP-1030 (default 5 ms in 4.0). It also fails to flag version dependence. B correctly gives 5 ms and 16384 bytes, explains t… |

Losses:

| Category | Question | Base | Tuned | Judge note |
|---|---|---|---|---|
| kafka-ops | A broker took 40 minutes to come back after it crashed. Why so long and how can I make restarts faster? | 5 | 2 | A is vague and partly wrong: it claims reducing segment count/size is the main lever, but the reference notes emphasize num.recovery.threads.per.data.dir (default 1), unclean shutdown, partition/segment counts, disk speed, and con… |
| kafka-ops | We run Kafka in KRaft mode. How do I check that the controller quorum is healthy? | 4 | 1 | A is largely correct: uses kafka-metadata-quorum describe --status, mentions leader/epoch/high watermark/voters, and adds replication/logs/metrics. Minor issues: 'describe --replica' is not the documented subcommand (should be --r… |
| argo-cd | A bad Kafka config change was synced by Argo CD. What is the right way to roll it back? | 5 | 2 | A correctly recommends reverting the commit in Git and letting Argo CD sync, keeps Git as source of truth, warns against manual kubectl edits, and notes Strimzi broker restarts for non-dynamic configs. Minor issue: the emergency '… |
| streaming | How do we enable exactly-once processing in Kafka Streams and what does it require? | 5 | 2 | A has a dangerous factual error: claims EOS is only supported when input and output topics are internal topics, which is false (EOS works with external source/sink topics; only repartition/changelog topics are internal). Also misa… |
| kafka-ops | After deploying a new version of our consumer service, the consumer group keeps rebalancing every few minutes … | 4 | 2 | A misdiagnoses the issue: it attributes rebalances to unclean shutdown/session.timeout.ms rather than max.poll.interval.ms exceeded due to slow processing, which is the likely cause per reference. It also incorrectly states sessio… |

### Blind human review (10 of the real questions)

| Reviewer | Tuned better | Base better | Tie | Major errors base / tuned |
|---|---|---|---|---|
| Roman | 8 | 1 | 1 | 9 / 4 |
| Fable | 5 | 4 | 1 | 7 / 3 |
| DeepSeek judge (same 10) | 4 | 5 | 1 | |

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
