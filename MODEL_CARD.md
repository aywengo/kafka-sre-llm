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

> **Automatic eval gate: FAILED** (re-run with a fair 2048-token limit).
> DeepSeek judge on 40 real-world questions: tuned 3.05 vs base 3.12 (gate needed +0.3).
> A second judge (GLM-5.3-Flash) gives +0.18 on 39 of them, also below the gate.
> Better on Strimzi (+1.0), Kafka config (+0.7), Argo CD (+0.6); worse on Kafka
> operations (-1.6), streaming (-2.0) and Kubernetes (-0.2). Blind review by the author and a second
> LLM reviewer on 10 of those questions favoured the tuned model (8-1 and 5-4), mainly because
> the base model invents CRD fields, endpoints and defaults. The tuned model gives short answers
> (~260 tokens on average) that are more accurate but less complete for troubleshooting.

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
  state was not restored, which is why the resume has a short warmup. Later in the resume, training stalled for about 85
  minutes (from 23:14 to 00:42, between steps 310 and 320 of the resume) because another process had filled
  the machine's memory; it then continued unchanged.

## Training data
Synthetic question/answer pairs, 7,201 items (6,858 train / 198 validation / 145 test), generated with
**DeepSeek-V4.1-Flash** (MIT licence; thinking off) from pinned documentation, one excerpt at a time.
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

**Two local LLM judges, no human judge in the tables.** Judge 1 is DeepSeek-V4.1-Flash (local 4-bit build `DeepSeek-V4.1-Flash-1017-oQ4e-mtp`, thinking off). **It is the same model that generated the training data**, so its scores can favour the style and the mistakes of the training data. Judge 2 is GLM-5.3-Flash (local `oQ4e` build, thinking on; the chat template cannot turn it off; about 3,500 tokens and 2.8 minutes per item). Both score each answer 1-5 against reference notes, answer order randomised, content only; they were told not to penalise answers cut off by the length limit. Answers come from greedy decoding (temperature 0) with up to **2048** new tokens; base and tuned use the same non-thinking prompt. Read the scores together with the blind review below.

**Why this replaces the first run.** The first run allowed only 1024 new tokens. 122 of 152 base answers were cut off, so the comparison was not fair. All numbers below use 2048 tokens (8 of 152 base answers and 3 of 152 tuned answers are still cut off). Answers that had finished under 1024 tokens were reused (greedy output does not change). The gate result did not change: on `questions_real` the first run gave base 3.15, tuned 3.05 (-0.10); this run gives base 3.12, tuned 3.05 (-0.08).

### questions.jsonl (12 hand-written, first set)

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| gitops | 4.50 | 2.50 | -2.00 | 2 |
| kafka-ops | 4.33 | 4.00 | -0.33 | 3 |
| kubernetes | 3.00 | 4.00 | +1.00 | 2 |
| schema-registry | 2.00 | 2.00 | +0.00 | 1 |
| streaming | 5.00 | 4.00 | -1.00 | 1 |
| strimzi | 3.67 | 2.67 | -1.00 | 3 |
| **Overall** | **3.83** | **3.25** | **-0.58** | **12** |

### questions_real.jsonl (40 hand-written, publish gate)

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| argo-cd | 2.60 | 3.20 | +0.60 | 5 |
| kafka-config | 2.67 | 3.33 | +0.67 | 6 |
| kafka-ops | 4.00 | 2.40 | -1.60 | 10 |
| kubernetes | 3.60 | 3.40 | -0.20 | 5 |
| schema-registry | 1.50 | 2.00 | +0.50 | 2 |
| streaming | 4.00 | 2.00 | -2.00 | 2 |
| strimzi | 2.70 | 3.70 | +1.00 | 10 |
| **Overall** | **3.12** | **3.05** | **-0.08** | **40** |

### questions_synthetic.jsonl (100 held-out synthetic test items)

| Category | Base | Tuned | Delta | n |
|---|---|---|---|---|
| apache-kafka | 2.59 | 2.00 | -0.59 | 17 |
| apache-kafka-config | 1.80 | 2.70 | +0.90 | 10 |
| argo-cd | 2.06 | 1.83 | -0.22 | 18 |
| kubernetes | 2.71 | 2.54 | -0.17 | 24 |
| strimzi | 2.13 | 2.77 | +0.65 | 31 |
| **Overall** | **2.30** | **2.41** | **+0.11** | **100** |

### Truncation and length (2048-token limit)

| Set | Base truncated | Tuned truncated | Base mean tokens | Tuned mean tokens |
|---|---|---|---|---|
| questions | 2/12 | 0/12 | 1752 | 252 |
| questions_real | 2/40 | 0/40 | 1407 | 218 |
| questions_synthetic | 4/100 | 3/100 | 1331 | 281 |
| **All** | **8/152** | **3/152** | 1385 | 262 |

### Second judge (GLM-5.3-Flash), same 2048-token answers

GLM returned no valid JSON for 4 of 152 questions at 2048 tokens (1 real, 3 synthetic; the reply hit its 32,768-token limit while thinking). Those questions are left out for both judges in this table.

| Set | DeepSeek base / tuned (delta) | GLM-5.3-Flash base / tuned (delta) | n scored by GLM |
|---|---|---|---|
| questions | 3.83 / 3.25 (-0.58) | 3.17 / 3.75 (+0.58) | 12/12 |
| questions_real | 3.18 / 3.08 (-0.10) | 2.97 / 3.15 (+0.18) | 39/40 |
| questions_synthetic | 2.26 / 2.45 (+0.20) | 2.21 / 2.86 (+0.65) | 97/100 |

On the gate set GLM also stays below the +0.3 gate (+0.18). Across all questions the two judges agree on the exact score 55% of the time, within one point 93%, and have a correlation of 0.78; they pick the same better answer (base, tuned or tie) for 65% of questions.

### Five biggest wins and losses on the real set (tuned vs base, DeepSeek, 2048 tokens)

Judge notes are shortened; "base" and "tuned" replace the judge's A/B labels (answer order was randomised).

Wins:

| Category | Question | Base | Tuned | Judge note |
|---|---|---|---|---|
| kafka-config | What is the difference between log.retention.hours, log.retention.ms and retention.ms, and which one wins? | 1 | 5 | tuned is correct: log.retention.* are broker defaults, retention.ms is the topic-level override that wins, and log.retention.ms takes precedence over log.retention.hours at broker level. Minor nit: the deprecation caveat about log.retention.ms is qu… |
| strimzi | We added two brokers. How do we move existing partitions onto them with Cruise Control? | 2 | 5 | base describes raw Cruise Control REST API workflow but misses the Strimzi KafkaRebalance add-brokers mode entirely, and uses invented/incorrect endpoints and params (e.g. /kafka/cruisecontrol/proposal, dryrun=false, /execute, /execution, replica.mo… |
| strimzi | We still run a ZooKeeper-based Strimzi cluster. What do we need to know before upgrading Strimzi to the lates… | 2 | 5 | base is largely wrong/dangerous: claims latest Strimzi still supports ZooKeeper (deprecated only), invents CRD fields (kafka.controller, entityTopic/entityConfig), wrong ports (9092/9093 for controller/broker), wrong migration steps (stopping ZK ens… |
| argo-cd | Argo CD wants to prune KafkaTopics that the application created at runtime. How do we stop it from deleting t… | 2 | 5 | base: invents AppProject 'exclusions' field (not a real Argo CD AppProject field; real mechanism is ignoreDifferences/Resource customizations or sync-options), and wrongly claims ignoreDifferences doesn't prevent pruning without extra config; also c… |
| kafka-config | What are the default values of linger.ms and batch.size in current Kafka producers, and when would I change t… | 2 | 4 | base states linger.ms default is 0 and explicitly claims no change in Kafka 4.0, contradicting KIP-1030 (default 5 ms in 4.0); it also fails to flag version dependence, so capped at 2 despite otherwise good tuning guidance. tuned correctly gives lin… |

Losses:

| Category | Question | Base | Tuned | Judge note |
|---|---|---|---|---|
| kafka-ops | After deploying a new version of our consumer service, the consumer group keeps rebalancing every few minutes… | 5 | 2 | tuned misdiagnoses the problem: it attributes rebalances to unclean shutdown/LeaveGroup and session.timeout.ms, whereas the reference points to max.poll.interval.ms exceeded due to slower processing (members leaving group). It also incorrectly state… |
| kafka-ops | We run Kafka in KRaft mode. How do I check that the controller quorum is healthy? | 4 | 1 | base is largely correct: uses kafka-metadata-quorum describe --status, mentions leader/epoch/high watermark/voters, and adds replication/logs/metrics. Minor issues: 'describe --replica' is not the documented subcommand (should be --replication), and… |
| kafka-ops | We need more throughput on a keyed topic and want to go from 12 to 24 partitions. What are the risks? | 5 | 2 | base is correct, specific and safe: explains hash(key)%partitions remapping, per-key ordering break, split key history, consumer rebalance, producer metadata refresh, Streams/state impact, and gives both in-place and new-topic migration paths plus S… |
| streaming | How do we enable exactly-once processing in Kafka Streams and what does it require? | 5 | 2 | tuned has a serious factual error: it claims EOS is only supported when input and output topics are internal topics, which is wrong (EOS works with external source/sink topics; it's the internal repartition/changelog topics that matter). It also mis… |
| kafka-ops | One broker's data disk fills much faster than the others in the same cluster. How do I find out why and rebal… | 4 | 2 | base is thorough and mostly correct: covers partition/leader skew, kafka-log-dirs, retention configs, reassignment with generate/execute/verify, compaction, and caveats. Minor issues: kafka-leader-election.sh --all-replica-leaders is not a real flag… |

### Blind review by the author and a second LLM reviewer (10 of the real questions)

The blind review used the answers from the first run (1024-token limit); the judge rows use the same answers.

| Reviewer | Tuned better | Base better | Tie | Major errors base / tuned |
|---|---|---|---|---|
| The author | 8 | 1 | 1 | 9 / 4 |
| A second LLM reviewer | 5 | 4 | 1 | 7 / 3 |
| DeepSeek judge (same 10) | 4 | 5 | 1 | |
| GLM-5.3-Flash judge (same 10) | 6 | 2 | 2 | |

Agreement with the author's verdict on the 10: GLM 8, DeepSeek 6. Agreement with the second reviewer: GLM 8, DeepSeek 9. The author and the second reviewer agree on 7 of 10. Ten questions is a small sample.

## Known failure modes
Found in the evaluation and in manual review of answers. Check these especially:
- **Consumer group rebalancing:** misses `max.poll.interval.ms` as the usual cause when members
  leave the group after a deploy.
- **Unclean leader election:** confuses broker-level and topic-level scope.
- **Broker recovery after a crash:** vague; misses `num.recovery.threads.per.data.dir` and the effect of unclean shutdown.
- **Kafka Streams exactly-once:** gives wrong internals (for example, claims EOS only works with internal topics).
- **Schema Registry:** weak on the wire format and the "Unknown magic byte" error.
- **Invents CRD fields:** for example `kafkaVersion` instead of `spec.kafka.version` (seen in a Q4_K_M smoke test).
- **Invented details:** settings, endpoints and examples added in caveats that the source did not
  contain, and wrong placement of a setting (broker vs topic vs client vs Connect worker).
- **No system prompt:** without the recommended system prompt the model can invent tools and
  scripts (for example a non-existent `strimzi-lag-report.sh` for consumer lag). Always use the prompt below.
- **Short answers:** about 260 tokens on average; less complete than the base model for
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
| `kafka-sre-27b-v1-preview-GGUF` | Q4_K_M (Q5_K_M and Q8_0 to follow) | text only (no `mmproj`); for llama.cpp and Ollama |

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

**llama.cpp** (the `-GGUF` repo has Q4_K_M now; Q5_K_M and Q8_0 to follow):
```bash
llama-cli -m kafka-sre-27b-v1-preview-Q4_K_M.gguf --jinja -sys "<system prompt above>" -p "..."
```

**Ollama:** not supported by Ollama 0.14.2. Importing the GGUF works, but loading it fails with
`unknown model architecture: 'qwen35'` (this model is a hybrid with linear-attention layers). Use
llama.cpp. Newer Ollama releases (v0.40.x) list Qwen3.5 and Qwen3.8 support; this GGUF was not
tested with them.

## License
Apache-2.0, the same as the base model. The Kubernetes documentation part of the training data
is CC BY 4.0 (see above).

## Version
v1 is an experimental preview. v2 is planned, with generation rules that reject caveats and
examples not found in the source, a stricter premise check, and a second verifier.
