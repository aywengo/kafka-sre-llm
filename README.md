# kafka-sre-llm

A Kafka / Strimzi / Kubernetes / Argo CD SRE assistant on an open-weight 27B model
(Qwen3.8-27B), on Apple Silicon with mlx-lm. This repository holds two things:

- the **retrieval runtime** `kafka_sre_rag/` (v3), which is what currently works;
- the **fine-tuning pipeline** that produced the v1 and v2 adapters (described at the end).

## Status

- **v1** is published as an experimental preview. It is a fine-tuned model that did not pass
  its eval gate.
- **v2** was trained and is not released.
- **v3:** retrieval on the unmodified base model is what works. On 40 real questions, scored
  by a local LLM judge in two passes, base model plus retrieval on the shipped index scores
  **+1.48** against the plain base model (standard error 0.21; 33 wins, 5 ties, 2 losses). The
  earlier measurement on the previous index was +1.19 (26 / 8 / 6). The fine-tuned adapters
  without retrieval score about zero (v1 +0.08, v2 -0.18). An adapter trained with retrieved
  excerpts in the prompt was stopped after a ceiling check. v3 therefore ships as retrieval on
  the base model, with no adapter.
- The size of the result: 40 questions, two judge passes, one judge model, standard error
  0.21. The +1.19 was measured on the previous version of the documentation index (6,292
  chunks); +1.48 is on the shipped one (6,444 chunks). On the 34 questions whose answers are
  identical in both measurements the two agree (+1.41 in each). The whole +0.29 difference
  comes from the 6 questions whose retrieved excerpts changed with the index (-0.08 before,
  +1.83 now). Documentation was added to the index after some of these same questions showed
  no matching content, so that part of the gain is fitted to this question set: it shows the
  index now covers those topics, and is not evidence that retrieval improved in general.

## Published model (experimental preview)

v1 is out as an experimental preview. Its automatic eval gate failed; read the model card
before using it, and verify every config and command before applying it to a cluster. The v1
weights are not what the v3 result is about: the v3 number is for the base model with retrieval.

- Article: [Kafka SRE agent, part 1](https://huggingface.co/blog/aywengo/kafka-sre-agent-part-1)
- MLX 4-bit: [aywengo/kafka-sre-27b-v1-preview-MLX-4bit](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview-MLX-4bit)
- GGUF (Q4_K_M now; Q5_K_M and Q8_0 to follow): [aywengo/kafka-sre-27b-v1-preview-GGUF](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview-GGUF)
- bf16, Hugging Face layout (upload in progress): [aywengo/kafka-sre-27b-v1-preview](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview)

Feedback and review are welcome.

## Run with retrieval (v3)

What it does, for each question:

1. The model writes a line of search terms (config keys, tool names, error names) for the question.
2. BM25 searches the documentation index with the question plus those terms and keeps 40 candidates.
3. A cross-encoder reranks the candidates on CPU.
4. The top chunks (6 by default) go into the prompt, and the model answers from them.

### Install

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

The reranker is `BAAI/bge-reranker-v2-m3`. It is downloaded from Hugging Face on first use
(through sentence-transformers) and runs on CPU. The revision the runtime was measured with
is written in `kafka_sre_rag/frozen.json`; `verify` compares it with your local cache, but the
download itself is not pinned to it yet.

### Get the index

The index is not in this repository (`work/` is git-ignored). Build it from `sources.yaml`:

```bash
python scripts/fetch_sources.py                              # shallow clones into raw/
python scripts/chunk_docs.py --out work/v3/chunks_s1.jsonl   # the path frozen.json expects
python -m kafka_sre_rag verify
```

`verify` checks the index hash, the four prompt hashes and the reranker revision in your
cache against `frozen.json`. A real `ask` refuses to run if they do not match. On the
machine where the index was built, `verify` passes. Not yet verified: that a fresh fetch and chunk on
another machine gives the same hash. If it does not, `verify` fails and you should not trust
the numbers below for your index.

### Ask a question

In-process with mlx-lm (the path that was measured; the model is the bf16 base model wrapped
so that its chat template is non-thinking, see "How v1 was trained", section 4):

```bash
export BASE_PATH=$PWD/runs/v1/base27b
python -m kafka_sre_rag ask --sources "How do I check consumer lag?"
```

`--model` takes the model directory and defaults to `BASE_PATH`. Runtime check: on 3 of the 40
questions, the real path (mlx, base model, frozen index, k6, default mode) reproduced the stored
search terms, chunk ids and answers exactly.

With an OpenAI-compatible server (a `/v1/chat/completions` endpoint):

```bash
export OMLX_URL=<your server>/v1          # or pass --base-url
python -m kafka_sre_rag ask --backend openai --model <model name on the server> "How do I check consumer lag?"
```

`OMLX_API_KEY` is sent as a bearer token if it is set. The client sends temperature 0 and strips
a `<think>` block from the reply, but it cannot switch thinking off for you: do that in the
server. Not yet verified end to end: the server backend has unit tests (a mocked transport)
but no recorded run against a real server.

`chat` is the same loop, one question at a time. Each turn is retrieved and answered on its
own; no conversation history is sent, and multi-turn use is not measured.

To see exactly what would be sent, without a model:

```bash
python -m kafka_sre_rag ask --dry-run --no-rerank-in-dry-run "How do I check consumer lag?"
```

This was run and works: it prints the search-term request and the answer request with the
retrieved excerpts. `--no-rerank-in-dry-run` skips the cross-encoder, so the excerpts shown
are in BM25 order.

Other options: `--k k3` (3 chunks, shorter prompt), `--json` (one JSON object with the text,
sources, search terms, timings and the reasons the run is unmeasured), `--adapter <dir>`
(mlx only).

### Modes

- **default:** the prompt that was measured.
- **strict:** adds one sentence to the user message: "Use only commands, flags, configuration
  keys, resource fields and annotations that appear in the excerpts. If the excerpts do not
  cover part of the question, say so in one sentence instead of filling the gap."
  Measured trade-off: on the 10 answers read in full it had no invented identifiers (the
  default had 7 in 5 answers), and the answers were about half as long (516 against 1,217
  tokens). On the 40 questions it scored -0.11 against default, which is a tie within the
  noise. It did not make the model say so on the questions the index cannot cover: only one
  of those three was refused. Default stays the default.
- **Prompted answer rules:** adding answer rules to the prompt without an adapter scored -0.15
  against base plus retrieval on the 40 questions (standard error 0.16; 9 wins, 16 ties, 15
  losses), so no such prompt ships.

### Sources

`--sources` prints the retrieved chunks under the answer: source, title, file path, BM25 rank
and reranker score. Read them. If the excerpts are not about your question, the answer is
not grounded in them.

### UNMEASURED CONFIGURATION

If a run differs from the configuration behind the headline number, a line
`UNMEASURED CONFIGURATION: <reasons>` is printed (and `measured: false` is in the JSON). The
reasons are: mode other than default, `--k k3`, a generator that is not the reference model
(the bf16 base model, no adapter), a different index file, or no reranker. It does not mean
the run is broken. For `strict` and `k3` it means the number was measured on an earlier
index, not on this exact setup. For another model, a 4-bit or GGUF build, or a server, it
means nobody has measured it.

### Timings

Measured on one Apple Silicon machine in Phase 0, per question:

| Step | Time |
|---|---|
| Search terms (base model) | about 8 s |
| BM25 and rerank on CPU | about 6 s |
| Answer, bf16 model through mlx-lm | about 1.6 minutes (3 chunks) to 1.9 minutes (6 chunks) |

About 2 minutes in total.

### What to expect

- It still invents some flags, fields and commands in the default mode. Verify every command
  and every config key before you run it on a cluster.
- It gets worse on questions the documentation index does not cover. On the three such
  questions in the test set, retrieval lowered the score by 1.0 on average. The index covers
  Apache Kafka, Strimzi, Argo CD and the Kubernetes concepts and tasks pages, plus a few
  Schema Registry, cAdvisor and runbook pages. It has little on Schema Registry and on CPU
  throttling.
- Answers in the default mode are long (about 1,200 tokens on average); 2 of 40 hit the
  2,048-token limit.
- The documentation snapshots are fixed (Kafka 4.3.2, Strimzi 1.3.1, Argo CD v3.6.1).
- English only. Tested on short questions, not on long incident threads.

## How v1 was trained (fine-tuning pipeline)

The sections below describe the fine-tuning pipeline that produced the v1 adapter. v3 does
not use an adapter.

```
sources.yaml ─► fetch_sources.py ─► chunk_docs.py ─► generate_qa.py ─► build_dataset.py
                  raw/                work/chunks.jsonl  work/raw_qa.jsonl   data/*.jsonl
                                                                                 │
              publish.sh ◄── eval/compare.py ◄── mlx_lm.lora -c configs/lora.yaml ┘
```

### 0. Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
hf auth login                      # write token from huggingface.co/settings/tokens
```

For GGUF export, clone and build llama.cpp separately (`cmake -B build && cmake --build build -j`).

### 1. Collect and chunk docs

```bash
python scripts/fetch_sources.py            # sparse, shallow clones into raw/
python scripts/chunk_docs.py               # -> work/chunks.jsonl
```

Put your own runbooks/postmortems in `private/runbooks/` (scrub secrets, hostnames and
customer data first). They are usually the highest-value data you have.

### 2. Generate training data

Serve a strong instruct model locally as the "teacher". With 512 GB you can run a
large one; generation quality matters far more than speed here.

```bash
mlx_lm.server --model <teacher-model> --port 8080
# test on a small slice first and read the output by hand
python scripts/generate_qa.py --limit 30 --verify
less work/raw_qa.jsonl
# then the full run (resumable — rerun to continue)
python scripts/generate_qa.py --verify --concurrency 2
```

Check the teacher model's license: some forbid using outputs to train other models.
LM Studio (`--url http://localhost:1234/v1`) or Ollama (`--url http://localhost:11434/v1
--model <name>`) work too.

### 3. Build the dataset

```bash
python scripts/build_dataset.py                       # -> data/{train,valid,test}.jsonl
# optional: mix in general chat data to limit forgetting
python scripts/build_dataset.py --general general.jsonl --general-ratio 0.15
```

It prints a suggested `iters` value; put it in `configs/lora.yaml` (both `iters` and the
second number in `lr_schedule.arguments`).

### 4. Train

```bash
mlx_lm.lora -c configs/lora.yaml
mlx_lm.lora -c configs/lora.yaml --train false --test   # test-set loss
```

Watch validation loss. If it bottoms out and climbs, stop and use the best checkpoint
(`adapters/kafka-sre-v1/<step>_adapters.safetensors`).

`BASE_PATH` is the base model directory. v1 uses mlx-community/Qwen3.8-27B-bf16 at
revision 6f265714824f3c38d4452baa1628aef3d9b9aae9, wrapped so that the chat template is
always non-thinking (weights are symlinked, only `chat_template.jinja` is patched):

```bash
SNAP=~/.cache/huggingface/hub/models--mlx-community--Qwen3.8-27B-bf16/snapshots/6f265714824f3c38d4452baa1628aef3d9b9aae9
python scripts/make_nothink_base.py "$SNAP" runs/v1/base27b
export BASE_PATH=$PWD/runs/v1/base27b
```

Training (`runs/v1/lora.yaml`), `eval/compare.py --model` and `scripts/publish.sh`
(`BASE=$BASE_PATH`) all use this directory.

Quick manual check:

```bash
mlx_lm.generate --model "$BASE_PATH" --adapter-path adapters/kafka-sre-v1 \
  --prompt "Strimzi rolling update is stuck because of under-replicated partitions. What now?"
```

### 5. Evaluate

Extend `eval/questions.jsonl` to ~200 questions written by you (not generated from the
same docs), then:

```bash
python eval/compare.py --model "$BASE_PATH" --adapter adapters/kafka-sre-v1 \
  --judge-url http://localhost:8080/v1
```

Read the answers yourself too — judge scores are a rough signal, invented config keys
are what matters most in this domain.

### 6. Publish

Fill in `MODEL_CARD.md` (commits are in `raw/*/.source-commit`, eval scores, counts), then:

```bash
HF_USER=<you> NAME=kafka-sre-14b-v1 bash scripts/publish.sh
```

This uploads three repos: full-precision safetensors, an MLX 4-bit variant and GGUF
quants (Q4_K_M, Q5_K_M, Q8_0).

### Rough budget

| Step | Time on M3 Ultra |
|---|---|
| Generation, ~20k chunks with verify pass | 1–3 days, depends on teacher size |
| LoRA, 14B bf16, ~20k examples, 2 epochs | several hours to ~1 day |
| LoRA, 32B | roughly 2–3× the 14B time |

Start with a small run end to end (a few hundred examples, 200 iters) to shake out the
pipeline before the long runs.

## Licence

The code in this repository is licensed under the Apache License 2.0 (see `LICENSE`).

The documentation index is a mixed-licence collection: Apache-2.0 for eight of the sources and
CC BY 4.0 for the Kubernetes documentation. The attribution is in `NOTICE`.

The base model's weights are under their own licence; see the model card (`MODEL_CARD.md`).
