# kafka-sre-llm

A Kafka / Strimzi / Kubernetes / Argo CD SRE assistant on an open-weight 27B model
(Qwen3.8-27B), on Apple Silicon with mlx-lm. This repository holds two things:

- the **retrieval runtime** `kafka_sre_rag/` (v3), which is what currently works;
- the **fine-tuning pipeline** that produced the v1 and v2 adapters (described at the end).

## Status

- **v1** is published as an experimental preview. It is a fine-tuned model that did not pass
  its eval gate.
- **v2** is released as a retrieval-only adapter (see "Published models"): it is for use with
  retrieved excerpts and its own search terms, not on its own. Base model plus retrieval stays
  the recommended default.
- **v3:** retrieval on the unmodified base model is what works. On 40 real questions, scored
  by a local LLM judge in two passes, base model plus retrieval on the shipped index scores
  **+1.48** against the plain base model (standard error 0.21; 33 wins, 5 ties, 2 losses). The
  earlier measurement on the previous index was +1.19 (26 / 8 / 6). The fine-tuned adapters
  without retrieval score about zero (v1 +0.08, v2 -0.18). An adapter trained with retrieved
  excerpts in the prompt was stopped after a ceiling check. v3 therefore ships as retrieval on
  the base model; the v2 adapter is an option on top of it, not the default.
- The size of the result: 40 questions, two judge passes, one judge model, standard error
  0.21. The +1.19 was measured on the previous version of the documentation index (6,292
  chunks); +1.48 is on the shipped one (6,444 chunks). On the 34 questions whose answers are
  identical in both measurements the two agree (+1.41 in each; per pass the same answers moved by +0.12 and -0.12, the judge's own variation). The whole +0.29 difference
  comes from the 6 questions whose retrieved excerpts changed with the index (-0.08 before,
  +1.83 now). Documentation was added to the index after some of these same questions showed
  no matching content, so that part of the gain is fitted to this question set: it shows the
  index now covers those topics, and is not evidence that retrieval improved in general.

## Published models

### v1 (experimental preview)

v1 is out as an experimental preview. Its automatic eval gate failed; read the model card
before using it, and verify every config and command before applying it to a cluster. The v1
weights are not what the v3 result is about: the v3 number is for the base model with retrieval.

- Article: [Kafka SRE agent, part 1](https://huggingface.co/blog/aywengo/kafka-sre-agent-part-1)
- MLX 4-bit: [aywengo/kafka-sre-27b-v1-preview-MLX-4bit](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview-MLX-4bit)
- GGUF (Q4_K_M now; Q5_K_M and Q8_0 to follow): [aywengo/kafka-sre-27b-v1-preview-GGUF](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview-GGUF)
- bf16, Hugging Face layout (upload in progress): [aywengo/kafka-sre-27b-v1-preview](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview)

### v2 (retrieval-only adapter)

- Adapter only (LoRA weights, no base model):
  [aywengo/kafka-sre-27b-v2-retrieval-only](https://huggingface.co/aywengo/kafka-sre-27b-v2-retrieval-only)
- **The one rule of use: only with retrieved excerpts, and with its own search terms** (the
  runtime with `--adapter` does both). Without retrieval it scores -0.18 against the plain base
  model (40 questions, two judge passes).
- Result, from `runs/v3/V2RAG_CHECK.md` (40 real questions, two judge passes, one judge model,
  standard error 0.21): with its own search terms it scores -0.07 against base plus retrieval
  (15 wins, 6 ties, 19 losses), which is the same score within noise; with the base model's
  search terms it is behind, -0.29 (9 / 7 / 24). Its answers are about a quarter the length
  (median 1,208 against 4,714 characters). A reviewer's reading of ten answers (one reader, one
  pass) found an essential step dropped in 3 to 4 of them. Pick it for short answers, not for
  better ones; base plus retrieval remains the default. This leans on the 40 real questions
  again.

### Dataset

- Documentation index (the frozen 6,444-chunk file, with its attribution and checksums):
  [aywengo/kafka-sre-docs-index-v3](https://huggingface.co/datasets/aywengo/kafka-sre-docs-index-v3),
  going out with the v3.0.0 release.

Feedback and review are welcome.

## Run with retrieval (v3)

What it does, for each question:

1. The model writes a line of search terms (config keys, tool names, error names) for the question.
2. BM25 searches the documentation index with the question plus those terms and keeps 40 candidates.
3. A cross-encoder reranks the candidates on CPU.
4. The top chunks (up to 6, within 14,000 characters) go into the prompt, and the model answers from them.

### Install

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

The reranker is `BAAI/bge-reranker-v2-m3`. It is downloaded from Hugging Face on first use
(through sentence-transformers) and runs on CPU. The runtime loads it at the revision in
`kafka_sre_rag/frozen.json`; `verify` compares that with your local cache.

### Get the index

The index is not in this repository (`work/` is git-ignored). Two routes.

Download the frozen index (about 10 MB) and check it:

```bash
hf download aywengo/kafka-sre-docs-index-v3 --repo-type dataset --local-dir idx
(cd idx && shasum -a 256 -c SHA256SUMS)
mkdir -p work/v3 && cp idx/chunks.jsonl work/v3/chunks_s1.jsonl
python -m kafka_sre_rag verify
```

Or build it from `sources.yaml`:

```bash
python scripts/fetch_sources.py                              # shallow clones into raw/
python scripts/chunk_docs.py --out work/v3/chunks_s1.jsonl   # the path frozen.json expects
python -m kafka_sre_rag verify
```

`verify` checks the index hash, the four prompt hashes and the reranker revision in your
cache against `frozen.json`. A real `ask` refuses to run if they do not match. On the
machine where the index was built, `verify` passes, and the downloaded index passes `verify` in
a fresh clone of the tag (end-to-end test, 2026-11-18). Not yet verified: that a fresh fetch and
chunk on another machine gives the same hash. If it does not, `verify` fails and you should not
trust the numbers below for your index.

The unit tests (`python -m unittest discover -s tests`) skip four tests that need the index when
`work/v3/chunks_s1.jsonl` is absent.

### Ask a question

In-process with mlx-lm (the path that was measured; the model is the bf16 base model wrapped
so that its chat template is non-thinking, see "How v1 was trained", section 4):

```bash
export BASE_PATH=$PWD/runs/v1/base27b
python -m kafka_sre_rag ask --sources "How do I check consumer lag?"
```

`--model` takes the model directory and defaults to `BASE_PATH`. Runtime check: on 3 of the 40
questions, the real path (mlx, base model, frozen index, k6, default mode) reproduced the stored
search terms, chunk ids and answers exactly. The same holds with the v2 adapter
(`--adapter <adapter directory>` on the same base model): on 3 questions it reproduced the stored
search terms, chunk ids and answers. These checks were run with mlx-lm 0.32.2 and mlx 0.32.4
(`requirements.txt` does not pin them).

With an OpenAI-compatible server (a `/v1/chat/completions` endpoint):

```bash
export OMLX_URL=<your server>/v1          # or pass --base-url
python -m kafka_sre_rag ask --backend openai --model <model name on the server> "How do I check consumer lag?"
```

`OMLX_API_KEY` is sent as a bearer token if it is set. The client sends temperature 0 and strips
a `<think>` block from the reply, but it cannot switch thinking off for you: pass the model's
switch with `--extra-body`, for the base model `--extra-body '{"chat_template_kwargs": {"enable_thinking": false}}'`.
A reasoning model that is left thinking needs a larger `--max-tokens` instead (the run is then
unmeasured), and when the model returns no answer text the runtime exits with an error that says so.
The server backend was run end to end as a path check with timing, not a quality measurement: base
model `mlx-community--Qwen3.8-27B-bf16` served by the local model server (full-precision build),
thinking off, 3 questions. They took 134 to 212 s per question in default mode and 77 to 117 s in
strict mode. The output was **not** identical to the mlx path: the search terms diverge after 260
to 440 characters; that changes the order of the excerpts (on one of 3 questions also two of
them), and the answers differ from the first sentence. The measured results
therefore apply to the mlx path; the server path is checked but not measured. An earlier path
check with another model (GLM-5.3-Flash-oQ8e-mtp, `--max-tokens 16000`) also produced answers.

A second path check, from the published artefacts in a fresh clone of the tag (2026-11-18): the MLX
4-bit build of the base model, `mlx-community/Qwen3.8-27B-4bit` (about 16 GB), downloaded and
served by the local model server, thinking off, ten questions per mode. The median was 70.8 s per
answer in default mode (65.0 to 88.9 s) and 47.2 s in strict mode (36.8 to 64.6 s), and the server
held about 16.9 GB by its own estimate. 20 of 20 answers were non-empty and not cut off. This is a
path check with timing on a large machine, not a quality measurement and not the reference laptop.
If the server lists its models, the client checks the `--model` name against the list and stops
with an error when the server does not have it, instead of letting the server answer with another
model. For a server on the same machine (`localhost`, an address in 127.0.0.0/8, or `::1`) the runtime
ignores the proxy environment variables (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`), whatever `NO_PROXY`
says, so a prompt never goes to a proxy; for a server elsewhere on your network, add its host to `NO_PROXY`.

**Ollama.** Tested with Ollama 0.40.16, which works; 0.14.2 cannot load Qwen3.8 (the library model's own
metadata says it needs 0.32.12 or later, which was not tested, so the minimum version is not established).
Do the steps in this order. First start the server, with a context that holds the prompt and the answer (a
prompt is up to about 4,100 tokens and the answer up to 2,048). If an Ollama server is already running, stop
it first, so that the context setting takes effect:

```bash
OLLAMA_CONTEXT_LENGTH=16384 ollama serve
```

Then, in a second terminal, pull the base model by its exact name and ask (`ollama pull` needs the running
server):

```bash
ollama pull qwen3.8:27b                     # 18.2 GB, Apache-2.0
ollama ps                                   # after the first answer: the CONTEXT column shows 16384
export OMLX_URL=http://localhost:11434/v1
python -m kafka_sre_rag ask --backend openai --model qwen3.8:27b --extra-body '{"reasoning_effort": "none"}' "How do I check consumer lag?"
```

At the time of the test the library model `qwen3.8:27b` is an NVFP4 build on Ollama's MLX runner, not a
Q4_K_M GGUF. A Q4_K_M GGUF from the Hub, `ollama pull hf.co/lmstudio-community/Qwen3.8-27B-GGUF:Q4_K_M`
(17.7 GB, llama.cpp runner), also loads and answered through the runtime once, but was not timed. Ollama's
documented default context is 4,096 on machines under 24 GiB of video memory. With that context the two
runners behaved differently in the test: the llama.cpp runner enforced it and cut an answer off at the limit
(finish_reason `length`; a longer prompt got a clear HTTP 400), the MLX runner did not enforce it. Do not
rely on that; set the context. Use the exact name that `ollama list` shows.

`reasoning_effort: "none"` turns thinking off on Ollama's OpenAI endpoint; `{"think": false}` is ignored
there. The runtime refuses a model name that `ollama list` does not show: the short alias `qwen3.8`, which
Ollama itself resolves, is refused with a message that lists the available names; use `qwen3.8:27b`.
Path check with timing, not a quality measurement: ten questions per mode with `qwen3.8:27b` and a
16,384 context, median 38.6 s per answer in default mode (31.0 to 46.1 s) and 26.7 s in strict mode
(18.3 to 32.1 s). `ollama ps` showed about 19 GB idle and 27 GB as the largest value seen during answers,
which is above the 20 GB bar in `GOAL.md` (the 27 GB is the largest of about 30 samples, not a measured
peak). One strict answer was a one-sentence refusal. Answers on this path were not repeatable at
temperature 0 in one observed case (one strict run of one question gave a 28-token refusal, two further runs
467 and 528 tokens). Quality was not measured. The machine is not the reference laptop, and the Ollama and
the oMLX path above use different 4-bit builds, so the figures are not a comparison of the two servers.

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

### Run as a local service

`ask` starts a process, loads the index, builds the BM25 structures and loads the reranker for every question.
`serve` does that once and keeps it:

```bash
# OMLX_URL set as in the server-backend part above
python -m kafka_sre_rag serve --backend openai --model qwen3.8:27b --extra-body '{"reasoning_effort": "none"}'
# in another terminal
python -m kafka_sre_rag ask --service-url http://127.0.0.1:8765 --stream --sources "How do I check consumer lag?"
curl -s http://127.0.0.1:8765/healthz
```

The service takes the same model options as `ask` (`--backend`, `--model`, `--adapter`, `--base-url`,
`--extra-body`, `--max-tokens`, `--mode`, `--k`), fixed at start. It speaks the OpenAI chat format, so a chat
front end or an editor plug-in can point at `http://127.0.0.1:8765/v1`: `POST /v1/chat/completions` (also with
`"stream": true`, server-sent events), `GET /v1/models` (`kafka-sre` and `kafka-sre-strict`; a request can also
carry `"mode": "strict"`) and `GET /healthz` (index hash, reranker revision, backend, model, whether `verify`
passed at start). The last user message is the question; every request is retrieved and answered on its own and
no conversation history is sent (the response says so in the `X-Kafka-Sre-History` header and the `kafka_sre`
field, which also carries the sources and chunk ids). One request at a time reaches the model; the rest wait.

**Loopback only.** The service binds to `127.0.0.1` and refuses any other address; `--allow-non-local` overrides
that and prints a warning, because `GOAL.md` requires that nothing leaves the machine. Requests whose `Host`
header is not a loopback name are refused too.

`ask --stream` prints the answer as it is generated, with or without a service, and then the sources; a
`<think>` block is not shown. The final text is the same as without `--stream`.

Not yet measured: the time to the first words and the total time per answer with a real model through the
service. Only retrieval was timed without a model (see the work-package report in `runs/m4/reports/`).

### Checking an answer

```bash
python scripts/build_catalogues.py        # once: the catalogues of what really exists (work/m4/catalogues/)
python -m kafka_sre_rag ask --check --sources "How do I check consumer lag?"
python -m kafka_sre_rag serve --check ...  # the same for every request; or {"check": true} in one request body
```

After the answer is complete, the checker looks up the names in it (configuration keys and their values, fields of
Strimzi resources, command-line tools and flags, REST paths, error names, annotations) in the catalogues and in the
excerpts the answer was written from, and prints a short note under the answer. The note has three kinds of lines:

- `Could not confirm: ...` for a name that does not exist (`invented`: the line gives the nearest real name when there
  is one) or a real name used wrongly (`wrong_use`: for example a value or an HTTP method the reference does not allow).
- `Not checked, no reference covers them: ...` for names the catalogues cannot judge (`unverified`). This is
  information, not an accusation.
- When there is nothing to report, one line says that the names were confirmed and how many were checked, so that
  silence is not mistaken for "not checked".

The answer and the prompt are not changed; the note is separate (in `--json` output it is the `check` field with the
findings and the catalogue hash; the service appends it to the answer text after a blank line, so any chat front end
shows it, and also returns it in the `kafka_sre` field). `--catalogues DIR` points at another catalogue directory;
`--check` stops with an error that says how to build them when the directory is missing, and `verify` shows the
catalogue hash, or `catalogues: not built`.

What it can and cannot do: it catches made-up names and wrong values; it does not catch a wrong statement that uses
real names. On the 30 errors that readers had found in stored answers (a set the checker's rules were developed on) it
caught 7, reported 5 as unverified and did not catch 18, of which 15 were wrong statements that use real names and 3
were names a catalogue could in principle judge but this one does not. It also gave no false `invented` finding on 244
stored answers, again the set it was developed on. It is not yet measured on fresh answers (see
`runs/m4/reports/wp2b_checker.md`).

### Modes

- **default:** the prompt that was measured, and the default. Its answers are longer and
  invent or misstate more (table below).
- **strict** (`--mode strict`): adds one sentence to the user message: "Use only commands,
  flags, configuration keys, resource fields and annotations that appear in the excerpts. If
  the excerpts do not cover part of the question, say so in one sentence instead of filling the
  gap."
- **Prompted answer rules:** adding answer rules to the prompt without an adapter scored -0.15
  against base plus retrieval on the 40 questions (standard error 0.16; 9 wins, 16 ties, 15
  losses), so no such prompt ships.

Evidence on the two modes. Both readings are by one reader, one pass; invented or wrong items
are counted from a reviewer's reading against the excerpts, not by a program.

| | Default | Strict |
|---|---|---|
| Invented or wrong items, 10 real questions read in full | 17 in 7 answers | 1 in 1 answer |
| Invented or wrong items, 10 holdout questions, ten fixed positions (1, 5, 10, 15, 21, 24, 28, 31, 37, 41), apart from the ten shortest strict answers | 3 in 1 answer | 0 |
| Essential omissions, wrong refusals (10 real) | not assessed | 2 omissions (1 more partial), 1 wrong refusal |
| Essential omissions, wrong refusals (10 holdout) | 0 (1 partial), 0 | 0 (2 partial), 0 |
| Judged score of strict minus default | - | -0.11 on the 40 real questions (Phase 0), -0.05 on the 42 holdout questions (standard error 0.14); both within noise |
| Mean answer length | 1,217 tokens (40 real), 459 (42 holdout) | 516 tokens (40 real), 160 (42 holdout) |

What this means. The project's bars (`GOAL.md`, on questions not used to build or tune the
assistant): at most 2 invented or wrong items per 10 answers and none that would do damage; at
most 1 wrong refusal and at most 1 dropped essential step per 10. On the 10 real questions (a
set reused many times): default has 17 items in 7 answers, one of them damaging; strict has 1
item, but drops an essential step in 2 answers (1 more partly) and wrongly refuses 1. On the 10
holdout questions: default has 3 items in 1 answer, one probably damaging; strict has none, and
no omissions (2 partial). So: **use `--mode strict` for anything you will paste into a cluster,
and check that no step is missing.** The price is that when retrieval misses the page, strict
can give a non-answer ("the excerpts do not cover this") where default writes something.
Default stays the default because it is the mode every headline number was measured in. Sources: `runs/v3/reports/`
(`strict_mode_invention_reading.md`, `strict_holdout_reading.md`) and `runs/v3/STRICT_HOLDOUT_CHECK.md`.

### Sources

`--sources` prints the retrieved chunks under the answer: source, title, file path, BM25 rank
and reranker score. Read them. If the excerpts are not about your question, the answer is
not grounded in them.

### UNMEASURED CONFIGURATION

If a run differs from the configuration behind the headline number, a line
`UNMEASURED CONFIGURATION: <reasons>` is printed (and `measured: false` is in the JSON). The
reasons are: mode other than default, `--k k3`, a generator that is not the reference model
(the bf16 base model, no adapter), a different index file, or no reranker. It does not mean
the run is broken. For `k3` it means the number was measured on an earlier
index, not on this exact setup. For `strict`, the 40-question figure is from the earlier index;
on the shipped index it was measured on the 42 holdout questions only. For another model, a 4-bit or GGUF build, or a server, it
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
- It gets worse on questions the documentation index does not cover. On the three test
  questions the earlier index could not answer (29, 37, 38), retrieval scored 1.0 below the
  plain base model. Pages on these topics were then added; on the shipped index the same three
  score +2.17 on average (2 passes). That gain is fitted to these questions. On a topic the
  index does not cover, expect answers from unrelated excerpts or from memory. The index covers
  Apache Kafka, Strimzi, Argo CD and the Kubernetes concepts and tasks pages, plus a few
  Schema Registry, cAdvisor and runbook pages. It has little on Schema Registry and on CPU
  throttling.
- Answers in the default mode are long (about 1,200 tokens on average); 2 of 40 hit the
  2,048-token limit.
- Some default-mode answers carried wrong advice, found by a reviewer's reading of 20 answers: a
  statement that the offset-reset time is read in the broker's timezone (in Kafka 4.3.2 a
  `--to-datetime` value without an offset is read as UTC; write the offset explicitly, for example
  `2026-11-18T09:00:00.000+01:00`); a statement that NetworkPolicies are additive in the sense that one policy that
  does not allow the traffic blocks it (the reverse holds: allowed traffic is the union of what every policy selecting the pod
  allows; the source pod's egress must allow it too); and clearing a replication throttle by
  setting the rates to 0 (0 is the tightest throttle, not a clear: the rate is an upper bound in
  bytes per second for the throttled replicas; the Kafka documentation removes the throttle with
  `kafka-reassign-partitions.sh --verify` after the reassignment completes, or remove the
  throttle configs with `kafka-configs.sh --alter --delete-config`). Check such details.
- Retrieval missed the source page on at least 5 of the 42 holdout questions (13, 14, 22, 34,
  41): the source page was not among the excerpts. Strict mode then can give a non-answer (questions 13, 14, 34);
  default may fill the gap from memory.
- Time per answer: about 2 minutes in Phase 0; 112, 151 and 170 s in the one end-to-end check
  (3 questions, default mode, bf16 27B through mlx-lm). `GOAL.md` asks for a median under 60 s;
  that is not met. Strict mode is shorter and quicker (77 to 117 s on the server path check).
  On the 4-bit server path check (10 questions per mode, large machine): median 70.8 s in default mode,
  47.2 s in strict mode.
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

The documentation index is a mixed-licence collection: Apache-2.0 for nine of the sources (eight upstream projects and the project's runbooks) and
CC BY 4.0 for the Kubernetes documentation. The attribution is in `NOTICE`.

The base model's weights are under their own licence; see the model card (`MODEL_CARD.md`).
